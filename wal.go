package hazedb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// WAL record framing — a typed, versioned, self-delimiting envelope:
//
//	magic:2     (walMagic) — guards against reading non-WAL bytes
//	type:1      (recMutation, recTxn, recCreateTable, recDropTable)
//	version:1   (walVersion) — replay aborts on any non-current version
//	length:4    (payload length, little-endian)
//	payload     (length bytes)
//	crc32c:4    (Castagnoli, over magic|type|version|length|payload)
//
// The payload format (typed mutations, cells, rows, txn) lives in
// mutation_codec.go; this file owns the envelope + the WAL mechanics.
//
// Born-sealed segments. The WAL is a directory of immutable segment files
// (seg-<n>.wal). Writes accumulate in an in-memory buffer; the buffer is sealed
// into the NEXT segment — written to a temp file, fsynced, and atomically
// renamed into place — as soon as it reaches flushMaxBytes (1 MiB) OR the flush
// interval (~0.5s) elapses, whichever comes first. There is no "active" file
// being appended to: every seg-*.wal on disk is complete by construction (the
// atomic rename makes a partial write invisible), so recovery never has to
// tolerate a torn record — any seg-*.wal that fails to parse is real corruption,
// not an interrupted tail. A crash loses only the un-sealed in-memory buffer: at
// most the flush window of the most recent writes. Stronger durability than that
// — acknowledge only after fsync — is deliberately not offered; reach for a
// disk-first database if you need it.
const (
	walMagic   uint16 = 0x485A // "HZ"
	walVersion uint8  = 2      // current envelope version; replay rejects any other

	recMutation    uint8 = 1
	recTxn         uint8 = 2 // transaction: a group of sub-mutations, atomic
	recCheckpoint  uint8 = 3 // reserved — unused (the snapshot base is the SQLite mirror)
	recCreateTable uint8 = 4 // catalog: CREATE TABLE
	recDropTable   uint8 = 5 // catalog: DROP TABLE
)

// flushMaxBytes is the size trigger: the buffer seals into a segment once it
// reaches this, bounding RAM and segment size under load. Not an operator
// option. A record is never split across a flush, so a single record larger
// than flushMaxBytes simply makes a larger segment. The time trigger
// (defaultFlushInterval, in options.go) bounds the crash-loss window when writes
// are slow; whichever fires first seals.
const flushMaxBytes = 1 << 20 // 1 MiB

// crc32c is the Castagnoli table used for every envelope checksum.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// errWALFraming marks a record whose ENVELOPE is unreadable — a bad magic or a
// CRC mismatch on a fully-present record. This is bit-rot: the bytes are garbage
// and no binary can recover them, so recovery and the drain apply the good prefix
// before it, log, and skip the rest of the segment. It is deliberately distinct
// from a version mismatch (ErrWALVersion) and from an undecodable but CRC-valid
// payload (ErrWALCorrupt) — both of those carry intact, intentional bytes and
// stay FATAL, because silently dropping a committed record is a data-loss bug.
var errWALFraming = errors.New("hazedb: WAL framing corrupt")

// errWALMissingSegment marks a gap in the segment numbering above the drain
// cursor. Born-sealed numbers never skip and drained segments are reclaimed from
// the bottom, so the undrained range is always contiguous; a missing number means
// a committed segment was lost externally. Recovery refuses to boot past it and
// the drain refuses to advance the cursor past it — the higher segments depend on
// the missing one, so crossing the gap silently would recover an inconsistent
// state and mirror later mutations onto a missing base.
var errWALMissingSegment = errors.New("hazedb: WAL segment missing")

// appendU16LE appends v little-endian.
func appendU16LE(b []byte, v uint16) []byte { return append(b, byte(v), byte(v>>8)) }

type wal struct {
	mu   sync.Mutex
	buf  []byte // pending records, sealed into the next segment on flush
	dir  string // segment directory
	seg  uint64 // last sealed segment number; the next flush writes seg+1
	recs uint64 // envelopes written (one per writeRecord call; observability/tests)

	// err is the sticky WAL error. Once set (a failed write/flush), every
	// subsequent write returns it; recovery is only via close+reopen.
	err error

	// failWrites is test-only fault injection: when set, writeRecord fails (and
	// sets the sticky error), to exercise WAL-failure atomicity paths.
	failWrites bool

	// Background flush goroutine. stop is closed by close(); the loop signals
	// done via wg. Both nil/zero until startFlusher runs.
	stop chan struct{}
	wg   sync.WaitGroup

	// close() may be called more than once (explicit Close + t.Cleanup);
	// closeOnce makes it idempotent and closeErr returns the same result.
	closeOnce sync.Once
	closeErr  error
}

// openWAL opens (creating if needed) the segment directory, clears stale segment
// temps from a crash mid-flush, and scans for the highest existing segment so the
// next flush seals after it. It does NOT start the flusher — the caller replays
// existing segments first, then startFlusher().
func openWAL(dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}
	if err := removeStaleTemps(dir); err != nil {
		return nil, err
	}
	maxSeg, err := scanMaxSeg(dir)
	if err != nil {
		return nil, err
	}
	return &wal{dir: dir, seg: maxSeg, buf: make([]byte, 0, flushMaxBytes)}, nil
}

// startFlusher launches the background flush goroutine when interval > 0.
// interval <= 0 leaves the WAL in manual-flush mode (flush()/Close only) — for
// test determinism. Started by Open after replay so it never races a replay
// reader. It seals the pending buffer every interval; the size trigger fires
// inline on writeRecord in between.
func (w *wal) startFlusher(interval time.Duration) {
	if interval <= 0 {
		return
	}
	w.stop = make(chan struct{})
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-t.C:
				w.mu.Lock()
				if w.err == nil {
					_ = w.flushLocked()
				}
				w.mu.Unlock()
			}
		}
	}()
}

// writeRecord serialises one op + payload into the pending buffer. Holds w.mu so
// records are appended whole and in order. A failed write sets the sticky error
// and reports it so the caller aborts before applying the mutation to memory.
// When the buffer reaches flushMaxBytes it seals inline.
func (w *wal) writeRecord(recType uint8, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.failWrites {
		w.err = fmt.Errorf("wal: injected write failure (test)")
		return w.err
	}

	plen := len(payload)
	var hdr [8]byte
	binary.LittleEndian.PutUint16(hdr[0:2], walMagic)
	hdr[2] = recType
	hdr[3] = walVersion
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(plen))

	start := len(w.buf)
	w.buf = append(w.buf, hdr[:]...)
	w.buf = append(w.buf, payload...)
	crc := crc32.Checksum(w.buf[start:start+8+plen], crc32c)
	var crcb [4]byte
	binary.LittleEndian.PutUint32(crcb[:], crc)
	w.buf = append(w.buf, crcb[:]...)
	w.recs++

	if len(w.buf) >= flushMaxBytes {
		return w.flushLocked()
	}
	return nil
}

// flushLocked seals the pending buffer into the next segment: write to a temp
// file, fsync, atomically rename into place, fsync the directory. Caller holds
// w.mu. A no-op when the buffer is empty. Any failure sets the sticky error.
// Because the rename is atomic, a crash mid-flush leaves either no segment or a
// complete one — never a torn seg-*.wal.
func (w *wal) flushLocked() error {
	if w.err != nil {
		return w.err
	}
	if len(w.buf) == 0 {
		return nil
	}
	n := w.seg + 1
	final := w.segPath(n)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		w.err = fmt.Errorf("wal: create %q: %w", tmp, err)
		return w.err
	}
	if _, err := f.Write(w.buf); err != nil {
		f.Close()
		w.err = fmt.Errorf("wal: write %q: %w", tmp, err)
		return w.err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		w.err = fmt.Errorf("wal: sync %q: %w", tmp, err)
		return w.err
	}
	if err := f.Close(); err != nil {
		w.err = fmt.Errorf("wal: close %q: %w", tmp, err)
		return w.err
	}
	if err := os.Rename(tmp, final); err != nil {
		w.err = fmt.Errorf("wal: rename %q: %w", tmp, err)
		return w.err
	}
	// Make the rename's directory entry durable across power loss. On Unix a dir
	// fsync is supported and meaningful, so a failure is a real durability fault
	// and is made sticky — the symmetric counterpart to the file fsync above. On
	// Windows it is a no-op (FlushFileBuffers rejects a directory handle).
	if err := fsyncDir(w.dir); err != nil {
		w.err = fmt.Errorf("wal: sync dir %q: %w", w.dir, err)
		return w.err
	}
	w.seg = n
	w.buf = w.buf[:0]
	return nil
}

// flush seals the pending buffer into a segment now (the manual durability
// boundary behind FlushWAL() and the drain's final seal). Safe on a nil/empty
// buffer.
func (w *wal) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushLocked()
}

// close stops the flusher and seals any remaining buffer into a final segment.
// Idempotent and safe on a nil wal. The flusher is stopped (and joined) BEFORE
// taking w.mu so it cannot be blocked on the lock while close waits for it.
func (w *wal) close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		if w.stop != nil {
			close(w.stop)
			w.wg.Wait()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		w.closeErr = w.flushLocked()
	})
	return w.closeErr
}

// replayFile replays one open WAL file into apply (the recovery path). Reads via
// scanRecords; the caller dispatches by record type.
func (w *wal) replayFile(f *os.File, apply func(recType uint8, payload []byte) error) error {
	return scanRecords(f, apply)
}

// scanRecords reads f from the start to EOF, handing each complete record's
// (type, payload) to apply. Pure — it touches no WAL state, so a drainer can
// scan a segment without disturbing the live WAL.
//
// Born-sealed means a visible segment is complete by construction, so any short
// or truncated tail is corruption, not an interrupted write: a partial header, a
// truncated payload, or a declared length running past EOF all return
// errWALFraming, as does a bad magic or a CRC mismatch on a fully-present record.
// These are one and the same non-fatal break — the caller applies the good prefix
// before it, logs, and skips the rest of the segment, so a truncated suffix is
// reported rather than silently dropped. A clean EOF on a record boundary is the
// normal end of segment and returns nil. A non-current version returns
// ErrWALVersion, and an apply() error (an undecodable but CRC-valid payload, or an
// unknown record type) propagates unchanged — both FATAL: the bytes are
// intact/intentional, so the caller must STOP, never silently drop a committed
// record.
// payload aliases the read buffer; the caller's decoders copy what they keep.
func scanRecords(f *os.File, apply func(recType uint8, payload []byte) error) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	// Buffer the sequential reads: each record otherwise costs two read(2) calls
	// (header + payload). bufio batches the underlying reads.
	r := bufio.NewReader(f)
	var pos int64
	var hdr [8]byte
	var buf []byte // grown once, reused per record — decoders copy what they keep
	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			// Partial header. Born-sealed: a sealed segment is complete, so trailing
			// bytes shorter than a header are corruption, not an interrupted write.
			return fmt.Errorf("%w: partial header at offset %d", errWALFraming, pos)
		}
		if err != nil {
			return err
		}
		pos += 8

		magic := binary.LittleEndian.Uint16(hdr[0:2])
		recType := hdr[2]
		version := hdr[3]
		length := binary.LittleEndian.Uint32(hdr[4:8])
		if magic != walMagic {
			return fmt.Errorf("%w: bad magic %#04x at offset %d", errWALFraming, magic, pos-8)
		}
		if version != walVersion {
			return fmt.Errorf("%w: record version %d != supported %d — shut the old binary down cleanly (which seals + drains the WAL) before upgrading", ErrWALVersion, version, walVersion)
		}
		// Bounds-check before allocating (also caps a bogus huge length, so a
		// corrupt record can't drive an OOM). If payload+crc can't fit in what
		// remains, the record is truncated — corruption under born-sealed.
		if int64(length)+4 > fileSize-pos {
			return fmt.Errorf("%w: record length %d past end at offset %d", errWALFraming, length, pos-8)
		}
		need := int(length) + 4
		if cap(buf) < need {
			buf = make([]byte, need)
		} else {
			buf = buf[:need]
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf("%w: truncated payload at offset %d", errWALFraming, pos-8)
			}
			return err
		}
		pos += int64(length) + 4
		payload := buf[:length]
		crc := binary.LittleEndian.Uint32(buf[length:])
		want := crc32.Update(crc32.Update(0, crc32c, hdr[:]), crc32c, payload)
		if want != crc {
			return fmt.Errorf("%w: crc mismatch at offset %d", errWALFraming, pos-int64(length)-4-8)
		}
		if err := apply(recType, payload); err != nil {
			return err
		}
	}
}
