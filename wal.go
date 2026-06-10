package hazedb

import (
	"bufio"
	"encoding/binary"
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
//	type:1      (recMutation, recTxn; recCheckpoint reserved for M7)
//	version:1   (walVersion) — replay aborts on any non-current version
//	length:4    (payload length, little-endian)
//	payload     (length bytes)
//	crc32c:4    (Castagnoli, over magic|type|version|length|payload)
//
// The payload format (typed mutations, cells, rows, txn) lives in
// mutation_codec.go; this file owns the envelope + the WAL file mechanics.
const (
	walMagic   uint16 = 0x485A // "HZ"
	walVersion uint8  = 2 // bumped when opUpdate nsets widened u8->u16; replay rejects any other version

	recMutation    uint8 = 1
	recTxn         uint8 = 2 // transaction: a group of sub-mutations, atomic
	recCheckpoint  uint8 = 3 // reserved — snapshot/checkpoint
	recCreateTable uint8 = 4 // catalog: CREATE TABLE
	recDropTable   uint8 = 5 // catalog: DROP TABLE
)

// crc32c is the Castagnoli table used for every envelope checksum.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// appendU16LE appends v little-endian.
func appendU16LE(b []byte, v uint16) []byte { return append(b, byte(v), byte(v>>8)) }

type wal struct {
	mu      sync.Mutex
	f       *os.File
	bw      *bufio.Writer
	scratch []byte // reused under mu; size grows as needed
	lsn     uint64

	// Durability config (immutable after openWAL).
	sync         bool // fsync on each ticker fire (when dirty)
	syncPerWrite bool // flush+fsync after every record, under mu

	// dirtySinceSync is set on every record append and cleared only after a
	// successful fsync. It is NOT derived from bw.Buffered(): bufio can
	// auto-flush a full buffer into the page cache while leaving Buffered()
	// at 0, so gating fsync on Buffered() would leave that data unsynced and
	// break the "<= flush interval" power-loss bound of WALSync mode.
	dirtySinceSync bool

	// err is the sticky WAL error. Once set (a failed append/flush/sync),
	// every subsequent write returns it; recovery is only via close+reopen.
	err error

	// failWrites is test-only fault injection: when set, writeRecord fails
	// (and sets the sticky error), to exercise WAL-failure atomicity paths.
	failWrites bool

	// Background flush/sync ticker. stop is closed by close(); the loop
	// signals done via wg. Both nil/zero until startTicker runs.
	stop chan struct{}
	wg   sync.WaitGroup

	// Segmentation (segmented mode only; dir == "" ⇒ single-file mode). In
	// segmented mode the WAL is a directory of sealed segment files plus one
	// active segment (w.f / w.bw). rotate() seals the active segment and opens
	// the next, so a background drainer can consume sealed segments without ever
	// touching the file being appended to. See docs/durability.md.
	dir        string        // segment directory; empty ⇒ single-file mode
	seg        uint64        // active segment number (segmented mode)
	segHasData bool          // active segment has ≥1 record since it was opened
	rotateStop chan struct{} // closed by close() to stop the rotate ticker

	// close() may be called more than once (explicit Close + t.Cleanup);
	// closeOnce makes it idempotent and closeErr returns the same result.
	closeOnce sync.Once
	closeErr  error
}

// openWAL opens (creating if needed) the WAL file and records the durability
// config. It does NOT start the ticker and does NOT seek — the caller replays
// first, then calls seekToEnd() and startTicker(), so the ticker never races
// the replay reader on the shared file handle.
func openWAL(path string, sync, syncPerWrite bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &wal{
		f:            f,
		bw:           bufio.NewWriterSize(f, 64<<10),
		scratch:      make([]byte, 0, 4096),
		sync:         sync,
		syncPerWrite: syncPerWrite,
	}, nil
}

// seekToEnd positions the file at EOF so appends land after replayed data.
func (w *wal) seekToEnd() error {
	_, err := w.f.Seek(0, io.SeekEnd)
	return err
}

// startTicker launches the background flush/sync goroutine when interval > 0.
// interval <= 0 leaves the WAL in manual-flush mode (FlushWAL() only).
func (w *wal) startTicker(interval time.Duration) {
	if interval <= 0 {
		return
	}
	w.stop = make(chan struct{})
	w.wg.Add(1)
	go w.tickerLoop(interval)
}

// tickerLoop runs until close() closes w.stop. On each fire it flushes any
// buffered bytes and, when sync is enabled and data is dirty, fsyncs — all
// under w.mu (bufio.Writer is not concurrent-safe; appenders hold the same
// lock). The fsync decision keys off dirtySinceSync, never bw.Buffered().
func (w *wal) tickerLoop(interval time.Duration) {
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
				if w.bw.Buffered() > 0 {
					if err := w.bw.Flush(); err != nil {
						w.err = fmt.Errorf("wal: flush (ticker): %w", err)
					}
				}
				if w.sync && w.dirtySinceSync && w.err == nil {
					if err := w.f.Sync(); err != nil {
						w.err = fmt.Errorf("wal: sync (ticker): %w", err)
					} else {
						w.dirtySinceSync = false
					}
				}
			}
			w.mu.Unlock()
		}
	}
}

// writeRecord serialises one op + payload and appends it. Holds w.mu so the
// bufio.Writer sees one record at a time and the scratch buffer is reusable.
// A failed append sets the sticky error and reports it so the caller aborts
// before applying the mutation to memory (RFC pipeline step 6).
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
	need := 8 + plen + 4 // header(8) + payload + crc(4)
	if cap(w.scratch) < need {
		w.scratch = make([]byte, 0, need*2)
	}
	buf := w.scratch[:need]

	binary.LittleEndian.PutUint16(buf[0:2], walMagic)
	buf[2] = recType
	buf[3] = walVersion
	binary.LittleEndian.PutUint32(buf[4:8], uint32(plen))
	copy(buf[8:8+plen], payload)
	crc := crc32.Checksum(buf[:8+plen], crc32c)
	binary.LittleEndian.PutUint32(buf[8+plen:], crc)

	if _, err := w.bw.Write(buf); err != nil {
		w.err = fmt.Errorf("wal: append: %w", err)
		return w.err
	}
	w.dirtySinceSync = true
	w.segHasData = true
	w.lsn++

	if w.syncPerWrite {
		return w.flushAndSyncLocked()
	}
	return nil
}

// flushAndSyncLocked flushes the bufio buffer to the OS then fsyncs. Caller
// holds w.mu. Any failure sets the sticky error and returns it. Both steps
// happen under the one lock so no other writer can interleave between them.
func (w *wal) flushAndSyncLocked() error {
	if w.err != nil {
		return w.err
	}
	if err := w.bw.Flush(); err != nil {
		w.err = fmt.Errorf("wal: flush: %w", err)
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: sync: %w", err)
		return w.err
	}
	w.dirtySinceSync = false
	return nil
}

// flush forces the bufio buffer to the OS (write syscall); it does not fsync.
// Used by FlushWAL() and as a durability boundary in tests.
func (w *wal) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	if w.bw == nil { // segmented WAL before its active segment is open
		return nil
	}
	return w.bw.Flush()
}

// close stops the ticker, flushes, and closes the file. Idempotent (safe to
// call more than once) and safe on a nil wal. The ticker is stopped (and
// joined) BEFORE taking w.mu so it cannot be blocked on the lock while close
// waits for it.
func (w *wal) close() error {
	if w == nil {
		return nil
	}
	w.closeOnce.Do(func() {
		// Stop both background goroutines (flush ticker + rotate ticker) and
		// join them BEFORE taking w.mu, so neither can be blocked on the lock
		// while close waits for it.
		if w.stop != nil {
			close(w.stop)
		}
		if w.rotateStop != nil {
			close(w.rotateStop)
		}
		if w.stop != nil || w.rotateStop != nil {
			w.wg.Wait()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.f == nil {
			return
		}
		if w.bw != nil && w.err == nil {
			if err := w.bw.Flush(); err != nil {
				w.f.Close()
				w.closeErr = err
				return
			}
		}
		w.closeErr = w.f.Close()
		// Segmented mode: drop an empty trailing active segment so idle
		// open/close cycles don't accrete zero-record files.
		if w.dir != "" && !w.segHasData && w.closeErr == nil {
			_ = os.Remove(w.segPath(w.seg))
		}
	})
	return w.closeErr
}

// replay reads the WAL from the start to EOF, handing each record's (type,
// payload) to apply. The caller dispatches by type (mutation vs catalog).
// Single-file mode only; segmented mode replays via replayAll.
func (w *wal) replay(apply func(recType uint8, payload []byte) error) error {
	return w.replayFile(w.f, apply)
}

// replayFile replays one open WAL file into apply, counting each record in the
// WAL's lsn (the recovery path). Reads via scanRecords.
func (w *wal) replayFile(f *os.File, apply func(recType uint8, payload []byte) error) error {
	return scanRecords(f, func(recType uint8, payload []byte) error {
		w.lsn++
		return apply(recType, payload)
	})
}

// scanRecords reads f from the start to EOF, handing each complete record's
// (type, payload) to apply. Pure — it touches no WAL state, so a drainer can
// scan a sealed segment without disturbing the live WAL's counters.
//
// Tail tolerance: a truncated final record (short header/payload read, or a
// declared length past EOF) is the incomplete tail of an interrupted write
// and is discarded. A wrong magic, a version other than this binary's, or a CRC
// mismatch on a fully-present record is hard corruption and aborts the caller.
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
	var pos int64
	var hdr [8]byte
	var buf []byte // grown once, reused per record — decoders copy what they keep
	for {
		_, err := io.ReadFull(f, hdr[:])
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return nil // partial header at tail — tolerated
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
			return fmt.Errorf("%w: bad magic %#04x at offset %d", ErrWALCorrupt, magic, pos-8)
		}
		if version != walVersion {
			return fmt.Errorf("%w: record version %d != supported %d — shut the old binary down cleanly (which drains the WAL) before upgrading", ErrWALCorrupt, version, walVersion)
		}
		// Bounds-check before allocating: a torn/corrupt tail can carry a
		// bogus huge length. If payload+crc can't fit in what remains, it is
		// the truncated tail — stop.
		if int64(length)+4 > fileSize-pos {
			return nil
		}
		need := int(length) + 4
		if cap(buf) < need {
			buf = make([]byte, need)
		} else {
			buf = buf[:need]
		}
		if _, err := io.ReadFull(f, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil // truncated payload at tail — tolerated
			}
			return err
		}
		pos += int64(length) + 4
		payload := buf[:length]
		crc := binary.LittleEndian.Uint32(buf[length:])
		want := crc32.Update(crc32.Update(0, crc32c, hdr[:]), crc32c, payload)
		if want != crc {
			return fmt.Errorf("%w: crc mismatch at offset %d", ErrWALCorrupt, pos-int64(length)-4-8)
		}
		if err := apply(recType, payload); err != nil {
			return err
		}
	}
}

// truncate empties the WAL file. Used by tests; production code uses
// rotate-and-checkpoint instead, which lands in M5.
func (w *wal) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekStart)
	w.bw.Reset(w.f)
	w.lsn = 0
	return err
}
