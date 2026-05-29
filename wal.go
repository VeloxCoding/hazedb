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
//	type:1      (recMutation; recTxn/recCheckpoint reserved for M5/M7)
//	version:1   (walVersion) — replay aborts on a newer version
//	length:4    (payload length, little-endian)
//	payload     (length bytes)
//	crc32c:4    (Castagnoli, over magic|type|version|length|payload)
//
// MUTATION payload — logical typed-mutation, not a physical row image:
//
//	op:1 | tableID:2 | op-body
//	  opInsert: full row     (numCols:2 + typed cells)
//	  opUpdate: pk-cell | nsets:1 | (col_ordinal:2 | typed cell) × nsets
//	  opDelete: pk-cell
//
// UPDATE carries only the changed columns (the spike's measured win); replay
// re-applies every mutation through the store's apply path.
const (
	walMagic   uint16 = 0x485A // "HZ"
	walVersion uint8  = 1

	recMutation   uint8 = 1
	recTxn        uint8 = 2 // reserved — M5
	recCheckpoint uint8 = 3 // reserved — M7

	opInsert uint8 = 1
	opUpdate uint8 = 2
	opDelete uint8 = 3
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

	// Background flush/sync ticker. stop is closed by close(); the loop
	// signals done via wg. Both nil/zero until startTicker runs.
	stop chan struct{}
	wg   sync.WaitGroup

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
	w.lsn++

	if w.syncPerWrite {
		return w.flushAndSyncLocked()
	}
	return nil
}

// --- MUTATION payload encoders (op:1 | tableID:2 | op-body) ---

func encodeInsertMutation(buf []byte, tableID uint16, row Row) []byte {
	buf = append(buf, opInsert)
	buf = appendU16LE(buf, tableID)
	return encodeRow(buf, row)
}

// encodeUpdateMutation writes pk + only the changed columns (ordinal+value
// pairs), reading the new values from row[ord]. row is the full post-update
// row; only the pk cell and the ords cells are journaled.
func encodeUpdateMutation(buf []byte, tableID uint16, pk Value, ords []int, row Row) []byte {
	buf = append(buf, opUpdate)
	buf = appendU16LE(buf, tableID)
	buf = encodeCell(buf, pk)
	buf = append(buf, byte(len(ords)))
	for _, ord := range ords {
		buf = appendU16LE(buf, uint16(ord))
		buf = encodeCell(buf, row[ord])
	}
	return buf
}

func encodeDeleteMutation(buf []byte, tableID uint16, pk Value) []byte {
	buf = append(buf, opDelete)
	buf = appendU16LE(buf, tableID)
	return encodeCell(buf, pk)
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
		if w.stop != nil {
			close(w.stop)
			w.wg.Wait()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.bw != nil && w.err == nil {
			if err := w.bw.Flush(); err != nil {
				w.f.Close()
				w.closeErr = err
				return
			}
		}
		w.closeErr = w.f.Close()
	})
	return w.closeErr
}

// replayRecord is one decoded MUTATION handed to the apply callback. Body is
// the op-body (the bytes after op+tableID) and aliases the read buffer; the
// decoders copy strings/bytes so a decoded Row is safe to retain.
type replayRecord struct {
	Op      uint8
	TableID uint16
	Body    []byte
}

// replay reads the WAL from the start to EOF, applying each MUTATION.
//
// Tail tolerance: a truncated final record (short header/payload read, or a
// declared length past EOF) is the incomplete tail of an interrupted write
// and is discarded. Everything else is hard corruption and aborts Open: a
// wrong magic, a version newer than this binary, an unknown record type, or
// a CRC mismatch on a fully-present record. CHECKPOINT records are recognised
// and skipped (their effect is captured by loading the snapshot); TXN is
// reserved and not yet emitted.
func (w *wal) replay(apply func(replayRecord) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	fi, err := w.f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	var pos int64
	var hdr [8]byte
	for {
		_, err := io.ReadFull(w.f, hdr[:])
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
		if version > walVersion {
			return fmt.Errorf("%w: record version %d newer than supported %d", ErrWALCorrupt, version, walVersion)
		}
		// Bounds-check before allocating: a torn/corrupt tail can carry a
		// bogus huge length. If payload+crc can't fit in what remains, it is
		// the truncated tail — stop.
		if int64(length)+4 > fileSize-pos {
			return nil
		}
		buf := make([]byte, length+4)
		if _, err := io.ReadFull(w.f, buf); err != nil {
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
		w.lsn++

		switch recType {
		case recMutation:
			if len(payload) < 3 {
				return fmt.Errorf("%w: short mutation payload", ErrWALCorrupt)
			}
			rec := replayRecord{
				Op:      payload[0],
				TableID: binary.LittleEndian.Uint16(payload[1:3]),
				Body:    payload[3:],
			}
			if err := apply(rec); err != nil {
				return err
			}
		case recCheckpoint:
			// Recognised, no row state — skip.
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrWALCorrupt, recType)
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

// Cell + row encoding for WAL payloads. A cell is a kind byte + a
// kind-dependent payload; a row is a uint16 count followed by that many cells.
//
//	per cell: kind uint8 + payload
//	  KindNull   : nothing
//	  KindInt    : int64 little-endian
//	  KindString : len uint32 + bytes
//	  KindBytes  : len uint32 + bytes
//	  KindUUID   : 16 raw bytes
//	  KindBool   : uint8

func appendU32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func encodeCell(buf []byte, v Value) []byte {
	buf = append(buf, byte(v.Kind))
	switch v.Kind {
	case KindInt:
		var u [8]byte
		binary.LittleEndian.PutUint64(u[:], uint64(v.I))
		buf = append(buf, u[:]...)
	case KindString:
		buf = appendU32LE(buf, uint32(len(v.S)))
		buf = append(buf, v.S...)
	case KindBytes:
		buf = appendU32LE(buf, uint32(len(v.B)))
		buf = append(buf, v.B...)
	case KindUUID:
		buf = append(buf, v.U[:]...)
	case KindBool:
		buf = append(buf, byte(v.I))
	case KindNull:
		// kind byte only
	}
	return buf
}

// decodeCell decodes one cell and returns the bytes consumed. Every length is
// bounds-checked against b before it is trusted.
func decodeCell(b []byte) (Value, int, error) {
	if len(b) < 1 {
		return Value{}, 0, fmt.Errorf("%w: cell kind truncated", ErrWALCorrupt)
	}
	kind := ValueKind(b[0])
	off := 1
	switch kind {
	case KindNull:
		return Value{Kind: KindNull}, off, nil
	case KindInt:
		if off+8 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: int cell truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindInt, I: int64(binary.LittleEndian.Uint64(b[off : off+8]))}, off + 8, nil
	case KindString:
		if off+4 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: string len truncated", ErrWALCorrupt)
		}
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		if ln < 0 || off+ln > len(b) {
			return Value{}, 0, fmt.Errorf("%w: string body truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindString, S: string(b[off : off+ln])}, off + ln, nil
	case KindBytes:
		if off+4 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bytes len truncated", ErrWALCorrupt)
		}
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		if ln < 0 || off+ln > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bytes body truncated", ErrWALCorrupt)
		}
		cp := make([]byte, ln)
		copy(cp, b[off:off+ln])
		return Value{Kind: KindBytes, B: cp}, off + ln, nil
	case KindUUID:
		if off+16 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: uuid cell truncated", ErrWALCorrupt)
		}
		var u UUID
		copy(u[:], b[off:off+16])
		return Value{Kind: KindUUID, U: u}, off + 16, nil
	case KindBool:
		if off+1 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bool cell truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindBool, I: int64(b[off])}, off + 1, nil
	}
	return Value{}, 0, fmt.Errorf("%w: unknown cell kind %d", ErrWALCorrupt, kind)
}

func encodeRow(buf []byte, row Row) []byte {
	buf = appendU16LE(buf, uint16(len(row)))
	for _, v := range row {
		buf = encodeCell(buf, v)
	}
	return buf
}

func decodeRow(b []byte) (Row, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("%w: row header truncated", ErrWALCorrupt)
	}
	n := int(binary.LittleEndian.Uint16(b[0:2]))
	off := 2
	row := make(Row, n)
	for i := 0; i < n; i++ {
		v, sz, err := decodeCell(b[off:])
		if err != nil {
			return nil, err
		}
		row[i] = v
		off += sz
	}
	return row, nil
}
