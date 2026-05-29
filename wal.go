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

// WAL framing per record:
//
//	totalLen  uint32   (length of the payload-and-crc that follows)
//	opType    uint8    (1=insert 2=update 3=delete)
//	tableID   uint16   (index into Schema.Tables order)
//	bodyLen   uint32   (length of body)
//	body      []byte   (row payload for insert/update; PK for delete)
//	crc32     uint32   (over opType..body)
//
// The framing is identical for all three op types so a single replay
// loop covers them.
const (
	opInsert uint8 = 1
	opUpdate uint8 = 2
	opDelete uint8 = 3
)

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
func (w *wal) writeRecord(op uint8, tableID uint16, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}

	bodyLen := uint32(len(body))
	innerLen := 1 + 2 + 4 + int(bodyLen) // opType + tableID + bodyLen + body
	totalLen := uint32(innerLen + 4)     // + crc

	need := 4 + innerLen + 4 // totalLen prefix + inner + crc
	if cap(w.scratch) < need {
		w.scratch = make([]byte, 0, need*2)
	}
	buf := w.scratch[:need]

	binary.LittleEndian.PutUint32(buf[0:4], totalLen)
	off := 4
	buf[off] = op
	off++
	binary.LittleEndian.PutUint16(buf[off:off+2], tableID)
	off += 2
	binary.LittleEndian.PutUint32(buf[off:off+4], bodyLen)
	off += 4
	copy(buf[off:off+int(bodyLen)], body)
	off += int(bodyLen)
	crc := crc32.ChecksumIEEE(buf[4 : 4+innerLen])
	binary.LittleEndian.PutUint32(buf[off:off+4], crc)

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

// replayRecord is the per-record callback for replay. err on the
// returned closure stops the replay; ok=false signals truncated record
// (tail-recoverable).
type replayRecord struct {
	Op      uint8
	TableID uint16
	Body    []byte // aliases the read buffer; copy if retained
}

// replay reads the WAL from the start to EOF, calling apply on each
// record. Tolerates a single truncated record at the tail (clean
// shutdown left it that way, or process crashed mid-write); rejects
// CRC mismatches on any record because that's actual corruption.
func (w *wal) replay(apply func(replayRecord) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	fi, err := w.f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	var pos int64 // bytes consumed so far
	var u32 [4]byte
	for {
		n, err := io.ReadFull(w.f, u32[:])
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			// Partial length prefix at tail — tolerated.
			return nil
		}
		if err != nil {
			return err
		}
		pos += int64(n)
		totalLen := binary.LittleEndian.Uint32(u32[:])
		// Bounds-check the declared length against the bytes actually left
		// in the file BEFORE allocating. A crash-torn or corrupt final
		// record can carry a bogus huge length; make([]byte, totalLen) on
		// it would over-allocate (OOM). A record that can't fit in the
		// remaining bytes — or is too small to even hold its CRC — is the
		// truncated tail: stop here. CRC sits after the length-driven read,
		// so it cannot guard this.
		if totalLen < 4 || int64(totalLen) > fileSize-pos {
			return nil
		}
		buf := make([]byte, totalLen)
		if _, err := io.ReadFull(w.f, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				// Truncated record body — tail loss tolerated.
				return nil
			}
			return err
		}
		pos += int64(totalLen)
		innerLen := int(totalLen) - 4
		innerBuf := buf[:innerLen]
		crc := binary.LittleEndian.Uint32(buf[innerLen:])
		if crc32.ChecksumIEEE(innerBuf) != crc {
			return fmt.Errorf("%w: crc mismatch at lsn ~%d", ErrWALCorrupt, w.lsn+1)
		}
		w.lsn++

		op := innerBuf[0]
		tableID := binary.LittleEndian.Uint16(innerBuf[1:3])
		bodyLen := binary.LittleEndian.Uint32(innerBuf[3:7])
		body := innerBuf[7 : 7+bodyLen]
		if err := apply(replayRecord{Op: op, TableID: tableID, Body: body}); err != nil {
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

// Row encoding for WAL bodies. Fixed-width header per cell + variable
// payload for strings and bytes. Cheap to write, single-pass to read.
//
//	per row: numCols uint16
//	per col: kind uint8 + (kind-dependent payload)
//	  KindNull   : nothing
//	  KindInt    : int64 little-endian
//	  KindString : len uint32 + bytes
//	  KindBytes  : len uint32 + bytes
//	  KindBool   : uint8

func encodeRow(buf []byte, row Row) []byte {
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], uint16(len(row)))
	buf = append(buf, u16[:]...)
	for _, v := range row {
		buf = append(buf, byte(v.Kind))
		switch v.Kind {
		case KindNull:
			// no payload
		case KindInt:
			var u64 [8]byte
			binary.LittleEndian.PutUint64(u64[:], uint64(v.I))
			buf = append(buf, u64[:]...)
		case KindString:
			var u32 [4]byte
			binary.LittleEndian.PutUint32(u32[:], uint32(len(v.S)))
			buf = append(buf, u32[:]...)
			buf = append(buf, v.S...)
		case KindBytes:
			var u32 [4]byte
			binary.LittleEndian.PutUint32(u32[:], uint32(len(v.B)))
			buf = append(buf, u32[:]...)
			buf = append(buf, v.B...)
		case KindUUID:
			buf = append(buf, v.U[:]...)
		case KindBool:
			buf = append(buf, byte(v.I))
		}
	}
	return buf
}

func decodeRow(b []byte) (Row, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("%w: row header truncated", ErrWALCorrupt)
	}
	n := binary.LittleEndian.Uint16(b[0:2])
	off := 2
	row := make(Row, n)
	for i := 0; i < int(n); i++ {
		if off >= len(b) {
			return nil, fmt.Errorf("%w: kind byte truncated at col %d", ErrWALCorrupt, i)
		}
		kind := ValueKind(b[off])
		off++
		switch kind {
		case KindNull:
			row[i] = Value{Kind: KindNull}
		case KindInt:
			if off+8 > len(b) {
				return nil, fmt.Errorf("%w: int payload truncated at col %d", ErrWALCorrupt, i)
			}
			row[i] = Value{Kind: KindInt, I: int64(binary.LittleEndian.Uint64(b[off : off+8]))}
			off += 8
		case KindString:
			if off+4 > len(b) {
				return nil, fmt.Errorf("%w: string length truncated at col %d", ErrWALCorrupt, i)
			}
			ln := binary.LittleEndian.Uint32(b[off : off+4])
			off += 4
			if off+int(ln) > len(b) {
				return nil, fmt.Errorf("%w: string body truncated at col %d", ErrWALCorrupt, i)
			}
			row[i] = Value{Kind: KindString, S: string(b[off : off+int(ln)])}
			off += int(ln)
		case KindBytes:
			if off+4 > len(b) {
				return nil, fmt.Errorf("%w: bytes length truncated at col %d", ErrWALCorrupt, i)
			}
			ln := binary.LittleEndian.Uint32(b[off : off+4])
			off += 4
			if off+int(ln) > len(b) {
				return nil, fmt.Errorf("%w: bytes body truncated at col %d", ErrWALCorrupt, i)
			}
			cp := make([]byte, ln)
			copy(cp, b[off:off+int(ln)])
			row[i] = Value{Kind: KindBytes, B: cp}
			off += int(ln)
		case KindUUID:
			if off+16 > len(b) {
				return nil, fmt.Errorf("%w: uuid payload truncated at col %d", ErrWALCorrupt, i)
			}
			var u UUID
			copy(u[:], b[off:off+16])
			row[i] = Value{Kind: KindUUID, U: u}
			off += 16
		case KindBool:
			if off+1 > len(b) {
				return nil, fmt.Errorf("%w: bool payload truncated at col %d", ErrWALCorrupt, i)
			}
			row[i] = Value{Kind: KindBool, I: int64(b[off])}
			off++
		default:
			return nil, fmt.Errorf("%w: unknown kind %d at col %d", ErrWALCorrupt, kind, i)
		}
	}
	return row, nil
}

// encodePK encodes a single Value (the PK) for delete records. Same
// per-cell framing as encodeRow's inner loop.
func encodePK(buf []byte, v Value) []byte {
	buf = append(buf, byte(v.Kind))
	switch v.Kind {
	case KindInt:
		var u64 [8]byte
		binary.LittleEndian.PutUint64(u64[:], uint64(v.I))
		buf = append(buf, u64[:]...)
	case KindString:
		var u32 [4]byte
		binary.LittleEndian.PutUint32(u32[:], uint32(len(v.S)))
		buf = append(buf, u32[:]...)
		buf = append(buf, v.S...)
	case KindBytes:
		var u32 [4]byte
		binary.LittleEndian.PutUint32(u32[:], uint32(len(v.B)))
		buf = append(buf, u32[:]...)
		buf = append(buf, v.B...)
	case KindUUID:
		buf = append(buf, v.U[:]...)
	case KindBool:
		buf = append(buf, byte(v.I))
	}
	return buf
}

func decodePK(b []byte) (Value, error) {
	if len(b) < 1 {
		return Value{}, fmt.Errorf("%w: pk truncated", ErrWALCorrupt)
	}
	kind := ValueKind(b[0])
	off := 1
	switch kind {
	case KindInt:
		if off+8 > len(b) {
			return Value{}, fmt.Errorf("%w: int pk truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindInt, I: int64(binary.LittleEndian.Uint64(b[off : off+8]))}, nil
	case KindString:
		if off+4 > len(b) {
			return Value{}, fmt.Errorf("%w: string pk length truncated", ErrWALCorrupt)
		}
		ln := binary.LittleEndian.Uint32(b[off : off+4])
		off += 4
		if off+int(ln) > len(b) {
			return Value{}, fmt.Errorf("%w: string pk body truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindString, S: string(b[off : off+int(ln)])}, nil
	case KindBytes:
		if off+4 > len(b) {
			return Value{}, fmt.Errorf("%w: bytes pk length truncated", ErrWALCorrupt)
		}
		ln := binary.LittleEndian.Uint32(b[off : off+4])
		off += 4
		if off+int(ln) > len(b) {
			return Value{}, fmt.Errorf("%w: bytes pk body truncated", ErrWALCorrupt)
		}
		cp := make([]byte, ln)
		copy(cp, b[off:off+int(ln)])
		return Value{Kind: KindBytes, B: cp}, nil
	case KindUUID:
		if off+16 > len(b) {
			return Value{}, fmt.Errorf("%w: uuid pk truncated", ErrWALCorrupt)
		}
		var u UUID
		copy(u[:], b[off:off+16])
		return Value{Kind: KindUUID, U: u}, nil
	case KindBool:
		if off+1 > len(b) {
			return Value{}, fmt.Errorf("%w: bool pk truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindBool, I: int64(b[off])}, nil
	}
	return Value{}, fmt.Errorf("%w: unknown pk kind %d", ErrWALCorrupt, kind)
}
