package hazedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
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
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &wal{
		f:       f,
		bw:      bufio.NewWriterSize(f, 64<<10),
		scratch: make([]byte, 0, 4096),
	}, nil
}

// writeRecord serialises one op + payload using the framing above.
// Holds w.mu so the bufio.Writer sees one record at a time and the
// scratch buffer is reusable across calls.
func (w *wal) writeRecord(op uint8, tableID uint16, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lsn++

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

	_, err := w.bw.Write(buf)
	return err
}

func (w *wal) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bw.Flush()
}

func (w *wal) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.bw != nil {
		if err := w.bw.Flush(); err != nil {
			w.f.Close()
			return err
		}
	}
	return w.f.Close()
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
		_ = n
		totalLen := binary.LittleEndian.Uint32(u32[:])
		buf := make([]byte, totalLen)
		if _, err := io.ReadFull(w.f, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				// Truncated record body — tail loss tolerated.
				return nil
			}
			return err
		}
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
	case KindBool:
		if off+1 > len(b) {
			return Value{}, fmt.Errorf("%w: bool pk truncated", ErrWALCorrupt)
		}
		return Value{Kind: KindBool, I: int64(b[off])}, nil
	}
	return Value{}, fmt.Errorf("%w: unknown pk kind %d", ErrWALCorrupt, kind)
}
