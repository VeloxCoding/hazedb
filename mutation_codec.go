package hazedb

import (
	"encoding/binary"
	"fmt"
)

// Mutation wire format — the typed-mutation payload carried inside a WAL record
// envelope (the envelope framing itself lives in wal.go). Encoder and decoder
// sit together here so a format change touches one file; both the in-memory WAL
// replay (db.go applyMutation) and the SQLite drain (drain.go) consume through
// these.
//
//	op:1 | tableID:2 | op-body
//	  opInsert: full row     (numCols:2 + typed cells)
//	  opUpdate: pk-cell | nsets:2 | (col_ordinal:2 | typed cell) × nsets
//	  opDelete: pk-cell
//
// UPDATE carries only the changed columns (the spike's measured win); replay
// re-applies every mutation through the store's apply path.
const (
	opInsert uint8 = 1
	opUpdate uint8 = 2
	opDelete uint8 = 3
)

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
	buf = appendU16LE(buf, uint16(len(ords))) // nsets: uint16, matching INSERT's numCols + the per-cell ordinal (u8 wrapped past 255 SET columns)
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

// decodeUpdateMutation walks an opUpdate op-body — pk-cell | nsets:2 |
// (ordinal:2 | cell) × nsets — invoking set(ord, v) for each changed column,
// and returns the decoded pk cell. The byte framing lives ONLY here: both the
// in-memory replay (applyMutation) and the SQLite drain consume the update
// through this, so a format change (e.g. the nsets width) is made in one place
// and can never diverge RAM from the mirror. set may return an error to abort
// (the drain uses it for an out-of-range ordinal).
func decodeUpdateMutation(body []byte, set func(ord int, v Value) error) (Value, error) {
	pk, n, err := decodeCell(body)
	if err != nil {
		return Value{}, err
	}
	body = body[n:]
	if len(body) < 2 {
		return Value{}, fmt.Errorf("%w: update missing nsets", ErrWALCorrupt)
	}
	nsets := int(binary.LittleEndian.Uint16(body[0:2]))
	body = body[2:]
	for i := 0; i < nsets; i++ {
		if len(body) < 2 {
			return Value{}, fmt.Errorf("%w: update ordinal truncated", ErrWALCorrupt)
		}
		ord := int(binary.LittleEndian.Uint16(body[0:2]))
		body = body[2:]
		v, m, err := decodeCell(body)
		if err != nil {
			return Value{}, err
		}
		if err := set(ord, v); err != nil {
			return Value{}, err
		}
		body = body[m:]
	}
	if len(body) != 0 {
		return Value{}, fmt.Errorf("%w: update has %d trailing bytes", ErrWALCorrupt, len(body))
	}
	return pk, nil
}

// decodeDeleteBody decodes an opDelete op-body: a single PK cell that must consume
// the whole body. The shared framing check (replay + drain both call it) keeps a
// record with trailing bytes from being silently accepted on either path.
func decodeDeleteBody(body []byte) (Value, error) {
	pk, n, err := decodeCell(body)
	if err != nil {
		return Value{}, err
	}
	if n != len(body) {
		return Value{}, fmt.Errorf("%w: delete has %d trailing bytes", ErrWALCorrupt, len(body)-n)
	}
	return pk, nil
}

// encodeTxn frames a transaction as one record payload: a count followed by
// each sub-mutation length-prefixed. Each sub-mutation is the same
// op|tableID|op-body the single-statement encoders above produce, so replay
// reuses applyMutation per element. The whole group is one WAL envelope, so a
// torn write discards the entire transaction (the commit boundary IS the
// envelope boundary) and a complete, CRC-valid record replays all-or-nothing.
//
//	nmut:2 | (mlen:4 | op|tableID|op-body) × nmut
func encodeTxn(buf []byte, muts [][]byte) []byte {
	buf = appendU16LE(buf, uint16(len(muts)))
	for _, m := range muts {
		buf = appendU32LE(buf, uint32(len(m)))
		buf = append(buf, m...)
	}
	return buf
}

// forEachTxnMutation parses a recTxn payload (nmut:2 | (mlen:4 | mutation) × nmut)
// and invokes apply on each sub-mutation body in order. It enforces full
// consumption — a truncated length/body, or trailing bytes after the last
// mutation, is ErrWALCorrupt — so the in-memory replay and the SQLite drain share
// one framing check and can never disagree about what a transaction contains.
func forEachTxnMutation(payload []byte, apply func(mut []byte) error) error {
	if len(payload) < 2 {
		return fmt.Errorf("%w: short txn payload", ErrWALCorrupt)
	}
	nmut := int(binary.LittleEndian.Uint16(payload[0:2]))
	off := 2
	for i := 0; i < nmut; i++ {
		if off+4 > len(payload) {
			return fmt.Errorf("%w: txn sub-mutation length truncated", ErrWALCorrupt)
		}
		mlen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if mlen < 0 || off+mlen > len(payload) {
			return fmt.Errorf("%w: txn sub-mutation body truncated", ErrWALCorrupt)
		}
		if err := apply(payload[off : off+mlen]); err != nil {
			return err
		}
		off += mlen
	}
	if off != len(payload) {
		return fmt.Errorf("%w: txn has %d trailing bytes", ErrWALCorrupt, len(payload)-off)
	}
	return nil
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
		binary.LittleEndian.PutUint64(u[:], uint64(v.Int()))
		buf = append(buf, u[:]...)
	case KindString:
		buf = appendU32LE(buf, uint32(len(v.Str())))
		buf = append(buf, v.Str()...)
	case KindBytes:
		b := v.Bytes()
		buf = appendU32LE(buf, uint32(len(b)))
		buf = append(buf, b...)
	case KindUUID:
		u := v.UUID()
		buf = append(buf, u[:]...)
	case KindBool:
		buf = append(buf, byte(v.Int()))
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
		return Null(), off, nil
	case KindInt:
		if off+8 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: int cell truncated", ErrWALCorrupt)
		}
		return Int(int64(binary.LittleEndian.Uint64(b[off : off+8]))), off + 8, nil
	case KindString:
		if off+4 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: string len truncated", ErrWALCorrupt)
		}
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		// ln<0 catches a >2 GiB length wrapping negative on 32-bit; dead on 64-bit.
		if ln < 0 || off+ln > len(b) {
			return Value{}, 0, fmt.Errorf("%w: string body truncated", ErrWALCorrupt)
		}
		return Str(string(b[off : off+ln])), off + ln, nil
	case KindBytes:
		if off+4 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bytes len truncated", ErrWALCorrupt)
		}
		ln := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		// ln<0 catches a >2 GiB length wrapping negative on 32-bit; dead on 64-bit.
		if ln < 0 || off+ln > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bytes body truncated", ErrWALCorrupt)
		}
		cp := make([]byte, ln)
		copy(cp, b[off:off+ln])
		return Bytes(cp), off + ln, nil
	case KindUUID:
		if off+16 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: uuid cell truncated", ErrWALCorrupt)
		}
		var u UUID
		copy(u[:], b[off:off+16])
		return UUIDVal(u), off + 16, nil
	case KindBool:
		if off+1 > len(b) {
			return Value{}, 0, fmt.Errorf("%w: bool cell truncated", ErrWALCorrupt)
		}
		return Bool(b[off] == 1), off + 1, nil
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
	if off != len(b) {
		return nil, fmt.Errorf("%w: insert row has %d trailing bytes", ErrWALCorrupt, len(b)-off)
	}
	return row, nil
}
