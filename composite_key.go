package hazedb

import "encoding/binary"

// Composite index keys — an order-preserving byte encoding of a tuple of indexed
// cells. The whole point: bytewise comparison of two encodings equals
// column-by-column comparison of the tuples, so a composite ORDERED index reuses
// the scalar indexKey machinery untouched. The result is an indexKey whose s
// field carries the encoding and whose less() falls through to the bytewise
// string compare (KindBytes is the default branch) — no change to less, rev,
// sorted, or the ordered walk.
//
// Per-column encoding, each chosen so memcmp == the scalar index's own ordering
// for that kind (see indexKey.less):
//   - int  : sign bit flipped, then 8 bytes big-endian. Flipping bit 63 maps the
//     signed range onto unsigned byte order (negatives sort below
//     non-negatives), matching less's int64 compare.
//   - bool : one byte, 0 or 1.
//   - uuid : 16 bytes big-endian — byte order already equals UUID value order.
//   - string/bytes : content with 0x00 escaped to 0x00 0xFF, then a 0x00 0x00
//     terminator. The terminator (0x00) sorts below any escaped data byte
//     (a real 0x00 becomes 0x00 0xFF, any other byte is itself ≥ 0x01), so
//     "a" < "ab" and one column's bytes can never bleed into the next.
//
// Precondition: every component is non-NULL. A composite index only indexes rows
// where all component columns are non-NULL (mirrors the scalar "NULL is never
// indexed" rule); the caller enforces indexability, so a NULL never reaches here.
func encodeCompositeKey(vals []Value) indexKey {
	var buf []byte
	for i := range vals {
		buf = appendOrderedColumn(buf, vals[i])
	}
	// KindBytes routes less() to the bytewise s compare — exactly what a composite
	// key needs. The component count is fixed per index, so two keys of the same
	// index always have comparable, well-formed encodings.
	return indexKey{kind: KindBytes, s: string(buf)}
}

// appendOrderedColumn appends v's order-preserving encoding to buf. See the file
// header for the per-kind scheme. KindNull is a caller bug (precondition) and
// contributes nothing.
func appendOrderedColumn(buf []byte, v Value) []byte {
	switch v.Kind {
	case KindInt:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v.Int())^(uint64(1)<<63))
		return append(buf, b[:]...)
	case KindBool:
		if v.Bool() {
			return append(buf, 1)
		}
		return append(buf, 0)
	case KindUUID:
		var b [16]byte
		hi, lo := v.uuidWords()
		binary.BigEndian.PutUint64(b[0:8], hi)
		binary.BigEndian.PutUint64(b[8:16], lo)
		return append(buf, b[:]...)
	case KindString:
		return appendEscapedTerminated(buf, v.Str())
	case KindBytes:
		return appendEscapedTerminated(buf, string(v.Bytes()))
	}
	return buf
}

// appendEscapedTerminated appends s with 0x00 escaped to 0x00 0xFF, then a
// 0x00 0x00 terminator, so variable-length fields stay self-delimiting and
// order-preserving across a column boundary.
func appendEscapedTerminated(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		buf = append(buf, c)
		if c == 0x00 {
			buf = append(buf, 0xFF)
		}
	}
	return append(buf, 0x00, 0x00)
}
