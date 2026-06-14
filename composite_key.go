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
	k, _ := encodeCompositeKeyInto(nil, vals)
	return k
}

// encodeCompositeKeyInto encodes vals into buf[:0] and returns the key plus the
// (grown) buffer to reuse on the next call. A merge encoding many keys then
// allocates only the per-key string (string(buf)), not a fresh scratch buffer
// per row.
func encodeCompositeKeyInto(buf []byte, vals []Value) (indexKey, []byte) {
	buf = buf[:0]
	for i := range vals {
		buf = appendOrderedColumn(buf, vals[i])
	}
	return indexKey{kind: KindBytes, s: string(buf)}, buf
}

// appendOrderedColumn appends v's order-preserving encoding to buf. KindNull is a
// caller bug (precondition) and contributes nothing.
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

// appendEscapedTerminated appends s as a self-delimiting, order-preserving field,
// so one column's bytes can never bleed into the next.
func appendEscapedTerminated(buf []byte, s string) []byte {
	// Chunked copy: bulk-append the spans between NUL bytes (rare in real keys),
	// escaping only at each NUL — the common no-NUL string is one append, not a
	// byte-at-a-time loop. Same shape as appendJSONString.
	last := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0x00 {
			buf = append(buf, s[last:i]...)
			buf = append(buf, 0x00, 0xFF)
			last = i + 1
		}
	}
	buf = append(buf, s[last:]...)
	return append(buf, 0x00, 0x00)
}
