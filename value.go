// Package hazedb is a memory-resident SQL store for embedded Go applications
// with single-process deployment and latency-sensitive reads.
package hazedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"unsafe"
)

// ValueKind discriminates the union below.
type ValueKind uint8

const (
	KindNull ValueKind = iota
	KindInt
	KindString
	KindBytes
	KindBool
	KindUUID
)

// Value is the runtime cell type: a packed 32-byte tagged union (versus the
// 72 bytes a struct with separate int/string/bytes/uuid fields would take).
// Kind is the public tag; the payload is overlapped across two words and a
// pointer, since a cell is only ever one kind at a time:
//
//   - KindInt / KindBool : the value lives in w0
//   - KindUUID           : the 16 bytes live inline in (w0, w1), big-endian so
//     comparing the words equals byte-lexicographic order
//   - KindString/KindBytes: ptr is the backing-data pointer, w0 the length
//   - KindNull           : all zero
//
// ptr is ALWAYS nil or a real Go pointer (a string/[]byte backing) — never a
// reinterpreted non-pointer — so the garbage collector scans it correctly and
// keeps the backing alive. Read payloads through the typed accessors below;
// never touch the private fields.
type Value struct {
	Kind ValueKind
	w0   uint64
	w1   uint64
	ptr  unsafe.Pointer
}

func Int(v int64) Value { return Value{Kind: KindInt, w0: uint64(v)} }
func Bool(v bool) Value { return Value{Kind: KindBool, w0: boolToWord(v)} }

func Str(v string) Value {
	if len(v) == 0 {
		return Value{Kind: KindString}
	}
	return Value{Kind: KindString, w0: uint64(len(v)), ptr: unsafe.Pointer(unsafe.StringData(v))}
}

func Bytes(v []byte) Value {
	if len(v) == 0 {
		return Value{Kind: KindBytes}
	}
	return Value{Kind: KindBytes, w0: uint64(len(v)), ptr: unsafe.Pointer(&v[0])}
}

func UUIDVal(u UUID) Value {
	return Value{
		Kind: KindUUID,
		w0:   binary.BigEndian.Uint64(u[0:8]),
		w1:   binary.BigEndian.Uint64(u[8:16]),
	}
}

func Null() Value { return Value{Kind: KindNull} }

func boolToWord(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// Typed accessors — read a cell's payload for a known Kind (checked first, or
// known from the schema). These are the stable read API; the field layout is
// private/packed, so always read through these.
//
//   - Int   — KindInt value, or a KindBool as 0/1
//   - Bool  — KindBool value
//   - Str   — KindString value (shares the backing; immutable, so safe)
//   - Bytes — KindBytes value (the live backing slice; clone before escaping a lock)
//   - UUID  — KindUUID value

func (v Value) Int() int64 { return int64(v.w0) }
func (v Value) Bool() bool { return v.w0 == 1 }

func (v Value) Str() string {
	if v.ptr == nil {
		return ""
	}
	return unsafe.String((*byte)(v.ptr), int(v.w0))
}

func (v Value) Bytes() []byte {
	if v.ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(v.ptr), int(v.w0))
}

func (v Value) UUID() UUID {
	var u UUID
	binary.BigEndian.PutUint64(u[0:8], v.w0)
	binary.BigEndian.PutUint64(u[8:16], v.w1)
	return u
}

// uuidWords returns the two big-endian words backing a KindUUID value (hi = the
// first 8 bytes, lo = the last 8). The secondary index keys on these directly —
// no [16]byte round-trip, no allocation. Caller must know Kind == KindUUID.
func (v Value) uuidWords() (hi, lo uint64) { return v.w0, v.w1 }

func (v Value) IsNull() bool { return v.Kind == KindNull }

// AsString returns the value formatted as text. Used for PK keys in
// the map index and for display.
func (v Value) AsString() string {
	switch v.Kind {
	case KindNull:
		return ""
	case KindInt:
		return strconv.FormatInt(v.Int(), 10)
	case KindString:
		return v.Str()
	case KindBytes:
		return string(v.Bytes())
	case KindUUID:
		u := v.UUID()
		return u.String()
	case KindBool:
		if v.Bool() {
			return "true"
		}
		return "false"
	}
	return ""
}

// AsInt returns the value as int64, coercing strings via strconv.
// Used for ORDER BY / range comparisons.
func (v Value) AsInt() (int64, error) {
	switch v.Kind {
	case KindInt, KindBool:
		return v.Int(), nil
	case KindString:
		return strconv.ParseInt(v.Str(), 10, 64)
	case KindNull:
		return 0, errors.New("null cannot be coerced to int")
	}
	return 0, fmt.Errorf("value of kind %d cannot be coerced to int", v.Kind)
}

// Compare returns -1/0/1 for v < o, v == o, v > o. Coerces along the
// most-precise route: int-int → numeric, otherwise string-string.
// Null comparisons return 0 with comparable=false to let callers decide.
func (v Value) Compare(o Value) (int, bool) {
	if v.Kind == KindNull || o.Kind == KindNull {
		return 0, false
	}
	if (v.Kind == KindInt && o.Kind == KindInt) || (v.Kind == KindBool && o.Kind == KindBool) {
		a, b := v.Int(), o.Int()
		switch {
		case a < b:
			return -1, true
		case a > b:
			return 1, true
		}
		return 0, true
	}
	if v.Kind == KindUUID && o.Kind == KindUUID {
		// UUID is stored big-endian in (w0, w1), so unsigned word comparison
		// equals byte-lexicographic order (= creation order for UUIDv7). No alloc.
		switch {
		case v.w0 != o.w0:
			if v.w0 < o.w0 {
				return -1, true
			}
			return 1, true
		case v.w1 != o.w1:
			if v.w1 < o.w1 {
				return -1, true
			}
			return 1, true
		}
		return 0, true
	}
	// Fall through to string compare. Lexicographic is correct for strings;
	// for mixed int/string callers should not rely on the result.
	a, b := v.AsString(), o.AsString()
	switch {
	case a < b:
		return -1, true
	case a > b:
		return 1, true
	}
	return 0, true
}

// Equal returns true when v and o represent the same value, including
// null-equals-null (SQL semantics differ — null != null in WHERE — but
// the executor checks IsNull explicitly).
func (v Value) Equal(o Value) bool {
	if v.Kind != o.Kind {
		return false
	}
	switch v.Kind {
	case KindNull:
		return true
	case KindInt, KindBool:
		return v.w0 == o.w0
	case KindString:
		return v.Str() == o.Str()
	case KindBytes:
		return string(v.Bytes()) == string(o.Bytes())
	case KindUUID:
		return v.w0 == o.w0 && v.w1 == o.w1
	}
	return false
}

// Row is a single tuple. Ordinals match the table's column order.
type Row []Value

// Clone returns a deep copy of the row. Needed when the executor
// returns rows that the caller may mutate while the underlying storage
// lock is no longer held.
func (r Row) Clone() Row {
	c := make(Row, len(r))
	copy(c, r)
	for i := range c {
		c[i] = cloneValue(c[i])
	}
	return c
}

// cloneValue returns a Value that aliases nothing mutable: a KindBytes payload
// is deep-copied; strings are immutable and ints/uuids are inline, so they are
// copied by value.
func cloneValue(v Value) Value {
	if v.Kind == KindBytes && v.ptr != nil {
		return Bytes(cloneBytes(v.Bytes()))
	}
	return v
}

// cloneBytes returns a fresh copy of b (nil stays nil).
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func appendRowClone(dst []Value, r Row) []Value {
	for _, v := range r {
		dst = append(dst, cloneValue(v))
	}
	return dst
}

func appendProjectClone(dst []Value, r Row, ords []int) []Value {
	for _, ord := range ords {
		dst = append(dst, cloneValue(r[ord]))
	}
	return dst
}
