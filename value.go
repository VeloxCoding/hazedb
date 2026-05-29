// Package fastsql is a memory-resident SQL store for embedded Go
// applications with single-process deployment and latency-sensitive
// reads. See FASTSQL_v1_RFC.md and FASTSQL_PITCH.md at the repo root.
//
// This is the v1 first-cut: generic Value-based execution.
// Codegen for typed-Go hot paths arrives in M3.
package hazedb

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
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

// Value is the runtime cell type. Generic and slow-ish vs a typed struct;
// appropriate for the interpreter path (codegen replaces []Value with typed
// structs per table on the eventual hot path).
//
//   - KindInt:    I holds the value
//   - KindString: S holds the value
//   - KindBytes:  B holds the value (avoid allocs by reusing slices upstream)
//   - KindBool:   I == 0 false, I == 1 true
//   - KindUUID:   U holds the 16 bytes (no heap alloc; usable as a map key)
//   - KindNull:   all zero
//
// U is a fixed [16]byte rather than living in B so a UUID PK is a comparable,
// allocation-free map key. It costs 16 bytes on every cell; if that proves to
// matter it can be packed into two uint64 — measure first.
type Value struct {
	Kind ValueKind
	I    int64
	S    string
	B    []byte
	U    UUID
}

func Int(v int64) Value       { return Value{Kind: KindInt, I: v} }
func Str(v string) Value      { return Value{Kind: KindString, S: v} }
func Bytes(v []byte) Value    { return Value{Kind: KindBytes, B: v} }
func Bool(v bool) Value       { return Value{Kind: KindBool, I: boolToInt(v)} }
func UUIDVal(u UUID) Value    { return Value{Kind: KindUUID, U: u} }
func Null() Value             { return Value{Kind: KindNull} }
func boolToInt(b bool) int64 { if b { return 1 }; return 0 }

func (v Value) IsNull() bool { return v.Kind == KindNull }

// AsString returns the value formatted as text. Used for PK keys in
// the map index and for display.
func (v Value) AsString() string {
	switch v.Kind {
	case KindNull:
		return ""
	case KindInt:
		return strconv.FormatInt(v.I, 10)
	case KindString:
		return v.S
	case KindBytes:
		return string(v.B)
	case KindUUID:
		return v.U.String()
	case KindBool:
		if v.I == 1 {
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
		return v.I, nil
	case KindString:
		return strconv.ParseInt(v.S, 10, 64)
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
	if v.Kind == KindInt && o.Kind == KindInt {
		switch {
		case v.I < o.I:
			return -1, true
		case v.I > o.I:
			return 1, true
		}
		return 0, true
	}
	if v.Kind == KindBool && o.Kind == KindBool {
		switch {
		case v.I < o.I:
			return -1, true
		case v.I > o.I:
			return 1, true
		}
		return 0, true
	}
	if v.Kind == KindUUID && o.Kind == KindUUID {
		// Byte order == creation-time order for UUIDv7. No alloc.
		return bytes.Compare(v.U[:], o.U[:]), true
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
		return v.I == o.I
	case KindString:
		return v.S == o.S
	case KindBytes:
		return string(v.B) == string(o.B)
	case KindUUID:
		return v.U == o.U
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
	// Strings and ints are value-copied. Bytes need a fresh slice.
	for i := range c {
		if c[i].Kind == KindBytes && c[i].B != nil {
			b := make([]byte, len(c[i].B))
			copy(b, c[i].B)
			c[i].B = b
		}
	}
	return c
}
