package hazedb

import (
	"math/rand"
	"testing"
)

// cmpKey returns -1/0/1 for indexKey order via the production less().
func cmpKey(a, b indexKey) int {
	if a.less(b) {
		return -1
	}
	if b.less(a) {
		return 1
	}
	return 0
}

// refTupleCmp is the reference order: column-by-column, using the SAME scalar
// ordering a single-column index would apply (keyOf + less). The encoder must
// agree with this for every pair — that IS the composite-index invariant.
func refTupleCmp(a, b []Value) int {
	for i := range a {
		switch c := cmpKey(keyOf(a[i]), keyOf(b[i])); c {
		case -1, 1:
			return c
		}
	}
	return 0
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// TestCompositeKeyOrdering — randomized property: bytewise order of the encoded
// tuple equals column-by-column scalar order, across mixed-kind schemas.
func TestCompositeKeyOrdering(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	schemas := [][]ValueKind{
		{KindInt, KindString, KindUUID},
		{KindString, KindString}, // adjacent strings: boundary/escape stress
		{KindInt, KindInt},       // signed ordering across two columns
		{KindBool, KindString, KindInt},
		{KindUUID, KindInt},
	}
	for _, schema := range schemas {
		for iter := 0; iter < 4000; iter++ {
			a := randTuple(rng, schema)
			b := randTuple(rng, schema)
			want := refTupleCmp(a, b)
			got := sign(cmpKey(encodeCompositeKey(a), encodeCompositeKey(b)))
			if got != want {
				t.Fatalf("schema %v: tuple order mismatch\n a=%v\n b=%v\n want %d got %d",
					schema, fmtTuple(a), fmtTuple(b), want, got)
			}
		}
	}
}

// TestCompositeKeyEdgeCases — the boundary cases the random pass might rarely
// hit: string prefixes, embedded NUL, negative-vs-positive ints, tie-break into
// a later column, and equal tuples encoding equal.
func TestCompositeKeyEdgeCases(t *testing.T) {
	cases := []struct {
		name string
		a, b []Value
		want int
	}{
		{"prefix", []Value{Str("a")}, []Value{Str("ab")}, -1},
		{"embedded-nul-longer", []Value{Str("a\x00")}, []Value{Str("a")}, 1},
		{"embedded-nul-order", []Value{Str("a\x00")}, []Value{Str("a\x01")}, -1},
		{"neg-vs-pos", []Value{Int(-1)}, []Value{Int(1)}, -1},
		{"neg-extreme", []Value{Int(-9223372036854775808)}, []Value{Int(9223372036854775807)}, -1},
		{"first-equal-second-decides", []Value{Str("x"), Int(1)}, []Value{Str("x"), Int(2)}, -1},
		{"first-decides-ignores-second", []Value{Str("a"), Int(9)}, []Value{Str("b"), Int(0)}, -1},
		{"equal", []Value{Str("k"), Int(7)}, []Value{Str("k"), Int(7)}, 0},
		{"high-byte-vs-empty", []Value{Str("\xff")}, []Value{Str("")}, 1},
	}
	for _, c := range cases {
		got := sign(cmpKey(encodeCompositeKey(c.a), encodeCompositeKey(c.b)))
		if got != c.want {
			t.Errorf("%s: want %d got %d", c.name, c.want, got)
		}
		// Reference must agree too — guards the test's own expectations.
		if ref := refTupleCmp(c.a, c.b); ref != c.want {
			t.Errorf("%s: reference disagrees: want %d got %d", c.name, c.want, ref)
		}
	}
}

func randTuple(rng *rand.Rand, schema []ValueKind) []Value {
	out := make([]Value, len(schema))
	for i, k := range schema {
		out[i] = randValue(rng, k)
	}
	return out
}

func randValue(rng *rand.Rand, k ValueKind) Value {
	switch k {
	case KindInt:
		return Int(int64(rng.Uint64())) // full signed range, incl. negatives
	case KindBool:
		return Bool(rng.Intn(2) == 1)
	case KindString:
		return Str(randStr(rng))
	case KindUUID:
		var u UUID
		for j := range u {
			u[j] = byte(rng.Intn(256))
		}
		return UUIDVal(u)
	}
	return Null()
}

// randStr draws short strings from a tiny alphabet that includes 0x00 and 0xFF,
// with frequent empties and prefixes — the inputs most likely to break the
// escape/terminator scheme.
func randStr(rng *rand.Rand) string {
	alphabet := []byte{0x00, 0x01, 'a', 'b', 0xFF}
	n := rng.Intn(4) // 0..3, so empties and prefixes recur
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

func fmtTuple(t []Value) []string {
	out := make([]string, len(t))
	for i, v := range t {
		out[i] = v.AsString()
	}
	return out
}
