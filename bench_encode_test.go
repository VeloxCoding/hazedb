package hazedb

// bench_encode_test.go — Part 2: how best to hand result rows back to a
// non-Go caller (PHP). Compares result-set encoders on CPU / bytes / allocs,
// holding the data constant (cols + []Row from a real query):
//
//   - rowsToJSONStd       : the prior path — [][]any boxing + stdlib json.Marshal
//   - RowsToJSON          : production hand-rolled JSON (wire.go), single buffer
//   - appendJSONRows+reuse: same encoder writing into a reused scratch buffer
//   - appendMsgpackRows   : hand-rolled MessagePack straight from Values
//
// TestEncodeParity pins the production encoder to the stdlib one's output.
// Run: go test -run x -bench Encode -benchmem -count=2

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"
)

const hexDigits = "0123456789abcdef"

// shiftUUID drops the leading byte (msgpack UUID-string helper).
func shiftUUID(u UUID) UUID {
	copy(u[0:], u[1:])
	return u
}

// --- stdlib reference (the path RowsToJSON replaced) ---

func valueToAnyStd(v Value) any {
	switch v.Kind {
	case KindNull:
		return nil
	case KindInt:
		return v.Int()
	case KindBool:
		return v.Bool()
	case KindString:
		return v.Str()
	case KindBytes:
		return base64.StdEncoding.EncodeToString(v.Bytes())
	case KindUUID:
		return v.UUID().String()
	}
	return nil
}

func rowsToJSONStd(cols []string, rows []Row) ([]byte, error) {
	env := struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	}{Columns: cols, Rows: make([][]any, len(rows))}
	for i, r := range rows {
		cells := make([]any, len(r))
		for j := range r {
			cells[j] = valueToAnyStd(r[j])
		}
		env.Rows[i] = cells
	}
	return json.Marshal(env)
}

// --- hand-rolled MessagePack (comparison only; not promoted to production) ---

func mpStr(b []byte, s string) []byte {
	n := len(s)
	switch {
	case n < 32:
		b = append(b, 0xa0|byte(n))
	case n < 256:
		b = append(b, 0xd9, byte(n))
	default:
		b = append(b, 0xda, byte(n>>8), byte(n))
	}
	return append(b, s...)
}

func mpArrayHeader(b []byte, n int) []byte {
	switch {
	case n < 16:
		return append(b, 0x90|byte(n))
	case n < 65536:
		return append(b, 0xdc, byte(n>>8), byte(n))
	default:
		return append(b, 0xdd, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
}

func mpInt(b []byte, n int64) []byte {
	if n >= 0 && n < 128 {
		return append(b, byte(n))
	}
	u := uint64(n)
	return append(b, 0xd3, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func appendUUIDStr(b []byte, u UUID) []byte {
	for i, n := range [5]int{4, 2, 2, 2, 6} {
		if i > 0 {
			b = append(b, '-')
		}
		for ; n > 0; n-- {
			c := u[0]
			u = shiftUUID(u)
			b = append(b, hexDigits[c>>4], hexDigits[c&0xf])
		}
	}
	return b
}

func appendValueMsgpack(b []byte, v Value) []byte {
	switch v.Kind {
	case KindNull:
		return append(b, 0xc0)
	case KindInt:
		return mpInt(b, v.Int())
	case KindBool:
		if v.Bool() {
			return append(b, 0xc3)
		}
		return append(b, 0xc2)
	case KindString:
		return mpStr(b, v.Str())
	case KindBytes:
		raw := v.Bytes()
		if len(raw) < 256 {
			b = append(b, 0xc4, byte(len(raw)))
		} else {
			b = append(b, 0xc5, byte(len(raw)>>8), byte(len(raw)))
		}
		return append(b, raw...)
	case KindUUID:
		var tmp [36]byte
		return mpStr(b, string(appendUUIDStr(tmp[:0], v.UUID())))
	}
	return append(b, 0xc0)
}

func appendMsgpackRows(b []byte, cols []string, rows []Row) []byte {
	b = append(b, 0x82)
	b = mpStr(b, "columns")
	b = mpArrayHeader(b, len(cols))
	for _, c := range cols {
		b = mpStr(b, c)
	}
	b = mpStr(b, "rows")
	b = mpArrayHeader(b, len(rows))
	for i := range rows {
		b = mpArrayHeader(b, len(rows[i]))
		for _, cell := range rows[i] {
			b = appendValueMsgpack(b, cell)
		}
	}
	return b
}

// TestEncodeParity asserts the production hand-rolled RowsToJSON decodes to
// exactly what the stdlib reference produces — a faster encoder must not ship
// different bytes. Covers every value kind, escapes, and empty/negative cases.
func TestEncodeParity(t *testing.T) {
	cols := []string{"u", "i", "s", "b", "n", "by"}
	rows := []Row{
		{UUIDVal(tid(7)), Int(-42), Str(`he"llo` + "\n\tx"), Bool(true), Null(), Bytes([]byte{1, 2, 3})},
		{UUIDVal(tid(8)), Int(127), Str(""), Bool(false), Null(), Bytes(nil)},
	}
	want, err := rowsToJSONStd(cols, rows)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := RowsToJSON(cols, rows)

	var wj, gj any
	if err := json.Unmarshal(want, &wj); err != nil {
		t.Fatalf("stdlib output invalid: %v", err)
	}
	if err := json.Unmarshal(got, &gj); err != nil {
		t.Fatalf("production output invalid JSON: %v\n%s", err, got)
	}
	if !reflect.DeepEqual(wj, gj) {
		t.Fatalf("parity mismatch:\n stdlib: %s\n  prod: %s", want, got)
	}
}

// --- benchmarks ---

func benchEncode(b *testing.B, n int, fn func([]byte, []string, []Row) []byte, reuse bool) {
	cols, rows := benchRows(b, n)
	var buf []byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if reuse {
			buf = fn(buf[:0], cols, rows)
		} else {
			buf = fn(nil, cols, rows)
		}
	}
	_ = buf
}

func benchStd(b *testing.B, n int) {
	cols, rows := benchRows(b, n)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := rowsToJSONStd(cols, rows); err != nil {
			b.Fatal(err)
		}
	}
}

func benchProd(b *testing.B, n int) {
	cols, rows := benchRows(b, n)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		RowsToJSON(cols, rows)
	}
}

func BenchmarkEncode_StdJSON_1(b *testing.B)   { benchStd(b, 1) }
func BenchmarkEncode_StdJSON_10(b *testing.B)  { benchStd(b, 10) }
func BenchmarkEncode_StdJSON_100(b *testing.B) { benchStd(b, 100) }

func BenchmarkEncode_ProdJSON_1(b *testing.B)   { benchProd(b, 1) }
func BenchmarkEncode_ProdJSON_10(b *testing.B)  { benchProd(b, 10) }
func BenchmarkEncode_ProdJSON_100(b *testing.B) { benchProd(b, 100) }

func BenchmarkEncode_HandJSONReuse_1(b *testing.B)   { benchEncode(b, 1, appendJSONRows, true) }
func BenchmarkEncode_HandJSONReuse_10(b *testing.B)  { benchEncode(b, 10, appendJSONRows, true) }
func BenchmarkEncode_HandJSONReuse_100(b *testing.B) { benchEncode(b, 100, appendJSONRows, true) }

func BenchmarkEncode_Msgpack_1(b *testing.B)        { benchEncode(b, 1, appendMsgpackRows, false) }
func BenchmarkEncode_Msgpack_100(b *testing.B)      { benchEncode(b, 100, appendMsgpackRows, false) }
func BenchmarkEncode_MsgpackReuse_1(b *testing.B)   { benchEncode(b, 1, appendMsgpackRows, true) }
func BenchmarkEncode_MsgpackReuse_100(b *testing.B) { benchEncode(b, 100, appendMsgpackRows, true) }
