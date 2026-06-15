package hazedb

// Correctness oracle for the hand-rolled result encoder in wire.go: the
// production RowsToJSON must decode to exactly what a stdlib json.Marshal of the
// same rows produces. A faster encoder may not ship different bytes. (The encoder
// comparison benchmarks that once lived alongside this test were retired once the
// hand-rolled path won and shipped.)

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"
)

// valueToAnyStd maps a Value to the any the stdlib encoder expects, mirroring
// RowsToJSON's per-kind rules (bytes → base64, uuid → canonical string).
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

// rowsToJSONStd is the stdlib reference: [][]any boxing + json.Marshal, the path
// RowsToJSON replaced. Kept only as the parity oracle below.
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

// TestEncodeParity asserts the production hand-rolled RowsToJSON decodes to
// exactly what the stdlib reference produces. Covers every value kind, escapes,
// and empty/negative cases.
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

// jsonEqual asserts want and got decode to the same JSON value (key order and
// formatting irrelevant).
func jsonEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var wj, gj any
	if err := json.Unmarshal(want, &wj); err != nil {
		t.Fatalf("reference output invalid JSON: %v\n%s", err, want)
	}
	if err := json.Unmarshal(got, &gj); err != nil {
		t.Fatalf("production output invalid JSON: %v\n%s", err, got)
	}
	if !reflect.DeepEqual(wj, gj) {
		t.Fatalf("parity mismatch:\n reference: %s\n      prod: %s", want, got)
	}
}

// rowObjStd is the stdlib reference for one {"col":val,...} object.
func rowObjStd(cols []string, r Row) map[string]any {
	m := make(map[string]any, len(r))
	for j := range r {
		key := ""
		if j < len(cols) {
			key = cols[j]
		}
		m[key] = valueToAnyStd(r[j])
	}
	return m
}

// TestRowToJSONObjectParity: the single-object encoder matches a stdlib Marshal
// of the same row as a map.
func TestRowToJSONObjectParity(t *testing.T) {
	cols := []string{"u", "i", "s", "b", "n", "by"}
	row := Row{UUIDVal(tid(7)), Int(-42), Str(`he"llo` + "\n\tx"), Bool(true), Null(), Bytes([]byte{1, 2, 3})}
	want, err := json.Marshal(rowObjStd(cols, row))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := RowToJSONObject(cols, row)
	jsonEqual(t, want, got)
}

// TestRowsToJSONObjectsParity: the array-of-objects encoder matches a stdlib
// Marshal of the same rows as []map.
func TestRowsToJSONObjectsParity(t *testing.T) {
	cols := []string{"u", "i", "s"}
	rows := []Row{
		{UUIDVal(tid(7)), Int(1), Str("a")},
		{UUIDVal(tid(8)), Int(2), Str(`x"y`)},
	}
	ref := make([]map[string]any, len(rows))
	for i, r := range rows {
		ref[i] = rowObjStd(cols, r)
	}
	want, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := RowsToJSONObjects(cols, rows)
	jsonEqual(t, want, got)
}

// TestExecResultJSONShape pins the fixed write-result envelope.
func TestExecResultJSONShape(t *testing.T) {
	if got := string(ExecResultJSON(7)); got != `{"affected":7}` {
		t.Fatalf("ExecResultJSON(7) = %s", got)
	}
	if got := string(ExecResultJSON(0)); got != `{"affected":0}` {
		t.Fatalf("ExecResultJSON(0) = %s", got)
	}
}

// TestArgsFromJSONRejectsTrailing: Decode reads only the first value, so trailing
// tokens must be rejected, not silently dropped.
func TestArgsFromJSONRejectsTrailing(t *testing.T) {
	for _, in := range []string{"[1] 2", `[1] {"x":1}`, "[1] trailing", "[1][2]"} {
		if _, err := ArgsFromJSON([]byte(in)); err == nil {
			t.Fatalf("expected error for trailing data %q", in)
		}
	}
	// A clean array, even with surrounding whitespace, still parses.
	if args, err := ArgsFromJSON([]byte(" [1, 2] ")); err != nil || len(args) != 2 {
		t.Fatalf("clean array should parse: args=%v err=%v", args, err)
	}
}

// TestQueryArgsPreservesStringSpaces: the direct-string form must pass the value
// verbatim — leading/trailing spaces are part of the value, not noise.
func TestQueryArgsPreservesStringSpaces(t *testing.T) {
	args, err := QueryArgs(" alice ")
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 || args[0] != " alice " {
		t.Fatalf("direct string must be verbatim, got %#v", args)
	}
}

// TestArgsKeepStringsVerbatim pins the boundary contract: the text/JSON arg
// surfaces do NOT guess types by shape — a canonical-UUID-form string stays a
// STRING here. UUID coercion happens downstream, driven by the column type (see
// the param-coercion DB tests for the end-to-end resolution).
func TestArgsKeepStringsVerbatim(t *testing.T) {
	id := tid(42).String()
	qa, err := QueryArgs(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(qa) != 1 || qa[0] != id {
		t.Fatalf("QueryArgs should keep the string verbatim (not a UUID), got %#v", qa)
	}
	ja, err := ArgsFromJSON([]byte(`["` + id + `"]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ja) != 1 || ja[0] != id {
		t.Fatalf("ArgsFromJSON should keep the string verbatim (not a UUID), got %#v", ja)
	}
}
