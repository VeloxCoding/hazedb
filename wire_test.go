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
