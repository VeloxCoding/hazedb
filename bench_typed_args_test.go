package hazedb

// bench_typed_args_test.go — the Go-side ceiling for native-array inserts.
// Decomposes the insert arg cost three ways so we can see what eliminating the
// JSON args round-trip would actually buy, BEFORE building any cgo trampoline:
//
//   Insert_PHPJSONArgs : QueryArgs(jsonStr) + Exec  — today's PHP insert path
//                        (json.Decode -> []any -> toValues -> execInsert)
//   Insert_AnyArgs     : Exec(sql, anyArgs...)      — skip JSON, still []any
//   Insert_TypedValues : ExecValues(sql, Values...) — skip JSON AND boxing
//
// Win the native-array unlocks  = PHPJSONArgs - TypedValues.
// Of that, JSON decode alone     = PHPJSONArgs - AnyArgs.
// Run: go test -run x -bench Insert_ -benchmem -count=3

import (
	"strconv"
	"testing"
)

const typedInsertSQL = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"

// TestExecValuesParity asserts ExecValues writes the exact same row Exec does
// (via the []any path), including a bytes column that must be cloned at the
// write boundary, not aliased.
func TestExecValuesParity(t *testing.T) {
	schema := Schema{Tables: []TableDef{{
		Name: "t",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeUUID, PK: true},
			{Name: "name", Type: TypeString},
			{Name: "age", Type: TypeInt},
			{Name: "blob", Type: TypeBytes},
		},
	}}}
	db, err := Open(Options{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idA, idB := tid(1), tid(2)
	// Bytes arg we mutate AFTER the call — ExecValues must have cloned it.
	raw := []byte{9, 8, 7}
	if _, err := db.ExecValues("INSERT INTO t (id, name, age, blob) VALUES (?, ?, ?, ?)",
		UUIDVal(idA), Str("alice"), Int(30), Bytes(raw)); err != nil {
		t.Fatal(err)
	}
	raw[0] = 0 // must not affect stored row
	if _, err := db.Exec("INSERT INTO t (id, name, age, blob) VALUES (?, ?, ?, ?)",
		idB, "alice", 30, []byte{9, 8, 7}); err != nil {
		t.Fatal(err)
	}

	_, rowsA, err := db.Query("SELECT name, age, blob FROM t WHERE id = ?", idA)
	if err != nil {
		t.Fatal(err)
	}
	_, rowsB, err := db.Query("SELECT name, age, blob FROM t WHERE id = ?", idB)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsA) != 1 || len(rowsB) != 1 {
		t.Fatalf("want 1 row each, got %d / %d", len(rowsA), len(rowsB))
	}
	a, err := RowsToJSON([]string{"name", "age", "blob"}, rowsA)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RowsToJSON([]string{"name", "age", "blob"}, rowsB)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("ExecValues row != Exec row:\n  values: %s\n  any   : %s", a, b)
	}
}

func BenchmarkInsert_PHPJSONArgs(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	jss := make([]string, b.N)
	for i := range jss {
		jss[i] = `["` + tid(i).String() + `","name",` + strconv.Itoa(i%100) + `]`
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		args, err := QueryArgs(jss[i])
		if err != nil {
			b.Fatal(err)
		}
		if _, err := db.Exec(typedInsertSQL, args...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_AnyArgs(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec(typedInsertSQL, tid(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_TypedValues(b *testing.B) {
	db, err := Open(Options{Schema: benchSchema(), SizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecValues(typedInsertSQL, UUIDVal(tid(i)), Str("name"), Int(int64(i%100))); err != nil {
			b.Fatal(err)
		}
	}
}
