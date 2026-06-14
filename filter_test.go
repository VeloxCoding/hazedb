package hazedb

import (
	"strconv"
	"testing"
)

// A compiled filter must return the SAME verdict as evalExpr for every row, for
// each recognized shape; and shapes it does not recognize must report ok=false
// so the caller falls back to evalExpr (correctness never depends on coverage).
func TestCompileFilterParity(t *testing.T) {
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE t (id uuid primary key, code text, age int null, tag text)"); err != nil {
		t.Fatal(err)
	}
	// Varied rows, including NULL age (column omitted) and duplicate codes.
	type seed struct {
		code, tag string
		age       any // int or nil
	}
	rows := []seed{
		{"c1", "x", 30}, {"c2", "y", 40}, {"c1", "z", nil},
		{"c3", "x", 10}, {"c2", "x", 40}, {"c4", "y", nil}, {"c5", "z", 99},
	}
	for _, s := range rows {
		if s.age == nil {
			if _, err := db.Exec("INSERT INTO t (id, code, tag) VALUES (?, ?, ?)", NewUUIDv7(), s.code, s.tag); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := db.Exec("INSERT INTO t (id, code, age, tag) VALUES (?, ?, ?, ?)", NewUUIDv7(), s.code, s.age, s.tag); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Full rows in declared column order (id, code, age, tag = ords 0..3), the
	// layout evalExpr's bound ords index into.
	_, full, err := db.Query("SELECT id, code, age, tag FROM t")
	if err != nil {
		t.Fatal(err)
	}
	tbl := db.cat.Load().byName["t"].table

	cases := []struct {
		sql         string
		args        []Value
		wantCompile bool
	}{
		{"SELECT id FROM t WHERE code = ?", []Value{Str("c1")}, true},
		{"SELECT id FROM t WHERE code != ?", []Value{Str("c1")}, true},
		{"SELECT id FROM t WHERE age = ?", []Value{Int(40)}, true},
		{"SELECT id FROM t WHERE age < ?", []Value{Int(40)}, true},
		{"SELECT id FROM t WHERE age <= ?", []Value{Int(40)}, true},
		{"SELECT id FROM t WHERE age > ?", []Value{Int(10)}, true},
		{"SELECT id FROM t WHERE age >= ?", []Value{Int(40)}, true},
		{"SELECT id FROM t WHERE age IS NULL", nil, true},
		{"SELECT id FROM t WHERE age IS NOT NULL", nil, true},
		{"SELECT id FROM t WHERE code = ?", []Value{Str("c2")}, true}, // param rhs
		{"SELECT id FROM t WHERE code = ? AND age > ?", []Value{Str("c2"), Int(0)}, true},
		{"SELECT id FROM t WHERE code = ? OR tag = ?", []Value{Str("c1"), Str("y")}, true},
		{"SELECT id FROM t WHERE NOT (code = ?)", []Value{Str("c1")}, true},
		{"SELECT id FROM t WHERE age + ? = ?", []Value{Int(1), Int(41)}, false}, // arithmetic lhs → fallback
		{"SELECT id FROM t WHERE age = age", nil, false},                        // col vs col → fallback
	}

	for _, c := range cases {
		pl, err := db.prepare(c.sql, db.cat.Load())
		if err != nil {
			t.Fatalf("prepare %q: %v", c.sql, err)
		}
		where := pl.st.(*selectStmt).where
		fast, ok := compileFilter(where, c.args)
		if ok != c.wantCompile {
			t.Fatalf("%q: compiled=%v, want %v", c.sql, ok, c.wantCompile)
		}
		if !ok {
			continue // fallback path is evalExpr itself — parity is trivial
		}
		ctx := evalCtx{cols: tbl.def.colByName, args: c.args}
		for i, r := range full {
			ctx.row = r
			v, err := evalExpr(where, &ctx)
			if err != nil {
				t.Fatalf("%q row %d: evalExpr err %v", c.sql, i, err)
			}
			if got, want := fast(r), truthy(v); got != want {
				t.Fatalf("%q row %d: compiled=%v evalExpr=%v", c.sql, i, got, want)
			}
		}
	}
}

// End to end: a scan query through db.Query (which now uses the compiled matcher)
// returns the right rows, for a compiled shape and a fallback shape.
func TestScanMatcherEndToEnd(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, code text, age int)")
	for i := 0; i < 20; i++ {
		db.Exec("INSERT INTO t (id, code, age) VALUES (?, ?, ?)", NewUUIDv7(), "c"+strconv.Itoa(i%5), int64(i))
	}
	// compiled: code = ? AND age >= ?
	if _, r, err := db.Query("SELECT id FROM t WHERE code = ? AND age >= ?", "c2", 10); err != nil || len(r) != 2 {
		t.Fatalf("compiled shape: rows=%d err=%v (want 2: age 12,17)", len(r), err)
	}
	// fallback: arithmetic in WHERE
	if _, r, err := db.Query("SELECT id FROM t WHERE age + ? = ?", 0, 7); err != nil || len(r) != 1 {
		t.Fatalf("fallback shape: rows=%d err=%v (want 1)", len(r), err)
	}
}
