package hazedb

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// TestMultiRowInsert exercises the multi-VALUES INSERT path: N tuples, global
// param numbering, mixed literals/params, auto-PK per row, and the all-or-
// nothing atomicity of the batch (it commits as one transaction).
func TestMultiRowInsert(t *testing.T) {
	t.Run("explicit_pk_params", func(t *testing.T) {
		db := openMem(t)
		n, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
			tid(1), "a", 1, tid(2), "b", 2, tid(3), "c", 3)
		if err != nil {
			t.Fatalf("multi-insert: %v", err)
		}
		if n != 3 {
			t.Fatalf("count: got %d want 3", n)
		}
		_, rows, err := db.Query("SELECT id FROM users")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 3 {
			t.Fatalf("rows: got %d want 3", len(rows))
		}
		want := map[int]struct {
			name string
			age  int64
		}{1: {"a", 1}, 2: {"b", 2}, 3: {"c", 3}}
		for k, w := range want {
			_, r, err := db.Query("SELECT name, age FROM users WHERE id = ?", tid(k))
			if err != nil || len(r) != 1 {
				t.Fatalf("pk %d: rows=%d err=%v", k, len(r), err)
			}
			if r[0][0].Str() != w.name || r[0][1].Int() != w.age {
				t.Errorf("pk %d: got %q/%d want %q/%d", k, r[0][0].Str(), r[0][1].Int(), w.name, w.age)
			}
		}
	})

	t.Run("mixed_literals_and_params", func(t *testing.T) {
		db := openMem(t)
		// Tuple 0 is all literals (the UUID literal string equals tid(1)); tuple
		// 1 is mixed; tuple 2 is all params. Params get global indices in source
		// order, so the five args bind across the three tuples.
		// The UUID literal is tid(1)'s canonical form (n=1 lands in big-endian
		// byte 5, i.e. the "0001" group), so the literal tuple is addressable by
		// tid(1) below.
		n, err := db.Exec("INSERT INTO users (id, name, age) VALUES "+
			"('00000000-0001-7000-8000-000000000000', 'lit', 10), "+
			"(?, 'half', ?), "+
			"(?, ?, ?)",
			tid(2), int64(20), tid(3), "params", int64(30))
		if err != nil {
			t.Fatalf("mixed insert: %v", err)
		}
		if n != 3 {
			t.Fatalf("count: got %d want 3", n)
		}
		// literal tuple
		_, r0, err := db.Query("SELECT name, age FROM users WHERE id = ?", tid(1))
		if err != nil || len(r0) != 1 || r0[0][0].Str() != "lit" || r0[0][1].Int() != 10 {
			t.Fatalf("literal tuple: rows=%d %v err=%v", len(r0), r0, err)
		}
		// mixed tuple: id+age from params, name literal
		_, r1, err := db.Query("SELECT name, age FROM users WHERE id = ?", tid(2))
		if err != nil || len(r1) != 1 || r1[0][0].Str() != "half" || r1[0][1].Int() != 20 {
			t.Fatalf("mixed tuple: rows=%d %v err=%v", len(r1), r1, err)
		}
		// all-param tuple
		_, r2, err := db.Query("SELECT name, age FROM users WHERE id = ?", tid(3))
		if err != nil || len(r2) != 1 || r2[0][0].Str() != "params" || r2[0][1].Int() != 30 {
			t.Fatalf("param tuple: rows=%d %v err=%v", len(r2), r2, err)
		}
	})

	t.Run("auto_pk_per_row", func(t *testing.T) {
		db := openMem(t)
		n, err := db.Exec("INSERT INTO users (name, age) VALUES (?, ?), (?, ?), (?, ?)",
			"x", 1, "y", 2, "z", 3)
		if err != nil {
			t.Fatalf("auto-pk insert: %v", err)
		}
		if n != 3 {
			t.Fatalf("count: got %d want 3", n)
		}
		_, rows, err := db.Query("SELECT id FROM users")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 3 {
			t.Fatalf("rows: got %d want 3", len(rows))
		}
		seen := map[UUID]bool{}
		for _, r := range rows {
			u := r[0].UUID()
			if seen[u] {
				t.Errorf("duplicate auto-generated PK %v", u)
			}
			seen[u] = true
		}
	})

	t.Run("atomic_duplicate_pk_in_batch", func(t *testing.T) {
		db := openMem(t)
		// Two tuples share a PK: the whole statement fails, nothing is inserted.
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?, ?)",
			tid(1), "a", 1, tid(1), "b", 2)
		if !errors.Is(err, ErrDuplicatePK) {
			t.Fatalf("want ErrDuplicatePK, got %v", err)
		}
		_, rows, _ := db.Query("SELECT id FROM users")
		if len(rows) != 0 {
			t.Fatalf("atomicity: %d rows present after failed batch, want 0", len(rows))
		}
	})

	t.Run("atomic_duplicate_pk_against_store", func(t *testing.T) {
		db := openMem(t)
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "existing", 9); err != nil {
			t.Fatal(err)
		}
		// A fresh row then one colliding with the existing PK: all-or-nothing.
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?, ?)",
			tid(1), "fresh", 1, tid(2), "dup", 2)
		if !errors.Is(err, ErrDuplicatePK) {
			t.Fatalf("want ErrDuplicatePK, got %v", err)
		}
		if _, r1, _ := db.Query("SELECT id FROM users WHERE id = ?", tid(1)); len(r1) != 0 {
			t.Errorf("atomicity: tid(1) present after failed batch")
		}
		if _, r2, _ := db.Query("SELECT name FROM users WHERE id = ?", tid(2)); len(r2) != 1 || r2[0][0].Str() != "existing" {
			t.Errorf("tid(2) changed by failed batch: %v", r2)
		}
	})

	t.Run("atomic_type_error", func(t *testing.T) {
		db := openMem(t)
		// Second tuple has a bad age (string into INT): the whole batch fails.
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?, ?)",
			tid(1), "ok", 1, tid(2), "bad", "not-an-int")
		if err == nil {
			t.Fatal("want type error, got nil")
		}
		_, rows, _ := db.Query("SELECT id FROM users")
		if len(rows) != 0 {
			t.Fatalf("atomicity: %d rows after failed batch, want 0", len(rows))
		}
	})

	t.Run("column_count_mismatch", func(t *testing.T) {
		db := openMem(t)
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?)",
			tid(1), "a", 1, tid(2), "b")
		if !errors.Is(err, ErrParse) {
			t.Fatalf("want ErrParse, got %v", err)
		}
	})

	t.Run("batch_size_cap", func(t *testing.T) {
		mk := func(n int) (string, []any) {
			var sb strings.Builder
			sb.WriteString("INSERT INTO users (id, name, age) VALUES ")
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString("(?, ?, ?)")
				args = append(args, tid(i+1), "n", i%100)
			}
			return sb.String(), args
		}
		// Exactly the cap is accepted.
		db := openMem(t)
		sql, args := mk(maxTxnMutations)
		if n, err := db.Exec(sql, args...); err != nil || n != maxTxnMutations {
			t.Fatalf("at cap: n=%d err=%v", n, err)
		}
		// One over the cap is rejected, and nothing from it is inserted.
		db2 := openMem(t)
		sql, args = mk(maxTxnMutations + 1)
		if _, err := db2.Exec(sql, args...); !errors.Is(err, ErrBatchTooLarge) {
			t.Fatalf("over cap: want ErrBatchTooLarge, got %v", err)
		}
		if _, rows, _ := db2.Query("SELECT id FROM users"); len(rows) != 0 {
			t.Fatalf("over-cap statement inserted %d rows, want 0", len(rows))
		}
	})
}

// TestMultiRowInsertWALReplay confirms a multi-row INSERT (one TXN envelope)
// survives a close + reopen replay with every row intact.
func TestMultiRowInsertWALReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	db, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	n, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		tid(1), "a", 1, tid(2), "b", 2, tid(3), "c", 3)
	if err != nil || n != 3 {
		t.Fatalf("insert: n=%d err=%v", n, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	_, rows, err := db2.Query("SELECT id, name, age FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("after replay: got %d rows want 3", len(rows))
	}
}

// TestInsertRejectsColumnRef: a column reference in INSERT VALUES has no current
// row to resolve against — evalExpr would index a nil ctx.row and panic the
// whole process (a wire-API DoS). It must be a clean plan error instead. Running
// to completion (no panic) is itself part of the assertion.
func TestInsertRejectsColumnRef(t *testing.T) {
	db := openMem(t)
	cases := []string{
		"INSERT INTO users (name, age) VALUES (name, 1)",            // bare colRef
		"INSERT INTO users (name, age) VALUES ('x', age + 1)",       // colRef in arithmetic
		"INSERT INTO users (name, age) VALUES (nope, 1)",            // unknown-column colRef
		"INSERT INTO users (name, age) VALUES ('a', 1), ('b', age)", // multi-row, second tuple
	}
	for _, sql := range cases {
		if _, err := db.Exec(sql); !errors.Is(err, ErrParse) {
			t.Errorf("%q: want ErrParse, got %v", sql, err)
		}
	}
}

// TestInsertRejectsDuplicateColumn: a column named twice in the INSERT target
// list silently took the last value before; it must be a plan error.
func TestInsertRejectsDuplicateColumn(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (name, name) VALUES ('a', 'b')"); !errors.Is(err, ErrParse) {
		t.Fatalf("duplicate target column: want ErrParse, got %v", err)
	}
}

// TestArgCountStrict: the arg count must match the statement's parameter count
// in both directions (too many were silently ignored before). Covers all three
// dispatch funnels: execWrite (INSERT), queryPlanV (Query), queryRowPlanV
// (QueryRow).
func TestArgCountStrict(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 1); err != nil {
		t.Fatalf("exact count: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "b", 2, "extra"); !errors.Is(err, ErrParamMismatch) {
		t.Errorf("INSERT too many: want ErrParamMismatch, got %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "c"); !errors.Is(err, ErrParamMismatch) {
		t.Errorf("INSERT too few: want ErrParamMismatch, got %v", err)
	}
	if _, _, err := db.Query("SELECT id FROM users WHERE id = ?", tid(1), "extra"); !errors.Is(err, ErrParamMismatch) {
		t.Errorf("Query too many: want ErrParamMismatch, got %v", err)
	}
	if _, _, err := db.QueryRow("SELECT id FROM users WHERE id = ?", tid(1), "extra"); !errors.Is(err, ErrParamMismatch) {
		t.Errorf("QueryRow too many: want ErrParamMismatch, got %v", err)
	}
}
