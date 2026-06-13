package hazedb

import (
	"errors"
	"testing"
)

// COUNT(*) is the sole aggregate: it counts matching rows without materializing
// them, reusing the WHERE planning (total scan, predicate scan, PK-pinned).
func TestCountStar(t *testing.T) {
	db := openMem(t)
	const n = 50
	for i := 0; i < n; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	count := func(sql string, args ...any) int64 {
		t.Helper()
		cols, rows, err := db.Query(sql, args...)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		if len(cols) != 1 || cols[0] != "count" {
			t.Fatalf("cols = %v, want [count]", cols)
		}
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("want one count row, got %v", rows)
		}
		return rows[0][0].Int()
	}

	// Total (full scan), predicate (scan with WHERE), and PK-pinned (0 or 1).
	if got := count("SELECT COUNT(*) FROM users"); got != n {
		t.Errorf("total: got %d, want %d", got, n)
	}
	if got := count("SELECT COUNT(*) FROM users WHERE age >= ?", 30); got != 20 {
		t.Errorf("age>=30: got %d, want 20", got)
	}
	if got := count("SELECT COUNT(*) FROM users WHERE id = ?", tid(3)); got != 1 {
		t.Errorf("pk hit: got %d, want 1", got)
	}
	if got := count("SELECT COUNT(*) FROM users WHERE id = ?", tid(999)); got != 0 {
		t.Errorf("pk miss: got %d, want 0", got)
	}

	// QueryRow returns the single count row.
	_, row, err := db.QueryRow("SELECT COUNT(*) FROM users WHERE age >= ?", 40)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if row[0].Int() != 10 {
		t.Errorf("QueryRow age>=40: got %d, want 10", row[0].Int())
	}

	// JSON shape: a single {"count":N} object.
	_, js, err := db.QueryJSON("SELECT COUNT(*) FROM users WHERE age >= ?", Int(45))
	if err != nil {
		t.Fatalf("QueryJSON: %v", err)
	}
	if string(js) != `[{"count":5}]` {
		t.Errorf("QueryJSON = %s, want [{\"count\":5}]", js)
	}
}

// COUNT(*) WHERE indexed_col = ? returns the merged index bucket size without
// touching rows. The result is as of the last merge: a not-yet-merged write is
// not reflected until the next merge (the documented bounded-staleness contract).
func TestCountStarIndexBucket(t *testing.T) {
	db, err := Open(Options{
		Schema: Schema{Tables: []TableDef{{
			Name: "items",
			Columns: []ColumnDef{
				{Name: "id", Type: TypeUUID, PK: true},
				{Name: "cat", Type: TypeString},
			},
			Indexes: []IndexDef{{Name: "by_cat", Columns: []string{"cat"}}},
		}}},
		indexMergeInterval: -1, // manual merge → deterministic staleness
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	for i := 0; i < 30; i++ { // i%3==0 → "b" (10 rows), else "a" (20 rows)
		cat := "a"
		if i%3 == 0 {
			cat = "b"
		}
		if _, err := db.Exec("INSERT INTO items (id, cat) VALUES (?, ?)", tid(i), cat); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	db.mergeIndexes()

	count := func(c string) int64 {
		t.Helper()
		_, r, err := db.Query("SELECT COUNT(*) FROM items WHERE cat = ?", c)
		if err != nil {
			t.Fatalf("count %q: %v", c, err)
		}
		return r[0][0].Int()
	}
	if got := count("b"); got != 10 {
		t.Errorf("cat=b: got %d, want 10", got)
	}
	if got := count("a"); got != 20 {
		t.Errorf("cat=a: got %d, want 20", got)
	}

	// Bounded staleness: a write before the next merge is not yet counted.
	if _, err := db.Exec("INSERT INTO items (id, cat) VALUES (?, ?)", tid(100), "b"); err != nil {
		t.Fatal(err)
	}
	if got := count("b"); got != 10 {
		t.Errorf("pre-merge: got %d, want 10 (indexed count is merge-stale)", got)
	}
	db.mergeIndexes()
	if got := count("b"); got != 11 {
		t.Errorf("post-merge: got %d, want 11", got)
	}
}

// COUNT(*) is FROM + WHERE only: JOIN / ORDER BY / LIMIT / OFFSET are rejected.
func TestCountStarRejectsClauses(t *testing.T) {
	for _, sql := range []string{
		"SELECT COUNT(*) FROM users ORDER BY age",
		"SELECT COUNT(*) FROM users LIMIT 5",
		"SELECT COUNT(*) FROM users WHERE age >= 0 OFFSET 2",
	} {
		if _, err := parseSQL(sql); !errors.Is(err, ErrParse) {
			t.Errorf("%q: got err %v, want ErrParse", sql, err)
		}
	}
}
