package hazedb

import (
	"path/filepath"
	"testing"
)

// Arithmetic SET on a PK-pinned row: col = col +/-/* ?.
func TestArithmeticSetPK(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "a", 100)

	if _, err := db.Exec("UPDATE users SET age = age - ? WHERE id = ?", 30, 1); err != nil {
		t.Fatal(err)
	}
	if got := ageOf(t, db, 1); got != 70 {
		t.Fatalf("after -30: got %d, want 70", got)
	}
	db.Exec("UPDATE users SET age = age + ? WHERE id = ?", 5, 1)
	if got := ageOf(t, db, 1); got != 75 {
		t.Fatalf("after +5: got %d, want 75", got)
	}
	db.Exec("UPDATE users SET age = age * ? WHERE id = ?", 2, 1)
	if got := ageOf(t, db, 1); got != 150 {
		t.Fatalf("after *2: got %d, want 150", got)
	}
}

// Arithmetic SET via a multi-shard predicate (no PK pin).
func TestArithmeticSetMultiShard(t *testing.T) {
	db := openMem(t)
	for i := 0; i < 100; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "u", 10)
	}
	n, err := db.Exec("UPDATE users SET age = age + ? WHERE age = ?", 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("n=%d, want 100", n)
	}
	if got := ageOf(t, db, 42); got != 15 {
		t.Fatalf("got %d, want 15", got)
	}
}

// The WAL must record the RESOLVED value, not the expression, so replay is
// deterministic: reopening reproduces 70, never re-applies "age - 40" against
// whatever age happens to be at replay time.
func TestArithmeticSetWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.wal")
	db, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "a", 100)
	db.Exec("UPDATE users SET age = age - ? WHERE id = ?", 40, 1) // -> 60
	db.Exec("UPDATE users SET age = age + ? WHERE id = ?", 10, 1) // -> 70
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := ageOf(t, db2, 1); got != 70 {
		t.Fatalf("after replay: got %d, want 70", got)
	}
}

// Arithmetic is also accepted in WHERE.
func TestArithmeticInWhere(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 1, "a", 10)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", 2, "b", 20)
	_, rows, err := db.Query("SELECT id FROM users WHERE age + ? > ?", 5, 22)
	if err != nil {
		t.Fatal(err)
	}
	// age+5 => {15, 25}; > 22 keeps only id=2.
	if len(rows) != 1 || rows[0][0].I != 2 {
		t.Fatalf("got %v, want only id=2", rows)
	}
}

func ageOf(t *testing.T, db *DB, id int) int64 {
	t.Helper()
	_, rows, err := db.Query("SELECT age FROM users WHERE id = ?", id)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("id=%d: expected 1 row, got %d", id, len(rows))
	}
	return rows[0][0].I
}
