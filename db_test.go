package hazedb

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// testSchema returns a small schema used across most tests. The PK is a
// UUID (hard requirement since M4); tests build deterministic ordered PKs
// with tid(n) so query strings and ORDER BY id semantics carry over from the
// old integer-PK tests unchanged.
func testSchema() Schema {
	return Schema{
		Tables: []TableDef{
			{
				Name: "users",
				Columns: []ColumnDef{
					{Name: "id", Type: TypeUUID, PK: true},
					{Name: "name", Type: TypeString},
					{Name: "age", Type: TypeInt},
					{Name: "active", Type: TypeBool, Nullable: true},
				},
			},
		},
	}
}

// tid builds a deterministic, byte-ordered, valid-v7 UUID from an int — the
// stand-in for the integer PKs the tests used before M4. Monotonic in n
// (n lives in the 48-bit timestamp field), so ORDER BY id == order by n.
func tid(n int) UUID {
	var u UUID
	u[0] = byte(n >> 40)
	u[1] = byte(n >> 32)
	u[2] = byte(n >> 24)
	u[3] = byte(n >> 16)
	u[4] = byte(n >> 8)
	u[5] = byte(n)
	u[6] = 0x70 // version 7
	u[8] = 0x80 // variant 10
	return u
}

func openMem(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{Schema: testSchema()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func openDBWithWAL(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	db, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func TestInsertAndSelect(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25); err != nil {
		t.Fatalf("insert: %v", err)
	}

	cols, rows, err := db.Query("SELECT id, name, age FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(cols) != 3 || cols[0] != "id" || cols[1] != "name" || cols[2] != "age" {
		t.Errorf("cols: %v", cols)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: got %d", len(rows))
	}
	sort.Slice(rows, func(i, j int) bool { c, _ := rows[i][0].Compare(rows[j][0]); return c < 0 })
	if rows[0][1].S != "alice" || rows[0][2].I != 30 {
		t.Errorf("row 0: %v", rows[0])
	}
	if rows[1][1].S != "bob" || rows[1][2].I != 25 {
		t.Errorf("row 1: %v", rows[1])
	}
}

func TestSelectStar(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	cols, rows, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(cols) != 4 {
		t.Fatalf("cols: %v", cols)
	}
	if len(rows) != 1 || rows[0][0].U != tid(1) || rows[0][1].S != "alice" {
		t.Errorf("row: %v", rows[0])
	}
}

func TestSelectWhere(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "carol", 40)

	_, rows, err := db.Query("SELECT id FROM users WHERE age > ?", 28)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}

	_, rows, err = db.Query("SELECT id FROM users WHERE name = ?", "alice")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0][0].U != tid(1) {
		t.Errorf("expected alice row, got %v", rows)
	}

	_, rows, err = db.Query("SELECT id FROM users WHERE age >= ? AND age <= ?", 25, 30)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("range: got %d rows", len(rows))
	}
}

func TestSelectOrderByLimit(t *testing.T) {
	db := openMem(t)
	for i, name := range []string{"alice", "bob", "carol", "dave"} {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i+1), name, 20+i*5)
	}
	_, rows, err := db.Query("SELECT id, age FROM users ORDER BY age DESC LIMIT 2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("limit: %d", len(rows))
	}
	if rows[0][1].I != 35 || rows[1][1].I != 30 {
		t.Errorf("order desc: got %v", rows)
	}

	_, rows, _ = db.Query("SELECT id, age FROM users ORDER BY age LIMIT 1")
	if len(rows) != 1 || rows[0][1].I != 20 {
		t.Errorf("order asc default: %v", rows)
	}
}

func TestUpdate(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)

	n, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, tid(1))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if n != 1 {
		t.Errorf("n=%d", n)
	}
	_, rows, _ := db.Query("SELECT age FROM users WHERE id = ?", tid(1))
	if rows[0][0].I != 31 {
		t.Errorf("got %v", rows[0][0])
	}

	n, _ = db.Exec("UPDATE users SET age = ?", 99)
	if n != 2 {
		t.Errorf("bulk update n=%d", n)
	}
}

func TestUpdatePKRefused(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	if _, err := db.Exec("UPDATE users SET id = ? WHERE id = ?", tid(9), tid(1)); err == nil {
		t.Error("expected error on PK update")
	}
}

func TestDelete(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(3), "carol", 40)

	n, err := db.Exec("DELETE FROM users WHERE age < ?", 30)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n != 1 {
		t.Errorf("n=%d (expected 1: bob)", n)
	}

	_, rows, _ := db.Query("SELECT id FROM users")
	if len(rows) != 2 {
		t.Errorf("rows after delete: %d", len(rows))
	}

	n, _ = db.Exec("DELETE FROM users")
	if n != 2 {
		t.Errorf("full delete n=%d", n)
	}
}

func TestDuplicatePK(t *testing.T) {
	db := openMem(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice2", 31); err == nil {
		t.Error("expected duplicate PK error")
	}
}

func TestNullHandling(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("INSERT INTO users (id, name, age, active) VALUES (?, ?, ?, ?)", tid(1), "alice", 30, nil); err != nil {
		t.Fatalf("insert null: %v", err)
	}
	_, rows, _ := db.Query("SELECT active FROM users WHERE active IS NULL")
	if len(rows) != 1 {
		t.Errorf("IS NULL: %d", len(rows))
	}

	db.Exec("UPDATE users SET active = ? WHERE id = ?", true, tid(1))
	_, rows, _ = db.Query("SELECT active FROM users WHERE active IS NOT NULL")
	if len(rows) != 1 || rows[0][0].I != 1 {
		t.Errorf("IS NOT NULL: %v", rows)
	}
}

func TestWALRoundTrip(t *testing.T) {
	db, path := openDBWithWAL(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25)
	db.Exec("UPDATE users SET age = ? WHERE id = ?", 31, tid(1))
	db.Exec("DELETE FROM users WHERE id = ?", tid(2))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id, age FROM users")
	if len(rows) != 1 || rows[0][0].U != tid(1) || rows[0][1].I != 31 {
		t.Errorf("after replay: got %v", rows)
	}
}

func TestWALPartialTail(t *testing.T) {
	db, path := openDBWithWAL(t)
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Close()

	// Append garbage that looks like the start of a record but is
	// truncated. Replay must tolerate the dangling tail.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0x01}) // says len=16, but body is 1 byte
	f.Close()

	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatalf("partial tail should be tolerated, got %v", err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id FROM users")
	if len(rows) != 1 {
		t.Errorf("expected 1 surviving row, got %d", len(rows))
	}
}

func TestParseErrors(t *testing.T) {
	db := openMem(t)
	cases := []string{
		"SELECT FROM users",
		"INSERT INTO users (id) VALUES",
		"UPDATE users WHERE id = 1",
		"DELETE users",
		"SELECT bogus FROM users",
		"SELECT * FROM nonexistent",
	}
	for _, q := range cases {
		if _, _, err := db.Query(q); err == nil {
			if _, err := db.Exec(q); err == nil {
				t.Errorf("expected error for %q", q)
			}
		}
	}
}
