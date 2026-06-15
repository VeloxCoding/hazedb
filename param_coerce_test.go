package hazedb

import (
	"errors"
	"testing"
)

// A UUID column addressed by a STRING parameter must resolve by the COLUMN type,
// not by the value's shape: bindParamUUIDCoercion records which params target a
// UUID column and coerceParams parses them before execution. These exercise the
// NATIVE path (db.Query/Exec/Values), which never guessed UUIDs by shape, so they
// pin the coercion mechanism itself (independent of the text/JSON surfaces).

func seedRefTable(t *testing.T) *DB {
	t.Helper()
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, ref uuid, name text, INDEX (ref))")
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec("INSERT INTO t (id, ref, name) VALUES (?, ?, ?)", tid(i), tid(100+i), "n"); err != nil {
			t.Fatal(err)
		}
	}
	db.mergeIndexes()
	return db
}

// Secondary-indexed UUID column, queried with a string arg.
func TestParamCoerceSecondaryIndexUUID(t *testing.T) {
	db := seedRefTable(t)
	_, rows, err := db.Query("SELECT id FROM t WHERE ref = ?", tid(102).String())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(2) {
		t.Fatalf("indexed ref=string: got %d rows %v", len(rows), rows)
	}
}

// Non-indexed UUID column → full scan; the WHERE matcher must still match.
func TestParamCoerceScanUUID(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE s (id uuid primary key, ref uuid)")
	db.Exec("INSERT INTO s (id, ref) VALUES (?, ?)", tid(1), tid(200))
	db.Exec("INSERT INTO s (id, ref) VALUES (?, ?)", tid(2), tid(201))
	_, rows, err := db.Query("SELECT id FROM s WHERE ref = ?", tid(201).String())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(2) {
		t.Fatalf("scan ref=string: got %d rows %v", len(rows), rows)
	}
}

// Composite ordered index with a UUID leading column, pinned by a string arg.
func TestParamCoerceCompositeUUIDPrefix(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE c (id uuid primary key, a uuid, b int, ORDERED INDEX (a, b))")
	db.Exec("INSERT INTO c (id, a, b) VALUES (?, ?, ?)", tid(1), tid(300), 1)
	db.Exec("INSERT INTO c (id, a, b) VALUES (?, ?, ?)", tid(2), tid(300), 2)
	db.Exec("INSERT INTO c (id, a, b) VALUES (?, ?, ?)", tid(3), tid(301), 1)
	db.mergeIndexes()
	_, rows, err := db.Query("SELECT id FROM c WHERE a = ? ORDER BY b", tid(300).String())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("composite a=string: got %d rows %v", len(rows), rows)
	}
}

// UPDATE SET on a UUID column with a string arg — the SET path has no coercion of
// its own, so it depends on coerceParams — plus a PK key as a string.
func TestParamCoerceUpdateSetUUID(t *testing.T) {
	db := seedRefTable(t)
	n, err := db.Exec("UPDATE t SET ref = ? WHERE id = ?", tid(999).String(), tid(2).String())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("update affected %d, want 1", n)
	}
	_, rows, _ := db.Query("SELECT ref FROM t WHERE id = ?", tid(2))
	if len(rows) != 1 || rows[0][0].UUID() != tid(999) {
		t.Fatalf("SET uuid=string not applied: %v", rows)
	}
}

// DELETE WHERE on a UUID column with a string arg.
func TestParamCoerceDeleteUUID(t *testing.T) {
	db := seedRefTable(t)
	n, err := db.Exec("DELETE FROM t WHERE ref = ?", tid(103).String())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("delete affected %d, want 1", n)
	}
}

// A string that is not a valid UUID, against a UUID column, is a type error —
// consistent with the PK and INSERT paths.
func TestParamCoerceInvalidUUIDErrors(t *testing.T) {
	db := seedRefTable(t)
	if _, _, err := db.Query("SELECT id FROM t WHERE ref = ?", "not-a-uuid"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("invalid UUID string for a UUID column: err=%v, want ErrTypeMismatch", err)
	}
}

// A join WHERE on a UUID column (bound to a global concat ordinal) with a string
// arg — exercises the join-aware type resolution in bindParamUUIDCoercion.
func TestParamCoerceJoinUUID(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, INDEX (author))")
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(1), "u1")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", tid(10), tid(1), "p1")
	db.mergeIndexes()
	_, rows, err := db.Query("SELECT p.title FROM posts p JOIN users u ON p.author = u.id WHERE p.author = ?", tid(1).String())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].Str() != "p1" {
		t.Fatalf("join WHERE author=string: got %d rows %v", len(rows), rows)
	}
}

// End-to-end through the text arg surface: QueryArgs keeps the UUID a STRING, then
// db.Query resolves the UUID column from it — coercion is by column type, not shape.
func TestParamCoerceTextSurfaceUUIDLookup(t *testing.T) {
	db := seedRefTable(t)
	args, err := QueryArgs(tid(101).String())
	if err != nil {
		t.Fatal(err)
	}
	_, rows, err := db.Query("SELECT id FROM t WHERE ref = ?", args...)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) {
		t.Fatalf("text-surface UUID lookup: got %d rows %v", len(rows), rows)
	}
}

// A TEXT column that happens to hold a canonical-UUID-form value must NOT be
// coerced: the value stays a string and matches the stored text (the forum case,
// here on the native path).
func TestParamCoerceTextColumnStaysString(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE tt (id uuid primary key, code text, INDEX (code))")
	uuidish := tid(7).String()
	db.Exec("INSERT INTO tt (id, code) VALUES (?, ?)", tid(1), uuidish)
	db.mergeIndexes()
	_, rows, err := db.Query("SELECT id FROM tt WHERE code = ?", uuidish)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) {
		t.Fatalf("text column holding a UUID-shaped value: got %d rows %v", len(rows), rows)
	}
}
