package hazedb

import (
	"strconv"
	"testing"
)

// UPDATE / DELETE WHERE <secondary-indexed column> = ? must resolve candidates
// through the index (idxLookup) and touch only the matching rows — not full-scan
// the table. These pin that behaviour and its correctness (multi-match, residual
// conjunct, dirty overlay).

func TestUpdateByIndex(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, email text, age int, INDEX (email))")
	for i := 0; i < 5; i++ {
		db.Exec("INSERT INTO u (id, email, age) VALUES (?, ?, ?)", NewUUIDv7(), "e"+strconv.Itoa(i), 10)
	}
	db.mergeIndexes()

	pl, err := db.prepare("UPDATE u SET age = ? WHERE email = ?", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if !pl.idxLookup {
		t.Fatal("UPDATE WHERE indexed column should plan as an index lookup, not a scan")
	}
	if n, err := db.Exec("UPDATE u SET age = ? WHERE email = ?", 99, "e2"); err != nil || n != 1 {
		t.Fatalf("update by index: n=%d err=%v", n, err)
	}
	if _, rows, _ := db.Query("SELECT age FROM u WHERE email = ?", "e2"); len(rows) != 1 || rows[0][0].Int() != 99 {
		t.Fatalf("e2 not updated: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT age FROM u WHERE email = ?", "e1"); rows[0][0].Int() != 10 {
		t.Fatal("e1 wrongly updated")
	}
}

func TestUpdateByIndexMultiMatchAndResidual(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, tag text, age int, INDEX (tag))")
	for i := 0; i < 6; i++ {
		db.Exec("INSERT INTO u (id, tag, age) VALUES (?, ?, ?)", NewUUIDv7(), "x", i)
	}
	db.mergeIndexes()
	// Non-unique index: WHERE tag = x matches all 6.
	if n, _ := db.Exec("UPDATE u SET age = ? WHERE tag = ?", 100, "x"); n != 6 {
		t.Fatalf("multi-match update: n=%d want 6", n)
	}
	// Residual on a non-indexed column (age) re-checked against the live row.
	if n, _ := db.Exec("UPDATE u SET age = ? WHERE tag = ? AND age = ?", 7, "x", 100); n != 6 {
		t.Fatalf("residual update (all match): n=%d want 6", n)
	}
	if n, _ := db.Exec("UPDATE u SET age = ? WHERE tag = ? AND age = ?", 8, "x", 999); n != 0 {
		t.Fatalf("residual update (none match): n=%d want 0", n)
	}
}

func TestDeleteByIndex(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, email text, INDEX (email))")
	for i := 0; i < 5; i++ {
		db.Exec("INSERT INTO u (id, email) VALUES (?, ?)", NewUUIDv7(), "e"+strconv.Itoa(i))
	}
	db.mergeIndexes()

	pl, _ := db.prepare("DELETE FROM u WHERE email = ?", db.cat.Load())
	if !pl.idxLookup {
		t.Fatal("DELETE WHERE indexed column should plan as an index lookup, not a scan")
	}
	if n, err := db.Exec("DELETE FROM u WHERE email = ?", "e3"); err != nil || n != 1 {
		t.Fatalf("delete by index: n=%d err=%v", n, err)
	}
	if _, rows, _ := db.Query("SELECT id FROM u WHERE email = ?", "e3"); len(rows) != 0 {
		t.Fatal("e3 not deleted")
	}
	if got := db.cat.Load().byName["u"].table.liveCount(); got != 4 {
		t.Fatalf("live count after delete = %d, want 4", got)
	}
}

// A row written but not yet merged lives only in the dirty overlay; the by-index
// update/delete must still find it (idxCandidates unions the overlay + re-checks).
func TestUpdateDeleteByIndexDirtyOverlay(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, email text, age int, INDEX (email))")
	db.Exec("INSERT INTO u (id, email, age) VALUES (?, ?, ?)", NewUUIDv7(), "fresh", 1)
	// No mergeIndexes() — the row is only in the dirty overlay.
	if n, _ := db.Exec("UPDATE u SET age = ? WHERE email = ?", 42, "fresh"); n != 1 {
		t.Fatalf("dirty-overlay update by index: n=%d want 1", n)
	}
	if _, rows, _ := db.Query("SELECT age FROM u WHERE email = ?", "fresh"); len(rows) != 1 || rows[0][0].Int() != 42 {
		t.Fatalf("dirty update not applied: %v", rows)
	}
	if n, _ := db.Exec("DELETE FROM u WHERE email = ?", "fresh"); n != 1 {
		t.Fatalf("dirty-overlay delete by index: n=%d want 1", n)
	}
	if _, rows, _ := db.Query("SELECT id FROM u WHERE email = ?", "fresh"); len(rows) != 0 {
		t.Fatal("fresh row not deleted")
	}
}
