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

// An UPDATE that changes only a NON-indexed column leaves every index entry
// valid, so it must not enter the dirty overlay (which would slow later indexed
// lookups). An UPDATE of the indexed column itself must still dirty + be findable
// by its new value.
func TestUpdateNonIndexedColumnSkipsDirty(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, name text, score int, INDEX (name))")
	for i := 0; i < 5; i++ {
		db.Exec("INSERT INTO u (id, name, score) VALUES (?, ?, ?)", NewUUIDv7(), "n"+strconv.Itoa(i), 0)
	}
	db.mergeIndexes()
	tbl := db.cat.Load().byName["u"].table
	if d := tbl.dirtyCount.Load(); d != 0 {
		t.Fatalf("dirty after merge = %d, want 0", d)
	}
	// Non-indexed column: must not dirty the overlay, row still found with new value.
	if n, _ := db.Exec("UPDATE u SET score = ? WHERE name = ?", 42, "n2"); n != 1 {
		t.Fatalf("update non-indexed: n=%d want 1", n)
	}
	if d := tbl.dirtyCount.Load(); d != 0 {
		t.Fatalf("non-indexed update marked dirty (%d) — must stay out of the overlay", d)
	}
	if _, rows, _ := db.Query("SELECT score FROM u WHERE name = ?", "n2"); len(rows) != 1 || rows[0][0].Int() != 42 {
		t.Fatalf("indexed read after non-indexed update wrong: %v", rows)
	}
	// Indexed column: must dirty and be findable by the NEW value (not the old).
	if n, _ := db.Exec("UPDATE u SET name = ? WHERE name = ?", "renamed", "n3"); n != 1 {
		t.Fatalf("update indexed: n=%d want 1", n)
	}
	if d := tbl.dirtyCount.Load(); d == 0 {
		t.Fatal("indexed-column update should mark dirty")
	}
	if _, rows, _ := db.Query("SELECT score FROM u WHERE name = ?", "renamed"); len(rows) != 1 {
		t.Fatalf("row not findable by new indexed value: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT id FROM u WHERE name = ?", "n3"); len(rows) != 0 {
		t.Fatal("row still findable by OLD indexed value after rename")
	}
}

// When the dirty overlay outgrows the table (merger not keeping up), the hybrid
// candidate walk costs more than a scan, so idxLookup falls through to the scan
// path (dirtyTooDenseForScan). That fallback must still touch exactly the right
// rows — these inserts are never merged, so dirtyCount == liveCount triggers it.
func TestIndexWriteScanFallbackWhenDirtyDense(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, email text, age int, INDEX (email))")
	for i := 0; i < 50; i++ {
		db.Exec("INSERT INTO u (id, email, age) VALUES (?, ?, ?)", NewUUIDv7(), "e"+strconv.Itoa(i), 10)
	}
	// No mergeIndexes(): every row is in the dirty overlay, so dirtyCount equals
	// liveCount and the density guard forces the scan path.
	tbl := db.cat.Load().byName["u"].table
	if !tbl.dirtyTooDenseForScan() {
		t.Fatal("dense dirty overlay should force the scan fallback")
	}
	if n, err := db.Exec("UPDATE u SET age = ? WHERE email = ?", 99, "e7"); err != nil || n != 1 {
		t.Fatalf("scan-fallback update: n=%d err=%v", n, err)
	}
	if _, rows, _ := db.Query("SELECT age FROM u WHERE email = ?", "e7"); len(rows) != 1 || rows[0][0].Int() != 99 {
		t.Fatalf("e7 not updated via scan fallback: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT age FROM u WHERE email = ?", "e8"); rows[0][0].Int() != 10 {
		t.Fatal("e8 wrongly updated by scan fallback")
	}
	if n, err := db.Exec("DELETE FROM u WHERE email = ?", "e7"); err != nil || n != 1 {
		t.Fatalf("scan-fallback delete: n=%d err=%v", n, err)
	}
	if got := tbl.liveCount(); got != 49 {
		t.Fatalf("live count after scan-fallback delete = %d, want 49", got)
	}
}
