package hazedb

import (
	"strconv"
	"testing"
)

// S2: a WHERE on an indexed column plans as an index lookup and returns the
// right rows (point hit, miss).
func TestIndexPointReadAndPlan(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text, email text, UNIQUE INDEX (email))")
	for i := 0; i < 5; i++ {
		db.Exec("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", NewUUIDv7(), "u"+strconv.Itoa(i), "e"+strconv.Itoa(i))
	}
	pl, err := db.prepare("SELECT name FROM users WHERE email = ?", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if !pl.idxLookup {
		t.Fatal("WHERE on indexed column did not plan as an index lookup")
	}
	if _, rows, _ := db.Query("SELECT name FROM users WHERE email = ?", "e3"); len(rows) != 1 || rows[0][0].Str() != "u3" {
		t.Fatalf("index point read wrong: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT name FROM users WHERE email = ?", "nope"); len(rows) != 0 {
		t.Fatalf("index miss should be empty: %v", rows)
	}
}

// S2: incremental maintenance keeps the index correct across an indexed-column
// UPDATE (old value gone, new value findable) and a DELETE.
func TestIndexMaintainedOnUpdateDelete(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, email text, UNIQUE INDEX (email))")
	id := NewUUIDv7()
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", id, "a@x")
	if _, err := db.Exec("UPDATE users SET email = ? WHERE id = ?", "b@x", id); err != nil {
		t.Fatal(err)
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a@x"); len(rows) != 0 {
		t.Fatal("stale email still indexed after update")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "b@x"); len(rows) != 1 {
		t.Fatal("new email not indexed after update")
	}
	db.Exec("DELETE FROM users WHERE id = ?", id)
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "b@x"); len(rows) != 0 {
		t.Fatal("deleted row still indexed")
	}
}

// S2: bulk (non-PK predicate) update/delete is not tracked incrementally, so it
// triggers a full index rebuild — the index must still match afterwards.
func TestIndexBulkRebuild(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, grp int, email text, INDEX (email))")
	for i := 0; i < 6; i++ {
		db.Exec("INSERT INTO users (id, grp, email) VALUES (?, ?, ?)", NewUUIDv7(), i%2, "e"+strconv.Itoa(i))
	}
	db.Exec("UPDATE users SET email = ? WHERE grp = ?", "shared", 0) // i=0,2,4
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "shared"); len(rows) != 3 {
		t.Fatalf("bulk update not reflected in index: got %d, want 3", len(rows))
	}
	db.Exec("DELETE FROM users WHERE grp = ?", 1) // i=1,3,5
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "e1"); len(rows) != 0 {
		t.Fatal("bulk delete not reflected in index")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "shared"); len(rows) != 3 {
		t.Fatalf("bulk delete disturbed unrelated index entries: %d", len(rows))
	}
}

// S3: the hybrid read re-checks each candidate against the live row, so a stale
// index entry (a phantom PK, or a PK whose live value no longer matches) yields
// no wrong row. Injected directly to simulate a lagging async index.
func TestIndexHybridRecheckFiltersStale(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, email text, INDEX (email))")
	id := NewUUIDv7()
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", id, "real@x")

	tbl := db.cat.Load().byName["users"].table
	si := tbl.indexFor(tbl.def.colByName["email"])
	phantom := NewUUIDv7() // never inserted
	si.mu.Lock()
	// "ghost@x" bucket: a phantom PK (absent row) and the real id (whose live
	// email is "real@x", not "ghost@x"). Both must be filtered by the re-check.
	si.fwd[keyOf(Str("ghost@x"))] = []UUID{phantom, id}
	si.mu.Unlock()

	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "ghost@x"); len(rows) != 0 {
		t.Fatalf("hybrid re-check did not filter stale entries: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "real@x"); len(rows) != 1 {
		t.Fatal("hybrid re-check dropped a valid row")
	}
}

// S4: with maintenance off the write path, a freshly written row is found via
// the dirty overlay before any merge; after merge it lives in the index and the
// dirty list is drained.
func TestIndexDirtyOverlayThenMerge(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, email text, UNIQUE INDEX (email))")
	id := NewUUIDv7()
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", id, "a@x")
	tbl := db.cat.Load().byName["users"].table
	si := tbl.indexFor(tbl.def.colByName["email"])

	if got := si.lookup(keyOf(Str("a@x"))); len(got) != 0 {
		t.Fatalf("index should be empty before merge, got %v", got)
	}
	if n := len(tbl.dirtyPKs()); n != 1 {
		t.Fatalf("expected 1 dirty PK before merge, got %d", n)
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a@x"); len(rows) != 1 {
		t.Fatal("row not found via dirty overlay before merge")
	}

	db.mergeIndexes()
	if got := si.lookup(keyOf(Str("a@x"))); len(got) != 1 {
		t.Fatalf("index should hold the row after merge, got %v", got)
	}
	if n := len(tbl.dirtyPKs()); n != 0 {
		t.Fatalf("dirty not drained after merge: %d", n)
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a@x"); len(rows) != 1 {
		t.Fatal("row not found via index after merge")
	}
}

// S4: merge reconciles updates and deletes against the live rows.
func TestIndexMergeReconciles(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, email text, INDEX (email))")
	a, b := NewUUIDv7(), NewUUIDv7()
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", a, "a@x")
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", b, "b@x")
	db.mergeIndexes()
	db.Exec("UPDATE users SET email = ? WHERE id = ?", "a2@x", a)
	db.Exec("DELETE FROM users WHERE id = ?", b)
	db.mergeIndexes()

	tbl := db.cat.Load().byName["users"].table
	si := tbl.indexFor(tbl.def.colByName["email"])
	if len(si.lookup(keyOf(Str("a@x")))) != 0 {
		t.Fatal("old email still in index after merge")
	}
	if len(si.lookup(keyOf(Str("a2@x")))) != 1 {
		t.Fatal("updated email not in index after merge")
	}
	if len(si.lookup(keyOf(Str("b@x")))) != 0 {
		t.Fatal("deleted row still in index after merge")
	}
	if n := len(tbl.dirtyPKs()); n != 0 {
		t.Fatalf("dirty not drained: %d", n)
	}
}

// S2: a non-unique index returns every matching row (bucket of PKs).
func TestIndexNonUniqueBucket(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, city text, INDEX (city))")
	for i := 0; i < 4; i++ {
		db.Exec("INSERT INTO users (id, city) VALUES (?, ?)", NewUUIDv7(), "AMS")
	}
	db.Exec("INSERT INTO users (id, city) VALUES (?, ?)", NewUUIDv7(), "RTM")
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "AMS"); len(rows) != 4 {
		t.Fatalf("non-unique index bucket wrong: got %d, want 4", len(rows))
	}
}
