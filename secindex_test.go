package hazedb

import (
	"strconv"
	"sync"
	"testing"
	"time"
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

// S5: the golden invariant under concurrent writers, readers, and the
// background merger. Run with -race. Two checks: (1) live — an index query never
// returns a row that does not actually match (no false positive; the hybrid
// re-check guarantees this); (2) quiescent — after writers stop and a drain,
// the index query result equals a brute-force full scan for every value (no
// false negative).
func TestIndexConcurrentInvariant(t *testing.T) {
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE users (id uuid primary key, grp int, email text, INDEX (grp))")
	const N, groups = 200, 10
	ids := make([]UUID, N)
	for i := range ids {
		ids[i] = NewUUIDv7()
		db.Exec("INSERT INTO users (id, grp, email) VALUES (?, ?, ?)", ids[i], i%groups, "e")
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ { // writers churn grp values
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				db.Exec("UPDATE users SET grp = ? WHERE id = ?", r%groups, ids[r%N])
				r += 7
			}
		}(w)
	}
	for rd := 0; rd < 4; rd++ { // readers assert no false positive
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 3000; k++ {
				select {
				case <-stop:
					return
				default:
				}
				v := k % groups
				_, rows, err := db.Query("SELECT grp FROM users WHERE grp = ?", v)
				if err != nil {
					t.Error(err)
					return
				}
				for _, row := range rows {
					if row[0].Int() != int64(v) {
						t.Errorf("false positive: index query grp=%d returned a row with grp=%d", v, row[0].Int())
						return
					}
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Quiescent consistency: drain, then index query == brute-force scan.
	db.mergeIndexes()
	_, all, _ := db.Query("SELECT id, grp FROM users") // no WHERE -> full scan
	want := make(map[int64]int)
	for _, r := range all {
		want[r[1].Int()]++
	}
	for v := int64(0); v < groups; v++ {
		_, idxRows, _ := db.Query("SELECT id FROM users WHERE grp = ?", v)
		if len(idxRows) != want[v] {
			t.Errorf("grp=%d: index returned %d rows, full scan expects %d", v, len(idxRows), want[v])
		}
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

// S7: two indexes on one table. Each plans its own lookup; an update to one
// indexed column moves only that index; a delete drops the row from both.
func TestIndexMultiplePerTable(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, email text, city text, UNIQUE INDEX (email), INDEX (city))")
	a, b := NewUUIDv7(), NewUUIDv7()
	db.Exec("INSERT INTO users (id, email, city) VALUES (?, ?, ?)", a, "a@x", "AMS")
	db.Exec("INSERT INTO users (id, email, city) VALUES (?, ?, ?)", b, "b@x", "AMS")
	db.mergeIndexes()

	plE, _ := db.prepare("SELECT id FROM users WHERE email = ?", db.cat.Load())
	plC, _ := db.prepare("SELECT id FROM users WHERE city = ?", db.cat.Load())
	if !plE.idxLookup || plE.idxColOrd != 1 {
		t.Fatalf("email query did not plan on the email index: %+v", plE.idxLookup)
	}
	if !plC.idxLookup || plC.idxColOrd != 2 {
		t.Fatalf("city query did not plan on the city index")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a@x"); len(rows) != 1 {
		t.Fatal("email lookup wrong")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "AMS"); len(rows) != 2 {
		t.Fatal("city lookup wrong")
	}

	db.Exec("UPDATE users SET email = ? WHERE id = ?", "a2@x", a) // moves only the email index
	db.mergeIndexes()
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a@x"); len(rows) != 0 {
		t.Fatal("old email lingered")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a2@x"); len(rows) != 1 {
		t.Fatal("new email missing")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "AMS"); len(rows) != 2 {
		t.Fatal("city index disturbed by an email-only update")
	}

	db.Exec("DELETE FROM users WHERE id = ?", a) // drops from both indexes
	db.mergeIndexes()
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ?", "a2@x"); len(rows) != 0 {
		t.Fatal("deleted row's email lingered")
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "AMS"); len(rows) != 1 {
		t.Fatal("city index not updated on delete")
	}
}

// S6: churn within non-unique buckets — moving a PK between buckets (update) and
// removing one (delete), across merges, must keep both buckets exact. Exercises
// removeFwdLocked's swap-remove on multi-PK buckets.
func TestIndexNonUniqueChurn(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, city text, INDEX (city))")
	ams := make([]UUID, 3)
	for i := range ams {
		ams[i] = NewUUIDv7()
		db.Exec("INSERT INTO users (id, city) VALUES (?, ?)", ams[i], "AMS")
	}
	rtm := NewUUIDv7()
	db.Exec("INSERT INTO users (id, city) VALUES (?, ?)", rtm, "RTM")
	db.mergeIndexes()

	db.Exec("UPDATE users SET city = ? WHERE id = ?", "RTM", ams[0]) // AMS -> RTM
	db.Exec("DELETE FROM users WHERE id = ?", ams[1])                // drop one AMS
	db.mergeIndexes()

	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "AMS"); len(rows) != 1 {
		t.Fatalf("AMS bucket after churn: got %d, want 1", len(rows))
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "RTM"); len(rows) != 2 {
		t.Fatalf("RTM bucket after churn: got %d, want 2", len(rows))
	}
	tbl := db.cat.Load().byName["users"].table
	if n := len(tbl.dirtyPKs()); n != 0 {
		t.Fatalf("dirty not drained: %d", n)
	}
}
