package hazedb

import (
	"path/filepath"
	"sort"
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
	if !plE.idxLookup || len(plE.idxCols) != 1 || plE.idxCols[0] != 1 {
		t.Fatalf("email query did not plan on the email index: %+v", plE.idxCols)
	}
	if !plC.idxLookup || len(plC.idxCols) != 1 || plC.idxCols[0] != 2 {
		t.Fatalf("city query did not plan on the city index: %+v", plC.idxCols)
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

// S8: after a restart the index is rebuilt from the replayed live rows. The
// reopened DB has the merger disabled, so si.lookup reflects only what the
// post-replay rebuild produced (not the overlay or a later merge).
func TestIndexRebuildAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idxrec.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path, IndexMergeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := NewUUIDv7(), NewUUIDv7(), NewUUIDv7()
	db.Exec("CREATE TABLE users (id uuid primary key, email text, INDEX (email))")
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", a, "a@x")
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", b, "b@x")
	db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", c, "c@x")
	db.Exec("UPDATE users SET email = ? WHERE id = ?", "a2@x", a)
	db.Exec("DELETE FROM users WHERE id = ?", b)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALPath: path, IndexMergeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	tbl := db2.cat.Load().byName["users"].table
	si := tbl.indexFor(tbl.def.colByName["email"])
	if n := len(tbl.dirtyPKs()); n != 0 {
		t.Fatalf("dirty not cleared after post-replay rebuild: %d", n)
	}
	if len(si.lookup(keyOf(Str("a2@x")))) != 1 {
		t.Fatal("updated email missing from rebuilt index")
	}
	if len(si.lookup(keyOf(Str("a@x")))) != 0 {
		t.Fatal("pre-update email present in rebuilt index")
	}
	if len(si.lookup(keyOf(Str("b@x")))) != 0 {
		t.Fatal("deleted row present in rebuilt index")
	}
	if len(si.lookup(keyOf(Str("c@x")))) != 1 {
		t.Fatal("untouched email missing from rebuilt index")
	}
	if _, rows, _ := db2.Query("SELECT id FROM users WHERE email = ?", "a2@x"); len(rows) != 1 {
		t.Fatal("index query wrong after restart")
	}
}

// O1: ORDERED INDEX parses, resolves with the ordered flag, survives a restart,
// and (until O2) still serves equality like a hash index.
func TestOrderedIndexDeclared(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ord.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path, IndexMergeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE posts (id uuid primary key, email text, ORDERED INDEX (email))"); err != nil {
		t.Fatal(err)
	}
	rt := db.cat.Load().byName["posts"].table
	if len(rt.def.indexes) != 1 || !rt.def.indexes[0].ordered {
		t.Fatalf("ordered flag not resolved: %+v", rt.def.indexes)
	}
	id := NewUUIDv7()
	db.Exec("INSERT INTO posts (id, email) VALUES (?, ?)", id, "a@x")
	if _, rows, _ := db.Query("SELECT id FROM posts WHERE email = ?", "a@x"); len(rows) != 1 {
		t.Fatal("equality on ordered index broken")
	}
	db.Close()

	db2, err := Open(Options{Schema: Schema{}, WALPath: path, IndexMergeInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	rt2 := db2.cat.Load().byName["posts"].table
	if len(rt2.def.indexes) != 1 || !rt2.def.indexes[0].ordered {
		t.Fatalf("ordered flag lost after restart: %+v", rt2.def.indexes)
	}
}

// O2: an ordered index builds a sorted view on merge and answers equality via
// binary search (and via the dirty overlay before merge).
func TestOrderedIndexEquality(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, email text, ORDERED INDEX (email))")
	for _, e := range []string{"c@x", "a@x", "b@x", "a@x"} {
		db.Exec("INSERT INTO posts (id, email) VALUES (?, ?)", NewUUIDv7(), e)
	}
	// before merge: served via the dirty overlay
	if _, rows, _ := db.Query("SELECT id FROM posts WHERE email = ?", "a@x"); len(rows) != 2 {
		t.Fatalf("pre-merge equality via overlay: %d, want 2", len(rows))
	}
	db.mergeIndexes()

	tbl := db.cat.Load().byName["posts"].table
	si := tbl.indexFor(tbl.def.colByName["email"])
	if len(si.sorted) != 4 {
		t.Fatalf("sorted view len %d, want 4", len(si.sorted))
	}
	for i := 1; i < len(si.sorted); i++ {
		if si.sorted[i].key.less(si.sorted[i-1].key) {
			t.Fatal("sorted view is not in order")
		}
	}
	if got := len(si.lookup(keyOf(Str("a@x")))); got != 2 {
		t.Fatalf("ordered lookup a@x: %d, want 2", got)
	}
	if got := len(si.lookup(keyOf(Str("z@x")))); got != 0 {
		t.Fatalf("ordered lookup miss z@x: %d, want 0", got)
	}
	if _, rows, _ := db.Query("SELECT id FROM posts WHERE email = ?", "b@x"); len(rows) != 1 {
		t.Fatal("post-merge equality via ordered index wrong")
	}
}

// O3: a global ORDER BY on an ordered index walks the sorted view (no scan +
// sort). Correct ASC/DESC/LIMIT, before and after merge, and merging the sorted
// view with the not-yet-merged dirty overlay.
func TestOrderedIndexOrderBy(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, email text, ORDERED INDEX (email))")
	for _, e := range []string{"d@x", "a@x", "e@x", "b@x", "c@x"} {
		db.Exec("INSERT INTO posts (id, email) VALUES (?, ?)", NewUUIDv7(), e)
	}
	pl, _ := db.prepare("SELECT email FROM posts ORDER BY email ASC LIMIT 3", db.cat.Load())
	if !pl.orderWalk {
		t.Fatal("ORDER BY on an ordered index did not plan as an ordered walk")
	}

	get := func(sql string) []string {
		_, rows, err := db.Query(sql)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r[0].Str()
		}
		return out
	}
	eqS := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	for _, phase := range []string{"overlay", "merged"} {
		if phase == "merged" {
			db.mergeIndexes()
		}
		if got := get("SELECT email FROM posts ORDER BY email ASC"); !eqS(got, []string{"a@x", "b@x", "c@x", "d@x", "e@x"}) {
			t.Fatalf("[%s] ASC all: %v", phase, got)
		}
		if got := get("SELECT email FROM posts ORDER BY email DESC LIMIT 2"); !eqS(got, []string{"e@x", "d@x"}) {
			t.Fatalf("[%s] DESC LIMIT 2: %v", phase, got)
		}
		if got := get("SELECT email FROM posts ORDER BY email ASC LIMIT 3"); !eqS(got, []string{"a@x", "b@x", "c@x"}) {
			t.Fatalf("[%s] ASC LIMIT 3: %v", phase, got)
		}
	}

	// merged sorted view + not-yet-merged dirty overlay, interleaved in order
	db.Exec("INSERT INTO posts (id, email) VALUES (?, ?)", NewUUIDv7(), "aa@x") // between a@x and b@x
	db.Exec("INSERT INTO posts (id, email) VALUES (?, ?)", NewUUIDv7(), "f@x")  // after e@x
	if got := get("SELECT email FROM posts ORDER BY email ASC LIMIT 4"); !eqS(got, []string{"a@x", "aa@x", "b@x", "c@x"}) {
		t.Fatalf("snap+overlay ASC LIMIT 4: %v", got)
	}
}

// O4: the golden invariant for the ordered walk under concurrent writers,
// readers, and the background merger (run with -race). Live: an ORDER BY result
// is monotonic (no out-of-order row). Quiescent: the ordered walk equals a
// brute-force scan-then-sort.
func TestOrderedIndexConcurrentInvariant(t *testing.T) {
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE posts (id uuid primary key, score int, ORDERED INDEX (score))")
	const N = 300
	ids := make([]UUID, N)
	for i := range ids {
		ids[i] = NewUUIDv7()
		db.Exec("INSERT INTO posts (id, score) VALUES (?, ?)", ids[i], i%50)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ { // writers churn score
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
				db.Exec("UPDATE posts SET score = ? WHERE id = ?", r%50, ids[r%N])
				r += 7
			}
		}(w)
	}
	for rd := 0; rd < 4; rd++ { // readers: ORDER BY must come back monotonic
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 3000; k++ {
				select {
				case <-stop:
					return
				default:
				}
				_, rows, err := db.Query("SELECT score FROM posts ORDER BY score ASC LIMIT 20")
				if err != nil {
					t.Error(err)
					return
				}
				for i := 1; i < len(rows); i++ {
					if rows[i][0].Int() < rows[i-1][0].Int() {
						t.Errorf("ordered walk not sorted: %d after %d", rows[i][0].Int(), rows[i-1][0].Int())
						return
					}
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Quiescent: ordered walk == brute-force scan + sort.
	db.mergeIndexes()
	_, all, _ := db.Query("SELECT score FROM posts")
	scores := make([]int, len(all))
	for i, r := range all {
		scores[i] = int(r[0].Int())
	}
	sort.Ints(scores)
	_, top, _ := db.Query("SELECT score FROM posts ORDER BY score ASC LIMIT 30")
	if len(top) != 30 {
		t.Fatalf("ordered walk returned %d rows, want 30", len(top))
	}
	for i := range top {
		if int(top[i][0].Int()) != scores[i] {
			t.Fatalf("walk[%d]=%d, scan-sorted=%d", i, top[i][0].Int(), scores[i])
		}
	}
}

// AND of equalities: the planner picks one index for a conjunct and
// residual-filters the full WHERE on the candidates. SELECT ... WHERE email = ?
// AND name = ? returns only rows matching both.
func TestIndexAndQuery(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text, age int null, email text, INDEX (email), INDEX (name))")
	a, b, c := NewUUIDv7(), NewUUIDv7(), NewUUIDv7()
	db.Exec("INSERT INTO users (id, name, age, email) VALUES (?, ?, ?, ?)", a, "Alice", 30, "shared@x")
	db.Exec("INSERT INTO users (id, name, age, email) VALUES (?, ?, ?, ?)", b, "Bob", 25, "shared@x")
	db.Exec("INSERT INTO users (id, name, age, email) VALUES (?, ?, ?, ?)", c, "Alice", 40, "other@x")

	pl, _ := db.prepare("SELECT id FROM users WHERE email = ? AND name = ?", db.cat.Load())
	if !pl.idxLookup {
		t.Fatal("AND query did not plan as an index lookup")
	}
	_, rows, err := db.Query("SELECT id, name, email FROM users WHERE email = ? AND name = ?", "shared@x", "Alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str() != "Alice" || rows[0][2].Str() != "shared@x" {
		t.Fatalf("AND query returned wrong rows: %v", rows)
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE email = ? AND name = ?", "shared@x", "Carol"); len(rows) != 0 {
		t.Fatalf("AND query with no match should be empty: %v", rows)
	}
}

// Two non-unique indexes intersect: WHERE name = ? AND city = ? fetches only
// the rows matching BOTH, not the whole name bucket or the whole city bucket.
// The "1000 Peters in Amsterdam" case, at small scale.
func TestIndexIntersection(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text, age int null, city text, INDEX (name), INDEX (city))")
	ins := func(name, city string) {
		db.Exec("INSERT INTO users (id, name, age, city) VALUES (?, ?, ?, ?)", NewUUIDv7(), name, 30, city)
	}
	for i := 0; i < 10; i++ {
		ins("Peter", "Amsterdam") // both
	}
	for i := 0; i < 20; i++ {
		ins("Peter", "Rotterdam") // name only
	}
	for i := 0; i < 30; i++ {
		ins("Jan", "Amsterdam") // city only
	}
	db.mergeIndexes()

	pl, _ := db.prepare("SELECT id FROM users WHERE name = ? AND city = ?", db.cat.Load())
	if len(pl.idxCols) != 2 {
		t.Fatalf("expected both indexes used, got %d", len(pl.idxCols))
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE name = ? AND city = ?", "Peter", "Amsterdam"); len(rows) != 10 {
		t.Fatalf("intersection wrong: got %d, want 10", len(rows))
	}
	// Sanity: each bucket alone is larger than the intersection.
	if _, rows, _ := db.Query("SELECT id FROM users WHERE name = ?", "Peter"); len(rows) != 30 {
		t.Fatalf("name bucket: got %d, want 30", len(rows))
	}
	if _, rows, _ := db.Query("SELECT id FROM users WHERE city = ?", "Amsterdam"); len(rows) != 40 {
		t.Fatalf("city bucket: got %d, want 40", len(rows))
	}
	// Pre-merge (dirty overlay) must also intersect correctly.
	ins("Peter", "Amsterdam") // 11th, not yet merged
	if _, rows, _ := db.Query("SELECT id FROM users WHERE name = ? AND city = ?", "Peter", "Amsterdam"); len(rows) != 11 {
		t.Fatalf("intersection via dirty overlay: got %d, want 11", len(rows))
	}
}

// Index-assisted ORDER BY on a filtered subset: WHERE author = ? ORDER BY day
// [ASC|DESC] [LIMIT n]. The index narrows to the author's rows; the executor
// sorts that subset. Exercised both before a merge (dirty overlay) and after.
func TestIndexOrderBy(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, day int, INDEX (author))")
	for _, d := range []int{3, 1, 2} {
		db.Exec("INSERT INTO posts (id, author, day) VALUES (?, ?, ?)", NewUUIDv7(), "A", d)
	}
	db.Exec("INSERT INTO posts (id, author, day) VALUES (?, ?, ?)", NewUUIDv7(), "B", 9)

	pl, _ := db.prepare("SELECT day FROM posts WHERE author = ? ORDER BY day ASC", db.cat.Load())
	if !pl.idxLookup {
		t.Fatal("WHERE author = ? ORDER BY day did not use the index")
	}

	toDays := func(rows []Row) []int64 {
		out := make([]int64, len(rows))
		for i, r := range rows {
			out[i] = r[0].Int()
		}
		return out
	}
	eqI := func(a, b []int64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	q := func(sql string, args ...any) []int64 {
		_, rows, err := db.Query(sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		return toDays(rows)
	}

	// run the same assertions via the dirty overlay (no merge) and via the index
	for _, phase := range []string{"overlay", "merged"} {
		if phase == "merged" {
			db.mergeIndexes()
		}
		if got := q("SELECT day FROM posts WHERE author = ? ORDER BY day ASC", "A"); !eqI(got, []int64{1, 2, 3}) {
			t.Fatalf("[%s] ASC: %v", phase, got)
		}
		if got := q("SELECT day FROM posts WHERE author = ? ORDER BY day DESC", "A"); !eqI(got, []int64{3, 2, 1}) {
			t.Fatalf("[%s] DESC: %v", phase, got)
		}
		if got := q("SELECT day FROM posts WHERE author = ? ORDER BY day DESC LIMIT 2", "A"); !eqI(got, []int64{3, 2}) {
			t.Fatalf("[%s] DESC LIMIT 2: %v", phase, got)
		}
		// author B's row must not leak in
		if got := q("SELECT day FROM posts WHERE author = ? ORDER BY day ASC", "A"); len(got) != 3 {
			t.Fatalf("[%s] author A count: %v", phase, got)
		}
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
