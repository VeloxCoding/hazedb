package hazedb

import (
	"path/filepath"
	"strconv"
	"testing"
)

// compositeIndexOf returns the (first) multi-column secondary index on the named
// table, or nil. indexFor only matches single-column indexes, so composite tests
// reach into t.indexes directly.
func compositeIndexOf(db *DB, table string) (*table, *secIndex) {
	tbl := db.cat.Load().byName[table].table
	for _, si := range tbl.indexes {
		if len(si.ordinals) > 1 {
			return tbl, si
		}
	}
	return tbl, nil
}

// tupleOf reads the indexed component cells of pk's live row, in the index's
// ordinal order — the tuple the composite key encodes.
func tupleOf(t *testing.T, tbl *table, si *secIndex, pk UUID) []Value {
	t.Helper()
	r, ok := tbl.getByPK(pk)
	if !ok {
		t.Fatalf("row %v missing", pk)
	}
	out := make([]Value, len(si.ordinals))
	for i, ord := range si.ordinals {
		out[i] = r[ord]
	}
	return out
}

// Step 2: an ORDERED composite index parses, resolves to the right ordinal list,
// and its sorted view is maintained in full tuple order after a merge.
func TestCompositeIndexResolvesAndMaintains(t *testing.T) {
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))"); err != nil {
		t.Fatal(err)
	}

	rt := resolvedTableByName(t, db, "posts")
	if len(rt.indexes) != 1 || !sameResolvedIndex(rt.indexes[0], "idx_author_title", 1, 2) || !rt.indexes[0].ordered {
		t.Fatalf("composite index resolved wrong: %+v", rt.indexes)
	}

	// Insert deliberately out of (author, title) order.
	rows := []struct{ author, title string }{
		{"bob", "zeta"}, {"alice", "beta"}, {"bob", "alpha"},
		{"alice", "alpha"}, {"carol", "x"}, {"alice", "beta"}, // dup (author,title) on purpose
	}
	for _, r := range rows {
		if _, err := db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), r.author, r.title); err != nil {
			t.Fatal(err)
		}
	}
	db.mergeIndexes()

	tbl, si := compositeIndexOf(db, "posts")
	if si == nil {
		t.Fatal("no composite index found")
	}
	snap := si.snapshot()
	if len(snap) != len(rows) {
		t.Fatalf("sorted view has %d entries, want %d", len(snap), len(rows))
	}
	// The encoded keys must be non-decreasing, and the decoded (author, title)
	// tuples must be sorted the same way — that is the whole composite contract.
	var prev []Value
	for i, e := range snap {
		if i > 0 && e.key.less(snap[i-1].key) {
			t.Fatalf("sorted view not ordered at %d: encoded key regressed", i)
		}
		tup := tupleOf(t, tbl, si, e.pk)
		if prev != nil && tupleCmp(prev, tup) > 0 {
			t.Fatalf("tuple order regressed at %d: %v after %v",
				i, fmtTuple(tup), fmtTuple(prev))
		}
		prev = tup
	}
}

// tupleCmp compares two tuples column-by-column via the scalar key order.
func tupleCmp(a, b []Value) int {
	return refTupleCmp(a, b) // reuse the reference from composite_key_test.go
}

// Step 2: a row with a NULL in any component is not indexed (mirrors the scalar
// "NULL is never indexed" rule).
func TestCompositeIndexNullComponentExcluded(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, score int null, ORDERED INDEX (author, score))")
	full := NewUUIDv7()
	db.Exec("INSERT INTO posts (id, author, score) VALUES (?, ?, ?)", full, "alice", 5)
	db.Exec("INSERT INTO posts (id, author) VALUES (?, ?)", NewUUIDv7(), "bob") // score NULL
	db.mergeIndexes()

	_, si := compositeIndexOf(db, "posts")
	snap := si.snapshot()
	if len(snap) != 1 || snap[0].pk != full {
		t.Fatalf("NULL-component row should be excluded: got %d entries", len(snap))
	}
}

// Step 2: the parser accepts ORDERED composite and rejects the hash form
// (covered for the error path by TestIndexValidationErrors; this pins the
// positive case and the exact rejection).
func TestCompositeIndexParserOrderedOnly(t *testing.T) {
	db := openEmpty(t)
	if _, err := db.Exec("CREATE TABLE a (id uuid primary key, x int, y int, ORDERED INDEX (x, y))"); err != nil {
		t.Fatalf("ORDERED composite should parse: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE b (id uuid primary key, x int, y int, INDEX (x, y))"); err == nil {
		t.Fatal("hash composite should be rejected")
	}
}

// Step 3: pinning both components plans as a composite lookup (compLookup, not
// the single-column idxLookup) and returns the right row.
func TestCompositeIndexFullPinLookup(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	want := NewUUIDv7()
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", want, "alice", "beta")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", "alpha")
	db.mergeIndexes()

	pl, err := db.prepare("SELECT id FROM posts WHERE author = ? AND title = ?", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if pl.idxLookup || !pl.compLookup {
		t.Fatalf("want composite lookup: idxLookup=%v compLookup=%v", pl.idxLookup, pl.compLookup)
	}
	_, rows, err := db.Query("SELECT id FROM posts WHERE author = ? AND title = ?", "alice", "beta")
	if err != nil || len(rows) != 1 || rows[0][0].UUID() != want {
		t.Fatalf("composite lookup returned wrong rows: %v err=%v", rows, err)
	}
}

// Step 3: WHERE on the leading column only (a prefix of length 1) plans as a
// composite lookup and returns every row under that prefix.
func TestCompositeIndexPrefixLookup(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	for _, ti := range []string{"gamma", "alpha", "beta"} {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", ti)
	}
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "bob", "x")
	db.mergeIndexes()

	pl, err := db.prepare("SELECT title FROM posts WHERE author = ?", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if !pl.compLookup || len(pl.compPrefixSrcs) != 1 {
		t.Fatalf("want compLookup with prefix len 1: compLookup=%v prefix=%d", pl.compLookup, len(pl.compPrefixSrcs))
	}
	_, rows, err := db.Query("SELECT title FROM posts WHERE author = ?", "alice")
	if err != nil || len(rows) != 3 {
		t.Fatalf("prefix lookup: got %d rows err=%v", len(rows), err)
	}
}

// Step 3: composite lookup + ORDER BY + LIMIT routes through the shared
// candidate machinery (gather + top-N sort) and returns the right ordered window.
func TestCompositeIndexLookupOrderBy(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	for _, ti := range []string{"gamma", "alpha", "delta", "beta"} {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", ti)
	}
	db.mergeIndexes()

	_, rows, err := db.Query("SELECT title FROM posts WHERE author = ? ORDER BY title DESC LIMIT 2", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0].Str() != "gamma" || rows[1][0].Str() != "delta" {
		t.Fatalf("ordered window wrong: %v", strs(rows, 0))
	}
}

// Step 3: a not-yet-merged write is found via the dirty overlay (the composite
// candidate set unions it, then the full WHERE re-checks the live row).
func TestCompositeIndexLookupDirtyOverlay(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	want := NewUUIDv7()
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", want, "alice", "beta")
	// No mergeIndexes: the row lives only in the dirty overlay.
	_, rows, err := db.Query("SELECT id FROM posts WHERE author = ? AND title = ?", "alice", "beta")
	if err != nil || len(rows) != 1 || rows[0][0].UUID() != want {
		t.Fatalf("dirty-overlay composite lookup missed the row: %v err=%v", rows, err)
	}
}

// Step 3: a NULL in the pinned prefix matches nothing.
func TestCompositeIndexLookupNullPrefix(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text null, title text, ORDERED INDEX (author, title))")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", "beta")
	db.mergeIndexes()
	if _, rows, err := db.Query("SELECT id FROM posts WHERE author = ?", nil); err != nil || len(rows) != 0 {
		t.Fatalf("NULL prefix should match nothing: %d rows err=%v", len(rows), err)
	}
}

// seedWalkPosts creates posts(author, title NOT NULL) with an (author, title)
// composite index and inserts alice's titles plus a decoy author, then merges.
func seedWalkPosts(t *testing.T, titles ...string) *DB {
	t.Helper()
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	for _, ti := range titles {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", ti)
	}
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "bob", "zzz") // decoy: must not leak
	db.mergeIndexes()
	return db
}

// Step 3b: WHERE author = ? ORDER BY title plans as a composite WALK (not a
// lookup) and returns the prefix in trailing-column order — prefix-isolated from
// other authors.
func TestCompositeWalkPlanAndOrder(t *testing.T) {
	db := seedWalkPosts(t, "delta", "alpha", "charlie", "bravo")
	pl, err := db.prepare("SELECT title FROM posts WHERE author = ? ORDER BY title", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if !pl.compWalk || pl.compLookup {
		t.Fatalf("want compWalk: compWalk=%v compLookup=%v", pl.compWalk, pl.compLookup)
	}
	_, rows, err := db.Query("SELECT title FROM posts WHERE author = ? ORDER BY title", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); len(got) != 4 || got[0] != "alpha" || got[1] != "bravo" || got[2] != "charlie" || got[3] != "delta" {
		t.Fatalf("walk order wrong: %v", got)
	}
}

// Step 3b: ASC/DESC + LIMIT + OFFSET windows off the walk.
func TestCompositeWalkWindows(t *testing.T) {
	db := seedWalkPosts(t, "delta", "alpha", "charlie", "bravo")
	check := func(sql, want string) {
		_, rows, err := db.Query(sql, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if got := strs(rows, 0); join(got) != want {
			t.Errorf("%s => %v, want %s", sql, got, want)
		}
	}
	check("SELECT title FROM posts WHERE author = ? ORDER BY title LIMIT 2", "alpha,bravo")
	check("SELECT title FROM posts WHERE author = ? ORDER BY title DESC LIMIT 2", "delta,charlie")
	check("SELECT title FROM posts WHERE author = ? ORDER BY title LIMIT 2 OFFSET 1", "bravo,charlie")
	check("SELECT title FROM posts WHERE author = ? ORDER BY title DESC LIMIT 2 OFFSET 1", "charlie,bravo")
}

// Step 3b: the walk merges the dirty overlay into the correct ORDER BY position
// — not-yet-merged writes interleave with index entries on one comparator.
func TestCompositeWalkDirtyOverlay(t *testing.T) {
	db := seedWalkPosts(t, "delta", "alpha") // merged: alpha, delta
	// Add two more for alice WITHOUT merging — they live only in the dirty overlay.
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", "charlie")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", "bravo")

	_, rows, err := db.Query("SELECT title FROM posts WHERE author = ? ORDER BY title", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); join(got) != "alpha,bravo,charlie,delta" {
		t.Fatalf("dirty+index merge order wrong: %v", got)
	}
}

// Step 3b: a nullable trailing column disqualifies the composite index (a
// title=NULL row would match WHERE author=? but be absent from the index). The
// plan must fall back (no compWalk/compLookup) and still return correct rows.
func TestCompositeWalkNullableFallsBack(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text null, ORDERED INDEX (author, title))")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), "alice", "beta")
	db.Exec("INSERT INTO posts (id, author) VALUES (?, ?)", NewUUIDv7(), "alice") // title NULL — matches WHERE
	db.mergeIndexes()

	pl, err := db.prepare("SELECT title FROM posts WHERE author = ? ORDER BY title", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if pl.compWalk || pl.compLookup {
		t.Fatalf("nullable composite must not be used: compWalk=%v compLookup=%v", pl.compWalk, pl.compLookup)
	}
	// Scan path must still see BOTH rows (incl. the title=NULL one).
	if _, rows, _ := db.Query("SELECT title FROM posts WHERE author = ?", "alice"); len(rows) != 2 {
		t.Fatalf("scan fallback dropped the NULL-title row: %d rows", len(rows))
	}
}

// Single-column analogue: an ordered index excludes NULL values, so ORDER BY a
// nullable indexed column must NOT walk the index (the walk reads only the index
// snapshot + dirty overlay, so after a merge the NULL rows vanish) — it falls
// back to scan+sort, which sees every row.
func TestOrderWalkNullableFallsBack(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE t (id uuid primary key, email text null, ORDERED INDEX (email))")
	db.Exec("INSERT INTO t (id, email) VALUES (?, ?)", NewUUIDv7(), "a")
	db.Exec("INSERT INTO t (id) VALUES (?)", NewUUIDv7()) // email NULL — excluded from the index
	db.mergeIndexes()

	pl, err := db.prepare("SELECT id, email FROM t ORDER BY email", db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if pl.orderWalk {
		t.Fatalf("ORDER BY a nullable indexed column must not orderWalk (it drops NULL rows)")
	}
	if _, rows, _ := db.Query("SELECT id, email FROM t ORDER BY email"); len(rows) != 2 {
		t.Fatalf("ORDER BY dropped the NULL-email row: got %d rows, want 2", len(rows))
	}
}

// seedJoin builds users(name indexed) + posts(author uuid, title) with the given
// posts index DDL, one "alice" (returned) and a decoy "bob", alice's titles out
// of order plus a bob post that must never leak, then merges.
func seedJoin(t *testing.T, postsIndexDDL string, titles ...string) (*DB, UUID) {
	t.Helper()
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text, INDEX (name))")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, " + postsIndexDDL + ")")
	alice := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", alice, "alice")
	bob := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", bob, "bob")
	for _, ti := range titles {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), alice, ti)
	}
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), bob, "zzz")
	db.mergeIndexes()
	return db, alice
}

const joinHeadline = "SELECT p.title FROM posts p JOIN users u ON p.author = u.id WHERE u.name = ? ORDER BY p.title"

// Step 4: the headline join plans as a probe walk and returns the probe rows in
// trailing-column order, prefix-isolated from the decoy author.
func TestJoinProbeWalkPlanAndOrder(t *testing.T) {
	db, _ := seedJoin(t, "ORDERED INDEX (author, title)", "delta", "alpha", "charlie", "bravo")
	pl, err := db.prepare(joinHeadline, db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if pl.joinPlan == nil || !pl.joinPlan.probeWalk {
		t.Fatalf("headline join should plan as a probe walk: %+v", pl.joinPlan)
	}
	_, rows, err := db.Query(joinHeadline, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); join(got) != "alpha,bravo,charlie,delta" {
		t.Fatalf("probe walk order wrong: %v", got)
	}
}

// Step 4: the probe walk must match the top-N path byte-for-byte across ASC/DESC
// and LIMIT/OFFSET — same data, composite (walk) vs single-column INDEX (top-N).
func TestJoinProbeWalkCrossCheck(t *testing.T) {
	titles := []string{"delta", "alpha", "charlie", "bravo", "echo", "foxtrot"}
	walkDB, _ := seedJoin(t, "ORDERED INDEX (author, title)", titles...)
	refDB, _ := seedJoin(t, "INDEX (author)", titles...)
	// Sanity: the two plans really take different paths.
	if pw, _ := walkDB.prepare(joinHeadline, walkDB.cat.Load()); pw.joinPlan == nil || !pw.joinPlan.probeWalk {
		t.Fatal("walkDB should use the probe walk")
	}
	if pr, _ := refDB.prepare(joinHeadline, refDB.cat.Load()); pr.joinPlan != nil && pr.joinPlan.probeWalk {
		t.Fatal("refDB should NOT use the probe walk")
	}
	for _, q := range []string{
		joinHeadline,
		joinHeadline + " LIMIT 3",
		joinHeadline + " LIMIT 2 OFFSET 1",
		joinHeadline + " DESC",
		joinHeadline + " DESC LIMIT 3",
		joinHeadline + " DESC LIMIT 2 OFFSET 3",
	} {
		_, wr, we := walkDB.Query(q, "alice")
		_, rr, re := refDB.Query(q, "alice")
		if we != nil || re != nil {
			t.Fatalf("%q: walk err=%v ref err=%v", q, we, re)
		}
		if join(strs(wr, 0)) != join(strs(rr, 0)) {
			t.Fatalf("%q: walk %v != ref %v", q, strs(wr, 0), strs(rr, 0))
		}
	}
}

// Step 4: >1 driver (two users sharing the filter name) falls back to the top-N
// path — still correct (both authors' posts, globally ordered by title).
func TestJoinProbeWalkMultiDriverFallback(t *testing.T) {
	db, _ := seedJoin(t, "ORDERED INDEX (author, title)", "delta", "bravo")
	// A second "alice": her posts must merge into the global title order.
	alice2 := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", alice2, "alice")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), alice2, "alpha")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), alice2, "charlie")
	db.mergeIndexes()
	_, rows, err := db.Query(joinHeadline, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); join(got) != "alpha,bravo,charlie,delta" {
		t.Fatalf("multi-driver fallback order wrong: %v", got)
	}
}

// Step 4: a not-yet-merged probe row appears in the correct walk position (the
// probe walk merges the dirty overlay, like the single-table walk).
func TestJoinProbeWalkDirtyOverlay(t *testing.T) {
	db, alice := seedJoin(t, "ORDERED INDEX (author, title)", "delta", "alpha")
	// Add two more for alice WITHOUT merging.
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), alice, "charlie")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), alice, "bravo")
	_, rows, err := db.Query(joinHeadline, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); join(got) != "alpha,bravo,charlie,delta" {
		t.Fatalf("probe walk dirty merge order wrong: %v", got)
	}
}

// Step (driver walk): ORDER BY a driver column with an ordered index plans as a
// driver walk and matches the materialise-then-sort path byte-for-byte across
// ASC/DESC and LIMIT/OFFSET (walk db has ORDERED INDEX (title); ref db does not).
func TestJoinDriverWalkCrossCheck(t *testing.T) {
	titles := []string{"delta", "alpha", "charlie", "bravo", "echo", "foxtrot"}
	walk, _ := seedJoin(t, "ORDERED INDEX (title)", titles...)
	ref, _ := seedJoin(t, "INDEX (author)", titles...)
	q := "SELECT p.title FROM posts p JOIN users u ON p.author = u.id ORDER BY p.title"
	if pl, _ := walk.prepare(q, walk.cat.Load()); pl.joinPlan == nil || !pl.joinPlan.driverWalk {
		t.Fatalf("walk db should use driverWalk: %+v", pl.joinPlan)
	}
	if pl, _ := ref.prepare(q, ref.cat.Load()); pl.joinPlan != nil && pl.joinPlan.driverWalk {
		t.Fatal("ref db should NOT use driverWalk")
	}
	for _, qq := range []string{q, q + " LIMIT 3", q + " LIMIT 2 OFFSET 1", q + " DESC", q + " DESC LIMIT 3", q + " DESC LIMIT 2 OFFSET 3"} {
		_, wr, we := walk.Query(qq)
		_, rr, re := ref.Query(qq)
		if we != nil || re != nil {
			t.Fatalf("%q: walk %v ref %v", qq, we, re)
		}
		if join(strs(wr, 0)) != join(strs(rr, 0)) {
			t.Fatalf("%q: walk %v != ref %v", qq, strs(wr, 0), strs(rr, 0))
		}
	}
}

// Driver walk over a LEFT join: the driver (preserved side) is walked in order,
// an orphan is NULL-padded in place, and a not-yet-merged driver row merges into
// the walk order.
func TestJoinDriverWalkLeftAndDirty(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, ORDERED INDEX (title))")
	u := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", u, "alice")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), u, "beta")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), NewUUIDv7(), "alpha") // orphan
	db.mergeIndexes()
	_, rows, err := db.Query("SELECT p.title, u.name FROM posts p LEFT JOIN users u ON p.author = u.id ORDER BY p.title")
	if err != nil || len(rows) != 2 || rows[0][0].Str() != "alpha" || !rows[0][1].IsNull() || rows[1][0].Str() != "beta" {
		t.Fatalf("LEFT driverWalk: %v err=%v", rows, err)
	}
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), u, "aaa") // dirty, sorts first
	if _, r2, _ := db.Query("SELECT p.title FROM posts p LEFT JOIN users u ON p.author = u.id ORDER BY p.title LIMIT 1"); len(r2) != 1 || r2[0][0].Str() != "aaa" {
		t.Fatalf("dirty driverWalk: want aaa first, got %v", strs(r2, 0))
	}
}

// Streaming driver: a no-WHERE no-ORDER-BY join with LIMIT must early-stop
// without materialising the whole driver — across multiple shards/chunks
// (chunk=256) — and stay correct: exact count at LIMIT, full count without, and
// LEFT must still emit the orphan miss in the streamed path.
func TestJoinStreamingDriverNoWhere(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, INDEX (author))")
	u := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", u, "alice")
	const n = 700 // > chunk (256) and spans shards: exercises chunk-resume
	for i := 0; i < n; i++ {
		db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), u, "t"+strconv.Itoa(i))
	}
	db.mergeIndexes()

	// INNER, drives posts (scanned, no WHERE/ORDER BY) and probes users by PK.
	if _, rows, err := db.Query("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id LIMIT 10"); err != nil || len(rows) != 10 {
		t.Fatalf("streamed LIMIT 10: rows=%d err=%v", len(rows), err)
	}
	if _, all, err := db.Query("SELECT p.title FROM posts p JOIN users u ON p.author = u.id"); err != nil || len(all) != n {
		t.Fatalf("streamed no-limit: rows=%d want %d err=%v", len(all), n, err)
	}

	// LEFT JOIN with an orphan post (author not in users) — the streamed path must
	// still NULL-pad the miss.
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", NewUUIDv7(), NewUUIDv7(), "orphan")
	db.mergeIndexes()
	_, lrows, err := db.Query("SELECT p.title, u.name FROM posts p LEFT JOIN users u ON p.author = u.id")
	if err != nil || len(lrows) != n+1 {
		t.Fatalf("streamed LEFT no-limit: rows=%d want %d err=%v", len(lrows), n+1, err)
	}
	orphans := 0
	for _, r := range lrows {
		if r[1].IsNull() {
			orphans++
		}
	}
	if orphans != 1 {
		t.Fatalf("streamed LEFT: want 1 orphan (NULL name), got %d", orphans)
	}
}

// seedCatJoin builds users(name) + posts(author uuid, cat, title) with the given
// posts index, one user "alice" with tech/food posts (titles out of order) — for
// driver composite-prefix-walk tests (WHERE p.cat = ? ORDER BY p.title).
func seedCatJoin(t *testing.T, postsIdx string) *DB {
	t.Helper()
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, cat text, title text, " + postsIdx + ")")
	u := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", u, "alice")
	for _, ct := range []struct{ cat, title string }{
		{"tech", "delta"}, {"tech", "alpha"}, {"tech", "charlie"}, {"tech", "bravo"}, {"food", "zzz"},
	} {
		db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, ct.cat, ct.title)
	}
	db.mergeIndexes()
	return db
}

// Driver composite-prefix walk: WHERE pins the leading column of a composite and
// ORDER BY is the next column → walk that prefix in order. Must match the
// materialise+sort path (ref: single-column INDEX (cat)) across ASC/DESC/LIMIT/OFFSET.
func TestJoinDriverCompWalkCrossCheck(t *testing.T) {
	walk := seedCatJoin(t, "ORDERED INDEX (cat, title)")
	ref := seedCatJoin(t, "INDEX (cat)")
	q := "SELECT p.title FROM posts p JOIN users u ON p.author = u.id WHERE p.cat = ? ORDER BY p.title"
	if pl, _ := walk.prepare(q, walk.cat.Load()); pl.joinPlan == nil || !pl.joinPlan.driverCompWalk {
		t.Fatalf("walk db should use driverCompWalk: %+v", pl.joinPlan)
	}
	if pl, _ := ref.prepare(q, ref.cat.Load()); pl.joinPlan != nil && pl.joinPlan.driverCompWalk {
		t.Fatal("ref db should NOT use driverCompWalk")
	}
	for _, qq := range []string{q, q + " LIMIT 2", q + " LIMIT 2 OFFSET 1", q + " DESC", q + " DESC LIMIT 2 OFFSET 1"} {
		_, wr, we := walk.Query(qq, "tech")
		_, rr, re := ref.Query(qq, "tech")
		if we != nil || re != nil {
			t.Fatalf("%q: walk %v ref %v", qq, we, re)
		}
		if join(strs(wr, 0)) != join(strs(rr, 0)) {
			t.Fatalf("%q: walk %v != ref %v", qq, strs(wr, 0), strs(rr, 0))
		}
	}
}

// Driver composite-prefix walk over a LEFT join (the codex weak spot): the
// preserved driver is walked at the cat-prefix in title order, an orphan is
// padded in place, and a not-yet-merged row merges into the walk.
func TestJoinDriverCompWalkLeft(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, cat text, title text, ORDERED INDEX (cat, title))")
	u := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", u, "alice")
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, "tech", "beta")
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), NewUUIDv7(), "tech", "alpha") // orphan
	db.mergeIndexes()
	q := "SELECT p.title, u.name FROM posts p LEFT JOIN users u ON p.author = u.id WHERE p.cat = ? ORDER BY p.title"
	if pl, _ := db.prepare(q, db.cat.Load()); pl.joinPlan == nil || !pl.joinPlan.driverCompWalk {
		t.Fatalf("LEFT should use driverCompWalk: %+v", pl.joinPlan)
	}
	_, rows, err := db.Query(q, "tech")
	if err != nil || len(rows) != 2 || rows[0][0].Str() != "alpha" || !rows[0][1].IsNull() || rows[1][0].Str() != "beta" {
		t.Fatalf("LEFT driverCompWalk: %v err=%v", rows, err)
	}
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, "tech", "aaa") // dirty
	if _, r2, _ := db.Query(q+" LIMIT 1", "tech"); len(r2) != 1 || r2[0][0].Str() != "aaa" {
		t.Fatalf("dirty driverCompWalk: want aaa first, got %v", strs(r2, 0))
	}
}

// Codex regression: driver-pushdown must fetch through a composite index's
// LEADING column (probeIndexFor), not only single-column indexes — else a driver
// filtered on a composite-only column falls back to a full scan (the slow
// composite-only case). Pins driverIdxSrc + checks correctness.
func TestJoinDriverPushdownComposite(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, cat text, title text, ORDERED INDEX (cat, title))")
	u := NewUUIDv7()
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", u, "alice")
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, "tech", "a")
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, "tech", "b")
	db.Exec("INSERT INTO posts (id, author, cat, title) VALUES (?, ?, ?, ?)", NewUUIDv7(), u, "food", "c")
	db.mergeIndexes()
	q := "SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id WHERE p.cat = ?"
	pl, err := db.prepare(q, db.cat.Load())
	if err != nil {
		t.Fatal(err)
	}
	if pl.joinPlan == nil || pl.joinPlan.driverIdxSrc == nil {
		t.Fatalf("driver filter on a composite leading column must push down (driverIdxSrc set): %+v", pl.joinPlan)
	}
	if _, rows, err := db.Query(q, "tech"); err != nil || len(rows) != 2 {
		t.Fatalf("composite-leading pushdown wrong: rows=%d err=%v", len(rows), err)
	}
}

// join concatenates string column values with commas (test readability).
func join(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

// Step 2: a composite index survives a restart through the catalog WAL record
// (the index section now carries N columns).
func TestCompositeIndexSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "comp.wal")
	db, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, title text, ORDERED INDEX (author, title))")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(Options{Schema: Schema{}, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	rt := resolvedTableByName(t, db2, "posts")
	if len(rt.indexes) != 1 || !sameResolvedIndex(rt.indexes[0], "idx_author_title", 1, 2) || !rt.indexes[0].ordered {
		t.Fatalf("composite index lost or changed after restart: %+v", rt.indexes)
	}
}
