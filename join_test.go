package hazedb

import (
	"errors"
	"testing"
)

// joinFixture builds posts (FK author → users.id PK) + users, with a secondary
// index on posts.author so both probe directions are available. Returns the DB.
func joinFixture(t *testing.T) *DB {
	t.Helper()
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, INDEX (author))")
	// three users
	for i := 1; i <= 3; i++ {
		db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(i), "user"+string(rune('0'+i)))
	}
	// posts: u1 has 2 posts, u2 has 1, u3 has none; one orphan post (author tid(9))
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", tid(101), tid(1), "p1a")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", tid(102), tid(1), "p1b")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", tid(103), tid(2), "p2a")
	db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)", tid(104), tid(9), "orphan")
	db.mergeIndexes()
	return db
}

// strs pulls one string column from every row.
func strs(rows []Row, col int) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r[col].Str()
	}
	return out
}

func eqStrSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

// INNER JOIN probing the parent PK (posts.author → users.id). Each post with a
// real author matches exactly one user; the orphan post drops.
func TestJoinInnerPK(t *testing.T) {
	db := joinFixture(t)
	cols, rows, err := db.Query("SELECT title, name FROM posts JOIN users ON posts.author = users.id ORDER BY title")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0] != "title" || cols[1] != "name" {
		t.Fatalf("cols=%v", cols)
	}
	// p1a,p1b → user1; p2a → user2; orphan drops.
	if got := strs(rows, 0); !eqStrSet(got, []string{"p1a", "p1b", "p2a"}) {
		t.Fatalf("titles=%v", got)
	}
	if rows[0][1].Str() != "user1" || rows[2][1].Str() != "user2" {
		t.Fatalf("names wrong: %v", strs(rows, 1))
	}
}

// LEFT JOIN preserves every left (posts) row; the orphan keeps a NULL name.
func TestJoinLeftKeepsUnmatched(t *testing.T) {
	db := joinFixture(t)
	_, rows, err := db.Query("SELECT title, name FROM posts LEFT JOIN users ON posts.author = users.id ORDER BY title")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 { // all four posts, including the orphan
		t.Fatalf("want 4 rows, got %d (%v)", len(rows), strs(rows, 0))
	}
	// orphan sorts first ("orphan" < "p..."): NULL name.
	if rows[0][0].Str() != "orphan" || !rows[0][1].IsNull() {
		t.Fatalf("orphan row should have NULL name: %v", rows[0])
	}

	// The explicit LEFT OUTER JOIN spelling parses and behaves identically.
	_, rows2, err := db.Query("SELECT title, name FROM posts LEFT OUTER JOIN users ON posts.author = users.id ORDER BY title")
	if err != nil {
		t.Fatalf("LEFT OUTER JOIN: %v", err)
	}
	if len(rows2) != 4 || rows2[0][0].Str() != "orphan" || !rows2[0][1].IsNull() {
		t.Fatalf("LEFT OUTER JOIN mismatch: %d rows, first=%v", len(rows2), rows2[0])
	}
}

// RIGHT JOIN preserves the right table (users). user3 has no posts → a row with
// NULL post columns; the orphan post (author not a user) is on the left, so it
// is dropped. Mirror of LEFT.
func TestJoinRight(t *testing.T) {
	db := joinFixture(t)
	_, rows, err := db.Query("SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id ORDER BY u.name")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 { // u1×2, u2×1, u3×1(null)
		t.Fatalf("want 4 rows, got %d (%v)", len(rows), strs(rows, 0))
	}
	found := false
	for _, r := range rows {
		if r[0].Str() == "user3" {
			found = true
			if !r[1].IsNull() {
				t.Fatalf("user3 has no posts → title must be NULL, got %v", r[1])
			}
		}
	}
	if !found {
		t.Fatal("user3 (no posts) missing from RIGHT JOIN result")
	}

	// RIGHT OUTER spelling parses + behaves identically.
	if _, r2, err := db.Query("SELECT u.name, p.title FROM posts p RIGHT OUTER JOIN users u ON p.author = u.id"); err != nil || len(r2) != 4 {
		t.Fatalf("RIGHT OUTER JOIN: rows=%d err=%v", len(r2), err)
	}

	// Indexed-only law: RIGHT probes the LEFT side, so the left join column must
	// be indexed.
	db.Exec("CREATE TABLE notes (id uuid primary key, tag text)") // tag not indexed
	if _, _, err := db.Query("SELECT u.name FROM notes n RIGHT JOIN users u ON n.tag = u.id"); !errors.Is(err, ErrUnindexedJoin) {
		t.Fatalf("RIGHT JOIN on unindexed left: want ErrUnindexedJoin, got %v", err)
	}
}

// RIGHT join with WHERE (preserved side + the nullable-side gotcha), ORDER BY /
// LIMIT / OFFSET, and QueryJSON NULL rendering.
func TestJoinRightWhereOrderJSON(t *testing.T) {
	db := joinFixture(t)
	// WHERE on the preserved (right/users) side → pushed down to the driver.
	if _, rows, err := db.Query("SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id WHERE u.name = ?", "user1"); err != nil || len(rows) != 2 {
		t.Fatalf("RIGHT + WHERE preserved side: rows=%d err=%v", len(rows), err)
	}
	// WHERE on the nullable (left/posts) side drops the NULL-padded row — RIGHT
	// degrades to INNER for that predicate: only user2/p2a survives.
	if _, rows, err := db.Query("SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id WHERE p.title = ?", "p2a"); err != nil || len(rows) != 1 || rows[0][0].Str() != "user2" {
		t.Fatalf("RIGHT + WHERE nullable side: rows=%d %v err=%v", len(rows), strs(rows, 0), err)
	}
	// ORDER BY + LIMIT + OFFSET on the joined result: names sorted
	// [user1,user1,user2,user3], OFFSET 1 LIMIT 2 → [user1,user2].
	_, rows, err := db.Query("SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id ORDER BY u.name LIMIT 2 OFFSET 1")
	if err != nil || len(rows) != 2 || rows[0][0].Str() != "user1" || rows[1][0].Str() != "user2" {
		t.Fatalf("RIGHT + order/limit/offset: %v err=%v", strs(rows, 0), err)
	}
	// QueryJSON renders the NULL-padded user3 (no posts) row as JSON null.
	_, body, err := db.QueryJSON("SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id WHERE u.name = ?", Str("user3"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `[{"name":"user3","title":null}]` {
		t.Fatalf("RIGHT QueryJSON null: got %s", body)
	}
}

// Driving the other direction: users JOIN posts ON users.id = posts.author. The
// probe is posts.author (a secondary index), exercising the index probe +
// dirty/stale re-check path.
func TestJoinInnerSecondaryIndexProbe(t *testing.T) {
	db := joinFixture(t)
	_, rows, err := db.Query("SELECT name, title FROM users JOIN posts ON users.id = posts.author")
	if err != nil {
		t.Fatal(err)
	}
	// user1 (2 posts) + user2 (1) = 3 rows; user3 (no posts) absent.
	if got := strs(rows, 1); !eqStrSet(got, []string{"p1a", "p1b", "p2a"}) {
		t.Fatalf("titles=%v", got)
	}
}

// Aliases + qualified columns + a cross-table WHERE on the joined row.
func TestJoinAliasesAndWhere(t *testing.T) {
	db := joinFixture(t)
	_, rows, err := db.Query("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id WHERE u.name = ?", "user1")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); !eqStrSet(got, []string{"p1a", "p1b"}) {
		t.Fatalf("WHERE on right table: %v", got)
	}
}

// ORDER BY / LIMIT / OFFSET apply to the joined result.
func TestJoinOrderLimitOffset(t *testing.T) {
	db := joinFixture(t)
	_, rows, err := db.Query("SELECT p.title FROM posts p JOIN users u ON p.author = u.id ORDER BY p.title LIMIT 1 OFFSET 1")
	if err != nil {
		t.Fatal(err)
	}
	// matched titles sorted: p1a,p1b,p2a → OFFSET 1 LIMIT 1 → p1b.
	if len(rows) != 1 || rows[0][0].Str() != "p1b" {
		t.Fatalf("order/limit/offset: %v", strs(rows, 0))
	}
}

// Regression: when a composite-prefix walk drives the join, a driver WHERE
// equality on an indexed column OUTSIDE the walked prefix must still filter. The
// pushdown picks that column as the fetch key and drops it from driverPreds, but
// the composite walk does NOT run the index fetch — so the equality has to be
// re-checked by passDriver, or rows failing it leak into the result.
func TestJoinCompositeWalkEnforcesNonPrefixFilter(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	// region: a single secondary index — first in the WHERE, so the pushdown picks
	// it as the fetch key. (status, created): the ordered composite the driver walks
	// for ORDER BY created. region is NOT part of that index.
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, region text, status text, created int, title text, INDEX (region), ORDERED INDEX (status, created))")
	db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(1), "u1")
	// Two posts, same author + status (so both fall in the walked prefix), DIFFERENT
	// region. WHERE region='eu' must keep only the first.
	db.Exec("INSERT INTO posts (id, author, region, status, created, title) VALUES (?, ?, ?, ?, ?, ?)", tid(101), tid(1), "eu", "open", 1, "keep")
	db.Exec("INSERT INTO posts (id, author, region, status, created, title) VALUES (?, ?, ?, ?, ?, ?)", tid(102), tid(1), "us", "open", 2, "drop")
	db.mergeIndexes()

	_, rows, err := db.Query(
		"SELECT p.title FROM posts p JOIN users u ON p.author = u.id WHERE p.region = ? AND p.status = ? ORDER BY p.created",
		"eu", "open")
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(rows, 0); !eqStrSet(got, []string{"keep"}) {
		t.Fatalf("region filter not enforced under composite walk: titles=%v (want [keep])", got)
	}
}

// SELECT * over a join yields both tables' columns, qualified to avoid collision.
func TestJoinStarColumns(t *testing.T) {
	db := joinFixture(t)
	cols, rows, err := db.Query("SELECT * FROM posts p JOIN users u ON p.author = u.id")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"p.id", "p.author", "p.title", "u.id", "u.name"}
	if len(cols) != len(want) {
		t.Fatalf("star cols=%v", cols)
	}
	for i := range want {
		if cols[i] != want[i] {
			t.Fatalf("star col %d = %q, want %q", i, cols[i], want[i])
		}
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 matched rows, got %d", len(rows))
	}
}

// The indexed-only law: a join on a non-indexed, non-PK column is rejected at
// plan time rather than doing an O(A×B) scan.
func TestJoinUnindexedRejected(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE a (id uuid primary key, tag text)")           // tag not indexed
	db.Exec("CREATE TABLE b (id uuid primary key, tag text)")           // tag not indexed
	_, _, err := db.Query("SELECT a.id FROM a JOIN b ON a.tag = b.tag") // neither side indexed on tag
	if !errors.Is(err, ErrUnindexedJoin) {
		t.Fatalf("want ErrUnindexedJoin, got %v", err)
	}
	// LEFT JOIN: right side's join column must be indexed even if the left is.
	db.Exec("CREATE TABLE c (id uuid primary key, ref uuid, INDEX (ref))")
	if _, _, err := db.Query("SELECT c.id FROM c LEFT JOIN b ON c.ref = b.tag"); !errors.Is(err, ErrUnindexedJoin) {
		t.Fatalf("LEFT JOIN on unindexed right: want ErrUnindexedJoin, got %v", err)
	}
}

// Ambiguity + resolution errors are reported at plan time.
func TestJoinResolutionErrors(t *testing.T) {
	db := joinFixture(t)
	// "id" exists in both posts and users → ambiguous unqualified ref.
	if _, _, err := db.Query("SELECT id FROM posts JOIN users ON posts.author = users.id"); err == nil {
		t.Fatal("ambiguous unqualified column should error")
	}
	// ON comparing two columns of the same table.
	if _, _, err := db.Query("SELECT posts.title FROM posts JOIN users ON posts.id = posts.author"); err == nil {
		t.Fatal("ON must compare one column from each table")
	}
	// unknown qualifier.
	if _, _, err := db.Query("SELECT x.title FROM posts JOIN users ON posts.author = users.id"); err == nil {
		t.Fatal("unknown table qualifier should error")
	}
}

// QueryJSON (streaming surface) over a join materialises and honours the join.
func TestJoinQueryJSON(t *testing.T) {
	db := joinFixture(t)
	_, body, err := db.QueryJSON("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id WHERE u.name = ?", Str("user2"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	want := `[{"title":"p2a","name":"user2"}]`
	if got != want {
		t.Fatalf("QueryJSON join: got %s want %s", got, want)
	}
}
