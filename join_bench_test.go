package hazedb

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// Join benchmarks at a realistic forum scale: 200 users, 20k posts (~100/user),
// indexes on users.id (PK) and posts.author. The headline query:
//
//	SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id
//	WHERE u.name = ? ORDER BY p.title LIMIT 10 OFFSET 20
//
// compared against the same query on in-memory SQLite (cgo, its best case). The
// datasets are built once and shared across the benchmark's re-runs.

const (
	joinUsers = 200
	joinPosts = 20000
	joinName  = "user0007" // the WHERE filter value (one user, ~100 posts)
)

var (
	joinHzOnce sync.Once
	joinHzDB   *DB
	joinSqOnce sync.Once
	joinSqDB   *sql.DB
)

func joinHazedb(b *testing.B) *DB {
	joinHzOnce.Do(func() {
		db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1})
		if err != nil {
			b.Fatal(err)
		}
		db.Exec("CREATE TABLE users (id uuid primary key, name text, INDEX (name))")
		db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, INDEX (author))")
		for i := 0; i < joinUsers; i++ {
			db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(i+1), fmt.Sprintf("user%04d", i))
		}
		for j := 0; j < joinPosts; j++ {
			db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)",
				tid(1_000_000+j), tid((j%joinUsers)+1), fmt.Sprintf("t%06d", j))
		}
		db.mergeIndexes()
		joinHzDB = db
	})
	return joinHzDB
}

func joinSQLite(b *testing.B) *sql.DB {
	joinSqOnce.Do(func() {
		d, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			b.Fatal(err)
		}
		d.SetMaxOpenConns(1)
		mustExec := func(q string, args ...any) {
			if _, err := d.Exec(q, args...); err != nil {
				b.Fatal(err)
			}
		}
		mustExec("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT)")
		mustExec("CREATE TABLE posts (id BLOB PRIMARY KEY, author BLOB, title TEXT)")
		mustExec("CREATE INDEX idx_posts_author ON posts(author)")
		mustExec("CREATE INDEX idx_users_name ON users(name)")
		// Composite so SQLite can serve WHERE author=? ORDER BY title without a sort
		// — the fair counterpart to hazedb's ORDERED INDEX (author, title). SQLite's
		// planner picks the best index per query (PK for point, this for the feed).
		mustExec("CREATE INDEX idx_posts_author_title ON posts(author, title)")
		// Plain (title) so SQLite can serve a global ORDER BY title LIMIT by walking
		// the index — the fair counterpart to hazedb's ORDERED INDEX (title) driver walk.
		mustExec("CREATE INDEX idx_posts_title ON posts(title)")
		for i := 0; i < joinUsers; i++ {
			mustExec("INSERT INTO users (id, name) VALUES (?, ?)", key16(i+1), fmt.Sprintf("user%04d", i))
		}
		for j := 0; j < joinPosts; j++ {
			mustExec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)",
				key16(1_000_000+j), key16((j%joinUsers)+1), fmt.Sprintf("t%06d", j))
		}
		joinSqDB = d
	})
	return joinSqDB
}

const joinFilteredSQL = "SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id WHERE u.name = ? ORDER BY p.title LIMIT 10 OFFSET 20"

// The exact query, hazedb.
func BenchmarkJoinFiltered_Hazedb(b *testing.B) {
	db := joinHazedb(b)
	arg := Str(joinName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.QueryValues(joinFilteredSQL, arg)
		if err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

var (
	joinHzCompOnce sync.Once
	joinHzCompDB   *DB
)

// Same dataset as joinHazedb but posts carries ORDERED INDEX (author, title), so
// the headline query plans as a single-driver probe walk instead of gather +
// top-N sort.
func joinHazedbComposite(b *testing.B) *DB {
	joinHzCompOnce.Do(func() {
		db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1})
		if err != nil {
			b.Fatal(err)
		}
		db.Exec("CREATE TABLE users (id uuid primary key, name text, INDEX (name))")
		db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, ORDERED INDEX (author, title))")
		for i := 0; i < joinUsers; i++ {
			db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(i+1), fmt.Sprintf("user%04d", i))
		}
		for j := 0; j < joinPosts; j++ {
			db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)",
				tid(1_000_000+j), tid((j%joinUsers)+1), fmt.Sprintf("t%06d", j))
		}
		db.mergeIndexes()
		joinHzCompDB = db
	})
	return joinHzCompDB
}

var (
	joinHzTitleOnce sync.Once
	joinHzTitleDB   *DB
)

// Same dataset as joinHazedb but posts carries ORDERED INDEX (title), so a global
// ORDER BY p.title (no WHERE) plans as a driver walk: walk posts in title order,
// probe each by PK, stop at offset+limit — no materialise + sort.
func joinHazedbTitleIdx(b *testing.B) *DB {
	joinHzTitleOnce.Do(func() {
		db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1})
		if err != nil {
			b.Fatal(err)
		}
		db.Exec("CREATE TABLE users (id uuid primary key, name text, INDEX (name))")
		db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, title text, ORDERED INDEX (title))")
		for i := 0; i < joinUsers; i++ {
			db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(i+1), fmt.Sprintf("user%04d", i))
		}
		for j := 0; j < joinPosts; j++ {
			db.Exec("INSERT INTO posts (id, author, title) VALUES (?, ?, ?)",
				tid(1_000_000+j), tid((j%joinUsers)+1), fmt.Sprintf("t%06d", j))
		}
		db.mergeIndexes()
		joinHzTitleDB = db
	})
	return joinHzTitleDB
}

// Global ORDER BY with an ordered index on the sort column — the driver walk.
func BenchmarkJoinOrderedNoWhere_HazedbTitleIdx(b *testing.B) {
	db := joinHazedbTitleIdx(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.QueryValues("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id ORDER BY p.title LIMIT 10 OFFSET 20")
		if err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

// The exact query, hazedb with a composite (author, title) index — probe walk.
func BenchmarkJoinFiltered_HazedbComposite(b *testing.B) {
	db := joinHazedbComposite(b)
	arg := Str(joinName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.QueryValues(joinFilteredSQL, arg)
		if err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

// The exact query, in-memory SQLite (cgo).
func BenchmarkJoinFiltered_SQLiteMem(b *testing.B) {
	d := joinSQLite(b)
	stmt, err := d.Prepare(joinFilteredSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(joinName)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var title, name string
			if err := rows.Scan(&title, &name); err != nil {
				b.Fatal(err)
			}
			n++
		}
		rows.Close()
		if n != 10 {
			b.Fatalf("rows=%d", n)
		}
	}
}

// LEFT join filtered on the driver (posts) with ORDER BY the trailing column:
// SELECT p.title, u.name FROM posts p LEFT JOIN users u ON p.author = u.id
// WHERE p.author = ? ORDER BY p.title LIMIT 10 OFFSET 20.
// On the composite (author,title) dataset this is the driver composite-prefix
// walk; on the single-column INDEX(author) dataset it is fetch-author-then-sort.
const joinLeftSQL = "SELECT p.title, u.name FROM posts p LEFT JOIN users u ON p.author = u.id WHERE p.author = ? ORDER BY p.title LIMIT 10 OFFSET 20"

func BenchmarkJoinLeftFiltered_Hazedb(b *testing.B) { // single-col INDEX(author): fetch + sort
	db := joinHazedb(b)
	arg := UUIDVal(tid(8)) // user0007's id; ~100 posts
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, rows, err := db.QueryValues(joinLeftSQL, arg); err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkJoinLeftFiltered_HazedbComposite(b *testing.B) { // ORDERED INDEX(author,title): driver walk
	db := joinHazedbComposite(b)
	arg := UUIDVal(tid(8))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, rows, err := db.QueryValues(joinLeftSQL, arg); err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkJoinLeftFiltered_SQLiteMem(b *testing.B) {
	stmt, err := joinSQLite(b).Prepare(joinLeftSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	arg := key16(8)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		scanSQLiteJoin(b, stmt, 10, arg)
	}
}

// No WHERE, no ORDER BY, just LIMIT — the cheapest join shape (can stop early in
// phase 2), but v1 still materialises the whole driver in phase 1.
func BenchmarkJoinNoWhereLimit_Hazedb(b *testing.B) {
	db := joinHazedb(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.QueryValues("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id LIMIT 10")
		if err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

// RIGHT join mirror of the filtered query: preserve users, filter to one user
// (pushed down via the name index → drive users), probe its ~100 posts. Should
// track the INNER filtered number since the matched work is identical.
const joinRightSQL = "SELECT u.name, p.title FROM posts p RIGHT JOIN users u ON p.author = u.id WHERE u.name = ? ORDER BY p.title LIMIT 10 OFFSET 20"

func BenchmarkJoinRightFiltered_Hazedb(b *testing.B) {
	db := joinHazedb(b)
	arg := Str(joinName)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, rows, err := db.QueryValues(joinRightSQL, arg); err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkJoinRightFiltered_SQLiteMem(b *testing.B) {
	d := joinSQLite(b)
	stmt, err := d.Prepare(joinRightSQL)
	if err != nil {
		b.Skipf("SQLite RIGHT JOIN unsupported in this build: %v", err)
	}
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(joinName)
		if err != nil {
			b.Skipf("SQLite RIGHT JOIN: %v", err)
		}
		n := 0
		for rows.Next() {
			var title, name string
			rows.Scan(&name, &title)
			n++
		}
		rows.Close()
		if n != 10 {
			b.Fatalf("rows=%d", n)
		}
	}
}

// Low-fanout point join: one post by PK + its author by PK (1×1). hazedb's
// wheelhouse — two in-process point reads, no sort.
const joinPointSQL = "SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id WHERE p.id = ?"

func BenchmarkJoinPoint_Hazedb(b *testing.B) {
	db := joinHazedb(b)
	arg := UUIDVal(tid(1_000_000 + 5))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, rows, err := db.QueryValues(joinPointSQL, arg); err != nil || len(rows) != 1 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkJoinPoint_SQLiteMem(b *testing.B) {
	d := joinSQLite(b)
	stmt, err := d.Prepare(joinPointSQL)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	arg := key16(1_000_000 + 5)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(arg)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for rows.Next() {
			var title, name string
			rows.Scan(&title, &name)
			n++
		}
		rows.Close()
		if n != 1 {
			b.Fatalf("rows=%d", n)
		}
	}
}

// No WHERE, ORDER BY + LIMIT/OFFSET: materialises the full join then sorts.
func BenchmarkJoinOrderedNoWhere_Hazedb(b *testing.B) {
	db := joinHazedb(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.QueryValues("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id ORDER BY p.title LIMIT 10 OFFSET 20")
		if err != nil || len(rows) != 10 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

// scanSQLiteJoin runs q on the shared SQLite db and counts (title,name) rows,
// asserting want — the read loop every SQLite join bench shares.
func scanSQLiteJoin(b *testing.B, stmt *sql.Stmt, want int, args ...any) {
	rows, err := stmt.Query(args...)
	if err != nil {
		b.Fatal(err)
	}
	n := 0
	for rows.Next() {
		var a, c string
		rows.Scan(&a, &c)
		n++
	}
	rows.Close()
	if n != want {
		b.Fatalf("rows=%d want %d", n, want)
	}
}

func BenchmarkJoinNoWhereLimit_SQLiteMem(b *testing.B) {
	stmt, err := joinSQLite(b).Prepare("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id LIMIT 10")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		scanSQLiteJoin(b, stmt, 10)
	}
}

func BenchmarkJoinOrderedNoWhere_SQLiteMem(b *testing.B) {
	stmt, err := joinSQLite(b).Prepare("SELECT p.title, u.name FROM posts p JOIN users u ON p.author = u.id ORDER BY p.title LIMIT 10 OFFSET 20")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		scanSQLiteJoin(b, stmt, 10)
	}
}

// BenchmarkJoinDirtyOverlay drives many rows against an indexed probe table whose
// dirty overlay is fully populated (merge disabled), the shape that made the
// per-probe dirtyPKs() re-snapshot cost O(driver × dirty): a LEFT JOIN drives all
// `users` rows and probes posts.author (a secondary index, so the overlay path)
// once per driver row. With the snapshot hoisted out of the probe loop the
// overlay is copied once, not per probe.
func BenchmarkJoinDirtyOverlay(b *testing.B) {
	const (
		users = 1000
		posts = 5000
	)
	db, err := Open(Options{Schema: Schema{}, indexMergeInterval: -1}) // merge off → overlay stays full
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE users (id uuid primary key, name text)")
	db.Exec("CREATE TABLE posts (id uuid primary key, author uuid, INDEX (author))")
	for i := 0; i < users; i++ {
		db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", tid(i), "u")
	}
	for i := 0; i < posts; i++ {
		db.Exec("INSERT INTO posts (id, author) VALUES (?, ?)", tid(1_000_000+i), tid(i%users))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, _, err := db.Query("SELECT users.name FROM users LEFT JOIN posts ON posts.author = users.id"); err != nil {
			b.Fatal(err)
		}
	}
}
