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
