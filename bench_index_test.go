package hazedb

import (
	"strconv"
	"testing"
)

// S9 benchmarks for secondary indexes:
//   read    — index point lookup vs full scan, same data
//   write   — insert overhead of the per-shard dirty mark (with vs without)
//   merge   — cost of reconciling N dirty rows into the index

func benchIndexSeed(b *testing.B, withIndex bool, n int) (*DB, []string) {
	idx := ""
	if withIndex {
		idx = ", INDEX (email)"
	}
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: -1, SizeHint: n})
	if err != nil {
		b.Fatal(err)
	}
	db.Exec("CREATE TABLE t (id uuid primary key, email text" + idx + ")")
	emails := make([]string, n)
	for i := 0; i < n; i++ {
		emails[i] = "user" + strconv.Itoa(i) + "@x"
		db.Exec("INSERT INTO t (id, email) VALUES (?, ?)", NewUUIDv7(), emails[i])
	}
	if withIndex {
		db.mergeIndexes()
	}
	return db, emails
}

func benchIndexRead(b *testing.B, withIndex bool, n int) {
	db, emails := benchIndexSeed(b, withIndex, n)
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT email FROM t WHERE email = ?", emails[i%n])
	}
}

// Indexed lookup is O(1)+re-check; the scan is O(n) over the whole table.
func BenchmarkIndexRead_Indexed_10k(b *testing.B) { benchIndexRead(b, true, 10000) }
func BenchmarkIndexRead_Scan_10k(b *testing.B)    { benchIndexRead(b, false, 10000) }

// QueryRow on an indexed column goes through the lean single-row path
// (execSelectIdxOne) — no []Row slice. Compare to the Query (multi-row) bench.
func BenchmarkIndexReadOne_10k(b *testing.B) {
	db, emails := benchIndexSeed(b, true, 10000)
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.QueryRow("SELECT email FROM t WHERE email = ?", emails[i%10000])
	}
}

func benchIndexInsert(b *testing.B, withIndex bool) {
	idx := ""
	if withIndex {
		idx = ", INDEX (email)"
	}
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: -1, SizeHint: b.N})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE t (id uuid primary key, email text" + idx + ")")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO t (id, email) VALUES (?, ?)", NewUUIDv7(), "u")
	}
}

// The delta is the per-shard dirty mark (one append under the held shard lock).
func BenchmarkIndexInsert_WithIndex(b *testing.B)    { benchIndexInsert(b, true) }
func BenchmarkIndexInsert_WithoutIndex(b *testing.B) { benchIndexInsert(b, false) }

// Merge cost over a backlog of dirty rows (boot/rebuild-scale work).
func benchIndexMerge(b *testing.B, n int) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: -1, SizeHint: n})
		if err != nil {
			b.Fatal(err)
		}
		db.Exec("CREATE TABLE t (id uuid primary key, name text, email text, INDEX (email), INDEX (name))")
		for j := 0; j < n; j++ {
			db.Exec("INSERT INTO t (id, name, email) VALUES (?, ?, ?)", NewUUIDv7(), "n"+strconv.Itoa(j%100), "u"+strconv.Itoa(j))
		}
		b.StartTimer()
		db.mergeIndexes()
		b.StopTimer()
		db.Close()
	}
}

func BenchmarkIndexMerge_10k(b *testing.B) { benchIndexMerge(b, 10000) }
func BenchmarkIndexMerge_50k(b *testing.B) { benchIndexMerge(b, 50000) }

// Index-assisted ORDER BY on a filtered list: WHERE author = ? ORDER BY day
// DESC LIMIT 20. Varying the author's post count shows whether the cost scales
// with the matched set (gather-all-then-sort) or just the LIMIT.
func benchIndexOrderBy(b *testing.B, perAuthor int) {
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: -1, SizeHint: perAuthor * 2})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE posts (id uuid primary key, author text, day int, INDEX (author))")
	for _, author := range []string{"A", "B"} { // B is noise in another bucket
		for i := 0; i < perAuthor; i++ {
			db.Exec("INSERT INTO posts (id, author, day) VALUES (?, ?, ?)", NewUUIDv7(), author, i)
		}
	}
	db.mergeIndexes()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rows, err := db.Query("SELECT id, author, day FROM posts WHERE author = ? ORDER BY day DESC LIMIT 20", "A")
		if err != nil || len(rows) != 20 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}

func BenchmarkIndexOrderBy_50(b *testing.B)   { benchIndexOrderBy(b, 50) }
func BenchmarkIndexOrderBy_5000(b *testing.B) { benchIndexOrderBy(b, 5000) }

// Two non-unique indexes intersected: WHERE name = ? AND city = ?. ~1/6 of 50k
// rows are "Peter" (~8300) spread over 8 cities, so ~1040 Peters per city. The
// intersection fetches only that ~1040, not the whole ~8300 "Peter" bucket a
// single-index plan would walk.
func BenchmarkIndexIntersect_50k(b *testing.B) {
	const n = 50000
	db, err := Open(Options{Schema: Schema{}, IndexMergeInterval: -1, SizeHint: n})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.Exec("CREATE TABLE users (id uuid primary key, name text, city text, INDEX (name), INDEX (city))")
	cities := []string{"AMS", "RTM", "UTR", "DHG", "EIN", "GRN", "TIL", "ALM"}
	for i := 0; i < n; i++ {
		name := "other" + strconv.Itoa(i%40)
		if i%6 == 0 {
			name = "Peter"
		}
		db.Exec("INSERT INTO users (id, name, city) VALUES (?, ?, ?)", NewUUIDv7(), name, cities[i%8])
	}
	db.mergeIndexes()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, rows, err := db.Query("SELECT id, name, city FROM users WHERE name = ? AND city = ?", "Peter", "AMS"); err != nil || len(rows) == 0 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
}
