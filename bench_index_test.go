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
