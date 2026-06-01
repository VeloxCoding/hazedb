package hazedb

import (
	"strconv"
	"testing"
	"time"
)

// A/B: pure time-trigger (threshold 0) vs size-trigger (threshold N) merger.
// The insert+delete-by-index loop is a write burst that outruns the 50ms timer,
// so time-only lets the overlay grow while size-trigger bounds it. Measures both
// the index-op cost (does a small overlay keep deletes fast?) and write
// throughput (does the extra merging slow inserts?).

func benchTriggerDB(b *testing.B, threshold int64, rows int) *DB {
	db, _ := Open(Options{Schema: Schema{}, indexMergeInterval: 50 * time.Millisecond, indexMergeThreshold: threshold, sizeHint: rows})
	db.Exec("CREATE TABLE t (id uuid primary key, email text, age int, INDEX (email))")
	for i := 0; i < rows; i++ {
		db.Exec("INSERT INTO t (id,email,age) VALUES (?,?,?)", NewUUIDv7(), "e"+strconv.Itoa(i), 1)
	}
	db.mergeIndexes()
	return db
}

func BenchmarkMergeTrigger_InsertDeleteByIndex(b *testing.B) {
	cases := []struct {
		name string
		thr  int64
	}{{"time-only", -1}, {"adaptive", 0}, {"size-1000", 1000}, {"size-250", 250}}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			db := benchTriggerDB(b, c.thr, 5000)
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				em := "k" + strconv.Itoa(i)
				db.Exec("INSERT INTO t (id,email,age) VALUES (?,?,?)", NewUUIDv7(), em, 1)
				db.Exec("DELETE FROM t WHERE email = ?", em)
			}
			b.StopTimer()
			b.ReportMetric(float64(db.totalDirty()), "overlay-end")
		})
	}
}

func BenchmarkMergeTrigger_InsertOnly(b *testing.B) {
	cases := []struct {
		name string
		thr  int64
	}{{"time-only", -1}, {"adaptive", 0}}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			db := benchTriggerDB(b, c.thr, 5000)
			defer db.Close()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				db.Exec("INSERT INTO t (id,email,age) VALUES (?,?,?)", NewUUIDv7(), "z"+strconv.Itoa(i), 1)
			}
		})
	}
}
