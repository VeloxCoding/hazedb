package hazedb

import (
	"sync/atomic"
	"testing"
)

// Parallel benchmarks — verify that the shard fan-out scales with
// available cores. Reads should land in different shards because
// b.RunParallel rotates over many goroutines hitting different IDs.

func BenchmarkSelectByPK_Parallel(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddInt64(&counter, 1) * 997
		i := int(base)
		for pb.Next() {
			db.Query("SELECT name, age FROM users WHERE id = ?", tid(i%N))
			i++
		}
	})
}

func BenchmarkInsert_Parallel_Mem(b *testing.B) {
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: 2 * 1024 * 1024})
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddInt64(&counter, 1) * 100000
		i := int(base)
		for pb.Next() {
			db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
			i++
		}
	})
}

func BenchmarkInsert_Parallel_WAL(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: 2 * 1024 * 1024, WALPath: dir + "/bench.wal"})
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddInt64(&counter, 1) * 100000
		i := int(base)
		for pb.Next() {
			db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100)
			i++
		}
	})
}

func BenchmarkUpdateByPK_Parallel(b *testing.B) {
	const N = 10000
	db := newBenchDB(b, N)
	b.ResetTimer()
	b.ReportAllocs()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddInt64(&counter, 1) * 997
		i := int(base)
		for pb.Next() {
			db.Exec("UPDATE users SET age = ? WHERE id = ?", i%100, tid(i%N))
			i++
		}
	})
}
