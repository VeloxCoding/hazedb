package hazedb

import (
	"path/filepath"
	"testing"
	"time"
)

// Insert throughput with segmented WAL + 5s rotation (rotation runs on a ticker,
// off the write path) — compare against BenchmarkInsert_WAL (single file) to see
// the per-write cost of segmented mode.
func BenchmarkInsert_WALSegmented(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N,
		WALPath: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

// Insert throughput with the full durability stack live: segmented WAL rotating
// fast and the SQLite drain loop running concurrently — measures whether the
// background drain steals write throughput.
func BenchmarkInsert_WALDrain(b *testing.B) {
	dir := b.TempDir()
	sqPath := filepath.Join(b.TempDir(), "m.db")
	db, err := Open(Options{Schema: benchSchema(), sizeHint: b.N,
		WALPath: dir, CompanionPath: sqPath,
		drainInterval: 200 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "name", i%100); err != nil {
			b.Fatal(err)
		}
	}
}

// Drain throughput: rows/sec applied to SQLite (modernc) from a sealed segment,
// one transaction per segment. Setup (inserts + rotate) is excluded from timing.
func BenchmarkDrainThroughput(b *testing.B) {
	const K = 20000
	dir := b.TempDir()
	sqPath := filepath.Join(b.TempDir(), "m.db")
	db, err := Open(Options{Schema: benchSchema(), sizeHint: K,
		WALPath: dir, CompanionPath: sqPath,
		drainInterval: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		base := i * K
		for j := 0; j < K; j++ {
			if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(base+j+1), "name", j%100); err != nil {
				b.Fatal(err)
			}
		}
		if err := db.wal.flush(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := db.drainOnce(); err != nil {
			b.Fatal(err)
		}
		total += K
	}
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(total)/secs, "rows/s")
	}
}
