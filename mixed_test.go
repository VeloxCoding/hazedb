package hazedb

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMixedWorkloadLatency measures p50 / p99 / p999 of a SELECT-by-PK
// under concurrent insert pressure, via the SQL interpreter path. The
// number lines up with the FASTSQL v1 RFC headline claim
// ("tail-scan p99 < 50 µs under mixed concurrent workload"), though
// the spike measured a *tail-scan* (ORDER BY DESC LIMIT N) — here we
// measure SELECT * WHERE pk = ?, which is the SQL-engine equivalent.
//
// Run as a test (not a bench) so output renders normally.
// `go test -run TestMixedWorkloadLatency -v ./fastsql/...`
func TestMixedWorkloadLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("mixed workload test takes ~3s")
	}
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: 100_000})
	defer db.Close()

	// Pre-seed
	const seedN = 50_000
	for i := 0; i < seedN; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "seed", i%100)
	}

	const writers = 4
	const readers = 16
	const dur = 2 * time.Second

	var insertN, readN int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers (each owns a non-overlapping id range starting beyond the seed).
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			id := seedN + seed*1_000_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "n", id%100)
				if err == nil {
					atomic.AddInt64(&insertN, 1)
				}
				id++
			}
		}(w)
	}

	// Readers: record each read latency in a per-goroutine slice; merge afterwards.
	perReader := make([][]int64, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lats := make([]int64, 0, 1<<20)
			rng := rand.New(rand.NewSource(int64(idx) + 1))
			for {
				select {
				case <-stop:
					perReader[idx] = lats
					return
				default:
				}
				id := rng.Intn(seedN)
				t0 := time.Now()
				_, _, err := db.Query("SELECT name, age FROM users WHERE id = ?", id)
				dt := time.Since(t0).Nanoseconds()
				if err == nil {
					lats = append(lats, dt)
					atomic.AddInt64(&readN, 1)
				}
			}
		}(r)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()

	// Merge latencies.
	var all []int64
	for _, s := range perReader {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(q float64) int64 {
		if len(all) == 0 {
			return 0
		}
		idx := int(float64(len(all)) * q)
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return all[idx]
	}
	t.Logf("writers=%d readers=%d duration=%v", writers, readers, dur)
	t.Logf("inserts=%d  rate=%.2f M/s", insertN, float64(insertN)/dur.Seconds()/1e6)
	t.Logf("reads=%d    rate=%.2f M/s", readN, float64(readN)/dur.Seconds()/1e6)
	t.Logf("SELECT WHERE id = ? latencies (ns):")
	t.Logf("  p50  = %d", p(0.50))
	t.Logf("  p90  = %d", p(0.90))
	t.Logf("  p99  = %d", p(0.99))
	t.Logf("  p999 = %d", p(0.999))
	t.Logf("  max  = %d", p(1.0))
}

// TestMixedWorkloadLatency_WAL — same shape, with WAL enabled. Tests
// whether walMu contention shows up in p99 for reads (it shouldn't —
// reads bypass walMu entirely).
func TestMixedWorkloadLatency_WAL(t *testing.T) {
	if testing.Short() {
		t.Skip("mixed workload test takes ~3s")
	}
	dir := t.TempDir()
	db, _ := Open(Options{Schema: benchSchema(), SizeHint: 100_000, WALPath: dir + "/m.wal"})
	defer db.Close()

	const seedN = 50_000
	for i := 0; i < seedN; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", i, "seed", i%100)
	}

	const writers = 4
	const readers = 16
	const dur = 2 * time.Second

	var insertN, readN int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			id := seedN + seed*1_000_000
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "n", id%100)
				if err == nil {
					atomic.AddInt64(&insertN, 1)
				}
				id++
			}
		}(w)
	}

	perReader := make([][]int64, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lats := make([]int64, 0, 1<<20)
			rng := rand.New(rand.NewSource(int64(idx) + 1))
			for {
				select {
				case <-stop:
					perReader[idx] = lats
					return
				default:
				}
				id := rng.Intn(seedN)
				t0 := time.Now()
				_, _, err := db.Query("SELECT name, age FROM users WHERE id = ?", id)
				dt := time.Since(t0).Nanoseconds()
				if err == nil {
					lats = append(lats, dt)
					atomic.AddInt64(&readN, 1)
				}
			}
		}(r)
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()

	var all []int64
	for _, s := range perReader {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(q float64) int64 {
		if len(all) == 0 {
			return 0
		}
		idx := int(float64(len(all)) * q)
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return all[idx]
	}
	t.Logf("writers=%d readers=%d duration=%v WAL=yes", writers, readers, dur)
	t.Logf("inserts=%d  rate=%.2f M/s", insertN, float64(insertN)/dur.Seconds()/1e6)
	t.Logf("reads=%d    rate=%.2f M/s", readN, float64(readN)/dur.Seconds()/1e6)
	t.Logf("SELECT WHERE id = ? latencies (ns):")
	t.Logf("  p50  = %d", p(0.50))
	t.Logf("  p90  = %d", p(0.90))
	t.Logf("  p99  = %d", p(0.99))
	t.Logf("  p999 = %d", p(0.999))
	t.Logf("  max  = %d", p(1.0))
}
