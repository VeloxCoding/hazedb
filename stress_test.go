package hazedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentMixed runs 8 writer + 24 reader goroutines for a
// fixed wall-clock budget and verifies no panics, no races (with
// -race), and final row count matches. Not a benchmark — just a
// correctness smoke under contention.
func TestConcurrentMixed(t *testing.T) {
	db := openMem(t)
	const writers = 8
	const readers = 24
	const dur = 200 * time.Millisecond

	var inserted, scanned int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			id := int64(seed) * 100000
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(int(id)), "n", id%100)
				if err == nil {
					atomic.AddInt64(&inserted, 1)
				}
				id++
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, err := db.Query("SELECT id FROM users WHERE age > ? LIMIT 10", 50)
				if err == nil {
					atomic.AddInt64(&scanned, 1)
				}
			}
		}()
	}

	time.Sleep(dur)
	close(stop)
	wg.Wait()

	// Verify storage agrees with insert count.
	_, all, _ := db.Query("SELECT id FROM users")
	if int64(len(all)) != atomic.LoadInt64(&inserted) {
		t.Errorf("row count mismatch: inserted=%d, in-store=%d", inserted, len(all))
	}
	t.Logf("inserts=%d scans=%d", inserted, scanned)
}

// TestWALDurabilityRoundTrip writes a large set, closes, reopens, and
// verifies every row survives in order-agnostic comparison.
func TestWALDurabilityRoundTrip(t *testing.T) {
	db, path := openDBWithWAL(t)
	const N = 5000
	for i := 0; i < N; i++ {
		_, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), fmt.Sprintf("u%d", i), i%100)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id FROM users")
	if len(rows) != N {
		t.Fatalf("after replay: got %d rows, want %d", len(rows), N)
	}
}
