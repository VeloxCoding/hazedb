package hazedb

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Compacting a non-partitioned shard reclaims every dead slot, and afterwards
// every live PK still resolves to its row, deleted PKs stay gone, full + indexed
// scans return exactly the live set, and the byte tally is unchanged.
func TestCompactNonPartitioned(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE u (id uuid primary key, n int, body text, INDEX (body))")
	const n = 1000
	for i := 0; i < n; i++ {
		db.Exec("INSERT INTO u (id, n, body) VALUES (?, ?, ?)", tid(i), i, "b")
	}
	beforeBytes := db.MetaSnapshot().TableStats[0].ApproxBytes
	for i := 0; i < n; i += 2 { // delete evens → 500 live, 500 dead
		db.Exec("DELETE FROM u WHERE id = ?", tid(i))
	}
	rt := db.cat.Load().byName["u"]
	for i := range rt.shards {
		rt.compactShard(i)
	}

	st := db.MetaSnapshot().TableStats[0]
	if st.Tombstones != 0 {
		t.Fatalf("after compaction: tombstones=%d, want 0", st.Tombstones)
	}
	if st.Rows != n/2 {
		t.Fatalf("rows=%d, want %d", st.Rows, n/2)
	}
	if st.ApproxBytes != beforeBytes/2 {
		t.Fatalf("bytes=%d, want %d (half)", st.ApproxBytes, beforeBytes/2)
	}
	reconcileBytes(t, db) // tally still equals a full walk

	for i := 1; i < n; i += 2 { // odds are live
		_, rows, err := db.Query("SELECT n FROM u WHERE id = ?", tid(i))
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0][0].Int() != int64(i) {
			t.Fatalf("live pk tid(%d) lost after compaction: %v", i, rows)
		}
	}
	if _, rows, _ := db.Query("SELECT n FROM u WHERE id = ?", tid(0)); len(rows) != 0 {
		t.Fatalf("deleted pk resurfaced after compaction")
	}
	// Indexed read uses the secondary index (PK-keyed, untouched by compaction).
	if _, rows, _ := db.Query("SELECT id FROM u WHERE body = ?", "b"); len(rows) != n/2 {
		t.Fatalf("indexed scan=%d, want %d", len(rows), n/2)
	}
}

// Compacting a partitioned shard reclaims dead slots, preserves the live scan
// order, and keeps every pkDirectory location valid.
func TestCompactPartitioned(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE q (id uuid primary key, thread uuid partition key, n int)")
	thread := tid(1)
	const n = 1000
	for i := 0; i < n; i++ {
		db.Exec("INSERT INTO q (id, thread, n) VALUES (?, ?, ?)", tid(100+i), thread, i)
	}
	for i := 0; i < n; i += 2 { // delete evens
		db.Exec("DELETE FROM q WHERE id = ?", tid(100+i))
	}
	rt := db.cat.Load().byName["q"]
	for i := range rt.shards {
		rt.compactShard(i)
	}

	if tomb := db.MetaSnapshot().TableStats[0].Tombstones; tomb != 0 {
		t.Fatalf("after compaction: tombstones=%d, want 0", tomb)
	}
	// scanPartition returns the 500 live rows in insert order (odds: 1,3,5,...).
	_, rows, err := db.Query("SELECT n FROM q WHERE thread = ?", thread)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n/2 {
		t.Fatalf("partition scan=%d, want %d", len(rows), n/2)
	}
	for k, r := range rows {
		if want := int64(2*k + 1); r[0].Int() != want {
			t.Fatalf("scan order[%d]=%d, want %d", k, r[0].Int(), want)
		}
	}
	// PK lookup resolves through the rewritten directory.
	if _, r, _ := db.Query("SELECT n FROM q WHERE id = ?", tid(101)); len(r) != 1 || r[0][0].Int() != 1 {
		t.Fatalf("pk lookup lost after compaction: %v", r)
	}
}

// Compaction runs under the shard write lock while readers hold the read lock, so
// a renumber is never visible mid-read. Hammer reads (PK + partition scan) while
// repeatedly compacting; the race detector + the assertions catch any torn view.
func TestCompactConcurrentReads(t *testing.T) {
	db := openEmpty(t)
	db.Exec("CREATE TABLE q (id uuid primary key, thread uuid partition key, n int)")
	thread := tid(1)
	const n = 400
	for i := 0; i < n; i++ {
		db.Exec("INSERT INTO q (id, thread, n) VALUES (?, ?, ?)", tid(100+i), thread, i)
	}
	for i := 0; i < n; i += 2 {
		db.Exec("DELETE FROM q WHERE id = ?", tid(100+i))
	}
	rt := db.cat.Load().byName["q"]

	var stop atomic.Bool
	var compactor, readers sync.WaitGroup
	compactor.Add(1)
	go func() {
		defer compactor.Done()
		for !stop.Load() {
			for i := range rt.shards {
				rt.compactShard(i)
			}
		}
	}()
	// Readers: a live PK lookup and a partition scan must always see the live row.
	for g := 0; g < 6; g++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 2000; j++ {
				if _, r, err := db.Query("SELECT n FROM q WHERE id = ?", tid(101)); err != nil || len(r) != 1 || r[0][0].Int() != 1 {
					t.Errorf("pk read saw torn state: err=%v rows=%v", err, r)
					return
				}
				if _, r, err := db.Query("SELECT n FROM q WHERE thread = ?", thread); err != nil || len(r) != n/2 {
					t.Errorf("scan saw %d rows, want %d (err=%v)", len(r), n/2, err)
					return
				}
			}
		}()
	}
	readers.Wait()
	stop.Store(true)
	compactor.Wait()
}
