package hazedb

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func openMsgsMem(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{Schema: msgsSchema()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func insMsg(t *testing.T, db *DB, id, thread UUID, seq int, body string) {
	t.Helper()
	if _, err := db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", id, thread, seq, body); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionedCRUD(t *testing.T) {
	db := openMsgsMem(t)
	thread := NewUUIDv7()
	ids := make([]UUID, 5)
	for i := range ids {
		ids[i] = NewUUIDv7()
		insMsg(t, db, ids[i], thread, i, "m")
	}
	_, rows, _ := db.Query("SELECT body FROM messages WHERE id = ?", ids[2])
	if len(rows) != 1 || rows[0][0].Str() != "m" {
		t.Fatalf("get by pk: %v", rows)
	}
	if _, err := db.Exec("UPDATE messages SET body = ? WHERE id = ?", "edited", ids[2]); err != nil {
		t.Fatal(err)
	}
	_, rows, _ = db.Query("SELECT body FROM messages WHERE id = ?", ids[2])
	if rows[0][0].Str() != "edited" {
		t.Fatalf("update by pk: %v", rows)
	}
	if n, _ := db.Exec("DELETE FROM messages WHERE id = ?", ids[2]); n != 1 {
		t.Fatalf("delete count")
	}
	_, rows, _ = db.Query("SELECT body FROM messages WHERE id = ?", ids[2])
	if len(rows) != 0 {
		t.Fatalf("deleted row still present: %v", rows)
	}
	_, all, _ := db.Query("SELECT id FROM messages")
	if len(all) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(all))
	}
}

// The pkDirectory enforces PK uniqueness across the WHOLE table — the same id
// in two different partitions must be rejected.
func TestPartitionedTableWidePKUniqueness(t *testing.T) {
	db := openMsgsMem(t)
	tA, tB := NewUUIDv7(), NewUUIDv7()
	id := NewUUIDv7()
	insMsg(t, db, id, tA, 1, "a")
	if _, err := db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", id, tB, 1, "b"); !errors.Is(err, ErrDuplicatePK) {
		t.Fatalf("expected duplicate PK across partitions, got %v", err)
	}
}

// DELETE then re-INSERT the same PK: the row gets a new location, and a PK
// lookup must observe the new row (the re-resolve path), not a phantom miss.
func TestPartitionedDeleteReinsertSamePK(t *testing.T) {
	db := openMsgsMem(t)
	th := NewUUIDv7()
	id := NewUUIDv7()
	insMsg(t, db, id, th, 1, "old")
	if _, err := db.Exec("DELETE FROM messages WHERE id = ?", id); err != nil {
		t.Fatal(err)
	}
	insMsg(t, db, id, th, 2, "new")
	_, rows, _ := db.Query("SELECT body FROM messages WHERE id = ?", id)
	if len(rows) != 1 || rows[0][0].Str() != "new" {
		t.Fatalf("after delete+reinsert: %v", rows)
	}
}

// A multi-shard predicate DELETE must tombstone matching rows AND drop their
// directory entries, keeping the directory consistent with the shards.
func TestPartitionedMultiShardDelete(t *testing.T) {
	db := openMsgsMem(t)
	threads := []UUID{NewUUIDv7(), NewUUIDv7(), NewUUIDv7()}
	for i := 0; i < 300; i++ {
		insMsg(t, db, NewUUIDv7(), threads[i%3], i%50, "m")
	}
	n, err := db.Exec("DELETE FROM messages WHERE seq < ?", 10) // seq in 0..9 → 60 of 300
	if err != nil {
		t.Fatal(err)
	}
	if n != 60 {
		t.Fatalf("multi-shard delete n=%d, want 60", n)
	}
	_, all, _ := db.Query("SELECT id FROM messages")
	if len(all) != 240 {
		t.Fatalf("remaining %d, want 240", len(all))
	}
	// Every surviving row must still resolve by PK (directory consistent).
	for _, r := range all {
		_, got, _ := db.Query("SELECT id FROM messages WHERE id = ?", r[0].UUID())
		if len(got) != 1 {
			t.Fatalf("survivor not addressable by pk after multi-shard delete")
		}
	}
}

func TestPartitionedWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.wal")
	db, err := Open(Options{Schema: msgsSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	th := NewUUIDv7()
	keep, gone := NewUUIDv7(), NewUUIDv7()
	insMsg(t, db, keep, th, 1, "keep")
	insMsg(t, db, gone, th, 2, "gone")
	db.Exec("UPDATE messages SET body = ? WHERE id = ?", "kept", keep)
	db.Exec("DELETE FROM messages WHERE id = ?", gone)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: msgsSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id, body FROM messages")
	if len(rows) != 1 || rows[0][1].Str() != "kept" {
		t.Fatalf("after replay: %v", rows)
	}
	// Replayed row is addressable by PK (directory rebuilt on replay).
	_, r2, _ := db2.Query("SELECT body FROM messages WHERE id = ?", keep)
	if len(r2) != 1 || r2[0][0].Str() != "kept" {
		t.Fatalf("pk lookup after replay: %v", r2)
	}
}

// Concurrent insert/read/delete across partitions; afterwards every surviving
// row must be addressable by PK (directory/shard consistency). Run with -race.
func TestPartitionedConcurrent(t *testing.T) {
	db := openMsgsMem(t)
	threads := []UUID{NewUUIDv7(), NewUUIDv7(), NewUUIDv7(), NewUUIDv7()}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := NewUUIDv7()
				insMsg2(db, id, threads[(g+i)%4], i)
				db.Query("SELECT body FROM messages WHERE id = ?", id)
				if i%3 == 0 {
					db.Exec("DELETE FROM messages WHERE id = ?", id)
				}
			}
		}(g)
	}
	wg.Wait()
	_, all, err := db.Query("SELECT id FROM messages")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range all {
		_, got, _ := db.Query("SELECT id FROM messages WHERE id = ?", r[0].UUID())
		if len(got) != 1 {
			t.Fatalf("surviving row not addressable by pk")
		}
	}
}

// insMsg2 is the goroutine-safe insert helper (no *testing.T in the hot loop).
func insMsg2(db *DB, id, thread UUID, seq int) {
	db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", id, thread, seq, "m")
}

// A WHERE partition = ? SELECT returns only that partition's rows (reading
// just its row list), respects ORDER BY + LIMIT, and skips deleted rows.
func TestPartitionScanQuery(t *testing.T) {
	db := openMsgsMem(t)
	tA, tB := NewUUIDv7(), NewUUIDv7()
	for i := 0; i < 20; i++ {
		insMsg(t, db, NewUUIDv7(), tA, i, "a")
		insMsg(t, db, NewUUIDv7(), tB, i, "b")
	}
	_, rows, err := db.Query("SELECT body FROM messages WHERE thread = ?", tA)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("thread A: got %d rows, want 20", len(rows))
	}
	for _, r := range rows {
		if r[0].Str() != "a" {
			t.Fatalf("scan returned a row from another partition: %v", r)
		}
	}
	_, top, err := db.Query("SELECT seq FROM messages WHERE thread = ? ORDER BY seq DESC LIMIT 3", tB)
	if err != nil {
		t.Fatal(err)
	}
	if len(top) != 3 || top[0][0].Int() != 19 || top[1][0].Int() != 18 || top[2][0].Int() != 17 {
		t.Fatalf("ORDER BY seq DESC LIMIT 3: %v", top)
	}
	// Delete the whole partition; the scan must then skip the tombstones.
	if _, err := db.Exec("DELETE FROM messages WHERE thread = ?", tA); err != nil {
		t.Fatal(err)
	}
	_, rows, _ = db.Query("SELECT body FROM messages WHERE thread = ?", tA)
	if len(rows) != 0 {
		t.Fatalf("after deleting partition: got %d rows, want 0", len(rows))
	}
	_, rb, _ := db.Query("SELECT body FROM messages WHERE thread = ?", tB)
	if len(rb) != 20 {
		t.Fatalf("other partition disturbed: got %d, want 20", len(rb))
	}
}

// Indexed partition scan: one thread (~100 rows) out of a 10k-row table.
// Contrast with BenchmarkSelectRange_Mem (~790us full scan of 10k).
func BenchmarkPartitionScan(b *testing.B) {
	db, _ := Open(Options{Schema: msgsSchema(), SizeHint: 10000})
	defer db.Close()
	threads := make([]UUID, 100)
	for i := range threads {
		threads[i] = NewUUIDv7()
	}
	for i := 0; i < 10000; i++ {
		db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", NewUUIDv7(), threads[i%100], i, "m")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT id FROM messages WHERE thread = ? ORDER BY seq DESC LIMIT 10", threads[i%100])
	}
}
