package hazedb

import (
	"os"
	"testing"
	"time"
)

// openSegmented opens a segmented-WAL DB whose rotate ticker is set long enough
// not to fire during the test; rotation is driven explicitly via db.wal.rotate()
// for determinism. Tests that exercise the ticker pass a short interval instead.
func openSegmented(t *testing.T, dir string, rotate time.Duration) *DB {
	t.Helper()
	db, err := Open(Options{Schema: testSchema(), WALPath: dir, WALRotateInterval: rotate})
	if err != nil {
		t.Fatalf("open segmented: %v", err)
	}
	return db
}

func countUsers(t *testing.T, db *DB) int {
	t.Helper()
	_, rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

// Inserts span two segments (a rotate mid-stream) plus an UPDATE and a DELETE;
// reopening must replay every segment in order and reproduce the exact state.
func TestSegmentedWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db := openSegmented(t, dir, time.Hour)
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "n", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.wal.rotate(); err != nil { // seal segment 1
		t.Fatal(err)
	}
	for i := 6; i <= 10; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "n", i); err != nil {
			t.Fatal(err)
		}
	}
	db.Exec("UPDATE users SET age = ? WHERE id = ?", 99, tid(3))
	db.Exec("DELETE FROM users WHERE id = ?", tid(7))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openSegmented(t, dir, time.Hour)
	defer db2.Close()
	if got := countUsers(t, db2); got != 9 { // 10 inserted - 1 deleted
		t.Fatalf("after reopen: %d rows, want 9", got)
	}
	_, rows, _ := db2.Query("SELECT age FROM users WHERE id = ?", tid(3))
	if len(rows) != 1 || rows[0][0].Int() != 99 {
		t.Fatalf("update not replayed across segments: %v", rows)
	}
	_, rows, _ = db2.Query("SELECT id FROM users WHERE id = ?", tid(7))
	if len(rows) != 0 {
		t.Fatalf("delete not replayed: id=7 still present")
	}
}

// rotate() advances and seals only when the active segment holds data; an empty
// rotate is a no-op (no zero-record segments mid-stream).
func TestSegmentedRotateBehavior(t *testing.T) {
	dir := t.TempDir()
	db := openSegmented(t, dir, time.Hour)
	defer db.Close()
	if db.wal.seg != 1 {
		t.Fatalf("active seg = %d, want 1", db.wal.seg)
	}
	if err := db.wal.rotate(); err != nil { // empty → no-op
		t.Fatal(err)
	}
	if db.wal.seg != 1 {
		t.Fatalf("empty rotate advanced seg to %d", db.wal.seg)
	}
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 1)
	if err := db.wal.rotate(); err != nil {
		t.Fatal(err)
	}
	if db.wal.seg != 2 {
		t.Fatalf("after data rotate seg = %d, want 2", db.wal.seg)
	}
	if _, err := os.Stat(db.wal.segPath(1)); err != nil {
		t.Fatalf("sealed segment 1 missing: %v", err)
	}
	sealed, err := db.wal.sealedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1 || sealed[0] != 1 {
		t.Fatalf("sealedSegments = %v, want [1]", sealed)
	}
}

// The background rotate ticker seals the active segment on its own cadence.
func TestSegmentedRotateTicker(t *testing.T) {
	dir := t.TempDir()
	db := openSegmented(t, dir, 25*time.Millisecond)
	defer db.Close()
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 1)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sealed, _ := db.wal.sealedSegments(); len(sealed) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("rotate ticker did not seal a segment within 3s")
}

// A clean open/close with no writes must not leave an empty segment behind.
func TestSegmentedEmptyActiveRemovedOnClose(t *testing.T) {
	dir := t.TempDir()
	db := openSegmented(t, dir, time.Hour)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 0 {
		t.Fatalf("empty active segment not removed: %v", segs)
	}
}

// A broad UPDATE is journaled as one recTxn; it must replay all-or-nothing after
// a rotate seals the segment it landed in.
func TestSegmentedTxnReplay(t *testing.T) {
	dir := t.TempDir()
	db := openSegmented(t, dir, time.Hour)
	for i := 1; i <= 4; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "x", 10)
	}
	if _, err := db.Exec("UPDATE users SET age = ? WHERE age = ?", 20, 10); err != nil { // multi-row → recTxn
		t.Fatal(err)
	}
	if err := db.wal.rotate(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2 := openSegmented(t, dir, time.Hour)
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id FROM users WHERE age = ?", 20)
	if len(rows) != 4 {
		t.Fatalf("txn replay across segment: %d rows at age=20, want 4", len(rows))
	}
}
