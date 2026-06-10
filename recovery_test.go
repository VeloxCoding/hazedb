package hazedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// dumpUsers serialises the whole users table in PK order for equality
// comparison between in-memory state and post-replay state.
func dumpUsers(t *testing.T, db *DB) string {
	t.Helper()
	_, rows, err := db.Query("SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "%s|%s|%d\n", r[0].AsString(), r[1].Str(), r[2].Int())
	}
	return b.String()
}

// dumpWideRow renders every cell of wide's row for id, for equality comparison
// across a close+reopen (WAL replay).
func dumpWideRow(t *testing.T, db *DB, id UUID) string {
	t.Helper()
	_, row, err := db.QueryRow("SELECT * FROM wide WHERE id = ?", id)
	if err != nil || row == nil {
		t.Fatalf("queryrow wide: err=%v nil=%v", err, row == nil)
	}
	var b strings.Builder
	for _, v := range row {
		fmt.Fprintf(&b, "%s,", v.AsString())
	}
	return b.String()
}

// TestWideUpdateWALRoundTrip pins the uint16 nsets fix: an UPDATE setting more
// than 255 columns in one statement must replay correctly. With nsets as a
// single byte the SET count wrapped (300 -> 44), so replay applied the wrong
// columns and the reopened DB diverged from RAM (or Open aborted on a decode
// error). The per-cell ordinal was always uint16, so only the count was at fault.
func TestWideUpdateWALRoundTrip(t *testing.T) {
	const ncol = 300
	dir := t.TempDir()
	path := filepath.Join(dir, "wide.wal")

	id := tid(1)
	// Columns are NOT NULL by default, so insert every column (value 0), then
	// UPDATE all ncol of them to distinct values — the >255-SET-column write.
	var create, insert, upd strings.Builder
	create.WriteString("CREATE TABLE wide (id uuid primary key")
	insert.WriteString("INSERT INTO wide (id")
	upd.WriteString("UPDATE wide SET ")
	insArgs := []any{id}
	updArgs := make([]any, 0, ncol+1)
	for i := 0; i < ncol; i++ {
		fmt.Fprintf(&create, ", c%d int", i)
		fmt.Fprintf(&insert, ", c%d", i)
		insArgs = append(insArgs, int64(0))
		if i > 0 {
			upd.WriteString(", ")
		}
		fmt.Fprintf(&upd, "c%d = ?", i)
		updArgs = append(updArgs, int64(i*7+1))
	}
	create.WriteString(")")
	insert.WriteString(") VALUES (?")
	for i := 0; i < ncol; i++ {
		insert.WriteString(", ?")
	}
	insert.WriteString(")")
	upd.WriteString(" WHERE id = ?")
	updArgs = append(updArgs, id)

	db, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(create.String()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(insert.String(), insArgs...); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(upd.String(), updArgs...); err != nil {
		t.Fatalf("update %d SET columns: %v", ncol, err)
	}
	want := dumpWideRow(t, db, id)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: Schema{}, WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatalf("reopen (replay of >255-column UPDATE) must succeed: %v", err)
	}
	defer db2.Close()
	if got := dumpWideRow(t, db2, id); got != want {
		t.Fatalf("post-replay row diverged from in-memory:\n in-mem: %s\n replay: %s", want, got)
	}
}

// A rejected duplicate INSERT must not be journaled. Before the fix, the
// WAL record was written before the uniqueness check, so the rejected
// insert landed in the WAL and the next Open() re-hit ErrDuplicatePK
// during replay and failed permanently.
func TestRejectedDuplicateInsertDoesNotCorruptWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dup.wal")

	db, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	// Duplicate — must be rejected and must NOT reach the WAL.
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "dup", 99); !errors.Is(err, ErrDuplicatePK) {
		t.Fatalf("expected ErrDuplicatePK, got %v", err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: replay must succeed (no journaled duplicate) and show exactly
	// the two accepted rows.
	db2, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatalf("reopen after rejected duplicate must succeed, got: %v", err)
	}
	defer db2.Close()
	_, rows, err := db2.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after replay, got %d", len(rows))
	}
	// id=1 must still be "alice" (the duplicate "dup" was never applied).
	_, r1, _ := db2.Query("SELECT name FROM users WHERE id = ?", tid(1))
	if len(r1) != 1 || r1[0][0].Str() != "alice" {
		t.Errorf("id=1 should be alice, got %v", r1)
	}
}

// A WAL whose final record carries a corrupt, oversized length must be
// treated as a truncated tail — not cause an over-allocation (OOM) or a
// hard error. Recovery must bounds-check the declared length against the
// bytes actually remaining before allocating/reading the body.
func TestWALCorruptTailLengthIsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huge.wal")

	db, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30)
	db.Close()

	// Append a record header claiming a 4 GiB body, then only a few bytes.
	// A naive make([]byte, totalLen) would try to allocate 4 GiB.
	f, err := os.OpenFile(walSegmentFile(t, path), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	// totalLen = 0xFFFFFFF0 (~4 GiB) little-endian, then 3 stray bytes.
	f.Write([]byte{0xF0, 0xFF, 0xFF, 0xFF, 0x01, 0x02, 0x03})
	f.Close()

	db2, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatalf("corrupt oversized tail length must be tolerated, got %v", err)
	}
	defer db2.Close()
	_, rows, _ := db2.Query("SELECT id FROM users")
	if len(rows) != 1 {
		t.Errorf("expected 1 surviving row, got %d", len(rows))
	}
}

// A multi-shard predicate UPDATE/DELETE must journal in the same order it
// applies, so WAL replay reproduces the in-memory state exactly. This is
// the single-threaded sanity check for the lock-all-shards path.
func TestMultiShardUpdateDeleteWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ms.wal")
	db, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i%50); err != nil {
			t.Fatal(err)
		}
	}
	// Predicate writes that span shards (no PK pin).
	if _, err := db.Exec("UPDATE users SET age = ? WHERE age > ?", 7, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE age = ?", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE users SET name = ? WHERE age = ?", "x", 7); err != nil {
		t.Fatal(err)
	}
	want := dumpUsers(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := dumpUsers(t, db2); got != want {
		t.Fatalf("replay diverged from memory state:\n--want--\n%s--got--\n%s", want, got)
	}
}

// Concurrent multi-shard predicate updates must still replay to exactly the
// final in-memory state. The old one-shard-at-a-time path could interleave
// per shard so RAM ended in a state the WAL's total order did not reproduce.
// Run with -race for extra signal.
func TestConcurrentMultiShardWritesReplayConsistent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "msc.wal")
	db, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 800; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i%100); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				db.Exec("UPDATE users SET age = ? WHERE age > ?", g, 50+k%40)
			}
		}(g)
	}
	wg.Wait()
	want := dumpUsers(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := dumpUsers(t, db2); got != want {
		t.Fatal("concurrent multi-shard writes: WAL replay diverged from in-memory state")
	}
}

// TestWALRejectsForeignVersion: a record stamped with a non-current walVersion
// (e.g. a WAL written before the opUpdate nsets u8->u16 change, version 1) must
// abort Open with ErrWALCorrupt — never be silently misparsed under the new
// layout. The version byte exists precisely to catch a format change.
func TestWALRejectsForeignVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	db, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "a", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Stamp the first record's version byte (header is magic:2 | type:1 |
	// version:1 | ...) with an old version. The check fires before the CRC, so
	// this exercises the version guard, not a CRC mismatch.
	seg := walSegmentFile(t, path)
	f, err := os.OpenFile(seg, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{walVersion - 1}, 3); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Options{Schema: testSchema(), WALLevel: WALPeriodic, WALPath: path}); !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("Open with a stale-version WAL: want ErrWALCorrupt, got %v", err)
	}
}
