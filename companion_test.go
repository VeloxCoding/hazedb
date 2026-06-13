package hazedb

// Tests for the file-backed SQLite companion: durability of the data mirror and
// the _hz_events log across a restart, that the file is a standard SQLite
// database any client can read, and that ops logging is independent of data
// durability. The blank modernc driver import in drain.go registers "sqlite", so
// the raw sql.Open here needs no extra import.

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// A file companion persists BOTH halves across a restart: reopen rebuilds the
// rows from the mirror base, and the _hz_events rows are still there.
func TestFileCompanionPersistsDataAndEvents(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	comp := filepath.Join(dir, "hazedb.db")
	openDB := func() *DB {
		db, err := Open(Options{
			Schema: testSchema(), WALPath: walDir, CompanionPath: comp,
			walFlushInterval: -1, drainInterval: -1, // manual flush + manual drain
		})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	db := openDB()
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i); err != nil {
			t.Fatal(err)
		}
	}
	db.logEvent("info", "boot", "first session")
	if err := db.wal.flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.drainOnce(); err != nil { // fold the data into the mirror file
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openDB() // recovery base is the file
	defer db2.Close()
	if got := countUsers(t, db2); got != 3 {
		t.Fatalf("rows after restart from the file mirror: got %d, want 3", got)
	}
	var events int
	if err := db2.sq.sdb.QueryRow(`SELECT count(*) FROM _hz_events WHERE kind='boot'`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("boot events in the file after restart: got %d, want 1", events)
	}
}

// The companion file is a standard SQLite database: after hazedb closes it, a
// plain sql.Open (no hazedb) reads both the data tables and the _hz_events log —
// the portability / dashboard promise.
func TestFileCompanionIsStandardSQLite(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	comp := filepath.Join(dir, "hazedb.db")

	db, err := Open(Options{
		Schema: testSchema(), WALPath: walDir, CompanionPath: comp,
		walFlushInterval: -1, drainInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatal(err)
	}
	db.logEvent("warn", "demo", "readable by any sqlite client")
	if err := db.wal.flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.drainOnce(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the file with the bare driver — hazedb is not involved.
	raw, err := sql.Open("sqlite", comp)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var users int
	if err := raw.QueryRow(`SELECT count(*) FROM users`).Scan(&users); err != nil {
		t.Fatalf("reading users from the raw file: %v", err)
	}
	if users != 1 {
		t.Fatalf("raw file: users=%d, want 1", users)
	}
	var kind, msg string
	if err := raw.QueryRow(`SELECT kind, message FROM _hz_events WHERE level='warn'`).Scan(&kind, &msg); err != nil {
		t.Fatalf("reading _hz_events from the raw file: %v", err)
	}
	if kind != "demo" || msg != "readable by any sqlite client" {
		t.Fatalf("raw file event: kind=%q msg=%q", kind, msg)
	}
}

// Ops logging is independent of data durability: with NO WAL but a file
// companion, data lives only in RAM (lost on restart) while _hz_events persists
// and accumulates across sessions.
func TestFileCompanionEventsSurviveMemoryOnly(t *testing.T) {
	dir := t.TempDir()
	comp := filepath.Join(dir, "ops.db")
	openOps := func() *DB {
		db, err := Open(Options{Schema: testSchema(), CompanionPath: comp}) // no WAL
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	db := openOps()
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "ephemeral", 1); err != nil {
		t.Fatal(err)
	}
	db.logEvent("info", "boot", "session one")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := openOps()
	defer db2.Close()
	// Data is gone — memory-only, no WAL ...
	if got := countUsers(t, db2); got != 0 {
		t.Fatalf("memory-only data must not survive restart: got %d rows, want 0", got)
	}
	// ... but the event from session one persisted in the companion file.
	db2.logEvent("info", "boot", "session two")
	var n int
	if err := db2.sq.sdb.QueryRow(`SELECT count(*) FROM _hz_events WHERE kind='boot'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("boot events across two memory-only sessions: got %d, want 2", n)
	}
}

// With WAL on and CompanionPath left empty, the companion defaults to a file
// "hazedb.db" inside WALPath, the mirror is on, and data persists across a
// restart through that default file — the zero-config durable shape.
func TestDefaultCompanionFileWithWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	openDB := func() *DB {
		db, err := Open(Options{Schema: testSchema(), WALPath: walDir}) // no CompanionPath
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	db := openDB()
	for i := 1; i <= 3; i++ {
		if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "u", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil { // final drain folds the data into the default file
		t.Fatal(err)
	}

	// The default companion file exists inside WALPath.
	want := filepath.Join(walDir, "hazedb.db")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("default companion file %s should exist: %v", want, err)
	}

	db2 := openDB() // same default path → recovery base
	defer db2.Close()
	if got := countUsers(t, db2); got != 3 {
		t.Fatalf("rows after restart via the default companion file: got %d, want 3", got)
	}
}
