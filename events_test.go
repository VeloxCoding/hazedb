package hazedb

import (
	"errors"
	"path/filepath"
	"testing"
)

// Even with NO WAL, the companion file exists and logEvent records a queryable
// row in _hz_events — SQLite logging does not depend on durability.
func TestCompanionEventsTable(t *testing.T) {
	db, err := Open(Options{Schema: testSchema(), CompanionPath: filepath.Join(t.TempDir(), "hazedb.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.sq == nil {
		t.Fatal("companion must be present even without WAL")
	}
	db.logEvent("warn", "test-event", "hello world")

	var n int
	if err := db.sq.sdb.QueryRow(`SELECT count(*) FROM _hz_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("event rows: got %d, want 1", n)
	}
	var level, kind, msg string
	if err := db.sq.sdb.QueryRow(`SELECT level, kind, message FROM _hz_events`).Scan(&level, &kind, &msg); err != nil {
		t.Fatal(err)
	}
	if level != "warn" || kind != "test-event" || msg != "hello world" {
		t.Fatalf("event row: level=%q kind=%q msg=%q", level, kind, msg)
	}
}

// A user CREATE TABLE in the reserved _hz_ namespace is rejected (case-insensitive),
// so user data can never collide with hazedb's internal companion tables.
func TestReservedTablePrefixRejected(t *testing.T) {
	db := openMem(t)
	for _, name := range []string{"_hz_events", "_hz_foo", "_HZ_Bar"} {
		if _, err := db.Exec("CREATE TABLE " + name + " (id uuid primary key)"); !errors.Is(err, ErrReservedName) {
			t.Fatalf("CREATE TABLE %s: want ErrReservedName, got %v", name, err)
		}
	}
	// A name that merely starts with "hz" (no leading underscore) is fine.
	if _, err := db.Exec("CREATE TABLE hz_ok (id uuid primary key)"); err != nil {
		t.Fatalf("non-reserved name rejected: %v", err)
	}
}

// Mirror recovery must ignore the operational _hz_ tables: a round-trip through
// the mirror, with an event logged, reopens cleanly and preserves the data —
// never treating _hz_events as a data table to load.
func TestRecoveryIgnoresOpsTables(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	comp := filepath.Join(dir, "companion.db")
	open := func() *DB {
		db, err := Open(Options{
			Schema: testSchema(), WALPath: walDir, CompanionPath: comp,
			walFlushInterval: -1, drainInterval: -1, // manual flush + manual drain
		})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatal(err)
	}
	db.logEvent("info", "test", "before close")
	if err := db.wal.flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.drainOnce(); err != nil { // mirror the data into the companion
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2 := open() // recovery from the mirror base; must not choke on _hz_events
	defer db2.Close()
	_, rows, err := db2.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][1].Str() != "alice" {
		t.Fatalf("data not preserved across mirror recovery: %v", rows)
	}
}
