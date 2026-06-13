package hazedb

import (
	"path/filepath"
	"testing"
)

// After a drain, recovery must rebuild exact state from SQLite (the drained
// bulk, incl. a runtime table) plus the undrained WAL tail replayed on top —
// including a tail UPDATE and DELETE that target rows already in SQLite.
func TestSQLiteRecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	opts := Options{
		Schema: testSchema(), WALPath: dir, SQLitePath: sqPath,
		drainInterval: -1,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// batch A + a runtime table -> drained to SQLite, segment reclaimed
	for i := 1; i <= 20; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "a", i)
	}
	db.Exec("CREATE TABLE logs (id uuid primary key, msg text)")
	for i := 1; i <= 5; i++ {
		db.Exec("INSERT INTO logs (id, msg) VALUES (?, ?)", tid(1000+i), "m")
	}
	if err := db.wal.flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.drainOnce(); err != nil {
		t.Fatal(err)
	}

	// batch B = undrained tail: a fresh insert, plus an UPDATE and DELETE that
	// target rows already living in SQLite (ids 5 and 6).
	for i := 21; i <= 30; i++ {
		db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(i), "b", i)
	}
	db.Exec("UPDATE users SET age = ? WHERE id = ?", 777, tid(5))
	db.Exec("DELETE FROM users WHERE id = ?", tid(6))
	db.FlushWAL()
	// "crash": DrainInterval=-1 means Close runs no final drain, so batch B
	// stays in the WAL undrained.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if got := countUsers(t, db2); got != 29 { // 30 distinct ids - id6 deleted
		t.Fatalf("users after recovery = %d, want 29", got)
	}
	if _, rows, _ := db2.Query("SELECT age FROM users WHERE id = ?", tid(25)); len(rows) != 1 || rows[0][0].Int() != 25 {
		t.Fatalf("undrained insert id25 missing/wrong: %v", rows)
	}
	if _, rows, _ := db2.Query("SELECT age FROM users WHERE id = ?", tid(5)); len(rows) != 1 || rows[0][0].Int() != 777 {
		t.Fatalf("undrained UPDATE of a drained row not applied: %v", rows)
	}
	if _, rows, _ := db2.Query("SELECT id FROM users WHERE id = ?", tid(6)); len(rows) != 0 {
		t.Fatal("undrained DELETE of a drained row not applied")
	}
	if _, rows, _ := db2.Query("SELECT id FROM logs"); len(rows) != 5 {
		t.Fatalf("runtime table logs after recovery = %d, want 5", len(rows))
	}
}

// Draining reclaims sealed segments, so WAL disk stays bounded under repeated
// write/rotate/drain rounds.
func TestDrainReclaimsSegments(t *testing.T) {
	dir := t.TempDir()
	sqPath := filepath.Join(t.TempDir(), "m.db")
	db, err := Open(Options{
		Schema: testSchema(), WALPath: dir, SQLitePath: sqPath,
		drainInterval: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for round := 0; round < 4; round++ {
		for i := 0; i < 10; i++ {
			db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(round*10+i+1), "x", i)
		}
		if err := db.wal.flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.drainOnce(); err != nil {
			t.Fatal(err)
		}
	}
	sealed, err := db.wal.sealedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 0 {
		t.Fatalf("sealed segments not reclaimed after draining: %v", sealed)
	}
	if segs, _ := listSegments(dir); len(segs) > 1 {
		t.Fatalf("WAL not bounded: %d segment files remain, want <=1 (active only)", len(segs))
	}
}
