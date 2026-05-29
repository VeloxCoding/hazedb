package hazedb

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// Every durability mode must round-trip: write under the mode, close, reopen,
// and see all records replayed.
func TestWALDurabilityModesRoundTrip(t *testing.T) {
	modes := []struct {
		name string
		opt  Options
	}{
		{"flush-only", Options{}},
		{"ticker-sync", Options{WALSync: true, WALFlushInterval: 10 * time.Millisecond}},
		{"sync-per-write", Options{WALSyncPerWrite: true}},
		{"manual-only", Options{WALFlushInterval: -1}},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "m.wal")
			opt := m.opt
			opt.Schema = testSchema()
			opt.WALPath = path
			db, err := Open(opt)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			for k := 0; k < 50; k++ {
				if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(k), "u", k); err != nil {
					t.Fatalf("insert %d: %v", k, err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			db2, err := Open(Options{Schema: testSchema(), WALPath: path})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer db2.Close()
			_, rows, _ := db2.Query("SELECT id FROM users")
			if len(rows) != 50 {
				t.Errorf("expected 50 rows after replay, got %d", len(rows))
			}
		})
	}
}

// Once the WAL enters the sticky error state, every write must return that
// error and must NOT apply to memory (no RAM mutation absent from the WAL).
// Exercises all three PK-pinned paths (insert/update/delete).
func TestWALErrorStateBlocksAndDoesNotApply(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "e.wal")
	db, err := Open(Options{Schema: testSchema(), WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(1), "alice", 30); err != nil {
		t.Fatal(err)
	}

	// Inject a sticky WAL error.
	injected := errors.New("injected wal failure")
	db.wal.mu.Lock()
	db.wal.err = injected
	db.wal.mu.Unlock()

	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), "bob", 25); !errors.Is(err, injected) {
		t.Errorf("insert: expected injected error, got %v", err)
	}
	if _, err := db.Exec("UPDATE users SET age = ? WHERE id = ?", 99, tid(1)); !errors.Is(err, injected) {
		t.Errorf("update: expected injected error, got %v", err)
	}
	if _, err := db.Exec("DELETE FROM users WHERE id = ?", tid(1)); !errors.Is(err, injected) {
		t.Errorf("delete: expected injected error, got %v", err)
	}

	// State must be exactly the original row, unchanged: id=2 never inserted,
	// id=1 still age 30 (the update was reverted), id=1 not deleted.
	_, rows, _ := db.Query("SELECT id, age FROM users")
	if len(rows) != 1 || rows[0][0].UUID() != tid(1) || rows[0][1].Int() != 30 {
		t.Errorf("error state must not apply writes; got rows=%v", rows)
	}
}
