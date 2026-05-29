package spike

import (
	"os"
	"path/filepath"
	"testing"
)

func tmpWAL(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "wal.log")
}

func TestInsertGet(t *testing.T) {
	db, err := Open(tmpWAL(t), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Insert(User{ID: "u1", Email: "a@x", Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Get("u1")
	if err != nil || !ok {
		t.Fatalf("get failed: ok=%v err=%v", ok, err)
	}
	if got.Email != "a@x" || got.Name != "Alice" {
		t.Fatalf("bad row: %+v", got)
	}
}

func TestDuplicateRejected(t *testing.T) {
	db, _ := Open(tmpWAL(t), false)
	defer db.Close()
	if _, err := db.Insert(User{ID: "u1", Email: "a", Name: "A"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert(User{ID: "u1", Email: "b", Name: "B"}); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestUpdateDelete(t *testing.T) {
	db, _ := Open(tmpWAL(t), false)
	defer db.Close()
	db.Insert(User{ID: "u1", Email: "a", Name: "A"})
	if err := db.Update(User{ID: "u1", Email: "a2", Name: "A2"}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := db.Get("u1")
	if got.Email != "a2" {
		t.Fatalf("update lost: %+v", got)
	}
	if err := db.Delete("u1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Get("u1"); ok {
		t.Fatal("delete lost")
	}
}

func TestRecovery(t *testing.T) {
	path := tmpWAL(t)

	// Phase 1: write some data with sync WAL.
	db, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	db.Insert(User{ID: "u1", Email: "a@x", Name: "Alice"})
	db.Insert(User{ID: "u2", Email: "b@x", Name: "Bob"})
	db.Update(User{ID: "u1", Email: "a2@x", Name: "Alice2"})
	db.Delete("u2")
	db.Insert(User{ID: "u3", Email: "c@x", Name: "Carol"})
	db.Close()

	// Phase 2: reopen, expect same logical state.
	db2, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	u1, ok, _ := db2.Get("u1")
	if !ok || u1.Email != "a2@x" {
		t.Fatalf("u1 recovery wrong: ok=%v u=%+v", ok, u1)
	}
	if _, ok, _ := db2.Get("u2"); ok {
		t.Fatal("u2 should be deleted")
	}
	u3, ok, _ := db2.Get("u3")
	if !ok || u3.Name != "Carol" {
		t.Fatalf("u3 recovery wrong: %+v", u3)
	}

	// Inserting a new row after recovery should not collide.
	if _, err := db2.Insert(User{ID: "u4", Email: "d@x", Name: "Dan"}); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptionDetected(t *testing.T) {
	path := tmpWAL(t)
	db, _ := Open(path, true)
	db.Insert(User{ID: "u1", Email: "a", Name: "A"})
	db.Close()

	// Flip a byte in the middle of the file.
	data, _ := os.ReadFile(path)
	if len(data) < 20 {
		t.Fatal("wal too short to corrupt")
	}
	data[15] ^= 0xFF
	os.WriteFile(path, data, 0644)

	if _, err := Open(path, false); err == nil {
		t.Fatal("expected corruption error on replay")
	}
}
