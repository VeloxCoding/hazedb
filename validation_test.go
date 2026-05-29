package hazedb

import "testing"

// A STRING column is text: it accepts only valid UTF-8 (including multibyte),
// and rejects arbitrary bytes at the write boundary.
func TestStringColumnRequiresUTF8(t *testing.T) {
	db := openMem(t)
	id := tid(1)
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", id, "café ☕", 1); err != nil {
		t.Fatalf("valid UTF-8 rejected: %v", err)
	}
	bad := string([]byte{0xff, 0xfe, 0x80}) // 0xff is never valid UTF-8
	if _, err := db.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)", tid(2), bad, 1); err == nil {
		t.Fatal("invalid UTF-8 string accepted, want error")
	}
	// The valid row round-trips intact; the bad one was never stored.
	if _, row, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", id); row == nil || row[0].Str() != "café ☕" {
		t.Fatalf("round-trip: %v", row)
	}
	if _, row, _ := db.QueryRow("SELECT name FROM users WHERE id = ?", tid(2)); row != nil {
		t.Fatalf("rejected row was stored: %v", row)
	}
}

// BYTES is the home for arbitrary (non-UTF-8) bytes — those stay accepted.
func TestBytesColumnAllowsArbitraryBytes(t *testing.T) {
	db := openMem(t)
	if _, err := db.Exec("CREATE TABLE blobs (id uuid primary key, data bytes)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO blobs (id, data) VALUES (?, ?)", tid(1), []byte{0xff, 0x00, 0xfe}); err != nil {
		t.Fatalf("BYTES rejected arbitrary bytes: %v", err)
	}
}
