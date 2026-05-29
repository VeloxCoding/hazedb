package hazedb

import "testing"

// messages is the feed-shaped table: UUID PK, a thread UUID, an immutable
// seq order column (the stable schema M5's tail index will build against),
// and a body.
func msgsSchema() Schema {
	return Schema{Tables: []TableDef{{
		Name: "messages",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeUUID, PK: true},
			{Name: "thread", Type: TypeUUID},
			{Name: "seq", Type: TypeInt, Immutable: true},
			{Name: "body", Type: TypeString},
		},
	}}}
}

// The immutable seq column (and the PK) must reject UPDATE SET at plan time,
// while a normal column updates fine.
func TestSeqAndPKImmutable(t *testing.T) {
	db, err := Open(Options{Schema: msgsSchema()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id, th := NewUUIDv7(), NewUUIDv7()
	if _, err := db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", id, th, 1, "hi"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE messages SET body = ? WHERE id = ?", "edited", id); err != nil {
		t.Fatalf("body update should be allowed: %v", err)
	}
	if _, err := db.Exec("UPDATE messages SET seq = ? WHERE id = ?", 5, id); err == nil {
		t.Error("expected error updating immutable seq column")
	}
	if _, err := db.Exec("UPDATE messages SET id = ? WHERE id = ?", NewUUIDv7(), id); err == nil {
		t.Error("expected error updating PK column")
	}
}

// INSERT may omit the PK (auto-generated UUIDv7) or supply one as a canonical
// string (parsed to UUID at the boundary).
func TestPKAutoGenAndStringSupplied(t *testing.T) {
	db, err := Open(Options{Schema: msgsSchema()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	th := NewUUIDv7()

	// Omitted PK -> auto-generated, valid v7.
	if _, err := db.Exec("INSERT INTO messages (thread, seq, body) VALUES (?, ?, ?)", th, 1, "a"); err != nil {
		t.Fatal(err)
	}
	_, rows, _ := db.Query("SELECT id FROM messages")
	if len(rows) != 1 || !rows[0][0].U.IsV7() {
		t.Fatalf("auto-gen PK should be a valid v7 UUID, got %v", rows)
	}

	// Client-supplied PK as a canonical string is parsed and looked up.
	cid := NewUUIDv7()
	if _, err := db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", cid.String(), th, 2, "b"); err != nil {
		t.Fatalf("string PK should parse: %v", err)
	}
	_, r2, _ := db.Query("SELECT body FROM messages WHERE id = ?", cid)
	if len(r2) != 1 || r2[0][0].S != "b" {
		t.Fatalf("string-supplied PK lookup failed: %v", r2)
	}
}
