package hazedb

import (
	"strings"
	"testing"
)

// A PartitionKey column is immutable: UPDATE SET on it is rejected at plan
// time (a partition move is DELETE + INSERT). Until the partitioned storage
// core lands, a partitioned table still functions as a normal UUID-PK table.
func TestPartitionKeyImmutableAndUsable(t *testing.T) {
	db, err := Open(Options{Schema: msgsSchema()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id, th := NewUUIDv7(), NewUUIDv7()
	if _, err := db.Exec("INSERT INTO messages (id, thread, seq, body) VALUES (?, ?, ?, ?)", id, th, 1, "hi"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE messages SET thread = ? WHERE id = ?", NewUUIDv7(), id); err == nil {
		t.Error("expected error updating the PartitionKey column")
	}
	// Reads still work (routed by PK for now).
	_, rows, err := db.Query("SELECT body FROM messages WHERE id = ?", id)
	if err != nil || len(rows) != 1 || rows[0][0].Str() != "hi" {
		t.Fatalf("read failed: rows=%v err=%v", rows, err)
	}
}

// PartitionKey validation: must be UUID, at most one, distinct from the PK.
func TestPartitionKeySchemaValidation(t *testing.T) {
	cases := []struct {
		name string
		cols []ColumnDef
		want string
	}{
		{
			"non-uuid partition key",
			[]ColumnDef{{Name: "id", Type: TypeUUID, PK: true}, {Name: "p", Type: TypeInt, PartitionKey: true}},
			"must be UUID",
		},
		{
			"two partition keys",
			[]ColumnDef{{Name: "id", Type: TypeUUID, PK: true}, {Name: "a", Type: TypeUUID, PartitionKey: true}, {Name: "b", Type: TypeUUID, PartitionKey: true}},
			"multiple PartitionKey",
		},
		{
			"partition key on the pk column",
			[]ColumnDef{{Name: "id", Type: TypeUUID, PK: true, PartitionKey: true}},
			"different column than the PK",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Open(Options{Schema: Schema{Tables: []TableDef{{Name: "t", Columns: c.cols}}}})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
		})
	}
}
