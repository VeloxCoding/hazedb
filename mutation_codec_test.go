package hazedb

import (
	"errors"
	"testing"
)

// WAL record decoders must consume their whole buffer: a CRC-valid record with
// trailing bytes is malformed (a correct encoder never emits them), so it is
// rejected as ErrWALCorrupt rather than silently applying the valid prefix.
func TestDecodersRejectTrailingBytes(t *testing.T) {
	pk := UUIDVal(tid(1))
	withTrailer := func(b []byte) []byte { return append(append([]byte{}, b...), 0x99) }

	// insert row body
	row := encodeRow(nil, Row{pk, Str("a"), Int(3)})
	if _, err := decodeRow(row); err != nil {
		t.Fatalf("decodeRow(valid): %v", err)
	}
	if _, err := decodeRow(withTrailer(row)); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("decodeRow(trailing): got %v, want ErrWALCorrupt", err)
	}

	// update op-body (strip the op|tableID prefix the mutation encoder adds)
	noset := func(int, Value) error { return nil }
	upd := encodeUpdateMutation(nil, 1, pk, []int{2}, Row{pk, Str("a"), Int(3)})[3:]
	if _, err := decodeUpdateMutation(upd, noset); err != nil {
		t.Fatalf("decodeUpdateMutation(valid): %v", err)
	}
	if _, err := decodeUpdateMutation(withTrailer(upd), noset); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("decodeUpdateMutation(trailing): got %v, want ErrWALCorrupt", err)
	}

	// delete body (a single PK cell)
	del := encodeCell(nil, pk)
	if _, err := decodeDeleteBody(del); err != nil {
		t.Fatalf("decodeDeleteBody(valid): %v", err)
	}
	if _, err := decodeDeleteBody(withTrailer(del)); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("decodeDeleteBody(trailing): got %v, want ErrWALCorrupt", err)
	}

	// catalog: drop-table and create-table
	if _, err := decodeDropTable(withTrailer(encodeDropTable(nil, "t"))); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("decodeDropTable(trailing): got %v, want ErrWALCorrupt", err)
	}
	td := TableDef{Name: "t", Columns: []ColumnDef{{Name: "id", Type: TypeUUID, PK: true}}}
	ct := encodeCreateTable(nil, 1, td)
	if _, _, err := decodeCreateTable(ct); err != nil {
		t.Fatalf("decodeCreateTable(valid): %v", err)
	}
	if _, _, err := decodeCreateTable(withTrailer(ct)); !errors.Is(err, ErrWALCorrupt) {
		t.Errorf("decodeCreateTable(trailing): got %v, want ErrWALCorrupt", err)
	}
}

// A recTxn envelope with trailing bytes after its declared sub-mutations (here a
// zero-mutation group with a stray byte) is rejected, not silently accepted.
func TestTxnEnvelopeRejectsTrailingBytes(t *testing.T) {
	db := openMem(t)
	if err := db.applyReplayRecord(recTxn, []byte{0, 0, 0x99}); !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("recTxn with trailing byte: got %v, want ErrWALCorrupt", err)
	}
}
