package hazedb

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// CREATE TABLE must reject a definition whose names or counts would overflow the
// uint16 fields of the catalog WAL codec. A silent truncation there writes a wrong
// length prefix over full-length bytes, corrupting the record so replay fails on the
// next Open.
func TestCreateTableRejectsOversizedWireDef(t *testing.T) {
	db := openEmpty(t)
	pk := []ColumnDef{{Name: "id", Type: TypeUUID, PK: true}}

	// Table name longer than uint16 can encode.
	if err := db.createTable(TableDef{Name: strings.Repeat("t", math.MaxUint16+1), Columns: pk}); !errors.Is(err, ErrCatalogTooLarge) {
		t.Fatalf("oversized table name: err=%v, want ErrCatalogTooLarge", err)
	}
	// More columns than the uint16 column count can encode.
	if err := db.createTable(TableDef{Name: "wide", Columns: make([]ColumnDef, math.MaxUint16+2)}); !errors.Is(err, ErrCatalogTooLarge) {
		t.Fatalf("oversized column count: err=%v, want ErrCatalogTooLarge", err)
	}
	// A normal definition still creates — the guard does not false-positive.
	if err := db.createTable(TableDef{Name: "ok", Columns: pk}); err != nil {
		t.Fatalf("normal create rejected: %v", err)
	}
}

// A bootstrap schema with more tables than the uint16 table-id space is rejected,
// rather than wrapping a table id to 0 (which would mis-route WAL mutations).
func TestNewCatalogRejectsTooManyTables(t *testing.T) {
	if _, err := newCatalog(Schema{Tables: make([]TableDef, math.MaxUint16+2)}, 0, nil); !errors.Is(err, ErrTooManyTables) {
		t.Fatalf("too many bootstrap tables: err=%v, want ErrTooManyTables", err)
	}
}
