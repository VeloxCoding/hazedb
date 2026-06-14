package hazedb

// SQLite-backed recovery — the read side of the mirror. When the mirror is active
// (WAL on + a persistent companion), the current state lives in SQLite (drained
// segments are deleted), so the engine is rebuilt from the mirror and only the
// undrained WAL tail is replayed on top. Rows enter via rt.insert directly, so
// recovery never re-journals them to the WAL.

import (
	"database/sql"
	"fmt"
	"strings"
)

// recoverFromSQLite reconciles the catalog with the mirror's tables, then loads
// every mirror row into the store. Runs before the undrained WAL tail is
// replayed. Tables created at runtime in a prior session (whose CREATE record
// may have been drained and deleted) are re-added at their durable table IDs.
func (db *DB) recoverFromSQLite() error {
	m := db.sq
	for id, dt := range m.tables {
		cat := db.cat.Load()
		if int(id) < len(cat.byID) && cat.byID[id] != nil {
			// Already present (bootstrap table). The mirror's persisted def is the
			// shape its rows were written in; the catalog def is this session's
			// Open() schema. If they differ, the operator changed the schema between
			// sessions and we would load old-shape rows into the new runtime table.
			// Fail closed — silently mixing shapes is data corruption, not recovery.
			if got := cat.byID[id].table.def.def; !sameTableDef(got, dt.def) {
				return fmt.Errorf("%w: table %q (id %d)", ErrMirrorSchema, dt.name, id)
			}
			continue
		}
		resolved, err := resolveSchema(Schema{Tables: []TableDef{dt.def}})
		if err != nil {
			return fmt.Errorf("recover: resolve %q: %w", dt.name, err)
		}
		rt := &tableRT{table: newTable(resolved[dt.name], db.sizeHint, db.budget), tableID: id}
		db.cat.Store(cat.withTable(rt))
	}
	for id, dt := range m.tables {
		rt := db.cat.Load().byID[id]
		if rt == nil {
			return fmt.Errorf("recover: table id %d missing after reconcile", id)
		}
		if err := db.loadTableRows(m.sdb, dt, rt); err != nil {
			return err
		}
	}
	return nil
}

// loadTableRows reads every row of one mirror table, validates each cell against
// the schema, and inserts it into the in-memory store (declared column order;
// reverse type map). Fails closed on a cell the schema rejects.
func (db *DB) loadTableRows(sdb *sql.DB, dt *drainTable, rt *tableRT) error {
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, c := range dt.cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c.Name))
	}
	b.WriteString(" FROM ")
	b.WriteString(quoteIdent(dt.name))
	rows, err := sdb.Query(b.String())
	if err != nil {
		return fmt.Errorf("recover: scan %q: %w", dt.name, err)
	}
	defer rows.Close()
	ncol := len(dt.cols)
	for rows.Next() {
		raw := make([]any, ncol)
		ptrs := make([]any, ncol)
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		row := make(Row, ncol)
		for i, c := range dt.cols {
			v, err := sqliteValToValue(c.Type, raw[i])
			if err != nil {
				return fmt.Errorf("recover: %q.%q: %w", dt.name, c.Name, err)
			}
			// rt.insert is a boot path and does not validate. SQLite enforces part of
			// the schema on the mirror (NOT NULL, affinity) but not all — invalid UTF-8
			// in a TEXT column slips through — so validate every cell here and fail
			// closed, matching the WAL replay path.
			if err := validateValue(c, v); err != nil {
				return fmt.Errorf("recover: %q.%q: %w: %v", dt.name, c.Name, ErrMirrorCorrupt, err)
			}
			row[i] = v
		}
		if err := rt.insert(row); err != nil {
			return fmt.Errorf("recover: insert into %q: %w", dt.name, err)
		}
	}
	return rows.Err()
}

// sqliteValToValue maps a scanned SQLite value back to a typed hazedb cell,
// using the column's declared type (the inverse of valueToArg / sqlColType).
func sqliteValToValue(t ColumnType, x any) (Value, error) {
	if x == nil {
		return Null(), nil
	}
	switch t {
	case TypeInt:
		if i, ok := x.(int64); ok {
			return Int(i), nil
		}
	case TypeBool:
		// Strict: a BOOL column is stored as INTEGER 0/1 by the drain. Any other
		// integer (2, -1) is a value no write produced — reject it rather than
		// silently coercing every nonzero to true.
		if i, ok := x.(int64); ok && (i == 0 || i == 1) {
			return Bool(i == 1), nil
		}
	case TypeString:
		switch s := x.(type) {
		case string:
			return Str(s), nil
		case []byte:
			return Str(string(s)), nil
		}
	case TypeBytes:
		if b, ok := x.([]byte); ok {
			return Bytes(append([]byte(nil), b...)), nil
		}
	case TypeUUID:
		if b, ok := x.([]byte); ok && len(b) == 16 {
			var u UUID
			copy(u[:], b)
			return UUIDVal(u), nil
		}
	}
	return Value{}, fmt.Errorf("%w: unexpected value %T for column type %v", ErrMirrorCorrupt, x, t)
}

// sameTableDef reports whether two table defs are identical down to column types,
// flags, nullability, and indexes. The persisted def round-trips losslessly through
// encode/decodeCreateTable, so an unchanged schema always compares equal.
func sameTableDef(a, b TableDef) bool {
	if a.Name != b.Name || len(a.Columns) != len(b.Columns) || len(a.Indexes) != len(b.Indexes) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] { // ColumnDef has only comparable fields
			return false
		}
	}
	for i := range a.Indexes {
		x, y := a.Indexes[i], b.Indexes[i]
		if x.Name != y.Name || x.Ordered != y.Ordered || len(x.Columns) != len(y.Columns) {
			return false
		}
		for j := range x.Columns {
			if x.Columns[j] != y.Columns[j] {
				return false
			}
		}
	}
	return true
}
