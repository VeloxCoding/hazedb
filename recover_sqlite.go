package hazedb

// SQLite-backed recovery — the read side of the mirror. On Open with SQLitePath
// set, the current state lives in SQLite (drained segments are deleted), so the
// engine is rebuilt from the mirror and only the undrained WAL tail is replayed
// on top. WAL-free: rows enter via rt.insert and are never re-journaled.

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
			continue // already present (bootstrap table)
		}
		resolved, err := resolveSchema(Schema{Tables: []TableDef{dt.def}})
		if err != nil {
			return fmt.Errorf("recover: resolve %q: %w", dt.name, err)
		}
		rt := &tableRT{table: newTable(resolved[dt.name], db.sizeHint), tableID: id}
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

// loadTableRows reads every row of one mirror table and inserts it into the
// in-memory store (declared column order; reverse type map).
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
		if i, ok := x.(int64); ok {
			return Bool(i != 0), nil
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
	return Value{}, fmt.Errorf("unexpected sqlite value %T for column type %v", x, t)
}
