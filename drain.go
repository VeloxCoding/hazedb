package hazedb

// SQLite mirror + drain — opt-in (Options.SQLitePath). A background loop feeds
// sealed WAL segments into an on-disk SQLite database that holds CURRENT state
// (compacted: an UPDATE overwrites, a DELETE removes), so the mirror is a
// queryable, portable, copy-one-file snapshot of the engine. The drain reuses
// the WAL record framing (scanRecords) and the catalog encoders, and writes one
// SQLite transaction per segment with the segment number recorded in the same
// transaction — so a crash mid-drain leaves SQLite at a clean segment boundary.
//
// modernc.org/sqlite (pure Go, no cgo) is the driver. The drain runs off the
// hot path; reads/writes never touch it. See docs/durability.md.

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// drainTable is the mirror's view of one table: enough to build the row SQL.
type drainTable struct {
	name      string
	cols      []ColumnDef
	pkOrd     int
	insertSQL string // INSERT OR REPLACE INTO "t" (...) VALUES (?,...)
	deleteSQL string // DELETE FROM "t" WHERE "pk" = ?
}

// sqliteMirror owns the SQLite handle and the durable drain cursor.
type sqliteMirror struct {
	sdb         *sql.DB
	tables      map[uint16]*drainTable // tableID -> mirror table (single-threaded: drain only)
	lastDrained uint64                 // highest segment fully committed to SQLite
	minAge      time.Duration          // skip segments sealed more recently than this
}

// execer is satisfied by both *sql.DB and *sql.Tx, so register/apply work
// during seeding (on the DB) and during a drain (in a Tx).
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// newSQLiteMirror opens (creating if needed) the mirror DB, ensures the meta
// tables, loads the drain cursor and the known tables, then seeds any bootstrap
// tables from cat that the mirror does not yet have (the Open() schema is not
// journaled as CREATE TABLE records, so it must be seeded here).
func newSQLiteMirror(path string, cat *catalog, minAge time.Duration) (*sqliteMirror, error) {
	sdb, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("mirror: open %q: %w", path, err)
	}
	// One connection: SQLite is a single writer, and the drain is single-threaded.
	// This also keeps the WAL/page cache warm across drains.
	sdb.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := sdb.Exec(pragma); err != nil {
			sdb.Close()
			return nil, fmt.Errorf("mirror: %q: %w", pragma, err)
		}
	}
	m := &sqliteMirror{sdb: sdb, tables: map[uint16]*drainTable{}, minAge: minAge}
	if err := m.ensureMeta(); err != nil {
		sdb.Close()
		return nil, err
	}
	if err := m.loadCursor(); err != nil {
		sdb.Close()
		return nil, err
	}
	if err := m.loadTables(); err != nil {
		sdb.Close()
		return nil, err
	}
	for _, rt := range cat.byID {
		if rt == nil {
			continue
		}
		if _, ok := m.tables[rt.tableID]; ok {
			continue
		}
		if err := m.register(sdb, rt.tableID, rt.table.def.def); err != nil {
			sdb.Close()
			return nil, err
		}
	}
	return m, nil
}

func (m *sqliteMirror) ensureMeta() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS _hz_meta (k TEXT PRIMARY KEY, v INTEGER)`,
		`CREATE TABLE IF NOT EXISTS _hz_tables (table_id INTEGER PRIMARY KEY, name TEXT, def BLOB)`,
	}
	for _, s := range stmts {
		if _, err := m.sdb.Exec(s); err != nil {
			return fmt.Errorf("mirror: meta: %w", err)
		}
	}
	return nil
}

func (m *sqliteMirror) loadCursor() error {
	var v int64
	err := m.sdb.QueryRow(`SELECT v FROM _hz_meta WHERE k='last_drained_segment'`).Scan(&v)
	if err == sql.ErrNoRows {
		m.lastDrained = 0
		return nil
	}
	if err != nil {
		return fmt.Errorf("mirror: load cursor: %w", err)
	}
	m.lastDrained = uint64(v)
	return nil
}

// loadTables rebuilds the tableID->drainTable map from the persisted defs, so
// the drain knows every table's schema after a restart without re-reading the
// (possibly already-deleted) CREATE TABLE records.
func (m *sqliteMirror) loadTables() error {
	rows, err := m.sdb.Query(`SELECT table_id, def FROM _hz_tables`)
	if err != nil {
		return fmt.Errorf("mirror: load tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var def []byte
		if err := rows.Scan(&id, &def); err != nil {
			return err
		}
		_, td, err := decodeCreateTable(def)
		if err != nil {
			return err
		}
		m.tables[uint16(id)] = buildDrainTable(td)
	}
	return rows.Err()
}

// register creates the SQLite table (idempotent), persists its def, and records
// it in the in-memory map. Used both for bootstrap seeding and for replayed
// CREATE TABLE records.
func (m *sqliteMirror) register(e execer, tableID uint16, td TableDef) error {
	if _, err := e.Exec(createTableSQL(td)); err != nil {
		return fmt.Errorf("mirror: create %q: %w", td.Name, err)
	}
	if _, err := e.Exec(
		`INSERT OR REPLACE INTO _hz_tables (table_id, name, def) VALUES (?, ?, ?)`,
		int64(tableID), td.Name, encodeCreateTable(nil, tableID, td),
	); err != nil {
		return fmt.Errorf("mirror: record table %q: %w", td.Name, err)
	}
	m.tables[tableID] = buildDrainTable(td)
	return nil
}

func (m *sqliteMirror) close() error {
	if m == nil || m.sdb == nil {
		return nil
	}
	return m.sdb.Close()
}

// --- SQL building -----------------------------------------------------------

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// sqlColType maps a hazedb column type to a SQLite storage class. UUID and BYTES
// are stored as BLOB (UUID = the raw 16 bytes); BOOL is an INTEGER 0/1.
func sqlColType(t ColumnType) string {
	switch t {
	case TypeInt, TypeBool:
		return "INTEGER"
	case TypeString:
		return "TEXT"
	case TypeBytes, TypeUUID:
		return "BLOB"
	}
	return "BLOB"
}

func createTableSQL(td TableDef) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdent(td.Name))
	b.WriteString(" (")
	for i, c := range td.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdent(c.Name))
		b.WriteByte(' ')
		b.WriteString(sqlColType(c.Type))
		if c.PK {
			b.WriteString(" PRIMARY KEY")
		} else if !c.Nullable {
			b.WriteString(" NOT NULL")
		}
	}
	b.WriteByte(')')
	return b.String()
}

func buildDrainTable(td TableDef) *drainTable {
	dt := &drainTable{name: td.Name, cols: td.Columns, pkOrd: 0}
	var cols, ph strings.Builder
	for i, c := range td.Columns {
		if c.PK {
			dt.pkOrd = i
		}
		if i > 0 {
			cols.WriteString(", ")
			ph.WriteString(", ")
		}
		cols.WriteString(quoteIdent(c.Name))
		ph.WriteByte('?')
	}
	dt.insertSQL = "INSERT OR REPLACE INTO " + quoteIdent(td.Name) + " (" + cols.String() + ") VALUES (" + ph.String() + ")"
	dt.deleteSQL = "DELETE FROM " + quoteIdent(td.Name) + " WHERE " + quoteIdent(td.Columns[dt.pkOrd].Name) + " = ?"
	return dt
}

// valueToArg converts a hazedb cell into a database/sql argument. Byte-backed
// values (BYTES, UUID) are copied so the driver never aliases WAL-decode memory.
func valueToArg(v Value) any {
	switch v.Kind {
	case KindNull:
		return nil
	case KindInt:
		return v.Int()
	case KindBool:
		if v.Bool() {
			return int64(1)
		}
		return int64(0)
	case KindString:
		return v.Str()
	case KindBytes:
		b := v.Bytes()
		return append([]byte(nil), b...)
	case KindUUID:
		u := v.UUID()
		return append([]byte(nil), u[:]...)
	}
	return nil
}

// --- drain ------------------------------------------------------------------

// drainOnce drains every sealed segment past the cursor into SQLite, one segment
// per transaction. When respectAge is true it stops at the first segment sealed
// more recently than minAge (settled-history only); the final drain on Close
// passes false to flush everything. A no-op when the mirror or WAL is absent.
func (db *DB) drainOnce(respectAge bool) error {
	if db.sq == nil || db.wal == nil {
		return nil
	}
	sealed, err := db.wal.sealedSegments()
	if err != nil {
		return err
	}
	for _, n := range sealed {
		if n <= db.sq.lastDrained {
			continue
		}
		path := db.wal.segPath(n)
		if respectAge && db.sq.minAge > 0 {
			fi, err := os.Stat(path)
			if err != nil {
				return err
			}
			if time.Since(fi.ModTime()) < db.sq.minAge {
				break // too young; this and all later segments wait for the next pass
			}
		}
		if err := db.drainSegment(n, path); err != nil {
			return err
		}
	}
	return nil
}

// drainSegment applies one sealed segment's records to SQLite in a single
// transaction and advances the durable cursor in that same transaction.
func (db *DB) drainSegment(n uint64, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	tx, err := db.sq.sdb.Begin()
	if err != nil {
		return err
	}
	applyErr := scanRecords(f, func(recType uint8, payload []byte) error {
		return db.sq.applyRecord(tx, recType, payload)
	})
	if applyErr != nil {
		tx.Rollback()
		return fmt.Errorf("mirror: drain segment %d: %w", n, applyErr)
	}
	if _, err := tx.Exec(
		`INSERT INTO _hz_meta (k, v) VALUES ('last_drained_segment', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`,
		int64(n),
	); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	db.sq.lastDrained = n
	return nil
}

// applyRecord mirrors replayWAL's dispatch, but writes to SQLite instead of the
// in-memory store. Single-threaded (drain only), so the tables map is mutated
// without a lock.
func (m *sqliteMirror) applyRecord(tx *sql.Tx, recType uint8, payload []byte) error {
	switch recType {
	case recCreateTable:
		tableID, td, err := decodeCreateTable(payload)
		if err != nil {
			return err
		}
		return m.register(tx, tableID, td)
	case recDropTable:
		name, err := decodeDropTable(payload)
		if err != nil {
			return err
		}
		return m.dropTable(tx, name)
	case recCheckpoint:
		return nil
	case recMutation:
		return m.applyMutation(tx, payload)
	case recTxn:
		if len(payload) < 2 {
			return fmt.Errorf("%w: short txn payload", ErrWALCorrupt)
		}
		count := int(binary.LittleEndian.Uint16(payload[0:2]))
		off := 2
		for i := 0; i < count; i++ {
			if off+4 > len(payload) {
				return fmt.Errorf("%w: txn sub-mutation length truncated", ErrWALCorrupt)
			}
			mlen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
			off += 4
			if mlen < 0 || off+mlen > len(payload) {
				return fmt.Errorf("%w: txn sub-mutation body truncated", ErrWALCorrupt)
			}
			if err := m.applyMutation(tx, payload[off:off+mlen]); err != nil {
				return err
			}
			off += mlen
		}
		return nil
	}
	return fmt.Errorf("%w: unknown record type %d", ErrWALCorrupt, recType)
}

func (m *sqliteMirror) dropTable(tx *sql.Tx, name string) error {
	if _, err := tx.Exec("DROP TABLE IF EXISTS " + quoteIdent(name)); err != nil {
		return err
	}
	for id, dt := range m.tables {
		if dt.name == name {
			if _, err := tx.Exec(`DELETE FROM _hz_tables WHERE table_id = ?`, int64(id)); err != nil {
				return err
			}
			delete(m.tables, id)
			break
		}
	}
	return nil
}

func (m *sqliteMirror) applyMutation(tx *sql.Tx, payload []byte) error {
	if len(payload) < 3 {
		return fmt.Errorf("%w: short mutation payload", ErrWALCorrupt)
	}
	op := payload[0]
	tableID := binary.LittleEndian.Uint16(payload[1:3])
	body := payload[3:]
	dt := m.tables[tableID]
	if dt == nil {
		return fmt.Errorf("%w: mutation for unknown table id %d", ErrWALCorrupt, tableID)
	}
	switch op {
	case opInsert:
		row, err := decodeRow(body)
		if err != nil {
			return err
		}
		args := make([]any, len(row))
		for i, v := range row {
			args[i] = valueToArg(v)
		}
		_, err = tx.Exec(dt.insertSQL, args...)
		return err
	case opUpdate:
		pk, n, err := decodeCell(body)
		if err != nil {
			return err
		}
		body = body[n:]
		if len(body) < 1 {
			return fmt.Errorf("%w: update missing nsets", ErrWALCorrupt)
		}
		nsets := int(body[0])
		body = body[1:]
		var set strings.Builder
		args := make([]any, 0, nsets+1)
		for i := 0; i < nsets; i++ {
			if len(body) < 2 {
				return fmt.Errorf("%w: update ordinal truncated", ErrWALCorrupt)
			}
			ord := int(binary.LittleEndian.Uint16(body[0:2]))
			body = body[2:]
			v, sz, err := decodeCell(body)
			if err != nil {
				return err
			}
			body = body[sz:]
			if ord < 0 || ord >= len(dt.cols) {
				return fmt.Errorf("%w: update ordinal %d out of range", ErrWALCorrupt, ord)
			}
			if i > 0 {
				set.WriteString(", ")
			}
			set.WriteString(quoteIdent(dt.cols[ord].Name))
			set.WriteString(" = ?")
			args = append(args, valueToArg(v))
		}
		args = append(args, valueToArg(pk))
		sqlStr := "UPDATE " + quoteIdent(dt.name) + " SET " + set.String() +
			" WHERE " + quoteIdent(dt.cols[dt.pkOrd].Name) + " = ?"
		_, err = tx.Exec(sqlStr, args...)
		return err
	case opDelete:
		pk, _, err := decodeCell(body)
		if err != nil {
			return err
		}
		_, err = tx.Exec(dt.deleteSQL, valueToArg(pk))
		return err
	}
	return fmt.Errorf("%w: unknown op %d", ErrWALCorrupt, op)
}

// --- drain loop (mirrors the index merger lifecycle) ------------------------

func (db *DB) startDrainLoop(interval time.Duration) {
	db.drainStop = make(chan struct{})
	db.drainDone = make(chan struct{})
	go func() {
		defer close(db.drainDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-db.drainStop:
				// Final drain: seal the active segment so its records reach the
				// mirror, then drain everything regardless of age.
				db.wal.rotate()
				_ = db.drainOnce(false)
				return
			case <-t.C:
				_ = db.drainOnce(true)
			}
		}
	}()
}

func (db *DB) stopDrainLoop() {
	if db.drainStop == nil {
		return
	}
	close(db.drainStop)
	<-db.drainDone
	db.drainStop = nil
}
