package hazedb

// SQLite companion + data mirror. The companion is a file next to the WAL
// (Options.CompanionPath; default hazedb.db inside WALPath) and is always
// present — a real on-disk file in every mode, never in-memory. It always holds
// the _hz_events operational log; when WAL is on it additionally becomes the
// data mirror: a background loop feeds sealed WAL segments into it as CURRENT
// state (compacted: an UPDATE overwrites, a DELETE removes), so the mirror is a
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// drainTable is the mirror's view of one table: enough to build the row SQL and
// to rebuild the in-memory table on SQLite-backed recovery (def carries columns,
// types, and indexes).
type drainTable struct {
	name      string
	def       TableDef
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
}

// execer is satisfied by both *sql.DB and *sql.Tx, so register/apply work
// during seeding (on the DB) and during a drain (in a Tx).
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// openCompanion opens (creating if needed) the SQLite companion file at path —
// always a real file, never in-memory. The handle is configured but holds no
// tables yet; the data-mirror tables are added by activateMirror.
func openCompanion(path string) (*sqliteMirror, error) {
	// The companion file lives inside WALPath, and openCompanion runs before
	// openWAL creates that directory — so ensure the parent exists first.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("companion: mkdir %q: %w", filepath.Dir(path), err)
	}
	sdb, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("companion: open %q: %w", path, err)
	}
	// One connection: SQLite is a single writer, the drain is single-threaded, and
	// it keeps the page cache warm across drains.
	sdb.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		// FULL, not NORMAL: the drain reclaims a hazedb WAL segment right after its
		// SQLite commit, so that commit must be power-loss durable — otherwise a
		// power loss could roll back the (NORMAL, unsynced) commit while the segment
		// that held the same writes is already deleted, losing data the WAL had
		// already fsynced (durability.md §5). FULL fsyncs the SQLite WAL per commit;
		// the cost lands on the background drain (one fsync per ~1 MiB segment), not
		// on any user write.
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := sdb.Exec(pragma); err != nil {
			sdb.Close()
			return nil, fmt.Errorf("companion: %q: %w", pragma, err)
		}
	}
	comp := &sqliteMirror{sdb: sdb, tables: map[uint16]*drainTable{}}
	if err := comp.ensureOps(); err != nil {
		sdb.Close()
		return nil, err
	}
	return comp, nil
}

// ensureOps creates the companion's operational tables: today the _hz_events log;
// periodic /meta samples land here later. Run by openCompanion, before any mirror
// activation.
func (m *sqliteMirror) ensureOps() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS _hz_events (
	id      INTEGER PRIMARY KEY,
	ts      INTEGER NOT NULL,
	level   TEXT    NOT NULL,
	kind    TEXT    NOT NULL,
	message TEXT    NOT NULL,
	data    TEXT
)`,
		`CREATE INDEX IF NOT EXISTS _hz_events_ts ON _hz_events(ts)`,
	}
	for _, s := range stmts {
		if _, err := m.sdb.Exec(s); err != nil {
			return fmt.Errorf("companion: ops tables: %w", err)
		}
	}
	return nil
}

// activateMirror turns the companion into the data mirror: ensure the mirror meta
// tables, load the drain cursor and the known tables, then seed any bootstrap
// tables from cat the mirror does not yet have (the Open() schema is not
// journaled as CREATE TABLE records, so it must be seeded here). Called from Open
// only when the mirror is enabled (WAL on + a persistent companion); Open closes
// the companion if this fails.
func (m *sqliteMirror) activateMirror(cat *catalog) error {
	if err := m.ensureMirrorMeta(); err != nil {
		return err
	}
	if err := m.loadCursor(); err != nil {
		return err
	}
	if err := m.loadTables(); err != nil {
		return err
	}
	for _, rt := range cat.byID {
		if rt == nil {
			continue
		}
		if _, ok := m.tables[rt.tableID]; ok {
			continue
		}
		if err := m.register(m.sdb, rt.tableID, rt.table.def.def); err != nil {
			return err
		}
	}
	return nil
}

// ensureMirrorMeta creates the mirror's bookkeeping tables: the drain cursor
// (_hz_meta) and the table registry (_hz_tables). Created only when the mirror is
// active; the operational _hz_events table is separate and always present.
func (m *sqliteMirror) ensureMirrorMeta() error {
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
	if v < 0 {
		// A negative cursor would become a near-max uint64, so removeDrainedSegments
		// would delete every WAL tail segment and replay would skip them all. Fail
		// closed on the impossible value instead of silently destroying the tail.
		return fmt.Errorf("%w: companion drain cursor is negative (%d)", ErrWALCorrupt, v)
	}
	m.lastDrained = uint64(v)
	return nil
}

// loadTables rebuilds the tableID->drainTable map from the persisted defs, so
// the drain knows every table's schema after a restart without re-reading the
// (possibly already-deleted) CREATE TABLE records.
func (m *sqliteMirror) loadTables() error {
	rows, err := m.sdb.Query(`SELECT table_id, name, def FROM _hz_tables`)
	if err != nil {
		return fmt.Errorf("mirror: load tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var name string
		var def []byte
		if err := rows.Scan(&id, &name, &def); err != nil {
			return err
		}
		if id < 0 || id > 0xFFFF { // uint16 range: a wrapped id would bind a table under the wrong key
			return fmt.Errorf("%w: companion table_id %d out of range", ErrWALCorrupt, id)
		}
		decID, td, err := decodeCreateTable(def)
		if err != nil {
			return err
		}
		// The registry columns are redundant with the def BLOB (register writes both
		// from the same table); a disagreement means corruption — fail closed rather
		// than bind a table under an id/name that does not match its own def.
		if decID != uint16(id) || td.Name != name {
			return fmt.Errorf("%w: companion table row (id=%d, name=%q) disagrees with its def (id=%d, name=%q)", ErrWALCorrupt, id, name, decID, td.Name)
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
	dt := &drainTable{name: td.Name, def: td, cols: td.Columns, pkOrd: 0}
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
// The copy must be a NON-NIL slice even when empty: database/sql stores a nil
// []byte as SQL NULL, which would erase the empty-bytes vs NULL distinction —
// so make+copy is used rather than append([]byte(nil), …) (nil for empty input).
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
		cp := make([]byte, len(b))
		copy(cp, b)
		return cp
	case KindUUID:
		u := v.UUID()
		cp := make([]byte, len(u))
		copy(cp, u[:])
		return cp
	}
	return nil
}

// --- drain ------------------------------------------------------------------

// drainOnce drains every sealed segment past the cursor into SQLite, one segment
// per transaction. sealedSegments returns only segments below the active one, so
// every candidate is already flushed, fsynced, and closed — there is no open
// file to race and no age gate is needed. A no-op when the mirror or WAL is absent.
//
// The undrained range is contiguous above the cursor (born-sealed numbers never
// skip; drained segments are reclaimed from the bottom), so a gap means a
// committed segment vanished. drainOnce drains the run below the gap and then
// stops with errWALMissingSegment, leaving the cursor below the gap — advancing
// past it would mirror later mutations onto a missing base. The drain loop logs
// and retries on the next tick.
func (db *DB) drainOnce() error {
	if !db.mirrorOn || db.wal == nil {
		return nil
	}
	sealed, err := db.wal.sealedSegments()
	if err != nil {
		return err
	}
	expected := db.sq.lastDrained + 1
	for _, n := range sealed {
		if n < expected {
			continue // already drained
		}
		if n != expected {
			return fmt.Errorf("%w: missing segment %d before %d above drain cursor", errWALMissingSegment, expected, n)
		}
		if err := db.drainSegment(n, db.wal.segPath(n)); err != nil {
			return err
		}
		expected = n + 1
	}
	return nil
}

// drainSegment applies one sealed segment's records to SQLite in a single
// transaction and advances the durable cursor in that same transaction.
//
// A bit-rot segment (bad magic / CRC mismatch — errWALFraming) is not fatal: the
// good prefix already in the transaction is committed, the cursor advances past
// the WHOLE segment, and the unreadable suffix is dropped with a logged event — so
// random corruption can never stall the mirror (the old code rolled back and the
// loop retried the same segment forever). Any OTHER error — a transient SQLite/IO
// failure, or an intact-but-unhandleable record (version, unknown type, a
// CRC-valid payload that won't decode) — rolls back and leaves the cursor unmoved,
// so the loop retries on the next tick rather than silently dropping a committed
// record.
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
	if applyErr != nil && !errors.Is(applyErr, errWALFraming) {
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
	// Reclaim WAL disk: this segment's records are now durably in SQLite, which
	// is the recovery source for everything up to lastDrained.
	_ = os.Remove(path)
	// Log AFTER the commit: logEvent writes to the companion on the same single
	// connection the transaction above held, so logging mid-tx would deadlock.
	if applyErr != nil {
		db.logEvent("error", "wal-corruption", fmt.Sprintf("segment %d during drain: %v — good prefix mirrored, suffix skipped", n, applyErr))
	}
	return nil
}

// applyRecord mirrors applyReplayRecord's dispatch, but writes to SQLite instead of the
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
		return forEachTxnMutation(payload, func(mut []byte) error {
			return m.applyMutation(tx, mut)
		})
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

// applyMutation mirrors one decoded mutation into SQLite. It validates against
// the schema first — exactly as the in-memory replay does (validateInsertRow,
// validateValue, coerceToUUID) — because the companion is dynamically typed and
// would otherwise store a wrong-typed CRC-valid record without error. A bad record
// returns ErrWALCorrupt, so drainSegment rolls back and leaves the cursor unmoved
// rather than committing garbage into the recovery base.
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
		if err := validateInsertRow(dt.cols, row); err != nil {
			return err
		}
		args := make([]any, len(row))
		for i, v := range row {
			args[i] = valueToArg(v)
		}
		_, err = tx.Exec(dt.insertSQL, args...)
		return err
	case opUpdate:
		var set strings.Builder
		var args []any // changed-column values in order; pk appended at the end
		pk, err := decodeUpdateMutation(body, func(ord int, v Value) error {
			if ord < 0 || ord >= len(dt.cols) {
				return fmt.Errorf("%w: update ordinal %d out of range", ErrWALCorrupt, ord)
			}
			if err := validateValue(dt.cols[ord], v); err != nil {
				return fmt.Errorf("%w: update %v", ErrWALCorrupt, err)
			}
			if set.Len() > 0 {
				set.WriteString(", ")
			}
			set.WriteString(quoteIdent(dt.cols[ord].Name))
			set.WriteString(" = ?")
			args = append(args, valueToArg(v))
			return nil
		})
		if err != nil {
			return err
		}
		pkU, err := coerceToUUID(pk)
		if err != nil {
			return fmt.Errorf("%w: update %v", ErrWALCorrupt, err)
		}
		args = append(args, valueToArg(UUIDVal(pkU)))
		sqlStr := "UPDATE " + quoteIdent(dt.name) + " SET " + set.String() +
			" WHERE " + quoteIdent(dt.cols[dt.pkOrd].Name) + " = ?"
		_, err = tx.Exec(sqlStr, args...)
		return err
	case opDelete:
		pk, err := decodeDeleteBody(body)
		if err != nil {
			return err
		}
		pkU, err := coerceToUUID(pk)
		if err != nil {
			return fmt.Errorf("%w: delete %v", ErrWALCorrupt, err)
		}
		_, err = tx.Exec(dt.deleteSQL, valueToArg(UUIDVal(pkU)))
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
				// Final drain: seal the pending buffer so its records reach the
				// mirror, then drain every sealed segment.
				_ = db.wal.flush()
				if err := db.drainOnce(); err != nil {
					db.logEvent("error", "drain-error", err.Error())
				}
				return
			case <-t.C:
				if err := db.drainOnce(); err != nil {
					db.logEvent("error", "drain-error", err.Error())
				}
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
