package hazedb

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// table is the runtime form of a resolvedTable with its storage attached.
type tableRT struct {
	*table
	tableID uint16
}

// --- Gateway contract -------------------------------------------------------
//
// *DB is the single official entry point — the "gateway". Every consumer enters
// through it: Caddy calls these methods as native Go, and the FrankenPHP/PHP
// extension reaches them via cgo (C → exported Go → these same methods). There
// is no second transport — the PHP path is cgo calling the very same verbs, not
// a parallel API. The gateway verbs are Open/Close/FlushWAL/Exec/Query/QueryRow
// here, plus Transaction (txn.go).
//
// Every verb upholds these guarantees, so all consumers inherit them for free:
//
//   - Validation. SQL is parsed, planned, and bound to the live catalog in
//     prepare(); args are type-coerced in toValue. Bad SQL or args fail here.
//   - Boundary clone. []byte/Value args are deep-copied on the way in (storage
//     never aliases caller memory), and returned rows are deep-cloned on the way
//     out (callers may retain them past later writes).
//   - No bypass. table/shard/catalog/wal are unexported, so no consumer can
//     reach storage around the validated verbs.
//
// Boundary rule: db semantics live behind the gateway (this package);
// cross-cutting concerns — auth, tenancy, logging, and the PHP↔Go marshalling
// the extension needs — live in the consumer/adapter, which then calls these
// same verbs. Never push consumer-specific concerns into the core.
//
// DB is the embedded database handle. One DB per process per WAL
// path. Open is goroutine-safe; Exec and Query are goroutine-safe.
type DB struct {
	schema   Schema // bootstrap schema, re-applied each Open
	sizeHint int

	// budget is the store-wide byte-capacity admission control (MaxBytes). One
	// per DB, shared by every table. max == 0 (the default) makes it a no-op.
	budget *byteBudget

	// cat is the live table catalog, published atomically. Reads/writes load
	// it lock-free; DDL swaps in a new one. ddlMu serialises CREATE/DROP only.
	cat   atomic.Pointer[catalog]
	ddlMu sync.Mutex

	wal     *wal
	scratch *scratchPool

	// stmtCache memoises (SQL → *plan). A cached plan is stamped with the
	// catalog version it was bound against; prepare re-binds it when the
	// catalog has changed since (CREATE/DROP), so a plan never points at a
	// stale table.
	stmtCache sync.Map

	// mergeStop/mergeDone drive the background secondary-index merger (nil when
	// the merge loop is disabled). Closing mergeStop runs a final drain and the
	// goroutine then closes mergeDone. See docs/secondary-indexes.md.
	mergeStop chan struct{}
	mergeDone chan struct{}

	// compactStop/compactDone drive the background arena-compaction sweeper (nil
	// when disabled). See compact.go.
	compactStop chan struct{}
	compactDone chan struct{}

	// sq is the on-disk SQLite mirror (nil when SQLitePath is unset). The drain
	// loop feeds sealed WAL segments into it; drainStop/drainDone drive that
	// goroutine, mirroring the merger. See docs/durability.md.
	sq        *sqliteMirror
	drainStop chan struct{}
	drainDone chan struct{}
}

// Open prepares the database. If WALPath is non-empty, the file is
// opened and any existing records are replayed into memory before
// Open returns. Open is blocking until replay completes.
func Open(opts Options) (*DB, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	opts.applyDefaults()
	budget := &byteBudget{max: opts.MaxBytes}
	// An empty schema is allowed — tables can be created at runtime.
	cat, err := newCatalog(opts.Schema, opts.sizeHint, budget)
	if err != nil {
		return nil, err
	}
	db := &DB{
		schema:   opts.Schema,
		sizeHint: opts.sizeHint,
		budget:   budget,
		scratch:  newScratchPool(),
	}
	db.cat.Store(cat)
	if opts.walEnabled() {
		w, err := openWAL(opts.WALPath)
		if err != nil {
			return nil, err
		}
		// Recover from existing segments before starting the flusher, so no
		// background flush races a replay reader.
		if opts.SQLitePath != "" {
			// SQLite-backed recovery: the mirror is the base. Load it into memory,
			// then replay only the undrained WAL tail (segments past the drained
			// cursor) on top.
			m, merr := newSQLiteMirror(opts.SQLitePath, db.cat.Load())
			if merr != nil {
				w.close()
				return nil, merr
			}
			db.sq = m
			if err := db.recoverFromSQLite(); err != nil {
				w.close()
				m.close()
				return nil, err
			}
			if err := w.removeDrainedSegments(m.lastDrained); err != nil {
				w.close()
				m.close()
				return nil, err
			}
			if err := w.replayFrom(m.lastDrained, db.applyReplayRecord, db.onWALCorrupt); err != nil {
				w.close()
				m.close()
				return nil, err
			}
			// Keep segment numbers above the drained cursor across restarts: the
			// drain deletes the segments it consumes, so the highest on-disk segment
			// can fall below lastDrained. Without this, a post-restart flush could
			// reuse a number <= lastDrained that drainOnce would skip forever —
			// those writes would never reach the mirror and be lost on next recovery.
			if w.seg < m.lastDrained {
				w.seg = m.lastDrained
			}
		} else {
			// No mirror: the WAL segments themselves replay into memory.
			if err := db.replayWAL(w); err != nil {
				w.close()
				return nil, err
			}
		}
		db.wal = w
		w.startFlusher(opts.walFlushInterval)
		// Replay marked rows dirty but never built the indexes; rebuild them from
		// the live rows now, so reads are index-fast before serving.
		db.rebuildAllIndexes()

		if opts.SQLitePath != "" && opts.drainInterval > 0 {
			db.startDrainLoop(opts.drainInterval)
		}
	}
	if opts.indexMergeInterval > 0 {
		db.startMergeLoop(opts.indexMergeInterval, opts.indexMergeThreshold)
	}
	if opts.compactInterval > 0 {
		db.startCompactLoop(opts.compactInterval)
	}
	return db, nil
}

// Close stops the SQLite drain loop (which seals the active segment and runs a
// final drain so the mirror is current), then the index merger, then flushes
// and closes the WAL, then closes the mirror. Memory-only DBs still stop the
// merger. The drain loop is stopped first, while the WAL is still open.
func (db *DB) Close() error {
	db.stopDrainLoop()
	db.stopMergeLoop()
	db.stopCompactLoop()
	var err error
	if db.wal != nil {
		err = db.wal.close()
	}
	if db.sq != nil {
		if cerr := db.sq.close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// FlushWAL forces bufio to fsync. Use before reading a record back
// from disk in tests, or for an explicit durability boundary in
// callers. Memory-only DBs are a no-op.
func (db *DB) FlushWAL() error {
	if db.wal == nil {
		return nil
	}
	return db.wal.flush()
}

// Exec runs an INSERT, UPDATE, DELETE, CREATE TABLE, or DROP TABLE. Returns
// the affected row count (0 for DDL).
func (db *DB) Exec(sql string, args ...any) (int, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return 0, err
	}
	return db.execPlan(pl, args)
}

// ExecValues runs a write with pre-typed Value arguments, skipping the []any
// arg-conversion layer Exec uses (no JSON decode, no interface boxing, no
// per-arg type switch). It is the in-process fast path for callers that already
// hold typed Values — notably the PHP extension reading a native zend_array via
// cgo, which avoids the json_encode/json.Decode round-trip the string args form
// pays. Returns the affected row count (0 for DDL).
func (db *DB) ExecValues(sql string, args ...Value) (int, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return 0, err
	}
	return db.execPlanValues(pl, args)
}

// execPlan runs a non-SELECT plan against raw args. Shared by Exec (which looks
// the plan up by SQL each call) and *Stmt.Exec (which holds a compiled plan).
func (db *DB) execPlan(pl *plan, args []any) (int, error) {
	var argBuf [8]Value
	vargs, err := toValuesInto(args, argBuf[:])
	if err != nil {
		return 0, err
	}
	return db.execWrite(pl, vargs)
}

// execPlanValues is execPlan for pre-typed args: it clones each arg with
// cloneValue (a no-op except for KindBytes, which must not alias caller memory
// across the write boundary — the same guarantee toValue gives the []any path)
// and dispatches to the same write executors.
func (db *DB) execPlanValues(pl *plan, args []Value) (int, error) {
	var argBuf [8]Value
	var vargs []Value
	if len(args) <= len(argBuf) {
		vargs = argBuf[:len(args)]
	} else {
		vargs = make([]Value, len(args))
	}
	for i, a := range args {
		vargs[i] = cloneValue(a)
	}
	return db.execWrite(pl, vargs)
}

// execWrite dispatches a write plan to its executor. Shared by execPlan (any
// args) and execPlanValues (pre-typed Value args) once each has converted its
// args; DDL ignores vargs.
// checkArgs rejects an arg count that does not match the statement's parameter
// count. Standard drivers fail loud on a count mismatch in either direction;
// before this, too many args were silently ignored and too few hit a per-param
// bounds error deeper in.
func (pl *plan) checkArgs(n int) error {
	if n != pl.nparams {
		return fmt.Errorf("%w: got %d args, statement has %d parameters", ErrParamMismatch, n, pl.nparams)
	}
	return nil
}

// pkKeyFromArgs resolves a PK-lookup key straight from the raw args, converting
// only the key arg — skipping the []Value slice toValues allocates per call.
// ok=false sends the caller down the generic toValues path, which must own
// every mismatch so errors keep their exact text and order (toValues' type
// error before checkArgs' count error).
func (pl *plan) pkKeyFromArgs(args []any) (key Value, ok bool, err error) {
	if len(args) != pl.nparams {
		return Value{}, false, nil
	}
	switch src := pl.pkSource.(type) {
	case *paramRef:
		// A SELECT PK lookup is the bare `pk = ?` equality (detectPKEq), so the
		// key is the statement's only parameter. More params would mean another
		// arg could fail conversion — only the generic path orders those errors
		// right, so any other shape falls back.
		if pl.nparams != 1 || src.index != 0 {
			return Value{}, false, nil
		}
		key, err = toValue(args[0], 0)
		return key, true, err
	case *litValue:
		if pl.nparams != 0 {
			return Value{}, false, nil
		}
		return src.v, true, nil
	}
	return Value{}, false, nil
}

func (db *DB) execWrite(pl *plan, vargs []Value) (int, error) {
	if err := pl.checkArgs(len(vargs)); err != nil {
		return 0, err
	}
	switch s := pl.st.(type) {
	case *createStmt:
		return 0, db.createTable(s.def)
	case *dropStmt:
		return 0, db.dropTable(s.name)
	case *insertStmt:
		return db.execInsert(pl, vargs)
	case *updateStmt:
		return db.execUpdate(pl, vargs)
	case *deleteStmt:
		return db.execDelete(pl, vargs)
	}
	return 0, fmt.Errorf("hazedb: Exec used with SELECT — use Query instead")
}

// Query runs a SELECT. Returns the column names (in projection order)
// and the rows. Rows are deep-cloned; callers may retain them past
// future Exec calls without worrying about aliasing into storage.
func (db *DB) Query(sql string, args ...any) ([]string, []Row, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, nil, err
	}
	return db.queryPlan(pl, args)
}

// queryPlan runs a SELECT plan against raw args. Shared by Query and *Stmt.Query.
// A PK lookup converts only the key arg (pkKeyFromArgs), skipping the []Value
// slice toValues allocates per call.
func (db *DB) queryPlan(pl *plan, args []any) ([]string, []Row, error) {
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("hazedb: Query used with non-SELECT — use Exec instead")
	}
	if pl.pkLookup {
		keyVal, ok, err := pl.pkKeyFromArgs(args)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return db.execSelectPK(pl, keyVal)
		}
	}
	vargs, err := toValues(args)
	if err != nil {
		return nil, nil, err
	}
	return db.queryPlanV(pl, vargs)
}

// queryPlanV runs a SELECT plan against pre-typed args, routing a PK lookup to
// the point reader. Shared by the []any entry points (after toValues) and the
// []Value entry points (QueryValues).
func (db *DB) queryPlanV(pl *plan, vargs []Value) ([]string, []Row, error) {
	if err := pl.checkArgs(len(vargs)); err != nil {
		return nil, nil, err
	}
	if pl.pkLookup {
		keyVal, err := evalLitOrParamValue(pl.pkSource, vargs)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPK(pl, keyVal)
	}
	return db.execSelect(pl, vargs)
}

// QueryRow runs a SELECT expected to yield a single row and returns the first
// matching row, or a nil Row if there is none — without allocating the []Row
// result slice that Query needs. For a PK-pinned query (WHERE id = ?) it goes
// straight through the point-read path (the common case); for an unpinned query
// it returns the first row of the scan, so constrain such queries with LIMIT 1
// to avoid scanning more rows than needed. The returned row is deep-cloned, as
// with Query.
func (db *DB) QueryRow(sql string, args ...any) ([]string, Row, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, nil, err
	}
	return db.queryRowPlan(pl, args)
}

// queryRowPlan runs a single-row SELECT plan against raw args. Shared by
// QueryRow and *Stmt.QueryRow. PK lookups take the same single-arg lane as
// queryPlan.
func (db *DB) queryRowPlan(pl *plan, args []any) ([]string, Row, error) {
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("hazedb: QueryRow used with non-SELECT — use Exec instead")
	}
	if pl.pkLookup {
		keyVal, ok, err := pl.pkKeyFromArgs(args)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return db.execSelectPKOne(pl, keyVal)
		}
	}
	vargs, err := toValues(args)
	if err != nil {
		return nil, nil, err
	}
	return db.queryRowPlanV(pl, vargs)
}

// queryRowPlanV is queryPlanV for single-row reads: a PK lookup goes through
// the alloc-free point reader (execSelectPKOne), an indexed point lookup
// through execSelectIdxOne, else it takes the first row of the scan. Shared by
// the []any entry points (after toValues) and QueryRowValues.
func (db *DB) queryRowPlanV(pl *plan, vargs []Value) ([]string, Row, error) {
	if err := pl.checkArgs(len(vargs)); err != nil {
		return nil, nil, err
	}
	if pl.pkLookup {
		keyVal, err := evalLitOrParamValue(pl.pkSource, vargs)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPKOne(pl, keyVal)
	}
	if pl.idxLookup && pl.orderOrdinal < 0 && pl.st.(*selectStmt).offset == 0 {
		return db.execSelectIdxOne(pl, vargs)
	}
	cols, rows, err := db.execSelect(pl, vargs)
	if err != nil || len(rows) == 0 {
		return cols, nil, err
	}
	return cols, rows[0], nil
}

// QueryValues is Query with pre-typed args — the read counterpart of ExecValues,
// for in-process callers (the PHP extension) that already hold typed Values and
// want to skip the []any/JSON conversion. Reads never store the args, so no
// per-arg clone is needed.
func (db *DB) QueryValues(sql string, args ...Value) ([]string, []Row, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, nil, err
	}
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("hazedb: Query used with non-SELECT — use Exec instead")
	}
	return db.queryPlanV(pl, args)
}

// QueryRowJSONByPK looks up a PK-pinned SELECT and appends the single matching
// row as a flat JSON object {"col":val,...} into dst, encoding the cells UNDER
// the shard read lock straight from the live row (no Row clone) with a typed id
// (no string→any boxing). dst is caller-owned and reused across calls, so a
// steady-state call allocates nothing. Returns the grown buffer and whether a
// row matched. The allocation-free read lane for an in-process JSON consumer
// (the Caddy GET handler); requires WHERE id = ? — a non-PK-pinned SELECT is
// rejected, like QueryRowByPK.
func (db *DB) QueryRowJSONByPK(dst []byte, sql string, id UUID) (out []byte, found bool, err error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return dst, false, err
	}
	if !pl.pkLookup {
		return dst, false, fmt.Errorf("hazedb: QueryRowJSONByPK requires a PK-pinned SELECT (WHERE id = ?)")
	}
	out, found = pl.rt.getByPKJSONInto(id, pl.colNames, pl.projOrdinals, dst)
	return out, found, nil
}

// QueryRowJSONByIndex is QueryRowJSONByPK for a single-indexed-equality point
// lookup: WHERE <indexed> = ? LIMIT 1 (no ORDER BY / OFFSET). It resolves the
// candidates through the secondary index, re-checks the full WHERE on each live
// candidate, and appends the first match as a flat JSON object into dst UNDER
// the shard read lock (no Row clone). key is the typed lookup value (no arg
// boxing); dst is caller-owned and reused, so a steady-state hit allocates only
// the index-bucket copy. Returns the grown buffer and whether a row matched.
func (db *DB) QueryRowJSONByIndex(dst []byte, sql string, key Value) (out []byte, found bool, err error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return dst, false, err
	}
	st, ok := pl.st.(*selectStmt)
	if !ok || !pl.idxLookup || len(pl.idxCols) != 1 || pl.orderOrdinal >= 0 || st.offset != 0 || st.limit != 1 {
		return dst, false, fmt.Errorf("hazedb: QueryRowJSONByIndex requires a single-indexed-equality point lookup (WHERE <indexed> = ? LIMIT 1), no ORDER BY or OFFSET")
	}
	tbl := pl.rt
	ctx := evalCtx{cols: tbl.def.colByName, args: []Value{key}}
	keyVal, err := evalExpr(pl.idxSrcs[0], &ctx)
	if err != nil {
		return dst, false, err
	}
	if keyVal.IsNull() {
		return dst, false, nil
	}
	si := tbl.indexFor(pl.idxCols[0])
	if si == nil {
		return dst, false, nil
	}
	var predErr error
	pred := func(r Row) bool {
		ctx.row = r
		v, e := evalExpr(st.where, &ctx)
		if e != nil {
			predErr = e
			return false
		}
		return truthy(v)
	}
	base := len(dst)
	for _, pk := range si.lookup(keyOf(keyVal)) {
		if out, found = tbl.appendMatchJSON(pk, pred, pl.colNames, pl.projOrdinals, st.starAll, dst[:base]); found {
			return out, true, nil
		}
		if predErr != nil {
			return dst[:base], false, predErr
		}
	}
	for _, pk := range tbl.dirtyPKs() {
		if out, found = tbl.appendMatchJSON(pk, pred, pl.colNames, pl.projOrdinals, st.starAll, dst[:base]); found {
			return out, true, nil
		}
		if predErr != nil {
			return dst[:base], false, predErr
		}
	}
	return dst[:base], false, nil
}

// QueryRowValues is QueryRow with pre-typed args (see QueryValues).
func (db *DB) QueryRowValues(sql string, args ...Value) ([]string, Row, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, nil, err
	}
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("hazedb: QueryRow used with non-SELECT — use Exec instead")
	}
	return db.queryRowPlanV(pl, args)
}

// prepare returns a plan bound against cat. A cached plan is reused only if it
// was bound against the same catalog version; otherwise it is re-parsed and
// re-bound so it never references a table that has since changed.
//
// sql may be a non-owned view (e.g. the PHP extension aliasing zend_string
// memory): the cache lookup only hashes/compares it and never retains it, so a
// hit copies nothing. On a miss we clone it before parsing, because the AST
// slices the SQL for identifiers and integer literals and the cache stores it
// as a permanent key — both must own their bytes. Net effect: callers pass a
// view and pay the copy only once per unique statement, not per call.
func (db *DB) prepare(sql string, cat *catalog) (*plan, error) {
	if cached, ok := db.stmtCache.Load(sql); ok {
		if pl := cached.(*plan); pl.catVersion == cat.version {
			return pl, nil
		}
	}
	sql = strings.Clone(sql)
	st, err := parseSQL(sql)
	if err != nil {
		return nil, err
	}
	nparams := assignParamIndices(st)
	pl, err := db.plan(st, cat)
	if err != nil {
		return nil, err
	}
	pl.nparams = nparams
	db.stmtCache.Store(sql, pl) // overwrite any stale-version entry
	return pl, nil
}

// onWALCorrupt records a corrupt segment found during recovery. Every record
// before the break was already applied; the unparseable suffix is skipped and
// recovery continues with the next segment rather than aborting Open.
func (db *DB) onWALCorrupt(seg uint64, err error) {
	db.logEvent("wal-corruption", fmt.Sprintf("segment %d during recovery: %v — good prefix recovered, suffix skipped", seg, err))
}

// replayWAL rebuilds state from the log. It is single-threaded (runs inside
// Open before the DB is returned), so it mutates the catalog directly via the
// atomic pointer. Catalog records (CREATE/DROP) come before any mutation that
// references the table, so a mutation always resolves against an
// already-rebuilt catalog.
func (db *DB) replayWAL(w *wal) error {
	return w.replayAll(db.applyReplayRecord, db.onWALCorrupt)
}

// applyReplayRecord applies one decoded WAL record to the in-memory store during
// recovery. Shared by full replay (replayWAL) and SQLite-backed tail replay
// (replayFrom): catalog records rebuild the catalog, mutations re-apply rows.
func (db *DB) applyReplayRecord(recType uint8, payload []byte) error {
	switch recType {
	case recCreateTable:
		tableID, td, err := decodeCreateTable(payload)
		if err != nil {
			return err
		}
		resolved, err := resolveSchema(Schema{Tables: []TableDef{td}})
		if err != nil {
			return err
		}
		rt := &tableRT{table: newTable(resolved[td.Name], db.sizeHint, db.budget), tableID: tableID}
		db.cat.Store(db.cat.Load().withTable(rt))
		return nil
	case recDropTable:
		name, err := decodeDropTable(payload)
		if err != nil {
			return err
		}
		db.cat.Store(db.cat.Load().withoutTable(name))
		return nil
	case recCheckpoint:
		return nil // no row state — skip
	case recMutation:
		return db.applyMutationRecord(payload)
	case recTxn:
		// A transaction is a count-prefixed group of sub-mutations, applied
		// in order. The whole group arrived as one CRC-valid envelope, so it
		// is all-or-nothing by construction; a torn group was discarded by
		// the tail check before reaching here.
		if len(payload) < 2 {
			return fmt.Errorf("%w: short txn payload", ErrWALCorrupt)
		}
		n := int(binary.LittleEndian.Uint16(payload[0:2]))
		off := 2
		for i := 0; i < n; i++ {
			if off+4 > len(payload) {
				return fmt.Errorf("%w: txn sub-mutation length truncated", ErrWALCorrupt)
			}
			mlen := int(binary.LittleEndian.Uint32(payload[off : off+4]))
			off += 4
			if mlen < 0 || off+mlen > len(payload) {
				return fmt.Errorf("%w: txn sub-mutation body truncated", ErrWALCorrupt)
			}
			if err := db.applyMutationRecord(payload[off : off+mlen]); err != nil {
				return err
			}
			off += mlen
		}
		return nil
	}
	return fmt.Errorf("%w: unknown record type %d", ErrWALCorrupt, recType)
}

// applyMutationRecord decodes one op|tableID|op-body mutation record and
// applies it through the table's apply path. Shared by recMutation (one per
// envelope) and recTxn (many per envelope).
func (db *DB) applyMutationRecord(payload []byte) error {
	if len(payload) < 3 {
		return fmt.Errorf("%w: short mutation payload", ErrWALCorrupt)
	}
	op := payload[0]
	tableID := binary.LittleEndian.Uint16(payload[1:3])
	cat := db.cat.Load()
	if int(tableID) >= len(cat.byID) || cat.byID[tableID] == nil {
		return fmt.Errorf("%w: mutation for unknown table id %d", ErrWALCorrupt, tableID)
	}
	return db.applyMutation(cat.byID[tableID], op, payload[3:])
}

// applyMutation re-applies one decoded mutation to a table during replay.
func (db *DB) applyMutation(rt *tableRT, op uint8, body []byte) error {
	ncols := len(rt.def.def.Columns)
	switch op {
	case opInsert:
		row, err := decodeRow(body)
		if err != nil {
			return err
		}
		// A tampered/corrupt record can decode to a row of the wrong width; insert
		// indexes row[pkOrdinal] (and row[partitionOrdinal]) unchecked, so a short
		// row would panic at boot — which recover() cannot catch for a slice
		// out-of-range during replay-in-Open, crash-looping the process. Fail
		// closed instead, mirroring the drain path's validation.
		if len(row) != ncols {
			return fmt.Errorf("%w: insert row width %d != table column count %d", ErrWALCorrupt, len(row), ncols)
		}
		return rt.insert(row)
	case opUpdate:
		var ords []int
		var vals []Value
		pk, err := decodeUpdateMutation(body, func(ord int, v Value) error {
			// Range-check the decoded ordinal before it is used as r[ord] in the
			// apply below — the same guard drain.go applies, absent here until now.
			if ord < 0 || ord >= ncols {
				return fmt.Errorf("%w: update ordinal %d out of range [0,%d)", ErrWALCorrupt, ord, ncols)
			}
			ords = append(ords, ord)
			vals = append(vals, v)
			return nil
		})
		if err != nil {
			return err
		}
		if !rt.update(pk.UUID(), func(r Row) Row {
			for i := range ords {
				r[ords[i]] = vals[i]
			}
			return r
		}) {
			return fmt.Errorf("%w: update for absent pk during replay", ErrWALCorrupt)
		}
		return nil
	case opDelete:
		pk, _, err := decodeCell(body)
		if err != nil {
			return err
		}
		rt.deleteByPK(pk.UUID())
		return nil
	}
	return fmt.Errorf("%w: unknown op %d", ErrWALCorrupt, op)
}

// toValues converts variadic args into Value cells. Supports int, int64,
// string, []byte, bool, nil, and Value pass-through.
func toValues(args []any) ([]Value, error) {
	return toValuesInto(args, nil)
}

func toValuesInto(args []any, scratch []Value) ([]Value, error) {
	var out []Value
	if len(args) <= len(scratch) {
		out = scratch[:len(args)]
	} else {
		out = make([]Value, len(args))
	}
	for i, a := range args {
		v, err := toValue(a, i)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func toValue(a any, index int) (Value, error) {
	switch x := a.(type) {
	case nil:
		return Null(), nil
	case int:
		return Int(int64(x)), nil
	case int64:
		return Int(x), nil
	case int32:
		return Int(int64(x)), nil
	case string:
		return Str(x), nil
	case []byte:
		// Clone at the write boundary: storage must not alias a caller slice
		// the caller can mutate after the call returns — that would corrupt the
		// stored row and diverge from the (already-written) WAL record.
		return Bytes(cloneBytes(x)), nil
	case bool:
		return Bool(x), nil
	case UUID:
		return UUIDVal(x), nil
	case Value:
		// A caller-built Value can also carry an aliased []byte; deep-copy it.
		return cloneValue(x), nil
	default:
		return Value{}, fmt.Errorf("%w: unsupported arg type %T at %d", ErrTypeMismatch, a, index)
	}
}

// journalTxnBodies writes a group of pre-encoded mutation bodies as ONE atomic
// TXN WAL envelope. The broad UPDATE/DELETE paths use it so a multi-row
// statement is all-or-nothing: the single record is written before any row is
// applied (a WAL failure leaves nothing applied), and a torn envelope is
// discarded whole on replay. Caller guarantees db.wal != nil.
func (db *DB) journalTxnBodies(bodies [][]byte) error {
	bp := db.scratch.get()
	*bp = encodeTxn(*bp, bodies)
	err := db.wal.writeRecord(recTxn, *bp)
	db.scratch.put(bp)
	return err
}

// scratchPool hands out small []byte buffers for WAL record encoding.
// sync.Pool gives per-P caching with no contention; the GC reclaims
// pooled items on its own schedule. The box (*[]byte) travels with the
// buffer through get/put — a Put with a fresh &b per call would heap-
// allocate one slice header per WAL record on the write hot path.
type scratchPool struct {
	p sync.Pool
}

func newScratchPool() *scratchPool {
	return &scratchPool{p: sync.Pool{New: func() any {
		b := make([]byte, 0, 256)
		return &b
	}}}
}

func (p *scratchPool) get() *[]byte {
	bp := p.p.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

func (p *scratchPool) put(bp *[]byte) {
	if cap(*bp) > 64<<10 {
		// drop oversize buffers so a one-off huge row doesn't pin memory
		return
	}
	p.p.Put(bp)
}
