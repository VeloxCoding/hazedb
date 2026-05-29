package hazedb

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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
}

// Options control Open behaviour.
type Options struct {
	// WALPath is the on-disk write-ahead log. Empty = memory-only
	// (no durability). The file is created if it doesn't exist.
	WALPath string

	// Schema declares the tables. Required; at least one table.
	Schema Schema

	// SizeHint is a per-table row-count estimate for shard arena
	// pre-allocation. Zero = use a small default.
	SizeHint int

	// WALFlushInterval is how often the background goroutine flushes the WAL
	// buffer to the OS (and fsyncs when WALSync is set). Zero selects the
	// safe default of 1s; a negative value disables the ticker entirely
	// (manual FlushWAL() only). Ignored when WALPath is empty.
	WALFlushInterval time.Duration

	// WALSync makes the ticker fsync after flushing when anything is dirty,
	// bounding power-loss to <= WALFlushInterval. Default false (flush only;
	// survives process crash, not power loss).
	WALSync bool

	// WALSyncPerWrite flushes and fsyncs after every individual WAL record,
	// under the WAL lock. Strongest durability (no acknowledged-loss window),
	// highest per-write cost. Overrides the ticker's sync cadence.
	WALSyncPerWrite bool
}

// Open prepares the database. If WALPath is non-empty, the file is
// opened and any existing records are replayed into memory before
// Open returns. Open is blocking until replay completes.
func Open(opts Options) (*DB, error) {
	sizeHint := opts.SizeHint
	if sizeHint <= 0 {
		sizeHint = 1024
	}
	// An empty schema is allowed — tables can be created at runtime.
	cat, err := newCatalog(opts.Schema, sizeHint)
	if err != nil {
		return nil, err
	}
	db := &DB{
		schema:   opts.Schema,
		sizeHint: sizeHint,
		scratch:  newScratchPool(),
	}
	db.cat.Store(cat)
	if opts.WALPath != "" {
		w, err := openWAL(opts.WALPath, opts.WALSync, opts.WALSyncPerWrite)
		if err != nil {
			return nil, err
		}
		// Replay first (reads from the start), then position for appends and
		// only then start the ticker — so the background goroutine never
		// races the replay reader on the shared file handle.
		if err := db.replayWAL(w); err != nil {
			w.close()
			return nil, err
		}
		if err := w.seekToEnd(); err != nil {
			w.close()
			return nil, err
		}
		flushInterval := opts.WALFlushInterval
		if flushInterval == 0 {
			flushInterval = time.Second // safe default
		}
		w.startTicker(flushInterval)
		db.wal = w
	}
	return db, nil
}

// Close flushes and closes the WAL. Memory-only DBs return nil.
func (db *DB) Close() error {
	if db.wal != nil {
		return db.wal.close()
	}
	return nil
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

// execPlan runs a non-SELECT plan against raw args. Shared by Exec (which looks
// the plan up by SQL each call) and *Stmt.Exec (which holds a compiled plan).
func (db *DB) execPlan(pl *plan, args []any) (int, error) {
	switch s := pl.st.(type) {
	case *createStmt:
		return 0, db.createTable(s.def)
	case *dropStmt:
		return 0, db.dropTable(s.name)
	}
	var argBuf [8]Value
	vargs, err := toValuesInto(args, argBuf[:])
	if err != nil {
		return 0, err
	}
	switch pl.st.(type) {
	case *insertStmt:
		return db.execInsert(pl, vargs)
	case *updateStmt:
		return db.execUpdate(pl, vargs)
	case *deleteStmt:
		return db.execDelete(pl, vargs)
	}
	return 0, fmt.Errorf("fastsql: Exec used with SELECT — use Query instead")
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
func (db *DB) queryPlan(pl *plan, args []any) ([]string, []Row, error) {
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("fastsql: Query used with non-SELECT — use Exec instead")
	}
	if pl.pkLookup {
		keyVal, err := evalLitOrParamAny(pl.pkSource, args)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPK(pl, keyVal)
	}
	vargs, err := toValues(args)
	if err != nil {
		return nil, nil, err
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
// QueryRow and *Stmt.QueryRow.
func (db *DB) queryRowPlan(pl *plan, args []any) ([]string, Row, error) {
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("fastsql: QueryRow used with non-SELECT — use Exec instead")
	}
	if pl.pkLookup {
		keyVal, err := evalLitOrParamAny(pl.pkSource, args)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPKOne(pl, keyVal)
	}
	vargs, err := toValues(args)
	if err != nil {
		return nil, nil, err
	}
	cols, rows, err := db.execSelect(pl, vargs)
	if err != nil || len(rows) == 0 {
		return cols, nil, err
	}
	return cols, rows[0], nil
}

// prepare returns a plan bound against cat. A cached plan is reused only if it
// was bound against the same catalog version; otherwise it is re-parsed and
// re-bound so it never references a table that has since changed.
func (db *DB) prepare(sql string, cat *catalog) (*plan, error) {
	if cached, ok := db.stmtCache.Load(sql); ok {
		if pl := cached.(*plan); pl.catVersion == cat.version {
			return pl, nil
		}
	}
	st, err := parseSQL(sql)
	if err != nil {
		return nil, err
	}
	assignParamIndices(st)
	pl, err := db.plan(st, cat)
	if err != nil {
		return nil, err
	}
	db.stmtCache.Store(sql, pl) // overwrite any stale-version entry
	return pl, nil
}

// replayWAL rebuilds state from the log. It is single-threaded (runs inside
// Open before the DB is returned), so it mutates the catalog directly via the
// atomic pointer. Catalog records (CREATE/DROP) come before any mutation that
// references the table, so a mutation always resolves against an
// already-rebuilt catalog.
func (db *DB) replayWAL(w *wal) error {
	return w.replay(func(recType uint8, payload []byte) error {
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
			rt := &tableRT{table: newTable(resolved[td.Name], db.sizeHint), tableID: tableID}
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
	})
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
	switch op {
	case opInsert:
		row, err := decodeRow(body)
		if err != nil {
			return err
		}
		return rt.insert(row)
	case opUpdate:
		// op-body: pk-cell | nsets:1 | (ordinal:2 | cell) × nsets.
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
		ords := make([]int, nsets)
		vals := make([]Value, nsets)
		for i := 0; i < nsets; i++ {
			if len(body) < 2 {
				return fmt.Errorf("%w: update ordinal truncated", ErrWALCorrupt)
			}
			ords[i] = int(binary.LittleEndian.Uint16(body[0:2]))
			body = body[2:]
			v, m, err := decodeCell(body)
			if err != nil {
				return err
			}
			vals[i] = v
			body = body[m:]
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
	buf := db.scratch.get()
	buf = encodeTxn(buf, bodies)
	err := db.wal.writeRecord(recTxn, buf)
	db.scratch.put(buf)
	return err
}

// scratchPool hands out small []byte buffers for WAL record encoding.
// sync.Pool gives per-P caching with no contention; the GC reclaims
// pooled items on its own schedule.
type scratchPool struct {
	p sync.Pool
}

func newScratchPool() *scratchPool {
	return &scratchPool{p: sync.Pool{New: func() any {
		b := make([]byte, 0, 256)
		return &b
	}}}
}

func (p *scratchPool) get() []byte {
	bp := p.p.Get().(*[]byte)
	return (*bp)[:0]
}

func (p *scratchPool) put(b []byte) {
	if cap(b) > 64<<10 {
		// drop oversize buffers so a one-off huge row doesn't pin memory
		return
	}
	p.p.Put(&b)
}
