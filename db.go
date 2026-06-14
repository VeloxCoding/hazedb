package hazedb

import (
	"sync"
	"sync/atomic"
)

// tableRT is the runtime form of a resolvedTable with its storage attached.
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
// here, plus Transaction (txn.go). The *Values (pre-typed args) and *JSON*
// (encode-in-place) methods are in-process fast-path variants of the same verbs,
// upholding the same guarantees. The verbs live in db_exec.go (writes) and
// db_query.go (reads); WAL/recovery replay is in db_replay.go.
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
// Caller obligation — parameterize. Pass values as ? placeholders, never spliced
// into the SQL text. prepare caches one compiled plan per unique SQL string and
// keeps it for the process lifetime; a parameterized statement set is finite so
// the cache stays bounded, but interpolating values mints a new key per call and
// the cache grows without bound in a long-lived process.
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

	// closed is set once by Close (CompareAndSwap, so Close is idempotent). Every
	// public verb loads it and rejects with ErrClosed once set, so a use-after-close
	// cannot mutate or read through a torn-down WAL/companion. Checked at the two
	// gateway chokepoints — prepare (all SQL verbs) and Stmt.bound (prepared verbs)
	// — plus Transaction and FlushWAL, which bypass both.
	closed atomic.Bool

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

	// sq is the SQLite companion — always present, a real file on disk next to the
	// WAL in every mode (never in-memory). It holds the _hz_events operational log
	// always, and becomes the data mirror + recovery base when WAL is on: the drain
	// then feeds sealed WAL segments into it. mirrorOn marks that WAL-on state;
	// drainStop/drainDone drive the drain goroutine, mirroring the merger. See
	// docs/durability.md.
	sq        *sqliteMirror
	mirrorOn  bool
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
	// The SQLite companion is always present — a real file on disk (CompanionPath;
	// default hazedb.db inside WALPath, or in the working directory with no WAL). It
	// holds the _hz_events operational log in every mode, and becomes the data
	// mirror + recovery base when WAL is on. Never in-memory.
	comp, err := openCompanion(opts.CompanionPath)
	if err != nil {
		return nil, err
	}
	db.sq = comp

	// All-or-nothing cleanup: anything opened or started below — the WAL, its
	// flusher, the background loops, the companion — is torn down unless Open
	// reaches the success line (ok = true). This fires on an error return AND on a
	// panic in one of the void calls after the flusher is live (rebuildAllIndexes,
	// start*Loop), so a half-built DB never leaks the flusher goroutine or the open
	// files. The stop*Loop helpers are no-ops when their loop never started, and
	// w/comp are closed only if opened. Order mirrors Close: stop loops (the drain
	// loop seals the WAL) before closing the WAL.
	var w *wal
	ok := false
	defer func() {
		if ok {
			return
		}
		db.stopDrainLoop()
		db.stopMergeLoop()
		db.stopCompactLoop()
		if w != nil {
			w.close()
		}
		comp.close()
	}()

	if opts.walEnabled() {
		w, err = openWAL(opts.WALPath)
		if err != nil {
			return nil, err
		}
		db.mirrorOn = true
		// Recover before starting the flusher, so no background flush races a replay
		// reader. The companion is the compacted base: load it into memory, then
		// replay only the undrained WAL tail (segments past the drained cursor) on top.
		if err := comp.activateMirror(db.cat.Load()); err != nil {
			return nil, err
		}
		if err := db.recoverFromSQLite(); err != nil {
			return nil, err
		}
		if err := w.removeDrainedSegments(comp.lastDrained); err != nil {
			return nil, err
		}
		if err := w.replayFrom(comp.lastDrained, db.applyReplayRecord, db.onWALCorrupt); err != nil {
			return nil, err
		}
		// Keep segment numbers above the drained cursor across restarts: the drain
		// deletes the segments it consumes, so the highest on-disk segment can fall
		// below lastDrained. Without this, a post-restart flush could reuse a number
		// <= lastDrained that drainOnce would skip forever — those writes would never
		// reach the mirror and be lost on next recovery.
		if w.seg < comp.lastDrained {
			w.seg = comp.lastDrained
		}
		db.wal = w
		w.startFlusher(opts.walFlushInterval)
		// Replay marked rows dirty but never built the indexes; rebuild them from
		// the live rows now, so reads are index-fast before serving.
		db.rebuildAllIndexes()

		if opts.drainInterval > 0 {
			db.startDrainLoop(opts.drainInterval)
		}
	}
	if opts.indexMergeInterval > 0 {
		db.startMergeLoop(opts.indexMergeInterval, opts.indexMergeThreshold)
	}
	if opts.compactInterval > 0 {
		db.startCompactLoop(opts.compactInterval)
	}
	ok = true
	return db, nil
}

// Close stops the SQLite drain loop (which seals the active segment and runs a
// final drain so the mirror is current), then the index merger and the
// compaction sweeper, then flushes and closes the WAL, then closes the mirror.
// Memory-only DBs still stop the background loops. The drain loop is stopped
// first, while the WAL is still open. Close marks the DB closed before tearing
// anything down, so every public verb then fails with ErrClosed; a second Close
// is a no-op.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil // already closed — idempotent
	}
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
	if db.closed.Load() {
		return ErrClosed
	}
	if db.wal == nil {
		return nil
	}
	return db.wal.flush()
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
