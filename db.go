package hazedb

import (
	"fmt"
	"sync"
	"time"
)

// table is the runtime form of a resolvedTable with its storage attached.
type tableRT struct {
	*table
	tableID uint16
}

// DB is the embedded database handle. One DB per process per WAL
// path. Open is goroutine-safe; Exec and Query are goroutine-safe.
type DB struct {
	schema    Schema
	tables    map[string]*resolvedTable
	t         map[string]*tableRT
	tableByID []*tableRT // tableID → tableRT (for replay)

	wal     *wal
	scratch *scratchPool

	// stmtCache memoises (SQL → *plan). Hit path is lockless read;
	// miss path takes the write lock once per unique SQL string. A
	// plan is read-only after prepare so sharing across goroutines
	// is safe.
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
	if len(opts.Schema.Tables) == 0 {
		return nil, fmt.Errorf("fastsql: schema has no tables")
	}
	resolved, err := resolveSchema(opts.Schema)
	if err != nil {
		return nil, err
	}
	db := &DB{
		schema:    opts.Schema,
		tables:    resolved,
		t:         make(map[string]*tableRT, len(resolved)),
		tableByID: make([]*tableRT, 0, len(resolved)),
		scratch:   newScratchPool(),
	}
	sizeHint := opts.SizeHint
	if sizeHint <= 0 {
		sizeHint = 1024
	}
	for i, td := range opts.Schema.Tables {
		rt := &tableRT{
			table:   newTable(resolved[td.Name], sizeHint),
			tableID: uint16(i),
		}
		db.t[td.Name] = rt
		db.tableByID = append(db.tableByID, rt)
	}
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

// Exec runs an INSERT, UPDATE, or DELETE. Returns the affected row count.
func (db *DB) Exec(sql string, args ...any) (int, error) {
	pl, err := db.prepare(sql)
	if err != nil {
		return 0, err
	}
	vargs, err := toValues(args)
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
	pl, err := db.prepare(sql)
	if err != nil {
		return nil, nil, err
	}
	vargs, err := toValues(args)
	if err != nil {
		return nil, nil, err
	}
	if _, ok := pl.st.(*selectStmt); !ok {
		return nil, nil, fmt.Errorf("fastsql: Query used with non-SELECT — use Exec instead")
	}
	return db.execSelect(pl, vargs)
}

func (db *DB) prepare(sql string) (*plan, error) {
	if cached, ok := db.stmtCache.Load(sql); ok {
		return cached.(*plan), nil
	}
	st, err := parseSQL(sql)
	if err != nil {
		return nil, err
	}
	assignParamIndices(st)
	pl, err := db.plan(st)
	if err != nil {
		return nil, err
	}
	// LoadOrStore so a concurrent parse of the same SQL doesn't double-bind.
	actual, _ := db.stmtCache.LoadOrStore(sql, pl)
	return actual.(*plan), nil
}

func (db *DB) replayWAL(w *wal) error {
	return w.replay(func(rec replayRecord) error {
		if int(rec.TableID) >= len(db.tableByID) {
			return fmt.Errorf("%w: unknown table id %d", ErrWALCorrupt, rec.TableID)
		}
		rt := db.tableByID[rec.TableID]
		switch rec.Op {
		case opInsert:
			row, err := decodeRow(rec.Body)
			if err != nil {
				return err
			}
			return rt.insert(row)
		case opUpdate:
			row, err := decodeRow(rec.Body)
			if err != nil {
				return err
			}
			pk := row[rt.def.pkOrdinal].AsString()
			if !rt.update(pk, func(_ Row) Row { return row }) {
				// Replay may see an UPDATE before the INSERT if the WAL was
				// rewound mid-write; treat as fresh insert.
				return rt.insert(row)
			}
			return nil
		case opDelete:
			pk, err := decodePK(rec.Body)
			if err != nil {
				return err
			}
			rt.deleteByPK(pk.AsString())
			return nil
		}
		return fmt.Errorf("%w: unknown op %d", ErrWALCorrupt, rec.Op)
	})
}

// toValues converts variadic args into Value cells. Supports int, int64,
// string, []byte, bool, nil, and Value pass-through.
func toValues(args []any) ([]Value, error) {
	out := make([]Value, len(args))
	for i, a := range args {
		switch x := a.(type) {
		case nil:
			out[i] = Null()
		case int:
			out[i] = Int(int64(x))
		case int64:
			out[i] = Int(x)
		case int32:
			out[i] = Int(int64(x))
		case string:
			out[i] = Str(x)
		case []byte:
			out[i] = Bytes(x)
		case bool:
			out[i] = Bool(x)
		case Value:
			out[i] = x
		default:
			return nil, fmt.Errorf("%w: unsupported arg type %T at %d", ErrTypeMismatch, a, i)
		}
	}
	return out, nil
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
