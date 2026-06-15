package hazedb

import (
	"fmt"
	"strings"
)

// Read verbs, plan preparation, and the PK-arg fast path. Every read enters
// through Query/QueryRow (and their *Values / *JSON* variants), binds a plan via
// prepare, then routes PK / indexed / scan lookups to the executors. See db.go
// for the gateway contract.

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
	if pl.pkLookup && !pl.countStar {
		// Direct-UUID fast path: when the single PK arg is already a UUID, a single
		// type assertion replaces the Value round-trip (toValue's 8-case switch +
		// coerceToUUID's reconstruction) — the common Caddy/PHP point read.
		if pl.nparams == 1 && len(args) == 1 {
			if u, isUUID := args[0].(UUID); isUUID {
				if _, isParam := pl.pkSource.(*paramRef); isParam {
					return db.execSelectPKResolved(pl, u)
				}
			}
		}
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
	if err := coerceParams(pl, vargs); err != nil {
		return nil, nil, err
	}
	if pl.countStar {
		n, err := db.countRows(pl, vargs)
		if err != nil {
			return nil, nil, err
		}
		return pl.colNames, []Row{{Int(n)}}, nil
	}
	if pl.pkLookup {
		keyVal, err := evalLitOrParamValue(pl.pkSource, vargs)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPK(pl, keyVal)
	}
	if pl.idxLookup {
		// Route directly, skipping execSelect's eval-context construction: the
		// point-read fast path needs no context, and the general path builds its own.
		return db.execSelectIdx(pl, vargs)
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
	if pl.pkLookup && !pl.countStar {
		// Direct-UUID fast path (mirrors queryPlan): a single UUID arg bound to the
		// PK param skips toValue's type switch + the Value round-trip + coerceToUUID —
		// the common Caddy/PHP single-row point read.
		if pl.nparams == 1 && len(args) == 1 {
			if u, isUUID := args[0].(UUID); isUUID {
				if _, isParam := pl.pkSource.(*paramRef); isParam {
					return db.execSelectPKOneResolved(pl, u)
				}
			}
		}
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
	if err := coerceParams(pl, vargs); err != nil {
		return nil, nil, err
	}
	if pl.countStar {
		n, err := db.countRows(pl, vargs)
		if err != nil {
			return nil, nil, err
		}
		return pl.colNames, Row{Int(n)}, nil
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
	kargs := []Value{key}
	if err := coerceParams(pl, kargs); err != nil {
		return dst, false, err
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: kargs}
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
// view and pay the copy only once per unique statement, not per call. The cache
// is never evicted: one plan per unique SQL string, kept for the process
// lifetime.
func (db *DB) prepare(sql string, cat *catalog) (*plan, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
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
	if err := rejectValueLiterals(st); err != nil {
		return nil, err
	}
	nparams := assignParamIndices(st)
	pl, err := db.plan(st, cat)
	if err != nil {
		return nil, err
	}
	pl.nparams = nparams
	pl.bindParamUUIDCoercion()
	db.stmtCache.Store(sql, pl) // overwrite any stale-version entry
	return pl, nil
}

// prepareTrusted compiles one statement from a trusted boot/seed script (see
// ExecScript). It skips the inline-literal ban — a seed file legitimately writes
// constant values and has no ? args to bind — and does NOT touch stmtCache, so an
// unchecked literal plan can never be served to a later Exec through a cache hit
// (and the one-shot boot work pays nothing to cache). The caller checks db.closed.
func (db *DB) prepareTrusted(sql string, cat *catalog) (*plan, error) {
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
	pl.bindParamUUIDCoercion()
	return pl, nil
}
