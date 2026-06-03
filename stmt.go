package hazedb

import (
	"fmt"
	"sync/atomic"
)

// Stmt is a SQL string compiled to a plan once and reused. Versus the bare
// DB.Query/Exec path it skips the per-call statement-cache lookup (no SQL-string
// hash), and it adds a typed, zero-allocation point-read fast path,
// QueryRowByPK. Safe for concurrent use: the held plan is rebound automatically
// if a CREATE/DROP changes the catalog after Prepare.
type Stmt struct {
	db  *DB
	sql string
	pl  atomic.Pointer[plan]
}

// Prepare compiles sql into a reusable statement bound to the current catalog.
func (db *DB) Prepare(sql string) (*Stmt, error) {
	pl, err := db.prepare(sql, db.cat.Load())
	if err != nil {
		return nil, err
	}
	s := &Stmt{db: db, sql: sql}
	s.pl.Store(pl)
	return s, nil
}

// bound returns the held plan, rebinding it if a CREATE/DROP has changed the
// catalog since it was compiled. Hot path: one atomic load + version compare.
func (s *Stmt) bound() (*plan, error) {
	cat := s.db.cat.Load()
	if pl := s.pl.Load(); pl.catVersion == cat.version {
		return pl, nil
	}
	pl, err := s.db.prepare(s.sql, cat)
	if err != nil {
		return nil, err
	}
	s.pl.Store(pl)
	return pl, nil
}

// Columns returns the SELECT output column names (nil for a non-SELECT). The
// slice is shared and read-only — callers must not mutate it.
func (s *Stmt) Columns() []string {
	pl, err := s.bound()
	if err != nil {
		return nil
	}
	return pl.colNames
}

// Exec runs a prepared INSERT/UPDATE/DELETE/DDL. See DB.Exec.
func (s *Stmt) Exec(args ...any) (int, error) {
	pl, err := s.bound()
	if err != nil {
		return 0, err
	}
	return s.db.execPlan(pl, args)
}

// Query runs a prepared SELECT. See DB.Query.
func (s *Stmt) Query(args ...any) ([]string, []Row, error) {
	pl, err := s.bound()
	if err != nil {
		return nil, nil, err
	}
	return s.db.queryPlan(pl, args)
}

// QueryRow runs a prepared single-row SELECT. See DB.QueryRow.
func (s *Stmt) QueryRow(args ...any) ([]string, Row, error) {
	pl, err := s.bound()
	if err != nil {
		return nil, nil, err
	}
	return s.db.queryRowPlan(pl, args)
}

// QueryRowByPK is the zero-allocation point-read fast path. The statement must
// be a PK-pinned SELECT (WHERE <pk> = ?); the key is taken as a typed UUID (no
// interface boxing) and the projected cells are written into dst, which is
// grown only if too small — reuse it across calls. found reports whether a row
// matched. Non-byte cells are copied directly; BYTES cells are cloned to honour
// storage's no-alias guarantee, so a projection without BYTES columns allocates
// nothing.
func (s *Stmt) QueryRowByPK(pk UUID, dst []Value) (out []Value, found bool, err error) {
	pl, err := s.bound()
	if err != nil {
		return dst[:0], false, err
	}
	// pkLookup is set only on a SELECT plan, and projOrdinals is already nil for
	// SELECT * (the planner only fills it for an explicit projection) — so neither
	// a type assertion nor a starAll branch is needed on this hot path.
	if !pl.pkLookup {
		return dst[:0], false, fmt.Errorf("hazedb: QueryRowByPK requires a PK-pinned SELECT (WHERE id = ?)")
	}
	out, found = pl.rt.getByPKProjectInto(pk, pl.projOrdinals, dst)
	return out, found, nil
}

// QueryRowByIndex is the point-lookup fast path for a single secondary-index
// equality: the statement must pin exactly one indexed column with an explicit
// single-row cap — WHERE <indexed> = ? LIMIT 1 — and no ORDER BY or OFFSET. It
// returns the first live row that passes the full WHERE and stops. The match is
// ordering-undefined by design: this serves unique-key lookups (fetch the
// account row for an email at login) and existence checks (does this email
// already exist?), where "any one match" is the answer.
//
// It cannot return the newest/highest row — there is no ordering on this path.
// For that, write ORDER BY <ordered-indexed col> DESC LIMIT 1, which compiles to
// the ordered-index walk and is rejected here (orderOrdinal >= 0). The mandatory
// LIMIT 1 keeps that distinction explicit at the call site.
//
// key is the typed value for the one parameter (no interface boxing); the
// matching row's projection is written into dst, grown only if too small — reuse
// it across calls. found reports whether a row matched. In steady state it
// allocates only the index-bucket copy (~one small slice); the projection scan
// and the empty dirty overlay add none. The full WHERE is re-checked on each
// live candidate (staleness + any residual), so a statement needing a second
// parameter returns an error rather than a wrong row.
func (s *Stmt) QueryRowByIndex(key Value, dst []Value) (out []Value, found bool, err error) {
	pl, err := s.bound()
	if err != nil {
		return dst[:0], false, err
	}
	st, ok := pl.st.(*selectStmt)
	if !ok || !pl.idxLookup || len(pl.idxCols) != 1 || pl.orderOrdinal >= 0 || st.offset != 0 || st.limit != 1 {
		return dst[:0], false, fmt.Errorf("hazedb: QueryRowByIndex requires a single-indexed-equality point lookup (WHERE <indexed> = ? LIMIT 1), no ORDER BY or OFFSET")
	}
	tbl := pl.rt
	ctx := evalCtx{cols: tbl.def.colByName, args: []Value{key}}
	keyVal, err := evalExpr(pl.idxSrcs[0], &ctx)
	if err != nil {
		return dst[:0], false, err
	}
	if keyVal.IsNull() {
		return dst[:0], false, nil
	}
	si := tbl.indexFor(pl.idxCols[0])
	if si == nil {
		return dst[:0], false, nil
	}
	ords := pl.projOrdinals // already nil for SELECT * — no starAll branch needed
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
	// Index bucket first (option b: a safe copy under the index RLock, released
	// before the shard lock — no lock nesting), then the dirty overlay for a
	// not-yet-merged match (nil in steady state). First live row that passes the
	// full WHERE wins.
	for _, pk := range si.lookup(keyOf(keyVal)) {
		if out, found = tbl.getMatchProjectInto(pk, pred, ords, st.starAll, dst); found {
			return out, true, nil
		}
		if predErr != nil {
			return dst[:0], false, predErr
		}
	}
	for _, pk := range tbl.dirtyPKs() {
		if out, found = tbl.getMatchProjectInto(pk, pred, ords, st.starAll, dst); found {
			return out, true, nil
		}
		if predErr != nil {
			return dst[:0], false, predErr
		}
	}
	return dst[:0], false, nil
}
