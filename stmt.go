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
	st, ok := pl.st.(*selectStmt)
	if !ok || !pl.pkLookup {
		return dst[:0], false, fmt.Errorf("hazedb: QueryRowByPK requires a PK-pinned SELECT (WHERE id = ?)")
	}
	ords := pl.projOrdinals
	if st.starAll {
		ords = nil
	}
	out, found = pl.rt.getByPKProjectInto(pk, ords, dst)
	return out, found, nil
}
