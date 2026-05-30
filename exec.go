package hazedb

import (
	"fmt"
	"sort"
)

// assignParamIndices walks the AST and replaces every paramRef.index
// = -1 with a running count, so positional args bind in source order.
// Called once after parse, before plan.
func assignParamIndices(st stmt) {
	var n int
	var walk func(expr) expr
	walk = func(e expr) expr {
		switch x := e.(type) {
		case nil:
			return nil
		case *paramRef:
			x.index = n
			n++
			return x
		case *binOp:
			x.lhs = walk(x.lhs)
			x.rhs = walk(x.rhs)
			return x
		case *notExpr:
			x.e = walk(x.e)
			return x
		case *isNullExpr:
			x.e = walk(x.e)
			return x
		}
		return e
	}
	switch s := st.(type) {
	case *selectStmt:
		s.where = walk(s.where)
	case *insertStmt:
		for i := range s.vals {
			s.vals[i] = walk(s.vals[i])
		}
	case *updateStmt:
		for i := range s.sets {
			s.sets[i].val = walk(s.sets[i].val)
		}
		s.where = walk(s.where)
	case *deleteStmt:
		s.where = walk(s.where)
	}
}

// plan resolves table/column names and validates them. Produces a
// minimal plan with the resolved table pointer + column ordinals
// baked in, so the runtime path does no map lookups.
type plan struct {
	st    stmt
	table *resolvedTable
	// rt is the table's runtime storage, resolved from the catalog snapshot
	// this plan was bound against. catVersion is that snapshot's version;
	// prepare re-binds the plan if the catalog has changed since.
	rt         *tableRT
	catVersion uint64
	// SELECT projection: ordinals into the row, in output order. nil if
	// SELECT *.
	projOrdinals []int
	// colNames is the SELECT's output column names, computed once at plan time
	// and returned by every Query on this (cached) plan. Read-only: it is
	// shared across concurrent callers, so callers must not mutate it.
	colNames []string
	// SELECT ORDER BY: ordinal of the order column. -1 if none.
	orderOrdinal int
	// INSERT column ordinals matching the values list.
	insertOrdinals []int
	// UPDATE SET column ordinals matching the sets list.
	updateOrdinals []int
	// setRowDependent is true when any SET right-hand side references a
	// column (e.g. col = col - ?), so the value must be evaluated per row
	// instead of once up front.
	setRowDependent bool

	// pkLookup is true when the WHERE clause is a single equality of
	// the PK column against a literal or parameter. The executor can
	// then go straight through tableShard.getByPK and skip the full
	// scan. pkSource is the expression that yields the key.
	pkLookup bool
	pkSource expr

	// partLookup is true when a SELECT on a partitioned table pins the
	// PartitionKey column to a value (WHERE thread = ?). The executor then
	// scans only that partition's rows instead of the whole table. partSource
	// yields the partition value.
	partLookup bool
	partSource expr

	// idxLookup is true when a SELECT (no ORDER BY) pins one or more
	// secondary-indexed columns by equality (WHERE name = ? AND city = ?). The
	// executor resolves candidate PKs through the index(es) instead of scanning.
	// idxCols / idxSrcs are parallel: the indexed column ordinals and the exprs
	// yielding their values. Two or more => intersect their buckets before the
	// residual full-WHERE filter.
	idxLookup bool
	idxCols   []int
	idxSrcs   []expr

	// orderWalk is true when ORDER BY is on an ordered-indexed column and no
	// equality index was chosen: the executor walks the sorted index (merged
	// with the dirty overlay) in order and stops at LIMIT, instead of scanning +
	// sorting. orderOrdinal names the column.
	orderWalk bool
}

func (db *DB) plan(st stmt, cat *catalog) (*plan, error) {
	pl := &plan{st: st, orderOrdinal: -1, catVersion: cat.version}
	// DDL needs no table resolution: CREATE defines a new table, DROP names
	// one; both are dispatched straight from Exec.
	switch st.(type) {
	case *createStmt, *dropStmt:
		return pl, nil
	}
	tname := ""
	switch s := st.(type) {
	case *selectStmt:
		tname = s.table
	case *insertStmt:
		tname = s.table
	case *updateStmt:
		tname = s.table
	case *deleteStmt:
		tname = s.table
	}
	trt, ok := cat.byName[tname]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTable, tname)
	}
	pl.rt = trt
	rt := trt.table.def
	pl.table = rt
	switch s := st.(type) {
	case *selectStmt:
		if !s.starAll {
			pl.projOrdinals = make([]int, 0, len(s.cols))
			for _, c := range s.cols {
				ord, ok := rt.colByName[c.col]
				if !ok {
					return nil, fmt.Errorf("%w: %q.%q", ErrUnknownColumn, tname, c.col)
				}
				pl.projOrdinals = append(pl.projOrdinals, ord)
			}
		}
		// Output column names, computed once and reused by every Query on this
		// cached plan (read-only).
		if s.starAll {
			pl.colNames = make([]string, len(rt.def.Columns))
			for i, c := range rt.def.Columns {
				pl.colNames[i] = c.Name
			}
		} else {
			pl.colNames = make([]string, len(s.cols))
			for i, c := range s.cols {
				pl.colNames[i] = c.col
			}
		}
		if s.orderCol != "" {
			ord, ok := rt.colByName[s.orderCol]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (ORDER BY)", ErrUnknownColumn, tname, s.orderCol)
			}
			pl.orderOrdinal = ord
		}
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			} else if rt.partitioned() {
				if ok, src := detectColEq(s.where, rt, rt.partitionOrdinal); ok {
					pl.partLookup = true
					pl.partSource = src
				}
			} else if len(rt.indexes) > 0 {
				// Secondary-index lookup. Collect every indexed column constrained
				// by equality in the WHERE's AND-chain; the executor intersects
				// their buckets and evaluates the FULL WHERE on each candidate
				// (residual-filtering extra conjuncts). An ORDER BY is honoured by
				// sorting the candidate set — the filtered-list pattern (WHERE
				// author = ? ORDER BY date). A query with no indexed equality
				// conjunct (a bare ORDER BY, or only a range) falls back to scan.
				eqs := map[int]expr{}
				collectEqConjuncts(s.where, eqs)
				for i := range rt.indexes {
					ri := &rt.indexes[i]
					if src, has := eqs[ri.ordinal]; has {
						pl.idxCols = append(pl.idxCols, ri.ordinal)
						pl.idxSrcs = append(pl.idxSrcs, src)
					}
				}
				pl.idxLookup = len(pl.idxCols) > 0
			}
		}
		// ORDER BY on an ordered-indexed column, with no equality index chosen
		// (applies with or without a WHERE): walk the sorted index in order
		// instead of scanning + sorting.
		if s.orderCol != "" && !pl.idxLookup && rt.orderedIndexOn(pl.orderOrdinal) {
			pl.orderWalk = true
		}
	case *insertStmt:
		pl.insertOrdinals = make([]int, 0, len(s.cols))
		for _, c := range s.cols {
			ord, ok := rt.colByName[c]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (INSERT)", ErrUnknownColumn, tname, c)
			}
			pl.insertOrdinals = append(pl.insertOrdinals, ord)
		}
	case *updateStmt:
		pl.updateOrdinals = make([]int, 0, len(s.sets))
		for _, a := range s.sets {
			ord, ok := rt.colByName[a.col]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (UPDATE SET)", ErrUnknownColumn, tname, a.col)
			}
			if ord == rt.pkOrdinal {
				return nil, fmt.Errorf("%w: %q.%q", ErrPKUpdate, tname, a.col)
			}
			if ord == rt.partitionOrdinal {
				return nil, fmt.Errorf("%w: %q.%q is the PartitionKey (immutable; move via DELETE + INSERT)", ErrPKUpdate, tname, a.col)
			}
			if rt.def.Columns[ord].Immutable {
				return nil, fmt.Errorf("%w: %q.%q is immutable", ErrPKUpdate, tname, a.col)
			}
			pl.updateOrdinals = append(pl.updateOrdinals, ord)
			// Validate the right-hand side (catches unknown columns in
			// arithmetic like SET x = bogus - ?) and note row dependence.
			if err := validateExpr(a.val, rt); err != nil {
				return nil, err
			}
			if exprRefsColumn(a.val) {
				pl.setRowDependent = true
			}
		}
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			}
		}
	case *deleteStmt:
		if err := validateExpr(s.where, rt); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			}
		}
	}
	return pl, nil
}

// detectColEq returns (true, valueSide) when e is a single binOp = between the
// named column (by ordinal) and a literal/parameter. Walks across AND chains
// in a future pass — v1 only accepts the bare equality.
func detectColEq(e expr, rt *resolvedTable, ordinal int) (bool, expr) {
	bop, ok := e.(*binOp)
	if !ok || bop.op != tkEq {
		return false, nil
	}
	name := rt.def.Columns[ordinal].Name
	if cr, ok := bop.lhs.(*colRef); ok && cr.name == name {
		if isLitOrParam(bop.rhs) {
			return true, bop.rhs
		}
	}
	if cr, ok := bop.rhs.(*colRef); ok && cr.name == name {
		if isLitOrParam(bop.lhs) {
			return true, bop.lhs
		}
	}
	return false, nil
}

// detectPKEq is detectColEq pinned to the PK column.
func detectPKEq(e expr, rt *resolvedTable) (bool, expr) {
	return detectColEq(e, rt, rt.pkOrdinal)
}

// orderedIndexOn reports whether column ord has an ORDERED secondary index.
func (rt *resolvedTable) orderedIndexOn(ord int) bool {
	for i := range rt.indexes {
		if rt.indexes[i].ordinal == ord && rt.indexes[i].ordered {
			return true
		}
	}
	return false
}

// collectEqConjuncts walks an AND-chain of the WHERE and records every
// `col = lit/param` equality as ordinal -> value expr. Only AND nodes are
// descended (an OR cannot be answered from a single index). colRef ords are
// already bound by validateExpr. Used to pick a secondary index for a query
// whose WHERE may carry extra conjuncts beyond the indexed equality.
func collectEqConjuncts(e expr, out map[int]expr) {
	bop, ok := e.(*binOp)
	if !ok {
		return
	}
	switch bop.op {
	case tkAnd:
		collectEqConjuncts(bop.lhs, out)
		collectEqConjuncts(bop.rhs, out)
	case tkEq:
		if cr, ok := bop.lhs.(*colRef); ok && cr.ord >= 0 && isLitOrParam(bop.rhs) {
			out[cr.ord] = bop.rhs
		} else if cr, ok := bop.rhs.(*colRef); ok && cr.ord >= 0 && isLitOrParam(bop.lhs) {
			out[cr.ord] = bop.lhs
		}
	}
}

func isLitOrParam(e expr) bool {
	switch e.(type) {
	case *litValue, *paramRef:
		return true
	}
	return false
}

// coerceToUUID turns a PK-lookup value into a UUID: a UUID passes through; a
// string is parsed (API-boundary convenience). Anything else is an error.
func coerceToUUID(v Value) (UUID, error) {
	switch v.Kind {
	case KindUUID:
		return v.UUID(), nil
	case KindString:
		return ParseUUID(v.Str())
	}
	return UUID{}, fmt.Errorf("%w: PK lookup expects UUID, got kind %d", ErrTypeMismatch, v.Kind)
}

// exprRefsColumn reports whether e reads any column (vs only literals and
// parameters). Used to decide if a SET value must be evaluated per row.
func exprRefsColumn(e expr) bool {
	switch x := e.(type) {
	case *colRef:
		return true
	case *binOp:
		return exprRefsColumn(x.lhs) || exprRefsColumn(x.rhs)
	case *notExpr:
		return exprRefsColumn(x.e)
	case *isNullExpr:
		return exprRefsColumn(x.e)
	}
	return false
}

func validateExpr(e expr, rt *resolvedTable) error {
	if e == nil {
		return nil
	}
	switch x := e.(type) {
	case *colRef:
		ord, ok := rt.colByName[x.name]
		if !ok {
			return fmt.Errorf("%w: %q.%q", ErrUnknownColumn, rt.def.Name, x.name)
		}
		x.ord = ord // bind once at plan time; evalExpr indexes by ord
	case *binOp:
		if err := validateExpr(x.lhs, rt); err != nil {
			return err
		}
		if err := validateExpr(x.rhs, rt); err != nil {
			return err
		}
	case *notExpr:
		return validateExpr(x.e, rt)
	case *isNullExpr:
		return validateExpr(x.e, rt)
	}
	return nil
}

// evalCtx carries the per-call state expression eval needs.
type evalCtx struct {
	row  Row
	cols map[string]int // points into resolvedTable.colByName
	args []Value
}

func evalExpr(e expr, ctx *evalCtx) (Value, error) {
	switch x := e.(type) {
	case nil:
		return Bool(true), nil
	case *colRef:
		if x.ord >= 0 {
			return ctx.row[x.ord], nil
		}
		return ctx.row[ctx.cols[x.name]], nil
	case *litValue:
		return x.v, nil
	case *paramRef:
		if x.index < 0 || x.index >= len(ctx.args) {
			return Value{}, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, x.index)
		}
		return ctx.args[x.index], nil
	case *binOp:
		switch x.op {
		case tkAnd:
			lv, err := evalExpr(x.lhs, ctx)
			if err != nil {
				return Value{}, err
			}
			if !truthy(lv) {
				return Bool(false), nil
			}
			rv, err := evalExpr(x.rhs, ctx)
			if err != nil {
				return Value{}, err
			}
			return Bool(truthy(rv)), nil
		case tkOr:
			lv, err := evalExpr(x.lhs, ctx)
			if err != nil {
				return Value{}, err
			}
			if truthy(lv) {
				return Bool(true), nil
			}
			rv, err := evalExpr(x.rhs, ctx)
			if err != nil {
				return Value{}, err
			}
			return Bool(truthy(rv)), nil
		}
		lv, err := evalExpr(x.lhs, ctx)
		if err != nil {
			return Value{}, err
		}
		rv, err := evalExpr(x.rhs, ctx)
		if err != nil {
			return Value{}, err
		}
		switch x.op {
		case tkPlus, tkMinus, tkStar:
			// Integer arithmetic. NULL propagates (any null operand -> null),
			// matching SQL semantics. int64 wraps on overflow.
			if lv.IsNull() || rv.IsNull() {
				return Null(), nil
			}
			a, err := lv.AsInt()
			if err != nil {
				return Value{}, err
			}
			b, err := rv.AsInt()
			if err != nil {
				return Value{}, err
			}
			switch x.op {
			case tkPlus:
				return Int(a + b), nil
			case tkMinus:
				return Int(a - b), nil
			default:
				return Int(a * b), nil
			}
		case tkEq:
			return Bool(lv.Equal(rv)), nil
		case tkNeq:
			return Bool(!lv.Equal(rv)), nil
		}
		cmp, ok := lv.Compare(rv)
		if !ok {
			return Bool(false), nil
		}
		switch x.op {
		case tkLt:
			return Bool(cmp < 0), nil
		case tkLte:
			return Bool(cmp <= 0), nil
		case tkGt:
			return Bool(cmp > 0), nil
		case tkGte:
			return Bool(cmp >= 0), nil
		}
	case *notExpr:
		v, err := evalExpr(x.e, ctx)
		if err != nil {
			return Value{}, err
		}
		return Bool(!truthy(v)), nil
	case *isNullExpr:
		v, err := evalExpr(x.e, ctx)
		if err != nil {
			return Value{}, err
		}
		isn := v.IsNull()
		if x.not {
			return Bool(!isn), nil
		}
		return Bool(isn), nil
	}
	return Value{}, fmt.Errorf("internal: unknown expr type %T", e)
}

func truthy(v Value) bool {
	if v.Kind == KindBool {
		return v.Int() == 1
	}
	if v.Kind == KindInt {
		return v.Int() != 0
	}
	return false
}

func evalLitOrParamAny(e expr, args []any) (Value, error) {
	switch x := e.(type) {
	case *litValue:
		return x.v, nil
	case *paramRef:
		if x.index < 0 || x.index >= len(args) {
			return Value{}, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, x.index)
		}
		return toValue(args[x.index], x.index)
	default:
		return Value{}, fmt.Errorf("internal: expected literal or parameter, got %T", e)
	}
}

// evalLitOrParamValue is evalLitOrParamAny for pre-typed Value args (the
// QueryValues / QueryRowValues path) — no toValue conversion needed.
func evalLitOrParamValue(e expr, args []Value) (Value, error) {
	switch x := e.(type) {
	case *litValue:
		return x.v, nil
	case *paramRef:
		if x.index < 0 || x.index >= len(args) {
			return Value{}, fmt.Errorf("%w: param index %d out of range", ErrParamMismatch, x.index)
		}
		return args[x.index], nil
	default:
		return Value{}, fmt.Errorf("internal: expected literal or parameter, got %T", e)
	}
}

func (db *DB) execSelectPK(pl *plan, keyVal Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if keyVal.IsNull() {
		return colNames, nil, nil
	}
	pk, err := coerceToUUID(keyVal)
	if err != nil {
		return nil, nil, err
	}
	if st.starAll {
		r, ok := tbl.getByPK(pk)
		if !ok {
			return colNames, nil, nil
		}
		return colNames, []Row{r}, nil
	}
	pr, ok := tbl.getByPKProject(pk, pl.projOrdinals)
	if !ok {
		return colNames, nil, nil
	}
	return colNames, []Row{pr}, nil
}

// execSelectPKOne is execSelectPK for QueryRow: it returns the single matched
// row directly (nil if none), skipping the []Row result slice Query allocates.
func (db *DB) execSelectPKOne(pl *plan, keyVal Value) ([]string, Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if keyVal.IsNull() {
		return colNames, nil, nil
	}
	pk, err := coerceToUUID(keyVal)
	if err != nil {
		return nil, nil, err
	}
	if st.starAll {
		r, _ := tbl.getByPK(pk)
		return colNames, r, nil
	}
	pr, _ := tbl.getByPKProject(pk, pl.projOrdinals)
	return colNames, pr, nil
}

// offerLiveRow offers pk's live row to an ORDER BY top-N heap under the shard
// read lock: pred (the full WHERE) and the order-column compare read the row in
// place, and topN.offer clones it only if it makes the cut. So ORDER BY ... LIMIT
// n over a large filtered set clones ~n rows, not the whole matched set.
func (t *table) offerLiveRow(pk UUID, pred func(Row) bool, top *topN) {
	s := t.shardOf(pk)
	s.mu.RLock()
	if rowID, ok := s.pk[pk]; ok {
		if r := s.rows[rowID]; r != nil && pred(r) {
			top.offer(r)
		}
	}
	s.mu.RUnlock()
}

// execSelectIdx runs a SELECT whose WHERE pins one or more secondary-indexed
// columns by equality. It resolves candidate PKs through the index(es)
// (intersecting buckets for an AND of equalities) plus the dirty overlay, then
// evaluates the full WHERE on each live row. With an ORDER BY it gathers all
// matches and sorts before LIMIT (the filtered-list pattern); otherwise it
// projects and stops at LIMIT.
func (db *DB) execSelectIdx(pl *plan, ctx *evalCtx) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if st.limit == 0 {
		return colNames, nil, nil
	}
	// Index side: one bucket per indexed equality conjunct, intersected. With
	// two indexes (WHERE name = ? AND city = ?) this shrinks the candidate set
	// to rows matching BOTH before any row is fetched — e.g. the 1000 Peters in
	// Amsterdam, not all 8000 Peters. A NULL key matches nothing.
	var pks []UUID
	for i, ord := range pl.idxCols {
		keyVal, err := evalExpr(pl.idxSrcs[i], ctx)
		if err != nil {
			return nil, nil, err
		}
		if keyVal.IsNull() {
			return colNames, nil, nil
		}
		si := tbl.indexFor(ord)
		if si == nil {
			return colNames, nil, nil
		}
		bucket := si.lookup(keyOf(keyVal))
		if i == 0 {
			pks = bucket
		} else {
			pks = intersectPKs(pks, bucket)
		}
		if len(pks) == 0 {
			break // index side empty; the dirty overlay below may still match
		}
	}
	// Hybrid candidate set: the (intersected) index hits UNION the dirty PKs
	// (mutated since the last merge, membership uncertain). Every candidate's
	// live row is evaluated against the FULL WHERE, so neither a stale entry, an
	// unrelated dirty PK, nor an extra conjunct can yield a wrong row. starAll
	// keeps the whole row; otherwise project the wanted columns.
	dirty := tbl.dirtyPKs()
	if len(pks) == 0 && len(dirty) == 0 {
		return colNames, nil, nil
	}
	// emit visits each candidate PK once, calling fn (stop on true). The index
	// buckets are already unique, so a dedup set is only built when the dirty
	// overlay is non-empty (a PK may then be in both) — the steady-state read
	// (merged, dirty empty) walks the bucket with no per-candidate map.
	emit := func(fn func(pk UUID) bool) {
		if len(dirty) == 0 {
			for _, pk := range pks {
				if fn(pk) {
					return
				}
			}
			return
		}
		seen := make(map[UUID]struct{}, len(pks)+len(dirty))
		visit := func(pk UUID) bool {
			if _, dup := seen[pk]; dup {
				return false
			}
			seen[pk] = struct{}{}
			return fn(pk)
		}
		for _, pk := range pks {
			if visit(pk) {
				return
			}
		}
		for _, pk := range dirty {
			if visit(pk) {
				return
			}
		}
	}

	// ORDER BY: gather every matching live row, sort by the order column, then
	// LIMIT. The candidate set is the index-narrowed (filtered) subset — the
	// "list view" pattern (WHERE author = ? ORDER BY date LIMIT 20) — so sorting
	// it is cheap. (Sorting a near-whole-table subset is the caller's call; that
	// is no longer a list view.)
	if pl.orderOrdinal >= 0 {
		pred := func(r Row) bool {
			ctx.row = r
			v, err := evalExpr(st.where, ctx)
			return err == nil && truthy(v)
		}
		// ORDER BY + LIMIT: a top-N heap clones only ~limit rows, so the cost
		// tracks the LIMIT, not the matched-set size (offer reads/clones the live
		// row under the shard lock). st.limit == 0 already returned above.
		if st.limit >= 0 {
			top := &topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: st.limit}
			emit(func(pk UUID) bool {
				tbl.offerLiveRow(pk, pred, top)
				return false
			})
			return colNames, projectRows(top.sorted(), st.starAll, pl.projOrdinals), nil
		}
		// ORDER BY without LIMIT: gather every match, then sort.
		var matched []Row
		emit(func(pk UUID) bool {
			if r, ok := tbl.getByPK(pk); ok && pred(r) {
				matched = append(matched, r)
			}
			return false
		})
		sortRowsByCol(matched, pl.orderOrdinal, st.orderDesc)
		return colNames, projectRows(matched, st.starAll, pl.projOrdinals), nil
	}

	// No ORDER BY: project and stop as soon as LIMIT is reached.
	out := make([]Row, 0, len(pks))
	emit(func(pk UUID) bool {
		r, ok := tbl.getByPK(pk)
		if !ok {
			return false
		}
		ctx.row = r
		if v, err := evalExpr(st.where, ctx); err != nil || !truthy(v) {
			return false
		}
		if st.starAll {
			out = append(out, r)
		} else {
			out = append(out, projectClone(r, pl.projOrdinals))
		}
		return st.limit >= 0 && len(out) >= st.limit
	})
	return colNames, out, nil
}

// execSelectIdxOne is the single-row (QueryRow) form of execSelectIdx for a
// SELECT with no ORDER BY: it returns the first candidate whose live row passes
// the full WHERE and stops, skipping the []Row slice + collect machinery the
// multi-row path builds. A dirty PK already covered by the index pass that
// failed the WHERE simply re-fails (no dedup needed for first-match).
func (db *DB) execSelectIdxOne(pl *plan, args []Value) ([]string, Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if st.limit == 0 {
		return colNames, nil, nil
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	var pks []UUID
	for i, ord := range pl.idxCols {
		keyVal, err := evalExpr(pl.idxSrcs[i], &ctx)
		if err != nil {
			return nil, nil, err
		}
		if keyVal.IsNull() {
			return colNames, nil, nil
		}
		si := tbl.indexFor(ord)
		if si == nil {
			return colNames, nil, nil
		}
		bucket := si.lookup(keyOf(keyVal))
		if i == 0 {
			pks = bucket
		} else {
			pks = intersectPKs(pks, bucket)
		}
		if len(pks) == 0 {
			break
		}
	}
	try := func(pk UUID) (Row, bool) {
		r, ok := tbl.getByPK(pk)
		if !ok {
			return nil, false
		}
		ctx.row = r
		if v, err := evalExpr(st.where, &ctx); err != nil || !truthy(v) {
			return nil, false
		}
		if st.starAll {
			return r, true
		}
		return projectClone(r, pl.projOrdinals), true
	}
	for _, pk := range pks {
		if r, ok := try(pk); ok {
			return colNames, r, nil
		}
	}
	for _, pk := range tbl.dirtyPKs() {
		if r, ok := try(pk); ok {
			return colNames, r, nil
		}
	}
	return colNames, nil, nil
}

// execSelectOrderedWalk serves an ORDER BY on an ordered-indexed column by
// walking the sorted index in order, merged with the dirty overlay (rows
// mutated since the last merge), applying any residual WHERE, and stopping at
// LIMIT — touching ~LIMIT rows, not the whole table. A non-dirty index entry is
// fresh (its key equals the live value), so the index key drives the ordering
// and the row is fetched only when selected. Dirty PKs are excluded from the
// index walk (the entry may be stale) and supplied from their live rows.
func (db *DB) execSelectOrderedWalk(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if st.limit == 0 {
		return colNames, nil, nil
	}
	si := tbl.indexFor(pl.orderOrdinal)
	if si == nil {
		return colNames, nil, nil
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	matches := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, &ctx)
		return err == nil && truthy(v)
	}
	project := func(r Row) Row {
		if st.starAll {
			return r
		}
		return projectClone(r, pl.projOrdinals)
	}

	snap := si.snapshot()
	dirty := tbl.dirtyPKs()
	dirtySet := make(map[UUID]struct{}, len(dirty))
	type dcand struct {
		key indexKey
		row Row
	}
	var dc []dcand
	for _, pk := range dirty {
		if _, dup := dirtySet[pk]; dup {
			continue
		}
		dirtySet[pk] = struct{}{}
		if r, ok := tbl.getByPK(pk); ok && matches(r) {
			dc = append(dc, dcand{keyOf(r[pl.orderOrdinal]), r})
		}
	}
	sort.Slice(dc, func(i, j int) bool { return dc[i].key.less(dc[j].key) })

	desc := st.orderDesc
	before := func(a, b indexKey) bool { // a comes before b in the requested order
		if desc {
			return b.less(a)
		}
		return a.less(b)
	}

	capHint := st.limit
	if capHint < 0 || capHint > 1024 {
		capHint = 1024
	}
	out := make([]Row, 0, capHint)
	ii, dj := 0, 0
	if desc {
		ii, dj = len(snap)-1, len(dc)-1
	}
	idxOK := func() bool { return ii >= 0 && ii < len(snap) }
	dcOK := func() bool { return dj >= 0 && dj < len(dc) }
	stepIdx := func() {
		if desc {
			ii--
		} else {
			ii++
		}
	}
	stepDc := func() {
		if desc {
			dj--
		} else {
			dj++
		}
	}
	takeIdx := func() {
		pk := snap[ii].pk
		stepIdx()
		if st.where == nil { // no residual: fetch projected directly (one clone)
			if st.starAll {
				if r, ok := tbl.getByPK(pk); ok {
					out = append(out, r)
				}
			} else if pr, ok := tbl.getByPKProject(pk, pl.projOrdinals); ok {
				out = append(out, pr)
			}
			return
		}
		if r, ok := tbl.getByPK(pk); ok && matches(r) { // residual needs the full row
			out = append(out, project(r))
		}
	}
	for st.limit < 0 || len(out) < st.limit {
		for idxOK() { // skip index entries shadowed by the dirty overlay
			if _, d := dirtySet[snap[ii].pk]; !d {
				break
			}
			stepIdx()
		}
		switch {
		case idxOK() && dcOK():
			if before(dc[dj].key, snap[ii].key) {
				out = append(out, project(dc[dj].row))
				stepDc()
			} else {
				takeIdx()
			}
		case dcOK():
			out = append(out, project(dc[dj].row))
			stepDc()
		case idxOK():
			takeIdx()
		default:
			return colNames, out, nil // both streams exhausted
		}
	}
	return colNames, out, nil
}

// execSelect runs the SELECT plan. Returns the columns and a slice of
// projected rows. Rows are deep-cloned before returning so the caller
// may mutate them without affecting storage.
func (db *DB) execSelect(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt

	colNames := pl.colNames

	ctx := evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single map lookup, no scan, no sort.
	// Project directly into the result row to skip the matched-list
	// allocation and the full-row clone.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &ctx)
		if err != nil {
			return nil, nil, err
		}
		if keyVal.IsNull() {
			return colNames, nil, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return nil, nil, err
		}
		// SELECT * needs the whole row; a projection clones only its columns
		// under the lock (getByPKProject), skipping a full-row clone.
		if st.starAll {
			r, ok := tbl.getByPK(pk)
			if !ok {
				return colNames, nil, nil
			}
			return colNames, []Row{r}, nil
		}
		pr, ok := tbl.getByPKProject(pk, pl.projOrdinals)
		if !ok {
			return colNames, nil, nil
		}
		return colNames, []Row{pr}, nil
	}

	// Secondary-index lookup: resolve candidate PKs through the index, fetch
	// each by PK and project. No scan.
	if pl.idxLookup {
		return db.execSelectIdx(pl, &ctx)
	}

	// Ordered-index ORDER BY: walk the sorted index (merged with the dirty
	// overlay) in order and stop at LIMIT — no scan, no sort.
	if pl.orderWalk {
		return db.execSelectOrderedWalk(pl, args)
	}

	// Collect matching rows. A partition-pinned SELECT (WHERE partkey = ?)
	// reads only that partition's rows; otherwise scan every shard.
	var part UUID
	partPinned := false
	if pl.partLookup {
		pv, err := evalExpr(pl.partSource, &ctx)
		if err != nil {
			return nil, nil, err
		}
		if pv.IsNull() {
			return colNames, nil, nil
		}
		u, err := coerceToUUID(pv)
		if err != nil {
			return nil, nil, err
		}
		part = u
		partPinned = true
	}

	// No ORDER BY means LIMIT can be applied during the scan. Copy only rows
	// that will be returned, while the shard read lock is still held.
	if pl.orderOrdinal < 0 && st.limit >= 0 {
		if st.limit == 0 {
			return colNames, nil, nil
		}
		// Cap the prealloc: a huge LIMIT over a small result must not allocate a
		// giant slice up front. The scan still stops at st.limit; append grows
		// past the hint only if that many rows actually match.
		capHint := st.limit
		if capHint > 1024 {
			capHint = 1024
		}
		out := make([]Row, 0, capHint)
		width := len(pl.projOrdinals)
		if st.starAll {
			width = len(tbl.def.def.Columns)
		}
		var packed []Value
		if partPinned {
			s := tbl.shardOf(part)
			s.mu.RLock()
			for _, rowID := range s.tails[part] {
				if rowID >= uint64(len(s.rows)) {
					continue
				}
				r := s.rows[rowID]
				if r == nil {
					continue
				}
				if st.where != nil {
					ctx.row = r
					v, err := evalExpr(st.where, &ctx)
					if err != nil || !truthy(v) {
						continue
					}
				}
				if packed == nil {
					packed = make([]Value, 0, capHint*width)
				}
				start := len(packed)
				if st.starAll {
					packed = appendRowClone(packed, r)
				} else {
					packed = appendProjectClone(packed, r, pl.projOrdinals)
				}
				out = append(out, Row(packed[start:len(packed):len(packed)]))
				if len(out) >= st.limit {
					break
				}
			}
			s.mu.RUnlock()
			return colNames, out, nil
		}
		for i := range tbl.shards {
			s := &tbl.shards[i]
			s.mu.RLock()
			stop := false
			for _, r := range s.rows {
				if r == nil {
					continue
				}
				if st.where != nil {
					ctx.row = r
					v, err := evalExpr(st.where, &ctx)
					if err != nil || !truthy(v) {
						continue
					}
				}
				if packed == nil {
					packed = make([]Value, 0, capHint*width)
				}
				start := len(packed)
				if st.starAll {
					packed = appendRowClone(packed, r)
				} else {
					packed = appendProjectClone(packed, r, pl.projOrdinals)
				}
				out = append(out, Row(packed[start:len(packed):len(packed)]))
				if len(out) >= st.limit {
					stop = true
					break
				}
			}
			s.mu.RUnlock()
			if stop {
				break
			}
		}
		return colNames, out, nil
	}

	scan := tbl.scanAll
	if partPinned {
		scan = func(fn func(Row) bool) { tbl.scanPartition(part, fn) }
	}

	// ORDER BY + LIMIT: keep only the best `limit` rows by the order column,
	// cloning a row only when it enters the running top-N — O(limit) clones
	// instead of O(matched). (Ties on the order column drop arbitrarily, which
	// SQL permits for a non-unique ORDER BY.)
	if pl.orderOrdinal >= 0 && st.limit >= 0 {
		if st.limit == 0 {
			return colNames, nil, nil
		}
		top := topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: st.limit}
		scan(func(r Row) bool {
			if st.where != nil {
				ctx.row = r
				v, err := evalExpr(st.where, &ctx)
				if err != nil || !truthy(v) {
					return true
				}
			}
			top.offer(r)
			return true
		})
		return colNames, projectRows(top.sorted(), st.starAll, pl.projOrdinals), nil
	}

	// ORDER BY without LIMIT, or no-ORDER-BY/no-LIMIT full scan: gather all
	// matches (clone under the lock), then sort if ordered.
	var matched []Row
	scan(func(r Row) bool {
		if st.where != nil {
			ctx.row = r
			v, err := evalExpr(st.where, &ctx)
			if err != nil || !truthy(v) {
				return true
			}
		}
		matched = append(matched, r.Clone())
		return true
	})
	if pl.orderOrdinal >= 0 {
		sortRowsByCol(matched, pl.orderOrdinal, st.orderDesc)
	}
	if st.limit >= 0 && st.limit < len(matched) {
		matched = matched[:st.limit]
	}
	return colNames, projectRows(matched, st.starAll, pl.projOrdinals), nil
}

// sortRowsByCol stable-sorts rows by column ord (ascending, or descending when
// desc). Incomparable cells (NULL) sort as "not less".
func sortRowsByCol(rows []Row, ord int, desc bool) {
	sort.SliceStable(rows, func(i, j int) bool {
		c, ok := rows[i][ord].Compare(rows[j][ord])
		if !ok {
			return false
		}
		if desc {
			return c > 0
		}
		return c < 0
	})
}

// projectRows narrows each row to the projection ordinals (a no-op for
// SELECT *). The narrowed rows shallow-copy already-private cells.
func projectRows(matched []Row, starAll bool, ords []int) []Row {
	if starAll {
		return matched
	}
	out := make([]Row, len(matched))
	for i, r := range matched {
		pr := make(Row, len(ords))
		for j, ord := range ords {
			pr[j] = r[ord]
		}
		out[i] = pr
	}
	return out
}

// topN keeps the best capN rows by column ord (ascending, or descending when
// desc), cloning a row only when it makes the cut. Backed by a binary heap
// whose root is the most-evictable kept row, so a candidate that cannot beat
// the root is dropped without cloning.
type topN struct {
	ord  int
	desc bool
	capN int
	h    []Row
}

// evictable reports whether a ranks below b under the ORDER BY (a drops first).
func (t *topN) evictable(a, b Row) bool {
	c, ok := a[t.ord].Compare(b[t.ord])
	if !ok {
		return false
	}
	if t.desc {
		return c < 0 // DESC keeps the largest, so the smaller drops first
	}
	return c > 0 // ASC keeps the smallest, so the larger drops first
}

func (t *topN) offer(r Row) {
	if len(t.h) < t.capN {
		t.h = append(t.h, r.Clone())
		for i := len(t.h) - 1; i > 0; {
			p := (i - 1) / 2
			if !t.evictable(t.h[i], t.h[p]) {
				break
			}
			t.h[i], t.h[p] = t.h[p], t.h[i]
			i = p
		}
		return
	}
	if !t.evictable(t.h[0], r) { // r can't beat the current worst
		return
	}
	t.h[0] = r.Clone()
	for i, n := 0, len(t.h); ; {
		worst, l, rr := i, 2*i+1, 2*i+2
		if l < n && t.evictable(t.h[l], t.h[worst]) {
			worst = l
		}
		if rr < n && t.evictable(t.h[rr], t.h[worst]) {
			worst = rr
		}
		if worst == i {
			break
		}
		t.h[i], t.h[worst] = t.h[worst], t.h[i]
		i = worst
	}
}

// sorted returns the kept rows in ORDER BY order.
func (t *topN) sorted() []Row {
	sortRowsByCol(t.h, t.ord, t.desc)
	return t.h
}

// buildInsertRow materialises the full row for an INSERT plan: evaluates each
// supplied value (with API-boundary string→UUID coercion), validates it,
// auto-generates the PK when omitted, and enforces NOT NULL. The resolved row
// is what both the single-statement path and the transaction path journal, so
// replay reproduces the exact same row (including any auto-generated UUID).
func (db *DB) buildInsertRow(pl *plan, args []Value) (Row, error) {
	st := pl.st.(*insertStmt)
	tbl := pl.rt
	row := make(Row, len(tbl.def.def.Columns))
	for i := range row {
		row[i] = Null()
	}
	ctx := &evalCtx{args: args}
	pkOrd := tbl.def.pkOrdinal
	pkProvided := false
	for i, ord := range pl.insertOrdinals {
		v, err := evalExpr(st.vals[i], ctx)
		if err != nil {
			return nil, err
		}
		col := tbl.def.def.Columns[ord]
		// API-boundary coercion: a string destined for a UUID column is
		// parsed into a UUID — storage only ever sees [16]byte.
		if col.Type == TypeUUID && v.Kind == KindString {
			u, perr := ParseUUID(v.Str())
			if perr != nil {
				return nil, perr
			}
			v = UUIDVal(u)
		}
		if err := validateValue(col, v); err != nil {
			return nil, err
		}
		row[ord] = v
		if ord == pkOrd {
			pkProvided = true
		}
	}
	// Auto-generate the PK when omitted. The resolved UUID is placed in the
	// row before the WAL record is written, so replay reproduces it exactly.
	if !pkProvided {
		row[pkOrd] = UUIDVal(NewUUIDv7())
	}
	// Check NOT NULL on omitted columns.
	for ord, c := range tbl.def.def.Columns {
		if row[ord].IsNull() && !c.Nullable {
			return nil, fmt.Errorf("column %q is NOT NULL", c.Name)
		}
	}
	return row, nil
}

// execInsert builds the row and appends. Returns the count (1 if
// inserted) and an error.
func (db *DB) execInsert(pl *plan, args []Value) (int, error) {
	tbl := pl.rt
	row, err := db.buildInsertRow(pl, args)
	if err != nil {
		return 0, err
	}
	// PK uniqueness + WAL append + apply run atomically under the shard
	// lock (see insertJournaled). Ordering matters: a duplicate PK must be
	// rejected before anything is journaled, and a WAL failure must abort
	// before the row is applied.
	if err := tbl.insertJournaled(row, func() error {
		if db.wal == nil {
			return nil
		}
		body := encodeInsertMutation(db.scratch.get(), tbl.tableID, row)
		werr := db.wal.writeRecord(recMutation, body)
		db.scratch.put(body)
		return werr
	}); err != nil {
		return 0, err
	}
	return 1, nil
}

// execUpdate evaluates the SET values once, then dispatches on the WHERE
// shape. A PK-pinned update hits exactly one shard and mutates in place
// under that shard's lock (hot path, allocation-free). An unpinned predicate
// update can span shards and goes through updateWhereAll, which holds every
// shard lock across the journal+apply so the WAL order and in-memory order
// stay identical — the one-shard-at-a-time form is a replay-divergence bug.
func (db *DB) execUpdate(pl *plan, args []Value) (int, error) {
	st := pl.st.(*updateStmt)
	tbl := pl.rt
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Common hot path: one-column PK update. Avoid materialising a []Value
	// SET buffer; compute and apply the single cell directly under the shard
	// lock.
	if pl.pkLookup && len(pl.updateOrdinals) == 1 {
		ord := pl.updateOrdinals[0]
		col := tbl.def.def.Columns[ord]
		computeOne := func(r Row) (Value, error) {
			ctx.row = r
			v, err := evalExpr(st.sets[0].val, ctx)
			if err != nil {
				return Value{}, err
			}
			if err := validateValue(col, v); err != nil {
				return Value{}, err
			}
			return v, nil
		}
		if !pl.setRowDependent {
			v, err := computeOne(nil)
			if err != nil {
				return 0, err
			}
			computeOne = func(Row) (Value, error) { return v, nil }
		}
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		var journal func(Row) error
		if db.wal != nil {
			journal = func(nr Row) error {
				body := encodeUpdateMutation(db.scratch.get(), tbl.tableID, nr[tbl.def.pkOrdinal], pl.updateOrdinals, nr)
				werr := db.wal.writeRecord(recMutation, body)
				db.scratch.put(body)
				return werr
			}
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		ok, err := tbl.updateByPKOneJournaled(pk, ord, computeOne, journal)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	match := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		if err != nil {
			return false
		}
		return truthy(v)
	}

	// SET right-hand sides evaluate into a reused buffer. evalSet validates
	// each result against its column. For constant SETs (no column ref) we
	// evaluate once up front and hand back the same buffer for every row
	// (allocation-free hot path); for row-dependent SETs (col = col - ?) we
	// re-evaluate per row with the row in context.
	buf := make([]Value, len(st.sets))
	cols := tbl.def.def.Columns
	evalSet := func(r Row) ([]Value, error) {
		ctx.row = r
		for i, a := range st.sets {
			v, err := evalExpr(a.val, ctx)
			if err != nil {
				return nil, err
			}
			if err := validateValue(cols[pl.updateOrdinals[i]], v); err != nil {
				return nil, err
			}
			buf[i] = v
		}
		return buf, nil
	}
	var compute func(Row) ([]Value, error)
	if pl.setRowDependent {
		compute = evalSet
	} else {
		if _, err := evalSet(nil); err != nil { // constant: evaluate once
			return 0, err
		}
		compute = func(Row) ([]Value, error) { return buf, nil }
	}

	// Fast path: PK equality — single shard, journal-before-apply under that
	// shard's lock (updateByPKJournaled). nil journal for memory-only keeps
	// this hot path allocation-free.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		var journal func(Row) error
		if db.wal != nil {
			journal = func(nr Row) error {
				body := encodeUpdateMutation(db.scratch.get(), tbl.tableID, nr[tbl.def.pkOrdinal], pl.updateOrdinals, nr)
				werr := db.wal.writeRecord(recMutation, body)
				db.scratch.put(body)
				return werr
			}
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		ok, err := tbl.updateByPKJournaled(pk, pl.updateOrdinals, compute, journal)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// Multi-shard predicate path: updateWhereAll collects every matched row's
	// new image under all shard locks, journals the batch as ONE TXN envelope,
	// then applies — so the statement is atomic (all-or-nothing on WAL failure
	// or crash). encode/journalAll are nil for a memory-only DB.
	var encode func(Row) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(nr Row) []byte {
			return encodeUpdateMutation(nil, tbl.tableID, nr[tbl.def.pkOrdinal], pl.updateOrdinals, nr)
		}
		journalAll = db.journalTxnBodies
	}
	return tbl.updateWhereAll(match, pl.updateOrdinals, compute, encode, journalAll)
}

// execDelete dispatches on the WHERE shape, mirroring execUpdate. A
// PK-pinned delete hits one shard. An unpinned predicate delete goes
// through deleteWhereAll, which holds every shard lock across journal+apply
// (the one-shard-at-a-time form diverges on replay). Journaling is done by
// the store under the locks — never as a side effect of the match predicate.
func (db *DB) execDelete(pl *plan, args []Value) (int, error) {
	st := pl.st.(*deleteStmt)
	tbl := pl.rt
	ctx := &evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single shard, journal-before-tombstone under
	// the shard lock (deleteByPKJournaled, which also closes the old
	// getByPK→deleteByPK TOCTOU). nil journal for memory-only.
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &evalCtx{args: args})
		if err != nil {
			return 0, err
		}
		if keyVal.IsNull() {
			return 0, nil
		}
		pk, err := coerceToUUID(keyVal)
		if err != nil {
			return 0, err
		}
		pkVal := UUIDVal(pk) // journal the resolved UUID, not the raw arg
		var journal func() error
		if db.wal != nil {
			journal = func() error {
				body := encodeDeleteMutation(db.scratch.get(), tbl.tableID, pkVal)
				werr := db.wal.writeRecord(recMutation, body)
				db.scratch.put(body)
				return werr
			}
		}
		ok, err := tbl.deleteByPKJournaled(pk, journal)
		if err != nil {
			return 0, err
		}
		if ok {
			return 1, nil
		}
		return 0, nil
	}

	// Multi-shard predicate path: deleteWhereAll collects matched PKs under all
	// shard locks, journals the batch as ONE TXN envelope, then tombstones — so
	// the statement is atomic. encode/journalAll are nil for a memory-only DB.
	match := func(r Row) bool {
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		if err != nil {
			return false
		}
		return truthy(v)
	}
	var encode func(Value) []byte
	var journalAll func([][]byte) error
	if db.wal != nil {
		encode = func(pk Value) []byte {
			return encodeDeleteMutation(nil, tbl.tableID, pk)
		}
		journalAll = db.journalTxnBodies
	}
	return tbl.deleteWhereAll(match, encode, journalAll)
}
