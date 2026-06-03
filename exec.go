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
	// colJSONPrefix is each column's pre-escaped object-key fragment ("col":),
	// computed once at plan time so QueryJSON appends it per row instead of
	// re-escaping the (constant) column name on every row. Read-only, shared.
	colJSONPrefix [][]byte
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

	// compLookup / compWalk select a composite ordered index whose leading
	// columns are pinned by equality (WHERE a = ? [AND b = ?]). compOrdinals is
	// the chosen index's full ordinal list (locates the *secIndex at exec time);
	// compPrefixSrcs yields the pinned leading-column values, in order, encoded
	// into a key prefix. compLookup resolves candidate PKs via that prefix;
	// compWalk additionally walks the prefix sub-range in ORDER BY order (the
	// trailing column) and stops at LIMIT — no sort.
	compLookup     bool
	compWalk       bool
	compOrdinals   []int
	compPrefixSrcs []expr
	// compResidual (compWalk only) is the WHERE conjuncts the pinned prefix does
	// NOT already guarantee. The walk's snap sub-range is exactly the pinned-key
	// rows, so re-checking the prefix equalities per row is wasted work; an empty
	// residual lets the walk take the one-clone fast path.
	compResidual []expr

	// joinPlan is non-nil for a two-table join; execSelect dispatches to execJoin.
	// projOrdinals / colNames / orderOrdinal are then bound to global concat-row
	// ordinals (see join.go).
	joinPlan *joinPlan
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
		// Two-table join: resolved on its own path (concat-row ordinals, the
		// indexed-only law, drive-side). pl.rt above is the FROM table; planJoin
		// re-resolves both tables and sets pl.joinPlan.
		if len(s.joins) > 0 {
			if err := db.planJoin(pl, s, cat); err != nil {
				return nil, err
			}
			return pl, nil
		}
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
		// Pre-escape each column's "col": object-key fragment once, so the
		// per-row JSON encoder appends bytes instead of re-escaping a constant.
		pl.colJSONPrefix = make([][]byte, len(pl.colNames))
		for i, name := range pl.colNames {
			pl.colJSONPrefix[i] = append(appendJSONString(nil, name), ':')
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
					if len(ri.ordinals) != 1 {
						continue // composite indexes are handled by planComposite below
					}
					ord := ri.ordinals[0]
					if src, has := eqs[ord]; has {
						pl.idxCols = append(pl.idxCols, ord)
						pl.idxSrcs = append(pl.idxSrcs, src)
					}
				}
				pl.idxLookup = len(pl.idxCols) > 0
				if !pl.idxLookup {
					rt.planComposite(pl, s.where, eqs, pl.orderOrdinal)
				}
			}
		}
		// ORDER BY on an ordered-indexed column, with no equality index chosen
		// (applies with or without a WHERE): walk the sorted index in order
		// instead of scanning + sorting.
		if s.orderCol != "" && !pl.idxLookup && !pl.compLookup && !pl.compWalk && rt.orderedIndexOn(pl.orderOrdinal) {
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
			} else {
				rt.planIndexEq(pl, s.where) // secondary-index candidates instead of a scan
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
			} else {
				rt.planIndexEq(pl, s.where) // secondary-index candidates instead of a scan
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

// planIndexEq sets idxLookup + idxCols/idxSrcs when the WHERE pins one or more
// single-column secondary-indexed columns by equality, so UPDATE/DELETE resolve
// candidates through the index (like SELECT) instead of scanning every row.
func (rt *resolvedTable) planIndexEq(pl *plan, where expr) {
	if where == nil || len(rt.indexes) == 0 {
		return
	}
	eqs := map[int]expr{}
	collectEqConjuncts(where, eqs)
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		if len(ri.ordinals) != 1 {
			continue
		}
		if src, has := eqs[ri.ordinals[0]]; has {
			pl.idxCols = append(pl.idxCols, ri.ordinals[0])
			pl.idxSrcs = append(pl.idxSrcs, src)
		}
	}
	pl.idxLookup = len(pl.idxCols) > 0
}

// planComposite selects a composite ordered index for a SELECT whose WHERE pins
// a contiguous leading prefix of its columns by equality (eqs maps a pinned
// column ordinal to the expr yielding its value). It records the index's ordinals
// + the prefix value-sources, then chooses compWalk when the ORDER BY is on the
// first unpinned column (already sorted within the prefix — no sort) or
// compLookup otherwise. The leading column must be pinned (k ≥ 1). First usable
// index wins.
func (rt *resolvedTable) planComposite(pl *plan, where expr, eqs map[int]expr, orderOrdinal int) {
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		if len(ri.ordinals) < 2 || !ri.ordered {
			continue
		}
		// Correctness guard: a composite index excludes any row with a NULL in any
		// component, so it can only serve a query completely when every component
		// is NOT NULL — else a (a=X, b=NULL) row matches WHERE a=? yet is absent
		// from the index. Nullable-component composites fall back to scan.
		if rt.anyNullable(ri.ordinals) {
			continue
		}
		k := 0
		for k < len(ri.ordinals) {
			if _, has := eqs[ri.ordinals[k]]; !has {
				break
			}
			k++
		}
		if k == 0 {
			continue // leading column not pinned by equality
		}
		srcs := make([]expr, k)
		pinned := make(map[int]bool, k)
		for j := 0; j < k; j++ {
			srcs[j] = eqs[ri.ordinals[j]]
			pinned[ri.ordinals[j]] = true
		}
		pl.compOrdinals = ri.ordinals
		pl.compPrefixSrcs = srcs
		if orderOrdinal >= 0 && k < len(ri.ordinals) && ri.ordinals[k] == orderOrdinal {
			pl.compWalk = true // ORDER BY on the trailing column: walk in order, no sort
			// Residual = WHERE conjuncts the prefix does not already guarantee. Drop
			// only the EXACT pin equalities (by value-expr identity), so a second
			// constraint on a pinned column survives and stays correct.
			var conj []expr
			collectConjuncts(where, &conj)
			for _, c := range conj {
				if b, ok := c.(*binOp); ok && b.op == tkEq {
					if cr, val := colAndLit(b); cr != nil && pinned[cr.ord] && val == eqs[cr.ord] {
						continue
					}
				}
				pl.compResidual = append(pl.compResidual, c)
			}
		} else {
			pl.compLookup = true
		}
		return
	}
}

// anyNullable reports whether any of the columns at ords is declared nullable.
func (rt *resolvedTable) anyNullable(ords []int) bool {
	for _, o := range ords {
		if rt.def.Columns[o].Nullable {
			return true
		}
	}
	return false
}

// orderedIndexOn reports whether column ord has an ORDERED secondary index.
func (rt *resolvedTable) orderedIndexOn(ord int) bool {
	for i := range rt.indexes {
		if rt.indexes[i].singleOn(ord) && rt.indexes[i].ordered {
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
	// A PK match is at most one row, so LIMIT 0 or any OFFSET drops it.
	if st.limit == 0 || st.offset > 0 {
		return colNames, nil, nil
	}
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
	// A PK match is at most one row, so LIMIT 0 or any OFFSET drops it.
	if st.limit == 0 || st.offset > 0 {
		return colNames, nil, nil
	}
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

// getMatchProject fetches pk's live row, evaluates pred (the full WHERE) on it
// UNDER the shard read lock, and on a pass returns the projection: the ords
// columns cloned, or the whole row for SELECT *. Folding the WHERE check into the
// lock lets an indexed read skip the full-row clone getByPK takes only to
// evaluate and then project from — it clones just the columns it returns.
// Mirrors offerLiveRow; the secondary-index path is non-partitioned.
func (t *table) getMatchProject(pk UUID, pred func(Row) bool, ords []int, starAll bool) (Row, bool) {
	s := t.shardOf(pk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return nil, false
	}
	r := s.rows[rowID]
	if r == nil || !pred(r) {
		return nil, false
	}
	if starAll {
		return r.Clone(), true
	}
	return projectClone(r, ords), true
}

// getMatchProjectInto is the scan-into form of getMatchProject: it writes the
// projection (or the whole row for SELECT *) into dst, reset to empty first, so a
// projection without BYTES columns makes no allocation. The single-row
// QueryRowByIndex fast path. Non-partitioned.
func (t *table) getMatchProjectInto(pk UUID, pred func(Row) bool, ords []int, starAll bool, dst []Value) ([]Value, bool) {
	return t.appendMatchProject(pk, pred, ords, starAll, dst[:0])
}

// appendMatchProject is getMatchProjectInto without the reset: on a WHERE pass it
// APPENDS the projection (or the whole row for SELECT *) to dst and returns the
// grown slice; on a miss it returns dst unchanged. This lets a multi-row result
// pack every row's cells into one backing buffer — each Row a capped view of its
// own span — so the set costs one allocation instead of one per row. A projection
// without BYTES columns appends no allocation of its own. Non-partitioned.
func (t *table) appendMatchProject(pk UUID, pred func(Row) bool, ords []int, starAll bool, dst []Value) ([]Value, bool) {
	s := t.shardOf(pk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return dst, false
	}
	r := s.rows[rowID]
	if r == nil || !pred(r) {
		return dst, false
	}
	if starAll {
		return appendRowClone(dst, r), true
	}
	return appendProjectClone(dst, r, ords), true
}

// execSelectIdx runs a SELECT whose WHERE pins one or more secondary-indexed
// columns by equality. It resolves candidate PKs through the index(es)
// (intersecting buckets for an AND of equalities) plus the dirty overlay, then
// evaluates the full WHERE on each live row. With an ORDER BY it gathers all
// matches and sorts before LIMIT (the filtered-list pattern); otherwise it
// projects and stops at LIMIT.
// idxCandidateSet is the candidate-PK enumerator for an indexed SELECT: the
// (intersected) index hits UNION the dirty overlay. Returned by value — pks
// aliases the index bucket and dirty aliases dirtyPKs(), so constructing it
// allocates nothing, and emit takes its visitor as an argument rather than
// being a heap closure that captures the slices (which escaped). Shared by
// execSelectIdx (materialized) and selectEach (streaming) so the hybrid
// index∪dirty correctness has a single definition.
type idxCandidateSet struct {
	pks   []UUID // index hits (intersected); already unique within the index
	dirty []UUID // dirty overlay (mutated since the last merge); may overlap pks
}

// capHint is a result-slice capacity proportional to the candidate count,
// capped by limit (the scan stops there) and an absolute ceiling (a huge
// candidate set with no LIMIT must not prealloc pathologically). limit < 0
// means no LIMIT.
func (c idxCandidateSet) capHint(limit int) int {
	n := len(c.pks) + len(c.dirty)
	if limit >= 0 && limit < n {
		n = limit
	}
	if n > 1024 {
		n = 1024
	}
	return n
}

// emit visits each unique candidate PK once, calling fn (true to stop). The
// index buckets are already unique, so a dedup set is built only when the dirty
// overlay is non-empty (a PK may then be in both) — the steady-state read
// (merged, dirty empty) walks the bucket with no per-candidate map.
func (c idxCandidateSet) emit(fn func(pk UUID) bool) {
	if len(c.dirty) == 0 {
		for _, pk := range c.pks {
			if fn(pk) {
				return
			}
		}
		return
	}
	seen := make(map[UUID]struct{}, len(c.pks)+len(c.dirty))
	visit := func(pk UUID) bool {
		if _, dup := seen[pk]; dup {
			return false
		}
		seen[pk] = struct{}{}
		return fn(pk)
	}
	for _, pk := range c.pks {
		if visit(pk) {
			return
		}
	}
	for _, pk := range c.dirty {
		if visit(pk) {
			return
		}
	}
}

// idxCandidates resolves the candidate set for an indexed SELECT. ok=false means
// the result is provably empty (a NULL key, a missing index, or no index hit AND
// no dirty overlay) — the caller returns no rows.
func (db *DB) idxCandidates(pl *plan, ctx *evalCtx) (cand idxCandidateSet, ok bool, err error) {
	tbl := pl.rt
	// Index side: one bucket per indexed equality conjunct, intersected. With
	// two indexes (WHERE name = ? AND city = ?) this shrinks the candidate set
	// to rows matching BOTH before any row is fetched — e.g. the 1000 Peters in
	// Amsterdam, not all 8000 Peters. A NULL key matches nothing.
	var pks []UUID
	for i, ord := range pl.idxCols {
		keyVal, err := evalExpr(pl.idxSrcs[i], ctx)
		if err != nil {
			return idxCandidateSet{}, false, err
		}
		if keyVal.IsNull() {
			return idxCandidateSet{}, false, nil
		}
		si := tbl.indexFor(ord)
		if si == nil {
			return idxCandidateSet{}, false, nil
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
	// Hybrid candidate set: index hits UNION the dirty PKs (membership uncertain).
	// Every candidate's live row is evaluated against the FULL WHERE by the
	// caller, so neither a stale entry, an unrelated dirty PK, nor an extra
	// conjunct yields a wrong row.
	dirty := tbl.dirtyPKs()
	if len(pks) == 0 && len(dirty) == 0 {
		return idxCandidateSet{}, false, nil
	}
	return idxCandidateSet{pks: pks, dirty: dirty}, true, nil
}

func (db *DB) execSelectIdx(pl *plan, ctx *evalCtx) ([]string, []Row, error) {
	if pl.st.(*selectStmt).limit == 0 {
		return pl.colNames, nil, nil
	}
	cand, ok, err := db.idxCandidates(pl, ctx)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return pl.colNames, nil, nil
	}
	return db.execCandidates(pl, ctx, cand)
}

// execCandidates materializes a SELECT from a resolved candidate set (index hits
// ∪ dirty overlay): it evaluates the full WHERE on each candidate's live row and,
// with an ORDER BY, sorts before LIMIT (a top-N heap when LIMITed); otherwise it
// projects and stops at LIMIT. Shared by the single-column index path and the
// composite prefix-lookup path. Callers handle the LIMIT 0 early-out.
func (db *DB) execCandidates(pl *plan, ctx *evalCtx, cand idxCandidateSet) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames

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
			// Keep offset+limit rows so the offset can be dropped after sorting.
			top := &topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: fetchBound(st.limit, st.offset)}
			cand.emit(func(pk UUID) bool {
				tbl.offerLiveRow(pk, pred, top)
				return false
			})
			kept := sliceOffsetLimit(top.sorted(), st.offset, st.limit)
			return colNames, projectRows(kept, st.starAll, pl.projOrdinals), nil
		}
		// ORDER BY without LIMIT: gather every match, then sort (OFFSET drops the
		// leading rows).
		var matched []Row
		cand.emit(func(pk UUID) bool {
			if r, ok := tbl.getByPK(pk); ok && pred(r) {
				matched = append(matched, r)
			}
			return false
		})
		sortRowsByCol(matched, pl.orderOrdinal, st.orderDesc)
		kept := sliceOffsetLimit(matched, st.offset, st.limit)
		return colNames, projectRows(kept, st.starAll, pl.projOrdinals), nil
	}

	// No ORDER BY: project and stop once offset+limit rows are collected. Pack
	// every result row's cells into ONE growing backing buffer; each Row is a
	// capped view of its own span (the full-slice expression bounds its capacity),
	// so the set owns its cells with one allocation instead of one per row, while a
	// caller still cannot append past its row into the next. packed is presized
	// from the candidate count × cells-per-row so a within-hint bucket never
	// regrows. A later regrow is still correct: earlier Row views keep pointing at
	// their (still-live) prior backing.
	eff := fetchBound(st.limit, st.offset)
	hint := cand.capHint(eff)
	out := make([]Row, 0, hint)
	packed := make([]Value, 0, hint*len(colNames))
	pred := func(r Row) bool {
		ctx.row = r
		v, err := evalExpr(st.where, ctx)
		return err == nil && truthy(v)
	}
	cand.emit(func(pk UUID) bool {
		start := len(packed)
		var ok bool
		packed, ok = tbl.appendMatchProject(pk, pred, pl.projOrdinals, st.starAll, packed)
		if !ok {
			return false
		}
		out = append(out, Row(packed[start:len(packed):len(packed)]))
		return eff >= 0 && len(out) >= eff
	})
	return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
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
	pred := func(r Row) bool {
		ctx.row = r
		v, err := evalExpr(st.where, &ctx)
		return err == nil && truthy(v)
	}
	try := func(pk UUID) (Row, bool) {
		return tbl.getMatchProject(pk, pred, pl.projOrdinals, st.starAll)
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

// compositeCandidates resolves the candidate set for a composite prefix lookup:
// the PKs whose composite key starts with the encoded pinned prefix, UNION the
// dirty overlay. ok=false means provably empty (a NULL in the prefix, a missing
// index, or no hit and no dirty). The caller evaluates the full WHERE on every
// candidate, so a stale entry or an over-broad prefix yields no wrong row.
func (db *DB) compositeCandidates(pl *plan, ctx *evalCtx) (idxCandidateSet, bool, error) {
	tbl := pl.rt
	si := tbl.indexByOrdinals(pl.compOrdinals)
	if si == nil {
		return idxCandidateSet{}, false, nil
	}
	prefix := make([]Value, len(pl.compPrefixSrcs))
	for i, src := range pl.compPrefixSrcs {
		v, err := evalExpr(src, ctx)
		if err != nil {
			return idxCandidateSet{}, false, err
		}
		if v.IsNull() {
			return idxCandidateSet{}, false, nil
		}
		prefix[i] = v
	}
	pks := si.prefixLookup(encodeCompositeKey(prefix))
	dirty := tbl.dirtyPKs()
	if len(pks) == 0 && len(dirty) == 0 {
		return idxCandidateSet{}, false, nil
	}
	return idxCandidateSet{pks: pks, dirty: dirty}, true, nil
}

// execSelectCompositeLookup runs a SELECT whose WHERE pins a leading prefix of a
// composite index by equality. It resolves candidates through the prefix and
// reuses the shared candidate machinery (full-WHERE residual + ORDER BY sort).
func (db *DB) execSelectCompositeLookup(pl *plan, ctx *evalCtx) ([]string, []Row, error) {
	if pl.st.(*selectStmt).limit == 0 {
		return pl.colNames, nil, nil
	}
	cand, ok, err := db.compositeCandidates(pl, ctx)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return pl.colNames, nil, nil
	}
	return db.execCandidates(pl, ctx, cand)
}

// execSelectCompositeWalk serves WHERE <leading prefix> = ? ORDER BY <next col>
// via a composite ordered index: it walks the pinned-prefix sub-range of the
// sorted index — already ordered by the trailing column — and stops at LIMIT, so
// no sort runs. The walk reuses orderedWalk with a composite dirty key (the
// encoded tuple) so dirty rows merge into the same key space as the index
// entries. Every component is NOT NULL (planComposite's guard), so a matching
// row always has a fully-encodable key.
// compositeWalkArgs resolves the (snap, dirtyKey, residual) inputs orderedWalk
// needs for a composite prefix walk. ok=false means provably empty (missing
// index or a NULL in the pinned prefix). Shared by the materializing and
// streaming composite-walk entry points so both compute the walk identically.
func (db *DB) compositeWalkArgs(pl *plan, args []Value) (snap []ordEntry, dirtyKey func(Row) indexKey, residual []expr, ok bool, err error) {
	tbl := pl.rt
	si := tbl.indexByOrdinals(pl.compOrdinals)
	if si == nil {
		return nil, nil, nil, false, nil
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	prefix := make([]Value, len(pl.compPrefixSrcs))
	for i, src := range pl.compPrefixSrcs {
		v, err := evalExpr(src, &ctx)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if v.IsNull() {
			return nil, nil, nil, false, nil
		}
		prefix[i] = v
	}
	ords := si.ordinals
	dirtyKey = func(r Row) indexKey {
		cells := make([]Value, len(ords))
		for i, o := range ords {
			cells[i] = r[o]
		}
		return encodeCompositeKey(cells)
	}
	return si.snapshotPrefix(encodeCompositeKey(prefix)), dirtyKey, pl.compResidual, true, nil
}

func (db *DB) execSelectCompositeWalk(pl *plan, args []Value) ([]string, []Row, error) {
	snap, dirtyKey, residual, ok, err := db.compositeWalkArgs(pl, args)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return pl.colNames, nil, nil
	}
	return db.orderedWalk(pl, args, snap, dirtyKey, residual)
}

// execSelectOrderedWalk serves an ORDER BY on an ordered-indexed column by
// walking the sorted index in order, merged with the dirty overlay (rows
// mutated since the last merge), applying any residual WHERE, and stopping at
// LIMIT — touching ~LIMIT rows, not the whole table. A non-dirty index entry is
// fresh (its key equals the live value), so the index key drives the ordering
// and the row is fetched only when selected. Dirty PKs are excluded from the
// index walk (the entry may be stale) and supplied from their live rows.

// dcand is one dirty-overlay candidate for an ordered walk: a row mutated since
// the last merge, tagged with its sort key.
type dcand struct {
	key indexKey
	row Row
}

// buildDirtyCands returns the dirty rows that pass match, each tagged with its
// sort key (keyFn) and sorted ascending, plus dirtySet — ALL dirty PKs, used to
// shadow possibly-stale index entries during the merge. Shared by every ordered
// walk (single-column, composite, join probe).
func (tbl *tableRT) buildDirtyCands(match func(Row) bool, keyFn func(Row) indexKey) ([]dcand, map[UUID]struct{}) {
	dirty := tbl.dirtyPKs()
	dirtySet := make(map[UUID]struct{}, len(dirty))
	var dc []dcand
	for _, pk := range dirty {
		if _, dup := dirtySet[pk]; dup {
			continue
		}
		dirtySet[pk] = struct{}{}
		if r, ok := tbl.getByPK(pk); ok && match(r) {
			dc = append(dc, dcand{keyFn(r), r})
		}
	}
	sort.Slice(dc, func(i, j int) bool { return dc[i].key.less(dc[j].key) })
	return dc, dirtySet
}

// mergeOrderedStreams walks the sorted index slice snap and the pre-matched dirty
// candidates dc in the requested order (desc reverses both), skipping snap entries
// shadowed by dirtySet, and calls emitIdx(pk) for each surviving index entry and
// emitDirty(row) for each dirty row — in one merged ORDER BY order. It stops when
// done() reports enough collected, or both streams are exhausted. The consumer
// owns fetching/filtering/counting (so done() reflects rows actually kept).
func mergeOrderedStreams(snap []ordEntry, dc []dcand, dirtySet map[UUID]struct{}, desc bool, done func() bool, emitIdx func(pk UUID), emitDirty func(row Row)) {
	before := func(a, b indexKey) bool { // a comes before b in the requested order
		if desc {
			return b.less(a)
		}
		return a.less(b)
	}
	ii, dj := 0, 0
	if desc {
		ii, dj = len(snap)-1, len(dc)-1
	}
	idxOK := func() bool { return ii >= 0 && ii < len(snap) }
	dcOK := func() bool { return dj >= 0 && dj < len(dc) }
	step := func(p *int) {
		if desc {
			*p--
		} else {
			*p++
		}
	}
	for !done() {
		for idxOK() { // skip index entries shadowed by the dirty overlay
			if _, d := dirtySet[snap[ii].pk]; !d {
				break
			}
			step(&ii)
		}
		switch {
		case idxOK() && dcOK():
			if before(dc[dj].key, snap[ii].key) {
				emitDirty(dc[dj].row)
				step(&dj)
			} else {
				emitIdx(snap[ii].pk)
				step(&ii)
			}
		case dcOK():
			emitDirty(dc[dj].row)
			step(&dj)
		case idxOK():
			emitIdx(snap[ii].pk)
			step(&ii)
		default:
			return // both streams exhausted
		}
	}
}

// orderedWalkArgs resolves the (snap, dirtyKey, residual) inputs orderedWalk
// needs for a single-column ordered walk. ok=false means a missing index. The
// index sits on the ORDER BY column, not the WHERE columns, so the snap
// guarantees nothing about the WHERE: the whole WHERE is residual. Shared by the
// materializing and streaming entry points.
func (db *DB) orderedWalkArgs(pl *plan) (snap []ordEntry, dirtyKey func(Row) indexKey, residual []expr, ok bool) {
	si := pl.rt.indexFor(pl.orderOrdinal)
	if si == nil {
		return nil, nil, nil, false
	}
	ord := pl.orderOrdinal
	collectConjuncts(pl.st.(*selectStmt).where, &residual)
	return si.snapshot(), func(r Row) indexKey { return keyOf(r[ord]) }, residual, true
}

func (db *DB) execSelectOrderedWalk(pl *plan, args []Value) ([]string, []Row, error) {
	snap, dirtyKey, residual, ok := db.orderedWalkArgs(pl)
	if !ok {
		return pl.colNames, nil, nil
	}
	return db.orderedWalk(pl, args, snap, dirtyKey, residual)
}

// orderedWalk merges a sorted index slice (snap, ascending key order) with the
// dirty overlay and emits rows in ORDER BY order, stopping at LIMIT — touching
// ~LIMIT rows, not the whole set. dirtyKey maps a dirty row to its sort key in
// snap's key space (single-column: keyOf(orderOrdinal); composite: the encoded
// tuple), so both streams merge on one comparator. A non-dirty snap entry is
// fresh, so its key drives the ordering and the row is fetched only when
// selected; dirty PKs are excluded from the snap walk (their entry may be stale)
// and supplied from their live rows.
//
// residual is the WHERE conjuncts NOT already guaranteed by the snap (for a
// single-column walk that is the whole WHERE; for a composite prefix walk it
// excludes the pinned equalities the sub-range already enforces). An empty
// residual means index rows need no per-row check, so they take the one-clone
// getByPKProject fast path. Dirty rows are always filtered by the FULL WHERE
// (buildDirtyCands' match) — the snap guarantees nothing about an unmerged row.
func (db *DB) orderedWalk(pl *plan, args []Value, snap []ordEntry, dirtyKey func(Row) indexKey, residual []expr) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	if st.limit == 0 {
		return colNames, nil, nil
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	matches := func(r Row) bool { // full WHERE — filters dirty candidates
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, &ctx)
		return err == nil && truthy(v)
	}
	passResidual := func(r Row) bool { // only the conjuncts the snap doesn't guarantee
		for _, p := range residual {
			ctx.row = r
			v, err := evalExpr(p, &ctx)
			if err != nil || !truthy(v) {
				return false
			}
		}
		return true
	}
	dc, dirtySet := tbl.buildDirtyCands(matches, dirtyKey)

	eff := fetchBound(st.limit, st.offset) // collect offset+limit, drop offset at the end
	capHint := eff
	if capHint < 0 || capHint > 1024 {
		capHint = 1024
	}
	out := make([]Row, 0, capHint)
	done := func() bool { return eff >= 0 && len(out) >= eff }
	emitIdx := func(pk UUID) {
		if len(residual) == 0 { // snap fully satisfies the filter: one clone, projected
			if st.starAll {
				if r, ok := tbl.getByPK(pk); ok {
					out = append(out, r)
				}
			} else if pr, ok := tbl.getByPKProject(pk, pl.projOrdinals); ok {
				out = append(out, pr)
			}
			return
		}
		if r, ok := tbl.getByPK(pk); ok && passResidual(r) { // residual needs the full row
			if st.starAll {
				out = append(out, r)
			} else {
				out = append(out, projectClone(r, pl.projOrdinals))
			}
		}
	}
	emitDirty := func(row Row) { // dc rows are owned clones, already matched
		if st.starAll {
			out = append(out, row)
		} else {
			out = append(out, projectClone(row, pl.projOrdinals))
		}
	}
	mergeOrderedStreams(snap, dc, dirtySet, st.orderDesc, done, emitIdx, emitDirty)
	return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
}

// orderedWalkEach is the streaming form of orderedWalk: it drives the SAME
// snap+dirty merge in the SAME ORDER BY order (mergeOrderedStreams + the same
// residual/dirty-shadow semantics), but instead of materializing a []Row with a
// per-row clone it visits each selected row in place. Index rows are visited
// under their shard RLock (valid only for that call, per the streaming
// memory-safety contract); dirty rows are visited from their owned clone. OFFSET
// and LIMIT are applied as rows are visited — a stream cannot post-slice. visit
// returns false to stop early. Ordered walks only exist on non-partitioned
// tables (ordered indexes are barred on partitioned ones), so the live row is
// reached via the per-shard pk map, as the idxLookup stream does.
func (db *DB) orderedWalkEach(pl *plan, args []Value, snap []ordEntry, dirtyKey func(Row) indexKey, residual []expr, visit func(row Row) bool) error {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	if st.limit == 0 {
		return nil
	}
	ctx := evalCtx{cols: tbl.def.colByName, args: args}
	matches := func(r Row) bool { // full WHERE — filters dirty candidates
		if st.where == nil {
			return true
		}
		ctx.row = r
		v, err := evalExpr(st.where, &ctx)
		return err == nil && truthy(v)
	}
	passResidual := func(r Row) bool { // only the conjuncts the snap doesn't guarantee
		for _, p := range residual {
			ctx.row = r
			v, err := evalExpr(p, &ctx)
			if err != nil || !truthy(v) {
				return false
			}
		}
		return true
	}
	dc, dirtySet := tbl.buildDirtyCands(matches, dirtyKey)

	var scratch Row
	if !st.starAll {
		scratch = make(Row, len(pl.projOrdinals))
	}
	skipped, n, stop := 0, 0, false
	// deliver applies OFFSET (skip the first offset qualified rows) and LIMIT to
	// one qualified row, projecting into the reused scratch (Value headers only,
	// valid for this call) and visiting it. It sets stop on early-exit or once
	// LIMIT is reached; mergeOrderedStreams' done() then halts the walk.
	deliver := func(r Row) {
		if skipped < st.offset {
			skipped++
			return
		}
		row := r
		if !st.starAll {
			for j, ord := range pl.projOrdinals {
				scratch[j] = r[ord]
			}
			row = scratch
		}
		if !visit(row) {
			stop = true
			return
		}
		n++
		if st.limit >= 0 && n >= st.limit {
			stop = true
		}
	}
	emitIdx := func(pk UUID) { // fresh index entry: visit the live row under its shard lock
		s := tbl.shardOf(pk)
		s.mu.RLock()
		if rowID, ok := s.pk[pk]; ok {
			if r := s.rows[rowID]; r != nil && (len(residual) == 0 || passResidual(r)) {
				deliver(r)
			}
		}
		s.mu.RUnlock()
	}
	emitDirty := func(row Row) { deliver(row) } // owned clone, already matched
	mergeOrderedStreams(snap, dc, dirtySet, st.orderDesc, func() bool { return stop }, emitIdx, emitDirty)
	return nil
}

// execSelectOrderedWalkEach / execSelectCompositeWalkEach are the streaming
// counterparts of execSelectOrderedWalk / execSelectCompositeWalk: same walk
// inputs, but visiting rows instead of returning a []Row.
func (db *DB) execSelectOrderedWalkEach(pl *plan, args []Value, visit func(row Row) bool) error {
	snap, dirtyKey, residual, ok := db.orderedWalkArgs(pl)
	if !ok {
		return nil
	}
	return db.orderedWalkEach(pl, args, snap, dirtyKey, residual, visit)
}

func (db *DB) execSelectCompositeWalkEach(pl *plan, args []Value, visit func(row Row) bool) error {
	snap, dirtyKey, residual, ok, err := db.compositeWalkArgs(pl, args)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return db.orderedWalkEach(pl, args, snap, dirtyKey, residual, visit)
}

// fetchBound is the number of in-order matches a LIMIT/OFFSET query collects
// before discarding the first offset: offset+limit, or -1 (unbounded) when there
// is no LIMIT. Early-stopping paths fetch this many (and the top-N heap keeps
// this many); sliceOffsetLimit then drops the offset.
func fetchBound(limit, offset int) int {
	if limit < 0 {
		return -1
	}
	return limit + offset
}

// sliceOffsetLimit returns the rows[offset : offset+limit] window: it drops the
// first offset rows (offset<=0 is a no-op) then caps the rest at limit (limit<0
// means to the end). For a path that already fetched exactly offset+limit rows
// the cap is a no-op; the gather-all paths rely on it to apply both bounds.
func sliceOffsetLimit(rows []Row, offset, limit int) []Row {
	if offset > 0 {
		if offset >= len(rows) {
			return nil
		}
		rows = rows[offset:]
	}
	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

// execSelect runs the SELECT plan. Returns the columns and a slice of
// projected rows. Rows are deep-cloned before returning so the caller
// may mutate them without affecting storage.
func (db *DB) execSelect(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt

	colNames := pl.colNames

	// Two-table join: indexed nested-loop, its own executor.
	if pl.joinPlan != nil {
		return db.execJoin(pl, args)
	}

	ctx := evalCtx{cols: tbl.def.colByName, args: args}

	// Fast path: PK equality — single map lookup, no scan, no sort.
	// Project directly into the result row to skip the matched-list
	// allocation and the full-row clone.
	if pl.pkLookup {
		// A PK match is at most one row, so LIMIT 0 or any OFFSET drops it.
		if st.limit == 0 || st.offset > 0 {
			return colNames, nil, nil
		}
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

	// Composite ordered walk: WHERE pins a prefix, ORDER BY the trailing column —
	// walk the prefix sub-range in order, no sort.
	if pl.compWalk {
		return db.execSelectCompositeWalk(pl, args)
	}

	// Composite prefix lookup: WHERE pins a leading prefix of a composite index.
	if pl.compLookup {
		return db.execSelectCompositeLookup(pl, &ctx)
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
		// Collect offset+limit rows during the scan, then drop the offset. Cap the
		// prealloc: a huge bound over a small result must not allocate a giant
		// slice up front; append grows past the hint only if that many match.
		eff := fetchBound(st.limit, st.offset)
		capHint := eff
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
				if len(out) >= eff {
					break
				}
			}
			s.mu.RUnlock()
			return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
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
				if len(out) >= eff {
					stop = true
					break
				}
			}
			s.mu.RUnlock()
			if stop {
				break
			}
		}
		return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
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
		// Keep offset+limit rows so the offset can be dropped after sorting.
		top := topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: fetchBound(st.limit, st.offset)}
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
		kept := sliceOffsetLimit(top.sorted(), st.offset, st.limit)
		return colNames, projectRows(kept, st.starAll, pl.projOrdinals), nil
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
	matched = sliceOffsetLimit(matched, st.offset, st.limit)
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
	// Secondary-index WHERE: update only the index candidates (∪ dirty), re-checked
	// against the full WHERE, instead of scanning every row. Under a heavy write
	// burst the dirty overlay can outgrow the table, making the candidate walk
	// cost more than a scan — dirtyTooDenseForScan falls through to updateWhereAll.
	if pl.idxLookup && !tbl.dirtyTooDenseForScan() {
		cand, ok, err := db.idxCandidates(pl, ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		var pks []UUID
		cand.emit(func(pk UUID) bool { pks = append(pks, pk); return false })
		// Fast path — one candidate (the common unique/steady-state index update)
		// + single-column SET: update in place, no full-row clone (the bulk of the
		// cost), mirroring the PK single-column path.
		if len(pks) == 1 && len(pl.updateOrdinals) == 1 {
			ord := pl.updateOrdinals[0]
			computeOne := func(r Row) (Value, error) {
				v, cerr := compute(r)
				if cerr != nil {
					return Value{}, cerr
				}
				return v[0], nil
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
			return tbl.updateOneByCandidate(pks[0], ord, match, computeOne, journal)
		}
		return tbl.updateByCandidates(pks, match, pl.updateOrdinals, compute, encode, journalAll)
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
	// Secondary-index WHERE: delete only the index candidates (∪ dirty), re-checked
	// against the full WHERE, instead of scanning every row. Under a heavy write
	// burst the dirty overlay can outgrow the table, making the candidate walk
	// cost more than a scan — dirtyTooDenseForScan falls through to deleteWhereAll.
	if pl.idxLookup && !tbl.dirtyTooDenseForScan() {
		cand, ok, err := db.idxCandidates(pl, ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, nil
		}
		var pks []UUID
		cand.emit(func(pk UUID) bool { pks = append(pks, pk); return false })
		return tbl.deleteByCandidates(pks, match, encode, journalAll)
	}
	return tbl.deleteWhereAll(match, encode, journalAll)
}
