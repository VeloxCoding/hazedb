package hazedb

import "fmt"

// assignParamIndices walks the AST and replaces every paramRef.index
// = -1 with a running count, so positional args bind in source order.
// Called once after parse, before plan. Returns the parameter count.
func assignParamIndices(st stmt) int {
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
		for _, tuple := range s.rows {
			for i := range tuple {
				tuple[i] = walk(tuple[i])
			}
		}
	case *updateStmt:
		for i := range s.sets {
			s.sets[i].val = walk(s.sets[i].val)
		}
		s.where = walk(s.where)
	case *deleteStmt:
		s.where = walk(s.where)
	}
	return n
}

// insCell is one compiled INSERT VALUES entry. The three shapes are
// distinguished by arg: a param reads args[arg] at exec; a literal (arg ==
// insCellLit) is pre-validated and pre-coerced into lit at plan time; anything
// else (arg == insCellExpr, e.g. ? + 1) falls back to evalExpr on expr.
type insCell struct {
	ord  int   // target column ordinal
	arg  int   // >=0: param index; insCellLit: literal; insCellExpr: expr
	lit  Value // arg == insCellLit: pre-validated, pre-coerced literal
	expr expr  // arg == insCellExpr: fallback expression
}

const (
	insCellLit  = -1
	insCellExpr = -2
)

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
	// nparams is the number of positional ? parameters in the statement, set
	// from assignParamIndices. Exec/Query reject an arg count that does not
	// match it (too many were silently ignored before; too few panicked a
	// per-param bounds check).
	nparams int
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
	// insertTmpl is the compiled INSERT VALUES list: one cell template per
	// VALUES tuple (len 1 for single-row INSERT, N for multi-row), each with one
	// cell per provided column. Built once at plan time so buildRowFromTmpl
	// skips per-cell evalExpr dispatch for params and skips eval+validate
	// entirely for literals. Read-only and shared across concurrent callers.
	insertTmpl [][]insCell
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
		provided := make([]bool, len(rt.def.Columns))
		for _, c := range s.cols {
			ord, ok := rt.colByName[c]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (INSERT)", ErrUnknownColumn, tname, c)
			}
			if provided[ord] {
				return nil, fmt.Errorf("%w: column %q specified more than once in INSERT", ErrParse, c)
			}
			pl.insertOrdinals = append(pl.insertOrdinals, ord)
			provided[ord] = true
		}
		// Compile each VALUES tuple into its own cell template: params bind by
		// arg index, literals are validated + coerced once here (not per insert),
		// and anything else falls back to evalExpr. Single-row INSERT is the
		// len-1 case; multi-row gets one template per tuple.
		pl.insertTmpl = make([][]insCell, len(s.rows))
		for r, tuple := range s.rows {
			tmpl := make([]insCell, len(pl.insertOrdinals))
			for i, ord := range pl.insertOrdinals {
				// INSERT VALUES has no current row, so a column reference can
				// never resolve — and evalExpr would index a nil ctx.row and
				// panic the process. Reject any colRef (bare or inside an
				// expression like a+1) at plan time.
				if exprRefsColumn(tuple[i]) {
					return nil, fmt.Errorf("%w: column reference not allowed in INSERT VALUES", ErrParse)
				}
				col := rt.def.Columns[ord]
				cell := insCell{ord: ord, arg: insCellExpr, expr: tuple[i]}
				switch v := tuple[i].(type) {
				case *paramRef:
					cell.arg, cell.expr = v.index, nil
				case *litValue:
					lv := v.v
					if col.Type == TypeUUID && lv.Kind == KindString {
						u, perr := ParseUUID(lv.Str())
						if perr != nil {
							return nil, perr
						}
						lv = UUIDVal(u)
					}
					if err := validateValue(col, lv); err != nil {
						return nil, err
					}
					cell.arg, cell.lit, cell.expr = insCellLit, lv, nil
				}
				tmpl[i] = cell
			}
			pl.insertTmpl[r] = tmpl
		}
		// NOT NULL on omitted columns is a property of the statement, not the
		// args: a non-nullable column left out of the list can never be filled,
		// so reject the plan now rather than on every insert. The PK is exempt —
		// it is auto-generated when omitted.
		for ord := range rt.def.Columns {
			c := rt.def.Columns[ord]
			if !provided[ord] && ord != rt.pkOrdinal && !c.Nullable {
				return nil, fmt.Errorf("column %q is NOT NULL", c.Name)
			}
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
