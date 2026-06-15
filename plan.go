package hazedb

import "fmt"

// rejectValueLiterals enforces the parameterize-everything contract: a value in a
// WHERE/SET/VALUES position must be a ? placeholder, never an inline literal. This
// bounds the plan cache (only finite parameterized shapes are ever cached) and
// removes the SQL-injection path — a value never reaches the parser as syntax.
// LIMIT/OFFSET/ORDER BY are not value literals (separate stmt fields) and IS NULL
// is its own node, so those are unaffected; only litValue nodes are rejected.
func rejectValueLiterals(st stmt) error {
	bad := false
	var walk func(expr)
	walk = func(e expr) {
		switch x := e.(type) {
		case *litValue:
			bad = true
		case *binOp:
			walk(x.lhs)
			walk(x.rhs)
		case *notExpr:
			walk(x.e)
		case *isNullExpr:
			walk(x.e)
		}
	}
	switch s := st.(type) {
	case *selectStmt:
		walk(s.where)
	case *insertStmt:
		for _, tuple := range s.rows {
			for _, e := range tuple {
				walk(e)
			}
		}
	case *updateStmt:
		for _, set := range s.sets {
			walk(set.val)
		}
		walk(s.where)
	case *deleteStmt:
		walk(s.where)
	}
	if bad {
		return fmt.Errorf("%w: inline value literals are not allowed — use ? placeholders for all values", ErrParse)
	}
	return nil
}

// assignParamIndices walks the AST and replaces every paramRef.index = -1 with a
// running count, so positional args bind in source order. Called once after parse,
// before plan. Returns the parameter count.
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

// insCell is one compiled INSERT VALUES entry. Two shapes, distinguished by arg:
// a param reads args[arg] at exec; anything else (arg == insCellExpr, e.g. ? + ?)
// falls back to evalExpr on expr. Inline value literals are rejected before
// planning, so a literal never reaches here.
type insCell struct {
	ord  int  // target column ordinal
	arg  int  // >=0: param index; insCellExpr: expr
	expr expr // arg == insCellExpr: fallback expression
}

const insCellExpr = -2

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
	// from assignParamIndices. Exec/Query reject an arg count that does not match
	// it: extra args would otherwise be silently ignored, and too few would overrun
	// a per-param bounds check.
	nparams int
	// paramWantUUID[i] marks parameter i as bound to a UUID column — compared to it
	// in WHERE or assigned to it in SET — so coerceParams parses its string arg into
	// a UUID before execution. The text/JSON/PHP arg surfaces no longer guess UUIDs
	// by string shape; the column type decides, recorded here at plan time where it
	// is known. nil when no parameter targets a UUID column. See bindParamUUIDCoercion.
	paramWantUUID []bool
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
	// cell per provided column. Built once at plan time so buildRowFromTmpl binds
	// params by arg index and only falls back to evalExpr for compound expressions
	// (? + ?). Read-only and shared across concurrent callers.
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

	// pkProbe pins the PK fast path for a WHERE that constrains the PK by equality
	// inside an AND-chain but is NOT a bare PK equality (so detectPKEq missed it)
	// and has no index/partition/order path — i.e. it would otherwise full-scan. A
	// PK equality bounds the result to <=1 row, so the executor fetches that row by
	// PK and re-checks the FULL WHERE on it (residual conjuncts honoured) instead
	// of scanning. pkProbe holds the PK value source. Distinct from pkLookup so the
	// alloc-free PK/JSON/stream fast paths (which never re-check a residual) keep
	// firing only on a bare PK equality.
	pkProbe expr

	// countStar is true for SELECT COUNT(*): the WHERE planning below still runs
	// (pkLookup / idxLookup / partLookup / scan), but the executor counts matches
	// instead of projecting rows. colNames is ["count"].
	countStar bool
	// countIdxBucket is set when countStar's WHERE is exactly one single-column
	// indexed equality (col = ?): the executor returns the merged index bucket size
	// directly, touching no rows. The count is as of the last index merge (bounded
	// staleness ~50ms); see countRows.
	countIdxBucket bool
	pkSource       expr

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
	// idxExact is true when the WHERE is EXACTLY the indexed equalities (no residual
	// conjunct). A fresh index hit then needs no per-row re-check: when the dirty
	// overlay is empty the bucket is authoritative for those columns, and a deleted
	// row is dropped by the row fetch. The executor skips the matcher in that case.
	idxExact bool

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
		if s.countStar {
			// COUNT(*): one int result, no row projection. The WHERE planning below
			// still runs; the executor counts matches instead of projecting.
			pl.countStar = true
			pl.colNames = []string{"count"}
		} else {
			if !s.starAll {
				pl.projOrdinals = make([]int, 0, len(s.cols))
				for _, c := range s.cols {
					if !qualifierMatches(c.qual, tname, s.alias) {
						return nil, fmt.Errorf("%w: %q.%q", ErrUnknownColumn, c.qual, c.col)
					}
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
		}
		// Pre-escape each column's "col": object-key fragment once, so the
		// per-row JSON encoder appends bytes instead of re-escaping a constant.
		pl.colJSONPrefix = make([][]byte, len(pl.colNames))
		for i, name := range pl.colNames {
			pl.colJSONPrefix[i] = append(appendJSONString(nil, name), ':')
		}
		if s.orderCol != "" {
			if !qualifierMatches(s.orderQual, tname, s.alias) {
				return nil, fmt.Errorf("%w: %q.%q (ORDER BY)", ErrUnknownColumn, s.orderQual, s.orderCol)
			}
			ord, ok := rt.colByName[s.orderCol]
			if !ok {
				return nil, fmt.Errorf("%w: %q.%q (ORDER BY)", ErrUnknownColumn, tname, s.orderCol)
			}
			pl.orderOrdinal = ord
		}
		if err := validateExpr(s.where, rt, s.alias); err != nil {
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
				// Secondary-index selection. A composite ordered WALK serves
				// WHERE-prefix + ORDER BY-trailing without a sort (and stops early at
				// LIMIT), so it beats a single-column index lookup — which resolves
				// candidates by one column's bucket and then SORTS them for the ORDER
				// BY (the filtered-list pattern, WHERE author = ? ORDER BY date). Try
				// the walk first; only when none applies collect single-column indexed
				// equalities (intersect their buckets, residual-filter the full WHERE),
				// then fall back to a composite prefix lookup. No indexed equality and
				// no walk → scan.
				eqs := map[int]expr{}
				collectEqConjuncts(s.where, eqs)
				if !rt.planCompositeWalk(pl, s.where, eqs, pl.orderOrdinal) {
					for i := range rt.indexes {
						ri := &rt.indexes[i]
						if len(ri.ordinals) != 1 {
							continue // composite indexes handled by the walk/lookup helpers
						}
						ord := ri.ordinals[0]
						if src, has := eqs[ord]; has {
							pl.idxCols = append(pl.idxCols, ord)
							pl.idxSrcs = append(pl.idxSrcs, src)
						}
					}
					pl.idxLookup = len(pl.idxCols) > 0
					if pl.idxLookup {
						// Exact when every top-level WHERE conjunct is one of the indexed
						// equalities — i.e. no residual to re-check on a fresh index hit.
						var conj []expr
						collectConjuncts(s.where, &conj)
						pl.idxExact = len(conj) == len(pl.idxCols)
					} else {
						rt.planCompositeLookup(pl, eqs)
					}
				}
				// COUNT(*) WHERE col = ? on a single indexed column, and the WHERE is
				// exactly that equality (a bare tkEq, no residual conjuncts): the index
				// bucket size is the answer when the index is clean. See countRows.
				if pl.countStar && pl.idxLookup && len(pl.idxCols) == 1 {
					if be, ok := s.where.(*binOp); ok && be.op == tkEq {
						pl.countIdxBucket = true
					}
				}
			}
		}
		// ORDER BY on an ordered-indexed column, with no equality index chosen
		// (applies with or without a WHERE): walk the sorted index in order
		// instead of scanning + sorting. Correctness guard (same as the composite
		// path's anyNullable): an ordered index excludes NULL values, so a nullable
		// ORDER BY column would drop its NULL rows from the walk — fall back to
		// scan+sort, which sees every row.
		if s.orderCol != "" && !pl.idxLookup && !pl.compLookup && !pl.compWalk && rt.orderedIndexOn(pl.orderOrdinal) && !rt.def.Columns[pl.orderOrdinal].Nullable {
			pl.orderWalk = true
		}
		// PK pinned inside an AND-chain with no cheaper path chosen: probe by PK
		// and re-check the full WHERE instead of scanning. After every strategy
		// above so it fills only the would-otherwise-scan gap.
		rt.planPKProbe(pl, s.where)
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
		// arg index, anything else (e.g. ? + ?) falls back to evalExpr. Inline
		// literals are rejected before planning, so none reach here. Single-row
		// INSERT is the len-1 case; multi-row gets one template per tuple.
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
				cell := insCell{ord: ord, arg: insCellExpr, expr: tuple[i]}
				if v, ok := tuple[i].(*paramRef); ok {
					cell.arg, cell.expr = v.index, nil
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
			if err := validateExpr(a.val, rt, ""); err != nil {
				return nil, err
			}
			if exprRefsColumn(a.val) {
				pl.setRowDependent = true
			}
		}
		if err := validateExpr(s.where, rt, ""); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			} else {
				rt.planIndexEq(pl, s.where) // secondary-index candidates instead of a scan
				rt.planPKProbe(pl, s.where) // PK pinned in an AND-chain with no index path
			}
		}
	case *deleteStmt:
		if err := validateExpr(s.where, rt, ""); err != nil {
			return nil, err
		}
		if s.where != nil {
			if ok, src := detectPKEq(s.where, rt); ok {
				pl.pkLookup = true
				pl.pkSource = src
			} else {
				rt.planIndexEq(pl, s.where) // secondary-index candidates instead of a scan
				rt.planPKProbe(pl, s.where) // PK pinned in an AND-chain with no index path
			}
		}
	}
	return pl, nil
}

// detectColEq returns (true, valueSide) when e is a single binOp = between the
// named column (by ordinal) and a literal/parameter. It matches the bare equality
// only; an equality inside an AND-chain is found by collectEqConjuncts instead.
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

// planPKProbe fills the one gap detectPKEq leaves: a WHERE that pins the PK by
// equality inside an AND-chain (WHERE id = ? AND age = ?) rather than as the sole
// equality. It activates only when no cheaper-than-scan path was already chosen
// (bare pkLookup / index / partition), so it strictly replaces a full scan with a
// single getByPK plus a residual full-WHERE re-check. Must run after those paths
// are decided.
func (rt *resolvedTable) planPKProbe(pl *plan, where expr) {
	if where == nil || pl.pkLookup || pl.idxLookup || pl.partLookup || pl.compLookup || pl.compWalk || pl.orderWalk {
		return
	}
	// Non-partitioned only: the write candidate paths (updateByCandidates etc.)
	// locate rows by t.shardOf(pk), which is not how a partitioned table routes a
	// bare PK (pkDir). Gating here keeps SELECT/UPDATE/DELETE consistent — a
	// partitioned table takes this shape via the scan, as before.
	if rt.partitioned() {
		return
	}
	eqs := map[int]expr{}
	collectEqConjuncts(where, eqs)
	if src, ok := eqs[rt.pkOrdinal]; ok {
		pl.pkProbe = src
	}
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

// compositePrefix returns the length of the contiguous leading prefix of ri's
// columns the WHERE pins by equality (eqs maps a pinned ordinal to its value
// expr). It returns 0 — unusable — unless ri is a composite ordered index whose
// every component is NOT NULL and whose leading column is pinned. The NOT-NULL
// guard is a correctness rule: a composite index excludes any row with a NULL in
// any component, so a (a=X, b=NULL) row would match WHERE a=? yet be absent from
// the index — nullable-component composites must fall back to scan.
func (rt *resolvedTable) compositePrefix(ri *resolvedIndex, eqs map[int]expr) int {
	if len(ri.ordinals) < 2 || !ri.ordered || rt.anyNullable(ri.ordinals) {
		return 0
	}
	k := 0
	for k < len(ri.ordinals) {
		if _, has := eqs[ri.ordinals[k]]; !has {
			break
		}
		k++
	}
	return k
}

// prefixSrcs returns the value exprs for ri's first k pinned columns, in order.
func prefixSrcs(ri *resolvedIndex, eqs map[int]expr, k int) []expr {
	srcs := make([]expr, k)
	for j := 0; j < k; j++ {
		srcs[j] = eqs[ri.ordinals[j]]
	}
	return srcs
}

// planCompositeWalk picks a composite ordered index that serves the ORDER BY by
// walking it in order: the WHERE pins a leading prefix and the ORDER BY column is
// the next column, so the prefix sub-range is already sorted — no sort, and a
// LIMIT stops the walk early. Preferred over a single-column index lookup, which
// would sort the whole candidate set. Scans every index and takes the first that
// yields such a walk; returns whether one did.
func (rt *resolvedTable) planCompositeWalk(pl *plan, where expr, eqs map[int]expr, orderOrdinal int) bool {
	if orderOrdinal < 0 {
		return false
	}
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		k := rt.compositePrefix(ri, eqs)
		if k == 0 || k >= len(ri.ordinals) || ri.ordinals[k] != orderOrdinal {
			continue
		}
		pl.compOrdinals = ri.ordinals
		pl.compPrefixSrcs = prefixSrcs(ri, eqs, k)
		pl.compWalk = true
		// Residual = WHERE conjuncts the prefix does not already guarantee. Drop
		// only the EXACT pin equalities (by value-expr identity), so a second
		// constraint on a pinned column survives and stays correct.
		pinned := make(map[int]bool, k)
		for j := 0; j < k; j++ {
			pinned[ri.ordinals[j]] = true
		}
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
		return true
	}
	return false
}

// planCompositeLookup picks a composite ordered index whose leading prefix the
// WHERE pins by equality, resolving candidates through that prefix instead of
// scanning. The fallback when no sort-avoiding walk and no single-column index
// applies. First usable index wins; returns whether one did.
func (rt *resolvedTable) planCompositeLookup(pl *plan, eqs map[int]expr) bool {
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		k := rt.compositePrefix(ri, eqs)
		if k == 0 {
			continue
		}
		pl.compOrdinals = ri.ordinals
		pl.compPrefixSrcs = prefixSrcs(ri, eqs, k)
		pl.compLookup = true
		return true
	}
	return false
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

// bindParamUUIDCoercion records, per parameter, whether its value must be coerced
// to a UUID before execution — i.e. the parameter is compared to a UUID column in
// WHERE or assigned to one in SET. String LITERALS in those same positions are
// rewritten to UUIDs in place (once; the plan is cached, and inline literals reach
// only a trusted seed script — the normal path bans them). Parameters and literals
// anywhere else are left untouched. Called once per plan, after the WHERE colRefs
// are bound (validateExpr / planJoin) and nparams is set.
func (pl *plan) bindParamUUIDCoercion() {
	// isUUIDCol reports whether a colRef names a UUID column. Join: ord is a bound
	// global concat ordinal into left||right. Single-table: the bound ordinal, or a
	// name lookup if it is unbound.
	isUUIDCol := func(cr *colRef) bool {
		if jp := pl.joinPlan; jp != nil {
			ord := cr.ord
			if ord < 0 {
				return false
			}
			if ord < jp.nLeft {
				cols := jp.leftRT.def.def.Columns
				return ord < len(cols) && cols[ord].Type == TypeUUID
			}
			cols := jp.rightRT.def.def.Columns
			r := ord - jp.nLeft
			return r < len(cols) && cols[r].Type == TypeUUID
		}
		ord := cr.ord
		if ord < 0 {
			var ok bool
			if ord, ok = pl.table.colByName[cr.name]; !ok {
				return false
			}
		}
		cols := pl.table.def.Columns
		return ord >= 0 && ord < len(cols) && cols[ord].Type == TypeUUID
	}
	// markValue marks the param (or rewrites a string literal) when its target column
	// is UUID.
	markValue := func(val expr) {
		switch v := val.(type) {
		case *paramRef:
			if v.index >= 0 && v.index < len(pl.paramWantUUID) {
				pl.paramWantUUID[v.index] = true
			}
		case *litValue:
			if v.v.Kind == KindString {
				if u, err := ParseUUID(v.v.Str()); err == nil {
					v.v = UUIDVal(u) // a non-UUID literal stays a string; compare/validate handles it
				}
			}
		}
	}
	// walkWhere visits each `col OP value` comparison (either operand order) under
	// AND/OR/NOT; a param inside any other expression has no single column type and
	// is left untyped.
	var walkWhere func(e expr)
	walkWhere = func(e expr) {
		switch x := e.(type) {
		case *binOp:
			switch x.op {
			case tkAnd, tkOr:
				walkWhere(x.lhs)
				walkWhere(x.rhs)
			case tkEq, tkNeq, tkLt, tkLte, tkGt, tkGte:
				if cr, ok := x.lhs.(*colRef); ok && isUUIDCol(cr) {
					markValue(x.rhs)
				}
				if cr, ok := x.rhs.(*colRef); ok && isUUIDCol(cr) {
					markValue(x.lhs)
				}
			}
		case *notExpr:
			walkWhere(x.e)
		}
	}

	if pl.nparams > 0 {
		pl.paramWantUUID = make([]bool, pl.nparams)
	}
	switch s := pl.st.(type) {
	case *selectStmt:
		walkWhere(s.where)
	case *deleteStmt:
		walkWhere(s.where)
	case *updateStmt:
		walkWhere(s.where)
		cols := pl.table.def.Columns
		for i := range s.sets {
			if ord, ok := pl.table.colByName[s.sets[i].col]; ok && ord < len(cols) && cols[ord].Type == TypeUUID {
				markValue(s.sets[i].val)
			}
		}
	}
	// Drop the slice when no parameter targets a UUID column, so coerceParams' empty
	// fast path fires and a plan with no UUID params carries no per-plan baggage.
	allFalse := true
	for _, w := range pl.paramWantUUID {
		if w {
			allFalse = false
			break
		}
	}
	if allFalse {
		pl.paramWantUUID = nil
	}
}

// coerceParams parses any string arg bound to a UUID column into a UUID, once,
// before execution — the column type (recorded in pl.paramWantUUID at plan time)
// decides, so a string destined for a non-UUID column stays a string. Mutates the
// caller's private args copy in place. A string that is not a valid UUID against a
// UUID column is a type error, consistent with the PK (coerceToUUID) and INSERT
// (buildRowFromTmpl) paths. A non-string arg (e.g. NULL) is left as-is. Ranges over
// nil when no param needs it — a no-op.
func coerceParams(pl *plan, args []Value) error {
	for i, want := range pl.paramWantUUID {
		if !want || i >= len(args) || args[i].Kind != KindString {
			continue
		}
		u, err := ParseUUID(args[i].Str())
		if err != nil {
			return fmt.Errorf("%w: argument %d for a UUID column is not a valid UUID", ErrTypeMismatch, i)
		}
		args[i] = UUIDVal(u)
	}
	return nil
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

// qualifierMatches reports whether a single-table column qualifier is one this
// FROM clause exposes: empty (unqualified), the table name, or its alias. Any
// other qualifier names a table not in the query (SELECT bogus.col FROM users),
// which is an error rather than a silently-ignored prefix.
func qualifierMatches(qual, table, alias string) bool {
	return qual == "" || qual == table || (alias != "" && qual == alias)
}

func validateExpr(e expr, rt *resolvedTable, alias string) error {
	if e == nil {
		return nil
	}
	switch x := e.(type) {
	case *colRef:
		if !qualifierMatches(x.qual, rt.def.Name, alias) {
			return fmt.Errorf("%w: %q.%q", ErrUnknownColumn, x.qual, x.name)
		}
		ord, ok := rt.colByName[x.name]
		if !ok {
			return fmt.Errorf("%w: %q.%q", ErrUnknownColumn, rt.def.Name, x.name)
		}
		x.ord = ord // bind once at plan time; evalExpr indexes by ord
	case *binOp:
		if err := validateExpr(x.lhs, rt, alias); err != nil {
			return err
		}
		if err := validateExpr(x.rhs, rt, alias); err != nil {
			return err
		}
	case *notExpr:
		return validateExpr(x.e, rt, alias)
	case *isNullExpr:
		return validateExpr(x.e, rt, alias)
	}
	return nil
}
