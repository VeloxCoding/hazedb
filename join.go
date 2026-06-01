package hazedb

// Two-table joins (INNER / LEFT, single equi-join). The result row is the LEFT
// table's columns concatenated with the RIGHT table's (nLeft+nRight wide); every
// colRef in the projection, WHERE, and ORDER BY is bound at plan time to a
// GLOBAL ordinal into that concat row, so evalExpr runs unchanged on it.
//
// Execution is an indexed nested-loop join: scan the driving table, and for each
// row probe the other side through its PK map or a secondary index. The
// indexed-only law (enforced in planJoin) guarantees the probed side's join
// column is the PK or indexed, so a join is always O(driver) probes — never an
// O(A×B) scan. The driver is fully materialised first so no driver shard lock is
// held across the probe (which locks the other table) — avoiding cross-table
// lock cycles. A join is therefore per-shard-consistent, not point-in-time
// (the same contract as any multi-shard read).
//
// v1 scope: one JOIN (two tables), single `ON a.col = b.col` equality, INNER or
// LEFT. RIGHT/FULL/CROSS, N-way joins, and WHERE pushdown are deferred.

import "fmt"

// joinPlan is the resolved form of a two-table join. Set on plan.joinPlan;
// execSelect dispatches to execJoin when it is non-nil.
type joinPlan struct {
	leftRT, rightRT *tableRT
	typ             tokenKind // tkInner or tkLeft
	nLeft, nRight   int

	driverRT, probeRT *tableRT
	driverIsLeft      bool
	driverOnOrd       int // join-key ordinal within the driver table's row
	probeOnOrd        int // join-key ordinal within the probe table's row
	probeByPK         bool

	// Driver-side pushdown. driverPreds are the WHERE conjuncts that reference
	// only the driver table (bound to global ordinals); they pre-filter the
	// driver before the probe. When the driver carries an equality on an indexed
	// column (driverIdxOrd >= 0), the driver is fetched through that index/PK
	// (driverIdxSrc yields the key) instead of a full scan.
	driverPreds   []expr
	driverIdxOrd  int // within-driver ordinal of the indexed equality, -1 if none
	driverIdxSrc  expr
	driverIdxByPK bool

	orderOrdinal int // global ordinal of the ORDER BY column, -1 if none
}

// planJoin resolves the two tables, binds every column to a global concat-row
// ordinal, enforces the indexed-only law, and picks the drive side. Called from
// plan() when the SELECT carries a JOIN.
func (db *DB) planJoin(pl *plan, st *selectStmt, cat *catalog) error {
	if len(st.joins) != 1 {
		return fmt.Errorf("%w: only single two-table joins are supported (got %d JOINs)", ErrParse, len(st.joins))
	}
	jc := st.joins[0]
	if st.table == jc.table {
		return fmt.Errorf("%w: self-join (the same table twice) is not supported", ErrParse)
	}
	leftRT, ok := cat.byName[st.table]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownTable, st.table)
	}
	rightRT, ok := cat.byName[jc.table]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownTable, jc.table)
	}
	leftDef, rightDef := leftRT.def, rightRT.def
	nLeft, nRight := len(leftDef.def.Columns), len(rightDef.def.Columns)

	// A qualifier (table name or alias) resolves to a side; resolveCol maps a
	// (qual,name) to (side 0=left/1=right, within-table ordinal). Unqualified
	// names must be unambiguous across the two tables.
	leftQuals := map[string]bool{st.table: true}
	if st.alias != "" {
		leftQuals[st.alias] = true
	}
	rightQuals := map[string]bool{jc.table: true}
	if jc.alias != "" {
		rightQuals[jc.alias] = true
	}
	resolveCol := func(qual, name string) (side, within int, err error) {
		if qual != "" {
			inL, inR := leftQuals[qual], rightQuals[qual]
			if inL && inR {
				return 0, 0, fmt.Errorf("%w: ambiguous table qualifier %q", ErrParse, qual)
			}
			if inL {
				ord, ok := leftDef.colByName[name]
				if !ok {
					return 0, 0, fmt.Errorf("%w: %q.%q", ErrUnknownColumn, qual, name)
				}
				return 0, ord, nil
			}
			if inR {
				ord, ok := rightDef.colByName[name]
				if !ok {
					return 0, 0, fmt.Errorf("%w: %q.%q", ErrUnknownColumn, qual, name)
				}
				return 1, ord, nil
			}
			return 0, 0, fmt.Errorf("%w: unknown table qualifier %q", ErrParse, qual)
		}
		lo, inL := leftDef.colByName[name]
		ro, inR := rightDef.colByName[name]
		if inL && inR {
			return 0, 0, fmt.Errorf("%w: column %q is ambiguous; qualify it as table.%s", ErrParse, name, name)
		}
		if inL {
			return 0, lo, nil
		}
		if inR {
			return 1, ro, nil
		}
		return 0, 0, fmt.Errorf("%w: %q", ErrUnknownColumn, name)
	}
	globalOf := func(side, within int) int {
		if side == 0 {
			return within
		}
		return nLeft + within
	}
	resolveGlobal := func(qual, name string) (int, error) {
		s, w, err := resolveCol(qual, name)
		if err != nil {
			return -1, err
		}
		return globalOf(s, w), nil
	}

	// ON: the two sides must reference different tables.
	ls, lw, err := resolveCol(jc.lref.qual, jc.lref.name)
	if err != nil {
		return err
	}
	rs, rw, err := resolveCol(jc.rref.qual, jc.rref.name)
	if err != nil {
		return err
	}
	if ls == rs {
		return fmt.Errorf("%w: JOIN ... ON must compare one column from each table", ErrParse)
	}
	leftOnOrd, rightOnOrd := lw, rw
	if ls == 1 { // lref was the right table; swap so the names match the tables
		leftOnOrd, rightOnOrd = rw, lw
	}

	indexed := func(rt *tableRT, ord int) bool { return ord == rt.def.pkOrdinal || rt.indexFor(ord) != nil }
	leftIdx, rightIdx := indexed(leftRT, leftOnOrd), indexed(rightRT, rightOnOrd)

	// Bind every WHERE colRef to its global ordinal first, so the conjunct split
	// below can classify each by which table it touches (the same late-bind
	// validateExpr does for single-table plans).
	if err := bindJoinExpr(st.where, resolveGlobal); err != nil {
		return err
	}
	// Split the WHERE into per-table conjuncts so the filter on the driving side
	// can be pushed into its scan/lookup instead of applied after the join.
	var conj []expr
	collectConjuncts(st.where, &conj)
	var leftConj, rightConj []expr
	for _, c := range conj {
		switch {
		case exprOrdsWithin(c, 0, nLeft):
			leftConj = append(leftConj, c)
		case exprOrdsWithin(c, nLeft, nLeft+nRight):
			rightConj = append(rightConj, c)
		}
	}
	hasL, hasR := len(leftConj) > 0, len(rightConj) > 0

	jp := &joinPlan{leftRT: leftRT, rightRT: rightRT, typ: jc.typ, nLeft: nLeft, nRight: nRight, orderOrdinal: -1, driverIdxOrd: -1}
	driveLeft := func() {
		jp.driverIsLeft = true
		jp.driverRT, jp.probeRT = leftRT, rightRT
		jp.driverOnOrd, jp.probeOnOrd = leftOnOrd, rightOnOrd
		jp.probeByPK = rightOnOrd == rightDef.pkOrdinal
	}
	driveRight := func() {
		jp.driverIsLeft = false
		jp.driverRT, jp.probeRT = rightRT, leftRT
		jp.driverOnOrd, jp.probeOnOrd = rightOnOrd, leftOnOrd
		jp.probeByPK = leftOnOrd == leftDef.pkOrdinal
	}
	switch jc.typ {
	case tkLeft:
		// LEFT preserves the left rows → must drive left, probe right.
		if !rightIdx {
			return fmt.Errorf("%w: LEFT JOIN needs an index on the right join column %q.%q", ErrUnindexedJoin, jc.table, rightDef.def.Columns[rightOnOrd].Name)
		}
		driveLeft()
	case tkRight:
		// RIGHT preserves the right rows → must drive right, probe left.
		if !leftIdx {
			return fmt.Errorf("%w: RIGHT JOIN needs an index on the left join column %q.%q", ErrUnindexedJoin, st.table, leftDef.def.Columns[leftOnOrd].Name)
		}
		driveRight()
	default: // INNER drive-side, in priority order:
		//  1. drive the side carrying a WHERE filter (push it down) — selectivity
		//     is the biggest lever;
		//  2. else probe by PK when possible (a PK probe is a single map lookup —
		//     far cheaper per row than a secondary-index probe, which fetches a
		//     bucket + dirty overlay + re-check), so drive the non-PK side;
		//  3. else (both join cols secondary-indexed) drive the smaller table;
		//  4. else drive whichever side can be probed at all.
		leftJoinPK := leftOnOrd == leftDef.pkOrdinal
		rightJoinPK := rightOnOrd == rightDef.pkOrdinal
		switch {
		case hasL && !hasR && rightIdx:
			driveLeft()
		case hasR && !hasL && leftIdx:
			driveRight()
		case rightJoinPK:
			driveLeft() // probe the right side by PK
		case leftJoinPK:
			driveRight() // probe the left side by PK
		case leftIdx && rightIdx:
			if leftRT.liveCount() <= rightRT.liveCount() {
				driveLeft()
			} else {
				driveRight()
			}
		case rightIdx:
			driveLeft()
		case leftIdx:
			driveRight()
		default:
			return fmt.Errorf("%w: index %q.%q or %q.%q", ErrUnindexedJoin,
				st.table, leftDef.def.Columns[leftOnOrd].Name, jc.table, rightDef.def.Columns[rightOnOrd].Name)
		}
	}

	// Driver pushdown: the driver's own conjuncts pre-filter it, and an equality
	// on an indexed driver column lets us FETCH the driver through that index/PK
	// (driverIdxSrc → key) instead of scanning the whole table.
	driverConj, driverDef, driverOff := leftConj, leftDef, 0
	if !jp.driverIsLeft {
		driverConj, driverDef, driverOff = rightConj, rightDef, nLeft
	}
	jp.driverPreds = driverConj
	for _, p := range driverConj {
		b, ok := p.(*binOp)
		if !ok || b.op != tkEq {
			continue
		}
		cr, val := colAndLit(b)
		if cr == nil {
			continue
		}
		within := cr.ord - driverOff
		if within < 0 || within >= len(driverDef.def.Columns) {
			continue
		}
		if within == driverDef.pkOrdinal || jp.driverRT.indexFor(within) != nil {
			jp.driverIdxOrd, jp.driverIdxSrc, jp.driverIdxByPK = within, val, within == driverDef.pkOrdinal
			break
		}
	}

	// Projection + output names (global ordinals). SELECT * qualifies names as
	// alias.col / table.col so the two tables' columns never collide.
	if st.starAll {
		lq, rq := st.alias, jc.alias
		if lq == "" {
			lq = st.table
		}
		if rq == "" {
			rq = jc.table
		}
		pl.colNames = make([]string, 0, nLeft+nRight)
		for _, c := range leftDef.def.Columns {
			pl.colNames = append(pl.colNames, lq+"."+c.Name)
		}
		for _, c := range rightDef.def.Columns {
			pl.colNames = append(pl.colNames, rq+"."+c.Name)
		}
	} else {
		pl.projOrdinals = make([]int, 0, len(st.cols))
		pl.colNames = make([]string, 0, len(st.cols))
		for _, rc := range st.cols {
			g, err := resolveGlobal(rc.qual, rc.col)
			if err != nil {
				return err
			}
			pl.projOrdinals = append(pl.projOrdinals, g)
			pl.colNames = append(pl.colNames, rc.col)
		}
	}

	if st.orderCol != "" {
		g, err := resolveGlobal(st.orderQual, st.orderCol)
		if err != nil {
			return err
		}
		jp.orderOrdinal = g
	}

	// Pre-escape the output column names for the streaming JSON encoder, as the
	// single-table planner does.
	pl.colJSONPrefix = make([][]byte, len(pl.colNames))
	for i, name := range pl.colNames {
		pl.colJSONPrefix[i] = append(appendJSONString(nil, name), ':')
	}

	pl.joinPlan = jp
	return nil
}

// bindJoinExpr walks a WHERE expression and binds each colRef to its global
// concat-row ordinal via resolve. Mirrors validateExpr, but two-table aware.
func bindJoinExpr(e expr, resolve func(qual, name string) (int, error)) error {
	switch x := e.(type) {
	case nil:
		return nil
	case *colRef:
		ord, err := resolve(x.qual, x.name)
		if err != nil {
			return err
		}
		x.ord = ord
	case *binOp:
		if err := bindJoinExpr(x.lhs, resolve); err != nil {
			return err
		}
		return bindJoinExpr(x.rhs, resolve)
	case *notExpr:
		return bindJoinExpr(x.e, resolve)
	case *isNullExpr:
		return bindJoinExpr(x.e, resolve)
	}
	return nil
}

// collectConjuncts appends the top-level AND conjuncts of e to out (a single
// predicate if there is no AND).
func collectConjuncts(e expr, out *[]expr) {
	if b, ok := e.(*binOp); ok && b.op == tkAnd {
		collectConjuncts(b.lhs, out)
		collectConjuncts(b.rhs, out)
		return
	}
	if e != nil {
		*out = append(*out, e)
	}
}

// exprOrdsWithin reports whether e references at least one column and every
// colRef's (already-bound) ordinal lies in [lo,hi) — i.e. the predicate touches
// exactly one table's columns.
func exprOrdsWithin(e expr, lo, hi int) bool {
	any, ok := false, true
	var walk func(expr)
	walk = func(e expr) {
		switch x := e.(type) {
		case *colRef:
			any = true
			if x.ord < lo || x.ord >= hi {
				ok = false
			}
		case *binOp:
			walk(x.lhs)
			walk(x.rhs)
		case *notExpr:
			walk(x.e)
		case *isNullExpr:
			walk(x.e)
		}
	}
	walk(e)
	return any && ok
}

// colAndLit returns the colRef side and the literal/parameter side of an
// equality binOp (either order), or (nil,nil) if it is not col=lit/param.
func colAndLit(b *binOp) (*colRef, expr) {
	if cr, ok := b.lhs.(*colRef); ok && isLitOrParam(b.rhs) {
		return cr, b.rhs
	}
	if cr, ok := b.rhs.(*colRef); ok && isLitOrParam(b.lhs) {
		return cr, b.lhs
	}
	return nil, nil
}

// probeRows calls fn for each row of tbl whose join column (onOrd) equals key —
// via the PK map (byPK) or the secondary index (bucket ∪ dirty overlay, with a
// live re-check, exactly like idxCandidates). A NULL key matches nothing (SQL
// join semantics). fn returns true to stop. Rows handed to fn are owned clones
// (getByPK), so the caller may retain them.
func (db *DB) probeRows(tbl *tableRT, onOrd int, byPK bool, key Value, fn func(Row) bool) {
	if key.IsNull() {
		return
	}
	if byPK {
		pk, err := coerceToUUID(key)
		if err != nil {
			return
		}
		if r, ok := tbl.getByPK(pk); ok {
			fn(r)
		}
		return
	}
	si := tbl.indexFor(onOrd)
	if si == nil {
		return
	}
	cand := idxCandidateSet{pks: si.lookup(keyOf(key)), dirty: tbl.dirtyPKs()}
	cand.emit(func(pk UUID) bool {
		r, ok := tbl.getByPK(pk)
		if !ok {
			return false
		}
		if !r[onOrd].Equal(key) { // re-check: index entry may be stale / dirty
			return false
		}
		return fn(r)
	})
}

// execJoin runs a two-table indexed nested-loop join: materialise the driver
// (so no driver lock is held across the probe), probe the other side per row,
// assemble [left..right..] concat rows, filter by the full WHERE, then sort /
// OFFSET / LIMIT / project on the result.
func (db *DB) execJoin(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	jp := pl.joinPlan
	colNames := pl.colNames
	if st.limit == 0 {
		return colNames, nil, nil
	}

	ctx := evalCtx{args: args}
	nLeft, nRight := jp.nLeft, jp.nRight

	// Phase 1: gather the driver rows (owned, so no driver shard lock is held
	// across the probe below). Push the driver's own WHERE conjuncts down here so
	// the probe only runs for rows that can survive; if the driver carries an
	// equality on an indexed column, fetch it through that index instead of a
	// full scan. driverOff positions a driver row inside a full-width scratch so
	// the global-ordinal-bound predicates evaluate directly.
	driverOff, driverWidth := 0, nLeft
	if !jp.driverIsLeft {
		driverOff, driverWidth = nLeft, nRight
	}
	var scratch Row
	if len(jp.driverPreds) > 0 {
		scratch = make(Row, nLeft+nRight)
	}
	passDriver := func(r Row) bool {
		if len(jp.driverPreds) == 0 {
			return true
		}
		copy(scratch[driverOff:driverOff+driverWidth], r)
		ctx.row = scratch
		for _, p := range jp.driverPreds {
			v, err := evalExpr(p, &ctx)
			if err != nil || !truthy(v) {
				return false
			}
		}
		return true
	}
	var drivers []Row
	if jp.driverIdxSrc != nil {
		// Indexed driver fetch: key from the equality's value side.
		kctx := evalCtx{args: args}
		key, err := evalExpr(jp.driverIdxSrc, &kctx)
		if err != nil {
			return nil, nil, err
		}
		db.probeRows(jp.driverRT, jp.driverIdxOrd, jp.driverIdxByPK, key, func(dr Row) bool {
			if passDriver(dr) { // dr is an owned clone (getByPK)
				drivers = append(drivers, dr)
			}
			return false
		})
	} else {
		jp.driverRT.scanAll(func(r Row) bool {
			if passDriver(r) {
				drivers = append(drivers, r.Clone())
			}
			return true
		})
	}

	// Phase 2: probe + assemble, holding no driver lock.
	eff := fetchBound(st.limit, st.offset)
	ordered := jp.orderOrdinal >= 0
	var out []Row

	assemble := func(left, right Row) Row { // a nil side is NULL-padded (outer miss)
		row := make(Row, nLeft+nRight)
		if left != nil {
			copy(row, left)
		} else {
			for i := 0; i < nLeft; i++ {
				row[i] = Null()
			}
		}
		if right != nil {
			copy(row[nLeft:], right)
		} else {
			for i := nLeft; i < nLeft+nRight; i++ {
				row[i] = Null()
			}
		}
		return row
	}
	// keep filters by the full WHERE and collects; returns true to STOP (the
	// fetch bound is offset+limit, dropped to limit by sliceOffsetLimit later).
	keep := func(concat Row) bool {
		if st.where != nil {
			ctx.row = concat
			v, err := evalExpr(st.where, &ctx)
			if err != nil || !truthy(v) {
				return false
			}
		}
		out = append(out, concat)
		return !ordered && eff >= 0 && len(out) >= eff
	}

	for _, drow := range drivers {
		key := drow[jp.driverOnOrd]
		matched, stop := false, false
		db.probeRows(jp.probeRT, jp.probeOnOrd, jp.probeByPK, key, func(prow Row) bool {
			matched = true
			left, right := drow, prow
			if !jp.driverIsLeft {
				left, right = prow, drow
			}
			if keep(assemble(left, right)) {
				stop = true
				return true
			}
			return false
		})
		if stop {
			break
		}
		// An OUTER join preserves its driver: an unmatched driver row is emitted
		// with the other side NULL-padded. LEFT drives left → pad right; RIGHT
		// drives right → pad left.
		if !matched {
			var stopOuter bool
			switch jp.typ {
			case tkLeft:
				stopOuter = keep(assemble(drow, nil))
			case tkRight:
				stopOuter = keep(assemble(nil, drow))
			}
			if stopOuter {
				break
			}
		}
	}

	if ordered {
		sortRowsByCol(out, jp.orderOrdinal, st.orderDesc)
	}
	out = sliceOffsetLimit(out, st.offset, st.limit)
	return colNames, projectRows(out, st.starAll, pl.projOrdinals), nil
}
