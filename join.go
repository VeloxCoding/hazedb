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
// v1 scope: one JOIN (two tables), single `ON a.col = b.col` equality, INNER,
// LEFT, or RIGHT. FULL/CROSS, N-way joins, and non-equi conditions are deferred.

import "fmt"

// joinPlan is the resolved form of a two-table join. Set on plan.joinPlan;
// execSelect dispatches to execJoin when it is non-nil.
type joinPlan struct {
	leftRT, rightRT *tableRT
	typ             tokenKind // tkInner, tkLeft, or tkRight
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

	// residualPreds are the WHERE conjuncts NOT pushed into the driver (probe-side,
	// cross-table, and constant conjuncts), evaluated on the assembled concat row.
	// Driver conjuncts are already enforced by passDriver, so re-checking the full
	// WHERE there would be wasted work; AND(driverPreds) ∧ AND(residualPreds) == WHERE.
	residualPreds []expr

	orderOrdinal int // global ordinal of the ORDER BY column, -1 if none

	// probeWalk: ORDER BY is on a probe-side column backed by a composite
	// (joinkey, ordercol) ordered index, so the probe can return that key's rows
	// already sorted. probeWalkOrdinals is the index's column list (within the
	// probe table). Used only for the single-driver case in execJoin (walk the
	// prefix sub-range + early-stop, no sort); >1 driver falls back to top-N.
	probeWalk         bool
	probeWalkOrdinals []int

	// driverWalk: ORDER BY is on a DRIVER-table column carrying an ORDERED index,
	// and the driver is a full scan (not index-fetched by a WHERE equality). The
	// join then walks the driver in ORDER BY order, probes each, and stops at
	// offset+limit results — no full materialise + sort. driverSortOrd is the sort
	// column's within-driver ordinal. INNER and OUTER (drives the preserved side).
	driverWalk    bool
	driverSortOrd int

	// driverCompWalk: the driver's WHERE pins the leading column(s) of a composite
	// ORDERED index and ORDER BY is the next column — walk that prefix in order
	// (the driver analog of the single-table composite walk / probeWalk), probe
	// each, early-stop. Supersedes the pushdown fetch + sort. driverCompOrdinals is
	// the index's column list (within driver); driverCompPrefixSrcs yields the
	// pinned leading values. INNER and OUTER.
	driverCompWalk       bool
	driverCompOrdinals   []int
	driverCompPrefixSrcs []expr
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

	indexed := func(rt *tableRT, ord int) bool { return ord == rt.def.pkOrdinal || rt.probeIndexFor(ord) != nil }
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
	// residualPreds = every conjunct NOT pushed into the driver. driverConj ⊂ conj,
	// so this is conj \ driverConj (probe-only + cross-table + constant conjuncts).
	// passDriver already enforces driverConj; the phase-2 filter only needs these.
	for _, c := range conj {
		isDriver := false
		for _, d := range driverConj {
			if c == d {
				isDriver = true
				break
			}
		}
		if !isDriver {
			jp.residualPreds = append(jp.residualPreds, c)
		}
	}
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
		// probeIndexFor (not indexFor): a composite index's LEADING column drives
		// the pushdown too — the fetch below (probeRows) already resolves it via a
		// prefix lookup, so the detection must match or the driver falls back to a
		// full scan (the slow composite-only LEFT case).
		if within == driverDef.pkOrdinal || jp.driverRT.probeIndexFor(within) != nil {
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

	// Probe-side ordered walk (INNER only): ORDER BY a probe column backed by a
	// composite (joinkey, ordercol) ordered index lets the single-driver probe
	// return that key's rows already sorted — walk + early-stop instead of
	// gather + sort. OUTER joins add NULL-padded misses that complicate the walk;
	// they keep the top-N path.
	if jp.typ == tkInner && jp.orderOrdinal >= 0 {
		jp.planProbeWalk()
	}
	// Driver ordered walk: ORDER BY a driver column with an ordered index (INNER or
	// OUTER). Walk the driver in order + probe + early-stop, instead of
	// materialising the whole join and sorting. Not when probeWalk already applies.
	if jp.orderOrdinal >= 0 && !jp.probeWalk {
		jp.planDriverWalk()
	}
	// Driver composite-prefix walk: the driver's WHERE pins the leading column(s)
	// of a composite ordered index and ORDER BY is the next column — walk that
	// prefix in order (this is the lever for a filtered LEFT join, where probeWalk
	// does not apply). driverWalk (unfiltered single-column) takes precedence.
	if jp.orderOrdinal >= 0 && !jp.probeWalk && !jp.driverWalk {
		jp.planDriverCompWalk(driverConj, driverOff)
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

// planProbeWalk sets probeWalk when the ORDER BY column is a probe-table column
// whose ordinal, together with the join key, forms the leading two columns of a
// NOT-NULL composite ordered index on the probe — i.e. (joinkey, ordercol). Then
// probing one join key yields its rows already ordered by ordercol.
func (jp *joinPlan) planProbeWalk() {
	probeOff, probeWidth := 0, jp.nLeft // probe is the left table
	if jp.driverIsLeft {
		probeOff, probeWidth = jp.nLeft, jp.nRight // probe is the right table
	}
	orderWithin := jp.orderOrdinal - probeOff
	if orderWithin < 0 || orderWithin >= probeWidth {
		return // ORDER BY is not on the probe table
	}
	rt := jp.probeRT.def
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		if !ri.ordered || len(ri.ordinals) < 2 {
			continue
		}
		if ri.ordinals[0] != jp.probeOnOrd || ri.ordinals[1] != orderWithin {
			continue
		}
		if rt.anyNullable(ri.ordinals) {
			continue
		}
		jp.probeWalkOrdinals = ri.ordinals
		jp.probeWalk = true
		return
	}
}

// planDriverWalk sets driverWalk when the ORDER BY column is a DRIVER-table column
// carrying an ORDERED index and the driver is a full scan (not fetched through a
// WHERE-equality index — that is already bounded). The join can then walk the
// driver in ORDER BY order and stop at offset+limit results instead of
// materialising the whole join and sorting.
func (jp *joinPlan) planDriverWalk() {
	if jp.driverIdxSrc != nil {
		return // driver already index-fetched → bounded, materialise it
	}
	driverOff, driverWidth := 0, jp.nLeft
	if !jp.driverIsLeft {
		driverOff, driverWidth = jp.nLeft, jp.nRight
	}
	sortWithin := jp.orderOrdinal - driverOff
	if sortWithin < 0 || sortWithin >= driverWidth {
		return // ORDER BY is not on the driver table
	}
	if !jp.driverRT.def.orderedIndexOn(sortWithin) {
		return // no ordered index on the driver sort column to walk
	}
	jp.driverSortOrd = sortWithin
	jp.driverWalk = true
}

// planDriverCompWalk sets driverCompWalk when the driver's WHERE pins a
// contiguous leading prefix of a composite ORDERED index and ORDER BY is the
// first unpinned column of that index — so probing one prefix yields the driver
// rows already ordered by the sort column (the driver analog of the single-table
// composite walk; covers the filtered LEFT join, where probeWalk cannot apply).
func (jp *joinPlan) planDriverCompWalk(driverConj []expr, driverOff int) {
	driverWidth := jp.nLeft
	if !jp.driverIsLeft {
		driverWidth = jp.nRight
	}
	orderWithin := jp.orderOrdinal - driverOff
	if orderWithin < 0 || orderWithin >= driverWidth {
		return // ORDER BY is not on the driver table
	}
	// pinned: within-driver ordinal -> value expr, from the driver's equality conjuncts.
	pinned := map[int]expr{}
	for _, p := range driverConj {
		if b, ok := p.(*binOp); ok && b.op == tkEq {
			if cr, val := colAndLit(b); cr != nil {
				if within := cr.ord - driverOff; within >= 0 && within < driverWidth {
					pinned[within] = val
				}
			}
		}
	}
	if len(pinned) == 0 {
		return
	}
	rt := jp.driverRT.def
	for i := range rt.indexes {
		ri := &rt.indexes[i]
		if !ri.ordered || len(ri.ordinals) < 2 || rt.anyNullable(ri.ordinals) {
			continue
		}
		k := 0
		for k < len(ri.ordinals) {
			if _, has := pinned[ri.ordinals[k]]; !has {
				break
			}
			k++
		}
		if k == 0 || k >= len(ri.ordinals) || ri.ordinals[k] != orderWithin {
			continue // need ≥1 pinned leading column AND ORDER BY as the next column
		}
		srcs := make([]expr, k)
		for j := 0; j < k; j++ {
			srcs[j] = pinned[ri.ordinals[j]]
		}
		jp.driverCompOrdinals = ri.ordinals
		jp.driverCompPrefixSrcs = srcs
		jp.driverCompWalk = true
		return
	}
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
//
// dirty is the table's dirty-PK overlay, snapshotted ONCE by the caller (every
// probe in a join shares it) — re-snapshotting per probe row would RLock all
// shards and copy the whole overlay on each of O(driver) probes. Ignored on the
// byPK path. nil is fine (no overlay).
// reuse and collect are mutually-exclusive optional buffers for the fetched row.
//   - reuse: one caller-owned buffer reused per row (getByPKProjectInto), for a
//     caller that CONSUMES each row immediately (the probe-and-assemble loops copy
//     the cells into a concat scratch right away) — no per-row allocation.
//   - collect: a backing the matched rows are PACKED into (each Row a window),
//     presized here from the candidate count, for a caller that RETAINS them (the
//     index-fetched driver) — one allocation for the whole driver set instead of a
//     getByPK clone per row. A passDriver reject in fn leaves an unused slot (the
//     backing was sized to the upper bound, so it never regrows).
//
// nil for both keeps the owned-clone behaviour. dirty is the table's dirty-PK
// overlay, snapshotted ONCE by the caller (ignored on the byPK path; nil = none).
func (db *DB) probeRows(tbl *tableRT, onOrd int, byPK bool, key Value, dirty []UUID, reuse, collect *[]Value, fn func(Row) bool) {
	if key.IsNull() {
		return
	}
	if byPK {
		pk, err := coerceToUUID(key)
		if err != nil {
			return
		}
		if collect != nil {
			start := len(*collect)
			if c, ok := tbl.appendMatchProject(pk, nil, nil, true, *collect); ok {
				*collect = c
				fn(Row(c[start:len(c):len(c)]))
			}
			return
		}
		if reuse != nil {
			if out, ok := tbl.getByPKProjectInto(pk, nil, *reuse); ok {
				*reuse = out
				fn(Row(out))
			}
			return
		}
		if r, ok := tbl.getByPK(pk); ok {
			fn(r)
		}
		return
	}
	si := tbl.probeIndexFor(onOrd)
	if si == nil {
		return
	}
	cand := idxCandidateSet{pks: si.lookupLeading(key), dirty: dirty}
	if collect != nil && cap(*collect) == 0 {
		*collect = make([]Value, 0, (len(cand.pks)+len(dirty))*len(tbl.def.def.Columns))
	}
	cand.emit(func(pk UUID) bool {
		if collect != nil {
			start := len(*collect)
			c, ok := tbl.appendMatchProject(pk, nil, nil, true, *collect)
			*collect = c
			if !ok {
				return false
			}
			r := Row(c[start:len(c):len(c)])
			if !r[onOrd].Equal(key) { // re-check: index entry may be stale / dirty
				*collect = c[:start] // drop the rejected row's slot
				return false
			}
			return fn(r)
		}
		var r Row
		var ok bool
		if reuse != nil {
			var out []Value
			if out, ok = tbl.getByPKProjectInto(pk, nil, *reuse); ok {
				*reuse = out
				r = Row(out)
			}
		} else {
			r, ok = tbl.getByPK(pk)
		}
		if !ok {
			return false
		}
		if !r[onOrd].Equal(key) { // re-check: index entry may be stale / dirty
			return false
		}
		return fn(r)
	})
}

// appendProjected appends src's projection (or a full clone for SELECT *) to out,
// already in output shape — so an ordered-walk join needs no projectRows pass. A
// bounded result (the caller presized packed to offset+limit × width) carves each
// row from the one shared packed backing — one allocation for the whole result
// instead of one per row; the backing never regrows, so no earlier row view points
// at a superseded generation. An unbounded result allocates per row, since a
// regrowing shared backing would retain every doubling generation. Returns the
// grown out and packed.
func appendProjected(out []Row, packed []Value, src Row, ords []int, starAll, bounded bool) ([]Row, []Value) {
	if !bounded {
		if starAll {
			return append(out, src.Clone()), packed
		}
		return append(out, projectClone(src, ords)), packed
	}
	start := len(packed)
	if starAll {
		packed = appendRowClone(packed, src)
	} else {
		packed = appendProjectClone(packed, src, ords)
	}
	return append(out, Row(packed[start:len(packed):len(packed)])), packed
}

// execJoin runs a two-table indexed nested-loop join: feed driver rows (never
// holding a driver lock across the probe), probe the other side per row, assemble
// [left..right..] concat rows, filter by the full WHERE, then sort / OFFSET /
// LIMIT / project on the result. The driver is materialised when it is
// index-fetched or an ORDER BY needs all of it; a scanned no-ORDER-BY driver is
// streamed chunk by chunk with early-stop (see drive).
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
	driverMatch := conjunctsMatcher(jp.driverPreds, &ctx)
	passDriver := func(r Row) bool {
		if len(jp.driverPreds) == 0 {
			return true
		}
		copy(scratch[driverOff:driverOff+driverWidth], r)
		return driverMatch(scratch) // driverPreds are bound to concat ordinals
	}
	// A scanned driver with no ORDER BY can STREAM: drive() pulls it chunk by chunk
	// and stops at offset+limit instead of materialising the whole table up front
	// (the result order is undefined, so any offset+limit rows are valid). With an
	// ORDER BY every driver must be seen to sort, and an index-fetched driver is
	// already bounded — both materialise here.
	ordered := jp.orderOrdinal >= 0
	streaming := jp.driverIdxSrc == nil && !ordered
	var drivers []Row
	if jp.driverIdxSrc != nil && !jp.driverCompWalk {
		// Indexed driver fetch: key from the equality's value side. (Skipped for a
		// driver composite walk, which pulls the driver via its own prefix walk.)
		kctx := evalCtx{args: args}
		key, err := evalExpr(jp.driverIdxSrc, &kctx)
		if err != nil {
			return nil, nil, err
		}
		// Pack the retained driver rows into ONE backing (collect) — presized from the
		// index candidate count inside probeRows — instead of a getByPK clone per row.
		var driverBacking []Value
		db.probeRows(jp.driverRT, jp.driverIdxOrd, jp.driverIdxByPK, key, jp.driverRT.dirtyPKs(), nil, &driverBacking, func(dr Row) bool {
			if passDriver(dr) { // dr is a window into driverBacking
				drivers = append(drivers, dr)
			}
			return false
		})
	} else if jp.driverIdxSrc == nil && !streaming && !jp.driverWalk {
		// Pack every retained driver row into ONE backing slice — each Row a capped
		// window into it — instead of a make()+copy Clone per row. The driver set is
		// held for the whole probe phase, so the backing lives as long as the rows.
		// PRESIZE to liveCount × width: the backing must NOT regrow — a regrow copies
		// every cell AND leaves all earlier windows pointing at their (now superseded
		// but still-referenced) prior backing, so each doubling generation stays alive.
		// liveCount is an upper bound (passDriver may filter), so one alloc covers it.
		hint := jp.driverRT.liveCount()
		backing := make([]Value, 0, hint*driverWidth)
		drivers = make([]Row, 0, hint)
		jp.driverRT.scanAll(func(r Row) bool {
			if passDriver(r) {
				start := len(backing)
				backing = appendRowClone(backing, r)
				drivers = append(drivers, Row(backing[start:len(backing):len(backing)]))
			}
			return true
		})
	}

	// Phase 2: probe + assemble, holding no driver lock.
	eff := fetchBound(st.limit, st.offset)

	// Snapshot the probe table's dirty overlay ONCE for the whole probe phase.
	// Every per-driver-row probe shares it; without this, each probe re-RLocks all
	// shards and copies the overlay (O(driver × dirty)). Consistent with the join's
	// per-shard, not-point-in-time contract (the driver is already materialised).
	probeDirty := jp.probeRT.dirtyPKs()

	// One reusable buffer for the probe row across every probe in phase 2: each
	// probe row is copied into it and consumed immediately (fillConcat into the
	// concat scratch), never retained, so the whole probe phase allocates the probe
	// row once instead of once per matched pair. Probes run sequentially.
	var probeScratch []Value

	// passResidual applies the WHERE conjuncts NOT already pushed into the driver
	// (probe-side / cross-table / constant). Driver conjuncts ran in passDriver, so
	// re-checking the full WHERE here would be wasted work.
	passResidual := conjunctsMatcher(jp.residualPreds, &ctx)
	// fillConcat writes [left.. right..] into dst; a nil side is NULL-padded (an
	// outer-join miss). dst is caller-owned — reused as scratch on the top-N path,
	// freshly allocated on the gather path.
	fillConcat := func(dst, left, right Row) {
		if left != nil {
			copy(dst[:nLeft], left)
		} else {
			for i := 0; i < nLeft; i++ {
				dst[i] = Null()
			}
		}
		if right != nil {
			copy(dst[nLeft:], right)
		} else {
			for i := nLeft; i < nLeft+nRight; i++ {
				dst[i] = Null()
			}
		}
	}
	// drive feeds driver rows to emit(left, right) per matched pair — plus once with
	// the padded side nil for an unmatched OUTER driver (LEFT pads right, RIGHT pads
	// left). emit returns true to stop the whole join early. A materialised driver
	// (index-fetched, or an ORDER BY scan) is iterated directly; a streaming driver
	// is pulled chunk by chunk via scanShardsBatched (clone under the shard lock,
	// release, probe — no driver lock held across the probe), so emit's early-stop
	// avoids materialising the rest of the table.
	drive := func(emit func(left, right Row) bool) {
		probeAndEmit := func(drow Row) bool { // returns true to stop the whole join
			matched, stop := false, false
			db.probeRows(jp.probeRT, jp.probeOnOrd, jp.probeByPK, drow[jp.driverOnOrd], probeDirty, &probeScratch, nil, func(prow Row) bool {
				matched = true
				left, right := drow, prow
				if !jp.driverIsLeft {
					left, right = prow, drow
				}
				if emit(left, right) {
					stop = true
					return true
				}
				return false
			})
			if stop {
				return true
			}
			if !matched {
				switch jp.typ {
				case tkLeft:
					return emit(drow, nil)
				case tkRight:
					return emit(nil, drow)
				}
			}
			return false
		}
		if !streaming {
			for _, drow := range drivers {
				if probeAndEmit(drow) {
					return
				}
			}
			return
		}
		// Size the clone-chunk to the output bound: a small LIMIT should clone ~its
		// own size, not a fixed 256, before the first probe. Floor 32 absorbs probe
		// misses without an extra lock cycle; unbounded (eff<0) uses 256.
		chunk := 256
		if eff >= 0 && eff < chunk {
			if chunk = eff; chunk < 32 {
				chunk = 32
			}
		}
		jp.driverRT.scanShardsBatched(chunk, passDriver, func(batch []Row) bool {
			for _, dr := range batch {
				if probeAndEmit(dr) {
					return true
				}
			}
			return false
		})
	}

	// Single-driver probe walk: ORDER BY is on a probe column backed by a composite
	// (joinkey, ordercol) index. Walk that key's prefix sub-range — already ordered
	// by ordercol — assembling concat rows and stopping at offset+limit, so neither
	// the whole bucket is fetched nor a sort runs. One driver only: >1 needs a k-way
	// merge across prefixes and falls through to the top-N path below.
	if jp.probeWalk && ordered && len(drivers) == 1 {
		drow := drivers[0]
		key := drow[jp.driverOnOrd]
		if key.IsNull() {
			return colNames, nil, nil // INNER: a NULL join key matches nothing
		}
		if si := jp.probeRT.indexByOrdinals(jp.probeWalkOrdinals); si != nil {
			ords := si.ordinals
			dirtyKey := func(r Row) indexKey {
				cells := make([]Value, len(ords))
				for i, o := range ords {
					cells[i] = r[o]
				}
				return encodeCompositeKey(cells)
			}
			matchKey := func(r Row) bool { return r[jp.probeOnOrd].Equal(key) }
			dc, dirtySet := jp.probeRT.buildDirtyCands(matchKey, dirtyKey)
			snap := si.snapshotPrefix(encodeCompositeKey([]Value{key}))
			scratch := make(Row, nLeft+nRight)
			// Project each kept row straight into one presized backing during the walk —
			// no full-concat clone per row and no projectRows pass afterwards (the walk is
			// already in ORDER BY order, so collecting offset+limit rows is the result).
			// Bounded (eff>=0) packs into one backing; unbounded projects per row (a
			// regrowing backing would retain superseded generations, so it is not packed).
			width := len(pl.projOrdinals)
			if st.starAll {
				width = nLeft + nRight
			}
			bounded := eff >= 0
			outCap := 0
			var packed []Value
			if bounded {
				outCap = eff
				packed = make([]Value, 0, eff*width)
			}
			out := make([]Row, 0, outCap)
			done := func() bool { return eff >= 0 && len(out) >= eff }
			keep := func(prow Row) {
				left, right := drow, prow
				if !jp.driverIsLeft {
					left, right = prow, drow
				}
				fillConcat(scratch, left, right)
				if passResidual(scratch) {
					out, packed = appendProjected(out, packed, scratch, pl.projOrdinals, st.starAll, bounded)
				}
			}
			mergeOrderedStreams(snap, dc, dirtySet, st.orderDesc, done,
				func(pk UUID) {
					// keep consumes the probe row immediately (copies into scratch), so
					// reuse one buffer instead of a throwaway getByPK clone per walk step.
					if out, ok := jp.probeRT.getByPKProjectInto(pk, nil, probeScratch); ok {
						probeScratch = out
						keep(Row(out))
					}
				},
				func(r Row) { keep(r) },
			)
			return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
		}
	}

	// Driver ordered walk: ORDER BY a driver column reachable in order — either a
	// single-column ordered index over the whole driver (driverWalk), or a
	// composite (filter…, ordercol) whose leading columns the WHERE pins, walked at
	// that prefix (driverCompWalk, the filtered-LEFT lever). Walk the driver in
	// ORDER BY order (index ∪ dirty), probe each, emit matched (OUTER pads a miss),
	// stop at offset+limit — no full materialise, no sort. Reuses mergeOrderedStreams.
	if jp.driverWalk || jp.driverCompWalk {
		var snap []ordEntry
		var dirtyKey func(Row) indexKey
		walkable := false
		if jp.driverCompWalk {
			if si := jp.driverRT.indexByOrdinals(jp.driverCompOrdinals); si != nil {
				prefix := make([]Value, len(jp.driverCompPrefixSrcs))
				nullPrefix := false
				for i, src := range jp.driverCompPrefixSrcs {
					v, err := evalExpr(src, &ctx)
					if err != nil {
						return nil, nil, err
					}
					if v.IsNull() {
						nullPrefix = true
						break
					}
					prefix[i] = v
				}
				if nullPrefix {
					return colNames, nil, nil // WHERE pins the prefix to NULL → no driver rows
				}
				ords := si.ordinals
				snap = si.snapshotPrefix(encodeCompositeKey(prefix))
				dirtyKey = func(r Row) indexKey {
					cells := make([]Value, len(ords))
					for i, o := range ords {
						cells[i] = r[o]
					}
					return encodeCompositeKey(cells)
				}
				walkable = true
			}
		} else if si := jp.driverRT.indexFor(jp.driverSortOrd); si != nil {
			ord := jp.driverSortOrd
			snap = si.snapshot()
			dirtyKey = func(r Row) indexKey { return keyOf(r[ord]) }
			walkable = true
		}
		if walkable {
			// Project each kept row into one presized backing during the walk (already
			// in ORDER BY order) — no full-concat clone per row and no projectRows pass.
			// Bounded packs into one backing; unbounded projects per row (see appendProjected).
			width := len(pl.projOrdinals)
			if st.starAll {
				width = nLeft + nRight
			}
			bounded := eff >= 0
			outCap := 0
			var packed []Value
			if bounded {
				outCap = eff
				packed = make([]Value, 0, eff*width)
			}
			out := make([]Row, 0, outCap)
			concat := make(Row, nLeft+nRight)
			appendJoined := func(drow Row) {
				matched := false
				db.probeRows(jp.probeRT, jp.probeOnOrd, jp.probeByPK, drow[jp.driverOnOrd], probeDirty, &probeScratch, nil, func(prow Row) bool {
					matched = true
					left, right := drow, prow
					if !jp.driverIsLeft {
						left, right = prow, drow
					}
					fillConcat(concat, left, right)
					if passResidual(concat) {
						out, packed = appendProjected(out, packed, concat, pl.projOrdinals, st.starAll, bounded)
					}
					return false // collect every match for this driver
				})
				if !matched { // OUTER miss: pad the probed side
					pad := false
					switch jp.typ {
					case tkLeft:
						fillConcat(concat, drow, nil)
						pad = true
					case tkRight:
						fillConcat(concat, nil, drow)
						pad = true
					}
					if pad && passResidual(concat) {
						out, packed = appendProjected(out, packed, concat, pl.projOrdinals, st.starAll, bounded)
					}
				}
			}
			dc, dirtySet := jp.driverRT.buildDirtyCands(passDriver, dirtyKey)
			done := func() bool { return eff >= 0 && len(out) >= eff }
			// appendJoined consumes the driver row immediately (fillConcat copies it),
			// so reuse one buffer for the walk's index-row fetches instead of a
			// throwaway getByPK clone per step. Dirty rows are owned clones already.
			var driverScratch []Value
			mergeOrderedStreams(snap, dc, dirtySet, st.orderDesc, done,
				func(pk UUID) {
					if out, ok := jp.driverRT.getByPKProjectInto(pk, nil, driverScratch); ok {
						driverScratch = out
						if r := Row(out); passDriver(r) {
							appendJoined(r)
						}
					}
				},
				func(r Row) { appendJoined(r) },
			)
			return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
		}
	}

	// ORDER BY + LIMIT: keep only the best offset+limit rows by the order column
	// via a top-N heap. Each candidate is assembled into one reused scratch row and
	// cloned into the heap only when it places — O(offset+limit) clones instead of
	// O(matched) full joined rows. Never stops early: a later row may outrank the heap.
	if ordered && st.limit >= 0 {
		top := topN{ord: jp.orderOrdinal, desc: st.orderDesc, capN: eff, proj: projOrNil(st.starAll, pl.projOrdinals)}
		scratch := make(Row, nLeft+nRight)
		drive(func(left, right Row) bool {
			fillConcat(scratch, left, right)
			if passResidual(scratch) {
				top.offer(scratch) // projects into the heap only if it enters
			}
			return false
		})
		return colNames, sliceOffsetLimit(top.sorted(), st.offset, st.limit), nil
	}

	// No ORDER BY (stop once offset+limit rows are collected), or ORDER BY without
	// LIMIT (gather all, then sort). A kept row is a fresh alloc since it is retained.
	var out []Row
	drive(func(left, right Row) bool {
		row := make(Row, nLeft+nRight)
		fillConcat(row, left, right)
		if !passResidual(row) {
			return false
		}
		out = append(out, row)
		return !ordered && eff >= 0 && len(out) >= eff
	})
	if ordered {
		sortRowsByCol(out, jp.orderOrdinal, st.orderDesc)
	}
	out = sliceOffsetLimit(out, st.offset, st.limit)
	return colNames, projectRows(out, st.starAll, pl.projOrdinals), nil
}
