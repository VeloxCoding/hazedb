package hazedb

import (
	"slices"
	"sort"
)

// execSelectPK reads the single PK-matched row, projected (or the whole row for
// SELECT *). Returns no rows for LIMIT 0, any OFFSET, or a NULL key.
func (db *DB) execSelectPK(pl *plan, keyVal Value) ([]string, []Row, error) {
	if keyVal.IsNull() {
		return pl.colNames, nil, nil
	}
	pk, err := coerceToUUID(keyVal)
	if err != nil {
		return nil, nil, err
	}
	return db.execSelectPKResolved(pl, pk)
}

// execSelectPKResolved serves a PK SELECT once the key is a resolved UUID — the
// shared core of execSelectPK and the direct-UUID read fast path (which skips the
// Value round-trip + coerce).
func (db *DB) execSelectPKResolved(pl *plan, pk UUID) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
	// A PK match is at most one row, so LIMIT 0 or any OFFSET drops it.
	if st.limit == 0 || st.offset > 0 {
		return colNames, nil, nil
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

// execSelectPKResidual serves a SELECT whose WHERE pins the PK by equality inside
// an AND-chain (pl.pkProbe): fetch the one PK-addressed row, then return it only
// if the FULL WHERE matches, so residual conjuncts (WHERE id = ? AND age = ?) are
// honoured. A PK match is at most one row, so LIMIT 0 / any OFFSET drops it and
// ORDER BY needs no sort. The row is cloned under the lock by getByPK, so matching
// and projecting it off-lock is consistent.
func (db *DB) execSelectPKResidual(pl *plan, keyVal Value, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	tbl := pl.rt
	colNames := pl.colNames
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
	r, ok := tbl.getByPK(pk)
	if !ok {
		return colNames, nil, nil
	}
	match := rowMatcher(st.where, &evalCtx{cols: tbl.def.colByName, args: args})
	if !match(r) {
		return colNames, nil, nil
	}
	if st.starAll {
		return colNames, []Row{r}, nil
	}
	proj := make(Row, len(pl.projOrdinals))
	for i, ord := range pl.projOrdinals {
		proj[i] = r[ord]
	}
	return colNames, []Row{proj}, nil
}

// offerLiveRow offers pk's live row to an ORDER BY top-N heap under the shard
// read lock: pred (the full WHERE) and the order-column compare read the row in
// place, and topN.offer clones it only if it makes the cut. So ORDER BY ... LIMIT
// n over a large filtered set clones ~n rows, not the whole matched set.
func (t *table) offerLiveRow(pk UUID, pred func(Row) bool, top *topN) {
	s := t.shardOf(pk)
	s.mu.RLock()
	if rowID, ok := s.pk.get(pk); ok {
		if r := s.rows[rowID]; r != nil && pred(r) {
			top.offer(r)
		}
	}
	s.mu.RUnlock()
}

// collectLiveRow is offerLiveRow's unbounded form (ORDER BY without LIMIT): under
// the shard read lock it fetches pk's live row, checks pred, and on a pass
// collects its (order key + projection) into top — capturing only those, never a
// full-row clone to narrow again later.
func (t *table) collectLiveRow(pk UUID, pred func(Row) bool, top *topN) {
	s := t.shardOf(pk)
	s.mu.RLock()
	if rowID, ok := s.pk.get(pk); ok {
		if r := s.rows[rowID]; r != nil && pred(r) {
			top.collect(r)
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
	rowID, ok := s.pk.get(pk)
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
	rowID, ok := s.pk.get(pk)
	if !ok {
		return dst, false
	}
	r := s.rows[rowID]
	if r == nil || (pred != nil && !pred(r)) { // nil pred = caller guarantees the match
		return dst, false
	}
	if starAll {
		return appendRowClone(dst, r), true
	}
	return appendProjectClone(dst, r, ords), true
}

// appendMatchJSON is appendMatchProject's encode-under-lock form: it fetches
// pk's live row, checks pred (the full WHERE) under the shard read lock, and on
// a pass appends the row as a flat JSON object into dst straight from the live
// row — no Row clone. Used by the index → JSON read path; non-partitioned, as
// secondary indexes are. Appends nothing on a miss, so a caller probing several
// index candidates can pass the same dst each time.
func (t *table) appendMatchJSON(pk UUID, pred func(Row) bool, cols []string, ords []int, starAll bool, dst []byte) ([]byte, bool) {
	s := t.shardOf(pk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return dst, false
	}
	r := s.rows[rowID]
	if r == nil || !pred(r) {
		return dst, false
	}
	if starAll {
		return appendRowJSONObject(dst, cols, r), true
	}
	return appendRowJSONObjectProject(dst, cols, r, ords), true
}

// idxCandidateSet is the candidate-PK enumerator for an indexed SELECT: the
// (intersected) index hits UNION the dirty overlay. pks holds the slice
// secIndex.lookup returns (a fresh copy of the bucket — the live bucket must not
// escape the index lock) and dirty holds dirtyPKs()'s freshly-copied overlay, so
// the set owns its slices rather than aliasing index state. Returned by value,
// and emit takes its visitor as an argument rather than being a heap closure that
// captures the slices (which escaped). Shared by execSelectIdx (materialized) and
// selectEach (streaming) so the hybrid index∪dirty correctness has a single
// definition.
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

// execSelectIdx runs a SELECT whose WHERE pins one or more secondary-indexed
// columns by equality. It resolves candidate PKs through the index(es)
// (intersecting buckets for an AND of equalities) plus the dirty overlay, then
// evaluates the full WHERE on each live row. With an ORDER BY it gathers all
// matches and sorts before LIMIT (the filtered-list pattern); otherwise it
// projects and stops at LIMIT.
func (db *DB) execSelectIdx(pl *plan, args []Value) ([]string, []Row, error) {
	st := pl.st.(*selectStmt)
	if st.limit == 0 {
		return pl.colNames, nil, nil
	}
	// Point-read fast path: a single indexed equality that fully covers the WHERE
	// (idxExact), no ORDER BY, no dirty overlay, and a single live hit. Fetch and
	// project that one row directly — no eval context, bucket copy, candidate set,
	// or result packing. A multi-hit or any dirty row falls through to the general
	// path (which builds the context lazily below).
	if pl.idxExact && pl.orderOrdinal < 0 && len(pl.idxCols) == 1 && pl.rt.readDirtyCount.Load() == 0 {
		keyVal, err := evalLitOrParamValue(pl.idxSrcs[0], args)
		if err != nil {
			return nil, nil, err
		}
		if keyVal.IsNull() {
			return pl.colNames, nil, nil
		}
		if si := pl.rt.indexFor(pl.idxCols[0]); si != nil {
			pk, found, one := si.lookupOne(keyOf(keyVal))
			if !found {
				return pl.colNames, nil, nil
			}
			if one {
				if st.offset > 0 {
					return pl.colNames, nil, nil // the lone row is dropped by OFFSET
				}
				var pr Row
				var ok bool
				if st.starAll {
					pr, ok = pl.rt.getByPK(pk)
				} else {
					pr, ok = pl.rt.getByPKProject(pk, pl.projOrdinals)
				}
				if !ok {
					return pl.colNames, nil, nil
				}
				return pl.colNames, []Row{pr}, nil
			}
			// found && !one: a multi-hit bucket → fall through to the general path.
		}
	}
	// General path: a multi-hit / multi-column / ORDER BY / dirty-overlay index
	// read. Build the eval context once for candidate resolution + the WHERE check.
	ctx := &evalCtx{cols: pl.rt.def.colByName, args: args}
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
		pred := rowMatcher(st.where, ctx)
		// ORDER BY + LIMIT: a top-N heap clones only ~limit rows, so the cost
		// tracks the LIMIT, not the matched-set size (offer reads/clones the live
		// row under the shard lock). st.limit == 0 already returned above.
		if st.limit >= 0 {
			// Keep offset+limit rows so the offset can be dropped after sorting.
			top := &topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: fetchBound(st.limit, st.offset), proj: projOrNil(st.starAll, pl.projOrdinals)}
			cand.emit(func(pk UUID) bool {
				tbl.offerLiveRow(pk, pred, top)
				return false
			})
			return colNames, sliceOffsetLimit(top.sorted(), st.offset, st.limit), nil
		}
		// ORDER BY without LIMIT: capture only (order key + projection) per match
		// under the shard lock, then sort by the captured key (OFFSET drops the
		// leading rows). No full-row clone, no second projection pass.
		top := topN{ord: pl.orderOrdinal, desc: st.orderDesc, proj: projOrNil(st.starAll, pl.projOrdinals)}
		cand.emit(func(pk UUID) bool {
			tbl.collectLiveRow(pk, pred, &top)
			return false
		})
		return colNames, sliceOffsetLimit(top.sorted(), st.offset, st.limit), nil
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
	// A fresh index hit needs no re-check when the WHERE is exactly the indexed
	// equalities (idxExact) and nothing is dirty: the bucket is authoritative and a
	// deleted row is dropped by the fetch. Skipping the matcher also avoids
	// compiling it. With a dirty overlay (possibly stale entries) or a residual
	// conjunct, the full WHERE is re-checked per candidate.
	var pred func(Row) bool
	if !(pl.idxExact && len(cand.dirty) == 0) {
		pred = rowMatcher(st.where, ctx)
	}
	// Dirty-empty fast path: walk the index hits directly, no dedup map and no emit
	// closure (which would escape to the heap). The dirty union needs both.
	if len(cand.dirty) == 0 {
		for _, pk := range cand.pks {
			start := len(packed)
			var ok bool
			packed, ok = tbl.appendMatchProject(pk, pred, pl.projOrdinals, st.starAll, packed)
			if !ok {
				continue
			}
			out = append(out, Row(packed[start:len(packed):len(packed)]))
			if eff >= 0 && len(out) >= eff {
				break
			}
		}
		return colNames, sliceOffsetLimit(out, st.offset, st.limit), nil
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
	pred := rowMatcher(st.where, &ctx)
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
	// Reuse one cells buffer across every dirty row: buildDirtyCands calls dirtyKey
	// serially, and encodeCompositeKey copies the cells into a fresh key string, so
	// the buffer never escapes — one alloc per query instead of one per dirty row.
	cells := make([]Value, len(ords))
	dirtyKey = func(r Row) indexKey {
		for i, o := range ords {
			cells[i] = r[o]
		}
		return encodeCompositeKey(cells)
	}
	return si.snapshotPrefix(encodeCompositeKey(prefix)), dirtyKey, pl.compResidual, true, nil
}

// execSelectCompositeWalk serves WHERE <leading prefix> = ? ORDER BY <next col>
// via a composite ordered index: it walks the pinned-prefix sub-range of the
// sorted index — already ordered by the trailing column — and stops at LIMIT, so
// no sort runs. The walk reuses orderedWalk with a composite dirty key (the
// encoded tuple) so dirty rows merge into the same key space as the index
// entries. Every component is NOT NULL (planComposite's guard), so a matching
// row always has a fully-encodable key.
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
	if len(dirty) == 0 {
		// Steady state after a merge: no overlay to shadow or sort. Skip the empty
		// map alloc + sort — mergeOrderedStreams handles a nil dc and nil dirtySet
		// (a nil-map lookup reports "not shadowed").
		return nil, nil
	}
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
	slices.SortFunc(dc, func(a, b dcand) int {
		if a.key.less(b.key) {
			return -1
		}
		if b.key.less(a.key) {
			return 1
		}
		return 0
	})
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

// flipRangeOp mirrors a comparison so a `value OP col` predicate reads as
// `col flip(OP) value` (e.g. ? < age == age > ?).
func flipRangeOp(op tokenKind) tokenKind {
	switch op {
	case tkLt:
		return tkGt
	case tkLte:
		return tkGte
	case tkGt:
		return tkLt
	case tkGte:
		return tkLte
	}
	return op
}

// rangeBound is one side of a range constraint on a column: the evaluated bound
// value, its column-oriented operator, and whether it was present.
type rangeBound struct {
	val Value
	op  tokenKind
	ok  bool
}

// orderColumnRange scans the WHERE conjuncts for the lower (>= / >) and upper
// (< / <=) range bounds on the ORDER BY column (ordinal ord), evaluating each
// bound value. Either side may be absent. A `value OP col` predicate is read as
// `col flip(OP) value`. The first bound found on each side wins (a looser extra
// bound is harmless — the residual still filters).
func orderColumnRange(conj []expr, ord int, args []Value) (lo, hi rangeBound) {
	for _, c := range conj {
		b, isBin := c.(*binOp)
		if !isBin {
			continue
		}
		var op tokenKind
		var valExpr expr
		if cr, ok := b.lhs.(*colRef); ok && cr.ord == ord && isLitOrParam(b.rhs) {
			op, valExpr = b.op, b.rhs
		} else if cr, ok := b.rhs.(*colRef); ok && cr.ord == ord && isLitOrParam(b.lhs) {
			op, valExpr = flipRangeOp(b.op), b.lhs
		} else {
			continue
		}
		switch op {
		case tkGt, tkGte:
			if !lo.ok {
				if v, err := evalLitOrParamValue(valExpr, args); err == nil {
					lo = rangeBound{v, op, true}
				}
			}
		case tkLt, tkLte:
			if !hi.ok {
				if v, err := evalLitOrParamValue(valExpr, args); err == nil {
					hi = rangeBound{v, op, true}
				}
			}
		}
	}
	return
}

// seekOrderedSnap clips the ascending index snapshot to the window the WHERE's
// bounds on the ORDER BY column allow, so an ordered walk both STARTS at the lower
// bound and STOPS at the upper bound instead of scanning the rest of the index —
// each cut an O(log n) seek. The window is direction-independent: ASC walks it
// front-to-back, DESC back-to-front. The full WHERE stays the residual matcher, so
// a missing / kind-mismatched / NULL bound just skips that cut (the residual still
// filters); only entries that provably fail a bound are dropped.
func seekOrderedSnap(snap []ordEntry, conj []expr, ord int, args []Value) []ordEntry {
	if len(snap) == 0 {
		return snap
	}
	lo, hi := orderColumnRange(conj, ord, args)
	start, end := 0, len(snap)
	if lo.ok && !lo.val.IsNull() {
		if lk := keyOf(lo.val); lk.kind == snap[0].key.kind {
			if lo.op == tkGt { // first key > lo
				start = sort.Search(len(snap), func(i int) bool { return lk.less(snap[i].key) })
			} else { // first key >= lo
				start = sort.Search(len(snap), func(i int) bool { return !snap[i].key.less(lk) })
			}
		}
	}
	if hi.ok && !hi.val.IsNull() {
		if hk := keyOf(hi.val); hk.kind == snap[0].key.kind {
			if hi.op == tkLt { // first key >= hi
				end = sort.Search(len(snap), func(i int) bool { return !snap[i].key.less(hk) })
			} else { // first key > hi
				end = sort.Search(len(snap), func(i int) bool { return hk.less(snap[i].key) })
			}
		}
	}
	if start >= end { // empty or contradictory window
		return snap[:0]
	}
	return snap[start:end]
}

// orderedWalkArgs resolves the (snap, dirtyKey, residual) inputs orderedWalk
// needs for a single-column ordered walk. ok=false means a missing index. The
// index sits on the ORDER BY column, not the WHERE columns, so the snap
// guarantees nothing about the WHERE: the whole WHERE is residual. When the WHERE
// bounds the ORDER BY column by a range, the snap is clipped to that window so the
// walk skips the entries before AND after it. Shared by the materializing and
// streaming entry points.
func (db *DB) orderedWalkArgs(pl *plan, args []Value) (snap []ordEntry, dirtyKey func(Row) indexKey, residual []expr, ok bool) {
	si := pl.rt.indexFor(pl.orderOrdinal)
	if si == nil {
		return nil, nil, nil, false
	}
	ord := pl.orderOrdinal
	st := pl.st.(*selectStmt)
	collectConjuncts(st.where, &residual)
	snap = seekOrderedSnap(si.snapshot(), residual, ord, args)
	return snap, func(r Row) indexKey { return keyOf(r[ord]) }, residual, true
}

// execSelectOrderedWalk serves an ORDER BY on an ordered-indexed column by
// walking the sorted index in order, merged with the dirty overlay (rows
// mutated since the last merge), applying any residual WHERE, and stopping at
// LIMIT — touching ~LIMIT rows, not the whole table. A non-dirty index entry is
// fresh (its key equals the live value), so the index key drives the ordering
// and the row is fetched only when selected. Dirty PKs are excluded from the
// index walk (the entry may be stale) and supplied from their live rows.
func (db *DB) execSelectOrderedWalk(pl *plan, args []Value) ([]string, []Row, error) {
	snap, dirtyKey, residual, ok := db.orderedWalkArgs(pl, args)
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
	matches := rowMatcher(st.where, &ctx)            // full WHERE — filters dirty candidates
	passResidual := conjunctsMatcher(residual, &ctx) // conjuncts the snap doesn't guarantee
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
		// Residual present: filter under the shard lock and clone only the
		// projection in one pass (appendMatchProject), instead of cloning the whole
		// row (getByPK) and then cloning the projection again.
		n := len(pl.projOrdinals)
		if st.starAll {
			n = len(tbl.def.def.Columns)
		}
		if cells, ok := tbl.appendMatchProject(pk, passResidual, pl.projOrdinals, st.starAll, make([]Value, 0, n)); ok {
			out = append(out, cells)
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
	matches := rowMatcher(st.where, &ctx)            // full WHERE — filters dirty candidates
	passResidual := conjunctsMatcher(residual, &ctx) // conjuncts the snap doesn't guarantee
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
		if rowID, ok := s.pk.get(pk); ok {
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
	snap, dirtyKey, residual, ok := db.orderedWalkArgs(pl, args)
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

	// Fast path: PK equality — single map lookup, no scan, no sort. Evaluate
	// the key here (execSelect has the eval context), then delegate to the
	// point reader so the limit/offset/null/coerce/projection logic lives in
	// one place (execSelectPK, which db.go also routes PK queries to directly).
	if pl.pkLookup {
		keyVal, err := evalExpr(pl.pkSource, &ctx)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPK(pl, keyVal)
	}

	// Secondary-index lookup: resolve candidate PKs through the index, fetch
	// each by PK and project. No scan.
	if pl.idxLookup {
		return db.execSelectIdx(pl, ctx.args)
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

	// PK pinned inside an AND-chain (no bare-PK / index / partition path): fetch
	// the one PK-addressed row and return it only if the full WHERE matches —
	// O(1) instead of a scan. <=1 row, so ORDER BY needs no sort.
	if pl.pkProbe != nil {
		keyVal, err := evalExpr(pl.pkProbe, &ctx)
		if err != nil {
			return nil, nil, err
		}
		return db.execSelectPKResidual(pl, keyVal, args)
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

	// One predicate for the whole scan: a compiled fast path for simple WHERE
	// shapes (col = ?, ranges, AND/OR/NOT, IS NULL), else an evalExpr fallback.
	// Built once, not per row. nil for a partition-pinned scan: partLookup is set
	// only when the WHERE is exactly the PartitionKey equality (detectColEq matches
	// a bare equality, not an AND-chain), so every row the partition scan yields
	// already satisfies it — the per-row match would be pure overhead.
	var match func(Row) bool
	if !partPinned {
		match = rowMatcher(st.where, &ctx)
	}

	// No ORDER BY: project straight into one packed buffer during the scan (each
	// result Row is a capped view of its span) — no full-row clone, no second
	// projection pass. Works with or without a LIMIT: with one the scan stops at
	// offset+limit; without one (eff < 0) it gathers every match, still projecting
	// in place rather than cloning whole rows.
	if pl.orderOrdinal < 0 {
		if st.limit == 0 {
			return colNames, nil, nil
		}
		// Collect offset+limit rows during the scan, then drop the offset. Cap the
		// prealloc: a huge bound over a small result must not allocate a giant
		// slice up front, and a no-LIMIT scan starts empty and grows; append grows
		// past the hint only if that many match.
		eff := fetchBound(st.limit, st.offset)
		capHint := eff
		if capHint < 0 {
			capHint = 0 // no LIMIT — grow from empty
		} else if capHint > 1024 {
			capHint = 1024
		}
		out := make([]Row, 0, capHint)
		width := len(pl.projOrdinals)
		if st.starAll {
			width = len(tbl.def.def.Columns)
		}
		var packed []Value
		// The partition-pinned and all-shards loops below share an identical
		// per-row body (match → packed projection → capped view → limit check),
		// differing only in the row source. It is left inlined on purpose:
		// factoring the body into a helper measured slower on the scan path — the
		// helper does not inline past the clone calls, so every matched row pays a
		// call.
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
				// match is nil here (partition-pinned): every partition row matches.
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
				if eff >= 0 && len(out) >= eff {
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
				if !match(r) {
					continue
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
				if eff >= 0 && len(out) >= eff {
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
		// Scan the partition in the ORDER BY direction: a DESC walk reads the tail
		// newest-first so a top-N heap fills with the highest keys and rejects the
		// rest clone-free (the common "recent N in a partition" shape). Correct for
		// any data — only heap churn differs.
		if st.orderDesc {
			scan = func(fn func(Row) bool) { tbl.scanPartitionRev(part, fn) }
		} else {
			scan = func(fn func(Row) bool) { tbl.scanPartition(part, fn) }
		}
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
		top := topN{ord: pl.orderOrdinal, desc: st.orderDesc, capN: fetchBound(st.limit, st.offset), proj: projOrNil(st.starAll, pl.projOrdinals)}
		if match == nil { // partition-pinned: every scanned row matches
			scan(func(r Row) bool { top.offer(r); return true })
		} else {
			scan(func(r Row) bool {
				if match(r) {
					top.offer(r)
				}
				return true
			})
		}
		return colNames, sliceOffsetLimit(top.sorted(), st.offset, st.limit), nil
	}

	// ORDER BY without LIMIT: capture only (order key + projection) per match —
	// the packed form topN keeps — instead of a full-row clone we'd narrow again.
	// For a wide table with a narrow projection that drops the per-match copy from
	// the whole row to the projected cells, and removes the second projection pass.
	// (orderOrdinal >= 0 here: the no-ORDER-BY case returned via the packed path above.)
	top := topN{ord: pl.orderOrdinal, desc: st.orderDesc, proj: projOrNil(st.starAll, pl.projOrdinals)}
	if match == nil { // partition-pinned: every scanned row matches
		scan(func(r Row) bool { top.collect(r); return true })
	} else {
		scan(func(r Row) bool {
			if match(r) {
				top.collect(r)
			}
			return true
		})
	}
	return colNames, sliceOffsetLimit(top.sorted(), st.offset, st.limit), nil
}

// sortRowsByCol sorts rows by column ord (ascending, or descending when desc).
// Incomparable cells (NULL) compare equal. Unstable: SQL leaves the order of
// equal ORDER BY keys unspecified, so the stability bookkeeping is pure cost —
// the same reasoning that put topN.sorted on slices.SortFunc.
func sortRowsByCol(rows []Row, ord int, desc bool) {
	slices.SortFunc(rows, func(a, b Row) int {
		c, ok := a[ord].Compare(b[ord])
		if !ok {
			return 0 // incomparable (NULL): treat as equal
		}
		if desc {
			return -c
		}
		return c
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
// desc), materialising a row only when it makes the cut. Backed by a binary heap
// whose root is the most-evictable kept row, so a candidate that cannot beat the
// root is dropped without copying. Each kept entry stores the row already
// PROJECTED to the output columns (proj) plus the sort key captured before
// projection — so the heap holds output-width rows, not the full (possibly
// joined) input row, and no second projection pass is needed. proj nil keeps the
// whole row (SELECT *).
type topN struct {
	ord  int
	desc bool
	capN int
	proj []int
	h    []topEntry
	// backing carves every kept row from one slice (each topEntry.row a fixed-width
	// window into it) so a bounded fixed-projection top-N allocates the kept-row
	// storage once instead of once per row that enters. Slots are reused as the
	// heap evicts. nil for SELECT * (variable width) or an unbounded/huge capN.
	backing []Value
}

// topPackCap bounds the packed-backing prealloc: above it, a top-N that may keep
// few rows would waste a large contiguous buffer, so fall back to per-row clones.
const topPackCap = 1024

// topEntry is one kept row plus its sort key, captured before projection so the
// projected row need not contain the order column.
type topEntry struct {
	key Value
	row Row
}

// evictable reports whether key a ranks below b under the ORDER BY (a drops first).
func (t *topN) evictable(a, b Value) bool {
	c, ok := a.Compare(b)
	if !ok {
		return false
	}
	if t.desc {
		return c < 0 // DESC keeps the largest, so the smaller drops first
	}
	return c > 0 // ASC keeps the smallest, so the larger drops first
}

// take materialises the kept form of r: the projected output columns (deep
// copied, so it never aliases reused scratch or arena storage), or a full clone
// when proj is nil (SELECT *).
func (t *topN) take(r Row) Row {
	if t.proj == nil {
		return r.Clone()
	}
	pr := make(Row, len(t.proj))
	for j, ord := range t.proj {
		pr[j] = cloneValue(r[ord])
	}
	return pr
}

// packs reports whether kept rows are carved from the shared backing (a bounded,
// fixed-width projection) rather than allocated per row.
func (t *topN) packs() bool { return t.proj != nil && t.capN > 0 && t.capN <= topPackCap }

// projInto deep-copies r's projected columns into dst (len == len(proj)).
func (t *topN) projInto(dst, r Row) {
	for j, ord := range t.proj {
		dst[j] = cloneValue(r[ord])
	}
}

// fillSlot returns the kept form of r for the fill phase at heap position i:
// backing slot i when packing, else a fresh allocation.
func (t *topN) fillSlot(r Row, i int) Row {
	if !t.packs() {
		return t.take(r)
	}
	w := len(t.proj)
	if t.backing == nil {
		t.backing = make([]Value, t.capN*w)
	}
	off := i * w
	slot := t.backing[off : off+w : off+w]
	t.projInto(slot, r)
	return slot
}

// reuseSlot writes r's projection into the evicted root's window (no allocation),
// or allocates when not packing.
func (t *topN) reuseSlot(r, slot Row) Row {
	if !t.packs() {
		return t.take(r)
	}
	t.projInto(slot, r)
	return slot
}

func (t *topN) offer(r Row) {
	key := cloneValue(r[t.ord]) // capture before projection; cloned so reused scratch can't mutate it
	if t.h == nil && t.packs() {
		t.h = make([]topEntry, 0, t.capN) // presize: a bounded heap never reallocs
	}
	if len(t.h) < t.capN {
		t.h = append(t.h, topEntry{key, t.fillSlot(r, len(t.h))})
		for i := len(t.h) - 1; i > 0; {
			p := (i - 1) / 2
			if !t.evictable(t.h[i].key, t.h[p].key) {
				break
			}
			t.h[i], t.h[p] = t.h[p], t.h[i]
			i = p
		}
		return
	}
	if !t.evictable(t.h[0].key, key) { // r can't beat the current worst
		return
	}
	t.h[0] = topEntry{key, t.reuseSlot(r, t.h[0].row)}
	for i, n := 0, len(t.h); ; {
		worst, l, rr := i, 2*i+1, 2*i+2
		if l < n && t.evictable(t.h[l].key, t.h[worst].key) {
			worst = l
		}
		if rr < n && t.evictable(t.h[rr].key, t.h[worst].key) {
			worst = rr
		}
		if worst == i {
			break
		}
		t.h[i], t.h[worst] = t.h[worst], t.h[i]
		i = worst
	}
}

// collect appends r's kept form (captured order key + projection) with no
// bounded-heap eviction — the ORDER BY without LIMIT case, where every match is
// kept and the final order comes from sorted(), not the heap.
func (t *topN) collect(r Row) {
	t.h = append(t.h, topEntry{cloneValue(r[t.ord]), t.take(r)})
}

// sorted returns the kept rows in ORDER BY order (already projected). Ties sort
// arbitrarily — SQL leaves the order of equal ORDER BY keys unspecified — so an
// unstable sort is used (faster, no stability bookkeeping).
func (t *topN) sorted() []Row {
	slices.SortFunc(t.h, func(a, b topEntry) int {
		c, ok := a.key.Compare(b.key)
		if !ok {
			return 0 // incomparable (NULL): treat as equal
		}
		if t.desc {
			return -c
		}
		return c
	})
	out := make([]Row, len(t.h))
	for i := range t.h {
		out[i] = t.h[i].row
	}
	return out
}

// projOrNil is the projection ordinals, or nil for SELECT * (keep all columns).
func projOrNil(starAll bool, ords []int) []int {
	if starAll {
		return nil
	}
	return ords
}
