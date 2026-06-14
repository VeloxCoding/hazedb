package hazedb

import (
	"bytes"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Secondary indexes — optional value->PK lookup structures on a non-PK column,
// declared in DDL (INDEX (col)). See docs/secondary-indexes.md for the full
// design (async maintenance + hybrid reads). This file holds the in-memory
// structures and the async merger: the write path only marks a PK dirty
// (markDirtyLocked, store.go), and the background merger reconciles the indexes
// against the live rows off the write path.
//
// CALLER CONTRACT — an index is NOT a complete row source for a nullable column.
// NULL cells are never indexed (keyFromCells), so a row with a NULL in an indexed
// column is absent from that index. Any path that walks an index as the WHOLE row
// source (an ORDER BY walk, a scan substitute) MUST guard nullable columns with
// the planner's anyNullable and fall back to a scan, or it silently drops the NULL
// rows. Equality lookups are exempt — a NULL never '='-matches anything.

// indexKey is a comparable, value-typed encoding of an indexed cell, usable as
// a Go map key (Value carries an unsafe.Pointer and is not itself comparable).
// NULL is never indexed (it is not '='-queryable), so a NULL key is only ever a
// sentinel, never stored.
//
// Payload by kind (mirrors Value's word packing, so keyOf needs no conversion):
//   - int  : w0 holds the value, compared SIGNED (int64(w0)).
//   - bool : w0 is 0/1.
//   - uuid : w0/w1 are the two big-endian words, compared UNSIGNED high-then-low
//     (big-endian => word order == byte order == UUID order). No [16]byte
//     round-trip and no allocation, unlike a string-encoded key.
//   - string/bytes : s holds the content, compared bytewise. A KindString key
//     aliases the row's immutable backing; KindBytes copies.
type indexKey struct {
	kind ValueKind
	w0   uint64
	w1   uint64
	s    string
}

func keyOf(v Value) indexKey {
	switch v.Kind {
	case KindInt:
		return indexKey{kind: KindInt, w0: uint64(v.Int())}
	case KindBool:
		var b uint64
		if v.Bool() {
			b = 1
		}
		return indexKey{kind: KindBool, w0: b}
	case KindString:
		return indexKey{kind: KindString, s: v.Str()}
	case KindBytes:
		return indexKey{kind: KindBytes, s: string(v.Bytes())}
	case KindUUID:
		hi, lo := v.uuidWords()
		return indexKey{kind: KindUUID, w0: hi, w1: lo}
	}
	return indexKey{kind: KindNull}
}

// less orders two keys of the SAME kind (all keys in one index share a kind):
// signed for int (bool's 0/1 rides along), the two big-endian words high-then-low
// and UNSIGNED for uuid (so word order == UUID byte order), lexicographic for
// string/bytes.
func (k indexKey) less(o indexKey) bool {
	switch k.kind {
	case KindInt, KindBool:
		return int64(k.w0) < int64(o.w0)
	case KindUUID:
		if k.w0 != o.w0 {
			return k.w0 < o.w0
		}
		return k.w1 < o.w1
	default:
		return k.s < o.s
	}
}

// ordEntry is one (key, pk) pair in an ordered index's sorted slice.
type ordEntry struct {
	key indexKey
	pk  UUID
}

// secIndex is one secondary index: a forward map value->PKs and a reverse map
// PK->current key, so a change can drop the stale forward entry without the
// caller supplying the old value. Guarded by mu.
type secIndex struct {
	ordinals []int // one column for a single-column index, more for a composite
	ordered  bool  // sorted index (equality + ranges + ORDER BY); see O2
	mu       sync.RWMutex
	fwd      map[indexKey][]UUID // hash mode: value -> PKs
	rev      map[UUID]indexKey   // pk -> current key (both modes)
	sorted   []ordEntry          // ordered mode: rev sorted by key, rebuilt on merge
}

func newSecIndex(ri resolvedIndex) *secIndex {
	return &secIndex{
		ordinals: ri.ordinals,
		ordered:  ri.ordered,
		fwd:      make(map[indexKey][]UUID),
		rev:      make(map[UUID]indexKey),
	}
}

// keyFromCells builds the index key from the indexed cells (one per ordinal, in
// declaration order). indexable=false when any cell is NULL — a NULL is never
// indexed, so the row drops out of this index entirely. Single-column reuses the
// scalar keyOf (no allocation, unchanged hot path); composite encodes the tuple.
func (si *secIndex) keyFromCells(cells []Value) (indexKey, bool) {
	k, ok, _ := si.keyFromCellsInto(cells, nil)
	return k, ok
}

// keyFromCellsInto is keyFromCells with a reusable encode buffer: it returns the
// (grown) buffer so a merge encoding many composite keys reuses one scratch and
// allocates only the per-key string. The single-column path ignores buf.
func (si *secIndex) keyFromCellsInto(cells []Value, buf []byte) (indexKey, bool, []byte) {
	if len(si.ordinals) == 1 {
		if cells[0].Kind == KindNull {
			return indexKey{}, false, buf
		}
		return keyOf(cells[0]), true, buf
	}
	for i := range cells {
		if cells[i].Kind == KindNull {
			return indexKey{}, false, buf
		}
	}
	k, buf := encodeCompositeKeyInto(buf, cells)
	return k, true, buf
}

// apply records pk's current indexed key. indexable=false means the row is gone
// or its value is NULL: any prior entry is removed. The reverse map supplies the
// old key, so callers never need to remember the pre-change value. Returns
// whether this index actually changed — false for a no-op (same key re-set, or a
// drop of a PK that was not indexed). For an ordered index the caller uses that
// to skip the sorted-view merge when nothing moved (mergeSorted is O(n) even with
// an empty change set).
func (si *secIndex) apply(pk UUID, newKey indexKey, indexable bool) (changed bool) {
	si.mu.Lock()
	if si.ordered {
		// rev is authoritative; the sorted view is folded once per merge.
		old, had := si.rev[pk]
		if indexable {
			if had && old == newKey {
				si.mu.Unlock()
				return false // key unchanged: sorted view unaffected
			}
			si.rev[pk] = newKey
		} else {
			if !had {
				si.mu.Unlock()
				return false // was not indexed (e.g. NULL value): nothing to drop
			}
			delete(si.rev, pk)
		}
		si.mu.Unlock()
		return true
	}
	old, had := si.rev[pk]
	if had && indexable && old == newKey {
		si.mu.Unlock()
		return false // key unchanged: fwd already holds pk under it — skip the remove+append churn
	}
	if had {
		si.removeFwdLocked(old, pk)
		delete(si.rev, pk)
	}
	if indexable {
		si.fwd[newKey] = append(si.fwd[newKey], pk)
		si.rev[pk] = newKey
	}
	si.mu.Unlock()
	return true
}

// ordCmp is the total order on the sorted view: by key, then PK as a stable
// tie-break among equal keys. Returns <0/0/>0 for slices.SortFunc (which uses a
// generic swapper — no reflect, unlike sort.Slice). Shared by the full rebuild
// and the incremental merge.
func ordCmp(a, b ordEntry) int {
	if a.key.less(b.key) {
		return -1
	}
	if b.key.less(a.key) {
		return 1
	}
	return bytes.Compare(a.pk[:], b.pk[:])
}

// ordLess is ordCmp as a bool, for the merge's 2-way comparisons.
func ordLess(a, b ordEntry) bool { return ordCmp(a, b) < 0 }

// rebuildSorted regenerates the whole sorted view from rev — O(n log n). Used for
// the cold build (after WAL replay / rebuildIndexes), where there is no prior
// sorted view to fold into. The per-merge hot path uses mergeSorted instead.
// Ordered indexes only.
func (si *secIndex) rebuildSorted() {
	si.mu.Lock()
	s := make([]ordEntry, 0, len(si.rev))
	for pk, key := range si.rev {
		s = append(s, ordEntry{key: key, pk: pk})
	}
	slices.SortFunc(s, ordCmp)
	si.sorted = s
	si.mu.Unlock()
}

// mergeSorted folds a merge batch's changed PKs into the sorted view
// incrementally: it sorts only the (≤ d) changed entries and 2-way-merges them
// with the existing sorted slice, skipping every old entry whose PK changed — an
// update repositions it (old skipped, new in add), a delete drops it (skipped,
// absent from rev → not in add), an insert has no old entry. O(n + d log d) vs
// rebuildSorted's O(n log n) — the win for a write-heavy large ordered index
// whose merges each touch few rows. A FRESH slice is allocated, so the old one
// stays valid for lock-free in-flight readers (snapshot semantics unchanged).
// dirtyPKs is the set of PKs applied this merge; duplicates are tolerated.
func (si *secIndex) mergeSorted(dirtyPKs []UUID) {
	si.mu.Lock()
	defer si.mu.Unlock()
	dirty := make(map[UUID]struct{}, len(dirtyPKs))
	for _, pk := range dirtyPKs {
		dirty[pk] = struct{}{}
	}
	// add: the changed PKs still live, at their current key, sorted. Iterating the
	// dedup set (not dirtyPKs) means a PK touched twice yields at most one entry.
	add := make([]ordEntry, 0, len(dirty))
	for pk := range dirty {
		if key, ok := si.rev[pk]; ok {
			add = append(add, ordEntry{key: key, pk: pk})
		}
	}
	slices.SortFunc(add, ordCmp)

	old := si.sorted
	out := make([]ordEntry, 0, len(old)+len(add))
	i, j := 0, 0
	for i < len(old) && j < len(add) {
		if _, changed := dirty[old[i].pk]; changed {
			i++ // superseded or removed; the live version (if any) is in add
			continue
		}
		if ordLess(old[i], add[j]) {
			out = append(out, old[i])
			i++
		} else {
			out = append(out, add[j])
			j++
		}
	}
	for ; i < len(old); i++ {
		if _, changed := dirty[old[i].pk]; !changed {
			out = append(out, old[i])
		}
	}
	out = append(out, add[j:]...)
	si.sorted = out
}

// removeFwdLocked drops pk from k's bucket (swap-remove; order is irrelevant),
// deleting the bucket when empty. Caller holds mu.
func (si *secIndex) removeFwdLocked(k indexKey, pk UUID) {
	b := si.fwd[k]
	for i := range b {
		if b[i] == pk {
			last := len(b) - 1
			b[i] = b[last]
			b = b[:last]
			break
		}
	}
	if len(b) == 0 {
		delete(si.fwd, k)
	} else {
		si.fwd[k] = b
	}
}

// lookup returns a copy of the PKs for k (copied so the caller never aliases a
// bucket a concurrent writer may mutate).
func (si *secIndex) lookup(k indexKey) []UUID {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if si.ordered {
		// first entry with key >= k, then walk the equal-key run.
		lo := sort.Search(len(si.sorted), func(i int) bool { return !si.sorted[i].key.less(k) })
		var out []UUID
		for i := lo; i < len(si.sorted); i++ {
			e := si.sorted[i]
			if k.less(e.key) {
				break // past the equal range
			}
			out = append(out, e.pk)
		}
		return out
	}
	b := si.fwd[k]
	var out []UUID
	if len(b) > 0 {
		out = make([]UUID, len(b))
		copy(out, b)
	}
	return out
}

// lookupOne resolves k to a single PK without allocating. found reports whether
// any entry has key k; one reports whether EXACTLY one does. The single-row
// indexed-delete fast path uses it to skip lookup's []UUID slice; a multi-hit
// bucket (found && !one) falls back to the candidate path. Single-column indexes
// only — the caller guarantees one indexed equality.
func (si *secIndex) lookupOne(k indexKey) (pk UUID, found, one bool) {
	si.mu.RLock()
	defer si.mu.RUnlock()
	if si.ordered {
		lo := sort.Search(len(si.sorted), func(i int) bool { return !si.sorted[i].key.less(k) })
		if lo >= len(si.sorted) || k.less(si.sorted[lo].key) {
			return UUID{}, false, false // no entry == k
		}
		// sorted[lo].key == k. Exactly one iff the next entry has a different key.
		if lo+1 < len(si.sorted) && !k.less(si.sorted[lo+1].key) {
			return UUID{}, true, false // >= 2 in the equal run
		}
		return si.sorted[lo].pk, true, true
	}
	switch b := si.fwd[k]; len(b) {
	case 0:
		return UUID{}, false, false
	case 1:
		return b[0], true, true
	default:
		return UUID{}, true, false
	}
}

// prefixLookup returns the PKs whose composite key starts with prefix.s (the
// encoded leading columns of a composite index — e.g. (a = ?) on an (a, b)
// index), in sorted-key order. The sorted view groups all keys sharing a byte
// prefix contiguously, so a binary search to the lower bound then a forward walk
// while the prefix matches enumerates them. Ordered indexes only. Like lookup,
// it reads the merged view only; the caller unions the dirty overlay.
func (si *secIndex) prefixLookup(prefix indexKey) []UUID {
	si.mu.RLock()
	defer si.mu.RUnlock()
	lo := sort.Search(len(si.sorted), func(i int) bool { return si.sorted[i].key.s >= prefix.s })
	var out []UUID
	for i := lo; i < len(si.sorted); i++ {
		if !strings.HasPrefix(si.sorted[i].key.s, prefix.s) {
			break // past the prefix run
		}
		out = append(out, si.sorted[i].pk)
	}
	return out
}

// snapshotPrefix returns the contiguous slice of the sorted view whose keys
// start with prefix.s — the (a = ?) sub-range of a composite (a, b) index,
// already ordered by the trailing column(s). The returned header is a stable,
// immutable view (rebuildSorted never mutates in place), walkable without mu.
func (si *secIndex) snapshotPrefix(prefix indexKey) []ordEntry {
	si.mu.RLock()
	s := si.sorted
	si.mu.RUnlock()
	lo := sort.Search(len(s), func(i int) bool { return s[i].key.s >= prefix.s })
	// Upper bound by binary search too — a linear scan to the end of the prefix
	// run would be O(bucket), defeating the walk's "touch ~LIMIT rows" goal. ub is
	// the least string strictly greater than every string with this prefix.
	hi := len(s)
	if ub := prefixUpperBound(prefix.s); ub != "" {
		hi = sort.Search(len(s), func(i int) bool { return s[i].key.s >= ub })
	}
	return s[lo:hi]
}

// prefixUpperBound returns the least string strictly greater than every string
// starting with p (p with its last non-0xFF byte incremented, trailing 0xFF
// bytes dropped). "" means p is all-0xFF (no finite upper bound — walk to end).
func prefixUpperBound(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// intersectPKs returns the PKs present in both a and b. It builds a set from
// the smaller slice and probes it with the larger, so the cost is O(|a|+|b|)
// with no row fetches. Index buckets hold each PK once, so no de-duplication is
// needed. Used to combine two indexes' buckets (name = ? AND city = ?) before
// fetching any row.
func intersectPKs(a, b []UUID) []UUID {
	if len(a) > len(b) {
		a, b = b, a
	}
	if len(a) == 0 {
		return nil
	}
	set := make(map[UUID]struct{}, len(a))
	for _, pk := range a {
		set[pk] = struct{}{}
	}
	out := make([]UUID, 0, len(a))
	for _, pk := range b {
		if _, ok := set[pk]; ok {
			out = append(out, pk)
		}
	}
	return out
}

// snapshot returns the current sorted view of an ordered index. rebuildSorted
// always assigns a NEW slice (never mutates in place), so the returned header is
// a stable, immutable view the caller can walk without holding mu.
func (si *secIndex) snapshot() []ordEntry {
	si.mu.RLock()
	s := si.sorted
	si.mu.RUnlock()
	return s
}

// indexFor returns the table's secondary index on column ord, or nil.
func (t *table) indexFor(ord int) *secIndex {
	for _, si := range t.indexes {
		if len(si.ordinals) == 1 && si.ordinals[0] == ord {
			return si
		}
	}
	return nil
}

// probeIndexFor returns an index usable to probe equality on column ord: a
// single-column index on ord, or a composite whose LEADING column is ord (an
// (ord, ...) prefix lookup yields the same PKs). Used by joins, which can probe
// through a composite's leading column. First match wins.
func (t *table) probeIndexFor(ord int) *secIndex {
	for _, si := range t.indexes {
		if len(si.ordinals) >= 1 && si.ordinals[0] == ord {
			return si
		}
	}
	return nil
}

// lookupLeading returns the PKs whose leading indexed column equals key: a direct
// bucket lookup for a single-column index, or a prefix lookup on the encoded
// leading value for a composite.
func (si *secIndex) lookupLeading(key Value) []UUID {
	if len(si.ordinals) == 1 {
		return si.lookup(keyOf(key))
	}
	return si.prefixLookup(encodeCompositeKey([]Value{key}))
}

// countKey returns how many PKs a single-column index holds for key, without
// allocating a candidate slice (unlike lookup). Hash: O(1) bucket length. Ordered:
// the equal-key run sized by two binary searches (hi - lo), O(log N) and flat in
// bucket size. Used by COUNT(*) WHERE col = ? when the index is authoritative (no
// pending dirty overlay).
func (si *secIndex) countKey(key Value) int {
	si.mu.RLock()
	defer si.mu.RUnlock()
	k := keyOf(key)
	if !si.ordered {
		return len(si.fwd[k])
	}
	// equal-range: [lo, hi) is the run of entries whose key == k.
	lo := sort.Search(len(si.sorted), func(i int) bool { return !si.sorted[i].key.less(k) })
	hi := sort.Search(len(si.sorted), func(i int) bool { return k.less(si.sorted[i].key) })
	return hi - lo
}

// indexByOrdinals returns the secondary index whose columns are exactly ords, in
// order, or nil. Locates a composite index chosen at plan time (stored as its
// ordinal list on the plan).
func (t *table) indexByOrdinals(ords []int) *secIndex {
	for _, si := range t.indexes {
		if len(si.ordinals) != len(ords) {
			continue
		}
		match := true
		for i := range ords {
			if si.ordinals[i] != ords[i] {
				match = false
				break
			}
		}
		if match {
			return si
		}
	}
	return nil
}

// startMergeLoop launches the background merger: every interval it reconciles
// all indexed tables. Mirrors the WAL flush ticker. Started by Open; stopped by
// Close (which runs a final drain first).
func (db *DB) startMergeLoop(interval time.Duration, threshold int64) {
	db.mergeStop = make(chan struct{})
	db.mergeDone = make(chan struct{})
	go func() {
		defer close(db.mergeDone)
		// Size-trigger disabled (threshold < 0): tick once per interval, merge
		// every tick — the pure time-trigger. Active (threshold >= 0, adaptive or
		// fixed): poll at interval/pollDiv and merge as soon as the overlay grows
		// dense, OR the full interval elapses (a freshness floor) — whichever comes
		// first. The merger reads the dirty counters itself, so the size-trigger
		// never adds work to the write path.
		const pollDiv = 10
		sizeActive := threshold >= 0
		poll := interval
		if sizeActive && interval/pollDiv > 0 {
			poll = interval / pollDiv
		}
		ticksPerInterval := int(interval / poll)
		if ticksPerInterval < 1 {
			ticksPerInterval = 1
		}
		t := time.NewTicker(poll)
		defer t.Stop()
		ticks := 0
		for {
			select {
			case <-db.mergeStop:
				db.mergeIndexes() // final drain so a clean Close leaves no lag
				return
			case <-t.C:
				ticks++
				sizeHit := sizeActive && db.sizeTriggerFired(threshold)
				if sizeHit || ticks >= ticksPerInterval {
					db.mergeIndexes()
					ticks = 0
				}
			}
		}
	}()
}

// totalDirty sums the pending dirty-PK count (read + delete entries — both need
// merging) across every table in the current catalog. Cheap (two atomic loads
// per table) — the merger's size-trigger poll.
func (db *DB) totalDirty() int64 {
	var n int64
	for _, rt := range db.cat.Load().byID {
		if rt != nil {
			n += rt.table.readDirtyCount.Load() + rt.table.delDirtyCount.Load()
		}
	}
	return n
}

// sizeTriggerFired reports whether any table's dirty overlay has grown dense
// enough to merge before the interval elapses. fixed>0 uses an absolute total
// threshold; fixed==0 is ADAPTIVE — a table fires when its overlay reaches a
// quarter of its live rows. The floor skips the per-table liveCount sweep for
// small overlays (cheap to walk anyway) and stops a near-empty table from
// merge-spamming.
func (db *DB) sizeTriggerFired(fixed int64) bool {
	if fixed > 0 {
		return db.totalDirty() >= fixed
	}
	const floor = 256
	for _, rt := range db.cat.Load().byID {
		if rt == nil {
			continue
		}
		d := rt.table.readDirtyCount.Load() + rt.table.delDirtyCount.Load()
		if d < floor {
			continue // overlay tiny: cheap to walk, no early merge
		}
		thr := int64(rt.table.liveCount() / 4)
		if thr < floor {
			thr = floor
		}
		if d >= thr {
			return true
		}
	}
	return false
}

// stopMergeLoop signals the merger to drain once and exit, then waits for it.
// Idempotent; safe on a DB whose loop was never started.
func (db *DB) stopMergeLoop() {
	if db.mergeStop == nil {
		return
	}
	close(db.mergeStop)
	<-db.mergeDone
	db.mergeStop = nil
}

// mergeIndexes reconciles every indexed table in the current catalog. The
// explicit trigger (S4); the background loop (S5) drives it on a ticker.
func (db *DB) mergeIndexes() {
	cat := db.cat.Load()
	for _, rt := range cat.byID {
		if rt != nil && len(rt.indexes) > 0 {
			rt.mergeIndexes()
		}
	}
}

// dirtyPKs returns every PK marked dirty (mutated since the last merge) across
// all shards. The read overlay unions these with the index hits, so a
// not-yet-merged row is still a candidate (then re-checked against its live
// row). Copied out under each shard's read lock.
func (t *table) dirtyPKs() []UUID {
	if t.readDirtyCount.Load() == 0 {
		return nil // no read-relevant dirty (deletes don't count): skip the scan
	}
	var out []UUID
	partitioned := t.pkDir != nil
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		// Walk only dirtyRead (inserts + indexed updates); deletes live in dirtyDel
		// and never belong in the read overlay. The s.pk liveness check still drops
		// the rare insert-then-delete pair (the insert's PK lingers in dirtyRead but
		// its row is already gone). Partitioned shards have no per-shard pk map
		// (they route through pkDir), so they keep the unfiltered behaviour.
		if partitioned {
			out = append(out, s.dirtyRead...)
		} else {
			for _, pk := range s.dirtyRead {
				if _, live := s.pk.get(pk); live {
					out = append(out, pk)
				}
			}
		}
		s.mu.RUnlock()
	}
	return out
}

// mergeIndexes reconciles the secondary indexes with the live rows for every
// dirty PK, then clears the processed dirty entries. It takes each shard lock
// only briefly (to snapshot, and later to drop the processed prefix); the
// recompute itself runs against a lock-free getByPK.
//
// No-gap ordering: dirty entries are snapshotted and applied to the indexes
// (hash: into fwd; ordered: into rev + the sorted view rebuilt) BEFORE they are
// dropped, so a concurrent read sees a row via the dirty overlay until it is in
// the index — never in the gap between. Entries appended during the merge stay
// and are reconciled next time.
func (t *table) mergeIndexes() {
	if len(t.indexes) == 0 {
		return
	}
	t.mergeMu.Lock()
	defer t.mergeMu.Unlock()
	// The merge reads only the indexed columns, so project just those into a
	// reused scratch buffer instead of cloning the whole row per dirty PK
	// (getByPK copies every column, incl. any large BYTES payload). ords[k] is
	// the column ordinal of t.indexes[k], so the projected row lines up 1:1 with
	// the index list.
	// Flatten every index's component ordinals into one projection list and
	// remember each index's slice within it, so one getByPKProjectInto fetches all
	// the cells every index needs (single-column contributes one ordinal,
	// composite several). spans[k] selects index k's cells from the projected row.
	var ords []int
	type span struct{ off, n int }
	spans := make([]span, len(t.indexes))
	for k, si := range t.indexes {
		spans[k] = span{off: len(ords), n: len(si.ordinals)}
		ords = append(ords, si.ordinals...)
	}
	var scratch []Value
	var encBuf []byte // reused across rows so composite encoding allocs only the key string
	type pending struct {
		s      *tableShard
		nr, nd int
	}
	var drops []pending
	// changedPerIdx[k] collects only the PKs whose key actually moved in ordered
	// index k this merge — so an index untouched by the batch (writes hit a
	// hash-indexed or NULL-valued column, or a no-op SET) skips its O(n) sorted-view
	// fold entirely, and the folds that do run get the minimal change set.
	changedPerIdx := make([][]UUID, len(t.indexes))
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		nr := len(s.dirtyRead)
		nd := len(s.dirtyDel)
		var batch []UUID
		if nr > 0 {
			batch = append(batch, s.dirtyRead[:nr]...)
		}
		if nd > 0 {
			batch = append(batch, s.dirtyDel[:nd]...)
		}
		s.mu.RUnlock()
		if len(batch) == 0 {
			continue
		}
		for _, pk := range batch {
			pr, ok := t.getByPKProjectInto(pk, ords, scratch)
			scratch = pr // reuse the buffer next iteration
			for k, si := range t.indexes {
				if !ok {
					if si.apply(pk, indexKey{}, false) && si.ordered {
						changedPerIdx[k] = append(changedPerIdx[k], pk)
					}
					continue
				}
				cells := pr[spans[k].off : spans[k].off+spans[k].n]
				key, indexable, b := si.keyFromCellsInto(cells, encBuf)
				encBuf = b
				if si.apply(pk, key, indexable) && si.ordered {
					changedPerIdx[k] = append(changedPerIdx[k], pk)
				}
			}
		}
		drops = append(drops, pending{s, nr, nd})
	}
	if len(drops) == 0 {
		return
	}
	// Fold ordered indexes' changed PKs into the sorted view BEFORE dropping
	// dirty, so a reader sees a row via the dirty overlay until it lands in the
	// sorted view (no gap). Incremental: O(n + d log d), not a full re-sort.
	for k, si := range t.indexes {
		if si.ordered && len(changedPerIdx[k]) > 0 {
			si.mergeSorted(changedPerIdx[k])
		}
	}
	for _, d := range drops {
		d.s.mu.Lock()
		// Copy the un-merged tail down to the front rather than reslicing forward:
		// s[nr:] advances the start so the consumed prefix is dead capacity until a
		// realloc, and a steady write/merge cycle keeps growing the backing. Copying
		// down reuses it from index 0.
		d.s.dirtyRead = append(d.s.dirtyRead[:0], d.s.dirtyRead[d.nr:]...)
		d.s.dirtyDel = append(d.s.dirtyDel[:0], d.s.dirtyDel[d.nd:]...)
		d.s.mu.Unlock()
		t.readDirtyCount.Add(-int64(d.nr))
		t.delDirtyCount.Add(-int64(d.nd))
	}
}

// rebuildAllIndexes rebuilds every indexed table in the current catalog. Called
// once after WAL replay so the indexes are correct and ready before serving,
// regardless of how the rows were loaded (replay today, a snapshot later).
func (db *DB) rebuildAllIndexes() {
	cat := db.cat.Load()
	for _, rt := range cat.byID {
		if rt != nil && len(rt.indexes) > 0 {
			rt.rebuildIndexes()
		}
	}
}

// rebuildIndexes rebuilds every secondary index from a full scan of live rows
// and clears the dirty lists (the rebuild supersedes any pending deltas). Used
// after WAL replay. Not atomic w.r.t. concurrent writes; callers serialise (it
// runs at boot, before the merger and request serving start).
func (t *table) rebuildIndexes() {
	if len(t.indexes) == 0 {
		return
	}
	for _, si := range t.indexes {
		si.mu.Lock()
		si.fwd = make(map[indexKey][]UUID)
		si.rev = make(map[UUID]indexKey)
		si.sorted = nil
		si.mu.Unlock()
	}
	pkOrd := t.def.pkOrdinal
	var comp []Value // reused composite-component scratch (single-column stays alloc-free)
	t.scanAll(func(r Row) bool {
		pk := r[pkOrd].UUID()
		for _, si := range t.indexes {
			if len(si.ordinals) == 1 {
				if v := r[si.ordinals[0]]; v.Kind != KindNull {
					si.apply(pk, keyOf(v), true)
				}
				continue
			}
			comp = comp[:0]
			for _, ord := range si.ordinals {
				comp = append(comp, r[ord])
			}
			if key, ok := si.keyFromCells(comp); ok {
				si.apply(pk, key, true)
			}
		}
		return true
	})
	for _, si := range t.indexes {
		if si.ordered {
			si.rebuildSorted()
		}
	}
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.dirtyRead = nil // replay marked rows dirty; the full rebuild covers them
		s.dirtyDel = nil
		s.mu.Unlock()
	}
	t.readDirtyCount.Store(0)
	t.delDirtyCount.Store(0)
}
