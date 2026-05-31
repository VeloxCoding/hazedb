package hazedb

import (
	"bytes"
	"sort"
	"sync"
	"time"
)

// Secondary indexes — optional value->PK lookup structures on a non-PK column,
// declared in DDL (INDEX (col)). See docs/secondary-indexes.md for the full
// design (async maintenance + hybrid reads). This file holds the in-memory
// structures and the async merger: the write path only marks a PK dirty
// (markDirtyLocked, store.go), and the background merger reconciles the indexes
// against the live rows off the write path.

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

func uuidLess(a, b UUID) bool { return bytes.Compare(a[:], b[:]) < 0 }

// secIndex is one secondary index: a forward map value->PKs and a reverse map
// PK->current key, so a change can drop the stale forward entry without the
// caller supplying the old value. Guarded by mu.
type secIndex struct {
	ordinal int
	ordered bool // sorted index (equality + ranges + ORDER BY); see O2
	mu      sync.RWMutex
	fwd     map[indexKey][]UUID // hash mode: value -> PKs
	rev     map[UUID]indexKey   // pk -> current key (both modes)
	sorted  []ordEntry          // ordered mode: rev sorted by key, rebuilt on merge
}

func newSecIndex(ri resolvedIndex) *secIndex {
	return &secIndex{
		ordinal: ri.ordinal,
		ordered: ri.ordered,
		fwd:     make(map[indexKey][]UUID),
		rev:     make(map[UUID]indexKey),
	}
}

// apply records pk's current indexed key. indexable=false means the row is gone
// or its value is NULL: any prior entry is removed. The reverse map supplies the
// old key, so callers never need to remember the pre-change value.
func (si *secIndex) apply(pk UUID, newKey indexKey, indexable bool) {
	si.mu.Lock()
	if si.ordered {
		// rev is authoritative; the sorted view is rebuilt once per merge.
		if indexable {
			si.rev[pk] = newKey
		} else {
			delete(si.rev, pk)
		}
		si.mu.Unlock()
		return
	}
	if old, had := si.rev[pk]; had {
		si.removeFwdLocked(old, pk)
		delete(si.rev, pk)
	}
	if indexable {
		si.fwd[newKey] = append(si.fwd[newKey], pk)
		si.rev[pk] = newKey
	}
	si.mu.Unlock()
}

// rebuildSorted regenerates the sorted view from rev. The merger calls it once
// after applying a batch (and recovery after a full scan), before dropping the
// dirty entries, so a reader sees a row via the dirty overlay until it is in the
// sorted view (no gap). Ordered indexes only. O(n log n).
func (si *secIndex) rebuildSorted() {
	si.mu.Lock()
	s := make([]ordEntry, 0, len(si.rev))
	for pk, key := range si.rev {
		s = append(s, ordEntry{key: key, pk: pk})
	}
	sort.Slice(s, func(i, j int) bool {
		if s[i].key.less(s[j].key) {
			return true
		}
		if s[j].key.less(s[i].key) {
			return false
		}
		return uuidLess(s[i].pk, s[j].pk) // stable tie-break among equal keys
	})
	si.sorted = s
	si.mu.Unlock()
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
		if si.ordinal == ord {
			return si
		}
	}
	return nil
}

// startMergeLoop launches the background merger: every interval it reconciles
// all indexed tables. Mirrors the WAL flush ticker. Started by Open; stopped by
// Close (which runs a final drain first).
func (db *DB) startMergeLoop(interval time.Duration) {
	db.mergeStop = make(chan struct{})
	db.mergeDone = make(chan struct{})
	go func() {
		defer close(db.mergeDone)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-db.mergeStop:
				db.mergeIndexes() // final drain so a clean Close leaves no lag
				return
			case <-t.C:
				db.mergeIndexes()
			}
		}
	}()
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
	if t.dirtyCount.Load() == 0 {
		return nil // steady state: skip the 32-shard scan entirely
	}
	var out []UUID
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		if len(s.dirty) > 0 {
			out = append(out, s.dirty...)
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
	ords := make([]int, len(t.indexes))
	for k, si := range t.indexes {
		ords[k] = si.ordinal
	}
	var scratch []Value
	type pending struct {
		s *tableShard
		n int
	}
	var drops []pending
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		n := len(s.dirty)
		var batch []UUID
		if n > 0 {
			batch = append(batch, s.dirty[:n]...)
		}
		s.mu.RUnlock()
		if n == 0 {
			continue
		}
		for _, pk := range batch {
			pr, ok := t.getByPKProjectInto(pk, ords, scratch)
			scratch = pr // reuse the buffer next iteration
			for k, si := range t.indexes {
				if !ok {
					si.apply(pk, indexKey{}, false)
					continue
				}
				v := pr[k]
				si.apply(pk, keyOf(v), v.Kind != KindNull)
			}
		}
		drops = append(drops, pending{s, n})
	}
	if len(drops) == 0 {
		return
	}
	// Rebuild ordered indexes' sorted view from rev BEFORE dropping dirty.
	for _, si := range t.indexes {
		if si.ordered {
			si.rebuildSorted()
		}
	}
	for _, d := range drops {
		d.s.mu.Lock()
		d.s.dirty = d.s.dirty[d.n:] // drop the processed prefix; later appends remain
		d.s.mu.Unlock()
		t.dirtyCount.Add(-int64(d.n))
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
	t.scanAll(func(r Row) bool {
		pk := r[pkOrd].UUID()
		for _, si := range t.indexes {
			if v := r[si.ordinal]; v.Kind != KindNull {
				si.apply(pk, keyOf(v), true)
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
		s.dirty = nil // replay marked rows dirty; the full rebuild covers them
		s.mu.Unlock()
	}
	t.dirtyCount.Store(0)
}
