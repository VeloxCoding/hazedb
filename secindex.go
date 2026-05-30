package hazedb

import (
	"sync"
	"time"
)

// Secondary indexes — optional value->PK lookup structures on a non-PK column,
// declared in DDL (INDEX (col)). See docs/secondary-indexes.md for the full
// design (async maintenance + hybrid reads). This file holds the in-memory
// structure and the synchronous-maintenance baseline (S2); later steps move
// maintenance off the write path.

// indexKey is a comparable, value-typed encoding of an indexed cell, usable as
// a Go map key (Value carries an unsafe.Pointer and is not itself comparable).
// NULL is never indexed (it is not '='-queryable), so a NULL key is only ever a
// sentinel, never stored. Go strings are immutable, so a KindString key may
// alias the row's backing safely; KindBytes/KindUUID copy.
type indexKey struct {
	kind ValueKind
	i    int64
	s    string
}

func keyOf(v Value) indexKey {
	switch v.Kind {
	case KindInt:
		return indexKey{kind: KindInt, i: v.Int()}
	case KindBool:
		var b int64
		if v.Bool() {
			b = 1
		}
		return indexKey{kind: KindBool, i: b}
	case KindString:
		return indexKey{kind: KindString, s: v.Str()}
	case KindBytes:
		return indexKey{kind: KindBytes, s: string(v.Bytes())}
	case KindUUID:
		u := v.UUID()
		return indexKey{kind: KindUUID, s: string(u[:])}
	}
	return indexKey{kind: KindNull}
}

// secIndex is one secondary index: a forward map value->PKs and a reverse map
// PK->current key, so a change can drop the stale forward entry without the
// caller supplying the old value. Guarded by mu. UNIQUE is a read hint (the
// operator promises <=1 row per value, enabling early-exit) — NOT an enforced
// constraint, since enforcement cannot be synchronous once maintenance is async.
type secIndex struct {
	ordinal int
	unique  bool
	mu      sync.RWMutex
	fwd     map[indexKey][]UUID
	rev     map[UUID]indexKey
}

func newSecIndex(ri resolvedIndex) *secIndex {
	return &secIndex{
		ordinal: ri.ordinal,
		unique:  ri.unique,
		fwd:     make(map[indexKey][]UUID),
		rev:     make(map[UUID]indexKey),
	}
}

// apply records pk's current indexed key. indexable=false means the row is gone
// or its value is NULL: any prior entry is removed. The reverse map supplies the
// old key, so callers never need to remember the pre-change value.
func (si *secIndex) apply(pk UUID, newKey indexKey, indexable bool) {
	si.mu.Lock()
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
	b := si.fwd[k]
	var out []UUID
	if len(b) > 0 {
		out = make([]UUID, len(b))
		copy(out, b)
	}
	si.mu.RUnlock()
	return out
}

// getByPKCheckProject is the hybrid read's per-candidate fetch: under the shard
// read lock it confirms the row's checkOrd column STILL equals wantKey (the live
// re-check that makes a stale index entry harmless), and only then clones/
// projects. Returns (nil,false) when the row is absent or no longer matches.
// ords nil means all columns. Indexed tables are never partitioned, so there is
// no pkDir branch.
func (t *table) getByPKCheckProject(pk UUID, checkOrd int, wantKey indexKey, ords []int) (Row, bool) {
	s := t.shardOf(pk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return nil, false
	}
	r := s.rows[rowID]
	if r == nil || keyOf(r[checkOrd]) != wantKey {
		return nil, false
	}
	if ords == nil {
		return r.Clone(), true
	}
	return projectClone(r, ords), true
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

// idxApply updates every secondary index for one single-row change. newRow nil
// means the row was deleted. Called off the shard lock; each index has its own
// mu. The synchronous-maintenance baseline (S2) — moved off the write path in
// S4.
func (t *table) idxApply(pk UUID, newRow Row) {
	for _, si := range t.indexes {
		if newRow == nil {
			si.apply(pk, indexKey{}, false)
			continue
		}
		v := newRow[si.ordinal]
		si.apply(pk, keyOf(v), v.Kind != KindNull)
	}
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
// No-gap ordering: the dirty entries are snapshotted but NOT cleared before the
// index is updated, so a concurrent read sees a row via dirty until it is in the
// index — never in the gap between. Entries appended during the merge stay
// (positions >= n) and are reconciled next time.
func (t *table) mergeIndexes() {
	if len(t.indexes) == 0 {
		return
	}
	t.mergeMu.Lock()
	defer t.mergeMu.Unlock()
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
			if r, ok := t.getByPK(pk); ok {
				t.idxApply(pk, r)
			} else {
				t.idxApply(pk, nil)
			}
		}
		s.mu.Lock()
		s.dirty = s.dirty[n:] // drop the processed prefix; later appends remain
		s.mu.Unlock()
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
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.dirty = nil // replay marked rows dirty; the full rebuild covers them
		s.mu.Unlock()
	}
}
