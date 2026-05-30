package hazedb

import "sync"

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

// rebuildIndexes rebuilds every secondary index from a full scan of live rows.
// Used after bulk writes (whose per-row deltas are not tracked incrementally)
// and after WAL replay. Not atomic w.r.t. concurrent writes; callers that need
// that serialise externally.
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
}
