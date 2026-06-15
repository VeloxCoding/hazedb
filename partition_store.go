package hazedb

import "sync"

// Partitioned-table storage. A partitioned table routes rows to shards by the
// PartitionKey value (so all rows of one partition value land in one shard),
// and a table-wide pkDirectory maps PK → location. The per-shard pk map is
// absent here; uniqueness and WHERE id=? go through the directory.
//
// Global lock order (invariant): pkDirectory → data shard → walMu. Every
// method below acquires in that order and releases in reverse.

// rowLocation pins a row: which shard's arena, and the rowID within it. rowIDs
// are stable while a row lives; the background compactor (compact.go) can renumber
// a shard's rows, but only under the pkDirectory + shard write locks (the lock
// order above), updating every location in lockstep — and a stale location surfaces
// as a tombstone / PK mismatch that forces a directory re-lookup, so it never
// silently aliases a different live row.
type rowLocation struct {
	shard uint32
	rowID uint64
}

// pkDirectory is the table-wide PK → location index for a partitioned table.
// It enforces table-wide PK uniqueness (partitioned shards route by
// PartitionKey, so no single shard sees all PKs) and answers WHERE id=?
// without scanning every shard.
type pkDirectory struct {
	mu  sync.RWMutex
	idx map[UUID]rowLocation
}

// pkRetryBudget bounds the release-then-retry on a stale read location. rowIDs
// never reuse, so a single retry resolves the common case; the bound only
// guards a pathological move-storm.
const pkRetryBudget = 8

// insertPartitioned routes by PartitionKey value, rejects a duplicate PK via
// the directory, journals, appends the row, and records its location — under
// the directory lock then the shard lock (then walMu inside the journal). A
// duplicate is rejected before journaling; a WAL failure aborts before the
// row is applied. j may be the zero value (replay / memory-only).
func (t *table) insertPartitioned(row Row, j mutJournal) error {
	pk := row[t.def.pkOrdinal].UUID()
	part := row[t.def.partitionOrdinal].UUID()
	idx := t.shardIdxOf(part)
	s := &t.shards[idx]

	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	if _, exists := t.pkDir.idx[pk]; exists {
		return ErrDuplicatePK
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cost := rowCost(row, len(t.indexes))
	if !t.budget.reserve(cost) {
		return ErrCapacity
	}
	if err := j.insert(row); err != nil {
		t.budget.release(cost) // un-reserve: the row is not added
		return err
	}
	rowID := s.addRowLocked(row, cost)
	s.tails[part] = append(s.tails[part], rowID)
	t.pkDir.idx[pk] = rowLocation{shard: idx, rowID: rowID}
	return nil
}

// scanPartition visits every live row of one partition value in insert order,
// reading only that partition's rowID list (O(partition size), not O(table)).
// fn returning false stops the scan. Holds the owning shard's read lock.
func (t *table) scanPartition(part UUID, fn func(Row) bool) {
	s := t.shardOf(part)
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rowID := range s.tails[part] {
		if rowID >= uint64(len(s.rows)) {
			continue
		}
		r := s.rows[rowID]
		if r == nil {
			continue // tombstoned
		}
		if !fn(r) {
			return
		}
	}
}

// scanPartitionRev visits the partition's live rows newest-insertion-first — the
// reverse of scanPartition. For an ORDER BY DESC on a column that grows with
// insertion (a sequence, a timestamp), this hands a top-N heap the highest keys
// first, so the rest are rejected without a clone instead of each evicting the
// heap root. Correct for any data; only the clone/CPU count differs.
func (t *table) scanPartitionRev(part UUID, fn func(Row) bool) {
	s := t.shardOf(part)
	s.mu.RLock()
	defer s.mu.RUnlock()
	tail := s.tails[part]
	for i := len(tail) - 1; i >= 0; i-- {
		rowID := tail[i]
		if rowID >= uint64(len(s.rows)) {
			continue
		}
		r := s.rows[rowID]
		if r == nil {
			continue // tombstoned
		}
		if !fn(r) {
			return
		}
	}
}

// getByPKPartitioned resolves PK → location, reads under the shard lock, and
// re-resolves on a tombstone / PK-mismatch. Re-resolving (not returning
// not-found from a stale location) is required: a DELETE+INSERT of the same PK
// committed atomically moves the row, and a reader holding the old location
// must observe the new row, not a phantom disappearance.
func (t *table) getByPKPartitioned(pk UUID) (Row, bool) {
	for retry := 0; retry < pkRetryBudget; retry++ {
		t.pkDir.mu.RLock()
		loc, ok := t.pkDir.idx[pk]
		t.pkDir.mu.RUnlock()
		if !ok {
			return nil, false
		}
		s := &t.shards[loc.shard]
		s.mu.RLock()
		var cl Row
		if loc.rowID < uint64(len(s.rows)) {
			if r := s.rows[loc.rowID]; r != nil && r[t.def.pkOrdinal].UUID() == pk {
				cl = r.Clone() // validate + clone UNDER the lock
			}
		}
		s.mu.RUnlock()
		if cl != nil {
			return cl, true
		}
		// Stale location (deleted or moved) → re-resolve via the directory.
	}
	return nil, false
}

// getByPKProjectIntoPartitioned is the scan-into form of
// getByPKProjectPartitioned: it appends projected cells into dst (caller-owned
// and reused) instead of allocating a Row. Same resolve + stale-location retry.
func (t *table) getByPKProjectIntoPartitioned(pk UUID, ords []int, dst []Value) ([]Value, bool) {
	for retry := 0; retry < pkRetryBudget; retry++ {
		t.pkDir.mu.RLock()
		loc, ok := t.pkDir.idx[pk]
		t.pkDir.mu.RUnlock()
		if !ok {
			return dst[:0], false
		}
		s := &t.shards[loc.shard]
		s.mu.RLock()
		out, found := dst[:0], false
		if loc.rowID < uint64(len(s.rows)) {
			if r := s.rows[loc.rowID]; r != nil && r[t.def.pkOrdinal].UUID() == pk {
				if ords == nil {
					out = appendRowClone(out, r)
				} else {
					out = appendProjectClone(out, r, ords)
				}
				found = true
			}
		}
		s.mu.RUnlock()
		if found {
			return out, true
		}
		// Stale location (deleted or moved) → re-resolve via the directory.
	}
	return dst[:0], false
}

// getByPKJSONIntoPartitioned is getByPKJSONInto for a partitioned table: same
// resolve + retry-on-stale-location loop as getByPKProjectIntoPartitioned, but
// it appends the row as a flat JSON object into dst under the shard lock instead
// of cloning cells. dst is reset to its entry length on each retry so a stale
// first attempt leaves no partial object behind.
func (t *table) getByPKJSONIntoPartitioned(pk UUID, cols []string, ords []int, dst []byte) ([]byte, bool) {
	base := len(dst)
	for retry := 0; retry < pkRetryBudget; retry++ {
		t.pkDir.mu.RLock()
		loc, ok := t.pkDir.idx[pk]
		t.pkDir.mu.RUnlock()
		if !ok {
			return dst[:base], false
		}
		s := &t.shards[loc.shard]
		s.mu.RLock()
		out, found := dst[:base], false
		if loc.rowID < uint64(len(s.rows)) {
			if r := s.rows[loc.rowID]; r != nil && r[t.def.pkOrdinal].UUID() == pk {
				if ords == nil {
					out = appendRowJSONObject(out, cols, r)
				} else {
					out = appendRowJSONObjectProject(out, cols, r, ords)
				}
				found = true
			}
		}
		s.mu.RUnlock()
		if found {
			return out, true
		}
		// Stale location (deleted or moved) → re-resolve via the directory.
	}
	return dst[:base], false
}

// getByPKProjectPartitioned is getByPKPartitioned for a projected SELECT:
// same resolve + retry-on-stale-location loop, but it clones only the ords
// columns under the shard read lock instead of the whole row.
func (t *table) getByPKProjectPartitioned(pk UUID, ords []int) (Row, bool) {
	for retry := 0; retry < pkRetryBudget; retry++ {
		t.pkDir.mu.RLock()
		loc, ok := t.pkDir.idx[pk]
		t.pkDir.mu.RUnlock()
		if !ok {
			return nil, false
		}
		s := &t.shards[loc.shard]
		s.mu.RLock()
		var pr Row
		if loc.rowID < uint64(len(s.rows)) {
			if r := s.rows[loc.rowID]; r != nil && r[t.def.pkOrdinal].UUID() == pk {
				pr = projectClone(r, ords)
			}
		}
		s.mu.RUnlock()
		if pr != nil {
			return pr, true
		}
		// Stale location (deleted or moved) → re-resolve via the directory.
	}
	return nil, false
}

// updateByPKJournaledPartitioned applies a PK-pinned update. It holds the
// directory read lock across the shard write so a concurrent move (which needs
// the directory write lock) cannot invalidate the location mid-update. PK and
// PartitionKey are immutable, so the row never changes shard. Hot path is
// allocation-free (stack-saved revert on WAL failure); a zero j = no
// save/revert.
func (t *table) updateByPKJournaledPartitioned(pk UUID, ords []int, compute func(Row) ([]Value, error), j mutJournal) (bool, error) {
	t.pkDir.mu.RLock()
	defer t.pkDir.mu.RUnlock()
	loc, ok := t.pkDir.idx[pk]
	if !ok {
		return false, nil
	}
	s := &t.shards[loc.shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if loc.rowID >= uint64(len(s.rows)) {
		return false, nil
	}
	r := s.rows[loc.rowID]
	if r == nil || r[t.def.pkOrdinal].UUID() != pk {
		return false, nil
	}
	vals, err := compute(r)
	if err != nil {
		return false, err
	}
	if !j.live() {
		var delta int64
		for i, ord := range ords {
			delta += cellDelta(r[ord], vals[i])
			r[ord] = vals[i]
		}
		s.addBytesLocked(delta, t.budget)
		return true, nil
	}
	var saved [8]Value
	old := saved[:0]
	for _, ord := range ords {
		old = append(old, r[ord])
	}
	for i, ord := range ords {
		r[ord] = vals[i]
	}
	if err := j.update(r); err != nil {
		for i, ord := range ords {
			r[ord] = old[i]
		}
		return false, err
	}
	var delta int64
	for i := range ords {
		delta += cellDelta(old[i], vals[i])
	}
	s.addBytesLocked(delta, t.budget)
	return true, nil
}

func (t *table) updateByPKOneJournaledPartitioned(pk UUID, ord int, compute func(Row) (Value, error), j mutJournal) (bool, error) {
	t.pkDir.mu.RLock()
	defer t.pkDir.mu.RUnlock()
	loc, ok := t.pkDir.idx[pk]
	if !ok {
		return false, nil
	}
	s := &t.shards[loc.shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if loc.rowID >= uint64(len(s.rows)) {
		return false, nil
	}
	r := s.rows[loc.rowID]
	if r == nil || r[t.def.pkOrdinal].UUID() != pk {
		return false, nil
	}
	val, err := compute(r)
	if err != nil {
		return false, err
	}
	if !j.live() {
		s.addBytesLocked(cellDelta(r[ord], val), t.budget)
		r[ord] = val
		return true, nil
	}
	old := r[ord]
	r[ord] = val
	if err := j.update(r); err != nil {
		r[ord] = old
		return false, err
	}
	s.addBytesLocked(cellDelta(old, val), t.budget)
	return true, nil
}

// updatePartitioned is the replay-path update (no journal). Same invariant
// guard as update(): mutate must return a non-nil row with an unchanged PK, or
// it returns false (→ ErrWALCorrupt) rather than corrupting the directory.
func (t *table) updatePartitioned(pk UUID, mutate func(Row) Row) bool {
	t.pkDir.mu.RLock()
	defer t.pkDir.mu.RUnlock()
	loc, ok := t.pkDir.idx[pk]
	if !ok {
		return false
	}
	s := &t.shards[loc.shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if loc.rowID >= uint64(len(s.rows)) {
		return false
	}
	// Capture the old cost before mutate runs — it may edit the row in place and
	// return the same slice (see update()).
	nIdx := len(t.indexes)
	before := rowCost(s.rows[loc.rowID], nIdx)
	nr := mutate(s.rows[loc.rowID])
	if nr == nil || nr[t.def.pkOrdinal].UUID() != pk {
		return false
	}
	s.addBytesLocked(rowCost(nr, nIdx)-before, t.budget)
	s.rows[loc.rowID] = nr
	return true
}

// deleteByPKJournaledPartitioned holds the directory WRITE lock across the
// whole op (so the location is stable: a move needs the same lock), journals,
// tombstones the row, and removes the directory entry.
func (t *table) deleteByPKJournaledPartitioned(pk UUID, j mutJournal) (bool, error) {
	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	loc, ok := t.pkDir.idx[pk]
	if !ok {
		return false, nil
	}
	s := &t.shards[loc.shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := j.delete(); err != nil {
		return false, err
	}
	if loc.rowID < uint64(len(s.rows)) {
		part := s.rows[loc.rowID][t.def.partitionOrdinal].UUID()
		s.tombstoneLocked(loc.rowID, len(t.indexes), t.budget)
		s.tailsTombstone(part)
	}
	delete(t.pkDir.idx, pk)
	return true, nil
}

// deleteByPKPartitioned is the replay-path delete (no journal).
func (t *table) deleteByPKPartitioned(pk UUID) bool {
	ok, _ := t.deleteByPKJournaledPartitioned(pk, mutJournal{})
	return ok
}

// deleteWhereAllPartitioned tombstones every matching row across the table,
// holding the directory write lock and every shard lock for the whole
// operation, and removes each deleted PK from the directory. Two-pass with a
// single TXN envelope, same atomicity as deleteWhereAll: collect → journal once
// → apply. A WAL failure aborts with nothing applied.
func (t *table) deleteWhereAllPartitioned(match func(Row) bool, encode func(pk Value) []byte, journalAll func([][]byte) error) (int, error) {
	pkOrd := t.def.pkOrdinal
	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	t.lockAllShards()
	defer t.unlockAllShards()
	partOrd := t.def.partitionOrdinal
	type pendingDelete struct {
		s    *tableShard
		j    int
		pk   UUID
		part UUID
	}
	var pending []pendingDelete
	var bodies [][]byte
	for i := range t.shards {
		s := &t.shards[i]
		for j, r := range s.rows {
			if r == nil || !match(r) {
				continue
			}
			pending = append(pending, pendingDelete{s, j, r[pkOrd].UUID(), r[partOrd].UUID()})
			if encode != nil {
				bodies = append(bodies, encode(r[pkOrd]))
			}
		}
	}
	if journalAll != nil && len(bodies) > 0 {
		if err := journalAll(bodies); err != nil {
			return 0, err
		}
	}
	for _, p := range pending {
		p.s.tombstoneLocked(uint64(p.j), len(t.indexes), t.budget)
		p.s.tailsTombstone(p.part)
		delete(t.pkDir.idx, p.pk)
	}
	return len(pending), nil
}
