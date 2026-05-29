package hazedb

import "sync"

// Partitioned-table storage. A partitioned table routes rows to shards by the
// PartitionKey value (so all rows of one partition value land in one shard),
// and a table-wide pkDirectory maps PK → location. The per-shard pk map is
// absent here; uniqueness and WHERE id=? go through the directory.
//
// Global lock order (invariant): pkDirectory → data shard → walMu. Every
// method below acquires in that order and releases in reverse.

// rowLocation pins a row: which shard's arena, and the rowID within it.
// rowIDs are append-only and never reused before a snapshot restart, so a
// stale location is detectable (tombstone / PK mismatch) and never silently
// aliases a different live row.
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
// the directory lock then the shard lock (then walMu inside journal). A
// duplicate is rejected before journaling; a WAL failure aborts before the
// row is applied. journal may be nil (replay / memory-only).
func (t *table) insertPartitioned(row Row, journal func() error) error {
	pk := row[t.def.pkOrdinal].U
	part := row[t.def.partitionOrdinal].U
	idx := t.shardIdxOf(part)
	s := &t.shards[idx]

	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	if _, exists := t.pkDir.idx[pk]; exists {
		return ErrDuplicatePK
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if journal != nil {
		if err := journal(); err != nil {
			return err
		}
	}
	rowID := uint64(len(s.rows))
	s.rows = append(s.rows, row)
	s.live++
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
		var r Row
		if loc.rowID < uint64(len(s.rows)) {
			r = s.rows[loc.rowID]
		}
		s.mu.RUnlock()
		if r != nil && r[t.def.pkOrdinal].U == pk {
			return r, true
		}
		// Stale location (deleted or moved) → re-resolve via the directory.
	}
	return nil, false
}

// updateByPKJournaledPartitioned applies a PK-pinned update. It holds the
// directory read lock across the shard write so a concurrent move (which needs
// the directory write lock) cannot invalidate the location mid-update. PK and
// PartitionKey are immutable, so the row never changes shard. Hot path is
// allocation-free (stack-saved revert on WAL failure); journal nil = no
// save/revert.
func (t *table) updateByPKJournaledPartitioned(pk UUID, ords []int, compute func(Row) ([]Value, error), journal func(Row) error) (bool, error) {
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
	if r == nil || r[t.def.pkOrdinal].U != pk {
		return false, nil
	}
	vals, err := compute(r)
	if err != nil {
		return false, err
	}
	if journal == nil {
		for i, ord := range ords {
			r[ord] = vals[i]
		}
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
	if err := journal(r); err != nil {
		for i, ord := range ords {
			r[ord] = old[i]
		}
		return false, err
	}
	return true, nil
}

// updatePartitioned is the replay-path update (no journal).
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
	s.rows[loc.rowID] = mutate(s.rows[loc.rowID])
	return true
}

// deleteByPKJournaledPartitioned holds the directory WRITE lock across the
// whole op (so the location is stable: a move needs the same lock), journals,
// tombstones the row, and removes the directory entry.
func (t *table) deleteByPKJournaledPartitioned(pk UUID, journal func() error) (bool, error) {
	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	loc, ok := t.pkDir.idx[pk]
	if !ok {
		return false, nil
	}
	s := &t.shards[loc.shard]
	s.mu.Lock()
	defer s.mu.Unlock()
	if journal != nil {
		if err := journal(); err != nil {
			return false, err
		}
	}
	if loc.rowID < uint64(len(s.rows)) {
		s.rows[loc.rowID] = nil
		s.live--
	}
	delete(t.pkDir.idx, pk)
	return true, nil
}

// deleteByPKPartitioned is the replay-path delete (no journal).
func (t *table) deleteByPKPartitioned(pk UUID) bool {
	ok, _ := t.deleteByPKJournaledPartitioned(pk, nil)
	return ok
}

// deleteWhereAllPartitioned tombstones every matching row across the table,
// holding the directory write lock and every shard lock for the whole
// operation (same serializability requirement as the non-partitioned
// deleteWhereAll), and removes each deleted PK from the directory. journal
// runs under the locks, before each tombstone.
func (t *table) deleteWhereAllPartitioned(match func(Row) bool, journal func(pk Value) error) (int, error) {
	pkOrd := t.def.pkOrdinal
	t.pkDir.mu.Lock()
	defer t.pkDir.mu.Unlock()
	t.lockAllShards()
	defer t.unlockAllShards()
	count := 0
	for i := range t.shards {
		s := &t.shards[i]
		for j, r := range s.rows {
			if r == nil {
				continue
			}
			if !match(r) {
				continue
			}
			if journal != nil {
				if err := journal(r[pkOrd]); err != nil {
					return count, err
				}
			}
			delete(t.pkDir.idx, r[pkOrd].U)
			s.rows[j] = nil
			s.live--
			count++
		}
	}
	return count, nil
}
