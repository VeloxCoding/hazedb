package hazedb

// Dead-slot lifecycle: tombstoning and arena compaction. Tombstones nil a row
// in place (rowIDs stay stable for the indexes that point at them); the
// background sweeper (compact.go) later reclaims dense shards through
// compactShardLocked. Core types live in store.go.

// minTailPrune is the smallest tails list worth pruning — below it the dead
// entries cost less than the rebuild.
const minTailPrune = 16

// tombstoneLocked nil-marks rowID, drops the live count, subtracts the removed
// row's byte cost from the per-shard tally, and releases those bytes back to the
// budget b. Caller holds s.mu and has confirmed s.rows[rowID] is a live row.
func (s *tableShard) tombstoneLocked(rowID uint64, nIdx int, b *byteBudget) {
	cost := rowCost(s.rows[rowID], nIdx)
	s.bytes -= cost
	s.rows[rowID] = nil
	s.live--
	b.release(cost)
}

// tailsTombstone records that one row of partition `part` was just tombstoned and
// prunes the partition's tails list once dead rowIDs reach half of it. Pruning
// drops only the dead entries (arena slots and rowIDs are untouched, so pkDir
// locations stay valid) and resets the dead count, so the next prune fires only
// after ~half the survivors die again — amortized O(1) per delete, keeping
// scanPartition O(live) instead of O(ever-inserted). Caller holds s.mu. No-op for
// non-partitioned shards (tailsDead is nil; this is only called on the
// partitioned delete paths).
func (s *tableShard) tailsTombstone(part UUID) {
	s.tailsDead[part]++
	ids := s.tails[part]
	if s.tailsDead[part]*2 < len(ids) || len(ids) < minTailPrune {
		return
	}
	live := ids[:0] // filter in place: kept rowIDs only ever move toward the front
	for _, rid := range ids {
		if rid < uint64(len(s.rows)) && s.rows[rid] != nil {
			live = append(live, rid)
		}
	}
	if len(live) == 0 {
		delete(s.tails, part)
		delete(s.tailsDead, part)
		return
	}
	s.tails[part] = live
	s.tailsDead[part] = 0
}

// compactShardLocked reclaims dead arena slots: it rebuilds s.rows with only the
// live rows, renumbering rowIDs to fill the gaps, and rewrites every rowID
// reference — the pkMap for a non-partitioned table, or the pkDirectory entries
// for this shard plus the per-partition tails lists for a partitioned one. Live
// rows keep their relative (insert) order, so scanPartition order is preserved.
//
// Secondary indexes and the ordered sorted view are PK-keyed (not rowID), so they
// need no update; rowIDs are not persisted, so the WAL/replay are unaffected. The
// byte tally and live count are unchanged (only dead slots go away).
//
// Caller holds s.mu (write) and, for a partitioned table, t.pkDir.mu (write) — the
// global lock order. Readers are excluded for the duration (they take those
// locks); a partitioned reader that cached a now-stale rowLocation re-resolves via
// the directory on the PK-mismatch retry, so the renumber is transparent to it.
func (t *table) compactShardLocked(s *tableShard, shardIdx uint32) {
	newRows := make([]Row, 0, s.live)
	if t.pkDir != nil {
		partOrd, pkOrd := t.def.partitionOrdinal, t.def.pkOrdinal
		s.tails = make(map[UUID][]uint64, len(s.tails))
		s.tailsDead = make(map[UUID]int)
		for _, r := range s.rows {
			if r == nil {
				continue
			}
			id := uint64(len(newRows))
			newRows = append(newRows, r)
			part := r[partOrd].UUID()
			s.tails[part] = append(s.tails[part], id)
			t.pkDir.idx[r[pkOrd].UUID()] = rowLocation{shard: shardIdx, rowID: id}
		}
		s.rows = newRows
		return
	}
	pkOrd := t.def.pkOrdinal
	s.pk = pkMap{}
	s.pk.init(s.live)
	for _, r := range s.rows {
		if r == nil {
			continue
		}
		id := uint64(len(newRows))
		newRows = append(newRows, r)
		s.pk.put(r[pkOrd].UUID(), id)
	}
	s.rows = newRows
}

// compactShard acquires the write lock(s) for shard shardIdx in the global order
// (pkDirectory then shard, for a partitioned table) and compacts it
// unconditionally. Used by tests; the background sweeper compacts dense shards
// through compactShardIfDense, which does its own dead-fraction check and locking.
// Both share the lock-free compactShardLocked body.
func (t *table) compactShard(shardIdx int) {
	s := &t.shards[shardIdx]
	if t.pkDir != nil {
		t.pkDir.mu.Lock()
		s.mu.Lock()
		t.compactShardLocked(s, uint32(shardIdx))
		s.mu.Unlock()
		t.pkDir.mu.Unlock()
		return
	}
	s.mu.Lock()
	t.compactShardLocked(s, uint32(shardIdx))
	s.mu.Unlock()
}
