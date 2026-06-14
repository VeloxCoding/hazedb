package hazedb

import "sync"

// Write paths: insert / update / delete (PK-pinned, multi-shard predicate, and
// indexed-candidate variants), the shard-locking helpers they share, and the
// small dirty-overlay / byte-tally / index-touch helpers. Core types live in
// store.go; tombstoning and compaction in store_compact.go.

// markDirtyLocked records pk as needing an index merge. No-op when the table has
// no secondary indexes. Caller holds s.mu (every live write already does), so
// this adds no lock to the write path.
func (t *table) markDirtyLocked(s *tableShard, pk UUID) {
	if len(t.indexes) > 0 {
		s.dirtyRead = append(s.dirtyRead, pk)
		t.readDirtyCount.Add(1)
	}
}

// markDelDirtyLocked records a deleted pk for index cleanup only. The merger
// removes its stale index entry, but it never enters the read overlay — a
// deleted row can never match a read. Same one-append, no-new-lock cost as
// markDirtyLocked. No-op when the table has no secondary indexes.
func (t *table) markDelDirtyLocked(s *tableShard, pk UUID) {
	if len(t.indexes) > 0 {
		s.dirtyDel = append(s.dirtyDel, pk)
		t.delDirtyCount.Add(1)
	}
}

// addRowLocked appends row to the shard arena, bumps the live count, adds cost
// (its precomputed rowCost) to the per-shard tally, and returns its rowID. Caller
// holds s.mu and has already reserved cost against the table budget. The single
// append point for the byte tally.
func (s *tableShard) addRowLocked(row Row, cost int64) uint64 {
	rowID := uint64(len(s.rows))
	s.rows = append(s.rows, row)
	s.live++
	s.bytes += cost
	return rowID
}

// addBytesLocked applies a signed byte delta from an in-place or whole-row
// UPDATE to both the per-shard tally and the shared budget, keeping the budget
// total equal to the sum of the shard tallies. Caller holds s.mu. A grow is not
// gated (only inserts reserve); it can push the budget over max, after which
// inserts are rejected until space frees.
func (s *tableShard) addBytesLocked(delta int64, b *byteBudget) {
	s.bytes += delta
	b.adjust(delta)
}

// ordIsIndexed reports whether column ordinal ord is part of any secondary index
// (any component of a composite index counts).
func (t *table) ordIsIndexed(ord int) bool {
	for _, ix := range t.indexes {
		for _, ixOrd := range ix.ordinals {
			if ord == ixOrd {
				return true
			}
		}
	}
	return false
}

// updateTouchesIndex reports whether an UPDATE of these column ordinals changes
// any indexed column. When it does not, every index entry stays valid: the row
// needs no merge and no place in the read overlay, so the UPDATE paths skip
// markDirtyLocked — keeping non-indexed updates out of the dirty overlay (which
// would otherwise grow with every update and slow the next indexed lookup).
func (t *table) updateTouchesIndex(ords []int) bool {
	for _, ord := range ords {
		if t.ordIsIndexed(ord) {
			return true
		}
	}
	return false
}

// insert places a row under its PK. Returns ErrDuplicatePK if the key
// already exists (live or tombstone-replaced rows take new slots).
//
// This is the BOOT path only — WAL replay (applyMutation) and SQLite recovery
// (loadTableRows). It deliberately does NOT mark the row dirty: Open rebuilds
// every secondary index from a full scan (rebuildAllIndexes) once replay and
// recovery finish, which clears and regenerates the overlay wholesale, so a
// per-row dirty mark here is work that is immediately discarded. Live writes use
// insertJournaled, which does mark dirty. Do not call insert on a live path.
func (t *table) insert(row Row) error {
	if t.pkDir != nil {
		return t.insertPartitioned(row, mutJournal{})
	}
	pk := row[t.def.pkOrdinal].UUID()
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	slot, exists := s.pk.reserve(pk)
	if exists {
		return ErrDuplicatePK
	}
	cost := rowCost(row, len(t.indexes))
	if !t.budget.reserve(cost) {
		return ErrCapacity
	}
	rowID := s.addRowLocked(row, cost)
	s.pk.commit(slot, pk, rowID)
	return nil
}

// insertJournaled is the live-write insert path: it checks PK uniqueness,
// runs j.insert (the WAL append), and adds the row — all under one shard
// lock, in that order. This enforces the RFC pipeline (validate → WAL →
// apply) atomically:
//
//   - a duplicate PK returns ErrDuplicatePK BEFORE the journal runs, so a
//     rejected insert never reaches the WAL (otherwise replay re-hits the
//     duplicate and Open fails permanently);
//   - if the journal returns an error the row is NOT added, so RAM never
//     holds a mutation absent from the WAL;
//   - holding the shard lock across journal+append makes WAL order and
//     in-memory order identical for inserts on the same shard.
//
// j is the zero value for a memory-only DB (journals nothing).
func (t *table) insertJournaled(row Row, j mutJournal) error {
	if t.pkDir != nil {
		return t.insertPartitioned(row, j)
	}
	pk := row[t.def.pkOrdinal].UUID()
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	// reserve probes ONCE for both the duplicate check and the target slot;
	// the reservation holds across journal because the shard lock is held and
	// the WAL append never touches the pk map.
	slot, exists := s.pk.reserve(pk)
	if exists {
		return ErrDuplicatePK
	}
	cost := rowCost(row, len(t.indexes))
	if !t.budget.reserve(cost) {
		return ErrCapacity
	}
	if err := j.insert(row); err != nil {
		t.budget.release(cost) // un-reserve: the row is not added
		return err
	}
	rowID := s.addRowLocked(row, cost)
	s.pk.commit(slot, pk, rowID)
	t.markDirtyLocked(s, pk)
	return nil
}

// updateByPKJournaled is the live PK-pinned update: under the one shard
// lock it computes the SET values (compute sees the current row, so
// arithmetic like col = col - ? works), journals the new row image, and on a
// WAL failure reverts — so a row is never applied without a durable record.
// The hot path allocates nothing: the pre-update values of the touched
// columns are saved in a small stack array (heap only if >8 columns are
// set, which is rare). j may be the zero value (memory-only), in which case
// the values are simply applied with no save/revert overhead.
func (t *table) updateByPKJournaled(pk UUID, ords []int, compute func(Row) ([]Value, error), j mutJournal) (bool, error) {
	if t.pkDir != nil {
		return t.updateByPKJournaledPartitioned(pk, ords, compute, j)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return false, nil
	}
	r := s.rows[rowID]
	vals, err := compute(r)
	if err != nil {
		return false, err
	}
	// A non-indexed update leaves every index entry valid, so skip the dirty mark
	// (and the overlay/merge cost it adds) — same guard as updateWhereAll.
	touch := t.updateTouchesIndex(ords)
	if !j.live() {
		var delta int64
		for i, ord := range ords {
			delta += cellDelta(r[ord], vals[i])
			r[ord] = vals[i]
		}
		s.addBytesLocked(delta, t.budget)
		if touch {
			t.markDirtyLocked(s, pk)
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
	if touch {
		t.markDirtyLocked(s, pk)
	}
	return true, nil
}

// updateByPKOneJournaled is the one-column variant of updateByPKJournaled.
// It avoids building a temporary []Value for the common point-update path.
func (t *table) updateByPKOneJournaled(pk UUID, ord int, compute func(Row) (Value, error), j mutJournal) (bool, error) {
	if t.pkDir != nil {
		return t.updateByPKOneJournaledPartitioned(pk, ord, compute, j)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return false, nil
	}
	r := s.rows[rowID]
	val, err := compute(r)
	if err != nil {
		return false, err
	}
	touch := t.ordIsIndexed(ord) // non-indexed update needs no overlay (see updateByPKJournaled)
	if !j.live() {
		s.addBytesLocked(cellDelta(r[ord], val), t.budget)
		r[ord] = val
		if touch {
			t.markDirtyLocked(s, pk)
		}
		return true, nil
	}
	old := r[ord]
	r[ord] = val
	if err := j.update(r); err != nil {
		r[ord] = old
		return false, err
	}
	s.addBytesLocked(cellDelta(old, val), t.budget)
	if touch {
		t.markDirtyLocked(s, pk)
	}
	return true, nil
}

// deleteByPKJournaled is the live PK-pinned delete: under the one shard lock
// it journals (via j.delete) before tombstoning, so a WAL failure aborts
// before the row is removed. j may be the zero value (memory-only).
func (t *table) deleteByPKJournaled(pk UUID, j mutJournal) (bool, error) {
	if t.pkDir != nil {
		return t.deleteByPKJournaledPartitioned(pk, j)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return false, nil
	}
	if err := j.delete(); err != nil {
		return false, err
	}
	s.tombstoneLocked(rowID, len(t.indexes), t.budget)
	s.pk.del(pk)
	t.markDelDirtyLocked(s, pk)
	return true, nil
}

// update mutates the row at pk using mutate (the WAL-replay apply path).
// Returns false if the row is absent, OR if mutate violates the storage
// invariant: it must return a non-nil row whose PK is unchanged (PK and
// PartitionKey are immutable). A violation returns false instead of silently
// corrupting the index — s.pk still maps pk, so a changed-PK or nil result
// would leave the index pointing at the wrong row or a tombstone. The replay
// caller turns false into ErrWALCorrupt (a WAL update record never legitimately
// changes a PK; the live plan rejects PK updates), so corruption fails Open
// loudly rather than serving a broken index. mutate runs under the shard write
// lock and must not call back into the store.
func (t *table) update(pk UUID, mutate func(Row) Row) bool {
	if t.pkDir != nil {
		return t.updatePartitioned(pk, mutate)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return false
	}
	// mutate may edit the row in place and return the same slice, so capture the
	// old cost BEFORE calling it — afterwards s.rows[rowID] already holds the new
	// values and the delta would read as zero.
	nIdx := len(t.indexes)
	before := rowCost(s.rows[rowID], nIdx)
	nr := mutate(s.rows[rowID])
	if nr == nil || nr[t.def.pkOrdinal].UUID() != pk {
		return false
	}
	s.addBytesLocked(rowCost(nr, nIdx)-before, t.budget)
	s.rows[rowID] = nr
	t.markDirtyLocked(s, pk)
	return true
}

// lockAllShards / unlockAllShards take (and release) every shard lock in
// the global order: ascending index to lock, descending to unlock. Used by
// the multi-shard predicate writes below, which must hold all shard locks
// for the whole operation.
func (t *table) lockAllShards() {
	for i := range t.shards {
		t.shards[i].mu.Lock()
	}
}

func (t *table) unlockAllShards() {
	for i := len(t.shards) - 1; i >= 0; i-- {
		t.shards[i].mu.Unlock()
	}
}

// lockCandidateShards locks only the shard(s) the candidate PKs fall on and
// returns an unlock func. The common case — every candidate on one shard (a
// single-row index update/delete) — takes exactly ONE lock with no allocation,
// instead of all 32. A multi-shard candidate set falls back to lockAllShards
// (ascending, deadlock-safe, same order); narrowing buys little there and
// avoids per-call bookkeeping. A single held lock never waits while holding
// another, so it cannot deadlock with the ascending all-shard path.
// shardUnlock releases what lockCandidateShards took: one shard (mu set) or all
// shards (all set). Returned by value so the common single-shard case allocates
// nothing — a returned method value (s.mu.Unlock / t.unlockAllShards) heap-escapes
// the receiver, which this avoids on every indexed UPDATE/DELETE.
type shardUnlock struct {
	mu  *sync.RWMutex
	all *table
}

func (u shardUnlock) unlock() {
	if u.mu != nil {
		u.mu.Unlock()
	} else if u.all != nil {
		u.all.unlockAllShards()
	}
}

func (t *table) lockCandidateShards(pks []UUID) shardUnlock {
	if len(pks) == 0 {
		return shardUnlock{}
	}
	oi := t.shardIdxOf(pks[0])
	for _, pk := range pks[1:] {
		if t.shardIdxOf(pk) != oi {
			t.lockAllShards()
			return shardUnlock{all: t}
		}
	}
	s := &t.shards[oi]
	s.mu.Lock()
	return shardUnlock{mu: &s.mu}
}

// updateWhereAll applies a predicate UPDATE across the whole table while
// holding EVERY shard lock for the entire operation — both for serializability
// (the one-shard-at-a-time pattern is a write-serializability + replay-
// divergence bug) AND for statement atomicity.
//
// It runs in two passes under the locks: pass 1 matches rows, computes each new
// row image (compute sees the live row, so arithmetic works), and collects the
// encoded mutation bodies; then the whole batch is journaled as ONE TXN
// envelope (journalAll); only then does pass 2 apply the new rows to memory. So
// a WAL failure aborts with NOTHING applied (return 0, err), and a crash leaves
// either the whole statement in the WAL or none of it — the statement is
// all-or-nothing, not partially applied. encode/journalAll are nil for a
// memory-only DB (apply directly, no atomicity concern without a WAL).
func (t *table) updateWhereAll(match func(Row) bool, ords []int, compute func(Row) ([]Value, error), encode func(Row) []byte, journalAll func([][]byte) error) (int, error) {
	t.lockAllShards()
	defer t.unlockAllShards()
	type pendingUpdate struct {
		s  *tableShard
		j  int
		nr Row
	}
	var pending []pendingUpdate
	var bodies [][]byte
	for i := range t.shards {
		s := &t.shards[i]
		for j, r := range s.rows {
			if r == nil || !match(r) {
				continue
			}
			vals, err := compute(r)
			if err != nil {
				return 0, err // nothing applied yet
			}
			nr := r.Clone()
			for k, ord := range ords {
				nr[ord] = vals[k]
			}
			pending = append(pending, pendingUpdate{s, j, nr})
			if encode != nil {
				bodies = append(bodies, encode(nr))
			}
		}
	}
	if journalAll != nil && len(bodies) > 0 {
		if err := journalAll(bodies); err != nil {
			return 0, err // atomic: WAL failed, apply nothing
		}
	}
	pkOrd := t.def.pkOrdinal
	touch := t.updateTouchesIndex(ords)
	nIdx := len(t.indexes)
	for _, p := range pending {
		p.s.addBytesLocked(rowCost(p.nr, nIdx)-rowCost(p.s.rows[p.j], nIdx), t.budget)
		p.s.rows[p.j] = p.nr
		if touch {
			t.markDirtyLocked(p.s, p.nr[pkOrd].UUID())
		}
	}
	return len(pending), nil
}

// updateByCandidates is updateWhereAll narrowed to a candidate PK set resolved
// through a secondary index: it visits only those rows — re-checking match (the
// full WHERE) under the lock, so a stale index hit or unrelated dirty PK yields
// no wrong write — instead of scanning every row. Same all-shard-lock +
// single-TXN atomicity. Non-partitioned only (secondary indexes are not on
// partitioned tables, so idxLookup never fires for them).
func (t *table) updateByCandidates(pks []UUID, match func(Row) bool, ords []int, compute func(Row) ([]Value, error), encode func(Row) []byte, journalAll func([][]byte) error) (int, error) {
	defer t.lockCandidateShards(pks).unlock()
	type pendingUpdate struct {
		s  *tableShard
		j  uint64
		nr Row
	}
	var pending []pendingUpdate
	var bodies [][]byte
	for _, pk := range pks {
		s := t.shardOf(pk)
		rowID, ok := s.pk.get(pk)
		if !ok {
			continue
		}
		r := s.rows[rowID]
		if r == nil || !match(r) {
			continue
		}
		vals, err := compute(r)
		if err != nil {
			return 0, err
		}
		nr := r.Clone()
		for k, ord := range ords {
			nr[ord] = vals[k]
		}
		pending = append(pending, pendingUpdate{s, rowID, nr})
		if encode != nil {
			bodies = append(bodies, encode(nr))
		}
	}
	if journalAll != nil && len(bodies) > 0 {
		if err := journalAll(bodies); err != nil {
			return 0, err
		}
	}
	pkOrd := t.def.pkOrdinal
	touch := t.updateTouchesIndex(ords)
	nIdx := len(t.indexes)
	for _, p := range pending {
		p.s.addBytesLocked(rowCost(p.nr, nIdx)-rowCost(p.s.rows[p.j], nIdx), t.budget)
		p.s.rows[p.j] = p.nr
		if touch {
			t.markDirtyLocked(p.s, p.nr[pkOrd].UUID())
		}
	}
	return len(pending), nil
}

// updateOneByCandidate applies a single-column update to ONE index candidate
// in place (no full-row clone — the big cost updateByCandidates pays) under its
// shard lock, after re-checking match (the full WHERE: the candidate may be a
// stale index hit or an unrelated dirty PK). Mirrors updateByPKOneJournaled with
// a WHERE gate; journal-before-apply with revert on WAL failure. Returns 1 if
// updated, 0 if the candidate is gone or no longer matches.
func (t *table) updateOneByCandidate(pk UUID, ord int, match func(Row) bool, computeOne func(Row) (Value, error), j mutJournal) (int, error) {
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return 0, nil
	}
	r := s.rows[rowID]
	if r == nil || !match(r) {
		return 0, nil
	}
	val, err := computeOne(r)
	if err != nil {
		return 0, err
	}
	touch := t.ordIsIndexed(ord)
	if !j.live() {
		s.addBytesLocked(cellDelta(r[ord], val), t.budget)
		r[ord] = val
		if touch {
			t.markDirtyLocked(s, pk)
		}
		return 1, nil
	}
	old := r[ord]
	r[ord] = val
	if err := j.update(r); err != nil {
		r[ord] = old
		return 0, err
	}
	s.addBytesLocked(cellDelta(old, val), t.budget)
	if touch {
		t.markDirtyLocked(s, pk)
	}
	return 1, nil
}

// deleteByPK removes the row at pk. Returns false if absent.
// Tombstones in place (rows[rowID] = nil) so rowIDs stay stable.
func (t *table) deleteByPK(pk UUID) bool {
	if t.pkDir != nil {
		return t.deleteByPKPartitioned(pk)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk.del(pk) // one probe: del returns the removed rowID
	if !ok {
		return false
	}
	s.tombstoneLocked(rowID, len(t.indexes), t.budget)
	t.markDelDirtyLocked(s, pk)
	return true
}

// deleteWhereAll tombstones all matching rows across the whole table while
// holding EVERY shard lock — same two-pass, single-TXN-envelope atomicity as
// updateWhereAll. Pass 1 collects the matched rows + encoded delete bodies;
// the batch is journaled as one TXN envelope; pass 2 tombstones. A WAL failure
// aborts with nothing applied. encode/journalAll are nil for a memory-only DB.
func (t *table) deleteWhereAll(match func(Row) bool, encode func(pk Value) []byte, journalAll func([][]byte) error) (int, error) {
	if t.pkDir != nil {
		return t.deleteWhereAllPartitioned(match, encode, journalAll)
	}
	pkOrd := t.def.pkOrdinal
	t.lockAllShards()
	defer t.unlockAllShards()
	type pendingDelete struct {
		s  *tableShard
		j  int
		pk UUID
	}
	var pending []pendingDelete
	var bodies [][]byte
	for i := range t.shards {
		s := &t.shards[i]
		for j, r := range s.rows {
			if r == nil || !match(r) {
				continue
			}
			pending = append(pending, pendingDelete{s, j, r[pkOrd].UUID()})
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
		p.s.pk.del(p.pk)
		p.s.tombstoneLocked(uint64(p.j), len(t.indexes), t.budget)
		t.markDelDirtyLocked(p.s, p.pk)
	}
	return len(pending), nil
}

// deleteByCandidates is deleteWhereAll narrowed to a secondary-index candidate
// set: it visits only those PKs — re-checking match under the lock — instead of
// scanning. Same all-shard-lock + single-TXN atomicity. Non-partitioned only.
func (t *table) deleteByCandidates(pks []UUID, match func(Row) bool, encode func(pk Value) []byte, journalAll func([][]byte) error) (int, error) {
	pkOrd := t.def.pkOrdinal
	defer t.lockCandidateShards(pks).unlock()
	type pendingDelete struct {
		s  *tableShard
		j  uint64
		pk UUID
	}
	var pending []pendingDelete
	var bodies [][]byte
	for _, pk := range pks {
		s := t.shardOf(pk)
		rowID, ok := s.pk.get(pk)
		if !ok {
			continue
		}
		r := s.rows[rowID]
		if r == nil || !match(r) {
			continue
		}
		pending = append(pending, pendingDelete{s, rowID, pk})
		if encode != nil {
			bodies = append(bodies, encode(r[pkOrd]))
		}
	}
	if journalAll != nil && len(bodies) > 0 {
		if err := journalAll(bodies); err != nil {
			return 0, err
		}
	}
	for _, p := range pending {
		p.s.pk.del(p.pk)
		p.s.tombstoneLocked(p.j, len(t.indexes), t.budget)
		t.markDelDirtyLocked(p.s, p.pk)
	}
	return len(pending), nil
}

// dirtyTooDenseForScan reports whether the READ overlay has grown large enough
// that the hybrid candidate path (idxCandidates walks every read-dirty PK to
// dedup against the index hits) would do MORE work than a full table scan. When
// true, an indexed UPDATE/DELETE falls back to the scan path so it is never
// slower than the pre-index full-scan behaviour under a heavy write burst. Only
// readDirtyCount matters: deletes never enter the overlay, so a delete burst
// must not push UPDATE/DELETE onto the scan path. Steady state returns false
// without the 32-shard liveCount sweep.
func (t *table) dirtyTooDenseForScan() bool {
	dc := t.readDirtyCount.Load()
	if dc == 0 {
		return false
	}
	return dc >= int64(t.liveCount())
}
