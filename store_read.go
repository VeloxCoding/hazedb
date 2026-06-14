package hazedb

// Read paths: PK point reads (clone / project / encode under the shard lock),
// full and batched scans, and the live-row count. Core types live in store.go.

// getByPK returns a private full-row clone for pk, or (nil, false). The clone
// is taken UNDER the shard read lock so the result never aliases the arena.
func (t *table) getByPK(pk UUID) (Row, bool) {
	if t.pkDir != nil {
		return t.getByPKPartitioned(pk)
	}
	s := t.shardOf(pk)
	s.mu.RLock()
	rowID, ok := s.pk.get(pk)
	var cl Row
	if ok {
		if r := s.rows[rowID]; r != nil {
			cl = r.Clone() // clone UNDER the lock — a concurrent writer holds the write lock
		}
	}
	s.mu.RUnlock()
	return cl, cl != nil
}

// getByPKProject is getByPK for a projected SELECT: it clones only the ords
// columns into a fresh Row under the shard read lock, skipping the full-row
// clone the caller would otherwise make and then project from. The win grows
// with the gap between table width and projection width.
func (t *table) getByPKProject(pk UUID, ords []int) (Row, bool) {
	if t.pkDir != nil {
		return t.getByPKProjectPartitioned(pk, ords)
	}
	s := t.shardOf(pk)
	s.mu.RLock()
	var pr Row
	if rowID, ok := s.pk.get(pk); ok {
		if r := s.rows[rowID]; r != nil {
			pr = projectClone(r, ords)
		}
	}
	s.mu.RUnlock()
	return pr, pr != nil
}

// getByPKProjectInto is the scan-into form of getByPKProject: it appends the
// projected cells into dst (caller-owned and reused) rather than allocating a
// fresh Row, so a projection without BYTES columns makes no allocation. ords
// nil means all columns. Returns the grown slice and whether a row matched.
func (t *table) getByPKProjectInto(pk UUID, ords []int, dst []Value) ([]Value, bool) {
	if t.pkDir != nil {
		return t.getByPKProjectIntoPartitioned(pk, ords, dst)
	}
	s := t.shardOf(pk)
	s.mu.RLock()
	out, found := dst[:0], false
	if rowID, ok := s.pk.get(pk); ok {
		if r := s.rows[rowID]; r != nil {
			if ords == nil {
				out = appendRowClone(out, r)
			} else {
				out = appendProjectClone(out, r, ords)
			}
			found = true
		}
	}
	s.mu.RUnlock()
	return out, found
}

// getByPKJSONInto finds pk's live row and appends it as a flat JSON object into
// dst UNDER the shard read lock, encoding the cells straight from the live row
// (appendValueJSON copies every cell, so nothing aliases the arena) — so it
// makes no Row clone. ords nil = all columns (SELECT *). Returns the grown
// buffer and whether a row matched. The encode-under-lock counterpart of
// getByPKProjectInto, for an in-process JSON consumer.
func (t *table) getByPKJSONInto(pk UUID, cols []string, ords []int, dst []byte) ([]byte, bool) {
	if t.pkDir != nil {
		return t.getByPKJSONIntoPartitioned(pk, cols, ords, dst)
	}
	s := t.shardOf(pk)
	s.mu.RLock()
	defer s.mu.RUnlock()
	rowID, ok := s.pk.get(pk)
	if !ok {
		return dst, false
	}
	r := s.rows[rowID]
	if r == nil {
		return dst, false
	}
	if ords == nil {
		return appendRowJSONObject(dst, cols, r), true
	}
	return appendRowJSONObjectProject(dst, cols, r, ords), true
}

// projectClone copies the ords columns of r into a fresh Row, deep-copying any
// []byte cells so the result aliases nothing in the arena. Caller holds the
// shard read lock.
func projectClone(r Row, ords []int) Row {
	pr := make(Row, len(ords))
	for j, ord := range ords {
		pr[j] = cloneValue(r[ord])
	}
	return pr
}

// scanAll walks every row across every shard, invoking fn. Returning
// false from fn halts the scan. Holds each shard's RLock only for the
// duration of that shard's pass.
//
// Order is undefined (shard order, then arena insertion order). Use
// the WHERE/ORDER-BY pipeline above for deterministic ordering.
func (t *table) scanAll(fn func(Row) bool) {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		stop := false
		for _, r := range s.rows {
			if r == nil {
				continue
			}
			if !fn(r) {
				stop = true
				break
			}
		}
		s.mu.RUnlock()
		if stop {
			return
		}
	}
}

// scanShardsBatched walks rows shard by shard in chunks: it snapshots each
// shard's live PKs under one read lock, then resolves and clones them in chunks,
// calling process(batch) per chunk (process returns true to stop the whole walk).
// The shard lock is NEVER held across process, so process may safely lock another
// table (the streaming join driver probes the other side there) without the
// cross-table lock cycle that holding the driver lock would risk.
//
// Resolving by PK (not by arena index) is what makes the walk safe against the
// background compaction sweeper, which renumbers rowIDs (compactShardLocked). A
// snapshotted PK re-resolves to its current row each chunk, so a renumber between
// chunks can neither skip a live row nor yield one twice; a PK deleted mid-walk
// resolves to nothing and is dropped. Per-shard consistent (the snapshot fixes
// the row set at lock time), the standard multi-shard-read contract.
func (t *table) scanShardsBatched(chunk int, keep func(Row) bool, process func([]Row) bool) {
	pkOrd := t.def.pkOrdinal
	for i := range t.shards {
		s := &t.shards[i]
		// Snapshot this shard's live PKs — stable keys, immune to the rowID
		// renumbering compaction does. Cheap: UUID copies, no row clone.
		s.mu.RLock()
		pks := make([]UUID, 0, s.live)
		for _, r := range s.rows {
			if r != nil {
				pks = append(pks, r[pkOrd].UUID())
			}
		}
		s.mu.RUnlock()
		for start := 0; start < len(pks); start += chunk {
			end := start + chunk
			if end > len(pks) {
				end = len(pks)
			}
			var batch []Row
			if t.pkDir != nil {
				// Partitioned: the directory resolves each PK (with its own renumber
				// retry, getByPKPartitioned) and clones; keep filters the clone.
				for _, pk := range pks[start:end] {
					if r, ok := t.getByPKPartitioned(pk); ok && keep(r) {
						batch = append(batch, r)
					}
				}
			} else {
				// Non-partitioned: one read lock for the chunk; resolve each PK to its
				// current row, filter, then clone only on a pass (keep before clone).
				s.mu.RLock()
				for _, pk := range pks[start:end] {
					if rowID, ok := s.pk.get(pk); ok {
						if r := s.rows[rowID]; r != nil && keep(r) {
							batch = append(batch, r.Clone())
						}
					}
				}
				s.mu.RUnlock()
			}
			if len(batch) > 0 && process(batch) {
				return
			}
		}
	}
}

// liveCount returns the number of non-tombstoned rows across all
// shards. Useful for tests and operator-facing stats.
func (t *table) liveCount() int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		n += s.live
		s.mu.RUnlock()
	}
	return n
}
