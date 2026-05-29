package hazedb

import (
	"math/bits"
	"runtime"
	"sync"
)

// shardCount picks a power-of-two shard count from runtime.NumCPU()*4,
// floored at 64 and capped at 1024. The spike validated this against
// uniform and skewed workloads.
func shardCount() int {
	n := runtime.NumCPU() * 4
	if n < 64 {
		n = 64
	}
	if n > 1024 {
		n = 1024
	}
	return 1 << bits.Len(uint(n-1))
}

// table is the in-memory storage for one declared table. Sharded by
// PK string hash so independent partitions seldom contend.
//
// Row layout: each shard owns a contiguous []Row arena. rowIDs are
// per-shard indices into that arena. Deleted rows are tombstoned in
// place (Row=nil) and reclaimed on rebuild — never auto-compacted, so
// rowIDs stay stable for the indexes that point at them.
type table struct {
	def    *resolvedTable
	shards []tableShard
	mask   uint32
}

type tableShard struct {
	mu   sync.RWMutex
	rows []Row              // arena; nil entries are tombstones
	pk   map[string]uint32  // PK-string → rowID
	live int                // count of non-tombstoned rows
}

func newTable(def *resolvedTable, sizeHint int) *table {
	n := shardCount()
	t := &table{
		def:    def,
		shards: make([]tableShard, n),
		mask:   uint32(n - 1),
	}
	per := sizeHint / n
	if per < 16 {
		per = 16
	}
	for i := range t.shards {
		t.shards[i].rows = make([]Row, 0, per)
		t.shards[i].pk = make(map[string]uint32, per)
	}
	return t
}

// shardOf hashes the PK string (FNV-1a 32-bit) and returns the owning
// shard. FNV-1a is fast, branchless, and well-distributed for short
// keys.
func (t *table) shardOf(pk string) *tableShard {
	var h uint32 = 2166136261
	for i := 0; i < len(pk); i++ {
		h ^= uint32(pk[i])
		h *= 16777619
	}
	return &t.shards[h&t.mask]
}

// insert places a row under its PK. Returns ErrDuplicatePK if the key
// already exists (live or tombstone-replaced rows take new slots).
func (t *table) insert(row Row) error {
	pk := row[t.def.pkOrdinal].AsString()
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pk[pk]; exists {
		return ErrDuplicatePK
	}
	rowID := uint32(len(s.rows))
	s.rows = append(s.rows, row)
	s.pk[pk] = rowID
	s.live++
	return nil
}

// getByPK returns the row for pk, or (nil, false). The returned Row is
// a shallow alias into the shard arena; callers that escape it past
// the call must Clone.
func (t *table) getByPK(pk string) (Row, bool) {
	s := t.shardOf(pk)
	s.mu.RLock()
	rowID, ok := s.pk[pk]
	if !ok {
		s.mu.RUnlock()
		return nil, false
	}
	r := s.rows[rowID]
	s.mu.RUnlock()
	return r, r != nil
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

// update mutates the row at pk using mutate. Returns false if the row
// is absent. mutate runs under the shard write lock and must not call
// back into the store.
func (t *table) update(pk string, mutate func(Row) Row) bool {
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return false
	}
	s.rows[rowID] = mutate(s.rows[rowID])
	return true
}

// updateWhere walks rows under each shard's write lock and applies
// mutate when match returns true. Returns the count of mutated rows.
// match runs under the lock but must be pure (no store calls).
//
// PK changes: if mutate changes the row's PK value, the pk index must
// be reseated. The executor refuses UPDATEs that touch the PK column,
// so updateWhere can ignore this case.
func (t *table) updateWhere(match func(Row) bool, mutate func(Row) Row) int {
	count := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		for j, r := range s.rows {
			if r == nil {
				continue
			}
			if match(r) {
				s.rows[j] = mutate(r)
				count++
			}
		}
		s.mu.Unlock()
	}
	return count
}

// deleteByPK removes the row at pk. Returns false if absent.
// Tombstones in place (rows[rowID] = nil) so rowIDs stay stable.
func (t *table) deleteByPK(pk string) bool {
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return false
	}
	s.rows[rowID] = nil
	delete(s.pk, pk)
	s.live--
	return true
}

// deleteWhere tombstones all matching rows and removes their PK
// entries. Returns the count deleted. match runs under the shard
// write lock and must not call back into the store.
func (t *table) deleteWhere(match func(Row) bool) int {
	count := 0
	pkOrd := t.def.pkOrdinal
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		for j, r := range s.rows {
			if r == nil {
				continue
			}
			if match(r) {
				delete(s.pk, r[pkOrd].AsString())
				s.rows[j] = nil
				s.live--
				count++
			}
		}
		s.mu.Unlock()
	}
	return count
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
