package hazedb

import (
	"encoding/binary"
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

// table is the in-memory storage for one declared table. Sharded by the
// UUID primary key's hash so independent keys seldom contend.
//
// Row layout: each shard owns a contiguous []Row arena. rowIDs are
// per-shard indices into that arena. Deleted rows are tombstoned in
// place (Row=nil) and reclaimed on rebuild — never auto-compacted, so
// rowIDs stay stable for the indexes that point at them.
type table struct {
	def    *resolvedTable
	shards []tableShard
	mask   uint32
	// pkDir is the table-wide PK→location directory for PARTITIONED tables
	// (nil otherwise). Partitioned shards route by PartitionKey value, so the
	// per-shard pk map can't enforce table-wide PK uniqueness or answer
	// WHERE id=? — the directory does both.
	pkDir *pkDirectory
}

type tableShard struct {
	mu   sync.RWMutex
	rows []Row // arena; nil entries are tombstones
	// pk is the per-shard PK→rowID index for NON-partitioned tables.
	pk map[UUID]uint64
	// tails groups rowIDs by PartitionKey value for PARTITIONED tables, in
	// insert order. A WHERE partition=? scan reads only the matching list
	// instead of every row, so it is O(partition size), not O(table). Deleted
	// rowIDs stay in the list (rows[rowID] is nil) and the scan skips them.
	tails map[UUID][]uint64
	live  int // count of non-tombstoned rows
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
		if def.partitioned() {
			t.shards[i].tails = make(map[UUID][]uint64)
		} else {
			t.shards[i].pk = make(map[UUID]uint64, per)
		}
	}
	if def.partitioned() {
		t.pkDir = &pkDirectory{idx: make(map[UUID]rowLocation, sizeHint)}
	}
	return t
}

// shardIdxOf hashes the 16-byte UUID (FNV-1a 32-bit) to a shard index. For a
// non-partitioned table the UUID is the PK; for a partitioned table it is the
// PartitionKey value (so all rows of one partition land in one shard).
// shardIdxOf maps a UUID to a shard: a multiplicative (Fibonacci) fold of both
// 64-bit halves, returning high, well-mixed bits. Reads all 16 bytes so the
// spread holds wherever the entropy sits — random low bytes of a real UUIDv7,
// or the high timestamp bytes of a sequential key. Not persisted (the WAL
// stores rows, never shard indices), so the constant can change freely.
func (t *table) shardIdxOf(u UUID) uint32 {
	a := binary.LittleEndian.Uint64(u[0:8])
	b := binary.LittleEndian.Uint64(u[8:16])
	h := (a ^ bits.RotateLeft64(b, 32)) * 0x9E3779B97F4A7C15
	return uint32(h>>32) & t.mask
}

func (t *table) shardOf(u UUID) *tableShard { return &t.shards[t.shardIdxOf(u)] }

// insert places a row under its PK. Returns ErrDuplicatePK if the key
// already exists (live or tombstone-replaced rows take new slots).
func (t *table) insert(row Row) error {
	if t.pkDir != nil {
		return t.insertPartitioned(row, nil)
	}
	pk := row[t.def.pkOrdinal].UUID()
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pk[pk]; exists {
		return ErrDuplicatePK
	}
	rowID := uint64(len(s.rows))
	s.rows = append(s.rows, row)
	s.pk[pk] = rowID
	s.live++
	return nil
}

// insertJournaled is the live-write insert path: it checks PK uniqueness,
// runs journal() (the WAL append), and adds the row — all under one shard
// lock, in that order. This enforces the RFC pipeline (validate → WAL →
// apply) atomically:
//
//   - a duplicate PK returns ErrDuplicatePK BEFORE journal runs, so a
//     rejected insert never reaches the WAL (otherwise replay re-hits the
//     duplicate and Open fails permanently);
//   - if journal returns an error the row is NOT added, so RAM never holds
//     a mutation absent from the WAL;
//   - holding the shard lock across journal+append makes WAL order and
//     in-memory order identical for inserts on the same shard.
//
// journal may be nil (memory-only DB).
func (t *table) insertJournaled(row Row, journal func() error) error {
	if t.pkDir != nil {
		return t.insertPartitioned(row, journal)
	}
	pk := row[t.def.pkOrdinal].UUID()
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pk[pk]; exists {
		return ErrDuplicatePK
	}
	if journal != nil {
		if err := journal(); err != nil {
			return err
		}
	}
	rowID := uint64(len(s.rows))
	s.rows = append(s.rows, row)
	s.pk[pk] = rowID
	s.live++
	return nil
}

// updateByPKJournaled is the live PK-pinned update: under the one shard
// lock it computes the SET values (compute sees the current row, so
// arithmetic like col = col - ? works), journals the new row image, and on a
// WAL failure reverts — so a row is never applied without a durable record.
// The hot path allocates nothing: the pre-update values of the touched
// columns are saved in a small stack array (heap only if >8 columns are
// set, which is rare). journal may be nil (memory-only), in which case the
// values are simply applied with no save/revert overhead.
func (t *table) updateByPKJournaled(pk UUID, ords []int, compute func(Row) ([]Value, error), journal func(Row) error) (bool, error) {
	if t.pkDir != nil {
		return t.updateByPKJournaledPartitioned(pk, ords, compute, journal)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return false, nil
	}
	r := s.rows[rowID]
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

// updateByPKOneJournaled is the one-column variant of updateByPKJournaled.
// It avoids building a temporary []Value for the common point-update path.
func (t *table) updateByPKOneJournaled(pk UUID, ord int, compute func(Row) (Value, error), journal func(Row) error) (bool, error) {
	if t.pkDir != nil {
		return t.updateByPKOneJournaledPartitioned(pk, ord, compute, journal)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return false, nil
	}
	r := s.rows[rowID]
	val, err := compute(r)
	if err != nil {
		return false, err
	}
	if journal == nil {
		r[ord] = val
		return true, nil
	}
	old := r[ord]
	r[ord] = val
	if err := journal(r); err != nil {
		r[ord] = old
		return false, err
	}
	return true, nil
}

// deleteByPKJournaled is the live PK-pinned delete: under the one shard lock
// it journals (via journal) before tombstoning, so a WAL failure aborts
// before the row is removed. journal may be nil (memory-only).
func (t *table) deleteByPKJournaled(pk UUID, journal func() error) (bool, error) {
	if t.pkDir != nil {
		return t.deleteByPKJournaledPartitioned(pk, journal)
	}
	s := t.shardOf(pk)
	s.mu.Lock()
	defer s.mu.Unlock()
	rowID, ok := s.pk[pk]
	if !ok {
		return false, nil
	}
	if journal != nil {
		if err := journal(); err != nil {
			return false, err
		}
	}
	s.rows[rowID] = nil
	delete(s.pk, pk)
	s.live--
	return true, nil
}

// getByPK returns a private full-row clone for pk, or (nil, false). The clone
// is taken UNDER the shard read lock so the result never aliases the arena.
func (t *table) getByPK(pk UUID) (Row, bool) {
	if t.pkDir != nil {
		return t.getByPKPartitioned(pk)
	}
	s := t.shardOf(pk)
	s.mu.RLock()
	rowID, ok := s.pk[pk]
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
	if rowID, ok := s.pk[pk]; ok {
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
	if rowID, ok := s.pk[pk]; ok {
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
	rowID, ok := s.pk[pk]
	if !ok {
		return false
	}
	nr := mutate(s.rows[rowID])
	if nr == nil || nr[t.def.pkOrdinal].UUID() != pk {
		return false
	}
	s.rows[rowID] = nr
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
	for _, p := range pending {
		p.s.rows[p.j] = p.nr
	}
	return len(pending), nil
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
	rowID, ok := s.pk[pk]
	if !ok {
		return false
	}
	s.rows[rowID] = nil
	delete(s.pk, pk)
	s.live--
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
		delete(p.s.pk, p.pk)
		p.s.rows[p.j] = nil
		p.s.live--
	}
	return len(pending), nil
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
