package hazedb

import (
	"encoding/binary"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// Core storage types and construction: the per-table shard set, its layout, and
// the UUID→shard hash. Read paths live in store_read.go, write paths in
// store_write.go, and tombstoning + compaction in store_compact.go.

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
// per-shard indices into that arena. A delete tombstones in place (Row=nil),
// keeping rowIDs stable for the pkMap / pkDirectory / tails that point at them;
// a background sweeper (compact.go) later compacts a shard once it has gone
// mostly dead — relocating the live rows and renumbering, off the write path.
type table struct {
	def    *resolvedTable
	shards []tableShard
	mask   uint32
	// budget is the store-wide byte-capacity admission control, shared by every
	// table in the DB (nil only in a few internal tests that bypass Open). Inserts
	// reserve against it; deletes and size-changing updates release/adjust it.
	budget *byteBudget
	// indexes are the secondary indexes declared on non-PK columns (nil when
	// none). Maintained per docs/secondary-indexes.md; only non-partitioned
	// tables may declare them (enforced in resolveSchema).
	indexes []*secIndex
	// mergeMu serialises mergeIndexes so two concurrent merges (e.g. the
	// background ticker and an explicit drain) never both reslice a shard's
	// dirty list. Only mergers take it; reads/writes never do.
	mergeMu sync.Mutex
	// pkDir is the table-wide PK→location directory for PARTITIONED tables
	// (nil otherwise). Partitioned shards route by PartitionKey value, so the
	// per-shard pk map can't enforce table-wide PK uniqueness or answer
	// WHERE id=? — the directory does both.
	pkDir *pkDirectory
	// readDirtyCount / delDirtyCount are the pending dirty-PK totals across all
	// shards, split by purpose. readDirtyCount tracks inserts + indexed-column
	// updates — rows whose live state may not yet be in the index, so reads must
	// overlay them. delDirtyCount tracks deletes — needed only so the merger
	// later removes the stale index entry, never for reads (a deleted row can
	// never match). An indexed read skips the overlay scan when readDirtyCount is
	// 0; the size-trigger and merge use the sum (both lists need merging).
	readDirtyCount atomic.Int64
	delDirtyCount  atomic.Int64
}

type tableShard struct {
	mu   sync.RWMutex
	rows []Row // arena; nil entries are tombstones
	// pk is the per-shard PK→rowID index for NON-partitioned tables (an
	// open-addressed table, see pkmap.go; zero/empty on partitioned shards,
	// which route through pkDir instead).
	pk pkMap
	// tails groups rowIDs by PartitionKey value for PARTITIONED tables, in
	// insert order. A WHERE partition=? scan reads only the matching list
	// instead of every row, so it is O(partition size), not O(table). Deleted
	// rowIDs stay in the list (rows[rowID] is nil) and the scan skips them.
	tails map[UUID][]uint64
	// tailsDead counts tombstoned rowIDs still lingering in tails[part] (partitioned
	// tables only). When it reaches half a partition's list, the delete path prunes
	// the dead entries so scanPartition stays O(live), not O(ever-inserted).
	tailsDead map[UUID]int
	live      int   // count of non-tombstoned rows
	bytes     int64 // running in-RAM byte tally of the live rows (see rowCost); the
	// O(1) source MetaSnapshot reads and a future byte cap checks, maintained
	// under s.mu by every row mutation so it never walks the arena.
	// dirtyRead / dirtyDel list PKs mutated on this shard since the last merge
	// (nil unless the table has secondary indexes). dirtyRead holds inserts +
	// indexed-column updates (rows the read overlay must consider); dirtyDel holds
	// deletes (merge-only — the merger removes their stale index entry, but reads
	// skip them). Appended under mu by live writes; both drained by mergeIndexes.
	// See docs/secondary-indexes.md.
	dirtyRead []UUID
	dirtyDel  []UUID
}

func newTable(def *resolvedTable, sizeHint int, budget *byteBudget) *table {
	n := shardCount()
	t := &table{
		def:    def,
		shards: make([]tableShard, n),
		mask:   uint32(n - 1),
		budget: budget,
	}
	per := sizeHint / n
	if per < 16 {
		per = 16
	}
	for i := range t.shards {
		t.shards[i].rows = make([]Row, 0, per)
		if def.partitioned() {
			t.shards[i].tails = make(map[UUID][]uint64)
			t.shards[i].tailsDead = make(map[UUID]int)
		} else {
			t.shards[i].pk.init(per)
		}
	}
	if def.partitioned() {
		t.pkDir = &pkDirectory{idx: make(map[UUID]rowLocation, sizeHint)}
	}
	for _, ri := range def.indexes {
		t.indexes = append(t.indexes, newSecIndex(ri))
	}
	return t
}

// shardIdxOf maps a UUID to a shard index. For a non-partitioned table the UUID
// is the PK; for a partitioned table it is the PartitionKey value (so all rows of
// one partition land in one shard). The hash is a multiplicative (Fibonacci) fold
// of both 64-bit halves, returning high, well-mixed bits. Reads all 16 bytes so
// the spread holds wherever the entropy sits — random low bytes of a real UUIDv7,
// or the high timestamp bytes of a sequential key. Not persisted (the WAL stores
// rows, never shard indices), so the constant can change freely.
func (t *table) shardIdxOf(u UUID) uint32 {
	a := binary.LittleEndian.Uint64(u[0:8])
	b := binary.LittleEndian.Uint64(u[8:16])
	h := (a ^ bits.RotateLeft64(b, 32)) * 0x9E3779B97F4A7C15
	return uint32(h>>32) & t.mask
}

func (t *table) shardOf(u UUID) *tableShard { return &t.shards[t.shardIdxOf(u)] }
