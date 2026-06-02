package hazedb

import (
	"sort"
	"unsafe"
)

// MetaSnapshot / StoreMeta give a lock-light overview of what the store holds:
// the table count plus, per table, row count, column count, secondary-index
// count, and an approximate in-RAM size. For operator dashboards and health
// checks — not the hot path.
//
// Sizes are deliberate ESTIMATES. The snapshot never walks the whole arena and
// adds nothing to the write path: per shard it loads the live count and samples
// up to sampleTargetRows live rows for an average cell-byte size (under the same
// brief RLock liveCount uses), scales that average by the live count, and adds a
// fixed per-row overhead plus a flat per-row charge for each secondary index.
// It answers "how big, roughly" and sizes a future byte cap — it is not
// byte-exact, and is biased to slightly over-estimate so a cap trips early.
type StoreMeta struct {
	Tables     int
	TableStats []TableStat
}

// TableStat is one table's line in a StoreMeta. ApproxBytes is an estimate (see
// StoreMeta); a display layer formats it to MiB.
type TableStat struct {
	Name        string
	Rows        int
	Columns     int
	Indexes     int
	ApproxBytes int64
}

const (
	// rowFixedOverhead estimates the per-row bookkeeping outside the cell data:
	// the Row slice header, the PK-map entry (16-byte key + 8-byte rowID + bucket
	// share), and the arena slot. A round over-estimate on purpose.
	rowFixedOverhead = 64
	// perIndexRowOverhead estimates what each secondary index adds per row: a
	// UUID in the forward bucket, a reverse-map entry, and (ordered indexes) a
	// sort-view entry.
	perIndexRowOverhead = 48
	// sampleTargetRows bounds the per-shard sample so the snapshot stays O(1) in
	// table size: at most this many live rows are measured per shard for the
	// average-cell-size estimate, regardless of how large the table is.
	sampleTargetRows = 256
)

// approxBytes estimates a cell's in-RAM footprint: the 32-byte packed Value plus
// any string/bytes backing it points at (w0 holds that backing's length). An
// approximation for MetaSnapshot, not exact accounting.
func (v Value) approxBytes() int {
	n := int(unsafe.Sizeof(Value{}))
	if v.Kind == KindString || v.Kind == KindBytes {
		n += int(v.w0)
	}
	return n
}

// sampleStats returns the live row count and an approximate in-RAM byte size for
// t in a single per-shard RLock sweep (see StoreMeta). Empty shards contribute
// nothing; a shard's bytes are its sampled average cell size plus the fixed
// per-row and per-index overhead, scaled by its live count.
func (t *table) sampleStats() (rows int, approxBytes int64) {
	nIdx := len(t.indexes)
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		live := s.live
		var sampleBytes, sampleRows int
		for _, r := range s.rows {
			if sampleRows >= sampleTargetRows {
				break
			}
			if r == nil { // tombstone
				continue
			}
			for _, v := range r {
				sampleBytes += v.approxBytes()
			}
			sampleRows++
		}
		s.mu.RUnlock()

		rows += live
		if live == 0 {
			continue
		}
		avgCell := 0
		if sampleRows > 0 {
			avgCell = sampleBytes / sampleRows
		}
		perRow := int64(avgCell + rowFixedOverhead + nIdx*perIndexRowOverhead)
		approxBytes += perRow * int64(live)
	}
	return rows, approxBytes
}

// MetaSnapshot returns the current StoreMeta. It loads the catalog atomically
// (lock-free) and reads each table with one brief per-shard RLock sweep. Stats
// are sorted by table name for a stable overview.
func (db *DB) MetaSnapshot() StoreMeta {
	cat := db.cat.Load()
	out := StoreMeta{
		Tables:     len(cat.byName),
		TableStats: make([]TableStat, 0, len(cat.byName)),
	}
	for _, rt := range cat.byName {
		t := rt.table
		rows, bytes := t.sampleStats()
		out.TableStats = append(out.TableStats, TableStat{
			Name:        rt.name(),
			Rows:        rows,
			Columns:     len(t.def.def.Columns),
			Indexes:     len(t.indexes),
			ApproxBytes: bytes,
		})
	}
	sort.Slice(out.TableStats, func(i, j int) bool {
		return out.TableStats[i].Name < out.TableStats[j].Name
	})
	return out
}
