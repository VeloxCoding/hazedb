package hazedb

import (
	"encoding/json"
	"sort"
	"unsafe"
)

// MetaSnapshot / StoreMeta give a lock-light overview of what the store holds:
// the table count plus, per table, row count, column count, secondary-index
// count, and an in-RAM size. For operator dashboards and health checks — a read
// path, never the write path.
//
// The size walks every live row under a brief per-shard RLock and sums each
// cell's exact footprint, so the payload term — the part that varies — is
// exact, not sampled. It adds the fixed per-row overhead and a flat per-row
// charge for each secondary index, both modeled constants biased slightly high.
// Cost is O(live rows): right for an occasional dashboard hit, not for a
// per-write budget check — a future byte cap maintains its own O(1) counter on
// the write path rather than calling this.
type StoreMeta struct {
	Tables int `json:"tables"`
	// MaxBytes is the configured byte cap (Options.MaxBytes); 0 means unlimited.
	// TotalApproxBytes is what it is measured against — an insert is rejected once
	// the total would exceed it.
	MaxBytes int64 `json:"max_bytes"`
	// TotalRows / TotalApproxBytes / TotalTombstones roll up every table's line, so
	// a caller gets the store-wide figures without summing TableStats itself.
	// TotalApproxBytes is the sum of the per-table estimates and inherits their
	// slight over-bias.
	TotalRows        int         `json:"total_rows"`
	TotalApproxBytes int64       `json:"total_approx_bytes"`
	TotalTombstones  int         `json:"total_tombstones"`
	TableStats       []TableStat `json:"table_stats"`
}

// TableStat is one table's line in a StoreMeta. ApproxBytes sums exact cell
// payloads plus modeled overhead (see StoreMeta); a display layer formats it to
// MiB.
//
// Tombstones is the count of dead arena slots — rows deleted but whose arena
// slot is not yet reclaimed (the store never compacts a running arena; only a
// restart-from-checkpoint does). It is a MEMORY signal: a high
// Tombstones/(Rows+Tombstones) fraction on an insert+delete workload means the
// arena carries dead weight until the next restart. (Partitioned SCAN cost is no
// longer tied to it — the delete path prunes a partition's scan list of dead
// rowIDs, so scanPartition stays O(live). Reclaiming the arena slots themselves
// is a separate, open step.)
type TableStat struct {
	Name        string `json:"name"`
	Rows        int    `json:"rows"`
	Columns     int    `json:"columns"`
	Indexes     int    `json:"indexes"`
	ApproxBytes int64  `json:"approx_bytes"`
	Tombstones  int    `json:"tombstones"`
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
)

// approxBytes returns a cell's in-RAM footprint: the 32-byte packed Value plus
// any string/bytes backing it points at (w0 holds that backing's length). Exact
// for the cell payload; the per-row/per-index overheads are added by the caller.
func (v Value) approxBytes() int {
	n := int(unsafe.Sizeof(Value{}))
	if v.Kind == KindString || v.Kind == KindBytes {
		n += int(v.w0)
	}
	return n
}

// rowCost is one row's contribution to a shard's byte tally: every cell's exact
// footprint plus the fixed per-row overhead and a flat charge per secondary
// index. nIdx is the table's index count (fixed at CREATE TABLE — hazedb has no
// runtime CREATE INDEX — so a row's cost never shifts under it). The running
// tally maintained with this equals what a full live-row walk would sum, which
// reconcileBytes asserts in tests.
func rowCost(row Row, nIdx int) int64 {
	cells := 0
	for _, v := range row {
		cells += v.approxBytes()
	}
	return int64(cells + rowFixedOverhead + nIdx*perIndexRowOverhead)
}

// cellDelta is the byte-tally change from replacing one cell's value. Only
// string/bytes backing lengths move it — the fixed 32-byte Value cancels in the
// difference — so a same-kind fixed-width SET (the common int/uuid update)
// computes to zero with two Kind checks and no arithmetic. The in-place UPDATE
// paths sum it over the SET columns, keeping the running tally exact without a
// full-row rescan; a whole-row replacement uses rowCost(new) - rowCost(old).
func cellDelta(oldv, newv Value) int64 {
	var d int64
	if newv.Kind == KindString || newv.Kind == KindBytes {
		d += int64(newv.w0)
	}
	if oldv.Kind == KindString || oldv.Kind == KindBytes {
		d -= int64(oldv.w0)
	}
	return d
}

// liveStats returns the live row count, dead-slot (tombstone) count, and in-RAM
// byte size for t by reading the per-shard counters under a brief RLock —
// O(shards), not O(rows). live/bytes are running counters; tombstones is the
// arena length minus live, i.e. deleted-but-not-reclaimed slots.
func (t *table) liveStats() (rows, tombstones int, approxBytes int64) {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		rows += s.live
		tombstones += len(s.rows) - s.live
		approxBytes += s.bytes
		s.mu.RUnlock()
	}
	return rows, tombstones, approxBytes
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
	if db.budget != nil {
		out.MaxBytes = db.budget.max
	}
	for _, rt := range cat.byName {
		t := rt.table
		rows, tombstones, bytes := t.liveStats()
		out.TableStats = append(out.TableStats, TableStat{
			Name:        rt.name(),
			Rows:        rows,
			Columns:     len(t.def.def.Columns),
			Indexes:     len(t.indexes),
			ApproxBytes: bytes,
			Tombstones:  tombstones,
		})
		out.TotalRows += rows
		out.TotalApproxBytes += bytes
		out.TotalTombstones += tombstones
	}
	sort.Slice(out.TableStats, func(i, j int) bool {
		return out.TableStats[i].Name < out.TableStats[j].Name
	})
	return out
}

// MetaJSON renders MetaSnapshot as a JSON object for the out-of-core adapters
// (Caddy GET /meta, PHP hazedb_meta) — one wire-shape definition both share, so
// the HTTP and cgo surfaces always emit identical bytes. Cold operator path, so
// it uses stdlib json.Marshal (like ExecResultJSON/ErrorJSON) rather than the
// hand-rolled row encoder. Never errors — the struct is plain data.
func (db *DB) MetaJSON() []byte {
	b, _ := json.Marshal(db.MetaSnapshot())
	return b
}
