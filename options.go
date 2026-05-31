package hazedb

import "time"

// Defaults for the tunable Options fields, in one place. applyDefaults fills any
// field left at its zero value; a NEGATIVE value is preserved as "explicitly
// disabled" (e.g. DrainInterval < 0 = no drain loop), so it is never overwritten.
//
// These are the only knobs an operator sets. Genuine implementation constants —
// the WAL buffer size, the SQLite PRAGMAs and one-connection limit, segment file
// naming, shard counts, CRC/magic — are NOT settings: they live next to the code
// that uses them and are deliberately not exposed here.
const (
	defaultSizeHint           = 1024
	defaultWALFlushInterval   = time.Second
	defaultWALRotateInterval  = 5 * time.Second
	defaultDrainInterval      = time.Minute
	defaultSegmentDrainMinAge = 5 * time.Second
	defaultIndexMergeInterval = 50 * time.Millisecond
)

// Options control Open behaviour. Fields are grouped by who is expected to set
// them: the first two groups are operator-facing (a deployment legitimately
// tunes them); the last group is hazedb's own tuning, where the default is right
// for almost everyone and is exposed mainly for tests and unusual workloads.
type Options struct {
	// --- Required ---

	// Schema declares the tables. May be empty — tables can be created at
	// runtime with CREATE TABLE.
	Schema Schema

	// --- Operator settings: where the data lives, and how durable/fast ---

	// WALPath is the on-disk write-ahead log. Empty = memory-only (no
	// durability). In segmented mode (see SQLitePath / WALRotateInterval) this
	// is a DIRECTORY of segment files; otherwise a single file. Created if absent.
	WALPath string

	// SizeHint is a per-table row-count estimate for shard arena pre-allocation.
	// Zero or negative = a small default.
	SizeHint int

	// WALFlushInterval is how often the background goroutine flushes the WAL
	// buffer to the OS (and fsyncs when WALSync is set). Zero = 1s; a negative
	// value disables the ticker (manual FlushWAL() only). Ignored without WALPath.
	WALFlushInterval time.Duration

	// WALSync makes the flush ticker fsync when anything is dirty, bounding
	// power-loss to <= WALFlushInterval. Default false (flush only: survives a
	// process crash, not power loss).
	WALSync bool

	// WALSyncPerWrite flushes and fsyncs after every WAL record, under the WAL
	// lock — strongest durability, highest per-write cost. Overrides the ticker.
	WALSyncPerWrite bool

	// SQLitePath enables the on-disk SQLite mirror at this path: a background loop
	// drains sealed WAL segments into it (current, compacted, queryable state),
	// and it becomes the recovery source. Empty = no mirror. Requires WALPath and
	// forces segmented mode. See docs/durability.md.
	SQLitePath string

	// DrainInterval is how often sealed segments are drained into SQLite — the
	// trade between recovery-tail size / IO and freshness of the mirror. Zero =
	// 1 minute; negative disables the loop (manual drain, tests). Needs SQLitePath.
	DrainInterval time.Duration

	// --- Advanced: hazedb's own tuning. The default is right for almost
	// everyone; override mainly for tests or unusual workloads. ---

	// WALRotateInterval is how often the active WAL segment is sealed and a new
	// one opened. Zero keeps the single-file WAL; a positive value turns on
	// segmented mode on its own. Forced to 5s when SQLitePath is set (the drain
	// consumes sealed segments).
	WALRotateInterval time.Duration

	// SegmentDrainMinAge skips segments sealed more recently than this, so the
	// drain only touches settled history. Zero = 5s; negative disables the gate.
	// Ignored without SQLitePath.
	SegmentDrainMinAge time.Duration

	// IndexMergeInterval is how often the background goroutine reconciles
	// secondary indexes with the dirty rows. Zero = 50ms; negative disables it
	// (manual merge — used by tests that assert pre-merge state). Cheap when no
	// table has an index.
	IndexMergeInterval time.Duration
}

// applyDefaults fills unset (zero) fields from the default constants. Negative
// values are left intact (they mean "disabled"). The SQLite-mirror defaults are
// only applied when a mirror is configured.
func (o *Options) applyDefaults() {
	if o.SizeHint <= 0 {
		o.SizeHint = defaultSizeHint
	}
	if o.WALFlushInterval == 0 {
		o.WALFlushInterval = defaultWALFlushInterval
	}
	if o.IndexMergeInterval == 0 {
		o.IndexMergeInterval = defaultIndexMergeInterval
	}
	if o.SQLitePath != "" {
		if o.WALRotateInterval <= 0 {
			o.WALRotateInterval = defaultWALRotateInterval
		}
		if o.DrainInterval == 0 {
			o.DrainInterval = defaultDrainInterval
		}
		if o.SegmentDrainMinAge == 0 {
			o.SegmentDrainMinAge = defaultSegmentDrainMinAge
		}
	}
}
