package hazedb

import (
	"fmt"
	"time"
)

// WALLevel selects on-disk durability — the single switch for the write-ahead
// log. WALPath turns nothing on by itself; the level does.
const (
	WALOff      = iota // 0: memory only; nothing survives a crash.
	WALPeriodic        // 1: writes fsynced by a background ticker (~1s); the caller never waits.
	WALPerWrite        // 2: every write fsynced before the caller gets a reply — slowest, safest.
)

// Defaults for the tunable fields, in one place. applyDefaults fills any field
// left at its zero value; a NEGATIVE duration is preserved as "explicitly
// disabled" and is never overwritten.
const (
	defaultSizeHint           = 1024
	defaultWALFlushInterval   = time.Second
	defaultWALRotateInterval  = 5 * time.Second
	defaultIndexMergeInterval = 50 * time.Millisecond
)

// Options configure Open. The EXPORTED fields are the entire operator surface;
// the unexported fields are internal tuning, settable only from package tests
// (the defaults are right for every deployment). Genuine implementation
// constants — buffer sizes, SQLite PRAGMAs, segment naming, shard counts — are
// not settings and live next to the code that uses them.
type Options struct {
	// Schema declares the tables present at startup. May be empty — tables can
	// also be created at runtime with CREATE TABLE.
	Schema Schema

	// WALLevel is the durability switch: WALOff (memory only), WALPeriodic
	// (background fsync), or WALPerWrite (fsync per write). Above WALOff, WALPath
	// is required; setting WALPath or SQLitePath with WALOff is rejected.
	WALLevel int

	// WALPath is the directory holding the write-ahead log segments, created if
	// absent. Required when WALLevel > 0.
	WALPath string

	// WALRotateInterval is how often the active segment is sealed and the next
	// opened, so a drain (or any external consumer) reads settled segments and
	// the log never grows as one unbounded file. Zero = 5s.
	WALRotateInterval time.Duration

	// SQLitePath enables the on-disk SQLite mirror: the drain feeds sealed
	// segments into it (compacted current state) and it becomes the recovery
	// source. Empty = no mirror (the WAL itself replays into memory on boot).
	// Requires WALLevel > 0.
	SQLitePath string

	// MaxBytes caps the store's approximate in-RAM size (the sum of every table's
	// byte tally, the same figure MetaSnapshot reports). An INSERT that would push
	// the total past it is rejected with ErrCapacity; the store never auto-evicts,
	// so the caller frees space with DELETE / DROP TABLE. 0 (the default) is
	// unlimited and adds no write-path cost. The estimate counts cell payloads
	// plus fixed per-row and per-index overhead, biased slightly high.
	MaxBytes int64

	// --- internal tuning (package tests only; not operator-facing) ---

	// sizeHint pre-sizes shard arenas (a per-table row-count estimate).
	sizeHint int
	// walFlushInterval is the WALPeriodic flush/fsync ticker period. Zero = 1s;
	// negative disables the ticker (manual FlushWAL only).
	walFlushInterval time.Duration
	// drainInterval is how often sealed segments drain into SQLite. Zero =
	// WALRotateInterval; negative disables the loop (manual drain).
	drainInterval time.Duration
	// indexMergeInterval is how often secondary indexes reconcile dirty rows.
	// Zero = 50ms; negative disables it (manual merge, for pre-merge assertions).
	indexMergeInterval time.Duration
	// compactInterval is how often the background arena-compaction sweeper runs.
	// Zero = 1s; negative disables it (manual compactShard only, for tests). The
	// sweeper reclaims dead arena slots from shards that have gone more-than-half
	// dead, off the write path — see compact.go.
	compactInterval time.Duration
	// indexMergeThreshold is the size-trigger: the merger fires early (before
	// indexMergeInterval elapses) when the dirty overlay grows dense, bounding
	// overlay growth under a write burst. Zero (the default) is ADAPTIVE — a
	// table fires when its overlay reaches a quarter of its live rows (floored,
	// so a near-empty table does not merge-spam). A positive value is a fixed
	// absolute total-overlay threshold (explicit tuning). Negative disables the
	// size-trigger (pure time-trigger). The merger polls these counters itself,
	// so this never touches the write path.
	indexMergeThreshold int64
}

// validate rejects contradictory configs before any resource is opened. The
// level is the switch: a path without a level (or a level without a path) is a
// mistake, not a silent no-op.
func (o *Options) validate() error {
	switch o.WALLevel {
	case WALOff:
		if o.WALPath != "" {
			return fmt.Errorf("hazedb: WALPath is set but WALLevel is WALOff — set WALLevel to WALPeriodic or WALPerWrite, or clear WALPath")
		}
		if o.SQLitePath != "" {
			return fmt.Errorf("hazedb: SQLitePath is set but WALLevel is WALOff — the mirror is fed from the WAL")
		}
	case WALPeriodic, WALPerWrite:
		if o.WALPath == "" {
			return fmt.Errorf("hazedb: WALLevel > 0 requires WALPath")
		}
	default:
		return fmt.Errorf("hazedb: invalid WALLevel %d (want WALOff, WALPeriodic, or WALPerWrite)", o.WALLevel)
	}
	return nil
}

// walEnabled reports whether on-disk persistence is on.
func (o *Options) walEnabled() bool { return o.WALLevel != WALOff }

// walSync / walSyncPerWrite translate the level into the two fsync flags the wal
// layer takes. WALPeriodic fsyncs on the ticker; WALPerWrite fsyncs every write.
func (o *Options) walSync() bool         { return o.WALLevel == WALPeriodic }
func (o *Options) walSyncPerWrite() bool { return o.WALLevel == WALPerWrite }

// applyDefaults fills unset (zero) fields. Negative values are left intact (they
// mean "disabled"). WAL-dependent defaults apply only when the WAL is enabled.
func (o *Options) applyDefaults() {
	if o.sizeHint <= 0 {
		o.sizeHint = defaultSizeHint
	}
	if o.indexMergeInterval == 0 {
		o.indexMergeInterval = defaultIndexMergeInterval
	}
	if o.compactInterval == 0 {
		o.compactInterval = defaultCompactInterval
	}
	if !o.walEnabled() {
		return
	}
	if o.walFlushInterval == 0 {
		o.walFlushInterval = defaultWALFlushInterval
	}
	if o.WALRotateInterval == 0 {
		o.WALRotateInterval = defaultWALRotateInterval
	}
	if o.SQLitePath != "" && o.drainInterval == 0 {
		// Drain after every rotation: the sealed segment is the unit of work.
		o.drainInterval = o.WALRotateInterval
	}
}
