package hazedb

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Defaults for the tunable fields, in one place. applyDefaults fills any field
// left at its zero value; a NEGATIVE duration is preserved as "explicitly
// disabled" and is never overwritten.
const (
	defaultSizeHint           = 1024
	defaultFlushInterval      = 500 * time.Millisecond
	defaultDrainInterval      = time.Second
	defaultIndexMergeInterval = 50 * time.Millisecond
	defaultCompanionFile      = "hazedb.db" // companion filename inside WALPath
)

// Options configure Open. The EXPORTED fields are the entire operator surface;
// the unexported fields are internal tuning, settable only from package tests
// (the defaults are right for every deployment). Genuine implementation
// constants — buffer sizes, flush triggers, SQLite PRAGMAs, segment naming,
// shard counts — are not settings and live next to the code that uses them.
type Options struct {
	// Schema declares the tables present at startup. May be empty — tables can
	// also be created at runtime with CREATE TABLE.
	Schema Schema

	// WALPath is the directory holding the write-ahead log segments, created if
	// absent. Setting it turns the WAL ON; leaving it empty is memory-only
	// (nothing survives a restart). The WAL has one durability story: a write is
	// sealed to disk within ~0.5s (or sooner once 1 MiB has accumulated), so a
	// crash loses at most that window. Acknowledge-after-fsync is intentionally
	// not offered — use a disk-first database if you need it.
	WALPath string

	// CompanionPath is where the SQLite companion file lives — always a real file on
	// disk, present in EVERY mode (WAL on or off). It is hazedb's always-on store:
	// the _hz_events operational log (logging, health, later perf samples) and,
	// when WAL is on, the data mirror + recovery base. Set it to pin hazedb.db at
	// one fixed path regardless of WAL. Empty defaults to "hazedb.db" inside WALPath
	// when WAL is on, else "hazedb.db" in the working directory. Never in-memory.
	CompanionPath string

	// MaxBytes caps the store's approximate in-RAM size (the sum of every table's
	// byte tally, the same figure MetaSnapshot reports). An INSERT that would push
	// the total past it is rejected with ErrCapacity; the store never auto-evicts,
	// so the caller frees space with DELETE / DROP TABLE. 0 (the default) is
	// unlimited and adds no write-path cost.
	MaxBytes int64

	// --- internal tuning (package tests only; not operator-facing) ---

	// sizeHint pre-sizes shard arenas (a per-table row-count estimate).
	sizeHint int
	// walFlushInterval is how often the background flusher seals the pending
	// buffer into a segment. Zero = 500ms; negative disables the flusher (manual
	// flush() / Close only, for deterministic segment-count tests). The 1 MiB
	// size trigger always applies regardless.
	walFlushInterval time.Duration
	// drainInterval is how often the drain feeds sealed segments into SQLite.
	// Zero = 1s; negative disables the loop (manual drain, for tests).
	drainInterval time.Duration
	// indexMergeInterval is how often secondary indexes reconcile dirty rows.
	// Zero = 50ms; negative disables it (manual merge, for pre-merge assertions).
	indexMergeInterval time.Duration
	// compactInterval is how often the background arena-compaction sweeper runs.
	// Zero = 200ms (defaultCompactInterval); negative disables it. See compact.go.
	compactInterval time.Duration
	// indexMergeThreshold is the size-trigger: the merger fires early (before
	// indexMergeInterval elapses) when the dirty overlay grows dense. Zero is
	// ADAPTIVE (a quarter of a table's live rows); positive is a fixed total;
	// negative disables the size-trigger. The merger polls these itself, so this
	// never touches the write path.
	indexMergeThreshold int64
}

// validate rejects contradictory configs before any resource is opened. Every
// WAL/companion combination is legal: in-RAM store (no WAL), WAL-only, WAL +
// mirror, and a companion with no WAL (ops-only). The one hard rule is that the
// companion is always an on-disk file — an explicit in-memory DSN is rejected,
// since the companion must survive a restart to serve logging, health, and the
// data mirror. An empty CompanionPath is fine here: applyDefaults turns it into a
// real file path.
func (o *Options) validate() error {
	if isInMemoryDSN(o.CompanionPath) {
		return fmt.Errorf("%w: CompanionPath=%q", ErrCompanionInMemory, o.CompanionPath)
	}
	return nil
}

// isInMemoryDSN reports whether a SQLite path/DSN opens an in-memory database
// rather than a file. Covers the bare ":memory:" form, the "file::memory:" URI,
// and any DSN carrying the mode=memory query parameter.
func isInMemoryDSN(path string) bool {
	return strings.Contains(path, ":memory:") || strings.Contains(path, "mode=memory")
}

// walEnabled reports whether on-disk persistence is on.
func (o *Options) walEnabled() bool { return o.WALPath != "" }

// mirrorEnabled reports whether the data mirror is active — exactly when WAL is
// on. The companion file is always present (it also holds the _hz_events log with
// no WAL); the mirror is the data-replication role layered on top when WAL is on.
func (o *Options) mirrorEnabled() bool { return o.walEnabled() }

// applyDefaults fills unset (zero) fields. Negative values are left intact (they
// mean "disabled"). The drain default applies only when the mirror is enabled.
func (o *Options) applyDefaults() {
	if o.sizeHint <= 0 {
		o.sizeHint = defaultSizeHint
	}
	if o.walFlushInterval == 0 {
		o.walFlushInterval = defaultFlushInterval
	}
	if o.indexMergeInterval == 0 {
		o.indexMergeInterval = defaultIndexMergeInterval
	}
	if o.compactInterval == 0 {
		o.compactInterval = defaultCompactInterval
	}
	// The companion is always a real file on disk. Default it to "hazedb.db" inside
	// WALPath when WAL is on, else "hazedb.db" in the working directory.
	if o.CompanionPath == "" {
		if o.walEnabled() {
			o.CompanionPath = filepath.Join(o.WALPath, defaultCompanionFile)
		} else {
			o.CompanionPath = defaultCompanionFile
		}
	}
	if o.mirrorEnabled() && o.drainInterval == 0 {
		o.drainInterval = defaultDrainInterval
	}
}
