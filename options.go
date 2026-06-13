package hazedb

import (
	"path/filepath"
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
	defaultCompanionFile      = "hazedb.db" // default companion filename
)

// noWALCompanionDefault is the companion path used when CompanionPath is empty and
// WAL is off — a file in the working directory. It is a var, not a const, only so
// package tests can set it to ":memory:" (in an init) and avoid scattering a
// hazedb.db across the repo; production never changes it.
var noWALCompanionDefault = defaultCompanionFile

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

	// CompanionPath is the SQLite companion — hazedb's always-present sidecar for
	// operational data (the _hz_events log; later, periodic /meta samples) and the
	// data mirror. Left empty it defaults to a FILE:
	//   - WAL on  → "hazedb.db" inside WALPath: the data mirror and events persist
	//     there, and the file is the recovery base.
	//   - WAL off → "hazedb.db" in the working directory: a memory-only store keeps
	//     its rows in RAM, but the companion still persists the _hz_events log.
	// Set it to a path to choose the location (a ramdisk path for an ephemeral
	// file), or to ":memory:" to keep the companion in-memory (with WAL on that is
	// WAL-only mode: segments replay into memory on boot, no mirror — the drain
	// deletes drained segments, so an in-memory mirror could not be a recovery base).
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
// WAL/companion combination is currently legal: memory-only, WAL-only (no
// mirror), WAL + mirror, and a companion file with no WAL (ops-only).
func (o *Options) validate() error {
	return nil
}

// walEnabled reports whether on-disk persistence is on.
func (o *Options) walEnabled() bool { return o.WALPath != "" }

// mirrorEnabled reports whether the data mirror is active: the WAL feeds it and
// the companion is a real file (not in-memory). An in-memory companion cannot be
// a recovery base — the drain deletes WAL segments once mirrored — so ":memory:"
// (set explicitly, or the WAL-off default) means WAL-only. Call only after
// applyDefaults, which resolves an empty path to a file or ":memory:".
func (o *Options) mirrorEnabled() bool { return o.walEnabled() && o.CompanionPath != ":memory:" }

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
	// Default the companion to a FILE: inside WALPath when durable (data mirror +
	// events persist, recovery base), else "hazedb.db" in the working directory
	// (memory-only data stays in RAM, but the _hz_events log still persists).
	// noWALCompanionDefault is ":memory:" under test so the suite writes no files.
	if o.CompanionPath == "" {
		if o.walEnabled() {
			o.CompanionPath = filepath.Join(o.WALPath, defaultCompanionFile)
		} else {
			o.CompanionPath = noWALCompanionDefault
		}
	}
	if o.mirrorEnabled() && o.drainInterval == 0 {
		o.drainInterval = defaultDrainInterval
	}
}
