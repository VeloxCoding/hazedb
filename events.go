package hazedb

import (
	"log"
	"runtime/debug"
	"time"
)

// Operational event logging. Rare, operator-relevant occurrences — a corrupt WAL
// segment, a failing drain — are recorded both to the standard logger and, in the
// always-present SQLite companion, to the _hz_events table, so a dashboard can
// query a persistent history with a plain SELECT. _hz_events is created during
// openCompanion, so it exists in every mode, memory-only included.
//
// This is NOT the path for metrics or per-request performance counters: those
// stay in memory and are read through the stats endpoints, and a high-frequency
// occurrence (e.g. a capacity rejection) is counted, not logged once per event.

// reservedTablePrefix is the namespace hazedb reserves for its own companion
// tables (_hz_events, _hz_meta, _hz_tables). A user CREATE TABLE in this
// namespace is rejected so user data can never collide with internal tables.
const reservedTablePrefix = "_hz_"

// logEvent records one operational event: to the standard logger always, and to
// the companion's _hz_events table when the companion is open. level is a
// severity tag ("info"/"warn"/"error"); kind is a short stable category
// ("wal-corruption", "drain-error", ...); message is an already-formatted line.
// Logging never fails the caller — a companion write error is itself logged and
// swallowed.
//
// The caller must NOT hold an open companion transaction: the companion uses a
// single connection, so the INSERT here would deadlock against an open tx on the
// same goroutine. The drain therefore logs only after it commits.
func (db *DB) logEvent(level, kind, message string) {
	log.Printf("hazedb %s [%s]: %s", level, kind, message)
	if db.sq == nil {
		return
	}
	if _, err := db.sq.sdb.Exec(
		`INSERT INTO _hz_events (ts, level, kind, message) VALUES (?, ?, ?, ?)`,
		time.Now().UnixMilli(), level, kind, message,
	); err != nil {
		log.Printf("hazedb error [events]: recording %q event failed: %v", kind, err)
	}
}

// runRecovered runs fn with a deferred recover, so a panic in one iteration of a
// long-lived background goroutine (drain, compaction, index merge, WAL flush) is
// logged and the loop survives to its next tick instead of taking the process
// down. hazedb is embedded in-process (Caddy/FrankenPHP): an unrecovered panic in
// any of these goroutines crashes the whole host — unlike a request-path panic,
// which the HTTP and cgo boundaries already recover.
//
// It logs only to the standard logger (with a stack), never to the _hz_events
// companion table: a panic may have left a mirror transaction or a lock in an
// indeterminate state, and the companion is single-connection, so an INSERT here
// could deadlock (see logEvent). Stderr always works.
//
// NOTE: recovering a panic does NOT release resources the panicked code held (a
// half-open mirror tx, a lock not guarded by defer). This prevents the process
// crash, not every downstream stall — per-work-unit cleanup (a deferred tx
// rollback, defer-unlock) is the deeper hardening, tracked separately.
func runRecovered(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("hazedb error [background-panic]: recovered panic in %s: %v\n%s", name, r, debug.Stack())
		}
	}()
	fn()
}
