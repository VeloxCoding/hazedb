package hazedb

import (
	"log"
	"time"
)

// Operational event logging. Rare, operator-relevant occurrences — a corrupt WAL
// segment, a failing drain — are recorded both to the standard logger and, in the
// always-present SQLite companion, to the _hz_events table, so a dashboard can
// query a persistent history with a plain SELECT. _hz_events is created in
// openCompanion (see ensureOps), so it exists in every mode, memory-only included.
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
