package hazedb

import "log"

// Operational event logging. Rare, operator-relevant occurrences — a corrupt WAL
// segment, a failing drain — are surfaced here so they are visible instead of
// swallowed. For now they go to the standard logger; a later step also records
// them in the SQLite mirror (an _hz_events table) so a dashboard can query them.
//
// This is NOT the path for metrics or per-request performance counters: those
// stay in memory and are read through the stats endpoints, never logged per event.

// logEvent reports one operational event. kind is a short stable tag
// (e.g. "wal-corruption", "drain-error"); detail is an already-formatted,
// human-readable description.
func (db *DB) logEvent(kind, detail string) {
	log.Printf("hazedb %s: %s", kind, detail)
}
