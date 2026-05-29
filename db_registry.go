// db_registry.go — process-wide named *DB lookup for in-process consumers
// that don't otherwise hold a handle on the database the Caddy module opened.
//
// When hazedb runs as a Caddy module, the adapter's Provision() opens a *DB
// and serves HTTP from it. A PHP extension (or any other in-process caller)
// that wants to hit the SAME database instance the HTTP routes serve cannot
// reach the adapter's unexported handle through ordinary import. This registry
// solves that without coupling the core to any adapter: the caddymodule
// registers its *DB under a conventional name ("default") during Provision and
// clears it during Cleanup; consumers look it up at use time.
//
// Storage shape mirrors scopecache's gateway registry: a name maps to a stable
// atomic.Pointer slot (created lazily, never removed), so hot-path consumers
// cache the slot once and call Load() per use — a single lock-free atomic load,
// no registry RLock per call. Caddy config reload swaps the slot contents
// atomically; DeregisterDBIf uses a CAS so a stale Cleanup never clobbers a
// newer Provision's registration. In-flight callers holding a *DB from a prior
// Load stay valid (the GC keeps a referenced *DB alive).

package hazedb

import (
	"sync"
	"sync/atomic"
)

var (
	dbRegistryMu    sync.RWMutex
	dbRegistrySlots = map[string]*atomic.Pointer[DB]{}
)

// dbSlotForName returns the slot for name, lazily creating it under the write
// lock. The returned slot is stable for the process lifetime; callers may cache it.
func dbSlotForName(name string) *atomic.Pointer[DB] {
	dbRegistryMu.RLock()
	slot, ok := dbRegistrySlots[name]
	dbRegistryMu.RUnlock()
	if ok {
		return slot
	}
	dbRegistryMu.Lock()
	defer dbRegistryMu.Unlock()
	if slot, ok = dbRegistrySlots[name]; ok {
		return slot
	}
	slot = &atomic.Pointer[DB]{}
	dbRegistrySlots[name] = slot
	return slot
}

// RegisterDB publishes db under name. Pass nil to clear unconditionally; from
// Cleanup-style paths use DeregisterDBIf to avoid clobbering a newer registration.
func RegisterDB(name string, db *DB) {
	dbSlotForName(name).Store(db)
}

// LookupDB returns the *DB registered under name, or nil. Convenience wrapper;
// hot-path consumers should cache the slot via LookupDBSlot and Load() per use.
func LookupDB(name string) *DB {
	return dbSlotForName(name).Load()
}

// LookupDBSlot returns the stable atomic slot for name (lazily created). Cache
// it once and Load() per use:
//
//	var defaultSlot = hazedb.LookupDBSlot("default")
//	db := defaultSlot.Load() // nil until a caddymodule Provision registers one
func LookupDBSlot(name string) *atomic.Pointer[DB] {
	return dbSlotForName(name)
}

// DeregisterDBIf clears the slot only if it still holds db (CAS). Safe for
// caddymodule Cleanup during reload: preserves a newer Provision's registration.
func DeregisterDBIf(name string, db *DB) {
	dbSlotForName(name).CompareAndSwap(db, nil)
}
