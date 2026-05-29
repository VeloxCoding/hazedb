// hazedb_ext.go — FrankenPHP extension exposing hazedb's *DB directly to PHP,
// so PHP runs SQL in-process (a Go function call) instead of over a socket or
// HTTP. PHP and the Caddy module share one *DB through the process-wide
// registry in the core (db_registry.go): the caddymodule registers it under
// "default" during Provision; defaultSlot here Loads it per call. A Caddy
// config reload swaps the slot atomically — nothing to invalidate here.
//
// Three PHP functions:
//
//	hazedb_query(string $sql, string $args): ?string
//	    Run a SELECT. Returns {"columns":[...],"rows":[[...],...]} as a JSON
//	    string, {"error":"..."} on a SQL error, or PHP null if no Caddy module
//	    has registered a DB. $args (see QueryArgs): "" = none; a value starting
//	    with '[' = a JSON array (multi-arg / typed); anything else = ONE arg
//	    passed directly (a UUID string you already have → no json_encode).
//
//	hazedb_exec(string $sql, string $args): ?string
//	    Run INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE — the write
//	    path (this is the "insert" function, generalised). Same $args rule.
//	    Returns {"affected":N}, an error envelope, or null (no DB).
//
//	hazedb_uuidv7(): string
//	    A fresh UUIDv7 string — convenience for populating UUID primary keys,
//	    since hazedb PKs are UUIDs and the string-only PHP surface needs a way
//	    to mint them. Never null.
//
// cgo lifetime contract (see addons/frankenphp-ext/build/README.md pitfall #8):
//   - args_json is read synchronously (ArgsFromJSON copies values out via
//     json.Decode), so it is taken as a zero-copy view.
//   - sql is deep-copied: db.prepare stores the SQL string as a permanent
//     stmtCache map key, so an alias over PHP-arena memory would dangle after
//     the request ends and index the plan cache by freed bytes.
//   - response bytes are copied into a PHP-owned zend_string by
//     phpStringFromBytes before returning.

package hazedb_ext

/*
#include <php.h>
*/
import "C"

import (
	"unsafe"

	"github.com/VeloxCoding/hazedb"

	// Blank import so a single `xcaddy --with .../frankenphp-ext` also pulls in
	// the Caddy HTTP handler module — one flag yields the full bundle (PHP cgo
	// functions + the /query and /exec HTTP endpoints, sharing one *DB).
	_ "github.com/VeloxCoding/hazedb/caddymodule"
)

// defaultSlot is the atomic *DB slot for "default", resolved once at init.
// Load returns nil until a caddymodule Provision registers a DB; every function
// treats nil as "no Caddy module loaded".
var defaultSlot = hazedb.LookupDBSlot("default")

// zendStringView returns a zero-copy Go string aliasing a zend_string's bytes.
// Valid only for the duration of the calling PHP function — read paths only.
func zendStringView(s *C.zend_string) string {
	if s == nil {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(&s.val)), int(s.len))
}

// zendStringCopy returns a Go-owned copy of a zend_string's bytes. Required when
// the bytes outlive the call — here, the SQL string that becomes a stmtCache key.
func zendStringCopy(s *C.zend_string) string {
	if s == nil {
		return ""
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(&s.val)), C.int(s.len))
}

// phpStringFromBytes emalloc's a PHP zend_string from b. Empty input returns
// nil, which the RETURN_EMPTY_STRING→RETURN_NULL build patch surfaces as null.
func phpStringFromBytes(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(C.zend_string_init(
		(*C.char)(unsafe.Pointer(&b[0])),
		C.size_t(len(b)),
		C._Bool(false),
	))
}

// hazedb_query runs a SELECT and returns the rows envelope as a JSON string.
//
// export_php:function hazedb_query(string $sql, string $args): ?string
func hazedb_query(sql *C.zend_string, argsJSON *C.zend_string) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	args, err := hazedb.QueryArgs(zendStringView(argsJSON))
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	cols, rows, err := db.Query(zendStringCopy(sql), args...)
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	body, err := hazedb.RowsToJSON(cols, rows)
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	return phpStringFromBytes(body)
}

// hazedb_exec runs a write (INSERT/UPDATE/DELETE/CREATE/DROP) and returns
// {"affected":N} as a JSON string.
//
// export_php:function hazedb_exec(string $sql, string $args): ?string
func hazedb_exec(sql *C.zend_string, argsJSON *C.zend_string) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	args, err := hazedb.QueryArgs(zendStringView(argsJSON))
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	n, err := db.Exec(zendStringCopy(sql), args...)
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	return phpStringFromBytes(hazedb.ExecResultJSON(n))
}

// hazedb_uuidv7 returns a fresh UUIDv7 as a 36-char string.
//
// export_php:function hazedb_uuidv7(): string
func hazedb_uuidv7() unsafe.Pointer {
	return phpStringFromBytes([]byte(hazedb.NewUUIDv7().String()))
}
