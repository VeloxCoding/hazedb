// hazedb_ext.go — FrankenPHP extension exposing hazedb's *DB directly to PHP,
// so PHP runs SQL in-process (a Go function call) instead of over a socket or
// HTTP. PHP and the Caddy module share one *DB through the process-wide
// registry in the core (db_registry.go): the caddymodule registers it under
// "default" during Provision; defaultSlot here Loads it per call. A Caddy
// config reload swaps the slot atomically — nothing to invalidate here.
//
// The surface mirrors PDO's vocabulary. $args takes either a native PHP array
// of positional params ([$a, $b], like PDOStatement::execute) OR, for a single
// param, the bare scalar ($id) — the scalar form is the fast path (one zval, no
// array build/iteration). It is optional (default null = no args). No JSON
// crosses the boundary either way; result rows come back as native PHP arrays
// (assoc, keyed by column name) built via zval trampolines.
//
// PHP functions:
//
//	hazedb_fetch(string $sql, mixed $args = null): ?array
//	    One row as a flat assoc array ['col'=>val,...] (≈ PDOStatement::fetch).
//	    null if there is no matching row, no DB, or an error.
//
//	hazedb_fetchall(string $sql, mixed $args = null): ?array
//	    All rows as a list of assoc arrays [['col'=>val,...],...]
//	    (≈ fetchAll(PDO::FETCH_ASSOC)). Empty result = []; null on error / no DB.
//
//	hazedb_fetchall_json(string $sql, mixed $args = null): ?string
//	    Same data as hazedb_fetchall, returned as a JSON string [{...},...] for
//	    pass-through (forward to an HTTP response / cache) with no PHP decode.
//	    null on error / no DB.
//
//	hazedb_exec(string $sql, mixed $args = null): int
//	    INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE. Returns the
//	    affected row count (≈ PDOStatement::rowCount), or -1 on error / no DB.
//
//	hazedb_ping(): string
//	    Liveness probe: "pong" if a Caddy module registered a DB under "default",
//	    "pong (no db)" otherwise. Never null.
//
// cgo lifetime contract (see addons/frankenphp-ext/build/README.md pitfalls):
//   - sql is a zero-copy view; db.prepare clones it on a cache miss (the only
//     time it is retained), so the hot cache-hit path copies nothing.
//   - string args are copied out of the zval (C.GoStringN) before being stored,
//     so nothing aliases PHP-arena memory.
//   - result strings/arrays are PHP-owned (zend_string_init / the zval
//     trampolines copy into the request heap).

package hazedb_ext

/*
#include <php.h>
#include <Zend/zend_API.h>
#include <Zend/zend_hash.h>
#include <Zend/zend_types.h>

// zval trampolines: cgo cannot call the ZVAL_ and zend_hash_ macros directly,
// so wrap them as static-inline C (build/README.md pitfall #5).

// --- build a PHP array result (Go rows -> PHP) ---
static zend_array *hzd_arr_new(uint32_t n) { return zend_new_array(n); }
static void hzd_push_arr(zend_array *a, zend_array *child) {
    zval z; ZVAL_ARR(&z, child); zend_hash_next_index_insert(a, &z);
}
// keyed scalar setters: build a flat assoc row (column name -> value)
static void hzd_set_long(zend_array *a, const char *k, size_t kl, zend_long v) {
    zval z; ZVAL_LONG(&z, v); zend_hash_str_update(a, k, kl, &z);
}
static void hzd_set_bool(zend_array *a, const char *k, size_t kl, int b) {
    zval z; ZVAL_BOOL(&z, b); zend_hash_str_update(a, k, kl, &z);
}
static void hzd_set_null(zend_array *a, const char *k, size_t kl) {
    zval z; ZVAL_NULL(&z); zend_hash_str_update(a, k, kl, &z);
}
static void hzd_set_strn(zend_array *a, const char *k, size_t kl, const char *s, size_t n) {
    zval z;
    if (n == 0) { ZVAL_EMPTY_STRING(&z); } else { ZVAL_STRINGL(&z, s, n); }
    zend_hash_str_update(a, k, kl, &z);
}

// --- read a PHP array of args (PHP -> Go) ---
static uint32_t hzd_arr_count(zend_array *a) { return zend_hash_num_elements(a); }
static zval *hzd_arr_get(zend_array *a, uint32_t i) { return zend_hash_index_find(a, i); }
// hzd_zval_kind normalises the zval type to a small stable code (avoids
// depending on cgo exposing the IS_* macros):
//   0 null  1 false  2 true  3 long  4 string  5 double  6 other  7 array
static int hzd_zval_kind(zval *z) {
    switch (Z_TYPE_P(z)) {
        case IS_NULL:   return 0;
        case IS_FALSE:  return 1;
        case IS_TRUE:   return 2;
        case IS_LONG:   return 3;
        case IS_STRING: return 4;
        case IS_DOUBLE: return 5;
        case IS_ARRAY:  return 7;
        default:        return 6;
    }
}
static zend_long hzd_zval_long(zval *z) { return Z_LVAL_P(z); }
static const char *hzd_zval_strptr(zval *z) { return Z_STRVAL_P(z); }
static size_t hzd_zval_strlen(zval *z) { return Z_STRLEN_P(z); }
static zend_array *hzd_zval_arr(zval *z) { return Z_ARRVAL_P(z); }
*/
import "C"

import (
	"unsafe"

	"github.com/VeloxCoding/hazedb"

	// Blank import so a single `xcaddy --with .../frankenphp-ext` also pulls in
	// the Caddy HTTP handler module — one flag yields the full bundle (PHP cgo
	// functions + the HTTP endpoints, sharing one *DB).
	_ "github.com/VeloxCoding/hazedb/caddymodule"
)

// defaultSlot is the atomic *DB slot for "default", resolved once at init.
// Load returns nil until a caddymodule Provision registers a DB; every function
// treats nil as "no Caddy module loaded".
var defaultSlot = hazedb.LookupDBSlot("default")

// zendStringView returns a zero-copy Go string aliasing a zend_string's bytes.
// Valid only for the duration of the calling PHP function. Safe for the SQL
// string because db.prepare clones it on a cache miss before retaining it.
func zendStringView(s *C.zend_string) string {
	if s == nil {
		return ""
	}
	return unsafe.String((*byte)(unsafe.Pointer(&s.val)), int(s.len))
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

// valueFromZval converts one scalar zval to a Value. Type mapping: int -> INT,
// true/false -> BOOL, null -> NULL, string -> STRING unless it parses as a
// canonical UUID -> UUID. Strings are copied (GoStringN) so storage never
// aliases PHP memory. ok=false on a non-scalar / unsupported type (float,
// nested array).
func valueFromZval(z *C.zval) (hazedb.Value, bool) {
	switch C.hzd_zval_kind(z) {
	case 0:
		return hazedb.Null(), true
	case 1:
		return hazedb.Bool(false), true
	case 2:
		return hazedb.Bool(true), true
	case 3:
		return hazedb.Int(int64(C.hzd_zval_long(z))), true
	case 4:
		s := C.GoStringN(C.hzd_zval_strptr(z), C.int(C.hzd_zval_strlen(z)))
		if u, err := hazedb.ParseUUID(s); err == nil {
			return hazedb.UUIDVal(u), true
		}
		return hazedb.Str(s), true
	default: // double, array, or other — not a positional scalar
		return hazedb.Value{}, false
	}
}

// argsFromMixed reads the $args param, which may be a native PHP array of
// positional params ([$a, $b]) OR a single bare scalar ($id) — the latter is
// the fast path for single-key reads (one zval, no array build/iteration). A
// nil (omitted) or null $args yields no args.
func argsFromMixed(a *C.zval) ([]hazedb.Value, bool) {
	if a == nil {
		return nil, true
	}
	switch C.hzd_zval_kind(a) {
	case 0: // null / omitted
		return nil, true
	case 7: // array of positional params
		arr := C.hzd_zval_arr(a)
		n := int(C.hzd_arr_count(arr))
		if n == 0 {
			return nil, true
		}
		vals := make([]hazedb.Value, 0, n)
		for i := 0; i < n; i++ {
			z := C.hzd_arr_get(arr, C.uint32_t(i))
			if z == nil {
				vals = append(vals, hazedb.Null())
				continue
			}
			v, ok := valueFromZval(z)
			if !ok {
				return nil, false
			}
			vals = append(vals, v)
		}
		return vals, true
	default: // a bare scalar -> exactly one positional arg (fast path)
		v, ok := valueFromZval(a)
		if !ok {
			return nil, false
		}
		return []hazedb.Value{v}, true
	}
}

// setStr stores a Go string under a key (handles the empty case so
// unsafe.StringData(nil) is never deref'd).
func setStr(a *C.zend_array, kp *C.char, kl C.size_t, s string) {
	var p *C.char
	if len(s) > 0 {
		p = (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
	}
	C.hzd_set_strn(a, kp, kl, p, C.size_t(len(s)))
}

// setCell stores one cell under its column-name key (the assoc-row shape).
// UUID -> canonical string, Bytes -> raw byte string; ints/bools stay native.
func setCell(a *C.zend_array, key string, v hazedb.Value) {
	kp := (*C.char)(unsafe.Pointer(unsafe.StringData(key)))
	kl := C.size_t(len(key))
	switch v.Kind {
	case hazedb.KindNull:
		C.hzd_set_null(a, kp, kl)
	case hazedb.KindInt:
		C.hzd_set_long(a, kp, kl, C.zend_long(v.Int()))
	case hazedb.KindBool:
		b := C.int(0)
		if v.Bool() {
			b = 1
		}
		C.hzd_set_bool(a, kp, kl, b)
	case hazedb.KindString:
		setStr(a, kp, kl, v.Str())
	case hazedb.KindUUID:
		setStr(a, kp, kl, v.UUID().String())
	case hazedb.KindBytes:
		bs := v.Bytes()
		var p *C.char
		if len(bs) > 0 {
			p = (*C.char)(unsafe.Pointer(&bs[0]))
		}
		C.hzd_set_strn(a, kp, kl, p, C.size_t(len(bs)))
	default:
		C.hzd_set_null(a, kp, kl)
	}
}

// rowToAssoc builds a single assoc PHP array from cols + one row.
func rowToAssoc(cols []string, row hazedb.Row) *C.zend_array {
	a := C.hzd_arr_new(C.uint32_t(len(cols)))
	for i, c := range cols {
		setCell(a, c, row[i])
	}
	return a
}

// hazedb_fetch returns one row as a flat assoc PHP array, or null (no row / no
// DB / error). ≈ PDOStatement::fetch(PDO::FETCH_ASSOC).
//
// export_php:function hazedb_fetch(string $sql, mixed $args = null): ?array
func hazedb_fetch(sql *C.zend_string, args *C.zval) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return nil
	}
	cols, row, err := db.QueryRowValues(zendStringView(sql), vals...)
	if err != nil || row == nil {
		return nil
	}
	return unsafe.Pointer(rowToAssoc(cols, row))
}

// hazedb_fetchall returns all rows as a list of assoc PHP arrays. Empty result
// is an empty array (not null); null only on no DB / error.
// ≈ PDOStatement::fetchAll(PDO::FETCH_ASSOC).
//
// export_php:function hazedb_fetchall(string $sql, mixed $args = null): ?array
func hazedb_fetchall(sql *C.zend_string, args *C.zval) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return nil
	}
	cols, rows, err := db.QueryValues(zendStringView(sql), vals...)
	if err != nil {
		return nil
	}
	out := C.hzd_arr_new(C.uint32_t(len(rows)))
	for i := range rows {
		C.hzd_push_arr(out, rowToAssoc(cols, rows[i]))
	}
	return unsafe.Pointer(out)
}

// hazedb_fetchall_json returns the same data as hazedb_fetchall as a JSON string
// [{...},...] — for forwarding to an HTTP/JSON response without a PHP decode.
//
// export_php:function hazedb_fetchall_json(string $sql, mixed $args = null): ?string
func hazedb_fetchall_json(sql *C.zend_string, args *C.zval) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return nil
	}
	cols, rows, err := db.QueryValues(zendStringView(sql), vals...)
	if err != nil {
		return nil
	}
	body, _ := hazedb.RowsToJSONObjects(cols, rows)
	return phpStringFromBytes(body)
}

// hazedb_exec runs a write and returns the affected row count, or -1 on error /
// no DB. ≈ PDOStatement::execute(...) + rowCount().
//
// export_php:function hazedb_exec(string $sql, mixed $args = null): int
func hazedb_exec(sql *C.zend_string, args *C.zval) int64 {
	db := defaultSlot.Load()
	if db == nil {
		return -1
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return -1
	}
	affected, err := db.ExecValues(zendStringView(sql), vals...)
	if err != nil {
		return -1
	}
	return int64(affected)
}

// hazedb_ping reports that the extension is loaded and whether a DB is wired up.
//
// export_php:function hazedb_ping(): string
func hazedb_ping() unsafe.Pointer {
	if defaultSlot.Load() == nil {
		return phpStringFromBytes([]byte("pong (no db)"))
	}
	return phpStringFromBytes([]byte("pong"))
}
