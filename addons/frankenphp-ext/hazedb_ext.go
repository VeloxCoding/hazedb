// hazedb_ext.go — FrankenPHP extension exposing hazedb's *DB directly to PHP,
// so PHP runs SQL in-process (a Go function call) instead of over a socket or
// HTTP. PHP and the Caddy module share one *DB through the process-wide
// registry in the core (db_registry.go): the caddymodule registers it under
// "default" during Provision; defaultSlot here Loads it per call. A Caddy
// config reload swaps the slot atomically — nothing to invalidate here.
//
// PHP functions:
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
//	hazedb_query_arr(string $sql, string $args): ?array
//	    Like hazedb_query but returns a native PHP array (['columns'=>[...],
//	    'rows'=>[[...]]]) built directly via zval trampolines — no JSON encode,
//	    no PHP json_decode. null on no DB / query error.
//
//	hazedb_exec_arr(string $sql, array $args): ?string
//	    Like hazedb_exec but takes a native PHP array of positional args instead
//	    of a JSON string — no json_encode / json.Decode. Read straight into typed
//	    Values and applied via db.ExecValues.
//
//	hazedb_ping(): string
//	    Liveness probe for the extension itself: "pong" if a Caddy module has
//	    registered a DB under "default", "pong (no db)" otherwise. Takes no
//	    args, never null — the minimal end-to-end check that the cgo bridge and
//	    the shared-DB slot are wired up.
//
// cgo lifetime contract (see addons/frankenphp-ext/build/README.md pitfall #8):
//   - Both sql and args_json are passed as zero-copy views: each function reads
//     them synchronously while the PHP-arena memory is still valid. db.prepare
//     clones the SQL itself on a cache miss (the only time it is retained), so
//     the hot cache-hit path copies nothing — see db.prepare's contract.
//   - response bytes are copied into a PHP-owned zend_string by
//     phpStringFromBytes before returning.

package hazedb_ext

/*
#include <php.h>
#include <Zend/zend_API.h>
#include <Zend/zend_hash.h>
#include <Zend/zend_types.h>

// --- zval trampolines: cgo cannot call the ZVAL_ and zend_hash_ macros
// directly, so wrap them as static-inline C (build/README.md pitfall #5). These
// build a PHP array result straight from Go rows, so PHP gets a native array
// with no JSON encode (Go) and no json_decode (PHP).

static zend_array *hzd_arr_new(uint32_t n) { return zend_new_array(n); }

static void hzd_push_long(zend_array *a, zend_long v) {
    zval z; ZVAL_LONG(&z, v); zend_hash_next_index_insert(a, &z);
}
static void hzd_push_bool(zend_array *a, int b) {
    zval z; ZVAL_BOOL(&z, b); zend_hash_next_index_insert(a, &z);
}
static void hzd_push_null(zend_array *a) {
    zval z; ZVAL_NULL(&z); zend_hash_next_index_insert(a, &z);
}
static void hzd_push_strn(zend_array *a, const char *s, size_t n) {
    zval z;
    if (n == 0) { ZVAL_EMPTY_STRING(&z); } else { ZVAL_STRINGL(&z, s, n); }
    zend_hash_next_index_insert(a, &z);
}
static void hzd_push_arr(zend_array *a, zend_array *child) {
    zval z; ZVAL_ARR(&z, child); zend_hash_next_index_insert(a, &z);
}
static void hzd_set_arr(zend_array *a, const char *key, size_t klen, zend_array *child) {
    zval z; ZVAL_ARR(&z, child); zend_hash_str_update(a, key, klen, &z);
}

// --- array readers (PHP zend_array -> Go), for the args-in direction. ---
static uint32_t hzd_arr_count(zend_array *a) { return zend_hash_num_elements(a); }
static zval *hzd_arr_get(zend_array *a, uint32_t i) { return zend_hash_index_find(a, i); }

// hzd_zval_kind normalises the zval type to a small stable code we switch on in
// Go (avoids depending on cgo exposing the IS_* macros):
//   0 null  1 false  2 true  3 long  4 string  5 double  6 other
static int hzd_zval_kind(zval *z) {
    switch (Z_TYPE_P(z)) {
        case IS_NULL:   return 0;
        case IS_FALSE:  return 1;
        case IS_TRUE:   return 2;
        case IS_LONG:   return 3;
        case IS_STRING: return 4;
        case IS_DOUBLE: return 5;
        default:        return 6;
    }
}
static zend_long hzd_zval_long(zval *z) { return Z_LVAL_P(z); }
static const char *hzd_zval_strptr(zval *z) { return Z_STRVAL_P(z); }
static size_t hzd_zval_strlen(zval *z) { return Z_STRLEN_P(z); }
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
// The SQL string is safe to pass as a view because db.prepare clones it on a
// cache miss before retaining it (the cache-hit path never retains it).
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
	cols, rows, err := db.Query(zendStringView(sql), args...)
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
	n, err := db.Exec(zendStringView(sql), args...)
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	return phpStringFromBytes(hazedb.ExecResultJSON(n))
}

// pushStr appends a Go string to a PHP array as a copied zend_string (the
// trampoline handles the empty case so unsafe.StringData(nil) is never deref'd).
func pushStr(a *C.zend_array, s string) {
	var p *C.char
	if len(s) > 0 {
		p = (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
	}
	C.hzd_push_strn(a, p, C.size_t(len(s)))
}

// setArr stores child under a string key in a (associative entry).
func setArr(a *C.zend_array, key string, child *C.zend_array) {
	C.hzd_set_arr(a, (*C.char)(unsafe.Pointer(unsafe.StringData(key))), C.size_t(len(key)), child)
}

// pushCell appends one result cell to a PHP row array, mapping the Value kind to
// the native PHP scalar (UUID -> canonical string, Bytes -> raw byte string).
func pushCell(a *C.zend_array, v hazedb.Value) {
	switch v.Kind {
	case hazedb.KindNull:
		C.hzd_push_null(a)
	case hazedb.KindInt:
		C.hzd_push_long(a, C.zend_long(v.Int()))
	case hazedb.KindBool:
		b := C.int(0)
		if v.Bool() {
			b = 1
		}
		C.hzd_push_bool(a, b)
	case hazedb.KindString:
		pushStr(a, v.Str())
	case hazedb.KindUUID:
		pushStr(a, v.UUID().String())
	case hazedb.KindBytes:
		bs := v.Bytes()
		var p *C.char
		if len(bs) > 0 {
			p = (*C.char)(unsafe.Pointer(&bs[0]))
		}
		C.hzd_push_strn(a, p, C.size_t(len(bs)))
	default:
		C.hzd_push_null(a)
	}
}

// hazedb_query_arr is hazedb_query that returns a native PHP array instead of a
// JSON string: ['columns'=>[...], 'rows'=>[[...],...]]. It skips both the Go
// JSON encode and the PHP json_decode the string form pays. Returns null on no
// DB or query error (the array form carries no error envelope); an empty result
// is a valid array with an empty 'rows'. $args is the same string form as
// hazedb_query (direct UUID or JSON array).
//
// export_php:function hazedb_query_arr(string $sql, string $args): ?array
func hazedb_query_arr(sql *C.zend_string, argsStr *C.zend_string) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	args, err := hazedb.QueryArgs(zendStringView(argsStr))
	if err != nil {
		return nil
	}
	cols, rows, err := db.Query(zendStringView(sql), args...)
	if err != nil {
		return nil
	}
	env := C.hzd_arr_new(2)
	colsArr := C.hzd_arr_new(C.uint32_t(len(cols)))
	for _, c := range cols {
		pushStr(colsArr, c)
	}
	setArr(env, "columns", colsArr)
	rowsArr := C.hzd_arr_new(C.uint32_t(len(rows)))
	for i := range rows {
		ra := C.hzd_arr_new(C.uint32_t(len(rows[i])))
		for _, cell := range rows[i] {
			pushCell(ra, cell)
		}
		C.hzd_push_arr(rowsArr, ra)
	}
	setArr(env, "rows", rowsArr)
	return unsafe.Pointer(env)
}

// hazedb_exec_arr is hazedb_exec that takes a native PHP array of positional
// args instead of a JSON string, skipping the json_encode (PHP) and json.Decode
// (Go) round-trip. Args are read straight from the zend_array into typed Values
// and applied via db.ExecValues. Type mapping: PHP int -> INT, true/false ->
// BOOL, null -> NULL, string -> STRING unless it parses as a canonical UUID ->
// UUID (same rule as the JSON form). A float arg is rejected (hazedb has no
// float type). Returns {"affected":N}, an error envelope, or null (no DB).
//
// export_php:function hazedb_exec_arr(string $sql, array $args): ?string
func hazedb_exec_arr(sql *C.zend_string, args *C.zend_array) unsafe.Pointer {
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	n := int(C.hzd_arr_count(args))
	var buf [8]hazedb.Value
	var vals []hazedb.Value
	if n <= len(buf) {
		vals = buf[:0]
	} else {
		vals = make([]hazedb.Value, 0, n)
	}
	for i := 0; i < n; i++ {
		z := C.hzd_arr_get(args, C.uint32_t(i))
		if z == nil {
			vals = append(vals, hazedb.Null())
			continue
		}
		switch C.hzd_zval_kind(z) {
		case 0: // null
			vals = append(vals, hazedb.Null())
		case 1: // false
			vals = append(vals, hazedb.Bool(false))
		case 2: // true
			vals = append(vals, hazedb.Bool(true))
		case 3: // long
			vals = append(vals, hazedb.Int(int64(C.hzd_zval_long(z))))
		case 4: // string — owned copy (stored), UUID if canonical
			s := C.GoStringN(C.hzd_zval_strptr(z), C.int(C.hzd_zval_strlen(z)))
			if u, err := hazedb.ParseUUID(s); err == nil {
				vals = append(vals, hazedb.UUIDVal(u))
			} else {
				vals = append(vals, hazedb.Str(s))
			}
		default: // double or other — unsupported
			return phpStringFromBytes(hazedb.ErrorJSON("hazedb_exec_arr: unsupported arg type (floats not supported)"))
		}
	}
	affected, err := db.ExecValues(zendStringView(sql), vals...)
	if err != nil {
		return phpStringFromBytes(hazedb.ErrorJSON(err.Error()))
	}
	return phpStringFromBytes(hazedb.ExecResultJSON(affected))
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
