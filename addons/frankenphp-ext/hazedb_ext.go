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
//	hazedb_meta(): ?string
//	    Store-size overview as a JSON string — the same bytes the Caddy GET
//	    /meta route returns ({"tables":N,"max_bytes":M,"total_rows":R,
//	    "total_approx_bytes":B,"total_tombstones":T,"table_stats":[...]}). For
//	    dashboards / health checks; sizes are estimates. null only when no DB.
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
// hzd_build_row builds one flat assoc row (column name -> value) in a SINGLE
// cgo crossing, replacing one crossing per cell. To stay within cgo's pointer
// rules everything arrives as pointer-free buffers: keys packed in keybuf with
// (n+1) offsets koff; per-cell kind selects the value source —
//   0 null  1 false  2 true  3 long(lvals[i])  4 string(valbuf[voff[i]:voff[i+1]])
// lvals is indexed per cell (entry unused for non-long kinds); voff is
// monotonic, so non-string cells contribute a zero-length span.
static zend_array *hzd_build_row(uint32_t n,
        const char *keybuf, const int32_t *koff,
        const uint8_t *kinds, const int64_t *lvals,
        const char *valbuf, const int32_t *voff) {
    zend_array *a = zend_new_array(n);
    for (uint32_t i = 0; i < n; i++) {
        zval z;
        switch (kinds[i]) {
            case 1:  ZVAL_FALSE(&z); break;
            case 2:  ZVAL_TRUE(&z); break;
            case 3:  ZVAL_LONG(&z, lvals[i]); break;
            case 4: {
                size_t sl = (size_t)(voff[i+1] - voff[i]);
                if (sl == 0) { ZVAL_EMPTY_STRING(&z); }
                else { ZVAL_STRINGL(&z, valbuf + voff[i], sl); }
                break;
            }
            default: ZVAL_NULL(&z); break;
        }
        zend_hash_str_update(a, keybuf + koff[i], (size_t)(koff[i+1] - koff[i]), &z);
    }
    return a;
}

// --- read a PHP array of args (PHP -> Go) ---
static uint32_t hzd_arr_count(zend_array *a) { return zend_hash_num_elements(a); }
static zval *hzd_arr_get(zend_array *a, uint32_t i) { return zend_hash_index_find(a, i); }
// hzd_arr_is_list reports whether a is a real positional list — sequential
// integer keys 0..n-1, array_is_list() semantics. Positional params require it;
// a sparse or associative array is rejected rather than read by index, which
// would map every missing index to NULL and silently drop the caller's values.
static int hzd_arr_is_list(zend_array *a) { return zend_array_is_list(a) ? 1 : 0; }
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
	"fmt"
	"os"
	"sync"
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
// true/false -> BOOL, null -> NULL, string -> STRING. A PHP string stays a STRING
// regardless of shape; the type is not guessed here. A string destined for a UUID
// column is parsed into a UUID downstream, driven by the column type the planner
// knows (the core's coerceParams), so a UUID column addressed by a PHP string works
// and a TEXT column holding a canonical-UUID-form value stays text. Strings are
// copied (GoStringN) so storage never aliases PHP memory. ok=false on a non-scalar /
// unsupported type (float, nested array).
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
		return hazedb.Str(C.GoStringN(C.hzd_zval_strptr(z), C.int(C.hzd_zval_strlen(z)))), true
	default: // double, array, or other — not a positional scalar
		return hazedb.Value{}, false
	}
}

// argsFromMixed reads the $args param, which may be a native PHP array of
// positional params ([$a, $b]) OR a single bare scalar ($id) — the latter is
// the fast path for single-key reads (one zval, no array build/iteration). A
// nil (omitted) or null $args yields no args. The array must be a true list
// (keys 0..n-1); a sparse or associative array is rejected (ok=false), never
// read positionally.
func argsFromMixed(a *C.zval) ([]hazedb.Value, bool) {
	if a == nil {
		return nil, true
	}
	switch C.hzd_zval_kind(a) {
	case 0: // null / omitted
		return nil, true
	case 7: // array of positional params
		arr := C.hzd_zval_arr(a)
		// Positional params must be a real list (keys 0..n-1). Reject a sparse or
		// associative array — reading it by index would turn every missing index
		// into NULL and silently drop the caller's values (e.g. [1=>$id] reads
		// index 0 as NULL; ['id'=>$id] has no index 0 at all).
		if C.hzd_arr_is_list(arr) == 0 {
			return nil, false
		}
		n := int(C.hzd_arr_count(arr))
		if n == 0 {
			return nil, true
		}
		vals := make([]hazedb.Value, 0, n)
		for i := 0; i < n; i++ {
			z := C.hzd_arr_get(arr, C.uint32_t(i))
			if z == nil {
				return nil, false // list verified above; a hole is unexpected — reject, never guess NULL
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

// Pointer helpers: hand a pointer-free Go slice's backing to C, or nil when
// empty (so &s[0] is never taken on a zero-length slice). The pointed-to memory
// holds no Go pointers, so it satisfies cgo's pointer-passing rule, and
// hzd_build_row copies everything out before returning — nothing is retained.
func charPtr(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}
func i32Ptr(s []int32) *C.int32_t {
	if len(s) == 0 {
		return nil
	}
	return (*C.int32_t)(unsafe.Pointer(&s[0]))
}
func u8Ptr(s []uint8) *C.uint8_t {
	if len(s) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&s[0]))
}
func i64Ptr(s []int64) *C.int64_t {
	if len(s) == 0 {
		return nil
	}
	return (*C.int64_t)(unsafe.Pointer(&s[0]))
}

// rowScratch holds every reusable buffer for the batched row build — the packed
// keys (stable per query) plus the per-row value buffers. Pooled (scratchPool)
// so a call reuses backing capacity instead of allocating ~5 slices each time;
// that per-call alloc cost otherwise sinks the single-row fetch path, where the
// saved cgo crossings don't amortize. hzd_build_row copies everything into the
// Zend heap before returning, so the scratch is free to reuse/return at once.
type rowScratch struct {
	keybuf []byte
	koff   []int32
	kinds  []uint8
	lvals  []int64
	valbuf []byte
	voff   []int32
}

var scratchPool = sync.Pool{New: func() any { return new(rowScratch) }}

// Pool buffers are reused across calls on a long-lived worker, so a single outlier
// query (a huge BLOB/string cell, or an enormous result) would otherwise pin its
// grown backing in the pool indefinitely. putScratch / putJSONBuf drop a backing
// that grew past these caps before returning it — trading a rare re-grow for
// bounded steady-state pool memory. A normal row/result stays well under the caps
// and is pooled intact.
const (
	maxPooledValbuf  = 1 << 20  // 1 MiB — one row's packed value bytes
	maxPooledKeybuf  = 64 << 10 // 64 KiB — packed column names (large only for absurd schemas)
	maxPooledJSONBuf = 1 << 20  // 1 MiB — one fetchall_json body
)

// putScratch returns sc to the pool, dropping the value/key backings if an outlier
// row grew them past the caps so they do not linger on this worker.
func putScratch(sc *rowScratch) {
	if cap(sc.valbuf) > maxPooledValbuf {
		sc.valbuf = nil
	}
	if cap(sc.keybuf) > maxPooledKeybuf {
		sc.keybuf = nil
	}
	scratchPool.Put(sc)
}

// jsonBufPool holds reusable JSON output buffers for hazedb_fetchall_json, so a
// worker reuses one backing array instead of allocating (and growing from a tiny
// seed) per call. *[]byte, not []byte, so Put stores the grown slice header
// without boxing it into a fresh allocation.
var jsonBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

// putJSONBuf returns bp to the pool, dropping the body backing if an outlier result
// grew it past the cap (the New seed re-grows from nil on the next big call).
func putJSONBuf(bp *[]byte) {
	if cap(*bp) > maxPooledJSONBuf {
		*bp = nil
	}
	jsonBufPool.Put(bp)
}

// fillKeys packs the column names into keybuf + (n+1) offsets, once per query
// since the keys are identical for every row.
func (sc *rowScratch) fillKeys(cols []string) {
	sc.keybuf = sc.keybuf[:0]
	sc.koff = append(sc.koff[:0], 0)
	for _, c := range cols {
		sc.keybuf = append(sc.keybuf, c...)
		sc.koff = append(sc.koff, int32(len(sc.keybuf)))
	}
}

// rowToAssoc builds one assoc PHP array from the packed keys + one row in a
// single cgo crossing. The value buffers are refilled on every call. UUID ->
// canonical string, Bytes -> raw byte string; ints/bools stay native.
func (sc *rowScratch) rowToAssoc(row hazedb.Row) *C.zend_array {
	n := len(sc.koff) - 1
	sc.kinds = sc.kinds[:0]
	sc.lvals = sc.lvals[:0]
	sc.valbuf = sc.valbuf[:0]
	sc.voff = append(sc.voff[:0], 0)
	for i := 0; i < n; i++ {
		v := row[i]
		switch v.Kind {
		case hazedb.KindBool:
			k := uint8(1)
			if v.Bool() {
				k = 2
			}
			sc.kinds = append(sc.kinds, k)
			sc.lvals = append(sc.lvals, 0)
		case hazedb.KindInt:
			sc.kinds = append(sc.kinds, 3)
			sc.lvals = append(sc.lvals, v.Int())
		case hazedb.KindString:
			sc.kinds = append(sc.kinds, 4)
			sc.lvals = append(sc.lvals, 0)
			sc.valbuf = append(sc.valbuf, v.Str()...)
		case hazedb.KindUUID:
			sc.kinds = append(sc.kinds, 4)
			sc.lvals = append(sc.lvals, 0)
			sc.valbuf = v.UUID().AppendString(sc.valbuf) // 0-alloc: no temp String
		case hazedb.KindBytes:
			sc.kinds = append(sc.kinds, 4)
			sc.lvals = append(sc.lvals, 0)
			sc.valbuf = append(sc.valbuf, v.Bytes()...)
		default: // KindNull or unknown
			sc.kinds = append(sc.kinds, 0)
			sc.lvals = append(sc.lvals, 0)
		}
		sc.voff = append(sc.voff, int32(len(sc.valbuf)))
	}
	return C.hzd_build_row(C.uint32_t(n),
		charPtr(sc.keybuf), i32Ptr(sc.koff),
		u8Ptr(sc.kinds), i64Ptr(sc.lvals),
		charPtr(sc.valbuf), i32Ptr(sc.voff))
}

// hazedb_fetch returns one row as a flat assoc PHP array, or null (no row / no
// DB / error). ≈ PDOStatement::fetch(PDO::FETCH_ASSOC).
//
// cgoRecoverPtr / cgoRecoverInt translate a recovered panic into the entry
// point's error sentinel (nil / -1) instead of letting it unwind across the cgo
// boundary back into C/PHP, which aborts the whole FrankenPHP process. The panic
// is logged to stderr so the underlying bug still surfaces. Unlike the Caddy
// HTTP path (net/http recovers each request), this cgo path has no other net.
func cgoRecoverPtr(fn string, ret *unsafe.Pointer) {
	if r := recover(); r != nil {
		fmt.Fprintf(os.Stderr, "hazedb: recovered panic in %s: %v\n", fn, r)
		*ret = nil
	}
}

func cgoRecoverInt(fn string, ret *int64) {
	if r := recover(); r != nil {
		fmt.Fprintf(os.Stderr, "hazedb: recovered panic in %s: %v\n", fn, r)
		*ret = -1
	}
}

// export_php:function hazedb_fetch(string $sql, mixed $args = null): ?array
func hazedb_fetch(sql *C.zend_string, args *C.zval) (ret unsafe.Pointer) {
	defer cgoRecoverPtr("hazedb_fetch", &ret)
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
	sc := scratchPool.Get().(*rowScratch)
	sc.fillKeys(cols)
	res := unsafe.Pointer(sc.rowToAssoc(row))
	putScratch(sc)
	return res
}

// hazedb_fetchall returns all rows as a list of assoc PHP arrays. Empty result
// is an empty array (not null); null only on no DB / error.
// ≈ PDOStatement::fetchAll(PDO::FETCH_ASSOC).
//
// export_php:function hazedb_fetchall(string $sql, mixed $args = null): ?array
func hazedb_fetchall(sql *C.zend_string, args *C.zval) (ret unsafe.Pointer) {
	defer cgoRecoverPtr("hazedb_fetchall", &ret)
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return nil
	}
	// Stream: build each PHP assoc array straight from the live row, skipping the
	// Go []Row + per-row clone QueryValues would materialize only to re-encode.
	// QueryEach runs the visitor under the row's shard lock and the row is valid
	// only for that call; rowToAssoc copies every cell into the Zend heap before
	// returning, so nothing is retained. The array is allocated on the first row
	// (and for an empty result below), so an error — which QueryEach only returns
	// before any row is visited — leaks nothing.
	sc := scratchPool.Get().(*rowScratch)
	var out *C.zend_array
	err := db.QueryEach(zendStringView(sql), vals, func(cols []string, row hazedb.Row) bool {
		if out == nil {
			sc.fillKeys(cols)
			out = C.hzd_arr_new(C.uint32_t(0))
		}
		C.hzd_push_arr(out, sc.rowToAssoc(row))
		return true
	})
	putScratch(sc)
	if err != nil {
		return nil
	}
	if out == nil {
		out = C.hzd_arr_new(C.uint32_t(0)) // empty result -> empty array (not null)
	}
	return unsafe.Pointer(out)
}

// hazedb_fetchall_json returns the same data as hazedb_fetchall as a JSON string
// [{...},...] — for forwarding to an HTTP/JSON response without a PHP decode.
//
// export_php:function hazedb_fetchall_json(string $sql, mixed $args = null): ?string
func hazedb_fetchall_json(sql *C.zend_string, args *C.zval) (ret unsafe.Pointer) {
	defer cgoRecoverPtr("hazedb_fetchall_json", &ret)
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	vals, ok := argsFromMixed(args)
	if !ok {
		return nil
	}
	// Stream straight to JSON bytes into a POOLED per-worker buffer: QueryJSONInto
	// encodes the live rows into one buffer (no []Row clone), and reusing the
	// buffer across calls drops the per-call envelope allocation — for a result
	// of any size the 256 B->payload doubling otherwise throws away ~4x the final
	// size every call. phpStringFromBytes copies the bytes into the Zend heap
	// before we return the buffer to the pool, so nothing aliases reused storage.
	bp := jsonBufPool.Get().(*[]byte)
	_, body, err := db.QueryJSONInto((*bp)[:0], zendStringView(sql), vals...)
	if err != nil {
		putJSONBuf(bp)
		return nil
	}
	*bp = body // keep the grown backing for the next call on this worker
	res := phpStringFromBytes(body)
	putJSONBuf(bp)
	return res
}

// hazedb_exec runs a write and returns the affected row count, or -1 on error /
// no DB. ≈ PDOStatement::execute(...) + rowCount().
//
// export_php:function hazedb_exec(string $sql, mixed $args = null): int
func hazedb_exec(sql *C.zend_string, args *C.zval) (ret int64) {
	defer cgoRecoverInt("hazedb_exec", &ret)
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

// hazedb_meta returns the store-size overview (table count + per-table rows /
// columns / index count / approximate bytes) as a JSON string — the exact bytes
// the Caddy GET /meta route emits, since both call db.MetaJSON. No args. The
// caller json_decode's it. null only when no Caddy module has registered a DB.
//
// export_php:function hazedb_meta(): ?string
func hazedb_meta() (ret unsafe.Pointer) {
	defer cgoRecoverPtr("hazedb_meta", &ret)
	db := defaultSlot.Load()
	if db == nil {
		return nil
	}
	return phpStringFromBytes(db.MetaJSON())
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
