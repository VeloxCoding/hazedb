# PHP ↔ hazedb array bridge

**Status: implemented** (FrankenPHP extension under [`addons/frankenphp-ext`](../addons/frankenphp-ext)).

This note records why and how PHP exchanges data with hazedb as **native PHP
arrays** instead of JSON strings, the design alternatives weighed, the measured
results, and exactly which files changed. It documents shipped work — it is not
an open proposal.

---

## 1. Summary

The PHP extension originally passed all multi-value arguments and all results as
**JSON strings** (`json_encode` in PHP → `json.Decode` in Go, and back). That
round-trip dominated the per-call cost: ~1.1 µs of Go-side JSON decode per
insert and a ~447 ns `json_decode` tax per read — more than the entire rest of
the call.

The fix is a small **zval trampoline layer** (static-inline C wrapping the Zend
macros cgo cannot call) plus a typed Go write path, exposing three functions
that skip JSON entirely:

- `hazedb_get($sql, $id): ?array` — point read → one flat assoc array.
- `hazedb_query_arr($sql, $args): ?array` — multi-row read → `{columns, rows}`.
- `hazedb_exec_arr($sql, array $args): ?string` — write with a native array of args.

Net effect (in-memory, in-process from PHP): point reads **~1.16M → ~2.85M
qps**, inserts **~445K → ~875K qps**. Packaging is unchanged — the trampolines
compile into the same single FrankenPHP binary.

---

## 2. The problem

The PHP surface is string-only by nature (the first functions were
string-in/string-out, which needs no C glue). To pass *several* typed values
through one string parameter, args were serialised as a JSON array; results came
back as a JSON envelope the caller had to `json_decode`. Measured costs:

| step | cost | where |
|---|---|---|
| Args JSON decode (Go), 3-element array | ~1100 ns / ~20 allocs | `QueryArgs` → `json.Decode` |
| `json_decode` of a result (PHP) | ~447 ns (≈55% of the decoded call) | PHP runtime |

So for an **insert**, half the per-call cost was the JSON args round-trip; for a
**read returning usable data**, more than half was PHP's `json_decode`. Single-key
reads already avoided JSON on the args side (the direct-UUID form), but the
result still came back as a string to decode.

---

## 3. Design space considered

| option | verdict |
|---|---|
| **Keep JSON** | Simple, dependency-free, but pays the full round-trip both ways. |
| **Hand-rolled flat-array args parser** (still a JSON string, faster Go decode) | Removes ~half the args tax, but PHP still `json_encode`s. Partial. |
| **goccy/go-json** for encode/decode | Measured ~1.3× over stdlib for this shape — *slower* than the hand-rolled JSON encoder already in `wire.go`, and adds a core dependency. Rejected. |
| **MessagePack** | No Go-side win over hand-rolled JSON; adds a PHP extension dependency; payload size is irrelevant in-process. Rejected. |
| **Native PHP arrays via zval trampolines** | Removes JSON on both sides entirely. Needs static-inline C glue (cgo can't call the Zend array macros), but stays in one binary. **Chosen.** |

The native-array path wins because it deletes the serialisation step rather than
speeding it up. The decision was gated on a pure-Go measurement first (see §6):
the typed write path proved ~4.2× faster than the JSON-args path before any C
glue was written.

---

## 4. The solution

Two layers.

**Core (Go), in [`db.go`](../db.go):** a typed write entry point that bypasses
the `[]any`/JSON conversion `Exec` uses.

- [`ExecValues(sql string, args ...Value)`](../db.go) — prepare + dispatch with
  pre-typed `Value`s, no JSON, no interface boxing, no per-arg type switch.
- `execPlanValues` clones each arg with `cloneValue` (a no-op except for
  `KindBytes`, which must not alias caller memory across the write boundary —
  the same guarantee `toValue` gives the `[]any` path).
- Reads reuse the existing [`QueryRow`](../db.go) / `Query` — no new core read
  function was needed; they already return typed rows.

**Extension (cgo), in
[`hazedb_ext.go`](../addons/frankenphp-ext/hazedb_ext.go):** a zval trampoline
layer plus the three functions.

- **Build trampolines** (Go rows → PHP array): `hzd_arr_new`, `hzd_push_*`
  (numeric-indexed), `hzd_set_arr` / `hzd_set_*` (keyed). Used to construct the
  result arrays.
- **Read trampolines** (PHP array → Go): `hzd_arr_count`, `hzd_arr_get`,
  `hzd_zval_kind` (normalises the zval type to a small stable code), and the
  value accessors. Used to read native args.

### API shape — which function when

Reads come in **two array variants, by row count**:

- **single row → `hazedb_get`** — returns one flat associative array
  (`['name'=>…, 'age'=>…]`), or `null` if the row is absent. The point-read fast
  path (`WHERE id = ?`).
- **multiple rows → `hazedb_query_arr`** — returns the `{columns, rows}`
  envelope (`['columns'=>[…], 'rows'=>[[…],…]]`). Use for lists / scans.

(`hazedb_exec_arr` is the write variant; `hazedb_query` stays for raw
JSON-string pass-through.) Full reference:

| function | use for | shape returned |
|---|---|---|
| `hazedb_get($sql, $id)` | point read (`WHERE id = ?`) — the hot path | one flat assoc array `['name'=>…, 'age'=>…]`, or `null` |
| `hazedb_query_arr($sql, $args)` | multi-row reads / lists | `['columns'=>[…], 'rows'=>[[…],…]]`, or `null` on error |
| `hazedb_exec_arr($sql, array $args)` | writes (insert/update/delete) | `{"affected":N}` string, error envelope, or `null` |
| `hazedb_query($sql, $args)` | pass-through (proxy to HTTP / cache) | raw JSON string |

`hazedb_get` is both fastest and most ergonomic for the dominant case
(`$u = hazedb_get(...); echo $u['name'];`); `hazedb_query_arr` covers multi-row;
the JSON-string `hazedb_query` stays for when PHP just forwards the bytes.

---

## 5. Technical detail — files changed

| file | change |
|---|---|
| [`db.go`](../db.go) | `ExecValues` + `execPlanValues` — typed write path (no JSON/boxing). |
| [`wire.go`](../wire.go) | (related) `RowsToJSON` hand-rolled — the JSON-string path's encoder, ~5× faster than stdlib; baseline for the array comparison. |
| [`addons/frankenphp-ext/hazedb_ext.go`](../addons/frankenphp-ext/hazedb_ext.go) | The zval trampoline layer (build + read), `hazedb_query_arr`, `hazedb_exec_arr`, `hazedb_get`, and the Go helpers (`pushCell`/`setCell`/`pushStr`/`setStr`). |
| `addons/frankenphp-ext/hazedb_ext.{c,_arginfo.h,_generated.go}`, `hazedb_ext.stub.php` | Regenerated wrappers (by `regenerate.sh`). Commit alongside `hazedb_ext.go`. |
| [`addons/frankenphp-ext/build/test.php`](../addons/frankenphp-ext/build/test.php) + [`smoke.sh`](../addons/frankenphp-ext/build/smoke.sh) | Correctness checks: `query_arr` re-encodes byte-identical to the JSON path; `exec_arr` insert → read-back; `get` → assoc row + `null` when absent. |
| [`bench_typed_args_test.go`](../bench_typed_args_test.go) | `TestExecValuesParity` + 3-way insert benchmark (JSON args / `[]any` / typed Values) isolating the JSON tax. |
| `addons/frankenphp-ext/build/bench.php` | Read bench: raw / json_decode / query_arr / get. |
| `addons/frankenphp-ext/build/hazedb_insert_bench.php` | Insert bench: JSON-args vs array-args. |
| `addons/frankenphp-ext/build/sqlite_bench.php`, `sqlite_insert_bench.php` | SQLite `:memory:` baselines (read + insert). |

Commits: `faca45e` (core `ExecValues`), `453b770` (trampolines + `query_arr` +
`exec_arr`), `cfe3886` (`hazedb_get`).

### The C layer in detail

**There is no hand-maintained `.c` file.** The trampolines live inline in the
cgo preamble — the `/* … */` block directly above `import "C"` in
[`hazedb_ext.go`](../addons/frankenphp-ext/hazedb_ext.go). The
`.c` / `.h` / `_arginfo.h` / `_generated.go` / `.stub.php` files are
machine-generated by `regenerate.sh` from the `// export_php:` directives and
must not be edited by hand.

**Trampolines** — 17 `static`-inline C functions wrapping the Zend macros cgo
cannot call directly (build/README pitfall #5):

*Build, numeric-indexed list (Go rows → PHP array):*

| function | wraps |
|---|---|
| `hzd_arr_new(n)` | `zend_new_array(n)` (pre-sized) |
| `hzd_push_long` / `hzd_push_bool` / `hzd_push_null` | `ZVAL_LONG/BOOL/NULL` + `zend_hash_next_index_insert` |
| `hzd_push_strn(a,s,n)` | `ZVAL_STRINGL` (copies) or `ZVAL_EMPTY_STRING`, then append |
| `hzd_push_arr(a,child)` | `ZVAL_ARR` + append (nested rows) |

*Build, keyed/associative (column name → value), for `hazedb_get` and the envelope keys:*

| function | wraps |
|---|---|
| `hzd_set_arr(a,key,klen,child)` | `ZVAL_ARR` + `zend_hash_str_update` |
| `hzd_set_long` / `hzd_set_bool` / `hzd_set_null` / `hzd_set_strn` | `ZVAL_*` + `zend_hash_str_update` |

*Read (PHP array → Go), for the args-in direction:*

| function | wraps |
|---|---|
| `hzd_arr_count(a)` | `zend_hash_num_elements` |
| `hzd_arr_get(a,i)` | `zend_hash_index_find` (positional/list arrays) |
| `hzd_zval_kind(z)` | normalises `Z_TYPE_P` → `0` null, `1` false, `2` true, `3` long, `4` string, `5` double, `6` other (avoids depending on cgo exposing the `IS_*` macros) |
| `hzd_zval_long` / `hzd_zval_strptr` / `hzd_zval_strlen` | `Z_LVAL_P` / `Z_STRVAL_P` / `Z_STRLEN_P` |

Ownership: strings are **copied** into PHP (`ZVAL_STRINGL`) or out to Go
(`C.GoStringN`), so neither side aliases the other's memory; the result array is
returned with a single ref that `RETURN_ARR` takes.

**Generated files** (by `regenerate.sh`, from the directives):

| file | role |
|---|---|
| `hazedb_ext.c` | one `PHP_FUNCTION(...)` per directive — parses params (`Z_PARAM_STR` / `Z_PARAM_ARRAY_HT`), calls the matching `go_*` export, returns `RETURN_STR` / `RETURN_ARR` / `RETURN_NULL`; plus the `zend_module_entry`. |
| `hazedb_ext_arginfo.h` | arg/return type metadata (`ZEND_BEGIN_ARG_*`, `IS_ARRAY` / `IS_STRING`) + the `ext_functions[]` table. |
| `hazedb_ext_generated.go` | the `//export go_*` shims that call the hand-written Go functions, + `frankenphp.RegisterExtension`. |
| `hazedb_ext.stub.php` | the PHP signatures (IDE / documentation). |

The hand-written Go functions (`hazedb_get`, `hazedb_query_arr`,
`hazedb_exec_arr` and the `pushCell` / `setCell` / `pushStr` / `setStr` helpers)
call the trampolines; the generated `go_*` shims call those Go functions.

---

## 6. Measured results

All in-memory, in-process from PHP (FrankenPHP, PHP 8.5.6), AMD Ryzen AI MAX+
395; SQLite baselines in `php:8.4-cli`. Numbers are steady-state; re-run the
benches under `build/` to reproduce.

**Go-side write ceiling** (`bench_typed_args_test.go`):

| path | ns/op | allocs/op |
|---|---|---|
| JSON args (`QueryArgs` + `Exec`) | ~1515 | 22 |
| `[]any` args (skip JSON) | ~395 | 2 |
| typed `Values` (`ExecValues`) | ~360 | 1 |

The JSON decode alone is ~1120 ns / 20 allocs of pure overhead. A CPU profile of
the typed path shows **zero** arg-handling cost remaining — the rest is the
storage floor (PK map ~37%, row build ~18%).

**Inserts** (independent, in-memory):

| path | qps | ns/op |
|---|---|---|
| `hazedb_exec` (JSON args) | ~445K | ~2230 |
| **`hazedb_exec_arr` (native array)** | **~875K** | **~1140** |
| SQLite `:memory:` autocommit | ~383K | ~2610 |
| SQLite `:memory:` batched (reference) | ~980K | ~1020 |

`hazedb_exec_arr` ≈ 2× the JSON form, ~2.3× SQLite autocommit, ~90% of SQLite's
batched best case — with independent inserts.

**Reads** (PHP gets usable data):

| path | qps | ns/op |
|---|---|---|
| `hazedb_query` + `json_decode` | ~1.16M | ~860 |
| `hazedb_query_arr` (envelope) | ~1.6M | ~630 |
| **`hazedb_get` (flat assoc)** | **~2.85M** | **~350** |
| SQLite `:memory:` prepared-reuse | ~1.28M | ~780 |
| `hazedb_query` raw JSON string (not usable) | ~3.0M | ~330 |

`hazedb_get` ≈ 2.5× `json_decode`, ~1.8× `query_arr`, ~2.2× SQLite — and lands at
roughly the raw-JSON-string speed while returning a directly usable array.

---

## 7. Gotchas & ABI notes

- **frankenphp-gen array ABI** (verified empirically): an `array` parameter
  requires the Go param to be `*C.zend_array` (a `Z_PARAM_ARRAY_HT`); a `?array`
  return makes the Go function return `unsafe.Pointer` (a `zend_array*`) and the
  C wrapper does `RETURN_ARR`. `regenerate.sh` patches `RETURN_EMPTY_ARRAY`→
  `RETURN_NULL` so a nil return surfaces as PHP `null`.
- **cgo cannot call the `ZVAL_` / `zend_hash_` macros directly** — hence the
  static-inline C trampolines in the `hazedb_ext.go` preamble (build/README
  pitfall #5).
- **`*/` inside a cgo comment closes the block.** A doc line containing
  `ZVAL_*/zend_hash_*` once terminated the `/* */` cgo preamble early and the C
  was parsed as Go. Keep `*/` out of cgo-preamble prose.
- **String lifetime.** Args read from a zval are copied into Go memory
  (`C.GoStringN`) before being stored, so nothing aliases PHP-arena memory. The
  SQL string is a zero-copy view (`db.prepare` clones it on a cache miss).
- **`hazedb_get` is point-read only** (single row); use `hazedb_query_arr` for
  multi-row.
- **Building a `zend_array` is not free** (~300 ns for an envelope): that is why
  `query_arr`'s read win is modest (~1.3×) while `hazedb_get` — which builds one
  array instead of four — is ~2.5×.
- **Packaging unchanged.** The trampolines are extra C in the same extension;
  one `xcaddy` build still produces a single FrankenPHP binary with hazedb baked
  in.

---

## 8. Open / future

- **Storage floor.** After removing the JSON tax, inserts sit on the storage
  engine (PK map + row alloc). The largest lever (a single-probe find-or-insert
  on the PK map) was measured earlier at only ~8% and shelved as not worth the
  complexity.
- **Pooled result buffer** for the JSON-string path (zero-alloc steady state)
  remains an option for `hazedb_query`.
- **Assoc rows for multi-row** (`query_arr` returning `[['name'=>…],…]`) could be
  offered if the per-row keyed shape is wanted, at the cost of per-cell key
  copies.
