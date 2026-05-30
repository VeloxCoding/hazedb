# PHP ↔ hazedb array bridge

**Status: implemented** (FrankenPHP extension under [`addons/frankenphp-ext`](../addons/frankenphp-ext)).

This note records how PHP exchanges data with hazedb as **native PHP arrays**
(no JSON either way), the PDO-shaped API that resulted, the design alternatives
weighed, the measured results, and exactly which files changed. It documents
shipped work — not an open proposal.

---

## 1. Summary

The extension originally passed all multi-value args and all results as **JSON
strings** (`json_encode` in PHP → `json.Decode` in Go, and back). That
round-trip dominated the per-call cost: ~1.1 µs of Go-side JSON decode per
insert and a ~447 ns `json_decode` tax per read.

The fix is a small **zval trampoline layer** (static-inline C wrapping the Zend
macros cgo cannot call) plus typed Go entry points, exposing a **PDO-shaped**
API that skips JSON entirely:

| function | ≈ PDO | returns |
|---|---|---|
| `hazedb_fetch($sql, $args=null)` | `fetch(PDO::FETCH_ASSOC)` | one assoc row, or `null` |
| `hazedb_fetchall($sql, $args=null)` | `fetchAll(PDO::FETCH_ASSOC)` | list of assoc rows; `[]` if none |
| `hazedb_fetchall_json($sql, $args=null)` | — | the same data as a JSON string (pass-through) |
| `hazedb_exec($sql, $args=null)` | `execute(...)` + `rowCount()` | affected `int`, or `-1` on error |

`$args` (`mixed`, optional) takes either a native PHP **array** of positional
params (`[$a, $b]`, like `execute`) **or**, for a single param, the **bare
scalar** (`$id`) — the scalar form is the fast path (~80 ns/call cheaper; no
array built in PHP or read in Go). Result cells come back as **native PHP types**
(int, bool, string) — not stringified the way PDO+SQLite does by default.

Measured in one FrankenPHP process (PHP 8.5.6), in-memory:

| | hazedb | SQLite `:memory:` | factor |
|---|---|---|---|
| INSERT (independent) | ~940K qps | ~370K (autocommit) / ~850K (batched) | ~2.5× autocommit; beats batched |
| point read → assoc row | ~2.07M qps (`hazedb_fetch`) | ~1.06M (prepared + FETCH_ASSOC) | ~2× |

---

## 2. The problem

The PHP surface is string-only by nature (the first functions were
string-in/string-out, which needs no C glue). To pass several typed values
through one string parameter, args were JSON-encoded; results came back as a
JSON envelope the caller had to `json_decode`. Measured costs:

| step | cost |
|---|---|
| Args JSON decode (Go), 3-element array | ~1100 ns / ~20 allocs |
| `json_decode` of a result (PHP) | ~447 ns (≈55% of the decoded call) |

For an insert, half the per-call cost was the JSON args round-trip; for a read
returning usable data, more than half was PHP's `json_decode`.

---

## 3. Design space considered

| option | verdict |
|---|---|
| **Keep JSON** | Simple, but pays the full round-trip both ways. |
| **Hand-rolled flat-array args parser** (still a JSON string) | Removes ~half the args tax, but PHP still `json_encode`s. Partial. |
| **goccy/go-json** | ~1.3× over stdlib for this shape — *slower* than the hand-rolled JSON encoder in `wire.go`, and adds a core dependency. Rejected. |
| **MessagePack** | No Go-side win over hand-rolled JSON; needs a PHP extension; payload size is irrelevant in-process. Rejected. |
| **Native PHP arrays via zval trampolines** | Removes JSON on both sides. Needs static-inline C glue, but stays in one binary. **Chosen.** |

The native-array path wins because it deletes the serialisation step rather than
speeding it up. The decision was gated on a pure-Go measurement first: the typed
write path proved ~4.2× faster than the JSON-args path before any C glue was
written.

**API naming** landed on PDO's vocabulary (`fetch`/`fetchall`/`exec`) so it is
instantly familiar. The single-vs-many distinction is two functions (`fetch` vs
`fetchall`), matching PDO's `fetch()` vs `fetchAll()` — it is the caller's
explicit choice, never inferred from the data or the args.

---

## 4. The solution

Two layers.

**Core (Go) — typed entry points** that bypass the `[]any`/JSON conversion:

- [`ExecValues(sql, args ...Value)`](../db.go) — write with pre-typed Values.
- [`QueryValues`](../db.go) / [`QueryRowValues`](../db.go) — read counterparts.
- [`RowsToJSONObjects`](../wire.go) — hand-rolled `[{...},...]` encoder for
  `hazedb_fetchall_json`.

These clone bytes only at the write boundary (`cloneValue`); reads never store
args, so they pass Values straight through.

**Extension (cgo) — the zval trampolines + the four functions** in
[`hazedb_ext.go`](../addons/frankenphp-ext/hazedb_ext.go):

- **Build** (Go rows → PHP): `hzd_arr_new`, `hzd_push_arr`, and keyed setters
  `hzd_set_long/bool/null/strn` (assoc rows keyed by column name).
- **Read** (PHP args array → Go): `hzd_arr_count`, `hzd_arr_get`,
  `hzd_zval_kind` (normalises the zval type) and the value accessors →
  `valuesFromZval` builds `[]Value`.

### Which function when

- **single row** → `hazedb_fetch` → flat assoc `['name'=>…]` or `null`.
- **many rows** → `hazedb_fetchall` → `[['name'=>…],…]` (`[]` if none).
- **forward as JSON** (HTTP/cache, no PHP decode) → `hazedb_fetchall_json`.
- **write** → `hazedb_exec` → affected `int`.

---

## 5. Technical detail — files changed

| file | change |
|---|---|
| [`db.go`](../db.go) | `ExecValues` + `QueryValues` + `QueryRowValues` (typed entry points). |
| [`exec.go`](../exec.go) | `evalLitOrParamValue` (Value-arg PK eval). |
| [`wire.go`](../wire.go) | `RowsToJSON` hand-rolled; `RowsToJSONObjects` (list-of-objects). |
| [`addons/frankenphp-ext/hazedb_ext.go`](../addons/frankenphp-ext/hazedb_ext.go) | The zval trampolines + `hazedb_fetch` / `hazedb_fetchall` / `hazedb_fetchall_json` / `hazedb_exec` / `hazedb_ping` + `valuesFromZval` / `setCell` helpers. |
| `addons/frankenphp-ext/hazedb_ext.{c,_arginfo.h,_generated.go}`, `.stub.php` | Generated wrappers (`regenerate.sh`). Commit with `hazedb_ext.go`. |
| `addons/frankenphp-ext/build/test.php` + `smoke.sh` | Correctness checks (emit `*_ok=yes` markers). |
| `bench_typed_args_test.go`, `bench_encode_test.go` | `TestExecValuesParity`, `TestQueryValuesParity`, `TestEncodeParity` + insert/encode benchmarks. |
| `addons/frankenphp-ext/build/{bench,hazedb_insert_bench,sqlite_bench,sqlite_insert_bench,compare_bench}.php` | Benchmark harness (hazedb + SQLite `:memory:` baselines). |

### The C layer in detail

**There is no hand-maintained `.c` file.** The trampolines live inline in the
cgo preamble (the `/* … */` block above `import "C"`) of `hazedb_ext.go`; the
generated files are machine-made and must not be edited.

| trampoline | wraps |
|---|---|
| `hzd_arr_new(n)` | `zend_new_array(n)` |
| `hzd_push_arr(a, child)` | `ZVAL_ARR` + `zend_hash_next_index_insert` (append a row to the list) |
| `hzd_set_long/bool/null/strn(a, key, klen, …)` | `ZVAL_*` + `zend_hash_str_update` (assoc cell by column name) |
| `hzd_arr_count(a)` | `zend_hash_num_elements` |
| `hzd_arr_get(a, i)` | `zend_hash_index_find` |
| `hzd_zval_kind(z)` | `Z_TYPE_P` → `0` null `1` false `2` true `3` long `4` string `5` double `6` other |
| `hzd_zval_long/strptr/strlen(z)` | `Z_LVAL_P` / `Z_STRVAL_P` / `Z_STRLEN_P` |

Generated files: `hazedb_ext.c` = one `PHP_FUNCTION(...)` per directive (param
parse → `go_*` export → `RETURN_ARR`/`RETURN_STR`/`RETURN_LONG`/`RETURN_NULL`);
`_arginfo.h` = type metadata + the `ext_functions[]` table; `_generated.go` =
the `//export go_*` shims + `RegisterExtension`; `.stub.php` = PHP signatures.

---

## 6. Measured results

All in the **same** FrankenPHP binary (PHP 8.5.6), AMD Ryzen AI MAX+ 395 —
hazedb and the SQLite baselines run in one environment (the binary ships
`pdo_sqlite`/`sqlite3`). Reproduce with the `build/` scripts (`compare_bench.php`
over HTTP runs both back-to-back). Numbers vary with host load; ratios are
stable because both are measured in one process.

**Go-side write ceiling** (`bench_typed_args_test.go`):

| path | ns/op | allocs/op |
|---|---|---|
| JSON args (`QueryArgs` + `Exec`) | ~1515 | 22 |
| typed `Values` (`ExecValues`) | ~360 | 1 |

The JSON decode alone was ~1120 ns / 20 allocs. A CPU profile of the typed path
shows zero arg-handling cost remaining — the rest is the storage floor.

**Inserts** (independent, in-memory):

| path | qps |
|---|---|
| **`hazedb_exec` (native array)** | **~940K** |
| SQLite `:memory:` autocommit | ~370K |
| SQLite `:memory:` batched (reference) | ~850K |

`hazedb_exec` ≈ 2.5× SQLite autocommit and **beats** SQLite's batched best case —
with independent inserts.

**Reads** (point read by PK → usable assoc row):

| path | qps | ns/op |
|---|---|---|
| **`hazedb_fetch($sql, $id)` — scalar arg** | **~2.4M** | ~417 |
| `hazedb_fetch($sql, [$id])` — array arg | ~2.0M | ~500 |
| SQLite `:memory:` prepared + FETCH_ASSOC | ~1.06M | ~940 |

`hazedb_fetch` ≈ 2–2.3× SQLite for the same usable assoc shape.

**Array-args cost and the scalar fast path.** Wrapping a single id as `[$id]`
costs ~80 ns/call — PHP builds a one-element array and Go reads the
`zend_array`. The `mixed $args` param therefore accepts a **bare scalar** too:
`hazedb_fetch($sql, $id)` reads one zval directly (no array build/iterate) and
recovers that cost (~2.4M vs ~2.0M). Arrays remain the form for multi-param
calls; the scalar form is the fast path for the dominant single-key read.

---

## 7. Gotchas & ABI notes

- **frankenphp-gen ABI** (verified empirically):
  - `array $args` → Go `*C.zend_array` (`Z_PARAM_ARRAY_HT`).
  - `mixed $args = null` → Go `*C.zval` (`Z_PARAM_OPTIONAL` + `Z_PARAM_ZVAL`);
    Go gets a **nil** `*C.zval` when omitted. This is what lets `$args` be either
    an array or a bare scalar — Go branches on `Z_TYPE`.
  - `?array` return → Go `unsafe.Pointer` (a `zend_array*`) + `RETURN_ARR`;
    `regenerate.sh` patches `RETURN_EMPTY_ARRAY`→`RETURN_NULL` so nil → PHP null.
  - `int` return → Go `int64` + `RETURN_LONG` (no nullable scalar, hence `exec`
    uses `-1` as the error sentinel).
- **cgo cannot call the `ZVAL_` / `zend_hash_` macros directly** — hence the
  static-inline trampolines (build/README pitfall #5).
- **`*/` inside a cgo comment closes the block.** A doc line containing
  `ZVAL_*/zend_hash_*` once terminated the `/* */` preamble early and the C was
  parsed as Go. Keep `*/` out of cgo-preamble prose.
- **String lifetime.** Args are copied out of the zval (`C.GoStringN`) before
  storage; the SQL string is a zero-copy view (`db.prepare` clones on a miss).
- **Native types out** — int/bool/string come back as the real PHP type, unlike
  PDO+SQLite's default stringify. UUID/BYTES come back as strings.
- **Building a `zend_array` is not free** (~300 ns for a small assoc row) — that
  cost, plus the array-args read, is why `fetch` (~470 ns) does not match the raw
  json-string speed.

---

## 8. Open / future

- **Storage floor.** After removing the JSON tax, inserts sit on the storage
  engine (PK map + row alloc). The largest lever (a single-probe find-or-insert
  on the PK map) was measured earlier at only ~8% and shelved.
- **Pooled result buffer** for `hazedb_fetchall_json` (zero-alloc steady state).

(The scalar-arg fast path — `mixed $args` accepting a bare scalar — is
**implemented**; see §6.)
