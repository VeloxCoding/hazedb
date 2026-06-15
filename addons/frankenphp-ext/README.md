# Calling hazedb directly from PHP

FrankenPHP lets you add PHP functions written in Go. This addon uses that to let
PHP run SQL against hazedb **in-process** — a Go function call inside the same
FrankenPHP/Caddy process, no socket, no protocol, no HTTP roundtrip.

The surface mirrors **PDO**'s vocabulary: pass args as an array (like
`PDOStatement::execute([...])`), read rows back as native PHP arrays keyed by
column name (like `fetch`/`fetchAll(PDO::FETCH_ASSOC)`), and get an affected
count from writes (like `rowCount`). No JSON crosses the boundary in either
direction.

```php
$id = '0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';   // app-supplied PK (any canonical UUID)

hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');
hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)', [$id, 'Alice', 30]);

$row = hazedb_fetch('SELECT name, age FROM users WHERE id = ?', $id);   // single arg: pass it bare (fast path)
// ['name' => 'Alice', 'age' => 30]   (age is a native int, not "30")   — or null

$rows = hazedb_fetchall('SELECT name, age FROM users WHERE age >= ?', 18);
// [['name' => 'Alice', 'age' => 30], ...]   (empty array if none)
```

You can also **omit the id** on INSERT — hazedb auto-fills a UUIDv7 for a `uuid
primary key` column. Supply your own id only when the app needs to address the
row later.

PHP and the Caddy module share **one** `*DB` through hazedb's process-wide
registry: the Caddy module publishes it under `"default"` during `Provision`;
these functions resolve that same slot at call time. There is no second hidden
database — PHP and HTTP clients see identical state. If no `hazedb` directive is
in the Caddyfile (the module never provisioned), reads return `null` and `exec`
returns `-1`.

## PHP functions

| function | runs | returns |
|---|---|---|
| `hazedb_fetch(string $sql, mixed $args = null): ?array` | one-row `SELECT` | a flat assoc row `['col'=>val,...]`, or `null` (no row / no DB / error) |
| `hazedb_fetchall(string $sql, mixed $args = null): ?array` | multi-row `SELECT` | a list of assoc rows `[['col'=>val,...],...]`; `[]` if none; `null` on error |
| `hazedb_fetchall_json(string $sql, mixed $args = null): ?string` | multi-row `SELECT` | the same data as a JSON string `[{...},...]` for pass-through; `null` on error |
| `hazedb_exec(string $sql, mixed $args = null): int` | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | affected row count, or `-1` on error / no DB |
| `hazedb_meta(): ?string` | store-size overview | a JSON string `{"tables":N,"max_bytes":M,"total_rows":R,"total_approx_bytes":B,"total_tombstones":T,"table_stats":[...]}` (same bytes as Caddy `GET /meta`); `json_decode` it. `null` only when no DB is registered |
| `hazedb_ping(): string` | — | `pong` if a Caddy module registered a DB, `pong (no db)` otherwise |

When the store has a `max_bytes` cap and it is full, an insert via `hazedb_exec`
returns `-1` (like any error) — read `hazedb_meta`'s `total_approx_bytes` vs
`max_bytes` to tell a full store from a malformed statement, and free space with
`DELETE` / `DROP TABLE`.

PDO mapping: `hazedb_fetch` ≈ `fetch(PDO::FETCH_ASSOC)`, `hazedb_fetchall` ≈
`fetchAll(PDO::FETCH_ASSOC)`, `hazedb_exec` ≈ `execute(...)` + `rowCount()`. The
three PDO steps (`prepare` → `execute` → `fetch`) collapse into one call.

**`$args` takes two forms:** a native PHP **array** of positional params
(`[$a, $b]`, like `execute([...])`), **or** — for a single param — the **bare
scalar** (`$id`). The scalar form is the fast path: no array is built in PHP or
read in Go, ~80 ns/call cheaper, so `hazedb_fetch($sql, $id)` beats
`hazedb_fetch($sql, [$id])` for the hot single-key read. Both are accepted; use
the array whenever there is more than one param. `$args` is optional (omit it for
no-arg statements like `CREATE TABLE`). The arg type mapping and the full SQL
surface — accepted statements, `CREATE TABLE` syntax (one required `uuid` PK),
indexes, joins — have one owner: [docs/php-sql-layer.md](../../docs/php-sql-layer.md).

**Native types out.** Result cells come back as the real PHP type — `int`,
`bool`, `string` — not stringified. (PDO + SQLite returns everything as a string
by default; hazedb does not.) UUID and BYTES columns come back as strings.

## Build + smoke

```bash
cd addons/frankenphp-ext/build
./regenerate.sh     # generate the committed C wrappers from hazedb_ext.go (~3 min cold)
./build.sh          # xcaddy build dist/frankenphp with the ext + Caddy module
./smoke.sh          # boot the binary, run test.php, assert the cgo path works
```

`build.sh` passes all three hazedb modules (core, caddymodule, this ext) to
xcaddy as local `--with` paths, so no published hazedb tag is needed yet.

## Files

| file | role |
|---|---|
| `hazedb_ext.go` | The extension source — `//export_php:function` directives + the zval trampolines (build + read PHP arrays) + cgo helpers. |
| `hazedb_ext.{c,h,_arginfo.h,_generated.go}`, `hazedb_ext.stub.php` | Generated wrappers (by `regenerate.sh`). Commit alongside `hazedb_ext.go` changes. |
| `go.mod` | Module pin; build overrides versions via local `--with`. |
| `build/` | Build / smoke / benchmark tooling — see `build/README.md`. |

Design + benchmarks: [`docs/php-array-bridge.md`](../../docs/php-array-bridge.md).

## cgo lifetime note

The SQL string is a zero-copy view; `db.prepare` clones it on a cache miss (the
only time it is retained as a `stmtCache` key), so the hot cache-hit path copies
nothing. String args are copied out of the `zend_array` before being stored, so
storage never aliases PHP-arena memory. Result strings/arrays are PHP-owned.
