# Calling hazedb directly from PHP

FrankenPHP lets you add PHP functions written in Go. This addon uses that to let
PHP run SQL against hazedb **in-process** — a Go function call inside the same
FrankenPHP/Caddy process, no socket, no protocol, no HTTP roundtrip.

```php
$id = '0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';   // app-supplied PK (any canonical UUID)

hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
            json_encode([$id, 'Alice', 30]));

// Reads: pass the key you already have DIRECTLY — no json_encode.
$json = hazedb_query('SELECT name, age FROM users WHERE id = ?', $id);
// {"columns":["name","age"],"rows":[["Alice",30]]}
```

You can also **omit the id** on INSERT — hazedb auto-fills a UUIDv7 for a `uuid
primary key` column. Supply your own id (as above) only when the app needs to
address the row later.

PHP and the Caddy module share **one** `*DB` through hazedb's process-wide
registry: the Caddy module publishes it under `"default"` during `Provision`;
these functions resolve that same slot at call time. There is no second hidden
database — PHP and HTTP clients see identical state. If no `hazedb` directive is
in the Caddyfile (the module never provisioned), every function returns `null`.

## PHP functions

| function | runs | returns |
|---|---|---|
| `hazedb_query(string $sql, string $args): ?string` | `SELECT` | `{"columns":[...],"rows":[[...],...]}` (JSON string), `{"error":"..."}` on SQL error, `null` if no DB |
| `hazedb_exec(string $sql, string $args): ?string` | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | `{"affected":N}`, error envelope, or `null` |
| `hazedb_ping(): string` | — | `pong` if a Caddy module registered a DB, `pong (no db)` otherwise |

`hazedb_exec` is the write path — it is the "insert" function, generalised to
every write/DDL statement, mirroring the Go API's `db.Query` / `db.Exec` split.

**`$args` has two forms:**

- **Direct (one arg, no JSON):** a value not starting with `[` is bound as a
  single positional arg — a canonical-UUID string → UUID, otherwise STRING. Use
  this for the common single-key read: `hazedb_query($sql, $id)`. No
  `json_encode`, no `json.Decode` — measured ~2× faster than the JSON form
  (~0.70 µs vs ~1.6 µs per call).
- **JSON array (multi-arg / typed):** a value starting with `[` is a JSON array
  of positional args — `json_encode([$id, 'Alice', 30])`. Mapping: number →
  INT, bool → BOOL, null → NULL, string → STRING, canonical-UUID string → UUID.
  Use this for inserts and multi-condition queries. (hazedb has no float type;
  non-integer numbers are rejected.)

Pass `""` for no args.

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
| `hazedb_ext.go` | The extension source — `//export_php:function` directives + cgo helpers. Returns are `?string` (JSON). |
| `hazedb_ext.{c,h,_arginfo.h,_generated.go}`, `hazedb_ext.stub.php` | Generated wrappers (by `regenerate.sh`). Commit alongside `hazedb_ext.go` changes. |
| `go.mod` | Module pin; build overrides versions via local `--with`. |
| `build/` | Build / smoke tooling — see `build/README.md` for the build-chain pitfalls (the recipe is mirrored from the scopecache addon). |

## cgo lifetime note

The SQL string is **deep-copied** at the boundary because `db.prepare` keeps it
as a `stmtCache` key (a zero-copy alias over PHP-arena memory would dangle after
the request ends). `args_json` is read synchronously and taken as a view.
