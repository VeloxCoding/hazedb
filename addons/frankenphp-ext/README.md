# Calling hazedb directly from PHP

FrankenPHP lets you add PHP functions written in Go. This addon uses that to let
PHP run SQL against hazedb **in-process** — a Go function call inside the same
FrankenPHP/Caddy process, no socket, no protocol, no HTTP roundtrip.

```php
$id = hazedb_uuidv7();

hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
            json_encode([$id, 'Alice', 30]));

$json = hazedb_query('SELECT name, age FROM users WHERE id = ?', json_encode([$id]));
// {"columns":["name","age"],"rows":[["Alice",30]]}
$rows = json_decode($json, true)['rows'];
```

PHP and the Caddy module share **one** `*DB` through hazedb's process-wide
registry: the Caddy module publishes it under `"default"` during `Provision`;
these functions resolve that same slot at call time. There is no second hidden
database — PHP and HTTP clients see identical state. If no `hazedb` directive is
in the Caddyfile (the module never provisioned), every function returns `null`.

## PHP functions

| function | runs | returns |
|---|---|---|
| `hazedb_query(string $sql, string $args_json): ?string` | `SELECT` | `{"columns":[...],"rows":[[...],...]}` (JSON string), `{"error":"..."}` on SQL error, `null` if no DB |
| `hazedb_exec(string $sql, string $args_json): ?string` | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | `{"affected":N}`, error envelope, or `null` |
| `hazedb_uuidv7(): string` | — | a fresh UUIDv7 string (for UUID primary keys) |

`hazedb_exec` is the write path — it is the "insert" function, generalised to
every write/DDL statement, mirroring the Go API's `db.Query` / `db.Exec` split.

`args_json` is a JSON array of positional args for `?` placeholders, or `""` /
`"[]"` for none. Mapping: number → INT, bool → BOOL, null → NULL, string →
STRING, **except** a canonical-UUID string → UUID (so you can pass the
`hazedb_uuidv7()` value straight into a UUID column). hazedb has no float type;
non-integer numbers are rejected.

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
