# hazedb as a Caddy module

This package wraps hazedb as a Caddy HTTP handler, so a single Caddy binary
serves your site **and** an in-process SQL store — no separate database process,
no network hop.

It exposes two endpoints. Both take `POST` with a JSON body
`{"sql": "...", "args": [...]}`:

| Endpoint | Runs | Returns |
|---|---|---|
| `POST /query` | `SELECT` | `{"columns":[...],"rows":[[...],...]}` |
| `POST /exec`  | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | `{"affected":N}` |

`args` is an optional positional list for `?` placeholders. JSON → SQL value
mapping: number → INT, bool → BOOL, null → NULL, string → STRING, **except** a
string in canonical UUID form (`8-4-4-4-12` hex) → UUID — so you can address and
insert UUID columns from JSON.

## Build a Caddy binary with the module

Uses [xcaddy](https://github.com/caddyserver/xcaddy). Until the core hazedb
version that ships `db_registry.go` + `wire.go` is tagged, build against the
local checkout (the submodule's `replace` points at `../`):

```bash
# from a checkout of this repo
xcaddy build \
    --with github.com/VeloxCoding/hazedb/caddymodule=./caddymodule \
    --with github.com/VeloxCoding/hazedb=.
```

After the core is published, the plain form works:

```bash
xcaddy build --with github.com/VeloxCoding/hazedb/caddymodule
```

## Configure (Caddyfile)

```caddyfile
:8080 {
    handle /db/* {
        hazedb {
            wal_path      /var/lib/hazedb/data.wal   # omit for memory-only
            size_hint     100000
            wal_sync                                 # fsync on the flush ticker
            wal_flush_ms  1000
            init_sql      /etc/hazedb/schema.sql     # CREATE TABLE + seed, run once
            registry_name default
        }
    }
}
```

All subdirectives are optional. With no `wal_path` the store is memory-only.
Tables are created at runtime: put your `CREATE TABLE` statements in the
`init_sql` file, or `POST` them to `/exec`.

## Schema / tables

hazedb creates tables at runtime, so the module opens with an empty schema.
Define tables one of two ways:

- **`init_sql`** — a file of `;`-separated statements run once at startup
  (typical: `CREATE TABLE ...` plus any seed rows). Don't put a `;` inside a
  string literal in that file.
- **`POST /exec`** — send `CREATE TABLE ...` like any other write.

## Sharing the instance with PHP

The module publishes its `*DB` in hazedb's process-wide registry under
`registry_name` (default `"default"`). The FrankenPHP/PHP extension
(`addons/frankenphp-ext`) looks up that same name, so PHP calls and HTTP calls
hit one identical database — no second copy.

## WAL + config reload caveat

With `wal_path` set, a Caddy **graceful config reload** runs the new handler's
`Provision` (which opens the WAL file) before the old handler's `Cleanup`
(which closes it) — briefly two writers on one file. Memory mode reloads
cleanly. For durable deployments, prefer a full restart over a graceful reload
when changing this handler.
