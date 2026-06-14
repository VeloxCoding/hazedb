# hazedb as a Caddy module

This package wraps hazedb as a Caddy HTTP handler, so a single Caddy binary
serves your site **and** an in-process SQL store — no separate database process,
no network hop.

It exposes five endpoints. `POST /query` and `POST /exec` take a JSON body
`{"sql": "...", "args": [...]}`; the `GET` routes take URL parameters.

| Endpoint | Runs | Returns |
|---|---|---|
| `POST /query` | ad-hoc `SELECT` (incl. JOINs) | `{"columns":[...],"rows":[[...],...]}` |
| `POST /exec`  | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | `{"affected":N}` |
| `GET /get?table=T&id=UUID` (or `&col=C&val=V`) `[&cols=a,b]` | one-row read (PK or indexed-column fast path) | one JSON object, or `null` |
| `GET /list?table=T` `[&cols=a,b][&col=C&val=V][&limit=N]` | multi-row read | `[{...},...]` |
| `GET /meta` | store-size overview | `{"tables":N,"max_bytes":M,"total_rows":R,"total_approx_bytes":B,"total_tombstones":T,"table_stats":[{name,rows,columns,indexes,approx_bytes,tombstones},...]}` |

`GET /meta` takes no parameters; it reports the table count, the configured
`max_bytes` cap (0 = unlimited), the store-wide `total_rows` /
`total_approx_bytes` / `total_tombstones`, and per table the row / column / index
counts, an approximate in-RAM byte size, and `tombstones` — for dashboards and
health checks. The byte sizes are estimates (cell payloads plus modeled
overhead, biased slightly high), not exact accounting.

**Tombstones** are rows deleted but whose memory slot is not yet reclaimed. A
background sweeper compacts shards that have gone mostly dead, so `tombstones` is
a **gauge** — it rises with deletes and falls as the sweeper runs, rather than
only resetting on restart. A momentarily high `tombstones / (rows + tombstones)`
fraction between sweeps is normal; a persistently high one means deletes are
outrunning the sweeper. (Scan cost is independent — partitioned scans stay
proportional to live rows regardless.)

**Byte cap.** Set `max_bytes` (below) to bound the store's RAM. An `INSERT` that
would push `total_approx_bytes` past the cap is rejected with **HTTP 507**
(Insufficient Storage); the store never auto-evicts, so the client frees space
with `DELETE` / `DROP TABLE`. `total_approx_bytes` vs `max_bytes` from `/meta` is
the headroom gauge.

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
            wal            on                        # on (default) = durable (crash loses ≤~0.5s); off = memory-only
            wal_path       /var/lib/hazedb/wal       # optional: override the WAL directory. Default <caddy-data-dir>/hazedb/wal
            companion_path /var/lib/hazedb/hazedb.db # optional: on-disk SQLite companion (events/health always, + data mirror when wal is on). Default <caddy-data-dir>/hazedb/hazedb.db (or next to wal_path)
            init_sql       /etc/hazedb/schema.sql   # CREATE TABLE + seed, run once at startup
            registry_name  default                  # name the *DB is published under for the PHP extension
            max_body_bytes 4194304                  # POST body cap for /query and /exec (default 4 MiB)
            max_bytes      1073741824               # cap the store's RAM (1 GiB); over-cap INSERT → HTTP 507. 0/unset = unlimited
        }
    }
}
```

All subdirectives are optional. By default the store is durable (`wal on`): with nothing set, the WAL and the SQLite companion both live under `<caddy-data-dir>/hazedb/` (e.g. `/var/lib/caddy/hazedb/` under systemd). Set `wal off` for a memory-only store.
The WAL has one durability story — writes seal to disk within ~0.5s, so a crash
loses at most that window; there are no durability levels or fsync modes. Tables
are created at runtime: put your `CREATE TABLE` statements in the `init_sql`
file, or `POST` them to `/exec`.

## Schema / tables

hazedb creates tables at runtime, so the module opens with an empty schema.
Define tables one of two ways:

- **`init_sql`** — a file of `;`-separated statements run once at startup
  (typical: `CREATE TABLE ...` plus any seed rows). It runs as a trusted script,
  so a `;` inside a string literal is safe and seed rows may use inline literal
  values.
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
