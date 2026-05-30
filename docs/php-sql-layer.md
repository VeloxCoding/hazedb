# hazedb SQL layer — what you can and cannot run

Reference for the SQL hazedb's parser actually accepts, and how the PHP
functions map onto it. hazedb is a fast key-addressed store with a practical
single-table query layer — **not** a relational engine. Source of truth:
[parser.go](../parser.go) (grammar), [exec.go](../exec.go) (execution). The
*transport* side of these functions (native PHP arrays, no JSON, benchmarks)
lives in [php-array-bridge.md](php-array-bridge.md); this doc is about the SQL
grammar and the PHP API surface.

## The PHP entry points (PDO-shaped)

| function | statement | returns |
|---|---|---|
| `hazedb_fetch()` | one-row `SELECT` | a flat assoc row `['col'=>val,...]`, or `null` |
| `hazedb_fetchall()` | multi-row `SELECT` | a list of assoc rows `[['col'=>val,...],...]`; `[]` if none |
| `hazedb_fetchall_json()` | multi-row `SELECT` | the same data as a JSON string `[{...},...]` for pass-through |
| `hazedb_exec()` | `INSERT` / `UPDATE` / `DELETE` / `CREATE TABLE` / `DROP TABLE` | affected `int`, or `-1` on error |

Pick the right one for the verb: a `SELECT` through `exec` (or a write through a
`fetch*`) errors. `$args` is a `mixed` positional-param holder — a native PHP
**array** (`[$a, $b]`, like PDO `execute`) or, for a single param, the **bare
scalar** (`$id`, the fast path); optional. Between these functions you reach
every statement the parser accepts — but that set is a deliberate subset of SQL
(below).

## Supported SQL

| area | accepted |
|---|---|
| DDL | `CREATE TABLE name (col TYPE …)` with `PRIMARY KEY` / `PARTITION KEY` constraints and `[UNIQUE] INDEX [name] (col)` declarations; `DROP TABLE name` |
| Column types | `int`, `text`/`string`, `bool`, `bytes`/`blob`, `uuid` |
| Writes | `INSERT INTO … VALUES (…)`, `UPDATE … SET … WHERE …`, `DELETE FROM … WHERE …` |
| `SELECT` | `*` or an explicit column list, **one** table (`FROM t`), optional `WHERE`, `ORDER BY col [ASC\|DESC]`, `LIMIT n` |
| `WHERE` expressions | comparisons `= != < <= > >=`, `IS [NOT] NULL`, boolean `AND` / `OR` / `NOT`, arithmetic `+ - *`, parentheses, `?` positional parameters |
| Literals | integer, string, `true`/`false`, `null` |

**Primary key:** every table has exactly one PK, type `uuid`. INSERT
auto-generates a UUIDv7 when the id is omitted; supply your own (any canonical
UUID string) only when the app needs to address the row later.

## Secondary indexes

Declare an index on a non-PK column to make `WHERE col = ?` a lookup instead of
a full scan:

```sql
CREATE TABLE users (
    id uuid primary key, name text, age int null, email text,
    INDEX (email),
    UNIQUE INDEX (name)   -- UNIQUE is a selectivity hint, not an enforced constraint
)
```

A query that pins an indexed column by equality uses the index. When several
indexed columns are pinned in an `AND` (`WHERE name = ? AND city = ?`), it
**intersects** their index buckets — fetching only the rows matching *both*
(e.g. the 1000 Peters in Amsterdam, not all 8000 Peters) — then applies the rest
of the `WHERE` as a residual filter. `OR`, `ORDER BY`, and ranges (`<`, `>`) fall
back to a scan (a range or OR conjunct is just left to the residual filter).

**Async, but always correct.** Indexes are maintained *off the write path*: a
write only flags its row, and a background merger reconciles the index shortly
after. A read combines the index with the just-written (not-yet-merged) rows and
re-checks each against the live row, so it is correct at any merge lag — never
stale, never a wrong row. This fits the cache contract (a brief index lag is
acceptable in front of a source of truth).

**Costs** (measured, 50k rows; see [secondary-indexes.md](secondary-indexes.md)
for the full design; the `idxcmp` harness in the external `demo_and_perf`
testbed reproduces the SQLite comparison):

- **Read:** ~1.6 µs for `WHERE email = ? AND name = ?` — ~75× faster than the
  equivalent full scan, and ~100× faster than the same query on SQLite `:memory:`.
- **Write:** ~+69 ns per insert/update/delete (one flag, independent of how many
  indexes the table has).
- **Re-indexing (merge):** ~30 ms to reconcile 50k rows across two indexes —
  one-time, in the background (every 50 ms) or once at boot after WAL replay.

**Limits:** non-partitioned tables only (an `INDEX` on a partitioned table is
rejected); single-column only (no composite); `UNIQUE` does not reject
duplicates (uniqueness is the operator's promise, used only to read faster).

## Not supported — will error or fail to parse

- **No `JOIN`** — one table per query.
- **No aggregates / grouping** — `COUNT`, `SUM`, `AVG`, `GROUP BY`, `HAVING` are absent.
- **No `DISTINCT`, no `OFFSET`, no subqueries**, no expressions or aliases in the `SELECT` column list (bare column names only).
- **No `ALTER TABLE`**, and **no `IF [NOT] EXISTS`** — re-running `CREATE TABLE` on an existing table errors.
- **No SQL transactions** — there is no `BEGIN` / `COMMIT` / `ROLLBACK` in the
  grammar. Transactions exist only as a **Go-API** feature (a `db.Tx` closure)
  and are deliberately not exposed to the PHP/HTTP surface.
- Numeric literals are integers only — hazedb has no float type (a `float` arg is rejected too).

## What this means in practice

For the common PHP pattern — read or write a row by id, look one up by an
indexed column (`WHERE email = ?`), or list + filter + order + limit rows in a
single table — these functions cover everything. For relational work (joins,
aggregation, reporting), do that in your application or in the source-of-truth
database; hazedb is the hot-read / write-buffer layer in front of it, not the
system of record.

## Future additions — feasibility tiers

Most missing features *can* be added; the engine isn't fighting them. They
split by cost, and one ("just expose transactions") is misleading. The deeper
question is design fit, not feasibility — see the note at the end.

**Tier 1 — cheap, drop-in, no architectural impact:**

- **`OFFSET`** — one parser token + a skip counter before the `LIMIT` cut;
  `ORDER BY` + `LIMIT` are already there. (Inherent caveat: large offsets are
  O(offset) — the skipped rows are still walked, true of every DB.)
- **`DISTINCT`** — a dedup pass over the result rows. Single table, no complication.
- **`COUNT(*)` and simple aggregates without grouping** (`SUM`/`MIN`/`MAX` over
  the matched set) — a single accumulator during the scan.

**Tier 2 — real but tractable, because hazedb is single-table:**

- **`COUNT`/`SUM`/`AVG` + `GROUP BY`/`HAVING`** — a grouping pass (hash of
  group-key → accumulator) plus teaching the `SELECT` list to understand
  *aggregate expressions* instead of today's bare column names. No fundamental
  blocker.
- **`ALTER TABLE`** — the runtime catalog already swaps schema atomically (how
  `CREATE`/`DROP` work); add/drop column means rewriting rows + handling it in
  WAL replay. Touches storage, but doable.

**Tier 3 — genuine design effort:**

- **`JOIN`** — the deliberate line. hazedb is partitioned/sharded by a single
  table's PK; joins need a planner, a multi-table scan strategy, and careful
  ascending lock ordering to avoid deadlock. The RFC moves **multi-table to
  post-1.0** on purpose — deferred, not overlooked.
- **SQL `BEGIN`/`COMMIT`** — *not* "add a token." The transaction engine already
  exists (the Go `db.Tx` closure, M6). The hard part is that SQL-level
  transactions need **session state** to tie multiple statements together, and
  the PHP/HTTP surface is stateless — each call is independent. Exposing it
  means inventing a connection/session concept first.

**Design-fit note.** Almost all of the above is technically open; the real
question is whether it *should* land here. hazedb is the hot-read / write-buffer
layer in front of a source-of-truth DB — its value is speed + simplicity.
`OFFSET` / `DISTINCT` / `COUNT(*)` fit without bloat. Joins and reporting-style
aggregation start turning it into a general SQL engine — exactly the work
normally left to the database behind it. So Tier 3 is a deliberate "probably
not, by design," not a "not yet."

## Examples

```php
// DDL + write (hazedb_exec) — multiple params: a native PHP array
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');
hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
            ['0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b', 'Alice', 30]);

// Read one row by key — single param: pass it bare (fast path). Assoc, or null.
$row = hazedb_fetch('SELECT name, age FROM users WHERE id = ?',
                    '0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b');

// Filter + order + limit, one table — list of assoc rows
$rows = hazedb_fetchall('SELECT name FROM users WHERE age >= ? ORDER BY age DESC LIMIT 10', 18);
```

`$args` (`mixed`, optional): a native PHP **array** of positional params, or a
**bare scalar** for a single param (the fast path). Type mapping: number→INT,
bool→BOOL, null→NULL, string→STRING unless it parses as a canonical UUID.
See [../addons/frankenphp-ext/README.md](../addons/frankenphp-ext/README.md).
