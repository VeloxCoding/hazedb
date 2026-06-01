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
| DDL | `CREATE TABLE name (col TYPE …)` with `PRIMARY KEY` / `PARTITION KEY` constraints and `[ORDERED] INDEX [name] (col[, col…])` declarations (a composite/multi-column index must be `ORDERED`); `DROP TABLE name` |
| Column types | `int`, `text`/`string`, `bool`, `bytes`/`blob`, `uuid` |
| Writes | `INSERT INTO … VALUES (…)`, `UPDATE … SET … WHERE …`, `DELETE FROM … WHERE …` |
| `SELECT` | `*` or an explicit column list, `FROM t [alias]`, optional `WHERE`, `ORDER BY col [ASC\|DESC]`, `LIMIT n`, `OFFSET m` |
| `JOIN` | two-table `[INNER\|LEFT [OUTER]\|RIGHT [OUTER]] JOIN t2 [alias] ON a.col = b.col` (single equi-join); the probed join column **must be the PK or indexed**; columns are `table.col` / `alias.col` |
| `WHERE` expressions | comparisons `= != < <= > >=`, `IS [NOT] NULL`, boolean `AND` / `OR` / `NOT`, arithmetic `+ - *`, parentheses, `?` positional parameters |
| Literals | integer, string, `true`/`false`, `null` |

**Primary key:** every table has exactly one PK, type `uuid`. INSERT
auto-generates a UUIDv7 when the id is omitted; supply your own (any canonical
UUID string) only when the app needs to address the row later.

## CREATE TABLE — full syntax

```sql
CREATE TABLE name (
    col_name TYPE [constraints],
    ...,
    [ORDERED] INDEX [index_name] (col[, col…])
)
```

**Column constraints** (any order, all optional):

| constraint | meaning |
|---|---|
| `PRIMARY KEY` | required PK column (exactly one, type `uuid`) |
| `PARTITION KEY` | co-locates rows sharing this value into one shard |
| `IMMUTABLE` | value set on INSERT, rejected on UPDATE |
| `NULL` | column is nullable (default is `NOT NULL`) |
| `NOT NULL` | explicit — same as the default |

**Index variants:**

| syntax | type | serves |
|---|---|---|
| `INDEX (col)` | hash | `WHERE col = ?` lookups |
| `ORDERED INDEX (col)` | sorted | `WHERE col = ?` + `ORDER BY col` walks |
| `ORDERED INDEX (a, b)` | sorted, composite | `WHERE a = ?`, `WHERE a = ? AND b = ?`, and the killer `WHERE a = ? ORDER BY b` (no sort) — see *Composite indexes* below |
| `INDEX name (col)` | hash, named | same as hash; name is optional metadata |
| `ORDERED INDEX name (col)` | sorted, named | same as ordered |

A composite index must be `ORDERED` (the hash form would only serve exact
whole-tuple equality — no better than the bucket intersection the planner already
does) and all its columns must be `NOT NULL` (a `(a=X, b=NULL)` row matches
`WHERE a=?` but a composite never indexes a row with a NULL component, so a
nullable-component composite would miss it — such a query falls back to a scan).

**Examples:**

```sql
-- Minimal
CREATE TABLE users (id uuid primary key, name text, age int)

-- Hash index: equality lookups
CREATE TABLE users (id uuid primary key, email text, INDEX (email))

-- Named hash index
CREATE TABLE users (id uuid primary key, name text, INDEX by_name (name))

-- Ordered index: equality lookups + ORDER BY walks (no full scan)
CREATE TABLE posts (id uuid primary key, score int, ORDERED INDEX (score))

-- Multiple indexes on one table
CREATE TABLE users (
    id uuid primary key,
    name text,
    email text,
    INDEX (email),
    ORDERED INDEX (name)
)

-- Composite (must be ORDERED): serves WHERE author = ? ORDER BY title with no
-- sort, the per-author list-view pattern
CREATE TABLE posts (
    id uuid primary key,
    author uuid,
    title text,
    ORDERED INDEX (author, title)
)

-- All constraint types combined
CREATE TABLE messages (
    id   uuid primary key,
    thread uuid partition key,
    seq  int  immutable,
    body text null,
    INDEX (thread)
)
```

**Rejected — will error:**

| case | example | error |
|---|---|---|
| Hash composite index | `INDEX (a, b)` | "composite index must be ORDERED" (use `ORDERED INDEX (a, b)`) |
| Index on the PK column | `INDEX (id)` | rejected at plan time |
| Index repeating a column | `ORDERED INDEX (a, a)` | rejected |
| Duplicate index on same column | two `INDEX (email)` | rejected |
| Index on a partitioned table | table has `PARTITION KEY` + `INDEX` | rejected |
| Re-creating an existing table | `CREATE TABLE users …` twice | `ErrTableExists` |
| `ALTER TABLE` | not in the grammar | parse error |
| `IF NOT EXISTS` | not in the grammar | parse error |

## Secondary indexes

Declare an index on a non-PK column to make `WHERE col = ?` a lookup instead of
a full scan:

```sql
CREATE TABLE users (
    id uuid primary key, name text, age int null, email text,
    INDEX (email),
    ORDERED INDEX (name)   -- ORDERED also serves ranges + ORDER BY; default is hash (equality only)
)
```

A query that pins an indexed column by equality uses the index. When several
indexed columns are pinned in an `AND` (`WHERE name = ? AND city = ?`), it
**intersects** their index buckets — fetching only the rows matching *both*
(e.g. the 1000 Peters in Amsterdam, not all 8000 Peters) — then applies the rest
of the `WHERE` as a residual filter. A range (`<`, `>`) or `OR` conjunct is left
to that residual filter, not the index.

**`ORDER BY` on a filtered list** is index-assisted: `WHERE author = ? ORDER BY
date DESC LIMIT 20` resolves the author's rows through the index, then sorts that
(small) subset and applies the `LIMIT` — the everyday list-view pattern, no full
scan.

**A global `ORDER BY` on an `ORDERED INDEX`** walks the sorted index in order and
stops at `LIMIT` — `SELECT … ORDER BY email ASC LIMIT 100` on a hash `INDEX`
would scan every row + keep a top-N heap, but on `ORDERED INDEX (email)` it
touches ~`LIMIT` rows. A hash index serves equality only; an `ORDERED INDEX`
serves equality (binary search) + `ORDER BY` (and, later, ranges). A *bare*
`ORDER BY` on a column with no ordered index still scans then sorts.

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
rejected). Composite (multi-column) indexes are supported in the `ORDERED` form
with `NOT NULL` columns — see below.

### Composite indexes

`ORDERED INDEX (a, b)` stores rows ordered by the `(a, b)` tuple, so a query
pinning a leading prefix is served directly:

- **`WHERE a = ?`** — a prefix lookup (every row under that `a`). ~310 ns whether
  the prefix holds 50 or 5000 rows — O(1) in bucket size.
- **`WHERE a = ? AND b = ?`** — an exact tuple lookup to ~1 row.
- **`WHERE a = ? ORDER BY b [LIMIT n]`** — the killer: the `a = ?` sub-range is
  *already sorted by `b`*, so the walk emits in order and stops at `LIMIT` — **no
  sort, no whole-bucket fetch.** Measured ~1.6 µs / 27 allocs at a 5000-row
  bucket vs ~196 µs for the single-column gather-then-sort — ~124×, and **flat in
  bucket size**. Concurrent readers scale (≈1.0 µs/op across 32 cores).

**In a join**, a composite `(joinkey, ordercol)` on the probed table serves a
probe-side `ORDER BY` the same way: the single-driver case walks the probe in
order and stops at `LIMIT`. The headline `posts p JOIN users u ON p.author = u.id
WHERE u.name = ? ORDER BY p.title LIMIT 10 OFFSET 20` runs in ~4.9 µs — ~4× the
single-column top-N plan, and faster than the same query on SQLite `:memory:`.

**Rules:** composite must be `ORDERED` (a hash composite only serves exact
whole-tuple equality — no better than bucket intersection — and is rejected);
all components must be `NOT NULL` (else a NULL-component row matching the prefix
would be missing from the index, so the planner falls back to a scan); only a
contiguous leading prefix can be pinned (`(a, b)` serves `WHERE a = ?` but not
`WHERE b = ?` alone). Maintenance cost is ~1 alloc/row on the background merge —
on par with a single single-column index.

### `ORDER BY` cost on very large buckets

A *single-column* index ordered by a *different* column than the filter still
ranks the whole bucket (the case below). When the hot pattern is `WHERE key = ?
ORDER BY sortcol`, an `ORDERED INDEX (key, sortcol)` composite removes that cost
structurally — the walk above is flat in bucket size. The remarks below apply to
the single-column case where no such composite exists.

Index-assisted `ORDER BY` scales with how many rows share the filter value (the
bucket), not with the `LIMIT`. A `LIMIT n` keeps only the top `n` via a heap, so
it stays light for normal list views (tens–hundreds of rows per key: an author's
posts, a category's products). A *single* key holding thousands of rows — a forum
thread with 10k messages, a category with 50k items — re-checks and ranks the
whole bucket: measured ~200 µs for a 5000-row bucket with `LIMIT 20`. Still far
cheaper than a full scan, but it grows with the bucket.

Three known levers, **deferred** — each only pays off for these huge, hot
single-key buckets, and is weighed against keeping the code simple (profiled
shares of that path):

1. **Residual-only re-check (~23% CPU).** Every candidate is re-validated against
   the *full* `WHERE`, including the indexed equality the index already
   guarantees. Re-confirming that conjunct with a cheap key compare and running
   the general evaluator only for the *residual* (`AND age > ?`) would skip the
   evaluator entirely for a pure `WHERE col = ?`.
2. **Batched shard lock (~16% CPU).** Candidates are fetched one shard-lock at a
   time (one lock pair per row). Grouping candidates by shard and locking each
   shard once would cut thousands of lock pairs to ~32.
3. **Key-only top-N (~78% of allocs).** The heap clones rows that transiently
   enter the top-N. Collecting `(sort-key, pk)` first and cloning only the final
   `LIMIT` rows removes those clones — at the cost of a small consistency window
   between reading the key and fetching the row.

For the common (filtered, modest-bucket) list view none of these matter; see
[secondary-indexes.md](secondary-indexes.md) for the design.

## Not supported — will error or fail to parse

- **No `FULL OUTER` / `CROSS` / N-way `JOIN`** — two-table `INNER`/`LEFT`/`RIGHT` equi-joins work (the probed column must be the PK or indexed); `CROSS` is excluded by design (a Cartesian product has no indexable join column), `FULL OUTER` and 3+ table joins are deferred.
- **No aggregates / grouping** — `COUNT`, `SUM`, `AVG`, `GROUP BY`, `HAVING` are absent.
- **No `DISTINCT`, no subqueries**, no expressions or aliases in the `SELECT` column list (bare column names only).
- **No `ALTER TABLE`**, and **no `IF [NOT] EXISTS`** — re-running `CREATE TABLE` on an existing table errors.
- **No SQL transactions** — there is no `BEGIN` / `COMMIT` / `ROLLBACK` in the
  grammar. Transactions exist only as a **Go-API** feature (a `db.Tx` closure)
  and are deliberately not exposed to the PHP/HTTP surface.
- Numeric literals are integers only — hazedb has no float type (a `float` arg is rejected too).

## What this means in practice

For the common PHP pattern — read or write a row by id, look one up by an
indexed column (`WHERE email = ?`), or list + filter + order + limit rows in a
single table — plus a two-table indexed join (`INNER`/`LEFT`/`RIGHT`) — these
functions cover everything. For relational work beyond that (multi-table joins,
aggregation, reporting), do that in your application or in the source-of-truth
database; hazedb is the hot-read / write-buffer layer in front of it, not the
system of record.

## Future additions — feasibility tiers

Most missing features *can* be added; the engine isn't fighting them. They
split by cost, and one ("just expose transactions") is misleading. The deeper
question is design fit, not feasibility — see the note at the end.

**Tier 1 — cheap, drop-in, no architectural impact:**

- **`OFFSET`** — *shipped* (rev. 25). A skip counter before the `LIMIT` cut on
  every read path. (Inherent caveat: large offsets are O(offset) — the skipped
  rows are still walked, true of every DB.)
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

- **`JOIN`** — *partly shipped* (rev. 26). Two-table `INNER`/`LEFT`/`RIGHT`
  equi-joins run today as an indexed nested-loop, but only where the probed
  join column is the PK or indexed (the **indexed-only law** — no O(A×B) scan).
  Still on the deliberate line: `FULL OUTER` (would need driving both sides),
  `CROSS` (a Cartesian product has no indexable join column — excluded by
  design), N-way joins, and non-equi `ON` conditions.
- **SQL `BEGIN`/`COMMIT`** — *not* "add a token." The transaction engine already
  exists (the Go `db.Tx` closure, M6). The hard part is that SQL-level
  transactions need **session state** to tie multiple statements together, and
  the PHP/HTTP surface is stateless — each call is independent. Exposing it
  means inventing a connection/session concept first.

**Design-fit note.** Almost all of the above is technically open; the real
question is whether it *should* land here. hazedb is the hot-read / write-buffer
layer in front of a source-of-truth DB — its value is speed + simplicity.
`OFFSET` / `DISTINCT` / `COUNT(*)` fit without bloat. The two-table indexed
join landed for the same reason — it stays `O(driver)` and never degrades to a
scan. Unbounded joins (`FULL OUTER`, `CROSS`, N-way) and reporting-style
aggregation are where it starts turning into a general SQL engine — exactly the
work normally left to the database behind it. So that part of Tier 3 stays a
deliberate "probably not, by design," not a "not yet."

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
