# Secondary indexes — async maintenance + hybrid reads

**Status: implemented** (S1–S9 shipped; non-partitioned tables only).

This note records the design for optional **secondary indexes** on non-PK
columns, declared in DDL (`INDEX (col)`), maintained **asynchronously** by a
background merger, and read through a **hybrid** path that stays correct at any
index lag. It is the agreed answer to "can we index `email` / `name` without
wrecking write throughput?"

---

## 1. Why this shape

hazedb shards every table by its PK ([store.go](../store.go) `shardIdxOf`) and
each insert/update/delete locks **exactly one** shard — that one-op-one-lock rule
is the whole basis of its write parallelism. A synchronous secondary index
breaks it: the index is keyed by `email`, but the row lives in `hash(PK)`, so
maintaining it on the write path forces either a global contention point or a
second shard lock per write (and the deadlock-ordering surface grows with the
number of indexes).

**Async maintenance removes that tension.** The index is *derived state* owned by
a single background goroutine; the write path never touches it. This is
defensible here precisely because hazedb is a hot-read / write-buffer cache in
front of a source of truth — its durability contract is already "2xx = applied,
not durable," so a brief index lag is acceptable where it would not be in a
system of record.

Two facts about the current engine shaped the design (both verified in the code,
2026-05-30):

- **Updates mutate the row arena in place** (`s.rows[rowID] = nr`,
  [store.go](../store.go); deletes tombstone in place). So "scan the arena past a
  cursor" cannot serve as the un-indexed tail — an in-place update to an old slot
  would be missed.
- **There is no global `seq` counter** (only a hypothetical mention in
  [schema.go](../schema.go)). So the un-indexed tail must be tracked explicitly,
  not derived from a sequence number.

Both point to the same choice: the un-indexed tail is an **explicit per-shard
dirty list**, not a seq watermark.

---

## 2. Core idea

The index is derived state, owned by one background **merger**. The write path
never touches it — it only appends the mutated **PK to the dirty list of the
shard it already holds locked**, so one-op-one-lock is preserved exactly (no new
lock, no second shard, no contention point, regardless of how many indexes
exist). Reads are **index ∪ dirty-overlay**, then **re-evaluated against the
live row** as a correctness backstop. No WAL change; no seq needed.

---

## 3. Data structures

**Schema.** `TableDef.Indexes []IndexDef{Name string, Column string, Ordered bool}`;
resolved in the catalog to a column ordinal (`resolvedIndex{name string, ordinal int, ordered bool}`).

**The index** (one per declared index): value → PK(s). The forward map is always
a bucket list — a value may map to many PKs (there is no enforced-unique form):

- hash (default): forward `map[indexKey][]UUID` (bucket list — same shape as the
  existing partition `tails`) plus a reverse `map[UUID]indexKey`.
- ordered: a key-sorted slice (`[]ordEntry`) serving equality + ranges + ORDER BY,
  rebuilt on merge.
- `indexKey` is a comparable, normalised encoding of the indexed `Value`
  (`string`→string, `int`→int64, `uuid`→its two big-endian uint64 words,
  `bool`→bool — mirroring `Value`'s packing, so a UUID key needs no `[16]byte`
  round-trip or allocation). `Value` itself is not map-key-able (it carries an
  `unsafe.Pointer`).
- Guarded by one `RWMutex`, **written only by the merger**; readers `RLock`.
  (A later optimisation may swap to an immutable snapshot + atomic pointer for
  lock-free reads — only if reads are shown to contend on the lock.)

**Per-shard dirty lists.** Added to `tableShard`: `dirtyRead []UUID` and
`dirtyDel []UUID` — just the PKs mutated since the last merge, no op or key
payload. The split is by read-relevance: `dirtyRead` holds inserts and
indexed-column updates (rows the read overlay must consider — their live state
may not yet be in the index); `dirtyDel` holds deletes (the merger removes their
stale entry via the reverse map `rev`, but reads skip them — a deleted row can
never match). An UPDATE that touches no indexed column is recorded in neither:
the index stays valid, so the row needs no merge and no overlay slot. Allocated
only when the table declares ≥1 index — zero cost otherwise, like `tails`/`pkDir`
for partitioned tables. Two store-wide atomics: `readDirtyCount` lets a read skip
the overlay scan in steady state (and gates `dirtyTooDenseForScan`, so a delete
burst never pushes reads onto the scan path); `delDirtyCount` adds in for the
merge size-trigger (both lists need merging). The write side records one PK in
one list — one append, no extra atomic.

---

## 4. Write path

Under the shard lock that insert/update/delete **already** holds, after the row
mutation: append the PK to the shard's `dirty` list (`markDirtyLocked`). It is
appended unconditionally on every live write — the merger re-reads the live row,
so a write that left the indexed column unchanged just re-applies the same key
(idempotent), never a wrong entry. That is the entire write-path cost: one
append, no new locks, independent of the index count.

---

## 5. Read path (hybrid)

For `WHERE indexedcol = ?`:

1. **Index lookup**(value) → candidate PKs (pre-merge coverage). When the WHERE
   pins *several* indexed columns by equality (`name = ? AND city = ?`), look up
   each one's bucket and **intersect** them (smaller set first, a pure PK set
   operation — no row fetches), so only rows matching all of them survive (the
   1000 Peters in Amsterdam, not all 8000 Peters).
2. **Dirty overlay**: union the dirty PKs (mutated since the last merge,
   membership uncertain) into the candidate set — always, regardless of the
   intersection, since a not-yet-merged row may match.
3. For each resulting PK: **O(1) fetch** from its PK shard, **evaluate the FULL
   `WHERE` on the live row**, project. The full-WHERE check both re-validates the
   indexed equality (so a stale entry is harmless) and applies any residual
   conjuncts (`AND age > ?`, an `OR` group).

**Why this is correct at any lag.** The index covers everything before the merge,
the dirty overlay everything after → the union misses no row. The live full-WHERE
check filters false positives (a stale entry whose row has since changed or died
— and tombstones are already skipped, [exec.go](../exec.go)). The index/overlay
only *narrows* the candidate set; the truth always comes from the live row.

---

## 6. The merger (async)

One goroutine per DB. It fires on `indexMergeInterval` (like the WAL flush
ticker) **or early on a size-trigger** — whichever comes first — so a write burst
cannot grow the overlay unbounded between ticks. The size-trigger is adaptive by
default (`indexMergeThreshold == 0`): a table fires when its dirty overlay reaches
~¼ of its live rows. A positive `indexMergeThreshold` is a fixed absolute cap; a
negative value disables it (pure interval). The merger polls the dirty counters
itself, so the trigger never touches the write path. `Close` runs one final merge
so a clean shutdown leaves no lag. Each cycle, per shard:

1. Lock, **snapshot the current dirty prefix** (`dirty[:n]` into a batch),
   unlock — the shard lock is held only momentarily.
2. For each PK in the batch: project the indexed columns from the live row and
   `apply` to each index (hash: into `fwd`; ordered: into `rev`). Re-reading the
   live row makes duplicate PKs in the batch naturally idempotent — no explicit
   coalescing pass.
3. After every shard's batch is applied, rebuild each ordered index's sorted
   view from `rev`, then re-lock each shard and **drop the processed prefix**
   (`dirty = dirty[n:]`), leaving entries appended during the merge for next time.

**No-gap rule.** A dirty PK is dropped only *after* its effect is in the index
(and the ordered sorted view rebuilt). Applying before dropping guarantees a
concurrent reader sees the row via the dirty overlay until the moment it is in
the index — never in the gap between the two.

**Backpressure.** Two mechanisms bound a sustained write burst. (1) The merger's
size-trigger (above) fires early when the overlay grows dense, capping its size
instead of waiting out the interval. (2) On the read side, `dirtyTooDenseForScan`
([store.go](../store.go)): when the overlay still outgrows the table (the trigger
hasn't caught up), an indexed UPDATE/DELETE falls back to a full scan rather than
walking an overlay larger than the table — so it is never slower than the
pre-index scan path. Both are bounds, not drops: every dirty PK is still merged.

---

## 7. Recovery

The index is **not** in the WAL (derived state). After WAL replay, rebuild it
once by a full scan (or mark everything dirty so the first merge builds it).
A one-time boot cost; no WAL format change.

---

## 8. Placement

**Core, opt-in via DDL `INDEX(col)`, zero cost when not declared** — consistent
with `tails`/`pkDir` existing only for partitioned tables. An addon would have to
intercept core writes (needs a hook), which is messier; the async model removes
the original lock objections, so there is no longer a reason to keep it out of
the core.

---

## 9. Phased rollout (`go test ./...` green between every step)

| step | content | test |
|---|---|---|
| **S1** | parser + `TableDef.Indexes` + catalog resolution | DDL round-trip; `INDEX` on an unknown column errors |
| **S2** | index maintained synchronously inline on writes + index-only read ("correct, slow-write" baseline) | query == full-scan result |
| **S3** | hybrid read with live re-check (staleness-safe), still synchronous maintenance | inject a stale entry → re-check filters it |
| **S4** | per-shard dirty list; maintenance moved **off** the write path; read = index ∪ dirty; merge via an explicit `Merge()` call | write→query before merge finds the row via the dirty overlay |
| **S5** | background merger goroutine + atomic swap + threshold/interval config | `-race` stress (writers+readers+merger); invariant always holds |
| **S6** | non-unique buckets (`name`) | multiple PKs per value |
| **S7** | multiple indexes on one table | both dimensions correct |
| **S8** | recovery rebuild after replay | crash/replay → index correct |
| **S9** | benchmarks: read speedup vs scan, write overhead, merge cost | + a `bench.sh` mode |

The "correct first (synchronous), fast second (async)" order pins the read path
in S2–S3 before the async complexity arrives — the read path does not change
after that.

### Measured (S9, AMD Ryzen AI MAX+ 395, 10k-row table, `bench_index_test.go`)

| benchmark | result | note |
|---|---|---|
| indexed point read | ~1330 ns/op | O(1) bucket + live re-check |
| full-scan read (same query, no index) | ~100400 ns/op | O(n) over 10k rows — **~75× slower** |
| insert WITHOUT index | ~425 ns/op | baseline |
| insert WITH index | ~494 ns/op | **+~69 ns** = the per-shard dirty append |
| merge 10k dirty rows | ~3.86 ms | boot/rebuild-scale; runs in the background |

The read speedup is the whole point; the write overhead is one append under the
lock the write already holds, independent of the index count. The per-index
RWMutex is kept (no lock-free snapshot swap): the reader cost is dominated by the
by-PK re-check clone, not lock contention, so a snapshot swap is not yet
warranted.

---

## 10. The golden invariant + testing

One property everything must preserve: **at any instant, for any value,
`hybrid-read(value) == full-scan(value)`.** That is a property/fuzz target under
`-race` with concurrent writes, merges and reads. If that invariant holds, the
whole async mechanism is correct regardless of timing.

---

## 11. Open decisions / risks

- **`indexKey` encoding**: typed maps per column type vs one encoded-string key —
  measure (allocation vs uniformity).
- **Writes outrun the merger** (not yet implemented): the merger is interval-only
  with no dirty-size cap, so a sustained burst grows the dirty lists and the merge
  lag between ticks. Intended guard: a bounded-dirty threshold that triggers a
  synchronous merge (or a one-off full scan for that read); log, never silently
  drop (§6).
- **Index concurrency**: start with `RWMutex`; move to snapshot + atomic swap only
  if reads are shown to contend.
- **Memory**: the index + dirty lists are extra residency; the dirty lists are
  bounded by the merge lag.
- **Large-bucket `ORDER BY`**: index-assisted `ORDER BY` scales with the bucket
  (rows sharing the filter value), not the `LIMIT`. Fine for normal list views;
  for a single huge hot key (a 10k-message thread) three deferred levers apply —
  residual-only re-check (skip re-evaluating the index-guaranteed conjunct),
  batched per-shard locking, and a key-only top-N (clone only the final `LIMIT`).
  Profiled shares and trade-offs in [php-sql-layer.md](php-sql-layer.md). Deferred:
  they only pay off for huge hot buckets, against keeping the path simple.
- **Sorted buckets for allocation-free intersection — considered & rejected (v0.1.13).**
  `intersectPKs` builds a `map[UUID]struct{}` per multi-index `AND` query; keeping
  buckets PK-sorted would allow a two-pointer intersection with no map. Rejected
  for now: profiling `WHERE name=? AND city=?` (~1040 of 50k rows) put the
  intersection map at only **~15%** of the query's ~958 KB/op — the bulk (~71%) is
  fetching and projecting the result rows (`Row.Clone` + `projectClone` +
  `execSelectIdx`), which the change does not touch, and the two-pointer's own
  `out` slice (~11%) stays regardless. The cost is permanent and on the write
  side (sorted buckets = insertion cost per append, or a per-merge re-sort), to
  optimize a narrow query shape that is not shown hot. If this path ever profiles
  hot, prefer a bounded scratch-map pool (`sync.Pool`) first — it removes the map
  allocation with no change to bucket storage or the write path.
- **Per-shard batch dedup in the merger — considered & deferred (v0.1.13).** Under
  update churn a shard's `dirty` can hold the same PK many times before a merge
  runs; the merge then does a redundant `getByPKProjectInto` + `apply` per copy
  (merge cost scales with total dirty entries, not the working set — measured
  ~8.6 ms to reconcile 100 hot rows churned 500× vs ~2.3 µs un-churned). The
  larger, cheaper half was taken instead: `apply` now skips the fwd remove+append
  when the key is unchanged (`rev[pk] == newKey`), which on that churn case cut
  ~3× and removed ~all the allocations with no map and no tuning knob. The
  residual is the K× `getByPKProjectInto` per duplicate (~63 ns each); collapsing
  it needs deduping the batch to unique PKs. Deferred because the trigger is
  awkward — gating on `len(batch)` misfires on a large but dup-FREE batch (a 50k
  unique-insert merge would build a 50k-entry dedup map that finds nothing), and
  the real signal (dup ratio) isn't known without scanning. If a real churn
  workload shows the residual hurts, add it with a REUSED scratch map (not a
  per-merge alloc), per shard (a PK lives in exactly one shard, so per-shard dedup
  catches all its copies). NOT on the write path — that would tax every write to
  help the rare hot-churn case.

## 12. Ordered indexes (implemented)

`ORDERED INDEX (col)` is a sorted index that serves equality + `ORDER BY` (and,
later, ranges), where the default hash `INDEX` serves equality only. It plugs
into the same async-merge + dirty-overlay model:

- **Structure**: a `[]ordEntry{key, pk}` sorted by key, rebuilt from `rev` by the
  merger once per batch (and recovery after a full scan) — *before* dropping the
  dirty entries, so the no-gap guarantee holds. Equality `lookup()`
  binary-searches it, so the existing equality/AND path works unchanged.
- **Read** (`execSelectOrderedWalk`): a global `ORDER BY col [ASC|DESC] [LIMIT n]`
  merges the sorted-index walk (a non-dirty entry is fresh, so its key drives the
  order and the row is fetched only when selected) with the dirty overlay (live
  rows of not-yet-merged PKs), applies any residual `WHERE`, and stops at `LIMIT`.
- **Measured**: global `ORDER BY email ASC LIMIT 100` over 50k rows went from
  ~811 µs (hash scan + top-N, ~27× behind SQLite) to ~24 µs — ahead of native C
  SQLite. See [php-sql-layer.md](php-sql-layer.md).

Open: range predicates (`col > ?`) on an ordered index reuse the same sorted
view (seek + walk) — not yet wired. Maintenance currently rebuilds the sorted
slice per merge (O(n log n)); a write-heavy large table may want incremental
maintenance (skiplist/btree) — measure before adding.
