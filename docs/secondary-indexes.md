# Secondary indexes — async maintenance + hybrid reads

**Status: in progress** (phased rollout S1–S9 below).

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
pending list**, not a seq watermark.

---

## 2. Core idea

The index is derived state, owned by one background **merger**. The write path
never touches it — it only appends a delta record to the **pending list of the
shard it already holds locked**, so one-op-one-lock is preserved exactly (no new
lock, no second shard, no contention point, regardless of how many indexes
exist). Reads are **index ∪ pending-replay**, then **re-evaluated against the
live row** as a correctness backstop. No WAL change; no seq needed.

---

## 3. Data structures

**Schema.** `TableDef.Indexes []IndexDef{Name string, Column string, Unique bool}`;
resolved in the catalog to a column ordinal (`resolvedIndex{ordinal int, unique bool}`).

**The index** (one per declared index): value → PK(s).

- unique: `map[indexKey]UUID`
- non-unique: `map[indexKey][]UUID` (bucket list — same shape as the existing
  partition `tails`)
- `indexKey` is a comparable, normalised encoding of the indexed `Value`
  (`string`→string, `int`→int64, `uuid`→array, `bool`→bool). `Value` itself is
  not map-key-able (it carries an `unsafe.Pointer`).
- Guarded by one `RWMutex`, **written only by the merger**; readers `RLock`.
  (A later optimisation may swap to an immutable snapshot + atomic pointer for
  lock-free reads — only if reads are shown to contend on the lock.)

**Per-shard pending delta.** Added to `tableShard`:
`pending []pendingEntry{pk UUID, op uint8, oldKey, newKey indexKey}`. Allocated
only when the table declares ≥1 index — zero cost otherwise, exactly like
`tails`/`pkDir` exist only for partitioned tables.

---

## 4. Write path

Under the shard lock that insert/update/delete **already** holds, after the row
mutation: append one `pendingEntry`. On update, record an entry only when an
**indexed** column actually changed (a cheap ordinal check). That is the entire
write-path cost: one append, no new locks, independent of the index count.

---

## 5. Read path (hybrid)

For `WHERE indexedcol = ?`:

1. **Index lookup**(value) → candidate PKs (pre-merge coverage).
2. **Pending overlay**: walk each shard's pending list; apply
   additions/removals/changes for *this* value to the candidate set (post-merge
   coverage).
3. For each resulting PK: **O(1) fetch** from its PK shard, **re-evaluate the
   predicate on the live row**, project.

**Why this is correct at any lag.** The index covers everything before the merge,
pending covers everything after → the union misses no row. The live re-check
filters false positives (a stale entry whose row has since changed or died — and
tombstones are already skipped, [exec.go](../exec.go)). The index/pending only
*narrows* the candidate set; the truth always comes from the live row.

---

## 6. The merger (async)

One goroutine per DB. Triggered on an interval **or** a pending-size threshold
(env-configurable, like the other caps). Each cycle:

1. Per shard: lock, **swap the pending slice out** (leave an empty one), unlock —
   the shard lock is held only momentarily.
2. Coalesce the batch per PK (last write wins).
3. Apply to the index under the index lock (or build a new snapshot + atomic swap).

**Atomicity rule.** A pending entry is "consumed" only once its effect is in the
index. Swapping the pending slice out *before* applying to the index guarantees a
concurrent reader sees the row in pending until the moment it is in the index —
never in the gap between the two.

**Backpressure.** If writes outrun the merger, pending grows. Cap it: on overflow
force a synchronous merge (or fall back to a full scan for that read) and `log()`
the event — never silently truncate.

---

## 7. Recovery

The index is **not** in the WAL (derived state). After WAL replay, rebuild it
once by a full scan (or mark everything pending so the first merge builds it).
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
| **S4** | per-shard pending delta; maintenance moved **off** the write path; read = index ∪ pending; merge via an explicit `Merge()` call | write→query before merge finds the row via pending |
| **S5** | background merger goroutine + atomic swap + threshold/interval config | `-race` stress (writers+readers+merger); invariant always holds |
| **S6** | non-unique buckets (`name`) | multiple PKs per value |
| **S7** | multiple indexes on one table | both dimensions correct |
| **S8** | recovery rebuild after replay | crash/replay → index correct |
| **S9** | benchmarks: read speedup vs scan, write overhead, merge cost | + a `bench.sh` mode |

The "correct first (synchronous), fast second (async)" order pins the read path
in S2–S3 before the async complexity arrives — the read path does not change
after that.

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
- **Writes outrun the merger**: bounded pending + synchronous-merge fallback;
  log, never silently drop (§6).
- **Index concurrency**: start with `RWMutex`; move to snapshot + atomic swap only
  if reads are shown to contend.
- **Memory**: the index + pending are extra residency; pending is bounded by the
  merge lag.
