# Durability — WAL buffer, SQLite system-of-record, in-memory serving copy

**Status: proposed** (design note — nothing below is implemented yet).

This note records the agreed durability design: keep hazedb's in-memory engine
as the hot serving layer, batch the existing WAL into an **on-disk SQLite file**
that holds current state, and make recovery a single unified path. It answers
three open gaps at once — *how big may the dataset get*, *how do we stop the WAL
growing forever*, and *how do we recover quickly* — without putting disk on the
hot write path.

It builds on two pieces that already exist and are tested: the typed-mutation
**WAL** ([wal.go](../wal.go), fsync modes `WALSync` / `WALSyncPerWrite` in
[db.go](../db.go)) and **WAL-replay recovery** (`Open` → `replayWAL` in
[db.go](../db.go), covered by `recovery_test.go`). It reuses the async-merger's
**dirty-set tracking** (`markDirtyLocked`, `dirty []UUID`, `dirtyCount` in
[store.go](../store.go); the merge loop in [secindex.go](../secindex.go)).

---

## 1. The gaps today

Verified in the code, 2026-05-30:

- **No memory budget.** Nothing tracks how many bytes the dataset occupies; there
  is no cap, no admission control, no eviction. A sustained insert load grows RAM
  until the OS OOM-kills the process.
- **No snapshot / checkpoint.** `recCheckpoint` is reserved in the WAL framing
  ([wal.go](../wal.go)) but unimplemented; production WAL rotation does not exist.
  The WAL is append-only forever.
- **Recovery replays the entire history.** `Open` calls `replayWAL` from byte
  zero on every boot. This is correct and crash-tolerant, but its cost is
  O(all mutations ever written), not O(current dataset). A small dataset under
  heavy update/delete churn still accrues an unbounded WAL and an unbounded
  restart time.

Recovery *works* — the missing pieces are the two that **bound resources**: RAM
(budget) and log/restart-time (a compacted on-disk copy).

## 2. The model and its invariant

Three layers with one strict rule:

```
   reads / writes
        ↓
  in-memory engine        ← hot serving copy (sharded arenas, secondary indexes)
        ↓ (mutations)
       WAL                 ← durability buffer: fsynced every ~0.5–1s
        ↓ (batched drain, ~every 60s)
  SQLite file (disk)       ← system of record: current state, compacted, portable
```

> **Invariant: the in-memory engine is only ever loaded *from SQLite*. The WAL
> only ever feeds *SQLite*.**

The WAL never replays directly into memory. That collapses the design to exactly
two code paths:

- **apply mutations → SQLite** (the drainer; also used during recovery), and
- **load SQLite → memory** (every boot).

The existing in-memory `replayWAL` path is retired: the in-memory layer no longer
needs to understand the WAL format — it bulk-loads rows from SQLite and rebuilds
its secondary indexes (`rebuildAllIndexes`, [secindex.go](../secindex.go)).

Mental model: **in-memory = hot serving copy, SQLite = system of record on disk,
WAL = the buffer batching writes into SQLite.**

## 3. Memory budget — bounds RAM (independent, build first)

SQLite bounds *disk and recovery*; it says nothing about RAM. The live set must
still fit in memory, so a byte budget is a separate, prior requirement.

- **Accounting:** track bytes per shard/table as rows are added and removed.
- **Policy:** hazedb is a system of record now, so overflow **rejects new writes**
  (backpressure — return an error) rather than evicting committed rows.
- **Reference:** the sibling project *scopecache* already implements this shape
  (`reserveBytes` admission control, byte accounting, 507-on-overflow). Lift the
  accounting; swap the policy from evict to reject.

Order rationale: it is the smaller piece, it has a proven pattern to copy, and it
puts a hard ceiling on the live dataset — which in turn caps the SQLite file size
and the boot-load time, de-risking everything downstream.

## 4. The drain — WAL → SQLite

### Cadence

- **WAL fsync every ~0.5–1s** (the existing `WALSync` ticker). This is the
  durability window — see §5.
- **SQLite drain every ~60s**, applied as **one transaction**.

### Coalesce by dirty-set, do not replay history

Do not apply ten updates to one row as ten SQLite writes. Reuse the merger's
per-shard `dirty []UUID`: each interval, take the set of changed PKs, read their
*current* state from the in-memory store, and bulk-**UPSERT** each changed row
once; **DELETE** the ones now gone. Two consequences:

- **Cost is bounded by distinct rows touched**, not operation count. A hot row
  hammered 10,000×/min is one UPSERT.
- **Idempotent.** Re-UPSERTing the same current state is a no-op-equivalent, which
  is what makes a crash mid-drain safe (§6).

### Measured drain throughput

The drain is single-threaded (one writer to SQLite), so single-threaded numbers
are the right ones. Measured in the build container (FrankenPHP's bundled PHP /
`pdo_sqlite`, WSL2 disk), single transaction, 500k simple indexed rows:

| pattern | rows/s |
| --- | --- |
| disk WAL, autocommit (1 txn/row) | ~52,000 |
| **disk WAL, one big transaction** | **~528,000** |
| disk rollback-journal, one txn | ~612,000 |
| `:memory:`, one txn | ~1,220,000 |

Batching is the whole game: ~10× over per-row autocommit. At hazedb's measured
~67k writes/s, a minute is ~4M mutations; draining ~4M rows at ~528k/s ≈ **8 s**.
SQLite works ~8 s of every 60 s and idles the rest — it keeps up with a wide
margin, with headroom for far higher bare-metal write rates.

> The ~10k inserts/s seen in the concurrent HTTP insert benchmark is **not** the
> drain rate. That was the pathological case — HTTP overhead plus 1,000
> connections contending on one write-lock, one implicit transaction per row. The
> drainer is a single writer doing one bulk transaction; its rate is the ~528k/s
> above.

### The only standing constraint

`drain_time < interval`. With coalescing and ~528k/s this holds until
distinct-rows-changed-per-minute approaches the whole dataset repeatedly at
extreme rates. It is directly measurable, so it never has to be guessed; if a
workload ever threatens it, shorten the interval or apply write backpressure.

## 5. The durability window is the WAL's, not SQLite's

SQLite lagging ~60s behind does **not** mean ~60s of data-loss exposure. Data is
durable once it is fsynced to the **WAL** — every ~0.5–1s. SQLite is only the
compacted bulk store; the WAL tail always covers the gap between the last drain
and a crash. **Worst-case loss on crash ≈ the WAL fsync interval (~1s)**,
independent of the drain cadence.

## 6. Boot and recovery are one path

Both reduce to:

> **Every boot:** replay any WAL tail into SQLite (one transaction, commit), then
> load SQLite into memory. Only then rotate/clear the consumed WAL.

1. After a crash, on-disk SQLite is up to ~1 interval stale.
2. **Replay the WAL tail into SQLite inside one transaction, commit** → SQLite is
   at the crash point (minus the ≤1s the WAL had not fsynced).
3. **Load SQLite into the in-memory shards**, rebuild secondary indexes.
4. Rotate/clear the consumed WAL.

The single-transaction wrap in step 2 makes recovery itself crash-safe: if the
process dies mid-replay, SQLite rolls back to the last drain state, the WAL tail
is still intact (step 4 has not run), and the next boot replays the whole tail
again from the same starting point — all-or-nothing, re-runnable.

Clean shutdown flushes a final drain, so the tail is empty and boot is a straight
load. Crash leaves ≤1 interval of tail. **Same path either way** — no separate
"clean boot" vs "crash boot" logic.

## 7. What this costs

The honest trade versus a hand-rolled snapshot format:

- **A SQLite dependency** — cgo (`mattn/go-sqlite3`) or pure-Go
  (`modernc.org/sqlite`, slower). For a lean-and-fast project, making SQLite the
  durability backend is a deliberate choice. It is defensible — the memory hot
  path and the on-disk cold store are different roles — but the framing must be
  explicit: hazedb serves from RAM and *persists via* SQLite.
- **A type-mapping layer** — every hazedb type (UUID, typed cells, NULL,
  secondary-index defs, and eventually partitioned tables) must map faithfully to
  SQLite columns in both directions.
- **A drain ceiling** — ~528k/s batched. Bursty-then-quiet workloads catch up;
  sustained write rates above the drain ceiling would grow the WAL tail without
  bound. See §4's constraint.

In exchange: no snapshot serializer to write, native compaction (SQLite stores
current state, not history), a portable copy-one-file backup, an inspectable DB,
and SQLite's battle-tested crash recovery instead of hand-rolled correctness.

## 8. Open decisions (deferred)

- **SQLite driver:** cgo (`mattn`) vs pure-Go (`modernc`) — measure both against
  the §4 drain rate before choosing.
- **One SQLite file vs one per shard.** Per-shard files would let the drain run
  concurrently (one writer each, no cross-shard lock) and sidestep SQLite's
  single-writer limit entirely, at the cost of N files and a fan-in load. Decide
  after measuring whether a single-file drain keeps up (§4 says it does, with
  margin).
- **Drain trigger:** fixed interval vs WAL-size threshold vs dirty-count
  threshold (or a combination). The merger's `dirtyCount` is already a cheap
  signal.
- **Snapshot-free alternative retained for comparison:** a custom in-memory
  snapshot + WAL-truncate remains viable if the SQLite dependency is judged too
  heavy. The decision table is dependency-appetite + portable-file value vs.
  novel code; this note assumes the SQLite route.
