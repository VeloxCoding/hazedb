# Durability — WAL buffer, SQLite system-of-record, in-memory serving copy

**Status: implemented** (opt-in; WAL segments + SQLite mirror + reclaim +
SQLite-backed recovery all shipped and tested). The memory budget (§3) remains
the one open piece.

This note records the agreed durability design: keep hazedb's in-memory engine
as the hot serving layer, batch the existing WAL into an **on-disk SQLite file**
that holds current state, and make recovery a single unified path. It answers
three open gaps at once — *how big may the dataset get*, *how do we stop the WAL
growing forever*, and *how do we recover quickly* — without putting disk on the
hot write path.

## As built (and where it refined the proposal)

The drain is keyed on **sealed WAL segments**, not the byte-offset / dirty-set
mechanics first sketched in §4 below. Two reasons the segment approach won:

- The dirty-set is owned by the secondary-index merger, which **clears it every
  ~50ms** — a 60s drainer cannot reuse it. Reading sealed segments sidesteps that.
- A drainer only ever touches **sealed** (closed) segments, never the open one,
  so the "don't read a file being appended to" question disappears by construction.

What shipped (opt-in via `Options.WALPath`: set = on, empty = memory-only):

- **Born-sealed WAL**: `WALPath` is a directory of immutable `seg-<n>.wal` files.
  A write appends a complete record to an in-memory buffer under the WAL lock;
  the buffer seals into the next segment — written to `seg-<n>.tmp`, fsynced,
  then **atomically renamed** into place — once it reaches 1 MiB or ~0.5s,
  whichever first. There is no "active" appended-to file and no rotate step: a
  flush *is* a new sealed segment. So every `seg-*.wal` is complete by
  construction (a crash mid-flush leaves a `.tmp`, cleaned on open) and the log
  never grows as one unbounded file. See [wal.go](../wal.go) /
  [wal_segment.go](../wal_segment.go).
- **SQLite mirror** (`SQLitePath`; drains on its interval, default 1s): a
  background loop replays each sealed segment **faithfully** (one SQLite
  transaction per segment; INSERT/UPDATE/DELETE in WAL order — *not* coalesced),
  recording the segment number in the same transaction
  (`_hz_meta.last_drained_segment`), then **deletes the segment**. So the WAL
  stays bounded at ~the undrained tail. Every segment on disk is sealed (there is
  no open active file), so there is no open file to race and no age gate is
  needed. Driver: `modernc.org/sqlite` (pure Go). See [drain.go](../drain.go).
- **Recovery** = load current state from SQLite (reconciling the catalog with
  runtime-created tables via `_hz_tables`), then replay only segments past the
  drained cursor on top. Boot and crash recovery are the same path. See
  [recover_sqlite.go](../recover_sqlite.go).

Measured (AMD Ryzen AI MAX+ 395, 32 threads, `golang:1.25`):

| | result |
| --- | --- |
| insert, WAL on (born-sealed) | ~330 ns/op — sealing is off the write path |
| drain throughput (one txn/segment, pure-Go SQLite) | ~203k rows/s |

The drain runs faithful per-record replay; coalescing (§4) stays a future lever
if drain throughput ever binds. The ~203k rows/s pure-Go rate vs the ~528k/s
native-C rate measured earlier is the modernc-vs-cgo trade — at the 60s cadence
there is ample headroom either way.

It builds on two pieces that already exist and are tested: the typed-mutation
**WAL** ([wal.go](../wal.go) — each segment is fsynced as it seals, ~every
0.5s or 1 MiB; there is no per-write-fsync mode) and **WAL-replay
recovery** (`Open` → `replayWAL` in
[db.go](../db.go), covered by `recovery_test.go`). It reuses the async-merger's
**dirty-set tracking** (`markDirtyLocked`, `dirty []UUID`, `dirtyCount` in
[store.go](../store.go); the merge loop in [secindex.go](../secindex.go)).

---

## 1. Where things stand

Of the three gaps in the original proposal (verified 2026-05-30), two are now
**closed** by the as-built work above; one remains open:

- **WAL growth — closed.** Born-sealed segments (~0.5s / 1 MiB) plus the SQLite mirror, which
  drains each sealed segment and then **deletes** it, bound the WAL to ~the
  undrained tail. The log no longer grows forever. (`recCheckpoint` was never
  needed: SQLite is the compacted store, so there is no in-WAL checkpoint to take.)
- **Recovery cost — closed.** Boot loads current state from SQLite — O(current
  dataset) — and replays only the undrained tail, not the entire history.
- **Memory budget — still open.** Nothing tracks how many bytes the dataset
  occupies; there is no cap, no admission control, no eviction. A sustained insert
  load grows RAM until the OS OOM-kills the process. This is the one remaining
  piece (§3).

## 2. The model and its invariant

Three layers with one strict rule:

```
   reads / writes
        ↓
  in-memory engine        ← hot serving copy (sharded arenas, secondary indexes)
        ↓ (mutations)
       WAL                 ← born-sealed segments: each fsynced as it seals (~0.5s / 1 MiB)
        ↓ (drain on its interval, ~1s)
  SQLite file (disk)       ← system of record: current state, compacted, portable
```

> **Invariant: in-memory state = SQLite (the drained current state) + the
> undrained WAL tail.** The bulk loads from SQLite; the small tail past the drain
> cursor replays directly into memory.

The ~5s drain cadence bounds how much tail any boot ever replays. Two code paths
carry the design:

- **apply sealed segments → SQLite** (the drainer; bulk compaction), and
- **load SQLite → memory, then replay the undrained tail → memory** (every boot).

The bulk comes from SQLite via `loadTableRows` ([recover_sqlite.go](../recover_sqlite.go));
the in-memory layer still understands the WAL format for the **tail** replay,
applying typed mutations through `rt.insert` (WAL-free — the tail rows are not
re-journaled). Secondary indexes are rebuilt after the load.

Mental model: **in-memory = hot serving copy, SQLite = system of record on disk,
WAL = the buffer batching writes into SQLite and covering the not-yet-drained tail.**

## 3. Memory budget — bounds RAM (the one remaining piece)

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

- **WAL seal+fsync every ~0.5s (or sooner at 1 MiB)** — each segment is fsynced
  as it is sealed. This is the durability window — see §5.
- **SQLite drain on its interval (default 1s)**, one transaction per sealed
  segment. (The original ~60s single-batch cadence was dropped for per-segment
  draining — see *As built*: a drainer that touches only sealed segments needs no
  age gate and never races the open segment.)

### Coalesce by dirty-set, do not replay history — *future lever, not as-built*

The as-built drain replays each segment **faithfully** (one SQLite txn per
segment, INSERT/UPDATE/DELETE in WAL order). The coalescing scheme below is the
next throughput lever **if** the drain ever binds (it does not today: ~203k
rows/s pure-Go, draining ~1s of buffered writes per ~1s interval with wide margin).

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

> **Every boot:** load current state from SQLite into memory, then replay the
> undrained WAL tail (segments past the drain cursor) directly into memory.

1. After a crash, on-disk SQLite holds everything up to the last drained segment;
   the undrained tail segments are still on disk (the drainer deletes a segment
   only after its SQLite transaction commits).
2. **Load SQLite → in-memory shards** ([recover_sqlite.go](../recover_sqlite.go)),
   reconciling runtime-created tables via `_hz_tables`.
3. **Replay the undrained tail segments → memory** through the apply path
   (`rt.insert`; WAL-free, not re-journaled). State is now at the crash point,
   minus the ≤1s the WAL had not fsynced.
4. Rebuild secondary indexes; begin serving.

Recovery is re-runnable by construction: it only *reads* SQLite + the tail and
*writes* memory — it never mutates SQLite or deletes a segment, so a crash
mid-boot just restarts from the same SQLite state and the same tail. The drain's
crash-safety is separate: each segment drains in one SQLite transaction and is
deleted only after commit, so a crash mid-drain rolls that segment back and the
next drain re-applies it.

Clean shutdown flushes a final drain, so the tail is empty and boot is a straight
SQLite load. Crash leaves a few tail segments. **Same path either way** — no
separate "clean boot" vs "crash boot" logic.

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

## 8. Open decisions

Decided (shipped):

- **SQLite driver:** pure-Go `modernc.org/sqlite` shipped (~203k rows/s drain —
  ample margin at the ~5s cadence). cgo `mattn/go-sqlite3` (~528k/s measured)
  stays a drop-in swap if the drain ever binds.
- **Drain trigger:** a background interval (~1s) consuming whatever segments
  have sealed since — not a WAL-size or dirty-count threshold. Segments seal
  independently on the WAL's own 1 MiB / ~0.5s flush.

Still open:

- **One SQLite file vs one per shard.** Per-shard files would let the drain run
  concurrently (one writer each, no cross-shard lock) and sidestep SQLite's
  single-writer limit, at the cost of N files and a fan-in load. Single-file keeps
  up with margin (§4), so this stays deferred until measurement says otherwise.
- **Snapshot-free alternative retained for comparison:** a custom in-memory
  snapshot + WAL-truncate remains viable if the SQLite dependency is ever judged
  too heavy — notably a **WAL-only deployment with no `SQLitePath`**, where today
  the segments are not drained or reclaimed (the one mode that still grows the log
  and replays the full history on boot). Dependency-appetite + portable-file value
  vs. novel code; this note assumes the SQLite route.
