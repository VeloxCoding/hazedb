# hazedb RFC

**Status:** M1+M2 implemented; M3–M8 open. See *Implementation status* for what is running vs designed.  
**Module:** `github.com/VeloxCoding/hazedb`  
**Updated:** 2026-05-29 (rev. 7 — WAL format settled as typed-mutation after benchmarking; see *Review coverage* and *Changelog*)

---

## What it is

An embedded, memory-resident SQL store for single-process Go applications.
All reads come from RAM. Disk stores append-only WAL segments and log-derived snapshots — never table pages or a buffer pool. No network protocol, no separate
process, no buffer pool.

**Target:** latency-sensitive OLTP where the working set fits in RAM — chat
feeds, session state, hot leaderboards, in-process caches. Compile it
directly into a Caddy module, FrankenPHP extension, or standalone Go binary.

---

## Non-goals (load-bearing)

| | |
|---|---|
| Not a PostgreSQL/SQLite replacement | No joins, no window functions, no ad-hoc query performance guarantee |
| Not for data > RAM | WAL + checkpoints only; no page eviction |
| Not multi-process | One Go process owns the DB |
| Not OLAP | No aggregation engine, no columnar storage |
| No runtime schema migration | `go generate && go build && restart` |
| No JOINs in v1 | Multiple Gets in app code; not promised for v2 |
| No migration tooling | Write your own transfer script; store your old PK as a regular column |

---

## Implementation status

The remainder of this RFC describes the **target architecture** — the full design hazedb is being built toward. Not all of it is implemented. This section is the single source of truth on what runs today vs what is planned.

### Running today (M1 + M2 + M3 + M4 foundation)

- Sharded RWMutex storage: `[]Value` typed rows, append-only arena, tombstone deletes, per-shard `map[UUID]uint64` PK index, `uint64` rowID
- **UUIDv7 PK, enforced** — `[16]byte` stored inline in `Value` (no per-cell alloc); INSERT auto-generates a monotonic UUIDv7 when omitted (resolved before the WAL write), or accepts a client UUID; a canonical-string PK is parsed to UUID at the API boundary, never in storage
- Immutable order column (`seq`) support: an `Immutable` column flag rejects `UPDATE SET` at plan time (PK is implicitly immutable) — the stable schema M5's tail index builds against
- **Logical typed-mutation WAL**: versioned self-delimiting envelope (`magic|type|version|length|payload|crc32c`); MUTATION payload is `op|tableID|op-body` (insert=full row, update=pk+changed-column deltas, delete=pk); CRC32C; replay fails loud on bad magic / newer version / unknown type / CRC mismatch on a complete record, tolerates truncated tails
- WAL durability modes (M3): flush ticker (`WALFlushInterval`, 0=1s default, <0=manual), `WALSync` (ticker fsync via a `dirtySinceSync` flag), `WALSyncPerWrite`, sticky WAL error state
- Write pipeline enforces validate → WAL append → apply under the relevant shard lock(s); multi-shard predicate writes hold every shard lock so WAL order == in-memory order
- Runtime SQL interpreter: `SELECT` / `INSERT` / `UPDATE` / `DELETE` with arithmetic in `SET` and `WHERE` (`col +/-/* ?`)
- Statement cache (`sync.Map`) and PK fast path (`WHERE id = ?` → pk-map lookup, one shard)
- The *Measured benchmarks* table predates M3/M4. M4 perf cost recorded in `bench/baseline_m4.txt`: the 16-byte UUID in every `Value` cell adds ~10-22% ns and +50-100 B/op vs M3; allocs flat-or-better. Reclaimed later by the codegen typed-struct path.

### Designed, not yet implemented (M5+)

| Feature | Milestone |
|---|---|
| PartitionKey shard routing + table-wide `pkDirectory` + per-partition tail index | M5 |
| Compiled query API: `go generate` → `*Queries` typed methods + strict mode | M5 |
| `db.Transaction()` Go closure API + WAL `TXN` envelope grouping | M6 |
| WAL segments + snapshot checkpoint | M7 |
| FrankenPHP cgo binding (`hazedb_exec_transaction`, named transaction codegen) | M8 |

---

## Store foundation

### Sharded RWMutex over generic rows (typed generated structs planned)

```
shards = runtime.NumCPU() * 4   (floor 64, cap 1024, rounded to power-of-two)
```

The shard count is computed once at `Open()` and fixed for the process lifetime (power-of-two so routing is a mask, not a modulo). It is **runtime-derived, not persisted**: nothing shard-specific is written to the WAL or snapshot (both are logical — typed mutations / row dumps, never shard ids), so a WAL/snapshot written on a 32-core box (128 shards) replays correctly on an 8-core box (64 shards). Every row simply re-routes under the new count, and since PK lookups, `pkDirectory`, and tail indexes all derive placement from the same live count, the result is internally consistent — only the physical layout differs. The single hard requirement is that routing be identical for live writes, replay, and snapshot-load within one process; do not cache a routing result computed under a different count.

Two table shapes exist; shard routing and PK uniqueness enforcement differ between them.

**Non-partitioned table** (default — no `PartitionKey` declared):

```go
type tableShard struct {
    mu   sync.RWMutex
    rows []Row             // append-only arena; tombstones for deletes
    pk   map[UUID]uint64   // PK → rowID; shard determined by FNV-1a(PK)
    live int
}
```

PK uniqueness and `WHERE id = ?` are fully local to the shard — one lock, O(1).

> **Note:** the target keys this map by `UUID` (a fixed `[16]byte`), matching the partitioned `pkDirectory`'s `map[UUID]rowLocation`. A `[16]byte` is a comparable array usable as a map key with no allocation. The current M1+M2 code keys by `string` (integer/string PK, UUIDv7 not yet enforced — see *Implementation status*); that costs a string allocation + string hash per lookup and is inconsistent with the partitioned path. Switch to `map[UUID]uint64` when UUIDv7 enforcement lands (M3).

**Partitioned table** (`PartitionKey` declared):

```go
type tableShard struct {
    mu    sync.RWMutex
    rows  []Row                          // rows for ALL partition values that hash to this shard
    tails map[PartitionValue]*partitionIndex  // one ordered tail index per partition value
    live  int
}

// One pkDirectory per partitioned table — not per shard.
type pkDirectory struct {
    mu  sync.RWMutex
    idx map[UUID]rowLocation
}

type rowLocation struct {
    shard uint16
    rowID uint64
}
```

**A shard is not a partition value.** Routing is `FNV-1a(PartitionKey value) % shards`, and there are far fewer shards (64–1024) than distinct partition values, so multiple partition values necessarily collide into the same shard. The guarantee is one-directional: all rows for a *given* partition value land in *one* shard — but that shard's arena holds rows for every partition value that hashes to it. Therefore the ordered tail index **must be namespaced per partition value** (`map[PartitionValue]*partitionIndex` above), not a single per-shard index. A single per-shard index would interleave rows from unrelated partition values into one `seqs`/`rowIDs` order, so a tail scan for conversation P would return conversation Q's messages whenever P and Q collide. The rowIDs in each per-partition index still point into the shard's single shared arena (rowIDs are unique within the shard), so no `(shardID, rowID)` pairs are needed.

The per-shard `pk` map is absent for partitioned tables. PK uniqueness and `WHERE id = ?` go through the table-wide `pkDirectory`. INSERT: acquire pkDirectory lock → reject duplicate → acquire partition shard lock → write row → record location in pkDirectory → release both. `WHERE id = ?`: pkDirectory lookup → rowLocation → shard read. Two lock acquisitions instead of one; both O(1).

**Read-path TOCTOU — the shard read must re-validate liveness.** If the pkDirectory lock is released before the shard lock is taken (the concurrency-favouring choice), a concurrent `DELETE` can tombstone the row between the lookup and the read:

```
reader:  pkDirectory → {shard 5, rowID 100}, release pkDirectory lock
deleter: pkDirectory lock (remove entry); shard 5 lock (tombstone rowID 100)
reader:  shard 5 lock → reads rowID 100 → finds a tombstone
```

**Correction to an earlier draft (this was wrong before).** A previous revision said the read path should treat "rowLocation points at a tombstone / PK mismatch" as **not-found**. That is itself a bug. Consider a `DELETE` + `INSERT` of the *same* PK committed atomically in one transaction — exactly the PartitionKey-move pattern (`DELETE` + `INSERT` under a transaction, per *Immutability*). The transaction tombstones the old location, removes the old `pkDirectory` entry, writes the new row, and records the **new** location in the directory — all atomically. A reader that captured the *old* location before the transaction, then reads the shard after the transaction commits, sees a tombstone at the old rowID. Returning not-found is a phantom disappearance: the PK existed before the transaction and exists after it (at the new location); it was never absent. The row only "vanishes" because the reader is holding a stale location.

**The correct rule: on tombstone or PK-mismatch at the resolved location, re-do the `pkDirectory` lookup; do not return not-found from a stale location.** The retry observes the directory's current state: either the entry is gone (genuine delete → now correctly not-found) or it points to the new location (move → read the new row). Because rowIDs are append-only and never reused before a snapshot restart, there is no ABA hazard, so a single retry suffices in the common case; bound the retries to avoid a pathological move-storm loop. The alternative remains holding the `pkDirectory` read lock across the shard read — no TOCTOU at all, since the move (which needs the directory write lock for both the entry-removal and the entry-add) cannot interleave, but every point-read then serialises against every delete/move on that table. Pick one explicitly; the recommended default is release-then-**retry** (not release-then-return-not-found).

**The `pkDirectory` is not deferred.** Without it, two different partitions can hold the same UUID undetected (each shard sees no local duplicate), and `WHERE id = ?` has no deterministic shard to check. PartitionKey tables are semantically broken without a table-wide PK directory — it is a hard prerequisite, not an optimisation.

**Key design choices:**

- **PK is always UUIDv7** — see *Primary key* section above.
- **`[]Value` tagged union in memory** — no binary-encoded rows, no deserialization on the read path. A `Row` is `[]Value` where `Value` carries `Kind` (Int/String/Bytes/Bool/Null). Codegen (M4) moves the hot path to typed Go structs per table; `[]Value` stays for the interpreter fallback.
- **Shard routing:**
  - **No `PartitionKey`** — shard by FNV-1a(PK). `WHERE id = ?` → one shard, one lock. Use for lookup-heavy tables (users, sessions).
  - **`PartitionKey` declared** — shard by FNV-1a(PartitionKey value). All rows for the same partition value land in one shard, but that shard also holds other partition values that hash to it, so the tail index is namespaced per partition value (`map[PartitionValue]*partitionIndex`) with rowIDs into the shard's shared arena. `WHERE id = ?` → pkDirectory → rowLocation → shard, two locks. Use for append/scan-heavy tables (messages, events, logs).
- **Immutability — enforced at plan time:**
  - The PK column (`id`) is never a valid target of `UPDATE SET` — rejected at plan time.
  - The `PartitionKey` column is never a valid target of `UPDATE SET` — rejected at plan time. Moving a row to a different partition requires `DELETE` + `INSERT` under a transaction.
  - **The tail-index order column is also immutable** on partitioned tables that declare one — rejected at plan time. The `partitionIndex` caches each row's order value in `seqs` parallel to `rowIDs`; an `UPDATE messages SET seq = ?` would change the row's stored value while leaving `seqs` stale, silently corrupting tail-scan order. If a mutable order column is ever required, the index must be maintained on update (find the entry, re-position it — an O(N) slice-shift, the same cost as an out-of-order insert), not just the row. v1 takes the simpler immutable-order-column route: the ordering value is set at insert and never changed; a new order requires `DELETE` + `INSERT`.
- **Tombstone on delete** — `rows[i] = nil`; for non-partitioned tables the local pk map entry is removed; for partitioned tables the pkDirectory entry is removed. RowIDs stay stable so the tail index does not need updating. The arena never shrinks — tombstone slots accumulate until snapshot restart (M7).
  - **Churn caveat (load-bearing for the stated use cases).** Two of the target workloads — session state and in-process caches — are high-eviction by nature. Because nothing reclaims tombstone slots before a snapshot/restart (M7, the *last* milestone), a delete-heavy table grows memory monotonically, and `partitionIndex` tail-scans with `LIMIT n` degrade over time: dead entries stay in the index (rowIDs are kept stable on delete), so a scan must skip accumulating tombstones to reach `n` live rows, turning an O(n) scan into O(n + tombstones). For insert+expire workloads this is unbounded growth and slowly rising tail latency between restarts. If those use cases are real targets, pull a minimal in-place arena/index compaction earlier than M7, or document the restart cadence required to bound memory.

### Lock ordering — global invariant

Every operation that acquires more than one lock must acquire them in this fixed order:

```
pkDirectory  (per partitioned table involved, ascending table index)
→ data shards  (lexicographic by (table index, shard index))
→ walMu
```

Violating this order causes deadlock. The canonical failure mode without this rule:

- regular partitioned write holds pkDirectory, waits for shard
- concurrent transaction holds shard, waits for pkDirectory → neither can proceed

The shard order is **lexicographic `(table index, shard index)`**, not shard index alone. Within a single table shard indices are unique, so for single-table operations (everything through M5) this reduces to plain ascending shard index. But the moment a transaction spans two tables that both have, say, shard index 3, "ascending shard index" is ambiguous: one transaction could grab `(A.3, B.3)` while another grabs `(B.3, A.3)` → deadlock. The table-index-first tie-break removes this. (Relevant once cross-table transactions land — M6 / v1.1 — but the invariant is stated globally, so it must be unambiguous now.)

**Rules that follow from this order:**

- Regular writes on partitioned tables: pkDirectory lock → shard lock(s). Never the reverse.
- Transactions that touch one or more partitioned tables: acquire all involved pkDirectories in ascending table index order, then all involved shards in lexicographic `(table index, shard index)` order, then WAL. Never acquire a shard before a pkDirectory for the same table.
- Non-partitioned tables have no pkDirectory, so they follow: shard lock(s) → walMu.
- Table schema is read-only after `Open()`. If future runtime schema changes are added, a schema lock must be acquired before all of the above.
- **The M7 checkpoint write-barrier sits at the very top of this order.** The consistent-cut checkpoint (see *Roadmap → M7*) pauses all writes via a global barrier; model it as an RWMutex where every write path takes the barrier in *read* mode before acquiring any pkDirectory/shard/WAL lock, and the checkpoint takes it in *write* mode. Because it is acquired before everything else, it cannot deadlock against the write path. Omitting it from the documented order is how a "pause all writes" barrier silently becomes a lock-order violation once it is implemented.

### Primary key — UUIDv7, enforced

Every table has exactly one primary key column. Its type is always UUIDv7 — a 128-bit, time-ordered UUID. This is not configurable.

**Why enforce it:**

- **Client-side generation** — the caller generates the ID before the insert; no roundtrip to hazedb for a sequence number
- **`ORDER BY id` ≈ temporal order at millisecond granularity** — UUIDv7's high 48 bits are a unix-ms timestamp, so IDs sort by creation time *across* milliseconds. **Within a single millisecond the order is not guaranteed** unless the generator implements RFC 9562 monotonicity (a sub-ms counter in `rand_a`); a plain random-fill UUIDv7 sorts randomly inside the same ms. For strict feed order, either mandate a monotonic UUIDv7 generator or order by an explicit sequence column — which is exactly what the tail index's `seqs` provides. Do not rely on `ORDER BY id` alone for exact within-ms ordering.
- **WAL merge is collision-safe in practice** — UUIDv7 carries 74 bits of randomness; the birthday-bound collision probability across billions of IDs is negligible. We accept this residual theoretical risk in exchange for coordination-free client-side generation
- **Codegen is simpler** — generated functions always know the PK type at compile time; no `any`, no type switch

**Auto-generation:** if the INSERT omits the PK column, hazedb generates a UUIDv7. If the caller supplies one, hazedb accepts it as-is. In both cases the concrete UUID is written to the WAL record before execution — the WAL never contains a bare INSERT without an explicit PK column, because replay must regenerate the exact same row under the exact same ID.

**Migration from an existing database with a different PK scheme:** write a transfer script, insert rows into hazedb (which generates new UUIDv7 PKs), and store the original key as a regular column (e.g. `external_id`). hazedb provides no migration tooling — this is intentionally the caller's responsibility.

### Ordered index (tail-scan path)

Only valid on tables with a `PartitionKey` declared. There is **one `partitionIndex` per partition value**, held in the owning shard's `tails map[PartitionValue]*partitionIndex` (see *Partitioned table* above). Because all rows for a given partition value live in one shard, that index's `rowIDs` point unambiguously into that shard's arena — no `(shardID, rowID)` pairs needed. A scan resolves `PartitionKey value → shard → tails[value]` and walks only that partition value's `seqs`/`rowIDs`, so rows from other partition values that happen to share the shard are never mixed in.

```go
type partitionIndex struct {
    seqs   []int64   // ordered by this column's value, for ONE partition value
    rowIDs []uint64  // parallel pointers into the owning shard's shared arena
}
```

Monotone-append (chat/log) is O(1). Out-of-order is O(N) slice-shift.

**rowID is `uint64`, not `uint32` — overflow is a real hazard otherwise.** RowIDs are monotonic indices into an append-only arena that includes tombstone slots and never shrinks before a snapshot/restart (M7, the last milestone). A `uint32` caps a shard at ~4.29 billion slots *ever allocated*, tombstones included. A hot or skewed shard under high insert/churn can reach that within a single long-running process — at the benchmarked 690k inserts/s concentrated on one shard, in well under two hours — and `uint32` wraparound is silent: a reused rowID aliases a different live row, so reads and updates corrupt unrelated data with no error. `uint64` removes the practical ceiling (the arena hits RAM limits long first). If `uint32` is kept for memory reasons, the allocator **must** hard-detect approaching `MaxUint32` and force a compaction/snapshot-restart before wraparound rather than silently wrapping.

### WAL — format (logical typed-mutation)

hazedb uses a **logical typed-mutation WAL**: each record stores the resolved *operation* — op kind, target table, and the concrete typed parameters that were applied — not the SQL text and not physical page/row-image bytes. It is *logical* in that replay re-applies the mutation through the store's apply path (so it survives storage-layout changes and carries transaction grouping + checkpoint markers), but it is **not** the SQL-string form used by Redis AOF or statement-based binlog.

> **The SQL-string form was benchmarked against this and rejected** (spike preserved in [`wal_format_spike_test.go`](wal_format_spike_test.go)). Carrying the SQL text per record cost **+50% bytes on insert, +37% on a narrow update, +69% on a wide update, 2.5× on delete**, and **~2× replay time with more allocations** (every record re-runs prepare + the eval pipeline). Its only plausible advantage — a human/replication-friendly log — is largely illusory: the envelope is binary-framed with CRC regardless, and consumers already need the exact schema + UUID + transaction semantics to replay it. Typed-mutation keeps every architectural benefit of "logical" without the parser tax. See *Settled decisions* and the changelog (rev. 7).

**Record envelope.** A single mutation body is not enough: the WAL must also carry grouped transactions (multiple mutations committed atomically) and checkpoint markers, and recovery must be able to tell records apart. Every record is therefore wrapped in a typed, versioned, self-delimiting envelope. **All multi-byte integers are little-endian, fixed.**

```
Envelope: magic:2 | type:1 | version:1 | length:4 | payload:length | crc32c:4
          // crc32c (Castagnoli) computed over magic|type|version|length|payload
          // type: 1=MUTATION  2=TXN  3=CHECKPOINT
          // length bounds-checked against bytes-remaining before payload is read

MUTATION payload:   op:1 | tableID:2 | op-body
  INSERT op-body:   row            (numCols:2, then a typed cell per column)
  UPDATE op-body:   pk-cell | nsets:1 | (col_ordinal:2 | typed cell) × nsets
  DELETE op-body:   pk-cell
TXN payload:        stmt_count:4 | MUTATION-payload × stmt_count
CHECKPOINT payload: snapshot_path_len:2 | snapshot_path | lsn:8

typed cell:         kind:1 | payload
  Int / Bool:       value:8
  String / Bytes:   len:4 | bytes
  Null:             (kind byte only)
```

The asymmetry between op-bodies is the whole point of the format: INSERT carries the full row (it must), but UPDATE carries only the PK plus the changed columns (not the whole row, as a physical row-image would), which is where the measured size win comes from — a one-column edit on a wide row is 51B here vs 218B for a full row-image.

**Unknown version/type must fail loud, never skip.** (Correcting an earlier draft that said recovery should "skip records it doesn't understand" — that is a silent data-loss bug for a *data* WAL: skipping an unrecognised data record drops a committed mutation and diverges from the true state.) On replay, an envelope whose `version` is newer than the binary understands, or whose `type` is unknown, is a hard error that aborts `Open()` — the operator must use a compatible binary. Skipping is only ever acceptable for record types explicitly defined as optional/advisory (none today; `CHECKPOINT` is recognised-and-skipped only because its effect is already captured by loading the snapshot, not because it is ignorable). This is distinct from *tail* truncation, where a torn/CRC-failing record at the very end is the incomplete tail and is correctly discarded.

Parameters are serialised as typed values (UUIDv7, int64, string, bool, bytes, null) — **always the concrete, resolved values that were actually applied**, never the caller's original unresolved arguments. Auto-generated values (the UUIDv7 PK when the caller omits it, any server-side defaults) are resolved before the WAL record is written.

**Atomicity comes from the envelope, not a separate COMMIT token.** A TXN record is one self-delimiting envelope holding all of the transaction's statements; it is durable iff the whole envelope is present with a valid CRC. A torn or truncated TXN envelope (interrupted mid-write) fails the CRC / length check during tail recovery and is discarded in its entirety — there is no such thing as a half-applied transaction in the WAL. This replaces the earlier "pairs followed by a `COMMIT` marker" framing: the commit boundary is the envelope boundary.

**Execution pipeline for every write (mandatory order — follows global lock ordering):**
1. Resolve all auto-generated values (generate UUIDv7 PK if absent, apply server-side defaults)
2. Determine affected pkDirectory (if table is partitioned) and data shards
3. Acquire pkDirectory write lock (partitioned tables only)
4. Lock all affected data shards in ascending shard index order
5. Validate (PK uniqueness, type checks, any constraints)
6. Append WAL envelope — while holding all locks; **if the append (`bw.Write`) returns an error, abort here, enter the WAL error state, and do not proceed to step 7**
7. Apply mutation to in-memory store (only reached if the append succeeded)
8. Unlock: release shard locks, then pkDirectory lock

**Why locking before WAL write is critical:** without it, two concurrent writers can append their WAL records in one order but have the OS scheduler apply them to RAM in a different order. WAL and RAM diverge. Holding the lock across both step 5 and step 6 ensures WAL order and in-memory application order are identical by construction — the only way to write the WAL record is while you hold the lock that serialises the memory mutation.

**Multi-shard writes:** when the WHERE clause can touch more than one shard (i.e., no PK or PartitionKey constraint that pins a single shard), all affected shards must be locked before the WAL write. For arbitrary WHERE with an unknown shard set, the two safe choices are: lock all table shards simultaneously (guaranteed correct, potential contention spike), or require the caller to wrap the operation in an explicit `db.Transaction()`. The one-shard-at-a-time alternative is unsafe (non-serialisable writes + replay divergence) — see *Transactions → The problem* and *Settled decisions → Multi-shard non-PK writes*.

A bare `INSERT INTO messages (body) VALUES (?)` does not journal any SQL — the WAL record is the resolved INSERT *mutation*: op=INSERT, the table id, and the full row including the generated UUIDv7 PK. The PK is resolved (generated if the caller omitted it) before the record is written, so replay reproduces the exact same row under the exact same id.

A grouped transaction is one `TXN` envelope containing `stmt_count` MUTATION payloads, applied in order on replay. A `TXN` envelope that fails its CRC/length check (torn write) is discarded whole.

**Why typed-mutation — not physical row-image, not SQL-string:**

| | Physical row-image | SQL-string logical | Typed-mutation (chosen) |
|---|---|---|---|
| Write size (insert / wide update / delete) | 127B / 218B / 24B | 190B / 86B / 60B | **127B / 51B / 24B** |
| Update payload | full row every time | SQL + pk + changed params | **pk + changed columns only** |
| Replay cost | direct apply (fast) | parse + eval pipeline (~2× slower, +allocs) | **direct apply (fast)** |
| Encode | per-type cell codec | per-type cell codec + SQL copy | **per-type cell codec** |
| Human readable | No | No — binary-framed with typed params + CRC anyway | No — `hazedb dump` reconstructs SQL |
| Cross-version safe | No — breaks on storage format change | survives storage format changes | survives storage format changes (replay through apply path); breaks on schema changes |
| Sync / replication | Hard | consumer needs schema + UUID + txn semantics | same — consumer needs schema + codec + UUID + txn semantics |

Physical and typed-mutation are byte-identical for insert and delete; the difference is the update payload (delta vs full row) and that typed-mutation replays *logically* (through the apply path), enabling snapshots, TXN grouping, and checkpoint markers. SQL-string lost on the two dimensions that matter — write size and replay — without a real simplicity win (see the spike note above).

**Replay:** apply each typed mutation against the store in order, through the apply path — no SQL parse, no re-validation (the values were validated before they were journaled). A mutation in the WAL either applies completely or was never written — no partial row state possible.

**Tail-recovery robustness — validate lengths before trusting them.** Both the envelope `length` and the inner cell lengths (a row's `numCols`, each string/bytes `len`) are unauthenticated integers that must be read *before* the CRC can be verified (you must read `length` bytes to reach the CRC). A crash-truncated or corrupt final record can therefore carry a bogus length. Recovery must bounds-check the envelope `length` against the bytes remaining in the file before reading the payload, and likewise bound each inner cell length against the payload size — otherwise a corrupt tail length causes an over-allocation (OOM) or an out-of-range read/panic. A record whose declared length exceeds what remains, whose `magic` is wrong, or whose CRC fails, is the truncated tail: stop there and truncate the WAL to the last good record. CRC alone does not protect against this, since it sits *after* the length-driven read.

**`hazedb dump <wal-file>`** reconstructs each typed mutation into readable SQL for inspection. Because the WAL stores typed mutations rather than SQL text, this is a small reconstruction step (op + table + params → SQL), not a raw passthrough.

### WAL — durability

`bw.Flush()` calls the OS `write()` syscall, moving data from the Go bufio buffer into the **kernel's page cache**. Without `File.Sync()`, flushed WAL records generally survive a process crash, but are not guaranteed durable across machine restart, kernel panic, filesystem error, or storage failure.

Four practical modes:

| Mode | Process-crash loss window | Power-loss guarantee |
|---|---|---|
| buffered only (`WALFlushInterval < 0`) | until next manual `FlushWAL()` | none |
| **flush every N s** *(default, `WALFlushInterval: 1s`)* | ≤ flush interval | none |
| flush + sync every N s (`WALSync: true`) | ≤ flush interval | ≤ flush interval |
| flush + sync per write (`WALSyncPerWrite: true`) | none | strongest — flush then fsync after every WAL write, under WAL lock |

The ticker-based fsync is amortised — one `f.Sync()` per ticker fire regardless of write volume. At 1 s interval and 690 k/s inserts that is one fsync per 690 k records.

`WALSyncPerWrite` calls `bw.Flush()` then `f.Sync()` after every individual WAL write, both under the same WAL lock. The sequence — write record → flush buffer to OS → sync OS to stable storage — must be atomic with respect to other WAL writers; releasing the lock between Flush and Sync would allow another writer to interleave and leave unsynced data. Error handling is required on both calls: a Flush or Sync failure must be treated as a fatal WAL error (see *WAL error handling* below). This is the only mode with no acknowledged-loss window; it also has the highest per-operation cost and is appropriate when callers need the strongest durability guarantee at the expense of write throughput.

Configured via `Options`:

```go
WALFlushInterval time.Duration  // 0 = safe 1s default; <0 = manual FlushWAL() only
WALSync          bool           // flush then fsync after each ticker fire; default false
WALSyncPerWrite  bool           // flush then fsync after every individual WAL write; default false
```

A background goroutine started in `Open()` wakes every `WALFlushInterval` and, **holding `walMu` for the whole sequence**, flushes and (when `WALSync` is set) syncs. The lock is mandatory, not incidental: `bufio.Writer` is not safe for concurrent use, and writers append records via `bw.Write` under `walMu`, so the ticker must take the same lock before it touches `bw` or `f` — otherwise a concurrent append and flush race on the buffer's internal state and corrupt the WAL. When `WALSyncPerWrite` is set, `bw.Flush()` followed by `f.Sync()` is called inline after each WAL write under the WAL lock, independently of the ticker. The goroutine exits when the DB is closed.

**The sync decision uses a `dirtySinceSync` flag, not `bw.Buffered()`.** It is tempting to skip work when `bw.Buffered() == 0`, but that is wrong for the fsync decision: `bufio.Writer` auto-flushes to the underlying fd whenever its buffer fills, so a large or bursty write can have already pushed data into the kernel page cache while leaving `Buffered() == 0`. If the ticker keyed `f.Sync()` off `Buffered() > 0`, that auto-flushed data would never be synced until some later write happened to leave the buffer non-empty at a tick — so after a quiet period the newest records sit unsynced indefinitely, breaking the "≤ flush interval" power-loss guarantee of `WALSync` mode. Track a `dirtySinceSync bool` set on every `bw.Write` (and on any auto-flush) and cleared only after a successful `f.Sync()`. The ticker flushes if the buffer is non-empty, and syncs if `dirtySinceSync` — independent conditions.

**Interval semantics (implemented).** `WALFlushInterval == 0` selects the safe **1s default** (a zero-value `Options` should not silently disable durability flushing); a **negative** value is manual-only (`FlushWAL()` is the only flush path) and starts no goroutine. `startTicker` only spawns the goroutine for a strictly positive interval, so `time.NewTicker` never sees a non-positive value. (This resolves the earlier draft's contradiction between "default 1s" and "0 = manual": 0 is the default, negative is manual.) The ticker is started **after** WAL replay so it never races the replay reader on the shared file handle, and `close()` is idempotent (`sync.Once`).

**WAL error handling:** if `bw.Write` (the record append), `bw.Flush()`, or `f.Sync()` returns an error — whether from the execution pipeline, the background goroutine, or an inline `WALSyncPerWrite` call — the DB must enter a permanent error state. **The append error matters as much as flush/sync:** `bw.Write` can fail (notably when it triggers an auto-flush of a full buffer whose underlying write to the fd fails), and step 6 of the execution pipeline must check it. If the WAL append fails, the pipeline must abort *before* step 7 — the in-memory mutation is never applied — otherwise RAM holds a change that is not (and may never be) in the WAL, diverging from any replay. In the error state all subsequent write calls return the WAL error immediately without touching in-memory state. Read-only queries may continue (subject to the usual async loss window: already-applied-but-unsynced writes remain visible and will be lost on restart if they were never synced). The error state is not recoverable without closing and reopening the DB (which triggers WAL replay from the last successfully flushed record).

### SQL interpreter (M1+M2 complete)

Parse → plan → execute path:

```
parseSQL(sql) → assignParamIndices → plan() → execSelect/execInsert/...
```

**Statement cache** (`sync.Map` keyed by SQL string) eliminates parse+plan on repeated calls. **It is unbounded.** This is safe only as long as the key space is bounded — i.e. callers parameterise with `?` so the cache key is the query *shape*, not the data. Any path that inlines literal values into the SQL string (`... WHERE id = 'abc-123'`) produces a fresh key per value and grows the map without limit — a quiet memory leak / DoS vector. Enforce "always parameterise" at the API boundary, or bound the cache (LRU) if ad-hoc literal SQL must be allowed.  
**PK fast path** — `WHERE id = ?` is detected at plan time. Non-partitioned tables: FNV-1a(id) → shard → local pk map, one lock, O(1). Partitioned tables: pkDirectory lookup → rowLocation → shard, two locks, O(1). No scan in either case.

Supported today:

```sql
SELECT col_list FROM table [WHERE expr] [ORDER BY col [DESC]] [LIMIT n]
INSERT INTO table (cols) VALUES (vals)
UPDATE table SET col = val [WHERE expr]
DELETE FROM table [WHERE expr]
```

WHERE supports: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`, `?` params, literals (int/string/bool/null).

**Read consistency of multi-shard SELECT (explicit).** A `SELECT` pinned to a single shard — `WHERE id = ?` on a non-partitioned table, or any scan confined to one partition value on a partitioned table — reads under that shard's read lock and is a consistent point-in-time view. A `SELECT` whose `WHERE`/`ORDER BY`/`LIMIT` spans multiple shards (no PK/PartitionKey pin) does **not** read all shards under a single lock by default: it takes and releases each shard's read lock in turn, so concurrent writes between shard reads mean the assembled result can reflect a mix of moments and may represent no single instant that ever existed. This is the read-side counterpart of the multi-shard write rule. The contract is: **per-shard consistent, not globally point-in-time.** Callers needing a consistent cross-shard snapshot must either pin the query to one shard, or use the consistent path — read-lock all involved shards for the duration of the scan (correct, but an all-shard read-lock contention spike, same tradeoff as multi-shard writes). `ORDER BY` + `LIMIT` over a multi-shard scan inherits this: it merges per-shard results that were each consistent only at their own read instant. **And it must gather-then-sort: `LIMIT n` cannot be pushed down to each shard.** A correct multi-shard `ORDER BY col LIMIT n` collects all matching rows (or at least the top-n per shard) from every involved shard, merges, sorts globally, then applies `LIMIT n`. Taking `n` rows *per shard* and concatenating gives wrong results; even taking the per-shard top-n is only valid as an optimisation if each shard is individually sorted on `col` first. Document which mode a given query uses; do not assume snapshot semantics for unpinned scans.

**Not yet supported:** arithmetic expressions in `SET` clauses (`balance = balance - ?`). The right-hand side of every `SET` assignment must be a literal or `?` parameter — the caller computes derived values before passing them. Arithmetic in SET is planned for M3.  Without it, in-transaction value updates require a prior read to compute the new value before the transaction opens.

---

## Query API and codegen model

SQL is the **definition language** for typed queries. The hot path never parses SQL at runtime — `go generate` compiles every declared SQL query into a typed Go method before the binary is built.

### Two tiers

**Tier 1 — generic runtime path** (interpreter, `[][]Value` returns, always available):

```go
rows, err := db.Query("SELECT body FROM messages WHERE id = ?", id)
// rows is [][]Value — untyped, works for any valid SQL string

n, err := db.Exec("INSERT INTO messages (id, body) VALUES (?, ?)", id, "hello")
// n is rows affected
```

Useful for ad-hoc queries, tooling, `hazedb dump`, and tests. Not the hot path.

**Tier 2 — generated typed path** (no SQL at runtime, typed struct returns):

```go
// generated by go generate:
type MessageBodyRow struct{ Body string }

func (q *Queries) SelectBodyByMessageID(id UUID) ([]MessageBodyRow, error)
func (q *Queries) InsertMessage(id UUID, body string) (int64, error)

// call site — fully typed, no Value union, no type assertions:
rows, err := q.SelectBodyByMessageID(id)
// rows[0].Body == "hello"
```

`go generate` reads `.sql` query files (one named query per file), validates each against the current schema, and emits a `*Queries` struct with one method per query. Each method calls directly into the store — no dispatch table, no hash lookup, no SQL parsing at runtime.

### Why not a single runtime-dispatch function

A single `hazedb_query(sql string, params ...any)` function cannot return different typed structs depending on the runtime value of `sql`. A dispatch map `map[uint64]fn` can only hold functions with one fixed signature — forcing the return type to `any` or `[][]Value`, which discards all type safety. Named generated methods are the correct model: each method has its own exact return type known at compile time.

This is the sqlc model: SQL files are the source; generated named methods are the API; no SQL string appears at runtime on the hot path.

### What go generate produces

Given a query file:

```sql
-- name: SelectBodyByMessageID
SELECT body FROM messages WHERE id = ?
```

`go generate` emits:

```go
type MessageBodyRow struct{ Body string }

func (q *Queries) SelectBodyByMessageID(id UUID) ([]MessageBodyRow, error) {
    _, rows, err := q.db.execSelect(selectBodyByMessageIDPlan, id)
    if err != nil {
        return nil, err
    }
    out := make([]MessageBodyRow, len(rows))
    for i, r := range rows {
        out[i] = MessageBodyRow{Body: r[0].S}
    }
    return out, nil
}
```

`selectBodyByMessageIDPlan` is a pre-compiled, reusable query plan — the same structure the statement cache produces, but pinned at compile time and never re-parsed.

### Fallback and strict mode

- **Declared queries** (`.sql` files processed by `go generate`) → `*Queries` typed methods, hot path
- **Ad-hoc SQL** passed to `db.Query()` / `db.Exec()` → interpreter fallback, `[][]Value` return
- **`StrictMode bool` in `Options`** → `db.Query()` / `db.Exec()` return an error for any SQL not backed by a generated method; guarantees no interpreter is ever called in production

### Role of the SQL interpreter

The interpreter (lexer → parser → planner → executor) serves two purposes: it is the `go generate` backend (validates SQL at codegen time and emits plans), and it is the runtime engine for the generic `db.Query()` / `db.Exec()` fallback path. In strict mode the executor is never called at runtime. The `[]Value` / `Row` types are internal to the interpreter and the `hazedb dump` CLI — not on the hot path.

### FrankenPHP / cgo boundary

Two functions exposed through cgo:

```php
// Generated per-query wrapper — typed params, fast, one cgo crossing
hazedb_select_body_by_message_id($id)

// Generic fallback — JSON-encoded rows, slower, ad-hoc use only
hazedb_sql("SELECT body FROM messages WHERE id = ?", $id)
```

`go generate` emits both the typed Go method and a matching cgo-compatible C wrapper for each declared query. `hazedb_sql` maps to `db.Query()` / `db.Exec()` and returns JSON-encoded rows — useful for admin tooling, not the hot path.

---

## Transactions and atomicity

### The problem

`UPDATE ... WHERE expr` and `DELETE ... WHERE expr` can touch rows across multiple shards. The naive implementation locks and writes one shard at a time:

```
lock shard 0 → mutate matching rows → unlock
lock shard 1 → mutate matching rows → unlock  ← UNSAFE: see below
...
```

**This pattern is not just a torn-read problem — it is a write-serializability and replay-divergence bug, and it must not be used.** The lock-before-WAL-write invariant (*WAL — format*, step 6) guarantees "WAL order = in-memory apply order by construction" **only when the single lock that serialises the mutation is held across the WAL append**. That holds for single-shard / PK-pinned writes. It does *not* hold once a statement spans shards and releases each shard lock before taking the next.

Concretely, two concurrent multi-shard statements S1 and S2, both touching shards A and B and both writing some row on each:

```
S1 locks A, applies, unlocks A
S2 locks A, applies, unlocks A      // on A: S1 then S2  → A's row = S2
S2 locks B, applies, unlocks B
S1 locks B, applies, unlocks B      // on B: S2 then S1  → B's row = S1
```

Live memory ends at (A=S2, B=S1) — a state with no single serial order. The WAL is a total order via `walMu` (say S1 then S2), so replay applies S1 fully, then S2 fully, ending at (A=S2, **B=S2**). **Post-crash replay diverges from pre-crash memory**, directly violating the "identical by construction" invariant. Even with no crash, the live state is non-serialisable.

The fix is to hold **all** affected shard locks (in ascending shard-index order — see *Lock ordering*) across the single WAL append and all applies, exactly as `db.Transaction()` does. There are therefore only two safe ways to run a multi-shard, non-PK-pinned write:

1. Lock all affected shards simultaneously before the WAL write (guaranteed correct; possible all-shard contention spike), or
2. Require the caller to wrap it in `db.Transaction()`.

The one-shard-at-a-time pattern above is neither and is a bug. See *Settled decisions → Multi-shard non-PK writes* — this is **closed by correctness**, not an open tradeoff: the status quo third option is unsafe and one of the two safe options is mandatory. Until M5 lands `db.Transaction()`, multi-shard non-PK `UPDATE`/`DELETE` must take the lock-all-shards path (or be rejected at plan time).

**Crash safety (PK-pinned and single-shard writes)** is solved by the logical WAL combined with the lock-before-WAL-write ordering: the resolved statement is appended to the WAL buffer while holding the shard lock that serialises the mutation, and only then applied to memory (still under lock). For these writes WAL order and in-memory application order are identical by construction. Crash mid-execution → the statement is either fully in the WAL (replay re-executes it deterministically) or not in the WAL at all (nothing to replay) — no partial row state is possible. For multi-shard writes the same guarantee holds **only** under option 1 or 2 above.

**What is atomic today:** PK-based operations (`WHERE id = ?`) on non-partitioned tables hit exactly one shard under that shard's lock — fully atomic. On partitioned tables, `WHERE id = ?` acquires pkDirectory lock then shard lock in that fixed order — also fully atomic (no other operation acquires them in reverse order).

**What is not atomic:** multi-shard `WHERE` operations not run under option 1 or 2 (non-serialisable writes *and* torn reads — must not be used), and any sequence of statements that must succeed or fail together.

### Design decision — explicit opt-in

Non-transactional operations pay zero overhead. Atomicity is explicit opt-in. No implicit transaction wrapping, no global serialisation for callers that don't need it.

### Go API

```go
// Arithmetic in SET (balance = balance - ?) is required here and is planned for M3,
// before transactions land in M5. Pre-reading balances outside the transaction
// creates a lost-update race — do not use that pattern.
err := db.Transaction(func(tx *Tx) error {
    if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 100, fromID); err != nil {
        return err  // propagate → rollback
    }
    if _, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 100, toID); err != nil {
        return err
    }
    return nil  // commit; return non-nil = rollback
})
```

**A failed `tx.Exec` poisons the transaction — ignoring the error cannot commit a partial result.** The example checks every `tx.Exec` error, and that is the recommended style, but correctness must not *depend* on the caller doing so. Once any `tx.Exec` returns an error, the `Tx` is marked poisoned: every subsequent `tx.Exec` is a no-op that returns the same sticky error, and at the end `db.Transaction` **forces a rollback and returns that error even if the closure returned `nil`**. Without this, a closure that ignores a failed statement and falls through to `return nil` would commit everything *except* the failed statement — a silent partial transaction. Fatal-on-first-error makes "ignored error" fail safe (whole transaction rolls back) rather than fail open (partial commit).

**How it works internally:**

1. Entering the closure, statements are **not** applied to the live arena, but they are **not** evaluated blind either: each `tx.Exec` evaluates against a per-transaction **staged overlay** layered over the committed store. Statement *N* sees the effects of statements 1…*N*−1 in the same transaction (read-your-writes). The overlay records pending inserts/updates/deletes keyed by `(table, PK)`; reads inside the transaction consult the overlay first, then the live store.
2. `return nil` → determine all affected pkDirectories and data shards (union across all staged mutations, including predicate-evaluation under lock — see *Predicate writes* below); acquire all pkDirectories in ascending table index order; lock all data shards in lexicographic `(table index, shard index)` order (global lock order — deadlock-safe); re-validate the staged set against the now-locked live state (PK conflicts, constraints, types) — and re-evaluate any arithmetic `SET` against the locked live values plus earlier in-transaction effects, so the overlay reflects the true committed-time result; if any validation fails, unlock in reverse and return error — nothing is written to the WAL; write the single `TXN` envelope (commit boundary = envelope boundary; no separate COMMIT token); apply the staged mutations to the live arena in statement order; unlock shards then pkDirectories; return success
3. `return err` → discard the overlay, nothing written, nothing in WAL

**Read-your-writes (required, not optional).** Without the overlay, `INSERT INTO t (id, …) VALUES (X, …); UPDATE t SET … WHERE id = X` would fail or operate on stale state at commit, because the row X does not exist in the committed store until apply time — and two updates to the same row in one transaction would lose the first. The overlay makes intra-transaction reads observe prior intra-transaction writes, which is the SQL contract. The `db.Transaction` transfer example below touches two *different* rows (`fromID`, `toID`) and so does not exercise this path, but the general guarantee must hold. (This is distinct from the lost-update warning about pre-reading *outside* the transaction.)

**Predicate writes — the matching set must be resolved under the commit locks, not frozen at buffer time.** The `(table, PK)` overlay correctly represents pending *effects*, but it cannot pre-freeze *which rows a predicate matches*. For `UPDATE/DELETE … WHERE status = ?`, the set of matching rows can change between the closure body and commit (a concurrent writer flips a row's `status`, or inserts a new matching row). Evaluating the predicate when the statement is first seen and replaying that frozen PK set at commit is a serializability bug — the transaction would touch rows that no longer match and miss rows that now do. Two correct options:

- **Default in v1 — restrict transactions to PK-pinned statements.** Every statement inside `db.Transaction()` must pin its target row(s) by PK (`WHERE id = ?`/`IN (…)`); routing for partitioned tables additionally pins the PartitionKey. The affected shard set is then known up front, no predicate re-evaluation is needed, and contention stays bounded. Non-PK-pinned statements inside a transaction are rejected at plan time.
- **Predicate writes (later) — evaluate under all-shard locks.** If unpinned `WHERE` inside a transaction is supported, the statement's shard set is unknown in advance, so it must lock **all** shards of each table it touches, then evaluate the predicate against the locked live state (plus prior in-transaction effects) and apply. This is correct but reintroduces the all-shard contention spike, which is exactly why it is not the v1 default.

Either way, predicate matching happens under the same locks that protect the apply — never against a stale pre-lock snapshot.

**Read isolation — what a transaction may read, and what is not promised in v1.** The overlay gives *read-your-writes* (a statement sees the transaction's own earlier effects), but it does **not** by itself give isolation against *other* committed transactions. The dangerous pattern is read-compute-write spanning the closure: read value A early, compute something from it in Go, then write B based on it — a concurrent transaction can commit a change to A in between, and because only the *write set* is validated under the commit locks, the stale read of A is never rechecked → lost update / non-serialisable result. v1 closes this by construction rather than by adding optimistic-concurrency machinery:

- **v1 transactions are write-only at the API.** `tx.Exec` only; there is no `tx.Query` that hands committed row data back to the closure for arbitrary computation. The only "reads" are internal: read-your-writes of the transaction's own staged effects, and the read embedded in an arithmetic `SET` (`balance = balance - ?`), which is evaluated against the **locked** live value at commit — so that read is consistent, not a pre-lock snapshot. Read-then-write logic must be expressed as arithmetic `SET`, not as app-side compute over a `tx.Query` result.
- **Arbitrary read-for-compute inside a transaction is not promised in v1.** Supporting it requires tracking the transaction's read set and validating it under the commit locks (abort if any read row changed — optimistic concurrency / SSI), or taking read locks on read rows. Both are deferred; the spec must not imply serialisable read-compute-write until one is implemented.

**Auto-generated PKs resolve at statement-execution time, not at commit.** When a transaction's `INSERT` omits the PK, the UUIDv7 is generated when that statement executes inside the closure (and recorded in the overlay), not deferred to commit. This is required so that (a) a later statement in the same transaction can refer to the row via read-your-writes, and (b) the exact same concrete UUID is what lands in the `TXN` envelope and what replay regenerates-by-reading. Deferring generation to commit would make in-transaction back-references impossible and risk a mismatch between the value the closure observed and the value written to the WAL.

**Why this ordering is critical:** the `TXN` envelope is written only after all mutations have been validated (and predicates resolved) under lock. A committed `TXN` envelope therefore always means the transaction was validated and will apply cleanly on replay. It is never possible for a committed WAL record to represent a transaction that would fail on re-execution. Writing the envelope before validating (the naive order) is wrong — a PK conflict discovered after the WAL write leaves a committed record that was never successfully applied.

WAL replay: a torn or CRC-failing `TXN` envelope is discarded in its entirety (the commit boundary is the envelope boundary). A complete, CRC-valid `TXN` envelope is always safe to replay — it was written only after successful in-memory validation under the relevant locks.

### FrankenPHP / cgo API

A Go closure cannot cross the cgo boundary. `START TRANSACTION` / `COMMIT` as separate calls would work but requires goroutine-local state between calls and four cgo crossings (~200 ns each).

**The array form is strictly better:**

```php
// Arithmetic in SET required here; planned for M3 before transactions land in M5.
// Pre-reading balances outside the transaction creates a lost-update race.
hazedb_exec_transaction([
    ["UPDATE accounts SET balance = balance - ? WHERE id = ?", [100, $fromID]],
    ["UPDATE accounts SET balance = balance + ? WHERE id = ?", [100, $toID]],
])
```

- One cgo crossing instead of four
- No goroutine-local state between calls — pure function, input in, result out
- If PHP crashes before the call: the call never happened, no leaked state to clean up

On the Go side this maps directly onto the closure API:

```go
func hazedb_exec_transaction(stmts []Statement) error {
    return db.Transaction(func(tx *Tx) error {
        for _, s := range stmts {
            if _, err := tx.Exec(s.SQL, s.Params...); err != nil {
                return err
            }
        }
        return nil
    })
}
```

### Codegen — named transactions

`go generate` can precompile a fixed array of SQL literals into a single typed Go function:

```go
// generated — zero SQL parsing at runtime, one cgo crossing
func TransferBalance(fromID, toID UUID, amount int64) error
```

PHP calls `hazedb_transfer(fromID, toID, amount)` — SQL parsing and dispatch are compiled away. Locking, WAL write, and commit still execute at runtime.

### What this does not cover

Cross-table transactions (debit one table, credit another) require locking shards across two tables. The locking order must be globally consistent (table index ascending, then shard index ascending within each table) to remain deadlock-safe. Deferred to v1.1+.

---

## Measured benchmarks

> **Scope:** these measurements apply to the M1+M2 implementation — physical binary WAL, runtime SQL interpreter, integer PK. They do not reflect the target architecture (logical WAL, codegen, UUIDv7 PK). Benchmarks for the target architecture will be taken as milestones complete.

All on AMD Ryzen AI MAX+ 395 (32 threads), Docker container, golang:1.22.

### Point-operation comparison (single-thread, stmt cache warm)

| Operation | hazedb | SQLite (cgo+database/sql) | Bolt (per-tx) |
|---|---:|---:|---:|
| INSERT | 0.58 µs | 10.5 µs | 1 443 µs* |
| SELECT WHERE pk=? | 0.22 µs | 2.65 µs | 0.52 µs |
| UPDATE WHERE pk=? | 0.19 µs | 2.26 µs | 1 411 µs* |
| DELETE WHERE pk=? | 0.40 µs | 10.7 µs | 1 422 µs* |

*Bolt: fsync per transaction — unfair for writes. Reads are fair.

### Parallel scaling (32 goroutines)

| Operation | Single | Parallel | Speedup |
|---|---:|---:|---:|
| SELECT WHERE pk=? | 0.22 µs | 0.08 µs | 2.8× |
| INSERT (memory) | 0.58 µs | 0.08 µs | 7.0× |
| INSERT (WAL) | 0.58 µs | 0.41 µs | 1.4× ← walMu bottleneck |
| UPDATE WHERE pk=? | 0.19 µs | 0.07 µs | 2.7× |

### Mixed workload — 4 writers + 16 readers, 2 s, WAL on

| | Value |
|---|---:|
| Insert throughput | 690k/sec |
| Read throughput | 7.64M/sec |
| SELECT WHERE pk=? p50 | 0.71 µs |
| SELECT WHERE pk=? p99 | **10.7 µs** |
| SELECT WHERE pk=? p99.9 | 242 µs |

### Tail-scan (spike, hand-written, messages table)

| Workload | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| Uniform (8 writers + 24 scanners) | 0.80 µs | 11 µs | 54 µs |
| Skewed 90% → 10% threads | 0.85 µs | 19 µs | 67 µs |
| Skewed 99% → 10% threads | 0.71 µs | 16 µs | 56 µs |

**p99 < 50 µs holds under all skew levels tested.**

---

## Current file layout

Files that exist today (M1+M2). Where a file's eventual scope differs from what runs now (notably `wal.go`), the planned additions are annotated inline and dated by milestone — they are not implemented yet.

```
github.com/VeloxCoding/hazedb   (package hazedb)
├── value.go         Value union (Int/String/Bytes/Bool/Null), Row, Clone
├── schema.go        Schema, TableDef, ColumnDef, resolvedTable, validateValue
├── errors.go        Sentinel errors
├── store.go         Sharded RWMutex storage: insert/getByPK/scanAll/update/delete
├── wal.go           Logical typed-mutation WAL: versioned envelope (magic|type|version|length|payload|crc32c), MUTATION payload, CRC32C, durability modes, bounds-checked tail recovery. Planned: TXN envelope grouping (M6), snapshot load (M7)
├── uuid.go          UUID [16]byte type + monotonic RFC-9562 UUIDv7 generator
├── lexer.go         Tokenizer
├── ast.go           AST node types
├── parser.go        Recursive-descent parser
├── exec.go          Planner + executor (PK fast path, scan fallback)
├── db.go            Public API: Open/Exec/Query/Close, stmt cache
├── *_test.go        Unit, race, stress, mixed-latency, bench, comparison
└── spike/           Preserved prototype code (package spike) — reference only
```

---

## Open decisions

| # | Question | Default if left open |
|---|---|---|
| 1 | Out-of-order seq policy | Accept O(N) shift, document |
| 2 | walMu contention ceiling | Single mutex until parallel-WAL benchmark demands change |
| 3 | pkDirectory mutex strategy for partitioned tables | Single `sync.RWMutex` until a contention benchmark shows it is the bottleneck; then shard by FNV-1a(UUID top bits) |

**Settled decisions (not revisitable without good reason):**

| Decision | Choice |
|---|---|
| PK type | UUIDv7, enforced, auto-generated if omitted |
| Shard routing | By `PartitionKey` if declared; by PK hash otherwise |
| Tail index rowID ambiguity | Solved by PartitionKey sharding — all rows for a partition value in one shard; `(shardID, rowID)` pairs rejected |
| pkDirectory for partitioned tables | Required from day one — not deferred. Enforces table-wide PK uniqueness and enables O(1) `WHERE id = ?` without scanning all shards. PK and PartitionKey columns are immutable (enforced at plan time). |
| WAL format | Logical **typed-mutation**: op + tableID + resolved typed params per record (full row on insert; PK + changed-column deltas on update; PK on delete). **NOT SQL-string** — benchmarked and rejected (SQL text cost +50% bytes/insert, 2.5× bytes/delete, ~2× replay; spike in `wal_format_spike_test.go`). All auto-generated values resolved before write; deterministic replay via the apply path; `hazedb dump` reconstructs SQL for inspection. |
| WAL durability default | async-bufio + ticker (1 s), fsync opt-in via `WALSync bool` |
| Public API | Two tiers: `db.Query()`/`db.Exec()` generic interpreter path (`[][]Value`) + `*Queries` generated typed methods per declared `.sql` query (hot path); no runtime SQL dispatch table |
| Multi-shard non-PK writes | Closed by correctness, not preference. The one-shard-at-a-time pattern is a write-serializability + replay-divergence bug (see *Transactions — The problem*). A multi-shard `UPDATE`/`DELETE` not pinned to a single shard by PK/PartitionKey must either lock all affected shards before the WAL write, or be wrapped in `db.Transaction()`. Until `db.Transaction()` lands (M5), such statements take the lock-all-shards path or are rejected at plan time. |

---

## Roadmap

| Milestone | Content | Status |
|---|---|---|
| **M1** | Single-table store, WAL, tail-recovery, CI bench gate | ✅ done |
| **M2** | SQL parser + interpreter (SELECT/INSERT/UPDATE/DELETE) | ✅ done |
| **M3** | WAL ticker flush + optional fsync (`WALFlushInterval`, `WALSync`, `WALSyncPerWrite`, sticky error state); arithmetic expressions in `SET`/`WHERE` (`col + ?`, `col - ?`, `col * ?`) | ✅ done |
| **M4** | Codegen: `go generate` reads `.sql` query files → `*Queries` struct with one typed method per query + pre-compiled plan; strict mode (`StrictMode` option); generated cgo wrappers per query for FrankenPHP; generic `hazedb_sql` fallback | open |
| **M5** | Single-table transactions: `db.Transaction(func)` Go API + staged overlay (read-your-writes) + atomic `TXN` WAL envelope + torn-envelope discard on replay | open |
| **M6** | Multi-table support, secondary indexes on non-PK columns (note: `pkDirectory` for partitioned tables is a primary-key directory, not a secondary index — it is required from M4 onward, not deferred here) | open |
| **M7** | WAL segments (each with a `base` global-offset header) + snapshot checkpoint with consistent cut: pause all writes → record current global LSN → dump all live rows as `INSERT` statements to snapshot file → fsync snapshot + dir → write `CHECKPOINT <file> <lsn>` to WAL → atomically update `MANIFEST{snapshot,lsn}` → resume writes; on restart read manifest (or two-pass scan) to find the newest *verified* checkpoint, load its snapshot, then replay WAL from its global LSN (resolved to `(segment, offset)` by base comparison); delete pre-checkpoint segments | open |
| **M8** | CLI (`hazedb dump/verify/checkpoint`), Caddy module, FrankenPHP cgo binding (`hazedb_exec_transaction` array API + named transaction codegen) | open |

**M7 note:** the snapshot IS a logical WAL file — a series of INSERT *mutations* (typed-mutation records, not SQL text) for every live row at a known WAL position. Loading it produces a fresh arena with no tombstones. No special arena compaction code is needed: tombstones accumulate in active memory until a snapshot restart or live reload; once the snapshot loads, the arena starts clean.

**Consistent cut is required.** If writes continue during the dump, the snapshot can contain rows that are also replayed after the checkpoint, miss rows that belong before it, or represent a row combination that never existed simultaneously. The correct protocol is: briefly pause all writes (global write barrier) → record the current global LSN → dump all live rows → write `CHECKPOINT <file> <lsn>` → resume writes. On restart: load snapshot, then replay WAL from LSN onward (resolving the global LSN to a `(segment, offset)` by segment-base comparison). Without the write barrier, checkpoint recovery is not reliable.

**Durability ordering of the checkpoint itself is also load-bearing.** The `CHECKPOINT <file> <lsn>` marker must not become durable, and pre-checkpoint segments must not be deleted, until the snapshot file is actually on stable storage. The required order is: dump snapshot → `fsync` the snapshot file **and** `fsync` its containing directory (so the new file's directory entry survives power loss) → only then write and flush/sync the `CHECKPOINT` marker → only then delete pre-checkpoint segments. If the marker is made durable (or old segments deleted) before the snapshot is fsync'd, a crash can leave a committed checkpoint pointing at a snapshot that is absent or partial, with the WAL prefix it depended on already gone — unrecoverable. The same directory-fsync requirement applies whenever a new WAL segment file is created, not just for snapshots.

**LSN semantics must be pinned down (off-by-one otherwise).** Define `lsn` as the **exclusive** position of the first WAL record *not* reflected in the snapshot — i.e. the write cursor at the moment the write barrier is taken, before the `CHECKPOINT` marker is appended. Because the barrier guarantees no data records are written between dumping the snapshot and appending the marker, the snapshot reflects exactly everything before `lsn` and the `CHECKPOINT` marker itself is written *at or after* `lsn`. Replay then: (1) read the snapshot to rebuild state as of `lsn`, (2) re-open the WAL and scan **from `lsn` inclusive**, (3) treat any `CHECKPOINT` record encountered during replay as a no-op marker (skip it; it carries no row state), (4) apply every data record from `lsn` onward exactly once. Getting this wrong in either direction is a real bug: an inclusive-vs-exclusive mismatch double-applies or skips the first post-snapshot record, and replaying the `CHECKPOINT` marker as if it were a statement fails. State the offset convention in the marker format and in the replay code comment so both sides agree.

**LSN must be segment-aware (a bare byte offset is ambiguous once the WAL is segmented, M7).** With multiple segment files, "byte offset 4096" does not identify a position — offset *into which segment*? Define the LSN as a **global, monotonically increasing logical offset** across the whole WAL, and give every segment file a header recording its `base` global offset (the global offset of its first byte). An LSN then maps to a physical location by finding the segment with `base ≤ lsn < base + segment_size` and seeking to `lsn − base` within it. Equivalently, store the LSN as an explicit `(segmentID, offsetInSegment)` pair. Either is fine, but it must be one of them: a raw per-file offset in the `CHECKPOINT` marker can, after segment rotation, point recovery at the wrong segment or the wrong place in the right segment. The `lsn:8` field in the `CHECKPOINT` payload is this global logical offset; segment selection during recovery is by segment-base comparison, not by filename ordering alone.

**Records never span a segment boundary; segment headers are outside LSN space.** Two framing rules make segmented recovery simple and unambiguous:

- **No record straddles two segments.** Before appending an envelope that would not fit in the current segment's remaining capacity, rotate to a new segment and write the whole envelope there. Recovery then reads each segment as a self-contained sequence of complete envelopes and never has to stitch a record's bytes across files. (Any trailing free space in a segment is just padding the tail scan stops at.)
- **The segment header does not consume LSN space.** The global LSN counts only the logical record stream, not the per-file framing header. So `lsn − base` is the offset to the first *record* in a segment, and resolving an LSN never lands the reader in the middle of (or at the start of) a segment header. State explicitly whether `base` is the global LSN of the segment's first record (recommended) so the arithmetic is unambiguous on both write and recovery paths.

**Checkpoint discovery at recovery must be explicit — naive one-pass replay is wrong.** The snapshot path and `lsn` live inside a `CHECKPOINT` marker that is itself in the WAL, which creates a chicken-and-egg: you cannot just open the first segment and replay forward, because the records *before* the latest checkpoint are already captured in the snapshot — replaying them re-applies pre-checkpoint history (double-apply, and far slower than necessary), and you also have to find the *newest valid* checkpoint, not the first one. Recovery is therefore explicitly staged:

- **Preferred — a checkpoint manifest.** Maintain a tiny `MANIFEST` file holding the current `{snapshot_path, lsn}` (and the live segment list). It is updated by atomic replace (write `MANIFEST.tmp` → fsync → rename → fsync dir) **after** the snapshot is durable and **before** old segments are deleted. Recovery reads `MANIFEST` first — no WAL scan needed to locate the checkpoint — verifies the named snapshot exists and passes a integrity check (e.g. a length/CRC recorded in the manifest), loads it, then replays WAL data records from `lsn` onward. A torn `MANIFEST.tmp` is ignored; the last good `MANIFEST` always points at a complete checkpoint because of the ordering above.
- **Alternative — explicit two-pass recovery.** Pass 1 scans the WAL **without applying anything**, tracking the highest-LSN `CHECKPOINT` marker whose envelope CRC is valid *and* whose named snapshot verifies. Pass 2 loads that snapshot and replays data records from its `lsn` forward. Slower (a full scan to find the checkpoint) but needs no extra file.

Either way the invariant is: find the latest *verified* checkpoint first, load its snapshot, then replay strictly from its `lsn`. Never apply records below the chosen `lsn`. If no valid checkpoint exists (fresh DB, or all checkpoints fail verification), fall back to replaying the entire WAL from the beginning.

Snapshot also functions as a sync baseline for replication consumers, provided the consumer receives both the snapshot file and the WAL offset it corresponds to.

**Pre-M7 caveat — no log truncation means unbounded WAL and linear recovery time.** Checkpointing is the *only* mechanism that lets the WAL be truncated (delete pre-checkpoint segments) and that bounds recovery work. It is the last milestone (M7). Until it lands, the WAL grows without bound on disk for the life of the process, and — more importantly — **restart replays the entire history from the beginning every time**, so recovery time grows linearly with total writes ever made, not with live data size. For a long-running, write-heavy deployment this is an operational ceiling distinct from the in-memory churn caveat (that one is about RAM; this one is about WAL disk footprint and cold-start time). If long uptimes are expected before M7, either schedule periodic restarts from a freshly exported baseline, or prioritise pulling the snapshot/checkpoint work earlier.

**Deferred to v1.1+:** cross-table transactions, group-commit drainer, skiplist index, blob out-of-line storage, lock-free reads via `atomic.Pointer`.

---

## Review coverage (invariant × operation sweep)

Each mechanism was checked against every operation that can touch it — insert, single-shard read, multi-shard read, PK update/delete, predicate update/delete, transaction, WAL append, flush/sync, tail recovery, replay, checkpoint, snapshot-load — under concurrency and under crash-at-each-step. Status: **safe** (holds as written), **§** (addressed, see section), **open** (documented limitation or deferred). This table is the audit trail for what has been examined; anything not listed has *not* been swept and should be treated as unreviewed.

| Mechanism / invariant | Result | Where |
|---|---|---|
| Non-partitioned shard routing (FNV-1a PK) | safe; routing re-derived, nothing shard-specific persisted | *Store foundation* |
| Partitioned shard ≠ partition value; tail index per partition value | § fixed (was a mixing bug) | *Partitioned table*, *Ordered index* |
| `pkDirectory` table-wide uniqueness | safe; catches cross-partition dup UUID | *Partitioned table* |
| Partitioned `WHERE id=?` read TOCTOU | § retry pkDirectory on tombstone/mismatch (not return not-found) | *Read-path TOCTOU* |
| Transaction error handling | § first `tx.Exec` error poisons the tx; ignored error → rollback, not partial commit | *Go API* |
| Checkpoint discovery at recovery | § manifest or two-pass; load newest verified checkpoint before replay | *Checkpoint discovery* |
| rowID width | § `uint64` (uint32 overflow under churn) | *Ordered index* note |
| Tombstones / arena never shrinks | open: RAM churn + scan degradation until M7 | *Churn caveat* |
| Tail-index order column mutability | § immutable at plan time | *Immutability* |
| Global lock order (incl. checkpoint barrier, cross-table tie-break) | § lexicographic + barrier topmost | *Lock ordering* |
| Multi-shard non-PK writes | § lock-all-shards or `db.Transaction()`; one-at-a-time is a bug | *Transactions → The problem* |
| Multi-shard SELECT consistency | open by design: per-shard consistent, not point-in-time; gather-then-sort for LIMIT | *Read consistency* |
| Lock-before-WAL-append ordering | safe for pinned/single-shard; holds for multi-shard only under lock-all | *WAL format* |
| WAL envelope (mutation/txn/checkpoint, versioned, CRC32C, LE) | § typed self-delimiting envelope; typed-mutation payload chosen over SQL-string after benchmarking | *Record envelope* |
| Unknown WAL version/type | § fail loud, never skip (corrected from a bad draft) | *Record envelope* |
| Tail-recovery length validation | § bounds-check envelope + inner lengths before read | *Tail-recovery robustness* |
| `bw.Write` append error | § abort before apply; enter error state | *Execution pipeline*, *WAL error handling* |
| Flush vs fsync after auto-flush | § `dirtySinceSync` flag, not `Buffered()` | *WAL durability* |
| Flush goroutine concurrency | § holds `walMu`; no ticker when interval ≤ 0 | *WAL durability* |
| Logical-WAL replay determinism | safe; all non-deterministic values resolved before append | *Primary key*, *WAL format* |
| Transaction read-your-writes | § staged overlay | *Read-your-writes* |
| Transaction predicate writes | § resolve matching set under commit locks; v1 = PK-pinned only | *Predicate writes* |
| Transaction read isolation (read-compute-write) | open: v1 write-only API; SSI/read-set validation deferred | *Read isolation* |
| Auto-gen PK inside a transaction | § resolved at statement-execution time | *Auto-generated PKs* |
| Checkpoint consistent cut + fsync ordering | § barrier + fsync snapshot/dir before marker | *Consistent cut* |
| Checkpoint LSN inclusive/exclusive + marker skip | § exclusive; scan from LSN; skip marker | *LSN semantics* |
| Segmented WAL: LSN ambiguity, record-boundary, header LSN-space | § global LSN + base; no record spans a segment; header outside LSN-space | *LSN segment-aware* |
| WAL truncation / recovery time pre-M7 | open: unbounded WAL + linear cold-start until M7 | *Pre-M7 caveat* |
| Statement cache growth | open: unbounded; safe only under parameterisation | *SQL interpreter* |
| UUIDv7 ordering | § ms-granularity; strict order needs monotone gen or `seq` | *Primary key* |

**Not yet swept (flagged for a future pass):** backpressure when `walMu` or the fsync path cannot keep up with writers (does append block, drop, or error?); behaviour of a *failed* replay/`Open()` (partially-applied state vs clean abort); `Close()` semantics with in-flight writes and a pending flush; clock-regression effects on UUIDv7 monotonic generators; per-value size caps (large blob params vs the `uint32` payload length).

---

## One line

hazedb compiles into your Go binary, keeps all data in RAM, writes a WAL for durability, and serves SQL queries at sub-µs p50 / <50 µs p99 under concurrent mixed workload.

---

## Changelog

**rev. 7 (2026-05-29) — WAL format settled by benchmark**

The WAL format was an open architectural choice between three encodings. A spike (preserved in `wal_format_spike_test.go`, run via `golang:1.22` docker) measured all three on write size and decode+apply (replay) cost, sharing one value codec for fairness:

| record | Physical row-image | SQL-string logical | Typed-mutation (chosen) |
|---|--:|--:|--:|
| insert | 127B | 190B | 127B |
| update narrow | 127B | 151B | 110B |
| update wide (1 of 9 cols) | 218B | 86B | **51B** |
| delete | 24B | 60B | 24B |
| replay insert (ns/op, allocs) | 198 / 3 | 389 / 5 | 198 / 3 |
| replay update narrow | 185 / 3 | 225 / 5 | 149 / 4 |
| replay delete | 92 / 1 | 183 / 3 | 92 / 1 |

- **Adopted typed-mutation, rejected SQL-string.** SQL-string lost on the two dimensions that matter here — WAL write size (+50% insert, 2.5× delete) and replay (~2× slower, more allocs, because every record re-runs prepare + the eval pipeline). Its only plausible edge (a human/replication-friendly SQL log) is illusory: the envelope is binary-framed with CRC regardless, and a consumer already needs the exact schema + UUID + transaction semantics to replay. Typed-mutation keeps every "logical" benefit (replay through the apply path, snapshots, TXN/CHECKPOINT envelope) without the parser tax, and additionally beats the *current physical* format on updates by delta-encoding (51B vs 218B on a wide one-column edit).
- **Envelope payload redefined.** `STATEMENT (sql_len|sql|params_len|params)` → `MUTATION (op|tableID|op-body)`, where INSERT carries the full row, UPDATE carries `pk + (ordinal,value)` deltas, DELETE carries the pk. Type byte renamed `1=MUTATION`. TXN groups MUTATION payloads. Reconciled the format section, replay, tail-recovery (inner-length wording), `hazedb dump`, settled-decisions, file-layout, and review-coverage rows.

**rev. 6 (2026-05-29) — fifth review pass**

Of the eight points raised, five were already fixed in rev. 5 (the review was against rev. 4): tx read isolation, `dirtySinceSync`, `bw.Write` fatal-path, segment record-boundary, and `uint64` rowID. Three were genuinely new and are applied here:

- **Partitioned `WHERE id=?` could return a phantom not-found — correction of a rev. 3 mistake.** rev. 3 told the read path to treat a tombstone/PK-mismatch at the resolved location as not-found. That is wrong for `DELETE`+`INSERT` of the same PK (PartitionKey-move): the row exists before and after the transaction, only at a new location, so not-found is a phantom. Fixed: on tombstone/mismatch, **retry the `pkDirectory` lookup** (bounded) — it finds the new location or a genuine deletion — or hold the directory read lock across the shard read. The "return not-found" rule is removed.
- **Transaction error handling — poisoned tx.** First `tx.Exec` error now makes the `Tx` sticky-failed: later `tx.Exec` calls no-op with the same error, and `db.Transaction` forces rollback returning that error even if the closure returns `nil`. Prevents an ignored statement error from silently committing a partial transaction. Go example updated to propagate errors.
- **Checkpoint discovery at recovery.** Specified how recovery *finds* the checkpoint before replaying: a `MANIFEST{snapshot, lsn}` file (atomic-replace, fsync, after snapshot durable / before segment deletion) read first, or an explicit two-pass scan for the newest verified `CHECKPOINT`. Naive one-pass replay from the first segment would double-apply pre-checkpoint records. M7 roadmap updated.

**rev. 5 (2026-05-29) — fourth review pass + full invariant sweep**

External points (all confirmed and applied):
- **Transaction read isolation.** v1 transactions made write-only at the API; read-compute-write across the closure (lost-update / non-serialisable) is not promised without read-set validation / SSI, now stated. Internal reads (read-your-writes, arithmetic `SET`) are evaluated under commit locks.
- **fsync skipped after auto-flush.** Sync decision now keys off a `dirtySinceSync` flag, not `bw.Buffered()`; `bufio` auto-flush could leave unsynced data invisible to a `Buffered()`-gated sync, breaking the `WALSync` power-loss bound.
- **WAL append errors.** `bw.Write` errors are now fatal and checked at pipeline step 6 — abort before applying to memory; added to the error-state rules alongside flush/sync.
- **Segmented WAL framing.** Records never span a segment (rotate before append); segment headers are outside LSN space; recovery resolves LSN→segment by base comparison.
- **rowID overflow.** `rowID` widened to `uint64` (uint32 ≈ 4.29B slots incl. tombstones is reachable on a hot shard before M7 snapshot; wraparound is silent corruption).

Found in own sweep (not externally flagged):
- **Bug introduced in rev. 4, now fixed:** the envelope text said recovery should "skip records it doesn't understand" — that is silent data loss for a data WAL. Unknown version/type now fails loud (aborts `Open()`); only tail truncation discards.
- **Checkpoint write-barrier** added to the top of the global lock order (an unlisted "pause all writes" barrier is a latent lock-order violation).
- **Pre-M7 caveat:** no log truncation → unbounded WAL on disk and cold-start recovery time linear in total history (distinct from the RAM churn caveat).
- **Multi-shard `ORDER BY` + `LIMIT`** must gather-then-sort; `LIMIT n` cannot be pushed per-shard.
- **Shard count** clarified as runtime-derived and never persisted; routing re-derived on replay/snapshot-load, self-consistent across machines.
- **Auto-gen PK in a transaction** resolves at statement-execution time (read-your-writes + WAL consistency).
- Added a **Review coverage** matrix (invariant × operation) plus an explicit "not yet swept" list.

**rev. 4 (2026-05-29) — third review pass applied**

- **Multi-shard SELECT consistency (now defined).** Added an explicit read-consistency model to the SQL interpreter: single-shard/pinned scans are point-in-time; unpinned multi-shard scans are per-shard consistent but *not* globally point-in-time (the read-side counterpart of the multi-shard write rule). Consistent cross-shard reads require read-locking all involved shards.
- **Transaction predicate writes (overlay fixed).** The `(table, PK)` overlay captures effects but cannot pre-freeze a predicate's matching set. Added *Predicate writes*: v1 restricts transactions to PK-pinned statements (non-pinned rejected at plan time); the later predicate-write path must lock all shards of each table and evaluate the predicate under those locks. No frozen-pre-lock row sets.
- **Tail-index order column is now immutable.** Added to the plan-time immutability rules: `UPDATE … SET <order_col> = ?` is rejected, because it would leave `partitionIndex.seqs` stale and corrupt tail-scan order. Mutable-order alternative (maintain the index on update, O(N) shift) documented but not the v1 default.
- **WAL record envelope (replaces single-statement format).** Defined a typed, versioned, self-delimiting envelope `magic|type|version|length|payload|crc32c` with STATEMENT / TXN / CHECKPOINT payloads, explicit little-endian byte order, and CRC32C. Transaction atomicity is the envelope boundary (torn envelope discarded), replacing the "(sql, params) pairs + COMMIT marker" framing. Reconciled the tail-recovery, transaction-internals, replay, and M5 roadmap text.
- **Segment-aware LSN.** LSN redefined as a global monotonic logical offset; each segment header records its `base` global offset, and recovery resolves an LSN to `(segment, offset)` by base comparison (or store `(segmentID, offset)` explicitly). A bare per-file byte offset is ambiguous once the WAL is segmented. Updated the M7 roadmap and consistent-cut protocol to match.

**rev. 3 (2026-05-29) — second review pass applied**

- **Partitioned shard ≠ partition value (P1).** Corrected the `tableShard` comment ("rows for one partition value" → all partition values that hash to the shard) and added a per-partition-value tail index `tails map[PartitionValue]*partitionIndex`. A single per-shard index would interleave rows from colliding partition values into one scan order. Updated *Ordered index* and the shard-routing bullet to match.
- **Transaction read-your-writes (P1).** Replaced pure buffering with a staged overlay: statements evaluate sequentially against an overlay so statement *N* observes statements 1…*N*−1 (correct SQL semantics for `INSERT x; UPDATE x` and repeated writes to one row). Arithmetic `SET` re-evaluates against locked live state + earlier in-tx effects at commit.
- **WAL flush goroutine concurrency (P1).** Specified that the ticker holds `walMu` across `Buffered`/`Flush`/`Sync` (`bufio.Writer` is not concurrent-safe), and that `WALFlushInterval <= 0` starts no ticker (`time.NewTicker(0)` panics; manual-only mode).
- **UUIDv7 ordering over-claim (P2).** Softened "`ORDER BY id` = temporal order" to ms-granularity; within a millisecond, ordering requires an RFC 9562 monotonic generator or an explicit `seq` column (the tail index's `seqs`).
- **Checkpoint LSN semantics (P2).** Defined `lsn` as the exclusive offset of the first record not in the snapshot; replay scans from `lsn` inclusive and skips `CHECKPOINT` marker records. Removes the double-apply / skip off-by-one.
- **WAL status contradiction (P2).** `wal.go` in *Current file layout* now describes the current physical binary WAL, with logical/COMMIT/snapshot annotated as planned (M4/M5/M7), matching *Implementation status*.

**rev. 2 (2026-05-29) — correctness review applied**

- **Multi-shard non-PK writes (blocking).** Rewrote *Transactions → The problem*: the one-shard-at-a-time pattern is a write-serializability + replay-divergence bug, not just a torn read. Removed the contradictory claim that the WAL append happens "while holding all relevant shard locks" *and* that readers see a torn view (mutually exclusive). Such writes must lock all affected shards before the WAL write or run under `db.Transaction()`. Moved *Open decision* #4 to *Settled decisions* — closed by correctness.
- **Partitioned `WHERE id = ?` read path.** Documented the pkDirectory→shard TOCTOU: the shard read must re-validate liveness (tombstone / PK mismatch = not-found), or hold the directory lock across the read. Default is release-then-revalidate.
- **Churn vs. compaction.** Flagged that high-eviction targets (sessions, caches) grow memory monotonically and degrade tail-scans (tombstone skipping) until M7 snapshot/restart.
- **Cross-table lock order.** Shard lock order specified as lexicographic `(table index, shard index)`, not shard index alone, to prevent multi-table deadlock.
- **WAL tail recovery.** Lengths (`sql_len`/`params_len`) must be bounds-checked against remaining file size before allocate/read; CRC sits after the length-driven read and does not protect against a corrupt length.
- **M7 checkpoint durability.** Snapshot (and its directory) must be fsync'd before the `CHECKPOINT` marker is made durable and before pre-checkpoint segments are deleted; directory fsync also required on new WAL segment creation.
- **Non-partitioned `pk` map.** Target key type changed to `map[UUID]uint32` to match the partitioned path (no string allocation); current string-keyed impl noted as M1+M2-only.
- **Statement cache.** Noted the `sync.Map` is unbounded and safe only under strict parameterisation; inlined literals leak.
