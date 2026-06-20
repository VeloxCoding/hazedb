# hazedb RFC

hazedb compiles into your Go binary, keeps all data in RAM, writes a WAL for durability, and serves SQL at sub-µs p50 / <50 µs p99 under concurrent mixed workload.

This RFC is the **design spec** — how hazedb works, as built and as targeted. Dense by intent.

---

## What it is

A **general-purpose, embedded, memory-resident SQL database** for a single Go process. Generic data model (tables, columns, one PK, optional partitioning + indexes, SQL) — not domain-specific. All reads come from RAM; disk holds only append-only WAL segments + a log-derived SQLite mirror, never table pages or a buffer pool. No network protocol, no separate process.

**Target:** latency-sensitive OLTP with the working set in RAM. Compiled directly into a Caddy module, FrankenPHP extension, or standalone Go binary. (Concrete tables in examples — `messages`, sessions, leaderboards — are *illustrations of the profile*, not built-in features or scope.)

## Non-goals (load-bearing)

| | |
|---|---|
| Not a PostgreSQL/SQLite replacement | only two-table indexed equi-joins; no window functions; no ad-hoc-query perf guarantee |
| Not for data > RAM | WAL + mirror only; no page eviction |
| Not multi-process | one Go process owns the DB |
| Not OLAP | no aggregation engine, no columnar storage |
| No `ALTER TABLE` in v1 | `CREATE`/`DROP TABLE` run at runtime (WAL-logged, survive restart); no in-place column change |
| No `FULL OUTER` / `CROSS` / N-way joins | two-table `INNER`/`LEFT`/`RIGHT` equi-joins only; the probed column must be indexed |
| No migration tooling | write your own transfer script; keep your old PK as a regular column |

## Implementation status

This RFC describes the **target architecture**; not all of it is built. What is built, what is open, and the milestone roadmap live in [rfc-status-roadmap.md](rfc-status-roadmap.md) — kept out of the design doc so it needs no upkeep here.

---

## Store foundation

### Sharded RWMutex over generic rows

```
shards = runtime.NumCPU() * 4   (floor 64, cap 1024, rounded to power-of-two)
```

Fixed at `Open()`, power-of-two so routing is a mask. **Runtime-derived, not persisted** — nothing shard-specific reaches the WAL or mirror (both are logical), so a WAL/snapshot from a 32-core box (128 shards) replays on an 8-core box (64 shards): every row re-routes under the new count, and PK lookups, `pkDirectory`, and tail indexes all derive placement from the same live count. The one requirement: routing is identical for live writes, replay, and load within one process.

Two table shapes.

**Non-partitioned** (default) — shard by `FNV-1a(PK)`; PK uniqueness and `WHERE id = ?` are shard-local (one lock, O(1)). For lookup-heavy tables (users, sessions).

```go
type tableShard struct {
    mu   sync.RWMutex
    rows []Row             // append-only arena; tombstones for deletes
    pk   map[UUID]uint64   // PK → rowID
    live int
}
```

**Partitioned** (`PartitionKey` declared) — shard by `FNV-1a(PartitionKey value)`. For append/scan-heavy tables (messages, events, logs).

```go
type tableShard struct {
    mu    sync.RWMutex
    rows  []Row                              // rows for ALL partition values hashing here
    tails map[PartitionValue]*partitionIndex // one ordered tail index per partition value
    live  int
}
// One pkDirectory per table, not per shard:
type pkDirectory struct { mu sync.RWMutex; idx map[UUID]rowLocation }
type rowLocation struct { shard uint16; rowID uint64 }
```

There are far fewer shards than partition values, so many partition values collide into one shard. The guarantee is one-directional: all rows for a *given* partition value land in *one* shard, but a shard holds many partition values. So the **tail index is namespaced per partition value**, never a single per-shard index — which would interleave unrelated partitions, returning conversation Q's messages on a scan for P. rowIDs are unique within a shard and index its shared arena, so no `(shard, rowID)` pairs are needed.

Partitioned tables have no per-shard `pk` map: PK uniqueness and `WHERE id = ?` go through the table-wide **`pkDirectory`**. INSERT takes the pkDirectory lock (reject dup) → shard lock → write → record location. `WHERE id = ?` is pkDirectory lookup → `rowLocation` → shard read — two O(1) locks instead of one. **The `pkDirectory` is not deferred:** without it two partitions could hold the same UUID undetected and `WHERE id = ?` has no deterministic shard — PartitionKey tables are broken without it.

**Read-path TOCTOU.** Releasing the pkDirectory lock before the shard lock lets a concurrent `DELETE` (or a `DELETE`+`INSERT` move) tombstone the captured location. The rule: **on a tombstone or PK-mismatch at the resolved location, re-do the pkDirectory lookup — never return not-found from a stale location.** A PartitionKey move tombstones the old slot and writes a new one atomically; a reader holding the old location must re-look-up (→ the new location, or genuinely gone), else it reports a phantom disappearance for a PK that never left. The re-lookup also covers the compactor renumbering rowIDs; bound the retries against a move-storm. (Alternative: hold the pkDirectory read lock across the shard read — no TOCTOU, but every point-read serialises against every delete/move. Default is release-then-retry.)

**Row representation — `[]Value` tagged union.** A `Row` is `[]Value`; `Value` is a **packed 32-byte tagged union** (down from 72): int/bool in a word, a UUID inline, a string/bytes backing pointer in one `unsafe.Pointer` (nil or a real Go pointer, so the GC scans it). No binary rows, no deserialization on reads; read through typed accessors (`Int`/`Str`/`Bytes`/`UUID`/`Bool`). Halves resident memory and bytes copied per read. (A post-1.0 typed-struct wrapper can copy a `Row` into a Go struct for ergonomics; the engine runs on `[]Value`.)

**Immutability, enforced at plan time** — the PK, the `PartitionKey`, and (on partitioned tables) the tail-index order column are never valid `UPDATE SET` targets; a move or re-order is `DELETE` + `INSERT` under a transaction. The order column is immutable because `partitionIndex` caches it in `seqs` parallel to `rowIDs`; mutating it in place would leave `seqs` stale and corrupt tail-scan order.

**Tombstone on delete** — `rows[i] = nil`, drop the pk-map/pkDirectory entry; rowIDs stay stable so the tail index needs no update. A background sweeper (`compact.go`) compacts a mostly-dead shard off the write path — relocating live rows, renumbering, rewriting pkMap/pkDirectory + tails — so tombstone slots are reclaimed in-process, not only at restart; `/meta`'s `tombstones` is the transient backlog between sweeps.

### Lock ordering — global invariant

Every operation taking more than one lock acquires them in this order, or it can deadlock:

```
pkDirectory  (per partitioned table involved, ascending table index)
→ data shards  (lexicographic by (table index, shard index))
→ walMu
```

The shard order is **lexicographic `(table index, shard index)`**, not shard index alone — within one table that reduces to ascending shard index, but a cross-table transaction touching `A.3` and `B.3` would otherwise let one acquire `(A.3, B.3)` and another `(B.3, A.3)` → deadlock. Rules: partitioned writes take pkDirectory → shard(s), never the reverse; transactions take all pkDirectories (ascending table index) → all shards (lexicographic) → WAL; non-partitioned tables have no pkDirectory (shard → walMu). Schema is read-only after `Open()`; any future runtime schema lock sits above all of these.

### Background goroutines

Four long-lived `time.Ticker` loops, each one maintenance unit per tick. Started by `Open()` (after replay, so they never race a recovery reader), stopped by `Close()` via stop+done channels:

| Goroutine | File | Work per tick | On `Close()` |
|---|---|---|---|
| **Drain loop** | `drain.go` | feed sealed WAL segments into the SQLite mirror, advance the drain cursor | flush + drain a final time |
| **Compaction sweeper** | `compact.go` | compact any >half-dead shard (relocate live rows, renumber, rewrite pkMap/pkDirectory + tails) | stop immediately (reclamation is optional) |
| **Index merger** | `secindex.go` | merge each indexed table's dirty overlay into its ordered base (time/size-triggered) | final merge |
| **WAL flusher** | `wal.go` | seal the pending WAL buffer into the next segment if the size trigger has not | joined before the final seal |

**Panic containment — `runRecovered`** ([events.go](events.go)). hazedb is embedded **in-process** (Caddy/FrankenPHP), so an unrecovered panic in any of these would crash the *whole host* — and they sit behind no request-boundary recover (the Caddy handler and cgo entry points recover only the calling request). Each tick's work is wrapped in `runRecovered(name, fn)`: a deferred `recover()` that logs to the standard logger (never the single-connection `_hz_events` companion — a panic may hold a mirror tx/lock, so an INSERT could deadlock) and lets the loop continue. It does **not** release resources the panicked code held (a half-open mirror tx, a non-`defer`'d lock), so a subsystem may stall until restart — better than a crash, loudly logged. It cannot catch Go *fatal* errors (stack overflow — bounded separately; OOM; concurrent-map-write; cgo/`unsafe` faults); defense there is prevention + the external durable store.

### Primary key — UUIDv7, enforced

Every table has exactly one PK column, always UUIDv7 (128-bit, time-ordered). Not configurable. Benefits:

- **Client-side generation** — the caller mints the ID before insert; no round-trip for a sequence number.
- **Engine simplicity** — PK is always `UUID`, so lookups are uniform `map[UUID]uint64`, no `any`-PK or per-table type switch.
- **`ORDER BY id` = creation order to the millisecond, total within one process.** The high 48 bits are a unix-ms timestamp. Within a ms, `NewUUIDv7` ([uuid.go](uuid.go)) is monotonic via a 12-bit counter in `rand_a` (RFC 9562 §6.2 fixed-length dedicated counter), advanced by a lock-free CAS stamp that restarts at 0 each ms, borrows from the next ms on overflow, and never regresses on a backward clock — so byte-wise compare is exact creation order. **Across multiple independent writers** within-ms order is *not* coordinated (per-process counters): IDs interleave by ms but not sub-ms, so a cross-writer feed cursor must mint from one front-door, tolerate sub-ms reordering, or order by an explicit `seq`.
- **Collision-safe even across writers.** With the counter in `rand_a`, an auto-gen UUIDv7 carries **62 bits** of crypto-random `rand_b`. In one process IDs are unique by construction. Two IDs from *different* writers collide only on identical ms **and** counter **and** `rand_b` — `1/2⁶² ≈ 1 in 4.6×10¹⁸` per eligible pair (~1 collision per ~30,000 years even at 1M IDs/s across 10 writers). So a UUIDv7 PK is safe as the replication/feed cursor; the only multi-writer caveat is *ordering*, not collision. (62 bits < UUIDv4's 122 — the price of time-ordering; for guaranteed cross-writer uniqueness, single front-door or a node-id embedded in `rand_b`.)

**Auto-generation:** INSERT omitting the PK gets a generated UUIDv7; a caller-supplied one is accepted as-is. Either way the concrete UUID is in the WAL record **before** execution, so replay reproduces the exact row under the exact id. **Migration:** insert into hazedb (new UUIDv7 PKs), keep the original key as a regular column; no migration tooling is provided.

### Ordered index (tail-scan path)

Only on tables with a `PartitionKey`. One `partitionIndex` per partition value, in the owning shard's `tails`:

```go
type partitionIndex struct {
    seqs   []int64   // ordered by the order column, for ONE partition value
    rowIDs []uint64  // parallel pointers into the owning shard's shared arena
}
```

A scan resolves `PartitionKey value → shard → tails[value]` and walks only that value's `seqs`/`rowIDs`. Monotone-append (chat/log) is O(1); out-of-order is an O(N) slice-shift. **rowID is `uint64`, not `uint32`:** rowIDs are monotonic indices into an append-only arena (tombstones included) that only shrinks on a sweep/restart; `uint32` caps a shard at ~4.29B slots *ever allocated* and wraps silently (a reused rowID aliases a live row → silent corruption), reachable on a hot shard in under two hours at 690k inserts/s. `uint64` removes the ceiling (RAM binds first); if `uint32` is ever kept, the allocator must hard-detect approaching `MaxUint32` and force a compaction before wrap.

### Byte capacity and store stats (`MaxBytes`, `/meta`)

Every shard keeps a running **byte tally** of its live rows, maintained under the shard lock by each insert/delete/update (never a walk). `rowCost` = the cells' in-RAM footprint (32-byte `Value` + any string/bytes backing) + a fixed per-row overhead + a flat per-secondary-index charge (payload exact, overheads modelled slightly high). Two features read this O(1) tally:

- **Store stats.** `MetaSnapshot` — HTTP `GET /meta`, PHP `hazedb_meta`, same JSON — reports table count, `MaxBytes`, store-wide `total_rows`/`total_approx_bytes`/`total_tombstones`, and per table its row/column/secondary-index counts, `approx_bytes`, `tombstones`. It sums the per-shard tallies under a brief RLock — O(shards), independent of row count. Sizes are estimates.
- **Byte cap.** `Options.MaxBytes` (Caddyfile `max_bytes`) bounds approximate RAM via a db-wide `byteBudget`: every INSERT **reserves** its `rowCost` before the WAL append, and a reservation that would exceed `MaxBytes` is rejected with `ErrCapacity` (HTTP **507**, PHP **-1**), applying nothing. Deletes release; size-changing UPDATEs adjust. **Never auto-evicts** — no LRU; the caller frees space with `DELETE`/`DROP TABLE`. `MaxBytes == 0` (default) is unlimited (one predictable branch on the hot path; ~+11 ns per insert when capped). `reserve` adds-then-backs-out, so the ceiling is never exceeded (two inserts racing the last bytes can both reject — conservative). An UPDATE that grows a row is accounted but not gated, so a grow can push over `MaxBytes` and inserts then reject until space frees — a known edge, not a leak.

---

## WAL — format (logical typed-mutation)

Each record stores the resolved *operation* — op kind, target table, the concrete typed parameters applied — not SQL text and not physical row-image bytes. *Logical* because replay re-applies through the store's apply path (surviving storage-layout changes, carrying TXN grouping); **not** the SQL-string form of Redis AOF / statement binlog — benchmarked and rejected (SQL text cost +50% bytes/insert, 2.5× bytes/delete, ~2× replay; see *Settled decisions*, changelog rev. 7, spike `wal_format_spike_test.go`).

**Record envelope** — typed, versioned, self-delimiting. All multi-byte integers little-endian, fixed.

```
Envelope: magic:2 | type:1 | version:1 | length:4 | payload:length | crc32c:4
          // crc32c (Castagnoli) over magic|type|version|length|payload
          // type: 1=MUTATION  2=TXN  3=CHECKPOINT (reserved, unused)
          // length bounds-checked against bytes-remaining before payload is read

MUTATION payload:   op:1 | tableID:2 | op-body
  INSERT op-body:   row            (numCols:2, then a typed cell per column)
  UPDATE op-body:   pk-cell | nsets:2 | (col_ordinal:2 | typed cell) × nsets
  DELETE op-body:   pk-cell
TXN payload:        stmt_count:4 | MUTATION-payload × stmt_count

typed cell:         kind:1 | payload
  Int / Bool:       value:8
  String / Bytes:   len:4 | bytes
  Null:             (kind byte only)
```

The asymmetry is the point: INSERT carries the full row, UPDATE only the PK + changed columns (a one-column edit on a wide row is 51B vs 218B for a full row-image). Parameters are the **resolved** typed values actually applied — the auto-gen UUIDv7 PK and any defaults resolved *before* the record is written — so replay reproduces the exact row. A `[]byte` argument is deep-copied at the write boundary so storage never aliases a caller-held slice (only `bytes` columns).

**Two replay invariants, both fail-closed:**

- **Unknown `version`/`type` aborts `Open()` — never skipped** (skipping a data record silently drops a committed mutation).
- **Decoded records are validated, not just parsed.** A CRC-valid record can decode to an impossible mutation (UPDATE ordinal past the column count, INSERT row of the wrong width); applying it indexes a `Row` out of range, and replay runs inside `Open()` with no `recover()` → a panic crash-loops every boot. `applyMutation` range-checks every ordinal and the INSERT width, failing closed with `ErrWALCorrupt` — symmetric with the SQLite drain.

**Execution pipeline (mandatory order, follows global lock ordering):** resolve auto-values → determine pkDirectory + shards → lock pkDirectory (partitioned) → lock all affected shards ascending → validate → **append WAL envelope while still holding the locks; on a `bw.Write` error, abort here and do not apply** → apply to memory → unlock. Holding the locks across both validate and append makes WAL order == in-memory apply order — two writers cannot append in one order and apply to RAM in another. For an arbitrary multi-shard WHERE, lock all table shards or wrap in `db.Transaction()`; one-shard-at-a-time is non-serialisable (see *Transactions*).

**Atomicity is the envelope.** A TXN record is one self-delimiting envelope holding all the transaction's MUTATION payloads; durable iff fully present with a valid CRC. A torn TXN fails its CRC/length check on tail recovery and is discarded whole — no half-applied transaction exists in the WAL.

**Tail recovery validates lengths before trusting them.** The envelope `length` and inner cell lengths are unauthenticated integers read *before* the CRC is reachable, so a truncated/corrupt final record can carry a bogus length — recovery bounds-checks each against the bytes remaining (else over-alloc/OOM or out-of-range panic). A short read or over-long `length` is the incomplete **tail** of an interrupted write → recovery stops there. A bad `magic` or a CRC mismatch on a *fully-present* record is **bit-rot**, not a tail: the good prefix is applied, the break logged, the rest of that segment skipped (recovery and the drain both continue, never abort/stall). With born-sealed segments a sealed file should not present a torn tail at all.

**Replay** applies each typed mutation through the apply path in order — no parse, no re-validation. **`hazedb dump <wal>`** reconstructs readable SQL from the typed mutations for inspection.

## WAL — durability

The WAL is a directory of immutable, **born-sealed** segments. A write appends a complete envelope to an in-memory buffer under `walMu`; the buffer seals into the next segment — temp file → fsync → **atomic rename** — once it reaches **1 MiB** (`flushMaxBytes`) or **~0.5s** elapses, whichever first. A flush *is* a new sealed segment: no open "active" file, no rotate step. **One switch:** `WALPath` set turns it on, empty is memory-only — no durability levels, no per-write fsync.

| WAL | Process-crash loss | Power-loss |
|---|---|---|
| off (empty `WALPath`) | everything (memory only) | none |
| **on** | ≤ the flush window (~0.5s / 1 MiB) | ≤ the flush window — each segment is fsynced on creation |

A crash loses only the un-sealed buffer. **Acknowledge-after-fsync (zero acknowledged-loss) is deliberately not offered** — an fsync per write defeats the in-memory throughput that is the point, and this data is rebuildable from the source of truth; a zero-loss deployment uses a disk-first database. **Why born-sealed over append-then-rotate:** appending to an open segment leaves a partially-written file (a torn tail recovery must tolerate, indistinguishable from a later-corrupted sealed segment); writing each segment whole to a temp file and renaming makes a partial write invisible — a crash leaves either no segment or a complete one. So every `seg-*.wal` is complete by construction and any parse failure is unambiguous corruption (hard error). **Concurrency + error state:** writers and the flusher both take `walMu` (buffer never raced); `Close()` joins the flusher before the final seal. If a buffer append or seal errors, the DB enters a **permanent error state** — step 6 of the pipeline aborts *before* applying to memory (else RAM holds a change not in the WAL), all later writes return the error, reads continue; recovery requires close + reopen.

## Recovery — SQLite mirror + WAL tail (M7)

Durability across restart rests on two on-disk artefacts: the **WAL segments** (recent tail) and the **SQLite mirror** (compacted base). The mirror lives in the **SQLite companion** — always a real file (`Options.CompanionPath`; default `hazedb.db` in WALPath, or the working dir with no WAL), **never in-memory** (an in-memory `CompanionPath` is rejected at `Open()` with `ErrCompanionInMemory`). The companion holds the `_hz_events` operational log (`ts, level, kind, message` — corrupt-segment skips, drain failures; the reserved `_hz_` table prefix can't collide with user tables) in every mode; the mirror part is filled by the drain when WAL is on.

**The drain (normal operation).** A background loop feeds *sealed* segments into SQLite holding **current state** — compacted (`INSERT OR REPLACE` overwrites, `DELETE` removes), one row per live PK. Each segment applies in **one SQLite transaction**, and the segment number — the durable **drain cursor** (`last_drained_segment` in `_hz_meta`) — commits in that same transaction, so a crash mid-drain leaves a clean boundary (whole segment + cursor, or neither). A drained segment is then deleted from disk; the mirror is the system of record up to the cursor. Driver: `modernc.org/sqlite` (pure Go, no cgo). The drain is off the write path.

**Recovery on `Open()` (mirror present) — base-first, tail-on-top:**

1. Open the mirror, read the drain cursor.
2. Rebuild memory from SQLite (the compacted base, rows up to the cursor) through the insert path, **not re-journaled**.
3. Remove WAL segments ≤ the cursor (already in the mirror).
4. Replay the undrained tail (segments past the cursor) directly into memory, **on top of the base** — a tail UPDATE/DELETE of a mirrored row lands correctly because newer mutations apply last.
5. Keep the WAL's segment counter **above the drain cursor** across restarts, so the next flush seals at `cursor+1` or higher — otherwise it could reuse a number ≤ the cursor that `drainOnce` skips forever, silently losing those writes.

The tail is replayed into memory, never re-drained into SQLite. **No WAL → no mirror:** data lives only in RAM (restart starts empty), but the companion file still exists for `_hz_events`. **`recCheckpoint` is reserved** — the original CHECKPOINT-record-naming-a-snapshot design was superseded by the mirror; the type stays reserved for format stability, unused.

---

## SQL interpreter

Path: `parseSQL(sql) → assignParamIndices → plan() → execSelect/execInsert/…`.

**Statement cache** (`sync.Map` keyed by SQL string) eliminates parse+plan on repeated calls. **Unbounded** — safe *only* because every value must be a `?` placeholder (see *Parameterize all values*), so the key space is the set of query *shapes*, finite. Inlining literals would mint a new key per value — a memory-leak/DoS vector.

**PK fast path** — `WHERE id = ?` detected at plan time: non-partitioned → FNV-1a(id) → shard → local pk map (one lock, O(1)); partitioned → pkDirectory → rowLocation → shard (two locks, O(1)). No scan.

Supported surface:

```sql
SELECT col_list FROM table [alias]
       [[INNER | LEFT [OUTER] | RIGHT [OUTER]] JOIN table2 [alias] ON a.col = b.col]
       [WHERE expr] [ORDER BY col [DESC]] [LIMIT n] [OFFSET m]
INSERT INTO table (cols) VALUES (vals)        -- multiple VALUES tuples allowed; one atomic TXN
UPDATE table SET col = val [WHERE expr]        -- arithmetic SET: balance = balance - ? (M3)
DELETE FROM table [WHERE expr]
```

WHERE: `= != <> < <= > >= AND OR NOT IS NULL IS NOT NULL`, `?` params. **Arithmetic in `SET`** (`balance = balance - ?`) is evaluated per row against the live value under the commit lock — read-modify-write with no lost-update window and no prior read.

**Expression nesting is bounded (`maxExprDepth = 256`).** The recursive-descent parser recurses only on parentheses (`parseAtom` → `parseExpr`) and chained `NOT`; everything else iterates. Unbounded, a ~1–2 MB nested-paren `WHERE` drives the parser past the goroutine stack limit, and a Go **stack overflow is a fatal error `recover()` cannot catch** — one request kills the process, bypassing every boundary recover. The parser rejects past 256 with `ErrParse` (→ 400 / PHP -1); because all expression trees originate in `parseSQL`, this transitively bounds the downstream AST walks too.

**Joins (two-table, equi-join, indexed-only).** Result row = left columns ++ right columns; address with `table.col`/`alias.col` (unqualified must be unambiguous). **Law: the probed (non-driving) join column must be the PK or carry an index** — an unindexed-probe join is rejected (`ErrUnindexedJoin`), never an O(A×B) scan. Execution is an indexed nested-loop: scan the driver, probe the other side via its PK map or secondary index (O(driver) probes). `INNER` drives whichever side keeps the probe indexed; `LEFT`/`RIGHT` drive the preserved side and NULL-pad. The driver is materialised before probing (no cross-table lock held), so a join is **per-shard consistent, not point-in-time**. A column is "indexed" for the probe if it is the PK, a single-column index, or the **leading column of a composite `ORDERED INDEX`**; with `ORDERED INDEX (joinkey, ordercol)`, a probe-side `ORDER BY ordercol` walks the already-sorted sub-range and stops at `LIMIT` (single-driver). **Not supported (deliberate): `FULL OUTER`, `CROSS` (no join column to index), N-way joins, non-equi `ON`, covering indexes.** Composite multi-column `ORDERED INDEX (a, b)` *is* supported (prefix equality + `WHERE a=? ORDER BY b` without a sort).

**`OFFSET m`** skips the first `m` matches (in `ORDER BY` order; else the same undefined scan order `LIMIT` uses), on every read path; standard fetch-and-skip (a large offset still walks that many). `LIMIT m, n` not accepted — use `LIMIT n OFFSET m`.

**Read consistency of multi-shard SELECT.** A SELECT pinned to one shard (`WHERE id = ?`, or a scan confined to one partition value) reads under that shard's lock — a consistent point-in-time view. A SELECT spanning shards takes and releases each shard's read lock in turn, so the assembled result may reflect a mix of moments. The contract is **per-shard consistent, not globally point-in-time** (the read-side counterpart of the multi-shard write rule). A multi-shard `ORDER BY col LIMIT n` must **gather-then-sort**: collect (at least the top-n per shard, only if each shard is sorted on `col`), merge, sort globally, then `LIMIT` — taking `n` per shard and concatenating is wrong. For a consistent cross-shard snapshot, pin to one shard or read-lock all involved shards for the scan (all-shard contention spike).

---

## Query API

**One runtime engine serves every query** — `db.Query()`/`db.Exec()` parse a SQL string once, cache the compiled plan, re-run from cache. There is no separate "hot path"; this *is* it (~8× SQLite on point reads with no codegen).

```go
rows, err := db.Query("SELECT body FROM messages WHERE id = ?", id)   // []Row; plan cached
_, row, err := db.QueryRow("SELECT body FROM messages WHERE id = ?", id) // single Row, nil if none; no []Row alloc
n, err := db.Exec("INSERT INTO messages (id, body) VALUES (?, ?)", id, "hello") // rows affected
```

`QueryRow` skips the `[]Row` slice; for an unpinned query it returns the first scan row, so add `LIMIT 1`.

**The gateway — one official entry point.** `*DB` is the single entry point: Caddy calls these methods as native Go; the FrankenPHP extension reaches the *same* methods via cgo (`C → exported Go → same verbs`). There is no second transport. Verbs: `Open`/`Close`/`FlushWAL`/`Exec`/`Query`/`QueryRow`/`Transaction`. Three guarantees every consumer inherits:

- **Validation** — SQL parsed, planned, bound to the live catalog (`prepare()`); args type-coerced (`toValue`). Every value must be a `?` placeholder (below), which bounds the plan cache and makes injection structurally impossible.
- **Boundary clone** — `[]byte`/`Value` args deep-copied in; returned rows deep-cloned out, so callers may retain them past later writes.
- **No bypass** — storage types (`table`, `shard`, `catalog`, `wal`) are unexported. Cross-cutting concerns (auth, tenancy, PHP↔Go marshalling) live in the consumer/adapter, which calls the same verbs.

**Parameterize all values — the `?` requirement.** Every value **must** be a `?`; an inline literal (`WHERE email = 'a@b'`, `SET age = 30`) is rejected at `prepare()` with `ErrParse` (`rejectValueLiterals`). `LIMIT`/`OFFSET`/`ORDER BY`/`IS [NOT] NULL` are structural, not values. Two load-bearing properties: (1) **the plan cache stays bounded** (key = SQL string = query shape, finite — splicing values mints a new key per value, unbounded growth + reparse); (2) **SQL injection is structurally impossible** (a `?` value binds as a typed `Value` at execution, never reaching the lexer/parser). **Escape hatch — `db.ExecScript(sql)`:** a trusted, uncached, multi-statement boot/seed file with inline literals allowed (splits on top-level `;` via the lexer); never fed untrusted input.

**Prepared statements.** `db.Prepare(sql)` returns a `*Stmt` holding the plan, skipping the per-call statement-cache hash; it rebinds automatically if a `CREATE`/`DROP` bumps the catalog version (one atomic load + compare on the hot path), concurrency-safe.

```go
st, _ := db.Prepare("SELECT name, age FROM users WHERE id = ?")
cols, rows, err := st.Query(args...)
dst := make([]Value, 0, 4)                       // caller-owned, reused
out, found, err := st.QueryRowByPK(id, dst)      // typed UUID key + scan-into buffer
```

`QueryRowByPK` is the zero-alloc point-read fast path: typed `UUID` key (no interface boxing), cells written into the caller's reused buffer (no result clone), so a projection without `BYTES` columns **allocates nothing** (`BYTES` cells are cloned for the no-alias guarantee; a non-PK-pinned statement is rejected). Measured point read (go1.25): `db.QueryRow` ~87 ns / 2 allocs → `Stmt.QueryRowByPK` ~36 ns / **0 allocs**. (The handle alone saves only the cache hash ~6%; the win is the typed key + scan-into buffer — the stable, allocation-free read a non-Go consumer wants without changing the core.)

**FrankenPHP / cgo boundary.** The cgo entry points map straight onto the engine; one parse per distinct string (cached), one cgo crossing per call. Args are a native PHP array (PDO-style); result rows come back as native PHP arrays via zval trampolines — no JSON crosses the boundary. See [docs/php-array-bridge.md](docs/php-array-bridge.md).

```php
hazedb_fetch("SELECT body FROM messages WHERE id = ?", [$id])                  // → ['body'=>…] or null
hazedb_exec("INSERT INTO messages (id, body) VALUES (?, ?)", [$id, "hello"])    // → affected count
```

*(Deferred: an optional `bridge/` package of portable encoders (JSON/MsgPack) for wire consumers, and a post-1.0 generated typed-struct wrapper over the same plans for compile-time-typed call sites — ergonomics, not throughput. The open bench-decided question is whether the fast PHP path uses JSON or goes `Value`→zval direct.)*

---

## Runtime catalog and DDL

The set of tables is **live DB state, not a compile-time constant.** `CREATE TABLE`/`DROP TABLE` are first-class SQL run while serving; the schema need not be known at `Open()` (an empty schema is valid).

```go
type catalog struct {
    version uint64
    byName  map[string]*tableRT  // name → table
    byID    []*tableRT           // durable tableID → table; a nil slot is a dropped table
}
```

All tables live in an immutable `catalog` behind an `atomic.Pointer[catalog]`. Every read/write loads the pointer **once** and uses that snapshot for the whole call — lock-free. DDL builds a **new** catalog (copying only the small registry maps; table storage is shared by pointer) and swaps atomically (RCU); in-flight queries keep their view, the old catalog is GC'd when no call holds it. `ddlMu` serialises `CREATE`/`DROP` against each other; reads/writes never take it.

- **Durable table IDs.** `tableID` = the length of `byID` at creation, **append-only, never reused**: `DROP` nils the slot but keeps the length, so a recreate gets a new id. This makes WAL replay unambiguous — a mutation carries its `tableID`, and a drop+recreate never collides.
- **Catalog version → plan invalidation.** `version` increments on every `CREATE`/`DROP`; cached plans are stamped with it and re-bind lazily on the next use if it changed, so a plan never runs against a table that changed under it (a dropped table → `ErrUnknownTable` cleanly). DDL is rare, so re-binding all cached plans is acceptable.
- **WAL-logged, replayed before mutations.** `CREATE`/`DROP` are journaled (`recCreateTable`/`recDropTable`) **before** the new catalog is published, so a crash between journal and swap replays to the same state. On `Open()`, catalog records replay in order before the mutations that reference them; `CREATE` records the full `TableDef` (types + PK/PartitionKey/Immutable/Nullable), so a runtime partitioned table rebuilds its `pkDirectory` + tail index on replay.
- **v1 limits.** `CREATE`/`DROP` only — **no `ALTER`** (not even a keyword; to reshape: create new, copy, drop old). No "DROP while a cursor holds the table" lock is needed — `Query()` fully materialises + deep-clones before returning, so there are no streaming cursors aliasing storage.

---

## Transactions and atomicity

`db.Transaction(func(tx *Tx) error)` (M6, v1 scope) commits a group of **PK-pinned statements on one table** atomically under a single `TXN` WAL envelope. Non-transactional ops pay zero overhead; atomicity is explicit opt-in.

**The multi-shard hazard (why the rule exists).** `UPDATE/DELETE … WHERE expr` can touch rows across shards. Locking and writing one shard at a time, releasing each lock before the next, is a **write-serializability + replay-divergence bug**, not just a torn read: two concurrent multi-shard statements can interleave to a live state with no single serial order (`A=S2, B=S1`), while the WAL's `walMu` total order replays to a *different* state (`A=S2, B=S2`) — post-crash memory diverges from pre-crash by construction. So a multi-shard, non-PK-pinned write has exactly two safe forms: **(1)** lock *all* affected shards before the WAL append (the `updateWhereAll`/`deleteWhereAll` path — collect+validate every new row image under all shard locks, journal as one `TXN`, then apply — all-or-nothing, possible contention spike), or **(2)** wrap it in `db.Transaction()`. One-shard-at-a-time is closed by correctness, never used.

**What is atomic today.** PK ops (`WHERE id = ?`) hit one shard (non-partitioned) or pkDirectory→shard in fixed order (partitioned) — fully atomic. A broad predicate `UPDATE`/`DELETE` runs the lock-all-shards two-pass path above — all-or-nothing. Multi-statement groups are atomic via `db.Transaction()`. **Not atomic:** one-shard-at-a-time (the unsafe pattern, never used) and cross-table / non-PK-pinned statement *groups* (out of v1 scope).

```go
err := db.Transaction(func(tx *Tx) error {
    if _, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ?", 100, fromID); err != nil {
        return err  // any non-nil return → rollback
    }
    _, err := tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = ?", 100, toID)
    return err
})
```

**Poison-on-first-error.** Once any `tx.Exec` errors, the `Tx` is poisoned: later `tx.Exec` are no-ops returning the sticky error, and `db.Transaction` **forces rollback even if the closure returns `nil`**. So an ignored error fails safe (whole rollback), never a silent partial commit.

**Internals.** Statements are staged in a per-transaction **overlay** keyed by `(table, PK)`, layered over the committed store — so statement *N* sees 1…*N*−1's effects (**read-your-writes**, the SQL contract; required, not optional). On `return nil`: union the affected pkDirectories + shards, acquire them in global lock order, **re-validate the staged set against the now-locked live state** (PK conflicts, types) and **re-evaluate arithmetic `SET` against the locked live values**, write the single `TXN` envelope (commit boundary = envelope boundary), apply in statement order, unlock. **The envelope is written only after validation** — a committed `TXN` always replays cleanly; writing it before validating could leave a committed record that never applied. A torn/CRC-failing `TXN` is discarded whole on replay.

**v1 scope (closed by construction, not machinery).**
- **PK-pinned only.** Every statement must pin its rows by PK (partitioned: also the PartitionKey), so the shard set is known up front and no predicate is re-evaluated. A predicate matching set must be resolved *under the commit locks*, never frozen at buffer time (a concurrent writer can change what matches) — supporting unpinned `WHERE` inside a transaction would require locking all shards of each table, deferred.
- **Write-only API.** `tx.Exec` only — no `tx.Query` handing committed data back for app-side compute. The only reads are read-your-writes and the read embedded in arithmetic `SET` (evaluated against the **locked** live value at commit). This closes read-compute-write lost updates without optimistic-concurrency/SSI machinery (deferred).
- **Auto-gen PKs resolve at statement-execution time** (recorded in the overlay), so a later statement can back-reference the row and the exact UUID lands in the `TXN` envelope = what replay regenerates.

**cgo transaction API.** A Go closure can't cross cgo, so the array form is one crossing (vs four for `START`/`COMMIT` calls), pure (no goroutine-local state), and leak-free if PHP crashes before the call:

```php
hazedb_exec_transaction([
    ["UPDATE accounts SET balance = balance - ? WHERE id = ?", [100, $fromID]],
    ["UPDATE accounts SET balance = balance + ? WHERE id = ?", [100, $toID]],
])  // → db.Transaction(loop tx.Exec)
```

**Not covered:** cross-table transactions (lock shards across two tables in the global order — table index then shard index) — deferred to v1.1+.

---

## Measured benchmarks

Headline: the runtime engine (no codegen) runs **~18× SQLite `:memory:`** on point reads — INSERT 0.38 µs, SELECT WHERE id=? 0.11 µs (0 allocs via `QueryRowByPK`), UPDATE 0.085 µs (hazedb in-memory; AMD Ryzen AI MAX+ 395, go1.25; ratios, not an SLA). Full tables in [rfc-benchmarks.md](rfc-benchmarks.md).

## Current file layout

`package hazedb` in the repo root; every `.go` file carries a one-line doc comment stating its scope — the code is the source of truth, and a per-file table here only drifts. Subsystems: **core types** (`value` / `schema` / `uuid` / `pkmap` / `composite_key` / `errors`); **storage** (`store*`, `partition_store`, `compact`, `secindex`, `budget`); **catalog / DDL** (`catalog`); **SQL engine** (`lexer` → `parser` → `plan` → `eval` → `filter` / `exec_*` / `join`, `stmt`, `txn`); **WAL · durability · recovery** (`wal*`, `mutation_codec`, `drain`, `recover_sqlite`, `db_replay`, `events`); **public API · adapters** (`db*`, `query_stream`, `options`, `stats`, `wire`). `spike/` is reference-only prototype code.

## Open decisions

| # | Question | Default if left open |
|---|---|---|
| 1 | Out-of-order seq policy | accept O(N) shift, document |
| 2 | walMu contention ceiling | single mutex until a parallel-WAL benchmark demands change |
| 3 | pkDirectory mutex strategy | single `sync.RWMutex` until a contention benchmark shows it is the bottleneck; then shard by FNV-1a(UUID top bits) |

**Settled decisions (not revisitable without good reason):**

| Decision | Choice |
|---|---|
| PK type | UUIDv7, enforced, auto-generated if omitted |
| Shard routing | by `PartitionKey` if declared; by PK hash otherwise |
| Tail index rowID ambiguity | solved by PartitionKey sharding — all rows for a partition value in one shard; `(shardID, rowID)` pairs rejected |
| pkDirectory for partitioned tables | required from day one; enforces table-wide PK uniqueness + O(1) `WHERE id = ?`; PK/PartitionKey immutable at plan time |
| WAL format | logical **typed-mutation** (op + tableID + resolved typed params; delta on update). NOT SQL-string (benchmarked + rejected). Deterministic replay via the apply path |
| WAL durability | one switch (`WALPath` on/off); born-sealed segments, fsync + atomic rename at 1 MiB / ~0.5s; no per-write-fsync mode |
| Public API | one runtime engine: parse once, cache the plan per SQL string, re-bind on catalog-version change. Optional typed-struct wrapper is post-1.0 ergonomics |
| Schema lifecycle | `CREATE`/`DROP TABLE` at runtime over an atomic catalog (RCU); durable append-only table IDs; no `ALTER` in v1 |
| Multi-shard non-PK writes | closed by correctness: lock-all-shards or `db.Transaction()`; one-shard-at-a-time is a write-serializability + replay-divergence bug |
