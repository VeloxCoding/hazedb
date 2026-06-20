# hazedb — Status & Roadmap

Current implementation status, TODO, and the milestone roadmap. Split out of [RFC.md](RFC.md) (the design spec) so this time-varying state does not clutter the design and can be updated on its own.

## Status & TODO

**Built (M1–M7):** sharded in-memory store; runtime SQL engine (`SELECT`/`INSERT`/`UPDATE`/`DELETE`, two-table joins, arithmetic in `SET`/`WHERE`); enforced monotonic UUIDv7 PK; partition routing + table-wide `pkDirectory` + per-partition tail index; runtime catalog + `CREATE`/`DROP TABLE`; single-table transactions; logical typed-mutation WAL; born-sealed segments + SQLite-mirror recovery. Each subsystem is detailed in its own section of [RFC.md](RFC.md).

**TODO:**

- **M8** — `hazedb_exec_transaction` across the FrankenPHP cgo boundary (the native-array `hazedb_fetch`/`hazedb_fetchall`/`hazedb_exec` calls already ship).
- **post-1.0** — optional typed-struct query wrapper (ergonomics, not a speed mechanism).

## Roadmap

| Milestone | Content | Status |
|---|---|---|
| **M1** | Single-table store, WAL, tail-recovery, CI bench gate | ✅ done |
| **M2** | SQL parser + interpreter (SELECT/INSERT/UPDATE/DELETE) | ✅ done |
| **M3** | WAL ticker flush + optional fsync; arithmetic expressions in `SET`/`WHERE` (`col + ?`, `col - ?`, `col * ?`) | ✅ done (the flush/fsync-modes design was later replaced by the born-sealed WAL — see *WAL — durability* in RFC.md) |
| **M4** | UUIDv7 PK enforced (`[16]byte` inline, monotonic auto-gen) + immutable order column + logical typed-mutation WAL | ✅ done |
| **M5** | PartitionKey routing + table-wide `pkDirectory` + indexed partition scan; **runtime catalog + first-class `CREATE`/`DROP TABLE`** (atomic RCU swap, durable table IDs, catalog-version plan invalidation, WAL-logged DDL) | ✅ done |
| **M6** | Single-table transactions: `db.Transaction(func)` Go API + staged overlay (read-your-writes) + atomic `TXN` WAL envelope + torn-envelope discard on replay | ✅ done (v1 scope: tx.Exec only, PK-pinned, single-table) |
| **M7** | Segmented WAL + recovery base: a background drain mirrors sealed segments into an on-disk SQLite database (compacted current state, durable drain cursor); `Open()` rebuilds from the mirror, then replays the undrained WAL tail on top. See *Recovery — SQLite mirror + WAL tail* in RFC.md. | ✅ done |
| **M8** | CLI (`hazedb dump/verify/checkpoint`), Caddy module, FrankenPHP cgo binding (`hazedb_exec_transaction` array API) | open |
| **post-1.0** | Multi-table support + secondary indexes on non-PK columns (note: `pkDirectory` for partitioned tables is a primary-key directory, not a secondary index — it is core, not deferred here); optional typed-struct query wrapper | open |

## Deferred to v1.1+

Cross-table transactions, group-commit drainer, skiplist index, blob out-of-line storage, lock-free reads via `atomic.Pointer`, read-only standby replication (`VACUUM INTO` snapshot + cursor-gated WAL-tail shipping — see *Proposed — read-only standby* below).

## Proposed — read-only standby (snapshot + WAL tail), v1.1+

**Not implemented.** A second host runs a **read-only standby** rebuilt from two shippable artefacts the primary already produces, reusing the existing SQLite-mirror recovery path (see *Recovery — SQLite mirror + WAL tail* in [RFC.md](RFC.md)) almost verbatim:

- **Base = a `VACUUM INTO` snapshot** — a consistent, compacted, standard SQLite copy of the mirror, taken on the live DB (it reads a WAL-MVCC snapshot, so the drain keeps running; a naive `cp` is unsafe because of mid-flight transactions and the `-wal`/`-shm` sidecars). It copies `_hz_meta`, so the snapshot **carries its own drain cursor** and is self-describing about where the WAL tail resumes.
- **Start = the existing recovery path** — ship the snapshot + the WAL segments past its cursor; the standby calls `Open(CompanionPath=snap.db, WALPath=copied-segments)` and the unchanged `recoverFromSQLite` + tail-replay rebuilds the engine. No replication-specific code.
- **Immutable files share safely** — SQLite's network-FS locking hazards apply only to a live, written DB, not a static read-only copy; a snapshot + sealed segments are immutable, so any number of standbys read their own copies over shared disk / rsync / object storage.
- **The one correctness gate — retain WAL by cursor, never by age.** The snapshot is current to cursor `C`; the standby needs every segment `> C`. But the live drain deletes a segment once it is in SQLite, so gate WAL reclamation on `min(drain cursor, oldest still-supported snapshot cursor)` (or copy the segments atomically with the `VACUUM`). Age-based deletion ("keep the last hour") is the trap — if the drain falls behind it drops a segment the snapshot still needs.

**Constraints.** Single-writer: the standby never produces its own canonical history (else the two diverge). Freshness = snapshot + shipped tail (an hourly snapshot ≈ 1 h behind; streaming each sealed segment as it seals ≈ within one flush of live). Gives cold standbys, read replicas, fast node bring-up, and backup/restore — **not** multi-master / shared-write, which is a consensus + conflict-resolution problem this file-shipping model does not address.
