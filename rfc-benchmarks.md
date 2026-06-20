# hazedb — Measured benchmarks

Split out of [RFC.md](RFC.md): these numbers are re-measured per rev and would otherwise drift inside the design spec. Read them as ratios, not an SLA.

> **Scope:** the *Point operations* table below — **all** columns, hazedb and SQLite/Bolt — was re-measured at rev. 23 under **go1.25**, on top of the fold shard-hash and the prepared-statement path. The *Parallel* / *Durability* / *Mixed* sub-tables are still from earlier sweeps (those paths are unchanged or only get faster, so treat them as conservative). These are the runtime engine itself (no code generation); it runs ~18× SQLite `:memory:` on point reads. All on AMD Ryzen AI MAX+ 395 (32 threads), Docker, go1.25; absolute µs are load-sensitive on this dev box, so read them as ratios, not an SLA.

## Point operations vs SQLite and Bolt (single-thread, fair 16-byte UUID keys)

All four stores key by the same 16-byte UUID, so the comparison is fair on key width. SQLite appears twice: `:memory:` (RAM, no disk — the like-for-like in-memory comparison) and on-disk (WAL journal).

| Operation | hazedb (mem) | hazedb (+WAL) | SQLite (mem) | SQLite (disk) | Bolt |
|---|---:|---:|---:|---:|---:|
| INSERT | **0.38 µs** | 0.50 µs | 1.8 µs | 22 µs | 4 100 µs † |
| SELECT WHERE id=? | **0.11 µs** (`QueryRow` 0.087, `QueryRowByPK` 0.036) | — | 2.0 µs | 3.0 µs | 0.52 µs |
| UPDATE WHERE id=? | **0.085 µs** | — | 1.07 µs | 2.9 µs | 1 480 µs † |
| DELETE WHERE id=? | **0.30 µs** | — | — | ~45 µs | 4 100 µs † |

**Even RAM-vs-RAM, hazedb leads:** vs SQLite `:memory:` it is ~18× on reads (~23× via `QueryRow`, ~55× via the zero-alloc `QueryRowByPK`), ~4.7× on inserts, ~12× on updates. Allocations per op are 1 (update/delete), 2 (insert, or point read via `QueryRow`), 3 (point read via `Query`), 4 (range scan), and **0 via the prepared `QueryRowByPK`** (typed key + scan-into buffer); bytes/op roughly halved by the packed 32-byte `Value` (below).

**What the gap is — and isn't.** It is mostly the Go *access layer*, and it is **not** the cgo crossing. Evidence: swapping the cgo driver for **pure-Go SQLite** (`modernc.org/sqlite`, no cgo, same `database/sql`) made it *slower*, not faster — read **4.1 µs**, insert **15.3 µs**, update **3.4 µs** vs the cgo build's 2.0 / 1.8 / 1.1 µs. So removing cgo costs speed; the crossing was never the bottleneck. What a Go program actually pays to use SQLite is the `database/sql` layer (reflection, interface conversions, ~24 allocations per read vs hazedb's 3, or 0 via `QueryRowByPK`) on top of a general-purpose engine. hazedb is faster because it skips that layer — typed rows returned in-process, no SQL dispatch per call — which is the project thesis, **not** a claim that its lookup beats SQLite's B-tree. † Write rows for SQLite-disk and Bolt are **not** like-for-like on durability (they fsync/journal to disk; hazedb-mem does not). Allocations/op: hazedb 0–4, SQLite 8–24, Bolt 50–66.

## Transactions (single-table, v1)

| Operation | Time | Allocs |
|---|---:|---:|
| 2-row transfer — `db.Transaction` with two PK-pinned arithmetic UPDATEs | **~1.1 µs** | 19 |

Commit locks only the shards the staged statements touch (not all shards) and writes one `TXN` WAL envelope; ~2× a bare PK update, the price of atomicity + the staged overlay. See *Transactions* in RFC.md.

## Parallel scaling (32 cores)

| Operation | Single | Parallel |
|---|---:|---:|
| SELECT WHERE id=? | 0.15 µs | **0.06 µs** |
| INSERT (memory) | 0.42 µs | **0.10 µs** |
| UPDATE WHERE id=? | 0.11 µs | **0.04 µs** |

## Durability — INSERT, WAL off vs on (relative; overlay FS, not a real-disk SLA)

| | INSERT |
|---|---:|
| WAL off (memory) | 0.42 µs |
| WAL on (born-sealed) | ~0.6 µs |

WAL on adds only the in-RAM buffer append to the write path — the fsync happens off-path on the background seal (1 MiB / ~0.5s), so per-write cost stays sub-µs. There is no per-write-fsync mode to pay for. (The ~0.6 µs carries over from the prior buffered-write path, which has the same write-path shape; re-measure after the born-sealed rewrite if a precise figure is needed.)

## Indexed partition scan, and the LIMIT short-circuit

A feed query `SELECT … WHERE partitionkey=? ORDER BY seq DESC LIMIT n` reads only the matching partition's rows — O(partition), not O(table):

| Scan | Time | Allocs |
|---|---:|---:|
| One partition (~120 rows) of a 10k-row table, `ORDER BY … LIMIT 10` | **~11.6 µs** | 124 |

The partition index earns its keep when `ORDER BY` forces examining the whole matching set. An `ORDER BY … LIMIT n` keeps only the running top-n (a bounded heap, cloning a row only when it makes the cut) and sorts just those n, instead of cloning + sorting every match — ~2× faster on the feed query above; the clone savings grow when the scan order is not adversarial to the sort order. **Without `ORDER BY`, `LIMIT` now short-circuits the scan** (stop at the limit, project under the lock): an unindexed `SELECT id FROM users WHERE age > ? LIMIT 10` over 10k rows (≈4 900 match) is **~0.6 µs / 4 allocs** — versus **~770 µs / 4 932 allocs before the pushdown** (rev. 12), which cloned every matching row before truncating. So the index matters for ordered tail scans; for an unordered `LIMIT`, the short-circuit already makes a full scan cheap.

## Mixed workload — 4 writers + 16 readers, 2 s, WAL on

*Not re-measured at rev. 12; these predate the read-path fast path, so the read percentiles are if anything conservative.*

| | Value |
|---|---:|
| Insert throughput | 0.72 M/sec |
| Read throughput | 7.0 M/sec |
| SELECT WHERE id=? p50 | 0.70 µs |
| SELECT WHERE id=? p90 | 1.3 µs |
| SELECT WHERE id=? p99 | **17 µs** |
| SELECT WHERE id=? p99.9 | 259 µs |
