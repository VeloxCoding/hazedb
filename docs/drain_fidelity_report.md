# Drain fidelity report — does SQLite hold exactly what the engine holds?

**Date:** 2026-05-31
**Question asked:** after heavy insert/update/delete traffic on a table, is every
row found back in the SQLite mirror *exactly* — same rows, same values, same types?

**Short answer:** yes — but only after fixing **two real bugs** the tests
uncovered. Before the fixes, the mirror silently diverged from the engine in two
distinct ways. After the fixes, all variants pass three-way exact equality, under
the race detector.

The work lives in [`drain_fidelity_test.go`](../drain_fidelity_test.go) (new,
reusable). The fixes are in [`drain.go`](../drain.go) and [`db.go`](../db.go).

---

## Method

Each test drives a randomized but **seeded** (reproducible) workload through a
*reference model* kept in lockstep in Go: a `map[pk]row` updated on every
insert/update/delete exactly as the engine should end up. Then it seals the WAL
(`rotate`) and drains (`drainOnce`), and asserts **three-way equality**:

```
reference model  ==  engine (in-memory SELECT)  ==  SQLite mirror (direct read)
```

The reference model is the independent oracle. An engine-vs-mirror check alone
would miss a bug that corrupts *both* the same way; comparing both against an
independently-maintained expectation closes that gap. Every cell is canonicalised
to the same string on both sides (bytes/UUID → hex, bool → 0/1, NULL → `NULL`)
and compared keyed by primary key.

The table is deliberately wide — one column of every type, with nullability:

| column | type | notes |
|---|---|---|
| `id` | UUID | primary key |
| `n` | INT | non-null; values span negative … large |
| `label` | STRING | nullable; empty, unicode, separator/quote chars |
| `blob` | BYTES | nullable; zero-length and arbitrary bytes incl. `0x00` |
| `flag` | BOOL | nullable |
| `ref` | UUID | nullable, non-PK |

---

## Variants and results

All seven pass after the fixes (`go test -run TestFidelity`, ~0.55s; also clean
under `-race`). Counts are the actual workload each variant exercised:

| # | variant | what it stresses | workload |
|---|---|---|---|
| 1 | `InsertOnlyVolume` | bulk insert fidelity | 5 000 inserts |
| 2 | `RandomMix` | the general case | 4 355 ins / 2 431 upd / 1 214 del → 3 141 live |
| 3 | `UpdateChurn` | compaction to *current* state | 1 000 rows, 10 000 updates |
| 4 | `AllTypesAndNulls` | every type + a NULL in every nullable column | 600 ins / 200 upd |
| 5 | `Transactions` | multi-mutation TXN envelopes drain faithfully | 300 txns → 679 ins / 299 upd |
| 6 | `IncrementalDrain` | mirror assembled across ~20 sealed segments | 6 529 ins / 3 667 upd / 1 804 del → 4 725 live |
| 7 | `RecoveryRoundTrip` | close → reopen from SQLite, then keep writing | 2 173→ restart →3 836 ins total, 2 807 live |

Variant 3 specifically proves the mirror never keeps a stale intermediate: 10 000
updates over 1 000 rows must collapse to each row's final value (the drain replays
faithfully, and SQLite's `INSERT OR REPLACE` overwrites). Variant 7 proves the
full durability loop: state survives a restart through SQLite, and the engine
keeps mirroring correctly afterwards.

---

## Bugs found and fixed

### Bug 1 — empty BYTES became NULL in the mirror (fidelity)

A zero-length but **non-NULL** `BYTES` value was stored in SQLite as `NULL`,
erasing the empty-bytes-vs-NULL distinction. On recovery it would come back as
NULL — a silent value change.

**Cause:** `valueToArg` copied byte values with `append([]byte(nil), b...)`.
Appending nothing to a `nil` slice returns `nil`, and `database/sql` writes a
`nil []byte` as SQL `NULL`. A non-NULL empty value must reach the driver as a
non-nil, zero-length slice.

**Fix** ([`drain.go`](../drain.go), `valueToArg`): use `make([]byte, len(b))` +
`copy`, which is non-nil even when empty, so SQLite stores an empty BLOB. (Empty
TEXT was never affected — `""` is a non-nil string. UUIDs are always 16 bytes.)

### Bug 2 — post-restart writes were never mirrored, and lost on next recovery (durability)

After a clean restart, new writes did **not** reach the SQLite mirror at all, and
were then **lost** on the following recovery (which reloads state from the mirror).

**Cause:** the drain cursor `last_drained_segment` is a WAL segment number,
compared as `segment <= cursor → already drained, skip`. But segment numbers are
**not monotonic across restarts**: the drain deletes the segments it consumes, and
`wal.close()` deletes the empty trailing active segment, so the WAL directory can
be *empty* at reopen — and the segment counter restarts at 1. With a persisted
cursor of, say, 1, the fresh segment 1 holding all post-restart writes satisfies
`1 <= 1` and the drain skips it forever. The next recovery loads from the mirror
(missing those writes) and deletes/ignores that segment → the writes are gone.

This is the more serious of the two: it is silent data loss across two restarts,
not just a single value mismatch.

**Fix** ([`db.go`](../db.go), SQLite-recovery branch): before opening the new
active segment on recovery, lift the WAL's segment counter to at least
`lastDrained`, so every new segment is numbered strictly above the cursor and can
never be mistaken for already-drained. The undrained tail (segments above the
cursor) is still replayed into memory and still drains normally.

---

## Type-fidelity matrix (verified end to end)

| hazedb type | SQLite storage | round-trips exactly |
|---|---|---|
| INT (incl. negative/large) | INTEGER | ✓ |
| STRING (empty, unicode, quotes, tab, `\|`) | TEXT | ✓ |
| BYTES (incl. zero-length, `0x00`) | BLOB | ✓ *(after Bug 1 fix)* |
| BOOL | INTEGER 0/1 | ✓ |
| UUID (PK and non-PK) | BLOB (16 raw bytes) | ✓ |
| NULL in any nullable column | NULL | ✓ |

---

## How to run

```bash
# in the golang:1.25 container, repo root mounted at /app
go test -run TestFidelity -v ./...      # the fidelity suite
go test -race -run TestFidelity ./...   # under the race detector
go test ./...                           # full suite stays green
```

---

## Status / recommendation

- Two genuine bugs fixed; both are core correctness/durability issues, not test
  artefacts. Changes touch `drain.go`, `db.go`, plus the new test file.
- Full suite + `-race` are green.
- **Not yet committed or released** — these are real durability fixes and warrant
  a patch release (v0.1.10) once reviewed. Suggested commit: the two fixes + the
  fidelity test as one change.
