# Bench baseline

Reference numbers for the standard harness. Re-measure and update this file
whenever the harness or the binary changes. All later tests (hazedb vs SQLite,
etc.) are compared against these.

**Harness** (see hazedb `CLAUDE.md` → *Standard bench harness*):
- container `--cpuset-cpus=0-8` (9 cores); app pinned to cores 0–6 (7 cores),
  `wrk` pinned to cores 7–8 (2 cores); same container (loopback).
- scripts served from container-native `/app` (NOT the bind-mounted `/work`).
- modes: **w-low** `-t2 -c10 -d5s`, **w-high** `-t32 -c250 -d5s`.

## 2026-06-08 — hello-world (no store)

**Method:** per config+mode, 1 discard (warmup) run then **median of 7** runs
(run-1-discarded rule). cv shown per row to confirm the ~2.5% noise floor.

| config | mode | req/s | p50 | p99 | cv |
|---|---|---|---|---|---|
| static Caddy (no PHP, `respond`) | w-low | 85,367 | 93 µs | 466 µs | 2.1% |
| static Caddy (no PHP, `respond`) | w-high | 294,977 | 547 µs | 5.23 ms | 3.4% |
| PHP classic (`php_server`) | w-low | 14,283 | 665 µs | 1.42 ms | 1.4% |
| PHP classic (`php_server`) | w-high | 19,909 | 10.88 ms | 22.61 ms | 2.7% |
| PHP worker (`frankenphp { worker }`) | w-low | 28,921 | 317 µs | 0.96 ms | 2.2% |
| PHP worker (`frankenphp { worker }`) | w-high | 34,223 | 6.30 ms | 15.19 ms | 1.9% |

Notes:
- Static Caddy is the path ceiling (~9× PHP-worker at w-high). Go-native.
- Worker vs classic gap widens with concurrency: ~1.7–2× here (c250); was ~10×
  at c1000 because classic re-bootstraps PHP per request and queues under load.
- Re-run with `bash /work/run_suite.sh hello_worker.php worker` (and `... hello_classic.php classic`).

## 2026-06-08 — Caddy + hazedb, GET /get random PK (50k rows)

Caddy-native read path: `GET /get` → fused in-process read → flat-object JSON,
wrk-Lua picks a random key per request over 50k rows. PK = `?id=<uuid>`
(QueryRowJSONByPK, 0 alloc); index = `?col=name&val=<v>` (QueryRowJSONByIndex,
1 alloc — the index-bucket copy). Method: 1 discard + median of 7.

| config | mode | req/s | p50 | p99 | cv |
|---|---|---|---|---|---|
| GET PK | w-low | 84,019 | 93 µs | 332 µs | 1.4% |
| GET PK | w-high | 263,387 | 699 µs | 11.49 ms | 8.6% |
| GET index | w-low | 80,124 | 97 µs | 339 µs | 1.8% |
| GET index | w-high | 276,023 | 663 µs | 9.78 ms | 6.7% |

PK ≈ index at the HTTP level (within noise): the per-op gap (PK ~77 ns vs index
~171 ns in Go) is swamped by per-request HTTP cost. Earlier pre-fusion PK run
(81,566 / 274,640) is within noise of the fused PK run above — fusion cut allocs,
not the HTTP-bound throughput.

- **Store is nearly free:** ~95% of the static-Caddy ceiling (85k/295k) — a real
  50k-row PK lookup + JSON encode adds only ~5% over returning a constant string.
- **~3× (w-low) to ~8× (w-high) faster than the PHP-worker path** (28.9k/34.2k).
  Go-native in-process beats the PHP/cgo route decisively.

## Measurement noise (2026-06-08)

10× w-low, same server (one boot, no reboot between runs):

| config | min | max | mean | stdev | spread | cv |
|---|---|---|---|---|---|---|
| static Caddy | 83,304 | 89,674 | 85,742 | 1,989 | 7.4% | **2.3%** |
| PHP worker | 27,836 | 30,955 | 29,304 | 754 | 10.6% | **2.6%** |

- **Noise floor ≈ 2.5% (cv)** for both paths — the PHP/cgo layer adds no extra
  noise. Trust cv (stdev-based), not spread (outlier-sensitive: PHP worker's
  10.6% is two single outliers, run 1 high + run 4 low).
- **Warmup-high in run 1** (both): the first run after boot/warmup runs hot
  (~6% above plateau for static). Systematic, not noise.

**Rules for reading results:** discard the first 1–2 runs as warmup, take the
**median of ≥5 runs**, and treat any config-to-config difference **< ~5%** as
within noise (not real).
