#!/usr/bin/env sh
# Run hazedb benchmarks with allocation reporting and save the output.
#
# There is no Go on the host — run this inside the golang container:
#
#   docker run --rm -v "$PWD":/src \
#     -v hazedb-gocache:/root/.cache/go-build -v hazedb-gomod:/go/pkg/mod \
#     -w /src golang:1.25 scripts/bench.sh [name-regex] [count]
#
# Examples:
#   scripts/bench.sh                         # all benchmarks, count=6
#   scripts/bench.sh BenchmarkInsert 10      # just inserts, count=10
#
# Compare two result files with benchstat:
#   go install golang.org/x/perf/cmd/benchstat@latest
#   benchstat bench/baseline_m2.txt bench/results-<ts>.txt
set -eu

BENCH="${1:-.}"
COUNT="${2:-6}"
OUT="bench/results-$(date +%Y%m%d-%H%M%S).txt"

mkdir -p bench
echo "bench=$BENCH count=$COUNT -> $OUT"
go test -run='^$' -bench="$BENCH" -benchmem -count="$COUNT" . | tee "$OUT"
