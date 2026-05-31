#!/usr/bin/env bash
# load_wrk.sh — concurrent HTTP loadtest of the read endpoint against a given
# frankenphp binary. Boots the binary in the builder image, seeds the table,
# warms, then runs wrk against /loadtest_read.php. Used to A/B two binaries
# (streaming vs pre-streaming) under concurrency.
#
#   ./load_wrk.sh /abs/path/to/frankenphp [duration_s]
#
# Server + wrk share the container's CPUs, so absolute req/s is lower than a
# split host/load setup — but both binaries run identically, so the A/B ratio
# is fair.
set -euo pipefail

BIN="$1"
DUR="${2:-10}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEN_IMAGE="hazedb-frankenphp-builder:latest"

MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$BIN:/usr/local/bin/frankenphp:ro" \
    -v "$SCRIPT_DIR:/work:ro" \
    -w /work \
    "$GEN_IMAGE" \
    bash -c '
        set -eu
        apt-get update -qq >/dev/null 2>&1
        apt-get install -y -qq wrk >/dev/null 2>&1
        /usr/local/bin/frankenphp run --config /work/Caddyfile --adapter caddyfile >/tmp/srv.log 2>&1 &
        PID=$!
        for i in $(seq 1 40); do
            curl -sf http://localhost:8080/loadtest_read_setup.php >/dev/null 2>&1 && break
            sleep 0.5
        done
        echo -n "seed: "; curl -s http://localhost:8080/loadtest_read_setup.php
        for i in $(seq 1 50); do curl -s http://localhost:8080/loadtest_read.php >/dev/null; done
        echo "--- wrk -t4 -c50 -d'"$DUR"'s ---"
        wrk -t4 -c50 -d'"$DUR"'s --latency http://localhost:8080/loadtest_read.php
        kill $PID 2>/dev/null || true
    '
