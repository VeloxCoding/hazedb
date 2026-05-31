#!/usr/bin/env bash
# run_bench.sh — boot a given frankenphp binary inside the builder image and run
# multirow_bench.php through it, printing the result. Used to A/B two binaries
# (streaming vs pre-streaming) against the same bench script.
#
#   ./run_bench.sh /abs/path/to/frankenphp
set -euo pipefail

BIN="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEN_IMAGE="hazedb-frankenphp-builder:latest"

MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$BIN:/usr/local/bin/frankenphp:ro" \
    -v "$SCRIPT_DIR:/work:ro" \
    -w /work \
    "$GEN_IMAGE" \
    bash -c '
        set -eu
        /usr/local/bin/frankenphp run --config /work/Caddyfile --adapter caddyfile >/tmp/srv.log 2>&1 &
        PID=$!
        for i in $(seq 1 30); do
            curl -sf http://localhost:8080/multirow_bench.php >/dev/null 2>&1 && break
            sleep 0.5
        done
        curl -s http://localhost:8080/multirow_bench.php
        kill $PID 2>/dev/null || true
    '
