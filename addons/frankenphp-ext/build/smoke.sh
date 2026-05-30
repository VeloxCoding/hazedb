#!/usr/bin/env bash
# smoke.sh — boot dist/frankenphp inside the builder image (which carries the
# matching PHP runtime libs the binary is dynamically linked to), run test.php
# through it, and assert the hazedb cgo path works end-to-end (the inserted
# "alice" row reads back). Run after build.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="$SCRIPT_DIR/dist"
GEN_IMAGE="hazedb-frankenphp-builder:latest"

[ -f "$DIST_DIR/frankenphp" ] || { echo "ERROR: $DIST_DIR/frankenphp not found — run build.sh first" >&2; exit 1; }

MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$DIST_DIR/frankenphp:/usr/local/bin/frankenphp:ro" \
    -v "$SCRIPT_DIR:/work:ro" \
    -w /work \
    "$GEN_IMAGE" \
    bash -c '
        set -eu
        /usr/local/bin/frankenphp run --config /work/Caddyfile --adapter caddyfile &
        PID=$!
        for i in $(seq 1 20); do
            curl -sf http://localhost:8080/test.php >/dev/null 2>&1 && break
            sleep 0.5
        done
        OUT=$(curl -s http://localhost:8080/test.php)
        echo "----- test.php output -----"
        echo "$OUT"
        echo "---------------------------"
        kill $PID 2>/dev/null || true
        # test.php emits quote-free *_ok=yes markers; require all of them.
        ok=1
        for marker in ping=pong exec_ok=yes exec_int_ok=yes fetch_ok=yes \
                      fetch_missing_ok=yes fetch_scalar_ok=yes fetchall_ok=yes fetchall_json_ok=yes; do
            echo "$OUT" | grep -q "^$marker$" || { echo "MISSING: $marker"; ok=0; }
        done
        [ "$ok" = "1" ] && { echo "SMOKE: PASS"; exit 0; }
        echo "SMOKE: FAIL"; exit 1
    '
