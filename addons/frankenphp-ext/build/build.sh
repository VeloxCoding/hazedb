#!/usr/bin/env bash
# build.sh — produce ./dist/frankenphp with the hazedb extension + Caddy module
# compiled in.
#
#   Stage 1: reuse the cached generator image if present, else build it
#            (Dockerfile.gen); --rebuild-gen-image forces a clean rebuild.
#   Stage 2: stage the in-repo hazedb core + caddymodule + this ext (with the
#            committed C wrappers), then `xcaddy build` the final binary.
#
# Requires the five generated wrappers in ../ (run regenerate.sh first, commit).
# All three hazedb modules are passed to xcaddy as local --with paths, so no
# published hazedb tag is needed.
#
# Cold (no image, no layer cache): ~10-15 min. Warm: ~1-2 min.
# Build-chain pitfalls: see README.md in this directory.
#
# Usage: ./build.sh [--rebuild-gen-image]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
EXT_SRC="$REPO_ROOT/addons/frankenphp-ext"
DIST_DIR="$SCRIPT_DIR/dist"
GEN_IMAGE="hazedb-frankenphp-builder:latest"

mkdir -p "$DIST_DIR"

# Reuse the gen image if it already exists — `docker build` otherwise contacts
# the registry (base-image auth token) on every run, which fails when Docker Hub
# is flaky (seen: 504 Gateway Timeout) even though nothing needs rebuilding.
# Build only when the image is missing or --rebuild-gen-image forces it.
if [ "${1:-}" = "--rebuild-gen-image" ]; then
    echo ">>> [pre] --rebuild-gen-image: forcing a clean rebuild of $GEN_IMAGE"
    ( cd "$SCRIPT_DIR" && MSYS_NO_PATHCONV=1 docker build --no-cache -t "$GEN_IMAGE" -f Dockerfile.gen . )
elif docker image inspect "$GEN_IMAGE" >/dev/null 2>&1; then
    echo ">>> [pre] reusing existing $GEN_IMAGE (skip build; --rebuild-gen-image to force, e.g. after editing Dockerfile.gen)"
else
    echo ">>> [pre] $GEN_IMAGE not found — building it"
    ( cd "$SCRIPT_DIR" && MSYS_NO_PATHCONV=1 docker build -t "$GEN_IMAGE" -f Dockerfile.gen . )
fi

WRAPPER_FILES=(
    "$EXT_SRC/hazedb_ext.c"
    "$EXT_SRC/hazedb_ext.h"
    "$EXT_SRC/hazedb_ext_arginfo.h"
    "$EXT_SRC/hazedb_ext_generated.go"
    "$EXT_SRC/hazedb_ext.stub.php"
)
missing=()
for f in "${WRAPPER_FILES[@]}"; do
    [ -f "$f" ] || missing+=("$(basename "$f")")
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "ERROR: generated wrappers missing under $EXT_SRC/:" >&2
    for m in "${missing[@]}"; do echo "  - $m" >&2; done
    echo "Run $SCRIPT_DIR/regenerate.sh to produce them, then commit." >&2
    exit 1
fi
echo ">>> [pre] committed wrappers present"

# No apostrophes inside the outer bash -c '...' (README pitfall #3).
MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$REPO_ROOT:/hazedb:ro" \
    -v "$EXT_SRC:/ext-src:ro" \
    -v "$DIST_DIR:/out" \
    -w /work \
    "$GEN_IMAGE" \
    bash -c '
        set -euo pipefail

        echo ">>> [1/2] staging hazedb core + caddymodule + ext under /work"
        mkdir -p /work/hazedb /work/ext
        cp /hazedb/go.mod /hazedb/go.sum /work/hazedb/
        cp /hazedb/*.go /work/hazedb/
        cp -r /hazedb/caddymodule /work/hazedb/caddymodule
        cp -r /hazedb/addons /work/hazedb/addons

        cp /ext-src/go.mod /work/ext/
        cp /ext-src/hazedb_ext.go /work/ext/
        cp /ext-src/hazedb_ext.c /work/ext/
        cp /ext-src/hazedb_ext.h /work/ext/
        cp /ext-src/hazedb_ext_arginfo.h /work/ext/
        cp /ext-src/hazedb_ext_generated.go /work/ext/
        cp /ext-src/hazedb_ext.stub.php /work/ext/

        echo ">>> [2/2] xcaddy build"
        CGO_ENABLED=1 \
        XCADDY_GO_BUILD_FLAGS="-ldflags=-linkmode=external" \
        CGO_CFLAGS="-D_GNU_SOURCE $(php-config --includes)" \
        CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
        xcaddy build \
            --output /out/frankenphp \
            --with github.com/dunglas/frankenphp=/go/src/app \
            --with github.com/dunglas/frankenphp/caddy=/go/src/app/caddy \
            --with github.com/VeloxCoding/hazedb=/work/hazedb \
            --with github.com/VeloxCoding/hazedb/caddymodule=/work/hazedb/caddymodule \
            --with github.com/VeloxCoding/hazedb/addons/frankenphp-ext=/work/ext

        ls -lh /out/frankenphp
    '

echo
echo "Binary: $DIST_DIR/frankenphp"
echo "Smoke:  $SCRIPT_DIR/smoke.sh"
