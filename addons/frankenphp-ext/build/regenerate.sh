#!/usr/bin/env bash
# regenerate.sh — produce the committed C wrappers for the hazedb PHP
# extension. Run whenever hazedb_ext.go changes (new //export_php:function,
# signature change, return-type change). Output: five files into ../, ready to
# git add alongside the hazedb_ext.go change:
#
#   hazedb_ext.c
#   hazedb_ext.h
#   hazedb_ext_arginfo.h
#   hazedb_ext_generated.go
#   hazedb_ext.stub.php
#
# Reuses the cached generator image from Dockerfile.gen. First run ~3 min,
# subsequent runs instant. Clean rebuild: ./regenerate.sh --rebuild-gen-image
#
# Does NOT touch hazedb_ext.go (handwritten) or README.md (the generator wants
# to write its own; we discard it).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
EXT_SRC="$REPO_ROOT/addons/frankenphp-ext"
GEN_IMAGE="hazedb-frankenphp-builder:latest"

DOCKER_BUILD_ARGS=()
if [ "${1:-}" = "--rebuild-gen-image" ]; then
    echo ">>> [pre] --rebuild-gen-image: forcing a clean rebuild of $GEN_IMAGE"
    DOCKER_BUILD_ARGS+=(--no-cache)
fi

echo ">>> [pre] building (or reusing cached) $GEN_IMAGE"
( cd "$SCRIPT_DIR" && MSYS_NO_PATHCONV=1 docker build \
    "${DOCKER_BUILD_ARGS[@]}" \
    -t "$GEN_IMAGE" \
    -f Dockerfile.gen \
    . )

# Stage source under writable /work/ext, generate there, copy the five
# committable files back to /ext on the host at the end — a mid-run failure
# can't leave a half-generated state in the committed tree.
MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$EXT_SRC:/ext" \
    "$GEN_IMAGE" \
    bash -c '
        set -euo pipefail

        mkdir -p /work/ext
        cp /ext/go.mod /work/ext/
        cp /ext/hazedb_ext.go /work/ext/

        # Tighten "// export_php:" -> "//export_php:" so the generator picks up
        # the directives. Source on disk keeps the space form (gofmt-clean).
        sed -i "s|^// export_php:|//export_php:|g" /work/ext/hazedb_ext.go

        cd /work/ext
        /usr/local/bin/frankenphp-gen extension-init hazedb_ext.go

        # RETURN_EMPTY_STRING() -> RETURN_NULL() so PHP null is preserved for
        # nullable returns (build README pitfall #2).
        sed -i \
            -e "s|RETURN_EMPTY_STRING();|RETURN_NULL();|g" \
            -e "s|RETURN_EMPTY_ARRAY();|RETURN_NULL();|g" \
            hazedb_ext.c

        rm -f README.md

        cp hazedb_ext.c \
           hazedb_ext.h \
           hazedb_ext_arginfo.h \
           hazedb_ext_generated.go \
           hazedb_ext.stub.php \
           /ext/
    '

echo
echo "Updated in $EXT_SRC/:"
echo "  hazedb_ext.c hazedb_ext.h hazedb_ext_arginfo.h hazedb_ext_generated.go hazedb_ext.stub.php"
