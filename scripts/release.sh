#!/usr/bin/env bash
# release.sh — cut an ALIGNED hazedb release: tag the core module, bump the
# caddymodule's core require to match, tag the caddymodule submodule, push all,
# and smoke-test a clean external install. Run from the repo root on a clean
# `main`, with the Go toolchain and push credentials available.
#
#   scripts/release.sh v0.1.12
#
# Why both tags: caddymodule is a separate Go module. External consumers
# (`go get`, xcaddy) resolve it by its own `caddymodule/vX.Y.Z` tag, and its
# go.mod must require a PUBLISHED core tag — otherwise they silently get whatever
# stale core the require points at (the bug this script exists to prevent). The
# core is tagged first so the module proxy can serve it when the caddymodule is
# tidied against it.
set -euo pipefail

v="${1:-}"
[[ "$v" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "usage: scripts/release.sh vX.Y.Z" >&2; exit 1; }

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

[[ -z "$(git status --porcelain)" ]] || { echo "working tree not clean" >&2; exit 1; }
[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "not on main" >&2; exit 1; }
git rev-parse -q --verify "refs/tags/$v" >/dev/null && { echo "tag $v already exists" >&2; exit 1; }

# 1. Tag + push the core module first, so the proxy can serve it for step 2.
git tag -a "$v" -m "$v"
git push origin "$v"

# 2. Point the caddymodule at the just-published core tag and refresh go.sum.
( cd caddymodule
  go mod edit -require="github.com/VeloxCoding/hazedb@$v"
  GOPROXY=direct go mod tidy
  go build ./... && go vet ./... )

# 3. Commit the bump, then tag + push the caddymodule submodule.
git add caddymodule/go.mod caddymodule/go.sum
git commit -m "release $v: align caddymodule core require"
git tag -a "caddymodule/$v" -m "caddymodule $v"
git push origin main "caddymodule/$v"

# 4. Prove a clean external install resolves the matching core.
"$root/scripts/caddymodule_smoke.sh" "$v"
echo "released $v (core) + caddymodule/$v"
