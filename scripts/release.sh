#!/usr/bin/env bash
# release.sh — cut an ALIGNED hazedb release: ONE commit that BOTH module tags
# point at, so a checkout of either tag has a self-consistent caddymodule/go.mod.
# Run from the repo root on a clean `main`, with the Go toolchain and push
# credentials available.
#
#   scripts/release.sh v0.1.14
#
# The two-module problem. caddymodule is a separate Go module; external consumers
# (`go get`, xcaddy) resolve it by its own `caddymodule/vX.Y.Z` tag, and its
# go.mod must require a PUBLISHED core tag — else they silently build against
# whatever stale core the require points at. That creates a chicken-and-egg: the
# core tag must exist before `go mod tidy` can fetch its go.sum, yet we want BOTH
# tags on the single commit that already carries the aligned go.mod + go.sum.
#
# The fix. Commit the caddymodule bump, publish the core tag so it is fetchable,
# refresh go.sum against it, fold that into the same commit, then MOVE the core
# tag onto the final aligned commit (force-push). Moving it is checksum-safe:
# caddymodule/ is a NESTED module, so it is excluded from the CORE module's zip
# (verified — the core zip carries zero caddymodule/ entries). Edits confined to
# caddymodule/ therefore leave the core module's bytes — and its h1 checksum —
# identical, so no consumer can observe a different hash across the move.
#
# The OLD script tagged the core BEFORE the bump commit, so the root tag landed
# one commit behind: a checkout of `vX.Y.Z` had caddymodule/go.mod still pointing
# at the PREVIOUS core. The published caddymodule tag was fine, but the root
# source tree was misleading. This script makes both tags land on one commit and
# asserts it before declaring success.
set -euo pipefail

v="${1:-}"
[[ "$v" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "usage: scripts/release.sh vX.Y.Z" >&2; exit 1; }

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

[[ -z "$(git status --porcelain)" ]] || { echo "working tree not clean" >&2; exit 1; }
[[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || { echo "not on main" >&2; exit 1; }
git rev-parse -q --verify "refs/tags/$v" >/dev/null && { echo "tag $v already exists" >&2; exit 1; }
git rev-parse -q --verify "refs/tags/caddymodule/$v" >/dev/null && { echo "tag caddymodule/$v already exists" >&2; exit 1; }

# 1. Commit the caddymodule core-require bump. This commit is where BOTH tags
#    ultimately land; go.sum is refreshed onto it in step 3.
( cd caddymodule && go mod edit -require="github.com/VeloxCoding/hazedb@$v" )
git add caddymodule/go.mod
git commit -m "release $v: align caddymodule core require"

# 2. Publish the core tag so direct git / the proxy can serve it for the tidy in
#    step 3. Pointed at the aligned commit from the start.
git tag -a "$v" -m "$v"
git push origin "$v"

# 3. Refresh the caddymodule's go.sum against the just-published core, fold it
#    into the same commit, and re-point the core tag at the result. The force is
#    checksum-safe (see header): caddymodule-only edits do not change the core
#    module's zip.
( cd caddymodule
  GOPROXY=direct go mod tidy
  go build ./... && go vet ./... )
if ! git diff --quiet -- caddymodule/go.sum; then
  git add caddymodule/go.sum
  git commit --amend --no-edit
  git tag -f -a "$v" -m "$v"
  git push -f origin "$v"
fi

# 4. Tag the caddymodule submodule at the now-aligned commit and push everything.
git tag -a "caddymodule/$v" -m "caddymodule $v"
git push origin main "caddymodule/$v"

# 5. Post-conditions — fail loudly if the alignment regressed (the bug this
#    script exists to prevent): both tags on one commit, and that commit's
#    caddymodule/go.mod requires exactly this core.
core_sha="$(git rev-parse "$v^{commit}")"
cad_sha="$(git rev-parse "caddymodule/$v^{commit}")"
[[ "$core_sha" == "$cad_sha" ]] || { echo "FAIL: $v ($core_sha) and caddymodule/$v ($cad_sha) point to different commits" >&2; exit 1; }
req="$(git show "$v:caddymodule/go.mod" | sed -nE 's#^[[:space:]]*github.com/VeloxCoding/hazedb (v[0-9].*)$#\1#p')"
[[ "$req" == "$v" ]] || { echo "FAIL: caddymodule/go.mod at tag $v requires $req, expected $v" >&2; exit 1; }

# 6. Prove a clean external install resolves the matching core.
"$root/scripts/caddymodule_smoke.sh" "$v"
echo "released $v (core) + caddymodule/$v — both tags on ${core_sha:0:12}, caddymodule requires $req"
