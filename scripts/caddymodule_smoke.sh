#!/usr/bin/env bash
# caddymodule_smoke.sh — prove a clean external consumer of the caddymodule at a
# given tag resolves the MATCHING core (not a stale one). Needs the Go toolchain
# and network; uses GOPROXY=direct so it reads freshly pushed tags immediately.
#
#   scripts/caddymodule_smoke.sh v0.1.12
set -euo pipefail

v="${1:-}"
[[ "$v" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "usage: scripts/caddymodule_smoke.sh vX.Y.Z" >&2; exit 1; }

d="$(mktemp -d)"
trap 'rm -rf "$d"' EXIT
cd "$d"
export GOPROXY=direct GOFLAGS=-mod=mod GOSUMDB=off
go mod init smoketest >/dev/null
go get "github.com/VeloxCoding/hazedb/caddymodule@$v"
cat > main.go <<'EOF'
package main

import _ "github.com/VeloxCoding/hazedb/caddymodule"

func main() {}
EOF
go build ./...
core="$(go list -m -f '{{.Version}}' github.com/VeloxCoding/hazedb)"
echo "caddymodule@$v -> core $core"
[[ "$core" == "$v" ]] || { echo "FAIL: expected core $v, got $core" >&2; exit 1; }
echo "smoke OK"
