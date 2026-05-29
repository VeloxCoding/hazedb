#!/usr/bin/env sh
# Vet + test the whole module. Pass "race" to enable the race detector.
#
# Run inside the golang container (no Go on the host):
#
#   docker run --rm -v "$PWD":/src \
#     -v hazedb-gocache:/root/.cache/go-build -v hazedb-gomod:/go/pkg/mod \
#     -w /src golang:1.25 scripts/test.sh [race]
set -eu

go vet ./...

if [ "${1:-}" = "race" ]; then
  go test -race -count=1 ./...
else
  go test -count=1 ./...
fi
