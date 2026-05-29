// FrankenPHP-extension addon for hazedb.
//
// Own Go module so the hazedb core stays stdlib + goccy only (same pattern as
// caddymodule/). The extension wraps *DB in PHP-callable functions via
// FrankenPHP's extension generator.
//
// Build: see build/build.sh. The build stages the in-repo core + caddymodule +
// this module and passes all three to xcaddy as local --with paths, so the
// require versions below are placeholders the build overrides — no published
// hazedb tag is needed. Requires the FrankenPHP builder image + xcaddy.
module github.com/VeloxCoding/hazedb/addons/frankenphp-ext

go 1.25

require (
	github.com/VeloxCoding/hazedb v0.1.3
	github.com/VeloxCoding/hazedb/caddymodule v0.0.0-00010101000000-000000000000
	github.com/dunglas/frankenphp v1.12.3
)

// Local build: the build harness passes --with local paths to xcaddy, which
// adds the replaces to its generated main module. Uncomment for a bare
// `go build`/`go mod tidy` of this module against the in-repo source.
//
// replace github.com/VeloxCoding/hazedb => ../..
// replace github.com/VeloxCoding/hazedb/caddymodule => ../../caddymodule
