# hazedb FrankenPHP-extension tooling

Build / smoke tooling for the hazedb PHP extension (source in
[`../`](..)). The recipe mirrors the scopecache addon's proven chain.

```bash
./regenerate.sh   # generate the committed C wrappers from ../hazedb_ext.go
./build.sh        # xcaddy build dist/frankenphp (ext + Caddy module baked in)
./smoke.sh        # boot the binary, run test.php, assert the cgo path works
```

| file | role |
|---|---|
| `Dockerfile.gen` | Cached builder image: PHP-ZTS headers + `frankenphp-gen` (built from master for `extension-init`) + `xcaddy` + the `GEN_STUB_SCRIPT` path. ~3 min cold, instant warm. |
| `regenerate.sh` | Runs `frankenphp-gen extension-init` on `hazedb_ext.go` → the five committed wrappers. Patches `RETURN_EMPTY_STRING`→`RETURN_NULL`. |
| `build.sh` | Stages core + caddymodule + ext, then `xcaddy build` with local `--with` paths for all three + FrankenPHP. |
| `smoke.sh` + `test.php` + `Caddyfile` | Boot `dist/frankenphp`, run `test.php`, assert the inserted row reads back via the cgo path. |
| `dist/` (gitignored) | Build output. |

## Build-chain pitfalls (from the scopecache addon — all still apply)

1. **`// export_php:` (space) on disk, `//export_php:` (tight) at gen time.** gofmt rewrites the tight form; `regenerate.sh` seds it back inside the container.
2. **`RETURN_EMPTY_STRING`→`RETURN_NULL`.** The extgen template collapses a nil Go return into `""` for `?string`; the sed patch restores PHP `null`.
3. **No apostrophes inside the outer `bash -c '...'`.** A stray `'` closes the string mid-script.
4. **PHP `int` params must be Go `int64`, not `C.zend_long`** (the generator silently skips the function otherwise). hazedb's functions take only strings, so N/A here, but keep it in mind for new functions.
5. **cgo can't call `ZVAL_*`/`zend_new_array` macros** — they need static-inline C trampolines. hazedb returns only `?string` (via `zend_string_init`, a function), so no trampolines are needed.
6. **`MSYS_NO_PATHCONV=1`** on Windows/Git-Bash so docker paths aren't rewritten.
7. **cgo xcaddy flags:** `-D_GNU_SOURCE`, `php-config --includes/--ldflags/--libs`, `-linkmode=external` (Go 1.26 internal linker chokes stitching multiple cgo packages). See `build.sh`.
8. **Write paths that retain keys must deep-copy** the zend_string (UAF otherwise). hazedb copies the SQL string (it becomes a `stmtCache` key); `args_json` is read synchronously and aliased.

## Versioning

Both the gen image and the build pin `dunglas/frankenphp:1.12-builder-php8`.
Bump the tag in `Dockerfile.gen` for a newer FrankenPHP/PHP/Go, then
`./regenerate.sh --rebuild-gen-image`.
