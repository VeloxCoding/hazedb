# hazedb FrankenPHP-extension tooling

Build / smoke tooling for the hazedb PHP extension (source in
[`../`](..)). The recipe mirrors the scopecache addon's proven chain.

```bash
./regenerate.sh   # generate the committed C wrappers from ../hazedb_ext.go
./build.sh        # xcaddy build dist/frankenphp (ext + Caddy module baked in)
./smoke.sh        # boot the binary, run test.php, assert the cgo path works
```

## Adding a new PHP function (the workflow)

The whole loop is: **edit one Go file → regenerate → build → smoke.**

1. **Write the Go function in [`../hazedb_ext.go`](../hazedb_ext.go)** with an
   `// export_php:` directive. Keep the space after `//` (gofmt-clean — the
   build tightens it to `//export_php:` inside the container, pitfall #1):

   ```go
   // export_php:function hazedb_count(string $sql): ?string
   func hazedb_count(sql *C.zend_string) unsafe.Pointer {
       db := defaultSlot.Load()
       if db == nil {
           return nil // no Caddy module registered a DB -> PHP null
       }
       // ... call db.Query / db.Exec, build the response bytes, then:
       return phpStringFromBytes(body)
   }
   ```

   Signature + cgo rules (detail in the pitfalls below):
   - PHP `string` param → Go `*C.zend_string`; PHP `int` param → Go **`int64`**
     (never `C.zend_long` — the generator silently skips the function, #4).
   - Return `?string` → `unsafe.Pointer` from `phpStringFromBytes` (nil → PHP
     `null`). Returning a PHP array would need C trampolines (#5) — avoid; return
     JSON (or a scalar) as a string.
   - Read a param with `zendStringView` (zero-copy alias, read-only). If the
     bytes are **retained past the call** (e.g. become a map key), deep-copy with
     `zendStringCopy` — that's why hazedb copies the SQL string (#8).
   - Resolve the shared `*DB` via `defaultSlot.Load()`; treat nil as "no Caddy
     module loaded".

2. **Regenerate the wrappers:** `./regenerate.sh` rewrites the five
   `hazedb_ext.*` files from the directives. `git add` them together with your
   `hazedb_ext.go` change. Needed whenever a **signature** changes (new function,
   added/renamed param, changed return type). A body-only logic change needs
   only step 3.

3. **Build:** `./build.sh` → `dist/frankenphp`.

4. **Smoke:** add a call to [`test.php`](test.php), run `./smoke.sh`. Bench with
   [`bench.php`](bench.php) if it's a hot path.

5. **Use it:** the function is callable from any PHP served by the binary.

Starting a *brand-new* extension (not just a new function) is the same chain
with a fresh `addons/<name>/` directory + `go.mod`; copy this `build/` dir and
swap the `hazedb_ext` / module names.

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
