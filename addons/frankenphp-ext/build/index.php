<?php
// index.php — the wrk target: one classic per-request page load that inserts a
// row with a random indexed name (the PK is auto-generated UUIDv7). Exercises
// the full HTTP -> FrankenPHP (per-request) -> hazedb concurrent insert path,
// with the name index maintained asynchronously by the background merger.
hazedb_exec('INSERT INTO loadtest (name) VALUES (?)', ['name' . random_int(0, 999)]);
echo 'ok';
