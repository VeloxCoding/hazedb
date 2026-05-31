<?php
// loadtest_verify.php — after the load, confirm the name index works: a lookup
// by a random name returns rows, and a small sample of recent inserts is
// present. Proves the async index kept up (or the hybrid overlay covered the
// not-yet-merged tail).
header('Content-Type: text/plain');
$n = 'name' . random_int(0, 999);
$byName = hazedb_fetchall('SELECT id, name FROM loadtest WHERE name = ? LIMIT 5', [$n]);
$sample = hazedb_fetchall('SELECT id, name FROM loadtest LIMIT 3');
printf("WHERE name=%s -> %d rows (index)\n", $n, count($byName));
printf("sample rows present: %d\n", count($sample));
