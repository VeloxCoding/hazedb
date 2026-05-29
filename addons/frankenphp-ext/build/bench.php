<?php
// bench.php — PHP SELECT throughput against hazedb via the in-process cgo
// extension, the LEAN way: the id is known (as in real life — you clicked a
// row, the id arrives in the request), passed DIRECTLY as the arg (no
// json_encode), and the result is NOT decoded. This is the realistic hot-read
// cost: cgo crossing + SQL stmtCache lookup + UUID parse + the point read +
// JSON result build. (Inserts still use the JSON-array arg form for multi-arg.)
//
// Seeds N rows, warms, then times a point-read-by-PK loop. Prints qps + ns/op.

header('Content-Type: text/plain');
set_time_limit(0);

$N     = (int)($_GET['n']    ?? 10000);
$iters = (int)($_GET['iter'] ?? 1000000);

// --- seed (writes use the JSON-array arg form: multiple typed values) ---
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
$ids = [];
for ($i = 0; $i < $N; $i++) {
    $id = hazedb_uuidv7();
    $ids[] = $id;
    hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
                json_encode([$id, "user$i", $i % 100]));
}

// --- sanity (raw JSON string, no decode) ---
echo 'sanity=', hazedb_query('SELECT name, age FROM users WHERE id = ?', $ids[0]), "\n";

// --- warm: id passed DIRECTLY, no json_encode ---
for ($i = 0; $i < 50000; $i++) {
    hazedb_query('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]);
}

// --- measure: id direct, no decode ---
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    hazedb_query('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]);
}
$dt = microtime(true) - $t0;

printf("rows=%d iters=%d time=%.3fs\n", $N, $iters, $dt);
printf("qps=%d  ns/op=%.0f  us/op=%.2f\n", (int)($iters / $dt), $dt * 1e9 / $iters, $dt * 1e6 / $iters);
