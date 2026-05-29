<?php
// bench.php — measure PHP-side SELECT throughput against hazedb via the
// in-process cgo extension. This is the REALISTIC PHP number: it includes
// json_encode of the args, the cgo crossing, the SQL statement-cache lookup +
// key copy, JSON arg parse + UUID parse, the point read, and JSON result
// encoding. (The raw in-Go point read is far cheaper; this is what a PHP caller
// actually pays per hazedb_query call.)
//
// Seeds N rows, warms, then times a point-read-by-PK loop. Prints qps + ns/op.

header('Content-Type: text/plain');
set_time_limit(0);

$N     = (int)($_GET['n']    ?? 10000);   // rows seeded
$iters = (int)($_GET['iter'] ?? 1000000); // timed SELECTs

// --- seed ---
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
$ids = [];
for ($i = 0; $i < $N; $i++) {
    $id = hazedb_uuidv7();
    $ids[] = $id;
    hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
                json_encode([$id, "user$i", $i % 100]));
}

// --- correctness sanity before timing ---
$probe = json_decode(hazedb_query('SELECT name, age FROM users WHERE id = ?',
                                  json_encode([$ids[0]])), true);
echo 'sanity=', json_encode($probe['rows'][0] ?? null), "\n";

// --- warm ---
for ($i = 0; $i < 50000; $i++) {
    hazedb_query('SELECT name, age FROM users WHERE id = ?', json_encode([$ids[$i % $N]]));
}

// --- measure ---
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    hazedb_query('SELECT name, age FROM users WHERE id = ?', json_encode([$ids[$i % $N]]));
}
$dt = microtime(true) - $t0;

printf("rows=%d iters=%d time=%.3fs\n", $N, $iters, $dt);
printf("qps=%d  ns/op=%.0f  us/op=%.2f\n", (int)($iters / $dt), $dt * 1e9 / $iters, $dt * 1e6 / $iters);
