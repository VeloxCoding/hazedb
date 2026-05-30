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

// App-supplied PK (canonical UUID string), as a real PHP app would mint it.
function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);     // version 4
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);     // RFC 4122 variant
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

// --- seed (writes use the JSON-array arg form: multiple typed values) ---
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
$ids = [];
for ($i = 0; $i < $N; $i++) {
    $id = make_uuid();
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

// --- measure A: id direct, raw JSON string returned (no PHP decode) ---
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    hazedb_query('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]);
}
$rawDt = microtime(true) - $t0;

// --- measure B: same call + json_decode to a PHP array (the realistic full cost) ---
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    $r = json_decode(hazedb_query('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]), true);
}
$decDt = microtime(true) - $t0;

// --- measure C: hazedb_query_arr — native PHP array, no JSON encode/decode ---
for ($i = 0; $i < 50000; $i++) { hazedb_query_arr('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]); }
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    $r = hazedb_query_arr('SELECT name, age FROM users WHERE id = ?', $ids[$i % $N]);
}
$arrDt = microtime(true) - $t0;

printf("rows=%d iters=%d\n", $N, $iters);
printf("RAW (json string, no decode) : time=%.3fs  qps=%d  ns/op=%.0f\n", $rawDt, (int)($iters / $rawDt), $rawDt * 1e9 / $iters);
printf("DECODE (json + json_decode)  : time=%.3fs  qps=%d  ns/op=%.0f\n", $decDt, (int)($iters / $decDt), $decDt * 1e9 / $iters);
printf("ARR (hazedb_query_arr)       : time=%.3fs  qps=%d  ns/op=%.0f\n", $arrDt, (int)($iters / $arrDt), $arrDt * 1e9 / $iters);
printf("ARR vs DECODE: %.2fx faster, saves %.0f ns/op\n", $decDt / $arrDt, ($decDt - $arrDt) * 1e9 / $iters);
