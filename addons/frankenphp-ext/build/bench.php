<?php
// bench.php — PHP read throughput against hazedb via the in-process cgo
// extension, point-read by PK (the id is known — it arrived in the request).
// Two read shapes:
//   FETCH          : hazedb_fetch -> a usable assoc row (['name'=>..,'age'=>..])
//   FETCHALL_JSON  : hazedb_fetchall_json -> a JSON string for pass-through
//                    (forward to an HTTP response / cache, no PHP decode)
// Seeds N rows, warms, then times each loop. Prints qps + ns/op.

header('Content-Type: text/plain');
set_time_limit(0);

$N     = (int)($_GET['n']    ?? 10000);
$iters = (int)($_GET['iter'] ?? 1000000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

// --- seed (writes take a native PHP array of args) ---
hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');
$ids = [];
for ($i = 0; $i < $N; $i++) {
    $id = make_uuid();
    $ids[] = $id;
    hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)', [$id, "user$i", $i % 100]);
}

$SEL = 'SELECT name, age FROM users WHERE id = ?';
echo 'sanity=', json_encode(hazedb_fetch($SEL, [$ids[0]])), "\n";

// --- FETCH (array arg [$id]) ---
for ($i = 0; $i < 50000; $i++) { hazedb_fetch($SEL, [$ids[$i % $N]]); }
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) { $r = hazedb_fetch($SEL, [$ids[$i % $N]]); }
$arrDt = microtime(true) - $t0;

// --- FETCH (scalar arg $id — fast path) ---
for ($i = 0; $i < 50000; $i++) { hazedb_fetch($SEL, $ids[$i % $N]); }
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) { $r = hazedb_fetch($SEL, $ids[$i % $N]); }
$scDt = microtime(true) - $t0;

// --- FETCHALL_JSON (scalar arg): JSON string for pass-through ---
for ($i = 0; $i < 50000; $i++) { hazedb_fetchall_json($SEL, $ids[$i % $N]); }
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) { $r = hazedb_fetchall_json($SEL, $ids[$i % $N]); }
$jsonDt = microtime(true) - $t0;

printf("rows=%d iters=%d\n", $N, $iters);
printf("FETCH array arg [\$id]   : time=%.3fs  qps=%d  ns/op=%.0f\n", $arrDt, (int)($iters / $arrDt), $arrDt * 1e9 / $iters);
printf("FETCH scalar arg \$id    : time=%.3fs  qps=%d  ns/op=%.0f\n", $scDt, (int)($iters / $scDt), $scDt * 1e9 / $iters);
printf("FETCHALL_JSON (scalar)  : time=%.3fs  qps=%d  ns/op=%.0f\n", $jsonDt, (int)($iters / $jsonDt), $jsonDt * 1e9 / $iters);
printf("scalar vs array: saves %.0f ns/op\n", ($arrDt - $scDt) * 1e9 / $iters);
