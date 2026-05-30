<?php
// hazedb_insert_bench.php — INSERT throughput from PHP into hazedb (in-memory,
// no WAL in the bench Caddyfile). Mirrors the SQLite insert bench: same table,
// same 3-column rows, unique UUID PK per row, ids pre-generated outside the
// timed loop. The per-insert arg prep (json_encode of [id,name,age]) IS inside
// the timed loop, because that is the real cost an app pays for a hazedb
// multi-value insert (the string-only PHP surface needs the JSON array form).
//
// The *DB is process-wide (shared across requests), so reset the table first.

header('Content-Type: text/plain');
set_time_limit(0);

$M = (int)($_GET['m'] ?? 500000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

$SQL = 'INSERT INTO users (id, name, age) VALUES (?, ?, ?)';

// Pre-generate the known PKs outside the timed loop (the app already has the id).
$ids = [];
for ($i = 0; $i < $M; $i++) { $ids[] = make_uuid(); }

// --- JSON-args path: hazedb_exec(sql, json_encode([...])) ---
hazedb_exec('DROP TABLE users', '');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
echo 'sanity_json=', hazedb_exec($SQL, json_encode([$ids[0], 'user0', 0])), "\n";
$t0 = microtime(true);
for ($i = 1; $i < $M; $i++) {
    hazedb_exec($SQL, json_encode([$ids[$i], "user$i", $i % 100]));
}
$jsonDt = microtime(true) - $t0;

// --- native-array path: hazedb_exec_arr(sql, [...]) — no json_encode ---
hazedb_exec('DROP TABLE users', '');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', '');
echo 'sanity_arr=', hazedb_exec_arr($SQL, [$ids[0], 'user0', 0]), "\n";
$t0 = microtime(true);
for ($i = 1; $i < $M; $i++) {
    hazedb_exec_arr($SQL, [$ids[$i], "user$i", $i % 100]);
}
$arrDt = microtime(true) - $t0;

$n = $M - 1;
printf("hazedb inserts (in-memory): rows=%d\n", $n);
printf("JSON_ARGS : time=%.3fs  qps=%d  ns/op=%.0f\n", $jsonDt, (int)($n / $jsonDt), $jsonDt * 1e9 / $n);
printf("ARRAY_ARGS: time=%.3fs  qps=%d  ns/op=%.0f\n", $arrDt,  (int)($n / $arrDt),  $arrDt  * 1e9 / $n);
printf("ARRAY vs JSON: %.2fx faster, saves %.0f ns/op\n", $jsonDt / $arrDt, ($jsonDt - $arrDt) * 1e9 / $n);
