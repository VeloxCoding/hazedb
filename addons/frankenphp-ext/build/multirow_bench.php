<?php
// multirow_bench.php — PHP read throughput for MULTI-ROW results, the case the
// streaming core path (QueryJSON / QueryEach) targets. A no-WHERE SELECT returns
// every row through the scan-stream path; both fetchall shapes are timed:
//   FETCHALL_JSON : hazedb_fetchall_json -> one JSON string (streamed buffer)
//   FETCHALL zval : hazedb_fetchall     -> a PHP array of assoc rows
// Seeds N rows, warms, then times `iter` calls that each return all N rows.

header('Content-Type: text/plain');
set_time_limit(0);

$N    = (int)($_GET['n']    ?? 500);   // rows returned per call
$iter = (int)($_GET['iter'] ?? 3000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, email text, age int)');
for ($i = 0; $i < $N; $i++) {
    hazedb_exec('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)',
        [make_uuid(), "user$i", "user$i@example.com", $i % 100]);
}

$SEL = 'SELECT id, name, email, age FROM users'; // no WHERE -> all N rows, scan-stream

// sanity
$probe = hazedb_fetchall($SEL);
echo 'sanity_rows=', count($probe), "\n";

// --- FETCHALL_JSON (one JSON string) ---
for ($i = 0; $i < 100; $i++) { hazedb_fetchall_json($SEL); }
$t0 = microtime(true);
for ($i = 0; $i < $iter; $i++) { $r = hazedb_fetchall_json($SEL); }
$jsonDt = microtime(true) - $t0;

// --- FETCHALL zval (PHP array of assoc rows) ---
for ($i = 0; $i < 100; $i++) { hazedb_fetchall($SEL); }
$t0 = microtime(true);
for ($i = 0; $i < $iter; $i++) { $r = hazedb_fetchall($SEL); }
$allDt = microtime(true) - $t0;

printf("rows/query=%d iters=%d\n", $N, $iter);
printf("FETCHALL_JSON : %.3fs  %d calls/s  %.1f us/call  %.1f ns/row\n",
    $jsonDt, (int)($iter / $jsonDt), $jsonDt * 1e6 / $iter, $jsonDt * 1e9 / $iter / $N);
printf("FETCHALL zval : %.3fs  %d calls/s  %.1f us/call  %.1f ns/row\n",
    $allDt, (int)($iter / $allDt), $allDt * 1e6 / $iter, $allDt * 1e9 / $iter / $N);
printf("peak_mem=%.1f MB\n", memory_get_peak_usage(true) / 1048576);
