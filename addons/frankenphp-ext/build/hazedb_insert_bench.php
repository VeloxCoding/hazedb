<?php
// hazedb_insert_bench.php — INSERT throughput from PHP into hazedb (in-memory,
// no WAL in the bench Caddyfile). Same table/rows as the SQLite insert bench,
// unique UUID PK per row, ids pre-generated outside the timed loop. Args are a
// native PHP array (hazedb_exec(sql, [...])). The *DB is process-wide, so reset
// the table first.

header('Content-Type: text/plain');
set_time_limit(0);

$M = (int)($_GET['m'] ?? 200000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

$SQL = 'INSERT INTO users (id, name, age) VALUES (?, ?, ?)';
$ids = [];
for ($i = 0; $i < $M; $i++) { $ids[] = make_uuid(); }

hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');
echo 'sanity=', hazedb_exec($SQL, [$ids[0], 'user0', 0]), "\n";   // 1

$t0 = microtime(true);
for ($i = 1; $i < $M; $i++) {
    hazedb_exec($SQL, [$ids[$i], "user$i", $i % 100]);
}
$dt = microtime(true) - $t0;
$n = $M - 1;
printf("hazedb inserts (in-memory): rows=%d  time=%.3fs  qps=%d  ns/op=%.0f\n",
       $n, $dt, (int)($n / $dt), $dt * 1e9 / $n);
