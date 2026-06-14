<?php
// Seed both stores with N rows under the SAME deterministic UUIDs so the fetch
// endpoints can compute a valid existing id from a random index, no id-list needed.
header('Content-Type: text/plain');
set_time_limit(0);
const N = 50000;
function id_for(int $i): string {
    $h = md5("row$i");                                   // 32 hex chars
    $v = dechex((hexdec($h[16]) & 0x3) | 0x8);           // valid v4 variant nibble
    return substr($h,0,8).'-'.substr($h,8,4).'-4'.substr($h,13,3).'-'.$v.substr($h,17,3).'-'.substr($h,20,12);
}
// hazedb (process-global store)
hazedb_exec('DROP TABLE t');
hazedb_exec('CREATE TABLE t (id uuid primary key, name text, age int)');
for ($i = 0; $i < N; $i++) hazedb_exec('INSERT INTO t (id, name, age) VALUES (?, ?, ?)', [id_for($i), "n$i", $i % 100]);
// SQLite file + WAL (shared, survives requests; in RAM via page cache after warmup)
@unlink('/tmp/bench.sqlite'); @unlink('/tmp/bench.sqlite-wal'); @unlink('/tmp/bench.sqlite-shm');
$pdo = new PDO('sqlite:/tmp/bench.sqlite');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('PRAGMA journal_mode=WAL'); $pdo->exec('PRAGMA synchronous=NORMAL');
$pdo->exec('CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
$ins = $pdo->prepare('INSERT INTO t (id, name, age) VALUES (?, ?, ?)');
$pdo->beginTransaction();
for ($i = 0; $i < N; $i++) $ins->execute([id_for($i), "n$i", $i % 100]);
$pdo->commit();
// Redis (hash per row), localhost TCP, pipelined seed
$r = new Redis();
$r->connect('127.0.0.1', 6379);
$r->flushAll();
$pipe = $r->multi(Redis::PIPELINE);
for ($i = 0; $i < N; $i++) {
    $pipe->hMSet('row:'.id_for($i), ['name' => "n$i", 'age' => $i % 100]);
    if ($i % 2000 === 1999) { $pipe->exec(); $pipe = $r->multi(Redis::PIPELINE); }
}
$pipe->exec();
// verify
$h = hazedb_fetchall('SELECT name FROM t WHERE id = ?', id_for(123));
$l = $pdo->query("SELECT name FROM t WHERE id = '".id_for(123)."'")->fetch(PDO::FETCH_ASSOC);
$rc = $r->hGetAll('row:'.id_for(123));
printf("seeded N=%d  haze_check=%s  lite_check=%s  redis_check=%s\n",
    N, $h[0]['name'] ?? 'MISS', $l['name'] ?? 'MISS', $rc['name'] ?? 'MISS');
