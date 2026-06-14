<?php
// Seed hazedb (process-global store) + SQLite file with the SAME 50k
// deterministic UUIDs the fetch workers compute. No Redis. Run once before wrk.
header('Content-Type: text/plain');
set_time_limit(0);
const N = 50000;
function id_for(int $i): string {
    $h = md5("row$i");
    $v = dechex((hexdec($h[16]) & 0x3) | 0x8);
    return substr($h, 0, 8).'-'.substr($h, 8, 4).'-4'.substr($h, 13, 3).'-'.$v.substr($h, 17, 3).'-'.substr($h, 20, 12);
}
// hazedb (process-global)
hazedb_exec('DROP TABLE t');
hazedb_exec('CREATE TABLE t (id uuid primary key, name text, age int)');
for ($i = 0; $i < N; $i++) hazedb_exec('INSERT INTO t (id, name, age) VALUES (?, ?, ?)', [id_for($i), "n$i", $i % 100]);
// SQLite file (WAL, in RAM via page cache after warmup)
@unlink('/tmp/bench.sqlite'); @unlink('/tmp/bench.sqlite-wal'); @unlink('/tmp/bench.sqlite-shm');
$pdo = new PDO('sqlite:/tmp/bench.sqlite');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('PRAGMA journal_mode=WAL'); $pdo->exec('PRAGMA synchronous=NORMAL');
$pdo->exec('CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
$ins = $pdo->prepare('INSERT INTO t (id, name, age) VALUES (?, ?, ?)');
$pdo->beginTransaction();
for ($i = 0; $i < N; $i++) $ins->execute([id_for($i), "n$i", $i % 100]);
$pdo->commit();
// verify both
$h = hazedb_fetch('SELECT name FROM t WHERE id = ?', id_for(123));
$l = $pdo->query("SELECT name FROM t WHERE id = '".id_for(123)."'")->fetch(PDO::FETCH_ASSOC);
printf("seeded N=%d  haze_check=%s  lite_check=%s\n", N, $h ? 'OK' : 'MISS', $l['name'] ?? 'MISS');
