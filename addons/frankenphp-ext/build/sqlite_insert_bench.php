<?php
// sqlite_insert_bench.php — INSERT throughput from PHP into SQLite :memory:,
// matching hazedb_insert_bench.php: same table, same 3-column rows, unique
// UUID-string PK, ids pre-generated outside the timed loop, arg list built
// inside the loop. Two modes:
//   AUTOCOMMIT : each INSERT is its own implicit transaction — the fair match
//                to hazedb, which applies every hazedb_exec independently.
//   BATCHED    : all inserts wrapped in one BEGIN/COMMIT — SQLite's best case
//                (shown for reference; hazedb exposes no PHP-level transaction).

$M = (int)($argv[1] ?? 500000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

function run(int $M, bool $batched): array {
    $pdo = new PDO('sqlite::memory:');
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
    $ins = $pdo->prepare('INSERT INTO users (id, name, age) VALUES (?, ?, ?)');

    $ids = [];
    for ($i = 0; $i < $M; $i++) { $ids[] = make_uuid(); }

    $t0 = microtime(true);
    if ($batched) $pdo->beginTransaction();
    for ($i = 0; $i < $M; $i++) {
        $ins->execute([$ids[$i], "user$i", $i % 100]);
    }
    if ($batched) $pdo->commit();
    $dt = microtime(true) - $t0;
    return [$dt, (int)($M / $dt), $dt * 1e9 / $M];
}

printf("php=%s  rows=%d\n", PHP_VERSION, $M);
[$dt, $qps, $ns] = run($M, false);
printf("AUTOCOMMIT : time=%.3fs  qps=%d  ns/op=%.0f\n", $dt, $qps, $ns);
[$dt, $qps, $ns] = run($M, true);
printf("BATCHED    : time=%.3fs  qps=%d  ns/op=%.0f\n", $dt, $qps, $ns);
