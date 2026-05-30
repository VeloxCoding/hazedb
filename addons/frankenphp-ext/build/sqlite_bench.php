<?php
// sqlite_bench.php — baseline: how many point-reads/sec PHP can do against an
// in-memory SQLite (:memory:) database, mirroring the hazedb_fetch bench as
// closely as possible:
//   - same logical table: users(id, name, age), id is the PK (TEXT — SQLite has
//     no uuid type, so the canonical UUID string is stored, as hazedb accepts)
//   - same query: SELECT name, age FROM users WHERE id = ?  (point read by PK)
//   - same N rows seeded, same id-by-index access pattern
//
// SQLite is in-process in PHP (a linked library, no socket/network), so this is
// the fair "no network hop" comparison to hazedb's in-process cgo call.
//
// Two modes, because it changes the result a lot:
//   PREPARED_REUSE : prepare the SELECT once, execute many — matches hazedb,
//                    which caches the compiled plan by SQL string internally.
//   PREPARE_EACH   : $pdo->prepare() inside the loop — what naive PHP code does.
//
// Result is fetched into a PHP array (PDO::FETCH_NUM) — i.e. usable data, the
// fair equivalent of hazedb_fetch (a usable assoc row).

$N     = (int)($argv[1] ?? 10000);
$iters = (int)($argv[2] ?? 1000000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');

$ins = $pdo->prepare('INSERT INTO users (id, name, age) VALUES (?, ?, ?)');
$ids = [];
for ($i = 0; $i < $N; $i++) {
    $id = make_uuid();
    $ids[] = $id;
    $ins->execute([$id, "user$i", $i % 100]);
}

$sql = 'SELECT name, age FROM users WHERE id = ?';

// sanity
$st = $pdo->prepare($sql);
$st->execute([$ids[0]]);
echo 'sanity=', json_encode($st->fetch(PDO::FETCH_NUM)), "\n";

// --- A: prepared statement reused ---
$st = $pdo->prepare($sql);
for ($i = 0; $i < 50000; $i++) { $st->execute([$ids[$i % $N]]); $st->fetch(PDO::FETCH_NUM); }
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    $st->execute([$ids[$i % $N]]);
    $r = $st->fetch(PDO::FETCH_NUM);
}
$dtA = microtime(true) - $t0;

// --- B: prepare on every call ---
$t0 = microtime(true);
for ($i = 0; $i < $iters; $i++) {
    $s = $pdo->prepare($sql);
    $s->execute([$ids[$i % $N]]);
    $r = $s->fetch(PDO::FETCH_NUM);
}
$dtB = microtime(true) - $t0;

printf("php=%s  rows=%d  iters=%d\n", PHP_VERSION, $N, $iters);
printf("PREPARED_REUSE : time=%.3fs  qps=%d  ns/op=%.0f\n", $dtA, (int)($iters / $dtA), $dtA * 1e9 / $iters);
printf("PREPARE_EACH   : time=%.3fs  qps=%d  ns/op=%.0f\n", $dtB, (int)($iters / $dtB), $dtB * 1e9 / $iters);
