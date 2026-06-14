<?php
// pk_loop_bench.php — time ONLY the PK-SELECT call, 100x, for hazedb vs SQLite.
// 1ms sleep sits BEFORE the timed window, so it is never counted.
//   loop 100x: usleep(1000); t0=hrtime; <fetch one row by PK>; t1=hrtime; sample=t1-t0
// Both stores: in-memory, 50k rows, random UUID primary keys, same ids, same query.
// SQLite statement is prepared ONCE (fair against hazedb's cached compiled plan;
// neither re-parses SQL per call).

header('Content-Type: text/plain');
set_time_limit(0);
if (!function_exists('hazedb_fetch')) { echo "run over HTTP with the hazedb directive\n"; exit; }

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}
function stats(array $s): string {
    sort($s); $c = count($s);
    $p = fn(float $q) => $s[(int)floor(($c - 1) * $q)];
    return sprintf("min=%6d  p50=%6d  avg=%6.0f  p99=%7d  max=%7d", $s[0], $p(0.5), array_sum($s) / $c, $p(0.99), $s[$c - 1]);
}

$N     = (int)($_GET['n']  ?? 50000);
$ITERS = (int)($_GET['it'] ?? 100);
$Q     = 'SELECT name, age FROM t WHERE id = ?';

// --- shared dataset: 50k random UUIDs ---
$ids = [];
for ($i = 0; $i < $N; $i++) $ids[] = make_uuid();

// --- hazedb setup ---
hazedb_exec('DROP TABLE t');
hazedb_exec('CREATE TABLE t (id uuid primary key, name text, age int)');
for ($i = 0; $i < $N; $i++) hazedb_exec('INSERT INTO t (id, name, age) VALUES (?, ?, ?)', [$ids[$i], "n$i", $i % 100]);

// --- SQLite setup (in-memory) ---
$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
$ins = $pdo->prepare('INSERT INTO t (id, name, age) VALUES (?, ?, ?)');
$pdo->beginTransaction();
for ($i = 0; $i < $N; $i++) $ins->execute([$ids[$i], "n$i", $i % 100]);
$pdo->commit();
$sel = $pdo->prepare($Q);

// --- warm up both ---
for ($i = 0; $i < 5000; $i++) hazedb_fetch($Q, $ids[$i % $N]);
for ($i = 0; $i < 5000; $i++) { $sel->execute([$ids[$i % $N]]); $sel->fetch(PDO::FETCH_ASSOC); }

// --- hazedb: time only the call, warm tight loop (no sleep) ---
$haze = [];
for ($k = 0; $k < $ITERS; $k++) {
    $id = $ids[random_int(0, $N - 1)];
    $t0 = hrtime(true); $row = hazedb_fetch($Q, $id); $t1 = hrtime(true);
    $haze[] = $t1 - $t0;
}

// --- SQLite: time only the call (execute + fetch), warm tight loop (no sleep) ---
$lite = [];
for ($k = 0; $k < $ITERS; $k++) {
    $id = $ids[random_int(0, $N - 1)];
    $t0 = hrtime(true); $sel->execute([$id]); $row = $sel->fetch(PDO::FETCH_ASSOC); $t1 = hrtime(true);
    $lite[] = $t1 - $t0;
}

printf("PHP %s   rows=%d   iters=%d   (ns)   warm tight loop, no sleep\n\n", PHP_VERSION, $N, $ITERS);
printf("hazedb  PK fetch : %s\n", stats($haze));
printf("SQLite  PK fetch : %s\n", stats($lite));
