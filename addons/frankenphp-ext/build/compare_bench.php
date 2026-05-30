<?php
// compare_bench.php — head-to-head hazedb vs SQLite :memory:, run BACK-TO-BACK
// in ONE FrankenPHP process (PHP 8.5.6) so both see identical host conditions,
// warmup, and runtime. Same logical table, same rows, same access pattern.
//
//   table : users(id <uuid|text> PK, name, age)
//   id    : canonical UUID string, app-supplied, pre-generated outside timing
//
// INSERT comparison (independent / one-row-at-a-time, the fair match):
//   hazedb_exec (native array args)  vs  SQLite autocommit
//   + SQLite batched (one BEGIN/COMMIT) as a reference ceiling
// READ comparison (point read by PK, PHP gets a usable assoc array):
//   hazedb_fetch  vs  SQLite prepared-statement reuse (FETCH_ASSOC)
//
// Must be served over HTTP (the hazedb *DB is provisioned by the Caddy module;
// php-cli has no provisioned DB). PDO/SQLite is in the same binary.

ini_set('memory_limit', '2G');
header('Content-Type: text/plain');
set_time_limit(0);

$M     = (int)($_GET['m']     ?? 200000);   // rows inserted (insert test)
$RN    = (int)($_GET['rn']    ?? 10000);    // distinct keys in the read pool
$RITER = (int)($_GET['riter'] ?? 1000000);  // read iterations

if (!function_exists('hazedb_fetch')) { echo "hazedb functions absent (run over HTTP with the hazedb directive)\n"; exit; }

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}
function rate(float $dt, int $n): string {
    return sprintf("%9d qps  %7.0f ns/op", (int)($n / $dt), $dt * 1e9 / $n);
}

$ids = [];
for ($i = 0; $i < $M; $i++) { $ids[] = make_uuid(); }
$INS = 'INSERT INTO users (id, name, age) VALUES (?, ?, ?)';
$SEL = 'SELECT name, age FROM users WHERE id = ?';

// ===================== INSERTS =====================

// --- hazedb (native array, independent inserts) ---
hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');
$t = microtime(true);
for ($i = 0; $i < $M; $i++) { hazedb_exec($INS, [$ids[$i], "u$i", $i % 100]); }
$hazedbIns = microtime(true) - $t;

// --- SQLite autocommit (independent inserts) ---
$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
$ins = $pdo->prepare($INS);
$t = microtime(true);
for ($i = 0; $i < $M; $i++) { $ins->execute([$ids[$i], "u$i", $i % 100]); }
$sqliteInsAuto = microtime(true) - $t;

// --- SQLite batched (one transaction) — reference ceiling ---
$pdoB = new PDO('sqlite::memory:');
$pdoB->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdoB->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER)');
$insB = $pdoB->prepare($INS);
$t = microtime(true);
$pdoB->beginTransaction();
for ($i = 0; $i < $M; $i++) { $insB->execute([$ids[$i], "u$i", $i % 100]); }
$pdoB->commit();
$sqliteInsBatch = microtime(true) - $t;
$pdoB = null; $insB = null; // free before reads

// ===================== READS (point read by PK) =====================

$sel = $pdo->prepare($SEL);
// warmup both
for ($i = 0; $i < 50000; $i++) { hazedb_fetch($SEL, [$ids[$i % $RN]]); $sel->execute([$ids[$i % $RN]]); $sel->fetch(PDO::FETCH_ASSOC); }

$t = microtime(true);
for ($i = 0; $i < $RITER; $i++) { $r = hazedb_fetch($SEL, [$ids[$i % $RN]]); }
$hazedbGet = microtime(true) - $t;

$t = microtime(true);
for ($i = 0; $i < $RITER; $i++) { $sel->execute([$ids[$i % $RN]]); $r = $sel->fetch(PDO::FETCH_ASSOC); }
$sqliteGet = microtime(true) - $t;

// ===================== REPORT =====================
printf("env=PHP %s (one FrankenPHP process)  insert_rows=%d  read_pool=%d  read_iters=%d\n\n", PHP_VERSION, $M, $RN, $RITER);

echo "INSERTS (independent, in-memory)\n";
printf("  hazedb_exec         : %s\n", rate($hazedbIns, $M));
printf("  sqlite autocommit   : %s\n", rate($sqliteInsAuto, $M));
printf("  sqlite batched (ref): %s\n", rate($sqliteInsBatch, $M));
printf("  -> hazedb vs sqlite-autocommit: %.2fx\n\n", $sqliteInsAuto / $hazedbIns);

echo "READS (point read by PK -> usable assoc array)\n";
printf("  hazedb_fetch        : %s\n", rate($hazedbGet, $RITER));
printf("  sqlite prepared     : %s\n", rate($sqliteGet, $RITER));
printf("  -> hazedb vs sqlite: %.2fx\n", $sqliteGet / $hazedbGet);
