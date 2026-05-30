<?php
// pointread_breakdown.php — isolate where a single-row hazedb_fetch spends its
// time vs native SQLite, so we know whether the engine, the cgo crossing, the
// arg read, or the zval result build is the bottleneck on `WHERE email = ?`.

header('Content-Type: text/plain');
set_time_limit(0);
if (!function_exists('hazedb_fetch')) { echo "run over HTTP with the hazedb directive\n"; exit; }

$N    = (int)($_GET['n']    ?? 50000);
$IT   = (int)($_GET['it']   ?? 300000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}
function ns(float $dt, int $n): string { return sprintf("%7.0f ns/op", $dt * 1e9 / $n); }

$emails = [];
hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int null, email text, city text, INDEX (email))');
$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER, email TEXT, city TEXT)');
$pdo->exec('CREATE INDEX idx_email ON users(email)');
$insS = $pdo->prepare('INSERT INTO users (id,name,age,email,city) VALUES (?,?,?,?,?)');
for ($i = 0; $i < $N; $i++) {
    $e = "user$i@x"; $emails[] = $e; $id = make_uuid();
    hazedb_exec('INSERT INTO users (id,name,age,email,city) VALUES (?,?,?,?,?)', [$id, "n$i", $i % 100, $e, "c"]);
    $insS->execute([$id, "n$i", $i % 100, $e, "c"]);
}
usleep(800000); // let the merger index

$Q1 = 'SELECT email FROM users WHERE email = ?';
$Q5 = 'SELECT id, name, age, email, city FROM users WHERE email = ?';
$selS = $pdo->prepare($Q5);

function bench(int $it, callable $fn): float {
    for ($i = 0; $i < 5000; $i++) $fn($i);
    $t = microtime(true);
    for ($i = 0; $i < $it; $i++) $fn($i);
    return microtime(true) - $t;
}

$nE = $N;
$pingT  = bench($IT, fn($i) => hazedb_ping());
$f1sc   = bench($IT, fn($i) => hazedb_fetch($Q1, $emails[$i % $nE]));
$f5sc   = bench($IT, fn($i) => hazedb_fetch($Q5, $emails[$i % $nE]));
$f5arr  = bench($IT, fn($i) => hazedb_fetch($Q5, [$emails[$i % $nE]]));
$jsonT  = bench($IT, fn($i) => hazedb_fetchall_json($Q5, $emails[$i % $nE]));
$sqlT   = bench($IT, function ($i) use ($selS, $emails, $nE) { $selS->execute([$emails[$i % $nE]]); $selS->fetch(PDO::FETCH_ASSOC); });

printf("PHP %s  rows=%d  iters=%d\n\n", PHP_VERSION, $N, $IT);
printf("hazedb_ping (bare cgo crossing)        : %s\n", ns($pingT, $IT));
printf("hazedb_fetch  1 col, scalar arg        : %s\n", ns($f1sc, $IT));
printf("hazedb_fetch  5 col, scalar arg        : %s\n", ns($f5sc, $IT));
printf("hazedb_fetch  5 col, array arg [\$e]     : %s\n", ns($f5arr, $IT));
printf("hazedb_fetchall_json 5 col (JSON out)  : %s\n", ns($jsonT, $IT));
printf("sqlite  5 col, prepared + FETCH_ASSOC  : %s\n", ns($sqlT, $IT));
printf("\nderived:\n");
printf("  per-extra-column zval build (5 vs 1) : %7.0f ns  (/4 = %.0f ns/col)\n", ($f5sc - $f1sc) * 1e9 / $IT, ($f5sc - $f1sc) * 1e9 / $IT / 4);
printf("  array-arg overhead ([\$e] vs \$e)       : %7.0f ns\n", ($f5arr - $f5sc) * 1e9 / $IT);
printf("  assoc-build vs JSON-string (5 col)    : %7.0f ns\n", ($f5sc - $jsonT) * 1e9 / $IT);
printf("  hazedb(5,scalar) vs sqlite            : %.2fx\n", $f5sc / $sqlT);
