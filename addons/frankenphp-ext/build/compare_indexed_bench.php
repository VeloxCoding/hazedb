<?php
// compare_indexed_bench.php — bigger hazedb vs SQLite :memory: head-to-head,
// back-to-back in ONE FrankenPHP process. Same logical table, the SAME indexes
// on both engines, and identical query text. Exercises the realistic web-app
// workload: many single-row inserts, double-WHERE lookups on indexed columns
// (intersection), and filtered ORDER BY ASC/DESC LIMIT.
//
//   table : users(id <uuid|text> PK, name, age, email, city)
//   index : email, name, city  (declared on BOTH engines)
//   id    : canonical UUID string, app-supplied, pre-generated outside timing
//
// NOTE: single-row inserts on BOTH sides (no SQLite transaction batching here —
// bulk is a separate hazedb test). LIMIT is an inline literal so the query text
// is byte-identical on both engines (hazedb's parser takes a literal LIMIT).
//
// Must be served over HTTP (the hazedb *DB is provisioned by the Caddy module).

ini_set('memory_limit', '4G');
header('Content-Type: text/plain');
set_time_limit(0);

if (!function_exists('hazedb_fetch')) {
    echo "hazedb functions absent (run over HTTP with the hazedb directive)\n";
    exit;
}

$N        = (int)($_GET['n']    ?? 50000);  // rows inserted
$IT_PK    = (int)($_GET['pk']   ?? 30000);  // point-read by PK (id)
$IT_PT    = (int)($_GET['pt']   ?? 30000);  // point-read by indexed email
$IT_AND   = (int)($_GET['and']  ?? 15000);  // double-WHERE iterations
$IT_OBIG  = (int)($_GET['obig'] ?? 4000);   // ORDER BY over a big bucket (city)
$IT_OSML  = (int)($_GET['osml'] ?? 15000);  // ORDER BY over a small bucket (name)
$IT_OEM   = (int)($_GET['oem']  ?? 2000);   // global ORDER BY email ASC LIMIT 100

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}
function rate(float $dt, int $n): string {
    return sprintf("%8.0f ns/op  %9d/s", $dt * 1e9 / $n, (int)($n / $dt));
}

// --- data pools (generated outside timing) ---
// 199 names is COPRIME with 10 cities, so a given name spreads across all
// cities (name[i%199], city[i%10]): name bucket ~N/199, city bucket ~N/10, and
// name AND city ~N/1990 — a genuinely small intersection of two large buckets,
// not a correlated subset.
$NAMES  = [];
for ($i = 0; $i < 199; $i++) { $NAMES[] = "name$i"; }
$CITIES = ['AMS', 'RTM', 'UTR', 'DHG', 'EIN', 'GRN', 'TIL', 'ALM', 'BRD', 'NIJ'];

$ids = $emails = $names = $cities = $ages = [];
for ($i = 0; $i < $N; $i++) {
    $ids[]    = make_uuid();
    $emails[] = "user$i@x";
    $names[]  = $NAMES[$i % count($NAMES)];
    $cities[] = $CITIES[$i % count($CITIES)];
    $ages[]   = $i % 100;
}

// =================== schema (identical indexes on both) ===================
hazedb_exec('DROP TABLE users');
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int null, email text, city text, INDEX (email), INDEX (name), INDEX (city))');

$pdo = new PDO('sqlite::memory:');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INTEGER, email TEXT, city TEXT)');
$pdo->exec('CREATE INDEX idx_email ON users(email)');
$pdo->exec('CREATE INDEX idx_name  ON users(name)');
$pdo->exec('CREATE INDEX idx_city  ON users(city)');

$INS = 'INSERT INTO users (id, name, age, email, city) VALUES (?, ?, ?, ?, ?)';

// =================== INSERTS (single-row, both sides) ===================
$t = microtime(true);
for ($i = 0; $i < $N; $i++) { hazedb_exec($INS, [$ids[$i], $names[$i], $ages[$i], $emails[$i], $cities[$i]]); }
$hzIns = microtime(true) - $t;

$insS = $pdo->prepare($INS);
$t = microtime(true);
for ($i = 0; $i < $N; $i++) { $insS->execute([$ids[$i], $names[$i], $ages[$i], $emails[$i], $cities[$i]]); }
$sqIns = microtime(true) - $t;

usleep(800000); // let hazedb's background merger finish indexing before reads

// =================== query definitions (identical text) ===================
$Q_PK   = 'SELECT id, name, age, email, city FROM users WHERE id = ?';
$Q_PT   = 'SELECT id, name, age, email, city FROM users WHERE email = ?';
$Q_AND  = 'SELECT id, name, age FROM users WHERE name = ? AND city = ?';
$Q_OBIG = 'SELECT id, name, age FROM users WHERE city = ? ORDER BY age DESC LIMIT 20';
$Q_OSML = 'SELECT id, name, age FROM users WHERE name = ? ORDER BY age ASC LIMIT 20';
$Q_OEM  = 'SELECT id, name, age, email, city FROM users ORDER BY email ASC LIMIT 100';

$selPK   = $pdo->prepare($Q_PK);
$selPT   = $pdo->prepare($Q_PT);
$selAND  = $pdo->prepare($Q_AND);
$selOBIG = $pdo->prepare($Q_OBIG);
$selOSML = $pdo->prepare($Q_OSML);
$selOEM  = $pdo->prepare($Q_OEM);

// time a hazedb closure vs a sqlite closure over `iters`, with a short warmup
function duel(int $iters, callable $hz, callable $sq): array {
    for ($i = 0; $i < 2000; $i++) { $hz($i); $sq($i); } // warmup both
    $t = microtime(true);
    for ($i = 0; $i < $iters; $i++) { $hz($i); }
    $dh = microtime(true) - $t;
    $t = microtime(true);
    for ($i = 0; $i < $iters; $i++) { $sq($i); }
    $ds = microtime(true) - $t;
    return [$dh, $ds];
}

// point read by PK (id) -> one assoc row
[$pkH, $pkS] = duel($IT_PK,
    fn($i) => hazedb_fetch($Q_PK, [$ids[$i % $N]]),
    function ($i) use ($selPK, $ids, $N) { $selPK->execute([$ids[$i % $N]]); $selPK->fetch(PDO::FETCH_ASSOC); }
);

// point read by unique indexed email -> one assoc row
[$ptH, $ptS] = duel($IT_PT,
    fn($i) => hazedb_fetch($Q_PT, [$emails[$i % $N]]),
    function ($i) use ($selPT, $emails, $N) { $selPT->execute([$emails[$i % $N]]); $selPT->fetch(PDO::FETCH_ASSOC); }
);

// double WHERE on two indexed columns (name AND city) -> list
$nN = count($NAMES); $nC = count($CITIES);
[$andH, $andS] = duel($IT_AND,
    fn($i) => hazedb_fetchall($Q_AND, [$NAMES[$i % $nN], $CITIES[$i % $nC]]),
    function ($i) use ($selAND, $NAMES, $CITIES, $nN, $nC) { $selAND->execute([$NAMES[$i % $nN], $CITIES[$i % $nC]]); $selAND->fetchAll(PDO::FETCH_ASSOC); }
);

// ORDER BY age DESC LIMIT 20 over a big bucket (one city ~ N/10 rows)
[$obigH, $obigS] = duel($IT_OBIG,
    fn($i) => hazedb_fetchall($Q_OBIG, [$CITIES[$i % $nC]]),
    function ($i) use ($selOBIG, $CITIES, $nC) { $selOBIG->execute([$CITIES[$i % $nC]]); $selOBIG->fetchAll(PDO::FETCH_ASSOC); }
);

// ORDER BY age ASC LIMIT 20 over a small bucket (one name ~ N/200 rows)
[$osmlH, $osmlS] = duel($IT_OSML,
    fn($i) => hazedb_fetchall($Q_OSML, [$NAMES[$i % $nN]]),
    function ($i) use ($selOSML, $NAMES, $nN) { $selOSML->execute([$NAMES[$i % $nN]]); $selOSML->fetchAll(PDO::FETCH_ASSOC); }
);

// GLOBAL ORDER BY email ASC LIMIT 100 (no filter). email is indexed on both, but
// SQLite can WALK its btree index in order (no sort); hazedb's hash index cannot
// order, so it scans all rows + keeps a top-100 heap. The case where an ordered
// index would help hazedb (not built).
[$oemH, $oemS] = duel($IT_OEM,
    fn($i) => hazedb_fetchall($Q_OEM),
    function ($i) use ($selOEM) { $selOEM->execute(); $selOEM->fetchAll(PDO::FETCH_ASSOC); }
);

// =================== report ===================
printf("env=PHP %s (one FrankenPHP process)  rows=%d\n", PHP_VERSION, $N);
printf("table=users(id,name,age,email,city)  indexes: email,name,city (both engines)\n");
printf("sanity: hazedb point=%s  AND=%d rows  obig=%d rows  osml=%d rows\n\n",
    json_encode(hazedb_fetch($Q_PT, [$emails[0]])) !== 'null' ? 'ok' : 'MISS',
    count(hazedb_fetchall($Q_AND, [$NAMES[0], $CITIES[0]])),
    count(hazedb_fetchall($Q_OBIG, [$CITIES[0]])),
    count(hazedb_fetchall($Q_OSML, [$NAMES[0]]))
);

$line = function (string $label, float $h, float $s, int $n) {
    printf("  %-34s hazedb %s | sqlite %s | %5.1fx\n", $label, rate($h, $n), rate($s, $n), $s / $h);
};

echo "INSERT (single-row, autocommit both)\n";
$line("INSERT", $hzIns, $sqIns, $N);
echo "\nREADS (identical SQL, identical indexes)\n";
$line("point: WHERE id = ?  (PK)", $pkH, $pkS, $IT_PK);
$line("point: WHERE email = ?  (index)", $ptH, $ptS, $IT_PT);
$line("AND: WHERE name = ? AND city = ?", $andH, $andS, $IT_AND);
$line("ORDER BY age DESC LIMIT 20 (city)", $obigH, $obigS, $IT_OBIG);
$line("ORDER BY age ASC LIMIT 20 (name)", $osmlH, $osmlS, $IT_OSML);
$line("ORDER BY email ASC LIMIT 100 (global)", $oemH, $oemS, $IT_OEM);
