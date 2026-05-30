<?php
// conv_bench.php — isolate the zval<->Value conversion cost across the cgo
// boundary, the one thing the Go-side benchmarks can't reach. Strategy: hold
// the query shape constant (point-read PK hit, or a fixed full scan) and vary
// only the cell count, so the per-cell / per-row DELTA between two otherwise
// identical calls IS the conversion cost.
//
//   PER-CELL build (Go->PHP)  : fetch 1 col vs 9 cols, row=1  -> delta/8
//   PER-ROW  build (Go->PHP)  : fetchall LIMIT 1 vs LIMIT 100 -> delta/99
//   zval-build vs JSON-encode : fetchall vs fetchall_json, same 100x9 data
//   int cell vs string cell   : fetch 9 int cols vs 9 text cols -> setStr cost
//   arg read (PHP->Go)        : scalar arg vs [arg] array, row=1 -> delta

header('Content-Type: text/plain');
set_time_limit(0);

$iters = (int)($_GET['iter'] ?? 1000000);

function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

function cols(string $prefix, int $n, string $type): string {
    $p = [];
    for ($i = 0; $i < $n; $i++) $p[] = "$prefix$i $type";
    return implode(', ', $p);
}
function names(string $prefix, int $n): string {
    $p = [];
    for ($i = 0; $i < $n; $i++) $p[] = "$prefix$i";
    return implode(', ', $p);
}

// --- schema: 1-int-col, 9-int-col, 9-text-col tables ---
hazedb_exec('DROP TABLE c1');  hazedb_exec('DROP TABLE c9');  hazedb_exec('DROP TABLE s9');
hazedb_exec('CREATE TABLE c1 (id uuid primary key, '.cols('a', 1, 'int').')');
hazedb_exec('CREATE TABLE c9 (id uuid primary key, '.cols('a', 9, 'int').')');
hazedb_exec('CREATE TABLE s9 (id uuid primary key, '.cols('a', 9, 'text').')');

// c1/c9: 100 rows so we can also fetchall; remember row0's id for point reads.
$id1 = $id9 = $ids9 = null;
for ($i = 0; $i < 100; $i++) {
    $u = make_uuid();
    $argsC1 = [$u]; for ($j = 0; $j < 1; $j++) $argsC1[] = $i + $j;
    hazedb_exec('INSERT INTO c1 (id, '.names('a',1).') VALUES (?, ?)', $argsC1);

    $u9 = make_uuid();
    $argsC9 = [$u9]; for ($j = 0; $j < 9; $j++) $argsC9[] = $i * 9 + $j;
    hazedb_exec('INSERT INTO c9 (id, '.names('a',9).') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', $argsC9);

    $us = make_uuid();
    $argsS9 = [$us]; for ($j = 0; $j < 9; $j++) $argsS9[] = "val_{$i}_{$j}";
    hazedb_exec('INSERT INTO s9 (id, '.names('a',9).') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', $argsS9);

    if ($i === 0) { $id1 = $u; $id9 = $u9; $ids9 = $us; }
}

$SEL_C1  = 'SELECT '.names('a',1).' FROM c1 WHERE id = ?';
$SEL_C9  = 'SELECT '.names('a',9).' FROM c9 WHERE id = ?';
$SEL_S9  = 'SELECT '.names('a',9).' FROM s9 WHERE id = ?';
$ALL_C9_1   = 'SELECT '.names('a',9).' FROM c9 LIMIT 1';
$ALL_C9_100 = 'SELECT '.names('a',9).' FROM c9 LIMIT 100';

echo 'sanity_c1=',  json_encode(hazedb_fetch($SEL_C1, $id1)), "\n";
echo 'sanity_c9=',  json_encode(hazedb_fetch($SEL_C9, $id9)), "\n";
echo 'sanity_s9=',  json_encode(hazedb_fetch($SEL_S9, $ids9)), "\n";
echo 'sanity_all=', count(hazedb_fetchall($ALL_C9_100)), " rows\n\n";

function bench(string $label, int $iters, callable $fn): float {
    for ($i = 0; $i < 50000; $i++) $fn($i);   // warm
    $t0 = microtime(true);
    for ($i = 0; $i < $iters; $i++) $fn($i);
    $dt = microtime(true) - $t0;
    printf("%-34s time=%.3fs  ns/op=%7.1f\n", $label, $dt, $dt * 1e9 / $iters);
    return $dt * 1e9 / $iters;
}

printf("iters=%d\n\n", $iters);

// === Go->PHP build cost: vary cell count at row=1 ===
$c1 = bench('fetch  1 int cell  (scalar arg)', $iters, fn($i) => hazedb_fetch($SEL_C1, $id1));
$c9 = bench('fetch  9 int cells (scalar arg)', $iters, fn($i) => hazedb_fetch($SEL_C9, $id9));
$s9 = bench('fetch  9 txt cells (scalar arg)', $iters, fn($i) => hazedb_fetch($SEL_S9, $ids9));
printf("  -> per-int-cell build  ~ %.1f ns\n",  ($c9 - $c1) / 8);
printf("  -> per-txt-cell extra  ~ %.1f ns (vs int)\n\n", ($s9 - $c9) / 9);

// === Go->PHP build cost: vary row count (fixed 9 int cols) ===
$r1   = bench('fetchall 1 row   x9 int', $iters, fn($i) => hazedb_fetchall($ALL_C9_1));
$r100 = bench('fetchall 100 rows x9 int', max(1, (int)($iters / 50)), fn($i) => hazedb_fetchall($ALL_C9_100));
printf("  -> per-row build       ~ %.1f ns (incl 9 cells + key rehash)\n", ($r100 - $r1) / 99);

// === zval build vs hand-JSON encode, same 100x9 data ===
$j100 = bench('fetchall_json 100 rows x9', max(1, (int)($iters / 50)), fn($i) => hazedb_fetchall_json($ALL_C9_100));
printf("  -> zval-build vs JSON  : assoc=%.0f ns  json=%.0f ns  (delta %.0f)\n\n", $r100, $j100, $r100 - $j100);

// === PHP->Go arg read: scalar vs [arg] ===
$as = bench('fetch arg: scalar $id', $iters, fn($i) => hazedb_fetch($SEL_C9, $id9));
$aa = bench('fetch arg: array [$id]', $iters, fn($i) => hazedb_fetch($SEL_C9, [$id9]));
printf("  -> array-arg overhead  ~ %.1f ns (build+iterate vs scalar)\n", $aa - $as);
