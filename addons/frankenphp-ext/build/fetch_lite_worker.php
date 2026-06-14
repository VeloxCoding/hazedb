<?php
// Worker-mode target: 10 PK fetches per request against a SQLite file
// (/tmp/bench.sqlite, seeded once by seed_hl.php). The connection is opened
// ONCE in the worker bootstrap and the SELECT prepared lazily on first request
// (the table does not exist until seed_hl.php has run), then reused across
// every request the worker serves — the worker-mode equivalent of a warm pool.
const N = 50000;
function id_for(int $i): string {
    $h = md5("row$i");
    $v = dechex((hexdec($h[16]) & 0x3) | 0x8);
    return substr($h, 0, 8).'-'.substr($h, 8, 4).'-4'.substr($h, 13, 3).'-'.$v.substr($h, 17, 3).'-'.substr($h, 20, 12);
}
$pdo = new PDO('sqlite:/tmp/bench.sqlite');
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$sel = null;
$handler = static function () use ($pdo, &$sel) {
    if ($sel === null) $sel = $pdo->prepare('SELECT name, age FROM t WHERE id = ?');
    $hits = 0;
    for ($k = 0; $k < 10; $k++) {
        $sel->execute([id_for(mt_rand(0, N - 1))]);
        if ($sel->fetch(PDO::FETCH_ASSOC)) $hits++;
    }
    echo $hits;
};
$n = 0;
while (\frankenphp_handle_request($handler)) {
    if (++$n >= 1000000) break;
}
