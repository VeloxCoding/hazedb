<?php
const N = 50000;
const F = 10;                                    // fetches per request (like a real page)
function id_for(int $i): string {
    $h = md5("row$i");
    $v = dechex((hexdec($h[16]) & 0x3) | 0x8);
    return substr($h,0,8).'-'.substr($h,8,4).'-4'.substr($h,13,3).'-'.$v.substr($h,17,3).'-'.substr($h,20,12);
}
$pdo = new PDO('sqlite:/tmp/bench.sqlite', null, null, [PDO::ATTR_PERSISTENT => true]);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$sel = $pdo->prepare('SELECT name, age FROM t WHERE id = ?');
$hits = 0;
for ($k = 0; $k < F; $k++) {
    $sel->execute([id_for(mt_rand(0, N-1))]);
    if ($sel->fetch(PDO::FETCH_ASSOC)) $hits++;
}
echo $hits;
