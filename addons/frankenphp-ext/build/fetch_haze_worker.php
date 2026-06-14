<?php
// Worker-mode target: 10 PK fetches per request against the process-global
// hazedb store (seeded once by seed_hl.php). Worker mode keeps the script +
// PHP runtime warm between requests; the store itself is process-global, so it
// persists across requests regardless. Mirrors fetch_haze.php's per-request work.
const N = 50000;
function id_for(int $i): string {
    $h = md5("row$i");
    $v = dechex((hexdec($h[16]) & 0x3) | 0x8);
    return substr($h, 0, 8).'-'.substr($h, 8, 4).'-4'.substr($h, 13, 3).'-'.$v.substr($h, 17, 3).'-'.substr($h, 20, 12);
}
$handler = static function () {
    $hits = 0;
    for ($k = 0; $k < 10; $k++) {
        if (hazedb_fetch('SELECT name, age FROM t WHERE id = ?', id_for(mt_rand(0, N - 1)))) $hits++;
    }
    echo $hits;
};
$n = 0;
while (\frankenphp_handle_request($handler)) {
    if (++$n >= 1000000) break;
}
