<?php
// Worker-mode probe. Top-level runs ONCE per worker in worker mode, so $c
// survives across requests and the response counts up (1,2,3,...). In classic
// mode the whole script re-runs per request, so it would always print 1 — and
// frankenphp_handle_request returns false outside a worker context.
$c = 0;
$ok = \frankenphp_handle_request(static function () use (&$c) {
    $c++;
    echo "count=$c";
});
while ($ok) {
    $ok = \frankenphp_handle_request(static function () use (&$c) {
        $c++;
        echo "count=$c";
    });
}
