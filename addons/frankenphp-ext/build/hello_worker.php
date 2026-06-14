<?php
// Bare hello-world worker: no store, no I/O — just echo. Measures the FrankenPHP
// worker-mode request-handling ceiling under the same wrk params as the store
// benchmarks, so the numbers are directly comparable.
$h = static function () {
    echo 'Hello, World!';
};
while (\frankenphp_handle_request($h)) {
}
