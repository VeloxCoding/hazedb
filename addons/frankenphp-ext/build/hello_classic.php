<?php
// Classic-mode hello-world: no frankenphp_handle_request loop. Served by
// php_server, which re-enters the script on every request (the opposite of a
// warm worker). Compared against hello_worker.php to isolate worker-mode gain.
echo 'Hello, World!';
