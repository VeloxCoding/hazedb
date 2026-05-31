<?php
// loadtest_read.php — the read wrk target: a classic "GET a list as JSON"
// endpoint. One hazedb_fetchall_json over the whole seeded table, streamed
// straight to the HTTP response (no PHP decode). Exercises the multi-row
// streaming core path (QueryJSON) under concurrent HTTP load.
header('Content-Type: application/json');
echo hazedb_fetchall_json('SELECT id, name, email, age FROM loadtest_read');
