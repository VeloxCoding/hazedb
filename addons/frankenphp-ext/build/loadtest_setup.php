<?php
// loadtest_setup.php — hit once before wrk to (re)create the table the
// page-load insert writes into. INDEX (name) is maintained async by the merger
// as the inserts pour in under load.
header('Content-Type: text/plain');
hazedb_exec('DROP TABLE loadtest');
$rc = hazedb_exec('CREATE TABLE loadtest (id uuid primary key, name text, INDEX (name))');
echo $rc === -1 ? "setup FAILED\n" : "setup ok\n";
