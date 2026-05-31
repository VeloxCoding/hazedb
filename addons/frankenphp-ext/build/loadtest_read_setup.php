<?php
// loadtest_read_setup.php — hit once before the read loadtest to (re)create and
// seed the table loadtest_read.php lists. N rows, PK auto-generated (id omitted).
header('Content-Type: text/plain');
$N = (int)($_GET['n'] ?? 500);
hazedb_exec('DROP TABLE loadtest_read');
$rc = hazedb_exec('CREATE TABLE loadtest_read (id uuid primary key, name text, email text, age int)');
if ($rc === -1) {
    echo "setup FAILED\n";
    return;
}
for ($i = 0; $i < $N; $i++) {
    hazedb_exec('INSERT INTO loadtest_read (name, email, age) VALUES (?, ?, ?)',
        ["user$i", "user$i@example.com", $i % 100]);
}
echo "seeded $N\n";
