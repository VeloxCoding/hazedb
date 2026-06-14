<?php
// Smoke test for the hazedb PHP extension — the PDO-shaped array API.
// Exercises the full cgo path and emits quote-free *_ok=yes markers that
// smoke.sh greps (avoids quoting headaches in the bash -c block). The *DB is
// process-wide, so reset the table first.

header('Content-Type: text/plain');

function ok($label, $cond) { echo $label, '=', ($cond ? 'yes' : 'no'), "\n"; }
function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

echo 'ping=', hazedb_ping(), "\n";                 // pong

hazedb_exec('DROP TABLE users');                   // ignore -1 on first run
hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)');

$a = make_uuid();
ok('exec_ok',     hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)', [$a, 'alice', 30]) === 1);
ok('exec_int_ok', is_int(hazedb_exec('INSERT INTO users (id, name, age) VALUES (?, ?, ?)', [make_uuid(), 'carol', 40])));

// fetch — one assoc row with NATIVE types (age is int 30, not "30")
$row = hazedb_fetch('SELECT name, age FROM users WHERE id = ?', [$a]);
ok('fetch_ok',         $row !== null && $row['name'] === 'alice' && $row['age'] === 30);
ok('fetch_missing_ok', hazedb_fetch('SELECT name FROM users WHERE id = ?', [make_uuid()]) === null);

// scalar-arg fast path: pass the id bare ($a), not [$a]
$scalar = hazedb_fetch('SELECT name, age FROM users WHERE id = ?', $a);
ok('fetch_scalar_ok',  $scalar !== null && $scalar['name'] === 'alice' && $scalar['age'] === 30);

// fetchall — list of assoc rows; fetchall_json — byte-identical JSON of the same
$all = hazedb_fetchall('SELECT name, age FROM users WHERE age >= ? ORDER BY age ASC', [30]);
$js  = hazedb_fetchall_json('SELECT name, age FROM users WHERE age >= ? ORDER BY age ASC', [30]);
ok('fetchall_ok',      is_array($all) && count($all) === 2 && $all[0]['name'] === 'alice' && $all[1]['name'] === 'carol');
ok('fetchall_json_ok', json_encode($all) === $js);

// meta — store-size overview as a JSON string; decode and find the users table.
$meta = hazedb_meta();
$m = $meta !== null ? json_decode($meta, true) : null;
$users = null;
if (is_array($m) && isset($m['table_stats'])) {
    foreach ($m['table_stats'] as $t) {
        if ($t['name'] === 'users') { $users = $t; }
    }
}
ok('meta_ok', $users !== null && $users['rows'] === 2 && $users['columns'] === 3 && $users['approx_bytes'] > 0);

// for eyeballing
echo 'sample=', $js, "\n";
echo 'meta=', $meta, "\n";
