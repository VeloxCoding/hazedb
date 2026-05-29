<?php
// Smoke test for the hazedb PHP extension. Exercises the full path in-process:
// mint a UUID, CREATE a table, INSERT with args, SELECT it back. Prints lines
// smoke.sh greps for. The binary boots a fresh in-memory DB each run, so CREATE
// succeeds once per boot. If the Caddy hazedb module did not provision, the
// functions return null (printed "NULL").

header('Content-Type: text/plain');

function show($label, $v) {
    echo $label, '=', ($v === null ? 'NULL' : $v), "\n";
}

$id = hazedb_uuidv7();
echo 'uuidlen=', strlen($id), "\n";              // expect 36

show('create', hazedb_exec('CREATE TABLE users (id uuid primary key, name text, age int)', ''));
show('insert', hazedb_exec(
    'INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
    json_encode([$id, 'alice', 30])
));                                               // expect {"affected":1}
show('query', hazedb_query(
    'SELECT name, age FROM users WHERE id = ?',
    json_encode([$id])
));                                               // expect {"columns":["name","age"],"rows":[["alice",30]]}
