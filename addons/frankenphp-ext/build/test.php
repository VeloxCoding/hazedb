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

// The app supplies its own PK (canonical UUID string). hazedb also auto-fills
// a UUIDv7 when the id is omitted on INSERT — generate one here only because we
// read the row back by id below.
function make_uuid(): string {
    $b = random_bytes(16);
    $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);     // version 4
    $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);     // RFC 4122 variant
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($b), 4));
}

show('ping', hazedb_ping());                     // expect pong (module provisioned a DB)

$id = make_uuid();
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
