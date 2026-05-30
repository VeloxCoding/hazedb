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

// hazedb_query_arr returns a native PHP array; re-encoding it must match the
// JSON path above (proves the zval-direct builder produces identical data).
$arr = hazedb_query_arr('SELECT name, age FROM users WHERE id = ?', $id);
echo 'query_arr_is_array=', (is_array($arr) ? 'yes' : 'no'), "\n";
echo 'query_arr=', json_encode($arr), "\n";      // expect identical to 'query' above

// hazedb_exec_arr: insert with a NATIVE PHP array (no json_encode), read back.
$id2 = make_uuid();
show('exec_arr', hazedb_exec_arr(
    'INSERT INTO users (id, name, age) VALUES (?, ?, ?)',
    [$id2, 'bob', 25]
));                                               // expect {"affected":1}
echo 'exec_arr_read=', json_encode(hazedb_query_arr('SELECT name, age FROM users WHERE id = ?', $id2)), "\n";

// hazedb_get: single row as a flat assoc array, null when absent.
echo 'get=', json_encode(hazedb_get('SELECT name, age FROM users WHERE id = ?', $id)), "\n"; // expect {"name":"alice","age":30}
echo 'get_missing=', (hazedb_get('SELECT name FROM users WHERE id = ?', make_uuid()) === null ? 'NULL' : 'NOTNULL'), "\n"; // expect NULL
