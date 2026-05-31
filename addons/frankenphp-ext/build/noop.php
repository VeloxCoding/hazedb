<?php
// noop.php — baseline: a bare per-request page load that touches hazedb not at
// all. wrk against this vs index.php isolates FrankenPHP's per-request cost from
// the hazedb insert.
echo 'ok';
