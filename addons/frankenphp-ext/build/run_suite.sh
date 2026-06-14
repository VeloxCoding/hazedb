#!/usr/bin/env bash
# run_suite.sh — inside-container bench harness, standard 9-cpu setup.
# Boots dist/frankenphp pinned to the app cores (0-6) serving from
# container-native /app, then runs the 2 standard wrk modes (wrk pinned to
# cores 7-8) against an endpoint. Run inside a container started with
# --cpuset-cpus=0-8.
#
#   docker run --rm --cpuset-cpus=0-8 \
#       -v "$PWD/dist/frankenphp:/usr/local/bin/frankenphp:ro" \
#       -v "$PWD:/work:ro" -w /work <builder-image> \
#       bash /work/run_suite.sh <script.php> [worker|classic]
#
# Modes:  w-low  -t2  -c10  -d5s   (latency at low concurrency; still thousands/s)
#         w-high -t32 -c250 -d5s   (throughput near saturation)
# 7 app cores = the Go-native sweet spot on this host (peaks ~7, declines above);
# wrk on 2 cores is never the limiter; same-container loopback is fastest.
set -euo pipefail
SCRIPT="${1:?usage: run_suite.sh <script.php> [worker|classic]}"
MODE="${2:-worker}"

command -v wrk >/dev/null || { apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq wrk util-linux >/dev/null 2>&1; }
mkdir -p /app && cp /work/*.php /app/

if [ "$MODE" = worker ]; then
    FP=$'frankenphp {\n\t\tworker /app/'"$SCRIPT"$'\n\t}'
else
    FP='frankenphp'
fi
cat > /app/Caddyfile <<EOF
{
	auto_https off
	$FP
}
:8080 {
	root * /app
	php_server
}
EOF

taskset -c 0-6 /usr/local/bin/frankenphp run --config /app/Caddyfile --adapter caddyfile >/tmp/srv.log 2>&1 &
PID=$!
for i in $(seq 1 40); do curl -sf "http://localhost:8080/$SCRIPT" >/dev/null 2>&1 && break; sleep 0.5; done
grep "🐘" /tmp/srv.log || true
for i in $(seq 1 300); do curl -s "http://localhost:8080/$SCRIPT" >/dev/null; done  # warm

run() { echo; echo "=== $1 ($MODE): wrk $2 ==="; taskset -c 7-8 wrk $2 --latency "http://localhost:8080/$SCRIPT"; }
run w-low  "-t2  -c10  -d5s"
run w-high "-t32 -c250 -d5s"

kill "$PID" 2>/dev/null || true
