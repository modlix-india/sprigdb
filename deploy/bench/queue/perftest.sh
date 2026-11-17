#!/usr/bin/env bash
# RabbitMQ PerfTest (pivotalrabbitmq/perf-test:2.21.0) against an AMQP 0-9-1
# endpoint: persistent messages with publisher confirms, 1 queue and 10
# queues (one producer and one consumer each), --time seconds, --runs runs.
# Records queue / point / perftest: send_msg_s, recv_msg_s, p50_ms, p99_ms
# (consumer latency), variant queues=N. Speed workload: benchmark VMs only.
#
#   queue/perftest.sh --target 10.0.0.3:5672 --product SprigDB --user acme.bench --password pw --db mq \
#       -- [--queues "1 10"] [--time 60] [--runs 3] [--size 1000]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
QS="1 10" TIME=60 RUNS=3 SIZE=1000 NET=host
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --queues) QS=$2; shift 2 ;; --time) TIME=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;;
    --size) SIZE=$2; shift 2 ;; --network) NET=$2; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
need docker python3
vh=$(python3 -I -c 'import sys,urllib.parse as u;print(u.quote(sys.argv[1],safe=""))' "${B_DB:-/}")
uri="amqp://$B_USER:$B_PASS@$HOST:$PORT/$vh"
[[ $DRY == 1 ]] && { echo "would run PerfTest for queues $QS"; exit 0; }
for q in $QS; do
    VARIANT="queues=$q"
    for r in $(seq 1 "$RUNS"); do
        f=$LOGDIR/q$q-run$r.log
        docker run --rm --name "bench-perftest-$$" --network "$NET" pivotalrabbitmq/perf-test:2.21.0 --uri "$uri" \
            --queue-pattern 'bench-pt-%d' --queue-pattern-from 1 --queue-pattern-to "$q" --producers "$q" --consumers "$q" \
            --flag persistent --confirm 100 --size "$SIZE" --time "$TIME" --auto-delete true >"$f" 2>&1 || true
        read -r snd rcv p50 p99 < <(python3 -I - "$f" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
snd = re.search(r"sending rate avg: (\d+)", t)
rcv = re.search(r"receiving rate avg: (\d+)", t)
lat = re.search(r"consumer latency min/median/75th/95th/99th ([\d./]+) (µs|ms)", t)
p50 = p99 = ""
if lat:
    v = [float(x) for x in lat.group(1).split("/")]
    k = 0.001 if lat.group(2) == "µs" else 1.0
    p50, p99 = v[1] * k, v[4] * k
print(snd.group(1) if snd else "", rcv.group(1) if rcv else "", p50, p99)
PY
)
        RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
        emit queue point perftest send_msg_s "$snd" "msg/s" higher --config "run=$r" --config "persistent=yes" --config "confirms=100" --config "size=$SIZE"
        emit queue point perftest recv_msg_s "$rcv" "msg/s" higher --config "run=$r"
        emit queue point perftest p50_ms "$p50" ms lower --config "run=$r"
        emit queue point perftest p99_ms "$p99" ms lower --config "run=$r"
        RUN_ID=$RUN_ID_SAVE
    done
done
