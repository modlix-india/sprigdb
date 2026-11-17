#!/usr/bin/env bash
# memtier_benchmark (redislabs/memtier_benchmark:2.1.4) against a Redis-protocol
# endpoint: SET:GET 1:10, 50 clients (5 threads x 10), pipelines 1 and 16,
# --time seconds per run, --runs runs after a warm-up. Records kv / point /
# memtier: ops_per_s, p50_ms, p99_ms (all commands), variant pipeline=N.
# For matching durability, configure the product (Redis/Valkey: appendfsync
# always) and say so with --durability. Speed workload: benchmark VMs only.
#
#   kv/memtier.sh --target 10.0.0.3:6379 --product Valkey --version 9.0.0 \
#       -- [--pipelines "1 16"] [--time 60] [--runs 3] [--durability "appendfsync always"]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
PIPES="1 16" TIME=60 RUNS=3 DUR="product default" NET=host
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --pipelines) PIPES=$2; shift 2 ;; --time) TIME=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;;
    --durability) DUR=$2; shift 2 ;; --network) NET=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
need docker python3
auth=(); [[ -n $B_PASS ]] && auth=(-a "${B_USER:+$B_USER:}$B_PASS")
mt() { docker run --rm --name "bench-memtier-$$" --network "$NET" -v "$LOGDIR:/out" redislabs/memtier_benchmark:2.1.4 \
    -s "$HOST" -p "$PORT" "${auth[@]+"${auth[@]}"}" --ratio=1:10 --clients=10 --threads=5 --data-size=100 \
    --key-pattern=R:R --key-maximum=1000000 --hide-histogram "$@"; }
[[ $DRY == 1 ]] && { echo "would run memtier pipelines $PIPES"; exit 0; }
for pl in $PIPES; do
    VARIANT="pipeline=$pl"
    mt --pipeline="$pl" --test-time=$((TIME / 2)) >"$LOGDIR/warm-$pl.log" 2>&1
    for r in $(seq 1 "$RUNS"); do
        mt --pipeline="$pl" --test-time="$TIME" --json-out-file="/out/p$pl-run$r.json" >"$LOGDIR/p$pl-run$r.log" 2>&1
        read -r ops p50 p99 < <(python3 -I - "$LOGDIR/p$pl-run$r.json" <<'PY'
import json, sys
t = json.load(open(sys.argv[1]))["ALL STATS"]["Totals"]
pl = t.get("Percentile Latencies", {})
print(t["Ops/sec"], pl.get("p50.00", pl.get("p50", "")), pl.get("p99.00", pl.get("p99", "")))
PY
)
        RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
        emit kv point memtier ops_per_s "$ops" "ops/s" higher --config "run=$r" --config "durability=$DUR" --config "ratio=SET:GET 1:10" --config "clients=50"
        emit kv point memtier p50_ms "$p50" ms lower --config "run=$r"
        emit kv point memtier p99_ms "$p99" ms lower --config "run=$r"
        RUN_ID=$RUN_ID_SAVE
    done
done
