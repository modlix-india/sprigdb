#!/usr/bin/env bash
# redis-benchmark (from the redis:7.4.11 image) core commands against a
# Redis-protocol endpoint: GET, SET, INCR, LPUSH, RPOP, SADD, HSET, ZADD,
# MSET, 50 clients, --requests each, --runs runs. Records kv / point /
# redis-benchmark: ops_per_s, variant cmd=<name>. Speed workload: VMs only.
#
#   kv/redis-benchmark.sh --target 10.0.0.3:6379 --product SprigDB --user u --password p -- [--requests 200000] [--runs 3]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
REQ=200000 RUNS=3 NET=host
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --requests) REQ=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;; --network) NET=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
need docker
auth=(); [[ -n $B_PASS ]] && auth=(--user "${B_USER:-default}" -a "$B_PASS")
[[ $DRY == 1 ]] && { echo "would run redis-benchmark"; exit 0; }
for r in $(seq 1 "$RUNS"); do
    docker run --rm --name "bench-rb-$$" --network "$NET" redis:7.4.11 redis-benchmark -h "$HOST" -p "$PORT" \
        "${auth[@]+"${auth[@]}"}" -c 50 -n "$REQ" -t get,set,incr,lpush,rpop,sadd,hset,zadd,mset --csv >"$LOGDIR/run$r.csv" 2>"$LOGDIR/run$r.err"
    while IFS=, read -r name rps _; do
        name=${name//\"/}; rps=${rps//\"/}
        [[ $name == test || -z $rps ]] && continue
        VARIANT="cmd=$(echo "$name" | cut -d' ' -f1)"
        RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
        emit kv point redis-benchmark ops_per_s "$rps" "ops/s" higher --config "run=$r" --config "clients=50" --config "requests=$REQ"
        RUN_ID=$RUN_ID_SAVE
    done <"$LOGDIR/run$r.csv"
done
