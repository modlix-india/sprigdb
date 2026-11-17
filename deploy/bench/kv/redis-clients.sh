#!/usr/bin/env bash
# Redis client scenarios (deploy/compose/r6/redis-clients): redis-py RESP2 and
# RESP3, ioredis, go-redis and, with --suites java, Jedis and Lettuce. Each
# runs the same scenario against a reference Redis (--ref, no login) and the
# endpoint under test (--target, --user/--password), and compares every
# result. Records kv / compat / redis-clients / matched (checks equal, of all
# checks) and one measurement per client.
#
#   kv/redis-clients.sh --target 127.0.0.1:47102 --ref 127.0.0.1:47101 \
#       --product Valkey --version 9.0.0 --user app --password pw -- [--suites "py node go"]
#
# The clients come from $R6_CLIENTS_DIR (default target/r6-redis-clients):
# py/venv with redis 5.2.1, node/node_modules with ioredis 5.4.2, and go/
# (a built `suite` binary, else `go run`). Missing ones are installed by
# deploy/compose/r6/redis-clients/run.sh at its pinned versions.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
[[ -n $REF ]] || { echo "--ref HOST:PORT (a reference Redis) is required" >&2; exit 2; }
SUITES="py node go"
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --suites) SUITES=$2; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
SRC=$BENCH_DIR/../compose/r6/redis-clients
W=${R6_CLIENTS_DIR:-$BENCH_DIR/../../target/r6-redis-clients}
args=("$REF" "$TARGET" "$B_USER" "$B_PASS")
log=$LOGDIR/clients.txt
: >"$log"
for s in $SUITES; do
    case $s in
    py) "$W/py/venv/bin/python" -I "$SRC/suite_py.py" "${args[@]}" 2>&1 | grep -v "^Push response" >>"$log" || true ;;
    node) NODE_PATH=$W/node/node_modules node "$SRC/suite_node.js" "${args[@]}" >>"$log" 2>&1 || true ;;
    go) if [[ -x $W/go/suite ]]; then "$W/go/suite" "${args[@]}" >>"$log" 2>&1 || true
        else R6_CLIENTS_DIR=$W R6_SUITES=go "$SRC/run.sh" "${args[@]}" >>"$log" 2>&1 || true; fi ;;
    java) R6_CLIENTS_DIR=$W R6_SUITES=java "$SRC/run.sh" "${args[@]}" >>"$log" 2>&1 || true ;;
    *) echo "unknown suite $s" >&2; exit 2 ;;
    esac
done
cat "$log"
# Lines look like "redis-py 5.2.1 RESP2: 53 checks, 0 differences".
read -r checks diffs < <(awk '/checks, [0-9]+ differences/ {for(i=1;i<=NF;i++){if($(i+1)~/^checks/)c+=$i; if($(i+1)~/^differences/)d+=$i}} END {print c+0, d+0}' "$log")
[[ $checks -gt 0 ]] || { echo "no client produced a summary line; see $log" >&2; exit 1; }
emit kv compat redis-clients matched $((checks - diffs)) checks higher --total "$checks" \
    --title "Redis client scenarios: $((checks - diffs)) of $checks checks equal to the reference ($SUITES)" \
    --notes "$(grep -E 'checks, [0-9]+ differences' "$log" | tr '\n' ';' | sed 's/;$//')" \
    --config "reference=$REF" --config "suites=$SUITES"
