#!/usr/bin/env bash
# Runs a live test suite: starts what it needs (a reference server in a
# container, the engine-only stack of stack.sh), exports the SPRIG_TEST_*
# variables its tests read, and runs its #[ignore]d tests with --ignored.
# A test whose variables are missing fails; nothing passes by skipping.
#
#   tools/live/run.sh list                 the suites and what each needs
#   tools/live/run.sh <suite> [--keep]     run one; --keep leaves its containers and stack up
#
# Suites:
#   redis-engine  the engine's Redis commands, Lua and streams against a reference
#                 Redis 7.4.11 (sprig-redis-engine: diff_engine, diff_lua,
#                 diff_streams); the engine runs in the test process. Needs Docker.
#   redis         the gateway's Redis listener against a reference Redis 7.4.11
#                 (sprig-redis: diff_live, all but the sharded case). Needs Docker
#                 and the stack's binaries.
#   mongo         the gateway's MongoDB listener against a reference MongoDB 8.0.34
#                 (sprig-proto-mongo: driver_live, expr_live, stage_diff_live).
#                 Needs Docker and the stack's binaries.
#
# Variables: SPRIG_LIVE_REF_PORT (reference servers' host port base, default
# 45500), CARGO_TEST_ARGS (extra cargo test arguments, e.g. --release), and
# stack.sh's SPRIG_LIVE_* variables. Containers are named sprig-live-*-ref and
# removed at the end unless --keep.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
HERE=$ROOT/tools/live
STACK=$HERE/stack.sh
REF=${SPRIG_LIVE_REF_PORT:-45500}
KEEP=0
read -r -a CARGO_X <<<"${CARGO_TEST_ARGS:-}"
say() { echo "live: $*"; }
die() {
    echo "live: $*" >&2
    exit 1
}

ref_redis() { # a reference Redis on 127.0.0.1:$REF
    docker rm -f sprig-live-redis-ref >/dev/null 2>&1 || true
    docker run -d --name sprig-live-redis-ref --memory 512m -p "127.0.0.1:$REF:6379" redis:7.4.11 \
        redis-server --save "" --appendonly no >/dev/null
    for _ in $(seq 1 60); do
        docker exec sprig-live-redis-ref redis-cli ping 2>/dev/null | grep -q PONG && return
        sleep 0.5
    done
    die "the reference Redis didn't start"
}
ref_mongo() { # a reference MongoDB on 127.0.0.1:$REF+1
    docker rm -f sprig-live-mongo-ref >/dev/null 2>&1 || true
    docker run -d --name sprig-live-mongo-ref --memory 768m -p "127.0.0.1:$((REF + 1)):27017" mongo:8.0.34 \
        --wiredTigerCacheSizeGB 0.25 >/dev/null
    for _ in $(seq 1 120); do
        docker exec sprig-live-mongo-ref mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' 2>/dev/null | grep -q 1 && return
        sleep 0.5
    done
    die "the reference MongoDB didn't start"
}
stack_up() { # the stack with one gateway and tenant "live"
    "$STACK" up 1
    "$STACK" tenant live sprig-live-app
}
cleanup() {
    [[ $KEEP == 1 ]] && return
    docker rm -f sprig-live-redis-ref sprig-live-mongo-ref >/dev/null 2>&1 || true
    [[ ${STACK_STARTED:-0} == 1 ]] && "$STACK" down || true
}

tests() { # tests <crate> <test binary>... [-- <test harness args>]: their #[ignore]d tests, one at a time
    local crate=$1 args=()
    shift
    while [[ $# -gt 0 && $1 != -- ]]; do
        args+=(--test "$1")
        shift
    done
    [[ ${1:-} == -- ]] && shift
    (cd "$ROOT" && cargo test --locked "${CARGO_X[@]+"${CARGO_X[@]}"}" -p "$crate" "${args[@]}" -- --ignored --test-threads 1 "$@")
}

suite() {
    case $1 in
    redis-engine)
        ref_redis
        export SPRIG_TEST_REDIS_REF=127.0.0.1:$REF
        tests sprig-redis-engine diff_engine diff_lua diff_streams
        ;;
    redis)
        ref_redis
        STACK_STARTED=1
        stack_up
        export SPRIG_TEST_REDIS_REF=127.0.0.1:$REF
        p=$("$STACK" port 1 redis)
        export SPRIG_TEST_REDIS_SPRIG=redis://live.app:sprig-live-app@127.0.0.1:$p
        # The sharded case needs a second cluster and a pin: not this stack.
        tests sprig-redis diff_live -- --skip sharded_by_a_pin
        ;;
    mongo)
        ref_mongo
        STACK_STARTED=1
        stack_up
        export SPRIG_TEST_MONGO_REF_URI="mongodb://127.0.0.1:$((REF + 1))/?directConnection=true"
        p=$("$STACK" port 1 mongo)
        export SPRIG_TEST_MONGO_URI="mongodb://live.app:sprig-live-app@127.0.0.1:$p/?authSource=admin&directConnection=true"
        tests sprig-proto-mongo driver_live expr_live stage_diff_live
        ;;
    *) die "unknown suite $1 (run.sh list)" ;;
    esac
}

case ${1:-list} in
list) sed -n '/^# Suites:/,/^# Variables:/p' "$0" | sed 's/^# \{0,1\}//' | sed '$d' ;;
*)
    s=$1
    shift
    [[ ${1:-} == --keep ]] && KEEP=1
    trap cleanup EXIT
    suite "$s"
    say "suite $s passed"
    ;;
esac
