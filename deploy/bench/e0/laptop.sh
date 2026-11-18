#!/usr/bin/env bash
# One laptop measurement set, each through the heavy-slot queue (one hold per
# set, well under 30 minutes):
#
#   e0/laptop.sh sql   <mysql|mariadb|sprig|e0>   up, sysbench point_select 1/16/64 (prepare + 3 runs), down
#   e0/laptop.sh kv    <redis|sprig|e0>           up, preload + footprint, memtier 1/16, hot-key SET/INCR/LPUSH + GET, down
#   e0/laptop.sh diff  <sprig|e0>                 the SQL differential against MySQL 8.4 and MariaDB 11.8, Redis against Redis 7.4
#   e0/laptop.sh crash <redis|mysql|e0> [loops]   kill -9 loops (Redis protocol for redis, SQL for mysql, both for e0)
#
# Laptop timings (override with E0_TIME, E0_WARMUP, E0_RUNS): 15 s warm-up, 3 x 30 s.
# Hot-key requests: 100,000 per command (E0_RB_REQUESTS_SPRIG=5000 for today's SprigDB,
# which writes one hot key at a few hundred a second).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
LOCK=$E0_ROOT/target/heavy-lock.sh
what=${1:?sql|kv|diff|crash}; arm=${2:?arm}
T=${E0_TIME:-30} W=${E0_WARMUP:-15} R=${E0_RUNS:-3}
export E0_RB_REQUESTS_SPRIG=${E0_RB_REQUESTS_SPRIG:-5000}
heavy() { if [[ ${E0_NOLOCK:-0} == 1 ]]; then "$@"; else "$LOCK" run e0-bench -- "$@"; fi; }  # E0_NOLOCK=1: the caller already holds a slot
case $what in
sql)
    heavy bash -c "'$E0_DIR/stack.sh' up $arm && '$E0_DIR/bench.sh' sql --arm $arm --threads '1 16 64' --time $T --warmup $W --runs $R; rc=\$?; [[ \${E0_KEEP:-0} == 1 ]] || '$E0_DIR/stack.sh' down $arm; exit \$rc" ;;
kv)
    heavy bash -c "'$E0_DIR/stack.sh' up $arm && '$E0_DIR/bench.sh' kv --arm $arm --time $T --warmup $W --runs $R; rc=\$?; [[ \${E0_KEEP:-0} == 1 ]] || '$E0_DIR/stack.sh' down $arm; exit \$rc" ;;
diff)
    "$E0_DIR/stack.sh" up mysql; "$E0_DIR/stack.sh" up mariadb; "$E0_DIR/stack.sh" up redis; "$E0_DIR/stack.sh" up "$arm"
    prod=$(e0_product "$arm"); ver=$(sed -n 's/^stamp: //p' "$E0_WORK/bin/$([[ $arm == e0 ]] && echo e0 || echo before)/STAMP")
    export BENCH_BUILD="release $ver, Linux build in e0bench-build"; rc=0
    "$E0_DIR/diff.sh" sql --target e0bench-sprig:3306 --ref e0bench-mysql:3306 --flavor mysql --user bench.app --password "$E0_APP_PW" \
        --ref-user "$E0_APP_USER" --ref-password "$E0_APP_PW" --product "$prod" --version "$ver" || rc=1
    "$E0_DIR/diff.sh" sql --target e0bench-sprig:${E0_PORT_NODE_MARIADB} --ref e0bench-mariadb:3306 --flavor mariadb --user bench.app --password "$E0_APP_PW" \
        --ref-user "$E0_APP_USER" --ref-password "$E0_APP_PW" --product "$prod" --version "$ver" || rc=1
    "$E0_DIR/diff.sh" redis --target e0bench-sprig:6379 --ref e0bench-redis:6379 --user bench.app --password "$E0_APP_PW" \
        --product "$prod" --version "$ver" || rc=1
    exit $rc ;;
crash)
    loops=${3:-100}
    case $arm in
    redis) heavy bash -c "'$E0_DIR/stack.sh' up redis && '$E0_DIR/crash.sh' --arm redis --proto redis --loops $loops" ;;
    mysql) heavy bash -c "'$E0_DIR/stack.sh' up mysql && '$E0_DIR/crash.sh' --arm mysql --proto sql --loops $loops" ;;
    e0) heavy bash -c "'$E0_DIR/stack.sh' up e0 && '$E0_DIR/crash.sh' --arm e0 --proto redis --loops $loops && '$E0_DIR/crash.sh' --arm e0 --proto sql --loops $loops" ;;
    *) echo "crash arms: redis mysql e0" >&2; exit 2 ;;
    esac ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
