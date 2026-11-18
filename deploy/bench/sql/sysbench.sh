#!/usr/bin/env bash
# sysbench OLTP against a MySQL-protocol endpoint (SprigDB, MySQL, MariaDB,
# TiDB): oltp_point_select, oltp_read_only and oltp_read_write at 1, 16 and
# 64 threads; warm-up, then --runs measured runs per point. Records sql / point
# (point_select) or complex (read_only, read_write) / sysbench: tps, p50_ms,
# p99_ms (latencies from sysbench's histogram), variant test=..,threads=..
# Speed workload: run it on the benchmark VMs only, never on a shared laptop.
#
#   sql/sysbench.sh --target 10.0.0.3:3306 --product SprigDB --version <build> \
#       --user bench --password pw --db sbtest -- [--tests "..."] [--threads "1 16 64"] \
#       [--tables 10] [--table-size 1000000] [--time 120] [--warmup 60] [--runs 3] [--no-prepare]
#       [--driver mysql|pgsql]   (pgsql: PostgreSQL through sysbench's own pgsql driver)
#       [--model dsql]           (record under the distributed-SQL model)
#
# Needs sysbench 1.0.20 on PATH ($BENCH_SYSBENCH to override).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
TESTS="oltp_point_select oltp_read_only oltp_read_write" THREADS="1 16 64" TABLES=10 SIZE=1000000 TIME=120 WARM=60 RUNS=3 PREP=1 DRIVER=mysql MODEL=sql
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --tests) TESTS=$2; shift 2 ;; --threads) THREADS=$2; shift 2 ;; --tables) TABLES=$2; shift 2 ;;
    --table-size) SIZE=$2; shift 2 ;; --time) TIME=$2; shift 2 ;; --warmup) WARM=$2; shift 2 ;;
    --runs) RUNS=$2; shift 2 ;; --no-prepare) PREP=0; shift ;; --driver) DRIVER=$2; shift 2 ;; --model) MODEL=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
SB=${BENCH_SYSBENCH:-sysbench}
need "$SB" python3
if [[ $DRIVER == pgsql ]]; then
    conn=(--db-driver=pgsql --pgsql-host="$HOST" --pgsql-port="$PORT" --pgsql-user="$B_USER" --pgsql-password="$B_PASS"
          --pgsql-db="${B_DB:-sbtest}" --tables="$TABLES" --table-size="$SIZE")
else
    conn=(--db-driver=mysql --mysql-host="$HOST" --mysql-port="$PORT" --mysql-user="$B_USER" --mysql-password="$B_PASS"
          --mysql-db="${B_DB:-sbtest}" --mysql-ssl=off --tables="$TABLES" --table-size="$SIZE")
fi
# BENCH_SB_EXTRA: more sysbench options for every call, e.g. --auto_inc=off (explicit ids 1..N)
read -r -a SBX <<<"${BENCH_SB_EXTRA:-}"
conn+=("${SBX[@]+"${SBX[@]}"}")
[[ $PREP == 1 && $DRY == 0 ]] && "$SB" oltp_read_write "${conn[@]}" --threads=16 prepare >"$LOGDIR/prepare.log" 2>&1
for t in $TESTS; do
    op=complex; [[ $t == oltp_point_select ]] && op=point
    for th in $THREADS; do
        VARIANT="test=$t,threads=$th"
        [[ $DRY == 1 ]] && { echo "would run $t at $th threads"; continue; }
        "$SB" "$t" "${conn[@]}" --threads="$th" --time="$WARM" run >"$LOGDIR/$t-$th-warmup.log" 2>&1
        for r in $(seq 1 "$RUNS"); do
            f=$LOGDIR/$t-$th-run$r.log
            "$SB" "$t" "${conn[@]}" --threads="$th" --time="$TIME" --histogram=on --percentile=99 run >"$f" 2>&1
            read -r tps p50 p99 < <(python3 -I - "$f" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
tps = re.search(r"transactions:\s+\d+\s+\(([\d.]+) per sec", t).group(1)
hist = [(float(a), int(b)) for a, b in re.findall(r"^\s*([\d.]+)\s+\|[*\s]*(\d+)\s*$", t, re.M)]
n = sum(c for _, c in hist)
def pct(p):
    acc = 0
    for v, c in hist:
        acc += c
        if acc >= p * n:
            return v
    return hist[-1][0] if hist else float("nan")
print(tps, pct(0.50), pct(0.99))
PY
)
            RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
            emit "$MODEL" "$op" sysbench tps "$tps" "transactions/s" higher --config "tables=$TABLES" --config "table_size=$SIZE" --config "seconds=$TIME" --config "run=$r"
            emit "$MODEL" "$op" sysbench p50_ms "$p50" ms lower --config "run=$r"
            emit "$MODEL" "$op" sysbench p99_ms "$p99" ms lower --config "run=$r"
            RUN_ID=$RUN_ID_SAVE
        done
    done
done
