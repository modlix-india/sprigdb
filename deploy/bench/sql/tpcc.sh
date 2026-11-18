#!/usr/bin/env bash
# TPC-C-like load with Percona's sysbench-tpcc (tpcc.lua) against a
# MySQL-protocol endpoint: --warehouses (10), --threads (16), --time (600 s),
# --runs measured runs after a warm-up. Records sql / tx / tpcc: tps (all
# transaction types) and tpmC (New-Order transactions per minute, taken as
# 45% of all transactions, the TPC-C mix sysbench-tpcc runs). For a
# distributed run, point --target at the cluster's entry and use --model dsql.
# Speed workload: benchmark VMs only.
#
#   sql/tpcc.sh --target 10.0.0.3:3306 --product SprigDB --user bench --password pw --db tpcc \
#       -- --tpcc-dir /opt/sysbench-tpcc [--warehouses 10] [--threads 16] [--time 600] [--runs 3] [--model dsql]
#          [--driver mysql|pgsql] [--no-prepare]
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
DIR="" WH=10 TH=16 TIME=600 WARM=120 RUNS=3 MODEL=sql DRIVER=mysql PREP=1
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --tpcc-dir) DIR=$2; shift 2 ;; --warehouses) WH=$2; shift 2 ;; --threads) TH=$2; shift 2 ;;
    --time) TIME=$2; shift 2 ;; --warmup) WARM=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;; --model) MODEL=$2; shift 2 ;; --driver) DRIVER=$2; shift 2 ;; --no-prepare) PREP=0; shift ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
[[ -f $DIR/tpcc.lua ]] || { echo "--tpcc-dir must hold tpcc.lua (Percona-Lab/sysbench-tpcc)" >&2; exit 2; }
SB=${BENCH_SYSBENCH:-sysbench}
need "$SB"
# tpcc.lua requires its sibling files; the runs start in the log directory
export LUA_PATH="$DIR/?.lua;${LUA_PATH:-;}"
if [[ $DRIVER == pgsql ]]; then
    conn=(--db-driver=pgsql --pgsql-host="$HOST" --pgsql-port="$PORT" --pgsql-user="$B_USER" --pgsql-password="$B_PASS"
          --pgsql-db="${B_DB:-tpcc}" --tables=1 --scale="$WH")
else
    conn=(--db-driver=mysql --mysql-host="$HOST" --mysql-port="$PORT" --mysql-user="$B_USER" --mysql-password="$B_PASS"
          --mysql-db="${B_DB:-tpcc}" --mysql-ssl=off --tables=1 --scale="$WH")
fi
VARIANT="warehouses=$WH,threads=$TH"
[[ $DRY == 1 ]] && { echo "would load $WH warehouses and run $RUNS x $TIME s"; exit 0; }
[[ $PREP == 1 ]] && { (cd "$LOGDIR" && "$SB" "$DIR/tpcc.lua" "${conn[@]}" --threads=8 prepare) >"$LOGDIR/prepare.log" 2>&1; }
(cd "$LOGDIR" && "$SB" "$DIR/tpcc.lua" "${conn[@]}" --threads="$TH" --time="$WARM" run) >"$LOGDIR/warmup.log" 2>&1
for r in $(seq 1 "$RUNS"); do
    f=$LOGDIR/run$r.log
    (cd "$LOGDIR" && "$SB" "$DIR/tpcc.lua" "${conn[@]}" --threads="$TH" --time="$TIME" --report-interval=0 run) >"$f" 2>&1
    tps=$(grep -Eo 'transactions:[^(]+\(([0-9.]+) per sec' "$f" | grep -Eo '[0-9.]+ per sec' | grep -Eo '[0-9.]+')
    tpmc=$(python3 -I -c "print(round(float('$tps') * 60 * 0.45, 1))")
    RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
    emit "$MODEL" tx tpcc tps "$tps" "transactions/s" higher --config "run=$r" --config "seconds=$TIME"
    emit "$MODEL" tx tpcc tpmC "$tpmc" "New-Order/min" higher --config "run=$r" --config "new_order_share=0.45 (derived)"
    RUN_ID=$RUN_ID_SAVE
done
