#!/usr/bin/env bash
# sysbench OLTP against a MySQL- or PostgreSQL-protocol endpoint (SprigDB,
# MySQL, MariaDB, TiDB, CockroachDB, PostgreSQL): oltp_point_select,
# oltp_read_only and oltp_read_write at 1, 16 and 64 threads; warm-up, then
# --runs measured runs per point (at least 3), then the median of each point
# with its spread (lib/summarize.py). Records sql / point (point_select) or
# complex (read_only, read_write) / sysbench: tps, p50_ms, p99_ms (latencies
# from sysbench's histogram), variant test=..,threads=..,isolation=..,data=..
# Speed workload: run it on the benchmark VMs only, never on a shared laptop.
#
#   sql/sysbench.sh --target 10.0.0.3:3306 --product SprigDB --version <build> \
#       --user bench --password pw --db sbtest -- --isolation read-committed \
#       --memory-cap-gb 4 --data-bytes-cmd "ssh vm1 du -sb /data/engine" \
#       [--tests "..."] [--threads "1 16 64"] [--tables 10] [--table-size 1000000]
#       [--time 120] [--warmup 60] [--runs 3] [--no-prepare]
#       [--driver mysql|pgsql]   (pgsql: CockroachDB or PostgreSQL through sysbench's pgsql driver)
#       [--model dsql]           (record under the distributed-SQL model)
#
# Matched conditions, so one product's number can be divided by another's:
#
# --isolation read-committed | repeatable-read | serializable (default
#   read-committed, the level SprigDB, MySQL, TiDB and CockroachDB all offer).
#   Every connection sets it before the first transaction (sql/isolation.lua)
#   and the run fails unless the server then reports that level: CockroachDB
#   upgrades READ COMMITTED to SERIALIZABLE unless its cluster setting
#   sql.txn.read_committed_isolation.enabled is true, and TiDB refuses
#   SERIALIZABLE. Compare products only at the same level; the level is in
#   the variant, so the report pairs nothing else.
#
# --memory-cap-gb G with --data-bytes-cmd CMD: the data is on disk. G is the
#   memory the product under test was capped at (a container's --memory, or
#   systemd-run -p MemoryMax for a process; both count the page cache), per
#   node; CMD prints the bytes of one node's data directory after the prepare
#   (with three replicas every node holds all the data; if CMD prints several
#   lines they are summed). The run refuses to measure unless the data is at
#   least --min-data-ratio times the cap (default 3). --in-memory instead records
#   data=memory and measures a dataset that fits; such a cell is never paired
#   with a data=disk one.
#
# --runs: at least 3 (--allow-fewer-runs for a smoke run, which is
#   recorded as such).
#
# Needs sysbench 1.0.20 on PATH ($BENCH_SYSBENCH to override).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
TESTS="oltp_point_select oltp_read_only oltp_read_write" THREADS="1 16 64" TABLES=10 SIZE=1000000 TIME=120 WARM=60 RUNS=3 PREP=1 DRIVER=mysql MODEL=sql
ISO=read-committed MEMCAP="" DATACMD="" RATIO=3 INMEM=0 FEWER=0
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --tests) TESTS=$2; shift 2 ;; --threads) THREADS=$2; shift 2 ;; --tables) TABLES=$2; shift 2 ;;
    --table-size) SIZE=$2; shift 2 ;; --time) TIME=$2; shift 2 ;; --warmup) WARM=$2; shift 2 ;;
    --runs) RUNS=$2; shift 2 ;; --no-prepare) PREP=0; shift ;; --driver) DRIVER=$2; shift 2 ;; --model) MODEL=$2; shift 2 ;;
    --isolation) ISO=$2; shift 2 ;; --memory-cap-gb) MEMCAP=$2; shift 2 ;; --data-bytes-cmd) DATACMD=$2; shift 2 ;;
    --min-data-ratio) RATIO=$2; shift 2 ;; --in-memory) INMEM=1; shift ;; --allow-fewer-runs) FEWER=1; shift ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
SB=${BENCH_SYSBENCH:-sysbench}
need "$SB" python3
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

((RUNS >= 3 || FEWER == 1)) || { echo "--runs must be at least 3 (or --allow-fewer-runs for a smoke run)" >&2; exit 2; }
case $ISO in
read-committed) MY_ISO=READ-COMMITTED PG_ISO="read committed" ;;
repeatable-read) MY_ISO=REPEATABLE-READ PG_ISO="repeatable read" ;;
serializable) MY_ISO=SERIALIZABLE PG_ISO="serializable" ;;
*) echo "--isolation must be read-committed, repeatable-read or serializable" >&2; exit 2 ;;
esac
if [[ $DRIVER == pgsql ]]; then
    SB_ISOLATION_SQL="SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL $(echo "$PG_ISO" | tr '[:lower:]' '[:upper:]')"
    export SB_ISOLATION_SQL
    export SB_ISOLATION_CHECK="SHOW transaction_isolation"
    WANT=$PG_ISO
else
    export SB_ISOLATION_SQL="SET SESSION transaction_isolation = '$MY_ISO'"
    export SB_ISOLATION_CHECK="SELECT @@transaction_isolation"
    WANT=$MY_ISO
fi
if [[ $INMEM == 0 && ( -z $MEMCAP || -z $DATACMD ) ]]; then
    echo "give --memory-cap-gb and --data-bytes-cmd (the data must be on disk), or --in-memory" >&2
    exit 2
fi
DATA=disk
[[ $INMEM == 1 ]] && DATA=memory

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

DATA_BYTES=unknown
if [[ $INMEM == 0 && $DRY == 0 ]]; then
    DATA_BYTES=$(bash -c "$DATACMD" | awk '{ s += $1 } END { print s + 0 }')
    need_bytes=$(python3 -I -c "print(int($RATIO * $MEMCAP * 1024**3))")
    if ((DATA_BYTES < need_bytes)); then
        echo "the data is $DATA_BYTES bytes, under $RATIO x the ${MEMCAP} GiB cap ($need_bytes): it would be served" \
            "from memory. Raise --table-size or lower the cap." >&2
        exit 3
    fi
    echo "data $DATA_BYTES bytes on disk, memory cap ${MEMCAP} GiB"
fi
CFG=(--config "tables=$TABLES" --config "table_size=$SIZE" --config "seconds=$TIME" --config "isolation=$ISO"
     --config "data=$DATA" --config "data_bytes=$DATA_BYTES" --config "memory_cap_gb=${MEMCAP:-none}")
[[ $FEWER == 1 ]] && CFG+=(--config "smoke=fewer than 3 runs")

run_sb() { # run_sb <test> <threads> <seconds> <log> [more sysbench options]: the test with the isolation set
    local t=$1 th=$2 secs=$3 log=$4
    shift 4
    SB_TEST=$t "$SB" "$HERE/isolation.lua" "${conn[@]}" --threads="$th" --time="$secs" "$@" run >"$log" 2>&1
    local got
    got=$(sed -n 's/^isolation in effect: //p' "$log" | head -1)
    if [[ $(echo "$got" | tr '[:upper:]' '[:lower:]') != $(echo "$WANT" | tr '[:upper:]' '[:lower:]') ]]; then
        echo "$t at $th threads ran at '${got:-?}', not $WANT (see $log)" >&2
        exit 4
    fi
}

for t in $TESTS; do
    op=complex; [[ $t == oltp_point_select ]] && op=point
    for th in $THREADS; do
        VARIANT="test=$t,threads=$th,isolation=$ISO,data=$DATA"
        [[ $DRY == 1 ]] && { echo "would run $t at $th threads, $ISO, data on $DATA"; continue; }
        run_sb "$t" "$th" "$WARM" "$LOGDIR/$t-$th-warmup.log"
        for r in $(seq 1 "$RUNS"); do
            f=$LOGDIR/$t-$th-run$r.log
            run_sb "$t" "$th" "$TIME" "$f" --histogram=on --percentile=99
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
            emit "$MODEL" "$op" sysbench tps "$tps" "transactions/s" higher "${CFG[@]}" --config "run=$r"
            emit "$MODEL" "$op" sysbench p50_ms "$p50" ms lower "${CFG[@]}" --config "run=$r"
            emit "$MODEL" "$op" sysbench p99_ms "$p99" ms lower "${CFG[@]}" --config "run=$r"
            RUN_ID=$RUN_ID_SAVE
            echo "$t $th threads run $r: $tps tps, p50 $p50 ms, p99 $p99 ms"
        done
    done
done
# The paired result of each point: the median of its runs, with min, max and spread.
[[ $DRY == 1 ]] || python3 -I "$BENCH_DIR/lib/summarize.py" --run-id "$RUN_ID"
