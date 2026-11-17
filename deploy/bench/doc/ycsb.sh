#!/usr/bin/env bash
# YCSB core workloads (A, B, C, E, F) against a MongoDB-protocol endpoint
# (SprigDB, MongoDB, FerretDB) or Couchbase: --records documents loaded once,
# then each workload after a warm-up, --runs measured runs. Records doc / point
# (A, B, C, F) or complex (E, scans) / ycsb-<w>: ops_per_s, read_p50_us,
# read_p99_us, update_p99_us (HdrHistogram). Load throughput goes to doc / load.
# Speed workload: benchmark VMs only.
#
#   doc/ycsb.sh --target 10.0.0.3:27017 --product SprigDB --user acme.bench --password pw --db ycsb \
#       -- --ycsb-dir /opt/ycsb-0.17.0 [--binding mongodb|couchbase3] [--workloads "a b c e f"] \
#       [--records 1000000] [--ops 2000000] [--threads 16] [--runs 3] [--no-load] [--write-concern majority]
#       [--time S]   (cap each measured run at S seconds, the warm-up at S/2: YCSB's maxexecutiontime)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
Y="" BIND=mongodb WL="a b c e f" REC=1000000 OPS=2000000 TH=16 RUNS=3 LOAD=1 WC=majority TLIM=""
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --ycsb-dir) Y=$2; shift 2 ;; --binding) BIND=$2; shift 2 ;; --workloads) WL=$2; shift 2 ;;
    --records) REC=$2; shift 2 ;; --ops) OPS=$2; shift 2 ;; --threads) TH=$2; shift 2 ;;
    --runs) RUNS=$2; shift 2 ;; --no-load) LOAD=0; shift ;; --write-concern) WC=$2; shift 2 ;; --time) TLIM=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
[[ -x $Y/bin/ycsb.sh ]] || { echo "--ycsb-dir must hold bin/ycsb.sh (YCSB 0.17.0)" >&2; exit 2; }
need java python3
if [[ $BIND == mongodb ]]; then
    cred=""; [[ -n $B_USER ]] && cred="$B_USER:$B_PASS@"
    props=(-p "mongodb.url=mongodb://$cred$HOST:$PORT/${B_DB:-ycsb}?w=$WC&journal=true&authSource=admin")
else
    props=(-p "couchbase.host=$HOST" -p "couchbase.bucket=${B_DB:-ycsb}" -p "couchbase.username=$B_USER" -p "couchbase.password=$B_PASS")
fi
common=(-p recordcount="$REC" -p operationcount="$OPS" -p measurementtype=hdrhistogram -p hdrhistogram.percentiles=50,99 -threads "$TH")
warmcap=(); runcap=()
[[ -n $TLIM ]] && warmcap=(-p maxexecutiontime=$((TLIM / 2))) && runcap=(-p maxexecutiontime="$TLIM")
parse() { # file op metric -> value
    python3 -I - "$@" <<'PY'
import sys
f, op, metric = sys.argv[1:4]
for line in open(f):
    p = [x.strip() for x in line.split(",")]
    if len(p) == 3 and p[0] == f"[{op}]" and p[1] == metric:
        print(p[2]); break
else:
    print("")
PY
}
[[ $DRY == 1 ]] && { echo "would load $REC and run $WL"; exit 0; }
if [[ $LOAD == 1 ]]; then
    "$Y/bin/ycsb.sh" load "$BIND" -s -P "$Y/workloads/workloada" "${props[@]}" "${common[@]}" >"$LOGDIR/load.log" 2>&1
    VARIANT="records=$REC,threads=$TH"
    emit doc load ycsb-load ops_per_s "$(parse "$LOGDIR/load.log" OVERALL 'Throughput(ops/sec)')" "ops/s" higher
fi
for w in $WL; do
    op=point; [[ $w == e ]] && op=complex
    VARIANT="records=$REC,threads=$TH"
    "$Y/bin/ycsb.sh" run "$BIND" -s -P "$Y/workloads/workload$w" "${props[@]}" "${common[@]}" -p operationcount=$((OPS / 4)) "${warmcap[@]+"${warmcap[@]}"}" >"$LOGDIR/$w-warmup.log" 2>&1
    for r in $(seq 1 "$RUNS"); do
        f=$LOGDIR/$w-run$r.log
        "$Y/bin/ycsb.sh" run "$BIND" -s -P "$Y/workloads/workload$w" "${props[@]}" "${common[@]}" "${runcap[@]+"${runcap[@]}"}" >"$f" 2>&1
        RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
        emit doc "$op" "ycsb-$w" ops_per_s "$(parse "$f" OVERALL 'Throughput(ops/sec)')" "ops/s" higher --config "run=$r" --config "write_concern=$WC"
        rd=READ; [[ $w == e ]] && rd=SCAN
        emit doc "$op" "ycsb-$w" read_p50_us "$(parse "$f" $rd '50thPercentileLatency(us)')" us lower --config "run=$r"
        emit doc "$op" "ycsb-$w" read_p99_us "$(parse "$f" $rd '99thPercentileLatency(us)')" us lower --config "run=$r"
        up=$(parse "$f" UPDATE '99thPercentileLatency(us)')
        [[ -n $up ]] && emit doc "$op" "ycsb-$w" update_p99_us "$up" us lower --config "run=$r"
        RUN_ID=$RUN_ID_SAVE
    done
done
