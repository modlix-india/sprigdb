# Shared by the workload scripts in deploy/bench. Source it after `set -euo pipefail`.
#
# Every script takes the same options, so one command line works on a laptop
# and on the benchmark VMs:
#   --target HOST:PORT     the endpoint under test (required; never a default port)
#   --product NAME         SprigDB, MySQL, Valkey, ... (required)
#   --version V            the product's version, recorded in the result
#   --competitor NAME      the report column (default: the product)
#   --user U --password P  credentials, when the endpoint needs them
#   --db NAME              database / tenant database / vhost, when it applies
#   --ref HOST:PORT        a reference endpoint, for differential suites
#   --run ID               run id suffix (default: date and time)
#   --out DIR              results directory (default $BENCH_RESULTS)
#   --dry-run              print what would run, write nothing
# Script-specific options follow `--`.
# shellcheck shell=bash

BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RESULT_PY=$BENCH_DIR/lib/result.py
TARGET="" PRODUCT="" VERSION="" COMPETITOR="" B_USER="" B_PASS="" B_DB="" REF="" RUN_ID="" DRY=0
EXTRA=()

bench_args() {
    CMDLINE=$(printf '%s ' "$0" "$@" | sed -E 's/(--password )[^ ]+/\1***/')
    while [[ $# -gt 0 ]]; do
        case $1 in
        --target) TARGET=$2; shift 2 ;;
        --product) PRODUCT=$2; shift 2 ;;
        --version) VERSION=$2; shift 2 ;;
        --competitor) COMPETITOR=$2; shift 2 ;;
        --user) B_USER=$2; shift 2 ;;
        --password) B_PASS=$2; shift 2 ;;
        --db) B_DB=$2; shift 2 ;;
        --ref) REF=$2; shift 2 ;;
        --run) RUN_ID=$2; shift 2 ;;
        --out) export BENCH_RESULTS=$2; shift 2 ;;
        --dry-run) DRY=1; shift ;;
        --) shift; EXTRA=("$@"); break ;;
        -h | --help) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown option $1 (see --help)" >&2; exit 2 ;;
        esac
    done
    [[ -n $TARGET ]] || { echo "--target HOST:PORT is required" >&2; exit 2; }
    [[ -n $PRODUCT ]] || { echo "--product is required" >&2; exit 2; }
    [[ $TARGET == *:* ]] || { echo "--target must be HOST:PORT" >&2; exit 2; }
    HOST=${TARGET%:*} PORT=${TARGET##*:}
    COMPETITOR=${COMPETITOR:-$PRODUCT}
    RUN_ID=${RUN_ID:-$(date +%Y%m%d-%H%M%S)}
    PSLUG=$(echo "$PRODUCT" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9\n' '-')
    LOGROOT=${BENCH_LOGS:-${BENCH_RESULTS:-$BENCH_DIR/results}/../logs}
    LOGREL=$(basename "$0" .sh)-$PSLUG-$RUN_ID
    LOGDIR=$LOGROOT/$LOGREL
    mkdir -p "$LOGDIR"
    export HOST PORT
}

need() { for t in "$@"; do command -v "$t" >/dev/null || { echo "needs $t on PATH" >&2; exit 2; }; done; }

# emit MODEL OP WORKLOAD METRIC VALUE UNIT BETTER [more result.py options...]
emit() {
    local model=$1 op=$2 workload=$3 metric=$4 value=$5 unit=$6 better=$7
    shift 7
    if [[ $DRY == 1 ]]; then echo "would record $model/$op/$workload $metric=$value $unit"; return; fi
    # One of several measured runs (a "run=N" config) is kept out of the pairing: lib/summarize.py
    # writes the paired median of the runs. BENCH_UNPAIRED=1 keeps every result of this
    # invocation out of the pairing (e.g. a second durability setting reported separately), and
    # BENCH_VARIANT_SUFFIX is appended to the variant (e.g. ",persistence=default").
    local unpaired=${BENCH_UNPAIRED:-0} a
    for a in "$@"; do [[ $a == run=* ]] && unpaired=1; done
    local variant=${VARIANT:-}${BENCH_VARIANT_SUFFIX:-}
    variant=${variant#,}
    python3 -I "$RESULT_PY" --model "$model" --op "$op" --workload "$workload" --metric "$metric" \
        --value "$value" --unit "$unit" --better "$better" --product "$PRODUCT" \
        --product-version "$VERSION" --competitor "$COMPETITOR" --run "$workload-$variant-$metric-$PRODUCT-$RUN_ID" \
        ${variant:+--variant "$variant"} --source "logs/$LOGREL" --command "$CMDLINE" ${BENCH_NOTE:+--notes "$BENCH_NOTE"} \
        $([[ ${BENCH_UNPAIRED:-0} == 1 ]] && echo --extra-json '{"pair":false,"keep_unpaired":true}' || { [[ $unpaired == 1 ]] && echo --extra-json '{"pair":false}'; }) "$@"
}
