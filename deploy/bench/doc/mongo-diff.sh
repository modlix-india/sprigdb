#!/usr/bin/env bash
# MongoDB differentials (doc/mongo-diff.js): the expression, stage and
# validation corpora recorded on MongoDB 8.0.34, replayed over a collection
# on the endpoint under test. Records doc / correct / mongo-diff-<corpus> /
# matched. With --calibration <cases.json> (the per-case file of a run against
# MongoDB 8.0.34 itself), "matched" counts only the cases MongoDB matches
# under this same replay, so products are compared on equal terms.
#
#   doc/mongo-diff.sh --target 127.0.0.1:47401 --product FerretDB --version 2.5.0 \
#       --user u --password p --db bench_mdiff -- [--auth-source admin] \
#       [--calibration results/../logs/mongo-diff-mongodb-<run>/cases.json] [--direct]
#
# The mongodb driver comes from $BENCH_NODE_PATH (a node_modules directory,
# e.g. the Mongoose clone's).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
AUTHSRC=admin CAL="" DIRECT=""
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --auth-source) AUTHSRC=$2; shift 2 ;; --calibration) CAL=$2; shift 2 ;;
    --direct) DIRECT="&directConnection=true"; shift ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
cred=""
[[ -n $B_USER ]] && cred="$(python3 -I -c 'import sys,urllib.parse as u;print(u.quote(sys.argv[1],safe="")+":"+u.quote(sys.argv[2],safe=""))' "$B_USER" "$B_PASS")@"
uri="mongodb://${cred}${HOST}:${PORT}/?authSource=$AUTHSRC$DIRECT"
CORPORA=$BENCH_DIR/../../crates/sprig-doc/tests
(cd "$LOGDIR" && MDIFF_CASES_OUT=$LOGDIR/cases.json NODE_PATH=${BENCH_NODE_PATH:-} \
    node "$BENCH_DIR/doc/mongo-diff.js" "$uri" "${B_DB:-bench_mdiff}" "$CORPORA") >"$LOGDIR/summary.jsonl" 2>"$LOGDIR/diffs.txt" || true
cat "$LOGDIR/summary.jsonl"
python3 -I - "$LOGDIR/cases.json" "${CAL:-}" >"$LOGDIR/scored.txt" <<'EOF'
import json, sys
got = json.load(open(sys.argv[1]))
cal = json.load(open(sys.argv[2])) if sys.argv[2] else None
for corpus, cases in got.items():
    names = [n for n in cases if cal is None or cal.get(corpus, {}).get(n)]
    print(corpus, sum(1 for n in names if cases[n]), len(names), len(cases))
EOF
cat "$LOGDIR/scored.txt"
while read -r corpus matched total all; do
    emit doc correct "mongo-diff-$corpus" matched "$matched" cases higher --total "$total" \
        --title "MongoDB $corpus corpus: $matched of $total cases answer as MongoDB 8.0.34 recorded" \
        --notes "${CAL:+Counted over the $total of $all cases MongoDB 8.0.34 itself matches under this replay. }Replayed over a collection; values with exact BSON types, errors by code." \
        --config "corpus=crates/sprig-doc/tests/${corpus}_diff/cases.json" --config "calibrated=${CAL:+yes}"
done <"$LOGDIR/scored.txt"
