#!/usr/bin/env bash
# The Neo4j drivers' testkit (tests.neo4j) through the Python driver's backend
# against a Bolt server. Records graph / compat / testkit-python: passed (of
# tests run), with failed and skipped. Run it on a reference Neo4j too, and
# compare like for like (deploy/compose/clients-bolt/run.sh does the same in CI).
#
#   graph/testkit.sh --target 127.0.0.1:47801 --product Memgraph --version 3.6.1 \
#       --user neo4j --password pw -- --testkit DIR --driver DIR --python VENV/bin/python
#
# DIR are checkouts of neo4j-drivers/testkit (75f4aa8) and the Python driver
# (9c876c1); the venv holds ifaddr 0.2.0, lark 1.2.2, pytz 2025.2 and
# typing_extensions 4.15.0. The backend runs from the log directory with the
# driver on PYTHONPATH, never from inside the checkout. Port 9876 must be free.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
TK="" DRV="" PY=python3 EDITION=community NEOVER=5.26
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --testkit) TK=$2; shift 2 ;; --driver) DRV=$2; shift 2 ;; --python) PY=$2; shift 2 ;;
    --server-version) NEOVER=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
[[ -d $TK && -d $DRV ]] || { echo "--testkit and --driver checkouts are required" >&2; exit 2; }
(cd "$LOGDIR" && PYTHONPATH="$DRV/src:$DRV" TEST_BACKEND_VERSION=python exec "$PY" -m testkitbackend) >"$LOGDIR/backend.log" 2>&1 &
backend=$!
trap 'kill $backend 2>/dev/null || true' EXIT
for _ in $(seq 1 60); do (echo >/dev/tcp/127.0.0.1/9876) 2>/dev/null && break; sleep 1; done
(cd "$LOGDIR" && TEST_DRIVER_NAME=python TEST_NEO4J_HOST=$HOST TEST_NEO4J_PORT=$PORT TEST_NEO4J_USER=$B_USER \
    TEST_NEO4J_PASS=$B_PASS TEST_NEO4J_SCHEME=bolt TEST_NEO4J_VERSION=$NEOVER TEST_NEO4J_EDITION=$EDITION \
    TEST_NEO4J_CLUSTER=False TEST_NEO4J_DEFAULT_DB=neo4j \
    "$PY" "$BENCH_DIR/../compose/clients-bolt/testkit_run.py" "$TK" "$LOGDIR/tests.tsv") 2>"$LOGDIR/unittest.log" || true
read -r pass fail skip < <(awk -F'\t' '{c[$2]++} END {print c["pass"]+0, c["fail"]+0, c["skip"]+0}' "$LOGDIR/tests.tsv")
echo "testkit python: $pass pass, $fail fail, $skip skip"
emit graph compat testkit-python passed "$pass" tests higher --total $((pass + fail)) \
    --title "Neo4j testkit (Python driver): $pass pass, $fail fail, $skip skip" \
    --extra-json "{\"failed\": $fail, \"skipped\": $skip}" --config "testkit=75f4aa8" --config "driver=python 9c876c1"
