#!/usr/bin/env bash
# Mongoose 8.24.8's own test suite (deploy/compose/clients-mongo/run.sh) against
# a MongoDB-compatible endpoint, in replica-set mode (transaction tests on the
# same URI). Records doc / compat / mongoose: passed, failed and pending.
#
#   doc/mongoose.sh --target 127.0.0.1:47401 --product FerretDB --version 2.5.0 \
#       --user u --password p --db mongoose_test -- [--auth-source admin] [--no-replset]
#
# The Mongoose clone with its node_modules is $SPRIG_CLIENTS_CACHE/mongoose-8.24.8
# (deploy/compose/clients-mongo/run.sh fetches it there). Mocha runs from the
# log directory with the clone's paths as arguments, never from inside it.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
AUTHSRC=admin REPLSET=1
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --auth-source) AUTHSRC=$2; shift 2 ;; --no-replset) REPLSET=0; shift ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
cred=""
[[ -n $B_USER ]] && cred="$(python3 -I -c 'import sys,urllib.parse as u;print(u.quote(sys.argv[1],safe="")+":"+u.quote(sys.argv[2],safe=""))' "$B_USER" "$B_PASS")@"
uri="mongodb://${cred}${HOST}:${PORT}/${B_DB:-mongoose_test}?authSource=$AUTHSRC"
[[ $REPLSET == 1 ]] && export START_REPLICA_SET=1
src=${SPRIG_CLIENTS_CACHE:?SPRIG_CLIENTS_CACHE must name the Mongoose cache}/mongoose-8.24.8
[[ -d $src/node_modules ]] || { echo "no Mongoose clone in $src; run deploy/compose/clients-mongo/run.sh once" >&2; exit 2; }
(cd "$LOGDIR" && MONGOOSE_TEST_URI=$uri MONGOOSE_REPLSET_URI=$uri node "$src/node_modules/mocha/bin/mocha.js" \
    --no-config --ui bdd --require "$src/test/mocha-fixtures.js" --exit --timeout 30000 --reporter dot \
    "$src"/test/*.test.js) >"$LOGDIR/mocha.txt" 2>&1 || true
tail -n 5 "$LOGDIR/mocha.txt"
passing=$(grep -Eo '^ *[0-9]+ passing' "$LOGDIR/mocha.txt" | tail -1 | grep -Eo '[0-9]+' || echo 0)
failing=$(grep -Eo '^ *[0-9]+ failing' "$LOGDIR/mocha.txt" | tail -1 | grep -Eo '[0-9]+' || echo 0)
pending=$(grep -Eo '^ *[0-9]+ pending' "$LOGDIR/mocha.txt" | tail -1 | grep -Eo '[0-9]+' || echo 0)
echo "mongoose: $passing passing, $failing failing, $pending pending"
emit doc compat mongoose passed "$passing" tests higher --total $((passing + failing + pending)) \
    --title "Mongoose 8.24.8 suite: $passing passing, $failing failing, $pending pending" \
    --config "replica_set_mode=$REPLSET" --extra-json "{\"failed\": $failing, \"pending\": $pending}"
