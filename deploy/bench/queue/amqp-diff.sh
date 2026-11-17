#!/usr/bin/env bash
# AMQP 0-9-1 differential with pika 1.3.2: the 21 scenarios of
# deploy/compose/clients-amqp/amqp_diff.py (confirms, get counts, nack and
# requeue, dead letters with x-death, TTL, max-length, routing by topic,
# fanout and headers, priorities, mandatory returns, 404/405/406 errors,
# purge, transactions, prefetch, alternate exchange, properties, direct
# reply-to) run against a reference RabbitMQ (--ref) and the endpoint under
# test (--target); the observations are compared key by key.
#
#   queue/amqp-diff.sh --target 127.0.0.1:13672 --product SprigDB --user acme.app \
#       --password pw --db bench_amqp --ref 127.0.0.1:47201 \
#       -- --ref-user app --ref-password pw --ref-vhost / --ref-version 4.1.4
#
# --db is the virtual host on the target (SprigDB: a database of the tenant).
# pika comes from $BENCH_PIKA (a wheel or a directory on PYTHONPATH), else
# the python3 on PATH must import it. Records queue / correct / amqp-diff /
# matched (scenarios equal, of all).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
[[ -n $REF ]] || { echo "--ref HOST:PORT is required" >&2; exit 2; }
RUSER=guest RPASS=guest RVHOST=/ RVER=""
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --ref-user) RUSER=$2; shift 2 ;; --ref-password) RPASS=$2; shift 2 ;;
    --ref-vhost) RVHOST=$2; shift 2 ;; --ref-version) RVER=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
SCRIPT=$BENCH_DIR/../compose/clients-amqp/amqp_diff.py
py() { PYTHONPATH=${BENCH_PIKA:-} python3 "$SCRIPT" "$@"; }
pre=bench$(date +%s)
py "${REF%:*}" "${REF##*:}" "$RUSER" "$RPASS" "$RVHOST" "$pre" >"$LOGDIR/ref.json" 2>"$LOGDIR/ref.err"
py "$HOST" "$PORT" "$B_USER" "$B_PASS" "${B_DB:-/}" "$pre" >"$LOGDIR/target.json" 2>"$LOGDIR/target.err"
read -r total same < <(python3 -I - "$LOGDIR/ref.json" "$LOGDIR/target.json" "$LOGDIR/diff.txt" <<'EOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
keys = sorted(k for k in a if k != "cleanup" and not k.endswith("_seconds"))
diff = [k for k in keys if json.dumps(a.get(k), sort_keys=True) != json.dumps(b.get(k), sort_keys=True)]
with open(sys.argv[3], "w") as f:
    for k in diff:
        f.write(f"{k}\n  reference: {json.dumps(a.get(k))}\n  target:    {json.dumps(b.get(k))}\n")
print(len(keys), len(keys) - len(diff))
EOF
)
cat "$LOGDIR/diff.txt"
echo "amqp-diff: $same of $total scenarios equal"
emit queue correct amqp-diff matched "$same" scenarios higher --total "$total" \
    --title "pika differential: $same of $total scenarios equal to RabbitMQ $RVER" \
    --config "reference=RabbitMQ $RVER at $REF" --config "client=pika 1.3.2"
