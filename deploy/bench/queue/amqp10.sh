#!/usr/bin/env bash
# AMQP 1.0 checks with rhea (queue/amqp10.js) on a reference RabbitMQ 4.x
# (--ref) and the endpoint under test (--target). The queue is declared over
# AMQP 0-9-1 first (pika, as in amqp-diff.sh). Records queue / compat /
# amqp10-rhea / matched (observations equal to the reference, of all).
#
#   queue/amqp10.sh --target 127.0.0.1:13672 --product SprigDB --user acme.app \
#       --password pw --db bench_amqp --ref 127.0.0.1:47201 \
#       -- --ref-user app --ref-password pw --ref-vhost / --ref-version 4.1.4
#
# rhea comes from $BENCH_NODE_PATH (a node_modules directory).
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
q=bench.a10.$(date +%s).$$.$RANDOM
declare_q() {
    PYTHONPATH=${BENCH_PIKA:-} python3 - "$@" <<'EOF'
import sys, pika
h, p, u, pw, vh, q = sys.argv[1:7]
c = pika.BlockingConnection(pika.ConnectionParameters(host=h, port=int(p), virtual_host=vh, credentials=pika.PlainCredentials(u, pw)))
c.channel().queue_declare(q, durable=True)
c.close()
EOF
}
run_side() { NODE_PATH=${BENCH_NODE_PATH:-} node "$BENCH_DIR/queue/amqp10.js" "$@"; }
declare_q "${REF%:*}" "${REF##*:}" "$RUSER" "$RPASS" "$RVHOST" "$q"
declare_q "$HOST" "$PORT" "$B_USER" "$B_PASS" "${B_DB:-/}" "$q"
run_side "${REF%:*}" "${REF##*:}" "$RUSER" "$RPASS" "$RVHOST" "$q" >"$LOGDIR/ref.json" 2>"$LOGDIR/ref.err" || true
run_side "$HOST" "$PORT" "$B_USER" "$B_PASS" "${B_DB:-/}" "$q" >"$LOGDIR/target.json" 2>"$LOGDIR/target.err" || true
read -r total same < <(python3 -I - "$LOGDIR/ref.json" "$LOGDIR/target.json" "$LOGDIR/diff.txt" <<'EOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
keys = sorted(k for k in set(a) | set(b) if k not in ("done", "order"))
diff = [k for k in keys if a.get(k) != b.get(k)]
open(sys.argv[3], "w").write("".join(f"{k}: reference {a.get(k)!r} target {b.get(k)!r}\n" for k in diff))
print(len(keys), len(keys) - len(diff))
EOF
)
cat "$LOGDIR/ref.json" "$LOGDIR/target.json" "$LOGDIR/diff.txt"
emit queue compat amqp10-rhea matched "$same" observations higher --total "$total" \
    --title "AMQP 1.0 (rhea): $same of $total observations equal to RabbitMQ $RVER" \
    --config "reference=RabbitMQ $RVER at $REF" --config "client=rhea $(NODE_PATH=${BENCH_NODE_PATH:-} node -p 'require("rhea/package.json").version' 2>/dev/null || echo '?')"
