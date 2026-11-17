#!/usr/bin/env bash
# Kafka client checks on a reference Apache Kafka (--ref) and the endpoint
# under test (--target):
#   kafka-python 2.0.6  deploy/compose/clients-kafka/kafka_suite.py: produce
#                       with gzip and acks=all, keys stable per partition, a
#                       group read in key order with headers and timestamps,
#                       a second run reading nothing, seeks, end offsets and
#                       offsets for times (timings not compared);
#   kcat 1.7.1          keyed produce with headers, read back from the start,
#                       a balanced group read, metadata, offsets by time.
# Records stream / compat / kafka-python and kcat, metric matched.
#
#   stream/kafka-clients.sh --target 127.0.0.1:47302 --product Redpanda --version 25.2.10 \
#       --ref 127.0.0.1:47301 -- --ref-version 4.1.0 \
#       --kcat-target bench-redpanda:9092 --kcat-ref bench-kafka:19092 --kcat-network bench-net
#
# --user/--password turn on SASL SCRAM-SHA-512 for the target (SprigDB).
# kcat runs as the edenhill/kcat:1.7.1 image on --kcat-network (default
# host), reaching the brokers at --kcat-target / --kcat-ref (default the
# same addresses). kafka-python comes from $BENCH_KAFKA_PYTHON (a wheel).
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
[[ -n $REF ]] || { echo "--ref HOST:PORT is required" >&2; exit 2; }
RVER="" KT=$TARGET KR=$REF KNET=host
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in
    --ref-version) RVER=$2; shift 2 ;; --kcat-target) KT=$2; shift 2 ;;
    --kcat-ref) KR=$2; shift 2 ;; --kcat-network) KNET=$2; shift 2 ;;
    *) echo "unknown $1" >&2; exit 2 ;; esac; done
stamp=$(date +%s)
compare() { # compare ref.json target.json diff.txt -> "total same"
    python3 -I - "$@" <<'EOF'
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:3])
keys = sorted(k for k in set(a) | set(b) if not k.endswith(("_seconds", "_s", "_rate")))
diff = [k for k in keys if a.get(k) != b.get(k)]
open(sys.argv[3], "w").write("".join(f"{k}: reference {a.get(k)!r} target {b.get(k)!r}\n" for k in diff))
print(len(keys), len(keys) - len(diff))
EOF
}

# kafka-python
kp() { PYTHONPATH=${BENCH_KAFKA_PYTHON:-} python3 "$BENCH_DIR/../compose/clients-kafka/kafka_suite.py" "$@"; }
kp "$REF" - - "bench.kp.r.$stamp" "bench-g-r-$stamp" >"$LOGDIR/kp-ref.json" 2>"$LOGDIR/kp-ref.err" || echo '{}' >"$LOGDIR/kp-ref.json"
kp "$TARGET" "${B_USER:--}" "${B_PASS:--}" "bench.kp.t.$stamp" "bench-g-t-$stamp" >"$LOGDIR/kp-target.json" 2>"$LOGDIR/kp-target.err" || echo '{}' >"$LOGDIR/kp-target.json"
read -r total same < <(compare "$LOGDIR/kp-ref.json" "$LOGDIR/kp-target.json" "$LOGDIR/kp-diff.txt")
cat "$LOGDIR/kp-diff.txt"
echo "kafka-python: $same of $total observations equal"
emit stream compat kafka-python matched "$same" observations higher --total "$total" \
    --title "kafka-python 2.0.6: $same of $total observations equal to Apache Kafka $RVER" \
    --config "reference=Apache Kafka $RVER at $REF"

# kcat
kcat_side() { # broker sasl-user sasl-pass topic -> JSON on stdout
    local b=$1 u=$2 p=$3 t=$4 sasl=()
    [[ $u != - ]] && sasl=(-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=SCRAM-SHA-512 -X "sasl.username=$u" -X "sasl.password=$p")
    k() { docker run --rm -i --name "bench-kcat-$$-$RANDOM" --network "$KNET" edenhill/kcat:1.7.1 -b "$b" "${sasl[@]+"${sasl[@]}"}" "$@"; }
    local out=$LOGDIR/kcat-$t
    mkdir -p "$out"
    seq 0 59 | awk '{printf "k%d:v%d\n", $1 % 6, $1}' | k -P -t "$t" -K: -z gzip -H src=bench >"$out/produce.log" 2>&1
    echo "produce_rc=$?" >>"$out/produce.log"
    k -C -t "$t" -o beginning -e -f '%k %s %h\n' >"$out/read.txt" 2>"$out/read.err" || true
    k -G "bench-kc-$t" -X auto.offset.reset=earliest -c 60 -f '%p %k %s\n' "$t" >"$out/group.txt" 2>"$out/group.err" &
    local gp=$!
    for _ in $(seq 1 60); do kill -0 $gp 2>/dev/null || break; sleep 1; done
    kill $gp 2>/dev/null || true
    k -L -t "$t" -J >"$out/meta.json" 2>"$out/meta.err" || true
    k -Q -t "$t:0:$(( ($(date +%s) - 3600) * 1000 ))" >"$out/query.txt" 2>"$out/query.err" || true
    python3 -I - "$out" <<'EOF'
import json, os, sys
d = sys.argv[1]
r = {}
lines = [l.split() for l in open(os.path.join(d, "read.txt")) if l.strip()]
r["read_count"] = len(lines)
r["headers_kept"] = bool(lines) and all(len(x) > 2 and x[2] == "src=bench" for x in lines)
order = {}
for k, v, *_ in lines:
    order.setdefault(k, []).append(int(v[1:]))
r["per_key_order"] = bool(order) and all(v == sorted(v) for v in order.values())
g = [l.split() for l in open(os.path.join(d, "group.txt")) if l.strip()]
r["group_read_count"] = len(g)
parts = {}
for p, k, v in g:
    parts.setdefault(k, set()).add(p)
r["key_partition_stable"] = bool(parts) and all(len(s) == 1 for s in parts.values())
try:
    m = json.load(open(os.path.join(d, "meta.json")))
    r["partitions"] = len(m["topics"][0]["partitions"])
except Exception:
    r["partitions"] = None
q = open(os.path.join(d, "query.txt")).read()
r["offset_by_time_found"] = "Offset: 0" in q or "offset 0" in q.lower()
print(json.dumps(r, sort_keys=True))
EOF
}
kcat_side "$KR" - - "bench.kc.r.$stamp" >"$LOGDIR/kcat-ref.json"
kcat_side "$KT" "${B_USER:--}" "${B_PASS:--}" "bench.kc.t.$stamp" >"$LOGDIR/kcat-target.json"
read -r total same < <(compare "$LOGDIR/kcat-ref.json" "$LOGDIR/kcat-target.json" "$LOGDIR/kcat-diff.txt")
cat "$LOGDIR/kcat-ref.json" "$LOGDIR/kcat-target.json" "$LOGDIR/kcat-diff.txt"
echo "kcat: $same of $total observations equal"
emit stream compat kcat matched "$same" observations higher --total "$total" \
    --title "kcat 1.7.1: $same of $total observations equal to Apache Kafka $RVER" \
    --config "reference=Apache Kafka $RVER at $REF"
