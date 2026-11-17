#!/usr/bin/env bash
# Kafka's own perf tools (apache/kafka:4.1.0 image) against a Kafka-protocol
# endpoint (SprigDB's Kafka listener, Kafka, Redpanda): producer perf test
# (--records of --size bytes, acks=all), consumer perf test, and end-to-end
# latency. Records stream / point / kafka-perf: produce_mb_s, produce_p99_ms,
# consume_mb_s, e2e_p99_ms. --user/--password turn on SASL SCRAM-SHA-512.
# Speed workload: benchmark VMs only.
#
#   stream/kafka-perf.sh --target 10.0.0.3:9092 --product Redpanda -- [--records 2000000] [--size 1024] [--runs 3] [--partitions 12]
#       [--topic-db bench]   (topics are <topic-db>.perf_<run>_<n>: SprigDB maps a topic to <database>.<stream>)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
REC=2000000 SIZE=1024 RUNS=3 PARTS=12 NET=host TDB=bench
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --records) REC=$2; shift 2 ;; --size) SIZE=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;;
    --partitions) PARTS=$2; shift 2 ;; --network) NET=$2; shift 2 ;; --topic-db) TDB=$2; shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
need docker python3
cfg=$LOGDIR/client.properties
: >"$cfg"
if [[ -n $B_USER ]]; then
    printf '%s\n' "security.protocol=SASL_PLAINTEXT" "sasl.mechanism=SCRAM-SHA-512" \
        "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$B_USER\" password=\"$B_PASS\";" >"$cfg"
fi
k() { docker run --rm --name "bench-kperf-$$" --network "$NET" -v "$LOGDIR:/out" --entrypoint /opt/kafka/bin/"$1" apache/kafka:4.1.0 "${@:2}"; }
[[ $DRY == 1 ]] && { echo "would run kafka perf tools"; exit 0; }
for r in $(seq 1 "$RUNS"); do
    t=$TDB.perf_${RUN_ID//[^A-Za-z0-9]/_}_$r
    k kafka-topics.sh --bootstrap-server "$TARGET" --command-config /out/client.properties --create --topic "$t" --partitions "$PARTS" --replication-factor 1 >"$LOGDIR/topic$r.log" 2>&1 || true
    k kafka-producer-perf-test.sh --topic "$t" --num-records "$REC" --record-size "$SIZE" --throughput -1 \
        --producer-props bootstrap.servers="$TARGET" acks=all --producer.config /out/client.properties >"$LOGDIR/produce$r.log" 2>&1
    k kafka-consumer-perf-test.sh --bootstrap-server "$TARGET" --topic "$t" --messages "$REC" --consumer.config /out/client.properties >"$LOGDIR/consume$r.log" 2>&1
    k kafka-run-class.sh org.apache.kafka.tools.EndToEndLatency "$TARGET" "$t" 10000 all 1024 /out/client.properties >"$LOGDIR/e2e$r.log" 2>&1 || true
    read -r pmb p99 cmb e99 < <(python3 -I - "$LOGDIR" "$r" <<'PY'
import re, sys, os
d, r = sys.argv[1], sys.argv[2]
p = open(os.path.join(d, f"produce{r}.log")).read().strip().splitlines()[-1]
pmb = re.search(r"\(([\d.]+) MB/sec\)", p); p99 = re.search(r"([\d.]+) ms 99th", p)
c = [l for l in open(os.path.join(d, f"consume{r}.log")) if re.match(r"\d{4}-", l)]
cmb = c[-1].split(",")[3].strip() if c else ""
e = open(os.path.join(d, f"e2e{r}.log")).read()
e99 = re.search(r"Percentiles: 50th = [\d.]+, 99th = ([\d.]+)", e)
print(pmb.group(1) if pmb else "", p99.group(1) if p99 else "", cmb, e99.group(1) if e99 else "")
PY
)
    VARIANT="size=$SIZE,partitions=$PARTS"
    RUN_ID_SAVE=$RUN_ID; RUN_ID=$RUN_ID_SAVE-run$r
    emit stream point kafka-perf produce_mb_s "$pmb" "MB/s" higher --config "run=$r" --config "acks=all" --config "records=$REC"
    emit stream point kafka-perf produce_p99_ms "$p99" ms lower --config "run=$r"
    emit stream point kafka-perf consume_mb_s "$cmb" "MB/s" higher --config "run=$r"
    emit stream point kafka-perf e2e_p99_ms "$e99" ms lower --config "run=$r"
    RUN_ID=$RUN_ID_SAVE
done
