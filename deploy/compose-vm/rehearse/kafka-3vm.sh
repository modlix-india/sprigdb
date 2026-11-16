#!/usr/bin/env bash
# Rehearsal: Kafka clients through every VM's gateway (RUNBOOK.md, "Load
# balancer" and "Rehearsals").
#
#   kafka-3vm.sh <out dir>
#
# kcat (edenhill/kcat:1.7.1) bootstraps through the load balancer, then
# connects to each broker Kafka's Metadata names: every gateway under its
# own name (SPRIG_KAFKA_HOST, here kafka-vm1..3), all on the DC's one Kafka
# port. A stream of 6 partitions is led by all three gateways.
#   1. Metadata: three brokers, every partition led by one of them;
#   2. 600 keyed records produced (idempotent), and each gateway served
#      produce requests (sprig_kafka_requests_total on each);
#   3. read back from the beginning, and by a consumer group;
#   4. VM 2's gateway stopped: within seconds Metadata names two brokers,
#      their partitions move, 300 more records are produced and read; the
#      gateway started again rejoins.
# SPRIG_REHEARSE_KAFKA_BOOTSTRAP (default lb:<SPRIG_REHEARSE_KAFKA_PORT>),
# SPRIG_REHEARSE_CLIENT_NET (the network kcat runs on),
# SPRIG_REHEARSE_METRICS1..3 (each gateway's telemetry address, host:port).
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
out=${1:?usage: kafka-3vm.sh <out dir>}
pw=${SPRIG_REHEARSE_PASSWORD:-rehearse-pass-0001}
mkdir -p "$out"
out=$(cd "$out" && pwd)
boot=${SPRIG_REHEARSE_KAFKA_BOOTSTRAP:-lb:${SPRIG_REHEARSE_KAFKA_PORT:?SPRIG_REHEARSE_KAFKA_PORT}}
topic=kdb.ev$(date +%H%M%S)
kc() { # kcat with the tenant's login, TLS verified against the deployment CA
    docker run --rm -i --network "$rh_client_net" -m 128m -v "$SPRIG_REHEARSE_CA:/ca.pem:ro" --platform linux/amd64 edenhill/kcat:1.7.1 \
        -b "$boot" -X security.protocol=SASL_SSL -X sasl.mechanisms=PLAIN -X sasl.username=rhk.app \
        -X "sasl.password=$pw" -X ssl.ca.location=/ca.pem "$@"
}
requests() { # <n> <api>: requests of one Kafka API VM n's gateway served so far
    local v="SPRIG_REHEARSE_METRICS$1"
    curl -fsS "http://${!v}/metrics" 2>/dev/null | awk -v api="api=\"$2\"" 'index($0, "sprig_kafka_requests_total{") == 1 && index($0, api) {s += $2} END {print s + 0}'
}
metadata() { kc -L -t "$topic" -J 2>/dev/null | jq -c '{brokers: [.brokers[] | "\(.id)@\(.name)"], leaders: ([.topics[0].partitions[] | .leader] | group_by(.) | map({(.[0] | tostring): length}) | add)}'; }

ensure_tenant rhk app "$pw"
sql rhk.app "$pw" "CREATE DATABASE IF NOT EXISTS kdb"
sql rhk.app "$pw" "CREATE STREAM $topic PARTITIONS 6"
sleep 3

say "1. Metadata through the balancer ($boot)"
metadata | tee "$out/metadata.txt"
b0=$(for n in 1 2 3; do requests "$n" Produce; done | paste -sd' ' -)

say "2. 600 keyed records, idempotent"
seq 1 600 | awk '{print "k" ($1 % 50) ":v-" $1}' |
    kc -P -t "$topic" -K: -X enable.idempotence=true -X linger.ms=5 2>&1 | tail -3 | tee "$out/produce1.txt"
b1=$(for n in 1 2 3; do requests "$n" Produce; done | paste -sd' ' -)
echo "Produce requests served by the gateways of VM 1, 2, 3: before $b0, after $b1" | tee "$out/requests.txt"

say "3. read back"
f0=$(for n in 1 2 3; do requests "$n" Fetch; done | paste -sd' ' -)
kc -C -t "$topic" -o beginning -e -q -f '%p|%o|%k|%s\n' >"$out/all1.txt"
f1=$(for n in 1 2 3; do requests "$n" Fetch; done | paste -sd' ' -)
echo "Fetch requests served by the gateways of VM 1, 2, 3: before $f0, after $f1" | tee -a "$out/requests.txt"
echo "records $(grep -c '|' "$out/all1.txt"), distinct $(cut -d'|' -f4 "$out/all1.txt" | sort -u | grep -c .), per partition: $(cut -d'|' -f1 "$out/all1.txt" | sort | uniq -c | tr -s ' ' | tr '\n' ' ')" | tee "$out/read1.txt"
kc -X auto.offset.reset=earliest -e -q -f '%p|%o|%s\n' -G rhkgrp "$topic" >"$out/group1.txt" 2>/dev/null || true
echo "consumer group rhkgrp: $(grep -c '|' "$out/group1.txt") records" | tee -a "$out/read1.txt"

say "4. VM 2's gateway stopped"
t0=$(now)
vm_dc 2 stop gateway >/dev/null 2>&1
for i in $(seq 1 30); do
    m=$(metadata)
    [[ $(jq '.brokers | length' <<<"$m") == 2 ]] && break
    sleep 1
done
echo "after $(python3 -c "print(round($(now) - $t0, 1))") s ($i tries): $m" | tee "$out/metadata-2.txt"
seq 601 900 | awk '{print "k" ($1 % 50) ":v-" $1}' |
    kc -P -t "$topic" -K: -X enable.idempotence=true -X linger.ms=5 2>&1 | tail -3 | tee "$out/produce2.txt"
kc -C -t "$topic" -o beginning -e -q -f '%p|%o|%k|%s\n' >"$out/all2.txt"
echo "records $(grep -c '|' "$out/all2.txt"), distinct $(cut -d'|' -f4 "$out/all2.txt" | sort -u | grep -c .)" | tee "$out/read2.txt"
vm_dc 2 start gateway >/dev/null 2>&1
for i in $(seq 1 60); do
    m=$(metadata)
    [[ $(jq '.brokers | length' <<<"$m") == 3 ]] && break
    sleep 2
done
echo "VM 2's gateway started again: $m" | tee -a "$out/metadata-2.txt"
