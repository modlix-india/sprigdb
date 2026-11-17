#!/usr/bin/env bash
# Rehearsal: two load balancers in front of the gateways (RUNBOOK.md, "Load
# balancer" and "Rehearsals").
#
#   two-lb.sh <out dir>
#
# Two Envoys with the same configuration, each health-checking every
# gateway. Clients reach them by one name that resolves to the balancers
# that run (here Docker's DNS name "lb", which leaves out a stopped
# container, as a DNS record with health checks or a client's list of
# hosts would). A client writes through the name, on a new connection
# every 20 writes (once a second), while:
#   1. balancer 1 is stopped, then started again;
#   2. balancer 2 is stopped, then started again;
#   3. VM 3's gateway is stopped and started (both balancers drop it, then
#      take it back).
# Each balancer's view of the gateways (Envoy's /clusters) is recorded. A
# floating address (VRRP with keepalived) is the other way to give the two
# balancers one address: config/keepalived.conf.example and RUNBOOK.md
# describe it; it needs an approved keepalived image to rehearse.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
out=${1:?usage: two-lb.sh <out dir>}
pw=${SPRIG_REHEARSE_PASSWORD:-rehearse-pass-0001}
mkdir -p "$out"
out=$(cd "$out" && pwd)
lb_dc() { local n=$1; shift; in_vm "LB$n" docker compose --project-directory . -f compose.lb.yaml -f laptop-lb.yaml "$@"; }
lb_health() { # <n>: each gateway's health flags in balancer n
    local port
    port=$(in_vm "LB$1" bash -c '. ./.env; echo ${SPRIG_LB_ADMIN_PORT:-9901}')
    curl -fsS "http://127.0.0.1:$port/clusters" 2>/dev/null | awk -F'::' '$1 == "mysql" && $3 == "health_flags" {printf "%s=%s ", $2, $4}'
    echo
}
ensure_tenant rh app "$pw"
sql rh.app "$pw" "CREATE DATABASE IF NOT EXISTS rh; DROP TABLE IF EXISTS rh.lb_w;
    CREATE TABLE rh.lb_w (id INT PRIMARY KEY, at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
{ echo "lb1: $(lb_health 1)"; echo "lb2: $(lb_health 2)"; } | tee "$out/health-start.txt"
writer_start "$out" rh.app "$pw" rh.lb_w 0.05 20
sleep 10
events=()
for n in 1 2; do
    say "balancer $n stopped"
    events+=("$(now)")
    lb_dc "$n" stop lb >/dev/null 2>&1
    sleep 15
    say "balancer $n started"
    lb_dc "$n" start lb >/dev/null 2>&1
    sleep 15
done
say "VM 3's gateway stopped"
events+=("$(now)")
vm_dc 3 stop gateway >/dev/null 2>&1
sleep 5
{ echo "lb1: $(lb_health 1)"; echo "lb2: $(lb_health 2)"; } | tee "$out/health-gw3-stopped.txt"
vm_dc 3 start gateway >/dev/null 2>&1
sleep 20
{ echo "lb1: $(lb_health 1)"; echo "lb2: $(lb_health 2)"; } | tee "$out/health-end.txt"
writer_stop "$out"
say "time-wait sockets: $(time_wait)"
sql rh.app "$pw" "SELECT id FROM rh.lb_w ORDER BY id" >"$out/ids.txt"
python3 "$rh_dir/report.py" "$out/writes.jsonl" "$out/ids.txt" "${events[@]}" | tee "$out/report.txt"
