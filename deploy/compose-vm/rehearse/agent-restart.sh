#!/usr/bin/env bash
# Rehearsal: restarting the node agent of the MariaDB writer while a client
# writes through the load balancer (RUNBOOK.md, "Rehearsals").
#
#   agent-restart.sh <out dir> [upgrade image]
#
# Three events, 20 s apart, on the writer's VM:
#   1. ./sprig-vm.sh restart-agent: the restart is announced (SIGUSR1), the
#      agent hands its node over: no write should be refused;
#   2. docker compose restart agent: an unannounced restart, which fences
#      the node until the new agent has its permit and lease (writes get
#      1290 for a moment), for comparison;
#   3. with an image: ./sprig-vm.sh upgrade <image> on that VM (controller,
#      worker, agent, gateway), the path upgrades take.
# Then every acknowledged write must be in the table.
# Tenant rh (user app), created if missing; SPRIG_REHEARSE_PASSWORD sets
# its password.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
out=${1:?usage: agent-restart.sh <out dir> [upgrade image]}
img=${2:-}
pw=${SPRIG_REHEARSE_PASSWORD:-rehearse-pass-0001}
mkdir -p "$out"
out=$(cd "$out" && pwd)
ensure_tenant rh app "$pw"
sql rh.app "$pw" "CREATE DATABASE IF NOT EXISTS rh; DROP TABLE IF EXISTS rh.agent_w;
    CREATE TABLE rh.agent_w (id INT PRIMARY KEY, at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
w=$(writer_vm)
say "the writer is db$w (VM $w)"
writer_start "$out" rh.app "$pw" rh.agent_w 0.05
sleep 10
events=()
say "1. restart-agent on VM $w (announced)"
events+=("$(now)")
vm "$w" restart-agent >"$out/event1.log" 2>&1
sleep 20
agent_log() { vm_dc "$w" logs --since "$1" agent 2>&1 | grep -E 'hand-over|restart announced|fenced|read_only OFF|agent fencing' | cut -c1-400; }
agent_log 35s >"$out/agent-event1.log" || true
say "2. docker compose restart agent on VM $w (not announced)"
events+=("$(now)")
vm_dc "$w" restart agent >"$out/event2.log" 2>&1
sleep 20
agent_log 25s >"$out/agent-event2.log" || true
if [[ -n $img ]]; then
    say "3. upgrade VM $w to $img"
    events+=("$(now)")
    vm "$w" upgrade "$img" >"$out/event3.log" 2>&1
    sleep 20
    agent_log 2m >"$out/agent-event3.log" || true
fi
writer_stop "$out"
say "time-wait sockets: $(time_wait)"
sql rh.app "$pw" "SELECT id FROM rh.agent_w ORDER BY id" >"$out/ids.txt"
python3 "$rh_dir/report.py" "$out/writes.jsonl" "$out/ids.txt" "${events[@]}" | tee "$out/report.txt"
