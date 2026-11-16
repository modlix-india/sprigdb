#!/usr/bin/env bash
# Rehearsal: moving a one-VM bundle's tenants onto the highly available
# cluster with `cluster drain` (RUNBOOK.md, "From one VM to three" and
# "Rehearsals"), and changing a registration's node address with
# `cluster set-nodes`.
#
#   drain.sh <out dir>
#
# 1. On VM 1, a second MariaDB (compose.mariadb-2.example.yaml) stands in
#    for the old bundle's, registered as cluster dc1-old (single).
# 2. Two tenants on it with data.
# 3. `cluster set-nodes` points the record at the Compose name
#    mariadb-2:3306, as a one-VM bundle registered its MariaDB: logins
#    through VM 2's and VM 3's gateways fail (only VM 1 resolves that
#    name); `cluster set-nodes` back to VM 1's private address, and they
#    work. (Registering at the Compose name directly is refused when the
#    DC leader's controller runs on another VM: its prechecks can't reach
#    the node.)
# 4. A client writes to one of the tenants while `cluster drain` moves
#    both onto dc1-main (the three-node cluster); the drain must end
#    `drained`, with every acknowledged write and every row's fingerprint
#    the same before and after.
# 5. `cluster retire`, and the second MariaDB is removed.
# SPRIG_REHEARSE_GW1..3: each VM's gateway MySQL address (host:port).
# SPRIG_REHEARSE_OLD_ADDR: the old MariaDB's private address (default VM 1's
# SPRIG_PRIVATE_HOST:SPRIG_BACKEND_PORT_2).
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
out=${1:?usage: drain.sh <out dir>}
pw=${SPRIG_REHEARSE_PASSWORD:-rehearse-pass-0001}
mkdir -p "$out"
out=$(cd "$out" && pwd)
v1() { in_vm VM1 bash -c ". ./.env; echo \${$1:-$2}"; }
addr=${SPRIG_REHEARSE_OLD_ADDR:-$(v1 SPRIG_PRIVATE_HOST mariadb-2):$(v1 SPRIG_BACKEND_PORT_2 3316)}

say "1. VM 1: a second MariaDB as the old bundle's (mariadb-2)"
in_vm VM1 bash -c 'set -e
    [ -f compose.mariadb-2.yaml ] || cp compose.mariadb-2.example.yaml compose.mariadb-2.yaml
    . ./.env
    case ":${COMPOSE_FILE:-compose.yaml}:" in *:compose.mariadb-2.yaml:*) ;; *)
        sed -i.bak "s|^COMPOSE_FILE=.*|COMPOSE_FILE=${COMPOSE_FILE:-compose.yaml}:compose.mariadb-2.yaml|" .env && rm -f .env.bak ;; esac'
vm_dc 1 up -d --no-deps --wait mariadb-2
old=$(vm 1 ctl -o json cluster list | jq -r '.clusters[]? | select(.name == "dc1-old") | .cluster_id' | head -1)
if [[ -z $old ]]; then
    old=$(vm 1 ctl -o json cluster register dc1-old --dc dc1 --topology single --node "n1=$addr" \
        --credentials file:/run/sprig/sprig/cluster-credentials.toml --memory-gb 1 --storage-gb 20 --yes | jq -r .cluster.cluster_id)
fi
[[ -n $old && $old != null ]] || die "dc1-old didn't register"
vm 1 ctl -o json cluster show "$old" >"$out/old-before.json"
say "dc1-old is $old, node $(jq -c '[.nodes[]? | {node, addr}]' "$out/old-before.json" 2>/dev/null)"

say "2. tenants rhold1 and rhold2 on $old, with data"
for t in rhold1 rhold2; do
    ensure_tenant "$t" app "$pw" "$old"
    rows=$(python3 -c 'print(",".join(f"({i},MD5({i}),{i * 7},JSON_OBJECT(\"k\",{i % 13}))" for i in range(1, 3001)))')
    sql "$t.app" "$pw" "CREATE DATABASE IF NOT EXISTS d; DROP TABLE IF EXISTS d.t1, d.w;
        CREATE TABLE d.t1 (id INT PRIMARY KEY, v VARCHAR(64), n INT, j JSON, KEY (n));
        INSERT INTO d.t1 VALUES $rows;
        CREATE TABLE d.w (id INT PRIMARY KEY, at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
done
fp() { # <tenant>: the rows' fingerprint, through the load balancer
    sql "$1.app" "$pw" "SELECT COUNT(*), BIT_XOR(CRC32(CONCAT_WS('|', id, v, n, j))) FROM d.t1"
}

say "3. the record at the Compose name (a one-VM bundle's), then back with cluster set-nodes"
logins() { local n r=""; for n in 1 2 3; do sql_at "$n" rhold1.app "$pw" "SELECT COUNT(*) FROM d.t1" >/dev/null 2>&1 && r+=" vm$n:ok" || r+=" vm$n:FAILED"; done; echo "$r"; }
node=$(jq -r '.nodes[0].node // "n1"' "$out/old-before.json")
echo "registered at $addr:$(logins)" | tee "$out/logins.txt"
vm 1 ctl cluster set-nodes "$old" --node "$node=mariadb-2:3306" --yes | tee "$out/set-nodes-compose-name.txt"
sleep 8
echo "at mariadb-2:3306 (a one-VM bundle's record):$(logins)" | tee -a "$out/logins.txt"
vm 1 ctl cluster set-nodes "$old" --node "$node=$addr" --dry-run | tee "$out/set-nodes-dry-run.txt"
vm 1 ctl cluster set-nodes "$old" --node "$node=$addr" --yes | tee "$out/set-nodes.txt"
sleep 8
echo "after set-nodes back to $addr:$(logins)" | tee -a "$out/logins.txt"
before1=$(fp rhold1) before2=$(fp rhold2)
echo "fingerprints before: rhold1 $before1, rhold2 $before2" | tee "$out/fingerprints.txt"

say "4. a client writes to rhold1 while $old drains onto dc1-main"
writer_start "$out" rhold1.app "$pw" d.w 0.05
sleep 5
t0=$(now)
vm 1 ctl cluster drain "$old" --yes | tee "$out/drain.txt"
state=
for _ in $(seq 1 180); do
    state=$(vm 1 ctl -o json cluster list | jq -r --arg id "$old" '.clusters[]? | select(.cluster_id == $id) | .admin_state')
    [[ $state == drained ]] && break
    sleep 5
done
t1=$(now)
echo "drain: $state after $(python3 -c "print(round($t1 - $t0, 1))") s" | tee -a "$out/drain.txt"
sleep 5
writer_stop "$out"
say "time-wait sockets: $(time_wait)"
for t in rhold1 rhold2; do vm 1 ctl -o json tenant show "$t" >"$out/$t-after.json" || true; done
sql rhold1.app "$pw" "SELECT id FROM d.w ORDER BY id" >"$out/ids.txt"
after1=$(fp rhold1) after2=$(fp rhold2)
echo "fingerprints after: rhold1 $after1, rhold2 $after2" | tee -a "$out/fingerprints.txt"
[[ $before1 == "$after1" && $before2 == "$after2" ]] && echo "fingerprints: same" | tee -a "$out/fingerprints.txt" ||
    echo "fingerprints: DIFFERENT" | tee -a "$out/fingerprints.txt"
echo "SHOW DATABASES" | vm_db 1 | grep -E '^t[0-9]+_' >"$out/db1-schemas.txt" || true
in_vm VM1 docker compose --project-directory . exec -T mariadb-2 bash -c \
    'MYSQL_PWD=$(cat /run/sprig/mariadb/root-password) mariadb -uroot -h127.0.0.1 --ssl-ca=/run/sprig/tls/ca.pem -N -e "SHOW DATABASES"' |
    grep -E '^t[0-9]+_' >"$out/old-schemas.txt" || true
echo "tenant schemas: on db1 $(wc -l <"$out/db1-schemas.txt"), left on the old MariaDB $(wc -l <"$out/old-schemas.txt")" | tee -a "$out/drain.txt"
python3 "$rh_dir/report.py" "$out/writes.jsonl" "$out/ids.txt" "$t0" "$t1" | tee "$out/report.txt"

if [[ $state == drained && ${SPRIG_REHEARSE_KEEP_OLD:-} != 1 ]]; then
    say "5. retire $old and remove the second MariaDB"
    vm 1 ctl cluster retire "$old" --yes | tee "$out/retire.txt"
    vm_dc 1 rm -sf mariadb-2
    in_vm VM1 bash -c 'sed -i.bak "s|:compose.mariadb-2.yaml||" .env && rm -f .env.bak'
    in_vm VM1 bash -c '. ./.env; docker volume rm "${COMPOSE_PROJECT_NAME:-sprigdb}_mariadb-2-data" >/dev/null' || true
fi
