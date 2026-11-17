#!/usr/bin/env bash
# Rehearsal: losing all three VMs' data and restoring the MariaDB
# high-availability layout from an off-VM backup (RUNBOOK.md, "Restore",
# "MariaDB high availability" and "Rehearsals").
#
#   restore-ha.sh <out dir> [--failover]
#
# W is the VM whose node is the writer when it starts (db<W>).
#  1. Tenant rhr with data (three tables, an index, a view) and rows a
#     client wrote through the load balancer; every table's checksum on
#     the writer (the tenants' data and SprigDB's catalog schemas), the
#     tenant and cluster records, and the writer's GTID position.
#  2. ./sprig-vm.sh backup on VM W (copied off the VM by SPRIG_BACKUP_COPY_TO).
#  3. Changes after the backup that the restore must undo: rows deleted, a
#     table dropped, a tenant (rhx<time>) created.
#  4. Destroy: every VM's containers and volumes removed (MariaDB, etcd and
#     the SprigDB roles' data), and VM W's local backups.
#  5. Restore: the backups' private key back on VM W for the run; restore
#     from the off-VM copy (db<W> and etcd as one member); on the other two
#     etcd-rejoin, db-seed from a stream of db<W>, up; configure on VM W.
#  6. Check: every checksum as before on all three nodes, the post-backup
#     changes gone, the replicas attached with semi-sync, logins through
#     every gateway, a new write replicated, and the writer's GTID sequence
#     carrying on from the backup's position.
#  7. With --failover: the writer's mysqld killed while the client writes;
#     the controller promotes a replica; no acknowledged write lost; the
#     old writer started again rejoins as a replica.
# Tables SprigDB rewrites by itself while it runs are left out of the
# comparison (SPRIG_REHEARSE_VOLATILE, a regular expression; by default its
# heartbeat, its change-feed clock, stream sequencers' checkpoints and the
# workload recorder's statistics).
# SPRIG_REHEARSE_OFFLINE: where the backups' private key is kept
# (backup-key.pem); SPRIG_REHEARSE_OFFVM: the off-VM copy (VM 1's
# SPRIG_BACKUP_COPY_TO). SPRIG_REHEARSE_GW1..3 as in drain.sh.
set -euo pipefail
# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
out=${1:?usage: restore-ha.sh <out dir> [--failover]}
failover=${2:-}
pw=${SPRIG_REHEARSE_PASSWORD:-rehearse-pass-0001}
offline=${SPRIG_REHEARSE_OFFLINE:?SPRIG_REHEARSE_OFFLINE: where backup-key.pem is kept}
offvm=${SPRIG_REHEARSE_OFFVM:?SPRIG_REHEARSE_OFFVM: the off-VM backup copies}
mkdir -p "$out"
out=$(cd "$out" && pwd)
export SPRIG_REHEARSE_VOLATILE=${SPRIG_REHEARSE_VOLATILE:-'^__sprig_sys\.__sprig_(heartbeat|feed_time)$|\.__sprig_sl_ckpt$|_sprig\.workload_stats$'}
late=rhx$(date +%H%M%S)
w=$(writer_vm)
others=()
for n in 1 2 3; do [[ $n == "$w" ]] || others+=("$n"); done
say "the writer is db$w (VM $w)"

say "1. tenant rhr, data, a client writing for 20 s"
ensure_tenant rhr app "$pw"
rows=$(python3 -c 'print(",".join(f"({i},MD5({i}),{i % 97})" for i in range(1, 4001)))')
sql rhr.app "$pw" "CREATE DATABASE IF NOT EXISTS r; DROP VIEW IF EXISTS r.v; DROP TABLE IF EXISTS r.a, r.b, r.w, r.after, r.fw;
    CREATE TABLE r.a (id INT PRIMARY KEY, s VARCHAR(64), g INT, KEY (g));
    INSERT INTO r.a VALUES $rows;
    CREATE TABLE r.b (id INT PRIMARY KEY AUTO_INCREMENT, note TEXT);
    INSERT INTO r.b (note) SELECT CONCAT('note ', id) FROM r.a WHERE id <= 500;
    CREATE VIEW r.v AS SELECT g, COUNT(*) AS n FROM r.a GROUP BY g;
    CREATE TABLE r.w (id INT PRIMARY KEY, at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
writer_start "$out/w1" rhr.app "$pw" r.w 0.05
sleep 20
writer_stop "$out/w1"
python3 "$rh_dir/report.py" "$out/w1/writes.jsonl" - | tee "$out/w1/report.txt"
sleep 3
db_checksums "$w" >"$out/checksums-before.txt"
vm 1 ctl -o json tenant list >"$out/tenants-before.json"
vm 1 ctl -o json cluster list >"$out/clusters-before.json"
echo "SELECT @@gtid_binlog_pos" | vm_db "$w" >"$out/gtid-before.txt"
echo "tables $(wc -l <"$out/checksums-before.txt"), db$w at GTID $(cat "$out/gtid-before.txt")"

say "2. backup on VM $w"
ts=rehearse-$(date -u +%Y%m%dT%H%M%SZ)
vm "$w" backup "$ts" | tee "$out/backup.txt"
[[ -d $offvm/$ts ]] || die "the backup wasn't copied off the VM ($offvm/$ts)"

say "3. changes after the backup"
sql rhr.app "$pw" "DELETE FROM r.a WHERE id > 3000; DROP TABLE r.b"
ensure_tenant "$late" app "$pw"

say "4. destroying every VM's containers, volumes and local backups"
t_destroy=$(now)
for n in 3 2 1; do vm_dc "$n" down -v --remove-orphans >"$out/down-vm$n.log" 2>&1; done
in_vm "VM$w" rm -rf "backups/$ts"

say "5. restore"
t0=$(now)
in_vm "VM$w" bash -c 'umask 077; cat > secrets/backup/backup-key.pem' <"$offline/backup-key.pem"
vm "$w" restore "$ts" "$offvm" 2>&1 | tee "$out/restore-vm$w.log"
in_vm "VM$w" rm -f secrets/backup/backup-key.pem
t1=$(now)
for n in "${others[@]}"; do
    vm "$n" etcd-rejoin >"$out/rejoin-vm$n.log" 2>&1
    in_vm "VM$w" ./sprig-vm.sh db-stream | in_vm "VM$n" ./sprig-vm.sh db-seed "db$w" 2>&1 | tee "$out/seed-vm$n.log"
    vm "$n" up >"$out/up-vm$n.log" 2>&1
done
vm "$w" configure >"$out/configure-vm$w.log" 2>&1
t2=$(now)
echo "restore: VM $w $(python3 -c "print(round($t1 - $t0, 1))") s; the other two (rejoin, seed, up) $(python3 -c "print(round($t2 - $t1, 1))") s; destroy to done $(python3 -c "print(round($t2 - $t_destroy, 1))") s" | tee "$out/timing.txt"

say "6. checks"
for _ in $(seq 1 30); do [[ $(writer_vm 2>/dev/null) == "$w" ]] && break; sleep 2; done
for n in 1 2 3; do vm "$n" db-status; done 2>&1 | tee "$out/db-status.txt"
for _ in $(seq 1 20); do
    db_checksums "$w" >"$out/checksums-after-db$w.txt"
    cmp -s "$out/checksums-before.txt" "$out/checksums-after-db$w.txt" && break
    sleep 3
done
for n in "${others[@]}"; do db_checksums "$n" >"$out/checksums-after-db$n.txt"; done
for n in 1 2 3; do
    if cmp -s "$out/checksums-before.txt" "$out/checksums-after-db$n.txt"; then echo "db$n: every table as before the backup"
    else echo "db$n: tables that differ from before the backup:"; diff "$out/checksums-before.txt" "$out/checksums-after-db$n.txt" | sed 's/^/   /' | head -20 || true; fi
done | tee "$out/checks.txt"
{
    echo "rows in r.a: $(sql rhr.app "$pw" "SELECT COUNT(*) FROM r.a") (4000 before the backup)"
    echo "r.b back: $(sql rhr.app "$pw" "SELECT COUNT(*) FROM r.b" 2>&1)"
    echo "r.v: $(sql rhr.app "$pw" "SELECT SUM(n) FROM r.v" 2>&1)"
    echo "tenant $late (made after the backup): $(vm 1 ctl -o json tenant list | jq -r --arg t "$late" '[.tenants[]?.name] | index($t) | if . == null then "gone" else "STILL THERE" end')"
    echo "tenants: $(vm 1 ctl -o json tenant list | jq -c '[.tenants[]?.name]') (before: $(jq -c '[.tenants[]?.name]' "$out/tenants-before.json"))"
    echo "clusters: $(vm 1 ctl -o json cluster list | jq -c '[.clusters[]? | {cluster_id, admin_state, health}]')"
    for n in 1 2 3; do echo "login through VM $n's gateway: $(sql_at "$n" rhr.app "$pw" "SELECT COUNT(*) FROM r.w" 2>&1)"; done
    sql rhr.app "$pw" "CREATE TABLE r.after (id INT PRIMARY KEY); INSERT INTO r.after VALUES (1), (2), (3)"
    sleep 2
    for n in "${others[@]}"; do echo "r.after on db$n: $(echo "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'after'" | vm_db "$n") table(s)"; done
    echo "db$w GTID: backup at $(cat "$out/gtid-before.txt"), now $(echo "SELECT @@gtid_binlog_pos" | vm_db "$w")"
} 2>&1 | tee -a "$out/checks.txt"

if [[ $failover == --failover ]]; then
    say "7. failover after the restore: db$w's mysqld killed while the client writes"
    sql rhr.app "$pw" "DROP TABLE IF EXISTS r.fw; CREATE TABLE r.fw (id INT PRIMARY KEY, at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
    writer_start "$out/w2" rhr.app "$pw" r.fw 0.05
    sleep 10
    tk=$(now)
    vm_dc "$w" kill mariadb-1 >/dev/null
    sleep 30
    writer_stop "$out/w2"
    echo "new writer: db$(writer_vm)" | tee "$out/failover.txt"
    sql rhr.app "$pw" "SELECT id FROM r.fw ORDER BY id" >"$out/w2/ids.txt"
    python3 "$rh_dir/report.py" "$out/w2/writes.jsonl" "$out/w2/ids.txt" "$tk" | tee -a "$out/failover.txt"
    vm_dc "$w" start mariadb-1 >/dev/null
    sleep 30
    vm "$w" db-status | tee -a "$out/failover.txt"
fi
say "time-wait sockets: $(time_wait)"
