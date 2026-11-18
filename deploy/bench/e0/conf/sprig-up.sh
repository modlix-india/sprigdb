#!/usr/bin/env bash
# Runs inside the e0bench-sprig container (stack.sh up sprig): today's SprigDB
# as one VM runs it, with the controller, one worker and one gateway sharing
# the container's loopback, over etcd (e0bench-etcd) and MariaDB
# (e0bench-sprigmaria). The same settings as the market benchmark's SprigDB:
# client listeners in plain TCP, backend TLS skip_verify, 96 backend
# connections per account, a native-password app user, and only the MySQL and
# Redis listeners on. Writes /var/lib/sprig/READY when the tenant answers, then
# waits on the processes (the container ends when one of them ends).
set -euo pipefail
B=/opt/sprig H=/var/lib/sprig X=/opt/e0conf-x
ETCD=http://e0bench-etcd:2379 ADMIN=http://127.0.0.1:7480 DC=dc-e0
BASE=${E0_TENANT_BASE:?}
mkdir -p "$H/logs"
say() { echo "sprig-up: $(date '+%T') $*"; }
die() { say "$*"; exit 1; }
ctl() { "$B/sprigctl" --endpoint "$ADMIN" "$@"; }
etcd_put() { curl -fsS -X POST "$ETCD/v3/kv/put" -d "$(jq -cn --arg k "$1" --arg v "$2" '{key: ($k|@base64), value: ($v|@base64)}')" >/dev/null; }
meta() { jq -cn --argjson v "$1" '{v: 1, value: $v, set_by: "op:e0bench", reason: "e0 benchmark before arm"}'; }
set_setting() { etcd_put "/sprig/dc/$DC/settings/$1" "$(meta "$2")"; }
set_global() { etcd_put "/sprig/g/settings/$1" "$(meta "$2")"; }
start() { local name=$1 config=$2 role=$3; shift 3
    env "$@" "$B/sprigdb" --config "$config" "$role" >>"$H/logs/$name.log" 2>&1 </dev/null &
    echo $! >"$H/$name.pid"; say "$name pid $!"; }
wait_ready() { local i
    for i in $(seq 1 240); do curl -fsS -o /dev/null "$2" 2>/dev/null && { say "$1 ready"; return; }
        kill -0 "$(cat "$H/$1.pid")" 2>/dev/null || { tail -30 "$H/logs/$1.log"; die "$1 exited"; }; sleep 0.5; done
    tail -30 "$H/logs/$1.log"; die "$1 not ready"; }

for _ in $(seq 1 60); do curl -fsS "$ETCD/health" >/dev/null 2>&1 && break; sleep 1; done
common() { printf '[node]\ndc = "%s"\nid = "%s"\n[meta]\ndc_seeds = ["%s"]\nglobal_seeds = []\ndeployment_id = "e0bench"\n' "$DC" "$1" "$ETCD"; }
{ common e0-ctl; printf '[controller]\nbootstrap = true\nadmin_addr = "127.0.0.1:7480"\nworkload_key_file = "%s/workload-keys.dev"\n[telemetry]\nbootstrap_log_level = "info"\nlog_format = "json"\nbootstrap_addr = "127.0.0.1:9180"\n' "$X"; } >"$H/controller.toml"
{ common e0-gw; printf '[gateway]\nlogin_mode = "catalog"\nbackends_file = "%s/backends.toml"\nadvertise_addr = "127.0.0.1"\n[gateway.listen_override]\nmysql = "0.0.0.0:3306"\nmariadb = "0.0.0.0:3307"\nredis = "0.0.0.0:6379"\nmongo = "127.0.0.1:27117"\namqp = "127.0.0.1:5772"\nkafka = "127.0.0.1:9192"\nhttp = "127.0.0.1:8080"\nhttps = "127.0.0.1:8443"\npeer = "127.0.0.1:7403"\nbolt = "127.0.0.1:7787"\n[telemetry]\nbootstrap_log_level = "info"\nlog_format = "json"\nbootstrap_addr = "127.0.0.1:9190"\n' "$H"; } >"$H/gateway.toml"
(umask 077 && printf '[default]\nbackend_user = "root"\nbackend_credentials = "file:backends.toml#backend_root"\n[backend_root]\nuser = "root"\npassword = "%s"\ntls = "skip_verify"\n' "$E0_ROOT_PW" >"$H/backends.toml")
(umask 077 && printf '[sprig_ctl]\nuser = "root"\npassword = "%s"\ntls = "skip_verify"\n[sprig_repl]\nuser = "root"\npassword = "%s"\ntls = "skip_verify"\n' "$E0_ROOT_PW" "$E0_ROOT_PW" >"$H/creds.toml")

ulimit -n 65536 || true
set_global tenant.id_min "$BASE"
set_global tenant.id_max "$((BASE + 99999))"
start controller "$H/controller.toml" controller
wait_ready controller http://127.0.0.1:9180/readyz
for _ in $(seq 1 60); do ctl -o json dc show "$DC" 2>/dev/null | jq -e '.leader != null' >/dev/null && break; sleep 0.5; done
ctl -o json cluster list | jq -e '.clusters[] | select(.name == "e0-single")' >/dev/null 2>&1 ||
    ctl cluster register e0-single --topology single --node e0bench-sprigmaria:3306 --credentials "file:$H/creds.toml" >/dev/null
set_setting tls.bundles.default.cert_path "\"$X/certs/server.pem\""
set_setting tls.bundles.default.key_path "\"$X/certs/server-key.pem\""
set_setting listener.mysql.tls '"off"'
set_setting listener.mariadb.tls '"off"'
set_setting listener.redis.tls '"off"'
for l in http https bolt mongo amqp kafka; do set_setting "listener.$l.enabled" false; done
set_setting listener.redis.enabled true
set_setting backend.max_conns_per_account_node 96
set_setting failover.self_fence false
start worker "$H/controller.toml" worker SPRIGDB_NODE_ID=e0-wkr SPRIGDB_TELEMETRY_BOOTSTRAP_ADDR=127.0.0.1:9185
wait_ready worker http://127.0.0.1:9185/readyz
start gateway "$H/gateway.toml" gateway SPRIGDB_GATEWAY_ADVERTISE_ADDR=127.0.0.1
wait_ready gateway http://127.0.0.1:9190/readyz

cid=$(ctl -o json cluster list | jq -r '.clusters[] | select(.name == "e0-single") | .cluster_id')
ctl tenant create bench --cluster "$cid" >/dev/null 2>&1 || true
tid=$(ctl -o json tenant show bench | jq -r '.tenant_id')
(( tid >= BASE && tid <= BASE + 99999 )) || die "tenant id $tid outside $BASE..+99999"
ctl tenant user add bench app --password "$E0_APP_PW" --native --if-not-exists >/dev/null
ctl settings set auth.mysql_native_password true --scope tenant:bench --reason e0bench --yes >/dev/null
python3 -I /opt/e0/lib/mysqlwire.py --host 127.0.0.1 --port 3306 --user bench.app --password "$E0_APP_PW" --retry 90 \
    -e "CREATE DATABASE IF NOT EXISTS sbtest; CREATE DATABASE IF NOT EXISTS e0diff; CREATE DATABASE IF NOT EXISTS e0crash; CREATE DATABASE IF NOT EXISTS kv" ||
    { tail -20 "$H/logs/gateway.log"; die "tenant login failed"; }
say "tenant bench id $tid; databases sbtest, e0diff, kv"
touch "$H/READY"
wait -n
say "a process ended; stopping"
