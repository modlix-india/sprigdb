#!/usr/bin/env bash
# An engine-only SprigDB stack on this machine, for the live test suites and
# the checkers in tools/checkers: one etcd container, a controller and one
# to five gateways, each gateway with its own engine directory. With three
# or more gateways every tenant's unit is replicated over Raft. No MariaDB.
#
#   tools/live/stack.sh up [N]                 start etcd, the controller and N gateways (default 1)
#   tools/live/stack.sh tenant <name> [pw]     create tenant <name> and its login <name>.app
#                                              (password pw, default sprig-live-app)
#   tools/live/stack.sh env [tenant] [pw]      print the stack's addresses as VAR=value lines
#   tools/live/stack.sh start|stop|kill gwN    one gateway: start, stop (SIGTERM), kill -9
#   tools/live/stack.sh status | down | purge  down stops the processes and etcd; purge also
#                                              deletes the state directory
#   tools/live/stack.sh ctl <sprigctl args>    sprigctl against this stack's controller
#
# Variables:
#   SPRIG_LIVE_BIN    directory holding sprigdb and sprigctl (default target/debug;
#                     build them with `cargo build -p sprigdb -p sprigctl`)
#   SPRIG_LIVE_STATE  state directory: logs, engine directories, configs
#                     (default target/live-stack)
#   SPRIG_LIVE_PORT   port base B (default 45000). Ports, all on 127.0.0.1:
#                       etcd B+79; controller admin B+80, readyz B+81;
#                       gateway n (1..5): bolt B+100n, http +1, kafka +2, peer +3,
#                       mongo +4, amqp +5, mysql +6, mariadb +7, redis +9;
#                       Raft B+70+n; readyz B+90+n
#   SPRIG_LIVE_NAME   name of the etcd container (default sprig-live-etcd)
#   SPRIG_LIVE_RAFT_SYNC_PIPELINE  passed to the gateways as SPRIGDB_ENGINE_RAFT_SYNC_PIPELINE
#   SPRIG_LIVE_PLACEMENT  `on`: each tenant's unit only on the nodes its descriptor
#                     names (SPRIGDB_ENGINE_PLACEMENT); default: every node holds every unit
#
# Tenant ids come from B * 1,000,000 up (45,000,000,000 by default), below
# 2^38. Nothing here touches the default ports of other servers.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
B=${SPRIG_LIVE_PORT:-45000}
S=${SPRIG_LIVE_STATE:-$ROOT/target/live-stack}
BIN=${SPRIG_LIVE_BIN:-$ROOT/target/debug}
NAME=${SPRIG_LIVE_NAME:-sprig-live-etcd}
ETCD_PORT=$((B + 79))
ETCD=http://127.0.0.1:$ETCD_PORT
ADMIN=http://127.0.0.1:$((B + 80))
ID_BASE=$((B * 1000000))
mkdir -p "$S"

say() { echo "stack: $*"; }
die() {
    echo "stack: $*" >&2
    exit 1
}
need() { command -v "$1" >/dev/null || die "needs $1 on the path"; }
port() { # port <gateway n> <listener>
    local n=$1 off
    case $2 in
    bolt) off=0 ;; http) off=1 ;; kafka) off=2 ;; peer) off=3 ;; mongo) off=4 ;;
    amqp) off=5 ;; mysql) off=6 ;; mariadb) off=7 ;; redis) off=9 ;;
    raft) echo $((B + 70 + n)); return ;; readyz) echo $((B + 90 + n)); return ;;
    *) die "unknown listener $2" ;;
    esac
    echo $((B + 100 * n + off))
}
ctl() { "$BIN/sprigctl" --endpoint "$ADMIN" "$@"; }
b64() { printf '%s' "$1" | base64 | tr -d '\n'; }
etcd_put() {
    curl -fsS -X POST "$ETCD/v3/kv/put" \
        -d "$(jq -cn --arg k "$(b64 "$1")" --arg v "$(b64 "$2")" '{key: $k, value: $v}')" >/dev/null
}
record() { jq -cn --argjson v "$1" '{v: 1, value: $v, set_by: "op:live-stack", reason: "local live-test stack"}'; }
set_dc() { etcd_put "/sprig/dc/dc-a/settings/$1" "$(record "$2")"; }
set_global() { etcd_put "/sprig/g/settings/$1" "$(record "$2")"; }

pid_of() { cat "$S/$1.pid" 2>/dev/null || true; }
running() {
    local p
    p=$(pid_of "$1")
    [[ -n $p ]] && kill -0 "$p" 2>/dev/null
}
readyz_port() { case $1 in controller) echo $((B + 81)) ;; gw[1-5]) port "${1#gw}" readyz ;; esac; }
ready() {
    local p
    p=$(readyz_port "$1")
    for _ in $(seq 1 240); do
        curl -fsS -o /dev/null "http://127.0.0.1:$p/readyz" 2>/dev/null && return 0
        running "$1" || return 1
        sleep 0.5
    done
    return 1
}
gateways() { cat "$S/gateways" 2>/dev/null || echo 1; }

write_configs() {
    local n=$1 i
    cat >"$S/controller.toml" <<EOF
[node]
dc = "dc-a"
id = "live-ctl-1"
[meta]
dc_seeds = ["$ETCD"]
global_seeds = []
deployment_id = "sprig-live"
[controller]
bootstrap = true
workload_key_file = "$ROOT/deploy/compose/workload-keys.dev"
[telemetry]
bootstrap_log_level = "info"
log_format = "pretty"
bootstrap_addr = "127.0.0.1:$((B + 81))"
EOF
    for i in $(seq 1 "$n"); do
        cat >"$S/gw$i.toml" <<EOF
[node]
dc = "dc-a"
id = "live-gw-$i"
[meta]
dc_seeds = ["$ETCD"]
global_seeds = []
deployment_id = "sprig-live"
[gateway]
login_mode = "catalog"
advertise_addr = "127.0.0.1"
backends_file = "$ROOT/deploy/compose/gateway-backends.dev.toml"
workload_key_file = "$ROOT/deploy/compose/workload-keys.dev"
[gateway.listen_override]
peer = "127.0.0.1:$(port "$i" peer)"
mysql = "127.0.0.1:$(port "$i" mysql)"
mariadb = "127.0.0.1:$(port "$i" mariadb)"
redis = "127.0.0.1:$(port "$i" redis)"
http = "127.0.0.1:$(port "$i" http)"
mongo = "127.0.0.1:$(port "$i" mongo)"
kafka = "127.0.0.1:$(port "$i" kafka)"
amqp = "127.0.0.1:$(port "$i" amqp)"
bolt = "127.0.0.1:$(port "$i" bolt)"
[telemetry]
bootstrap_log_level = "info"
log_format = "json"
bootstrap_addr = "127.0.0.1:$(port "$i" readyz)"
EOF
    done
}

start() {
    local name=$1 role cfg extra=()
    case $name in
    controller) role=controller cfg=$S/controller.toml extra=("SPRIGDB_CONTROLLER_ADMIN_ADDR=127.0.0.1:$((B + 80))") ;;
    gw[1-5])
        role=gateway cfg=$S/$name.toml
        local n=${name#gw} peers
        peers=$(for i in $(seq 1 "$(gateways)"); do printf '%s=127.0.0.1:%s,' "$i" "$(port "$i" raft)"; done)
        mkdir -p "$S/engine-$n"
        : >>"$S/cut-$n"
        extra=(SPRIGDB_GATEWAY_ADVERTISE_ADDR=127.0.0.1 "SPRIGDB_ENGINE_DIR=$S/engine-$n"
            SPRIGDB_ENGINE_MB="${SPRIG_LIVE_ENGINE_MB:-256}"
            "SPRIGDB_ENGINE_RAFT_ID=$n" "SPRIGDB_ENGINE_RAFT_PEERS=${peers%,}"
            "SPRIGDB_ENGINE_RAFT_LISTEN=127.0.0.1:$(port "$n" raft)"
            "SPRIGDB_ENGINE_RAFT_CUT_FILE=$S/cut-$n")
        [[ -n ${SPRIG_LIVE_PLACEMENT:-} ]] && extra+=("SPRIGDB_ENGINE_PLACEMENT=$SPRIG_LIVE_PLACEMENT")
        [[ -n ${SPRIG_LIVE_RAFT_SYNC_PIPELINE:-} ]] &&
            extra+=("SPRIGDB_ENGINE_RAFT_SYNC_PIPELINE=$SPRIG_LIVE_RAFT_SYNC_PIPELINE")
        ;;
    *) die "unknown process $name" ;;
    esac
    [[ -f $cfg ]] || die "$cfg is missing: run up first"
    if running "$name"; then
        say "$name already running"
        return
    fi
    [[ -x $BIN/sprigdb ]] || die "no $BIN/sprigdb: build it (cargo build -p sprigdb -p sprigctl) or set SPRIG_LIVE_BIN"
    # The process itself is the background job (env and nohup exec it), so
    # its pid is the one stop and kill signal.
    (
        cd "$ROOT" || exit 1
        env "${extra[@]}" nohup "$BIN/sprigdb" --config "$cfg" "$role" >>"$S/$name.log" 2>&1 </dev/null &
        echo $! >"$S/$name.pid"
    )
    say "$name started (pid $(pid_of "$name"))"
}
stop() {
    local p
    p=$(pid_of "$1")
    [[ -n $p ]] && kill -TERM "$p" 2>/dev/null || true
    for _ in $(seq 1 60); do
        running "$1" || break
        sleep 0.5
    done
    running "$1" && kill -KILL "$(pid_of "$1")" 2>/dev/null
    rm -f "$S/$1.pid"
}
kill9() {
    local p
    p=$(pid_of "$1")
    [[ -n $p ]] && kill -KILL "$p" 2>/dev/null || true
    rm -f "$S/$1.pid"
}

up() {
    local n=${1:-1} i
    [[ $n =~ ^[1-5]$ ]] || die "1 to 5 gateways"
    need docker
    need curl
    need jq
    echo "$n" >"$S/gateways"
    write_configs "$n"
    if ! docker inspect "$NAME" >/dev/null 2>&1; then
        docker run -d --name "$NAME" --memory 256m -p "127.0.0.1:$ETCD_PORT:2379" \
            quay.io/coreos/etcd:v3.6.5 etcd --name=etcd-0 --data-dir=/etcd-data \
            --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls="$ETCD" \
            --auto-compaction-mode=periodic --auto-compaction-retention=1h >/dev/null
    else
        docker start "$NAME" >/dev/null
    fi
    for _ in $(seq 1 60); do
        curl -fsS "$ETCD/health" >/dev/null 2>&1 && break
        sleep 0.5
    done
    set_global tenant.id_min "$ID_BASE"
    set_global tenant.id_max "$((ID_BASE + 99999))"
    set_dc tls.bundles.default.cert_path "\"$ROOT/deploy/compose/gateway-certs/server.pem\""
    set_dc tls.bundles.default.key_path "\"$ROOT/deploy/compose/gateway-certs/server-key.pem\""
    for l in mysql mariadb redis mongo bolt; do set_dc "listener.$l.tls" '"optional"'; done
    for l in redis http mongo kafka amqp bolt; do set_dc "listener.$l.enabled" true; done
    for l in http kafka amqp; do set_dc "listener.$l.tls" '"off"'; done
    set_dc listener.https.enabled false
    set_dc listener.amqps.enabled false
    set_dc failover.self_fence false
    start controller
    ready controller || {
        tail -30 "$S/controller.log"
        die "the controller didn't become ready"
    }
    for _ in $(seq 1 60); do
        ctl -o json dc show dc-a 2>/dev/null | jq -e '.leader != null' >/dev/null && break
        sleep 0.5
    done
    for i in $(seq 1 "$n"); do start "gw$i"; done
    for i in $(seq 1 "$n"); do
        ready "gw$i" || {
            tail -30 "$S/gw$i.log"
            die "gw$i didn't become ready"
        }
    done
    say "up: $n gateway(s); gw1 mysql 127.0.0.1:$(port 1 mysql), admin $ADMIN, state $S"
}

tenant() {
    local t=${1:?tenant name} pw=${2:-sprig-live-app} id
    ctl tenant create "$t" --yes >/dev/null 2>&1 || true
    id=$(ctl -o json tenant show "$t" | jq -r '.tenant_id // .id')
    if ((id < ID_BASE || id > ID_BASE + 99999)); then die "tenant $t has id $id, outside $ID_BASE..+99999"; fi
    for _ in $(seq 1 20); do
        ctl tenant user add "$t" app --password "$pw" --native --if-not-exists --yes >/dev/null 2>&1 && break
        sleep 2
    done
    ctl settings set auth.mysql_native_password true --scope "tenant:$t" --reason live-stack --yes >/dev/null 2>&1 || true
    echo "$t $id" >>"$S/tenants"
    say "tenant $t ($id), login $t.app"
}

env_lines() {
    local t=${1:-live} pw=${2:-sprig-live-app} n i
    n=$(gateways)
    echo "SPRIG_LIVE_ETCD=$ETCD"
    echo "SPRIG_LIVE_ADMIN=$ADMIN"
    echo "SPRIG_LIVE_SPRIGCTL=$BIN/sprigctl --endpoint $ADMIN"
    echo "SPRIG_LIVE_TENANT=$t"
    echo "SPRIG_LIVE_USER=$t.app"
    echo "SPRIG_LIVE_PASSWORD=$pw"
    for i in $(seq 1 "$n"); do
        for l in mysql mariadb redis mongo http kafka amqp bolt; do
            echo "SPRIG_LIVE_GW${i}_$(echo "$l" | tr '[:lower:]' '[:upper:]')=127.0.0.1:$(port "$i" "$l")"
        done
    done
}

status() {
    local p
    for p in controller gw1 gw2 gw3 gw4 gw5; do
        if running "$p"; then echo "$p up $(pid_of "$p")"; elif [[ -f $S/$p.toml ]]; then echo "$p down"; fi
    done
    docker inspect -f '{{.State.Status}}' "$NAME" 2>/dev/null | sed "s/^/$NAME /" || true
}
down() {
    local p
    for p in gw5 gw4 gw3 gw2 gw1 controller; do stop "$p"; done
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    say "down"
}

cmd=${1:-status}
shift || true
case $cmd in
up) up "${1:-1}" ;;
tenant) tenant "$@" ;;
env) env_lines "$@" ;;
start)
    start "$1"
    ready "$1"
    ;;
stop) stop "$1" ;;
kill) kill9 "$1" ;;
status) status ;;
down) down ;;
purge)
    down
    rm -rf "$S"
    ;;
ctl) ctl "$@" ;;
port) port "$@" ;;
*) die "unknown command $cmd (up, tenant, env, start, stop, kill, status, down, purge, ctl, port)" ;;
esac
