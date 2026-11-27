#!/usr/bin/env bash
# The deployment CI's "S1 integration (Compose, one DC)" job runs its
# gateway-level live tests against, and the SPRIG_TEST_* variables that
# point them at it. Locally it reproduces the job on your own ports.
#
#   deploy/compose/integration.sh up     # gateway-dev.sh up, then the extras below
#   source "$SPRIG_GATEWAY_DEV_STATE/test-env"   # (CI: appended to $GITHUB_ENV)
#   cargo nextest run --workspace --profile ci-s1
#   deploy/compose/integration.sh down
#
# up runs gateway-dev.sh up (catalog mode: tenants acme and globex on
# dc-a-c1, users acme.app and globex.app) and then adds what the tests need
# on top:
# - the http (plain), https and mongo listeners, each bound before it is
#   enabled (ports SPRIG_IT_HTTP_PORT 13080, SPRIG_IT_HTTPS_PORT 13443,
#   SPRIG_IT_MONGO_PORT 13017);
# - acme's sink.destination_allowlist = ["127.0.0.1:9"] (the process-list
#   secrets test makes a connection to it; nothing listens there);
# - the tenant tree ig-a > ig-b > ig-c, each with the user app, for the
#   inheritable-groups and record-hierarchy suites;
# and writes $state/test-env: one VAR=value line per variable. Which tests
# read which variable, and the tests this deployment can't serve, is in
# .config/nextest.toml.
#
# Everything else (ports, etcd, tenant id range, state directory) is
# gateway-dev.sh's: its variables apply unchanged. down is gateway-dev.sh
# down (it deletes the tenants on a deployment whose etcd up bootstrapped,
# ig-* included).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

gwdev=deploy/compose/gateway-dev.sh
state=${SPRIG_GATEWAY_DEV_STATE:-deploy/compose/.gateway-dev}
state=${state%/}
http_port=${SPRIG_IT_HTTP_PORT:-13080}
https_port=${SPRIG_IT_HTTPS_PORT:-13443}
mongo_port=${SPRIG_IT_MONGO_PORT:-13017}
password=sprig-dev-app

say() { echo "integration: $*"; }
die() {
    echo "integration: $*" >&2
    exit 1
}

listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Runs SQL through the gateway's MySQL listener as $1 (password
# sprig-dev-app), over TLS when the gateway offers it: a cold caching_sha2 login over
# plain TCP fails with 2061 until the gateway serves its RSA public key: with the mysql client when there is one, else with
# compose.yaml's MariaDB image (host network, Linux).
sql_as() {
    if command -v mysql >/dev/null; then
        MYSQL_PWD=$password mysql -h 127.0.0.1 -P "$mysql_port" -u "$1" --ssl-mode=PREFERRED -e "$2"
    else
        docker run --rm --network host "${MARIADB_IMAGE:-mariadb:11.8}" \
            mariadb -h 127.0.0.1 -P "$mysql_port" -u "$1" -p"$password" --skip-ssl -e "$2"
    fi
}

up() {
    "$gwdev" up
    # gateway-dev.sh recorded its values (ports, binaries) in $state.
    local admin_port mysql_port mariadb_port gw_telemetry_port
    # shellcheck disable=SC1091 # written by gateway-dev.sh up
    source "$state/env"
    : "${admin_port:?}" "${mysql_port:?}" "${mariadb_port:?}" "${gw_telemetry_port:?}"
    local bin
    bin=$(cat "$state/bin")
    ctl() { "$bin/sprigctl" --endpoint "http://127.0.0.1:$admin_port" "$@"; }
    set_dc() { ctl settings set "$1" "$2" --scope dc:dc-a --reason "deploy/compose/integration.sh" >/dev/null; }

    set_dc listener.http.bind "127.0.0.1:$http_port"
    set_dc listener.http.tls off
    set_dc listener.https.bind "127.0.0.1:$https_port"
    set_dc listener.https.tls required
    set_dc listener.mongo.bind "127.0.0.1:$mongo_port"
    set_dc listener.mongo.tls optional
    set_dc listener.http.enabled true
    set_dc listener.https.enabled true
    set_dc listener.mongo.enabled true
    local p
    for p in "$http_port" "$https_port" "$mongo_port"; do
        for _ in $(seq 1 40); do
            listening "$p" && break
            sleep 0.5
        done
        listening "$p" || die "the gateway isn't listening on $p; see $state/gateway.log"
    done
    say "http 127.0.0.1:$http_port, https 127.0.0.1:$https_port, mongo 127.0.0.1:$mongo_port"

    ctl settings set sink.destination_allowlist '["127.0.0.1:9"]' --scope tenant:acme \
        --reason "deploy/compose/integration.sh" >/dev/null
    # 127.0.0.1 is an internal address: the operator's list covers it too.
    ctl settings set sink.internal_destinations '["127.0.0.1:9"]' --scope tenant:acme \
        --reason "deploy/compose/integration.sh" >/dev/null
    # The process-list test also makes connections: acme.app needs
    # SPRIG_SINK_ADMIN, which the tenant's administrator may grant.
    sql_as acme.app "GRANT SPRIG_SINK_ADMIN ON *.* TO app"

    local parent="" t
    for t in ig-a ig-b ig-c; do
        if ! ctl -o json tenant show "$t" >/dev/null 2>&1; then
            ctl tenant create "$t" --cluster dc-a-c1 ${parent:+--under "$parent"} >/dev/null
        fi
        ctl tenant user add "$t" app --password "$password" --native --if-not-exists >/dev/null
        ctl settings set auth.mysql_native_password true --scope "tenant:$t" \
            --reason "deploy/compose/integration.sh" >/dev/null
        parent=$t
    done
    say "tenants ig-a > ig-b > ig-c ready (user app)"

    local my="127.0.0.1:$mysql_port" ma="127.0.0.1:$mariadb_port"
    local http="127.0.0.1:$http_port"
    cat >"$state/test-env" <<EOF
SPRIG_TEST_M03_GATEWAY=$my,$ma
SPRIG_TEST_RLS=$my,$ma
SPRIG_TEST_RLS_TENANT=acme
SPRIG_TEST_HL_GATEWAY=$my,$ma
SPRIG_TEST_HL_TENANT=acme
SPRIG_TEST_HL_TENANTS=ig-a,ig-b,ig-c
SPRIG_TEST_R6_GATEWAY=$my,$ma
SPRIG_TEST_R6_USER=acme.app
SPRIG_TEST_R6_PASSWORD=$password
SPRIG_TEST_IG_GATEWAY=$my,$ma
SPRIG_TEST_IG_TENANTS=ig-a,ig-b,ig-c
SPRIG_TEST_IG_MODE=read_through
SPRIG_TEST_LOGIC_GATEWAY=$my,$http
SPRIG_TEST_LOGIC_USER=acme.app
SPRIG_TEST_LOGIC_PASSWORD=$password
SPRIG_TEST_HTTP_DISCONNECT=$my,$http
SPRIG_TEST_LANG_GATEWAY=$my,$http,127.0.0.1:$mongo_port
SPRIG_TEST_M7_GATEWAY=$my,$http,127.0.0.1:$https_port
SPRIG_TEST_M7_USER=acme.app
SPRIG_TEST_M7_PASSWORD=$password
SPRIG_TEST_PROCESSLIST_SECRETS=$my
SPRIG_TEST_OPS_TRACE=$ma,http://127.0.0.1:$gw_telemetry_port,acme.app,$password
EOF
    say "test variables in $state/test-env"
}

case ${1:-} in
    up) up ;;
    down) "$gwdev" down ;;
    *) die "usage: $0 up|down" ;;
esac
