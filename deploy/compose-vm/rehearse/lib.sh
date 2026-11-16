# shellcheck shell=bash
# Shared parts of the rehearsals in this directory (sourced, not run).
#
# Where the VMs are: SPRIG_REHEARSE_VM1..3 (and _LB1, _LB2) are each either
# a directory on this machine (laptop-lab.sh env prints them) or
# ssh:<host>:<bundle dir> for a real VM, reached with ssh as the user that
# runs sprig-vm.sh there. The client side: SPRIG_REHEARSE_HOST and
# SPRIG_REHEARSE_MYSQL_PORT (the load balancer's MySQL port, as clients
# reach it), SPRIG_REHEARSE_CA (the deployment CA certificate).
#
# The writer (writer.py) runs in a container: SPRIG_REHEARSE_CLIENT_IMAGE
# (an image with python3 and PyMySQL), on SPRIG_REHEARSE_CLIENT_NET with
# the load balancer at SPRIG_REHEARSE_WRITER_HOST:SPRIG_REHEARSE_WRITER_PORT
# (laptop lab: the client network, lb:<the DC's MySQL port>; real VMs:
# --network host and the balancer's name).
set -euo pipefail
: "${SPRIG_REHEARSE_VM1:?set SPRIG_REHEARSE_VM1..3 (laptop-lab.sh env prints them)}"
: "${SPRIG_REHEARSE_CA:?SPRIG_REHEARSE_CA: the deployment CA certificate}"
: "${SPRIG_REHEARSE_HOST:?SPRIG_REHEARSE_HOST: the load balancer as clients reach it}"
: "${SPRIG_REHEARSE_MYSQL_PORT:?SPRIG_REHEARSE_MYSQL_PORT: the load balancer MySQL port}"
rh_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
rh_client_image=${SPRIG_REHEARSE_CLIENT_IMAGE:-sprigdb-conn-python:dev}
rh_client_net=${SPRIG_REHEARSE_CLIENT_NET:-host}
rh_writer_host=${SPRIG_REHEARSE_WRITER_HOST:-lb}
rh_writer_port=${SPRIG_REHEARSE_WRITER_PORT:-${SPRIG_REHEARSE_DC_MYSQL_PORT:-$SPRIG_REHEARSE_MYSQL_PORT}}

say() { echo "== $(date +%T) $*"; }
die() { echo "rehearse: $*" >&2; exit 1; }
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }

rh_target() { # <VM1|VM2|VM3|LB1|LB2>
    local v="SPRIG_REHEARSE_$1"
    [[ -n ${!v:-} ]] || die "$v isn't set"
    echo "${!v}"
}

# Runs <command...> in a VM's bundle directory (locally or over ssh).
in_vm() { # <VM1|...> <command...>
    local t
    t=$(rh_target "$1")
    shift
    case $t in
    ssh:*)
        local h=${t#ssh:}
        # shellcheck disable=SC2029 # expanded here on purpose
        ssh "${h%%:*}" "cd $(printf %q "${h#*:}") && $(printf '%q ' "$@")" ;;
    *) (cd "$t" && "$@") ;;
    esac
}

# sprig-vm.sh on VM <n>.
vm() { local n=$1; shift; in_vm "VM$n" ./sprig-vm.sh "$@"; }
# docker compose in VM <n>'s bundle (its .env names the files).
vm_dc() { local n=$1; shift; in_vm "VM$n" docker compose --project-directory . "$@"; }

# SQL as root on VM <n>'s MariaDB node (statements on stdin).
vm_db() { # <n> [mariadb client options]
    local n=$1
    shift
    in_vm "VM$n" docker compose --project-directory . exec -T mariadb-1 bash -c \
        'MYSQL_PWD=$(cat /run/sprig/mariadb/root-password) exec mariadb -uroot -h127.0.0.1 -P3306 --ssl-ca=/run/sprig/tls/ca.pem -N -B "$@"' _ "$@"
}

# SQL through the load balancer: sql <login> <password> <statements>.
sql() {
    local login=$1 pw=$2
    shift 2
    MYSQL_PWD=$pw mysql -h"$SPRIG_REHEARSE_HOST" -P"$SPRIG_REHEARSE_MYSQL_PORT" -u"$login" \
        --ssl-mode=VERIFY_CA --ssl-ca="$SPRIG_REHEARSE_CA" --connect-timeout=5 -N -B -e "$*"
}

# Every user table on VM <n>'s node with its checksum and row count, sorted:
# the tenants' data and SprigDB's own catalog schemas. Tables matching
# SPRIG_REHEARSE_VOLATILE (a regular expression on schema.table) are left
# out: ones SprigDB rewrites by itself while it runs.
db_checksums() { # <n>
    local n=$1 list
    list=$(echo "SELECT CONCAT(table_schema, '.', table_name) FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
        ORDER BY 1" | vm_db "$n")
    [[ -n ${SPRIG_REHEARSE_VOLATILE:-} ]] && list=$(grep -Ev "$SPRIG_REHEARSE_VOLATILE" <<<"$list" || true)
    [[ -n $list ]] || return 0
    {
        sed 's/^\(.*\)\.\(.*\)$/CHECKSUM TABLE `\1`.`\2` EXTENDED; SELECT COUNT(*) FROM `\1`.`\2`;/' <<<"$list"
    } | vm_db "$n" | paste - - | sort
}

# The writer: start in the background, stop, and the rows in its table.
writer_start() { # <out dir> <login> <password> <db.table> [interval] [reconnect every n writes]
    local out=$1 login=$2 pw=$3 table=$4 iv=${5:-0.05} every=${6:-0} name
    name=rehearse-writer-$(basename "$out")
    mkdir -p "$out"
    rm -f "$out/stop" "$out/writes.jsonl"
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" --network "$rh_client_net" -m 128m \
        -v "$rh_dir/writer.py:/w/writer.py:ro" -v "$SPRIG_REHEARSE_CA:/w/ca.pem:ro" -v "$out:/out" \
        --entrypoint python3 "$rh_client_image" /w/writer.py "$rh_writer_host" "$rh_writer_port" \
        "$login" "$pw" /w/ca.pem "$table" /out/writes.jsonl /out/stop "$iv" "$every" >/dev/null
    echo "$name" >"$out/writer.name"
}

writer_stop() { # <out dir>
    local out=$1 name
    name=$(cat "$out/writer.name")
    touch "$out/stop"
    docker wait "$name" >/dev/null 2>&1 || true
    docker logs "$name" >"$out/writer.log" 2>&1 || true
    docker rm -f "$name" >/dev/null 2>&1 || true
}

# The time WAIT sockets on this machine (keep under about 8,000).
time_wait() { netstat -an 2>/dev/null | grep -c TIME_WAIT || true; }

# A tenant and its user, created through VM 1's controller unless the login
# works already: ensure_tenant <tenant> <user> <password> [cluster id].
# Waits until the login works through every gateway (each VM's,
# SPRIG_REHEARSE_GW<n>, when set; else five logins in a row through the load
# balancer): a new account reaches the other gateways' caches a moment later.
ensure_tenant() {
    local t=$1 u=$2 pw=$3 cl=${4:-} t0 _
    sql "$t.$u" "$pw" "SELECT 1" >/dev/null 2>&1 && return 0
    t0=$(now)
    vm 1 ctl tenant create "$t" ${cl:+--cluster "$cl"} --yes >/dev/null
    vm 1 ctl tenant user add "$t" "$u" --password "$pw" --if-not-exists --yes >/dev/null
    for _ in $(seq 1 60); do
        if logins_ok "$t.$u" "$pw"; then
            echo "tenant $t: its login works through every gateway $(python3 -c "print(round($(now) - $t0, 1))") s after the create call" >&2
            return 0
        fi
        sleep 1
    done
    die "can't log in as $t.$u through every gateway"
}

logins_ok() { # <login> <password>
    local n v any=0
    for n in 1 2 3; do
        v="SPRIG_REHEARSE_GW$n"
        [[ -n ${!v:-} ]] || continue
        any=1
        sql_at "$n" "$1" "$2" "SELECT 1" >/dev/null 2>&1 || return 1
    done
    ((any)) && return 0
    for n in 1 2 3 4 5; do sql "$1" "$2" "SELECT 1" >/dev/null 2>&1 || return 1; done
}

# The VM whose MariaDB node is writable now (its agent cleared read_only:
# it holds the writer lease): 1, 2 or 3.
writer_vm() {
    local n
    for n in 1 2 3; do
        [[ $(echo "SELECT @@global.read_only" | vm_db "$n" 2>/dev/null) == 0 ]] && { echo "$n"; return 0; }
    done
    die "no writable MariaDB node found"
}

# SQL through one VM's gateway: sql_at <n> <login> <password> <statements>.
# SPRIG_REHEARSE_GW<n> is that gateway's MySQL address as host:port.
sql_at() {
    local v="SPRIG_REHEARSE_GW$1" login=$2 pw=$3
    shift 3
    [[ -n ${!v:-} ]] || die "$v isn't set (host:port of VM $1's gateway MySQL port)"
    MYSQL_PWD=$pw mysql -h"${!v%:*}" -P"${!v##*:}" -u"$login" \
        --ssl-mode=VERIFY_CA --ssl-ca="$SPRIG_REHEARSE_CA" --connect-timeout=5 -N -B -e "$*"
}
