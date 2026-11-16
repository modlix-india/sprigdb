#!/usr/bin/env bash
# A three-VM SprigDB data center and two load balancers on one Docker host
# (a laptop), for the rehearsals in this directory (RUNBOOK.md,
# "Rehearsals"). Each "VM" is a copy of this bundle run as its own Compose
# project on its own network; they reach each other only through ports
# published on the host (host.docker.internal), as VMs reach each other
# on a private network. Every VM runs compose.yaml, compose.ha.yaml and
# compose.db-ha.yaml: an etcd member, a MariaDB node (a primary and two
# semi-sync replicas), its agent, a controller, a worker and a gateway.
#
#   SPRIG_LAB_DIR=<dir> laptop-lab.sh up <sprigdb image>   build and start the lab
#   SPRIG_LAB_DIR=<dir> laptop-lab.sh env                  the variables the rehearsals read
#   SPRIG_LAB_DIR=<dir> laptop-lab.sh down                 remove its containers, volumes and network
#
# SPRIG_LAB_PREFIX (default sprig-lab) names the Compose projects
# (<prefix>-vm1..3, <prefix>-lb1..2) and the shared client network
# (<prefix>-clients). SPRIG_LAB_PORT_BASE (default 41000): VM n publishes on
# base + (n-1) * 1000 + the usual last three digits (41306 MySQL, 41379
# etcd, 41316 MariaDB, ...); the load balancers on base + 3000 and + 4000.
# The client ports are DC-wide settings, so every gateway listens on the
# first VM's numbers inside its container; VMs 2 and 3 publish them shifted.
#
# On the client network <prefix>-clients, the gateways are kafka-vm1..3
# (each VM's Kafka name, all on the same port, as on real VMs) and both
# load balancers answer to "lb" (and lb1, lb2).
#
# Needs: docker compose v2, openssl, jq, rsync; about 9 GB of Docker memory.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
bundle=$(cd "$here/.." && pwd)
lab=${SPRIG_LAB_DIR:?SPRIG_LAB_DIR: the directory the lab lives in}
prefix=${SPRIG_LAB_PREFIX:-sprig-lab}
base=${SPRIG_LAB_PORT_BASE:-41000}
net=$prefix-clients
say() { echo "laptop-lab: $*"; }
die() { echo "laptop-lab: $*" >&2; exit 1; }

# The DC-wide client ports: <name> <port>.
client_ports() {
    cat <<EOT
SPRIG_MYSQL_PORT $((base + 306))
SPRIG_MARIADB_PORT $((base + 307))
SPRIG_HTTPS_PORT $((base + 443))
SPRIG_MONGO_PORT $((base + 17))
SPRIG_REDIS_PORT $((base + 637))
SPRIG_BOLT_PORT $((base + 687))
SPRIG_KAFKA_PORT $((base + 92))
SPRIG_AMQPS_PORT $((base + 671))
EOT
}

hdi=host.docker.internal

vm_env() { # <n>
    local n=$1 o=$(( ($1 - 1) * 1000 )) k v
    cat <<EOT
COMPOSE_PROJECT_NAME=$prefix-vm$n
COMPOSE_FILE=compose.yaml:compose.ha.yaml:compose.db-ha.yaml:laptop.yaml
SPRIGDB_IMAGE=$image
SPRIG_DC=dc1
SPRIG_DEPLOYMENT_ID=$prefix
SPRIG_CLUSTER_NAME=dc1-main
SPRIG_PUBLIC_HOST=127.0.0.1
SPRIG_TLS_SANS=DNS:lb,DNS:lb1,DNS:lb2,DNS:kafka-vm1,DNS:kafka-vm2,DNS:kafka-vm3,DNS:$hdi
SPRIG_BIND_IP=127.0.0.1
EOT
    client_ports | while read -r k v; do echo "$k=$v"; done
    cat <<EOT
SPRIG_ADMIN_PORT=$((base + o + 480))
SPRIG_METRICS_PORT=$((base + o + 90))
MARIADB_MEM_LIMIT=768m
MARIADB_BUFFER_POOL=256M
MARIADB_REDO_SIZE=64M
MARIADB_MAX_CONNECTIONS=400
GATEWAY_MEM_LIMIT=768m
CONTROLLER_MEM_LIMIT=512m
WORKER_MEM_LIMIT=384m
ETCD_MEM_LIMIT=256m
AGENT_MEM_LIMIT=128m
SPRIG_CLUSTER_MEMORY_GB=1
SPRIG_CLUSTER_STORAGE_GB=20
SPRIG_LOG_MAX_SIZE=20m
SPRIG_LOG_MAX_FILES=3
SPRIG_VM_ID=$n
SPRIG_CONTROLLER_BOOTSTRAP=$([[ $n == 1 ]] && echo true || echo false)
SPRIG_PRIVATE_HOST=$hdi
SPRIG_PRIVATE_IP=127.0.0.1
SPRIG_INTERNAL_SANS=DNS:$hdi
SPRIG_ADMIN_BIND_IP=127.0.0.1
SPRIG_ETCD_NAME=etcd-vm$n
SPRIG_ETCD_INITIAL_CLUSTER=etcd-vm1=https://$hdi:$((base + 380)),etcd-vm2=https://$hdi:$((base + 1380)),etcd-vm3=https://$hdi:$((base + 2380))
SPRIG_ETCD_SEEDS='["https://$hdi:$((base + 379))","https://$hdi:$((base + 1379))","https://$hdi:$((base + 2379))"]'
SPRIG_ETCD_PEER_SKIP_SAN=true
SPRIG_ETCD_CLIENT_PORT=$((base + o + 379))
SPRIG_ETCD_PEER_PORT=$((base + o + 380))
SPRIG_INTERNAL_PORT=$((base + o + 481))
SPRIG_PEER_PORT=$((base + o + 403))
SPRIG_BACKEND_PORT=$((base + o + 316))
SPRIG_BACKEND_PORT_2=$((base + o + 326))
SPRIG_TELEMETRY_PORT_GATEWAY=$((base + o + 180))
SPRIG_TELEMETRY_PORT_CONTROLLER=$((base + o + 181))
SPRIG_TELEMETRY_PORT_WORKER=$((base + o + 182))
SPRIG_TELEMETRY_PORT_AGENT=$((base + o + 183))
SPRIG_KAFKA_HOST=kafka-vm$n
SPRIG_DB_NODES=db1=$hdi:$((base + 316)),db2=$hdi:$((base + 1316)),db3=$hdi:$((base + 2316))
SPRIG_DB_PRIMARY=db1
SPRIG_BACKUP_COPY_TO=$lab/offvm-backups
SPRIG_BACKUP_KEEP=3
EOT
}

# laptop.yaml: the gateway joins the client network as kafka-vm<n>; VMs 2
# and 3 publish the DC's client ports shifted (one host, one port number).
vm_override() { # <n>
    local n=$1 o=$(( ($1 - 1) * 1000 )) k v
    echo "# laptop-lab.sh: laptop only."
    echo "services:"
    echo "  gateway:"
    echo "    networks:"
    echo "      sprig: {}"
    echo "      clients: { aliases: [kafka-vm$n] }"
    if [[ $n != 1 ]]; then
        echo "    ports: !override"
        client_ports | while read -r k v; do echo "      - \"127.0.0.1:$((v + o)):$v\""; done
        echo "      - \"127.0.0.1:$((base + o + 403)):$((base + o + 403))\""
        echo "      - \"127.0.0.1:$((base + o + 180)):9180\""
    fi
    echo "networks:"
    echo "  clients: { external: true, name: $net }"
}

lb_env() { # <n> (1 or 2)
    local n=$1 k v
    echo "COMPOSE_PROJECT_NAME=$prefix-lb$n"
    client_ports | while read -r k v; do echo "$k=$v"; done
    echo "SPRIG_LB_GATEWAYS=$hdi+0,$hdi+1000,$hdi+2000"
    echo "SPRIG_LB_HEALTH_PORT=$((base + 180))"
    echo "SPRIG_LB_ADMIN_PORT=$((base + 2000 + n * 1000 + 901))"
    echo "SPRIG_LB_COMPOSE_EXTRA=laptop-lb.yaml"
    echo "LB_MEM_LIMIT=128m"
}

lb_override() { # <n>
    local n=$1 k v
    local o=$(( (n + 2) * 1000 ))
    echo "# laptop-lab.sh: laptop only."
    echo "services:"
    echo "  lb:"
    echo "    networks:"
    echo "      default: {}"
    echo "      clients: { aliases: [lb, lb$n] }"
    echo "    ports: !override"
    client_ports | while read -r k v; do echo "      - \"127.0.0.1:$((v + o)):$v\""; done
    echo "      - \"127.0.0.1:$((base + o + 901)):9901\""
    echo "networks:"
    echo "  clients: { external: true, name: $net }"
}

copy_bundle() { # <dir>
    mkdir -p "$1"
    rsync -a --exclude .env --exclude secrets/ --exclude backups/ --exclude .state/ --exclude out/ --exclude lb/ \
        --exclude rehearse/ --exclude config/prometheus-targets.json "$bundle/" "$1/"
}

vm() { local n=$1; shift; (cd "$lab/vm$n" && ./sprig-vm.sh "$@"); }

up() {
    image=${1:?usage: laptop-lab.sh up <sprigdb image>}
    docker image inspect "$image" >/dev/null || die "no image $image (build-image.sh)"
    mkdir -p "$lab/offvm-backups" "$lab/offline"
    docker network inspect "$net" >/dev/null 2>&1 || docker network create "$net" >/dev/null
    local n
    for n in 1 2 3; do
        copy_bundle "$lab/vm$n"
        vm_env "$n" >"$lab/vm$n/.env"
        vm_override "$n" >"$lab/vm$n/laptop.yaml"
    done
    if [[ ! -d $lab/vm1/secrets ]]; then
        vm 1 init
        # What a real install keeps off the VMs.
        mv "$lab/vm1/secrets/ca" "$lab/offline/ca"
        mv "$lab/vm1/secrets/backup/backup-key.pem" "$lab/offline/backup-key.pem"
    fi
    for n in 2 3; do
        [[ -d $lab/vm$n/secrets ]] && continue
        rsync -a "$lab/vm1/secrets/" "$lab/vm$n/secrets/"
        chmod 700 "$lab/vm$n/secrets"
    done
    for n in 1 2 3; do vm "$n" up-etcd; done
    vm 1 up
    vm 2 up
    vm 3 up
    vm 1 configure
    vm 2 configure
    vm 3 configure
    for n in 1 2; do
        copy_bundle "$lab/lb$n"
        lb_env "$n" >"$lab/lb$n/.env"
        lb_override "$n" >"$lab/lb$n/laptop-lb.yaml"
        (cd "$lab/lb$n" && ./sprig-vm.sh lb-up)
    done
    vm 1 status
    env_out
}

env_out() {
    cat <<EOT
export SPRIG_REHEARSE_VM1=$lab/vm1 SPRIG_REHEARSE_VM2=$lab/vm2 SPRIG_REHEARSE_VM3=$lab/vm3
export SPRIG_REHEARSE_LB1=$lab/lb1 SPRIG_REHEARSE_LB2=$lab/lb2
export SPRIG_REHEARSE_CA=$lab/vm1/secrets/tls/ca.pem
export SPRIG_REHEARSE_HOST=127.0.0.1 SPRIG_REHEARSE_MYSQL_PORT=$((base + 3306))
export SPRIG_REHEARSE_CLIENT_NET=$net SPRIG_REHEARSE_DC_MYSQL_PORT=$((base + 306)) SPRIG_REHEARSE_KAFKA_PORT=$((base + 92))
export SPRIG_REHEARSE_OFFLINE=$lab/offline SPRIG_REHEARSE_OFFVM=$lab/offvm-backups
EOT
}

down() {
    local d
    for d in lb1 lb2; do
        [[ -f $lab/$d/.env ]] && (cd "$lab/$d" && docker compose --project-directory . -f compose.lb.yaml -f laptop-lb.yaml down -v) || true
    done
    # Each VM's .env names its Compose files (COMPOSE_FILE), including any
    # a rehearsal added.
    for d in vm1 vm2 vm3; do
        [[ -f $lab/$d/.env ]] && (cd "$lab/$d" && docker compose --project-directory . down -v --remove-orphans) || true
    done
    docker network rm "$net" >/dev/null 2>&1 || true
    say "lab containers, volumes and network removed (files stay in $lab)"
}

case ${1:-} in
up) shift; up "$@" ;;
env) env_out ;;
down) down ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
