#!/usr/bin/env bash
# The systems under test for the E0 harness, each in its own containers on one
# private Docker network (no published ports, so no docker-proxy or port
# forward in any measured path). Every database arm gets the same memory limit
# ($E0_MEM), the same CPU set ($E0_SUT_CPUS) and the same primary cache
# ($E0_CACHE_MB), and is durable per acknowledged write.
#
#   e0/stack.sh net                 create the network (idempotent)
#   e0/stack.sh up   <arm>          start an arm and wait until it answers
#   e0/stack.sh down <arm>          remove the arm's containers (exact names)
#   e0/stack.sh kill <arm>          kill -9 the arm's server process(es) (crash runs)
#   e0/stack.sh start <arm>         start the arm's stopped containers again
#   e0/stack.sh status
#   e0/stack.sh images              build the local images (e0bench-run, e0bench-build)
#
# Arms:
#   mysql    MySQL 8.4: innodb_flush_log_at_trx_commit=1, sync_binlog=1, binlog on, no TLS
#   mariadb  MariaDB 11.8 direct: the same settings
#   redis    Redis 7.4.11: appendonly yes, appendfsync always, no RDB snapshots, io-threads 4
#   sprig    today's SprigDB: controller, worker and gateway (one container, $E0_WORK/bin/before)
#            over its own etcd and MariaDB 11.8 (durable as above); MySQL and Redis listeners
#   e0       the E0 node ($E0_WORK/bin/e0/sprigdb): MySQL and Redis listeners, data in /data
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
D=$E0_DOCKER
SUT=(--network "$E0_NET" --cpuset-cpus "$E0_SUT_CPUS" -m "$E0_MEM" --memory-swap "$E0_MEM" --ulimit nofile=65536:65536)

exists() { $D container inspect "$1" >/dev/null 2>&1; }
rmc() { local c; for c in "$@"; do exists "$c" && $D rm -f "$c" >/dev/null && e0_say "removed $c"; done; return 0; }
run_tool() { $D run --rm --network "$E0_NET" --cpuset-cpus "$E0_CLIENT_CPUS" -v "$E0_DIR:/opt/e0:ro" "$IMG_RUN" "$@"; }

wait_sql() { # container host port user password
    local i
    for i in $(seq 1 120); do
        run_tool python3 -I -c "import socket,sys; s=socket.create_connection(('$2',$3),2); d=s.recv(5); sys.exit(0 if len(d)==5 and d[4]!=0xff else 1)" 2>/dev/null && return 0
        sleep 1
    done
    $D logs --tail 30 "$1" >&2 || true
    e0_say "$1 did not answer on $2:$3"; return 1
}
wait_redis() { # container host port
    local i
    for i in $(seq 1 120); do
        run_tool python3 -I -c "import socket,sys; s=socket.create_connection(('$2',$3),2); s.sendall(b'PING\r\n'); d=s.recv(64); sys.exit(0 if d.startswith((b'+PONG',b'-NOAUTH')) else 1)" 2>/dev/null && return 0
        sleep 1
    done
    $D logs --tail 30 "$1" >&2 || true
    e0_say "$1 did not answer on $2:$3"; return 1
}

net() { $D network inspect "$E0_NET" >/dev/null 2>&1 || { [[ $E0_NET == host ]] || $D network create "$E0_NET" >/dev/null; e0_say "network $E0_NET"; }; }

images() {
    $D image inspect "$IMG_RUN" >/dev/null 2>&1 || $D build -q -t "$IMG_RUN" -f "$E0_DIR/Dockerfile.run" "$E0_DIR" >/dev/null
    $D image inspect "$IMG_BUILD" >/dev/null 2>&1 || $D build -q -t "$IMG_BUILD" -f "$E0_DIR/Dockerfile.build" "$E0_DIR" >/dev/null
    e0_say "images $IMG_RUN $IMG_BUILD ready"
}

innodb_args() { # the durable InnoDB settings both MySQL and MariaDB get
    echo --innodb-buffer-pool-size="${E0_CACHE_MB}M" --innodb-flush-log-at-trx-commit=1 --sync-binlog=1 \
        --max-connections=2000 --skip-name-resolve
}

up_mysql() {
    rmc e0bench-mysql
    # shellcheck disable=SC2046
    $D run -d --name e0bench-mysql --hostname e0bench-mysql "${SUT[@]}" -e MYSQL_ROOT_PASSWORD="$E0_ROOT_PW" \
        "$IMG_MYSQL" $(innodb_args) --innodb-redo-log-capacity=1G --mysql-native-password=ON --tls-version= >/dev/null
    wait_sql e0bench-mysql e0bench-mysql 3306
    for _ in $(seq 1 60); do $D exec e0bench-mysql mysql -uroot -p"$E0_ROOT_PW" -e 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
    $D exec e0bench-mysql mysql -uroot -p"$E0_ROOT_PW" -e "
        CREATE USER IF NOT EXISTS '$E0_APP_USER'@'%' IDENTIFIED WITH mysql_native_password BY '$E0_APP_PW';
        GRANT ALL ON *.* TO '$E0_APP_USER'@'%'; CREATE DATABASE IF NOT EXISTS sbtest; CREATE DATABASE IF NOT EXISTS e0diff; CREATE DATABASE IF NOT EXISTS e0crash;" 2>/dev/null
}

up_mariadb() {
    rmc e0bench-mariadb
    # shellcheck disable=SC2046
    $D run -d --name e0bench-mariadb --hostname e0bench-mariadb "${SUT[@]}" -e MARIADB_ROOT_PASSWORD="$E0_ROOT_PW" \
        "$IMG_MARIADB" $(innodb_args) --innodb-log-file-size=1G --log-bin=bin --binlog-format=ROW --server-id=1 --skip-ssl >/dev/null
    wait_sql e0bench-mariadb e0bench-mariadb 3306
    for _ in $(seq 1 60); do $D exec e0bench-mariadb mariadb --skip-ssl -uroot -p"$E0_ROOT_PW" -e 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
    $D exec e0bench-mariadb mariadb --skip-ssl -uroot -p"$E0_ROOT_PW" -e "
        CREATE USER IF NOT EXISTS '$E0_APP_USER'@'%' IDENTIFIED BY '$E0_APP_PW';
        GRANT ALL ON *.* TO '$E0_APP_USER'@'%'; CREATE DATABASE IF NOT EXISTS sbtest; CREATE DATABASE IF NOT EXISTS e0diff; CREATE DATABASE IF NOT EXISTS e0crash;"
}

up_redis() {
    rmc e0bench-redis
    $D run -d --name e0bench-redis --hostname e0bench-redis "${SUT[@]}" "$IMG_REDIS" redis-server --port 6379 \
        --protected-mode no --maxmemory "$E0_REDIS_MAXMEM" --io-threads 4 --appendonly yes --appendfsync always --save '' >/dev/null
    wait_redis e0bench-redis e0bench-redis 6379
}

# Today's SprigDB: etcd and MariaDB in their own containers, and the controller,
# worker and gateway processes in one container sharing its loopback (as on one
# VM), started by conf/sprig-up.sh with the binaries in $E0_WORK/bin/before.
up_sprig() {
    local bin=${SPRIG_BIN:-$E0_WORK/bin/before}
    [[ -x $bin/sprigdb && -x $bin/sprigctl ]] || { e0_say "no Linux sprigdb/sprigctl in $bin (run e0/build-linux.sh before)"; exit 2; }
    rmc e0bench-sprig e0bench-sprigmaria e0bench-etcd
    $D run -d --name e0bench-etcd --hostname e0bench-etcd "${SUT[@]}" -m 256m --memory-swap 256m "$IMG_ETCD" etcd --name=e0 \
        --data-dir=/etcd-data --listen-client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://e0bench-etcd:2379 \
        --listen-peer-urls=http://127.0.0.1:2380 --initial-advertise-peer-urls=http://127.0.0.1:2380 \
        --initial-cluster=e0=http://127.0.0.1:2380 --auto-compaction-mode=periodic --auto-compaction-retention=1h >/dev/null
    # shellcheck disable=SC2046
    $D run -d --name e0bench-sprigmaria --hostname e0bench-sprigmaria "${SUT[@]}" -e MARIADB_ROOT_PASSWORD="$E0_ROOT_PW" \
        -v "$E0_ROOT/deploy/compose/mariadb/binlog.cnf:/etc/mysql/conf.d/10-binlog.cnf:ro" \
        "$IMG_MARIADB" $(innodb_args) --innodb-log-file-size=1G --server-id=1 --gtid-domain-id=1 >/dev/null
    wait_sql e0bench-sprigmaria e0bench-sprigmaria 3306
    for _ in $(seq 1 60); do $D exec e0bench-sprigmaria mariadb --skip-ssl -uroot -p"$E0_ROOT_PW" -e 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
    $D run -d --name e0bench-sprig --hostname e0bench-sprig "${SUT[@]}" --init \
        -e E0_ROOT_PW="$E0_ROOT_PW" -e E0_APP_PW="$E0_APP_PW" -e E0_TENANT_BASE="${E0_TENANT_BASE:-57306000000}" \
        -v "$bin:/opt/sprig:ro" -v "$E0_DIR:/opt/e0:ro" \
        -v "$E0_ROOT/deploy/compose/workload-keys.dev:/opt/e0conf-x/workload-keys.dev:ro" \
        -v "$E0_ROOT/deploy/compose/gateway-certs:/opt/e0conf-x/certs:ro" \
        "$IMG_RUN" bash /opt/e0/conf/sprig-up.sh >/dev/null
    for _ in $(seq 1 240); do
        $D exec e0bench-sprig test -f /var/lib/sprig/READY 2>/dev/null && break
        exists e0bench-sprig && [[ $($D inspect -f '{{.State.Running}}' e0bench-sprig) == true ]] || { $D logs --tail 40 e0bench-sprig >&2; exit 1; }
        sleep 1
    done
    $D exec e0bench-sprig test -f /var/lib/sprig/READY || { $D logs --tail 40 e0bench-sprig >&2; e0_say "sprig not ready"; exit 1; }
    e0_say "sprig up: mysql e0bench-sprig:3306 (sbtest), redis e0bench-sprig:6379, user bench.app"
}

# The E0 node: one process with the MySQL and Redis listeners and the engine.
# How it starts is in conf/e0-node.sh (kept beside the harness so the command
# line can follow the binary without editing this script).
up_e0() {
    local bin=${E0_BIN:-$E0_WORK/bin/e0}
    [[ -x $bin/sprigdb ]] || { e0_say "no E0 binary in $bin"; exit 2; }
    rmc e0bench-node
    $D run -d --name e0bench-node --hostname e0bench-node "${SUT[@]}" --init \
        -e E0_APP_PW="$E0_APP_PW" -e E0_CACHE_MB="$E0_CACHE_MB" \
        -v "$bin:/opt/sprig:ro" -v "$E0_DIR:/opt/e0:ro" "$IMG_RUN" bash /opt/e0/conf/e0-node.sh >/dev/null
    wait_sql e0bench-node e0bench-node 3306
    wait_redis e0bench-node e0bench-node 6379
}

containers_of() {
    case $1 in
    mysql) echo e0bench-mysql ;; mariadb) echo e0bench-mariadb ;; redis) echo e0bench-redis ;;
    sprig) echo e0bench-sprig e0bench-sprigmaria e0bench-etcd ;; e0) echo e0bench-node ;;
    *) echo "unknown arm $1" >&2; exit 2 ;;
    esac
}

# kill -9 of the server processes. For one-process containers that is PID 1's
# child under --init, or PID 1 itself: `docker kill -s KILL` ends the container,
# and `start` brings it back on the same data, which is what a crash test wants.
killarm() {
    local c
    for c in $(containers_of "$1"); do
        [[ $1 == sprig && $c == e0bench-etcd ]] && continue
        $D kill -s KILL "$c" >/dev/null 2>&1 && e0_say "killed $c" || true
    done
}
startarm() {
    local c
    for c in $(containers_of "$1"); do $D start "$c" >/dev/null; done
    case $1 in
    mysql | mariadb) wait_sql "e0bench-$1" "e0bench-$1" 3306 ;;
    redis) wait_redis e0bench-redis e0bench-redis 6379 ;;
    e0) wait_sql e0bench-node e0bench-node 3306; wait_redis e0bench-node e0bench-node 6379 ;;
    sprig) e0_say "sprig restart after a kill is not supported by sprig-up.sh; use up" ;;
    esac
}

cmd=${1:-status}; arm=${2:-}
case $cmd in
net) net ;;
images) images ;;
up) net; images; "up_$arm" ;;
down) # shellcheck disable=SC2046
    rmc $(containers_of "$arm") ;;
kill) killarm "$arm" ;;
start) startarm "$arm" ;;
status) $D ps -a --filter name=e0bench- --format '{{.Names}}\t{{.Status}}\t{{.Image}}' ;;
*) sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2 ;;
esac
