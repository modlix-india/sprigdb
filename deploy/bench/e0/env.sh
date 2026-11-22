# Shared settings for the E0 harness (deploy/bench/e0). Source it; every value
# can be overridden from the environment, so the same scripts run on the laptop
# (inside Docker Desktop's Linux VM) and on the benchmark VMs.
# shellcheck shell=bash
E0_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
E0_ROOT=$(cd "$E0_DIR/../../.." && pwd)            # the repository
E0_BENCH=$(cd "$E0_DIR/.." && pwd)                 # deploy/bench
# The node's dev key, certificates and MariaDB binlog config (the sprig and e0 arms mount them).
E0_CONF=${E0_CONF:-$E0_ROOT/deploy/compose}
# Working state (binaries, data, raw logs): never inside the repository's tracked files.
E0_WORK=${E0_WORK:-$E0_ROOT/target/agent-e0-bench}
E0_RAW=${E0_RAW:-$E0_WORK/raw}
# Results in the market format (deploy/bench/lib/result.py).
export BENCH_RESULTS=${BENCH_RESULTS:-$E0_WORK/results}
export BENCH_LOGS=${BENCH_LOGS:-$E0_WORK/logs}

# The host label every result carries; the E0 page groups results by it.
export BENCH_HOST_LABEL=${BENCH_HOST_LABEL:-laptop Docker VM (SUT pinned to 4 CPUs, 1g)}

# Docker: one private network; no published ports, so no docker-proxy and no
# Docker Desktop port forward sits in any measured path. On a benchmark VM set
# E0_NET=host (and the *_HOST variables to the VM's private address).
E0_NET=${E0_NET:-e0bench}
E0_DOCKER=${E0_DOCKER:-docker}

# Images (exact tags; digests are recorded with every result).
IMG_BUILD=${IMG_BUILD:-e0bench-build:1.94}          # rust:1.94.0-slim-bookworm + clang for RocksDB
IMG_RUN=${IMG_RUN:-e0bench-run:13.7}                # debian:13.7-slim + sysbench + curl + jq
IMG_MYSQL=${IMG_MYSQL:-mysql:8.4}
IMG_MARIADB=${IMG_MARIADB:-mariadb:11.8}
IMG_REDIS=${IMG_REDIS:-redis:7.4.11}
IMG_MEMTIER=${IMG_MEMTIER:-redislabs/memtier_benchmark:2.1.4}
IMG_ETCD=${IMG_ETCD:-quay.io/coreos/etcd:v3.6.5}

# Same footing for every database arm: one memory limit, one CPU set for the
# system under test (all of its processes share it, as they share one VM on
# OCI), another for the load generator, and the same primary cache.
E0_MEM=${E0_MEM:-1g}                                # 768m killed mysqld and MariaDB under the write loads (E0-104)
E0_SUT_CPUS=${E0_SUT_CPUS:-8-11}
E0_CLIENT_CPUS=${E0_CLIENT_CPUS:-12-15}
E0_CACHE_MB=${E0_CACHE_MB:-384}                     # InnoDB buffer pool = the engine's cache budget
E0_REDIS_MAXMEM=${E0_REDIS_MAXMEM:-512mb}

# sysbench dataset: the laptop uses 10 x 100,000 rows; the benchmark VMs 10 x 1,000,000.
E0_TABLES=${E0_TABLES:-10}
E0_TABLE_SIZE=${E0_TABLE_SIZE:-100000}

# Credentials of the private containers (test-only, never reused elsewhere).
E0_ROOT_PW=${E0_ROOT_PW:-e0bench-root}
E0_APP_USER=${E0_APP_USER:-sbtest}
E0_APP_PW=${E0_APP_PW:-e0bench-app}

# Endpoints. On the laptop every arm is its own container on $E0_NET, reached by
# name on its own port. On a benchmark VM (E0_NET=host) set E0_SUT_HOST to the
# SUT VM's private address; the arms then listen on these ports there (one arm
# at a time), and E0_SUT_DOCKER (e.g. "ssh vm2 docker") reads the SUT's
# containers from the load-generator VM.
E0_SUT_HOST=${E0_SUT_HOST:-}
E0_SUT_DOCKER=${E0_SUT_DOCKER:-$E0_DOCKER}
E0_PORT_MYSQL=${E0_PORT_MYSQL:-3306}
E0_PORT_MARIADB=${E0_PORT_MARIADB:-3306}
E0_PORT_REDIS=${E0_PORT_REDIS:-6379}
E0_PORT_NODE_SQL=${E0_PORT_NODE_SQL:-3306}
E0_PORT_NODE_REDIS=${E0_PORT_NODE_REDIS:-6379}
E0_PORT_NODE_MARIADB=${E0_PORT_NODE_MARIADB:-3307}
# The node's own etcd and catalog MariaDB: container names on the laptop,
# loopback ports on a benchmark VM (E0_NET=host).
if [[ $E0_NET == host ]]; then
    E0_ETCD_ADDR=${E0_ETCD_ADDR:-127.0.0.1:33379} E0_CATALOG_ADDR=${E0_CATALOG_ADDR:-127.0.0.1:33301}
else
    E0_ETCD_ADDR=${E0_ETCD_ADDR:-e0bench-etcd:2379} E0_CATALOG_ADDR=${E0_CATALOG_ADDR:-e0bench-sprigmaria:3306}
fi
e0_host() { echo "${E0_SUT_HOST:-$1}"; }  # container name -> the address clients use
export E0_SUT_HOST E0_SUT_DOCKER E0_PORT_MYSQL E0_PORT_MARIADB E0_PORT_REDIS E0_PORT_NODE_SQL E0_PORT_NODE_REDIS E0_PORT_NODE_MARIADB E0_ETCD_ADDR E0_CATALOG_ADDR
e0_product() { # arm -> product name in the result files (the market harness's names)
    case $1 in
    mysql) echo MySQL ;; mariadb) echo MariaDB ;; redis) echo Redis ;;
    sprig-sql | sprig-redis | sprig) echo SprigDB ;; e0-sql | e0-redis | e0) echo "SprigDB E0" ;;
    esac
}
e0_competitor() { # arm -> the report column
    case $1 in
    mysql) echo "MySQL 8.4" ;; mariadb) echo "MariaDB 11.8 (direct)" ;; redis) echo "Redis 7.4 / 8.1" ;;
    *) e0_product "$1" ;;
    esac
}
e0_say() { echo "e0: $(date '+%H:%M:%S') $*" >&2; }
