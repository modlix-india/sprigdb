#!/usr/bin/env bash
# Runs lib/bench.py (the E0 speed runner) with env.sh's settings exported.
#   e0/bench.sh sql|kv|calib|idle --arm <arm> [options]   (see lib/bench.py --help)
# Speed runs are heavy: take a slot first, e.g.
#   target/heavy-lock.sh run e0-bench -- deploy/bench/e0/bench.sh sql --arm mysql
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
export E0_DOCKER E0_NET IMG_RUN IMG_REDIS IMG_MEMTIER E0_CLIENT_CPUS E0_SUT_CPUS E0_MEM E0_WORK E0_APP_USER E0_APP_PW E0_TABLES E0_TABLE_SIZE
export E0_HOST_NOTE=${E0_HOST_NOTE:-"Laptop run inside Docker Desktop's Linux VM on a private network (no port forward or docker-proxy); system under test pinned to CPUs $E0_SUT_CPUS with $E0_MEM, client to $E0_CLIENT_CPUS; other sessions' work shares the VM, so read ratios, not absolute numbers."}
exec python3 -I "$E0_DIR/lib/bench.py" "$@"
