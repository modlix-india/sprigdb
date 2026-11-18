#!/usr/bin/env bash
# Runs lib/diff.py (the E0 differential) in a container on the harness network,
# so it reaches every arm without a port forward. Results and reports go to
# $BENCH_RESULTS and $BENCH_LOGS (mounted).
#   e0/diff.sh sql   --target e0bench-node:3306 --ref e0bench-mysql:3306 --flavor mysql --user U --password P [--ref-user U --ref-password P] --product "SprigDB E0"
#   e0/diff.sh redis --target e0bench-node:6379 --ref e0bench-redis:6379 [--user U --password P] --product "SprigDB E0"
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$BENCH_RESULTS" "$BENCH_LOGS"
R=$(cd "$BENCH_RESULTS" && pwd) L=$(cd "$BENCH_LOGS" && pwd)
exec $E0_DOCKER run --rm --name "e0bench-diff-$$" --network "$E0_NET" --cpuset-cpus "$E0_CLIENT_CPUS" \
    -v "$E0_BENCH:/opt/bench:ro" -v "$R:/results" -v "$L:/logs" \
    -e BENCH_RESULTS=/results -e BENCH_LOGS=/logs -e BENCH_HOST_LABEL="$BENCH_HOST_LABEL" -e BENCH_BUILD="${BENCH_BUILD:-not recorded}" \
    "$IMG_RUN" python3 -I /opt/bench/e0/lib/diff.py "$@" --logs /logs
