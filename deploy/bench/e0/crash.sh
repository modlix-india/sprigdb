#!/usr/bin/env bash
# Runs lib/crash.py (kill -9 loops with acknowledged-write checks) with env.sh's settings.
#   e0/crash.sh --arm e0 --proto redis [--loops 100]     (the arm must be up: e0/stack.sh up <arm>)
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/env.sh"
export E0_DOCKER E0_NET IMG_RUN E0_CLIENT_CPUS E0_APP_USER E0_APP_PW BENCH_RESULTS BENCH_LOGS
exec python3 -I "$E0_DIR/lib/crash.py" "$@"
