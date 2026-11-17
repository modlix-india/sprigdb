#!/usr/bin/env bash
# Memory and storage after a workload has loaded the same dataset into each
# product: resident memory of the product's container(s) and bytes on disk of
# its data directory. Records <model> / mem / footprint: resident_mb and
# disk_mb (lower is better), variant dataset=<name>.
#
#   all/footprint.sh --target 10.0.0.3:27017 --product MongoDB --version 8.0.34 \
#       -- --model doc --dataset ycsb-1m --container mongo:/data/db [--container other:/path ...]
#
# Each --container is NAME:DATA_DIR on the machine running the product (use
# `ssh host` through BENCH_DOCKER, e.g. BENCH_DOCKER="ssh vm2 docker"). For a
# process outside Docker, use --pid PID:DATA_DIR on that machine instead.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
MODEL="" DATASET="" CONTAINERS=() PIDS=()
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --model) MODEL=$2; shift 2 ;; --dataset) DATASET=$2; shift 2 ;;
    --container) CONTAINERS+=("$2"); shift 2 ;; --pid) PIDS+=("$2"); shift 2 ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
[[ -n $MODEL && -n $DATASET ]] || { echo "--model and --dataset are required" >&2; exit 2; }
D=${BENCH_DOCKER:-docker}
rss=0 disk=0
for c in "${CONTAINERS[@]+"${CONTAINERS[@]}"}"; do
    name=${c%%:*} dir=${c#*:}
    m=$($D stats --no-stream --format '{{.MemUsage}}' "$name" | awk '{v=$1; u=v; sub(/[A-Za-z]+$/,"",u); if (v~/GiB/) u*=1024; else if (v~/KiB/) u/=1024; print u}')
    k=$($D exec "$name" du -sk "$dir" | awk '{print $1}')
    rss=$(python3 -I -c "print($rss + $m)"); disk=$((disk + k))
    echo "$name: ${m} MiB resident, ${k} KiB in $dir" | tee -a "$LOGDIR/footprint.txt"
done
for p in "${PIDS[@]+"${PIDS[@]}"}"; do
    pid=${p%%:*} dir=${p#*:}
    m=$(ps -o rss= -p "$pid" | awk '{print $1/1024}'); k=$(du -sk "$dir" | awk '{print $1}')
    rss=$(python3 -I -c "print($rss + $m)"); disk=$((disk + k))
    echo "pid $pid: ${m} MiB resident, ${k} KiB in $dir" | tee -a "$LOGDIR/footprint.txt"
done
VARIANT="dataset=$DATASET"
emit "$MODEL" mem footprint resident_mb "$rss" MiB lower
emit "$MODEL" mem footprint disk_mb "$(python3 -I -c "print(round($disk / 1024, 1))")" MiB lower
