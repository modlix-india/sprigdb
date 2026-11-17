#!/usr/bin/env bash
# Fetches a BEIR dataset (scifact, nfcorpus) into its own directory once and
# runs fts/beir.py on it with the venv named by $BENCH_BEIR_VENV (or python3
# for OpenSearch, which needs only the standard library). Speed workload:
# benchmark VMs only.
#   fts/beir.sh <scifact|nfcorpus> <beir.py options...>
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ds=${1:?dataset}; shift
dir=${BENCH_DATA:-$here/../../../target/bench-data}/beir-$ds
if [[ ! -f $dir/$ds/corpus.jsonl ]]; then
    mkdir -p "$dir"
    curl -fL --retry 3 -o "$dir/$ds.zip" "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/$ds.zip"
    (cd "$dir" && unzip -q -o "$ds.zip")
fi
py=python3; [[ -n ${BENCH_BEIR_VENV:-} ]] && py=$BENCH_BEIR_VENV/bin/python
exec "$py" -I "$here/beir.py" --dataset "$dir/$ds" "$@"
