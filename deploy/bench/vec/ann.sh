#!/usr/bin/env bash
# Fetches an ann-benchmarks dataset into its own directory (once) and runs
# vec/ann.py on it with the venv named by $BENCH_ANN_VENV. Speed workload:
# benchmark VMs only.
#   vec/ann.sh <dataset: sift-128-euclidean|glove-100-angular> <ann.py options...>
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ds=${1:?dataset}; shift
dir=${BENCH_DATA:-$here/../../../target/bench-data}/ann-$ds
mkdir -p "$dir"
[[ -f $dir/$ds.hdf5 ]] || curl -fL --retry 3 -o "$dir/$ds.hdf5" "https://ann-benchmarks.com/$ds.hdf5"
exec "${BENCH_ANN_VENV:?BENCH_ANN_VENV must name a venv with h5py and numpy}/bin/python" -I "$here/ann.py" --dataset "$dir/$ds.hdf5" "$@"
