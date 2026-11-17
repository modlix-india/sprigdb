#!/usr/bin/env bash
# Runs every feature probe for the products named (default: all in the conf).
#   features/run-all.sh targets.json [product ...]
set -uo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
conf=${1:?targets.json}; shift
products=("$@")
if [[ ${#products[@]} -eq 0 ]]; then
    while read -r p; do products+=("$p"); done < <(python3 -I -c 'import json,sys;print("\n".join(json.load(open(sys.argv[1]))))' "$conf")
fi
for p in "${products[@]}"; do
    for f in joins multi_doc_tx cross_model_tx rls change_feed vector_search text_search graph_traversal resharding; do
        python3 -I "$here/$f.py" --conf "$conf" --product "$p"
    done
    for w in mysql mongo redis bolt; do python3 -I "$here/wire.py" --conf "$conf" --product "$p" --protocol "$w"; done
done
