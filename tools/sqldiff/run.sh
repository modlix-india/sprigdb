#!/usr/bin/env bash
# The SQL differential: runs each corpus in corpus/ through SprigDB and an
# oracle server for the same persona (MariaDB 11.8 on the MariaDB port, MySQL
# 8.4 on the MySQL port) and compares every response packet (diff.py).
#
#   tools/sqldiff/run.sh [--oracles] [--record] [name...]
#
# name: a corpus without its suffix (dates, views, ...); default every corpus.
# <name>.diff runs on both ports, <name>.maria.diff / <name>.mysql.diff on one.
# --oracles   start throwaway oracle containers first (sprig-sqldiff-maria on
#             127.0.0.1:45611, sprig-sqldiff-mysql on 127.0.0.1:45621) and remove
#             them at the end; otherwise point SQLDIFF_ORACLE_* at your own.
# --record    write this run's counts to floors.tsv (its corpora's rows) instead
#             of checking them.
#
# Endpoints, each host:port:user:password (defaults: tools/live/stack.sh's first
# gateway with tenant sqldiff, and the --oracles containers):
#   SQLDIFF_SPRIG_MARIA  SprigDB's MariaDB port   (127.0.0.1:45107:sqldiff.app:sprig-live-app)
#   SQLDIFF_SPRIG_MYSQL  SprigDB's MySQL port     (127.0.0.1:45106:sqldiff.app:sprig-live-app)
#   SQLDIFF_ORACLE_MARIA MariaDB 11.8             (127.0.0.1:45611:root:sqldiff-root)
#   SQLDIFF_ORACLE_MYSQL MySQL 8.4                (127.0.0.1:45621:root:sqldiff-root)
# SQLDIFF_DB / SQLDIFF_DB2 (default sqldiff_a, sqldiff_b): the databases the
# corpora use, created on every side. SQLDIFF_OUT: where each run's output goes
# (default target/sqldiff).
#
# A corpus passes when it matches at least as many statements byte for byte
# as floors.tsv records for it; the run fails on any corpus below its floor.
# Corpora without a floor are reported, not judged. Exit status 0 when nothing
# fell below its floor.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
OUT=${SQLDIFF_OUT:-$ROOT/target/sqldiff}
SM=${SQLDIFF_SPRIG_MARIA:-127.0.0.1:45107:sqldiff.app:sprig-live-app}
SY=${SQLDIFF_SPRIG_MYSQL:-127.0.0.1:45106:sqldiff.app:sprig-live-app}
OM=${SQLDIFF_ORACLE_MARIA:-127.0.0.1:45611:root:sqldiff-root}
OY=${SQLDIFF_ORACLE_MYSQL:-127.0.0.1:45621:root:sqldiff-root}
DB=${SQLDIFF_DB:-sqldiff_a}
DB2=${SQLDIFF_DB2:-sqldiff_b}
ORACLES=0
RECORD=0
names=()
for a in "$@"; do
    case $a in
    --oracles) ORACLES=1 ;;
    --record) RECORD=1 ;;
    *) names+=("$a") ;;
    esac
done
mkdir -p "$OUT"

oracles_up() {
    docker rm -f sprig-sqldiff-maria sprig-sqldiff-mysql >/dev/null 2>&1 || true
    docker run -d --name sprig-sqldiff-maria --memory 768m -p 127.0.0.1:45611:3306 \
        -e MARIADB_ROOT_PASSWORD=sqldiff-root mariadb:11.8 >/dev/null
    docker run -d --name sprig-sqldiff-mysql --memory 768m -p 127.0.0.1:45621:3306 \
        -e MYSQL_ROOT_PASSWORD=sqldiff-root mysql:8.4 --local-infile=ON >/dev/null
    for c in sprig-sqldiff-maria sprig-sqldiff-mysql; do
        for _ in $(seq 1 120); do
            docker exec "$c" sh -c 'mariadb-admin ping -uroot -psqldiff-root 2>/dev/null || mysqladmin ping -uroot -psqldiff-root 2>/dev/null' |
                grep -q alive && break
            sleep 1
        done
    done
    sleep 5 # MySQL restarts once after initialising
}
[[ $ORACLES == 1 ]] && {
    oracles_up
    trap 'docker rm -f sprig-sqldiff-maria sprig-sqldiff-mysql >/dev/null 2>&1 || true' EXIT
}

probe() { # probe <host:port:user:pw> <tls 0|1> <sql>...: runs statements without a database
    local IFS=:
    read -r h p u w <<<"$1"
    shift
    local tls=()
    [[ $1 == 1 ]] && tls=(--tls)
    shift
    local items=()
    for q in "$@"; do items+=("q:$q"); done
    python3 -I "$HERE/../checkers/probe.py" "$h" "$p" "$u" "$w" "" "${tls[@]+"${tls[@]}"}" --caps -CONNECT_WITH_DB "${items[@]}" >/dev/null
}
probe "$OM" 0 "CREATE DATABASE IF NOT EXISTS $DB" "CREATE DATABASE IF NOT EXISTS $DB2"
probe "$OY" 1 "CREATE DATABASE IF NOT EXISTS $DB" "CREATE DATABASE IF NOT EXISTS $DB2"
probe "$SM" 1 "CREATE DATABASE IF NOT EXISTS $DB" "CREATE DATABASE IF NOT EXISTS $DB2"

if [[ ${#names[@]} -eq 0 ]]; then
    while IFS= read -r n; do names+=("$n"); done < <(ls "$HERE/corpus" | sed -E 's/\.(maria|mysql)?\.?diff$//' | sort -u)
fi

floor() { awk -v n="$1" -v s="$2" '$1 == n && $2 == s { print $3 }' "$HERE/floors.tsv" 2>/dev/null; }
results=$OUT/results.tsv
: >"$results"
below=0
for n in "${names[@]}"; do
    for side in maria mysql; do
        f=$HERE/corpus/$n.diff
        [[ -f $HERE/corpus/$n.$side.diff ]] && f=$HERE/corpus/$n.$side.diff
        [[ -f $f ]] || continue
        if [[ $side == maria ]]; then o=$OM:$DB s=$SM:$DB:tls; else o=$OY:$DB:tls s=$SY:$DB:tls; fi
        out=$OUT/$n-$side.out
        python3 -I "$HERE/diff.py" "$o" "$s" "$f" --caps "-SESSION_TRACK${SQLDIFF_CAPS:+,$SQLDIFF_CAPS}" --side "$side" >"$out" 2>&1 || true
        line=$(grep -E '^[0-9]+/[0-9]+ statements byte-identical' "$out" | tail -1 || true)
        vals=$(grep -E '^values equal [0-9]+/[0-9]+' "$out" | tail -1 | awk '{print $3}' || true)
        if [[ -z $line ]]; then
            echo "$n $side: no result (see $out)"
            printf '%s\t%s\t%s\t%s\t%s\n' "$n" "$side" "-" "-" "-" >>"$results"
            below=1
            continue
        fi
        got=${line%%/*}
        tot=${line#*/}
        tot=${tot%% *}
        printf '%s\t%s\t%s\t%s\t%s\n' "$n" "$side" "$got" "$tot" "${vals:-?}" >>"$results"
        fl=$(floor "$n" "$side")
        verdict="no floor"
        if [[ -n $fl ]]; then
            if ((got >= fl)); then verdict="ok (floor $fl)"; else
                verdict="BELOW FLOOR $fl"
                below=1
            fi
        fi
        echo "$n $side: $got/$tot byte-identical, values equal ${vals:-?}; $verdict"
    done
done
if [[ $RECORD == 1 ]]; then
    # This run's rows replace their corpora's; the other rows stay.
    {
        echo "# corpus	side	byte-identical floor	of	recorded"
        awk -F'\t' -v d="$(date +%F)" '
            FNR == NR { if ($3 != "-") { new[$1 "\t" $2] = $1 "\t" $2 "\t" $3 "\t" $4 "\t" d }; next }
            /^#/ { next }
            !(($1 "\t" $2) in new) { print }
            END { for (k in new) print new[k] }' "$results" "$HERE/floors.tsv" 2>/dev/null | sort
    } >"$HERE/floors.tsv.new"
    mv "$HERE/floors.tsv.new" "$HERE/floors.tsv"
    echo "floors recorded in $HERE/floors.tsv"
    exit 0
fi
exit "$below"
