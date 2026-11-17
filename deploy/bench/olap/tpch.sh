#!/usr/bin/env bash
# TPC-H at --sf (1) on an analytic engine: data from DuckDB's tpch extension
# (dbgen), loaded into the target, then the 22 queries, each --runs times
# after one warm-up pass. Records olap / complex / tpch: q01_ms ... q22_ms
# (median of the runs) and geomean_ms; a query that fails records nothing
# and is named in the log. Speed workload: benchmark VMs only.
#
#   olap/tpch.sh --target 10.0.0.3:3306 --product SprigDB --user u --password p --db tpch \
#       -- --engine mysql|clickhouse|duckdb [--sf 1] [--runs 3] [--duckdb /opt/duckdb] [--no-load]
#
# engine mysql: any MySQL-protocol endpoint (SprigDB's analytic path, MySQL,
#   MariaDB) through the mysql client; clickhouse: the HTTP port (--target
#   HOST:8123); duckdb: an in-process DuckDB file (--target local:0).
# Needs the duckdb CLI 1.3.2 (for dbgen and the query text) and python3.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/../lib/common.sh"
bench_args "$@"
ENGINE=mysql SF=1 RUNS=3 DUCK=${BENCH_DUCKDB:-duckdb} LOAD=1
set -- "${EXTRA[@]+"${EXTRA[@]}"}"
while [[ $# -gt 0 ]]; do case $1 in --engine) ENGINE=$2; shift 2 ;; --sf) SF=$2; shift 2 ;; --runs) RUNS=$2; shift 2 ;;
    --duckdb) DUCK=$2; shift 2 ;; --no-load) LOAD=0; shift ;; *) echo "unknown $1" >&2; exit 2 ;; esac; done
need "$DUCK" python3
data=$LOGDIR/data
mkdir -p "$data"
[[ $DRY == 1 ]] && { echo "would generate SF$SF and run 22 queries on $ENGINE"; exit 0; }
if [[ ! -f $data/lineitem.csv ]]; then
    "$DUCK" "$data/gen.duckdb" -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=$SF); EXPORT DATABASE '$data' (FORMAT csv, DELIMITER '|', HEADER false);" >"$LOGDIR/dbgen.log" 2>&1
fi
"$DUCK" -csv -noheader -c "INSTALL tpch; LOAD tpch; SELECT query_nr, replace(query, chr(10), ' ') FROM tpch_queries() ORDER BY 1" >"$LOGDIR/queries.csv"
VARIANT="sf=$SF"
python3 -I - "$ENGINE" "$HOST" "$PORT" "$B_USER" "$B_PASS" "${B_DB:-tpch}" "$data" "$LOGDIR" "$RUNS" "$LOAD" "$DUCK" <<'PY' >"$LOGDIR/times.tsv"
import csv, os, statistics, subprocess, sys, time, urllib.request, base64
engine, host, port, user, pw, db, data, logdir, runs, load, duck = sys.argv[1:12]
runs = int(runs)
TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]
schema = open(os.path.join(data, "schema.sql")).read()
def mysql(sql, database=db):
    env = dict(os.environ, MYSQL_PWD=pw)
    r = subprocess.run(["mysql", "-h", host, "-P", port, "-u", user, "--local-infile=1", "-N", "-B", "-D", database, "-e", sql],
                       capture_output=True, text=True, env=env)
    if r.returncode: raise RuntimeError(r.stderr.strip()[-300:])
def ch(sql):
    req = urllib.request.Request(f"http://{host}:{port}/?database={db}", data=sql.encode(), method="POST")
    req.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
    with urllib.request.urlopen(req, timeout=3600) as r: r.read()
def duckq(sql):
    r = subprocess.run([duck, os.path.join(logdir, "tpch.duckdb"), "-c", sql], capture_output=True, text=True)
    if r.returncode or "Error" in r.stderr: raise RuntimeError(r.stderr[-300:])
run = {"mysql": mysql, "clickhouse": ch, "duckdb": duckq}[engine]
if load == "1":
    if engine == "duckdb":
        duckq(schema + "".join(f"COPY {t} FROM '{data}/{t}.csv' (DELIMITER '|');" for t in TABLES))
    elif engine == "mysql":
        mysql(f"CREATE DATABASE IF NOT EXISTS {db}", "information_schema")
        for stmt in schema.split(";"):
            if stmt.strip(): mysql(stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"))
        for t in TABLES:
            mysql(f"LOAD DATA LOCAL INFILE '{data}/{t}.csv' INTO TABLE {t} FIELDS TERMINATED BY '|'")
    else:
        for stmt in schema.split(";"):
            if stmt.strip(): ch(stmt.strip().rstrip(")") + ") ENGINE = MergeTree ORDER BY tuple()")
        for t in TABLES:
            with open(f"{data}/{t}.csv", "rb") as f:
                req = urllib.request.Request(f"http://{host}:{port}/?database={db}&query=" + urllib.parse.quote(f"INSERT INTO {t} FORMAT CSV"), data=f.read(), method="POST")
                req.add_header("Authorization", "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
                urllib.request.urlopen(req, timeout=3600).read()
qs = list(csv.reader(open(os.path.join(logdir, "queries.csv"))))
for n, q in qs:
    try:
        run(q)  # warm-up
        ts = []
        for _ in range(runs):
            t = time.perf_counter(); run(q); ts.append((time.perf_counter() - t) * 1000)
        print(f"q{int(n):02d}\t{statistics.median(ts):.1f}")
    except Exception as e:
        print(f"q{int(n):02d}\tFAIL\t{str(e)[:200]}")
PY
cat "$LOGDIR/times.tsv"
geo=$(awk -F'\t' '$2!="FAIL"{s+=log($2); n++} END {if (n==22) printf "%.1f", exp(s/n)}' "$LOGDIR/times.tsv")
while IFS=$'\t' read -r q ms _; do
    [[ $ms == FAIL ]] && continue
    emit olap complex tpch "${q}_ms" "$ms" ms lower --config "runs=$RUNS" --config "engine=$ENGINE"
done <"$LOGDIR/times.tsv"
[[ -n $geo ]] && emit olap complex tpch geomean_ms "$geo" ms lower --config "runs=$RUNS" --config "engine=$ENGINE" \
    || echo "not every query ran: no geometric mean recorded" >&2
