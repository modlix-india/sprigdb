# deploy/bench: the market benchmark harness

One script per workload. Every script takes the endpoint and the product as
arguments and writes one JSON file per result to
`$BENCH_RESULTS/<model>/<product>/<run>.json`, with its raw logs under
`$BENCH_RESULTS/../logs/`. Nothing assumes the laptop: hosts and ports are
always arguments, so the same command runs on the benchmark VMs.

## Two kinds of workload

**Correctness, compatibility and features** (run anywhere):

| Script | What it runs | Result |
|---|---|---|
| `kv/redis-corpus.py` | The 416-command corpus of `crates/sprig-redis/tests/diff_live.rs` against a reference Redis (standard library RESP2 client) | `kv/correct/redis-corpus` matched |
| `kv/redis-clients.sh` | redis-py (RESP2, RESP3), ioredis, go-redis, optionally Jedis and Lettuce (`deploy/compose/r6/redis-clients`) against a reference Redis | `kv/compat/redis-clients` matched |
| `doc/mongoose.sh` | Mongoose 8.24.8's own suite, replica-set mode | `doc/compat/mongoose` passed |
| `doc/mongo-diff.sh` | The expression, stage and validation corpora of `crates/sprig-doc/tests/*_diff` replayed over a collection, against MongoDB 8.0.34's recorded answers | `doc/correct/mongo-diff-<corpus>` matched |
| `graph/cypher-corpus.py` | The Cypher corpus of `crates/sprig-graph/tests/bolt_live.rs` against a reference Neo4j (standard library Bolt 4.4 client) | `graph/correct/cypher-corpus` matched |
| `graph/tck.py` | The openCypher TCK feature files over Bolt | `graph/compat/opencypher-tck-bolt` passed |
| `graph/testkit.sh` | The Neo4j drivers' testkit through the Python driver's backend | `graph/compat/testkit-python` passed |
| `queue/amqp-diff.sh` | The 21 pika scenarios of `deploy/compose/clients-amqp/amqp_diff.py` against a reference RabbitMQ | `queue/correct/amqp-diff` matched |
| `queue/amqp10.sh` | AMQP 1.0 with rhea (`queue/amqp10.js`) against a reference RabbitMQ 4.x | `queue/compat/amqp10-rhea` matched |
| `stream/kafka-clients.sh` | kafka-python (`deploy/compose/clients-kafka/kafka_suite.py`) and kcat against a reference Apache Kafka | `stream/compat/kafka-python`, `kcat` |
| `features/*.py` | One probe per feature (below) | `<model>/features/feature-<name>` probe |

**Speed, CPU, memory and storage** (benchmark VMs only, never a shared laptop):

| Script | Workload | Measures |
|---|---|---|
| `sql/sysbench.sh` | sysbench oltp_point_select, read_only, read_write at 1/16/64 threads | tps, p50, p99 |
| `sql/tpcc.sh` | sysbench-tpcc, 10 warehouses (`--model dsql` for a distributed run) | tps, tpmC |
| `doc/ycsb.sh` | YCSB A, B, C, E, F over 1M documents, 16 threads (MongoDB protocol or Couchbase) | ops/s, read p50/p99, update p99; load ops/s |
| `kv/memtier.sh` | memtier_benchmark SET:GET 1:10, 50 clients, pipelines 1 and 16 | ops/s, p50, p99 |
| `kv/redis-benchmark.sh` | redis-benchmark core commands | ops/s per command |
| `queue/perftest.sh` | RabbitMQ PerfTest, 1 and 10 queues, persistent with confirms | send and receive msg/s, p50, p99 |
| `stream/kafka-perf.sh` | Kafka producer and consumer perf tests and end-to-end latency, acks=all | MB/s, p99 |
| `olap/tpch.sh` | TPC-H SF1 (DuckDB's dbgen and query text), 22 queries | per-query median, geometric mean |
| `vec/ann.sh` | ann-benchmarks sift-128-euclidean and glove-100-angular (pgvector, Qdrant, MongoDB-protocol `$vectorSearch`) | recall@10, QPS, p50/p99, build time |
| `fts/beir.sh` | BEIR scifact and nfcorpus (OpenSearch, `$search`, MySQL `FULLTEXT`) | nDCG@10, p50/p99, index time, freshness lag |
| `all/footprint.sh` | Resident memory and bytes on disk after the same dataset is loaded | MiB resident, MiB on disk |

LDBC SNB stays with its own tooling; record its results with `lib/result.py`.

## Common options

Shell scripts share `lib/common.sh`:

```
--target HOST:PORT   the endpoint under test (required)
--product NAME       SprigDB, MySQL 8.4, Valkey, ... (required)
--version V          recorded with the result
--competitor NAME    the report column (default: the product)
--user, --password, --db, --ref HOST:PORT, --run ID, --out DIR, --dry-run
-- ...              the script's own options (see each script's header)
```

`BENCH_RESULTS` names the results directory, `BENCH_BUILD` the build stamp
(e.g. `release 0.9.0 (abc1234)`), `BENCH_HOST_LABEL` the host (e.g.
`oci-vm2 A1 4/24`). Python scripts take the same options as flags.

## The result file

`lib/result.py` writes it (also usable from the command line):

```json
{"schema": "sprigdb-market-result/1", "kind": "measurement",
 "model": "kv", "op": "correct", "workload": "redis-corpus", "variant": null,
 "metric": "matched", "value": 415, "total": 416, "unit": "commands", "better": "higher",
 "product": "Valkey", "product_version": "9.0.0", "competitor": "Valkey",
 "title": "...", "notes": "...", "config": {"reference": "Redis 7.4.11"},
 "host": {"label": "laptop", "os": "Darwin", "cpus": 16, "memory_gb": 64},
 "date": "2026-11-17", "build": "release image (vendor)",
 "source": "logs/redis-corpus-valkey.txt", "command": "kv/redis-corpus.py ..."}
```

`kind` is `measurement` (one product's value; the report pairs SprigDB's with
each rival's of the same model, op, workload, variant and metric), `fact` (an
already-compared result with `verdict_inputs`), or `planned` (a workload that
is written but not run). Results are never edited by hand.

## Feature probes

`features/<feature>.py --conf targets.json --product <key>`, or
`features/run-all.sh targets.json [product ...]` for all of them:

joins, multi-document transactions, cross-model transactions, row-level
security, change feeds, vector search, text search, graph traversal, the
MySQL / MongoDB / Redis / Bolt wire protocols (`wire.py --protocol`), and
online resharding (multi-node only). Each probe uses the product's own
interface and records 1 (ran and passed), 0 (failed, or no such construct) or
no value (not probed, with the reason). A feature is "yes" only when its probe
ran and passed.

`targets.json` maps a product key to its family and endpoints:

```json
{"surrealdb": {"family": "surreal", "label": "SurrealDB", "version": "2.3.10",
               "http": "127.0.0.1:47603", "user": "root", "password": "...",
               "ns": "bench", "db": "bench", "ports": ["127.0.0.1:47603"]},
 "sprigdb": {"family": "sprig", "label": "SprigDB", "mysql": "HOST:3306", "ssl_mode": "DISABLED",
             "user": "acme.bench", "password": "...", "db": "bench_feat",
             "mongo": "HOST:27017", "mongo_user": "acme.bench", "mongo_password": "...",
             "ports": ["HOST:3306", "HOST:27017", "HOST:6379", "HOST:7687"]}}
```

Families: `mysql`, `pg`, `mongo`, `surreal`, `arango`, `couchbase` (`query`,
`search`, `bucket`), `sprig`. Needs the `mysql`, `psql` (`BENCH_PSQL`) and
`mongosh` clients.

## Rules the scripts follow

- Every client names its host and port; nothing uses a default port.
- Containers the scripts start are named `bench-*` and removed with `--rm`.
- Downloads (datasets, suites) go to their own directory; nothing runs an
  interpreter or build tool from inside a download directory.
- Speed numbers come only from the benchmark VMs: warm-up, then three runs,
  the median and spread reported; durability matched across products
  (fsync per acknowledged write) or reported both ways.

## A SQL comparison that can be quoted

`sql/sysbench.sh` refuses the shortcuts that made earlier SQL ratios weak:

- **The same isolation level.** `--isolation read-committed` (the default)
  is the level SprigDB, MySQL, TiDB and CockroachDB all offer; each
  connection sets it (`sql/isolation.lua`) and the run stops if the server
  reports another. On CockroachDB first run
  `SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true`, or it
  quietly runs SERIALIZABLE. A SERIALIZABLE comparison is SprigDB against
  CockroachDB only (TiDB has no SERIALIZABLE).
- **Three runs or more per cell**, after a warm-up, with the median, minimum,
  maximum and spread written by `lib/summarize.py`. Report the spread next to
  the ratio; a ratio inside the two products' spreads is a tie.
- **Data on disk.** Every product runs under the same memory cap per node,
  counting the page cache: a container's `--memory 4g --memory-swap 4g` for
  the rivals, `systemd-run --scope -p MemoryMax=4G -p MemorySwapMax=0` around
  `sprigdb` (with `SPRIGDB_ENGINE_MB` at half the cap), and each product's
  own cache at a quarter of it (CockroachDB `--cache=1GiB
  --max-sql-memory=1GiB`, TiKV `storage.block-cache.capacity = "1GB"`).
  The tables are sized so one node's data directory is at least three times
  the cap (10 tables of 10,000,000 rows for a 4 GiB cap), and the script
  checks it with `--data-bytes-cmd` (for example
  `"ssh vm1 sudo du -sb /data/engine"`) before it measures. A run that fits in
  memory must say so (`--in-memory`) and is never paired with one that
  doesn't.
- Rivals at a current release, and their versions in the result.

```sh
sql/sysbench.sh --target 10.97.1.11:3306 --product SprigDB --version "$BUILD" \
  --user bench.app --password "$PW" --db sbtest -- \
  --isolation read-committed --table-size 10000000 \
  --memory-cap-gb 4 --data-bytes-cmd "ssh ubuntu@10.97.1.11 sudo du -sb /data/engine"
```

## e0/: the storage-engine prototype's harness

`e0/` measures the engine prototype against MySQL 8.4 and Redis 7.4
(`appendfsync always`), with today's SprigDB as the "before" arm, all inside
Linux: on a laptop every arm and load generator is a container on the private
network `e0bench` (no port forward, no docker-proxy), with the same memory
limit, CPU set and primary cache; on benchmark VMs the same scripts run with
`E0_NET=host`. Settings are in `e0/env.sh`.

| Script | What it does |
|---|---|
| `e0/build-linux.sh <name>` | Linux release build in a container (glibc 2.36) into `$E0_WORK/bin/<name>/` |
| `e0/stack.sh up\|down\|kill\|start <arm>` | Arms: `mysql`, `mariadb`, `redis`, `sprig` (today's stack), `e0` (the same stack with the engine build and the benchmark databases on the engine) |
| `e0/bench.sh sql\|kv\|calib --arm <arm>` | sysbench point_select 1/16/64, memtier pipelines 1/16, hot-key redis-benchmark, 1M-key and sysbench footprints; CPU per operation from every SUT container's cgroup (or the VM's `/proc/stat`) |
| `e0/diff.sh sql\|redis` | The differential: E0's SQL statements against MySQL 8.4 or MariaDB 11.8 (answers, and prepared point-select packets byte for byte), E0's Redis commands against Redis |
| `e0/crash.sh --arm <arm> --proto redis\|sql` | kill -9 loops; every acknowledged write and the atomicity of multi-record writes checked after restart |
| `e0/laptop.sh sql\|kv\|diff\|crash <arm>` | One laptop set, through the heavy-slot queue |

Results use the format above, so the same report generator reads them.
