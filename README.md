<p align="center">
  <img src="brand/mascot/coralgold/png/01-waving-1024.png" width="220" alt="Sprig, the SprigDB axolotl, waving">
</p>

<h1 align="center">SprigDB</h1>

<p align="center">
  <b>One database for every data model. It runs itself.</b><br>
  An open-source, multi-model database with its own storage engine, built in Rust.
</p>

<p align="center">
  <a href="LICENSE"><img alt="Licence: Apache-2.0" src="https://img.shields.io/badge/licence-Apache--2.0-e0805a"></a>
  <img alt="Rust" src="https://img.shields.io/badge/built%20with-Rust-a5502c">
</p>

---

Say hello to **Sprig** 👋, the axolotl on this page. Axolotls regrow what they lose, and SprigDB aims to do the same for your data: it moves, reshapes and heals itself so you don't have to.

Your applications connect as they already do, with a MySQL or MariaDB driver, `mongosh`, `redis-cli`, a Neo4j driver, a Kafka client or an AMQP client. Behind that, every model lives in one storage engine of SprigDB's own: RocksDB underneath, multi-version transactions with row locks on top, and each tenant's data replicated over Raft across nodes. SprigDB aims to take care of what normally needs a DBA:
- deciding where data lives;
- moving and replicating it as nodes come and go;
- keeping tenants apart;
- surviving a failed node.

## Why SprigDB

- **Many models, one store.** Tables, documents, key-value, vectors, graphs, streams and queues in one engine, with transactions that span them.
- **Your drivers just work.** MySQL 8.4 and MariaDB personas, the MongoDB 8 wire protocol, Redis (RESP), Neo4j Bolt, Kafka and AMQP, plus REST and gRPC.
- **Placement without a DBA.** Each tenant's data is placed on nodes in distinct failure domains, replicated, and moved or re-replicated when a node drains or dies.
- **Multi-tenant by design.** Tenant trees, inherited settings, grants per user and row-level security.
- **Logic next to the data.** Functions and triggers in KIRun, JavaScript or Lua, and sinks to other systems.
- **You stay in charge.** Every automatic decision can run on its own, wait for your approval, or only recommend.
- **Studio and `sprig`.** A browser workspace and a terminal shell with the same calm, coral-gold look.

## A look inside

| Studio | Editing a table |
|---|---|
| ![Studio's workspace with the explorer and the mascot](site/public/img/shots/welcome-light.png) | ![A table with staged edits: changed cells amber, new rows green, deleted rows struck through](site/public/img/shots/table-staged.png) |
| **Automatic indexing** | **`sprig`, the terminal UI** |
| ![Automatic indexing in the Performance section](site/public/img/shots/performance-light.png) | ![The sprig terminal UI with a result grid](site/public/img/shots/tui-grid.png) |

## Quick start

You need Docker (for etcd), `jq`, and Rust: `rust-toolchain.toml` pins the version, and rustup installs it on first use.

```sh
# 1. Build the node and the command-line tool
cargo build -p sprigdb -p sprigctl

# 2. etcd in a container, a controller and one node on your machine (tools/live/README.md)
tools/live/stack.sh up 1

# 3. A tenant, acme, with the user app
tools/live/stack.sh tenant acme sprig-dev-app

# 4. Connect with any MySQL client (port 45107 is the MariaDB persona)
mysql -h 127.0.0.1 -P 45106 -u acme.app -psprig-dev-app --ssl-mode=REQUIRED
```

```sql
CREATE DATABASE shop;
CREATE TABLE shop.orders (id INT PRIMARY KEY, status VARCHAR(20), total DECIMAL(10,2));
INSERT INTO shop.orders VALUES (1, 'paid', 42.50);
SELECT * FROM shop.orders;
```

`tools/live/stack.sh up 3` starts three nodes that replicate each tenant over Raft; `tools/live/stack.sh down` stops everything.

**Studio** is served on the node's HTTP port, <http://127.0.0.1:45101/studio> (sign in as `acme.app`), from a build that includes it: `cd studio && pnpm install && pnpm build`, then build `sprigdb` again (it embeds `studio/dist`). The **`sprig`** shell is `cargo run -p sprig-shell --bin sprig -- --url http://127.0.0.1:45101 -u acme.app`.

These credentials are for local development only. Never reuse them.

## Documentation

- **[Concepts](docs/concepts/)**: what SprigDB is and how it works: tenants, clusters, shards, automatic indexing, approvals.
- **[Get started](docs/get-started/)** and **[Connect](docs/connect/)**: run it, then connect with MySQL, MongoDB, Redis, Bolt, Kafka, AMQP or REST.
- **[SQL](docs/sql/)**, **[Data models](docs/data-models/)**, **[Performance](docs/performance/)**, **[Security](docs/security/)**: using it.
- **[Operate](docs/operate/)**, **[Tools](docs/tools/)**, **[Troubleshooting](docs/troubleshooting/)**: running it.
- **[Reference](docs/reference/)**: every setting, error code, metric, REST route and command. These pages are generated from the code, so they always match this release.
- **[Developing SprigDB](docs/operate/development.md)**: building from source, the local environments, running a single role, the tests.

The same pages are published as a website, with search and a PDF download, from [`site/`](site/).

## Project status

SprigDB is under active development, and not yet ready for production. A single node, its protocols, Studio and `sprig` work today. Replication, failover and placement across nodes are built and still being verified; until that is done, don't rely on them. Expect changes before 1.0.

## Building from source

```sh
cargo build --workspace
cargo test --workspace          # tests that need a running stack are ignored here
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
tools/live/run.sh list          # the live suites, against a local stack
```

The checkers behind the correctness numbers (the kill loop, follower reads under faults, the SQL differential) are in [`tools/`](tools/README.md).

See [Developing SprigDB](docs/operate/development.md) for everything else.

## Licence

SprigDB is licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE).

<p align="center">
  <img src="brand/mascot/coralgold/png/08-celebrating-1024.png" width="120" alt="Sprig celebrating">
</p>
