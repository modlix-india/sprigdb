# Fault checkers

Python 3 scripts, standard library only, that run a workload against a
replicated SprigDB, inject faults and check the outcome. Each exits 0 only
when the check holds. `probe.py` is a minimal MySQL-protocol client the
others use (TLS, prepared statements, LOCAL INFILE).

Start a three-gateway stack first (see `tools/live/README.md`):

```sh
cargo build -p sprigdb -p sprigctl
tools/live/stack.sh up 3
```

| Script | What it checks | Run |
|---|---|---|
| `gw_kill.py` | **Kill loop.** SQL and Redis writers on one tenant; each round `kill -9`s the gateway leading the tenant's unit and restarts it. RPO: every acknowledged write is on every replica at the end (after a restart of all three). RTO: kill to the first acknowledged write, per front end. Tests failover of insert-only keys; not fsync durability, overwrites or transactions. | `tools/live/stack.sh tenant kill`; `python3 tools/checkers/gw_kill.py 10 4 4` (rounds, SQL writers, Redis writers) |
| `ddl_faults.py` | **Follower reads under DDL and faults.** A writer creates tables on the leader; readers on the followers read the table before the newest acknowledged one, which must always exist (the two-version catalog rule); faults: SIGSTOP or kill a follower, kill the leader, cut Raft links. Every replica lists the same tables at the end. Needs the `mysql` client. | `tools/live/stack.sh tenant ddl`; `python3 tools/checkers/ddl_faults.py ddl 300 12` (tenant, tables, seconds between faults) |
| `repl_kill_loop.py` | **Replicated units without the gateway.** Three `repl_node` processes (`crates/sprig-raft/examples/repl_node.rs`) on loopback ports 26171-26183; kills the leader of a unit in a loop under writes; RPO and RTO as above. | `cargo build --release -p sprig-raft --example repl_node`; `python3 tools/checkers/repl_kill_loop.py target/release/examples/repl_node 30` |

Related checks that live elsewhere:

- The Kafka kill loop: `crates/sprig-proto-kafka/tests/live_repl.rs` (`live_kill_loop`, ignored; its header lists the variables).
- Follower reads under load and their staleness bound: `cargo run -p sprig-gateway --example followcheck` (`crates/sprig-gateway/examples/followcheck.rs`).
- Read load for follower-read throughput: `crates/sprig-testkit/examples/readload.rs`.

Remote clusters (the benchmark VMs): `gw_kill.py` takes `GW_HOSTS`,
`GW_MYSQL`, `GW_REDIS`, `GW_HTTP`, `GW_READYZ` and `GW_CTL` (a command taking
`kill gwN`, `start gwN` and `ctl ...`); see its header.
