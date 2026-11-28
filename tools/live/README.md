# Live test suites

A live test needs something running: a SprigDB stack, a reference server
(Redis, MongoDB, MySQL), etcd, Docker. Each one is marked

```rust
#[ignore = "live: needs SPRIG_TEST_MONGO_URI; tools/live/README.md"]
```

so `cargo test` and `cargo nextest run` list it as ignored (skipped) instead
of passing it without running. Run on purpose (`--ignored`,
`--run-ignored all`) without its variables, it fails: a green run means the
tests ran. The attribute's text names the variables the test reads; the
test file's header says what each holds.

## The local stack

```sh
cargo build -p sprigdb -p sprigctl          # target/debug/sprigdb, sprigctl
tools/live/stack.sh up 1                    # etcd container, controller, gateway 1
tools/live/stack.sh tenant live             # tenant live, login live.app / sprig-live-app
tools/live/stack.sh env live                # the addresses, as VAR=value lines
tools/live/stack.sh down                    # stop; `purge` also deletes target/live-stack
```

`up 3` starts three gateways whose units replicate over Raft, for the
checkers in `tools/checkers`. Ports are on 127.0.0.1 from a base of 45000
(`SPRIG_LIVE_PORT`): gateway n's MySQL port is 45000 + 100n + 6, MariaDB +7,
Redis +9, MongoDB +4, HTTP +1, Kafka +2, AMQP +5, Bolt +0; `stack.sh port
<n> <listener>` prints one. The script's header lists every variable.

## Suites

```sh
tools/live/run.sh list
tools/live/run.sh redis-engine     # Docker only: a reference Redis 7.4.11
tools/live/run.sh redis            # + the stack: the gateway's Redis port
tools/live/run.sh mongo            # + the stack and a reference MongoDB 8.0.34
```

`run.sh` starts what the suite needs, sets its `SPRIG_TEST_*` variables,
runs its ignored tests one at a time, and removes its containers (`--keep`
leaves them). CI's "S1 live suites (engine)" job runs `redis-engine` and,
on demand, the others.

## Tests that still need an environment of their own

Most live tests were written for the stacks before the engine (MariaDB
clusters on Compose, `deploy/compose/integration.sh`) or for one agent's
private deployment, and read variables no suite above sets. They are
ignored like the rest; run one by setting the variables its `#[ignore]`
text names and

```sh
cargo test -p <crate> --test <file> -- --ignored <test name>
```

CI's MariaDB jobs (`integration`, `m03`, `studio-e2e`, on demand and
weekly) run the ones `deploy/compose/integration.sh` serves, with
`--run-ignored all` and the `ci-s1` profile of `.config/nextest.toml`.

Some tests are not marked, on purpose: child processes that a kill test
starts through the test harness (`*_child`), tools that run through the
harness (fixture capture, fuzz-seed writers, probes), and tests that run
in process and skip one step when pointed at a server. A few live tests
also skip an optional step when its extra variable is unset (a direct
backend probe, an Elle check); the test itself still runs.
