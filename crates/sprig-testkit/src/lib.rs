//! `sprig-testkit`: the test harness, deterministic simulation, differential testing and Compose fixtures.
//!
//! Layer: dev only.
//!
//! Modules:
//!
//! - [`env`](mod@env): [`Env`], a description of a test environment (data centers,
//!   MariaDB clusters with their topology and endpoints, etcd tiers), read
//!   from `SPRIG_TEST_*` variables or a TOML file;
//! - [`skip`]: the `SPRIG_TEST_*` convention and [`require_env!`], which
//!   fails a live test run without its environment (live tests are
//!   `#[ignore]`d, so plain runs list them as skipped);
//! - [`compose`]: Docker Compose fixtures that start a project, wait for
//!   health, collect logs on failure and tear down on drop;
//! - [`util`]: unique names, temporary directories and retry with a timeout;
//! - [`sim`]: deterministic simulation: seeded runs on a paused-clock
//!   runtime, a simulated network, etcd ([`sim::SimEtcd`]) and clocks,
//!   fault plans that shrink on failure, and the MariaDB model with its
//!   conformance scenarios ([`sim::mariadb`]);
//! - [`faults`]: the [`faults::Faults`] trait, toxiproxy and `faultd` for
//!   Compose, throwaway `sprig-sim-*` containers, operation histories and
//!   `elle-cli`;
//! - [`fleet`]: the fleet simulator (placement and
//!   rebalancing over months of synthetic load, with a fake mover).
//!
//! The differential suites live with the crates they test (for example
//! `sprig-router`'s `differential_live`).
//!
//! # Using it from another crate
//!
//! Add `sprig-testkit` as a dev-dependency, mark each live test
//! `#[ignore = "live: needs <VAR>; tools/live/README.md"]` and read its
//! variable:
//!
//! ```no_run
//! # async fn example() {
//! use sprig_testkit::{require_env, skip};
//!
//! // Fails the test unless SPRIG_TEST_ETCD is set.
//! let endpoints = require_env!(skip::ETCD);
//! # let _ = endpoints;
//! # }
//! ```

pub mod compose;
pub mod connectors;
pub mod env;
pub mod faults;
pub mod fleet;
pub mod sim;
pub mod skip;
pub mod util;

pub use compose::{ComposeProject, ComposeStack};
pub use env::{ClusterEnv, DcEnv, Env, Link, Topology};
pub use util::{Retry, TempDir, unique_ident, unique_name};
