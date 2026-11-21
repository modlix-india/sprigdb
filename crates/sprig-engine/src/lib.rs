//! SprigDB's storage engine.
//!
//! One node keeps one ordered, versioned keyspace in RocksDB (the `data`
//! and `local` column families), behind the [`store::SortedStore`] trait so
//! an in-memory fake ([`store::MemStore`]) can stand in for it in the
//! deterministic simulator and in model tests. Commits go through a
//! [`log::Consensus`] implementation: on a single node that is
//! [`log::LocalLog`], raft-engine with one voter and group commit. The log is
//! the write-ahead log; RocksDB's own WAL is off for engine data, and
//! [`Engine::open`] replays the log into RocksDB after a crash.
//!
//! Module map:
//! - [`keys`]: the key layout (`tenant | shard | object | kind | index |
//!   user key | !version`), memcomparable encodings and collations;
//! - [`store`]: `SortedStore`, `RocksStore`, `MemStore`;
//! - [`log`]: the commit log, durability classes, group commit, the apply
//!   step and the payload format;
//! - [`row`]: typed SQL values, column types and the row record;
//! - [`budget`]: the engine's memory budgets;
//! - [`txn`] and [`locks`]: transactions, MVCC reads and record locks;
//! - [`Engine`]: the handle that ties them together.

pub mod budget;
pub mod config;
mod engine;
pub mod keys;
pub mod locks;
pub mod log;
pub mod row;
pub mod store;
pub mod txn;

pub use config::{EngineConfig, StoreKind};
pub use engine::{Engine, EngineStats, VersionGc};
pub use keys::Ts;
pub use log::{Durability, UnitId};
pub use store::{Cf, Read};

#[cfg(test)]
pub(crate) mod testutil;
