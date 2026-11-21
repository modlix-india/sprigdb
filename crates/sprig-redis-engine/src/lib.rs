//! `sprig-redis-engine`: Redis commands executed directly on SprigDB's
//! storage engine, for the Redis databases the engine holds.
//!
//! Layer: 2 front ends (with `sprig-redis`, whose session hands these
//! databases' data commands here through [`sprig_redis::engine`]).
//!
//! - [`store`]: what the commands need from the engine: a batch that holds
//!   its keys, reads the latest versions, and commits once, durably;
//! - [`layout`]: keys (metadata, elements, expiry entries) and the metadata
//!   encoding, with small hashes and lists inline;
//! - [`exec`]: the commands;
//! - [`db`]: one database: a pipeline's commands as one batch;
//! - [`sweep`]: active expiry;
//! - [`backend`]: which tenants are on the engine;
//! - [`engine_unit`]: the engine's transactions as a unit;
//! - [`mem`]: an in-memory unit for tests.
//!
//! Commands on one key are linearizable: a batch holds every key it may
//! write from its start until its commit is queued, so writes of a key run
//! one after another; reads take no lock and see the latest version queued
//! for commit. A batch's replies go out only once its commit, and
//! everything it read, is durable.

// Lengths and counts become RESP integers.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    // Command code names its arguments as Redis's documentation does
    // (k, f, v, m, s, ...).
    clippy::many_single_char_names,
    // Scores and increments are compared exactly, as Redis compares them.
    clippy::float_cmp,
    clippy::too_many_lines,
    // Scores, geohashes and sketch counts are doubles, as in Redis.
    clippy::cast_precision_loss
)]

pub mod backend;
pub mod db;
pub mod engine_unit;
pub mod exec;
pub mod layout;
pub mod mem;
pub mod store;
pub mod sweep;

pub use backend::{Backend, Tenants};
pub use db::{EngineDb, Uids, Versions, Waiters};
pub use engine_unit::EngineUnit;
pub use store::{Batch, StoreError, StoreResult, Unit};
