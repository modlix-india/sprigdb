//! `sprig-streams`: event streams: storage, retention, change topics,
//! consumer groups and offsets.
//!
//! Layer: 2 data layer.
//!
//! What exists:
//!
//! - [`seg`]: streams stored on the node's engine: segment files fed by
//!   the engine's commit log, and offsets, groups and the topic catalog as
//!   engine records;
//! - [`grammar`], [`measure`]: `CREATE | ALTER | DROP STREAM`, `SHOW
//!   STREAMS`, `INSERT INTO <stream>` and the other statements the
//!   gateway recognises;
//! - [`partition`]: Kafka's murmur2 partitioner;
//! - [`envelope`]: change records of SQL tables;
//! - [`model`], [`publish`], [`read`], [`offsets`], [`settings`]: the
//!   records, checks, small parsers and `stream.*` keys those share;
//! - [`maria`]: the MariaDB error mapper the remaining MariaDB-backed
//!   messaging code shares.

// Statements read best as text built with format!, and the short
// pseudocode names (s, p, o, q) are kept.
#![allow(
    clippy::many_single_char_names,
    clippy::format_push_string,
    clippy::type_complexity
)]

pub mod envelope;
pub mod grammar;
pub mod maria;
pub mod measure;
pub mod model;
pub mod offsets;
pub mod partition;
pub mod publish;
pub mod read;
pub mod seg;
pub mod settings;

pub use model::{Cleanup, NewRecord, Offset, PartitionNo, Record, Source, StreamId, StreamKind};
