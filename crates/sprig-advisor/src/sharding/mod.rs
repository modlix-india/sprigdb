//! The sharding advisor's design logic: table groups, distribution keys
//! and verdicts from a workload and schema [`Input`], with no storage of
//! its own (the input assembly, run records and apply steps that worked on
//! MariaDB shard schemas are gone; this logic is kept for sharding big
//! tenants across nodes).
//!
//! | Part | Module |
//! |---|---|
//! | Graph, structural design, key filters, scoring, search, confidence, verdicts | [`design`] |
//! | Key-value units: shard a Redis database or namespace only when a rule says so | [`kv`] |
//! | The periodic run's `Rekey` verdict for tenants with table groups | [`periodic`] |
//!
//! [`settings`] declares the `advisor.*` keys.

// Short names (c, d, s, t, g) follow the design's pseudocode; the apply
// steps follow the design's plan step by step.
#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]

pub mod design;
pub mod kv;
pub mod periodic;
pub mod settings;

pub use design::{Cause, Design, Input, Score, Verdict, decide, search, structural};
pub use settings::{AdvisorMode, AdvisorSettings};
