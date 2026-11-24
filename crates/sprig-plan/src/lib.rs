//! SprigDB's SQL planner: from a parsed statement and a catalog to a tree
//! of `sprig-exec` operators over row sources the caller supplies.
//!
//! - [`catalog`]: tables, keys and where they are found by name.
//! - [`access`]: what the planner asks of storage ([`access::Storage`]):
//!   row estimates for key ranges, point lookups and range scans, each
//!   counting the rows it hands over.
//! - [`mem`]: tables in memory behind both traits, for tests and for
//!   callers that have rows but no engine.
//! - [`Planner`]: name resolution, the range analyzer, access-path choice,
//!   the join-order search, grouping, ordering and `LIMIT` placement, and
//!   `EXPLAIN`.
//!
//! The planner's structure follows MariaDB's (the same resolution rules,
//! the same access types, a left-deep search); the algorithms and the
//! cost figures are SprigDB's own.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::float_cmp,
    clippy::match_same_arms,
    clippy::type_complexity,
    clippy::struct_excessive_bools,
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::needless_range_loop,
    clippy::single_match_else,
    clippy::items_after_statements,
    clippy::redundant_closure_for_method_calls,
    clippy::if_not_else,
    clippy::unnested_or_patterns,
    clippy::cognitive_complexity,
    // As in sprig-exec: small `Copy` types by reference, explicit
    // remainders, methods that keep `self` for symmetry, and conditions
    // written the way the rule they implement reads.
    clippy::trivially_copy_pass_by_ref,
    clippy::manual_is_multiple_of,
    clippy::unused_self,
    clippy::wrong_self_convention,
    clippy::assigning_clones,
    clippy::nonminimal_bool,
    clippy::needless_for_each,
    clippy::elidable_lifetime_names,
    clippy::unnecessary_wraps,
    clippy::needless_pass_by_value
)]

pub mod access;
mod block;
pub mod catalog;
mod dependent;
pub mod dialect;
mod explain;
mod exprs;
mod keyval;
pub mod mem;
mod plan;
mod range;
mod resolve;
mod sources;
pub mod whatif;

pub use explain::ExplainRow;
pub use plan::{AccessStat, Column, Plan, PlanOptions, Planner};
