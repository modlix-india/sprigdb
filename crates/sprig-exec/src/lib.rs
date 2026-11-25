//! SprigDB's SQL expression engine and row executor.
//!
//! - [`expr`]: a resolved expression tree ([`Expr`]) is compiled once per
//!   plan ([`compile`]) into a typed closure over row slots. Typing,
//!   conversion, `NULL` handling, warnings and errors follow MariaDB 11.8
//!   or MySQL 8.4, as the session's [`Persona`] says.
//! - [`ops`]: row operators over a [`ops::RowSource`]: filter, project,
//!   limit, sort, top-N, aggregation, joins, `UNION`.
//!
//! Values are the storage engine's own ([`sprig_engine::row::Value`]), so a
//! scan's rows are evaluated without conversion.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::float_cmp,
    clippy::match_same_arms,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::struct_excessive_bools,
    clippy::should_implement_trait,
    // Matches on one pattern, nested ifs and explicit loops are written
    // out where they read closer to the rule they implement.
    clippy::single_match_else,
    clippy::single_match,
    clippy::collapsible_if,
    clippy::collapsible_else_if,
    clippy::needless_range_loop,
    clippy::trivially_copy_pass_by_ref,
    clippy::format_push_string,
    clippy::manual_is_multiple_of,
    clippy::manual_range_contains,
    clippy::nonminimal_bool,
    clippy::missing_fields_in_debug,
    clippy::explicit_iter_loop,
    clippy::assigning_clones,
    clippy::if_not_else,
    clippy::comparison_chain,
    clippy::items_after_statements,
    clippy::redundant_closure_for_method_calls,
    clippy::unnested_or_patterns,
    clippy::match_wildcard_for_single_variants,
    clippy::bool_to_int_with_if,
    clippy::struct_field_names
)]

mod charset;
mod conv;
pub mod error;
pub mod expr;
mod func;
mod num;
pub mod ops;
pub mod session;
mod temporal;
pub mod types;
mod tz;

pub use error::{Level, Result, SqlError, Warning};
pub use expr::{BinOp, CastTarget, ColumnRef, Compiled, Expr, Truth, compile, value_text};
pub use func::json::json_order;
pub use func::misc::{SessionInfo, take_set_insert_id};
pub use func::sequence::Sequences;
pub use session::{Ctx, Persona, Session, SqlMode};
pub use types::{ExprType, FieldType, Kind, Meta};
