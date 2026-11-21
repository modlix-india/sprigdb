//! Typed SQL values, column types and the stored row record.
//!
//! - [`Value`] and [`ColumnType`]: one value of one column, for every
//!   column type of MariaDB 11.8 and MySQL 8.4 (integers, `BIT`, `DECIMAL`,
//!   `FLOAT`, `DOUBLE`, the temporal types, character and binary strings,
//!   `ENUM`, `SET`, `JSON`).
//! - [`record`]: the row record a primary key points at.
//!
//! The types hold every value a server can have stored. Which values a
//! statement may store (ranges, truncation, rounding, zero dates under an
//! `sql_mode`) is decided by the SQL layer before a value gets here. The key
//! encodings of the same values are in [`crate::keys::typed`].

pub mod decimal;
pub mod record;
pub mod temporal;
pub mod types;
pub mod value;

pub use decimal::Decimal;
pub use record::{Column, RowHeader, RowSchema};
pub use temporal::{Date, DateTime, Time, Timestamp};
pub use types::{ColumnType, LobSize};
pub use value::Value;
