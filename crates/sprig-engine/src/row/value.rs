//! One SQL value, typed as it is stored.

use bytes::Bytes;

use super::decimal::Decimal;
use super::temporal::{Date, DateTime, Time, Timestamp};

/// A value of one column. Which variant a column holds follows from its
/// [`ColumnType`](super::ColumnType): see [`ColumnType::check`](super::ColumnType::check).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// SQL `NULL`, in a column of any type.
    Null,
    /// A signed integer column (`TINYINT` to `BIGINT`, and `BOOL`).
    Int(i64),
    /// An unsigned integer column.
    UInt(u64),
    /// A `BIT(n)` column: the bits as an unsigned number.
    Bit(u64),
    /// A `DECIMAL(p,s)` column: exact, with exactly `s` fraction digits.
    Decimal(Decimal),
    /// A `FLOAT` column.
    Float(f32),
    /// A `DOUBLE` column.
    Double(f64),
    /// A `DATE` column.
    Date(Date),
    /// A `TIME(f)` column.
    Time(Time),
    /// A `DATETIME(f)` column.
    DateTime(DateTime),
    /// A `TIMESTAMP(f)` column, in UTC.
    Timestamp(Timestamp),
    /// A `YEAR` column: 0 or 1901 to 2155.
    Year(u16),
    /// A `CHAR`, `VARCHAR` or `TEXT` column: bytes in the column's character
    /// set. A `CHAR` value is held without its trailing pad spaces, as the
    /// servers return it.
    Str(Bytes),
    /// A `BINARY`, `VARBINARY` or `BLOB` column. A `BINARY(n)` value is held
    /// with its zero padding, all `n` bytes.
    Bytes(Bytes),
    /// An `ENUM` column: the member's 1-based index, or 0 for the error
    /// value `''`.
    Enum(u16),
    /// A `SET` column: bit `i` is the `i`-th member (from 0).
    Set(u64),
    /// A `JSON` column: opaque bytes (the document as the SQL layer encodes
    /// it).
    Json(Bytes),
}

impl Value {
    /// Whether this is `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// A string value from text.
    #[must_use]
    pub fn str(s: &str) -> Self {
        Value::Str(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// A binary value from bytes.
    #[must_use]
    pub fn bytes(b: &[u8]) -> Self {
        Value::Bytes(Bytes::copy_from_slice(b))
    }

    /// The variant's name, for error messages.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Int(_) => "a signed integer",
            Value::UInt(_) => "an unsigned integer",
            Value::Bit(_) => "a bit value",
            Value::Decimal(_) => "a decimal",
            Value::Float(_) => "a float",
            Value::Double(_) => "a double",
            Value::Date(_) => "a date",
            Value::Time(_) => "a time",
            Value::DateTime(_) => "a datetime",
            Value::Timestamp(_) => "a timestamp",
            Value::Year(_) => "a year",
            Value::Str(_) => "a string",
            Value::Bytes(_) => "a byte string",
            Value::Enum(_) => "an enum index",
            Value::Set(_) => "a set mask",
            Value::Json(_) => "a JSON document",
        }
    }
}
