//! Column types: what a column may hold and how two of its values order.

use std::cmp::Ordering;

use sprig_common::{Result, SprigError};

use super::decimal::Decimal;
use super::temporal::{frac_bytes, frac_fits};
use super::value::Value;
use crate::keys::collation::Collation;

/// The four sizes of the `TEXT` and `BLOB` families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LobSize {
    /// `TINYTEXT`, `TINYBLOB`: up to 255 bytes.
    Tiny,
    /// `TEXT`, `BLOB`: up to 65,535 bytes.
    Normal,
    /// `MEDIUMTEXT`, `MEDIUMBLOB`: up to 16,777,215 bytes.
    Medium,
    /// `LONGTEXT`, `LONGBLOB`: up to 4,294,967,295 bytes.
    Long,
}

impl LobSize {
    /// The largest value in bytes.
    #[must_use]
    pub const fn max_bytes(self) -> u64 {
        match self {
            LobSize::Tiny => 255,
            LobSize::Normal => 65_535,
            LobSize::Medium => 16_777_215,
            LobSize::Long => 4_294_967_295,
        }
    }
}

/// The type of one column, with everything storage depends on.
///
/// Display widths, `ZEROFILL`, member names of `ENUM` and `SET`, and whether
/// a `utf8` column is the 3-byte kind are catalog facts the SQL layer keeps:
/// they change neither the stored bytes nor the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    /// `TINYINT` (1 byte), `SMALLINT` (2), `MEDIUMINT` (3), `INT` (4) or
    /// `BIGINT` (8). `BOOL` is a signed 1-byte integer.
    Int {
        /// 1, 2, 3, 4 or 8.
        bytes: u8,
        /// `UNSIGNED`.
        unsigned: bool,
    },
    /// `BIT(bits)`, 1 to 64 bits.
    Bit {
        /// 1 to 64.
        bits: u8,
    },
    /// `DECIMAL(precision, scale)`.
    Decimal {
        /// Digits in all, 1 to 65.
        precision: u8,
        /// Digits after the point, 0 to 38 and at most `precision`.
        scale: u8,
    },
    /// `FLOAT`: IEEE 754 single precision.
    Float,
    /// `DOUBLE`: IEEE 754 double precision.
    Double,
    /// `DATE`.
    Date,
    /// `TIME(fsp)`.
    Time {
        /// Fraction digits, 0 to 6.
        fsp: u8,
    },
    /// `DATETIME(fsp)`.
    DateTime {
        /// Fraction digits, 0 to 6.
        fsp: u8,
    },
    /// `TIMESTAMP(fsp)`, in UTC.
    Timestamp {
        /// Fraction digits, 0 to 6.
        fsp: u8,
    },
    /// `YEAR`.
    Year,
    /// `CHAR(chars)`.
    Char {
        /// Length in characters, 0 to 255.
        chars: u16,
        /// The column's collation (and, through it, its character set).
        collation: Collation,
    },
    /// `VARCHAR(chars)`.
    VarChar {
        /// Length in characters, up to 65,535.
        chars: u32,
        /// The column's collation.
        collation: Collation,
    },
    /// `TINYTEXT` to `LONGTEXT`.
    Text {
        /// Which of the four.
        size: LobSize,
        /// The column's collation.
        collation: Collation,
    },
    /// `BINARY(bytes)`: fixed length, zero padded.
    Binary {
        /// Length in bytes, 0 to 255.
        bytes: u16,
    },
    /// `VARBINARY(bytes)`.
    VarBinary {
        /// Length in bytes, up to 65,535.
        bytes: u32,
    },
    /// `TINYBLOB` to `LONGBLOB`.
    Blob {
        /// Which of the four.
        size: LobSize,
    },
    /// `ENUM` with this many members.
    Enum {
        /// 1 to 65,535.
        members: u16,
    },
    /// `SET` with this many members.
    Set {
        /// 1 to 64.
        members: u8,
    },
    /// `JSON`: opaque bytes; never part of a key.
    Json,
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::internal(msg)
}

impl ColumnType {
    /// `TINYINT`.
    #[must_use]
    pub const fn tinyint(unsigned: bool) -> Self {
        Self::Int { bytes: 1, unsigned }
    }
    /// `SMALLINT`.
    #[must_use]
    pub const fn smallint(unsigned: bool) -> Self {
        Self::Int { bytes: 2, unsigned }
    }
    /// `MEDIUMINT`.
    #[must_use]
    pub const fn mediumint(unsigned: bool) -> Self {
        Self::Int { bytes: 3, unsigned }
    }
    /// `INT`.
    #[must_use]
    pub const fn int(unsigned: bool) -> Self {
        Self::Int { bytes: 4, unsigned }
    }
    /// `BIGINT`.
    #[must_use]
    pub const fn bigint(unsigned: bool) -> Self {
        Self::Int { bytes: 8, unsigned }
    }
    /// `BOOL`, which is `TINYINT(1)`.
    #[must_use]
    pub const fn boolean() -> Self {
        Self::tinyint(false)
    }

    /// Checks the declaration against the servers' limits.
    ///
    /// # Errors
    /// A width, precision, scale, length or member count out of range.
    pub fn validate(&self) -> Result<()> {
        let ok = match *self {
            Self::Int { bytes, .. } => matches!(bytes, 1..=4 | 8),
            Self::Bit { bits } => (1..=64).contains(&bits),
            Self::Decimal { precision, scale } => {
                (1..=65).contains(&precision) && scale <= 38 && scale <= precision
            }
            Self::Time { fsp } | Self::DateTime { fsp } | Self::Timestamp { fsp } => fsp <= 6,
            Self::Char { chars, .. } => chars <= 255,
            Self::VarChar { chars, .. } => chars <= 65_535,
            Self::Binary { bytes } => bytes <= 255,
            Self::VarBinary { bytes } => bytes <= 65_535,
            Self::Enum { members } => members >= 1,
            Self::Set { members } => (1..=64).contains(&members),
            Self::Float
            | Self::Double
            | Self::Date
            | Self::Year
            | Self::Text { .. }
            | Self::Blob { .. }
            | Self::Json => true,
        };
        if ok {
            Ok(())
        } else {
            Err(bad(format!("{self:?} is not a valid column type")))
        }
    }

    /// The collation of a character column.
    #[must_use]
    pub const fn collation(&self) -> Option<Collation> {
        match *self {
            Self::Char { collation, .. }
            | Self::VarChar { collation, .. }
            | Self::Text { collation, .. } => Some(collation),
            _ => None,
        }
    }

    /// The bytes a value takes in the fixed part of a row record, or `None`
    /// for a variable-length type.
    #[must_use]
    pub fn fixed_width(&self) -> Option<usize> {
        Some(match *self {
            Self::Int { bytes, .. } => usize::from(bytes),
            Self::Bit { bits } => usize::from(bits).div_ceil(8),
            Self::Decimal { precision, scale } => packed_decimal_len(precision, scale),
            Self::Float => 4,
            Self::Double | Self::Set { .. } => 8,
            Self::Date => 3,
            Self::Time { fsp } => 3 + frac_bytes(fsp),
            Self::DateTime { fsp } => 5 + frac_bytes(fsp),
            Self::Timestamp { fsp } => 4 + frac_bytes(fsp),
            Self::Year => 1,
            Self::Binary { bytes } => usize::from(bytes),
            Self::Enum { .. } => 2,
            Self::Char { .. }
            | Self::VarChar { .. }
            | Self::Text { .. }
            | Self::VarBinary { .. }
            | Self::Blob { .. }
            | Self::Json => return None,
        })
    }

    /// Whether a column of this type can be part of a key. `JSON` can't: an
    /// index over JSON encodes the extracted value in its own type.
    #[must_use]
    pub const fn is_key_type(&self) -> bool {
        !matches!(self, Self::Json)
    }

    /// Checks that `v` is a value this column stores as it is: the right
    /// variant, inside the type's range, with no digits or characters the
    /// column has no room for. `NULL` passes; nullability is the column
    /// definition's.
    ///
    /// # Errors
    /// The value is of another type or outside the column's domain.
    pub fn check(&self, v: &Value) -> Result<()> {
        if v.is_null() || self.holds(v) {
            Ok(())
        } else {
            Err(bad(format!(
                "{} is not a storable value of {self:?}",
                v.kind()
            )))
        }
    }

    fn holds(self, v: &Value) -> bool {
        match (self, v) {
            (
                Self::Int {
                    bytes,
                    unsigned: false,
                },
                Value::Int(i),
            ) => {
                let bits = u32::from(bytes) * 8;
                bits == 64 || (-(1i64 << (bits - 1))..(1i64 << (bits - 1))).contains(i)
            }
            (
                Self::Int {
                    bytes,
                    unsigned: true,
                },
                Value::UInt(u),
            ) => bytes == 8 || *u < 1u64 << (u32::from(bytes) * 8),
            (Self::Bit { bits }, Value::Bit(b)) => bits >= 64 || *b < 1u64 << bits,
            (Self::Decimal { precision, scale }, Value::Decimal(d)) => d.fits(precision, scale),
            (Self::Float, Value::Float(f)) => f.is_finite(),
            (Self::Double, Value::Double(f)) => f.is_finite(),
            (Self::Date, Value::Date(d)) => d.is_storable(),
            (Self::Time { fsp }, Value::Time(t)) => {
                // Below 10^6, so the cast is exact.
                #[allow(clippy::cast_possible_truncation)]
                let frac = (t.micros().unsigned_abs() % 1_000_000) as u32;
                frac_fits(frac, fsp)
            }
            (Self::DateTime { fsp }, Value::DateTime(d)) => {
                d.is_storable() && frac_fits(d.micros, fsp)
            }
            (Self::Timestamp { fsp }, Value::Timestamp(t)) => frac_fits(t.micros, fsp),
            (Self::Year, Value::Year(y)) => *y == 0 || (1901..=2155).contains(y),
            (Self::Char { chars, collation }, Value::Str(s)) => {
                collation.charset().char_count(s) <= usize::from(chars)
            }
            (Self::VarChar { chars, collation }, Value::Str(s)) => {
                collation.charset().char_count(s) <= chars as usize
            }
            (Self::Text { size, .. }, Value::Str(s)) => s.len() as u64 <= size.max_bytes(),
            (Self::Binary { bytes }, Value::Bytes(b)) => b.len() == usize::from(bytes),
            (Self::VarBinary { bytes }, Value::Bytes(b)) => b.len() <= bytes as usize,
            (Self::Blob { size }, Value::Bytes(b)) => b.len() as u64 <= size.max_bytes(),
            (Self::Enum { members }, Value::Enum(i)) => *i <= members,
            (Self::Set { members }, Value::Set(m)) => members >= 64 || *m < 1u64 << members,
            (Self::Json, Value::Json(_)) => true,
            _ => false,
        }
    }

    /// The order of two values of this column, as `ORDER BY` on the column
    /// gives it: `NULL` first, numbers numerically (`1.0 = 1.00`,
    /// `-0.0 = 0.0`), temporals chronologically, character strings by the
    /// collation, byte strings by bytes, `ENUM` by index, `SET` and `BIT` by
    /// their number. This is the reference the key encoding is tested
    /// against.
    ///
    /// Values of the wrong variant order by variant, so the function is
    /// total; [`ColumnType::check`] is what rejects them.
    #[must_use]
    pub fn compare(&self, left: &Value, right: &Value) -> Ordering {
        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::UInt(x), Value::UInt(y))
            | (Value::Bit(x), Value::Bit(y))
            | (Value::Set(x), Value::Set(y)) => x.cmp(y),
            (Value::Decimal(x), Value::Decimal(y)) => x.cmp_numeric(y),
            (Value::Float(x), Value::Float(y)) => cmp_f64(f64::from(*x), f64::from(*y)),
            (Value::Double(x), Value::Double(y)) => cmp_f64(*x, *y),
            (Value::Date(x), Value::Date(y)) => x.cmp(y),
            (Value::Time(x), Value::Time(y)) => x.cmp(y),
            (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
            (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
            (Value::Year(x), Value::Year(y)) | (Value::Enum(x), Value::Enum(y)) => x.cmp(y),
            (Value::Str(x), Value::Str(y)) => match self.collation() {
                Some(c) => c.compare(x, y),
                None => x.cmp(y),
            },
            (Value::Bytes(x), Value::Bytes(y)) | (Value::Json(x), Value::Json(y)) => x.cmp(y),
            _ => variant_rank(left).cmp(&variant_rank(right)),
        }
    }
}

/// The numeric order of two doubles, with `-0.0 = 0.0`; a NaN (never
/// stored) sorts by its bits, after the numbers of its sign.
fn cmp_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or_else(|| a.total_cmp(&b))
}

fn variant_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Int(_) => 1,
        Value::UInt(_) => 2,
        Value::Bit(_) => 3,
        Value::Decimal(_) => 4,
        Value::Float(_) => 5,
        Value::Double(_) => 6,
        Value::Date(_) => 7,
        Value::Time(_) => 8,
        Value::DateTime(_) => 9,
        Value::Timestamp(_) => 10,
        Value::Year(_) => 11,
        Value::Str(_) => 12,
        Value::Bytes(_) => 13,
        Value::Enum(_) => 14,
        Value::Set(_) => 15,
        Value::Json(_) => 16,
    }
}

/// Bytes that hold 0 to 9 decimal digits in the packed decimal layout.
const DIGIT_BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

/// The bytes of a packed `DECIMAL(precision, scale)`: four per nine digits
/// on each side of the point, and one to four for the digits left over (the
/// sizes both servers document).
#[must_use]
pub fn packed_decimal_len(precision: u8, scale: u8) -> usize {
    let int = usize::from(precision.saturating_sub(scale));
    let frac = usize::from(scale);
    int / 9 * 4 + DIGIT_BYTES[int % 9] + frac / 9 * 4 + DIGIT_BYTES[frac % 9]
}

/// Packs a decimal that [fits](Decimal::fits) `DECIMAL(precision, scale)`
/// into [`packed_decimal_len`] bytes: the integer digits in groups of nine
/// from the point leftwards, the fraction digits in groups of nine from the
/// point rightwards, each group a big-endian number in the bytes its digit
/// count needs; then the top bit set, and every byte inverted for a negative
/// value. The bytes order like the numbers.
///
/// # Errors
/// The value doesn't fit the column.
pub fn pack_decimal(d: &Decimal, precision: u8, scale: u8, out: &mut Vec<u8>) -> Result<()> {
    if !d.fits(precision, scale) {
        return Err(bad(format!("{d} doesn't fit DECIMAL({precision},{scale})")));
    }
    let start = out.len();
    let int_len = usize::from(precision - scale);
    let mut int = vec![0u8; int_len - d.int_digits().len()];
    int.extend_from_slice(d.int_digits());
    let lead = int_len % 9;
    put_group(out, &int[..lead]);
    for g in int[lead..].chunks(9) {
        put_group(out, g);
    }
    for g in d.frac_digits().chunks(9) {
        put_group(out, g);
    }
    if out.len() == start {
        return Ok(());
    }
    out[start] ^= 0x80;
    if d.is_negative() {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
    Ok(())
}

fn put_group(out: &mut Vec<u8>, digits: &[u8]) {
    let v = digits.iter().fold(0u32, |v, &d| v * 10 + u32::from(d));
    let n = DIGIT_BYTES[digits.len()];
    out.extend_from_slice(&v.to_be_bytes()[4 - n..]);
}

/// Reads a decimal written by [`pack_decimal`] for the same column.
///
/// # Errors
/// The input is not [`packed_decimal_len`] bytes, or a group holds a number
/// with too many digits.
pub fn unpack_decimal(input: &[u8], precision: u8, scale: u8) -> Result<Decimal> {
    let corrupt = || bad("a packed decimal doesn't decode");
    if scale > precision || input.len() != packed_decimal_len(precision, scale) {
        return Err(corrupt());
    }
    if input.is_empty() {
        return Decimal::from_parts(false, &[], &[]).ok_or_else(corrupt);
    }
    let negative = input[0] & 0x80 == 0;
    let mut raw = input.to_vec();
    if negative {
        for b in &mut raw {
            *b = !*b;
        }
    }
    raw[0] ^= 0x80;
    let int_len = usize::from(precision - scale);
    let mut pos = 0;
    let mut take = |n_digits: usize, digits: &mut Vec<u8>| -> Result<()> {
        if n_digits == 0 {
            return Ok(());
        }
        let n = DIGIT_BYTES[n_digits];
        let mut be = [0u8; 4];
        be[4 - n..].copy_from_slice(&raw[pos..pos + n]);
        pos += n;
        let mut v = u32::from_be_bytes(be);
        let at = digits.len();
        digits.resize(at + n_digits, 0);
        for slot in digits[at..].iter_mut().rev() {
            // A remainder of 10 fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            let digit = (v % 10) as u8;
            *slot = digit;
            v /= 10;
        }
        if v == 0 { Ok(()) } else { Err(corrupt()) }
    };
    let (mut int, mut frac) = (Vec::with_capacity(int_len), Vec::new());
    take(int_len % 9, &mut int)?;
    for _ in 0..int_len / 9 {
        take(9, &mut int)?;
    }
    let frac_len = usize::from(scale);
    for _ in 0..frac_len / 9 {
        take(9, &mut frac)?;
    }
    take(frac_len % 9, &mut frac)?;
    Decimal::from_parts(negative, &int, &frac).ok_or_else(corrupt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::collation::Charset;
    use crate::row::temporal::{Date, DateTime, Time, Timestamp};
    use proptest::prelude::*;

    fn dec(s: &str) -> Decimal {
        Decimal::parse(s).unwrap()
    }

    #[test]
    fn declarations() {
        assert!(ColumnType::int(false).validate().is_ok());
        assert!(
            ColumnType::Int {
                bytes: 5,
                unsigned: false
            }
            .validate()
            .is_err()
        );
        assert!(ColumnType::Bit { bits: 0 }.validate().is_err());
        assert!(ColumnType::Bit { bits: 64 }.validate().is_ok());
        let d = |precision, scale| ColumnType::Decimal { precision, scale };
        assert!(d(65, 38).validate().is_ok());
        assert!(d(66, 0).validate().is_err());
        assert!(d(10, 11).validate().is_err());
        assert!(d(40, 39).validate().is_err());
        assert!(ColumnType::Time { fsp: 7 }.validate().is_err());
        assert!(ColumnType::Set { members: 65 }.validate().is_err());
        assert!(ColumnType::Enum { members: 0 }.validate().is_err());
        assert!(ColumnType::Binary { bytes: 256 }.validate().is_err());
        assert!(!ColumnType::Json.is_key_type());
    }

    #[test]
    fn domains() {
        let ok = |t: ColumnType, v: Value| assert!(t.check(&v).is_ok(), "{t:?} {v:?}");
        let no = |t: ColumnType, v: Value| assert!(t.check(&v).is_err(), "{t:?} {v:?}");
        ok(ColumnType::tinyint(false), Value::Int(-128));
        no(ColumnType::tinyint(false), Value::Int(128));
        no(ColumnType::tinyint(false), Value::UInt(1));
        ok(ColumnType::mediumint(true), Value::UInt(16_777_215));
        no(ColumnType::mediumint(true), Value::UInt(16_777_216));
        ok(ColumnType::bigint(true), Value::UInt(u64::MAX));
        ok(ColumnType::bigint(false), Value::Int(i64::MIN));
        ok(ColumnType::Bit { bits: 3 }, Value::Bit(7));
        no(ColumnType::Bit { bits: 3 }, Value::Bit(8));
        ok(ColumnType::Bit { bits: 64 }, Value::Bit(u64::MAX));
        let d52 = ColumnType::Decimal {
            precision: 5,
            scale: 2,
        };
        ok(d52, Value::Decimal(dec("-999.99")));
        no(d52, Value::Decimal(dec("1000.00")));
        no(d52, Value::Decimal(dec("1.5")));
        no(ColumnType::Float, Value::Float(f32::NAN));
        no(ColumnType::Double, Value::Double(f64::INFINITY));
        ok(ColumnType::Double, Value::Double(-0.0));
        ok(ColumnType::Date, Value::Date(Date::new(0, 0, 0)));
        no(ColumnType::Date, Value::Date(Date::new(10_000, 1, 1)));
        let t = Time::new(true, 1, 0, 0, 500_000).unwrap();
        ok(ColumnType::Time { fsp: 1 }, Value::Time(t));
        no(ColumnType::Time { fsp: 0 }, Value::Time(t));
        let dt = DateTime::new(Date::new(2024, 1, 1), 1, 2, 3, 120_000);
        ok(ColumnType::DateTime { fsp: 2 }, Value::DateTime(dt));
        no(ColumnType::DateTime { fsp: 1 }, Value::DateTime(dt));
        ok(
            ColumnType::Timestamp { fsp: 0 },
            Value::Timestamp(Timestamp::new(u32::MAX, 0)),
        );
        ok(ColumnType::Year, Value::Year(0));
        ok(ColumnType::Year, Value::Year(2155));
        no(ColumnType::Year, Value::Year(1900));
        let c = Collation::Utf8mb4Bin;
        let ch = |chars| ColumnType::Char {
            chars,
            collation: c,
        };
        ok(ch(2), Value::str("\u{e9}\u{1F600}"));
        no(ch(1), Value::str("ab"));
        ok(ColumnType::Binary { bytes: 2 }, Value::bytes(b"a\0"));
        no(ColumnType::Binary { bytes: 2 }, Value::bytes(b"a"));
        ok(ColumnType::VarBinary { bytes: 2 }, Value::bytes(b"a"));
        ok(ColumnType::Enum { members: 2 }, Value::Enum(0));
        no(ColumnType::Enum { members: 2 }, Value::Enum(3));
        ok(ColumnType::Set { members: 2 }, Value::Set(3));
        no(ColumnType::Set { members: 2 }, Value::Set(4));
        ok(ColumnType::Json, Value::Json(Bytes::from_static(b"{}")));
        ok(ColumnType::Json, Value::Null);
    }

    use bytes::Bytes;

    #[test]
    fn packed_decimal_sizes() {
        // The servers' documented examples.
        assert_eq!(packed_decimal_len(18, 9), 8);
        assert_eq!(packed_decimal_len(20, 6), 10);
        assert_eq!(packed_decimal_len(10, 0), 5);
        assert_eq!(packed_decimal_len(65, 30), 30);
        assert_eq!(packed_decimal_len(5, 2), 3);
        assert_eq!(packed_decimal_len(1, 1), 1);
    }

    #[test]
    fn packed_decimal_edges() {
        for (s, p, sc) in [
            ("0.00", 5u8, 2u8),
            ("-999.99", 5, 2),
            ("999.99", 5, 2),
            ("0.5", 1, 1),
            ("-0.5", 1, 1),
            ("1234567890.123456789", 19, 9),
            (
                "-1234567890123456789012345678901234567.8901234567890123456789012345",
                65,
                28,
            ),
        ] {
            let d = dec(s);
            let mut b = Vec::new();
            pack_decimal(&d, p, sc, &mut b).unwrap();
            assert_eq!(b.len(), packed_decimal_len(p, sc), "{s}");
            assert_eq!(unpack_decimal(&b, p, sc).unwrap(), d, "{s}");
        }
        let mut b = Vec::new();
        assert!(pack_decimal(&dec("1000.00"), 5, 2, &mut b).is_err());
        assert!(unpack_decimal(&[0x80], 5, 2).is_err());
        // A group above its digit count is corruption, not a value.
        assert!(unpack_decimal(&[0x80 | 0x7F, 0xFF, 0xFF], 5, 2).is_err());
    }

    fn decimal_of(precision: u8, scale: u8) -> impl Strategy<Value = Decimal> {
        let int = usize::from(precision - scale);
        (
            any::<bool>(),
            proptest::collection::vec(0u8..10, 0..=int),
            proptest::collection::vec(0u8..10, usize::from(scale)),
        )
            .prop_map(|(neg, i, f)| Decimal::from_parts(neg, &i, &f).unwrap())
    }

    proptest! {
        #[test]
        fn packed_decimal_order_and_round_trip(
            (p, s, a, b) in (1u8..=65).prop_flat_map(|p| (Just(p), 0..=p.min(38)))
                .prop_flat_map(|(p, s)| (Just(p), Just(s), decimal_of(p, s), decimal_of(p, s)))
        ) {
            let (mut ea, mut eb) = (Vec::new(), Vec::new());
            pack_decimal(&a, p, s, &mut ea).unwrap();
            pack_decimal(&b, p, s, &mut eb).unwrap();
            prop_assert_eq!(a.cmp_numeric(&b), ea.cmp(&eb));
            prop_assert_eq!(unpack_decimal(&ea, p, s).unwrap(), a);
        }
    }

    #[test]
    fn character_prefixes() {
        let s = "a\u{e9}\u{4e00}\u{1F600}z".as_bytes();
        assert_eq!(Charset::Utf8mb4.char_count(s), 5);
        assert_eq!(Charset::Utf8mb4.char_prefix(s, 2), "a\u{e9}".as_bytes());
        assert_eq!(Charset::Utf8mb4.char_prefix(s, 9), s);
        assert_eq!(Charset::Utf8mb4.char_prefix(s, 0), b"");
        assert_eq!(Charset::Binary.char_count(s), s.len());
        assert_eq!(Charset::Binary.char_prefix(s, 2), &s[..2]);
    }

    #[test]
    fn reference_order() {
        let t = ColumnType::Double;
        assert_eq!(
            t.compare(&Value::Double(-0.0), &Value::Double(0.0)),
            Ordering::Equal
        );
        assert_eq!(
            t.compare(&Value::Null, &Value::Double(-1e300)),
            Ordering::Less
        );
        let c = ColumnType::VarChar {
            chars: 10,
            collation: Collation::Utf8mb4GeneralCi,
        };
        assert_eq!(
            c.compare(&Value::str("ABC"), &Value::str("abc  ")),
            Ordering::Equal
        );
        let d = ColumnType::Decimal {
            precision: 5,
            scale: 2,
        };
        assert_eq!(
            d.compare(&Value::Decimal(dec("-1.50")), &Value::Decimal(dec("-1.05"))),
            Ordering::Less
        );
    }
}
