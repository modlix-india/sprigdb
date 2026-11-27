//! Key encodings of typed column values: the user-key part of primary,
//! secondary and unique index entries.
//!
//! One column's bytes, ascending:
//!
//! | Type | Bytes |
//! |---|---|
//! | nullable column | a marker first: `00` for `NULL` (nothing follows), `01` otherwise |
//! | signed integers | big-endian in the column's width, sign bit flipped |
//! | unsigned integers, `BIT(n)` | big-endian, in the column's width or `(n + 7) / 8` bytes |
//! | `DECIMAL` | [`memcmp::put_decimal`]: the same bytes for `1.0` and `1.00` |
//! | `FLOAT`, `DOUBLE` | [`memcmp::put_f32`], [`memcmp::put_f64`] |
//! | `DATE`, `TIME`, `DATETIME`, `TIMESTAMP` | the packed forms of [`crate::row::temporal`] |
//! | `YEAR` | one byte: 0, or the year minus 1900 |
//! | `ENUM` | the member index, 2 bytes big-endian |
//! | `SET` | the bit mask, 8 bytes big-endian |
//! | `CHAR`, `VARCHAR`, `TEXT` | the collation's key ([`Collation::encode_key`]) |
//! | `BINARY`, `VARBINARY`, `BLOB` | the bytes, `00` escaped, `00 00` terminated |
//!
//! A descending column has every byte inverted, marker included, so `NULL`
//! sorts last. An index prefix (`col(20)`) encodes the first 20 characters
//! (bytes, for binary strings). Every encoding ends itself, so columns
//! concatenate into a composite key whose byte order is the order of the
//! column values taken left to right.
//!
//! Decoding gives back numbers, temporals and binary strings. A character
//! string is stored as weights, which don't map back to text: decoding
//! steps over it and reports `None`; the text is in the row.

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use super::collation::{Charset, Collation};
use super::memcmp;
use crate::row::decimal::Decimal;
use crate::row::temporal::{
    pack_date, pack_datetime, pack_time, pack_timestamp, unpack_date, unpack_datetime, unpack_time,
    unpack_timestamp,
};
use crate::row::{ColumnType, Value};

/// One column of a key: its type and how the index declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyColumn {
    /// The column's type.
    pub ty: ColumnType,
    /// Whether the column can hold `NULL`. A nullable column's encoding
    /// starts with the `NULL` marker; a `NOT NULL` column's has none.
    pub nullable: bool,
    /// Whether the index orders the column descending.
    pub desc: bool,
    /// The index prefix length, in characters for character strings and in
    /// bytes for binary strings; `None` for the whole value.
    pub prefix: Option<u32>,
}

impl KeyColumn {
    /// A `NOT NULL`, ascending, whole-value key column.
    #[must_use]
    pub const fn new(ty: ColumnType) -> Self {
        Self {
            ty,
            nullable: false,
            desc: false,
            prefix: None,
        }
    }

    /// The same column, nullable.
    #[must_use]
    pub const fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    /// The same column, descending.
    #[must_use]
    pub const fn desc(mut self) -> Self {
        self.desc = true;
        self
    }

    /// The same column, with an index prefix of `len` characters or bytes.
    #[must_use]
    pub const fn prefix(mut self, len: u32) -> Self {
        self.prefix = Some(len);
        self
    }
}

fn mismatch(col: &KeyColumn, v: &Value) -> SprigError {
    SprigError::internal(format!("{} can't be a key value of {:?}", v.kind(), col.ty))
}

fn malformed() -> SprigError {
    SprigError::internal("typed key is malformed")
}

/// Appends the key bytes of `v` for column `col`.
///
/// # Errors
/// `v` isn't a value of the column's type, is `NULL` in a `NOT NULL` column,
/// is outside the type's range, or the type can't be a key (`JSON`).
pub fn encode_column(out: &mut Vec<u8>, col: &KeyColumn, v: &Value) -> Result<()> {
    let start = out.len();
    if col.nullable {
        memcmp::put_null_marker(out, v.is_null());
    } else if v.is_null() {
        return Err(mismatch(col, v));
    }
    if !v.is_null()
        && let Err(e) = encode_value(out, col, v)
    {
        out.truncate(start);
        return Err(e);
    }
    if col.desc {
        memcmp::invert_from(out, start);
    }
    Ok(())
}

fn put_be(out: &mut Vec<u8>, v: u64, width: usize) {
    out.extend_from_slice(&v.to_be_bytes()[8 - width..]);
}

fn encode_value(out: &mut Vec<u8>, col: &KeyColumn, v: &Value) -> Result<()> {
    let prefix = |charset: Charset, s: &'_ Bytes| -> Bytes {
        match col.prefix {
            Some(n) => s.slice(..charset.char_prefix(s, n as usize).len()),
            None => s.clone(),
        }
    };
    match (col.ty, v) {
        (
            ColumnType::Int { .. }
            | ColumnType::Bit { .. }
            | ColumnType::Decimal { .. }
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::Date
            | ColumnType::Time { .. }
            | ColumnType::DateTime { .. }
            | ColumnType::Timestamp { .. }
            | ColumnType::Year
            | ColumnType::Enum { .. }
            | ColumnType::Set { .. },
            _,
        ) => {
            // Cheap for these types, and a value outside the range would
            // lose its high bytes and sort in the wrong place.
            col.ty.check(v)?;
            encode_scalar(out, col.ty, v);
            Ok(())
        }
        (
            ColumnType::Char { collation, .. }
            | ColumnType::VarChar { collation, .. }
            | ColumnType::Text { collation, .. },
            Value::Str(s),
        ) => {
            collation.encode_key(&prefix(collation.charset(), s), out);
            Ok(())
        }
        (
            ColumnType::Binary { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. },
            Value::Bytes(b),
        ) => {
            memcmp::put_bytes(out, &prefix(Charset::Binary, b));
            Ok(())
        }
        // Fixed width, and the stored bytes already order as the values do.
        (ColumnType::Fbt(_), Value::Bytes(b)) => {
            col.ty.check(v)?;
            out.extend_from_slice(b);
            Ok(())
        }
        _ => Err(mismatch(col, v)),
    }
}

/// The fixed-width encodings. The caller has checked `v` against `ty`.
fn encode_scalar(out: &mut Vec<u8>, ty: ColumnType, v: &Value) {
    match (ty, v) {
        (ColumnType::Int { bytes, .. }, Value::Int(i)) => {
            memcmp::put_int(out, *i, usize::from(bytes));
        }
        (ColumnType::Int { bytes, .. }, Value::UInt(u)) => {
            memcmp::put_uint(out, *u, usize::from(bytes));
        }
        (ColumnType::Bit { bits }, Value::Bit(b)) => {
            put_be(out, *b, usize::from(bits).div_ceil(8));
        }
        (_, Value::Decimal(d)) => {
            memcmp::put_decimal(out, d.is_negative(), d.int_digits(), d.frac_digits());
        }
        (_, Value::Float(f)) => memcmp::put_f32(out, *f),
        (_, Value::Double(f)) => memcmp::put_f64(out, *f),
        (_, Value::Date(d)) => pack_date(*d, out),
        (ColumnType::Time { fsp }, Value::Time(t)) => pack_time(*t, fsp, out),
        (ColumnType::DateTime { fsp }, Value::DateTime(d)) => pack_datetime(*d, fsp, out),
        (ColumnType::Timestamp { fsp }, Value::Timestamp(t)) => pack_timestamp(*t, fsp, out),
        (_, Value::Year(y)) => {
            // 0, or 1901 to 2155: the difference fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            out.push(y.saturating_sub(1900) as u8);
        }
        (_, Value::Enum(i)) => out.extend_from_slice(&i.to_be_bytes()),
        (_, Value::Set(m)) => out.extend_from_slice(&m.to_be_bytes()),
        // `check` passed, so the pair is one of the above.
        _ => {}
    }
}

/// Appends a composite key: each value under its column, left to right.
///
/// # Errors
/// The counts differ, or a value fails [`encode_column`]. Nothing is
/// appended then.
pub fn encode_key(out: &mut Vec<u8>, cols: &[KeyColumn], vals: &[Value]) -> Result<()> {
    if cols.len() != vals.len() {
        return Err(SprigError::internal(format!(
            "a key of {} columns was given {} values",
            cols.len(),
            vals.len()
        )));
    }
    let start = out.len();
    for (col, v) in cols.iter().zip(vals) {
        if let Err(e) = encode_column(out, col, v) {
            out.truncate(start);
            return Err(e);
        }
    }
    Ok(())
}

/// Reads one column from the front of a key and returns it with the rest of
/// the key. The value is `None` for a character string, whose weights don't
/// map back to text (a `binary`-collated one does come back). An index
/// prefix column gives back the prefix.
///
/// # Errors
/// The input isn't a key of this column.
pub fn decode_column<'a>(input: &'a [u8], col: &KeyColumn) -> Result<(Option<Value>, &'a [u8])> {
    if !col.desc {
        return decode_asc(input, col);
    }
    // A descending column is its ascending encoding inverted. Its length
    // isn't known before it is read, so the rest of the key is inverted
    // with it and only the bytes it used are consumed.
    let inverted: Vec<u8> = input.iter().map(|b| !b).collect();
    let (v, rest) = decode_asc(&inverted, col)?;
    Ok((v, &input[input.len() - rest.len()..]))
}

#[allow(clippy::too_many_lines)] // one arm per column type
fn decode_asc<'a>(input: &'a [u8], col: &KeyColumn) -> Result<(Option<Value>, &'a [u8])> {
    let mut input = input;
    if col.nullable {
        let (&marker, rest) = input.split_first().ok_or_else(malformed)?;
        match marker {
            0 => return Ok((Some(Value::Null), rest)),
            1 => input = rest,
            _ => return Err(malformed()),
        }
    }
    let take = |n: usize| input.split_at_checked(n).ok_or_else(malformed);
    let (value, rest) = match col.ty {
        ColumnType::Int {
            bytes,
            unsigned: false,
        } => {
            let (v, rest) = memcmp::get_int(input, usize::from(bytes))?;
            (Value::Int(v), rest)
        }
        ColumnType::Int {
            bytes,
            unsigned: true,
        } => {
            let (v, rest) = memcmp::get_uint(input, usize::from(bytes))?;
            (Value::UInt(v), rest)
        }
        ColumnType::Bit { bits } => {
            let (v, rest) = memcmp::get_uint(input, usize::from(bits).div_ceil(8))?;
            (Value::Bit(v), rest)
        }
        ColumnType::Decimal { scale, .. } => {
            let ((negative, int, frac), rest) = memcmp::get_decimal(input)?;
            let d = Decimal::from_parts(negative, &int, &frac)
                .and_then(|d| d.with_scale(scale))
                .ok_or_else(malformed)?;
            (Value::Decimal(d), rest)
        }
        ColumnType::Float => {
            let (v, rest) = memcmp::get_f32(input)?;
            (Value::Float(v), rest)
        }
        ColumnType::Double => {
            let (v, rest) = memcmp::get_f64(input)?;
            (Value::Double(v), rest)
        }
        ColumnType::Date => {
            let (v, rest) = unpack_date(input).ok_or_else(malformed)?;
            (Value::Date(v), rest)
        }
        ColumnType::Time { fsp } => {
            let (v, rest) = unpack_time(input, fsp).ok_or_else(malformed)?;
            (Value::Time(v), rest)
        }
        ColumnType::DateTime { fsp } => {
            let (v, rest) = unpack_datetime(input, fsp).ok_or_else(malformed)?;
            (Value::DateTime(v), rest)
        }
        ColumnType::Timestamp { fsp } => {
            let (v, rest) = unpack_timestamp(input, fsp).ok_or_else(malformed)?;
            (Value::Timestamp(v), rest)
        }
        ColumnType::Year => {
            let (head, rest) = take(1)?;
            let y = match head[0] {
                0 => 0,
                n => 1900 + u16::from(n),
            };
            (Value::Year(y), rest)
        }
        ColumnType::Enum { .. } => {
            let (head, rest) = take(2)?;
            (Value::Enum(u16::from_be_bytes([head[0], head[1]])), rest)
        }
        ColumnType::Set { .. } => {
            let (v, rest) = memcmp::get_uint(input, 8)?;
            (Value::Set(v), rest)
        }
        ColumnType::Char { collation, .. }
        | ColumnType::VarChar { collation, .. }
        | ColumnType::Text { collation, .. } => {
            if collation == Collation::Binary {
                let (b, rest) = memcmp::get_bytes(input)?;
                (Value::Str(Bytes::from(b)), rest)
            } else {
                return Ok((None, collation.skip_key(input)?));
            }
        }
        ColumnType::Binary { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. } => {
            let (b, rest) = memcmp::get_bytes(input)?;
            (Value::Bytes(Bytes::from(b)), rest)
        }
        ColumnType::Fbt(f) => {
            let (head, rest) = take(f.bytes())?;
            (Value::Bytes(Bytes::copy_from_slice(head)), rest)
        }
        ColumnType::Json => return Err(malformed()),
    };
    // Bytes of the right shape can still spell a value outside the column
    // (an ENUM index past its members, a NaN): no key the encoder writes.
    if col.ty.fixed_width().is_some()
        && !matches!(col.ty, ColumnType::Binary { .. })
        && col.ty.check(&value).is_err()
    {
        return Err(malformed());
    }
    Ok((Some(value), rest))
}

/// Reads the columns of a composite key, left to right, and returns them
/// with what follows (for a secondary index entry, list the index columns
/// and then the primary key's).
///
/// # Errors
/// The input isn't a key of these columns.
pub fn decode_key<'a>(
    mut input: &'a [u8],
    cols: &[KeyColumn],
) -> Result<(Vec<Option<Value>>, &'a [u8])> {
    let mut out = Vec::with_capacity(cols.len());
    for col in cols {
        let (v, rest) = decode_column(input, col)?;
        out.push(v);
        input = rest;
    }
    Ok((out, input))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::temporal::{Date, DateTime, Time, Timestamp};

    fn key(col: &KeyColumn, v: &Value) -> Vec<u8> {
        let mut o = Vec::new();
        encode_column(&mut o, col, v).unwrap();
        o
    }

    #[test]
    fn bytes_per_type() {
        let k = |ty: ColumnType, v: Value| key(&KeyColumn::new(ty), &v);
        assert_eq!(k(ColumnType::tinyint(false), Value::Int(-1)), [0x7F]);
        assert_eq!(
            k(ColumnType::mediumint(true), Value::UInt(0x01_02_03)),
            [1, 2, 3]
        );
        assert_eq!(
            k(ColumnType::Bit { bits: 12 }, Value::Bit(0xABC)),
            [0x0A, 0xBC]
        );
        assert_eq!(
            k(ColumnType::Bit { bits: 40 }, Value::Bit(1)),
            [0, 0, 0, 0, 1]
        );
        assert_eq!(k(ColumnType::Year, Value::Year(0)), [0]);
        assert_eq!(k(ColumnType::Year, Value::Year(1901)), [1]);
        assert_eq!(k(ColumnType::Year, Value::Year(2155)), [255]);
        assert_eq!(
            k(ColumnType::Enum { members: 300 }, Value::Enum(258)),
            [1, 2]
        );
        assert_eq!(
            k(ColumnType::Set { members: 9 }, Value::Set(0x101)),
            [0, 0, 0, 0, 0, 0, 1, 1]
        );
        assert_eq!(
            k(ColumnType::Binary { bytes: 3 }, Value::bytes(b"a\0\0")),
            [b'a', 0, 0xFF, 0, 0xFF, 0, 0]
        );
        assert_eq!(
            k(ColumnType::Date, Value::Date(Date::new(1, 1, 1))).len(),
            3
        );
        assert_eq!(
            k(
                ColumnType::DateTime { fsp: 4 },
                Value::DateTime(DateTime::default())
            )
            .len(),
            7
        );
        assert_eq!(
            k(
                ColumnType::Timestamp { fsp: 0 },
                Value::Timestamp(Timestamp::new(1, 0))
            ),
            [0, 0, 0, 1]
        );
        assert_eq!(
            k(ColumnType::Time { fsp: 0 }, Value::Time(Time::default())),
            [0x80, 0, 0]
        );
    }

    #[test]
    fn nulls_and_direction() {
        let asc = KeyColumn::new(ColumnType::int(false)).nullable();
        let desc = asc.desc();
        assert_eq!(key(&asc, &Value::Null), [0]);
        assert_eq!(key(&asc, &Value::Int(0)), [1, 0x80, 0, 0, 0]);
        assert_eq!(key(&desc, &Value::Null), [0xFF]);
        assert_eq!(key(&desc, &Value::Int(0)), [0xFE, 0x7F, 0xFF, 0xFF, 0xFF]);
        // NULL first ascending, last descending.
        assert!(key(&asc, &Value::Null) < key(&asc, &Value::Int(i64::from(i32::MIN))));
        assert!(key(&desc, &Value::Null) > key(&desc, &Value::Int(i64::from(i32::MIN))));
        assert!(key(&desc, &Value::Int(5)) < key(&desc, &Value::Int(4)));
        for (col, v) in [
            (asc, Value::Null),
            (desc, Value::Null),
            (desc, Value::Int(-7)),
        ] {
            let mut k = key(&col, &v);
            k.push(0x55);
            assert_eq!(decode_column(&k, &col).unwrap(), (Some(v), &[0x55][..]));
        }
    }

    #[test]
    fn refusals_leave_the_buffer_alone() {
        let mut out = vec![9];
        let not_null = KeyColumn::new(ColumnType::int(false));
        assert!(encode_column(&mut out, &not_null, &Value::Null).is_err());
        assert!(encode_column(&mut out, &not_null, &Value::UInt(1)).is_err());
        assert!(encode_column(&mut out, &not_null, &Value::Int(1 << 40)).is_err());
        let json = KeyColumn::new(ColumnType::Json).nullable();
        assert!(encode_column(&mut out, &json, &Value::Json(Bytes::new())).is_err());
        let cols = [not_null, not_null];
        assert!(encode_key(&mut out, &cols, &[Value::Int(1)]).is_err());
        assert!(encode_key(&mut out, &cols, &[Value::Int(1), Value::Null]).is_err());
        assert_eq!(out, [9]);
    }

    #[test]
    fn prefixes() {
        let c = Collation::Utf8mb4GeneralCi;
        let whole = KeyColumn::new(ColumnType::VarChar {
            chars: 20,
            collation: c,
        });
        let two = whole.prefix(2);
        assert_eq!(
            key(&two, &Value::str("\u{e9}tage")),
            key(&whole, &Value::str("ET"))
        );
        assert_eq!(key(&two, &Value::str("a")), key(&whole, &Value::str("a")));
        let bin = KeyColumn::new(ColumnType::VarBinary { bytes: 20 }).prefix(3);
        let k = key(&bin, &Value::bytes(b"abcdef"));
        assert_eq!(
            decode_column(&k, &bin).unwrap(),
            (Some(Value::bytes(b"abc")), &[][..])
        );
    }

    #[test]
    fn composite_keys_decode_past_strings() {
        let cols = [
            KeyColumn::new(ColumnType::VarChar {
                chars: 20,
                collation: Collation::Utf8mb4Uca1400AiCi,
            })
            .nullable()
            .desc(),
            KeyColumn::new(ColumnType::Char {
                chars: 5,
                collation: Collation::Utf8mb4NopadBin,
            }),
            KeyColumn::new(ColumnType::Text {
                size: crate::row::LobSize::Normal,
                collation: Collation::Binary,
            })
            .prefix(4),
            KeyColumn::new(ColumnType::Decimal {
                precision: 10,
                scale: 3,
            })
            .desc(),
            KeyColumn::new(ColumnType::bigint(true)),
        ];
        let vals = [
            Value::str("Stra\u{df}e \u{1F600}"),
            Value::str("a\0b"),
            Value::str("x\0yzzz"),
            Value::Decimal(Decimal::parse("-12.500").unwrap()),
            Value::UInt(u64::MAX),
        ];
        let mut k = Vec::new();
        encode_key(&mut k, &cols, &vals).unwrap();
        k.extend_from_slice(b"pk");
        let (got, rest) = decode_key(&k, &cols).unwrap();
        assert_eq!(rest, b"pk");
        assert_eq!(
            got,
            [
                None,
                None,
                Some(Value::str("x\0yz")),
                Some(vals[3].clone()),
                Some(vals[4].clone())
            ]
        );
        // A truncated key is an error, not a panic.
        for cut in 0..k.len() - 2 {
            assert!(decode_key(&k[..cut], &cols).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn fixed_binary_types_are_their_stored_bytes() {
        use crate::row::Fbt;
        let col = KeyColumn::new(ColumnType::Fbt(Fbt::Uuid)).nullable();
        let a = Fbt::Uuid
            .parse(b"00000000-0000-1000-8000-000000000003")
            .unwrap();
        let b = Fbt::Uuid
            .parse(b"10000000-0000-0000-0000-000000000002")
            .unwrap();
        let (ka, kb) = (key(&col, &Value::bytes(&a)), key(&col, &Value::bytes(&b)));
        assert!(ka < kb);
        assert!(key(&col, &Value::Null) < ka);
        let (got, rest) = decode_column(&ka, &col).unwrap();
        assert_eq!((got, rest), (Some(Value::bytes(&a)), &[][..]));
        let i4 = KeyColumn::new(ColumnType::Fbt(Fbt::Inet4));
        assert_eq!(key(&i4, &Value::bytes(&[1, 2, 3, 4])), [1, 2, 3, 4]);
        let mut out = Vec::new();
        assert!(encode_column(&mut out, &i4, &Value::bytes(&[1, 2, 3])).is_err());
    }
}
