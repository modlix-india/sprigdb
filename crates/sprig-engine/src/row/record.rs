//! The row record: the value a primary key points at.
//!
//! ```text
//! format u8 (1) | schema_version varint | flags u8 | [expires_at u64 LE]
//! | null bitmap | fixed columns | var offsets | var bytes
//! ```
//!
//! - **`schema_version`** names the column list the row was written under
//!   (LEB128). A reader that knows a newer list reads the row through
//!   [`decode_as`]: added columns show their default, dropped ones are
//!   skipped, widened ones are converted. So adding or dropping a column and
//!   widening a type change the catalog only.
//! - **`flags`**: bit 0, the row has an expiry and `expires_at` follows;
//!   bit 1, the var offsets are `u32` instead of `u16`.
//! - **Null bitmap**: one bit per column of the schema, in column order,
//!   least significant bit first; set means `NULL`. A `NULL` column takes no
//!   other bytes.
//! - **Fixed columns**: the non-`NULL` fixed-width columns in column order,
//!   each in its type's width: integers little-endian; `BIT` big-endian;
//!   `DECIMAL` packed nine digits to four bytes; `FLOAT` and `DOUBLE` as
//!   IEEE 754 little-endian (`-0.0` keeps its sign); temporals in their
//!   packed forms; `YEAR` as one byte; `BINARY(n)` as its `n` bytes; `ENUM`
//!   as `u16` and `SET` as `u64`, little-endian.
//! - **Var offsets**: for each non-`NULL` variable-length column in column
//!   order, where its bytes end, counted from the start of the var bytes.
//!   `u16` while the var bytes total at most 65,535, else `u32`.
//! - **Var bytes**: the strings, byte strings and JSON documents, back to
//!   back.
//!
//! Old rows are rewritten lazily: [`is_stale`] says when a row has fallen
//! far enough behind, and [`upgrade`] re-encodes it under the newer schema.
//!
//! One column is read without touching the others: its place follows from
//! the bitmap and the schema ([`decode_column`]). Decoding from [`Bytes`]
//! doesn't copy string values; they are slices of the row.

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use super::temporal::{
    pack_date, pack_datetime, pack_time, pack_timestamp, unpack_date, unpack_datetime, unpack_time,
    unpack_timestamp,
};
use super::types::{ColumnType, pack_decimal, unpack_decimal};
use super::value::Value;

/// The record format this module writes.
pub const FORMAT: u8 = 1;

const FLAG_EXPIRY: u8 = 0x01;
const FLAG_WIDE_OFFSETS: u8 = 0x02;
const KNOWN_FLAGS: u8 = FLAG_EXPIRY | FLAG_WIDE_OFFSETS;

fn corrupt() -> SprigError {
    SprigError::internal("a stored row doesn't decode")
}

fn bad(msg: impl Into<String>) -> SprigError {
    SprigError::internal(msg)
}

/// One column of a row schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// The column's identity across schema versions: it stays the same when
    /// the column is renamed, moved or widened, and is never reused after a
    /// drop.
    pub id: u32,
    /// The column's type in this version.
    pub ty: ColumnType,
    /// Whether the column can hold `NULL`.
    pub nullable: bool,
    /// What a row written before the column existed shows for it: the
    /// `DEFAULT` the column was added with. `NULL` for a nullable column
    /// added without one.
    pub default: Value,
}

impl Column {
    /// A nullable column whose missing value is `NULL`.
    #[must_use]
    pub const fn new(id: u32, ty: ColumnType) -> Self {
        Self {
            id,
            ty,
            nullable: true,
            default: Value::Null,
        }
    }

    /// The same column, `NOT NULL`, showing `default` in rows written before
    /// it existed.
    #[must_use]
    pub fn not_null(mut self, default: Value) -> Self {
        self.nullable = false;
        self.default = default;
        self
    }

    /// The same column with another missing-value default.
    #[must_use]
    pub fn with_default(mut self, default: Value) -> Self {
        self.default = default;
        self
    }
}

/// One version of a table's column list.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSchema {
    version: u32,
    columns: Vec<Column>,
    /// Per column: its fixed width, or `None` if variable-length.
    widths: Vec<Option<usize>>,
}

impl RowSchema {
    /// A schema version from its columns.
    ///
    /// # Errors
    /// An invalid type, a repeated column id, or a default the column can't
    /// hold (`NULL` in a `NOT NULL` column included).
    pub fn new(version: u32, columns: Vec<Column>) -> Result<Self> {
        let mut ids: Vec<u32> = columns.iter().map(|c| c.id).collect();
        ids.sort_unstable();
        if ids.windows(2).any(|w| w[0] == w[1]) {
            return Err(bad("a row schema repeats a column id"));
        }
        for c in &columns {
            c.ty.validate()?;
            c.ty.check(&c.default)?;
            if !c.nullable && c.default.is_null() {
                return Err(bad(format!(
                    "NOT NULL column {} has no value for older rows",
                    c.id
                )));
            }
        }
        let widths = columns.iter().map(|c| c.ty.fixed_width()).collect();
        Ok(Self {
            version,
            columns,
            widths,
        })
    }

    /// The version number rows written under this schema carry.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The columns, in order.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    fn position_of(&self, id: u32) -> Option<usize> {
        self.columns.iter().position(|c| c.id == id)
    }
}

/// What a row says about itself before its columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowHeader {
    /// The schema version the row was written under.
    pub schema_version: u32,
    /// When the row expires, if it does: the engine's timestamp, opaque here.
    pub expires_at: Option<u64>,
}

/// Appends the record of `values` (one per column of `schema`).
///
/// # Errors
/// The count differs from the schema's, a value isn't storable in its
/// column, or a `NOT NULL` column is given `NULL`. Nothing is appended then.
pub fn encode(
    schema: &RowSchema,
    values: &[Value],
    expires_at: Option<u64>,
    out: &mut Vec<u8>,
) -> Result<()> {
    if values.len() != schema.columns.len() {
        return Err(bad(format!(
            "a row of {} columns was given {} values",
            schema.columns.len(),
            values.len()
        )));
    }
    let mut var_total = 0usize;
    for (c, v) in schema.columns.iter().zip(values) {
        c.ty.check(v)?;
        if v.is_null() && !c.nullable {
            return Err(bad(format!("NULL for NOT NULL column {}", c.id)));
        }
        if let Value::Str(b) | Value::Bytes(b) | Value::Json(b) = v
            && c.ty.fixed_width().is_none()
        {
            var_total += b.len();
        }
    }
    let wide = var_total > usize::from(u16::MAX);
    if u32::try_from(var_total).is_err() {
        return Err(bad("a row's variable-length columns pass 4 GiB"));
    }

    let row_start = out.len();
    out.push(FORMAT);
    put_varint(out, schema.version);
    let mut flags = 0;
    if expires_at.is_some() {
        flags |= FLAG_EXPIRY;
    }
    if wide {
        flags |= FLAG_WIDE_OFFSETS;
    }
    out.push(flags);
    if let Some(at) = expires_at {
        out.extend_from_slice(&at.to_le_bytes());
    }
    let bitmap_at = out.len();
    out.resize(bitmap_at + schema.columns.len().div_ceil(8), 0);
    for (i, v) in values.iter().enumerate() {
        if v.is_null() {
            out[bitmap_at + i / 8] |= 1 << (i % 8);
        }
    }
    for ((c, width), v) in schema.columns.iter().zip(&schema.widths).zip(values) {
        if width.is_some()
            && !v.is_null()
            && let Err(e) = put_fixed(out, c.ty, v)
        {
            out.truncate(row_start);
            return Err(e);
        }
    }
    let mut end = 0usize;
    for (width, v) in schema.widths.iter().zip(values) {
        if let (None, Value::Str(b) | Value::Bytes(b) | Value::Json(b)) = (width, v) {
            end += b.len();
            // Both fit: `wide` and the 4 GiB check above.
            #[allow(clippy::cast_possible_truncation)]
            if wide {
                out.extend_from_slice(&(end as u32).to_le_bytes());
            } else {
                out.extend_from_slice(&(end as u16).to_le_bytes());
            }
        }
    }
    for (width, v) in schema.widths.iter().zip(values) {
        if let (None, Value::Str(b) | Value::Bytes(b) | Value::Json(b)) = (width, v) {
            out.extend_from_slice(b);
        }
    }
    Ok(())
}

fn put_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        // The low seven bits.
        #[allow(clippy::cast_possible_truncation)]
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    #[allow(clippy::cast_possible_truncation)]
    out.push(v as u8);
}

fn get_varint(input: &[u8]) -> Option<(u32, &[u8])> {
    let mut v = 0u32;
    for (i, &b) in input.iter().enumerate().take(5) {
        let part = u32::from(b & 0x7F);
        if i == 4 && part > 0x0F {
            return None;
        }
        v |= part << (7 * i);
        if b & 0x80 == 0 {
            return Some((v, &input[i + 1..]));
        }
    }
    None
}

/// The fixed-width bytes of a checked, non-`NULL` value.
fn put_fixed(out: &mut Vec<u8>, ty: ColumnType, v: &Value) -> Result<()> {
    match (ty, v) {
        (ColumnType::Int { bytes, .. }, Value::Int(i)) => {
            out.extend_from_slice(&i.to_le_bytes()[..usize::from(bytes)]);
        }
        (ColumnType::Int { bytes, .. }, Value::UInt(u)) => {
            out.extend_from_slice(&u.to_le_bytes()[..usize::from(bytes)]);
        }
        (ColumnType::Bit { bits }, Value::Bit(b)) => {
            out.extend_from_slice(&b.to_be_bytes()[8 - usize::from(bits).div_ceil(8)..]);
        }
        (ColumnType::Decimal { precision, scale }, Value::Decimal(d)) => {
            pack_decimal(d, precision, scale, out)?;
        }
        (_, Value::Float(f)) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
        (_, Value::Double(f)) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
        (_, Value::Date(d)) => pack_date(*d, out),
        (ColumnType::Time { fsp }, Value::Time(t)) => pack_time(*t, fsp, out),
        (ColumnType::DateTime { fsp }, Value::DateTime(d)) => pack_datetime(*d, fsp, out),
        (ColumnType::Timestamp { fsp }, Value::Timestamp(t)) => pack_timestamp(*t, fsp, out),
        (_, Value::Year(y)) => {
            // 0, or 1901 to 2155: the difference fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            out.push(y.saturating_sub(1900) as u8);
        }
        (ColumnType::Binary { .. } | ColumnType::Fbt(_), Value::Bytes(b)) => {
            out.extend_from_slice(b);
        }
        (_, Value::Enum(i)) => out.extend_from_slice(&i.to_le_bytes()),
        (_, Value::Set(m)) => out.extend_from_slice(&m.to_le_bytes()),
        _ => return Err(bad("a fixed-width column was given another type's value")),
    }
    Ok(())
}

fn get_fixed(ty: ColumnType, b: &[u8], row: &Bytes) -> Result<Value> {
    let le = |signed: bool| -> u64 {
        let fill = if signed && b.last().is_some_and(|&x| x & 0x80 != 0) {
            0xFF
        } else {
            0
        };
        let mut raw = [fill; 8];
        raw[..b.len()].copy_from_slice(b);
        u64::from_le_bytes(raw)
    };
    Ok(match ty {
        ColumnType::Int {
            unsigned: false, ..
        } => {
            // Sign-extended above, so the bits are the value.
            #[allow(clippy::cast_possible_wrap)]
            let v = le(true) as i64;
            Value::Int(v)
        }
        ColumnType::Int { unsigned: true, .. } => Value::UInt(le(false)),
        ColumnType::Bit { .. } => {
            let mut raw = [0u8; 8];
            raw[8 - b.len()..].copy_from_slice(b);
            Value::Bit(u64::from_be_bytes(raw))
        }
        ColumnType::Decimal { precision, scale } => {
            Value::Decimal(unpack_decimal(b, precision, scale)?)
        }
        ColumnType::Float => Value::Float(f32::from_bits(u32::from_le_bytes(
            b.try_into().map_err(|_| corrupt())?,
        ))),
        ColumnType::Double => Value::Double(f64::from_bits(le(false))),
        ColumnType::Date => Value::Date(unpack_date(b).ok_or_else(corrupt)?.0),
        ColumnType::Time { fsp } => Value::Time(unpack_time(b, fsp).ok_or_else(corrupt)?.0),
        ColumnType::DateTime { fsp } => {
            Value::DateTime(unpack_datetime(b, fsp).ok_or_else(corrupt)?.0)
        }
        ColumnType::Timestamp { fsp } => {
            Value::Timestamp(unpack_timestamp(b, fsp).ok_or_else(corrupt)?.0)
        }
        ColumnType::Year => Value::Year(match b[0] {
            0 => 0,
            n => 1900 + u16::from(n),
        }),
        ColumnType::Binary { .. } | ColumnType::Fbt(_) => Value::Bytes(row.slice_ref(b)),
        ColumnType::Enum { .. } => Value::Enum(u16::from_le_bytes([b[0], b[1]])),
        ColumnType::Set { .. } => Value::Set(le(false)),
        _ => return Err(corrupt()),
    })
}

fn var_value(ty: ColumnType, b: Bytes) -> Result<Value> {
    Ok(match ty {
        ColumnType::Char { .. } | ColumnType::VarChar { .. } | ColumnType::Text { .. } => {
            Value::Str(b)
        }
        ColumnType::VarBinary { .. } | ColumnType::Blob { .. } => Value::Bytes(b),
        ColumnType::Json => Value::Json(b),
        _ => return Err(corrupt()),
    })
}

/// A row split into its parts, columns not yet decoded.
struct Parsed<'a> {
    bitmap: &'a [u8],
    /// The fixed columns, the var offsets and the var bytes.
    body: &'a [u8],
    wide: bool,
}

impl Parsed<'_> {
    fn is_null(&self, i: usize) -> bool {
        self.bitmap[i / 8] & (1 << (i % 8)) != 0
    }
}

fn parse<'a>(schema: &RowSchema, row: &'a [u8]) -> Result<Parsed<'a>> {
    let (&format, rest) = row.split_first().ok_or_else(corrupt)?;
    if format != FORMAT {
        return Err(bad(format!("row format {format} is not known")));
    }
    let (schema_version, rest) = get_varint(rest).ok_or_else(corrupt)?;
    if schema_version != schema.version {
        return Err(bad(format!(
            "a row of schema version {schema_version} was read as version {}",
            schema.version
        )));
    }
    let (&flags, mut rest) = rest.split_first().ok_or_else(corrupt)?;
    if flags & !KNOWN_FLAGS != 0 {
        return Err(corrupt());
    }
    if flags & FLAG_EXPIRY != 0 {
        rest = rest.get(8..).ok_or_else(corrupt)?;
    }
    let (bitmap, body) = rest
        .split_at_checked(schema.columns.len().div_ceil(8))
        .ok_or_else(corrupt)?;
    Ok(Parsed {
        bitmap,
        body,
        wide: flags & FLAG_WIDE_OFFSETS != 0,
    })
}

/// Reads a row's header without knowing its schema: which version wrote it
/// and when it expires.
///
/// # Errors
/// The row is truncated or of an unknown format.
pub fn peek_header(row: &[u8]) -> Result<RowHeader> {
    let (&format, rest) = row.split_first().ok_or_else(corrupt)?;
    if format != FORMAT {
        return Err(bad(format!("row format {format} is not known")));
    }
    let (schema_version, rest) = get_varint(rest).ok_or_else(corrupt)?;
    let (&flags, rest) = rest.split_first().ok_or_else(corrupt)?;
    let expires_at = if flags & FLAG_EXPIRY == 0 {
        None
    } else {
        let head = rest.get(..8).ok_or_else(corrupt)?;
        Some(u64::from_le_bytes(head.try_into().map_err(|_| corrupt())?))
    };
    Ok(RowHeader {
        schema_version,
        expires_at,
    })
}

/// Where the variable-length part starts and how many columns it has.
fn var_layout(schema: &RowSchema, p: &Parsed<'_>) -> (usize, usize) {
    let (mut fixed, mut vars) = (0, 0);
    for (i, w) in schema.widths.iter().enumerate() {
        if !p.is_null(i) {
            match w {
                Some(w) => fixed += w,
                None => vars += 1,
            }
        }
    }
    (fixed, vars)
}

fn offset_at(p: &Parsed<'_>, offsets: &[u8], k: usize) -> Result<usize> {
    let size = if p.wide { 4 } else { 2 };
    let b = offsets.get(k * size..(k + 1) * size).ok_or_else(corrupt)?;
    Ok(if p.wide {
        u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize
    } else {
        usize::from(u16::from_le_bytes([b[0], b[1]]))
    })
}

/// Decodes every column of a row written under `schema`.
///
/// # Errors
/// The row was written under another schema version, or doesn't decode.
pub fn decode(schema: &RowSchema, row: &Bytes) -> Result<Vec<Value>> {
    let p = parse(schema, row)?;
    let (fixed_len, vars) = var_layout(schema, &p);
    let offset_size = if p.wide { 4 } else { 2 };
    let fixed = p.body.get(..fixed_len).ok_or_else(corrupt)?;
    let offsets = p
        .body
        .get(fixed_len..fixed_len + vars * offset_size)
        .ok_or_else(corrupt)?;
    let var_bytes = &p.body[fixed_len + vars * offset_size..];
    let mut out = Vec::with_capacity(schema.columns.len());
    let (mut at, mut k, mut start) = (0, 0, 0);
    for (i, (c, w)) in schema.columns.iter().zip(&schema.widths).enumerate() {
        if p.is_null(i) {
            out.push(Value::Null);
        } else if let Some(w) = *w {
            out.push(get_fixed(c.ty, &fixed[at..at + w], row)?);
            at += w;
        } else {
            let end = offset_at(&p, offsets, k)?;
            let b = var_bytes.get(start..end).ok_or_else(corrupt)?;
            out.push(var_value(c.ty, row.slice_ref(b))?);
            k += 1;
            start = end;
        }
    }
    if start != var_bytes.len() {
        return Err(corrupt());
    }
    Ok(out)
}

/// Decodes column `index` of a row written under `schema`, without decoding
/// the others.
///
/// # Errors
/// `index` is past the schema's columns, the row was written under another
/// schema version, or it doesn't decode.
pub fn decode_column(schema: &RowSchema, row: &Bytes, index: usize) -> Result<Value> {
    let p = parse(schema, row)?;
    column_of(schema, &p, row, index)
}

fn column_of(schema: &RowSchema, p: &Parsed<'_>, row: &Bytes, index: usize) -> Result<Value> {
    let col = schema
        .columns
        .get(index)
        .ok_or_else(|| bad("no such column in the row schema"))?;
    if p.is_null(index) {
        return Ok(Value::Null);
    }
    // Bytes of the non-NULL fixed columns before this one, and the count of
    // non-NULL variable-length columns before it.
    let (mut fixed_before, mut vars_before) = (0, 0);
    for (i, w) in schema.widths[..index].iter().enumerate() {
        if !p.is_null(i) {
            match w {
                Some(w) => fixed_before += w,
                None => vars_before += 1,
            }
        }
    }
    if let Some(w) = schema.widths[index] {
        let b = p
            .body
            .get(fixed_before..fixed_before + w)
            .ok_or_else(corrupt)?;
        return get_fixed(col.ty, b, row);
    }
    let (fixed_len, vars) = var_layout(schema, p);
    let offset_size = if p.wide { 4 } else { 2 };
    let offsets = p
        .body
        .get(fixed_len..fixed_len + vars * offset_size)
        .ok_or_else(corrupt)?;
    let var_bytes = &p.body[fixed_len + vars * offset_size..];
    let start = match vars_before {
        0 => 0,
        k => offset_at(p, offsets, k - 1)?,
    };
    let end = offset_at(p, offsets, vars_before)?;
    let b = var_bytes.get(start..end).ok_or_else(corrupt)?;
    var_value(col.ty, row.slice_ref(b))
}

/// Whether a value stored as `from` can be read as `to` without rewriting
/// the row: the same type, or a widening.
///
/// Widenings: an integer to a wider one of the same signedness, or an
/// unsigned one to a wider signed one; `BIT` to more bits; `DECIMAL` to at
/// least as many digits on each side of the point; `FLOAT` to `DOUBLE`;
/// a temporal to a higher fractional precision; `CHAR` and `VARCHAR` to a
/// longer `CHAR` or `VARCHAR`, or to `TEXT`, and `TEXT` to a larger `TEXT`,
/// within one character set; `BINARY` to a longer `BINARY` (zero padded) or
/// to `VARBINARY`; `VARBINARY` to a longer one or to `BLOB`; `BLOB` to a
/// larger one; `ENUM` and `SET` to more members (appended).
#[must_use]
// The arms that look alike compare lengths of different integer types.
#[allow(clippy::match_same_arms)]
pub fn reads_as(from: &ColumnType, to: &ColumnType) -> bool {
    use ColumnType as T;
    match (*from, *to) {
        (a, b) if a == b => true,
        (
            T::Int {
                bytes: a,
                unsigned: ua,
            },
            T::Int {
                bytes: b,
                unsigned: ub,
            },
        ) => (ua == ub && b >= a) || (ua && !ub && b > a),
        (T::Bit { bits: a }, T::Bit { bits: b }) => b >= a,
        (
            T::Decimal {
                precision: pa,
                scale: sa,
            },
            T::Decimal {
                precision: pb,
                scale: sb,
            },
        ) => sb >= sa && pb - sb >= pa - sa,
        (T::Float, T::Double) => true,
        (T::Time { fsp: a }, T::Time { fsp: b })
        | (T::DateTime { fsp: a }, T::DateTime { fsp: b })
        | (T::Timestamp { fsp: a }, T::Timestamp { fsp: b }) => b >= a,
        (
            T::Char {
                chars: a,
                collation: ca,
            },
            T::Char {
                chars: b,
                collation: cb,
            },
        ) => b >= a && ca.charset() == cb.charset(),
        (
            T::Char {
                chars: a,
                collation: ca,
            },
            T::VarChar {
                chars: b,
                collation: cb,
            },
        ) => b >= u32::from(a) && ca.charset() == cb.charset(),
        (
            T::VarChar {
                chars: a,
                collation: ca,
            },
            T::VarChar {
                chars: b,
                collation: cb,
            },
        ) => b >= a && ca.charset() == cb.charset(),
        (
            T::Char { collation: ca, .. } | T::VarChar { collation: ca, .. },
            T::Text {
                size,
                collation: cb,
            },
        ) => {
            // The longest value the old column holds, at 4 bytes a character.
            let chars = match *from {
                T::Char { chars, .. } => u64::from(chars),
                T::VarChar { chars, .. } => u64::from(chars),
                _ => 0,
            };
            ca.charset() == cb.charset() && chars * 4 <= size.max_bytes()
        }
        (
            T::Text {
                size: a,
                collation: ca,
            },
            T::Text {
                size: b,
                collation: cb,
            },
        ) => b.max_bytes() >= a.max_bytes() && ca.charset() == cb.charset(),
        (T::Binary { bytes: a }, T::Binary { bytes: b }) => b >= a,
        (T::Binary { bytes: a }, T::VarBinary { bytes: b }) => b >= u32::from(a),
        (T::VarBinary { bytes: a }, T::VarBinary { bytes: b }) => b >= a,
        (T::Binary { bytes }, T::Blob { size }) => u64::from(bytes) <= size.max_bytes(),
        (T::VarBinary { bytes }, T::Blob { size }) => u64::from(bytes) <= size.max_bytes(),
        (T::Blob { size: a }, T::Blob { size: b }) => b.max_bytes() >= a.max_bytes(),
        (T::Enum { members: a }, T::Enum { members: b }) => b >= a,
        (T::Set { members: a }, T::Set { members: b }) => b >= a,
        _ => false,
    }
}

/// Converts a value stored as `from` into its value as `to`, for a pair
/// [`reads_as`] accepts.
fn widen(v: Value, to: ColumnType) -> Result<Value> {
    let lost = || bad("a stored value doesn't fit its widened column");
    Ok(match (v, to) {
        (Value::Null, _) => Value::Null,
        (
            Value::UInt(u),
            ColumnType::Int {
                unsigned: false, ..
            },
        ) => Value::Int(i64::try_from(u).map_err(|_| lost())?),
        (Value::Decimal(d), ColumnType::Decimal { scale, .. }) => {
            Value::Decimal(d.with_scale(scale).ok_or_else(lost)?)
        }
        (Value::Float(f), ColumnType::Double) => Value::Double(f64::from(f)),
        (Value::Bytes(b), ColumnType::Binary { bytes }) if b.len() < usize::from(bytes) => {
            let mut padded = b.to_vec();
            padded.resize(usize::from(bytes), 0);
            Value::Bytes(Bytes::from(padded))
        }
        (v, _) => v,
    })
}

/// A reader's plan for rows of an older schema version: for each of its
/// columns, where the value comes from.
enum Source {
    /// The writer's column at this position, to be widened.
    Stored(usize),
    /// The column didn't exist: its missing-value default.
    Default,
}

fn source_of(writer: &RowSchema, reader: &RowSchema, index: usize) -> Result<Source> {
    let col = reader
        .columns
        .get(index)
        .ok_or_else(|| bad("no such column in the row schema"))?;
    let Some(at) = writer.position_of(col.id) else {
        return Ok(Source::Default);
    };
    if !reads_as(&writer.columns[at].ty, &col.ty) {
        return Err(bad(format!(
            "column {} changed from {:?} to {:?}, which needs its rows rewritten",
            col.id, writer.columns[at].ty, col.ty
        )));
    }
    Ok(Source::Stored(at))
}

fn finish(reader: &Column, stored: bool, v: Value) -> Result<Value> {
    let v = if stored { widen(v, reader.ty)? } else { v };
    if v.is_null() && !reader.nullable {
        return Err(bad(format!(
            "column {} became NOT NULL but a stored row has NULL",
            reader.id
        )));
    }
    Ok(v)
}

/// Decodes a row written under `writer` as the columns of `reader`, a later
/// (or earlier) version of the same table. Columns are matched by id: one
/// only `reader` has shows its default, one only `writer` has is skipped,
/// one in both is widened if its type changed.
///
/// # Errors
/// The row wasn't written under `writer`, doesn't decode, or a column's type
/// change isn't one [`reads_as`] accepts.
pub fn decode_as(writer: &RowSchema, reader: &RowSchema, row: &Bytes) -> Result<Vec<Value>> {
    if writer.version == reader.version {
        return decode(reader, row);
    }
    let p = parse(writer, row)?;
    (0..reader.columns.len())
        .map(|i| column_as(writer, reader, &p, row, i))
        .collect()
}

/// Decodes column `index` of `reader` from a row written under `writer`,
/// without decoding the others.
///
/// # Errors
/// As [`decode_as`].
pub fn decode_column_as(
    writer: &RowSchema,
    reader: &RowSchema,
    row: &Bytes,
    index: usize,
) -> Result<Value> {
    if writer.version == reader.version {
        return decode_column(reader, row, index);
    }
    let p = parse(writer, row)?;
    column_as(writer, reader, &p, row, index)
}

fn column_as(
    writer: &RowSchema,
    reader: &RowSchema,
    p: &Parsed<'_>,
    row: &Bytes,
    index: usize,
) -> Result<Value> {
    let col = &reader.columns[index];
    match source_of(writer, reader, index)? {
        Source::Default => finish(col, false, col.default.clone()),
        Source::Stored(at) => finish(col, true, column_of(writer, p, row, at)?),
    }
}

/// How many schema versions behind a row may fall before it is rewritten
/// when next touched: old rows are read through [`decode_as`] until then.
pub const REWRITE_AFTER_VERSIONS: u32 = 8;

/// Whether a row written under `row_version` should be rewritten under
/// `current`: it is [`REWRITE_AFTER_VERSIONS`] or more versions behind.
#[must_use]
pub const fn is_stale(row_version: u32, current: u32) -> bool {
    current.saturating_sub(row_version) >= REWRITE_AFTER_VERSIONS
}

/// Appends the record of an older row rewritten under `reader`: what
/// [`decode_as`] reads, encoded under the newer schema with the row's own
/// expiry. This is the lazy rewrite a compaction or an update does.
///
/// # Errors
/// As [`decode_as`]; nothing is appended then.
pub fn upgrade(
    writer: &RowSchema,
    reader: &RowSchema,
    row: &Bytes,
    out: &mut Vec<u8>,
) -> Result<()> {
    let expires_at = peek_header(row)?.expires_at;
    let values = decode_as(writer, reader, row)?;
    encode(reader, &values, expires_at, out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::collation::Collation;
    use crate::row::LobSize;
    use crate::row::temporal::Date;

    fn schema(version: u32, cols: Vec<Column>) -> RowSchema {
        RowSchema::new(version, cols).unwrap()
    }

    fn row(s: &RowSchema, vals: &[Value], expiry: Option<u64>) -> Bytes {
        let mut out = Vec::new();
        encode(s, vals, expiry, &mut out).unwrap();
        Bytes::from(out)
    }

    fn varchar(chars: u32) -> ColumnType {
        ColumnType::VarChar {
            chars,
            collation: Collation::Utf8mb4Uca1400AiCi,
        }
    }

    #[test]
    fn layout() {
        let s = schema(
            300,
            vec![
                Column::new(1, ColumnType::int(false)),
                Column::new(2, varchar(10)),
                Column::new(3, ColumnType::smallint(true)),
                Column::new(
                    4,
                    ColumnType::Blob {
                        size: LobSize::Normal,
                    },
                ),
                Column::new(5, varchar(10)),
            ],
        );
        let vals = [
            Value::Int(-2),
            Value::str("ab"),
            Value::Null,
            Value::bytes(b""),
            Value::str("c"),
        ];
        let r = row(&s, &vals, None);
        assert_eq!(
            &r[..],
            [
                1, // format
                0xAC,
                0x02,        // schema version 300
                0,           // flags
                0b0000_0100, // column 3 is NULL
                0xFE,
                0xFF,
                0xFF,
                0xFF, // -2
                2,
                0,
                2,
                0,
                3,
                0, // var ends
                b'a',
                b'b',
                b'c',
            ]
        );
        assert_eq!(decode(&s, &r).unwrap(), vals);
        let r = row(&s, &vals, Some(0x0102));
        assert_eq!(&r[3..12], [1, 2, 1, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            peek_header(&r).unwrap(),
            RowHeader {
                schema_version: 300,
                expires_at: Some(0x0102)
            }
        );
        assert_eq!(decode(&s, &r).unwrap(), vals);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(&decode_column(&s, &r, i).unwrap(), v);
        }
        assert!(decode_column(&s, &r, 5).is_err());
    }

    #[test]
    fn strings_are_slices_of_the_row() {
        let s = schema(
            1,
            vec![
                Column::new(1, varchar(10)),
                Column::new(2, ColumnType::Binary { bytes: 2 }),
            ],
        );
        let r = row(&s, &[Value::str("hello"), Value::bytes(b"zz")], None);
        let lo = r.as_ptr() as usize;
        for v in decode(&s, &r).unwrap() {
            let (Value::Str(b) | Value::Bytes(b)) = v else {
                panic!("not a string");
            };
            let at = b.as_ptr() as usize;
            assert!(at >= lo && at + b.len() <= lo + r.len());
        }
    }

    #[test]
    fn wide_offsets() {
        let text = ColumnType::Text {
            size: LobSize::Medium,
            collation: Collation::Utf8mb4Bin,
        };
        let s = schema(
            1,
            vec![
                Column::new(1, text),
                Column::new(2, text),
                Column::new(3, ColumnType::Json),
            ],
        );
        let big = Value::Str(Bytes::from(vec![b'x'; 40_000]));
        let vals = [big.clone(), big, Value::Json(Bytes::from_static(b"{}"))];
        let r = row(&s, &vals, None);
        assert_eq!(r[2], FLAG_WIDE_OFFSETS);
        assert_eq!(r.len(), 3 + 1 + 3 * 4 + 80_002);
        assert_eq!(decode(&s, &r).unwrap(), vals);
        assert_eq!(decode_column(&s, &r, 2).unwrap(), vals[2]);
        let small = [Value::str("a"), Value::str("b"), Value::Null];
        assert_eq!(row(&s, &small, None)[2], 0);
    }

    #[test]
    fn refusals() {
        let s = schema(
            1,
            vec![
                Column::new(1, ColumnType::tinyint(false)).not_null(Value::Int(0)),
                Column::new(2, varchar(2)),
            ],
        );
        let mut out = vec![7];
        assert!(encode(&s, &[Value::Int(1)], None, &mut out).is_err());
        assert!(encode(&s, &[Value::Null, Value::Null], None, &mut out).is_err());
        assert!(encode(&s, &[Value::Int(200), Value::Null], None, &mut out).is_err());
        assert!(encode(&s, &[Value::Int(1), Value::str("abc")], None, &mut out).is_err());
        assert!(encode(&s, &[Value::Int(1), Value::bytes(b"a")], None, &mut out).is_err());
        assert_eq!(out, [7]);
        // Schemas: duplicate ids, bad defaults.
        let c = |id| Column::new(id, ColumnType::int(false));
        assert!(RowSchema::new(1, vec![c(1), c(1)]).is_err());
        assert!(RowSchema::new(1, vec![c(1).not_null(Value::Null)]).is_err());
        assert!(RowSchema::new(1, vec![c(1).with_default(Value::str("x"))]).is_err());
        // Another version's row, another format, unknown flags.
        let r = row(&s, &[Value::Int(1), Value::Null], None);
        let other = schema(2, s.columns().to_vec());
        assert!(decode(&other, &r).is_err());
        let mut bad_format = r.to_vec();
        bad_format[0] = 9;
        assert!(decode(&s, &Bytes::from(bad_format.clone())).is_err());
        assert!(peek_header(&bad_format).is_err());
        let mut bad_flags = r.to_vec();
        bad_flags[2] = 0x80;
        assert!(decode(&s, &Bytes::from(bad_flags)).is_err());
        // Trailing bytes are corruption.
        let mut long = r.to_vec();
        long.push(0);
        assert!(decode(&s, &Bytes::from(long)).is_err());
    }

    #[test]
    fn varints() {
        for v in [0u32, 1, 127, 128, 300, 16_383, 16_384, u32::MAX] {
            let mut b = Vec::new();
            put_varint(&mut b, v);
            b.push(0xEE);
            assert_eq!(get_varint(&b), Some((v, &[0xEE][..])));
        }
        assert_eq!(get_varint(&[0x80, 0x80, 0x80, 0x80, 0x10]), None);
        assert_eq!(get_varint(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]), None);
        assert_eq!(get_varint(&[0x80]), None);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn older_rows_read_under_a_newer_schema() {
        let v1 = schema(
            1,
            vec![
                Column::new(1, ColumnType::int(false)).not_null(Value::Int(0)),
                Column::new(2, varchar(5)),
                Column::new(3, ColumnType::smallint(true)),
                Column::new(
                    4,
                    ColumnType::Decimal {
                        precision: 5,
                        scale: 2,
                    },
                ),
                Column::new(5, ColumnType::Float),
                Column::new(6, ColumnType::Binary { bytes: 2 }),
                Column::new(7, ColumnType::Date),
            ],
        );
        let dec = |s| Value::Decimal(crate::row::Decimal::parse(s).unwrap());
        let r = row(
            &v1,
            &[
                Value::Int(-7),
                Value::str("abc"),
                Value::UInt(65_535),
                dec("-12.50"),
                Value::Float(0.1),
                Value::bytes(b"ab"),
                Value::Date(Date::new(2024, 2, 29)),
            ],
            None,
        );
        // Version 2: column 7 dropped, 9 and 8 added (one with a default,
        // one without), the others widened and reordered.
        let v2 = schema(
            2,
            vec![
                Column::new(9, ColumnType::Year).not_null(Value::Year(2024)),
                Column::new(6, ColumnType::Binary { bytes: 4 }),
                Column::new(5, ColumnType::Double),
                Column::new(
                    4,
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 4,
                    },
                ),
                Column::new(3, ColumnType::bigint(false)),
                Column::new(
                    2,
                    ColumnType::Text {
                        size: LobSize::Normal,
                        collation: Collation::Utf8mb4GeneralCi,
                    },
                ),
                Column::new(1, ColumnType::bigint(false)).not_null(Value::Int(0)),
                Column::new(8, varchar(3)),
            ],
        );
        let want = vec![
            Value::Year(2024),
            Value::bytes(b"ab\0\0"),
            Value::Double(f64::from(0.1f32)),
            dec("-12.5000"),
            Value::Int(65_535),
            Value::str("abc"),
            Value::Int(-7),
            Value::Null,
        ];
        assert_eq!(decode_as(&v1, &v2, &r).unwrap(), want);
        for (i, w) in want.iter().enumerate() {
            assert_eq!(&decode_column_as(&v1, &v2, &r, i).unwrap(), w);
        }
        assert_eq!(peek_header(&r).unwrap().schema_version, 1);
        // Rewritten under version 2, it is a version-2 row with the same
        // values and the same expiry.
        let expiring = row(&v1, &decode(&v1, &r).unwrap(), Some(77));
        let mut newer = Vec::new();
        upgrade(&v1, &v2, &expiring, &mut newer).unwrap();
        let newer = Bytes::from(newer);
        assert_eq!(
            peek_header(&newer).unwrap(),
            RowHeader {
                schema_version: 2,
                expires_at: Some(77)
            }
        );
        assert_eq!(decode(&v2, &newer).unwrap(), want);
        assert!(!is_stale(1, 2));
        assert!(!is_stale(1, 8));
        assert!(is_stale(1, 9));
        assert!(!is_stale(9, 1));
        // The writer must be the row's own version.
        assert!(decode_as(&v2, &v1, &r).is_err());
        // The same version on both sides is a plain decode.
        assert_eq!(decode_as(&v1, &v1, &r).unwrap().len(), 7);
        // Narrowing, a sign change and a NULL under NOT NULL need a rewrite.
        let narrow = schema(3, vec![Column::new(3, ColumnType::tinyint(true))]);
        assert!(decode_as(&v1, &narrow, &r).is_err());
        let signed = schema(3, vec![Column::new(1, ColumnType::bigint(true))]);
        assert!(decode_as(&v1, &signed, &r).is_err());
        let nulls = row(
            &v1,
            &[
                Value::Int(1),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            None,
        );
        let strict = schema(3, vec![Column::new(2, varchar(5)).not_null(Value::str(""))]);
        assert!(decode_as(&v1, &strict, &nulls).is_err());
        assert_eq!(decode_as(&v1, &strict, &r).unwrap(), [Value::str("abc")]);
    }

    #[test]
    fn what_reads_as_what() {
        let yes = |a: ColumnType, b: ColumnType| assert!(reads_as(&a, &b), "{a:?} -> {b:?}");
        let no = |a: ColumnType, b: ColumnType| assert!(!reads_as(&a, &b), "{a:?} -> {b:?}");
        yes(ColumnType::int(false), ColumnType::bigint(false));
        yes(ColumnType::int(true), ColumnType::bigint(false));
        no(ColumnType::int(true), ColumnType::int(false));
        no(ColumnType::int(false), ColumnType::bigint(true));
        no(ColumnType::bigint(false), ColumnType::int(false));
        yes(ColumnType::Bit { bits: 3 }, ColumnType::Bit { bits: 64 });
        let d = |precision, scale| ColumnType::Decimal { precision, scale };
        yes(d(5, 2), d(6, 2));
        yes(d(5, 2), d(7, 4));
        no(d(5, 2), d(6, 4));
        no(d(5, 2), d(5, 1));
        yes(ColumnType::Float, ColumnType::Double);
        no(ColumnType::Double, ColumnType::Float);
        yes(ColumnType::Time { fsp: 0 }, ColumnType::Time { fsp: 6 });
        no(
            ColumnType::DateTime { fsp: 3 },
            ColumnType::DateTime { fsp: 0 },
        );
        no(
            ColumnType::DateTime { fsp: 0 },
            ColumnType::Timestamp { fsp: 0 },
        );
        let bin = Collation::Utf8mb4Bin;
        let ci = Collation::Utf8mb4GeneralCi;
        yes(
            ColumnType::Char {
                chars: 3,
                collation: bin,
            },
            ColumnType::VarChar {
                chars: 3,
                collation: ci,
            },
        );
        no(
            ColumnType::VarChar {
                chars: 3,
                collation: bin,
            },
            ColumnType::VarChar {
                chars: 3,
                collation: Collation::Binary,
            },
        );
        no(
            ColumnType::VarChar {
                chars: 300,
                collation: bin,
            },
            ColumnType::Text {
                size: LobSize::Tiny,
                collation: bin,
            },
        );
        yes(
            ColumnType::VarChar {
                chars: 63,
                collation: bin,
            },
            ColumnType::Text {
                size: LobSize::Tiny,
                collation: bin,
            },
        );
        yes(
            ColumnType::Binary { bytes: 2 },
            ColumnType::VarBinary { bytes: 2 },
        );
        no(
            ColumnType::VarBinary { bytes: 2 },
            ColumnType::Binary { bytes: 4 },
        );
        yes(
            ColumnType::Enum { members: 2 },
            ColumnType::Enum { members: 300 },
        );
        no(
            ColumnType::Set { members: 9 },
            ColumnType::Set { members: 8 },
        );
        no(
            ColumnType::Json,
            ColumnType::Blob {
                size: LobSize::Long,
            },
        );
    }
}
