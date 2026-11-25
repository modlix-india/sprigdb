//! The static type of an expression: what the servers report in result
//! metadata and use to choose comparison and arithmetic rules.

use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::{ColumnType, LobSize};

use crate::error::{Result, SqlError};
use crate::session::Persona;

/// The length, in characters, of a value computed from a JSON value whose
/// type isn't JSON (a merge of JSON and text, a JSON value on MariaDB):
/// reported as 4294967295 bytes, the most there is.
pub const JSON_TEXT_LEN: u32 = u32::MAX / 4 + 1;

/// The "decimals not fixed" marker of `DOUBLE` values.
pub const NOT_FIXED: u8 = 31;

/// The value family of an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    /// The untyped `NULL` literal.
    Null,
    /// A 64-bit integer, signed or unsigned.
    Int,
    /// An exact decimal.
    Decimal,
    /// A double.
    Double,
    /// A single-precision float (a `FLOAT` column).
    Float,
    /// A character or binary string.
    Str,
    /// A date.
    Date,
    /// A time of day or duration.
    Time,
    /// A date and time.
    DateTime,
    /// A `TIMESTAMP` column's value, already in the session's time zone; it
    /// behaves as a `DATETIME`.
    Timestamp,
    /// A `YEAR` column's value; it behaves as an integer.
    Year,
    /// A `BIT` column's value; it behaves as an unsigned integer.
    Bit,
}

impl Kind {
    /// Whether values of this kind are numbers.
    #[must_use]
    pub fn is_numeric(self) -> bool {
        matches!(
            self,
            Kind::Int | Kind::Decimal | Kind::Double | Kind::Float | Kind::Year | Kind::Bit
        )
    }

    /// Whether values of this kind are dates or times.
    #[must_use]
    pub fn is_temporal(self) -> bool {
        matches!(
            self,
            Kind::Date | Kind::Time | Kind::DateTime | Kind::Timestamp
        )
    }

    /// Whether values of this kind are integers.
    #[must_use]
    pub fn is_int(self) -> bool {
        matches!(self, Kind::Int | Kind::Year | Kind::Bit)
    }

    /// Whether values of this kind are exact numbers.
    #[must_use]
    pub fn is_exact(self) -> bool {
        self.is_int() || self == Kind::Decimal
    }

    /// Whether values of this kind are approximate numbers.
    #[must_use]
    pub fn is_real(self) -> bool {
        matches!(self, Kind::Double | Kind::Float)
    }
}

/// How strongly a string value holds on to its collation when two meet:
/// lower wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Derivation {
    /// A `COLLATE` clause.
    Explicit,
    /// A mix of collations that couldn't be resolved.
    None,
    /// A column, or a cast to a binary string.
    Implicit,
    /// A system constant such as `USER()`.
    SysConst,
    /// A cast to a character string on MariaDB (MySQL holds it as
    /// `Implicit`).
    Cast,
    /// A string literal, or a cast to a character string.
    Coercible,
    /// A number or temporal value used as a string.
    Numeric,
    /// `NULL`.
    Ignorable,
}

impl Derivation {
    /// The word the servers print in error 1267.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Derivation::Explicit => "EXPLICIT",
            Derivation::None => "NONE",
            Derivation::Implicit => "IMPLICIT",
            Derivation::SysConst => "SYSCONST",
            Derivation::Cast => "CAST",
            Derivation::Coercible => "COERCIBLE",
            Derivation::Numeric => "NUMERIC",
            Derivation::Ignorable => "IGNORABLE",
        }
    }
}

/// The static type of an expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExprType {
    /// The value family.
    pub kind: Kind,
    /// Integers and decimals: whether the type is unsigned.
    pub unsigned: bool,
    /// Integers and decimals: the most decimal digits a value has.
    pub prec: u8,
    /// Decimals: the digits after the point. Temporals: the fractional
    /// second digits. Doubles: the fixed number of printed decimals, or
    /// [`NOT_FIXED`].
    pub dec: u8,
    /// The longest text of a value, in characters.
    pub len: u32,
    /// Whether the value can be `NULL`.
    pub nullable: bool,
    /// Strings: the collation. `binary` for everything else.
    pub coll: Collation,
    /// Strings: how strongly the collation is held.
    pub deriv: Derivation,
    /// Whether the value is a hex or bit literal: a binary string that acts
    /// as an unsigned integer among numbers.
    pub hex: bool,
    /// Whether values print in their own shape instead of the type's scale
    /// (MySQL's `IF` and `CASE` hand a branch's text through unchanged).
    pub raw: bool,
    /// Integers that come straight from a column: the column's storage
    /// width in bytes (1, 2, 3, 4 or 8), which the wire type follows.
    /// Zero for computed integers.
    pub width: u8,
    /// Whether the value is a column's, unchanged.
    pub column: bool,
    /// MariaDB's `CAST(x AS CHAR)`: a string whose number, in a decimal
    /// comparison, is rounded to a whole number.
    pub cast_char: bool,
    /// A string the servers report as a fixed-length `CHAR` (the text
    /// answer of `DATE_ADD` on something that is no date type).
    pub fixed: bool,
    /// Strings: every character is known to be ASCII (a pure-ASCII literal,
    /// an `ascii` column, a number's text): such a string converts to any
    /// character set, and gives way to the other side's set when
    /// collations are merged.
    pub ascii: bool,
    /// A JSON value: a `JSON` column's, or a JSON function's result. MySQL
    /// types it `JSON` (and prints it in its own form, compares it as
    /// JSON); MariaDB's JSON is text, so there it only marks where the
    /// value came from.
    pub json: bool,
}

impl ExprType {
    const BASE: ExprType = ExprType {
        kind: Kind::Null,
        unsigned: false,
        prec: 0,
        dec: 0,
        len: 0,
        nullable: true,
        coll: Collation::Binary,
        deriv: Derivation::Ignorable,
        hex: false,
        raw: false,
        width: 0,
        column: false,
        cast_char: false,
        fixed: false,
        ascii: false,
        json: false,
    };

    /// The type of the `NULL` literal.
    #[must_use]
    pub fn null() -> Self {
        Self::BASE
    }

    /// An integer type whose values print in at most `len` characters.
    #[must_use]
    pub fn int(len: u32, unsigned: bool) -> Self {
        Self {
            kind: Kind::Int,
            unsigned,
            prec: len.saturating_sub(u32::from(!unsigned)).clamp(1, 65) as u8,
            len,
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// An integer type from its digit count: the length adds the sign.
    #[must_use]
    pub fn int_prec(prec: u32, unsigned: bool) -> Self {
        Self {
            prec: prec.clamp(1, 65) as u8,
            ..Self::int(prec + u32::from(!unsigned), unsigned)
        }
    }

    /// The boolean type of predicates: 1, 0 or `NULL`.
    #[must_use]
    pub fn boolean() -> Self {
        Self {
            prec: 1,
            ..Self::int(1, false)
        }
    }

    /// `BIGINT`, signed or unsigned, at its full width.
    #[must_use]
    pub fn bigint(unsigned: bool) -> Self {
        Self {
            prec: if unsigned { 20 } else { 19 },
            ..Self::int(if unsigned { 20 } else { 21 }, unsigned)
        }
    }

    /// `DECIMAL(prec, scale)`; both are clipped to the servers' limits (65
    /// and 38).
    #[must_use]
    pub fn decimal(prec: u32, scale: u32, unsigned: bool) -> Self {
        let scale = scale.min(38);
        let prec = prec.clamp(1, 65).max(scale.min(65));
        Self {
            kind: Kind::Decimal,
            unsigned,
            prec: prec as u8,
            dec: scale as u8,
            len: prec + u32::from(scale > 0) + u32::from(!unsigned),
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// `DOUBLE` with shortest-digits printing.
    #[must_use]
    pub fn double() -> Self {
        Self {
            kind: Kind::Double,
            dec: NOT_FIXED,
            len: 23,
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// A string of at most `chars` characters.
    #[must_use]
    pub fn string(chars: u32, coll: Collation, deriv: Derivation) -> Self {
        Self {
            kind: Kind::Str,
            dec: NOT_FIXED,
            len: chars,
            nullable: false,
            coll,
            deriv,
            ..Self::BASE
        }
    }

    /// `DATE`.
    #[must_use]
    pub fn date() -> Self {
        Self {
            kind: Kind::Date,
            len: 10,
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// `TIME(fsp)`.
    #[must_use]
    pub fn time(fsp: u8) -> Self {
        Self {
            kind: Kind::Time,
            dec: fsp,
            len: 10 + if fsp > 0 { u32::from(fsp) + 1 } else { 0 },
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// `DATETIME(fsp)`.
    #[must_use]
    pub fn datetime(fsp: u8) -> Self {
        Self {
            kind: Kind::DateTime,
            dec: fsp,
            len: 19 + if fsp > 0 { u32::from(fsp) + 1 } else { 0 },
            nullable: false,
            deriv: Derivation::Numeric,
            ..Self::BASE
        }
    }

    /// The same type, able to be `NULL`.
    #[must_use]
    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    /// The same type with the given nullability.
    #[must_use]
    pub fn with_null(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// The type an expression has when it reads a column of `ty`.
    #[must_use]
    pub fn of_column(ty: &ColumnType, nullable: bool) -> Self {
        let t = match *ty {
            ColumnType::Int { bytes, unsigned } => {
                let (len, prec) = match (bytes, unsigned) {
                    (1, false) => (4, 3),
                    (1, true) => (3, 3),
                    (2, false) => (6, 5),
                    (2, true) => (5, 5),
                    (3, false) => (9, 7),
                    (3, true) => (8, 8),
                    (4, false) => (11, 10),
                    (4, true) => (10, 10),
                    (_, false) => (20, 19),
                    (_, true) => (20, 20),
                };
                Self {
                    prec,
                    width: bytes,
                    ..Self::int(len, unsigned)
                }
            }
            ColumnType::Bit { bits } => Self {
                kind: Kind::Bit,
                unsigned: true,
                prec: 20,
                len: u32::from(bits),
                ..Self::int(u32::from(bits), true)
            },
            ColumnType::Decimal { precision, scale } => {
                Self::decimal(u32::from(precision), u32::from(scale), false)
            }
            ColumnType::Float => Self {
                kind: Kind::Float,
                len: 12,
                ..Self::double()
            },
            ColumnType::Double => Self {
                len: 22,
                ..Self::double()
            },
            ColumnType::Date => Self::date(),
            ColumnType::Time { fsp } => Self::time(fsp),
            ColumnType::DateTime { fsp } => Self::datetime(fsp),
            ColumnType::Timestamp { fsp } => Self {
                kind: Kind::Timestamp,
                ..Self::datetime(fsp)
            },
            ColumnType::Year => Self {
                kind: Kind::Year,
                unsigned: true,
                prec: 4,
                ..Self::int(4, true)
            },
            ColumnType::Char { chars, collation } => {
                Self::string(u32::from(chars), collation, Derivation::Implicit)
            }
            ColumnType::VarChar { chars, collation } => {
                Self::string(chars, collation, Derivation::Implicit)
            }
            ColumnType::Text { size, collation } => {
                let bytes = lob_bytes(size);
                let mb = mb_max_len(collation);
                Self::string(bytes / mb, collation, Derivation::Implicit)
            }
            ColumnType::Binary { bytes } => {
                Self::string(u32::from(bytes), Collation::Binary, Derivation::Implicit)
            }
            ColumnType::VarBinary { bytes } => {
                Self::string(bytes, Collation::Binary, Derivation::Implicit)
            }
            ColumnType::Blob { size } => {
                Self::string(lob_bytes(size), Collation::Binary, Derivation::Implicit)
            }
            ColumnType::Enum { .. } => Self {
                prec: 5,
                ..Self::int(5, true)
            },
            ColumnType::Set { .. } => Self::bigint(true),
            ColumnType::Json => Self {
                json: true,
                ..Self::string(u32::MAX / 4, Collation::Utf8mb4Bin, Derivation::Implicit)
            },
        };
        Self {
            column: true,
            ascii: t.kind == Kind::Str && t.coll.charset() == Charset::Ascii,
            ..t.with_null(nullable)
        }
    }

    /// Whether the type is a string (character or binary).
    #[must_use]
    pub fn is_string(&self) -> bool {
        self.kind == Kind::Str
    }

    /// Whether the type is a binary string.
    #[must_use]
    pub fn is_binary_string(&self) -> bool {
        self.kind == Kind::Str && self.coll == Collation::Binary
    }

    /// The digits before the point of an exact number.
    #[must_use]
    pub fn int_part(&self) -> u32 {
        match self.kind {
            Kind::Decimal => u32::from(self.prec) - u32::from(self.dec).min(u32::from(self.prec)),
            _ => u32::from(self.prec),
        }
    }

    /// The scale an exact number contributes to decimal arithmetic.
    #[must_use]
    pub fn scale(&self) -> u32 {
        match self.kind {
            Kind::Decimal => u32::from(self.dec),
            Kind::Time | Kind::DateTime | Kind::Timestamp => u32::from(self.dec),
            _ => 0,
        }
    }

    /// The result-set metadata of a value of this type.
    #[must_use]
    pub fn meta(&self, persona: Persona) -> Meta {
        let maria = persona.is_maria();
        let (field, len, dec) = match self.kind {
            Kind::Null => (FieldType::Null, 0, 0),
            Kind::Int => (
                match self.width {
                    1 => FieldType::Tiny,
                    2 => FieldType::Short,
                    3 => FieldType::Int24,
                    4 => FieldType::Long,
                    8 => FieldType::LongLong,
                    _ if maria && self.len <= 9 => FieldType::Long,
                    _ => FieldType::LongLong,
                },
                self.len,
                0,
            ),
            Kind::Year => (FieldType::Year, 4, 0),
            Kind::Bit => (FieldType::Bit, self.len, 0),
            Kind::Decimal => (FieldType::NewDecimal, self.len, self.dec),
            Kind::Double => (FieldType::Double, self.len, self.dec),
            Kind::Float => (FieldType::Float, self.len, self.dec),
            // MySQL's JSON type: its fixed length, in the result character
            // set.
            Kind::Str if self.json && !maria => {
                (FieldType::Json, u32::MAX / 4 * 4, persona.not_fixed_dec())
            }
            Kind::Str => {
                // A character string is sent in the result character set,
                // utf8mb4: four bytes a character, whatever its own.
                let per = if self.coll.charset() == Charset::Binary {
                    1
                } else {
                    4
                };
                let bytes = u64::from(self.len) * per;
                let field = if self.fixed {
                    FieldType::String
                } else if bytes >= 1 << 24 {
                    FieldType::LongBlob
                } else if bytes >= 1 << 16 {
                    FieldType::MediumBlob
                } else {
                    FieldType::VarString
                };
                let dec = if self.column {
                    0
                } else if self.dec == NOT_FIXED {
                    persona.not_fixed_dec()
                } else {
                    self.dec
                };
                (field, bytes.min(u64::from(u32::MAX)) as u32, dec)
            }
            Kind::Date => (FieldType::Date, 10, 0),
            Kind::Time => (FieldType::Time, self.len, self.dec),
            Kind::DateTime => (FieldType::DateTime, self.len, self.dec),
            Kind::Timestamp => (FieldType::Timestamp, self.len, self.dec),
        };
        Meta {
            field,
            len,
            dec,
            unsigned: self.unsigned,
            not_null: !self.nullable,
            binary: self.kind != Kind::Str || self.coll == Collation::Binary,
            coll: self.coll,
        }
    }
}

fn lob_bytes(size: LobSize) -> u32 {
    size.max_bytes().min(u64::from(u32::MAX)) as u32
}

/// The most bytes one character takes in a collation's character set.
#[must_use]
pub fn mb_max_len(coll: Collation) -> u32 {
    match coll.charset() {
        Charset::Utf8mb4 => 4,
        _ => 1,
    }
}

/// The wire type of a result column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldType {
    /// `NULL`.
    Null,
    /// 8-bit integer.
    Tiny,
    /// 16-bit integer.
    Short,
    /// 24-bit integer.
    Int24,
    /// 32-bit integer.
    Long,
    /// 64-bit integer.
    LongLong,
    /// `YEAR`.
    Year,
    /// `BIT`.
    Bit,
    /// `DECIMAL`.
    NewDecimal,
    /// `FLOAT`.
    Float,
    /// `DOUBLE`.
    Double,
    /// A string up to 65,535 bytes.
    VarString,
    /// A fixed-length string (`CHAR`).
    String,
    /// `BLOB` or `TEXT`.
    Blob,
    /// `MEDIUMBLOB` or `MEDIUMTEXT`.
    MediumBlob,
    /// `LONGBLOB` or `LONGTEXT`.
    LongBlob,
    /// `DATE`.
    Date,
    /// `TIME`.
    Time,
    /// `DATETIME`.
    DateTime,
    /// `TIMESTAMP`.
    Timestamp,
    /// MySQL's `JSON`.
    Json,
}

impl FieldType {
    /// The name the `mysql` client prints with `--column-type-info`.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            FieldType::Null => "NULL",
            FieldType::Tiny => "TINY",
            FieldType::Short => "SHORT",
            FieldType::Int24 => "INT24",
            FieldType::Long => "LONG",
            FieldType::LongLong => "LONGLONG",
            FieldType::Year => "YEAR",
            FieldType::Bit => "BIT",
            FieldType::NewDecimal => "NEWDECIMAL",
            FieldType::Float => "FLOAT",
            FieldType::Double => "DOUBLE",
            FieldType::VarString => "VAR_STRING",
            FieldType::String => "STRING",
            FieldType::Blob => "BLOB",
            FieldType::MediumBlob => "MEDIUM_BLOB",
            FieldType::LongBlob => "LONG_BLOB",
            FieldType::Date => "DATE",
            FieldType::Time => "TIME",
            FieldType::DateTime => "DATETIME",
            FieldType::Timestamp => "TIMESTAMP",
            FieldType::Json => "JSON",
        }
    }
}

/// The result-set metadata of one column, as a client sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Meta {
    /// The wire type.
    pub field: FieldType,
    /// The display length (bytes for strings).
    pub len: u32,
    /// The decimals.
    pub dec: u8,
    /// The `UNSIGNED` flag.
    pub unsigned: bool,
    /// The `NOT_NULL` flag.
    pub not_null: bool,
    /// Whether the value is binary (everything but character strings).
    pub binary: bool,
    /// The collation of a character string.
    pub coll: Collation,
}

/// The `_bin` collation of a collation's character set.
#[must_use]
pub fn bin_collation(coll: Collation) -> Collation {
    match coll.charset() {
        Charset::Binary => Collation::Binary,
        Charset::Ascii => Collation::AsciiBin,
        Charset::Latin1 => Collation::Latin1Bin,
        Charset::Utf8mb4 => Collation::Utf8mb4Bin,
    }
}

fn is_bin(coll: Collation) -> bool {
    matches!(
        coll,
        Collation::Binary
            | Collation::Utf8mb4Bin
            | Collation::Utf8mb4NopadBin
            | Collation::Latin1Bin
            | Collation::Latin1NopadBin
            | Collation::AsciiBin
            | Collation::AsciiNopadBin
    )
}

/// A collation with its derivation: what two string operands are merged
/// by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dtc {
    /// The collation.
    pub coll: Collation,
    /// Its derivation.
    pub deriv: Derivation,
}

impl Dtc {
    /// The collation and derivation of a type used as a string: its own for
    /// strings; for numbers and temporals the connection collation with
    /// the `NUMERIC` derivation.
    #[must_use]
    pub fn of(ty: &ExprType, connection: Collation) -> Self {
        match ty.kind {
            Kind::Str => Dtc {
                coll: ty.coll,
                deriv: ty.deriv,
            },
            Kind::Null => Dtc {
                coll: Collation::Binary,
                deriv: Derivation::Ignorable,
            },
            _ => Dtc {
                coll: connection,
                deriv: Derivation::Numeric,
            },
        }
    }
}

/// Merges two operands' collations by the servers' rules. `None` means
/// the mix is illegal (error 1267 and its siblings).
#[must_use]
pub fn aggregate2(a: Dtc, b: Dtc) -> Option<Dtc> {
    if a.deriv == Derivation::Ignorable {
        return Some(b);
    }
    if b.deriv == Derivation::Ignorable {
        return Some(a);
    }
    let (ca, cb) = (a.coll.charset(), b.coll.charset());
    if ca != cb {
        // The binary character set wins unless the other side holds its
        // collation strictly stronger.
        if ca == Charset::Binary {
            return Some(if b.deriv < a.deriv { b } else { a });
        }
        if cb == Charset::Binary {
            return Some(if a.deriv < b.deriv { a } else { b });
        }
        // A Unicode side takes in the narrower character set.
        let wide = |c: Charset| c == Charset::Utf8mb4;
        if wide(ca) && !wide(cb) {
            return Some(if b.deriv < a.deriv && a.deriv >= Derivation::Coercible {
                b
            } else {
                a
            });
        }
        if wide(cb) && !wide(ca) {
            return Some(if a.deriv < b.deriv && b.deriv >= Derivation::Coercible {
                a
            } else {
                b
            });
        }
        // Two narrow sets: the weaker side converts if it is a literal or
        // weaker.
        if a.deriv < b.deriv && b.deriv >= Derivation::SysConst {
            return Some(a);
        }
        if b.deriv < a.deriv && a.deriv >= Derivation::SysConst {
            return Some(b);
        }
        return None;
    }
    if a.deriv != b.deriv {
        return Some(if a.deriv < b.deriv { a } else { b });
    }
    if a.coll == b.coll {
        return Some(a);
    }
    if a.deriv == Derivation::Explicit {
        return None;
    }
    if is_bin(a.coll) && !is_bin(b.coll) {
        return Some(a);
    }
    if is_bin(b.coll) && !is_bin(a.coll) {
        return Some(b);
    }
    Some(Dtc {
        coll: bin_collation(a.coll),
        deriv: Derivation::None,
    })
}

/// Whether a type's strings are all ASCII (see [`ExprType::ascii`]):
/// numbers, dates and times print in ASCII.
#[must_use]
pub fn ascii_repertoire(ty: &ExprType) -> bool {
    ty.kind != Kind::Str || ty.ascii
}

/// Merges two operands' collations as [`aggregate2`] does, knowing which
/// of them hold only ASCII characters. Between character sets that
/// differ: a side takes in the other when it is utf8mb4 and the other is
/// narrower and held no more strongly, or when the other holds only ASCII
/// and is held no more strongly (the left side is tried first);
/// otherwise the weaker side converts if it is a literal or weaker. The
/// result is ASCII when both sides are.
#[must_use]
pub fn aggregate2_rep(a: (Dtc, bool), b: (Dtc, bool)) -> Option<(Dtc, bool)> {
    let ((da, ra), (db, rb)) = (a, b);
    let (ca, cb) = (da.coll.charset(), db.coll.charset());
    if da.deriv == Derivation::Ignorable {
        return Some(b);
    }
    if db.deriv == Derivation::Ignorable {
        return Some(a);
    }
    if ca == cb || ca == Charset::Binary || cb == Charset::Binary {
        return aggregate2(da, db).map(|d| (d, ra && rb));
    }
    // Whether the left side takes in the right: as utf8mb4 taking in a
    // narrower set held no more strongly, or as any set taking in an
    // ASCII side held no more strongly (the left one first: the order of
    // the operands counts).
    let takes_in = |l: (Dtc, bool), r: (Dtc, bool)| {
        let unicode = l.0.coll.charset() == Charset::Utf8mb4
            && (l.0.deriv < r.0.deriv
                || (l.0.deriv == r.0.deriv && r.0.coll.charset() != Charset::Utf8mb4));
        let from_ascii = r.1 && (l.0.deriv < r.0.deriv || (l.0.deriv == r.0.deriv && !l.1));
        unicode || from_ascii
    };
    if takes_in(a, b) {
        return Some(a);
    }
    if takes_in(b, a) {
        return Some(b);
    }
    if da.deriv < db.deriv && db.deriv >= Derivation::SysConst {
        return Some((da, ra));
    }
    if db.deriv < da.deriv && da.deriv >= Derivation::SysConst {
        return Some((db, rb));
    }
    None
}

/// Merges the collations of operands given by their types, as
/// [`aggregate`] does, with what [`aggregate2_rep`] knows of ASCII.
pub fn aggregate_types(
    tys: &[&ExprType],
    connection: Collation,
    op: &str,
    for_compare: bool,
) -> Result<Dtc> {
    let items: Vec<(Dtc, bool)> = tys
        .iter()
        .map(|t| (Dtc::of(t, connection), ascii_repertoire(t)))
        .collect();
    let mut acc = items.first().copied().unwrap_or((
        Dtc {
            coll: Collation::Binary,
            deriv: Derivation::Ignorable,
        },
        true,
    ));
    let mut failed = false;
    for it in items.iter().skip(1) {
        match aggregate2_rep(acc, *it) {
            Some(m) => acc = m,
            None => {
                failed = true;
                break;
            }
        }
    }
    let dtcs: Vec<Dtc> = items.iter().map(|(d, _)| *d).collect();
    if failed || (for_compare && acc.0.deriv == Derivation::None) {
        return Err(illegal_mix(&dtcs, op));
    }
    Ok(acc.0)
}

/// Merges the collations of the operands of `op`. With `for_compare`, a
/// result of derivation `NONE` is an error too, since nothing can be
/// compared under it.
pub fn aggregate(items: &[Dtc], op: &str, for_compare: bool) -> Result<Dtc> {
    let mut acc = items.first().copied().unwrap_or(Dtc {
        coll: Collation::Binary,
        deriv: Derivation::Ignorable,
    });
    let mut failed = false;
    for d in items.iter().skip(1) {
        match aggregate2(acc, *d) {
            Some(m) => acc = m,
            None => {
                failed = true;
                break;
            }
        }
    }
    if failed || (for_compare && acc.deriv == Derivation::None) {
        return Err(illegal_mix(items, op));
    }
    Ok(acc)
}

pub(crate) fn illegal_mix(items: &[Dtc], op: &str) -> SqlError {
    let one = |d: &Dtc| format!("({},{})", d.coll.name(), d.deriv.name());
    match items {
        [a, b] => SqlError::new(
            1267,
            "HY000",
            format!(
                "Illegal mix of collations {} and {} for operation '{op}'",
                one(a),
                one(b)
            ),
        ),
        [a, b, c] => SqlError::new(
            1270,
            "HY000",
            format!(
                "Illegal mix of collations {}, {}, {} for operation '{op}'",
                one(a),
                one(b),
                one(c)
            ),
        ),
        _ => SqlError::new(
            1271,
            "HY000",
            format!("Illegal mix of collations for operation '{op}'"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dtc(coll: Collation, deriv: Derivation) -> Dtc {
        Dtc { coll, deriv }
    }

    #[test]
    fn collations_merge_by_derivation() {
        let lit = dtc(Collation::Utf8mb4Uca1400AiCi, Derivation::Coercible);
        let col = dtc(Collation::Utf8mb4GeneralCi, Derivation::Implicit);
        let bin = dtc(Collation::Utf8mb4Bin, Derivation::Implicit);
        let expl = dtc(Collation::Utf8mb4Bin, Derivation::Explicit);
        let bytes = dtc(Collation::Binary, Derivation::Implicit);
        assert_eq!(aggregate2(lit, col), Some(col));
        assert_eq!(aggregate2(col, bin), Some(bin));
        assert_eq!(aggregate2(lit, expl), Some(expl));
        assert_eq!(aggregate2(lit, bytes), Some(bytes));
        let none = aggregate2(col, dtc(Collation::Utf8mb4UnicodeCi, Derivation::Implicit))
            .expect("merges to NONE");
        assert_eq!(none.deriv, Derivation::None);
        assert!(
            aggregate(
                &[col, dtc(Collation::Utf8mb4UnicodeCi, Derivation::Implicit)],
                "=",
                true
            )
            .is_err()
        );
        let e = aggregate(
            &[expl, dtc(Collation::Utf8mb4GeneralCi, Derivation::Explicit)],
            "=",
            true,
        )
        .expect_err("two explicit collations");
        assert_eq!(e.code, 1267);
    }

    #[test]
    fn decimal_type_lengths() {
        let t = ExprType::decimal(3, 2, false);
        assert_eq!((t.prec, t.dec, t.len), (3, 2, 5));
        let t = ExprType::decimal(80, 40, false);
        assert_eq!((t.prec, t.dec), (65, 38));
        assert_eq!(
            ExprType::int(1, false).meta(Persona::MariaDb).field,
            FieldType::Long
        );
        assert_eq!(
            ExprType::int(10, false).meta(Persona::MariaDb).field,
            FieldType::LongLong
        );
        assert_eq!(
            ExprType::int(1, false).meta(Persona::MySql).field,
            FieldType::LongLong
        );
    }
}
