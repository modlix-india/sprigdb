//! Storage assignment: the value a column stores for a value a statement
//! gives it, with the conditions the servers raise on the way (observed
//! from MariaDB 11.8 and MySQL 8.4 as black boxes).
//!
//! Every rule produces a value and, when the value had to change, a
//! condition: a **note** (never more than that: a rounded decimal, cut
//! trailing spaces), or a **warning** that the session's strict mode
//! (`STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`) turns into the statement's
//! error. Without strict mode the changed value is stored and the warning
//! counted.
//!
//! | Column | A value that doesn't fit |
//! |---|---|
//! | integers | nearest bound, 1264; text that is no number: 0, 1366; a number then other text: the number, 1265 |
//! | `DECIMAL(m,d)` | rounded half away from zero to `d` decimals with a note; too many digits before the point: the largest value, 1264 |
//! | `FLOAT`, `DOUBLE` | beyond the type's range: the largest value, 1264; with `(M,D)`, rounded to `D` decimals and clipped to `M` digits |
//! | `CHAR`, `VARCHAR`, `TEXT` | cut to the length: 1406 in strict mode, else 1265; only spaces cut: a note at most |
//! | `DATE`, `DATETIME`, `TIMESTAMP` | not a date, or not in the calendar or the type's range: the zero value, 1292 (strict) or 1265/1264; a time of day cut from a `DATE`: a note |
//! | `TIME` | beyond 838:59:59: the bound, 1292 (strict) or 1264 |
//! | `YEAR` | outside 1901 to 2155 (after the two-digit rule): 0000, 1264 |
//! | `BINARY`, `VARBINARY`, `BLOB` | longer than the column: 1406 (strict), or cut with 1265; every byte counts, spaces too |
//! | `ENUM`, `SET` | text that is no member, a number that is no index or mask: 1265, storing `''` (the empty set keeps the members that matched) |
//! | `BIT(n)` | a number or bytes of more than `n` bits: 1406 (strict), or all ones with 1264 |
//! | `JSON` | text that is no JSON document: 4025 |
//! | any `NOT NULL` column | `NULL`: 1048, an error in either mode for a single row |
//!
//! Messages are MariaDB's; the MySQL port's error mapping rewords them as
//! it does for any backend error.

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::{Date, DateTime, Decimal, Time, Timestamp};

use super::expr;
use super::row::Datum;
use super::table::{ColType, Column};
use super::temporal::{self, Read, ReadTime};

/// What the session's variables say about storing values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // one per `sql_mode` flag that matters here
pub(crate) struct Env {
    /// `STRICT_TRANS_TABLES` or `STRICT_ALL_TABLES`.
    pub strict: bool,
    /// The MySQL port: MySQL 8.4's rule where the two servers differ.
    pub mysql: bool,
    /// `NO_ZERO_DATE`.
    pub no_zero_date: bool,
    /// `NO_ZERO_IN_DATE`.
    pub no_zero_in_date: bool,
    /// `ALLOW_INVALID_DATES`.
    pub allow_invalid_dates: bool,
    /// Fractional seconds beyond a column's precision round instead of
    /// being cut (MySQL's rule; MariaDB's `TIME_ROUND_FRACTIONAL`).
    pub round_fraction: bool,
    /// `NO_AUTO_VALUE_ON_ZERO`.
    pub no_auto_value_on_zero: bool,
    /// The session's time zone as seconds east of UTC; `None`: a named
    /// zone, which the engine can't apply yet.
    pub tz: Option<i32>,
    /// `foreign_key_checks = 0`: foreign keys are not checked.
    pub no_fk_checks: bool,
    /// The session's `LAST_INSERT_ID()`.
    pub last_insert_id: u64,
    /// The session's connection id (0: none), which keeps what a
    /// statement's `LAST_INSERT_ID(expr)` sets.
    pub conn: u64,
}

impl Env {
    /// Strict mode with nothing else set, in UTC: what a table's own
    /// constants (its `DEFAULT`s) are read under.
    pub(crate) const STRICT: Env = Env {
        strict: true,
        mysql: false,
        no_zero_date: false,
        no_zero_in_date: false,
        allow_invalid_dates: false,
        round_fraction: false,
        no_auto_value_on_zero: false,
        tz: Some(0),
        no_fk_checks: false,
        last_insert_id: 0,
        conn: 0,
    };

    /// The session's rules from its `sql_mode` (as the backend spells it;
    /// `None`: the port's default) and `time_zone`.
    pub(crate) fn from_vars(mysql: bool, sql_mode: Option<&str>, time_zone: Option<&str>) -> Self {
        let mut e = Env {
            // Both servers' defaults are strict.
            strict: true,
            mysql,
            no_zero_date: mysql,
            no_zero_in_date: mysql,
            allow_invalid_dates: false,
            // The MySQL port's backend mode carries TIME_ROUND_FRACTIONAL.
            round_fraction: mysql,
            no_auto_value_on_zero: false,
            tz: time_zone.map_or(Some(0), zone_offset),
            no_fk_checks: false,
            last_insert_id: 0,
            conn: 0,
        };
        if let Some(m) = sql_mode {
            let has = |f: &str| m.split(',').any(|x| x.trim().eq_ignore_ascii_case(f));
            let traditional = has("TRADITIONAL");
            e.strict = has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES") || traditional;
            e.no_zero_date = has("NO_ZERO_DATE") || traditional;
            e.no_zero_in_date = has("NO_ZERO_IN_DATE") || traditional;
            e.allow_invalid_dates = has("ALLOW_INVALID_DATES");
            e.round_fraction = has("TIME_ROUND_FRACTIONAL");
            e.no_auto_value_on_zero = has("NO_AUTO_VALUE_ON_ZERO");
        }
        e
    }
}

/// A `time_zone` value as seconds east of UTC: `SYSTEM` and `UTC` are 0
/// (the engine's servers run in UTC), `+05:30` is 19,800.
fn zone_offset(z: &str) -> Option<i32> {
    let z = z.trim().trim_matches('\'');
    if z.eq_ignore_ascii_case("SYSTEM") || z.eq_ignore_ascii_case("UTC") || z.is_empty() {
        return Some(0);
    }
    let (sign, rest) = match z.as_bytes().first()? {
        b'+' => (1, &z[1..]),
        b'-' => (-1, &z[1..]),
        _ => return None,
    };
    let (h, m) = rest.split_once(':')?;
    let (h, m) = (h.parse::<i32>().ok()?, m.parse::<i32>().ok()?);
    (h <= 14 && m < 60).then_some(sign * (h * 3600 + m * 60))
}

/// How bad a condition is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    /// Never an error.
    Note,
    /// An error in strict mode.
    Warning,
}

/// A note or warning a statement raised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Condition {
    /// Its level.
    pub level: Level,
    /// The error number.
    pub code: u16,
    /// The message.
    pub message: String,
}

/// One statement's store context: where the conditions go and what the
/// messages name.
pub(crate) struct Store<'a> {
    /// The session's rules.
    pub env: Env,
    /// The logical database name (messages).
    pub db: &'a [u8],
    /// The table's name (messages).
    pub table: &'a str,
    /// The row of the statement being built, from 1.
    pub row: usize,
    /// Notes and warnings raised so far.
    pub conds: &'a mut Vec<Condition>,
}

impl Store<'_> {
    /// A warning: the statement's error in strict mode.
    fn warn(&mut self, code: u16, state: [u8; 5], message: String) -> Result<()> {
        if self.env.strict {
            return Err(SprigError::standard(code, state, message));
        }
        self.conds.push(Condition {
            level: Level::Warning,
            code,
            message,
        });
        Ok(())
    }

    /// A note.
    fn note(&mut self, code: u16, message: String) {
        self.conds.push(Condition {
            level: Level::Note,
            code,
            message,
        });
    }

    /// 1264.
    fn out_of_range(&mut self, c: &Column) -> Result<()> {
        let m = format!(
            "Out of range value for column '{}' at row {}",
            c.name, self.row
        );
        self.warn(1264, *b"22003", m)
    }

    /// 1265 as a warning.
    fn truncated(&mut self, c: &Column) -> Result<()> {
        let m = self.truncated_text(c);
        self.warn(1265, *b"01000", m)
    }

    fn truncated_text(&self, c: &Column) -> String {
        format!("Data truncated for column '{}' at row {}", c.name, self.row)
    }

    /// 1366 for text that is no value of the column's type.
    fn incorrect(&mut self, what: &str, text: &[u8], c: &Column) -> Result<()> {
        let m = format!(
            "Incorrect {what} value: '{}' for column `{}`.`{}`.`{}` at row {}",
            String::from_utf8_lossy(text),
            String::from_utf8_lossy(self.db),
            self.table,
            c.name,
            self.row
        );
        self.warn(1366, *b"22007", m)
    }
}

/// The stored value of a constant of the table's own definition (a
/// `DEFAULT`): `None` when the column can't hold it as it is.
pub(crate) fn constant(c: &Column, d: Datum) -> Option<Datum> {
    let mut conds = Vec::new();
    let mut cx = Store {
        env: Env::STRICT,
        db: b"",
        table: "",
        row: 1,
        conds: &mut conds,
    };
    store(&mut cx, c, d).ok()
}

/// The value column `c` stores for `d`.
///
/// # Errors
/// 1048 for `NULL` in a `NOT NULL` column; in strict mode the value's
/// warning (1264, 1265, 1366, 1406); `ER_SPRIG_NOT_ON_ENGINE` for a
/// conversion the engine doesn't make yet.
pub(crate) fn store(cx: &mut Store<'_>, c: &Column, d: Datum) -> Result<Datum> {
    if d.is_null() {
        if c.nullable {
            return Ok(Datum::Null);
        }
        return Err(SprigError::standard(
            1048,
            *b"23000",
            format!("Column '{}' cannot be null", c.name),
        ));
    }
    match c.ty {
        ColType::Int { .. } => integer(cx, c, &d),
        ColType::Decimal { precision, scale } => decimal(cx, c, &d, precision, scale),
        ColType::Float | ColType::Double => float(cx, c, &d),
        ColType::Date => date(cx, c, &d),
        ColType::DateTime { fsp } => datetime(cx, c, &d, fsp).map(Datum::DateTime),
        ColType::Timestamp { fsp } => timestamp(cx, c, &d, fsp),
        ColType::Time { fsp } => time(cx, c, &d, fsp),
        ColType::Year => year(cx, c, &d),
        ColType::Binary { .. } | ColType::VarBinary { .. } | ColType::Blob { .. } => {
            binary(cx, c, d)
        }
        ColType::Enum { .. } => enumeration(cx, c, &d),
        ColType::Set { .. } => set(cx, c, &d),
        ColType::Bit { bits } => bit(cx, c, &d, bits),
        ColType::Json => json(cx, c, d),
        ColType::Char { .. } | ColType::VarChar { .. } | ColType::Text { .. } => string(cx, c, d),
        ColType::Fbt(f) => fixed(cx, c, f, &d),
    }
}

/// A `UUID`, `INET4` or `INET6` column: text of the type, or a binary
/// string of its stored length. A number or date is refused (4078); other
/// text is 1292 (a warning and `NULL` outside strict mode).
fn fixed(cx: &mut Store<'_>, c: &Column, f: sprig_engine::row::Fbt, d: &Datum) -> Result<Datum> {
    let stored = match d {
        Datum::Str(s) | Datum::Json(s) => f.parse(s),
        Datum::Bytes(b) => f.from_binary(b),
        other => {
            let from = match other {
                Datum::Int(_) | Datum::UInt(_) | Datum::Bit(_) | Datum::Year(_) => "int",
                Datum::Decimal(_) => "decimal",
                Datum::Float(_) | Datum::Double(_) => "double",
                Datum::Date(_) => "date",
                Datum::Time(_) => "time",
                _ => "datetime",
            };
            return Err(SprigError::standard(
                4078,
                *b"HY000",
                format!(
                    "Cannot cast '{from}' as '{}' in assignment of `{}`.`{}`.`{}`",
                    f.name(),
                    String::from_utf8_lossy(cx.db),
                    cx.table,
                    c.name
                ),
            ));
        }
    };
    if let Some(b) = stored {
        return Ok(Datum::Bytes(Bytes::from(b)));
    }
    let text = text_of(d);
    let m = format!(
        "Incorrect {} value: '{}' for column `{}`.`{}`.`{}` at row {}",
        f.name(),
        String::from_utf8_lossy(&text),
        String::from_utf8_lossy(cx.db),
        cx.table,
        c.name,
        cx.row
    );
    cx.warn(1292, *b"22007", m)?;
    Ok(if c.nullable {
        Datum::Null
    } else {
        Datum::Bytes(Bytes::from(vec![0u8; f.bytes()]))
    })
}

/// The range of an integer type.
pub(crate) fn int_range(ty: ColType) -> Option<(i128, i128)> {
    let ColType::Int { bytes, unsigned } = ty else {
        return None;
    };
    let bits = u32::from(bytes) * 8;
    Some(if unsigned {
        (0, (1i128 << bits) - 1)
    } else {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    })
}

fn int_datum(ty: ColType, v: i128) -> Datum {
    match ty {
        // Callers clip to the type's range first.
        ColType::Int { unsigned: true, .. } => Datum::UInt(u64::try_from(v).unwrap_or(u64::MAX)),
        _ => Datum::Int(i64::try_from(v).unwrap_or(i64::MAX)),
    }
}

/// A number read from the front of a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NumPrefix {
    /// A minus sign.
    pub negative: bool,
    /// The digits before and after the point, without the point.
    pub digits: Vec<u8>,
    /// Where the point is, counted from the front of `digits`, after the
    /// exponent moved it.
    pub point: i64,
    /// Whether any digit was read.
    pub any: bool,
    /// What follows the number: nothing, white space only, or other text.
    pub rest: Rest,
}

/// What follows a number in a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rest {
    /// Nothing.
    Empty,
    /// White space only.
    Space,
    /// Other characters.
    Text,
}

/// Reads the number at the front of `s`: white space, a sign, digits with
/// an optional point, an optional exponent.
pub(crate) fn num_prefix(s: &[u8]) -> NumPrefix {
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c);
    let mut i = 0;
    while i < s.len() && ws(s[i]) {
        i += 1;
    }
    let mut negative = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        negative = s[i] == b'-';
        i += 1;
    }
    let mut digits = Vec::new();
    let mut any = false;
    while i < s.len() && s[i].is_ascii_digit() {
        digits.push(s[i] - b'0');
        any = true;
        i += 1;
    }
    let mut point = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    if i < s.len() && s[i] == b'.' {
        let mut j = i + 1;
        while j < s.len() && s[j].is_ascii_digit() {
            digits.push(s[j] - b'0');
            any = true;
            j += 1;
        }
        // A point with no digit on either side is not a number.
        if any {
            i = j;
        }
    }
    if any && i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        let mut j = i + 1;
        let mut eneg = false;
        if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
            eneg = s[j] == b'-';
            j += 1;
        }
        let ds = j;
        let mut e: i64 = 0;
        while j < s.len() && s[j].is_ascii_digit() {
            e = (e * 10 + i64::from(s[j] - b'0')).min(100_000);
            j += 1;
        }
        if j > ds {
            point += if eneg { -e } else { e };
            i = j;
        }
    }
    let rest = if i == s.len() {
        Rest::Empty
    } else if s[i..].iter().all(|b| ws(*b)) {
        Rest::Space
    } else {
        Rest::Text
    };
    NumPrefix {
        negative,
        digits,
        point,
        any,
        rest,
    }
}

impl NumPrefix {
    /// The number rounded to an integer, halves away from zero; beyond
    /// ±10^38 it is ±`i128::MAX`.
    pub(crate) fn rounded(&self) -> i128 {
        let mut v: i128 = 0;
        let n = i64::try_from(self.digits.len()).unwrap_or(i64::MAX);
        let mut k = 0i64;
        while k < self.point {
            let d = if k < n {
                // k is below the digit count.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let d = self.digits[k as usize];
                d
            } else {
                0
            };
            if v == 0 && k >= n {
                // Only zeros so far, and only zeros follow.
                break;
            }
            v = match v.checked_mul(10).and_then(|x| x.checked_add(i128::from(d))) {
                Some(x) if x < 10i128.pow(38) => x,
                _ => {
                    v = i128::MAX;
                    break;
                }
            };
            k += 1;
        }
        if v != i128::MAX && self.point >= 0 && self.point < n {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let next = self.digits[self.point as usize];
            if next >= 5 {
                v += 1;
            }
        }
        if self.negative { -v } else { v }
    }

    /// The number as an exact decimal, when it has at most 65 digits before
    /// the point and its fraction fits 38 digits (more are cut).
    pub(crate) fn decimal(&self) -> Option<Decimal> {
        let n = i64::try_from(self.digits.len()).ok()?;
        let mut int: Vec<u8> = Vec::new();
        let mut frac: Vec<u8> = Vec::new();
        if self.point > 65 + n {
            return None;
        }
        for k in 0..self.point.max(0) {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            int.push(if k < n { self.digits[k as usize] } else { 0 });
        }
        let mut k = self.point;
        while k < n && frac.len() < Decimal::MAX_SCALE {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            frac.push(if k < 0 { 0 } else { self.digits[k as usize] });
            k += 1;
        }
        Decimal::from_parts(self.negative, &int, &frac)
    }
}

/// An exact number (an integer, a decimal, a hexadecimal literal) or a
/// floating-point one (through the shortest digits that read back to it,
/// as the servers convert it) as digits; `None` for other values.
pub(crate) fn number_digits(d: &Datum) -> Option<NumPrefix> {
    let whole = |negative: bool, v: u128| {
        let digits: Vec<u8> = v.to_string().bytes().map(|b| b - b'0').collect();
        NumPrefix {
            negative,
            point: i64::try_from(digits.len()).unwrap_or(i64::MAX),
            digits,
            any: true,
            rest: Rest::Empty,
        }
    };
    Some(match d {
        Datum::Int(v) => whole(*v < 0, u128::from(v.unsigned_abs())),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => whole(false, u128::from(*v)),
        Datum::Enum(v) | Datum::Year(v) => whole(false, u128::from(*v)),
        Datum::Bytes(b) => whole(false, u128::from(bytes_number(b)?)),
        Datum::Decimal(x) => {
            let mut digits = x.int_digits().to_vec();
            let point = i64::try_from(digits.len()).unwrap_or(i64::MAX);
            digits.extend_from_slice(x.frac_digits());
            NumPrefix {
                negative: x.is_negative(),
                digits,
                point,
                any: true,
                rest: Rest::Empty,
            }
        }
        Datum::Float(f) => num_prefix(format!("{f:e}").as_bytes()),
        Datum::Double(f) => num_prefix(format!("{f:e}").as_bytes()),
        _ => return None,
    })
}

impl NumPrefix {
    /// Whether the number is zero.
    pub(crate) fn is_zero(&self) -> bool {
        self.digits.iter().all(|d| *d == 0)
    }

    /// The number rounded half away from zero to `scale` decimals: the
    /// digits before the point (no leading zeros; more than 70 are cut to
    /// 70, which no column holds), the `scale` digits after it, and
    /// whether a digit other than zero was dropped.
    pub(crate) fn to_scale(&self, scale: u8) -> (Vec<u8>, Vec<u8>, bool) {
        let n = i64::try_from(self.digits.len()).unwrap_or(i64::MAX);
        let at = |k: i64| -> u8 {
            if k < 0 || k >= n {
                0
            } else {
                // k is inside the digits.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let d = self.digits[k as usize];
                d
            }
        };
        let mut int: Vec<u8> = (0..self.point.clamp(0, 70)).map(at).collect();
        let end = self.point.saturating_add(i64::from(scale));
        let mut frac: Vec<u8> = (self.point..end).map(at).collect();
        let dropped = (end.max(0)..n).any(|k| at(k) != 0);
        if at(end) >= 5 {
            // Carry through the fraction, then the integer part.
            let mut carry = true;
            for d in frac.iter_mut().rev().chain(int.iter_mut().rev()) {
                if !carry {
                    break;
                }
                if *d == 9 {
                    *d = 0;
                } else {
                    *d += 1;
                    carry = false;
                }
            }
            if carry {
                int.insert(0, 1);
            }
        }
        let lead = int.iter().take_while(|d| **d == 0).count();
        int.drain(..lead);
        (int, frac, dropped)
    }
}

/// A decimal rounded to an integer, halves away from zero.
pub(crate) fn decimal_rounded(d: &Decimal) -> i128 {
    let mut v: i128 = 0;
    for &x in d.int_digits() {
        v = v.saturating_mul(10).saturating_add(i128::from(x));
    }
    if d.frac_digits().first().is_some_and(|x| *x >= 5) {
        v += 1;
    }
    if d.is_negative() { -v } else { v }
}

/// A hexadecimal or bit literal as the number it is in numeric context:
/// its bytes, big-endian. `None` past eight significant bytes.
pub(crate) fn bytes_number(b: &[u8]) -> Option<u64> {
    let lead = b.iter().take_while(|x| **x == 0).count();
    let b = &b[lead..];
    if b.len() > 8 {
        return None;
    }
    Some(b.iter().fold(0u64, |v, x| v << 8 | u64::from(*x)))
}

fn integer(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    let Some((lo, hi)) = int_range(c.ty) else {
        return Err(SprigError::internal("engine: not an integer column"));
    };
    let unsigned = lo == 0;
    // The number, and whether it is out of range whatever it rounds to.
    let (v, mut out): (i128, bool) = match d {
        Datum::Int(v) => (i128::from(*v), false),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => (i128::from(*v), false),
        Datum::Enum(v) | Datum::Year(v) => (i128::from(*v), false),
        // A negative decimal is out of an unsigned column's range even
        // when it rounds to zero.
        Datum::Decimal(x) => (decimal_rounded(x), unsigned && x.is_negative()),
        Datum::Float(f) => double_integer(f64::from(*f), unsigned, cx.env.mysql),
        Datum::Double(f) => double_integer(*f, unsigned, cx.env.mysql),
        Datum::Bytes(b) => match bytes_number(b) {
            Some(n) => (i128::from(n), false),
            None => (i128::MAX, true),
        },
        Datum::Str(s) | Datum::Json(s) => {
            let p = num_prefix(s);
            if !p.any {
                cx.incorrect("integer", s, c)?;
                return Ok(int_datum(c.ty, 0));
            }
            let v = p.rounded();
            match p.rest {
                Rest::Empty => {}
                // MariaDB notes trailing white space; MySQL says nothing.
                Rest::Space => {
                    if !cx.env.mysql && (lo..=hi).contains(&v) {
                        let m = cx.truncated_text(c);
                        cx.note(1265, m);
                    }
                }
                Rest::Text => {
                    cx.truncated(c)?;
                }
            }
            (v, false)
        }
        // A date or time is its digits: YYYYMMDD, hhmmss, YYYYMMDDhhmmss.
        Datum::Date(x) => (date_number(*x), false),
        Datum::DateTime(x) => (
            date_number(x.date) * 1_000_000 + clock_number(x.hour, x.minute, x.second),
            false,
        ),
        Datum::Timestamp(t) => {
            let x = temporal::from_timestamp(*t, zone(cx.env)?);
            (
                date_number(x.date) * 1_000_000 + clock_number(x.hour, x.minute, x.second),
                false,
            )
        }
        Datum::Time(t) => {
            let secs = t.micros() / 1_000_000;
            let abs = secs.unsigned_abs();
            let n = i128::from(abs / 3600 * 10_000 + abs / 60 % 60 * 100 + abs % 60);
            (if secs < 0 { -n } else { n }, false)
        }
        Datum::Null => return Ok(Datum::Null),
    };
    out |= v < lo || v > hi;
    if out {
        cx.out_of_range(c)?;
    }
    Ok(int_datum(c.ty, v.clamp(lo, hi)))
}

/// A double as an integer (halves to even), and whether it is beyond the
/// 64-bit range in the way each server tests it.
fn double_integer(f: f64, unsigned: bool, mysql: bool) -> (i128, bool) {
    const TWO63: f64 = 9_223_372_036_854_775_808.0;
    const TWO64: f64 = 18_446_744_073_709_551_616.0;
    let r = f.round_ties_even();
    if r.is_nan() {
        return (0, true);
    }
    let over = if unsigned {
        r >= TWO64
    } else if mysql {
        r >= TWO63
    } else {
        // MariaDB takes 2^63 itself as the largest value.
        r > TWO63
    };
    // Inside ±2^64 the cast is exact; beyond, it saturates.
    #[allow(clippy::cast_possible_truncation)]
    let v = if r >= TWO64 {
        i128::MAX
    } else if r <= -TWO64 {
        i128::MIN
    } else {
        r as i128
    };
    let v = if !unsigned && !over && r >= TWO63 {
        i128::from(i64::MAX)
    } else {
        v
    };
    (v, over)
}

fn decimal(cx: &mut Store<'_>, c: &Column, d: &Datum, precision: u8, scale: u8) -> Result<Datum> {
    let zero = || Datum::Decimal(Decimal::zero().with_scale(scale).unwrap_or_default());
    let num = match d {
        Datum::Str(s) | Datum::Json(s) => {
            let p = num_prefix(s);
            if !p.any {
                cx.incorrect("decimal", s, c)?;
                return Ok(zero());
            }
            match p.rest {
                Rest::Empty => {}
                // MariaDB notes trailing white space; MySQL says nothing.
                Rest::Space => {
                    if !cx.env.mysql {
                        let m = cx.truncated_text(c);
                        cx.note(1265, m);
                    }
                }
                // A number then other text: MariaDB's warning 1265; MySQL
                // refuses the text in strict mode (1366) and only notes
                // it otherwise.
                Rest::Text if cx.env.mysql => {
                    if cx.env.strict {
                        cx.incorrect("decimal", s, c)?;
                    }
                    let m = cx.truncated_text(c);
                    cx.note(1265, m);
                }
                Rest::Text => cx.truncated(c)?,
            }
            p
        }
        other => match number_digits(other) {
            Some(p) => p,
            None => {
                return Err(super::not_on_engine(
                    "a date or time value for a DECIMAL column",
                ));
            }
        },
    };
    // A negative number is out of an unsigned column's range even when it
    // rounds to zero.
    if c.unsigned && num.negative && !num.is_zero() {
        cx.out_of_range(c)?;
        return Ok(zero());
    }
    let (int, frac, dropped) = num.to_scale(scale);
    let room = usize::from(precision - scale);
    if int.len() > room {
        cx.out_of_range(c)?;
        let nines = vec![9u8; usize::from(precision)];
        let (i, f) = nines.split_at(room);
        return Ok(Datum::Decimal(
            Decimal::from_parts(num.negative, i, f).unwrap_or_default(),
        ));
    }
    if dropped {
        let m = cx.truncated_text(c);
        cx.note(1265, m);
    }
    Decimal::from_parts(num.negative, &int, &frac)
        .map(Datum::Decimal)
        .ok_or_else(|| SprigError::internal("engine: a decimal value doesn't fit its own column"))
}

/// A number rounded to `d` decimals the way a `FLOAT(M,D)` or
/// `DOUBLE(M,D)` column rounds it: the fraction alone, times 10^D, to the
/// nearest whole number (halves to even), in double arithmetic.
fn round_decimals(v: f64, d: u8) -> f64 {
    let scale = 10f64.powi(i32::from(d));
    let whole = v.trunc();
    whole + ((v - whole) * scale).round_ties_even() / scale
}

fn float(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    let mut v: f64 = match d {
        Datum::Str(s) | Datum::Json(s) => {
            let p = num_prefix(s);
            if p.any {
                match p.rest {
                    Rest::Empty => {}
                    Rest::Space => {
                        if !cx.env.mysql {
                            let m = cx.truncated_text(c);
                            cx.note(1265, m);
                        }
                    }
                    Rest::Text => cx.truncated(c)?,
                }
                expr::str_to_f64(s).0
            } else {
                // Text that is no number: MariaDB's 1366, MySQL's 1265.
                if cx.env.mysql {
                    cx.truncated(c)?;
                } else {
                    cx.incorrect("double", s, c)?;
                }
                0.0
            }
        }
        Datum::Date(_) | Datum::Time(_) | Datum::DateTime(_) | Datum::Timestamp(_) => {
            return Err(super::not_on_engine(
                "a date or time value for a FLOAT or DOUBLE column",
            ));
        }
        other => expr::as_f64(other),
    };
    if !v.is_finite() {
        cx.out_of_range(c)?;
        v = if v < 0.0 { f64::MIN } else { f64::MAX };
    }
    if c.unsigned && v < 0.0 {
        cx.out_of_range(c)?;
        v = 0.0;
    }
    if let Some((m, dd)) = c.float_md {
        v = round_decimals(v, dd);
        let max = (10f64.powi(i32::from(m)) - 1.0) / 10f64.powi(i32::from(dd));
        if v.abs() > max {
            cx.out_of_range(c)?;
            v = max.copysign(v);
        }
    }
    if matches!(c.ty, ColType::Float) {
        if v.abs() > f64::from(f32::MAX) {
            cx.out_of_range(c)?;
            v = f64::from(f32::MAX).copysign(v);
        }
        // In range: the cast rounds to the nearest single-precision value.
        #[allow(clippy::cast_possible_truncation)]
        let f = v as f32;
        return Ok(Datum::Float(f));
    }
    Ok(Datum::Double(v))
}

fn date_number(d: Date) -> i128 {
    i128::from(d.year) * 10_000 + i128::from(d.month) * 100 + i128::from(d.day)
}

fn clock_number(h: u8, m: u8, s: u8) -> i128 {
    i128::from(h) * 10_000 + i128::from(m) * 100 + i128::from(s)
}

/// The session's zone offset, for a `TIMESTAMP`.
///
/// # Errors
/// `ER_SPRIG_NOT_ON_ENGINE` under a named time zone.
pub(crate) fn zone(env: Env) -> Result<i32> {
    env.tz.ok_or_else(|| {
        super::not_on_engine(
            "a TIMESTAMP value under a named time zone (use an offset such as '+05:30')",
        )
    })
}

/// 1292 for a value that is no date or time of the column's type: the
/// statement's error in strict mode, a warning (1265) otherwise.
fn not_temporal(cx: &mut Store<'_>, c: &Column, kind: &str, text: &str) -> Result<()> {
    if cx.env.strict {
        return Err(SprigError::standard(
            1292,
            *b"22007",
            format!(
                "Incorrect {kind} value: '{text}' for column `{}`.`{}`.`{}` at row {}",
                String::from_utf8_lossy(cx.db),
                cx.table,
                c.name,
                cx.row
            ),
        ));
    }
    cx.truncated(c)
}

/// What `d` says as a date and time, and its text for messages. `None`:
/// it says none.
fn read_datetime(cx: &Store<'_>, d: &Datum) -> Result<(Option<Read>, String)> {
    let whole = |dt: DateTime, has_time: bool| Read {
        dt,
        has_time,
        seventh: 0,
        long_fraction: false,
        garbage: false,
    };
    Ok(match d {
        Datum::Str(s) | Datum::Json(s) => (
            temporal::datetime_from_str(s),
            String::from_utf8_lossy(s).into_owned(),
        ),
        Datum::Date(x) => (
            Some(whole(DateTime::new(*x, 0, 0, 0, 0), false)),
            x.to_string(),
        ),
        Datum::DateTime(x) => (Some(whole(*x, true)), x.to_string()),
        Datum::Timestamp(t) => {
            let x = temporal::from_timestamp(*t, zone(cx.env)?);
            (Some(whole(x, true)), x.to_string())
        }
        Datum::Time(_) => {
            return Err(super::not_on_engine(
                "a TIME value for a date column (it would take the current date)",
            ));
        }
        other => {
            let text = String::from_utf8_lossy(&text_of(other)).into_owned();
            match number_digits(other) {
                Some(p) => (temporal::datetime_from_number(&p), text),
                None => (None, text),
            }
        }
    })
}

/// The date and time column `c` stores for `d` at precision `fsp`, in the
/// session's zone: the zero value (after the mode's error or warning) for
/// what is no date.
fn datetime(cx: &mut Store<'_>, c: &Column, d: &Datum, fsp: u8) -> Result<DateTime> {
    let kind = if matches!(c.ty, ColType::Date) {
        "date"
    } else {
        "datetime"
    };
    let (r, text) = read_datetime(cx, d)?;
    let Some(mut r) = r.filter(|r| temporal::date_ok(r.dt.date, &cx.env)) else {
        not_temporal(cx, c, kind, &text)?;
        return Ok(DateTime::default());
    };
    if r.garbage {
        // A date followed by other text: refused in strict mode, kept
        // with a warning otherwise.
        not_temporal(cx, c, kind, &text)?;
    }
    // A number that is a date alone, with a fraction: MariaDB drops the
    // fraction with a note; MySQL doesn't read it as a date.
    if !r.has_time && (r.dt.micros != 0 || r.seventh != 0) {
        if cx.env.mysql {
            not_temporal(cx, c, kind, &text)?;
            return Ok(DateTime::default());
        }
        r.dt.micros = 0;
        r.seventh = 0;
        let m = cx.truncated_text(c);
        cx.note(1265, m);
    }
    let round = cx.env.round_fraction;
    let Some(v) = temporal::settle(&r, fsp, round) else {
        not_temporal(cx, c, kind, &text)?;
        return Ok(DateTime::default());
    };
    if !round && r.long_fraction {
        let m = cx.truncated_text(c);
        cx.note(1265, m);
    }
    Ok(v)
}

fn date(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    // Read at full precision: only the date is kept.
    let v = datetime(cx, c, d, 6)?;
    if v.hour != 0 || v.minute != 0 || v.second != 0 || v.micros != 0 {
        // The time of day is cut: a note (MySQL words it as 1292).
        let m = cx.truncated_text(c);
        cx.note(if cx.env.mysql { 1292 } else { 1265 }, m);
    }
    Ok(Datum::Date(v.date))
}

fn timestamp(cx: &mut Store<'_>, c: &Column, d: &Datum, fsp: u8) -> Result<Datum> {
    if let Datum::Timestamp(t) = d {
        let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
        return Ok(Datum::Timestamp(Timestamp::new(
            t.seconds,
            t.micros - t.micros % unit,
        )));
    }
    let tz = zone(cx.env)?;
    let before = cx.conds.len();
    let v = datetime(cx, c, d, fsp)?;
    if v == DateTime::default() && cx.conds.len() > before {
        // No date at all: the zero value, already warned about.
        return Ok(Datum::Timestamp(Timestamp::default()));
    }
    if let Some(t) = temporal::to_timestamp(v, tz, cx.env.mysql) {
        return Ok(Datum::Timestamp(t));
    }
    // A real date outside the type's range.
    if cx.env.strict {
        let (_, text) = read_datetime(cx, d)?;
        not_temporal(cx, c, "datetime", &text)?;
    }
    cx.out_of_range(c)?;
    Ok(Datum::Timestamp(Timestamp::default()))
}

fn time(cx: &mut Store<'_>, c: &Column, d: &Datum, fsp: u8) -> Result<Datum> {
    let of_day = |x: &DateTime| ReadTime {
        micros: (i64::from(x.hour) * 3600 + i64::from(x.minute) * 60 + i64::from(x.second))
            * 1_000_000
            + i64::from(x.micros),
        seventh: 0,
        long_fraction: false,
        garbage: false,
        from_datetime: false,
    };
    let (r, text) = match d {
        Datum::Str(s) | Datum::Json(s) => (
            temporal::time_from_str(s),
            String::from_utf8_lossy(s).into_owned(),
        ),
        Datum::Time(t) => (
            Some(ReadTime {
                micros: t.micros(),
                seventh: 0,
                long_fraction: false,
                garbage: false,
                from_datetime: false,
            }),
            t.to_string(),
        ),
        Datum::DateTime(x) => (Some(of_day(x)), x.to_string()),
        Datum::Timestamp(t) => {
            let x = temporal::from_timestamp(*t, zone(cx.env)?);
            (Some(of_day(&x)), x.to_string())
        }
        Datum::Date(_) => (Some(of_day(&DateTime::default())), String::new()),
        other => {
            let text = String::from_utf8_lossy(&text_of(other)).into_owned();
            match number_digits(other) {
                Some(p) => (temporal::time_from_number(&p), text),
                None => (None, text),
            }
        }
    };
    let Some(r) = r else {
        not_temporal(cx, c, "time", &text)?;
        return Ok(Datum::Time(Time::default()));
    };
    if r.garbage {
        not_temporal(cx, c, "time", &text)?;
    }
    let round = cx.env.round_fraction;
    let mut v = temporal::settle_time(&r, fsp, round);
    let max = temporal::time_max(fsp, cx.env.mysql);
    if v.abs() > max {
        // Beyond 838:59:59: refused in strict mode, the bound otherwise.
        if cx.env.strict {
            not_temporal(cx, c, "time", &text)?;
        }
        cx.out_of_range(c)?;
        v = if v < 0 { -max } else { max };
    }
    if r.from_datetime {
        let m = cx.truncated_text(c);
        cx.note(if cx.env.mysql { 1292 } else { 1265 }, m);
    }
    if !round && r.long_fraction {
        let m = cx.truncated_text(c);
        cx.note(1265, m);
    }
    Ok(Datum::Time(Time::from_micros(v).unwrap_or_default()))
}

/// A constant as a year number, as storing into and comparing with a
/// `YEAR` read it: a decimal rounds, a double is cut, text is its number
/// (`'0'` and `'00'` are 2000). `None` for what is no number at all.
pub(crate) fn year_number(d: &Datum) -> Option<(i128, Rest)> {
    Some(match d {
        Datum::Int(v) => (i128::from(*v), Rest::Empty),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => (i128::from(*v), Rest::Empty),
        Datum::Enum(v) | Datum::Year(v) => (i128::from(*v), Rest::Empty),
        Datum::Bytes(b) => (i128::from(bytes_number(b)?), Rest::Empty),
        Datum::Decimal(x) => (decimal_rounded(x), Rest::Empty),
        // Inside ±2^64 the cast is exact; a year is far inside.
        #[allow(clippy::cast_possible_truncation)]
        Datum::Float(f) => (
            f64::from(*f).trunc().clamp(-1e18, 1e18) as i128,
            Rest::Empty,
        ),
        #[allow(clippy::cast_possible_truncation)]
        Datum::Double(f) => (f.trunc().clamp(-1e18, 1e18) as i128, Rest::Empty),
        Datum::Str(s) | Datum::Json(s) => {
            let p = num_prefix(s);
            if !p.any {
                return None;
            }
            let v = p.rounded();
            // Zero in fewer than four digits is the year 2000.
            let short = p.point < 4 && s.iter().filter(|b| b.is_ascii_digit()).count() < 4;
            (if v == 0 && short { 2000 } else { v }, p.rest)
        }
        Datum::Date(x) => (i128::from(x.year), Rest::Empty),
        Datum::DateTime(x) => (i128::from(x.date.year), Rest::Empty),
        Datum::Timestamp(_) | Datum::Time(_) | Datum::Null => return None,
    })
}

fn year(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    let Some((n, rest)) = year_number(d) else {
        if let Datum::Str(s) | Datum::Json(s) = d {
            cx.incorrect("integer", s, c)?;
            return Ok(Datum::Year(0));
        }
        return Err(super::not_on_engine("a time value for a YEAR column"));
    };
    match rest {
        Rest::Empty => {}
        Rest::Space => {
            if !cx.env.mysql {
                let m = cx.truncated_text(c);
                cx.note(1265, m);
            }
        }
        Rest::Text => cx.truncated(c)?,
    }
    if let Some(y) = temporal::year_from_int(n) {
        Ok(Datum::Year(y))
    } else {
        cx.out_of_range(c)?;
        Ok(Datum::Year(0))
    }
}

/// 1406 in strict mode, a warning (1265) otherwise.
fn too_long(cx: &mut Store<'_>, c: &Column) -> Result<()> {
    if cx.env.strict {
        return Err(SprigError::standard(
            1406,
            *b"22001",
            format!("Data too long for column '{}' at row {}", c.name, cx.row),
        ));
    }
    cx.truncated(c)
}

fn binary(cx: &mut Store<'_>, c: &Column, d: Datum) -> Result<Datum> {
    let b = match d {
        Datum::Str(s) | Datum::Bytes(s) | Datum::Json(s) => s,
        Datum::Timestamp(t) => Bytes::from(temporal::from_timestamp(t, zone(cx.env)?).to_string()),
        other => text_of(&other),
    };
    let max = match c.ty {
        ColType::Binary { bytes } => usize::from(bytes),
        ColType::VarBinary { bytes } => bytes as usize,
        ColType::Blob { size } => usize::try_from(size.max_bytes()).unwrap_or(usize::MAX),
        _ => return Err(SprigError::internal("engine: not a byte string column")),
    };
    let b = if b.len() > max {
        too_long(cx, c)?;
        b.slice(..max)
    } else {
        b
    };
    if matches!(c.ty, ColType::Binary { .. }) && b.len() < max {
        // BINARY(n) is its n bytes: zero bytes fill it.
        let mut v = b.to_vec();
        v.resize(max, 0);
        return Ok(Datum::Bytes(Bytes::from(v)));
    }
    Ok(Datum::Bytes(b))
}

/// A statement's text in the column's character set, for matching
/// members; `None` when the set can't hold it (no member is equal then).
fn member_text(c: &Column, s: &Bytes) -> Option<Bytes> {
    let charset = c.collation.map_or(Charset::Utf8mb4, Collation::charset);
    to_charset(s, charset, c.mb3).ok()
}

/// The member of `c` (from 0) equal to `s` under the column's collation,
/// trailing spaces aside.
fn member_of(c: &Column, s: &[u8]) -> Option<usize> {
    let mut end = s.len();
    while end > 0 && s[end - 1] == b' ' {
        end -= 1;
    }
    let s = &s[..end];
    c.members.iter().position(|m| match c.collation {
        Some(coll) => coll.compare(m, s) == std::cmp::Ordering::Equal,
        None => m[..] == *s,
    })
}

/// A whole number for an `ENUM` index or a `SET` mask: decimals and
/// doubles are cut.
fn whole_number(d: &Datum) -> Option<i128> {
    Some(match d {
        Datum::Int(v) => i128::from(*v),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => i128::from(*v),
        Datum::Enum(v) | Datum::Year(v) => i128::from(*v),
        Datum::Bytes(b) => i128::from(bytes_number(b)?),
        Datum::Decimal(x) => {
            let mut v: i128 = 0;
            for &digit in x.int_digits() {
                v = v.saturating_mul(10).saturating_add(i128::from(digit));
            }
            if x.is_negative() { -v } else { v }
        }
        // Clamped far inside i128 first.
        #[allow(clippy::cast_possible_truncation)]
        Datum::Float(f) => f64::from(*f).trunc().clamp(-1e30, 1e30) as i128,
        #[allow(clippy::cast_possible_truncation)]
        Datum::Double(f) => f.trunc().clamp(-1e30, 1e30) as i128,
        _ => return None,
    })
}

fn enumeration(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    let count = c.members.len();
    let index = match d {
        Datum::Str(s) | Datum::Json(s) => {
            let by_name = member_text(c, s).and_then(|t| member_of(c, &t));
            match by_name {
                Some(i) => Some(i + 1),
                // A short string of digits that names no member is an
                // index.
                None if s.len() < 6 && !s.is_empty() && s.iter().all(u8::is_ascii_digit) => {
                    std::str::from_utf8(s)
                        .ok()
                        .and_then(|t| t.parse::<usize>().ok())
                        .filter(|n| (1..=count).contains(n))
                }
                None => None,
            }
        }
        other => match whole_number(other) {
            Some(n) => usize::try_from(n).ok().filter(|n| (1..=count).contains(n)),
            None => {
                return Err(super::not_on_engine(
                    "a date or time value for an ENUM column",
                ));
            }
        },
    };
    if let Some(i) = index {
        return Ok(Datum::Enum(u16::try_from(i).unwrap_or(0)));
    }
    cx.truncated(c)?;
    Ok(Datum::Enum(0))
}

fn set(cx: &mut Store<'_>, c: &Column, d: &Datum) -> Result<Datum> {
    let count = c.members.len();
    let all: u64 = if count >= 64 {
        u64::MAX
    } else {
        (1u64 << count) - 1
    };
    let from_number = |cx: &mut Store<'_>, n: i128| -> Result<Datum> {
        // The low 64 bits, as the servers take a negative number.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let mask = n as u64;
        if mask & !all != 0 {
            cx.truncated(c)?;
        }
        Ok(Datum::Set(mask & all))
    };
    match d {
        Datum::Str(s) | Datum::Json(s) => {
            if s.is_empty() {
                return Ok(Datum::Set(0));
            }
            let mut mask = 0u64;
            let mut unknown = false;
            match member_text(c, s) {
                Some(t) => {
                    for part in t.split(|b| *b == b',') {
                        match member_of(c, part) {
                            Some(i) if i < 64 => mask |= 1 << i,
                            _ => unknown = true,
                        }
                    }
                }
                None => unknown = true,
            }
            if unknown && mask == 0 && s.len() < 22 && s.iter().all(u8::is_ascii_digit) {
                // Text that names no member and is a number: the mask.
                if let Some(n) = std::str::from_utf8(s)
                    .ok()
                    .and_then(|t| t.parse::<i128>().ok())
                {
                    return from_number(cx, n);
                }
            }
            if unknown {
                cx.truncated(c)?;
            }
            Ok(Datum::Set(mask))
        }
        other => match whole_number(other) {
            Some(n) => from_number(cx, n),
            None => Err(super::not_on_engine(
                "a date or time value for a SET column",
            )),
        },
    }
}

fn bit(cx: &mut Store<'_>, c: &Column, d: &Datum, bits: u8) -> Result<Datum> {
    let all: u64 = if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    // The value as 64 bits, or `None` when it needs more.
    let v: Option<u64> = match d {
        // A string is its bytes, not its digits.
        Datum::Str(s) | Datum::Bytes(s) | Datum::Json(s) => bytes_number(s),
        Datum::Int(v) => u64::try_from(*v).ok(),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => Some(*v),
        Datum::Enum(v) | Datum::Year(v) => Some(u64::from(*v)),
        Datum::Decimal(x) => u64::try_from(decimal_rounded(x)).ok(),
        Datum::Float(_) | Datum::Double(_) => {
            let (n, over) = double_integer(expr::as_f64(d), true, cx.env.mysql);
            if over { None } else { u64::try_from(n).ok() }
        }
        Datum::Date(_) | Datum::Time(_) | Datum::DateTime(_) | Datum::Timestamp(_) => {
            return Err(super::not_on_engine(
                "a date or time value for a BIT column",
            ));
        }
        Datum::Null => return Ok(Datum::Null),
    };
    match v {
        Some(v) if v & !all == 0 => Ok(Datum::Bit(v)),
        _ => {
            // Too many bits: 1406 in strict mode, all ones with 1264
            // otherwise.
            if cx.env.strict {
                too_long(cx, c)?;
            }
            cx.out_of_range(c)?;
            Ok(Datum::Bit(all))
        }
    }
}

fn json(cx: &mut Store<'_>, c: &Column, d: Datum) -> Result<Datum> {
    let text = match d {
        Datum::Str(s) | Datum::Json(s) | Datum::Bytes(s) => s,
        Datum::Timestamp(_) | Datum::Date(_) | Datum::Time(_) | Datum::DateTime(_) => {
            return Err(super::not_on_engine(
                "a date or time value for a JSON column",
            ));
        }
        other => text_of(&other),
    };
    // The document is kept as it was written (MariaDB's JSON); it only
    // has to be one.
    if serde_json::from_slice::<serde::de::IgnoredAny>(&text).is_err() {
        return Err(SprigError::standard(
            4025,
            *b"23000",
            format!(
                "CONSTRAINT `{}.{}` failed for `{}`.`{}`",
                cx.table,
                c.name,
                String::from_utf8_lossy(cx.db),
                cx.table
            ),
        ));
    }
    Ok(Datum::Json(text))
}

/// A value's text as a string column stores it.
pub(crate) fn text_of(d: &Datum) -> Bytes {
    match d {
        Datum::Null => Bytes::from_static(b"NULL"),
        Datum::Int(v) => Bytes::from(v.to_string()),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => Bytes::from(v.to_string()),
        Datum::Decimal(v) => Bytes::from(v.to_string()),
        Datum::Float(v) => Bytes::from(expr::float_text(*v)),
        Datum::Double(v) => Bytes::from(expr::double_text(*v)),
        Datum::Str(s) | Datum::Bytes(s) | Datum::Json(s) => s.clone(),
        Datum::Date(v) => Bytes::from(v.to_string()),
        Datum::Time(v) => Bytes::from(v.to_string()),
        Datum::DateTime(v) => Bytes::from(v.to_string()),
        Datum::Timestamp(v) => Bytes::from(v.seconds.to_string()),
        Datum::Year(v) => Bytes::from(format!("{v:04}")),
        Datum::Enum(v) => Bytes::from(v.to_string()),
    }
}

/// The Windows-1252 byte of `ch` as both servers' `latin1` has it: the
/// five code points Windows-1252 leaves undefined map to themselves.
pub(crate) fn latin1_byte(ch: char) -> Option<u8> {
    let c = u32::from(ch);
    if c < 0x80 || (0xA0..=0xFF).contains(&c) {
        return u8::try_from(c).ok();
    }
    if let Some(i) = CP1252_HIGH.iter().position(|x| *x == c) {
        return u8::try_from(0x80 + i).ok();
    }
    None
}

/// The character of `latin1` byte `b`.
pub(crate) fn latin1_char(b: u8) -> char {
    if (0x80..0xA0).contains(&b) {
        char::from_u32(CP1252_HIGH[usize::from(b - 0x80)]).unwrap_or('?')
    } else {
        char::from(b)
    }
}

/// Bytes 0x80 to 0x9F of `latin1`.
const CP1252_HIGH: [u32; 32] = [
    0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160, 0x2039,
    0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
];

/// A statement's string (UTF-8) in the column's character set. A
/// character the set doesn't have becomes `?` and the bytes from it on are
/// returned for the message.
fn to_charset(
    s: &Bytes,
    charset: Charset,
    mb3: bool,
) -> std::result::Result<Bytes, (Bytes, usize)> {
    match charset {
        Charset::Binary => Ok(s.clone()),
        Charset::Utf8mb4 => match std::str::from_utf8(s) {
            Ok(t) if !mb3 || t.chars().all(|c| u32::from(c) < 0x1_0000) => Ok(s.clone()),
            Ok(t) => {
                let mut out = String::with_capacity(s.len());
                let mut first = None;
                for (i, c) in t.char_indices() {
                    if u32::from(c) < 0x1_0000 {
                        out.push(c);
                    } else {
                        first.get_or_insert(i);
                        out.push('?');
                    }
                }
                Err((Bytes::from(out), first.unwrap_or(0)))
            }
            Err(e) => {
                let good = e.valid_up_to();
                Err((s.slice(..good), good))
            }
        },
        Charset::Ascii | Charset::Latin1 => {
            let t = match std::str::from_utf8(s) {
                Ok(t) => t,
                Err(e) => {
                    let good = e.valid_up_to();
                    return Err((s.slice(..good), good));
                }
            };
            if t.is_ascii() {
                return Ok(s.clone());
            }
            let mut out = Vec::with_capacity(s.len());
            let mut first = None;
            for (i, c) in t.char_indices() {
                let b = if charset == Charset::Ascii {
                    u8::try_from(u32::from(c)).ok().filter(u8::is_ascii)
                } else {
                    latin1_byte(c)
                };
                if let Some(b) = b {
                    out.push(b);
                } else {
                    first.get_or_insert(i);
                    out.push(b'?');
                }
            }
            match first {
                None => Ok(Bytes::from(out)),
                Some(i) => Err((Bytes::from(out), i)),
            }
        }
    }
}

/// The bytes of `s` from `at` as the servers print them in 1366: the
/// first six, bytes outside ASCII as `\xHH`, then `...` when more follow.
fn hex_escapes(s: &[u8], at: usize) -> String {
    use std::fmt::Write as _;
    let rest = s.get(at..).unwrap_or(&[]);
    let mut m = String::new();
    for b in rest.iter().take(6) {
        if b.is_ascii_graphic() || *b == b' ' {
            m.push(char::from(*b));
        } else {
            let _ = write!(m, "\\x{b:02X}");
        }
    }
    if rest.len() > 6 {
        m.push_str("...");
    }
    m
}

fn string(cx: &mut Store<'_>, c: &Column, d: Datum) -> Result<Datum> {
    let Some(coll) = c.collation else {
        return Err(SprigError::internal(
            "engine: a string column without a collation",
        ));
    };
    let charset = coll.charset();
    let s = match d {
        Datum::Str(s) => s,
        Datum::Timestamp(t) => {
            let x = temporal::from_timestamp(t, zone(cx.env)?);
            Bytes::from(x.to_string())
        }
        other => text_of(&other),
    };
    let s = match to_charset(&s, charset, c.mb3) {
        Ok(b) => b,
        Err((replaced, at)) => {
            let m = format!(
                "Incorrect string value: '{}' for column `{}`.`{}`.`{}` at row {}",
                hex_escapes(&s, at),
                String::from_utf8_lossy(cx.db),
                cx.table,
                c.name,
                cx.row
            );
            cx.warn(1366, *b"22007", m)?;
            replaced
        }
    };
    let too_long = |cx: &mut Store<'_>| -> Result<()> {
        if cx.env.strict {
            return Err(SprigError::standard(
                1406,
                *b"22001",
                format!("Data too long for column '{}' at row {}", c.name, cx.row),
            ));
        }
        cx.truncated(c)
    };
    match c.ty {
        ColType::Char { chars, .. } => {
            // CHAR drops trailing spaces: they are never "too long".
            let mut end = s.len();
            while end > 0 && s[end - 1] == b' ' {
                end -= 1;
            }
            let b = s.slice(..end);
            let keep = charset.char_prefix(&b, usize::from(chars)).len();
            if keep < b.len() {
                too_long(cx)?;
                let mut end = keep;
                while end > 0 && b[end - 1] == b' ' {
                    end -= 1;
                }
                return Ok(Datum::Str(b.slice(..end)));
            }
            Ok(Datum::Str(b))
        }
        ColType::VarChar { chars, .. } => {
            let keep = charset.char_prefix(&s, chars as usize).len();
            if keep < s.len() {
                if s[keep..].iter().all(|x| *x == b' ') {
                    // Only spaces cut: a note.
                    let m = cx.truncated_text(c);
                    cx.note(1265, m);
                } else {
                    too_long(cx)?;
                }
                return Ok(Datum::Str(s.slice(..keep)));
            }
            Ok(Datum::Str(s))
        }
        ColType::Text { size, .. } => {
            let max = usize::try_from(size.max_bytes()).unwrap_or(usize::MAX);
            if s.len() > max {
                // Cut at a character boundary.
                let mut keep = max;
                if charset == Charset::Utf8mb4 {
                    while keep > 0 && s[keep] & 0xC0 == 0x80 {
                        keep -= 1;
                    }
                }
                if s[keep..].iter().all(|x| *x == b' ') {
                    let m = cx.truncated_text(c);
                    cx.note(1265, m);
                } else {
                    too_long(cx)?;
                }
                return Ok(Datum::Str(s.slice(..keep)));
            }
            Ok(Datum::Str(s))
        }
        _ => Err(SprigError::internal("engine: not a string column")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_at_the_front_of_strings() {
        let r = |s: &str| num_prefix(s.as_bytes()).rounded();
        assert_eq!(r("12"), 12);
        assert_eq!(r(" 13 "), 13);
        assert_eq!(r("14.6"), 15);
        assert_eq!(r("1e2"), 100);
        assert_eq!(r("1.5"), 2);
        assert_eq!(r("2.5"), 3);
        assert_eq!(r("-3.5"), -4);
        assert_eq!(r("-0.4"), 0);
        assert_eq!(r("-0.5"), -1);
        assert_eq!(r(".5"), 1);
        assert_eq!(r("5."), 5);
        assert_eq!(r("1.5e1"), 15);
        assert_eq!(r("5e-1"), 1);
        assert_eq!(r("1e-1"), 0);
        assert_eq!(r("0.9999999999999999999999999"), 1);
        assert_eq!(r("18446744073709551615.4"), 18_446_744_073_709_551_615);
        assert_eq!(r("1e400"), i128::MAX);
        assert_eq!(r("0x10"), 0);
        let p = num_prefix(b"12abc");
        assert_eq!((p.any, p.rest), (true, Rest::Text));
        assert_eq!(num_prefix(b"13  ").rest, Rest::Space);
        assert!(!num_prefix(b"abc").any);
        assert!(!num_prefix(b".").any);
        assert!(!num_prefix(b"-").any);
        assert!(!num_prefix(b"e5").any);
        // `1e` is 1 and then text.
        assert_eq!(num_prefix(b"1e").rest, Rest::Text);
        assert_eq!(
            num_prefix(b"-12.50").decimal().unwrap().to_string(),
            "-12.50"
        );
        assert_eq!(num_prefix(b"1.5e3").decimal().unwrap().to_string(), "1500");
        assert_eq!(num_prefix(b"15e-3").decimal().unwrap().to_string(), "0.015");
    }

    #[test]
    fn doubles_round_to_even_and_meet_the_bounds_as_each_server_does() {
        assert_eq!(double_integer(2.5, false, false), (2, false));
        assert_eq!(double_integer(3.5, false, false), (4, false));
        assert_eq!(double_integer(-0.4, true, false), (0, false));
        // 2^63: MariaDB stores the largest BIGINT, MySQL refuses.
        let two63 = 9_223_372_036_854_775_808.0;
        assert_eq!(
            double_integer(two63, false, false),
            (i128::from(i64::MAX), false)
        );
        assert!(double_integer(two63, false, true).1);
        assert!(double_integer(1e30, false, false).1);
        assert!(double_integer(18_446_744_073_709_551_616.0, true, false).1);
    }

    #[test]
    fn latin1_is_windows_1252() {
        assert_eq!(latin1_byte('é'), Some(0xE9));
        assert_eq!(latin1_byte('€'), Some(0x80));
        assert_eq!(latin1_byte('\u{81}'), Some(0x81));
        assert_eq!(latin1_byte('中'), None);
        assert_eq!(latin1_char(0x80), '€');
        assert_eq!(latin1_char(0xE9), 'é');
        for b in 0..=255u8 {
            assert_eq!(latin1_byte(latin1_char(b)), Some(b));
        }
    }

    #[test]
    fn hexadecimal_literals_are_numbers_in_numeric_context() {
        assert_eq!(bytes_number(&[0x31, 0x32]), Some(12594));
        assert_eq!(bytes_number(&[0; 12]), Some(0));
        assert_eq!(
            bytes_number(&[1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
            None
        );
    }
}
