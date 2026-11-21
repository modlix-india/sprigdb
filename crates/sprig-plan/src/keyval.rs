//! Where a constant falls among the values a key column can hold.
//!
//! A comparison can drive a key lookup only when it is carried out in the
//! column's own type and collation: then "the rows the predicate accepts"
//! is a run of the key. This module decides that for a column type and a
//! constant (or, for a join, the type of the other side), and gives the
//! constant as a value of the column: exactly, or as the two neighbouring
//! column values it lies between (`int_col < 2.5` is `int_col <= 2`).

use bytes::Bytes;
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, Value};
use sprig_exec::types::Derivation;
use sprig_exec::{ExprType, Kind};

/// A constant as seen from a key column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyVal {
    /// The constant is this value of the column.
    Exact(Value),
    /// No value of the column equals the constant; it lies between the
    /// greatest column value below it and the least above it (`None` when
    /// the column has no value on that side).
    Between(Option<Value>, Option<Value>),
    /// The constant is `NULL`: no comparison with it is true.
    Null,
    /// The comparison isn't done in the column's type: the key can't
    /// answer it.
    Unusable,
}

/// Where an exact number falls on the integer line.
enum Num {
    Int(i128),
    /// Strictly between `floor` and `floor + 1`.
    Frac(i128),
    /// Beyond every integer column's range, on this side.
    Huge(bool),
}

fn decimal_num(d: &Decimal) -> Num {
    let neg = d.is_negative();
    if d.int_digits().len() > 30 {
        return Num::Huge(neg);
    }
    let mut n: i128 = 0;
    for &x in d.int_digits() {
        n = n * 10 + i128::from(x);
    }
    let frac = d.frac_digits().iter().any(|&x| x != 0);
    match (neg, frac) {
        (false, false) => Num::Int(n),
        (true, false) => Num::Int(-n),
        (false, true) => Num::Frac(n),
        (true, true) => Num::Frac(-n - 1),
    }
}

fn int_key(bytes: u8, unsigned: bool, v: &Value) -> KeyVal {
    let (min, max): (i128, i128) = if unsigned {
        (0, (1i128 << (u32::from(bytes) * 8)) - 1)
    } else {
        let half = 1i128 << (u32::from(bytes) * 8 - 1);
        (-half, half - 1)
    };
    let num = match v {
        Value::Int(i) => Num::Int(i128::from(*i)),
        Value::UInt(u) => Num::Int(i128::from(*u)),
        Value::Decimal(d) => decimal_num(d),
        Value::Double(_) | Value::Float(_) => {
            let x = match v {
                Value::Double(x) => *x,
                Value::Float(f) => f64::from(*f),
                _ => 0.0,
            };
            if !x.is_finite() {
                return KeyVal::Unusable;
            }
            // Past 2^53 a BIGINT column's values lose digits when the
            // servers turn them into doubles for this comparison.
            if x.abs() >= 9_007_199_254_740_992.0 {
                if bytes == 8 {
                    return KeyVal::Unusable;
                }
                Num::Huge(x < 0.0)
            } else if x.fract() == 0.0 {
                Num::Int(x as i128)
            } else {
                Num::Frac(x.floor() as i128)
            }
        }
        _ => return KeyVal::Unusable,
    };
    let val = |n: i128| {
        if unsigned {
            Value::UInt(n as u64)
        } else {
            Value::Int(n as i64)
        }
    };
    match num {
        Num::Int(n) if n < min => KeyVal::Between(None, Some(val(min))),
        Num::Int(n) if n > max => KeyVal::Between(Some(val(max)), None),
        Num::Int(n) => KeyVal::Exact(val(n)),
        Num::Frac(f) if f < min => KeyVal::Between(None, Some(val(min))),
        Num::Frac(f) if f >= max => KeyVal::Between(Some(val(max)), None),
        Num::Frac(f) => KeyVal::Between(Some(val(f)), Some(val(f + 1))),
        Num::Huge(true) => KeyVal::Between(None, Some(val(min))),
        Num::Huge(false) => KeyVal::Between(Some(val(max)), None),
    }
}

/// One unit in the last place added to a magnitude given as digits.
/// `None` when the integer part would grow past `int_len` digits.
fn bump(int: &[u8], frac: &[u8], int_len: usize) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut digits: Vec<u8> = vec![0; int_len - int.len()];
    digits.extend_from_slice(int);
    digits.extend_from_slice(frac);
    let mut i = digits.len();
    loop {
        if i == 0 {
            return None;
        }
        i -= 1;
        if digits[i] == 9 {
            digits[i] = 0;
        } else {
            digits[i] += 1;
            break;
        }
    }
    let frac = digits.split_off(int_len);
    Some((digits, frac))
}

fn decimal_key(precision: u8, scale: u8, v: &Value) -> KeyVal {
    let d = match v {
        Value::Int(i) => Decimal::from_i64(*i),
        Value::UInt(u) => Decimal::from_u64(*u),
        Value::Decimal(d) => d.clone(),
        _ => return KeyVal::Unusable,
    };
    let (p, s) = (usize::from(precision), usize::from(scale));
    let int_len = p - s;
    let neg = d.is_negative();
    let nines = |neg: bool| {
        Decimal::from_parts(neg, &vec![9; int_len], &vec![9; s]).map_or(Value::Null, Value::Decimal)
    };
    if d.int_digits().len() > int_len {
        return if neg {
            KeyVal::Between(None, Some(nines(true)))
        } else {
            KeyVal::Between(Some(nines(false)), None)
        };
    }
    let mut frac: Vec<u8> = d.frac_digits().iter().copied().take(s).collect();
    frac.resize(s, 0);
    let extra = d.frac_digits().iter().skip(s).any(|&x| x != 0);
    let make = |neg: bool, int: &[u8], frac: &[u8]| {
        Decimal::from_parts(neg, int, frac).map(Value::Decimal)
    };
    // The constant cut to the column's scale: toward zero.
    let Some(cut) = make(neg, d.int_digits(), &frac) else {
        return KeyVal::Unusable;
    };
    if !extra {
        return KeyVal::Exact(cut);
    }
    let away = bump(d.int_digits(), &frac, int_len).and_then(|(i, f)| make(neg, &i, &f));
    if neg {
        KeyVal::Between(away, Some(cut))
    } else {
        KeyVal::Between(Some(cut), away)
    }
}

fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn digits(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut n = 0u32;
    for b in s {
        n = n * 10 + u32::from(b - b'0');
    }
    Some(n)
}

/// `YYYY-MM-DD`, a real calendar day from year 1000 on. Anything looser
/// is left to the servers' own conversion rules, row by row.
fn strict_date(s: &[u8]) -> Option<Date> {
    if s.len() != 10 || s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let (y, m, d) = (digits(&s[..4])?, digits(&s[5..7])?, digits(&s[8..])?);
    let (y, m, d) = (y as u16, m as u8, d as u8);
    (y >= 1000 && (1..=12).contains(&m) && d >= 1 && d <= days_in_month(y, m))
        .then(|| Date::new(y, m, d))
}

/// `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS[.ffffff]` with at most `fsp`
/// fraction digits that aren't zero.
fn strict_datetime(s: &[u8], fsp: u8) -> Option<DateTime> {
    if s.len() == 10 {
        return strict_date(s).map(|d| DateTime::new(d, 0, 0, 0, 0));
    }
    if s.len() < 19 || s[10] != b' ' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let date = strict_date(&s[..10])?;
    let (h, mi, sec) = (
        digits(&s[11..13])?,
        digits(&s[14..16])?,
        digits(&s[17..19])?,
    );
    if h > 23 || mi > 59 || sec > 59 {
        return None;
    }
    let mut micros = 0u32;
    if s.len() > 19 {
        let frac = &s[20..];
        if s[19] != b'.' || frac.is_empty() || frac.len() > 6 {
            return None;
        }
        micros = digits(frac)? * 10u32.pow(6 - frac.len() as u32);
    }
    if micros % 10u32.pow(6 - u32::from(fsp.min(6))) != 0 {
        return None;
    }
    Some(DateTime::new(date, h as u8, mi as u8, sec as u8, micros))
}

fn is_date(d: Date) -> bool {
    d.year >= 1000 && d.month >= 1 && d.day >= 1 && d.day <= days_in_month(d.year, d.month)
}

/// Whether a string of type `ty` meets a column of collation `column` in
/// the column's collation.
pub(crate) fn collation_holds(column: Collation, ty: &ExprType) -> bool {
    if ty.kind != Kind::Str {
        return false;
    }
    if ty.coll == column {
        return true;
    }
    ty.deriv > Derivation::Implicit
        && ty.coll != Collation::Binary
        && column != Collation::Binary
        && ty.coll.charset() == Charset::Utf8mb4
}

/// A string that is plainly a number: an optional minus, digits, an
/// optional point and digits, at most 15 digits in all. Such a string
/// means the same number whether a server reads it as a decimal (MariaDB)
/// or as a double (MySQL) when it meets an integer column. Anything else
/// (spaces, exponents, trailing text, longer numbers) is left to the
/// servers' own conversion, row by row.
fn plain_number(s: &[u8]) -> Option<Decimal> {
    let text = std::str::from_utf8(s).ok()?;
    let body = text.strip_prefix('-').unwrap_or(text);
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let digits = |t: &str| t.bytes().all(|b| b.is_ascii_digit());
    if int.is_empty()
        || !digits(int)
        || !digits(frac)
        || (body.contains('.') && frac.is_empty())
        || int.len() + frac.len() > 15
    {
        return None;
    }
    Decimal::parse(text)
}

/// The constant `v`, of static type `ty`, as a value of a column of type
/// `column`.
pub(crate) fn key_value(column: &ColumnType, v: &Value, ty: &ExprType) -> KeyVal {
    if v.is_null() {
        return KeyVal::Null;
    }
    match *column {
        ColumnType::Int { bytes, unsigned } => {
            if ty.kind.is_numeric() && !ty.hex {
                int_key(bytes, unsigned, v)
            } else if let (Value::Str(s), Kind::Str, false) = (v, ty.kind, ty.hex) {
                // `int_col = '5'`: both servers compare numbers.
                match plain_number(s) {
                    Some(d) if ty.coll != Collation::Binary => {
                        int_key(bytes, unsigned, &Value::Decimal(d))
                    }
                    _ => KeyVal::Unusable,
                }
            } else {
                KeyVal::Unusable
            }
        }
        ColumnType::Decimal { precision, scale } => {
            if ty.kind.is_exact() && !ty.hex {
                decimal_key(precision, scale, v)
            } else {
                KeyVal::Unusable
            }
        }
        ColumnType::Double => {
            if !ty.kind.is_numeric() || ty.hex {
                return KeyVal::Unusable;
            }
            let x = match v {
                Value::Int(i) => *i as f64,
                Value::UInt(u) => *u as f64,
                Value::Decimal(d) => match d.to_string().parse::<f64>() {
                    Ok(x) => x,
                    Err(_) => return KeyVal::Unusable,
                },
                Value::Double(x) => *x,
                Value::Float(f) => f64::from(*f),
                _ => return KeyVal::Unusable,
            };
            if x.is_nan() {
                return KeyVal::Unusable;
            }
            KeyVal::Exact(Value::Double(if x == 0.0 { 0.0 } else { x }))
        }
        ColumnType::Char { collation, .. }
        | ColumnType::VarChar { collation, .. }
        | ColumnType::Text { collation, .. } => match v {
            Value::Str(s) if collation_holds(collation, ty) => {
                if collation.charset() != Charset::Utf8mb4 && !s.is_ascii() {
                    return KeyVal::Unusable;
                }
                KeyVal::Exact(Value::Str(s.clone()))
            }
            _ => KeyVal::Unusable,
        },
        ColumnType::Binary { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. } => {
            match v {
                Value::Str(s) | Value::Bytes(s) if ty.kind == Kind::Str && !ty.hex => {
                    KeyVal::Exact(Value::Bytes(s.clone()))
                }
                _ => KeyVal::Unusable,
            }
        }
        ColumnType::Date => match v {
            Value::Date(d) if is_date(*d) => KeyVal::Exact(Value::Date(*d)),
            Value::Str(s) if ty.kind == Kind::Str && ty.coll != Collation::Binary => {
                strict_date(s).map_or(KeyVal::Unusable, |d| KeyVal::Exact(Value::Date(d)))
            }
            _ => KeyVal::Unusable,
        },
        ColumnType::DateTime { fsp } => match v {
            Value::Date(d) if is_date(*d) => {
                KeyVal::Exact(Value::DateTime(DateTime::new(*d, 0, 0, 0, 0)))
            }
            Value::DateTime(d)
                if is_date(d.date) && d.micros % 10u32.pow(6 - u32::from(fsp.min(6))) == 0 =>
            {
                KeyVal::Exact(Value::DateTime(*d))
            }
            Value::Str(s) if ty.kind == Kind::Str && ty.coll != Collation::Binary => {
                strict_datetime(s, fsp)
                    .map_or(KeyVal::Unusable, |d| KeyVal::Exact(Value::DateTime(d)))
            }
            _ => KeyVal::Unusable,
        },
        _ => KeyVal::Unusable,
    }
}

/// Whether an equality between a column of type `column` and an expression
/// of static type `ty` is carried out in the column's type, so that the
/// expression's value (through [`key_value`]) can be looked up in a key.
pub(crate) fn key_usable(column: &ColumnType, ty: &ExprType) -> bool {
    match *column {
        ColumnType::Int { .. } | ColumnType::Decimal { .. } => ty.kind.is_exact() && !ty.hex,
        ColumnType::Double => ty.kind.is_numeric() && !ty.hex,
        ColumnType::Char { collation, .. }
        | ColumnType::VarChar { collation, .. }
        | ColumnType::Text { collation, .. } => collation_holds(collation, ty),
        ColumnType::Binary { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. } => {
            ty.kind == Kind::Str && !ty.hex
        }
        ColumnType::Date => ty.kind == Kind::Date,
        ColumnType::DateTime { .. } => matches!(ty.kind, Kind::Date | Kind::DateTime),
        _ => false,
    }
}

/// Whether [`key_value`] turns every value of an expression of static
/// type `ty` into the column's own value for it (or finds there is none):
/// a lookup by such an expression needs no second check of the equality.
pub(crate) fn lookup_exact(column: &ColumnType, ty: &ExprType) -> bool {
    match *column {
        ColumnType::Int { .. } | ColumnType::Decimal { .. } => {
            matches!(ty.kind, Kind::Int | Kind::Decimal) && !ty.hex
        }
        ColumnType::Char { collation, .. }
        | ColumnType::VarChar { collation, .. }
        | ColumnType::Text { collation, .. } => {
            collation.charset() == Charset::Utf8mb4 && collation_holds(collation, ty)
        }
        _ => false,
    }
}

/// The first `chars` characters (bytes, for a binary string) of a key
/// value: what an index prefix holds of it.
pub(crate) fn truncate(column: &ColumnType, v: Value, chars: u32) -> Value {
    let charset = column
        .collation()
        .map_or(Charset::Binary, Collation::charset);
    let cut = |s: &Bytes| s.slice(..charset.char_prefix(s, chars as usize).len());
    match v {
        Value::Str(s) => Value::Str(cut(&s)),
        Value::Bytes(s) => Value::Bytes(cut(&s)),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Value {
        Value::Decimal(Decimal::parse(s).expect("a decimal"))
    }

    fn num() -> ExprType {
        ExprType::bigint(false)
    }

    #[test]
    fn integers_clamp_and_round() {
        let tiny = ColumnType::tinyint(false);
        assert_eq!(
            key_value(&tiny, &Value::Int(5), &num()),
            KeyVal::Exact(Value::Int(5))
        );
        assert_eq!(
            key_value(&tiny, &Value::Int(1000), &num()),
            KeyVal::Between(Some(Value::Int(127)), None)
        );
        assert_eq!(
            key_value(&tiny, &Value::Int(-1000), &num()),
            KeyVal::Between(None, Some(Value::Int(-128)))
        );
        assert_eq!(
            key_value(&tiny, &dec("2.5"), &ExprType::decimal(2, 1, false)),
            KeyVal::Between(Some(Value::Int(2)), Some(Value::Int(3)))
        );
        assert_eq!(
            key_value(&tiny, &dec("-2.5"), &ExprType::decimal(2, 1, false)),
            KeyVal::Between(Some(Value::Int(-3)), Some(Value::Int(-2)))
        );
        assert_eq!(
            key_value(&tiny, &dec("2.0"), &ExprType::decimal(2, 1, false)),
            KeyVal::Exact(Value::Int(2))
        );
        let unsigned = ColumnType::int(true);
        assert_eq!(
            key_value(&unsigned, &Value::Int(-1), &num()),
            KeyVal::Between(None, Some(Value::UInt(0)))
        );
        assert_eq!(
            key_value(&unsigned, &Value::Double(1.5), &ExprType::double()),
            KeyVal::Between(Some(Value::UInt(1)), Some(Value::UInt(2)))
        );
        // A string that is plainly a number is that number; any other
        // string is compared row by row.
        let s = ExprType::string(1, Collation::Utf8mb4GeneralCi, Derivation::Coercible);
        assert_eq!(
            key_value(&tiny, &Value::str("5"), &s),
            KeyVal::Exact(Value::Int(5))
        );
        assert_eq!(
            key_value(&tiny, &Value::str("-2.5"), &s),
            KeyVal::Between(Some(Value::Int(-3)), Some(Value::Int(-2)))
        );
        for odd in ["5 ", " 5", "5e0", "5x", "", "1234567890123456", "+5", "5."] {
            assert_eq!(
                key_value(&tiny, &Value::str(odd), &s),
                KeyVal::Unusable,
                "{odd:?}"
            );
        }
        assert_eq!(
            key_value(
                &ColumnType::bigint(false),
                &Value::Double(1e19),
                &ExprType::double()
            ),
            KeyVal::Unusable
        );
    }

    #[test]
    fn decimals_cut_to_scale() {
        let col = ColumnType::Decimal {
            precision: 4,
            scale: 2,
        };
        let ty = ExprType::decimal(10, 4, false);
        assert_eq!(
            key_value(&col, &dec("1.5"), &ty),
            KeyVal::Exact(dec("1.50"))
        );
        assert_eq!(
            key_value(&col, &dec("1.234"), &ty),
            KeyVal::Between(Some(dec("1.23")), Some(dec("1.24")))
        );
        assert_eq!(
            key_value(&col, &dec("-1.234"), &ty),
            KeyVal::Between(Some(dec("-1.24")), Some(dec("-1.23")))
        );
        assert_eq!(
            key_value(&col, &dec("99.999"), &ty),
            KeyVal::Between(Some(dec("99.99")), None)
        );
        assert_eq!(
            key_value(&col, &dec("100"), &ty),
            KeyVal::Between(Some(dec("99.99")), None)
        );
        assert_eq!(
            key_value(&col, &Value::Int(-100), &num()),
            KeyVal::Between(None, Some(dec("-99.99")))
        );
        assert_eq!(
            key_value(&col, &dec("-0.001"), &ty),
            KeyVal::Between(Some(dec("-0.01")), Some(dec("0.00")))
        );
    }

    #[test]
    fn strings_need_the_columns_collation() {
        let col = ColumnType::VarChar {
            chars: 10,
            collation: Collation::Utf8mb4Bin,
        };
        let lit = ExprType::string(3, Collation::Utf8mb4GeneralCi, Derivation::Coercible);
        assert_eq!(
            key_value(&col, &Value::str("abc"), &lit),
            KeyVal::Exact(Value::str("abc"))
        );
        let explicit = ExprType::string(3, Collation::Utf8mb4GeneralCi, Derivation::Explicit);
        assert_eq!(
            key_value(&col, &Value::str("abc"), &explicit),
            KeyVal::Unusable
        );
        assert_eq!(key_value(&col, &Value::Int(1), &num()), KeyVal::Unusable);
        assert!(!key_usable(&col, &explicit));
        assert!(key_usable(&col, &lit));
    }

    #[test]
    fn dates_are_strict() {
        let lit = ExprType::string(10, Collation::Utf8mb4GeneralCi, Derivation::Coercible);
        assert_eq!(
            key_value(&ColumnType::Date, &Value::str("2024-02-29"), &lit),
            KeyVal::Exact(Value::Date(Date::new(2024, 2, 29)))
        );
        assert_eq!(
            key_value(&ColumnType::Date, &Value::str("2023-02-29"), &lit),
            KeyVal::Unusable
        );
        assert_eq!(
            key_value(&ColumnType::Date, &Value::str("2024-2-9"), &lit),
            KeyVal::Unusable
        );
        assert_eq!(
            key_value(
                &ColumnType::DateTime { fsp: 0 },
                &Value::str("2024-01-15 10:30:00"),
                &lit
            ),
            KeyVal::Exact(Value::DateTime(DateTime::new(
                Date::new(2024, 1, 15),
                10,
                30,
                0,
                0
            )))
        );
        assert_eq!(
            key_value(
                &ColumnType::DateTime { fsp: 0 },
                &Value::str("2024-01-15 10:30:00.5"),
                &lit
            ),
            KeyVal::Unusable
        );
    }
}
