//! Conversions between value families with the servers' rules: strings
//! read as numbers (longest numeric prefix, warning 1292), numbers printed
//! as strings, and each value read as a double, a decimal, an integer or a
//! boolean.

use bytes::Bytes;
use sprig_engine::row::{Decimal, Value};

#[cfg(test)]
use crate::error::Level;
use crate::error::{Result, SqlError};
use crate::num::{BigU, Dec, Round};
use crate::session::Ctx;
use crate::temporal;
use crate::types::{ExprType, Kind, NOT_FIXED};

/// A 64-bit integer with its signedness: `v` holds the bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntVal {
    pub(crate) v: i64,
    pub(crate) unsigned: bool,
}

impl IntVal {
    pub(crate) fn signed(v: i64) -> Self {
        Self { v, unsigned: false }
    }

    pub(crate) fn unsigned(v: u64) -> Self {
        Self {
            v: v as i64,
            unsigned: true,
        }
    }

    pub(crate) fn wide(self) -> i128 {
        if self.unsigned {
            i128::from(self.v as u64)
        } else {
            i128::from(self.v)
        }
    }

    pub(crate) fn value(self) -> Value {
        if self.unsigned {
            Value::UInt(self.v as u64)
        } else {
            Value::Int(self.v)
        }
    }

    pub(crate) fn dec(self) -> Dec {
        if self.unsigned {
            Dec::from_u64(self.v as u64)
        } else {
            Dec::from_i64(self.v)
        }
    }
}

pub(crate) fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Where the numeric prefix of a string lies.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NumScan {
    /// After the leading white space.
    pub(crate) start: usize,
    /// After the sign and the integer digits.
    pub(crate) int_end: usize,
    /// After the whole prefix (fraction and exponent included).
    pub(crate) end: usize,
    /// Whether the prefix has any digit.
    pub(crate) digits: bool,
}

/// Finds `[ws][+-]digits[.digits][e[+-]digits]` at the start of `s`.
pub(crate) fn scan_number(s: &[u8]) -> NumScan {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let start = i;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        i += 1;
    }
    let int_start = i;
    while i < s.len() && s[i].is_ascii_digit() {
        i += 1;
    }
    let int_end = i;
    let mut digits = int_end > int_start;
    if i < s.len() && s[i] == b'.' {
        let mut j = i + 1;
        while j < s.len() && s[j].is_ascii_digit() {
            j += 1;
        }
        if j > i + 1 || digits {
            digits = digits || j > i + 1;
            i = j;
        }
    }
    if !digits {
        return NumScan {
            start,
            int_end: start,
            end: start,
            digits: false,
        };
    }
    if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        let mut j = i + 1;
        if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < s.len() && s[j].is_ascii_digit() {
            j += 1;
        }
        if j > exp_start {
            i = j;
        }
    }
    NumScan {
        start,
        int_end,
        end: i,
        digits: true,
    }
}

/// The text of a value inside a warning message. A binary string shows
/// every byte outside printable ASCII as `\xNN`.
pub(crate) fn err_text_of(s: &[u8], binary: bool) -> String {
    let cut = &s[..s.len().min(255)];
    if !binary {
        return String::from_utf8_lossy(cut).into_owned();
    }
    let mut out = String::with_capacity(cut.len());
    for &b in cut {
        if (0x20..0x7f).contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("\\x{b:02X}"));
        }
    }
    out
}

/// The text of a character string inside a warning message.
pub(crate) fn err_text(s: &[u8]) -> String {
    err_text_of(s, false)
}

/// What follows a numeric prefix, for choosing the warning.
enum Tail {
    Clean,
    Spaces,
    Garbage,
}

fn tail_of(s: &[u8], end: usize) -> Tail {
    let rest = &s[end..];
    if rest.is_empty() {
        Tail::Clean
    } else if rest.iter().all(|&b| is_space(b)) {
        Tail::Spaces
    } else {
        Tail::Garbage
    }
}

/// Raises the servers' condition for a string that wasn't wholly a number.
/// `what` is `DOUBLE`, `DECIMAL` or `INTEGER`. `empty_warns` tells whether
/// MySQL warns for a string without any number in it that is empty or all
/// spaces (it does in casts, not in arithmetic).
fn truncation_warning(ctx: &mut Ctx<'_>, s: &[u8], sc: &NumScan, what: &str, empty_warns: bool) {
    let bin = ctx.err_bin;
    let msg = || {
        format!(
            "Truncated incorrect {what} value: '{}'",
            err_text_of(s, bin)
        )
    };
    if !sc.digits {
        let blank = s[sc.start..].is_empty();
        if ctx.maria() || empty_warns || !blank {
            ctx.warn(1292, msg());
        }
        return;
    }
    match tail_of(s, sc.end) {
        Tail::Clean => {}
        Tail::Spaces => {
            if ctx.maria() {
                ctx.note(1292, msg());
            }
        }
        Tail::Garbage => ctx.warn(1292, msg()),
    }
}

/// A string read as a double: the longest numeric prefix, the nearest
/// finite double beyond the range, and warning 1292 when the string holds
/// anything else.
pub(crate) fn str_to_f64(ctx: &mut Ctx<'_>, s: &[u8]) -> f64 {
    str_to_f64_as(ctx, s, "DOUBLE")
}

/// [`str_to_f64`] with the type name the warning shows.
pub(crate) fn str_to_f64_as(ctx: &mut Ctx<'_>, s: &[u8], what: &str) -> f64 {
    let sc = scan_number(s);
    if !sc.digits {
        truncation_warning(ctx, s, &sc, what, false);
        return 0.0;
    }
    let text = std::str::from_utf8(&s[sc.start..sc.end]).unwrap_or("0");
    let v = text.parse::<f64>().unwrap_or(0.0);
    if v.is_infinite() {
        ctx.warn(
            1292,
            format!("Truncated incorrect {what} value: '{}'", err_text(s)),
        );
        return if v > 0.0 { f64::MAX } else { f64::MIN };
    }
    truncation_warning(ctx, s, &sc, what, false);
    v
}

/// The decimal a numeric prefix denotes, exponent applied; fraction digits
/// beyond 38 are rounded away.
pub(crate) fn prefix_to_dec(text: &[u8]) -> Dec {
    let (mant, exp) = match text.iter().position(|&b| b == b'e' || b == b'E') {
        Some(p) => (
            &text[..p],
            std::str::from_utf8(&text[p + 1..])
                .ok()
                .and_then(|t| t.parse::<i64>().ok())
                .unwrap_or(0),
        ),
        None => (text, 0),
    };
    let neg = mant.first() == Some(&b'-');
    let mut digits = Vec::with_capacity(mant.len());
    let mut scale: i64 = 0;
    let mut seen_point = false;
    for &b in mant {
        if b == b'.' {
            seen_point = true;
        } else if b.is_ascii_digit() {
            digits.push(b - b'0');
            if seen_point {
                scale += 1;
            }
        }
    }
    let exp = exp.clamp(-400, 400);
    scale -= exp;
    let mut mag = BigU::from_digits(&digits);
    if scale < 0 {
        mag = mag.mul_pow10((-scale) as u32);
        scale = 0;
    }
    let d = Dec {
        neg: neg && !mag.is_zero(),
        mag,
        scale: scale as u32,
    };
    if d.scale > 38 {
        d.round(38, Round::HalfUp)
    } else {
        d
    }
}

/// A string read as a decimal, with warning 1292 (`DECIMAL`).
pub(crate) fn str_to_dec(ctx: &mut Ctx<'_>, s: &[u8], empty_warns: bool) -> Dec {
    let sc = scan_number(s);
    if !sc.digits {
        truncation_warning(ctx, s, &sc, "DECIMAL", empty_warns);
        return Dec::zero();
    }
    let d = prefix_to_dec(&s[sc.start..sc.end]);
    if d.int_digits() > 65 {
        // More digits than a decimal holds: the largest one, with the
        // overflow reported around the string's own complaint.
        let shown = err_text_of(s, ctx.err_bin);
        if ctx.maria() {
            ctx.warn(
                1916,
                "Got overflow when converting '' to DECIMAL. Value truncated",
            );
            ctx.warn(
                1292,
                format!("Truncated incorrect DECIMAL value: '{shown}'"),
            );
            ctx.warn(
                1916,
                "Got overflow when converting '' to DECIMAL. Value truncated",
            );
        } else {
            ctx.warn(1292, "Truncated incorrect DECIMAL value: '1'");
            ctx.warn(
                1292,
                format!("Truncated incorrect DECIMAL value: '{shown}'"),
            );
        }
        return Dec::max_of(65, 0, d.neg);
    }
    truncation_warning(ctx, s, &sc, "DECIMAL", empty_warns);
    d
}

/// A string read as an integer the way `CAST(... AS SIGNED)` and integer
/// arguments do: sign and digits only, so a `.` or an exponent ends the
/// number with a warning. Beyond 64 bits the value sticks at the unsigned
/// maximum (which reads as -1 when signed) or at the signed minimum.
pub(crate) fn str_to_int(ctx: &mut Ctx<'_>, s: &[u8]) -> IntVal {
    let sc = scan_number(s);
    let bin = ctx.err_bin;
    let msg = || {
        format!(
            "Truncated incorrect INTEGER value: '{}'",
            err_text_of(s, bin)
        )
    };
    if !sc.digits || sc.int_end == sc.start {
        // No digits at all, or the number starts with its fraction (".5").
        ctx.warn(1292, msg());
        return IntVal::signed(0);
    }
    let body = &s[sc.start..sc.int_end];
    let (neg, digits) = match body[0] {
        b'-' => (true, &body[1..]),
        b'+' => (false, &body[1..]),
        _ => (false, body),
    };
    if digits.is_empty() {
        ctx.warn(1292, msg());
        return IntVal::signed(0);
    }
    let mut v: u128 = 0;
    for &b in digits {
        v = (v * 10 + u128::from(b - b'0')).min(u128::from(u64::MAX) * 4);
    }
    let overflow;
    let out = if neg {
        overflow = v > 1u128 << 63;
        if overflow {
            IntVal::signed(i64::MIN)
        } else {
            IntVal::signed((v as i128).wrapping_neg() as i64)
        }
    } else {
        overflow = v > u128::from(u64::MAX);
        if overflow {
            IntVal::unsigned(u64::MAX)
        } else if v > i64::MAX as u128 {
            IntVal::unsigned(v as u64)
        } else {
            IntVal::signed(v as i64)
        }
    };
    if overflow {
        ctx.warn(1292, msg());
        return out;
    }
    match tail_of(s, sc.int_end) {
        Tail::Clean => {}
        Tail::Spaces => {
            if ctx.maria() {
                ctx.note(1292, msg());
            }
        }
        Tail::Garbage => ctx.warn(1292, msg()),
    }
    out
}

/// The shortest digits that read back to `x` (not zero, finite) and the
/// decimal exponent of the first one. Where the number lies exactly
/// halfway between two candidates of that length, the one ending in an
/// even digit is taken, as the servers' conversion does.
pub(crate) fn shortest_digits(x: f64) -> (Vec<u8>, i32) {
    let s = format!("{:e}", x.abs());
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let mut digits: Vec<u8> = mant.bytes().filter(u8::is_ascii_digit).collect();
    let n = digits.len();
    // A double's exact decimal expansion is finite; a tie shows as a 5
    // followed by nothing but zeros right after the kept digits.
    let probe = format!("{:.*e}", n + 3, x.abs());
    let probe: Vec<u8> = probe
        .split('e')
        .next()
        .unwrap_or("")
        .bytes()
        .filter(u8::is_ascii_digit)
        .collect();
    if probe.len() > n + 2 && probe[n] == b'5' && probe[n + 1] == b'0' && probe[n + 2] == b'0' {
        let exact = format!("{:.800e}", x.abs());
        let all: Vec<u8> = exact
            .split('e')
            .next()
            .unwrap_or("")
            .bytes()
            .filter(u8::is_ascii_digit)
            .collect();
        let tie = all.len() > n && all[n] == b'5' && all[n + 1..].iter().all(|&d| d == b'0');
        if tie {
            let mut even = all[..n].to_vec();
            if (even[n - 1] - b'0') % 2 == 1 {
                // The neighbour above ends in an even digit.
                let mut i = n;
                while i > 0 {
                    i -= 1;
                    if even[i] == b'9' {
                        even[i] = b'0';
                    } else {
                        even[i] += 1;
                        break;
                    }
                }
            }
            let text = if n > 1 {
                format!(
                    "{}.{}e{exp}",
                    char::from(even[0]),
                    String::from_utf8_lossy(&even[1..])
                )
            } else {
                format!("{}e{exp}", char::from(even[0]))
            };
            if even[0] != b'0' && text.parse::<f64>().ok() == Some(x.abs()) {
                digits = even;
            }
        }
    }
    (digits, exp)
}

fn plain_digits(neg: bool, digits: &[u8], exp: i32) -> String {
    let mut out = String::with_capacity(digits.len() + 8);
    if neg {
        out.push('-');
    }
    if exp >= 0 {
        let int_len = exp as usize + 1;
        for i in 0..int_len {
            out.push(char::from(digits.get(i).copied().unwrap_or(b'0')));
        }
        if digits.len() > int_len {
            out.push('.');
            out.extend(digits[int_len..].iter().map(|&b| char::from(b)));
        }
    } else {
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        out.extend(digits.iter().map(|&b| char::from(b)));
    }
    out
}

/// A double's text: the shortest digits that read back to it, in plain
/// notation from 1e-15 up to (not including) 1e15 and with an exponent
/// outside; a number from 1e15 up that still has digits after the point
/// stays plain.
pub(crate) fn fmt_double(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    if !x.is_finite() {
        return if x.is_nan() {
            "NaN".to_owned()
        } else if x > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    let (digits, exp) = shortest_digits(x);
    let neg = x < 0.0;
    if exp < -15 || (exp >= 15 && digits.len() as i32 <= exp + 1) {
        let mut out = String::with_capacity(24);
        if neg {
            out.push('-');
        }
        out.push(char::from(digits[0]));
        if digits.len() > 1 {
            out.push('.');
            out.extend(digits[1..].iter().map(|&b| char::from(b)));
        }
        out.push_str(&format!("e{exp}"));
        return out;
    }
    plain_digits(neg, &digits, exp)
}

/// A double's text inside a string expression. MySQL gives it at most 22
/// characters: a plain form longer than that is written with an exponent,
/// and if that is still too long the digits are rounded until it fits.
pub(crate) fn fmt_double_in_string(x: f64, maria: bool) -> String {
    let plain = fmt_double(x);
    if maria || plain.len() <= 22 || x == 0.0 || !x.is_finite() {
        return plain;
    }
    let (digits, exp) = shortest_digits(x);
    let write = |digits: &[u8], exp: i32| {
        let mut out = String::with_capacity(24);
        if x < 0.0 {
            out.push('-');
        }
        out.push(char::from(digits[0]));
        if digits.len() > 1 {
            out.push('.');
            out.extend(digits[1..].iter().map(|&b| char::from(b)));
        }
        out.push_str(&format!("e{exp}"));
        out
    };
    let full = write(&digits, exp);
    if full.len() <= 22 {
        return full;
    }
    // Round to as many digits as fit.
    let mut keep = digits.len() - (full.len() - 22);
    loop {
        let rounded = format!("{:.*e}", keep.saturating_sub(1), x.abs());
        let (mant, e) = rounded.split_once('e').unwrap_or((&rounded, "0"));
        let mut d: Vec<u8> = mant.bytes().filter(u8::is_ascii_digit).collect();
        while d.len() > 1 && d.last() == Some(&b'0') {
            d.pop();
        }
        let out = write(&d, e.parse().unwrap_or(exp));
        if out.len() <= 22 || keep <= 1 {
            return out;
        }
        keep -= 1;
    }
}

/// A value's text as an argument of a string expression (see
/// [`fmt_double_in_string`]); otherwise as [`to_text`].
pub(crate) fn to_text_in_string(v: &Value, ty: &ExprType, maria: bool) -> Option<Bytes> {
    match v {
        Value::Double(d) if !maria && !(ty.kind == Kind::Double && ty.dec < NOT_FIXED) => {
            Some(Bytes::from(fmt_double_in_string(*d, maria)))
        }
        _ => to_text(v, ty, maria),
    }
}

/// A single-precision float's text: its shortest digits, by the same
/// notation rule as doubles.
pub(crate) fn fmt_float(x: f32) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let s = format!("{x:e}");
    let Some((mant, exp)) = s.split_once('e') else {
        return s;
    };
    let exp: i32 = exp.parse().unwrap_or(0);
    if !(-15..15).contains(&exp) {
        return s;
    }
    let digits: Vec<u8> = mant.bytes().filter(u8::is_ascii_digit).collect();
    plain_digits(x < 0.0, &digits, exp)
}

/// A double with a fixed number of decimals. When the shortest digits
/// that read back to the number end before those decimals, they are
/// padded with zeros (so a large number shows its 17 digits, then zeros);
/// otherwise the number is rounded exactly.
pub(crate) fn fmt_double_fixed(x: f64, dec: u8) -> String {
    if x == 0.0 || !x.is_finite() {
        return format!("{:.*}", usize::from(dec), x);
    }
    let (digits, exp) = shortest_digits(x);
    let frac_len = (digits.len() as i32 - 1 - exp).max(0);
    if frac_len > i32::from(dec) {
        return format!("{:.*}", usize::from(dec), x);
    }
    let mut out = plain_digits(x < 0.0, &digits, exp);
    if dec > 0 {
        if frac_len == 0 {
            out.push('.');
        }
        for _ in frac_len..i32::from(dec) {
            out.push('0');
        }
    }
    out
}

/// The decimal value of a double: its shortest digits.
pub(crate) fn f64_to_dec(x: f64) -> Dec {
    if !x.is_finite() {
        return Dec::zero();
    }
    if x == 0.0 {
        return Dec::zero();
    }
    let (digits, exp) = shortest_digits(x);
    let s = format!(
        "{}{}e{}",
        if x < 0.0 { "-" } else { "" },
        String::from_utf8_lossy(&digits),
        i64::from(exp) - (digits.len() as i64 - 1)
    );
    prefix_to_dec(s.as_bytes())
}

/// A double as a signed integer: to the nearest, ties to even, sticking at
/// the ends of the range.
pub(crate) fn f64_to_i64(x: f64) -> (i64, bool) {
    let r = x.round_ties_even();
    if r >= 9_223_372_036_854_775_808.0 {
        (i64::MAX, true)
    } else if r < -9_223_372_036_854_775_808.0 {
        (i64::MIN, true)
    } else {
        (r as i64, false)
    }
}

/// The engine value of a decimal. A value of more than 65 digits in all
/// gives up digits after the point (cut, not rounded) until it fits; one
/// with more than 65 before the point becomes the largest value.
pub(crate) fn dec_value(d: &Dec) -> Value {
    if let Some(v) = d.to_decimal() {
        return Value::Decimal(v);
    }
    let int_digits = d.int_digits();
    if int_digits <= 65 {
        let scale = (65 - int_digits.max(1)).min(38).min(d.scale as usize);
        let r = d.round(scale as i32, Round::Truncate);
        if let Some(v) = r.to_decimal() {
            return Value::Decimal(v);
        }
    }
    Value::Decimal(
        Dec::max_of(65, 0, d.neg)
            .to_decimal()
            .unwrap_or_else(Decimal::zero),
    )
}

/// A value read as a double. `None` is `NULL`.
pub(crate) fn to_f64(ctx: &mut Ctx<'_>, v: &Value) -> Option<f64> {
    Some(match v {
        Value::Null => return None,
        Value::Int(i) => *i as f64,
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => *u as f64,
        Value::Year(y) => f64::from(*y),
        Value::Enum(e) => f64::from(*e),
        Value::Decimal(d) => Dec::from_decimal(d).to_f64(),
        Value::Float(f) => f64::from(*f),
        Value::Double(d) => *d,
        Value::Bytes(s) => {
            ctx.err_bin = true;
            let r = str_to_f64(ctx, s);
            ctx.err_bin = false;
            r
        }
        Value::Str(s) | Value::Json(s) => str_to_f64(ctx, s),
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
            temporal::to_dec(v).to_f64()
        }
    })
}

/// A value read as a decimal. `None` is `NULL`.
pub(crate) fn to_dec(ctx: &mut Ctx<'_>, v: &Value) -> Option<Dec> {
    Some(match v {
        Value::Null => return None,
        Value::Int(i) => Dec::from_i64(*i),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => Dec::from_u64(*u),
        Value::Year(y) => Dec::from_u64(u64::from(*y)),
        Value::Enum(e) => Dec::from_u64(u64::from(*e)),
        Value::Decimal(d) => Dec::from_decimal(d),
        Value::Float(f) => f64_to_dec(f64::from(*f)),
        Value::Double(d) => {
            let r = f64_to_dec(*d);
            if r.int_digits() > 65 {
                // The number has more digits than a decimal holds.
                if ctx.maria() {
                    ctx.warn(
                        1916,
                        "Got overflow when converting '' to DECIMAL. Value truncated",
                    );
                } else {
                    ctx.warn(1292, "Truncated incorrect DECIMAL value: '1'");
                }
                Dec::max_of(65, 0, r.neg)
            } else {
                r
            }
        }
        Value::Bytes(s) => {
            ctx.err_bin = true;
            let r = str_to_dec(ctx, s, false);
            ctx.err_bin = false;
            r
        }
        Value::Str(s) | Value::Json(s) => str_to_dec(ctx, s, false),
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
            temporal::to_dec(v)
        }
    })
}

/// A decimal as an integer for an integer context, rounding half away from
/// zero; beyond 64 bits the value sticks at the range's end with the
/// persona's condition.
pub(crate) fn dec_to_int(ctx: &mut Ctx<'_>, d: &Dec, unsigned: bool) -> IntVal {
    if unsigned {
        return match d.to_u64() {
            Ok(u) => IntVal::unsigned(u),
            Err(neg) => {
                overflow_condition(ctx, &d.to_string(), "DECIMAL", "UNSIGNED INT");
                IntVal::unsigned(if neg { 0 } else { u64::MAX })
            }
        };
    }
    match d.to_i64() {
        Ok(i) => IntVal::signed(i),
        Err(neg) => {
            overflow_condition(ctx, &d.to_string(), "DECIMAL", "INT");
            IntVal::signed(if neg { i64::MIN } else { i64::MAX })
        }
    }
}

/// MariaDB's reading of a bit operator's operand: anything that isn't an
/// integer goes through a decimal, rounds half away from zero, and sticks
/// at the unsigned maximum or the signed minimum.
pub(crate) fn maria_bit_int(ctx: &mut Ctx<'_>, v: &Value, info: Info) -> Option<u64> {
    let d = match v {
        Value::Null => return None,
        Value::Int(i) => return Some(*i as u64),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => return Some(*u),
        Value::Bytes(b) | Value::Str(b) if info.hex => return Some(hex_uint(b)),
        other => conv_dec(ctx, other, info)?,
    };
    let r = d.round(0, Round::HalfUp);
    if r.neg {
        match r.to_i64() {
            Ok(i) => Some(i as u64),
            Err(_) => {
                overflow_condition(ctx, &d.to_string(), "DECIMAL", "INT");
                Some(i64::MIN as u64)
            }
        }
    } else {
        match r.to_u64() {
            Ok(u) => Some(u),
            Err(_) => {
                overflow_condition(ctx, &d.to_string(), "DECIMAL", "UNSIGNED INT");
                Some(u64::MAX)
            }
        }
    }
}

/// The condition for a number that left the 64-bit range in a conversion:
/// MariaDB's 1916, MySQL's 1292.
pub(crate) fn overflow_condition(ctx: &mut Ctx<'_>, text: &str, mysql_what: &str, maria_to: &str) {
    if ctx.maria() {
        ctx.warn(
            1916,
            format!("Got overflow when converting '{text}' to {maria_to}. Value truncated"),
        );
    } else {
        ctx.warn(
            1292,
            format!("Truncated incorrect {mysql_what} value: '{text}'"),
        );
    }
}

/// A value read as an integer, for integer contexts (bit operators,
/// `DIV`'s operands excepted, function arguments that count). `None` is
/// `NULL`.
pub(crate) fn to_int(ctx: &mut Ctx<'_>, v: &Value) -> Option<IntVal> {
    Some(match v {
        Value::Null => return None,
        Value::Int(i) => IntVal::signed(*i),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => IntVal::unsigned(*u),
        Value::Year(y) => IntVal::signed(i64::from(*y)),
        Value::Enum(e) => IntVal::signed(i64::from(*e)),
        Value::Decimal(d) => dec_to_int(ctx, &Dec::from_decimal(d), false),
        Value::Float(f) => IntVal::signed(f64_to_i64(f64::from(*f)).0),
        Value::Double(d) => IntVal::signed(f64_to_i64(*d).0),
        Value::Bytes(s) => {
            ctx.err_bin = true;
            let r = str_to_int(ctx, s);
            ctx.err_bin = false;
            r
        }
        Value::Str(s) | Value::Json(s) => str_to_int(ctx, s),
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
            // MariaDB drops the fraction of a second; MySQL rounds the
            // time or datetime to whole seconds first (23:59:59.9 is
            // 240000, and a datetime moves on to the next day).
            let whole = temporal::to_whole_seconds(ctx, v);
            dec_to_int(ctx, &temporal::to_dec(&whole), false)
        }
    })
}

/// Whether a value is true: a number other than zero. Strings are read as
/// doubles. `None` is `NULL`.
pub(crate) fn to_bool(ctx: &mut Ctx<'_>, v: &Value) -> Option<bool> {
    Some(match v {
        Value::Null => return None,
        Value::Int(i) => *i != 0,
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => *u != 0,
        Value::Year(y) => *y != 0,
        Value::Enum(e) => *e != 0,
        Value::Decimal(d) => !d.is_zero(),
        Value::Float(f) => *f != 0.0,
        Value::Double(d) => *d != 0.0,
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => str_to_f64(ctx, s) != 0.0,
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
            !temporal::to_dec(v).is_zero()
        }
    })
}

/// A value's text, as the servers print a value of static type `ty`.
/// `None` is `NULL`.
pub(crate) fn to_text(v: &Value, ty: &ExprType, maria: bool) -> Option<Bytes> {
    let s: String = match v {
        Value::Null => return None,
        // MySQL keeps JSON in its own form: its text is always that form.
        Value::Json(s) if !maria => return Some(crate::func::json::mysql_text(s)),
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => return Some(s.clone()),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => u.to_string(),
        Value::Year(y) => format!("{y:04}"),
        Value::Enum(e) => e.to_string(),
        Value::Decimal(d) => {
            if ty.kind == Kind::Decimal && !ty.raw && d.scale() != ty.dec {
                let x = Dec::from_decimal(d);
                // MariaDB shows no more fraction digits than its nine
                // nine-digit groups leave beside the integer part.
                let mut dec = i32::from(ty.dec);
                if maria {
                    let room = 81 - 9 * x.int_digits().div_ceil(9) as i32;
                    dec = dec.min(room.max(0));
                }
                x.round(dec, Round::HalfUp).to_string()
            } else {
                d.to_string()
            }
        }
        Value::Float(f) => fmt_float(*f),
        // MariaDB prints a negative zero without its sign.
        Value::Double(d) if maria && *d == 0.0 => {
            if ty.kind == Kind::Double && ty.dec < NOT_FIXED {
                fmt_double_fixed(0.0, ty.dec)
            } else {
                "0".to_owned()
            }
        }
        Value::Double(d) => {
            if ty.kind == Kind::Double && ty.dec < NOT_FIXED && !ty.raw {
                let text = fmt_double_fixed(*d, ty.dec);
                // MariaDB shows a number that rounds to zero at no
                // decimals with a bare point: 0.3 is `0.`.
                if maria && ty.dec == 0 && (text == "0" || text == "-0") {
                    format!("{text}.")
                } else {
                    text
                }
            } else {
                fmt_double(*d)
            }
        }
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::Timestamp(_) => {
            temporal::fmt_value(v, ty.dec)
        }
    };
    Some(Bytes::from(s))
}

/// 1690 for a result outside its range.
pub(crate) fn range_error(what: &str, text: &str) -> SqlError {
    SqlError::out_of_range(what, text)
}

/// Checks a double result: an infinite or NaN value is error 1690.
pub(crate) fn check_double(x: f64, text: &str) -> Result<Value> {
    if x.is_finite() {
        Ok(Value::Double(x))
    } else {
        Err(range_error("DOUBLE", text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Persona, Session};

    #[test]
    fn doubles_print_as_the_servers_do() {
        for (x, s) in [
            (1.0, "1"),
            (-1.5e10, "-15000000000"),
            (1e15, "1e15"),
            (1e14, "100000000000000"),
            (1.234_567_890_123_456e15, "1.234567890123456e15"),
            (123_456_789_012_345.0, "123456789012345"),
            (1e-15, "0.000000000000001"),
            (1e-16, "1e-16"),
            (-1.5e-16, "-1.5e-16"),
            (0.1 + 0.2, "0.30000000000000004"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e308"),
            (1.5e-5, "0.000015"),
            (100_000_000_000_000.5, "100000000000000.5"),
            // Halfway between two 16-digit candidates: the even one.
            (0.1 * -9_007_199_254_740_992.0, "-900719925474099.2"),
            (1.0 - 1.5 / 65536.0, "0.9999771118164062"),
        ] {
            assert_eq!(fmt_double(x), s);
        }
        assert_eq!(fmt_double_fixed(1.5, 3), "1.500");
    }

    #[test]
    fn strings_read_as_numbers() {
        let maria = Session::new(Persona::MariaDb);
        let mysql = Session::new(Persona::MySql);
        let mut c = Ctx::new(&maria);
        assert_eq!(str_to_f64(&mut c, b" 12 "), 12.0);
        assert_eq!(c.warnings()[0].level, Level::Note);
        assert_eq!(str_to_f64(&mut c, b"1e"), 1.0);
        assert_eq!(str_to_f64(&mut c, b"1e400"), f64::MAX);
        assert_eq!(str_to_f64(&mut c, b""), 0.0);
        assert_eq!(c.warnings().len(), 4);
        let mut c = Ctx::new(&mysql);
        assert_eq!(str_to_f64(&mut c, b" 12 "), 12.0);
        assert_eq!(str_to_f64(&mut c, b""), 0.0);
        assert!(c.warnings().is_empty());
        assert_eq!(str_to_f64(&mut c, b"0x10"), 0.0);
        assert_eq!(c.warnings().len(), 1);
        assert_eq!(str_to_dec(&mut c, b"1e2", true).to_string(), "100");
        assert_eq!(str_to_dec(&mut c, b"-1.5e-2x", true).to_string(), "-0.015");
        assert_eq!(str_to_int(&mut c, b"12.7").v, 12);
        assert_eq!(str_to_int(&mut c, b"99999999999999999999").v, -1);
        assert_eq!(str_to_int(&mut c, b"-99999999999999999999").v, i64::MIN);
        assert_eq!(f64_to_dec(0.1).to_string(), "0.1");
        assert_eq!(f64_to_dec(1.5e3).to_string(), "1500");
        assert_eq!(f64_to_i64(2.5).0, 2);
        assert_eq!(f64_to_i64(-3.5).0, -4);
    }
}

/// What a consumer knows about the operand whose value it converts.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Info {
    /// A hex or bit literal: its bytes are a big-endian unsigned integer.
    pub(crate) hex: bool,
    /// Reading its strings as numbers raises no condition.
    pub(crate) quiet: bool,
    /// A double computed by an operator or function: on MySQL, reading it
    /// as an integer outside the 64-bit range is error 1690.
    pub(crate) strict_real: bool,
    /// A literal.
    pub(crate) literal: bool,
    /// A column's value.
    pub(crate) column: bool,
}

fn hex_uint(b: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &x in b.iter().rev().take(8).rev() {
        v = (v << 8) | u64::from(x);
    }
    v
}

fn muted<T>(ctx: &mut Ctx<'_>, quiet: bool, f: impl FnOnce(&mut Ctx<'_>) -> T) -> T {
    if quiet {
        ctx.mute += 1;
    }
    let out = f(ctx);
    if quiet {
        ctx.mute -= 1;
    }
    out
}

/// An operand's value as a double.
pub(crate) fn conv_f64(ctx: &mut Ctx<'_>, v: &Value, info: Info) -> Option<f64> {
    match v {
        Value::Bytes(b) | Value::Str(b) if info.hex => Some(hex_uint(b) as f64),
        // MySQL reads a column's string whose text after the number is
        // exactly two bytes without complaint (observed: 'ab', '1ab',
        // 'é' and 0x0001 are silent, 'x', '1a', 'xyz' and 'éé' are not).
        Value::Bytes(b) | Value::Str(b) if info.column && !ctx.maria() => {
            let sc = scan_number(b);
            let end = if sc.digits { sc.end } else { sc.start };
            let rest = &b[end..];
            let silent = info.quiet || (rest.len() == 2 && !rest.iter().all(|&c| is_space(c)));
            muted(ctx, silent, |ctx| to_f64(ctx, v))
        }
        _ => muted(ctx, info.quiet, |ctx| to_f64(ctx, v)),
    }
}

/// An operand's value as a decimal.
pub(crate) fn conv_dec(ctx: &mut Ctx<'_>, v: &Value, info: Info) -> Option<Dec> {
    match v {
        Value::Bytes(b) | Value::Str(b) if info.hex => Some(Dec::from_u64(hex_uint(b))),
        Value::Bytes(s) | Value::Str(s) if !ctx.maria() && !info.literal => {
            // MySQL reads a computed string as a decimal with its own
            // complaint; a column's string gets the usual one as well.
            let sc = scan_number(s);
            if !sc.digits || !matches!(tail_of(s, sc.end), Tail::Clean | Tail::Spaces) {
                ctx.warn(1366, "Incorrect DECIMAL value: '0' for column '' at row -1");
            }
            muted(ctx, !info.column, |ctx| to_dec(ctx, v))
        }
        _ => muted(ctx, info.quiet, |ctx| to_dec(ctx, v)),
    }
}

/// An operand's value as an integer. `text` names the operand in MySQL's
/// error for a computed double outside the 64-bit range.
pub(crate) fn conv_int(
    ctx: &mut Ctx<'_>,
    v: &Value,
    info: Info,
    text: &str,
) -> Result<Option<IntVal>> {
    Ok(match v {
        Value::Bytes(b) | Value::Str(b) if info.hex => Some(IntVal::unsigned(hex_uint(b))),
        Value::Double(x) if info.strict_real && !ctx.maria() => {
            let (i, over) = f64_to_i64(*x);
            if over {
                return Err(range_error("BIGINT", text));
            }
            Some(IntVal::signed(i))
        }
        // A double column's value that no 64-bit integer holds.
        Value::Double(x) if info.column && !info.quiet => {
            let (i, over) = f64_to_i64(*x);
            if over {
                overflow_condition(ctx, &fmt_double(*x), "INTEGER", "INT");
            }
            Some(IntVal::signed(i))
        }
        _ => muted(ctx, info.quiet, |ctx| to_int(ctx, v)),
    })
}

/// Whether an operand's value is true.
pub(crate) fn conv_bool(ctx: &mut Ctx<'_>, v: &Value, info: Info) -> Option<bool> {
    match v {
        Value::Bytes(b) | Value::Str(b) if info.hex => Some(hex_uint(b) != 0),
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => {
            let what = if ctx.maria() && !info.literal {
                "BOOLEAN"
            } else {
                "DOUBLE"
            };
            ctx.err_bin = matches!(v, Value::Bytes(_));
            let x = muted(ctx, info.quiet, |ctx| str_to_f64_as(ctx, s, what));
            ctx.err_bin = false;
            Some(x != 0.0)
        }
        _ => to_bool(ctx, v),
    }
}
