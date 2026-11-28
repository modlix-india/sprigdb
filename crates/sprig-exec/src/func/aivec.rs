//! Vector functions over binary strings of little-endian `f32` values (the
//! layout MariaDB's `VECTOR(n)` stores, and what `AI_EMBED` and embedded
//! columns hold): `VEC_DISTANCE_COSINE(a, b)`, `VEC_DISTANCE_EUCLIDEAN(a,
//! b)`, `VEC_FROMTEXT('[1, 2, 3]')` and `VEC_TOTEXT(v)`.
//!
//! - A `NULL` argument gives `NULL`.
//! - A binary string whose length isn't a multiple of four, or that holds
//!   a value that isn't finite, is error 4206.
//! - Two vectors of different lengths, and the cosine distance of a zero
//!   vector, give `NULL`.
//! - `VEC_FROMTEXT` takes a JSON array of numbers; anything else is error
//!   4205, naming the offset where it stops making sense.

use std::fmt::Write as _;

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::Value;

use super::string::text;
use crate::error::{Result, SqlError};
use crate::expr::{Env, Node, string_node};
use crate::types::{Derivation, ExprType};

/// The functions this module builds.
pub(crate) const NAMES: &[&str] = &[
    "VEC_DISTANCE_COSINE",
    "VEC_DISTANCE_EUCLIDEAN",
    "VEC_FROMTEXT",
    "VEC_TOTEXT",
];

/// The longest vector a function takes (MariaDB's limit for `VECTOR(n)`).
const MAX_DIM: usize = 16_383;

fn invalid_binary() -> SqlError {
    SqlError::new(
        4206,
        "22007",
        "Invalid binary vector format. Must use IEEE standard float representation in little-endian format. Use VEC_FromText() to generate it.",
    )
}

fn invalid_text(at: usize, shown: &[u8]) -> SqlError {
    let s = String::from_utf8_lossy(shown);
    let s: String = s.chars().take(100).collect();
    SqlError::new(
        4205,
        "22007",
        format!(
            "Invalid vector format at offset: {at} for '{s}'. Must be a valid JSON array of numbers."
        ),
    )
}

/// The values of a binary vector.
///
/// # Errors
/// 4206 when it isn't one.
pub(crate) fn floats(b: &[u8]) -> Result<Vec<f32>> {
    if b.len() % 4 != 0 || b.len() / 4 > MAX_DIM {
        return Err(invalid_binary());
    }
    let mut out = Vec::with_capacity(b.len() / 4);
    for c in b.chunks_exact(4) {
        let v = f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        if !v.is_finite() {
            return Err(invalid_binary());
        }
        out.push(v);
    }
    Ok(out)
}

/// The binary vector of `v`.
#[must_use]
pub(crate) fn bytes_of(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

/// Cosine distance, `1 - a·b / (|a| |b|)`, in `f64`; `None` for vectors of
/// different lengths or a zero vector.
#[must_use]
pub(crate) fn cosine(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    let (mut dot, mut na, mut nb) = (0f64, 0f64, 0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some(1.0 - dot / (na.sqrt() * nb.sqrt()))
}

/// Euclidean distance in `f64`; `None` for vectors of different lengths.
#[must_use]
pub(crate) fn euclidean(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() {
        return None;
    }
    Some(
        a.iter()
            .zip(b)
            .map(|(x, y)| {
                let d = f64::from(*x) - f64::from(*y);
                d * d
            })
            .sum::<f64>()
            .sqrt(),
    )
}

/// The values of a vector's text, a JSON array of numbers.
///
/// # Errors
/// 4205 at the first offset that isn't one.
pub(crate) fn parse_text(t: &[u8]) -> Result<Vec<f32>> {
    let mut i = 0;
    let skip = |i: &mut usize| {
        while *i < t.len() && t[*i].is_ascii_whitespace() {
            *i += 1;
        }
    };
    skip(&mut i);
    if t.get(i) != Some(&b'[') {
        return Err(invalid_text(i, t));
    }
    i += 1;
    let mut out = Vec::new();
    skip(&mut i);
    if t.get(i) == Some(&b']') {
        i += 1;
    } else {
        loop {
            skip(&mut i);
            let start = i;
            while i < t.len() && matches!(t[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
                i += 1;
            }
            let v = std::str::from_utf8(&t[start..i])
                .ok()
                .filter(|s| !s.is_empty() && !s.starts_with('+'))
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|v| v.is_finite())
                .ok_or_else(|| invalid_text(start, t))?;
            #[allow(clippy::cast_possible_truncation)] // a vector's values are f32
            let f = v as f32;
            if !f.is_finite() {
                return Err(invalid_text(start, t));
            }
            out.push(f);
            if out.len() > MAX_DIM {
                return Err(invalid_text(start, t));
            }
            skip(&mut i);
            match t.get(i) {
                Some(b',') => i += 1,
                Some(b']') => {
                    i += 1;
                    break;
                }
                _ => return Err(invalid_text(i, t)),
            }
        }
    }
    skip(&mut i);
    if i != t.len() {
        return Err(invalid_text(i, t));
    }
    Ok(out)
}

/// The longest text of one value ([`value_text`]): a sign, nine
/// significant digits, a point and a three-character exponent with its
/// sign (`-1.17549435e-38`).
pub(crate) const VALUE_TEXT_MAX: u32 = 15;

/// One value as `%g` prints it, with the fewest significant digits (at
/// most nine) that read back to the same `f32`: with an exponent when it
/// is below -4 or at least the precision (six, or more digits when the
/// value needs them), plain otherwise (`1`, `0.5`, `100`, `1e+06`,
/// `3.4028235e+38`, `1e-07`).
#[must_use]
pub(crate) fn value_text(x: f32) -> String {
    // Values that aren't finite (never in a vector `floats` accepts) as
    // Rust prints them.
    if !x.is_finite() {
        return x.to_string();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    // Rust's `{:e}` is the shortest round-trip form: `d.ddde[-]x`.
    let sci = format!("{x:e}");
    let (mant, exp) = sci.split_once('e').unwrap_or((sci.as_str(), "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant),
    };
    let digits: String = mant.chars().filter(char::is_ascii_digit).collect();
    // `%g`'s precision: six, or the digits the value needs.
    let p = i32::try_from(digits.len().max(6)).unwrap_or(6);
    let mut out = String::with_capacity(16);
    if neg {
        out.push('-');
    }
    if exp < -4 || exp >= p {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let sign = if exp < 0 { '-' } else { '+' };
        let _ = write!(out, "e{sign}{:02}", exp.abs());
    } else if exp < 0 {
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let whole = usize::try_from(exp + 1).unwrap_or(1);
        out.push_str(&digits[..whole.min(digits.len())]);
        for _ in digits.len()..whole {
            out.push('0');
        }
        if digits.len() > whole {
            out.push('.');
            out.push_str(&digits[whole..]);
        }
    }
    out
}

/// A vector's text: `[v1,v2,...]`, each value as [`value_text`] writes it.
#[must_use]
pub(crate) fn to_text(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| value_text(*x)).collect();
    format!("[{}]", parts.join(","))
}

/// Compiles the call of one of [`NAMES`].
///
/// # Errors
/// 1582 for a wrong number of arguments.
pub(crate) fn build(name: &str, args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let want = if name.starts_with("VEC_DISTANCE") {
        2
    } else {
        1
    };
    if args.len() != want {
        return Err(SqlError::param_count(name));
    }
    let mut args: Vec<Node> = args.into_iter().map(|a| string_node(a, env)).collect();
    match name {
        "VEC_DISTANCE_COSINE" | "VEC_DISTANCE_EUCLIDEAN" => {
            let b = args.pop().ok_or_else(|| SqlError::param_count(name))?;
            let a = args.pop().ok_or_else(|| SqlError::param_count(name))?;
            let cos = name == "VEC_DISTANCE_COSINE";
            let label = if cos {
                "vec_distance_cosine"
            } else {
                "vec_distance_euclidean"
            };
            Ok(Node::new(
                ExprType::double().nullable(),
                label,
                move |ctx, row| {
                    let (Some(x), Some(y)) = (text(a.eval(ctx, row)?), text(b.eval(ctx, row)?))
                    else {
                        return Ok(Value::Null);
                    };
                    let (x, y) = (floats(&x)?, floats(&y)?);
                    let d = if cos {
                        cosine(&x, &y)
                    } else {
                        euclidean(&x, &y)
                    };
                    Ok(d.map_or(Value::Null, Value::Double))
                },
            ))
        }
        "VEC_FROMTEXT" => {
            let a = args.pop().ok_or_else(|| SqlError::param_count(name))?;
            // Up to four bytes per value of a text of `len` characters.
            let len = a.ty.len.saturating_mul(2);
            let ty = ExprType::string(len, Collation::Binary, Derivation::Coercible).nullable();
            Ok(Node::new(ty, "vec_fromtext", move |ctx, row| {
                let Some(t) = text(a.eval(ctx, row)?) else {
                    return Ok(Value::Null);
                };
                let v = parse_text(&t)?;
                Ok(Value::Bytes(bytes_of(&v).into()))
            }))
        }
        _ => {
            let a = args.pop().ok_or_else(|| SqlError::param_count(name))?;
            // At most VALUE_TEXT_MAX characters and a comma per value of
            // a binary of `len` bytes, and the brackets.
            let len = (a.ty.len / 4)
                .saturating_mul(VALUE_TEXT_MAX + 1)
                .saturating_add(2);
            let ty = ExprType::string(len, env.conn(), Derivation::Coercible).nullable();
            Ok(Node::new(ty, "vec_totext", move |ctx, row| {
                let Some(b) = text(a.eval(ctx, row)?) else {
                    return Ok(Value::Null);
                };
                let v = floats(&b)?;
                Ok(Value::Str(to_text(&v).into_bytes().into()))
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_match_their_definitions() {
        let a = [1.0f32, 0.0, 0.0];
        let b = [0.0f32, 1.0, 0.0];
        assert!((cosine(&a, &b).unwrap() - 1.0).abs() < 1e-12);
        assert!(cosine(&a, &a).unwrap().abs() < 1e-12);
        assert!((euclidean(&a, &b).unwrap() - 2f64.sqrt()).abs() < 1e-12);
        assert_eq!(cosine(&a, &[1.0, 0.0]), None);
        assert_eq!(cosine(&a, &[0.0, 0.0, 0.0]), None);
        assert_eq!(euclidean(&a, &[1.0]), None);
    }

    #[test]
    fn binary_vectors_round_trip_and_bad_ones_are_refused() {
        let v = [1.5f32, -2.0, 0.25];
        let b = bytes_of(&v);
        assert_eq!(b.len(), 12);
        assert_eq!(floats(&b).unwrap(), v);
        assert_eq!(floats(&b[..5]).unwrap_err().code, 4206);
        assert_eq!(floats(&f32::NAN.to_le_bytes()).unwrap_err().code, 4206);
        assert_eq!(&b[..4], &[0, 0, 0xC0, 0x3F]);
    }

    #[test]
    fn text_is_a_json_array_of_numbers() {
        assert_eq!(parse_text(b"[1, 2.5, -3e1]").unwrap(), [1.0, 2.5, -30.0]);
        assert_eq!(parse_text(b" [ ] ").unwrap(), Vec::<f32>::new());
        for bad in [
            &b"1,2"[..],
            b"[1,]",
            b"[1 2]",
            b"[a]",
            b"[1]x",
            b"[1e99]",
            b"[+1]",
        ] {
            assert_eq!(parse_text(bad).unwrap_err().code, 4205, "{bad:?}");
        }
        let e = parse_text(b"[1,x]").unwrap_err();
        assert!(e.message.contains("offset: 3"), "{}", e.message);
        assert_eq!(to_text(&[1.0, 0.5, -2.25]), "[1,0.5,-2.25]");
        assert_eq!(value_text(f32::NAN), "NaN");
        assert_eq!(value_text(f32::INFINITY), "inf");
        assert_eq!(value_text(f32::NEG_INFINITY), "-inf");
        for (x, t) in [
            (f32::MAX, "3.4028235e+38"),
            (f32::MIN_POSITIVE, "1.1754944e-38"),
            (-1.175_494_4e-38, "-1.1754944e-38"),
            (1e-7, "1e-07"),
            (0.0001, "0.0001"),
            (123_456.0, "123456"),
            (1e6, "1e+06"),
            (100.0, "100"),
            (0.0, "0"),
            (-0.0, "-0"),
        ] {
            assert_eq!(value_text(x), t, "{x}");
            assert!(u32::try_from(value_text(x).len()).unwrap() <= VALUE_TEXT_MAX);
            assert_eq!(parse_text(format!("[{t}]").as_bytes()).unwrap(), [x]);
        }
        assert_eq!(
            parse_text(to_text(&[0.1, 3.0e-7]).as_bytes()).unwrap(),
            [0.1, 3.0e-7]
        );
    }
}
