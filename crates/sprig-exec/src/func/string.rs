//! String functions.

use std::cmp::Ordering;

use std::fmt::Write as _;

use bytes::Bytes;
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::Value;

use crate::error::{Result, SqlError};
use crate::expr::like::chars_of;
use crate::expr::{Env, Node, recode_checked, string_node};
use crate::session::Ctx;
use crate::types::{Derivation, Dtc, ExprType, Kind, aggregate_types, mb_max_len};

pub(super) fn text(v: Value) -> Option<Bytes> {
    match v {
        Value::Str(b) | Value::Bytes(b) | Value::Json(b) => Some(b),
        _ => None,
    }
}

pub(super) fn wrap(b: impl Into<Bytes>, binary: bool) -> Value {
    if binary {
        Value::Bytes(b.into())
    } else {
        Value::Str(b.into())
    }
}

pub(super) fn strings(args: Vec<Node>, env: Env<'_>) -> Vec<Node> {
    args.into_iter().map(|a| string_node(a, env)).collect()
}

pub(super) fn dtc_of(nodes: &[&Node], op: &str, for_compare: bool, env: Env<'_>) -> Result<Dtc> {
    let tys: Vec<&ExprType> = nodes.iter().map(|n| &n.ty).collect();
    aggregate_types(&tys, env.conn(), op, for_compare)
}

/// A string function's result never holds its collation as loosely as a
/// number does: what was a number's text counts as a literal's.
pub(super) fn loosen(d: Derivation) -> Derivation {
    if d == Derivation::Numeric {
        Derivation::Coercible
    } else {
        d
    }
}

pub(super) fn str_type(chars: u64, dtc: Dtc, nullable: bool) -> ExprType {
    ExprType {
        mb3: dtc.mb3,
        ..ExprType::string(
            chars.min(u64::from(u32::MAX / 4)) as u32,
            dtc.coll,
            loosen(dtc.deriv),
        )
    }
    .with_null(nullable)
}

/// The type of a function that answers a 32-bit integer on MariaDB (MySQL
/// reports every integer function as 64-bit).
pub(super) fn int32(len: u32, env: Env<'_>) -> ExprType {
    ExprType {
        prec: len.min(10) as u8,
        width: if env.maria() { 4 } else { 0 },
        ..ExprType::int(len, false)
    }
}

pub(super) fn too_big(ctx: &mut Ctx<'_>, name: &str) -> Value {
    let max = ctx.session.max_allowed_packet;
    ctx.warn(
        1301,
        format!("Result of {name}() was larger than max_allowed_packet ({max}) - truncated"),
    );
    Value::Null
}

/// The characters a string function reports when its length is known
/// only per row: room for `max_allowed_packet` (MariaDB counts its bytes
/// as characters).
pub(super) fn unknown_chars(env: Env<'_>, mb: u64) -> u64 {
    let packet = env.s.max_allowed_packet;
    if env.maria() {
        packet
    } else {
        packet / mb.max(1)
    }
}

/// `CONCAT(s, ...)`.
pub(crate) fn concat(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let args = strings(args, env);
    let refs: Vec<&Node> = args.iter().collect();
    let dtc = dtc_of(&refs, "concat", false, env)?;
    let args = recode_checked(args, dtc.coll, "concat", env)?;
    let chars: u64 = args.iter().map(|a| u64::from(a.ty.len)).sum();
    let binary = dtc.coll == Collation::Binary;
    Ok(Node::new(
        str_type(chars, dtc, true),
        "concat",
        move |ctx, row| {
            let mut out = Vec::new();
            for a in &args {
                let Some(b) = text(a.eval(ctx, row)?) else {
                    return Ok(Value::Null);
                };
                out.extend_from_slice(&b);
                if out.len() as u64 > ctx.session.max_allowed_packet {
                    return Ok(too_big(ctx, "concat"));
                }
            }
            Ok(wrap(out, binary))
        },
    ))
}

/// `CONCAT_WS(separator, s, ...)`: `NULL` arguments are skipped.
pub(crate) fn concat_ws(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let args = strings(args, env);
    let refs: Vec<&Node> = args.iter().collect();
    let dtc = dtc_of(&refs, "concat_ws", false, env)?;
    let args = recode_checked(args, dtc.coll, "concat_ws", env)?;
    let n = args.len() as u64;
    let chars: u64 = args
        .iter()
        .skip(1)
        .map(|a| u64::from(a.ty.len))
        .sum::<u64>()
        + u64::from(args[0].ty.len) * n.saturating_sub(2);
    let binary = dtc.coll == Collation::Binary;
    Ok(Node::new(
        str_type(chars, dtc, true),
        "concat_ws",
        move |ctx, row| {
            let Some(sep) = text(args[0].eval(ctx, row)?) else {
                return Ok(Value::Null);
            };
            let mut out = Vec::new();
            let mut first = true;
            for a in &args[1..] {
                let Some(b) = text(a.eval(ctx, row)?) else {
                    continue;
                };
                if !first {
                    out.extend_from_slice(&sep);
                }
                first = false;
                out.extend_from_slice(&b);
            }
            Ok(wrap(out, binary))
        },
    ))
}

/// A function of one string giving an integer.
fn measure(
    a: Node,
    name: &'static str,
    len: u32,
    env: Env<'_>,
    f: impl Fn(Charset, &[u8]) -> i64 + Send + Sync + 'static,
) -> Node {
    let a = string_node(a, env);
    let charset = a.ty.coll.charset();
    let ty = int32(len, env).with_null(a.ty.nullable);
    let binary = a.ty.coll == Collation::Binary;
    Node::new(ty, name, move |ctx, row| {
        let v = a.eval(ctx, row)?;
        // A text that reaches a binary result keeps its own character set
        // (see `COALESCE`).
        let charset = if binary && matches!(v, Value::Str(_)) {
            Charset::Utf8mb4
        } else {
            charset
        };
        Ok(match text(v) {
            None => Value::Null,
            Some(b) => Value::Int(f(charset, &b)),
        })
    })
}

/// `LENGTH`, `OCTET_LENGTH`, `CHAR_LENGTH`, `BIT_LENGTH`, `ASCII`, `ORD`.
pub(crate) fn measure_fn(name: &str, a: Node, env: Env<'_>) -> Node {
    match name {
        "LENGTH" | "OCTET_LENGTH" => measure(a, "length", 10, env, |_, b| b.len() as i64),
        "BIT_LENGTH" => measure(a, "bit_length", 11, env, |_, b| b.len() as i64 * 8),
        "ASCII" => measure(a, "ascii", 3, env, |_, b| {
            i64::from(b.first().copied().unwrap_or(0))
        }),
        "ORD" => measure(a, "ord", 7, env, |cs, b| {
            let first = chars_of(cs, b).first().copied().unwrap_or(&[]);
            first.iter().fold(0i64, |acc, &x| (acc << 8) | i64::from(x))
        }),
        _ => measure(a, "char_length", 10, env, |cs, b| cs.char_count(b) as i64),
    }
}

/// A function of one string giving a string of the same collation.
fn map_string(
    a: Node,
    name: &'static str,
    env: Env<'_>,
    f: impl Fn(Charset, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> Node {
    let a = string_node(a, env);
    let charset = a.ty.coll.charset();
    let binary = a.ty.coll == Collation::Binary;
    // MariaDB's UCA 14 collations can double a string's length when its
    // case changes.
    let grow =
        if matches!(name, "upper" | "lower") && env.maria() && a.ty.coll.name().contains("uca1400")
        {
            2
        } else {
            1
        };
    let ty = ExprType {
        mb3: a.ty.mb3,
        ..ExprType::string(a.ty.len.saturating_mul(grow), a.ty.coll, loosen(a.ty.deriv))
    }
    .nullable();
    Node::new(ty, name, move |ctx, row| {
        Ok(match text(a.eval(ctx, row)?) {
            None => Value::Null,
            Some(b) => wrap(f(charset, &b), binary),
        })
    })
}

fn map_case(cs: Charset, b: &[u8], upper: bool) -> Vec<u8> {
    match cs {
        Charset::Binary => b.to_vec(),
        Charset::Utf8mb4 => {
            let Ok(s) = std::str::from_utf8(b) else {
                return b.to_vec();
            };
            let mut out = String::with_capacity(s.len());
            for c in s.chars() {
                // The servers map one character to one character: a
                // letter whose other case is longer (ß) stays as it is.
                let mut it: Box<dyn Iterator<Item = char>> = if upper {
                    Box::new(c.to_uppercase())
                } else {
                    Box::new(c.to_lowercase())
                };
                match (it.next(), it.next()) {
                    (Some(m), None) => out.push(m),
                    _ => out.push(c),
                }
            }
            out.into_bytes()
        }
        // latin1's letters À to Þ and à to þ change case too (not × and
        // ÷, nor the Windows-1252 additions).
        Charset::Latin1 => b
            .iter()
            .map(|&c| match c {
                0xC0..=0xDE if c != 0xD7 && !upper => c + 0x20,
                0xE0..=0xFE if c != 0xF7 && upper => c - 0x20,
                _ if upper => c.to_ascii_uppercase(),
                _ => c.to_ascii_lowercase(),
            })
            .collect(),
        _ => b
            .iter()
            .map(|c| {
                if upper {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                }
            })
            .collect(),
    }
}

fn trim_spaces(b: &[u8], left: bool, right: bool) -> &[u8] {
    let mut s = b;
    if left {
        while s.first() == Some(&b' ') {
            s = &s[1..];
        }
    }
    if right {
        while s.last() == Some(&b' ') {
            s = &s[..s.len() - 1];
        }
    }
    s
}

/// `UPPER`, `LOWER`, `REVERSE`, `LTRIM`, `RTRIM`, `TRIM` (spaces).
pub(crate) fn map_fn(name: &str, a: Node, env: Env<'_>) -> Node {
    match name {
        "UPPER" | "UCASE" => map_string(a, "upper", env, |cs, b| map_case(cs, b, true)),
        "LOWER" | "LCASE" => map_string(a, "lower", env, |cs, b| map_case(cs, b, false)),
        "REVERSE" => map_string(a, "reverse", env, |cs, b| {
            let mut out = Vec::with_capacity(b.len());
            for c in chars_of(cs, b).iter().rev() {
                out.extend_from_slice(c);
            }
            out
        }),
        "LTRIM" => map_string(a, "ltrim", env, |_, b| trim_spaces(b, true, false).to_vec()),
        "RTRIM" => map_string(a, "rtrim", env, |_, b| trim_spaces(b, false, true).to_vec()),
        _ => map_string(a, "trim", env, |_, b| trim_spaces(b, true, true).to_vec()),
    }
}

/// `TRIM([BOTH | LEADING | TRAILING] remove FROM s)`: the remove string is
/// matched byte by byte.
pub(crate) fn trim_remove(
    rem: Node,
    s: Node,
    left: bool,
    right: bool,
    env: Env<'_>,
) -> Result<Node> {
    let (rem, s) = (string_node(rem, env), string_node(s, env));
    // MySQL accepts operands whose collations do not merge (the result
    // then has none); MariaDB refuses them.
    let first_null = s.ty.kind == Kind::Str && s.ty.deriv == Derivation::Ignorable;
    let mut dtc = dtc_of(&[&s, &rem], "trim", env.maria(), env)?;
    let [s, rem]: [Node; 2] = recode_checked(vec![s, rem], dtc.coll, "trim", env)?
        .try_into()
        .map_err(|_| SqlError::param_count("trim"))?;
    if first_null && !env.maria() {
        dtc.coll = Collation::Binary;
        dtc.deriv = Derivation::Ignorable;
    }
    let binary = dtc.coll == Collation::Binary;
    let ty = str_type(u64::from(s.ty.len), dtc, true);
    Ok(Node::new(ty, "trim", move |ctx, row| {
        let Some(b) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(r) = text(rem.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let mut out: &[u8] = &b;
        if !r.is_empty() {
            if left {
                while out.starts_with(&r) {
                    out = &out[r.len()..];
                }
            }
            if right {
                while out.ends_with(&r) {
                    out = &out[..out.len() - r.len()];
                }
            }
        }
        Ok(wrap(out.to_vec(), binary))
    }))
}

/// The byte range of characters `[from, from + count)` of a string.
pub(super) fn char_slice(cs: Charset, b: &[u8], from: usize, count: usize) -> &[u8] {
    if cs != Charset::Utf8mb4 {
        let start = from.min(b.len());
        let end = start.saturating_add(count).min(b.len());
        return &b[start..end];
    }
    let rest = &b[cs.char_prefix(b, from).len()..];
    cs.char_prefix(rest, count)
}

/// `SUBSTRING(s, pos [, len])`, `LEFT(s, n)`, `RIGHT(s, n)`.
pub(crate) fn substring(name: &str, args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let mut it = args.into_iter();
    let first = it.next().ok_or_else(|| SqlError::param_count(name))?;
    let was_string = first.ty.kind == Kind::Str;
    let s = string_node(first, env);
    let (p, l) = (it.next(), it.next());
    let Some(p) = p else {
        return Err(SqlError::param_count(name));
    };
    let charset = s.ty.coll.charset();
    let binary = s.ty.coll == Collation::Binary;
    let kind = match name {
        "LEFT" => 0,
        "RIGHT" => 1,
        _ => 2,
    };
    // A constant count bounds the length of LEFT and RIGHT.
    let pos = p.const_i64(env)?;
    let mut chars = match (kind, pos) {
        (0 | 1, Some(n)) => s.ty.len.min(u32::try_from(n.max(0)).unwrap_or(u32::MAX)),
        // (A NULL count: no characters.)
        (0 | 1, None) if p.ty.kind == Kind::Null => 0,
        _ => s.ty.len,
    };
    let count = match &l {
        Some(l) => l.const_i64(env)?,
        None => None,
    };
    if kind == 2 {
        // SUBSTRING's reported length from a constant position and count:
        // MySQL counts characters; MariaDB starts from the string's bytes
        // (four a character of utf8mb4) and does not cap a negative
        // position or the count by them.
        let maria = env.maria();
        let base = if maria {
            u64::from(s.ty.len)
                * if was_string && s.ty.coll.charset() == Charset::Utf8mb4 {
                    4
                } else {
                    1
                }
        } else {
            u64::from(s.ty.len)
        };
        let null_pos = p.ty.kind == Kind::Null;
        let mut c: u64 = match pos {
            _ if null_pos => {
                if maria {
                    0
                } else {
                    base
                }
            }
            Some(n) if n > 0 => base.saturating_sub((n - 1) as u64),
            Some(0) => 0,
            Some(n) => {
                let back = n.unsigned_abs();
                if !maria && back > base { 0 } else { back }
            }
            None => base,
        };
        if let Some(k) = count {
            c = if k <= 0 { 0 } else { c.min(k as u64) };
        }
        chars = c.min(u64::from(u32::MAX / 4)) as u32;
    }
    let ty = ExprType {
        mb3: s.ty.mb3,
        ..ExprType::string(chars, s.ty.coll, loosen(s.ty.deriv))
    }
    .nullable();
    Ok(Node::new(ty, "substring", move |ctx, row| {
        let sv = s.eval(ctx, row)?;
        // A text that reaches a binary result keeps its own character set
        // (see `COALESCE` and MariaDB's `CASE`).
        let charset = if binary && matches!(sv, Value::Str(_)) {
            Charset::Utf8mb4
        } else {
            charset
        };
        let Some(b) = text(sv) else {
            return Ok(Value::Null);
        };
        let Some(pos) = p.int_typed(ctx, row)? else {
            return Ok(Value::Null);
        };
        let len = match &l {
            Some(l) => match l.int_typed(ctx, row)? {
                Some(v) => Some(v),
                None => return Ok(Value::Null),
            },
            None => None,
        };
        let total = charset.char_count(&b);
        let out: &[u8] = match kind {
            0 | 1 => {
                // A negative count is none.
                let n = usize::try_from(pos.wide().max(0))
                    .unwrap_or(usize::MAX)
                    .min(total);
                if kind == 0 {
                    char_slice(charset, &b, 0, n)
                } else {
                    char_slice(charset, &b, total - n, n)
                }
            }
            _ => {
                let count = match len {
                    None => usize::MAX,
                    Some(v) if v.unsigned => usize::try_from(v.v as u64).unwrap_or(usize::MAX),
                    Some(v) => usize::try_from(v.v.max(0)).unwrap_or(usize::MAX),
                };
                let start = if pos.unsigned || pos.v > 0 {
                    usize::try_from(pos.wide() - 1).unwrap_or(usize::MAX)
                } else if pos.v == 0 {
                    usize::MAX
                } else {
                    let back = usize::try_from(pos.v.unsigned_abs()).unwrap_or(usize::MAX);
                    if back > total {
                        usize::MAX
                    } else {
                        total - back
                    }
                };
                if start >= total {
                    &[]
                } else {
                    char_slice(charset, &b, start, count)
                }
            }
        };
        Ok(wrap(out.to_vec(), binary))
    }))
}

/// `REPLACE(s, from, to)`: every occurrence, matched byte by byte.
pub(crate) fn replace(s: Node, from: Node, to: Node, env: Env<'_>) -> Result<Node> {
    let first_null = s.ty.kind == Kind::Null;
    // MariaDB reads the replacement only when the search string is found:
    // otherwise the string comes back, even with a NULL replacement
    // (except the NULL literal in a utf8mb4 result, which is NULL).
    let null_literal = to.ty.kind == Kind::Null;
    let (s, from, to) = (
        string_node(s, env),
        string_node(from, env),
        string_node(to, env),
    );
    let mut dtc = dtc_of(&[&s, &from, &to], "replace", env.maria(), env)?;
    let [s, from, to]: [Node; 3] = recode_checked(vec![s, from, to], dtc.coll, "replace", env)?
        .try_into()
        .map_err(|_| SqlError::param_count("replace"))?;
    let lazy = env.maria() && !(null_literal && dtc.coll.charset() == Charset::Utf8mb4);
    if first_null && !env.maria() {
        dtc.coll = Collation::Binary;
        dtc.deriv = Derivation::Ignorable;
    }
    let binary = dtc.coll == Collation::Binary;
    // MySQL grows the length by what each replacement may add; MariaDB
    // by the replacement's whole length.
    let grow = if env.maria() {
        u64::from(to.ty.len.max(1))
    } else {
        u64::from(to.ty.len.saturating_sub(from.ty.len).max(1))
    };
    let ty = str_type(u64::from(s.ty.len) * grow, dtc, true);
    Ok(Node::new(ty, "replace", move |ctx, row| {
        let Some(b) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(f) = text(from.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        // MariaDB answers the string itself when the search string is
        // empty (or, see above, not in it), whatever the replacement.
        if ctx.maria() && (f.is_empty() || (lazy && !b.windows(f.len()).any(|w| w == &f[..]))) {
            return Ok(wrap(b, binary));
        }
        let Some(t) = text(to.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        if f.is_empty() {
            return Ok(wrap(b, binary));
        }
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i..].starts_with(&f) {
                out.extend_from_slice(&t);
                i += f.len();
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        if out.len() as u64 > ctx.session.max_allowed_packet {
            return Ok(too_big(ctx, "replace"));
        }
        Ok(wrap(out, binary))
    }))
}

/// `LPAD(s, n, pad)` and `RPAD(s, n, pad)`.
pub(crate) fn pad(name: &'static str, s: Node, n: Node, p: Node, env: Env<'_>) -> Result<Node> {
    let first_null = s.ty.kind == Kind::Null;
    // MariaDB sends RPAD of a number on as latin1: every byte above ASCII
    // of the result arrives encoded once more ('É' shows as 'Ã‰'). LPAD,
    // and RPAD inside another expression, are not affected.
    let latin1_number =
        env.maria() && name == "rpad" && !matches!(s.ty.kind, Kind::Str | Kind::Null);
    let (s, p) = (string_node(s, env), string_node(p, env));
    let mut dtc = dtc_of(&[&s, &p], name, false, env)?;
    let [s, p]: [Node; 2] = recode_checked(vec![s, p], dtc.coll, name, env)?
        .try_into()
        .map_err(|_| SqlError::param_count(name))?;
    if first_null && !env.maria() {
        // MySQL types the result of padding the NULL literal as binary.
        dtc.coll = Collation::Binary;
        dtc.deriv = Derivation::Ignorable;
    }
    let charset = dtc.coll.charset();
    let binary = dtc.coll == Collation::Binary;
    // The reported length: the count; one known only per row (MySQL:
    // also a negative one) is room for max_allowed_packet.
    let mb = u64::from(mb_max_len(dtc.coll));
    let unknown = unknown_chars(env, mb);
    let null_count = n.ty.kind == Kind::Null;
    let chars = match n.const_i64(env)? {
        Some(v) if v >= 0 => v as u64,
        Some(_) if env.maria() => 0,
        Some(_) => unknown,
        None if null_count && !env.maria() => 0,
        None => unknown,
    };
    let left = name == "lpad";
    Ok(Node::new(
        str_type(chars, dtc, true),
        name,
        move |ctx, row| {
            let sv = s.eval(ctx, row)?;
            // MySQL counts by the string's own character set (a text
            // reaching a binary result keeps it, see `COALESCE`, and a
            // binary string reaching a text result keeps its bytes);
            // MariaDB by the result's.
            let charset = match &sv {
                Value::Str(_) if !ctx.maria() && binary => Charset::Utf8mb4,
                Value::Bytes(_) if !ctx.maria() && !binary => Charset::Binary,
                _ => charset,
            };
            let Some(b) = text(sv) else {
                return Ok(Value::Null);
            };
            let Some(count) = n.int_typed(ctx, row)? else {
                return Ok(Value::Null);
            };
            let Some(padding) = text(p.eval(ctx, row)?) else {
                return Ok(Value::Null);
            };
            if !count.unsigned && count.v < 0 {
                return Ok(Value::Null);
            }
            let want = usize::try_from(count.wide()).unwrap_or(usize::MAX);
            if want as u64 > ctx.session.max_allowed_packet {
                return Ok(too_big(ctx, name));
            }
            let have = charset.char_count(&b);
            if want <= have {
                return Ok(wrap(char_slice(charset, &b, 0, want).to_vec(), binary));
            }
            let pad_chars = chars_of(charset, &padding);
            if pad_chars.is_empty() {
                // Nothing to pad with: NULL on MariaDB, the empty string
                // on MySQL.
                if ctx.maria() {
                    return Ok(Value::Null);
                }
                return Ok(wrap(Vec::new(), binary));
            }
            let mut fill = Vec::new();
            for i in 0..(want - have) {
                fill.extend_from_slice(pad_chars[i % pad_chars.len()]);
            }
            let mut out = Vec::with_capacity(b.len() + fill.len());
            if left {
                out.extend_from_slice(&fill);
                out.extend_from_slice(&b);
            } else {
                out.extend_from_slice(&b);
                out.extend_from_slice(&fill);
            }
            Ok(wrap(out, binary))
        },
    ))
    .map(|mut n| {
        n.latin1_send = latin1_number && !binary;
        n
    })
}

/// Bytes read as latin1 (Windows-1252, as the servers' latin1 is) and
/// written as UTF-8.
pub(crate) fn latin1_as_utf8(b: &[u8]) -> Vec<u8> {
    // 0x80 to 0x9F are Windows-1252's printable characters.
    const HIGH: [u32; 32] = [
        0x20AC, 0x81, 0x201A, 0x192, 0x201E, 0x2026, 0x2020, 0x2021, 0x2C6, 0x2030, 0x160, 0x2039,
        0x152, 0x8D, 0x17D, 0x8F, 0x90, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
        0x2DC, 0x2122, 0x161, 0x203A, 0x153, 0x9D, 0x17E, 0x178,
    ];
    let mut out = String::with_capacity(b.len() * 2);
    for &c in b {
        let cp = match c {
            0x80..=0x9F => HIGH[usize::from(c - 0x80)],
            _ => u32::from(c),
        };
        out.push(char::from_u32(cp).unwrap_or('?'));
    }
    out.into_bytes()
}

/// Where `sub` first occurs in `s` at or after character `from` (from 0);
/// 1-based, 0 for none. As both servers do it: at each character of `s`,
/// the next bytes, as many as `sub` has, are compared with `sub` under
/// the collation. So `É` is found in `été` (two bytes against two) but
/// `e` is not found in `É` (one byte against half a character).
fn find(coll: Collation, s: &[u8], sub: &[u8], from: usize, strict_start: bool) -> i64 {
    let cs = coll.charset();
    let hay = chars_of(cs, s);
    // With a start position MariaDB wants it inside the string, also for
    // an empty search string.
    if from > hay.len() || (strict_start && from >= hay.len()) {
        return 0;
    }
    if sub.is_empty() {
        return from as i64 + 1;
    }
    let mut offset: usize = hay[..from].iter().map(|c| c.len()).sum();
    for (i, c) in hay.iter().enumerate().skip(from) {
        let end = offset + sub.len();
        if end > s.len() {
            break;
        }
        let window = &s[offset..end];
        let whole_chars = end == s.len() || s[end] & 0xC0 != 0x80 || cs != Charset::Utf8mb4;
        if window == sub || (whole_chars && coll.compare(window, sub) == Ordering::Equal) {
            return i as i64 + 1;
        }
        offset += c.len();
    }
    0
}

/// `LOCATE(sub, s [, pos])` and `INSTR(s, sub)`.
pub(crate) fn locate(sub: Node, s: Node, pos: Option<Node>, env: Env<'_>) -> Result<Node> {
    let hay_is_string = matches!(s.ty.kind, Kind::Str | Kind::Null);
    let (sub, s) = (string_node(sub, env), string_node(s, env));
    let hay_deriv = s.ty.deriv;
    // (MySQL searches a string whose collation is of no derivation, such
    // as a CASE over two columns' collations, under that collation;
    // MariaDB refuses it.)
    let merged = dtc_of(&[&s, &sub], "locate", env.maria(), env)?.coll;
    let [s, sub]: [Node; 2] = recode_checked(vec![s, sub], merged, "locate", env)?
        .try_into()
        .map_err(|_| SqlError::param_count("locate"))?;
    // MySQL searches under the collation of the string searched in, when
    // the two are of one character set.
    let coll = if !env.maria() && s.ty.coll.charset() == merged.charset() {
        s.ty.coll
    } else {
        merged
    };
    let maria = env.maria();
    let nullable = s.ty.nullable || sub.ty.nullable || pos.as_ref().is_some_and(|p| p.ty.nullable);
    let mut ty = int32(11, env).with_null(nullable);
    if !maria {
        // MySQL reports the position with the collation strength of the
        // string searched in (a number's counts as a literal's).
        ty.deriv = if hay_is_string {
            hay_deriv
        } else {
            Derivation::Coercible
        };
    }
    Ok(Node::new(ty, "locate", move |ctx, row| {
        let Some(needle) = text(sub.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(hay) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let from = match &pos {
            None => 0,
            Some(p) => match p.int_typed(ctx, row)? {
                None => return Ok(Value::Null),
                Some(v) if !v.unsigned && v.v < 1 => return Ok(Value::Int(0)),
                Some(v) => usize::try_from(v.wide() - 1).unwrap_or(usize::MAX),
            },
        };
        Ok(Value::Int(find(
            coll,
            &hay,
            &needle,
            from,
            maria && pos.is_some(),
        )))
    }))
}

/// `REPEAT(s, n)` and `SPACE(n)`.
pub(crate) fn repeat(s: Node, n: Node, name: &'static str, env: Env<'_>) -> Result<Node> {
    let s = string_node(s, env);
    let binary = s.ty.coll == Collation::Binary;
    let unknown = unknown_chars(env, u64::from(mb_max_len(s.ty.coll)));
    let null_count = n.ty.kind == Kind::Null;
    let chars = match n.const_i64(env)? {
        Some(v) if v >= 0 || env.maria() => u64::from(s.ty.len).saturating_mul(v.max(0) as u64),
        Some(_) => unknown,
        None if null_count && !env.maria() => 0,
        None => unknown,
    };
    let ty = str_type(
        chars,
        Dtc {
            coll: s.ty.coll,
            deriv: s.ty.deriv,
            mb3: s.ty.mb3,
        },
        true,
    );
    Ok(Node::new(ty, name, move |ctx, row| {
        let Some(b) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(count) = n.int_typed(ctx, row)? else {
            return Ok(Value::Null);
        };
        if (!count.unsigned && count.v < 1) || b.is_empty() {
            return Ok(wrap(Vec::new(), binary));
        }
        let times = usize::try_from(count.wide()).unwrap_or(usize::MAX);
        if (b.len() as u64).saturating_mul(times as u64) > ctx.session.max_allowed_packet {
            return Ok(too_big(ctx, name));
        }
        Ok(wrap(b.repeat(times), binary))
    }))
}

/// `STRCMP(a, b)`.
pub(crate) fn strcmp(a: Node, b: Node, env: Env<'_>) -> Result<Node> {
    let (a, b) = (string_node(a, env), string_node(b, env));
    let coll = dtc_of(&[&a, &b], "strcmp", true, env)?.coll;
    let [a, b]: [Node; 2] = recode_checked(vec![a, b], coll, "strcmp", env)?
        .try_into()
        .map_err(|_| SqlError::param_count("strcmp"))?;
    let ty = int32(2, env).with_null(a.ty.nullable || b.ty.nullable);
    Ok(Node::new(ty, "strcmp", move |ctx, row| {
        let Some(x) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(y) = text(b.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        Ok(Value::Int(match coll.compare(&x, &y) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }))
    }))
}

/// `COLLATION(x)`, `CHARSET(x)` and `COERCIBILITY(x)`: what the type
/// derivation decided, for the argument as it stands.
pub(crate) fn describe(name: &str, a: Node, env: Env<'_>) -> Node {
    let dtc = if a.ty.fbt.is_some() {
        // MariaDB's `UUID`, `INET4` and `INET6` describe themselves as
        // `latin1_swedish_ci`, held as loosely as numbers are.
        Dtc::new(Collation::Latin1SwedishCi, Derivation::Numeric)
    } else if a.ty.kind == Kind::Str || a.ty.kind == Kind::Null {
        Dtc::of(&a.ty, env.conn())
    } else {
        // A number is held as loosely as numbers are, except where a
        // function says otherwise (MySQL's LOCATE and INSTR).
        Dtc::new(Collation::Binary, a.ty.deriv)
    };
    let sys = ExprType::string(64, Collation::Utf8mb4GeneralCi, Derivation::SysConst).nullable();
    match name {
        "COLLATION" => {
            let n = dtc.name();
            Node::new(sys, "collation", move |_, _| Ok(Value::str(&n)))
        }
        "CHARSET" => Node::new(sys, "charset", move |_, _| {
            Ok(Value::str(dtc.charset_name()))
        }),
        _ => {
            let maria = env.maria();
            let n = match (dtc.deriv, maria) {
                (Derivation::Explicit, _) => 0,
                (Derivation::None, _) => 1,
                (Derivation::Implicit, _) => 2,
                (Derivation::SysConst, _) => 3,
                (Derivation::Cast, true) => 4,
                (Derivation::Cast, false) => 2,
                (Derivation::Coercible, true) => 6,
                (Derivation::Coercible, false) => 4,
                (Derivation::Numeric, true) => 7,
                (Derivation::Numeric, false) => 5,
                (Derivation::Ignorable, true) => 8,
                (Derivation::Ignorable, false) => 6,
            };
            Node::new(int32(10, env), "coercibility", move |_, _| {
                Ok(Value::Int(n))
            })
        }
    }
}

/// `HEX(x)`: a string's bytes, or a number's integer value, in upper-case
/// hexadecimal.
pub(crate) fn hex(a: Node, env: Env<'_>) -> Node {
    // A UUID or address: its bytes as written (MariaDB's 32 or 8 digits).
    if let Some(f) = a.ty.fbt {
        // (Typed by the text's length, as MariaDB does.)
        let ty = ExprType {
            dec: 0,
            ..ExprType::string(f.text_len() * 2, env.conn(), Derivation::Coercible)
        }
        .nullable();
        let from = a.ty;
        return Node::new(ty, "hex", move |ctx, row| {
            let v = a.eval(ctx, row)?;
            Ok(match crate::expr::fbt_stored(f, &v, &from) {
                Some(b) => {
                    let mut h = String::with_capacity(b.len() * 2);
                    for x in f.written(&b) {
                        let _ = write!(h, "{x:02X}");
                    }
                    Value::Str(Bytes::from(h.into_bytes()))
                }
                None => Value::Null,
            })
        });
    }
    let numeric = a.ty.kind != Kind::Str && a.ty.kind != Kind::Null;
    let chars = if numeric {
        16
    } else {
        a.ty.len.saturating_mul(mb_max_len(a.ty.coll) * 2)
    };
    let ty = ExprType {
        dec: if env.maria() {
            0
        } else {
            crate::types::NOT_FIXED
        },
        ..ExprType::string(chars, env.conn(), Derivation::Coercible)
    }
    .nullable();
    let via_double = env.maria() && numeric && !a.ty.kind.is_int();
    Node::new(ty, "hex", move |ctx, row| {
        if via_double {
            // MariaDB reads a decimal or a double as a double, rounds
            // halves away from zero by adding a half in double arithmetic
            // (which itself rounds near 2^53), and answers all ones
            // outside the 64-bit range.
            return Ok(match a.f64(ctx, row)? {
                None => Value::Null,
                Some(x) => {
                    let r = (x + if x > 0.0 { 0.5 } else { -0.5 }).trunc();
                    let bits =
                        if r >= 18_446_744_073_709_551_616.0 || r <= -9_223_372_036_854_775_808.0 {
                            u64::MAX
                        } else if r >= 0.0 {
                            r as u64
                        } else {
                            (r as i64) as u64
                        };
                    Value::str(&format!("{bits:X}"))
                }
            });
        }
        if numeric {
            return Ok(match a.int(ctx, row)? {
                None => Value::Null,
                Some(i) => Value::str(&format!("{:X}", i.v as u64)),
            });
        }
        Ok(match text(a.eval(ctx, row)?) {
            None => Value::Null,
            Some(b) => {
                let mut s = String::with_capacity(b.len() * 2);
                for x in b.iter() {
                    s.push_str(&format!("{x:02X}"));
                }
                Value::str(&s)
            }
        })
    })
}
