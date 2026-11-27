//! Common functions apps use in statements: UUID conversions, base64,
//! IPv4 and IPv6 addresses, `BIT_COUNT`, `SOUNDEX`, the day-number and
//! seconds conversions (`FROM_DAYS`, `MAKEDATE`, `TIME_TO_SEC`,
//! `SEC_TO_TIME`), and the session's own values (`DATABASE()`, `USER()`,
//! `CONNECTION_ID()`, `LAST_INSERT_ID()`, `VERSION()`, `ROW_COUNT()`,
//! `FOUND_ROWS()`), which the caller puts into [`crate::Session::info`].

use std::fmt::Write as _;

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{Date, Time, Value};

use super::string::{text, wrap};
use crate::error::{Result, SqlError};
use crate::expr::{Env, Node, string_node};
use crate::temporal;
use crate::types::{Derivation, ExprType, mb_max_len};

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// The 16 bytes of a UUID's text: 32 hex digits, with or without the
/// four dashes in their places, optionally in braces.
fn uuid_bytes(s: &[u8]) -> Option<[u8; 16]> {
    let s = if s.first() == Some(&b'{') && s.last() == Some(&b'}') && s.len() == 38 {
        &s[1..37]
    } else {
        s
    };
    let digits: Vec<u8> = if s.len() == 36 {
        for (i, c) in s.iter().enumerate() {
            if matches!(i, 8 | 13 | 18 | 23) != (*c == b'-') {
                return None;
            }
        }
        s.iter().copied().filter(|c| *c != b'-').collect()
    } else if s.len() == 32 {
        s.to_vec()
    } else {
        return None;
    };
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = (hex_val(digits[2 * i])? << 4) | hex_val(digits[2 * i + 1])?;
    }
    Some(out)
}

fn swap_flag(
    ctx: &mut crate::session::Ctx<'_>,
    n: Option<&Node>,
    row: &[Value],
) -> Result<Option<bool>> {
    match n {
        None => Ok(Some(false)),
        Some(n) => Ok(n.int_arg(ctx, row)?.map(|v| v != 0)),
    }
}

/// `UUID_TO_BIN(s [, swap])` (MySQL).
pub(crate) fn uuid_to_bin(a: Node, swap: Option<Node>, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let ty = ExprType::string(16, Collation::Binary, Derivation::Coercible).nullable();
    Node::new(ty, "uuid_to_bin", move |ctx, row| {
        let v = a.eval(ctx, row)?;
        let Some(s) = text(v) else {
            return Ok(Value::Null);
        };
        let Some(sw) = swap_flag(ctx, swap.as_ref(), row)? else {
            return Ok(Value::Null);
        };
        let Some(mut b) = uuid_bytes(&s) else {
            return Err(SqlError::new(
                1411,
                "HY000",
                format!(
                    "Incorrect string value: '{}' for function uuid_to_bin",
                    String::from_utf8_lossy(&s)
                ),
            ));
        };
        if sw {
            // time-high, time-mid, time-low first.
            let mut o = [0u8; 16];
            o[..2].copy_from_slice(&b[6..8]);
            o[2..4].copy_from_slice(&b[4..6]);
            o[4..8].copy_from_slice(&b[..4]);
            o[8..].copy_from_slice(&b[8..]);
            b = o;
        }
        Ok(Value::Bytes(b.to_vec().into()))
    })
}

/// `BIN_TO_UUID(b [, swap])` (MySQL).
pub(crate) fn bin_to_uuid(a: Node, swap: Option<Node>, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let ty = ExprType::string(36, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    Node::new(ty, "bin_to_uuid", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(sw) = swap_flag(ctx, swap.as_ref(), row)? else {
            return Ok(Value::Null);
        };
        if s.len() != 16 {
            let mut shown = String::with_capacity(s.len() * 2);
            for c in s.iter() {
                let _ = write!(shown, "{c:02X}");
            }
            return Err(SqlError::new(
                1411,
                "HY000",
                format!("Incorrect string value: '{shown}' for function bin_to_uuid"),
            ));
        }
        let mut b = [0u8; 16];
        b.copy_from_slice(&s);
        if sw {
            let mut o = [0u8; 16];
            o[..4].copy_from_slice(&b[4..8]);
            o[4..6].copy_from_slice(&b[2..4]);
            o[6..8].copy_from_slice(&b[..2]);
            o[8..].copy_from_slice(&b[8..]);
            b = o;
        }
        let mut out = String::with_capacity(36);
        for (i, x) in b.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                out.push('-');
            }
            let _ = write!(out, "{x:02x}");
        }
        Ok(wrap(out.into_bytes(), binary))
    })
}

/// `IS_UUID(s)` (MySQL).
pub(crate) fn is_uuid(a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    Node::new(
        ExprType::int(1, false).nullable(),
        "is_uuid",
        move |ctx, row| {
            Ok(match text(a.eval(ctx, row)?) {
                Some(s) => Value::Int(i64::from(uuid_bytes(&s).is_some())),
                None => Value::Null,
            })
        },
    )
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// `TO_BASE64(s)`: a newline after every 76 characters, as both servers
/// write it.
pub(crate) fn to_base64(a: Node, env: Env<'_>) -> Node {
    let n = u64::from(a.ty.len) * u64::from(mb_max_len(a.ty.coll));
    let a = string_node(a, env);
    let chars = n.div_ceil(3) * 4;
    let len = (chars + chars.saturating_sub(1) / 76).min(u64::from(u32::MAX >> 2)) as u32;
    let ty = ExprType::string(len, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    Node::new(ty, "to_base64", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let mut out = Vec::with_capacity(s.len().div_ceil(3) * 4 + s.len() / 57);
        let mut col = 0;
        for c in s.chunks(3) {
            if col == 76 {
                out.push(b'\n');
                col = 0;
            }
            let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
            let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
            out.push(B64[(n >> 18) as usize & 63]);
            out.push(B64[(n >> 12) as usize & 63]);
            out.push(if c.len() > 1 {
                B64[(n >> 6) as usize & 63]
            } else {
                b'='
            });
            out.push(if c.len() > 2 {
                B64[n as usize & 63]
            } else {
                b'='
            });
            col += 4;
        }
        Ok(wrap(out, binary))
    })
}

/// `FROM_BASE64(s)`: `NULL` for text that isn't base64.
pub(crate) fn from_base64(a: Node, env: Env<'_>) -> Node {
    let n = a.ty.len.saturating_mul(mb_max_len(a.ty.coll));
    let a = string_node(a, env);
    let ty = ExprType::string(n / 4 * 3, Collation::Binary, Derivation::Coercible).nullable();
    Node::new(ty, "from_base64", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let mut bits = 0u32;
        let mut nbits = 0;
        let mut out = Vec::with_capacity(s.len() / 4 * 3);
        let mut pad = 0;
        for &c in s.iter() {
            let v = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => {
                    pad += 1;
                    continue;
                }
                b' ' | b'\n' | b'\r' | b'\t' => continue,
                _ => return Ok(Value::Null),
            };
            if pad > 0 {
                return Ok(Value::Null);
            }
            bits = (bits << 6) | u32::from(v);
            nbits += 6;
            if nbits >= 8 {
                nbits -= 8;
                out.push((bits >> nbits) as u8);
            }
        }
        if pad > 2 || (s.iter().filter(|c| !c.is_ascii_whitespace()).count() % 4) != 0 {
            return Ok(Value::Null);
        }
        Ok(Value::Bytes(out.into()))
    })
}

/// `INET_ATON(s)`: `a.b.c.d` (or fewer parts, the last one filling the
/// rest) as a number; `NULL` for anything else.
pub(crate) fn inet_aton(a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let ty = ExprType::int(21, true).nullable();
    let mysql = !env.maria();
    Node::new(ty, "inet_aton", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let v = aton(&s);
        // MySQL warns about what isn't an address.
        if v.is_null() && mysql {
            ctx.warn(
                1411,
                format!(
                    "Incorrect string value: '{}' for function inet_aton",
                    String::from_utf8_lossy(&s)
                ),
            );
        }
        Ok(v)
    })
}

fn aton(s: &[u8]) -> Value {
    {
        let parts: Vec<&[u8]> = s.split(|c| *c == b'.').collect();
        if parts.is_empty() || parts.len() > 4 || s.last() == Some(&b'.') {
            return Value::Null;
        }
        let mut vals = Vec::with_capacity(4);
        for p in &parts {
            if p.is_empty() || p.len() > 3 || !p.iter().all(u8::is_ascii_digit) {
                return Value::Null;
            }
            let v: u64 = std::str::from_utf8(p)
                .ok()
                .and_then(|t| t.parse().ok())
                .unwrap_or(256);
            if v > 255 {
                return Value::Null;
            }
            vals.push(v);
        }
        // `a.b` is `a.0.0.b`: the last part fills the low bytes.
        let last = vals.pop().unwrap_or(0);
        let mut n = 0u64;
        for (i, v) in vals.iter().enumerate() {
            n |= v << (24 - 8 * i);
        }
        Value::UInt(n | last)
    }
}

/// `INET_NTOA(n)`.
pub(crate) fn inet_ntoa(a: Node, env: Env<'_>) -> Node {
    let mut ty = ExprType::string(31, env.conn(), Derivation::Coercible).nullable();
    if env.maria() {
        ty.dec = 0;
    }
    let binary = env.conn() == Collation::Binary;
    Node::new(ty, "inet_ntoa", move |ctx, row| {
        let Some(v) = a.int_arg(ctx, row)? else {
            return Ok(Value::Null);
        };
        let Ok(n) = u32::try_from(v) else {
            return Ok(Value::Null);
        };
        let s = format!(
            "{}.{}.{}.{}",
            n >> 24,
            (n >> 16) & 255,
            (n >> 8) & 255,
            n & 255
        );
        Ok(wrap(s.into_bytes(), binary))
    })
}

/// `BIT_COUNT(n)`.
pub(crate) fn bit_count(a: Node, env: Env<'_>) -> Node {
    let mut ty = if env.maria() {
        ExprType {
            width: 4,
            ..ExprType::int(2, false)
        }
    } else {
        ExprType::int(21, false)
    };
    ty.nullable = a.ty.nullable;
    Node::new(ty, "bit_count", move |ctx, row| {
        Ok(match a.int_typed(ctx, row)? {
            Some(i) => Value::Int(i64::from((i.v as u64).count_ones())),
            None => Value::Null,
        })
    })
}

/// `FROM_DAYS(n)`: the date of day number `n` (`0000-00-00` before day
/// 366, as both servers give it).
pub(crate) fn from_days(a: Node) -> Node {
    Node::new(ExprType::date().nullable(), "from_days", move |ctx, row| {
        let Some(n) = a.int_arg(ctx, row)? else {
            return Ok(Value::Null);
        };
        if n < 366 || n > 3_652_424 {
            return Ok(Value::Date(Date::default()));
        }
        Ok(match temporal::date_from_daynr(n) {
            Some((y, m, d)) if y <= 9999 => Value::Date(Date {
                year: y as u16,
                month: m as u8,
                day: d as u8,
            }),
            _ => Value::Null,
        })
    })
}

/// `MAKEDATE(year, day of year)`.
pub(crate) fn makedate(y: Node, d: Node) -> Node {
    Node::new(ExprType::date().nullable(), "makedate", move |ctx, row| {
        let (Some(mut year), Some(day)) = (y.int_arg(ctx, row)?, d.int_arg(ctx, row)?) else {
            return Ok(Value::Null);
        };
        if day <= 0 || !(0..=9999).contains(&year) {
            return Ok(Value::Null);
        }
        if year < 100 {
            year += if year < 70 { 2000 } else { 1900 };
        }
        let nr = temporal::daynr(year as u32, 1, 1) + day - 1;
        Ok(match temporal::date_from_daynr(nr) {
            Some((yy, m, dd)) if yy <= 9999 => Value::Date(Date {
                year: yy as u16,
                month: m as u8,
                day: dd as u8,
            }),
            _ => Value::Null,
        })
    })
}

/// `SEC_TO_TIME(n)` for an integer `n`, within `TIME`'s range (clipped
/// with warning 1292 as both servers do).
pub(crate) fn sec_to_time(a: Node) -> Node {
    Node::new(
        ExprType::time(0).nullable(),
        "sec_to_time",
        move |ctx, row| {
            let Some(n) = a.int_arg(ctx, row)? else {
                return Ok(Value::Null);
            };
            const MAX: i64 = 838 * 3600 + 59 * 60 + 59;
            let c = n.clamp(-MAX, MAX);
            if c != n {
                ctx.warn(1292, format!("Truncated incorrect time value: '{n}'"));
            }
            Ok(Value::Time(
                Time::from_micros(c * 1_000_000).unwrap_or_default(),
            ))
        },
    )
}

/// 64 bits that differ on every call (the process's random hasher keys
/// over a counter).
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    h.finish()
}

/// 16 bytes as a UUID's text.
fn uuid_text(b: [u8; 16]) -> String {
    let h = b.iter().fold(String::with_capacity(32), |mut h, x| {
        let _ = write!(h, "{x:02x}");
        h
    });
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// Which `UUID` function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UuidKind {
    /// `UUID()`: version 1 (time, clock sequence, node).
    V1,
    /// `UUID_V4()`: random.
    V4,
    /// `UUID_V7()`: Unix milliseconds, then random.
    V7,
}

/// A new UUID of `kind`'s layout.
fn new_uuid(kind: UuidKind) -> String {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let (a, r) = (random_u64(), random_u64());
    match kind {
        UuidKind::V1 => {
            // 100 ns intervals since 1582-10-15.
            let ts = (t.as_nanos() / 100) as u64 + 0x01B2_1DD2_1381_4000;
            format!(
                "{:08x}-{:04x}-1{:03x}-{:04x}-{:012x}",
                ts & 0xffff_ffff,
                (ts >> 32) & 0xffff,
                (ts >> 48) & 0x0fff,
                ((r >> 48) & 0x3fff) | 0x8000,
                r & 0xffff_ffff_ffff
            )
        }
        UuidKind::V4 | UuidKind::V7 => {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&a.to_be_bytes());
            b[8..].copy_from_slice(&r.to_be_bytes());
            if kind == UuidKind::V7 {
                let ms = u64::try_from(t.as_millis()).unwrap_or(u64::MAX);
                b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
            }
            b[6] = (b[6] & 0x0f) | if kind == UuidKind::V7 { 0x70 } else { 0x40 };
            b[8] = (b[8] & 0x3f) | 0x80;
            uuid_text(b)
        }
    }
}

/// `UUID()` (version 1), MariaDB's `UUID_V4()` and `UUID_V7()`. On
/// MariaDB the result is a `UUID` value (so `UUID() + 1` is 4078); on
/// MySQL `UUID()` is a string.
pub(crate) fn uuid(env: Env<'_>, kind: UuidKind) -> Node {
    let name = match kind {
        UuidKind::V1 => "uuid()",
        UuidKind::V4 => "uuid_v4()",
        UuidKind::V7 => "uuid_v7()",
    };
    if env.maria() {
        let ty = ExprType::fbt_of(sprig_engine::row::Fbt::Uuid);
        return Node::new(ty, name, move |_, _| Ok(Value::str(&new_uuid(kind))));
    }
    let ty = ExprType::string(36, env.conn(), Derivation::Coercible);
    let binary = env.conn() == Collation::Binary;
    Node::new(ty, name, move |_, _| {
        Ok(wrap(new_uuid(kind).into_bytes(), binary))
    })
}

/// `UUID_SHORT()`: an unsigned 64-bit number that grows by one per call,
/// MariaDB's layout: the server id's low byte, the process's start in
/// seconds, then a counter (`(id & 255) << 56 | start << 24`, plus one
/// per call).
pub(crate) fn uuid_short() -> Node {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    static BASE: OnceLock<u64> = OnceLock::new();
    static SEQ: AtomicU64 = AtomicU64::new(0);
    Node::new(ExprType::int(21, true), "uuid_short()", |_, _| {
        let base = *BASE.get_or_init(|| {
            let start = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            (1u64 << 56) | ((start & 0xff_ffff_ffff) << 24)
        });
        Ok(Value::UInt(
            base.wrapping_add(SEQ.fetch_add(1, Ordering::Relaxed)),
        ))
    })
}

/// `ADDTIME(t, d)` and `SUBTIME(t, d)`: the duration `d` (a time) added
/// to or taken from `t`. A `DATETIME`, `TIMESTAMP` or `DATE` gives a
/// `DATETIME`, a `TIME` a `TIME`; anything else is read as a datetime
/// when its text has a date and a time, else as a time, and answered as
/// text. `NULL` when `d` has a date part or a datetime leaves years 1 to
/// 9999; a time past 838:59:59 is cut to it with 1292.
pub(crate) fn addtime(a: Node, b: Node, sub: bool, env: Env<'_>) -> Node {
    use crate::types::Kind;
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Out {
        DateTime,
        Time,
        Text,
    }
    // A string literal brings the digits it writes (`'1:00:01.5'`: one).
    let fsp = temporal::fsp_of(&a, env)
        .max(temporal::fsp_of(&b, env))
        .min(6);
    let out = match a.ty.kind {
        Kind::DateTime | Kind::Timestamp | Kind::Date => Out::DateTime,
        Kind::Time => Out::Time,
        _ => Out::Text,
    };
    let ty = match out {
        Out::DateTime => ExprType::datetime(fsp),
        Out::Time => ExprType::time(fsp),
        Out::Text => {
            let mut t = ExprType::string(
                if env.maria() { 26 } else { 29 },
                env.conn(),
                Derivation::Coercible,
            );
            t.dec = env.s.persona.not_fixed_dec();
            t.fixed = true;
            t
        }
    }
    .nullable();
    let name = if sub { "subtime" } else { "addtime" };
    Node::new(ty, name, move |ctx, row| {
        let (x, y) = (a.temporal(ctx, row)?, b.temporal(ctx, row)?);
        if matches!(x, Value::Null) || matches!(y, Value::Null) {
            return Ok(Value::Null);
        }
        // The duration: a time, never a date.
        let dur_dated = match &y {
            Value::Date(_) | Value::DateTime(_) => true,
            Value::Str(t) | Value::Bytes(t) => {
                temporal::parse_datetime_mode(ctx, t, temporal::DateMode::Fuzzy)
                    .is_some_and(|(tm, _)| tm.has_date && tm.has_time)
            }
            _ => false,
        };
        if dur_dated {
            return Ok(Value::Null);
        }
        let Some(d) = temporal::to_time(ctx, &y) else {
            return Ok(Value::Null);
        };
        let mut micros = tm_micros(&d);
        if sub {
            micros = -micros;
        }
        let fraction = |v: &Value| match v {
            Value::Str(t) | Value::Bytes(t) => t.contains(&b'.'),
            _ => false,
        };
        let digits = if fsp > 0 || fraction(&x) || fraction(&y) {
            6
        } else {
            0
        };
        let dated = match out {
            Out::DateTime => true,
            Out::Time => false,
            Out::Text => match &x {
                Value::Date(_) | Value::DateTime(_) => true,
                Value::Str(t) | Value::Bytes(t) => {
                    temporal::parse_datetime_mode(ctx, t, temporal::DateMode::Fuzzy)
                        .is_some_and(|(tm, _)| tm.has_date && tm.has_time)
                }
                _ => false,
            },
        };
        if dated {
            let Some(t) = temporal::to_datetime_mode(ctx, &x, temporal::DateMode::Fuzzy) else {
                return Ok(Value::Null);
            };
            let iv = super::datetime::Interval { months: 0, micros };
            let Some(r) = super::datetime::add_interval(&t, iv, false) else {
                return Ok(Value::Null);
            };
            if r.year == 0 {
                return Ok(Value::Null);
            }
            return Ok(match out {
                Out::Text => Value::str(&temporal::fmt_datetime(
                    r.datetime(),
                    if r.micros != 0 { 6 } else { digits },
                )),
                _ => temporal::tm_value(ctx, r, Kind::DateTime, fsp),
            });
        }
        let Some(t) = temporal::to_time(ctx, &x) else {
            return Ok(Value::Null);
        };
        let total = tm_micros(&t) + micros;
        let max = Time::MAX_MICROS;
        let c = total.clamp(-max, max);
        if c != total {
            ctx.warn(1292, "Truncated incorrect time value".to_owned());
        }
        let v = Time::from_micros(c).unwrap_or_default();
        Ok(match out {
            Out::Text => Value::str(&temporal::fmt_time(
                v,
                if c % 1_000_000 != 0 { 6 } else { digits },
            )),
            _ => Value::Time(v),
        })
    })
}

/// The session values a statement's functions read.
#[derive(Debug, Clone, Default)]
pub struct SessionInfo {
    /// `DATABASE()`; `None` is `NULL`.
    pub database: Option<String>,
    /// `USER()`: `name@host`.
    pub user: String,
    /// `CURRENT_USER()`.
    pub current_user: String,
    /// `CONNECTION_ID()`.
    pub connection_id: u64,
    /// `LAST_INSERT_ID()`.
    pub last_insert_id: u64,
    /// `VERSION()`.
    pub version: String,
    /// `ROW_COUNT()`.
    pub row_count: i64,
    /// `FOUND_ROWS()`.
    pub found_rows: u64,
    /// The sequences `NEXTVAL()` and its kin read; `None` where none are
    /// served.
    pub sequences: Option<std::sync::Arc<dyn super::sequence::Sequences>>,
    /// The user variables a statement reads and assigns (`@v`, `@v :=
    /// e`); `None` where none are served.
    pub user_vars: Option<std::sync::Arc<dyn super::uservar::UserVars>>,
    /// The full-text indexes `MATCH ... AGAINST` searches; `None` where
    /// none are served.
    pub fulltext: Option<std::sync::Arc<dyn super::fulltext::FullText>>,
}

/// The session's own values: `DATABASE()`, `USER()`, ... (none takes an
/// argument).
pub(crate) fn session_value(name: &str, env: Env<'_>) -> Result<Node> {
    let conn = env.conn();
    let s = |chars: u32| ExprType::string(chars, conn, Derivation::Implicit);
    let st = |v: &str| Value::Str(v.as_bytes().to_vec().into());
    let (ty, text): (ExprType, &'static str) = match name {
        "DATABASE" | "SCHEMA" => (s(34).nullable(), "database()"),
        "CONNECTION_ID" => (
            ExprType::int(if env.maria() { 10 } else { 21 }, true),
            "connection_id()",
        ),
        "LAST_INSERT_ID" => (ExprType::int(21, true), "last_insert_id()"),
        "ROW_COUNT" => (ExprType::int(21, false), "row_count()"),
        "FOUND_ROWS" => (ExprType::int(21, true), "found_rows()"),
        "USER" | "SESSION_USER" | "SYSTEM_USER" => {
            (s(if env.maria() { 384 } else { 288 }), "user()")
        }
        "CURRENT_USER" => (s(if env.maria() { 384 } else { 288 }), "current_user()"),
        "VERSION" => {
            let n = u32::try_from(env.s.info.version.chars().count()).unwrap_or(0);
            let mut t = s(n.max(1));
            t.nullable = false;
            (t, "version()")
        }
        _ => return Err(SqlError::no_function(name)),
    };
    // Read when the statement runs: the session's values as of then.
    Ok(Node::new(ty, text, move |ctx, _| {
        let info = &ctx.session.info;
        Ok(match text {
            "database()" => info.database.as_deref().map_or(Value::Null, st),
            "connection_id()" => Value::UInt(info.connection_id),
            "user()" => st(&info.user),
            "current_user()" => st(&info.current_user),
            "version()" => st(&info.version),
            "row_count()" => Value::Int(info.row_count),
            "found_rows()" => {
                // MySQL notes the deprecation once per statement.
                const DEPRECATED: &str = "FOUND_ROWS() is deprecated and will be removed in a future release. Consider using COUNT(*) instead.";
                if !ctx.maria() && !ctx.warnings().iter().any(|w| w.message == DEPRECATED) {
                    ctx.warn(1287, DEPRECATED);
                }
                Value::UInt(ctx.session.info.found_rows)
            }
            _ => Value::UInt(set_insert_id(info.connection_id).unwrap_or(info.last_insert_id)),
        })
    }))
}

/// `LAST_INSERT_ID(expr)` values set by the statements running now, by
/// connection: the session takes its value when the statement ends
/// ([`take_set_insert_id`]).
fn set_insert_ids() -> &'static std::sync::Mutex<std::collections::HashMap<u64, u64>> {
    static M: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, u64>>> =
        std::sync::OnceLock::new();
    M.get_or_init(Default::default)
}

fn set_insert_id(conn: u64) -> Option<u64> {
    if conn == 0 {
        return None;
    }
    set_insert_ids()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&conn)
        .copied()
}

/// The value the running statement of connection `conn` gave
/// `LAST_INSERT_ID(expr)`, if any, forgotten here: the session's
/// `LAST_INSERT_ID()` from now on.
#[must_use]
pub fn take_set_insert_id(conn: u64) -> Option<u64> {
    set_insert_ids()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&conn)
}

/// `LAST_INSERT_ID(expr)`: `expr` as an integer (`NULL` stays `NULL`),
/// which becomes the session's `LAST_INSERT_ID()` (0 for `NULL`), also for
/// the rest of the statement. It is kept by the session's connection id;
/// with none (a statement run as the system) only the value is returned.
pub(crate) fn set_last_insert_id(a: Node, _env: Env<'_>) -> Result<Node> {
    Ok(Node::new(
        ExprType::int(21, true).nullable(),
        "last_insert_id",
        move |ctx, row| {
            let v = a.int_arg(ctx, row)?;
            #[allow(clippy::cast_sign_loss)] // the servers keep the bits
            let id = v.map_or(0, |x| x as u64);
            let conn = ctx.session.info.connection_id;
            if conn != 0 {
                set_insert_ids()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(conn, id);
            }
            Ok(v.map_or(Value::Null, |_| Value::UInt(id)))
        },
    ))
}

/// A time's signed microseconds.
fn tm_micros(t: &temporal::Tm) -> i64 {
    let v = ((i64::from(t.hour) * 60 + i64::from(t.minute)) * 60 + i64::from(t.second)) * 1_000_000
        + i64::from(t.micros);
    if t.neg { -v } else { v }
}

/// The fractional digits a temporal argument carries (its type's, or 6
/// for text).
fn fsp_of(n: &Node) -> u8 {
    use crate::types::Kind;
    match n.ty.kind {
        Kind::Time | Kind::DateTime | Kind::Timestamp => n.ty.dec.min(6),
        _ => 0,
    }
}

/// `TIME_TO_SEC(t)`: the seconds of a time. MariaDB keeps the fraction
/// (a decimal when the argument has one); MySQL drops it.
pub(crate) fn time_to_sec(a: Node, env: Env<'_>) -> Node {
    use crate::types::Kind;
    let maria = env.maria();
    let frac =
        maria && matches!(a.ty.kind, Kind::Time | Kind::DateTime | Kind::Timestamp) && a.ty.dec > 0;
    let ty = if frac {
        ExprType::decimal(16 + u32::from(a.ty.dec), u32::from(a.ty.dec), false).nullable()
    } else {
        ExprType::int(if maria { 17 } else { 10 }, false).nullable()
    };
    let dec = a.ty.dec;
    Node::new(ty, "time_to_sec", move |ctx, row| {
        let v = a.time_arg(ctx, row)?;
        let Some(t) = temporal::to_time(ctx, &v) else {
            return Ok(Value::Null);
        };
        let us = tm_micros(&t);
        if frac {
            let d = usize::from(dec.min(6));
            let neg = us < 0;
            let abs = us.unsigned_abs();
            let int: Vec<u8> = (abs / 1_000_000)
                .to_string()
                .bytes()
                .map(|b| b - b'0')
                .collect();
            let frac: Vec<u8> = format!("{:06}", abs % 1_000_000)
                .bytes()
                .take(d)
                .map(|b| b - b'0')
                .collect();
            return Ok(sprig_engine::row::Decimal::from_parts(neg, &int, &frac)
                .map_or(Value::Null, Value::Decimal));
        }
        Ok(Value::Int(us / 1_000_000))
    })
}

/// `MAKETIME(h, m, s)`: `NULL` for minutes or seconds outside 0..59.
pub(crate) fn maketime(h: Node, m: Node, sec: Node) -> Node {
    Node::new(ExprType::time(0).nullable(), "maketime", move |ctx, row| {
        let (Some(h), Some(m), Some(sv)) = (
            h.int_arg(ctx, row)?,
            m.int_arg(ctx, row)?,
            sec.int_arg(ctx, row)?,
        ) else {
            return Ok(Value::Null);
        };
        if !(0..60).contains(&m) || !(0..60).contains(&sv) {
            return Ok(Value::Null);
        }
        let total = (h.abs().min(1_000_000) * 3600 + m * 60 + sv) * 1_000_000;
        let max = Time::MAX_MICROS - 999_999;
        let c = total.min(max);
        if c != total {
            ctx.warn(
                1292,
                format!("Truncated incorrect time value: '{h}:{m:02}:{sv:02}'"),
            );
        }
        let c = if h < 0 { -c } else { c };
        Ok(Value::Time(Time::from_micros(c).unwrap_or_default()))
    })
}

/// `TIMEDIFF(a, b)`: `a - b` as a time, when both are times or both are
/// dates with times; `NULL` for a mix.
pub(crate) fn timediff(a: Node, b: Node) -> Node {
    let fsp = fsp_of(&a).max(fsp_of(&b)).min(6);
    Node::new(
        ExprType::time(fsp).nullable(),
        "timediff",
        move |ctx, row| {
            let (x, y) = (a.temporal(ctx, row)?, b.temporal(ctx, row)?);
            let full = |ctx: &mut crate::Ctx<'_>, v: &Value| -> Option<(bool, i64)> {
                let dated = matches!(v, Value::Date(_) | Value::DateTime(_))
                    || matches!(v, Value::Str(s) | Value::Bytes(s) if s.contains(&b'-'));
                if dated {
                    let t = temporal::to_datetime_mode(ctx, v, temporal::DateMode::Fuzzy)?;
                    let days = temporal::daynr(t.year, t.month, t.day);
                    Some((
                        true,
                        days * 86_400_000_000 + tm_micros(&temporal::Tm { neg: false, ..t }),
                    ))
                } else {
                    temporal::to_time(ctx, v).map(|t| (false, tm_micros(&t)))
                }
            };
            let (Some((dx, ux)), Some((dy, uy))) = (full(ctx, &x), full(ctx, &y)) else {
                return Ok(Value::Null);
            };
            if dx != dy {
                return Ok(Value::Null);
            }
            let d = ux - uy;
            let max = Time::MAX_MICROS;
            let c = d.clamp(-max, max);
            if c != d {
                ctx.warn(1292, "Truncated incorrect time value".to_owned());
            }
            Ok(Value::Time(Time::from_micros(c).unwrap_or_default()))
        },
    )
}

/// `SOUNDEX(s)`: the servers' code: the first letter, then the digits of
/// the following consonants, runs of one digit once, at least four long.
pub(crate) fn soundex(a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let chars = a.ty.len.max(4);
    let ty = ExprType::string(chars, env.conn(), Derivation::Coercible).nullable();
    Node::new(ty, "soundex", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let code = |c: char| -> u8 {
            match c.to_ascii_uppercase() {
                'B' | 'F' | 'P' | 'V' => b'1',
                'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => b'2',
                'D' | 'T' => b'3',
                'L' => b'4',
                'M' | 'N' => b'5',
                'R' => b'6',
                _ => b'0',
            }
        };
        let mut out = String::new();
        let mut last = 0u8;
        for c in String::from_utf8_lossy(&s)
            .chars()
            .filter(|c| c.is_alphabetic())
        {
            if out.is_empty() {
                out.push(c.to_ascii_uppercase());
                last = code(c);
                continue;
            }
            // A digit is written when it differs from the last one
            // written (vowels don't separate runs, as on the servers).
            let d = code(c);
            if d != b'0' && d != last {
                out.push(d as char);
                last = d;
            }
        }
        if out.is_empty() {
            return Ok(Value::Str(Vec::new().into()));
        }
        while out.len() < 4 {
            out.push('0');
        }
        Ok(Value::Str(out.into_bytes().into()))
    })
}

/// `INET6_ATON(s)`: an IPv6 address as 16 bytes, an IPv4 one as 4;
/// `NULL` for anything else.
pub(crate) fn inet6_aton(a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let ty = ExprType::string(16, Collation::Binary, Derivation::Coercible).nullable();
    let maria = env.maria();
    Node::new(ty, "inet6_aton", move |ctx, row| {
        let Some(s) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let s = String::from_utf8_lossy(&s);
        if let Ok(v4) = s.parse::<std::net::Ipv4Addr>() {
            return Ok(Value::Bytes(v4.octets().to_vec().into()));
        }
        if let Ok(v6) = s.parse::<std::net::Ipv6Addr>() {
            return Ok(Value::Bytes(v6.octets().to_vec().into()));
        }
        // MySQL warns; MariaDB doesn't.
        if !maria {
            ctx.warn(
                1411,
                format!("Incorrect string value: '{s}' for function inet6_aton"),
            );
        }
        Ok(Value::Null)
    })
}

/// `INET6_NTOA(b)`: 4 or 16 bytes as an address's text.
pub(crate) fn inet6_ntoa(a: Node, env: Env<'_>) -> Node {
    let mut ty = ExprType::string(39, env.conn(), Derivation::Coercible).nullable();
    if env.maria() {
        ty.dec = 0;
    }
    Node::new(ty, "inet6_ntoa", move |ctx, row| {
        let (Value::Bytes(b) | Value::Str(b)) = a.eval(ctx, row)? else {
            return Ok(Value::Null);
        };
        let out = match b.len() {
            4 => std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]).to_string(),
            16 => {
                let mut o = [0u8; 16];
                o.copy_from_slice(&b);
                let v6 = std::net::Ipv6Addr::from(o);
                // The servers write an IPv4-compatible address (`::a.b.c.d`)
                // with its IPv4 part.
                if o[..12].iter().all(|x| *x == 0)
                    && o[12..].iter().any(|x| *x != 0)
                    && !(o[12] == 0 && o[13] == 0 && o[14] == 0)
                {
                    format!("::{}.{}.{}.{}", o[12], o[13], o[14], o[15])
                } else {
                    v6.to_string()
                }
            }
            _ => return Ok(Value::Null),
        };
        Ok(wrap(out.into_bytes(), false))
    })
}

#[cfg(test)]
mod uuid_tests {
    use super::{UuidKind, new_uuid};

    #[test]
    fn uuids_carry_their_version_and_variant() {
        for (k, v) in [
            (UuidKind::V1, '1'),
            (UuidKind::V4, '4'),
            (UuidKind::V7, '7'),
        ] {
            let u = new_uuid(k);
            assert_eq!(u.len(), 36, "{u}");
            assert_eq!(u.as_bytes()[14] as char, v, "{u}");
            assert!(matches!(u.as_bytes()[19], b'8' | b'9' | b'a' | b'b'), "{u}");
            assert_ne!(new_uuid(k), u);
        }
        // Version 7 starts with the time: later ones sort after.
        let a = new_uuid(UuidKind::V7);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(new_uuid(UuidKind::V7) > a);
    }
}
