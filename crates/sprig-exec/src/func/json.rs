//! JSON functions: `JSON_EXTRACT` (and `->`, `->>`), `JSON_UNQUOTE`,
//! `JSON_VALUE`, `JSON_TYPE`, `JSON_VALID`, `JSON_LENGTH`, `JSON_KEYS`,
//! `JSON_OBJECT`, `JSON_ARRAY`, `JSON_QUOTE`, `JSON_CONTAINS`.
//!
//! A document is parsed into [`J`] and printed the way each server prints
//! a JSON result: `", "` and `": "` between members; MySQL also orders an
//! object's keys (shorter first, then by bytes, the last of a duplicate
//! key kept) and prints numbers with a fraction or exponent as doubles,
//! where MariaDB keeps the document's order and number text.

use bytes::Bytes;
use sprig_engine::row::Value;

use super::string::text;
use crate::error::{Result, SqlError};
use crate::expr::{Env, Node, string_node};
use crate::session::Ctx;
use crate::types::{Derivation, ExprType};

/// A JSON value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum J {
    Null,
    Bool(bool),
    /// A number as written.
    Num(String),
    Str(String),
    Arr(Vec<J>),
    Obj(Vec<(String, J)>),
}

struct P<'a> {
    b: &'a [u8],
    i: usize,
}

impl P<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn lit(&mut self, w: &[u8]) -> bool {
        if self.b[self.i..].starts_with(w) {
            self.i += w.len();
            true
        } else {
            false
        }
    }

    fn string(&mut self) -> Option<String> {
        // At the opening quote.
        self.i += 1;
        let mut out: Vec<u8> = Vec::new();
        while self.i < self.b.len() {
            let c = self.b[self.i];
            self.i += 1;
            match c {
                b'"' => return String::from_utf8(out).ok(),
                b'\\' => {
                    let e = *self.b.get(self.i)?;
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let hex = |p: &mut Self| -> Option<u32> {
                                let h = std::str::from_utf8(p.b.get(p.i..p.i + 4)?).ok()?;
                                p.i += 4;
                                u32::from_str_radix(h, 16).ok()
                            };
                            let mut cp = hex(self)?;
                            if (0xD800..0xDC00).contains(&cp) && self.lit(b"\\u") {
                                let lo = hex(self)?;
                                cp = 0x10000
                                    + ((cp - 0xD800) << 10)
                                    + (lo.wrapping_sub(0xDC00) & 0x3ff);
                            }
                            let ch = char::from_u32(cp)?;
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        _ => return None,
                    }
                }
                c if c < 0x20 => return None,
                c => out.push(c),
            }
        }
        None
    }

    fn number(&mut self) -> Option<String> {
        let start = self.i;
        if self.b.get(self.i) == Some(&b'-') {
            self.i += 1;
        }
        let digits = |p: &mut Self| {
            let s = p.i;
            while p.i < p.b.len() && p.b[p.i].is_ascii_digit() {
                p.i += 1;
            }
            p.i > s
        };
        if !digits(self) {
            return None;
        }
        if self.b.get(self.i) == Some(&b'.') {
            self.i += 1;
            if !digits(self) {
                return None;
            }
        }
        if matches!(self.b.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.b.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if !digits(self) {
                return None;
            }
        }
        std::str::from_utf8(&self.b[start..self.i])
            .ok()
            .map(str::to_owned)
    }

    fn value(&mut self, depth: usize) -> Option<J> {
        if depth > 100 {
            return None;
        }
        self.ws();
        let c = *self.b.get(self.i)?;
        Some(match c {
            b'{' => {
                self.i += 1;
                let mut m = Vec::new();
                self.ws();
                if self.lit(b"}") {
                    return Some(J::Obj(m));
                }
                loop {
                    self.ws();
                    if self.b.get(self.i) != Some(&b'"') {
                        return None;
                    }
                    let k = self.string()?;
                    self.ws();
                    if !self.lit(b":") {
                        return None;
                    }
                    let v = self.value(depth + 1)?;
                    m.push((k, v));
                    self.ws();
                    if self.lit(b",") {
                        continue;
                    }
                    if self.lit(b"}") {
                        break;
                    }
                    return None;
                }
                J::Obj(m)
            }
            b'[' => {
                self.i += 1;
                let mut a = Vec::new();
                self.ws();
                if self.lit(b"]") {
                    return Some(J::Arr(a));
                }
                loop {
                    a.push(self.value(depth + 1)?);
                    self.ws();
                    if self.lit(b",") {
                        continue;
                    }
                    if self.lit(b"]") {
                        break;
                    }
                    return None;
                }
                J::Arr(a)
            }
            b'"' => J::Str(self.string()?),
            b't' if self.lit(b"true") => J::Bool(true),
            b'f' if self.lit(b"false") => J::Bool(false),
            b'n' if self.lit(b"null") => J::Null,
            _ => J::Num(self.number()?),
        })
    }
}

/// Parses a whole document.
pub(crate) fn parse(b: &[u8]) -> Option<J> {
    let mut p = P { b, i: 0 };
    let v = p.value(0)?;
    p.ws();
    (p.i == b.len()).then_some(v)
}

/// MySQL's form of a document: keys ordered, the last duplicate kept,
/// numbers with a fraction or exponent as doubles.
pub(crate) fn normalize(j: J) -> J {
    match j {
        J::Obj(m) => {
            let mut out: Vec<(String, J)> = Vec::new();
            for (k, v) in m {
                let v = normalize(v);
                match out.iter_mut().find(|(x, _)| *x == k) {
                    Some(slot) => slot.1 = v,
                    None => out.push((k, v)),
                }
            }
            out.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(&b.0)));
            J::Obj(out)
        }
        J::Arr(a) => J::Arr(a.into_iter().map(normalize).collect()),
        J::Num(n) if n.contains(['.', 'e', 'E']) => match n.parse::<f64>() {
            Ok(f) => J::Num(double_text(f)),
            Err(_) => J::Num(n),
        },
        other => other,
    }
}

/// A double as MySQL prints it in JSON (`2.5`, `100.0`, `1e100`).
fn double_text(f: f64) -> String {
    if f.is_finite() && f == f.trunc() && f.abs() < 1e15 {
        format!("{f:.1}")
    } else if f.abs() >= 1e15 || (f != 0.0 && f.abs() < 1e-5) {
        format!("{f:e}")
    } else {
        format!("{f}")
    }
}

fn quote_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = std::fmt::Write::write_fmt(out, format_args!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Prints `j` as the servers print a JSON result.
pub(crate) fn print(j: &J, out: &mut String) {
    match j {
        J::Null => out.push_str("null"),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Num(n) => out.push_str(n),
        J::Str(s) => quote_into(s, out),
        J::Arr(a) => {
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                print(v, out);
            }
            out.push(']');
        }
        J::Obj(m) => {
            out.push('{');
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                quote_into(k, out);
                out.push_str(": ");
                print(v, out);
            }
            out.push('}');
        }
    }
}

/// A stored JSON document's text as MySQL prints it (keys ordered, its
/// spacing); the bytes unchanged when they aren't a document.
pub(crate) fn mysql_text(b: &Bytes) -> Bytes {
    match parse(b) {
        Some(j) => Bytes::from(printed(&normalize(j)).into_bytes()),
        None => b.clone(),
    }
}

fn printed(j: &J) -> String {
    let mut s = String::new();
    print(j, &mut s);
    s
}

// ---- MySQL's comparison of JSON values ----

/// The order of JSON types in MySQL's comparison, lowest first: `null`,
/// numbers, strings, objects, arrays, booleans.
fn rank(j: &J) -> u8 {
    match j {
        J::Null => 0,
        J::Num(_) => 1,
        J::Str(_) => 2,
        J::Obj(_) => 3,
        J::Arr(_) => 4,
        J::Bool(_) => 5,
    }
}

/// Two JSON numbers compared by value: exactly for integers and decimal
/// text, as doubles otherwise.
fn num_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(x), Ok(y)) = (a.parse::<i128>(), b.parse::<i128>()) {
        return x.cmp(&y);
    }
    match (crate::num::Dec::parse(a), crate::num::Dec::parse(b)) {
        (Some(x), Some(y)) => x.cmp(&y),
        _ => {
            let (x, y) = (
                a.parse::<f64>().unwrap_or(0.0),
                b.parse::<f64>().unwrap_or(0.0),
            );
            x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
        }
    }
}

/// MySQL's order of two JSON values: by type ([`rank`]), then numbers by
/// value, strings by their bytes, booleans `false` first, arrays element
/// by element (a prefix first), objects by their member count, then by
/// their members in key order.
pub(crate) fn mysql_cmp(a: &J, b: &J) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (J::Num(x), J::Num(y)) => num_cmp(x, y),
        (J::Str(x), J::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (J::Bool(x), J::Bool(y)) => x.cmp(y),
        (J::Null, J::Null) => Ordering::Equal,
        (J::Arr(x), J::Arr(y)) => {
            for (p, q) in x.iter().zip(y) {
                let o = mysql_cmp(p, q);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (J::Obj(_), J::Obj(_)) => {
            let (J::Obj(x), J::Obj(y)) = (normalize(a.clone()), normalize(b.clone())) else {
                return Ordering::Equal;
            };
            x.len().cmp(&y.len()).then_with(|| {
                for ((kx, vx), (ky, vy)) in x.iter().zip(&y) {
                    let o = kx
                        .as_bytes()
                        .cmp(ky.as_bytes())
                        .then_with(|| mysql_cmp(vx, vy));
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                Ordering::Equal
            })
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

/// Bytes that are equal exactly when MySQL compares two JSON values as
/// equal (grouping, `DISTINCT`, hash joins): numbers by value, objects
/// with their keys ordered.
pub(crate) fn mysql_hash(j: &J, out: &mut Vec<u8>) {
    out.push(rank(j));
    match j {
        J::Null => {}
        J::Bool(b) => out.push(u8::from(*b)),
        J::Num(n) => {
            // The same number in any notation gives the same bytes.
            let mut t = if crate::num::Dec::parse(n).is_some() {
                n.clone()
            } else {
                n.parse::<f64>()
                    .map_or_else(|_| n.clone(), |f| format!("{f}"))
            };
            if t.contains('.') {
                while t.ends_with('0') {
                    t.pop();
                }
                if t.ends_with('.') {
                    t.pop();
                }
            }
            if t == "-0" {
                t = "0".to_owned();
            }
            out.extend_from_slice(t.as_bytes());
            out.push(0xff);
        }
        J::Str(s) => {
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        J::Arr(a) => {
            out.extend_from_slice(&(a.len() as u32).to_be_bytes());
            for v in a {
                mysql_hash(v, out);
            }
        }
        J::Obj(_) => {
            if let J::Obj(m) = normalize(j.clone()) {
                out.extend_from_slice(&(m.len() as u32).to_be_bytes());
                for (k, v) in &m {
                    out.extend_from_slice(&(k.len() as u32).to_be_bytes());
                    out.extend_from_slice(k.as_bytes());
                    mysql_hash(v, out);
                }
            }
        }
    }
}

/// A value as MySQL compares it with a JSON value: a JSON value (of a
/// JSON type) is its document; other text is a JSON string, numbers are
/// numbers, dates and times their text. `None` for `NULL`.
pub(crate) fn compared(v: &Value, ty: &ExprType) -> Option<J> {
    Some(match v {
        Value::Null => return None,
        Value::Json(b) => parse(b).unwrap_or(J::Null),
        Value::Str(b) | Value::Bytes(b) if ty.json => {
            parse(b).unwrap_or_else(|| J::Str(String::from_utf8_lossy(b).into_owned()))
        }
        Value::Str(b) | Value::Bytes(b) => J::Str(String::from_utf8_lossy(b).into_owned()),
        Value::Int(n) => J::Num(n.to_string()),
        Value::UInt(n) | Value::Bit(n) | Value::Set(n) => J::Num(n.to_string()),
        Value::Year(n) | Value::Enum(n) => J::Num(n.to_string()),
        other => {
            let s = crate::value_text(other, ty, crate::Persona::MySql)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            if matches!(
                other,
                Value::Decimal(_) | Value::Double(_) | Value::Float(_)
            ) {
                J::Num(s)
            } else {
                J::Str(s)
            }
        }
    })
}

/// MySQL's order of two values one of which is a JSON value (a stored
/// document, `Value::Json`); the other is read as a JSON scalar. `None`
/// when either is `NULL`.
#[must_use]
pub fn json_order(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    let ty = |v: &Value| match v {
        Value::Double(_) | Value::Float(_) => ExprType::double(),
        _ => ExprType::null(),
    };
    let x = compared(a, &ty(a))?;
    let y = compared(b, &ty(b))?;
    Some(mysql_cmp(&x, &y))
}

/// A JSON value's text, as a comparison key kept as a value.
pub(crate) fn key_text(j: &J) -> Bytes {
    Bytes::from(printed(j).into_bytes())
}

// ---- paths ----

#[derive(Debug, Clone, PartialEq)]
enum Idx {
    N(usize),
    /// `last - n`.
    Last(usize),
}

#[derive(Debug, Clone, PartialEq)]
enum Leg {
    Key(String),
    AnyKey,
    Index(Idx),
    AnyIndex,
    /// `**`.
    Desc,
}

/// A path; `None` when it isn't one.
fn parse_path(s: &str) -> Option<Vec<Leg>> {
    let b = s.trim().as_bytes();
    if b.first() != Some(&b'$') {
        return None;
    }
    let mut i = 1;
    let mut legs = Vec::new();
    let ws = |i: &mut usize| {
        while *i < b.len() && b[*i] == b' ' {
            *i += 1;
        }
    };
    loop {
        ws(&mut i);
        if i >= b.len() {
            break;
        }
        match b[i] {
            b'.' => {
                i += 1;
                ws(&mut i);
                if b.get(i) == Some(&b'*') {
                    i += 1;
                    legs.push(Leg::AnyKey);
                } else if b.get(i) == Some(&b'"') {
                    let mut p = P { b, i };
                    legs.push(Leg::Key(p.string()?));
                    i = p.i;
                } else {
                    let start = i;
                    while i < b.len()
                        && (b[i].is_ascii_alphanumeric()
                            || b[i] == b'_'
                            || b[i] == b'$'
                            || b[i] >= 0x80)
                    {
                        i += 1;
                    }
                    if i == start || b[start].is_ascii_digit() {
                        return None;
                    }
                    legs.push(Leg::Key(std::str::from_utf8(&b[start..i]).ok()?.to_owned()));
                }
            }
            b'[' => {
                i += 1;
                ws(&mut i);
                let close = i + b[i..].iter().position(|c| *c == b']')?;
                let inner = std::str::from_utf8(&b[i..close]).ok()?.trim();
                i = close + 1;
                if inner == "*" {
                    legs.push(Leg::AnyIndex);
                } else if let Some(rest) = inner.strip_prefix("last") {
                    let rest = rest.trim();
                    let off = if rest.is_empty() {
                        0
                    } else {
                        rest.strip_prefix('-')?.trim().parse().ok()?
                    };
                    legs.push(Leg::Index(Idx::Last(off)));
                } else {
                    legs.push(Leg::Index(Idx::N(inner.parse().ok()?)));
                }
            }
            b'*' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                legs.push(Leg::Desc);
            }
            _ => return None,
        }
    }
    if legs.last() == Some(&Leg::Desc) {
        return None;
    }
    Some(legs)
}

fn wild(path: &[Leg]) -> bool {
    path.iter()
        .any(|l| matches!(l, Leg::AnyKey | Leg::AnyIndex | Leg::Desc))
}

/// The values `path` selects in `j`, in document order.
fn select<'a>(j: &'a J, path: &[Leg], out: &mut Vec<&'a J>) {
    select_in(j, path, out);
}

/// MariaDB's paths have no `last`, and don't read a scalar or an object
/// as an array of one.
fn maria_skips(j: &J, p: &[Leg]) -> bool {
    p.iter().any(|l| matches!(l, Leg::Index(Idx::Last(_))))
        || (p.first().is_some_and(|l| matches!(l, Leg::Index(_))) && !matches!(j, J::Arr(_)))
}

fn select_in<'a>(j: &'a J, path: &[Leg], out: &mut Vec<&'a J>) {
    let Some((leg, rest)) = path.split_first() else {
        if !out.iter().any(|x| std::ptr::eq(*x, j)) {
            out.push(j);
        }
        return;
    };
    match (leg, j) {
        (Leg::Key(k), J::Obj(m)) => {
            if let Some((_, v)) = m.iter().rev().find(|(x, _)| x == k) {
                select(v, rest, out);
            }
        }
        (Leg::AnyKey, J::Obj(m)) => {
            for (_, v) in m {
                select(v, rest, out);
            }
        }
        (Leg::Index(ix), J::Arr(a)) => {
            let at = match ix {
                Idx::N(n) => Some(*n),
                Idx::Last(off) => a.len().checked_sub(1 + off),
            };
            if let Some(v) = at.and_then(|n| a.get(n)) {
                select(v, rest, out);
            }
        }
        // A scalar is an array of one at index 0 (and `last`).
        (Leg::Index(Idx::N(0) | Idx::Last(0)), v) if !matches!(v, J::Arr(_)) => {
            select(v, rest, out);
        }
        (Leg::AnyIndex, J::Arr(a)) => {
            for v in a {
                select(v, rest, out);
            }
        }
        (Leg::Desc, v) => {
            select(v, rest, out);
            match v {
                J::Obj(m) => {
                    for (_, c) in m {
                        select(c, path, out);
                    }
                }
                J::Arr(a) => {
                    for c in a {
                        select(c, path, out);
                    }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ---- reading arguments ----

fn invalid_doc(maria: bool, func: &str, n: usize) -> SqlError {
    if maria {
        SqlError::new(
            4038,
            "HY000",
            format!("Syntax error in JSON text in argument {n} to function '{func}'"),
        )
    } else {
        SqlError::new(
            3141,
            "22032",
            format!(
                "Invalid JSON text in argument {n} to function {func}: \"Invalid value.\" at position 0."
            ),
        )
    }
}

fn invalid_path(maria: bool, func: &str, n: usize) -> SqlError {
    if maria {
        SqlError::new(
            4042,
            "HY000",
            format!("Syntax error in JSON path in argument {n} to function '{func}'"),
        )
    } else {
        SqlError::new(
            3143,
            "42000",
            "Invalid JSON path expression. The error is around character position 1.",
        )
    }
}

/// The document argument `n` (1-based) of `func`: `Ok(None)` for `NULL`;
/// MariaDB warns and gives `NULL` for text that isn't JSON, MySQL fails.
fn doc(
    ctx: &mut Ctx<'_>,
    a: &Node,
    row: &[Value],
    maria: bool,
    func: &str,
    n: usize,
) -> Result<Option<J>> {
    let Some(b) = text(a.eval(ctx, row)?) else {
        return Ok(None);
    };
    match parse(&b) {
        Some(j) => Ok(Some(j)),
        None if maria => {
            let e = invalid_doc(true, func, n);
            ctx.warn(e.code, e.message);
            Ok(None)
        }
        None => Err(invalid_doc(false, func, n)),
    }
}

fn path(
    ctx: &mut Ctx<'_>,
    a: &Node,
    row: &[Value],
    maria: bool,
    func: &str,
    n: usize,
) -> Result<Option<Vec<Leg>>> {
    let Some(b) = text(a.eval(ctx, row)?) else {
        return Ok(None);
    };
    match parse_path(&String::from_utf8_lossy(&b)) {
        Some(p) => Ok(Some(p)),
        None if maria => {
            let e = invalid_path(true, func, n);
            ctx.warn(e.code, e.message);
            Ok(None)
        }
        None => Err(invalid_path(false, func, n)),
    }
}

/// A SQL value as a JSON value: JSON text from a JSON column or another
/// JSON function stays JSON; text is a string; numbers are numbers.
fn as_json(ctx: &mut Ctx<'_>, a: &Node, row: &[Value]) -> Result<J> {
    let v = a.eval(ctx, row)?;
    let json_node =
        a.text.starts_with("json_") || a.text.starts_with("JSON_") || (a.ty.json && !ctx.maria());
    Ok(match v {
        Value::Null => J::Null,
        Value::Json(b) => parse(&b).unwrap_or(J::Null),
        Value::Str(b) | Value::Bytes(b) if json_node => {
            parse(&b).unwrap_or_else(|| J::Str(String::from_utf8_lossy(&b).into_owned()))
        }
        Value::Str(b) | Value::Bytes(b) => J::Str(String::from_utf8_lossy(&b).into_owned()),
        Value::Int(n) => J::Num(n.to_string()),
        Value::UInt(n) => J::Num(n.to_string()),
        other => {
            let s = crate::value_text(&other, &a.ty, crate::Persona::MySql)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            if matches!(
                other,
                Value::Decimal(_) | Value::Double(_) | Value::Float(_)
            ) {
                J::Num(s)
            } else {
                J::Str(s)
            }
        }
    })
}

/// A JSON document result: MySQL's `JSON` type; MariaDB's long text.
fn json_type(env: Env<'_>) -> ExprType {
    ExprType {
        json: !env.maria(),
        ..ExprType::string(u32::MAX / 4, env.conn(), Derivation::Implicit).nullable()
    }
}

/// A result built from document `doc`: MariaDB sizes it at twice the
/// document (at most 16M characters) in the document's collation; MySQL's
/// is a JSON value.
fn doc_type(doc: &Node, env: Env<'_>) -> ExprType {
    if !env.maria() {
        return json_type(env);
    }
    let chars = doc.ty.len.saturating_mul(2).clamp(1, 16 << 20);
    let coll = if doc.ty.kind == crate::types::Kind::Str {
        doc.ty.coll
    } else {
        env.conn()
    };
    ExprType::string(chars, coll, Derivation::Implicit).nullable()
}

/// An integer result (`INT` on MariaDB).
fn count_type(env: Env<'_>) -> ExprType {
    ExprType {
        prec: 10,
        width: if env.maria() { 4 } else { 0 },
        ..ExprType::int(10, false)
    }
    .nullable()
}

fn out_value(j: &J, maria: bool) -> Value {
    let j = if maria {
        j.clone()
    } else {
        normalize(j.clone())
    };
    Value::Str(Bytes::from(printed(&j).into_bytes()))
}

// ---- the functions ----

/// `JSON_EXTRACT(doc, path, ...)`.
pub(crate) fn extract(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 2 {
        return Err(SqlError::param_count("JSON_EXTRACT"));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_extract",
        move |ctx, row| {
            let Some(d) = doc(ctx, &nodes[0], row, maria, "json_extract", 1)? else {
                return Ok(Value::Null);
            };
            let d = if maria { d } else { normalize(d) };
            let mut found = Vec::new();
            let mut many = nodes.len() > 2;
            for (i, p) in nodes[1..].iter().enumerate() {
                let Some(p) = path(ctx, p, row, maria, "json_extract", i + 2)? else {
                    return Ok(Value::Null);
                };
                many |= wild(&p);
                if maria && maria_skips(&d, &p) {
                    continue;
                }
                select(&d, &p, &mut found);
            }
            Ok(match found.as_slice() {
                [] => Value::Null,
                [one] if !many => out_value(one, maria),
                all => out_value(&J::Arr(all.iter().map(|j| (*j).clone()).collect()), maria),
            })
        },
    ))
}

/// The text of a JSON string unquoted; anything else as it is.
fn unquoted(b: &[u8]) -> Vec<u8> {
    if b.len() >= 2
        && b[0] == b'"'
        && b[b.len() - 1] == b'"'
        && let Some(J::Str(s)) = parse(b)
    {
        return s.into_bytes();
    }
    b.to_vec()
}

/// MySQL's `CAST(a AS JSON)` (and `JSON_COMPACT(a)`, which is what the
/// MySQL port's text rewrite makes of a cast of a constant): text is parsed
/// as a document (3141 when it isn't one, 3144 for a binary string), a JSON
/// value stays itself, numbers are numbers and other values their text as
/// a JSON string. The result is a JSON value in MySQL's form.
pub(crate) fn cast_json(a: Node, env: Env<'_>) -> Node {
    let ty = json_type(env);
    let text = format!("cast({} as json)", a.text);
    Node::new(ty, text, move |ctx, row| {
        let j = match a.eval(ctx, row)? {
            Value::Null => return Ok(Value::Null),
            Value::Json(b) => parse(&b).unwrap_or(J::Null),
            Value::Str(b) => parse(&b).ok_or_else(|| invalid_doc(false, "cast_as_json", 1))?,
            Value::Bytes(b) if a.ty.json => {
                parse(&b).ok_or_else(|| invalid_doc(false, "cast_as_json", 1))?
            }
            Value::Bytes(_) => {
                return Err(SqlError::new(
                    3144,
                    "22032",
                    "Cannot create a JSON value from a string with CHARACTER SET 'binary'.",
                ));
            }
            Value::Int(n) => J::Num(n.to_string()),
            Value::UInt(n) => J::Num(n.to_string()),
            other => {
                let s = crate::value_text(&other, &a.ty, crate::Persona::MySql)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default();
                match other {
                    Value::Decimal(_)
                    | Value::Double(_)
                    | Value::Float(_)
                    | Value::Year(_)
                    | Value::Bit(_)
                    | Value::Enum(_)
                    | Value::Set(_) => J::Num(s),
                    _ => J::Str(s),
                }
            }
        };
        Ok(out_value(&j, false))
    })
}

fn compact_into(j: &J, out: &mut String) {
    match j {
        J::Arr(a) => {
            out.push('[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                compact_into(v, out);
            }
            out.push(']');
        }
        J::Obj(m) => {
            out.push('{');
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                quote_into(k, out);
                out.push(':');
                compact_into(v, out);
            }
            out.push('}');
        }
        other => print(other, out),
    }
}

/// `JSON_COMPACT(doc)` on MariaDB: the document without spaces, in its own
/// order and number text; text that isn't JSON warns (4038) and gives
/// `NULL`. On MySQL it is the JSON cast (see [`cast_json`]).
pub(crate) fn compact(a: Node, env: Env<'_>) -> Node {
    if !env.maria() {
        return cast_json(a, env);
    }
    let ty = doc_type(&a, env);
    Node::new(ty, "json_compact", move |ctx, row| {
        let Some(j) = doc(ctx, &a, row, true, "json_compact", 1)? else {
            return Ok(Value::Null);
        };
        let mut s = String::new();
        compact_into(&j, &mut s);
        Ok(Value::Str(Bytes::from(s.into_bytes())))
    })
}

/// `JSON_UNQUOTE(s)`.
pub(crate) fn unquote(a: Node, env: Env<'_>) -> Node {
    // A JSON value's text: binary text of any length.
    let ty = if a.ty.json {
        ExprType::string(
            crate::types::JSON_TEXT_LEN,
            sprig_engine::keys::collation::Collation::Utf8mb4Bin,
            Derivation::Implicit,
        )
        .nullable()
    } else {
        ExprType::string(a.ty.len.max(1), env.conn(), Derivation::Implicit).nullable()
    };
    let a = string_node(a, env);
    Node::new(ty, "json_unquote", move |ctx, row| {
        Ok(match text(a.eval(ctx, row)?) {
            Some(b) => Value::Str(Bytes::from(unquoted(&b))),
            None => Value::Null,
        })
    })
}

/// `JSON_VALUE(doc, path)`: a scalar's text; `NULL` for an object or array.
pub(crate) fn value(d: Node, p: Node, env: Env<'_>) -> Node {
    let maria = env.maria();
    let ty = ExprType::string(
        if maria { u32::MAX / 4 } else { 512 },
        env.conn(),
        Derivation::Implicit,
    )
    .nullable();
    Node::new(ty, "json_value", move |ctx, row| {
        let Some(j) = doc(ctx, &d, row, maria, "json_value", 1)? else {
            return Ok(Value::Null);
        };
        let Some(path) = path(ctx, &p, row, maria, "json_value", 2)? else {
            return Ok(Value::Null);
        };
        let mut found = Vec::new();
        select(&j, &path, &mut found);
        Ok(match found.first() {
            Some(J::Str(s)) => Value::Str(Bytes::from(s.clone().into_bytes())),
            Some(J::Num(n)) => Value::Str(Bytes::from(n.clone().into_bytes())),
            Some(J::Bool(b)) => Value::Str(Bytes::from_static(if *b { b"true" } else { b"false" })),
            Some(v @ (J::Arr(_) | J::Obj(_))) if !maria => out_value(v, false),
            _ => Value::Null,
        })
    })
}

/// What `JSON_VALUE(doc, path RETURNING type)` returns (MySQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Returning {
    Signed,
    Unsigned,
    Decimal(u8, u8),
    Double,
    Float,
    /// `CHAR` (`None`: no length given).
    Char(Option<u32>),
    Binary(Option<u32>),
    Date,
    Time(u8),
    DateTime(u8),
    Year,
    Json,
}

impl Returning {
    /// From the planner's keyword (`RETURNING SIGNED`, `RETURNING
    /// DECIMAL(5,2)`, ...).
    pub(crate) fn parse(k: &str) -> Option<Self> {
        let t = k.strip_prefix("RETURNING ")?.trim();
        let (word, arg) = match t.split_once('(') {
            Some((w, rest)) => (w.trim(), Some(rest.trim_end_matches(')'))),
            None => (t, None),
        };
        let n = |a: Option<&str>| a.and_then(|a| a.trim().parse::<u32>().ok());
        Some(match word {
            "SIGNED" => Returning::Signed,
            "UNSIGNED" => Returning::Unsigned,
            "DECIMAL" => {
                let (p, sc) = match arg {
                    Some(a) => match a.split_once(',') {
                        Some((p, sc)) => (n(Some(p))?, n(Some(sc))?),
                        None => (n(Some(a))?, 0),
                    },
                    None => (10, 0),
                };
                let p = p.clamp(1, 65);
                Returning::Decimal(p as u8, sc.min(30).min(p) as u8)
            }
            "DOUBLE" => Returning::Double,
            "FLOAT" => Returning::Float,
            "CHAR" => Returning::Char(n(arg)),
            "BINARY" => Returning::Binary(n(arg)),
            "DATE" => Returning::Date,
            "TIME" => Returning::Time(n(arg).unwrap_or(0).min(6) as u8),
            "DATETIME" => Returning::DateTime(n(arg).unwrap_or(0).min(6) as u8),
            "YEAR" => Returning::Year,
            "JSON" => Returning::Json,
            _ => return None,
        })
    }

    /// The result's type, as MySQL reports it.
    fn ty(self, env: Env<'_>) -> ExprType {
        use sprig_engine::keys::collation::Collation;
        match self {
            Returning::Signed => ExprType::bigint(false),
            Returning::Unsigned => ExprType {
                len: 21,
                ..ExprType::bigint(true)
            },
            Returning::Decimal(p, sc) => ExprType::decimal(u32::from(p), u32::from(sc), false),
            Returning::Double => ExprType::double(),
            Returning::Float => ExprType {
                kind: crate::types::Kind::Float,
                ..ExprType::double()
            },
            Returning::Char(n) => ExprType::string(
                n.unwrap_or(512),
                Collation::Utf8mb4Bin,
                Derivation::Implicit,
            ),
            Returning::Binary(n) => ExprType::string(
                n.unwrap_or(u32::MAX),
                Collation::Binary,
                Derivation::Implicit,
            ),
            Returning::Date => ExprType::date(),
            Returning::Time(f) => ExprType::time(f),
            Returning::DateTime(f) => ExprType::datetime(f),
            Returning::Year => ExprType {
                kind: crate::types::Kind::Year,
                unsigned: true,
                width: 1,
                ..ExprType::int(4, true)
            },
            Returning::Json => json_type(env),
        }
        .nullable()
    }
}

/// A JSON scalar's number, when it is one (a JSON number, or a string
/// that is wholly a number); booleans are 1 and 0.
fn scalar_number(j: &J) -> Option<crate::num::Dec> {
    let text = match j {
        J::Num(n) => n.clone(),
        J::Str(s) => s.trim().to_owned(),
        J::Bool(b) => return Some(crate::num::Dec::from_i64(i64::from(*b))),
        _ => return None,
    };
    crate::num::Dec::parse(&text).or_else(|| {
        let f = text.parse::<f64>().ok().filter(|f| f.is_finite())?;
        crate::num::Dec::parse(&format!("{f}"))
    })
}

/// A JSON scalar's text: a string unquoted, anything else as printed.
fn scalar_text(j: &J) -> String {
    match j {
        J::Str(s) => s.clone(),
        other => printed(&normalize(other.clone())),
    }
}

/// The found value converted to `r`; `None` when it can't be (MySQL's
/// `NULL ON ERROR`, the default: no warning).
fn returned(ctx: &Ctx<'_>, j: &J, r: Returning) -> Option<Value> {
    use crate::num::Round;
    if matches!(j, J::Null) {
        return None;
    }
    let scalar = !matches!(j, J::Arr(_) | J::Obj(_));
    Some(match r {
        Returning::Json => out_value(j, false),
        Returning::Signed | Returning::Unsigned | Returning::Year => {
            // Text must be a whole number; a JSON number is rounded.
            if let J::Str(s) = j {
                let t = s.trim();
                let digits = t.strip_prefix(['-', '+']).unwrap_or(t);
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
            }
            let d = scalar_number(j)?;
            let whole = d.round(0, Round::HalfUp).to_string();
            match r {
                Returning::Signed => Value::Int(whole.parse::<i64>().ok()?),
                Returning::Unsigned => Value::UInt(whole.parse::<u64>().ok()?),
                _ => {
                    // A year is a whole number in its range.
                    if d.to_string() != whole {
                        return None;
                    }
                    let y = whole.parse::<u16>().ok()?;
                    let y = match y {
                        0 => 0,
                        1..=69 => 2000 + y,
                        70..=99 => 1900 + y,
                        1901..=2155 => y,
                        _ => return None,
                    };
                    Value::Year(y)
                }
            }
        }
        Returning::Decimal(p, sc) => {
            let d = scalar_number(j)?.round(i32::from(sc), Round::HalfUp);
            if d.int_digits() > usize::from(p - sc) {
                return None;
            }
            crate::conv::dec_value(&d)
        }
        Returning::Double | Returning::Float => {
            let f = scalar_number(j)?.to_string().parse::<f64>().ok()?;
            if r == Returning::Float {
                #[allow(clippy::cast_possible_truncation)]
                let x = f as f32;
                if !x.is_finite() {
                    return None;
                }
                Value::Float(x)
            } else {
                Value::Double(f)
            }
        }
        Returning::Char(n) | Returning::Binary(n) => {
            let t = if scalar {
                scalar_text(j)
            } else {
                printed(&normalize(j.clone()))
            };
            if let Some(n) = n
                && t.chars().count() > n as usize
            {
                return None;
            }
            if matches!(r, Returning::Binary(_)) {
                Value::Bytes(Bytes::from(t.into_bytes()))
            } else {
                Value::Str(Bytes::from(t.into_bytes()))
            }
        }
        Returning::Date | Returning::Time(_) | Returning::DateTime(_) => {
            if !scalar {
                return None;
            }
            let (kind, fsp) = match r {
                Returning::Date => (crate::types::Kind::Date, 0),
                Returning::Time(f) => (crate::types::Kind::Time, f),
                _ => match r {
                    Returning::DateTime(f) => (crate::types::Kind::DateTime, f),
                    _ => return None,
                },
            };
            // A value read with any complaint is an error here.
            let mut scratch = Ctx::new(ctx.session);
            let v = crate::temporal::coerce(
                &mut scratch,
                Value::Str(Bytes::from(scalar_text(j).into_bytes())),
                kind,
                fsp,
            );
            if scratch.warning_count() > 0 || v.is_null() {
                return None;
            }
            v
        }
    })
}

/// `JSON_VALUE(doc, path RETURNING type)` (MySQL; MariaDB has no
/// `RETURNING`): the value at `path` converted to `type`, `NULL` when it
/// is missing, JSON `null`, or doesn't convert.
pub(crate) fn value_returning(d: Node, p: Node, r: Returning, env: Env<'_>) -> Node {
    let ty = r.ty(env);
    Node::new(ty, "json_value", move |ctx, row| {
        let Some(j) = doc(ctx, &d, row, false, "json_value", 1)? else {
            return Ok(Value::Null);
        };
        let Some(path) = path(ctx, &p, row, false, "json_value", 2)? else {
            return Ok(Value::Null);
        };
        let mut found = Vec::new();
        select(&j, &path, &mut found);
        Ok(match found.first() {
            Some(v) => returned(ctx, v, r).unwrap_or(Value::Null),
            None => Value::Null,
        })
    })
}

// ---- JSON_PRETTY and JSON_SEARCH ----

fn indent(out: &mut String, n: usize, width: usize) {
    out.extend(std::iter::repeat_n(' ', n * width));
}

/// MySQL's pretty form: two spaces a level, `{}` and `[]` when empty.
fn pretty_mysql(j: &J, level: usize, out: &mut String) {
    match j {
        J::Arr(a) if !a.is_empty() => {
            out.push_str("[\n");
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(",\n");
                }
                indent(out, level + 1, 2);
                pretty_mysql(v, level + 1, out);
            }
            out.push('\n');
            indent(out, level, 2);
            out.push(']');
        }
        J::Obj(m) if !m.is_empty() => {
            out.push_str("{\n");
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push_str(",\n");
                }
                indent(out, level + 1, 2);
                quote_into(k, out);
                out.push_str(": ");
                pretty_mysql(v, level + 1, out);
            }
            out.push('\n');
            indent(out, level, 2);
            out.push('}');
        }
        J::Arr(_) => out.push_str("[]"),
        J::Obj(_) => out.push_str("{}"),
        other => print(other, out),
    }
}

/// MariaDB's detailed form: four spaces a level; a member's container
/// value starts on its own line; an array of one scalar (or none) stays
/// on one line; an empty object still opens and closes on two lines.
fn pretty_maria(j: &J, level: usize, out: &mut String) {
    let container = |v: &J| matches!(v, J::Arr(_) | J::Obj(_));
    match j {
        J::Arr(a) if a.is_empty() || (a.len() == 1 && !container(&a[0])) => {
            out.push('[');
            if let Some(v) = a.first() {
                print(v, out);
            }
            out.push(']');
        }
        J::Arr(a) => {
            out.push_str("[\n");
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push_str(",\n");
                }
                indent(out, level + 1, 4);
                pretty_maria(v, level + 1, out);
            }
            out.push('\n');
            indent(out, level, 4);
            out.push(']');
        }
        J::Obj(m) => {
            out.push_str("{\n");
            for (i, (k, v)) in m.iter().enumerate() {
                if i > 0 {
                    out.push_str(",\n");
                }
                indent(out, level + 1, 4);
                quote_into(k, out);
                out.push_str(": ");
                if container(v) {
                    out.push('\n');
                    indent(out, level + 1, 4);
                }
                pretty_maria(v, level + 1, out);
            }
            if !m.is_empty() {
                out.push('\n');
            }
            indent(out, level, 4);
            out.push('}');
        }
        other => print(other, out),
    }
}

/// `JSON_PRETTY(doc)` (MariaDB's `JSON_DETAILED` too): the document
/// indented, each server its own way.
pub(crate) fn pretty(name: &'static str, a: Node, env: Env<'_>) -> Node {
    let maria = env.maria();
    let ty = if maria {
        ExprType {
            dec: 0,
            ..doc_type(&a, env)
        }
    } else {
        ExprType::string(
            1 << 26,
            sprig_engine::keys::collation::Collation::Utf8mb4Bin,
            Derivation::Implicit,
        )
        .nullable()
    };
    let func = name.to_ascii_lowercase();
    Node::new(ty, func.clone(), move |ctx, row| {
        let Some(j) = doc(ctx, &a, row, maria, &func, 1)? else {
            return Ok(Value::Null);
        };
        let mut out = String::new();
        if maria {
            pretty_maria(&j, 0, &mut out);
        } else {
            pretty_mysql(&normalize(j), 0, &mut out);
        }
        Ok(Value::Str(Bytes::from(out.into_bytes())))
    })
}

/// A key as a path leg: MySQL quotes a key that isn't an identifier,
/// MariaDB writes it as it is.
fn key_leg(k: &str, maria: bool, out: &mut String) {
    out.push('.');
    let ident = !k.is_empty()
        && !k.starts_with(|c: char| c.is_ascii_digit())
        && k.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$');
    if maria || ident {
        out.push_str(k);
    } else {
        quote_into(k, out);
    }
}

/// Collects the paths below `j` (written `prefix`) of every string (and,
/// on MariaDB, number) the pattern matches, in document order.
fn search_in(
    j: &J,
    prefix: &mut String,
    m: &dyn Fn(&str) -> bool,
    maria: bool,
    out: &mut Vec<String>,
) {
    match j {
        J::Str(s) if m(s) => out.push(prefix.clone()),
        J::Num(n) if maria && m(n) => out.push(prefix.clone()),
        J::Arr(a) => {
            for (i, v) in a.iter().enumerate() {
                let len = prefix.len();
                let _ = std::fmt::Write::write_fmt(prefix, format_args!("[{i}]"));
                search_in(v, prefix, m, maria, out);
                prefix.truncate(len);
            }
        }
        J::Obj(o) => {
            for (k, v) in o {
                let len = prefix.len();
                key_leg(k, maria, prefix);
                search_in(v, prefix, m, maria, out);
                prefix.truncate(len);
            }
        }
        _ => {}
    }
}

/// Every concrete location a path names in `j`, with its written path.
fn locate<'a>(j: &'a J, p: &[Leg], at: String, maria: bool, out: &mut Vec<(String, &'a J)>) {
    let Some((first, rest)) = p.split_first() else {
        out.push((at, j));
        return;
    };
    match (first, j) {
        (Leg::Key(k), J::Obj(m)) => {
            if let Some((_, v)) = m.iter().rev().find(|(x, _)| x == k) {
                let mut a = at;
                key_leg(k, maria, &mut a);
                locate(v, rest, a, maria, out);
            }
        }
        (Leg::AnyKey, J::Obj(m)) => {
            for (k, v) in m {
                let mut a = at.clone();
                key_leg(k, maria, &mut a);
                locate(v, rest, a, maria, out);
            }
        }
        (Leg::Index(i), J::Arr(a)) => {
            let n = match i {
                Idx::N(n) => Some(*n),
                Idx::Last(back) => a.len().checked_sub(1 + back),
            };
            if let Some(n) = n
                && let Some(v) = a.get(n)
            {
                locate(v, rest, format!("{at}[{n}]"), maria, out);
            }
        }
        (Leg::AnyIndex, J::Arr(a)) => {
            for (n, v) in a.iter().enumerate() {
                locate(v, rest, format!("{at}[{n}]"), maria, out);
            }
        }
        _ => {}
    }
}

/// `JSON_SEARCH(doc, one_or_all, search[, escape[, path ...]])`: the
/// paths of the strings that match `search` as a `LIKE` pattern. MySQL
/// compares bytes and searches strings only, in its key order, quoting
/// keys that aren't identifiers; MariaDB compares under the search
/// string's collation and finds numbers too, in the document's order.
pub(crate) fn search(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 3 {
        return Err(SqlError::param_count("JSON_SEARCH"));
    }
    let maria = env.maria();
    // MariaDB matches under the document's collation (a JSON column's is
    // utf8mb4_bin), or the pattern's when the document isn't text.
    let coll = if !maria {
        sprig_engine::keys::collation::Collation::Utf8mb4Bin
    } else if nodes[0].ty.kind == crate::types::Kind::Str {
        nodes[0].ty.coll
    } else {
        nodes[2].ty.coll
    };
    let ty = if maria {
        doc_type(&nodes[0], env)
    } else {
        json_type(env)
    };
    Ok(Node::new(ty, "json_search", move |ctx, row| {
        let Some(d) = doc(ctx, &nodes[0], row, maria, "json_search", 1)? else {
            return Ok(Value::Null);
        };
        let d = if maria { d } else { normalize(d) };
        let Some(mode) = text(nodes[1].eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let all = if mode.eq_ignore_ascii_case(b"all") {
            true
        } else if mode.eq_ignore_ascii_case(b"one") {
            false
        } else if maria {
            ctx.warn(
                4046,
                "Argument 2 to function 'json_search' must be \"one\" or \"all\".",
            );
            return Ok(Value::Null);
        } else {
            return Err(SqlError::new(
                3154,
                "42000",
                "The oneOrAll argument to json_search may take these values: 'one' or 'all'.",
            ));
        };
        let Some(pattern) = text(nodes[2].eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let escape = match nodes.get(3) {
            Some(e) => text(e.eval(ctx, row)?).filter(|e| !e.is_empty()),
            None => None,
        }
        .unwrap_or_else(|| Bytes::from_static(b"\\"));
        let m = |s: &str| {
            crate::expr::like::like_match(coll, s.as_bytes(), &pattern, Some(&escape[..]))
        };
        let mut found = Vec::new();
        if nodes.len() > 4 {
            let mut starts = Vec::new();
            for (i, p) in nodes[4..].iter().enumerate() {
                let Some(p) = path(ctx, p, row, maria, "json_search", i + 5)? else {
                    return Ok(Value::Null);
                };
                locate(&d, &p, "$".to_owned(), maria, &mut starts);
            }
            for (mut at, j) in starts {
                search_in(j, &mut at, &m, maria, &mut found);
            }
        } else {
            search_in(&d, &mut "$".to_owned(), &m, maria, &mut found);
        }
        // A path found twice (overlapping starts) counts once.
        let mut seen = std::collections::HashSet::new();
        found.retain(|p| seen.insert(p.clone()));
        Ok(match found.as_slice() {
            [] => Value::Null,
            [one] => out_value(&J::Str(one.clone()), maria),
            [one, ..] if !all => out_value(&J::Str(one.clone()), maria),
            many => out_value(&J::Arr(many.iter().cloned().map(J::Str).collect()), maria),
        })
    }))
}

/// `JSON_TYPE(doc)`.
pub(crate) fn type_of(a: Node, env: Env<'_>) -> Node {
    let maria = env.maria();
    let ty = ExprType::string(
        if maria { 12 } else { 51 },
        env.conn(),
        Derivation::Implicit,
    )
    .nullable();
    Node::new(ty, "json_type", move |ctx, row| {
        let Some(j) = doc(ctx, &a, row, maria, "json_type", 1)? else {
            return Ok(Value::Null);
        };
        let t = match j {
            J::Null => "NULL",
            J::Bool(_) => "BOOLEAN",
            J::Num(n) if n.contains(['.', 'e', 'E']) => "DOUBLE",
            J::Num(_) => "INTEGER",
            J::Str(_) => "STRING",
            J::Arr(_) => "ARRAY",
            J::Obj(_) => "OBJECT",
        };
        Ok(Value::Str(Bytes::from_static(t.as_bytes())))
    })
}

/// `JSON_VALID(x)`.
pub(crate) fn valid(a: Node, env: Env<'_>) -> Node {
    let _ = env;
    Node::new(
        ExprType::int(1, false).nullable(),
        "json_valid",
        move |ctx, row| {
            Ok(match a.eval(ctx, row)? {
                Value::Null => Value::Null,
                Value::Str(b) | Value::Bytes(b) | Value::Json(b) => {
                    Value::Int(i64::from(parse(&b).is_some()))
                }
                _ => Value::Int(0),
            })
        },
    )
}

/// `JSON_LENGTH(doc[, path])` and `JSON_KEYS(doc[, path])`.
pub(crate) fn length_or_keys(keys: bool, nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let name = if keys { "json_keys" } else { "json_length" };
    if nodes.is_empty() || nodes.len() > 2 {
        return Err(SqlError::param_count(&name.to_uppercase()));
    }
    let maria = env.maria();
    let ty = if keys {
        doc_type(&nodes[0], env)
    } else {
        count_type(env)
    };
    Ok(Node::new(ty, name, move |ctx, row| {
        let Some(j) = doc(ctx, &nodes[0], row, maria, name, 1)? else {
            return Ok(Value::Null);
        };
        let j = if maria || !keys { j } else { normalize(j) };
        let target = match nodes.get(1) {
            Some(p) => {
                let Some(p) = path(ctx, p, row, maria, name, 2)? else {
                    return Ok(Value::Null);
                };
                let mut found = Vec::new();
                select(&j, &p, &mut found);
                match found.first() {
                    Some(v) => (*v).clone(),
                    None => return Ok(Value::Null),
                }
            }
            None => j,
        };
        Ok(match (keys, &target) {
            (true, J::Obj(m)) => out_value(
                &J::Arr(m.iter().map(|(k, _)| J::Str(k.clone())).collect()),
                maria,
            ),
            (true, _) => Value::Null,
            (false, J::Obj(m)) => Value::Int(m.len() as i64),
            (false, J::Arr(a)) => Value::Int(a.len() as i64),
            (false, _) => Value::Int(1),
        })
    }))
}

/// `JSON_OBJECT(k, v, ...)` and `JSON_ARRAY(v, ...)`.
pub(crate) fn build(object: bool, nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if object && nodes.len() % 2 == 1 {
        return Err(SqlError::param_count("JSON_OBJECT"));
    }
    let maria = env.maria();
    let mut ty = json_type(env);
    ty.nullable = !maria;
    let name = if object { "json_object" } else { "json_array" };
    Ok(Node::new(ty, name, move |ctx, row| {
        let j = if object {
            let mut m = Vec::new();
            for kv in nodes.chunks(2) {
                let k = match kv[0].eval(ctx, row)? {
                    Value::Null => {
                        return Err(if maria {
                            SqlError::new(4044, "HY000", "JSON key cannot be NULL")
                        } else {
                            SqlError::new(
                                3158,
                                "22032",
                                "JSON documents may not contain NULL member names.",
                            )
                        });
                    }
                    // A number or temporal key is its text (`{"1": ...}`).
                    v => crate::conv::to_text(&v, &kv[0].ty, maria)
                        .map(|b| String::from_utf8_lossy(&b).into_owned())
                        .unwrap_or_default(),
                };
                m.push((k, as_json(ctx, &kv[1], row)?));
            }
            J::Obj(m)
        } else {
            let mut a = Vec::new();
            for n in &nodes {
                a.push(as_json(ctx, n, row)?);
            }
            J::Arr(a)
        };
        Ok(out_value(&j, maria))
    }))
}

/// `JSON_QUOTE(s)`.
pub(crate) fn quote(a: Node, env: Env<'_>) -> Node {
    let ty = ExprType::string(
        a.ty.len.saturating_mul(2).saturating_add(2),
        env.conn(),
        Derivation::Implicit,
    )
    .nullable();
    let a = string_node(a, env);
    Node::new(ty, "json_quote", move |ctx, row| {
        Ok(match text(a.eval(ctx, row)?) {
            Some(b) => {
                let mut s = String::new();
                quote_into(&String::from_utf8_lossy(&b), &mut s);
                Value::Str(Bytes::from(s.into_bytes()))
            }
            None => Value::Null,
        })
    })
}

fn contains(t: &J, c: &J) -> bool {
    match (t, c) {
        (J::Obj(tm), J::Obj(cm)) => cm
            .iter()
            .all(|(k, cv)| tm.iter().any(|(tk, tv)| tk == k && contains(tv, cv))),
        (J::Arr(ta), J::Arr(ca)) => ca.iter().all(|cv| ta.iter().any(|tv| contains(tv, cv))),
        (J::Arr(ta), c) => ta.iter().any(|tv| contains(tv, c)),
        (J::Num(a), J::Num(b)) => a.parse::<f64>().ok() == b.parse::<f64>().ok(),
        (a, b) => a == b,
    }
}

/// `JSON_CONTAINS(target, candidate[, path])`.
pub(crate) fn contains_fn(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 2 || nodes.len() > 3 {
        return Err(SqlError::param_count("JSON_CONTAINS"));
    }
    let maria = env.maria();
    Ok(Node::new(
        ExprType::int(1, false).nullable(),
        "json_contains",
        move |ctx, row| {
            let Some(t) = doc(ctx, &nodes[0], row, maria, "json_contains", 1)? else {
                return Ok(Value::Null);
            };
            let Some(c) = doc(ctx, &nodes[1], row, maria, "json_contains", 2)? else {
                return Ok(Value::Null);
            };
            let t = match nodes.get(2) {
                Some(p) => {
                    let Some(p) = path(ctx, p, row, maria, "json_contains", 3)? else {
                        return Ok(Value::Null);
                    };
                    let mut found = Vec::new();
                    select(&t, &p, &mut found);
                    match found.first() {
                        Some(v) => (*v).clone(),
                        None => return Ok(Value::Null),
                    }
                }
                None => t,
            };
            Ok(Value::Int(i64::from(contains(&t, &c))))
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn show(s: &str, mysql: bool) -> String {
        let j = parse(s.as_bytes()).unwrap();
        printed(&if mysql { normalize(j) } else { j })
    }

    #[test]
    fn documents_print_as_each_server_prints_them() {
        let d = r#"{"b":1,  "a":{"x":[1, 2.50,"s"]}}"#;
        assert_eq!(show(d, false), r#"{"b": 1, "a": {"x": [1, 2.50, "s"]}}"#);
        assert_eq!(show(d, true), r#"{"a": {"x": [1, 2.5, "s"]}, "b": 1}"#);
        assert_eq!(show(r#""hé\n""#, false), "\"h\u{e9}\\n\"");
        assert!(parse(b"{a}").is_none());
        assert!(parse(b"[1,]").is_none());
    }

    #[test]
    fn paths_select_in_document_order() {
        let j = parse(br#"{"a":[1,[2,3]],"b":{"c":4}}"#).unwrap();
        let mut out = Vec::new();
        select(&j, &parse_path("$.a[1][last]").unwrap(), &mut out);
        assert_eq!(out, vec![&J::Num("3".into())]);
        out.clear();
        select(&j, &parse_path("$**.c").unwrap(), &mut out);
        assert_eq!(out, vec![&J::Num("4".into())]);
        assert!(parse_path("a.b").is_none());
        assert!(parse_path("$.").is_none());
    }
}

/// How `JSON_SET`, `JSON_INSERT` and `JSON_REPLACE` treat a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Modify {
    /// Replace or add.
    Set,
    /// Add only.
    Insert,
    /// Replace only.
    Replace,
}

/// Puts `v` at `path` in `j` (simple paths: keys and indexes).
fn put(j: &mut J, path: &[Leg], v: J, how: Modify) {
    let Some((leg, rest)) = path.split_first() else {
        if how != Modify::Insert {
            *j = v;
        }
        return;
    };
    match (leg, &mut *j) {
        (Leg::Key(k), J::Obj(m)) => match m.iter_mut().rev().find(|(x, _)| x == k) {
            Some((_, c)) => put(c, rest, v, how),
            None if rest.is_empty() && how != Modify::Replace => m.push((k.clone(), v)),
            None => {}
        },
        (Leg::Index(ix), J::Arr(a)) => {
            let at = match ix {
                Idx::N(n) => Some(*n),
                Idx::Last(off) => a.len().checked_sub(1 + off),
            };
            match at {
                Some(n) if n < a.len() => put(&mut a[n], rest, v, how),
                _ if rest.is_empty() && how != Modify::Replace => a.push(v),
                _ => {}
            }
        }
        // A scalar or object read as an array of one: index 0 is itself;
        // a later index appends, making it an array.
        (Leg::Index(ix), other) => match ix {
            Idx::N(0) | Idx::Last(0) => put(other, rest, v, how),
            _ if rest.is_empty() && how != Modify::Replace => {
                let first = std::mem::replace(other, J::Null);
                *other = J::Arr(vec![first, v]);
            }
            _ => {}
        },
        _ => {}
    }
}

/// Removes what `path` names from `j`.
fn remove(j: &mut J, path: &[Leg]) {
    let Some((leg, rest)) = path.split_first() else {
        return;
    };
    match (leg, j) {
        (Leg::Key(k), J::Obj(m)) => {
            if rest.is_empty() {
                m.retain(|(x, _)| x != k);
            } else if let Some((_, c)) = m.iter_mut().rev().find(|(x, _)| x == k) {
                remove(c, rest);
            }
        }
        (Leg::Index(ix), J::Arr(a)) => {
            let at = match ix {
                Idx::N(n) => Some(*n),
                Idx::Last(off) => a.len().checked_sub(1 + off),
            };
            if let Some(n) = at.filter(|n| *n < a.len()) {
                if rest.is_empty() {
                    a.remove(n);
                } else {
                    remove(&mut a[n], rest);
                }
            }
        }
        _ => {}
    }
}

fn simple(p: &[Leg]) -> bool {
    !wild(p)
}

/// `JSON_SET` / `JSON_INSERT` / `JSON_REPLACE(doc, path, value, ...)`.
pub(crate) fn modify(how: Modify, nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let name = match how {
        Modify::Set => "json_set",
        Modify::Insert => "json_insert",
        Modify::Replace => "json_replace",
    };
    if nodes.len() < 3 || nodes.len() % 2 == 0 {
        return Err(SqlError::param_count(&name.to_uppercase()));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        name,
        move |ctx, row| {
            let Some(mut d) = doc(ctx, &nodes[0], row, maria, name, 1)? else {
                return Ok(Value::Null);
            };
            for (i, pv) in nodes[1..].chunks(2).enumerate() {
                let Some(p) = path(ctx, &pv[0], row, maria, name, 2 * i + 2)? else {
                    return Ok(Value::Null);
                };
                if !simple(&p) || p.is_empty() {
                    if maria {
                        // MariaDB warns and gives NULL.
                        ctx.warn(
                        4044,
                        format!(
                            "Wildcards or range in JSON path not allowed in argument {} to function '{name}'",
                            2 * i + 2
                        ),
                    );
                        return Ok(Value::Null);
                    }
                    return Err(SqlError::new(
                        3149,
                        "42000",
                        "In this situation, path expressions may not contain the * and ** tokens or an array range.",
                    ));
                }
                let v = as_json(ctx, &pv[1], row)?;
                put(&mut d, &p, v, how);
            }
            Ok(out_value(&d, maria))
        },
    ))
}

/// `JSON_REMOVE(doc, path, ...)`.
pub(crate) fn remove_fn(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 2 {
        return Err(SqlError::param_count("JSON_REMOVE"));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_remove",
        move |ctx, row| {
            let Some(mut d) = doc(ctx, &nodes[0], row, maria, "json_remove", 1)? else {
                return Ok(Value::Null);
            };
            for (i, p) in nodes[1..].iter().enumerate() {
                let Some(p) = path(ctx, p, row, maria, "json_remove", i + 2)? else {
                    return Ok(Value::Null);
                };
                if p.is_empty() || !simple(&p) {
                    return Err(SqlError::new(
                        if maria { 4044 } else { 3153 },
                        if maria { "HY000" } else { "42000" },
                        if maria {
                            format!(
                                "Wildcards or range in JSON path not allowed in argument {} to function 'json_remove'",
                                i + 2
                            )
                        } else {
                            "The path expression '$' is not allowed in this context.".to_owned()
                        },
                    ));
                }
                remove(&mut d, &p);
            }
            Ok(out_value(&d, maria))
        },
    ))
}

/// Appends `v` to the array at `path` (a non-array becomes `[it, v]`).
fn append_at(j: &mut J, path: &[Leg], v: J) {
    let Some((leg, rest)) = path.split_first() else {
        match j {
            J::Arr(a) => a.push(v),
            other => {
                let first = std::mem::replace(other, J::Null);
                *other = J::Arr(vec![first, v]);
            }
        }
        return;
    };
    match (leg, j) {
        (Leg::Key(k), J::Obj(m)) => {
            if let Some((_, c)) = m.iter_mut().rev().find(|(x, _)| x == k) {
                append_at(c, rest, v);
            }
        }
        (Leg::Index(ix), J::Arr(a)) => {
            let at = match ix {
                Idx::N(n) => Some(*n),
                Idx::Last(off) => a.len().checked_sub(1 + off),
            };
            if let Some(c) = at.and_then(|n| a.get_mut(n)) {
                append_at(c, rest, v);
            }
        }
        (Leg::Index(Idx::N(0) | Idx::Last(0)), other) => append_at(other, rest, v),
        _ => {}
    }
}

/// `JSON_ARRAY_APPEND(doc, path, value, ...)`.
pub(crate) fn array_append(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 3 || nodes.len() % 2 == 0 {
        return Err(SqlError::param_count("JSON_ARRAY_APPEND"));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_array_append",
        move |ctx, row| {
            let Some(mut d) = doc(ctx, &nodes[0], row, maria, "json_array_append", 1)? else {
                return Ok(Value::Null);
            };
            for (i, pv) in nodes[1..].chunks(2).enumerate() {
                let Some(p) = path(ctx, &pv[0], row, maria, "json_array_append", 2 * i + 2)? else {
                    return Ok(Value::Null);
                };
                // MariaDB: a path that names nothing gives NULL.
                if maria {
                    let mut found = Vec::new();
                    select(&d, &p, &mut found);
                    if found.is_empty() {
                        return Ok(Value::Null);
                    }
                }
                let v = as_json(ctx, &pv[1], row)?;
                append_at(&mut d, &p, v);
            }
            Ok(out_value(&d, maria))
        },
    ))
}

/// RFC 7396's merge of `patch` into `target`.
fn merge_patch(target: J, patch: J) -> J {
    let J::Obj(pm) = patch else {
        return patch;
    };
    let mut tm = match target {
        J::Obj(m) => m,
        _ => Vec::new(),
    };
    for (k, v) in pm {
        let at = tm.iter().position(|(x, _)| *x == k);
        match (v, at) {
            (J::Null, Some(i)) => {
                tm.remove(i);
            }
            (J::Null, None) => {}
            (v, Some(i)) => {
                let old = std::mem::replace(&mut tm[i].1, J::Null);
                tm[i].1 = merge_patch(old, v);
            }
            (v, None) => tm.push((k, merge_patch(J::Null, v))),
        }
    }
    J::Obj(tm)
}

/// `JSON_MERGE_PATCH(doc, doc, ...)`.
pub(crate) fn merge_patch_fn(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 2 {
        return Err(SqlError::param_count("JSON_MERGE_PATCH"));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_merge_patch",
        move |ctx, row| {
            let mut acc: Option<J> = None;
            for (i, n) in nodes.iter().enumerate() {
                let Some(d) = doc(ctx, n, row, maria, "json_merge_patch", i + 1)? else {
                    return Ok(Value::Null);
                };
                acc = Some(match acc {
                    None => d,
                    Some(a) => merge_patch(a, d),
                });
            }
            Ok(acc.map_or(Value::Null, |j| out_value(&j, maria)))
        },
    ))
}

/// `JSON_MERGE_PRESERVE` of two documents: arrays are joined, objects
/// merged with the values of a key both have merged in turn, and any other
/// pair joined as arrays (a scalar or an object is an array of one).
fn merge_preserve(a: J, b: J) -> J {
    match (a, b) {
        (J::Obj(mut am), J::Obj(bm)) => {
            for (k, v) in bm {
                match am.iter().position(|(x, _)| *x == k) {
                    Some(i) => {
                        let old = std::mem::replace(&mut am[i].1, J::Null);
                        am[i].1 = merge_preserve(old, v);
                    }
                    None => am.push((k, v)),
                }
            }
            J::Obj(am)
        }
        (a, b) => {
            let mut out = match a {
                J::Arr(x) => x,
                other => vec![other],
            };
            match b {
                J::Arr(y) => out.extend(y),
                other => out.push(other),
            }
            J::Arr(out)
        }
    }
}

/// `JSON_MERGE_PRESERVE(doc, doc, ...)` (and `JSON_MERGE`, its older
/// name).
pub(crate) fn merge_preserve_fn(name: &str, nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 2 {
        return Err(SqlError::param_count(name));
    }
    let maria = env.maria();
    let fname = name.to_ascii_lowercase();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_merge_preserve",
        move |ctx, row| {
            let mut acc: Option<J> = None;
            for (i, n) in nodes.iter().enumerate() {
                let Some(d) = doc(ctx, n, row, maria, &fname, i + 1)? else {
                    return Ok(Value::Null);
                };
                acc = Some(match acc {
                    None => d,
                    Some(a) => merge_preserve(a, d),
                });
            }
            Ok(acc.map_or(Value::Null, |j| out_value(&j, maria)))
        },
    ))
}

/// Inserts `v` into the array the path's last leg indexes, at that place
/// (or at the end when the index is past it); nothing when the place
/// isn't in an array.
fn insert_at(j: &mut J, path: &[Leg], v: J) {
    let Some((leg, rest)) = path.split_first() else {
        return;
    };
    if rest.is_empty() {
        if let (Leg::Index(ix), J::Arr(a)) = (leg, j) {
            let at = match ix {
                Idx::N(n) => (*n).min(a.len()),
                Idx::Last(off) => a.len().saturating_sub(*off).saturating_sub(1),
            };
            a.insert(at, v);
        }
        return;
    }
    match (leg, j) {
        (Leg::Key(k), J::Obj(m)) => {
            if let Some((_, c)) = m.iter_mut().rev().find(|(x, _)| x == k) {
                insert_at(c, rest, v);
            }
        }
        (Leg::Index(ix), J::Arr(a)) => {
            let at = match ix {
                Idx::N(n) => Some(*n),
                Idx::Last(off) => a.len().checked_sub(1 + off),
            };
            if let Some(c) = at.and_then(|n| a.get_mut(n)) {
                insert_at(c, rest, v);
            }
        }
        _ => {}
    }
}

/// `JSON_ARRAY_INSERT(doc, path, value, ...)`: each path must end with an
/// array index and name no wildcard (3165 on MySQL; MariaDB 4044).
pub(crate) fn array_insert(nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    if nodes.len() < 3 || nodes.len() % 2 == 0 {
        return Err(SqlError::param_count("JSON_ARRAY_INSERT"));
    }
    let maria = env.maria();
    Ok(Node::new(
        doc_type(&nodes[0], env),
        "json_array_insert",
        move |ctx, row| {
            let Some(mut d) = doc(ctx, &nodes[0], row, maria, "json_array_insert", 1)? else {
                return Ok(Value::Null);
            };
            for (i, pv) in nodes[1..].chunks(2).enumerate() {
                let Some(p) = path(ctx, &pv[0], row, maria, "json_array_insert", 2 * i + 2)? else {
                    return Ok(Value::Null);
                };
                if wild(&p) || !matches!(p.last(), Some(Leg::Index(_))) {
                    if maria {
                        ctx.warn(
                            4044,
                            format!(
                                "Wildcards or range in JSON path not allowed in argument {} to \
                                 function 'json_array_insert'",
                                2 * i + 2
                            ),
                        );
                        return Ok(Value::Null);
                    }
                    return Err(SqlError::new(
                        3165,
                        "42000",
                        "A path expression is not a path to a cell in an array.",
                    ));
                }
                let v = as_json(ctx, &pv[1], row)?;
                insert_at(&mut d, &p, v);
            }
            Ok(out_value(&d, maria))
        },
    ))
}

/// Two JSON values are equal (numbers by value).
fn same(a: &J, b: &J) -> bool {
    contains(a, b) && contains(b, a)
}

/// `JSON_OVERLAPS(a, b)`: whether two arrays share an element, two
/// objects a key with the same value, an array holds a scalar, or two
/// scalars are equal.
pub(crate) fn overlaps(a: Node, b: Node, env: Env<'_>) -> Node {
    let maria = env.maria();
    Node::new(
        ExprType::int(1, false).nullable(),
        "json_overlaps",
        move |ctx, row| {
            let Some(x) = doc(ctx, &a, row, maria, "json_overlaps", 1)? else {
                return Ok(Value::Null);
            };
            let Some(y) = doc(ctx, &b, row, maria, "json_overlaps", 2)? else {
                return Ok(Value::Null);
            };
            let hit = match (&x, &y) {
                (J::Arr(p), J::Arr(q)) => p.iter().any(|e| q.iter().any(|f| same(e, f))),
                (J::Obj(p), J::Obj(q)) => p
                    .iter()
                    .any(|(k, v)| q.iter().any(|(k2, v2)| k == k2 && same(v, v2))),
                (J::Arr(p), other) | (other, J::Arr(p)) => p.iter().any(|e| same(e, other)),
                (p, q) => same(p, q),
            };
            Ok(Value::Int(i64::from(hit)))
        },
    )
}

/// `x MEMBER OF (doc)`: whether `x` is an element of the array (or is
/// the scalar) `doc`.
pub(crate) fn member_of(x: Node, d: Node, env: Env<'_>) -> Node {
    let maria = env.maria();
    Node::new(
        ExprType::int(1, false).nullable(),
        "member of",
        move |ctx, row| {
            let Some(j) = doc(ctx, &d, row, maria, "member of", 2)? else {
                return Ok(Value::Null);
            };
            let v = as_json(ctx, &x, row)?;
            if v == J::Null && !x.text.starts_with("json_") {
                return Ok(Value::Null);
            }
            let hit = match &j {
                J::Arr(a) => a.iter().any(|e| contains(e, &v) && contains(&v, e)),
                other => contains(other, &v) && contains(&v, other),
            };
            Ok(Value::Int(i64::from(hit)))
        },
    )
}
