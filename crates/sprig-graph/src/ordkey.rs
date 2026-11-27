//! Ordered property keys: the byte form of a property value that range
//! indexes store, so that byte order is Cypher's `ORDER BY` order and byte
//! equality is the equality today's index keys have.
//!
//! A key is a class byte, then a payload that orders like the values:
//!
//! | Class | Values | Payload |
//! |---|---|---|
//! | 3 | byte arrays | the bytes, escaped |
//! | 6 | zoned datetimes | epoch seconds, nanoseconds, then the stored text |
//! | 7 | local datetimes | epoch seconds (as UTC), nanoseconds |
//! | 8 | dates | days since 1970-01-01 |
//! | 9 | zoned times | seconds from midnight at UTC, nanoseconds, then the stored text |
//! | 10 | local times | nanoseconds from midnight |
//! | 11 | durations | the stored text (equality only) |
//! | 12 | strings | the UTF-8 bytes, escaped |
//! | 13 | booleans | `00` or `01` |
//! | 14 | numbers | the value as an ordered double, then the exact integer residual |
//!
//! The class bytes follow [`crate::value::Value`]'s global order (points,
//! lists and maps aren't indexed, as before). Numbers: an integer `i` is
//! `(i as f64, i - (i as f64))`, a float `f` is `(f, 0)`, so `1` and `1.0`
//! are one key, `2^53 + 1` and `2^53` two keys in the right order, and an
//! integer and a float compare exactly. `-0.0` is `0.0`; NaN sorts after
//! every number. Zoned values order by instant and keep their text, so two
//! texts of one instant are two keys (equality stays as exact as the typed
//! texts of the equality-only keys). Strings longer than [`LONG`] bytes
//! keep their first [`LONG`] bytes, then a marker and their SHA-256: equal
//! keys mean equal strings, but such strings order only by their prefix
//! ([`tie_len`] says how far a key orders exactly).
//!
//! Every component ends where its encoding says, so keys of several
//! properties (composite indexes) are the components one after another.

use serde_json::Value as Json;
use sprig_engine::keys::memcmp;

use crate::value::Value;

/// Byte arrays.
pub(crate) const C_BYTES: u8 = 3;
/// Zoned datetimes.
pub(crate) const C_DATETIME: u8 = 6;
/// Local datetimes.
pub(crate) const C_LOCALDATETIME: u8 = 7;
/// Dates.
pub(crate) const C_DATE: u8 = 8;
/// Zoned times.
pub(crate) const C_TIME: u8 = 9;
/// Local times.
pub(crate) const C_LOCALTIME: u8 = 10;
/// Durations.
pub(crate) const C_DURATION: u8 = 11;
/// Strings.
pub(crate) const C_STRING: u8 = 12;
/// Booleans.
pub(crate) const C_BOOL: u8 = 13;
/// Numbers.
pub(crate) const C_NUMBER: u8 = 14;

/// String bytes kept in a key before the hash.
pub const LONG: usize = 1024;

/// The marker between a long string's prefix and its hash: it sorts after
/// the terminator (`00 00`) and before an escaped zero (`00 FF`).
const LONG_MARK: [u8; 2] = [0, 1];

/// Appends the escaped bytes of `b` without a terminator.
fn put_escaped(out: &mut Vec<u8>, b: &[u8]) {
    for &c in b {
        out.push(c);
        if c == 0 {
            out.push(0xFF);
        }
    }
}

fn put_string(out: &mut Vec<u8>, s: &str) {
    out.push(C_STRING);
    if s.len() <= LONG {
        memcmp::put_bytes(out, s.as_bytes());
    } else {
        put_escaped(out, &s.as_bytes()[..LONG]);
        out.extend_from_slice(&LONG_MARK);
        let d = ring::digest::digest(&ring::digest::SHA256, s.as_bytes());
        out.extend_from_slice(d.as_ref());
    }
}

/// The ordered double and the integer residual of a number.
fn put_number(out: &mut Vec<u8>, v: &Value) {
    out.push(C_NUMBER);
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    let (f, residual) = match v {
        Value::Int(i) => {
            let f = *i as f64;
            // `f` is integral and within 2^63 in size: exact as i128.
            let r = i128::from(*i) - f as i128;
            (f, i64::try_from(r).unwrap_or(0))
        }
        Value::Float(f) if f.is_nan() => (f64::NAN, 0),
        Value::Float(f) => (*f, 0),
        _ => (0.0, 0),
    };
    let f = if f.is_nan() {
        f64::from_bits(0x7FF8_0000_0000_0000)
    } else {
        f
    };
    memcmp::put_f64(out, f);
    memcmp::put_i64(out, residual);
}

/// The key of one value; `None` when the value isn't indexed (null,
/// lists, maps, points, graph elements).
#[must_use]
pub fn value_key(v: &Value) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    put_value(&mut out, v).then_some(out)
}

/// Appends the key of `v`; `false` when it isn't indexed.
pub(crate) fn put_value(out: &mut Vec<u8>, v: &Value) -> bool {
    use chrono::Timelike;
    match v {
        Value::Int(_) | Value::Float(_) => put_number(out, v),
        Value::String(s) => put_string(out, s),
        Value::Bool(b) => {
            out.push(C_BOOL);
            out.push(u8::from(*b));
        }
        Value::Bytes(b) => {
            out.push(C_BYTES);
            memcmp::put_bytes(out, b);
        }
        Value::Date(d) => {
            out.push(C_DATE);
            let days =
                (*d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default()).num_days();
            memcmp::put_i64(out, days);
        }
        Value::LocalTime(t) => {
            out.push(C_LOCALTIME);
            let n = i64::from(t.num_seconds_from_midnight()) * 1_000_000_000
                + i64::from(t.nanosecond());
            memcmp::put_i64(out, n);
        }
        Value::Time(t, off) => {
            out.push(C_TIME);
            memcmp::put_i64(
                out,
                i64::from(t.num_seconds_from_midnight()) - i64::from(*off),
            );
            memcmp::put_i64(out, i64::from(t.nanosecond()));
            memcmp::put_bytes(out, crate::temporal::text(v).unwrap_or_default().as_bytes());
        }
        Value::LocalDateTime(dt) => {
            out.push(C_LOCALDATETIME);
            let u = dt.and_utc();
            memcmp::put_i64(out, u.timestamp());
            memcmp::put_i64(out, i64::from(u.timestamp_subsec_nanos()));
        }
        Value::DateTime(z) => {
            out.push(C_DATETIME);
            let (s, n) = z.epoch();
            memcmp::put_i64(out, s);
            memcmp::put_i64(out, i64::from(n));
            memcmp::put_bytes(out, crate::temporal::text(v).unwrap_or_default().as_bytes());
        }
        Value::Duration(_) => {
            out.push(C_DURATION);
            memcmp::put_bytes(out, crate::temporal::text(v).unwrap_or_default().as_bytes());
        }
        _ => return false,
    }
    true
}

/// The value of property `p` of a stored element (`props` and `_types`),
/// as the executor reads it.
#[must_use]
pub fn stored_value(props: &Json, types: Option<&Json>, p: &str) -> Option<Value> {
    let j = props.get(p)?;
    if j.is_null() {
        return None;
    }
    let tag = types.and_then(|t| t.get(p)).and_then(Json::as_str);
    match (tag, j) {
        (Some("float"), Json::Number(n)) => n.as_f64().map(Value::Float),
        (Some(kind), _) => crate::temporal::from_stored(kind, j),
        _ => Some(crate::value::from_json(j)),
    }
}

/// The key of a stored element for an index on `props_of` (in order);
/// `None` when one of them is missing or not indexed.
#[must_use]
pub fn element_key(props: &Json, types: Option<&Json>, props_of: &[String]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for p in props_of {
        let v = stored_value(props, types, p)?;
        if !put_value(&mut out, &v) {
            return None;
        }
    }
    Some(out)
}

/// How many leading bytes of a one-property key order exactly: the whole
/// key, except a long string's hash.
#[must_use]
pub fn tie_len(key: &[u8]) -> usize {
    if key.first() == Some(&C_STRING) {
        let mut i = 1;
        while i + 1 < key.len() {
            if key[i] == 0 {
                match key[i + 1] {
                    0xFF => {
                        i += 2;
                        continue;
                    }
                    1 => return i,
                    _ => return key.len(),
                }
            }
            i += 1;
        }
    }
    key.len()
}

/// One bound of a range over one property's keys.
#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    /// The value.
    pub value: Value,
    /// Whether the value itself is in the range.
    pub inclusive: bool,
}

/// A condition on one property that a range index can serve: `lo` and
/// `hi` bounds (`<`, `<=`, `>`, `>=`), or a string prefix (`STARTS WITH`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RangePred {
    /// Lower bound.
    pub lo: Option<Bound>,
    /// Upper bound.
    pub hi: Option<Bound>,
    /// `STARTS WITH` this string.
    pub prefix: Option<String>,
}

impl RangePred {
    /// Whether `v` passes, exactly as the executor's `<`, `<=`, `>`, `>=`
    /// and `STARTS WITH` say.
    #[must_use]
    pub fn passes(&self, v: &Value) -> bool {
        use std::cmp::Ordering as O;
        if let Some(b) = &self.lo {
            match v.cypher_cmp(&b.value) {
                Some(O::Greater) => {}
                Some(O::Equal) if b.inclusive => {}
                _ => return false,
            }
        }
        if let Some(b) = &self.hi {
            match v.cypher_cmp(&b.value) {
                Some(O::Less) => {}
                Some(O::Equal) if b.inclusive => {}
                _ => return false,
            }
        }
        if let Some(p) = &self.prefix {
            match v {
                Value::String(s) if s.starts_with(p.as_str()) => {}
                _ => return false,
            }
        }
        true
    }

    /// Adds another condition on the same property (both must hold).
    /// `false` when the two can't be compared, so one of them was left out
    /// (the range is then only a superset of the conditions).
    pub fn merge(&mut self, o: RangePred) -> bool {
        use std::cmp::Ordering as O;
        let mut exact = true;
        let mut pick = |a: Option<Bound>, b: Bound, keep_greater: bool| -> Bound {
            let Some(a) = a else { return b };
            match b.value.cypher_cmp(&a.value) {
                Some(O::Equal) => {
                    if b.inclusive {
                        a
                    } else {
                        b
                    }
                }
                Some(o) if (o == O::Greater) == keep_greater => b,
                Some(_) => a,
                None => {
                    exact = false;
                    a
                }
            }
        };
        if let Some(b) = o.lo {
            self.lo = Some(pick(self.lo.take(), b, true));
        }
        if let Some(b) = o.hi {
            self.hi = Some(pick(self.hi.take(), b, false));
        }
        if let Some(p) = o.prefix {
            self.prefix = Some(match self.prefix.take() {
                Some(a) if a.starts_with(p.as_str()) => a,
                Some(a) if p.starts_with(a.as_str()) => p,
                Some(a) => {
                    exact = false;
                    a
                }
                None => p,
            });
        }
        exact
    }

    /// The class every value the predicate passes belongs to (Cypher
    /// compares only within a class; other values give `null`); `None`
    /// when the bounds disagree or aren't indexed.
    #[must_use]
    pub fn class(&self) -> Option<u8> {
        let mut c: Option<u8> = None;
        let mut take = |v: &Value| -> bool {
            let mut k = Vec::new();
            if !put_value(&mut k, v) {
                return false;
            }
            match c {
                None => {
                    c = Some(k[0]);
                    true
                }
                Some(x) => x == k[0],
            }
        };
        if let Some(b) = &self.lo
            && !take(&b.value)
        {
            return None;
        }
        if let Some(b) = &self.hi
            && !take(&b.value)
        {
            return None;
        }
        if let Some(p) = &self.prefix
            && !take(&Value::String(p.clone()))
        {
            return None;
        }
        // Durations order only by their text, not as Cypher orders them.
        c.filter(|c| *c != C_DURATION)
    }

    /// The byte range `[start, end)` of one-property keys that holds every
    /// value passing the predicate (a superset: bounds are widened to whole
    /// doubles, instants and hashed prefixes; callers recheck). `None` when
    /// the predicate has no class ([`RangePred::class`]).
    #[must_use]
    pub fn key_range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let class = self.class()?;
        let mut start = vec![class];
        let mut end = vec![class + 1];
        if let Some(b) = &self.lo {
            start = widened(&b.value, false)?;
        }
        if let Some(b) = &self.hi {
            end = widened(&b.value, true)?;
        }
        if let Some(p) = &self.prefix {
            let mut ps = vec![C_STRING];
            put_escaped(&mut ps, &p.as_bytes()[..p.len().min(LONG)]);
            let pe = crate::engine_keys::end_of(&ps);
            if ps > start {
                start = ps;
            }
            if pe < end {
                end = pe;
            }
        }
        (start < end).then_some((start, end))
    }
}

/// A bound widened to every key whose leading, exactly ordered part equals
/// `v`'s: the start of that part (`after` false) or the end after it.
fn widened(v: &Value, after: bool) -> Option<Vec<u8>> {
    let k = value_key(v)?;
    let keep = match k[0] {
        // The double alone: integers near it differ only in the residual.
        C_NUMBER => 9,
        // The instant alone (texts of one instant follow it).
        C_DATETIME | C_TIME => 17,
        _ => tie_len(&k),
    };
    let head = &k[..keep];
    if after {
        Some(crate::engine_keys::end_of(head))
    } else {
        Some(head.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(v: &Value) -> Vec<u8> {
        value_key(v).unwrap()
    }

    #[test]
    fn numbers_order_and_equal_exactly() {
        let big = 9_007_199_254_740_993_i64; // 2^53 + 1
        assert_eq!(key(&Value::Int(1)), key(&Value::Float(1.0)));
        assert_eq!(key(&Value::Float(-0.0)), key(&Value::Int(0)));
        assert_ne!(
            key(&Value::Int(big)),
            key(&Value::Float(9_007_199_254_740_992.0))
        );
        assert!(key(&Value::Int(big)) > key(&Value::Float(9_007_199_254_740_992.0)));
        assert!(key(&Value::Int(big)) < key(&Value::Int(big + 1)));
        let mut vals = vec![
            Value::Float(f64::NEG_INFINITY),
            Value::Int(i64::MIN),
            Value::Float(-1.5),
            Value::Int(-1),
            Value::Int(0),
            Value::Float(0.5),
            Value::Int(1),
            Value::Float(1.5),
            Value::Int(i64::MAX - 1),
            Value::Int(i64::MAX),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NAN),
        ];
        let sorted = vals.clone();
        vals.reverse();
        vals.sort_by_key(key);
        assert_eq!(format!("{vals:?}"), format!("{sorted:?}"));
    }

    #[test]
    fn classes_follow_the_global_order() {
        let mut vals = vec![
            Value::Int(3),
            Value::Bool(true),
            Value::String("a".into()),
            Value::Date(chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()),
            Value::Bytes(vec![1]),
            Value::Bool(false),
            Value::String(String::new()),
        ];
        let mut by_order = vals.clone();
        by_order.sort_by(Value::order);
        vals.sort_by_key(key);
        assert_eq!(format!("{vals:?}"), format!("{by_order:?}"));
    }

    #[test]
    fn strings_order_by_bytes_and_prefixes_cover() {
        let ss = [
            "",
            "a",
            "a\u{0}",
            "a\u{0}b",
            "ab",
            "b",
            "\u{e9}",
            "\u{1F600}",
        ];
        for w in ss.windows(2) {
            assert!(key(&Value::String(w[0].into())) < key(&Value::String(w[1].into())));
        }
        let p = RangePred {
            prefix: Some("a".into()),
            ..RangePred::default()
        };
        let (s, e) = p.key_range().unwrap();
        for x in ["a", "a\u{0}", "a\u{0}b", "ab"] {
            let k = key(&Value::String(x.into()));
            assert!(k >= s && k < e, "{x}");
        }
        for x in ["", "b", "\u{e9}"] {
            let k = key(&Value::String(x.into()));
            assert!(!(k >= s && k < e), "{x}");
        }
        let long = "x".repeat(3000);
        let long2 = format!("{}y", "x".repeat(2999));
        assert_ne!(
            key(&Value::String(long.clone())),
            key(&Value::String(long2))
        );
        assert_eq!(tie_len(&key(&Value::String(long))), 1 + LONG);
    }

    #[test]
    fn ranges_hold_their_values() {
        let r = RangePred {
            lo: Some(Bound {
                value: Value::Int(5),
                inclusive: false,
            }),
            hi: Some(Bound {
                value: Value::Float(10.5),
                inclusive: true,
            }),
            prefix: None,
        };
        let (s, e) = r.key_range().unwrap();
        for v in [
            Value::Int(5),
            Value::Int(6),
            Value::Float(10.5),
            Value::Float(7.25),
        ] {
            let k = key(&v);
            assert!(k >= s && k < e, "{v:?}");
        }
        for v in [Value::Int(4), Value::Int(11), Value::String("7".into())] {
            let k = key(&v);
            assert!(!(k >= s && k < e), "{v:?}");
        }
        let bad = RangePred {
            lo: Some(Bound {
                value: Value::Int(1),
                inclusive: true,
            }),
            hi: Some(Bound {
                value: Value::String("z".into()),
                inclusive: true,
            }),
            prefix: None,
        };
        assert!(bad.key_range().is_none());
    }
}
