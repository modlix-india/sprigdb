//! A Redis database's table under the planner's storage
//! ([`crate::engine::redistable`]): its keys (and their elements) read at
//! the statement's read view, in the same engine transaction as the
//! tables.
//!
//! ```text
//! a point on k (every range)   that key's metadata entry, then its
//!                              elements (a big collection's own entries)
//! any other range              every key's metadata entry, in key order,
//!                              the rows in range kept
//! ```
//!
//! Keys and hash fields whose expiry has passed are left out; a key's
//! elements come in primary-key order (by field, position or member).

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound as RangeBound;
use std::sync::Arc;

use bytes::Bytes;
use sprig_exec::SqlError;
use sprig_exec::ops::Row;
use sprig_plan::access::{Counters, KeyRange, RowCursor};
use sprig_plan::catalog::KeyId;
use sprig_redis_engine::layout::{
    self as rl, Body, HashBody, KIND_META, ListBody, Meta, SetBody, Space, ZBody,
};

use super::{Bound, COUNT_CAP, Inner, LARGE_TABLE, SCAN_BATCH, internal};
use crate::engine::redistable::{RedisTable, Rel};
use crate::engine::row::Datum;

/// Elements a key is taken to hold, for estimates.
const ELEMENTS_PER_KEY: u64 = 4;

/// A locking read of a Redis table: refused (Redis keys aren't row-locked).
pub(super) fn locked() -> SqlError {
    SqlError::new(
        1235,
        "42000",
        "This version of SprigDB doesn't yet support 'a locking read of a Redis database'",
    )
}

fn corrupt() -> SqlError {
    internal("a Redis key's stored form isn't readable")
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The metadata entries of the database: `[start, end)` (up to the scan
/// index, [`rl::SCAN_INDEX`]).
fn meta_range(sp: &Space) -> (Vec<u8>, Vec<u8>) {
    let lo = sp.prefix(KIND_META);
    let (hi, _) = sp.scan_index_range();
    (lo, hi)
}

/// Up to `want` entries of `[lo, hi)` from `lo`, at the read view.
fn batch(
    inner: &Inner,
    lo: &[u8],
    hi: &[u8],
    want: usize,
) -> Result<Vec<(Bytes, Bytes)>, SqlError> {
    Ok(inner
        .with(|tx| tx.scan(lo, hi, false, want))?
        .unwrap_or_default())
}

/// Every entry of `[lo, hi)`, at the read view.
fn all_entries(inner: &Inner, lo: &[u8], hi: &[u8]) -> Result<Vec<(Bytes, Bytes)>, SqlError> {
    let mut out = Vec::new();
    let mut from = lo.to_vec();
    loop {
        inner.check_time()?;
        let got = batch(inner, &from, hi, SCAN_BATCH * 4)?;
        let full = got.len() == SCAN_BATCH * 4;
        if let Some((k, _)) = got.last() {
            from = rl::after(k);
        }
        out.extend(got);
        if !full {
            return Ok(out);
        }
    }
}

/// Every live key of the database with its metadata, in key order.
fn all_keys(inner: &Inner, sp: &Space) -> Result<Vec<(Vec<u8>, Meta)>, SqlError> {
    let (lo, hi) = meta_range(sp);
    let now = now_ms();
    let mut out = Vec::new();
    for (k, v) in all_entries(inner, &lo, &hi)? {
        let key = sp.meta_user_key(&k).ok_or_else(corrupt)?;
        let m = Meta::decode(&v).ok_or_else(corrupt)?;
        if !m.expired(now) {
            out.push((key, m));
        }
    }
    Ok(out)
}

/// Key `key` with its metadata, when it exists and is live.
fn one_key(inner: &Inner, sp: &Space, key: &[u8]) -> Result<Option<(Vec<u8>, Meta)>, SqlError> {
    let Some(Some(v)) = inner.with(|tx| tx.get(&sp.meta(key)))? else {
        return Ok(None);
    };
    let m = Meta::decode(&v).ok_or_else(corrupt)?;
    if m.expired(now_ms()) {
        return Ok(None);
    }
    Ok(Some((key.to_vec(), m)))
}

/// A split collection's element entries.
fn elements(inner: &Inner, sp: &Space, uid: u64) -> Result<Vec<(Bytes, Bytes)>, SqlError> {
    let (lo, hi) = sp.elems_range(uid);
    all_entries(inner, &lo, &hi)
}

fn bin(b: &[u8]) -> Datum {
    Datum::Bytes(Bytes::copy_from_slice(b))
}

/// The type's name, as `TYPE` answers.
fn type_name(m: &Meta) -> Datum {
    Datum::Str(Bytes::from_static(m.kind().name().as_bytes()))
}

/// A string's value (a chunked one put together; a missing chunk reads as
/// zeros).
fn string_value(inner: &Inner, sp: &Space, m: &Meta) -> Result<Option<Bytes>, SqlError> {
    match &m.body {
        Body::Str(s) => Ok(Some(s.clone())),
        Body::Chunked { uid, len } => {
            let len = usize::try_from(*len).map_err(|_| corrupt())?;
            let mut out = vec![0u8; len];
            for (k, v) in elements(inner, sp, *uid)? {
                let i = Space::parse_chunk(&k).ok_or_else(corrupt)?;
                let at = usize::try_from(i).map_err(|_| corrupt())? * rl::CHUNK;
                if at >= len {
                    continue;
                }
                let n = v.len().min(len - at);
                out[at..at + n].copy_from_slice(&v[..n]);
            }
            Ok(Some(Bytes::from(out)))
        }
        _ => Ok(None),
    }
}

/// `ms` since the epoch as a UTC `DATETIME(3)`.
fn datetime(ms: i64) -> Datum {
    crate::engine::queuetable::chrono_free(ms).map_or(Datum::Null, Datum::DateTime)
}

/// The rows key `key` gives table `rel`, in primary-key order.
#[allow(clippy::too_many_lines)] // one arm per type and table
fn rows_of(
    inner: &Inner,
    sp: &Space,
    rel: Rel,
    key: &[u8],
    m: &Meta,
    now: i64,
) -> Result<Vec<Row>, SqlError> {
    let k = bin(key);
    let mut out: Vec<Row> = Vec::new();
    match (rel, &m.body) {
        (Rel::Keys, _) => {
            out.push(vec![
                k,
                type_name(m),
                string_value(inner, sp, m)?.map_or(Datum::Null, Datum::Bytes),
                m.exp.map_or(Datum::Null, datetime),
            ]);
        }
        (Rel::Hash, Body::Hash(HashBody::Inline(fields))) => {
            let mut f: Vec<&(Bytes, Bytes)> = fields.iter().collect();
            f.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, v) in f {
                out.push(vec![k.clone(), bin(name), Datum::Bytes(v.clone())]);
            }
        }
        (Rel::Hash, Body::Hash(HashBody::Split { uid, .. })) => {
            let mut f = Vec::new();
            for (ek, ev) in elements(inner, sp, *uid)? {
                let (_, name) = Space::parse_field(&ek).ok_or_else(corrupt)?;
                let (exp, v) = rl::field_parts(&ev).ok_or_else(corrupt)?;
                if exp.is_some_and(|e| e <= now) {
                    continue;
                }
                f.push((name, v));
            }
            f.sort_by(|a, b| a.0.cmp(&b.0));
            for (name, v) in f {
                out.push(vec![k.clone(), bin(&name), Datum::Bytes(v)]);
            }
        }
        (Rel::List, Body::List(ListBody::Inline(items))) => {
            for (i, v) in items.iter().enumerate() {
                out.push(vec![
                    k.clone(),
                    Datum::UInt(i as u64),
                    Datum::Bytes(v.clone()),
                ]);
            }
        }
        (Rel::List, Body::List(ListBody::Split { uid, head, .. })) => {
            for (ek, ev) in elements(inner, sp, *uid)? {
                let pos = Space::parse_pos(&ek).ok_or_else(corrupt)?;
                let idx = u64::try_from(pos - head).map_err(|_| corrupt())?;
                out.push(vec![k.clone(), Datum::UInt(idx), Datum::Bytes(ev)]);
            }
        }
        (Rel::Set, Body::Set(SetBody::Inline { members, .. })) => {
            let mut ms: Vec<&Bytes> = members.iter().collect();
            ms.sort();
            for x in ms {
                out.push(vec![k.clone(), Datum::Bytes(x.clone())]);
            }
        }
        (Rel::Set, Body::Set(SetBody::Split { uid, .. })) => {
            let mut ms = Vec::new();
            for (ek, _) in elements(inner, sp, *uid)? {
                ms.push(Space::parse_field(&ek).ok_or_else(corrupt)?.1);
            }
            ms.sort();
            for x in ms {
                out.push(vec![k.clone(), bin(&x)]);
            }
        }
        (Rel::ZSet, Body::ZSet(ZBody::Inline(members))) => {
            let mut ms: Vec<(Bytes, f64)> = members.iter().map(|(s, x)| (x.clone(), *s)).collect();
            ms.sort_by(|a, b| a.0.cmp(&b.0));
            for (x, s) in ms {
                out.push(zrow(&k, &x, s));
            }
        }
        (Rel::ZSet, Body::ZSet(ZBody::Split { uid, .. })) => {
            let mut ms = Vec::new();
            for (ek, ev) in elements(inner, sp, *uid)? {
                let (_, x) = Space::parse_field(&ek).ok_or_else(corrupt)?;
                let bits: [u8; 8] = ev
                    .get(..8)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(corrupt)?;
                ms.push((x, f64::from_bits(u64::from_be_bytes(bits))));
            }
            ms.sort_by(|a, b| a.0.cmp(&b.0));
            for (x, s) in ms {
                out.push(zrow(&k, &x, s));
            }
        }
        // A key of another type has no rows in this table.
        _ => {}
    }
    Ok(out)
}

/// A sorted set's row: an infinite score is `NULL` with its class.
fn zrow(k: &Datum, member: &[u8], score: f64) -> Row {
    let (s, class) = if !score.is_infinite() {
        (Datum::Double(score), 0)
    } else if score.is_sign_positive() {
        (Datum::Null, 1)
    } else {
        (Datum::Null, -1)
    };
    vec![k.clone(), bin(member), s, Datum::Int(class)]
}

/// The key bytes of a bound value of `k` (or of a member or field).
fn bytes_of(v: &Datum) -> Option<&[u8]> {
    match v {
        Datum::Bytes(b) | Datum::Str(b) => Some(b),
        _ => None,
    }
}

/// How a row's key part compares with a bound's.
fn cmp_part(row: &Datum, bound: &Datum) -> Result<Ordering, SqlError> {
    match (row, bound) {
        (Datum::UInt(a), Datum::UInt(b)) => Ok(a.cmp(b)),
        (Datum::UInt(a), Datum::Int(b)) => Ok(i128::from(*a).cmp(&i128::from(*b))),
        (a, b) => match (bytes_of(a), bytes_of(b)) {
            (Some(x), Some(y)) => Ok(x.cmp(y)),
            _ => Err(internal("a key value of the wrong type")),
        },
    }
}

/// How the row's first key parts compare with bound `b` (as many parts as
/// it has).
fn cmp_prefix(row: &[Datum], b: &[Datum]) -> Result<Ordering, SqlError> {
    for (r, x) in row.iter().zip(b) {
        let o = cmp_part(r, x)?;
        if o != Ordering::Equal {
            return Ok(o);
        }
    }
    Ok(Ordering::Equal)
}

/// Whether a row's key (its first `parts` values) lies in `r`.
fn in_range(row: &[Datum], parts: usize, r: &KeyRange) -> Result<bool, SqlError> {
    let key = &row[..parts.min(row.len())];
    let lo = match &r.start {
        RangeBound::Unbounded => true,
        RangeBound::Included(b) => cmp_prefix(key, b)?.is_ge(),
        RangeBound::Excluded(b) => cmp_prefix(key, b)?.is_gt(),
    };
    let hi = match &r.end {
        RangeBound::Unbounded => true,
        RangeBound::Included(b) => cmp_prefix(key, b)?.is_le(),
        RangeBound::Excluded(b) => cmp_prefix(key, b)?.is_lt(),
    };
    Ok(lo && hi)
}

/// The one key every range names, when each is a point on `k`.
fn point_keys(ranges: &[KeyRange]) -> Option<Vec<Vec<u8>>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for r in ranges {
        let (RangeBound::Included(a), RangeBound::Included(b)) = (&r.start, &r.end) else {
            return None;
        };
        let x = bytes_of(a.first()?)?;
        let y = bytes_of(b.first()?)?;
        if x != y {
            return None;
        }
        if !out.iter().any(|k| k == x) {
            out.push(x.to_vec());
        }
    }
    out.sort();
    Some(out)
}

/// The rows of `t` the keys `keys` (all of them: `None`) give.
fn rows(inner: &Inner, t: &RedisTable, keys: Option<&[Vec<u8>]>) -> Result<Vec<Row>, SqlError> {
    let now = now_ms();
    let metas = match keys {
        None => all_keys(inner, &t.space)?,
        Some(ks) => {
            let mut v = Vec::new();
            for k in ks {
                if let Some(x) = one_key(inner, &t.space, k)? {
                    v.push(x);
                }
            }
            v
        }
    };
    let mut out = Vec::new();
    for (k, m) in &metas {
        inner.check_time()?;
        out.extend(rows_of(inner, &t.space, t.rel, k, m, now)?);
    }
    Ok(out)
}

/// The keys the database holds, counted up to the cap.
fn key_count(inner: &Inner, t: &RedisTable) -> u64 {
    let (lo, hi) = meta_range(&t.space);
    let n = inner.count(&lo, &hi, COUNT_CAP);
    if n >= COUNT_CAP {
        LARGE_TABLE
    } else {
        n as u64
    }
}

pub(super) fn table_rows(inner: &Inner, t: &RedisTable) -> u64 {
    let keys = key_count(inner, t);
    match t.rel {
        Rel::Keys => keys,
        _ => keys.saturating_mul(ELEMENTS_PER_KEY),
    }
}

pub(super) fn rows_in_range(inner: &Inner, t: &RedisTable, key: KeyId, range: &KeyRange) -> u64 {
    if key != KeyId::Primary {
        return 0;
    }
    match t.rel {
        Rel::Keys if range.is_point(1) => 1,
        _ if range.is_point(2) => 1,
        _ if range.is_point(1) => ELEMENTS_PER_KEY,
        _ => table_rows(inner, t),
    }
}

#[allow(clippy::cast_precision_loss)] // a small constant
pub(super) fn rows_per_key(t: &RedisTable, key: KeyId, parts: usize) -> Option<f64> {
    if key != KeyId::Primary {
        return None;
    }
    match (t.rel, parts) {
        (Rel::Keys, 1) | (_, 2) => Some(1.0),
        (_, 1) => Some(ELEMENTS_PER_KEY as f64),
        _ => None,
    }
}

pub(super) fn get(
    inner: &Inner,
    b: &Bound,
    t: &RedisTable,
    key: &[Datum],
    counters: &Counters,
) -> Result<Option<Row>, SqlError> {
    if b.lock.is_some() {
        return Err(locked());
    }
    let parts = b.def.pk.len();
    if key.len() != parts {
        return Err(internal("a point lookup needs every primary-key part"));
    }
    inner.check_time()?;
    counters.add_seek();
    let k = bytes_of(&key[0]).ok_or_else(|| internal("a key value of the wrong type"))?;
    let found = rows(inner, t, Some(&[k.to_vec()]))?
        .into_iter()
        .find(|r| cmp_prefix(&r[..parts], key).is_ok_and(Ordering::is_eq));
    if found.is_some() {
        counters.add_row();
    }
    Ok(found)
}

pub(super) fn scan(
    inner: &Inner,
    b: &Bound,
    t: &RedisTable,
    key: KeyId,
    ranges: &[KeyRange],
    reverse: bool,
    counters: &Arc<Counters>,
) -> Result<Box<dyn RowCursor>, SqlError> {
    if b.lock.is_some() {
        return Err(locked());
    }
    if key != KeyId::Primary {
        return Err(internal("a Redis table has no secondary index"));
    }
    inner.check_time()?;
    counters.add_seek();
    let parts = b.def.pk.len();
    let keys = point_keys(ranges);
    let all = rows(inner, t, keys.as_deref())?;
    let mut out = VecDeque::new();
    for r in ranges {
        let mut got = Vec::new();
        for row in &all {
            if in_range(row, parts, r)? {
                got.push(row.clone());
            }
        }
        if reverse {
            got.reverse();
        }
        out.extend(got);
    }
    for _ in &out {
        counters.add_row();
    }
    Ok(Box::new(Rows(out)))
}

/// Rows already read.
struct Rows(VecDeque<Row>);

impl RowCursor for Rows {
    fn next(&mut self) -> Result<Option<Row>, SqlError> {
        Ok(self.0.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ranges_compare_leading_parts() {
        let row = vec![bin(b"user:1"), Datum::UInt(3), bin(b"x")];
        let r = KeyRange {
            start: RangeBound::Included(vec![bin(b"user:1")]),
            end: RangeBound::Included(vec![Datum::Str(Bytes::from_static(b"user:1"))]),
        };
        assert!(in_range(&row, 2, &r).unwrap());
        let r = KeyRange {
            start: RangeBound::Excluded(vec![bin(b"user:1"), Datum::Int(3)]),
            end: RangeBound::Unbounded,
        };
        assert!(!in_range(&row, 2, &r).unwrap());
        assert!(in_range(&row, 2, &KeyRange::all()).unwrap());
        assert_eq!(point_keys(std::slice::from_ref(&KeyRange::all())), None);
        let p = KeyRange {
            start: RangeBound::Included(vec![bin(b"b")]),
            end: RangeBound::Included(vec![bin(b"b")]),
        };
        let q = KeyRange {
            start: RangeBound::Included(vec![bin(b"a")]),
            end: RangeBound::Included(vec![bin(b"a")]),
        };
        assert_eq!(
            point_keys(&[p, q]),
            Some(vec![b"a".to_vec(), b"b".to_vec()])
        );
    }

    #[test]
    fn infinite_scores_are_null_with_a_class() {
        let k = bin(b"z");
        assert_eq!(zrow(&k, b"m", f64::INFINITY)[2], Datum::Null);
        assert_eq!(zrow(&k, b"m", f64::NEG_INFINITY)[3], Datum::Int(-1));
        assert_eq!(zrow(&k, b"m", 1.5)[2], Datum::Double(1.5));
    }
}
