//! Hashes: inline in the metadata while small (insertion order), else one
//! element entry per field, ordered by the field's CRC32.

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{ld2string_human, parse_f64, parse_i64};
use sprig_redis::{glob, reply};

use super::keys::{cursor, rand_u64, scan_opts};
use super::{Arg, Cx, Out, corrupt, int, tri, upper};
use crate::layout::{
    Body, HashBody, INLINE_MAX_BYTES, INLINE_MAX_ENTRIES, Meta, Space, field_parts, field_value,
    field_value_ttl,
};
use crate::store::{Batch, StoreError};

/// Runs a hash command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "hset" => hset(cx, a, false),
        "hmset" => hset(cx, a, true),
        "hsetnx" => hsetnx(cx, a),
        "hget" => hget(cx, a),
        "hmget" => hmget(cx, a),
        "hgetall" => hgetall(cx, a, true, true),
        "hkeys" => hgetall(cx, a, true, false),
        "hvals" => hgetall(cx, a, false, true),
        "hlen" => hlen(cx, a),
        "hexists" => hexists(cx, a),
        "hdel" => hdel(cx, a),
        "hincrby" => hincrby(cx, a),
        "hincrbyfloat" => hincrbyfloat(cx, a),
        "hstrlen" => hstrlen(cx, a),
        "hrandfield" => hrandfield(cx, a),
        "hscan" => hscan(cx, a),
        "hexpire" => hexpire(cx, a, 1000, false),
        "hpexpire" => hexpire(cx, a, 1, false),
        "hexpireat" => hexpire(cx, a, 1000, true),
        "hpexpireat" => hexpire(cx, a, 1, true),
        "httl" => httl(cx, a, false, false),
        "hpttl" => httl(cx, a, true, false),
        "hexpiretime" => httl(cx, a, false, true),
        "hpexpiretime" => httl(cx, a, true, true),
        "hpersist" => hpersist(cx, a),
        _ => return None,
    })
}

/// Whether a collection of `n` entries with these sizes fits inline.
pub(crate) fn fits_inline<'a>(n: usize, mut sizes: impl Iterator<Item = &'a Bytes>) -> bool {
    n <= INLINE_MAX_ENTRIES && sizes.all(|s| s.len() <= INLINE_MAX_BYTES)
}

/// A key's hash: its body and expiry, and the stored metadata.
pub(crate) struct Hash {
    pub body: Option<HashBody>,
    pub exp: Option<i64>,
    pub raw: Option<Meta>,
}

/// One field change.
pub(crate) enum Edit {
    /// Set a field (a lifetime it had goes).
    Set(Bytes, Bytes),
    /// Set a field, keeping its lifetime (`HINCRBY`).
    Keep(Bytes, Bytes),
    /// Remove a field.
    Del(Bytes),
}

/// Reads every entry of `[start, end)`, in pages.
pub(crate) fn scan_all<B: Batch>(
    b: &mut B,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(Bytes, Bytes)>, StoreError> {
    let mut out = Vec::new();
    let mut from = start.to_vec();
    loop {
        let got = b.scan(&from, end, 1024)?;
        let n = got.len();
        if let Some((last, _)) = got.last() {
            from = crate::layout::after(last);
        }
        out.extend(got);
        if n < 1024 {
            return Ok(out);
        }
    }
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a hash; `Err` answers `WRONGTYPE`.
    pub(crate) fn hash(&mut self, key: &[u8]) -> Result<Arg<Hash>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok(Hash {
                body: None,
                exp: None,
                raw,
            }),
            Some(Meta {
                body: Body::Hash(h),
                exp,
            }) => Ok(Hash {
                body: Some(h),
                exp,
                raw,
            }),
            Some(_) => Err(reply::wrongtype()),
        })
    }

    /// Field `f` of hash `h`, with its expiry: `None` when absent or its
    /// lifetime has passed.
    pub(crate) fn hash_entry(
        &mut self,
        h: &HashBody,
        f: &[u8],
    ) -> Result<Option<(Option<i64>, Bytes)>, StoreError> {
        match h {
            HashBody::Inline(fields) => Ok(fields
                .iter()
                .find(|(k, _)| k == f)
                .map(|(_, v)| (None, v.clone()))),
            HashBody::Split { uid, .. } => match self.b.get(&self.space.field(*uid, f))? {
                None => Ok(None),
                Some(v) => {
                    let (exp, v) = field_parts(&v).ok_or_else(|| corrupt("hash field", v.len()))?;
                    Ok((exp.is_none_or(|e| e > self.now)).then_some((exp, v)))
                }
            },
        }
    }

    /// Field `f` of hash `h`.
    pub(crate) fn hash_get(&mut self, h: &HashBody, f: &[u8]) -> Result<Option<Bytes>, StoreError> {
        Ok(self.hash_entry(h, f)?.map(|(_, v)| v))
    }

    /// Every live field of hash `h` with its value.
    pub(crate) fn hash_all(&mut self, h: &HashBody) -> Result<Vec<(Bytes, Bytes)>, StoreError> {
        match h {
            HashBody::Inline(fields) => Ok(fields.clone()),
            HashBody::Split { uid, .. } => {
                let (lo, hi) = self.space.elems_range(*uid);
                let now = self.now;
                let mut out = Vec::new();
                for (k, v) in scan_all(self.b, &lo, &hi)? {
                    let (_, f) =
                        Space::parse_field(&k).ok_or_else(|| corrupt("hash field", k.len()))?;
                    let (exp, v) = field_parts(&v).ok_or_else(|| corrupt("hash field", v.len()))?;
                    if exp.is_none_or(|e| e > now) {
                        out.push((Bytes::from(f), v));
                    }
                }
                Ok(out)
            }
        }
    }

    /// Removes the fields of split hash `uid` whose lifetime has passed:
    /// how many.
    fn purge_due(&mut self, uid: u64) -> Result<u64, StoreError> {
        let (lo, hi) = self.space.fexp_range(uid, Some(self.now));
        let due = scan_all(self.b, &lo, &hi)?;
        for (k, _) in &due {
            let (_, f) = Space::parse_fexp(k).ok_or_else(|| corrupt("field expiry", k.len()))?;
            self.b.delete(k.clone())?;
            self.b.delete(self.space.field(uid, &f))?;
        }
        Ok(due.len() as u64)
    }

    /// Moves `key`'s hash-due entry from `old` to `new` (the earliest field
    /// expiry before and after a change).
    fn move_hdue(
        &mut self,
        key: &[u8],
        old: Option<i64>,
        new: Option<i64>,
    ) -> Result<(), StoreError> {
        if old != new {
            if let Some(at) = old {
                self.b.delete(self.space.hdue(at, key))?;
            }
            if let Some(at) = new {
                self.b.put(self.space.hdue(at, key), Bytes::new())?;
            }
        }
        Ok(())
    }

    /// Applies `edits` in order to hash `h` at `key` (absent: a new hash):
    /// fields added and removed. The key goes when its last field does.
    /// Fields whose lifetime has passed go first.
    pub(crate) fn hash_write(
        &mut self,
        key: &[u8],
        h: &Hash,
        edits: Vec<Edit>,
    ) -> Result<(i64, i64), StoreError> {
        let (mut added, mut removed) = (0i64, 0i64);
        let body = match h.body.clone().unwrap_or(HashBody::Inline(Vec::new())) {
            HashBody::Inline(mut fields) => {
                for e in edits {
                    match e {
                        Edit::Set(f, v) | Edit::Keep(f, v) => {
                            if let Some(slot) = fields.iter_mut().find(|(k, _)| *k == f) {
                                slot.1 = v;
                            } else {
                                fields.push((f, v));
                                added += 1;
                            }
                        }
                        Edit::Del(f) => {
                            if let Some(i) = fields.iter().position(|(k, _)| *k == f) {
                                fields.remove(i);
                                removed += 1;
                            }
                        }
                    }
                }
                if fields.is_empty() {
                    None
                } else if fits_inline(fields.len(), fields.iter().flat_map(|(f, v)| [f, v])) {
                    Some(HashBody::Inline(fields))
                } else {
                    // Too big to stay inline: the fields become elements.
                    let uid = self.take_uid()?;
                    for (f, v) in &fields {
                        self.b.put(self.space.field(uid, f), field_value(v))?;
                    }
                    Some(HashBody::Split {
                        uid,
                        count: fields.len() as u64,
                        ttl: false,
                    })
                }
            }
            HashBody::Split {
                uid,
                mut count,
                ttl,
            } => {
                let first = if ttl {
                    self.fexp_first(self.space, uid)?
                } else {
                    None
                };
                if ttl {
                    count -= self.purge_due(uid)?;
                }
                for e in edits {
                    let (f, v, keep) = match e {
                        Edit::Set(f, v) => (f, Some(v), false),
                        Edit::Keep(f, v) => (f, Some(v), true),
                        Edit::Del(f) => (f, None, false),
                    };
                    let k = self.space.field(uid, &f);
                    let old = match self.b.get(&k)? {
                        None => None,
                        Some(o) => {
                            Some(field_parts(&o).ok_or_else(|| corrupt("hash field", o.len()))?)
                        }
                    };
                    let old_exp = old.as_ref().and_then(|(e, _)| *e);
                    match v {
                        Some(v) => {
                            if old.is_none() {
                                count += 1;
                                added += 1;
                            }
                            let value = match old_exp {
                                Some(at) if keep => field_value_ttl(&v, at),
                                Some(at) => {
                                    self.b.delete(self.space.fexp(uid, at, &f))?;
                                    field_value(&v)
                                }
                                None => field_value(&v),
                            };
                            self.b.put(k, value)?;
                        }
                        None if old.is_some() => {
                            if let Some(at) = old_exp {
                                self.b.delete(self.space.fexp(uid, at, &f))?;
                            }
                            self.b.delete(k)?;
                            count -= 1;
                            removed += 1;
                        }
                        None => {}
                    }
                }
                if ttl {
                    let next = if count > 0 {
                        self.fexp_first(self.space, uid)?
                    } else {
                        None
                    };
                    self.move_hdue(key, first, next)?;
                }
                (count > 0).then_some(HashBody::Split { uid, count, ttl })
            }
        };
        let new = body.map(|b| Meta {
            exp: h.exp,
            body: Body::Hash(b),
        });
        if new.is_some() || h.raw.is_some() {
            self.write(key, h.raw.as_ref(), new.as_ref())?;
        }
        Ok((added, removed))
    }
}

/// `HSET k f v [f v ...]` (the number of new fields), `HMSET` (`OK`).
fn hset<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], hmset: bool) -> Out {
    if !a.len().is_multiple_of(2) {
        return Ok(reply::arity(if hmset { "hmset" } else { "hset" }));
    }
    let h = tri!(cx.hash(&a[1])?);
    let edits = a[2..]
        .chunks(2)
        .map(|c| Edit::Set(c[0].clone(), c[1].clone()))
        .collect();
    let (added, _) = cx.hash_write(&a[1], &h, edits)?;
    Ok(if hmset {
        Frame::ok()
    } else {
        Frame::Integer(added)
    })
}

/// `HSETNX k f v`.
fn hsetnx<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    if let Some(body) = &h.body
        && cx.hash_get(body, &a[2])?.is_some()
    {
        return Ok(Frame::Integer(0));
    }
    cx.hash_write(&a[1], &h, vec![Edit::Set(a[2].clone(), a[3].clone())])?;
    Ok(Frame::Integer(1))
}

/// `HGET k f`.
fn hget<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    Ok(match h.body {
        None => Frame::null(),
        Some(b) => cx
            .hash_get(&b, &a[2])?
            .map_or_else(Frame::null, Frame::Bulk),
    })
}

/// `HMGET k f ...`.
fn hmget<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    let mut out = Vec::with_capacity(a.len() - 2);
    for f in &a[2..] {
        out.push(match &h.body {
            None => Frame::null(),
            Some(b) => cx.hash_get(b, f)?.map_or_else(Frame::null, Frame::Bulk),
        });
    }
    Ok(Frame::Array(out))
}

/// `HGETALL` (fields and values), `HKEYS`, `HVALS`.
fn hgetall<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], keys: bool, vals: bool) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    let all = match &h.body {
        None => Vec::new(),
        Some(b) => cx.hash_all(b)?,
    };
    Ok(if keys && vals {
        Frame::Map(
            all.into_iter()
                .map(|(f, v)| (Frame::Bulk(f), Frame::Bulk(v)))
                .collect(),
        )
    } else {
        Frame::Array(
            all.into_iter()
                .map(|(f, v)| Frame::Bulk(if keys { f } else { v }))
                .collect(),
        )
    })
}

/// The number of fields of `h`.
pub(crate) fn hash_len(h: &HashBody) -> i64 {
    match h {
        HashBody::Inline(f) => f.len() as i64,
        HashBody::Split { count, .. } => *count as i64,
    }
}

/// `HLEN k`.
fn hlen<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    let due = match &h.body {
        Some(HashBody::Split { uid, ttl: true, .. }) => cx.fexp_due(cx.space, *uid)?,
        _ => 0,
    };
    Ok(Frame::Integer(h.body.as_ref().map_or(0, hash_len) - due))
}

/// `HEXISTS k f`.
fn hexists<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    Ok(Frame::Integer(match &h.body {
        None => 0,
        Some(b) => i64::from(cx.hash_get(b, &a[2])?.is_some()),
    }))
}

/// `HSTRLEN k f`.
fn hstrlen<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    Ok(Frame::Integer(match &h.body {
        None => 0,
        Some(b) => cx.hash_get(b, &a[2])?.map_or(0, |v| v.len() as i64),
    }))
}

/// `HDEL k f ...`: the number removed.
fn hdel<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let h = tri!(cx.hash(&a[1])?);
    if h.body.is_none() {
        return Ok(Frame::Integer(0));
    }
    let edits = a[2..].iter().map(|f| Edit::Del(f.clone())).collect();
    let (_, removed) = cx.hash_write(&a[1], &h, edits)?;
    Ok(Frame::Integer(removed))
}

/// `HINCRBY k f n`.
fn hincrby<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let by = tri!(int(&a[3]));
    let h = tri!(cx.hash(&a[1])?);
    let cur = match &h.body {
        None => None,
        Some(b) => cx.hash_get(b, &a[2])?,
    };
    let cur = match cur {
        None => 0,
        Some(v) => match parse_i64(&v) {
            Some(n) => n,
            None => return Ok(Frame::err("hash value is not an integer")),
        },
    };
    let Some(n) = cur.checked_add(by) else {
        return Ok(Frame::err("increment or decrement would overflow"));
    };
    cx.hash_write(
        &a[1],
        &h,
        vec![Edit::Keep(a[2].clone(), Bytes::from(n.to_string()))],
    )?;
    Ok(Frame::Integer(n))
}

/// `HINCRBYFLOAT k f x`.
fn hincrbyfloat<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let Some(by) = parse_f64(&a[3]) else {
        return Ok(reply::not_float());
    };
    if by.is_nan() || by.is_infinite() {
        return Ok(Frame::err("value is NaN or Infinity"));
    }
    let h = tri!(cx.hash(&a[1])?);
    let cur = match &h.body {
        None => None,
        Some(b) => cx.hash_get(b, &a[2])?,
    };
    let cur = match cur {
        None => 0.0,
        Some(v) => match parse_f64(&v) {
            Some(n) => n,
            None => return Ok(Frame::err("hash value is not a float")),
        },
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return Ok(Frame::err("increment would produce NaN or Infinity"));
    }
    let text = Bytes::from(ld2string_human(n));
    cx.hash_write(&a[1], &h, vec![Edit::Keep(a[2].clone(), text.clone())])?;
    Ok(Frame::Bulk(text))
}

/// `count` random picks out of `n` (Redis's rules: positive distinct, at
/// most `n`; negative `|count|` picks with repeats), as indexes.
pub(crate) fn pick_indexes(n: usize, count: i64) -> Vec<usize> {
    if n == 0 || count == 0 {
        return Vec::new();
    }
    if count < 0 {
        return (0..count.unsigned_abs())
            .map(|_| (rand_u64() % n as u64) as usize)
            .collect();
    }
    let want = (count as u64).min(n as u64) as usize;
    let mut idx: Vec<usize> = (0..n).collect();
    // A partial Fisher-Yates shuffle.
    for i in 0..want {
        let j = i + (rand_u64() % (n - i) as u64) as usize;
        idx.swap(i, j);
    }
    idx.truncate(want);
    idx
}

/// Collections up to this many entries are read whole for random picks.
const RANDOM_READ_ALL: u64 = 4096;
/// Entries read around each random position of a bigger one.
const RANDOM_WINDOW: usize = 64;

/// Random element entries of split collection `uid`, which holds `n`, by
/// Redis's count rules (positive: distinct, negative: repeats allowed).
/// Each pick seeks to a random CRC32 position and chooses uniformly among
/// the next 64 entries (wrapping), so its cost doesn't grow with `n`.
/// `None` when the collection is better read whole: small, or most of it
/// asked for.
pub(crate) fn random_elems<B: Batch>(
    cx: &mut Cx<'_, B>,
    uid: u64,
    n: u64,
    count: i64,
) -> Result<Option<Vec<(Bytes, Bytes)>>, StoreError> {
    // A pick reads a 64-entry window: reading everything is cheaper once
    // the picks would read as much.
    if n <= RANDOM_READ_ALL || count.unsigned_abs().saturating_mul(RANDOM_WINDOW as u64) >= n {
        return Ok(None);
    }
    let want = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
    let mut out = Vec::with_capacity(want.min(4096));
    let mut seen = std::collections::HashSet::new();
    let (start, end) = cx.space.elems_range(uid);
    let mut attempts = 0usize;
    while out.len() < want {
        attempts += 1;
        if attempts > want.saturating_mul(20).saturating_add(100) {
            return Ok(None);
        }
        let at = cx.space.elems_from_crc(uid, (rand_u64() >> 32) as u32);
        let mut win = cx.b.scan(&at, &end, RANDOM_WINDOW)?;
        if win.len() < RANDOM_WINDOW {
            win.extend(cx.b.scan(&start, &at, RANDOM_WINDOW - win.len())?);
        }
        if win.is_empty() {
            return Ok(None);
        }
        let pick = win.swap_remove((rand_u64() % win.len() as u64) as usize);
        if count > 0 && !seen.insert(pick.0.clone()) {
            continue;
        }
        out.push(pick);
    }
    Ok(Some(out))
}

/// A `count` argument with Redis's range check for random picks.
pub(crate) fn rand_count(a: &[u8]) -> Arg<i64> {
    let n = int(a)?;
    if n == i64::MIN {
        return Err(Frame::err(
            "value is out of range, value must between -9223372036854775807 and 9223372036854775807",
        ));
    }
    // Picks with repeats are built in memory: a bounded number.
    if n < -MAX_REPEATED_PICKS {
        return Err(Frame::err(&format!(
            "count is out of range: at most {MAX_REPEATED_PICKS} random picks with repeats"
        )));
    }
    Ok(n)
}

/// The most random picks with repeats (a negative count) one command
/// returns.
pub(crate) const MAX_REPEATED_PICKS: i64 = 1_000_000;

/// A list of pairs: flat in RESP2, nested pairs in RESP3.
pub(crate) fn pairs(resp3: bool, items: Vec<(Frame, Frame)>) -> Frame {
    if resp3 {
        Frame::Array(
            items
                .into_iter()
                .map(|(a, b)| Frame::Array(vec![a, b]))
                .collect(),
        )
    } else {
        Frame::Array(items.into_iter().flat_map(|(a, b)| [a, b]).collect())
    }
}

/// `HRANDFIELD k [count [WITHVALUES]]`.
fn hrandfield<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (count, withvalues) = match a.len() {
        2 => (None, false),
        3 => (Some(tri!(rand_count(&a[2]))), false),
        4 if upper(&a[3]) == "WITHVALUES" => (Some(tri!(rand_count(&a[2]))), true),
        _ => return Ok(reply::syntax()),
    };
    let h = tri!(cx.hash(&a[1])?);
    if let Some(HashBody::Split {
        uid,
        count: n,
        ttl: false,
    }) = h.body
        && let Some(got) = random_elems(cx, uid, n, count.unwrap_or(1))?
    {
        let mut picked = Vec::with_capacity(got.len());
        for (k, v) in got {
            let (_, f) = Space::parse_field(&k).ok_or_else(|| corrupt("hash field", k.len()))?;
            let (_, v) = field_parts(&v).ok_or_else(|| corrupt("hash field", v.len()))?;
            picked.push((Bytes::from(f), v));
        }
        return Ok(match count {
            None => picked
                .into_iter()
                .next()
                .map_or_else(Frame::null, |(f, _)| Frame::Bulk(f)),
            Some(_) if withvalues => pairs(
                cx.resp3,
                picked
                    .into_iter()
                    .map(|(f, v)| (Frame::Bulk(f), Frame::Bulk(v)))
                    .collect(),
            ),
            Some(_) => Frame::Array(picked.into_iter().map(|(f, _)| Frame::Bulk(f)).collect()),
        });
    }
    let all = match &h.body {
        None => Vec::new(),
        Some(b) => cx.hash_all(b)?,
    };
    let Some(count) = count else {
        return Ok(match pick_indexes(all.len(), 1).first() {
            Some(&i) => Frame::Bulk(all[i].0.clone()),
            None => Frame::null(),
        });
    };
    let picks = pick_indexes(all.len(), count);
    Ok(if withvalues {
        pairs(
            cx.resp3,
            picks
                .into_iter()
                .map(|i| (Frame::Bulk(all[i].0.clone()), Frame::Bulk(all[i].1.clone())))
                .collect(),
        )
    } else {
        Frame::Array(
            picks
                .into_iter()
                .map(|i| Frame::Bulk(all[i].0.clone()))
                .collect(),
        )
    })
}

/// Collections of at most this many entries are scanned whole.
pub(crate) const SCAN_WHOLE: usize = INLINE_MAX_ENTRIES;

/// One page of a split collection's elements (`uid`) from field hash
/// `from`: the elements, and the next cursor (0 at the end). A page ends
/// after every element sharing its last element's hash.
pub(crate) fn elem_page<B: Batch>(
    cx: &mut Cx<'_, B>,
    uid: u64,
    from: u64,
    count: usize,
) -> Result<(Vec<(Bytes, Bytes)>, u64), StoreError> {
    let Ok(from) = u32::try_from(from) else {
        return Ok((Vec::new(), 0));
    };
    let (_, end) = cx.space.elems_range(uid);
    let mut got =
        cx.b.scan(&cx.space.elems_from_crc(uid, from), &end, count)?;
    if got.len() < count {
        return Ok((got, 0));
    }
    let Some((last, _)) = got.last() else {
        return Ok((got, 0));
    };
    let (crc, _) = Space::parse_field(last).ok_or_else(|| corrupt("element", last.len()))?;
    let hi = match crc.checked_add(1) {
        Some(n) => cx.space.elems_from_crc(uid, n),
        None => end.clone(),
    };
    let rest = scan_all(cx.b, &crate::layout::after(last), &hi)?;
    got.extend(rest);
    Ok((got, crc.checked_add(1).map_or(0, u64::from)))
}

/// `HSCAN k cursor [MATCH p] [COUNT n] [NOVALUES]`.
fn hscan<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let at = tri!(cursor(&a[2]));
    let o = tri!(scan_opts(a, 3, false, true));
    let h = tri!(cx.hash(&a[1])?);
    let (items, next) = match &h.body {
        None => (Vec::new(), 0),
        Some(b @ HashBody::Inline(_)) => (cx.hash_all(b)?, 0),
        Some(b @ HashBody::Split { count, .. }) if *count as usize <= SCAN_WHOLE => {
            (cx.hash_all(b)?, 0)
        }
        Some(HashBody::Split { uid, .. }) => {
            let (page, next) = elem_page(cx, *uid, at, o.count)?;
            let mut items = Vec::new();
            for (k, v) in page {
                let (_, f) =
                    Space::parse_field(&k).ok_or_else(|| corrupt("hash field", k.len()))?;
                let (exp, v) = field_parts(&v).ok_or_else(|| corrupt("hash field", v.len()))?;
                if exp.is_none_or(|e| e > cx.now) {
                    items.push((Bytes::from(f), v));
                }
            }
            (items, next)
        }
    };
    let mut out = Vec::new();
    for (f, v) in items {
        if o.pattern.as_ref().is_some_and(|p| !glob::matches(p, &f)) {
            continue;
        }
        out.push(Frame::Bulk(f));
        if !o.novalues {
            out.push(Frame::Bulk(v));
        }
    }
    Ok(Frame::Array(vec![
        Frame::Bulk(Bytes::from(next.to_string())),
        Frame::Array(out),
    ]))
}

/// Field lifetimes past this instant are refused (Redis's limit, about
/// the year 4199: 2^46 - 1 ms).
const FIELD_EXP_MAX: i64 = ((1 << 48) - 1) >> 2;

/// The fields of `... FIELDS n f...` at `a[at]`; `httl` picks the read
/// commands' error text for a bad count.
fn fields_arg(a: &[Bytes], at: usize, httl: bool) -> Arg<&[Bytes]> {
    if a.get(at).is_none_or(|f| upper(f) != "FIELDS") {
        return Err(Frame::err(
            "Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    let n = match a
        .get(at + 1)
        .and_then(|n| sprig_proto_redis::num::parse_i64(n))
    {
        Some(n) if n > 0 => n as usize,
        _ if httl => return Err(Frame::err("Number of fields must be a positive integer")),
        _ => return Err(Frame::err("Parameter `numFields` should be greater than 0")),
    };
    let fields = &a[at + 2..];
    if fields.len() != n {
        return Err(Frame::err(
            "The `numfields` parameter must match the number of arguments",
        ));
    }
    Ok(fields)
}

/// `HEXPIRE`, `HPEXPIRE` (`absolute` false), `HEXPIREAT`, `HPEXPIREAT k t
/// [NX|XX|GT|LT] FIELDS n f...`: per field -2 (no such field), 0 (the
/// condition failed), 1 (set) or 2 (the instant has passed: the field is
/// deleted).
fn hexpire<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], unit: i64, absolute: bool) -> Out {
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let when = tri!(int(&a[2]));
    if when < 0 {
        return Ok(Frame::err("invalid expire time, must be >= 0"));
    }
    let cond = match upper(&a[3]).as_str() {
        c @ ("NX" | "XX" | "GT" | "LT") => Some(c.to_owned()),
        _ => None,
    };
    let fields = tri!(fields_arg(a, if cond.is_some() { 4 } else { 3 }, false));
    let at = when
        .checked_mul(unit)
        .and_then(|t| {
            if absolute {
                Some(t)
            } else {
                t.checked_add(cx.now)
            }
        })
        .filter(|t| *t <= FIELD_EXP_MAX);
    let Some(at) = at else {
        return Ok(Frame::err(&format!(
            "invalid expire time in '{name}' command"
        )));
    };
    let h = tri!(cx.hash(&a[1])?);
    let Some(body) = h.body.clone() else {
        return Ok(Frame::Array(
            fields.iter().map(|_| Frame::Integer(-2)).collect(),
        ));
    };
    // The answers first, from the hash as it is.
    let mut out = Vec::with_capacity(fields.len());
    let mut sets: Vec<(Bytes, Bytes, Option<i64>)> = Vec::new();
    let mut dels = Vec::new();
    // A field named twice is decided again on what the first decided.
    let mut decided: std::collections::HashMap<Bytes, (Option<i64>, Bytes)> =
        std::collections::HashMap::new();
    for f in fields {
        if dels.contains(f) {
            out.push(Frame::Integer(-2));
            continue;
        }
        let entry = match decided.get(f) {
            Some(e) => Some(e.clone()),
            None => cx.hash_entry(&body, f)?,
        };
        let Some((cur, v)) = entry else {
            out.push(Frame::Integer(-2));
            continue;
        };
        let ok = match cond.as_deref() {
            Some("NX") => cur.is_none(),
            Some("XX") => cur.is_some(),
            Some("GT") => cur.is_some_and(|c| at > c),
            Some("LT") => cur.is_none_or(|c| at < c),
            _ => true,
        };
        if !ok {
            out.push(Frame::Integer(0));
        } else if at <= cx.now {
            dels.push(f.clone());
            out.push(Frame::Integer(2));
        } else {
            if !sets.iter().any(|(x, _, _)| x == f) {
                sets.push((f.clone(), v.clone(), cur));
            }
            decided.insert(f.clone(), (Some(at), v));
            out.push(Frame::Integer(1));
        }
    }
    if sets.is_empty() && dels.is_empty() {
        return Ok(Frame::Array(out));
    }
    // Only a split hash holds lifetimes.
    let h = cx.hash_with_ttl(&a[1], h)?;
    if !dels.is_empty() {
        cx.hash_write(&a[1], &h, dels.into_iter().map(Edit::Del).collect())?;
    }
    if !sets.is_empty() {
        let h = tri!(cx.hash(&a[1])?);
        if let Some(HashBody::Split { uid, .. }) = h.body {
            let first = cx.fexp_first(cx.space, uid)?;
            for (f, v, cur) in sets {
                if let Some(c) = cur {
                    cx.b.delete(cx.space.fexp(uid, c, &f))?;
                }
                cx.b.put(cx.space.fexp(uid, at, &f), Bytes::new())?;
                cx.b.put(cx.space.field(uid, &f), field_value_ttl(&v, at))?;
            }
            let next = cx.fexp_first(cx.space, uid)?;
            cx.move_hdue(&a[1], first, next)?;
            cx.touched.push(cx.space.meta(&a[1]));
        }
    }
    Ok(Frame::Array(out))
}

impl<B: Batch> Cx<'_, B> {
    /// Hash `h` at `key` made able to hold field lifetimes: split, with the
    /// flag set (an inline hash moves its fields to elements).
    fn hash_with_ttl(&mut self, key: &[u8], h: Hash) -> Result<Hash, StoreError> {
        let body = match h.body.clone() {
            Some(HashBody::Split { ttl: true, .. }) | None => return Ok(h),
            Some(HashBody::Split { uid, count, .. }) => HashBody::Split {
                uid,
                count,
                ttl: true,
            },
            Some(HashBody::Inline(fields)) => {
                let uid = self.take_uid()?;
                for (f, v) in &fields {
                    self.b.put(self.space.field(uid, f), field_value(v))?;
                }
                HashBody::Split {
                    uid,
                    count: fields.len() as u64,
                    ttl: true,
                }
            }
        };
        let new = Meta {
            exp: h.exp,
            body: Body::Hash(body.clone()),
        };
        self.write(key, h.raw.as_ref(), Some(&new))?;
        Ok(Hash {
            body: Some(body),
            exp: h.exp,
            raw: Some(new),
        })
    }
}

/// `HTTL`, `HPTTL` (`ms`), `HEXPIRETIME`, `HPEXPIRETIME` (`absolute`) `k
/// FIELDS n f...`: per field -2 (no such field), -1 (no lifetime) or the
/// time.
fn httl<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], ms: bool, absolute: bool) -> Out {
    let fields = tri!(fields_arg(a, 2, true));
    let h = tri!(cx.hash(&a[1])?);
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        let e = match &h.body {
            None => None,
            Some(b) => cx.hash_entry(b, f)?,
        };
        out.push(Frame::Integer(match e {
            None => -2,
            Some((None, _)) => -1,
            Some((Some(at), _)) => match (absolute, ms) {
                (true, true) => at,
                (true, false) => at / 1000,
                (false, true) => (at - cx.now).max(0),
                (false, false) => ((at - cx.now).max(0) + 999) / 1000,
            },
        }));
    }
    Ok(Frame::Array(out))
}

/// `HPERSIST k FIELDS n f...`: per field -2 (no such field), -1 (no
/// lifetime) or 1 (removed).
fn hpersist<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let fields = tri!(fields_arg(a, 2, false));
    let h = tri!(cx.hash(&a[1])?);
    let mut out = Vec::with_capacity(fields.len());
    let mut keep = Vec::new();
    for f in fields {
        let e = match &h.body {
            None => None,
            Some(b) => cx.hash_entry(b, f)?,
        };
        out.push(Frame::Integer(match e {
            None => -2,
            Some((None, _)) => -1,
            // Named again after its lifetime went: none left.
            Some((Some(_), _)) if keep.iter().any(|(x, _)| x == f) => -1,
            Some((Some(_), v)) => {
                keep.push((f.clone(), v));
                1
            }
        }));
    }
    if !keep.is_empty() {
        cx.hash_write(
            &a[1],
            &h,
            keep.into_iter().map(|(f, v)| Edit::Set(f, v)).collect(),
        )?;
    }
    Ok(Frame::Array(out))
}

/// Removes the due fields of the hash at `key` whose hash-due entry is
/// `(at, key)`, and moves the entry to its next field expiry; the key goes
/// when no field is left. A stale entry is deleted. (The sweeper.)
/// Whether the key went, when fields were removed; `None` for a stale
/// entry.
///
/// # Errors
/// The engine's.
pub fn sweep_hash<B: Batch>(
    cx: &mut Cx<'_, B>,
    at: i64,
    key: &[u8],
) -> Result<Option<bool>, StoreError> {
    let raw = cx.raw(key)?;
    match raw {
        Some(Meta {
            body: Body::Hash(HashBody::Split { uid, ttl: true, .. }),
            exp,
        }) if exp.is_none_or(|e| e > cx.now) && cx.fexp_first(cx.space, uid)? == Some(at) => {
            let h = tri_store(cx.hash(key)?);
            match h {
                Some(h) if h.body.is_some() => {
                    cx.hash_write(key, &h, Vec::new())?;
                    Ok(Some(false))
                }
                // Every field is due: the key goes.
                _ => {
                    cx.write(key, raw.as_ref(), None)?;
                    Ok(Some(true))
                }
            }
        }
        _ => {
            cx.b.delete(cx.space.hdue(at, key))?;
            Ok(None)
        }
    }
}

fn tri_store(r: Arg<Hash>) -> Option<Hash> {
    r.ok()
}
