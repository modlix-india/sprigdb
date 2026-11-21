//! Sorted sets: inline in the metadata while small, ordered by (score,
//! member); else a member entry (`field` key, the score as its value) and a
//! score-index entry (`Z`, ordered by score then member) per member.
//! Ranges by score or rank read the index; ranges by member (`BYLEX`) read
//! the whole set in order, as they are defined only for equal scores.

use std::cmp::Ordering;
use std::collections::HashMap;

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{parse_f64, parse_i64};
use sprig_redis::{glob, reply};

use super::hashes::{
    SCAN_WHOLE, elem_page, fits_inline, pairs, pick_indexes, rand_count, random_elems,
};
use super::keys::{cursor, scan_opts};
use super::{Arg, Cx, Out, corrupt, index_range, int, tri, upper};
use crate::layout::{Body, KIND_ZSCORE, Meta, Space, ZBody};
use crate::store::{Batch, StoreError};

/// Runs a sorted-set command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "zadd" => zadd(cx, a),
        "zincrby" => zincrby(cx, a),
        "zrem" => zrem(cx, a),
        "zscore" => zscore(cx, a),
        "zmscore" => zmscore(cx, a),
        "zcard" => zcard(cx, a),
        "zcount" => zcount(cx, a, false),
        "zlexcount" => zcount(cx, a, true),
        "zrange" => zrange(cx, a, Form::Generic, false, None),
        "zrangestore" => zrange(cx, &a[1..], Form::Generic, false, Some(&a[1])),
        "zrevrange" => zrange(cx, a, Form::Rank, true, None),
        "zrangebyscore" => zrange(cx, a, Form::Score, false, None),
        "zrevrangebyscore" => zrange(cx, a, Form::Score, true, None),
        "zrangebylex" => zrange(cx, a, Form::Lex, false, None),
        "zrevrangebylex" => zrange(cx, a, Form::Lex, true, None),
        "zrank" => zrank(cx, a, false),
        "zrevrank" => zrank(cx, a, true),
        "zremrangebyrank" => zremrange(cx, a, By::Rank),
        "zremrangebyscore" => zremrange(cx, a, By::Score),
        "zremrangebylex" => zremrange(cx, a, By::Lex),
        "zpopmin" => zpop(cx, a, false),
        "zpopmax" => zpop(cx, a, true),
        "zmpop" => zmpop(cx, a, 1),
        "zrandmember" => zrandmember(cx, a),
        "zunion" => zalg(cx, a, ZOp::Union, None),
        "zinter" => zalg(cx, a, ZOp::Inter, None),
        "zdiff" => zalg(cx, a, ZOp::Diff, None),
        "zunionstore" => zalg(cx, &a[1..], ZOp::Union, Some(&a[1])),
        "zinterstore" => zalg(cx, &a[1..], ZOp::Inter, Some(&a[1])),
        "zdiffstore" => zalg(cx, &a[1..], ZOp::Diff, Some(&a[1])),
        "zintercard" => zintercard(cx, a),
        "zscan" => zscan(cx, a),
        _ => return None,
    })
}

/// A key's sorted set: its body and expiry, and the stored metadata.
pub(crate) struct ZK {
    pub body: Option<ZBody>,
    pub exp: Option<i64>,
    pub raw: Option<Meta>,
}

/// Total order of (score, member) pairs.
pub(crate) fn cmp_pair(a: &(f64, Bytes), b: &(f64, Bytes)) -> Ordering {
    a.0.partial_cmp(&b.0)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.1.cmp(&b.1))
}

/// A score as stored: `-0` as `0`.
fn norm(s: f64) -> f64 {
    if s == 0.0 { 0.0 } else { s }
}

/// The number of members of `z`.
pub(crate) fn zlen(z: &ZBody) -> i64 {
    match z {
        ZBody::Inline(items) => items.len() as i64,
        ZBody::Split { count, .. } => *count as i64,
    }
}

/// One change of a sorted set.
pub(crate) enum ZEdit {
    /// Set a member's score.
    Set(f64, Bytes),
    /// Remove a member.
    Rem(Bytes),
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a sorted set; `Err` answers `WRONGTYPE`.
    pub(crate) fn zset(&mut self, key: &[u8]) -> Result<Arg<ZK>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok(ZK {
                body: None,
                exp: None,
                raw,
            }),
            Some(Meta {
                body: Body::ZSet(z),
                exp,
            }) => Ok(ZK {
                body: Some(z),
                exp,
                raw,
            }),
            Some(_) => Err(reply::wrongtype()),
        })
    }

    /// Member `m`'s score in `z`.
    pub(crate) fn zscore_of(&mut self, z: &ZBody, m: &[u8]) -> Result<Option<f64>, StoreError> {
        match z {
            ZBody::Inline(items) => Ok(items.iter().find(|(_, x)| x == m).map(|(s, _)| *s)),
            ZBody::Split { uid, .. } => match self.b.get(&self.space.field(*uid, m))? {
                None => Ok(None),
                Some(v) => {
                    let b: [u8; 8] = v
                        .as_ref()
                        .try_into()
                        .map_err(|_| corrupt("sorted-set member", v.len()))?;
                    Ok(Some(f64::from_bits(u64::from_be_bytes(b))))
                }
            },
        }
    }

    /// Score-index entries `[lo, hi)` of a split set, up to `limit`, as
    /// pairs, from the low end or (`rev`) the high end.
    fn zindex(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
        rev: bool,
    ) -> Result<Vec<(f64, Bytes)>, StoreError> {
        let mut out = Vec::new();
        let (mut lo, mut hi) = (lo.to_vec(), hi.to_vec());
        while out.len() < limit {
            let want = (limit - out.len()).min(1024);
            let got = if rev {
                self.b.scan_rev(&lo, &hi, want)?
            } else {
                self.b.scan(&lo, &hi, want)?
            };
            let n = got.len();
            if let Some((last, _)) = got.last() {
                if rev {
                    hi = last.to_vec();
                } else {
                    lo = crate::layout::after(last);
                }
            }
            for (k, _) in got {
                let (s, m) =
                    Space::parse_zscore(&k).ok_or_else(|| corrupt("score index", k.len()))?;
                out.push((s, Bytes::from(m)));
            }
            if n < want {
                break;
            }
        }
        Ok(out)
    }

    /// Every member of `z` with its score, in order (`rev`: highest first).
    pub(crate) fn z_all(&mut self, z: &ZBody, rev: bool) -> Result<Vec<(f64, Bytes)>, StoreError> {
        match z {
            ZBody::Inline(items) => {
                let mut v = items.clone();
                if rev {
                    v.reverse();
                }
                Ok(v)
            }
            ZBody::Split { uid, .. } => {
                let (lo, hi) = self.space.under_range(KIND_ZSCORE, *uid);
                self.zindex(&lo, &hi, usize::MAX, rev)
            }
        }
    }

    /// Ranks `s..=e` (normalized) of `z`, from the low end or (`rev`) the
    /// high end.
    pub(crate) fn z_ranks(
        &mut self,
        z: &ZBody,
        s: i64,
        e: i64,
        rev: bool,
    ) -> Result<Vec<(f64, Bytes)>, StoreError> {
        match z {
            ZBody::Inline(_) => Ok(self
                .z_all(z, rev)?
                .into_iter()
                .skip(s as usize)
                .take((e - s + 1) as usize)
                .collect()),
            ZBody::Split { uid, .. } => {
                let (lo, hi) = self.space.under_range(KIND_ZSCORE, *uid);
                let mut v = self.zindex(&lo, &hi, (e + 1) as usize, rev)?;
                Ok(v.split_off((s as usize).min(v.len())))
            }
        }
    }

    /// The members of `z` with scores in `r`, in order (`rev`: highest
    /// first), after skipping `offset`, at most `limit`.
    fn z_scores(
        &mut self,
        z: &ZBody,
        r: &ScoreRange,
        rev: bool,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(f64, Bytes)>, StoreError> {
        match z {
            ZBody::Inline(_) => Ok(self
                .z_all(z, rev)?
                .into_iter()
                .filter(|(s, _)| r.has(*s))
                .skip(offset)
                .take(limit)
                .collect()),
            ZBody::Split { uid, .. } => {
                let lo = if r.min_ex {
                    self.space.zscore_after(*uid, r.min)
                } else {
                    self.space.zscore_from(*uid, r.min)
                };
                let hi = if r.max_ex {
                    self.space.zscore_from(*uid, r.max)
                } else {
                    self.space.zscore_after(*uid, r.max)
                };
                if lo >= hi {
                    return Ok(Vec::new());
                }
                let v = self.zindex(&lo, &hi, offset.saturating_add(limit), rev)?;
                Ok(v.into_iter().skip(offset).collect())
            }
        }
    }

    /// `m`'s rank in `z` and its score, counted from the low end or
    /// (`rev`) the high end.
    fn z_rank(&mut self, z: &ZBody, m: &[u8], rev: bool) -> Result<Option<(i64, f64)>, StoreError> {
        let Some(score) = self.zscore_of(z, m)? else {
            return Ok(None);
        };
        let n = zlen(z);
        let below = match z {
            ZBody::Inline(items) => items.iter().take_while(|(_, x)| x != m).count() as i64,
            ZBody::Split { uid, .. } => {
                let (lo, _) = self.space.under_range(KIND_ZSCORE, *uid);
                let at = self.space.zscore(*uid, score, m);
                let mut start = lo;
                let mut c = 0i64;
                loop {
                    let got = self.b.scan(&start, &at, 4096)?;
                    let Some((last, _)) = got.last() else {
                        break;
                    };
                    start = crate::layout::after(last);
                    c += got.len() as i64;
                }
                c
            }
        };
        Ok(Some((if rev { n - 1 - below } else { below }, score)))
    }

    /// Applies `edits` in order to sorted set `z` at `key` (absent: a new
    /// one): members added, members whose score changed, members removed.
    /// The key goes when it empties.
    pub(crate) fn z_write(
        &mut self,
        key: &[u8],
        z: &ZK,
        edits: Vec<ZEdit>,
    ) -> Result<(i64, i64, i64), StoreError> {
        let (mut added, mut changed, mut removed) = (0i64, 0i64, 0i64);
        let body = match z.body.clone().unwrap_or(ZBody::Inline(Vec::new())) {
            ZBody::Inline(mut items) => {
                for e in edits {
                    match e {
                        ZEdit::Set(s, m) => {
                            let s = norm(s);
                            match items.iter().position(|(_, x)| *x == m) {
                                Some(i) if items[i].0 == s => continue,
                                Some(i) => {
                                    items.remove(i);
                                    changed += 1;
                                }
                                None => added += 1,
                            }
                            let p = (s, m);
                            let at = items.partition_point(|x| cmp_pair(x, &p) == Ordering::Less);
                            items.insert(at, p);
                        }
                        ZEdit::Rem(m) => {
                            if let Some(i) = items.iter().position(|(_, x)| *x == m) {
                                items.remove(i);
                                removed += 1;
                            }
                        }
                    }
                }
                if items.is_empty() {
                    None
                } else if fits_inline(items.len(), items.iter().map(|(_, m)| m)) {
                    Some(ZBody::Inline(items))
                } else {
                    // Too big to stay inline: member and index entries.
                    let uid = self.take_uid()?;
                    for (s, m) in &items {
                        self.z_put(uid, *s, m)?;
                    }
                    Some(ZBody::Split {
                        uid,
                        count: items.len() as u64,
                    })
                }
            }
            ZBody::Split { uid, mut count } => {
                for e in edits {
                    match e {
                        ZEdit::Set(s, m) => {
                            let s = norm(s);
                            let old = self.zscore_of(&ZBody::Split { uid, count }, &m)?;
                            match old {
                                Some(o) if o == s => continue,
                                Some(o) => {
                                    self.b.delete(self.space.zscore(uid, o, &m))?;
                                    changed += 1;
                                }
                                None => {
                                    added += 1;
                                    count += 1;
                                }
                            }
                            self.z_put(uid, s, &m)?;
                        }
                        ZEdit::Rem(m) => {
                            if let Some(o) = self.zscore_of(&ZBody::Split { uid, count }, &m)? {
                                self.b.delete(self.space.zscore(uid, o, &m))?;
                                self.b.delete(self.space.field(uid, &m))?;
                                removed += 1;
                                count -= 1;
                            }
                        }
                    }
                }
                (count > 0).then_some(ZBody::Split { uid, count })
            }
        };
        let new = body.map(|b| Meta {
            exp: z.exp,
            body: Body::ZSet(b),
        });
        if new.is_some() || z.raw.is_some() {
            self.write(key, z.raw.as_ref(), new.as_ref())?;
        }
        Ok((added, changed, removed))
    }

    /// Writes member `m` of split set `uid` at score `s`: its member and
    /// index entries.
    fn z_put(&mut self, uid: u64, s: f64, m: &[u8]) -> Result<(), StoreError> {
        self.b.put(
            self.space.field(uid, m),
            Bytes::copy_from_slice(&s.to_bits().to_be_bytes()),
        )?;
        self.b.put(self.space.zscore(uid, s, m), Bytes::new())
    }

    /// Replaces whatever `key` holds with a sorted set of `items` (no
    /// expiry): its size.
    pub(crate) fn z_store(
        &mut self,
        key: &[u8],
        items: Vec<(f64, Bytes)>,
    ) -> Result<i64, StoreError> {
        let raw = self.raw(key)?;
        if raw.is_some() {
            self.write(key, raw.as_ref(), None)?;
        }
        let fresh = ZK {
            body: None,
            exp: None,
            raw: None,
        };
        let edits = items.into_iter().map(|(s, m)| ZEdit::Set(s, m)).collect();
        Ok(self.z_write(key, &fresh, edits)?.0)
    }
}

/// A score as a reply.
pub(crate) fn score(s: f64) -> Frame {
    Frame::Double(s)
}

/// Members, with their scores when asked (flat in RESP2, pairs in RESP3).
pub(crate) fn members(resp3: bool, items: Vec<(f64, Bytes)>, withscores: bool) -> Frame {
    if withscores {
        pairs(
            resp3,
            items
                .into_iter()
                .map(|(s, m)| (Frame::Bulk(m), score(s)))
                .collect(),
        )
    } else {
        Frame::Array(items.into_iter().map(|(_, m)| Frame::Bulk(m)).collect())
    }
}

/// `ZADD k [NX|XX] [GT|LT] [CH] [INCR] score m [score m ...]`.
fn zadd<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, false);
    let mut i = 2;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            "CH" => ch = true,
            "INCR" => incr = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &a[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Ok(reply::syntax());
    }
    if nx && xx {
        return Ok(Frame::err(
            "XX and NX options at the same time are not compatible",
        ));
    }
    if (nx && (gt || lt)) || (gt && lt) {
        return Ok(Frame::err(
            "GT, LT, and/or NX options at the same time are not compatible",
        ));
    }
    if incr && rest.len() > 2 {
        return Ok(Frame::err(
            "INCR option supports a single increment-element pair",
        ));
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for c in rest.chunks(2) {
        pairs.push((tri!(super::float(&c[0])), c[1].clone()));
    }
    let z = tri!(cx.zset(&a[1])?);
    let mut cur: HashMap<Bytes, Option<f64>> = HashMap::new();
    let mut edits = Vec::new();
    let mut result = None;
    for (s, m) in pairs {
        let old = match cur.get(&m) {
            Some(o) => *o,
            None => match &z.body {
                None => None,
                Some(b) => cx.zscore_of(b, &m)?,
            },
        };
        let new = if incr {
            let n = old.unwrap_or(0.0) + s;
            if n.is_nan() {
                return Ok(Frame::err("resulting score is not a number (NaN)"));
            }
            n
        } else {
            s
        };
        let go = match old {
            None => !xx,
            Some(o) => !nx && (!gt || new > o) && (!lt || new < o),
        };
        if go {
            cur.insert(m.clone(), Some(new));
            edits.push(ZEdit::Set(new, m));
            result = Some(new);
        } else if incr {
            result = None;
        }
    }
    let (added, changed, _) = cx.z_write(&a[1], &z, edits)?;
    Ok(if incr {
        result.map_or_else(Frame::null, score)
    } else if ch {
        Frame::Integer(added + changed)
    } else {
        Frame::Integer(added)
    })
}

/// `ZINCRBY k incr m`.
fn zincrby<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let by = tri!(super::float(&a[2]));
    let z = tri!(cx.zset(&a[1])?);
    let old = match &z.body {
        None => None,
        Some(b) => cx.zscore_of(b, &a[3])?,
    };
    let n = old.unwrap_or(0.0) + by;
    if n.is_nan() {
        return Ok(Frame::err("resulting score is not a number (NaN)"));
    }
    cx.z_write(&a[1], &z, vec![ZEdit::Set(n, a[3].clone())])?;
    Ok(score(n))
}

/// `ZREM k m ...`.
fn zrem<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let z = tri!(cx.zset(&a[1])?);
    if z.body.is_none() {
        return Ok(Frame::Integer(0));
    }
    let edits = a[2..].iter().map(|m| ZEdit::Rem(m.clone())).collect();
    Ok(Frame::Integer(cx.z_write(&a[1], &z, edits)?.2))
}

/// `ZSCORE k m`.
fn zscore<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let z = tri!(cx.zset(&a[1])?);
    Ok(match &z.body {
        None => Frame::null(),
        Some(b) => cx.zscore_of(b, &a[2])?.map_or_else(Frame::null, score),
    })
}

/// `ZMSCORE k m ...`.
fn zmscore<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let z = tri!(cx.zset(&a[1])?);
    let mut out = Vec::new();
    for m in &a[2..] {
        out.push(match &z.body {
            None => Frame::null(),
            Some(b) => cx.zscore_of(b, m)?.map_or_else(Frame::null, score),
        });
    }
    Ok(Frame::Array(out))
}

/// `ZCARD k`.
fn zcard<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let z = tri!(cx.zset(&a[1])?);
    Ok(Frame::Integer(z.body.as_ref().map_or(0, zlen)))
}

/// A score range: `(` makes a bound exclusive; `-inf`, `+inf`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScoreRange {
    pub min: f64,
    pub max: f64,
    pub min_ex: bool,
    pub max_ex: bool,
}

impl ScoreRange {
    fn has(&self, s: f64) -> bool {
        let lo = if self.min_ex {
            s > self.min
        } else {
            s >= self.min
        };
        let hi = if self.max_ex {
            s < self.max
        } else {
            s <= self.max
        };
        lo && hi
    }
}

fn score_bound(a: &[u8]) -> Option<(f64, bool)> {
    let (ex, body) = match a.first() {
        Some(b'(') => (true, &a[1..]),
        _ => (false, a),
    };
    parse_f64(body).filter(|f| !f.is_nan()).map(|f| (f, ex))
}

/// `min max` as a score range.
pub(crate) fn score_range(min: &[u8], max: &[u8]) -> Arg<ScoreRange> {
    match (score_bound(min), score_bound(max)) {
        (Some((min, min_ex)), Some((max, max_ex))) => Ok(ScoreRange {
            min,
            max,
            min_ex,
            max_ex,
        }),
        _ => Err(Frame::err("min or max is not a float")),
    }
}

/// One end of a member range: `-`, `+`, `[x` or `(x`.
#[derive(Debug, Clone)]
enum Lex {
    Min,
    Max,
    In(Bytes),
    Ex(Bytes),
}

fn lex_bound(a: &Bytes) -> Option<Lex> {
    match a.first() {
        Some(b'-') if a.len() == 1 => Some(Lex::Min),
        Some(b'+') if a.len() == 1 => Some(Lex::Max),
        Some(b'[') => Some(Lex::In(a.slice(1..))),
        Some(b'(') => Some(Lex::Ex(a.slice(1..))),
        _ => None,
    }
}

/// A member range.
#[derive(Debug, Clone)]
struct LexRange {
    min: Lex,
    max: Lex,
}

impl LexRange {
    fn has(&self, m: &[u8]) -> bool {
        let lo = match &self.min {
            Lex::Min => true,
            Lex::Max => false,
            Lex::In(x) => m >= x.as_ref(),
            Lex::Ex(x) => m > x.as_ref(),
        };
        let hi = match &self.max {
            Lex::Min => false,
            Lex::Max => true,
            Lex::In(x) => m <= x.as_ref(),
            Lex::Ex(x) => m < x.as_ref(),
        };
        lo && hi
    }
}

fn lex_range(min: &Bytes, max: &Bytes) -> Arg<LexRange> {
    match (lex_bound(min), lex_bound(max)) {
        (Some(min), Some(max)) => Ok(LexRange { min, max }),
        _ => Err(Frame::err("min or max not valid string range item")),
    }
}

/// `ZCOUNT k min max`, `ZLEXCOUNT k min max`.
fn zcount<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], lex: bool) -> Out {
    if lex {
        let r = tri!(lex_range(&a[2], &a[3]));
        let z = tri!(cx.zset(&a[1])?);
        let Some(b) = &z.body else {
            return Ok(Frame::Integer(0));
        };
        let n = cx.z_all(b, false)?.iter().filter(|(_, m)| r.has(m)).count();
        return Ok(Frame::Integer(n as i64));
    }
    let r = tri!(score_range(&a[2], &a[3]));
    let z = tri!(cx.zset(&a[1])?);
    let Some(b) = &z.body else {
        return Ok(Frame::Integer(0));
    };
    let n = cx.z_scores(b, &r, false, 0, usize::MAX)?.len();
    Ok(Frame::Integer(n as i64))
}

/// Which range command form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    /// `ZRANGE` (and `ZRANGESTORE`): `BYSCORE`, `BYLEX`, `REV` as options.
    Generic,
    /// By rank.
    Rank,
    /// By score.
    Score,
    /// By member.
    Lex,
}

/// The range a `ZRANGE`-family command asks for.
enum Range {
    Rank(i64, i64),
    Score(ScoreRange),
    Lex(LexRange),
}

/// The members of `z` a range asks for, in order.
fn select<B: Batch>(
    cx: &mut Cx<'_, B>,
    z: &ZBody,
    r: &Range,
    rev: bool,
    offset: i64,
    limit: i64,
) -> Result<Vec<(f64, Bytes)>, StoreError> {
    // A negative offset gives nothing; a negative count means all.
    if offset < 0 {
        return Ok(Vec::new());
    }
    let (off, lim) = (
        offset as usize,
        if limit < 0 {
            usize::MAX
        } else {
            limit as usize
        },
    );
    match r {
        Range::Rank(s, e) => match index_range(zlen(z), *s, *e) {
            None => Ok(Vec::new()),
            Some((s, e)) => cx.z_ranks(z, s, e, rev),
        },
        Range::Score(sr) => cx.z_scores(z, sr, rev, off, lim),
        Range::Lex(lr) => Ok(cx
            .z_all(z, rev)?
            .into_iter()
            .filter(|(_, m)| lr.has(m))
            .skip(off)
            .take(lim)
            .collect()),
    }
}

/// `ZRANGE k start stop [BYSCORE|BYLEX] [REV] [LIMIT o c] [WITHSCORES]` and
/// its older forms; with `dst`, `ZRANGESTORE dst k ...` (`a` from `dst`).
fn zrange<B: Batch>(
    cx: &mut Cx<'_, B>,
    a: &[Bytes],
    form: Form,
    rev0: bool,
    dst: Option<&Bytes>,
) -> Out {
    let (key, start, stop) = (&a[1], &a[2], &a[3]);
    let (mut by, mut rev) = (form, rev0);
    let (mut withscores, mut limit) = (false, None);
    let mut i = 4;
    while i < a.len() {
        let o = upper(&a[i]);
        match o.as_str() {
            "WITHSCORES" if dst.is_none() && form != Form::Lex => withscores = true,
            "LIMIT" if i + 2 < a.len() && form != Form::Rank => {
                limit = Some((tri!(int(&a[i + 1])), tri!(int(&a[i + 2]))));
                i += 2;
            }
            "BYSCORE" if form == Form::Generic && by == Form::Generic => by = Form::Score,
            "BYLEX" if form == Form::Generic && by == Form::Generic => by = Form::Lex,
            "REV" if form == Form::Generic => rev = true,
            _ => return Ok(reply::syntax()),
        }
        i += 1;
    }
    if limit.is_some() && matches!(by, Form::Generic | Form::Rank) {
        return Ok(Frame::err(
            "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        ));
    }
    if withscores && by == Form::Lex {
        return Ok(Frame::err(
            "syntax error, WITHSCORES not supported in combination with BYLEX",
        ));
    }
    // Reversed score and member ranges give the high end first.
    let (lo, hi) = if rev && matches!(by, Form::Score | Form::Lex) {
        (stop, start)
    } else {
        (start, stop)
    };
    let range = match by {
        Form::Generic | Form::Rank => Range::Rank(tri!(int(lo)), tri!(int(hi))),
        Form::Score => Range::Score(tri!(score_range(lo, hi))),
        Form::Lex => Range::Lex(tri!(lex_range(lo, hi))),
    };
    let z = tri!(cx.zset(key)?);
    let (offset, count) = limit.unwrap_or((0, -1));
    let items = match &z.body {
        None => Vec::new(),
        Some(b) => select(cx, b, &range, rev, offset, count)?,
    };
    match dst {
        Some(d) => Ok(Frame::Integer(cx.z_store(d, items)?)),
        None => Ok(members(cx.resp3, items, withscores)),
    }
}

/// `ZRANK k m [WITHSCORE]`, `ZREVRANK`.
fn zrank<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], rev: bool) -> Out {
    let withscore = match a.get(3) {
        None => false,
        Some(o) if upper(o) == "WITHSCORE" && a.len() == 4 => true,
        _ => return Ok(reply::syntax()),
    };
    let z = tri!(cx.zset(&a[1])?);
    let r = match &z.body {
        None => None,
        Some(b) => cx.z_rank(b, &a[2], rev)?,
    };
    Ok(match (r, withscore) {
        (None, true) => Frame::null_array(),
        (None, false) => Frame::null(),
        (Some((r, _)), false) => Frame::Integer(r),
        (Some((r, s)), true) => Frame::Array(vec![Frame::Integer(r), score(s)]),
    })
}

/// What `ZREMRANGEBY*` removes by.
#[derive(Debug, Clone, Copy)]
enum By {
    Rank,
    Score,
    Lex,
}

/// `ZREMRANGEBYRANK|SCORE|LEX k min max`.
fn zremrange<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], by: By) -> Out {
    let range = match by {
        By::Rank => Range::Rank(tri!(int(&a[2])), tri!(int(&a[3]))),
        By::Score => Range::Score(tri!(score_range(&a[2], &a[3]))),
        By::Lex => Range::Lex(tri!(lex_range(&a[2], &a[3]))),
    };
    let z = tri!(cx.zset(&a[1])?);
    let Some(b) = z.body.clone() else {
        return Ok(Frame::Integer(0));
    };
    let gone = select(cx, &b, &range, false, 0, -1)?;
    if gone.is_empty() {
        return Ok(Frame::Integer(0));
    }
    let edits = gone.into_iter().map(|(_, m)| ZEdit::Rem(m)).collect();
    Ok(Frame::Integer(cx.z_write(&a[1], &z, edits)?.2))
}

/// Pops up to `n` members from the low (or `max`: high) end of `z` at
/// `key`.
pub(crate) fn z_pop<B: Batch>(
    cx: &mut Cx<'_, B>,
    key: &[u8],
    z: &ZK,
    n: usize,
    max: bool,
) -> Result<Vec<(f64, Bytes)>, StoreError> {
    let Some(b) = &z.body else {
        return Ok(Vec::new());
    };
    let len = zlen(b);
    if n == 0 || len == 0 {
        return Ok(Vec::new());
    }
    let items = cx.z_ranks(b, 0, (n as i64).min(len) - 1, max)?;
    let edits = items.iter().map(|(_, m)| ZEdit::Rem(m.clone())).collect();
    cx.z_write(key, z, edits)?;
    Ok(items)
}

/// `ZPOPMIN k [count]`, `ZPOPMAX`.
fn zpop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], max: bool) -> Out {
    if a.len() > 3 {
        return Ok(reply::syntax());
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => match parse_i64(c) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return Ok(Frame::err("value is out of range, must be positive")),
        },
    };
    let z = tri!(cx.zset(&a[1])?);
    let items = z_pop(cx, &a[1], &z, count.unwrap_or(1), max)?;
    Ok(match count {
        // One member: a flat pair in either protocol.
        None if cx.resp3 => match items.into_iter().next() {
            Some((s, m)) => Frame::Array(vec![Frame::Bulk(m), score(s)]),
            None => Frame::Array(Vec::new()),
        },
        _ => members(cx.resp3, items, true),
    })
}

/// The keys, end and count of `numkeys k... MIN|MAX [COUNT n]` at `a[at]`.
pub(crate) fn zmpop_args(a: &[Bytes], at: usize) -> Arg<(Vec<Bytes>, bool, usize)> {
    let n = match parse_i64(&a[at]) {
        Some(n) if n > 0 => n as usize,
        _ => return Err(Frame::err("numkeys should be greater than 0")),
    };
    if n >= a.len() - at {
        return Err(reply::syntax());
    }
    let keys = a[at + 1..at + 1 + n].to_vec();
    let rest = &a[at + 1 + n..];
    let max = match rest.first().map(|e| upper(e)).as_deref() {
        Some("MIN") => false,
        Some("MAX") => true,
        _ => return Err(reply::syntax()),
    };
    let mut count = 1;
    match rest.len() {
        1 => {}
        3 if upper(&rest[1]) == "COUNT" => {
            count = match parse_i64(&rest[2]) {
                Some(c) if c > 0 => c as usize,
                _ => return Err(Frame::err("count should be greater than 0")),
            };
        }
        _ => return Err(reply::syntax()),
    }
    Ok((keys, max, count))
}

/// A key and the members popped from it.
pub(crate) type ZPopped = (Bytes, Vec<(f64, Bytes)>);

/// Pops from the first non-empty sorted set of `keys`.
pub(crate) fn try_zmpop<B: Batch>(
    cx: &mut Cx<'_, B>,
    keys: &[Bytes],
    max: bool,
    count: usize,
) -> Result<Arg<Option<ZPopped>>, StoreError> {
    for k in keys {
        let z = match cx.zset(k)? {
            Ok(z) => z,
            Err(f) => return Ok(Err(f)),
        };
        if z.body.is_some() {
            let items = z_pop(cx, k, &z, count, max)?;
            return Ok(Ok(Some((k.clone(), items))));
        }
    }
    Ok(Ok(None))
}

/// `[key, [[m, s], ...]]`: pairs nest in both protocols.
pub(crate) fn mpop_reply(k: Bytes, items: Vec<(f64, Bytes)>) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(k),
        Frame::Array(
            items
                .into_iter()
                .map(|(s, m)| Frame::Array(vec![Frame::Bulk(m), score(s)]))
                .collect(),
        ),
    ])
}

/// `ZMPOP numkeys k... MIN|MAX [COUNT n]`.
fn zmpop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], at: usize) -> Out {
    let (keys, max, count) = tri!(zmpop_args(a, at));
    Ok(match tri!(try_zmpop(cx, &keys, max, count)?) {
        None => Frame::null_array(),
        Some((k, items)) => mpop_reply(k, items),
    })
}

/// `ZRANDMEMBER k [count [WITHSCORES]]`.
fn zrandmember<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (count, withscores) = match a.len() {
        2 => (None, false),
        3 => (Some(tri!(rand_count(&a[2]))), false),
        4 if upper(&a[3]) == "WITHSCORES" => (Some(tri!(rand_count(&a[2]))), true),
        _ => return Ok(reply::syntax()),
    };
    let z = tri!(cx.zset(&a[1])?);
    if let Some(ZBody::Split { uid, count: n }) = z.body
        && let Some(got) = random_elems(cx, uid, n, count.unwrap_or(1))?
    {
        let mut picked = Vec::with_capacity(got.len());
        for (k, v) in got {
            let (_, m) =
                Space::parse_field(&k).ok_or_else(|| corrupt("sorted-set member", k.len()))?;
            let b: [u8; 8] = v
                .as_ref()
                .try_into()
                .map_err(|_| corrupt("sorted-set member", v.len()))?;
            picked.push((f64::from_bits(u64::from_be_bytes(b)), Bytes::from(m)));
        }
        return Ok(match count {
            None => picked
                .into_iter()
                .next()
                .map_or_else(Frame::null, |(_, m)| Frame::Bulk(m)),
            Some(_) => members(cx.resp3, picked, withscores),
        });
    }
    let all = match &z.body {
        None => Vec::new(),
        Some(b) => cx.z_all(b, false)?,
    };
    let Some(count) = count else {
        return Ok(match pick_indexes(all.len(), 1).first() {
            Some(&i) => Frame::Bulk(all[i].1.clone()),
            None => Frame::null(),
        });
    };
    let picked = pick_indexes(all.len(), count)
        .into_iter()
        .map(|i| all[i].clone())
        .collect();
    Ok(members(cx.resp3, picked, withscores))
}

/// A sorted-set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZOp {
    Union,
    Inter,
    Diff,
}

/// How scores of a member in several inputs combine.
#[derive(Debug, Clone, Copy)]
enum Agg {
    Sum,
    Min,
    Max,
}

fn agg(a: Agg, x: f64, y: f64) -> f64 {
    let r = match a {
        Agg::Sum => x + y,
        Agg::Min => x.min(y),
        Agg::Max => x.max(y),
    };
    // inf + -inf: Redis counts it as 0.
    if r.is_nan() { 0.0 } else { r }
}

/// Members with their scores.
type Scored = Vec<(f64, Bytes)>;

/// The members and scores of each input key (a set counts with score 1, a
/// missing key is empty); `Err` for another type.
fn inputs<B: Batch>(cx: &mut Cx<'_, B>, keys: &[Bytes]) -> Result<Arg<Vec<Scored>>, StoreError> {
    let mut out = Vec::with_capacity(keys.len());
    for k in keys {
        match cx.live(k)?.map(|m| m.body) {
            None => out.push(Vec::new()),
            Some(Body::ZSet(z)) => out.push(cx.z_all(&z, false)?),
            Some(Body::Set(s)) => out.push(cx.set_all(&s)?.into_iter().map(|m| (1.0, m)).collect()),
            Some(_) => return Ok(Err(reply::wrongtype())),
        }
    }
    Ok(Ok(out))
}

/// `ZUNION|ZINTER|ZDIFF numkeys k... [WEIGHTS w...] [AGGREGATE ...]
/// [WITHSCORES]`; with `dst`, the `STORE` forms (`a` from `dst`).
fn zalg<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], op: ZOp, dst: Option<&Bytes>) -> Out {
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let name = if dst.is_some() {
        format!(
            "{}store",
            match op {
                ZOp::Union => "zunion",
                ZOp::Inter => "zinter",
                ZOp::Diff => "zdiff",
            }
        )
    } else {
        name
    };
    let n = match parse_i64(&a[1]) {
        Some(n) if n > 0 => n as usize,
        Some(_) => {
            return Ok(Frame::err(&format!(
                "at least 1 input key is needed for '{name}' command"
            )));
        }
        None => return Ok(reply::not_int()),
    };
    if n > a.len() - 2 {
        return Ok(reply::syntax());
    }
    let keys = &a[2..2 + n];
    let mut weights = vec![1.0; n];
    let mut how = Agg::Sum;
    let mut withscores = false;
    let mut i = 2 + n;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "WEIGHTS" if op != ZOp::Diff && i + n < a.len() => {
                for (j, w) in weights.iter_mut().enumerate() {
                    match parse_f64(&a[i + 1 + j]).filter(|f| !f.is_nan()) {
                        Some(f) => *w = f,
                        None => return Ok(Frame::err("weight value is not a float")),
                    }
                }
                i += n;
            }
            "AGGREGATE" if op != ZOp::Diff && i + 1 < a.len() => {
                how = match upper(&a[i + 1]).as_str() {
                    "SUM" => Agg::Sum,
                    "MIN" => Agg::Min,
                    "MAX" => Agg::Max,
                    _ => return Ok(reply::syntax()),
                };
                i += 1;
            }
            "WITHSCORES" if dst.is_none() => withscores = true,
            _ => return Ok(reply::syntax()),
        }
        i += 1;
    }
    let sets = tri!(inputs(cx, keys)?);
    let weighted = |s: f64, w: f64| {
        let r = s * w;
        if r.is_nan() { 0.0 } else { r }
    };
    let mut acc: Vec<(Bytes, f64)> = Vec::new();
    let mut at: HashMap<Bytes, usize> = HashMap::new();
    match op {
        ZOp::Union => {
            for (set, w) in sets.iter().zip(&weights) {
                for (s, m) in set {
                    let v = weighted(*s, *w);
                    if let Some(&j) = at.get(m) {
                        acc[j].1 = agg(how, acc[j].1, v);
                    } else {
                        at.insert(m.clone(), acc.len());
                        acc.push((m.clone(), v));
                    }
                }
            }
        }
        ZOp::Inter => {
            let maps: Vec<HashMap<&Bytes, f64>> = sets[1..]
                .iter()
                .map(|s| s.iter().map(|(sc, m)| (m, *sc)).collect())
                .collect();
            for (s, m) in &sets[0] {
                let mut v = weighted(*s, weights[0]);
                let mut all = true;
                for (j, mp) in maps.iter().enumerate() {
                    if let Some(x) = mp.get(m) {
                        v = agg(how, v, weighted(*x, weights[j + 1]));
                    } else {
                        all = false;
                        break;
                    }
                }
                if all {
                    acc.push((m.clone(), v));
                }
            }
        }
        ZOp::Diff => {
            let rest: std::collections::HashSet<&Bytes> =
                sets[1..].iter().flatten().map(|(_, m)| m).collect();
            for (s, m) in &sets[0] {
                if !rest.contains(m) {
                    acc.push((m.clone(), *s));
                }
            }
        }
    }
    let mut items: Vec<(f64, Bytes)> = acc.into_iter().map(|(m, s)| (norm(s), m)).collect();
    items.sort_by(cmp_pair);
    match dst {
        Some(d) => Ok(Frame::Integer(cx.z_store(d, items)?)),
        None => Ok(members(cx.resp3, items, withscores)),
    }
}

/// `ZINTERCARD numkeys k... [LIMIT n]`.
fn zintercard<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    if parse_i64(&a[1]).is_some_and(|n| n <= 0) {
        return Ok(Frame::err(
            "at least 1 input key is needed for 'zintercard' command",
        ));
    }
    let (keys, limit) = tri!(super::sets::numkeys_limit(a));
    let sets = tri!(inputs(cx, keys)?);
    let rest: Vec<std::collections::HashSet<&Bytes>> = sets[1..]
        .iter()
        .map(|s| s.iter().map(|(_, m)| m).collect())
        .collect();
    let n = sets[0]
        .iter()
        .filter(|(_, m)| rest.iter().all(|r| r.contains(m)))
        .count();
    Ok(Frame::Integer(
        if limit > 0 { n.min(limit) } else { n } as i64
    ))
}

/// `ZSCAN k cursor [MATCH p] [COUNT n]`: members with their scores.
fn zscan<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let at = tri!(cursor(&a[2]));
    let o = tri!(scan_opts(a, 3, false, false));
    let z = tri!(cx.zset(&a[1])?);
    let (items, next): (Vec<(f64, Bytes)>, u64) = match &z.body {
        None => (Vec::new(), 0),
        Some(b @ ZBody::Inline(_)) => (cx.z_all(b, false)?, 0),
        Some(b @ ZBody::Split { count, .. }) if *count as usize <= SCAN_WHOLE => {
            (cx.z_all(b, false)?, 0)
        }
        Some(ZBody::Split { uid, .. }) => {
            let (page, next) = elem_page(cx, *uid, at, o.count)?;
            let items = page
                .into_iter()
                .map(|(k, v)| {
                    let (_, m) = Space::parse_field(&k)
                        .ok_or_else(|| corrupt("sorted-set member", k.len()))?;
                    let b: [u8; 8] = v
                        .as_ref()
                        .try_into()
                        .map_err(|_| corrupt("sorted-set member", v.len()))?;
                    Ok((f64::from_bits(u64::from_be_bytes(b)), Bytes::from(m)))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            (items, next)
        }
    };
    let mut out = Vec::new();
    for (s, m) in items {
        if o.pattern.as_ref().is_some_and(|p| !glob::matches(p, &m)) {
            continue;
        }
        out.push(Frame::Bulk(m));
        out.push(Frame::bulk(sprig_proto_redis::num::d2string(s)));
    }
    Ok(Frame::Array(vec![
        Frame::Bulk(Bytes::from(next.to_string())),
        Frame::Array(out),
    ]))
}
