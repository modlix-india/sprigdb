//! Sets: inline in the metadata while small (integers only, in numeric
//! order, up to 512; otherwise in insertion order, as hashes), else one
//! element entry per member, ordered by the member's CRC32.

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_i64;
use sprig_redis::{glob, reply};

use super::hashes::{
    SCAN_WHOLE, elem_page, fits_inline, pick_indexes, rand_count, random_elems, scan_all,
};
use super::keys::{cursor, scan_opts};
use super::{Arg, Cx, Out, corrupt, int, tri};
use crate::layout::{Body, INTSET_MAX_ENTRIES, Meta, SetBody, Space};
use crate::store::{Batch, StoreError};

/// Runs a set command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "sadd" => sadd(cx, a),
        "srem" => srem(cx, a),
        "smembers" => smembers(cx, a),
        "sismember" => sismember(cx, a),
        "smismember" => smismember(cx, a),
        "scard" => scard(cx, a),
        "spop" => spop(cx, a),
        "srandmember" => srandmember(cx, a),
        "smove" => smove(cx, a),
        "sinter" => algebra(cx, &a[1..], Op::Inter, None),
        "sunion" => algebra(cx, &a[1..], Op::Union, None),
        "sdiff" => algebra(cx, &a[1..], Op::Diff, None),
        "sinterstore" => algebra(cx, &a[2..], Op::Inter, Some(&a[1])),
        "sunionstore" => algebra(cx, &a[2..], Op::Union, Some(&a[1])),
        "sdiffstore" => algebra(cx, &a[2..], Op::Diff, Some(&a[1])),
        "sintercard" => sintercard(cx, a),
        "sscan" => sscan(cx, a),
        _ => return None,
    })
}

/// A key's set: its body and expiry, and the stored metadata.
pub(crate) struct SetK {
    pub body: Option<SetBody>,
    pub exp: Option<i64>,
    pub raw: Option<Meta>,
}

/// Whether `m` is an integer as an integer set holds it.
fn is_int(m: &[u8]) -> bool {
    m.len() <= 20 && parse_i64(m).is_some_and(|n| n.to_string().as_bytes() == m)
}

/// The number of members of `s`.
pub(crate) fn set_len(s: &SetBody) -> i64 {
    match s {
        SetBody::Inline { members, .. } => members.len() as i64,
        SetBody::Split { count, .. } => *count as i64,
    }
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a set; `Err` answers `WRONGTYPE`.
    pub(crate) fn set(&mut self, key: &[u8]) -> Result<Arg<SetK>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok(SetK {
                body: None,
                exp: None,
                raw,
            }),
            Some(Meta {
                body: Body::Set(s),
                exp,
            }) => Ok(SetK {
                body: Some(s),
                exp,
                raw,
            }),
            Some(_) => Err(reply::wrongtype()),
        })
    }

    /// Whether `m` is a member of `s`.
    pub(crate) fn set_has(&mut self, s: &SetBody, m: &[u8]) -> Result<bool, StoreError> {
        match s {
            SetBody::Inline { members, .. } => Ok(members.iter().any(|x| x == m)),
            SetBody::Split { uid, .. } => Ok(self.b.get(&self.space.field(*uid, m))?.is_some()),
        }
    }

    /// Every member of `s`.
    pub(crate) fn set_all(&mut self, s: &SetBody) -> Result<Vec<Bytes>, StoreError> {
        match s {
            SetBody::Inline { members, .. } => Ok(members.clone()),
            SetBody::Split { uid, .. } => {
                let (lo, hi) = self.space.elems_range(*uid);
                scan_all(self.b, &lo, &hi)?
                    .into_iter()
                    .map(|(k, _)| {
                        Space::parse_field(&k)
                            .map(|(_, m)| Bytes::from(m))
                            .ok_or_else(|| corrupt("set member", k.len()))
                    })
                    .collect()
            }
        }
    }

    /// Adds `adds` to and removes `dels` from set `s` at `key` (absent: a
    /// new set): members added and removed. The key goes when it empties.
    pub(crate) fn set_write(
        &mut self,
        key: &[u8],
        s: &SetK,
        adds: &[Bytes],
        dels: &[Bytes],
    ) -> Result<(i64, i64), StoreError> {
        let (mut added, mut removed) = (0i64, 0i64);
        let empty = SetBody::Inline {
            ints: true,
            members: Vec::new(),
        };
        let body = match s.body.clone().unwrap_or(empty) {
            SetBody::Inline {
                mut ints,
                mut members,
            } => {
                for m in adds {
                    if members.contains(m) {
                        continue;
                    }
                    added += 1;
                    if ints && is_int(m) {
                        let v = parse_i64(m).unwrap_or(0);
                        let at = members.partition_point(|x| parse_i64(x).unwrap_or(0) < v);
                        members.insert(at, m.clone());
                    } else {
                        ints = false;
                        members.push(m.clone());
                    }
                }
                for m in dels {
                    if let Some(i) = members.iter().position(|x| x == m) {
                        members.remove(i);
                        removed += 1;
                    }
                }
                let fits = if ints {
                    members.len() <= INTSET_MAX_ENTRIES
                } else {
                    fits_inline(members.len(), members.iter())
                };
                if members.is_empty() {
                    None
                } else if fits {
                    Some(SetBody::Inline { ints, members })
                } else {
                    // Too big to stay inline: the members become elements.
                    let uid = self.take_uid()?;
                    for m in &members {
                        self.b.put(self.space.field(uid, m), Bytes::new())?;
                    }
                    Some(SetBody::Split {
                        uid,
                        count: members.len() as u64,
                    })
                }
            }
            SetBody::Split { uid, mut count } => {
                for m in adds {
                    let k = self.space.field(uid, m);
                    if self.b.get(&k)?.is_none() {
                        count += 1;
                        added += 1;
                        self.b.put(k, Bytes::new())?;
                    }
                }
                for m in dels {
                    let k = self.space.field(uid, m);
                    if self.b.get(&k)?.is_some() {
                        count -= 1;
                        removed += 1;
                        self.b.delete(k)?;
                    }
                }
                (count > 0).then_some(SetBody::Split { uid, count })
            }
        };
        let new = body.map(|b| Meta {
            exp: s.exp,
            body: Body::Set(b),
        });
        if new.is_some() || s.raw.is_some() {
            self.write(key, s.raw.as_ref(), new.as_ref())?;
        }
        Ok((added, removed))
    }

    /// Replaces whatever `key` holds with a new set of `members` (no
    /// expiry): its size.
    pub(crate) fn set_store(&mut self, key: &[u8], members: &[Bytes]) -> Result<i64, StoreError> {
        let raw = self.raw(key)?;
        if raw.is_some() {
            self.write(key, raw.as_ref(), None)?;
        }
        let fresh = SetK {
            body: None,
            exp: None,
            raw: None,
        };
        let (n, _) = self.set_write(key, &fresh, members, &[])?;
        Ok(n)
    }
}

/// `SADD k m ...`.
fn sadd<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    Ok(Frame::Integer(cx.set_write(&a[1], &s, &a[2..], &[])?.0))
}

/// `SREM k m ...`.
fn srem<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    if s.body.is_none() {
        return Ok(Frame::Integer(0));
    }
    Ok(Frame::Integer(cx.set_write(&a[1], &s, &[], &a[2..])?.1))
}

/// Members as a reply: a set in RESP3, an array in RESP2.
fn members_frame(resp3: bool, ms: Vec<Bytes>) -> Frame {
    let items = ms.into_iter().map(Frame::Bulk).collect();
    if resp3 {
        Frame::Set(items)
    } else {
        Frame::Array(items)
    }
}

/// `SMEMBERS k`.
fn smembers<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    let all = match &s.body {
        None => Vec::new(),
        Some(b) => cx.set_all(b)?,
    };
    Ok(members_frame(cx.resp3, all))
}

/// `SISMEMBER k m`.
fn sismember<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    Ok(Frame::Integer(match &s.body {
        None => 0,
        Some(b) => i64::from(cx.set_has(b, &a[2])?),
    }))
}

/// `SMISMEMBER k m ...`.
fn smismember<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    let mut out = Vec::new();
    for m in &a[2..] {
        out.push(Frame::Integer(match &s.body {
            None => 0,
            Some(b) => i64::from(cx.set_has(b, m)?),
        }));
    }
    Ok(Frame::Array(out))
}

/// `SCARD k`.
fn scard<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    Ok(Frame::Integer(s.body.as_ref().map_or(0, set_len)))
}

/// `SPOP k [count]`.
fn spop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    if a.len() > 3 {
        return Ok(reply::syntax());
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => match parse_i64(c) {
            Some(n) if n >= 0 => Some(n),
            _ => return Ok(Frame::err("value is out of range, must be positive")),
        },
    };
    let s = tri!(cx.set(&a[1])?);
    let Some(body) = &s.body else {
        return Ok(if count.is_some() {
            members_frame(cx.resp3, Vec::new())
        } else {
            Frame::null()
        });
    };
    let picked: Vec<Bytes> = if let Some(p) = picks(cx, body, count.unwrap_or(1))? {
        p
    } else {
        let all = cx.set_all(body)?;
        pick_indexes(all.len(), count.unwrap_or(1))
            .into_iter()
            .map(|i| all[i].clone())
            .collect()
    };
    cx.set_write(&a[1], &s, &[], &picked)?;
    Ok(match count {
        None => picked
            .into_iter()
            .next()
            .map_or_else(Frame::null, Frame::Bulk),
        Some(_) => members_frame(cx.resp3, picked),
    })
}

/// `SRANDMEMBER k [count]`.
fn srandmember<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    if a.len() > 3 {
        return Ok(reply::syntax());
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => Some(tri!(rand_count(c))),
    };
    let s = tri!(cx.set(&a[1])?);
    if let Some(b) = &s.body
        && let Some(p) = picks(cx, b, count.unwrap_or(1))?
    {
        return Ok(match count {
            None => p.into_iter().next().map_or_else(Frame::null, Frame::Bulk),
            Some(_) => Frame::Array(p.into_iter().map(Frame::Bulk).collect()),
        });
    }
    let all = match &s.body {
        None => Vec::new(),
        Some(b) => cx.set_all(b)?,
    };
    let Some(count) = count else {
        return Ok(match pick_indexes(all.len(), 1).first() {
            Some(&i) => Frame::Bulk(all[i].clone()),
            None => Frame::null(),
        });
    };
    Ok(Frame::Array(
        pick_indexes(all.len(), count)
            .into_iter()
            .map(|i| Frame::Bulk(all[i].clone()))
            .collect(),
    ))
}

/// Random members of a big split set (see [`random_elems`]); `None` when
/// the set is better read whole.
fn picks<B: Batch>(
    cx: &mut Cx<'_, B>,
    s: &SetBody,
    count: i64,
) -> Result<Option<Vec<Bytes>>, StoreError> {
    let SetBody::Split { uid, count: n } = s else {
        return Ok(None);
    };
    let Some(got) = random_elems(cx, *uid, *n, count)? else {
        return Ok(None);
    };
    got.into_iter()
        .map(|(k, _)| {
            Space::parse_field(&k)
                .map(|(_, m)| Some(Bytes::from(m)))
                .ok_or_else(|| corrupt("set member", k.len()))
        })
        .collect::<Result<Option<Vec<_>>, _>>()
}

/// `SMOVE src dst m`.
fn smove<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.set(&a[1])?);
    let Some(body) = s.body.clone() else {
        return Ok(Frame::Integer(0));
    };
    let d = tri!(cx.set(&a[2])?);
    if a[1] == a[2] {
        return Ok(Frame::Integer(i64::from(cx.set_has(&body, &a[3])?)));
    }
    if !cx.set_has(&body, &a[3])? {
        return Ok(Frame::Integer(0));
    }
    cx.set_write(&a[1], &s, &[], std::slice::from_ref(&a[3]))?;
    cx.set_write(&a[2], &d, std::slice::from_ref(&a[3]), &[])?;
    Ok(Frame::Integer(1))
}

/// A set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Inter,
    Union,
    Diff,
}

/// The members of each of `keys` (a missing key is empty); `Err` when one
/// holds another type.
fn sources<B: Batch>(
    cx: &mut Cx<'_, B>,
    keys: &[Bytes],
) -> Result<Arg<Vec<Vec<Bytes>>>, StoreError> {
    let mut sets = Vec::with_capacity(keys.len());
    for k in keys {
        let s = match cx.set(k)? {
            Ok(s) => s,
            Err(f) => return Ok(Err(f)),
        };
        sets.push(s.body);
    }
    let mut out = Vec::with_capacity(sets.len());
    for b in sets {
        out.push(match b {
            None => Vec::new(),
            Some(b) => cx.set_all(&b)?,
        });
    }
    Ok(Ok(out))
}

/// `op` over `sets`, in the first set's order.
fn combine(sets: &[Vec<Bytes>], op: Op) -> Vec<Bytes> {
    use std::collections::HashSet;
    match op {
        Op::Union => {
            let mut seen = HashSet::new();
            sets.iter()
                .flatten()
                .filter(|m| seen.insert((*m).clone()))
                .cloned()
                .collect()
        }
        Op::Inter => {
            let rest: Vec<HashSet<&Bytes>> = sets[1..].iter().map(|s| s.iter().collect()).collect();
            sets[0]
                .iter()
                .filter(|m| rest.iter().all(|r| r.contains(m)))
                .cloned()
                .collect()
        }
        Op::Diff => {
            let rest: HashSet<&Bytes> = sets[1..].iter().flatten().collect();
            sets[0]
                .iter()
                .filter(|m| !rest.contains(m))
                .cloned()
                .collect()
        }
    }
}

/// `SINTER`, `SUNION`, `SDIFF` over `keys`, and their `STORE` forms into
/// `dst`.
fn algebra<B: Batch>(cx: &mut Cx<'_, B>, keys: &[Bytes], op: Op, dst: Option<&Bytes>) -> Out {
    let sets = tri!(sources(cx, keys)?);
    let result = combine(&sets, op);
    match dst {
        None => Ok(members_frame(cx.resp3, result)),
        Some(d) => Ok(Frame::Integer(cx.set_store(d, &result)?)),
    }
}

/// `numkeys k... [LIMIT n]` at `a[1]`.
pub(crate) fn numkeys_limit(a: &[Bytes]) -> Arg<(&[Bytes], usize)> {
    let n = match parse_i64(&a[1]) {
        Some(n) if n > 0 => n as usize,
        _ => return Err(Frame::err("numkeys should be greater than 0")),
    };
    if n > a.len() - 2 {
        return Err(Frame::err(
            "Number of keys can't be greater than number of args",
        ));
    }
    let keys = &a[2..2 + n];
    let rest = &a[2 + n..];
    let mut limit = 0;
    let mut i = 0;
    while i < rest.len() {
        if super::upper(&rest[i]) == "LIMIT" && i + 1 < rest.len() {
            limit = match int(&rest[i + 1]) {
                Ok(l) if l >= 0 => l as usize,
                Ok(_) => return Err(Frame::err("LIMIT can't be negative")),
                Err(f) => return Err(f),
            };
            i += 2;
        } else {
            return Err(reply::syntax());
        }
    }
    Ok((keys, limit))
}

/// `SINTERCARD numkeys k... [LIMIT n]`.
fn sintercard<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (keys, limit) = tri!(numkeys_limit(a));
    let sets = tri!(sources(cx, keys)?);
    let n = combine(&sets, Op::Inter).len();
    Ok(Frame::Integer(
        if limit > 0 { n.min(limit) } else { n } as i64
    ))
}

/// `SSCAN k cursor [MATCH p] [COUNT n]`.
fn sscan<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let at = tri!(cursor(&a[2]));
    let o = tri!(scan_opts(a, 3, false, false));
    let s = tri!(cx.set(&a[1])?);
    let (items, next) = match &s.body {
        None => (Vec::new(), 0),
        Some(b @ SetBody::Inline { .. }) => (cx.set_all(b)?, 0),
        Some(b @ SetBody::Split { count, .. }) if *count as usize <= SCAN_WHOLE => {
            (cx.set_all(b)?, 0)
        }
        Some(SetBody::Split { uid, .. }) => {
            let (page, next) = elem_page(cx, *uid, at, o.count)?;
            let items = page
                .into_iter()
                .map(|(k, _)| {
                    Space::parse_field(&k)
                        .map(|(_, m)| Bytes::from(m))
                        .ok_or_else(|| corrupt("set member", k.len()))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            (items, next)
        }
    };
    let out = items
        .into_iter()
        .filter(|m| o.pattern.as_ref().is_none_or(|p| glob::matches(p, m)))
        .map(Frame::Bulk)
        .collect();
    Ok(Frame::Array(vec![
        Frame::Bulk(Bytes::from(next.to_string())),
        Frame::Array(out),
    ]))
}
