//! Lists: inline in the metadata while small (head first), else one
//! element entry per item at dense positions `head..tail`. Pushes and pops
//! at either end touch only their items; a change in the middle
//! (`LINSERT`, `LREM`) rewrites the list.

use std::collections::VecDeque;

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_i64;
use sprig_redis::reply;

use super::hashes::fits_inline;
use super::{Arg, Cx, Out, index_range, int, tri, upper};
use crate::layout::{Body, ListBody, Meta};
use crate::store::{Batch, StoreError};

/// Runs a list command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "lpush" => push(cx, a, true, false),
        "rpush" => push(cx, a, false, false),
        "lpushx" => push(cx, a, true, true),
        "rpushx" => push(cx, a, false, true),
        "lpop" => pop(cx, a, true),
        "rpop" => pop(cx, a, false),
        "llen" => llen(cx, a),
        "lrange" => lrange(cx, a),
        "lindex" => lindex(cx, a),
        "lset" => lset(cx, a),
        "linsert" => linsert(cx, a),
        "lrem" => lrem(cx, a),
        "ltrim" => ltrim(cx, a),
        "lpos" => lpos(cx, a),
        "lmove" => match (end_of(&a[3]), end_of(&a[4])) {
            (Some(f), Some(t)) => lmove(cx, &a[1], &a[2], f, t).map(|r| r.unwrap_or_else(|f| f)),
            _ => Ok(reply::syntax()),
        },
        "rpoplpush" => lmove(cx, &a[1], &a[2], false, true).map(|r| r.unwrap_or_else(|f| f)),
        "lmpop" => lmpop(cx, a, 1),
        _ => return None,
    })
}

/// `LEFT` (true) or `RIGHT` (false).
pub(crate) fn end_of(a: &[u8]) -> Option<bool> {
    match upper(a).as_str() {
        "LEFT" => Some(true),
        "RIGHT" => Some(false),
        _ => None,
    }
}

/// A key's list: its body and expiry, and the stored metadata.
pub(crate) struct List {
    pub body: Option<ListBody>,
    pub exp: Option<i64>,
    pub raw: Option<Meta>,
}

/// The number of items of `l`.
pub(crate) fn list_len(l: &ListBody) -> i64 {
    match l {
        ListBody::Inline(items) => items.len() as i64,
        ListBody::Split { head, tail, .. } => tail - head,
    }
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a list; `Err` answers `WRONGTYPE`.
    pub(crate) fn list(&mut self, key: &[u8]) -> Result<Arg<List>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok(List {
                body: None,
                exp: None,
                raw,
            }),
            Some(Meta {
                body: Body::List(l),
                exp,
            }) => Ok(List {
                body: Some(l),
                exp,
                raw,
            }),
            Some(_) => Err(reply::wrongtype()),
        })
    }

    /// Items `s..=e` (normalized indexes) of `l`.
    pub(crate) fn list_range(
        &mut self,
        l: &ListBody,
        s: i64,
        e: i64,
    ) -> Result<Vec<Bytes>, StoreError> {
        match l {
            ListBody::Inline(items) => Ok(items
                .iter()
                .skip(s as usize)
                .take((e - s + 1) as usize)
                .cloned()
                .collect()),
            ListBody::Split { uid, head, .. } => {
                let lo = self.space.item(*uid, head + s);
                let hi = self.space.item(*uid, head + e + 1);
                Ok(super::hashes::scan_all(self.b, &lo, &hi)?
                    .into_iter()
                    .map(|(_, v)| v)
                    .collect())
            }
        }
    }

    /// Every item of `l`.
    pub(crate) fn list_all(&mut self, l: &ListBody) -> Result<Vec<Bytes>, StoreError> {
        let n = list_len(l);
        if n == 0 {
            return Ok(Vec::new());
        }
        self.list_range(l, 0, n - 1)
    }

    /// Item `i` (normalized) of `l`.
    fn list_get(&mut self, l: &ListBody, i: i64) -> Result<Option<Bytes>, StoreError> {
        match l {
            ListBody::Inline(items) => Ok(items.get(i as usize).cloned()),
            ListBody::Split { uid, head, .. } => self.b.get(&self.space.item(*uid, head + i)),
        }
    }

    /// Stores list `body` at `key` (`None`: the key goes).
    fn list_store(
        &mut self,
        key: &[u8],
        l: &List,
        body: Option<ListBody>,
    ) -> Result<(), StoreError> {
        let new = body.map(|b| Meta {
            exp: l.exp,
            body: Body::List(b),
        });
        if new.is_some() || l.raw.is_some() {
            self.write(key, l.raw.as_ref(), new.as_ref())?;
        }
        Ok(())
    }

    /// Pushes `items` at the head (`left`) or tail of list `l` at `key`: the
    /// new length.
    pub(crate) fn list_push(
        &mut self,
        key: &[u8],
        l: &List,
        items: &[Bytes],
        left: bool,
    ) -> Result<i64, StoreError> {
        let body = match l.body.clone().unwrap_or(ListBody::Inline(VecDeque::new())) {
            ListBody::Inline(mut cur) => {
                for e in items {
                    if left {
                        cur.push_front(e.clone());
                    } else {
                        cur.push_back(e.clone());
                    }
                }
                if fits_inline(cur.len(), cur.iter()) {
                    ListBody::Inline(cur)
                } else {
                    // Too big to stay inline: the items become elements at
                    // positions 0..len.
                    let uid = self.take_uid()?;
                    for (i, e) in cur.iter().enumerate() {
                        self.b.put(self.space.item(uid, i as i64), e.clone())?;
                    }
                    ListBody::Split {
                        uid,
                        head: 0,
                        tail: cur.len() as i64,
                    }
                }
            }
            ListBody::Split {
                uid,
                mut head,
                mut tail,
            } => {
                for e in items {
                    if left {
                        head -= 1;
                        self.b.put(self.space.item(uid, head), e.clone())?;
                    } else {
                        self.b.put(self.space.item(uid, tail), e.clone())?;
                        tail += 1;
                    }
                }
                ListBody::Split { uid, head, tail }
            }
        };
        let n = list_len(&body);
        self.list_store(key, l, Some(body))?;
        Ok(n)
    }

    /// Pops up to `n` items from the head (`left`) or tail of list `l` at
    /// `key`, in pop order. The key goes when the list empties.
    pub(crate) fn list_pop(
        &mut self,
        key: &[u8],
        l: &List,
        n: usize,
        left: bool,
    ) -> Result<Vec<Bytes>, StoreError> {
        let Some(body) = l.body.clone() else {
            return Ok(Vec::new());
        };
        let (out, body) = match body {
            ListBody::Inline(mut cur) => {
                let mut out = Vec::new();
                while out.len() < n {
                    let Some(e) = (if left {
                        cur.pop_front()
                    } else {
                        cur.pop_back()
                    }) else {
                        break;
                    };
                    out.push(e);
                }
                let body = (!cur.is_empty()).then_some(ListBody::Inline(cur));
                (out, body)
            }
            ListBody::Split {
                uid,
                mut head,
                mut tail,
            } => {
                let (lo, hi) = (self.space.item(uid, head), self.space.item(uid, tail));
                let take = n.min((tail - head) as usize);
                let got = if left {
                    self.b.scan(&lo, &hi, take)?
                } else {
                    self.b.scan_rev(&lo, &hi, take)?
                };
                let mut out = Vec::with_capacity(got.len());
                for (k, v) in got {
                    self.b.delete(k)?;
                    out.push(v);
                }
                if left {
                    head += out.len() as i64;
                } else {
                    tail -= out.len() as i64;
                }
                let body = (head < tail).then_some(ListBody::Split { uid, head, tail });
                (out, body)
            }
        };
        if !out.is_empty() {
            self.list_store(key, l, body)?;
        }
        Ok(out)
    }

    /// Replaces list `l`'s items with `items` (a split list stays split,
    /// under the same uid).
    fn list_rewrite(&mut self, key: &[u8], l: &List, items: Vec<Bytes>) -> Result<(), StoreError> {
        let body = if items.is_empty() {
            None
        } else {
            match &l.body {
                Some(ListBody::Split { uid, .. }) => {
                    let uid = *uid;
                    self.drop_under(self.space, crate::layout::KIND_ELEM, uid)?;
                    for (i, e) in items.iter().enumerate() {
                        self.b.put(self.space.item(uid, i as i64), e.clone())?;
                    }
                    Some(ListBody::Split {
                        uid,
                        head: 0,
                        tail: items.len() as i64,
                    })
                }
                _ => Some(ListBody::Inline(items.into())),
            }
        };
        self.list_store(key, l, body)
    }
}

/// `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`.
fn push<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], left: bool, only_existing: bool) -> Out {
    let l = tri!(cx.list(&a[1])?);
    if only_existing && l.body.is_none() {
        return Ok(Frame::Integer(0));
    }
    Ok(Frame::Integer(cx.list_push(&a[1], &l, &a[2..], left)?))
}

/// `LPOP k [count]`, `RPOP k [count]`.
fn pop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], left: bool) -> Out {
    if a.len() > 3 {
        return Ok(reply::arity(if left { "lpop" } else { "rpop" }));
    }
    let count = match a.get(2) {
        None => None,
        Some(c) => match parse_i64(c) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return Ok(Frame::err("value is out of range, must be positive")),
        },
    };
    let l = tri!(cx.list(&a[1])?);
    if l.body.is_none() {
        return Ok(if count.is_some() {
            Frame::null_array()
        } else {
            Frame::null()
        });
    }
    let items = cx.list_pop(&a[1], &l, count.unwrap_or(1), left)?;
    Ok(match count {
        None => items
            .into_iter()
            .next()
            .map_or_else(Frame::null, Frame::Bulk),
        Some(_) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
    })
}

/// `LLEN k`.
fn llen<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let l = tri!(cx.list(&a[1])?);
    Ok(Frame::Integer(l.body.as_ref().map_or(0, list_len)))
}

/// `LRANGE k start stop`.
fn lrange<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (s, e) = (tri!(int(&a[2])), tri!(int(&a[3])));
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body else {
        return Ok(Frame::Array(Vec::new()));
    };
    let Some((s, e)) = index_range(list_len(&body), s, e) else {
        return Ok(Frame::Array(Vec::new()));
    };
    Ok(Frame::Array(
        cx.list_range(&body, s, e)?
            .into_iter()
            .map(Frame::Bulk)
            .collect(),
    ))
}

/// Index `i` (negative from the end) of a list of `n` items, if inside.
fn index(n: i64, i: i64) -> Option<i64> {
    let i = if i < 0 { i.checked_add(n)? } else { i };
    (0..n).contains(&i).then_some(i)
}

/// `LINDEX k i`.
fn lindex<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body else {
        return Ok(Frame::null());
    };
    let i = tri!(int(&a[2]));
    Ok(match index(list_len(&body), i) {
        None => Frame::null(),
        Some(i) => cx.list_get(&body, i)?.map_or_else(Frame::null, Frame::Bulk),
    })
}

/// `LSET k i v`.
fn lset<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body.clone() else {
        return Ok(reply::no_such_key());
    };
    let i = tri!(int(&a[2]));
    let Some(i) = index(list_len(&body), i) else {
        return Ok(Frame::err("index out of range"));
    };
    match body {
        ListBody::Inline(mut items) => {
            items[i as usize] = a[3].clone();
            if fits_inline(items.len(), items.iter()) {
                cx.list_store(&a[1], &l, Some(ListBody::Inline(items)))?;
            } else {
                let uid = cx.take_uid()?;
                for (j, e) in items.iter().enumerate() {
                    cx.b.put(cx.space.item(uid, j as i64), e.clone())?;
                }
                let body = ListBody::Split {
                    uid,
                    head: 0,
                    tail: items.len() as i64,
                };
                cx.list_store(&a[1], &l, Some(body))?;
            }
        }
        ListBody::Split { uid, head, .. } => {
            cx.b.put(cx.space.item(uid, head + i), a[3].clone())?;
            cx.touched.push(cx.space.meta(&a[1]));
        }
    }
    Ok(Frame::ok())
}

/// `LINSERT k BEFORE|AFTER pivot e`: the new length, -1 when the pivot
/// isn't there, 0 when the key isn't.
fn linsert<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let before = match upper(&a[2]).as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return Ok(reply::syntax()),
    };
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body.clone() else {
        return Ok(Frame::Integer(0));
    };
    let mut items = cx.list_all(&body)?;
    let Some(p) = items.iter().position(|e| *e == a[3]) else {
        return Ok(Frame::Integer(-1));
    };
    items.insert(if before { p } else { p + 1 }, a[4].clone());
    let n = items.len() as i64;
    if matches!(body, ListBody::Inline(_)) && !fits_inline(items.len(), items.iter()) {
        // Grown past the inline limits: a push of the whole list as new.
        let empty = List {
            body: None,
            exp: l.exp,
            raw: l.raw.clone(),
        };
        cx.list_push(&a[1], &empty, &items, false)?;
    } else {
        cx.list_rewrite(&a[1], &l, items)?;
    }
    Ok(Frame::Integer(n))
}

/// `LREM k count e`: the number removed.
fn lrem<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let count = tri!(int(&a[2]));
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body.clone() else {
        return Ok(Frame::Integer(0));
    };
    let items = cx.list_all(&body)?;
    let limit = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut drop = vec![false; items.len()];
    let mut n = 0;
    let order: Box<dyn Iterator<Item = usize>> = if count < 0 {
        Box::new((0..items.len()).rev())
    } else {
        Box::new(0..items.len())
    };
    for i in order {
        if n == limit {
            break;
        }
        if items[i] == a[3] {
            drop[i] = true;
            n += 1;
        }
    }
    if n > 0 {
        let kept = items
            .into_iter()
            .zip(drop)
            .filter_map(|(e, d)| (!d).then_some(e))
            .collect();
        cx.list_rewrite(&a[1], &l, kept)?;
    }
    Ok(Frame::Integer(n as i64))
}

/// `LTRIM k start stop`.
fn ltrim<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (s, e) = (tri!(int(&a[2])), tri!(int(&a[3])));
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body.clone() else {
        return Ok(Frame::ok());
    };
    let n = list_len(&body);
    let keep = index_range(n, s, e);
    let new = match (body, keep) {
        (_, None) => None,
        (ListBody::Inline(items), Some((s, e))) => Some(ListBody::Inline(
            items
                .into_iter()
                .skip(s as usize)
                .take((e - s + 1) as usize)
                .collect(),
        )),
        (ListBody::Split { uid, head, tail }, Some((s, e))) => {
            cx.delete_range(&cx.space.item(uid, head), &cx.space.item(uid, head + s))?;
            cx.delete_range(&cx.space.item(uid, head + e + 1), &cx.space.item(uid, tail))?;
            Some(ListBody::Split {
                uid,
                head: head + s,
                tail: head + e + 1,
            })
        }
    };
    if new.as_ref().map(list_len) != Some(n) {
        cx.list_store(&a[1], &l, new)?;
    }
    Ok(Frame::ok())
}

/// `LPOS k e [RANK r] [COUNT n] [MAXLEN m]`.
fn lpos<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (mut rank, mut count, mut maxlen) = (1i64, None, 0i64);
    let mut i = 3;
    while i < a.len() {
        let o = upper(&a[i]);
        let Some(v) = a.get(i + 1) else {
            return Ok(reply::syntax());
        };
        match o.as_str() {
            "RANK" => {
                rank = tri!(int(v));
                if rank == 0 {
                    return Ok(Frame::err(
                        "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                    ));
                }
                if rank == i64::MIN {
                    return Ok(Frame::err(
                        "value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                    ));
                }
            }
            "COUNT" => {
                let c = tri!(int(v));
                if c < 0 {
                    return Ok(Frame::err("COUNT can't be negative"));
                }
                count = Some(c);
            }
            "MAXLEN" => {
                maxlen = tri!(int(v));
                if maxlen < 0 {
                    return Ok(Frame::err("MAXLEN can't be negative"));
                }
            }
            _ => return Ok(reply::syntax()),
        }
        i += 2;
    }
    let l = tri!(cx.list(&a[1])?);
    let Some(body) = l.body else {
        return Ok(if count.is_some() {
            Frame::Array(Vec::new())
        } else {
            Frame::null()
        });
    };
    let n = list_len(&body);
    let scanned = if maxlen == 0 { n } else { maxlen.min(n) };
    // The items scanned, nearest end first.
    let items = if rank > 0 {
        if scanned == 0 {
            Vec::new()
        } else {
            cx.list_range(&body, 0, scanned - 1)?
        }
    } else if scanned == 0 {
        Vec::new()
    } else {
        let mut v = cx.list_range(&body, n - scanned, n - 1)?;
        v.reverse();
        v
    };
    let want = match count {
        None => 1,
        Some(0) => usize::MAX,
        Some(c) => c as usize,
    };
    let mut skip = rank.unsigned_abs() - 1;
    let mut found = Vec::new();
    for (j, e) in items.iter().enumerate() {
        if *e != a[2] {
            continue;
        }
        if skip > 0 {
            skip -= 1;
            continue;
        }
        let idx = if rank > 0 { j as i64 } else { n - 1 - j as i64 };
        found.push(Frame::Integer(idx));
        if found.len() >= want {
            break;
        }
    }
    Ok(match count {
        Some(_) => Frame::Array(found),
        None => found.into_iter().next().unwrap_or_else(Frame::null),
    })
}

/// `LMOVE src dst from to` (`RPOPLPUSH` is right, left): the item moved, or
/// nil when the source is empty; `Err` for `WRONGTYPE`.
pub(crate) fn lmove<B: Batch>(
    cx: &mut Cx<'_, B>,
    src: &Bytes,
    dst: &Bytes,
    from_left: bool,
    to_left: bool,
) -> Result<Arg<Frame>, StoreError> {
    let l = match cx.list(src)? {
        Ok(l) => l,
        Err(f) => return Ok(Err(f)),
    };
    if l.body.is_none() {
        return Ok(Ok(Frame::null()));
    }
    let d = match cx.list(dst)? {
        Ok(d) => d,
        Err(f) => return Ok(Err(f)),
    };
    let got = cx.list_pop(src, &l, 1, from_left)?;
    let Some(item) = got.into_iter().next() else {
        return Ok(Ok(Frame::null()));
    };
    // The destination as it is after the pop (the same key rotates).
    let d = if src == dst {
        match cx.list(dst)? {
            Ok(d) => d,
            Err(f) => return Ok(Err(f)),
        }
    } else {
        d
    };
    cx.list_push(dst, &d, std::slice::from_ref(&item), to_left)?;
    Ok(Ok(Frame::Bulk(item)))
}

/// The keys, end and count of `numkeys k... LEFT|RIGHT [COUNT n]` at
/// `a[at]`.
pub(crate) fn mpop_args(a: &[Bytes], at: usize) -> Arg<(Vec<Bytes>, bool, usize)> {
    let n = match parse_i64(&a[at]) {
        Some(n) if n > 0 => n as usize,
        _ => return Err(Frame::err("numkeys should be greater than 0")),
    };
    if n >= a.len() - at {
        return Err(reply::syntax());
    }
    let keys = a[at + 1..at + 1 + n].to_vec();
    let rest = &a[at + 1 + n..];
    let end = rest
        .first()
        .and_then(|e| end_of(e))
        .ok_or_else(reply::syntax)?;
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
    Ok((keys, end, count))
}

/// A key and the items popped from it.
pub(crate) type Popped = (Bytes, Vec<Bytes>);

/// Pops from the first non-empty list of `keys`: `[key, [items]]`, or
/// `None` when all are empty.
pub(crate) fn try_mpop<B: Batch>(
    cx: &mut Cx<'_, B>,
    keys: &[Bytes],
    left: bool,
    count: usize,
) -> Result<Arg<Option<Popped>>, StoreError> {
    for k in keys {
        let l = match cx.list(k)? {
            Ok(l) => l,
            Err(f) => return Ok(Err(f)),
        };
        if l.body.is_some() {
            let items = cx.list_pop(k, &l, count, left)?;
            return Ok(Ok(Some((k.clone(), items))));
        }
    }
    Ok(Ok(None))
}

/// `LMPOP numkeys k... LEFT|RIGHT [COUNT n]` (`at` the index of numkeys).
fn lmpop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], at: usize) -> Out {
    let (keys, left, count) = tri!(mpop_args(a, at));
    Ok(match tri!(try_mpop(cx, &keys, left, count)?) {
        None => Frame::null_array(),
        Some((k, items)) => Frame::Array(vec![
            Frame::Bulk(k),
            Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
        ]),
    })
}
