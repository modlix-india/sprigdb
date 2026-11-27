//! Streams: `XADD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XDEL`, `XTRIM`,
//! `XSETID`, `XREAD`, `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`,
//! `XAUTOCLAIM` and `XINFO`.
//!
//! A stream is always split: its metadata entry holds the counters
//! ([`StreamMeta`]); everything else lives under its uid, one tag byte
//! first:
//!
//! ```text
//! entry       E|0|uid|'e'|ms u64|seq u64                -> ordinal varint | n varint | (field, value)*
//! group       E|0|uid|'g'|str(group)                    -> last id | entries read i64 | pending u64 | consumers u64
//! consumer    E|0|uid|'c'|str(group)|str(consumer)      -> seen i64 | active i64 | pending u64
//! pending     E|0|uid|'p'|str(group)|ms|seq             -> delivered i64 | count u64 | consumer
//! by consumer E|0|uid|'q'|str(group)|str(consumer)|ms|seq -> (empty)
//! ```
//!
//! An entry's ordinal is the stream's `entries-added` when it was added:
//! approximate trimming (`~`) drops whole "nodes" of 100 ordinals, as
//! Redis drops whole nodes of its radix tree. Groups and consumers come in
//! byte order, as Redis's radix trees give them.

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_i64;
use sprig_redis::reply;

use super::{Arg, Cx, Out, tri, upper};
use crate::layout::{self, Body, Meta, Space, StreamId, StreamMeta, get_str, put_str};
use crate::store::{Batch, StoreError};

const TAG_CONSUMER: u8 = b'c';
const TAG_ENTRY: u8 = b'e';
const TAG_GROUP: u8 = b'g';
const TAG_PENDING: u8 = b'p';
const TAG_BYCONS: u8 = b'q';

/// Entries per modelled node (Redis's `stream-node-max-entries`).
pub const NODE_ENTRIES: u64 = 100;

/// Entries per scan page.
const PAGE: usize = 512;

/// The commands of this module.
#[must_use]
pub fn is_stream(name: &str) -> bool {
    matches!(
        name,
        "xadd"
            | "xrange"
            | "xrevrange"
            | "xlen"
            | "xdel"
            | "xtrim"
            | "xsetid"
            | "xread"
            | "xreadgroup"
            | "xgroup"
            | "xack"
            | "xpending"
            | "xclaim"
            | "xautoclaim"
            | "xinfo"
    )
}

/// Runs a stream command, if `name` is one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    if !is_stream(name) {
        return None;
    }
    Some(match name {
        "xadd" => xadd(cx, a),
        "xrange" => xrange(cx, a, false),
        "xrevrange" => xrange(cx, a, true),
        "xlen" => xlen(cx, a),
        "xdel" => xdel(cx, a),
        "xtrim" => xtrim(cx, a),
        "xsetid" => xsetid(cx, a),
        "xread" => xread(cx, a, false),
        "xreadgroup" => xread(cx, a, true),
        "xgroup" => xgroup(cx, a),
        "xack" => xack(cx, a),
        "xpending" => xpending(cx, a),
        "xclaim" => xclaim(cx, a),
        "xautoclaim" => xautoclaim(cx, a),
        _ => xinfo(cx, a),
    })
}

// ---------------------------------------------------------------------------
// Errors Redis answers.

fn invalid_id() -> Frame {
    Frame::err("Invalid stream ID specified as stream command argument")
}

fn nogroup(key: &[u8], group: &[u8]) -> Frame {
    Frame::error(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
}

fn nogroup_for_key(key: &[u8], group: &[u8]) -> Frame {
    Frame::error(format!(
        "NOGROUP No such consumer group '{}' for key name '{}'",
        String::from_utf8_lossy(group),
        String::from_utf8_lossy(key)
    ))
}

fn key_must_exist() -> Frame {
    Frame::err(
        "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
    )
}

// ---------------------------------------------------------------------------
// IDs.

/// A decimal u64 as Redis reads an ID's parts: leading white space and a
/// `+` allowed, then digits to the end (leading zeros allowed).
fn parse_u64(s: &[u8]) -> Option<u64> {
    let mut s = s;
    while let Some((c, rest)) = s.split_first()
        && matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
    {
        s = rest;
    }
    let s = s.strip_prefix(b"+").unwrap_or(s);
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut v: u64 = 0;
    for c in s {
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(v)
}

/// `ms` or `ms-seq`; a missing `seq` is `missing_seq`.
#[must_use]
pub fn parse_id(s: &[u8], missing_seq: u64) -> Option<StreamId> {
    match s.iter().position(|&c| c == b'-') {
        None => Some(StreamId {
            ms: parse_u64(s)?,
            seq: missing_seq,
        }),
        Some(i) => Some(StreamId {
            ms: parse_u64(&s[..i])?,
            seq: parse_u64(&s[i + 1..])?,
        }),
    }
}

/// An ID argument where `-` and `+` mean the ends.
fn parse_id_or_ends(s: &[u8], missing_seq: u64) -> Option<StreamId> {
    match s {
        b"-" => Some(StreamId::MIN),
        b"+" => Some(StreamId::MAX),
        _ => parse_id(s, missing_seq),
    }
}

/// A range bound: `-`, `+`, an ID (incomplete: `missing_seq`), or `(` and
/// an ID for an exclusive one. `Ok(None)`: the range is empty.
fn range_bound(s: &[u8], start: bool) -> Arg<Option<StreamId>> {
    let missing = if start { 0 } else { u64::MAX };
    if let Some(rest) = s.strip_prefix(b"(") {
        if rest == b"-" || rest == b"+" {
            return Err(invalid_id());
        }
        let id = parse_id(rest, missing).ok_or_else(invalid_id)?;
        return if start {
            id.next()
                .map(Some)
                .ok_or_else(|| Frame::err("invalid start ID for the interval"))
        } else {
            id.prev()
                .map(Some)
                .ok_or_else(|| Frame::err("invalid end ID for the interval"))
        };
    }
    parse_id_or_ends(s, missing)
        .map(Some)
        .ok_or_else(invalid_id)
}

fn id_frame(id: StreamId) -> Frame {
    Frame::Bulk(Bytes::from(id.to_string()))
}

// ---------------------------------------------------------------------------
// Keys under the uid.

fn sub(sp: Space, uid: u64, tag: u8) -> BytesMut {
    let e = sp.elems(uid);
    let mut b = BytesMut::with_capacity(e.len() + 48);
    b.put_slice(&e);
    b.put_u8(tag);
    b
}

fn entry_key(sp: Space, uid: u64, id: StreamId) -> Bytes {
    let mut b = sub(sp, uid, TAG_ENTRY);
    b.put_slice(&id.bytes());
    b.freeze()
}

/// The entry keys from `from` (inclusive) to `to` (inclusive).
fn entry_range(sp: Space, uid: u64, from: StreamId, to: StreamId) -> (Vec<u8>, Vec<u8>) {
    let lo = entry_key(sp, uid, from).to_vec();
    let hi = match to.next() {
        Some(n) => entry_key(sp, uid, n).to_vec(),
        None => sub(sp, uid, TAG_ENTRY + 1).to_vec(),
    };
    (lo, hi)
}

fn id_of_key(k: &[u8]) -> Option<StreamId> {
    StreamId::of_bytes(k.get(k.len().checked_sub(16)?..)?)
}

fn group_key(sp: Space, uid: u64, g: &[u8]) -> Bytes {
    let mut b = sub(sp, uid, TAG_GROUP);
    put_str(&mut b, g);
    b.freeze()
}

fn groups_range(sp: Space, uid: u64) -> (Vec<u8>, Vec<u8>) {
    (
        sub(sp, uid, TAG_GROUP).to_vec(),
        sub(sp, uid, TAG_GROUP + 1).to_vec(),
    )
}

fn group_name_of(sp: Space, uid: u64, k: &[u8]) -> Option<Bytes> {
    let p = sub(sp, uid, TAG_GROUP);
    let (g, rest) = get_str(k.strip_prefix(&p[..])?)?;
    rest.is_empty().then(|| Bytes::from(g))
}

fn with_group(sp: Space, uid: u64, tag: u8, g: &[u8]) -> BytesMut {
    let mut b = sub(sp, uid, tag);
    put_str(&mut b, g);
    b
}

fn consumer_key(sp: Space, uid: u64, g: &[u8], c: &[u8]) -> Bytes {
    let mut b = with_group(sp, uid, TAG_CONSUMER, g);
    put_str(&mut b, c);
    b.freeze()
}

/// Every key starting with `p`: `[p, after(p))`.
fn prefixed(p: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (p.to_vec(), layout::after(p))
}

fn pending_key(sp: Space, uid: u64, g: &[u8], id: StreamId) -> Bytes {
    let mut b = with_group(sp, uid, TAG_PENDING, g);
    b.put_slice(&id.bytes());
    b.freeze()
}

fn bycons_key(sp: Space, uid: u64, g: &[u8], c: &[u8], id: StreamId) -> Bytes {
    let mut b = with_group(sp, uid, TAG_BYCONS, g);
    put_str(&mut b, c);
    b.put_slice(&id.bytes());
    b.freeze()
}

/// Group `g`'s pending entries from `from` to `to`, inclusive.
fn pending_range(
    sp: Space,
    uid: u64,
    g: &[u8],
    from: StreamId,
    to: StreamId,
) -> (Vec<u8>, Vec<u8>) {
    let lo = pending_key(sp, uid, g, from).to_vec();
    let hi = match to.next() {
        Some(n) => pending_key(sp, uid, g, n).to_vec(),
        None => layout::after(&with_group(sp, uid, TAG_PENDING, g)),
    };
    (lo, hi)
}

/// Consumer `c`'s pending entries of group `g`, from `from` to `to`.
fn bycons_range(
    sp: Space,
    uid: u64,
    g: &[u8],
    c: &[u8],
    from: StreamId,
    to: StreamId,
) -> (Vec<u8>, Vec<u8>) {
    let lo = bycons_key(sp, uid, g, c, from).to_vec();
    let hi = if let Some(n) = to.next() {
        bycons_key(sp, uid, g, c, n).to_vec()
    } else {
        let mut p = with_group(sp, uid, TAG_BYCONS, g);
        put_str(&mut p, c);
        layout::after(&p)
    };
    (lo, hi)
}

// ---------------------------------------------------------------------------
// Values.

fn put_varint(b: &mut BytesMut, mut v: u64) {
    while v >= 0x80 {
        b.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    b.put_u8(v as u8);
}

fn get_varint(b: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let x = *b.get(*at)?;
        *at += 1;
        v |= u64::from(x & 0x7f) << shift;
        if x < 0x80 {
            return Some(v);
        }
    }
    None
}

fn entry_value(ordinal: u64, fields: &[Bytes]) -> Bytes {
    let mut b = BytesMut::with_capacity(8 + fields.iter().map(|f| f.len() + 2).sum::<usize>());
    put_varint(&mut b, ordinal);
    put_varint(&mut b, fields.len() as u64);
    for f in fields {
        put_varint(&mut b, f.len() as u64);
        b.put_slice(f);
    }
    b.freeze()
}

/// An entry's ordinal and fields (and values, alternating).
fn parse_entry(v: &Bytes) -> Option<(u64, Vec<Bytes>)> {
    let mut at = 0;
    let ord = get_varint(v, &mut at)?;
    let n = usize::try_from(get_varint(v, &mut at)?).ok()?;
    let mut out = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let l = usize::try_from(get_varint(v, &mut at)?).ok()?;
        let end = at.checked_add(l)?;
        if end > v.len() {
            return None;
        }
        out.push(v.slice(at..end));
        at = end;
    }
    (at == v.len()).then_some((ord, out))
}

fn entry_ordinal(v: &[u8]) -> Option<u64> {
    let mut at = 0;
    get_varint(v, &mut at)
}

/// A consumer group's record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Group {
    last: StreamId,
    /// Entries read, or −1 when unknown.
    read: i64,
    pending: u64,
    consumers: u64,
}

impl Group {
    fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(40);
        b.put_slice(&self.last.bytes());
        b.put_i64(self.read);
        b.put_u64(self.pending);
        b.put_u64(self.consumers);
        b.freeze()
    }

    fn decode(v: &[u8]) -> Option<Self> {
        Some(Self {
            last: StreamId::of_bytes(v.get(..16)?)?,
            read: i64::from_be_bytes(v.get(16..24)?.try_into().ok()?),
            pending: u64::from_be_bytes(v.get(24..32)?.try_into().ok()?),
            consumers: u64::from_be_bytes(v.get(32..40)?.try_into().ok()?),
        })
    }
}

/// A consumer's record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Consumer {
    seen: i64,
    /// −1: never active.
    active: i64,
    pending: u64,
}

impl Consumer {
    fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(24);
        b.put_i64(self.seen);
        b.put_i64(self.active);
        b.put_u64(self.pending);
        b.freeze()
    }

    fn decode(v: &[u8]) -> Option<Self> {
        Some(Self {
            seen: i64::from_be_bytes(v.get(..8)?.try_into().ok()?),
            active: i64::from_be_bytes(v.get(8..16)?.try_into().ok()?),
            pending: u64::from_be_bytes(v.get(16..24)?.try_into().ok()?),
        })
    }
}

/// A pending entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    delivered: i64,
    count: u64,
    consumer: Bytes,
}

impl Pending {
    fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(16 + self.consumer.len());
        b.put_i64(self.delivered);
        b.put_u64(self.count);
        b.put_slice(&self.consumer);
        b.freeze()
    }

    fn decode(v: &Bytes) -> Option<Self> {
        Some(Self {
            delivered: i64::from_be_bytes(v.get(..8)?.try_into().ok()?),
            count: u64::from_be_bytes(v.get(8..16)?.try_into().ok()?),
            consumer: v.slice(16..),
        })
    }
}

fn bad(what: &str, len: usize) -> StoreError {
    super::corrupt(what, len)
}

// ---------------------------------------------------------------------------
// The stream and its groups, in a command.

/// A stream key as loaded: the live metadata (with its stream body), and
/// as stored.
struct Loaded {
    raw: Option<Meta>,
    live: Option<Meta>,
}

impl Loaded {
    fn stream(&self) -> Option<StreamMeta> {
        match &self.live {
            Some(Meta {
                body: Body::Stream(s),
                ..
            }) => Some(*s),
            _ => None,
        }
    }
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a stream: `WRONGTYPE` when it holds another type.
    fn load_stream(&mut self, key: &[u8]) -> Result<Arg<Loaded>, StoreError> {
        let (live, raw) = self.load(key)?;
        if live
            .as_ref()
            .is_some_and(|m| !matches!(m.body, Body::Stream(_)))
        {
            return Ok(Err(reply::wrongtype()));
        }
        Ok(Ok(Loaded { raw, live }))
    }

    /// Stores stream `s` at `key` (whose stored metadata was `l`).
    fn save_stream(&mut self, key: &[u8], l: &Loaded, s: StreamMeta) -> Result<(), StoreError> {
        let exp = l.live.as_ref().and_then(|m| m.exp);
        let new = Meta {
            exp,
            body: Body::Stream(s),
        };
        if l.live.as_ref() == Some(&new) {
            self.touched.push(self.space.meta(key));
            return Ok(());
        }
        self.write(key, l.raw.as_ref(), Some(&new))
    }

    /// Marks `key` as written without changing its metadata.
    fn mark(&mut self, key: &[u8]) {
        self.touched.push(self.space.meta(key));
    }

    /// Records event `name` on `key` for keyspace notifications.
    fn event(&mut self, key: &[u8], name: &'static str) {
        self.events
            .push((self.space, Bytes::copy_from_slice(key), name));
    }

    fn entry(&mut self, uid: u64, id: StreamId) -> Result<Option<Vec<Bytes>>, StoreError> {
        let k = entry_key(self.space, uid, id);
        match self.b.get(&k)? {
            None => Ok(None),
            Some(v) => parse_entry(&v)
                .map(|(_, f)| Some(f))
                .ok_or_else(|| bad("stream entry", v.len())),
        }
    }

    /// Up to `limit` entries from `from` to `to` (inclusive), last first
    /// when `rev`.
    fn entries(
        &mut self,
        uid: u64,
        from: StreamId,
        to: StreamId,
        limit: usize,
        rev: bool,
    ) -> Result<Vec<(StreamId, u64, Vec<Bytes>)>, StoreError> {
        let mut out = Vec::new();
        if from > to || limit == 0 {
            return Ok(out);
        }
        let (mut lo, mut hi) = entry_range(self.space, uid, from, to);
        loop {
            let want = (limit - out.len()).min(PAGE);
            let got = if rev {
                self.b.scan_rev(&lo, &hi, want)?
            } else {
                self.b.scan(&lo, &hi, want)?
            };
            let n = got.len();
            for (k, v) in got {
                let id = id_of_key(&k).ok_or_else(|| bad("stream entry key", k.len()))?;
                let (ord, f) = parse_entry(&v).ok_or_else(|| bad("stream entry", v.len()))?;
                if rev {
                    hi = k.to_vec();
                } else {
                    lo = layout::after(&k);
                }
                out.push((id, ord, f));
            }
            if n < want || out.len() >= limit {
                return Ok(out);
            }
        }
    }

    fn first_entry(&mut self, uid: u64) -> Result<Option<(StreamId, u64)>, StoreError> {
        Ok(self
            .entries(uid, StreamId::MIN, StreamId::MAX, 1, false)?
            .into_iter()
            .next()
            .map(|(id, o, _)| (id, o)))
    }

    fn last_entry(&mut self, uid: u64) -> Result<Option<(StreamId, u64)>, StoreError> {
        Ok(self
            .entries(uid, StreamId::MIN, StreamId::MAX, 1, true)?
            .into_iter()
            .next()
            .map(|(id, o, _)| (id, o)))
    }

    fn group(&mut self, uid: u64, g: &[u8]) -> Result<Option<Group>, StoreError> {
        match self.b.get(&group_key(self.space, uid, g))? {
            None => Ok(None),
            Some(v) => Group::decode(&v)
                .map(Some)
                .ok_or_else(|| bad("stream group", v.len())),
        }
    }

    fn put_group(&mut self, uid: u64, g: &[u8], r: &Group) -> Result<(), StoreError> {
        self.b.put(group_key(self.space, uid, g), r.encode())
    }

    fn consumer(&mut self, uid: u64, g: &[u8], c: &[u8]) -> Result<Option<Consumer>, StoreError> {
        match self.b.get(&consumer_key(self.space, uid, g, c))? {
            None => Ok(None),
            Some(v) => Consumer::decode(&v)
                .map(Some)
                .ok_or_else(|| bad("stream consumer", v.len())),
        }
    }

    fn put_consumer(
        &mut self,
        uid: u64,
        g: &[u8],
        c: &[u8],
        r: &Consumer,
    ) -> Result<(), StoreError> {
        self.b.put(consumer_key(self.space, uid, g, c), r.encode())
    }

    fn pending(&mut self, uid: u64, g: &[u8], id: StreamId) -> Result<Option<Pending>, StoreError> {
        match self.b.get(&pending_key(self.space, uid, g, id))? {
            None => Ok(None),
            Some(v) => Pending::decode(&v)
                .map(Some)
                .ok_or_else(|| bad("stream pending entry", v.len())),
        }
    }

    /// Every group: name and record, in name order.
    fn groups(&mut self, uid: u64) -> Result<Vec<(Bytes, Group)>, StoreError> {
        let (mut lo, hi) = groups_range(self.space, uid);
        let mut out = Vec::new();
        loop {
            let got = self.b.scan(&lo, &hi, PAGE)?;
            let n = got.len();
            for (k, v) in got {
                let name = group_name_of(self.space, uid, &k)
                    .ok_or_else(|| bad("stream group key", k.len()))?;
                let r = Group::decode(&v).ok_or_else(|| bad("stream group", v.len()))?;
                lo = layout::after(&k);
                out.push((name, r));
            }
            if n < PAGE {
                return Ok(out);
            }
        }
    }

    /// Group `g`'s consumers: name and record, in name order.
    fn consumers(&mut self, uid: u64, g: &[u8]) -> Result<Vec<(Bytes, Consumer)>, StoreError> {
        let p = with_group(self.space, uid, TAG_CONSUMER, g);
        let (mut lo, hi) = prefixed(&p);
        let mut out = Vec::new();
        loop {
            let got = self.b.scan(&lo, &hi, PAGE)?;
            let n = got.len();
            for (k, v) in got {
                let (c, rest) = k
                    .strip_prefix(&p[..])
                    .and_then(get_str)
                    .ok_or_else(|| bad("stream consumer key", k.len()))?;
                if !rest.is_empty() {
                    return Err(bad("stream consumer key", k.len()));
                }
                let r = Consumer::decode(&v).ok_or_else(|| bad("stream consumer", v.len()))?;
                lo = layout::after(&k);
                out.push((Bytes::from(c), r));
            }
            if n < PAGE {
                return Ok(out);
            }
        }
    }

    /// Up to `limit` pending entries of group `g` in `[lo, hi)` keys.
    fn pending_scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
        rev: bool,
    ) -> Result<Vec<(StreamId, Pending)>, StoreError> {
        let (mut lo, mut hi) = (lo.to_vec(), hi.to_vec());
        let mut out = Vec::new();
        while out.len() < limit {
            let want = (limit - out.len()).min(PAGE);
            let got = if rev {
                self.b.scan_rev(&lo, &hi, want)?
            } else {
                self.b.scan(&lo, &hi, want)?
            };
            let n = got.len();
            for (k, v) in got {
                let id = id_of_key(&k).ok_or_else(|| bad("stream pending key", k.len()))?;
                let p = Pending::decode(&v).ok_or_else(|| bad("stream pending entry", v.len()))?;
                if rev {
                    hi = k.to_vec();
                } else {
                    lo = layout::after(&k);
                }
                out.push((id, p));
            }
            if n < want {
                break;
            }
        }
        Ok(out)
    }

    /// Up to `limit` IDs consumer `c` of group `g` has pending, from
    /// `from` to `to`.
    fn bycons_ids(
        &mut self,
        uid: u64,
        g: &[u8],
        c: &[u8],
        from: StreamId,
        to: StreamId,
        limit: usize,
    ) -> Result<Vec<StreamId>, StoreError> {
        let mut out = Vec::new();
        if from > to {
            return Ok(out);
        }
        let (mut lo, hi) = bycons_range(self.space, uid, g, c, from, to);
        while out.len() < limit {
            let want = (limit - out.len()).min(PAGE);
            let got = self.b.scan(&lo, &hi, want)?;
            let n = got.len();
            for (k, _) in got {
                out.push(id_of_key(&k).ok_or_else(|| bad("stream pending key", k.len()))?);
                lo = layout::after(&k);
            }
            if n < want {
                break;
            }
        }
        Ok(out)
    }
}

/// A group and the consumers a command changed, written back at its end.
struct GroupEdit {
    uid: u64,
    name: Bytes,
    rec: Group,
    cons: BTreeMap<Bytes, Consumer>,
}

impl GroupEdit {
    fn new(uid: u64, name: &Bytes, rec: Group) -> Self {
        Self {
            uid,
            name: name.clone(),
            rec,
            cons: BTreeMap::new(),
        }
    }

    /// Consumer `c`, loaded (or `None`).
    fn get<B: Batch>(
        &mut self,
        cx: &mut Cx<'_, B>,
        c: &Bytes,
    ) -> Result<Option<Consumer>, StoreError> {
        if let Some(r) = self.cons.get(c) {
            return Ok(Some(*r));
        }
        let r = cx.consumer(self.uid, &self.name, c)?;
        if let Some(r) = r {
            self.cons.insert(c.clone(), r);
        }
        Ok(r)
    }

    /// Consumer `c`, created when missing (and the event recorded).
    fn ensure<B: Batch>(
        &mut self,
        cx: &mut Cx<'_, B>,
        key: &[u8],
        c: &Bytes,
    ) -> Result<Consumer, StoreError> {
        if let Some(r) = self.get(cx, c)? {
            return Ok(r);
        }
        let r = Consumer {
            seen: cx.now,
            active: -1,
            pending: 0,
        };
        self.cons.insert(c.clone(), r);
        self.rec.consumers += 1;
        cx.event(key, "xgroup-createconsumer");
        Ok(r)
    }

    fn set(&mut self, c: &Bytes, r: Consumer) {
        self.cons.insert(c.clone(), r);
    }

    /// Adds a pending entry for `id`, owned by `c` (whose record must be
    /// loaded); an existing one of another consumer moves.
    fn add_pending<B: Batch>(
        &mut self,
        cx: &mut Cx<'_, B>,
        id: StreamId,
        p: &Pending,
        old: Option<&Pending>,
    ) -> Result<(), StoreError> {
        let sp = cx.space;
        if let Some(o) = old {
            if o.consumer != p.consumer {
                cx.b.delete(bycons_key(sp, self.uid, &self.name, &o.consumer, id))?;
                if let Some(mut r) = self.get(cx, &o.consumer)? {
                    r.pending = r.pending.saturating_sub(1);
                    self.set(&o.consumer, r);
                }
            }
        } else {
            self.rec.pending += 1;
        }
        if old.is_none_or(|o| o.consumer != p.consumer) {
            cx.b.put(
                bycons_key(sp, self.uid, &self.name, &p.consumer, id),
                Bytes::new(),
            )?;
            if let Some(mut r) = self.get(cx, &p.consumer)? {
                r.pending += 1;
                self.set(&p.consumer, r);
            }
        }
        cx.b.put(pending_key(sp, self.uid, &self.name, id), p.encode())
    }

    /// Removes pending entry `p` of `id`.
    fn remove_pending<B: Batch>(
        &mut self,
        cx: &mut Cx<'_, B>,
        id: StreamId,
        p: &Pending,
    ) -> Result<(), StoreError> {
        let sp = cx.space;
        cx.b.delete(pending_key(sp, self.uid, &self.name, id))?;
        cx.b.delete(bycons_key(sp, self.uid, &self.name, &p.consumer, id))?;
        self.rec.pending = self.rec.pending.saturating_sub(1);
        if let Some(mut r) = self.get(cx, &p.consumer)? {
            r.pending = r.pending.saturating_sub(1);
            self.set(&p.consumer, r);
        }
        Ok(())
    }

    fn save<B: Batch>(self, cx: &mut Cx<'_, B>) -> Result<(), StoreError> {
        cx.put_group(self.uid, &self.name, &self.rec)?;
        for (c, r) in &self.cons {
            cx.put_consumer(self.uid, &self.name, c, r)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Lag and the entries-read counter (Redis's documented consumer group lag).

/// Whether a deleted entry may lie between `from` (or the first entry) and
/// `to` (or the last ID).
fn has_tombstones(s: &StreamMeta, from: Option<StreamId>, to: Option<StreamId>) -> bool {
    if s.length == 0 || s.max_deleted == StreamId::MIN {
        return false;
    }
    let from = from.unwrap_or(StreamId::MIN);
    let to = to.unwrap_or(StreamId::MAX);
    from <= s.max_deleted && s.max_deleted <= to
}

/// The logical number of entries up to and including `id` ever added, when
/// it can be told; −1 when not.
fn estimate_read(s: &StreamMeta, id: StreamId) -> i64 {
    let added = s.added as i64;
    if s.added == 0 {
        return 0;
    }
    if s.length == 0 && id < s.last {
        return added;
    }
    if id == s.last {
        return added;
    }
    if id > s.last {
        return -1;
    }
    if s.max_deleted == StreamId::MIN || s.max_deleted < s.first {
        if id < s.first {
            return added - s.length as i64;
        }
        if id == s.first {
            return added - s.length as i64 + 1;
        }
    }
    -1
}

/// Whether a group's entries-read counter can be trusted: known, not past
/// the entries added, its last ID not before the first entry, and no
/// deleted entry after its last ID.
fn trusted(s: &StreamMeta, g: &Group) -> bool {
    g.read >= 0
        && g.read as u64 <= s.added
        && !(s.length > 0 && g.last < s.first)
        && !has_tombstones(s, Some(g.last), None)
}

/// A group's lag, when it can be told.
fn lag(s: &StreamMeta, g: &Group) -> Option<i64> {
    if s.added == 0 || s.length == 0 {
        return Some(0);
    }
    if trusted(s, g) {
        return Some(s.added as i64 - g.read);
    }
    let e = estimate_read(s, g.last);
    (e >= 0).then(|| s.added as i64 - e)
}

/// The group's entries-read counter once it is delivered `id` (called
/// before its last ID moves to `id`).
fn advance_read(s: &StreamMeta, g: &mut Group, id: StreamId) {
    if trusted(s, g) {
        g.read += 1;
    } else if s.added > 0 {
        g.read = estimate_read(s, id);
    }
}

fn opt_int(v: Option<i64>) -> Frame {
    v.map_or_else(Frame::null, Frame::Integer)
}

fn read_frame(read: i64) -> Frame {
    opt_int((read >= 0).then_some(read))
}

// ---------------------------------------------------------------------------
// Trimming.

#[derive(Debug, Clone, Copy)]
enum Strategy {
    MaxLen(u64),
    MinId(StreamId),
}

#[derive(Debug, Clone, Copy)]
struct Trim {
    strategy: Strategy,
    approx: bool,
    /// Entries one command may drop (0: no limit).
    limit: u64,
}

/// The options of `XADD` (from `a[2]`) or `XTRIM` (from `a[2]`): the trim,
/// `NOMKSTREAM`, and where the ID (for `XADD`) is.
struct AddArgs {
    trim: Option<Trim>,
    nomkstream: bool,
    at: usize,
}

fn trim_args(a: &[Bytes], xadd: bool) -> Arg<AddArgs> {
    let mut i = 2;
    let mut maxlen: Option<u64> = None;
    let mut minid: Option<StreamId> = None;
    let mut approx = false;
    let mut limit: Option<i64> = None;
    let mut nomk = false;
    while i < a.len() {
        let more = a.len() - 1 - i;
        let o = upper(&a[i]);
        if xadd && a[i].as_ref() == b"*" {
            break;
        }
        match o.as_str() {
            "MAXLEN" | "MINID" if more > 0 => {
                let mut v = i + 1;
                match a[v].as_ref() {
                    b"~" if more >= 2 => {
                        approx = true;
                        v += 1;
                    }
                    b"=" if more >= 2 => v += 1,
                    _ => {}
                }
                if o == "MAXLEN" {
                    let n = parse_i64(&a[v]).ok_or_else(reply::not_int)?;
                    if n < 0 {
                        return Err(Frame::err("The MAXLEN argument must be >= 0."));
                    }
                    maxlen = Some(n as u64);
                } else {
                    minid = Some(parse_id(&a[v], 0).ok_or_else(invalid_id)?);
                }
                i = v + 1;
            }
            "LIMIT" if more > 0 => {
                let n = parse_i64(&a[i + 1]).ok_or_else(reply::not_int)?;
                if n < 0 {
                    return Err(Frame::err("The LIMIT argument must be >= 0."));
                }
                limit = Some(n);
                i += 2;
            }
            "NOMKSTREAM" if xadd => {
                nomk = true;
                i += 1;
            }
            _ if xadd => break,
            _ => return Err(reply::syntax()),
        }
    }
    if limit.is_some() && !approx {
        return Err(Frame::err(
            "syntax error, LIMIT cannot be used without the special ~ option",
        ));
    }
    if maxlen.is_some() && minid.is_some() {
        return Err(Frame::err(
            "syntax error, MAXLEN and MINID options at the same time are not compatible",
        ));
    }
    let strategy = match (maxlen, minid) {
        (Some(n), _) => Some(Strategy::MaxLen(n)),
        (_, Some(id)) => Some(Strategy::MinId(id)),
        _ => None,
    };
    let limit = match limit {
        Some(n) => n as u64,
        None if approx => 100 * NODE_ENTRIES,
        None => 0,
    };
    Ok(AddArgs {
        trim: strategy.map(|strategy| Trim {
            strategy,
            approx,
            limit,
        }),
        nomkstream: nomk,
        at: i,
    })
}

/// Trims stream `s` (in place): the number of entries dropped.
fn trim<B: Batch>(cx: &mut Cx<'_, B>, s: &mut StreamMeta, t: &Trim) -> Result<u64, StoreError> {
    let uid = s.uid;
    let mut dropped = 0u64;
    let done = |s: &StreamMeta| match t.strategy {
        Strategy::MaxLen(n) => s.length <= n,
        Strategy::MinId(id) => s.length == 0 || s.first >= id,
    };
    if !t.approx {
        while !done(s) {
            let want = match t.strategy {
                Strategy::MaxLen(n) => usize::try_from(s.length - n)
                    .unwrap_or(usize::MAX)
                    .min(PAGE),
                Strategy::MinId(_) => PAGE,
            };
            let got = cx.entries(uid, StreamId::MIN, StreamId::MAX, want, false)?;
            if got.is_empty() {
                break;
            }
            for (id, _, _) in got {
                if let Strategy::MinId(m) = t.strategy
                    && id >= m
                {
                    s.first = id;
                    return Ok(dropped);
                }
                cx.b.delete(entry_key(cx.space, uid, id))?;
                s.length -= 1;
                dropped += 1;
                if done(s) {
                    break;
                }
            }
            refresh_first(cx, s)?;
        }
        refresh_first(cx, s)?;
        return Ok(dropped);
    }
    // Approximate: whole modelled nodes only.
    loop {
        if s.length == 0 {
            break;
        }
        let Some((_, ord)) = cx.first_entry(uid)? else {
            break;
        };
        let node = ord / NODE_ENTRIES;
        let got = cx.entries(
            uid,
            StreamId::MIN,
            StreamId::MAX,
            NODE_ENTRIES as usize,
            false,
        )?;
        let mine: Vec<StreamId> = got
            .iter()
            .take_while(|(_, o, _)| o / NODE_ENTRIES == node)
            .map(|(id, _, _)| *id)
            .collect();
        let n = mine.len() as u64;
        let fits = match t.strategy {
            Strategy::MaxLen(max) => s.length - n >= max,
            Strategy::MinId(m) => mine.last().is_some_and(|l| *l < m),
        };
        if !fits || (t.limit > 0 && dropped + n > t.limit) {
            break;
        }
        for id in mine {
            cx.b.delete(entry_key(cx.space, uid, id))?;
        }
        s.length -= n;
        dropped += n;
    }
    refresh_first(cx, s)?;
    Ok(dropped)
}

fn refresh_first<B: Batch>(cx: &mut Cx<'_, B>, s: &mut StreamMeta) -> Result<(), StoreError> {
    s.first = if s.length == 0 {
        StreamId::MIN
    } else {
        cx.first_entry(s.uid)?.map_or(StreamId::MIN, |(id, _)| id)
    };
    Ok(())
}

// ---------------------------------------------------------------------------
// Commands.

fn fields_frame(f: Vec<Bytes>) -> Frame {
    Frame::Array(f.into_iter().map(Frame::Bulk).collect())
}

fn entry_frame(id: StreamId, f: Vec<Bytes>) -> Frame {
    Frame::Array(vec![id_frame(id), fields_frame(f)])
}

/// `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT n]] *|id
/// field value [field value ...]`.
fn xadd<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    enum Want {
        Auto,
        Seq(u64),
        Exact(StreamId),
    }
    let args = tri!(trim_args(a, true));
    let at = args.at;
    if at >= a.len() {
        return Ok(reply::arity("xadd"));
    }
    let fields_ok = (a.len() - at - 1) >= 2 && (a.len() - at - 1).is_multiple_of(2);
    // The ID: `*`, `ms-*`, or an explicit one.
    let raw = &a[at];
    let want = if raw.as_ref() == b"*" {
        Want::Auto
    } else if let Some(ms) = raw.strip_suffix(b"-*") {
        Want::Seq(tri!(parse_u64(ms).ok_or_else(invalid_id)))
    } else {
        let id = tri!(parse_id(raw, 0).ok_or_else(invalid_id));
        if !fields_ok {
            return Ok(reply::arity("xadd"));
        }
        if id == StreamId::MIN {
            return Ok(Frame::err(
                "The ID specified in XADD must be greater than 0-0",
            ));
        }
        Want::Exact(id)
    };
    if !fields_ok {
        return Ok(reply::arity("xadd"));
    }
    let l = tri!(cx.load_stream(&a[1])?);
    let existing = l.stream();
    if existing.is_none() && args.nomkstream {
        return Ok(Frame::null());
    }
    let mut s = match existing {
        Some(s) => s,
        None => StreamMeta {
            uid: cx.take_uid()?,
            ..StreamMeta::default()
        },
    };
    let top = || {
        Frame::err("The ID specified in XADD is equal or smaller than the target stream top item")
    };
    let exhausted =
        || Frame::err("The stream has exhausted the last possible ID, unable to add more items");
    let id = match want {
        Want::Auto => {
            if s.last == StreamId::MAX {
                return Ok(exhausted());
            }
            let now = u64::try_from(cx.now).unwrap_or(0);
            if now > s.last.ms {
                StreamId { ms: now, seq: 0 }
            } else {
                match s.last.next() {
                    Some(n) => n,
                    None => return Ok(exhausted()),
                }
            }
        }
        Want::Seq(ms) => {
            if ms < s.last.ms {
                return Ok(top());
            }
            if ms == s.last.ms {
                if s.last.seq == u64::MAX {
                    return Ok(if ms == u64::MAX { exhausted() } else { top() });
                }
                StreamId {
                    ms,
                    seq: s.last.seq + 1,
                }
            } else {
                StreamId { ms, seq: 0 }
            }
        }
        Want::Exact(id) => {
            if s.last == StreamId::MAX {
                return Ok(exhausted());
            }
            if id <= s.last {
                return Ok(top());
            }
            id
        }
    };
    let ord = s.added;
    cx.b.put(
        entry_key(cx.space, s.uid, id),
        entry_value(ord, &a[at + 1..]),
    )?;
    if s.length == 0 {
        s.first = id;
    }
    s.length += 1;
    s.added += 1;
    s.last = id;
    if let Some(t) = &args.trim
        && trim(cx, &mut s, t)? > 0
    {
        cx.event(&a[1], "xtrim");
    }
    cx.save_stream(&a[1], &l, s)?;
    Ok(id_frame(id))
}

/// `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT n]`.
fn xtrim<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let args = tri!(trim_args(a, false));
    let Some(t) = args.trim else {
        return Ok(reply::syntax());
    };
    let l = tri!(cx.load_stream(&a[1])?);
    let Some(mut s) = l.stream() else {
        return Ok(Frame::Integer(0));
    };
    let n = trim(cx, &mut s, &t)?;
    if n > 0 {
        cx.save_stream(&a[1], &l, s)?;
    }
    Ok(Frame::Integer(n as i64))
}

/// `XLEN key`.
fn xlen<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let l = tri!(cx.load_stream(&a[1])?);
    Ok(Frame::Integer(l.stream().map_or(0, |s| s.length as i64)))
}

/// `XRANGE key start end [COUNT n]`, `XREVRANGE key end start [COUNT n]`.
fn xrange<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], rev: bool) -> Out {
    let (s_arg, e_arg) = if rev { (&a[3], &a[2]) } else { (&a[2], &a[3]) };
    let start = tri!(range_bound(s_arg, true));
    let end = tri!(range_bound(e_arg, false));
    let mut count: Option<i64> = None;
    let mut i = 4;
    while i < a.len() {
        if upper(&a[i]) == "COUNT" && i + 1 < a.len() {
            count = Some(tri!(super::int(&a[i + 1])));
            i += 2;
        } else {
            return Ok(reply::syntax());
        }
    }
    let l = tri!(cx.load_stream(&a[1])?);
    let Some(s) = l.stream() else {
        return Ok(Frame::Array(Vec::new()));
    };
    let limit = match count {
        Some(n) if n <= 0 => return Ok(Frame::null_array()),
        Some(n) => usize::try_from(n).unwrap_or(usize::MAX),
        None => usize::MAX,
    };
    let (Some(start), Some(end)) = (start, end) else {
        return Ok(Frame::Array(Vec::new()));
    };
    let got = cx.entries(s.uid, start, end, limit, rev)?;
    Ok(Frame::Array(
        got.into_iter()
            .map(|(id, _, f)| entry_frame(id, f))
            .collect(),
    ))
}

/// `XDEL key id [id ...]`.
fn xdel<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut ids = Vec::with_capacity(a.len() - 2);
    for x in &a[2..] {
        ids.push(tri!(parse_id(x, 0).ok_or_else(invalid_id)));
    }
    let l = tri!(cx.load_stream(&a[1])?);
    let Some(mut s) = l.stream() else {
        return Ok(Frame::Integer(0));
    };
    let mut n = 0;
    let mut first_gone = false;
    for id in ids {
        let k = entry_key(cx.space, s.uid, id);
        if cx.b.get(&k)?.is_some() {
            cx.b.delete(k)?;
            s.length -= 1;
            n += 1;
            if id > s.max_deleted {
                s.max_deleted = id;
            }
            first_gone |= id == s.first;
        }
    }
    if n > 0 {
        if first_gone || s.length == 0 {
            refresh_first(cx, &mut s)?;
        }
        cx.save_stream(&a[1], &l, s)?;
    }
    Ok(Frame::Integer(n))
}

/// `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`.
fn xsetid<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let id = tri!(parse_id(&a[2], 0).ok_or_else(invalid_id));
    let mut added: Option<u64> = None;
    let mut maxdel: Option<StreamId> = None;
    let mut i = 3;
    while i < a.len() {
        let o = upper(&a[i]);
        if o == "ENTRIESADDED" && i + 1 < a.len() {
            let n = tri!(super::int(&a[i + 1]));
            if n < 0 {
                return Ok(Frame::err("entries_added must be positive"));
            }
            added = Some(n as u64);
            i += 2;
        } else if o == "MAXDELETEDID" && i + 1 < a.len() {
            let m = tri!(parse_id(&a[i + 1], 0).ok_or_else(invalid_id));
            if id < m {
                return Ok(Frame::err(
                    "The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
                ));
            }
            maxdel = Some(m);
            i += 2;
        } else {
            return Ok(reply::syntax());
        }
    }
    let l = tri!(cx.load_stream(&a[1])?);
    let Some(mut s) = l.stream() else {
        return Ok(reply::no_such_key());
    };
    if let Some(n) = added
        && s.length > n
    {
        return Ok(Frame::err(
            "The entries_added specified in XSETID is smaller than the target stream length",
        ));
    }
    if maxdel.is_none() && id < s.max_deleted {
        return Ok(Frame::err(
            "The ID specified in XSETID is smaller than current max_deleted_entry_id",
        ));
    }
    if s.length > 0
        && let Some((top, _)) = cx.last_entry(s.uid)?
        && id < top
    {
        return Ok(Frame::err(
            "The ID specified in XSETID is smaller than the target stream top item",
        ));
    }
    s.last = id;
    if let Some(n) = added {
        s.added = n;
    }
    if let Some(m) = maxdel {
        s.max_deleted = m;
    }
    cx.save_stream(&a[1], &l, s)?;
    Ok(Frame::ok())
}

// ---------------------------------------------------------------------------
// XREAD and XREADGROUP.

/// What `XREAD`/`XREADGROUP` were asked.
pub struct ReadArgs {
    /// `GROUP group consumer`.
    pub group: Option<(Bytes, Bytes)>,
    /// `COUNT` (`None`: no limit).
    pub count: Option<usize>,
    /// `BLOCK` in ms.
    pub block: Option<i64>,
    /// `NOACK`.
    pub noack: bool,
    /// Index of the first key.
    pub keys_at: usize,
    /// How many keys (and IDs).
    pub n: usize,
}

/// Parses `XREAD`/`XREADGROUP`'s options.
///
/// # Errors
/// The reply for bad arguments.
pub fn read_args(a: &[Bytes], group: bool) -> Arg<ReadArgs> {
    let name = if group { "xreadgroup" } else { "xread" };
    let mut out = ReadArgs {
        group: None,
        count: None,
        block: None,
        noack: false,
        keys_at: 0,
        n: 0,
    };
    let mut i = 1;
    while i < a.len() {
        let more = a.len() - 1 - i;
        match upper(&a[i]).as_str() {
            "BLOCK" if more > 0 => {
                let t = parse_i64(&a[i + 1])
                    .ok_or_else(|| Frame::err("timeout is not an integer or out of range"))?;
                if t < 0 {
                    return Err(Frame::err("timeout is negative"));
                }
                out.block = Some(t);
                i += 2;
            }
            "COUNT" if more > 0 => {
                let c = parse_i64(&a[i + 1]).ok_or_else(reply::not_int)?;
                out.count = (c > 0).then(|| usize::try_from(c).unwrap_or(usize::MAX));
                i += 2;
            }
            "STREAMS" if more > 0 => {
                out.keys_at = i + 1;
                break;
            }
            "GROUP" if group && more >= 2 => {
                out.group = Some((a[i + 1].clone(), a[i + 2].clone()));
                i += 3;
            }
            "NOACK" if group => {
                out.noack = true;
                i += 1;
            }
            _ => return Err(reply::syntax()),
        }
    }
    if out.keys_at == 0 {
        return Err(reply::syntax());
    }
    let rest = a.len() - out.keys_at;
    if !rest.is_multiple_of(2) {
        return Err(Frame::err(&format!(
            "Unbalanced '{name}' list of streams: for each stream key an ID or '{}' must be specified.",
            if group { ">" } else { "$" }
        )));
    }
    out.n = rest / 2;
    if group && out.group.is_none() {
        return Err(Frame::err("Missing GROUP option for XREADGROUP"));
    }
    Ok(out)
}

/// The keys `XREAD`/`XREADGROUP` name (what a blocked one waits on).
#[must_use]
pub fn read_keys(a: &[Bytes], group: bool) -> Vec<Bytes> {
    read_args(a, group)
        .map(|r| a[r.keys_at..r.keys_at + r.n].to_vec())
        .unwrap_or_default()
}

/// What one stream's ID argument asks for.
enum Start {
    /// Entries after this ID.
    After(StreamId),
    /// Entries after the stream's last ID (`$`).
    Dollar,
    /// The last entry (`+`).
    Last,
    /// New entries for the group (`>`).
    New,
    /// The consumer's pending entries after this ID.
    History(StreamId),
}

fn streams_reply(resp3: bool, items: Vec<(Bytes, Vec<Frame>)>) -> Frame {
    if items.is_empty() {
        return Frame::null_array();
    }
    if resp3 {
        Frame::Map(
            items
                .into_iter()
                .map(|(k, e)| (Frame::Bulk(k), Frame::Array(e)))
                .collect(),
        )
    } else {
        Frame::Array(
            items
                .into_iter()
                .map(|(k, e)| Frame::Array(vec![Frame::Bulk(k), Frame::Array(e)]))
                .collect(),
        )
    }
}

fn xread<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], group: bool) -> Out {
    let r = tri!(read_args(a, group));
    let keys = &a[r.keys_at..r.keys_at + r.n];
    let ids = &a[r.keys_at + r.n..];
    // Parse every ID first.
    let mut starts = Vec::with_capacity(r.n);
    for x in ids {
        starts.push(match x.as_ref() {
            b">" if group => Start::New,
            b">" => {
                return Ok(Frame::err(
                    "The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.",
                ));
            }
            b"$" if group => {
                return Ok(Frame::err(
                    "The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.",
                ));
            }
            b"+" if group => {
                return Ok(Frame::err(
                    "The + ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The + ID would just return an empty result set.",
                ));
            }
            b"$" => Start::Dollar,
            b"+" => Start::Last,
            _ => {
                let id = tri!(parse_id(x, 0).ok_or_else(invalid_id));
                if group {
                    Start::History(id)
                } else {
                    Start::After(id)
                }
            }
        });
    }
    // Every key and group first: a refusal reads nothing.
    let mut loaded = Vec::with_capacity(r.n);
    for (k, st) in keys.iter().zip(&mut starts) {
        let l = tri!(cx.load_stream(k)?);
        if let Some((g, _)) = &r.group {
            let grp = match l.stream() {
                Some(s) => cx.group(s.uid, g)?,
                None => None,
            };
            if grp.is_none() {
                return Ok(Frame::error(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with GROUP option",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(g)
                )));
            }
        } else if matches!(st, Start::Dollar) {
            // `$`: the stream's last ID now.
            *st = Start::After(l.stream().map_or(StreamId::MIN, |s| s.last));
        }
        loaded.push(l);
    }
    let limit = r.count.unwrap_or(usize::MAX);
    let mut items = Vec::new();
    for ((k, st), l) in keys.iter().zip(starts).zip(loaded) {
        let Some(s) = l.stream() else {
            continue;
        };
        match st {
            Start::Dollar => {}
            Start::After(id) => {
                let Some(from) = id.next() else {
                    continue;
                };
                let got = cx.entries(s.uid, from, StreamId::MAX, limit, false)?;
                if !got.is_empty() {
                    items.push((
                        k.clone(),
                        got.into_iter()
                            .map(|(id, _, f)| entry_frame(id, f))
                            .collect(),
                    ));
                }
            }
            Start::Last => {
                if let Some(got) = cx
                    .entries(s.uid, StreamId::MIN, StreamId::MAX, 1, true)?
                    .pop()
                {
                    items.push((k.clone(), vec![entry_frame(got.0, got.2)]));
                }
            }
            Start::New => {
                let (g, c) = r.group.clone().unwrap_or_default();
                let got = read_new(cx, k, &l, s, &g, &c, limit, r.noack)?;
                if !got.is_empty() {
                    items.push((k.clone(), got));
                }
            }
            Start::History(id) => {
                let (g, c) = r.group.clone().unwrap_or_default();
                let got = read_history(cx, k, s, &g, &c, id, limit)?;
                items.push((k.clone(), got));
            }
        }
    }
    Ok(streams_reply(cx.resp3, items))
}

/// `XREADGROUP ... >` on one stream: new entries for the group.
#[allow(clippy::too_many_arguments)]
fn read_new<B: Batch>(
    cx: &mut Cx<'_, B>,
    key: &[u8],
    l: &Loaded,
    s: StreamMeta,
    g: &Bytes,
    c: &Bytes,
    limit: usize,
    noack: bool,
) -> Result<Vec<Frame>, StoreError> {
    let rec = cx
        .group(s.uid, g)?
        .ok_or_else(|| StoreError::fatal("stream group vanished in its batch"))?;
    let mut ge = GroupEdit::new(s.uid, g, rec);
    let mut me = ge.ensure(cx, key, c)?;
    me.seen = cx.now;
    ge.set(c, me);
    let got = match ge.rec.last.next() {
        Some(from) => cx.entries(s.uid, from, StreamId::MAX, limit, false)?,
        None => Vec::new(),
    };
    let mut out = Vec::with_capacity(got.len());
    for (id, _, f) in got {
        advance_read(&s, &mut ge.rec, id);
        ge.rec.last = id;
        if !noack {
            let old = cx.pending(s.uid, g, id)?;
            let p = Pending {
                delivered: cx.now,
                count: 1,
                consumer: c.clone(),
            };
            ge.add_pending(cx, id, &p, old.as_ref())?;
        }
        out.push(entry_frame(id, f));
    }
    if !out.is_empty()
        && !noack
        && let Some(mut me) = ge.get(cx, c)?
    {
        me.active = cx.now;
        ge.set(c, me);
    }
    ge.save(cx)?;
    let _ = l;
    cx.mark(key);
    Ok(out)
}

/// `XREADGROUP` with an ID on one stream: the consumer's pending entries
/// after it.
fn read_history<B: Batch>(
    cx: &mut Cx<'_, B>,
    key: &[u8],
    s: StreamMeta,
    g: &Bytes,
    c: &Bytes,
    after: StreamId,
    limit: usize,
) -> Result<Vec<Frame>, StoreError> {
    let rec = cx
        .group(s.uid, g)?
        .ok_or_else(|| StoreError::fatal("stream group vanished in its batch"))?;
    let mut ge = GroupEdit::new(s.uid, g, rec);
    let mut me = ge.ensure(cx, key, c)?;
    me.seen = cx.now;
    ge.set(c, me);
    let ids = match after.next() {
        Some(from) => cx.bycons_ids(s.uid, g, c, from, StreamId::MAX, limit)?,
        None => Vec::new(),
    };
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(f) = cx.entry(s.uid, id)? else {
            // Deleted since: answered with no fields, left as it is.
            out.push(Frame::Array(vec![id_frame(id), Frame::null_array()]));
            continue;
        };
        if let Some(mut p) = cx.pending(s.uid, g, id)? {
            p.count += 1;
            p.delivered = cx.now;
            cx.b.put(pending_key(cx.space, s.uid, g, id), p.encode())?;
        }
        out.push(entry_frame(id, f));
    }
    ge.save(cx)?;
    cx.mark(key);
    Ok(out)
}

// ---------------------------------------------------------------------------
// XGROUP.

const XGROUP_HELP: &[&str] = &[
    "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CREATE <key> <groupname> <id|$> [option]",
    "    Create a new consumer group. Options are:",
    "    * MKSTREAM",
    "      Create the empty stream if it does not exist.",
    "    * ENTRIESREAD entries_read",
    "      Set the group's entries_read counter (internal use).",
    "CREATECONSUMER <key> <groupname> <consumer>",
    "    Create a new consumer in the specified group.",
    "DELCONSUMER <key> <groupname> <consumer>",
    "    Remove the specified consumer.",
    "DESTROY <key> <groupname>",
    "    Remove the specified group.",
    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
    "    Set the current group ID and entries_read counter.",
    "HELP",
    "    Print this help.",
];

const XINFO_HELP: &[&str] = &[
    "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CONSUMERS <key> <groupname>",
    "    Show consumers of <groupname>.",
    "GROUPS <key>",
    "    Show the stream consumer groups.",
    "STREAM <key> [FULL [COUNT <count>]",
    "    Show information about the stream.",
    "HELP",
    "    Print this help.",
];

fn help(lines: &[&str]) -> Frame {
    Frame::Array(lines.iter().map(|l| Frame::simple(l)).collect())
}

/// The subcommand of a container command, checked for arity: its upper
/// name, or the reply.
fn subcommand(cmd: &str, a: &[Bytes], subs: &[(&str, i32)]) -> Arg<String> {
    let sub = upper(&a[1]);
    let Some((name, arity)) = subs.iter().find(|(n, _)| *n == sub) else {
        return Err(reply::unknown_sub(cmd, &a[1]));
    };
    let n = a.len() as i32;
    let ok = if *arity >= 0 {
        n == *arity
    } else {
        n >= -arity
    };
    if !ok {
        return Err(reply::arity(&format!(
            "{cmd}|{}",
            name.to_ascii_lowercase()
        )));
    }
    Ok(sub)
}

/// `$` or an ID: the group's starting ID.
fn group_id(arg: &[u8], s: Option<&StreamMeta>) -> Arg<StreamId> {
    if arg == b"$" {
        return Ok(s.map_or(StreamId::MIN, |s| s.last));
    }
    parse_id(arg, 0).ok_or_else(invalid_id)
}

fn entries_read_arg(v: &[u8]) -> Arg<i64> {
    let n = parse_i64(v).ok_or_else(reply::not_int)?;
    if n < -1 {
        return Err(Frame::err("value for ENTRIESREAD must be positive or -1"));
    }
    Ok(n)
}

fn xgroup<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let sub = tri!(subcommand(
        "xgroup",
        a,
        &[
            ("CREATE", -5),
            ("SETID", -5),
            ("DESTROY", 4),
            ("CREATECONSUMER", 5),
            ("DELCONSUMER", 5),
            ("HELP", 2),
        ],
    ));
    if sub == "HELP" {
        return Ok(help(XGROUP_HELP));
    }
    let key = &a[2];
    let g = &a[3];
    // Options first.
    let mut mkstream = false;
    let mut read: Option<i64> = None;
    if sub == "CREATE" || sub == "SETID" {
        let mut i = 5;
        while i < a.len() {
            let o = upper(&a[i]);
            if sub == "CREATE" && o == "MKSTREAM" {
                mkstream = true;
                i += 1;
            } else if o == "ENTRIESREAD" && i + 1 < a.len() {
                read = Some(tri!(entries_read_arg(&a[i + 1])));
                i += 2;
            } else {
                return Ok(Frame::err(&format!(
                    "unknown subcommand or wrong number of arguments for '{}'. Try XGROUP HELP.",
                    String::from_utf8_lossy(&a[1])
                )));
            }
        }
    }
    let l = tri!(cx.load_stream(key)?);
    let existing = l.stream();
    let Some(mut s) = existing.or_else(|| {
        (sub == "CREATE" && mkstream).then(|| StreamMeta {
            uid: 0,
            ..StreamMeta::default()
        })
    }) else {
        return Ok(key_must_exist());
    };
    match sub.as_str() {
        "CREATE" => {
            let id = tri!(group_id(&a[4], existing.as_ref()));
            if existing.is_some() && cx.group(s.uid, g)?.is_some() {
                return Ok(Frame::error("BUSYGROUP Consumer Group name already exists"));
            }
            if existing.is_none() {
                s.uid = cx.take_uid()?;
            }
            let rec = Group {
                last: id,
                read: read.unwrap_or(-1),
                pending: 0,
                consumers: 0,
            };
            cx.put_group(s.uid, g, &rec)?;
            s.groups += 1;
            cx.save_stream(key, &l, s)?;
            cx.event(key, "xgroup-create");
            Ok(Frame::ok())
        }
        "SETID" => {
            let id = tri!(group_id(&a[4], Some(&s)));
            let Some(mut rec) = cx.group(s.uid, g)? else {
                return Ok(nogroup_for_key(key, g));
            };
            rec.last = id;
            rec.read = read.unwrap_or(-1);
            cx.put_group(s.uid, g, &rec)?;
            cx.mark(key);
            cx.event(key, "xgroup-setid");
            Ok(Frame::ok())
        }
        "DESTROY" => {
            if cx.group(s.uid, g)?.is_none() {
                return Ok(Frame::Integer(0));
            }
            cx.b.delete(group_key(cx.space, s.uid, g))?;
            for tag in [TAG_CONSUMER, TAG_PENDING, TAG_BYCONS] {
                let (lo, hi) = prefixed(&with_group(cx.space, s.uid, tag, g));
                cx.delete_range(&lo, &hi)?;
            }
            s.groups = s.groups.saturating_sub(1);
            cx.save_stream(key, &l, s)?;
            cx.event(key, "xgroup-destroy");
            Ok(Frame::Integer(1))
        }
        "CREATECONSUMER" => {
            let Some(rec) = cx.group(s.uid, g)? else {
                return Ok(nogroup_for_key(key, g));
            };
            let c = &a[4];
            if cx.consumer(s.uid, g, c)?.is_some() {
                return Ok(Frame::Integer(0));
            }
            let mut ge = GroupEdit::new(s.uid, g, rec);
            ge.ensure(cx, key, c)?;
            ge.save(cx)?;
            cx.mark(key);
            Ok(Frame::Integer(1))
        }
        _ => {
            let Some(rec) = cx.group(s.uid, g)? else {
                return Ok(nogroup_for_key(key, g));
            };
            let c = &a[4];
            let Some(me) = cx.consumer(s.uid, g, c)? else {
                return Ok(Frame::Integer(0));
            };
            let mut ge = GroupEdit::new(s.uid, g, rec);
            // Its pending entries go.
            let ids = cx.bycons_ids(s.uid, g, c, StreamId::MIN, StreamId::MAX, usize::MAX)?;
            for id in &ids {
                cx.b.delete(pending_key(cx.space, s.uid, g, *id))?;
                cx.b.delete(bycons_key(cx.space, s.uid, g, c, *id))?;
            }
            ge.rec.pending = ge.rec.pending.saturating_sub(ids.len() as u64);
            ge.rec.consumers = ge.rec.consumers.saturating_sub(1);
            cx.b.delete(consumer_key(cx.space, s.uid, g, c))?;
            cx.put_group(s.uid, g, &ge.rec)?;
            cx.mark(key);
            cx.event(key, "xgroup-delconsumer");
            Ok(Frame::Integer(me.pending as i64))
        }
    }
}

// ---------------------------------------------------------------------------
// XACK, XPENDING, XCLAIM, XAUTOCLAIM.

/// `XACK key group id [id ...]`.
fn xack<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut ids = Vec::with_capacity(a.len() - 3);
    for x in &a[3..] {
        ids.push(tri!(parse_id(x, 0).ok_or_else(invalid_id)));
    }
    let l = tri!(cx.load_stream(&a[1])?);
    let Some(s) = l.stream() else {
        return Ok(Frame::Integer(0));
    };
    let g = &a[2];
    let Some(rec) = cx.group(s.uid, g)? else {
        return Ok(Frame::Integer(0));
    };
    let mut ge = GroupEdit::new(s.uid, g, rec);
    let mut n = 0;
    for id in ids {
        if let Some(p) = cx.pending(s.uid, g, id)? {
            ge.remove_pending(cx, id, &p)?;
            n += 1;
        }
    }
    if n > 0 {
        ge.save(cx)?;
        cx.mark(&a[1]);
    }
    Ok(Frame::Integer(n))
}

/// `XPENDING key group [[IDLE ms] start end count [consumer]]`.
fn xpending<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let key = &a[1];
    let g = &a[2];
    let extended = a.len() > 3;
    let mut idle: Option<i64> = None;
    let mut i = 3;
    if extended && upper(&a[3]) == "IDLE" {
        if a.len() < 5 {
            return Ok(reply::syntax());
        }
        idle = Some(tri!(super::int(&a[4])));
        i = 5;
    }
    let (mut start, mut end, mut count, mut who) = (None, None, 0i64, None);
    if extended {
        if a.len() < i + 3 {
            return Ok(reply::syntax());
        }
        start = tri!(range_bound(&a[i], true));
        end = tri!(range_bound(&a[i + 1], false));
        count = tri!(super::int(&a[i + 2]));
        who = a.get(i + 3).cloned();
    }
    let l = tri!(cx.load_stream(key)?);
    let grp = match l.stream() {
        Some(s) => cx.group(s.uid, g)?.map(|r| (s, r)),
        None => None,
    };
    let Some((s, rec)) = grp else {
        return Ok(nogroup(key, g));
    };
    if !extended {
        if rec.pending == 0 {
            return Ok(Frame::Array(vec![
                Frame::Integer(0),
                Frame::null(),
                Frame::null(),
                Frame::null_array(),
            ]));
        }
        let (lo, hi) = pending_range(cx.space, s.uid, g, StreamId::MIN, StreamId::MAX);
        let first = cx.pending_scan(&lo, &hi, 1, false)?;
        let last = cx.pending_scan(&lo, &hi, 1, true)?;
        let mut cons = Vec::new();
        for (name, c) in cx.consumers(s.uid, g)? {
            if c.pending > 0 {
                cons.push(Frame::Array(vec![
                    Frame::Bulk(name),
                    Frame::Bulk(Bytes::from(c.pending.to_string())),
                ]));
            }
        }
        return Ok(Frame::Array(vec![
            Frame::Integer(rec.pending as i64),
            first
                .first()
                .map_or_else(Frame::null, |(id, _)| id_frame(*id)),
            last.first()
                .map_or_else(Frame::null, |(id, _)| id_frame(*id)),
            Frame::Array(cons),
        ]));
    }
    let (Some(start), Some(end)) = (start, end) else {
        return Ok(Frame::Array(Vec::new()));
    };
    if count <= 0 || start > end {
        return Ok(Frame::Array(Vec::new()));
    }
    let limit = usize::try_from(count).unwrap_or(usize::MAX);
    let mut out = Vec::new();
    let now = cx.now;
    let keep = |p: &Pending| idle.is_none_or(|t| now - p.delivered >= t);
    if let Some(c) = who {
        let mut from = start;
        loop {
            let ids = cx.bycons_ids(s.uid, g, &c, from, end, PAGE)?;
            let n = ids.len();
            for id in ids {
                if let Some(p) = cx.pending(s.uid, g, id)?
                    && keep(&p)
                {
                    out.push(pending_row(id, &p, now));
                    if out.len() >= limit {
                        return Ok(Frame::Array(out));
                    }
                }
                match id.next() {
                    Some(nx) => from = nx,
                    None => return Ok(Frame::Array(out)),
                }
            }
            if n < PAGE {
                return Ok(Frame::Array(out));
            }
        }
    }
    let (mut lo, hi) = pending_range(cx.space, s.uid, g, start, end);
    loop {
        let got = cx.pending_scan(&lo, &hi, PAGE, false)?;
        let n = got.len();
        for (id, p) in got {
            lo = layout::after(&pending_key(cx.space, s.uid, g, id));
            if keep(&p) {
                out.push(pending_row(id, &p, now));
                if out.len() >= limit {
                    return Ok(Frame::Array(out));
                }
            }
        }
        if n < PAGE {
            return Ok(Frame::Array(out));
        }
    }
}

fn pending_row(id: StreamId, p: &Pending, now: i64) -> Frame {
    Frame::Array(vec![
        id_frame(id),
        Frame::Bulk(p.consumer.clone()),
        Frame::Integer((now - p.delivered).max(0)),
        Frame::Integer(p.count as i64),
    ])
}

/// `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME
/// ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]`.
fn xclaim<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (key, g, c) = (&a[1], &a[2], &a[3]);
    let min_idle = tri!(
        parse_i64(&a[4]).ok_or_else(|| Frame::err("Invalid min-idle-time argument for XCLAIM"))
    )
    .max(0);
    let mut i = 5;
    let mut ids = Vec::new();
    while i < a.len() {
        match parse_id(&a[i], 0) {
            Some(id) => ids.push(id),
            None => break,
        }
        i += 1;
    }
    let now = cx.now;
    let mut delivery: Option<i64> = None;
    let mut retry: Option<i64> = None;
    let (mut force, mut justid) = (false, false);
    let mut lastid: Option<StreamId> = None;
    while i < a.len() {
        let more = a.len() - 1 - i;
        match upper(&a[i]).as_str() {
            "FORCE" => force = true,
            "JUSTID" => justid = true,
            "IDLE" if more > 0 => {
                i += 1;
                let t = tri!(
                    parse_i64(&a[i])
                        .ok_or_else(|| Frame::err("Invalid IDLE option argument for XCLAIM"))
                );
                delivery = Some(now - t);
            }
            "TIME" if more > 0 => {
                i += 1;
                delivery = Some(tri!(
                    parse_i64(&a[i])
                        .ok_or_else(|| Frame::err("Invalid TIME option argument for XCLAIM"))
                ));
            }
            "RETRYCOUNT" if more > 0 => {
                i += 1;
                retry = Some(tri!(parse_i64(&a[i]).ok_or_else(|| Frame::err(
                    "Invalid RETRYCOUNT option argument for XCLAIM"
                ))));
            }
            "LASTID" if more > 0 => {
                i += 1;
                lastid = Some(tri!(parse_id(&a[i], 0).ok_or_else(invalid_id)));
            }
            _ => {
                return Ok(Frame::err(&format!(
                    "Unrecognized XCLAIM option '{}'",
                    String::from_utf8_lossy(&a[i])
                )));
            }
        }
        i += 1;
    }
    if let Some(d) = delivery.as_mut() {
        *d = (*d).clamp(0, now.max(0));
    }
    let l = tri!(cx.load_stream(key)?);
    let grp = match l.stream() {
        Some(s) => cx.group(s.uid, g)?.map(|r| (s, r)),
        None => None,
    };
    let Some((s, rec)) = grp else {
        return Ok(nogroup(key, g));
    };
    let mut ge = GroupEdit::new(s.uid, g, rec);
    if let Some(l) = lastid
        && l > ge.rec.last
    {
        ge.rec.last = l;
    }
    let mut me = ge.ensure(cx, key, c)?;
    me.seen = now;
    ge.set(c, me);
    let mut out = Vec::new();
    for id in ids {
        let mut old = cx.pending(s.uid, g, id)?;
        let fields = cx.entry(s.uid, id)?;
        if old.is_none() && (!force || fields.is_none()) {
            continue;
        }
        if let Some(o) = &old {
            if fields.is_none() {
                // Its entry is gone: the pending entry goes too.
                ge.remove_pending(cx, id, o)?;
                continue;
            }
            if min_idle > 0 && now - o.delivered < min_idle {
                continue;
            }
        }
        let base = old.take();
        let mut p = Pending {
            delivered: delivery.unwrap_or(now),
            count: base.as_ref().map_or(1, |o| o.count),
            consumer: c.clone(),
        };
        if let Some(r) = retry {
            p.count = u64::try_from(r).unwrap_or(0);
        } else if !justid {
            p.count += 1;
        }
        ge.add_pending(cx, id, &p, base.as_ref())?;
        out.push(match (justid, fields) {
            (true, _) | (_, None) => id_frame(id),
            (false, Some(f)) => entry_frame(id, f),
        });
    }
    if !out.is_empty()
        && let Some(mut me) = ge.get(cx, c)?
    {
        me.active = now;
        ge.set(c, me);
    }
    ge.save(cx)?;
    cx.mark(key);
    Ok(Frame::Array(out))
}

/// `XAUTOCLAIM key group consumer min-idle-time start [COUNT n] [JUSTID]`.
fn xautoclaim<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (key, g, c) = (&a[1], &a[2], &a[3]);
    let min_idle = tri!(
        parse_i64(&a[4]).ok_or_else(|| Frame::err("Invalid min-idle-time argument for XAUTOCLAIM"))
    )
    .max(0);
    let start = tri!(range_bound(&a[5], true));
    let mut count = 100i64;
    let mut justid = false;
    let mut i = 6;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "COUNT" if i + 1 < a.len() => {
                count = parse_i64(&a[i + 1]).unwrap_or(0);
                if !(1..=i64::MAX / 10).contains(&count) {
                    return Ok(Frame::err("COUNT must be > 0"));
                }
                i += 2;
            }
            "JUSTID" => {
                justid = true;
                i += 1;
            }
            _ => return Ok(reply::syntax()),
        }
    }
    let l = tri!(cx.load_stream(key)?);
    let grp = match l.stream() {
        Some(s) => cx.group(s.uid, g)?.map(|r| (s, r)),
        None => None,
    };
    let Some((s, rec)) = grp else {
        return Ok(nogroup(key, g));
    };
    let now = cx.now;
    let mut ge = GroupEdit::new(s.uid, g, rec);
    let mut me = ge.ensure(cx, key, c)?;
    me.seen = now;
    ge.set(c, me);
    let mut attempts = count.saturating_mul(10);
    let mut claimed = Vec::new();
    let mut deleted = Vec::new();
    let mut cursor = StreamId::MIN;
    if let Some(start) = start {
        let (mut lo, hi) = pending_range(cx.space, s.uid, g, start, StreamId::MAX);
        'scan: loop {
            let got = cx.pending_scan(&lo, &hi, PAGE, false)?;
            let n = got.len();
            for (id, p) in got {
                lo = layout::after(&pending_key(cx.space, s.uid, g, id));
                if attempts == 0 || claimed.len() as i64 >= count {
                    cursor = id;
                    break 'scan;
                }
                attempts -= 1;
                if min_idle > 0 && now - p.delivered < min_idle {
                    continue;
                }
                let Some(f) = cx.entry(s.uid, id)? else {
                    ge.remove_pending(cx, id, &p)?;
                    deleted.push(id_frame(id));
                    continue;
                };
                let np = Pending {
                    delivered: now,
                    count: if justid { p.count } else { p.count + 1 },
                    consumer: c.clone(),
                };
                ge.add_pending(cx, id, &np, Some(&p))?;
                claimed.push(if justid {
                    id_frame(id)
                } else {
                    entry_frame(id, f)
                });
            }
            if n < PAGE {
                break;
            }
        }
    }
    if !claimed.is_empty()
        && let Some(mut me) = ge.get(cx, c)?
    {
        me.active = now;
        ge.set(c, me);
    }
    ge.save(cx)?;
    cx.mark(key);
    Ok(Frame::Array(vec![
        id_frame(cursor),
        Frame::Array(claimed),
        Frame::Array(deleted),
    ]))
}

// ---------------------------------------------------------------------------
// XINFO.

fn kv(k: &str, v: Frame) -> (Frame, Frame) {
    (Frame::bulk(Bytes::copy_from_slice(k.as_bytes())), v)
}

fn xinfo<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let sub = tri!(subcommand(
        "xinfo",
        a,
        &[("STREAM", -3), ("GROUPS", 3), ("CONSUMERS", 4), ("HELP", 2)],
    ));
    if sub == "HELP" {
        return Ok(help(XINFO_HELP));
    }
    let key = &a[2];
    let mut full = false;
    let mut count: i64 = 10;
    if sub == "STREAM" {
        let mut i = 3;
        while i < a.len() {
            match upper(&a[i]).as_str() {
                "FULL" if !full => {
                    full = true;
                    i += 1;
                }
                "COUNT" if full && i + 1 < a.len() => {
                    count = tri!(super::int(&a[i + 1]));
                    i += 2;
                }
                _ => {
                    return Ok(Frame::err(&format!(
                        "unknown subcommand or wrong number of arguments for '{}'. Try XINFO HELP.",
                        String::from_utf8_lossy(&a[1])
                    )));
                }
            }
        }
    }
    let l = tri!(cx.load_stream(key)?);
    let Some(s) = l.stream() else {
        return Ok(reply::no_such_key());
    };
    let now = cx.now;
    match sub.as_str() {
        "GROUPS" => {
            let mut out = Vec::new();
            for (name, g) in cx.groups(s.uid)? {
                out.push(Frame::Map(vec![
                    kv("name", Frame::Bulk(name)),
                    kv("consumers", Frame::Integer(g.consumers as i64)),
                    kv("pending", Frame::Integer(g.pending as i64)),
                    kv("last-delivered-id", id_frame(g.last)),
                    kv("entries-read", read_frame(g.read)),
                    kv("lag", opt_int(lag(&s, &g))),
                ]));
            }
            Ok(Frame::Array(out))
        }
        "CONSUMERS" => {
            let g = &a[3];
            if cx.group(s.uid, g)?.is_none() {
                return Ok(nogroup_for_key(key, g));
            }
            let mut out = Vec::new();
            for (name, c) in cx.consumers(s.uid, g)? {
                out.push(Frame::Map(vec![
                    kv("name", Frame::Bulk(name)),
                    kv("pending", Frame::Integer(c.pending as i64)),
                    kv("idle", Frame::Integer((now - c.seen).max(0))),
                    kv(
                        "inactive",
                        Frame::Integer(if c.active < 0 {
                            -1
                        } else {
                            (now - c.active).max(0)
                        }),
                    ),
                ]));
            }
            Ok(Frame::Array(out))
        }
        _ => {
            let (rk, rn) = radix_counts(cx, &s)?;
            let mut m = vec![
                kv("length", Frame::Integer(s.length as i64)),
                kv("radix-tree-keys", Frame::Integer(rk)),
                kv("radix-tree-nodes", Frame::Integer(rn)),
                kv("last-generated-id", id_frame(s.last)),
                kv("max-deleted-entry-id", id_frame(s.max_deleted)),
                kv("entries-added", Frame::Integer(s.added as i64)),
                kv("recorded-first-entry-id", id_frame(s.first)),
            ];
            if !full {
                m.push(kv("groups", Frame::Integer(s.groups as i64)));
                let first = cx.entries(s.uid, StreamId::MIN, StreamId::MAX, 1, false)?;
                let last = cx.entries(s.uid, StreamId::MIN, StreamId::MAX, 1, true)?;
                let one = |v: Vec<(StreamId, u64, Vec<Bytes>)>| {
                    v.into_iter()
                        .next()
                        .map_or_else(Frame::null, |(id, _, f)| entry_frame(id, f))
                };
                m.push(kv("first-entry", one(first)));
                m.push(kv("last-entry", one(last)));
                return Ok(Frame::Map(m));
            }
            let limit = if count <= 0 {
                usize::MAX
            } else {
                usize::try_from(count).unwrap_or(usize::MAX)
            };
            let entries = cx.entries(s.uid, StreamId::MIN, StreamId::MAX, limit, false)?;
            m.push(kv(
                "entries",
                Frame::Array(
                    entries
                        .into_iter()
                        .map(|(id, _, f)| entry_frame(id, f))
                        .collect(),
                ),
            ));
            let mut groups = Vec::new();
            for (name, g) in cx.groups(s.uid)? {
                let (lo, hi) = pending_range(cx.space, s.uid, &name, StreamId::MIN, StreamId::MAX);
                let pel = cx.pending_scan(&lo, &hi, limit, false)?;
                let pel_rows: Vec<Frame> = pel
                    .iter()
                    .map(|(id, p)| {
                        Frame::Array(vec![
                            id_frame(*id),
                            Frame::Bulk(p.consumer.clone()),
                            Frame::Integer(p.delivered),
                            Frame::Integer(p.count as i64),
                        ])
                    })
                    .collect();
                let mut cons = Vec::new();
                for (cname, c) in cx.consumers(s.uid, &name)? {
                    let ids =
                        cx.bycons_ids(s.uid, &name, &cname, StreamId::MIN, StreamId::MAX, limit)?;
                    let mut rows = Vec::with_capacity(ids.len());
                    for id in ids {
                        if let Some(p) = cx.pending(s.uid, &name, id)? {
                            rows.push(Frame::Array(vec![
                                id_frame(id),
                                Frame::Integer(p.delivered),
                                Frame::Integer(p.count as i64),
                            ]));
                        }
                    }
                    cons.push(Frame::Map(vec![
                        kv("name", Frame::Bulk(cname)),
                        kv("seen-time", Frame::Integer(c.seen)),
                        kv("active-time", Frame::Integer(c.active)),
                        kv("pel-count", Frame::Integer(c.pending as i64)),
                        kv("pending", Frame::Array(rows)),
                    ]));
                }
                groups.push(Frame::Map(vec![
                    kv("name", Frame::Bulk(name)),
                    kv("last-delivered-id", id_frame(g.last)),
                    kv("entries-read", read_frame(g.read)),
                    kv("lag", opt_int(lag(&s, &g))),
                    kv("pel-count", Frame::Integer(g.pending as i64)),
                    kv("pending", Frame::Array(pel_rows)),
                    kv("consumers", Frame::Array(cons)),
                ]));
            }
            m.push(kv("groups", Frame::Array(groups)));
            Ok(Frame::Map(m))
        }
    }
}

/// `radix-tree-keys` and `radix-tree-nodes`: the modelled nodes holding
/// entries (by the first and last entries' ordinals), and one more node.
fn radix_counts<B: Batch>(cx: &mut Cx<'_, B>, s: &StreamMeta) -> Result<(i64, i64), StoreError> {
    if s.length == 0 {
        return Ok((0, 1));
    }
    let (Some((_, f)), Some((_, l))) = (cx.first_entry(s.uid)?, cx.last_entry(s.uid)?) else {
        return Ok((0, 1));
    };
    let keys = (l / NODE_ENTRIES - f / NODE_ENTRIES + 1) as i64;
    Ok((keys, keys + 1))
}

/// The ordinal of an entry value (for tests and tools).
#[must_use]
pub fn ordinal_of(v: &[u8]) -> Option<u64> {
    entry_ordinal(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_parse_like_redis() {
        assert_eq!(parse_id(b"5", 0), Some(StreamId { ms: 5, seq: 0 }));
        assert_eq!(parse_id(b"5", u64::MAX).map(|i| i.seq), Some(u64::MAX));
        assert_eq!(parse_id(b"5-3", 0), Some(StreamId { ms: 5, seq: 3 }));
        assert_eq!(parse_id(b"5-x", 0), None);
        assert_eq!(parse_id(b"-1", 0), None);
        assert_eq!(parse_id(b"18446744073709551616", 0), None);
        assert!(range_bound(b"(-", true).is_err());
        assert_eq!(
            range_bound(b"(5", true).unwrap(),
            Some(StreamId { ms: 5, seq: 1 })
        );
        assert!(range_bound(b"(18446744073709551615-18446744073709551615", true).is_err());
    }

    #[test]
    fn values_round_trip() {
        let f = vec![Bytes::from_static(b"a"), Bytes::from_static(b"")];
        let v = entry_value(300, &f);
        assert_eq!(parse_entry(&v), Some((300, f)));
        let g = Group {
            last: StreamId { ms: 1, seq: 2 },
            read: -1,
            pending: 3,
            consumers: 4,
        };
        assert_eq!(Group::decode(&g.encode()), Some(g));
        let p = Pending {
            delivered: 9,
            count: 2,
            consumer: Bytes::from_static(b"c\0x"),
        };
        assert_eq!(Pending::decode(&p.encode()), Some(p));
    }

    #[test]
    fn sub_keys_stay_under_the_uid() {
        let sp = Space::redis(1, 0);
        let (lo, hi) = sp.elems_range(7);
        for k in [
            entry_key(sp, 7, StreamId::MAX),
            group_key(sp, 7, b"\xff\xff"),
            consumer_key(sp, 7, b"g", b"c"),
            pending_key(sp, 7, b"g", StreamId::MAX),
            bycons_key(sp, 7, b"g", b"c", StreamId::MAX),
        ] {
            assert!(lo.as_slice() <= &k[..] && &k[..] < hi.as_slice());
        }
        // One group's pending entries don't run into another's.
        let (_, end) = pending_range(sp, 7, b"g", StreamId::MIN, StreamId::MAX);
        assert!(end.as_slice() <= &pending_key(sp, 7, b"g\0", StreamId::MIN)[..]);
        assert!(end.as_slice() <= &pending_key(sp, 7, b"h", StreamId::MIN)[..]);
    }
}
