//! Strings, bitmaps and bitfields. A string is its whole value in the
//! key's metadata entry; every partial write rewrites it.

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{ld2string_human, parse_f64, parse_i64};
use sprig_redis::reply;

use super::{Arg, Cx, Out, int, opt_bulk, tri, upper};
use crate::layout::{Body, CHUNK, CHUNK_THRESHOLD, Meta, Space};
use crate::store::{Batch, StoreError};

/// Longest string Redis allows (`proto-max-bulk-len`).
const PROTO_MAX: i64 = 512 * 1024 * 1024;

/// Longest string a partial write may grow to on SprigDB (the key-value
/// tables' namespace limit).
pub const MAX_VALUE: usize = 16 * 1024 * 1024;

/// Runs a string command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "get" => get(cx, a),
        "set" => set(cx, a),
        "setnx" => setnx(cx, a),
        "setex" => setex(cx, a, 1000, "setex"),
        "psetex" => setex(cx, a, 1, "psetex"),
        "getset" => getset(cx, a),
        "getdel" => getdel(cx, a),
        "getex" => getex(cx, a),
        "mget" => mget(cx, a),
        "mset" => mset(cx, a, false),
        "msetnx" => mset(cx, a, true),
        "append" => append(cx, a),
        "strlen" => strlen(cx, a),
        "incr" => incrby(cx, &a[1], 1),
        "decr" => incrby(cx, &a[1], -1),
        "incrby" => match int(&a[2]) {
            Ok(by) => incrby(cx, &a[1], by),
            Err(f) => Ok(f),
        },
        "decrby" => match int(&a[2]) {
            Ok(i64::MIN) => Ok(Frame::err("decrement would overflow")),
            Ok(by) => incrby(cx, &a[1], -by),
            Err(f) => Ok(f),
        },
        "incrbyfloat" => incrbyfloat(cx, a),
        "getrange" | "substr" => getrange(cx, a),
        "setrange" => setrange(cx, a),
        "lcs" => lcs(cx, a),
        "setbit" => setbit(cx, a),
        "getbit" => getbit(cx, a),
        "bitcount" => bitcount(cx, a),
        "bitpos" => bitpos(cx, a),
        "bitop" => bitop(cx, a),
        "bitfield" => bitfield(cx, a, false),
        "bitfield_ro" => bitfield(cx, a, true),
        _ => return None,
    })
}

/// A string key: its live value (`None` absent) and its stored metadata.
pub(crate) struct Str {
    /// The whole value (`None` when absent, or when chunked and not read).
    pub value: Option<Bytes>,
    pub exp: Option<i64>,
    pub raw: Option<Meta>,
    /// A chunked value: its uid and length.
    pub chunked: Option<(u64, u64)>,
}

impl Str {
    /// The value's length.
    pub(crate) fn len(&self) -> usize {
        match self.chunked {
            Some((_, len)) => len as usize,
            None => self.value.as_ref().map_or(0, Bytes::len),
        }
    }

    /// Whether the key exists.
    pub(crate) fn exists(&self) -> bool {
        self.value.is_some() || self.chunked.is_some()
    }
}

impl<B: Batch> Cx<'_, B> {
    /// `key` as a string, a chunked value left unread; `Err` answers
    /// `WRONGTYPE`.
    pub(crate) fn string_lazy(&mut self, key: &[u8]) -> Result<Arg<Str>, StoreError> {
        let (live, raw) = self.load(key)?;
        Ok(match live {
            None => Ok(Str {
                value: None,
                exp: None,
                raw,
                chunked: None,
            }),
            Some(Meta {
                body: Body::Str(s),
                exp,
            }) => Ok(Str {
                value: Some(s),
                exp,
                raw,
                chunked: None,
            }),
            Some(Meta {
                body: Body::Chunked { uid, len },
                exp,
            }) => Ok(Str {
                value: None,
                exp,
                raw,
                chunked: Some((uid, len)),
            }),
            Some(_) => Err(reply::wrongtype()),
        })
    }

    /// `key` as a string, read whole; `Err` answers `WRONGTYPE`.
    pub(crate) fn string(&mut self, key: &[u8]) -> Result<Arg<Str>, StoreError> {
        let mut s = match self.string_lazy(key)? {
            Ok(s) => s,
            Err(f) => return Ok(Err(f)),
        };
        if let Some((uid, len)) = s.chunked {
            s.value = Some(Bytes::from(self.chunk_range(uid, len, 0, len as usize)?));
        }
        Ok(Ok(s))
    }

    /// Bytes `[from, to)` of chunked string `uid` of length `len`, zeros
    /// past its end.
    pub(crate) fn chunk_range(
        &mut self,
        uid: u64,
        len: u64,
        from: usize,
        to: usize,
    ) -> Result<Vec<u8>, StoreError> {
        let mut out = vec![0u8; to.saturating_sub(from)];
        let stop = to.min(len as usize);
        if from >= stop {
            return Ok(out);
        }
        let (c0, c1) = (from / CHUNK, (stop - 1) / CHUNK);
        let lo = self.space.chunk(uid, c0 as u32);
        let hi = self.space.chunk(uid, c1 as u32 + 1);
        for (k, v) in super::hashes::scan_all(self.b, &lo, &hi)? {
            let ci =
                Space::parse_chunk(&k).ok_or_else(|| super::corrupt("string chunk", k.len()))?;
            let base = ci as usize * CHUNK;
            let (s, e) = (base.max(from), (base + v.len()).min(stop));
            if s < e {
                out[s - from..e - from].copy_from_slice(&v[s - base..e - base]);
            }
        }
        Ok(out)
    }

    /// Bytes `[from, to)` of string `s`, zeros past its end.
    pub(crate) fn string_bytes(
        &mut self,
        s: &Str,
        from: usize,
        to: usize,
    ) -> Result<Vec<u8>, StoreError> {
        match (s.chunked, &s.value) {
            (Some((uid, len)), _) => self.chunk_range(uid, len, from, to),
            (None, v) => {
                let v = v.as_deref().unwrap_or_default();
                let mut out = vec![0u8; to.saturating_sub(from)];
                let stop = to.min(v.len());
                if from < stop {
                    out[..stop - from].copy_from_slice(&v[from..stop]);
                }
                Ok(out)
            }
        }
    }

    /// A string value of either form, read whole; `None` for other types.
    pub(crate) fn string_of(&mut self, body: Body) -> Result<Option<Bytes>, StoreError> {
        match body {
            Body::Str(s) => Ok(Some(s)),
            Body::Chunked { uid, len } => Ok(Some(Bytes::from(self.chunk_range(
                uid,
                len,
                0,
                len as usize,
            )?))),
            _ => Ok(None),
        }
    }

    /// Writes `data` at byte `off` of string `s` at `key` (zeros fill a
    /// gap), keeping its expiry: the new length. A chunked value rewrites
    /// only the chunks it touches; a whole value that grows past
    /// [`CHUNK_THRESHOLD`] is stored in chunks from now on.
    pub(crate) fn string_write_at(
        &mut self,
        key: &[u8],
        s: &Str,
        off: usize,
        data: &[u8],
    ) -> Result<usize, StoreError> {
        let end = off + data.len();
        if let Some((uid, len)) = s.chunked {
            let len = len as usize;
            let new_len = len.max(end);
            if !data.is_empty() {
                for ci in off / CHUNK..=(end - 1) / CHUNK {
                    let base = ci * CHUNK;
                    let size = CHUNK.min(new_len - base);
                    let mut buf = self.chunk_range(uid, len as u64, base, base + size)?;
                    let (s0, e0) = (off.max(base), end.min(base + size));
                    buf[s0 - base..e0 - base].copy_from_slice(&data[s0 - off..e0 - off]);
                    self.b
                        .put(self.space.chunk(uid, ci as u32), Bytes::from(buf))?;
                }
            }
            let new = Meta {
                exp: s.exp,
                body: Body::Chunked {
                    uid,
                    len: new_len as u64,
                },
            };
            self.write(key, s.raw.as_ref(), Some(&new))?;
            return Ok(new_len);
        }
        let mut v = s.value.as_ref().map(|v| v.to_vec()).unwrap_or_default();
        if v.len() < end {
            v.resize(end, 0);
        }
        v[off..end].copy_from_slice(data);
        let n = v.len();
        if n > CHUNK_THRESHOLD {
            let uid = self.take_uid()?;
            for (ci, c) in v.chunks(CHUNK).enumerate() {
                // A chunk of zeros needn't be stored: it reads as zeros.
                if c.iter().any(|b| *b != 0) {
                    self.b
                        .put(self.space.chunk(uid, ci as u32), Bytes::copy_from_slice(c))?;
                }
            }
            let new = Meta {
                exp: s.exp,
                body: Body::Chunked { uid, len: n as u64 },
            };
            self.write(key, s.raw.as_ref(), Some(&new))?;
        } else {
            self.put_string(key, s.raw.as_ref(), Bytes::from(v), s.exp)?;
        }
        Ok(n)
    }

    /// Stores string `v` at `key` (stored as `raw`) with expiry `exp`.
    pub(crate) fn put_string(
        &mut self,
        key: &[u8],
        raw: Option<&Meta>,
        v: Bytes,
        exp: Option<i64>,
    ) -> Result<(), StoreError> {
        let new = Meta {
            exp,
            body: Body::Str(v),
        };
        self.write(key, raw, Some(&new))
    }
}

fn get<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    Ok(match cx.live(&a[1])?.map(|m| m.body) {
        None => Frame::null(),
        Some(Body::Str(s)) => Frame::Bulk(s),
        Some(b) => cx.string_of(b)?.map_or_else(reply::wrongtype, Frame::Bulk),
    })
}

/// An expiry option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ttl {
    Unset,
    In(i64),
    At(i64),
    Keep,
    Persist,
}

impl Ttl {
    /// The instant this option gives at `now`, for a key whose expiry was
    /// `old`.
    pub(crate) fn resolve(self, now: i64, old: Option<i64>) -> Option<i64> {
        match self {
            Self::Unset | Self::Persist => None,
            Self::In(ms) => Some(now.saturating_add(ms)),
            Self::At(ms) => Some(ms),
            Self::Keep => old,
        }
    }
}

/// An expiry amount of command `cmd`; `unit_ms` is 1000 for seconds.
pub(crate) fn ttl_amount(a: &[u8], unit_ms: i64, cmd: &str) -> Arg<i64> {
    let invalid = || Frame::err(&format!("invalid expire time in '{cmd}' command"));
    let v = int(a)?;
    if v <= 0 {
        return Err(invalid());
    }
    v.checked_mul(unit_ms)
        .filter(|ms| *ms <= i64::MAX / 2)
        .ok_or_else(invalid)
}

/// Parses `EX|PX|EXAT|PXAT n` (and `KEEPTTL` or `PERSIST` when allowed) at
/// `a[*at]`; `None` when `a[*at]` is none of them.
pub(crate) fn ttl_option(
    a: &[Bytes],
    at: &mut usize,
    ttl: &mut Ttl,
    cmd: &str,
    keep: bool,
    persist: bool,
) -> Option<Arg<()>> {
    let o = upper(&a[*at]);
    let next = a.get(*at + 1);
    match o.as_str() {
        "KEEPTTL" if keep && *ttl == Ttl::Unset => *ttl = Ttl::Keep,
        "PERSIST" if persist && *ttl == Ttl::Unset => *ttl = Ttl::Persist,
        "EX" | "PX" | "EXAT" | "PXAT" if *ttl == Ttl::Unset && next.is_some() => {
            let n = next.map_or(&[][..], |b| &b[..]);
            let unit = if o.starts_with('E') { 1000 } else { 1 };
            let ms = match ttl_amount(n, unit, cmd) {
                Ok(ms) => ms,
                Err(f) => return Some(Err(f)),
            };
            *ttl = if o.ends_with("AT") {
                Ttl::At(ms)
            } else {
                Ttl::In(ms)
            };
            *at += 1;
        }
        _ => return None,
    }
    *at += 1;
    Some(Ok(()))
}

/// `SET k v [NX|XX] [GET] [EX|PX|EXAT|PXAT|KEEPTTL]`.
fn set<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (mut nx, mut xx, mut getf) = (false, false, false);
    let mut ttl = Ttl::Unset;
    let mut at = 3;
    while at < a.len() {
        if let Some(r) = ttl_option(a, &mut at, &mut ttl, "set", true, false) {
            tri!(r);
            continue;
        }
        match upper(&a[at]).as_str() {
            "NX" if !xx => nx = true,
            "XX" if !nx => xx = true,
            "GET" => getf = true,
            _ => return Ok(reply::syntax()),
        }
        at += 1;
    }
    let (live, raw) = cx.load(&a[1])?;
    let old = match &live {
        Some(Meta {
            body: b @ (Body::Str(_) | Body::Chunked { .. }),
            ..
        }) if getf => cx.string_of(b.clone())?,
        Some(_) if getf => return Ok(reply::wrongtype()),
        _ => None,
    };
    let go = if nx {
        live.is_none()
    } else if xx {
        live.is_some()
    } else {
        true
    };
    if go {
        let exp = ttl.resolve(cx.now, live.as_ref().and_then(|m| m.exp));
        cx.put_string(&a[1], raw.as_ref(), a[2].clone(), exp)?;
    }
    Ok(if getf {
        opt_bulk(old)
    } else if go {
        Frame::ok()
    } else {
        Frame::null()
    })
}

/// `SETNX k v`.
fn setnx<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (live, raw) = cx.load(&a[1])?;
    if live.is_some() {
        return Ok(Frame::Integer(0));
    }
    cx.put_string(&a[1], raw.as_ref(), a[2].clone(), None)?;
    Ok(Frame::Integer(1))
}

/// `SETEX k s v`, `PSETEX k ms v`.
fn setex<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], unit: i64, name: &str) -> Out {
    let ms = tri!(ttl_amount(&a[2], unit, name));
    let raw = cx.raw(&a[1])?;
    let exp = Some(cx.now.saturating_add(ms));
    cx.put_string(&a[1], raw.as_ref(), a[3].clone(), exp)?;
    Ok(Frame::ok())
}

/// `GETSET k v`: the expiry is dropped.
fn getset<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.string(&a[1])?);
    cx.put_string(&a[1], s.raw.as_ref(), a[2].clone(), None)?;
    Ok(opt_bulk(s.value))
}

/// `GETDEL k`.
fn getdel<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.string(&a[1])?);
    if s.value.is_some() {
        cx.write(&a[1], s.raw.as_ref(), None)?;
    }
    Ok(opt_bulk(s.value))
}

/// `GETEX k [EX|PX|EXAT|PXAT n|PERSIST]`: a missing key or another type
/// answers before a bad amount does.
fn getex<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut ttl = Ttl::Unset;
    let mut bad = None;
    let mut at = 2;
    while at < a.len() {
        let o = upper(&a[at]);
        let free = ttl == Ttl::Unset && bad.is_none();
        match o.as_str() {
            "PERSIST" if free => {
                ttl = Ttl::Persist;
                at += 1;
            }
            "EX" | "PX" | "EXAT" | "PXAT" if free && at + 1 < a.len() => {
                let unit = if o.starts_with('E') { 1000 } else { 1 };
                match ttl_amount(&a[at + 1], unit, "getex") {
                    Ok(ms) if o.ends_with("AT") => ttl = Ttl::At(ms),
                    Ok(ms) => ttl = Ttl::In(ms),
                    Err(f) => bad = Some(f),
                }
                at += 2;
            }
            _ => return Ok(reply::syntax()),
        }
    }
    let s = tri!(cx.string(&a[1])?);
    let Some(v) = s.value.clone() else {
        return Ok(Frame::null());
    };
    if let Some(f) = bad {
        return Ok(f);
    }
    match ttl {
        Ttl::Unset => {}
        Ttl::Persist => {
            if s.exp.is_some() {
                cx.put_string(&a[1], s.raw.as_ref(), v.clone(), None)?;
            }
        }
        t => {
            let exp = t.resolve(cx.now, s.exp);
            if exp.is_some_and(|e| e <= cx.now) {
                cx.write(&a[1], s.raw.as_ref(), None)?;
            } else {
                cx.put_string(&a[1], s.raw.as_ref(), v.clone(), exp)?;
            }
        }
    }
    Ok(Frame::Bulk(v))
}

/// `MGET k ...`: a key of another type reads as nil.
fn mget<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut out = Vec::with_capacity(a.len() - 1);
    for k in &a[1..] {
        out.push(match cx.live(k)?.map(|m| m.body) {
            Some(b) => cx.string_of(b)?.map_or_else(Frame::null, Frame::Bulk),
            None => Frame::null(),
        });
    }
    Ok(Frame::Array(out))
}

/// `MSET k v [k v ...]`, `MSETNX`: every key a string without expiry.
fn mset<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], nx: bool) -> Out {
    if a.len().is_multiple_of(2) {
        return Ok(reply::arity(if nx { "msetnx" } else { "mset" }));
    }
    if nx {
        for c in a[1..].chunks(2) {
            if cx.live(&c[0])?.is_some() {
                return Ok(Frame::Integer(0));
            }
        }
    }
    for c in a[1..].chunks(2) {
        let raw = cx.raw(&c[0])?;
        cx.put_string(&c[0], raw.as_ref(), c[1].clone(), None)?;
    }
    Ok(if nx { Frame::Integer(1) } else { Frame::ok() })
}

fn too_big() -> Frame {
    reply::value_too_big(MAX_VALUE)
}

/// `APPEND k v`.
fn append<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.string_lazy(&a[1])?);
    let len = s.len();
    if len + a[2].len() > MAX_VALUE {
        return Ok(too_big());
    }
    if !s.exists() && a[2].is_empty() {
        cx.put_string(&a[1], s.raw.as_ref(), Bytes::new(), None)?;
        return Ok(Frame::Integer(0));
    }
    let n = cx.string_write_at(&a[1], &s, len, &a[2])?;
    Ok(Frame::Integer(n as i64))
}

/// `STRLEN k`.
fn strlen<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.string_lazy(&a[1])?);
    Ok(Frame::Integer(s.len() as i64))
}

/// `INCR`, `DECR`, `INCRBY`, `DECRBY`: the expiry is kept.
fn incrby<B: Batch>(cx: &mut Cx<'_, B>, key: &Bytes, by: i64) -> Out {
    let s = tri!(cx.string(key)?);
    let cur = match &s.value {
        None => 0,
        Some(v) => match parse_i64(v) {
            Some(n) => n,
            None => return Ok(reply::not_int()),
        },
    };
    let Some(n) = cur.checked_add(by) else {
        return Ok(Frame::err("increment or decrement would overflow"));
    };
    cx.put_string(key, s.raw.as_ref(), Bytes::from(n.to_string()), s.exp)?;
    Ok(Frame::Integer(n))
}

/// `INCRBYFLOAT k f`.
fn incrbyfloat<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(cx.string(&a[1])?);
    let Some(by) = parse_f64(&a[2]) else {
        return Ok(reply::not_float());
    };
    let cur = match &s.value {
        None => 0.0,
        Some(v) => match parse_f64(v) {
            Some(n) => n,
            None => return Ok(reply::not_float()),
        },
    };
    let n = cur + by;
    if n.is_nan() || n.is_infinite() {
        return Ok(Frame::err("increment would produce NaN or Infinity"));
    }
    let text = Bytes::from(ld2string_human(n));
    cx.put_string(&a[1], s.raw.as_ref(), text.clone(), s.exp)?;
    Ok(Frame::Bulk(text))
}

/// Redis's range rule for `GETRANGE` (`start`, `end` inclusive).
fn byte_range(len: i64, mut start: i64, mut end: i64) -> Option<(usize, usize)> {
    if start < 0 && end < 0 && start > end {
        return None;
    }
    if start < 0 {
        start = start.saturating_add(len);
    }
    if end < 0 {
        end = end.saturating_add(len);
    }
    start = start.max(0);
    end = end.max(0);
    if end >= len {
        end = len - 1;
    }
    if start > end || len == 0 {
        return None;
    }
    Some((start as usize, end as usize))
}

/// `GETRANGE k start end` (and `SUBSTR`).
fn getrange<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (s, e) = (tri!(int(&a[2])), tri!(int(&a[3])));
    let st = tri!(cx.string_lazy(&a[1])?);
    Ok(match byte_range(st.len() as i64, s, e) {
        Some((s, e)) => Frame::Bulk(Bytes::from(cx.string_bytes(&st, s, e + 1)?)),
        None => Frame::Bulk(Bytes::new()),
    })
}

/// `SETRANGE k offset v`.
fn setrange<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let off = tri!(int(&a[2]));
    if off < 0 {
        return Ok(Frame::err("offset is out of range"));
    }
    let s = tri!(cx.string_lazy(&a[1])?);
    if a[3].is_empty() {
        return Ok(Frame::Integer(s.len() as i64));
    }
    if off.saturating_add(a[3].len() as i64) > PROTO_MAX {
        return Ok(Frame::err(
            "string exceeds maximum allowed size (proto-max-bulk-len)",
        ));
    }
    let off = off as usize;
    let end = off + a[3].len();
    if end > MAX_VALUE {
        return Ok(too_big());
    }
    let n = cx.string_write_at(&a[1], &s, off, &a[3])?;
    Ok(Frame::Integer(n as i64))
}

/// A bit offset: 0 to 2^32 - 1.
fn bit_offset(a: &[u8]) -> Arg<u64> {
    match parse_i64(a) {
        Some(n) if (0..(1i64 << 32)).contains(&n) => Ok(n.unsigned_abs()),
        _ => Err(Frame::err("bit offset is not an integer or out of range")),
    }
}

/// `SETBIT k offset 0|1`.
fn setbit<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let off = tri!(bit_offset(&a[2]));
    let bit = match &a[3][..] {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return Ok(Frame::err("bit is not an integer or out of range")),
    };
    let byte = (off >> 3) as usize;
    if byte >= MAX_VALUE {
        return Ok(too_big());
    }
    let s = tri!(cx.string_lazy(&a[1])?);
    let cur = cx.string_bytes(&s, byte, byte + 1)?[0];
    let shift = 7 - (off & 7) as u8;
    let old = (cur >> shift) & 1;
    let new = (cur & !(1 << shift)) | (bit << shift);
    cx.string_write_at(&a[1], &s, byte, &[new])?;
    Ok(Frame::Integer(i64::from(old)))
}

/// `GETBIT k offset`.
fn getbit<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let off = tri!(bit_offset(&a[2]));
    let s = tri!(cx.string_lazy(&a[1])?);
    let byte = (off >> 3) as usize;
    let cur = cx.string_bytes(&s, byte, byte + 1)?[0];
    Ok(Frame::Integer(i64::from((cur >> (7 - (off & 7))) & 1)))
}

/// Redis's start/end rule for `BITCOUNT` and `BITPOS` over `len` units.
fn unit_range(len: i64, mut s: i64, mut e: i64) -> Option<(i64, i64)> {
    if s < 0 {
        s = s.saturating_add(len);
    }
    if e < 0 {
        e = e.saturating_add(len);
    }
    s = s.max(0);
    e = e.max(0);
    if e >= len {
        e = len - 1;
    }
    (s <= e && len > 0).then_some((s, e))
}

fn bit_at(v: &[u8], i: i64) -> u8 {
    let i = i as usize;
    (v[i >> 3] >> (7 - (i & 7))) & 1
}

/// `BITCOUNT k [start end [BYTE|BIT]]`.
fn bitcount<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut range = None;
    let mut bits = false;
    match a.len() {
        2 => {}
        4 | 5 => {
            range = Some((tri!(int(&a[2])), tri!(int(&a[3]))));
            if a.len() == 5 {
                match upper(&a[4]).as_str() {
                    "BIT" => bits = true,
                    "BYTE" => {}
                    _ => return Ok(reply::syntax()),
                }
            }
        }
        _ => return Ok(reply::syntax()),
    }
    let s = tri!(cx.string(&a[1])?);
    let v = s.value.unwrap_or_default();
    let len = v.len() as i64;
    let ones = |b: &u8| i64::from(b.count_ones());
    let count = match range {
        None => v.iter().map(ones).sum(),
        // Both ends negative, start after end: nothing.
        Some((s, e)) if s < 0 && e < 0 && s > e => 0,
        Some((s, e)) if bits => unit_range(len * 8, s, e)
            .map_or(0, |(s, e)| (s..=e).map(|i| i64::from(bit_at(&v, i))).sum()),
        Some((s, e)) => unit_range(len, s, e).map_or(0, |(s, e)| {
            v[s as usize..=e as usize].iter().map(ones).sum()
        }),
    };
    Ok(Frame::Integer(count))
}

/// `BITPOS k bit [start [end [BYTE|BIT]]]`.
fn bitpos<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let bit = match &a[2][..] {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return Ok(Frame::err("The bit argument must be 1 or 0.")),
    };
    if a.len() > 6 {
        return Ok(reply::syntax());
    }
    let start = match a.get(3) {
        Some(x) => Some(tri!(int(x))),
        None => None,
    };
    let end = match a.get(4) {
        Some(x) => Some(tri!(int(x))),
        None => None,
    };
    let bits = match a.get(5).map(|x| upper(x)).as_deref() {
        None | Some("BYTE") => false,
        Some("BIT") => true,
        _ => return Ok(reply::syntax()),
    };
    let s = tri!(cx.string(&a[1])?);
    let Some(v) = s.value else {
        return Ok(Frame::Integer(if bit == 1 { -1 } else { 0 }));
    };
    let blen = v.len() as i64;
    let units = if bits { blen * 8 } else { blen };
    let Some((s, e)) = unit_range(units, start.unwrap_or(0), end.unwrap_or(-1)) else {
        return Ok(Frame::Integer(-1));
    };
    let (bs, be) = if bits { (s, e) } else { (s * 8, e * 8 + 7) };
    for i in bs..=be {
        if bit_at(&v, i) == bit {
            return Ok(Frame::Integer(i));
        }
    }
    // A clear bit past the value, with no end given: the first bit after
    // the value.
    if bit == 0 && end.is_none() {
        return Ok(Frame::Integer(be + 1));
    }
    Ok(Frame::Integer(-1))
}

/// `BITOP AND|OR|XOR|NOT dest k...`.
fn bitop<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let op = upper(&a[1]);
    if !matches!(op.as_str(), "AND" | "OR" | "XOR" | "NOT") {
        return Ok(reply::syntax());
    }
    if op == "NOT" && a.len() != 4 {
        return Ok(Frame::err(
            "BITOP NOT must be called with a single source key.",
        ));
    }
    let mut srcs = Vec::new();
    for k in &a[3..] {
        let s = tri!(cx.string(k)?);
        srcs.push(s.value.unwrap_or_default());
    }
    let max = srcs.iter().map(Bytes::len).max().unwrap_or(0);
    let mut out = vec![0u8; max];
    for (i, o) in out.iter_mut().enumerate() {
        let at = |s: &Bytes| s.get(i).copied().unwrap_or(0);
        *o = match op.as_str() {
            "NOT" => !at(&srcs[0]),
            "AND" => srcs.iter().fold(0xFF, |acc, s| acc & at(s)),
            "OR" => srcs.iter().fold(0, |acc, s| acc | at(s)),
            _ => srcs.iter().fold(0, |acc, s| acc ^ at(s)),
        };
    }
    let raw = cx.raw(&a[2])?;
    if out.is_empty() {
        if raw.is_some() {
            cx.write(&a[2], raw.as_ref(), None)?;
        }
        return Ok(Frame::Integer(0));
    }
    let n = out.len();
    cx.put_string(&a[2], raw.as_ref(), Bytes::from(out), None)?;
    Ok(Frame::Integer(n as i64))
}

/// `LCS k1 k2 [LEN] [IDX] [MINMATCHLEN n] [WITHMATCHLEN]`.
fn lcs<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (mut len_only, mut idx, mut withlen) = (false, false, false);
    let mut minlen = 0i64;
    let mut i = 3;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "LEN" => len_only = true,
            "IDX" => idx = true,
            "WITHMATCHLEN" => withlen = true,
            "MINMATCHLEN" if i + 1 < a.len() => {
                minlen = tri!(int(&a[i + 1])).max(0);
                i += 1;
            }
            _ => return Ok(reply::syntax()),
        }
        i += 1;
    }
    if len_only && idx {
        return Ok(Frame::err(
            "If you want both the length and indexes, please just use IDX.",
        ));
    }
    let mut vals = Vec::new();
    for k in &a[1..3] {
        match cx.live(k)?.map(|m| m.body) {
            Some(b @ (Body::Str(_) | Body::Chunked { .. })) => {
                vals.push(cx.string_of(b)?.unwrap_or_default());
            }
            Some(_) => {
                return Ok(Frame::err("The specified keys must contain string values"));
            }
            None => vals.push(Bytes::new()),
        }
    }
    let (x, y) = (&vals[0], &vals[1]);
    let (n, m) = (x.len(), y.len());
    // The work table: 4 bytes a cell, at most 64 MiB.
    if (n + 1).saturating_mul(m + 1) > 16 * 1024 * 1024 {
        return Ok(Frame::err(
            "Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len",
        ));
    }
    let mut dp = vec![0u32; (n + 1) * (m + 1)];
    let at = |i: usize, j: usize| i * (m + 1) + j;
    for i in 1..=n {
        for j in 1..=m {
            dp[at(i, j)] = if x[i - 1] == y[j - 1] {
                dp[at(i - 1, j - 1)] + 1
            } else {
                dp[at(i - 1, j)].max(dp[at(i, j - 1)])
            };
        }
    }
    let total = dp[at(n, m)];
    if len_only {
        return Ok(Frame::Integer(i64::from(total)));
    }
    // Walk back from the end, collecting the string and the ranges.
    let mut out = vec![0u8; total as usize];
    let mut matches = Vec::new();
    let (mut i, mut j, mut k) = (n, m, total as usize);
    // a_start, a_end, b_start, b_end.
    let mut cur: Option<(usize, usize, usize, usize)> = None;
    let push = |c: Option<(usize, usize, usize, usize)>, ms: &mut Vec<_>| {
        if let Some((as_, ae, bs, be)) = c {
            let l = ae - as_ + 1;
            if l as i64 >= minlen {
                ms.push((as_, ae, bs, be, l));
            }
        }
    };
    while i > 0 && j > 0 {
        if x[i - 1] == y[j - 1] {
            k -= 1;
            out[k] = x[i - 1];
            let (ai, bj) = (i - 1, j - 1);
            match cur {
                Some((as_, ae, bs, be)) if as_ == ai + 1 && bs == bj + 1 => {
                    cur = Some((ai, ae, bj, be));
                }
                c => {
                    push(c, &mut matches);
                    cur = Some((ai, ai, bj, bj));
                }
            }
            i -= 1;
            j -= 1;
        } else if dp[at(i - 1, j)] > dp[at(i, j - 1)] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    push(cur, &mut matches);
    if !idx {
        return Ok(Frame::Bulk(Bytes::from(out)));
    }
    let to_i = |u: usize| Frame::Integer(u as i64);
    let list = matches
        .into_iter()
        .map(|(as_, ae, bs, be, l)| {
            let mut item = vec![
                Frame::Array(vec![to_i(as_), to_i(ae)]),
                Frame::Array(vec![to_i(bs), to_i(be)]),
            ];
            if withlen {
                item.push(to_i(l));
            }
            Frame::Array(item)
        })
        .collect();
    Ok(Frame::Map(vec![
        (Frame::bulk("matches"), Frame::Array(list)),
        (Frame::bulk("len"), Frame::Integer(i64::from(total))),
    ]))
}

/// One `BITFIELD` field type.
#[derive(Debug, Clone, Copy)]
struct Bf {
    signed: bool,
    bits: u32,
}

fn bf_type(a: &[u8]) -> Arg<Bf> {
    let bad = || {
        Frame::err(
            "Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
        )
    };
    let (&first, rest) = a.split_first().ok_or_else(bad)?;
    let signed = match first {
        b'i' | b'I' => true,
        b'u' | b'U' => false,
        _ => return Err(bad()),
    };
    let bits = parse_i64(rest)
        .and_then(|b| u32::try_from(b).ok())
        .ok_or_else(bad)?;
    if bits == 0 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(bad());
    }
    Ok(Bf { signed, bits })
}

fn bf_offset(a: &[u8], t: Bf) -> Arg<u64> {
    let (mult, body) = match a.first() {
        Some(b'#') => (u64::from(t.bits), &a[1..]),
        _ => (1, a),
    };
    match parse_i64(body) {
        Some(n) if n >= 0 => {
            let off = n.unsigned_abs().saturating_mul(mult);
            if off.saturating_add(u64::from(t.bits)) > (512 * 1024 * 1024 * 8) {
                return Err(Frame::err("bit offset is not an integer or out of range"));
            }
            Ok(off)
        }
        _ => Err(Frame::err("bit offset is not an integer or out of range")),
    }
}

fn bf_get(v: &[u8], off: u64, t: Bf) -> i64 {
    let mut u: u64 = 0;
    for i in 0..u64::from(t.bits) {
        let p = off + i;
        let b = v
            .get((p >> 3) as usize)
            .map_or(0, |b| (b >> (7 - (p & 7))) & 1);
        u = (u << 1) | u64::from(b);
    }
    if t.signed && t.bits < 64 && (u >> (t.bits - 1)) & 1 == 1 {
        (u | (u64::MAX << t.bits)) as i64
    } else {
        u as i64
    }
}

fn bf_set(v: &mut Vec<u8>, off: u64, t: Bf, val: i64) {
    let need = (off + u64::from(t.bits)).div_ceil(8) as usize;
    if v.len() < need {
        v.resize(need, 0);
    }
    let u = val as u64;
    for i in 0..u64::from(t.bits) {
        let p = off + i;
        let bit = (u >> (u64::from(t.bits) - 1 - i)) & 1;
        let byte = (p >> 3) as usize;
        let shift = 7 - (p & 7);
        v[byte] = (v[byte] & !(1 << shift)) | ((bit as u8) << shift);
    }
}

/// Overflow handling of `BITFIELD ... SET|INCRBY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

fn bf_incr(old: i64, by: i64, t: Bf, ov: Overflow) -> Option<i64> {
    let (min, max): (i128, i128) = if t.signed {
        (-(1i128 << (t.bits - 1)), (1i128 << (t.bits - 1)) - 1)
    } else {
        (0, (1i128 << t.bits) - 1)
    };
    let sum = i128::from(old) + i128::from(by);
    if sum >= min && sum <= max {
        return i64::try_from(sum).ok();
    }
    match ov {
        Overflow::Fail => None,
        Overflow::Sat => i64::try_from(if sum > max { max } else { min }).ok(),
        Overflow::Wrap => {
            let m = 1i128 << t.bits;
            let w = if t.signed {
                (sum - min).rem_euclid(m) + min
            } else {
                sum.rem_euclid(m)
            };
            i64::try_from(w).ok()
        }
    }
}

/// `BITFIELD k [GET t o] [SET t o v] [INCRBY t o n] [OVERFLOW ...]` (and
/// `BITFIELD_RO` with `GET` only).
fn bitfield<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], ro: bool) -> Out {
    enum Op {
        Get(Bf, u64),
        Set(Bf, u64, i64, Overflow),
        Incr(Bf, u64, i64, Overflow),
    }
    let ro_only = || Frame::err("BITFIELD_RO only supports the GET subcommand");
    let mut ops = Vec::new();
    let mut ov = Overflow::Wrap;
    let mut i = 2;
    while i < a.len() {
        let o = upper(&a[i]);
        let left = a.len() - i - 1;
        match o.as_str() {
            "GET" if left >= 2 => {
                let t = tri!(bf_type(&a[i + 1]));
                ops.push(Op::Get(t, tri!(bf_offset(&a[i + 2], t))));
                i += 3;
            }
            "SET" | "INCRBY" if left >= 3 => {
                if ro {
                    return Ok(ro_only());
                }
                let t = tri!(bf_type(&a[i + 1]));
                let off = tri!(bf_offset(&a[i + 2], t));
                let v = tri!(int(&a[i + 3]));
                // A write may grow the value: within SprigDB's limit.
                if (off + u64::from(t.bits)).div_ceil(8) > MAX_VALUE as u64 {
                    return Ok(too_big());
                }
                ops.push(if o == "SET" {
                    Op::Set(t, off, v, ov)
                } else {
                    Op::Incr(t, off, v, ov)
                });
                i += 4;
            }
            "OVERFLOW" if left >= 1 => {
                ov = match upper(&a[i + 1]).as_str() {
                    "WRAP" => Overflow::Wrap,
                    "SAT" => Overflow::Sat,
                    "FAIL" => Overflow::Fail,
                    _ => return Ok(Frame::err("Invalid OVERFLOW type specified")),
                };
                i += 2;
            }
            _ => return Ok(reply::syntax()),
        }
    }
    // Each operation reads only the bytes it covers, over what this command
    // already wrote; the written bytes go back in runs at the end.
    // Every SET and INCRBY grows the value to cover its field, even one
    // that fails on overflow.
    let grow_to = ops
        .iter()
        .filter_map(|op| match op {
            Op::Get(..) => None,
            Op::Set(t, off, _, _) | Op::Incr(t, off, _, _) => {
                Some((off + u64::from(t.bits)).div_ceil(8) as usize)
            }
        })
        .max()
        .unwrap_or(0);
    let s = tri!(cx.string_lazy(&a[1])?);
    let mut wrote: std::collections::BTreeMap<usize, u8> = std::collections::BTreeMap::new();
    let mut out = Vec::new();
    for op in ops {
        let (t, off) = match op {
            Op::Get(t, off) | Op::Set(t, off, _, _) | Op::Incr(t, off, _, _) => (t, off),
        };
        let lo = (off / 8) as usize;
        let hi = (off + u64::from(t.bits)).div_ceil(8) as usize;
        let mut buf = cx.string_bytes(&s, lo, hi)?;
        for (i, b) in wrote.range(lo..hi) {
            buf[i - lo] = *b;
        }
        let rel = off - lo as u64 * 8;
        let next = match op {
            Op::Get(..) => {
                out.push(Frame::Integer(bf_get(&buf, rel, t)));
                continue;
            }
            // A value out of the type's range wraps, saturates or fails as
            // an increment from 0 would.
            Op::Set(_, _, val, ov) => bf_incr(0, val, t, ov).map(|nv| (nv, bf_get(&buf, rel, t))),
            Op::Incr(_, _, by, ov) => bf_incr(bf_get(&buf, rel, t), by, t, ov).map(|nv| (nv, nv)),
        };
        match next {
            Some((nv, reply)) => {
                bf_set(&mut buf, rel, t, nv);
                for (i, b) in buf.iter().enumerate() {
                    wrote.insert(lo + i, *b);
                }
                out.push(Frame::Integer(reply));
            }
            None => out.push(Frame::null()),
        }
    }
    let mut runs: Vec<(usize, Vec<u8>)> = Vec::new();
    for (i, b) in wrote {
        match runs.last_mut() {
            Some((start, v)) if *start + v.len() == i => v.push(b),
            _ => runs.push((i, vec![b])),
        }
    }
    for (start, v) in runs {
        let cur = tri!(cx.string_lazy(&a[1])?);
        cx.string_write_at(&a[1], &cur, start, &v)?;
    }
    let cur = tri!(cx.string_lazy(&a[1])?);
    if cur.len() < grow_to {
        cx.string_write_at(&a[1], &cur, grow_to - 1, &[0])?;
    }
    Ok(Frame::Array(out))
}
