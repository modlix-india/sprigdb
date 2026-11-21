//! The keyspace: existence, types, expiry, renames and copies, and the
//! walks (`KEYS`, `SCAN`, `RANDOMKEY`, `DBSIZE`).

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::{glob, reply};

use super::{Cx, Out, int, tri, upper};
use crate::layout::{
    Body, HashBody, KIND_META, ListBody, Meta, SetBody, Space, Type, ZBody, get_str,
};
use crate::store::{Batch, StoreError};

/// Runs a keyspace command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "del" | "unlink" => del(cx, a),
        "exists" | "touch" => exists(cx, a),
        "type" => type_(cx, a),
        "expire" => expire(cx, a, 1000, false),
        "pexpire" => expire(cx, a, 1, false),
        "expireat" => expire(cx, a, 1000, true),
        "pexpireat" => expire(cx, a, 1, true),
        "ttl" => ttl(cx, a, false, false),
        "pttl" => ttl(cx, a, true, false),
        "expiretime" => ttl(cx, a, false, true),
        "pexpiretime" => ttl(cx, a, true, true),
        "persist" => persist(cx, a),
        "rename" => rename(cx, a, false),
        "renamenx" => rename(cx, a, true),
        "copy" => copy(cx, a),
        "move" => move_(cx, a),
        "keys" => keys(cx, a),
        "scan" => scan(cx, a),
        "randomkey" => randomkey(cx),
        "dbsize" => dbsize(cx),
        "flushdb" | "flushall" => flush(cx, name, a),
        "object" => object(cx, a),
        "sort" => sort(cx, a, false),
        "sort_ro" => sort(cx, a, true),
        "time" => {
            let us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_micros());
            Ok(Frame::Array(vec![
                Frame::bulk((us / 1_000_000).to_string()),
                Frame::bulk((us % 1_000_000).to_string()),
            ]))
        }
        // No replicas to wait for; every write is already durable locally.
        "wait" => match (int(&a[1]), int(&a[2])) {
            (Ok(_), Ok(t)) if t >= 0 => Ok(Frame::Integer(0)),
            (Ok(_), Ok(_)) => Ok(Frame::err("timeout is negative")),
            (Err(f), _) | (_, Err(f)) => Ok(f),
        },
        "waitaof" => match (int(&a[1]), int(&a[2]), int(&a[3])) {
            (Ok(_), Ok(_), Ok(t)) if t >= 0 => {
                Ok(Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)]))
            }
            (Ok(_), Ok(_), Ok(_)) => Ok(Frame::err("timeout is negative")),
            (Err(f), _, _) | (_, Err(f), _) | (_, _, Err(f)) => Ok(f),
        },
        _ => return None,
    })
}

/// `DEL k ...`, `UNLINK k ...`: a repeated key counts once.
fn del<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut n = 0;
    for k in &a[1..] {
        let (live, raw) = cx.load(k)?;
        if raw.is_some() {
            cx.write(k, raw.as_ref(), None)?;
        }
        if live.is_some() {
            n += 1;
        }
    }
    Ok(Frame::Integer(n))
}

/// `EXISTS k ...`, `TOUCH k ...`: a repeated key counts each time.
fn exists<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut n = 0;
    for k in &a[1..] {
        if cx.live(k)?.is_some() {
            n += 1;
        }
    }
    Ok(Frame::Integer(n))
}

/// `TYPE k`.
fn type_<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    Ok(Frame::simple(
        cx.live(&a[1])?.map_or("none", |m| m.kind().name()),
    ))
}

/// `EXPIRE`, `PEXPIRE` (`absolute` false) and `EXPIREAT`, `PEXPIREAT`:
/// `unit` ms per amount.
fn expire<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], unit: i64, absolute: bool) -> Out {
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for o in &a[3..] {
        match upper(o).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {
                return Ok(Frame::err(&format!(
                    "Unsupported option {}",
                    String::from_utf8_lossy(o)
                )));
            }
        }
    }
    if nx && (xx || gt || lt) {
        return Ok(Frame::err(
            "NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return Ok(Frame::err(
            "GT and LT options at the same time are not compatible",
        ));
    }
    let when = tri!(int(&a[2]));
    let name = String::from_utf8_lossy(&a[0]).to_ascii_lowercase();
    let invalid = || {
        Ok(Frame::err(&format!(
            "invalid expire time in '{name}' command"
        )))
    };
    let Some(mut at) = when.checked_mul(unit) else {
        return invalid();
    };
    if !absolute {
        let Some(t) = at.checked_add(cx.now) else {
            return invalid();
        };
        at = t;
    }
    let (live, raw) = cx.load(&a[1])?;
    let Some(mut m) = live else {
        return Ok(Frame::Integer(0));
    };
    // No expiry counts as infinite for GT and LT.
    let cur = m.exp;
    let ok = if nx {
        cur.is_none()
    } else if xx && cur.is_none() {
        false
    } else if gt {
        cur.is_some_and(|c| at > c)
    } else if lt {
        cur.is_none_or(|c| at < c)
    } else {
        true
    };
    if !ok {
        return Ok(Frame::Integer(0));
    }
    if at <= cx.now {
        cx.write(&a[1], raw.as_ref(), None)?;
    } else {
        m.exp = Some(at);
        cx.write(&a[1], raw.as_ref(), Some(&m))?;
    }
    Ok(Frame::Integer(1))
}

/// `TTL`, `PTTL` (`ms`), `EXPIRETIME`, `PEXPIRETIME` (`absolute`): -2
/// absent, -1 no expiry.
fn ttl<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], ms: bool, absolute: bool) -> Out {
    let Some(m) = cx.live(&a[1])? else {
        return Ok(Frame::Integer(-2));
    };
    let Some(exp) = m.exp else {
        return Ok(Frame::Integer(-1));
    };
    Ok(Frame::Integer(match (absolute, ms) {
        (true, true) => exp,
        (true, false) => exp / 1000,
        (false, true) => (exp - cx.now).max(0),
        (false, false) => ((exp - cx.now).max(0) + 500) / 1000,
    }))
}

/// `PERSIST k`: 1 when it removed an expiry.
fn persist<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (live, raw) = cx.load(&a[1])?;
    match live {
        Some(mut m) if m.exp.is_some() => {
            m.exp = None;
            cx.write(&a[1], raw.as_ref(), Some(&m))?;
            Ok(Frame::Integer(1))
        }
        _ => Ok(Frame::Integer(0)),
    }
}

/// `RENAME src dst`, `RENAMENX src dst`: the value, its elements and its
/// expiry move under the new name; the elements stay where they are.
fn rename<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], nx: bool) -> Out {
    let (live, raw) = cx.load(&a[1])?;
    let Some(m) = live else {
        return Ok(reply::no_such_key());
    };
    if a[1] == a[2] {
        return Ok(if nx { Frame::Integer(0) } else { Frame::ok() });
    }
    let (dlive, draw) = cx.load(&a[2])?;
    if nx && dlive.is_some() {
        return Ok(Frame::Integer(0));
    }
    // The source's metadata goes first, so its uid isn't dropped with it.
    cx.write_meta_only(&a[1], raw.as_ref())?;
    cx.write(&a[2], draw.as_ref(), Some(&m))?;
    // A hash with field lifetimes takes its hash-due entry along.
    if let Body::Hash(HashBody::Split { uid, ttl: true, .. }) = m.body
        && let Some(at) = cx.fexp_first(cx.space, uid)?
    {
        cx.b.delete(cx.space.hdue(at, &a[1]))?;
        cx.b.put(cx.space.hdue(at, &a[2]), Bytes::new())?;
    }
    Ok(if nx { Frame::Integer(1) } else { Frame::ok() })
}

impl<B: Batch> Cx<'_, B> {
    /// Deletes `key`'s metadata, scan-index and expiry entries, keeping its
    /// elements (they now belong to another key).
    fn write_meta_only(&mut self, key: &[u8], old: Option<&Meta>) -> Result<(), StoreError> {
        let Some(o) = old else {
            return Ok(());
        };
        self.touched.push(self.space.meta(key));
        self.b.delete(self.space.scan_entry(key))?;
        if let Some(e) = o.exp {
            self.b.delete(self.space.expiry(e, key))?;
        }
        self.b.delete(self.space.meta(key))
    }
}

/// `COPY src dst [DB n] [REPLACE]`.
fn copy<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let mut replace = false;
    let mut dst = cx.space;
    let mut i = 3;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "REPLACE" => replace = true,
            "DB" if i + 1 < a.len() => {
                let d = tri!(int(&a[i + 1]));
                let Some(d) = usize::try_from(d).ok().filter(|d| *d < cx.all.len()) else {
                    return Ok(Frame::err("DB index is out of range"));
                };
                dst = cx.all[d];
                i += 1;
            }
            _ => return Ok(reply::syntax()),
        }
        i += 1;
    }
    if dst == cx.space && a[1] == a[2] {
        return Ok(Frame::err("source and destination objects are the same"));
    }
    let Some(m) = cx.live(&a[1])? else {
        return Ok(Frame::Integer(0));
    };
    let (dlive, draw) = cx.load_in(dst, &a[2])?;
    if dlive.is_some() && !replace {
        return Ok(Frame::Integer(0));
    }
    // The old destination goes first, so dropping it can't remove an
    // index entry the copy writes.
    if draw.is_some() {
        cx.write_in(dst, &a[2], draw.as_ref(), None)?;
    }
    let new = cx.copy_body(cx.space, &m, dst, &a[2])?;
    cx.write_in(dst, &a[2], None, Some(&new))?;
    Ok(Frame::Integer(1))
}

/// `MOVE k db`.
fn move_<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let d = tri!(int(&a[2]));
    let Some(d) = usize::try_from(d).ok().filter(|d| *d < cx.all.len()) else {
        return Ok(Frame::err("DB index is out of range"));
    };
    if d == cx.db_index() {
        return Ok(Frame::err("source and destination objects are the same"));
    }
    let dst = cx.all[d];
    let (live, raw) = cx.load(&a[1])?;
    let Some(m) = live else {
        return Ok(Frame::Integer(0));
    };
    let (dlive, draw) = cx.load_in(dst, &a[1])?;
    if dlive.is_some() {
        return Ok(Frame::Integer(0));
    }
    if draw.is_some() {
        cx.write_in(dst, &a[1], draw.as_ref(), None)?;
    }
    let new = cx.copy_body(cx.space, &m, dst, &a[1])?;
    cx.write_in(dst, &a[1], None, Some(&new))?;
    cx.write(&a[1], raw.as_ref(), None)?;
    Ok(Frame::Integer(1))
}

/// The smallest key above every key that starts with `p`, or `None` when
/// no such key exists.
fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut v = p.to_vec();
    while let Some(last) = v.pop() {
        if last < 0xff {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

/// The metadata entries `[start, end)` of keys starting with `prefix`.
fn meta_range(space: Space, prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let base = space.prefix(KIND_META);
    let index_end = space.scan_index_range().0;
    if prefix.is_empty() {
        return (base, index_end);
    }
    let mut enc = bytes::BytesMut::new();
    crate::layout::put_str(&mut enc, prefix);
    // Without the terminator: every key starting with `prefix`.
    enc.truncate(enc.len() - 2);
    let start = [base.as_slice(), &enc].concat();
    let end = prefix_successor(&start).unwrap_or(index_end);
    (start, end)
}

/// `KEYS pattern`: a scan of the metadata, from the pattern's literal
/// prefix.
fn keys<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let pat = &a[1];
    let (mut start, end) = meta_range(cx.space, glob::literal_prefix(pat));
    let mut out = Vec::new();
    loop {
        let got = cx.b.scan(&start, &end, 1024)?;
        let Some((last, _)) = got.last() else {
            break;
        };
        start = crate::layout::after(last);
        for (k, v) in got {
            let Some(key) = cx.space.meta_user_key(&k) else {
                continue;
            };
            if !glob::matches(pat, &key) {
                continue;
            }
            let m = Meta::decode(&v).ok_or_else(|| super::corrupt("key metadata", v.len()))?;
            if !m.expired(cx.now) {
                out.push(Frame::Bulk(Bytes::from(key)));
            }
        }
    }
    Ok(Frame::Array(out))
}

/// A scan-index entry's routing hash and key.
fn parse_scan_entry(space: Space, stored: &[u8]) -> Option<(u64, Vec<u8>)> {
    let lo = space.scan_index_range().0;
    let rest = stored.strip_prefix(lo.as_slice())?;
    let (rk, key) = rest.split_at_checked(8)?;
    let rk = u64::from_be_bytes(rk.try_into().ok()?);
    let (key, tail) = get_str(key)?;
    tail.is_empty().then_some((rk, key))
}

/// The scan-index position of routing hash `rk`.
fn scan_pos(space: Space, rk: u64) -> Vec<u8> {
    let mut v = space.scan_index_range().0;
    v.extend_from_slice(&rk.to_be_bytes());
    v
}

/// `SCAN` options: `MATCH`, `COUNT` (at least 1) and, when `with_type`,
/// `TYPE`.
pub(crate) struct ScanOpts {
    pub pattern: Option<Bytes>,
    pub count: usize,
    pub kind: Option<String>,
    pub novalues: bool,
}

/// Parses the options after `a[at]`.
pub(crate) fn scan_opts(
    a: &[Bytes],
    at: usize,
    with_type: bool,
    novalues: bool,
) -> Result<ScanOpts, Frame> {
    let mut o = ScanOpts {
        pattern: None,
        count: 10,
        kind: None,
        novalues: false,
    };
    let mut i = at;
    while i < a.len() {
        let opt = upper(&a[i]);
        match opt.as_str() {
            "NOVALUES" if novalues => {
                o.novalues = true;
                i += 1;
                continue;
            }
            "NOVALUES" => return Err(Frame::err("NOVALUES option can only be used in HSCAN")),
            "MATCH" | "COUNT" | "TYPE" if i + 1 < a.len() => {}
            _ => return Err(reply::syntax()),
        }
        let v = &a[i + 1];
        match opt.as_str() {
            "MATCH" => o.pattern = Some(v.clone()),
            "COUNT" => {
                let n = int(v)?;
                if n < 1 {
                    return Err(reply::syntax());
                }
                o.count = usize::try_from(n).unwrap_or(usize::MAX);
            }
            "TYPE" if with_type => o.kind = Some(String::from_utf8_lossy(v).to_ascii_lowercase()),
            _ => return Err(reply::syntax()),
        }
        i += 2;
    }
    Ok(o)
}

/// A cursor: a decimal unsigned 64-bit number.
pub(crate) fn cursor(a: &[u8]) -> Result<u64, Frame> {
    std::str::from_utf8(a)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| Frame::err("invalid cursor"))
}

/// `SCAN cursor [MATCH p] [COUNT n] [TYPE t]`: keys in routing-hash order;
/// the cursor is the next hash to read.
fn scan<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let at = tri!(cursor(&a[1]));
    let o = tri!(scan_opts(a, 2, true, false));
    let space = cx.space;
    let (_, end) = space.scan_index_range();
    let mut got =
        cx.b.scan(&scan_pos(space, at), &end, o.count.min(100_000))?;
    let mut next = 0u64;
    if got.len() >= o.count.min(100_000)
        && let Some((last, _)) = got.last()
    {
        // Finish every key that shares the last one's hash.
        let (rk, _) = parse_scan_entry(space, last)
            .ok_or_else(|| super::corrupt("scan index", last.len()))?;
        let mut from = crate::layout::after(last);
        let hi = match rk.checked_add(1) {
            Some(n) => scan_pos(space, n),
            None => end.clone(),
        };
        loop {
            let more = cx.b.scan(&from, &hi, 1024)?;
            let done = more.len() < 1024;
            if let Some((k, _)) = more.last() {
                from = crate::layout::after(k);
            }
            got.extend(more);
            if done {
                break;
            }
        }
        next = rk.wrapping_add(1);
    }
    let mut keys = Vec::new();
    for (k, _) in &got {
        let Some((_, key)) = parse_scan_entry(space, k) else {
            continue;
        };
        if o.pattern.as_ref().is_some_and(|p| !glob::matches(p, &key)) {
            continue;
        }
        let Some(m) = cx.live(&key)? else {
            continue;
        };
        if o.kind.as_ref().is_some_and(|t| t != m.kind().name()) {
            continue;
        }
        keys.push(Frame::Bulk(Bytes::from(key)));
    }
    Ok(Frame::Array(vec![
        Frame::Bulk(Bytes::from(next.to_string())),
        Frame::Array(keys),
    ]))
}

/// A random 64-bit number (not for secrets).
pub(crate) fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(N.fetch_add(1, Ordering::Relaxed));
    h.finish()
}

/// `RANDOMKEY`: the first live key at or after a random routing hash,
/// wrapping at the end.
fn randomkey<B: Batch>(cx: &mut Cx<'_, B>) -> Out {
    let space = cx.space;
    let (lo, hi) = space.scan_index_range();
    let pivot = scan_pos(space, rand_u64());
    for (mut start, end) in [(pivot.clone(), hi), (lo, pivot)] {
        loop {
            let got = cx.b.scan(&start, &end, 64)?;
            let Some((last, _)) = got.last() else {
                break;
            };
            start = crate::layout::after(last);
            for (k, _) in got {
                if let Some((_, key)) = parse_scan_entry(space, &k)
                    && cx.live(&key)?.is_some()
                {
                    return Ok(Frame::Bulk(Bytes::from(key)));
                }
            }
        }
    }
    Ok(Frame::null())
}

/// Counts the entries in `[start, end)`.
fn count_range<B: Batch>(
    cx: &mut Cx<'_, B>,
    mut start: Vec<u8>,
    end: &[u8],
) -> Result<i64, StoreError> {
    let mut n = 0;
    loop {
        let got = cx.b.scan(&start, end, 4096)?;
        let Some((last, _)) = got.last() else {
            return Ok(n);
        };
        start = crate::layout::after(last);
        n += got.len() as i64;
    }
}

/// `DBSIZE`: the scan index's entries, less the keys whose expiry has
/// passed.
fn dbsize<B: Batch>(cx: &mut Cx<'_, B>) -> Out {
    let space = cx.space;
    let (lo, hi) = space.scan_index_range();
    let all = count_range(cx, lo, &hi)?;
    let (dlo, dhi) = space.expiry_due(cx.now);
    let due = count_range(cx, dlo, &dhi)?;
    Ok(Frame::Integer((all - due).max(0)))
}

/// `FLUSHDB [ASYNC|SYNC]`, `FLUSHALL [ASYNC|SYNC]`.
fn flush<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Out {
    match a.len() {
        1 => {}
        2 if matches!(upper(&a[1]).as_str(), "ASYNC" | "SYNC") => {}
        _ => return Ok(reply::syntax()),
    }
    if name == "flushall" {
        let all = cx.all;
        for s in all.iter().copied() {
            cx.flush_space(s)?;
        }
    } else {
        cx.flush_space(cx.space)?;
    }
    Ok(Frame::ok())
}

/// What `OBJECT ENCODING` answers for a value.
#[must_use]
pub fn encoding(m: &Meta) -> &'static str {
    match &m.body {
        Body::Str(s) => {
            if s.len() <= 20 && sprig_proto_redis::num::parse_i64(s).is_some() {
                "int"
            } else if s.len() <= 44 {
                "embstr"
            } else {
                "raw"
            }
        }
        Body::List(ListBody::Inline(_))
        | Body::Hash(HashBody::Inline(_))
        | Body::Set(SetBody::Inline { ints: false, .. })
        | Body::ZSet(ZBody::Inline(_)) => "listpack",
        Body::List(ListBody::Split { .. }) => "quicklist",
        Body::Chunked { .. } => "raw",
        Body::Hash(HashBody::Split { .. }) | Body::Set(SetBody::Split { .. }) => "hashtable",
        Body::Set(SetBody::Inline { ints: true, .. }) => "intset",
        Body::ZSet(ZBody::Split { .. }) => "skiplist",
    }
}

const OBJECT_HELP: &[&str] = &[
    "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "ENCODING <key>",
    "    Return the kind of internal representation used in order to store the value",
    "    associated with a <key>.",
    "FREQ <key>",
    "    Return the access frequency index of the <key>. The returned integer is",
    "    proportional to the logarithm of the recent access frequency of the key.",
    "IDLETIME <key>",
    "    Return the idle time of the <key>, that is the approximated number of",
    "    seconds elapsed since the last access to the key.",
    "REFCOUNT <key>",
    "    Return the number of references of the value associated with the specified",
    "    <key>.",
    "HELP",
    "    Print this help.",
];

/// `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ k`, `OBJECT HELP`.
fn object<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let sub = upper(&a[1]);
    let arity = |n: &str| reply::arity(&format!("object|{}", n.to_ascii_lowercase()));
    match sub.as_str() {
        "HELP" if a.len() == 2 => {
            return Ok(Frame::Array(
                OBJECT_HELP.iter().map(|l| Frame::simple(l)).collect(),
            ));
        }
        "HELP" => return Ok(arity("help")),
        "ENCODING" | "REFCOUNT" | "IDLETIME" | "FREQ" if a.len() != 3 => {
            return Ok(arity(&sub));
        }
        "ENCODING" | "REFCOUNT" | "IDLETIME" | "FREQ" => {}
        _ => return Ok(reply::unknown_sub("object", &a[1])),
    }
    let Some(m) = cx.live(&a[2])? else {
        return Ok(Frame::null());
    };
    Ok(match sub.as_str() {
        "ENCODING" => Frame::bulk(encoding(&m)),
        // Redis shares the objects of the integers 0 to 9,999.
        "REFCOUNT" => Frame::Integer(match &m.body {
            Body::Str(s)
                if s.len() <= 4
                    && sprig_proto_redis::num::parse_i64(s)
                        .is_some_and(|n| (0..10_000).contains(&n)) =>
            {
                i64::from(i32::MAX)
            }
            _ => 1,
        }),
        "IDLETIME" => Frame::Integer(0),
        _ => Frame::err(
            "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.",
        ),
    })
}

/// The value a `SORT` pattern names for `elem`: `#` is the element; the
/// first `*` is replaced by it, and `key->field` reads a hash field.
fn by_lookup<B: Batch>(
    cx: &mut Cx<'_, B>,
    pat: &[u8],
    elem: &Bytes,
) -> Result<Option<Bytes>, StoreError> {
    if pat == b"#" {
        return Ok(Some(elem.clone()));
    }
    let Some(star) = pat.iter().position(|c| *c == b'*') else {
        return Ok(None);
    };
    let rest = &pat[star + 1..];
    let (suffix, field) = match rest.windows(2).position(|w| w == b"->") {
        Some(f) if f + 2 < rest.len() => (&rest[..f], Some(&rest[f + 2..])),
        _ => (rest, None),
    };
    let key = [&pat[..star], &elem[..], suffix].concat();
    Ok(match (cx.live(&key)?.map(|m| m.body), field) {
        (Some(b @ (Body::Str(_) | Body::Chunked { .. })), None) => cx.string_of(b)?,
        (Some(Body::Hash(h)), Some(f)) => cx.hash_get(&h, f)?,
        _ => None,
    })
}

/// `SORT k [BY pattern] [LIMIT o c] [GET pattern ...] [ASC|DESC] [ALPHA]
/// [STORE d]` and `SORT_RO`.
fn sort<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], ro: bool) -> Out {
    let (mut desc, mut alpha, mut nosort) = (false, false, false);
    let mut by: Option<Bytes> = None;
    let mut gets: Vec<Bytes> = Vec::new();
    let mut limit: Option<(i64, i64)> = None;
    let mut store: Option<Bytes> = None;
    let mut i = 2;
    while i < a.len() {
        let left = a.len() - i - 1;
        match upper(&a[i]).as_str() {
            "ASC" => desc = false,
            "DESC" => desc = true,
            "ALPHA" => alpha = true,
            "LIMIT" if left >= 2 => {
                limit = Some((tri!(int(&a[i + 1])), tri!(int(&a[i + 2]))));
                i += 2;
            }
            "STORE" if left >= 1 && !ro => {
                store = Some(a[i + 1].clone());
                i += 1;
            }
            "BY" if left >= 1 => {
                // A pattern without `*` names no key: nothing to sort by.
                if a[i + 1].contains(&b'*') {
                    by = Some(a[i + 1].clone());
                } else {
                    nosort = true;
                }
                i += 1;
            }
            "GET" if left >= 1 => {
                gets.push(a[i + 1].clone());
                i += 1;
            }
            _ => return Ok(reply::syntax()),
        }
        i += 1;
    }
    let mut items: Vec<Bytes> = match cx.live(&a[1])?.map(|m| m.body) {
        None => Vec::new(),
        Some(Body::List(l)) => {
            let mut v = cx.list_all(&l)?;
            if nosort && desc {
                v.reverse();
            }
            v
        }
        Some(Body::Set(s)) => cx.set_all(&s)?,
        Some(Body::ZSet(z)) => cx
            .z_all(&z, nosort && desc)?
            .into_iter()
            .map(|(_, m)| m)
            .collect(),
        Some(_) => return Ok(reply::wrongtype()),
    };
    if !nosort {
        // Each element with what it sorts by.
        let mut keyed = Vec::with_capacity(items.len());
        for e in items {
            let by_val = match &by {
                Some(p) => by_lookup(cx, p, &e)?,
                None => Some(e.clone()),
            };
            let weight = if alpha {
                0.0
            } else {
                match &by_val {
                    None => 0.0,
                    Some(v) => match sprig_proto_redis::num::parse_f64(v).filter(|f| !f.is_nan()) {
                        Some(f) => f,
                        None => {
                            return Ok(Frame::err(
                                "One or more scores can't be converted into double",
                            ));
                        }
                    },
                }
            };
            keyed.push((e, weight, by_val));
        }
        keyed.sort_by(|x, y| {
            let o = if alpha {
                x.2.cmp(&y.2)
            } else {
                x.1.total_cmp(&y.1).then_with(|| x.0.cmp(&y.0))
            };
            if desc { o.reverse() } else { o }
        });
        items = keyed.into_iter().map(|(e, _, _)| e).collect();
    }
    if let Some((o, c)) = limit {
        let len = items.len() as i64;
        let start = o.clamp(0, len);
        let end = if c < 0 {
            len
        } else {
            start.saturating_add(c).min(len)
        };
        items = items[start as usize..end as usize].to_vec();
    }
    let mut out: Vec<Option<Bytes>> = Vec::with_capacity(items.len() * gets.len().max(1));
    for e in items {
        if gets.is_empty() {
            out.push(Some(e));
        } else {
            for g in &gets {
                out.push(by_lookup(cx, g, &e)?);
            }
        }
    }
    if let Some(dst) = store {
        let raw = cx.raw(&dst)?;
        if raw.is_some() {
            cx.write(&dst, raw.as_ref(), None)?;
        }
        let vals: Vec<Bytes> = out.into_iter().map(Option::unwrap_or_default).collect();
        if !vals.is_empty() {
            let empty = super::lists::List {
                body: None,
                exp: None,
                raw: None,
            };
            cx.list_push(&dst, &empty, &vals, false)?;
        }
        return Ok(Frame::Integer(vals.len() as i64));
    }
    Ok(Frame::Array(
        out.into_iter()
            .map(|v| v.map_or_else(Frame::null, Frame::Bulk))
            .collect(),
    ))
}

/// Whether `t` names a type (`SCAN … TYPE`).
#[must_use]
pub fn is_type(t: &str) -> bool {
    [Type::String, Type::Hash, Type::List, Type::Set, Type::ZSet]
        .iter()
        .any(|x| x.name() == t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_prefix_ranges() {
        let s = Space::redis(1, 0);
        let (lo, hi) = meta_range(s, b"ab");
        for k in [&b"ab"[..], b"ab\0", b"abc", b"ab\xff\xff"] {
            let m = s.meta(k);
            assert!(lo.as_slice() <= &m[..] && &m[..] < hi.as_slice(), "{k:?}");
        }
        for k in [&b"a"[..], b"aa", b"ac", b"b"] {
            let m = s.meta(k);
            assert!(
                !(lo.as_slice() <= &m[..] && &m[..] < hi.as_slice()),
                "{k:?}"
            );
        }
        assert_eq!(
            parse_scan_entry(s, &s.scan_entry(b"k\0")),
            Some((crate::layout::routing_hash(b"k\0"), b"k\0".to_vec()))
        );
    }
}
