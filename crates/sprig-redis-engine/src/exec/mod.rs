//! The commands, over one batch of the engine.
//!
//! Every command checks its arguments and the key's type before it writes,
//! so a refused command (a syntax error, `WRONGTYPE`, not an integer)
//! changes nothing and the rest of its batch still applies.
//!
//! Expiry is lazy: a key whose expiry has passed reads as absent, and the
//! next write of it removes its old elements and expiry entry. The sweeper
//! ([`crate::sweep`]) deletes the ones nobody writes.
//!
//! - [`keys`]: the keyspace (`EXISTS`, `RENAME`, `SCAN`, ...);
//! - [`strings`]: strings, bitmaps and bitfields;
//! - [`hashes`], [`lists`], [`sets`], [`zsets`]: the collections;
//! - [`hll`] and [`geo`]: `HyperLogLog` (on strings) and geo (on sorted sets).

pub mod blocking;
pub mod geo;
pub mod hashes;
pub mod hll;
pub mod keys;
pub mod lists;
pub mod sets;
pub mod strings;
pub mod zsets;

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::{parse_f64, parse_i64};
use sprig_redis::reply;

use crate::layout::{Body, KIND_ELEM, KIND_ZSCORE, Meta, Space, Type};
use crate::store::{Batch, StoreError};

/// A command's outcome: its reply, or an engine failure that ends the
/// whole batch.
pub type Out = Result<Frame, StoreError>;

/// A refused argument: the reply to send instead.
pub type Arg<T> = Result<T, Frame>;

/// Returns the error reply of a refused argument from the command.
macro_rules! tri {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(f) => return Ok(f),
        }
    };
}
pub(crate) use tri;

/// The commands the engine runs (lower case).
pub const COMMANDS: &[&str] = &[
    // Keyspace.
    "del",
    "unlink",
    "exists",
    "type",
    "touch",
    "expire",
    "pexpire",
    "expireat",
    "pexpireat",
    "ttl",
    "pttl",
    "expiretime",
    "pexpiretime",
    "persist",
    "rename",
    "renamenx",
    "copy",
    "move",
    "keys",
    "scan",
    "randomkey",
    "dbsize",
    "flushdb",
    "flushall",
    "object",
    "sort",
    "sort_ro",
    "time",
    "wait",
    "waitaof",
    // Strings.
    "get",
    "set",
    "setnx",
    "setex",
    "psetex",
    "getset",
    "getdel",
    "getex",
    "mget",
    "mset",
    "msetnx",
    "append",
    "strlen",
    "incr",
    "decr",
    "incrby",
    "decrby",
    "incrbyfloat",
    "getrange",
    "substr",
    "setrange",
    "lcs",
    "setbit",
    "getbit",
    "bitcount",
    "bitpos",
    "bitop",
    "bitfield",
    "bitfield_ro",
    // Hashes.
    "hset",
    "hmset",
    "hsetnx",
    "hget",
    "hmget",
    "hgetall",
    "hkeys",
    "hvals",
    "hlen",
    "hexists",
    "hdel",
    "hincrby",
    "hincrbyfloat",
    "hstrlen",
    "hrandfield",
    "hscan",
    "hexpire",
    "hpexpire",
    "hexpireat",
    "hpexpireat",
    "httl",
    "hpttl",
    "hexpiretime",
    "hpexpiretime",
    "hpersist",
    // Lists.
    "lpush",
    "rpush",
    "lpushx",
    "rpushx",
    "lpop",
    "rpop",
    "llen",
    "lrange",
    "lindex",
    "lset",
    "linsert",
    "lrem",
    "ltrim",
    "lpos",
    "lmove",
    "rpoplpush",
    "lmpop",
    // Sets.
    "sadd",
    "srem",
    "smembers",
    "sismember",
    "smismember",
    "scard",
    "spop",
    "srandmember",
    "smove",
    "sinter",
    "sunion",
    "sdiff",
    "sinterstore",
    "sunionstore",
    "sdiffstore",
    "sintercard",
    "sscan",
    // Sorted sets.
    "zadd",
    "zincrby",
    "zrem",
    "zscore",
    "zmscore",
    "zcard",
    "zcount",
    "zlexcount",
    "zrange",
    "zrangestore",
    "zrevrange",
    "zrangebyscore",
    "zrevrangebyscore",
    "zrangebylex",
    "zrevrangebylex",
    "zrank",
    "zrevrank",
    "zremrangebyrank",
    "zremrangebyscore",
    "zremrangebylex",
    "zpopmin",
    "zpopmax",
    "zmpop",
    "zrandmember",
    "zunion",
    "zinter",
    "zdiff",
    "zunionstore",
    "zinterstore",
    "zdiffstore",
    "zintercard",
    "zscan",
    // Blocking.
    "blpop",
    "brpop",
    "blmove",
    "brpoplpush",
    "blmpop",
    "bzpopmin",
    "bzpopmax",
    "bzmpop",
    // HyperLogLog.
    "pfadd",
    "pfcount",
    "pfmerge",
    // Geo.
    "geoadd",
    "geopos",
    "geodist",
    "geohash",
    "geosearch",
    "geosearchstore",
    "georadius",
    "georadius_ro",
    "georadiusbymember",
    "georadiusbymember_ro",
];

/// Whether the engine runs `name`.
#[must_use]
pub fn runs(name: &str) -> bool {
    COMMANDS.contains(&name)
}

/// Whether `name` may write.
fn writes(name: &str) -> bool {
    sprig_redis::engine::lookup(name).is_some_and(|s| s.has('w'))
}

/// The database index argument `a`, when it names one of `all`.
fn db_arg(a: &[u8], all: &[Space]) -> Option<usize> {
    parse_i64(a)
        .and_then(|d| usize::try_from(d).ok())
        .filter(|d| *d < all.len())
}

/// `numkeys key...` at `a[at]`: the keys.
fn numkeys(a: &[Bytes], at: usize) -> &[Bytes] {
    let n = a
        .get(at)
        .and_then(|n| parse_i64(n))
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    a.get(at + 1..(at + 1 + n).min(a.len())).unwrap_or_default()
}

/// The metadata entries command `a` locks: those of the keys it may
/// write, in `space` (or another of the tenant's databases, `all`). Reads
/// lock nothing: they see the latest version queued for commit, and their
/// batch's reply waits until what they saw is durable, so they stay
/// linearizable.
#[must_use]
pub fn lock_keys(name: &str, a: &[Bytes], space: Space, all: &[Space]) -> Vec<Bytes> {
    write_targets(name, a, space, all)
        .into_iter()
        .map(|(s, k)| s.meta(&k))
        .collect()
}

/// The keys command `a` may write, each with its database, in the order
/// the command names them (what [`lock_keys`] locks).
#[must_use]
pub fn write_targets(name: &str, a: &[Bytes], space: Space, all: &[Space]) -> Vec<(Space, Bytes)> {
    if !writes(name) {
        return Vec::new();
    }
    let here = |k: &Bytes| (space, k.clone());
    match name {
        "move" => {
            let mut v = vec![here(&a[1])];
            if let Some(d) = db_arg(&a[2], all) {
                v.push((all[d], a[1].clone()));
            }
            v
        }
        "copy" => {
            let mut dst = space;
            let mut i = 3;
            while i + 1 < a.len() {
                if a[i].eq_ignore_ascii_case(b"db")
                    && let Some(d) = db_arg(&a[i + 1], all)
                {
                    dst = all[d];
                }
                i += 1;
            }
            vec![here(&a[1]), (dst, a[2].clone())]
        }
        "lmpop" | "zmpop" => numkeys(a, 1).iter().map(here).collect(),
        "blmpop" | "bzmpop" => numkeys(a, 2).iter().map(here).collect(),
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => a[1..a.len() - 1].iter().map(here).collect(),
        "sort" | "georadius" | "georadiusbymember" => {
            let mut v = Vec::new();
            for i in 2..a.len().saturating_sub(1) {
                if a[i].eq_ignore_ascii_case(b"store") || a[i].eq_ignore_ascii_case(b"storedist") {
                    v.push(here(&a[i + 1]));
                }
            }
            v
        }
        "bitop" => a.get(2).map(here).into_iter().collect(),
        _ => sprig_redis::engine::lookup(name)
            .map(|s| s.key_indexes(a.len()))
            .unwrap_or_default()
            .into_iter()
            .map(|i| here(&a[i]))
            .collect(),
    }
}

/// How many collection uids command `name` may take: one when it may
/// create a split collection (or turn an inline one into elements).
#[must_use]
pub fn uids_needed(name: &str) -> usize {
    usize::from(
        writes(name)
            && !matches!(
                name,
                "set"
                    | "setnx"
                    | "setex"
                    | "psetex"
                    | "getset"
                    | "getdel"
                    | "getex"
                    | "mset"
                    | "msetnx"
                    | "incr"
                    | "decr"
                    | "incrby"
                    | "decrby"
                    | "incrbyfloat"
                    | "bitop"
                    | "del"
                    | "unlink"
                    | "expire"
                    | "pexpire"
                    | "expireat"
                    | "pexpireat"
                    | "persist"
                    | "rename"
                    | "renamenx"
                    | "flushdb"
                    | "flushall"
                    | "pfadd"
                    | "pfmerge"
            ),
    )
}

/// One command's view of its batch.
pub struct Cx<'a, B: Batch> {
    /// The batch.
    pub b: &'a mut B,
    /// The selected database.
    pub space: Space,
    /// The unit's clock for this batch, ms since the epoch.
    pub now: i64,
    /// Every database of the tenant, by index.
    pub all: &'a [Space],
    /// Uids reserved for the batch's commands ([`uids_needed`] each);
    /// those left over go back to the tenant's pool.
    pub uids: &'a mut Vec<u64>,
    /// The command's client speaks RESP3 (pairs nest, sets are sets).
    pub resp3: bool,
    /// The metadata entries of the keys the batch changed: they wake
    /// blocked clients and fail `EXEC`s that watch them.
    pub touched: Vec<Bytes>,
}

/// An argument in upper case, for matching options.
pub(crate) fn upper(a: &[u8]) -> String {
    String::from_utf8_lossy(a).to_ascii_uppercase()
}

/// An integer argument.
pub(crate) fn int(a: &[u8]) -> Arg<i64> {
    parse_i64(a).ok_or_else(reply::not_int)
}

/// A float argument (not NaN).
pub(crate) fn float(a: &[u8]) -> Arg<f64> {
    parse_f64(a)
        .filter(|f| !f.is_nan())
        .ok_or_else(reply::not_float)
}

/// A malformed stored entry: the batch fails.
pub(crate) fn corrupt(what: &str, len: usize) -> StoreError {
    StoreError::fatal(format!("malformed {what} entry ({len} bytes)"))
}

/// A bulk string, or nil.
pub(crate) fn opt_bulk(v: Option<Bytes>) -> Frame {
    v.map_or_else(Frame::null, Frame::Bulk)
}

/// Normalizes `start..=end` (negative from the end) over `len` items: the
/// inclusive range, or `None` when empty.
pub(crate) fn index_range(len: i64, mut start: i64, mut end: i64) -> Option<(i64, i64)> {
    if start < 0 {
        start = start.saturating_add(len).max(0);
    }
    if end < 0 {
        end = end.saturating_add(len);
    }
    end = end.min(len - 1);
    (start <= end && start < len).then_some((start, end))
}

impl<B: Batch> Cx<'_, B> {
    /// A fresh collection uid, from those reserved for the batch.
    pub(crate) fn take_uid(&mut self) -> Result<u64, StoreError> {
        self.uids
            .pop()
            .ok_or_else(|| StoreError::fatal("no collection uid reserved for the command"))
    }

    /// The index of the selected database.
    pub(crate) fn db_index(&self) -> usize {
        self.all.iter().position(|s| *s == self.space).unwrap_or(0)
    }

    /// `key`'s metadata in `space` as stored, expired or not.
    pub(crate) fn raw_in(&mut self, space: Space, key: &[u8]) -> Result<Option<Meta>, StoreError> {
        let Some(v) = self.b.get(&space.meta(key))? else {
            return Ok(None);
        };
        Meta::decode(&v).map_or_else(|| Err(corrupt("key metadata", v.len())), |m| Ok(Some(m)))
    }

    /// `key`'s metadata as stored, expired or not.
    pub(crate) fn raw(&mut self, key: &[u8]) -> Result<Option<Meta>, StoreError> {
        self.raw_in(self.space, key)
    }

    /// `key`'s metadata in `space` if it hasn't expired, and as stored.
    pub(crate) fn load_in(
        &mut self,
        space: Space,
        key: &[u8],
    ) -> Result<(Option<Meta>, Option<Meta>), StoreError> {
        let raw = self.raw_in(space, key)?;
        let mut live = raw.clone().filter(|m| !m.expired(self.now));
        // A hash whose every field's lifetime has passed is gone.
        if let Some(Meta {
            body:
                Body::Hash(crate::layout::HashBody::Split {
                    uid,
                    count,
                    ttl: true,
                }),
            ..
        }) = &live
            && self.fexp_due(space, *uid)? >= *count as i64
        {
            live = None;
        }
        Ok((live, raw))
    }

    /// The number of fields of hash `uid` in `space` whose lifetime has
    /// passed.
    pub(crate) fn fexp_due(&mut self, space: Space, uid: u64) -> Result<i64, StoreError> {
        let (mut lo, hi) = space.fexp_range(uid, Some(self.now));
        let mut n = 0;
        loop {
            let got = self.b.scan(&lo, &hi, 1024)?;
            let Some((last, _)) = got.last() else {
                return Ok(n);
            };
            lo = crate::layout::after(last);
            n += got.len() as i64;
        }
    }

    /// The earliest field expiry of hash `uid` in `space`.
    pub(crate) fn fexp_first(&mut self, space: Space, uid: u64) -> Result<Option<i64>, StoreError> {
        let (lo, hi) = space.fexp_range(uid, None);
        Ok(self
            .b
            .scan(&lo, &hi, 1)?
            .first()
            .and_then(|(k, _)| Space::parse_fexp(k))
            .map(|(at, _)| at))
    }

    /// `key`'s metadata if it hasn't expired, and as stored.
    pub(crate) fn load(&mut self, key: &[u8]) -> Result<(Option<Meta>, Option<Meta>), StoreError> {
        self.load_in(self.space, key)
    }

    /// `key`'s live metadata.
    pub(crate) fn live(&mut self, key: &[u8]) -> Result<Option<Meta>, StoreError> {
        Ok(self.load(key)?.0)
    }

    /// Deletes every entry of `kind` under collection `uid` of `space`.
    pub(crate) fn drop_under(
        &mut self,
        space: Space,
        kind: u8,
        uid: u64,
    ) -> Result<(), StoreError> {
        let (start, end) = space.under_range(kind, uid);
        self.delete_range(&start, &end)
    }

    /// Deletes every entry in `[start, end)`, as it is now.
    pub(crate) fn delete_range(&mut self, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        // Each page starts after the last: deleted entries aren't walked
        // again.
        let mut from = start.to_vec();
        loop {
            let got = self.b.scan_latest(&from, end, 1024)?;
            let Some((last, _)) = got.last() else {
                return Ok(());
            };
            from = crate::layout::after(last);
            for (k, _) in got {
                self.b.delete(k)?;
            }
        }
    }

    /// Deletes collection `m`'s elements (and score index, field-expiry
    /// index and hash-due entry) of `key` in `space`.
    fn drop_body(&mut self, space: Space, key: &[u8], m: &Meta) -> Result<(), StoreError> {
        if let Some(u) = m.uid() {
            let ttl = matches!(
                m.body,
                Body::Hash(crate::layout::HashBody::Split { ttl: true, .. })
            );
            if ttl && let Some(at) = self.fexp_first(space, u)? {
                self.b.delete(space.hdue(at, key))?;
            }
            // A big collection's entries go later, in the sweeper's
            // batches: no key reaches them now, and uids are never reused.
            if body_entries(m) > DEFER_DROP_ENTRIES {
                let mut flags = 0;
                if m.kind() == Type::ZSet {
                    flags |= crate::layout::DEAD_ZSCORE;
                }
                if ttl {
                    flags |= crate::layout::DEAD_FEXP;
                }
                return self.b.put(space.dead(u), Bytes::copy_from_slice(&[flags]));
            }
            if ttl {
                let (lo, hi) = space.fexp_range(u, None);
                self.delete_range(&lo, &hi)?;
            }
            self.drop_under(space, KIND_ELEM, u)?;
            if m.kind() == Type::ZSet {
                self.drop_under(space, KIND_ZSCORE, u)?;
            }
        }
        Ok(())
    }

    /// Replaces `key`'s stored metadata `old` with `new` in `space` (`None`
    /// deletes the key), keeping its elements, scan-index entry and expiry
    /// entry consistent.
    pub(crate) fn write_in(
        &mut self,
        space: Space,
        key: &[u8],
        old: Option<&Meta>,
        new: Option<&Meta>,
    ) -> Result<(), StoreError> {
        self.touched.push(space.meta(key));
        if let Some(o) = old
            && o.uid().is_some()
            && new.and_then(Meta::uid) != o.uid()
        {
            self.drop_body(space, key, o)?;
        }
        match (old.is_some(), new.is_some()) {
            (false, true) => self.b.put(space.scan_entry(key), Bytes::new())?,
            (true, false) => self.b.delete(space.scan_entry(key))?,
            _ => {}
        }
        let old_exp = old.and_then(|m| m.exp);
        let new_exp = new.and_then(|m| m.exp);
        if old_exp != new_exp {
            if let Some(e) = old_exp {
                self.b.delete(space.expiry(e, key))?;
            }
            if let Some(e) = new_exp {
                self.b.put(space.expiry(e, key), Bytes::new())?;
            }
        }
        match new {
            Some(m) => self.b.put(space.meta(key), m.encode()),
            None => self.b.delete(space.meta(key)),
        }
    }

    /// [`Cx::write_in`] in the selected database.
    pub(crate) fn write(
        &mut self,
        key: &[u8],
        old: Option<&Meta>,
        new: Option<&Meta>,
    ) -> Result<(), StoreError> {
        self.write_in(self.space, key, old, new)
    }

    /// Deletes every key of `space`.
    pub(crate) fn flush_space(&mut self, space: Space) -> Result<(), StoreError> {
        let start = space.prefix(KIND_ELEM);
        let end = space.prefix_end(KIND_ZSCORE);
        self.delete_range(&start, &end)
    }

    /// A copy of collection `m` of `from` as a new value at `key` in `to`:
    /// its elements under a fresh uid. The metadata to store.
    pub(crate) fn copy_body(
        &mut self,
        from: Space,
        m: &Meta,
        to: Space,
        key: &[u8],
    ) -> Result<Meta, StoreError> {
        let Some(old) = m.uid() else {
            return Ok(m.clone());
        };
        let uid = self.take_uid()?;
        let mut kinds = vec![KIND_ELEM];
        if m.kind() == Type::ZSet {
            kinds.push(KIND_ZSCORE);
        }
        for kind in kinds {
            let (mut start, end) = from.under_range(kind, old);
            loop {
                let got = self.b.scan(&start, &end, 1024)?;
                let Some((last, _)) = got.last() else {
                    break;
                };
                start = crate::layout::after(last);
                for (k, v) in got {
                    self.b.put(to.rekey(kind, uid, &k), v)?;
                }
            }
        }
        if let Body::Hash(crate::layout::HashBody::Split { ttl: true, .. }) = m.body {
            let (mut lo, hi) = from.fexp_range(old, None);
            loop {
                let got = self.b.scan(&lo, &hi, 1024)?;
                let Some((last, _)) = got.last() else {
                    break;
                };
                lo = crate::layout::after(last);
                for (k, v) in got {
                    self.b
                        .put(to.rekey_at(KIND_ELEM, crate::layout::TTL_INDEX, uid, &k), v)?;
                }
            }
            if let Some(at) = self.fexp_first(to, uid)? {
                self.b.put(to.hdue(at, key), Bytes::new())?;
            }
        }
        let mut out = m.clone();
        out.body = match out.body {
            Body::Hash(crate::layout::HashBody::Split { count, ttl, .. }) => {
                Body::Hash(crate::layout::HashBody::Split { uid, count, ttl })
            }
            Body::List(crate::layout::ListBody::Split { head, tail, .. }) => {
                Body::List(crate::layout::ListBody::Split { uid, head, tail })
            }
            Body::Set(crate::layout::SetBody::Split { count, .. }) => {
                Body::Set(crate::layout::SetBody::Split { uid, count })
            }
            Body::ZSet(crate::layout::ZBody::Split { count, .. }) => {
                Body::ZSet(crate::layout::ZBody::Split { uid, count })
            }
            Body::Chunked { len, .. } => Body::Chunked { uid, len },
            b => b,
        };
        Ok(out)
    }
}

/// Collections with more entries than this are deleted by the sweeper.
pub const DEFER_DROP_ENTRIES: u64 = 4096;

/// About how many element entries split value `m` has.
fn body_entries(m: &Meta) -> u64 {
    use crate::layout::{HashBody, ListBody, SetBody, ZBody};
    match &m.body {
        Body::Hash(HashBody::Split { count, .. })
        | Body::Set(SetBody::Split { count, .. })
        | Body::ZSet(ZBody::Split { count, .. }) => *count,
        Body::List(ListBody::Split { head, tail, .. }) => tail.saturating_sub(*head).unsigned_abs(),
        Body::Chunked { len, .. } => len.div_ceil(crate::layout::CHUNK as u64),
        _ => 0,
    }
}

/// Removes up to `budget` entries of the dead collections of `space`
/// (oldest uid first), and the dead-uid entries of those fully removed:
/// the number of entries removed. The batch holds no key; nothing can
/// reach these entries.
///
/// # Errors
/// The engine's.
pub fn collect_dead<B: Batch>(b: &mut B, space: Space, budget: usize) -> Result<usize, StoreError> {
    let (lo, hi) = space.dead_range();
    let dead = b.scan(&lo, &hi, 64)?;
    let mut removed = 0;
    for (k, v) in dead {
        let Some(uid) = k
            .get(19..27)
            .and_then(|u| u.try_into().ok())
            .map(u64::from_be_bytes)
        else {
            continue;
        };
        let flags = v.first().copied().unwrap_or(0);
        let mut ranges = vec![space.under_range(KIND_ELEM, uid)];
        if flags & crate::layout::DEAD_ZSCORE != 0 {
            ranges.push(space.under_range(KIND_ZSCORE, uid));
        }
        if flags & crate::layout::DEAD_FEXP != 0 {
            ranges.push(space.fexp_range(uid, None));
        }
        let mut left = false;
        for (start, end) in ranges {
            if removed >= budget {
                left = true;
                break;
            }
            let got = b.scan_latest(&start, &end, budget - removed)?;
            if got.len() == budget - removed {
                left = true;
            }
            removed += got.len();
            for (ek, _) in got {
                b.delete(ek)?;
            }
        }
        if left {
            return Ok(removed);
        }
        b.delete(k)?;
    }
    Ok(removed)
}

/// Runs command `a` (name first, lower-cased as `name`).
///
/// # Errors
/// The engine's; the batch then fails as a whole.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Out {
    if let Some(out) = keys::run(cx, name, a) {
        return out;
    }
    if let Some(out) = strings::run(cx, name, a) {
        return out;
    }
    if let Some(out) = hashes::run(cx, name, a) {
        return out;
    }
    if let Some(out) = lists::run(cx, name, a) {
        return out;
    }
    if let Some(out) = sets::run(cx, name, a) {
        return out;
    }
    if let Some(out) = zsets::run(cx, name, a) {
        return out;
    }
    if let Some(out) = blocking::run(cx, name, a) {
        return out;
    }
    if let Some(out) = hll::run(cx, name, a) {
        return out;
    }
    if let Some(out) = geo::run(cx, name, a) {
        return out;
    }
    Ok(reply::unknown(&a[0], &a[1..]))
}

/// Deletes the keys of `space` whose expiry passed by `now`, up to
/// `limit` expiry entries: the number deleted. The batch must hold the
/// keys of the expiry entries in `due`.
///
/// # Errors
/// The engine's.
pub fn expire_due<B: Batch>(
    b: &mut B,
    space: Space,
    now: i64,
    due: &[(i64, Bytes)],
) -> Result<usize, StoreError> {
    expire_due_keys(b, space, now, due).map(|k| k.len())
}

/// [`expire_due`], returning the keys deleted.
///
/// # Errors
/// The engine's.
pub fn expire_due_keys<B: Batch>(
    b: &mut B,
    space: Space,
    now: i64,
    due: &[(i64, Bytes)],
) -> Result<Vec<Bytes>, StoreError> {
    let mut none = Vec::new();
    let mut cx = Cx {
        b,
        space,
        now,
        all: &[],
        uids: &mut none,
        resp3: false,
        touched: Vec::new(),
    };
    let mut gone = Vec::new();
    for (at, key) in due {
        let raw = cx.raw(key)?;
        match &raw {
            Some(m) if m.exp == Some(*at) && m.expired(now) => {
                cx.write(key, raw.as_ref(), None)?;
                gone.push(key.clone());
            }
            Some(m) if m.exp == Some(*at) => {}
            // An entry its key no longer matches.
            _ => cx.b.delete(space.expiry(*at, key))?,
        }
    }
    Ok(gone)
}

/// Removes the due fields of the hashes in `due` (their hash-due entries,
/// `(at, key)`), deleting hashes left empty: the number of hashes handled.
/// The batch must hold the keys.
///
/// # Errors
/// The engine's.
pub fn expire_fields<B: Batch>(
    b: &mut B,
    space: Space,
    now: i64,
    due: &[(i64, Bytes)],
) -> Result<usize, StoreError> {
    let mut none = Vec::new();
    let mut cx = Cx {
        b,
        space,
        now,
        all: &[],
        uids: &mut none,
        resp3: false,
        touched: Vec::new(),
    };
    for (at, key) in due {
        hashes::sweep_hash(&mut cx, *at, key)?;
    }
    Ok(due.len())
}

/// [`expire_fields`], returning the hashes whose fields it removed, each
/// with whether the hash went.
///
/// # Errors
/// The engine's.
pub fn expire_fields_keys<B: Batch>(
    b: &mut B,
    space: Space,
    now: i64,
    due: &[(i64, Bytes)],
) -> Result<Vec<(Bytes, bool)>, StoreError> {
    let mut none = Vec::new();
    let mut cx = Cx {
        b,
        space,
        now,
        all: &[],
        uids: &mut none,
        resp3: false,
        touched: Vec::new(),
    };
    let mut out = Vec::new();
    for (at, key) in due {
        if let Some(gone) = hashes::sweep_hash(&mut cx, *at, key)? {
            out.push((key.clone(), gone));
        }
    }
    Ok(out)
}
