//! Keyspace notifications: the events each command causes, under Redis
//! 7.4's names and classes, recorded with the batch that caused them.
//!
//! ```text
//! a tenant's flags (CONFIG SET notify-keyspace-events, or the setting):
//!   none that send (no class, or neither K nor E): nothing below runs
//! a batch, per command it runs:
//!   before: each key the command may write, as stored (live, expired, its
//!           encoded metadata)
//!   after:  the same keys again, and which the command wrote
//!   events: by command (rules below), in Redis's order; kept when their
//!           class is on ("new" needs n, "expired" x)
//! at commit: one event note on the tenant's unit with every event of the
//!   batch; the change log records it, and the gateway publishes it
//! the sweeper: "expired" for each key it deletes, "hexpired" (and "del")
//!   for hashes whose fields it removes
//! a read command, with m: "keymiss" for each named key that is absent,
//!   handed to the backend's direct sink (not recorded: nothing changed)
//! ```
//!
//! Redis deletes an expired key when a command looks it up, sending
//! "expired" then. Here reads never delete: a key a command writes over
//! sends "expired" first (in the same note); any other expired key gets it
//! from the sweeper when it deletes it.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::keyspace::{
    EXPIRED, GENERIC, HASH, KEYMISS, LIST, NEW, SET, STREAM, STRING, ZSET, sends,
};

use crate::exec::{self, Cx};
use crate::layout::{Body, OBJECT_BASE, Space};
use crate::store::{Batch, StoreError};

/// The kind byte of the event notes' change-log key.
pub const EVENT_KIND: u8 = b'N';

/// A tenant's keyspace notification flags on this node, read by every
/// batch: one atomic load when they are off.
#[derive(Debug, Default)]
pub struct Flags(AtomicU16);

impl Flags {
    /// The flags now (Redis's bits, `sprig_redis::keyspace`).
    #[must_use]
    pub fn get(&self) -> u16 {
        self.0.load(Ordering::Relaxed)
    }

    /// The flags that record events now: 0 when none would be sent.
    #[must_use]
    pub fn recording(&self) -> u16 {
        let f = self.get();
        if sends(f) { f } else { 0 }
    }

    /// Sets them.
    pub fn set(&self, flags: u16) {
        self.0.store(flags, Ordering::Relaxed);
    }
}

/// Where events that change nothing go (`keymiss`): the gateway publishes
/// them at once. Called with the tenant and its events.
pub type Sink = Arc<dyn Fn(u64, Vec<Event>) + Send + Sync>;

/// A tenant's notifications on this node: its flags, and the node's sink
/// for events that change nothing.
#[derive(Default)]
pub struct Notifier {
    /// The tenant's flags.
    pub flags: Flags,
    sink: Arc<OnceLock<Sink>>,
}

impl std::fmt::Debug for Notifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notifier")
            .field("flags", &self.flags.get())
            .finish_non_exhaustive()
    }
}

impl Notifier {
    /// A tenant's, sharing the node's `sink`.
    #[must_use]
    pub fn new(sink: Arc<OnceLock<Sink>>) -> Self {
        Self {
            flags: Flags::default(),
            sink,
        }
    }

    /// Hands `events` of `tenant` to the sink, if any.
    pub fn direct(&self, tenant: u64, events: Vec<Event>) {
        if !events.is_empty()
            && let Some(s) = self.sink.get()
        {
            s(tenant, events);
        }
    }
}

/// One keyspace event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The database index.
    pub db: u32,
    /// Redis's event name.
    pub name: &'static str,
    /// The key.
    pub key: Bytes,
}

/// Redis's class of event `name`.
#[must_use]
pub fn class(name: &str) -> u16 {
    match name {
        "new" => NEW,
        "expired" => EXPIRED,
        "keymiss" => KEYMISS,
        "set" | "setrange" | "incrby" | "incrbyfloat" | "append" | "setbit" | "pfadd" => STRING,
        "lpush" | "rpush" | "lpop" | "rpop" | "linsert" | "lset" | "lrem" | "ltrim"
        | "sortstore" => LIST,
        "sadd" | "srem" | "spop" | "sinterstore" | "sunionstore" | "sdiffstore" => SET,
        "hset" | "hdel" | "hincrby" | "hincrbyfloat" | "hexpire" | "hpersist" | "hexpired" => HASH,
        "zadd" | "zincr" | "zrem" | "zremrangebyrank" | "zremrangebyscore" | "zremrangebylex"
        | "zpopmin" | "zpopmax" | "zunionstore" | "zinterstore" | "zdiffstore" | "zrangestore"
        | "geosearchstore" | "georadiusstore" => ZSET,
        "xadd"
        | "xdel"
        | "xtrim"
        | "xsetid"
        | "xgroup-create"
        | "xgroup-createconsumer"
        | "xgroup-delconsumer"
        | "xgroup-destroy"
        | "xgroup-setid" => STREAM,
        _ => GENERIC,
    }
}

/// The database index of `space`.
#[must_use]
pub fn db_of(space: Space) -> u32 {
    space.object.wrapping_sub(OBJECT_BASE)
}

/// The change-log key of `tenant`'s event notes.
#[must_use]
pub fn note_key(tenant: u64) -> Vec<u8> {
    Space::redis(tenant, 0).prefix(EVENT_KIND)
}

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn varint(b: &[u8], at: &mut usize) -> Option<usize> {
    let mut v = 0usize;
    for shift in (0..64).step_by(7) {
        let x = *b.get(*at)?;
        *at += 1;
        v |= usize::from(x & 0x7F) << shift;
        if x < 0x80 {
            return Some(v);
        }
    }
    None
}

/// The value of an event note: `count, (db, name, key)*`.
#[must_use]
pub fn encode(events: &[Event]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + events.iter().map(|e| e.key.len() + 16).sum::<usize>());
    put_varint(&mut out, events.len());
    for e in events {
        put_varint(&mut out, e.db as usize);
        put_varint(&mut out, e.name.len());
        out.extend_from_slice(e.name.as_bytes());
        put_varint(&mut out, e.key.len());
        out.extend_from_slice(&e.key);
    }
    out
}

/// The events of an event note's value: (db, name, key). `None` when
/// malformed.
#[must_use]
pub fn decode(v: &[u8]) -> Option<Vec<(u32, String, Bytes)>> {
    let mut at = 0;
    let n = varint(v, &mut at)?;
    let mut out = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        let db = u32::try_from(varint(v, &mut at)?).ok()?;
        let l = varint(v, &mut at)?;
        let name = std::str::from_utf8(v.get(at..at + l)?).ok()?.to_owned();
        at += l;
        let l = varint(v, &mut at)?;
        let key = Bytes::copy_from_slice(v.get(at..at + l)?);
        at += l;
        out.push((db, name, key));
    }
    (at == v.len()).then_some(out)
}

/// Adds the note holding `events` (if any) to batch `b` of `tenant`.
///
/// # Errors
/// The engine's.
pub fn add_note<B: Batch>(b: &mut B, tenant: u64, events: &[Event]) -> Result<(), StoreError> {
    if events.is_empty() {
        return Ok(());
    }
    b.note(
        tenant,
        sprig_engine::observe::event::encode(&note_key(tenant), &encode(events)),
    )
}

/// One key a command may write, before it ran.
#[derive(Debug, Clone)]
// Each flag is one fact about the key the rules ask for.
#[allow(clippy::struct_excessive_bools)]
struct Target {
    space: Space,
    key: Bytes,
    /// Live (not expired) before.
    live: bool,
    /// Stored but expired before.
    expired: bool,
    /// The stored metadata, encoded.
    raw: Option<Bytes>,
    /// After the command: live, the command wrote it, its metadata
    /// changed (or is a chunked string's, whose bytes live elsewhere).
    after: bool,
    wrote: bool,
    changed: bool,
}

/// What [`before`] saw.
#[derive(Debug)]
pub struct Before {
    keys: Vec<Target>,
    touched_from: usize,
    events_from: usize,
}

/// The state of the keys command `a` (named `name`) may write, before it
/// runs.
///
/// # Errors
/// The engine's.
pub fn before<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Result<Before, StoreError> {
    let mut keys = Vec::new();
    for (space, key) in exec::write_targets(name, a, cx.space, cx.all) {
        let (live, raw) = cx.load_in(space, &key)?;
        keys.push(Target {
            space,
            key,
            live: live.is_some(),
            expired: raw.is_some() && live.is_none(),
            raw: raw.map(|m| m.encode()),
            after: false,
            wrote: false,
            changed: false,
        });
    }
    Ok(Before {
        keys,
        touched_from: cx.touched.len(),
        events_from: cx.events.len(),
    })
}

fn is_ok(r: &Frame) -> bool {
    !matches!(r, Frame::Error(_))
}

fn int(r: &Frame) -> i64 {
    match r {
        Frame::Integer(n) => *n,
        Frame::Boolean(b) => i64::from(*b),
        _ => 0,
    }
}

/// A non-empty, non-null reply.
fn got(r: &Frame) -> bool {
    match r {
        Frame::Null(_) | Frame::Error(_) => false,
        Frame::Array(v) | Frame::Set(v) | Frame::Push(v) => !v.is_empty(),
        Frame::Map(v) => !v.is_empty(),
        _ => true,
    }
}

fn has_arg(a: &[Bytes], from: usize, opts: &[&[u8]]) -> bool {
    a.iter()
        .skip(from)
        .any(|x| opts.iter().any(|o| x.eq_ignore_ascii_case(o)))
}

/// The key a pop's reply names first (`LMPOP`, `BLPOP`, `ZMPOP`, ...).
fn reply_key(r: &Frame) -> Option<&Bytes> {
    match r {
        Frame::Array(v) | Frame::Push(v) => match v.first() {
            Some(Frame::Bulk(k)) => Some(k),
            _ => None,
        },
        _ => None,
    }
}

/// The events of one command, building them in Redis's order.
struct Rules<'a> {
    keys: &'a [Target],
    out: Vec<(usize, &'static str)>,
    /// Keys whose "new" was sent (a key named twice is new once).
    created: Vec<usize>,
}

impl Rules<'_> {
    fn same(&self, i: usize, j: usize) -> bool {
        self.keys[i].space == self.keys[j].space && self.keys[i].key == self.keys[j].key
    }

    fn ev(&mut self, i: usize, name: &'static str) {
        if i < self.keys.len() {
            self.out.push((i, name));
        }
    }

    /// "new" when the command created key `i`.
    fn new_if(&mut self, i: usize) {
        let Some(k) = self.keys.get(i) else {
            return;
        };
        if !k.live && k.after && !self.created.iter().any(|&c| self.same(c, i)) {
            self.created.push(i);
            self.out.push((i, "new"));
        }
    }

    /// "del" when the command left key `i` gone.
    fn del_if(&mut self, i: usize) {
        if let Some(k) = self.keys.get(i)
            && k.live
            && !k.after
        {
            self.out.push((i, "del"));
        }
    }

    /// A write that may create the key.
    fn write(&mut self, i: usize, name: &'static str) {
        self.new_if(i);
        self.ev(i, name);
    }

    /// A removal that may empty the key.
    fn remove(&mut self, i: usize, name: &'static str) {
        self.ev(i, name);
        self.del_if(i);
    }

    /// A `*STORE` destination: its event, or "del" when the result was
    /// empty and the key went.
    fn store(&mut self, i: usize, name: &'static str) {
        match self.keys.get(i) {
            Some(k) if k.after => self.write(i, name),
            Some(k) if k.live => self.ev(i, "del"),
            _ => {}
        }
    }

    fn index_of(&self, key: &Bytes) -> Option<usize> {
        self.keys.iter().position(|k| k.key == *key)
    }

    fn wrote(&self, i: usize) -> bool {
        self.keys.get(i).is_some_and(|k| k.wrote)
    }

    fn changed(&self, i: usize) -> bool {
        self.keys.get(i).is_some_and(|k| k.changed)
    }

    fn live(&self, i: usize) -> bool {
        self.keys.get(i).is_some_and(|k| k.live)
    }
}

/// The events of command `a` (named `name`), which answered `r`, under
/// `flags` (only the classes on are kept), added to `out`.
///
/// # Errors
/// The engine's.
pub fn after<B: Batch>(
    cx: &mut Cx<'_, B>,
    mut b: Before,
    name: &str,
    a: &[Bytes],
    r: &Frame,
    flags: u16,
    out: &mut Vec<Event>,
) -> Result<(), StoreError> {
    let touched: Vec<Bytes> = cx.touched[b.touched_from.min(cx.touched.len())..].to_vec();
    for k in &mut b.keys {
        let (live, raw) = cx.load_in(k.space, &k.key)?;
        k.after = live.is_some();
        k.wrote = touched.contains(&k.space.meta(&k.key));
        let chunked = matches!(raw.as_ref().map(|m| &m.body), Some(Body::Chunked { .. }));
        k.changed = k.wrote && (chunked || raw.map(|m| m.encode()) != k.raw);
    }
    let mut x = Rules {
        keys: &b.keys,
        out: Vec::new(),
        created: Vec::new(),
    };
    rules(&mut x, name, a, r);
    let mut events: Vec<(usize, &'static str)> = Vec::new();
    // An expired key the command wrote over: Redis deleted it first.
    for (i, k) in b.keys.iter().enumerate() {
        if k.expired
            && k.wrote
            && !events
                .iter()
                .any(|(j, _)| b.keys[*j].space == k.space && b.keys[*j].key == k.key)
        {
            events.push((i, "expired"));
        }
    }
    events.extend(x.out);
    for (i, ev) in events {
        if flags & class(ev) == 0 {
            continue;
        }
        let k = &b.keys[i];
        out.push(Event {
            db: db_of(k.space),
            name: ev,
            key: k.key.clone(),
        });
    }
    // Events the command recorded itself (streams), after its own.
    for (space, key, ev) in cx.events.get(b.events_from..).unwrap_or_default() {
        if flags & class(ev) != 0 {
            out.push(Event {
                db: db_of(*space),
                name: ev,
                key: key.clone(),
            });
        }
    }
    Ok(())
}

/// The per-command rules (Redis 7.4's `notifyKeyspaceEvent` calls).
#[allow(clippy::too_many_lines)] // one arm per command family
fn rules(x: &mut Rules<'_>, name: &str, a: &[Bytes], r: &Frame) {
    let ok = is_ok(r);
    match name {
        // Strings.
        "set" => {
            if x.wrote(0) {
                x.write(0, "set");
                if has_arg(a, 3, &[b"EX", b"PX", b"EXAT", b"PXAT"]) {
                    x.ev(0, "expire");
                }
            }
        }
        "setnx" | "getset" => {
            if x.wrote(0) {
                x.write(0, "set");
            }
        }
        "setex" | "psetex" => {
            if x.wrote(0) {
                x.write(0, "set");
                x.ev(0, "expire");
            }
        }
        "mset" | "msetnx" => {
            for i in 0..x.keys.len() {
                if x.wrote(i) {
                    x.write(i, "set");
                }
            }
        }
        "getdel" => {
            if x.wrote(0) {
                x.del_if(0);
            }
        }
        "getex" => {
            if x.wrote(0) {
                if has_arg(a, 2, &[b"PERSIST"]) {
                    x.ev(0, "persist");
                } else if x.keys.first().is_some_and(|k| !k.after) {
                    x.del_if(0);
                } else {
                    x.ev(0, "expire");
                }
            }
        }
        "append" => {
            if ok {
                x.write(0, "append");
            }
        }
        "incr" | "decr" | "incrby" | "decrby" => {
            if ok {
                x.write(0, "incrby");
            }
        }
        "incrbyfloat" => {
            if ok {
                x.write(0, "incrbyfloat");
            }
        }
        "setrange" => {
            if x.wrote(0) {
                x.write(0, "setrange");
            }
        }
        "setbit" => {
            if ok {
                x.new_if(0);
                let bit = a.get(3).map(|v| v.as_ref() == b"1");
                if bit.is_some_and(|b| int(r) != i64::from(b)) {
                    x.ev(0, "setbit");
                }
            }
        }
        "bitfield" => {
            if ok {
                x.new_if(0);
                if x.changed(0) {
                    x.ev(0, "setbit");
                }
            }
        }
        "bitop" => x.store(0, "set"),
        "pfadd" => {
            if int(r) == 1 {
                x.write(0, "pfadd");
            }
        }
        "pfmerge" => {
            if ok {
                x.write(0, "pfadd");
            }
        }
        // Keys.
        "del" | "unlink" => {
            for i in 0..x.keys.len() {
                // A key named twice goes once.
                if x.wrote(i) && !(0..i).any(|j| x.same(i, j)) {
                    x.del_if(i);
                }
            }
        }
        "expire" | "pexpire" | "expireat" | "pexpireat" => {
            if x.wrote(0) {
                if x.keys.first().is_some_and(|k| k.after) {
                    x.ev(0, "expire");
                } else {
                    x.del_if(0);
                }
            }
        }
        "persist" => {
            if x.wrote(0) {
                x.ev(0, "persist");
            }
        }
        "rename" | "renamenx" => {
            if x.keys.len() == 2 && !x.same(0, 1) && x.wrote(1) {
                x.ev(1, "new");
                x.ev(0, "rename_from");
                x.ev(1, "rename_to");
            }
        }
        "copy" => {
            if int(r) == 1 {
                x.ev(1, "new");
                x.ev(1, "copy_to");
            }
        }
        "move" => {
            if int(r) == 1 {
                x.ev(1, "new");
                x.ev(0, "move_from");
                x.ev(1, "move_to");
            }
        }
        "sort" if ok => {
            if !x.keys.is_empty() {
                x.store(0, "sortstore");
            }
        }
        // Hashes.
        "hset" | "hmset" => {
            if ok {
                x.write(0, "hset");
            }
        }
        "hsetnx" => {
            if int(r) == 1 {
                x.write(0, "hset");
            }
        }
        "hdel" => {
            if int(r) > 0 {
                x.remove(0, "hdel");
            }
        }
        "hincrby" | "hincrbyfloat" => {
            if ok {
                x.write(
                    0,
                    if name == "hincrby" {
                        "hincrby"
                    } else {
                        "hincrbyfloat"
                    },
                );
            }
        }
        "hexpire" | "hpexpire" | "hexpireat" | "hpexpireat" | "hpersist" => {
            let codes: Vec<i64> = match r {
                Frame::Array(v) => v.iter().map(int).collect(),
                _ => Vec::new(),
            };
            if name == "hpersist" {
                if codes.contains(&1) {
                    x.ev(0, "hpersist");
                }
            } else {
                if codes.contains(&1) {
                    x.ev(0, "hexpire");
                }
                if codes.contains(&2) {
                    x.remove(0, "hdel");
                }
            }
        }
        // Lists.
        "lpush" | "lpushx" | "rpush" | "rpushx" => {
            if int(r) > 0 {
                x.write(
                    0,
                    if name.starts_with('l') {
                        "lpush"
                    } else {
                        "rpush"
                    },
                );
            }
        }
        "lpop" | "rpop" => {
            if got(r) {
                x.remove(0, if name == "lpop" { "lpop" } else { "rpop" });
            }
        }
        "lset" => {
            if ok {
                x.ev(0, "lset");
            }
        }
        "linsert" => {
            if int(r) > 0 {
                x.ev(0, "linsert");
            }
        }
        "lrem" => {
            if int(r) > 0 {
                x.remove(0, "lrem");
            }
        }
        "ltrim" => {
            if ok && x.live(0) {
                x.remove(0, "ltrim");
            }
        }
        "lmove" | "blmove" | "rpoplpush" | "brpoplpush" => {
            if matches!(r, Frame::Bulk(_)) && x.keys.len() == 2 {
                let (from_left, to_left) = match name {
                    "lmove" | "blmove" => (
                        a.get(3).is_some_and(|w| w.eq_ignore_ascii_case(b"left")),
                        a.get(4).is_some_and(|w| w.eq_ignore_ascii_case(b"left")),
                    ),
                    _ => (false, true),
                };
                if !x.same(0, 1) {
                    x.new_if(1);
                }
                x.ev(1, if to_left { "lpush" } else { "rpush" });
                x.ev(0, if from_left { "lpop" } else { "rpop" });
                if !x.same(0, 1) {
                    x.del_if(0);
                }
            }
        }
        "lmpop" | "blmpop" | "blpop" | "brpop" => {
            if let Some(i) = reply_key(r).and_then(|k| x.index_of(k)) {
                let left = match name {
                    "blpop" => true,
                    "brpop" => false,
                    _ => has_arg(a, 2, &[b"LEFT"]),
                };
                x.remove(i, if left { "lpop" } else { "rpop" });
            }
        }
        // Sets.
        "sadd" => {
            if int(r) > 0 {
                x.write(0, "sadd");
            }
        }
        "srem" => {
            if int(r) > 0 {
                x.remove(0, "srem");
            }
        }
        "spop" => {
            if got(r) {
                x.remove(0, "spop");
            }
        }
        "smove" => {
            if int(r) == 1 && x.keys.len() == 2 && !x.same(0, 1) {
                x.remove(0, "srem");
                if x.changed(1) {
                    x.write(1, "sadd");
                }
            }
        }
        "sinterstore" | "sunionstore" | "sdiffstore" => {
            if ok {
                x.store(
                    0,
                    match name {
                        "sinterstore" => "sinterstore",
                        "sunionstore" => "sunionstore",
                        _ => "sdiffstore",
                    },
                );
            }
        }
        // Sorted sets.
        "zadd" => {
            if has_arg(a, 2, &[b"INCR"]) {
                if got(r) {
                    x.write(0, "zincr");
                }
            } else if ok && (int(r) > 0 || x.changed(0)) {
                x.write(0, "zadd");
            }
        }
        "zincrby" => {
            if ok {
                x.write(0, "zincr");
            }
        }
        "zrem" => {
            if int(r) > 0 {
                x.remove(0, "zrem");
            }
        }
        "zremrangebyrank" | "zremrangebyscore" | "zremrangebylex" => {
            if int(r) > 0 {
                x.remove(
                    0,
                    match name {
                        "zremrangebyrank" => "zremrangebyrank",
                        "zremrangebyscore" => "zremrangebyscore",
                        _ => "zremrangebylex",
                    },
                );
            }
        }
        "zpopmin" | "zpopmax" => {
            if got(r) {
                x.remove(
                    0,
                    if name == "zpopmin" {
                        "zpopmin"
                    } else {
                        "zpopmax"
                    },
                );
            }
        }
        "bzpopmin" | "bzpopmax" | "zmpop" | "bzmpop" => {
            if let Some(i) = reply_key(r).and_then(|k| x.index_of(k)) {
                let min = match name {
                    "bzpopmin" => true,
                    "bzpopmax" => false,
                    _ => has_arg(a, 2, &[b"MIN"]),
                };
                x.remove(i, if min { "zpopmin" } else { "zpopmax" });
            }
        }
        "zunionstore" | "zinterstore" | "zdiffstore" | "zrangestore" => {
            if ok {
                x.store(
                    0,
                    match name {
                        "zunionstore" => "zunionstore",
                        "zinterstore" => "zinterstore",
                        "zdiffstore" => "zdiffstore",
                        _ => "zrangestore",
                    },
                );
            }
        }
        "geoadd" => {
            if ok && (int(r) > 0 || x.changed(0)) {
                x.write(0, "zadd");
            }
        }
        "geosearchstore" => {
            if ok {
                x.store(0, "geosearchstore");
            }
        }
        "georadius" | "georadiusbymember" => {
            if ok && !x.keys.is_empty() {
                x.store(0, "georadiusstore");
            }
        }
        // Streams; group and consumer events come from the command itself.
        "xadd" => {
            if matches!(r, Frame::Bulk(_)) {
                x.write(0, "xadd");
            }
        }
        "xdel" => {
            if int(r) > 0 {
                x.ev(0, "xdel");
            }
        }
        "xtrim" => {
            if int(r) > 0 {
                x.ev(0, "xtrim");
            }
        }
        "xsetid" => {
            if ok {
                x.ev(0, "xsetid");
            }
        }
        "xgroup" => {
            if ok {
                x.new_if(0);
            }
        }
        _ => {}
    }
}

/// `numkeys key...` at `a[at]`: the keys.
fn numkeys(a: &[Bytes], at: usize) -> &[Bytes] {
    let n = a
        .get(at)
        .and_then(|n| sprig_proto_redis::num::parse_i64(n))
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    a.get(at + 1..(at + 1 + n).min(a.len())).unwrap_or_default()
}

/// The keys command `a` looks up for reading (Redis's `lookupKeyRead`,
/// which sends "keymiss"): a read command's keys, and the sources of
/// commands that write elsewhere.
fn read_keys<'a>(name: &str, a: &'a [Bytes]) -> Vec<&'a Bytes> {
    let from = |i: usize| a.get(i..).unwrap_or_default().iter().collect();
    match name {
        // Looked up without a notification.
        "object" => Vec::new(),
        "touch" => from(1),
        "zunion" | "zinter" | "zdiff" | "zintercard" | "sintercard" => {
            numkeys(a, 1).iter().collect()
        }
        "getex" | "getdel" | "getset" | "copy" | "sort" | "sort_ro" => {
            a.get(1).into_iter().collect()
        }
        "set" if has_arg(a, 3, &[b"GET"]) => a.get(1).into_iter().collect(),
        "sinterstore" | "sunionstore" | "sdiffstore" | "pfmerge" => from(2),
        "zunionstore" | "zinterstore" | "zdiffstore" => numkeys(a, 2).iter().collect(),
        "zrangestore" | "geosearchstore" => a.get(2).into_iter().collect(),
        "bitop" => from(3),
        "georadius" | "georadiusbymember" => a.get(1).into_iter().collect(),
        _ => match sprig_redis::engine::lookup(name) {
            Some(s) if !s.has('w') && s.has('r') => s
                .key_indexes(a.len())
                .into_iter()
                .filter_map(|i| a.get(i))
                .collect(),
            _ => Vec::new(),
        },
    }
}

/// "keymiss" for each key command `a` (named `name`) looks up for reading
/// that is absent, when `flags` has `m`. Called before the command runs;
/// the caller keeps them only when the command didn't answer an error
/// (Redis checks a destination's type before it reads the sources).
///
/// # Errors
/// The engine's.
pub fn keymiss<B: Batch>(
    cx: &mut Cx<'_, B>,
    name: &str,
    a: &[Bytes],
    flags: u16,
    out: &mut Vec<Event>,
) -> Result<(), StoreError> {
    if flags & KEYMISS == 0 {
        return Ok(());
    }
    for k in read_keys(name, a) {
        if cx.live(k)?.is_none() {
            out.push(Event {
                db: db_of(cx.space),
                name: "keymiss",
                key: k.clone(),
            });
        }
    }
    Ok(())
}

/// The events of the keys the sweeper deletes or trims: "expired" for a
/// key, "hexpired" (and "del" when it went) for a hash whose fields it
/// removed.
#[must_use]
pub fn swept(space: Space, expired: &[Bytes], hashes: &[(Bytes, bool)], flags: u16) -> Vec<Event> {
    let db = db_of(space);
    let mut out = Vec::new();
    if flags & EXPIRED != 0 {
        out.extend(expired.iter().map(|k| Event {
            db,
            name: "expired",
            key: k.clone(),
        }));
    }
    for (k, gone) in hashes {
        if flags & HASH != 0 {
            out.push(Event {
                db,
                name: "hexpired",
                key: k.clone(),
            });
        }
        if *gone && flags & GENERIC != 0 {
            out.push(Event {
                db,
                name: "del",
                key: k.clone(),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_round_trip() {
        let evs = vec![
            Event {
                db: 3,
                name: "set",
                key: Bytes::from_static(b"k"),
            },
            Event {
                db: 300,
                name: "rename_to",
                key: Bytes::from(vec![0u8; 200]),
            },
        ];
        let v = encode(&evs);
        let got = decode(&v).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (3, "set".to_owned(), Bytes::from_static(b"k")));
        assert_eq!(got[1].0, 300);
        assert_eq!(got[1].2.len(), 200);
        assert!(decode(&v[..v.len() - 1]).is_none());
    }

    #[test]
    fn classes_follow_redis() {
        assert_eq!(class("set"), STRING);
        assert_eq!(class("sortstore"), LIST);
        assert_eq!(class("geosearchstore"), ZSET);
        assert_eq!(class("rename_from"), GENERIC);
        assert_eq!(class("hexpired"), HASH);
        assert_eq!(class("new"), NEW);
    }
}
