//! One Redis database on the engine: a batch of commands runs as one
//! transaction with one commit.
//!
//! **Hot keys.** Batches whose writes all touch one key (a client's `SET`,
//! `INCR` or `LPUSH`, alone or with reads) are combined: they queue on the
//! key, and whichever batch finds the queue idle becomes its leader. The
//! leader takes every queued batch, runs them in queue order as one
//! transaction (one lock on the key, one commit) and hands each batch its
//! replies and the commit's ticket; if more batches queued meanwhile, a new
//! task leads the next round. So N clients on one key cost one lock
//! hand-off per round instead of one per command, and a round's commands
//! share a log sync. Each batch still answers only once the commit is
//! durable. Other batches run directly.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::engine::{EngineDatabase, LuaStore, Watched};
use tokio::sync::{Notify, oneshot};

use crate::exec::{self, Cx};
use crate::layout::Space;
use crate::store::{Batch, StoreError, Ticket, Unit};

mod script;
pub use script::ScriptTxn;

/// A batch's replies and the ticket they wait for.
type Outcome<T> = Result<(Vec<Frame>, T), StoreError>;

/// The ticket type of `U`'s batches.
type TicketOf<U> = <<U as Unit>::Batch as Batch>::Ticket;

/// A batch waiting on a hot key.
struct Waiting<T> {
    cmds: Vec<Vec<Bytes>>,
    names: Vec<String>,
    resp3: bool,
    tx: oneshot::Sender<Outcome<T>>,
}

/// One key's queue.
struct Queue<T> {
    waiting: Vec<Waiting<T>>,
}

/// Lock shards of the hot-key queues.
const SHARDS: usize = 16;

/// Collection uids taken from the database's counter at a time.
const UID_BLOCK: u64 = 1_000;

/// Uids taken from the counter (durably) and not yet handed out.
#[derive(Debug, Default)]
struct UidPool {
    /// The leader epoch the pool's uids were taken under: a pool of
    /// another epoch is dropped (a block taken by a leader whose log tail
    /// was later lost may have been handed out again by the next one).
    epoch: u64,
    /// Handed out but returned unused.
    free: Vec<u64>,
    /// The rest of the current block: `next..limit`.
    next: u64,
    limit: u64,
}

impl UidPool {
    fn available(&self) -> usize {
        self.free.len() + usize::try_from(self.limit - self.next).unwrap_or(usize::MAX)
    }

    fn take(&mut self, n: usize) -> Vec<u64> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            if let Some(u) = self.free.pop() {
                out.push(u);
            } else if self.next < self.limit {
                out.push(self.next);
                self.next += 1;
            } else {
                break;
            }
        }
        out
    }

    /// A new block `from..to`; what was left of the old one stays usable.
    fn add_block(&mut self, from: u64, to: u64) {
        self.free.extend(self.next..self.limit);
        self.next = from;
        self.limit = to;
    }
}

type Shard<T> = Mutex<HashMap<Bytes, Queue<T>>>;

/// The collection uids of one tenant's databases: one counter (in
/// database 0) and one pool, so a uid is never handed out twice in any of
/// them and a value can move between databases under a fresh one.
pub struct Uids<U: Unit> {
    unit: Arc<U>,
    counter: Bytes,
    pool: Mutex<UidPool>,
    /// One refill of the pool at a time.
    refill: tokio::sync::Mutex<()>,
}

impl<U: Unit> std::fmt::Debug for Uids<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Uids").finish_non_exhaustive()
    }
}

impl<U: Unit> Uids<U> {
    /// The uids of `tenant`'s databases.
    #[must_use]
    pub fn new(unit: Arc<U>, tenant: u64) -> Arc<Self> {
        Arc::new(Self {
            unit,
            counter: Space::redis(tenant, 0).uid_counter(),
            pool: Mutex::new(UidPool::default()),
            refill: tokio::sync::Mutex::new(()),
        })
    }

    fn pool(&self) -> MutexGuard<'_, UidPool> {
        self.pool.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The pool, emptied first if it was filled under another leader
    /// epoch than the unit's current one.
    fn current_pool(&self) -> MutexGuard<'_, UidPool> {
        let epoch = self.unit.leader_epoch(&self.counter);
        let mut p = self.pool();
        if p.epoch != epoch {
            *p = UidPool {
                epoch,
                ..UidPool::default()
            };
        }
        p
    }

    /// Gives back uids a batch didn't use (kept only under the epoch they
    /// were taken in).
    fn give_back(&self, uids: Vec<u64>, epoch: u64) {
        if !uids.is_empty() {
            let mut p = self.pool();
            if p.epoch == epoch {
                p.free.extend(uids);
            }
        }
    }

    /// `n` collection uids for one batch. When the pool runs short, the
    /// next block is taken from the counter in a transaction of its own,
    /// durable before any uid of it is handed out, so a uid is never
    /// handed out twice, even across a crash.
    /// Also returns the leader epoch they were taken under.
    async fn reserve(&self, n: usize) -> Result<(Vec<u64>, u64), StoreError> {
        if n == 0 {
            return Ok((Vec::new(), 0));
        }
        loop {
            {
                let mut p = self.current_pool();
                if p.available() >= n {
                    return Ok((p.take(n), p.epoch));
                }
            }
            let _one = self.refill.lock().await;
            let epoch = {
                let p = self.current_pool();
                if p.available() >= n {
                    continue;
                }
                p.epoch
            };
            let key = self.counter.clone();
            let mut b = self.unit.begin(vec![key.clone()]).await?;
            let from = match b.get(&key)? {
                None => 1,
                Some(v) => u64::from_be_bytes(
                    v.as_ref()
                        .try_into()
                        .map_err(|_| StoreError::fatal("malformed collection uid counter"))?,
                ),
            };
            let to = from
                .checked_add(UID_BLOCK.max(n as u64))
                .ok_or_else(|| StoreError::fatal("collection uids exhausted"))?;
            b.put(key, Bytes::copy_from_slice(&to.to_be_bytes()))?;
            b.commit()?.durable().await?;
            let mut p = self.pool();
            if p.epoch == epoch {
                p.add_block(from, to);
            }
        }
    }
}

/// Write versions for `WATCH`: every batch bumps, before its commit and
/// while it still holds its keys, one counter per key it locks (keys share
/// 65,536 counters by hash, so an unrelated write can make an `EXEC` fail,
/// rarely, as a retry would). A flush bumps every key at once.
pub struct Versions {
    slots: Box<[AtomicU64]>,
    flushes: AtomicU64,
    hasher: RandomState,
}

impl std::fmt::Debug for Versions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Versions").finish_non_exhaustive()
    }
}

impl Versions {
    /// Counters for one node.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: (0..65_536).map(|_| AtomicU64::new(0)).collect(),
            flushes: AtomicU64::new(0),
            hasher: RandomState::new(),
        })
    }

    fn slot(&self, meta: &[u8]) -> &AtomicU64 {
        let i = usize::try_from(self.hasher.hash_one(meta) % self.slots.len() as u64).unwrap_or(0);
        &self.slots[i]
    }

    /// What `EXEC` compares for the key with metadata entry `meta`.
    fn token(&self, meta: &[u8]) -> u64 {
        self.slot(meta)
            .load(Ordering::Acquire)
            .wrapping_add(self.flushes.load(Ordering::Acquire))
    }

    fn bump(&self, metas: &[Bytes]) {
        for m in metas {
            self.slot(m).fetch_add(1, Ordering::AcqRel);
        }
    }

    fn flushed(&self) {
        self.flushes.fetch_add(1, Ordering::AcqRel);
    }
}

/// The clients of one tenant blocked on keys, by the metadata entry each
/// waits on, oldest first. A batch that writes a key wakes the oldest
/// client waiting on it; a client that got an item wakes the next one, so
/// clients are served in the order they blocked. A wake is a permit, so
/// one that arrives between two attempts isn't lost.
#[derive(Debug, Default)]
pub struct Waiters {
    map: Mutex<HashMap<Bytes, std::collections::VecDeque<Arc<Notify>>>>,
    blocked: AtomicUsize,
    /// Counts the writes that woke anyone (a waiter that can't use a wake
    /// passes it on once per write).
    wakes: AtomicU64,
}

/// A blocked client's place in [`Waiters`]; dropping it (the client got
/// its reply, timed out or went away) gives the place up.
struct Registration<'a> {
    waiters: &'a Waiters,
    metas: Vec<Bytes>,
    note: Arc<Notify>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        // A wake this client may not have used goes to the next in line.
        self.waiters.wake_first(&self.metas, Some(&self.note));
        self.waiters.unregister(&self.metas, &self.note);
    }
}

impl Waiters {
    fn lock(&self) -> MutexGuard<'_, HashMap<Bytes, std::collections::VecDeque<Arc<Notify>>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn register(&self, metas: Vec<Bytes>) -> Registration<'_> {
        let note = Arc::new(Notify::new());
        let mut m = self.lock();
        for k in &metas {
            m.entry(k.clone()).or_default().push_back(Arc::clone(&note));
        }
        drop(m);
        self.blocked.fetch_add(1, Ordering::AcqRel);
        Registration {
            waiters: self,
            metas,
            note,
        }
    }

    fn unregister(&self, metas: &[Bytes], n: &Arc<Notify>) {
        let mut m = self.lock();
        for k in metas {
            if let Some(v) = m.get_mut(k) {
                v.retain(|x| !Arc::ptr_eq(x, n));
                if v.is_empty() {
                    m.remove(k);
                }
            }
        }
        drop(m);
        self.blocked.fetch_sub(1, Ordering::AcqRel);
    }

    /// Wakes the oldest client waiting on each of `metas` (other than
    /// `except`).
    fn wake_first(&self, metas: &[Bytes], except: Option<&Arc<Notify>>) {
        if self.blocked.load(Ordering::Acquire) == 0 {
            return;
        }
        let m = self.lock();
        for k in metas {
            if let Some(n) = m
                .get(k)
                .and_then(|v| v.iter().find(|x| except.is_none_or(|e| !Arc::ptr_eq(x, e))))
            {
                n.notify_one();
            }
        }
    }

    fn wake(&self, metas: &[Bytes]) {
        if self.blocked.load(Ordering::Acquire) == 0 {
            return;
        }
        self.wakes.fetch_add(1, Ordering::AcqRel);
        self.wake_first(metas, None);
    }
}

/// Times a multi-key batch that failed retryably (a deadlock with a
/// script) runs again before its error reaches the client.
const BATCH_RETRIES: u32 = 5;

/// How long a blocked client waits before trying again on its own.
const BLOCK_POLL: Duration = Duration::from_secs(1);

/// A Redis database on the engine.
pub struct EngineDb<U: Unit> {
    unit: Arc<U>,
    space: Space,
    /// Every database of the tenant, by index.
    all: Arc<[Space]>,
    queues: [Shard<TicketOf<U>>; SHARDS],
    hasher: RandomState,
    /// The tenant's collection uids.
    uids: Arc<Uids<U>>,
    /// The node's write versions.
    versions: Arc<Versions>,
    /// The tenant's blocked clients.
    waiters: Arc<Waiters>,
    /// The tenant's keyspace notifications (unset: off).
    notify: std::sync::OnceLock<Arc<crate::notify::Notifier>>,
    me: Weak<Self>,
}

impl<U: Unit> std::fmt::Debug for EngineDb<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineDb")
            .field("space", &self.space)
            .finish_non_exhaustive()
    }
}

/// Clears a key's hot-key queue when its round ends early.
struct RoundGuard<'a, U: Unit> {
    db: &'a EngineDb<U>,
    key: Bytes,
    armed: bool,
}

impl<U: Unit> Drop for RoundGuard<'_, U> {
    fn drop(&mut self) {
        if self.armed {
            self.db.shard(&self.key).remove(&self.key);
        }
    }
}

/// The reply every command of a batch gets when the engine failed it.
fn failed(e: &StoreError) -> Frame {
    if e.retryable {
        Frame::error(format!("TRYAGAIN {}", e.message))
    } else {
        Frame::err(&format!("storage engine: {}", e.message))
    }
}

/// The database `SELECT` names, or its error reply.
fn select_arg(c: &[Bytes], all: &[Space]) -> Result<Space, Frame> {
    let n = c
        .get(1)
        .and_then(|n| sprig_proto_redis::num::parse_i64(n))
        .ok_or_else(sprig_redis::reply::not_int)?;
    usize::try_from(n)
        .ok()
        .and_then(|i| all.get(i).copied())
        .ok_or_else(|| Frame::err("DB index is out of range"))
}

fn lower(c: &[Bytes]) -> String {
    c.first()
        .map(|n| String::from_utf8_lossy(n).to_ascii_lowercase())
        .unwrap_or_default()
}

impl<U: Unit> EngineDb<U> {
    /// Database `space` on `unit`; `all` are the tenant's databases,
    /// `uids` their collection uids and `waiters` their blocked clients;
    /// `versions` are the node's.
    #[must_use]
    pub fn new(
        unit: Arc<U>,
        space: Space,
        all: Arc<[Space]>,
        uids: Arc<Uids<U>>,
        versions: Arc<Versions>,
        waiters: Arc<Waiters>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            unit,
            space,
            all,
            queues: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            hasher: RandomState::new(),
            uids,
            versions,
            waiters,
            notify: std::sync::OnceLock::new(),
            me: me.clone(),
        })
    }

    /// Shares the tenant's keyspace notifications (once).
    pub fn set_notify(&self, n: Arc<crate::notify::Notifier>) {
        let _ = self.notify.set(n);
    }

    /// The flags that record events now (0: off).
    fn recording(&self) -> u16 {
        self.notify.get().map_or(0, |n| n.flags.recording())
    }

    /// Hands events that change nothing to the node's sink.
    fn direct(&self, events: Vec<crate::notify::Event>) {
        if let Some(n) = self.notify.get() {
            n.direct(self.space.tenant, events);
        }
    }

    /// The database's place in the keyspace.
    #[must_use]
    pub const fn space(&self) -> Space {
        self.space
    }

    fn shard(&self, key: &[u8]) -> MutexGuard<'_, HashMap<Bytes, Queue<TicketOf<U>>>> {
        let i = usize::try_from(self.hasher.hash_one(key) % SHARDS as u64).unwrap_or(0);
        self.queues[i]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The sorted keys `cmds` write.
    fn write_keys(&self, cmds: &[&[Bytes]], names: &[String]) -> Vec<Bytes> {
        let mut keys: Vec<Bytes> = cmds
            .iter()
            .zip(names)
            .flat_map(|(c, n)| exec::lock_keys(n, c, self.space, &self.all))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    /// Runs `cmds` (named `names`) in one batch holding `keys`; replies and
    /// the commit's ticket.
    async fn execute(
        &self,
        keys: Vec<Bytes>,
        cmds: &[&[Bytes]],
        names: &[String],
        resp3: &[bool],
    ) -> Outcome<TicketOf<U>> {
        let (mut uids, epoch) = self
            .uids
            .reserve(names.iter().map(|n| exec::uids_needed(n)).sum())
            .await?;
        let out = self
            .execute_with(keys, cmds, names, resp3, &mut uids, &[])
            .await;
        // Uids the batch didn't use go back (one it wrote but rolled back
        // was never committed, but it isn't returned either).
        self.uids.give_back(uids, epoch);
        // No watched keys: the batch always runs.
        out.and_then(|o| {
            o.ok_or_else(|| StoreError::fatal("a batch without watched keys was skipped"))
        })
    }

    async fn execute_with(
        &self,
        keys: Vec<Bytes>,
        cmds: &[&[Bytes]],
        names: &[String],
        resp3: &[bool],
        uids: &mut Vec<u64>,
        watched: &[Watched],
    ) -> Result<Option<(Vec<Frame>, TicketOf<U>)>, StoreError> {
        let mut b = self.unit.begin(keys).await?;
        let now = self.unit.now_ms();
        // A watched key that changed or expired since its WATCH: nothing
        // runs (the batch holds the watched keys, so none changes now).
        for w in watched {
            let Some(space) = self.all.get(w.db as usize) else {
                return Ok(None);
            };
            if self.versions.token(&space.meta(&w.key)) != w.token
                || w.exp.is_some_and(|e| e <= now)
            {
                return Ok(None);
            }
        }
        let mut cx = Cx {
            b: &mut b,
            space: self.space,
            now,
            all: &self.all,
            uids,
            resp3: false,
            touched: Vec::new(),
        };
        let mut replies = Vec::with_capacity(cmds.len());
        // Keyspace notifications: off costs this one load.
        let flags = self.recording();
        let mut events = Vec::new();
        let mut misses = Vec::new();
        for (i, (c, n)) in cmds.iter().zip(names).enumerate() {
            cx.resp3 = resp3.get(i).copied().unwrap_or(false);
            // `SELECT` inside `EXEC`: the commands after it use that
            // database.
            if n == "select" {
                replies.push(match select_arg(c, &self.all) {
                    Ok(s) => {
                        cx.space = s;
                        Frame::ok()
                    }
                    Err(f) => f,
                });
                continue;
            }
            if flags == 0 {
                replies.push(exec::run(&mut cx, n, c)?);
                continue;
            }
            let mut absent = Vec::new();
            crate::notify::keymiss(&mut cx, n, c, flags, &mut absent)?;
            let before = crate::notify::before(&mut cx, n, c)?;
            let r = exec::run(&mut cx, n, c)?;
            crate::notify::after(&mut cx, before, n, c, &r, flags, &mut events)?;
            if !r.is_error() {
                misses.append(&mut absent);
            }
            replies.push(r);
        }
        let mut touched = std::mem::take(&mut cx.touched);
        touched.sort_unstable();
        touched.dedup();
        self.versions.bump(&touched);
        if names.iter().any(|n| n.starts_with("flush")) {
            self.versions.flushed();
        }
        crate::notify::add_note(&mut b, self.space.tenant, &events)?;
        let ticket = b.commit()?;
        self.waiters.wake(&touched);
        self.direct(misses);
        Ok(Some((replies, ticket)))
    }

    /// `EXEC` of `cmds` after checking `watched` (see
    /// [`EngineDatabase::exec`]).
    pub async fn exec_watched(
        &self,
        cmds: &[&[Bytes]],
        watched: &[Watched],
        resp3: bool,
    ) -> Option<Vec<Frame>> {
        let names: Vec<String> = cmds.iter().map(|c| lower(c)).collect();
        // Lock keys in the database each command runs in.
        let mut keys = Vec::new();
        let mut space = self.space;
        for (c, n) in cmds.iter().zip(&names) {
            if n == "select" {
                if let Ok(s) = select_arg(c, &self.all) {
                    space = s;
                }
            } else {
                keys.extend(exec::lock_keys(n, c, space, &self.all));
            }
        }
        for w in watched {
            if let Some(s) = self.all.get(w.db as usize) {
                keys.push(s.meta(&w.key));
            }
        }
        keys.sort_unstable();
        keys.dedup();
        let protos = vec![resp3; cmds.len()];
        let all_failed = |e: &StoreError| Some(cmds.iter().map(|_| failed(e)).collect());
        let mut attempt = 0;
        let out = loop {
            let (mut uids, epoch) = match self
                .uids
                .reserve(names.iter().map(|n| exec::uids_needed(n)).sum())
                .await
            {
                Ok(u) => u,
                Err(e) => return all_failed(&e),
            };
            let out = self
                .execute_with(keys.clone(), cmds, &names, &protos, &mut uids, watched)
                .await;
            self.uids.give_back(uids, epoch);
            // Lost a deadlock to a script: nothing changed, run it again.
            match out {
                Err(e) if e.retryable && attempt < BATCH_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(1 << attempt)).await;
                }
                other => break other,
            }
        };
        match out {
            Ok(None) => None,
            Ok(Some((replies, ticket))) => match ticket.durable().await {
                Ok(()) => Some(replies),
                Err(e) => all_failed(&e),
            },
            Err(e) => all_failed(&e),
        }
    }

    /// `WATCH` of `keys` (see [`EngineDatabase::watch`]).
    pub async fn watch_keys(&self, keys: &[Bytes]) -> Vec<Watched> {
        let db = self.all.iter().position(|s| *s == self.space).unwrap_or(0) as u32;
        let now = self.unit.now_ms();
        let mut b = match self.unit.begin(Vec::new()).await {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::warn!(error = %e, "WATCH could not read key expiries");
                None
            }
        };
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let meta = self.space.meta(k);
            let token = self.versions.token(&meta);
            let exp = b
                .as_mut()
                .and_then(|b| b.get(&meta).ok().flatten())
                .and_then(|v| crate::layout::Meta::decode(&v))
                .filter(|m| !m.expired(now))
                .and_then(|m| m.exp);
            out.push(Watched {
                db,
                key: k.clone(),
                token,
                exp,
            });
        }
        if let Some(b) = b {
            let _ = b.commit();
        }
        out
    }

    /// Runs blocking command `c` (named `name`): attempts until one pops
    /// something or the timeout passes, holding no key while it waits.
    async fn run_blocking(&self, c: &[Bytes], name: &str, resp3: bool) -> Frame {
        let metas: Vec<Bytes> = exec::blocking::wait_keys(name, c)
            .iter()
            .map(|k| self.space.meta(k))
            .collect();
        let reg = self.waiters.register(metas);
        let mut deadline = None;
        let mut first = true;
        let mut passed = None;
        let out = loop {
            let r = self
                .run_plain(&[c], resp3)
                .await
                .pop()
                .unwrap_or_else(Frame::null);
            // A source that turned into another type after the first attempt
            // keeps the client waiting, as in Redis (a destination of
            // another type is an error, as there); the wake goes on to the
            // next client, once per write.
            let wrong = !first
                && !matches!(name, "blmove" | "brpoplpush")
                && matches!(&r, Frame::Error(e) if e.starts_with(b"WRONGTYPE"));
            first = false;
            if wrong {
                let g = self.waiters.wakes.load(Ordering::Acquire);
                if passed != Some(g) {
                    passed = Some(g);
                    self.waiters.wake_first(&reg.metas, Some(&reg.note));
                }
            }
            if !matches!(r, Frame::Null(_)) && !wrong {
                // Served: the next client in line may find more.
                self.waiters.wake_first(&reg.metas, Some(&reg.note));
                break r;
            }
            // The attempt checked the arguments, so the timeout is valid.
            let t = exec::blocking::timeout(name, c).unwrap_or(0.0);
            let now = Instant::now();
            let d = *deadline
                .get_or_insert_with(|| (t > 0.0).then(|| now + Duration::from_secs_f64(t)));
            let wait = match d {
                Some(d) if d <= now => break exec::blocking::timed_out(name),
                Some(d) => (d - now).min(BLOCK_POLL),
                None => BLOCK_POLL,
            };
            tokio::select! {
                () = reg.note.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        };
        drop(reg);
        out
    }

    /// Runs `cmds` as one batch (see [`EngineDatabase::run`]), for a RESP2
    /// client.
    pub async fn run_batch(&self, cmds: &[&[Bytes]]) -> Vec<Frame> {
        self.run_batch_as(cmds, false).await
    }

    /// Runs `cmds` as one batch for a client speaking RESP3 or not; a
    /// blocking command among them runs on its own, in order.
    pub async fn run_batch_as(&self, cmds: &[&[Bytes]], resp3: bool) -> Vec<Frame> {
        let names: Vec<String> = cmds.iter().map(|c| lower(c)).collect();
        if !names.iter().any(|n| exec::blocking::is_blocking(n)) {
            return self.run_plain(cmds, resp3).await;
        }
        let mut out = Vec::with_capacity(cmds.len());
        let mut start = 0;
        for (i, n) in names.iter().enumerate() {
            if exec::blocking::is_blocking(n) {
                if start < i {
                    out.extend(self.run_plain(&cmds[start..i], resp3).await);
                }
                out.push(self.run_blocking(cmds[i], n, resp3).await);
                start = i + 1;
            }
        }
        if start < cmds.len() {
            out.extend(self.run_plain(&cmds[start..], resp3).await);
        }
        out
    }

    /// Runs `cmds` (none waiting) as one batch.
    async fn run_plain(&self, cmds: &[&[Bytes]], resp3: bool) -> Vec<Frame> {
        let names: Vec<String> = cmds.iter().map(|c| lower(c)).collect();
        let keys = self.write_keys(cmds, &names);
        let outcome = if keys.len() == 1 && !names.iter().any(|n| n.starts_with("flush")) {
            self.combined(
                keys.into_iter().next().unwrap_or_default(),
                cmds,
                names,
                resp3,
            )
            .await
        } else {
            let protos = vec![resp3; cmds.len()];
            // A batch that lost a deadlock to a script (which takes its keys
            // one command at a time) changed nothing: run it again.
            let mut attempt = 0;
            loop {
                match self.execute(keys.clone(), cmds, &names, &protos).await {
                    Err(e) if e.retryable && attempt < BATCH_RETRIES => {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(1 << attempt)).await;
                    }
                    other => break other,
                }
            }
        };
        let all_failed = |e: &StoreError| cmds.iter().map(|_| failed(e)).collect();
        match outcome {
            Ok((replies, ticket)) => match ticket.durable().await {
                Ok(()) => replies,
                Err(e) => all_failed(&e),
            },
            Err(e) => all_failed(&e),
        }
    }

    /// Queues a batch that writes only `key`, leading the key's next round
    /// if nobody does.
    async fn combined(
        &self,
        key: Bytes,
        cmds: &[&[Bytes]],
        names: Vec<String>,
        resp3: bool,
    ) -> Outcome<TicketOf<U>> {
        let (tx, rx) = oneshot::channel();
        let w = Waiting {
            cmds: cmds.iter().map(|c| c.to_vec()).collect(),
            names,
            resp3,
            tx,
        };
        let lead = {
            let mut s = self.shard(&key);
            if let Some(q) = s.get_mut(&key) {
                q.waiting.push(w);
                false
            } else {
                s.insert(key.clone(), Queue { waiting: vec![w] });
                true
            }
        };
        // The round runs as its own task: a client that goes away mid-round
        // doesn't cancel the other batches' round.
        if lead && let Some(me) = self.me.upgrade() {
            Self::spawn_round(me, key);
        }
        rx.await
            .unwrap_or_else(|_| Err(StoreError::retry("the batch's leader ended")))
    }

    /// One round on `key`: every batch queued now, as one transaction.
    /// The key's queue entry stays while a round runs, marking it led.
    async fn round(&self, key: Bytes) {
        // Should the round end early (a panic), the key's queue goes, so
        // later batches lead their own rounds instead of waiting forever;
        // the batches queued meanwhile get a retryable error.
        let mut guard = RoundGuard {
            db: self,
            key: key.clone(),
            armed: true,
        };
        let batch = {
            let mut s = self.shard(&key);
            s.get_mut(&key)
                .map(|q| std::mem::take(&mut q.waiting))
                .unwrap_or_default()
        };
        if !batch.is_empty() {
            let cmds: Vec<&[Bytes]> = batch
                .iter()
                .flat_map(|w| w.cmds.iter().map(Vec::as_slice))
                .collect();
            let names: Vec<String> = batch.iter().flat_map(|w| w.names.iter().cloned()).collect();
            let protos: Vec<bool> = batch
                .iter()
                .flat_map(|w| std::iter::repeat_n(w.resp3, w.cmds.len()))
                .collect();
            match self
                .execute(vec![key.clone()], &cmds, &names, &protos)
                .await
            {
                Ok((replies, ticket)) => {
                    let mut it = replies.into_iter();
                    for w in batch {
                        let mine: Vec<Frame> = it.by_ref().take(w.cmds.len()).collect();
                        let _ = w.tx.send(Ok((mine, ticket.clone())));
                    }
                }
                Err(e) => {
                    for w in batch {
                        let _ = w.tx.send(Err(e.clone()));
                    }
                }
            }
        }
        guard.armed = false;
        // More batches queued while this round ran: another task leads the
        // next round, so this batch's own reply isn't held up.
        let more = {
            let mut s = self.shard(&key);
            if s.get(&key).is_some_and(|q| q.waiting.is_empty()) {
                s.remove(&key);
                false
            } else {
                true
            }
        };
        if more && let Some(me) = self.me.upgrade() {
            Self::spawn_round(me, key);
        }
    }

    fn spawn_round(me: Arc<Self>, key: Bytes) {
        tokio::spawn(async move { me.round(key).await });
    }
}

#[async_trait]
impl<U: Unit> EngineDatabase for EngineDb<U> {
    fn runs(&self, name: &str) -> bool {
        exec::runs(name)
    }

    async fn run(&self, cmds: &[&[Bytes]], resp3: bool) -> Vec<Frame> {
        self.run_batch_as(cmds, resp3).await
    }

    async fn watch(&self, keys: &[Bytes]) -> Vec<Watched> {
        self.watch_keys(keys).await
    }

    async fn exec(
        &self,
        cmds: &[&[Bytes]],
        watched: &[Watched],
        resp3: bool,
    ) -> Option<Vec<Frame>> {
        self.exec_watched(cmds, watched, resp3).await
    }

    async fn script(
        &self,
        watched: &[Watched],
    ) -> Result<Option<Box<dyn sprig_redis::engine::EngineScript>>, Frame> {
        let me = self
            .me
            .upgrade()
            .ok_or_else(|| Frame::err("the database is closing"))?;
        match me.begin_script(watched).await {
            Ok(Some(t)) => Ok(Some(Box::new(t))),
            Ok(None) => Ok(None),
            Err(e) => Err(failed(&e)),
        }
    }

    async fn lua_get(&self, store: LuaStore, name: &[u8]) -> Result<Option<Bytes>, Frame> {
        self.lua_read(store, name).await.map_err(|e| failed(&e))
    }

    async fn lua_swap(
        &self,
        store: LuaStore,
        name: &[u8],
        expect: Option<Option<Bytes>>,
        new: Option<Bytes>,
    ) -> Result<bool, Frame> {
        self.lua_write(store, name, expect, new)
            .await
            .map_err(|e| failed(&e))
    }

    async fn lua_clear(&self, store: LuaStore) -> Result<(), Frame> {
        self.lua_wipe(store).await.map_err(|e| failed(&e))
    }
}

#[cfg(test)]
mod uid_tests {
    use sprig_engine::config::{EngineConfig, StoreKind};
    use sprig_engine::{Engine, UnitId};

    use super::*;
    use crate::engine_unit::EngineUnit;

    /// A node that takes the tenant's leadership again drops the uids it
    /// held: the next ones come from a new block of the counter.
    #[tokio::test]
    async fn uids_held_under_an_older_leader_epoch_are_dropped() {
        let d = std::env::temp_dir().join(format!("redis-uids-epoch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let mut c = EngineConfig::new(&d);
        c.store = StoreKind::Mem;
        let engine = Engine::open(c).unwrap();
        let uids = Uids::new(Arc::new(EngineUnit::on_engine(engine.clone())), 7);
        let (a, e1) = uids.reserve(2).await.unwrap();
        assert_eq!(a, vec![1, 2]);
        uids.give_back(vec![2], e1);
        assert_eq!(uids.reserve(1).await.unwrap().0, vec![2]);
        let e2 = engine.lead(UnitId(7));
        assert_ne!(e1, e2);
        let (b, e) = uids.reserve(1).await.unwrap();
        assert_eq!(e, e2);
        assert!(b[0] > UID_BLOCK, "{b:?}");
        // A uid given back under the old epoch is not kept.
        uids.give_back(vec![3], e1);
        assert_ne!(uids.reserve(1).await.unwrap().0, vec![3]);
        drop(engine);
        let _ = std::fs::remove_dir_all(&d);
    }
}
