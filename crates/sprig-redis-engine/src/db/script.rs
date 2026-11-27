//! Lua scripts on an engine database: a script's transaction, and the
//! tenant's script cache and function libraries.
//!
//! A script's transaction is one engine batch. It starts holding only the
//! keys an `EXEC` watches; before each command the script calls, it takes
//! the metadata entries of the keys that command names (those it may
//! write, and those it reads), waiting for other batches as any batch
//! does. Every read of the batch is a current read, and every key a
//! command touches is held before the command runs, so the script sees and
//! changes the data as if it ran alone (keys read only through a pattern,
//! such as `KEYS`, `SCAN` or `SORT ... BY`, are read at their latest
//! version without a hold). The batch commits once, when the script ends;
//! dropped, it changes nothing. A deadlock between two scripts fails one
//! of them with a retryable error, and its caller runs it again.
//!
//! Scripts and functions live in database 0's part of the keyspace under
//! their own kind, which `FLUSHDB` and `FLUSHALL` don't reach.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::engine::{EngineScript, LuaStore, ScriptFailed, Watched};

use super::{EngineDb, failed, lower, select_arg};
use crate::exec::{self, Cx};
use crate::layout::{SCRIPT_CACHE, SCRIPT_FUNCTIONS, Space};
use crate::store::{Batch, StoreError, Ticket, Unit};

/// Runs one command of a script, adding its keyspace events.
fn notified<B: Batch>(
    cx: &mut Cx<'_, B>,
    name: &str,
    a: &[Bytes],
    flags: u16,
    events: &mut Vec<crate::notify::Event>,
    misses: &mut Vec<crate::notify::Event>,
) -> exec::Out {
    let mut absent = Vec::new();
    crate::notify::keymiss(cx, name, a, flags, &mut absent)?;
    let before = crate::notify::before(cx, name, a)?;
    let r = exec::run(cx, name, a)?;
    crate::notify::after(cx, before, name, a, &r, flags, events)?;
    if !r.is_error() {
        misses.append(&mut absent);
    }
    Ok(r)
}

/// A script's failure from an engine error.
fn script_failed(e: &StoreError) -> ScriptFailed {
    ScriptFailed {
        reply: failed(e),
        retry: e.retryable,
    }
}

/// A Lua script's transaction (see the module's notes).
pub struct ScriptTxn<U: Unit> {
    db: Arc<EngineDb<U>>,
    b: Option<U::Batch>,
    /// The entries the batch holds, sorted.
    held: Vec<Bytes>,
    /// The database the next command runs in (`SELECT` changes it).
    space: Space,
    /// The clock, read once: time stands still while a script runs, as in
    /// Redis, so a key can't expire halfway.
    now: i64,
    uids: Vec<u64>,
    epoch: u64,
    touched: Vec<Bytes>,
    flushed: bool,
    /// Keyspace notifications' flags, read when the script began (0: off),
    /// and the events of its commands so far.
    flags: u16,
    events: Vec<crate::notify::Event>,
    misses: Vec<crate::notify::Event>,
}

impl<U: Unit> std::fmt::Debug for ScriptTxn<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptTxn")
            .field("space", &self.space)
            .field("held", &self.held.len())
            .finish_non_exhaustive()
    }
}

impl<U: Unit> Drop for ScriptTxn<U> {
    fn drop(&mut self) {
        // Uids nobody used go back (a rolled-back batch wrote none).
        let uids = std::mem::take(&mut self.uids);
        if !uids.is_empty() {
            self.db.uids.give_back(uids, self.epoch);
        }
    }
}

impl<U: Unit> EngineDb<U> {
    /// Opens a script's transaction in this database, after checking that
    /// no key of `watched` changed since its `WATCH` (`Ok(None)`: one did).
    ///
    /// # Errors
    /// The engine's.
    pub async fn begin_script(
        self: &Arc<Self>,
        watched: &[Watched],
    ) -> Result<Option<ScriptTxn<U>>, StoreError> {
        let mut keys: Vec<Bytes> = watched
            .iter()
            .filter_map(|w| self.all.get(w.db as usize).map(|s| s.meta(&w.key)))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        let mut b = self.unit.begin(keys.clone()).await?;
        b.read_latest();
        let now = self.unit.now_ms();
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
        Ok(Some(ScriptTxn {
            db: Arc::clone(self),
            b: Some(b),
            held: keys,
            space: self.space,
            now,
            uids: Vec::new(),
            epoch: 0,
            touched: Vec::new(),
            flushed: false,
            flags: self.recording(),
            events: Vec::new(),
            misses: Vec::new(),
        }))
    }

    /// Database 0 of the tenant: where its scripts and functions live.
    fn script_space(&self) -> Space {
        self.all.first().copied().unwrap_or(self.space)
    }

    /// The stored entry `name` of `store`.
    fn script_entry(&self, store: LuaStore, name: &[u8]) -> Bytes {
        let index = match store {
            LuaStore::Scripts => SCRIPT_CACHE,
            LuaStore::Functions => SCRIPT_FUNCTIONS,
        };
        self.script_space().script_entry(index, name)
    }

    /// See [`sprig_redis::engine::EngineDatabase::lua_get`].
    ///
    /// # Errors
    /// The engine's.
    pub async fn lua_read(
        &self,
        store: LuaStore,
        name: &[u8],
    ) -> Result<Option<Bytes>, StoreError> {
        let key = self.script_entry(store, name);
        let mut b = self.unit.begin(Vec::new()).await?;
        b.read_latest();
        let v = b.get(&key)?;
        b.commit()?.durable().await?;
        Ok(v)
    }

    /// See [`sprig_redis::engine::EngineDatabase::lua_swap`].
    ///
    /// # Errors
    /// The engine's.
    pub async fn lua_write(
        &self,
        store: LuaStore,
        name: &[u8],
        expect: Option<Option<Bytes>>,
        new: Option<Bytes>,
    ) -> Result<bool, StoreError> {
        let key = self.script_entry(store, name);
        let mut b = self.unit.begin(vec![key.clone()]).await?;
        b.read_latest();
        let cur = b.get(&key)?;
        if expect.is_some_and(|e| e != cur) {
            return Ok(false);
        }
        if cur == new {
            return Ok(true);
        }
        match new {
            Some(v) => b.put(key, v)?,
            None => b.delete(key)?,
        }
        b.commit()?.durable().await?;
        Ok(true)
    }

    /// See [`sprig_redis::engine::EngineDatabase::lua_clear`].
    ///
    /// # Errors
    /// The engine's.
    pub async fn lua_wipe(&self, store: LuaStore) -> Result<(), StoreError> {
        let index = match store {
            LuaStore::Scripts => SCRIPT_CACHE,
            LuaStore::Functions => SCRIPT_FUNCTIONS,
        };
        let (start, end) = self.script_space().script_range(index);
        let mut from = start;
        loop {
            let mut b = self.unit.begin(Vec::new()).await?;
            b.read_latest();
            let got = b.scan_latest(&from, &end, 1024)?;
            let Some((last, _)) = got.last() else {
                return Ok(());
            };
            from = crate::layout::after(last);
            // Each entry is held as its writers hold it.
            let keys: Vec<Bytes> = got.into_iter().map(|(k, _)| k).collect();
            b.hold(keys.clone()).await?;
            for k in keys {
                b.delete(k)?;
            }
            b.commit()?.durable().await?;
        }
    }
}

impl<U: Unit> ScriptTxn<U> {
    fn batch(&mut self) -> Result<&mut U::Batch, ScriptFailed> {
        self.b.as_mut().ok_or_else(|| ScriptFailed {
            reply: Frame::err("the script's transaction has ended"),
            retry: false,
        })
    }

    /// The metadata entries command `a` (named `name`) names: those it may
    /// write, and those it reads.
    fn named(&self, name: &str, a: &[Bytes]) -> Vec<Bytes> {
        let mut want = exec::lock_keys(name, a, self.space, &self.db.all);
        // Keys after a count, which the command table's positions miss.
        let counted = match name {
            "zunion" | "zinter" | "zdiff" | "zintercard" | "sintercard" | "lmpop" | "zmpop" => {
                Some(1)
            }
            "zunionstore" | "zinterstore" | "zdiffstore" | "blmpop" | "bzmpop" => Some(2),
            _ => None,
        };
        // Stream reads name their keys after STREAMS, or after a
        // subcommand.
        match name {
            "xread" => want.extend(
                exec::streams::read_keys(a, false)
                    .iter()
                    .map(|k| self.space.meta(k)),
            ),
            "xinfo" => want.extend(a.get(2).map(|k| self.space.meta(k))),
            _ => {}
        }
        if let Some(at) = counted {
            let n = a
                .get(at)
                .and_then(|n| sprig_proto_redis::num::parse_i64(n))
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(0);
            want.extend(a.iter().skip(at + 1).take(n).map(|k| self.space.meta(k)));
        }
        if let Some(s) = sprig_redis::engine::lookup(name) {
            want.extend(
                s.key_indexes(a.len())
                    .into_iter()
                    .filter_map(|i| a.get(i))
                    .map(|k| self.space.meta(k)),
            );
        }
        want.sort_unstable();
        want.dedup();
        want.retain(|k| self.held.binary_search(k).is_err());
        want
    }

    /// Makes sure `need` uids are at hand.
    async fn uids_for(&mut self, need: usize) -> Result<(), StoreError> {
        if self.uids.len() >= need {
            return Ok(());
        }
        let (more, epoch) = self.db.uids.reserve(need - self.uids.len()).await?;
        if epoch != self.epoch && !self.uids.is_empty() {
            let old = std::mem::take(&mut self.uids);
            self.db.uids.give_back(old, self.epoch);
        }
        self.epoch = epoch;
        self.uids.extend(more);
        Ok(())
    }
}

#[async_trait]
impl<U: Unit> EngineScript for ScriptTxn<U> {
    async fn call(&mut self, a: &[Bytes], resp3: bool) -> Result<Frame, ScriptFailed> {
        let name = lower(a);
        if name == "select" {
            return Ok(match select_arg(a, &self.db.all) {
                Ok(s) => {
                    self.space = s;
                    Frame::ok()
                }
                Err(f) => f,
            });
        }
        let want = self.named(&name, a);
        if !want.is_empty() {
            self.batch()?
                .hold(want.clone())
                .await
                .map_err(|e| script_failed(&e))?;
            self.held.extend(want);
            self.held.sort_unstable();
        }
        self.uids_for(exec::uids_needed(&name))
            .await
            .map_err(|e| script_failed(&e))?;
        let space = self.space;
        let now = self.now;
        let db = Arc::clone(&self.db);
        let mut uids = std::mem::take(&mut self.uids);
        let flags = self.flags;
        let mut events = std::mem::take(&mut self.events);
        let mut misses = std::mem::take(&mut self.misses);
        let b = self.batch()?;
        let mut cx = Cx {
            b,
            space,
            now,
            all: &db.all,
            uids: &mut uids,
            resp3,
            touched: Vec::new(),
            events: Vec::new(),
        };
        let r = if flags == 0 {
            exec::run(&mut cx, &name, a)
        } else {
            notified(&mut cx, &name, a, flags, &mut events, &mut misses)
        };
        let touched = std::mem::take(&mut cx.touched);
        self.events = events;
        self.misses = misses;
        self.uids = uids;
        self.touched.extend(touched);
        if name.starts_with("flush") {
            self.flushed = true;
        }
        r.map_err(|e| script_failed(&e))
    }

    async fn commit(mut self: Box<Self>) -> Result<(), ScriptFailed> {
        let b = self.b.take().ok_or_else(|| ScriptFailed {
            reply: Frame::err("the script's transaction has ended"),
            retry: false,
        })?;
        let mut touched = std::mem::take(&mut self.touched);
        touched.sort_unstable();
        touched.dedup();
        self.db.versions.bump(&touched);
        if self.flushed {
            self.db.versions.flushed();
        }
        let mut b = b;
        crate::notify::add_note(&mut b, self.db.space.tenant, &self.events)
            .map_err(|e| script_failed(&e))?;
        let ticket = b.commit().map_err(|e| script_failed(&e))?;
        self.db.waiters.wake(&touched);
        self.db.direct(std::mem::take(&mut self.misses));
        // Once queued, the commit may have happened: never run it again.
        ticket.durable().await.map_err(|e| ScriptFailed {
            reply: failed(&e),
            retry: false,
        })
    }
}
