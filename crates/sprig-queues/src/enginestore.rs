//! Queue messages stored on the node's engine (no MariaDB): an ordered
//! keyspace per vhost, written straight through the engine's commit log.
//!
//! ```text
//! P(kind, index) = META_TENANT | 0 | OBJECT | kind | index | tenant u64 | db u32
//! P(E,0) | queue u64 | lane u16 | pclass u8 | seq u64  -> message
//! P(S,0) | queue u64 | msg_id                          -> lane u16 | pclass u8 | seq u64
//! P(M,0) | queue u64 | lane u16                        -> epoch u64 | lane state
//! P(E,1) | due_ms i64 | msg_id                         -> delayed message
//! P(E,2) | out_id                                      -> outbox row
//! P(E,3) | queue u64 | id                              -> forward-ledger time (ms)
//! ```
//!
//! **Why outside MVCC.** Messages are written once and deleted once, by
//! one owner: they need no versions. Unversioned keys let an acknowledged
//! message be a plain delete, so a message published and acknowledged
//! between two memtable flushes never reaches an SST (RocksDB drops the
//! shadowed put at flush), and a run of acknowledgements in sequence order
//! is one range delete. They sit under the engine's reserved tenant (with
//! the real tenant and database right after), where the version GC and
//! the clock ignore them.
//!
//! **One owner batch is one log entry**: its puts and deletes are applied
//! together or not at all. The owner epoch fence is checked in memory
//! before the entry is proposed; every writer of the vhost's store goes
//! through the one [`EngineQueues`] instance of the node, so the check
//! and the proposal order agree. A write returns once its entry is
//! durable and applied (publisher confirms follow from that).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use sprig_engine::keys::{KeyPrefix, KeyRange, META_TENANT, kind, memcmp, prefix_end};
use sprig_engine::log::Payload;
use sprig_engine::store::{Cf, IterOpts, ReadTier};
use sprig_engine::{Durability, Engine};

use crate::store::{
    Claim, DelayedRow, LaneState, MsgId, MsgRef, MsgStore, OutDest, OutKind, OutboxRow, StoreError,
    StoredMsg, WriteBatch,
};

/// The queues object under the engine's reserved tenant (`'Q'`).
pub const OBJECT: u32 = 0x5100_0000;

const MSGS: (u8, u16) = (kind::ELEMENTS, 0);
const IDS: (u8, u16) = (kind::SECONDARY, 0);
const LANES: (u8, u16) = (kind::META, 0);
const DELAYED: (u8, u16) = (kind::ELEMENTS, 1);
const OUTBOX: (u8, u16) = (kind::ELEMENTS, 2);
const LEDGER: (u8, u16) = (kind::ELEMENTS, 3);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn failed(e: impl std::fmt::Display) -> StoreError {
    StoreError::Failed(e.to_string())
}

/// Every vhost store of the node, one instance each.
pub struct EngineQueues {
    engine: Engine,
    durability: Durability,
    stores: Mutex<HashMap<(u64, u32), Arc<EngineMsgStore>>>,
}

impl std::fmt::Debug for EngineQueues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineQueues").finish_non_exhaustive()
    }
}

impl EngineQueues {
    /// Over `engine`, committing with `durability`.
    #[must_use]
    pub fn new(engine: Engine, durability: Durability) -> Self {
        Self {
            engine,
            durability,
            stores: Mutex::new(HashMap::new()),
        }
    }

    /// The store of one vhost (tenant, database).
    #[must_use]
    pub fn store(&self, tenant: u64, db: u32) -> Arc<EngineMsgStore> {
        Arc::clone(lock(&self.stores).entry((tenant, db)).or_insert_with(|| {
            Arc::new(EngineMsgStore {
                engine: self.engine.clone(),
                durability: self.durability,
                scope: (tenant, db),
                lanes: Mutex::new(HashMap::new()),
            })
        }))
    }
}

/// One vhost's messages on the engine.
pub struct EngineMsgStore {
    engine: Engine,
    durability: Durability,
    scope: (u64, u32),
    /// Each lane's newest proposed (epoch, state): the fence, and what a
    /// claim starts from (a write proposed but not yet applied is already
    /// in it). Held while a write checks the fence and proposes, and while
    /// a claim reads and proposes, so the two are ordered.
    lanes: Mutex<HashMap<(u64, u16), (u64, LaneState)>>,
}

impl std::fmt::Debug for EngineMsgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineMsgStore")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// A plain store write a SQL transaction on the engine carries for a
/// queue call made inside it ([`EngineMsgStore::txn_publish`],
/// [`EngineMsgStore::txn_ack`]): applied with the transaction's commit or
/// not at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOp {
    /// Put `key` = `value` (column family data).
    Put(Vec<u8>, Vec<u8>),
    /// Delete `key`.
    Delete(Vec<u8>),
}

impl EngineMsgStore {
    /// Waits (at most two seconds) until the engine applied every entry
    /// that was durable when called.
    async fn caught_up(&self) {
        let c = self.engine.consensus();
        let u = self.engine.unit();
        let target = c.durable_index(u);
        for _ in 0..2000 {
            if c.applied_index(u) >= target {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// The write of a publish inside a SQL transaction: the outbox row,
    /// which the vhost's forwarder routes once the commit has applied
    /// (as the MariaDB store's outbox row inserted in the transaction).
    #[must_use]
    pub fn txn_publish(&self, o: &OutboxRow) -> TxnOp {
        TxnOp::Put(self.out_key(&o.out_id), enc_out(o))
    }

    /// The write of a delayed publish inside a SQL transaction.
    #[must_use]
    pub fn txn_delay(&self, d: &DelayedRow) -> TxnOp {
        TxnOp::Put(self.delayed_key(d.due_ms, &d.msg_id), enc_delayed(d))
    }

    /// The writes of an acknowledgement inside a SQL transaction: the
    /// message and its id key go when the commit applies. `None` when the
    /// message is no longer stored (already settled). The caller holds the
    /// delivery for the transaction, so the owner neither redelivers nor
    /// rewrites the message meanwhile.
    ///
    /// # Errors
    /// The store can't be read.
    pub fn txn_ack(&self, queue: u64, msg_id: &MsgId) -> Result<Option<Vec<TxnOp>>, StoreError> {
        let k = self.id_key(queue, msg_id);
        let Some(loc) = self.get(&k)? else {
            return Ok(None);
        };
        if loc.len() != 11 {
            return Err(failed("queue store: an id key is corrupt"));
        }
        let lane = u16::from_be_bytes([loc[0], loc[1]]);
        let mut seq = [0u8; 8];
        seq.copy_from_slice(&loc[3..11]);
        let m = self.msg_key(queue, lane, loc[2], u64::from_be_bytes(seq));
        Ok(Some(vec![TxnOp::Delete(m), TxnOp::Delete(k)]))
    }
}

// ----- encoding ---------------------------------------------------------

struct W(Vec<u8>);

impl W {
    fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn i64(&mut self, v: i64) -> &mut Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.u32(u32::try_from(b.len()).unwrap_or(u32::MAX));
        self.0.extend_from_slice(b);
        self
    }
    fn opt(&mut self, b: Option<&[u8]>) -> &mut Self {
        match b {
            None => self.u8(0),
            Some(b) => self.u8(1).bytes(b),
        }
    }
}

struct R<'a>(&'a [u8]);

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], StoreError> {
        if self.0.len() < n {
            return Err(failed("queue store: corrupt record"));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    fn u8(&mut self) -> Result<u8, StoreError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, StoreError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(failed)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, StoreError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(failed)?,
        ))
    }
    fn i64(&mut self) -> Result<i64, StoreError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().map_err(failed)?,
        ))
    }
    fn bytes(&mut self) -> Result<&'a [u8], StoreError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn string(&mut self) -> Result<String, StoreError> {
        Ok(String::from_utf8_lossy(self.bytes()?).into_owned())
    }
    fn opt(&mut self) -> Result<Option<&'a [u8]>, StoreError> {
        Ok(match self.u8()? {
            0 => None,
            _ => Some(self.bytes()?),
        })
    }
    fn id(&mut self) -> Result<MsgId, StoreError> {
        self.take(16)?.try_into().map_err(failed)
    }
}

fn enc_msg(m: &StoredMsg) -> Vec<u8> {
    let mut w = W(Vec::with_capacity(64 + m.props.len() + m.body.len()));
    w.u8(1);
    w.0.extend_from_slice(&m.msg_id);
    w.u8(m.fmt)
        .bytes(m.exchange.as_bytes())
        .bytes(m.routing_key.as_bytes())
        .bytes(&m.props)
        .bytes(&m.body);
    match m.expires_at_ms {
        None => w.u8(0),
        Some(t) => w.u8(1).i64(t),
    };
    w.u32(m.delivery_count).opt(m.deaths.as_deref());
    w.0
}

fn dec_msg(lane: u16, pclass: u8, seq: u64, b: &[u8]) -> Result<StoredMsg, StoreError> {
    let mut r = R(b);
    if r.u8()? != 1 {
        return Err(failed("queue store: unknown message format"));
    }
    let msg_id = r.id()?;
    let fmt = r.u8()?;
    let exchange = r.string()?;
    let routing_key = r.string()?;
    let props = Bytes::copy_from_slice(r.bytes()?);
    let body = Bytes::copy_from_slice(r.bytes()?);
    let expires_at_ms = match r.u8()? {
        0 => None,
        _ => Some(r.i64()?),
    };
    let delivery_count = r.u32()?;
    let deaths = r.opt()?.map(Bytes::copy_from_slice);
    Ok(StoredMsg {
        lane,
        pclass,
        seq,
        msg_id,
        fmt,
        exchange,
        routing_key,
        props,
        body,
        expires_at_ms,
        delivery_count,
        deaths,
    })
}

fn enc_out(o: &OutboxRow) -> Vec<u8> {
    let mut w = W(Vec::with_capacity(64 + o.props.len() + o.body.len()));
    w.u8(1);
    w.0.extend_from_slice(&o.out_id);
    w.0.extend_from_slice(&o.msg_id);
    w.bytes(o.kind.as_str().as_bytes())
        .bytes(&serde_json::to_vec(&o.dest).unwrap_or_default())
        .u8(o.fmt)
        .bytes(o.exchange.as_bytes())
        .bytes(o.routing_key.as_bytes())
        .bytes(&o.props)
        .bytes(&o.body)
        .opt(o.deaths.as_deref());
    w.0
}

fn dec_out(b: &[u8]) -> Result<OutboxRow, StoreError> {
    let mut r = R(b);
    if r.u8()? != 1 {
        return Err(failed("queue store: unknown outbox format"));
    }
    let out_id = r.id()?;
    let msg_id = r.id()?;
    let kind = OutKind::parse(&r.string()?).ok_or_else(|| failed("queue store: outbox kind"))?;
    let dest: OutDest = serde_json::from_slice(r.bytes()?).map_err(failed)?;
    Ok(OutboxRow {
        out_id,
        kind,
        dest,
        msg_id,
        fmt: r.u8()?,
        exchange: r.string()?,
        routing_key: r.string()?,
        props: Bytes::copy_from_slice(r.bytes()?),
        body: Bytes::copy_from_slice(r.bytes()?),
        deaths: r.opt()?.map(Bytes::copy_from_slice),
    })
}

fn enc_delayed(d: &DelayedRow) -> Vec<u8> {
    let mut w = W(Vec::new());
    w.u8(1).u64(d.exchange_id);
    w.bytes(d.exchange.as_bytes())
        .bytes(d.routing_key.as_bytes())
        .u8(d.fmt)
        .bytes(&d.props)
        .bytes(&d.body);
    w.0
}

fn dec_delayed(due_ms: i64, msg_id: MsgId, b: &[u8]) -> Result<DelayedRow, StoreError> {
    let mut r = R(b);
    if r.u8()? != 1 {
        return Err(failed("queue store: unknown delayed format"));
    }
    Ok(DelayedRow {
        exchange_id: r.u64()?,
        exchange: r.string()?,
        due_ms,
        msg_id,
        routing_key: r.string()?,
        fmt: r.u8()?,
        props: Bytes::copy_from_slice(r.bytes()?),
        body: Bytes::copy_from_slice(r.bytes()?),
    })
}

fn enc_lane(epoch: u64, s: &LaneState) -> Vec<u8> {
    let mut w = W(Vec::with_capacity(40));
    w.u64(epoch)
        .u64(s.next_seq)
        .u64(s.delivered_hwm)
        .u64(s.ready_count)
        .u64(s.ready_bytes);
    w.0
}

fn dec_lane(b: &[u8]) -> Result<(u64, LaneState), StoreError> {
    let mut r = R(b);
    Ok((
        r.u64()?,
        LaneState {
            next_seq: r.u64()?,
            delivered_hwm: r.u64()?,
            ready_count: r.u64()?,
            ready_bytes: r.u64()?,
        },
    ))
}

// ----- the store --------------------------------------------------------

impl EngineMsgStore {
    fn p(&self, (k, index): (u8, u16)) -> Vec<u8> {
        let mut v = KeyPrefix {
            tenant: META_TENANT,
            shard: 0,
            object: OBJECT,
            kind: k,
            index,
        }
        .to_vec();
        v.extend_from_slice(&self.scope.0.to_be_bytes());
        v.extend_from_slice(&self.scope.1.to_be_bytes());
        v
    }

    fn queue_p(&self, f: (u8, u16), queue: u64) -> Vec<u8> {
        let mut v = self.p(f);
        v.extend_from_slice(&queue.to_be_bytes());
        v
    }

    fn lane_p(&self, queue: u64, lane: u16) -> Vec<u8> {
        let mut v = self.queue_p(MSGS, queue);
        v.extend_from_slice(&lane.to_be_bytes());
        v
    }

    fn msg_key(&self, queue: u64, lane: u16, pclass: u8, seq: u64) -> Vec<u8> {
        let mut v = self.lane_p(queue, lane);
        v.push(pclass);
        v.extend_from_slice(&seq.to_be_bytes());
        v
    }

    fn id_key(&self, queue: u64, id: &MsgId) -> Vec<u8> {
        let mut v = self.queue_p(IDS, queue);
        v.extend_from_slice(id);
        v
    }

    fn lane_key(&self, queue: u64, lane: u16) -> Vec<u8> {
        let mut v = self.queue_p(LANES, queue);
        v.extend_from_slice(&lane.to_be_bytes());
        v
    }

    fn ledger_key(&self, queue: u64, id: &MsgId) -> Vec<u8> {
        let mut v = self.queue_p(LEDGER, queue);
        v.extend_from_slice(id);
        v
    }

    fn out_key(&self, id: &MsgId) -> Vec<u8> {
        let mut v = self.p(OUTBOX);
        v.extend_from_slice(id);
        v
    }

    fn delayed_key(&self, due_ms: i64, id: &MsgId) -> Vec<u8> {
        let mut v = self.p(DELAYED);
        memcmp::put_i64(&mut v, due_ms);
        v.extend_from_slice(id);
        v
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StoreError> {
        self.engine
            .store()
            .get(Cf::Data, key, ReadTier::All)
            .and_then(sprig_engine::Read::expect_ready)
            .map_err(failed)
    }

    /// Up to `limit` entries of `[lo, hi)`.
    fn scan(
        &self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let r = KeyRange::new(lo, hi);
        let mut it = self.engine.store().iter(Cf::Data, IterOpts::range(&r));
        it.seek(lo);
        let mut out = Vec::new();
        while it.valid() && out.len() < limit {
            out.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        it.status()
            .and_then(sprig_engine::Read::expect_ready)
            .map_err(failed)?;
        Ok(out)
    }

    fn prefix_scan(&self, p: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
        let end = prefix_end(p).unwrap_or_default();
        self.scan(p, &end, limit)
    }

    /// Proposes `p` (its place in the log is taken now).
    fn propose(&self, p: Payload) -> Result<sprig_engine::log::Ticket, StoreError> {
        self.engine
            .consensus()
            .propose(self.engine.unit(), p.into_bytes(), self.durability)
            .map_err(failed)
    }

    /// Waits until a proposed entry is durable and applied.
    async fn finish(t: sprig_engine::log::Ticket) -> Result<(), StoreError> {
        t.wait_async().await.map_err(failed)?;
        t.wait_applied_async().await.map_err(failed)
    }

    /// Proposes `p` and waits until it is durable and applied.
    async fn commit(&self, p: Payload) -> Result<(), StoreError> {
        let t = self.propose(p)?;
        Self::finish(t).await
    }

    /// Proposes `p` for lane `(queue, lane)` if `epoch` still owns it,
    /// atomically with the check; `state` (when given) becomes the lane's
    /// newest state. Then waits for it.
    async fn fenced_commit(
        &self,
        queue: u64,
        lane: u16,
        epoch: u64,
        state: Option<LaneState>,
        p: Payload,
    ) -> Result<(), StoreError> {
        let t = {
            let mut g = lock(&self.lanes);
            let current = if let Some(x) = g.get(&(queue, lane)) {
                *x
            } else {
                let x = self.lane_state(queue, lane)?.ok_or(StoreError::Fenced)?;
                g.insert((queue, lane), x);
                x
            };
            if current.0 != epoch {
                return Err(StoreError::Fenced);
            }
            let t = self.propose(p)?;
            if let Some(st) = state {
                g.insert((queue, lane), (epoch, st));
            }
            t
        };
        Self::finish(t).await
    }

    fn lane_state(&self, queue: u64, lane: u16) -> Result<Option<(u64, LaneState)>, StoreError> {
        self.get(&self.lane_key(queue, lane))?
            .map(|b| dec_lane(&b))
            .transpose()
    }

    fn fence(&self, queue: u64, lane: u16, epoch: u64) -> Result<(), StoreError> {
        let known = lock(&self.lanes).get(&(queue, lane)).map(|x| x.0);
        let current = match known {
            Some(e) => e,
            None => match self.lane_state(queue, lane)? {
                Some(x) => lock(&self.lanes).entry((queue, lane)).or_insert(x).0,
                None => return Err(StoreError::Fenced),
            },
        };
        if current == epoch {
            Ok(())
        } else {
            Err(StoreError::Fenced)
        }
    }
}

/// Adds deletes of the messages `refs` of one lane: runs of consecutive
/// sequence numbers in one priority class become one range delete.
fn delete_runs(p: &mut Payload, s: &EngineMsgStore, queue: u64, lane: u16, refs: &[MsgRef]) {
    let mut sorted: Vec<&MsgRef> = refs.iter().collect();
    sorted.sort_by_key(|r| (r.pclass, r.seq));
    let mut i = 0;
    while i < sorted.len() {
        let mut j = i;
        while j + 1 < sorted.len()
            && sorted[j + 1].pclass == sorted[i].pclass
            && sorted[j + 1].seq == sorted[j].seq + 1
        {
            j += 1;
        }
        let first = sorted[i];
        if j == i {
            p.delete(Cf::Data, &s.msg_key(queue, lane, first.pclass, first.seq));
        } else {
            p.delete_range(
                Cf::Data,
                &s.msg_key(queue, lane, first.pclass, first.seq),
                &s.msg_key(queue, lane, first.pclass, sorted[j].seq + 1),
            );
        }
        i = j + 1;
    }
}

#[async_trait]
impl MsgStore for EngineMsgStore {
    async fn claim(&self, queue: u64, lane: u16) -> Result<Claim, StoreError> {
        let (t, epoch, state) = {
            let mut g = lock(&self.lanes);
            let newest = match g.get(&(queue, lane)) {
                Some(x) => Some(*x),
                None => self.lane_state(queue, lane)?,
            };
            let (epoch, state) = match newest {
                Some((e, s)) => (e + 1, s),
                None => (
                    1,
                    LaneState {
                        next_seq: 1,
                        ..LaneState::default()
                    },
                ),
            };
            let mut p = Payload::new();
            p.put(
                Cf::Data,
                &self.lane_key(queue, lane),
                &enc_lane(epoch, &state),
            );
            let t = self.propose(p)?;
            g.insert((queue, lane), (epoch, state));
            (t, epoch, state)
        };
        // Applied in log order: every earlier write of the lane is applied
        // too, so the scan below sees all its messages.
        Self::finish(t).await?;
        let mut count = 0u64;
        let mut bytes = 0u64;
        let lp = self.lane_p(queue, lane);
        let mut from = lp.clone();
        let end = prefix_end(&lp).unwrap_or_default();
        loop {
            let page = self.scan(&from, &end, 4096)?;
            for (k, v) in &page {
                let m = dec_msg(lane, 0, 0, v)?;
                count += 1;
                bytes += m.size();
                from.clone_from(k);
            }
            if page.len() < 4096 {
                break;
            }
            from.push(0);
        }
        Ok(Claim {
            epoch,
            state,
            count,
            bytes,
        })
    }

    async fn load(
        &self,
        queue: u64,
        lane: u16,
        after: Option<(u8, u64)>,
        limit: usize,
    ) -> Result<Vec<StoredMsg>, StoreError> {
        let lp = self.lane_p(queue, lane);
        let end = prefix_end(&lp).unwrap_or_default();
        let from = match after {
            None => lp.clone(),
            Some((pc, seq)) => match seq.checked_add(1) {
                Some(n) => self.msg_key(queue, lane, pc, n),
                None => self.msg_key(queue, lane, pc.saturating_add(1), 0),
            },
        };
        self.scan(&from, &end, limit)?
            .into_iter()
            .map(|(k, v)| {
                let tail = &k[lp.len()..];
                let pclass = tail[0];
                let seq = u64::from_be_bytes(tail[1..9].try_into().map_err(failed)?);
                dec_msg(lane, pclass, seq, &v)
            })
            .collect()
    }

    async fn existing(&self, queue: u64, ids: &[MsgId]) -> Result<Vec<MsgId>, StoreError> {
        // A SQL transaction's acknowledgement is durable when its session
        // asks the owner to check: wait until the engine applied it.
        self.caught_up().await;
        let mut out = Vec::new();
        for id in ids {
            if self.get(&self.id_key(queue, id))?.is_some()
                || self.get(&self.ledger_key(queue, id))?.is_some()
            {
                out.push(*id);
            }
        }
        Ok(out)
    }

    async fn write(&self, b: &WriteBatch) -> Result<(), StoreError> {
        self.fence(b.queue, b.lane, b.epoch)?;
        for m in &b.inserts {
            if self.get(&self.id_key(b.queue, &m.msg_id))?.is_some() {
                return Err(failed("duplicate msg_id (uk_msg)"));
            }
        }
        let mut p = Payload::with_capacity(
            256 + b
                .inserts
                .iter()
                .map(|m| 96 + m.props.len() + m.body.len())
                .sum::<usize>(),
        );
        // A message inserted and deleted (or recounted) by the same batch:
        // the batch applies inserts first, as the other stores do, so the
        // delete removes it and the count lands on it. Such a message is
        // simply not written.
        let mut inserted: HashMap<(u16, u8, u64), StoredMsg> = b
            .inserts
            .iter()
            .map(|m| ((m.lane, m.pclass, m.seq), m.clone()))
            .collect();
        let mut dels = Vec::with_capacity(b.deletes.len());
        let mut deletes = Vec::with_capacity(b.deletes.len());
        for d in &b.deletes {
            let k = (d.lane, d.pclass, d.seq);
            if inserted.get(&k).is_some_and(|m| m.msg_id == d.msg_id) {
                inserted.remove(&k);
            } else {
                deletes.push(*d);
            }
        }
        let mut counts = Vec::with_capacity(b.counts.len());
        for (r, n) in &b.counts {
            match inserted.get_mut(&(r.lane, r.pclass, r.seq)) {
                Some(m) if m.msg_id == r.msg_id => m.delivery_count = *n,
                _ => counts.push((*r, *n)),
            }
        }
        for m in b
            .inserts
            .iter()
            .filter_map(|m| inserted.get(&(m.lane, m.pclass, m.seq)))
        {
            p.put(
                Cf::Data,
                &self.msg_key(b.queue, m.lane, m.pclass, m.seq),
                &enc_msg(m),
            );
            let mut loc = Vec::with_capacity(11);
            loc.extend_from_slice(&m.lane.to_be_bytes());
            loc.push(m.pclass);
            loc.extend_from_slice(&m.seq.to_be_bytes());
            p.put(Cf::Data, &self.id_key(b.queue, &m.msg_id), &loc);
        }
        // Deletes check the id, as the SQL store's `WHERE msg_id = ?`.
        for d in &deletes {
            let k = self.id_key(b.queue, &d.msg_id);
            let Some(loc) = self.get(&k)? else { continue };
            if loc.len() == 11
                && u16::from_be_bytes([loc[0], loc[1]]) == d.lane
                && loc[2] == d.pclass
                && loc[3..11] == d.seq.to_be_bytes()
            {
                p.delete(Cf::Data, &k);
                dels.push(*d);
            }
        }
        let lane = b.lane;
        let by_lane: HashMap<u16, Vec<MsgRef>> = dels.iter().fold(HashMap::new(), |mut m, d| {
            m.entry(d.lane).or_default().push(*d);
            m
        });
        for (l, refs) in &by_lane {
            delete_runs(&mut p, self, b.queue, *l, refs);
        }
        // A count on a message this batch deletes is nothing (the other
        // stores' `UPDATE ... WHERE msg_id` finds no row); re-putting it
        // after the delete would bring it back without its id key.
        let gone: std::collections::HashSet<MsgId> = b.deletes.iter().map(|d| d.msg_id).collect();
        for (r, n) in counts.iter().filter(|(r, _)| !gone.contains(&r.msg_id)) {
            let k = self.msg_key(b.queue, r.lane, r.pclass, r.seq);
            if let Some(v) = self.get(&k)? {
                let mut m = dec_msg(r.lane, r.pclass, r.seq, &v)?;
                if m.msg_id == r.msg_id {
                    m.delivery_count = *n;
                    p.put(Cf::Data, &k, &enc_msg(&m));
                }
            }
        }
        for o in &b.outbox {
            p.put(Cf::Data, &self.out_key(&o.out_id), &enc_out(o));
        }
        let at = crate::store::msg_id_ms(&crate::store::new_msg_id());
        for id in &b.forwarded {
            p.put(Cf::Data, &self.ledger_key(b.queue, id), &at.to_be_bytes());
        }
        p.put(
            Cf::Data,
            &self.lane_key(b.queue, lane),
            &enc_lane(b.epoch, &b.state),
        );
        // The fence again, atomically with the entry taking its place in
        // the log: a claim in between fences this write.
        self.fenced_commit(b.queue, b.lane, b.epoch, Some(b.state), p)
            .await
    }

    async fn purge(
        &self,
        queue: u64,
        lane: u16,
        epoch: u64,
        keep: &[u64],
    ) -> Result<u64, StoreError> {
        self.fence(queue, lane, epoch)?;
        let all = self.load(queue, lane, None, usize::MAX).await?;
        let drop: Vec<&StoredMsg> = all.iter().filter(|m| !keep.contains(&m.seq)).collect();
        if drop.is_empty() {
            return Ok(0);
        }
        let mut p = Payload::new();
        for m in &drop {
            p.delete(Cf::Data, &self.id_key(queue, &m.msg_id));
        }
        let refs: Vec<MsgRef> = drop
            .iter()
            .map(|m| MsgRef {
                lane,
                pclass: m.pclass,
                seq: m.seq,
                msg_id: m.msg_id,
            })
            .collect();
        delete_runs(&mut p, self, queue, lane, &refs);
        self.fenced_commit(queue, lane, epoch, None, p).await?;
        Ok(drop.len() as u64)
    }

    async fn drop_queue(&self, queue: u64) -> Result<(), StoreError> {
        let mut p = Payload::new();
        for f in [MSGS, IDS, LANES, LEDGER] {
            let qp = self.queue_p(f, queue);
            p.delete_range(Cf::Data, &qp, &prefix_end(&qp).unwrap_or_default());
        }
        lock(&self.lanes).retain(|(q, _), _| *q != queue);
        self.commit(p).await
    }

    async fn outbox(&self, limit: usize) -> Result<Vec<OutboxRow>, StoreError> {
        self.prefix_scan(&self.p(OUTBOX), limit)?
            .iter()
            .map(|(_, v)| dec_out(v))
            .collect()
    }

    /// Every row: no feed forwards an engine store's publishes, so none
    /// are held back for it.
    async fn outbox_due(
        &self,
        limit: usize,
        _publish_before: MsgId,
    ) -> Result<Vec<OutboxRow>, StoreError> {
        self.outbox(limit).await
    }

    async fn outbox_done(&self, ids: &[MsgId]) -> Result<(), StoreError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut p = Payload::new();
        for id in ids {
            p.delete(Cf::Data, &self.out_key(id));
        }
        self.commit(p).await
    }

    async fn outbox_rows(&self, ids: &[MsgId]) -> Result<Vec<OutboxRow>, StoreError> {
        let mut out = Vec::new();
        for id in ids {
            if let Some(v) = self.get(&self.out_key(id))? {
                out.push(dec_out(&v)?);
            }
        }
        Ok(out)
    }

    async fn stored(&self, queue: u64) -> Result<u64, StoreError> {
        let qp = self.queue_p(MSGS, queue);
        let end = prefix_end(&qp).unwrap_or_default();
        let mut n = 0u64;
        let mut from = qp.clone();
        loop {
            let page = self.scan(&from, &end, 4096)?;
            n += page.len() as u64;
            match page.last() {
                Some((k, _)) if page.len() == 4096 => {
                    from.clone_from(k);
                    from.push(0);
                }
                _ => break,
            }
        }
        Ok(n)
    }

    async fn purge_ledger(
        &self,
        before_ms: i64,
        changes_before_ms: i64,
    ) -> Result<u64, StoreError> {
        let lp = self.p(LEDGER);
        let mut p = Payload::new();
        let mut n = 0u64;
        for (k, v) in self.prefix_scan(&lp, usize::MAX)? {
            if n >= crate::store::LEDGER_PURGE_BATCH {
                break;
            }
            let id: MsgId = k[k.len() - 16..].try_into().map_err(failed)?;
            let at = i64::from_be_bytes(v.as_slice().try_into().map_err(failed)?);
            let before = if crate::store::is_change_id(&id) {
                changes_before_ms
            } else {
                before_ms
            };
            if at < before && self.get(&self.out_key(&id))?.is_none() {
                p.delete(Cf::Data, &k);
                n += 1;
            }
        }
        if n > 0 {
            self.commit(p).await?;
        }
        Ok(n)
    }

    async fn delay(&self, row: &DelayedRow) -> Result<(), StoreError> {
        let mut p = Payload::new();
        p.put(
            Cf::Data,
            &self.delayed_key(row.due_ms, &row.msg_id),
            &enc_delayed(row),
        );
        self.commit(p).await
    }

    async fn release_due(&self, now_ms: i64, limit: usize) -> Result<usize, StoreError> {
        let dp = self.p(DELAYED);
        let mut hi = dp.clone();
        memcmp::put_i64(&mut hi, now_ms.saturating_add(1));
        let due = self.scan(&dp, &hi, limit)?;
        if due.is_empty() {
            return Ok(0);
        }
        let mut p = Payload::new();
        for (k, v) in &due {
            let tail = &k[dp.len()..];
            let (ms, rest) = memcmp::get_int(tail, 8).map_err(failed)?;
            let id: MsgId = rest[..16].try_into().map_err(failed)?;
            let d = dec_delayed(ms, id, v)?;
            let out_id = crate::store::new_msg_id();
            p.delete(Cf::Data, k);
            p.put(
                Cf::Data,
                &self.out_key(&out_id),
                &enc_out(&OutboxRow {
                    out_id,
                    kind: OutKind::Delayed,
                    dest: OutDest::Exchange {
                        name: d.exchange.clone(),
                        key: d.routing_key.clone(),
                    },
                    msg_id: d.msg_id,
                    fmt: d.fmt,
                    exchange: d.exchange,
                    routing_key: d.routing_key,
                    props: d.props,
                    body: d.body,
                    deaths: None,
                }),
            );
        }
        self.commit(p).await?;
        Ok(due.len())
    }

    async fn next_due(&self) -> Result<Option<i64>, StoreError> {
        let dp = self.p(DELAYED);
        match self.prefix_scan(&dp, 1)?.first() {
            None => Ok(None),
            Some((k, _)) => Ok(Some(memcmp::get_int(&k[dp.len()..], 8).map_err(failed)?.0)),
        }
    }
}
