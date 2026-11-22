//! Tenants' catalog records in the node's storage engine
//! ([`sprig_catalog::records`]), and the peer service through which the
//! controller and workers reach them (`sprig.internal.v1.CatalogPeer`).
//!
//! ```text
//! key = tenant u64 | shard 0 | object 0 | 'M' | index 0 | relative key
//! ```
//!
//! Object 0 is never a table, a collection, a graph or a Redis database,
//! so catalog records share no key with data. Every key a caller names
//! (here or over the peer service) is put under the tenant's catalog
//! prefix: a remote caller can't reach table data.
//!
//! - Plain reads are snapshot reads at the transaction's read view
//!   (REPEATABLE READ: taken at the first plain read).
//! - A read for update takes the record's exclusive lock (also on a record
//!   that doesn't exist yet, so two first writers queue instead of
//!   deadlocking on the gap) and reads its latest committed value; the lock
//!   wait is at most `records::LOCK_WAIT` (1205 and a deadlock, 1213, are
//!   39071).
//! - Writes lock first; the commit waits until it is durable.

use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use sprig_api::proto::internal::catalog_op::Op;
use sprig_api::proto::internal::catalog_peer_server::{CatalogPeer, CatalogPeerServer};
use sprig_api::proto::internal::{CatalogEntry, CatalogOp, CatalogReply};
use sprig_catalog::records::{LOCK_WAIT, RecordStore, RecordTx, busy, unavailable};
use sprig_common::ids::TenantId;
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_engine::keys::{KeyPrefix, KeyRange, kind};
use sprig_engine::txn::{Dir, InsertLock, LockMode, LockRequest, ReadMode, Txn, TxnOptions, Wait};
use tokio::sync::mpsc;

/// The exclusive lock of a catalog record, waiting.
const EXCLUSIVE: LockRequest = LockRequest {
    mode: LockMode::Exclusive,
    wait: Wait::Block,
    check_snapshot: false,
};

/// The prefix of `tenant`'s catalog records.
fn catalog_prefix(tenant: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    KeyPrefix {
        tenant,
        shard: 0,
        object: 0,
        kind: kind::META,
        index: 0,
    }
    .encode_into(&mut k);
    k
}

/// A lock timeout or deadlock is the catalog's "busy, retry".
fn engine_err(e: SprigError) -> SprigError {
    match e.kind {
        ErrorKind::Standard { errno, .. } | ErrorKind::Backend { errno, .. }
            if matches!(errno, 1205 | 1213 | 3572) =>
        {
            busy()
        }
        _ => e,
    }
}

/// Catalog records in this process's engine.
#[derive(Debug, Clone, Default)]
pub struct EngineRecords {
    /// The engine; `None`: the node's ([`crate::node_engine`]).
    eng: Option<sprig_engine::Engine>,
}

impl EngineRecords {
    /// Records in `eng` instead of the node's engine (tests).
    #[must_use]
    pub fn on(eng: sprig_engine::Engine) -> Self {
        Self { eng: Some(eng) }
    }

    /// A transaction on `tenant`'s records (also used by the peer service).
    ///
    /// # Errors
    /// 39070 when the engine isn't available.
    pub(crate) fn begin_tx(&self, tenant: TenantId) -> Result<EngineTx> {
        let eng = match &self.eng {
            Some(e) => e.clone(),
            None => crate::node_engine::engine().map_err(|e| unavailable(e.message))?,
        };
        let opts = TxnOptions {
            lock_wait_timeout: Some(LOCK_WAIT),
            ..TxnOptions::default()
        };
        let t = eng.begin(opts).map_err(engine_err)?;
        Ok(EngineTx {
            t: Some(t),
            prefix: catalog_prefix(tenant.0),
        })
    }
}

#[async_trait::async_trait]
impl RecordStore for EngineRecords {
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        Ok(Box::new(self.begin_tx(tenant)?))
    }
}

/// One engine transaction on one tenant's catalog records.
pub(crate) struct EngineTx {
    t: Option<Txn>,
    prefix: Vec<u8>,
}

impl EngineTx {
    fn key(&self, rel: &[u8]) -> Bytes {
        let mut k = Vec::with_capacity(self.prefix.len() + rel.len());
        k.extend_from_slice(&self.prefix);
        k.extend_from_slice(rel);
        Bytes::from(k)
    }

    fn t(&mut self) -> Result<&mut Txn> {
        self.t
            .as_mut()
            .ok_or_else(|| SprigError::internal("catalog transaction already ended"))
    }
}

#[async_trait::async_trait]
impl RecordTx for EngineTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let k = self.key(key);
        Ok(self
            .t()?
            .get_snapshot(&k)
            .map_err(engine_err)?
            .map(|v| v.to_vec()))
    }

    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let k = self.key(key);
        let t = self.t()?;
        // The record lock first, held even when the record is missing.
        t.lock(&k, EXCLUSIVE).await.map_err(engine_err)?;
        match t.lock_get(&k, EXCLUSIVE).await.map_err(engine_err)? {
            sprig_engine::txn::Locked::Row(v) => Ok(v.map(|v| v.to_vec())),
            sprig_engine::txn::Locked::Skipped => Err(busy()),
        }
    }

    async fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let rel_lo = lo.to_vec();
        let (lo, hi) = (self.key(lo), self.key(hi));
        let n = self.prefix.len();
        let r = KeyRange::new(lo.to_vec(), hi.to_vec());
        // Only keys in the range count, so paging callers (`lo` = last key
        // + 0x00) never see a row twice. The engine once returned the record
        // just before a start falling between two records; that is fixed,
        // and this stays as a guard. One row more is asked for, so a row
        // dropped here never makes a full page look like the last one.
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = self
            .t()?
            .scan(
                &r,
                Dir::Forward,
                limit.saturating_add(1),
                ReadMode::Snapshot,
            )
            .map_err(engine_err)?
            .into_iter()
            .map(|(k, v)| (k[n..].to_vec(), v.to_vec()))
            .filter(|(k, _)| *k >= rel_lo)
            .collect();
        rows.truncate(limit);
        Ok(rows)
    }

    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = self.key(key);
        let t = self.t()?;
        t.lock_insert(&k, InsertLock::replace())
            .await
            .map_err(engine_err)?;
        t.put(k, Bytes::copy_from_slice(value)).map_err(engine_err)
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let k = self.key(key);
        let t = self.t()?;
        t.lock(&k, EXCLUSIVE).await.map_err(engine_err)?;
        t.delete(k).map_err(engine_err)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let t = self
            .t
            .take()
            .ok_or_else(|| SprigError::internal("catalog transaction already ended"))?;
        let ticket = t.commit().map_err(|e| unavailable(e.message))?;
        ticket
            .wait_async()
            .await
            .map_err(|e| unavailable(e.message))
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        if let Some(t) = self.t.take() {
            t.rollback();
        }
        Ok(())
    }
}

impl Drop for EngineTx {
    fn drop(&mut self) {
        if let Some(t) = self.t.take() {
            t.rollback();
        }
    }
}

// ---- the peer service ----

/// `sprig.internal.v1.CatalogPeer` on this gateway's engine.
#[derive(Debug, Clone, Default)]
pub struct CatalogService {
    records: EngineRecords,
}

impl CatalogService {
    /// The tonic service on the node's engine.
    #[must_use]
    pub fn server() -> CatalogPeerServer<Self> {
        CatalogPeerServer::new(Self::default())
    }

    /// The service on `records` (tests).
    #[must_use]
    pub fn on(records: EngineRecords) -> Self {
        Self { records }
    }
}

type Replies = Pin<Box<dyn Stream<Item = std::result::Result<CatalogReply, tonic::Status>> + Send>>;

/// Longest a remote transaction may sit idle between operations before
/// it is rolled back (its caller is gone or stuck).
const IDLE: Duration = Duration::from_secs(60);

fn error_reply(e: &SprigError) -> CatalogReply {
    CatalogReply {
        error_code: u32::from(e.code().map_or(39_999, sprig_common::SprigCode::number)),
        error_message: e.message.clone(),
        ..CatalogReply::default()
    }
}

async fn next_reply(
    mut r: mpsc::Receiver<CatalogReply>,
) -> Option<(
    std::result::Result<CatalogReply, tonic::Status>,
    mpsc::Receiver<CatalogReply>,
)> {
    r.recv().await.map(|m| (Ok(m), r))
}

/// Runs one remote transaction: the stream's operations, in order.
async fn run(
    records: EngineRecords,
    mut ops: tonic::Streaming<CatalogOp>,
    out: mpsc::Sender<CatalogReply>,
) {
    let mut tx: Option<EngineTx> = None;
    loop {
        let op = match tokio::time::timeout(IDLE, ops.message()).await {
            Ok(Ok(Some(m))) => m.op,
            // Ended, failed or idle: whatever is open rolls back (drop).
            _ => return,
        };
        let reply = match (op, tx.as_mut()) {
            (Some(Op::BeginTenant(t)), None) => match records.begin_tx(TenantId(t)) {
                Ok(t) => {
                    tx = Some(t);
                    Ok(CatalogReply::default())
                }
                Err(e) => Err(e),
            },
            (Some(Op::Get(k)), Some(t)) => t.get(&k).await.map(found),
            (Some(Op::GetForUpdate(k)), Some(t)) => t.get_for_update(&k).await.map(found),
            (Some(Op::Scan(s)), Some(t)) => {
                let limit = usize::try_from(s.limit).unwrap_or(usize::MAX).min(65_536);
                t.scan(&s.lo, &s.hi, limit).await.map(|rows| CatalogReply {
                    entries: rows
                        .into_iter()
                        .map(|(key, value)| CatalogEntry { key, value })
                        .collect(),
                    ..CatalogReply::default()
                })
            }
            (Some(Op::Put(e)), Some(t)) => t
                .put(&e.key, &e.value)
                .await
                .map(|()| CatalogReply::default()),
            (Some(Op::Delete(k)), Some(t)) => t.delete(&k).await.map(|()| CatalogReply::default()),
            (Some(Op::Commit(_)), Some(_)) => {
                let r = match tx.take() {
                    Some(t) => Box::new(t).commit().await,
                    None => Ok(()),
                };
                let _ = out
                    .send(match r {
                        Ok(()) => CatalogReply::default(),
                        Err(e) => error_reply(&e),
                    })
                    .await;
                return;
            }
            (Some(Op::Rollback(_)), _) => {
                drop(tx.take());
                let _ = out.send(CatalogReply::default()).await;
                return;
            }
            (Some(Op::BeginTenant(_)), Some(_)) => Err(SprigError::internal(
                "a catalog transaction is already open on this stream",
            )),
            (_, None) | (None, _) => Err(SprigError::internal(
                "a catalog stream must begin with a tenant",
            )),
        };
        let m = match reply {
            Ok(m) => m,
            Err(e) => error_reply(&e),
        };
        if out.send(m).await.is_err() {
            return;
        }
    }
}

fn found(v: Option<Vec<u8>>) -> CatalogReply {
    match v {
        Some(value) => CatalogReply {
            found: true,
            value,
            ..CatalogReply::default()
        },
        None => CatalogReply::default(),
    }
}

#[tonic::async_trait]
impl CatalogPeer for CatalogService {
    type TxnStream = Replies;

    async fn txn(
        &self,
        req: tonic::Request<tonic::Streaming<CatalogOp>>,
    ) -> std::result::Result<tonic::Response<Self::TxnStream>, tonic::Status> {
        let (send, recv) = mpsc::channel(4);
        tokio::spawn(run(self.records.clone(), req.into_inner(), send));
        Ok(tonic::Response::new(Box::pin(futures::stream::unfold(
            recv, next_reply,
        ))))
    }
}

#[cfg(test)]
mod modules_kill;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn catalog_keys_are_under_object_zero() {
        let p = catalog_prefix(7);
        assert_eq!(&p[..8], &7u64.to_be_bytes());
        // shard 0, object 0, then the kind.
        assert_eq!(&p[8..16], &[0u8; 8]);
        assert_eq!(p[16], kind::META);
    }
}
