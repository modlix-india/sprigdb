//! The engine's transactions as a [`Unit`]: a batch is one transaction on
//! the node's unit, holding exclusive record locks on its keys, committed
//! with early lock release; its replies wait for the commit's durability
//! ticket, which also covers every pending version the batch read.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use sprig_common::SprigError;
use sprig_engine::Engine;
use sprig_engine::keys::KeyRange;
use sprig_engine::txn::{
    CommitPath, CommitTicket, Dir, Isolation, LockRequest, ReadMode, Txn, TxnOptions, Wait,
};

use crate::store::{Batch, StoreError, StoreResult, Ticket, Unit};

/// A commit's durability ticket.
#[derive(Debug, Clone)]
pub struct EngineTicket(CommitTicket);

impl Ticket for EngineTicket {
    async fn durable(&self) -> StoreResult<()> {
        self.0.wait_async().await.map_err(|e| conv(&e))
    }
}

fn conv(e: &SprigError) -> StoreError {
    StoreError {
        message: e.message.clone(),
        retryable: e.retryable,
    }
}

/// One engine unit serving Redis databases.
#[derive(Clone)]
pub struct EngineUnit {
    path: Arc<dyn CommitPath>,
    /// The engine, when `path` is one (for its counters).
    engine: Option<Engine>,
    opts: TxnOptions,
    /// A fixed clock for tests (`i64::MIN`: the system's).
    clock: Arc<AtomicI64>,
}

impl std::fmt::Debug for EngineUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineUnit")
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

impl EngineUnit {
    /// Redis batches on any commit path (the engine, or the transaction
    /// layer's in-memory test path).
    #[must_use]
    pub fn new(path: Arc<dyn CommitPath>) -> Self {
        // Every read a command makes under a key it holds is a current
        // read; READ COMMITTED keeps the read view (scans only) short.
        let opts = TxnOptions::mysql().with_isolation(Isolation::ReadCommitted);
        Self {
            path,
            engine: None,
            opts,
            clock: Arc::new(AtomicI64::new(i64::MIN)),
        }
    }

    /// Redis batches on the node's engine.
    #[must_use]
    pub fn on_engine(engine: Engine) -> Self {
        let mut u = Self::new(Arc::new(engine.clone()));
        u.engine = Some(engine);
        u
    }

    /// Fixes the clock at `ms` for tests (`None`: the system's again).
    pub fn set_clock(&self, ms: Option<i64>) {
        self.clock.store(ms.unwrap_or(i64::MIN), Ordering::Relaxed);
    }

    /// The engine, when the unit runs on one.
    #[must_use]
    pub fn engine(&self) -> Option<&Engine> {
        self.engine.as_ref()
    }
}

/// A batch: one engine transaction.
#[derive(Debug)]
pub struct EngineBatch {
    txn: Txn,
    /// The keys `begin` locked, sorted.
    held: Vec<Bytes>,
    /// Every read is a current read (a script's batch).
    latest: bool,
}

impl Unit for EngineUnit {
    type Batch = EngineBatch;

    async fn begin(&self, keys: Vec<Bytes>) -> StoreResult<EngineBatch> {
        let mut txn = Txn::begin_on(Arc::clone(&self.path), self.opts);
        for k in &keys {
            txn.lock(k, LockRequest::write())
                .await
                .map_err(|e| conv(&e))?;
        }
        Ok(EngineBatch {
            txn,
            held: keys,
            latest: false,
        })
    }

    fn leader_epoch(&self, key: &[u8]) -> u64 {
        self.path.leader_epoch_of(self.path.unit_of(key))
    }

    fn now_ms(&self) -> i64 {
        match self.clock.load(Ordering::Relaxed) {
            i64::MIN => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
            ms => ms,
        }
    }
}

impl EngineBatch {
    /// Locks `key` (an element, counter or expiry entry under a key the
    /// batch already holds, so nobody else can hold it: never waits).
    fn own(&mut self, key: &Bytes) -> StoreResult<()> {
        self.txn
            .lock_blocking(key, LockRequest::write().with_wait(Wait::NoWait))
            .map(|_| ())
            .map_err(|e| conv(&e))
    }
}

impl Batch for EngineBatch {
    type Ticket = EngineTicket;

    // Reads of keys the batch doesn't hold share one read view, opened at
    // its first such read (after `begin` took the batch's keys), so a
    // command never mixes a key's metadata from one moment with its
    // elements from another. A key the batch holds can't change under it,
    // so its latest version is the same as the view's, without opening
    // one (the hot-key path).
    fn get(&mut self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        if self.latest || self.held.binary_search_by(|k| k.as_ref().cmp(key)).is_ok() {
            return self.txn.read_current(key).map_err(|e| conv(&e));
        }
        self.txn.get_snapshot(key).map_err(|e| conv(&e))
    }

    fn scan_latest(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let r = KeyRange::new(start, end);
        self.txn
            .scan_current(&r, Dir::Forward, limit)
            .map_err(|e| conv(&e))
    }

    fn scan(&mut self, start: &[u8], end: &[u8], limit: usize) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let r = KeyRange::new(start, end);
        if self.latest {
            return self
                .txn
                .scan_current(&r, Dir::Forward, limit)
                .map_err(|e| conv(&e));
        }
        self.txn
            .scan(&r, Dir::Forward, limit, ReadMode::Snapshot)
            .map(|rows| rows.into_iter().map(|(k, v)| (Bytes::from(k), v)).collect())
            .map_err(|e| conv(&e))
    }

    fn scan_rev(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let r = KeyRange::new(start, end);
        if self.latest {
            return self
                .txn
                .scan_current(&r, Dir::Reverse, limit)
                .map_err(|e| conv(&e));
        }
        self.txn
            .scan(&r, Dir::Reverse, limit, ReadMode::Snapshot)
            .map(|rows| rows.into_iter().map(|(k, v)| (Bytes::from(k), v)).collect())
            .map_err(|e| conv(&e))
    }

    async fn hold(&mut self, keys: Vec<Bytes>) -> StoreResult<()> {
        for k in &keys {
            self.txn
                .lock(k, LockRequest::write())
                .await
                .map_err(|e| conv(&e))?;
        }
        self.held.extend(keys);
        self.held.sort_unstable();
        Ok(())
    }

    fn read_latest(&mut self) {
        self.latest = true;
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> StoreResult<()> {
        self.own(&key)?;
        self.txn.put(key, value).map_err(|e| conv(&e))
    }

    fn delete(&mut self, key: Bytes) -> StoreResult<()> {
        self.own(&key)?;
        self.txn.delete(key).map_err(|e| conv(&e))
    }

    fn note(&mut self, tenant: u64, note: Vec<u8>) -> StoreResult<()> {
        self.txn
            .add_note_for_tenant(tenant, note)
            .map_err(|e| conv(&e))
    }

    fn commit(self) -> StoreResult<EngineTicket> {
        self.txn.commit().map(EngineTicket).map_err(|e| conv(&e))
    }
}
