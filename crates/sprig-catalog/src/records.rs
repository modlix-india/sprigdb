//! Catalog records: a tenant's catalog rows as key/value records in the
//! storage engine, and versioned changes on them.
//!
//! ```text
//! key   = tenant u64 | shard 0 | object 0 | 'M' | index 0 | relative key
//! relative keys:
//!   v/<kind>/                    committed version (u64 BE) and commit time (µs, u64 BE)
//!   ctr/<name>/                  an id counter: the last id handed out (u64 BE)
//!   m/<module>/<table>/<pk>      a module's row (JSON), pk order-preserving
//! ```
//!
//! The engine node (the gateway process that opens the node's engine)
//! holds every tenant's records; other processes reach them through its
//! peer service ([`remote`]). This module knows nothing of the engine:
//!
//! - [`RecordStore`] begins a transaction on one tenant's records and
//!   [`RecordTx`] reads and writes them. Plain reads ([`RecordTx::get`],
//!   [`RecordTx::scan`]) are consistent reads at the transaction's read
//!   view with its own writes merged in; [`RecordTx::get_for_update`]
//!   takes the record's exclusive lock (waiting at most
//!   [`LOCK_WAIT`], else 39071) and returns its latest committed value;
//!   writes lock first; [`RecordTx::commit`] waits until the commit is
//!   durable; dropping a transaction rolls it back.
//! - Implementations: [`mem::MemRecords`] (tests), the engine (the
//!   gateway's `tenant::catalog_records`), and [`remote::RemoteRecords`].
//! - [`RowTable`] stores typed rows as JSON under a table's prefix;
//!   [`next_ids`] hands out ids from a counter.
//! - [`RecBackend`] is a [`VersionBackend`]: a change locks `v/<kind>`,
//!   writes its rows in the same transaction and commits them with the new
//!   version, so a change and its version bump are one engine transaction.
//!   [`AuthCatalog`] is the auth tables' [`Versioner`].

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use serde::Serialize;
use serde::de::DeserializeOwned;
use sprig_common::ids::TenantId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{Result, SprigCode, SprigError};

use crate::version::{VersionBackend, VersionKind, Versioner};

pub mod mem;
pub mod remote;
#[cfg(test)]
mod tests;

/// Longest wait for another transaction's lock on a record (as the
/// MariaDB catalog's version-row lock wait).
pub const LOCK_WAIT: Duration = Duration::from_secs(10);

/// Entries read per scan call when a whole range is walked.
const SCAN_BATCH: usize = 1024;

/// A transaction on one tenant's catalog records.
#[async_trait::async_trait]
pub trait RecordTx: Send {
    /// The record `key` at the read view (this transaction's own write
    /// included).
    ///
    /// # Errors
    /// The store's (39070 when it can't be reached).
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// The record `key`, locked exclusively until the transaction ends, as
    /// it is now (the latest committed value, or this transaction's own).
    ///
    /// # Errors
    /// 39071 when another transaction holds the lock past [`LOCK_WAIT`]
    /// (or a deadlock); the store's.
    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Up to `limit` records of `[lo, hi)` in key order at the read view,
    /// this transaction's own writes merged in.
    ///
    /// # Errors
    /// The store's.
    async fn scan(&mut self, lo: &[u8], hi: &[u8], limit: usize)
    -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Writes `key` (locking it first).
    ///
    /// # Errors
    /// As for [`RecordTx::get_for_update`].
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Deletes `key` (locking it first).
    ///
    /// # Errors
    /// As for [`RecordTx::get_for_update`].
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Commits and waits until the commit is durable.
    ///
    /// # Errors
    /// 39070 when the commit failed or its outcome is unknown.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rolls back (dropping the transaction does the same).
    ///
    /// # Errors
    /// The store's; the transaction ends anyway.
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Where tenants' catalog records live.
#[async_trait::async_trait]
pub trait RecordStore: Send + Sync + 'static {
    /// Begins a transaction on `tenant`'s records.
    ///
    /// # Errors
    /// 39070 when the store can't be reached.
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>>;
}

// ---- keys ----

/// The relative key of `kind`'s committed version. Ended by `/`, so no
/// key is the start of another (`v/a` against `v/ab`).
#[must_use]
pub fn version_key(kind: VersionKind) -> Vec<u8> {
    format!("v/{}/", kind.label()).into_bytes()
}

/// The relative key of counter `name`, ended by `/` like
/// [`version_key`] (`ctr/id` against `ctr/ids`).
#[must_use]
pub fn counter_key(name: &str) -> Vec<u8> {
    format!("ctr/{name}/").into_bytes()
}

/// The key a version or counter had before keys were ended by `/`: read
/// when the current key is missing, so records written by an older build
/// keep their versions and ids (never handed out again). Only the current
/// key is ever written.
fn legacy_key(key: &[u8]) -> &[u8] {
    key.strip_suffix(b"/").unwrap_or(key)
}

/// `key` locked for update; when it is missing, its value under the
/// legacy key (read, not locked: every writer locks `key` first).
async fn get_current(tx: &mut dyn RecordTx, key: &[u8], lock: bool) -> Result<Option<Vec<u8>>> {
    let found = if lock {
        tx.get_for_update(key).await?
    } else {
        tx.get(key).await?
    };
    match found {
        Some(v) => Ok(Some(v)),
        None => tx.get(legacy_key(key)).await,
    }
}

/// The first key after every key starting with `p` (`p` is never all
/// `0xFF`: relative keys are ASCII-led).
#[must_use]
pub fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut end = p.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    vec![0xFF; p.len() + 1]
}

/// Builds an order-preserving primary key from parts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Pk(pub Vec<u8>);

impl Pk {
    /// An empty key.
    #[must_use]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Appends an unsigned 32-bit part (big-endian: sorts numerically).
    #[must_use]
    pub fn u32(mut self, v: u32) -> Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Appends an unsigned 64-bit part.
    #[must_use]
    pub fn u64(mut self, v: u64) -> Self {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }

    /// Appends one byte.
    #[must_use]
    pub fn u8(mut self, v: u8) -> Self {
        self.0.push(v);
        self
    }

    /// Appends a byte string, escaped so that it is prefix-free (`00` as
    /// `00 FF`, ended by `00 00`) and parts after it sort correctly.
    #[must_use]
    pub fn bytes(mut self, v: &[u8]) -> Self {
        for b in v {
            self.0.push(*b);
            if *b == 0 {
                self.0.push(0xFF);
            }
        }
        self.0.extend_from_slice(&[0, 0]);
        self
    }

    /// Appends a fixed-width byte string as it is (the last part, or one
    /// whose width never varies).
    #[must_use]
    pub fn raw(mut self, v: &[u8]) -> Self {
        self.0.extend_from_slice(v);
        self
    }
}

/// Reads a big-endian u64 at the start of `b`.
fn be_u64(b: &[u8]) -> Option<u64> {
    b.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

fn bad_record(what: &str) -> SprigError {
    SprigError::internal(format!("catalog record {what} doesn't decode"))
}

// ---- typed rows ----

/// A table of typed rows stored as JSON under `m/<module>/<table>/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowTable {
    /// The module (`auth`, ...).
    pub module: &'static str,
    /// The table.
    pub table: &'static str,
}

impl RowTable {
    /// The table `table` of `module`.
    #[must_use]
    pub const fn new(module: &'static str, table: &'static str) -> Self {
        Self { module, table }
    }

    /// The prefix of every row of the table.
    #[must_use]
    pub fn prefix(&self) -> Vec<u8> {
        format!("m/{}/{}/", self.module, self.table).into_bytes()
    }

    /// The key of the row with primary key `pk`.
    #[must_use]
    pub fn key(&self, pk: &Pk) -> Vec<u8> {
        let mut k = self.prefix();
        k.extend_from_slice(&pk.0);
        k
    }

    fn decode<T: DeserializeOwned>(&self, raw: &[u8]) -> Result<T> {
        serde_json::from_slice(raw).map_err(|e| {
            SprigError::internal(format!(
                "catalog row of {}.{} doesn't decode: {e}",
                self.module, self.table
            ))
        })
    }

    /// The row `pk` at the read view.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn get<T: DeserializeOwned>(
        &self,
        tx: &mut dyn RecordTx,
        pk: &Pk,
    ) -> Result<Option<T>> {
        match tx.get(&self.key(pk)).await? {
            Some(raw) => self.decode(&raw).map(Some),
            None => Ok(None),
        }
    }

    /// The row `pk`, locked until the transaction ends, as it is now.
    ///
    /// # Errors
    /// As for [`RecordTx::get_for_update`]; `Internal` for a row that
    /// doesn't decode.
    pub async fn get_for_update<T: DeserializeOwned>(
        &self,
        tx: &mut dyn RecordTx,
        pk: &Pk,
    ) -> Result<Option<T>> {
        match tx.get_for_update(&self.key(pk)).await? {
            Some(raw) => self.decode(&raw).map(Some),
            None => Ok(None),
        }
    }

    /// Writes row `pk`.
    ///
    /// # Errors
    /// As for [`RecordTx::put`].
    pub async fn put<T: Serialize + ?Sized>(
        &self,
        tx: &mut dyn RecordTx,
        pk: &Pk,
        row: &T,
    ) -> Result<()> {
        let raw = serde_json::to_vec(row).map_err(|e| {
            SprigError::internal(format!(
                "catalog row of {}.{} doesn't encode: {e}",
                self.module, self.table
            ))
        })?;
        tx.put(&self.key(pk), &raw).await
    }

    /// Deletes row `pk`.
    ///
    /// # Errors
    /// As for [`RecordTx::delete`].
    pub async fn delete(&self, tx: &mut dyn RecordTx, pk: &Pk) -> Result<()> {
        tx.delete(&self.key(pk)).await
    }

    /// Every row whose primary key starts with `under` (every row for an
    /// empty one), in key order, with its primary key.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn scan<T: DeserializeOwned>(
        &self,
        tx: &mut dyn RecordTx,
        under: &Pk,
    ) -> Result<Vec<(Pk, T)>> {
        let base = self.prefix();
        let lo = self.key(under);
        let hi = prefix_end(&lo);
        let mut out = Vec::new();
        for (k, v) in scan_all(tx, &lo, &hi).await? {
            out.push((Pk(k[base.len()..].to_vec()), self.decode(&v)?));
        }
        Ok(out)
    }

    /// Every row's value of the table (or under `under`), in key order.
    ///
    /// # Errors
    /// As for [`RowTable::scan`].
    pub async fn rows<T: DeserializeOwned>(
        &self,
        tx: &mut dyn RecordTx,
        under: &Pk,
    ) -> Result<Vec<T>> {
        Ok(self
            .scan(tx, under)
            .await?
            .into_iter()
            .map(|(_, r)| r)
            .collect())
    }
}

/// Every record of `[lo, hi)`, in key order.
///
/// # Errors
/// The store's.
pub async fn scan_all(
    tx: &mut dyn RecordTx,
    lo: &[u8],
    hi: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut from = lo.to_vec();
    loop {
        let mut batch = tx.scan(&from, hi, SCAN_BATCH).await?;
        let full = batch.len() == SCAN_BATCH;
        // Never a row before the page's start (a repeat at the boundary).
        batch.retain(|(k, _)| k.as_slice() >= from.as_slice());
        if let Some((k, _)) = batch.last() {
            let mut next = k.clone();
            next.push(0);
            from = next;
        }
        out.extend(batch);
        if !full {
            return Ok(out);
        }
    }
}

/// Hands out `n` ids from counter `name` (locked until the transaction
/// ends): the first of them; the next caller gets ids after the last.
/// Ids start at 1 and are never reused.
///
/// # Errors
/// As for [`RecordTx::get_for_update`]; `Internal` if the counter would
/// overflow.
pub async fn next_ids(tx: &mut dyn RecordTx, name: &str, n: u64) -> Result<u64> {
    let key = counter_key(name);
    let last = match get_current(tx, &key, true).await? {
        Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("counter"))?,
        None => 0,
    };
    let first = last
        .checked_add(1)
        .ok_or_else(|| SprigError::internal(format!("catalog counter {name} exhausted")))?;
    let new_last = last
        .checked_add(n.max(1))
        .ok_or_else(|| SprigError::internal(format!("catalog counter {name} exhausted")))?;
    tx.put(&key, &new_last.to_be_bytes()).await?;
    Ok(first)
}

/// Moves counter `name` to at least `at` (ids at or below it are taken),
/// in the transaction.
///
/// # Errors
/// As for [`next_ids`].
pub async fn counter_at_least(tx: &mut dyn RecordTx, name: &str, at: u64) -> Result<()> {
    let key = counter_key(name);
    let last = match get_current(tx, &key, true).await? {
        Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("counter"))?,
        None => 0,
    };
    if at > last {
        tx.put(&key, &at.to_be_bytes()).await?;
    }
    Ok(())
}

/// The committed version of `kind` at the read view (0 when it never
/// changed).
///
/// # Errors
/// The store's; `Internal` for a version record that doesn't decode.
pub async fn read_version(tx: &mut dyn RecordTx, kind: VersionKind) -> Result<u64> {
    match get_current(tx, &version_key(kind), false).await? {
        Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("version")),
        None => Ok(0),
    }
}

// ---- versioned changes ----

/// The catalog writer on records ([`VersionBackend`]).
#[derive(Clone)]
pub struct RecBackend {
    store: Arc<dyn RecordStore>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for RecBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecBackend").finish_non_exhaustive()
    }
}

impl RecBackend {
    /// Changes the records of `store`.
    #[must_use]
    pub fn new(store: Arc<dyn RecordStore>) -> Self {
        Self {
            store,
            clock: Arc::new(SystemClock),
        }
    }

    /// The same backend with commit times from `clock`.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The record store (for unversioned transactions).
    #[must_use]
    pub fn store(&self) -> &Arc<dyn RecordStore> {
        &self.store
    }

    /// The wall clock.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
}

/// An open record transaction holding a version record.
pub struct RecTx {
    tx: Box<dyn RecordTx>,
    tenant: TenantId,
}

impl std::fmt::Debug for RecTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecTx")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl RecTx {
    /// The records, to read and write in the change.
    pub fn records(&mut self) -> &mut dyn RecordTx {
        &mut *self.tx
    }

    /// The tenant.
    #[must_use]
    pub fn tenant(&self) -> TenantId {
        self.tenant
    }

    /// The record transaction itself (to end it).
    pub(crate) fn into_records(self) -> Box<dyn RecordTx> {
        self.tx
    }
}

#[async_trait::async_trait]
impl VersionBackend for RecBackend {
    type Tx = RecTx;

    async fn begin_locked(&self, tenant: TenantId, kind: VersionKind) -> Result<(RecTx, u64)> {
        let mut tx = self.store.begin(tenant).await?;
        let v = match get_current(&mut *tx, &version_key(kind), true).await? {
            Some(raw) => be_u64(&raw).ok_or_else(|| bad_record("version"))?,
            None => 0,
        };
        Ok((RecTx { tx, tenant }, v))
    }

    async fn commit(&self, mut tx: RecTx, kind: VersionKind, version: u64) -> Result<()> {
        let micros = self
            .clock
            .wall()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX));
        let mut raw = Vec::with_capacity(16);
        raw.extend_from_slice(&version.to_be_bytes());
        raw.extend_from_slice(&micros.to_be_bytes());
        tx.tx.put(&version_key(kind), &raw).await?;
        tx.tx.commit().await
    }

    async fn rollback(&self, tx: RecTx) -> Result<()> {
        tx.tx.rollback().await
    }
}

/// The auth tables' catalog: versioned changes on engine records.
pub type AuthCatalog = Versioner<RecBackend>;

/// An open auth change.
pub type AuthTx = crate::version::Change<RecBackend>;

/// 39071: a record lock wait timed out or deadlocked.
#[must_use]
pub fn busy() -> SprigError {
    SprigError::new(SprigCode::CatalogConflict, "catalog record busy; retry")
}

/// 39070: the record store can't be reached.
#[must_use]
pub fn unavailable(why: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("catalog unavailable: {why}"),
    )
}
