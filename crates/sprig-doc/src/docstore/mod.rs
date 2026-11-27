//! MongoDB collections stored directly on SprigDB's storage engine.
//!
//! A document is stored as the BSON the client sent, under its
//! order-preserving `_id` key ([`crate::keyenc`]); secondary indexes are
//! engine entries maintained in the same transaction as the document
//! (one entry per array element for multikey fields). Reads plan an index
//! range from the filter, then recheck every candidate with the same
//! matcher the SQL path uses ([`crate::filter`]); updates run the same
//! update operators ([`crate::update`]) under the document's lock; the
//! aggregation stages run over the documents read ([`crate::agg`]).
//!
//! - [`layout`]: keys;
//! - [`catalog`]: collection records and index specifications;
//! - [`keys`]: a document's index entries;
//! - [`read`]: plans, scans and resumable cursors;
//! - [`write`]: inserts, updates, deletes, `findAndModify`;
//! - [`pipeline`]: aggregation over the store;
//! - [`cmd`]: the MongoDB commands, answered as reply documents;
//! - [`ttl`]: the TTL sweeper;
//! - [`guard`]: row-level security, applied by the store.

// One shape per match arm reads better here than `if let ... else`.
#![allow(clippy::single_match_else)]

pub mod build;
pub mod bulk;
pub mod catalog;
pub mod cmd;
pub mod guard;
pub mod keys;
pub mod layout;
pub mod pipeline;
pub mod read;
pub mod stats;
pub mod ttl;
pub mod write;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bson::{Document, doc};
use bytes::Bytes;
use sprig_common::{SprigCode, SprigError};
use sprig_engine::keys::KeyRange;
use sprig_engine::locks::Wait;
use sprig_engine::txn::{CommitPath, Dir, Isolation, LockRequest, Txn, TxnOptions};
use sprig_proto_mongo::errors::{MongoError, code};

use self::catalog::CollMeta;
use self::layout::Space;

/// A collection or view as its record names it ([`DocStore::namespaces`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    /// The database.
    pub db: String,
    /// The name.
    pub name: String,
    /// The engine object of its documents (`layout::CATALOG + n`).
    pub object: u32,
    /// A view.
    pub view: bool,
}

/// A collection as the store uses it: its record, its keyspace, and the
/// multikey facts of its indexes.
#[derive(Debug)]
pub struct Coll {
    /// The record.
    pub meta: CollMeta,
    /// The keyspace.
    pub space: Space,
    /// Per index (by position in `meta.indexes`): it has seen an array.
    multikey: Vec<AtomicBool>,
}

impl Coll {
    fn new(meta: CollMeta, tenant: u64, multikey: &[bool]) -> Self {
        let space = Space::new(tenant, meta.n);
        let multikey = meta
            .indexes
            .iter()
            .enumerate()
            .map(|(i, _)| AtomicBool::new(multikey.get(i).copied().unwrap_or(false)))
            .collect();
        Self {
            meta,
            space,
            multikey,
        }
    }

    /// Whether index `pos` (position in `meta.indexes`) may hold several
    /// entries for one document.
    #[must_use]
    pub fn is_multikey(&self, pos: usize) -> bool {
        self.multikey
            .get(pos)
            .is_some_and(|b| b.load(Ordering::Acquire))
    }

    /// `db.coll`.
    #[must_use]
    pub fn ns(&self) -> String {
        format!("{}.{}", self.meta.db, self.meta.name)
    }
}

/// Maps an engine error to MongoDB's: lock conflicts and deadlocks are
/// write conflicts.
#[must_use]
pub fn engine_error(e: &SprigError, multi: bool) -> MongoError {
    let errno = match e.kind {
        sprig_common::ErrorKind::Standard { errno, .. }
        | sprig_common::ErrorKind::Backend { errno, .. } => errno,
        sprig_common::ErrorKind::Sprig(_) => 0,
    };
    if matches!(errno, 1205 | 1213 | 1020 | 3572) {
        let m = MongoError::new(
            code::WRITE_CONFLICT,
            "Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.",
        );
        return if multi {
            m.label("TransientTransactionError")
        } else {
            m
        };
    }
    // The unit's leadership moved (or is moving): the driver retries on
    // the primary it finds next, as after a MongoDB step-down.
    if matches!(
        e.code(),
        Some(SprigCode::ShardMoved | SprigCode::NotLeader | SprigCode::SessionMoved)
    ) {
        let m = MongoError::new(
            code::NOT_WRITABLE_PRIMARY,
            format!("not primary: {}", e.message),
        )
        .label("RetryableWriteError");
        return if multi {
            m.label("TransientTransactionError")
        } else {
            m
        };
    }
    MongoError::new(
        code::INTERNAL_ERROR,
        format!("storage engine: {}", e.message),
    )
}

/// One engine transaction serving document commands.
#[derive(Debug)]
pub struct DocTxn {
    txn: Txn,
    /// A multi-document transaction: snapshot reads, and a lock another
    /// transaction holds is a write conflict at once.
    pub multi: bool,
    /// An SQL statement's transaction lent to the store ([`DocTxn::adopt`]):
    /// documents are locked exclusively, waiting as the session waits, and
    /// the first engine error is kept as it was ([`sql_errors`]).
    sql: bool,
    /// SQL mode: a document's lock makes the snapshot check (a session's
    /// transaction on MariaDB's port).
    sql_snapshot_check: bool,
}

/// The first engine error of each transaction an SQL statement lent to the
/// store, by transaction id: kept out of [`DocTxn`] itself, whose size every
/// command's future carries, as such errors are rare.
fn sql_errors() -> &'static Mutex<HashMap<u64, SprigError>> {
    static ERRS: std::sync::OnceLock<Mutex<HashMap<u64, SprigError>>> = std::sync::OnceLock::new();
    ERRS.get_or_init(Mutex::default)
}

impl DocTxn {
    fn begin(path: &Arc<dyn CommitPath>, multi: bool) -> Self {
        let opts = if multi {
            // One snapshot for the transaction; a document changed after
            // it was taken can't be written (MariaDB's snapshot check).
            TxnOptions::mariadb().with_isolation(Isolation::RepeatableRead)
        } else {
            TxnOptions::mysql().with_isolation(Isolation::ReadCommitted)
        };
        Self {
            txn: Txn::begin_on(Arc::clone(path), opts),
            multi,
            sql: false,
            sql_snapshot_check: false,
        }
    }

    /// Serves an SQL statement's engine transaction `txn`: the store's
    /// writes join it (and its commit); documents and entries are locked
    /// exclusively, waiting up to the transaction's lock wait, a document's
    /// lock with the snapshot check when `snapshot_check` (a session's
    /// transaction on MariaDB's port); the first engine error stays
    /// available ([`DocTxn::take_engine_error`]). [`DocTxn::into_txn`] gives
    /// the transaction back.
    #[must_use]
    pub fn adopt(txn: Txn, snapshot_check: bool) -> Self {
        Self {
            txn,
            multi: false,
            sql: true,
            sql_snapshot_check: snapshot_check,
        }
    }

    /// Puts `txn` in place of the engine transaction and returns that one:
    /// a transaction lent for a command the store doesn't run itself (an
    /// SQL statement on a table) and given back the same way.
    pub fn swap_engine_txn(&mut self, txn: Txn) -> Txn {
        std::mem::replace(&mut self.txn, txn)
    }

    /// The engine transaction, back to its owner.
    #[must_use]
    pub fn into_txn(mut self) -> Txn {
        let _ = self.take_engine_error();
        self.txn
    }

    /// The first engine error a call met (lock waits, deadlocks, snapshot
    /// conflicts), as the engine raised it; only kept in SQL mode.
    pub fn take_engine_error(&mut self) -> Option<SprigError> {
        if !self.sql {
            return None;
        }
        sql_errors()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.txn.id())
    }

    fn err(&mut self, e: &SprigError) -> MongoError {
        if self.sql {
            sql_errors()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(self.txn.id())
                .or_insert_with(|| SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                });
        }
        engine_error(e, self.multi)
    }

    /// Locks a document's key for writing (snapshot-checked inside a
    /// multi-document transaction).
    ///
    /// # Errors
    /// A write conflict.
    pub async fn lock_doc(&mut self, key: &[u8]) -> Result<(), MongoError> {
        let req = if self.sql {
            LockRequest {
                check_snapshot: self.sql_snapshot_check,
                ..LockRequest::write()
            }
        } else if self.multi {
            LockRequest::for_update().with_wait(Wait::NoWait)
        } else {
            LockRequest::write()
        };
        let k = Bytes::copy_from_slice(key);
        self.txn
            .lock(&k, req)
            .await
            .map(|_| ())
            .map_err(|e| self.err(&e))
    }

    /// Locks an entry key (unique entries, counters, records).
    ///
    /// # Errors
    /// A write conflict.
    pub async fn lock_entry(&mut self, key: &[u8]) -> Result<(), MongoError> {
        let req = if self.sql {
            // An entry is no row: no snapshot check.
            LockRequest::write()
        } else if self.multi {
            LockRequest::write().with_wait(Wait::NoWait)
        } else {
            LockRequest::write()
        };
        let k = Bytes::copy_from_slice(key);
        self.txn
            .lock(&k, req)
            .await
            .map(|_| ())
            .map_err(|e| self.err(&e))
    }

    /// A snapshot read.
    ///
    /// # Errors
    /// The store failed.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>, MongoError> {
        self.txn.get_snapshot(key).map_err(|e| self.err(&e))
    }

    /// A current read (after locking).
    ///
    /// # Errors
    /// The store failed.
    pub fn get_current(&mut self, key: &[u8]) -> Result<Option<Bytes>, MongoError> {
        self.txn.read_current(key).map_err(|e| self.err(&e))
    }

    /// Up to `limit` live keys of `[lo, hi)` at the snapshot.
    ///
    /// # Errors
    /// The store failed.
    pub fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        dir: Dir,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>, MongoError> {
        let r = KeyRange::new(lo, hi);
        self.txn
            .scan(&r, dir, limit, sprig_engine::txn::ReadMode::Snapshot)
            .map_err(|e| self.err(&e))
    }

    /// Buffers a put (the key locked first).
    ///
    /// # Errors
    /// The transaction is too big, or the store failed.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), MongoError> {
        self.txn.put(key, value).map_err(|e| self.err(&e))
    }

    /// Buffers a delete (the key locked first).
    ///
    /// # Errors
    /// As for [`Self::put`].
    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), MongoError> {
        self.txn.delete(key).map_err(|e| self.err(&e))
    }

    /// Notes what the write of document key `key` (a logical key) was, for
    /// the change log (a change stream's operation type).
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn hint(
        &mut self,
        key: &[u8],
        op: sprig_engine::observe::hint::HintOp,
    ) -> Result<(), MongoError> {
        let tenant = key
            .get(..8)
            .and_then(|b| b.try_into().ok())
            .map_or(0, u64::from_be_bytes);
        self.txn
            .add_note_for_tenant(tenant, sprig_engine::observe::hint::encode(key, op, &[]))
            .map_err(|e| self.err(&e))
    }

    /// Starts a statement: its writes can be undone alone.
    pub fn begin_statement(&mut self) {
        self.txn.begin_statement();
    }

    /// Undoes the current statement's writes.
    pub fn rollback_statement(&mut self) {
        self.txn.rollback_statement();
    }

    /// A point the writes since can be undone to ([`DocTxn::rollback_to`]).
    #[must_use]
    pub fn savepoint(&self) -> sprig_engine::txn::Savepoint {
        self.txn.savepoint()
    }

    /// Undoes the writes made since `sp`; locks stay.
    pub fn rollback_to(&mut self, sp: sprig_engine::txn::Savepoint) {
        self.txn.rollback_to(sp);
    }

    /// Commits, and waits until the commit (and everything the
    /// transaction read) is durable.
    ///
    /// # Errors
    /// The log refused or lost the commit.
    pub async fn commit(self) -> Result<(), MongoError> {
        let multi = self.multi;
        let gate = self.txn.reply_gate();
        let t = self.txn.commit().map_err(|e| engine_error(&e, multi))?;
        t.wait_async()
            .await
            .map_err(|e| engine_error(&e, multi).label("UnknownTransactionCommitResult"))?;
        gate.wait_async().await.map_err(|e| engine_error(&e, multi))
    }

    /// Waits until everything read so far is durable (a read-only
    /// command's reply), then ends the transaction.
    ///
    /// # Errors
    /// The log failed.
    pub async fn finish_read(self) -> Result<(), MongoError> {
        let gate = self.txn.reply_gate();
        self.txn.rollback();
        gate.wait_async().await.map_err(|e| engine_error(&e, false))
    }

    /// Rolls back.
    pub fn rollback(self) {
        self.txn.rollback();
    }
}

/// Checks a `changeStreamPreAndPostImages` option: `{enabled: <bool>}`.
///
/// # Errors
/// 40414 without `enabled`; 14 for another type; 40415 for another field.
pub fn check_pre_post(v: &bson::Bson) -> Result<(), MongoError> {
    let d = v.as_document().ok_or_else(|| {
        MongoError::new(
            code::TYPE_MISMATCH,
            "BSON field 'changeStreamPreAndPostImages' is the wrong type, expected type 'object'",
        )
    })?;
    for (k, x) in d {
        if k != "enabled" {
            return Err(MongoError::new(
                40415,
                format!("BSON field 'changeStreamPreAndPostImages.{k}' is an unknown field."),
            ));
        }
        if !matches!(x, bson::Bson::Boolean(_)) {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'changeStreamPreAndPostImages.enabled' is the wrong type, expected type 'bool'",
            ));
        }
    }
    if !d.contains_key("enabled") {
        return Err(MongoError::new(
            40414,
            "BSON field 'changeStreamPreAndPostImages.enabled' is missing but a required field",
        ));
    }
    Ok(())
}

/// `db.coll`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Name {
    /// The database.
    pub db: String,
    /// The collection.
    pub coll: String,
}

impl Name {
    /// `db.coll`.
    #[must_use]
    pub fn new(db: &str, coll: &str) -> Self {
        Self {
            db: db.to_owned(),
            coll: coll.to_owned(),
        }
    }
}

impl std::fmt::Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.db, self.coll)
    }
}

/// The documents of every tenant served by one engine unit.
pub struct DocStore {
    path: Arc<dyn CommitPath>,
    /// Collections by tenant and name, as last read or changed.
    colls: Mutex<HashMap<(u64, Name), Arc<Coll>>>,
    /// Per tenant: writers hold it shared for a statement (or a
    /// transaction); DDL holds it exclusively, so no write runs against an
    /// old index list.
    ddl: Mutex<HashMap<u64, Arc<tokio::sync::RwLock<()>>>>,
    /// The bytes a blocking sort may hold (`doc.max_sort_bytes`).
    pub max_sort_bytes: usize,
    /// The bytes an aggregation stage may hold.
    pub stage_bytes: usize,
    /// A fixed clock for tests, ms since the epoch (`i64::MIN`: the
    /// system's).
    clock: AtomicI64,
    /// Open multi-document transactions.
    pub(crate) txns: cmd::TxnTable,
    /// The tenants served since start (the TTL sweeper's list).
    tenants: Mutex<std::collections::BTreeSet<u64>>,
    /// The online index builds running here.
    builds: build::Builds,
    /// Per tenant: how many times its collections were created, dropped
    /// or renamed here since start (the catalog follows them).
    generations: Mutex<HashMap<u64, u64>>,
}

impl std::fmt::Debug for DocStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocStore").finish_non_exhaustive()
    }
}

/// A DDL change: the exclusive hold of the store's DDL lock.
pub type DdlGuard = tokio::sync::OwnedRwLockWriteGuard<()>;
/// A writer's shared hold of the store's DDL lock.
pub type WriteGuard = tokio::sync::OwnedRwLockReadGuard<()>;

impl DocStore {
    /// A store on `path` (the node's engine, or a test path).
    #[must_use]
    pub fn new(path: Arc<dyn CommitPath>) -> Self {
        Self {
            path,
            colls: Mutex::new(HashMap::new()),
            ddl: Mutex::new(HashMap::new()),
            max_sort_bytes: 100 * 1024 * 1024,
            stage_bytes: 100 * 1024 * 1024,
            clock: AtomicI64::new(i64::MIN),
            txns: cmd::TxnTable::default(),
            tenants: Mutex::new(std::collections::BTreeSet::new()),
            builds: build::Builds::default(),
            generations: Mutex::new(HashMap::new()),
        }
    }

    /// How many times `tenant`'s collections were created, dropped or
    /// renamed on this node since start: the catalog's copy of them is
    /// brought up to date when it moves.
    #[must_use]
    pub fn generation(&self, tenant: u64) -> u64 {
        self.generations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .copied()
            .unwrap_or(0)
    }

    fn bump_generation(&self, tenant: u64) {
        *self
            .generations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(tenant)
            .or_insert(0) += 1;
    }

    /// Every collection of `tenant` (views included), as its record holds
    /// it: (database, name, engine object, whether it is a view).
    ///
    /// # Errors
    /// The store failed.
    pub fn namespaces(&self, tenant: u64) -> Result<Vec<Namespace>, MongoError> {
        let p = layout::coll_records(tenant, None);
        let end = layout::after_prefix(&p);
        let mut t = self.begin(false);
        let rows = t.scan(&p, &end, Dir::Forward, usize::MAX);
        t.rollback();
        let mut out = Vec::new();
        for (_, v) in rows? {
            let Ok(rec) = Document::from_reader(&mut &v[..]) else {
                continue;
            };
            let (Ok(db), Ok(name), Ok(n)) =
                (rec.get_str("db"), rec.get_str("name"), rec.get_i64("n"))
            else {
                continue;
            };
            let Ok(n) = u32::try_from(n) else { continue };
            out.push(Namespace {
                db: db.to_owned(),
                name: name.to_owned(),
                object: Space::new(tenant, n).object,
                view: rec
                    .get_document("options")
                    .is_ok_and(|o| o.contains_key("viewOn")),
            });
        }
        Ok(out)
    }

    /// The tenants with collections here: the registry, and those served
    /// since start.
    #[must_use]
    pub fn tenants(&self) -> Vec<u64> {
        let mut all: std::collections::BTreeSet<u64> = self
            .tenants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        let p = layout::tenant_entries();
        let mut t = self.begin(false);
        if let Ok(rows) = t.scan(&p, &layout::after_prefix(&p), Dir::Forward, usize::MAX) {
            for (k, _) in rows {
                if let Some(b) = k.get(p.len()..p.len() + 8)
                    && let Ok(a) = <[u8; 8]>::try_from(b)
                {
                    all.insert(u64::from_be_bytes(a));
                }
            }
        }
        t.rollback();
        all.into_iter().collect()
    }

    /// Notes that `tenant` is served here.
    pub fn note_tenant(&self, tenant: u64) {
        self.tenants
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant);
    }

    /// Fixes the clock at `ms` for tests (`None`: the system's again).
    pub fn set_clock(&self, ms: Option<i64>) {
        self.clock.store(ms.unwrap_or(i64::MIN), Ordering::Relaxed);
    }

    /// Now, ms since the epoch.
    #[must_use]
    pub fn now_ms(&self) -> i64 {
        match self.clock.load(Ordering::Relaxed) {
            i64::MIN => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
            ms => ms,
        }
    }

    /// Now, as a BSON date.
    #[must_use]
    pub fn now(&self) -> bson::DateTime {
        bson::DateTime::from_millis(self.now_ms())
    }

    /// A new transaction.
    #[must_use]
    pub fn begin(&self, multi: bool) -> DocTxn {
        DocTxn::begin(&self.path, multi)
    }

    fn ddl_lock(&self, tenant: u64) -> Arc<tokio::sync::RwLock<()>> {
        let mut g = self.ddl.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(g.entry(tenant).or_default())
    }

    /// A writer's hold on `tenant`'s DDL lock.
    pub async fn write_guard(&self, tenant: u64) -> WriteGuard {
        self.ddl_lock(tenant).read_owned().await
    }

    /// DDL's exclusive hold. While it waits, transactions past MongoDB's
    /// 60-second lifetime are aborted (an idle one holds the writers'
    /// side).
    pub async fn ddl_guard(&self, tenant: u64) -> DdlGuard {
        loop {
            let w = self.ddl_lock(tenant).write_owned();
            if let Ok(g) = tokio::time::timeout(std::time::Duration::from_secs(1), w).await {
                return g;
            }
            self.txns.reap(std::time::Duration::from_secs(60));
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, HashMap<(u64, Name), Arc<Coll>>> {
        self.colls.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Forgets every cached collection of `tenant` (after a crash test or
    /// a change made elsewhere).
    pub fn forget(&self, tenant: u64) {
        self.cache().retain(|(t, _), _| *t != tenant);
    }

    /// Collection `name` of `tenant`, if it exists.
    ///
    /// # Errors
    /// The store failed, or the record is damaged.
    pub fn coll(&self, tenant: u64, name: &Name) -> Result<Option<Arc<Coll>>, MongoError> {
        self.note_tenant(tenant);
        if let Some(c) = self.cache().get(&(tenant, name.clone())) {
            return Ok(Some(Arc::clone(c)));
        }
        let mut t = self.begin(false);
        let got = Self::load(&mut t, tenant, name)?;
        t.rollback();
        if let Some(c) = &got {
            self.cache().insert((tenant, name.clone()), Arc::clone(c));
        }
        Ok(got)
    }

    fn load(t: &mut DocTxn, tenant: u64, name: &Name) -> Result<Option<Arc<Coll>>, MongoError> {
        let Some(v) = t.get(&layout::coll_record(tenant, &name.db, &name.coll))? else {
            return Ok(None);
        };
        let rec = Document::from_reader(&mut &v[..])
            .map_err(|_| MongoError::new(code::INTERNAL_ERROR, "damaged collection record"))?;
        let meta = CollMeta::from_record(&name.db, &name.coll, &rec)?;
        let space = Space::new(tenant, meta.n);
        let mut mk = Vec::with_capacity(meta.indexes.len());
        for i in &meta.indexes {
            mk.push(t.get(&space.multikey(i.id))?.is_some());
        }
        Ok(Some(Arc::new(Coll::new(meta, tenant, &mk))))
    }

    /// The collections of `db` (every database: `None`), from the catalog.
    ///
    /// # Errors
    /// The store failed.
    pub fn list(&self, tenant: u64, db: Option<&str>) -> Result<Vec<Arc<Coll>>, MongoError> {
        let p = layout::coll_records(tenant, db);
        let end = layout::after_prefix(&p);
        let mut t = self.begin(false);
        let rows = t.scan(&p, &end, Dir::Forward, usize::MAX)?;
        let mut out = Vec::with_capacity(rows.len());
        for (_, v) in rows {
            let rec = Document::from_reader(&mut &v[..])
                .map_err(|_| MongoError::new(code::INTERNAL_ERROR, "damaged collection record"))?;
            let (Ok(d), Ok(c)) = (rec.get_str("db"), rec.get_str("name")) else {
                continue;
            };
            let name = Name::new(d, c);
            if let Some(c) = Self::load(&mut t, tenant, &name)? {
                out.push(c);
            }
        }
        t.rollback();
        Ok(out)
    }

    /// Writes `meta` as collection `name`'s record in `t`, and caches it
    /// once `t` commits (the caller holds the DDL lock).
    fn put_record(t: &mut DocTxn, tenant: u64, meta: &CollMeta) -> Result<(), MongoError> {
        let mut rec = meta.to_record();
        rec.insert("db", meta.db.clone());
        rec.insert("name", meta.name.clone());
        let mut v = Vec::new();
        rec.to_writer(&mut v)
            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.to_string()))?;
        t.put(layout::coll_record(tenant, &meta.db, &meta.name), v)
    }

    /// Creates collection `name` with `options` (DDL lock held).
    /// `Ok(None)` when it exists.
    ///
    /// # Errors
    /// The store failed; 73 for bad names.
    pub async fn create_locked(
        &self,
        tenant: u64,
        name: &Name,
        options: Document,
    ) -> Result<Option<Arc<Coll>>, MongoError> {
        crate::engine::check_db_name(&name.db)?;
        crate::engine::check_coll_name(&name.coll)?;
        let mut t = self.begin(false);
        let rk = layout::coll_record(tenant, &name.db, &name.coll);
        t.lock_entry(&rk).await?;
        if t.get_current(&rk)?.is_some() {
            t.rollback();
            self.cache().remove(&(tenant, name.clone()));
            return Ok(None);
        }
        let ck = layout::coll_counter(tenant);
        t.lock_entry(&ck).await?;
        let n = t
            .get_current(&ck)?
            .and_then(|v| <[u8; 4]>::try_from(&v[..]).ok())
            .map_or(1, u32::from_be_bytes);
        if n > layout::MAX_COLLECTION {
            t.rollback();
            return Err(MongoError::new(
                code::CANNOT_CREATE_INDEX,
                "too many collections in this tenant",
            ));
        }
        t.put(ck, (n + 1).to_be_bytes().to_vec())?;
        if n == 1 {
            // The tenant's first collection: register the tenant.
            let tk = layout::tenant_entry(tenant);
            t.lock_entry(&tk).await?;
            t.put(tk, Vec::new())?;
        }
        let meta = CollMeta::new(&name.db, &name.coll, n, options);
        Self::put_record(&mut t, tenant, &meta)?;
        t.commit().await?;
        self.bump_generation(tenant);
        let c = Arc::new(Coll::new(meta, tenant, &[]));
        self.cache().insert((tenant, name.clone()), Arc::clone(&c));
        Ok(Some(c))
    }

    /// Collection `name`, created with default options if missing (by a
    /// write that creates it implicitly).
    ///
    /// # Errors
    /// As for [`Self::create_locked`].
    pub async fn coll_or_create(&self, tenant: u64, name: &Name) -> Result<Arc<Coll>, MongoError> {
        if let Some(c) = self.coll(tenant, name)? {
            return Ok(c);
        }
        // A new collection changes no writer's index list: no DDL lock (a
        // transaction holding the writers' side may create one).
        if let Some(c) = self.create_locked(tenant, name, Document::new()).await? {
            return Ok(c);
        }
        self.coll(tenant, name)?
            .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "collection vanished"))
    }

    /// Replaces collection `c`'s record with `meta` (DDL lock held).
    ///
    /// # Errors
    /// The store failed.
    async fn replace_meta(&self, tenant: u64, meta: CollMeta) -> Result<Arc<Coll>, MongoError> {
        let mut t = self.begin(false);
        let rk = layout::coll_record(tenant, &meta.db, &meta.name);
        t.lock_entry(&rk).await?;
        Self::put_record(&mut t, tenant, &meta)?;
        t.commit().await?;
        let name = Name::new(&meta.db, &meta.name);
        self.cache().remove(&(tenant, name.clone()));
        self.coll(tenant, &name)?
            .ok_or_else(|| MongoError::new(code::INTERNAL_ERROR, "collection vanished"))
    }

    /// Deletes every key in `[lo, hi)` in batches, each its own
    /// transaction (DDL lock held, so nothing writes there).
    async fn delete_range(&self, lo: &[u8], hi: &[u8]) -> Result<(), MongoError> {
        loop {
            let mut t = self.begin(false);
            let rows = t.scan(lo, hi, Dir::Forward, 1000)?;
            if rows.is_empty() {
                t.rollback();
                return Ok(());
            }
            for (k, _) in rows {
                t.lock_entry(&k).await?;
                t.delete(k)?;
            }
            t.commit().await?;
        }
    }

    /// Drops collection `name` (DDL lock held). Whether it existed.
    ///
    /// # Errors
    /// The store failed.
    pub async fn drop_locked(&self, tenant: u64, name: &Name) -> Result<bool, MongoError> {
        let Some(c) = self.coll(tenant, name)? else {
            return Ok(false);
        };
        let mut t = self.begin(false);
        let rk = layout::coll_record(tenant, &name.db, &name.coll);
        t.lock_entry(&rk).await?;
        t.delete(rk)?;
        t.commit().await?;
        self.bump_generation(tenant);
        self.cache().remove(&(tenant, name.clone()));
        let all = c.space.all();
        self.delete_range(&all, &layout::after_prefix(&all)).await?;
        Ok(true)
    }

    /// Renames collection `from` to `to` (DDL lock held): the number,
    /// documents and indexes stay.
    ///
    /// # Errors
    /// 26 when `from` is missing; 48 when `to` exists and `drop_target` is
    /// false.
    pub async fn rename_locked(
        &self,
        tenant: u64,
        from: &Name,
        to: &Name,
        drop_target: bool,
    ) -> Result<(), MongoError> {
        let Some(c) = self.coll(tenant, from)? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                "source namespace does not exist",
            ));
        };
        crate::engine::check_db_name(&to.db)?;
        crate::engine::check_coll_name(&to.coll)?;
        if from == to {
            return Err(MongoError::new(
                code::ILLEGAL_OPERATION,
                "Can't rename a collection to itself",
            ));
        }
        if self.coll(tenant, to)?.is_some() {
            if !drop_target {
                return Err(MongoError::new(
                    code::NAMESPACE_EXISTS,
                    "target namespace exists",
                ));
            }
            self.drop_locked(tenant, to).await?;
        }
        let mut meta = c.meta.clone();
        meta.db.clone_from(&to.db);
        meta.name.clone_from(&to.coll);
        let mut t = self.begin(false);
        let old = layout::coll_record(tenant, &from.db, &from.coll);
        t.lock_entry(&old).await?;
        t.delete(old)?;
        let to_key = layout::coll_record(tenant, &to.db, &to.coll);
        t.lock_entry(&to_key).await?;
        // Created meanwhile by a write (implicit creation takes no DDL lock).
        if t.get_current(&to_key)?.is_some() {
            t.rollback();
            self.cache().remove(&(tenant, to.clone()));
            return Err(MongoError::new(
                code::NAMESPACE_EXISTS,
                "target namespace exists",
            ));
        }
        Self::put_record(&mut t, tenant, &meta)?;
        t.commit().await?;
        self.bump_generation(tenant);
        self.cache().remove(&(tenant, from.clone()));
        self.cache().remove(&(tenant, to.clone()));
        Ok(())
    }

    /// Adds indexes to `name` (creating it if missing) and builds them over
    /// the documents present (DDL lock held, so no writer runs meanwhile).
    /// Returns (indexes before, after, created the collection).
    ///
    /// # Errors
    /// The specifications' errors; 11000 when a unique index meets a
    /// duplicate (the index is then not added); 171 and 16766 from
    /// documents that can't be indexed.
    pub async fn create_indexes_locked(
        &self,
        tenant: u64,
        name: &Name,
        specs: &[Document],
    ) -> Result<(usize, usize, bool), MongoError> {
        let (c, created) = match self.coll(tenant, name)? {
            Some(c) => (c, false),
            None => match self.create_locked(tenant, name, Document::new()).await? {
                Some(c) => (c, true),
                None => (
                    self.coll(tenant, name)?.ok_or_else(|| {
                        MongoError::new(code::INTERNAL_ERROR, "collection vanished")
                    })?,
                    false,
                ),
            },
        };
        let before = c.meta.indexes.len() + 1;
        let mut meta = c.meta.clone();
        let mut fresh = Vec::new();
        for s in specs {
            let m = catalog::parse_index(&meta.index_spec(s), meta.next_index)?;
            if let catalog::Added::New = meta.add_index(m)? {
                fresh.push(meta.next_index);
                meta.next_index += 1;
            }
        }
        if fresh.is_empty() {
            return Ok((before, before, created));
        }
        // A build cut short by a crash may have left entries under these
        // numbers (the record keeps the counter only once a build ends).
        for i in &fresh {
            for unique in [false, true] {
                let p = c.space.index(*i, unique);
                self.delete_range(&p, &layout::after_prefix(&p)).await?;
            }
            let mk = c.space.multikey(*i);
            self.delete_range(&mk, &layout::after_prefix(&mk)).await?;
        }
        // Build: every document's entries for the new indexes.
        let docs_p = c.space.docs();
        let docs_end = layout::after_prefix(&docs_p);
        let mut lo = docs_p.clone();
        let mut multikey: Vec<u16> = Vec::new();
        let new_idx: Vec<&catalog::IndexMeta> = meta
            .indexes
            .iter()
            .filter(|i| fresh.contains(&i.id))
            .collect();
        // MongoDB extracts every document's keys before it checks for
        // duplicates: a document that can't be indexed (171, 16766) is
        // reported first.
        let checked = async {
            let mut lo = docs_p.clone();
            loop {
                let mut t = self.begin(false);
                let rows = t.scan(&lo, &docs_end, Dir::Forward, 500)?;
                t.rollback();
                let Some((last, _)) = rows.last() else {
                    return Ok::<(), MongoError>(());
                };
                lo = layout::after_prefix(last);
                for (_, v) in &rows {
                    let d = read::decode(v)?;
                    for idx in &new_idx {
                        keys::entries(&d, idx)?;
                    }
                }
            }
        }
        .await;
        checked?;
        let built = async {
            loop {
                let mut t = self.begin(false);
                let rows = t.scan(&lo, &docs_end, Dir::Forward, 500)?;
                let Some((last, _)) = rows.last() else {
                    t.rollback();
                    return Ok::<(), MongoError>(());
                };
                lo = layout::after_prefix(last);
                for (k, v) in &rows {
                    let d = read::decode(v)?;
                    let id = &k[docs_p.len()..];
                    for idx in &new_idx {
                        let (entries, multi) = keys::entries(&d, idx)?;
                        if multi && !multikey.contains(&idx.id) {
                            multikey.push(idx.id);
                        }
                        write::put_entries(&mut t, &c.space, idx, id, &entries, &d, &c.ns())
                            .await?;
                    }
                }
                t.commit().await?;
            }
        }
        .await;
        if let Err(e) = built {
            for i in &fresh {
                for unique in [false, true] {
                    let p = c.space.index(*i, unique);
                    self.delete_range(&p, &layout::after_prefix(&p)).await?;
                }
            }
            return Err(e);
        }
        if !multikey.is_empty() {
            let mut t = self.begin(false);
            for i in &multikey {
                let k = c.space.multikey(*i);
                t.lock_entry(&k).await?;
                t.put(k, vec![1])?;
            }
            t.commit().await?;
        }
        let after = meta.indexes.len() + 1;
        self.replace_meta(tenant, meta).await?;
        Ok((before, after, created))
    }

    /// Drops index `which` (a name, or a key pattern) of `name`, or every
    /// index but `_id_` for `"*"` (DDL lock held). Returns the number of
    /// indexes before.
    ///
    /// # Errors
    /// 26 when the collection is missing; 27 for an unknown index; 72 for
    /// `_id_`.
    pub async fn drop_indexes_locked(
        &self,
        tenant: u64,
        name: &Name,
        which: &[bson::Bson],
    ) -> Result<usize, MongoError> {
        let Some(c) = self.coll(tenant, name)? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("ns not found {name}"),
            ));
        };
        let before = c.meta.indexes.len() + 1;
        let mut meta = c.meta.clone();
        let mut gone = Vec::new();
        for w in which {
            match w {
                bson::Bson::String(s) if s == "*" => {
                    gone.extend(meta.indexes.drain(..).map(|i| (i.id, i.unique)));
                }
                bson::Bson::String(s) if s == "_id_" => {
                    return Err(MongoError::new(
                        code::INVALID_OPTIONS,
                        "cannot drop _id index",
                    ));
                }
                bson::Bson::String(s) => {
                    let Some(p) = meta.indexes.iter().position(|i| &i.name == s) else {
                        return Err(MongoError::new(
                            code::INDEX_NOT_FOUND,
                            format!("index not found with name [{s}]"),
                        ));
                    };
                    let i = meta.indexes.remove(p);
                    gone.push((i.id, i.unique));
                }
                bson::Bson::Document(k) => {
                    if k == &doc! {"_id": 1} {
                        return Err(MongoError::new(
                            code::INVALID_OPTIONS,
                            "cannot drop _id index",
                        ));
                    }
                    let found: Vec<usize> = meta
                        .indexes
                        .iter()
                        .enumerate()
                        .filter(|(_, i)| &i.key() == k)
                        .map(|(p, _)| p)
                        .collect();
                    match found.as_slice() {
                        [] => {
                            return Err(MongoError::new(
                                code::INDEX_NOT_FOUND,
                                format!("can't find index with key: {k}"),
                            ));
                        }
                        [p] => {
                            let i = meta.indexes.remove(*p);
                            gone.push((i.id, i.unique));
                        }
                        _ => {
                            return Err(MongoError::new(
                                code::INDEX_NOT_FOUND,
                                format!(
                                    "{} indexes found for key: {k}, identify by name instead.",
                                    found.len()
                                ),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(MongoError::new(
                        code::TYPE_MISMATCH,
                        "dropIndexes index must be a string or an object",
                    ));
                }
            }
        }
        if gone.is_empty() {
            return Ok(before);
        }
        self.replace_meta(tenant, meta).await?;
        for (id, unique) in gone {
            let p = c.space.index(id, unique);
            self.delete_range(&p, &layout::after_prefix(&p)).await?;
            for p in [c.space.multikey(id), c.space.dups(id)] {
                self.delete_range(&p, &layout::after_prefix(&p)).await?;
            }
        }
        Ok(before)
    }

    /// Changes collection options or an index's `hidden` or
    /// `expireAfterSeconds` (DDL lock held). Returns the reply's extra
    /// fields.
    ///
    /// # Errors
    /// 26 for a missing collection; 27 for an unknown index; 72 for options
    /// the store doesn't change.
    pub async fn coll_mod_locked(
        &self,
        tenant: u64,
        name: &Name,
        body: &Document,
    ) -> Result<Document, MongoError> {
        let Some(c) = self.coll(tenant, name)? else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("ns does not exist: {name}"),
            ));
        };
        let mut meta = c.meta.clone();
        let mut out = Document::new();
        for (k, v) in body {
            match k.as_str() {
                "collMod"
                | "$db"
                | "lsid"
                | "$clusterTime"
                | "writeConcern"
                | "comment"
                | "txnNumber"
                | "$readPreference"
                | "apiVersion"
                | "apiStrict"
                | "apiDeprecationErrors" => {}
                "changeStreamPreAndPostImages" => {
                    check_pre_post(v)?;
                    meta.options.insert(k.clone(), v.clone());
                }
                "validator" | "validationLevel" | "validationAction" => {
                    meta.options.insert(k.clone(), v.clone());
                    if k == "validator" {
                        meta.validator = v.as_document().map(crate::filter::parse).transpose()?;
                    }
                }
                "index" => {
                    let d = v.as_document().ok_or_else(|| {
                        MongoError::new(code::TYPE_MISMATCH, "index must be an object")
                    })?;
                    let pos = if let Ok(n) = d.get_str("name") {
                        meta.indexes.iter().position(|i| i.name == n)
                    } else if let Ok(kp) = d.get_document("keyPattern") {
                        meta.indexes.iter().position(|i| &i.key() == kp)
                    } else {
                        return Err(MongoError::new(
                            code::INVALID_OPTIONS,
                            "must specify either index name or key pattern",
                        ));
                    };
                    let Some(p) = pos else {
                        return Err(MongoError::new(
                            code::INDEX_NOT_FOUND,
                            format!("cannot find index {d} for ns {name}"),
                        ));
                    };
                    let i = &mut meta.indexes[p];
                    if let Some(h) = d.get("hidden") {
                        let h = crate::filter::truthy(h);
                        out.insert("hidden_old", i.hidden);
                        out.insert("hidden_new", h);
                        i.hidden = h;
                        i.spec.insert("hidden", h);
                        if !h {
                            i.spec.remove("hidden");
                        }
                    }
                    if let Some(e) = d.get("expireAfterSeconds") {
                        let n = crate::filter::as_i64(e).ok_or_else(|| {
                            MongoError::new(
                                code::TYPE_MISMATCH,
                                "expireAfterSeconds must be a number",
                            )
                        })?;
                        if i.expire_after.is_none() {
                            return Err(MongoError::new(
                                code::INVALID_OPTIONS,
                                "no expireAfterSeconds field to update",
                            ));
                        }
                        out.insert("expireAfterSeconds_old", i.expire_after.unwrap_or(0));
                        out.insert("expireAfterSeconds_new", n);
                        i.expire_after = Some(n);
                        i.spec
                            .insert("expireAfterSeconds", i32::try_from(n).unwrap_or(i32::MAX));
                    }
                }
                other => {
                    return Err(MongoError::new(
                        code::INVALID_OPTIONS,
                        format!(
                            "collMod option '{other}' is not supported on collections stored on the SprigDB engine yet"
                        ),
                    ));
                }
            }
        }
        self.replace_meta(tenant, meta).await?;
        Ok(out)
    }
}
