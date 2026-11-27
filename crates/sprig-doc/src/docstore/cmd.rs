//! The MongoDB data commands over the store, answered as the reply
//! documents MongoDB 8.0 sends (cursor ids are the caller's: a command
//! that opens a cursor returns its first batch and the rest).
//!
//! Sessions: a multi-document transaction (`autocommit: false`) is one
//! engine transaction, held between commands by `(tenant, lsid)`. A
//! retryable write (`txnNumber` outside a transaction) stores its reply in
//! the same engine transaction as its writes, so a retry gets the first
//! answer back instead of writing twice. Outside a transaction a write
//! command is one engine transaction; a write conflict rolls it back
//! whole and runs it again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::read::{FindArgs, StoreCursor};
use super::write::UpdateStmt;
use super::{DocStore, DocTxn, Name, WriteGuard};

/// What a command answers.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // one per command, moved once
pub enum Reply {
    /// A plain reply document.
    Doc(Document),
    /// A cursor: the first batch now, and the rest when more may follow.
    Cursor {
        /// `cursor.ns`.
        ns: String,
        /// `cursor.firstBatch`.
        first: Vec<Document>,
        /// The rest (`None`: the cursor is exhausted, id 0).
        rest: Option<StoreCursor>,
    },
}

impl Reply {
    /// The reply document with cursor id `id` (`0` when exhausted).
    #[must_use]
    pub fn into_doc(self, id: i64) -> Document {
        match self {
            Reply::Doc(d) => d,
            Reply::Cursor { ns, first, rest } => {
                let id = if rest.is_some() { id } else { 0 };
                doc! {"cursor": {"firstBatch": first, "id": id, "ns": ns}, "ok": 1.0}
            }
        }
    }
}

/// A multi-document transaction between commands.
#[derive(Debug)]
pub struct TxnSlot {
    owner: String,
    txn_number: i64,
    state: TxnState,
    started: std::time::Instant,
    /// Collections and indexes the transaction created (at once, outside
    /// it): an abort drops them again.
    created: Vec<Created>,
}

/// A collection or index a transaction created.
#[derive(Debug, Clone)]
enum Created {
    Coll(Name),
    Index(Name, String),
}

#[derive(Debug)]
enum TxnState {
    Active(Box<DocTxn>, WriteGuard),
    Committed,
    Aborted,
}

/// The open transactions by tenant and `lsid`.
#[derive(Debug, Default)]
pub struct TxnTable {
    map: Mutex<Slots>,
    /// What each open transaction created, by tenant and `lsid`: other
    /// sessions' listings leave it out until the commit.
    hidden: Mutex<Hidden>,
}

type Slots = HashMap<(u64, Vec<u8>), Arc<tokio::sync::Mutex<TxnSlot>>>;
/// What open transactions created, by tenant and `lsid`.
type Hidden = HashMap<(u64, Vec<u8>), Vec<Created>>;
type SlotEntry = ((u64, Vec<u8>), Arc<tokio::sync::Mutex<TxnSlot>>);

impl TxnTable {
    /// Aborts every open transaction older than `lifetime` (MongoDB's
    /// `transactionLifetimeLimitSeconds`, 60 by default). Returns how many.
    pub fn reap(&self, lifetime: std::time::Duration) -> usize {
        let all: Vec<SlotEntry> = self
            .map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let mut n = 0;
        for (k, s) in all {
            // A slot busy with a command is in use: not idle.
            let Ok(mut g) = s.try_lock() else { continue };
            if g.started.elapsed() < lifetime || !matches!(g.state, TxnState::Active(..)) {
                continue;
            }
            if let TxnState::Active(t, _) = std::mem::replace(&mut g.state, TxnState::Aborted) {
                t.rollback();
                n += 1;
                // What it created stays, now visible (nothing drops it here).
                g.created.clear();
                self.set_hidden(k.0, &k.1, &[]);
            }
        }
        n
    }

    /// Records what the open transaction of `lsid` has created so far
    /// (nothing: it ended).
    fn set_hidden(&self, tenant: u64, lsid: &[u8], created: &[Created]) {
        let mut h = self.hidden.lock().unwrap_or_else(PoisonError::into_inner);
        if created.is_empty() {
            h.remove(&(tenant, lsid.to_vec()));
        } else {
            h.insert((tenant, lsid.to_vec()), created.to_vec());
        }
    }

    /// The collections of `db` and the indexes of `ns` that open
    /// transactions created: `(collections, indexes of ns)`.
    fn hidden(&self, tenant: u64, db: &str, ns: Option<&Name>) -> (Vec<String>, Vec<String>) {
        let h = self.hidden.lock().unwrap_or_else(PoisonError::into_inner);
        let (mut colls, mut idx) = (Vec::new(), Vec::new());
        for ((t, _), list) in h.iter() {
            if *t != tenant {
                continue;
            }
            for c in list {
                match c {
                    Created::Coll(n) if n.db == db => colls.push(n.coll.clone()),
                    Created::Index(n, i) if Some(n) == ns => idx.push(i.clone()),
                    _ => {}
                }
            }
        }
        (colls, idx)
    }

    fn get(&self, tenant: u64, lsid: &[u8]) -> Option<Arc<tokio::sync::Mutex<TxnSlot>>> {
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(tenant, lsid.to_vec()))
            .cloned()
    }

    fn put(&self, tenant: u64, lsid: &[u8], s: TxnSlot) -> Arc<tokio::sync::Mutex<TxnSlot>> {
        let a = Arc::new(tokio::sync::Mutex::new(s));
        self.map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((tenant, lsid.to_vec()), Arc::clone(&a));
        a
    }

    /// Ends every session of `user` of `tenant` (`killAllSessions`):
    /// their open transactions roll back. Returns how many were open.
    async fn kill_owner(&self, tenant: u64, user: &str, undo: &mut Vec<Created>) -> usize {
        let all: Vec<SlotEntry> = self
            .map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|((t, _), _)| *t == tenant)
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let mut n = 0;
        for (k, s) in all {
            let mut g = s.lock().await;
            if g.owner != user {
                continue;
            }
            if let TxnState::Active(t, _) = std::mem::replace(&mut g.state, TxnState::Aborted) {
                t.rollback();
                n += 1;
                undo.extend(std::mem::take(&mut g.created));
            }
            drop(g);
            self.set_hidden(k.0, &k.1, &[]);
            self.map
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&k);
        }
        n
    }

    /// Ends session `lsid` of `tenant`: an open transaction rolls back.
    /// Returns what it created, to drop.
    async fn end_session(&self, tenant: u64, lsid: &[u8]) -> Vec<Created> {
        let s = self
            .map
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant, lsid.to_vec()));
        self.set_hidden(tenant, lsid, &[]);
        if let Some(s) = s {
            let mut g = s.lock().await;
            if let TxnState::Active(t, _) = std::mem::replace(&mut g.state, TxnState::Aborted) {
                t.rollback();
                return std::mem::take(&mut g.created);
            }
        }
        Vec::new()
    }
}

/// A value as the shell prints it in error messages.
#[must_use]
pub fn shell(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{s}\""),
        Bson::Int32(n) => n.to_string(),
        Bson::Int64(n) => n.to_string(),
        Bson::Double(f) => {
            if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("{f:.1}")
            } else {
                f.to_string()
            }
        }
        Bson::ObjectId(o) => format!("ObjectId('{o}')"),
        Bson::Null => "null".to_owned(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Document(d) => {
            let inner = d
                .iter()
                .map(|(k, x)| format!("{k}: {}", shell(x)))
                .collect::<Vec<_>>()
                .join(", ");
            if inner.is_empty() {
                return "{}".to_owned();
            }
            format!("{{ {inner} }}")
        }
        Bson::Array(a) => {
            let inner = a.iter().map(shell).collect::<Vec<_>>().join(", ");
            format!("[ {inner} ]")
        }
        other => other.to_string(),
    }
}

fn lsid_of(body: &Document) -> Option<Vec<u8>> {
    match body.get_document("lsid").ok()?.get("id")? {
        Bson::Binary(b) => Some(b.bytes.clone()),
        other => Some(bson::to_vec(&doc! {"id": other.clone()}).ok()?),
    }
}

fn i64_of(body: &Document, k: &str) -> Option<i64> {
    crate::filter::as_i64(body.get(k)?)
}

fn coll_of(body: &Document, name: &str) -> Result<String, MongoError> {
    match body.get(name) {
        Some(Bson::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(Bson::String(_)) => Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!(
                "Invalid namespace specified '{}.'",
                body.get_str("$db").unwrap_or("")
            ),
        )),
        _ => Err(MongoError::new(
            code::INVALID_NAMESPACE,
            format!(
                "collection name has invalid type {}",
                body.get(name).map_or("missing", crate::update::type_name)
            ),
        )),
    }
}

fn docs_of(body: &Document, k: &str) -> Vec<Document> {
    body.get_array(k)
        .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
        .unwrap_or_default()
}

fn no_txn(n: i64) -> MongoError {
    MongoError::new(
        code::NO_SUCH_TRANSACTION,
        format!("Transaction with {{ txnNumber: {n} }} has been aborted."),
    )
    .label("TransientTransactionError")
}

/// The batch size a cursor command asks for (`None`: the default).
fn batch_of(body: &Document, name: &str) -> Result<Option<usize>, MongoError> {
    let v = if name == "find" {
        body.get("batchSize")
    } else {
        body.get_document("cursor")
            .ok()
            .and_then(|c| c.get("batchSize"))
    };
    match v.and_then(crate::filter::as_i64) {
        None => Ok(None),
        Some(n) if n < 0 => Err(MongoError::new(
            code::BAD_VALUE,
            "BSON field 'batchSize' value must be >= 0, actual value '-1'",
        )),
        Some(n) => Ok(Some(usize::try_from(n).unwrap_or(usize::MAX))),
    }
}

impl DocStore {
    /// Whether session `lsid` of `tenant` has a transaction here.
    #[must_use]
    pub fn has_txn(&self, tenant: u64, lsid: &[u8]) -> bool {
        self.txns.get(tenant, lsid).is_some()
    }

    /// Whether session `lsid` of `tenant` has transaction `n` here (open or
    /// ended): its commit or abort is the store's to answer.
    pub async fn has_txn_number(&self, tenant: u64, lsid: &[u8], n: i64) -> bool {
        match self.txns.get(tenant, lsid) {
            Some(s) => s.lock().await.txn_number == n,
            None => false,
        }
    }

    /// Ends session `lsid` of `tenant`: its open transaction rolls back.
    pub async fn end_session(&self, tenant: u64, lsid: &[u8]) {
        let undo = self.txns.end_session(tenant, lsid).await;
        self.undo_created(tenant, undo).await;
    }

    /// Ends every session `user` of `tenant` has here
    /// (`killAllSessions`): their open transactions roll back.
    pub async fn kill_sessions_of(&self, tenant: u64, user: &str) -> usize {
        let mut undo = Vec::new();
        let n = self.txns.kill_owner(tenant, user, &mut undo).await;
        self.undo_created(tenant, undo).await;
        n
    }

    /// Drops what an aborted transaction created: its indexes, and its
    /// collections while they hold no documents.
    async fn undo_created(&self, tenant: u64, list: Vec<Created>) {
        if list.is_empty() {
            return;
        }
        let _g = self.ddl_guard(tenant).await;
        for c in list.into_iter().rev() {
            match c {
                Created::Index(n, i) => {
                    let _ =
                        Box::pin(self.drop_indexes_locked(tenant, &n, &[Bson::String(i)])).await;
                }
                Created::Coll(n) => {
                    let Ok(Some(c)) = self.coll(tenant, &n) else {
                        continue;
                    };
                    let p = c.space.docs();
                    let mut t = self.begin(false);
                    let empty = t
                        .scan(
                            &p,
                            &super::layout::after_prefix(&p),
                            sprig_engine::txn::Dir::Forward,
                            1,
                        )
                        .is_ok_and(|r| r.is_empty());
                    t.rollback();
                    if empty {
                        let _ = Box::pin(self.drop_locked(tenant, &n)).await;
                    }
                }
            }
        }
    }

    /// Aborts session `lsid`'s open transaction, keeping its slot: the
    /// next statement or commit answers `NoSuchTransaction` (251).
    pub async fn abort_session_txn(&self, tenant: u64, lsid: &[u8]) {
        let Some(s) = self.txns.get(tenant, lsid) else {
            return;
        };
        let mut g = s.lock().await;
        if let TxnState::Active(t, guard) = std::mem::replace(&mut g.state, TxnState::Aborted) {
            t.rollback();
            drop(guard);
            let undo = std::mem::take(&mut g.created);
            drop(g);
            self.txns.set_hidden(tenant, lsid, &[]);
            self.undo_created(tenant, undo).await;
        }
    }

    /// A read (`find`, `aggregate`, `count`, `distinct`) at a kept
    /// snapshot: `t` is a read-only transaction opened for it
    /// (`readConcern: snapshot` outside a multi-document transaction).
    ///
    /// # Errors
    /// The command's.
    pub async fn snapshot_read(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        t: &mut DocTxn,
    ) -> Result<Reply, MongoError> {
        Box::pin(self.run(tenant, db, name, body, Some(t))).await
    }

    /// Aborts open transactions older than `lifetime`.
    #[must_use]
    pub fn reap_transactions(&self, lifetime: std::time::Duration) -> usize {
        self.txns.reap(lifetime)
    }
}

/// Whether `name` is a write command (`bulkWrite` included).
fn is_write(name: &str) -> bool {
    name == "bulkWrite" || WRITES.contains(&name)
}

const WRITES: [&str; 5] = [
    "insert",
    "update",
    "delete",
    "findAndModify",
    "findandmodify",
];

impl DocStore {
    /// Runs data command `name` of database `db` for `user` of `tenant`.
    ///
    /// # Errors
    /// The command's errors, as MongoDB codes.
    pub async fn command(
        &self,
        tenant: u64,
        user: &str,
        db: &str,
        name: &str,
        body: &Document,
    ) -> Result<Reply, MongoError> {
        let lsid = lsid_of(body);
        let txn_number = i64_of(body, "txnNumber");
        if matches!(name, "commitTransaction" | "abortTransaction") {
            let (Some(l), Some(n)) = (lsid, txn_number) else {
                return Err(MongoError::new(
                    code::NO_SUCH_TRANSACTION,
                    "no transaction in progress",
                ));
            };
            return self
                .end_txn(tenant, user, &l, n, name == "commitTransaction")
                .await;
        }
        if name == "endSessions" || name == "killSessions" {
            if let Ok(a) = body.get_array(name) {
                for s in a {
                    if let Some(Bson::Binary(b)) = s.as_document().and_then(|d| d.get("id")) {
                        self.end_session(tenant, &b.bytes).await;
                    }
                }
            }
            return Ok(Reply::Doc(doc! {"ok": 1.0}));
        }
        if name == "bulkWrite" && db != "admin" {
            return Err(MongoError::new(
                code::UNAUTHORIZED,
                "bulkWrite may only be run against the admin database.",
            ));
        }
        if body.get_bool("autocommit") == Ok(false) {
            let (Some(l), Some(n)) = (lsid, txn_number) else {
                return Err(MongoError::new(
                    code::INVALID_OPTIONS,
                    "autocommit: false needs lsid and txnNumber",
                ));
            };
            return self.in_txn(tenant, user, db, name, body, &l, n).await;
        }
        let retry = match (&lsid, txn_number) {
            (Some(l), Some(n)) if is_write(name) => Some((l.clone(), n)),
            _ => None,
        };
        if is_write(name) {
            return self.write_command(tenant, db, name, body, retry).await;
        }
        self.run(tenant, db, name, body, None).await
    }

    async fn end_txn(
        &self,
        tenant: u64,
        user: &str,
        lsid: &[u8],
        n: i64,
        commit: bool,
    ) -> Result<Reply, MongoError> {
        let Some(s) = self.txns.get(tenant, lsid) else {
            return Err(no_txn(n));
        };
        let mut g = s.lock().await;
        if g.owner != user || g.txn_number != n {
            return Err(no_txn(n));
        }
        match std::mem::replace(&mut g.state, TxnState::Aborted) {
            TxnState::Active(t, guard) => {
                if commit {
                    let r = t.commit().await;
                    drop(guard);
                    g.created.clear();
                    self.txns.set_hidden(tenant, lsid, &[]);
                    g.state = if r.is_ok() {
                        TxnState::Committed
                    } else {
                        TxnState::Aborted
                    };
                    r.map(|()| Reply::Doc(doc! {"ok": 1.0}))
                } else {
                    t.rollback();
                    drop(guard);
                    let undo = std::mem::take(&mut g.created);
                    drop(g);
                    self.txns.set_hidden(tenant, lsid, &[]);
                    self.undo_created(tenant, undo).await;
                    Ok(Reply::Doc(doc! {"ok": 1.0}))
                }
            }
            TxnState::Committed => {
                g.state = TxnState::Committed;
                if commit {
                    Ok(Reply::Doc(doc! {"ok": 1.0}))
                } else {
                    Err(MongoError::new(
                        256,
                        "Cannot abort a transaction that has already been committed",
                    ))
                }
            }
            TxnState::Aborted => Err(no_txn(n)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn in_txn(
        &self,
        tenant: u64,
        user: &str,
        db: &str,
        name: &str,
        body: &Document,
        lsid: &[u8],
        n: i64,
    ) -> Result<Reply, MongoError> {
        if !matches!(
            name,
            "find"
                | "getMore"
                | "aggregate"
                | "distinct"
                | "insert"
                | "update"
                | "delete"
                | "findAndModify"
                | "findandmodify"
                | "killCursors"
                | "bulkWrite"
                | "create"
                | "createIndexes"
        ) {
            return Err(MongoError::new(
                code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                format!("Cannot run '{name}' in a multi-document transaction."),
            ));
        }
        let start = body.get_bool("startTransaction") == Ok(true);
        let slot = match self.txns.get(tenant, lsid) {
            Some(s) => {
                let mut g = s.lock().await;
                if g.owner != user {
                    return Err(no_txn(n));
                }
                if start {
                    if n <= g.txn_number {
                        return Err(MongoError::new(
                            code::TRANSACTION_TOO_OLD,
                            format!(
                                "txnNumber {n} is less than or equal to the last txnNumber {} seen in session",
                                g.txn_number
                            ),
                        ));
                    }
                    if let TxnState::Active(t, _) =
                        std::mem::replace(&mut g.state, TxnState::Aborted)
                    {
                        t.rollback();
                    }
                    let guard = self.write_guard(tenant).await;
                    g.state = TxnState::Active(Box::new(self.begin(true)), guard);
                    g.created.clear();
                    self.txns.set_hidden(tenant, lsid, &[]);
                    g.txn_number = n;
                    g.started = std::time::Instant::now();
                } else if g.txn_number != n || !matches!(g.state, TxnState::Active(..)) {
                    return Err(no_txn(n));
                }
                drop(g);
                s
            }
            None if start => {
                let guard = self.write_guard(tenant).await;
                self.txns.put(
                    tenant,
                    lsid,
                    TxnSlot {
                        owner: user.to_owned(),
                        txn_number: n,
                        state: TxnState::Active(Box::new(self.begin(true)), guard),
                        started: std::time::Instant::now(),
                        created: Vec::new(),
                    },
                )
            }
            None => return Err(no_txn(n)),
        };
        let mut g = slot.lock().await;
        if !matches!(g.state, TxnState::Active(..)) {
            return Err(no_txn(n));
        }
        // A collection a write creates is created at once, outside the
        // transaction; an abort drops it again.
        let bulk = if name == "bulkWrite" {
            Some(super::bulk::parse(body)?)
        } else {
            None
        };
        let mut wanted = Vec::new();
        if let Some(ops) = &bulk {
            wanted = super::bulk::creates(ops);
        } else if WRITES.contains(&name) || matches!(name, "create" | "createIndexes") {
            wanted.push(Name::new(db, &coll_of(body, name)?));
        }
        let mut made = Vec::new();
        for ns in wanted {
            if self.coll(tenant, &ns)?.is_none() {
                made.push(Created::Coll(ns.clone()));
                if name != "create" {
                    let _ = self.coll_or_create(tenant, &ns).await?;
                }
            }
        }
        let indexes_before: Vec<String> = if name == "createIndexes" {
            let ns = Name::new(db, &coll_of(body, name)?);
            self.coll(tenant, &ns)?
                .map(|c| c.meta.indexes.iter().map(|i| i.name.clone()).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let TxnState::Active(t, _) = &mut g.state else {
            return Err(no_txn(n));
        };
        let r = if is_write(name) {
            self.write_any(tenant, db, name, body, bulk.as_deref(), t, true)
                .await
        } else {
            self.run(tenant, db, name, body, Some(t)).await
        };
        if r.is_ok() && name == "createIndexes" {
            let ns = Name::new(db, &coll_of(body, name)?);
            if !made
                .iter()
                .any(|m| matches!(m, Created::Coll(x) if *x == ns))
                && let Some(c) = self.coll(tenant, &ns)?
            {
                for i in &c.meta.indexes {
                    if !indexes_before.contains(&i.name) {
                        made.push(Created::Index(ns.clone(), i.name.clone()));
                    }
                }
            }
        }
        if r.is_ok() || name != "create" {
            g.created.extend(made);
            self.txns.set_hidden(tenant, lsid, &g.created);
        }
        if r.is_err() {
            // A failed command ends the transaction.
            if let TxnState::Active(t, guard) = std::mem::replace(&mut g.state, TxnState::Aborted) {
                t.rollback();
                drop(guard);
            }
            let undo = std::mem::take(&mut g.created);
            drop(g);
            self.txns.set_hidden(tenant, lsid, &[]);
            self.undo_created(tenant, undo).await;
        }
        r.map_err(|e| {
            if e.code == code::WRITE_CONFLICT {
                e.label("TransientTransactionError")
            } else {
                e
            }
        })
    }

    /// A write command outside a transaction: one engine transaction,
    /// rerun whole after a write conflict.
    async fn write_command(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        retry: Option<(Vec<u8>, i64)>,
    ) -> Result<Reply, MongoError> {
        let bulk = if name == "bulkWrite" {
            Some(super::bulk::parse(body)?)
        } else {
            None
        };
        let creates = if let Some(ops) = &bulk {
            super::bulk::creates(ops)
        } else {
            let c = coll_of(body, name)?;
            let creates = match name {
                "insert" => true,
                "update" => docs_of(body, "updates")
                    .iter()
                    .any(|u| u.get_bool("upsert") == Ok(true)),
                "findAndModify" | "findandmodify" => body.get_bool("upsert") == Ok(true),
                _ => false,
            };
            if creates {
                vec![Name::new(db, &c)]
            } else {
                Vec::new()
            }
        };
        for ns in creates {
            let _ = self.coll_or_create(tenant, &ns).await?;
        }
        let rk = retry
            .as_ref()
            .map(|(l, n)| super::layout::retry_record(tenant, l, *n));
        for attempt in 0..20u32 {
            let _g = self.write_guard(tenant).await;
            let mut t = self.begin(false);
            if let Some(k) = &rk {
                t.lock_entry(k).await?;
                if let Some(v) = t.get_current(k)? {
                    t.rollback();
                    let mut d = Document::from_reader(&mut &v[..]).map_err(|e| {
                        MongoError::new(code::INTERNAL_ERROR, format!("damaged retry record: {e}"))
                    })?;
                    d.insert("retriedStmtIds", Bson::Array(Vec::new()));
                    d.remove("retriedStmtIds");
                    return Ok(Reply::Doc(d));
                }
            }
            match self
                .write_any(tenant, db, name, body, bulk.as_deref(), &mut t, false)
                .await
            {
                Ok(Reply::Doc(d)) => {
                    if let Some(k) = rk {
                        let mut v = Vec::new();
                        d.to_writer(&mut v)
                            .map_err(|e| MongoError::new(code::INTERNAL_ERROR, e.to_string()))?;
                        t.put(k, v)?;
                    }
                    t.commit().await?;
                    return Ok(Reply::Doc(d));
                }
                Ok(other) => {
                    t.commit().await?;
                    return Ok(other);
                }
                Err(e) if e.code == code::WRITE_CONFLICT && attempt < 19 => {
                    t.rollback();
                    tokio::time::sleep(std::time::Duration::from_millis(u64::from(attempt) * 2))
                        .await;
                }
                Err(e) => {
                    t.rollback();
                    return Err(e);
                }
            }
        }
        Err(MongoError::new(code::WRITE_CONFLICT, "write conflict"))
    }

    /// A write command (`bulkWrite`: its parsed `ops`) in `t`, with
    /// validation bypassed when it asks.
    #[allow(clippy::too_many_arguments)]
    async fn write_any(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        bulk: Option<&[super::bulk::BulkOp]>,
        t: &mut DocTxn,
        multi: bool,
    ) -> Result<Reply, MongoError> {
        let bypass = body.get_bool("bypassDocumentValidation").unwrap_or(false);
        match bulk {
            Some(ops) => {
                crate::validate::bypass(
                    bypass,
                    Box::pin(self.bulk_body(tenant, body, ops, t, multi)),
                )
                .await
            }
            None => {
                crate::validate::bypass(
                    bypass,
                    Box::pin(self.write_body(tenant, db, name, body, t, multi)),
                )
                .await
            }
        }
    }

    /// A write command's statements in `t`. Statement errors become
    /// `writeErrors` (each statement's writes undone alone); a write
    /// conflict fails the whole command.
    pub(super) async fn write_body(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        t: &mut DocTxn,
        multi: bool,
    ) -> Result<Reply, MongoError> {
        let cname = coll_of(body, name)?;
        let ns = Name::new(db, &cname);
        crate::engine::check_coll_name(&cname)?;
        if let Err(e) = self.not_a_view(tenant, db, &cname) {
            // A write statement to a view is a write error, as MongoDB
            // answers it; findAndModify fails the command.
            if matches!(name, "insert" | "update" | "delete") {
                let mut d = doc! {"n": 0};
                if name == "update" {
                    d.insert("nModified", 0);
                }
                d.insert("writeErrors", vec![e.write_error(0)]);
                d.insert("ok", 1.0);
                return Ok(Reply::Doc(d));
            }
            return Err(e);
        }
        let ordered = body.get_bool("ordered").unwrap_or(true);
        let fatal = |e: &MongoError| e.code == code::WRITE_CONFLICT || multi;
        match name {
            "insert" => {
                let c = self.coll_or_create(tenant, &ns).await?;
                let docs = docs_of(body, "documents");
                let mut n = 0i32;
                let mut errs = Vec::new();
                for (i, d) in docs.into_iter().enumerate() {
                    t.begin_statement();
                    match self.insert_doc(t, &c, d).await {
                        Ok(_) => n += 1,
                        Err(e) if fatal(&e) => return Err(e),
                        Err(e) => {
                            t.rollback_statement();
                            errs.push(e.write_error(i));
                            if ordered {
                                break;
                            }
                        }
                    }
                }
                let mut d = doc! {"n": n};
                if !errs.is_empty() {
                    d.insert("writeErrors", errs);
                }
                d.insert("ok", 1.0);
                Ok(Reply::Doc(d))
            }
            "update" => {
                if super::bulk::sorted_multi(body) {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        super::bulk::SORT_WITH_MULTI,
                    ));
                }
                let mut n = 0i64;
                let mut modified = 0i64;
                let mut upserted = Vec::new();
                let mut errs = Vec::new();
                for (i, u) in docs_of(body, "updates").into_iter().enumerate() {
                    t.begin_statement();
                    let r = async {
                        let st = UpdateStmt {
                            q: u.get_document("q").cloned().unwrap_or_default(),
                            u: u.get("u").cloned().ok_or_else(|| {
                                MongoError::new(
                                    code::FAILED_TO_PARSE,
                                    "BSON field 'update.updates.u' is missing but a required field",
                                )
                            })?,
                            multi: u.get_bool("multi").unwrap_or(false),
                            upsert: u.get_bool("upsert").unwrap_or(false),
                            array_filters: docs_of(&u, "arrayFilters"),
                            hint: u.get("hint").cloned().filter(|h| !is_empty_hint(h)),
                            sort: u.get_document("sort").ok().cloned(),
                        };
                        let coll = crate::collation::Collation::of(&u)?;
                        let c = if st.upsert {
                            Some(self.coll_or_create(tenant, &ns).await?)
                        } else {
                            self.coll(tenant, &ns)?
                        };
                        let coll =
                            coll.or_else(|| c.as_ref().and_then(|c| c.meta.collation_for(&u)));
                        crate::collation::scope(coll, self.update(t, c.as_deref(), &st)).await
                    }
                    .await;
                    match r {
                        Ok(r) => {
                            if let Some(id) = r.upserted {
                                n += 1;
                                upserted.push(
                                    doc! {"index": i32::try_from(i).unwrap_or(i32::MAX), "_id": id},
                                );
                            } else {
                                n += i64::try_from(r.n).unwrap_or(i64::MAX);
                                modified += i64::try_from(r.modified).unwrap_or(i64::MAX);
                            }
                        }
                        Err(e) if fatal(&e) => return Err(e),
                        Err(e) => {
                            t.rollback_statement();
                            errs.push(e.write_error(i));
                            if ordered {
                                break;
                            }
                        }
                    }
                }
                let mut d = doc! {"n": int(n), "nModified": int(modified)};
                if !upserted.is_empty() {
                    d.insert("upserted", upserted);
                }
                if !errs.is_empty() {
                    d.insert("writeErrors", errs);
                }
                d.insert("ok", 1.0);
                Ok(Reply::Doc(d))
            }
            "delete" => {
                let mut n = 0i64;
                let mut errs = Vec::new();
                let c = self.coll(tenant, &ns)?;
                for (i, s) in docs_of(body, "deletes").into_iter().enumerate() {
                    t.begin_statement();
                    let r = async {
                        let q = s.get_document("q").cloned().map_err(|_| {
                            MongoError::new(
                                code::FAILED_TO_PARSE,
                                "BSON field 'delete.deletes.q' is missing but a required field",
                            )
                        })?;
                        let limit = match s.get("limit").and_then(crate::filter::as_i64) {
                            Some(0) => false,
                            Some(1) => true,
                            _ => {
                                return Err(MongoError::new(
                                    code::FAILED_TO_PARSE,
                                    "The limit field in delete objects must be 0 or 1. Got -1",
                                ));
                            }
                        };
                        let Some(c) = &c else {
                            crate::filter::parse(&q)?;
                            return Ok(0usize);
                        };
                        let coll = crate::collation::Collation::of(&s)?
                            .or_else(|| c.meta.collation_for(&s));
                        let hint = s.get("hint").filter(|h| !is_empty_hint(h)).cloned();
                        crate::collation::scope(
                            coll,
                            self.delete(t, c, &q, limit, hint.as_ref(), None),
                        )
                        .await
                        .map(|v| v.len())
                    }
                    .await;
                    match r {
                        Ok(k) => n += i64::try_from(k).unwrap_or(i64::MAX),
                        Err(e) if fatal(&e) => return Err(e),
                        Err(e) => {
                            t.rollback_statement();
                            errs.push(e.write_error(i));
                            if ordered {
                                break;
                            }
                        }
                    }
                }
                let mut d = doc! {"n": int(n)};
                if !errs.is_empty() {
                    d.insert("writeErrors", errs);
                }
                d.insert("ok", 1.0);
                Ok(Reply::Doc(d))
            }
            _ => {
                // A command naming no collation uses the collection's.
                let dc = self
                    .coll(tenant, &ns)?
                    .and_then(|c| c.meta.collation_for(body));
                crate::collation::scope(dc, Box::pin(self.find_and_modify(tenant, &ns, body, t)))
                    .await
                    .map(Reply::Doc)
            }
        }
    }

    async fn find_and_modify(
        &self,
        tenant: u64,
        ns: &Name,
        body: &Document,
        t: &mut DocTxn,
    ) -> Result<Document, MongoError> {
        let q = body.get_document("query").cloned().unwrap_or_default();
        let remove = body.get_bool("remove").unwrap_or(false);
        let upd = body.get("update").cloned();
        let new = body.get_bool("new").unwrap_or(false);
        let upsert = body.get_bool("upsert").unwrap_or(false);
        let fields = body.get_document("fields").ok().filter(|f| !f.is_empty());
        if remove && upd.is_some() {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "Cannot specify both an update and remove=true",
            ));
        }
        if !remove && upd.is_none() {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "Either an update or remove=true must be specified",
            ));
        }
        if remove && (upsert || new) {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                if upsert {
                    "Cannot specify both upsert=true and remove=true "
                } else {
                    "Cannot specify both new=true and remove=true; 'remove' always returns the deleted document"
                },
            ));
        }
        let project = |d: Option<Document>| -> Result<Bson, MongoError> {
            let Some(d) = d else { return Ok(Bson::Null) };
            Ok(Bson::Document(match fields {
                Some(f) => crate::agg::Projection::parse(f)?.apply(&d, self.now())?,
                None => d,
            }))
        };
        let hint = body.get("hint").filter(|h| !is_empty_hint(h)).cloned();
        let sort = body.get_document("sort").ok().cloned();
        if remove {
            let Some(c) = self.coll(tenant, ns)? else {
                crate::filter::parse(&q)?;
                return Ok(doc! {"lastErrorObject": {"n": 0}, "value": Bson::Null, "ok": 1.0});
            };
            let gone = self
                .delete(t, &c, &q, true, hint.as_ref(), sort.as_ref())
                .await?;
            let n = i32::try_from(gone.len()).unwrap_or(1);
            let v = project(gone.into_iter().next())?;
            return Ok(doc! {"lastErrorObject": {"n": n}, "value": v, "ok": 1.0});
        }
        let st = UpdateStmt {
            q,
            u: upd.unwrap_or(Bson::Null),
            multi: false,
            upsert,
            array_filters: docs_of(body, "arrayFilters"),
            hint,
            sort,
        };
        let c = if upsert {
            Some(self.coll_or_create(tenant, ns).await?)
        } else {
            self.coll(tenant, ns)?
        };
        let r = self.update(t, c.as_deref(), &st).await?;
        let mut leo = doc! {};
        if let Some(id) = &r.upserted {
            leo.insert("n", 1);
            leo.insert("updatedExisting", false);
            leo.insert("upserted", id.clone());
        } else {
            leo.insert("n", i32::try_from(r.n).unwrap_or(1));
            leo.insert("updatedExisting", r.n > 0);
        }
        let v = if new {
            project(r.new)?
        } else if r.upserted.is_some() {
            Bson::Null
        } else {
            project(r.old)?
        };
        Ok(doc! {"lastErrorObject": leo, "value": v, "ok": 1.0})
    }

    /// The view `db.coll` resolves to: its base collection and the
    /// pipelines of every view on the way, in order (`None`: not a view).
    fn resolve_view(
        &self,
        tenant: u64,
        db: &str,
        coll: &str,
    ) -> Result<Option<(String, Vec<Document>)>, MongoError> {
        let mut name = coll.to_owned();
        let mut pipes: Vec<Vec<Document>> = Vec::new();
        for _ in 0..20 {
            match self.coll(tenant, &Name::new(db, &name))? {
                Some(c) => match c.meta.view() {
                    Some((on, p)) => {
                        pipes.push(p);
                        name = on;
                    }
                    None => break,
                },
                None => break,
            }
        }
        if pipes.is_empty() {
            return Ok(None);
        }
        let mut all = Vec::new();
        for p in pipes.into_iter().rev() {
            all.extend(p);
        }
        Ok(Some((name, all)))
    }

    /// A read of a view: the same read as an aggregation over its base
    /// collection (`None`: `coll` isn't a view).
    async fn view_read(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        txn: Option<&mut DocTxn>,
    ) -> Result<Option<Reply>, MongoError> {
        let Some(Bson::String(coll)) = body.get(name) else {
            return Ok(None);
        };
        let Some((base, mut p)) = self.resolve_view(tenant, db, coll)? else {
            return Ok(None);
        };
        let ns = format!("{db}.{coll}");
        let filter = |k: &str| body.get_document(k).ok().filter(|f| !f.is_empty()).cloned();
        match name {
            "find" => {
                if let Some(f) = filter("filter") {
                    p.push(doc! {"$match": f});
                }
                if let Some(s) = filter("sort") {
                    p.push(doc! {"$sort": s});
                }
                if let Some(n) = i64_of(body, "skip").filter(|n| *n > 0) {
                    p.push(doc! {"$skip": n});
                }
                if let Some(n) = i64_of(body, "limit").filter(|n| *n != 0) {
                    p.push(doc! {"$limit": n.abs()});
                }
                if let Some(pr) = filter("projection") {
                    p.push(doc! {"$project": pr});
                }
                let docs = self.aggregate(tenant, db, &base, &p, txn).await?;
                let batch = batch_of(body, "find")?.unwrap_or(101);
                self.buffered(tenant, db, &base, ns, docs, batch).map(Some)
            }
            "aggregate" => {
                p.extend(docs_of(body, "pipeline"));
                let docs = self.aggregate(tenant, db, &base, &p, txn).await?;
                let batch = batch_of(body, "aggregate")?.unwrap_or(101);
                self.buffered(tenant, db, &base, ns, docs, batch).map(Some)
            }
            "count" => {
                if let Some(f) = filter("query") {
                    p.push(doc! {"$match": f});
                }
                if let Some(n) = i64_of(body, "skip").filter(|n| *n > 0) {
                    p.push(doc! {"$skip": n});
                }
                if let Some(n) = i64_of(body, "limit").filter(|n| *n != 0) {
                    p.push(doc! {"$limit": n.abs()});
                }
                p.push(doc! {"$count": "n"});
                let docs = self.aggregate(tenant, db, &base, &p, txn).await?;
                let n = docs
                    .first()
                    .and_then(|d| d.get("n"))
                    .and_then(crate::filter::as_i64)
                    .unwrap_or(0);
                Ok(Some(Reply::Doc(doc! {"n": int(n), "ok": 1.0})))
            }
            "distinct" => {
                if let Some(f) = filter("query") {
                    p.push(doc! {"$match": f});
                }
                let key = body.get_str("key").unwrap_or_default();
                let docs = self.aggregate(tenant, db, &base, &p, txn).await?;
                let v = super::pipeline::distinct_values(&docs, key);
                Ok(Some(Reply::Doc(doc! {"values": v, "ok": 1.0})))
            }
            _ => Ok(None),
        }
    }

    /// 166 when `db.coll` is a view.
    fn not_a_view(&self, tenant: u64, db: &str, coll: &str) -> Result<(), MongoError> {
        if self
            .coll(tenant, &Name::new(db, coll))?
            .is_some_and(|c| c.meta.view().is_some())
        {
            return Err(MongoError::new(
                166,
                format!("Namespace {db}.{coll} is a view, not a collection"),
            ));
        }
        Ok(())
    }

    /// Commands that read or change the catalog, and reads. A read that
    /// names no collation uses its collection's default one.
    async fn run(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        txn: Option<&mut DocTxn>,
    ) -> Result<Reply, MongoError> {
        if matches!(name, "find" | "aggregate" | "count" | "distinct")
            && let Some(Bson::String(cname)) = body.get(name)
            && let Some(c) = self.coll(tenant, &Name::new(db, cname))?
            && let Some(dc) = c.meta.collation_for(body)
        {
            return crate::collation::scope(
                Some(dc),
                Box::pin(self.run_cmd(tenant, db, name, body, txn)),
            )
            .await;
        }
        Box::pin(self.run_cmd(tenant, db, name, body, txn)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn run_cmd(
        &self,
        tenant: u64,
        db: &str,
        name: &str,
        body: &Document,
        mut txn: Option<&mut DocTxn>,
    ) -> Result<Reply, MongoError> {
        if matches!(name, "find" | "aggregate" | "count" | "distinct")
            && let Some(r) = self
                .view_read(tenant, db, name, body, txn.as_deref_mut())
                .await?
        {
            return Ok(r);
        }
        if matches!(
            name,
            "createIndexes" | "dropIndexes" | "deleteIndexes" | "listIndexes" | "collMod"
        ) && let Some(Bson::String(c)) = body.get(name)
        {
            self.not_a_view(tenant, db, c)?;
        }
        match name {
            "find" => {
                let cname = coll_of(body, "find")?;
                let ns = Name::new(db, &cname);
                let limit = i64_of(body, "limit").unwrap_or(0);
                let single = body.get_bool("singleBatch").unwrap_or(false) || limit < 0;
                let skip = i64_of(body, "skip").unwrap_or(0);
                if skip < 0 {
                    return Err(MongoError::new(
                        51024,
                        format!("BSON field 'skip' value must be >= 0, actual value '{skip}'"),
                    ));
                }
                let args = FindArgs {
                    filter: body.get_document("filter").cloned().unwrap_or_default(),
                    sort: body
                        .get_document("sort")
                        .ok()
                        .cloned()
                        .filter(|s| !s.is_empty()),
                    projection: body.get_document("projection").ok().cloned(),
                    skip: skip.cast_unsigned(),
                    limit: limit.unsigned_abs(),
                    hint: body.get("hint").cloned().filter(|h| !is_empty_hint(h)),
                    allow_disk_use: body.get_bool("allowDiskUse").unwrap_or(false),
                };
                let batch = batch_of(body, "find")?.unwrap_or(101);
                let Some(c) = self.coll(tenant, &ns)? else {
                    crate::filter::parse(&args.filter)?;
                    return Ok(Reply::Cursor {
                        ns: ns.to_string(),
                        first: Vec::new(),
                        rest: None,
                    });
                };
                let mut cur = self.find(tenant, &c, &args, txn).await?;
                let first = cur.next_batch(self, batch).await?;
                let rest = (!single && !cur.exhausted()).then_some(cur);
                Ok(Reply::Cursor {
                    ns: ns.to_string(),
                    first,
                    rest,
                })
            }
            "aggregate" => {
                let coll = match body.get("aggregate") {
                    Some(Bson::String(s)) => s.clone(),
                    Some(v) if crate::filter::as_i64(v) == Some(1) => String::new(),
                    _ => {
                        return Err(MongoError::new(
                            code::FAILED_TO_PARSE,
                            "aggregate field must be a collection name or 1",
                        ));
                    }
                };
                let pipeline = docs_of(body, "pipeline");
                if body.get("cursor").is_none() && body.get_bool("explain") != Ok(true) {
                    return Err(MongoError::new(
                        code::FAILED_TO_PARSE,
                        "The 'cursor' option is required, except for aggregate with the explain argument",
                    ));
                }
                let batch = batch_of(body, "aggregate")?.unwrap_or(101);
                // `hint` picks the index of the pipeline's first read.
                let hint = body.get("hint").filter(|h| !is_empty_hint(h)).cloned();
                let docs = super::pipeline::with_hint(
                    hint,
                    Box::pin(self.aggregate(tenant, db, &coll, &pipeline, txn)),
                )
                .await?;
                let ns = if coll.is_empty() {
                    format!("{db}.$cmd.aggregate")
                } else {
                    format!("{db}.{coll}")
                };
                self.buffered(tenant, db, &coll, ns, docs, batch)
            }
            "count" => {
                let cname = coll_of(body, "count")?;
                let q = body.get_document("query").cloned().unwrap_or_default();
                let Some(c) = self.coll(tenant, &Name::new(db, &cname))? else {
                    crate::filter::parse(&q)?;
                    return Ok(Reply::Doc(doc! {"n": 0, "ok": 1.0}));
                };
                let skip = i64_of(body, "skip").unwrap_or(0).max(0).cast_unsigned();
                let limit = i64_of(body, "limit").unwrap_or(0).unsigned_abs();
                let hint = body.get("hint").filter(|h| !is_empty_hint(h)).cloned();
                let n = self.count(&c, &q, skip, limit, hint.as_ref(), txn).await?;
                Ok(Reply::Doc(
                    doc! {"n": int(i64::try_from(n).unwrap_or(i64::MAX)), "ok": 1.0},
                ))
            }
            "distinct" => {
                let cname = coll_of(body, "distinct")?;
                let key = body.get_str("key").map_err(|_| {
                    MongoError::new(
                        code::FAILED_TO_PARSE,
                        "BSON field 'distinct.key' is missing but a required field",
                    )
                })?;
                let q = body.get_document("query").cloned().unwrap_or_default();
                let Some(c) = self.coll(tenant, &Name::new(db, &cname))? else {
                    crate::filter::parse(&q)?;
                    return Ok(Reply::Doc(doc! {"values": [], "ok": 1.0}));
                };
                let v = self.distinct(&c, key, &q, txn).await?;
                Ok(Reply::Doc(doc! {"values": v, "ok": 1.0}))
            }
            "create" => {
                let cname = coll_of(body, "create")?;
                let mut opts = Document::new();
                for (k, v) in body {
                    if matches!(
                        k.as_str(),
                        "validator"
                            | "validationLevel"
                            | "validationAction"
                            | "viewOn"
                            | "pipeline"
                    ) {
                        opts.insert(k.clone(), v.clone());
                    } else if k == "changeStreamPreAndPostImages" {
                        super::check_pre_post(v)?;
                        opts.insert(k.clone(), v.clone());
                    } else if k == "collation" && !body.contains_key("viewOn") {
                        // Kept with every field, as `listCollections` shows
                        // it; the simple collation is no option at all.
                        let Bson::Document(c) = v else {
                            return Err(MongoError::new(
                                code::TYPE_MISMATCH,
                                "BSON field 'create.collation' is the wrong type, expected type 'object'",
                            ));
                        };
                        if let Some(n) = crate::collation::Collation::normalize(c)? {
                            opts.insert("collation", n);
                        }
                    } else if matches!(
                        k.as_str(),
                        "capped" | "size" | "max" | "timeseries" | "clusteredIndex" | "collation"
                    ) {
                        return Err(MongoError::new(
                            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                            format!(
                                "the create option '{k}' is not supported on collections stored on the SprigDB engine yet"
                            ),
                        ));
                    }
                }
                if opts.contains_key("viewOn") || opts.contains_key("pipeline") {
                    check_view(&opts)?;
                } else {
                    crate::validate::check_create(&opts)?;
                }
                // Inside a transaction (which holds the writers' side of
                // the lock) a new collection is created at once, outside
                // it, as a write creates one.
                let _g = if txn.is_none() {
                    Some(self.ddl_guard(tenant).await)
                } else {
                    None
                };
                let ns = Name::new(db, &cname);
                match self.create_locked(tenant, &ns, opts.clone()).await? {
                    Some(_) => Ok(Reply::Doc(doc! {"ok": 1.0})),
                    None => {
                        let c = self.coll(tenant, &ns)?;
                        if c.is_some_and(|c| c.meta.options == opts) {
                            Ok(Reply::Doc(doc! {"ok": 1.0}))
                        } else {
                            Err(MongoError::new(
                                code::NAMESPACE_EXISTS,
                                format!("Collection {ns} already exists. NS: {ns}"),
                            ))
                        }
                    }
                }
            }
            "drop" => {
                let cname = coll_of(body, "drop")?;
                let ns = Name::new(db, &cname);
                let _g = self.ddl_guard(tenant).await;
                let n = self
                    .coll(tenant, &ns)?
                    .map(|c| i32::try_from(c.meta.indexes.len() + 1).unwrap_or(0));
                self.drop_locked(tenant, &ns).await?;
                Ok(Reply::Doc(match n {
                    Some(n) => doc! {"nIndexesWas": n, "ns": ns.to_string(), "ok": 1.0},
                    None => doc! {"ok": 1.0},
                }))
            }
            "dropDatabase" => {
                let _g = self.ddl_guard(tenant).await;
                let colls = self.list(tenant, Some(db))?;
                for c in &colls {
                    self.drop_locked(tenant, &Name::new(db, &c.meta.name))
                        .await?;
                }
                if !colls.is_empty() {
                    // The change log's `dropDatabase` event.
                    let mut t = self.begin(false);
                    t.delete(super::layout::db_drop_marker(tenant, db))?;
                    t.commit().await?;
                }
                Ok(Reply::Doc(doc! {"dropped": db, "ok": 1.0}))
            }
            "listCollections" => {
                let f = body.get_document("filter").cloned().unwrap_or_default();
                let pred = crate::filter::parse(&f)?;
                let names_only = body.get_bool("nameOnly").unwrap_or(false);
                // Collections an open transaction created are seen by
                // others only after its commit.
                let (hidden, _) = self.txns.hidden(tenant, db, None);
                let mut out = Vec::new();
                for c in self.list(tenant, Some(db))? {
                    if hidden.contains(&c.meta.name) {
                        continue;
                    }
                    let view = c.meta.view().is_some();
                    let d = if names_only {
                        doc! {"name": c.meta.name.clone(), "type": if view { "view" } else { "collection" }}
                    } else if view {
                        doc! {
                            "name": c.meta.name.clone(),
                            "type": "view",
                            "options": c.meta.options.clone(),
                            "info": {"readOnly": true},
                        }
                    } else {
                        doc! {
                            "name": c.meta.name.clone(),
                            "type": "collection",
                            "options": c.meta.options.clone(),
                            "info": {"readOnly": false, "uuid": c.meta.uuid_bson()},
                            "idIndex": c.meta.id_index_spec(),
                        }
                    };
                    if pred.matches(&d) {
                        out.push(d);
                    }
                }
                let batch = batch_of(body, "listCollections")?.unwrap_or(usize::MAX);
                self.buffered(
                    tenant,
                    db,
                    "",
                    format!("{db}.$cmd.listCollections"),
                    out,
                    batch,
                )
            }
            "listIndexes" => {
                let cname = coll_of(body, "listIndexes")?;
                let ns = Name::new(db, &cname);
                let (hidden_colls, hidden) = self.txns.hidden(tenant, db, Some(&ns));
                let Some(c) = self
                    .coll(tenant, &ns)?
                    .filter(|_| !hidden_colls.contains(&cname))
                else {
                    return Err(MongoError::new(
                        code::NAMESPACE_NOT_FOUND,
                        format!("ns does not exist: {ns}"),
                    ));
                };
                let batch = batch_of(body, "listIndexes")?.unwrap_or(usize::MAX);
                let specs = c
                    .meta
                    .index_specs()
                    .into_iter()
                    .filter(|s| {
                        s.get_str("name")
                            .is_ok_and(|n| !hidden.iter().any(|h| h == n))
                    })
                    .collect();
                self.buffered(tenant, db, &cname, ns.to_string(), specs, batch)
            }
            "createIndexes" => {
                let cname = coll_of(body, "createIndexes")?;
                let specs = match body.get("indexes") {
                    Some(Bson::Array(a)) => a
                        .iter()
                        .map(|x| {
                            x.as_document().cloned().ok_or_else(|| {
                                MongoError::new(
                                    code::TYPE_MISMATCH,
                                    "The 'indexes' field must be an array of objects",
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => {
                        return Err(MongoError::new(
                            code::FAILED_TO_PARSE,
                            "BSON field 'createIndexes.indexes' is missing but a required field",
                        ));
                    }
                };
                if specs.is_empty() {
                    return Err(MongoError::new(
                        code::BAD_VALUE,
                        "Must specify at least one index to create",
                    ));
                }
                // Built online (no DDL lock held while documents are read);
                // inside a transaction, only on a new or empty collection.
                let (before, after, created) = if txn.is_some() {
                    Box::pin(self.create_indexes_in_txn(tenant, &Name::new(db, &cname), &specs))
                        .await?
                } else {
                    Box::pin(self.create_indexes(tenant, &Name::new(db, &cname), &specs)).await?
                };
                let mut d = doc! {
                    "numIndexesBefore": i32::try_from(before).unwrap_or(0),
                    "numIndexesAfter": i32::try_from(after).unwrap_or(0),
                };
                if before == after {
                    d.insert("note", "all indexes already exist");
                }
                d.insert("createdCollectionAutomatically", created);
                d.insert("ok", 1.0);
                Ok(Reply::Doc(d))
            }
            "collStats" => {
                let cname = coll_of(body, "collStats")?;
                self.coll_stats_command(tenant, db, &cname, body)
                    .map(Reply::Doc)
            }
            "dbStats" => self.db_stats(tenant, db, body).map(Reply::Doc),
            "dropIndexes" | "deleteIndexes" => {
                let cname = coll_of(body, name)?;
                let which = match body.get("index") {
                    Some(Bson::Array(a)) => a.clone(),
                    Some(v) => vec![v.clone()],
                    None => {
                        return Err(MongoError::new(
                            code::FAILED_TO_PARSE,
                            "BSON field 'dropIndexes.index' is missing but a required field",
                        ));
                    }
                };
                let _g = self.ddl_guard(tenant).await;
                let n = self
                    .drop_indexes_locked(tenant, &Name::new(db, &cname), &which)
                    .await?;
                Ok(Reply::Doc(
                    doc! {"nIndexesWas": i32::try_from(n).unwrap_or(0), "ok": 1.0},
                ))
            }
            "collMod" => {
                let cname = coll_of(body, "collMod")?;
                let _g = self.ddl_guard(tenant).await;
                let mut d = self
                    .coll_mod_locked(tenant, &Name::new(db, &cname), body)
                    .await?;
                d.insert("ok", 1.0);
                Ok(Reply::Doc(d))
            }
            "renameCollection" => {
                let split = |s: &str| -> Result<Name, MongoError> {
                    s.split_once('.')
                        .map(|(d, c)| Name::new(d, c))
                        .ok_or_else(|| {
                            MongoError::new(
                                code::INVALID_NAMESPACE,
                                format!("Invalid namespace specified '{s}'"),
                            )
                        })
                };
                let from = split(body.get_str("renameCollection").unwrap_or_default())?;
                let to = split(body.get_str("to").unwrap_or_default())?;
                let drop_target = body.get_bool("dropTarget").unwrap_or(false);
                let _g = self.ddl_guard(tenant).await;
                self.rename_locked(tenant, &from, &to, drop_target).await?;
                Ok(Reply::Doc(doc! {"ok": 1.0}))
            }
            "listDatabases" => {
                let mut dbs: Vec<String> = self
                    .list(tenant, None)?
                    .iter()
                    .map(|c| c.meta.db.clone())
                    .collect();
                dbs.dedup();
                let names_only = body.get_bool("nameOnly").unwrap_or(false);
                let list: Vec<Document> = dbs
                    .iter()
                    .map(|d| {
                        if names_only {
                            doc! {"name": d}
                        } else {
                            doc! {"name": d, "sizeOnDisk": 0i64, "empty": false}
                        }
                    })
                    .collect();
                let mut r = doc! {"databases": list};
                if !names_only {
                    r.insert("totalSize", 0i64);
                    r.insert("totalSizeMb", 0i64);
                }
                r.insert("ok", 1.0);
                Ok(Reply::Doc(r))
            }
            "explain" => {
                let inner = body.get_document("explain").map_err(|_| {
                    MongoError::new(code::FAILED_TO_PARSE, "explain must be an object")
                })?;
                let iname = inner.keys().next().cloned().unwrap_or_default();
                let cname = inner.get_str(&iname).unwrap_or_default().to_owned();
                let ns = Name::new(db, &cname);
                let filter = inner
                    .get_document(if iname == "find" { "filter" } else { "query" })
                    .cloned()
                    .unwrap_or_default();
                let stage = match self.coll(tenant, &ns)? {
                    None => "EOF".to_owned(),
                    Some(c) => {
                        let pred = crate::filter::parse(&filter)?;
                        let want = super::read::Want {
                            sort: inner
                                .get_document("sort")
                                .ok()
                                .map(crate::agg::parse_sort)
                                .transpose()?
                                .unwrap_or_default(),
                            hint: inner.get("hint"),
                            natural: None,
                        };
                        let coll = crate::collation::Collation::of(inner)?
                            .or_else(|| c.meta.collation_for(inner));
                        let p = crate::collation::sync_scope(coll, || {
                            super::read::plan(&c, &pred, &want)
                        })?;
                        match p.index {
                            Some(i) => format!("IXSCAN:{i}"),
                            None => p.stage().to_owned(),
                        }
                    }
                };
                let (stage, index) = match stage.split_once(':') {
                    Some((s, i)) => (s.to_owned(), Some(i.to_owned())),
                    None => (stage, None),
                };
                let mut win = doc! {"stage": stage};
                if let Some(i) = index {
                    win =
                        doc! {"stage": "FETCH", "inputStage": {"stage": "IXSCAN", "indexName": i}};
                }
                Ok(Reply::Doc(doc! {
                    "queryPlanner": {"namespace": ns.to_string(), "parsedQuery": filter, "winningPlan": win, "rejectedPlans": []},
                    "ok": 1.0,
                }))
            }
            "killCursors" | "getMore" => Err(MongoError::new(
                code::INTERNAL_ERROR,
                "cursor commands are answered by the cursor table",
            )),
            other => Err(MongoError::new(
                code::COMMAND_NOT_FOUND,
                format!("no such command: '{other}'"),
            )),
        }
    }

    fn buffered(
        &self,
        tenant: u64,
        db: &str,
        coll: &str,
        ns: String,
        docs: Vec<Document>,
        batch: usize,
    ) -> Result<Reply, MongoError> {
        let mut docs = std::collections::VecDeque::from(docs);
        let mut first = Vec::new();
        let mut bytes = 0usize;
        while first.len() < batch {
            let Some(d) = docs.pop_front() else { break };
            bytes += crate::spill::doc_bytes(&d);
            first.push(d);
            if bytes > 16 * 1024 * 1024 {
                break;
            }
        }
        let rest = if docs.is_empty() {
            None
        } else {
            let c = self.coll(tenant, &Name::new(db, coll))?.unwrap_or_else(|| {
                Arc::new(super::Coll::new(
                    super::catalog::CollMeta::new(db, coll, 0, Document::new()),
                    tenant,
                    &[],
                ))
            });
            Some(StoreCursor::buffer(tenant, c, docs.into()))
        };
        Ok(Reply::Cursor { ns, first, rest })
    }
}

/// A view's options: `viewOn` a name, `pipeline` stages without `$out`
/// or `$merge`.
fn check_view(opts: &Document) -> Result<(), MongoError> {
    match opts.get("viewOn") {
        Some(Bson::String(s)) if !s.is_empty() => {}
        Some(Bson::String(_)) => {
            return Err(MongoError::new(code::BAD_VALUE, "'viewOn' cannot be empty"));
        }
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'create.viewOn' is the wrong type, expected type 'string'",
            ));
        }
        None => {
            return Err(MongoError::new(
                code::BAD_VALUE,
                "'pipeline' requires 'viewOn' to also be specified",
            ));
        }
    }
    if let Some(p) = opts.get("pipeline") {
        let Bson::Array(stages) = p else {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "BSON field 'create.pipeline' is the wrong type, expected type 'array'",
            ));
        };
        for s in stages {
            let Some(d) = s.as_document() else {
                return Err(MongoError::new(
                    code::TYPE_MISMATCH,
                    "Each element of the 'pipeline' array must be an object",
                ));
            };
            if d.contains_key("$out") || d.contains_key("$merge") {
                return Err(MongoError::new(
                    167,
                    "$out and $merge cannot be used in a view definition",
                ));
            }
        }
    }
    Ok(())
}

/// `hint: {}` or `hint: ""` means none.
fn is_empty_hint(h: &Bson) -> bool {
    match h {
        Bson::Document(d) => d.is_empty(),
        Bson::String(s) => s.is_empty(),
        _ => false,
    }
}

/// A count as MongoDB answers it: int32 when it fits.
fn int(n: i64) -> Bson {
    i32::try_from(n).map_or(Bson::Int64(n), Bson::Int32)
}

/// A multi-document transaction held for a command the store doesn't run
/// itself (an SQL table read or written from the MongoDB port), from
/// [`DocStore::txn_loan`]: its engine transaction can be lent out and
/// given back ([`DocTxn::swap_engine_txn`]). Nothing else uses the
/// transaction while the loan is held.
#[derive(Debug)]
pub struct TxnLoan {
    slot: tokio::sync::OwnedMutexGuard<TxnSlot>,
    tenant: u64,
}

impl TxnLoan {
    /// The transaction's document side.
    pub fn txn(&mut self) -> Option<&mut DocTxn> {
        match &mut self.slot.state {
            TxnState::Active(t, _) => Some(t.as_mut()),
            _ => None,
        }
    }
}

impl DocStore {
    /// The open multi-document transaction `n` of session `lsid` of `user`
    /// (`body`'s `lsid`, `txnNumber` and `startTransaction`; one is started
    /// when `startTransaction` says so), held for a command the store
    /// doesn't run itself; `None` when `body` isn't in a transaction.
    /// A command that fails ends the transaction ([`DocStore::loan_failed`]).
    ///
    /// # Errors
    /// No such transaction (251, with `TransientTransactionError`), an old
    /// `txnNumber`, a session of another user.
    pub async fn txn_loan(
        &self,
        tenant: u64,
        user: &str,
        body: &Document,
    ) -> Result<Option<TxnLoan>, MongoError> {
        if body.get_bool("autocommit") != Ok(false) {
            return Ok(None);
        }
        let (Some(lsid), Some(n)) = (lsid_of(body), i64_of(body, "txnNumber")) else {
            return Err(MongoError::new(
                code::INVALID_OPTIONS,
                "autocommit: false needs lsid and txnNumber",
            ));
        };
        let start = body.get_bool("startTransaction") == Ok(true);
        // As the store's own commands open or continue a transaction.
        let slot = match self.txns.get(tenant, &lsid) {
            Some(s) => {
                let mut g = s.lock().await;
                if g.owner != user {
                    return Err(no_txn(n));
                }
                if start {
                    if n <= g.txn_number {
                        return Err(MongoError::new(
                            code::TRANSACTION_TOO_OLD,
                            format!(
                                "txnNumber {n} is less than or equal to the last txnNumber {} seen in session",
                                g.txn_number
                            ),
                        ));
                    }
                    if let TxnState::Active(t, _) =
                        std::mem::replace(&mut g.state, TxnState::Aborted)
                    {
                        t.rollback();
                    }
                    let guard = self.write_guard(tenant).await;
                    g.state = TxnState::Active(Box::new(self.begin(true)), guard);
                    g.created.clear();
                    g.txn_number = n;
                    g.started = std::time::Instant::now();
                } else if g.txn_number != n || !matches!(g.state, TxnState::Active(..)) {
                    return Err(no_txn(n));
                }
                drop(g);
                s
            }
            None if start => {
                let guard = self.write_guard(tenant).await;
                self.txns.put(
                    tenant,
                    &lsid,
                    TxnSlot {
                        owner: user.to_owned(),
                        txn_number: n,
                        state: TxnState::Active(Box::new(self.begin(true)), guard),
                        started: std::time::Instant::now(),
                        created: Vec::new(),
                    },
                )
            }
            None => return Err(no_txn(n)),
        };
        let g = slot.lock_owned().await;
        if !matches!(g.state, TxnState::Active(..)) {
            return Err(no_txn(n));
        }
        Ok(Some(TxnLoan { slot: g, tenant }))
    }

    /// The command holding `loan` failed: the transaction ends (rolled
    /// back, what it created dropped), as after a failed store command.
    pub async fn loan_failed(&self, mut loan: TxnLoan) {
        if let TxnState::Active(t, guard) =
            std::mem::replace(&mut loan.slot.state, TxnState::Aborted)
        {
            t.rollback();
            drop(guard);
        }
        let undo = std::mem::take(&mut loan.slot.created);
        let tenant = loan.tenant;
        drop(loan);
        self.undo_created(tenant, undo).await;
    }
}
