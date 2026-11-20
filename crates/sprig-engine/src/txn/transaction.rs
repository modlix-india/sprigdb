//! A transaction: snapshot reads, locking (current) reads, buffered writes,
//! statement and savepoint rollback, and commit with a durability ticket.

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use super::durable::DurableTicket;
use super::errors;
use super::hlc::Ts;
use super::mvcc::{self, VersionScan};
use super::write_set::{Mark, WriteSet};
use super::{CommitPath, Dir, Isolation, Persona, ReadMode, TxnOptions, TxnShared};
use crate::Engine;
use crate::keys::KeyRange;
use crate::locks::{Acquire, LockMode, LockWait, TxnId, Wait, WaitOutcome};
use crate::log::Ticket;
use crate::store::{Read, SortedStore};

/// A locking read or write request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockRequest {
    /// S or X.
    pub mode: LockMode,
    /// The wait policy.
    pub wait: Wait,
    /// Apply MariaDB's snapshot-isolation check (1020). True for locking
    /// accesses to primary-key rows; false for secondary entries and blind
    /// single-command writes.
    pub check_snapshot: bool,
}

impl LockRequest {
    /// `FOR UPDATE` / DML on a primary-key row.
    #[must_use]
    pub const fn for_update() -> Self {
        Self {
            mode: LockMode::Exclusive,
            wait: Wait::Block,
            check_snapshot: true,
        }
    }

    /// `FOR SHARE` on a primary-key row.
    #[must_use]
    pub const fn for_share() -> Self {
        Self {
            mode: LockMode::Shared,
            wait: Wait::Block,
            check_snapshot: true,
        }
    }

    /// An exclusive lock for a write that reads nothing (Redis `SET`,
    /// secondary index entries).
    #[must_use]
    pub const fn write() -> Self {
        Self {
            mode: LockMode::Exclusive,
            wait: Wait::Block,
            check_snapshot: false,
        }
    }

    /// The same request with another wait policy.
    #[must_use]
    pub const fn with_wait(mut self, wait: Wait) -> Self {
        self.wait = wait;
        self
    }

    /// The request a [`ReadMode`] stands for; `None` for a plain read.
    #[must_use]
    pub const fn for_mode(mode: ReadMode) -> Option<Self> {
        match mode {
            ReadMode::Snapshot => None,
            ReadMode::ForShare => Some(Self::for_share()),
            ReadMode::ForUpdate => Some(Self::for_update()),
            ReadMode::NoWait => Some(Self::for_update().with_wait(Wait::NoWait)),
        }
    }
}

/// The result of a locking read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locked {
    /// The row's latest committed value (or this transaction's own), now
    /// locked. `None`: the key is absent.
    Row(Option<Bytes>),
    /// `SKIP LOCKED` skipped it.
    Skipped,
}

/// Why core ended a transaction on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// Deadlock victim (1213).
    Deadlock,
    /// Lock wait timeout with `innodb_rollback_on_timeout` (1205).
    Timeout {
        /// Always true here: without the setting a timeout ends only the
        /// statement.
        rollback_on_timeout: bool,
    },
    /// Snapshot-isolation conflict (1020).
    CheckRead,
    /// Lock memory exhausted (1206).
    LockTableFull,
    /// The log refused the commit or failed; reported as 1213.
    LeaderChange,
}

/// What the protocol layer needs for OK-packet status flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnState {
    /// Still open.
    pub is_open: bool,
    /// Has buffered writes.
    pub has_writes: bool,
    /// Declared read-only.
    pub read_only: bool,
    /// Set when core rolled the transaction back by itself.
    pub aborted_by: Option<AbortReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Active,
    Aborted(AbortReason),
    Done,
}

/// A savepoint (`SAVEPOINT s`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Savepoint(Mark);

/// A queued commit: answer the client only once it is durable.
#[derive(Debug, Clone)]
pub struct CommitTicket {
    cts: Option<Ts>,
    token: Ts,
    ticket: DurableTicket,
}

impl CommitTicket {
    /// The commit timestamp, or, for a transaction that wrote nothing, the
    /// timestamp of everything it could see (a causal token either way).
    #[must_use]
    pub fn cts(&self) -> Ts {
        self.cts.unwrap_or(self.token)
    }

    /// The commit timestamp, if the transaction wrote.
    #[must_use]
    pub fn commit_ts(&self) -> Option<Ts> {
        self.cts
    }

    /// The commit log's ticket: the transaction's own entry, or the entry
    /// of the newest pending version it read.
    #[must_use]
    pub fn log_ticket(&self) -> Option<&Ticket> {
        match &self.ticket {
            DurableTicket::Log(t) => Some(t),
            _ => None,
        }
    }

    /// What the reply waits for.
    #[must_use]
    pub fn durable_ticket(&self) -> &DurableTicket {
        &self.ticket
    }

    /// Whether the client may be answered now.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.ticket.is_durable()
    }

    /// Blocks until the client may be answered.
    ///
    /// # Errors
    /// The log failed first (1213): the commit is lost.
    pub fn wait(&self) -> Result<()> {
        self.ticket.wait()
    }

    /// The async form of [`CommitTicket::wait`].
    ///
    /// # Errors
    /// As for [`CommitTicket::wait`].
    pub async fn wait_async(&self) -> Result<()> {
        self.ticket.wait_async().await
    }
}

enum Step {
    Granted,
    Skipped,
    Wait(LockWait, Option<Duration>),
}

/// One transaction on one unit.
pub struct Txn {
    path: Arc<dyn CommitPath>,
    id: TxnId,
    opts: TxnOptions,
    writes: WriteSet,
    weight: Arc<AtomicUsize>,
    /// The open read view, if any.
    view: Option<Ts>,
    stmt_mark: Mark,
    /// The newest pending version this transaction read.
    dep: DurableTicket,
    xlocked: HashSet<Bytes>,
    phase: Phase,
}

impl std::fmt::Debug for Txn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("id", &self.id)
            .field("view", &self.view)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

impl Txn {
    /// Starts a transaction on the engine (called by [`Engine::begin`]).
    ///
    /// # Errors
    /// Never in E0.
    pub fn begin(engine: Engine, opts: TxnOptions) -> Result<Txn> {
        Ok(Self::begin_on(Arc::new(engine), opts))
    }

    /// Starts a transaction on any commit path.
    #[must_use]
    pub fn begin_on(path: Arc<dyn CommitPath>, opts: TxnOptions) -> Txn {
        path.shared().recover_once(&*path);
        let id = path.shared().next_txn_id();
        let mut t = Self {
            path,
            id,
            opts,
            writes: WriteSet::default(),
            weight: Arc::new(AtomicUsize::new(0)),
            view: None,
            stmt_mark: Mark::default(),
            dep: DurableTicket::Ready,
            xlocked: HashSet::new(),
            phase: Phase::Active,
        };
        // A consistent snapshot opens the view at once; SERIALIZABLE with
        // snapshot isolation does too, so 1020 can fire on the first
        // statement.
        if opts.consistent_snapshot
            || (opts.isolation == Isolation::Serializable && opts.snapshot_isolation)
        {
            t.view = Some(t.shared().open_view(id));
        }
        t
    }

    fn shared(&self) -> &TxnShared {
        self.path.shared()
    }

    fn store(&self) -> &dyn SortedStore {
        self.path.store()
    }

    /// This transaction's id (higher is younger).
    #[must_use]
    pub fn id(&self) -> TxnId {
        self.id
    }

    /// The options it was started with.
    #[must_use]
    pub fn options(&self) -> &TxnOptions {
        &self.opts
    }

    /// The read view's timestamp, if one is open.
    #[must_use]
    pub fn read_view(&self) -> Option<Ts> {
        self.view
    }

    /// The read view's timestamp, opening the view if needed.
    pub fn read_ts(&mut self) -> Ts {
        if let Some(ts) = self.view {
            return ts;
        }
        let ts = self.shared().open_view(self.id);
        self.view = Some(ts);
        ts
    }

    /// State for OK-packet flags.
    #[must_use]
    pub fn state(&self) -> TxnState {
        TxnState {
            is_open: self.phase == Phase::Active,
            has_writes: !self.writes.is_empty(),
            read_only: self.opts.read_only,
            aborted_by: match self.phase {
                Phase::Aborted(r) => Some(r),
                _ => None,
            },
        }
    }

    fn ensure_active(&self) -> Result<()> {
        match self.phase {
            Phase::Active => Ok(()),
            Phase::Aborted(_) => Err(SprigError::internal(
                "transaction was rolled back; start a new one",
            )),
            Phase::Done => Err(SprigError::internal("transaction already ended")),
        }
    }

    // ----- statements, savepoints -------------------------------------------

    /// Marks the start of a statement: its writes can be undone alone, and
    /// under READ COMMITTED it gets a fresh read view.
    pub fn begin_statement(&mut self) {
        self.stmt_mark = self.writes.mark();
        if self.opts.isolation == Isolation::ReadCommitted {
            self.close_view();
        }
    }

    /// Undoes the current statement's writes. Its locks are kept, as
    /// `InnoDB` keeps them.
    pub fn rollback_statement(&mut self) {
        self.writes.rollback_to(self.stmt_mark);
        self.sync_weight();
    }

    /// `SAVEPOINT`.
    #[must_use]
    pub fn savepoint(&self) -> Savepoint {
        Savepoint(self.writes.mark())
    }

    /// `ROLLBACK TO SAVEPOINT`: later writes go, later locks stay.
    pub fn rollback_to(&mut self, sp: Savepoint) {
        self.writes.rollback_to(sp.0);
        if self.stmt_mark > sp.0 {
            self.stmt_mark = sp.0;
        }
        self.sync_weight();
    }

    fn sync_weight(&self) {
        self.weight.store(self.writes.undo_len(), Ordering::Relaxed);
    }

    fn close_view(&mut self) {
        if let Some(ts) = self.view.take() {
            self.path.shared().close_view(self.id, ts);
        }
    }

    fn note_pending(&mut self, ticket: &DurableTicket) {
        self.shared()
            .stats
            .pending_reads
            .fetch_add(1, Ordering::Relaxed);
        if ticket.index() > self.dep.index() {
            self.dep = ticket.clone();
        }
    }

    // ----- reads ----------------------------------------------------------

    /// Reads `key`: a plain read for [`ReadMode::Snapshot`], otherwise a
    /// locking read that waits on this thread (async callers lock first
    /// with [`Txn::lock`]).
    ///
    /// # Errors
    /// The store failed, or a lock error (see [`Txn::lock`]).
    pub fn get(&mut self, key: &[u8], mode: ReadMode) -> Result<Read<Option<Bytes>>> {
        match LockRequest::for_mode(mode) {
            None => self.get_snapshot(key).map(Read::Ready),
            Some(req) => {
                let k = Bytes::copy_from_slice(key);
                match self.lock_get_blocking(&k, req)? {
                    Locked::Row(v) => Ok(Read::Ready(v)),
                    Locked::Skipped => Ok(Read::Ready(None)),
                }
            }
        }
    }

    /// A consistent (snapshot) read: this transaction's own writes, else
    /// the newest version at its read view.
    ///
    /// # Errors
    /// The transaction is not active, or the store failed.
    pub fn get_snapshot(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_active()?;
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        let rts = self.read_ts();
        if !self.shared().overlay.is_empty() {
            self.shared().prune(&*self.path);
            if let Some(p) = self.shared().overlay.read_at(key, rts) {
                self.note_pending(&p.ticket);
                return Ok(p.value);
            }
        }
        Ok(mvcc::read_at(self.store(), key, rts)?.and_then(|v| v.value))
    }

    /// The newest version of `key`, pending or applied, with its timestamp.
    fn latest(&mut self, key: &[u8]) -> Result<Option<(Ts, Option<Bytes>)>> {
        if let Some(p) = self.shared().overlay.read_at(key, Ts::MAX) {
            self.note_pending(&p.ticket);
            return Ok(Some((p.ts, p.value)));
        }
        Ok(mvcc::read_latest(self.store(), key)?.map(|v| (v.ts, v.value)))
    }

    /// A current read without locking: this transaction's own write, else
    /// the latest committed version (pending included). Callers lock first.
    ///
    /// # Errors
    /// The transaction is not active, or the store failed.
    pub fn read_current(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.ensure_active()?;
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        Ok(self.latest(key)?.and_then(|(_, v)| v))
    }

    /// Reads up to `limit` live keys in `range`, in `dir` order. A plain
    /// read for [`ReadMode::Snapshot`]; otherwise a current read that locks
    /// each returned row (record locks only: no gap locks in E0).
    ///
    /// # Errors
    /// The store failed, or a lock error.
    pub fn scan(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        mode: ReadMode,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let Some(req) = LockRequest::for_mode(mode) else {
            let rts = self.read_ts();
            return Ok(self
                .scan_at(range, dir, limit, rts)?
                .into_iter()
                .map(|(k, v)| (k.to_vec(), v))
                .collect());
        };
        let candidates = self.scan_at(range, dir, limit, Ts::MAX)?;
        let mut out = Vec::with_capacity(candidates.len());
        for (k, _) in candidates {
            if let Locked::Row(Some(v)) = self.lock_get_blocking(&k, req)? {
                out.push((k.to_vec(), v));
            }
        }
        Ok(out)
    }

    /// Live keys in `range` at `at`, merged with this transaction's writes.
    fn scan_at(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        at: Ts,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.ensure_active()?;
        let mut pending = self.shared().overlay.range_at(range, at);
        let mut own: Vec<(Bytes, Option<Bytes>)> = self
            .writes
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if dir == Dir::Reverse {
            pending.reverse();
            own.reverse();
        }
        let order = |a: &[u8], b: &[u8]| match dir {
            Dir::Forward => a.cmp(b),
            Dir::Reverse => b.cmp(a),
        };
        let mut pending = pending.into_iter().peekable();
        let mut own = own.into_iter().peekable();
        let mut out = Vec::new();
        let mut dep = DurableTicket::Ready;
        {
            let mut store = VersionScan::new(self.path.store(), range, dir, at);
            let mut store_head = store.next_visible()?;
            while out.len() < limit {
                // The first key (in scan order) among the three heads.
                let mut key: Option<Bytes> = None;
                for k in [
                    store_head.as_ref().map(|(k, _)| k),
                    pending.peek().map(|(k, _)| k),
                    own.peek().map(|(k, _)| k),
                ]
                .into_iter()
                .flatten()
                {
                    if key
                        .as_ref()
                        .is_none_or(|cur| order(k, cur) == CmpOrdering::Less)
                    {
                        key = Some(k.clone());
                    }
                }
                let Some(key) = key else {
                    break;
                };
                let mut best: Option<(Ts, Option<Bytes>)> = None;
                if store_head.as_ref().is_some_and(|(k, _)| *k == key) {
                    if let Some((_, v)) = store_head.take() {
                        best = Some((v.ts, v.value));
                    }
                    store_head = store.next_visible()?;
                }
                if pending.peek().is_some_and(|(k, _)| *k == key)
                    && let Some((_, p)) = pending.next()
                    && best.as_ref().is_none_or(|(ts, _)| p.ts > *ts)
                {
                    dep = dep.max(p.ticket);
                    best = Some((p.ts, p.value));
                }
                let value = if own.peek().is_some_and(|(k, _)| *k == key) {
                    own.next().and_then(|(_, v)| v)
                } else {
                    best.and_then(|(_, v)| v)
                };
                if let Some(v) = value {
                    out.push((key, v));
                }
            }
        }
        if dep.index() > 0 {
            self.note_pending(&dep);
        }
        Ok(out)
    }

    // ----- locks ----------------------------------------------------------

    fn timeout_for(&self, wait: Wait) -> Option<Duration> {
        match wait {
            Wait::Seconds(n) => Some(Duration::from_secs(u64::from(n))),
            Wait::NoWait | Wait::SkipLocked => Some(Duration::ZERO),
            Wait::Block => self.opts.lock_wait_timeout,
        }
    }

    fn si_applies(&self, req: &LockRequest) -> bool {
        req.check_snapshot
            && self.opts.snapshot_isolation
            && self.opts.persona == Persona::MariaDb
            && matches!(
                self.opts.isolation,
                Isolation::RepeatableRead | Isolation::Serializable
            )
            && self.view.is_some()
    }

    /// MariaDB's snapshot-isolation check: the row's last writer must be
    /// visible to the read view.
    fn check_snapshot(&mut self, key: &[u8], req: &LockRequest) -> Result<()> {
        if !self.si_applies(req) || self.writes.get(key).is_some() {
            return Ok(());
        }
        let Some(rts) = self.view else {
            return Ok(());
        };
        if let Some((ts, _)) = self.latest(key)?
            && ts > rts
        {
            return Err(self.abort(AbortReason::CheckRead, errors::check_read("")));
        }
        Ok(())
    }

    fn lock_begin(&mut self, key: &Bytes, req: &LockRequest) -> Result<Step> {
        self.ensure_active()?;
        // A writer that already committed past the view fails at once,
        // without waiting.
        self.check_snapshot(key, req)?;
        self.sync_weight();
        let enqueue = matches!(req.wait, Wait::Block | Wait::Seconds(_));
        let got = self.shared().locks.acquire(
            self.id,
            &self.weight,
            key,
            req.mode,
            enqueue,
            self.opts.deadlock_detect,
        );
        match got {
            Acquire::Granted { .. } => Ok(Step::Granted),
            Acquire::Wait(w) => Ok(Step::Wait(w, self.timeout_for(req.wait))),
            Acquire::WouldWait if req.wait == Wait::SkipLocked => Ok(Step::Skipped),
            Acquire::WouldWait => Err(self.nowait_error()),
            Acquire::Deadlock => Err(self.abort(AbortReason::Deadlock, errors::deadlock())),
            Acquire::TableFull => {
                Err(self.abort(AbortReason::LockTableFull, errors::lock_table_full()))
            }
        }
    }

    fn nowait_error(&mut self) -> SprigError {
        let e = match self.opts.persona {
            Persona::MariaDb => errors::lock_wait_timeout(),
            Persona::MySql => errors::lock_nowait(),
        };
        self.statement_error(e)
    }

    /// A statement-level lock error: the statement's writes go, its locks
    /// stay; with `rollback_on_timeout` the whole transaction goes.
    fn statement_error(&mut self, e: SprigError) -> SprigError {
        if self.opts.rollback_on_timeout {
            return self.abort(
                AbortReason::Timeout {
                    rollback_on_timeout: true,
                },
                e,
            );
        }
        self.rollback_statement();
        e
    }

    fn lock_end(&mut self, key: &Bytes, req: &LockRequest, out: WaitOutcome) -> Result<()> {
        match out {
            WaitOutcome::Granted => self.after_grant(key, req),
            WaitOutcome::TimedOut => Err(self.statement_error(errors::lock_wait_timeout())),
            WaitOutcome::Deadlock => Err(self.abort(AbortReason::Deadlock, errors::deadlock())),
        }
    }

    fn after_grant(&mut self, key: &Bytes, req: &LockRequest) -> Result<()> {
        // An active writer we waited for may have committed meanwhile.
        self.check_snapshot(key, req)?;
        if req.mode == LockMode::Exclusive {
            self.xlocked.insert(key.clone());
        }
        Ok(())
    }

    /// Locks `key`. Returns false when `SKIP LOCKED` skipped it.
    ///
    /// # Errors
    /// 1205 (timeout, MariaDB `NOWAIT`) or 3572 (MySQL `NOWAIT`) with the
    /// statement rolled back; 1213, 1020 or 1206 with the whole transaction
    /// rolled back.
    pub async fn lock(&mut self, key: &Bytes, req: LockRequest) -> Result<bool> {
        match self.lock_begin(key, &req)? {
            Step::Granted => self.after_grant(key, &req).map(|()| true),
            Step::Skipped => Ok(false),
            Step::Wait(w, limit) => {
                let out = w.wait(limit).await;
                self.lock_end(key, &req, out).map(|()| true)
            }
        }
    }

    /// Blocking form of [`Txn::lock`], for threads outside a runtime.
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub fn lock_blocking(&mut self, key: &Bytes, req: LockRequest) -> Result<bool> {
        match self.lock_begin(key, &req)? {
            Step::Granted => self.after_grant(key, &req).map(|()| true),
            Step::Skipped => Ok(false),
            Step::Wait(w, limit) => {
                let out = w.wait_blocking(limit);
                self.lock_end(key, &req, out).map(|()| true)
            }
        }
    }

    /// A locking read (`FOR UPDATE`, `FOR SHARE`, DML's read phase): lock,
    /// then read the latest committed version.
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub async fn lock_get(&mut self, key: &Bytes, req: LockRequest) -> Result<Locked> {
        if self.lock(key, req).await? {
            self.read_current(key).map(Locked::Row)
        } else {
            Ok(Locked::Skipped)
        }
    }

    /// Blocking form of [`Txn::lock_get`].
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub fn lock_get_blocking(&mut self, key: &Bytes, req: LockRequest) -> Result<Locked> {
        if self.lock_blocking(key, req)? {
            self.read_current(key).map(Locked::Row)
        } else {
            Ok(Locked::Skipped)
        }
    }

    /// READ COMMITTED's early unlock of a row that failed the `WHERE`.
    pub fn unlock(&mut self, key: &Bytes) {
        if self.writes.get(key).is_none() {
            self.xlocked.remove(key);
            self.shared().locks.unlock(self.id, key);
        }
    }

    // ----- writes ---------------------------------------------------------

    fn write_entry(&mut self, key: Bytes, value: Option<Bytes>) -> Result<()> {
        self.ensure_active()?;
        if !self.xlocked.contains(&key) {
            // A blind write: take the lock here. Async callers lock first.
            self.lock_blocking(&key, LockRequest::write())?;
        }
        let max = self.shared().max_txn_bytes.load(Ordering::Relaxed);
        if self.writes.bytes() + key.len() + value.as_ref().map_or(0, Bytes::len) > max {
            return Err(self.statement_error(SprigError::standard(
                1197,
                *b"HY000",
                "Multi-statement transaction required more than 'max_binlog_cache_size' bytes of storage",
            )));
        }
        self.writes.put(key, value);
        self.sync_weight();
        Ok(())
    }

    /// Buffers a put. Takes the key's X lock first if this transaction
    /// doesn't hold it (waiting on this thread).
    ///
    /// # Errors
    /// Not active, a lock error, or the transaction is too big.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) -> Result<()> {
        self.write_entry(Bytes::from(key.into()), Some(value.into()))
    }

    /// Buffers a delete, locking as [`Txn::put`] does.
    ///
    /// # Errors
    /// As for [`Txn::put`].
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.write_entry(Bytes::from(key.into()), None)
    }

    // ----- end ------------------------------------------------------------

    /// A ticket for replying to the current statement: ready once every
    /// pending version it read is durable.
    #[must_use]
    pub fn reply_gate(&self) -> DurableTicket {
        self.dep.clone()
    }

    /// Commits. The writes are queued in the log and the locks released
    /// (early lock release). Answer the client once the ticket is durable.
    ///
    /// # Errors
    /// The transaction is not active, or the log refused the entry (the
    /// transaction is then rolled back).
    pub fn commit(mut self) -> Result<CommitTicket> {
        self.ensure_active()?;
        let token = self.view.unwrap_or_else(|| self.shared().visible_ts());
        self.close_view();
        if self.writes.is_empty() {
            self.shared().locks.release_all(self.id);
            self.finish(Phase::Done);
            return Ok(CommitTicket {
                cts: None,
                token,
                ticket: std::mem::take(&mut self.dep),
            });
        }
        let writes = std::mem::take(&mut self.writes).into_writes();
        let path = self.path.clone();
        match path
            .shared()
            .commit_writes(&*path, self.id, writes, self.opts.durability)
        {
            Ok((cts, ticket)) => {
                self.finish(Phase::Done);
                Ok(CommitTicket {
                    cts: Some(cts),
                    token: cts,
                    ticket: ticket.max(std::mem::take(&mut self.dep)),
                })
            }
            Err(e) => Err(self.abort(AbortReason::LeaderChange, e)),
        }
    }

    /// Rolls back: drops the writes and releases every lock.
    pub fn rollback(mut self) {
        if self.phase == Phase::Active {
            self.rollback_inner();
            self.finish(Phase::Done);
        }
    }

    fn rollback_inner(&mut self) {
        self.writes = WriteSet::default();
        self.xlocked.clear();
        self.close_view();
        self.shared().locks.release_all(self.id);
        self.shared()
            .stats
            .rollbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Ends the transaction for a transaction-level error.
    fn abort(&mut self, reason: AbortReason, e: SprigError) -> SprigError {
        if self.phase == Phase::Active {
            self.rollback_inner();
            self.finish(Phase::Aborted(reason));
        }
        e
    }

    fn finish(&mut self, phase: Phase) {
        if self.phase == Phase::Active {
            self.shared().txn_closed();
        }
        self.phase = phase;
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        if self.phase == Phase::Active {
            self.rollback_inner();
            self.finish(Phase::Done);
        }
    }
}
