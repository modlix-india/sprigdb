//! A transaction: snapshot reads, locking (current) reads, buffered writes,
//! statement and savepoint rollback, and commit with a durability ticket.

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
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
use crate::keys::{self, KeyRange};
use crate::locks::{
    Acquire, GapAcquire, Interrupt, LockMode, LockTable, Pos, TxnId, Wait, WaitOutcome,
};
use crate::log::Ticket;
use crate::store::{Cf, Read, SortedStore};

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

/// How an insert locks its key ([`Txn::lock_insert`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertLock {
    dup: LockMode,
    wait: Wait,
    check_snapshot: bool,
}

impl InsertLock {
    /// A plain `INSERT`: if the key is already there, the duplicate check
    /// keeps a shared lock on it.
    #[must_use]
    pub const fn insert() -> Self {
        Self {
            dup: LockMode::Shared,
            wait: Wait::Block,
            check_snapshot: false,
        }
    }

    /// `REPLACE`, `INSERT … ON DUPLICATE KEY UPDATE`, `LOAD DATA … REPLACE`:
    /// an existing row is locked exclusively, because the statement goes on
    /// to change it.
    #[must_use]
    pub const fn replace() -> Self {
        Self {
            dup: LockMode::Exclusive,
            wait: Wait::Block,
            check_snapshot: false,
        }
    }

    /// The same request with another wait policy (`SKIP LOCKED` is treated
    /// as `NOWAIT`: an insert can't skip its own row).
    #[must_use]
    pub const fn with_wait(mut self, wait: Wait) -> Self {
        self.wait = wait;
        self
    }

    /// Also apply MariaDB's snapshot-isolation check (1020) to the row the
    /// duplicate check finds.
    #[must_use]
    pub const fn with_snapshot_check(mut self) -> Self {
        self.check_snapshot = true;
        self
    }
}

/// What [`Txn::lock_insert`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertProbe {
    /// The key is absent. The transaction holds its exclusive lock and no
    /// other transaction's gap lock covers it: the caller may `put`.
    Free,
    /// The key is present with this value (1062 for a plain insert). The
    /// duplicate check's lock stays until the transaction ends.
    Exists(Bytes),
}

/// Ends a transaction's lock wait from another thread (`KILL QUERY`, a
/// statement time limit). Get one from [`Txn::interrupt_handle`].
#[derive(Debug, Clone)]
pub struct InterruptHandle {
    flag: Arc<AtomicU8>,
    locks: Arc<LockTable>,
    txn: TxnId,
}

impl InterruptHandle {
    /// Interrupts the transaction's current statement: a lock wait in
    /// progress ends now, and any lock request the statement makes later
    /// fails at once. The statement is rolled back (1317; for a time limit
    /// 1969 on MariaDB, 3024 on MySQL); the transaction stays open with its
    /// locks. The next [`Txn::begin_statement`] clears the interrupt.
    pub fn interrupt(&self, why: Interrupt) {
        let code = match why {
            Interrupt::Kill => 1,
            Interrupt::StatementTimeout => 2,
        };
        self.flag.store(code, Ordering::Release);
        self.locks.interrupt(self.txn, why);
    }
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
pub(super) enum Phase {
    Active,
    Aborted(AbortReason),
    Done,
}

/// A savepoint (`SAVEPOINT s`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Savepoint(Mark, usize);

/// A queued commit: answer the client only once it is durable.
#[derive(Debug, Clone)]
pub struct CommitTicket {
    cts: Option<Ts>,
    token: Ts,
    ticket: DurableTicket,
    /// Entries queued after the ones the reply waits for (a commit across
    /// units resolving its participants on a log that needs waits).
    later: DurableTicket,
}

impl CommitTicket {
    pub(super) fn new(cts: Option<Ts>, token: Ts, ticket: DurableTicket) -> Self {
        Self {
            cts,
            token,
            ticket,
            later: DurableTicket::Ready,
        }
    }

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

    /// The commit log's ticket: the transaction's own entry (across units:
    /// the entry carrying its notes, else its first unit's), or the entry
    /// of the newest pending version it read.
    #[must_use]
    pub fn log_ticket(&self) -> Option<&Ticket> {
        self.ticket.first_log()
    }

    /// The commit's log ticket on `unit`, if it wrote there.
    #[must_use]
    pub fn log_ticket_of(&self, unit: u64) -> Option<&Ticket> {
        self.ticket.log_of(unit).or_else(|| self.later.log_of(unit))
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

/// Whether a wait parks the task (sessions on the runtime) or the thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum How {
    Async,
    Blocking,
}

/// Runs a future built with [`How::Blocking`]. Such a future does all its
/// waiting on the calling thread and never yields, so one poll finishes it.
fn run_now<F: Future>(f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
}

/// A granted record lock request.
#[derive(Debug, Clone, Copy)]
struct Grant {
    /// The transaction held no lock on the key before this request.
    new: bool,
    /// The request had to wait.
    waited: bool,
}

/// A key the locking scan saw in the latest state.
struct Seen {
    key: Bytes,
    /// The version's commit timestamp; `None` for the transaction's own
    /// write.
    ts: Option<Ts>,
    /// `None`: deleted (reported only when the caller asks for deletes).
    value: Option<Bytes>,
}

/// A row filter for locking scans: `(key, value) → keep`.
type RowPred<'a> = &'a mut (dyn FnMut(&[u8], &[u8]) -> bool + Send);

/// Rows a locking scan reads per round.
const SCAN_CHUNK: usize = 256;

/// The store bound that stands for the place `p`: as a range's inclusive
/// start, the range begins at `p`; as its exclusive end, it stops there.
/// `None`: after every key.
///
/// "At `k`" is `k` itself, which sorts before every stored version of `k`
/// (`k | !ts`). "Just after `k`" must sort after all of them and before
/// the next logical key: `k` followed by more `0xFF` bytes than a version
/// suffix has. Logical keys are prefix-free, so no other key starts with
/// `k` and none can fall in between.
fn bound(p: &Pos) -> Option<Vec<u8>> {
    match p {
        Pos::Key(k, false) => Some(k.to_vec()),
        Pos::Key(k, true) => {
            let mut v = Vec::with_capacity(k.len() + keys::VERSION_LEN + 1);
            v.extend_from_slice(k);
            v.resize(k.len() + keys::VERSION_LEN + 1, 0xFF);
            Some(v)
        }
        Pos::End => None,
    }
}

/// The keys between two places; `None` when there are none.
fn range_between(lo: &Pos, hi: &Pos) -> Option<KeyRange> {
    if lo >= hi {
        return None;
    }
    let start = bound(lo)?;
    let end = bound(hi);
    if end.as_ref().is_some_and(|e| *e <= start) {
        return None;
    }
    Some(KeyRange { start, end })
}

/// One transaction on one unit.
pub struct Txn {
    pub(super) path: Arc<dyn CommitPath>,
    pub(super) id: TxnId,
    pub(super) opts: TxnOptions,
    pub(super) writes: WriteSet,
    weight: Arc<AtomicUsize>,
    /// The open read view, if any.
    view: Option<Ts>,
    stmt_mark: Mark,
    /// The newest pending version this transaction read.
    pub(super) dep: DurableTicket,
    /// Keys this transaction holds exclusively. The number is the statement
    /// (`stmt_seq`) in which an insert of the key passed its check against
    /// other transactions' gap locks; 0 if none did. Only the same
    /// statement's `put` may rely on it.
    xlocked: HashMap<Bytes, u32>,
    /// Counts statements, from 1.
    stmt_seq: u32,
    /// READ COMMITTED: record locks the current statement was the first to
    /// take, which [`Txn::unlock`] may give back.
    stmt_fresh: Vec<Bytes>,
    /// Keys on which an insert put this transaction's first lock, each with
    /// the undo position the insert started at. When a statement rollback or
    /// `ROLLBACK TO SAVEPOINT` undoes the insert, the key's lock goes the
    /// way the persona's server lets it go (see [`Txn::undo_insert_locks`]).
    inserted: Vec<(Mark, Bytes)>,
    /// Set by an [`InterruptHandle`]: 0 none, 1 killed, 2 time limit.
    interrupt: Arc<AtomicU8>,
    phase: Phase,
    /// Log notes and plain store writes the commit carries
    /// ([`Txn::add_note`], [`Txn::add_plain_put`]).
    notes: Vec<Extra>,
    /// `notes.len()` when the current statement started.
    stmt_notes: usize,
    /// Whether reads may land on a replica that doesn't lead their unit
    /// (a replicated engine).
    follows: bool,
    /// Units this read view has already caught up on (see
    /// [`Txn::follow`]).
    followed: Vec<u64>,
}

/// What a commit's log entry carries beside the versioned writes.
#[derive(Debug, Clone)]
pub(crate) enum Extra {
    /// An opaque note (the apply step skips it; observers read it), in
    /// the entry of the notes unit, or of the unit holding a tenant's keys.
    Note(Option<u64>, Vec<u8>),
    /// A plain put.
    Put(Cf, Vec<u8>, Vec<u8>),
    /// A plain delete.
    Delete(Cf, Vec<u8>),
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
        let follows = path.follows();
        let mut t = Self {
            path,
            id,
            opts,
            writes: WriteSet::default(),
            weight: Arc::new(AtomicUsize::new(0)),
            view: None,
            stmt_mark: Mark::default(),
            dep: DurableTicket::Ready,
            xlocked: HashMap::new(),
            stmt_seq: 1,
            stmt_fresh: Vec::new(),
            inserted: Vec::new(),
            interrupt: Arc::new(AtomicU8::new(0)),
            phase: Phase::Active,
            notes: Vec::new(),
            stmt_notes: 0,
            follows,
            followed: Vec::new(),
        };
        // A consistent snapshot opens the view at once. (SERIALIZABLE with
        // snapshot isolation opens it when the first statement starts: see
        // `begin_statement`.)
        if opts.consistent_snapshot {
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

    pub(super) fn ensure_active(&self) -> Result<()> {
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
    /// under READ COMMITTED it gets a fresh read view. Under SERIALIZABLE
    /// with snapshot isolation the transaction's read view opens at its
    /// first statement, whatever that statement is (`BEGIN` alone opens
    /// none), so the snapshot check can fire from that statement on.
    pub fn begin_statement(&mut self) {
        self.stmt_mark = self.writes.mark();
        self.stmt_notes = self.notes.len();
        self.stmt_seq = self.stmt_seq.wrapping_add(1).max(1);
        self.stmt_fresh.clear();
        self.interrupt.store(0, Ordering::Release);
        match self.opts.isolation {
            Isolation::ReadCommitted => self.close_view(),
            Isolation::Serializable
                if self.opts.snapshot_isolation
                    && self.view.is_none()
                    && self.phase == Phase::Active =>
            {
                self.view = Some(self.shared().open_view(self.id));
            }
            Isolation::Serializable | Isolation::RepeatableRead => {}
        }
    }

    /// Undoes the current statement's writes. Its locks are kept, as
    /// `InnoDB` keeps them, except the lock of a row the statement inserted
    /// (see [`Txn::undo_insert_locks`]).
    pub fn rollback_statement(&mut self) {
        self.writes.rollback_to(self.stmt_mark);
        self.notes.truncate(self.stmt_notes);
        self.undo_insert_locks(self.stmt_mark);
        self.sync_weight();
    }

    /// The locks of inserts undone back to `mark`. A row whose insert is
    /// undone is gone, and so is the lock the insert held on it:
    /// - MariaDB keeps nothing: another transaction can insert the key at
    ///   once.
    /// - MySQL keeps the key's place closed to other transactions' inserts
    ///   until the transaction ends (a gap lock; at READ COMMITTED, which
    ///   has none, it keeps nothing either).
    ///
    /// Only a key whose first lock in this transaction came from the insert
    /// is touched, and only if no write of the transaction is left on it.
    fn undo_insert_locks(&mut self, mark: Mark) {
        while let Some((at, _)) = self.inserted.last() {
            if *at < mark {
                break;
            }
            let Some((_, key)) = self.inserted.pop() else {
                break;
            };
            if self.phase != Phase::Active || self.writes.get(&key).is_some() {
                continue;
            }
            if self.opts.persona == Persona::MySql && self.gap_locks() {
                // If the lock table is full the record lock simply stays.
                let closed = self.shared().locks.gap_acquire(
                    self.id,
                    &self.weight,
                    &Pos::at(key.clone()),
                    &Pos::after(key.clone()),
                    false,
                    false,
                );
                if matches!(closed, GapAcquire::TableFull) {
                    continue;
                }
            }
            if self.xlocked.remove(&key).is_some() {
                if let Some(pos) = self.stmt_fresh.iter().rposition(|k| *k == key) {
                    self.stmt_fresh.swap_remove(pos);
                }
                self.shared().locks.unlock(self.id, &key);
            }
        }
    }

    /// `SAVEPOINT`.
    #[must_use]
    pub fn savepoint(&self) -> Savepoint {
        Savepoint(self.writes.mark(), self.notes.len())
    }

    /// `ROLLBACK TO SAVEPOINT`: later writes go, later locks stay (but see
    /// [`Txn::undo_insert_locks`] for rows inserted since).
    pub fn rollback_to(&mut self, sp: Savepoint) {
        self.writes.rollback_to(sp.0);
        self.notes.truncate(sp.1);
        self.stmt_notes = self.stmt_notes.min(sp.1);
        self.undo_insert_locks(sp.0);
        if self.stmt_mark > sp.0 {
            self.stmt_mark = sp.0;
        }
        self.sync_weight();
    }

    fn sync_weight(&self) {
        self.weight.store(self.writes.undo_len(), Ordering::Relaxed);
    }

    fn close_view(&mut self) {
        self.followed.clear();
        if let Some(ts) = self.view.take() {
            self.path.shared().close_view(self.id, ts);
        }
    }

    /// Before the first read of `key`'s unit under this read view, on a
    /// replica that doesn't lead the unit: catches up with the leader (a
    /// read barrier) and, if this is the view's first such unit and the
    /// transaction hasn't written, opens (or moves up) the view to the
    /// unit's applied timestamp, so the read sees every commit acknowledged
    /// before it started. Later units are caught up and read at the same
    /// view (reads across units see each unit's state at that timestamp,
    /// as on the leader).
    fn follow(&mut self, key: &[u8]) -> Result<()> {
        if !self.follows {
            return Ok(());
        }
        let unit = self.path.unit_of(key);
        if self.followed.contains(&unit) {
            return Ok(());
        }
        let ts = self.path.follower_read(unit)?;
        let first = self.followed.is_empty();
        self.followed.push(unit);
        let Some(ts) = ts else {
            return Ok(());
        };
        match self.view {
            None => self.view = Some(self.shared().open_view_at(self.id, ts)),
            Some(v) if v < ts && first && self.writes.is_empty() => {
                self.shared().close_view(self.id, v);
                self.view = Some(self.shared().open_view_at(self.id, ts));
            }
            Some(_) => {}
        }
        Ok(())
    }

    fn note_pending(&mut self, ticket: &DurableTicket) {
        self.shared()
            .stats
            .pending_reads
            .fetch_add(1, Ordering::Relaxed);
        self.dep = std::mem::take(&mut self.dep).max(ticket.clone());
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
        self.follow(key)?;
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

    /// [`Txn::get_snapshot`] for several keys, in the order given: this
    /// transaction's own writes and versions still being applied first,
    /// the rest read from the store in key order through one cursor.
    ///
    /// # Errors
    /// The transaction is not active, or the store failed.
    pub fn get_snapshot_many(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        self.ensure_active()?;
        let mut out: Vec<Option<Bytes>> = vec![None; keys.len()];
        let mut rest: Vec<usize> = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            if let Some(v) = self.writes.get(key) {
                out[i].clone_from(v);
            } else {
                self.follow(key)?;
                rest.push(i);
            }
        }
        if rest.is_empty() {
            return Ok(out);
        }
        let rts = self.read_ts();
        if !self.shared().overlay.is_empty() {
            self.shared().prune(&*self.path);
            let mut still = Vec::with_capacity(rest.len());
            for i in rest {
                if let Some(p) = self.shared().overlay.read_at(keys[i], rts) {
                    self.note_pending(&p.ticket);
                    out[i] = p.value;
                } else {
                    still.push(i);
                }
            }
            rest = still;
        }
        rest.sort_by(|a, b| keys[*a].cmp(keys[*b]));
        let sorted: Vec<&[u8]> = rest.iter().map(|&i| keys[i]).collect();
        let got = mvcc::read_many_at(self.store(), &sorted, rts)?;
        for (i, v) in rest.into_iter().zip(got) {
            out[i] = v.and_then(|v| v.value);
        }
        Ok(out)
    }

    /// The newest version of `key`, pending or applied, with its timestamp.
    fn latest(&mut self, key: &[u8]) -> Result<Option<(Ts, Option<Bytes>)>> {
        self.follow(key)?;
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
    /// read for [`ReadMode::Snapshot`]; otherwise a locking current read
    /// that waits on this thread, with the locks of [`Txn::scan_lock`].
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
        let rows = match LockRequest::for_mode(mode) {
            None => {
                self.follow(&range.start)?;
                let rts = self.read_ts();
                self.scan_at(range, dir, limit, rts)?
            }
            Some(req) => self.scan_lock_blocking(range, dir, limit, req)?,
        };
        Ok(rows.into_iter().map(|(k, v)| (k.to_vec(), v)).collect())
    }

    /// Snapshot reads of several ranges, each up to `limit` live keys in
    /// ascending order, as [`Txn::scan`] with [`ReadMode::Snapshot`] gives
    /// them one by one, through one store cursor over `outer`, which must
    /// hold every range: many short ranges (a hop's adjacency prefixes)
    /// without making a store cursor for each.
    ///
    /// # Errors
    /// The transaction is not active, or the store failed.
    pub fn scan_many(
        &mut self,
        ranges: &[KeyRange],
        outer: &KeyRange,
        limit: usize,
    ) -> Result<Vec<Vec<(Bytes, Bytes)>>> {
        self.ensure_active()?;
        for r in ranges {
            self.follow(&r.start)?;
        }
        let rts = self.read_ts();
        let path = Arc::clone(&self.path);
        let mut vs = VersionScan::new_multi(path.store(), outer, rts);
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            vs.retarget(r);
            let (rows, _) = self.scan_rows_on(r, Dir::Forward, limit, rts, false, Some(&mut vs))?;
            out.push(
                rows.into_iter()
                    .filter_map(|x| x.value.map(|v| (x.key, v)))
                    .collect(),
            );
        }
        Ok(out)
    }

    /// A current read of a range without locking: up to `limit` live keys
    /// of `range` in `dir` order, each this transaction's own write, else
    /// the latest committed version (pending included). It opens no read
    /// view and takes no lock; nothing stops the rows from changing
    /// afterwards. Locking reads use [`Txn::scan_lock`].
    ///
    /// # Errors
    /// The transaction is not active, or the store failed.
    pub fn scan_current(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_at(range, dir, limit, Ts::MAX)
    }

    /// Live keys in `range` at `at`, merged with this transaction's writes.
    fn scan_at(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        at: Ts,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let (rows, _) = self.scan_rows(range, dir, limit, at, false)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| r.value.map(|v| (r.key, v)))
            .collect())
    }

    /// The keys of `range` as of `at`, merged with this transaction's
    /// writes, until `limit` live ones are found. With `want_dead`, keys
    /// whose newest version is another transaction's delete are reported
    /// too (they don't count towards `limit`). The flag is true when the
    /// range ran out before the limit.
    fn scan_rows(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        at: Ts,
        want_dead: bool,
    ) -> Result<(Vec<Seen>, bool)> {
        self.scan_rows_on(range, dir, limit, at, want_dead, None)
    }

    /// [`Txn::scan_rows`], through `ext`'s store cursor when given (a multi
    /// scan retargeted to `range`, [`Txn::scan_many`]).
    fn scan_rows_on(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        at: Ts,
        want_dead: bool,
        ext: Option<&mut VersionScan<'_>>,
    ) -> Result<(Vec<Seen>, bool)> {
        self.ensure_active()?;
        if range.end.as_ref().is_some_and(|e| *e <= range.start) {
            return Ok((Vec::new(), true));
        }
        self.follow(&range.start)?;
        let mut pending = self.shared().overlay.range_at(range, at);
        if dir == Dir::Reverse {
            pending.reverse();
        }
        let order = |a: &[u8], b: &[u8]| match dir {
            Dir::Forward => a.cmp(b),
            Dir::Reverse => b.cmp(a),
        };
        let mut pending = pending.into_iter().peekable();
        let mut out = Vec::new();
        let mut live = 0;
        let mut exhausted = false;
        let mut dep = DurableTicket::Ready;
        {
            // This transaction's own writes in the range, read as the merge
            // reaches them: a batch takes `limit` rows, and copying every
            // buffered write past them made a scan over a range the
            // transaction already rewrote quadratic in the rows written.
            let clone = |(k, v): (&Bytes, &Option<Bytes>)| (k.clone(), v.clone());
            let own: Box<dyn Iterator<Item = (Bytes, Option<Bytes>)> + '_> = match dir {
                Dir::Forward => Box::new(self.writes.range(range).map(clone)),
                Dir::Reverse => Box::new(self.writes.range(range).rev().map(clone)),
            };
            let mut own = own.peekable();
            let mut local;
            let store: &mut dyn Visible = if let Some(v) = ext {
                v
            } else {
                local = VersionScan::new(self.path.store(), range, dir, at);
                &mut local
            };
            let mut store_head = store.next_visible()?;
            while live < limit {
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
                    exhausted = true;
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
                let (ts, value, mine) = if own.peek().is_some_and(|(k, _)| *k == key) {
                    (None, own.next().and_then(|(_, v)| v), true)
                } else {
                    match best {
                        Some((ts, v)) => (Some(ts), v, false),
                        None => (None, None, false),
                    }
                };
                if value.is_some() {
                    live += 1;
                    out.push(Seen { key, ts, value });
                } else if want_dead && !mine && ts.is_some() {
                    out.push(Seen { key, ts, value });
                }
            }
        }
        if dep.index() > 0 {
            self.note_pending(&dep);
        }
        Ok((out, exhausted))
    }

    /// The newest version of `key` for a current read: this transaction's
    /// own write (no timestamp), else the latest committed one.
    fn current_version(&mut self, key: &[u8]) -> Result<(Option<Ts>, Option<Bytes>)> {
        if let Some(v) = self.writes.get(key) {
            return Ok((None, v.clone()));
        }
        Ok(match self.latest(key)? {
            Some((ts, v)) => (Some(ts), v),
            None => (None, None),
        })
    }

    // ----- locks ----------------------------------------------------------

    fn timeout_for(&self, wait: Wait) -> Option<Duration> {
        match wait {
            Wait::Seconds(n) => Some(Duration::from_secs(u64::from(n))),
            Wait::NoWait | Wait::SkipLocked => Some(Duration::ZERO),
            Wait::Block => self.opts.lock_wait_timeout,
        }
    }

    /// Whether a request with this wait policy may queue. One that would
    /// give up at once (`NOWAIT`, `SKIP LOCKED`, a zero timeout) never
    /// does, so it can't be picked, or pick anyone, as a deadlock victim.
    fn may_queue(&self, wait: Wait) -> bool {
        matches!(wait, Wait::Block | Wait::Seconds(_))
            && self.timeout_for(wait) != Some(Duration::ZERO)
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

    /// Whether locking reads take gap locks: every level but READ COMMITTED.
    fn gap_locks(&self) -> bool {
        self.opts.isolation != Isolation::ReadCommitted
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

    /// 1020 if a version stamped `ts` is newer than the read view.
    fn check_version(&mut self, si: bool, ts: Option<Ts>) -> Result<()> {
        if si
            && let (Some(ts), Some(rts)) = (ts, self.view)
            && ts > rts
        {
            return Err(self.abort(AbortReason::CheckRead, errors::check_read("")));
        }
        Ok(())
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

    /// A handle another thread can use to end this transaction's lock wait
    /// (`KILL QUERY`, a statement time limit).
    #[must_use]
    pub fn interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle {
            flag: self.interrupt.clone(),
            locks: self.shared().locks.clone(),
            txn: self.id,
        }
    }

    /// The interrupt's error. Only the statement is rolled back; its locks
    /// stay.
    fn interrupted_error(&mut self, why: Interrupt) -> SprigError {
        self.rollback_statement();
        match (why, self.opts.persona) {
            (Interrupt::Kill, _) => errors::query_interrupted(),
            (Interrupt::StatementTimeout, Persona::MariaDb) => errors::statement_timeout_mariadb(),
            (Interrupt::StatementTimeout, Persona::MySql) => errors::statement_timeout_mysql(),
        }
    }

    fn interrupted(&self) -> Option<Interrupt> {
        match self.interrupt.load(Ordering::Acquire) {
            0 => None,
            1 => Some(Interrupt::Kill),
            _ => Some(Interrupt::StatementTimeout),
        }
    }

    /// 1213 if the leadership of a unit whose locks the transaction holds
    /// or waited for left this node: the transaction is rolled back.
    fn check_doomed(&mut self) -> Result<()> {
        if self.shared().locks.is_doomed(self.id) {
            return Err(self.abort(AbortReason::LeaderChange, errors::leader_lost()));
        }
        Ok(())
    }

    fn check_interrupt(&mut self) -> Result<()> {
        self.check_doomed()?;
        match self.interrupted() {
            None => Ok(()),
            Some(why) => Err(self.interrupted_error(why)),
        }
    }

    /// Turns the lock table's answer into a grant, waiting if it must.
    /// `None`: `SKIP LOCKED` skipped the row.
    async fn settle(&mut self, got: Acquire, wait: Wait, how: How) -> Result<Option<Grant>> {
        if let Err(e) = self.check_doomed() {
            if let Acquire::Wait(w) = got {
                drop(w);
            }
            return Err(e);
        }
        match got {
            Acquire::Granted { fresh, upgrade } => Ok(Some(Grant {
                new: fresh && !upgrade,
                waited: false,
            })),
            Acquire::Wait(w) => {
                // An interrupt that arrived before the request was queued
                // found no wait to end.
                if let Some(why) = self.interrupted() {
                    drop(w);
                    return Err(self.interrupted_error(why));
                }
                let new = !w.is_upgrade() && !w.is_insert_intention();
                let limit = self.timeout_for(wait);
                let out = match how {
                    How::Async => w.wait(limit).await,
                    How::Blocking => w.wait_blocking(limit),
                };
                match out {
                    WaitOutcome::Granted => Ok(Some(Grant { new, waited: true })),
                    WaitOutcome::TimedOut => Err(self.statement_error(errors::lock_wait_timeout())),
                    WaitOutcome::Deadlock => {
                        Err(self.abort(AbortReason::Deadlock, errors::deadlock()))
                    }
                    WaitOutcome::Interrupted(why) => {
                        self.check_doomed()?;
                        Err(self.interrupted_error(why))
                    }
                }
            }
            Acquire::WouldWait if wait == Wait::SkipLocked => Ok(None),
            Acquire::WouldWait if wait == Wait::NoWait => Err(self.nowait_error()),
            // A zero timeout: the wait is over before it starts.
            Acquire::WouldWait => Err(self.statement_error(errors::lock_wait_timeout())),
            Acquire::Deadlock => Err(self.abort(AbortReason::Deadlock, errors::deadlock())),
            Acquire::TableFull => {
                Err(self.abort(AbortReason::LockTableFull, errors::lock_table_full()))
            }
        }
    }

    /// Takes a record lock, waiting per `wait`. No snapshot check.
    async fn acquire(
        &mut self,
        key: &Bytes,
        mode: LockMode,
        wait: Wait,
        how: How,
    ) -> Result<Option<Grant>> {
        self.sync_weight();
        let got = self.shared().locks.acquire(
            self.id,
            &self.weight,
            key,
            mode,
            self.may_queue(wait),
            self.opts.deadlock_detect,
        );
        let grant = self.settle(got, wait, how).await?;
        if let Some(g) = grant {
            self.note_grant(key, mode, g);
        }
        Ok(grant)
    }

    /// Takes a record lock only if nobody is in the way. `None`: it would
    /// have to wait.
    fn try_acquire(&mut self, key: &Bytes, mode: LockMode) -> Result<Option<Grant>> {
        self.sync_weight();
        let got = self
            .shared()
            .locks
            .acquire(self.id, &self.weight, key, mode, false, false);
        match got {
            Acquire::Granted { fresh, upgrade } => {
                let g = Grant {
                    new: fresh && !upgrade,
                    waited: false,
                };
                self.note_grant(key, mode, g);
                Ok(Some(g))
            }
            Acquire::TableFull => {
                Err(self.abort(AbortReason::LockTableFull, errors::lock_table_full()))
            }
            Acquire::Wait(_) | Acquire::WouldWait | Acquire::Deadlock => Ok(None),
        }
    }

    fn note_grant(&mut self, key: &Bytes, mode: LockMode, g: Grant) {
        if mode == LockMode::Exclusive && !self.xlocked.contains_key(key) {
            self.xlocked.insert(key.clone(), 0);
        }
        if g.new && self.opts.isolation == Isolation::ReadCommitted {
            self.stmt_fresh.push(key.clone());
        }
    }

    /// Gives back a record lock this request was the first to take.
    fn release_new(&mut self, key: &Bytes, g: Grant) {
        if !g.new {
            return;
        }
        self.xlocked.remove(key);
        if let Some(pos) = self.stmt_fresh.iter().rposition(|k| k == key) {
            self.stmt_fresh.swap_remove(pos);
        }
        self.shared().locks.unlock(self.id, key);
    }

    /// Takes a gap lock over `[lo, hi)`; see [`LockTable::gap_acquire`].
    fn gap(&mut self, lo: &Pos, hi: &Pos, reverse: bool, stop: bool) -> Result<Option<Bytes>> {
        match self
            .shared()
            .locks
            .gap_acquire(self.id, &self.weight, lo, hi, reverse, stop)
        {
            GapAcquire::Granted => Ok(None),
            GapAcquire::StoppedAt(k) => Ok(Some(k)),
            GapAcquire::TableFull => {
                Err(self.abort(AbortReason::LockTableFull, errors::lock_table_full()))
            }
        }
    }

    async fn lock_core(
        &mut self,
        key: &Bytes,
        req: &LockRequest,
        how: How,
    ) -> Result<Option<Grant>> {
        self.ensure_active()?;
        self.check_interrupt()?;
        // A writer that already committed past the view fails at once,
        // without waiting.
        self.check_snapshot(key, req)?;
        let grant = self.acquire(key, req.mode, req.wait, how).await?;
        if grant.is_some() {
            // An active writer we waited for may have committed meanwhile.
            self.check_snapshot(key, req)?;
        }
        Ok(grant)
    }

    /// Takes a record lock on `key`, and nothing else: no gap lock at any
    /// isolation level. Returns false when `SKIP LOCKED` skipped it.
    ///
    /// # Errors
    /// 1205 (timeout, MariaDB `NOWAIT`), 3572 (MySQL `NOWAIT`) or 1317/1969
    /// (interrupted) with the statement rolled back; 1213, 1020 or 1206 with
    /// the whole transaction rolled back.
    pub async fn lock(&mut self, key: &Bytes, req: LockRequest) -> Result<bool> {
        Ok(self.lock_core(key, &req, How::Async).await?.is_some())
    }

    /// Blocking form of [`Txn::lock`], for threads outside a runtime.
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub fn lock_blocking(&mut self, key: &Bytes, req: LockRequest) -> Result<bool> {
        Ok(run_now(self.lock_core(key, &req, How::Blocking))?.is_some())
    }

    async fn lock_get_core(&mut self, key: &Bytes, req: &LockRequest, how: How) -> Result<Locked> {
        let Some(grant) = self.lock_core(key, req, how).await? else {
            return Ok(Locked::Skipped);
        };
        let v = self.read_current(key)?;
        if v.is_none() {
            self.missed(key, grant)?;
        }
        Ok(Locked::Row(v))
    }

    /// A locking read found no row at `key`. Under READ COMMITTED nothing
    /// stays locked. Otherwise the place where the row would be is closed
    /// to other transactions' inserts with a gap lock, and the record lock
    /// is given back: two transactions may both hold the gap, as two
    /// `SELECT … FOR UPDATE` on a missing row both succeed.
    fn missed(&mut self, key: &Bytes, grant: Grant) -> Result<()> {
        if self.writes.get(key).is_some() {
            // Deleted by this transaction: the lock guards its own write.
            return Ok(());
        }
        if self.gap_locks() {
            self.gap(
                &Pos::at(key.clone()),
                &Pos::after(key.clone()),
                false,
                false,
            )?;
        }
        self.release_new(key, grant);
        Ok(())
    }

    /// A locking read of one key (`FOR UPDATE`, `FOR SHARE`, DML's read
    /// phase on a unique equality): lock, then read the latest committed
    /// version. A row that is found stays record-locked. For a missing row,
    /// REPEATABLE READ and SERIALIZABLE keep a gap lock on the place it
    /// would be (other transactions can't insert it), and READ COMMITTED
    /// keeps nothing.
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub async fn lock_get(&mut self, key: &Bytes, req: LockRequest) -> Result<Locked> {
        self.lock_get_core(key, &req, How::Async).await
    }

    /// Blocking form of [`Txn::lock_get`].
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub fn lock_get_blocking(&mut self, key: &Bytes, req: LockRequest) -> Result<Locked> {
        run_now(self.lock_get_core(key, &req, How::Blocking))
    }

    /// READ COMMITTED's early unlock of a row that failed the `WHERE`: gives
    /// back the record lock if the current statement was the first to take
    /// it and the transaction hasn't written the row. At other isolation
    /// levels, and for locks from earlier statements, it does nothing.
    pub fn unlock(&mut self, key: &Bytes) {
        if self.opts.isolation != Isolation::ReadCommitted || self.writes.get(key).is_some() {
            return;
        }
        if let Some(pos) = self.stmt_fresh.iter().rposition(|k| k == key) {
            self.stmt_fresh.swap_remove(pos);
            self.xlocked.remove(key);
            self.shared().locks.unlock(self.id, key);
        }
    }

    /// Takes a gap lock on every place in `range` and no record lock: other
    /// transactions can't insert into the range until this one ends. It is
    /// granted at once (gap locks never wait). A foreign-key check that
    /// finds no parent row uses it. READ COMMITTED takes no gap locks, so
    /// there it does nothing.
    ///
    /// # Errors
    /// 1206 with the whole transaction rolled back.
    pub fn lock_gap(&mut self, range: &KeyRange) -> Result<()> {
        self.ensure_active()?;
        if !self.gap_locks() {
            return Ok(());
        }
        let lo = Pos::at(Bytes::copy_from_slice(&range.start));
        let hi = range
            .end
            .as_ref()
            .map_or(Pos::End, |e| Pos::at(Bytes::copy_from_slice(e)));
        self.gap(&lo, &hi, false, false).map(|_| ())
    }

    // ----- locking scans ------------------------------------------------------

    /// A locking range read (`SELECT … FOR UPDATE` / `FOR SHARE`, the read
    /// phase of `UPDATE` and `DELETE`): up to `limit` live rows of `range`
    /// in `dir` order, each the latest committed version (or this
    /// transaction's own write), each record-locked in `req.mode`.
    ///
    /// Under REPEATABLE READ and SERIALIZABLE the scanned stretch is also
    /// gap-locked: from the range's first bound to its end bound, or to the
    /// last row returned when `limit` stops the scan early. Until this
    /// transaction ends no other transaction can insert into that stretch,
    /// so repeating the read returns the same rows. A row another
    /// transaction has inserted but not committed is waited for. Under READ
    /// COMMITTED only the returned rows are locked.
    ///
    /// `SKIP LOCKED` leaves out rows another transaction has locked;
    /// `NOWAIT` and `WAIT n` apply to each lock. With snapshot isolation
    /// (MariaDB), `req.check_snapshot` raises 1020 for a row changed or
    /// deleted since the read view.
    ///
    /// # Errors
    /// As for [`Txn::lock`], or the store failed.
    pub async fn scan_lock(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        req: LockRequest,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_lock_core(range, dir, limit, req, None, false, How::Async)
            .await
    }

    /// Blocking form of [`Txn::scan_lock`].
    ///
    /// # Errors
    /// As for [`Txn::scan_lock`].
    pub fn scan_lock_blocking(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        req: LockRequest,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        run_now(self.scan_lock_core(range, dir, limit, req, None, false, How::Blocking))
    }

    /// [`Txn::scan_lock`] with the statement's `WHERE` as `keep(key,
    /// value)`: only rows it accepts are returned and counted against
    /// `limit`.
    ///
    /// Under REPEATABLE READ and SERIALIZABLE every scanned row stays
    /// locked, accepted or not, as `InnoDB` keeps them. Under READ COMMITTED
    /// a rejected row is unlocked again (if this statement locked it).
    ///
    /// `semi_consistent` asks for READ COMMITTED's semi-consistent read,
    /// which `UPDATE` and `DELETE` use when they scan the primary key: a row
    /// another transaction has locked is first judged on its latest
    /// committed version, and skipped without waiting if `keep` rejects it;
    /// if `keep` accepts it, the scan waits for the lock and judges the row
    /// again. It applies only under READ COMMITTED, and not to a MariaDB
    /// session with snapshot isolation on (MariaDB waits there); elsewhere
    /// the flag is ignored. `keep` may therefore be called twice for a row.
    ///
    /// # Errors
    /// As for [`Txn::scan_lock`].
    pub async fn scan_lock_where<F>(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        req: LockRequest,
        semi_consistent: bool,
        mut keep: F,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        let keep: RowPred<'_> = &mut keep;
        self.scan_lock_core(
            range,
            dir,
            limit,
            req,
            Some(keep),
            semi_consistent,
            How::Async,
        )
        .await
    }

    /// Blocking form of [`Txn::scan_lock_where`].
    ///
    /// # Errors
    /// As for [`Txn::scan_lock`].
    pub fn scan_lock_where_blocking<F>(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        req: LockRequest,
        semi_consistent: bool,
        mut keep: F,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        let keep: RowPred<'_> = &mut keep;
        run_now(self.scan_lock_core(
            range,
            dir,
            limit,
            req,
            Some(keep),
            semi_consistent,
            How::Blocking,
        ))
    }

    /// Whether READ COMMITTED scans of this session may judge a locked row
    /// on its committed version instead of waiting.
    fn semi_consistent_allowed(&self) -> bool {
        self.opts.isolation == Isolation::ReadCommitted
            && !(self.opts.persona == Persona::MariaDb && self.opts.snapshot_isolation)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn scan_lock_core(
        &mut self,
        range: &KeyRange,
        dir: Dir,
        limit: usize,
        req: LockRequest,
        mut keep: Option<RowPred<'_>>,
        semi_consistent: bool,
        how: How,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.ensure_active()?;
        self.check_interrupt()?;
        let gaps = self.gap_locks();
        let semi = semi_consistent && keep.is_some() && self.semi_consistent_allowed();
        let si = self.si_applies(&req);
        // A reader that skips locked rows doesn't wait for rows being
        // inserted either.
        let stop = req.wait != Wait::SkipLocked;
        let fwd = dir == Dir::Forward;
        let first = Pos::at(Bytes::copy_from_slice(&range.start));
        let last = range
            .end
            .as_ref()
            .map_or(Pos::End, |e| Pos::at(Bytes::copy_from_slice(e)));
        // The scan moves from `pos` towards `goal`.
        let (mut pos, goal) = if fwd { (first, last) } else { (last, first) };
        let edge = |key: &Bytes| {
            if fwd {
                Pos::after(key.clone())
            } else {
                Pos::at(key.clone())
            }
        };
        let between = |a: &Pos, b: &Pos| {
            if fwd {
                range_between(a, b)
            } else {
                range_between(b, a)
            }
        };
        let mut out: Vec<(Bytes, Bytes)> = Vec::new();
        while out.len() < limit {
            self.check_interrupt()?;
            let n = (limit - out.len()).min(SCAN_CHUNK);
            let Some(sub) = between(&pos, &goal) else {
                break;
            };
            let mut seq = self.shared().commit_seq();
            let (mut rows, mut exhausted) = self.scan_rows(&sub, dir, n, Ts::MAX, si)?;
            // How far this round looks: to the last row read, or to the
            // range's end when the rows ran out.
            let last_live = |rows: &[Seen]| {
                rows.iter()
                    .rev()
                    .find(|r| r.value.is_some())
                    .map(|r| edge(&r.key))
            };
            let mut reach = if exhausted {
                goal.clone()
            } else {
                last_live(&rows).unwrap_or_else(|| goal.clone())
            };
            let mut stopped: Option<Bytes> = None;
            if gaps {
                // Close the stretch to inserts before trusting what was
                // read from it.
                let (lo, hi) = if fwd { (&pos, &reach) } else { (&reach, &pos) };
                if let Some(k) = self.gap(lo, hi, !fwd, stop)? {
                    reach = if fwd {
                        Pos::at(k.clone())
                    } else {
                        Pos::after(k.clone())
                    };
                    stopped = Some(k);
                }
                let now = self.shared().commit_seq();
                if now == seq {
                    if let Some(p) = &stopped {
                        rows.retain(|r| if fwd { r.key < *p } else { r.key > *p });
                    }
                } else if let Some(held) = between(&pos, &reach) {
                    // A commit was queued between the read and the gap
                    // lock, and it may have inserted into the stretch. Read
                    // it again now that nothing more can be inserted.
                    seq = now;
                    let (again, done) = self.scan_rows(&held, dir, n, Ts::MAX, si)?;
                    rows = again;
                    if !done {
                        // More rows in the stretch than this round takes.
                        if let Some(r) = last_live(&rows) {
                            reach = r;
                        }
                        stopped = None;
                        exhausted = false;
                    }
                } else {
                    rows.clear();
                }
            }
            for Seen { key, ts, value } in rows {
                if out.len() >= limit {
                    break;
                }
                self.check_version(si, ts)?;
                let Some(mut val) = value else {
                    continue;
                };
                let mut grant = None;
                if semi {
                    grant = self.try_acquire(&key, req.mode)?;
                    if grant.is_none()
                        && let Some(k) = keep.as_mut()
                        && !(*k)(&key, &val)
                    {
                        // Locked by another transaction, and its committed
                        // version doesn't match: skip it without waiting.
                        continue;
                    }
                }
                let grant = match grant {
                    Some(g) => g,
                    None => match self.acquire(&key, req.mode, req.wait, how).await? {
                        Some(g) => g,
                        None => continue,
                    },
                };
                if grant.waited || self.shared().commit_seq() != seq {
                    // The row may have changed since it was read.
                    let (ts, now) = self.current_version(&key)?;
                    self.check_version(si, ts)?;
                    let Some(v) = now else {
                        // Gone meanwhile: nothing to keep a lock on (the
                        // gap lock, where there is one, covers its place).
                        self.release_new(&key, grant);
                        continue;
                    };
                    val = v;
                }
                if let Some(k) = keep.as_mut()
                    && !(*k)(&key, &val)
                {
                    if !gaps {
                        // READ COMMITTED gives back the lock on a row that
                        // fails the `WHERE`, if this visit took it.
                        self.release_new(&key, grant);
                    }
                    continue;
                }
                out.push((key, val));
            }
            if out.len() >= limit {
                break;
            }
            if let Some(p) = stopped {
                // Another transaction holds this key exclusively and may be
                // inserting it: wait for the key like for any locked row,
                // then see whether a row is there.
                if let Some(grant) = self.acquire(&p, req.mode, req.wait, how).await? {
                    self.gap(&Pos::at(p.clone()), &Pos::after(p.clone()), false, false)?;
                    let (ts, now) = self.current_version(&p)?;
                    self.check_version(si, ts)?;
                    if now.is_none() && self.writes.get(&p).is_none() {
                        // No row came of it (its inserter rolled back, or
                        // the key was only locked): the gap lock on its
                        // place is enough.
                        self.release_new(&p, grant);
                    }
                    if let Some(v) = now {
                        let kept = match keep.as_mut() {
                            Some(k) => (*k)(&p, &v),
                            None => true,
                        };
                        if kept {
                            out.push((p.clone(), v));
                        }
                    }
                }
                pos = edge(&p);
                continue;
            }
            if exhausted {
                break;
            }
            pos = reach;
        }
        Ok(out)
    }

    // ----- inserts --------------------------------------------------------------

    /// Whether no other transaction's gap lock covers `key` right now. With
    /// the key's X lock held this settles it: a range read that comes later
    /// stops at the lock.
    fn gap_free(&mut self, key: &Bytes) -> bool {
        let got = self
            .shared()
            .locks
            .insert_intent(self.id, &self.weight, key, false, false);
        matches!(got, Acquire::Granted { .. })
    }

    /// Marks `key` as having passed the insert's gap check in the current
    /// statement, so the `put` that follows doesn't repeat it.
    fn insert_passed(&mut self, key: &Bytes) {
        let seq = self.stmt_seq;
        if let Some(passed) = self.xlocked.get_mut(key) {
            *passed = seq;
        }
    }

    /// An insert's wait for the gap locks other transactions hold over
    /// `key` (its insert intention). Returns once those are gone; a new one
    /// may have appeared, so the caller checks again with the X lock held.
    async fn insert_wait(&mut self, key: &Bytes, wait: Wait, how: How) -> Result<()> {
        if let Some(passed) = self.xlocked.get_mut(key) {
            *passed = 0;
        }
        self.sync_weight();
        let got = self.shared().locks.insert_intent(
            self.id,
            &self.weight,
            key,
            self.may_queue(wait),
            self.opts.deadlock_detect,
        );
        self.settle(got, wait, how).await.map(|_| ())
    }

    /// Gives back every lock this transaction holds on `key`.
    fn drop_record(&mut self, key: &Bytes) {
        self.release_new(
            key,
            Grant {
                new: true,
                waited: false,
            },
        );
    }

    async fn lock_insert_core(
        &mut self,
        key: &Bytes,
        il: InsertLock,
        how: How,
    ) -> Result<InsertProbe> {
        self.ensure_active()?;
        let wait = if il.wait == Wait::SkipLocked {
            Wait::NoWait
        } else {
            il.wait
        };
        loop {
            self.check_interrupt()?;
            let held_x = self.xlocked.contains_key(key);
            let mut mode = LockMode::Exclusive;
            // Whether this round put the transaction's first lock on the
            // key (so it may be given back while waiting).
            let mut created = false;
            if !held_x {
                self.sync_weight();
                let (got, asked) = self.shared().locks.acquire_insert_probe(
                    self.id,
                    &self.weight,
                    key,
                    il.dup,
                    self.may_queue(wait),
                    self.opts.deadlock_detect,
                );
                mode = asked;
                if let Some(g) = self.settle(got, wait, how).await? {
                    self.note_grant(key, mode, g);
                    created = g.new;
                }
            }
            if il.check_snapshot {
                let req = LockRequest {
                    mode,
                    wait,
                    check_snapshot: true,
                };
                self.check_snapshot(key, &req)?;
            }
            if let Some(v) = self.read_current(key)? {
                // The key is taken. A plain insert keeps a shared lock on
                // the row it collided with, unless this transaction already
                // held it exclusively.
                if !held_x && mode == LockMode::Exclusive && il.dup == LockMode::Shared {
                    self.xlocked.remove(key);
                    self.shared().locks.downgrade(self.id, key);
                }
                return Ok(InsertProbe::Exists(v));
            }
            if mode != LockMode::Exclusive {
                // Absent, and others are waiting on the key too: the insert
                // needs it exclusively. Two transactions doing this at once
                // deadlock, as they do in `InnoDB`.
                self.acquire(key, LockMode::Exclusive, wait, how).await?;
            }
            if self.writes.get(key).is_some() {
                // A key this transaction deleted and now writes again is
                // not a new row to anyone else: its lock never left.
                return Ok(InsertProbe::Free);
            }
            if self.gap_free(key) {
                self.insert_passed(key);
                if created {
                    self.inserted.push((self.writes.mark(), key.clone()));
                }
                return Ok(InsertProbe::Free);
            }
            // Another transaction's gap lock covers the key. Nobody waits
            // for a waiting insert, so the key's lock is given back for the
            // wait (unless the transaction held it before this call); when
            // the gap is gone the insert starts over, duplicate check
            // included.
            if created {
                self.drop_record(key);
            }
            self.insert_wait(key, wait, how).await?;
        }
    }

    /// The locking an `INSERT` does before it writes `key` (a primary-key
    /// or unique-index entry):
    ///
    /// 1. **The duplicate check.** If the key is present the answer is
    ///    [`InsertProbe::Exists`], and the lock the check took is kept until
    ///    the transaction ends, even though the statement fails with 1062:
    ///    shared for [`InsertLock::insert`], exclusive for
    ///    [`InsertLock::replace`].
    /// 2. **The insert intention.** If the key is absent, the transaction
    ///    takes its exclusive lock and waits while another transaction's gap
    ///    lock covers the key. Then the answer is [`InsertProbe::Free`] and
    ///    the caller writes the row with [`Txn::put`].
    ///
    /// A transaction that only wants to write, and doesn't care whether the
    /// key existed, can skip this and call `put`: `put` makes the same wait
    /// itself, on the calling thread.
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub async fn lock_insert(&mut self, key: &Bytes, how: InsertLock) -> Result<InsertProbe> {
        self.lock_insert_core(key, how, How::Async).await
    }

    /// Blocking form of [`Txn::lock_insert`].
    ///
    /// # Errors
    /// As for [`Txn::lock`].
    pub fn lock_insert_blocking(&mut self, key: &Bytes, how: InsertLock) -> Result<InsertProbe> {
        run_now(self.lock_insert_core(key, how, How::Blocking))
    }

    /// `put`'s own insert check, for callers that didn't use
    /// [`Txn::lock_insert`]: if another transaction's gap lock covers `key`
    /// and the key is absent, wait on this thread. `took`: `put` itself
    /// took the key's lock just now, so it can be given back for the wait.
    fn insert_check(&mut self, key: &Bytes, took: bool) -> Result<()> {
        if self.writes.get(key).is_some() {
            // Already written by this transaction: not a new row.
            return Ok(());
        }
        loop {
            if self.gap_free(key) {
                return Ok(());
            }
            // Covered by a gap lock. Only a new row waits for it; changing
            // a row that exists doesn't.
            if self.latest(key)?.is_some_and(|(_, v)| v.is_some()) {
                return Ok(());
            }
            if took {
                self.drop_record(key);
            }
            run_now(self.insert_wait(key, Wait::Block, How::Blocking))?;
            if took {
                self.lock_blocking(key, LockRequest::write())?;
            }
        }
    }

    // ----- writes ---------------------------------------------------------

    fn write_entry(&mut self, key: Bytes, value: Option<Bytes>) -> Result<()> {
        self.ensure_active()?;
        let (passed, took) = if let Some(passed) = self.xlocked.get(&key) {
            (*passed == self.stmt_seq, false)
        } else {
            // A blind write: take the lock here. Async callers lock first.
            self.lock_blocking(&key, LockRequest::write())?;
            (false, true)
        };
        if !passed && value.is_some() && self.shared().locks.has_gaps() {
            self.insert_check(&key, took)?;
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

    /// Adds an opaque note to the commit's log entry (the apply step's
    /// observers read it, the store doesn't; a stream append in a SQL
    /// transaction). It goes with the transaction's writes or not at all,
    /// and a statement or savepoint rollback removes it. Notes don't
    /// survive `XA PREPARE`.
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn add_note(&mut self, note: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        self.notes.push(Extra::Note(None, note));
        Ok(())
    }

    /// Adds a note to the log entry of the unit holding `tenant`'s keys
    /// (a stream append of that tenant), as [`Txn::add_note`] adds one to
    /// the notes unit's entry.
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn add_note_for_tenant(&mut self, tenant: u64, note: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        self.notes.push(Extra::Note(Some(tenant), note));
        Ok(())
    }

    /// The unit whose entry carries note `e`.
    fn note_unit(&self, e: &Extra) -> Option<u64> {
        match e {
            Extra::Note(None, _) => Some(self.path.notes_unit()),
            Extra::Note(Some(t), _) => Some(self.path.unit_of(&t.to_be_bytes())),
            _ => None,
        }
    }

    /// The notes added so far for `tenant`'s unit: the next such note's
    /// slot among that unit's entry's notes.
    #[must_use]
    pub fn notes_for_tenant(&self, tenant: u64) -> usize {
        let u = Some(self.path.unit_of(&tenant.to_be_bytes()));
        self.notes.iter().filter(|e| self.note_unit(e) == u).count()
    }

    /// Adds a plain (unversioned) put of `key` in `cf` to the commit's log
    /// entry: applied with the transaction's writes or not at all, never
    /// visible before the commit applies, not locked and not versioned
    /// (keys outside MVCC that one owner writes, such as queue messages: a
    /// queue publish or acknowledgement inside a SQL transaction). A
    /// statement or savepoint rollback removes it, as it removes notes.
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn add_plain_put(&mut self, cf: Cf, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        self.notes.push(Extra::Put(cf, key, value));
        Ok(())
    }

    /// Adds a plain (unversioned) delete of `key` in `cf` to the commit's
    /// log entry, as [`Txn::add_plain_put`] adds a put.
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn add_plain_delete(&mut self, cf: Cf, key: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        self.notes.push(Extra::Delete(cf, key));
        Ok(())
    }

    /// Whether the commit carries plain store writes (they can't be kept
    /// across `XA PREPARE`).
    #[must_use]
    pub fn has_plain_writes(&self) -> bool {
        self.notes.iter().any(|e| !matches!(e, Extra::Note(..)))
    }

    /// The notes added so far (plain writes not counted: a note's slot
    /// among the entry's notes is what observers see).
    #[must_use]
    pub fn notes(&self) -> usize {
        let u = Some(self.path.notes_unit());
        self.notes.iter().filter(|e| self.note_unit(e) == u).count()
    }

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
        self.check_doomed()?;
        let token = self.view.unwrap_or_else(|| self.shared().visible_ts());
        self.close_view();
        if self.writes.is_empty() && self.notes.is_empty() {
            self.shared().locks.release_all(self.id);
            self.finish(Phase::Done);
            return Ok(CommitTicket::new(
                None,
                token,
                std::mem::take(&mut self.dep),
            ));
        }
        let writes = std::mem::take(&mut self.writes).into_writes();
        let notes = std::mem::take(&mut self.notes);
        let path = self.path.clone();
        match path.shared().commit_full(
            &*path,
            self.id,
            writes,
            self.opts.durability,
            None,
            &notes,
            &self.dep,
        ) {
            Ok(done) => {
                self.finish(Phase::Done);
                Ok(CommitTicket {
                    cts: Some(done.cts),
                    token: done.cts,
                    ticket: done.ticket.max(std::mem::take(&mut self.dep)),
                    later: done.later,
                })
            }
            // A commit across units whose coordinator stopped part way: its
            // locks stay until a resolver or recovery finishes it.
            Err(e) if path.shared().cross_holds_locks(self.id) => {
                self.detach();
                self.finish(Phase::Aborted(AbortReason::LeaderChange));
                Err(e)
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
        self.notes.clear();
        self.xlocked.clear();
        self.stmt_fresh.clear();
        self.inserted.clear();
        self.close_view();
        self.shared().locks.release_all(self.id);
        self.shared()
            .stats
            .rollbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Forgets the session's side of a transaction that lives on without
    /// it (prepared): its writes, its read view and what it knew of its
    /// locks. The locks themselves stay in the lock table.
    pub(super) fn detach(&mut self) {
        self.writes = WriteSet::default();
        self.xlocked.clear();
        self.stmt_fresh.clear();
        self.inserted.clear();
        self.close_view();
    }

    /// Ends the transaction for a transaction-level error.
    pub(super) fn abort(&mut self, reason: AbortReason, e: SprigError) -> SprigError {
        if self.phase == Phase::Active {
            self.rollback_inner();
            self.finish(Phase::Aborted(reason));
        }
        e
    }

    pub(super) fn finish(&mut self, phase: Phase) {
        if self.phase == Phase::Active {
            self.shared().txn_closed();
        }
        self.phase = phase;
    }
}

/// A store scan's next visible version ([`VersionScan`]), as a trait so a
/// scan made for one range and a multi scan lent by the caller
/// ([`Txn::scan_many`]) are used alike.
trait Visible {
    fn next_visible(&mut self) -> Result<Option<(Bytes, super::mvcc::Version)>>;
}

impl Visible for VersionScan<'_> {
    fn next_visible(&mut self) -> Result<Option<(Bytes, super::mvcc::Version)>> {
        VersionScan::next_visible(self)
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
