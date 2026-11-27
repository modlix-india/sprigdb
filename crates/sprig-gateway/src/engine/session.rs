//! A session's transaction on the engine.
//!
//! ```text
//!            BEGIN / first statement with autocommit = 0
//!   (none) ───────────────────────────────────────────────► open
//!     ▲                                                       │
//!     │  COMMIT, ROLLBACK, an implicit commit,                │ statements,
//!     │  1213 / 1020 / 1206 (the engine rolled it back),      │ savepoints
//!     └───────────────────────────────────────────────────────┘
//! ```
//!
//! - A statement that fails undoes its own writes only; the transaction
//!   stays open and keeps the locks the statement took.
//! - Deadlock (1213), a snapshot conflict (1020), a full lock table (1206)
//!   and, with `innodb_rollback_on_timeout`, a lock wait timeout (1205)
//!   roll the whole transaction back: the session is then outside a
//!   transaction, even after `BEGIN`.
//! - `SET TRANSACTION` without a scope sets the next transaction's
//!   isolation level and access mode, once.
//! - Savepoint names compare without regard to case; setting a name again
//!   moves it. `ROLLBACK TO` keeps the locks taken since the savepoint,
//!   with one exception both servers have: a savepoint set before the
//!   transaction's first table statement stands for "nothing done yet",
//!   and rolling back to it releases every lock (the engine transaction
//!   starts afresh; the session's transaction stays open).

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_common::{Result, SprigError};
#[cfg(test)]
use sprig_engine::txn::Isolation;
use sprig_engine::txn::{CommitPath, Savepoint, Txn, TxnOptions};

use super::Engine;
use super::control::{Level, Xid};
use super::store::Tx;

/// 1305: no savepoint of that name.
pub(crate) fn no_savepoint(name: &str) -> SprigError {
    SprigError::standard(1305, *b"42000", format!("SAVEPOINT {name} does not exist"))
}

/// 1792: a write in a read-only transaction (`mysql`: in MySQL's wording,
/// which ends with a full stop).
pub(crate) fn read_only_transaction(mysql: bool) -> SprigError {
    SprigError::standard(
        1792,
        *b"25006",
        if mysql {
            "Cannot execute statement in a READ ONLY transaction."
        } else {
            "Cannot execute statement in a READ ONLY transaction"
        },
    )
}

/// 1568: `SET TRANSACTION` inside a transaction.
pub(crate) fn characteristics_in_transaction() -> SprigError {
    SprigError::standard(
        1568,
        *b"25001",
        "Transaction characteristics can't be changed while a transaction is in progress",
    )
}

/// 1399: an XA statement, or a statement an XA branch doesn't allow, in
/// the branch's `state` (as the servers word it, two spaces included).
pub(crate) fn xa_rmfail(state: &str) -> SprigError {
    SprigError::standard(
        1399,
        *b"XAE07",
        format!(
            "XAER_RMFAIL: The command cannot be executed when global transaction is in the  {state} state"
        ),
    )
}

/// 1397: no such branch.
pub(crate) fn xa_nota() -> SprigError {
    SprigError::standard(1397, *b"XAE04", "XAER_NOTA: Unknown XID")
}

/// 1398: a form neither server runs (`JOIN`, `SUSPEND`, an empty gtrid on
/// MariaDB, `ONE PHASE` on a prepared branch).
pub(crate) fn xa_inval() -> SprigError {
    SprigError::standard(
        1398,
        *b"XAE05",
        "XAER_INVAL: Invalid arguments (or unsupported command)",
    )
}

/// 1400: an XA statement while the session is in a transaction of its own.
pub(crate) fn xa_outside() -> SprigError {
    SprigError::standard(
        1400,
        *b"XAE09",
        "XAER_OUTSIDE: Some work is done outside global transaction",
    )
}

/// 1440: the xid is taken.
pub(crate) fn xa_dupid() -> SprigError {
    SprigError::standard(1440, *b"XAE08", "XAER_DUPID: The XID already exists")
}

/// 1614: MySQL's answer to a statement in a branch a deadlock rolled back.
pub(crate) fn xa_rbdeadlock() -> SprigError {
    SprigError::standard(
        1614,
        *b"XA102",
        "XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected",
    )
}

/// Where a session's XA branch stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XaPhase {
    /// After `XA START`: statements run in it.
    Active,
    /// After `XA END`.
    Idle,
    /// After `XA PREPARE`, on MariaDB's port, where the branch stays with
    /// its session until that session finishes it or leaves. (On MySQL's
    /// port a prepared branch belongs to no session.)
    Prepared,
    /// A deadlock (or a snapshot conflict) rolled the branch back: only
    /// `XA ROLLBACK` is left.
    RollbackOnly,
}

impl XaPhase {
    /// The state's name in 1399.
    pub(crate) fn name(self) -> &'static str {
        match self {
            XaPhase::Active => "ACTIVE",
            XaPhase::Idle => "IDLE",
            XaPhase::Prepared => "PREPARED",
            XaPhase::RollbackOnly => "ROLLBACK ONLY",
        }
    }
}

/// A session's XA branch.
#[derive(Debug)]
struct Branch {
    xid: Xid,
    /// Its key in the engine (the tenant's, see [`Xid::engine_key`]).
    key: Vec<u8>,
    phase: XaPhase,
}

/// The branches sessions of this node hold right now, by engine key: an
/// xid one of them uses is taken (1440), and a prepared branch still with
/// its session can't be finished from another one (1397), as on MariaDB.
/// Branches that are only prepared (after a restart, or left by their
/// session) are the engine's alone.
static XA_HELD: Mutex<Option<HashSet<Vec<u8>>>> = Mutex::new(None);

fn xa_held<R>(f: impl FnOnce(&mut HashSet<Vec<u8>>) -> R) -> R {
    let mut g = XA_HELD.lock().unwrap_or_else(PoisonError::into_inner);
    f(g.get_or_insert_with(HashSet::new))
}

/// The engine as the XA calls take it.
fn xa_path(e: &Engine) -> Arc<dyn CommitPath> {
    Arc::new(e.eng.clone())
}

/// Whether a branch with engine key `key` exists: held by a session, or
/// prepared in the engine.
pub(crate) fn xa_taken(e: &Engine, key: &[u8]) -> bool {
    xa_held(|h| h.contains(key)) || Txn::xa_exists(&xa_path(e), key)
}

/// `XA COMMIT` (`commit`) or `XA ROLLBACK` of a prepared branch no session
/// holds, by its engine key; waits until it is durable.
///
/// # Errors
/// 1397 when there is no such branch, or a session still holds it; the
/// log's.
pub(crate) async fn xa_finish(e: &Engine, key: &[u8], commit: bool) -> Result<()> {
    if xa_held(|h| h.contains(key)) {
        return Err(xa_nota());
    }
    finish_prepared(e, key, commit).await
}

async fn finish_prepared(e: &Engine, key: &[u8], commit: bool) -> Result<()> {
    let path = xa_path(e);
    if commit {
        Txn::xa_commit(&path, key)?.wait_async().await
    } else {
        Txn::xa_rollback(&path, key)?.wait_async().await
    }
}

/// The prepared branches of `tenant`, in key order (`XA RECOVER`).
pub(crate) fn xa_recover(e: &Engine, tenant: u64) -> Vec<Xid> {
    Txn::xa_recover(&xa_path(e))
        .iter()
        .filter_map(|k| Xid::from_engine_key(tenant, k))
        .collect()
}

/// How a transaction starts.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Start {
    /// `READ ONLY` / `READ WRITE`, when the statement says.
    pub read_only: Option<bool>,
    /// `WITH CONSISTENT SNAPSHOT`.
    pub snapshot: bool,
}

/// The session's transaction on the engine, and what the next one starts
/// with.
#[allow(clippy::struct_excessive_bools)] // independent facts of one transaction
#[derive(Debug, Default)]
pub(crate) struct SessionTxn {
    tx: Option<Tx>,
    read_only: bool,
    /// The options the open transaction started with.
    opts: Option<TxnOptions>,
    /// A statement read or wrote a table in the open transaction (or it
    /// started with a consistent snapshot).
    touched: bool,
    /// Each savepoint, and whether it was set before the transaction
    /// touched anything.
    savepoints: Vec<(String, Savepoint, bool)>,
    /// `SET TRANSACTION ISOLATION LEVEL` for the next transaction.
    next_isolation: Option<Level>,
    /// `SET TRANSACTION READ ONLY | READ WRITE` for the next transaction.
    next_read_only: Option<bool>,
    /// A statement is running: a request dropped in the middle (a killed
    /// query) leaves this set, and its writes are undone before anything
    /// else runs.
    in_flight: bool,
    /// Opened by a `SAVEPOINT` under `autocommit = 0`, before any table
    /// statement: the servers keep the savepoint but report no
    /// transaction until a statement reads or writes a table.
    quiet: bool,
    /// The session's XA branch, if it has one. While it is `Active` or
    /// `Idle` the branch is the open transaction (`tx`).
    xa: Option<Branch>,
}

impl SessionTxn {
    /// Whether a transaction is open.
    pub(crate) fn is_open(&self) -> bool {
        self.tx.is_some()
    }

    /// Whether a transaction is open that the client is told about (the
    /// status flags of every answer): also a prepared XA branch the
    /// session still holds (MariaDB's port).
    pub(crate) fn reported(&self) -> bool {
        (self.tx.is_some() && !self.quiet)
            || self
                .xa
                .as_ref()
                .is_some_and(|b| b.phase == XaPhase::Prepared)
    }

    /// The phase of the session's XA branch, if it holds one.
    pub(crate) fn xa_phase(&self) -> Option<XaPhase> {
        self.xa.as_ref().map(|b| b.phase)
    }

    /// Whether `xid` is the session's own branch.
    pub(crate) fn xa_is(&self, xid: &Xid) -> bool {
        self.xa.as_ref().is_some_and(|b| b.xid == *xid)
    }

    /// `XA START`: a branch `xid` (engine key `key`, which the caller found
    /// free) starts as the session's transaction.
    ///
    /// # Errors
    /// 1440 when another session took the xid meanwhile; the engine's.
    pub(crate) fn xa_start(
        &mut self,
        e: &Engine,
        base: TxnOptions,
        session_read_only: bool,
        xid: Xid,
        key: Vec<u8>,
    ) -> Result<()> {
        if !xa_held(|h| h.insert(key.clone())) {
            return Err(xa_dupid());
        }
        if let Err(err) = self.begin(e, base, session_read_only, Start::default()) {
            xa_held(|h| h.remove(&key));
            return Err(err);
        }
        self.touched = true;
        self.xa = Some(Branch {
            xid,
            key,
            phase: XaPhase::Active,
        });
        Ok(())
    }

    /// `XA END` of the session's active branch.
    pub(crate) fn xa_end(&mut self) {
        if let Some(b) = self.xa.as_mut() {
            b.phase = XaPhase::Idle;
        }
    }

    /// `XA PREPARE` of the session's idle branch: its writes and exclusive
    /// locks are durable under its xid when this returns. `keep`: the
    /// branch stays with the session (MariaDB's port); otherwise the
    /// session is free of it (MySQL's).
    ///
    /// # Errors
    /// The log's: the branch is rolled back and gone (the caller answers
    /// 1402).
    pub(crate) async fn xa_prepare(&mut self, keep: bool) -> Result<()> {
        let Some(mut b) = self.xa.take() else {
            return Ok(());
        };
        let tx = self.tx.take();
        self.forget();
        let Some(mut tx) = tx else {
            xa_held(|h| h.remove(&b.key));
            return Err(SprigError::internal(
                "engine: an XA branch without its transaction",
            ));
        };
        let done = match tx.xa_prepare(&b.key) {
            Ok(ticket) => ticket.wait_async().await,
            Err(e) => Err(e),
        };
        if let Err(e) = done {
            tx.rollback();
            xa_held(|h| h.remove(&b.key));
            return Err(e);
        }
        if keep {
            b.phase = XaPhase::Prepared;
            self.xa = Some(b);
        } else {
            xa_held(|h| h.remove(&b.key));
        }
        Ok(())
    }

    /// Ends the session's own branch: `commit` (`XA COMMIT`, with `ONE
    /// PHASE` from the idle state) or roll back (`XA ROLLBACK`). The caller
    /// checked that the phase allows it.
    ///
    /// # Errors
    /// The log's.
    pub(crate) async fn xa_finish_own(&mut self, e: &Engine, commit: bool) -> Result<()> {
        let Some(b) = self.xa.take() else {
            return Ok(());
        };
        let r = match b.phase {
            XaPhase::Prepared => finish_prepared(e, &b.key, commit).await,
            XaPhase::Active | XaPhase::Idle if commit => self.commit().await.map(|_| ()),
            XaPhase::Active | XaPhase::Idle | XaPhase::RollbackOnly => {
                self.rollback();
                Ok(())
            }
        };
        xa_held(|h| h.remove(&b.key));
        r
    }

    /// The session ends, is reset or changes its user: an active or idle
    /// branch is rolled back with its transaction; a prepared one stays in
    /// the engine for any session to finish.
    pub(crate) fn xa_leave(&mut self) {
        if let Some(b) = self.xa.take() {
            xa_held(|h| h.remove(&b.key));
        }
    }

    /// Whether the open transaction is read-only.
    pub(crate) fn read_only(&self) -> bool {
        self.tx.is_some() && self.read_only
    }

    /// The open transaction's isolation level.
    #[cfg(test)]
    pub(crate) fn isolation(&self) -> Option<Isolation> {
        self.tx.as_ref().map(|t| t.options().isolation)
    }

    /// `SET TRANSACTION` without a scope: the next transaction's
    /// characteristics.
    ///
    /// # Errors
    /// 1568 inside a transaction.
    pub(crate) fn set_next(
        &mut self,
        isolation: Option<Level>,
        read_only: Option<bool>,
    ) -> Result<()> {
        if self.is_open() {
            return Err(characteristics_in_transaction());
        }
        if isolation.is_some() {
            self.next_isolation = isolation;
        }
        if read_only.is_some() {
            self.next_read_only = read_only;
        }
        Ok(())
    }

    /// Forgets the next transaction's characteristics (a session-scope
    /// `SET` of the same variable replaces them, as the servers do).
    pub(crate) fn clear_next(&mut self) {
        self.next_isolation = None;
        self.next_read_only = None;
    }

    /// Starts a transaction with the session's options `base` and access
    /// mode `session_read_only`, overridden by the next-transaction
    /// characteristics and by `start`. An open transaction must have been
    /// ended first.
    ///
    /// # Errors
    /// The engine's.
    pub(crate) fn begin(
        &mut self,
        e: &Engine,
        base: TxnOptions,
        session_read_only: bool,
        start: Start,
    ) -> Result<()> {
        let mut opts = base;
        if let Some(l) = self.next_isolation.take() {
            opts.isolation = l.engine();
        }
        let read_only = start
            .read_only
            .or(self.next_read_only.take())
            .unwrap_or(session_read_only);
        opts.read_only = read_only;
        opts.consistent_snapshot = start.snapshot;
        let tx = e.begin_session(opts)?;
        self.tx = Some(tx);
        self.read_only = read_only;
        self.opts = Some(opts);
        self.touched = start.snapshot;
        self.savepoints.clear();
        self.in_flight = false;
        self.quiet = false;
        Ok(())
    }

    /// Starts the transaction a `SAVEPOINT` under `autocommit = 0` belongs
    /// to, unreported until its first table statement.
    ///
    /// # Errors
    /// The engine's.
    pub(crate) fn begin_quiet(
        &mut self,
        e: &Engine,
        base: TxnOptions,
        session_read_only: bool,
    ) -> Result<()> {
        self.begin(e, base, session_read_only, Start::default())?;
        self.quiet = true;
        Ok(())
    }

    /// The open transaction, for a statement. A statement that was dropped
    /// in the middle has its writes undone first.
    pub(crate) fn statement(&mut self) -> Option<&mut Tx> {
        let tx = self.tx.as_mut()?;
        if self.in_flight {
            tx.rollback_statement();
        }
        self.in_flight = true;
        self.quiet = false;
        self.touched = true;
        Some(tx)
    }

    /// The open transaction, for a setting made before a statement starts
    /// (its `sql_mode` and time zone): unlike [`SessionTxn::statement`] it
    /// starts nothing, so it never undoes the previous statement's writes.
    pub(crate) fn open_tx(&mut self) -> Option<&mut Tx> {
        self.tx.as_mut()
    }

    /// Hands the open transaction to a statement that must own it while
    /// it runs (a plan whose storage holds the transaction). The caller
    /// gives it back with [`SessionTxn::restore`] before anything is
    /// awaited, so the session is never seen without it.
    pub(crate) fn lend(&mut self) -> Option<Tx> {
        self.tx.take()
    }

    /// Takes back the transaction [`SessionTxn::lend`] handed out.
    pub(crate) fn restore(&mut self, tx: Tx) -> &mut Tx {
        self.tx.insert(tx)
    }

    /// Opens the session's transaction over `tx`, an engine transaction
    /// another front end lent (a MongoDB transaction's, for an SQL statement
    /// on a table): the session's statements run in it until
    /// [`SessionTxn::take_lent`]. `Some(tx)` back when the session has a
    /// transaction (or an XA branch) of its own.
    pub(crate) fn adopt_lent(&mut self, tx: Tx) -> Option<Tx> {
        if self.tx.is_some() || self.xa.is_some() {
            return Some(tx);
        }
        self.opts = Some(*tx.options());
        self.read_only = false;
        self.touched = true;
        self.savepoints.clear();
        self.in_flight = false;
        self.quiet = false;
        self.tx = Some(tx);
        None
    }

    /// Takes back the transaction [`SessionTxn::adopt_lent`] opened;
    /// `None` when the engine rolled it back meanwhile (a deadlock, a
    /// snapshot conflict).
    pub(crate) fn take_lent(&mut self) -> Option<Tx> {
        let tx = self.tx.take();
        self.forget();
        tx.filter(Tx::is_open)
    }

    /// The statement ended (with an answer or an error). When the engine
    /// rolled the transaction back by itself, the session is outside a
    /// transaction now: `true`.
    pub(crate) fn statement_done(&mut self) -> bool {
        self.in_flight = false;
        if self.tx.as_ref().is_some_and(|t| !t.is_open()) {
            self.forget();
            // An XA branch stays, with nothing left but `XA ROLLBACK`.
            if let Some(b) = self.xa.as_mut() {
                b.phase = XaPhase::RollbackOnly;
            }
            return true;
        }
        false
    }

    /// A request was dropped (a killed query): the running statement's
    /// writes are undone, the transaction stays open with its locks.
    pub(crate) fn abandon_statement(&mut self) {
        if self.in_flight {
            if let Some(tx) = self.tx.as_mut() {
                tx.rollback_statement();
            }
            self.in_flight = false;
            if self.tx.as_ref().is_some_and(|t| !t.is_open()) {
                self.forget();
                if let Some(b) = self.xa.as_mut() {
                    b.phase = XaPhase::RollbackOnly;
                }
            }
        }
    }

    fn forget(&mut self) {
        self.tx = None;
        self.savepoints.clear();
        self.read_only = false;
        self.in_flight = false;
        self.quiet = false;
        self.opts = None;
        self.touched = false;
    }

    /// Commits the open transaction, if any, and waits until it is
    /// durable. Returns the options it ran with (for `AND CHAIN`).
    ///
    /// # Errors
    /// The log refused the commit or failed before it was durable: the
    /// transaction is gone either way.
    pub(crate) async fn commit(&mut self) -> Result<Option<(TxnOptions, bool)>> {
        let Some(mut tx) = self.tx.take() else {
            return Ok(None);
        };
        if self.in_flight {
            tx.rollback_statement();
        }
        let ended = (*tx.options(), self.read_only);
        self.forget();
        if !tx.is_open() {
            return Ok(Some(ended));
        }
        tx.finish().await?;
        Ok(Some(ended))
    }

    /// Rolls the open transaction back, if any: its writes go and its
    /// locks are released. Returns the options it ran with.
    pub(crate) fn rollback(&mut self) -> Option<(TxnOptions, bool)> {
        let tx = self.tx.take()?;
        let ended = (*tx.options(), self.read_only);
        self.forget();
        tx.rollback();
        Some(ended)
    }

    /// `AND CHAIN`: a new transaction with the characteristics of the one
    /// that just ended (`ended`), or, when none was open, the session's
    /// (`base`, `session_read_only`): both servers start one either way.
    ///
    /// # Errors
    /// The engine's.
    pub(crate) fn chain(
        &mut self,
        e: &Engine,
        ended: Option<(TxnOptions, bool)>,
        base: TxnOptions,
        session_read_only: bool,
    ) -> Result<()> {
        let Some((mut opts, read_only)) = ended else {
            return self.begin(e, base, session_read_only, Start::default());
        };
        opts.consistent_snapshot = false;
        let tx = e.begin_session(opts)?;
        self.tx = Some(tx);
        self.read_only = read_only;
        self.opts = Some(opts);
        self.touched = false;
        Ok(())
    }

    fn position(&self, name: &str) -> Option<usize> {
        self.savepoints
            .iter()
            .position(|(n, _, _)| n.eq_ignore_ascii_case(name))
    }

    /// `SAVEPOINT name` in the open transaction (`false`: none is open).
    pub(crate) fn savepoint(&mut self, name: &str) -> bool {
        let Some(tx) = self.tx.as_ref() else {
            return false;
        };
        let sp = tx.savepoint();
        if let Some(i) = self.position(name) {
            self.savepoints.remove(i);
        }
        self.savepoints.push((name.to_owned(), sp, !self.touched));
        true
    }

    /// `ROLLBACK TO [SAVEPOINT] name`: later writes go, later savepoints
    /// are forgotten, this one stays. The locks taken since stay too,
    /// unless the savepoint was set before the transaction touched
    /// anything: then every lock is released (see the module notes).
    ///
    /// # Errors
    /// 1305 when there is no such savepoint; the engine's.
    pub(crate) fn rollback_to(&mut self, e: &Engine, name: &str) -> Result<()> {
        let i = self.position(name).ok_or_else(|| no_savepoint(name))?;
        let (sp, fresh) = (self.savepoints[i].1, self.savepoints[i].2);
        self.savepoints.truncate(i + 1);
        if fresh && let Some(opts) = self.opts {
            // Nothing was done before it: the engine transaction starts
            // again, without the locks and the read view of the old one.
            if let Some(old) = self.tx.take() {
                old.rollback();
            }
            let mut opts = opts;
            opts.consistent_snapshot = false;
            let tx = e.begin_session(opts)?;
            // Earlier savepoints (all fresh) now mark the new start.
            let start = tx.savepoint();
            for s in &mut self.savepoints {
                s.1 = start;
            }
            self.tx = Some(tx);
            self.touched = false;
            self.in_flight = false;
            return Ok(());
        }
        if let Some(tx) = self.tx.as_mut() {
            tx.rollback_to(sp);
        }
        Ok(())
    }

    /// `RELEASE SAVEPOINT name`: this savepoint and later ones are
    /// forgotten; nothing is undone.
    ///
    /// # Errors
    /// 1305 when there is no such savepoint.
    pub(crate) fn release(&mut self, name: &str) -> Result<()> {
        let i = self.position(name).ok_or_else(|| no_savepoint(name))?;
        self.savepoints.truncate(i);
        Ok(())
    }
}
