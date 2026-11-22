//! The engine as the SQL backend sees it: a transaction on `sprig_engine`,
//! one statement's (autocommit) or the session's.
//!
//! - **Plain reads** ([`Tx::get`], [`Tx::scan`]) are consistent reads at the
//!   transaction's read view, with its own writes merged in. They take no
//!   lock.
//! - **Current reads** ([`Tx::lock_read`], [`Tx::scan_lock_where`],
//!   [`Tx::scan_lock_entries`]) lock, waiting on the runtime, and return
//!   the latest committed version (InnoDB's current read) with the
//!   transaction's own writes. At REPEATABLE READ and SERIALIZABLE a scan
//!   also locks the gaps it read, and a missing row's place. They never
//!   open the transaction's read view. An autocommit statement opens no
//!   earlier read view, so the snapshot-isolation check (1020) doesn't
//!   apply to it; in a session's transaction it applies to locking
//!   accesses of rows, as on MariaDB.
//! - **Writes** ([`Tx::put`], [`Tx::put_new`], [`Tx::insert`],
//!   [`Tx::delete`]) take the key's exclusive lock first; a new key also
//!   waits for other transactions' gap locks over its place.
//! - **Statements** of a session's transaction are atomic
//!   ([`Tx::begin_statement`], [`Tx::rollback_statement`]): a failed
//!   statement's writes go, its locks stay, the transaction stays open.
//!   Deadlock (1213), a snapshot conflict (1020) and a full lock table
//!   (1206) end the whole transaction ([`Tx::is_open`] then says so).
//! - **The end** ([`Tx::finish`]): the commit is queued in the log, the
//!   locks are released at once (early lock release), and the reply waits
//!   until the commit, and every pending version the statement read, is
//!   durable. Between statements the reply waits the same way for what
//!   the statement read ([`Tx::gate`]).

use bytes::Bytes;
use sprig_common::Result;
use sprig_engine::keys::KeyRange;
use sprig_engine::txn::{
    Dir, InsertLock, InsertProbe, Isolation, LockMode, LockRequest, Locked, ReadMode, Savepoint,
    Txn, TxnOptions, Wait,
};

use std::sync::Arc;

use super::assign::Env;
use super::keys::Space;
use super::table::TableDef;

/// DML's lock on an index entry, and on a row in an autocommit statement:
/// exclusive, waiting, without the snapshot check.
const CURRENT: LockRequest = LockRequest {
    mode: LockMode::Exclusive,
    wait: Wait::Block,
    check_snapshot: false,
};

/// A statement's or a session's transaction.
pub(crate) struct Tx {
    t: Txn,
    /// A session's transaction (several statements): locking accesses of
    /// rows make the snapshot-isolation check.
    session: bool,
    /// The session's rules for storing values, as last given.
    env: Env,
    /// The tables a session's transaction used, with their keyspaces: it
    /// keeps using them until it ends, and a definition change that copies
    /// a table's rows waits for it (as it waits for a metadata lock on
    /// the servers).
    held: Vec<(Arc<TableDef>, Space)>,
}

impl std::fmt::Debug for Tx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.t.fmt(f)
    }
}

impl Tx {
    /// Starts a transaction.
    ///
    /// # Errors
    /// The engine's.
    pub(crate) fn begin(e: &sprig_engine::Engine, opts: TxnOptions) -> Result<Self> {
        let mut t = e.begin(opts)?;
        // The transaction is its one statement, which starts now (at
        // SERIALIZABLE on MariaDB's port its read view opens here, so a row
        // it had to wait for and that was changed meanwhile is a snapshot
        // conflict, as there).
        t.begin_statement();
        Ok(Self {
            t,
            session: false,
            env: Env::STRICT,
            held: Vec::new(),
        })
    }

    /// Starts a session's transaction: its statements run one after the
    /// other in it until [`Tx::finish`] or [`Tx::rollback`].
    ///
    /// # Errors
    /// The engine's.
    pub(crate) fn begin_session(e: &sprig_engine::Engine, opts: TxnOptions) -> Result<Self> {
        Ok(Self {
            t: e.begin(opts)?,
            session: true,
            env: Env::STRICT,
            held: Vec::new(),
        })
    }

    /// The session's rules for the statements that follow (its `sql_mode`
    /// and time zone now): given before each statement, since a `SET`
    /// between two statements of a transaction changes them. Without it a
    /// transaction runs in strict mode and UTC.
    pub(crate) fn set_env(&mut self, env: Env) {
        self.env = env;
    }

    /// The keyspace the transaction already uses for `def`.
    pub(crate) fn held(&self, def: &Arc<TableDef>) -> Option<Space> {
        self.held
            .iter()
            .find(|(d, _)| Arc::ptr_eq(d, def))
            .map(|(_, sp)| sp.clone())
    }

    /// The transaction uses `def` through `sp` from now on.
    pub(crate) fn hold(&mut self, def: &Arc<TableDef>, sp: &Space) {
        self.held.push((Arc::clone(def), sp.clone()));
    }

    /// The rules statements run under.
    pub(crate) fn env(&self) -> Env {
        self.env
    }

    /// Whether plain reads lock what they read: SERIALIZABLE inside a
    /// session's transaction (an autocommit read stays a consistent read).
    pub(crate) fn shared_reads(&self) -> bool {
        self.session && self.t.options().isolation == Isolation::Serializable
    }

    /// The options it runs with.
    pub(crate) fn options(&self) -> &TxnOptions {
        self.t.options()
    }

    /// The engine transaction itself, for the stream and queue calls made
    /// inside the session's transaction: their log notes and plain writes
    /// join its commit.
    pub(crate) fn engine_txn(&mut self) -> &mut Txn {
        &mut self.t
    }

    /// Whether the transaction is still open (a deadlock, a snapshot
    /// conflict or a full lock table ends it).
    pub(crate) fn is_open(&self) -> bool {
        self.t.state().is_open
    }

    /// A statement starts: its writes can be undone alone.
    pub(crate) fn begin_statement(&mut self) {
        self.t.begin_statement();
    }

    /// Undoes the current statement's writes; its locks stay.
    pub(crate) fn rollback_statement(&mut self) {
        if self.is_open() {
            self.t.rollback_statement();
        }
    }

    /// `SAVEPOINT`.
    pub(crate) fn savepoint(&self) -> Savepoint {
        self.t.savepoint()
    }

    /// `ROLLBACK TO SAVEPOINT`.
    pub(crate) fn rollback_to(&mut self, sp: Savepoint) {
        self.t.rollback_to(sp);
    }

    /// Waits until every pending version the transaction read so far is
    /// durable: a statement's answer inside a transaction goes out after
    /// it.
    ///
    /// # Errors
    /// The log failed first.
    pub(crate) async fn gate(&self) -> Result<()> {
        let g = self.t.reply_gate();
        if g.is_durable() {
            return Ok(());
        }
        g.wait_async().await
    }

    /// Whether locking accesses of rows make the snapshot-isolation check:
    /// in a session's transaction, and in any statement at SERIALIZABLE
    /// (MariaDB opens a view for an autocommit statement there too). The
    /// engine applies it only where MariaDB does: with a read view open.
    fn snapshot_checks(&self) -> bool {
        self.session || self.t.options().isolation == Isolation::Serializable
    }

    /// The request of a locking access to a row.
    fn row_request(&self, mode: LockMode, wait: Wait) -> LockRequest {
        LockRequest {
            mode,
            wait,
            check_snapshot: self.snapshot_checks(),
        }
    }

    /// One key at the read view.
    ///
    /// # Errors
    /// The store's.
    pub(crate) fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.t.get_snapshot(key)
    }

    /// Up to `limit` entries of `[lo, hi)` at the read view, descending
    /// when `reverse`.
    ///
    /// # Errors
    /// The store's.
    pub(crate) fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        reverse: bool,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let dir = if reverse { Dir::Reverse } else { Dir::Forward };
        let r = KeyRange::new(lo.to_vec(), hi.to_vec());
        Ok(self
            .t
            .scan(&r, dir, limit, ReadMode::Snapshot)?
            .into_iter()
            .map(|(k, v)| (Bytes::from(k), v))
            .collect())
    }

    /// A locking read of `[lo, hi)`, the rows of a table in key order:
    /// up to `limit` rows that `keep(key, value)` accepts, each as it is
    /// now (the latest committed version, or this transaction's own) and
    /// locked in `mode`. At REPEATABLE READ and SERIALIZABLE the stretch
    /// read is gap-locked too (no other transaction can insert into it),
    /// and every row read stays locked; at READ COMMITTED a row `keep`
    /// refuses is unlocked again. `semi_consistent`: an `UPDATE` or
    /// `DELETE` scanning the primary key (at READ COMMITTED a row another
    /// transaction holds is judged on its committed version first, and
    /// not waited for when `keep` refuses it; `keep` may then run twice).
    /// The transaction's read view isn't opened (InnoDB opens it at the
    /// first plain read only).
    ///
    /// # Errors
    /// As for [`Tx::lock_read`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn scan_lock_where<F>(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        reverse: bool,
        limit: usize,
        mode: LockMode,
        wait: Wait,
        semi_consistent: bool,
        keep: F,
    ) -> Result<Vec<(Bytes, Bytes)>>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send,
    {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let dir = if reverse { Dir::Reverse } else { Dir::Forward };
        let r = KeyRange::new(lo.to_vec(), hi.to_vec());
        let req = self.row_request(mode, wait);
        self.t
            .scan_lock_where(&r, dir, limit, req, semi_consistent, keep)
            .await
    }

    /// A locking read of the index entries in `[lo, hi)`: up to `limit`,
    /// each locked in `mode` (with the gap between them at REPEATABLE READ
    /// and SERIALIZABLE). The rows they point at are locked one by one
    /// with [`Tx::lock_read`].
    ///
    /// # Errors
    /// As for [`Tx::lock_read`], without 1020 (the snapshot check is the
    /// rows').
    pub(crate) async fn scan_lock_entries(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
        mode: LockMode,
        wait: Wait,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let r = KeyRange::new(lo.to_vec(), hi.to_vec());
        let req = LockRequest {
            mode,
            wait,
            check_snapshot: false,
        };
        self.t.scan_lock(&r, Dir::Forward, limit, req).await
    }

    /// A locking read of row `key` (`FOR UPDATE`, `FOR SHARE`, DML's read
    /// of a row): `None` when `SKIP LOCKED` skipped it, else its latest
    /// committed value (or this transaction's own).
    ///
    /// # Errors
    /// A lock error: 1205 (a timeout, MariaDB's `NOWAIT`) or 3572 (MySQL's
    /// `NOWAIT`) with the statement undone; 1213, and 1020 in a session's
    /// transaction on MariaDB's port, with the transaction rolled back. Or
    /// the store's.
    pub(crate) async fn lock_read(
        &mut self,
        key: &Bytes,
        mode: LockMode,
        wait: Wait,
    ) -> Result<Option<Option<Bytes>>> {
        let req = self.row_request(mode, wait);
        match self.t.lock_get(key, req).await? {
            Locked::Row(v) => Ok(Some(v)),
            Locked::Skipped => Ok(None),
        }
    }

    /// A locking read of key `key` in `mode`, waiting, without the
    /// snapshot check (a foreign key's lookup of a parent or child entry):
    /// its latest committed value (or this transaction's own), `None`
    /// inside when it doesn't exist (its place is then locked against
    /// inserts at REPEATABLE READ and SERIALIZABLE).
    ///
    /// # Errors
    /// A lock error (1205, 1213), or the store's.
    #[allow(clippy::option_option)] // skipped, missing, or the row: as `lock_read`
    pub(crate) async fn lock_key(
        &mut self,
        key: &Bytes,
        mode: LockMode,
    ) -> Result<Option<Option<Bytes>>> {
        let req = LockRequest {
            mode,
            wait: Wait::Block,
            check_snapshot: false,
        };
        match self.t.lock_get(key, req).await? {
            Locked::Row(v) => Ok(Some(v)),
            Locked::Skipped => Ok(None),
        }
    }

    /// [`Tx::lock_read`] for a thread that may block (the planner's
    /// synchronous section, off the runtime's worker).
    ///
    /// # Errors
    /// As for [`Tx::lock_read`].
    #[allow(clippy::option_option)] // skipped, missing, or the row: as `lock_read`
    pub(crate) fn lock_read_blocking(
        &mut self,
        key: &Bytes,
        mode: LockMode,
        wait: Wait,
    ) -> Result<Option<Option<Bytes>>> {
        let req = self.row_request(mode, wait);
        match self.t.lock_get_blocking(key, req)? {
            Locked::Row(v) => Ok(Some(v)),
            Locked::Skipped => Ok(None),
        }
    }

    /// A locking read of the keys in `[lo, hi)` (rows or index entries),
    /// up to `limit`, each locked in `mode`, with the stretch read
    /// gap-locked at REPEATABLE READ and SERIALIZABLE; for a thread that may
    /// block. `rows`: the keys are a table's rows (the snapshot check
    /// applies to them).
    ///
    /// # Errors
    /// As for [`Tx::lock_read`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_lock_blocking(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        reverse: bool,
        limit: usize,
        mode: LockMode,
        wait: Wait,
        rows: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if lo >= hi {
            return Ok(Vec::new());
        }
        let dir = if reverse { Dir::Reverse } else { Dir::Forward };
        let r = KeyRange::new(lo.to_vec(), hi.to_vec());
        let req = if rows {
            self.row_request(mode, wait)
        } else {
            LockRequest {
                mode,
                wait,
                check_snapshot: false,
            }
        };
        self.t.scan_lock_blocking(&r, dir, limit, req)
    }

    /// What ends this transaction's lock wait from another task
    /// (`KILL QUERY`, a statement time limit).
    pub(crate) fn interrupt_handle(&self) -> sprig_engine::txn::InterruptHandle {
        self.t.interrupt_handle()
    }

    /// Whether the session follows MySQL (where the two servers lock
    /// differently).
    pub(crate) fn mysql(&self) -> bool {
        self.t.options().persona == sprig_engine::txn::Persona::MySql
    }

    /// READ COMMITTED: gives up the lock on row `key`, which the statement's
    /// `WHERE` refused (unless the transaction wrote it). Other levels
    /// keep every lock they took.
    pub(crate) fn unlock_unmatched(&mut self, key: &Bytes) {
        if self.session && self.t.options().isolation == Isolation::ReadCommitted {
            self.t.unlock(key);
        }
    }

    /// Writes `key`, which exists and which the transaction locked (a row
    /// it read for an `UPDATE`), locking it first otherwise.
    ///
    /// # Errors
    /// A lock error, or the transaction is too big.
    pub(crate) async fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.t.lock(&key, CURRENT).await?;
        self.t.put(key, value)
    }

    /// Writes `key`, which may be new (a non-unique index entry): its
    /// exclusive lock is taken, and another transaction's gap lock over
    /// its place is waited for on the runtime, before it is written.
    ///
    /// # Errors
    /// As for [`Tx::put`].
    pub(crate) async fn put_new(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.t.lock_insert(&key, InsertLock::replace()).await?;
        self.t.put(key, value)
    }

    /// Writes `key` (`row`: a table row, not an index entry), which must
    /// not exist: `false` (nothing written) when it does; the duplicate's
    /// lock is then kept, as InnoDB keeps it.
    /// Another transaction's gap lock over the key's place is waited for.
    ///
    /// # Errors
    /// As for [`Tx::put`].
    pub(crate) async fn insert(&mut self, key: Bytes, value: Bytes, row: bool) -> Result<bool> {
        // In a session's transaction on MariaDB's port a duplicate row
        // newer than the read view is a snapshot conflict (1020), not a
        // duplicate key, as there.
        let how = if row && self.snapshot_checks() {
            InsertLock::insert().with_snapshot_check()
        } else {
            InsertLock::insert()
        };
        match self.t.lock_insert(&key, how).await? {
            InsertProbe::Exists(_) => Ok(false),
            InsertProbe::Free => {
                self.t.put(key, value)?;
                Ok(true)
            }
        }
    }

    /// Deletes `key`, locking it first.
    ///
    /// # Errors
    /// As for [`Tx::put`].
    pub(crate) async fn delete(&mut self, key: Bytes) -> Result<()> {
        self.t.lock(&key, CURRENT).await?;
        self.t.delete(key)
    }

    /// `XA PREPARE`: the transaction's writes and exclusive locks are kept
    /// under `xid` (durable when the ticket is), and the transaction is
    /// over for this session: the engine holds it until an `XA COMMIT` or
    /// `XA ROLLBACK` of `xid`.
    ///
    /// # Errors
    /// The engine's (the xid is taken, the log refused).
    pub(crate) fn xa_prepare(&mut self, xid: &[u8]) -> Result<sprig_engine::txn::DurableTicket> {
        self.t.xa_prepare(xid)
    }

    /// Commits and waits until the writes, and every pending version read,
    /// are durable.
    ///
    /// # Errors
    /// The log refused the commit, or failed before it was durable.
    pub(crate) async fn finish(self) -> Result<()> {
        let ticket = self.t.commit()?;
        ticket.wait_async().await
    }

    /// Commits without waiting for durability: the caller waits on the
    /// ticket. A walk over a whole table that commits batch after batch
    /// waits once, for its last batch (the log makes commits durable in
    /// order, so the earlier ones are durable by then).
    ///
    /// # Errors
    /// The log refused the commit.
    pub(crate) fn commit_queued(self) -> Result<sprig_engine::txn::CommitTicket> {
        self.t.commit()
    }

    /// Rolls back: the writes go and every lock is released.
    pub(crate) fn rollback(self) {
        self.t.rollback();
    }
}
