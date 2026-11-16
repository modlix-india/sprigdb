//! [`BackendSession`]: one client session's backend side (see the
//! [module docs](super) for the protocol an executor follows).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_auth::accounts::BackendTarget;
use sprig_cluster::failover::relay::RelayLease;
use sprig_cluster::record::TopologyKind;
use sprig_common::ids::{ClusterId, NodeId, ShardId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::client::{ClientError, PacketKind, RespPacket, TrackedChanges};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::resultset::Terminator;
use sprig_proto_mysql::{Caps, ErrPacket, Persona, ProtoResult, StatusFlags};
use tokio::time::Instant;

use super::classify::{Effect, StmtClass, StmtInfo};
use super::cluster::{ClusterView, NodeKey, NodeRef};
use super::conn::{self, BackendStmt, ConnectSpec, Warning};
use super::pins::{PinReason, Pins, TxnSetting};
use super::pool::{AcquireError, BackendLease, PoolKey, Release};
use super::settings::{BackendSettings, CausalScope, PoolMode};
use super::state::{self, SessionVars};
use super::writer::WriterSelector as _;
use super::{Backends, unavailable};
use crate::reads::{self, GtidSet, ReadTarget};

/// The rendezvous key whose Galera writer is the cluster's lock node for
/// user-level locks: not a shard, a fixed key every gateway hashes the
/// same way.
pub(crate) const LOCK_NODE_KEY: ShardId = ShardId(u32::MAX);

/// Options of a session's backend side, from its login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOpts {
    /// The listener's persona: the MySQL persona starts with MySQL 8.4's
    /// `sql_mode`.
    pub persona: Persona,
    /// The client negotiated `CLIENT_FOUND_ROWS`.
    pub found_rows: bool,
    /// MariaDB extended capabilities to ask backend connections for (3307),
    /// among `MARIADB_CLIENT_EXTENDED_METADATA` and
    /// `MARIADB_CLIENT_STMT_BULK_OPERATIONS`. A backend may grant fewer:
    /// [`BackendSession::backend_caps`] says what it did.
    pub mariadb_caps: Caps,
    /// The handshake collation id.
    pub collation: u16,
    /// The schema named at login.
    pub schema: Option<String>,
    /// The shard (shard 0 for a login's unsharded cluster).
    pub shard: ShardId,
    /// Sprig 2PC's relay path (`txn.sprig_2pc_path = relay` under
    /// `sprig_2pc`): on a cluster with relays, every connection goes
    /// through the relay writer with this tenant in its preamble.
    pub relay: Option<sprig_common::ids::TenantId>,
    /// A transaction coordinator's branch (the transaction's connection to
    /// one cluster): its connections come from pools of their own. A
    /// session holding a branch connection may still lease one for its own
    /// statements (preparing a statement during the transaction); from
    /// the same pool, sessions that each hold a branch could wait on each
    /// other until every wait times out.
    pub txn_branch: bool,
}

impl Default for SessionOpts {
    fn default() -> Self {
        Self {
            persona: Persona::MariaDb,
            found_rows: false,
            mariadb_caps: Caps::empty(),
            collation: 45,
            schema: None,
            shard: ShardId(0),
            relay: None,
            txn_branch: false,
        }
    }
}

/// How a statement was routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteKind {
    /// On the session's pinned connection.
    Pinned,
    /// On the writer.
    Writer,
    /// On a replica whose position covered the causal target.
    Replica,
    /// On a replica, after the wait-or-fail prefix.
    ReplicaWait,
    /// On a Galera node other than the writer, with no check: the read
    /// had no write to see, or the node's polled position covered it.
    Galera,
    /// On a Galera node other than the writer whose position covered the
    /// causal target, after the bounded sync-wait check
    /// ([`reads::GALERA_SYNC_CHECK`]).
    GaleraSyncWait,
    /// On a Galera node other than the writer whose polled position didn't
    /// show the causal target yet, after the bounded check
    /// (`reads.galera_unconfirmed = check`).
    GaleraUnconfirmed,
}

impl RouteKind {
    /// Off the writer: state must never appear on such a connection.
    fn reader(self) -> bool {
        matches!(
            self,
            RouteKind::Replica
                | RouteKind::ReplicaWait
                | RouteKind::Galera
                | RouteKind::GaleraSyncWait
                | RouteKind::GaleraUnconfirmed
        )
    }
}

/// What a read waits for before the client's statement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Wait {
    /// Nothing.
    None,
    /// A replica: the wait-or-fail prefix for this target.
    Replica(GtidSet),
    /// A Galera node: [`reads::GALERA_SYNC_CHECK`], bounded by
    /// `reads.galera_sync_wait_timeout_ms`.
    Galera,
}

/// How the sync (and wait) before a statement went.
enum Sent {
    /// The statement is on its way.
    Ready,
    /// A sync statement failed.
    SyncErr(ErrPacket),
    /// The wait failed (replica 1242, or the Galera check's error); the
    /// connection is idle and reusable.
    WaitFailed,
    /// The Galera check didn't finish in time; the connection is still
    /// busy with it and must be dropped.
    WaitTimedOut,
    /// [`TRX_GUARD`] failed: the transaction is gone and the statement
    /// wasn't run; the connection is idle and reusable. `None` for the
    /// guard's own error (no transaction open); otherwise the error the
    /// server reported in its place, such as the 1213 of an abort it did
    /// report.
    TrxGone(Option<ErrPacket>),
}

/// Run before a write inside a Galera transaction: fails with 1242, and
/// stops the statement after it, when no transaction is open. Galera can
/// roll a transaction back without an error; the write would then run,
/// and commit, on its own.
const TRX_GUARD: &str = "SET @__sprig_t = (SELECT CASE WHEN @@in_transaction = 1 THEN 1 \
                         ELSE (SELECT 1 UNION SELECT 2) END)";

/// What the gateway answers itself after a release.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Captured {
    /// `SHOW WARNINGS` of the last statement (empty when it had none).
    pub warnings: Vec<Warning>,
    /// `FOUND_ROWS()`: captured after `SQL_CALC_FOUND_ROWS`, else the rows
    /// the last statement returned.
    pub found_rows: u64,
    /// `ROW_COUNT()`: affected rows, or -1 after a resultset.
    pub row_count: i64,
}

/// The outcome of a finished statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StmtOutcome {
    /// Status flags of the final packet.
    pub status: StatusFlags,
    /// Warning count of the final packet.
    pub warnings: u16,
    /// Affected rows of the final OK.
    pub affected_rows: u64,
    /// Last insert id of the final OK.
    pub last_insert_id: u64,
    /// The server's error, if the statement failed.
    pub error: Option<ErrPacket>,
    /// The node it ran on.
    pub node: NodeKey,
    /// How it was routed.
    pub route: RouteKind,
    /// Whether the session now holds its connection.
    pub pinned: bool,
    /// Rows of the last resultset.
    pub rows: u64,
    /// Rows examined, when a `Handler_read_*` probe ran around the
    /// statement ([`BackendSession::probe_next_statement`]).
    pub examined: Option<u64>,
}

/// The statement in flight.
#[allow(clippy::struct_excessive_bools)] // independent flags of one statement
struct Current {
    lease: BackendLease,
    /// An autocommit write on a relayed connection: the gateway opened a
    /// transaction for it, and commits in a round trip of its own once
    /// the write answered (so a write waiting for a held branch's rows
    /// never commits after its client is gone).
    wrapped: bool,
    stmt: StmtInfo,
    route: RouteKind,
    first: Option<RespPacket>,
    relayed: bool,
    done: bool,
    failed: bool,
    in_trx_before: bool,
    status: Option<StatusFlags>,
    warnings: u16,
    last_ok: Option<OkPacket>,
    error: Option<ErrPacket>,
    tracked: TrackedChanges,
    rows: u64,
    resultset: bool,
    /// The first `Handler_read_*` reading of a probe.
    probe: Option<u64>,
    /// The statement is a `USE`: it may run on a connection whose default
    /// schema differs from the session's, so a failed one leaves that
    /// connection dirty.
    sets_schema: bool,
    /// It writes generated `AUTO_INCREMENT` values explicitly
    /// ([`BackendSession::explicit_ids_next_statement`]).
    explicit_ids: bool,
    /// The statement's `backend.query` trace span: open from `run` until
    /// `finish` drops the statement. A light child of the request's span,
    /// not a `tracing` span.
    span: Option<sprig_common::telemetry::trace::ChildSpan>,
    /// A plain `SELECT`, `INSERT`, `UPDATE`, `DELETE` or `REPLACE` inside
    /// a transaction on Galera: it can't end the transaction, so an answer
    /// saying the transaction is gone means Galera aborted it silently.
    galera_dml_in_trx: bool,
    /// Such a statement in a transaction that had written (the tracked
    /// state's `W`). With `autocommit = 0` the statement after a silent
    /// abort opens a new transaction, so its answer says a transaction is
    /// open; a reported state without the `W` shows it isn't the one that
    /// wrote.
    wrote_before: bool,
    /// A plain `INSERT`, `UPDATE`, `DELETE` or `REPLACE` on Galera, with
    /// `autocommit = 0` and no transaction open: it opens one (tenant
    /// tables are `InnoDB`; sessions with temporary tables, which may not
    /// be, are left out), so an answer saying none is open means Galera
    /// aborted it silently.
    opens_implicit: bool,
    /// A silent abort was seen while the backend had a transaction open
    /// (the new one `wrote_before` describes): it is rolled back before
    /// the connection serves the session again, as the client was told.
    rollback_after: bool,
    /// The statement may take row locks, or isn't one the statement check
    /// covers: anything but a plain read, a prepare or `BEGIN` (see
    /// [`BackendSession::trx_locks`]).
    takes_locks: bool,
}

impl Current {
    /// Whether `p` ends this statement showing that Galera rolled the
    /// transaction back without an error (see `galera_dml_in_trx`,
    /// `wrote_before` and `opens_implicit`).
    fn ended_silently(&self, p: &RespPacket) -> bool {
        p.status().is_some_and(|s| {
            shows_silent_abort(
                s,
                p.tracked.trx_state.as_deref(),
                self.galera_dml_in_trx || self.opens_implicit,
                self.wrote_before,
            )
        })
    }

    /// Answers `p` (the statement's last packet, see
    /// [`ended_silently`](Self::ended_silently)) with MariaDB's 1213 instead:
    /// counts it, and records that no transaction is open afterwards, after
    /// rolling back the one the backend may have opened since.
    fn silent_abort(
        &mut self,
        p: &RespPacket,
        cluster: &sprig_common::ids::ClusterId,
        what: &str,
    ) -> RespPacket {
        let node = self.lease.key().node_key();
        note_silent_abort(cluster, &node, what);
        let status = p.status().unwrap_or_default();
        self.rollback_after = status.contains(StatusFlags::IN_TRANS);
        self.status = Some(status - StatusFlags::IN_TRANS);
        silent_abort_packet(self.lease.conn_ref().client.caps())
    }

    fn observe(&mut self, p: &RespPacket) {
        match &p.kind {
            PacketKind::Ok(ok) => {
                self.status = Some(ok.status);
                self.warnings = ok.warnings;
                self.last_ok = Some(ok.clone());
            }
            PacketKind::End(t) => {
                self.status = Some(t.status());
                self.warnings = t.warnings();
                if let Terminator::Ok(ok) = t {
                    self.last_ok = Some(OkPacket {
                        affected_rows: 0,
                        last_insert_id: 0,
                        ..ok.clone()
                    });
                }
            }
            PacketKind::Err(e) => self.error = Some(e.clone()),
            PacketKind::Row => self.rows += 1,
            PacketKind::ColumnCount(_) => {
                self.resultset = true;
                self.rows = 0;
            }
            _ => {}
        }
        if !p.tracked.is_empty() {
            self.tracked.merge(p.tracked.clone());
        }
    }
}

/// A request to run.
/// Whether a connection must be closed after a statement: a `USE` that
/// left the connection's default schema different from the session's
/// (it failed on a connection that had another one).
fn dirty_after_use(sets_schema: bool, conn: Option<&str>, session: Option<&str>) -> bool {
    sets_schema && conn != session
}

/// Whether `sql` is a `USE <schema>` statement.
fn is_use(sql: &[u8]) -> bool {
    let t = sql.trim_ascii_start();
    t.len() > 4 && t[..3].eq_ignore_ascii_case(b"USE") && t[3].is_ascii_whitespace()
}

enum Req<'a> {
    Text(&'a [u8]),
    Binary {
        sql: &'a [u8],
        build: &'a mut (dyn FnMut(&BackendStmt) -> ProtoResult<Bytes> + Send + Sync),
    },
    Command(Bytes),
    /// Prepare only (into the connection's statement cache); the
    /// statement is copied out and nothing else is sent.
    Prepare {
        sql: &'a [u8],
        out: &'a mut Option<BackendStmt>,
    },
}

/// One client session's backend side: its client-visible state, pins,
/// held connection, causal position and captures.
#[allow(clippy::struct_excessive_bools)] // independent per-session flags
pub struct BackendSession {
    backends: Arc<Backends>,
    target: BackendTarget,
    opts: SessionOpts,
    vars: SessionVars,
    pins: Pins,
    held: Option<(BackendLease, RouteKind)>,
    causal: GtidSet,
    captured: Captured,
    pending_capture: Vec<String>,
    cur: Option<Current>,
    /// The connection `run` is sending on or waiting for the first packet
    /// from (before `cur` exists), so `KILL QUERY` can reach a statement
    /// abandoned in that window.
    inflight: Option<(NodeKey, u64)>,
    /// The error the next statement reports: an abandoned statement took
    /// the session's transaction or pinned state with its connection.
    deferred: Option<SprigError>,
    /// Probe the next statement's rows examined.
    probe_next: bool,
    /// The next statement writes `AUTO_INCREMENT` values the gateway
    /// generated (from ID blocks): its OK packet's insert id is an explicit
    /// value, not the connection's `LAST_INSERT_ID()`.
    explicit_ids_next: bool,
    /// The running statement's `backend.query` trace context, for the
    /// propagated `traceparent`.
    trace_ctx: Option<sprig_common::telemetry::trace::TraceContext>,
    /// Sprig 2PC: a held branch's record of its writes
    /// (`sprig_txn::BranchConn::start_redo`, in `backend::txn`).
    pub(super) redo: Option<super::txn::Recording>,
    /// Sprig 2PC's relay path on a cluster without a ready relay: this
    /// connection may not hold a branch (`start_redo` says no).
    pub(super) redo_off: bool,
    /// The held connection (thread id) and when its database-level
    /// grants were last refreshed with a `USE`.
    grants_seen: Option<(u64, std::time::Instant)>,
    /// Keep the last statement's connection for the next one
    /// ([`Self::set_keep`]).
    keep: bool,
    /// That connection: not a pin; the next statement uses it only when
    /// its route chooses the same node, and it goes back to the pool on
    /// [`Self::release_kept`]. Boxed: a lease holds its connection inline,
    /// and futures that own a session would grow by its size.
    kept: Option<Box<BackendLease>>,
    /// The open (or next) transaction may hold row locks, or ran a
    /// statement whose answer isn't checked for a silent abort: a write, a
    /// locking read or any statement but a plain read ran in it, its reads
    /// lock (`SERIALIZABLE`), or `SET TRANSACTION` preceded it. Until then a
    /// Galera write runs without [`TRX_GUARD`].
    trx_locks: bool,
    /// The statement running is the transaction coordinator's own
    /// (`sprig_txn::BranchConn::control`, in `backend::txn`), with the
    /// coordinator settings it changes (`true`) or puts back (`false`)
    /// once it succeeds.
    pub(super) txn_ctl: Option<Vec<(TxnSetting, bool)>>,
}

impl std::fmt::Debug for BackendSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendSession")
            .field("cluster", &self.target.cluster)
            .field("account", &self.target.user)
            .field("pins", &self.pins)
            .field("causal", &self.causal)
            .finish_non_exhaustive()
    }
}

fn pool_timeout() -> SprigError {
    SprigError::new(
        SprigCode::BackendPoolTimeout,
        "no backend connection became free in time",
    )
}

fn backend_err(e: &ErrPacket) -> SprigError {
    SprigError::backend(e.code, e.sqlstate, e.message.clone())
}

/// Server errors after which the server closes the connection.
/// Whether a transaction state (`session_track_transaction_info =
/// STATE`, such as `T___W___`) says the transaction wrote transactional
/// tables.
fn trx_wrote(state: &str) -> bool {
    state.as_bytes().get(4) == Some(&b'W')
}

/// Whether a Galera COMMIT answered `ok` produced no GTID: the transaction
/// that wrote was rolled back without an error. Galera can abort a
/// transaction for another node's conflicting write set while it runs a
/// statement that doesn't wait for locks, answer that statement
/// normally, and then answer COMMIT with OK: nothing was written. The OK
/// of a commit that wrote reports its GTID (`last_gtid`); without one, the
/// GTID is read, since MariaDB 11.8 can report `timestamp` in its place.
/// A GTID that can't be read counts as written: only a known non-commit is
/// turned into an error.
async fn commit_wrote_nothing(cur: &mut Current, ok: &RespPacket, before: Option<&str>) -> bool {
    if let Some(g) = ok.tracked.gtid.as_deref() {
        return gtid_unchanged(before, g);
    }
    match cur.lease.conn().capture_last_gtid().await {
        Ok(g) => gtid_unchanged(before, &g),
        Err(e) => {
            tracing::debug!(error = %e, "reading last_gtid after a Galera commit failed");
            false
        }
    }
}

/// Whether `sql` is a plain `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
/// `REPLACE` or `WITH`: a statement that can't end a transaction (stored
/// functions and triggers can't commit), after any leading comments.
/// Whether `p` ends a statement's response.
fn ends_response(p: &RespPacket) -> bool {
    match &p.kind {
        PacketKind::Err(_) => true,
        PacketKind::Ok(_) | PacketKind::End(_) => !p
            .status()
            .unwrap_or_default()
            .contains(StatusFlags::MORE_RESULTS_EXISTS),
        _ => false,
    }
}

/// `p` (an OK, or an OK-form or EOF terminator) without
/// `SERVER_STATUS_IN_TRANS`: the transaction the gateway opened around an
/// autocommit write has committed.
fn without_in_trans(mut p: RespPacket) -> RespPacket {
    let b = &p.payload;
    // OK (0x00) and OK-form terminators (0xfe, long): affected rows and
    // the insert id, length-encoded, then the status. EOF (0xfe, short):
    // warnings, then the status.
    let at = match b.first() {
        Some(0xfe) if b.len() < 9 => Some(3),
        Some(0x00 | 0xfe) => lenenc_len(b, 1).and_then(|n| lenenc_len(b, 1 + n).map(|m| 1 + n + m)),
        _ => None,
    };
    if let Some(at) = at
        && b.len() >= at + 2
    {
        let mut v = b.to_vec();
        let st = u16::from_le_bytes([v[at], v[at + 1]]) & !StatusFlags::IN_TRANS.bits();
        v[at..at + 2].copy_from_slice(&st.to_le_bytes());
        p.payload = Bytes::from(v);
        if let PacketKind::Ok(ok) = &mut p.kind {
            ok.status -= StatusFlags::IN_TRANS;
        }
    }
    p
}

/// The length of the length-encoded integer at `b[at]`.
fn lenenc_len(b: &[u8], at: usize) -> Option<usize> {
    match *b.get(at)? {
        x if x < 0xfb => Some(1),
        0xfc => Some(3),
        0xfd => Some(4),
        0xfe => Some(9),
        _ => None,
    }
}

/// Sprig 2PC's relay preamble for `tenant`.
fn relay_preamble(tenant: sprig_common::ids::TenantId) -> Vec<u8> {
    sprig_txn::fence::wire::preamble(tenant)
}

fn plain_dml(sql: &[u8]) -> bool {
    first_word_in(
        sql,
        &["SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "WITH"],
    )
}

/// Whether `sql` is a plain `INSERT`, `UPDATE`, `DELETE` or `REPLACE`: with
/// `autocommit = 0` it opens a transaction on an `InnoDB` table even when it
/// changes no row (a `SELECT` may read no table at all).
fn plain_write(sql: &[u8]) -> bool {
    first_word_in(sql, &["INSERT", "UPDATE", "DELETE", "REPLACE"])
}

/// Whether `sql`'s first word, after leading comments, is one of `words`.
/// An executable comment (`/*!` or `/*M!`) first makes it `false`: it holds
/// SQL.
fn first_word_in(sql: &[u8], words: &[&str]) -> bool {
    let Some(s) = skip_comments(sql) else {
        return false;
    };
    let word = s
        .iter()
        .position(|b| !b.is_ascii_alphabetic())
        .map_or(s, |n| &s[..n]);
    words
        .iter()
        .any(|k| word.eq_ignore_ascii_case(k.as_bytes()))
}

/// `sql` after leading white space and comments (`/* … */`, `-- …` and
/// `# …` lines, as ORMs add them); `None` at an executable comment or an
/// unterminated one.
fn skip_comments(sql: &[u8]) -> Option<&[u8]> {
    let mut s = sql.trim_ascii_start();
    loop {
        if let Some(rest) = s.strip_prefix(b"/*") {
            if rest.starts_with(b"!") || rest.starts_with(b"M!") {
                return None;
            }
            let end = rest.windows(2).position(|w| w == b"*/")?;
            s = rest[end + 2..].trim_ascii_start();
        } else if s.starts_with(b"#")
            || (s.starts_with(b"--") && s.get(2).is_some_and(u8::is_ascii_whitespace))
        {
            let end = s.iter().position(|b| *b == b'\n')?;
            s = s[end + 1..].trim_ascii_start();
        } else {
            return Some(s);
        }
    }
}

/// Whether a statement's last packet (`status`, and the transaction state
/// it reported, if any) shows that Galera rolled the transaction back
/// without an error: no transaction is open after a statement that must
/// leave one open (`needs_trx`), or the open one isn't the one that wrote
/// (`wrote_before`: its reported state has no `W`).
fn shows_silent_abort(
    status: StatusFlags,
    trx_state: Option<&str>,
    needs_trx: bool,
    wrote_before: bool,
) -> bool {
    if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
        return false;
    }
    if !status.contains(StatusFlags::IN_TRANS) {
        return needs_trx;
    }
    wrote_before && trx_state.is_some_and(|t| !trx_wrote(t))
}

/// Logs and counts a transaction Galera rolled back while answering OK
/// (`what`: the statement whose answer showed it).
fn note_silent_abort(cluster: &sprig_common::ids::ClusterId, node: &NodeKey, what: &str) {
    tracing::warn!(
        node = %node,
        "Galera answered {what} with OK, but the transaction was rolled back and wrote nothing; answering 1213"
    );
    metrics::counter!("sprig_backend_silent_aborts_total", "cluster" => cluster.to_string())
        .increment(1);
}

/// Whether the connection's GTID after a commit (`after`, empty for none)
/// is the one it had before (`before`): the commit wrote nothing.
fn gtid_unchanged(before: Option<&str>, after: &str) -> bool {
    after.is_empty() || before == Some(after)
}

/// The error a failed [`TRX_GUARD`] reports: `None` for the guard's own
/// (1242, no transaction open: a silent abort), else the server's, such as
/// the 1213 of an abort it reported as usual.
fn reported(e: ErrPacket) -> Option<ErrPacket> {
    (e.code != 1242).then_some(e)
}

/// MariaDB's own deadlock error, the answer for a transaction Galera rolled
/// back silently: it tells the client the transaction is gone and may be
/// retried.
fn silent_abort_err() -> ErrPacket {
    ErrPacket::new(
        1213,
        *b"40001",
        "Deadlock found when trying to get lock; try restarting transaction",
    )
}

/// [`silent_abort_err`] as a response packet, in place of an OK.
fn silent_abort_packet(caps: Caps) -> RespPacket {
    let e = silent_abort_err();
    RespPacket {
        payload: e.encode(caps),
        kind: PacketKind::Err(e),
        tracked: TrackedChanges::default(),
    }
}

fn conn_fatal(code: u16) -> bool {
    // 1927 ER_CONNECTION_KILLED, 1053 ER_SERVER_SHUTDOWN, 1152
    // ER_ABORTING_CONNECTION, 4031 (idle disconnect).
    matches!(code, 1927 | 1053 | 1152 | 4031)
}

/// 1213 (40001): the connection was lost and the open transaction rolled
/// back, so the client retries it as it would after a deadlock.
fn trx_rolled_back() -> SprigError {
    SprigError::standard(
        1213,
        *b"40001",
        "Deadlock found when trying to get lock; try restarting transaction \
         (the backend connection was lost and the transaction rolled back)",
    )
}

fn internal(e: &ClientError) -> SprigError {
    SprigError::new(SprigCode::BackendProtocol, e.to_string())
}

/// A session dropped with a statement under way stops that statement. The
/// router's per-shard streams (scatter, gather, analytic reads) are
/// sessions of their own, dropped with the request when its REST or gRPC
/// client goes away; their connections close with them, but MariaDB would
/// run a statement such as `SELECT SLEEP(30)` to its end. The connection is
/// never reused (a dropped lease closes it), so the `KILL QUERY` can't
/// reach another statement.
impl Drop for BackendSession {
    fn drop(&mut self) {
        self.release_kept();
        if !self.in_flight() {
            return;
        }
        let Some((node, id)) = self.thread_id() else {
            return;
        };
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let backends = Arc::clone(&self.backends);
        let target = self.target.clone();
        // The connection stays open until the kill is in: MariaDB's `KILL`
        // of a thread whose client has just closed its socket can wait up
        // to 2 s for that thread (seen live), one of a live client's doesn't.
        let conn = self.cur.take();
        drop(rt.spawn(async move {
            let t0 = std::time::Instant::now();
            let r =
                tokio::time::timeout(Duration::from_secs(3), backends.kill_query(&node, &target, id))
                    .await;
            drop(conn);
            let ms = t0.elapsed().as_millis();
            match r {
                Ok(Ok(())) if ms < 500 => {
                    tracing::debug!(node = %node, thread = id, ms, "statement of a dropped backend session killed");
                }
                Ok(Ok(())) => {
                    tracing::info!(node = %node, thread = id, ms, "statement of a dropped backend session killed, slowly");
                }
                Ok(Err(e)) => {
                    tracing::info!(node = %node, thread = id, ms, error = %e, "KILL QUERY for a dropped backend session failed");
                }
                Err(_) => {
                    tracing::info!(node = %node, thread = id, "KILL QUERY for a dropped backend session timed out");
                }
            }
        }));
    }
}

impl BackendSession {
    pub(super) fn new(backends: Arc<Backends>, target: BackendTarget, opts: SessionOpts) -> Self {
        let settings = backends.settings(&target.cluster);
        let mut pins = Pins::default();
        pins.set_session_mode(settings.pool_mode == PoolMode::Session);
        Self {
            vars: SessionVars::login(opts.persona, opts.collation, opts.schema.clone()),
            backends,
            target,
            opts,
            pins,
            held: None,
            grants_seen: None,
            causal: GtidSet::new(),
            captured: Captured::default(),
            pending_capture: Vec::new(),
            cur: None,
            inflight: None,
            deferred: None,
            probe_next: false,
            explicit_ids_next: false,
            trace_ctx: None,
            redo: None,
            redo_off: false,
            keep: false,
            kept: None,
            trx_locks: false,
            txn_ctl: None,
        }
    }

    /// Keeps each statement's connection for the session's next statement
    /// instead of returning it to the pool (MongoDB sessions, whose point
    /// reads then skip the pool's lease and release). It isn't a pin: a
    /// statement whose route chooses another node, or a connection the
    /// pool wouldn't reuse (in a transaction, too old, its account's grants
    /// changed), takes the pool's way. Turning it off returns the kept
    /// connection.
    pub fn set_keep(&mut self, on: bool) {
        self.keep = on;
        if !on {
            self.release_kept();
        }
    }

    /// Whether a connection is kept for the next statement.
    #[must_use]
    pub fn holds_kept(&self) -> bool {
        self.kept.is_some()
    }

    /// Returns the kept connection to the pool (the session went idle).
    pub fn release_kept(&mut self) {
        if let Some(l) = self.kept.take() {
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(*l, Release::Reuse, limits);
        }
    }

    /// The kept connection, when it is to `node` of `view`'s cluster for
    /// this session's account and options and the pool would hand it out
    /// again; otherwise it goes back to the pool.
    fn take_kept(&mut self, view: &ClusterView, node: &NodeRef) -> Option<BackendLease> {
        let mut l = *self.kept.take()?;
        let key = l.key();
        let same = key.cluster == view.id
            && key.node == node.node
            && key.account == self.target.user
            && key.found_rows == self.opts.found_rows
            && key.mariadb_caps == self.opts.mariadb_caps.bits()
            && key.relay == self.relay_via(view)
            && key.branch == self.opts.txn_branch;
        let limits = self.backends.limits(&self.target.cluster);
        let retired = self
            .backends
            .pools
            .retired_at(&key.account)
            .is_some_and(|at| {
                std::time::Instant::now()
                    .checked_sub(l.conn_ref().age())
                    .is_none_or(|opened| opened < at)
            });
        let conn = l.conn();
        let usable = !retired
            && conn.client.is_idle()
            && !conn.client.in_transaction()
            && !conn.client.closed_while_idle()
            && conn.age() < limits.max_lifetime;
        if same && usable {
            return Some(l);
        }
        self.backends.pools.release(
            l,
            if usable {
                Release::Reuse
            } else {
                Release::Discard
            },
            limits,
        );
        None
    }

    /// The client-visible session state.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// The client-visible session state, for the executor to change what
    /// the gateway answers itself (such as `SET NAMES` at login).
    pub fn vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    /// The pins.
    #[must_use]
    pub fn pins(&self) -> &Pins {
        &self.pins
    }

    /// Whether the session holds a backend connection (or will at its next
    /// statement).
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        !self.pins.is_empty()
    }

    /// The session's last commit GTIDs.
    #[must_use]
    pub fn causal(&self) -> &GtidSet {
        &self.causal
    }

    /// Sets the causal position (a router stream on its own connection
    /// waits for what the session's connection to that cluster saw).
    pub fn set_causal(&mut self, g: GtidSet) {
        self.causal = g;
    }

    /// The statement in flight's first packet, when it is an ERR (after
    /// [`query`](Self::query) returned): the router reads it before
    /// relaying, to retry after a stale shard map.
    #[must_use]
    pub fn first_error(&self) -> Option<&ErrPacket> {
        match &self.cur.as_ref()?.first.as_ref()?.kind {
            PacketKind::Err(e) => Some(e),
            _ => None,
        }
    }

    /// What the gateway answers after a release: warnings, `FOUND_ROWS()`,
    /// `ROW_COUNT()`.
    #[must_use]
    pub fn captured(&self) -> &Captured {
        &self.captured
    }

    /// The backend target.
    #[must_use]
    pub fn target(&self) -> &BackendTarget {
        &self.target
    }

    /// The backend thread id of the statement in flight or the held
    /// connection.
    #[must_use]
    pub fn thread_id(&self) -> Option<(NodeKey, u64)> {
        let Some(lease) = self
            .cur
            .as_ref()
            .map(|c| &c.lease)
            .or(self.held.as_ref().map(|(l, _)| l))
        else {
            return self.inflight.clone();
        };
        Some((lease.key().node_key(), lease.conn_ref().thread_id))
    }

    /// The capabilities the backend connection serving the statement in
    /// flight negotiated (or the held connection's, between statements);
    /// `None` without either. Call it after [`query`](Self::query),
    /// [`execute`](Self::execute) or [`command`](Self::command) returns:
    /// column definitions in the response are encoded with these (only
    /// `MARIADB_CLIENT_EXTENDED_METADATA` changes them), whatever
    /// [`SessionOpts::mariadb_caps`] asked for.
    #[must_use]
    pub fn backend_caps(&self) -> Option<Caps> {
        self.cur
            .as_ref()
            .map(|c| &c.lease)
            .or(self.held.as_ref().map(|(l, _)| l))
            .map(|l| l.conn_ref().client.caps())
    }

    /// Whether a statement is under way: sent, or its response not read to
    /// its end.
    #[must_use]
    pub fn in_flight(&self) -> bool {
        self.cur.as_ref().is_some_and(|c| !c.done && !c.failed) || self.inflight.is_some()
    }

    /// `KILL QUERY` on the backend thread of the statement in flight (or
    /// the held connection).
    ///
    /// # Errors
    /// As [`Backends::kill_query`].
    pub async fn kill_query(&self) -> Result<()> {
        match self.thread_id() {
            Some((node, id)) => self.backends.kill_query(&node, &self.target, id).await,
            None => Ok(()),
        }
    }

    /// A server-side cursor opened (`COM_STMT_EXECUTE` with a cursor
    /// flag): pins until [`close_cursor`](Self::close_cursor).
    pub fn open_cursor(&mut self) {
        self.pins.open_cursor();
    }

    /// A cursor closed.
    pub fn close_cursor(&mut self) {
        self.pins.close_cursor();
    }

    /// Measures the rows the next statement (`query` or `execute`)
    /// examines: the connection's `Handler_read_*` sum is read before it
    /// runs and after it ends, on the same connection, and the difference
    /// less the reading's own cost lands in [`StmtOutcome::examined`].
    ///
    /// Only statements that leave the connection free are measured *(the
    /// reading after would replace the diagnostics a pinned session reads
    /// next: `ROW_COUNT()`, `FOUND_ROWS()`, `SHOW WARNINGS`)*: in a
    /// transaction or with pinned state, no probe runs. The reading after
    /// comes once the release captures are done. A probe that fails is
    /// dropped; the statement's result is never affected.
    pub fn probe_next_statement(&mut self) {
        self.probe_next = true;
    }

    /// Marks the next statement as one that writes `AUTO_INCREMENT` values
    /// the gateway generated: its OK packet's insert id then changes
    /// neither the session's `LAST_INSERT_ID()` nor what the connection is
    /// known to hold (MariaDB keeps the old value after explicit values).
    /// The router sets the session's value itself.
    pub fn explicit_ids_next_statement(&mut self) {
        self.explicit_ids_next = true;
    }

    /// Runs one text statement (`COM_QUERY`). On success the statement's
    /// first response packet is ready for [`next`](Self::next).
    ///
    /// # Errors
    /// 39030, 39031, 39032, 39033, 1213 (see the module docs); the
    /// server's error for a failed sync (after one retry); 39100 without a
    /// writer.
    pub async fn query(&mut self, info: &StmtInfo, sql: &[u8]) -> Result<()> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        self.check_packet(sql.len() + 1)?;
        if let Some(r) = self.redo.as_mut() {
            r.before(info, sql);
        }
        self.run(info, Req::Text(sql)).await
    }

    /// A statement the gateway's rewrites made longer than the cluster's
    /// `max_allowed_packet` is refused with 1153 before it is sent:
    /// MariaDB would drop the connection, leaving the outcome unknown.
    fn check_packet(&self, len: usize) -> Result<()> {
        match self.backends.packet_limit(&self.target.cluster) {
            Some(limit) if len > limit => Err(super::packet_too_large()),
            _ => Ok(()),
        }
    }

    /// `sql` reading the session's protected tables through their policy
    /// views, if it reads any. The session's account can't read the tables
    /// themselves, so a reference this misses fails (1142) rather than
    /// reading unfiltered rows.
    fn policy_views(&self, sql: &[u8]) -> Option<Vec<u8>> {
        let v = self.vars();
        let swap = v.rls_views.as_ref()?;
        let mode = v.vars.get("sql_mode").map(|m| m.to_ascii_uppercase());
        let lex = sprig_sql::lex::LexOptions {
            ansi_quotes: mode.as_deref().is_some_and(|m| m.contains("ANSI_QUOTES")),
            no_backslash_escapes: mode
                .as_deref()
                .is_some_and(|m| m.contains("NO_BACKSLASH_ESCAPES")),
            ..sprig_sql::lex::LexOptions::mariadb()
        };
        swap.apply(sql, &lex, v.schema.as_deref())
    }

    /// Whether this connection records its writes for a held branch's
    /// redo (Sprig 2PC).
    #[must_use]
    pub fn recording(&self) -> bool {
        self.redo.is_some()
    }

    /// The next [`execute`](Self::execute)'s statement with its bound
    /// values written in, for the redo record: a prepared write on a
    /// recording branch is then replayable.
    pub fn bind_text(&mut self, text: String) {
        if let Some(r) = self.redo.as_mut() {
            r.bound = Some(text);
        }
    }

    /// Runs a prepared statement (binary protocol): prepares `sql` on the
    /// chosen connection if needed (lazily, under the node's budget) and
    /// sends the payload `build` makes for that backend statement. Set the
    /// new-params-bound flag when [`BackendStmt::types_sent`] is false.
    ///
    /// # Errors
    /// As [`query`](Self::query); the server's error for a bad prepare.
    pub async fn execute(
        &mut self,
        info: &StmtInfo,
        sql: &[u8],
        build: &mut (dyn FnMut(&BackendStmt) -> ProtoResult<Bytes> + Send + Sync),
    ) -> Result<()> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        if let Some(r) = self.redo.as_mut() {
            r.before_binary(info, sql);
        }
        self.run(info, Req::Binary { sql, build }).await
    }

    /// Prepares `sql` on the chosen connection's statement cache (for
    /// `COM_STMT_PREPARE`: the client's `PREPARE_OK` needs the metadata)
    /// and ends the statement: the connection is released, or kept when
    /// the session is pinned. Executes then find it cached.
    ///
    /// # Errors
    /// As [`query`](Self::query); the server's error for a bad statement.
    pub async fn prepare(&mut self, info: &StmtInfo, sql: &[u8]) -> Result<BackendStmt> {
        let views = self.policy_views(sql);
        let sql = views.as_deref().unwrap_or(sql);
        let mut out = None;
        self.run(info, Req::Prepare { sql, out: &mut out }).await?;
        self.finish().await?;
        out.ok_or_else(|| SprigError::internal("prepare returned no statement"))
    }

    /// Sends any other command payload (its first byte is the command
    /// code), after syncing state in its own round trip.
    ///
    /// # Errors
    /// As [`query`](Self::query).
    pub async fn command(&mut self, info: &StmtInfo, payload: Bytes) -> Result<()> {
        self.check_packet(payload.len())?;
        self.run(info, Req::Command(payload)).await
    }

    /// The next packet of the response, or `None` when it is complete.
    ///
    /// # Errors
    /// The lost-connection errors of the module docs.
    pub async fn next(&mut self) -> Result<Option<RespPacket>> {
        match self.next_packet().await? {
            Some(p) if self.cur.as_ref().is_some_and(|c| c.wrapped) && ends_response(&p) => {
                self.commit_wrapped(p).await.map(Some)
            }
            other => Ok(other),
        }
    }

    /// The end of a wrapped autocommit write's response: `COMMIT` (or,
    /// after an error, `ROLLBACK`) in a round trip of its own. The
    /// client gets the write's answer only once it committed; a failed
    /// `COMMIT` answers in its place.
    async fn commit_wrapped(&mut self, p: RespPacket) -> Result<RespPacket> {
        let Some(cur) = self.cur.as_mut() else {
            return Ok(p);
        };
        cur.wrapped = false;
        let rollback = matches!(p.kind, PacketKind::Err(_));
        let r: std::result::Result<Vec<RespPacket>, ClientError> = async {
            let client = &mut cur.lease.conn().client;
            // The write's response is complete: read its end.
            while client.next_packet().await?.is_some() {}
            client
                .send_query(if rollback { b"ROLLBACK" } else { b"COMMIT" })
                .await?;
            let mut v = Vec::new();
            while let Some(q) = client.next_packet().await? {
                v.push(q);
            }
            Ok(v)
        }
        .await;
        cur.done = true;
        match r {
            Ok(pkts) => {
                let (last_ok, warnings, error) =
                    (cur.last_ok.clone(), cur.warnings, cur.error.clone());
                let mut failed = None;
                for q in &pkts {
                    if matches!(q.kind, PacketKind::Err(_)) {
                        failed = Some(q.clone());
                    }
                    cur.observe(q);
                }
                if rollback {
                    cur.error = error;
                    return Ok(p);
                }
                if let Some(f) = failed {
                    metrics::counter!("sprig_backend_wrapped_commit_failures_total", "cluster" => self.target.cluster.to_string())
                        .increment(1);
                    return Ok(f);
                }
                cur.last_ok = last_ok;
                cur.warnings = warnings;
                let status = cur.status.unwrap_or_default() - StatusFlags::IN_TRANS;
                cur.status = Some(status);
                Ok(without_in_trans(p))
            }
            Err(e) if e.is_connection_lost() => {
                cur.failed = true;
                let node = cur.lease.key().node_key();
                let stmt = cur.stmt.clone();
                self.backends.note_failure(&node, "io");
                Err(self.lost(&stmt, false, true, &node))
            }
            Err(e) => Err(internal(&e)),
        }
    }

    async fn next_packet(&mut self) -> Result<Option<RespPacket>> {
        let Some(cur) = self.cur.as_mut() else {
            return Ok(None);
        };
        if let Some(p) = cur.first.take() {
            cur.relayed = true;
            return Ok(Some(p));
        }
        if cur.done || cur.failed {
            return Ok(None);
        }
        let waited = Instant::now();
        let packet = cur.lease.conn().client.next_packet().await;
        crate::observe::backend_time(waited.elapsed());
        match packet {
            Ok(Some(mut p)) => {
                if cur.ended_silently(&p) {
                    p = cur.silent_abort(&p, &self.target.cluster, "a statement");
                }
                cur.observe(&p);
                cur.relayed = true;
                Ok(Some(p))
            }
            Ok(None) => {
                cur.done = true;
                Ok(None)
            }
            Err(e) if e.is_connection_lost() => {
                cur.failed = true;
                let node = cur.lease.key().node_key();
                let (stmt, in_trx, relayed) = (cur.stmt.clone(), cur.in_trx_before, cur.relayed);
                self.backends.note_failure(&node, "io");
                Err(self.lost(&stmt, in_trx, relayed, &node))
            }
            Err(e) => Err(internal(&e)),
        }
    }

    /// Sends a chunk of a `LOCAL INFILE` file.
    ///
    /// # Errors
    /// 39012 if no request is pending; lost-connection errors.
    pub async fn send_infile_data(&mut self, data: &[u8]) -> Result<()> {
        let cur = self
            .cur
            .as_mut()
            .ok_or_else(|| SprigError::internal("no statement"))?;
        cur.lease
            .conn()
            .client
            .send_infile_data(data)
            .await
            .map_err(|e| internal(&e))
    }

    /// Ends (or declines) a `LOCAL INFILE` file.
    ///
    /// # Errors
    /// As [`send_infile_data`](Self::send_infile_data).
    pub async fn finish_infile(&mut self) -> Result<()> {
        let cur = self
            .cur
            .as_mut()
            .ok_or_else(|| SprigError::internal("no statement"))?;
        cur.lease
            .conn()
            .client
            .finish_infile()
            .await
            .map_err(|e| internal(&e))
    }

    /// Ends the statement: reads what the caller left, applies session
    /// tracking and pins, then keeps the connection (pinned) or captures
    /// and releases it.
    ///
    /// # Errors
    /// Lost-connection errors while draining; `Internal` without a
    /// statement.
    pub async fn finish(&mut self) -> Result<StmtOutcome> {
        let pending = self.redo.as_mut().and_then(|r| r.pending.take());
        let r = self.finish_statement().await;
        if let (Some(sql), Ok(o)) = (pending, &r)
            && o.error.is_none()
        {
            let vars = self.vars.clone();
            if let Some(rec) = self.redo.as_mut() {
                rec.record(sql, o.affected_rows, o.last_insert_id, &vars);
            }
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    async fn finish_statement(&mut self) -> Result<StmtOutcome> {
        let Some(mut cur) = self.cur.take() else {
            return Err(SprigError::internal("finish without a statement"));
        };
        let node = cur.lease.key().node_key();
        let draining = Instant::now();
        if !cur.failed && !cur.done {
            if let Some(p) = cur.first.take() {
                drop(p);
            }
            loop {
                match cur.lease.conn().client.next_packet().await {
                    Ok(Some(p)) => cur.observe(&p),
                    Ok(None) => break,
                    Err(ClientError::Busy(_)) => {
                        if let Err(e) = cur.lease.conn().client.finish_infile().await {
                            cur.failed = true;
                            tracing::debug!(error = %e, "declining LOCAL INFILE failed");
                            break;
                        }
                    }
                    Err(e) => {
                        cur.failed = true;
                        if e.is_connection_lost() {
                            self.backends.note_failure(&node, "io");
                            let (stmt, in_trx, relayed) =
                                (cur.stmt.clone(), cur.in_trx_before, cur.relayed);
                            drop(cur);
                            return Err(self.lost(&stmt, in_trx, relayed, &node));
                        }
                        break;
                    }
                }
            }
        }
        crate::observe::backend_time(draining.elapsed());
        let outcome = |cur: &Current, pinned: bool| StmtOutcome {
            status: cur.status.unwrap_or_default(),
            warnings: cur.warnings,
            affected_rows: cur.last_ok.as_ref().map_or(0, |o| o.affected_rows),
            last_insert_id: cur.last_ok.as_ref().map_or(0, |o| o.last_insert_id),
            error: cur.error.clone(),
            node: node.clone(),
            route: cur.route,
            pinned,
            rows: cur.rows,
            examined: None,
        };
        if cur.failed {
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }
        if cur.error.as_ref().is_some_and(|e| conn_fatal(e.code)) {
            // The server ended this connection (killed, shutting down): never
            // reuse it; what it held is gone.
            self.pins.lose_connection();
            self.pending_capture.clear();
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }

        if cur.rollback_after {
            // A silent abort answered while the backend had opened another
            // transaction since: roll it back, as the client was told.
            if let Err(e) = cur.lease.conn().client.query_drop("ROLLBACK").await {
                tracing::warn!(node = %node, error = %e, "rolling back after a silent abort failed; closing the connection");
                self.pins.after_statement(StatusFlags::empty());
                let o = outcome(&cur, false);
                drop(cur);
                self.connection_gone();
                return Ok(o);
            }
        }

        // Session tracking: schema, variables, commit GTIDs, untracked state.
        let mut observed = state::observe(&cur.tracked);
        if Self::drop_set_statement_reports(&mut cur, &mut observed) {
            observed.unexplained = false;
        }
        if self.drop_txn_setting_reports(&mut observed) {
            observed.unexplained = false;
        }
        self.vars.apply(&observed);
        // The MySQL persona's sql_mode view.
        self.vars.mysql_sql_mode_after(&observed);
        cur.lease.conn().applied.apply(&observed);
        if observed.charset_changed() {
            // Tracking names the character set only: read the collation
            // back.
            let set_names = cur.stmt.sets_names();
            let conn = cur.lease.conn();
            match conn.capture_collation().await {
                Ok(coll) => {
                    conn.applied.charset_changed(&observed, &coll, set_names);
                    self.vars.charset_changed(&observed, &coll, set_names);
                }
                Err(e) => {
                    tracing::warn!(node = %node, error = %e, "reading collation_connection failed; closing the connection");
                    let o = outcome(&cur, false);
                    drop(cur);
                    self.connection_gone();
                    return Ok(o);
                }
            }
        }
        let unexplained = observed.unexplained && !cur.stmt.expects_state_change();
        if !observed.untracked.is_empty() || unexplained {
            if !self.pins.reasons().contains(&PinReason::Untracked) {
                self.count_pin(PinReason::Untracked);
            }
            tracing::info!(
                cluster = %self.target.cluster,
                untracked = ?observed.untracked,
                unexplained,
                "session state SprigDB can't replay: pinning the backend connection"
            );
            self.pins.set_untracked();
        }
        if !cur.stmt.sets_gateway_vars.is_empty()
            && cur.error.is_none()
            && let Err(e) = self.restore_gateway_vars(&mut cur).await
        {
            tracing::warn!(node = %node, error = %e, "restoring the gateway's session variables failed; closing the connection");
            let o = outcome(&cur, false);
            drop(cur);
            self.connection_gone();
            return Ok(o);
        }
        // MariaDB 11.8 reports another variable (`timestamp`, on a
        // connection with a fixed clock: a held branch, or a client's `SET
        // timestamp`) in place of `last_gtid` after a commit. A statement
        // that may have committed and reported variables but no GTID reads
        // the GTID itself, or the session's next read could go to a
        // replica that hasn't applied its own commit.
        let committing = cur.stmt.effects.contains(&Effect::Commit)
            || (cur.stmt.class == StmtClass::Write
                && !cur
                    .status
                    .is_some_and(|st| st.contains(StatusFlags::IN_TRANS)));
        if committing
            && cur.error.is_none()
            && observed.gtid.is_none()
            && !cur.tracked.vars.is_empty()
        {
            match cur.lease.conn().capture_last_gtid().await {
                Ok(g) if !g.is_empty() => observed.gtid = Some(g),
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(node = %node, error = %e, "reading last_gtid after a commit failed; closing the connection");
                    let o = outcome(&cur, false);
                    drop(cur);
                    self.connection_gone();
                    return Ok(o);
                }
            }
            metrics::counter!("sprig_backend_gtid_reads_total", "cluster" => self.target.cluster.to_string())
                .increment(1);
        }
        if let Some(g) = observed
            .gtid
            .as_deref()
            .and_then(|g| GtidSet::parse(g).ok())
        {
            self.causal.union(&g);
            self.backends
                .watermarks
                .advance(&self.target.cluster, &self.target.user, &g);
        }
        if let Some(ok) = &cur.last_ok {
            if ok.last_insert_id != 0 && !cur.explicit_ids {
                self.vars.last_insert_id = ok.last_insert_id;
                cur.lease.conn().applied.last_insert_id = ok.last_insert_id;
            }
            self.captured.row_count = if cur.resultset {
                -1
            } else {
                i64::try_from(ok.affected_rows).unwrap_or(i64::MAX)
            };
        }
        for r in self.pins.apply_effects(&cur.stmt.effects) {
            self.count_pin(r);
        }
        if let Some(st) = cur.status
            && self.pins.after_statement(st)
        {
            self.count_pin(PinReason::Transaction);
        }
        // The coordinator's settings, as its statement left them.
        if let Some(changes) = &self.txn_ctl
            && cur.error.is_none()
        {
            for &(setting, changed) in changes {
                self.pins.set_txn_setting(setting, changed);
            }
        }
        if cur.takes_locks && self.pins.in_transaction() {
            self.trx_locks = true;
        }
        for v in &cur.stmt.assigns_user_vars {
            if !self.pending_capture.contains(v) {
                self.pending_capture.push(v.clone());
            }
        }
        // A failed statement (an interrupted one too) may have left a
        // definer routine's context in `@sprig_rls`; it is sent again
        // before the connection's next statement.
        if cur.error.is_some()
            && self.vars.user_vars.get("sprig_rls").is_some_and(|v| {
                matches!(v, super::state::VarValue::Text(t) if t.windows(5).any(|w| w == b"\"d\":{"))
            })
        {
            cur.lease.conn().applied.user_vars.remove("sprig_rls");
        }

        let route = cur.route;
        if dirty_after_use(
            cur.sets_schema,
            cur.lease.conn().applied.schema.as_deref(),
            self.vars.schema.as_deref(),
        ) {
            // A failed `USE` (unknown database, access denied, killed) on a
            // connection that had another default schema: the connection
            // still has it and the session doesn't, so it must not serve
            // this session again, or anyone: close it.
            tracing::debug!(node = %node, "a USE failed on a connection with another default schema; closing it");
            self.pins.lose_connection();
            let o = outcome(&cur, false);
            drop(cur);
            return Ok(o);
        }
        if route.reader() && !self.pins.is_empty() {
            // State appeared on a reader: it can't follow the session to the
            // writer. Drop that connection and keep the session conservative.
            tracing::warn!(
                node = %node,
                pins = ?self.pins.reasons(),
                tracked = ?cur.tracked,
                "unexpected session state on a reader; discarding it"
            );
            self.pins.lose_connection();
            self.pins.set_untracked();
            let o = outcome(&cur, true);
            drop(cur);
            return Ok(o);
        }
        if !self.pins.is_empty() {
            let o = outcome(&cur, true);
            self.held = Some((cur.lease, route));
            return Ok(o);
        }
        let mut o = outcome(&cur, false);
        let mut ok = self.capture(&mut cur).await;
        if let Some(before) = cur.probe.filter(|_| ok && cur.error.is_none()) {
            match cur.lease.conn().probe_end(before).await {
                Ok(n) => {
                    o.examined = Some(n);
                    metrics::counter!("sprig_workload_probes_run_total").increment(1);
                }
                Err(e) => {
                    tracing::debug!(node = %node, error = %e, "Handler_read_* probe failed; not measuring");
                    ok = !e.is_connection_lost();
                }
            }
        }
        if ok && self.keep && cur.error.is_none() {
            // Kept for the session's next statement (`set_keep`).
            self.release_kept();
            self.kept = Some(Box::new(cur.lease));
            return Ok(o);
        }
        let limits = self.backends.limits(&self.target.cluster);
        self.backends.pools.release(
            cur.lease,
            if ok { Release::Reuse } else { Release::Discard },
            limits,
        );
        Ok(o)
    }

    /// The captures before a release; false if one failed (the
    /// connection is then discarded).
    async fn capture(&mut self, cur: &mut Current) -> bool {
        let conn = cur.lease.conn();
        let mut ran = false;
        let mut ok = true;
        if cur.warnings > 0 {
            ran = true;
            match conn.capture_warnings().await {
                Ok(w) => self.captured.warnings = w,
                Err(_) => ok = false,
            }
        } else {
            self.captured.warnings.clear();
        }
        if cur.stmt.calc_found_rows && ok {
            ran = true;
            match conn.capture_found_rows().await {
                Ok(n) => self.captured.found_rows = n,
                Err(_) => ok = false,
            }
        } else if cur.resultset {
            self.captured.found_rows = cur.rows;
        }
        if !self.pending_capture.is_empty() && ok {
            ran = true;
            let names = std::mem::take(&mut self.pending_capture);
            match conn.capture_user_vars(&names).await {
                Ok(vals) => {
                    for (n, v) in vals {
                        conn.applied.user_vars.insert(n.clone(), v.clone());
                        self.vars.user_vars.insert(n, v);
                    }
                }
                Err(_) => ok = false,
            }
        }
        if ran {
            metrics::counter!("sprig_backend_state_syncs_total", "cluster" => self.target.cluster.to_string(), "kind" => "capture")
                .increment(1);
        }
        conn.client.take_changes();
        ok
    }

    /// `SET STATEMENT` quirks of MariaDB's tracking: it reports the
    /// temporary values as changes, though they revert when the statement
    /// ends, and reports each revert later, with the next statement on that
    /// connection that changes any session state. Neither is a change by
    /// the statement at hand: temporary values are dropped, and so are
    /// reverts of variables outside the allowed list, which would pin the
    /// session `Untracked` (an allowed variable's revert is its true value
    /// and applies as usual). A `CALL` may change any variable itself, so
    /// its reports are kept. Returns whether a report was dropped.
    fn drop_set_statement_reports(cur: &mut Current, observed: &mut state::Observed) -> bool {
        let applied = &mut cur.lease.conn().applied;
        let pending = std::mem::take(&mut applied.scoped_pending);
        let scoped = &cur.stmt.scoped_vars;
        let call = cur.stmt.effects.contains(&Effect::Call);
        let before = observed.vars.len() + observed.untracked.len();
        observed.vars.retain(|(n, _)| !scoped.contains(n));
        observed
            .untracked
            .retain(|n| !scoped.contains(n) && (call || !pending.contains(n)));
        let reported = |n: &String| cur.tracked.vars.iter().any(|(r, _)| r == n);
        applied.scoped_pending = pending
            .into_iter()
            .filter(|n| !reported(n) || scoped.contains(n))
            .chain(scoped.iter().cloned())
            .collect();
        observed.vars.len() + observed.untracked.len() != before
    }

    /// MariaDB's reports of the transaction coordinator's own settings. They
    /// aren't session state to replay or pin as `Untracked`: the
    /// coordinator's statements keep [`PinReason::TxnSettings`] instead,
    /// until it puts them back. A coordinator statement's reports of
    /// `innodb_lock_wait_timeout` and `timestamp` are dropped, and so is
    /// `timestamp` while the coordinator's clock is fixed (MariaDB reports
    /// it again after a commit on such a connection; whatever set it, the
    /// coordinator's reset to `DEFAULT` comes before the connection can
    /// leave). Returns whether the statement's state change is then
    /// explained: a coordinator statement changes nothing else, a client
    /// statement only if nothing else was reported.
    fn drop_txn_setting_reports(&self, observed: &mut state::Observed) -> bool {
        let coordinator = self.txn_ctl.is_some();
        if !coordinator && !self.pins.txn_setting(TxnSetting::Clock) {
            return false;
        }
        let before = observed.untracked.len();
        observed.untracked.retain(|n| match n.as_str() {
            "timestamp" => false,
            "innodb_lock_wait_timeout" => !coordinator,
            _ => true,
        });
        coordinator
            || (observed.untracked.len() != before
                && observed.untracked.is_empty()
                && observed.vars.is_empty()
                && observed.schema.is_none())
    }

    /// A client statement set gateway-owned variables: keep the client's
    /// values for the gateway to answer, put the gateway's back on the
    /// connection, and resync the allowed variables tracking may have
    /// missed while the client's settings were in force.
    async fn restore_gateway_vars(
        &mut self,
        cur: &mut Current,
    ) -> std::result::Result<(), ClientError> {
        let names = cur.stmt.sets_gateway_vars.clone();
        let conn = cur.lease.conn();
        let restored = conn.restore_gateway_vars(&names).await?;
        for (n, v) in restored.client {
            self.vars.gateway_vars.insert(n, v);
        }
        let base = Arc::clone(&conn.baseline);
        for (n, v) in restored.allowed {
            let have = conn
                .applied
                .vars
                .get(&n)
                .map(String::as_str)
                .or(base.get(&n));
            if have != Some(v.as_str()) {
                conn.applied.vars.insert(n.clone(), v.clone());
                self.vars.vars.insert(n, v);
            }
        }
        conn.applied.wsrep_sync_wait = 0;
        Ok(())
    }

    /// The session's connection is gone outside a statement: what it held
    /// is lost, and the next statement reports a lost transaction (1213)
    /// or other pinned state (39033).
    fn connection_gone(&mut self) {
        self.held = None;
        self.pending_capture.clear();
        let in_trx = self.pins.in_transaction();
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.deferred = if lost_state {
            Some(SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            ))
        } else if in_trx {
            Some(trx_rolled_back())
        } else {
            None
        };
    }

    /// The client set `wsrep_sync_wait` itself: it asks for causal reads
    /// across gateways, which the shard's writer gives, so its plain reads
    /// stay there.
    fn client_sync_wait(&self) -> bool {
        self.vars
            .gateway_vars
            .get("wsrep_sync_wait")
            .is_some_and(|v| !v.parse::<u32>().is_ok_and(|n| n == 0))
    }

    /// MariaDB keeps a connection's database-level privileges from its last
    /// `USE` (or its login). Pooled connections of an account whose grants
    /// changed are retired, but a pinned session keeps its connection, and
    /// after `rls.backend_enforcement` moved from `views` to `triggers` its
    /// reads would fail with 1142 until it reconnected. So a held
    /// connection opened (or refreshed) before the account's last
    /// retirement sends `USE` of the session's database again before its
    /// next statement.
    fn refresh_grants(&mut self, lease: &mut BackendLease) {
        let Some(at) = self.backends.pools.retired_at(&lease.key().account) else {
            return;
        };
        let conn = lease.conn();
        let since = match self.grants_seen {
            Some((id, t)) if id == conn.thread_id => t.max(conn.created_at),
            _ => conn.created_at,
        };
        if at <= since {
            return;
        }
        self.grants_seen = Some((conn.thread_id, std::time::Instant::now()));
        if self.vars.schema.is_some() {
            // The state sync then sends `USE` (state::sync_prefix).
            conn.applied.schema = None;
            tracing::debug!(
                thread = conn.thread_id,
                "account's grants changed: USE again on the pinned connection"
            );
        }
    }

    fn count_pin(&self, r: PinReason) {
        metrics::counter!("sprig_backend_pins_total", "cluster" => self.target.cluster.to_string(), "reason" => r.as_str())
            .increment(1);
    }

    /// The error for a connection lost mid-statement; the session forgets
    /// what the connection held.
    fn lost(&mut self, stmt: &StmtInfo, in_trx: bool, relayed: bool, node: &NodeKey) -> SprigError {
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.held = None;
        self.pending_capture.clear();
        let (code, kind) = if stmt.commits() || (stmt.class == StmtClass::Write && !in_trx) {
            tracing::warn!(
                event = "backend.outcome_unknown",
                cluster = %self.target.cluster,
                node = %node,
                "backend lost during a commit or an autocommit write"
            );
            (Some(SprigCode::OutcomeUnknown), "outcome_unknown")
        } else if in_trx && !lost_state {
            (None, "transaction_rolled_back")
        } else if lost_state {
            (Some(SprigCode::SessionStateLost), "state_lost")
        } else if relayed {
            (Some(SprigCode::BackendLost), "lost_mid_result")
        } else {
            (Some(SprigCode::BackendUnavailable), "lost")
        };
        metrics::counter!("sprig_backend_failures_total", "cluster" => self.target.cluster.to_string(), "kind" => kind)
            .increment(1);
        match code {
            Some(SprigCode::OutcomeUnknown) if stmt.commits() => SprigError::new(
                SprigCode::OutcomeUnknown,
                "the backend connection was lost during a commit; the outcome is unknown",
            ),
            Some(SprigCode::OutcomeUnknown) => SprigError::new(
                SprigCode::OutcomeUnknown,
                "the backend connection was lost while the statement ran; whether it took effect is unknown",
            ),
            Some(SprigCode::SessionStateLost) => SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            ),
            Some(SprigCode::BackendLost) => SprigError::new(
                SprigCode::BackendLost,
                "the backend connection was lost while sending the result",
            ),
            Some(c) => SprigError::new(c, "the backend connection was lost"),
            None => trx_rolled_back(),
        }
    }

    /// The error for a held connection the server closed before this
    /// statement was sent: nothing ran, so only what the connection held is
    /// lost. 1213 for an open transaction (rolled back), 39033 for other
    /// pinned state.
    fn lost_unsent(&mut self, node: &NodeKey) -> SprigError {
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.held = None;
        self.pending_capture.clear();
        let kind = if lost_state {
            "state_lost"
        } else {
            "transaction_rolled_back"
        };
        tracing::debug!(node = %node, kind, "held backend connection lost between statements");
        metrics::counter!("sprig_backend_failures_total", "cluster" => self.target.cluster.to_string(), "kind" => kind)
            .increment(1);
        if lost_state {
            SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost",
            )
        } else {
            trx_rolled_back()
        }
    }

    /// Whether the session runs with `autocommit = 0`, so its next
    /// statement opens a transaction.
    fn implicit_transactions(&self) -> bool {
        self.vars
            .vars
            .get("autocommit")
            .is_some_and(|v| v.eq_ignore_ascii_case("OFF") || v == "0")
    }

    /// Whether the session's transactions are `SERIALIZABLE`, where plain
    /// reads take shared locks too.
    fn serializable(&self) -> bool {
        ["transaction_isolation", "tx_isolation"].iter().any(|k| {
            self.vars
                .vars
                .get(*k)
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("SERIALIZABLE"))
        })
    }

    /// The causal target of a read.
    fn causal_target(&self, settings: &BackendSettings) -> GtidSet {
        let mut t = self.causal.clone();
        if settings.causal_scope == CausalScope::TenantGateway {
            t.union(
                &self
                    .backends
                    .watermarks
                    .get(&self.target.cluster, &self.target.user),
            );
        }
        t
    }

    async fn lease(
        &self,
        view: &ClusterView,
        node: &NodeRef,
        settings: &BackendSettings,
        deadline: Instant,
    ) -> std::result::Result<BackendLease, AcquireError<ClientError>> {
        let b = &self.backends;
        let key = PoolKey {
            cluster: view.id.clone(),
            node: node.node.clone(),
            account: self.target.user.clone(),
            found_rows: self.opts.found_rows,
            mariadb_caps: self.opts.mariadb_caps.bits(),
            relay: self.relay_via(view),
            branch: self.opts.txn_branch,
        };
        let node_key = view.key(&node.node);
        let budget = b.budget(&node_key);
        let vars = &self.vars;
        let started = Instant::now();
        let r = b
            .pools
            .acquire(
                &key,
                // The gateway keeps wsrep_sync_wait at 0: the Galera check
                // sets it for one statement only.
                |base| vars.digest(0, base),
                b.limits(&view.id),
                deadline,
                || async {
                    let cred = self
                        .target
                        .credential()
                        .map_err(|e| ClientError::Auth(e.message))?;
                    let tls = b.tls_for(&cred).map_err(|e| ClientError::Tls(e.message))?;
                    let spec = ConnectSpec {
                        node: node_key.clone(),
                        addr: node.addr.clone(),
                        user: cred.user.clone(),
                        password: zeroize::Zeroizing::new(cred.password().to_owned()),
                        tls,
                        found_rows: self.opts.found_rows,
                        mariadb_caps: self.opts.mariadb_caps,
                        galera: view.topology == TopologyKind::Galera,
                        compress: false,
                        connect_timeout: settings.connect_timeout,
                        net_write_timeout_secs: settings.net_write_timeout_secs,
                        wait_timeout_secs: settings.idle_timeout.as_secs().max(300) + 300,
                        preamble: key.relay.map(relay_preamble),
                    };
                    let r = conn::connect(&spec, Arc::clone(&budget)).await;
                    let outcome = if r.is_ok() { "ok" } else { "error" };
                    metrics::counter!("sprig_backend_connects_total", "cluster" => view.id.to_string(), "outcome" => outcome)
                        .increment(1);
                    if let Ok(c) = &r {
                        budget.set_limit(c.max_prepared, settings.prepared_budget_pct, b.gateways_in_dc);
                    }
                    r
                },
            )
            .await;
        stmt_metrics::pool_wait(&view.id).record(started.elapsed().as_secs_f64());
        r
    }

    /// The tenant whose connections to `view`'s cluster go through its
    /// relay writer: the session is on the relay path and the cluster has
    /// relays.
    fn relay_via(&self, view: &ClusterView) -> Option<sprig_common::ids::TenantId> {
        use sprig_cluster::failover::relay::RelayState;
        self.opts.relay.filter(|_| view.relay != RelayState::None)
    }

    /// This session is a transaction coordinator's branch: its connections
    /// come from the branch pools ([`SessionOpts::txn_branch`]).
    pub fn set_txn_branch(&mut self) {
        self.opts.txn_branch = true;
    }

    /// This session may not hold a Sprig 2PC branch (the relay path on a
    /// cluster without a ready relay writer).
    pub fn forbid_redo(&mut self) {
        self.redo_off = true;
    }

    /// The tenant whose connections go through relays, if any.
    #[must_use]
    pub fn relay(&self) -> Option<sprig_common::ids::TenantId> {
        self.opts.relay
    }

    /// Changes the session's relay path (a settings change): connections
    /// leased from now on follow it.
    pub fn set_relay(&mut self, relay: Option<sprig_common::ids::TenantId>) {
        self.opts.relay = relay;
    }

    /// Whether `cluster` takes this session's connections through a ready
    /// relay writer (a held branch there is fenced by the relay).
    ///
    /// # Errors
    /// As [`Backends::view`].
    pub async fn relay_ready(&self, cluster: &ClusterId) -> Result<Option<RelayLease>> {
        use sprig_cluster::failover::relay::RelayState;
        if self.opts.relay.is_none() {
            return Ok(None);
        }
        Ok(match &self.backends.view(cluster).await?.relay {
            RelayState::Ready(l) => Some(l.clone()),
            _ => None,
        })
    }

    /// A relayed tenant's connection: every statement, reads included, goes
    /// to the relay writer, through its relay. While the cluster has no
    /// ready relay writer (a takeover recovering held branches), it waits
    /// until the deadline, then fails with 39448 (retryable).
    async fn relay_lease(
        &mut self,
        view: &ClusterView,
        settings: &BackendSettings,
        deadline: Instant,
    ) -> Result<(BackendLease, RouteKind, Wait)> {
        use sprig_cluster::failover::relay::RelayState;
        let mut fresh: Option<Arc<ClusterView>> = None;
        loop {
            let v = fresh.as_deref().unwrap_or(view);
            let why = match &v.relay {
                RelayState::Ready(l) => {
                    let node = NodeRef {
                        node: NodeId::new(&l.node_id)
                            .map_err(|e| SprigError::internal(format!("relay lease node: {e}")))?,
                        addr: l.relay_addr.clone(),
                    };
                    if let Some(k) = self.take_kept(v, &node) {
                        return Ok((k, RouteKind::Writer, Wait::None));
                    }
                    match Box::pin(self.lease(v, &node, settings, deadline)).await {
                        Ok(k) => return Ok((k, RouteKind::Writer, Wait::None)),
                        Err(AcquireError::Timeout) => return Err(pool_timeout()),
                        Err(AcquireError::Connect(e)) => match e.server_error() {
                            Some(err) if err.code == sprig_txn::fence::FENCED_ERRNO => {
                                format!("the relay refused: {}", err.message)
                            }
                            Some(err) => {
                                return Err(unavailable(format!(
                                    "backend login through the relay on {} failed: {} {}",
                                    node.node, err.code, err.message
                                )));
                            }
                            None => format!("relay {}: {e}", node.addr),
                        },
                    }
                }
                RelayState::Unavailable(why) => why.clone(),
                RelayState::None => {
                    return Err(unavailable("the cluster's relays went away"));
                }
            };
            if Instant::now() >= deadline {
                return Err(SprigError::new(
                    SprigCode::HeldBranchFenced,
                    format!(
                        "cluster {} has no ready relay writer ({why}); retry",
                        view.id
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            fresh = Some(self.backends.view(&view.id).await?);
        }
    }

    /// Chooses the node (writer, or a reader for a plain read) and leases a
    /// connection to it. Readers that can't be reached are skipped; the
    /// writer is retried until the deadline.
    #[allow(clippy::too_many_lines)]
    async fn route_lease(
        &mut self,
        view: &ClusterView,
        info: &StmtInfo,
        settings: &BackendSettings,
        force_writer: bool,
        exclude: &mut Vec<NodeId>,
        deadline: Instant,
    ) -> Result<(BackendLease, RouteKind, Wait)> {
        // On Galera, user-level lock functions run on one node of the
        // cluster, the writer of `LOCK_NODE_KEY` that every gateway's
        // rendezvous agrees on, so sessions on any gateway (and any shard)
        // meet on the same locks. Lock names are scoped per tenant, so
        // tenants share the node.
        let shard = if info.user_lock && view.topology == TopologyKind::Galera {
            LOCK_NODE_KEY
        } else {
            self.opts.shard
        };
        // With autocommit = 0 every statement, a plain read too, opens a
        // transaction: it belongs on the writer, where the rest of that
        // transaction runs.
        let plain = info.class == StmtClass::Read
            && self.pins.is_empty()
            && !force_writer
            && !self.implicit_transactions()
            && !self.client_sync_wait();
        let mut last_err: Option<String> = None;
        if self.relay_via(view).is_some() {
            return self.relay_lease(view, settings, deadline).await;
        }
        loop {
            let writer = self.backends.writers.writer(view, shard);
            let (node, route, wait) = if plain {
                let target = self.causal_target(settings);
                let pool = &self.backends.pools;
                let load = |k: &NodeKey| pool.node_counts(k).1;
                let choice = reads::choose(
                    view,
                    writer.as_ref().ok(),
                    &self.backends.nodes,
                    &load,
                    &target,
                    settings,
                    exclude,
                    self.backends
                        .read_spread
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                );
                stmt_metrics::read(
                    &view.id,
                    choice.as_str(),
                    if target.is_empty() { "no" } else { "yes" },
                )
                .increment(1);
                match choice {
                    ReadTarget::Writer => match &writer {
                        Ok(w) => (w.clone(), RouteKind::Writer, Wait::None),
                        Err(e) => {
                            if Instant::now() >= deadline {
                                return Err(unavailable(
                                    last_err.unwrap_or_else(|| e.message.clone()),
                                ));
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    },
                    ReadTarget::Replica { node, wait_for } => match wait_for {
                        Some(t) => (node, RouteKind::ReplicaWait, Wait::Replica(t)),
                        None => (node, RouteKind::Replica, Wait::None),
                    },
                    ReadTarget::GaleraNode {
                        node,
                        sync_wait,
                        confirmed,
                    } => {
                        let galera_writer =
                            writer.as_ref().ok().is_some_and(|w| w.node == node.node);
                        if sync_wait && !confirmed {
                            (node, RouteKind::GaleraUnconfirmed, Wait::Galera)
                        } else if sync_wait {
                            (node, RouteKind::GaleraSyncWait, Wait::Galera)
                        } else if galera_writer {
                            (node, RouteKind::Writer, Wait::None)
                        } else {
                            (node, RouteKind::Galera, Wait::None)
                        }
                    }
                }
            } else {
                match writer {
                    Ok(w) => (w, RouteKind::Writer, Wait::None),
                    Err(e) => {
                        if Instant::now() >= deadline {
                            return Err(unavailable(last_err.unwrap_or(e.message)));
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
            };
            if let Some(l) = self.take_kept(view, &node) {
                return Ok((l, route, wait));
            }
            // Boxed: the pool's acquire (with its connect) is large, and a
            // kept connection skips it.
            match Box::pin(self.lease(view, &node, settings, deadline)).await {
                Ok(l) => return Ok((l, route, wait)),
                Err(AcquireError::Timeout) => return Err(pool_timeout()),
                Err(AcquireError::Connect(e)) => {
                    if let Some(err) = e.server_error() {
                        // The node answered: a login or setup problem, not a
                        // node failure.
                        return Err(unavailable(format!(
                            "backend login to {} failed: {} {}",
                            node.node, err.code, err.message
                        )));
                    }
                    let key = view.key(&node.node);
                    tracing::debug!(node = %key, error = %e, "backend connect failed");
                    self.backends.note_failure(&key, "connect");
                    last_err = Some(format!("{}: {e}", node.addr));
                    if route != RouteKind::Writer {
                        exclude.push(node.node.clone());
                        continue;
                    }
                    if Instant::now() >= deadline {
                        return Err(unavailable(last_err.unwrap_or_default()));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// [`run_statement`](Self::run_statement) inside a `backend.query`
    /// span, adding its time to the request's backend time.
    async fn run(&mut self, info: &StmtInfo, req: Req<'_>) -> Result<()> {
        use sprig_common::telemetry::trace::ChildSpan;
        // One `tracing` span per request (the caller's `gateway.request`);
        // this step is a light child of it.
        let mut span = ChildSpan::start("backend.query");
        if let Some(sp) = span.as_mut() {
            sp.record("cluster", &self.target.cluster);
        }
        self.trace_ctx = span.as_ref().map(ChildSpan::context);
        let started = Instant::now();
        let r = self.run_statement(info, req).await;
        self.trace_ctx = None;
        crate::observe::backend_time(started.elapsed());
        if let Some(mut sp) = span {
            if r.is_err() {
                sp.set_error();
            }
            if let Some(c) = self.cur.as_mut() {
                sp.record("node", c.lease.key().node_key());
                sp.record("route", format_args!("{:?}", c.route));
                c.span = Some(sp);
            }
        }
        r
    }

    #[allow(clippy::too_many_lines)]
    async fn run_statement(&mut self, info: &StmtInfo, mut req: Req<'_>) -> Result<()> {
        if self.cur.is_some() {
            let _ = self.finish().await;
        }
        self.inflight = None;
        if let Some(e) = self.deferred.take() {
            return Err(e);
        }
        let cluster = self.target.cluster.clone();
        let view = self.backends.view(&cluster).await?;
        self.backends.ensure_monitor(&view, &self.target);
        let settings = self.backends.settings(&cluster);
        let deadline = Instant::now() + settings.acquire_timeout;
        let in_trx_before = self.pins.in_transaction();
        let mut exclude: Vec<NodeId> = Vec::new();
        let mut force_writer = false;
        let mut sync_retried = false;
        let mut io_retried = false;
        let text = matches!(req, Req::Text(_));
        let galera = view.topology == TopologyKind::Galera;
        let stmt_sql = match &req {
            Req::Text(sql) | Req::Binary { sql, .. } if info.effects.is_empty() => Some(*sql),
            _ => None,
        };
        let galera_dml_in_trx = galera && in_trx_before && stmt_sql.is_some_and(plain_dml);
        let opens_implicit = galera
            && !in_trx_before
            && stmt_sql.is_some_and(plain_write)
            && self.implicit_transactions()
            && !self
                .pins
                .reasons()
                .iter()
                .any(|r| matches!(r, PinReason::TempTables(_)));
        if !in_trx_before {
            // The next transaction holds no locks yet, unless all its reads
            // lock or `SET TRANSACTION` set its characteristics.
            self.trx_locks = galera && (self.pins.next_transaction_set() || self.serializable());
        }
        // A write in such a transaction runs only if the transaction is
        // still open (`TRX_GUARD`). Galera aborts a transaction only for
        // locks another node's write set needs, so before the first write
        // or locking read the guard is left out: nothing can have aborted
        // the transaction yet, and an abort for the write's own locks shows
        // in its answer.
        let guard = galera_dml_in_trx && info.class == StmtClass::Write && self.trx_locks;
        // Anything but a plain read (which the statement check covers)
        // turns the guard on for the rest of the transaction.
        let takes_locks = galera
            && (info.class == StmtClass::Write || !stmt_sql.is_some_and(plain_dml))
            && !matches!(req, Req::Prepare { .. })
            && !info.effects.contains(&Effect::Begin);
        // `USE x` sets the schema itself, so any pooled connection serves
        // it, including one that already has a default schema.
        let sets_schema = matches!(req, Req::Text(sql) if is_use(sql));
        // Running it twice changes nothing: a lost connection retries it.
        let harmless = info.class == StmtClass::Read || matches!(req, Req::Prepare { .. });
        let explicit_ids = std::mem::take(&mut self.explicit_ids_next);
        let probe = std::mem::take(&mut self.probe_next)
            && matches!(req, Req::Text(_) | Req::Binary { .. });
        loop {
            let (mut lease, route, wait) = match self.held.take() {
                Some((mut l, _)) => {
                    if l.conn().client.closed_while_idle() {
                        // The server closed the held connection between
                        // statements (KILL CONNECTION, a restart): this
                        // statement never reached it.
                        let node = l.key().node_key();
                        drop(l);
                        tracing::debug!(node = %node, "held backend connection closed while idle");
                        if self.pins.in_transaction() || self.pins.holds_state() {
                            return Err(self.lost_unsent(&node));
                        }
                        // Only the session mode held it: the state replays.
                        continue;
                    }
                    (l, RouteKind::Pinned, Wait::None)
                }
                None => {
                    self.route_lease(&view, info, &settings, force_writer, &mut exclude, deadline)
                        .await?
                }
            };
            if route == RouteKind::Pinned {
                self.refresh_grants(&mut lease);
            }
            let node_key = lease.key().node_key();
            self.inflight = Some((node_key.clone(), lease.conn_ref().thread_id));
            let conn = lease.conn();
            // A Galera COMMIT of a transaction that wrote must produce a
            // GTID: the connection's last one, to compare.
            let commit_check = (view.topology == TopologyKind::Galera
                && in_trx_before
                && info.effects.contains(&Effect::Commit)
                && !self
                    .pins
                    .reasons()
                    .iter()
                    .any(|r| matches!(r, PinReason::TempTables(_)))
                && conn.client.tracker().trx_state().is_some_and(trx_wrote))
            .then(|| conn.client.tracker().last_gtid().map(str::to_owned));
            let wrote_before =
                galera_dml_in_trx && conn.client.tracker().trx_state().is_some_and(trx_wrote);
            if self.vars.schema.is_none()
                && conn.applied.schema.is_some()
                && route != RouteKind::Pinned
                && !sets_schema
            {
                // MariaDB can't unset the default schema: this connection can't
                // serve a session without one. New connections have none.
                // (The session's own `USE` doesn't drop it: otherwise every
                // session opened with a default database would close a pooled
                // connection and open a new one.)
                drop(lease);
                continue;
            }
            // User variables the last statement assigned on this held
            // connection are captured before this statement's sync, which
            // would otherwise write the session's older value over them: a
            // pinned session's `SET @v = ...` and then a read of `@v`, on a
            // pooled connection that held `@v` for an earlier session, read
            // NULL.
            if route == RouteKind::Pinned && !self.pending_capture.is_empty() {
                let names = std::mem::take(&mut self.pending_capture);
                match conn.capture_user_vars(&names).await {
                    Ok(vals) => {
                        for (n, v) in vals {
                            conn.applied.user_vars.insert(n.clone(), v.clone());
                            self.vars.user_vars.insert(n, v);
                        }
                    }
                    Err(e) => {
                        tracing::debug!(node = %node_key, error = %e, "user-variable capture failed");
                        self.pending_capture = names;
                        if e.is_connection_lost() {
                            drop(lease);
                            if self.pins.in_transaction() || self.pins.holds_state() {
                                return Err(self.lost_unsent(&node_key));
                            }
                            continue;
                        }
                    }
                }
            }
            // The probe's first reading, on a connection that will be free
            // again after the statement (see `probe_next_statement`).
            let probe_before = if probe && route != RouteKind::Pinned && self.pins.is_empty() {
                match conn.probe_start().await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        tracing::debug!(node = %node_key, error = %e, "Handler_read_* probe failed; not measuring");
                        if e.is_connection_lost() {
                            drop(lease);
                            continue;
                        }
                        None
                    }
                }
            } else {
                None
            };
            let base = Arc::clone(&conn.baseline);
            self.vars.settle_login(&base);
            // An autocommit write on a relayed connection never commits in
            // the round trip that ran it: a write waiting for a held
            // branch's rows when the relay dies would otherwise commit
            // although its client is gone.
            let wrapped = conn.relayed
                && !in_trx_before
                && !self.pins.in_transaction()
                && !self.implicit_transactions()
                && stmt_sql.is_some_and(plain_write);
            let (mut prefix, next_applied) =
                match state::sync_prefix(&conn.applied, &base, &self.vars, info, 0) {
                    Ok(x) => x,
                    Err(state::Unreplayable(why)) => {
                        tracing::warn!(%why, "session state can't be replayed");
                        drop(lease);
                        self.pins.lose_connection();
                        return Err(SprigError::new(
                            SprigCode::SessionStateLost,
                            "this session's state can't be replayed on a backend connection",
                        ));
                    }
                };
            if wrapped {
                prefix.push("START TRANSACTION".to_owned());
            }
            let kind = match (prefix.is_empty(), text) {
                (true, _) => "none",
                (false, true) => "prefix",
                (false, false) => "round_trip",
            };
            stmt_metrics::state_sync(&cluster, kind).increment(1);

            // Sync (and wait), then send.
            let wait_started = Instant::now();
            let galera_timeout = settings.galera_sync_wait_timeout;
            let sent: std::result::Result<Sent, ClientError> = async {
                if let Req::Text(sql) = &req {
                    let mut all = Vec::with_capacity(sql.len() + 256);
                    for p in &prefix {
                        all.extend_from_slice(p.as_bytes());
                        all.extend_from_slice(b"; ");
                    }
                    if guard {
                        all.extend_from_slice(TRX_GUARD.as_bytes());
                        all.extend_from_slice(b"; ");
                    }
                    match &wait {
                        Wait::Replica(t) => {
                            all.extend_from_slice(
                                reads::wait_prefix(t, settings.gtid_wait_timeout).as_bytes(),
                            );
                            all.extend_from_slice(b"; ");
                        }
                        Wait::Galera => {
                            all.extend_from_slice(reads::GALERA_SYNC_CHECK.as_bytes());
                            all.extend_from_slice(b"; ");
                        }
                        Wait::None => {}
                    }
                    // Sampled statements carry their trace context to
                    // MariaDB, after fingerprinting.
                    if let Some(tc) = self
                        .trace_ctx
                        .or_else(sprig_common::telemetry::trace::current)
                        && tc.sampled
                    {
                        all.extend_from_slice(b"/* traceparent='");
                        all.extend_from_slice(tc.traceparent().as_bytes());
                        all.extend_from_slice(b"' */ ");
                    }
                    all.extend_from_slice(sql);
                    conn.client.send_query(&all).await?;
                    for _ in 0..prefix.len() {
                        if let Some(e) = consume_statement(conn).await? {
                            return Ok(Sent::SyncErr(e));
                        }
                    }
                    if guard && let Some(e) = consume_statement(conn).await? {
                        return Ok(Sent::TrxGone(reported(e)));
                    }
                    return Ok(match &wait {
                        Wait::None => Sent::Ready,
                        Wait::Replica(_) => match consume_statement(conn).await? {
                            None => Sent::Ready,
                            Some(e) if e.code == reads::WAIT_TIMEOUT_ERRNO => Sent::WaitFailed,
                            Some(e) => Sent::SyncErr(e),
                        },
                        Wait::Galera => {
                            match tokio::time::timeout(galera_timeout, consume_statement(conn))
                                .await
                            {
                                Err(_) => Sent::WaitTimedOut,
                                Ok(r) => match r? {
                                    None => Sent::Ready,
                                    Some(_) => Sent::WaitFailed,
                                },
                            }
                        }
                    });
                }
                if !prefix.is_empty() {
                    match conn.client.query(&prefix.join("; ")).await {
                        Ok(_) => {}
                        Err(ClientError::Server(e)) => return Ok(Sent::SyncErr(e)),
                        Err(e) => return Err(e),
                    }
                }
                if guard {
                    match conn.client.query_drop(TRX_GUARD).await {
                        Ok(()) => {}
                        Err(ClientError::Server(e)) => return Ok(Sent::TrxGone(reported(e))),
                        Err(e) => return Err(e),
                    }
                }
                match &wait {
                    Wait::None => Ok(Sent::Ready),
                    Wait::Replica(t) => {
                        let row = conn
                            .client
                            .query_first(&reads::wait_query(t, settings.gtid_wait_timeout))
                            .await?;
                        let v = row
                            .and_then(|r| r.into_iter().next().flatten())
                            .map(|b| String::from_utf8_lossy(&b).into_owned());
                        Ok(if v.as_deref() == Some("0") {
                            Sent::Ready
                        } else {
                            Sent::WaitFailed
                        })
                    }
                    Wait::Galera => {
                        match tokio::time::timeout(
                            galera_timeout,
                            conn.client.query_drop(reads::GALERA_SYNC_CHECK),
                        )
                        .await
                        {
                            Err(_) => Ok(Sent::WaitTimedOut),
                            Ok(Ok(())) => Ok(Sent::Ready),
                            Ok(Err(ClientError::Server(_))) => Ok(Sent::WaitFailed),
                            Ok(Err(e)) => Err(e),
                        }
                    }
                }
            }
            .await;
            let waited = !matches!(wait, Wait::None);
            match sent {
                Ok(Sent::Ready) => {}
                Ok(Sent::TrxGone(reported)) => {
                    // The sync statements before the guard ran.
                    conn.applied = next_applied;
                    conn.client.take_changes();
                    if reported.is_none() {
                        note_silent_abort(&cluster, &node_key, "a write's transaction check");
                    }
                    self.pins.after_statement(StatusFlags::empty());
                    if route == RouteKind::Pinned {
                        self.held = Some((lease, route));
                    } else {
                        let limits = self.backends.limits(&cluster);
                        self.backends.pools.release(lease, Release::Reuse, limits);
                    }
                    self.inflight = None;
                    return Err(backend_err(&reported.unwrap_or_else(silent_abort_err)));
                }
                Ok(Sent::WaitFailed | Sent::WaitTimedOut) => {
                    // The read reruns on the writer (the replica, or the
                    // Galera node, was too slow).
                    metrics::counter!("sprig_backend_gtid_wait_timeouts_total", "cluster" => cluster.to_string())
                        .increment(1);
                    metrics::histogram!("sprig_backend_gtid_wait_seconds", "cluster" => cluster.to_string())
                        .record(wait_started.elapsed().as_secs_f64());
                    if matches!(sent, Ok(Sent::WaitTimedOut)) {
                        // Still waiting on the server: never reuse it.
                        tracing::debug!(node = %node_key, "Galera sync-wait check timed out; reading from the writer");
                        drop(lease);
                    } else {
                        // The sync statements before the wait ran.
                        conn.applied = next_applied;
                        conn.client.take_changes();
                        let limits = self.backends.limits(&cluster);
                        self.backends.pools.release(lease, Release::Reuse, limits);
                    }
                    force_writer = true;
                    continue;
                }
                Ok(Sent::SyncErr(e)) => {
                    metrics::counter!("sprig_backend_failures_total", "cluster" => cluster.to_string(), "kind" => "sync")
                        .increment(1);
                    drop(lease);
                    if route == RouteKind::Pinned {
                        let err = backend_err(&e);
                        self.pins.lose_connection();
                        return Err(SprigError::new(SprigCode::SessionStateLost, err.message));
                    }
                    if !sync_retried {
                        sync_retried = true;
                        continue;
                    }
                    return Err(backend_err(&e));
                }
                Err(e) => {
                    drop(lease);
                    if let Some(r) = self.retry_after_loss(
                        info,
                        harmless,
                        route,
                        &node_key,
                        &e,
                        &mut io_retried,
                        &mut exclude,
                        in_trx_before,
                    ) {
                        return Err(r);
                    }
                    continue;
                }
            }
            if waited {
                metrics::histogram!("sprig_backend_gtid_wait_seconds", "cluster" => cluster.to_string())
                    .record(wait_started.elapsed().as_secs_f64());
            }
            conn.applied = next_applied;
            conn.client.take_changes();

            // Prepared statements and other commands go out now.
            let outgoing = match &mut req {
                Req::Text(_) => Ok(()),
                Req::Binary { sql, build } => match conn.prepare_cached(sql).await {
                    Ok(st) => match build(st) {
                        Ok(payload) => {
                            st.types_sent = true;
                            conn.client.send_command_payload(payload).await
                        }
                        Err(e) => {
                            let limits = self.backends.limits(&cluster);
                            self.backends.pools.release(lease, Release::Reuse, limits);
                            return Err(SprigError::from(e));
                        }
                    },
                    Err(ClientError::Server(e)) => {
                        let limits = self.backends.limits(&cluster);
                        if route == RouteKind::Pinned {
                            self.held = Some((lease, route));
                        } else {
                            self.backends.pools.release(lease, Release::Reuse, limits);
                        }
                        return Err(backend_err(&e));
                    }
                    Err(e) => Err(e),
                },
                Req::Command(payload) => conn.client.send_command_payload(payload.clone()).await,
                Req::Prepare { sql, out } => match conn.prepare_cached(sql).await {
                    Ok(st) => {
                        **out = Some(st.clone());
                        Ok(())
                    }
                    Err(ClientError::Server(e)) => {
                        let limits = self.backends.limits(&cluster);
                        if route == RouteKind::Pinned {
                            self.held = Some((lease, route));
                        } else {
                            self.backends.pools.release(lease, Release::Reuse, limits);
                        }
                        return Err(backend_err(&e));
                    }
                    Err(e) => Err(e),
                },
            };
            let prepared_only = matches!(req, Req::Prepare { .. });
            let mut cur = Current {
                wrapped,
                lease,
                stmt: info.clone(),
                route,
                first: None,
                relayed: false,
                done: false,
                failed: false,
                in_trx_before,
                status: None,
                warnings: 0,
                last_ok: None,
                error: None,
                tracked: TrackedChanges::default(),
                rows: 0,
                resultset: false,
                probe: probe_before,
                sets_schema,
                explicit_ids,
                span: None,
                galera_dml_in_trx,
                wrote_before,
                opens_implicit,
                rollback_after: false,
                takes_locks,
            };
            let first = match outgoing {
                // A prepare has no response beyond what prepare_cached read.
                Ok(()) if prepared_only => Ok(None),
                Ok(()) => {
                    // Park the statement while it runs: if this future is
                    // dropped (KILL QUERY), `abandon` still has the
                    // connection to kill and drain, and an open transaction
                    // survives.
                    self.cur = Some(cur);
                    let r = match self.cur.as_mut() {
                        Some(c) => c.lease.conn().client.next_packet().await,
                        None => Err(ClientError::Closed),
                    };
                    let Some(c) = self.cur.take() else {
                        return Err(SprigError::internal("the statement in flight vanished"));
                    };
                    cur = c;
                    r
                }
                Err(e) => Err(e),
            };
            let fatal_first = matches!(
                &first,
                Ok(Some(RespPacket { kind: PacketKind::Err(e), .. })) if conn_fatal(e.code)
            );
            if fatal_first
                && harmless
                && route != RouteKind::Pinned
                && self.pins.is_empty()
                && !io_retried
            {
                // The server killed the connection before any result: a
                // plain read retries once elsewhere, as for an I/O error.
                drop(cur);
                io_retried = true;
                exclude.push(node_key.node.clone());
                continue;
            }
            match first {
                Ok(mut first) => {
                    let silent = match &first {
                        Some(p) if cur.ended_silently(p) => Some("a statement"),
                        Some(p) => match &commit_check {
                            Some(before)
                                if matches!(p.kind, PacketKind::Ok(_))
                                    && !p.more_results()
                                    && commit_wrote_nothing(&mut cur, p, before.as_deref())
                                        .await =>
                            {
                                Some("COMMIT")
                            }
                            _ => None,
                        },
                        None => None,
                    };
                    if let (Some(what), Some(p)) = (silent, &first) {
                        first = Some(cur.silent_abort(p, &cluster, what));
                    }
                    cur.done = first.is_none();
                    if let Some(p) = &first {
                        cur.observe(p);
                    }
                    cur.first = first;
                    self.cur = Some(cur);
                    self.inflight = None;
                    return Ok(());
                }
                Err(e) if e.is_connection_lost() => {
                    drop(cur);
                    if let Some(r) = self.retry_after_loss(
                        info,
                        harmless,
                        route,
                        &node_key,
                        &e,
                        &mut io_retried,
                        &mut exclude,
                        in_trx_before,
                    ) {
                        return Err(r);
                    }
                }
                Err(e) => return Err(internal(&e)),
            }
        }
    }

    /// A connection lost before the statement's first packet: a plain read
    /// or a prepare (`harmless`: running it twice changes nothing) of an
    /// unpinned session retries once elsewhere (`None`); anything else
    /// fails with the mapped error.
    #[allow(clippy::too_many_arguments)]
    fn retry_after_loss(
        &mut self,
        info: &StmtInfo,
        harmless: bool,
        route: RouteKind,
        node: &NodeKey,
        e: &ClientError,
        io_retried: &mut bool,
        exclude: &mut Vec<NodeId>,
        in_trx: bool,
    ) -> Option<SprigError> {
        tracing::debug!(node = %node, error = %e, "backend connection lost before the first packet");
        self.backends.note_failure(node, "io");
        let retry = harmless && route != RouteKind::Pinned && self.pins.is_empty() && !*io_retried;
        if retry {
            *io_retried = true;
            exclude.push(node.node.clone());
            return None;
        }
        if harmless && (in_trx || self.pins.holds_state()) {
            // A prepare never changes data: only what the connection held
            // is lost, never the outcome of a write.
            return Some(self.lost_unsent(node));
        }
        Some(self.lost(info, in_trx, false, node))
    }

    /// The executor dropped the future of a statement (`KILL QUERY`, or an
    /// abandoned request): `KILL QUERY` on its backend thread, then end it.
    /// A statement with a response under way is drained (up to `drain`), so
    /// the connection and an open transaction survive, as on MySQL. One
    /// dropped before its first packet took its connection with it: an
    /// open transaction or pinned state is then lost, and the session's
    /// next statement reports it (1213 or 39033) instead of running
    /// outside the transaction the client thinks it has.
    pub async fn abandon(&mut self, drain: Duration) {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(3), self.kill_query())
            .await
            .unwrap_or_else(|_| Err(unavailable("KILL QUERY timed out")))
        {
            tracing::debug!(error = %e, "KILL QUERY for an abandoned statement failed");
        }
        if self.cur.is_some() {
            match tokio::time::timeout(drain, self.finish()).await {
                Ok(Ok(_)) => return,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "abandoned statement ended with an error");
                    return;
                }
                // finish() had taken the statement: its connection is gone.
                Err(_) => tracing::warn!(
                    "abandoned backend statement didn't end; its connection is closed"
                ),
            }
        } else if self.inflight.take().is_none() {
            return;
        }
        // The connection is gone (dropped mid-send, or drained too slowly).
        self.held = None;
        self.pending_capture.clear();
        let in_trx = self.pins.in_transaction();
        let lost_state = self.pins.holds_state();
        self.pins.lose_connection();
        self.deferred = if lost_state {
            Some(SprigError::new(
                SprigCode::SessionStateLost,
                "the backend connection holding this session's state was lost when a statement was killed",
            ))
        } else if in_trx {
            Some(SprigError::standard(
                1213,
                *b"40001",
                "Deadlock found when trying to get lock; try restarting transaction \
                 (the transaction was rolled back when a statement was killed)",
            ))
        } else {
            None
        };
    }

    /// `COM_RESET_CONNECTION` or `COM_CHANGE_USER`: a held connection gets
    /// `COM_RESET_CONNECTION` and the baseline, then returns to the pool;
    /// the session's variables, user variables, pins and captures are
    /// cleared (the schema stays).
    ///
    /// # Errors
    /// Never fails the session: a connection that can't be reset is closed.
    pub async fn reset(&mut self) -> Result<()> {
        if self.cur.is_some() {
            let _ = self.finish().await;
        }
        self.release_kept();
        if let Some((mut lease, _)) = self.held.take() {
            let ok = lease.conn().reset().await.is_ok();
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(
                lease,
                if ok { Release::Reuse } else { Release::Discard },
                limits,
            );
        }
        let schema = self.vars.schema.clone();
        self.vars = SessionVars::login(self.opts.persona, self.opts.collation, schema);
        let session_mode = self.pins.reasons().contains(&PinReason::SessionMode);
        self.pins = Pins::default();
        self.pins.set_session_mode(session_mode);
        self.captured = Captured::default();
        self.pending_capture.clear();
        self.deferred = None;
        self.inflight = None;
        Ok(())
    }

    /// Points the session at another cluster: its tenant moved (a mover
    /// cutover). A held connection is discarded (the old cluster
    /// rolls back an open transaction, which the next statement reports as
    /// 1213, as for any lost connection); tracked session state is replayed
    /// on the next connection, which comes from the new cluster's pools.
    /// The causal position named the old cluster's GTIDs and is dropped.
    pub fn retarget(&mut self, cluster: sprig_common::ids::ClusterId) {
        if self.target.cluster == cluster {
            return;
        }
        drop(self.cur.take());
        self.release_kept();
        if let Some((lease, _)) = self.held.take() {
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(lease, Release::Discard, limits);
            self.connection_gone();
            // State that can't follow the tenant to another DC ends the
            // session with 39502 (SQLSTATE 08S01).
            let other_dc = sprig_cluster::registry::dc_of(&self.target.cluster)
                != sprig_cluster::registry::dc_of(&cluster);
            if other_dc
                && self
                    .deferred
                    .as_ref()
                    .is_some_and(|e| e.code() == Some(SprigCode::SessionStateLost))
            {
                self.deferred = Some(SprigError::new(
                    SprigCode::SessionMoved,
                    format!(
                        "the tenant moved to data center {}; this session held state that \
                         can't follow it (temporary tables, locks or XA): reconnect",
                        sprig_cluster::registry::dc_of(&cluster)
                            .map_or_else(|| "?".to_owned(), |d| d.to_string())
                    ),
                ));
            }
        }
        self.causal = GtidSet::default();
        tracing::info!(from = %self.target.cluster, to = %cluster, "session follows its tenant to another cluster");
        self.target.cluster = cluster;
    }

    /// Ends the session: a held connection is closed (the server rolls back
    /// an open transaction), anything in flight is abandoned.
    #[allow(clippy::unused_async)] // async in the API: closing may send COM_QUIT later
    pub async fn close(&mut self) {
        if let Some(cur) = self.cur.take() {
            drop(cur);
        }
        self.release_kept();
        if let Some((lease, _)) = self.held.take() {
            let limits = self.backends.limits(&self.target.cluster);
            self.backends.pools.release(lease, Release::Discard, limits);
        }
    }
}

/// Reads one statement's response of a multi-statement prefix; returns its
/// ERR, if any.
async fn consume_statement(
    conn: &mut conn::BackendConn,
) -> std::result::Result<Option<ErrPacket>, ClientError> {
    loop {
        match conn.client.next_packet().await? {
            Some(p) => match p.kind {
                PacketKind::Ok(_) | PacketKind::End(_) => return Ok(None),
                PacketKind::Err(e) => return Ok(Some(e)),
                _ => {}
            },
            None => {
                return Err(ClientError::Unexpected(
                    "the response ended inside the sync prefix".to_owned(),
                ));
            }
        }
    }
}

/// The metric handles every statement updates (`sprig_backend_reads_total`,
/// `sprig_backend_state_syncs_total`, `sprig_backend_pool_wait_seconds`),
/// kept per runtime thread and cluster: registering them with their labels
/// on every statement cost three label strings and three lookups in the
/// shared registry per point read. Same series, same values.
mod stmt_metrics {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use metrics::{Counter, Histogram};
    use sprig_common::ids::ClusterId;

    /// Clusters whose handles a thread keeps; past this many, each call
    /// registers as the macros do.
    const MAX_CLUSTERS: usize = 256;

    struct Handles {
        reads: Vec<(&'static str, &'static str, Counter)>,
        syncs: Vec<(&'static str, Counter)>,
        pool_wait: Option<Histogram>,
    }

    thread_local! {
        static HANDLES: RefCell<HashMap<String, Handles>> = RefCell::new(HashMap::new());
    }

    fn with<R>(cluster: &ClusterId, f: impl FnOnce(&mut Handles) -> R) -> R {
        HANDLES.with(|h| {
            let mut h = h.borrow_mut();
            if let Some(x) = h.get_mut(cluster.as_str()) {
                return f(x);
            }
            let mut fresh = Handles {
                reads: Vec::new(),
                syncs: Vec::new(),
                pool_wait: None,
            };
            if h.len() >= MAX_CLUSTERS {
                return f(&mut fresh);
            }
            f(h.entry(cluster.as_str().to_owned()).or_insert(fresh))
        })
    }

    pub(super) fn read(cluster: &ClusterId, target: &'static str, causal: &'static str) -> Counter {
        with(cluster, |h| {
            if let Some((_, _, c)) = h
                .reads
                .iter()
                .find(|(t, k, _)| *t == target && *k == causal)
            {
                return c.clone();
            }
            let c = metrics::counter!("sprig_backend_reads_total", "cluster" => cluster.to_string(), "target" => target, "causal" => causal);
            h.reads.push((target, causal, c.clone()));
            c
        })
    }

    pub(super) fn state_sync(cluster: &ClusterId, kind: &'static str) -> Counter {
        with(cluster, |h| {
            if let Some((_, c)) = h.syncs.iter().find(|(k, _)| *k == kind) {
                return c.clone();
            }
            let c = metrics::counter!("sprig_backend_state_syncs_total", "cluster" => cluster.to_string(), "kind" => kind);
            h.syncs.push((kind, c.clone()));
            c
        })
    }

    pub(super) fn pool_wait(cluster: &ClusterId) -> Histogram {
        with(cluster, |h| {
            h.pool_wait
                .get_or_insert_with(|| {
                    metrics::histogram!("sprig_backend_pool_wait_seconds", "cluster" => cluster.to_string())
                })
                .clone()
        })
    }
}

#[cfg(test)]
mod silent_abort_tests {
    use sprig_proto_mysql::Caps;
    use sprig_proto_mysql::client::PacketKind;

    use sprig_proto_mysql::StatusFlags;

    use super::{
        gtid_unchanged, plain_dml, plain_write, shows_silent_abort, silent_abort_packet, trx_wrote,
    };

    #[test]
    fn only_plain_dml_counts_as_unable_to_end_a_transaction() {
        for sql in [
            "SELECT v FROM t WHERE k = ?",
            "  insert into t values (1)",
            "UPDATE t SET v = 1",
            "delete from t",
            "REPLACE INTO t VALUES (1)",
            "WITH x AS (SELECT 1) SELECT * FROM x",
        ] {
            assert!(plain_dml(sql.as_bytes()), "{sql}");
        }
        for sql in [
            "COMMIT",
            "CALL p()",
            "SET autocommit = 1",
            "LOCK TABLES t WRITE",
            "SELECTED",
            "",
            // Executable comments hold SQL; an unterminated comment is
            // left alone.
            "/*!40101 COMMIT */ SELECT 1",
            "/*M!100100 COMMIT */ SELECT 1",
            "/* c SELECT 1",
            "-- c SELECT 1",
            "--c\nSELECT 1",
        ] {
            assert!(!plain_dml(sql.as_bytes()), "{sql}");
        }
        // Comments an ORM adds come before the statement.
        for sql in [
            "/* load Order */ SELECT v FROM t",
            "/* a */ /* b */\n  update t SET v = 1",
            "-- c\nINSERT INTO t VALUES (1)",
            "# c\r\nDELETE FROM t",
        ] {
            assert!(plain_dml(sql.as_bytes()), "{sql}");
        }
    }

    #[test]
    fn only_plain_writes_always_open_a_transaction() {
        for sql in [
            "INSERT INTO t VALUES (?)",
            "  update t SET v = 1 WHERE k = 0",
            "DELETE FROM t LIMIT 0",
            "replace into t values (1)",
            "/* c */ INSERT INTO t VALUES (1)",
        ] {
            assert!(plain_write(sql.as_bytes()), "{sql}");
        }
        // A SELECT may read no table (`SELECT 1`, `SELECT @x := 1`), and
        // then opens no transaction even with autocommit = 0.
        for sql in [
            "SELECT v FROM t",
            "SELECT 1 FOR UPDATE",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "/*!40101 INSERT INTO t VALUES (1) */",
            "INSERTS",
            "",
        ] {
            assert!(!plain_write(sql.as_bytes()), "{sql}");
        }
    }

    #[test]
    fn a_silent_abort_shows_in_the_status_or_the_transaction_state() {
        let open = StatusFlags::IN_TRANS | StatusFlags::AUTOCOMMIT;
        let none = StatusFlags::AUTOCOMMIT;
        // A statement that can't end the transaction, or one that opens it
        // with autocommit = 0, answered without one.
        assert!(shows_silent_abort(none, None, true, false));
        assert!(shows_silent_abort(
            StatusFlags::empty(),
            Some("________"),
            true,
            true
        ));
        // Any other statement may end it.
        assert!(!shows_silent_abort(none, None, false, false));
        // With autocommit = 0 the statement after the abort opened a new
        // transaction: open, but its state lost the earlier write.
        assert!(shows_silent_abort(open, Some("I_R___S_"), true, true));
        assert!(shows_silent_abort(open, Some("________"), true, true));
        // The transaction that wrote is still open: its state keeps the W,
        // or nothing new was reported.
        assert!(!shows_silent_abort(open, Some("I_R_W_S_"), true, true));
        assert!(!shows_silent_abort(open, None, true, true));
        // No write before: a new state without W says nothing.
        assert!(!shows_silent_abort(open, Some("I_R___S_"), true, false));
        // Only the statement's last result counts.
        assert!(!shows_silent_abort(
            none | StatusFlags::MORE_RESULTS_EXISTS,
            None,
            true,
            true
        ));
    }

    #[test]
    fn only_a_transactional_write_counts_as_writing() {
        assert!(trx_wrote("T___W___"));
        assert!(trx_wrote("T_R_W_S_"));
        // Reads, non-transactional writes, nothing.
        assert!(!trx_wrote("T_R___S_"));
        assert!(!trx_wrote("T__w____"));
        assert!(!trx_wrote("________"));
        assert!(!trx_wrote(""));
    }

    #[test]
    fn a_commit_that_wrote_moves_the_gtid() {
        // A new GTID: written.
        assert!(!gtid_unchanged(Some("3-301-5081"), "3-301-5090"));
        assert!(!gtid_unchanged(None, "3-301-5090"));
        // The GTID it had before, or none at all: nothing was written (the
        // 15 November CI loss and its local reproductions).
        assert!(gtid_unchanged(Some("3-301-5081"), "3-301-5081"));
        assert!(gtid_unchanged(None, ""));
        assert!(gtid_unchanged(Some("3-301-5081"), ""));
    }

    #[test]
    fn a_silent_abort_answers_mariadbs_deadlock_error() {
        let p = silent_abort_packet(Caps::PROTOCOL_41);
        let PacketKind::Err(e) = &p.kind else {
            panic!("an ERR packet: {p:?}");
        };
        assert_eq!((e.code, &e.sqlstate), (1213, b"40001"));
        assert_eq!(p.payload, e.encode(Caps::PROTOCOL_41));
    }
}

#[cfg(test)]
mod use_tests {
    use super::{dirty_after_use, is_use};

    #[test]
    fn use_statements_are_recognized() {
        for (sql, yes) in [
            (&b"USE shop"[..], true),
            (b"  use\tshop", true),
            (b"USE", false),
            (b"USER()", false),
            (b"SELECT 1", false),
            (b"used", false),
        ] {
            assert_eq!(is_use(sql), yes, "{}", String::from_utf8_lossy(sql));
        }
    }

    #[test]
    fn a_failed_use_on_another_schema_closes_the_connection() {
        // USE missing on a connection that had t1_a: the connection keeps
        // t1_a, the session has none: close it.
        assert!(dirty_after_use(true, Some("t1_a"), None));
        // USE t1_b succeeded: both agree.
        assert!(!dirty_after_use(true, Some("t1_b"), Some("t1_b")));
        // USE failed on a connection without a schema, for a session without one.
        assert!(!dirty_after_use(true, None, None));
        // Not a USE: the routing guard handles schema mismatches.
        assert!(!dirty_after_use(false, Some("t1_a"), None));
    }
}
