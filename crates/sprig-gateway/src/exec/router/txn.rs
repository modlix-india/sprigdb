//! The transaction coordinator's gateway side: a
//! [`TxnBackend`] whose branches are [`BackendSession`]s, and the admin
//! the coordinator finishes prepared branches with.
//!
//! - `caps(cluster)`: the cluster's capability profile (`derived.xa`),
//!   read when the session's layout names the cluster.
//! - `checkout(cluster)`: a new `BackendSession` for that cluster with the
//!   session's backend account (reconciled there first under per-user
//!   accounts) and the session's state, replayed on its connection; a
//!   branch pins its connection, so its statements go to the writer.
//! - `admin`: `sprig-txn::mysql::MyAdmin` on `sprig_ctl` connections, one
//!   small pool per cluster, per gateway.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use sprig_auth::BackendTarget;
use sprig_catalog::TenantCatalog;
use sprig_cluster::failover::relay::RelayLease;
use sprig_common::ids::ClusterId;
use sprig_txn::fence::wire::HolderCall;
use sprig_txn::mysql::{MyAdmin, MyCluster};
use sprig_txn::{
    ClusterAdmin, ClusterCaps, DbError, DbResult, Decision, DecisionRow, Dtid, TxnBackend, Xid,
};

use crate::backend::state::SessionVars;
use crate::backend::{BackendSession, Backends, SessionOpts};
use crate::tenant::TenantServices;
use crate::tenant::locator::GatewayLocator;

/// One session's branch factory.
pub(crate) struct GwTxn {
    pub(super) backends: Arc<Backends>,
    pub(super) target: BackendTarget,
    pub(super) opts: SessionOpts,
    pub(super) main: ClusterId,
    /// The session's state, copied before each statement.
    pub(super) vars: Mutex<SessionVars>,
    pub(super) caps: Mutex<HashMap<ClusterId, ClusterCaps>>,
    pub(super) admin: Arc<GwAdmin>,
    pub(super) svc: Arc<TenantServices>,
    pub(super) cat: Mutex<Option<Arc<TenantCatalog>>>,
    /// Sprig 2PC's relay path: the tenant named in relay preambles.
    pub(super) relay: Arc<Mutex<Option<sprig_common::ids::TenantId>>>,
    /// Clusters whose branch connections went through a ready relay
    /// writer, with its lease (where the holder calls go).
    pub(super) relayed: Arc<Mutex<HashMap<ClusterId, RelayLease>>>,
}

impl GwTxn {
    /// The tenant named in relay preambles now (Sprig 2PC's relay path):
    /// every session the router opens beside the main one takes it, so a
    /// relayed tenant's fan-out reads and scratch sessions go through the
    /// relay writer too.
    pub(super) fn relay_now(&self) -> Option<sprig_common::ids::TenantId> {
        *self.relay.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl std::fmt::Debug for GwTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwTxn")
            .field("main", &self.main)
            .finish_non_exhaustive()
    }
}

/// A new backend session on `cluster` with `vars` (the session's state;
/// the default schema only on the session's own cluster, where it lives).
pub(super) fn session_on(
    backends: &Arc<Backends>,
    target: &BackendTarget,
    opts: &SessionOpts,
    main: &ClusterId,
    cluster: &ClusterId,
    vars: &SessionVars,
) -> BackendSession {
    let mut t = target.clone();
    t.cluster = cluster.clone();
    let mut o = opts.clone();
    if cluster != main {
        o.schema = None;
    }
    let mut bs = backends.session(t, o);
    let mut v = vars.clone();
    if cluster != main {
        v.schema = None;
    }
    *bs.vars_mut() = v;
    bs
}

/// Per-user backend accounts on `cluster`, before a session
/// first uses it.
pub(super) async fn ensure_accounts(
    svc: &TenantServices,
    cat: Option<&Arc<TenantCatalog>>,
    cluster: &ClusterId,
) -> sprig_common::Result<()> {
    match cat {
        Some(c) if svc.authz.per_user() => svc.authz.ensure(svc, c, cluster).await,
        _ => Ok(()),
    }
}

#[async_trait::async_trait]
impl TxnBackend for GwTxn {
    type Conn = BackendSession;

    fn caps(&self, cluster: &ClusterId) -> ClusterCaps {
        self.caps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .copied()
            .unwrap_or_default()
    }

    async fn checkout(&self, cluster: &ClusterId) -> DbResult<BackendSession> {
        if cluster != &self.main {
            let cat = self
                .cat
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            ensure_accounts(&self.svc, cat.as_ref(), cluster)
                .await
                .map_err(|e| DbError::new(0, e.to_string()))?;
        }
        let vars = self
            .vars
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut bs = session_on(
            &self.backends,
            &self.target,
            &self.opts,
            &self.main,
            cluster,
            &vars,
        );
        bs.set_txn_branch();
        let relay = *self.relay.lock().unwrap_or_else(PoisonError::into_inner);
        bs.set_relay(relay);
        if relay.is_none() {
            self.relayed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(cluster);
        } else {
            // The relay path holds branches only where a ready relay
            // writer fences them; elsewhere a second writer without XA is
            // refused (39440), as under `reject`.
            let ready = bs
                .relay_ready(cluster)
                .await
                .map_err(|e| DbError::new(0, e.to_string()))?;
            let mut m = self.relayed.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(l) = ready {
                m.insert(cluster.clone(), l);
            } else {
                m.remove(cluster);
                bs.forbid_redo();
            }
        }
        Ok(bs)
    }

    // The coordinator keeps the admin it gets at first; the relay path
    // may be switched on later, so the wrapper decides per call.
    fn admin(&self) -> Arc<dyn ClusterAdmin> {
        Arc::new(RelayAdmin {
            inner: Arc::clone(&self.admin),
            relayed: Arc::clone(&self.relayed),
            relay: Arc::clone(&self.relay),
        })
    }

    fn relay_fenced(&self, cluster: &ClusterId) -> bool {
        self.relay
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
            && self
                .relayed
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(cluster)
    }
}

/// The admin of a session on Sprig 2PC's relay path: on clusters whose
/// branches went through a relay writer, a held branch's prepare, finish
/// and abandon go to that writer's agent, which registers the holder
/// before its redo rows exist; everything else is [`GwAdmin`]'s.
struct RelayAdmin {
    inner: Arc<GwAdmin>,
    relayed: Arc<Mutex<HashMap<ClusterId, RelayLease>>>,
    relay: Arc<Mutex<Option<sprig_common::ids::TenantId>>>,
}

impl RelayAdmin {
    fn tenant(&self) -> sprig_common::ids::TenantId {
        self.relay
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unwrap_or(sprig_common::ids::TenantId(0))
    }

    fn agent(&self, c: &ClusterId) -> Option<String> {
        self.relay
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()?;
        self.relayed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(c)
            .map(|l| l.internal_url.clone())
    }
}

#[async_trait::async_trait]
impl ClusterAdmin for RelayAdmin {
    async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
        self.inner.xa_recover(c).await
    }
    async fn ensure_redo(&self, c: &ClusterId, schema: &str) -> DbResult<()> {
        self.inner.ensure_redo(c, schema).await
    }
    async fn insert_redo(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &sprig_txn::Gtrid,
        stmts: &[sprig_txn::RedoStmt],
    ) -> DbResult<()> {
        self.inner.insert_redo(c, schema, dtid, gtrid, stmts).await
    }
    async fn insert_redo_held(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &sprig_txn::Gtrid,
        stmts: &[sprig_txn::RedoStmt],
        holder: u64,
    ) -> DbResult<()> {
        match self.agent(c) {
            Some(url) => {
                let call = HolderCall::Prepare {
                    tenant: gtrid.tenant,
                    thread: holder,
                    schema: schema.to_owned(),
                    dtid,
                    gtrid: *gtrid,
                    stmts: stmts.to_vec(),
                };
                crate::backend::relay_calls::call(&url, &call).await
            }
            None => self.inner.insert_redo(c, schema, dtid, gtrid, stmts).await,
        }
    }
    async fn abandon_held(
        &self,
        c: &ClusterId,
        _schema: &str,
        dtid: Dtid,
        holder: u64,
    ) -> DbResult<()> {
        match self.agent(c) {
            Some(url) => {
                let call = HolderCall::Abandon {
                    tenant: self.tenant(),
                    thread: holder,
                    dtid,
                };
                crate::backend::relay_calls::call(&url, &call).await
            }
            None => Ok(()),
        }
    }
    async fn redo_branches(&self, c: &ClusterId) -> DbResult<Vec<sprig_txn::RedoBranch>> {
        self.inner.redo_branches(c).await
    }
    async fn replay_redo(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<sprig_txn::Replay> {
        self.inner.replay_redo(c, schema, dtid).await
    }
    async fn delete_redo(&self, c: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        match self.agent(c) {
            Some(url) => {
                let call = HolderCall::Finished {
                    schema: schema.to_owned(),
                    dtid,
                };
                crate::backend::relay_calls::call(&url, &call).await
            }
            None => self.inner.delete_redo(c, schema, dtid).await,
        }
    }
    async fn xa_finish(&self, c: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        self.inner.xa_finish(c, xid, commit).await
    }
    async fn kill(&self, c: &ClusterId, thread: u64) -> DbResult<()> {
        self.inner.kill(c, thread).await
    }
    async fn insert_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()> {
        self.inner.insert_decision(c, schema, row).await
    }
    async fn read_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        self.inner.read_decision(c, schema, dtid).await
    }
    async fn settle_pending(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        o: Decision,
    ) -> DbResult<()> {
        self.inner.settle_pending(c, schema, dtid, o).await
    }
    async fn decision_rows(&self, c: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        self.inner.decision_rows(c).await
    }
    async fn delete_decision(&self, c: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        self.inner.delete_decision(c, schema, dtid).await
    }
}

/// Crash tests of a deployment: `SPRIGDB_TEST_TXN_PAUSE=<step>:<n>:<dir>`
/// stops one distributed transaction in `n` at the coordinator's `<step>`
/// (`before_prepare`, `prepared`, `commit_point`, `phase2_branch`): it
/// writes `<dir>/pause-<pid>-<dtid>` naming the step and waits until the
/// file is gone (at most 30 s), so the test can `kill -9` a process at that
/// moment. Unset in production.
pub(crate) fn pause_hooks() -> Option<Arc<dyn sprig_txn::TxnHooks>> {
    static HOOKS: std::sync::OnceLock<Option<Arc<dyn sprig_txn::TxnHooks>>> =
        std::sync::OnceLock::new();
    HOOKS
        .get_or_init(|| {
            let v = std::env::var("SPRIGDB_TEST_TXN_PAUSE").ok()?;
            let mut it = v.splitn(3, ':');
            let step = it.next()?.to_owned();
            let every: u64 = it.next()?.parse().ok()?;
            let dir = std::path::PathBuf::from(it.next()?);
            tracing::warn!(%step, every, dir = %dir.display(), "SPRIGDB_TEST_TXN_PAUSE is set: crash-test pauses on");
            Some(Arc::new(PauseHooks {
                step,
                every: every.max(1),
                dir,
                seen: std::sync::atomic::AtomicU64::new(0),
            }) as Arc<dyn sprig_txn::TxnHooks>)
        })
        .clone()
}

struct PauseHooks {
    step: String,
    every: u64,
    dir: std::path::PathBuf,
    seen: std::sync::atomic::AtomicU64,
}

fn step_name(p: sprig_txn::Point) -> &'static str {
    use sprig_txn::Point;
    match p {
        Point::BranchOpened => "branch_opened",
        Point::BeforePrepare => "before_prepare",
        Point::Prepared => "prepared",
        Point::PendingRecorded => "pending_recorded",
        Point::BeforeCommitPoint => "before_commit_point",
        Point::CommitPoint => "commit_point",
        Point::Phase2Branch => "phase2_branch",
        Point::Finished => "finished",
    }
}

#[async_trait::async_trait]
impl sprig_txn::TxnHooks for PauseHooks {
    async fn at(&self, dtid: Option<Dtid>, p: sprig_txn::Point) {
        if step_name(p) != self.step {
            return;
        }
        let n = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !(n + 1).is_multiple_of(self.every) {
            return;
        }
        let name = format!(
            "pause-{}-{}",
            std::process::id(),
            dtid.map_or_else(|| format!("n{n}"), |d| d.to_string())
        );
        let path = self.dir.join(name);
        if std::fs::write(&path, self.step.as_bytes()).is_err() {
            return;
        }
        tracing::warn!(step = %self.step, file = %path.display(), "crash-test pause");
        let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while path.exists() && std::time::Instant::now() < until {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let _ = std::fs::remove_file(&path);
    }
}

/// The coordinator's admin on `sprig_ctl`, per gateway: a `MyAdmin` per
/// cluster, made at first use.
pub(crate) struct GwAdmin {
    locator: Arc<GatewayLocator>,
    admins: tokio::sync::Mutex<HashMap<ClusterId, Arc<MyAdmin>>>,
}

impl std::fmt::Debug for GwAdmin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwAdmin").finish_non_exhaustive()
    }
}

impl GwAdmin {
    /// An admin finding clusters through `locator`.
    #[must_use]
    pub(crate) fn new(locator: Arc<GatewayLocator>) -> Self {
        Self {
            locator,
            admins: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn of(&self, cluster: &ClusterId) -> DbResult<Arc<MyAdmin>> {
        let mut m = self.admins.lock().await;
        if let Some(a) = m.get(cluster) {
            return Ok(Arc::clone(a));
        }
        let opts = self
            .locator
            .ctl_opts(cluster)
            .await
            .map_err(|e| DbError::lost(e.to_string()))?;
        let mut clusters = std::collections::BTreeMap::new();
        clusters.insert(
            cluster.clone(),
            MyCluster {
                opts: opts.clone(),
                admin_opts: opts,
                caps: ClusterCaps::default(),
            },
        );
        let a = Arc::new(MyAdmin::new(&clusters));
        m.insert(cluster.clone(), Arc::clone(&a));
        Ok(a)
    }
}

#[async_trait::async_trait]
impl ClusterAdmin for GwAdmin {
    async fn xa_recover(&self, c: &ClusterId) -> DbResult<Vec<Xid>> {
        self.of(c).await?.xa_recover(c).await
    }
    // Sprig 2PC: held branches' redo rows, through `sprig_ctl`.
    async fn ensure_redo(&self, c: &ClusterId, schema: &str) -> DbResult<()> {
        self.of(c).await?.ensure_redo(c, schema).await
    }
    async fn insert_redo(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        gtrid: &sprig_txn::Gtrid,
        stmts: &[sprig_txn::RedoStmt],
    ) -> DbResult<()> {
        self.of(c)
            .await?
            .insert_redo(c, schema, dtid, gtrid, stmts)
            .await
    }
    async fn redo_branches(&self, c: &ClusterId) -> DbResult<Vec<sprig_txn::RedoBranch>> {
        self.of(c).await?.redo_branches(c).await
    }
    async fn replay_redo(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<sprig_txn::Replay> {
        self.of(c).await?.replay_redo(c, schema, dtid).await
    }
    async fn delete_redo(&self, c: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        self.of(c).await?.delete_redo(c, schema, dtid).await
    }
    async fn xa_finish(&self, c: &ClusterId, xid: &Xid, commit: bool) -> DbResult<()> {
        self.of(c).await?.xa_finish(c, xid, commit).await
    }
    async fn kill(&self, c: &ClusterId, thread: u64) -> DbResult<()> {
        self.of(c).await?.kill(c, thread).await
    }
    async fn insert_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        row: &DecisionRow,
    ) -> DbResult<()> {
        self.of(c).await?.insert_decision(c, schema, row).await
    }
    async fn read_decision(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
    ) -> DbResult<Option<DecisionRow>> {
        self.of(c).await?.read_decision(c, schema, dtid).await
    }
    async fn settle_pending(
        &self,
        c: &ClusterId,
        schema: &str,
        dtid: Dtid,
        o: Decision,
    ) -> DbResult<()> {
        self.of(c).await?.settle_pending(c, schema, dtid, o).await
    }
    async fn decision_rows(&self, c: &ClusterId) -> DbResult<Vec<(String, DecisionRow)>> {
        self.of(c).await?.decision_rows(c).await
    }
    async fn delete_decision(&self, c: &ClusterId, schema: &str, dtid: Dtid) -> DbResult<()> {
        self.of(c).await?.delete_decision(c, schema, dtid).await
    }
}

/// Lookup-index statements on the gateway's own connections
/// (`sprig_txn::lookup::DistTxn`'s Pre, Main and Post): parameters are
/// written into the text as literals (strings as `_utf8mb4 X'…'`, so the
/// column's collation compares them), rows come back as `mysql_async`
/// rows of text values.
#[async_trait::async_trait]
impl sprig_txn::lookup::DistConn for BackendSession {
    async fn run(&mut self, sql: &str) -> DbResult<()> {
        let mut budget = sprig_router::Budget::mb(16);
        super::backend::run_on(self, sql, &mut budget)
            .await
            .map(|_| ())
            .map_err(|e| as_db(&e))
    }

    async fn query(
        &mut self,
        sql: &str,
        params: mysql_async::Params,
    ) -> DbResult<Vec<mysql_async::Row>> {
        let text = inline_params(sql, params).map_err(|m| DbError::new(1210, m))?;
        let mut budget = sprig_router::Budget::mb(64);
        match super::backend::run_on(self, &text, &mut budget).await {
            Ok(sprig_router::ShardOutcome::Rows(r)) => Ok(to_rows(&r)),
            Ok(sprig_router::ShardOutcome::Ok(_)) => Ok(Vec::new()),
            Err(e) => Err(as_db(&e)),
        }
    }
}

fn as_db(e: &sprig_common::SprigError) -> DbError {
    super::backend::db_error(e).unwrap_or_else(|| DbError::new(0, e.to_string()))
}

fn to_rows(r: &sprig_router::ShardRows) -> Vec<mysql_async::Row> {
    use mysql_common::constants::ColumnType;
    use mysql_common::packets::Column;
    let cols: Arc<[Column]> = r
        .columns
        .iter()
        .map(|c| Column::new(ColumnType::MYSQL_TYPE_VAR_STRING).with_name(&c.name))
        .collect();
    r.rows
        .iter()
        .map(|row| {
            let vals = row
                .iter()
                .map(|v| {
                    v.as_ref().map_or(mysql_async::Value::NULL, |b| {
                        mysql_async::Value::Bytes(b.to_vec())
                    })
                })
                .collect();
            mysql_common::row::new_row(vals, Arc::clone(&cols))
        })
        .collect()
}

/// `sql` with each `?` outside quotes replaced by its positional value as
/// a literal.
fn inline_params(sql: &str, params: mysql_async::Params) -> Result<String, String> {
    let vals = match params {
        mysql_async::Params::Empty => Vec::new(),
        mysql_async::Params::Positional(v) => v,
        mysql_async::Params::Named(_) => return Err("named parameters".to_owned()),
    };
    let mut out = String::with_capacity(sql.len() + 32 * vals.len());
    let mut it = vals.iter();
    let mut quote: Option<char> = None;
    for c in sql.chars() {
        match quote {
            Some(q) => {
                out.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if matches!(c, '`' | '\'' | '"') => {
                quote = Some(c);
                out.push(c);
            }
            None if c == '?' => {
                let v = it.next().ok_or("too few parameters")?;
                out.push_str(&literal(v));
            }
            None => out.push(c),
        }
    }
    if it.next().is_some() {
        return Err("too many parameters".to_owned());
    }
    Ok(out)
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(2 * b.len()), |mut s, x| {
            let _ = write!(s, "{x:02X}");
            s
        })
}

fn literal(v: &mysql_async::Value) -> String {
    use mysql_async::Value as V;
    match v {
        V::NULL => "NULL".to_owned(),
        V::Bytes(b) if std::str::from_utf8(b).is_ok() => format!("_utf8mb4 X'{}'", hex(b)),
        V::Bytes(b) => format!("_binary X'{}'", hex(b)),
        V::Int(i) => i.to_string(),
        V::UInt(u) => u.to_string(),
        V::Float(f) => format!("{f:e}"),
        V::Double(d) => format!("{d:e}"),
        V::Date(year, month, day, hour, minute, second, micros) => {
            format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'")
        }
        V::Time(neg, days, hour, minute, second, micros) => {
            let hours = u64::from(*days) * 24 + u64::from(*hour);
            let sign = if *neg { "-" } else { "" };
            format!("'{sign}{hours}:{minute:02}:{second:02}.{micros:06}'")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_become_literals() {
        let p = mysql_async::Params::Positional(vec![
            mysql_async::Value::Bytes(b"a@x".to_vec()),
            mysql_async::Value::Int(-4),
            mysql_async::Value::NULL,
            mysql_async::Value::Bytes(vec![0xff]),
        ]);
        assert_eq!(
            inline_params(
                "SELECT `q?` FROM t WHERE a = ? AND b = ? AND c <=> ? AND d = ?",
                p
            )
            .as_deref(),
            Ok(
                "SELECT `q?` FROM t WHERE a = _utf8mb4 X'614078' AND b = -4 AND c <=> NULL \
                 AND d = _binary X'FF'"
            )
        );
        assert!(inline_params("? ?", mysql_async::Params::Positional(vec![1.into()])).is_err());
    }
}
