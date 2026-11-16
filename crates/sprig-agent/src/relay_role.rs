//! The agent on a Galera node with `[agent] relay_addr`: Sprig 2PC's relay
//! and its fence, put in a deployment.
//!
//! ```text
//! start:  bind relay_addr (connections are refused with 39448 until ready)
//!         set relays/<cluster> if missing (from now on gateways reach the
//!           cluster's relayed tenants only through the relay writer)
//!         register agents/<id> under a lease
//! every 500 ms:
//!   usable = the node is Primary, Synced and wsrep_ready
//!   holding leases/relay/<cluster>:
//!     its etcd session may have lapsed, or the node is unusable for 2 s
//!       -> stand down (refuse everyone; end every relayed connection,
//!          holders last), then hand the lease back
//!   not holding, usable, the lease is missing:
//!     create it (ready = false, epoch + 1) under a session of its own
//!     recover every held branch on the node (any age: the previous
//!       holder's branches are redo rows replicated here)
//!     set ready = true: gateways connect from now on
//! holder calls (prepare, finished, abandon) on the internal service:
//!   POST /v1/relay/... answered by the fence
//! the relay host (what the node sees this agent's connections come
//!   from) goes into relays/<cluster>'s hosts before the agent competes
//!   for the lease: relayed tenants' accounts log in only from there
//! every 2 s: end the node's connections of relayed tenants' accounts
//!   (their '%' account locked) that came from no relay host (logins from
//!   before the tenant took the relay path, or an old gateway's)
//! ```
//!
//! A relay writer that loses its etcd session stands down before its lease
//! can expire ([`STAND_DOWN_MARGIN`] before the session's deadline), so two
//! nodes never admit the same tenant at once.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_cluster::failover::relay::{
    RelayCluster, RelayLease, relay_cluster_key, relay_lease_key,
};
use sprig_cluster::failover::writer::{AgentRecord, MysqldState, WriterLease, writer_lease_key};
use sprig_cluster::record::{ClusterRecord, NodeSpec};
use sprig_cluster::sql::ProbeSession;
use sprig_common::ids::ClusterId;
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Cmp, Consistency, MetaStore, Op, Record, Session, Txn};
use sprig_txn::fence::RelayFence;
use sprig_txn::fence::wire::{
    ABANDON_ROUTE, FINISHED_ROUTE, HolderCall, PREPARE_ROUTE, encode_answer,
};
use sprig_txn::mysql::{MyAdmin, PoolSource};
use sprig_txn::{DbError, DbResult, MapLocator, ResolverConfig, ShardMapStore, TenantMaps};

use crate::fence::Target;
use crate::relay::{Relay, RelayRegistry};

/// The loop's period.
pub const PERIOD: Duration = Duration::from_millis(500);
/// The relay lease's TTL.
pub const LEASE_TTL: Duration = Duration::from_secs(10);
/// The relay writer stands down this long before its session's deadline.
pub const STAND_DOWN_MARGIN: Duration = Duration::from_secs(3);
/// An unusable node keeps the lease this long (a short desync).
pub const UNUSABLE_GRACE: Duration = Duration::from_secs(2);
/// How often the node's connections are checked for logins around the
/// relays.
pub const DIRECT_LOGIN_SWEEP: Duration = Duration::from_secs(2);
/// How often the relay host is read again (an address can change when a
/// container restarts).
pub const HOST_RECHECK: Duration = Duration::from_secs(30);

/// MariaDB's account host for a `PROCESSLIST.HOST` value: the address
/// without its port (`10.0.0.5:51234` → `10.0.0.5`, `[::1]:5` → `::1`,
/// `localhost` as it is).
#[must_use]
pub fn account_host(processlist_host: &str) -> String {
    let h = processlist_host.trim();
    if let Some(rest) = h.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest).to_owned();
    }
    match h.rsplit_once(':') {
        Some((addr, port))
            if !addr.is_empty() && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            addr.to_owned()
        }
        _ => h.to_owned(),
    }
}

/// Of the node's connections, the ids to end: a tenant user account
/// (`t<tenant>_u<user>`) whose `'%'` account is locked (the tenant is on
/// the relay path) logged in from a host it has no account at (so it
/// matched the `'%'` account before the lock: not through a relay).
#[must_use]
pub fn direct_logins(
    accounts: &[(String, String, bool)],
    processes: &[(u64, String, String)],
) -> Vec<u64> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut locked: BTreeSet<&str> = BTreeSet::new();
    let mut hosts: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for (user, host, is_locked) in accounts {
        if !matches!(
            user.parse::<sprig_common::names::AccountName>(),
            Ok(sprig_common::names::AccountName::User { .. })
        ) {
            continue;
        }
        if host == "%" {
            if *is_locked {
                locked.insert(user);
            }
        } else {
            hosts
                .entry(user)
                .or_default()
                .insert(host.to_ascii_lowercase());
        }
    }
    processes
        .iter()
        .filter(|(_, user, host)| {
            locked.contains(user.as_str())
                && !hosts
                    .get(user.as_str())
                    .is_some_and(|h| h.contains(&account_host(host).to_ascii_lowercase()))
        })
        .map(|(id, _, _)| *id)
        .collect()
}

fn pool_of(opts: mysql_async::Opts, max: usize) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, max.max(1)).unwrap_or_default())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

/// `sprig_ctl` pools for the fence's resolver: this node for its own
/// cluster; for any other cluster (the home of a transaction, where its
/// decision is raced), the holder of its writer lease, else its first
/// node.
struct RegistryCtl {
    store: Arc<dyn MetaStore>,
    own: ClusterId,
    own_pool: Pool,
    pools: tokio::sync::Mutex<BTreeMap<ClusterId, Pool>>,
}

impl RegistryCtl {
    async fn open(&self, cluster: &ClusterId) -> Result<Pool> {
        let key = self.store.tier().key(&format!("clusters/{cluster}"))?;
        let kv = self
            .store
            .get(&key, Consistency::Linearizable)
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::ClusterNotFound,
                    format!("no cluster {cluster} in this data center"),
                )
            })?;
        let rec = Record::<ClusterRecord>::decode(&kv.value)?.body;
        let lease = match self
            .store
            .get(
                &writer_lease_key(self.store.tier(), cluster)?,
                Consistency::Serializable,
            )
            .await?
        {
            Some(kv) => Some(Record::<WriterLease>::decode(&kv.value)?.body.node_id),
            None => None,
        };
        let node: &NodeSpec = lease
            .and_then(|n| rec.nodes.iter().find(|s| s.node.as_str() == n))
            .or_else(|| rec.nodes.first())
            .ok_or_else(|| SprigError::internal(format!("cluster {cluster} has no nodes")))?;
        let cred = sprig_provision::credentials::resolve(&rec.secrets.ctl, "sprig_ctl")?;
        Ok(pool_of(cred.endpoint(node)?.opts, 2))
    }
}

#[async_trait::async_trait]
impl PoolSource for RegistryCtl {
    async fn pool(&self, cluster: &ClusterId) -> DbResult<Pool> {
        if *cluster == self.own {
            return Ok(self.own_pool.clone());
        }
        let mut m = self.pools.lock().await;
        if let Some(p) = m.get(cluster) {
            return Ok(p.clone());
        }
        let p = self
            .open(cluster)
            .await
            .map_err(|e| DbError::lost(e.to_string()))?;
        m.insert(cluster.clone(), p.clone());
        Ok(p)
    }
}

/// The relay role's shared parts.
pub struct RelayRole {
    agent_id: String,
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    target: Target,
    relay: Arc<Relay>,
    relay_addr: String,
    internal_url: String,
    /// `sprig_ctl` on this node (the relay host, the direct-login sweep).
    own_pool: Pool,
    /// Whether this agent is the ready relay writer, for logs and tests.
    status: Mutex<&'static str>,
}

impl std::fmt::Debug for RelayRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayRole")
            .field("agent", &self.agent_id)
            .field("cluster", &self.target.cluster)
            .finish_non_exhaustive()
    }
}

impl RelayRole {
    /// The role for `target`'s node: its relay forwards to the node's own
    /// address, the fence acts as `sprig_ctl` on it.
    #[must_use]
    pub fn new(
        agent_id: String,
        store: Arc<dyn MetaStore>,
        target: Target,
        relay_addr: String,
        internal_url: String,
    ) -> Arc<Self> {
        let own_pool = pool_of(target.endpoint.opts.clone(), 4);
        let source = Arc::new(RegistryCtl {
            store: Arc::clone(&store),
            own: target.cluster.clone(),
            own_pool: own_pool.clone(),
            pools: tokio::sync::Mutex::new(BTreeMap::new()),
        });
        let admin = Arc::new(MyAdmin::over(source));
        let locator = MapLocator::new(
            ShardMapStore::new(Arc::clone(&store)),
            TenantMaps::new(Arc::clone(&store)),
            Arc::clone(&admin) as Arc<dyn sprig_txn::GroupSchemas>,
        );
        let registry = Arc::new(RelayRegistry::new(own_pool.clone()));
        let fence = Arc::new(RelayFence::new(
            target.cluster.clone(),
            Arc::clone(&registry),
            admin,
            Arc::new(locator),
            ResolverConfig::default(),
        ));
        let relay = Arc::new(Relay::new(fence, registry, target.addr.clone()));
        Arc::new(Self {
            agent_id,
            store,
            clock: Arc::new(SystemClock),
            target,
            relay,
            relay_addr,
            internal_url,
            own_pool,
            status: Mutex::new("starting"),
        })
    }

    /// The holder-call routes for the internal service.
    pub fn routes(self: &Arc<Self>) -> Router {
        Router::new()
            .route(PREPARE_ROUTE, post(holder_call))
            .route(FINISHED_ROUTE, post(holder_call))
            .route(ABANDON_ROUTE, post(holder_call))
            .with_state(Arc::clone(self))
    }

    fn set_status(&self, s: &'static str) {
        let mut cur = self.status.lock().unwrap_or_else(PoisonError::into_inner);
        if *cur != s {
            tracing::info!(cluster = %self.target.cluster, node = %self.target.node, from = *cur, to = s, "relay writer state");
            *cur = s;
        }
        metrics::gauge!("sprig_agent_relay_writer", "cluster" => self.target.cluster.to_string())
            .set(if s == "ready" { 1.0 } else { 0.0 });
    }

    async fn usable(conn: &mut Option<ProbeSession>, target: &Target) -> bool {
        if conn.is_none() {
            *conn = ProbeSession::connect(&target.endpoint, Duration::from_secs(2))
                .await
                .ok();
        }
        let Some(c) = conn.as_mut() else {
            return false;
        };
        let Ok(rows) = c
            .rows(
                "SHOW GLOBAL STATUS WHERE Variable_name IN \
                 ('wsrep_cluster_status', 'wsrep_local_state_comment', 'wsrep_ready')",
            )
            .await
        else {
            *conn = None;
            return false;
        };
        let get = |n: &str| {
            rows.iter().find_map(|r| {
                let k = r.first()?.1.as_deref()?;
                k.eq_ignore_ascii_case(n)
                    .then(|| r.get(1).and_then(|v| v.1.clone()).unwrap_or_default())
            })
        };
        get("wsrep_cluster_status").as_deref() == Some("Primary")
            && get("wsrep_local_state_comment").as_deref() == Some("Synced")
            && get("wsrep_ready").is_some_and(|v| v.eq_ignore_ascii_case("ON") || v == "1")
    }

    async fn put(&self, s: &Session, value: Vec<u8>, when: Vec<Cmp>) -> bool {
        let Ok(key) = relay_lease_key(self.store.tier(), &self.target.cluster) else {
            return false;
        };
        let r = self
            .store
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key,
                    value,
                    lease: Some(s.lease()),
                }],
                otherwise: Vec::new(),
            })
            .await;
        r.is_ok_and(|t| t.succeeded)
    }

    fn lease_value(&self, epoch: u64, ready: bool) -> Option<Vec<u8>> {
        Record::new(RelayLease {
            node_id: self.target.node.to_string(),
            agent: self.agent_id.clone(),
            relay_addr: self.relay_addr.clone(),
            internal_url: self.internal_url.clone(),
            epoch,
            ready,
        })
        .encode()
        .ok()
    }

    async fn mark_cluster(&self) -> Result<()> {
        let key = relay_cluster_key(self.store.tier(), &self.target.cluster)?;
        let since_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(0);
        let value = Record::new(RelayCluster {
            first_agent: self.agent_id.clone(),
            since_ms,
            hosts: BTreeMap::new(),
        })
        .encode()?;
        self.store
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![Op::Put {
                    key,
                    value,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        Ok(())
    }

    /// The host this node sees this agent's connections come from.
    async fn own_host(&self) -> Option<String> {
        use mysql_async::prelude::Queryable;
        let mut c = self.own_pool.get_conn().await.ok()?;
        let h: Option<String> = c
            .query_first(
                "SELECT HOST FROM information_schema.PROCESSLIST WHERE ID = CONNECTION_ID()",
            )
            .await
            .ok()?;
        h.map(|h| account_host(&h)).filter(|h| !h.is_empty())
    }

    /// Adds `host` as this agent's relay host to `relays/<cluster>`
    /// (compare-and-set on the marker); true once the marker has it.
    async fn publish_host(&self, host: &str) -> Result<bool> {
        let key = relay_cluster_key(self.store.tier(), &self.target.cluster)?;
        for _ in 0..3 {
            let Some(kv) = self.store.get(&key, Consistency::Linearizable).await? else {
                return Ok(false);
            };
            let mut m = Record::<RelayCluster>::decode(&kv.value)?.body;
            if m.hosts.get(&self.agent_id).map(String::as_str) == Some(host) {
                return Ok(true);
            }
            m.hosts.insert(self.agent_id.clone(), host.to_owned());
            let value = Record::new(m).encode()?;
            let t = self
                .store
                .txn(Txn {
                    when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                    then: vec![Op::Put {
                        key: key.clone(),
                        value,
                        lease: None,
                    }],
                    otherwise: Vec::new(),
                })
                .await?;
            if t.succeeded {
                tracing::info!(cluster = %self.target.cluster, node = %self.target.node, host,
                    "relay: relayed tenants' accounts log in from this host");
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Ends the node's connections that logged in around the relays
    /// ([`direct_logins`]); how many.
    async fn end_direct_logins(&self) -> usize {
        use mysql_async::prelude::Queryable;
        let Ok(mut c) = self.own_pool.get_conn().await else {
            return 0;
        };
        let accounts: Vec<(String, String, bool)> = match c
            .query_map(
                "SELECT User, Host, IFNULL(JSON_VALUE(Priv, '$.account_locked'), 'false') = 'true' \
                 FROM mysql.global_priv WHERE User LIKE 't%'",
                |(u, h, l): (String, String, i64)| (u, h, l != 0),
            )
            .await
        {
            Ok(a) => a,
            Err(e) => {
                tracing::debug!(error = %e, "relay: the accounts can't be read");
                return 0;
            }
        };
        if !accounts.iter().any(|(_, h, l)| h == "%" && *l) {
            return 0;
        }
        let processes: Vec<(u64, String, String)> = c
            .query(
                "SELECT ID, USER, HOST FROM information_schema.PROCESSLIST \
                 WHERE USER LIKE 't%' AND ID <> CONNECTION_ID()",
            )
            .await
            .unwrap_or_default();
        let mut ended = 0;
        for id in direct_logins(&accounts, &processes) {
            match c.query_drop(format!("KILL CONNECTION {id}")).await {
                Ok(()) => ended += 1,
                // Gone already.
                Err(mysql_async::Error::Server(e)) if e.code == 1094 => {}
                Err(e) => tracing::warn!(id, error = %e, "relay: a direct login couldn't be ended"),
            }
        }
        if ended > 0 {
            tracing::warn!(cluster = %self.target.cluster, node = %self.target.node, ended,
                "relay: ended relayed tenants' connections that didn't come through a relay");
        }
        ended
    }

    async fn register(&self, reg: &mut Option<Session>) {
        if reg
            .as_ref()
            .is_some_and(|s| !s.is_lost() && self.clock.now() < s.valid_until())
        {
            return;
        }
        let Ok(s) = Session::open(
            Arc::clone(&self.store),
            Arc::clone(&self.clock),
            SessionOpts::new(LEASE_TTL),
        )
        .await
        else {
            return;
        };
        let Ok(key) = self.store.tier().key(&format!("agents/{}", self.agent_id)) else {
            return;
        };
        let value = Record::new(AgentRecord {
            cluster: self.target.cluster.to_string(),
            node: self.target.node.to_string(),
            addr: self.target.addr.clone(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            mysqld: MysqldState::Up,
            internal_url: Some(self.internal_url.clone()),
        })
        .encode();
        if let Ok(value) = value
            && self
                .store
                .txn(Txn {
                    when: Vec::new(),
                    then: vec![Op::Put {
                        key,
                        value,
                        lease: Some(s.lease()),
                    }],
                    otherwise: Vec::new(),
                })
                .await
                .is_ok_and(|t| t.succeeded)
        {
            *reg = Some(s);
        } else {
            let _ = s.close().await;
        }
    }

    /// Runs the relay on `listener` and the lease loop until `stop`.
    ///
    /// # Errors
    /// Internal never in practice.
    #[allow(clippy::too_many_lines)] // one loop, every step in order
    pub async fn run(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
        stop: CancellationToken,
    ) -> Result<()> {
        let relay_task = tokio::spawn(Arc::clone(&self.relay).serve_gated(listener, stop.clone()));
        loop {
            match self.mark_cluster().await {
                Ok(()) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "relay: can't mark the cluster as relayed yet; retrying");
                    tokio::select! {
                        () = stop.cancelled() => return Ok(()),
                        () = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
        let lease_key = relay_lease_key(self.store.tier(), &self.target.cluster)?;
        let mut reg: Option<Session> = None;
        let mut held: Option<(Session, u64)> = None;
        let mut conn: Option<ProbeSession> = None;
        let mut unusable_since: Option<Instant> = None;
        let mut last_epoch = 0u64;
        let bounded = Duration::from_secs(2);
        // The relay host in the marker: (host, when it was last checked).
        let mut published: Option<(String, Instant)> = None;
        let mut swept = Instant::now();
        self.set_status("standby");
        while !stop.is_cancelled() {
            let _ = tokio::time::timeout(bounded, self.register(&mut reg)).await;
            if published
                .as_ref()
                .is_none_or(|(_, at)| at.elapsed() >= HOST_RECHECK)
                && let Ok(Some(host)) = tokio::time::timeout(bounded, self.own_host()).await
            {
                match tokio::time::timeout(bounded, self.publish_host(&host)).await {
                    Ok(Ok(true)) => published = Some((host, Instant::now())),
                    Ok(Ok(false)) | Err(_) => {}
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "relay: the relay host can't be published yet");
                    }
                }
            }
            if swept.elapsed() >= DIRECT_LOGIN_SWEEP {
                swept = Instant::now();
                let _ = tokio::time::timeout(bounded, self.end_direct_logins()).await;
            }
            let usable = Self::usable(&mut conn, &self.target).await;
            if usable {
                unusable_since = None;
            } else {
                unusable_since.get_or_insert_with(Instant::now);
            }
            if let Some((s, epoch)) = &held {
                let lapsing = s.is_lost()
                    || s.valid_until().saturating_duration_since(self.clock.now())
                        < STAND_DOWN_MARGIN;
                let gone_bad = unusable_since.is_some_and(|t| t.elapsed() >= UNUSABLE_GRACE);
                if lapsing || gone_bad {
                    tracing::warn!(cluster = %self.target.cluster, epoch, lapsing, gone_bad,
                        "relay writer: standing down");
                    self.relay.fence().stand_down().await;
                    if let Some((s, _)) = held.take() {
                        let _ = s.close().await;
                    }
                    self.set_status("standby");
                }
            } else if usable && published.is_some() {
                // Not the writer: is the lease free? (Only once the relay
                // host is published: relayed tenants' accounts must exist
                // at this host before gateways log in through it.)
                let cur = tokio::time::timeout(
                    bounded,
                    self.store.get(&lease_key, Consistency::Linearizable),
                )
                .await;
                match cur {
                    Ok(Ok(Some(kv))) => {
                        if let Ok(r) = Record::<RelayLease>::decode(&kv.value) {
                            last_epoch = last_epoch.max(r.body.epoch);
                        }
                    }
                    Ok(Ok(None)) => {
                        let epoch = last_epoch + 1;
                        if let (Ok(s), Some(v)) = (
                            Session::open(
                                Arc::clone(&self.store),
                                Arc::clone(&self.clock),
                                SessionOpts::new(LEASE_TTL),
                            )
                            .await,
                            self.lease_value(epoch, false),
                        ) {
                            if self.put(&s, v, vec![Cmp::Missing(lease_key.clone())]).await {
                                last_epoch = epoch;
                                self.set_status("recovering");
                                tracing::info!(cluster = %self.target.cluster, epoch,
                                    "relay writer: lease taken; recovering every held branch on the node");
                                // Admits nobody until every redo row on
                                // the node is decided and replayed or
                                // dropped.
                                let started = Instant::now();
                                let recovered = loop {
                                    match self.relay.fence().start().await {
                                        Ok(r) => break Some(r),
                                        Err(e) => {
                                            tracing::warn!(error = %e, "relay writer: recovery failed; retrying");
                                            if s.is_lost() || stop.is_cancelled() {
                                                break None;
                                            }
                                            tokio::time::sleep(Duration::from_secs(1)).await;
                                        }
                                    }
                                };
                                let ready = recovered.is_some()
                                    && match self.lease_value(epoch, true) {
                                        Some(v) => self.put(&s, v, Vec::new()).await,
                                        None => false,
                                    };
                                if ready {
                                    tracing::info!(cluster = %self.target.cluster, epoch,
                                        ms = started.elapsed().as_millis(), report = ?recovered,
                                        "relay writer: ready");
                                    metrics::counter!("sprig_agent_relay_takeovers_total",
                                        "cluster" => self.target.cluster.to_string())
                                    .increment(1);
                                    self.set_status("ready");
                                    held = Some((s, epoch));
                                } else {
                                    self.relay.fence().stand_down().await;
                                    let _ = s.close().await;
                                    self.set_status("standby");
                                }
                            } else {
                                let _ = s.close().await;
                            }
                        }
                    }
                    _ => {}
                }
            }
            tokio::select! {
                () = stop.cancelled() => {}
                () = tokio::time::sleep(PERIOD) => {}
            }
        }
        // Stopping: refuse everyone, end every connection, hand back.
        self.relay.fence().stand_down().await;
        if let Some((s, _)) = held {
            let _ = s.close().await;
        }
        if let Some(s) = reg {
            let _ = s.close().await;
        }
        if let Some(c) = conn {
            c.close().await;
        }
        let _ = relay_task.await;
        Ok(())
    }
}

async fn holder_call(State(role): State<Arc<RelayRole>>, body: String) -> (StatusCode, String) {
    let call = match HolderCall::decode(&body) {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("err 39998 {e}")),
    };
    let r = role.relay.fence().call(&call).await;
    let code = if r.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    (code, encode_answer(&r))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_hosts_drop_the_port() {
        assert_eq!(account_host("172.20.0.5:51234"), "172.20.0.5");
        assert_eq!(account_host("localhost"), "localhost");
        assert_eq!(account_host("[::1]:3306"), "::1");
        assert_eq!(account_host("cf-relay-1:40000"), "cf-relay-1");
    }

    #[test]
    fn only_logins_around_the_relays_of_locked_tenants_are_ended() {
        let acc = |u: &str, h: &str, l: bool| (u.to_owned(), h.to_owned(), l);
        let accounts = vec![
            // Relayed tenant 7: '%' locked, logins from the relay host.
            acc("t7_u1", "%", true),
            acc("t7_u1", "10.0.0.9", false),
            acc("t7_u2", "%", true),
            acc("t7_u2", "10.0.0.9", false),
            // Tenant 8 isn't relayed.
            acc("t8_u1", "%", false),
            // A share definer is locked but isn't a user account.
            acc("t7_sprig_share", "%", true),
        ];
        let p = |id: u64, u: &str, h: &str| (id, u.to_owned(), h.to_owned());
        let processes = vec![
            p(1, "t7_u1", "10.0.0.9:4001"),
            p(2, "t7_u1", "192.168.65.1:4002"),
            p(3, "t7_u2", "10.0.0.10:4003"),
            p(4, "t8_u1", "192.168.65.1:4004"),
            p(5, "t7_sprig_share", "192.168.65.1:4005"),
        ];
        assert_eq!(direct_logins(&accounts, &processes), vec![2, 3]);
    }
}
