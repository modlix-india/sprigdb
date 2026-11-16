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
        let registry = Arc::new(RelayRegistry::new(own_pool));
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
        self.set_status("standby");
        while !stop.is_cancelled() {
            let _ = tokio::time::timeout(bounded, self.register(&mut reg)).await;
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
            } else if usable {
                // Not the writer: is the lease free?
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
