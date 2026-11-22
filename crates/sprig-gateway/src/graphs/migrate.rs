//! Moving a native graph from JSON tables to typed tables, online
//! (`sprig_graph::typed`). A graph moves when `graph.storage` names
//! `typed` for it (its object or database scope) and its `graphs.storage`
//! is 1. A statement on such a graph starts the move in the background
//! ([`GatewayGraphs::follow_storage`]): one task per graph in a process,
//! one process at a time through the catalog's lease (`storage_owner`,
//! renewed every [`RENEW`]). Every step is safe to run again, so a task
//! that dies or loses its lease leaves the graph for the next statement
//! to resume:
//!
//! ```text
//! storage 1, no phase  the typed base tables on every shard; phase dual (versioned)
//! storage 1, dual      every gateway acknowledges: writers write both, with the
//!                      same _v; two passes over every shard: nodes, then edges,
//!                      copied where the typed row is missing or has another _v,
//!                      then typed rows whose JSON row is gone deleted;
//!                      storage 2, phase drop
//! storage 2, drop      every gateway acknowledges (readers read typed tables);
//!                      the JSON node, edge, label and mirror tables go; no phase
//! ```
//!
//! Reads use the JSON tables until `storage` is 2. Index and constraint
//! commands wait for the move to end.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::Freshness;
use sprig_catalog::version::VersionKind;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::ast::Entity;
use sprig_graph::catalog::{GraphCatalog, Tables, backend_error};
use sprig_graph::maria::{ShardConns, ShardLoc};
use sprig_graph::typed::STORAGE_TYPED;
use sprig_graph::typed::store::{Shadow, TypedStore};

use super::{GatewayGraphs, GraphRef};

/// How long the lease lasts without a renewal.
const LEASE_SECS: u32 = 60;
/// How often the running task renews it.
const RENEW: Duration = Duration::from_secs(10);
/// How long after a failed (or refused) attempt a statement starts another.
const RETRY_AFTER: Duration = Duration::from_secs(30);

/// The moves this process runs or tried lately, and the writers' typed
/// copies of moving graphs.
#[derive(Debug, Default)]
pub(crate) struct MoveState {
    running: Mutex<HashSet<(u64, u64)>>,
    tried: Mutex<HashMap<(u64, u64), Instant>>,
    shadows: Mutex<HashMap<(u64, u64), Arc<Shadow>>>,
}

/// This process's name in the lease.
fn owner() -> &'static str {
    static OWNER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    OWNER.get_or_init(|| {
        let mut b = [0u8; 8];
        let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut b);
        format!(
            "gateway-{}-{:016x}",
            std::process::id(),
            u64::from_be_bytes(b)
        )
    })
}

/// The refusal of an index command while a graph moves.
pub(crate) fn moving_error() -> SprigError {
    SprigError::new(
        SprigCode::GraphUnsupported,
        "the graph is moving to typed tables; index and constraint changes wait until it finishes",
    )
}

/// Per-cluster connections of one copy page, each in its own transaction;
/// committed mirrors' clusters first (as the applier does).
struct PageConns {
    graphs: Arc<GatewayGraphs>,
    tenant: TenantId,
    conns: HashMap<String, mysql_async::Conn>,
}

#[async_trait]
impl ShardConns for PageConns {
    async fn conn(&mut self, at: &ShardLoc, _write: bool) -> Result<&mut mysql_async::Conn> {
        if !self.conns.contains_key(&at.cluster) {
            let id = ClusterId::new(&at.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", at.cluster)))?;
            let mut c = self
                .graphs
                .tenant_pool(self.tenant, &id)
                .await?
                .get_conn()
                .await
                .map_err(backend_error)?;
            c.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                .await
                .map_err(backend_error)?;
            c.query_drop("START TRANSACTION")
                .await
                .map_err(backend_error)?;
            self.conns.insert(at.cluster.clone(), c);
        }
        self.conns
            .get_mut(&at.cluster)
            .ok_or_else(|| SprigError::internal("copy connection"))
    }
}

impl PageConns {
    /// Commits every cluster's transaction, `home` (the page's shard,
    /// holding the share locks on the JSON rows) last; or rolls them all
    /// back.
    async fn end(self, home: &str, ok: bool) -> Result<()> {
        let mut order: Vec<String> = self.conns.keys().cloned().collect();
        order.sort_by_key(|c| (c == home, c.clone()));
        let mut conns = self.conns;
        let mut out = Ok(());
        for k in order {
            let Some(mut c) = conns.remove(&k) else {
                continue;
            };
            let end = if ok && out.is_ok() {
                "COMMIT"
            } else {
                "ROLLBACK"
            };
            if let Err(e) = c.query_drop(end).await {
                out = Err(backend_error(e));
            }
        }
        out
    }
}

impl GatewayGraphs {
    /// Whether `g` is moving to typed tables, or should start.
    pub(crate) fn wants_typed(&self, g: &GraphRef, cat: &sprig_catalog::TenantCatalog) -> bool {
        if g.def.kind != "native" || g.def.group_id.is_none() {
            return false;
        }
        if g.def.storage_phase.is_some() {
            return true;
        }
        g.def.storage != STORAGE_TYPED
            && self.svc.settings.get(
                &sprig_graph::settings::STORAGE,
                &cat.setting_ctx(&self.svc.dc)
                    .with_db(g.db_id)
                    .with_object(sprig_common::ids::ObjectId(g.graph_id)),
                Some(&cat.settings),
            ) == "typed"
    }
}

impl GatewayGraphs {
    /// The writers' typed copy of `g` while it moves (`None` otherwise).
    pub(crate) fn shadow(
        self: &Arc<Self>,
        g: &GraphRef,
        ids: Arc<sprig_graph::maria::CounterIds>,
    ) -> Option<Arc<Shadow>> {
        if g.def.storage == STORAGE_TYPED || g.def.storage_phase.as_deref() != Some("dual") {
            return None;
        }
        let key = (g.tenant.0, g.graph_id);
        let mut m = self
            .moves
            .shadows
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(s) = m.get(&key) {
            return Some(Arc::clone(s));
        }
        let (labels, types) = self.names_of(g);
        let s = Arc::new(Shadow::new(
            Arc::clone(&g.shards),
            self.typed_schema(g),
            self.typed_ddl(g),
            labels,
            types,
            self.dictionary(g),
            ids,
            g.def.indexes.clone(),
        ));
        m.insert(key, Arc::clone(&s));
        Some(s)
    }

    /// Starts moving `g` to typed tables in the background when it should
    /// (see the module docs). Never the statement's business: failures are
    /// logged.
    pub(crate) fn follow_storage(
        self: &Arc<Self>,
        g: &GraphRef,
        cat: &sprig_catalog::TenantCatalog,
    ) {
        if !self.wants_typed(g, cat) {
            return;
        }
        let key = (g.tenant.0, g.graph_id);
        {
            let mut t = self
                .moves
                .tried
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if t.get(&key).is_some_and(|x| x.elapsed() < RETRY_AFTER) {
                return;
            }
            t.insert(key, Instant::now());
        }
        if !self
            .moves
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key)
        {
            return;
        }
        let me = Arc::clone(self);
        let g = g.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            match me.move_storage(&g).await {
                Ok(true) => tracing::info!(tenant = %g.tenant, graph = g.graph_id,
                    secs = started.elapsed().as_secs(), "graph moved to typed tables"),
                Ok(false) => {}
                Err(e) => tracing::warn!(tenant = %g.tenant, graph = g.graph_id, error = %e,
                    "graph move to typed tables stopped; the next statement resumes it"),
            }
            me.moves
                .running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        });
    }

    async fn lease(&self, g: &GraphRef) -> Result<bool> {
        GraphCatalog::lease_storage(
            &*self.svc.records,
            g.tenant,
            g.graph_id,
            owner(),
            LEASE_SECS,
        )
        .await
    }

    async fn move_storage(self: &Arc<Self>, g: &GraphRef) -> Result<bool> {
        if !self.lease(g).await? {
            return Ok(false);
        }
        let lost = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let renew = {
            let (me, g, lost, stop) = (
                Arc::clone(self),
                g.clone(),
                Arc::clone(&lost),
                Arc::clone(&stop),
            );
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(RENEW).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if !matches!(me.lease(&g).await, Ok(true)) {
                        lost.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            })
        };
        let r = self.move_steps(g, &lost).await;
        stop.store(true, Ordering::Relaxed);
        renew.abort();
        let _ =
            GraphCatalog::release_storage(&*self.svc.records, g.tenant, g.graph_id, owner()).await;
        r
    }

    #[allow(clippy::too_many_lines)] // the phases, each a few steps
    async fn move_steps(self: &Arc<Self>, g: &GraphRef, lost: &AtomicBool) -> Result<bool> {
        loop {
            if lost.load(Ordering::Relaxed) {
                return Err(SprigError::internal("the graph move's lease was lost"));
            }
            let cat = self.catalog(g.tenant, Freshness::Latest).await?;
            let Some(now) = self.resolve(&cat, &g.db, &g.name).await? else {
                return Ok(false);
            };
            if now.graph_id != g.graph_id {
                return Ok(false);
            }
            match (now.def.storage, now.def.storage_phase.as_deref()) {
                (1, None) => {
                    if !self.wants_typed(&now, &cat) {
                        return Ok(false);
                    }
                    for sh in &now.shards.shards {
                        let mut c = self.shard_conn(g.tenant, &sh.cluster).await?;
                        for sql in sprig_graph::typed::base_ddl(&sh.schema, g.graph_id) {
                            c.query_drop(sql).await.map_err(backend_error)?;
                        }
                    }
                    self.set_storage(g, 1, Some("dual")).await?;
                    tracing::info!(tenant = %g.tenant, graph = g.graph_id, "graph move: writers write both");
                }
                (1, Some("dual")) => {
                    // Every gateway writes both before the passes.
                    crate::tenant::lookups_sql::publish_wait(&self.svc, g.tenant, cat.version)
                        .await;
                    for pass in 1..=2 {
                        let now = self.refresh(&now).await?;
                        let (read, written, pruned) = self.copy_pass(&now, lost).await?;
                        tracing::info!(tenant = %g.tenant, graph = g.graph_id, pass, read, written, pruned,
                            "graph move: rows copied to typed tables");
                    }
                    self.set_storage(g, STORAGE_TYPED, Some("drop")).await?;
                    self.moves
                        .shadows
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&(g.tenant.0, g.graph_id));
                }
                (s, Some(_)) => {
                    // Every gateway reads typed tables before the JSON ones go.
                    crate::tenant::lookups_sql::publish_wait(&self.svc, g.tenant, cat.version)
                        .await;
                    for sh in &now.shards.shards {
                        let mut c = self.shard_conn(g.tenant, &sh.cluster).await?;
                        let t = Tables::of(&sh.schema, g.graph_id);
                        for table in [&t.nodes, &t.edges, &t.labels, &t.mirrors] {
                            c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
                                .await
                                .map_err(backend_error)?;
                        }
                    }
                    self.set_storage(g, s, None).await?;
                    return Ok(true);
                }
                _ => return Ok(true),
            }
        }
    }

    /// One pass over every shard: nodes, then edges, copied page by page;
    /// then typed rows without a JSON row deleted. Answers rows read,
    /// written and pruned.
    async fn copy_pass(
        self: &Arc<Self>,
        g: &GraphRef,
        lost: &AtomicBool,
    ) -> Result<(u64, u64, u64)> {
        let (mut read, mut written, mut pruned) = (0u64, 0u64, 0u64);
        let ids = self.counters(g).await?;
        for entity in [Entity::Node, Entity::Relationship] {
            for (i, sh) in g.shards.shards.iter().enumerate() {
                let mut after = 0u64;
                loop {
                    if lost.load(Ordering::Relaxed) {
                        return Err(SprigError::internal("the graph move's lease was lost"));
                    }
                    let mut conns = PageConns {
                        graphs: Arc::clone(self),
                        tenant: g.tenant,
                        conns: HashMap::new(),
                    };
                    let r = {
                        let mut store = self.copy_store(g, &mut conns, Arc::clone(&ids));
                        let r = store.copy_page(i, entity, after).await;
                        if r.is_ok() {
                            self.learn_typed(g.tenant, g.graph_id, store.typed_schema());
                        }
                        r
                    };
                    conns.end(&sh.cluster, r.is_ok()).await?;
                    let p = r?;
                    read += p.read as u64;
                    written += p.written as u64;
                    match p.last {
                        Some(l) if p.read > 0 => after = l,
                        _ => break,
                    }
                }
            }
        }
        // Rows deleted from the JSON tables before every writer wrote both.
        let schema = self.typed_schema(g);
        let mut owners: Vec<(Entity, u32)> = vec![(Entity::Node, 0)];
        owners.extend(schema.types.iter().map(|t| (Entity::Relationship, *t)));
        for (entity, ty) in owners {
            for (i, sh) in g.shards.shards.iter().enumerate() {
                let mut after = 0u64;
                loop {
                    let mut conns = PageConns {
                        graphs: Arc::clone(self),
                        tenant: g.tenant,
                        conns: HashMap::new(),
                    };
                    let r = {
                        let mut store = self.copy_store(g, &mut conns, Arc::clone(&ids));
                        store.prune_page(i, entity, ty, after).await
                    };
                    conns.end(&sh.cluster, r.is_ok()).await?;
                    let p = r?;
                    pruned += p.written as u64;
                    match p.last {
                        Some(l) => after = l,
                        None => break,
                    }
                }
            }
        }
        Ok((read, written, pruned))
    }

    /// A typed store for a move's pages: the graph's typed schema, mirrors
    /// in the page's transaction, no uniqueness claims (the JSON rows'
    /// claims stand for both).
    fn copy_store<'c>(
        self: &Arc<Self>,
        g: &GraphRef,
        conns: &'c mut PageConns,
        ids: Arc<sprig_graph::maria::CounterIds>,
    ) -> TypedStore<'c> {
        let (labels, types) = self.names_of(g);
        let indexes: Vec<_> = g
            .def
            .indexes
            .iter()
            .filter(|d| !d.unique)
            .cloned()
            .collect();
        TypedStore::new(
            conns,
            Arc::clone(&g.shards),
            self.typed_schema(g),
            self.typed_ddl(g),
            labels,
            types,
            self.dictionary(g),
            ids,
        )
        .with_schema(indexes, None)
        .sync_mirrors()
    }

    /// A `sprig_ctl` connection to `cluster`.
    async fn shard_conn(&self, tenant: TenantId, cluster: &str) -> Result<mysql_async::Conn> {
        let id = ClusterId::new(cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
        self.tenant_pool(tenant, &id)
            .await?
            .get_conn()
            .await
            .map_err(backend_error)
    }

    /// Writes `graphs.storage` and `storage_phase` in a versioned change.
    async fn set_storage(&self, g: &GraphRef, s: u8, phase: Option<&str>) -> Result<u64> {
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = match ch.tx().records().await {
            Err(e) => Err(e),
            Ok(rec) => GraphCatalog::set_storage(rec, g.graph_id, s, phase).await,
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        ch.commit().await
    }
}
