//! `graph_mirror` (graph mirrors and their freshness): one long-running job per tenant
//! (`spec: {"tenant": <id>}`), keeping the asynchronous mirrors of the
//! tenant's grouped graphs ([`sprig_graph::mirror`]).
//!
//! ```text
//! serve(tenant):
//!   open a binlog reader on every cluster of the DC (registered, not
//!   retired), each after its checkpointed position or at its position now
//!   loop:
//!     every 2 s, beside the loop: the graphs served (grouped native
//!       graphs whose mirror_mode is starting or async, or whose mirrors
//!       are building) and their shard maps (the worker's watched copy),
//!       the DC's clusters, the catalog's and the clusters' writer pools;
//!       once that lookup is back, the loop takes them: clusters added to
//!       the DC get a reader; when a graph stopped being served, the
//!       position rows of dropped graphs are deleted
//!     take what the readers have (at most 512 groups, about 20,000 rows,
//!       or 20 ms)
//!     per group, in each cluster's commit order: skip the mover's copies
//!       and replays (they carry its position row); hold an XA branch's
//!       rows from its PREPARE to its COMMIT; edge-table rows of a served
//!       graph become EdgeChanges
//!     apply each graph's changes (apply_batch); a batch whose shards
//!       changed epoch meanwhile (in the watched map) runs again with the
//!       new map; one that failed on a moved shard reads the map from etcd
//!     each cluster's position moves past what was applied
//!     every 50 ms (and at least every second): a position row per shard
//!       of each served graph, its cluster's position
//!     every 2 s: the positions as the job's progress (its checkpoint),
//!       written beside the loop, backing off after failures (Checkpoint)
//! ```
//!
//! **No etcd in the loop.** The loop doesn't read etcd: shard maps come
//! from the worker's watched copy, the catalog's and the clusters' writer
//! pools are looked up beside the loop every 2 s (and soon after a
//! failure, the old ones serving meanwhile), and the checkpoint is written
//! beside the loop. When etcd was slow, each of those reads made the
//! applier fall behind, and reads waited for it. Only a batch that failed
//! on a moved shard reads its map from etcd, and only the job's first
//! lookup is waited for.
//!
//! Delivery is at least once: a new owner resumes at the checkpoint and
//! applies again, which is harmless because every step is idempotent and
//! re-checks the edge. Positions written are never ahead of what was
//! applied, so readers that waited on them never miss an edge.
//!
//! **Readers of every cluster.** A shard can move to any cluster, and its
//! writes there must be read from before the move's cutover, so the job
//! reads every cluster of the DC all the time, not only those holding the
//! graph now. Each reader is a subscription to the worker's shared reader
//! ([`sprig_feed::shared`]): one binlog dump per cluster and
//! worker, however many tenants have mirrors. Its view ([`shared_view`])
//! keeps the edge tables only, so the job isn't handed the rest of its
//! tenant's writes (a whole-tenant view had it detached from the reader
//! under load), and sends idle positions every 200 ms, as a dump of its
//! own did, not every second: a mirror read waits for the positions to
//! reach the cluster's position now, which others' writes move on.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_binlog::GtidPos;
use sprig_binlog::value::Value as BinValue;
use sprig_catalog::CatalogLocator;
use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};
use sprig_common::names::quote;
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_feed::shared::{IDLE_TICK, Kept, SharedClusters, SharedFeed};
use sprig_feed::source::{RawRow, RowKind, XaMark};
use sprig_feed::{Clusters, SourceItem, TxnSource};
use sprig_graph::catalog::Tables;
use sprig_graph::maria::{GraphShards, ShardLoc};
use sprig_graph::mirror::{self, ApplyConns, EdgeChange};
use sprig_meta::MetaStore;
use sprig_shard::{ShardMap, ShardMapCache, ShardMapStore, shard_schema};
use tokio::sync::mpsc;

use crate::runtime::{JobCtx, JobHandler};

pub use sprig_graph::mirror::JOB_KIND;

/// What the job needs.
#[derive(Clone)]
pub struct GraphMirrorEnv {
    /// This DC's tier (shard maps).
    pub dc: Arc<dyn MetaStore>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// Binlog sources: the worker's shared reader ([`shared_view`]).
    pub clusters: Arc<dyn Clusters>,
    /// This DC's shard maps, kept current by a watch: what a batch is
    /// checked against after it is applied (see [`Applier::apply`]).
    pub maps: ShardMapCache,
}

impl std::fmt::Debug for GraphMirrorEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphMirrorEnv").finish_non_exhaustive()
    }
}

/// Groups one take of the readers' items holds at most.
const TAKE_GROUPS: usize = 512;

/// Rows one take holds at most (about): a bulk load's groups carry 1,000
/// edges each, and 512 of them made one batch of half a million changes,
/// one transaction per cluster, decoded and coalesced in one poll of a
/// runtime thread.
const TAKE_ROWS: usize = 20_000;

/// Graphs' edge tables, in every tenant's shard schemas.
const EDGE_TABLES: &str = "__sprig_ge_*";

/// The job's view of the worker's shared reader: the edge tables of
/// every tenant (XA marks and the mover's applier marks come with them),
/// and an idle position every [`IDLE_TICK`] (see the module docs).
#[must_use]
pub fn shared_view(feed: &Arc<SharedFeed>) -> Arc<SharedClusters> {
    feed.view_every(Kept::Tables(vec![EDGE_TABLES.to_owned()].into()), IDLE_TICK)
        .named("graph_mirror")
}

/// `graph_mirror` (see the module docs).
#[derive(Debug, Clone)]
pub struct GraphMirror(pub GraphMirrorEnv);

#[async_trait]
impl JobHandler for GraphMirror {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(
            cx.spec["tenant"]
                .as_u64()
                .ok_or_else(|| "spec.tenant is missing".to_owned())?,
        );
        let mut job = Applier::new(self.0.clone(), tenant);
        job.serve(cx).await
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// A served graph.
#[derive(Debug, Clone)]
struct Served {
    layout: Arc<GraphShards>,
    /// Typed storage (`graphs.storage = 2`): a table per type.
    typed: bool,
}

/// Items from the readers: (cluster, the reader's generation, an item or
/// the error that ended it).
type Item = (ClusterId, u64, std::result::Result<SourceItem, String>);

/// An XA branch prepared but not committed yet: its cluster, xid, the
/// position before its PREPARE, and its rows.
type Held = (ClusterId, String, GtidPos, Vec<RawRow>);

/// Connections for [`mirror::apply_batch`]: fresh ones from the clusters'
/// writer pools, which the applier keeps between batches (looking one up
/// reads the cluster's record from etcd).
struct PoolConns {
    pools: Arc<dyn ClusterPools>,
    writers: HashMap<String, mysql_async::Pool>,
    /// This DC's tier and the tenant: the mirror locks and writes the
    /// tenant's edge rows, so for a tenant on Sprig 2PC's relay path
    /// connections go through the relay writer.
    relay: (Arc<dyn MetaStore>, TenantId),
}

#[async_trait]
impl ApplyConns for PoolConns {
    async fn connect(&mut self, cluster: &str) -> Result<mysql_async::Conn> {
        let pool = if let Some(p) = self.writers.get(cluster) {
            p.clone()
        } else {
            let id = ClusterId::new(cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
            let p = self.pools.writer(&id).await?;
            self.writers.insert(cluster.to_owned(), p.clone());
            p
        };
        let id = ClusterId::new(cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
        let pool = crate::relay_pools::shard_pool(&self.relay.0, self.relay.1, &id, pool).await?;
        pool.get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)
    }
}

struct Applier {
    env: GraphMirrorEnv,
    tenant: TenantId,
    graphs: HashMap<u64, Served>,
    /// Each cluster's applied position.
    pos: BTreeMap<ClusterId, GtidPos>,
    /// Readers running, by cluster: their generation and stop token.
    readers: HashMap<ClusterId, (u64, CancellationToken)>,
    next_gen: u64,
    /// XA branches prepared but not committed, oldest first.
    held: Vec<Held>,
    applied: u64,
    /// Whether the catalog's position rows were read (once, at the start).
    resumed: bool,
    /// Whether the position rows of dropped graphs were deleted since the
    /// served graphs last changed.
    orphans_cleared: bool,
    /// The tenant catalog's writer pool and schema, and the clusters'
    /// writer pools, looked up again at every refresh and after a
    /// failure. Looking them up reads etcd (the tenant's directory entry,
    /// the cluster's record and its published writer), which cost every
    /// position write and every batch an etcd round trip.
    catalog: Option<(mysql_async::Pool, String)>,
    writers: HashMap<String, mysql_async::Pool>,
    /// The refresh's lookups running beside the loop ([`look`]).
    looking: Option<tokio::task::JoinHandle<Result<Refreshed>>>,
}

/// What a refresh found ([`look`]).
struct Refreshed {
    /// The tenant catalog's writer pool and schema.
    catalog: (mysql_async::Pool, String),
    /// The graphs served, with their layouts.
    graphs: HashMap<u64, Served>,
    /// The DC's clusters (each gets a reader).
    clusters: HashSet<ClusterId>,
    /// Their writer pools, by cluster.
    writers: HashMap<String, mysql_async::Pool>,
}

/// A refresh's lookups, beside the loop: the catalog's writer pool, the
/// graphs served and their maps (the watched copy; `current`'s layout
/// where that copy is no newer), the DC's clusters and their writer
/// pools. Each lookup reads etcd.
async fn look(
    env: GraphMirrorEnv,
    tenant: TenantId,
    current: HashMap<u64, Served>,
) -> Result<Refreshed> {
    let ep = env.locator.writer(tenant).await?;
    let catalog = (ep.pool, ep.store.schema().to_owned());
    let mut c = catalog
        .0
        .get_conn()
        .await
        .map_err(sprig_graph::catalog::backend_error)?;
    let served = |cols: &str| {
        format!(
            "SELECT {cols} FROM {}.graphs WHERE kind = 'native' \
             AND group_id IS NOT NULL AND state = 'active' \
             AND (mirror_mode IN ('starting', 'async') OR mirror_state = 'building')",
            quote(&catalog.1)
        )
    };
    let rows: Vec<(u64, u32, u32, u8)> = match c
        .query(served("graph_id, db_id, group_id, storage"))
        .await
    {
        Ok(r) => r,
        // Before the catalog's graph migration 4: JSON graphs only.
        Err(mysql_async::Error::Server(e)) if e.code == 1054 => match c
            .query::<(u64, u32, u32), _>(served("graph_id, db_id, group_id"))
            .await
        {
            Ok(r) => r.into_iter().map(|(g, d, gr)| (g, d, gr, 1)).collect(),
            // Before migration 3: nothing to serve.
            Err(mysql_async::Error::Server(e)) if e.code == 1054 || e.code == 1146 => Vec::new(),
            Err(e) => return Err(sprig_graph::catalog::backend_error(e)),
        },
        Err(mysql_async::Error::Server(e)) if e.code == 1146 => Vec::new(),
        Err(e) => return Err(sprig_graph::catalog::backend_error(e)),
    };
    drop(c);
    let mut graphs = HashMap::new();
    for (graph, db, group, storage) in rows {
        let typed = storage == sprig_graph::typed::STORAGE_TYPED;
        // The watched map, unless a linearizable read after a failed
        // batch already gave a newer one.
        if let Some(s) = current.get(&graph)
            && s.layout.db == db
            && s.layout.group == group
            && s.typed == typed
            && env
                .maps
                .get(tenant, GroupId(group))
                .is_ok_and(|m| m.version <= s.layout.version)
        {
            graphs.insert(graph, s.clone());
            continue;
        }
        let (map, _) = map_of(&env, tenant, GroupId(group), false).await?;
        graphs.insert(
            graph,
            Served {
                layout: Arc::new(GraphShards {
                    graph,
                    db,
                    group,
                    version: map.version,
                    shards: shards_of(tenant, graph, db, group, &map),
                    seq: String::new(),
                    mirrors: true,
                    mirror_async: true,
                    mirror_building: false,
                    route: None,
                }),
                typed,
            },
        );
    }
    let clusters: HashSet<ClusterId> = env.pools.sweepable().await?.into_iter().collect();
    // Every cluster's writer pool (a shard can move to any of them); the
    // pools are cached by the cluster's writer, so a lookup that finds the
    // same writer gives the same pool.
    let mut writers = HashMap::new();
    if !graphs.is_empty() {
        for c in &clusters {
            match env.pools.writer(c).await {
                Ok(p) => {
                    writers.insert(c.to_string(), p);
                }
                Err(e) => {
                    tracing::debug!(%tenant, cluster = %c, error = %e, "graph mirror writer pool not looked up");
                }
            }
        }
    }
    Ok(Refreshed {
        catalog,
        graphs,
        clusters,
        writers,
    })
}

/// `group`'s map and whether it was read linearizably: the watched copy,
/// unless `latest` or the copy has none; the copy again when the
/// linearizable read fails (etcd timing out).
async fn map_of(
    env: &GraphMirrorEnv,
    tenant: TenantId,
    group: GroupId,
    latest: bool,
) -> Result<(Arc<ShardMap>, bool)> {
    if !latest && let Ok(m) = env.maps.get(tenant, group) {
        return Ok((m, false));
    }
    match ShardMapStore::new(Arc::clone(&env.dc))
        .load(tenant, group)
        .await
    {
        Ok((m, _)) => Ok((Arc::new(m), true)),
        Err(e) => match env.maps.get(tenant, group) {
            Ok(m) => {
                tracing::debug!(%tenant, %group, error = %e, "shard map read failed; using the watched copy");
                Ok((m, false))
            }
            Err(_) => Err(e),
        },
    }
}

/// The shards of `graph` under `map`.
fn shards_of(tenant: TenantId, graph: u64, db: u32, group: u32, map: &ShardMap) -> Vec<ShardLoc> {
    map.ranges
        .iter()
        .map(|r| {
            let schema = shard_schema(tenant, DbId(db), GroupId(group), r.shard);
            ShardLoc {
                shard: r.shard.0,
                lo: r.lo,
                epoch: r.epoch,
                cluster: r.cluster.to_string(),
                tables: Tables::of(&schema, graph),
                schema,
            }
        })
        .collect()
}

impl Applier {
    fn new(env: GraphMirrorEnv, tenant: TenantId) -> Self {
        Self {
            env,
            tenant,
            graphs: HashMap::new(),
            pos: BTreeMap::new(),
            readers: HashMap::new(),
            next_gen: 1,
            held: Vec::new(),
            applied: 0,
            resumed: false,
            orphans_cleared: false,
            catalog: None,
            writers: HashMap::new(),
            looking: None,
        }
    }

    #[allow(clippy::too_many_lines)] // the loop of the module docs
    async fn serve(&mut self, cx: &JobCtx) -> std::result::Result<Value, String> {
        // The checkpoint: positions per cluster.
        if let Some(m) = cx.progress.get("pos").and_then(Value::as_object) {
            for (c, p) in m {
                if let (Ok(c), Some(Ok(p))) =
                    (ClusterId::new(c), p.as_str().map(str::parse::<GtidPos>))
                {
                    self.pos.insert(c, p);
                }
            }
        }
        let (tx, mut rx) = mpsc::channel::<Item>(1024);
        let mut last_refresh: Option<Instant> = None;
        let mut last_pos_write = Instant::now();
        let mut checkpoint = Checkpoint::new(cx, self.tenant);
        let mut dirty = true;
        let result = loop {
            if cx.cancel.is_cancelled() {
                break Err("cancelled".to_owned());
            }
            // The refresh's lookups run beside the loop; the loop takes
            // their answer once it is back (it waits only for the first,
            // before it has readers).
            if self.looking.is_none()
                && last_refresh.is_none_or(|t| t.elapsed() >= Duration::from_secs(2))
            {
                self.looking = Some(tokio::spawn(look(
                    self.env.clone(),
                    self.tenant,
                    self.graphs.clone(),
                )));
                last_refresh = Some(Instant::now());
            }
            if self
                .looking
                .as_ref()
                .is_some_and(|h| h.is_finished() || self.readers.is_empty())
                && let Some(h) = self.looking.take()
            {
                let r = match h.await {
                    Ok(r) => r,
                    Err(e) => Err(SprigError::internal(format!("graph mirror lookup: {e}"))),
                };
                match r {
                    Ok(r) => {
                        if let Err(e) = self.refresh(r, &tx).await {
                            tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror refresh failed");
                        }
                        dirty = true;
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror refresh failed");
                        // Not again at once (the readers wait for the first).
                        if self.readers.is_empty() {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
            // Take what the readers have.
            let mut items: Vec<Item> = Vec::new();
            let mut rows = 0usize;
            let deadline = tokio::time::sleep(Duration::from_millis(20));
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    () = cx.cancel.cancelled() => break,
                    () = &mut deadline => break,
                    it = rx.recv() => match it {
                        Some(it) => {
                            if let (_, _, Ok(SourceItem::Txn(t))) = &it {
                                rows += t.rows.len();
                            }
                            items.push(it);
                            if items.len() >= TAKE_GROUPS || rows >= TAKE_ROWS {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            if !items.is_empty() {
                match self.handle(items).await {
                    Ok(moved) => dirty |= moved,
                    Err(e) => {
                        // Nothing past the batch was recorded: start the
                        // readers again from the positions (items of the
                        // old readers are ignored by generation).
                        tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror batch failed; rereading");
                        // The pools serve until the refresh, looked up
                        // again at once beside the loop, replaces them; the
                        // readers start again from the positions.
                        let clusters: HashSet<ClusterId> = self.readers.keys().cloned().collect();
                        self.stop_readers();
                        self.held.clear();
                        last_refresh = None;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if let Err(e) = self.start_readers(clusters, &tx).await {
                            tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror readers not started again");
                        }
                        continue;
                    }
                }
            }
            if (dirty && last_pos_write.elapsed() >= Duration::from_millis(50))
                || last_pos_write.elapsed() >= Duration::from_secs(1)
            {
                match self.write_positions().await {
                    Ok(()) => {
                        last_pos_write = Instant::now();
                        dirty = false;
                    }
                    Err(e) => {
                        // Looked up again beside the loop.
                        last_refresh = None;
                        tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror positions not written");
                    }
                }
            }
            if !checkpoint.poll().await {
                break Err("lost the job".to_owned());
            }
            if checkpoint.due() {
                let pos: serde_json::Map<String, Value> = self
                    .pos
                    .keys()
                    .filter_map(|c| {
                        self.safe_pos(c)
                            .map(|p| (c.to_string(), Value::String(p.to_string())))
                    })
                    .collect();
                checkpoint.start(
                    json!({ "pos": pos, "applied": self.applied, "graphs": self.graphs.len() }),
                );
            }
            checkpoint.report_age();
        };
        self.stop_readers();
        result
    }

    fn stop_readers(&mut self) {
        for (_, (_, t)) in self.readers.drain() {
            t.cancel();
        }
    }

    /// The position of `c` that is safe to record: before the oldest XA
    /// branch still held there (its rows aren't applied yet).
    fn safe_pos(&self, c: &ClusterId) -> Option<GtidPos> {
        self.held
            .iter()
            .find(|h| &h.0 == c)
            .map(|h| h.2.clone())
            .or_else(|| self.pos.get(c).cloned())
    }

    /// Takes a refresh's lookups ([`look`]): its pools, the graphs
    /// served (a layout read linearizably meanwhile stays where it is
    /// newer), and a reader for each cluster without one. Reads no etcd
    /// (a new cluster's position is its binlog's).
    async fn refresh(&mut self, r: Refreshed, tx: &mpsc::Sender<Item>) -> Result<()> {
        self.catalog = Some(r.catalog);
        self.writers.extend(r.writers);
        let mut graphs = r.graphs;
        for (g, s) in &mut graphs {
            if let Some(have) = self.graphs.get(g)
                && have.layout.group == s.layout.group
                && have.layout.version > s.layout.version
            {
                *s = have.clone();
            }
        }
        if self.graphs.keys().any(|g| !graphs.contains_key(g)) {
            self.orphans_cleared = false;
        }
        self.graphs = graphs;
        if !self.orphans_cleared {
            self.clear_orphans().await;
        }
        self.resume_once().await;
        // A reader on every cluster of the DC.
        self.readers.retain(|c, (_, t)| {
            let keep = r.clusters.contains(c) && !t.is_cancelled();
            if !keep {
                t.cancel();
            }
            keep
        });
        self.start_readers(r.clusters, tx).await
    }

    /// A reader for each of `clusters` without one, from its position (a
    /// new cluster's: its binlog's now).
    async fn start_readers(
        &mut self,
        clusters: HashSet<ClusterId>,
        tx: &mpsc::Sender<Item>,
    ) -> Result<()> {
        for c in clusters {
            if self.readers.contains_key(&c) {
                continue;
            }
            // A reader starts again before the oldest branch it held.
            let from = if let Some(p) = self.safe_pos(&c) {
                p
            } else {
                let p = self.env.clusters.position(&c).await?;
                self.pos.insert(c.clone(), p.clone());
                p
            };
            self.held.retain(|h| h.0 != c);
            self.pos.insert(c.clone(), from.clone());
            let token = CancellationToken::new();
            let gen_ = self.next_gen;
            self.next_gen += 1;
            self.readers.insert(c.clone(), (gen_, token.clone()));
            tokio::spawn(read(
                Arc::clone(&self.env.clusters),
                self.tenant,
                c,
                gen_,
                from,
                tx.clone(),
                token,
            ));
        }
        Ok(())
    }

    /// [`Self::resume_from_catalog`], until it succeeds once.
    async fn resume_once(&mut self) {
        if self.resumed {
            return;
        }
        match self.resume_from_catalog().await {
            Ok(()) => self.resumed = true,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e, "graph mirror positions not read; resuming at the checkpoint");
            }
        }
    }

    /// Moves each cluster's start up to what the catalog's position rows
    /// say every served graph has applied, when that is past the job's
    /// checkpoint (or there is no checkpoint). The rows are written every
    /// 50 ms to a second, never ahead of what was applied; the checkpoint
    /// only every 2 s, through the DC's store, and not at all while its
    /// writes time out. A job that moved after its worker died replayed
    /// everything since the checkpoint, its mirrors' count standing
    /// still meanwhile; and a job submitted afresh, with no checkpoint,
    /// started at the clusters' positions now, skipping the edges written
    /// since its predecessor's last position.
    async fn resume_from_catalog(&mut self) -> Result<()> {
        if self.graphs.is_empty() {
            return Ok(());
        }
        let (pool, schema) = self.catalog().await?;
        let mut conn = pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        // Per cluster: the oldest position over the served graphs, and
        // whether every served graph with shards there has rows for all
        // of them (else the rows can't bound the start).
        let mut at: BTreeMap<String, Option<GtidPos>> = BTreeMap::new();
        for (graph, served) in &self.graphs {
            let rows = mirror::read_pos(&mut conn, &schema, *graph).await?;
            for sh in &served.layout.shards {
                let row_pos = rows
                    .iter()
                    .find(|r| r.shard == sh.shard && r.cluster == sh.cluster)
                    .and_then(|r| r.pos.parse::<GtidPos>().ok());
                let oldest = at
                    .entry(sh.cluster.clone())
                    .or_insert_with(|| row_pos.clone());
                *oldest = match (oldest.take(), row_pos) {
                    (Some(have), Some(row)) => Some(sprig_feed::shared::older(&have, &row)),
                    _ => None,
                };
            }
        }
        drop(conn);
        for (cluster, p) in at {
            let (Ok(id), Some(p)) = (ClusterId::new(&cluster), p) else {
                continue;
            };
            let start = match self.pos.get(&id) {
                Some(cp) => sprig_feed::shared::newer(cp, &p),
                None => p,
            };
            if self.pos.get(&id) != Some(&start) {
                tracing::info!(tenant = %self.tenant, %cluster, checkpoint = ?self.pos.get(&id).map(ToString::to_string), from = %start, "graph mirror resumes at the catalog's positions");
                self.pos.insert(id, start);
            }
        }
        Ok(())
    }

    /// Applies one take of the readers' items; whether a position moved.
    async fn handle(&mut self, items: Vec<Item>) -> Result<bool> {
        // Changes per graph, and each cluster's position after them.
        let mut changes: BTreeMap<u64, Vec<EdgeChange>> = BTreeMap::new();
        let mut moved: BTreeMap<ClusterId, GtidPos> = BTreeMap::new();
        for (cluster, gen_, it) in items {
            if self.readers.get(&cluster).is_none_or(|r| r.0 != gen_) {
                continue; // a stopped reader's
            }
            let item = match it {
                Ok(i) => i,
                Err(e) => {
                    // The reader ended; it is opened again at the position.
                    if let Some((_, t)) = self.readers.remove(&cluster) {
                        t.cancel();
                    }
                    tracing::warn!(%cluster, error = %e, "graph mirror reader ended");
                    continue;
                }
            };
            match item {
                SourceItem::Idle { pos } => {
                    moved.insert(cluster, pos);
                }
                SourceItem::Txn(t) => {
                    let after = t.after();
                    let take = |held: &mut Vec<Held>, x: &str| -> Vec<RawRow> {
                        match held.iter().position(|h| h.0 == cluster && h.1 == x) {
                            Some(i) => held.remove(i).3,
                            None => Vec::new(),
                        }
                    };
                    let rows = match &t.xa {
                        Some(XaMark::Prepare(x)) => {
                            if !t.apply {
                                self.held.push((
                                    cluster.clone(),
                                    x.clone(),
                                    t.before.clone(),
                                    t.rows,
                                ));
                            }
                            Vec::new()
                        }
                        Some(XaMark::Commit(x)) => take(&mut self.held, x),
                        Some(XaMark::Rollback(x)) => {
                            take(&mut self.held, x);
                            Vec::new()
                        }
                        None if t.apply => Vec::new(),
                        None => t.rows,
                    };
                    for r in &rows {
                        if let Some((g, c)) = self.edge_change(r) {
                            changes.entry(g).or_default().push(c);
                        }
                    }
                    moved.insert(cluster, after);
                }
            }
        }
        for (graph, ch) in changes {
            self.apply(graph, &ch).await?;
        }
        let any = !moved.is_empty();
        for (c, p) in moved {
            self.pos.insert(c, p);
        }
        Ok(any)
    }

    /// An edge-table row of a served graph as a change.
    fn edge_change(&self, r: &RawRow) -> Option<(u64, EdgeChange)> {
        // `__sprig_ge_<graph>` (JSON) or `__sprig_ge_<graph>_<type>` (typed).
        let name = r.table.table.strip_prefix("__sprig_ge_")?;
        let (graph, ty): (u64, Option<u32>) = match name.split_once('_') {
            Some((g, t)) => (g.parse().ok()?, Some(t.parse().ok()?)),
            None => (name.parse().ok()?, None),
        };
        if self.graphs.get(&graph)?.typed != ty.is_some() {
            return None;
        }
        if !r.table.schema.starts_with(&format!("t{}_", self.tenant)) {
            return None;
        }
        let (img, live) = match r.kind {
            RowKind::Insert => (r.after.as_ref()?, true),
            RowKind::Delete => (r.before.as_ref()?, false),
            // An edge's ends and type never change.
            RowKind::Update => return None,
        };
        let col = |name: &str| -> Option<u64> {
            let i = r.table.columns.iter().position(|c| c == name)?;
            match img.get(i)? {
                BinValue::UInt(u) => Some(*u),
                #[allow(clippy::cast_sign_loss)] // BIGINT UNSIGNED without metadata
                BinValue::Int(v) => Some(*v as u64),
                _ => None,
            }
        };
        Some((
            graph,
            EdgeChange {
                id: col("id")?,
                src: col("src")?,
                dst: col("dst")?,
                rtype: match ty {
                    Some(t) => t,
                    None => u32::try_from(col("type")?).ok()?,
                },
                src_rk: col("_sprig_rk")?,
                dst_rk: col("dst_rk")?,
                live,
            },
        ))
    }

    /// Applies `ch` to `graph`'s mirrors, again with a newer map when one
    /// of the shards changed epoch meanwhile.
    async fn apply(&mut self, graph: u64, ch: &[EdgeChange]) -> Result<()> {
        let mut conns = PoolConns {
            pools: Arc::clone(&self.env.pools),
            writers: std::mem::take(&mut self.writers),
            relay: (Arc::clone(&self.env.dc), self.tenant),
        };
        let r = self.apply_with(graph, ch, &mut conns).await;
        // A failure may come from a stale pool (a new writer): the loop has
        // the pools looked up again beside it, and these serve meanwhile.
        self.writers = conns.writers;
        r
    }

    async fn apply_with(
        &mut self,
        graph: u64,
        ch: &[EdgeChange],
        conns: &mut PoolConns,
    ) -> Result<()> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let Some(served) = self.graphs.get(&graph).cloned() else {
                return Ok(()); // no longer served (dropped, or back to sync)
            };
            let r = if served.typed {
                mirror::apply_batch_typed(conns, &served.layout, ch, now_us()).await
            } else {
                mirror::apply_batch(conns, &served.layout, ch, now_us()).await
            };
            let stale = match &r {
                Ok(a) => {
                    // The watched map now: a cutover seen meanwhile runs
                    // the batch again under the new map. No etcd read per
                    // batch (see `reload`).
                    if let Err(e) = self.reload(graph, false).await {
                        tracing::debug!(tenant = %self.tenant, graph, error = %e, "graph mirror map not checked after a batch");
                    }
                    let now = self.graphs.get(&graph).map(|s| Arc::clone(&s.layout));
                    now.is_some_and(|l| !mirror::still_current(&a.touched, &l))
                }
                Err(e) => {
                    let code = match &e.kind {
                        sprig_common::ErrorKind::Backend { errno, .. } => *errno,
                        _ => 0,
                    };
                    if attempt < 10 && matches!(code, 1146 | 1049 | 1205 | 1213 | 1047) {
                        self.reload(graph, true).await?;
                        tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
                        true
                    } else {
                        return Err(r.err().unwrap_or_else(|| SprigError::internal("apply")));
                    }
                }
            };
            if !stale {
                if let Ok(a) = &r {
                    self.applied += a.upserts + a.deletes;
                    metrics::counter!("sprig_graph_mirror_applied_total")
                        .increment(a.upserts + a.deletes);
                }
                return Ok(());
            }
            if attempt >= 10 {
                return Err(SprigError::internal(format!(
                    "graph {graph}: the shard map kept changing under the mirror applier"
                )));
            }
        }
    }

    /// Reloads one graph's shard map: the watched copy, or (`latest`) a
    /// linearizable read.
    ///
    /// After a batch that succeeded, the watched copy is enough. A batch
    /// that wrote under an old map can't lose its writes to a move: the
    /// mover fences a shard's source tables by renaming them, which waits
    /// for open transactions, and then copies the source's binlog up to
    /// the position read after the fence. So a mirror write either
    /// committed before the fence (and is copied) or fails with 1146 (and
    /// the batch runs again after a linearizable read). The check here
    /// only spares a known-stale batch that round trip. Reading the map
    /// from etcd after every batch made each write cost the applier an
    /// etcd round trip, and failed whole batches while etcd timed out.
    async fn reload(&mut self, graph: u64, latest: bool) -> Result<()> {
        let Some(s) = self.graphs.get(&graph).cloned() else {
            return Ok(());
        };
        let (map, linearizable) =
            map_of(&self.env, self.tenant, GroupId(s.layout.group), latest).await?;
        // A watched copy can trail a linearizable read taken earlier.
        if map.version == s.layout.version || (!linearizable && map.version < s.layout.version) {
            return Ok(());
        }
        let mut l = (*s.layout).clone();
        l.version = map.version;
        l.shards = shards_of(self.tenant, graph, l.db, l.group, &map);
        self.graphs.insert(
            graph,
            Served {
                layout: Arc::new(l),
                typed: s.typed,
            },
        );
        Ok(())
    }

    /// Deletes the position rows of dropped graphs. `DROP GRAPH` deletes
    /// them in its own transaction, but an applier that still served the
    /// graph (it looks every 2 s) wrote them again; once the graph is no
    /// longer served, nothing writes them.
    async fn clear_orphans(&mut self) {
        let r = async {
            let (pool, schema) = self.catalog().await?;
            let mut c = pool
                .get_conn()
                .await
                .map_err(sprig_graph::catalog::backend_error)?;
            mirror::clear_orphan_pos(&mut c, &schema).await
        }
        .await;
        match r {
            Ok(n) => {
                self.orphans_cleared = true;
                if n > 0 {
                    tracing::info!(tenant = %self.tenant, rows = n, "graph mirror positions of dropped graphs deleted");
                }
            }
            // Before the catalog's graph migration 3: nothing to delete.
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. }) => {
                self.orphans_cleared = true;
            }
            Err(e) => {
                tracing::debug!(tenant = %self.tenant, error = %e, "graph mirror positions of dropped graphs not deleted");
            }
        }
    }

    /// The tenant catalog's writer pool and schema, kept until the next
    /// refresh or failure.
    async fn catalog(&mut self) -> Result<(mysql_async::Pool, String)> {
        if let Some(c) = &self.catalog {
            return Ok(c.clone());
        }
        let ep = self.env.locator.writer(self.tenant).await?;
        let c = (ep.pool, ep.store.schema().to_owned());
        self.catalog = Some(c.clone());
        Ok(c)
    }

    /// A position row per shard of every served graph.
    async fn write_positions(&mut self) -> Result<()> {
        if self.graphs.is_empty() {
            return Ok(());
        }
        let (pool, schema) = self.catalog().await?;
        let mut c = pool
            .get_conn()
            .await
            .map_err(sprig_graph::catalog::backend_error)?;
        for (graph, s) in &self.graphs {
            let rows: Vec<(u32, String, u64, String)> = s
                .layout
                .shards
                .iter()
                .filter_map(|sh| {
                    let c = ClusterId::new(&sh.cluster).ok()?;
                    // A cluster without a reader yet has no position.
                    if !self.readers.contains_key(&c) {
                        return None;
                    }
                    let p = self.safe_pos(&c)?;
                    Some((sh.shard, sh.cluster.clone(), sh.epoch, p.to_string()))
                })
                .collect();
            if rows.len() == s.layout.shards.len() {
                mirror::write_pos(&mut c, &schema, *graph, &rows).await?;
            }
        }
        Ok(())
    }
}

/// How often the positions are recorded as the job's progress.
const CHECKPOINT_EVERY: Duration = Duration::from_secs(2);

/// The longest wait between checkpoint attempts after failures.
const CHECKPOINT_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How often a run of failed checkpoints is logged again.
const CHECKPOINT_WARN_EVERY: Duration = Duration::from_secs(30);

/// A checkpoint write in flight.
type ProgressWrite<'a> = Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;

/// The job's checkpoint: its positions written to the job record in the
/// DC's store every [`CHECKPOINT_EVERY`], where a new owner resumes.
///
/// - The write runs beside the loop, never in it: the loop polls it
///   between takes, so a slow or failing store doesn't stop the applying
///   (a write used to hold the loop up to the store's 5 s timeout).
/// - One write at a time, of the positions when it starts: positions
///   that move meanwhile go in the next one (coalesced).
/// - After a failure the next attempt waits twice as long, up to
///   [`CHECKPOINT_BACKOFF_MAX`]; a success goes back to every 2 s.
/// - Meanwhile the catalog's position rows, written every 50 ms to a
///   second, are the resume point: a new owner starts at the newer of
///   the checkpoint and those rows ([`Applier::resume_from_catalog`]).
///
/// Failures count in `sprig_graph_mirror_checkpoint_failures_total`; the
/// oldest checkpoint of the worker's appliers is
/// `sprig_graph_mirror_checkpoint_age_seconds`.
struct Checkpoint<'a> {
    cx: &'a JobCtx,
    tenant: TenantId,
    inflight: Option<ProgressWrite<'a>>,
    next_at: Instant,
    backoff: Duration,
    last_ok: Instant,
    failures: u32,
    last_warn: Option<Instant>,
    last_report: Option<Instant>,
}

/// The last checkpoint of each applier job of this process, by tenant.
fn checkpoint_ages() -> &'static Mutex<HashMap<u64, Instant>> {
    static C: OnceLock<Mutex<HashMap<u64, Instant>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Sets the age gauge to the oldest of `ages`.
fn set_age_gauge(ages: &HashMap<u64, Instant>) {
    let oldest = ages
        .values()
        .map(|t| t.elapsed().as_secs_f64())
        .fold(0.0, f64::max);
    metrics::gauge!("sprig_graph_mirror_checkpoint_age_seconds").set(oldest);
}

impl<'a> Checkpoint<'a> {
    fn new(cx: &'a JobCtx, tenant: TenantId) -> Self {
        let now = Instant::now();
        Self {
            cx,
            tenant,
            inflight: None,
            next_at: now + CHECKPOINT_EVERY,
            backoff: CHECKPOINT_EVERY,
            last_ok: now,
            failures: 0,
            last_warn: None,
            last_report: None,
        }
    }

    /// Whether a write is due: none in flight, and its time has come.
    fn due(&self) -> bool {
        self.inflight.is_none() && Instant::now() >= self.next_at
    }

    /// Starts writing `progress`.
    fn start(&mut self, progress: Value) {
        self.inflight = Some(Box::pin(self.cx.progress(progress)));
    }

    /// Looks at the write in flight without waiting for it; `false` when
    /// the job is no longer this worker's.
    async fn poll(&mut self) -> bool {
        let Some(w) = self.inflight.as_mut() else {
            return true;
        };
        let done = tokio::select! {
            biased;
            r = w => Some(r),
            () = std::future::ready(()) => None,
        };
        let Some(r) = done else {
            return true;
        };
        self.inflight = None;
        let now = Instant::now();
        match r {
            Ok(true) => {
                if self.failures > 0 {
                    tracing::info!(tenant = %self.tenant, failures = self.failures, "graph mirror checkpoint recorded again");
                }
                self.failures = 0;
                self.backoff = CHECKPOINT_EVERY;
                self.last_ok = now;
                self.last_warn = None;
                self.next_at = now + CHECKPOINT_EVERY;
                true
            }
            Ok(false) => false,
            Err(e) => {
                self.failures += 1;
                metrics::counter!("sprig_graph_mirror_checkpoint_failures_total").increment(1);
                self.backoff = self.backoff.saturating_mul(2).min(CHECKPOINT_BACKOFF_MAX);
                self.next_at = now + self.backoff;
                if self
                    .last_warn
                    .is_none_or(|t| t.elapsed() >= CHECKPOINT_WARN_EVERY)
                {
                    tracing::warn!(
                        tenant = %self.tenant,
                        error = %e,
                        failures = self.failures,
                        age_s = self.last_ok.elapsed().as_secs(),
                        retry_ms = u64::try_from(self.backoff.as_millis()).unwrap_or(u64::MAX),
                        "graph mirror checkpoint not recorded; a new owner resumes at the catalog's positions meanwhile"
                    );
                    self.last_warn = Some(now);
                }
                true
            }
        }
    }

    /// Updates the age gauge, at most once a second.
    fn report_age(&mut self) {
        if self
            .last_report
            .is_some_and(|t| t.elapsed() < Duration::from_secs(1))
        {
            return;
        }
        self.last_report = Some(Instant::now());
        let mut ages = checkpoint_ages()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        ages.insert(self.tenant.0, self.last_ok);
        set_age_gauge(&ages);
    }
}

impl Drop for Checkpoint<'_> {
    fn drop(&mut self) {
        let mut ages = checkpoint_ages()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        ages.remove(&self.tenant.0);
        set_age_gauge(&ages);
    }
}

/// One cluster's reader: its items into `tx` until cancelled or broken.
async fn read(
    clusters: Arc<dyn Clusters>,
    tenant: TenantId,
    cluster: ClusterId,
    gen_: u64,
    from: GtidPos,
    tx: mpsc::Sender<Item>,
    stop: CancellationToken,
) {
    let mut src: Box<dyn TxnSource> = match clusters.open(&cluster, tenant, &from).await {
        Ok(s) => s,
        Err(e) => {
            stop.cancel();
            let _ = tx.send((cluster, gen_, Err(e.to_string()))).await;
            return;
        }
    };
    loop {
        let it = tokio::select! {
            () = stop.cancelled() => return,
            it = src.next() => it,
        };
        let end = it.is_err();
        let item = (cluster.clone(), gen_, it.map_err(|e| e.to_string()));
        tokio::select! {
            () = stop.cancelled() => return,
            r = tx.send(item) => if r.is_err() { return; },
        }
        if end {
            stop.cancel();
            return;
        }
    }
}
