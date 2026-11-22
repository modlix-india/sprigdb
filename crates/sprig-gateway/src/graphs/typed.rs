//! Typed graphs in the gateway (`graphs.storage = 2`, `sprig_graph::typed`):
//! which store a statement runs on, and the schema changes a typed write
//! needs.
//!
//! - **Schema.** A statement starts from the snapshot's typed schema
//!   merged with what this process saw change since (the schema only
//!   grows). A write that needs a new label set, relationship type, key
//!   or column has it made outside its transaction ([`GwTypedDdl`]): the
//!   tables or columns on every shard first, then the catalog rows, then
//!   a catalog version, so other gateways load them.
//! - **Mirrors.** As for JSON graphs: under `graph.mirror = async` the
//!   worker's applier keeps a typed graph's per-type mirrors, and reads
//!   use them once the gate finds them fresh.

use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::CatalogLocator;
use sprig_catalog::version::VersionKind;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_graph::ast::{Entity, Schema};
use sprig_graph::catalog::GraphCatalog;
use sprig_graph::catalog::backend_error;
use sprig_graph::maria::{CounterIds, Dictionary, HoldWait, MariaStore, SchemaOps, ShardConns};
use sprig_graph::mirror::MirrorGate;
use sprig_graph::schema::{plan_create, plan_drop};
use sprig_graph::store::SchemaCounts;
use sprig_graph::store::{GraphStore, IndexDef};
use sprig_graph::typed::Owner;
use sprig_graph::typed::catalog::{DdlConns, ShardSchemas, SqlDdl, TypedDdl, Want};
use sprig_graph::typed::store::TypedStore;
use sprig_graph::typed::{STORAGE_TYPED, TypedSchema};

use super::{GatewayGraphs, GraphRef};

/// What a statement's store is made of, whatever the graph's storage.
pub(crate) struct StoreParts {
    pub(crate) labels: BTreeMap<u32, String>,
    pub(crate) types: BTreeMap<u32, String>,
    pub(crate) dict: Arc<dyn Dictionary>,
    pub(crate) ids: Arc<CounterIds>,
    pub(crate) indexes: Vec<IndexDef>,
    pub(crate) ops: Option<Arc<dyn SchemaOps>>,
    /// The asynchronous mirrors' freshness gate.
    pub(crate) gate: Option<Arc<dyn MirrorGate>>,
    /// Waits for a mover's hold (JSON storage only).
    pub(crate) holds: Option<Arc<dyn HoldWait>>,
}

/// Whether `g` keeps its elements in typed tables.
#[must_use]
pub(crate) fn is_typed(g: &GraphRef) -> bool {
    g.def.storage == STORAGE_TYPED
}

impl GatewayGraphs {
    /// `g`'s typed schema now: the snapshot's and what this process saw
    /// added since.
    #[must_use]
    pub(crate) fn typed_schema(&self, g: &GraphRef) -> TypedSchema {
        let mut s = g.def.typed.as_deref().cloned().unwrap_or_default();
        if let Some(l) = self
            .typed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(g.tenant.0, g.graph_id))
        {
            s.merge(l);
        }
        s
    }

    pub(crate) fn learn_typed(&self, tenant: TenantId, graph: u64, s: &TypedSchema) {
        let mut m = self.typed.lock().unwrap_or_else(PoisonError::into_inner);
        let e = m
            .entry((tenant.0, graph))
            .or_insert_with(|| Arc::new(TypedSchema::default()));
        let mut now = (**e).clone();
        now.merge(s);
        *e = Arc::new(now);
    }

    /// Who makes the tables and columns `g`'s typed writes need.
    #[must_use]
    pub(crate) fn typed_ddl(self: &Arc<Self>, g: &GraphRef) -> Arc<dyn TypedDdl> {
        Arc::new(GwTypedDdl {
            graphs: Arc::clone(self),
            tenant: g.tenant,
            graph: g.graph_id,
            shards: g
                .shards
                .shards
                .iter()
                .map(|s| (s.cluster.clone(), s.schema.clone()))
                .collect(),
        })
    }

    /// The store a statement on `g` runs on, over `conns`.
    #[must_use]
    pub(crate) fn open_store<'c>(
        self: &Arc<Self>,
        g: &GraphRef,
        conns: &'c mut dyn ShardConns,
        p: StoreParts,
    ) -> Box<dyn GraphStore + 'c> {
        if is_typed(g) {
            Box::new(
                TypedStore::new(
                    conns,
                    Arc::clone(&g.shards),
                    self.typed_schema(g),
                    self.typed_ddl(g),
                    p.labels,
                    p.types,
                    p.dict,
                    p.ids,
                )
                .with_schema(p.indexes, p.ops)
                .with_gate(p.gate),
            )
        } else {
            let shadow = self.shadow(g, Arc::clone(&p.ids));
            Box::new(
                MariaStore::new(
                    conns,
                    Arc::clone(&g.shards),
                    p.labels,
                    p.types,
                    p.dict,
                    p.ids,
                )
                .with_shadow(shadow)
                .with_schema(p.indexes, p.ops)
                .with_gate(p.gate)
                .with_holds(p.holds),
            )
        }
    }

    /// Fails with 39906 if a node `deleted` removed still has
    /// relationships, in the writing transaction (`conns`) before it
    /// commits.
    ///
    /// # Errors
    /// 39906; backend errors.
    pub(crate) async fn check_deleted(
        &self,
        g: &GraphRef,
        conns: &mut dyn ShardConns,
        deleted: &[u64],
    ) -> Result<()> {
        if deleted.is_empty() {
            return Ok(());
        }
        if is_typed(g) {
            let s = self.typed_schema(g);
            sprig_graph::typed::store::check_deleted(conns, &g.shards, &s, deleted).await
        } else {
            sprig_graph::maria::check_deleted(conns, &g.shards, deleted).await
        }
    }
}

impl GatewayGraphs {
    /// `CREATE INDEX` / `DROP INDEX` on a typed graph: the catalog row,
    /// then an index per column of the property in every table it covers
    /// on every shard (new tables and columns get it as they're made).
    /// A uniqueness constraint also claims every existing key (the
    /// claims table, as JSON graphs keep it) before it goes online.
    ///
    /// # Errors
    /// `GraphUnsupported` for a constraint; catalog and backend errors.
    #[allow(clippy::too_many_lines)] // the catalog row, the DDL on every shard, online or undone
    pub(crate) async fn typed_index_cmd(
        self: &Arc<Self>,
        g: &GraphRef,
        cmd: &Schema,
    ) -> Result<SchemaCounts> {
        self.prepare(g.tenant).await?;
        let existing = self.current_indexes(g).await?;
        let schema = self.typed_schema(g);
        let owners: Vec<Owner> = schema
            .classes
            .keys()
            .map(|c| (Entity::Node, *c))
            .chain(schema.types.iter().map(|t| (Entity::Relationship, *t)))
            .collect();
        if let Schema::Drop { .. } = cmd {
            let Some(d) = plan_drop(&existing, cmd)? else {
                return Ok(vec![]);
            };
            let mut ch = self
                .svc
                .catalog
                .change(g.tenant, VersionKind::Catalog)
                .await?;
            let r = match ch.tx().records().await {
                Err(e) => Err(e),
                Ok(rec) => GraphCatalog::delete_index(rec, g.graph_id, d.id).await,
            };
            if let Err(e) = r {
                let _ = ch.rollback().await;
                return Err(e);
            }
            ch.commit().await?;
            for o in owners.iter().filter(|o| o.0 == d.entity) {
                self.on_typed_shards(g, |s| {
                    sprig_graph::typed::drop_index_ddl(s, g.graph_id, *o, d.id)
                })
                .await?;
            }
            if d.unique {
                self.on_typed_shards(g, |s| {
                    format!(
                        "DELETE FROM {} WHERE idx = {}",
                        sprig_graph::catalog::Tables::of(s, g.graph_id).claims,
                        d.id
                    )
                })
                .await?;
            }
            return Ok(vec![(
                if d.unique {
                    "constraints-removed"
                } else {
                    "indexes-removed"
                },
                1,
            )]);
        }
        let Some(mut d) = plan_create(&existing, cmd)? else {
            return Ok(vec![]);
        };
        d.token_id = self
            .dictionary(g)
            .create(d.entity == Entity::Node, &d.token)
            .await?;
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = match ch.tx().records().await {
            Err(e) => Err(e),
            Ok(rec) => GraphCatalog::insert_index(rec, g.graph_id, &d).await,
        };
        d.id = match r {
            Ok(id) => id,
            Err(e) => {
                let _ = ch.rollback().await;
                return Err(e);
            }
        };
        ch.commit().await?;
        let mut built = Ok(());
        for o in &owners {
            let clauses = sprig_graph::typed::index_clauses(&schema, *o, &d);
            if clauses.is_empty() {
                continue;
            }
            built = self
                .on_typed_shards(g, |s| {
                    sprig_graph::typed::add_index_ddl(s, g.graph_id, *o, &clauses, false)
                })
                .await;
            if built.is_err() {
                break;
            }
        }
        if built.is_ok() && d.unique {
            // Writers claim from the catalog's `building` row on; the
            // existing keys are claimed now.
            let mut dd = d.clone();
            dd.state = "building".into();
            built = self.typed_claim_all(g, &dd).await.map(|_| ());
        }
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = match ch.tx().records().await {
            Err(e) => Err(e),
            Ok(rec) => {
                if built.is_ok() {
                    GraphCatalog::index_online(rec, g.graph_id, d.id).await
                } else {
                    GraphCatalog::delete_index(rec, g.graph_id, d.id).await
                }
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        ch.commit().await?;
        if let Err(e) = built {
            if d.unique {
                let _ = self
                    .on_typed_shards(g, |s| {
                        format!(
                            "DELETE FROM {} WHERE idx = {}",
                            sprig_graph::catalog::Tables::of(s, g.graph_id).claims,
                            d.id
                        )
                    })
                    .await;
            }
            return Err(match e.kind {
                sprig_common::ErrorKind::Backend { errno: 1062, .. } => super::neo_schema(
                    "ConstraintCreationFailed",
                    format!(
                        "Unable to create constraint {}: existing data has duplicate values of {}",
                        d.name,
                        sprig_graph::schema::describe(d.entity, &d.token, &d.props)
                    ),
                ),
                _ => e,
            });
        }
        tracing::info!(tenant = g.tenant.0, graph = g.graph_id, index = %d.name, "typed graph index created");
        Ok(vec![(
            if d.unique {
                "constraints-added"
            } else {
                "indexes-added"
            },
            1,
        )])
    }

    /// Claims the existing keys of constraint `d` on typed graph `g`,
    /// each claim committed on its own (as the JSON graphs' backfill).
    async fn typed_claim_all(self: &Arc<Self>, g: &GraphRef, d: &IndexDef) -> Result<u64> {
        let mut conns = CtlConns {
            graphs: Arc::clone(self),
            tenant: g.tenant,
            conns: std::collections::HashMap::new(),
        };
        let (labels, types) = self.names_of(g);
        let mut store = TypedStore::new(
            &mut conns,
            Arc::clone(&g.shards),
            self.typed_schema(g),
            self.typed_ddl(g),
            labels,
            types,
            self.dictionary(g),
            self.counters(g).await?,
        )
        .with_schema(vec![d.clone()], None);
        store.claim_all(d).await
    }

    /// Runs `sql(schema)` on every shard schema of `g`, each on its
    /// cluster; stops at the first error.
    async fn on_typed_shards(&self, g: &GraphRef, sql: impl Fn(&str) -> String) -> Result<()> {
        for sh in &g.shards.shards {
            let id = ClusterId::new(&sh.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", sh.cluster)))?;
            let mut c = self
                .tenant_pool(g.tenant, &id)
                .await?
                .get_conn()
                .await
                .map_err(backend_error)?;
            c.query_drop(sql(&sh.schema)).await.map_err(backend_error)?;
        }
        Ok(())
    }
}

/// Autocommit `sprig_ctl` connections, one per cluster (a constraint's
/// backfill).
struct CtlConns {
    graphs: Arc<GatewayGraphs>,
    tenant: TenantId,
    conns: std::collections::HashMap<String, mysql_async::Conn>,
}

#[async_trait]
impl ShardConns for CtlConns {
    async fn conn(
        &mut self,
        at: &sprig_graph::maria::ShardLoc,
        _write: bool,
    ) -> Result<&mut mysql_async::Conn> {
        if !self.conns.contains_key(&at.cluster) {
            let id = ClusterId::new(&at.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", at.cluster)))?;
            let c = self
                .graphs
                .tenant_pool(self.tenant, &id)
                .await?
                .get_conn()
                .await
                .map_err(backend_error)?;
            self.conns.insert(at.cluster.clone(), c);
        }
        self.conns
            .get_mut(&at.cluster)
            .ok_or_else(|| SprigError::internal("backfill connection"))
    }
}

/// [`TypedDdl`] for one graph, through the gateway's connections.
struct GwTypedDdl {
    graphs: Arc<GatewayGraphs>,
    tenant: TenantId,
    graph: u64,
    shards: ShardSchemas,
}

impl GwTypedDdl {
    fn sql(&self, catalog: String, pool: mysql_async::Pool) -> SqlDdl {
        SqlDdl {
            cat: catalog,
            graph: self.graph,
            shards: self.shards.clone(),
            conns: Arc::new(GwDdlConns {
                graphs: Arc::clone(&self.graphs),
                tenant: self.tenant,
                catalog: pool,
            }),
        }
    }
}

/// How long a deferred column waits before it's added (writes of the same
/// load gather meanwhile).
const PROMOTE_AFTER: std::time::Duration = std::time::Duration::from_millis(300);

impl GatewayGraphs {
    /// Adds deferred columns of `owner` and moves their values out of the
    /// values table, one task per table at a time (see [`TypedDdl::defer`]).
    fn promote_later(
        self: &Arc<Self>,
        tenant: TenantId,
        graph: u64,
        shards: ShardSchemas,
        owner: Owner,
        cols: std::collections::BTreeSet<(String, sprig_graph::typed::Code)>,
    ) {
        let key = (tenant.0, graph, owner);
        {
            let mut m = self
                .promoting
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(pending) = m.get_mut(&key) {
                pending.extend(cols);
                return;
            }
            m.insert(key, cols);
        }
        let me = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(PROMOTE_AFTER).await;
                let cols = {
                    let mut m = me.promoting.lock().unwrap_or_else(PoisonError::into_inner);
                    match m.get_mut(&key) {
                        Some(c) if !c.is_empty() => std::mem::take(c),
                        _ => {
                            m.remove(&key);
                            return;
                        }
                    }
                };
                if let Err(e) = me.promote(tenant, graph, &shards, owner, &cols).await {
                    tracing::warn!(tenant = %tenant, graph, error = %e,
                        "typed graph columns not added after the commit; their values stay in the values table");
                }
            }
        });
    }

    async fn promote(
        self: &Arc<Self>,
        tenant: TenantId,
        graph: u64,
        shards: &ShardSchemas,
        owner: Owner,
        cols: &std::collections::BTreeSet<(String, sprig_graph::typed::Code)>,
    ) -> Result<()> {
        let ep = self.svc.locator.writer(tenant).await?;
        let ddl = GwTypedDdl {
            graphs: Arc::clone(self),
            tenant,
            graph,
            shards: shards.clone(),
        }
        .sql(ep.store.schema().to_owned(), ep.pool.clone());
        let mut tries = 0;
        let s = loop {
            match ddl.add_columns_wait(owner, cols).await {
                Ok(s) => break s,
                Err(e)
                    if tries < 5
                        && matches!(
                            e.kind,
                            sprig_common::ErrorKind::Backend {
                                errno: 1205 | 1213,
                                ..
                            }
                        ) =>
                {
                    tries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500 * tries)).await;
                }
                Err(e) => return Err(e),
            }
        };
        self.learn_typed(tenant, graph, &s);
        let have = s.columns.get(&owner).cloned().unwrap_or_default();
        let moved: Vec<(u32, sprig_graph::typed::Code)> = cols
            .iter()
            .filter_map(|(k, code)| s.keys.get(k).map(|id| (*id, *code)))
            .filter(|x| have.contains(x))
            .collect();
        for (cluster, schema) in shards {
            let mut c = self.cluster_conn(tenant, cluster).await?;
            for (k, code) in &moved {
                c.query_drop(sprig_graph::typed::promote_sql(
                    schema, graph, owner, *k, *code,
                ))
                .await
                .map_err(backend_error)?;
            }
        }
        // Readers that don't know the columns yet read the values table:
        // its copies go once every gateway has the schema.
        let v = self
            .catalog(tenant, sprig_catalog::Freshness::Latest)
            .await?
            .version;
        crate::tenant::lookups_sql::publish_wait(&self.svc, tenant, v).await;
        for (cluster, schema) in shards {
            let mut c = self.cluster_conn(tenant, cluster).await?;
            for (k, code) in &moved {
                for sql in sprig_graph::typed::cleanup_sql(schema, graph, owner, *k, *code) {
                    c.query_drop(sql).await.map_err(backend_error)?;
                }
            }
        }
        tracing::info!(tenant = %tenant, graph, columns = moved.len(), "typed graph columns added after the commit");
        Ok(())
    }

    async fn cluster_conn(&self, tenant: TenantId, cluster: &str) -> Result<mysql_async::Conn> {
        let id = ClusterId::new(cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
        self.tenant_pool(tenant, &id)
            .await?
            .get_conn()
            .await
            .map_err(backend_error)
    }
}

#[async_trait]
impl TypedDdl for GwTypedDdl {
    fn defer(
        &self,
        owner: Owner,
        cols: std::collections::BTreeSet<(String, sprig_graph::typed::Code)>,
    ) {
        self.graphs
            .promote_later(self.tenant, self.graph, self.shards.clone(), owner, cols);
    }

    async fn ensure(&self, want: &Want) -> Result<TypedSchema> {
        let ep = self.graphs.svc.locator.writer(self.tenant).await?;
        let ddl = SqlDdl {
            cat: ep.store.schema().to_owned(),
            graph: self.graph,
            shards: self.shards.clone(),
            conns: Arc::new(GwDdlConns {
                graphs: Arc::clone(&self.graphs),
                tenant: self.tenant,
                catalog: ep.pool.clone(),
            }),
        };
        let s = ddl.ensure(want).await?;
        self.graphs.learn_typed(self.tenant, self.graph, &s);
        Ok(s)
    }
}

struct GwDdlConns {
    graphs: Arc<GatewayGraphs>,
    tenant: TenantId,
    catalog: mysql_async::Pool,
}

#[async_trait]
impl DdlConns for GwDdlConns {
    async fn catalog(&self) -> Result<mysql_async::Conn> {
        self.catalog.get_conn().await.map_err(backend_error)
    }

    async fn cluster(&self, cluster: &str) -> Result<mysql_async::Conn> {
        let id = ClusterId::new(cluster)
            .map_err(|e| SprigError::internal(format!("cluster id {cluster}: {e}")))?;
        let pool = self.graphs.tenant_pool(self.tenant, &id).await?;
        pool.get_conn().await.map_err(backend_error)
    }

    async fn indexes(&self, graph: u64) -> Result<Vec<IndexDef>> {
        let mut rec = self.graphs.svc.records.begin(self.tenant).await?;
        Ok(GraphCatalog::indexes(&mut *rec, Some(graph))
            .await?
            .into_iter()
            .map(|(_, d)| d)
            .collect())
    }

    async fn changed(&self) -> Result<()> {
        // A change with no rows of its own: the version alone tells other
        // gateways to load the typed schema again.
        let ch = self
            .graphs
            .svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?;
        ch.commit().await?;
        Ok(())
    }
}
