//! Graphs in the gateway: finding, creating and dropping graphs through
//! the tenant catalog, and the privilege checks. Used by the Bolt front
//! end ([`crate::bolt`]) and by `CREATE GRAPH` / `DROP GRAPH` and
//! `GRAPH_TABLE` in SQL ([`crate::exec`]).
//!
//! - **Catalog.** A graph is an `objects` row (kind `graph`, namespace
//!   `graph`) and a graph record, written in one versioned change.
//! - **Storage.** A native graph's nodes, relationships, names, ids and
//!   indexes live in the node's storage engine ([`engine`]). A mapped graph
//!   (`CREATE PROPERTY GRAPH`) reads its database's tables in process.
//! - **Privileges.** A graph is an object with the table actions: reading
//!   needs `SELECT`, `CREATE`/`MERGE` need `INSERT`, `SET`/`REMOVE`
//!   `UPDATE`, `DELETE` `DELETE` on the graph or its database;
//!   `CREATE GRAPH` needs `CREATE`, `DROP GRAPH` `DROP`. Without `SELECT`
//!   on the graph, reads may rest on grants of single labels and types
//!   (`GRANT READ ON GRAPH db.g NODES L`): the statement
//!   then sees only those, through `sprig_graph::access`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::SystemTime;

use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::{DdlTx, database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::catalog::{GraphCatalog, GraphDef, GraphState, MODULE};
use sprig_meta::{Tier, TierView};
use sprig_shard::ShardMapCache;

use crate::tenant::TenantServices;

pub(crate) mod engine;

/// One resolved graph.
#[derive(Debug, Clone)]
pub struct GraphRef {
    /// The tenant.
    pub tenant: TenantId,
    /// The logical database.
    pub db_id: DbId,
    /// Its name.
    pub db: String,
    /// The graph's name.
    pub name: String,
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The database's schema name (a mapped graph's SQL names its tables
    /// with it).
    pub schema: String,
    /// Labels and types at resolution.
    pub def: Arc<GraphDef>,
}

/// The gateway's graph services.
pub struct GatewayGraphs {
    pub(crate) svc: Arc<TenantServices>,
    /// Bookmark keys by tenant (the tenant's `graph_keys` secret).
    keys: Mutex<HashMap<u64, Arc<ring::hmac::Key>>>,
}

impl std::fmt::Debug for GatewayGraphs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayGraphs").finish_non_exhaustive()
    }
}

fn not_found(db: &str, name: &str) -> SprigError {
    SprigError::new(
        SprigCode::GraphNotFound,
        format!("Graph not found: {db}.{name}"),
    )
}

fn state_of(cat: &TenantCatalog) -> GraphState {
    cat.ext
        .get::<GraphState>(MODULE)
        .cloned()
        .unwrap_or_default()
}

impl GatewayGraphs {
    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            keys: Mutex::new(HashMap::new()),
        }
    }

    /// The process's graph services over `svc`: one per tenant services,
    /// shared by Bolt and SQL (`GRAPH_TABLE`, graph DDL), so their caches
    /// are made once.
    #[must_use]
    pub fn shared(svc: &Arc<TenantServices>) -> Arc<Self> {
        static ALL: Mutex<Vec<(usize, Arc<GatewayGraphs>)>> = Mutex::new(Vec::new());
        // The entry holds `svc`, so its address isn't reused.
        let key = Arc::as_ptr(svc) as usize;
        let mut all = ALL.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, g)) = all.iter().find(|(k, _)| *k == key) {
            return Arc::clone(g);
        }
        let g = Arc::new(Self::new(Arc::clone(svc)));
        all.push((key, Arc::clone(&g)));
        g
    }

    /// The tenant's bookmark key: its catalog's `graph_keys` secret, the
    /// same on every gateway.
    ///
    /// # Errors
    /// The locator's; a backend error.
    pub async fn bookmark_key(&self, tenant: TenantId) -> Result<Arc<ring::hmac::Key>> {
        if let Some(k) = self
            .keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant.0)
        {
            return Ok(Arc::clone(k));
        }
        let secret = GraphCatalog::bookmark_secret(&*self.svc.records, tenant).await?;
        let k = Arc::new(sprig_graph::bookmark::key(&secret));
        self.keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant.0, Arc::clone(&k));
        Ok(k)
    }

    /// The tenant's catalog snapshot.
    ///
    /// # Errors
    /// The cache's.
    pub async fn catalog(&self, tenant: TenantId, fresh: Freshness) -> Result<Arc<TenantCatalog>> {
        self.svc.cache.get(tenant, fresh).await
    }

    /// Whether the tenant's names are case-insensitive.
    #[must_use]
    pub fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// The grants of `user` in `cat` (`None`: an internal principal).
    #[must_use]
    pub fn grants(
        &self,
        cat: &TenantCatalog,
        user: Option<UserId>,
    ) -> Option<Arc<dyn SessionGrants>> {
        user.map(|u| crate::tenant::authz::grants_of(&self.svc, cat, Some(u)))
    }

    /// Whether `g` allows `a` on graph `object` (or its database).
    #[must_use]
    pub fn allows(
        &self,
        g: Option<&dyn SessionGrants>,
        cat: &TenantCatalog,
        db: &str,
        db_id: DbId,
        object: Option<u64>,
        a: Action,
    ) -> bool {
        let Some(g) = g else { return true };
        let db_key = database_key(db, self.lctn(cat));
        // A mapped graph over a table whose policies bind the
        // session would read it unfiltered: refused.
        if a == Action::Select
            && object.is_some_and(|o| {
                g.row_filter(
                    Target::Object {
                        db_key: &db_key,
                        db: db_id,
                        object: ObjectId(o),
                    },
                    a,
                )
                .is_some()
            })
        {
            return false;
        }
        if g.unrestricted() || g.allows(a, Target::Tenant) {
            return true;
        }
        if g.allows(a, Target::Database { db_key: &db_key }) {
            return true;
        }
        object.is_some_and(|o| {
            g.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db: db_id,
                    object: ObjectId(o),
                },
            )
        })
    }

    /// The labels and types `g`'s grants on single elements of graph
    /// `graph` let it write with `a` (`INSERT` = `CREATE`, `DELETE`,
    /// `UPDATE` = `SET PROPERTY`): `None` when they name none.
    #[must_use]
    pub fn writable(
        g: &dyn SessionGrants,
        graph: u64,
        a: Action,
    ) -> Option<(HashSet<String>, HashSet<String>)> {
        let (mut labels, mut types) = (HashSet::new(), HashSet::new());
        for (rels, name, set) in g.graph_elements(ObjectId(graph)) {
            if !set.has(a) {
                continue;
            }
            if rels {
                types.insert(name);
            } else {
                labels.insert(name);
            }
        }
        (!labels.is_empty() || !types.is_empty()).then_some((labels, types))
    }

    /// What `g`'s grants on single labels and relationship types of graph
    /// `graph` let it read; `None` when they give nothing.
    #[must_use]
    pub fn visible(g: &dyn SessionGrants, graph: u64) -> Option<sprig_graph::access::Visible> {
        let mut v = sprig_graph::access::Visible::default();
        for (rels, name, a) in g.graph_elements(ObjectId(graph)) {
            if !a.has(Action::Select) {
                continue;
            }
            if rels {
                v.types.insert(name);
            } else {
                v.labels.insert(name);
            }
        }
        (!v.labels.is_empty() || !v.types.is_empty()).then_some(v)
    }

    /// Resolves `db.name` in `cat`: `None` if there is no such graph.
    ///
    /// # Errors
    /// 39902 for an unknown database.
    pub fn resolve(&self, cat: &TenantCatalog, db: &str, name: &str) -> Result<Option<GraphRef>> {
        let lctn = self.lctn(cat);
        let Some(row) = cat.database(&database_key(db, lctn)) else {
            return Err(SprigError::new(
                SprigCode::GraphNotFound,
                format!(
                    "Database '{db}' does not exist: create it in SQL first (graphs live in a logical database)"
                ),
            ));
        };
        let Some(o) = cat
            .object(
                row.db_id,
                Namespace::Graph,
                &object_key(ObjectKind::Graph, name, lctn),
            )
            .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Graph)
        else {
            return Ok(None);
        };
        let Some(def) = state_of(cat).graphs.get(&o.object_id.0).cloned() else {
            return Ok(None);
        };
        let schema = sprig_catalog::ddl::shard_schemas(cat.tenant, row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| not_found(db, name))?;
        Ok(Some(GraphRef {
            tenant: cat.tenant,
            db_id: row.db_id,
            db: row.name.clone(),
            name: o.name.clone(),
            graph_id: o.object_id.0,
            schema,
            def,
        }))
    }

    /// `CREATE GRAPH`: the catalog rows (object, graph) in one change; the
    /// graph's elements live in the storage engine ([`engine`]). Returns
    /// the graph and the catalog version that has it.
    ///
    /// # Errors
    /// 39902 for an unknown database; 39908 for a name in use (unless
    /// `if_not_exists`); catalog errors.
    pub async fn create(
        &self,
        tenant: TenantId,
        db: &str,
        name: &str,
        if_not_exists: bool,
    ) -> Result<(GraphRef, u64)> {
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        if let Some(g) = self.resolve(&cat, db, name)? {
            if if_not_exists {
                return Ok((g, cat.version));
            }
            return Err(SprigError::new(
                SprigCode::GraphDefinitionInvalid,
                format!("Graph '{db}.{name}' already exists"),
            ));
        }
        let lctn = self.lctn(&cat);
        let row = cat
            .database(&database_key(db, lctn))
            .cloned()
            .ok_or_else(|| not_found(db, name))?;
        let ids = self.svc.catalog.backend();
        let object_id = ids.next_object_id(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = async {
            let tx = ch.tx();
            let obj = ObjectRow {
                object_id,
                db_id: row.db_id,
                namespace: Namespace::Graph,
                name: name.to_owned(),
                name_key: object_key(ObjectKind::Graph, name, lctn),
                kind: ObjectKind::Graph,
                parent_id: None,
                state: ObjectState::Active,
                schema_version: 1,
                flags: ObjectFlags::default(),
                pending_job_id: None,
                created_at: SystemTime::now(),
                changed_version: version,
            };
            DdlTx::insert_object(tx, &obj).await?;
            GraphCatalog::insert_native_as(tx.records().await?, object_id.0, row.db_id.0, None, 1)
                .await?;
            Ok::<(), SprigError>(())
        }
        .await;
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        let v = ch.commit().await?;
        let cat = self
            .catalog(tenant, Freshness::Cached { min_version: v })
            .await?;
        let g = self
            .resolve(&cat, db, name)?
            .ok_or_else(|| SprigError::internal("graph missing after create"))?;
        tracing::info!(
            tenant = tenant.0,
            db,
            graph = name,
            id = object_id.0,
            "graph created"
        );
        Ok((g, v))
    }

    /// `DROP GRAPH`: the catalog rows in one change, then the graph's
    /// storage in the engine.
    /// Returns the catalog version (0 when nothing was dropped).
    ///
    /// # Errors
    /// 39902 for an unknown graph (unless `if_exists`).
    pub async fn drop(
        &self,
        tenant: TenantId,
        db: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<u64> {
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        let found = match self.resolve(&cat, db, name) {
            Err(e) if if_exists && e.code() == Some(SprigCode::GraphNotFound) => None,
            r => r?,
        };
        let Some(g) = found else {
            if if_exists {
                return Ok(0);
            }
            return Err(not_found(db, name));
        };
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let native = engine::on_engine(&g);
        let r = async {
            let tx = ch.tx();
            tx.set_object_state(ObjectId(g.graph_id), ObjectState::Dropped, version)
                .await?;
            GraphCatalog::delete(tx.records().await?, g.graph_id).await?;
            Ok::<(), SprigError>(())
        }
        .await;
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        let v = ch.commit().await?;
        if native {
            engine::drop_storage(&g).await;
        }
        tracing::info!(tenant = tenant.0, db, graph = name, "graph dropped");
        Ok(v)
    }

    /// The table objects a mapped graph reads, by name: `(table, object)`.
    ///
    /// # Errors
    /// 39908 for a table that doesn't exist.
    pub fn mapped_sources(
        &self,
        cat: &TenantCatalog,
        db: DbId,
        def: &sprig_graph::mapped::MappedDef,
    ) -> Result<Vec<(String, u64)>> {
        let lctn = self.lctn(cat);
        let mut out: Vec<(String, u64)> = Vec::new();
        let tables = def
            .vertices
            .iter()
            .map(|v| &v.table)
            .chain(def.edges.iter().map(|e| &e.table));
        for t in tables {
            if out.iter().any(|(x, _)| x == t) {
                continue;
            }
            let key = object_key(ObjectKind::Table, t, lctn);
            let o = cat
                .object(db, Namespace::Relation, &key)
                .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Table)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphDefinitionInvalid,
                        format!("Table '{t}' doesn't exist"),
                    )
                })?;
            out.push((t.clone(), o.object_id.0));
        }
        Ok(out)
    }

    /// `CREATE PROPERTY GRAPH`: checks the tables and their columns (every
    /// column of a table without a property list; each expression runs once
    /// with `LIMIT 0`), then writes the `objects` and `graphs` rows (kind
    /// `mapped`, the definition as JSON) in one change. `can_read(object)`
    /// is the `SELECT` privilege check on each table. Returns the catalog
    /// version (that of the snapshot when `IF NOT EXISTS` found the graph).
    ///
    /// # Errors
    /// 39908 for a name in use, a missing table or column or a bad
    /// expression; 1142 for a table the user can't read.
    #[allow(clippy::too_many_lines)] // tables, columns, probe, catalog
    pub async fn create_mapped(
        &self,
        tenant: TenantId,
        db: &str,
        c: &sprig_graph::mapped::CreatePropertyGraph,
        can_read: &(dyn Fn(u64) -> bool + Send + Sync),
    ) -> Result<u64> {
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        if let Some(g) = self.resolve(&cat, db, &c.name)? {
            if c.if_not_exists {
                return Ok(cat.version);
            }
            if !(c.or_replace && g.def.kind == "mapped") {
                return Err(SprigError::new(
                    SprigCode::GraphDefinitionInvalid,
                    format!("Graph '{db}.{}' already exists", c.name),
                ));
            }
            self.drop(tenant, db, &c.name, true).await?;
        }
        let cat = self.catalog(tenant, Freshness::Latest).await?;
        let lctn = self.lctn(&cat);
        let row = cat
            .database(&database_key(db, lctn))
            .cloned()
            .ok_or_else(|| not_found(db, &c.name))?;
        let schema = sprig_catalog::ddl::shard_schemas(tenant, &row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| not_found(db, &c.name))?;
        let sources = self.mapped_sources(&cat, row.db_id, &c.def)?;
        for (t, o) in &sources {
            inheritable_source(&cat, t, *o)?;
            if !can_read(*o) {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "SELECT command denied for table '{t}' (a source of graph '{}')",
                        c.name
                    ),
                ));
            }
        }
        // The tables' columns from the engine's definitions, and the probe
        // below run in process.
        let mut engine_sql = crate::engine::graphsql::EngineSql::new(tenant, &cat, lctn, false);
        let mut def = c.def.clone();
        let invalid = |m: String| SprigError::new(SprigCode::GraphDefinitionInvalid, m);
        let mut columns_of = HashMap::new();
        for (t, _) in &sources {
            let cols: Vec<String> =
                crate::engine::graphsql::EngineSql::columns(&cat, row.db_id, t, lctn)
                    .unwrap_or_default();
            let cols: Vec<String> = cols
                .into_iter()
                .filter(|x| !x.starts_with("_sprig"))
                .collect();
            columns_of.insert(t.clone(), cols);
        }
        let check_props =
            |table: &str, key: &str, props: &mut Option<Vec<(String, String)>>| -> Result<()> {
                let cols = &columns_of[table];
                if !cols.iter().any(|x| x.eq_ignore_ascii_case(key)) {
                    return Err(invalid(format!("table '{table}' has no column '{key}'")));
                }
                if props.is_none() {
                    *props = Some(cols.iter().map(|x| (x.clone(), x.clone())).collect());
                }
                Ok(())
            };
        for v in &mut def.vertices {
            check_props(&v.table, &v.key, &mut v.properties)?;
        }
        for e in &mut def.edges {
            check_props(&e.table, &e.key, &mut e.properties)?;
            for col in [&e.src.column, &e.dst.column] {
                if !columns_of[&e.table]
                    .iter()
                    .any(|x| x.eq_ignore_ascii_case(col))
                {
                    return Err(invalid(format!(
                        "table '{}' has no column '{col}'",
                        e.table
                    )));
                }
            }
        }
        // Each table's key and properties, once, with nothing read.
        let probes = def
            .vertices
            .iter()
            .map(|v| (&v.table, &v.key, v.properties.clone().unwrap_or_default()))
            .chain(
                def.edges
                    .iter()
                    .map(|e| (&e.table, &e.key, e.properties.clone().unwrap_or_default())),
            );
        for (t, key, props) in probes {
            let mut sql = format!("SELECT v.`{}`", key.replace('`', "``"));
            for (_, e) in &props {
                if e.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    let _ = write!(sql, ", v.`{e}`");
                } else {
                    let _ = write!(sql, ", {e}");
                }
            }
            let _ = write!(
                sql,
                " FROM {}.{} v LIMIT 0",
                sprig_common::names::quote(&schema),
                sprig_common::names::quote(t)
            );
            sprig_graph::mapped::SqlRunner::rows(&mut engine_sql, &sql, Vec::new())
                .await
                .map(drop)
                .map_err(|e| e.message)
                .map_err(|e| invalid(format!("a property of table '{t}' doesn't evaluate: {e}")))?;
        }
        let text = serde_json::to_string(&def).map_err(|e| SprigError::internal(e.to_string()))?;
        let object_id = self.svc.catalog.backend().next_object_id(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r = {
            let tx = ch.tx();
            {
                let obj = ObjectRow {
                    object_id,
                    db_id: row.db_id,
                    namespace: Namespace::Graph,
                    name: c.name.clone(),
                    name_key: object_key(ObjectKind::Graph, &c.name, lctn),
                    kind: ObjectKind::Graph,
                    parent_id: None,
                    state: ObjectState::Active,
                    schema_version: 1,
                    flags: ObjectFlags::default(),
                    pending_job_id: None,
                    created_at: SystemTime::now(),
                    changed_version: version,
                };
                match DdlTx::insert_object(tx, &obj).await {
                    Ok(()) => match tx.records().await {
                        Ok(rec) => {
                            GraphCatalog::insert_mapped(rec, object_id.0, row.db_id.0, &text).await
                        }
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(e),
                }
            }
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        let v = ch.commit().await?;
        tracing::info!(tenant = tenant.0, db, graph = %c.name, id = object_id.0, "mapped graph created");
        Ok(v)
    }

    /// Runs a read-only Cypher statement on graph `db.name` for a SQL
    /// session (`GRAPH_TABLE`): `SELECT` on the graph (and, for a mapped
    /// graph, on each source table), the graph budgets, a read-only engine
    /// transaction.
    ///
    /// # Errors
    /// 39902 for an unknown graph; 1142; the executor's errors.
    #[allow(clippy::too_many_lines)] // resolve, check, open, run, end
    pub async fn query_read(
        self: &Arc<Self>,
        cat: &TenantCatalog,
        grants: Option<&dyn SessionGrants>,
        user: &str,
        db: &str,
        name: &str,
        cypher: &str,
    ) -> Result<sprig_graph::Output> {
        let q = sprig_graph::parser::parse(cypher)?;
        let g = self
            .resolve(cat, db, name)?
            .ok_or_else(|| not_found(db, name))?;
        let denied = |what: &str| {
            SprigError::standard(
                1142,
                *b"42000",
                format!("SELECT command denied to user '{user}' for {what}"),
            )
        };
        if !self.allows(
            grants,
            cat,
            &g.db,
            g.db_id,
            Some(g.graph_id),
            Action::Select,
        ) {
            return Err(denied(&format!("graph '{}.{}'", g.db, g.name)));
        }
        let ctx = cat.setting_ctx(&self.svc.dc).with_db(g.db_id);
        let mut budgets = sprig_graph::settings::budgets(&|s| {
            self.svc.settings.get(s, &ctx, Some(&cat.settings))
        });
        budgets.tier = sprig_graph::exec::TraversalTier::parse(&self.svc.settings.get(
            &sprig_graph::settings::TRAVERSAL_TIER,
            &ctx,
            Some(&cat.settings),
        ));
        if engine::on_engine(&g) {
            // Read-only, on its own engine transaction that is never
            // committed. Index commands commit on their own and batches
            // only in an implicit transaction: both are refused here.
            if q.parts.iter().any(|x| {
                x.clauses
                    .iter()
                    .any(|c| matches!(c, sprig_graph::ast::Clause::Schema(_)))
            }) {
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    "GRAPH_TABLE reads only",
                ));
            }
            let eg = engine::graph(&g)?;
            let mut tx = eg.begin(false);
            let r = {
                let mut store = eg.store(&mut tx);
                sprig_graph::exec::run(&mut store, &q, &sprig_graph::eval::Params::new(), &budgets)
                    .await
            };
            tx.rollback();
            let out = r?;
            if out.qtype != "r" {
                return Err(SprigError::new(
                    SprigCode::GraphUnsupported,
                    "GRAPH_TABLE reads only",
                ));
            }
            return Ok(out);
        }
        let r = if g.def.kind == "mapped" {
            let def: sprig_graph::mapped::MappedDef =
                serde_json::from_str(g.def.definition.as_deref().unwrap_or("{}"))
                    .map_err(|e| SprigError::internal(format!("mapped graph definition: {e}")))?;
            for (t, o) in self.mapped_sources(cat, g.db_id, &def)? {
                inheritable_source(cat, &t, o)?;
                if !self.allows(grants, cat, &g.db, g.db_id, Some(o), Action::Select) {
                    return Err(denied(&format!("table '{}.{t}'", g.db)));
                }
            }
            // The tables are read in process.
            let mut run =
                crate::engine::graphsql::EngineSql::new(g.tenant, cat, self.lctn(cat), false);
            let mut store =
                sprig_graph::mapped::MappedStore::new(&mut run, g.graph_id, &g.schema, def);
            sprig_graph::exec::run(&mut store, &q, &sprig_graph::eval::Params::new(), &budgets)
                .await
        } else {
            return Err(SprigError::internal(
                "a native graph outside the storage engine",
            ));
        };
        let out = r?;
        if out.qtype != "r" {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "GRAPH_TABLE reads only",
            ));
        }
        Ok(out)
    }
}

/// 39908 when mapped-graph source `table` (object `o`) is inheritable: a
/// mapped graph reads the table's rows as they are stored, which for an
/// inheritable table are every space's layer rows (overrides, hide
/// markers, other spaces' records), not a space's merged records, so
/// neither inheritance nor the reader's row-level security would apply to
/// them. Checked when the graph is made and at every
/// read, since a table can become inheritable later.
fn inheritable_source(cat: &TenantCatalog, table: &str, o: u64) -> Result<()> {
    let inheritable = cat
        .ext
        .get::<sprig_inherit::InheritState>(sprig_inherit::MODULE)
        .is_some_and(|st| st.object(sprig_common::ids::ObjectId(o)).is_some());
    if inheritable {
        return Err(SprigError::new(
            SprigCode::GraphDefinitionInvalid,
            format!(
                "table '{table}' is inheritable: a mapped graph can't read it yet (its rows are \
                 every space's layers, not a space's merged records)"
            ),
        ));
    }
    Ok(())
}

/// The shard map cache of `tenant`'s home DC, one per DC for the process
/// (the graph store's partitioned graphs and retrieval's chunk tables read
/// their groups' maps here).
///
/// # Errors
/// `Internal` when the cache can't start.
pub(crate) fn map_cache(svc: &TenantServices, tenant: TenantId) -> Result<ShardMapCache> {
    static CACHES: std::sync::OnceLock<Mutex<HashMap<sprig_common::ids::DcId, ShardMapCache>>> =
        std::sync::OnceLock::new();
    let home = svc.locator.home_of(tenant);
    let mut c = CACHES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(x) = c.get(&home) {
        return Ok(x.clone());
    }
    let dc = svc.locator.dc_store();
    let store = if home == svc.dc {
        dc
    } else {
        TierView::over(&dc, Tier::Dc(home.clone()))
    };
    let cache = ShardMapCache::spawn(store)?;
    c.insert(home, cache.clone());
    Ok(cache)
}
