//! The `graph` catalog module: graphs, their native labels and types,
//! property indexes and the bookmark key, kept as the engine's catalog
//! records (`m/graph/<table>/<key>`), the snapshot state
//! ([`GraphExtension`]), graph DDL text, and the physical tables of a
//! native graph.
//!
//! ```text
//! m/graph/graphs/<graph u64>                     GraphRow
//! m/graph/nodes/<graph u64><source u32>          SourceRow (a native label)
//! m/graph/labels/<graph u64><name>               source id
//! m/graph/edges/<graph u64><source u32>          SourceRow (a relationship type)
//! m/graph/types/<graph u64><name>                source id
//! m/graph/indexes/<graph u64><index u32>         IndexRow
//! m/graph/inames/<graph u64><name>               index id
//! m/graph/keys/<kid u32>                         KeyRow   (not versioned)
//! m/graph/leases/<graph u64>                     LeaseRow (not versioned)
//! ```
//!
//! Every write of the versioned tables goes through the caller's catalog
//! change (its records), so every gateway's snapshot follows it. The name
//! rows stand for the unique keys the MariaDB tables had (a native label
//! or type per graph, an index name per graph). The bookmark key and the
//! lease on a storage move are written in short engine transactions of
//! their own, as they were written outside catalog changes before.
//!
//! **As built:**
//! - `CREATE PROPERTY GRAPH` makes a mapped graph (see
//!   [`crate::mapped`]); `ALTER GRAPH ... PARTITION BY` answers 39901.
//! - A native graph is a sharded table group (`group_id`, mirrors
//!   `ready`); graphs made before grouped graphs keep their tables in the
//!   database's first shard schema.
//! - Ids come from a counter table `__sprig_gq_<graph>` in the same
//!   schema (MariaDB graphs) or from the engine's graph keys.
//! - A graph is an `objects` row of kind `graph` in namespace `graph`.
//! - Native graphs stored in MariaDB tables keep three catalog tables of
//!   that path in MariaDB (`graph_label_sets`, `graph_prop_keys`,
//!   `graph_columns`: typed storage) and the mirror applier's positions
//!   (`graph_mirror_pos`), created by the `graph_sql` module.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_catalog::CatalogExtension;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{Migration, Module, Phase};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable};
use sprig_catalog::version::VersionKind;
use sprig_common::ids::TenantId;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

/// The module name.
pub const MODULE: &str = "graph";

/// Graphs by id.
pub const GRAPHS: RowTable = RowTable::new(MODULE, "graphs");
/// Native labels by graph and id.
pub const NODES: RowTable = RowTable::new(MODULE, "nodes");
/// Native label ids by graph and name.
pub const LABELS: RowTable = RowTable::new(MODULE, "labels");
/// Relationship types by graph and id.
pub const EDGES: RowTable = RowTable::new(MODULE, "edges");
/// Relationship type ids by graph and name.
pub const TYPES: RowTable = RowTable::new(MODULE, "types");
/// Property indexes and constraints by graph and id.
pub const INDEXES: RowTable = RowTable::new(MODULE, "indexes");
/// Index ids by graph and name.
pub const INAMES: RowTable = RowTable::new(MODULE, "inames");
/// The bookmark key.
pub const KEYS: RowTable = RowTable::new(MODULE, "keys");
/// Leases on moving a graph's storage.
pub const LEASES: RowTable = RowTable::new(MODULE, "leases");

/// The module's MariaDB migrations: none, its rows are engine records.
pub static GRAPH_MIGRATIONS: &[Migration] = &[];

/// The MariaDB graph path's own catalog tables (typed storage and the
/// mirror applier's positions): what native graphs stored in MariaDB
/// tables still keep in the tenant's MariaDB catalog.
pub const SQL_MODULE: &str = "graph_sql";

/// [`SQL_MODULE`]'s migrations.
pub static GRAPH_SQL_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "graph_mirror_pos, graph_label_sets, graph_prop_keys, graph_columns",
    statements: &[
        crate::mirror::DDL_POS,
        crate::typed::catalog::DDL[0],
        crate::typed::catalog::DDL[1],
        crate::typed::catalog::DDL[2],
    ],
}];

/// The `graph` module, for `Migrator::new`.
pub const GRAPH_MODULE: Module = Module {
    name: MODULE,
    migrations: GRAPH_MIGRATIONS,
};

/// The `graph_sql` module, for `Migrator::new`.
pub const GRAPH_SQL_MODULE: Module = Module {
    name: SQL_MODULE,
    migrations: GRAPH_SQL_MIGRATIONS,
};

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(
                s.code,
                state,
                format!("graph statement failed ({}): {}", s.code, s.message),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "graph storage unavailable"),
    }
    .with_source(e)
}

/// A MariaDB error as a `SprigError`.
#[must_use]
pub fn backend_error(e: mysql_async::Error) -> SprigError {
    map_err(e)
}

/// A taken unique name, as MariaDB's unique key answered it (1062).
fn duplicate(what: &str, name: &str) -> SprigError {
    SprigError::backend(
        1062,
        *b"23000",
        format!("Duplicate entry '{name}' for key '{what}'"),
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// One graph as the catalog has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDef {
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The database.
    pub db_id: u32,
    /// `native` or `mapped`.
    pub kind: String,
    /// `creating`, `active`, ...
    pub state: String,
    /// The graph's sharded table group; `None`: its tables are in the
    /// database's first shard schema (graphs made before grouped graphs).
    pub group_id: Option<u32>,
    /// Mirror rows are kept for every edge (`ready`).
    pub mirrors: bool,
    /// The worker's applier keeps them (`mirror_mode = async`).
    pub mirror_async: bool,
    /// `mirror_mode`: `sync`, `starting` or `async`.
    pub mirror_mode: String,
    /// A mapped graph's definition ([`crate::mapped::MappedDef`] as JSON).
    pub definition: Option<String>,
    /// Label ids to names.
    pub labels: BTreeMap<u32, String>,
    /// Type ids to names.
    pub types: BTreeMap<u32, String>,
    /// Indexes and constraints.
    pub indexes: Vec<crate::store::IndexDef>,
    /// `graphs.storage`: 1 one JSON table set, 2 typed tables
    /// ([`crate::typed`]).
    pub storage: u8,
    /// A typed graph's schema (`storage = 2`, or moving to it).
    pub typed: Option<Arc<crate::typed::TypedSchema>>,
    /// A move to typed tables under way: `dual` (writers write both,
    /// rows are copied), `drop` (the JSON tables go).
    pub storage_phase: Option<String>,
    /// When the row last changed (its mirror mode or storage), ms since
    /// the epoch.
    pub updated_ms: u64,
}

/// The module's state in a snapshot.
#[derive(Debug, Clone, Default)]
pub struct GraphState {
    /// Graphs by id.
    pub graphs: HashMap<u64, Arc<GraphDef>>,
}

impl ExtState for GraphState {
    fn bytes(&self) -> usize {
        self.graphs
            .values()
            .map(|g| 64 + (g.labels.len() + g.types.len()) * 48)
            .sum()
    }
}

/// A graph: what `graphs` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRow {
    /// Its id (`objects.object_id`).
    pub graph_id: u64,
    /// Its database.
    pub db_id: u32,
    /// `mapped` or `native`.
    pub kind: String,
    /// A native graph's table group.
    pub group_id: Option<u32>,
    /// `none`, `building` or `ready`.
    pub mirror_state: String,
    /// `sync`, `starting` or `async`.
    pub mirror_mode: String,
    /// `creating`, `active`, `repartitioning` or `dropping`.
    pub state: String,
    /// A mapped graph's definition (JSON).
    pub definition: Option<String>,
    /// Starts at 1.
    pub def_version: u64,
    /// 1 JSON tables, 2 typed tables.
    pub storage: u8,
    /// A move between storages under way.
    pub storage_phase: Option<String>,
    /// Ms since the epoch.
    pub created_ms: u64,
    /// Ms since the epoch.
    pub updated_ms: u64,
}

/// A native label or relationship type: what `graph_node_sources` and
/// `graph_edge_sources` held for kinds `label` and `type` (the only kinds
/// written).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRow {
    /// The label or type.
    pub name: String,
    /// Writable.
    pub writable: bool,
    /// `active` or `dropping`.
    pub state: String,
}

/// A property index or constraint: what `graph_indexes` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRow {
    /// Its name.
    pub name: String,
    /// `range` or `text`.
    pub kind: String,
    /// A uniqueness constraint.
    pub unique: bool,
    /// `node` or `relationship`.
    pub entity: String,
    /// The label or type id.
    pub source_id: u32,
    /// The properties, in order.
    pub properties: Vec<String>,
    /// `building`, `online` or `dropping`.
    pub state: String,
    /// Ms since the epoch.
    pub created_ms: u64,
}

/// The bookmark key: what `graph_keys` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRow {
    /// The secret.
    pub secret: Vec<u8>,
}

/// Who moves a graph's storage, until when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRow {
    /// The owner.
    pub owner: String,
    /// Ms since the epoch.
    pub until_ms: u64,
}

fn gpk(graph: u64) -> Pk {
    Pk::new().u64(graph)
}

fn name_pk(graph: u64, name: &str) -> Pk {
    Pk::new().u64(graph).bytes(name.as_bytes())
}

fn index_def(id: u32, r: IndexRow) -> crate::store::IndexDef {
    crate::store::IndexDef {
        id,
        name: r.name,
        unique: r.unique,
        kind: if r.kind == "text" {
            crate::ast::IndexKind::Text
        } else {
            crate::ast::IndexKind::Range
        },
        entity: if r.entity == "node" {
            crate::ast::Entity::Node
        } else {
            crate::ast::Entity::Relationship
        },
        token: String::new(),
        token_id: r.source_id,
        props: r.properties,
        state: r.state,
    }
}

fn last_u32(pk: &Pk) -> Option<u32> {
    let b = pk.0.get(pk.0.len().checked_sub(4)?..)?;
    <[u8; 4]>::try_from(b).ok().map(u32::from_be_bytes)
}

fn def_of(r: GraphRow) -> GraphDef {
    GraphDef {
        graph_id: r.graph_id,
        db_id: r.db_id,
        kind: r.kind,
        state: r.state,
        group_id: r.group_id,
        mirrors: r.mirror_state == "ready",
        mirror_async: r.mirror_mode == "async",
        mirror_mode: r.mirror_mode,
        definition: r.definition,
        labels: BTreeMap::new(),
        types: BTreeMap::new(),
        indexes: Vec::new(),
        storage: r.storage,
        typed: None,
        storage_phase: r.storage_phase,
        updated_ms: r.updated_ms,
    }
}

/// Reads and writes the module's records.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphCatalog;

impl GraphCatalog {
    /// Makes sure catalog `schema` can take the module's records and the
    /// MariaDB graph path's tables (idempotent).
    ///
    /// # Errors
    /// The migrator's.
    pub async fn migrate(q: &mut mysql_async::Conn, schema: &str) -> Result<()> {
        sprig_catalog::Migrator::new(&[GRAPH_SQL_MODULE])?
            .run(q, schema, &sprig_catalog::migrate::MigrateOpts::default())
            .await?;
        Ok(())
    }

    /// Every graph's row, by id.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn rows(rec: &mut dyn RecordTx) -> Result<Vec<GraphRow>> {
        GRAPHS.rows(rec, &Pk::new()).await
    }

    /// Graph `graph`'s row.
    ///
    /// # Errors
    /// The record store's.
    pub async fn row(rec: &mut dyn RecordTx, graph: u64) -> Result<Option<GraphRow>> {
        GRAPHS.get(rec, &gpk(graph)).await
    }

    /// Every graph, without typed schemas (see [`Self::load_typed`]).
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx) -> Result<GraphState> {
        let mut graphs: HashMap<u64, GraphDef> = Self::rows(rec)
            .await?
            .into_iter()
            .map(|r| (r.graph_id, def_of(r)))
            .collect();
        for (pk, s) in NODES.scan::<SourceRow>(rec, &Pk::new()).await? {
            let (Some(g), Some(id)) = (be_u64(&pk.0), last_u32(&pk)) else {
                continue;
            };
            if let Some(d) = graphs.get_mut(&g) {
                d.labels.insert(id, s.name);
            }
        }
        for (pk, s) in EDGES.scan::<SourceRow>(rec, &Pk::new()).await? {
            let (Some(g), Some(id)) = (be_u64(&pk.0), last_u32(&pk)) else {
                continue;
            };
            if let Some(d) = graphs.get_mut(&g) {
                d.types.insert(id, s.name);
            }
        }
        for (g, mut def) in Self::indexes(rec, None).await? {
            if let Some(d) = graphs.get_mut(&g) {
                let lookup = match def.entity {
                    crate::ast::Entity::Node => &d.labels,
                    crate::ast::Entity::Relationship => &d.types,
                };
                if let Some(t) = lookup.get(&def.token_id) {
                    def.token.clone_from(t);
                }
                d.indexes.push(def);
            }
        }
        Ok(GraphState {
            graphs: graphs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
        })
    }

    /// Adds the typed schemas of typed graphs (and graphs moving to typed
    /// storage) from the MariaDB graph path's tables in catalog `schema`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn load_typed<Q: Queryable + Send>(
        st: &mut GraphState,
        q: &mut Q,
        schema: &str,
    ) -> Result<()> {
        for d in st.graphs.values_mut() {
            if d.storage == crate::typed::STORAGE_TYPED || d.storage_phase.is_some() {
                let typed = crate::typed::catalog::load(q, schema, d.graph_id).await?;
                Arc::make_mut(d).typed = Some(Arc::new(typed));
            }
        }
        Ok(())
    }

    /// Indexes and constraints (of one graph, or all), with their token
    /// names left empty: `(graph, definition)`, by graph and index id.
    ///
    /// # Errors
    /// The record store's.
    pub async fn indexes(
        rec: &mut dyn RecordTx,
        graph: Option<u64>,
    ) -> Result<Vec<(u64, crate::store::IndexDef)>> {
        let under = graph.map_or_else(Pk::new, gpk);
        let mut out = Vec::new();
        for (pk, r) in INDEXES.scan::<IndexRow>(rec, &under).await? {
            let (Some(g), Some(id)) = (be_u64(&pk.0), last_u32(&pk)) else {
                continue;
            };
            out.push((g, index_def(id, r)));
        }
        Ok(out)
    }

    /// The tenant's bookmark secret, made on first use (in a short engine
    /// transaction of its own).
    ///
    /// # Errors
    /// The record store's.
    pub async fn bookmark_secret(store: &dyn RecordStore, tenant: TenantId) -> Result<Vec<u8>> {
        let pk = Pk::new().u32(1);
        {
            let mut tx = store.begin(tenant).await?;
            if let Some(k) = KEYS.get::<KeyRow>(&mut *tx, &pk).await? {
                return Ok(k.secret);
            }
        }
        let mut tx = store.begin(tenant).await?;
        if let Some(k) = KEYS.get_for_update::<KeyRow>(&mut *tx, &pk).await? {
            return Ok(k.secret);
        }
        let mut secret = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut secret)
            .map_err(|_| SprigError::internal("no randomness for the bookmark key"))?;
        let row = KeyRow {
            secret: secret.to_vec(),
        };
        KEYS.put(&mut *tx, &pk, &row).await?;
        tx.commit().await?;
        Ok(row.secret)
    }

    /// Adds index `d` of `graph` as `building`, with the next index id:
    /// that id.
    ///
    /// # Errors
    /// The record store's (a backend 1062 for a name in use).
    pub async fn insert_index(
        rec: &mut dyn RecordTx,
        graph: u64,
        d: &crate::store::IndexDef,
    ) -> Result<u32> {
        if INAMES
            .get::<u32>(rec, &name_pk(graph, &d.name))
            .await?
            .is_some()
        {
            return Err(duplicate("uk_name", &d.name));
        }
        let id = Self::indexes(rec, Some(graph))
            .await?
            .iter()
            .map(|(_, d)| d.id)
            .max()
            .unwrap_or(0)
            + 1;
        let row = IndexRow {
            name: d.name.clone(),
            kind: if d.kind == crate::ast::IndexKind::Text {
                "text"
            } else {
                "range"
            }
            .to_owned(),
            unique: d.unique,
            entity: if d.entity == crate::ast::Entity::Node {
                "node"
            } else {
                "relationship"
            }
            .to_owned(),
            source_id: d.token_id,
            properties: d.props.clone(),
            state: "building".to_owned(),
            created_ms: now_ms(),
        };
        INDEXES
            .put(rec, &Pk::new().u64(graph).u32(id), &row)
            .await?;
        INAMES.put(rec, &name_pk(graph, &d.name), &id).await?;
        Ok(id)
    }

    /// Marks index `id` of `graph` `online`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn index_online(rec: &mut dyn RecordTx, graph: u64, id: u32) -> Result<()> {
        let pk = Pk::new().u64(graph).u32(id);
        if let Some(mut r) = INDEXES.get::<IndexRow>(rec, &pk).await? {
            "online".clone_into(&mut r.state);
            INDEXES.put(rec, &pk, &r).await?;
        }
        Ok(())
    }

    /// Deletes index `id` of `graph`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_index(rec: &mut dyn RecordTx, graph: u64, id: u32) -> Result<()> {
        let pk = Pk::new().u64(graph).u32(id);
        if let Some(r) = INDEXES.get::<IndexRow>(rec, &pk).await? {
            INAMES.delete(rec, &name_pk(graph, &r.name)).await?;
            INDEXES.delete(rec, &pk).await?;
        }
        Ok(())
    }

    /// Inserts a native graph's row, `active`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_native(rec: &mut dyn RecordTx, graph: u64, db: u32) -> Result<()> {
        Self::insert_native_as(rec, graph, db, None, 1).await
    }

    /// Inserts a mapped graph's row with its definition (JSON).
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_mapped(
        rec: &mut dyn RecordTx,
        graph: u64,
        db: u32,
        definition: &str,
    ) -> Result<()> {
        let now = now_ms();
        let row = GraphRow {
            graph_id: graph,
            db_id: db,
            kind: "mapped".to_owned(),
            group_id: None,
            mirror_state: "none".to_owned(),
            mirror_mode: "sync".to_owned(),
            state: "active".to_owned(),
            definition: Some(definition.to_owned()),
            def_version: 1,
            storage: 1,
            storage_phase: None,
            created_ms: now,
            updated_ms: now,
        };
        GRAPHS.put(rec, &gpk(graph), &row).await
    }

    /// Inserts a native graph's row with its table group (`None`: one
    /// schema, no mirrors); a grouped graph keeps mirror rows from the
    /// start (`mirror_state = ready`).
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_native_in(
        rec: &mut dyn RecordTx,
        graph: u64,
        db: u32,
        group: Option<u32>,
    ) -> Result<()> {
        Self::insert_native_as(rec, graph, db, group, 1).await
    }

    /// [`Self::insert_native_in`] with its storage (`graphs.storage`: 1
    /// JSON, 2 typed tables).
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_native_as(
        rec: &mut dyn RecordTx,
        graph: u64,
        db: u32,
        group: Option<u32>,
        storage: u8,
    ) -> Result<()> {
        let now = now_ms();
        let row = GraphRow {
            graph_id: graph,
            db_id: db,
            kind: "native".to_owned(),
            group_id: group,
            mirror_state: if group.is_some() { "ready" } else { "none" }.to_owned(),
            mirror_mode: "sync".to_owned(),
            state: "active".to_owned(),
            definition: None,
            def_version: 1,
            storage,
            storage_phase: None,
            created_ms: now,
            updated_ms: now,
        };
        GRAPHS.put(rec, &gpk(graph), &row).await
    }

    /// Deletes a graph's records (its row, labels, types and indexes). The MariaDB graph path's rows go with
    /// [`Self::delete_sql`].
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete(rec: &mut dyn RecordTx, graph: u64) -> Result<()> {
        let under = gpk(graph);
        for t in [NODES, LABELS, EDGES, TYPES, INDEXES, INAMES] {
            for (pk, _) in t.scan::<serde_json::Value>(rec, &under).await? {
                t.delete(rec, &pk).await?;
            }
        }
        // The storage-move lease is written only directly (never through
        // a change); a dropped graph's lease is ignored (ids aren't reused).
        GRAPHS.delete(rec, &under).await
    }

    /// Deletes graph `graph`'s rows of the MariaDB graph path's tables in
    /// catalog `schema` (typed storage, mirror positions); tables that
    /// don't exist are skipped.
    ///
    /// # Errors
    /// A backend error.
    pub async fn delete_sql<Q: Queryable + Send>(
        q: &mut Q,
        schema: &str,
        graph: u64,
    ) -> Result<()> {
        for t in ["graph_label_sets", "graph_prop_keys", "graph_columns"] {
            match q
                .exec_drop(
                    format!("DELETE FROM {}.{t} WHERE graph_id = ?", quote(schema)),
                    (graph,),
                )
                .await
            {
                Err(mysql_async::Error::Server(e)) if e.code == 1146 => {}
                r => r.map_err(map_err)?,
            }
        }
        match crate::mirror::clear_pos(q, schema, graph).await {
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. }) => {}
            r => r?,
        }
        Ok(())
    }

    async fn update_row(
        rec: &mut dyn RecordTx,
        graph: u64,
        f: impl FnOnce(&mut GraphRow) + Send,
    ) -> Result<()> {
        let pk = gpk(graph);
        if let Some(mut r) = GRAPHS.get::<GraphRow>(rec, &pk).await? {
            f(&mut r);
            r.updated_ms = now_ms().max(r.updated_ms + 1);
            GRAPHS.put(rec, &pk, &r).await?;
        }
        Ok(())
    }

    /// Sets a graph's storage and the phase of its move (in the caller's
    /// versioned change).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_storage(
        rec: &mut dyn RecordTx,
        graph: u64,
        storage: u8,
        phase: Option<&str>,
    ) -> Result<()> {
        let phase = phase.map(str::to_owned);
        Self::update_row(rec, graph, move |r| {
            r.storage = storage;
            r.storage_phase = phase;
        })
        .await
    }

    /// Takes or renews the lease on moving a graph's storage for `owner`,
    /// for `secs` seconds: whether `owner` holds it (no one else holds a
    /// lease that hasn't run out). Written in a short engine transaction
    /// of its own, outside versioned changes: the snapshot doesn't carry
    /// it.
    ///
    /// # Errors
    /// The record store's.
    pub async fn lease_storage(
        store: &dyn RecordStore,
        tenant: TenantId,
        graph: u64,
        owner: &str,
        secs: u32,
    ) -> Result<bool> {
        let pk = gpk(graph);
        let mut tx = store.begin(tenant).await?;
        if GRAPHS.get::<GraphRow>(&mut *tx, &pk).await?.is_none() {
            return Ok(false);
        }
        let now = now_ms();
        let held = LEASES.get_for_update::<LeaseRow>(&mut *tx, &pk).await?;
        if let Some(l) = &held
            && l.owner != owner
            && l.until_ms >= now
        {
            return Ok(false);
        }
        let row = LeaseRow {
            owner: owner.to_owned(),
            until_ms: now + u64::from(secs) * 1000,
        };
        LEASES.put(&mut *tx, &pk, &row).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Gives the lease on moving a graph's storage back, if `owner` holds
    /// it.
    ///
    /// # Errors
    /// The record store's.
    pub async fn release_storage(
        store: &dyn RecordStore,
        tenant: TenantId,
        graph: u64,
        owner: &str,
    ) -> Result<()> {
        let pk = gpk(graph);
        let mut tx = store.begin(tenant).await?;
        if let Some(l) = LEASES.get_for_update::<LeaseRow>(&mut *tx, &pk).await?
            && l.owner == owner
        {
            LEASES.delete(&mut *tx, &pk).await?;
            tx.commit().await?;
        }
        Ok(())
    }

    /// Sets who keeps a graph's mirrors and their state:
    /// `mode` `sync`, `starting` or `async`, `state` `building` or
    /// `ready`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_mirror(
        rec: &mut dyn RecordTx,
        graph: u64,
        mode: &str,
        state: &str,
    ) -> Result<()> {
        let (mode, state) = (mode.to_owned(), state.to_owned());
        Self::update_row(rec, graph, move |r| {
            r.mirror_mode = mode;
            r.mirror_state = state;
        })
        .await
    }

    /// The id of a native label (`label = true`) or relationship type.
    ///
    /// # Errors
    /// The record store's.
    pub async fn find_source(
        rec: &mut dyn RecordTx,
        graph: u64,
        label: bool,
        name: &str,
    ) -> Result<Option<u32>> {
        let names = if label { LABELS } else { TYPES };
        names.get::<u32>(rec, &name_pk(graph, name)).await
    }

    /// Adds a native label (`label = true`) or relationship type, or
    /// returns the one there: its id.
    ///
    /// # Errors
    /// 39908 for a name of 0 or more than 255 bytes; the record store's.
    pub async fn add_source(
        rec: &mut dyn RecordTx,
        graph: u64,
        label: bool,
        name: &str,
    ) -> Result<u32> {
        if name.is_empty() || name.len() > 255 {
            return Err(SprigError::new(
                SprigCode::GraphDefinitionInvalid,
                "a label or relationship type is 1 to 255 characters",
            ));
        }
        let (rows, names) = if label {
            (NODES, LABELS)
        } else {
            (EDGES, TYPES)
        };
        if let Some(id) = names.get::<u32>(rec, &name_pk(graph, name)).await? {
            return Ok(id);
        }
        let id = rows
            .scan::<serde_json::Value>(rec, &gpk(graph))
            .await?
            .iter()
            .filter_map(|(pk, _)| last_u32(pk))
            .max()
            .unwrap_or(0)
            + 1;
        let row = SourceRow {
            name: name.to_owned(),
            writable: true,
            state: "active".to_owned(),
        };
        rows.put(rec, &Pk::new().u64(graph).u32(id), &row).await?;
        names.put(rec, &name_pk(graph, name), &id).await?;
        Ok(id)
    }
}

fn be_u64(b: &[u8]) -> Option<u64> {
    b.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

/// The module in the catalog cache: [`GraphState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphExtension;

#[async_trait::async_trait]
impl CatalogExtension for GraphExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        GRAPH_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        if !tx.catalog_records() {
            return Ok(Arc::new(GraphState::default()));
        }
        let mut st = GraphCatalog::load(tx.records()?).await?;
        if st
            .graphs
            .values()
            .any(|d| d.storage == crate::typed::STORAGE_TYPED || d.storage_phase.is_some())
        {
            let schema = tx.schema().to_owned();
            GraphCatalog::load_typed(&mut st, tx.conn(), &schema).await?;
        }
        Ok(Arc::new(st))
    }
}

// ----------------------------------------------------------------------
// Physical tables.
// ----------------------------------------------------------------------

/// Names of a native graph's tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    /// Nodes.
    pub nodes: String,
    /// Edges.
    pub edges: String,
    /// Label rows.
    pub labels: String,
    /// Mirrors of incoming edges (on the destination's shard).
    pub mirrors: String,
    /// Id counters.
    pub seq: String,
    /// Uniqueness claims, on the shard of each claim's hash.
    pub claims: String,
}

impl Tables {
    /// The tables of graph `g` in `schema`, quoted and qualified.
    #[must_use]
    pub fn of(schema: &str, g: u64) -> Self {
        let q = |p: &str| format!("{}.{}", quote(schema), quote(&format!("__sprig_{p}_{g}")));
        Self {
            nodes: q("gn"),
            edges: q("ge"),
            labels: q("gl"),
            mirrors: q("gm"),
            seq: q("gq"),
            claims: q("gu"),
        }
    }

    /// `CREATE TABLE IF NOT EXISTS` for all five, and the counter rows (a
    /// graph in one schema).
    #[must_use]
    pub fn create_ddl(&self) -> Vec<String> {
        let mut v = self.shard_ddl();
        v.extend(self.seq_ddl());
        v
    }

    /// The counter table and its rows (in the database's first shard
    /// schema: it has no `_sprig_rk`, so it never joins the graph's group).
    #[must_use]
    pub fn seq_ddl(&self) -> Vec<String> {
        vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  kind TINYINT UNSIGNED NOT NULL, next BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (kind)
) ENGINE=InnoDB",
                self.seq
            ),
            format!(
                "INSERT IGNORE INTO {} (kind, next) VALUES (1, 1), (2, 1)",
                self.seq
            ),
        ]
    }

    /// `CREATE TABLE IF NOT EXISTS` for the tables of one shard: nodes,
    /// edges, label rows and mirrors.
    #[must_use]
    pub fn shard_ddl(&self) -> Vec<String> {
        vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  id BIGINT UNSIGNED NOT NULL,
  part BIGINT UNSIGNED NOT NULL,
  labels JSON NOT NULL,
  props JSON NOT NULL, _types JSON NULL,
  _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (id), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.nodes
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  src BIGINT UNSIGNED NOT NULL, type INT UNSIGNED NOT NULL,
  dst BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  dst_rk BIGINT UNSIGNED NOT NULL,
  props JSON NOT NULL, _types JSON NULL, _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (src, type, dst, id),
  UNIQUE KEY uk_id (id),
  KEY rev (dst, type, src),
  KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.edges
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  label INT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (label, id), KEY k_id (id)
) ENGINE=InnoDB",
                self.labels
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  dst BIGINT UNSIGNED NOT NULL, type INT UNSIGNED NOT NULL,
  src BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  src_rk BIGINT UNSIGNED NOT NULL, _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (dst, type, src, id),
  KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.mirrors
            ),
            self.claims_ddl(),
        ]
    }

    /// `CREATE TABLE IF NOT EXISTS` for the uniqueness claims of one shard:
    /// a row per constraint and key, routed by the claim's hash
    /// (`schema::claim_rk`), naming the element that holds the key.
    #[must_use]
    pub fn claims_ddl(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  idx INT UNSIGNED NOT NULL, k VARBINARY(600) NOT NULL,
  owner BIGINT UNSIGNED NOT NULL, owner_rk BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (idx, k), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
            self.claims
        )
    }

    /// The virtual column of property `k` of index `d`.
    #[must_use]
    pub fn index_column(d: &crate::store::IndexDef, k: usize) -> String {
        format!("_sprig_i{}_{k}", d.id)
    }

    /// The virtual column holding property `p`'s typed text, shared by
    /// the indexes on `p`: `_sprig_p` and 16 hex digits of
    /// the name's SHA-256.
    #[must_use]
    pub fn property_column(p: &str) -> String {
        let d = ring::digest::digest(&ring::digest::SHA256, p.as_bytes());
        let mut s = String::from("_sprig_p");
        for b in &d.as_ref()[..8] {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// The SQL of property `p`'s typed text, as [`crate::schema`] says
    /// (texts over [`crate::schema::MAX_INLINE`] characters hashed); `p`
    /// was checked for quotes.
    fn typed_text(p: &str) -> String {
        let v = format!("JSON_VALUE(props, '$.\"{p}\"')");
        // A value `_types` names (bytes, a temporal
        // value) is `t<type>:<text>`, as `schema::prop_index_value` says,
        // so it never equals a plain string's `s<text>`.
        // An integral float inside the 64-bit range is its integer,
        // as `schema::numeric_key` says. Plain digits stay as they are
        // (no detour through a double); MariaDB calls `1e16` an INTEGER,
        // so both types take the same path. `JSON_VALUE` answers `1` for
        // `true`.
        let f = format!("({v} + 0e0)");
        let num = format!(
            "CONCAT('n', IF(INSTR({v}, 'e') + INSTR({v}, 'E') + INSTR({v}, '.') = 0, {v}, \
             IF(FLOOR({f}) = {f} AND ABS({f}) < 9223372036854775807e0, CAST({f} AS SIGNED), {v})))"
        );
        let t = format!(
            "(CASE JSON_TYPE(JSON_EXTRACT(props, '$.\"{p}\"')) \
             WHEN 'STRING' THEN CONCAT(IFNULL(CONCAT('t', JSON_VALUE(_types, '$.\"{p}\"'), ':'), 's'), {v}) \
             WHEN 'INTEGER' THEN {num} WHEN 'DOUBLE' THEN {num} \
             WHEN 'BOOLEAN' THEN IF({v} IN ('1', 'true'), 'btrue', 'bfalse') END)"
        );
        format!(
            "IF(CHAR_LENGTH({t}) <= {max}, {t}, CONCAT('h', SHA2({t}, 256)))",
            max = crate::schema::MAX_INLINE
        )
    }

    /// `ALTER TABLE` adding index `d`'s virtual columns and its index
    /// (`UNIQUE` for a constraint); see [`crate::schema`] for the values.
    ///
    /// # Errors
    /// 39908 for a property name with quotes or backslashes.
    pub fn index_ddl(&self, d: &crate::store::IndexDef) -> Result<String> {
        use crate::ast::{Entity, IndexKind};
        let (table, cond) = match d.entity {
            Entity::Node => (
                &self.nodes,
                format!("JSON_CONTAINS(labels, '{}')", d.token_id),
            ),
            Entity::Relationship => (&self.edges, format!("type = {}", d.token_id)),
        };
        let mut parts = Vec::new();
        let mut cols = Vec::new();
        for (k, p) in d.props.iter().enumerate() {
            if p.contains(['"', '\\', '\'', '`']) || p.chars().any(char::is_control) {
                return Err(SprigError::new(
                    SprigCode::GraphDefinitionInvalid,
                    format!(
                        "property '{p}' can't be indexed: quotes and backslashes aren't allowed in indexed property names"
                    ),
                ));
            }
            // MariaDB refused an LDBC run's 20th index (1117, table
            // definition too large), so the
            // typed text of a property is one virtual column per property
            // (`_sprig_p<hash>`), shared by every index on it; each
            // index's own column only picks it for its label or type. The
            // long expression is then in the table definition once per
            // property, not once per index.
            let pcol = Self::property_column(p);
            parts.push(format!(
                "ADD COLUMN IF NOT EXISTS `{pcol}` VARCHAR({max}) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin \
                 AS ({}) VIRTUAL",
                Self::typed_text(p),
                max = crate::schema::MAX_INLINE
            ));
            // A text index holds strings only: the typed texts that aren't
            // a number (`n`) or a boolean (`b`).
            let val = match d.kind {
                IndexKind::Range => format!("`{pcol}`"),
                IndexKind::Text => format!("IF(LEFT(`{pcol}`, 1) IN ('n', 'b'), NULL, `{pcol}`)"),
            };
            let col = Self::index_column(d, k);
            parts.push(format!(
                "ADD COLUMN IF NOT EXISTS `{col}` VARCHAR({max}) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin \
                 AS (IF({cond}, {val}, NULL)) VIRTUAL",
                max = crate::schema::MAX_INLINE
            ));
            cols.push(format!("`{col}`"));
        }
        parts.push(format!(
            "ADD {}INDEX IF NOT EXISTS `_sprig_i{}` ({})",
            if d.unique { "UNIQUE " } else { "" },
            d.id,
            cols.join(", ")
        ));
        Ok(format!("ALTER TABLE {table} {}", parts.join(", ")))
    }

    /// `ALTER TABLE` dropping index `d` and its columns.
    #[must_use]
    pub fn index_drop_ddl(&self, d: &crate::store::IndexDef) -> String {
        let table = match d.entity {
            crate::ast::Entity::Node => &self.nodes,
            crate::ast::Entity::Relationship => &self.edges,
        };
        let mut parts = vec![format!("DROP INDEX IF EXISTS `_sprig_i{}`", d.id)];
        for k in 0..d.props.len() {
            parts.push(format!(
                "DROP COLUMN IF EXISTS `{}`",
                Self::index_column(d, k)
            ));
        }
        format!("ALTER TABLE {table} {}", parts.join(", "))
    }

    /// `DROP TABLE IF EXISTS` for all five.
    #[must_use]
    pub fn drop_ddl(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {}, {}, {}",
            self.nodes, self.edges, self.labels, self.mirrors, self.seq, self.claims
        )
    }

    /// `DROP TABLE IF EXISTS` for one shard's four.
    #[must_use]
    pub fn shard_drop_ddl(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {}, {}",
            self.nodes, self.edges, self.labels, self.mirrors, self.claims
        )
    }
}

// ----------------------------------------------------------------------
// Graph DDL text (SQL side).
// ----------------------------------------------------------------------

/// A graph DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphDdl {
    /// `CREATE GRAPH [IF NOT EXISTS] [db.]name`.
    Create {
        /// The database, if named.
        db: Option<String>,
        /// The graph.
        name: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `CREATE [OR REPLACE] PROPERTY GRAPH ...` (a mapped graph).
    CreateProperty(Box<crate::mapped::CreatePropertyGraph>),
    /// `DROP GRAPH [IF EXISTS] [db.]name`.
    Drop {
        /// The database, if named.
        db: Option<String>,
        /// The graph.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
}

fn words(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in s.trim().trim_end_matches(';').chars() {
        if c == '`' {
            quoted = !quoted;
            cur.push(c);
        } else if !quoted && c.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn unquote(s: &str) -> String {
    s.strip_prefix('`')
        .and_then(|x| x.strip_suffix('`'))
        .unwrap_or(s)
        .to_owned()
}

/// Splits `db.name` (either part may be back-quoted).
fn qualified(s: &str) -> Result<(Option<String>, String)> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in s.chars() {
        match c {
            '`' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    parts.push(cur);
    let bad = || {
        SprigError::new(
            SprigCode::GraphDefinitionInvalid,
            format!("'{s}' is not a valid graph name"),
        )
    };
    let check = |n: &str| -> Result<()> {
        if n.is_empty()
            || n.len() > 64
            || n.to_ascii_lowercase().starts_with("__sprig_")
            || n.chars().any(char::is_control)
        {
            return Err(bad());
        }
        Ok(())
    };
    match parts.as_slice() {
        [n] => {
            check(n)?;
            Ok((None, n.clone()))
        }
        [d, n] => {
            check(d)?;
            check(n)?;
            Ok((Some(d.clone()), n.clone()))
        }
        _ => Err(bad()),
    }
}

/// Recognizes graph DDL in a SQL text: `None` if it isn't graph DDL.
///
/// # Errors
/// 39900 for malformed graph DDL; 39908 for a mapped graph this build
/// can't map; 39901 for `ALTER GRAPH`.
pub fn parse_ddl(sql: &str) -> Result<Option<GraphDdl>> {
    let w = words(sql);
    let up: Vec<String> = w.iter().map(|x| x.to_ascii_uppercase()).collect();
    let u: Vec<&str> = up.iter().map(String::as_str).collect();
    let syntax = || {
        SprigError::new(
            SprigCode::GraphSyntax,
            format!("malformed graph statement: {sql}"),
        )
    };
    match u.as_slice() {
        ["CREATE", "PROPERTY", "GRAPH", ..]
        | ["CREATE", "OR", "REPLACE", "PROPERTY", "GRAPH", ..] => {
            Ok(crate::mapped::parse_create(sql)?.map(|c| GraphDdl::CreateProperty(Box::new(c))))
        }
        ["ALTER", "GRAPH", ..] => Err(SprigError::new(
            SprigCode::GraphUnsupported,
            "ALTER GRAPH (partitioning) is not supported yet",
        )),
        ["CREATE", "GRAPH", "IF", "NOT", "EXISTS", _] => {
            let (db, name) = qualified(&w[5])?;
            Ok(Some(GraphDdl::Create {
                db,
                name: unquote(&name),
                if_not_exists: true,
            }))
        }
        ["CREATE", "GRAPH", _] | ["CREATE", "GRAPH", _, "ANY"] => {
            let (db, name) = qualified(&w[2])?;
            Ok(Some(GraphDdl::Create {
                db,
                name,
                if_not_exists: false,
            }))
        }
        ["DROP", "GRAPH", "IF", "EXISTS", _] => {
            let (db, name) = qualified(&w[4])?;
            Ok(Some(GraphDdl::Drop {
                db,
                name,
                if_exists: true,
            }))
        }
        ["DROP", "GRAPH", _] | ["DROP", "PROPERTY", "GRAPH", _] => {
            let (db, name) = qualified(w.last().map_or("", String::as_str))?;
            Ok(Some(GraphDdl::Drop {
                db,
                name,
                if_exists: false,
            }))
        }
        ["CREATE" | "DROP", "GRAPH", ..] => Err(syntax()),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        sprig_catalog::Migrator::new(&[GRAPH_MODULE, GRAPH_SQL_MODULE])
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(GRAPH_MODULE.newest(), 0);
        assert_eq!(GRAPH_SQL_MODULE.newest(), 1);
    }

    use sprig_catalog::records::mem::MemRecords;

    const T: TenantId = TenantId(57_406_000_001);

    fn idx(name: &str, token_id: u32, entity: crate::ast::Entity) -> crate::store::IndexDef {
        crate::store::IndexDef {
            id: 0,
            name: name.into(),
            unique: name.starts_with('u'),
            kind: crate::ast::IndexKind::Range,
            entity,
            token: String::new(),
            token_id,
            props: vec!["a".into(), "b".into()],
            state: String::new(),
        }
    }

    async fn load(store: &MemRecords) -> GraphState {
        let mut tx = store.begin(T).await.unwrap();
        GraphCatalog::load(&mut *tx).await.unwrap()
    }

    #[tokio::test]
    async fn graphs_sources_and_indexes_follow_the_sql_rules() {
        use crate::ast::Entity;
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        GraphCatalog::insert_native_as(&mut *tx, 900, 3, Some(7), 2)
            .await
            .unwrap();
        GraphCatalog::insert_native(&mut *tx, 901, 3).await.unwrap();
        GraphCatalog::insert_mapped(&mut *tx, 902, 4, "{}")
            .await
            .unwrap();
        // Labels and types: ids per graph from 1, a name added twice keeps
        // its id; the label and type namespaces are apart.
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, true, "Person")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, true, "City")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, true, "Person")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, false, "Person")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 901, true, "a\0b")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, true, "")
                .await
                .unwrap_err()
                .code(),
            Some(SprigCode::GraphDefinitionInvalid)
        );
        assert_eq!(
            GraphCatalog::find_source(&mut *tx, 900, true, "City")
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(
            GraphCatalog::find_source(&mut *tx, 900, false, "City")
                .await
                .unwrap(),
            None
        );
        // Indexes: ids per graph from 1, a taken name is 1062.
        let a = GraphCatalog::insert_index(&mut *tx, 900, &idx("by_name", 1, Entity::Node))
            .await
            .unwrap();
        let b = GraphCatalog::insert_index(&mut *tx, 900, &idx("u_rel", 1, Entity::Relationship))
            .await
            .unwrap();
        assert_eq!((a, b), (1, 2));
        let e = GraphCatalog::insert_index(&mut *tx, 900, &idx("by_name", 2, Entity::Node))
            .await
            .unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Backend { errno: 1062, .. }
        ));
        GraphCatalog::index_online(&mut *tx, 900, 1).await.unwrap();
        tx.commit().await.unwrap();

        let st = load(&store).await;
        assert_eq!(st.graphs.len(), 3);
        let g = &st.graphs[&900];
        assert_eq!((g.kind.as_str(), g.state.as_str()), ("native", "active"));
        assert_eq!(
            (g.group_id, g.storage, g.mirrors, g.mirror_async),
            (Some(7), 2, true, false)
        );
        assert_eq!(g.labels.get(&1).map(String::as_str), Some("Person"));
        assert_eq!(g.types.get(&1).map(String::as_str), Some("Person"));
        assert_eq!(g.indexes.len(), 2);
        assert_eq!(
            (g.indexes[0].token.as_str(), g.indexes[0].state.as_str()),
            ("Person", "online")
        );
        assert_eq!(g.indexes[0].props, vec!["a".to_owned(), "b".to_owned()]);
        assert!(g.indexes[1].unique);
        assert_eq!(g.indexes[1].state, "building");
        assert_eq!(
            st.graphs[&901].labels.get(&1).map(String::as_str),
            Some("a\0b")
        );
        assert!(!st.graphs[&901].mirrors);
        assert_eq!(st.graphs[&902].definition.as_deref(), Some("{}"));

        // Mirror mode and storage move the change time; a dropped index
        // frees its name; a dropped graph leaves nothing.
        let mut tx = store.begin(T).await.unwrap();
        let before = GraphCatalog::row(&mut *tx, 900)
            .await
            .unwrap()
            .unwrap()
            .updated_ms;
        GraphCatalog::set_mirror(&mut *tx, 900, "async", "ready")
            .await
            .unwrap();
        GraphCatalog::set_storage(&mut *tx, 901, 1, Some("dual"))
            .await
            .unwrap();
        GraphCatalog::delete_index(&mut *tx, 900, 1).await.unwrap();
        assert_eq!(
            GraphCatalog::insert_index(&mut *tx, 900, &idx("by_name", 2, Entity::Node))
                .await
                .unwrap(),
            3
        );
        GraphCatalog::delete(&mut *tx, 902).await.unwrap();
        tx.commit().await.unwrap();
        let st = load(&store).await;
        let g = &st.graphs[&900];
        assert!(g.mirror_async && g.mirrors && g.updated_ms > before);
        assert_eq!(st.graphs[&901].storage_phase.as_deref(), Some("dual"));
        assert!(!st.graphs.contains_key(&902));
        assert_eq!(
            g.indexes.iter().map(|d| d.id).collect::<Vec<_>>(),
            vec![2, 3]
        );
        let mut tx = store.begin(T).await.unwrap();
        GraphCatalog::delete(&mut *tx, 900).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = store.begin(T).await.unwrap();
        assert!(
            GraphCatalog::indexes(&mut *tx, Some(900))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            GraphCatalog::find_source(&mut *tx, 900, true, "Person")
                .await
                .unwrap(),
            None
        );
        // A graph made again under the same id starts its ids from 1.
        GraphCatalog::insert_native(&mut *tx, 900, 3).await.unwrap();
        assert_eq!(
            GraphCatalog::add_source(&mut *tx, 900, true, "City")
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn an_uncommitted_change_leaves_nothing() {
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        GraphCatalog::insert_native(&mut *tx, 5, 1).await.unwrap();
        GraphCatalog::add_source(&mut *tx, 5, true, "X")
            .await
            .unwrap();
        drop(tx);
        assert!(load(&store).await.graphs.is_empty());
    }

    #[tokio::test]
    async fn the_bookmark_key_is_made_once_and_leases_exclude() {
        let store = MemRecords::new();
        let a = GraphCatalog::bookmark_secret(&store, T).await.unwrap();
        let b = GraphCatalog::bookmark_secret(&store, T).await.unwrap();
        assert_eq!(a.len(), 32);
        assert_eq!(a, b);
        assert_ne!(
            GraphCatalog::bookmark_secret(&store, TenantId(2))
                .await
                .unwrap(),
            a
        );
        // No lease on a graph the catalog doesn't have.
        assert!(
            !GraphCatalog::lease_storage(&store, T, 9, "gw1", 30)
                .await
                .unwrap()
        );
        let mut tx = store.begin(T).await.unwrap();
        GraphCatalog::insert_native(&mut *tx, 9, 1).await.unwrap();
        tx.commit().await.unwrap();
        assert!(
            GraphCatalog::lease_storage(&store, T, 9, "gw1", 30)
                .await
                .unwrap()
        );
        assert!(
            GraphCatalog::lease_storage(&store, T, 9, "gw1", 30)
                .await
                .unwrap()
        );
        assert!(
            !GraphCatalog::lease_storage(&store, T, 9, "gw2", 30)
                .await
                .unwrap()
        );
        // Giving back someone else's lease does nothing.
        GraphCatalog::release_storage(&store, T, 9, "gw2")
            .await
            .unwrap();
        assert!(
            !GraphCatalog::lease_storage(&store, T, 9, "gw2", 30)
                .await
                .unwrap()
        );
        GraphCatalog::release_storage(&store, T, 9, "gw1")
            .await
            .unwrap();
        assert!(
            GraphCatalog::lease_storage(&store, T, 9, "gw2", 0)
                .await
                .unwrap()
        );
        // A lease that ran out can be taken.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(
            GraphCatalog::lease_storage(&store, T, 9, "gw1", 30)
                .await
                .unwrap()
        );
    }

    #[test]
    fn ddl_text() {
        assert_eq!(
            parse_ddl("CREATE GRAPH social").expect("ok"),
            Some(GraphDdl::Create {
                db: None,
                name: "social".into(),
                if_not_exists: false
            })
        );
        assert_eq!(
            parse_ddl("create graph if not exists shop.`my graph`;").expect("ok"),
            Some(GraphDdl::Create {
                db: Some("shop".into()),
                name: "my graph".into(),
                if_not_exists: true
            })
        );
        assert_eq!(
            parse_ddl("DROP GRAPH IF EXISTS g").expect("ok"),
            Some(GraphDdl::Drop {
                db: None,
                name: "g".into(),
                if_exists: true
            })
        );
        assert_eq!(parse_ddl("SELECT 1").expect("ok"), None);
        assert_eq!(
            parse_ddl("CREATE PROPERTY GRAPH shop VERTEX TABLES (x)")
                .expect_err("no key")
                .code(),
            Some(SprigCode::GraphDefinitionInvalid)
        );
        assert!(matches!(
            parse_ddl("CREATE PROPERTY GRAPH shop VERTEX TABLES (x KEY (id))"),
            Ok(Some(GraphDdl::CreateProperty(_)))
        ));
        assert_eq!(
            parse_ddl("CREATE GRAPH __sprig_x")
                .expect_err("reserved")
                .code(),
            Some(SprigCode::GraphDefinitionInvalid)
        );
        assert_eq!(
            parse_ddl("CREATE GRAPH a b c").expect_err("bad").code(),
            Some(SprigCode::GraphSyntax)
        );
        let t = Tables::of("t1_d2_g3_s1", 901);
        assert!(t.nodes.ends_with("`__sprig_gn_901`"));
        assert_eq!(t.create_ddl().len(), 7);
        assert_eq!(t.shard_ddl().len(), 5);
        assert!(t.mirrors.ends_with("`__sprig_gm_901`"));
    }
}
