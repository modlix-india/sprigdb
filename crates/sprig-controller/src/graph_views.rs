//! `sprigctl graph list|show`: a tenant's graphs as the operator sees them,
//! read from the tenant's catalog records (the graph rows, the label and
//! type rows, the indexes) and the core rows' names. Graphs are stored on
//! the engine, so there are no shards to count or measure.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

/// One graph in `sprigctl graph list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRow {
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The logical database.
    pub database: String,
    /// The graph's name.
    pub name: String,
    /// `native` or `mapped`.
    pub kind: String,
    /// `active`, ...
    pub state: String,
    /// The table group of a native graph, when it has one (graphs made
    /// before graphs moved onto the engine).
    pub group: Option<u32>,
}

/// An index or constraint in `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphIndexRow {
    /// The name.
    pub name: String,
    /// `range` or `text`.
    pub kind: String,
    /// A uniqueness constraint.
    pub unique: bool,
    /// `node` or `relationship`.
    pub entity: String,
    /// The label or type.
    pub token: String,
    /// The properties.
    pub properties: Vec<String>,
    /// `building` or `online`.
    pub state: String,
}

/// `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphView {
    /// The graph.
    pub graph: GraphRow,
    /// Labels.
    pub labels: Vec<String>,
    /// Relationship types.
    pub types: Vec<String>,
    /// Indexes and constraints.
    pub indexes: Vec<GraphIndexRow>,
    /// Who keeps the mirrors and their state, `<mirror_mode>/<mirror_state>`
    /// (`sync/ready`, `starting/ready`, `async/ready`, `sync/building`;
    /// absent before the catalog's graph migration 3.
    #[serde(default)]
    pub mirror: Option<String>,
}

/// A graph's catalog record (`m/graph/graphs/<id>` on the engine node):
/// the fields read here.
#[derive(Debug, Clone, Deserialize)]
struct GraphRecord {
    graph_id: u64,
    kind: String,
    group_id: Option<u32>,
    state: String,
    #[serde(default)]
    mirror_mode: String,
    #[serde(default)]
    mirror_state: String,
}

/// A native label or relationship type record (`m/graph/nodes|edges/<graph><id>`).
#[derive(Debug, Clone, Deserialize)]
struct SourceRecord {
    name: String,
}

/// A property index record (`m/graph/indexes/<graph><id>`).
#[derive(Debug, Clone, Deserialize)]
struct IndexRecord {
    name: String,
    kind: String,
    unique: bool,
    entity: String,
    source_id: u32,
    properties: Vec<String>,
    state: String,
}

/// The source id at the end of a `<graph u64><id u32>` key.
fn source_id(pk: &sprig_catalog::records::Pk) -> Option<u32> {
    let b = pk.0.get(pk.0.len().checked_sub(4)?..)?;
    <[u8; 4]>::try_from(b).ok().map(u32::from_be_bytes)
}

/// The graph module's graph rows.
const GRAPH_RECORDS: sprig_catalog::records::RowTable =
    sprig_catalog::records::RowTable::new("graph", "graphs");

/// The graph views of the Admin API.
pub struct GraphViews {
    records: Arc<dyn sprig_catalog::RecordStore>,
}

impl std::fmt::Debug for GraphViews {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphViews").finish_non_exhaustive()
    }
}

impl GraphViews {
    /// Over the tenants' catalog records.
    #[must_use]
    pub fn new(records: Arc<dyn sprig_catalog::RecordStore>) -> Self {
        Self { records }
    }

    async fn graph_rows(&self, tenant: TenantId) -> Result<Vec<GraphRow>> {
        let graphs: Vec<GraphRecord> = {
            let mut rec = self.records.begin(tenant).await?;
            GRAPH_RECORDS
                .rows(&mut *rec, &sprig_catalog::records::Pk::new())
                .await?
        };
        if graphs.is_empty() {
            return Ok(Vec::new());
        }
        // Names from the core rows (catalog records).
        let reader = sprig_catalog::store::rec::CoreReader::new(Arc::clone(&self.records), tenant);
        let ids: Vec<sprig_common::ids::ObjectId> = graphs
            .iter()
            .map(|g| sprig_common::ids::ObjectId(g.graph_id))
            .collect();
        let dbs: std::collections::HashMap<u32, String> = reader
            .databases()
            .await?
            .into_iter()
            .map(|d| (d.db_id.0, d.name))
            .collect();
        let names: std::collections::HashMap<u64, (String, String)> = reader
            .objects_by_ids(&ids)
            .await?
            .into_iter()
            .filter(|o| o.state != sprig_catalog::store::ObjectState::Dropped)
            .filter_map(|o| Some((o.object_id.0, (dbs.get(&o.db_id.0)?.clone(), o.name))))
            .collect();
        let mut rows: Vec<GraphRow> = graphs
            .into_iter()
            .filter_map(|g| {
                let (database, name) = names.get(&g.graph_id)?.clone();
                Some(GraphRow {
                    graph_id: g.graph_id,
                    database,
                    name,
                    kind: g.kind,
                    state: g.state,
                    group: g.group_id,
                })
            })
            .collect();
        rows.sort_by(|a, b| (&a.database, &a.name).cmp(&(&b.database, &b.name)));
        Ok(rows)
    }

    async fn find_graph(&self, tenant: TenantId, graph: &str) -> Result<GraphRow> {
        let (db, name) = graph.split_once('.').ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("{graph}: name the graph as database.graph"),
            )
        })?;
        self.graph_rows(tenant)
            .await?
            .into_iter()
            .find(|g| g.database == db && g.name == name)
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::GraphNotFound,
                    format!("Graph '{graph}' doesn't exist"),
                )
            })
    }

    /// `sprigctl graph list <tenant>`.
    ///
    /// # Errors
    /// Store and catalog errors.
    pub async fn graphs(&self, tenant: TenantId) -> Result<Vec<GraphRow>> {
        self.graph_rows(tenant).await
    }

    /// `sprigctl graph show <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 for an unknown graph; store and catalog errors.
    pub async fn graph_show(&self, tenant: TenantId, graph: &str) -> Result<GraphView> {
        let g = self.find_graph(tenant, graph).await?;
        let id = g.graph_id;
        let records = &self.records;
        let mut rec = records.begin(tenant).await?;
        let under = sprig_catalog::records::Pk::new().u64(id);
        let table = |t| sprig_catalog::records::RowTable::new("graph", t);
        let by_id = |rows: Vec<(sprig_catalog::records::Pk, SourceRecord)>| {
            rows.into_iter()
                .filter_map(|(pk, r)| source_id(&pk).map(|i| (i, r.name)))
                .collect::<std::collections::BTreeMap<u32, String>>()
        };
        let label_ids = by_id(table("nodes").scan(&mut *rec, &under).await?);
        let type_ids = by_id(table("edges").scan(&mut *rec, &under).await?);
        let mut labels: Vec<String> = label_ids.values().cloned().collect();
        labels.sort();
        let mut types: Vec<String> = type_ids.values().cloned().collect();
        types.sort();
        let mut idx: Vec<IndexRecord> = table("indexes").rows(&mut *rec, &under).await?;
        idx.sort_by(|a, b| a.name.cmp(&b.name));
        let mirror = GRAPH_RECORDS
            .get::<GraphRecord>(&mut *rec, &under)
            .await?
            .map(|r| format!("{}/{}", r.mirror_mode, r.mirror_state));
        drop(rec);
        let indexes = idx
            .into_iter()
            .map(|i| {
                let names = if i.entity == "node" {
                    &label_ids
                } else {
                    &type_ids
                };
                GraphIndexRow {
                    token: names
                        .get(&i.source_id)
                        .cloned()
                        .unwrap_or_else(|| i.source_id.to_string()),
                    name: i.name,
                    kind: i.kind,
                    unique: i.unique,
                    entity: i.entity,
                    properties: i.properties,
                    state: i.state,
                }
            })
            .collect();
        Ok(GraphView {
            graph: g,
            labels,
            types,
            indexes,
            mirror,
        })
    }
}
