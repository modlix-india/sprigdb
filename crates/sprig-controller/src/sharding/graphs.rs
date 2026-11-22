//! `sprigctl graph list|show|partition`: a tenant's graphs as the operator sees them, read from the
//! tenant catalog (`graphs`, `objects`, the label and type rows, the
//! indexes) and from the graph's shards.
//!
//! - `list`: every graph with its database, kind, table group and shard
//!   count.
//! - `show`: one graph: labels, relationship types, indexes and
//!   constraints, and each shard's cluster with its node, relationship,
//!   mirror and uniqueness-claim row counts.
//! - `partition`: how the graph is cut today (`part` = `id`, hash
//!   placement) and the share of relationships whose two ends live on
//!   different shards, from the edge tables' `dst_rk` against the map.
//!   A partitioning advisor (property scoring, label
//!   propagation) isn't built; this is its measurement.

use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::shard_schema;

use super::ShardingLoop;

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

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
    /// The table group of a native graph, when it has one.
    pub group: Option<u32>,
    /// Shards (1 for graphs without a group).
    pub shards: usize,
}

/// One shard of a graph in `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphShardRow {
    /// The shard id (1 for a graph without a group).
    pub shard: u32,
    /// Its cluster.
    pub cluster: String,
    /// Its schema.
    pub schema: String,
    /// Node rows.
    pub nodes: u64,
    /// Relationship rows (on their source's shard).
    pub relationships: u64,
    /// Mirror rows (on their destination's shard).
    pub mirrors: u64,
    /// Uniqueness claims routed here.
    pub claims: u64,
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
    /// Shards with their row counts (native graphs).
    pub shards: Vec<GraphShardRow>,
    /// Who keeps the mirrors and their state, `<mirror_mode>/<mirror_state>`
    /// (`sync/ready`, `starting/ready`, `async/ready`, `sync/building`;
    /// absent before the catalog's graph migration 3.
    #[serde(default)]
    pub mirror: Option<String>,
}

/// `sprigctl graph partition`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPartition {
    /// The graph.
    pub graph: GraphRow,
    /// How nodes are placed: `id` (hash of the node id) today.
    pub partition_by: String,
    /// Relationships counted.
    pub relationships: u64,
    /// Of which with their ends on different shards.
    pub cross_shard: u64,
    /// `cross_shard` / `relationships`, in percent.
    pub cross_shard_pct: f64,
    /// What the numbers suggest.
    pub advice: String,
}

/// A graph's catalog record (`m/graph/graphs/<id>` on the engine node):
/// the fields read here.
#[derive(Debug, Clone, Deserialize)]
struct GraphRecord {
    graph_id: u64,
    db_id: u32,
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

/// A live graph's object and database: graph name, database name, the
/// database's default group.
type GraphNames = (u64, String, String, u32);

/// A graph with its names: id, database id and name, graph name, kind,
/// state, group, the database's default group.
type GraphCatalogRow = (u64, u32, String, String, String, String, Option<u32>, u32);

/// A graph's catalog row and where its shards are.
struct Located {
    row: GraphRow,
    /// (shard, cluster, schema, first hash).
    shards: Vec<(u32, ClusterId, String, u64)>,
}

impl ShardingLoop {
    /// The tenant's home cluster (its catalog and default groups).
    async fn graph_home(&self, tenant: TenantId) -> Result<ClusterId> {
        let (tm, _) = self.tenant_maps.load(tenant).await?;
        Ok(sprig_shard::Placement::of(&tm)?.cluster)
    }

    async fn graph_rows(&self, tenant: TenantId) -> Result<Vec<Located>> {
        let home = self.graph_home(tenant).await?;
        let records = self.records.get().ok_or_else(|| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "this controller has no catalog records",
            )
        })?;
        let graphs: Vec<GraphRecord> = {
            let mut rec = records.begin(tenant).await?;
            GRAPH_RECORDS
                .rows(&mut *rec, &sprig_catalog::records::Pk::new())
                .await?
        };
        if graphs.is_empty() {
            return Ok(Vec::new());
        }
        // Names from the core rows (catalog records).
        let reader =
            sprig_catalog::store::rec::CoreReader::new(std::sync::Arc::clone(records), tenant);
        let ids: Vec<sprig_common::ids::ObjectId> = graphs
            .iter()
            .map(|g| sprig_common::ids::ObjectId(g.graph_id))
            .collect();
        let dbs: std::collections::HashMap<u32, (String, u32)> = reader
            .databases()
            .await?
            .into_iter()
            .map(|d| (d.db_id.0, (d.name, d.default_group_id.0)))
            .collect();
        let names: Vec<GraphNames> = reader
            .objects_by_ids(&ids)
            .await?
            .into_iter()
            .filter(|o| o.state != sprig_catalog::store::ObjectState::Dropped)
            .filter_map(|o| {
                let (db, dg) = dbs.get(&o.db_id.0)?.clone();
                Some((o.object_id.0, o.name, db, dg))
            })
            .collect();
        let names: std::collections::HashMap<u64, (String, String, u32)> = names
            .into_iter()
            .map(|(id, name, db, dg)| (id, (db, name, dg)))
            .collect();
        let mut rows: Vec<GraphCatalogRow> = graphs
            .into_iter()
            .filter_map(|g| {
                let (dbname, name, dg) = names.get(&g.graph_id)?.clone();
                Some((
                    g.graph_id, g.db_id, dbname, name, g.kind, g.state, g.group_id, dg,
                ))
            })
            .collect();
        rows.sort_by(|a, b| (&a.2, &a.3).cmp(&(&b.2, &b.3)));
        let mut out = Vec::new();
        for (id, db, dbname, name, kind, state, group, default_group) in rows {
            let shards = match group {
                Some(g) => match self.maps.load(tenant, GroupId(g)).await {
                    Ok((m, _)) => m
                        .ranges
                        .iter()
                        .map(|r| {
                            (
                                r.shard.0,
                                r.cluster.clone(),
                                shard_schema(tenant, DbId(db), GroupId(g), r.shard),
                                r.lo,
                            )
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                },
                None => vec![(
                    1,
                    home.clone(),
                    shard_schema(tenant, DbId(db), GroupId(default_group), ShardId(1)),
                    0,
                )],
            };
            out.push(Located {
                row: GraphRow {
                    graph_id: id,
                    database: dbname,
                    name,
                    kind,
                    state,
                    group,
                    shards: shards.len(),
                },
                shards,
            });
        }
        Ok(out)
    }

    async fn find_graph(&self, tenant: TenantId, graph: &str) -> Result<Located> {
        let (db, name) = graph.split_once('.').ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("{graph}: name the graph as database.graph"),
            )
        })?;
        self.graph_rows(tenant)
            .await?
            .into_iter()
            .find(|g| g.row.database == db && g.row.name == name)
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
        Ok(self
            .graph_rows(tenant)
            .await?
            .into_iter()
            .map(|g| g.row)
            .collect())
    }

    async fn count(&self, cluster: &ClusterId, schema: &str, table: &str) -> u64 {
        let Ok(mut c) = self.conn(cluster).await else {
            return 0;
        };
        c.query_first::<u64, _>(format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote(schema),
            quote(table)
        ))
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
    }

    /// `sprigctl graph show <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 for an unknown graph; store and catalog errors.
    pub async fn graph_show(&self, tenant: TenantId, graph: &str) -> Result<GraphView> {
        let g = self.find_graph(tenant, graph).await?;
        let id = g.row.graph_id;
        let records = self.records.get().ok_or_else(|| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "this controller has no catalog records",
            )
        })?;
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
        let mut shards = Vec::new();
        if g.row.kind == "native" {
            for (shard, cluster, schema, _) in &g.shards {
                shards.push(GraphShardRow {
                    shard: *shard,
                    cluster: cluster.to_string(),
                    schema: schema.clone(),
                    nodes: self
                        .count(cluster, schema, &format!("__sprig_gn_{id}"))
                        .await,
                    relationships: self
                        .count(cluster, schema, &format!("__sprig_ge_{id}"))
                        .await,
                    mirrors: self
                        .count(cluster, schema, &format!("__sprig_gm_{id}"))
                        .await,
                    claims: self
                        .count(cluster, schema, &format!("__sprig_gu_{id}"))
                        .await,
                });
            }
        }
        Ok(GraphView {
            graph: g.row,
            labels,
            types,
            indexes,
            shards,
            mirror,
        })
    }

    /// `sprigctl graph partition <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 for an unknown graph; store errors.
    pub async fn graph_partition(&self, tenant: TenantId, graph: &str) -> Result<GraphPartition> {
        let g = self.find_graph(tenant, graph).await?;
        if g.row.kind != "native" {
            return Err(SprigError::new(
                SprigCode::GraphReadOnlySource,
                format!("{graph} is a mapped graph: its tables' own sharding places it"),
            ));
        }
        let id = g.row.graph_id;
        let (mut total, mut cross) = (0u64, 0u64);
        for (i, (_, cluster, schema, lo)) in g.shards.iter().enumerate() {
            let hi = g.shards.get(i + 1).map(|s| s.3);
            let outside = match hi {
                Some(h) => format!("dst_rk < {lo} OR dst_rk >= {h}"),
                None => format!("dst_rk < {lo}"),
            };
            let mut c = self.conn(cluster).await?;
            let r: Option<(u64, u64)> = c
                .query_first(format!(
                    "SELECT COUNT(*), COALESCE(SUM({outside}), 0) FROM {}.{}",
                    quote(schema),
                    quote(&format!("__sprig_ge_{id}"))
                ))
                .await
                .map_err(|e| db_err("edges", &e))?;
            if let Some((t, x)) = r {
                total += t;
                cross += x;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let pct = if total == 0 {
            0.0
        } else {
            (cross as f64) * 100.0 / (total as f64)
        };
        let advice = if g.shards.len() < 2 {
            "one shard: every hop is local".to_owned()
        } else if pct > 50.0 {
            "most hops cross shards: a partition property or communities (graph.auto_partition, not built yet) would cut them".to_owned()
        } else {
            "hash placement keeps most hops local".to_owned()
        };
        Ok(GraphPartition {
            graph: g.row,
            partition_by: "id".into(),
            relationships: total,
            cross_shard: cross,
            cross_shard_pct: (pct * 10.0).round() / 10.0,
            advice,
        })
    }
}
