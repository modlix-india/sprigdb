//! [`GraphStore`] over typed tables ([`super`]), on the statement's
//! connections and transaction.
//!
//! - Nodes live on the shard of their id's routing hash, in their label
//!   set's table; edges on their source's shard, in their type's table;
//!   mirrors on their destination's shard, kept by the writers (in their
//!   transaction).
//! - Every read names the columns the schema knows and reads the values
//!   table only for rows whose `_x` says they have values there.
//! - A write first makes what the schema lacks ([`super::catalog::TypedDdl`]):
//!   new label sets' and types' tables with their columns; new columns of
//!   tables this transaction hasn't touched (else the values go to the
//!   values table).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Value as MyValue};
use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};

use super::catalog::{TypedDdl, Want};
use super::codec::{self, Encoded, ItemRow, Val};
use super::{Code, Owner, TypedSchema};
use crate::ast::{Dir, Entity, Schema};
use crate::catalog::backend_error;
use crate::maria::{
    CounterIds, Dictionary, GraphShards, SchemaOps, ShardConns, ShardLoc, inline_params,
};
use crate::store::{
    EdgeKey, EdgeRow, FarLink, FarRead, GraphStore, IdKind, IndexDef, Link, NodeRow, Reach, Scan,
    SchemaCounts, StoreStats, rk_of,
};

/// Ids per statement.
const IDS_PER_STATEMENT: usize = 1000;

/// One native graph in typed tables, on `conns`.
pub struct TypedStore<'c> {
    conns: &'c mut dyn ShardConns,
    layout: Arc<GraphShards>,
    schema: TypedSchema,
    ddl: Arc<dyn TypedDdl>,
    labels: BTreeMap<u32, String>,
    types: BTreeMap<u32, String>,
    dict: Arc<dyn Dictionary>,
    ids: Arc<CounterIds>,
    stats: StoreStats,
    now_us: u64,
    indexes: Vec<IndexDef>,
    ops: Option<Arc<dyn SchemaOps>>,
    /// `_v` per element id for a JSON graph's move (the JSON row's).
    copy_v: HashMap<u64, u64>,
    /// Writers keep the mirrors whatever the mirror mode (a move).
    force_mirrors: bool,
    /// The applier's mirrors' freshness gate (`mirror_mode = async`).
    gate: Option<Arc<dyn crate::mirror::MirrorGate>>,
    /// The gate's answer, asked once per statement.
    mirror_ok: Option<bool>,
    /// Tables this transaction has read or written, by shard: their
    /// metadata locks are held, so no `ALTER` may wait for them.
    touched: HashSet<(usize, Owner)>,
}

impl std::fmt::Debug for TypedStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedStore")
            .field("graph", &self.layout.graph)
            .finish_non_exhaustive()
    }
}

fn ids_list(ids: &[u64]) -> String {
    let mut s = String::with_capacity(ids.len() * 8);
    for (i, x) in ids.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{x}");
    }
    s
}

fn u64_col(v: Option<MyValue>) -> u64 {
    match v {
        Some(MyValue::UInt(u)) => u,
        Some(MyValue::Int(i)) => u64::try_from(i).unwrap_or(0),
        Some(MyValue::Bytes(b)) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

fn lit(v: &Val) -> String {
    match v {
        Val::Int(i) => i.to_string(),
        Val::Float(f) => format!("{f:e}"),
        Val::Bool(b) => i64::from(*b).to_string(),
        Val::Text(s) => hex("_utf8mb4 X'", s.as_bytes()),
        Val::Bytes(b) => hex("X'", b),
    }
}

fn hex(prefix: &str, b: &[u8]) -> String {
    let mut o = String::with_capacity(b.len() * 2 + prefix.len() + 1);
    o.push_str(prefix);
    for x in b {
        let _ = write!(o, "{x:02X}");
    }
    o.push('\'');
    o
}

/// Values-table rows (`id, key_id, ord, code, v_i, v_f, v_s, v_b`) by
/// element id.
fn items_of(rows: Vec<Row>, names: &HashMap<u32, String>) -> HashMap<u64, Vec<ItemRow>> {
    let mut out: HashMap<u64, Vec<ItemRow>> = HashMap::new();
    for mut r in rows {
        let id = u64_col(r.take(0));
        let key = u32::try_from(u64_col(r.take(1))).unwrap_or(0);
        let ord = u32::try_from(u64_col(r.take(2))).unwrap_or(0);
        let code_s: String = r.take(3).unwrap_or_default();
        let code = Code::of(&code_s);
        let val = code.and_then(|c| {
            let raw: Option<MyValue> = match c {
                Code::Int | Code::Bool => r.take(4),
                Code::Float => r.take(5),
                Code::Typed(super::TypedKind::Bytes) => r.take(7),
                _ => r.take(6),
            };
            raw.and_then(|v| codec::read_val(c, v))
        });
        if let Some(n) = names.get(&key) {
            out.entry(id).or_default().push(ItemRow {
                key: n.clone(),
                ord,
                code,
                val,
            });
        }
    }
    out
}

/// A row for [`TypedStore::insert_rows`]: its fixed columns' values, and
/// its scalars by column.
type BatchRow = (Vec<MyValue>, Vec<((u32, Code), MyValue)>);

/// Rows per multi-row `INSERT`.
const ROWS_PER_INSERT: usize = 500;

/// A JSON graph's writes made again in its typed tables while it moves
/// to them ([`crate::maria::MariaStore::with_shadow`]): a typed store's
/// parts, lent the writer's connections for each write, with the same
/// `_v` as the JSON write and the mirrors kept in the transaction.
pub struct Shadow {
    layout: Arc<GraphShards>,
    schema: std::sync::Mutex<TypedSchema>,
    ddl: Arc<dyn TypedDdl>,
    labels: BTreeMap<u32, String>,
    types: BTreeMap<u32, String>,
    dict: Arc<dyn Dictionary>,
    ids: Arc<CounterIds>,
    indexes: Vec<IndexDef>,
}

impl std::fmt::Debug for Shadow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shadow")
            .field("graph", &self.layout.graph)
            .finish_non_exhaustive()
    }
}

impl Shadow {
    /// The typed side of graph `layout`.
    #[allow(clippy::too_many_arguments)] // a typed store's parts
    #[must_use]
    pub fn new(
        layout: Arc<GraphShards>,
        schema: TypedSchema,
        ddl: Arc<dyn TypedDdl>,
        labels: BTreeMap<u32, String>,
        types: BTreeMap<u32, String>,
        dict: Arc<dyn Dictionary>,
        ids: Arc<CounterIds>,
        indexes: Vec<IndexDef>,
    ) -> Self {
        Self {
            layout,
            schema: std::sync::Mutex::new(schema),
            ddl,
            labels,
            types,
            dict,
            ids,
            indexes,
        }
    }

    /// A typed store over `conns` writing with `_v` = `now_us`.
    #[must_use]
    pub fn store<'c>(&self, conns: &'c mut dyn ShardConns, now_us: u64) -> TypedStore<'c> {
        let schema = self
            .schema
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        TypedStore::new(
            conns,
            Arc::clone(&self.layout),
            schema,
            Arc::clone(&self.ddl),
            self.labels.clone(),
            self.types.clone(),
            Arc::clone(&self.dict),
            Arc::clone(&self.ids),
        )
        .with_schema(self.indexes.clone(), None)
        .with_now(now_us)
        .sync_mirrors()
    }

    /// Keeps what `store`'s writes added to the schema.
    pub fn keep(&self, store: &TypedStore<'_>) {
        self.schema
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .merge(store.typed_schema());
    }
}

/// Rows per page of a JSON graph's move.
const COPY_PAGE: usize = 500;

/// One page of a JSON graph's move ([`TypedStore::copy_page`],
/// [`TypedStore::prune_page`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CopyPage {
    /// Rows looked at.
    pub read: usize,
    /// Rows written (or deleted).
    pub written: usize,
    /// The last id looked at (`None`: nothing was left).
    pub last: Option<u64>,
}

/// A class and the columns a read takes from its table.
type ClassCols = (u32, Vec<(u32, Code)>);

/// An edge's `(src, dst, id)`.
type EdgeAt = (u64, u64, u64);

/// A row of an element read: its id, hash and columns.
struct Read {
    id: u64,
    rk: u64,
    /// The row has values in the values table.
    x: bool,
    scalars: Vec<(String, Code, Val)>,
}

impl<'c> TypedStore<'c> {
    /// The graph of `layout` in typed tables, on `conns`, with the
    /// catalog's labels, types and typed schema.
    #[allow(clippy::too_many_arguments)] // the store's parts, as MariaStore takes them
    #[must_use]
    pub fn new(
        conns: &'c mut dyn ShardConns,
        layout: Arc<GraphShards>,
        schema: TypedSchema,
        ddl: Arc<dyn TypedDdl>,
        labels: BTreeMap<u32, String>,
        types: BTreeMap<u32, String>,
        dict: Arc<dyn Dictionary>,
        ids: Arc<CounterIds>,
    ) -> Self {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0));
        Self {
            conns,
            layout,
            schema,
            ddl,
            labels,
            types,
            dict,
            ids,
            stats: StoreStats::default(),
            now_us,
            indexes: Vec::new(),
            ops: None,
            touched: HashSet::new(),
            gate: None,
            mirror_ok: None,
            copy_v: HashMap::new(),
            force_mirrors: false,
        }
    }

    /// Writes mirrors in the writer's transaction whatever the graph's
    /// mirror mode (a JSON graph's move: its applier keeps the JSON
    /// mirrors only).
    #[must_use]
    pub fn sync_mirrors(mut self) -> Self {
        self.force_mirrors = true;
        self
    }

    /// Writes with `now_us` as their `_v` (a JSON graph's move: the typed
    /// copy of a write carries the JSON row's `_v`).
    #[must_use]
    pub fn with_now(mut self, now_us: u64) -> Self {
        self.now_us = now_us;
        self
    }

    /// The typed schema as this store knows it now (after the tables and
    /// columns its writes made).
    #[must_use]
    pub fn typed_schema(&self) -> &TypedSchema {
        &self.schema
    }

    /// One page of a JSON graph's move on shard `i` (`entity`'s rows with
    /// ids above `after`, in id order, share-locked in this store's
    /// transaction): rows the typed tables lack, or hold with another
    /// `_v`, are written there with the JSON row's `_v`. Answers the rows
    /// read, the rows written and the last id.
    ///
    /// # Errors
    /// Backend errors.
    pub async fn copy_page(&mut self, i: usize, entity: Entity, after: u64) -> Result<CopyPage> {
        let t = crate::catalog::Tables::of(&self.schema_name(i), self.layout.graph);
        let mut page = CopyPage::default();
        match entity {
            Entity::Node => {
                let rows = self
                    .multi_at(
                        i,
                        &[format!(
                            "SELECT id, _sprig_rk, labels, props, _types, _v FROM {} \
                             WHERE id > {after} ORDER BY id LIMIT {COPY_PAGE} LOCK IN SHARE MODE",
                            t.nodes
                        )],
                        true,
                    )
                    .await?
                    .pop()
                    .unwrap_or_default();
                let mut json: Vec<(NodeRow, u64)> = Vec::with_capacity(rows.len());
                for mut r in rows {
                    let v = u64_col(r.take(5));
                    if let Some(n) = crate::maria::node_at(&mut r, 0) {
                        json.push((n, v));
                    }
                }
                page.read = json.len();
                page.last = json.last().map(|(n, _)| n.id);
                let ids: Vec<u64> = json.iter().map(|(n, _)| n.id).collect();
                let have = self.typed_versions(i, Entity::Node, &ids).await?;
                let mut fresh = Vec::new();
                for (n, v) in json {
                    match have.get(&n.id) {
                        Some(tv) if *tv == v => {}
                        Some(_) => {
                            // Another `_v`: written before the writers wrote
                            // both; the JSON row wins.
                            self.copy_v.insert(n.id, v);
                            let old = self.class_now(n.id).await?;
                            let old_labels = old
                                .and_then(|c| self.schema.classes.get(&c).cloned())
                                .unwrap_or_default();
                            self.update_node(&n, &old_labels).await?;
                            page.written += 1;
                        }
                        None => {
                            self.copy_v.insert(n.id, v);
                            fresh.push(n);
                        }
                    }
                }
                page.written += fresh.len();
                self.insert_nodes(&fresh).await?;
            }
            Entity::Relationship => {
                let rows = self
                    .multi_at(
                        i,
                        &[format!(
                            "SELECT src, type, dst, id, dst_rk, _sprig_rk, props, _types, _v FROM {} \
                             WHERE id > {after} ORDER BY id LIMIT {COPY_PAGE} LOCK IN SHARE MODE",
                            t.edges
                        )],
                        true,
                    )
                    .await?
                    .pop()
                    .unwrap_or_default();
                let mut json: Vec<(EdgeRow, u64)> = Vec::with_capacity(rows.len());
                for mut r in rows {
                    let v = u64_col(r.take(8));
                    json.push((crate::maria::edge_at(&mut r, 0), v));
                }
                page.read = json.len();
                page.last = json.last().map(|(e, _)| e.id);
                let ids: Vec<u64> = json.iter().map(|(e, _)| e.id).collect();
                let have = self.typed_versions(i, Entity::Relationship, &ids).await?;
                let mut fresh = Vec::new();
                for (e, v) in json {
                    match have.get(&e.id) {
                        Some(tv) if *tv == v => {}
                        Some(_) => {
                            self.copy_v.insert(e.id, v);
                            self.update_edge(&e).await?;
                            page.written += 1;
                        }
                        None => {
                            self.copy_v.insert(e.id, v);
                            fresh.push(e);
                        }
                    }
                }
                page.written += fresh.len();
                self.insert_edges(&fresh).await?;
            }
        }
        Ok(page)
    }

    /// `_v` of the typed rows of `ids` on shard `i`.
    async fn typed_versions(
        &mut self,
        i: usize,
        entity: Entity,
        ids: &[u64],
    ) -> Result<HashMap<u64, u64>> {
        let mut out = HashMap::new();
        if ids.is_empty() {
            return Ok(out);
        }
        let l = ids_list(ids);
        let g = self.layout.graph;
        let schema = self.schema_name(i);
        let stmts: Vec<String> = match entity {
            Entity::Node => self
                .schema
                .classes
                .keys()
                .map(|c| {
                    format!(
                        "SELECT id, _v FROM {} WHERE id IN ({l})",
                        super::node_table(&schema, g, *c)
                    )
                })
                .collect(),
            Entity::Relationship => self
                .schema
                .types
                .iter()
                .map(|t| {
                    format!(
                        "SELECT id, _v FROM {} WHERE id IN ({l})",
                        super::edge_table(&schema, g, *t)
                    )
                })
                .collect(),
        };
        for rows in self.multi_at(i, &stmts, false).await? {
            for mut r in rows {
                out.insert(u64_col(r.take(0)), u64_col(r.take(1)));
            }
        }
        Ok(out)
    }

    /// Typed rows on shard `i` whose JSON row is gone (deleted before the
    /// writers wrote both): one page of ids above `after` of the directory
    /// (`Entity::Node`) or of type `ty`'s table, those gone deleted.
    /// Answers the rows looked at, the rows deleted and the last id.
    ///
    /// # Errors
    /// Backend errors.
    pub async fn prune_page(
        &mut self,
        i: usize,
        entity: Entity,
        ty: u32,
        after: u64,
    ) -> Result<CopyPage> {
        let t = crate::catalog::Tables::of(&self.schema_name(i), self.layout.graph);
        let g = self.layout.graph;
        let schema = self.schema_name(i);
        let mut page = CopyPage::default();
        let own = match entity {
            Entity::Node => super::dir_table(&schema, g),
            Entity::Relationship => super::edge_table(&schema, g, ty),
        };
        let json = match entity {
            Entity::Node => t.nodes,
            Entity::Relationship => t.edges,
        };
        let (inner, cols) = match entity {
            Entity::Node => ("id", "p.id, 0, 0, 0, 0"),
            Entity::Relationship => (
                "id, src, dst, _sprig_rk AS rk, dst_rk",
                "p.id, p.src, p.dst, p.rk, p.dst_rk",
            ),
        };
        let sets = self
            .multi_at(
                i,
                &[format!(
                    "SELECT {cols}, j.id IS NULL FROM (SELECT {inner} FROM {own} WHERE id > {after} \
                     ORDER BY id LIMIT {COPY_PAGE}) p LEFT JOIN {json} j ON j.id = p.id ORDER BY p.id"
                )],
                true,
            )
            .await?;
        let rows = sets.into_iter().next().unwrap_or_default();
        page.read = rows.len();
        let mut gone_nodes = Vec::new();
        let mut gone_edges = Vec::new();
        for mut r in rows {
            let id = u64_col(r.take(0));
            page.last = Some(id);
            if u64_col(r.take(5)) == 0 {
                continue;
            }
            match entity {
                Entity::Node => gone_nodes.push(id),
                Entity::Relationship => gone_edges.push(EdgeKey {
                    id,
                    src: u64_col(r.take(1)),
                    dst: u64_col(r.take(2)),
                    rtype: ty,
                    src_rk: u64_col(r.take(3)),
                    dst_rk: u64_col(r.take(4)),
                }),
            }
        }
        if !gone_nodes.is_empty() {
            let links = self.edges_touching(&gone_nodes, usize::MAX).await?;
            let keys: Vec<EdgeKey> = links.iter().map(EdgeKey::from).collect();
            self.delete_edges(&keys).await?;
            page.written = usize::try_from(self.delete_nodes(&gone_nodes).await?).unwrap_or(0);
        }
        if !gone_edges.is_empty() {
            page.written = usize::try_from(self.delete_edges(&gone_edges).await?).unwrap_or(0);
        }
        Ok(page)
    }

    /// Claims every existing key of new constraint `d` (its catalog row
    /// says `building`, so writers claim already): each covered table on
    /// each shard, in pages of 1,000 ids. Answers the elements claimed.
    ///
    /// # Errors
    /// 1062 naming two elements with the same key; backend errors.
    pub async fn claim_all(&mut self, d: &IndexDef) -> Result<u64> {
        let owners: Vec<u32> = match d.entity {
            Entity::Node => self.schema.classes_with(Some(d.token_id)),
            Entity::Relationship => self
                .schema
                .types
                .iter()
                .copied()
                .filter(|t| *t == d.token_id)
                .collect(),
        };
        let keys = d.props.clone();
        let mut n = 0u64;
        for i in 0..self.layout.shards.len() {
            for &o in &owners {
                let mut after = 0u64;
                loop {
                    let part = [(o, format!("id > {after}"))];
                    let tail = " ORDER BY id LIMIT 1000";
                    let got: Vec<(u64, u64, Option<Vec<u8>>)> = match d.entity {
                        Entity::Node => self
                            .read_nodes(i, &part, tail, Some(&keys), false)
                            .await?
                            .iter()
                            .map(|r| (r.id, r.rk, Self::node_key(d, r)))
                            .collect(),
                        Entity::Relationship => self
                            .read_edges(i, &part, tail, Some(&keys))
                            .await?
                            .iter()
                            .map(|r| (r.id, r.src_rk, Self::edge_key(d, r)))
                            .collect(),
                    };
                    let Some(last) = got.iter().map(|g| g.0).max() else {
                        break;
                    };
                    let what = match d.entity {
                        Entity::Node => "Node",
                        Entity::Relationship => "Relationship",
                    };
                    for (id, rk, key) in &got {
                        if let Some(k) = key {
                            self.claim(d, k, what, *id, *rk).await?;
                            n += 1;
                        }
                    }
                    if got.len() < 1000 {
                        break;
                    }
                    after = last;
                }
            }
        }
        Ok(n)
    }

    /// The graph's indexes, and who runs schema commands.
    #[must_use]
    pub fn with_schema(mut self, indexes: Vec<IndexDef>, ops: Option<Arc<dyn SchemaOps>>) -> Self {
        self.indexes = indexes;
        self.ops = ops;
        self
    }

    fn shard(&self, i: usize) -> ShardLoc {
        self.layout.shards[i].clone()
    }

    fn schema_name(&self, i: usize) -> String {
        self.layout.shards[i].schema.clone()
    }

    /// Whether mirror rows exist to keep up to date (a label move's class,
    /// a deleted edge's row): more than one shard, with mirrors.
    fn mirrors(&self) -> bool {
        self.layout.shards.len() > 1 && self.layout.mirrors
    }

    /// Whether writers insert the mirrors in their own transaction (the
    /// applier does, under `mirror_mode = async`).
    fn write_mirrors(&self) -> bool {
        self.mirrors() && (self.force_mirrors || !self.layout.mirror_async)
    }

    /// `_v` for a write of element `id`.
    fn v_for(&self, id: u64) -> u64 {
        self.copy_v.get(&id).copied().unwrap_or(self.now_us)
    }

    /// Asynchronous mirrors may be read when `gate` says they are fresh
    /// (read statements; without a gate incoming hops scatter).
    #[must_use]
    pub fn with_gate(mut self, gate: Option<Arc<dyn crate::mirror::MirrorGate>>) -> Self {
        self.gate = gate;
        self
    }

    /// Whether incoming hops read the mirrors: kept by the writers, or
    /// the applier's and fresh for this statement.
    async fn read_mirrors(&mut self) -> bool {
        if !self.mirrors() || self.layout.mirror_building {
            return false;
        }
        if !self.layout.mirror_async {
            return true;
        }
        if let Some(ok) = self.mirror_ok {
            return ok;
        }
        let ok = match &self.gate {
            Some(g) => g.fresh(&self.layout).await,
            None => false,
        };
        if !ok {
            let reason = if self.gate.is_some() { "lag" } else { "write" };
            metrics::counter!("sprig_graph_mirror_fallbacks_total", "reason" => reason)
                .increment(1);
        }
        self.mirror_ok = Some(ok);
        ok
    }

    fn key_names(&self) -> HashMap<u32, String> {
        self.schema
            .keys
            .iter()
            .map(|(n, k)| (*k, n.clone()))
            .collect()
    }

    /// Rows of a read on shard `i` (the transaction's connection to its
    /// cluster, which sees the transaction's own rows).
    async fn rows_at(&mut self, i: usize, sql: String) -> Result<Vec<Row>> {
        self.stats.shard_queries += 1;
        let at = self.shard(i);
        let t0 = std::time::Instant::now();
        let conn = self.conns.conn(&at, false).await?;
        let rows: Vec<Row> = conn.query(sql).await.map_err(backend_error)?;
        self.stats.sql_us += u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.stats.sql_rows += rows.len() as u64;
        Ok(rows)
    }

    /// The result sets of `stmts`, sent to shard `i` in one round trip
    /// (`write` for locking reads).
    async fn multi_at(&mut self, i: usize, stmts: &[String], write: bool) -> Result<Vec<Vec<Row>>> {
        if stmts.is_empty() {
            return Ok(Vec::new());
        }
        self.stats.shard_queries += 1;
        let at = self.shard(i);
        let t0 = std::time::Instant::now();
        let conn = self.conns.conn(&at, write).await?;
        let mut res = conn
            .query_iter(stmts.join(";\n"))
            .await
            .map_err(backend_error)?;
        let mut out = Vec::with_capacity(stmts.len());
        for _ in stmts {
            let rows: Vec<Row> = res.collect().await.map_err(backend_error)?;
            out.push(rows);
        }
        drop(res);
        self.stats.sql_us += u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.stats.sql_rows += out.iter().map(|r| r.len() as u64).sum::<u64>();
        Ok(out)
    }

    async fn run_at(&mut self, i: usize, sql: String, params: Params) -> Result<u64> {
        self.stats.shard_queries += 1;
        let at = self.shard(i);
        let text = self.conns.records().then(|| inline_params(&sql, &params));
        let conn = self.conns.conn(&at, true).await?;
        conn.exec_drop(sql, params).await.map_err(backend_error)?;
        let n = conn.affected_rows();
        if let Some(t) = text {
            self.conns.wrote(&at, &t);
        }
        Ok(n)
    }

    /// Ids by the shard of their node.
    fn by_shard(&self, ids: &[u64]) -> BTreeMap<usize, Vec<u64>> {
        self.layout.by_shard(ids)
    }

    /// The columns of `owner` that hold `keys` (`None`: all).
    fn cols_for(&self, owner: Owner, keys: Option<&[String]>) -> Vec<(u32, Code)> {
        let all = self.schema.columns_of(owner);
        match keys {
            None => all,
            Some(ks) => {
                let ids: HashSet<u32> = ks
                    .iter()
                    .filter_map(|k| self.schema.keys.get(k))
                    .copied()
                    .collect();
                all.into_iter().filter(|(k, _)| ids.contains(k)).collect()
            }
        }
    }

    /// `AND key_id IN (…)` for `keys` (`None`: nothing); `None` when no
    /// key is known (nothing to read).
    fn key_filter(&self, keys: Option<&[String]>) -> Option<String> {
        match keys {
            None => Some(String::new()),
            Some(ks) => {
                let ids: Vec<String> = ks
                    .iter()
                    .filter_map(|k| self.schema.keys.get(k))
                    .map(ToString::to_string)
                    .collect();
                (!ids.is_empty()).then(|| format!(" AND v.key_id IN ({})", ids.join(",")))
            }
        }
    }

    /// The result sets of each batch of statements (each on its shard),
    /// sent as one round trip per cluster: a cluster's shard schemas are
    /// on one server, so its batches go in one multi-statement query.
    async fn multi_shards(
        &mut self,
        batches: Vec<(usize, Vec<String>)>,
        write: bool,
    ) -> Result<Vec<Vec<Vec<Row>>>> {
        let mut out: Vec<Vec<Vec<Row>>> = batches.iter().map(|_| Vec::new()).collect();
        let mut by_cluster: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (k, (i, _)) in batches.iter().enumerate() {
            by_cluster
                .entry(self.layout.shards[*i].cluster.clone())
                .or_default()
                .push(k);
        }
        for ks in by_cluster.into_values() {
            let at = batches[ks[0]].0;
            let mut stmts = Vec::new();
            for &k in &ks {
                stmts.extend(batches[k].1.iter().cloned());
            }
            let mut sets = self.multi_at(at, &stmts, write).await?.into_iter();
            for &k in &ks {
                let n = batches[k].1.len();
                out[k] = sets.by_ref().take(n).collect();
            }
        }
        Ok(out)
    }

    /// The values-table rows of elements `ids` (by shard), of `kind`
    /// (`n` or `e`), with `keys` (`None`: all): read only for rows whose
    /// `_x` flag said they have some, so most reads never come here.
    async fn items_for(
        &mut self,
        kind: char,
        ids: BTreeMap<usize, Vec<u64>>,
        keys: Option<&[String]>,
    ) -> Result<HashMap<u64, Vec<ItemRow>>> {
        let Some(kf) = self.key_filter(keys) else {
            return Ok(HashMap::new());
        };
        let g = self.layout.graph;
        let mut batches = Vec::new();
        for (i, part) in ids {
            let vt = super::values_table(&self.schema_name(i), g);
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                batches.push((
                    i,
                    vec![format!(
                        "SELECT v.id, v.key_id, v.ord, v.code, v.v_i, v.v_f, v.v_s, v.v_b FROM {vt} v \
                         WHERE v.kind = '{kind}' AND v.id IN ({}){kf}",
                        ids_list(chunk)
                    )],
                ));
            }
        }
        if batches.is_empty() {
            return Ok(HashMap::new());
        }
        let names = self.key_names();
        let mut out = HashMap::new();
        for sets in self.multi_shards(batches, false).await? {
            for rows in sets {
                for (id, v) in items_of(rows, &names) {
                    out.entry(id).or_insert_with(Vec::new).extend(v);
                }
            }
        }
        Ok(out)
    }

    /// Nodes on shard `i` (see [`Self::read_nodes_shards`]).
    async fn read_nodes(
        &mut self,
        i: usize,
        parts: &[(u32, String)],
        tail: &str,
        keys: Option<&[String]>,
        write: bool,
    ) -> Result<Vec<NodeRow>> {
        self.read_nodes_shards(&[(i, parts.to_vec())], tail, keys, write)
            .await
    }

    /// Nodes on shards: for each shard, for each `(class, condition)`, the
    /// rows of that class's table, with `keys` of their properties (`None`:
    /// all), and their values-table rows; one round trip per cluster.
    async fn read_nodes_shards(
        &mut self,
        reqs: &[(usize, Vec<(u32, String)>)],
        tail: &str,
        keys: Option<&[String]>,
        write: bool,
    ) -> Result<Vec<NodeRow>> {
        let g = self.layout.graph;
        let kf = self.key_filter(keys);
        let mut batches = Vec::with_capacity(reqs.len());
        let mut colsets: Vec<Vec<Vec<(u32, Code)>>> = Vec::with_capacity(reqs.len());
        for (i, parts) in reqs {
            if parts.is_empty() {
                continue;
            }
            let schema = self.schema_name(*i);
            let mut stmts = Vec::with_capacity(parts.len() + 1);
            let mut cs = Vec::with_capacity(parts.len());
            for (c, cond) in parts {
                let owner = (Entity::Node, *c);
                let cols = self.cols_for(owner, keys);
                let t = super::node_table(&schema, g, *c);
                let mut sel = String::from("id, _sprig_rk, _x");
                for (k, code) in &cols {
                    let _ = write!(sel, ", {}", super::column(*k, *code));
                }
                stmts.push(format!("SELECT {sel} FROM {t} WHERE {cond}{tail}"));
                cs.push(cols);
                self.touched.insert((*i, owner));
            }
            batches.push((*i, stmts));
            colsets.push(cs);
        }
        let reqs: Vec<&(usize, Vec<(u32, String)>)> =
            reqs.iter().filter(|(_, p)| !p.is_empty()).collect();
        let results = self.multi_shards(batches, write).await?;
        let names = self.key_names();
        let mut read: Vec<(Vec<u32>, Read)> = Vec::new();
        let mut flagged: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for ((req, cs), sets) in reqs.into_iter().zip(colsets).zip(results) {
            for (((c, _), cols), rows) in req.1.iter().zip(&cs).zip(sets) {
                let labels = self.schema.classes.get(c).cloned().unwrap_or_default();
                for (_, r) in Self::decode_rows(rows, cols, 0, &names) {
                    if r.x {
                        flagged.entry(req.0).or_default().push(r.id);
                    }
                    read.push((labels.clone(), r));
                }
            }
        }
        let mut items = if flagged.is_empty() || kf.is_none() {
            HashMap::new()
        } else {
            self.items_for('n', flagged, keys).await?
        };
        Ok(read
            .into_iter()
            .map(|(labels, r)| {
                let (props, types) =
                    codec::decode(r.scalars, items.remove(&r.id).unwrap_or_default());
                NodeRow {
                    id: r.id,
                    rk: r.rk,
                    labels,
                    props,
                    types,
                }
            })
            .collect())
    }

    /// Edges on shard `i` (see [`Self::read_edges_shards`]).
    async fn read_edges(
        &mut self,
        i: usize,
        parts: &[(u32, String)],
        tail: &str,
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        self.read_edges_shards(&[(i, parts.to_vec())], tail, keys)
            .await
    }

    /// Edges on shards: for each shard, for each `(type, condition)`, the
    /// rows of that type's table with `keys` of their properties; one
    /// round trip per cluster.
    async fn read_edges_shards(
        &mut self,
        reqs: &[(usize, Vec<(u32, String)>)],
        tail: &str,
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        let g = self.layout.graph;
        let kf = self.key_filter(keys);
        let mut batches = Vec::with_capacity(reqs.len());
        let mut colsets: Vec<Vec<Vec<(u32, Code)>>> = Vec::with_capacity(reqs.len());
        for (i, parts) in reqs {
            if parts.is_empty() {
                continue;
            }
            let schema = self.schema_name(*i);
            let mut stmts = Vec::with_capacity(parts.len() + 1);
            let mut cs = Vec::with_capacity(parts.len());
            for (t, cond) in parts {
                let owner = (Entity::Relationship, *t);
                let cols = self.cols_for(owner, keys);
                let tbl = super::edge_table(&schema, g, *t);
                let mut sel = String::from("src, dst, dst_rk, id, _sprig_rk, _x");
                for (k, code) in &cols {
                    let _ = write!(sel, ", {}", super::column(*k, *code));
                }
                stmts.push(format!("SELECT {sel} FROM {tbl} WHERE {cond}{tail}"));
                cs.push(cols);
                self.touched.insert((*i, owner));
            }
            batches.push((*i, stmts));
            colsets.push(cs);
        }
        let reqs: Vec<&(usize, Vec<(u32, String)>)> =
            reqs.iter().filter(|(_, p)| !p.is_empty()).collect();
        let results = self.multi_shards(batches, false).await?;
        let names = self.key_names();
        let mut read: Vec<(u32, u64, u64, u64, Read)> = Vec::new();
        let mut flagged: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for ((req, cs), sets) in reqs.into_iter().zip(colsets).zip(results) {
            for (((t, _), cols), rows) in req.1.iter().zip(&cs).zip(sets) {
                for (mut row, r) in Self::decode_rows(rows, cols, 3, &names) {
                    let src = u64_col(row.take(0));
                    let dst = u64_col(row.take(1));
                    let dst_rk = u64_col(row.take(2));
                    if r.x {
                        flagged.entry(req.0).or_default().push(r.id);
                    }
                    read.push((*t, src, dst, dst_rk, r));
                }
            }
        }
        let mut items = if flagged.is_empty() || kf.is_none() {
            HashMap::new()
        } else {
            self.items_for('e', flagged, keys).await?
        };
        Ok(read
            .into_iter()
            .map(|(t, src, dst, dst_rk, r)| {
                let (props, types) =
                    codec::decode(r.scalars, items.remove(&r.id).unwrap_or_default());
                EdgeRow {
                    id: r.id,
                    src,
                    dst,
                    src_rk: r.rk,
                    dst_rk,
                    rtype: t,
                    props,
                    types,
                }
            })
            .collect())
    }

    /// Element rows: `head` columns first (`id`, hash, `_x` at `at..`),
    /// then the owner's columns.
    fn decode_rows(
        rows: Vec<Row>,
        cols: &[(u32, Code)],
        first: usize,
        names: &HashMap<u32, String>,
    ) -> Vec<(Row, Read)> {
        rows.into_iter()
            .map(|mut r| {
                let id = u64_col(r.take(first));
                let rk = u64_col(r.take(first + 1));
                let x = u64_col(r.take(first + 2)) != 0;
                let mut scalars = Vec::new();
                for (j, (k, c)) in cols.iter().enumerate() {
                    let v: Option<MyValue> = r.take(first + 3 + j);
                    if let (Some(v), Some(n)) = (v, names.get(k))
                        && let Some(val) = codec::read_val(*c, v)
                    {
                        scalars.push((n.clone(), *c, val));
                    }
                }
                (r, Read { id, rk, x, scalars })
            })
            .collect()
    }

    /// Nodes by id on their shards: every class's table (a node is in
    /// one), each shard's in one round trip.
    async fn nodes_by_id(&mut self, ids: &[u64], lock: Option<bool>) -> Result<Vec<NodeRow>> {
        self.nodes_keys(ids, None, lock).await
    }

    /// [`Self::nodes_by_id`] with only `keys` of their properties.
    async fn nodes_keys(
        &mut self,
        ids: &[u64],
        keys: Option<&[String]>,
        lock: Option<bool>,
    ) -> Result<Vec<NodeRow>> {
        let mut out = Vec::new();
        let classes: Vec<u32> = self.schema.classes.keys().copied().collect();
        let tail = match lock {
            Some(true) => " FOR UPDATE",
            Some(false) => " LOCK IN SHARE MODE",
            None => "",
        };
        let mut reqs = Vec::new();
        for (i, part) in self.by_shard(ids) {
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let cond = format!("id IN ({})", ids_list(chunk));
                let parts: Vec<(u32, String)> =
                    classes.iter().map(|c| (*c, cond.clone())).collect();
                reqs.push((i, parts));
            }
        }
        out.extend(
            self.read_nodes_shards(&reqs, tail, keys, lock.is_some())
                .await?,
        );
        Ok(out)
    }

    /// Edges by `(src_rk, id)` with only `keys` of their properties.
    async fn edges_keys(
        &mut self,
        at: &[(u64, u64)],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        let mut by: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        for (rk, id) in at {
            by.entry(self.layout.index_of(*rk)).or_default().push(*id);
        }
        let ts = self.types_of(&[]);
        let mut reqs = Vec::new();
        for (i, ids) in by {
            for chunk in ids.chunks(IDS_PER_STATEMENT) {
                let cond = format!("id IN ({})", ids_list(chunk));
                let parts: Vec<(u32, String)> = ts.iter().map(|t| (*t, cond.clone())).collect();
                reqs.push((i, parts));
            }
        }
        self.read_edges_shards(&reqs, "", keys).await
    }

    fn types_of(&self, types: &[u32]) -> Vec<u32> {
        if types.is_empty() {
            self.schema.types.iter().copied().collect()
        } else {
            types
                .iter()
                .copied()
                .filter(|t| self.schema.types.contains(t))
                .collect()
        }
    }

    /// Edges whose destination is one of `ids`, of `types`: through the
    /// mirrors on the destinations' shards, else every shard's `rev` key.
    async fn incoming(&mut self, ids: &[u64], types: &[u32]) -> Result<Vec<EdgeRow>> {
        self.incoming_keys(ids, types, None).await
    }

    async fn incoming_keys(
        &mut self,
        ids: &[u64],
        types: &[u32],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        let ts = self.types_of(types);
        let mut out = Vec::new();
        if ts.is_empty() || ids.is_empty() {
            return Ok(out);
        }
        if self.read_mirrors().await {
            let mut at: BTreeMap<(usize, u32), Vec<EdgeAt>> = BTreeMap::new();
            for l in self.in_links(ids, &ts).await? {
                at.entry((self.layout.index_of(l.src_rk), l.rtype))
                    .or_default()
                    .push((l.src, l.dst, l.id));
            }
            let mut by_shard: BTreeMap<usize, Vec<(u32, String)>> = BTreeMap::new();
            for ((i, t), keys3) in at {
                for chunk in keys3.chunks(IDS_PER_STATEMENT) {
                    let tuples = chunk
                        .iter()
                        .map(|(s, d, id)| format!("({s},{d},{id})"))
                        .collect::<Vec<_>>()
                        .join(",");
                    by_shard
                        .entry(i)
                        .or_default()
                        .push((t, format!("(src, dst, id) IN ({tuples})")));
                }
            }
            let reqs: Vec<(usize, Vec<(u32, String)>)> = by_shard.into_iter().collect();
            out.extend(self.read_edges_shards(&reqs, "", keys).await?);
            return Ok(out);
        }
        let mut reqs = Vec::new();
        for i in 0..self.layout.shards.len() {
            for chunk in ids.chunks(IDS_PER_STATEMENT) {
                let cond = format!("dst IN ({})", ids_list(chunk));
                let parts: Vec<(u32, String)> = ts.iter().map(|t| (*t, cond.clone())).collect();
                reqs.push((i, parts));
            }
        }
        out.extend(self.read_edges_shards(&reqs, "", keys).await?);
        Ok(out)
    }

    /// Links into `ids` over types `ts` from the mirrors on the ids'
    /// shards (one round trip per shard).
    async fn in_links(&mut self, ids: &[u64], ts: &[u32]) -> Result<Vec<Link>> {
        let mut out = Vec::new();
        let g = self.layout.graph;
        let mut batches = Vec::new();
        for (i, part) in self.by_shard(ids) {
            let schema = self.schema_name(i);
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let l = ids_list(chunk);
                let stmts: Vec<String> = ts
                    .iter()
                    .map(|t| {
                        format!(
                            "SELECT src, dst, id, src_rk, _sprig_rk FROM {} WHERE dst IN ({l})",
                            super::mirror_table(&schema, g, *t)
                        )
                    })
                    .collect();
                batches.push((i, stmts));
            }
        }
        for sets in self.multi_shards(batches, false).await? {
            {
                for (t, rows) in ts.iter().zip(sets) {
                    for mut r in rows {
                        out.push(Link {
                            src: u64_col(r.take(0)),
                            dst: u64_col(r.take(1)),
                            id: u64_col(r.take(2)),
                            src_rk: u64_col(r.take(3)),
                            dst_rk: u64_col(r.take(4)),
                            rtype: *t,
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    /// Links of types `ts` from `from` (`out_dir`: outgoing, by source;
    /// else incoming, through the mirrors) with their far nodes (`keys` of
    /// their properties), joined in the same round trip wherever the far
    /// node's shard schema is on the near shard's cluster. Far nodes it
    /// couldn't join are read by id after.
    async fn links_joined(
        &mut self,
        from: &[u64],
        ts: &[u32],
        out_dir: bool,
        keys: Option<&[String]>,
        only: &[u64],
        page: (Option<u64>, Option<usize>),
    ) -> Result<(Vec<Link>, HashMap<u64, NodeRow>)> {
        let g = self.layout.graph;
        let names = self.key_names();
        let (near, far_col, far_rk, far_cls) = if out_dir {
            ("src", "dst", "dst_rk", "dst_cls")
        } else {
            ("dst", "src", "src_rk", "src_cls")
        };
        let mut only_sql = if only.is_empty() {
            String::new()
        } else {
            format!(" AND e.{far_col} IN ({})", ids_list(only))
        };
        if let Some(a) = page.0 {
            let _ = write!(only_sql, " AND e.id > {a}");
        }
        // With a limit, each type's links are its first `n` by id (the
        // caller's `n` overall are among them), and the joins read the far
        // nodes of those links only.
        let lim = page.1.map(|n| format!(" ORDER BY e.id LIMIT {n}"));
        let kf = self.key_filter(keys);
        let mut links = Vec::new();
        let mut nodes: HashMap<u64, NodeRow> = HashMap::new();
        let mut batches: Vec<(usize, Vec<String>)> = Vec::new();
        let mut metas: Vec<(usize, Vec<ClassCols>)> = Vec::new();
        for (i, part) in self.by_shard(from) {
            let schema = self.schema_name(i);
            let cluster = self.layout.shards[i].cluster.clone();
            let same: Vec<usize> = (0..self.layout.shards.len())
                .filter(|j| self.layout.shards[*j].cluster == cluster)
                .collect();
            let layout = Arc::clone(&self.layout);
            let range = |j: usize| {
                let mut r = format!("e.{far_rk} >= {}", layout.shards[j].lo);
                if let Some(h) = layout.hi(j) {
                    let _ = write!(r, " AND e.{far_rk} < {h}");
                }
                r
            };
            let near_table = |t: u32| {
                if out_dir {
                    super::edge_table(&schema, g, t)
                } else {
                    super::mirror_table(&schema, g, t)
                }
            };
            let mut classes: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for &t in ts {
                for c in self.schema.far_classes(t, out_dir) {
                    if self.schema.classes.contains_key(&c) {
                        classes.entry(c).or_default().push(t);
                    }
                }
            }
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let l = ids_list(chunk);
                let mut stmts = Vec::new();
                for &t in ts {
                    let (a, b) = if out_dir {
                        ("_sprig_rk", "dst_rk")
                    } else {
                        ("src_rk", "_sprig_rk")
                    };
                    stmts.push(format!(
                        "SELECT e.src, e.dst, e.id, e.{a}, e.{b} FROM {} e WHERE e.{near} IN ({l}){only_sql}{}",
                        near_table(t),
                        lim.as_deref().unwrap_or("")
                    ));
                }
                let mut colsets = Vec::new();
                for (c, cts) in &classes {
                    let cols = self.cols_for((Entity::Node, *c), keys);
                    let mut sel = String::from("n.id, n._sprig_rk, n._x");
                    for (k, code) in &cols {
                        let _ = write!(sel, ", n.{}", super::column(*k, *code));
                    }
                    let mut branches = Vec::new();
                    for &t in cts {
                        for &j in &same {
                            let src = match &lim {
                                Some(lim) => format!(
                                    "(SELECT e.* FROM {} e WHERE e.{near} IN ({l}){only_sql}{lim})",
                                    near_table(t)
                                ),
                                None => near_table(t),
                            };
                            branches.push(format!(
                                "SELECT {sel} FROM {src} e JOIN {} n ON n.id = e.{far_col} \
                                 WHERE e.{near} IN ({l}){only_sql} AND e.{far_cls} = {c} AND {}",
                                super::node_table(&self.layout.shards[j].schema, g, *c),
                                range(j)
                            ));
                            self.touched.insert((j, (Entity::Node, *c)));
                        }
                    }
                    stmts.push(branches.join(" UNION ALL "));
                    colsets.push((*c, cols));
                }
                batches.push((i, stmts));
                metas.push((i, colsets));
            }
        }
        let results = self.multi_shards(batches, false).await?;
        // Far nodes with values in the values table: on their own shards.
        let mut flagged: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
        let mut read: Vec<(Vec<u32>, Read)> = Vec::new();
        for ((i, colsets), mut sets) in metas.into_iter().zip(results) {
            let node_sets = sets.split_off(ts.len().min(sets.len()));
            for (t, rows) in ts.iter().zip(sets) {
                if out_dir {
                    self.touched.insert((i, (Entity::Relationship, *t)));
                }
                for mut r in rows {
                    links.push(Link {
                        src: u64_col(r.take(0)),
                        dst: u64_col(r.take(1)),
                        id: u64_col(r.take(2)),
                        src_rk: u64_col(r.take(3)),
                        dst_rk: u64_col(r.take(4)),
                        rtype: *t,
                    });
                }
            }
            for ((c, cols), rows) in colsets.iter().zip(node_sets) {
                let labels = self.schema.classes.get(c).cloned().unwrap_or_default();
                for (_, r) in Self::decode_rows(rows, cols, 0, &names) {
                    if r.x {
                        flagged
                            .entry(self.layout.index_of(r.rk))
                            .or_default()
                            .push(r.id);
                    }
                    read.push((labels.clone(), r));
                }
            }
        }
        for v in flagged.values_mut() {
            v.sort_unstable();
            v.dedup();
        }
        let items = if flagged.is_empty() || kf.is_none() {
            HashMap::new()
        } else {
            self.items_for('n', flagged, keys).await?
        };
        for (labels, r) in read {
            if nodes.contains_key(&r.id) {
                continue;
            }
            let (props, types) =
                codec::decode(r.scalars, items.get(&r.id).cloned().unwrap_or_default());
            nodes.insert(
                r.id,
                NodeRow {
                    id: r.id,
                    rk: r.rk,
                    labels,
                    props,
                    types,
                },
            );
        }
        // Far nodes on other clusters, or of classes not recorded.
        let mut missing: Vec<u64> = links
            .iter()
            .map(|l| if out_dir { l.dst } else { l.src })
            .filter(|f| !nodes.contains_key(f))
            .collect();
        missing.sort_unstable();
        missing.dedup();
        if !missing.is_empty() {
            for n in self.nodes_keys(&missing, keys, None).await? {
                nodes.insert(n.id, n);
            }
        }
        Ok((links, nodes))
    }

    /// Links out of `ids` over types `ts` (one round trip per shard).
    async fn out_links(&mut self, ids: &[u64], ts: &[u32]) -> Result<Vec<Link>> {
        let mut out = Vec::new();
        let g = self.layout.graph;
        let mut batches = Vec::new();
        for (i, part) in self.by_shard(ids) {
            let schema = self.schema_name(i);
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let l = ids_list(chunk);
                let stmts: Vec<String> = ts
                    .iter()
                    .map(|t| {
                        format!(
                            "SELECT src, dst, id, _sprig_rk, dst_rk FROM {} WHERE src IN ({l})",
                            super::edge_table(&schema, g, *t)
                        )
                    })
                    .collect();
                batches.push((i, stmts));
            }
        }
        let shards: Vec<usize> = batches.iter().map(|(i, _)| *i).collect();
        for (i, sets) in shards
            .into_iter()
            .zip(self.multi_shards(batches, false).await?)
        {
            {
                for (t, rows) in ts.iter().zip(sets) {
                    self.touched.insert((i, (Entity::Relationship, *t)));
                    for mut r in rows {
                        out.push(Link {
                            src: u64_col(r.take(0)),
                            dst: u64_col(r.take(1)),
                            id: u64_col(r.take(2)),
                            src_rk: u64_col(r.take(3)),
                            dst_rk: u64_col(r.take(4)),
                            rtype: *t,
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    async fn outgoing(&mut self, ids: &[u64], types: &[u32]) -> Result<Vec<EdgeRow>> {
        let ts = self.types_of(types);
        let mut reqs = Vec::new();
        for (i, part) in self.by_shard(ids) {
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let cond = format!("src IN ({})", ids_list(chunk));
                let parts: Vec<(u32, String)> = ts.iter().map(|t| (*t, cond.clone())).collect();
                reqs.push((i, parts));
            }
        }
        self.read_edges_shards(&reqs, "", None).await
    }

    /// Makes what writing `rows` needs: their keys, label sets' tables,
    /// columns of tables this transaction hasn't touched.
    async fn prepare(&mut self, nodes: &[&NodeRow], edges: &[&EdgeRow]) -> Result<()> {
        self.prepare_ends(nodes, edges, &[]).await
    }

    /// [`Self::prepare`], with the classes relationships will end at.
    async fn prepare_ends(
        &mut self,
        nodes: &[&NodeRow],
        edges: &[&EdgeRow],
        ends: &[(u32, bool, u32)],
    ) -> Result<()> {
        let mut want = Want::default();
        let mut later: BTreeMap<Owner, BTreeSet<(String, Code)>> = BTreeMap::new();
        want.ends.extend(
            ends.iter()
                .filter(|e| !self.schema.ends.contains(e))
                .copied(),
        );
        let shards: Vec<usize> = (0..self.layout.shards.len()).collect();
        let need = |owner: Option<Owner>,
                    new_labels: Option<Vec<u32>>,
                    new_type: Option<u32>,
                    props: &Json,
                    types: Option<&Json>,
                    want: &mut Want,
                    later: &mut BTreeMap<Owner, BTreeSet<(String, Code)>>,
                    schema: &TypedSchema,
                    touched: &HashSet<(usize, Owner)>| {
            if let Json::Object(m) = props {
                want.keys
                    .extend(m.keys().filter(|k| !schema.keys.contains_key(*k)).cloned());
            }
            let (cols, _) = codec::needs(props, types);
            if let Some(l) = new_labels {
                want.classes.entry(l).or_default().extend(cols);
            } else if let Some(t) = new_type {
                want.types.entry(t).or_default().extend(cols);
            } else if let Some(o) = owner {
                let have = schema.columns.get(&o);
                let missing: BTreeSet<(String, Code)> = cols
                    .into_iter()
                    .filter(|(k, c)| {
                        !schema
                            .keys
                            .get(k)
                            .is_some_and(|id| have.is_some_and(|h| h.contains(&(*id, *c))))
                    })
                    .collect();
                // Only tables no shard of this transaction has touched;
                // the others' after the commit.
                if !missing.is_empty() {
                    if shards.iter().any(|i| touched.contains(&(*i, o))) {
                        later.entry(o).or_default().extend(missing);
                    } else {
                        want.columns.entry(o).or_default().extend(missing);
                    }
                }
            }
        };
        for n in nodes {
            let mut l = n.labels.clone();
            l.sort_unstable();
            l.dedup();
            match self.schema.class_of(&l) {
                Some(c) => need(
                    Some((Entity::Node, c)),
                    None,
                    None,
                    &n.props,
                    n.types.as_ref(),
                    &mut want,
                    &mut later,
                    &self.schema,
                    &self.touched,
                ),
                None => need(
                    None,
                    Some(l),
                    None,
                    &n.props,
                    n.types.as_ref(),
                    &mut want,
                    &mut later,
                    &self.schema,
                    &self.touched,
                ),
            }
        }
        for e in edges {
            if self.schema.types.contains(&e.rtype) {
                need(
                    Some((Entity::Relationship, e.rtype)),
                    None,
                    None,
                    &e.props,
                    e.types.as_ref(),
                    &mut want,
                    &mut later,
                    &self.schema,
                    &self.touched,
                );
            } else {
                need(
                    None,
                    None,
                    Some(e.rtype),
                    &e.props,
                    e.types.as_ref(),
                    &mut want,
                    &mut later,
                    &self.schema,
                    &self.touched,
                );
            }
        }
        if !want.is_empty() {
            want.indexes.clone_from(&self.indexes);
            self.schema = self.ddl.ensure(&want).await?;
        }
        for (owner, cols) in later {
            self.ddl.defer(owner, cols);
        }
        Ok(())
    }

    fn encode_for(&self, owner: Owner, props: &Json, types: Option<&Json>) -> Encoded {
        let have = self.schema.columns.get(&owner);
        codec::encode(props, types, |k, c| {
            self.schema
                .keys
                .get(k)
                .is_some_and(|id| have.is_some_and(|h| h.contains(&(*id, c))))
        })
    }

    fn class_for(&self, labels: &[u32]) -> Result<u32> {
        self.schema
            .class_of(labels)
            .ok_or_else(|| SprigError::internal("a label set without its table"))
    }

    /// `INSERT` of the values table's rows of one element.
    async fn insert_items(
        &mut self,
        i: usize,
        kind: char,
        id: u64,
        rk: u64,
        items: &[ItemRow],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let t = super::values_table(&self.schema_name(i), self.layout.graph);
        let mut sql = format!(
            "INSERT INTO {t} (kind, id, key_id, ord, code, v_i, v_f, v_s, v_b, _sprig_rk) VALUES "
        );
        let mut params: Vec<MyValue> = Vec::new();
        for (j, it) in items.iter().enumerate() {
            let key = *self
                .schema
                .keys
                .get(&it.key)
                .ok_or_else(|| SprigError::internal("a property key without its id"))?;
            if j > 0 {
                sql.push_str(", ");
            }
            sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            let (vi, vf, vs, vb) = match &it.val {
                Some(Val::Int(x)) => (
                    MyValue::Int(*x),
                    MyValue::NULL,
                    MyValue::NULL,
                    MyValue::NULL,
                ),
                Some(Val::Bool(b)) => (
                    MyValue::Int(i64::from(*b)),
                    MyValue::NULL,
                    MyValue::NULL,
                    MyValue::NULL,
                ),
                Some(Val::Float(f)) => (
                    MyValue::NULL,
                    MyValue::Double(*f),
                    MyValue::NULL,
                    MyValue::NULL,
                ),
                Some(Val::Text(s)) => (
                    MyValue::NULL,
                    MyValue::NULL,
                    MyValue::Bytes(s.clone().into_bytes()),
                    MyValue::NULL,
                ),
                Some(Val::Bytes(b)) => (
                    MyValue::NULL,
                    MyValue::NULL,
                    MyValue::NULL,
                    MyValue::Bytes(b.clone()),
                ),
                None => (MyValue::NULL, MyValue::NULL, MyValue::NULL, MyValue::NULL),
            };
            params.extend([
                MyValue::from(kind.to_string()),
                MyValue::from(id),
                MyValue::from(key),
                MyValue::from(it.ord),
                MyValue::from(it.code.map_or("e", Code::suffix)),
                vi,
                vf,
                vs,
                vb,
                MyValue::from(rk),
            ]);
        }
        self.run_at(i, sql, Params::Positional(params)).await?;
        Ok(())
    }

    async fn delete_items(&mut self, i: usize, kind: char, ids: &[u64]) -> Result<()> {
        let t = super::values_table(&self.schema_name(i), self.layout.graph);
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            self.run_at(
                i,
                format!(
                    "DELETE FROM {t} WHERE kind = '{kind}' AND id IN ({})",
                    ids_list(chunk)
                ),
                Params::Empty,
            )
            .await?;
        }
        Ok(())
    }

    /// Rows of one table in multi-row `INSERT`s: `fixed` columns (with
    /// their values per row first) then each row's scalars by column,
    /// `NULL` where a row lacks one; at most [`ROWS_PER_INSERT`] rows or
    /// 60,000 values a statement.
    async fn insert_rows(
        &mut self,
        i: usize,
        table: &str,
        fixed: &str,
        rows: Vec<BatchRow>,
        tail: &str,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut cols: Vec<(u32, Code)> = rows
            .iter()
            .flat_map(|(_, s)| s.iter().map(|(c, _)| *c))
            .collect();
        cols.sort_unstable();
        cols.dedup();
        let width = rows[0].0.len() + cols.len();
        let per = ROWS_PER_INSERT.min((60_000 / width.max(1)).max(1));
        let mut names = String::from(fixed);
        for (k, c) in &cols {
            let _ = write!(names, ", {}", super::column(*k, *c));
        }
        let one = format!("({})", vec!["?"; width].join(", "));
        let mut it = rows.into_iter().peekable();
        while it.peek().is_some() {
            let chunk: Vec<_> = it.by_ref().take(per).collect();
            let mut params = Vec::with_capacity(chunk.len() * width);
            for (f, s) in &chunk {
                params.extend(f.iter().cloned());
                let m: HashMap<(u32, Code), &MyValue> = s.iter().map(|(c, v)| (*c, v)).collect();
                for c in &cols {
                    params.push(m.get(c).map_or(MyValue::NULL, |v| (*v).clone()));
                }
            }
            let marks = vec![one.as_str(); chunk.len()].join(", ");
            self.run_at(
                i,
                format!("INSERT INTO {table} ({names}) VALUES {marks}{tail}"),
                Params::Positional(params),
            )
            .await?;
        }
        Ok(())
    }

    /// The uniqueness constraints of `entity` writers maintain claims for
    /// (`building` and `online`): every typed graph is grouped, so its
    /// claims table always exists.
    fn unique_defs(&self, entity: Entity) -> Vec<IndexDef> {
        self.indexes
            .iter()
            .filter(|d| {
                d.unique && d.entity == entity && matches!(d.state.as_str(), "building" | "online")
            })
            .cloned()
            .collect()
    }

    /// The claims table on shard `i`.
    fn claims_table(&self, i: usize) -> String {
        crate::catalog::Tables::of(&self.schema_name(i), self.layout.graph).claims
    }

    /// Claims `key` of constraint `d` for element `owner` (on the shard of
    /// `owner_rk`), in the statement's transaction, as the JSON store does:
    /// insert the claim row; when one is there, share-lock it and check
    /// its owner with an exclusive read on the owner's shard; a live owner
    /// with the same key is a duplicate (1062), an orphan is taken over.
    async fn claim(
        &mut self,
        d: &IndexDef,
        key: &[u8],
        what: &str,
        owner: u64,
        owner_rk: u64,
    ) -> Result<()> {
        let h = crate::schema::claim_rk(d.id, key);
        let i = self.layout.index_of(h);
        let table = self.claims_table(i);
        let ins = format!(
            "INSERT INTO {table} (idx, k, owner, owner_rk, _sprig_rk) VALUES (?, ?, ?, ?, ?)"
        );
        let vals = || {
            Params::Positional(vec![
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
                MyValue::from(owner),
                MyValue::from(owner_rk),
                MyValue::from(h),
            ])
        };
        match self.run_at(i, ins.clone(), vals()).await {
            Ok(_) => return Ok(()),
            Err(e) if crate::exec::is_duplicate(&e) => {}
            Err(e) => return Err(e),
        }
        let held = self
            .multi_at(
                i,
                &[format!(
                    "SELECT owner, owner_rk FROM {table} WHERE idx = {} AND k = {} LOCK IN SHARE MODE",
                    d.id,
                    hex("X'", key)
                )],
                true,
            )
            .await?
            .pop()
            .unwrap_or_default();
        let Some(mut row) = held.into_iter().next() else {
            // Released meanwhile: insert again.
            self.run_at(i, ins, vals()).await?;
            return Ok(());
        };
        let (o0, rk0) = (u64_col(row.take(0)), u64_col(row.take(1)));
        if o0 == owner {
            return Ok(());
        }
        let live = match d.entity {
            Entity::Node => self
                .nodes_by_id(&[o0], Some(true))
                .await?
                .into_iter()
                .any(|n| {
                    n.labels.contains(&d.token_id)
                        && crate::schema::claim_key(d, &n.props, n.types.as_ref()).as_deref()
                            == Some(key)
                }),
            Entity::Relationship => {
                self.edges_keys(&[(rk0, o0)], None)
                    .await?
                    .into_iter()
                    .any(|e| {
                        e.rtype == d.token_id
                            && crate::schema::claim_key(d, &e.props, e.types.as_ref()).as_deref()
                                == Some(key)
                    })
            }
        };
        if live {
            return Err(crate::maria::dup_error(d, what, o0));
        }
        self.run_at(
            i,
            format!("UPDATE {table} SET owner = ?, owner_rk = ? WHERE idx = ? AND k = ?"),
            Params::Positional(vec![
                MyValue::from(owner),
                MyValue::from(owner_rk),
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
            ]),
        )
        .await?;
        Ok(())
    }

    /// Releases `key` of `d` held by `owner` (a no-op if someone else
    /// holds it), in the statement's transaction.
    async fn release(&mut self, d: &IndexDef, key: &[u8], owner: u64) -> Result<()> {
        let h = crate::schema::claim_rk(d.id, key);
        let i = self.layout.index_of(h);
        let table = self.claims_table(i);
        self.run_at(
            i,
            format!("DELETE FROM {table} WHERE idx = ? AND k = ? AND owner = ?"),
            Params::Positional(vec![
                MyValue::from(d.id),
                MyValue::Bytes(key.to_vec()),
                MyValue::from(owner),
            ]),
        )
        .await?;
        Ok(())
    }

    /// The claim key of a node under `d`, if it has the label and every
    /// property.
    fn node_key(d: &IndexDef, n: &NodeRow) -> Option<Vec<u8>> {
        if n.labels.contains(&d.token_id) {
            crate::schema::claim_key(d, &n.props, n.types.as_ref())
        } else {
            None
        }
    }

    /// The claim key of a relationship under `d`.
    fn edge_key(d: &IndexDef, e: &EdgeRow) -> Option<Vec<u8>> {
        if e.rtype == d.token_id {
            crate::schema::claim_key(d, &e.props, e.types.as_ref())
        } else {
            None
        }
    }

    /// The classes of nodes `ids` now (the directory on their shards).
    async fn classes_of(&mut self, ids: &[u64]) -> Result<HashMap<u64, u32>> {
        let mut out = HashMap::new();
        for (i, part) in self.by_shard(ids) {
            let d = super::dir_table(&self.schema_name(i), self.layout.graph);
            for chunk in part.chunks(IDS_PER_STATEMENT) {
                let rows = self
                    .rows_at(
                        i,
                        format!("SELECT id, cls FROM {d} WHERE id IN ({})", ids_list(chunk)),
                    )
                    .await?;
                for mut r in rows {
                    let id = u64_col(r.take(0));
                    out.insert(id, u32::try_from(u64_col(r.take(1))).unwrap_or(0));
                }
            }
        }
        Ok(out)
    }

    /// `INSERT` of one node into its class's table (and the directory).
    async fn insert_node(&mut self, r: &NodeRow, cls: u32) -> Result<()> {
        let i = self.layout.index_of(r.rk);
        let owner = (Entity::Node, cls);
        let enc = self.encode_for(owner, &r.props, r.types.as_ref());
        let t = super::node_table(&self.schema_name(i), self.layout.graph, cls);
        let mut names = String::from("id, part, _v, _x, _sprig_rk");
        let mut marks = String::from("?, ?, ?, ?, ?");
        let mut params = vec![
            MyValue::from(r.id),
            MyValue::from(r.id),
            MyValue::from(self.v_for(r.id)),
            MyValue::from(i64::from(!enc.items.is_empty())),
            MyValue::from(r.rk),
        ];
        for ((k, c), v) in &enc.scalars {
            let id = self.schema.keys[k];
            let _ = write!(names, ", {}", super::column(id, *c));
            marks.push_str(", ?");
            params.push(v.my());
        }
        self.touched.insert((i, owner));
        if let Err(e) = self
            .run_at(
                i,
                format!("INSERT INTO {t} ({names}) VALUES ({marks})"),
                Params::Positional(params),
            )
            .await
        {
            return Err(dup_node(e, r.id));
        }
        let d = super::dir_table(&self.schema_name(i), self.layout.graph);
        self.run_at(
            i,
            format!("INSERT INTO {d} (id, cls, _sprig_rk) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE cls = VALUES(cls)"),
            Params::Positional(vec![MyValue::from(r.id), MyValue::from(cls), MyValue::from(r.rk)]),
        )
        .await?;
        self.insert_items(i, 'n', r.id, r.rk, &enc.items).await
    }

    /// The class a node is in now, from the directory on its shard.
    async fn class_now(&mut self, id: u64) -> Result<Option<u32>> {
        let i = self.layout.index_of(rk_of(id));
        let d = super::dir_table(&self.schema_name(i), self.layout.graph);
        let rows = self
            .rows_at(i, format!("SELECT cls FROM {d} WHERE id = {id}"))
            .await?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|mut r| u32::try_from(u64_col(r.take(0))).ok()))
    }

    /// After a node moved to class `cls`: the `src_cls`/`dst_cls` of its
    /// edges and mirrors.
    async fn reclass_edges(&mut self, id: u64, cls: u32) -> Result<()> {
        let types: Vec<u32> = self.schema.types.iter().copied().collect();
        let home = self.layout.index_of(rk_of(id));
        for &t in &types {
            let e = super::edge_table(&self.schema_name(home), self.layout.graph, t);
            self.run_at(
                home,
                format!("UPDATE {e} SET src_cls = {cls} WHERE src = {id}"),
                Params::Empty,
            )
            .await?;
        }
        // Mirrors of its outgoing edges, on their destinations' shards.
        let out = self.outgoing(&[id], &[]).await?;
        if self.mirrors() {
            for e in &out {
                let j = self.layout.index_of(e.dst_rk);
                let m = super::mirror_table(&self.schema_name(j), self.layout.graph, e.rtype);
                self.run_at(
                    j,
                    format!(
                        "UPDATE {m} SET src_cls = {cls} WHERE dst = {} AND src = {id} AND id = {}",
                        e.dst, e.id
                    ),
                    Params::Empty,
                )
                .await?;
            }
        }
        // Its incoming edges, on their sources' shards.
        let inc = self.incoming(&[id], &[]).await?;
        for e in &inc {
            let j = self.layout.index_of(e.src_rk);
            let tbl = super::edge_table(&self.schema_name(j), self.layout.graph, e.rtype);
            self.run_at(
                j,
                format!(
                    "UPDATE {tbl} SET dst_cls = {cls} WHERE src = {} AND dst = {id} AND id = {}",
                    e.src, e.id
                ),
                Params::Empty,
            )
            .await?;
        }
        Ok(())
    }
}

fn dup_node(e: SprigError, id: u64) -> SprigError {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno: 1062, .. } => {
            SprigError::backend(1062, *b"23000", format!("Node({id}) already exists"))
        }
        _ => e,
    }
}

/// Values a scan's equality may compare in SQL: (code, literal) per
/// JSON value; `None` when one of them can't be (the scan then reads
/// without that condition).
fn eq_lits(vals: &[Json]) -> Option<Vec<(Code, String)>> {
    let mut out = Vec::new();
    for v in vals {
        match v {
            Json::Number(n) => {
                // `1 = 1.0` in Cypher: both columns.
                let f = n.as_f64()?;
                if let Some(i) = n.as_i64() {
                    out.push((Code::Int, i.to_string()));
                }
                out.push((Code::Float, lit(&Val::Float(f))));
            }
            Json::String(s) => out.push((Code::Str, lit(&Val::Text(s.clone())))),
            Json::Bool(b) => out.push((Code::Bool, i64::from(*b).to_string())),
            _ => return None,
        }
    }
    Some(out)
}

#[async_trait]
impl GraphStore for TypedStore<'_> {
    fn graph_id(&self) -> u64 {
        self.layout.graph
    }

    fn shards(&self) -> usize {
        self.layout.shards.len()
    }

    fn shard_of(&self, rk: u64) -> usize {
        self.layout.index_of(rk)
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let classes = match s.label {
            Some(l) => self.schema.classes_with(Some(l)),
            None => self.schema.classes_with(None),
        };
        let mut out: Vec<NodeRow> = Vec::new();
        let shards: Vec<(usize, Option<Vec<u64>>)> = match &s.ids {
            Some(ids) => self
                .by_shard(ids)
                .into_iter()
                .map(|(i, v)| (i, Some(v)))
                .collect(),
            None => (0..self.layout.shards.len()).map(|i| (i, None)).collect(),
        };
        let mut reqs = Vec::new();
        for (i, ids) in shards {
            let mut parts = Vec::with_capacity(classes.len());
            for &c in &classes {
                let owner = (Entity::Node, c);
                let mut cond = String::from("1 = 1");
                if let Some(ids) = &ids {
                    let _ = write!(cond, " AND id IN ({})", ids_list(ids));
                }
                if let Some(a) = s.after {
                    let _ = write!(cond, " AND id > {a}");
                }
                // Equalities: a superset the caller rechecks. The first
                // drives the read: a branch per column of its key (so an
                // index on it is used), and one for values-table rows; the
                // others filter each branch. A value whose column the
                // table lacks may be in the values table (`_x`).
                let mut filters = String::new();
                let mut drive: Option<Vec<String>> = None;
                for (n, (k, vals)) in s.props_eq.iter().enumerate() {
                    let Some(lits) = eq_lits(vals) else { continue };
                    let key = self.schema.keys.get(k).copied();
                    let have = self.schema.columns.get(&owner).cloned().unwrap_or_default();
                    let mut grouped: BTreeMap<Code, Vec<String>> = BTreeMap::new();
                    for (code, l) in lits {
                        grouped.entry(code).or_default().push(l);
                    }
                    let mut ors: Vec<String> = Vec::new();
                    for (code, ls) in grouped {
                        if let Some(id) = key
                            && have.contains(&(id, code))
                        {
                            ors.push(format!(
                                "{} IN ({})",
                                super::column(id, code),
                                ls.join(", ")
                            ));
                        }
                    }
                    if n == 0 || drive.is_none() {
                        let mut br = ors;
                        if let Some(id) = key {
                            br.push(format!(
                                "_x = 1 AND id IN (SELECT v.id FROM {} v WHERE v.kind = 'n' AND v.key_id = {id})",
                                super::values_table(&self.schema_name(i), self.layout.graph)
                            ));
                        }
                        drive = Some(br);
                    } else {
                        ors.push("_x = 1".to_owned());
                        let _ = write!(filters, " AND ({})", ors.join(" OR "));
                    }
                }
                match drive {
                    Some(branches) => {
                        for b in branches {
                            parts.push((c, format!("{cond} AND {b}{filters}")));
                        }
                    }
                    None => parts.push((c, cond)),
                }
            }
            reqs.push((i, parts));
        }
        let tail = match (s.after, s.limit) {
            (_, Some(l)) => format!(" ORDER BY id LIMIT {l}"),
            (Some(_), None) => " ORDER BY id".to_owned(),
            _ => String::new(),
        };
        let keys = s.keys.clone();
        out.extend(
            self.read_nodes_shards(&reqs, &tail, keys.as_deref(), false)
                .await?,
        );
        // Branches of one class may find a node twice.
        out.sort_by_key(|n| n.id);
        out.dedup_by_key(|n| n.id);
        if let Some(l) = s.limit {
            out.truncate(l);
        }
        Ok(out)
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.nodes_by_id(ids, None).await
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        if from.is_empty() {
            return Ok(Vec::new());
        }
        let edges = match dir {
            Dir::Out => self.outgoing(from, types).await?,
            Dir::In => self.incoming(from, types).await?,
            Dir::Both => return Err(SprigError::internal("expand takes one direction")),
        };
        Ok(edges.into_iter().map(|e| (e, None)).collect())
    }

    async fn reach(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        let ts = self.types_of(types);
        if seeds.is_empty() || hops == 0 || ts.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.schema_name(shard);
        let g = self.layout.graph;
        let many = self.layout.shards.len() > 1;
        let mut seed_sql = String::new();
        for (k, (o, n)) in seeds.iter().enumerate() {
            if k > 0 {
                seed_sql.push_str(" UNION ALL ");
            }
            let _ = write!(seed_sql, "SELECT {o} AS o, {n} AS n");
        }
        let mut range = String::new();
        if many {
            let _ = write!(range, " AND r.rk >= {}", self.layout.shards[shard].lo);
            if let Some(h) = self.layout.hi(shard) {
                let _ = write!(range, " AND r.rk < {h}");
            }
        }
        let (mut base, mut step) = (Vec::new(), Vec::new());
        for t in ts {
            let (tbl, near, far, far_rk) = match (dir, many) {
                (Dir::In, true) => (super::mirror_table(&schema, g, t), "dst", "src", "src_rk"),
                (Dir::In, false) => (super::edge_table(&schema, g, t), "dst", "src", "_sprig_rk"),
                _ => (super::edge_table(&schema, g, t), "src", "dst", "dst_rk"),
            };
            base.push(format!(
                "SELECT s.o, e.{far}, e.{far_rk}, 1 FROM ({seed_sql}) s JOIN {tbl} e ON e.{near} = s.n"
            ));
            step.push(format!(
                "SELECT r.o, e.{far}, e.{far_rk}, r.d + 1 FROM r JOIN {tbl} e ON e.{near} = r.id WHERE r.d < {hops}{range}"
            ));
        }
        let sql = format!(
            "SET STATEMENT max_recursive_iterations = {} FOR WITH RECURSIVE r (o, id, rk, d) AS ({} UNION {}) \
             CYCLE o, id RESTRICT SELECT o, id, rk, MIN(d) FROM r GROUP BY o, id, rk",
            hops + 1,
            base.join(" UNION "),
            step.join(" UNION ")
        );
        self.stats.cte_queries += 1;
        let rows = self.rows_at(shard, sql).await?;
        Ok(rows
            .into_iter()
            .map(|mut r| Reach {
                origin: u64_col(r.take(0)),
                node: u64_col(r.take(1)),
                rk: u64_col(r.take(2)),
                depth: u32::try_from(u64_col(r.take(3))).unwrap_or(u32::MAX),
            })
            .collect())
    }

    async fn links(&mut self, from: &[u64], types: &[u32], dir: Dir) -> Result<Vec<Link>> {
        let ts = self.types_of(types);
        if from.is_empty() || ts.is_empty() {
            return Ok(Vec::new());
        }
        match dir {
            Dir::Out => self.out_links(from, &ts).await,
            Dir::In if self.read_mirrors().await => self.in_links(from, &ts).await,
            Dir::In => Ok(self
                .incoming_keys(from, &ts, Some(&[]))
                .await?
                .iter()
                .map(Link::from)
                .collect()),
            Dir::Both => {
                let mut v = self.out_links(from, &ts).await?;
                v.extend(Box::pin(self.links(from, &ts, Dir::In)).await?);
                Ok(v)
            }
        }
    }

    fn late_props(&self) -> bool {
        true
    }

    async fn nodes_part(&mut self, ids: &[u64], keys: Option<&[String]>) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.nodes_keys(ids, keys, None).await
    }

    async fn edges_part(
        &mut self,
        at: &[(u64, u64)],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        self.edges_keys(at, keys).await
    }

    async fn links_far(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        far: &FarRead<'_>,
    ) -> Result<Vec<FarLink>> {
        let ts = self.types_of(types);
        if from.is_empty() || ts.is_empty() {
            return Ok(Vec::new());
        }
        // A plain hop with its far nodes: one round trip per shard.
        if far.join
            && far.rel_keys.is_none()
            && (dir == Dir::Out || (dir == Dir::In && self.read_mirrors().await))
        {
            let out_dir = dir == Dir::Out;
            let (mut links, nodes) = self
                .links_joined(
                    from,
                    &ts,
                    out_dir,
                    far.keys,
                    far.only,
                    (far.after, far.limit),
                )
                .await?;
            if far.limit.is_some() || far.ordered {
                links.sort_by_key(|l| l.id);
            }
            if let Some(n) = far.limit {
                links.truncate(n);
            }
            return Ok(links
                .into_iter()
                .map(|l| {
                    let f = if out_dir { l.dst } else { l.src };
                    (l, nodes.get(&f).cloned(), None)
                })
                .collect());
        }
        // The links (with the relationships' properties when asked), then
        // the far nodes with the keys asked for: a round trip per shard
        // each.
        let mut links: Vec<(Link, Option<(Json, Option<Json>)>)> = match (dir, far.rel_keys) {
            (_, Some(keys)) => {
                let keys = (!keys.is_empty()).then_some(keys);
                let edges = match dir {
                    Dir::Out => {
                        let mut reqs = Vec::new();
                        for (i, part) in self.by_shard(from) {
                            for chunk in part.chunks(IDS_PER_STATEMENT) {
                                let cond = format!("src IN ({})", ids_list(chunk));
                                let parts: Vec<(u32, String)> =
                                    ts.iter().map(|t| (*t, cond.clone())).collect();
                                reqs.push((i, parts));
                            }
                        }
                        self.read_edges_shards(&reqs, "", keys).await?
                    }
                    Dir::In => self.incoming_keys(from, &ts, keys).await?,
                    Dir::Both => return Err(SprigError::internal("links_far takes one direction")),
                };
                edges
                    .into_iter()
                    .map(|e| (Link::from(&e), Some((e.props, e.types))))
                    .collect()
            }
            (Dir::Both, None) => return Err(SprigError::internal("links_far takes one direction")),
            (d, None) => self
                .links(from, &ts, d)
                .await?
                .into_iter()
                .map(|l| (l, None))
                .collect(),
        };
        let out_dir = dir == Dir::Out;
        let far_of = |l: &Link| if out_dir { l.dst } else { l.src };
        if !far.only.is_empty() {
            let only: HashSet<u64> = far.only.iter().copied().collect();
            links.retain(|(l, _)| only.contains(&far_of(l)));
        }
        if let Some(a) = far.after {
            links.retain(|(l, _)| l.id > a);
        }
        if far.limit.is_some() || far.ordered {
            links.sort_by_key(|(l, _)| l.id);
        }
        if let Some(n) = far.limit {
            links.truncate(n);
        }
        let mut nodes: HashMap<u64, NodeRow> = HashMap::new();
        if far.join && !links.is_empty() {
            let mut ids: Vec<u64> = links.iter().map(|(l, _)| far_of(l)).collect();
            ids.sort_unstable();
            ids.dedup();
            for n in self.nodes_keys(&ids, far.keys, None).await? {
                nodes.insert(n.id, n);
            }
        }
        Ok(links
            .into_iter()
            .map(|(l, r)| {
                let n = if far.join {
                    nodes.get(&far_of(&l)).cloned()
                } else {
                    None
                };
                (l, n, r)
            })
            .collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        let mut out = Vec::new();
        if ids.is_empty() {
            return Ok(out);
        }
        let ts = self.types_of(&[]);
        let mut reqs = Vec::new();
        for i in 0..self.layout.shards.len() {
            for chunk in ids.chunks(IDS_PER_STATEMENT) {
                let cond = format!("id IN ({})", ids_list(chunk));
                let parts: Vec<(u32, String)> = ts.iter().map(|t| (*t, cond.clone())).collect();
                reqs.push((i, parts));
            }
        }
        out.extend(self.read_edges_shards(&reqs, "", None).await?);
        Ok(out)
    }

    async fn edges_at(&mut self, keys: &[(u64, u64)]) -> Result<Vec<EdgeRow>> {
        self.edges_keys(keys, None).await
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        let mut out = self.outgoing(nodes, &[]).await?;
        if out.len() < limit {
            out.extend(self.incoming(nodes, &[]).await?);
        }
        out.truncate(limit);
        Ok(out)
    }

    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.nodes_by_id(ids, Some(exclusive)).await
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.ids.take(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        for d in self.unique_defs(Entity::Node) {
            for r in rows {
                if let Some(k) = Self::node_key(&d, r) {
                    self.claim(&d, &k, "Node", r.id, r.rk).await?;
                }
            }
        }
        let refs: Vec<&NodeRow> = rows.iter().collect();
        self.prepare(&refs, &[]).await?;
        // One multi-row INSERT per shard and class, one per shard for the
        // directory; list items row by row.
        let mut groups: BTreeMap<(usize, u32), Vec<BatchRow>> = BTreeMap::new();
        let mut dirs: BTreeMap<usize, Vec<BatchRow>> = BTreeMap::new();
        let mut items = Vec::new();
        for r in rows {
            let cls = self.class_for(&r.labels)?;
            let i = self.layout.index_of(r.rk);
            let owner = (Entity::Node, cls);
            let enc = self.encode_for(owner, &r.props, r.types.as_ref());
            let scalars = enc
                .scalars
                .iter()
                .map(|((k, c), v)| ((self.schema.keys[k], *c), v.my()))
                .collect();
            groups.entry((i, cls)).or_default().push((
                vec![
                    MyValue::from(r.id),
                    MyValue::from(r.id),
                    MyValue::from(self.v_for(r.id)),
                    MyValue::from(i64::from(!enc.items.is_empty())),
                    MyValue::from(r.rk),
                ],
                scalars,
            ));
            dirs.entry(i).or_default().push((
                vec![MyValue::from(r.id), MyValue::from(cls), MyValue::from(r.rk)],
                Vec::new(),
            ));
            if !enc.items.is_empty() {
                items.push((i, r.id, r.rk, enc.items));
            }
        }
        for ((i, cls), batch) in groups {
            self.touched.insert((i, (Entity::Node, cls)));
            let t = super::node_table(&self.schema_name(i), self.layout.graph, cls);
            let first = batch.first().and_then(|(f, _)| f.first().cloned());
            if let Err(e) = self
                .insert_rows(i, &t, "id, part, _v, _x, _sprig_rk", batch, "")
                .await
            {
                let id = match first {
                    Some(MyValue::UInt(u)) => u,
                    _ => 0,
                };
                return Err(dup_node(e, id));
            }
        }
        for (i, batch) in dirs {
            let d = super::dir_table(&self.schema_name(i), self.layout.graph);
            self.insert_rows(
                i,
                &d,
                "id, cls, _sprig_rk",
                batch,
                " ON DUPLICATE KEY UPDATE cls = VALUES(cls)",
            )
            .await?;
        }
        for (i, id, rk, its) in items {
            self.insert_items(i, 'n', id, rk, &its).await?;
        }
        Ok(())
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        for d in self.unique_defs(Entity::Relationship) {
            for r in rows {
                if let Some(k) = Self::edge_key(&d, r) {
                    self.claim(&d, &k, "Relationship", r.id, r.src_rk).await?;
                }
            }
        }
        let refs: Vec<&EdgeRow> = rows.iter().collect();
        let mut ends_ids: Vec<u64> = rows.iter().flat_map(|r| [r.src, r.dst]).collect();
        ends_ids.sort_unstable();
        ends_ids.dedup();
        let cls = self.classes_of(&ends_ids).await?;
        let class = |id: u64| cls.get(&id).copied().unwrap_or(0);
        let mut ends = Vec::new();
        for r in rows {
            ends.push((r.rtype, false, class(r.src)));
            ends.push((r.rtype, true, class(r.dst)));
        }
        self.prepare_ends(&[], &refs, &ends).await?;
        let mut groups: BTreeMap<(usize, u32), Vec<BatchRow>> = BTreeMap::new();
        let mut mirrors: BTreeMap<(usize, u32), Vec<BatchRow>> = BTreeMap::new();
        let mut items = Vec::new();
        let keep_mirrors = self.write_mirrors();
        for r in rows {
            let (sc, dc) = (class(r.src), class(r.dst));
            let i = self.layout.index_of(r.src_rk);
            let owner = (Entity::Relationship, r.rtype);
            let enc = self.encode_for(owner, &r.props, r.types.as_ref());
            let scalars = enc
                .scalars
                .iter()
                .map(|((k, c), v)| ((self.schema.keys[k], *c), v.my()))
                .collect();
            groups.entry((i, r.rtype)).or_default().push((
                vec![
                    MyValue::from(r.src),
                    MyValue::from(r.dst),
                    MyValue::from(r.id),
                    MyValue::from(sc),
                    MyValue::from(dc),
                    MyValue::from(r.dst_rk),
                    MyValue::from(self.v_for(r.id)),
                    MyValue::from(i64::from(!enc.items.is_empty())),
                    MyValue::from(r.src_rk),
                ],
                scalars,
            ));
            if keep_mirrors {
                mirrors
                    .entry((self.layout.index_of(r.dst_rk), r.rtype))
                    .or_default()
                    .push((
                        vec![
                            MyValue::from(r.dst),
                            MyValue::from(r.src),
                            MyValue::from(r.id),
                            MyValue::from(sc),
                            MyValue::from(r.src_rk),
                            MyValue::from(self.v_for(r.id)),
                            MyValue::from(r.dst_rk),
                        ],
                        Vec::new(),
                    ));
            }
            if !enc.items.is_empty() {
                items.push((i, r.id, r.src_rk, enc.items));
            }
        }
        for ((i, t), batch) in groups {
            self.touched.insert((i, (Entity::Relationship, t)));
            let tbl = super::edge_table(&self.schema_name(i), self.layout.graph, t);
            self.insert_rows(
                i,
                &tbl,
                "src, dst, id, src_cls, dst_cls, dst_rk, _v, _x, _sprig_rk",
                batch,
                "",
            )
            .await?;
        }
        for ((j, t), batch) in mirrors {
            let m = super::mirror_table(&self.schema_name(j), self.layout.graph, t);
            self.insert_rows(
                j,
                &m,
                "dst, src, id, src_cls, src_rk, _v, _sprig_rk",
                batch,
                "",
            )
            .await?;
        }
        for (i, id, rk, its) in items {
            self.insert_items(i, 'e', id, rk, &its).await?;
        }
        Ok(())
    }

    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()> {
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Node)
            .into_iter()
            .filter(|d| row.labels.contains(&d.token_id) || old_labels.contains(&d.token_id))
            .collect();
        if !defs.is_empty() {
            // The row as the transaction sees it (the caller locked it).
            let old = self.nodes_by_id(&[row.id], Some(true)).await?;
            for d in &defs {
                let was = old.first().and_then(|o| Self::node_key(d, o));
                let now = Self::node_key(d, row);
                if was == now {
                    continue;
                }
                if let Some(k) = &now {
                    self.claim(d, k, "Node", row.id, row.rk).await?;
                }
                if let Some(k) = &was {
                    self.release(d, k, row.id).await?;
                }
            }
        }
        self.prepare(&[row], &[]).await?;
        let cls = self.class_for(&row.labels)?;
        let mut old = old_labels.to_vec();
        old.sort_unstable();
        old.dedup();
        let old_cls = match self.schema.class_of(&old) {
            Some(c) => Some(c),
            None => self.class_now(row.id).await?,
        };
        let i = self.layout.index_of(row.rk);
        if old_cls == Some(cls) {
            let owner = (Entity::Node, cls);
            let enc = self.encode_for(owner, &row.props, row.types.as_ref());
            let t = super::node_table(&self.schema_name(i), self.layout.graph, cls);
            let mut set = String::from("_v = ?, _x = ?");
            let mut params = vec![
                MyValue::from(self.v_for(row.id)),
                MyValue::from(i64::from(!enc.items.is_empty())),
            ];
            for (k, c) in self.schema.columns_of(owner) {
                let name = self.schema.key_name(k).unwrap_or_default().to_owned();
                let _ = write!(set, ", {} = ?", super::column(k, c));
                params.push(enc.scalars.get(&(name, c)).map_or(MyValue::NULL, Val::my));
            }
            params.push(MyValue::from(row.id));
            self.touched.insert((i, owner));
            self.run_at(
                i,
                format!("UPDATE {t} SET {set} WHERE id = ?"),
                Params::Positional(params),
            )
            .await?;
            self.delete_items(i, 'n', &[row.id]).await?;
            self.insert_items(i, 'n', row.id, row.rk, &enc.items)
                .await?;
            return Ok(());
        }
        // A label change: the row moves to its new label set's table.
        if let Some(oc) = old_cls {
            let t = super::node_table(&self.schema_name(i), self.layout.graph, oc);
            self.touched.insert((i, (Entity::Node, oc)));
            self.run_at(
                i,
                format!("DELETE FROM {t} WHERE id = {}", row.id),
                Params::Empty,
            )
            .await?;
        }
        self.delete_items(i, 'n', &[row.id]).await?;
        self.insert_node(row, cls).await?;
        // Its relationships of any type may now end at the new class.
        let ends: Vec<(u32, bool, u32)> = self
            .schema
            .types
            .iter()
            .flat_map(|t| [(*t, false, cls), (*t, true, cls)])
            .collect();
        self.prepare_ends(&[], &[], &ends).await?;
        self.reclass_edges(row.id, cls).await
    }

    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()> {
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Relationship)
            .into_iter()
            .filter(|d| d.token_id == row.rtype)
            .collect();
        if !defs.is_empty() {
            let old = self.edges_keys(&[(row.src_rk, row.id)], None).await?;
            for d in &defs {
                let was = old.first().and_then(|o| Self::edge_key(d, o));
                let now = Self::edge_key(d, row);
                if was == now {
                    continue;
                }
                if let Some(k) = &now {
                    self.claim(d, k, "Relationship", row.id, row.src_rk).await?;
                }
                if let Some(k) = &was {
                    self.release(d, k, row.id).await?;
                }
            }
        }
        self.prepare(&[], &[row]).await?;
        let i = self.layout.index_of(row.src_rk);
        let owner = (Entity::Relationship, row.rtype);
        let enc = self.encode_for(owner, &row.props, row.types.as_ref());
        let t = super::edge_table(&self.schema_name(i), self.layout.graph, row.rtype);
        let mut set = String::from("_v = ?, _x = ?");
        let mut params = vec![
            MyValue::from(self.v_for(row.id)),
            MyValue::from(i64::from(!enc.items.is_empty())),
        ];
        for (k, c) in self.schema.columns_of(owner) {
            let name = self.schema.key_name(k).unwrap_or_default().to_owned();
            let _ = write!(set, ", {} = ?", super::column(k, c));
            params.push(enc.scalars.get(&(name, c)).map_or(MyValue::NULL, Val::my));
        }
        params.extend([
            MyValue::from(row.src),
            MyValue::from(row.dst),
            MyValue::from(row.id),
        ]);
        self.touched.insert((i, owner));
        self.run_at(
            i,
            format!("UPDATE {t} SET {set} WHERE src = ? AND dst = ? AND id = ?"),
            Params::Positional(params),
        )
        .await?;
        self.delete_items(i, 'e', &[row.id]).await?;
        self.insert_items(i, 'e', row.id, row.src_rk, &enc.items)
            .await
    }

    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64> {
        let defs: Vec<IndexDef> = self
            .unique_defs(Entity::Relationship)
            .into_iter()
            .filter(|d| edges.iter().any(|e| e.rtype == d.token_id))
            .collect();
        if !defs.is_empty() {
            let keys: Vec<(u64, u64)> = edges.iter().map(|e| (e.src_rk, e.id)).collect();
            for e in self.edges_keys(&keys, None).await? {
                for d in &defs {
                    if let Some(k) = Self::edge_key(d, &e) {
                        self.release(d, &k, e.id).await?;
                    }
                }
            }
        }
        let mut n = 0;
        for e in edges {
            if !self.schema.types.contains(&e.rtype) {
                continue;
            }
            let i = self.layout.index_of(e.src_rk);
            let t = super::edge_table(&self.schema_name(i), self.layout.graph, e.rtype);
            self.touched.insert((i, (Entity::Relationship, e.rtype)));
            n += self
                .run_at(
                    i,
                    format!(
                        "DELETE FROM {t} WHERE src = {} AND dst = {} AND id = {}",
                        e.src, e.dst, e.id
                    ),
                    Params::Empty,
                )
                .await?;
            self.delete_items(i, 'e', &[e.id]).await?;
            if self.mirrors() {
                let j = self.layout.index_of(e.dst_rk);
                let m = super::mirror_table(&self.schema_name(j), self.layout.graph, e.rtype);
                self.run_at(
                    j,
                    format!(
                        "DELETE FROM {m} WHERE dst = {} AND src = {} AND id = {}",
                        e.dst, e.src, e.id
                    ),
                    Params::Empty,
                )
                .await?;
            }
        }
        Ok(n)
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        let defs = self.unique_defs(Entity::Node);
        if !defs.is_empty() && !ids.is_empty() {
            for o in self.nodes_by_id(ids, Some(true)).await? {
                for d in &defs {
                    if let Some(k) = Self::node_key(d, &o) {
                        self.release(d, &k, o.id).await?;
                    }
                }
            }
        }
        let mut n = 0;
        for &id in ids {
            let Some(cls) = self.class_now(id).await? else {
                continue;
            };
            let i = self.layout.index_of(rk_of(id));
            let t = super::node_table(&self.schema_name(i), self.layout.graph, cls);
            self.touched.insert((i, (Entity::Node, cls)));
            n += self
                .run_at(i, format!("DELETE FROM {t} WHERE id = {id}"), Params::Empty)
                .await?;
            let d = super::dir_table(&self.schema_name(i), self.layout.graph);
            self.run_at(i, format!("DELETE FROM {d} WHERE id = {id}"), Params::Empty)
                .await?;
            self.delete_items(i, 'n', &[id]).await?;
        }
        Ok(n)
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some((id, _)) = self.labels.iter().find(|(_, n)| *n == name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        let id = self.dict.create(true, name).await?;
        self.labels.insert(id, name.to_owned());
        Ok(Some(id))
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some((id, _)) = self.types.iter().find(|(_, n)| *n == name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        let id = self.dict.create(false, name).await?;
        self.types.insert(id, name.to_owned());
        Ok(Some(id))
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.labels.get(&id).cloned()
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.types.get(&id).cloned()
    }

    fn all_labels(&self) -> Vec<String> {
        self.labels.values().cloned().collect()
    }

    fn all_types(&self) -> Vec<String> {
        self.types.values().cloned().collect()
    }

    async fn batches(&mut self) -> Result<()> {
        if self.conns.batches() {
            Ok(())
        } else {
            Err(crate::store::not_implicit())
        }
    }

    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        if ok && !deleted.is_empty() {
            let layout = Arc::clone(&self.layout);
            let schema = self.schema.clone();
            if let Err(e) = check_deleted(&mut *self.conns, &layout, &schema, deleted).await {
                let _ = self.conns.end_batch(false).await;
                return Err(e);
            }
        }
        self.touched.clear();
        self.conns.end_batch(ok).await
    }

    fn indexes(&self) -> Vec<IndexDef> {
        self.indexes.clone()
    }

    async fn schema(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        match &self.ops {
            Some(o) => o.run(cmd).await,
            None => Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "index and constraint commands are not supported here",
            )),
        }
    }
}

/// After a statement deleted nodes: refuses the commit when an edge still
/// touches one (Neo4j's "still has relationships"), as
/// [`crate::maria::check_deleted`] does for the old layout.
///
/// # Errors
/// 39906 naming the node.
pub async fn check_deleted(
    conns: &mut dyn ShardConns,
    layout: &GraphShards,
    schema: &TypedSchema,
    deleted: &[u64],
) -> Result<()> {
    if deleted.is_empty() {
        return Ok(());
    }
    let g = layout.graph;
    // Asynchronous mirrors may lag: the check scatters instead.
    let many = layout.shards.len() > 1
        && layout.mirrors
        && !layout.mirror_async
        && !layout.mirror_building;
    for (i, ids) in layout.by_shard(deleted) {
        let at = layout.shards[i].clone();
        let l = ids_list(&ids);
        for t in &schema.types {
            let mut q = format!(
                "SELECT src FROM {} WHERE src IN ({l}) UNION ALL SELECT dst FROM {} WHERE dst IN ({l})",
                super::edge_table(&at.schema, g, *t),
                if many {
                    super::mirror_table(&at.schema, g, *t)
                } else {
                    super::edge_table(&at.schema, g, *t)
                }
            );
            q.push_str(" LIMIT 1");
            let conn = conns.conn(&at, true).await?;
            let hit: Option<u64> = conn.query_first(q).await.map_err(backend_error)?;
            if let Some(id) = hit {
                return Err(SprigError::new(
                    SprigCode::GraphNodeHasRelationships,
                    format!(
                        "[Neo.ClientError.Schema.ConstraintValidationFailed] Cannot delete node<{id}>, because it still has relationships. To delete this node, you must first delete its relationships."
                    ),
                ));
            }
        }
    }
    Ok(())
}
