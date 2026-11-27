//! What the executor needs from storage: the [`GraphStore`] trait over one native graph's node, edge
//! and label tables, implemented over MariaDB ([`crate::maria`]) and in
//! memory for tests ([`crate::mem`]).
//!
//! The trait keeps the shard dimension: nodes carry the routing
//! hash `rk` of `part`, edges live on their source's shard with `dst_rk`,
//! and [`GraphStore::reach`] is one shard's recursive CTE, which stops at
//! boundary nodes (an `rk` outside the shard) for the gateway to continue.

use async_trait::async_trait;
use serde_json::Value as Json;
use sprig_common::Result;
use sprig_shard::{KeyClass, KeySpec, KeyValue, hash_key};

use crate::ast::{Dir, Entity, IndexKind, Schema};

/// A node row.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRow {
    /// `id`.
    pub id: u64,
    /// Routing hash of `part`.
    pub rk: u64,
    /// Label ids.
    pub labels: Vec<u32>,
    /// `props`.
    pub props: Json,
    /// `_types`.
    pub types: Option<Json>,
}

/// An edge row.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeRow {
    /// `id`.
    pub id: u64,
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// The source's routing hash (`_sprig_rk`).
    pub src_rk: u64,
    /// `dst_rk`.
    pub dst_rk: u64,
    /// Type id.
    pub rtype: u32,
    /// `props`.
    pub props: Json,
    /// `_types`.
    pub types: Option<Json>,
}

/// What deleting an edge needs: its id, its ends and their routing
/// hashes (the edge lives on its source's shard, its mirror on its
/// destination's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeKey {
    /// `id`.
    pub id: u64,
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// Type id.
    pub rtype: u32,
    /// The source's routing hash.
    pub src_rk: u64,
    /// The destination's routing hash.
    pub dst_rk: u64,
}

impl From<&EdgeRow> for EdgeKey {
    fn from(e: &EdgeRow) -> Self {
        Self {
            id: e.id,
            src: e.src,
            dst: e.dst,
            rtype: e.rtype,
            src_rk: e.src_rk,
            dst_rk: e.dst_rk,
        }
    }
}

/// A node scan: the start point of a pattern.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    /// Only nodes with this label (the label table).
    pub label: Option<u32>,
    /// Only these ids.
    pub ids: Option<Vec<u64>>,
    /// `props.<key>` equal to one of the JSON values: a superset filter,
    /// rechecked in the gateway.
    pub props_eq: Vec<(String, Vec<Json>)>,
    /// At most this many rows (the row budget plus one).
    pub limit: Option<usize>,
    /// Paging (streamed results): only ids above this one, in id order.
    pub after: Option<u64>,
    /// Only these property keys (`None`: all), as
    /// [`GraphStore::nodes_part`] reads them; stores without late reads
    /// give all.
    pub keys: Option<Vec<String>>,
    /// Range conditions on properties (`<`, `>`, `STARTS WITH`): a
    /// superset filter a store with a range index may apply, rechecked in
    /// the executor.
    pub props_range: Vec<(String, crate::ordkey::RangePred)>,
    /// `(key, ascending, n)`: the statement keeps only the first `n` nodes
    /// in the order of property `key` (ties broken by id) and drops none
    /// of them afterwards; a store that can read them in that order may
    /// return just those (with every node tied with the last), each
    /// passing the scan's conditions exactly, in id order. Others ignore it.
    pub top: Option<(String, bool, usize)>,
}

/// A condition on one property, for the planner's estimates.
#[derive(Debug, Clone, PartialEq)]
pub enum PropPred {
    /// Equal to one of these values.
    Eq(Vec<crate::value::Value>),
    /// In a range.
    Range(crate::ordkey::RangePred),
    /// Present (not null).
    Present,
}

/// One row of a reach (CTE) call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reach {
    /// The origin node the search started from.
    pub origin: u64,
    /// The node reached.
    pub node: u64,
    /// Its routing hash.
    pub rk: u64,
    /// Hops from the seed (1-based), smallest found in this call.
    pub depth: u32,
}

/// One hop's link without properties ([`GraphStore::links`]): an edge's
/// ends, their routing hashes, its id and type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    /// Source node.
    pub src: u64,
    /// Destination node.
    pub dst: u64,
    /// The source's routing hash (where the edge lives).
    pub src_rk: u64,
    /// The destination's routing hash.
    pub dst_rk: u64,
    /// `id`.
    pub id: u64,
    /// Type id.
    pub rtype: u32,
}

impl From<&EdgeRow> for Link {
    fn from(e: &EdgeRow) -> Self {
        Self {
            src: e.src,
            dst: e.dst,
            src_rk: e.src_rk,
            dst_rk: e.dst_rk,
            id: e.id,
            rtype: e.rtype,
        }
    }
}

/// What [`GraphStore::links_far`] reads of the far nodes, and which it
/// may leave out (a superset the caller rechecks).
#[derive(Debug, Clone, Copy)]
pub struct FarRead<'a> {
    /// Property keys (`None`: all).
    pub keys: Option<&'a [String]>,
    /// Numeric comparisons of properties the far node must pass.
    pub filter: &'a [(String, CmpOp, f64)],
    /// When not empty, the only far nodes wanted.
    pub only: &'a [u64],
    /// Read the far nodes (else links alone, restricted to `only`).
    pub join: bool,
    /// The relationships' property keys to read too (`Some(&[])`: all).
    pub rel_keys: Option<&'a [String]>,
    /// Numeric comparisons of the relationships' properties.
    pub rel_filter: &'a [(String, CmpOp, f64)],
    /// With `join`: at least the links to the first `n` far nodes by
    /// property `key` (ascending when `true`) in Cypher's order, ties
    /// kept (a store may give more).
    pub top: Option<(&'a str, bool, usize)>,
    /// At most this many links per shard statement, in id order (a
    /// caller that gets this many reads on with `after`, or its own way).
    pub limit: Option<usize>,
    /// Only links with ids above this one.
    pub after: Option<u64>,
    /// With `limit`: in link id order (for a later `after`).
    pub ordered: bool,
    /// With `join`: the far nodes are joined only where a statement's
    /// near nodes have at most this many links; past it, that statement's
    /// links come without them (the caller reads those by id), in the
    /// same round trip. `None`: always joined.
    pub join_max: Option<usize>,
}

/// [`GraphStore::scan_links`]'s answer: the scan's nodes, the links from
/// them (`true`: outgoing; `None` when the store read them through mirrors
/// that turned out stale, so the hop reads them again), and, for a capped
/// joined read, the same links unjoined (at most [`FUSED_PLAIN_MAX`] per
/// shard statement).
pub type ScanLinks = (
    Vec<NodeRow>,
    Option<Vec<(FarLink, bool)>>,
    Option<Vec<(FarLink, bool)>>,
);

/// The unjoined links a capped start read brings along (per shard
/// statement): a start with more reads them in its hop.
pub const FUSED_PLAIN_MAX: usize = 2000;

/// [`GraphStore::links_deep`]'s answer.
#[derive(Debug, Default)]
pub struct DeepLinks {
    /// The outgoing links of the nodes asked about.
    pub first: Vec<Link>,
    /// Outgoing links of their far nodes.
    pub second: Vec<Link>,
    /// The far nodes all of whose outgoing links are in `second`.
    pub complete: std::collections::HashSet<u64>,
    /// Far nodes read with the links (both levels), when asked.
    pub nodes: Vec<NodeRow>,
}

/// A link [`GraphStore::links_far`] gives: the link, its far node when
/// read, and its relationship's properties and types when read.
pub type FarLink = (Link, Option<NodeRow>, Option<(Json, Option<Json>)>);

/// Which id counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    /// Node ids.
    Node,
    /// Relationship ids.
    Edge,
}

/// Counters a store keeps for `PROFILE` and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Statements sent to shards.
    pub shard_queries: u64,
    /// Of which recursive CTEs.
    pub cte_queries: u64,
    /// Time spent waiting for shard statements, in microseconds.
    pub sql_us: u64,
    /// Rows the shard statements returned.
    pub sql_rows: u64,
}

/// A property index or uniqueness constraint of a native graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// `graph_indexes.index_id`: names its columns (`_sprig_i<id>_<k>`).
    pub id: u32,
    /// The name (shared by a constraint and its index).
    pub name: String,
    /// A uniqueness constraint.
    pub unique: bool,
    /// Range or text.
    pub kind: IndexKind,
    /// Nodes or relationships.
    pub entity: Entity,
    /// The label or relationship type.
    pub token: String,
    /// Its id.
    pub token_id: u32,
    /// The properties.
    pub props: Vec<String>,
    /// `building` or `online`.
    pub state: String,
}

impl IndexDef {
    /// Whether `other` covers the same entity, token and properties.
    #[must_use]
    pub fn same_schema(&self, other: &IndexDef) -> bool {
        self.entity == other.entity && self.token == other.token && self.props == other.props
    }
}

/// The counters a schema command changed (`indexes-added`, ...).
pub type SchemaCounts = Vec<(&'static str, i64)>;

/// `CALL { } IN TRANSACTIONS` where batches can't commit on their own.
#[must_use]
pub fn not_implicit() -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::GraphUnsupported,
        "[Neo.DatabaseError.Transaction.TransactionStartFailed] A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, but tried to execute in an explicit transaction.",
    )
}

/// The routing hash of a node's `part` (the shard map's numeric class).
#[must_use]
pub fn rk_of(part: u64) -> u64 {
    // The spec of one numeric part never fails to build. It is made once:
    // building one computes its digest from its JSON text, which cost more
    // than the hash itself (a hop computes this for every relationship).
    static SPEC: std::sync::OnceLock<Option<KeySpec>> = std::sync::OnceLock::new();
    SPEC.get_or_init(|| KeySpec::new(vec![KeyClass::Numeric]).ok())
        .as_ref()
        .and_then(|s| hash_key(s, &[KeyValue::Int(i128::from(part))]).ok())
        .map_or(part, |(h, _)| h.0)
}

/// One native graph's storage, inside the caller's transaction.
#[async_trait]
pub trait GraphStore: Send {
    /// The graph id.
    fn graph_id(&self) -> u64;

    /// Number of shards.
    fn shards(&self) -> usize;

    /// The shard owning routing hash `rk`.
    fn shard_of(&self, rk: u64) -> usize;

    /// Statements run so far.
    fn stats(&self) -> StoreStats;

    /// Nodes matching `s`.
    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>>;

    /// Nodes by id (missing ids are skipped).
    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>>;

    /// One hop from `from` over `types` (empty: any) in `dir` (`Out` or
    /// `In`), with the far node joined when it is on the same shard.
    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>>;

    /// One hop from `from` as links only: no properties and no far
    /// nodes (breadth-first searches and trail enumeration read only
    /// ids; the elements they keep are read afterwards).
    async fn links(&mut self, from: &[u64], types: &[u32], dir: Dir) -> Result<Vec<Link>> {
        Ok(self
            .expand(from, types, dir)
            .await?
            .iter()
            .map(|(e, _)| Link::from(e))
            .collect())
    }

    /// Whether hops may read links and far nodes without properties,
    /// reading properties late ([`GraphStore::nodes_part`]); stores that
    /// say no answer every read with whole rows.
    fn late_props(&self) -> bool {
        false
    }

    /// Outgoing links of `from` of `types`, and of their far nodes too
    /// where the store can read them in the same statement (the next
    /// level of a walk): [`DeepLinks::complete`] names the far nodes whose
    /// own outgoing links of `types` are all in [`DeepLinks::second`].
    /// With `keys`, the far nodes of both levels with those properties
    /// ([`DeepLinks::nodes`], where the store can).
    ///
    /// # Errors
    /// The store's.
    async fn links_deep(
        &mut self,
        from: &[u64],
        types: &[u32],
        _keys: Option<&[String]>,
    ) -> Result<DeepLinks> {
        Ok(DeepLinks {
            first: self.links(from, types, Dir::Out).await?,
            ..DeepLinks::default()
        })
    }

    /// A walk read ahead: every outgoing link of `types` within `hops`
    /// levels of `from`, in one statement where the store can. The links
    /// of `from` are [`DeepLinks::first`], the deeper ones
    /// [`DeepLinks::second`]; [`DeepLinks::complete`] names the nodes whose
    /// outgoing links of `types` are all there (`from` among them); with
    /// `keys`, [`DeepLinks::nodes`] has the far nodes with those
    /// properties. `None`: the store can't, or the walk has more than
    /// `limit` links or took too long (the caller reads it level by
    /// level). With `next` (types, and keys of its far nodes), the
    /// clause's next hop from every node the walk is at comes too, as
    /// [`GraphStore::queue_next_hop`] would bring it.
    ///
    /// # Errors
    /// The store's.
    async fn walk_ahead(
        &mut self,
        _from: &[u64],
        _types: &[u32],
        _hops: u32,
        _keys: Option<&[String]>,
        _next: Option<(&[u32], Option<&[String]>)>,
        _limit: usize,
    ) -> Result<Option<DeepLinks>> {
        Ok(None)
    }

    /// A pattern's start scan and its first hop in one round trip: the
    /// nodes `s` finds, and the links of `types` from them in `dir` (with
    /// far nodes as `far` says). `None`: the store can't (the caller
    /// scans, then hops).
    ///
    /// # Errors
    /// The store's.
    async fn scan_links(
        &mut self,
        _s: &Scan,
        _types: &[u32],
        _dir: Dir,
        _far: &FarRead<'_>,
    ) -> Result<Option<ScanLinks>> {
        Ok(None)
    }

    /// Nodes `ids` with properties `keys` will be read: a store may read
    /// them with its next statement (in the same round trip) and answer
    /// [`GraphStore::nodes_part`] for them from that. Read-only statements
    /// only.
    fn queue_nodes(&mut self, _ids: &[u64], _keys: &[String]) {}

    /// A scan a later pattern of the clause will make: a store may send
    /// it with its next read statement (in the same round trip) and answer
    /// [`GraphStore::scan`] for the same conditions from that (with every
    /// property). Read-only statements only.
    fn queue_scan(&mut self, _s: &Scan) {}

    /// The pattern's next hop, outgoing over `types` from the far nodes of
    /// the hop about to be read, whose far nodes need `keys` (`None`:
    /// all): a store may read it with that hop when it joins the far
    /// nodes, and answer [`GraphStore::links_far`] for it from that (whole
    /// links, with their far nodes and labels). Read-only statements only.
    /// `None` takes back a request no hop used.
    fn queue_next_hop(&mut self, _next: Option<(&[u32], Option<&[String]>)>) {}

    /// The statement will read incoming links: a store whose mirrors need
    /// a freshness check may start it now, alongside the first reads.
    fn warm_mirrors(&mut self) {}

    /// Nodes by id with only `keys` of their properties (`Some(&[])`:
    /// none, just ids and labels; `None`: all). A node without one of
    /// the keys simply lacks it.
    async fn nodes_part(&mut self, ids: &[u64], keys: Option<&[String]>) -> Result<Vec<NodeRow>> {
        let _ = keys;
        self.nodes(ids).await
    }

    /// [`GraphStore::nodes_part`], leaving out (or not) nodes that fail
    /// `filter`: numeric comparisons of properties, a superset the
    /// caller rechecks.
    async fn nodes_where(
        &mut self,
        ids: &[u64],
        keys: Option<&[String]>,
        filter: &[(String, CmpOp, f64)],
    ) -> Result<Vec<NodeRow>> {
        let _ = filter;
        self.nodes_part(ids, keys).await
    }

    /// Edges by `(src_rk, id)` with only `keys` of their properties, as
    /// [`GraphStore::nodes_part`].
    async fn edges_part(
        &mut self,
        at: &[(u64, u64)],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        let _ = keys;
        self.edges_at(at).await
    }

    /// Expand(Into): the links of `types` (not empty) between each pair's
    /// near node and its far node, in `dir` from the near one (both ways
    /// for `Both`, a self-loop once), each with `true` when it goes out of
    /// the near node, in relationship-id order. A point check per pair
    /// rather than a read of every link of the near node. `None`: the
    /// store can't (the caller reads the near nodes' links).
    ///
    /// # Errors
    /// The store's.
    async fn links_between(
        &mut self,
        _pairs: &[(u64, u64)],
        _types: &[u32],
        _dir: Dir,
    ) -> Result<Option<Vec<(Link, bool)>>> {
        Ok(None)
    }

    /// [`GraphStore::links`] both ways in one step where the store can:
    /// each link with `true` when it goes out of `from`.
    async fn links_both(&mut self, from: &[u64], types: &[u32]) -> Result<Vec<(Link, bool)>> {
        let mut v: Vec<(Link, bool)> = self
            .links(from, types, Dir::Out)
            .await?
            .into_iter()
            .map(|l| (l, true))
            .collect();
        v.extend(
            self.links(from, types, Dir::In)
                .await?
                .into_iter()
                .map(|l| (l, false)),
        );
        Ok(v)
    }

    /// [`GraphStore::links_far`] both ways in one step where the store
    /// can: each link with `true` when it goes out of `from`.
    async fn links_far_both(
        &mut self,
        from: &[u64],
        types: &[u32],
        far: &FarRead<'_>,
    ) -> Result<Vec<(FarLink, bool)>> {
        let mut v: Vec<(FarLink, bool)> = self
            .links_far(from, types, Dir::Out, far)
            .await?
            .into_iter()
            .map(|l| (l, true))
            .collect();
        v.extend(
            self.links_far(from, types, Dir::In, far)
                .await?
                .into_iter()
                .map(|l| (l, false)),
        );
        Ok(v)
    }

    /// [`GraphStore::links`] with the far nodes read in the same step
    /// where the store can (`keys` of their properties: `None` all;
    /// links to far nodes failing `filter` may be left out). A link
    /// without its node is for the caller to read by id.
    async fn links_far(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        far: &FarRead<'_>,
    ) -> Result<Vec<FarLink>> {
        let _ = far;
        Ok(self
            .links(from, types, dir)
            .await?
            .into_iter()
            .map(|l| (l, None, None))
            .collect())
    }

    /// Edges by id whose sources' routing hashes are known: `(src_rk,
    /// id)` (routed to their shards; [`GraphStore::edges`] otherwise).
    async fn edges_at(&mut self, keys: &[(u64, u64)]) -> Result<Vec<EdgeRow>> {
        let ids: Vec<u64> = keys.iter().map(|(_, id)| *id).collect();
        self.edges(&ids).await
    }

    /// One shard's recursive CTE: from `seeds` (origin, node) on `shard`,
    /// up to `hops` hops over `types` in `dir`, each node once per origin
    /// at its smallest depth; nodes outside the shard are returned but not
    /// expanded.
    async fn reach(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>>;

    /// Edges by id.
    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>>;

    /// Edges touching `nodes` in either direction, at most `limit`.
    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>>;

    /// Locks node rows: shared (`LOCK IN SHARE MODE`) or exclusive (`FOR
    /// UPDATE`), and returns them (fresh).
    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>>;

    /// Locks relationship records exclusively and returns them as they
    /// are now (before `SET` on them). Stores without record locks read
    /// them as [`GraphStore::edges`] does.
    async fn lock_edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        self.edges(ids).await
    }

    /// `n` new ids.
    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>>;

    /// Inserts nodes and their label rows.
    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()>;

    /// Inserts edges.
    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()>;

    /// Rewrites a node's labels and properties (label rows follow).
    async fn update_node(&mut self, row: &NodeRow, old_labels: &[u32]) -> Result<()>;

    /// Rewrites an edge's properties.
    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()>;

    /// Deletes edges (and their mirrors).
    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64>;

    /// Deletes nodes (and their label rows) by id.
    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64>;

    /// The id of a label; with `create`, a new label gets one.
    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>>;

    /// The id of a relationship type; with `create`, a new one gets one.
    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>>;

    /// A label's name.
    fn label_name(&self, id: u32) -> Option<String>;

    /// A type's name.
    fn type_name(&self, id: u32) -> Option<String>;

    /// Every label, by name.
    fn all_labels(&self) -> Vec<String>;

    /// Every relationship type, by name.
    fn all_types(&self) -> Vec<String>;

    /// The planner's estimate of the nodes with `label` (`None`: every
    /// node); `None` when the store keeps no statistics.
    fn est_nodes(&self, _label: Option<u32>) -> Option<f64> {
        None
    }

    /// The planner's estimate of the relationships of `types` (empty:
    /// any) from nodes with label `near` to nodes with label `far` (`None`:
    /// any), in `dir` from the near side.
    fn est_rels(
        &self,
        _near: Option<u32>,
        _types: &[u32],
        _dir: Dir,
        _far: Option<u32>,
    ) -> Option<f64> {
        None
    }

    /// The planner's estimate of the share of `label`'s nodes whose
    /// property `key` passes `pred`.
    fn est_prop(&self, _label: Option<u32>, _key: &str, _pred: &PropPred) -> Option<f64> {
        None
    }

    /// Runs a statistics pass now (`CALL sprig.graph.analyze()`): the
    /// nodes and relationships it counted.
    ///
    /// # Errors
    /// 39901 where the store keeps no statistics.
    fn analyze(&mut self) -> Result<(f64, f64)> {
        Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::GraphUnsupported,
            "this graph keeps no planner statistics",
        ))
    }

    /// Whether an online index with ordered keys on `label`'s property
    /// `key` (alone) exists: scans can then read ranges and orders of it.
    fn ordered_index(&self, _label: u32, _key: &str) -> bool {
        false
    }

    /// `EXPLAIN`'s lines for how the router plans this graph's steps: its
    /// group and shards and each step's rule; none for
    /// stores the router doesn't plan (in memory, mapped graphs).
    fn explain_routes(&self) -> Vec<String> {
        Vec::new()
    }

    /// `PROFILE`'s lines for the routes the statement ran: per step and
    /// plan kind, how many and on which shards.
    fn routes(&self) -> Vec<String> {
        Vec::new()
    }

    /// `CALL { } IN TRANSACTIONS`: whether the statement may
    /// commit batches of its own, checked before the first batch. Only an
    /// auto-commit statement may (Neo4j's rule).
    ///
    /// # Errors
    /// Neo4j's `TransactionStartFailed` (39901) when it may not.
    async fn batches(&mut self) -> Result<()> {
        Err(not_implicit())
    }

    /// Ends the current batch: commits it (`ok`; nodes `deleted` by a
    /// plain `DELETE` in it must have no relationships left) or rolls it
    /// back. The statement's next step starts a new transaction.
    ///
    /// # Errors
    /// The commit's (the batch is then rolled back).
    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        let _ = (ok, deleted);
        Err(not_implicit())
    }

    /// The graph's indexes and constraints.
    fn indexes(&self) -> Vec<IndexDef> {
        Vec::new()
    }

    /// Runs an index or constraint command (outside the data
    /// transaction): the counters it changed.
    ///
    /// # Errors
    /// 39901 where the store has no schema support; 39908 for a name in
    /// use or an equivalent index; 1062 when existing data violates a new
    /// constraint.
    async fn schema(&mut self, _cmd: &Schema) -> Result<SchemaCounts> {
        Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::GraphUnsupported,
            "index and constraint commands are not supported on this store",
        ))
    }
}

/// A numeric comparison of a pushed-down filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `<`.
    Lt,
    /// `<=`.
    Le,
    /// `>`.
    Gt,
    /// `>=`.
    Ge,
    /// `=`.
    Eq,
    /// Keeps the row unless the value is a JSON number below the given
    /// one (any other value, or none, passes): a sort bound, which must
    /// not drop the strings and `null`s Cypher orders elsewhere.
    NotNumBelow,
    /// The same for a JSON number above the given one.
    NotNumAbove,
}

impl CmpOp {
    /// The SQL operator.
    #[must_use]
    pub fn sql(self) -> &'static str {
        match self {
            Self::Lt | Self::NotNumBelow => "<",
            Self::Le => "<=",
            Self::Gt | Self::NotNumAbove => ">",
            Self::Ge => ">=",
            Self::Eq => "=",
        }
    }
}

/// A filter key naming a required label instead of a property:
/// `LABEL_KEY` then the label id (the value is ignored).
pub const LABEL_KEY: &str = "\u{1}label:";
