//! Graph hops as router routes (the graph store's
//! `Step::Shard(RouteSpec)`).
//!
//! The native graph store ([`sprig-graph`'s `MariaStore`]) no longer
//! decides which shards a hop reads or writes the statements itself: each
//! read step is a [`RouteSpec`] (the graph's table group from the router's
//! map cache, what the step reads, and how its targets are chosen), and
//! [`plan_route`] turns it into the router's own [`Plan`]:
//!
//! ```text
//! RouteSpec { group, graph, op, targets }
//!   targets ByKey(node ids)    → each id hashed with the group's key spec (part = id),
//!                                routed to its shard; each shard's statement names only
//!                                its own ids (narrowing)
//!   targets ByHash(hash, id)   → ids whose shard a hash already names (an edge's source)
//!   targets All(ids)           → every shard, each with every id (a scatter: incoming
//!                                hops without fresh mirrors, edges by id)
//!   targets Shard(i)           → one shard (a recursive CTE burst on the frontier's shard)
//! plan_route(spec) → Route { plan: Pass(target) | Read(Scatter, targets), held_until_ms }
//!   each Target: cluster, shard id, the statement with that shard's physical names,
//!   ShardAt (group, db, lo, schema) for the transaction's branches
//! ```
//!
//! `GRAPH_TABLE` statements and Bolt statements therefore share one
//! planner: the calls' hops are routes, and the outer statement is planned
//! by [`crate::planner::plan_graph`]. A route over a range a mover cutover
//! holds reports the hold's end ([`Route::held_until_ms`]); the gateway
//! waits for it before an auto-commit read, as it does for SQL. Values are
//! written into the text: integers as digits, strings as `_utf8mb4 X'…'`
//! (no escaping, whatever the session's `sql_mode`).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use crate::plan::{Plan, ReadKind, ReadPlan, Target};
use crate::topology::{Group, KeyValue};

/// The node columns every node read returns, in the store's order.
pub const NODE_COLS: &str = "n.id, n._sprig_rk, n.labels, n.props, n._types";
/// The edge columns every edge read returns, in the store's order.
pub const EDGE_COLS: &str = "e.src, e.type, e.dst, e.id, e.dst_rk, e._sprig_rk, e.props, e._types";
/// Edge ids per statement after a mirror read: larger lists split into
/// several targets on the same shard.
pub const IDS_PER_STATEMENT: usize = 1000;

/// One of a native graph's tables in a shard schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphTable {
    /// `__sprig_gn_<graph>`.
    Nodes,
    /// `__sprig_ge_<graph>`.
    Edges,
    /// `__sprig_gl_<graph>`.
    Labels,
    /// `__sprig_gm_<graph>`.
    Mirrors,
}

/// The quoted, qualified name of `t` of graph `graph` in `schema` (the
/// names `sprig-graph`'s `Tables::of` gives).
#[must_use]
pub fn table(schema: &str, graph: u64, t: GraphTable) -> String {
    let p = match t {
        GraphTable::Nodes => "gn",
        GraphTable::Edges => "ge",
        GraphTable::Labels => "gl",
        GraphTable::Mirrors => "gm",
    };
    format!("`{}`.`__sprig_{p}_{graph}`", schema.replace('`', "``"))
}

/// A value written into a route's statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lit {
    /// An integer.
    Int(i64),
    /// A string: `_utf8mb4 X'…'`.
    Str(String),
}

impl Lit {
    /// The SQL literal.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Int(i) => i.to_string(),
            Self::Str(s) => {
                let mut o = String::with_capacity(s.len() * 2 + 12);
                o.push_str("_utf8mb4 X'");
                for b in s.as_bytes() {
                    let _ = write!(o, "{b:02X}");
                }
                o.push('\'');
                o
            }
        }
    }
}

/// How a route's targets are chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Targets {
    /// Node ids, each to the shard of its routing hash (`part` = `id`).
    ByKey(Vec<u64>),
    /// Ids to the shard a known routing hash names: `(hash, id)`.
    ByHash(Vec<(u64, u64)>),
    /// Every shard, each with every id (none: a scan of every shard).
    All(Vec<u64>),
    /// One shard, by its index in the group.
    Shard(usize),
}

/// A node scan's conditions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanOp {
    /// Only nodes with this label (the label table).
    pub label: Option<u32>,
    /// An online index on the label and a pushed-down property: its column
    /// and the values' index texts (it replaces the label join).
    pub index: Option<(String, Vec<String>)>,
    /// `JSON_VALUE(n.props, '$."k"') IN (...)`: a superset, rechecked.
    pub props: Vec<(String, Vec<Lit>)>,
    /// Paging: ids above this one, in id order.
    pub after: Option<u64>,
    /// At most this many rows per shard.
    pub limit: Option<usize>,
    /// Only these property keys (`props` and `_types` of them alone, as
    /// [`HopOp::NodesPart`] reads them); `None`: every property.
    pub keys: Option<Vec<String>>,
}

/// The far nodes a [`HopOp::Links`] statement joins: after the six link
/// columns come `id, rk, labels, props, _types` of the far node, `NULL`s
/// when it lives on a shard of another cluster (the caller reads those by
/// id). Shard schemas on one cluster are one server, so the join reaches
/// every far node there in the same round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FarNodes {
    /// Property keys to read (`None`: all; empty: none).
    pub keys: Option<Vec<String>>,
    /// Numeric comparisons the far nodes must pass, as in
    /// [`HopOp::NodesPart`]: links to a joined far node that fails one
    /// are left out.
    pub filter: Vec<(String, CmpOp, String)>,
    /// When not empty: only links to these far nodes (the ids a `v IN
    /// list` of the clause allows).
    pub only: Vec<u64>,
    /// Join the far nodes (else no node columns).
    pub join: bool,
    /// The relationships' properties too: `props, _types` of these keys
    /// (`Some(&[])`: all of them) as the last two columns; `None`: no
    /// such columns. Incoming hops through the mirrors join the edges
    /// from the shard schemas of the statement's cluster (`NULL`s for
    /// edges elsewhere).
    pub rel_keys: Option<Vec<String>>,
    /// Numeric comparisons of the relationships' properties, as `filter`
    /// is of the far nodes'.
    pub rel_filter: Vec<(String, CmpOp, String)>,
    /// With `join`: only the links whose far nodes are among the first
    /// `n` by property `key` (ascending when `true`), ties kept, as Cypher
    /// orders values ([`top_parts`]).
    pub top: Option<(String, bool, usize)>,
    /// Without `top`: at most this many links per statement.
    pub limit: Option<usize>,
    /// Outgoing links only: also the far nodes' own outgoing links of the
    /// same types (the next level of a walk), left joined from the edges
    /// of every shard schema on the statement's cluster: six more link
    /// columns (`NULL`s when the far node has none or isn't on the
    /// cluster), then whether the far node is on it; with `join`, then the
    /// second level's far nodes as the first's.
    pub deep: bool,
    /// The near nodes are the ones this scan finds on the statement's
    /// shard (a subquery in place of the ids, so a pattern's start and
    /// its first hop go in one round trip). Only for links on the near
    /// nodes' own shards: outgoing, or incoming through the mirrors.
    pub near_scan: Option<Box<ScanOp>>,
    /// Only links with ids above this one (a continuation of a read that
    /// stopped at `limit` in id order).
    pub after: Option<u64>,
    /// With `limit`: in link id order (so a reader can continue `after`).
    pub ordered: bool,
    /// With `join` (no `top`, `deep` or `ordered`): the far nodes are
    /// joined only where the statement's near nodes have at most this many
    /// links; past it, the same statement gives its links with `NULL`
    /// far node columns (at most `limit`), and the caller reads those
    /// nodes by id. One statement either way: `(<joined> AND <probe> IS
    /// NULL LIMIT n) UNION ALL (<links> AND <probe> IS NOT NULL)`, where
    /// the probe (the near nodes' link past the `n`th, if any) is a
    /// constant MariaDB reads first, so only one part runs. Over many
    /// links the joins cost more than reading the far nodes by id in
    /// parallel, and a capped joined read had to be read again.
    pub join_max: Option<usize>,
    /// With `join` (not `deep`): the far nodes' own outgoing links of
    /// another type, the pattern's next hop, left joined from the edges
    /// of every shard schema on the statement's cluster with their far
    /// nodes ([`NextHop`]): six more link columns, then `id, rk, labels,
    /// props, _types` of the next far node (`NULL`s for a far node without
    /// such links, or not on the cluster). A row per pair; a joined part
    /// of a `join_max` statement has no `LIMIT` then (its links are
    /// bounded already).
    pub next: Option<NextHop>,
}

/// The next hop a [`FarNodes`] read brings along.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextHop {
    /// Type ids (empty: any).
    pub types: Vec<u32>,
    /// Property keys of its far nodes (`None`: all; empty: none). Their
    /// labels come too; the caller checks the pattern's.
    pub keys: Option<Vec<String>>,
}

/// The statement parts of [`FarNodes::top`] over far node columns `fid`
/// and `fprops`: `(top, rest, order)`. The statement becomes `(<links>
/// AND top ORDER ... FETCH FIRST n ROWS WITH TIES) UNION ALL (<links> AND
/// rest)`: a superset of the first `n` links in Cypher's order of `key`.
/// The first part ranks the far nodes it can: numbers by value; every
/// other kind of value ties within its class (strings, booleans, temporal
/// values and lists before the numbers ascending, after them descending;
/// a missing value, Cypher's `null`, after them ascending, before them
/// descending). The second keeps every link it can't rank: far nodes not
/// on the statement's cluster, and JSON `null` values (NaN and the
/// infinities, which JSON can't hold).
#[must_use]
pub fn top_parts(
    fid: &str,
    fprops: &str,
    key: &str,
    asc: bool,
    n: usize,
) -> Option<(String, String, String)> {
    if key.contains(['"', '\\']) || n == 0 {
        return None;
    }
    let path = Lit::Str(format!("$.\"{key}\"")).sql();
    let t = format!("JSON_TYPE(JSON_EXTRACT({fprops}, {path}))");
    let num = format!("{t} IN ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE')");
    let class = if asc {
        format!("CASE WHEN {num} THEN 2 WHEN {t} IS NULL THEN 3 ELSE 1 END")
    } else {
        format!("CASE WHEN {t} IS NULL THEN 0 WHEN {num} THEN 1 ELSE 2 END")
    };
    let dir = if asc { "ASC" } else { "DESC" };
    Some((
        format!("{fid} IS NOT NULL AND NOT ({t} <=> 'NULL')"),
        format!("({fid} IS NULL OR {t} <=> 'NULL')"),
        format!(
            " ORDER BY {class}, CASE WHEN {num} THEN JSON_VALUE({fprops}, {path}) + 0e0 END {dir} \
             FETCH FIRST {n} ROWS WITH TIES"
        ),
    ))
}

/// `sql` with `tag AS q` as its first column, for several routes' rows in
/// one result: a `SELECT`, or each part of a `UNION ALL` of parenthesized
/// `SELECT`s (a top-k or `join_max` hop's statement), never a subquery's.
/// `None` for any other statement.
#[must_use]
pub fn tagged(sql: &str, tag: usize) -> Option<String> {
    if let Some(rest) = sql.strip_prefix("SELECT ") {
        return Some(format!("(SELECT {tag} AS q, {rest})"));
    }
    let rest = sql.strip_prefix("(SELECT ")?;
    let head = format!("(SELECT {tag} AS q, ");
    Some(format!(
        "{head}{}",
        rest.replace(") UNION ALL (SELECT ", &format!(") UNION ALL {head}"))
    ))
}

/// A walk read ahead in one statement: every outgoing link of `types`
/// within `hops` levels of `seeds`, as a recursive CTE over the edge tables
/// of every shard schema (all of the group's shards on one cluster; `None`
/// otherwise, or for no seeds). Each link comes once, at the first level
/// that reaches it: `src, dst, src_rk, dst_rk, id, type, level` (1 for the
/// seeds' own links), then, with `far`, its far node's `id, rk, labels`
/// and `props, _types` of those keys. A node first reached below `hops`
/// has all its links in the rows. At most `limit` rows, and at most
/// `max_ms` of the server's time (it stops the statement past that).
///
/// With `next`, the clause's next hop from the walk's ends comes too:
/// every row starts with a kind, 0 for a walk row (its next columns
/// `NULL`) and 1 for a node the walk reaches or starts from (its walk
/// columns `NULL`): that node's id, then one of its outgoing links of
/// `next.types` (six columns, `NULL`s when it has none) and the link's
/// far node (`id, rk, labels, props, _types`).
#[allow(clippy::too_many_arguments)] // the walk's seeds, types, depth, far nodes and bounds
#[must_use]
pub fn walk_statement(
    group: &Group,
    graph: u64,
    seeds: &[u64],
    types: &[u32],
    hops: u32,
    far: Option<&[String]>,
    next: Option<&NextHop>,
    limit: usize,
    max_ms: u64,
) -> Option<String> {
    let cluster = &group.shards.first()?.cluster;
    if seeds.is_empty() || hops == 0 || group.shards.iter().any(|s| &s.cluster != cluster) {
        return None;
    }
    let mut per: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for &id in seeds {
        let i = node_hash(group, id).map_or(0, |h| group.route(h));
        per.entry(i).or_default().push(id);
    }
    let range = |j: usize, rk: &str| {
        let mut c = format!("{rk} >= {}", group.shards[j].lo);
        if let Some(next) = group.shards.get(j + 1) {
            let _ = write!(c, " AND {rk} < {}", next.lo);
        }
        c
    };
    let edges = |j: usize| table(&group.shards[j].schema, graph, GraphTable::Edges);
    let tf = types_filter("e", types);
    let mut parts: Vec<String> = Vec::new();
    for (i, ids) in &per {
        parts.push(format!(
            "SELECT e.src, e.dst, e._sprig_rk, e.dst_rk, e.id, e.type, 1 FROM {} e \
             WHERE e.src IN ({}){tf}",
            edges(*i),
            ids_list(ids)
        ));
    }
    for j in 0..group.shards.len() {
        parts.push(format!(
            "SELECT e.src, e.dst, e._sprig_rk, e.dst_rk, e.id, e.type, w.d + 1 FROM w \
             JOIN {} e ON e.src = w.dst{tf} WHERE w.d < {hops} AND {}",
            edges(j),
            range(j, "w.dst_rk")
        ));
    }
    let co = |v: &[String]| {
        if v.len() == 1 {
            v[0].clone()
        } else {
            format!("COALESCE({})", v.join(", "))
        }
    };
    let mut select = String::from("w.src, w.dst, w.src_rk, w.dst_rk, w.id, w.type, w.d");
    let mut joins = String::new();
    if let Some(keys) = far {
        let (mut ids, mut labels, mut props, mut tys) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for j in 0..group.shards.len() {
            let _ = write!(
                joins,
                " LEFT JOIN {} n{j} ON n{j}.id = w.dst AND {}",
                table(&group.shards[j].schema, graph, GraphTable::Nodes),
                range(j, "w.dst_rk")
            );
            ids.push(format!("n{j}.id"));
            labels.push(format!("n{j}.labels"));
            props.push(format!("n{j}.props"));
            tys.push(format!("n{j}._types"));
        }
        let _ = write!(
            select,
            ", {}, w.dst_rk, {}, {}",
            co(&ids),
            co(&labels),
            part_exprs(&co(&props), &co(&tys), keys)
        );
    }
    let mut body = format!("SELECT {select} FROM w{joins}");
    if let Some(nx) = next {
        // The nodes the walk is at (its seeds and every node it reaches),
        // each with its next hop's links and their far nodes.
        let mut at = String::from("SELECT dst AS n, dst_rk AS rk FROM w");
        for &id in seeds {
            let _ = write!(
                at,
                " UNION SELECT {id}, {}",
                node_hash(group, id).unwrap_or(0)
            );
        }
        let mut xs: [Vec<String>; 6] = Default::default();
        let mut njoins = String::new();
        for j in 0..group.shards.len() {
            let _ = write!(
                njoins,
                " LEFT JOIN {} x{j} ON x{j}.src = z.n AND {}{}",
                edges(j),
                range(j, "z.rk"),
                types_filter(&format!("x{j}"), &nx.types)
            );
            for (c, name) in xs
                .iter_mut()
                .zip(["src", "dst", "_sprig_rk", "dst_rk", "id", "type"])
            {
                c.push(format!("x{j}.{name}"));
            }
        }
        let (xdst, xrk) = (co(&xs[1]), co(&xs[3]));
        let (mut ids, mut labels, mut props, mut tys) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for j in 0..group.shards.len() {
            let _ = write!(
                njoins,
                " LEFT JOIN {} y{j} ON y{j}.id = {xdst} AND {}",
                table(&group.shards[j].schema, graph, GraphTable::Nodes),
                range(j, &xrk)
            );
            ids.push(format!("y{j}.id"));
            labels.push(format!("y{j}.labels"));
            props.push(format!("y{j}.props"));
            tys.push(format!("y{j}._types"));
        }
        let (yprops, ytypes) = (co(&props), co(&tys));
        let pcols = match &nx.keys {
            None => format!("{yprops}, {ytypes}"),
            Some(keys) => part_exprs(&yprops, &ytypes, keys),
        };
        let xcols: Vec<String> = xs.iter().map(|c| co(c)).collect();
        let walk_nulls = if far.is_some() { 12 } else { 7 };
        body = format!(
            "SELECT 0, {select}, {} FROM w{joins} UNION ALL SELECT 1, {}, z.n, {}, {}, {xrk}, {}, {pcols} \
             FROM ({at}) z{njoins}",
            ["NULL"; 12].join(", "),
            vec!["NULL"; walk_nulls].join(", "),
            xcols.join(", "),
            co(&ids),
            co(&labels)
        );
    }
    Some(format!(
        "SET STATEMENT max_recursive_iterations = {}, max_statement_time = {}.{:03} FOR \
         WITH RECURSIVE w (src, dst, src_rk, dst_rk, id, type, d) AS ({}) CYCLE id RESTRICT \
         {body} LIMIT {limit}",
        hops + 1,
        max_ms / 1000,
        max_ms % 1000,
        parts.join(" UNION ALL ")
    ))
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

/// What a route reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HopOp {
    /// Nodes by id.
    Nodes,
    /// Nodes by id, locked (`FOR UPDATE` or `LOCK IN SHARE MODE`).
    LockNodes {
        /// `FOR UPDATE`.
        exclusive: bool,
    },
    /// A node scan (the start of a pattern).
    Scan(ScanOp),
    /// One hop: edges whose near end is one of the ids, the far node
    /// left-joined when it is on the same shard. Outgoing reads the
    /// sources' shards by key; incoming without mirrors scatters the `rev`
    /// key.
    Expand {
        /// Outgoing (`src` is near).
        out: bool,
        /// Type ids (empty: any).
        types: Vec<u32>,
    },
    /// One hop's links without properties or far nodes: `src, dst,
    /// src_rk, dst_rk, id, type` of the edges whose near end is one of the
    /// ids. Outgoing reads the edges on the sources' shards by key;
    /// incoming reads the mirrors on the destinations' shards by key
    /// (`mirror`), or scatters the edges' `rev` key.
    Links {
        /// Outgoing (`src` is near).
        out: bool,
        /// Incoming through the mirrors.
        mirror: bool,
        /// Type ids (empty: any).
        types: Vec<u32>,
        /// The far nodes too, joined from every shard schema on the
        /// statement's cluster ([`FarNodes`]); `None`: links alone.
        far: Option<FarNodes>,
    },
    /// Nodes by id with only some of their properties (a late read):
    /// `id, rk, labels`, then `props` and `_types` as objects of `keys`
    /// alone (a key a node lacks is `null` in them); no keys: neither.
    NodesPart {
        /// The property keys.
        keys: Vec<String>,
        /// Numeric comparisons the nodes must pass (a superset the
        /// gateway rechecks): `(key, op, number)`, the number as its
        /// decimal text.
        filter: Vec<(String, CmpOp, String)>,
    },
    /// Edges by id (routed by their sources' hashes) with only some of
    /// their properties, as [`HopOp::NodesPart`] reads nodes.
    EdgesPart {
        /// The property keys.
        keys: Vec<String>,
    },
    /// Mirror rows `(id, src_rk)` of edges into the ids, on the
    /// destinations' shards.
    Mirrors {
        /// Type ids (empty: any).
        types: Vec<u32>,
    },
    /// Edges by id with their source node, on the sources' shards.
    EdgesWithSrc,
    /// Edges by id.
    Edges,
    /// Edges touching the ids: outgoing by key or incoming by scatter, at
    /// most `limit` per statement.
    Touching {
        /// Outgoing.
        out: bool,
        /// The `LIMIT`.
        limit: usize,
    },
    /// One shard's recursive CTE from `seeds` (origin, node).
    Reach {
        /// The seeds.
        seeds: Vec<(u64, u64)>,
        /// Type ids (empty: any).
        types: Vec<u32>,
        /// Outgoing.
        out: bool,
        /// Up to this many hops.
        hops: u32,
    },
}

impl HopOp {
    /// Snake-case name for metrics and EXPLAIN.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::LockNodes { .. } => "lock_nodes",
            Self::Scan(_) => "scan",
            Self::Expand { out: true, .. } => "expand_out",
            Self::Expand { out: false, .. } => "expand_in",
            Self::Links { out: true, .. } => "links_out",
            Self::Links { mirror: true, .. } => "links_in_mirror",
            Self::Links { .. } => "links_in",
            Self::NodesPart { .. } => "nodes_part",
            Self::EdgesPart { .. } => "edges_part",
            Self::Mirrors { .. } => "mirrors",
            Self::EdgesWithSrc => "edges_with_src",
            Self::Edges => "edges",
            Self::Touching { .. } => "touching",
            Self::Reach { .. } => "reach",
        }
    }
}

/// A graph step for the router to plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    /// The graph's table group as the router's map cache has it.
    pub group: Arc<Group>,
    /// The graph id (its tables' names).
    pub graph: u64,
    /// What it reads.
    pub op: HopOp,
    /// How the targets are chosen.
    pub targets: Targets,
}

/// A planned route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    /// The step, for metrics and EXPLAIN.
    pub op: &'static str,
    /// `Pass` (one target), or a `Scatter` read (none or several).
    pub plan: Plan,
    /// While a mover cutover holds a range a target reads: the hold's end
    /// (ms since the epoch).
    pub held_until_ms: Option<u64>,
}

impl Route {
    /// The statements, in order.
    #[must_use]
    pub fn targets(&self) -> &[Target] {
        match &self.plan {
            Plan::Pass(t) => std::slice::from_ref(t),
            Plan::Read(r) => &r.targets,
            _ => &[],
        }
    }

    /// One EXPLAIN line: the step, the plan kind and its shards.
    #[must_use]
    pub fn line(&self) -> String {
        let mut shards: Vec<String> = Vec::new();
        for t in self.targets() {
            let s = format!("s{}@{}", t.shard, t.cluster);
            if !shards.contains(&s) {
                shards.push(s);
            }
        }
        format!(
            "GraphRoute {}: {} over {} statement(s) on {}",
            self.op,
            self.plan.kind(),
            self.targets().len(),
            if shards.is_empty() {
                "no shard".to_owned()
            } else {
                shards.join(", ")
            }
        )
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

fn types_filter(alias: &str, types: &[u32]) -> String {
    if types.is_empty() {
        return String::new();
    }
    let t: Vec<u64> = types.iter().map(|x| u64::from(*x)).collect();
    format!(" AND {alias}.type IN ({})", ids_list(&t))
}

/// `props` and `_types` columns for only `keys` (`NULL, NULL` for none):
/// `JSON_OBJECT` of each key's `JSON_EXTRACT`, names and paths as hex
/// literals, and the whole `_types`. A key with `"` or `\` in it can't be a path, and is
/// left out (callers read every property for such keys).
fn part_cols(alias: &str, keys: &[String]) -> String {
    part_exprs(&format!("{alias}.props"), &format!("{alias}._types"), keys)
}

/// `JSON_VALUE(<props>, '$."k"') <op> <v>`, or `None` for a key that
/// can't be a path or a value that isn't a decimal number.
/// A filter key naming a required label instead of a property:
/// `LABEL_KEY` then the label id (the value is ignored).
pub const LABEL_KEY: &str = "\u{1}label:";

/// A node filter: [`number_filter`] on a property, or for a
/// [`LABEL_KEY`] key, `JSON_CONTAINS(<labels>, '<id>')`.
fn node_filter(props: &str, labels: &str, k: &str, op: CmpOp, v: &str) -> Option<String> {
    match k.strip_prefix(LABEL_KEY) {
        Some(id) => id
            .parse::<u32>()
            .ok()
            .map(|id| format!("JSON_CONTAINS({labels}, '{id}')")),
        None => number_filter(props, k, op, v),
    }
}

fn number_filter(props: &str, k: &str, op: CmpOp, v: &str) -> Option<String> {
    let number = !v.is_empty()
        && v.chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | '.' | 'e' | 'E' | '+'));
    if k.contains(['"', '\\']) || !number {
        return None;
    }
    let path = Lit::Str(format!("$.\"{k}\"")).sql();
    Some(match op {
        CmpOp::NotNumBelow | CmpOp::NotNumAbove => format!(
            "NOT (JSON_TYPE(JSON_EXTRACT({props}, {path})) IN ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE') \
             AND JSON_VALUE({props}, {path}) {} {v})",
            op.sql()
        ),
        _ => format!("JSON_VALUE({props}, {path}) {} {v}", op.sql()),
    })
}

/// [`part_cols`] over the expressions `props` and `types`.
fn part_exprs(props: &str, types: &str, keys: &[String]) -> String {
    let (p, t) = part_pair(props, types, keys);
    format!("{p}, {t}")
}

/// [`part_exprs`] as its two expressions.
fn part_pair(props: &str, types: &str, keys: &[String]) -> (String, String) {
    let keys: Vec<&String> = keys.iter().filter(|k| !k.contains(['"', '\\'])).collect();
    if keys.is_empty() {
        return ("NULL".into(), "NULL".into());
    }
    let obj = |col: &str| {
        let mut s = String::from("JSON_OBJECT(");
        for (i, k) in keys.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            let _ = write!(
                s,
                "{}, JSON_EXTRACT({col}, {})",
                Lit::Str((*k).clone()).sql(),
                Lit::Str(format!("$.\"{k}\"")).sql()
            );
        }
        s.push(')');
        s
    };
    // `_types` keys are paths (`dates[0]`, `m.k`): the whole map, which is
    // small (only values JSON can't type), not the keys' entries.
    (obj(props), types.to_owned())
}

/// The routing hash of node id `id` in `group` (the numeric key class;
/// `part` = `id`).
#[must_use]
pub fn node_hash(group: &Group, id: u64) -> Option<u64> {
    group.hash_of(&KeyValue::Int(i128::from(id)))
}

/// Shard `i`'s statement for `op` over `ids`; `None` when it can match
/// nothing.
fn statement(spec: &RouteSpec, i: usize, ids: &[u64]) -> Option<String> {
    let g = &spec.group;
    let schema = &g.shards.get(i)?.schema;
    let t = |x| table(schema, spec.graph, x);
    let nodes = t(GraphTable::Nodes);
    let edges = t(GraphTable::Edges);
    // A statement over no ids matches nothing; links whose near nodes
    // are a scan's name none.
    let scanned = matches!(&spec.op, HopOp::Links { far: Some(f), .. } if f.near_scan.is_some());
    if ids.is_empty() && !scanned && !matches!(spec.op, HopOp::Scan(_) | HopOp::Reach { .. }) {
        return None;
    }
    let id_list = ids_list(ids);
    Some(match &spec.op {
        HopOp::Nodes => format!("SELECT {NODE_COLS} FROM {nodes} n WHERE n.id IN ({id_list})"),
        HopOp::LockNodes { exclusive } => format!(
            "SELECT {NODE_COLS} FROM {nodes} n WHERE n.id IN ({id_list}) {}",
            if *exclusive {
                "FOR UPDATE"
            } else {
                "LOCK IN SHARE MODE"
            }
        ),
        HopOp::Scan(s) => {
            let mut sql = match &s.keys {
                None => format!("SELECT {NODE_COLS} FROM {nodes} n"),
                Some(keys) => format!(
                    "SELECT n.id, n._sprig_rk, n.labels, {} FROM {nodes} n",
                    part_cols("n", keys)
                ),
            };
            if let Some((col, texts)) = &s.index {
                if texts.is_empty() {
                    return None;
                }
                let vals: Vec<String> = texts.iter().map(|x| Lit::Str(x.clone()).sql()).collect();
                let _ = write!(
                    sql,
                    " WHERE 1 = 1 AND n.`{}` IN ({})",
                    col.replace('`', "``"),
                    vals.join(", ")
                );
            } else if let Some(l) = s.label {
                let _ = write!(
                    sql,
                    " JOIN {} l ON l.id = n.id AND l.label = {l} WHERE 1 = 1",
                    t(GraphTable::Labels)
                );
            } else {
                sql.push_str(" WHERE 1 = 1");
            }
            if matches!(spec.targets, Targets::ByKey(_) | Targets::ByHash(_)) || !ids.is_empty() {
                if ids.is_empty() {
                    return None;
                }
                let _ = write!(sql, " AND n.id IN ({id_list})");
            }
            for (k, vals) in &s.props {
                if vals.is_empty() || k.contains(['"', '\\', '\'']) {
                    continue;
                }
                let v: Vec<String> = vals.iter().map(Lit::sql).collect();
                let _ = write!(
                    sql,
                    " AND JSON_VALUE(n.props, '$.\"{k}\"') IN ({})",
                    v.join(", ")
                );
            }
            if let Some(a) = s.after {
                let _ = write!(sql, " AND n.id > {a} ORDER BY n.id");
            }
            if let Some(l) = s.limit {
                let _ = write!(sql, " LIMIT {l}");
            }
            sql
        }
        HopOp::Expand { out, types } => {
            let (near, far) = if *out { ("src", "dst") } else { ("dst", "src") };
            format!(
                "SELECT {EDGE_COLS}, {NODE_COLS} FROM {edges} e LEFT JOIN {nodes} n ON n.id = e.{far} \
                 WHERE e.{near} IN ({id_list}){}",
                types_filter("e", types)
            )
        }
        HopOp::Links {
            out,
            mirror,
            types,
            far,
        } => {
            // (alias, far end, its hash, the link columns, the table)
            let (a, far_col, far_rk, cols, tbl) = if *mirror && !*out {
                (
                    "m",
                    "m.src",
                    "m.src_rk",
                    "m.src, m.dst, m.src_rk, m._sprig_rk, m.id, m.type",
                    t(GraphTable::Mirrors),
                )
            } else if *out {
                (
                    "e",
                    "e.dst",
                    "e.dst_rk",
                    "e.src, e.dst, e._sprig_rk, e.dst_rk, e.id, e.type",
                    edges.clone(),
                )
            } else {
                (
                    "e",
                    "e.src",
                    "e._sprig_rk",
                    "e.src, e.dst, e._sprig_rk, e.dst_rk, e.id, e.type",
                    edges.clone(),
                )
            };
            let near = if *out { "src" } else { "dst" };
            // The near nodes: the ids, or the scan's on this shard.
            let id_list = match far.as_ref().and_then(|f| f.near_scan.as_ref()) {
                Some(scan) if *out || *mirror => {
                    let sub = statement(
                        &RouteSpec {
                            group: Arc::clone(&spec.group),
                            graph: spec.graph,
                            op: HopOp::Scan((**scan).clone()),
                            targets: Targets::All(Vec::new()),
                        },
                        i,
                        &[],
                    )?;
                    let from = sub.find(" FROM ")?;
                    format!("SELECT n.id{}", &sub[from..])
                }
                Some(_) => return None,
                None => id_list,
            };
            let plain = format!(
                "SELECT {cols} FROM {tbl} {a} WHERE {a}.{near} IN ({id_list}){}",
                types_filter(a, types)
            );
            let Some(far) = far else {
                return Some(plain);
            };
            let cluster = &g.shards[i].cluster;
            let local: Vec<(usize, &crate::topology::ShardLoc)> = g
                .shards
                .iter()
                .enumerate()
                .filter(|(_, sh)| &sh.cluster == cluster)
                .collect();
            // The range condition of shard `j` on hash column `rk`.
            let range = |j: usize, rk: &str| {
                let mut c = format!("{rk} >= {}", g.shards[j].lo);
                if let Some(next) = g.shards.get(j + 1) {
                    let _ = write!(c, " AND {rk} < {}", next.lo);
                }
                c
            };
            let co = |v: &[String]| {
                if v.len() == 1 {
                    v[0].clone()
                } else {
                    format!("COALESCE({})", v.join(", "))
                }
            };
            // Joined only up to `join_max` links: the same statement's
            // links without the join past it (five `NULL` node columns).
            let split = far
                .join_max
                .filter(|_| far.join && far.top.is_none() && !far.deep && !far.ordered);
            let mut select = cols.to_owned();
            let mut plain_select = format!("{cols}, NULL, NULL, NULL, NULL, NULL");
            let mut joins = String::new();
            let mut plain_joins = String::new();
            let mut conds: Vec<String> = Vec::new();
            // The conditions that don't name the far node (both parts of a
            // split statement).
            let mut plain_conds: Vec<String> = Vec::new();
            // `conds` with far node comparisons that keep only numbers.
            let mut exact_conds: Vec<String> = Vec::new();
            let mut far_cols: Option<(String, String)> = None;
            if far.join {
                // Every shard of this statement's cluster, joined by range.
                let (mut ids, mut labels, mut props, mut tys) =
                    (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                for (j, sh) in &local {
                    let _ = write!(
                        joins,
                        " LEFT JOIN {} n{j} ON n{j}.id = {far_col} AND {}",
                        table(&sh.schema, spec.graph, GraphTable::Nodes),
                        range(*j, far_rk)
                    );
                    ids.push(format!("n{j}.id"));
                    labels.push(format!("n{j}.labels"));
                    props.push(format!("n{j}.props"));
                    tys.push(format!("n{j}._types"));
                }
                let (fid, fprops, ftypes) = (co(&ids), co(&props), co(&tys));
                far_cols = Some((fid.clone(), fprops.clone()));
                let pcols = match &far.keys {
                    None => format!("{fprops}, {ftypes}"),
                    Some(keys) => part_exprs(&fprops, &ftypes, keys),
                };
                let _ = write!(select, ", {fid}, {far_rk}, {}, {pcols}", co(&labels));
                let flabels = co(&labels);
                for (k, op, v) in &far.filter {
                    if let Some(c) = node_filter(&fprops, &flabels, k, *op, v) {
                        // A top-k read must not rank a far node the
                        // comparison drops (it would take another's
                        // place): only numbers compare with a number.
                        let exact = if !k.starts_with(LABEL_KEY)
                            && !matches!(op, CmpOp::NotNumBelow | CmpOp::NotNumAbove)
                        {
                            let path = Lit::Str(format!("$.\"{k}\"")).sql();
                            format!(
                                "JSON_TYPE(JSON_EXTRACT({fprops}, {path})) IN ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE') AND {c}"
                            )
                        } else {
                            c.clone()
                        };
                        conds.push(format!("({fid} IS NULL OR {c})"));
                        exact_conds.push(format!("({fid} IS NULL OR {exact})"));
                    }
                }
            }
            let far_joins_end = joins.len();
            if far.rel_keys.is_some() || !far.rel_filter.is_empty() {
                // The edge row: the statement's own, or (mirrors) joined
                // from the edge's source shard.
                let (eid, eprops, etypes) = if a == "e" {
                    (None, "e.props".to_owned(), "e._types".to_owned())
                } else {
                    let (mut ids, mut props, mut tys) = (Vec::new(), Vec::new(), Vec::new());
                    for (j, sh) in &local {
                        let _ = write!(
                            joins,
                            " LEFT JOIN {} r{j} ON r{j}.id = m.id AND {}",
                            table(&sh.schema, spec.graph, GraphTable::Edges),
                            range(*j, "m.src_rk")
                        );
                        ids.push(format!("r{j}.id"));
                        props.push(format!("r{j}.props"));
                        tys.push(format!("r{j}._types"));
                    }
                    (Some(co(&ids)), co(&props), co(&tys))
                };
                if let Some(keys) = &far.rel_keys {
                    let (p, t) = if keys.is_empty() {
                        (eprops.clone(), etypes.clone())
                    } else {
                        part_pair(&eprops, &etypes, keys)
                    };
                    // A mirror whose edge isn't on this cluster: NULLs.
                    let rel_cols = match &eid {
                        Some(id) => {
                            format!(", IF({id} IS NULL, NULL, {p}), IF({id} IS NULL, NULL, {t})")
                        }
                        None => format!(", {p}, {t}"),
                    };
                    select.push_str(&rel_cols);
                    plain_select.push_str(&rel_cols);
                }
                for (k, op, v) in &far.rel_filter {
                    if let Some(c) = number_filter(&eprops, k, *op, v) {
                        let c = match &eid {
                            Some(id) => format!("({id} IS NULL OR {c})"),
                            None => c,
                        };
                        plain_conds.push(c.clone());
                        conds.push(c);
                    }
                }
                // The edges' joins (mirrors): every far node join comes
                // before them in `joins`.
                joins[far_joins_end..].clone_into(&mut plain_joins);
            }
            if let Some(nx) = far.next.as_ref().filter(|_| far.join && !far.deep)
                && let Some((fid, _)) = &far_cols
            {
                // The far node's id is `fid` (NULL off the cluster); its
                // hash, the link's far hash.
                let mut xs: [Vec<String>; 6] = Default::default();
                for (j, sh) in &local {
                    let _ = write!(
                        joins,
                        " LEFT JOIN {} x{j} ON x{j}.src = {fid} AND {}{}",
                        table(&sh.schema, spec.graph, GraphTable::Edges),
                        range(*j, far_rk),
                        types_filter(&format!("x{j}"), &nx.types)
                    );
                    for (c, name) in
                        xs.iter_mut()
                            .zip(["src", "dst", "_sprig_rk", "dst_rk", "id", "type"])
                    {
                        c.push(format!("x{j}.{name}"));
                    }
                }
                for c in &xs {
                    let _ = write!(select, ", {}", co(c));
                }
                plain_select.push_str(", NULL, NULL, NULL, NULL, NULL, NULL");
                let (xdst, xrk) = (co(&xs[1]), co(&xs[3]));
                let (mut ids, mut labels, mut props, mut tys) =
                    (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                for (j, sh) in &local {
                    let _ = write!(
                        joins,
                        " LEFT JOIN {} y{j} ON y{j}.id = {xdst} AND {}",
                        table(&sh.schema, spec.graph, GraphTable::Nodes),
                        range(*j, &xrk)
                    );
                    ids.push(format!("y{j}.id"));
                    labels.push(format!("y{j}.labels"));
                    props.push(format!("y{j}.props"));
                    tys.push(format!("y{j}._types"));
                }
                let (yid, yprops, ytypes, ylabels) = (co(&ids), co(&props), co(&tys), co(&labels));
                let pcols = match &nx.keys {
                    None => format!("{yprops}, {ytypes}"),
                    Some(keys) => part_exprs(&yprops, &ytypes, keys),
                };
                let _ = write!(select, ", {yid}, {xrk}, {ylabels}, {pcols}");
                plain_select.push_str(", NULL, NULL, NULL, NULL, NULL");
            }
            if far.deep && *out {
                let mut cols: [Vec<String>; 6] = Default::default();
                let mut known: Vec<String> = Vec::new();
                for (j, sh) in &local {
                    let _ = write!(
                        joins,
                        " LEFT JOIN {} x{j} ON x{j}.src = e.dst AND {}{}",
                        table(&sh.schema, spec.graph, GraphTable::Edges),
                        range(*j, "e.dst_rk"),
                        types_filter(&format!("x{j}"), types)
                    );
                    for (c, name) in
                        cols.iter_mut()
                            .zip(["src", "dst", "_sprig_rk", "dst_rk", "id", "type"])
                    {
                        c.push(format!("x{j}.{name}"));
                    }
                    known.push(format!("({})", range(*j, "e.dst_rk")));
                }
                for c in &cols {
                    let _ = write!(select, ", {}", co(c));
                }
                let _ = write!(select, ", ({})", known.join(" OR "));
                // With `join`, the second level's far nodes too (after the
                // known flag), as the first level's are.
                if far.join {
                    let (xdst, xrk) = (co(&cols[1]), co(&cols[3]));
                    let (mut ids, mut labels, mut props, mut tys) =
                        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                    for (j, sh) in &local {
                        let _ = write!(
                            joins,
                            " LEFT JOIN {} y{j} ON y{j}.id = {xdst} AND {}",
                            table(&sh.schema, spec.graph, GraphTable::Nodes),
                            range(*j, &xrk)
                        );
                        ids.push(format!("y{j}.id"));
                        labels.push(format!("y{j}.labels"));
                        props.push(format!("y{j}.props"));
                        tys.push(format!("y{j}._types"));
                    }
                    let (yprops, ytypes) = (co(&props), co(&tys));
                    let pcols = match &far.keys {
                        None => format!("{yprops}, {ytypes}"),
                        Some(keys) => part_exprs(&yprops, &ytypes, keys),
                    };
                    let _ = write!(select, ", {}, {xrk}, {}, {pcols}", co(&ids), co(&labels));
                }
            }
            if !far.only.is_empty() {
                conds.push(format!("{far_col} IN ({})", ids_list(&far.only)));
                plain_conds.push(format!("{far_col} IN ({})", ids_list(&far.only)));
            }
            if let Some(x) = far.after {
                conds.push(format!("{a}.id > {x}"));
                plain_conds.push(format!("{a}.id > {x}"));
            }
            let head = format!(
                "SELECT {select} FROM {tbl} {a}{joins} WHERE {a}.{near} IN ({id_list}){}",
                types_filter(a, types)
            );
            let with = |cs: &[String]| {
                let mut q = head.clone();
                for c in cs {
                    let _ = write!(q, " AND {c}");
                }
                q
            };
            // Shared conditions (`only`, relationships') are in both lists.
            let shared = &conds[exact_conds.len().min(conds.len())..];
            let mut exact = exact_conds.clone();
            exact.extend(shared.iter().cloned());
            if far.join
                && let (Some((k, asc, n)), Some((fid, fprops))) = (&far.top, &far_cols)
                && let Some((top, rest, order)) = top_parts(fid, fprops, k, *asc, *n)
            {
                return Some(format!(
                    "({} AND {top}{order}) UNION ALL ({} AND {rest})",
                    with(&exact),
                    with(&conds)
                ));
            }
            if let Some(n) = split {
                let types = types_filter(a, types);
                // Whether the near nodes have a link past the `n`th: NULL
                // when they don't (no derived table, and it stops there).
                let past = format!(
                    "(SELECT 1 FROM {tbl} {a} WHERE {a}.{near} IN ({id_list}){types} LIMIT {n}, 1)"
                );
                let mut joined = with(&conds);
                let _ = write!(joined, " AND {past} IS NULL");
                if far.next.is_none() {
                    let _ = write!(joined, " LIMIT {n}");
                }
                let mut plain = format!(
                    "SELECT {plain_select} FROM {tbl} {a}{plain_joins} WHERE {a}.{near} IN ({id_list}){types}"
                );
                for c in &plain_conds {
                    let _ = write!(plain, " AND {c}");
                }
                let _ = write!(plain, " AND {past} IS NOT NULL");
                if let Some(l) = far.limit {
                    let _ = write!(plain, " LIMIT {l}");
                }
                return Some(format!("({joined}) UNION ALL ({plain})"));
            }
            let mut q = with(&conds);
            if far.top.is_none()
                && let Some(n) = far.limit
            {
                // In id order when a reader may continue after the last
                // (the sort costs MariaDB every joined row's JSON first).
                if far.ordered {
                    let _ = write!(q, " ORDER BY {a}.id");
                }
                let _ = write!(q, " LIMIT {n}");
            }
            q
        }
        HopOp::NodesPart { keys, filter } => {
            let mut sql = format!(
                "SELECT n.id, n._sprig_rk, n.labels, {} FROM {nodes} n WHERE n.id IN ({id_list})",
                part_cols("n", keys)
            );
            // Numbers compare as doubles (a JSON number's text against a
            // number): exact for the integers callers push (|x| < 2^52);
            // a non-number gives a superset or the same rows as Cypher's
            // null.
            for (k, op, v) in filter {
                if let Some(c) = node_filter("n.props", "n.labels", k, *op, v) {
                    let _ = write!(sql, " AND {c}");
                }
            }
            sql
        }
        HopOp::EdgesPart { keys } => format!(
            "SELECT e.src, e.type, e.dst, e.id, e.dst_rk, e._sprig_rk, {} FROM {edges} e \
             WHERE e.id IN ({id_list})",
            part_cols("e", keys)
        ),
        HopOp::Mirrors { types } => format!(
            "SELECT m.id, m.src_rk FROM {} m WHERE m.dst IN ({id_list}){}",
            t(GraphTable::Mirrors),
            types_filter("m", types)
        ),
        HopOp::EdgesWithSrc => format!(
            "SELECT {EDGE_COLS}, {NODE_COLS} FROM {edges} e LEFT JOIN {nodes} n ON n.id = e.src \
             WHERE e.id IN ({id_list})"
        ),
        HopOp::Edges => format!("SELECT {EDGE_COLS} FROM {edges} e WHERE e.id IN ({id_list})"),
        HopOp::Touching { out, limit } => format!(
            "SELECT {EDGE_COLS} FROM {edges} e WHERE e.{} IN ({id_list}) LIMIT {limit}",
            if *out { "src" } else { "dst" }
        ),
        HopOp::Reach {
            seeds,
            types,
            out,
            hops,
        } => {
            if seeds.is_empty() || *hops == 0 {
                return None;
            }
            let many = g.shards.len() > 1;
            let (tbl, near, far, far_rk) = match (out, many) {
                (true, _) => (edges.clone(), "src", "dst", "dst_rk"),
                (false, true) => (t(GraphTable::Mirrors), "dst", "src", "src_rk"),
                (false, false) => (edges.clone(), "dst", "src", "_sprig_rk"),
            };
            let mut seed_sql = String::new();
            for (k, (o, n)) in seeds.iter().enumerate() {
                if k > 0 {
                    seed_sql.push_str(" UNION ALL ");
                }
                let _ = write!(seed_sql, "SELECT {o} AS o, {n} AS n");
            }
            let tf = types_filter("e", types);
            // Only nodes inside this shard's range continue: the others
            // are boundary nodes for the gateway's frontier.
            let mut range = String::new();
            if many {
                let _ = write!(range, " AND r.rk >= {}", g.shards[i].lo);
                if let Some(next) = g.shards.get(i + 1) {
                    let _ = write!(range, " AND r.rk < {}", next.lo);
                }
            }
            format!(
                "SET STATEMENT max_recursive_iterations = {iters} FOR \
                 WITH RECURSIVE r (o, id, rk, d) AS ( \
                   SELECT s.o, e.{far}, e.{far_rk}, 1 FROM ({seed_sql}) s \
                     JOIN {tbl} e ON e.{near} = s.n{tf} \
                   UNION \
                   SELECT r.o, e.{far}, e.{far_rk}, r.d + 1 FROM r \
                     JOIN {tbl} e ON e.{near} = r.id{tf} \
                    WHERE r.d < {hops}{range} \
                 ) CYCLE o, id RESTRICT \
                 SELECT o, id, rk, MIN(d) FROM r GROUP BY o, id, rk",
                iters = hops + 1,
            )
        }
    })
}

/// Plans `spec`: the router's targets for the step, narrowed to the shards
/// that can hold its rows.
#[must_use]
pub fn plan_route(spec: &RouteSpec) -> Route {
    let g = &spec.group;
    // Per shard index, the ids its statement names.
    let mut per: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    match &spec.targets {
        Targets::ByKey(ids) => {
            for &id in ids {
                // A graph's key is an integer: every id hashes.
                let i = node_hash(g, id).map_or(0, |h| g.route(h));
                per.entry(i).or_default().push(id);
            }
        }
        Targets::ByHash(v) => {
            for &(h, id) in v {
                per.entry(g.route(h)).or_default().push(id);
            }
        }
        Targets::All(ids) => {
            for i in 0..g.shards.len() {
                per.insert(i, ids.clone());
            }
        }
        Targets::Shard(i) => {
            if *i < g.shards.len() {
                per.insert(*i, Vec::new());
            }
        }
    }
    // Edges by id after the mirrors come in batches (the mirrors of a
    // whole frontier); every other step takes the executor's batch as is.
    let chunked = matches!(spec.op, HopOp::EdgesWithSrc);
    let mut targets: Vec<Target> = Vec::new();
    let mut held: Option<u64> = None;
    for (i, ids) in per {
        let Some(loc) = g.shards.get(i) else { continue };
        let chunks: Vec<&[u64]> = if chunked && ids.len() > IDS_PER_STATEMENT {
            ids.chunks(IDS_PER_STATEMENT).collect()
        } else {
            vec![ids.as_slice()]
        };
        let mut any = false;
        for c in chunks {
            let Some(sql) = statement(spec, i, c) else {
                continue;
            };
            any = true;
            targets.push(Target {
                cluster: loc.cluster.clone(),
                shard: loc.shard,
                sql,
                at: g.at(i),
            });
        }
        if any && let Some(u) = loc.held_until_ms {
            held = Some(held.map_or(u, |h| h.max(u)));
        }
    }
    let plan = if targets.len() == 1 {
        Plan::Pass(targets.remove(0))
    } else {
        Plan::Read(ReadPlan {
            kind: ReadKind::Scatter,
            targets,
            hidden: 0,
            order: Vec::new(),
            agg: None,
            offset: 0,
            limit: None,
            pushed_limit: None,
            group: g.id,
        })
    };
    let op = spec.op.name();
    metrics::counter!("sprig_router_graph_routes_total", "op" => op, "plan" => plan.kind())
        .increment(1);
    Route {
        op,
        plan,
        held_until_ms: held,
    }
}

/// EXPLAIN's lines for a graph's routes: its group and shards as the
/// router's map cache has them, and how each kind of step picks its
/// shards.
#[must_use]
pub fn explain_lines(group: &Group, async_mirrors: bool) -> Vec<String> {
    let mut out = vec![format!(
        "GraphRoutes: group {} (map version {}), {} shard(s), each step planned by the router",
        group.id,
        group.version,
        group.shards.len()
    )];
    for s in &group.shards {
        out.push(format!(
            "  GraphShard: s{} {} {} lo {} epoch {}{}",
            s.shard,
            s.cluster,
            s.schema,
            s.lo,
            s.epoch,
            if s.held_until_ms.is_some() {
                " (held)"
            } else {
                ""
            }
        ));
    }
    if group.shards.len() > 1 {
        out.push("  Route scan: every shard (scatter); by id: the ids' shards (by key)".into());
        out.push("  Route expand_out: the frontier's shards (by key), far node joined".into());
        out.push(format!(
            "  Route expand_in: mirrors on the frontier's shards (by key), then edges on their sources' shards (by hash){}; without them every shard (scatter)",
            if async_mirrors {
                " when the mirrors are fresh"
            } else {
                ""
            }
        ));
        out.push("  Route reach: a recursive CTE on each frontier shard (pass)".into());
    } else {
        out.push("  Route: every step on the one shard (pass)".into());
    }
    out
}

#[cfg(test)]
mod tests;
