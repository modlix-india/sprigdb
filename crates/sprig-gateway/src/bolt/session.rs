//! A logged-in Bolt session: statements, transactions and bookmarks.
//!
//! - **Statements.** A `RUN` is parsed, its graph resolved (the default
//!   graph made on its first write), its privileges checked, and run on
//!   the graph's storage in the node's engine ([`engine`]): one engine
//!   transaction per auto-commit `RUN`, or the explicit transaction's.
//!   A mapped graph reads its tables in process. Reads whose rows don't
//!   depend on each other stream ([`super::stream`]); the rest are computed
//!   whole, within `bolt.max_buffered_result_mb` (39918).
//! - **Bookmarks.** A commit is durable before it is answered, so any later
//!   read on this node sees it: the session answers a signed bookmark
//!   ([`sprig_graph::bookmark`]), and bookmarks a driver passes (`BEGIN` or
//!   `RUN`'s `bookmarks`) are checked (39917 for a bad or foreign one).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_auth::authz::Action;
use sprig_auth::mysql::Principal;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::access::{FilteredStore, Visible, Writable};
use sprig_graph::ast::{Clause, Query};
use sprig_graph::bookmark::{self, Bookmark};
use sprig_graph::eval::Params;
use sprig_graph::store::GraphStore;
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::server::{BoltFailure, BoltSession, QueryResult};
use tokio::sync::mpsc;

use super::stream::{CHANNEL_ROWS, ChanSink, Msg, Start, summary};
use super::{
    DEFAULT_GRAPH, LOGICAL_DATABASE, MAX_BUFFERED_RESULT_MB, failure, from_pack, refuse, to_pack,
};
use crate::graphs::{GatewayGraphs, GraphRef};

mod engine;
use engine::{EngineRun, EngineTx};

/// An open transaction.
struct Tx {
    db: Option<String>,
    timeout: Option<Duration>,
    /// `BEGIN`'s `tx_metadata`, for `CALL tx.getMetaData()`.
    metadata: Option<BTreeMap<String, sprig_graph::value::Value>>,
    /// Its engine transaction, once a statement used a graph stored in the
    /// storage engine.
    engine: Option<EngineTx>,
}

/// The session's last bookmark.
#[derive(Debug, Default)]
struct Causal {
    last: Option<String>,
}

pub(crate) struct GwSession {
    principal: Principal,
    /// The gateway session table's entry (`SHOW PROCESSLIST`, `KILL`).
    reg: Option<crate::session::Session>,
    graphs: Arc<GatewayGraphs>,
    dc: DcId,
    tx: Option<Tx>,
    causal: Arc<Mutex<Causal>>,
    advertise: String,
    ttl: u64,
}

/// A statement ready to run.
struct Prepared {
    q: Query,
    p: Params,
    g: GraphRef,
    cat: Arc<TenantCatalog>,
    budgets: sprig_graph::Budgets,
    /// Per-label and per-type grants, when the graph-wide ones don't give
    /// `SELECT` or a write action.
    visible: Option<Access>,
}

/// What per-label and per-type grants allow a statement: reads filtered
/// by `vis` (all when `None`), writes checked against `writes` (all when
/// `None`).
#[derive(Debug, Clone, Default)]
struct Access {
    vis: Option<Visible>,
    writes: Option<Writable>,
}

/// Runs `q` on `store`, filtered by `vis` when given.
#[allow(clippy::many_single_char_names)] // the executor's names
async fn run_on(
    store: &mut dyn GraphStore,
    vis: Option<Access>,
    q: &Query,
    p: &Params,
    b: &sprig_graph::Budgets,
) -> Result<sprig_graph::Output> {
    // Lazy: a read's row error comes after the rows before it.
    match vis {
        None => sprig_graph::exec::run_lazy(store, q, p, b).await,
        Some(v) => {
            let mut f = FilteredStore::guarded(store, v.vis, v.writes).await?;
            sprig_graph::exec::run_lazy(&mut f, q, p, b).await
        }
    }
}

/// [`run_on`] for a streamed read.
#[allow(clippy::many_single_char_names)] // the executor's names
async fn stream_on(
    store: &mut dyn GraphStore,
    vis: Option<Access>,
    q: &Query,
    p: &Params,
    b: &sprig_graph::Budgets,
    sink: &mut dyn sprig_graph::RowSink,
) -> Result<sprig_graph::Output> {
    match vis {
        None => sprig_graph::run_stream(store, q, p, b, sink).await,
        Some(v) => {
            let mut f = FilteredStore::guarded(store, v.vis, v.writes).await?;
            sprig_graph::run_stream(&mut f, q, p, b, sink).await
        }
    }
}

enum Prep {
    /// Answered without storage (the default graph before its first
    /// write).
    Done(QueryResult),
    Run(Box<Prepared>),
}

/// The privileges a statement needs.
fn actions(q: &Query) -> Vec<Action> {
    let mut a = vec![];
    let mut add = |x: Action| {
        if !a.contains(&x) {
            a.push(x);
        }
    };
    for part in &q.parts {
        for c in &part.clauses {
            match c {
                Clause::Match { .. }
                | Clause::Return(_)
                | Clause::With(_)
                | Clause::Unwind { .. }
                | Clause::Call { .. } => add(Action::Select),
                Clause::Create(_) | Clause::Merge { .. } => {
                    add(Action::Insert);
                    add(Action::Select);
                }
                Clause::Set(_) | Clause::Remove(_) => add(Action::Update),
                Clause::Delete { .. } => add(Action::Delete),
                Clause::Schema(_) => add(Action::Index),
                Clause::Subquery { query, .. } => {
                    for x in actions(query) {
                        add(x);
                    }
                }
                Clause::Foreach { body, .. } => {
                    let inner = Query {
                        explain: sprig_graph::ast::Explain::None,
                        parts: vec![sprig_graph::ast::SingleQuery {
                            clauses: body.clone(),
                        }],
                        union_all: vec![],
                    };
                    for x in actions(&inner) {
                        add(x);
                    }
                }
            }
        }
    }
    a
}

fn is_default(db: Option<&str>) -> bool {
    db.is_none_or(str::is_empty)
}

fn has_writes(q: &Query) -> bool {
    actions(q).iter().any(|a| *a != Action::Select)
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(0))
}

fn timeout_of(extra: &PackValue) -> Option<Duration> {
    extra
        .get("tx_timeout")
        .and_then(PackValue::as_int)
        .filter(|t| *t > 0)
        .map(|t| Duration::from_millis(u64::try_from(t).unwrap_or(0)))
}

/// `tx_metadata` of `BEGIN` or an auto-commit `RUN`: a map, else none
/// (values that aren't Cypher values leave it out; it is still audited).
fn metadata_of(extra: &PackValue) -> Option<BTreeMap<String, sprig_graph::value::Value>> {
    match extra.get("tx_metadata").map(from_pack) {
        Some(Ok(sprig_graph::value::Value::Map(m))) => Some(m),
        _ => None,
    }
}

/// A lock wait that ended because the transaction's `tx_timeout` passed
/// (the branch's `innodb_lock_wait_timeout` is set from it): Neo4j's
/// `LockClientStopped`, a client error, as Neo4j answers it, rather than
/// the transient 1205 drivers would retry.
fn timed_out_failure(e: &SprigError, timeout: Option<Duration>) -> BoltFailure {
    match (&e.kind, timeout) {
        (
            sprig_common::ErrorKind::Backend { errno: 1205, .. }
            | sprig_common::ErrorKind::Standard { errno: 1205, .. },
            Some(t),
        ) => BoltFailure::new(
            "Neo.ClientError.Transaction.LockClientStopped",
            format!(
                "The transaction has been terminated: it waited for a lock past its timeout of {} ms (tx_timeout)",
                t.as_millis()
            ),
        ),
        // The time budget that fired was the tx_timeout (it caps
        // graph.max_time_ms): Neo4j's transaction timeout.
        (sprig_common::ErrorKind::Sprig(SprigCode::GraphBudgetExceeded), Some(t))
            if e.message
                .ends_with(&format!("(time): graph.max_time_ms = {}", t.as_millis())) =>
        {
            BoltFailure::new(
                "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration",
                format!(
                    "The transaction has been terminated: it ran past its timeout of {} ms (tx_timeout)",
                    t.as_millis()
                ),
            )
        }
        _ => failure(e),
    }
}

/// `EXPLAIN`'s or `PROFILE`'s steps as Neo4j's plan map: a chain of
/// operators (`operatorType`, `args` with the step's text in `Details`,
/// `identifiers`, `children`) under `ProduceResults`, the last step on top.
/// A profile adds `rows` (the result's, on `ProduceResults`) and `dbHits`
/// (0: SprigDB doesn't count store reads per operator).
fn plan_pack(steps: &[String], columns: &[String], rows: Option<usize>) -> PackValue {
    let op = |kind: &str, details: &str, idents: &[String], child: Option<PackValue>, n: usize| {
        let mut m = vec![
            ("operatorType", PackValue::str(kind)),
            (
                "args",
                PackValue::map(vec![
                    ("Details", PackValue::str(details)),
                    ("planner", PackValue::str("SPRIG")),
                    ("runtime", PackValue::str("SPRIG")),
                ]),
            ),
            (
                "identifiers",
                PackValue::List(idents.iter().cloned().map(PackValue::String).collect()),
            ),
            ("children", PackValue::List(child.into_iter().collect())),
        ];
        if rows.is_some() {
            m.push(("rows", PackValue::Int(i64::try_from(n).unwrap_or(i64::MAX))));
            m.push(("dbHits", PackValue::Int(0)));
        }
        PackValue::map(m)
    };
    let mut child = None;
    for s in steps {
        let kind = s.split([':', ' ']).next().unwrap_or("Step");
        child = Some(op(kind, s, &[], child, 0));
    }
    op(
        "ProduceResults",
        &columns.join(", "),
        columns,
        child,
        rows.unwrap_or(0),
    )
}

/// A notification as Bolt's summary map.
fn notification_pack(n: &sprig_graph::exec::Notification) -> PackValue {
    PackValue::map(vec![
        ("code", PackValue::str(n.code)),
        ("title", PackValue::str(n.title)),
        ("description", PackValue::String(n.description.clone())),
        ("severity", PackValue::str(n.severity)),
        ("category", PackValue::str(n.category)),
    ])
}

/// A notification as a GQL status object (Bolt 5.5 on), as Neo4j 5.26
/// sends it.
fn status_pack(n: &sprig_graph::exec::Notification) -> PackValue {
    PackValue::map(vec![
        ("gql_status", PackValue::str(n.gql_status)),
        ("neo4j_code", PackValue::str(n.code)),
        (
            "status_description",
            PackValue::String(n.status_description.clone()),
        ),
        (
            "diagnostic_record",
            PackValue::map(vec![
                ("_classification", PackValue::str(n.category)),
                ("_severity", PackValue::str(n.severity)),
                (
                    "_position",
                    PackValue::map(vec![
                        ("offset", PackValue::Int(0)),
                        ("line", PackValue::Int(1)),
                        ("column", PackValue::Int(1)),
                    ]),
                ),
            ]),
        ),
        ("description", PackValue::String(n.description.clone())),
        ("title", PackValue::str(n.title)),
    ])
}

fn lost_conn() -> SprigError {
    SprigError::new(
        SprigCode::BoltProtocol,
        "the transaction's connection was lost by a failed statement; roll back",
    )
}

impl GwSession {
    pub(crate) fn new(
        principal: Principal,
        graphs: Arc<GatewayGraphs>,
        dc: DcId,
        advertise: String,
        ttl: u64,
    ) -> Self {
        Self {
            principal,
            reg: None,
            graphs,
            dc,
            tx: None,
            causal: Arc::new(Mutex::new(Causal::default())),
            advertise,
            ttl,
        }
    }

    /// The logged-in principal.
    pub(super) fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Joins the gateway's session table.
    pub(crate) fn attach(&mut self, s: crate::session::Session) {
        self.reg = Some(s);
    }

    fn tenant(&self) -> TenantId {
        self.principal.tenant.unwrap_or(TenantId(0))
    }

    fn causal(&self) -> std::sync::MutexGuard<'_, Causal> {
        self.causal.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `(logical db, graph)` of a Bolt database name.
    fn split_db(&self, db: Option<&str>, cat: &TenantCatalog) -> (String, String) {
        let ctx = cat.setting_ctx(&self.dc);
        let s = &self.graphs.svc.settings;
        let logical = s
            .get(&LOGICAL_DATABASE, &ctx, Some(&cat.settings))
            .into_owned();
        match db.filter(|d| !d.is_empty()) {
            Some(d) => match d.split_once('.') {
                Some((a, b)) => (a.to_owned(), b.to_owned()),
                None => (logical, d.to_owned()),
            },
            None => (
                logical,
                s.get(&DEFAULT_GRAPH, &ctx, Some(&cat.settings))
                    .into_owned(),
            ),
        }
    }

    /// Checks the bookmarks in `extra`.
    ///
    /// # Errors
    /// 39917 for a bad or foreign bookmark.
    async fn take_bookmarks(&self, extra: &PackValue) -> Result<()> {
        let list: Vec<&str> = extra
            .get("bookmarks")
            .and_then(PackValue::as_list)
            .unwrap_or_default()
            .iter()
            .filter_map(PackValue::as_str)
            .collect();
        if list.is_empty() {
            return Ok(());
        }
        let tenant = self.tenant();
        let key = self.graphs.bookmark_key(tenant).await?;
        for b in list {
            bookmark::decode(b, tenant.0, &key)?;
        }
        Ok(())
    }

    /// After a commit: the session's new bookmark.
    async fn after_commit(&self) -> Result<String> {
        let key = self.graphs.bookmark_key(self.tenant()).await?;
        let mut c = self.causal();
        let b = Bookmark {
            tenant: self.tenant().0,
            hlc: now_us(),
            gtids: BTreeMap::new(),
        };
        let text = bookmark::encode(&b, &key);
        c.last = Some(text.clone());
        Ok(text)
    }

    #[allow(clippy::many_single_char_names, clippy::too_many_lines)] // q, p, g; parse, resolve, authorize, budgets
    async fn prepare(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> Result<Prep> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(SprigError::new(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let q = sprig_graph::parser::parse_cached(query)?;
        let mut p = Params::new();
        for (k, v) in params.as_map().unwrap_or_default() {
            p.insert(k.clone(), from_pack(v)?);
        }
        if self.tx.is_none() {
            self.take_bookmarks(extra).await?;
        }
        let tenant = self.tenant();
        let cat = self.graphs.catalog(tenant, Freshness::Latest).await?;
        let db_req: Option<String> = match &self.tx {
            Some(t) => t.db.clone(),
            None => extra
                .get("db")
                .and_then(PackValue::as_str)
                .map(str::to_owned),
        };
        let (db, name) = self.split_db(db_req.as_deref(), &cat);
        let grants = self.graphs.grants(&cat, self.principal.user_id);
        let mut graph = self.graphs.resolve(&cat, &db, &name)?;
        let mut cat = cat;
        if graph.is_none() {
            if !(is_default(db_req.as_deref()) && has_writes(&q)) {
                if is_default(db_req.as_deref()) {
                    // The default graph before its first write: empty.
                    let mut mem = sprig_graph::mem::MemStore::new(1);
                    let out =
                        sprig_graph::exec::run(&mut mem, &q, &p, &sprig_graph::Budgets::default())
                            .await?;
                    return Ok(Prep::Done(self.result(out, &name, &cat)?));
                }
                return Err(SprigError::new(
                    SprigCode::GraphNotFound,
                    format!("Graph not found: {db}.{name}"),
                ));
            }
            let lctn = self.graphs.lctn(&cat);
            let db_row = cat
                .database(&sprig_catalog::ddl::database_key(&db, lctn))
                .map(|r| r.db_id)
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphNotFound,
                        format!("Database '{db}' does not exist"),
                    )
                })?;
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &db, db_row, None, Action::Create)
            {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "CREATE command denied to user '{}' for graph '{db}.{name}'",
                        self.principal.user
                    ),
                ));
            }
            let (g, v) = self.graphs.create(tenant, &db, &name, true).await?;
            cat = self
                .graphs
                .catalog(tenant, Freshness::Cached { min_version: v })
                .await?;
            graph = Some(g);
        }
        let Some(g) = graph else {
            return Err(SprigError::internal("graph missing"));
        };
        let mut visible: Option<Access> = None;
        for a in actions(&q) {
            if !self
                .graphs
                .allows(grants.as_deref(), &cat, &g.db, g.db_id, Some(g.graph_id), a)
            {
                // Reads may rest on grants of single labels and types.
                if a == Action::Select
                    && g.def.kind != "mapped"
                    && let Some(v) = grants
                        .as_deref()
                        .and_then(|gr| GatewayGraphs::visible(gr, g.graph_id))
                {
                    visible.get_or_insert_with(Access::default).vis = Some(v);
                    continue;
                }
                // Writes may rest on them too: each write is
                // then checked against the labels and types it touches.
                if matches!(a, Action::Insert | Action::Update | Action::Delete)
                    && g.def.kind != "mapped"
                    && let Some(gr) = grants.as_deref()
                    && let Some(w) = GatewayGraphs::writable(gr, g.graph_id, a)
                {
                    // The other write actions as held graph-wide (a label
                    // change is a CREATE or DELETE on that label, whatever
                    // clause makes it).
                    let wide = |x: Action| {
                        self.graphs.allows(
                            grants.as_deref(),
                            &cat,
                            &g.db,
                            g.db_id,
                            Some(g.graph_id),
                            x,
                        )
                    };
                    let (ci, cd, cu) = (
                        wide(Action::Insert),
                        wide(Action::Delete),
                        wide(Action::Update),
                    );
                    let acc = visible.get_or_insert_with(Access::default);
                    let have = acc.writes.get_or_insert_with(|| {
                        let mut wr = Writable {
                            create_all: ci,
                            delete_all: cd,
                            set_all: cu,
                            ..Writable::default()
                        };
                        for x in [Action::Insert, Action::Delete, Action::Update] {
                            if let Some((l, t)) = GatewayGraphs::writable(gr, g.graph_id, x) {
                                match x {
                                    Action::Insert => {
                                        wr.create_labels = l;
                                        wr.create_types = t;
                                    }
                                    Action::Delete => {
                                        wr.delete_labels = l;
                                        wr.delete_types = t;
                                    }
                                    _ => {
                                        wr.set_labels = l;
                                        wr.set_types = t;
                                    }
                                }
                            }
                        }
                        wr
                    });
                    match a {
                        Action::Insert => {
                            have.create_all = false;
                            have.create_labels = w.0;
                            have.create_types = w.1;
                        }
                        Action::Delete => {
                            have.delete_all = false;
                            have.delete_labels = w.0;
                            have.delete_types = w.1;
                        }
                        _ => {
                            have.set_all = false;
                            have.set_labels = w.0;
                            have.set_types = w.1;
                        }
                    }
                    continue;
                }
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "{} command denied to user '{}' for graph '{}.{}'",
                        format!("{a:?}").to_uppercase(),
                        self.principal.user,
                        g.db,
                        g.name
                    ),
                ));
            }
        }
        if g.def.kind == "mapped" {
            if has_writes(&q) {
                return Err(SprigError::new(
                    SprigCode::GraphReadOnlySource,
                    format!(
                        "graph '{}.{}' is mapped over tables: write them in SQL",
                        g.db, g.name
                    ),
                ));
            }
            // An invoker's check on every source table.
            let def = mapped_def(&g)?;
            for (t, o) in self.graphs.mapped_sources(&cat, g.db_id, &def)? {
                if !self.graphs.allows(
                    grants.as_deref(),
                    &cat,
                    &g.db,
                    g.db_id,
                    Some(o),
                    Action::Select,
                ) {
                    return Err(SprigError::standard(
                        1142,
                        *b"42000",
                        format!(
                            "SELECT command denied to user '{}' for table '{}.{t}' (a source of graph '{}')",
                            self.principal.user, g.db, g.name
                        ),
                    ));
                }
            }
        }
        let ctx = cat.setting_ctx(&self.dc).with_db(g.db_id);
        let settings = &self.graphs.svc.settings;
        let mut budgets =
            sprig_graph::settings::budgets(&|s| settings.get(s, &ctx, Some(&cat.settings)));
        crate::offload::GraphLimits::read(&|s| settings.get(s, &ctx, Some(&cat.settings))).apply();
        budgets.tier = sprig_graph::exec::TraversalTier::parse(&settings.get(
            &sprig_graph::settings::TRAVERSAL_TIER,
            &ctx,
            Some(&cat.settings),
        ));
        let timeout = match &self.tx {
            Some(t) => t.timeout,
            None => timeout_of(extra),
        };
        if let Some(t) = timeout {
            budgets.max_time = budgets.max_time.min(t);
        }
        budgets.tx_metadata = match &self.tx {
            Some(t) => t.metadata.clone(),
            None => metadata_of(extra),
        };
        Ok(Prep::Run(Box::new(Prepared {
            q,
            p,
            g,
            cat,
            budgets,
            visible,
        })))
    }

    #[allow(clippy::too_many_lines)] // auto-commit and transaction, buffered and streamed
    async fn execute(&mut self, pr: Prepared) -> Result<StartOrResult> {
        let Prepared {
            q,
            p,
            g,
            cat,
            budgets,
            visible,
        } = pr;
        let writes = has_writes(&q);
        if g.def.kind == "mapped" {
            // Read-only SQL over the user's tables, read in process (also
            // inside an explicit Bolt transaction).
            let def = mapped_def(&g)?;
            let mut run = crate::engine::graphsql::EngineSql::new(
                g.tenant,
                &cat,
                self.graphs.lctn(&cat),
                false,
            );
            let mut store =
                sprig_graph::mapped::MappedStore::new(&mut run, g.graph_id, &g.schema, def);
            let r = sprig_graph::exec::run(&mut store, &q, &p, &budgets).await;
            return Ok(StartOrResult::Result(self.result(r?, &g.name, &cat)?));
        }
        let streams = sprig_graph::streamable(&q).is_some();
        self.execute_engine(EngineRun {
            q,
            p,
            g,
            cat,
            budgets,
            visible,
            writes,
            streams,
        })
        .await
    }

    fn result(
        &self,
        out: sprig_graph::Output,
        graph: &str,
        cat: &TenantCatalog,
    ) -> Result<QueryResult> {
        let limit = usize::try_from(self.graphs.svc.settings.get(
            &MAX_BUFFERED_RESULT_MB,
            &cat.setting_ctx(&self.dc),
            Some(&cat.settings),
        ))
        .unwrap_or(64)
        .saturating_mul(1 << 20);
        let mut bytes = 0usize;
        let mut records = VecDeque::with_capacity(out.rows.len());
        for r in out.rows {
            let rec: Vec<PackValue> = r.iter().map(to_pack).collect();
            bytes += rec.iter().map(PackValue::encoded_len).sum::<usize>();
            if bytes > limit {
                return Err(SprigError::new(
                    SprigCode::BoltResultBufferLimit,
                    format!(
                        "the result is larger than bolt.max_buffered_result_mb = {}",
                        limit >> 20
                    ),
                ));
            }
            records.push_back(rec);
        }
        let mut summary = Vec::new();
        if let Some(steps) = &out.plan {
            let key = if out.profiled { "profile" } else { "plan" };
            summary.push((
                key,
                plan_pack(steps, &out.columns, out.profiled.then_some(records.len())),
            ));
        }
        if !out.notifications.is_empty() {
            summary.push((
                "notifications",
                PackValue::List(out.notifications.iter().map(notification_pack).collect()),
            ));
            // Bolt 5.5 on: the same as GQL status objects (the protocol
            // layer sends one or the other, by version).
            summary.push((
                "statuses",
                PackValue::List(out.notifications.iter().map(status_pack).collect()),
            ));
        }
        let stream = out.late_error.map(|e| {
            Box::new(super::stream::LateError(Some(failure(&e))))
                as Box<dyn sprig_proto_bolt::server::RecordStream>
        });
        Ok(QueryResult {
            fields: out.columns,
            records,
            qtype: out.qtype,
            stats: out.stats.into_iter().collect(),
            db: Some(graph.to_owned()),
            summary,
            stream,
            ..QueryResult::default()
        })
    }

    /// Ends the open transaction, whether it is here or still with a
    /// streamed result: everything rolls back.
    fn abandon(&mut self) {
        let Some(t) = self.tx.take() else { return };
        if let Some(et) = t.engine {
            tokio::spawn(et.abandon());
        }
    }
}

/// A mapped graph's definition.
fn mapped_def(g: &GraphRef) -> Result<sprig_graph::mapped::MappedDef> {
    serde_json::from_str(g.def.definition.as_deref().unwrap_or("{}"))
        .map_err(|e| SprigError::internal(format!("mapped graph definition: {e}")))
}

/// What `execute` produced.
enum StartOrResult {
    Result(QueryResult),
    Stream(mpsc::Receiver<Msg>, String),
}

impl GwSession {
    /// A `RUN`: prepare and execute.
    async fn run_inner(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<QueryResult, BoltFailure> {
        let started = Instant::now();
        let timeout = self
            .tx
            .as_ref()
            .map_or_else(|| timeout_of(extra), |t| t.timeout);
        let pr = match self.prepare(query, params, extra).await {
            Ok(Prep::Done(r)) => return Ok(r),
            Ok(Prep::Run(p)) => *p,
            Err(e) => return Err(failure(&e)),
        };
        let (start, graph) = match self.execute(pr).await {
            Ok(StartOrResult::Result(r)) => return Ok(r),
            Ok(StartOrResult::Stream(rx, g)) => (super::stream::start(rx).await?, g),
            Err(e) => return Err(timed_out_failure(&e, timeout)),
        };
        let t_first = i64::try_from(started.elapsed().as_millis()).unwrap_or(0);
        Ok(match start {
            Start::Rows(fields, first, stream) => QueryResult {
                fields,
                records: VecDeque::from([first]),
                qtype: "r",
                db: Some(graph),
                t_first,
                stream: Some(Box::new(stream)),
                ..QueryResult::default()
            },
            Start::Done(fields, end) => QueryResult {
                fields,
                qtype: "r",
                stats: end.stats,
                db: Some(graph),
                t_first,
                ..QueryResult::default()
            },
        })
    }
}

#[async_trait]
impl BoltSession for GwSession {
    async fn run(
        &mut self,
        query: &str,
        params: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<QueryResult, BoltFailure> {
        let handle = self.reg.as_ref().map(|s| {
            s.begin_command("Query", Some(query.chars().take(1024).collect()));
            s.set_db(
                extra
                    .get("db")
                    .and_then(PackValue::as_str)
                    .map(str::to_owned),
            );
            Arc::clone(s.handle())
        });
        let r = match handle {
            // On a blocking thread from its first poll (not guarded like a
            // MySQL statement, which moves only after 100 ms and while
            // slots last).
            Some(h) => {
                Box::pin(crate::offload::run_graph(async {
                    tokio::select! {
                        r = self.run_inner(query, params, extra) => r,
                        () = h.flags().query_killed() => {
                            h.flags().take_kill_query();
                            // Whatever the statement held is dropped: its
                            // connections close and roll back.
                            self.abandon();
                            Err(BoltFailure::new(
                                "Neo.TransientError.Transaction.Terminated",
                                "The transaction has been terminated (KILL)",
                            ))
                        }
                    }
                }))
                .await
            }
            None => self.run_inner(query, params, extra).await,
        };
        if let Some(s) = &self.reg {
            s.end_command();
        }
        r
    }

    async fn begin(&mut self, extra: &PackValue) -> std::result::Result<(), BoltFailure> {
        if extra
            .get("imp_user")
            .is_some_and(|v| !matches!(v, PackValue::Null))
        {
            return Err(refuse(
                SprigCode::BoltImpersonation,
                "impersonation (imp_user) is not supported",
            ));
        }
        let r: Result<Tx> = async {
            self.take_bookmarks(extra).await?;
            Ok(Tx {
                db: extra
                    .get("db")
                    .and_then(PackValue::as_str)
                    .map(str::to_owned),
                timeout: timeout_of(extra),
                metadata: metadata_of(extra),
                engine: None,
            })
        }
        .await;
        self.tx = Some(r.map_err(|e| failure(&e))?);
        Ok(())
    }

    async fn commit(&mut self) -> std::result::Result<Option<String>, BoltFailure> {
        if self.tx.is_none() {
            return Err(BoltFailure::invalid("no open transaction"));
        }
        // Its deleted-node check may still refuse.
        let Some(mut t) = self.tx.take() else {
            return Err(BoltFailure::invalid("no open transaction"));
        };
        if let Some(et) = t.engine.take() {
            et.commit().await.map_err(|e| failure(&e))?;
        }
        let b = self.after_commit().await.map_err(|e| failure(&e))?;
        Ok(Some(b))
    }

    async fn rollback(&mut self) -> std::result::Result<(), BoltFailure> {
        self.abandon();
        Ok(())
    }

    fn last_bookmark(&self) -> Option<String> {
        self.causal().last.clone()
    }

    async fn reset(&mut self) {
        self.abandon();
    }

    async fn route(
        &mut self,
        routing: &PackValue,
        extra: &PackValue,
    ) -> std::result::Result<PackValue, BoltFailure> {
        let addr = if self.advertise.is_empty() {
            routing
                .get("address")
                .and_then(PackValue::as_str)
                .unwrap_or("localhost:7687")
                .to_owned()
        } else {
            self.advertise.clone()
        };
        let db = extra
            .get("db")
            .and_then(PackValue::as_str)
            .unwrap_or("neo4j")
            .to_owned();
        let server = |role: &str| {
            PackValue::map([
                (
                    "addresses",
                    PackValue::List(vec![PackValue::str(addr.clone())]),
                ),
                ("role", PackValue::str(role)),
            ])
        };
        Ok(PackValue::map([
            (
                "ttl",
                PackValue::Int(i64::try_from(self.ttl).unwrap_or(300)),
            ),
            ("db", PackValue::String(db)),
            (
                "servers",
                PackValue::List(vec![server("ROUTE"), server("READ"), server("WRITE")]),
            ),
        ]))
    }
}

/// Past `graph.max_time_ms` by this much, an auto-commit read stops even
/// while it waits (see [`hard_time`]).
const HARD_SLACK: Duration = Duration::from_secs(5);

/// Runs an auto-commit read with a hard time limit: `graph.max_time_ms`
/// is checked between the executor's steps, so a wait that doesn't end
/// (a connection, a lock) would keep the statement, and the connections it
/// holds, until the client gave up (the SF1 benchmark's 120 s read
/// timeouts). Past `max` and [`HARD_SLACK`] the read stops with the
/// same budget error; what it held goes back to the pools.
async fn hard_time<T>(
    max: Duration,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(max.saturating_add(HARD_SLACK), fut).await {
        Ok(r) => r,
        Err(_) => Err(sprig_graph::exec::budget_error(
            "time",
            format!("graph.max_time_ms = {}", max.as_millis()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_read_that_never_ends_stops_past_its_time_budget() {
        // A wait that never ends (a connection that never comes) ends as
        // the time budget's error, graph.max_time_ms plus the slack later.
        let started = tokio::time::Instant::now();
        let r: Result<()> = hard_time(
            Duration::from_millis(250),
            std::future::pending::<Result<()>>(),
        )
        .await;
        let e = r.expect_err("stopped");
        assert_eq!(e.code(), Some(SprigCode::GraphBudgetExceeded));
        assert!(e.to_string().contains("graph.max_time_ms = 250"), "{e}");
        assert_eq!(started.elapsed(), Duration::from_millis(250) + HARD_SLACK);
        // One that ends in time keeps its own answer.
        let r = hard_time(Duration::from_millis(250), async { Ok(7) }).await;
        assert_eq!(r.ok(), Some(7));
    }

    #[test]
    fn tx_timeouts_answer_neo4j_codes() {
        // The drivers' testkit: a lock wait cut by tx_timeout is
        // LockClientStopped; the time budget set by tx_timeout is
        // TransactionTimedOutClientConfiguration; without a tx_timeout
        // both keep their usual codes.
        let t = Some(Duration::from_millis(250));
        let lock = SprigError::backend(1205, *b"HY000", "Lock wait timeout exceeded");
        assert_eq!(
            timed_out_failure(&lock, t).code,
            "Neo.ClientError.Transaction.LockClientStopped"
        );
        assert_eq!(
            timed_out_failure(&lock, None).code,
            "Neo.TransientError.Transaction.DeadlockDetected"
        );
        let budget = sprig_graph::exec::budget_error("time", "graph.max_time_ms = 250");
        assert_eq!(
            timed_out_failure(&budget, t).code,
            "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"
        );
        // graph.max_time_ms itself was the lower bound: a budget error.
        let budget = sprig_graph::exec::budget_error("time", "graph.max_time_ms = 100");
        assert_eq!(
            timed_out_failure(&budget, t).code,
            "Neo.ClientError.Statement.SprigGraphBudgetExceeded"
        );
    }

    #[test]
    fn tx_metadata_is_a_map_or_nothing() {
        let extra = PackValue::map([(
            "tx_metadata",
            PackValue::map([
                ("foo", PackValue::Float(1.5)),
                ("bar", PackValue::str("baz")),
            ]),
        )]);
        let m = metadata_of(&extra).expect("a map");
        assert_eq!(m.len(), 2);
        assert_eq!(m["bar"], sprig_graph::value::Value::String("baz".into()));
        assert!(metadata_of(&PackValue::map([("tx_metadata", PackValue::Int(1))])).is_none());
        assert!(metadata_of(&PackValue::Map(vec![])).is_none());
    }

    #[test]
    fn plans_are_neo4j_operator_trees() {
        let steps = vec!["NodeScan n: label".to_owned(), "Filter: n.x > 1".to_owned()];
        let cols = vec!["n".to_owned()];
        let plan = plan_pack(&steps, &cols, None);
        assert_eq!(
            plan.get("operatorType").and_then(PackValue::as_str),
            Some("ProduceResults")
        );
        assert!(plan.get("rows").is_none());
        let Some(PackValue::List(kids)) = plan.get("children") else {
            panic!("children");
        };
        // The last step sits right under ProduceResults.
        assert_eq!(
            kids[0].get("operatorType").and_then(PackValue::as_str),
            Some("Filter")
        );
        let profile = plan_pack(&steps, &cols, Some(3));
        assert_eq!(profile.get("rows"), Some(&PackValue::Int(3)));
        assert_eq!(profile.get("dbHits"), Some(&PackValue::Int(0)));
    }
}
