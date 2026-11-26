//! Change streams over inheritable tables: a subscriber sees the records of
//! its own space's merged view, never the layer rows behind them.
//!
//! ```text
//! map(event of table t, subscriber in space s of its tenant):
//!   a layer row of space s' (the key carries _sprig_space):
//!     s' not s or an ancestor of s                → nothing (another space's)
//!   a parent tenant's layer row (its feed is tailed from the stream's
//!     start, along the edges into this tenant): only its root space's
//!     rows, of a table this tenant inherits, as from above the root
//!   a copy row (__sprig_ih_<object>, a materialized edge into the tenant;
//!     the stream's feed keeps these tables, and decodes a tombstone as a
//!     delete) → an event of the object
//!   a parent tenant's copy row of its own tenant edge (its feed keeps
//!     copy tables too) → an event of the object, as from above
//!   a change of a tenant edge's source or copy (a parent tenant's root or
//!     copy row, the tenant's own edge copy): the edges' copies' handovers
//!     decide whether this change or the copy's sends it
//!     (`sprig_inherit::handover`), read after the batch arrived; held while
//!     an edge's first seed runs
//!   any other change the subscriber's view doesn't read now (a space
//!     above a serving copy inside the tenant, a copy of a share inside it
//!     being seeded) → nothing: the change that reaches its view comes as
//!     its own event
//!   read the record in s now, through the subscriber's session (its space,
//!     privileges and row-level security; path columns hidden), with
//!     SPRIG_ORIGIN()
//!   present:
//!     an update at an ancestor whose changed fields are all overridden
//!       nearer to s, or that changed no visible field  → nothing
//!     an insert of a record the subscriber could see  → update
//!     an insert whose fields come from s' or below    → insert
//!     else                                            → update (after: the
//!                                                       merged record)
//!   absent: the record was visible to the subscriber just before the
//!     change → delete; else nothing
//! ```
//!
//! "Visible before" is what the stream last knew of the key ([`Seen`]:
//! the record read after its last event, while the tenant's catalog
//! version holds), else, for a key the stream hasn't seen change, a probe
//! in the subscriber's session (`exec::inherit_probe`): its merged view
//! with the changed row as it was before, filtered by its policies. A
//! `delete` thus never reveals a key the subscriber couldn't see: one a
//! nearer space hid or overrode out of its policies, one outside a
//! share's filter, one its policies never showed.
//!
//! The emitted record is the merged one read at map time, by design:
//! events of one key in one batch carry its state at the end of the batch.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use serde_json::{Map, Value};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};
use sprig_feed::{
    ChangeEvent, Clusters, FeedItem, Op, PlacementView, StartAt, TailOptions, TenantTail,
};
use sprig_inherit::handover::{self, Gtid, Handover, Verdict};
use sprig_inherit::{InheritState, SpaceId};
use tokio::sync::mpsc;

use super::super::Core;
use super::super::sql::{Caller, SqlCall, collect};

/// The layer and copy columns an event's key carries.
const LAYER_KEYS: [&str; 2] = ["_sprig_space", "_sprig_share"];

/// What an event of an inheritable object is to the subscriber.
#[derive(Debug, Clone)]
pub(super) struct Mapped {
    /// The record's key, without the layer columns.
    pub key: Map<String, Value>,
    /// The space whose layer changed; `None` for a copy (a level above the
    /// tenant's root).
    pub space: Option<SpaceId>,
    /// A copy row's `_sprig_share`.
    pub share: Option<Value>,
    /// Where the changed row is, for the copies' handover.
    pub origin: Origin,
    /// The change's GTID: domain, server, sequence number.
    pub gtid: Gtid,
}

/// Where an inheritable object's changed row is, for the handover of the
/// copies on the tenant edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Origin {
    /// A layer row of a space of the subscriber's tenant, or a copy of a
    /// share inside it: decided by what the view reads now.
    Inside,
    /// The subscriber's tenant's copy of its tenant edge.
    OwnCopy,
    /// A parent tenant's root row; the tenant's place in the line (root
    /// first).
    Above(usize),
    /// A parent tenant's copy of its own tenant edge; its place.
    AboveCopy(usize),
    /// A parent tenant's row from a tenant the line doesn't know.
    Unknown,
}

fn gtid_of(e: &ChangeEvent) -> Gtid {
    (e.txn.domain, e.txn.server, e.txn.seq)
}

/// How an event is handled.
pub(super) enum Class {
    /// Not an inheritable object's: as before.
    Plain,
    /// Another space's change, or a copy of an object the tenant lost.
    Drop,
    /// The subscriber's view may have changed.
    Inherit(Mapped),
}

/// The subscriber's tenant and space, for one batch.
pub(super) struct Mapper {
    cat: std::sync::Arc<TenantCatalog>,
    space: SpaceId,
    tenant_name: String,
    /// The tenant's ancestors, root first, then the tenant (only the
    /// tenant when an ancestor's catalog can't be read).
    line: Vec<Arc<TenantCatalog>>,
}

impl Mapper {
    /// The mapper of `caller`, when its tenant has inheritable objects.
    pub(super) async fn load(core: &Core, caller: &Caller) -> Option<Self> {
        let svc = core.engine.tenants.clone()?;
        let tenant = caller.principal.tenant?;
        let cat = svc
            .cache
            .get(tenant, Freshness::Cached { min_version: 0 })
            .await
            .ok()?;
        let st = cat.ext.get::<InheritState>(sprig_inherit::MODULE)?;
        st.objects().next()?;
        let space = caller
            .principal
            .user_id
            .map_or(sprig_inherit::ROOT_SPACE, |u| st.home(u.0));
        let tenant_name = caller
            .principal
            .tenant_name
            .clone()
            .unwrap_or_else(|| tenant.to_string());
        let mut line = Vec::new();
        for a in cat.ancestry.iter() {
            if let Ok(c) = svc
                .cache
                .get(a.id, Freshness::Cached { min_version: 0 })
                .await
            {
                line.push(c);
            } else {
                line.clear();
                break;
            }
        }
        line.push(Arc::clone(&cat));
        Some(Self {
            cat,
            space,
            tenant_name,
            line,
        })
    }

    fn state(&self) -> Option<&InheritState> {
        self.cat.ext.get::<InheritState>(sprig_inherit::MODULE)
    }

    /// Classifies `e` (`own`: from this tenant's feed, else a parent
    /// tenant's); a copy row's event is renamed to its object's table.
    pub(super) fn classify(&self, e: &mut ChangeEvent, own: bool) -> Class {
        let Some(st) = self.state() else {
            return if own { Class::Plain } else { Class::Drop };
        };
        if !own {
            return self.from_above(st, e);
        }
        let Some(db) = self
            .cat
            .database(&sprig_catalog::ddl::database_key(&e.db_name, false))
        else {
            return Class::Plain;
        };
        let copy = e
            .table
            .strip_prefix("__sprig_ih_")
            .and_then(|n| n.parse::<u64>().ok());
        let obj = match copy {
            Some(id) => match st.object(ObjectId(id)) {
                Some(o) if o.db == db.db_id => o,
                _ => return Class::Drop,
            },
            None => match st.object_named_ci(db.db_id, &e.table) {
                Some(o) => o,
                None => return Class::Plain,
            },
        };
        if matches!(e.op, Op::Ddl(_) | Op::Truncate) {
            return if copy.is_some() {
                Class::Drop
            } else {
                Class::Plain
            };
        }
        let mut key = Map::new();
        let mut space = None;
        let mut share = None;
        for (n, v) in &e.key.0 {
            if n == "_sprig_share" {
                if let Value::Object(m) = sprig_streams::envelope::image_json(&sprig_feed::Image(
                    vec![(n.clone(), v.clone())],
                )) {
                    share = m.get(n).cloned();
                }
                continue;
            }
            if n == "_sprig_space" {
                space = match sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                    n.clone(),
                    v.clone(),
                )])) {
                    Value::Object(m) => m.get(n).and_then(Value::as_u64).map(SpaceId),
                    _ => None,
                };
                continue;
            }
            if LAYER_KEYS.contains(&n.as_str()) {
                continue;
            }
            if let Value::Object(m) =
                sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                    n.clone(),
                    v.clone(),
                )]))
            {
                key.extend(m);
            }
        }
        if copy.is_none() {
            let Some(s) = space else {
                return Class::Plain;
            };
            // Another branch's change, or a space below the subscriber's.
            if !st.is_ancestor_or_self(s, self.space) {
                return Class::Drop;
            }
        }
        e.table.clone_from(&obj.table);
        let share = copy.and(share);
        let origin = match &share {
            Some(v) if copy_share(v) == Some(sprig_inherit::copy::UPSTREAM) => Origin::OwnCopy,
            _ => Origin::Inside,
        };
        Class::Inherit(Mapped {
            key,
            space,
            share,
            origin,
            gtid: gtid_of(e),
        })
    }

    /// A parent tenant's event: its root space's layer rows of a table this
    /// tenant inherits, and the rows of its copy of its own tenant edge,
    /// renamed to the table.
    #[allow(clippy::wrong_self_convention)] // "from" the parent tenants
    fn from_above(&self, st: &InheritState, e: &mut ChangeEvent) -> Class {
        if matches!(e.op, Op::Ddl(_) | Op::Truncate) {
            return Class::Drop;
        }
        let at = self.line.iter().position(|c| c.tenant == e.tenant);
        let copy = e
            .table
            .strip_prefix("__sprig_ih_")
            .and_then(|n| n.parse::<u64>().ok());
        let table = match copy {
            Some(id) => {
                // Its copy's object, in that tenant's catalog.
                let Some(o) = at
                    .and_then(|j| self.line[j].ext.get::<InheritState>(sprig_inherit::MODULE))
                    .and_then(|s| s.object(ObjectId(id)))
                else {
                    return Class::Drop;
                };
                o.table.clone()
            }
            None if e.table.starts_with("__sprig") => return Class::Drop,
            None => e.table.clone(),
        };
        let Some(db) = self
            .cat
            .database(&sprig_catalog::ddl::database_key(&e.db_name, false))
        else {
            return Class::Drop;
        };
        let Some(obj) = st.object_named_ci(db.db_id, &table) else {
            return Class::Drop;
        };
        let mut key = Map::new();
        for (n, v) in &e.key.0 {
            let Value::Object(m) = sprig_streams::envelope::image_json(&sprig_feed::Image(vec![(
                n.clone(),
                v.clone(),
            )])) else {
                continue;
            };
            if n == "_sprig_space" {
                if m.get(n).and_then(Value::as_u64) != Some(sprig_inherit::ROOT_SPACE.0) {
                    return Class::Drop;
                }
                continue;
            }
            if n == "_sprig_share" {
                // Only the copy of the tenant edge: a copy of a share
                // inside that tenant feeds one of its spaces.
                if m.get(n).and_then(copy_share) != Some(sprig_inherit::copy::UPSTREAM) {
                    return Class::Drop;
                }
                continue;
            }
            if !LAYER_KEYS.contains(&n.as_str()) {
                key.extend(m);
            }
        }
        if copy.is_some() {
            e.table.clone_from(&obj.table);
        }
        let origin = match (at, copy) {
            (Some(j), None) => Origin::Above(j),
            (Some(j), Some(_)) => Origin::AboveCopy(j),
            (None, _) => Origin::Unknown,
        };
        Class::Inherit(Mapped {
            key,
            space: None,
            share: None,
            origin,
            gtid: gtid_of(e),
        })
    }

    /// The copies' handovers on the tenant edges into the line's tenants
    /// for `db`.`table`, by place in the line: `None` for an edge
    /// without a materialized copy, or whose copy has no state yet (a seed
    /// that hasn't begun reads later than any change the stream has).
    /// Read now, after the batch's changes arrived.
    pub(super) async fn handovers(
        &self,
        svc: &crate::tenant::TenantServices,
        db: &str,
        table: &str,
    ) -> Vec<Option<Handover>> {
        let mut out = vec![None; self.line.len()];
        for (k, pair) in self.line.windows(2).enumerate() {
            let k = k + 1;
            let (parent, child) = (&pair[0], &pair[1]);
            let materialized = matches!(
                sprig_inherit::plan::tenant_edge(parent, child, db, table, false),
                Ok(Some(up)) if up.share.mode == sprig_inherit::model::MergeMode::Materialized
            );
            if !materialized {
                continue;
            }
            let Some(st) = child.ext.get::<InheritState>(sprig_inherit::MODULE) else {
                continue;
            };
            let Some(o) = child
                .database(&sprig_catalog::ddl::database_key(db, false))
                .and_then(|d| st.object_named_ci(d.db_id, table))
            else {
                continue;
            };
            let Ok(schema) = sprig_inherit::plan::physical_schema(child, db, false) else {
                continue;
            };
            let read = async {
                let cluster = svc.locator.cluster(child.tenant).await?;
                let pool = svc.locator.ctl_pool(&cluster).await?;
                let mut conn = pool.get_conn_bounded().await.map_err(|e| {
                    sprig_common::SprigError::new(
                        sprig_common::SprigCode::BackendUnavailable,
                        e.to_string(),
                    )
                })?;
                sprig_inherit::copy::load_states(&mut conn, child.tenant, &schema).await
            };
            match read.await {
                Ok(rows) => {
                    out[k] = rows
                        .into_iter()
                        .find(|(key, _)| {
                            key.object == o.object_id && key.share == sprig_inherit::copy::UPSTREAM
                        })
                        .map(|(_, info)| info.handover);
                }
                Err(e) => {
                    // Unknown: the line's verdicts fall back to what the
                    // view reads now.
                    tracing::warn!(tenant = %child.tenant, error = %e, "a copy's handover wasn't read for a change stream");
                    out[k] = Some(Handover::At {
                        source: String::new(),
                        child: String::new(),
                    });
                }
            }
        }
        out
    }

    /// [`Self::handovers`] for copies on engine databases: their state rows
    /// in the child tenants' catalog records (`m/inherit/copies/`), whose
    /// handover is a pair of engine commit timestamps in the form the
    /// handover rules compare engine changes in (`0-0-<cts>`: one clock for
    /// every unit of the node).
    pub(super) async fn engine_handovers(
        &self,
        svc: &crate::tenant::TenantServices,
        db: &str,
        table: &str,
    ) -> Vec<Option<Handover>> {
        let mut out = vec![None; self.line.len()];
        for (k, pair) in self.line.windows(2).enumerate() {
            let k = k + 1;
            let (parent, child) = (&pair[0], &pair[1]);
            let materialized = matches!(
                sprig_inherit::plan::tenant_edge(parent, child, db, table, false),
                Ok(Some(up)) if up.share.mode == sprig_inherit::model::MergeMode::Materialized
            );
            if !materialized {
                continue;
            }
            let Some(st) = child.ext.get::<InheritState>(sprig_inherit::MODULE) else {
                continue;
            };
            let Some(o) = child
                .database(&sprig_catalog::ddl::database_key(db, false))
                .and_then(|d| st.object_named_ci(d.db_id, table))
            else {
                continue;
            };
            match crate::exec::inherit_tree::engine_copy_states(svc, child.tenant).await {
                Ok(rows) => {
                    // A copy seeded from another share (one dropped and
                    // made again) isn't this edge's yet.
                    out[k] = crate::exec::inherit_tree::current_copies(rows, child, Some(parent))
                        .into_iter()
                        .find(|(key, _)| {
                            key.object == o.object_id && key.share == sprig_inherit::copy::UPSTREAM
                        })
                        .map(|(_, row)| row.handover());
                }
                Err(e) => {
                    tracing::warn!(tenant = %child.tenant, error = %e, "a copy's handover wasn't read for a change stream");
                    out[k] = Some(Handover::At {
                        source: String::new(),
                        child: String::new(),
                    });
                }
            }
        }
        out
    }

    /// Whether `m`'s change counts, given [`Self::handovers`] of its table;
    /// `None` for a change inside the tenant (what the view reads now
    /// decides).
    pub(super) fn verdict(&self, m: &Mapped, edges: &[Option<Handover>]) -> Option<Verdict> {
        let n = self.line.len() - 1;
        let at = |k: usize| edges.get(k).and_then(Option::as_ref);
        let below = |j: usize| ((j + 1)..=n).map(at).collect::<Vec<_>>();
        Some(match m.origin {
            Origin::Inside => return None,
            Origin::OwnCopy => handover::along(Some(at(n)), &[], m.gtid),
            Origin::Above(j) if j < n => handover::along(None, &below(j), m.gtid),
            Origin::AboveCopy(j) if j < n => handover::along(Some(at(j)), &below(j), m.gtid),
            Origin::Unknown | Origin::Above(_) | Origin::AboveCopy(_) => Verdict::Unknown,
        })
    }

    /// The tenant catalog's version (policies and grants change it).
    pub(super) fn version(&self) -> u64 {
        self.cat.version
    }

    /// The names of the spaces strictly above `s` in the tenant, and the
    /// tenant's ancestors' names (origins a field inherited from).
    fn above(&self, s: Option<SpaceId>) -> BTreeSet<String> {
        let mut out: BTreeSet<String> = self.cat.ancestry.iter().map(|a| a.name.clone()).collect();
        let (Some(st), Some(s)) = (self.state(), s) else {
            return out;
        };
        for x in st.path(s) {
            if x == s {
                break;
            }
            if let Some(sp) = st.space(x, &self.tenant_name) {
                out.insert(sp.name);
            }
        }
        out
    }

    /// The names of the spaces strictly below `s` down to the subscriber's
    /// (overrides nearer to the subscriber).
    fn nearer(&self, s: Option<SpaceId>) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        let Some(st) = self.state() else {
            return out;
        };
        let path = st.path(self.space);
        let from = s.and_then(|s| path.iter().position(|x| *x == s));
        for x in path.iter().skip(from.map_or(0, |i| i + 1)) {
            if let Some(sp) = st.space(*x, &self.tenant_name) {
                out.insert(sp.name);
            }
        }
        out
    }
}

/// A copy row's `_sprig_share` value as a number.
fn copy_share(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// A merged record: its columns as JSON, and each field's origin.
pub(super) type Record = (Map<String, Value>, Map<String, Value>);

fn sql_lit(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")),
        other => format!(
            "'{}'",
            other.to_string().replace('\\', "\\\\").replace('\'', "''")
        ),
    }
}

/// A key's identity in the maps below.
pub(super) fn key_id(table: &str, key: &Map<String, Value>) -> String {
    format!(
        "{}\u{1}{}",
        table.to_lowercase(),
        Value::Object(key.clone())
    )
}

fn numeric(mysql_type: u32) -> bool {
    // DECIMAL, TINY, SHORT, LONG, FLOAT, DOUBLE, LONGLONG, INT24, YEAR,
    // NEWDECIMAL.
    matches!(mysql_type, 0..=5 | 8 | 9 | 13 | 246)
}

/// Reads the merged records of `keys` (table, key) in the subscriber's
/// space, through its own session.
///
/// # Errors
/// The statement's (a table the subscriber can't read: 1142).
pub(super) async fn read(
    core: &Core,
    caller: &Caller,
    db: &str,
    keys: &[(String, Map<String, Value>)],
) -> Result<HashMap<String, Record>> {
    let mut by_table: HashMap<String, Vec<&Map<String, Value>>> = HashMap::new();
    for (t, k) in keys {
        by_table.entry(t.clone()).or_default().push(k);
    }
    let mut out = HashMap::new();
    for (table, ks) in by_table {
        let Some(first) = ks.first() else {
            continue;
        };
        let cols: Vec<String> = first.keys().cloned().collect();
        if cols.is_empty() {
            continue;
        }
        let lhs = if cols.len() == 1 {
            sprig_common::names::quote(&cols[0])
        } else {
            format!(
                "({})",
                cols.iter()
                    .map(|c| sprig_common::names::quote(c))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let tuples: Vec<String> = ks
            .iter()
            .map(|k| {
                let vals: Vec<String> = cols
                    .iter()
                    .map(|c| sql_lit(k.get(c).unwrap_or(&Value::Null)))
                    .collect();
                if vals.len() == 1 {
                    vals[0].clone()
                } else {
                    format!("({})", vals.join(", "))
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let sql = format!(
            "SELECT *, SPRIG_ORIGIN() FROM {} WHERE {lhs} IN ({})",
            sprig_common::names::quote(&table),
            tuples.join(", ")
        );
        let (tx, rx) = mpsc::channel(4);
        let engine = std::sync::Arc::clone(&core.engine);
        let call = SqlCall {
            database: db.to_owned(),
            sql,
            multi: false,
        };
        let c = caller.clone();
        let space = crate::http::sql::call_space();
        let task = tokio::spawn(crate::http::sql::SPACE.scope(space, async move {
            engine.run(&c, call, None, tx).await;
        }));
        let resp = collect(rx, 64 << 20, None).await;
        let _ = task.await;
        let resp = resp?;
        let Some(res) = resp.results.first() else {
            continue;
        };
        let n = res.columns.len();
        for row in &res.rows {
            let mut rec = Map::new();
            let mut origins = Map::new();
            for (i, (c, v)) in res.columns.iter().zip(&row.values).enumerate() {
                let text = sprig_api::results::cell_bytes(v, false)
                    .map(|b| String::from_utf8_lossy(&b).into_owned());
                if i + 1 == n {
                    if let Some(Value::Object(m)) =
                        text.as_deref().and_then(|t| serde_json::from_str(t).ok())
                    {
                        origins = m;
                    }
                    continue;
                }
                let j = match text {
                    None => Value::Null,
                    Some(t) if numeric(c.mysql_type) => serde_json::from_str(&t)
                        .ok()
                        .filter(Value::is_number)
                        .unwrap_or(Value::String(t)),
                    Some(t) => Value::String(t),
                };
                rec.insert(c.name.clone(), j);
            }
            let key: Map<String, Value> = cols
                .iter()
                .filter_map(|c| rec.get(c).map(|v| (c.clone(), v.clone())))
                .collect();
            out.insert(key_id(&table, &key), (rec, origins));
        }
    }
    Ok(out)
}

/// The event an inheritable object's change is to the subscriber, given
/// its merged record now (`None`: not in its view) and whether the record
/// was visible to it just before the change (`None`: the stream doesn't
/// know; taken as not visible). `false`: nothing to send.
pub(super) fn decide(
    mapper: &Mapper,
    e: &ChangeEvent,
    m: &Mapped,
    rec: Option<&Record>,
    was: Option<bool>,
    full: bool,
    out: &mut super::WatchEvent,
) -> bool {
    out.key = Value::Object(m.key.clone()).to_string();
    let was_visible = was.unwrap_or(false);
    let Some((row, origins)) = rec else {
        if was_visible {
            out.op = "delete".into();
            return true;
        }
        return false;
    };
    let origin = |f: &str| {
        origins
            .get(f)
            .and_then(Value::as_str)
            .map(|o| o.trim_end_matches(" (orphaned)").to_owned())
    };
    let visible: Vec<String> = e
        .delta
        .as_ref()
        .map(|d| d.iter().filter(|c| row.contains_key(*c)).cloned().collect())
        .unwrap_or_default();
    // A record the stream last knew as gone (it sent its delete, or never
    // showed it) that is back: an insert. A cross-tenant override is out
    // of the view from its parent's delete until the orphan pass turns it
    // into an orphan, and the orphan's own change brings it back.
    if was == Some(false) && matches!(e.op, Op::Update | Op::Delete) {
        out.op = "insert".into();
        out.after = Value::Object(row.clone()).to_string();
        return true;
    }
    match e.op {
        Op::Update => {
            if visible.is_empty() {
                // Paths, bits or a hide's state: nothing the subscriber
                // sees changed.
                return false;
            }
            if m.space != Some(mapper.space) {
                let nearer = mapper.nearer(m.space);
                if visible
                    .iter()
                    .all(|f| origin(f).is_some_and(|o| nearer.contains(&o)))
                {
                    return false;
                }
            }
            out.op = "update".into();
            let changed: Map<String, Value> = row
                .iter()
                .filter(|(k, _)| visible.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            out.update = Value::Object(changed).to_string();
            if full {
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        Op::Insert => {
            let above = mapper.above(m.space);
            // A record the subscriber already saw (a copy row made while
            // its own row or a nearer override showed the record) isn't
            // new to it.
            let inherited = was_visible
                || (m.space.is_some()
                    && row
                        .keys()
                        .any(|f| origin(f).is_some_and(|o| above.contains(&o))));
            if inherited {
                out.op = "update".into();
                out.update = Value::Object(row.clone()).to_string();
                if full {
                    out.after = Value::Object(row.clone()).to_string();
                }
            } else {
                out.op = "insert".into();
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        // A row gone while the record stays: the fields that now come from
        // another level may have changed. Fields a level nearer to the
        // subscriber supplies didn't: a parent's delete that leaves the
        // child's override as an orphan (which keeps the values the child
        // saw) sends nothing. A row of the subscriber's own space (an
        // override, hide or own row) has no nearer level: every field.
        // The key's columns name the record (`key`), as in an update.
        Op::Delete => {
            let nearer = mapper.nearer(m.space);
            let changed: Map<String, Value> = row
                .iter()
                .filter(|(f, _)| !m.key.contains_key(*f))
                .filter(|(f, _)| !origin(f).is_some_and(|o| nearer.contains(&o)))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if changed.is_empty() {
                return false;
            }
            out.op = "update".into();
            out.update = Value::Object(changed).to_string();
            if full {
                out.after = Value::Object(row.clone()).to_string();
            }
        }
        Op::Ddl(_) | Op::Truncate => return false,
    }
    true
}

/// The most keys [`Seen`] holds; past it, it starts again (probes cover
/// the keys it forgot).
const SEEN_MAX: usize = 100_000;

/// What the stream last knew of each key's visibility: set from the
/// record read after each of the key's events, cleared when the tenant's
/// catalog version moves (a policy or grant may have changed what the
/// subscriber sees).
#[derive(Debug, Default)]
pub(super) struct Seen {
    version: u64,
    keys: HashMap<String, bool>,
}

impl Seen {
    /// Starts a batch read at catalog `version`.
    pub(super) fn at(&mut self, version: u64) {
        if version != self.version {
            self.keys.clear();
            self.version = version;
        }
    }

    /// The key's visibility after its last event, if the stream knows it.
    pub(super) fn get(&self, id: &str) -> Option<bool> {
        self.keys.get(id).copied()
    }

    /// Records the key's visibility after an event.
    pub(super) fn set(&mut self, id: String, visible: bool) {
        if self.keys.len() >= SEEN_MAX && !self.keys.contains_key(&id) {
            self.keys.clear();
        }
        self.keys.insert(id, visible);
    }
}

/// The probe of event `e` (mapped as `m`): its record before the change
/// ([`crate::exec::inherit_probe`]).
pub(super) fn probe_item(e: &ChangeEvent, m: &Mapped) -> crate::exec::inherit_probe::ProbeItem {
    let img = |i: Option<&sprig_feed::Image>| match i.map(sprig_streams::envelope::image_json) {
        Some(Value::Object(o)) => Some(o),
        _ => None,
    };
    let before = img(e.before.as_ref());
    let columns = if before.is_none() {
        img(e.after.as_ref())
            .map(|a| a.keys().cloned().collect())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    crate::exec::inherit_probe::ProbeItem {
        table: e.table.clone(),
        key: m.key.clone(),
        space: m.space.filter(|_| m.share.is_none()).map(|s| s.0),
        share: m.share.clone(),
        before,
        columns,
        reads_only: false,
        counted: false,
    }
}

/// Runs `req` in the subscriber's session: whether each probe's record was
/// visible to it before its change (`None`: its view doesn't read the
/// changed row).
///
/// # Errors
/// The statement's.
pub(super) async fn probe(
    core: &Core,
    caller: &Caller,
    req: &crate::exec::inherit_probe::ProbeRequest,
) -> Result<Vec<Option<bool>>> {
    let (tx, rx) = mpsc::channel(4);
    let engine = std::sync::Arc::clone(&core.engine);
    let call = SqlCall {
        database: req.db.clone(),
        sql: crate::exec::inherit_probe::statement(req),
        multi: false,
    };
    let c = caller.clone();
    let space = crate::http::sql::call_space();
    let task = tokio::spawn(crate::http::sql::SPACE.scope(space, async move {
        engine.run(&c, call, None, tx).await;
    }));
    let resp = collect(rx, 64 << 20, None).await;
    let _ = task.await;
    let resp = resp?;
    let rows: Vec<Vec<Option<String>>> = resp
        .results
        .first()
        .map(|r| {
            r.rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .map(|v| {
                            sprig_api::results::cell_bytes(v, false)
                                .map(|b| String::from_utf8_lossy(&b).into_owned())
                        })
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(crate::exec::inherit_probe::answer(req, &rows))
}

/// Tails the feeds of `tenant`'s ancestors that its inheritable objects
/// read through (nearest first, while an edge continues), from now; their
/// items in one channel. `None` when there is none.
pub(super) async fn ancestor_tails(
    svc: &crate::tenant::TenantServices,
    tenant: TenantId,
    clusters: &Arc<dyn Clusters>,
    placement: &Arc<dyn PlacementView>,
    names: &Arc<dyn sprig_feed::names::Resolver>,
) -> Option<mpsc::Receiver<Result<FeedItem>>> {
    let cat = svc
        .cache
        .get(tenant, Freshness::Cached { min_version: 0 })
        .await
        .ok()?;
    let st = cat.ext.get::<InheritState>(sprig_inherit::MODULE)?;
    if st.objects().next().is_none() || cat.ancestry.is_empty() {
        return None;
    }
    let mut line: Vec<Arc<TenantCatalog>> = Vec::new();
    for a in cat.ancestry.iter() {
        line.push(
            svc.cache
                .get(a.id, Freshness::Cached { min_version: 0 })
                .await
                .ok()?,
        );
    }
    line.push(Arc::clone(&cat));
    let mut tenants = Vec::new();
    let mut j = line.len() - 1;
    while j >= 1 {
        let (parent, child) = (&line[j - 1], &line[j]);
        let Some(cs) = child.ext.get::<InheritState>(sprig_inherit::MODULE) else {
            break;
        };
        let edge = cs.objects().any(|o| {
            child.databases.get(o.db).is_some_and(|d| {
                matches!(
                    sprig_inherit::plan::tenant_edge(parent, child, &d.name, &o.table, false),
                    Ok(Some(_))
                )
            })
        });
        if !edge {
            break;
        }
        tenants.push(parent.tenant);
        j -= 1;
    }
    if tenants.is_empty() {
        return None;
    }
    let (tx, rx) = mpsc::channel(256);
    for t in tenants {
        let tail = TenantTail::start(
            t,
            StartAt::Now,
            Arc::clone(clusters),
            Arc::clone(placement),
            Arc::clone(names),
            TailOptions {
                progress_every: std::time::Duration::from_millis(500),
                // The parents' copies of their own tenant edges.
                keep_internal: vec!["__sprig_ih_".to_owned()],
                ..TailOptions::default()
            },
        )
        .await;
        let mut tail = match tail {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(tenant = %t, error = %e, "a parent tenant's feed can't be tailed for a change stream");
                continue;
            }
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                match tail.next().await {
                    Ok(i) => {
                        if tx.send(Ok(i)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(tenant = %t, error = %e, "a parent tenant's feed stopped for a change stream");
                        return;
                    }
                }
            }
        });
    }
    Some(rx)
}

/// The next item of the stream's own feed or of a parent tenant's
/// (`true`: its own).
pub(super) async fn next(
    tail: &mut super::OwnFeed,
    above: &mut Option<mpsc::Receiver<Result<FeedItem>>>,
) -> (bool, Result<FeedItem>) {
    match above {
        None => (true, tail.next().await),
        Some(rx) => tokio::select! {
            i = tail.next() => (true, i),
            Some(i) = rx.recv() => (false, i),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId};
    use sprig_settings::Registry;

    /// A child tenant `child` whose subscriber reads its root space.
    fn mapper() -> Mapper {
        let mut c = TenantCatalog::empty(Registry::global().unwrap(), TenantId(2));
        c.ext = c.ext.with(vec![(
            sprig_inherit::MODULE,
            Arc::new(InheritState::default()) as Arc<_>,
        )]);
        let cat = Arc::new(c);
        Mapper {
            cat: Arc::clone(&cat),
            space: sprig_inherit::ROOT_SPACE,
            tenant_name: "child".into(),
            line: vec![cat],
        }
    }

    fn event(op: Op, delta: Option<&[&str]>) -> ChangeEvent {
        let gtid = sprig_binlog::Gtid {
            domain: 0,
            server: 0,
            seq: 1,
        };
        let cluster = ClusterId::new("engine").unwrap();
        ChangeEvent {
            id: sprig_feed::EventId {
                cluster: cluster.clone(),
                gtid,
                row: 0,
            },
            token: sprig_feed::ResumeToken {
                tenant: TenantId(1),
                clamp: 0,
                cluster,
                segment: 0,
                pos: sprig_binlog::GtidPos::default(),
                until: None,
                progress: true,
            },
            tenant: TenantId(1),
            db: DbId(1),
            db_name: "shop".into(),
            object: 1,
            table: "products".into(),
            shard: sprig_feed::ShardRef {
                group: GroupId(0),
                shard: ShardId(0),
            },
            key: sprig_feed::Image(Vec::new()),
            op,
            before: None,
            after: None,
            delta: delta.map(|d| d.iter().map(|s| (*s).to_owned()).collect()),
            hlc: 1,
            txn: gtid,
            ddl: None,
            hint: None,
        }
    }

    fn mapped(space: Option<SpaceId>) -> Mapped {
        Mapped {
            key: json!({"id": 3}).as_object().unwrap().clone(),
            space,
            share: None,
            origin: Origin::Above(0),
            gtid: (0, 0, 1),
        }
    }

    /// Record 3 as the child reads it: `price` from `price_from`, the rest
    /// from `rest_from`.
    fn record(price_from: &str, rest_from: &str) -> Record {
        let row = json!({"id": 3, "name": "n3", "price": 999});
        let origins = json!({"id": rest_from, "name": rest_from, "price": price_from});
        (
            row.as_object().unwrap().clone(),
            origins.as_object().unwrap().clone(),
        )
    }

    fn run(
        op: Op,
        space: Option<SpaceId>,
        rec: Option<&Record>,
        was: Option<bool>,
    ) -> Option<WatchEventView> {
        let mut out = super::super::WatchEvent::default();
        let e = event(op, Some(&["price"]));
        decide(&mapper(), &e, &mapped(space), rec, was, true, &mut out).then(|| WatchEventView {
            op: out.op.clone(),
            update: out.update.clone(),
        })
    }

    #[derive(Debug, PartialEq)]
    struct WatchEventView {
        op: String,
        update: String,
    }

    #[test]
    fn a_parent_delete_that_leaves_an_orphan_sends_nothing() {
        // The child's override is now an orphan: every field is its own.
        let rec = record("child (orphaned)", "child (orphaned)");
        assert_eq!(run(Op::Delete, None, Some(&rec), Some(true)), None);
    }

    #[test]
    fn a_delete_sends_the_fields_that_now_come_from_another_level() {
        // An ancestor's override of `price` gone: `price` comes from the
        // parent again, `name` stays the child's own.
        let rec = record("parent", "child");
        let got = run(Op::Delete, None, Some(&rec), Some(true)).unwrap();
        assert_eq!(got.op, "update");
        assert_eq!(got.update, json!({"price": 999}).to_string());
        // The subscriber's own override gone: every field but the key.
        let got = run(
            Op::Delete,
            Some(sprig_inherit::ROOT_SPACE),
            Some(&rec),
            Some(true),
        )
        .unwrap();
        assert_eq!(got.update, json!({"name": "n3", "price": 999}).to_string());
    }

    #[test]
    fn a_record_known_gone_that_is_back_is_an_insert() {
        let rec = record("child (orphaned)", "child (orphaned)");
        // The orphan pass's change after the stream sent the parent's delete.
        let got = run(
            Op::Update,
            Some(sprig_inherit::ROOT_SPACE),
            Some(&rec),
            Some(false),
        )
        .unwrap();
        assert_eq!(got.op, "insert");
        // Unknown before: as before (nothing visible changed).
        let e = event(Op::Update, Some(&["_sprig_state"]));
        let mut out = super::super::WatchEvent::default();
        assert!(!decide(
            &mapper(),
            &e,
            &mapped(Some(sprig_inherit::ROOT_SPACE)),
            Some(&rec),
            None,
            true,
            &mut out
        ));
        // Gone now: a delete only when it was visible.
        assert_eq!(
            run(Op::Delete, None, None, Some(true)).map(|v| v.op),
            Some("delete".into())
        );
        assert_eq!(run(Op::Delete, None, None, None), None);
    }
}
