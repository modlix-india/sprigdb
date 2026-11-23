//! After-commit triggers on engine tables: `CREATE TRIGGER g AFTER COMMIT
//! ON t [FOR INSERT, UPDATE [OF (c, ...)], DELETE] LANGUAGE ... AS ...` on
//! an engine database, run from the node's change log
//! ([`crate::table_changes`]).
//!
//! **Declaring.** The trigger is recorded as a trigger on the table's
//! change topic (`<table>.changes`, the stream id of
//! [`crate::kafka::changes`]): a binding with the trigger's events and
//! `FOR UPDATE OF` columns, a source row, and the index entry that makes
//! the controller start its job (`logic_stream_trigger`). Its position, an
//! engine record of the consumer group `__sprig_logic_<binding>`, starts at
//! the change log's head: the trigger fires for changes committed after it
//! was created. The definer needs `TRIGGER` and `SELECT` on the table.
//! Every version runs in the engine tier.
//!
//! **Running.** The worker's job asks a gateway for one run at a time (as
//! a queue trigger does); the run, in a session as the definer:
//!
//! ```text
//! pos := the group's committed offset (a change-topic offset)
//! changes after pos (kept between runs, else read from the log, waiting
//!   up to 2 s for one)
//! e := the first change the binding wants (its op, a FOR UPDATE OF column
//!   changed); none → the position moves past what was read: Empty
//! USE db; START TRANSACTION
//!   move the position from pos to after e (compare-and-set: moved by
//!   another run → ROLLBACK, Moved)
//!   the version with ctx.trigger = e (KIRun: parameters bound by name to
//!   the new row's columns, old_<column>, op)
//! COMMIT → Ok
//! failure → ROLLBACK; after logic.trigger_max_attempts failures that
//!   aren't transient: a dead letter, and the position moves past e
//! ```
//!
//! The position commits with the code's writes, so they happen exactly
//! once; outside effects are at least once, with `ctx.trigger.eventKey`
//! (`u<unit>:<cts>:<entry>:<record>`) as their idempotency key. Events
//! run one at a time in commit order. A position older than the change
//! log's retention moves to the oldest change kept, with a warning (the
//! changes in between are lost to the trigger).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::LogicStore;
use sprig_logic::declare::{Declaration, MessageSource, SourceKind, TierPref};
use sprig_logic::messages::{self, MsgIndex, RunOutcome, RunReply, RunRequest, SourceRow};
use sprig_streams::changes::Pos;

use super::PassSession;
use crate::kafka::changes::{STREAM_FLAG, next_offset, offset_of, pos_before};
use crate::pipeline::ExecCtx;
use crate::table_changes::{Event, Watched};

/// How long a run waits for a change when the log has none for it.
const WAIT: Duration = Duration::from_secs(2);

/// Bytes of the log one read takes at most.
const PAGE_BYTES: usize = 256 * 1024;

/// How long a binding's events and `FOR UPDATE OF` columns are cached.
const FILTER_TTL: Duration = Duration::from_secs(5);

/// How often the definer's `SELECT` on the table is checked again.
const CHECK_EVERY: Duration = Duration::from_secs(30);

/// Whether a trigger's source is an engine table's changes.
#[must_use]
pub(crate) fn is_change_source(src: &SourceRow) -> bool {
    src.kind == SourceKind::Stream && src.object_id & STREAM_FLAG != 0
}

/// What a binding wants.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Filter {
    events: Vec<String>,
    when: Vec<String>,
}

impl Filter {
    fn wants(&self, e: &Event) -> bool {
        let op = op_name(e.op);
        if !self.events.iter().any(|x| x == op) {
            return false;
        }
        if op != "update" || self.when.is_empty() {
            return true;
        }
        // No before-image (a reader far behind): it may have changed.
        let (Some(b), Some(a)) = (e.before.as_ref(), e.after.as_ref()) else {
            return true;
        };
        self.when.iter().any(|w| field(b, w) != field(a, w))
    }
}

/// `insert`, `update` or `delete` for a change's `c`, `u` or `d`.
fn op_name(op: &str) -> &'static str {
    match op {
        "c" => "insert",
        "d" => "delete",
        _ => "update",
    }
}

/// Column `name` of a row image, matched without case.
fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    let m = row.as_object()?;
    m.get(name).or_else(|| {
        m.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

/// Changes read past a run's position and not run yet, by tenant and
/// binding: the next run starting at `from` takes them instead of reading
/// the log again.
struct Pending {
    from: i64,
    events: VecDeque<Event>,
    end: Pos,
    at_end: bool,
}

type Key = (u64, u64);

static PENDING: LazyLock<Mutex<HashMap<Key, Pending>>> = LazyLock::new(Mutex::default);
/// Failed attempts of the event at an offset, by tenant and binding (kept
/// by the gateway the binding's runs go to; another gateway counts anew).
static FAILS: LazyLock<Mutex<HashMap<Key, (i64, u32)>>> = LazyLock::new(Mutex::default);
static FILTERS: LazyLock<Mutex<HashMap<Key, (Instant, Filter)>>> = LazyLock::new(Mutex::default);
static CHECKED: LazyLock<Mutex<HashMap<Key, Instant>>> = LazyLock::new(Mutex::default);

/// The change log's durable version hold of a binding
/// ([`sprig_streams::changes::ChangeLog::hold_for`]), at the end of what
/// its runs read last: the before-images of the changes after it outlive a
/// long wait (a failing change's retries) and a restart of the node, where
/// the engine keeps versions only a minute otherwise.
fn keep_hold(k: Key, cts: u64) {
    if let Some(log) = crate::changelog::changes() {
        log.hold_for(&format!("trigger/{}/{}", k.0, k.1), Some(cts));
    }
}

/// Forgets `k`'s hold (its table is gone).
fn drop_hold(k: Key) {
    if let Some(log) = crate::changelog::changes() {
        log.hold_for(&format!("trigger/{}/{}", k.0, k.1), None);
    }
}

/// Releases the holds of every binding of trigger module `module` of
/// `tenant` (it was dropped).
pub(super) async fn release_module(
    svc: &crate::tenant::TenantServices,
    tenant: sprig_common::ids::TenantId,
    module: u64,
) {
    let Some(log) = crate::changelog::changes() else {
        return;
    };
    let prefix = format!("trigger/{}/", tenant.0);
    let held: Vec<u64> = log
        .held()
        .iter()
        .filter_map(|n| n.strip_prefix(&prefix)?.parse().ok())
        .collect();
    if held.is_empty() {
        return;
    }
    let Ok(mut c) = svc.records.begin(tenant).await else {
        return;
    };
    let store = LogicStore::default();
    for b in held {
        if let Ok(Some(row)) = store.binding_by_id(&mut *c, b).await
            && row.module_id == module
        {
            drop_hold((tenant.0, b));
        }
    }
    let _ = c.rollback().await;
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The reader key of a binding's reads at `offset`.
fn consumer(tenant: u64, binding: u64, offset: i64) -> String {
    format!("trigger/{tenant}/{binding}/{offset}")
}

/// A transient failure: the worker tries again, never counted.
fn transient(e: &SprigError) -> RunReply {
    let mut r = RunReply::failed(e);
    r.transient = true;
    r
}

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// The trigger's event (`ctx.trigger`).
fn event_json(binding: u64, db: &str, table: &str, e: &Event) -> Value {
    let src = &e.envelope["source"];
    let key = format!(
        "u{}:{}:{}:{}",
        src["unit"].as_u64().unwrap_or(0),
        src["cts"].as_u64().unwrap_or(0),
        e.pos.index,
        e.pos.seq
    );
    json!({
        "binding": binding,
        "kind": "table",
        "op": op_name(e.op),
        "db": db,
        "table": table,
        "key": e.key,
        "before": e.before,
        "after": e.after,
        "row": if e.op == "d" { &e.before } else { &e.after },
        "eventKey": key,
        "ts_ms": e.ts_ms,
    })
}

/// A run's arguments, as test runs give them
/// ([`sprig_logic::testrun::trigger_args`]): `ctx.row` is the new row (the
/// old one for a delete), `ctx.old` the old row (`null` for an insert),
/// `ctx.trigger` the event. KIRun parameters are bound by name: a column
/// of `row`, `old_<column>` of `old`, `op`, `trigger`, `ctx`; JavaScript's
/// by the names in `signature` (also `row`, `old`, `ctx`), and a function
/// of `ctx` alone gets `ctx`; Lua gets `ctx`.
#[must_use]
pub(crate) fn args(
    runtime: &str,
    kirun_params: &[String],
    signature: &Value,
    event: &Value,
    ctx: &mut Value,
) -> Vec<Value> {
    let op = event["op"].as_str().unwrap_or("insert").to_owned();
    let row = if op == "delete" {
        event["before"].clone()
    } else {
        event["after"].clone()
    };
    let old = if op == "insert" {
        Value::Null
    } else {
        event["before"].clone()
    };
    ctx["row"] = row.clone();
    ctx["old"] = old.clone();
    let names: Vec<String> = if runtime == "javascript" {
        signature["params"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| p["name"].as_str().map(str::to_owned))
            .collect()
    } else {
        kirun_params.to_vec()
    };
    let js_names = runtime == "javascript" && names.iter().any(|p| p != "ctx");
    if runtime != "kirun" && !js_names {
        return vec![ctx.clone()];
    }
    names
        .iter()
        .map(|p| {
            let l = p.to_ascii_lowercase();
            match l.as_str() {
                "row" if js_names => row.clone(),
                "old" if js_names => old.clone(),
                "ctx" => ctx.clone(),
                "op" => Value::String(op.clone()),
                "trigger" => event.clone(),
                _ => match l.strip_prefix("old_") {
                    Some(c) if field(&row, &l).is_none() => field(&old, c).cloned(),
                    _ => field(&row, &l).cloned(),
                }
                .unwrap_or(Value::Null),
            }
        })
        .collect()
}

impl PassSession {
    /// `CREATE TRIGGER ... AFTER COMMIT ON t` on an engine database (see
    /// the module docs). Returns the client's notes.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_change_trigger(
        &mut self,
        decl: Declaration,
        db: &str,
    ) -> Result<Vec<String>> {
        let Some(log) = crate::changelog::changes() else {
            return Err(SprigError::new(
                SprigCode::NotOnEngine,
                "an after-commit trigger on an engine table needs the node's change log, which \
                 is not open (SPRIGDB_ENGINE_CHANGELOG)",
            ));
        };
        let Some(streams) = crate::kafka::engine::streams() else {
            return Err(SprigError::new(
                SprigCode::NotOnEngine,
                "an after-commit trigger on an engine table keeps its position in the engine's \
                 stream records, which this node doesn't serve",
            ));
        };
        if decl.tier == TierPref::Mariadb {
            return Err(SprigError::new(
                SprigCode::LogicTierUnavailable,
                format!(
                    "SPRIG_TIER = 'mariadb': database {db} is stored on the engine, where code \
                     runs on the engine tier"
                ),
            ));
        }
        let t = decl
            .table
            .clone()
            .ok_or_else(|| SprigError::internal("an after-commit trigger without its table"))?;
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(db, lctn);
        let row = catalog.database(&db_key).cloned().ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let user_name = ts.principal.user.clone();
        let tenant_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let o = catalog
            .object(
                row.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, &t.name, lctn),
            )
            .filter(|o| o.state == ObjectState::Active)
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{db}.{}' doesn't exist", t.name),
                )
            })?;
        let def = crate::engine::catalog::of(&catalog)
            .and_then(|e| e.tables.get(&o.object_id))
            .and_then(|p| p.as_ref().as_ref().ok())
            .cloned()
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::NotOnEngine,
                    format!("{db}.{} isn't an engine table", t.name),
                )
            })?;
        // The rights MySQL asks of a trigger reading OLD and NEW.
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        if !grants.unrestricted() {
            for (a, what) in [(Action::Trigger, "TRIGGER"), (Action::Select, "SELECT")] {
                let on = AuthzTarget::Object {
                    db_key: &db_key,
                    db: row.db_id,
                    object: o.object_id,
                };
                if !grants.allows(a, on) {
                    return Err(SprigError::standard(
                        1142,
                        *b"42000",
                        format!(
                            "{what} command denied to user '{user_name}' for table '{}'",
                            t.name
                        ),
                    ));
                }
            }
        }
        if sprig_auth::authz::rls::RlsState::of(&catalog)
            .and_then(|s| s.security(o.object_id))
            .is_some_and(|s| s.enabled)
        {
            return Err(SprigError::new(
                SprigCode::RlsUnsupported,
                format!(
                    "{}: table {} has row-level security, which after-commit triggers on engine \
                     tables don't apply yet",
                    SprigCode::RlsUnsupported.name(),
                    def.name
                ),
            ));
        }
        let object = o.object_id.0;
        let stream = STREAM_FLAG | object;
        let mut d = decl.clone();
        d.tier = TierPref::Engine;
        d.source = Some(MessageSource {
            kind: SourceKind::Stream,
            db: Some(row.name.clone()),
            name: format!("{}.changes", def.name),
            fan_in: false,
        });
        let columns: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let signature = messages::check_table(&d, &columns).await?;
        let events: Vec<String> = if decl.events.is_empty() {
            vec!["insert".into(), "update".into(), "delete".into()]
        } else {
            decl.events.clone()
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = svc.records.begin(tenant).await?;
        let deployed = messages::deploy_checked(
            &d,
            signature,
            messages::MsgTarget {
                catalog: &mut *conn,
                catalog_schema: ep.store.schema(),
                catalog_version: catalog.version,
                db_id: row.db_id.0,
                user_id: user.0,
                tenant_name: &tenant_name,
                source_db: (row.db_id.0, &row.name),
                object_id: stream,
                fanin_declarer: 0,
                compile: None,
            },
        )
        .await?;
        let store = LogicStore::default();
        if let Some(mut b) = store.binding_by_id(&mut *conn, deployed.binding_id).await? {
            b.events = events.join(",");
            b.when_fields.clone_from(&decl.when_fields);
            store.put_binding(&mut *conn, &b).await?;
        }
        conn.commit().await?;
        let group = messages::sprig_streams_group(deployed.binding_id);
        let have = streams
            .records()
            .offsets(tenant.0, &group, Some(&[(stream, 0)]))?;
        if deployed.created || have.is_empty() {
            let unit = sprig_engine::Engine::unit_of_tenant(tenant.0).0;
            let head = next_offset(log.head(unit).0);
            let o = sprig_proto_kafka::group::CommittedOffset {
                stream_id: stream,
                partition: 0,
                offset: head,
                leader_epoch: None,
                metadata: None,
            };
            streams
                .records()
                .commit_offsets(tenant.0, &group, &[o], sprig_streams::seg::now_ms())
                .await?;
        }
        lock(&FILTERS).remove(&(tenant.0, deployed.binding_id));
        messages::put_index(
            svc.locator.dc_store().as_ref(),
            &MsgIndex {
                tenant,
                binding: deployed.binding_id,
                kind: SourceKind::Stream,
            },
        )
        .await?;
        let mut notes = deployed.notes();
        notes.push(format!(
            "after-commit trigger on {}.{} ({}): runs for changes committed from now on, one at \
             a time in commit order",
            row.name,
            def.name,
            events.join(", ")
        ));
        tracing::info!(%tenant, binding = deployed.binding_id, table = %def.name, name = %decl.name, "after-commit trigger on an engine table deployed");
        Ok(notes)
    }

    /// A binding's events and `FOR UPDATE OF` columns (cached a few
    /// seconds).
    async fn change_filter(&mut self, tenant: u64, binding: u64) -> Result<Filter> {
        if let Some((at, f)) = lock(&FILTERS).get(&(tenant, binding))
            && at.elapsed() < FILTER_TTL
        {
            return Ok(f.clone());
        }
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let mut c = ts.services().records.begin(ts.tenant).await?;
        let store = LogicStore::default();
        let b = store.binding_by_id(&mut *c, binding).await?;
        let live = match &b {
            Some(b) => store
                .module_by_id(&mut *c, b.module_id)
                .await?
                .is_some_and(|m| m.state != "dropped"),
            None => false,
        };
        let _ = c.rollback().await;
        // A dropped trigger (its job ends within seconds) keeps no hold.
        let b = b.filter(|b| live && b.state == "active").ok_or_else(|| {
            drop_hold((tenant, binding));
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("binding {binding} is gone"),
            )
        })?;
        let f = Filter {
            events: b
                .events
                .split(',')
                .filter(|e| !e.is_empty())
                .map(str::to_owned)
                .collect(),
            when: b.when_fields,
        };
        lock(&FILTERS).insert((tenant, binding), (Instant::now(), f.clone()));
        Ok(f)
    }

    /// Moves the position from `from` to `to` in a transaction of its own
    /// (`false`: another run moved it).
    async fn change_skip(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        req: &RunRequest,
        from: i64,
        to: i64,
    ) -> Result<bool> {
        self.trigger_stmt(cx, &format!("USE {}", bq(db))).await?;
        self.trigger_stmt(cx, "START TRANSACTION").await?;
        match self.change_move(req, from, to).await {
            Ok(true) => {
                self.trigger_stmt(cx, "COMMIT").await?;
                Ok(true)
            }
            Ok(false) => {
                self.rollback_quietly(cx).await;
                Ok(false)
            }
            Err(e) => {
                self.rollback_quietly(cx).await;
                Err(e)
            }
        }
    }

    /// The position's compare-and-set in the session's engine transaction.
    async fn change_move(&mut self, req: &RunRequest, from: i64, to: i64) -> Result<bool> {
        let tenant = req.tenant;
        let Some(tx) = self.eng.txn.open_tx() else {
            return Err(SprigError::internal(
                "an after-commit trigger's run has no engine transaction",
            ));
        };
        sprig_streams::seg::records::move_offset(
            tx.engine_txn(),
            tenant,
            &messages::sprig_streams_group(req.source.binding_id),
            req.source.object_id,
            0,
            from,
            to,
            sprig_streams::seg::now_ms(),
        )
        .await
    }

    /// One run of an after-commit trigger on an engine table (see the
    /// module docs).
    #[allow(clippy::too_many_lines)]
    pub(super) async fn run_change_trigger(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
    ) -> RunReply {
        match self.run_change_inner(cx, req).await {
            Ok(r) => r,
            Err(e) => transient(&e),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_change_inner(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
    ) -> Result<RunReply> {
        let unavailable =
            |m: &str| SprigError::new(SprigCode::LogicEngineUnavailable, m.to_owned());
        let log = crate::changelog::changes()
            .ok_or_else(|| unavailable("the change log isn't open on this gateway"))?;
        let streams = crate::kafka::engine::streams().ok_or_else(|| {
            unavailable("the engine's stream records aren't served on this gateway")
        })?;
        let eng = crate::engine::engine()?;
        let (tenant, cat) = {
            let ts = self
                .tenant
                .as_deref_mut()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let cat = match ts.catalog() {
                Some(c) => Arc::clone(c),
                None => ts.refresh().await?,
            };
            (ts.tenant.0, cat)
        };
        let binding = req.source.binding_id;
        let bkey = (tenant, binding);
        let object = req.source.object_id & !STREAM_FLAG;
        let oid = sprig_common::ids::ObjectId(object);
        // A table that is gone: the worker's job ends when it sees it.
        let Some(def) = crate::engine::catalog::of(&cat)
            .and_then(|e| e.tables.get(&oid))
            .and_then(|pend| pend.as_ref().as_ref().ok())
            .cloned()
            .filter(|d| d.db.0 == req.source.db_id)
        else {
            drop_hold((tenant, binding));
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        let Some(db) = cat.databases.get(def.db).map(|d| d.name.clone()) else {
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        if sprig_auth::authz::rls::RlsState::of(&cat)
            .and_then(|s| s.security(oid))
            .is_some_and(|s| s.enabled)
        {
            return Ok(transient(&SprigError::new(
                SprigCode::RlsUnsupported,
                format!(
                    "table {} has row-level security, which after-commit triggers on engine \
                     tables don't apply yet: the trigger waits",
                    def.name
                ),
            )));
        }
        // The definer still reads the table.
        let due = lock(&CHECKED)
            .get(&bkey)
            .is_none_or(|at| at.elapsed() >= CHECK_EVERY);
        if due {
            let sql = format!("SELECT 1 FROM {}.{} LIMIT 0", bq(&db), bq(&def.name));
            if let Err(e) = self.run_checked(cx, &sql, false).await {
                return Ok(transient(&e));
            }
            lock(&CHECKED).insert(bkey, Instant::now());
        }
        let filter = self.change_filter(tenant, binding).await?;
        let object32 = u32::try_from(object)
            .map_err(|_| SprigError::internal(format!("table object {object}")))?;
        let group = messages::sprig_streams_group(binding);
        let stream = req.source.object_id;
        let unit = sprig_engine::Engine::unit_of_tenant(tenant).0;
        let committed = streams
            .records()
            .offsets(tenant, &group, Some(&[(stream, 0)]))?;
        let Some(cur) = committed.first().map(|o| o.offset) else {
            // Its deploy didn't get to write it: from the head.
            let head = next_offset(log.head(unit).0);
            self.change_skip(cx, &db, req, 0, head).await?;
            return Ok(RunReply::of(RunOutcome::Moved));
        };
        let start = next_offset(log.oldest(unit));
        if cur < start {
            lock(&PENDING).remove(&bkey);
            let moved = self.change_skip(cx, &db, req, cur, start).await?;
            let mut rep = RunReply::of(RunOutcome::Moved);
            if moved {
                tracing::warn!(
                    tenant,
                    binding,
                    from = cur,
                    to = start,
                    "after-commit trigger: changes expired from the change log before they ran"
                );
                rep.message = format!(
                    "{}: changes from offset {cur} to {start} left the change log before the \
                     trigger ran them; it goes on from the oldest change kept",
                    SprigCode::FeedHistoryLost.name()
                );
            }
            return Ok(rep);
        }
        // The changes after `cur`: kept from the last run, or read now.
        let kept = lock(&PENDING).remove(&bkey).filter(|pend| pend.from == cur);
        let mut pend = if let Some(pend) = kept {
            pend
        } else {
            let tables = HashMap::from([(
                object32,
                Watched {
                    def: Arc::clone(&def),
                    db: db.clone(),
                },
            )]);
            let key = consumer(tenant, binding, cur);
            let b = crate::table_changes::read_from(
                eng,
                &log,
                tenant,
                &tables,
                pos_before(cur),
                1,
                0,
                PAGE_BYTES,
                WAIT,
                &key,
            )
            .await?;
            crate::table_changes::rekey(&key, &consumer(tenant, binding, next_offset(b.pos)));
            if let Some(tk) = sprig_streams::changes::Token::from_hex(&b.next) {
                keep_hold(bkey, tk.cts);
            }
            Pending {
                from: cur,
                events: b.events.into(),
                end: b.pos,
                at_end: b.at_end,
            }
        };
        // The first change the binding wants.
        let e = loop {
            match pend.events.pop_front() {
                None => break None,
                Some(e) if offset_of(e.pos) < cur => {}
                Some(e) if filter.wants(&e) => break Some(e),
                Some(_) => {}
            }
        };
        let Some(e) = e else {
            let to = next_offset(pend.end);
            if to > cur {
                self.change_skip(cx, &db, req, cur, to).await?;
            }
            return Ok(RunReply::of(if pend.at_end {
                RunOutcome::Empty
            } else {
                RunOutcome::Moved
            }));
        };
        let at = offset_of(e.pos);
        let next = next_offset(e.pos);
        let event = event_json(binding, &db, &def.name, &e);
        let event_key = event["eventKey"].as_str().unwrap_or_default().to_owned();
        let attempts = match lock(&FAILS).get(&bkey) {
            Some((o, n)) if *o == at => *n,
            _ => 0,
        };
        let with_id = |mut rep: RunReply| {
            rep.message_id = Some(event_key.clone());
            rep.attempts = attempts;
            rep
        };
        // Kept for the next run, which starts after `e` (or at it again).
        let keep = |pend: Pending, e: Option<Event>, from: i64| {
            let mut pend = pend;
            if let Some(e) = e {
                pend.events.push_front(e);
            }
            pend.from = from;
            lock(&PENDING).insert(bkey, pend);
        };
        let failure = 'run: {
            if let Err(e) = self.trigger_stmt(cx, &format!("USE {}", bq(&db))).await {
                break 'run e;
            }
            if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
                break 'run e;
            }
            match self.change_move(req, cur, next).await {
                Ok(true) => {}
                Ok(false) => {
                    self.rollback_quietly(cx).await;
                    return Ok(with_id(RunReply::of(RunOutcome::Moved)));
                }
                Err(err) => {
                    self.rollback_quietly(cx).await;
                    break 'run err;
                }
            }
            // In a rollout, the row's key picks the version.
            let row_key = e.key.to_string();
            let (picked, counted) = self.pick_version(req, &json!({"key": row_key}), None).await;
            let started = Instant::now();
            let (ran, logs) = self
                .run_version(cx, &picked, "trigger", event.clone())
                .await;
            let outcome = RunReply::of(if ran.is_ok() {
                RunOutcome::Ok
            } else {
                RunOutcome::Failed
            });
            self.count_run(cx, counted, &picked, started, &outcome)
                .await;
            match ran {
                Ok(()) => match self.trigger_stmt(cx, "COMMIT").await {
                    Ok(()) => {
                        lock(&FAILS).remove(&bkey);
                        keep(pend, None, next);
                        return Ok(with_id(RunReply {
                            logs,
                            ..RunReply::of(RunOutcome::Ok)
                        }));
                    }
                    Err(err) => {
                        self.rollback_quietly(cx).await;
                        break 'run err;
                    }
                },
                Err(err) => {
                    self.rollback_quietly(cx).await;
                    let mut rep = with_id(RunReply {
                        logs,
                        ..RunReply::failed(&err)
                    });
                    if rep.transient {
                        keep(pend, Some(e), cur);
                        return Ok(rep);
                    }
                    if attempts.saturating_add(1) < req.max_attempts.max(1) {
                        lock(&FAILS).insert(bkey, (at, attempts + 1));
                        keep(pend, Some(e), cur);
                        return Ok(rep);
                    }
                    // The last attempt: a dead letter, then past it.
                    if let Err(werr) = self
                        .change_dead_letter(req, &event_key, at, &event, &rep)
                        .await
                    {
                        tracing::warn!(error = %werr, binding, "trigger dead letter not written");
                        keep(pend, Some(e), cur);
                        return Ok(rep);
                    }
                    lock(&FAILS).remove(&bkey);
                    match self.change_skip(cx, &db, req, cur, next).await {
                        Ok(_) => keep(pend, None, next),
                        Err(werr) => {
                            tracing::warn!(error = %werr, binding, "trigger position not moved past a dead letter");
                        }
                    }
                    rep.outcome = RunOutcome::Dead;
                    return Ok(rep);
                }
            }
        };
        keep(pend, Some(e), cur);
        Ok(with_id(transient(&failure)))
    }

    /// Writes a dead letter of a change and its run log line.
    async fn change_dead_letter(
        &mut self,
        req: &RunRequest,
        event_key: &str,
        offset: i64,
        event: &Value,
        reply: &RunReply,
    ) -> Result<()> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let mut conn = ts.services().records.begin(ts.tenant).await?;
        let store = LogicStore::default();
        store
            .put_dead_letter(
                &mut *conn,
                req.source.binding_id,
                req.version,
                &format!("c{event_key}"),
                offset.to_string().as_bytes(),
                event,
                reply.attempts.saturating_add(1),
                reply.code,
                &reply.message,
            )
            .await?;
        let _ = store
            .log(
                &mut *conn,
                req.source.module_id,
                Some(req.version),
                "error",
                "trigger",
                &format!(
                    "{}.{} change {event_key}: dead letter after {} attempts: {} {}",
                    req.source.db_name,
                    req.source.name,
                    reply.attempts.saturating_add(1),
                    reply.code,
                    reply.message
                ),
            )
            .await;
        conn.commit().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(op: &'static str, before: Option<Value>, after: Option<Value>) -> Event {
        Event {
            token: String::new(),
            object: 7,
            op,
            key: json!({"id": 1}),
            before,
            after,
            ts_ms: 5,
            envelope: json!({"source": {"unit": 3, "cts": 99}}),
            pos: Pos { index: 4, seq: 2 },
        }
    }

    #[test]
    fn filters_by_op_and_changed_columns() {
        let f = Filter {
            events: vec!["update".into()],
            when: vec!["Status".into()],
        };
        let same = ev(
            "u",
            Some(json!({"id": 1, "status": "a", "n": 1})),
            Some(json!({"id": 1, "status": "a", "n": 2})),
        );
        let changed = ev(
            "u",
            Some(json!({"id": 1, "status": "a"})),
            Some(json!({"id": 1, "status": "b"})),
        );
        assert!(!f.wants(&same));
        assert!(f.wants(&changed));
        assert!(!f.wants(&ev("c", None, Some(json!({"id": 1})))));
        // No before-image: it may have changed.
        assert!(f.wants(&ev("u", None, Some(json!({"id": 1})))));
    }

    #[test]
    fn arguments_by_name() {
        let e = ev(
            "u",
            Some(json!({"id": 1, "Status": "a"})),
            Some(json!({"id": 1, "Status": "b"})),
        );
        let event = event_json(9, "d", "t", &e);
        assert_eq!(event["eventKey"], "u3:99:4:2");
        assert_eq!(event["op"], "update");
        let mut ctx = json!({"trigger": event.clone()});
        let params: Vec<String> = ["status", "old_status", "op", "missing", "trigger"]
            .map(str::to_owned)
            .to_vec();
        let a = args("kirun", &params, &Value::Null, &event, &mut ctx);
        assert_eq!(a[0], "b");
        assert_eq!(a[1], "a");
        assert_eq!(a[2], "update");
        assert_eq!(a[3], Value::Null);
        assert_eq!(a[4], event);
        assert_eq!(ctx["row"]["Status"], "b");
        assert_eq!(ctx["old"]["Status"], "a");
        // A delete's row is the old row.
        let d = event_json(
            9,
            "d",
            "t",
            &ev("d", Some(json!({"id": 1, "Status": "a"})), None),
        );
        let mut c2 = json!({});
        assert_eq!(
            args("kirun", &["status".into()], &Value::Null, &d, &mut c2)[0],
            "a"
        );
        // JavaScript by the default export's names; `ctx` alone gets ctx.
        let sig = json!({"params": [{"name": "row"}, {"name": "old"}, {"name": "op"}]});
        let js = args("javascript", &[], &sig, &event, &mut ctx);
        assert_eq!(
            js,
            vec![ctx["row"].clone(), ctx["old"].clone(), json!("update")]
        );
        let only = json!({"params": [{"name": "ctx"}]});
        assert_eq!(
            args("javascript", &[], &only, &event, &mut ctx),
            vec![ctx.clone()]
        );
        assert_eq!(args("lua", &[], &only, &event, &mut ctx), vec![ctx.clone()]);
    }

    #[test]
    fn change_sources_are_marked() {
        let mut s = SourceRow {
            binding_id: 1,
            module_id: 2,
            kind: SourceKind::Stream,
            db_id: 1,
            db_name: "d".into(),
            name: "t.changes".into(),
            object_id: STREAM_FLAG | 5,
            definer_user_id: 1,
            tenant_name: "x".into(),
            fanin_declarer: 0,
        };
        assert!(is_change_source(&s));
        s.object_id = 5;
        assert!(!is_change_source(&s));
    }
}
