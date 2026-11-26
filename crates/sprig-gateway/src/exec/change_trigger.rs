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
//! On a table with row-level security each change is taken as the
//! definer's `SELECT` policies show it before the binding's events are
//! matched: a row the definer can't see is passed over, an update whose row
//! became visible runs as an insert, one whose row became hidden as a
//! delete (as on MariaDB tables).
//!
//! The position commits with the code's writes, so they happen exactly
//! once; outside effects are at least once, with `ctx.trigger.eventKey`
//! (`u<unit>:<cts>:<entry>:<record>`) as their idempotency key. A position
//! older than the change log's retention moves to the oldest change kept,
//! with a warning (the changes in between are lost to the trigger).
//!
//! **Key lanes.** The worker runs `logic.trigger_lanes` loops, each asking
//! for its lane's next change. A change belongs to lane `hash(row key) %
//! lanes`, and each lane has a position of its own, so one row key's
//! changes run in commit order and different keys' run in parallel. The
//! lanes' layout is kept in the trigger's group next to their positions:
//!
//! ```text
//! partition g << 11 | j  lane j of layout generation g (generation 0 is
//!                        one lane, partition 0: a trigger made before lanes)
//! row -1                 the layout: g << 11 | lanes
//! row -2                 the offset the layout's lanes started at (its barrier)
//! row -3                 the layout before it while its lanes still run up
//!                        to the barrier (0: none)
//! ```
//!
//! A run asking with another lane count changes the layout in one
//! transaction: it locks every lane's position, takes the furthest as the
//! barrier, starts the new layout's lanes there and keeps the old layout
//! as draining. The old lanes then run only their changes before the
//! barrier, and the new lanes wait until every old lane has reached it,
//! so no change runs twice or out of its key's order. Each run checks in
//! its own transaction that the layout it ran by is still the current one
//! (else it rolls back and asks again).

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

/// Lane `j` of layout generation `g` keeps its position as partition
/// `g << LANE_BITS | j` of the trigger's group.
const LANE_BITS: u32 = 11;

/// The most key lanes a trigger runs (`logic.trigger_lanes`' maximum).
const MAX_LANES: u32 = 1 << (LANE_BITS - 1);

/// The group's rows that describe the lanes (no partition is negative):
/// the layout, the offset its lanes started at, the layout before it
/// while its lanes still run up to that offset (0: none).
const LAYOUT: i32 = -1;
const BARRIER: i32 = -2;
const DRAINING: i32 = -3;

/// A lane layout: its generation and lane count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Layout {
    generation: i64,
    lanes: u32,
}

impl Layout {
    /// From its row (0, no row: generation 0, one lane).
    fn of(row: i64) -> Self {
        if row <= 0 {
            return Self {
                generation: 0,
                lanes: 1,
            };
        }
        Self {
            generation: row >> LANE_BITS,
            lanes: u32::try_from(row & ((1 << LANE_BITS) - 1))
                .unwrap_or(1)
                .max(1),
        }
    }

    fn row(self) -> i64 {
        (self.generation << LANE_BITS) | i64::from(self.lanes)
    }

    /// Lane `lane`'s position partition (`None`: generations ran out).
    fn partition(self, lane: u32) -> Option<i32> {
        i32::try_from((self.generation << LANE_BITS) | i64::from(lane)).ok()
    }
}

/// A trigger's lanes as its group's rows say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Lanes {
    /// Rows [`LAYOUT`], [`BARRIER`] and [`DRAINING`] as read (a run reads
    /// them again in its transaction).
    rows: [i64; 3],
    /// Each lane's position, by partition.
    at: HashMap<i32, i64>,
}

impl Lanes {
    /// The group's rows of change topic `stream`.
    fn read(
        records: &sprig_streams::seg::records::StreamRecords,
        tenant: u64,
        group: &str,
        stream: u64,
    ) -> Result<Self> {
        let mut s = Self::default();
        for o in records.offsets(tenant, group, None)? {
            if o.stream_id != stream {
                continue;
            }
            match o.partition {
                LAYOUT => s.rows[0] = o.offset,
                BARRIER => s.rows[1] = o.offset,
                DRAINING => s.rows[2] = o.offset,
                p => {
                    s.at.insert(p, o.offset);
                }
            }
        }
        Ok(s)
    }

    fn layout(&self) -> Layout {
        Layout::of(self.rows[0])
    }

    fn barrier(&self) -> i64 {
        self.rows[1]
    }

    fn draining(&self) -> Option<Layout> {
        (self.rows[2] != 0).then(|| Layout::of(self.rows[2]))
    }

    /// Lane `lane` of `layout`'s position (a lane with no row: the
    /// barrier, where a layout change starts its lanes).
    fn position(&self, layout: Layout, lane: u32) -> i64 {
        layout
            .partition(lane)
            .and_then(|p| self.at.get(&p).copied())
            .unwrap_or_else(|| self.barrier())
    }
}

/// The lane of row key `key` among `lanes`: FNV-1a of its JSON text, the
/// same on every node and build.
fn lane_of(key: &Value, lanes: u32) -> u32 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.to_string().bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    u32::try_from(h % u64::from(lanes.max(1))).unwrap_or(0)
}

/// What a lane's run reads by (the parts of a run that every lane shares).
struct LaneRun {
    db: String,
    def: Arc<crate::engine::table::TableDef>,
    object32: u32,
    secured: bool,
    filter: Filter,
    unit: u64,
}

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

/// Changes read past a lane's position and not run yet, by tenant,
/// binding and lane partition: the lane's next run starting at `from`
/// takes them instead of reading the log again.
struct Pending {
    from: i64,
    events: VecDeque<Event>,
    end: Pos,
    at_end: bool,
}

/// A binding: tenant and binding id.
type Binding = (u64, u64);

/// A lane: tenant, binding id and the lane's partition.
type Key = (u64, u64, i32);

static PENDING: LazyLock<Mutex<HashMap<Key, Pending>>> = LazyLock::new(Mutex::default);
/// Failed attempts of a lane's event at an offset (kept by the gateway
/// the binding's runs go to; another gateway counts anew).
static FAILS: LazyLock<Mutex<HashMap<Key, (i64, u32)>>> = LazyLock::new(Mutex::default);
static FILTERS: LazyLock<Mutex<HashMap<Binding, (Instant, Filter)>>> =
    LazyLock::new(Mutex::default);
static CHECKED: LazyLock<Mutex<HashMap<Binding, Instant>>> = LazyLock::new(Mutex::default);

/// The name of lane `k`'s hold (partition 0 keeps the name a trigger's
/// single position had).
fn hold_name(k: Key) -> String {
    if k.2 == 0 {
        format!("trigger/{}/{}", k.0, k.1)
    } else {
        format!("trigger/{}/{}/{}", k.0, k.1, k.2)
    }
}

/// The binding and lane partition of hold `name` of `tenant`.
fn held_lane(tenant: u64, name: &str) -> Option<(u64, i32)> {
    let rest = name.strip_prefix(&format!("trigger/{tenant}/"))?;
    match rest.split_once('/') {
        Some((b, p)) => Some((b.parse().ok()?, p.parse().ok()?)),
        None => Some((rest.parse().ok()?, 0)),
    }
}

/// The change log's durable version hold of a lane
/// ([`sprig_streams::changes::ChangeLog::hold_for`]), at the end of what
/// its runs read last: the before-images of the changes after it outlive a
/// long wait (a failing change's retries) and a restart of the node, where
/// the engine keeps versions only a minute otherwise.
fn keep_hold(k: Key, cts: u64) {
    if let Some(log) = crate::changelog::changes() {
        log.hold_for(&hold_name(k), Some(cts));
    }
}

/// Forgets the holds of binding `b`'s lanes whose partition `which` takes
/// (its table is gone, or a layout's lanes are done).
fn drop_holds(b: Binding, which: impl Fn(i32) -> bool) {
    if let Some(log) = crate::changelog::changes() {
        for n in log.held() {
            if let Some((binding, p)) = held_lane(b.0, &n)
                && binding == b.1
                && which(p)
            {
                log.hold_for(&n, None);
            }
        }
    }
    lock(&PENDING).retain(|k, _| !(k.0 == b.0 && k.1 == b.1 && which(k.2)));
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
    let mut held: Vec<u64> = log
        .held()
        .iter()
        .filter_map(|n| held_lane(tenant.0, n).map(|(b, _)| b))
        .collect();
    held.sort_unstable();
    held.dedup();
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
            drop_holds((tenant.0, b), |_| true);
        }
    }
    let _ = c.rollback().await;
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The reader key of lane `k`'s reads at `offset`.
fn consumer(k: Key, offset: i64) -> String {
    format!("trigger/{}/{}/{}/{offset}", k.0, k.1, k.2)
}

/// A transient failure: the worker tries again, never counted.
fn transient(e: &SprigError) -> RunReply {
    let mut r = RunReply::failed(e);
    r.transient = true;
    r
}

/// A run's statement found no engine transaction open.
fn no_txn() -> SprigError {
    SprigError::internal("an after-commit trigger's run has no engine transaction")
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
            "after-commit trigger on {}.{} ({}): runs for changes committed from now on, each \
             row key's in commit order (logic.trigger_lanes key lanes)",
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
            drop_holds((tenant, binding), |_| true);
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

    /// Moves lane partition `part`'s position from `from` to `to` in a
    /// transaction of its own, while the lane rows are as `lanes` read them
    /// (`false`: another run moved it, or the layout changed).
    #[allow(clippy::too_many_arguments)]
    async fn change_skip(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        req: &RunRequest,
        lanes: &Lanes,
        part: i32,
        from: i64,
        to: i64,
    ) -> Result<bool> {
        self.trigger_stmt(cx, &format!("USE {}", bq(db))).await?;
        self.trigger_stmt(cx, "START TRANSACTION").await?;
        let moved = match self.change_move(req, part, from, to).await {
            Ok(true) => self.lanes_unchanged(req, lanes),
            other => other,
        };
        match moved {
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

    /// Row `part`'s compare-and-set (a lane's position, or a lane row) in
    /// the session's engine transaction; it locks the row.
    async fn change_move(
        &mut self,
        req: &RunRequest,
        part: i32,
        from: i64,
        to: i64,
    ) -> Result<bool> {
        let tenant = req.tenant;
        let Some(tx) = self.eng.txn.open_tx() else {
            return Err(no_txn());
        };
        sprig_streams::seg::records::move_offset(
            tx.engine_txn(),
            tenant,
            &messages::sprig_streams_group(req.source.binding_id),
            req.source.object_id,
            part,
            from,
            to,
            sprig_streams::seg::now_ms(),
        )
        .await
    }

    /// Whether the lane rows are still as `lanes` read them, read in the
    /// session's engine transaction after it locked its lane's position. A
    /// layout change locks every lane's position before it commits, so
    /// either it committed first (and is seen here) or it waits for this
    /// transaction.
    fn lanes_unchanged(&mut self, req: &RunRequest, lanes: &Lanes) -> Result<bool> {
        let Some(tx) = self.eng.txn.open_tx() else {
            return Err(no_txn());
        };
        let group = messages::sprig_streams_group(req.source.binding_id);
        for (i, row) in [LAYOUT, BARRIER, DRAINING].into_iter().enumerate() {
            let v = sprig_streams::seg::records::read_offset(
                tx.engine_txn(),
                req.tenant,
                &group,
                req.source.object_id,
                row,
            )?;
            if v != lanes.rows[i] {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Changes the trigger's layout to `n` lanes (module docs). `Moved`
    /// either way: the next run reads the layout again.
    async fn lanes_change(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        req: &RunRequest,
        lanes: &Lanes,
        n: u32,
    ) -> Result<RunReply> {
        let old = lanes.layout();
        let new = Layout {
            generation: old.generation + 1,
            lanes: n,
        };
        let parts: Option<Vec<i32>> = (0..n).map(|j| new.partition(j)).collect();
        let (Some(parts), Some(olds)) = (
            parts,
            (0..old.lanes)
                .map(|j| old.partition(j))
                .collect::<Option<Vec<i32>>>(),
        ) else {
            return Err(SprigError::internal(
                "an after-commit trigger's key lanes changed too many times",
            ));
        };
        self.trigger_stmt(cx, &format!("USE {}", bq(db))).await?;
        self.trigger_stmt(cx, "START TRANSACTION").await?;
        let mut barrier = lanes.barrier();
        let done = async {
            // The lane rows first: a concurrent change finds them moved.
            if !self
                .change_move(req, LAYOUT, lanes.rows[0], new.row())
                .await?
                || !self
                    .change_move(req, DRAINING, lanes.rows[2], old.row())
                    .await?
            {
                return Ok(false);
            }
            // Every old lane's position, locked: a run holding one commits
            // first, and one after this sees the new layout.
            for p in &olds {
                let have = lanes.at.get(p).copied();
                let at = have.unwrap_or(0);
                if !self.change_move(req, *p, at, at).await? {
                    return Ok(false);
                }
                barrier = barrier.max(have.unwrap_or(barrier));
            }
            if !self
                .change_move(req, BARRIER, lanes.barrier(), barrier)
                .await?
            {
                return Ok(false);
            }
            for p in &parts {
                if !self.change_move(req, *p, 0, barrier).await? {
                    return Ok(false);
                }
            }
            Ok::<bool, SprigError>(true)
        }
        .await;
        match done {
            Ok(true) => {
                self.trigger_stmt(cx, "COMMIT").await?;
                tracing::info!(
                    tenant = req.tenant,
                    binding = req.source.binding_id,
                    from = old.lanes,
                    to = n,
                    barrier,
                    "after-commit trigger: key lanes changed"
                );
                let mut rep = RunReply::of(RunOutcome::Moved);
                rep.message = format!(
                    "key lanes {} -> {n}: the new lanes start at offset {barrier}, once the old \
                     ones have run every change before it",
                    old.lanes
                );
                Ok(rep)
            }
            Ok(false) => {
                self.rollback_quietly(cx).await;
                Ok(RunReply::of(RunOutcome::Moved))
            }
            Err(e) => {
                self.rollback_quietly(cx).await;
                Err(e)
            }
        }
    }

    /// Ends layout `old`'s draining: every one of its lanes has reached
    /// the barrier.
    async fn lanes_drained(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        req: &RunRequest,
        lanes: &Lanes,
        old: Layout,
    ) -> Result<()> {
        self.trigger_stmt(cx, &format!("USE {}", bq(db))).await?;
        self.trigger_stmt(cx, "START TRANSACTION").await?;
        let done = async {
            Ok::<bool, SprigError>(
                self.change_move(req, LAYOUT, lanes.rows[0], lanes.rows[0])
                    .await?
                    && self.change_move(req, DRAINING, lanes.rows[2], 0).await?,
            )
        }
        .await;
        match done {
            Ok(true) => {
                self.trigger_stmt(cx, "COMMIT").await?;
                drop_holds((req.tenant, req.source.binding_id), |p| {
                    p >= 0 && i64::from(p) >> LANE_BITS == old.generation
                });
                Ok(())
            }
            Ok(false) => {
                self.rollback_quietly(cx).await;
                Ok(())
            }
            Err(e) => {
                self.rollback_quietly(cx).await;
                Err(e)
            }
        }
    }

    /// One run of an after-commit trigger on an engine table (see the
    /// module docs).
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
            drop_holds(bkey, |_| true);
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        let Some(db) = cat.databases.get(def.db).map(|d| d.name.clone()) else {
            return Ok(RunReply::of(RunOutcome::Empty));
        };
        // Row-level security: each change as the definer's policies show
        // it (`as_seen`).
        let secured = sprig_auth::authz::rls::RlsState::of(&cat)
            .and_then(|s| s.security(oid))
            .is_some_and(|s| s.enabled);
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
        let unit = sprig_engine::Engine::unit_of_tenant(tenant).0;
        let lanes = Lanes::read(streams.records(), tenant, &group, req.source.object_id)?;
        let layout = lanes.layout();
        if layout.generation == 0 && !lanes.at.contains_key(&0) {
            // Its deploy didn't get to write it: from the head.
            let head = next_offset(log.head(unit).0);
            self.change_skip(cx, &db, req, &lanes, 0, 0, head).await?;
            return Ok(RunReply::of(RunOutcome::Moved));
        }
        let n = req.lanes.clamp(1, MAX_LANES);
        let lane = req.lane.min(n - 1);
        let run = LaneRun {
            db,
            def,
            object32,
            secured,
            filter,
            unit,
        };
        if let Some(old) = lanes.draining() {
            let barrier = lanes.barrier();
            let behind: Vec<(u32, i64)> = (0..old.lanes)
                .map(|j| (j, lanes.position(old, j)))
                .filter(|(_, at)| *at < barrier)
                .collect();
            if behind.is_empty() {
                self.lanes_drained(cx, &run.db, req, &lanes, old).await?;
                return Ok(RunReply::of(RunOutcome::Moved));
            }
            // This loop runs the old lanes `j % n == lane` up to the
            // barrier; a loop with none waits for the others.
            let Some(&(j, at)) = behind
                .iter()
                .filter(|(j, _)| j % n == lane)
                .min_by_key(|(_, at)| *at)
            else {
                return Ok(RunReply::of(RunOutcome::Empty));
            };
            return self
                .run_lane(cx, req, &run, &lanes, (old, j), at, Some(barrier))
                .await;
        }
        if layout.lanes != n {
            return self.lanes_change(cx, &run.db, req, &lanes, n).await;
        }
        let at = lanes.position(layout, lane);
        self.run_lane(cx, req, &run, &lanes, (layout, lane), at, None)
            .await
    }

    /// One run of lane `lane.1` of layout `lane.0`, whose position is
    /// `cur`: its next change before `limit` (a draining layout's
    /// barrier).
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn run_lane(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        run: &LaneRun,
        lanes: &Lanes,
        lane: (Layout, u32),
        cur: i64,
        limit: Option<i64>,
    ) -> Result<RunReply> {
        let log = crate::changelog::changes().ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicEngineUnavailable,
                "the change log isn't open on this gateway",
            )
        })?;
        let eng = crate::engine::engine()?;
        let (layout, j) = lane;
        let tenant = req.tenant;
        let binding = req.source.binding_id;
        let part = layout
            .partition(j)
            .ok_or_else(|| SprigError::internal("an after-commit trigger's lane partition"))?;
        let bkey: Key = (tenant, binding, part);
        let db = run.db.as_str();
        let start = next_offset(log.oldest(run.unit));
        if cur < start {
            lock(&PENDING).remove(&bkey);
            let moved = self
                .change_skip(cx, db, req, lanes, part, cur, start)
                .await?;
            let mut rep = RunReply::of(RunOutcome::Moved);
            if moved {
                tracing::warn!(
                    tenant,
                    binding,
                    lane = j,
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
        // The changes after `cur`: kept from the lane's last run, or read
        // now.
        let kept = lock(&PENDING).remove(&bkey).filter(|pend| pend.from == cur);
        let mut pend = if let Some(pend) = kept {
            pend
        } else {
            let tables = HashMap::from([(
                run.object32,
                Watched {
                    def: Arc::clone(&run.def),
                    db: run.db.clone(),
                },
            )]);
            let key = consumer(bkey, cur);
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
            crate::table_changes::rekey(&key, &consumer(bkey, next_offset(b.pos)));
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
        // The lane's first change before `limit` the binding wants, as the
        // definer sees it.
        let mut reached = false;
        let e = loop {
            match pend.events.pop_front() {
                None => break None,
                Some(e) if offset_of(e.pos) < cur => {}
                Some(e) if limit.is_some_and(|l| offset_of(e.pos) >= l) => {
                    reached = true;
                    break None;
                }
                Some(e) if lane_of(&e.key, layout.lanes) != j => {}
                Some(e) => {
                    let e = if run.secured {
                        match self.as_seen(cx, db, &run.def.name, e).await {
                            Ok(Some(e)) => e,
                            Ok(None) => continue,
                            Err(err) => {
                                // Can't tell: the trigger waits (retried).
                                return Ok(transient(&err));
                            }
                        }
                    } else {
                        e
                    };
                    if run.filter.wants(&e) {
                        break Some(e);
                    }
                }
            }
        };
        let Some(e) = e else {
            let to = limit.map_or(next_offset(pend.end), |l| next_offset(pend.end).min(l));
            if to > cur {
                self.change_skip(cx, db, req, lanes, part, cur, to).await?;
            }
            // A lane that reached its barrier asks again at once.
            let waiting = pend.at_end && !reached && limit.is_none_or(|l| to < l);
            return Ok(RunReply::of(if waiting {
                RunOutcome::Empty
            } else {
                RunOutcome::Moved
            }));
        };
        let at = offset_of(e.pos);
        let next = next_offset(e.pos);
        let event = event_json(binding, db, &run.def.name, &e);
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
        // Kept for the lane's next run, which starts after `e` (or at it
        // again).
        let keep = |pend: Pending, e: Option<Event>, from: i64| {
            let mut pend = pend;
            if let Some(e) = e {
                pend.events.push_front(e);
            }
            pend.from = from;
            lock(&PENDING).insert(bkey, pend);
        };
        let failure = 'run: {
            if let Err(e) = self.trigger_stmt(cx, &format!("USE {}", bq(db))).await {
                break 'run e;
            }
            if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
                break 'run e;
            }
            // The lane's position, then the layout it ran by (module docs).
            let moved = match self.change_move(req, part, cur, next).await {
                Ok(true) => self.lanes_unchanged(req, lanes),
                other => other,
            };
            match moved {
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
                    match self.change_skip(cx, db, req, lanes, part, cur, next).await {
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

impl PassSession {
    /// Change `e` of table `db`.`table` (whose row-level security is
    /// enabled) as the definer's `SELECT` policies show it, as on MariaDB
    /// tables: an insert or delete whose row the definer can't see is
    /// passed over, an update whose row became visible runs as an insert,
    /// one whose row became hidden as a delete. `None`: passed over.
    ///
    /// # Errors
    /// The policies' check.
    async fn as_seen(
        &mut self,
        cx: &mut ExecCtx<'_>,
        db: &str,
        table: &str,
        mut e: Event,
    ) -> Result<Option<Event>> {
        let obj = |v: &Option<Value>| match v {
            Some(Value::Object(m)) => Some(m.clone()),
            _ => None,
        };
        // A delete whose before-image the version GC dropped: its key
        // stands for the row.
        let before =
            obj(&e.before).or_else(|| (e.op == "d").then(|| obj(&Some(e.key.clone()))).flatten());
        let req = super::rls_images::ImagesRequest {
            tables: vec![super::rls_images::TableImages {
                db: db.to_owned(),
                table: table.to_owned(),
                images: vec![before.clone(), obj(&e.after)],
            }],
            plain_binary: false,
            watch: false,
        };
        let seen = self.check_images(cx, &req).await?;
        let shown = |i: usize| {
            seen.iter()
                .find(|(t, j, _)| *t == 0 && *j == i)
                .is_some_and(|(_, _, v)| *v)
        };
        let (b, a) = (before.is_some() && shown(0), e.after.is_some() && shown(1));
        let op = match (e.op, b, a) {
            ("c", _, true) | ("d", true, _) | ("u", true, true) => e.op,
            ("u", true, false) => "d",
            ("u", false, true) => "c",
            _ => {
                metrics::counter!("sprig_rls_feed_events_filtered_total", "protocol" => "trigger")
                    .increment(1);
                return Ok(None);
            }
        };
        if op != e.op {
            e.op = op;
            if op == "c" {
                e.before = None;
            } else {
                e.after = None;
            }
            e.envelope["op"] = Value::String(op.to_owned());
            e.envelope["before"] = e.before.clone().unwrap_or(Value::Null);
            e.envelope["after"] = e.after.clone().unwrap_or(Value::Null);
        }
        Ok(Some(e))
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
    fn lane_layouts_keys_and_holds() {
        let one = Layout {
            generation: 0,
            lanes: 1,
        };
        assert_eq!(Layout::of(0), one, "no row: one lane");
        assert_eq!(one.partition(0), Some(0), "a trigger's single position");
        let l = Layout {
            generation: 3,
            lanes: 8,
        };
        assert_eq!(Layout::of(l.row()), l);
        assert_eq!(l.partition(5), Some((3 << LANE_BITS) | 5));
        let last = Layout {
            generation: 1 << 21,
            lanes: 1,
        };
        assert_eq!(last.partition(0), None);
        // The same lane for a key on every node and build.
        assert_eq!(lane_of(&json!({"id": 1}), 1024), 155);
        assert_eq!(lane_of(&json!({"id": 1}), 8), 3);
        assert_eq!(lane_of(&json!({"id": 1}), 1), 0);
        let mut per = [0u32; 8];
        for i in 0..800 {
            per[lane_of(&json!({ "id": i }), 8) as usize] += 1;
        }
        assert!(per.iter().all(|n| *n > 50), "{per:?}");
        assert_eq!(hold_name((7, 12, 0)), "trigger/7/12");
        assert_eq!(hold_name((7, 12, 2049)), "trigger/7/12/2049");
        assert_eq!(held_lane(7, "trigger/7/12"), Some((12, 0)));
        assert_eq!(held_lane(7, "trigger/7/12/2049"), Some((12, 2049)));
        assert_eq!(held_lane(8, "trigger/7/12"), None);
        let mut s = Lanes {
            rows: [
                Layout {
                    generation: 1,
                    lanes: 2,
                }
                .row(),
                500,
                one.row(),
            ],
            ..Lanes::default()
        };
        s.at.insert(0, 300);
        assert_eq!(s.draining(), Some(one));
        assert_eq!(s.position(one, 0), 300);
        assert_eq!(s.position(s.layout(), 1), 500, "a lane with no row");
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
