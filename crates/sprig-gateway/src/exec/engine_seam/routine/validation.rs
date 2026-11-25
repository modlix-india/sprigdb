//! Validations on engine tables: `CREATE [OR REPLACE] VALIDATION v ON t
//! FOR INSERT, UPDATE LANGUAGE {javascript | kirun | lua} AS ...` on an
//! engine database, checked in the write path before each row is written.
//!
//! ```text
//! deploy  TRIGGER on t; the source checked by its runtime (QuickJS with the
//!         row's names, Lua with ctx, KIRun's definition: its parameters
//!         name columns, old_<column> or op); the module, a version (engine
//!         tier), the binding (before_write, enforcement step, its events)
//!         and the activation in one record transaction; nothing on MariaDB
//! write   a statement changing rows of a table with validations for its
//!         event runs row by row (routine::trigger): for each row its
//!         BEFORE triggers, then each validation, in the order they were
//!         created, on the final NEW (and OLD for an update), then the
//!         change, then its AFTER triggers; the statement runs in its own
//!         transaction or savepoint, so a refusal undoes all of it and
//!         nothing else
//! run     the version from the logic records (cached); the row bound by
//!         name as an after-commit trigger's is (KIRun parameters: a
//!         column, old_<column>, op; JavaScript: row, old, op, ctx or
//!         columns; Lua: ctx.row, ctx.old, ctx.trigger); wall budget
//!         logic.validation_budget_ms; db reads run as the definer, in the
//!         transaction, and nothing may be written (39862)
//! answer  sprig.reject(m), or a reject or error event with message m:
//!         39863 m (MongoDB shows 121); any other error as it is
//! ```
//!
//! Every writer meets them: SQL on both ports (text and prepared), HTTP's
//! `Sql.Execute`, and logic code called from MongoDB (`sprigCall`), Redis
//! (`FCALL`) or HTTP, whose `db` statements go through a session. A
//! tenant's validations are read from its records at most once a second
//! per node, and again at once after this node's own logic DDL.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::ddl::object_key;
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_common::SprigCode;
use sprig_common::ids::{DbId, TenantId, UserId};
use sprig_logic::LogicStore;
use sprig_logic::catalog::{BindingRow, ModuleRow, NewVersion};
use sprig_logic::declare::{Declaration, TierPref};
use sprig_logic::engine::bridge::{DbOp, Shape};
use sprig_logic::engine::kirun::Host;
use sprig_logic::engine::outbound::HttpOutbound;
use sprig_logic::engine::{ParamTy, Program, ScriptSig};
use tokio::sync::mpsc;

use super::{Datum, ExecCtx, ObjectId, PassSession, Result, SprigError, database_key};
use crate::engine::table::TableDef;
use crate::exec::logic::{ChannelDb, DbRequest};
use crate::pipeline::Wire;

/// How long a tenant's validations, as read, are used before they are read
/// again (another node's `CREATE` or `DROP` reaches this one within it).
const FRESH: Duration = Duration::from_secs(1);

/// The parameter names, besides columns, that row-bound code may take.
const ROW_NAMES: [&str; 5] = ["row", "old", "op", "ctx", "trigger"];

tokio::task_local! {
    /// The validations of the tenant as the running statement read them.
    static VALS: Arc<Validations>;
    /// Set while a validation runs: its statements may only read.
    static VALIDATING: String;
}

/// One active validation of a table.
#[derive(Debug)]
pub(super) struct Validation {
    module: u64,
    version: u64,
    name: String,
    db: DbId,
    insert: bool,
    update: bool,
    definer: UserId,
}

/// A tenant's active validations, by table, each table's in the order
/// they were created.
#[derive(Debug, Default)]
pub(super) struct Validations {
    by_table: HashMap<ObjectId, Vec<Arc<Validation>>>,
}

impl Validations {
    /// Whether the tenant has none.
    pub(super) fn is_empty(&self) -> bool {
        self.by_table.is_empty()
    }

    /// Whether `table` has any.
    pub(super) fn has(&self, table: ObjectId) -> bool {
        self.by_table.contains_key(&table)
    }

    /// Whether a write of `table` checks some: an update (`update`) or an
    /// insert.
    pub(super) fn fires(&self, table: ObjectId, update: bool) -> bool {
        self.by_table
            .get(&table)
            .is_some_and(|l| l.iter().any(|v| if update { v.update } else { v.insert }))
    }

    fn of(&self, table: ObjectId, update: bool) -> Vec<Arc<Validation>> {
        self.by_table
            .get(&table)
            .map(|l| {
                l.iter()
                    .filter(|v| if update { v.update } else { v.insert })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Each tenant's validations as last read, with when.
type Cache = HashMap<u64, (Instant, Arc<Validations>)>;

static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn cache() -> std::sync::MutexGuard<'static, Cache> {
    CACHE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Forgets `tenant`'s validations as read: the next write reads them again
/// (after this node's own logic DDL).
fn invalidate(tenant: u64) {
    cache().remove(&tenant);
}

/// Runs `fut` with `vals` as the validations its writes check.
pub(super) async fn with<F: std::future::Future>(vals: Arc<Validations>, fut: F) -> F::Output {
    VALS.scope(vals, fut).await
}

/// The validation running on this task, if any: its statements may only
/// read.
pub(super) fn running() -> Option<String> {
    VALIDATING.try_with(Clone::clone).ok()
}

/// Whether `text` starts with `INSERT`, `UPDATE`, `DELETE` or `REPLACE`
/// (after blanks, comments and opening parentheses).
pub(super) fn write_verb(text: &str) -> bool {
    let w = first_word(text);
    ["INSERT", "UPDATE", "DELETE", "REPLACE"]
        .iter()
        .any(|k| w.eq_ignore_ascii_case(k))
}

/// Whether `text` starts with `DELETE`: validations don't check it.
pub(super) fn deletes(text: &str) -> bool {
    first_word(text).eq_ignore_ascii_case("DELETE")
}

/// The first word of `text`, after blanks, comments and opening
/// parentheses.
fn first_word(text: &str) -> &str {
    let mut t = text;
    loop {
        let s = t.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(r) = s.strip_prefix("/*") {
            t = r.split_once("*/").map_or("", |x| x.1);
        } else if let Some(r) = s.strip_prefix("-- ").or_else(|| s.strip_prefix('#')) {
            t = r.split_once('\n').map_or("", |x| x.1);
        } else {
            t = s;
            break;
        }
    }
    let end = t
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(t.len());
    &t[..end]
}

/// 39862: a validation's statement that would write.
pub(super) fn writes_refused(name: &str, sql: &str) -> SprigError {
    let shown: String = sql.trim().chars().take(80).collect();
    SprigError::new(
        SprigCode::LogicUnsupported,
        format!("validation {name} may only read data; it ran: {shown}"),
    )
}

/// Whether a validation may run `op`: reads and calls of logic functions
/// (whose own writes are refused when they run).
fn may_run(op: &DbOp) -> bool {
    match op.shape {
        Shape::One | Shape::Many => {
            let w = first_word(&op.sql);
            ["SELECT", "WITH", "VALUES", "TABLE", "SHOW"]
                .iter()
                .any(|k| w.eq_ignore_ascii_case(k))
        }
        Shape::Call => true,
        Shape::Exec | Shape::Sprig => false,
    }
}

/// A row as a JSON object of its columns.
fn row_json(def: &TableDef, row: &[Datum]) -> Value {
    let mut m = Map::new();
    for (c, v) in def.columns.iter().zip(row) {
        m.insert(c.name.clone(), crate::table_changes::value_json(c, v));
    }
    Value::Object(m)
}

/// The message of a `reject` or `error` event.
fn event_message(result: &Value) -> String {
    result
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| result.to_string(), str::to_owned)
}

/// The signature recorded with a version: the names a JavaScript
/// validation takes its row by (`row`, `old`, ... or columns), `ctx` for
/// Lua; KIRun takes its own from the definition.
fn signature(d: &Declaration, events: &[String], table: &str) -> (ScriptSig, Value) {
    let sig = match d.runtime {
        sprig_logic::compiler::Runtime::Javascript => sprig_logic::testrun::js_row_sig(&d.body),
        _ => ScriptSig {
            params: vec![("ctx".to_owned(), ParamTy::Json)],
            scalar: false,
        },
    };
    let params: Vec<Value> = sig
        .params
        .iter()
        .map(|(n, _)| json!({"name": n, "type": "json"}))
        .collect();
    let json = json!({
        "params": params,
        "scalar": false,
        "events": [{"name": "output", "fields": []},
                   {"name": "reject", "fields": [{"name": "message", "type": "str"}]}],
        "validation": {"table": table, "events": events},
    });
    (sig, json)
}

/// 39874 unless every parameter name is a column of `def`, `old_<column>`,
/// or one of [`ROW_NAMES`].
fn check_params(names: &[String], def: &TableDef) -> Result<()> {
    let col = |n: &str| def.columns.iter().any(|c| c.name.eq_ignore_ascii_case(n));
    for p in names {
        let l = p.to_ascii_lowercase();
        let ok =
            ROW_NAMES.contains(&l.as_str()) || col(&l) || l.strip_prefix("old_").is_some_and(&col);
        if !ok {
            return Err(SprigError::new(
                SprigCode::LogicSignatureMismatch,
                format!(
                    "parameter {p} names no column of {} (use a column, old_<column>, op, row, \
                     old or ctx)",
                    def.name
                ),
            ));
        }
    }
    Ok(())
}

impl PassSession {
    /// This node's logic DDL changed the tenant's logic modules: its
    /// validations are read again by the next write.
    pub(in crate::exec) fn validations_changed(&self) {
        if let Some(ts) = self.tenant.as_deref() {
            invalidate(ts.tenant.0);
        }
    }

    /// The tenant's validations: as read less than [`FRESH`] ago, else
    /// read from its records now.
    pub(super) async fn validations(&self) -> Result<Arc<Validations>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(Arc::default());
        };
        let tenant = ts.tenant;
        if let Some((at, v)) = cache().get(&tenant.0)
            && at.elapsed() < FRESH
        {
            return Ok(Arc::clone(v));
        }
        let svc = Arc::clone(ts.services());
        let started = Instant::now();
        let store = LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let bindings = store.step_validations(&mut *conn).await?;
        let mut by_table: HashMap<ObjectId, Vec<Arc<Validation>>> = HashMap::new();
        for b in bindings {
            let Some(object) = b.object_id else { continue };
            let Some(m) = store.module_by_id(&mut *conn, b.module_id).await? else {
                continue;
            };
            let (true, true, Some(version)) = (
                m.state == "active",
                m.kind == "validation",
                m.active_version,
            ) else {
                continue;
            };
            let events: Vec<&str> = b.events.split(',').map(str::trim).collect();
            by_table
                .entry(ObjectId(object))
                .or_default()
                .push(Arc::new(Validation {
                    module: m.module_id,
                    version,
                    name: m.name,
                    db: DbId(m.db_id),
                    insert: events.contains(&"insert"),
                    update: events.contains(&"update"),
                    definer: UserId(m.created_by),
                }));
        }
        drop(conn);
        let v = Arc::new(Validations { by_table });
        cache().insert(tenant.0, (Instant::now(), Arc::clone(&v)));
        tracing::debug!(%tenant, tables = v.by_table.len(), ms = started.elapsed().as_millis(), "validations read");
        Ok(v)
    }

    /// The pre-write hook: runs the validations of `def` for one row about
    /// to be written, `new` as the BEFORE triggers left it, `old` the
    /// stored row of an update (`None`: an insert). The first refusal is
    /// the error.
    pub(super) async fn val_check(
        &mut self,
        cx: &mut ExecCtx<'_>,
        def: &TableDef,
        db: &str,
        new: &[Datum],
        old: Option<&[Datum]>,
    ) -> Result<()> {
        let list = VALS
            .try_with(|v| v.of(def.object, old.is_some()))
            .unwrap_or_default();
        if list.is_empty() {
            return Ok(());
        }
        let op = if old.is_some() { "update" } else { "insert" };
        let event = json!({
            "op": op,
            "db": db,
            "table": def.name,
            "before": old.map_or(Value::Null, |o| row_json(def, o)),
            "after": row_json(def, new),
        });
        for v in &list {
            Box::pin(self.val_run(cx, v, &event)).await?;
        }
        Ok(())
    }

    /// One validation on one row (see the module docs).
    #[allow(clippy::too_many_lines)] // load, arguments, the run's loop, the answer
    async fn val_run(&mut self, cx: &mut ExecCtx<'_>, v: &Validation, event: &Value) -> Result<()> {
        let (svc, tenant, cat, user) = {
            let ts = self
                .tenant
                .as_deref_mut()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let cat = match ts.catalog() {
                Some(c) => Arc::clone(c),
                None => ts.refresh().await?,
            };
            (
                Arc::clone(ts.services()),
                ts.tenant,
                cat,
                ts.principal.user.clone(),
            )
        };
        let ver = if let Some(x) = sprig_logic::invoke::cached(tenant.0, v.module, v.version) {
            x
        } else {
            let mut conn = svc.records.begin(tenant).await?;
            sprig_logic::invoke::load_any(&mut *conn, tenant.0, v.module, v.version).await?
        };
        let program = ver.program.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicEngineUnavailable,
                format!("{}: the {} runtime isn't built", ver.name, ver.runtime),
            )
        })?;
        let db_row = cat.databases.get(v.db).cloned();
        let mut sctx = cat.setting_ctx(&svc.dc);
        if let Some(r) = &db_row {
            sctx = sctx.with_db(r.db_id);
        }
        let (mut limits, policy) =
            sprig_logic::settings::limits(&svc.settings, &sctx, Some(&cat.settings));
        limits.wall = Duration::from_millis(
            svc.settings
                .get(
                    &sprig_logic::settings::VALIDATION_BUDGET_MS,
                    &sctx,
                    Some(&cat.settings),
                )
                .max(1),
        );
        limits.wall_setting = "logic.validation_budget_ms";
        let db_name = db_row.as_ref().map(|r| r.name.clone());
        let mut ctx = json!({
            "user": user,
            "tenant": tenant.0,
            "database": db_name,
            "function": ver.name,
            "version": ver.version,
            "trigger": event,
        });
        let params: Vec<String> = match program {
            Program::Kirun(p) => p.params().to_vec(),
            _ => Vec::new(),
        };
        let args = crate::exec::change_trigger::args(
            &ver.runtime,
            &params,
            &ver.signature,
            event,
            &mut ctx,
        );
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let (tx, mut rx) = mpsc::channel::<DbRequest>(1);
        let host = Host {
            db: Arc::new(ChannelDb(tx)),
            outbound: Arc::new(HttpOutbound::new(policy)),
            default_db: db_name,
            info: ctx,
        };
        let name = v.name.clone();
        let run = program.run(args, host, limits);
        // The session as the definer while the code's statements run.
        let mut became = None;
        let mut switched = false;
        let outcome = VALIDATING
            .scope(name.clone(), async {
                tokio::pin!(run);
                loop {
                    tokio::select! {
                        biased;
                        r = &mut run => break r,
                        Some((op, reply)) = rx.recv() => {
                            let op: DbOp = op;
                            if !may_run(&op) {
                                let _ = reply.send(Err(writes_refused(&name, &op.sql)));
                                continue;
                            }
                            if !switched {
                                switched = true;
                                match self.val_become(v.definer, &name) {
                                    Ok(saved) => became = saved,
                                    Err(e) => {
                                        let _ = reply.send(Err(e));
                                        continue;
                                    }
                                }
                            }
                            let r = self.engine_db(cx, op, wire).await;
                            let _ = reply.send(r);
                        }
                    }
                }
            })
            .await;
        if let (Some(w), Some(ts)) = (became, self.tenant.as_deref_mut()) {
            ts.principal.user_id = w.0;
            ts.principal.user = w.1;
            ts.principal.host = w.2;
            ts.authz = w.3;
        }
        let outcome = outcome?;
        if matches!(outcome.event.as_str(), "reject" | "error") {
            return Err(SprigError::new(
                SprigCode::LogicValidationFailed,
                event_message(&outcome.result),
            ));
        }
        Ok(())
    }

    /// The session as validation `name`'s definer (user `uid`); what to
    /// put back after (`None`: it already is).
    #[allow(clippy::type_complexity)]
    fn val_become(
        &mut self,
        uid: UserId,
        name: &str,
    ) -> Result<
        Option<(
            Option<UserId>,
            String,
            String,
            crate::tenant::authz::SessionAuthz,
        )>,
    > {
        let who = {
            let Some(ts) = self.tenant.as_deref() else {
                return Ok(None);
            };
            if ts.principal.user_id == Some(uid) {
                return Ok(None);
            }
            let Some(cat) = ts.catalog() else {
                return Ok(None);
            };
            sprig_auth::TenantAuth::of(cat)
                .and_then(|a| a.user(uid))
                .map(|u| (u.row.name.clone(), u.row.host.clone()))
        };
        let Some(who) = who else {
            return Err(SprigError::standard(
                1449,
                *b"HY000",
                format!(
                    "The user specified as a definer of validation {name} (user id {}) does not \
                     exist",
                    uid.0
                ),
            ));
        };
        self.sp_become(Some(&who))
    }

    /// `CREATE [OR REPLACE] VALIDATION` on engine database `db` (see the
    /// module docs). Returns the client's notes.
    #[allow(clippy::too_many_lines)] // checks, the source, then one record transaction
    pub(in crate::exec) async fn logic_deploy_validation(
        &mut self,
        decl: Declaration,
        db: &str,
    ) -> Result<Vec<String>> {
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
            .ok_or_else(|| SprigError::internal("a validation without its table"))?;
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
        // Validations run in the writer's statement and see only its row:
        // TRIGGER on the table is the right to bind code to its writes.
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        if !grants.unrestricted()
            && !grants.allows(
                Action::Trigger,
                AuthzTarget::Object {
                    db_key: &db_key,
                    db: row.db_id,
                    object: o.object_id,
                },
            )
        {
            return Err(SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "TRIGGER command denied to user '{user_name}' for table '{}'",
                    t.name
                ),
            ));
        }
        // The source, checked by the runtime that will run it.
        let events: Vec<String> = if decl.events.is_empty() {
            vec!["insert".into(), "update".into()]
        } else {
            decl.events.clone()
        };
        let (sig, sig_json) = signature(&decl, &events, &def.name);
        match decl.runtime {
            sprig_logic::compiler::Runtime::Kirun => {
                // Every function it calls must be one the runtime has.
                let missing = sprig_logic::engine::kirun::Prepared::new(&decl.body)?
                    .missing_functions()
                    .await;
                if !missing.is_empty() {
                    return Err(SprigError::new(
                        SprigCode::LogicUnsupported,
                        format!(
                            "{} calls functions no tier can run: {}",
                            decl.name,
                            missing.join(", ")
                        ),
                    ));
                }
                if let Program::Kirun(p) =
                    Program::new("kirun", &decl.name, &decl.body, sig.clone())?
                {
                    check_params(p.params(), &def)?;
                }
            }
            sprig_logic::compiler::Runtime::Javascript | sprig_logic::compiler::Runtime::Lua => {
                Program::new(decl.runtime.as_str(), &decl.name, &decl.body, sig.clone())?;
                let names: Vec<String> = sig.params.iter().map(|(n, _)| n.clone()).collect();
                check_params(&names, &def)?;
            }
            sprig_logic::compiler::Runtime::Sql => {
                return Err(SprigError::new(
                    SprigCode::LogicUnsupported,
                    format!(
                        "validation {}: validations are written in javascript, kirun or lua",
                        decl.name
                    ),
                ));
            }
        }
        let svc = Arc::clone(ts.services());
        let tenant: TenantId = ts.tenant;
        let store = LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let kind = decl.kind.as_str();
        let existing = store
            .module(&mut *conn, row.db_id.0, kind, &decl.name)
            .await?;
        if existing.is_some() && !decl.or_replace {
            return Err(SprigError::standard(
                1304,
                *b"42000",
                format!("VALIDATION {} already exists", decl.name),
            ));
        }
        let (module, exists) = match existing {
            Some(mut m) => {
                decl.runtime.as_str().clone_into(&mut m.runtime);
                "definer".clone_into(&mut m.security);
                TierPref::Engine.as_str().clone_into(&mut m.tier_pref);
                (m, true)
            }
            None => (
                ModuleRow {
                    module_id: sprig_catalog::store::rec::CoreRecords::next_ids(
                        &mut *conn,
                        sprig_catalog::store::Counter::Object,
                        1,
                    )
                    .await?,
                    db_id: row.db_id.0,
                    name: decl.name.clone(),
                    kind: kind.to_owned(),
                    runtime: decl.runtime.as_str().to_owned(),
                    security: "definer".to_owned(),
                    tier_pref: TierPref::Engine.as_str().to_owned(),
                    active_version: None,
                    state: "active".to_owned(),
                    created_by: user.0,
                },
                false,
            ),
        };
        if exists {
            store
                .update_options(&mut *conn, &module, catalog.version)
                .await?;
        } else {
            store
                .create_module(&mut *conn, &module, catalog.version)
                .await?;
        }
        let version = store.next_version(&mut *conn, module.module_id).await?;
        let detail = "validations on engine tables run on the language runtimes in the write path";
        store
            .add_version(
                &mut *conn,
                module.module_id,
                version,
                &NewVersion {
                    source: &decl.body,
                    signature: sig_json,
                    catalog_version: catalog.version,
                    tier: "engine",
                    placement: json!({
                        "tier": "engine",
                        "reasons": [{"code": "validation.engine", "at": "source", "detail": detail}],
                        "route": null,
                        "objects": [def.name.clone()],
                        "cost": 0,
                    }),
                    plsql: None,
                    created_by: user.0,
                },
            )
            .await?;
        let mut binding = store
            .binding(&mut *conn, module.module_id)
            .await?
            .unwrap_or(BindingRow {
                binding_id: 0,
                module_id: module.module_id,
                object_id: None,
                column_name: None,
                timing: String::new(),
                events: String::new(),
                when_fields: Vec::new(),
                enforcement: None,
                state: String::new(),
            });
        binding.object_id = Some(o.object_id.0);
        "before_write".clone_into(&mut binding.timing);
        binding.events = events.join(",");
        binding.enforcement = Some("step".to_owned());
        "active".clone_into(&mut binding.state);
        store.put_binding(&mut *conn, &binding).await?;
        sprig_logic::rollout::clear(&mut *conn, module.module_id).await?;
        store
            .activate(&mut *conn, module.module_id, version, catalog.version)
            .await?;
        conn.commit().await?;
        invalidate(tenant.0);
        let mut notes = vec![format!(
            "validation {} version {version} on {}.{} ({}): checked before each row is written, \
             on the {} runtime",
            decl.name,
            row.name,
            def.name,
            events.join(", "),
            decl.runtime.as_str()
        )];
        if decl.rollout.is_some() {
            notes.push(format!(
                "SPRIG_ROLLOUT doesn't apply to validations: version {version} checks every write \
                 now"
            ));
        }
        tracing::info!(%tenant, module = module.module_id, version, table = %def.name, name = %decl.name, "validation on an engine table deployed");
        Ok(notes)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn write_verbs_after_comments_and_parentheses() {
        assert!(write_verb("insert into t values (1)"));
        assert!(write_verb("  /* x */ -- y\n UPDATE t SET a = 1"));
        assert!(write_verb("# c\nREPLACE INTO t VALUES (1)"));
        assert!(write_verb("DELETE FROM t"));
        assert!(!write_verb("SELECT * FROM t"));
        assert!(!write_verb("(SELECT 1)"));
        assert!(!write_verb("INSERTED"));
        assert_eq!(first_word("  ( (select 1))"), "select");
    }

    #[test]
    fn validations_read_only_their_data() {
        let op = |shape, sql: &str| DbOp {
            shape,
            sql: sql.to_owned(),
        };
        assert!(may_run(&op(Shape::One, "SELECT price FROM p WHERE id = 7")));
        assert!(may_run(&op(
            Shape::Many,
            "WITH x AS (SELECT 1) SELECT * FROM x"
        )));
        assert!(may_run(&op(Shape::Call, "CALL `d`.`f`(1)")));
        assert!(!may_run(&op(Shape::Exec, "SELECT 1")));
        assert!(!may_run(&op(Shape::Many, "DELETE FROM t")));
        assert!(!may_run(&op(Shape::Sprig, "CALL sprig.mq_publish('x')")));
    }

    #[test]
    fn events_choose_validations() {
        let v = |name: &str, insert, update| {
            Arc::new(Validation {
                module: 1,
                version: 1,
                name: name.to_owned(),
                db: DbId(1),
                insert,
                update,
                definer: UserId(1),
            })
        };
        let mut by_table = HashMap::new();
        by_table.insert(
            ObjectId(9),
            vec![v("a", true, true), v("b", false, true), v("c", true, false)],
        );
        let vals = Validations { by_table };
        let names = |l: Vec<Arc<Validation>>| l.iter().map(|x| x.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(vals.of(ObjectId(9), false)), ["a", "c"]);
        assert_eq!(names(vals.of(ObjectId(9), true)), ["a", "b"]);
        assert!(vals.of(ObjectId(8), true).is_empty());
        assert!(vals.fires(ObjectId(9), true) && vals.has(ObjectId(9)));
        assert!(!vals.fires(ObjectId(8), false) && !vals.is_empty());
    }
}
