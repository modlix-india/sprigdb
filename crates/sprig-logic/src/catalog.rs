//! The logic catalog: modules, versions, bindings, schedules, dead
//! letters and the run log of one tenant, kept as engine catalog records
//! under `m/logic/<table>/`, and [`LogicStore`], the reads and writes the
//! deploy path, the jobs and `sprigctl logic` use.
//!
//! ```text
//! m/logic/modules/<module u64>                       a module
//! m/logic/mnames/<db u32><kind><name>                a live module's id
//! m/logic/versions/<module u64><version u64>         a version (source, signature, tier, state)
//! m/logic/bindings/<binding u64>                     a binding
//! m/logic/schedules/<module u64>                     a schedule and its run state
//! m/logic/dead/<dl u64>                              a dead letter
//! m/logic/dkeys/<binding u64><event key>             a dead letter's id
//! m/logic/log/<log u64>                              a run-log line
//! m/logic/sources/<binding u64>                      a message trigger's source ([`crate::messages`])
//! m/logic/rollouts/<module u64>                      a staged rollout ([`crate::rollout`])
//! m/logic/stats/<module u64><version u64>            a version's call numbers ([`crate::rollout`])
//! ```
//!
//! Writes go through the caller's record transaction. Module ids are
//! object ids; binding, dead-letter and log ids come from record counters
//! (`logic.binding`, `logic.dead`, `logic.log`).

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_catalog::{Migration, Module};
use sprig_common::{Result, SprigCode, SprigError};

/// The module name in the catalog's format records.
pub const MODULE: &str = "logic";

/// The module's MariaDB migrations: none, its rows are engine records.
pub static LOGIC_MIGRATIONS: &[Migration] = &[];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and
/// tenant bootstrap.
pub const LOGIC_MODULE: Module = Module {
    name: MODULE,
    migrations: LOGIC_MIGRATIONS,
};

/// A module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModuleRow {
    /// The module's id (an object id).
    pub module_id: u64,
    /// The database.
    pub db_id: u32,
    /// The name.
    pub name: String,
    /// `function`, `procedure`, ...
    pub kind: String,
    /// `kirun`, `javascript`, `sql`, `lua`.
    pub runtime: String,
    /// `invoker` or `definer`.
    pub security: String,
    /// `auto`, `mariadb` or `engine`.
    pub tier_pref: String,
    /// The live version.
    pub active_version: Option<u64>,
    /// `active`, `disabled`, `dropping`, `dropped`.
    pub state: String,
    /// Who created it.
    pub created_by: u32,
}

/// A version (without the source text).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VersionRow {
    /// The module.
    pub module_id: u64,
    /// The version, never reused.
    pub version: u64,
    /// The signature: params and events.
    pub signature: serde_json::Value,
    /// `mariadb`, `engine` or `refused`.
    pub tier: String,
    /// Reasons, route, objects, cost.
    pub placement: serde_json::Value,
    /// The version's state.
    pub state: String,
    /// The compiler that compiled it.
    pub compiler_version: String,
}

/// A new version to record.
#[derive(Debug, Clone)]
pub struct NewVersion<'a> {
    /// The source text.
    pub source: &'a str,
    /// The signature.
    pub signature: serde_json::Value,
    /// The catalog version compiled against.
    pub catalog_version: u64,
    /// `mariadb`, `engine` or `refused`.
    pub tier: &'a str,
    /// Placement as JSON.
    pub placement: serde_json::Value,
    /// The generated text.
    pub plsql: Option<String>,
    /// The deploying user.
    pub created_by: u32,
}

/// Reads and writes of one tenant's logic catalog (engine records under
/// `m/logic/`).
#[derive(Debug, Clone, Default)]
pub struct LogicStore {
    schema: String,
}

/// A binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindingRow {
    /// The binding.
    pub binding_id: u64,
    /// Its module.
    pub module_id: u64,
    /// The table (`objects.object_id`).
    pub object_id: Option<u64>,
    /// Computed fields: the column.
    pub column_name: Option<String>,
    /// `before_write`, `after_commit`, `on_read`, `on_write`.
    pub timing: String,
    /// `insert,update,delete`.
    pub events: String,
    /// `FOR UPDATE OF (...)`.
    pub when_fields: Vec<String>,
    /// `trigger`, `check`, `generated`, `gateway`.
    pub enforcement: Option<String>,
    /// `active`, `paused`, `dropping`.
    pub state: String,
}

/// A schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRow {
    /// The schedule's module.
    pub module_id: u64,
    /// Cron text.
    pub cron: String,
    /// IANA zone.
    pub time_zone: String,
    /// The procedure it calls.
    pub target_module_id: u64,
    /// Arguments, a JSON array.
    pub args: String,
    /// `skip` or `queue_one`.
    pub overlap: String,
    /// Paused.
    pub paused: bool,
    /// Next fire time, UTC, Unix ms.
    pub next_fire_ms: u64,
    /// Last fire time.
    pub last_fire_ms: Option<u64>,
    /// `ok`, `error`, `budget`, `skipped`, `misfired`.
    pub last_outcome: Option<String>,
    /// The live run's job id, hex.
    pub running_job_id: Option<String>,
    /// A run waits (`queue_one`).
    pub queued: bool,
}

/// A dead letter.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeadLetterRow {
    /// Its id.
    pub dl_id: u64,
    /// The binding.
    pub binding_id: u64,
    /// The version that failed.
    pub version: u64,
    /// Shard, GTID and row ordinal.
    pub event_key: String,
    /// The event.
    pub event: serde_json::Value,
    /// Attempts so far.
    pub attempts: u32,
    /// The last error's number.
    pub error_code: u32,
    /// Its message.
    pub error_message: String,
    /// When it last failed, `YYYY-MM-DD hh:mm:ss`.
    pub last_failed_at: String,
    /// `dead`, `replaying`, `replayed`, `discarded`.
    pub state: String,
}

/// A run-log line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LogRow {
    /// Its id.
    pub log_id: u64,
    /// The module.
    pub module_id: u64,
    /// The version, if one ran.
    pub version: Option<u64>,
    /// When, `YYYY-MM-DD hh:mm:ss.ffffff` UTC.
    pub at: String,
    /// `info`, `warn`, `error`.
    pub level: String,
    /// `trigger`, `schedule`, `deploy`.
    pub source: String,
    /// What happened.
    pub message: String,
}

/// A module's active or candidate version ([`LogicStore::live_versions`]).
#[derive(Debug, Clone, PartialEq)]
pub struct LiveVersion {
    /// The module.
    pub module_id: u64,
    /// Its name.
    pub name: String,
    /// Its kind.
    pub kind: String,
    /// The version.
    pub version: u64,
    /// Its tier.
    pub tier: String,
    /// Its signature.
    pub signature: serde_json::Value,
    /// Its placement.
    pub placement: serde_json::Value,
}

/// A function or procedure as a caller outside SQL finds it (logic
/// functions as endpoints).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Callable {
    /// The module.
    pub module_id: u64,
    /// Its name, as stored.
    pub name: String,
    /// `function` or `procedure`.
    pub kind: String,
    /// `kirun`, `javascript`, `lua`.
    pub runtime: String,
    /// The protocols it is exposed to (`sql`, `http`, `grpc`, ...).
    pub exposure: Vec<String>,
    /// The active version.
    pub version: Option<u64>,
    /// The active version's tier (`mariadb`, `engine`).
    pub tier: Option<String>,
    /// The active version's signature (params, events), if any.
    pub signature: Option<serde_json::Value>,
}

impl Callable {
    /// Whether it may be called over `protocol`.
    #[must_use]
    pub fn exposed_to(&self, protocol: &str) -> bool {
        self.exposure.iter().any(|p| p == protocol)
    }

    /// The parameter names, in call order.
    #[must_use]
    pub fn params(&self) -> Vec<String> {
        self.signature
            .as_ref()
            .and_then(|s| s["params"].as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|p| p["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Modules by id.
pub const MODULES: RowTable = RowTable::new(MODULE, "modules");
/// A live module's id by database, kind and name.
pub const MODULE_NAMES: RowTable = RowTable::new(MODULE, "mnames");
/// Versions by module and version.
pub const VERSIONS: RowTable = RowTable::new(MODULE, "versions");
/// Bindings by id.
pub const BINDINGS: RowTable = RowTable::new(MODULE, "bindings");
/// Schedules by module.
pub const SCHEDULES: RowTable = RowTable::new(MODULE, "schedules");
/// Dead letters by id.
pub const DEAD_LETTERS: RowTable = RowTable::new(MODULE, "dead");
/// A dead letter's id by binding and event key.
pub const DEAD_KEYS: RowTable = RowTable::new(MODULE, "dkeys");
/// The run log by id.
pub const LOG: RowTable = RowTable::new(MODULE, "log");

/// Unix milliseconds now.
#[must_use]
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Unix microseconds now.
fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// `us` (Unix microseconds) as UTC `YYYY-MM-DD hh:mm:ss` and the
/// microseconds within the second.
#[must_use]
pub(crate) fn utc_text(us: u64) -> (String, u64) {
    let secs = us / 1_000_000;
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (
        format!(
            "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
            rem / 3600,
            rem % 3600 / 60,
            rem % 60
        ),
        us % 1_000_000,
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModRec {
    module_id: u64,
    db_id: u32,
    name: String,
    kind: String,
    runtime: String,
    security: String,
    tier_pref: String,
    exposure: String,
    active_version: Option<u64>,
    candidate_version: Option<u64>,
    candidate_pct: u8,
    state: String,
    created_by: u32,
    created_ms: u64,
    changed_version: u64,
}

impl ModRec {
    fn row(&self) -> ModuleRow {
        ModuleRow {
            module_id: self.module_id,
            db_id: self.db_id,
            name: self.name.clone(),
            kind: self.kind.clone(),
            runtime: self.runtime.clone(),
            security: self.security.clone(),
            tier_pref: self.tier_pref.clone(),
            active_version: self.active_version,
            state: self.state.clone(),
            created_by: self.created_by,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VerRec {
    module_id: u64,
    version: u64,
    source: String,
    source_sha256: Vec<u8>,
    signature: serde_json::Value,
    catalog_version: u64,
    compiler_version: String,
    tier: String,
    placement: serde_json::Value,
    plsql: Option<String>,
    state: String,
    created_by: u32,
    created_ms: u64,
    activated_ms: Option<u64>,
}

impl VerRec {
    fn row(&self) -> VersionRow {
        VersionRow {
            module_id: self.module_id,
            version: self.version,
            signature: self.signature.clone(),
            tier: self.tier.clone(),
            placement: self.placement.clone(),
            state: self.state.clone(),
            compiler_version: self.compiler_version.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BindRec {
    binding_id: u64,
    module_id: u64,
    object_id: Option<u64>,
    column_name: Option<String>,
    timing: String,
    events: String,
    when_fields: Vec<String>,
    enforcement: Option<String>,
    state: String,
}

impl BindRec {
    fn row(&self) -> BindingRow {
        BindingRow {
            binding_id: self.binding_id,
            module_id: self.module_id,
            object_id: self.object_id,
            column_name: self.column_name.clone(),
            timing: self.timing.clone(),
            events: self.events.clone(),
            when_fields: self.when_fields.clone(),
            enforcement: self.enforcement.clone(),
            state: self.state.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeadRec {
    dl_id: u64,
    binding_id: u64,
    version: u64,
    event_key: String,
    resume_token: Vec<u8>,
    event: serde_json::Value,
    attempts: u32,
    error_code: u32,
    error_message: String,
    trace_id: Vec<u8>,
    last_failed_us: u64,
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogRec {
    log_id: u64,
    module_id: u64,
    version: Option<u64>,
    at_us: u64,
    level: String,
    source: String,
    message: String,
}

fn pk_module(module_id: u64) -> Pk {
    Pk::new().u64(module_id)
}

fn pk_name(db_id: u32, kind: &str, name: &str) -> Pk {
    Pk::new()
        .u32(db_id)
        .bytes(kind.as_bytes())
        .bytes(name.as_bytes())
}

fn pk_version(module_id: u64, version: u64) -> Pk {
    Pk::new().u64(module_id).u64(version)
}

impl LogicStore {
    async fn mod_rec(c: &mut dyn RecordTx, module_id: u64) -> Result<Option<ModRec>> {
        MODULES.get(c, &pk_module(module_id)).await
    }

    async fn ver_rec(c: &mut dyn RecordTx, module_id: u64, version: u64) -> Result<Option<VerRec>> {
        VERSIONS.get(c, &pk_version(module_id, version)).await
    }

    /// Records a module's `EXPOSE (...)` list (`sql` is always in it).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_exposure(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        exposure: &[String],
    ) -> Result<()> {
        let mut list: Vec<&str> = vec!["sql"];
        for e in exposure {
            if !list.contains(&e.as_str()) {
                list.push(e.as_str());
            }
        }
        let Some(mut m) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(module_id))
            .await?
        else {
            return Ok(());
        };
        m.exposure = list.join(",");
        MODULES.put(c, &pk_module(module_id), &m).await
    }

    /// The live function or procedure `name` of `db_id` (functions first),
    /// with its exposure and active signature.
    ///
    /// # Errors
    /// The record store's.
    pub async fn callable(
        &self,
        c: &mut dyn RecordTx,
        db_id: u32,
        name: &str,
    ) -> Result<Option<Callable>> {
        for kind in ["function", "procedure"] {
            let Some(id) = MODULE_NAMES
                .get::<u64>(c, &pk_name(db_id, kind, name))
                .await?
            else {
                continue;
            };
            let Some(m) = Self::mod_rec(c, id).await? else {
                continue;
            };
            if m.state != "active" {
                continue;
            }
            let v = match m.active_version {
                Some(v) => Self::ver_rec(c, id, v).await?,
                None => None,
            };
            return Ok(Some(Callable {
                module_id: id,
                name: m.name,
                kind: m.kind,
                runtime: m.runtime,
                exposure: m
                    .exposure
                    .split(',')
                    .filter(|x| !x.is_empty())
                    .map(str::to_owned)
                    .collect(),
                version: m.active_version,
                tier: v.as_ref().map(|v| v.tier.clone()),
                signature: v.map(|v| v.signature),
            }));
        }
        Ok(None)
    }

    /// The logic catalog of one tenant (its records); `schema` names the
    /// tenant's catalog for messages.
    #[must_use]
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
        }
    }

    /// The catalog name given to [`Self::new`].
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The live module of `kind` called `name` in `db_id`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn module(
        &self,
        c: &mut dyn RecordTx,
        db_id: u32,
        kind: &str,
        name: &str,
    ) -> Result<Option<ModuleRow>> {
        let Some(id) = MODULE_NAMES
            .get::<u64>(c, &pk_name(db_id, kind, name))
            .await?
        else {
            return Ok(None);
        };
        Ok(Self::mod_rec(c, id)
            .await?
            .filter(|m| m.state != "dropped")
            .map(|m| m.row()))
    }

    /// Every live module of `db_id` (all databases with `None`), by
    /// database and name.
    ///
    /// # Errors
    /// The record store's.
    pub async fn modules(
        &self,
        c: &mut dyn RecordTx,
        db_id: Option<u32>,
    ) -> Result<Vec<ModuleRow>> {
        let mut v: Vec<ModuleRow> = MODULES
            .rows::<ModRec>(c, &Pk::new())
            .await?
            .into_iter()
            .filter(|m| m.state != "dropped" && db_id.is_none_or(|d| m.db_id == d))
            .map(|m| m.row())
            .collect();
        v.sort_by(|a, b| (a.db_id, &a.name).cmp(&(b.db_id, &b.name)));
        Ok(v)
    }

    /// Inserts a module (its id already allocated).
    ///
    /// # Errors
    /// 39869 when another deploy created it first; the record store's.
    pub async fn create_module(
        &self,
        c: &mut dyn RecordTx,
        m: &ModuleRow,
        catalog_version: u64,
    ) -> Result<()> {
        let name = pk_name(m.db_id, &m.kind, &m.name);
        if MODULE_NAMES
            .get_for_update::<u64>(c, &name)
            .await?
            .is_some()
        {
            return Err(SprigError::new(
                SprigCode::LogicVersionConflict,
                format!("{} {} is being deployed by another session", m.kind, m.name),
            ));
        }
        let rec = ModRec {
            module_id: m.module_id,
            db_id: m.db_id,
            name: m.name.clone(),
            kind: m.kind.clone(),
            runtime: m.runtime.clone(),
            security: m.security.clone(),
            tier_pref: m.tier_pref.clone(),
            exposure: "sql".to_owned(),
            active_version: None,
            candidate_version: None,
            candidate_pct: 0,
            state: "active".to_owned(),
            created_by: m.created_by,
            created_ms: now_ms(),
            changed_version: catalog_version,
        };
        MODULE_NAMES.put(c, &name, &m.module_id).await?;
        MODULES.put(c, &pk_module(m.module_id), &rec).await
    }

    /// Updates a module's declared options for a new version.
    ///
    /// # Errors
    /// The record store's.
    pub async fn update_options(
        &self,
        c: &mut dyn RecordTx,
        m: &ModuleRow,
        catalog_version: u64,
    ) -> Result<()> {
        let Some(mut r) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(m.module_id))
            .await?
        else {
            return Ok(());
        };
        r.runtime.clone_from(&m.runtime);
        r.security.clone_from(&m.security);
        r.tier_pref.clone_from(&m.tier_pref);
        r.changed_version = catalog_version;
        MODULES.put(c, &pk_module(m.module_id), &r).await
    }

    /// The number the next version gets: one past every existing one.
    ///
    /// # Errors
    /// The record store's.
    pub async fn next_version(&self, c: &mut dyn RecordTx, module_id: u64) -> Result<u64> {
        let last = VERSIONS
            .scan::<serde_json::Value>(c, &pk_module(module_id))
            .await?
            .into_iter()
            .filter_map(|(_, v)| v["version"].as_u64())
            .max();
        Ok(last.unwrap_or(0) + 1)
    }

    /// Records version `version` (from [`next_version`](Self::next_version)).
    ///
    /// # Errors
    /// 39869 when another deploy took the same version; the record store's.
    pub async fn add_version(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
        v: &NewVersion<'_>,
    ) -> Result<()> {
        let pk = pk_version(module_id, version);
        if VERSIONS.get_for_update::<VerRec>(c, &pk).await?.is_some() {
            return Err(SprigError::new(
                SprigCode::LogicVersionConflict,
                "another deploy of this module took the same version",
            ));
        }
        let sha = {
            use sha2::Digest;
            sha2::Sha256::digest(v.source.as_bytes()).to_vec()
        };
        let state = if v.tier == "refused" {
            "failed"
        } else {
            "compiled"
        };
        let rec = VerRec {
            module_id,
            version,
            source: v.source.to_owned(),
            source_sha256: sha,
            signature: v.signature.clone(),
            catalog_version: v.catalog_version,
            compiler_version: sprig_logic_compile::COMPILER_VERSION.to_owned(),
            tier: v.tier.to_owned(),
            placement: v.placement.clone(),
            plsql: v.plsql.clone(),
            state: state.to_owned(),
            created_by: v.created_by,
            created_ms: now_ms(),
            activated_ms: None,
        };
        VERSIONS.put(c, &pk, &rec).await
    }

    /// Sets a version's state.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_version_state(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
        state: &str,
    ) -> Result<()> {
        let pk = pk_version(module_id, version);
        let Some(mut v) = VERSIONS.get_for_update::<VerRec>(c, &pk).await? else {
            return Ok(());
        };
        v.state = state.to_owned();
        VERSIONS.put(c, &pk, &v).await
    }

    /// Makes `version` the module's active version (the old one is
    /// superseded).
    ///
    /// # Errors
    /// The record store's.
    pub async fn activate(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
        catalog_version: u64,
    ) -> Result<()> {
        for (pk, mut v) in VERSIONS.scan::<VerRec>(c, &pk_module(module_id)).await? {
            if v.version == version {
                v.state = "active".to_owned();
                v.activated_ms = Some(now_ms());
            } else if v.state == "active" {
                v.state = "superseded".to_owned();
            } else {
                continue;
            }
            VERSIONS.put(c, &pk, &v).await?;
        }
        if let Some(mut m) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(module_id))
            .await?
        {
            m.active_version = Some(version);
            m.changed_version = catalog_version;
            MODULES.put(c, &pk_module(module_id), &m).await?;
        }
        Ok(())
    }

    /// Every version of a module, newest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn versions(&self, c: &mut dyn RecordTx, module_id: u64) -> Result<Vec<VersionRow>> {
        let mut v: Vec<VersionRow> = VERSIONS
            .rows::<VerRec>(c, &pk_module(module_id))
            .await?
            .iter()
            .map(VerRec::row)
            .collect();
        v.reverse();
        Ok(v)
    }

    /// A version's generated text.
    ///
    /// # Errors
    /// 39864 for an unknown version; the record store's.
    pub async fn plsql(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
    ) -> Result<Option<String>> {
        Self::ver_rec(c, module_id, version)
            .await?
            .map(|v| v.plsql)
            .ok_or_else(|| {
                SprigError::new(SprigCode::LogicNotFound, format!("no version {version}"))
            })
    }

    /// A module by id, whatever its state.
    ///
    /// # Errors
    /// The record store's.
    pub async fn module_by_id(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
    ) -> Result<Option<ModuleRow>> {
        Ok(Self::mod_rec(c, module_id).await?.map(|m| m.row()))
    }

    /// Enables or disables a module; `false` when it wasn't in the other
    /// state.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_module_state(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        enabled: bool,
    ) -> Result<bool> {
        let (to, from) = if enabled {
            ("active", "disabled")
        } else {
            ("disabled", "active")
        };
        let Some(mut m) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(module_id))
            .await?
        else {
            return Ok(false);
        };
        if m.state != from {
            return Ok(false);
        }
        m.state = to.to_owned();
        MODULES.put(c, &pk_module(module_id), &m).await?;
        Ok(true)
    }

    /// A version's source text.
    ///
    /// # Errors
    /// 39864 for an unknown version; the record store's.
    pub async fn source(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
    ) -> Result<String> {
        Self::ver_rec(c, module_id, version)
            .await?
            .map(|v| v.source)
            .ok_or_else(|| {
                SprigError::new(SprigCode::LogicNotFound, format!("no version {version}"))
            })
    }

    /// Who deployed the module's active version (`None` without one).
    ///
    /// # Errors
    /// The record store's.
    pub async fn active_definer(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
    ) -> Result<Option<u32>> {
        let Some(m) = self.module_by_id(c, module_id).await? else {
            return Ok(None);
        };
        let Some(v) = m.active_version else {
            return Ok(None);
        };
        Ok(Self::ver_rec(c, module_id, v).await?.map(|r| r.created_by))
    }

    /// A version's signature and tier.
    ///
    /// # Errors
    /// 39864 for an unknown version; the record store's.
    pub async fn version_signature(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: u64,
    ) -> Result<(serde_json::Value, String)> {
        let v = Self::ver_rec(c, module_id, version).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no version {version} of module {module_id}"),
            )
        })?;
        Ok((v.signature, v.tier))
    }

    async fn all_bindings(c: &mut dyn RecordTx) -> Result<Vec<BindRec>> {
        BINDINGS.rows::<BindRec>(c, &Pk::new()).await
    }

    /// A module's (first) binding.
    ///
    /// # Errors
    /// The record store's.
    pub async fn binding(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
    ) -> Result<Option<BindingRow>> {
        Ok(Self::all_bindings(c)
            .await?
            .iter()
            .find(|b| b.module_id == module_id)
            .map(BindRec::row))
    }

    /// A binding by id.
    ///
    /// # Errors
    /// The record store's.
    pub async fn binding_by_id(
        &self,
        c: &mut dyn RecordTx,
        binding_id: u64,
    ) -> Result<Option<BindingRow>> {
        Ok(BINDINGS
            .get::<BindRec>(c, &pk_module(binding_id))
            .await?
            .map(|b| b.row()))
    }

    /// Writes a binding: an update when it has an id, else an insert that
    /// returns the new id.
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_binding(&self, c: &mut dyn RecordTx, b: &BindingRow) -> Result<u64> {
        let id = if b.binding_id != 0 {
            b.binding_id
        } else {
            sprig_catalog::records::next_ids(c, "logic.binding", 1).await?
        };
        let rec = BindRec {
            binding_id: id,
            module_id: b.module_id,
            object_id: b.object_id,
            column_name: b.column_name.clone(),
            timing: b.timing.clone(),
            events: b.events.clone(),
            when_fields: b.when_fields.clone(),
            enforcement: b.enforcement.clone(),
            state: b.state.clone(),
        };
        BINDINGS.put(c, &pk_module(id), &rec).await?;
        Ok(id)
    }

    /// Marks a computed-field module's bindings `dropping`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn drop_column_bindings(&self, c: &mut dyn RecordTx, module_id: u64) -> Result<()> {
        for (pk, mut b) in BINDINGS.scan::<BindRec>(c, &Pk::new()).await? {
            if b.module_id == module_id && b.column_name.is_some() {
                b.state = "dropping".to_owned();
                BINDINGS.put(c, &pk, &b).await?;
            }
        }
        Ok(())
    }

    /// The active and candidate versions of `db_id`'s active modules, by
    /// module and version.
    ///
    /// # Errors
    /// The record store's.
    pub async fn live_versions(
        &self,
        c: &mut dyn RecordTx,
        db_id: u32,
    ) -> Result<Vec<LiveVersion>> {
        let mut out = Vec::new();
        for m in MODULES.rows::<ModRec>(c, &Pk::new()).await? {
            if m.db_id != db_id || m.state != "active" {
                continue;
            }
            for v in VERSIONS.rows::<VerRec>(c, &pk_module(m.module_id)).await? {
                if Some(v.version) == m.active_version || Some(v.version) == m.candidate_version {
                    out.push(LiveVersion {
                        module_id: m.module_id,
                        name: m.name.clone(),
                        kind: m.kind.clone(),
                        version: v.version,
                        tier: v.tier,
                        signature: v.signature,
                        placement: v.placement,
                    });
                }
            }
        }
        Ok(out)
    }

    /// The live modules whose schedule calls `target`: (id, name), by name.
    ///
    /// # Errors
    /// The record store's.
    pub async fn schedulers_of(
        &self,
        c: &mut dyn RecordTx,
        target: u64,
    ) -> Result<Vec<(u64, String)>> {
        let mut out = Vec::new();
        for s in SCHEDULES.rows::<ScheduleRow>(c, &Pk::new()).await? {
            if s.target_module_id == target
                && let Some(m) = Self::mod_rec(c, s.module_id).await?
                && m.state != "dropped"
            {
                out.push((m.module_id, m.name));
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(out)
    }

    /// The active source of every other live module of `db_id`: (kind,
    /// name, source), by name.
    ///
    /// # Errors
    /// The record store's.
    pub async fn active_sources(
        &self,
        c: &mut dyn RecordTx,
        db_id: u32,
        except: u64,
    ) -> Result<Vec<(String, String, String)>> {
        let mut out = Vec::new();
        for m in MODULES.rows::<ModRec>(c, &Pk::new()).await? {
            if m.db_id != db_id || m.state == "dropped" || m.module_id == except {
                continue;
            }
            if let Some(v) = m.active_version
                && let Some(v) = Self::ver_rec(c, m.module_id, v).await?
            {
                out.push((m.kind, m.name, v.source));
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(out)
    }

    /// A module's active computed-column bindings: (binding, table object,
    /// column).
    ///
    /// # Errors
    /// The record store's.
    pub async fn column_bindings(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
    ) -> Result<Vec<(u64, Option<u64>, String)>> {
        Ok(Self::all_bindings(c)
            .await?
            .into_iter()
            .filter(|b| b.module_id == module_id && b.state == "active")
            .filter_map(|b| Some((b.binding_id, b.object_id, b.column_name?)))
            .collect())
    }

    /// Every active after-commit binding.
    ///
    /// # Errors
    /// The record store's.
    pub async fn after_commit_bindings(&self, c: &mut dyn RecordTx) -> Result<Vec<BindingRow>> {
        Ok(Self::all_bindings(c)
            .await?
            .iter()
            .filter(|b| b.timing == "after_commit" && b.state == "active" && b.module_id != 0)
            .map(BindRec::row)
            .collect())
    }

    /// Every active `CHECK` validation: its binding, the module's active
    /// version, the module's name and the version's check message.
    ///
    /// # Errors
    /// The record store's.
    pub async fn check_validations(
        &self,
        c: &mut dyn RecordTx,
    ) -> Result<Vec<(u64, u64, String, Option<String>)>> {
        let mut out = Vec::new();
        for b in Self::all_bindings(c).await? {
            let b = b.row();
            if b.enforcement.as_deref() != Some("check") {
                continue;
            }
            let Some(m) = self.module_by_id(c, b.module_id).await? else {
                continue;
            };
            let (true, Some(v)) = (m.state == "active", m.active_version) else {
                continue;
            };
            let msg = Self::ver_rec(c, m.module_id, v)
                .await?
                .and_then(|r| r.placement["check_message"].as_str().map(str::to_owned));
            out.push((b.binding_id, v, m.name, msg));
        }
        Ok(out)
    }

    /// Writes (or replaces the definition of) a schedule; its run state
    /// is kept.
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_schedule(&self, c: &mut dyn RecordTx, s: &ScheduleRow) -> Result<()> {
        let pk = pk_module(s.module_id);
        let prev = SCHEDULES.get_for_update::<ScheduleRow>(c, &pk).await?;
        let mut row = s.clone();
        if let Some(p) = prev {
            row.last_fire_ms = p.last_fire_ms;
            row.last_outcome = p.last_outcome;
            row.running_job_id = p.running_job_id;
            row.queued = p.queued;
        } else {
            row.last_fire_ms = None;
            row.last_outcome = None;
            row.running_job_id = None;
            row.queued = false;
        }
        SCHEDULES.put(c, &pk, &row).await
    }

    /// A module's schedule.
    ///
    /// # Errors
    /// The record store's.
    pub async fn schedule(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
    ) -> Result<Option<ScheduleRow>> {
        SCHEDULES.get(c, &pk_module(module_id)).await
    }

    async fn sched_update(
        c: &mut dyn RecordTx,
        module_id: u64,
        f: impl FnOnce(&mut ScheduleRow) -> bool + Send,
    ) -> Result<bool> {
        let pk = pk_module(module_id);
        let Some(mut s) = SCHEDULES.get_for_update::<ScheduleRow>(c, &pk).await? else {
            return Ok(false);
        };
        if !f(&mut s) {
            return Ok(false);
        }
        SCHEDULES.put(c, &pk, &s).await?;
        Ok(true)
    }

    /// Starts a run if the schedule is still due as seen (`seen_next_ms`)
    /// and nothing runs; `false` when another node fired it.
    ///
    /// # Errors
    /// The record store's.
    pub async fn fire(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        seen_next_ms: u64,
        new_next_ms: u64,
        job: &[u8; 16],
    ) -> Result<bool> {
        let job = hex(job);
        Self::sched_update(c, module_id, |s| {
            if s.paused || s.running_job_id.is_some() || s.next_fire_ms != seen_next_ms {
                return false;
            }
            s.running_job_id = Some(job);
            s.last_fire_ms = Some(now_ms());
            s.queued = false;
            s.next_fire_ms = new_next_ms;
            true
        })
        .await
    }

    /// Moves the next fire time without a run (skipped, misfired), and
    /// queues one under `queue_one`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn pass(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        seen_next_ms: u64,
        new_next_ms: u64,
        outcome: &str,
        queue: bool,
    ) -> Result<bool> {
        let outcome = outcome.to_owned();
        Self::sched_update(c, module_id, |s| {
            if s.next_fire_ms != seen_next_ms {
                return false;
            }
            s.next_fire_ms = new_next_ms;
            s.last_outcome = Some(outcome);
            s.queued = s.queued || queue;
            true
        })
        .await
    }

    /// Starts the queued run once the previous one ended.
    ///
    /// # Errors
    /// The record store's.
    pub async fn fire_queued(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        job: &[u8; 16],
    ) -> Result<bool> {
        let job = hex(job);
        Self::sched_update(c, module_id, |s| {
            if s.paused || s.running_job_id.is_some() || !s.queued {
                return false;
            }
            s.running_job_id = Some(job);
            s.last_fire_ms = Some(now_ms());
            s.queued = false;
            true
        })
        .await
    }

    /// Ends run `job` with `outcome`; `false` when it wasn't the live run.
    ///
    /// # Errors
    /// The record store's.
    pub async fn run_ended(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        job: &[u8; 16],
        outcome: &str,
    ) -> Result<bool> {
        let job = hex(job);
        let outcome = outcome.to_owned();
        Self::sched_update(c, module_id, |s| {
            if s.running_job_id.as_deref() != Some(job.as_str()) {
                return false;
            }
            s.running_job_id = None;
            s.last_outcome = Some(outcome);
            true
        })
        .await
    }

    /// Whether run `job` is the schedule's live run.
    ///
    /// # Errors
    /// The record store's.
    pub async fn is_running(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        job: &[u8; 16],
    ) -> Result<bool> {
        let job = hex(job);
        Ok(SCHEDULES
            .get::<ScheduleRow>(c, &pk_module(module_id))
            .await?
            .is_some_and(|s| s.running_job_id.as_deref() == Some(job.as_str())))
    }

    /// Records (or adds an attempt to) a dead letter of `binding_id`'s
    /// event `event_key`.
    ///
    /// # Errors
    /// The record store's.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_dead_letter(
        &self,
        c: &mut dyn RecordTx,
        binding_id: u64,
        version: u64,
        event_key: &str,
        resume_token: &[u8],
        event: &serde_json::Value,
        attempts: u32,
        error_code: u32,
        error_message: &str,
    ) -> Result<()> {
        let key = Pk::new().u64(binding_id).bytes(event_key.as_bytes());
        if let Some(id) = DEAD_KEYS.get_for_update::<u64>(c, &key).await?
            && let Some(mut d) = DEAD_LETTERS
                .get_for_update::<DeadRec>(c, &pk_module(id))
                .await?
        {
            d.version = version;
            d.attempts = d.attempts.saturating_add(attempts);
            d.error_code = error_code;
            error_message.clone_into(&mut d.error_message);
            d.last_failed_us = now_us();
            d.state = "dead".to_owned();
            return DEAD_LETTERS.put(c, &pk_module(id), &d).await;
        }
        let trace = {
            use sha2::Digest;
            let d = sha2::Sha256::digest(format!("{binding_id}/{event_key}").as_bytes());
            d[..16].to_vec()
        };
        let id = sprig_catalog::records::next_ids(c, "logic.dead", 1).await?;
        let d = DeadRec {
            dl_id: id,
            binding_id,
            version,
            event_key: event_key.to_owned(),
            resume_token: resume_token.to_vec(),
            event: event.clone(),
            attempts,
            error_code,
            error_message: error_message.to_owned(),
            trace_id: trace,
            last_failed_us: now_us(),
            state: "dead".to_owned(),
        };
        DEAD_KEYS.put(c, &key, &id).await?;
        DEAD_LETTERS.put(c, &pk_module(id), &d).await
    }

    /// Dead letters, by id, at most `limit`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn dead_letters(
        &self,
        c: &mut dyn RecordTx,
        binding_id: Option<u64>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<DeadLetterRow>> {
        Ok(DEAD_LETTERS
            .rows::<DeadRec>(c, &Pk::new())
            .await?
            .into_iter()
            .filter(|d| {
                binding_id.is_none_or(|b| d.binding_id == b) && state.is_none_or(|s| d.state == s)
            })
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|d| DeadLetterRow {
                dl_id: d.dl_id,
                binding_id: d.binding_id,
                version: d.version,
                event_key: d.event_key,
                event: d.event,
                attempts: d.attempts,
                error_code: d.error_code,
                error_message: d.error_message,
                last_failed_at: utc_text(d.last_failed_us).0,
                state: d.state,
            })
            .collect())
    }

    /// Moves dead letters from state `from` to `to` (all of them, or
    /// `ids`); returns how many moved.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_dead_letters(
        &self,
        c: &mut dyn RecordTx,
        binding_id: Option<u64>,
        ids: &[u64],
        from: &str,
        to: &str,
    ) -> Result<u64> {
        let mut n = 0;
        for (pk, mut d) in DEAD_LETTERS.scan::<DeadRec>(c, &Pk::new()).await? {
            if d.state == from
                && binding_id.is_none_or(|b| d.binding_id == b)
                && (ids.is_empty() || ids.contains(&d.dl_id))
            {
                to.clone_into(&mut d.state);
                DEAD_LETTERS.put(c, &pk, &d).await?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Queues dead letters of after-commit and message triggers for
    /// replay; returns (queued, left: those of other timings, which
    /// can't be replayed).
    ///
    /// # Errors
    /// The record store's.
    pub async fn replay_dead_letters(
        &self,
        c: &mut dyn RecordTx,
        binding_id: Option<u64>,
        ids: &[u64],
    ) -> Result<(u64, u64)> {
        let timing: std::collections::HashMap<u64, String> = Self::all_bindings(c)
            .await?
            .into_iter()
            .map(|b| (b.binding_id, b.timing))
            .collect();
        let (mut queued, mut left) = (0, 0);
        for (pk, mut d) in DEAD_LETTERS.scan::<DeadRec>(c, &Pk::new()).await? {
            if d.state != "dead"
                || binding_id.is_some_and(|b| d.binding_id != b)
                || !(ids.is_empty() || ids.contains(&d.dl_id))
            {
                continue;
            }
            match timing.get(&d.binding_id).map(String::as_str) {
                Some("after_commit" | "on_message") => {
                    d.state = "replaying".to_owned();
                    DEAD_LETTERS.put(c, &pk, &d).await?;
                    queued += 1;
                }
                Some(_) => left += 1,
                None => {}
            }
        }
        Ok((queued, left))
    }

    /// Adds a run-log line (the message cut at 4,000 characters).
    ///
    /// # Errors
    /// The record store's.
    pub async fn log(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        version: Option<u64>,
        level: &str,
        source: &str,
        message: &str,
    ) -> Result<()> {
        let id = sprig_catalog::records::next_ids(c, "logic.log", 1).await?;
        let rec = LogRec {
            log_id: id,
            module_id,
            version,
            at_us: now_us(),
            level: level.to_owned(),
            source: source.to_owned(),
            message: message.chars().take(4000).collect(),
        };
        LOG.put(c, &pk_module(id), &rec).await
    }

    /// The newest `limit` log lines after `after_id` (of one module or
    /// all), oldest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn logs(
        &self,
        c: &mut dyn RecordTx,
        module_id: Option<u64>,
        after_id: u64,
        limit: u32,
    ) -> Result<Vec<LogRow>> {
        let mut rows: Vec<LogRec> = LOG
            .rows::<LogRec>(c, &Pk::new())
            .await?
            .into_iter()
            .filter(|l| l.log_id > after_id && module_id.is_none_or(|m| l.module_id == m))
            .collect();
        let keep = usize::try_from(limit).unwrap_or(usize::MAX);
        if rows.len() > keep {
            rows.drain(..rows.len() - keep);
        }
        Ok(rows
            .into_iter()
            .map(|l| {
                let (t, us) = utc_text(l.at_us);
                LogRow {
                    log_id: l.log_id,
                    module_id: l.module_id,
                    version: l.version,
                    at: format!("{t}.{us:06}"),
                    level: l.level,
                    source: l.source,
                    message: l.message,
                }
            })
            .collect())
    }

    /// Deletes log lines older than `days` and dead letters older than
    /// `dead_days`, at most `limit` of each.
    ///
    /// # Errors
    /// The record store's.
    pub async fn purge(
        &self,
        c: &mut dyn RecordTx,
        days: u32,
        dead_days: u32,
        limit: u32,
    ) -> Result<()> {
        let now = now_us();
        let cut = |d: u32| now.saturating_sub(u64::from(d) * 86_400_000_000);
        let keep = usize::try_from(limit).unwrap_or(usize::MAX);
        let old: Vec<Pk> = LOG
            .scan::<LogRec>(c, &Pk::new())
            .await?
            .into_iter()
            .filter(|(_, l)| l.at_us < cut(days))
            .take(keep)
            .map(|(pk, _)| pk)
            .collect();
        for pk in old {
            LOG.delete(c, &pk).await?;
        }
        let dead: Vec<(Pk, DeadRec)> = DEAD_LETTERS
            .scan::<DeadRec>(c, &Pk::new())
            .await?
            .into_iter()
            .filter(|(_, d)| d.last_failed_us < cut(dead_days))
            .take(keep)
            .collect();
        for (pk, d) in dead {
            DEAD_KEYS
                .delete(
                    c,
                    &Pk::new().u64(d.binding_id).bytes(d.event_key.as_bytes()),
                )
                .await?;
            DEAD_LETTERS.delete(c, &pk).await?;
        }
        Ok(())
    }

    /// Marks a module dropped (its name freed) and retires its versions.
    ///
    /// # Errors
    /// The record store's.
    pub async fn mark_dropped(
        &self,
        c: &mut dyn RecordTx,
        module_id: u64,
        catalog_version: u64,
    ) -> Result<()> {
        if let Some(mut m) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(module_id))
            .await?
        {
            let name = pk_name(m.db_id, &m.kind, &m.name);
            if MODULE_NAMES.get::<u64>(c, &name).await? == Some(module_id) {
                MODULE_NAMES.delete(c, &name).await?;
            }
            m.state = "dropped".to_owned();
            m.changed_version = catalog_version;
            m.name = format!("{}#{module_id}", m.name).chars().take(64).collect();
            MODULES.put(c, &pk_module(module_id), &m).await?;
        }
        for (pk, mut v) in VERSIONS.scan::<VerRec>(c, &pk_module(module_id)).await? {
            if matches!(v.state.as_str(), "active" | "compiled" | "superseded") {
                v.state = "retired".to_owned();
                VERSIONS.put(c, &pk, &v).await?;
            }
        }
        Ok(())
    }

    /// Sets a module's candidate version and its share (a rollout), or
    /// clears them.
    ///
    /// # Errors
    /// The record store's.
    pub(crate) async fn set_candidate(
        c: &mut dyn RecordTx,
        module_id: u64,
        candidate: Option<(u64, u8)>,
    ) -> Result<()> {
        if let Some(mut m) = MODULES
            .get_for_update::<ModRec>(c, &pk_module(module_id))
            .await?
        {
            m.candidate_version = candidate.map(|x| x.0);
            m.candidate_pct = candidate.map_or(0, |x| x.1);
            MODULES.put(c, &pk_module(module_id), &m).await?;
        }
        Ok(())
    }

    /// Sets the state of `module_id`'s versions: `version` when given,
    /// else every version in state `only`.
    ///
    /// # Errors
    /// The record store's.
    pub(crate) async fn set_versions_state(
        c: &mut dyn RecordTx,
        module_id: u64,
        version: Option<u64>,
        only: Option<&str>,
        to: &str,
    ) -> Result<()> {
        for (pk, mut v) in VERSIONS.scan::<VerRec>(c, &pk_module(module_id)).await? {
            if version.is_some_and(|x| x != v.version) || only.is_some_and(|s| s != v.state) {
                continue;
            }
            to.clone_into(&mut v.state);
            VERSIONS.put(c, &pk, &v).await?;
        }
        Ok(())
    }
}

/// `job` as lowercase hex.
fn hex(job: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(32);
    for b in job {
        let _ = write!(s, "{b:02x}");
    }
    s
}
