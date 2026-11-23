//! The `sinks` catalog module: connections, their sealed credentials,
//! sinks, `USAGE` grants, task rows and dead letters, kept as the engine's
//! catalog records (`m/sinks/<table>/<key>`), the snapshot loader
//! ([`SinksExtension`]) and the writes the statements and tasks make.
//!
//! ```text
//! m/sinks/connections/<connection_id u64>                       ConnectionDef
//! m/sinks/secrets/<connection_id u64><name><version u32>        SecretRow (sealed)
//! m/sinks/sinks/<sink_id u64>                                   SinkDef
//! m/sinks/state/<sink_id u64>                                   StateRow (not versioned)
//! m/sinks/usage/<db u32><connection><platform u8><user u32>     UsageRow
//! m/sinks/tasks/<sink_id u64><task_no u32>                      TaskRec (not versioned)
//! m/sinks/dead/<sink_id u64><dl_id u64>                         DeadRec (not versioned)
//! ```
//!
//! **Versioned and not.** Statements (`CREATE | ALTER | DROP CONNECTION`,
//! `CREATE | ALTER | DROP SINK`, `GRANT | REVOKE USAGE`) write through the
//! records of their catalog change ([`apply`](crate::sql::apply)). Tasks
//! write their own rows (claims, reports, dead letters) and a sink's state
//! in short engine transactions of their own, as they wrote MariaDB rows
//! without a catalog version; such rows are never written by a change's
//! staged records except when a statement sets a state or drops a sink.
//! A sink's state is its `state` record when there is one, else its
//! definition's.
//!
//! **Storage notes:**
//! - Sinks and connections have no `objects` row yet (as streams and
//!   queues have none): their ids come from the object counter, and
//!   privileges are checked by name (`SPRIG_SINK_ADMIN` on `*.*`).
//! - Credentials stay sealed at rest: a secret row holds the sealed bytes
//!   exactly as the MariaDB column did.
//! - `USAGE` grants are by connection name and user id (grants have no
//!   connection object yet).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, next_ids};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    ConnectionDef, ConnectionId, SecretMeta, SinkDef, SinkId, SinkState, TaskEpoch, TaskNo,
    TaskRow, TaskState, err,
};

/// The module name.
pub const MODULE: &str = "sinks";

/// Connections.
pub const CONNECTIONS: RowTable = RowTable::new(MODULE, "connections");
/// Sealed credentials.
pub const SECRETS: RowTable = RowTable::new(MODULE, "secrets");
/// Sinks' definitions.
pub const SINKS: RowTable = RowTable::new(MODULE, "sinks");
/// Sinks' current states.
pub const STATES: RowTable = RowTable::new(MODULE, "state");
/// `USAGE` grants.
pub const USAGE: RowTable = RowTable::new(MODULE, "usage");
/// Task rows.
pub const TASKS: RowTable = RowTable::new(MODULE, "tasks");
/// Dead letters.
pub const DEAD: RowTable = RowTable::new(MODULE, "dead");
/// The dead letters' id counter.
const DEAD_COUNTER: &str = "sink_dead";

/// The module's MariaDB migrations: none, its rows are engine records.
pub static SINKS_MIGRATIONS: &[Migration] = &[];

/// The module, for [`Migrator::new`].
pub const SINKS_MODULE: Module = Module {
    name: MODULE,
    migrations: SINKS_MIGRATIONS,
};

/// Makes sure catalog `schema` can take the module's records (its core
/// migrations, which create the record log).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("sinks catalog: {what}"))
}

fn exists() -> SprigError {
    err(
        SprigCode::SinkExists,
        "a sink or connection with this name exists",
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// `YYYY-MM-DD HH:MM:SS.fff` (UTC) of `ms` since the epoch.
fn utc_text(ms: u64) -> String {
    let secs = ms / 1000;
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;
    // Days to a civil date (proleptic Gregorian).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

fn b64(v: Option<&Vec<u8>>) -> Option<String> {
    v.map(|b| STANDARD.encode(b))
}

fn unb64(v: Option<&String>) -> Result<Option<Vec<u8>>> {
    v.map(|s| STANDARD.decode(s).map_err(|_| bad("base64")))
        .transpose()
}

/// A sealed credential version: what `sink_secrets` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRow {
    /// The sealing key's version.
    pub key_version: u32,
    /// The sealed value (base64 of the sealed bytes; empty once retired).
    pub sealed: String,
    /// `current`, `previous` or `retired`.
    pub state: String,
    /// Who set it.
    pub set_by: u32,
    /// When (ms since the epoch).
    pub set_at_ms: u64,
}

/// A sink's definition as stored, with its creation time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SinkRow {
    /// The definition.
    #[serde(flatten)]
    pub def: SinkDef,
    /// When it was created (ms since the epoch).
    pub created_at_ms: u64,
}

/// A sink's current state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRow {
    /// The state.
    pub state: SinkState,
    /// Why (failures and pauses).
    pub reason: Option<String>,
    /// When (ms since the epoch).
    pub at_ms: u64,
}

/// A `USAGE` grant's extra columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRow {
    /// Who granted it.
    pub granted_by: u32,
    /// When (ms since the epoch).
    pub granted_at_ms: u64,
}

/// A task row: what `sink_tasks` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRec {
    /// The task's share of the source.
    pub source_part: Value,
    /// Bumped on every claim.
    pub epoch: u64,
    /// The claiming worker.
    pub worker_id: Option<String>,
    /// Pending work (not written by this build).
    pub pending: Option<Value>,
    /// The task's state.
    pub state: TaskState,
    /// Failed attempts in a row.
    pub attempts: u32,
    /// The last error.
    pub last_error: Option<String>,
    /// When it happened (ms).
    pub last_error_at_ms: Option<u64>,
    /// Records delivered.
    pub delivered: u64,
    /// Last change (ms).
    pub updated_at_ms: u64,
}

/// A dead letter: what `sink_dead_letters` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadRec {
    /// The record's idempotency key.
    pub idem_key: String,
    /// Its key (base64).
    pub key: Option<String>,
    /// Its value (base64).
    pub value: Option<String>,
    /// Its headers.
    pub headers: Value,
    /// A change's operation letter.
    pub op: Option<String>,
    /// A change's before image.
    pub before: Option<Value>,
    /// A change's after image.
    pub after: Option<Value>,
    /// The record's time.
    pub ts_ms: i64,
    /// Its source position (base64 of the encoded position).
    pub position: String,
    /// Why it died.
    pub reason: String,
    /// The class of failure.
    pub class: String,
    /// Attempts made.
    pub attempts: u32,
    /// `dead`, `replaying`, `replayed`, `discarded`.
    pub state: String,
    /// When (ms).
    pub created_at_ms: u64,
}

fn secret_pk(conn: ConnectionId, name: &str, version: u32) -> Pk {
    Pk::new().u64(conn.0).bytes(name.as_bytes()).u32(version)
}

fn usage_pk(u: &Usage) -> Pk {
    Pk::new()
        .u32(u.db_id)
        .bytes(u.connection.as_bytes())
        .u8(u8::from(u.platform))
        .u32(u.user_id)
}

/// A usage key's parts back (database, connection, platform, user).
fn split_usage(k: &[u8]) -> Option<Usage> {
    let db_id = u32::from_be_bytes(k.get(..4)?.try_into().ok()?);
    let (name, rest) = unescape(k.get(4..)?)?;
    let platform = *rest.first()? != 0;
    let user_id = u32::from_be_bytes(rest.get(1..5)?.try_into().ok()?);
    Some(Usage {
        db_id,
        connection: String::from_utf8_lossy(&name).into_owned(),
        platform,
        user_id,
    })
}

/// A `Pk::bytes` part and what follows it.
fn unescape(k: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let b = *k.get(i)?;
        if b == 0 {
            match *k.get(i + 1)? {
                0 => return Some((out, k.get(i + 2..)?)),
                0xFF => {
                    out.push(0);
                    i += 2;
                    continue;
                }
                _ => return None,
            }
        }
        out.push(b);
        i += 1;
    }
}

/// A grant of `USAGE` on a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Usage {
    /// The connection's database.
    pub db_id: u32,
    /// Its name.
    pub connection: String,
    /// A platform connection.
    pub platform: bool,
    /// The grantee.
    pub user_id: u32,
}

/// The module in a catalog snapshot.
#[derive(Debug, Clone, Default)]
pub struct SinksState {
    /// The module's tables exist.
    pub migrated: bool,
    /// Connections by (database, name).
    pub connections: BTreeMap<(u32, String), Arc<ConnectionDef>>,
    /// Sinks by (database, name).
    pub sinks: BTreeMap<(u32, String), Arc<SinkDef>>,
    /// `USAGE` grants.
    pub usage: Vec<Usage>,
}

impl ExtState for SinksState {}

impl SinksState {
    /// A connection by database and name.
    #[must_use]
    pub fn connection(&self, db: u32, name: &str) -> Option<&Arc<ConnectionDef>> {
        self.connections.get(&(db, name.to_owned()))
    }

    /// A sink by database and name.
    #[must_use]
    pub fn sink(&self, db: u32, name: &str) -> Option<&Arc<SinkDef>> {
        self.sinks.get(&(db, name.to_owned()))
    }

    /// Whether `user` holds `USAGE` on the connection.
    #[must_use]
    pub fn has_usage(&self, db: u32, conn: &str, platform: bool, user: u32) -> bool {
        self.usage.iter().any(|u| {
            u.db_id == db && u.connection == conn && u.platform == platform && u.user_id == user
        })
    }

    /// Sinks using connection `id`.
    #[must_use]
    pub fn users_of(&self, id: ConnectionId) -> Vec<&Arc<SinkDef>> {
        self.sinks
            .values()
            .filter(|s| {
                s.connections
                    .iter()
                    .any(|c| matches!(c, crate::model::ConnRef::Tenant { id: i, .. } if *i == id))
            })
            .collect()
    }
}

/// The module's records: versioned writes inside a catalog change's
/// records (associated functions taking a [`RecordTx`]), and reads and
/// task writes in short transactions of their own on a tenant's records
/// (methods).
#[derive(Clone)]
pub struct SinksStore {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for SinksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinksStore")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl SinksStore {
    /// `tenant`'s sink records in `records`.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    /// `tenant`'s sink records through the engine node's peer service
    /// (`SPRIGDB_CATALOG_NODE`): operators' tools.
    #[must_use]
    pub fn remote(tenant: TenantId) -> Self {
        Self::new(
            Arc::new(sprig_catalog::records::remote::RemoteRecords::new(
                None, None,
            )),
            tenant,
        )
    }

    /// The ids of the sinks named `name` (in any database).
    ///
    /// # Errors
    /// The record store's.
    pub async fn ids_named(&self, name: &str) -> Result<Vec<SinkId>> {
        let name = name.to_owned();
        self.read(|rec| {
            Box::pin(async move {
                Ok(SINKS
                    .rows::<SinkRow>(rec, &Pk::new())
                    .await?
                    .into_iter()
                    .filter(|r| r.def.name == name)
                    .map(|r| r.def.id)
                    .collect())
            })
        })
        .await
    }

    async fn read<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a> FnOnce(
            &'a mut dyn RecordTx,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>,
        >,
    {
        let mut tx = self.records.begin(self.tenant).await?;
        let r = f(&mut *tx).await;
        let _ = tx.rollback().await;
        r
    }

    // ---- inside a catalog change, and the snapshot ----

    /// Everything a snapshot holds.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx) -> Result<SinksState> {
        let mut st = SinksState {
            migrated: true,
            ..SinksState::default()
        };
        let secrets = SECRETS.scan::<SecretRow>(rec, &Pk::new()).await?;
        for mut c in CONNECTIONS.rows::<ConnectionDef>(rec, &Pk::new()).await? {
            let under = Pk::new().u64(c.id.0);
            c.secrets = secrets
                .iter()
                .filter(|(pk, r)| pk.0.starts_with(&under.0) && r.state == "current")
                .filter_map(|(pk, r)| {
                    let (name, rest) = unescape(pk.0.get(8..)?)?;
                    let version = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
                    Some(SecretMeta {
                        name: String::from_utf8_lossy(&name).into_owned(),
                        version,
                        set_by: r.set_by,
                        set_at: utc_text(r.set_at_ms),
                    })
                })
                .collect();
            c.secrets.sort_by(|a, b| a.name.cmp(&b.name));
            st.connections
                .insert((c.db_id, c.name.clone()), Arc::new(c));
        }
        let states: BTreeMap<u64, StateRow> = STATES
            .scan::<StateRow>(rec, &Pk::new())
            .await?
            .into_iter()
            .filter_map(|(pk, r)| Some((u64::from_be_bytes(pk.0.get(..8)?.try_into().ok()?), r)))
            .collect();
        for row in SINKS.rows::<SinkRow>(rec, &Pk::new()).await? {
            let mut s = row.def;
            if let Some(r) = states.get(&s.id.0) {
                s.state = r.state;
                s.state_reason.clone_from(&r.reason);
            }
            if s.state == SinkState::Dropping {
                continue;
            }
            st.sinks.insert((s.db_id, s.name.clone()), Arc::new(s));
        }
        for (pk, _) in USAGE.scan::<UsageRow>(rec, &Pk::new()).await? {
            st.usage
                .push(split_usage(&pk.0).ok_or_else(|| bad("usage key"))?);
        }
        Ok(st)
    }

    /// Inserts a connection (its name must be free in its database).
    ///
    /// # Errors
    /// 39211 when the name is taken; the record store's.
    pub async fn insert_connection(rec: &mut dyn RecordTx, c: &ConnectionDef) -> Result<()> {
        let taken = CONNECTIONS
            .rows::<ConnectionDef>(rec, &Pk::new())
            .await?
            .iter()
            .any(|o| o.db_id == c.db_id && o.name == c.name && o.id != c.id);
        if taken
            || CONNECTIONS
                .get::<Value>(rec, &Pk::new().u64(c.id.0))
                .await?
                .is_some()
        {
            return Err(exists());
        }
        let mut row = c.clone();
        row.secrets.clear();
        CONNECTIONS.put(rec, &Pk::new().u64(c.id.0), &row).await
    }

    /// Updates a connection's options, hosts and tags.
    ///
    /// # Errors
    /// The record store's.
    pub async fn update_connection(rec: &mut dyn RecordTx, c: &ConnectionDef) -> Result<()> {
        let pk = Pk::new().u64(c.id.0);
        if let Some(mut row) = CONNECTIONS.get::<ConnectionDef>(rec, &pk).await? {
            row.options.clone_from(&c.options);
            row.hosts.clone_from(&c.hosts);
            row.residency_tags.clone_from(&c.residency_tags);
            CONNECTIONS.put(rec, &pk, &row).await?;
        }
        Ok(())
    }

    /// Deletes a connection, its secrets and its grants.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_connection(rec: &mut dyn RecordTx, c: &ConnectionDef) -> Result<()> {
        for (pk, _) in SECRETS
            .scan::<SecretRow>(rec, &Pk::new().u64(c.id.0))
            .await?
        {
            SECRETS.delete(rec, &pk).await?;
        }
        let under = Pk::new().u32(c.db_id).bytes(c.name.as_bytes()).u8(0);
        for (pk, _) in USAGE.scan::<UsageRow>(rec, &under).await? {
            USAGE.delete(rec, &pk).await?;
        }
        CONNECTIONS.delete(rec, &Pk::new().u64(c.id.0)).await
    }

    /// Writes a new version of credential `name`: the old current becomes
    /// previous, older previous versions are retired (their sealed bytes
    /// emptied).
    ///
    /// # Errors
    /// The record store's.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_secret(
        rec: &mut dyn RecordTx,
        conn: ConnectionId,
        name: &str,
        version: u32,
        key_version: u32,
        sealed: &[u8],
        set_by: u32,
    ) -> Result<()> {
        let under = Pk::new().u64(conn.0).bytes(name.as_bytes());
        for (pk, mut r) in SECRETS.scan::<SecretRow>(rec, &under).await? {
            let next = match r.state.as_str() {
                "previous" => "retired",
                "current" => "previous",
                _ => continue,
            };
            r.state = next.to_owned();
            if next == "retired" {
                r.sealed.clear();
            }
            SECRETS.put(rec, &pk, &r).await?;
        }
        SECRETS
            .put(
                rec,
                &secret_pk(conn, name, version),
                &SecretRow {
                    key_version,
                    sealed: STANDARD.encode(sealed),
                    state: "current".to_owned(),
                    set_by,
                    set_at_ms: now_ms(),
                },
            )
            .await
    }

    /// The next version of credential `name`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn next_secret_version(
        rec: &mut dyn RecordTx,
        conn: ConnectionId,
        name: &str,
    ) -> Result<u32> {
        let under = Pk::new().u64(conn.0).bytes(name.as_bytes());
        let last = SECRETS
            .scan::<SecretRow>(rec, &under)
            .await?
            .last()
            .and_then(|(pk, _)| {
                let k = &pk.0;
                k.len()
                    .checked_sub(4)
                    .and_then(|i| k.get(i..))
                    .and_then(|b| b.try_into().ok())
                    .map(u32::from_be_bytes)
            });
        Ok(last.unwrap_or(0) + 1)
    }

    /// Inserts a sink (its name must be free in its database).
    ///
    /// # Errors
    /// 39211 when the name is taken; the record store's.
    pub async fn insert_sink(rec: &mut dyn RecordTx, s: &SinkDef) -> Result<()> {
        let taken = SINKS
            .rows::<SinkRow>(rec, &Pk::new())
            .await?
            .iter()
            .any(|o| o.def.db_id == s.db_id && o.def.name == s.name && o.def.id != s.id);
        if taken
            || SINKS
                .get::<Value>(rec, &Pk::new().u64(s.id.0))
                .await?
                .is_some()
        {
            return Err(exists());
        }
        let row = SinkRow {
            def: s.clone(),
            created_at_ms: now_ms(),
        };
        SINKS.put(rec, &Pk::new().u64(s.id.0), &row).await
    }

    /// Rewrites a sink's definition (`ALTER SINK`), bumping its version;
    /// its state is the statement's.
    ///
    /// # Errors
    /// The record store's.
    pub async fn update_sink(rec: &mut dyn RecordTx, s: &SinkDef) -> Result<()> {
        let pk = Pk::new().u64(s.id.0);
        let Some(mut row) = SINKS.get::<SinkRow>(rec, &pk).await? else {
            return Ok(());
        };
        let d = &mut row.def;
        d.source = s.source.clone();
        d.target = s.target.clone();
        d.start_at = s.start_at;
        d.owner_user_id = s.owner_user_id;
        d.state = s.state;
        d.state_reason.clone_from(&s.state_reason);
        d.definition_ver += 1;
        d.options.clone_from(&s.options);
        SINKS.put(rec, &pk, &row).await
    }

    /// Writes a sink's state record in `rec`.
    async fn put_state(
        rec: &mut dyn RecordTx,
        id: SinkId,
        state: SinkState,
        reason: Option<&str>,
    ) -> Result<()> {
        STATES
            .put(
                rec,
                &Pk::new().u64(id.0),
                &StateRow {
                    state,
                    reason: reason.map(|r| r.chars().take(1024).collect()),
                    at_ms: now_ms(),
                },
            )
            .await
    }

    /// Deletes a sink's definition (its state and task rows go after the
    /// change commits: [`forget_sink`](Self::forget_sink)).
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_sink(rec: &mut dyn RecordTx, id: SinkId) -> Result<()> {
        SINKS.delete(rec, &Pk::new().u64(id.0)).await
    }

    /// Grants or revokes `USAGE`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_usage(rec: &mut dyn RecordTx, u: &Usage, grant: bool, by: u32) -> Result<()> {
        let pk = usage_pk(u);
        if !grant {
            return USAGE.delete(rec, &pk).await;
        }
        if USAGE.get::<UsageRow>(rec, &pk).await?.is_some() {
            return Ok(());
        }
        USAGE
            .put(
                rec,
                &pk,
                &UsageRow {
                    granted_by: by,
                    granted_at_ms: now_ms(),
                },
            )
            .await
    }

    // ---- reads, each in a short transaction ----

    /// One sink by id (workers), with its current state.
    ///
    /// # Errors
    /// The record store's.
    pub async fn sink(&self, id: SinkId) -> Result<Option<SinkDef>> {
        self.read(|rec| {
            Box::pin(async move {
                let Some(row) = SINKS.get::<SinkRow>(rec, &Pk::new().u64(id.0)).await? else {
                    return Ok(None);
                };
                let mut s = row.def;
                if let Some(r) = STATES.get::<StateRow>(rec, &Pk::new().u64(id.0)).await? {
                    s.state = r.state;
                    s.state_reason = r.reason;
                }
                Ok(Some(s))
            })
        })
        .await
    }

    /// When sink `id` was created (ms since the epoch).
    ///
    /// # Errors
    /// The record store's.
    pub async fn created_ms(&self, id: SinkId) -> Result<Option<i64>> {
        let r = self
            .read(|rec| {
                Box::pin(async move { SINKS.get::<SinkRow>(rec, &Pk::new().u64(id.0)).await })
            })
            .await?;
        Ok(r.map(|r| i64::try_from(r.created_at_ms).unwrap_or(i64::MAX)))
    }

    /// One connection by id (workers).
    ///
    /// # Errors
    /// The record store's.
    pub async fn connection(&self, id: ConnectionId) -> Result<Option<ConnectionDef>> {
        self.read(|rec| Box::pin(async move { CONNECTIONS.get(rec, &Pk::new().u64(id.0)).await }))
            .await
    }

    /// Sealed credentials of a connection: `(name, version, key version,
    /// sealed, previous)` for current and previous versions; previous ones
    /// only while their successor is younger than `previous_hours`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn sealed_secrets(
        &self,
        conn: ConnectionId,
        previous_hours: u64,
    ) -> Result<Vec<(String, u32, u32, Vec<u8>, bool)>> {
        let rows = self
            .read(|rec| {
                Box::pin(
                    async move { SECRETS.scan::<SecretRow>(rec, &Pk::new().u64(conn.0)).await },
                )
            })
            .await?;
        let mut parsed = Vec::new();
        for (pk, r) in rows {
            let Some((name, rest)) = pk.0.get(8..).and_then(unescape) else {
                return Err(bad("secret key"));
            };
            let version = u32::from_be_bytes(
                rest.get(..4)
                    .and_then(|b| b.try_into().ok())
                    .ok_or_else(|| bad("secret key"))?,
            );
            parsed.push((String::from_utf8_lossy(&name).into_owned(), version, r));
        }
        let horizon = now_ms().saturating_sub(previous_hours.saturating_mul(3_600_000));
        let mut out = Vec::new();
        for (name, version, r) in &parsed {
            let keep = match r.state.as_str() {
                "current" => true,
                "previous" => parsed
                    .iter()
                    .any(|(n, _, c)| n == name && c.state == "current" && c.set_at_ms > horizon),
                _ => false,
            };
            if keep {
                let sealed = STANDARD.decode(&r.sealed).map_err(|_| bad("sealed"))?;
                out.push((
                    name.clone(),
                    *version,
                    r.key_version,
                    sealed,
                    r.state == "previous",
                ));
            }
        }
        Ok(out)
    }

    /// The state, reason and definition version of a sink (tasks poll it).
    ///
    /// # Errors
    /// The record store's.
    pub async fn state_of(&self, id: SinkId) -> Result<Option<(SinkState, Option<String>, u64)>> {
        Ok(self
            .sink(id)
            .await?
            .map(|s| (s.state, s.state_reason, s.definition_ver)))
    }

    /// Every sink's current state and reason (tasks change them without a
    /// catalog version, so snapshots lag).
    ///
    /// # Errors
    /// The record store's.
    pub async fn states(
        &self,
    ) -> Result<std::collections::HashMap<u64, (SinkState, Option<String>)>> {
        self.read(|rec| {
            Box::pin(async move {
                let mut out = std::collections::HashMap::new();
                for r in SINKS.rows::<SinkRow>(rec, &Pk::new()).await? {
                    out.insert(r.def.id.0, (r.def.state, r.def.state_reason));
                }
                for (pk, r) in STATES.scan::<StateRow>(rec, &Pk::new()).await? {
                    let id = u64::from_be_bytes(
                        pk.0.get(..8)
                            .and_then(|b| b.try_into().ok())
                            .ok_or_else(|| bad("state key"))?,
                    );
                    if let Some(e) = out.get_mut(&id) {
                        *e = (r.state, r.reason);
                    }
                }
                Ok(out)
            })
        })
        .await
    }

    /// A sink's tasks (`SHOW SINK STATUS`).
    ///
    /// # Errors
    /// The record store's.
    pub async fn tasks(&self, sink: SinkId) -> Result<Vec<TaskRow>> {
        let rows = self
            .read(|rec| {
                Box::pin(async move { TASKS.scan::<TaskRec>(rec, &Pk::new().u64(sink.0)).await })
            })
            .await?;
        rows.into_iter()
            .map(|(pk, r)| {
                let n =
                    pk.0.get(8..12)
                        .and_then(|b| b.try_into().ok())
                        .map(u32::from_be_bytes)
                        .ok_or_else(|| bad("task key"))?;
                Ok(TaskRow {
                    sink,
                    task_no: TaskNo(n),
                    source_part: r.source_part,
                    epoch: TaskEpoch(r.epoch),
                    worker_id: r.worker_id,
                    pending: r.pending,
                    state: r.state,
                    attempts: r.attempts,
                    last_error: r.last_error,
                    delivered: r.delivered,
                })
            })
            .collect()
    }

    /// The task's last claim: its epoch, worker and source part, read
    /// before a new claim (a queue task's dead predecessor).
    ///
    /// # Errors
    /// The record store's.
    pub async fn task_claim(
        &self,
        sink: SinkId,
        task_no: TaskNo,
    ) -> Result<Option<(TaskEpoch, Option<String>, Value)>> {
        let r = self
            .read(|rec| {
                Box::pin(async move {
                    TASKS
                        .get::<TaskRec>(rec, &Pk::new().u64(sink.0).u32(task_no.0))
                        .await
                })
            })
            .await?;
        Ok(r.map(|r| (TaskEpoch(r.epoch), r.worker_id, r.source_part)))
    }

    /// The task's current epoch.
    ///
    /// # Errors
    /// The record store's.
    pub async fn task_epoch(&self, sink: SinkId, task_no: TaskNo) -> Result<Option<TaskEpoch>> {
        Ok(self.task_claim(sink, task_no).await?.map(|(e, _, _)| e))
    }

    /// Dead letters of a sink, oldest first.
    ///
    /// # Errors
    /// The record store's.
    pub async fn dead_letters(&self, sink: SinkId, limit: u32) -> Result<Vec<DeadRow>> {
        let rows = self
            .read(|rec| {
                Box::pin(async move { DEAD.scan::<DeadRec>(rec, &Pk::new().u64(sink.0)).await })
            })
            .await?;
        Ok(rows
            .into_iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|(pk, r)| DeadRow {
                id: dead_id(&pk),
                idem_key: r.idem_key,
                reason: r.reason,
                class: r.class,
                attempts: r.attempts,
                state: r.state,
                created_at: utc_text(r.created_at_ms).chars().take(19).collect(),
            })
            .collect())
    }

    // ---- task writes, each in a short transaction ----

    /// Deletes a dropped sink's state and task rows as they are now (after
    /// the drop's catalog change committed).
    ///
    /// # Errors
    /// The record store's.
    pub async fn forget_sink(&self, id: SinkId) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        for (pk, _) in TASKS.scan::<Value>(&mut *tx, &Pk::new().u64(id.0)).await? {
            TASKS.delete(&mut *tx, &pk).await?;
        }
        STATES.delete(&mut *tx, &Pk::new().u64(id.0)).await?;
        tx.commit().await
    }

    /// Sets a sink's state: tasks, and statements right after their change
    /// committed ([`crate::sql::after_commit`]). The state record is only
    /// ever written this way, so the last write wins, as on MariaDB.
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_state(
        &self,
        id: SinkId,
        state: SinkState,
        reason: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        Self::put_state(&mut *tx, id, state, reason).await?;
        tx.commit().await
    }

    /// Claims task `task_no` for `worker`: creates its row or bumps its
    /// epoch, and returns the new epoch (bumped on every claim).
    ///
    /// # Errors
    /// The record store's.
    pub async fn claim_task(
        &self,
        sink: SinkId,
        task_no: TaskNo,
        source_part: &Value,
        worker: &str,
    ) -> Result<TaskEpoch> {
        let pk = Pk::new().u64(sink.0).u32(task_no.0);
        let mut tx = self.records.begin(self.tenant).await?;
        let now = now_ms();
        let row = match TASKS.get_for_update::<TaskRec>(&mut *tx, &pk).await? {
            Some(mut r) => {
                r.epoch += 1;
                r.worker_id = Some(worker.to_owned());
                r.source_part = source_part.clone();
                r.state = TaskState::Running;
                r.updated_at_ms = now;
                r
            }
            None => TaskRec {
                source_part: source_part.clone(),
                epoch: 1,
                worker_id: Some(worker.to_owned()),
                pending: None,
                state: TaskState::Running,
                attempts: 0,
                last_error: None,
                last_error_at_ms: None,
                delivered: 0,
                updated_at_ms: now,
            },
        };
        TASKS.put(&mut *tx, &pk, &row).await?;
        tx.commit().await?;
        Ok(TaskEpoch(row.epoch))
    }

    /// Records a task's state, error and delivered count, under its epoch
    /// (a stale task writes nothing: `false`).
    ///
    /// # Errors
    /// The record store's.
    pub async fn report_task(
        &self,
        sink: SinkId,
        task_no: TaskNo,
        epoch: TaskEpoch,
        state: TaskState,
        error: Option<&str>,
        delivered: u64,
    ) -> Result<bool> {
        let pk = Pk::new().u64(sink.0).u32(task_no.0);
        let mut tx = self.records.begin(self.tenant).await?;
        let Some(mut r) = TASKS
            .get_for_update::<TaskRec>(&mut *tx, &pk)
            .await?
            .filter(|r| r.epoch == epoch.0)
        else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        let now = now_ms();
        r.state = state;
        r.delivered = r.delivered.saturating_add(delivered);
        match error {
            None => r.attempts = 0,
            Some(e) => {
                r.attempts += 1;
                r.last_error = Some(e.chars().take(2048).collect());
                r.last_error_at_ms = Some(now);
            }
        }
        r.updated_at_ms = now;
        TASKS.put(&mut *tx, &pk, &r).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Replaces the task's source part under its epoch (a `CHANGES OF`
    /// task's change position); a stale task writes nothing: `false`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn save_task_part(
        &self,
        sink: SinkId,
        task_no: TaskNo,
        epoch: TaskEpoch,
        part: &Value,
    ) -> Result<bool> {
        let pk = Pk::new().u64(sink.0).u32(task_no.0);
        let mut tx = self.records.begin(self.tenant).await?;
        let Some(mut r) = TASKS
            .get_for_update::<TaskRec>(&mut *tx, &pk)
            .await?
            .filter(|r| r.epoch == epoch.0)
        else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        r.source_part = part.clone();
        r.updated_at_ms = now_ms();
        TASKS.put(&mut *tx, &pk, &r).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Writes one dead letter.
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_dead(
        &self,
        sink: SinkId,
        rec: &crate::model::SinkRecord,
        info: &crate::runtime::DeadInfo,
    ) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        let id = next_ids(&mut *tx, DEAD_COUNTER, 1).await?;
        let row = DeadRec {
            idem_key: rec.idem_key.clone(),
            key: b64(rec.key.as_ref()),
            value: b64(rec.value.as_ref()),
            headers: rec.headers_json(),
            op: rec.op.map(|o| o.letter().to_string()),
            before: rec.before.clone(),
            after: rec.after.clone(),
            ts_ms: rec.ts_ms,
            position: STANDARD.encode(rec.pos.encode()),
            reason: info.reason.chars().take(2048).collect(),
            class: info.class.to_owned(),
            attempts: info.attempts,
            state: "dead".to_owned(),
            created_at_ms: now_ms(),
        };
        DEAD.put(&mut *tx, &Pk::new().u64(sink.0).u64(id), &row)
            .await?;
        tx.commit().await
    }

    /// Marks dead letters `replaying` (the task feeds them back) or
    /// `discarded`; `ids` empty means every `dead` one. Returns how many.
    ///
    /// # Errors
    /// The record store's.
    pub async fn mark_dead(&self, sink: SinkId, ids: &[u64], state: &str) -> Result<u64> {
        let mut tx = self.records.begin(self.tenant).await?;
        let mut n = 0;
        for (pk, mut r) in DEAD
            .scan::<DeadRec>(&mut *tx, &Pk::new().u64(sink.0))
            .await?
        {
            if r.state != "dead" || (!ids.is_empty() && !ids.contains(&dead_id(&pk))) {
                continue;
            }
            state.clone_into(&mut r.state);
            DEAD.put(&mut *tx, &pk, &r).await?;
            n += 1;
        }
        tx.commit().await?;
        Ok(n)
    }

    /// Takes up to `limit` dead letters marked `replaying`, as records, and
    /// marks them `replayed`.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn take_replays(
        &self,
        sink: SinkId,
        limit: u32,
    ) -> Result<Vec<crate::model::SinkRecord>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let mut out = Vec::new();
        for (pk, mut r) in DEAD
            .scan::<DeadRec>(&mut *tx, &Pk::new().u64(sink.0))
            .await?
        {
            if out.len() >= usize::try_from(limit).unwrap_or(usize::MAX) {
                break;
            }
            if r.state != "replaying" {
                continue;
            }
            let headers = r
                .headers
                .as_object()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| {
                    let b = match v {
                        Value::String(s) => s.into_bytes(),
                        other => other.to_string().into_bytes(),
                    };
                    (k, b)
                })
                .collect();
            let pos = STANDARD.decode(&r.position).map_err(|_| bad("position"))?;
            out.push(crate::model::SinkRecord {
                idem_key: r.idem_key.clone(),
                key: unb64(r.key.as_ref())?,
                value: unb64(r.value.as_ref())?,
                headers,
                op: r.op.as_deref().and_then(crate::model::ChangeOp::parse),
                before: r.before.clone(),
                after: r.after.clone(),
                ts_ms: r.ts_ms,
                pos: crate::model::Position::decode(&pos)
                    .ok_or_else(|| bad("dead letter position"))?,
            });
            "replayed".clone_into(&mut r.state);
            DEAD.put(&mut *tx, &pk, &r).await?;
        }
        tx.commit().await?;
        Ok(out)
    }
}

fn dead_id(pk: &Pk) -> u64 {
    pk.0.get(8..16)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u64::from_be_bytes)
}

/// A dead letter as listed.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadRow {
    /// Its id.
    pub id: u64,
    /// The record's idempotency key.
    pub idem_key: String,
    /// The reason.
    pub reason: String,
    /// The class.
    pub class: String,
    /// Attempts made.
    pub attempts: u32,
    /// `dead`, `replaying`, `replayed`, `discarded`.
    pub state: String,
    /// When (UTC text).
    pub created_at: String,
}

/// The module in the catalog cache: [`SinksState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct SinksExtension;

#[async_trait::async_trait]
impl CatalogExtension for SinksExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        SINKS_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        if !tx.catalog_records() {
            return Ok(Arc::new(SinksState::default()));
        }
        Ok(Arc::new(SinksStore::load(tx.records()?).await?))
    }
}

#[cfg(test)]
mod tests;
