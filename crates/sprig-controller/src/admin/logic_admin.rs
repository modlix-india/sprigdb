//! The Admin API's logic views (`sprigctl logic list|show|logs|dead-letters`),
//! over the tenants' logic catalogs (engine records under `m/logic/`).
//!
//! ```text
//! GET  /admin/v1/tenants/{name}/logic/modules               every module
//! GET  /admin/v1/tenants/{name}/logic/modules/{module}      one, by name, with its versions
//! GET  /admin/v1/tenants/{name}/logic/logs?module=&after=&limit=
//! GET  /admin/v1/tenants/{name}/logic/dead-letters?binding=&state=&limit=
//! POST /admin/v1/tenants/{name}/logic/dead-letters:replay   {"binding": n?, "ids": [..]}
//! POST /admin/v1/tenants/{name}/logic/dead-letters:discard  {"binding": n?, "ids": [..]}
//! ```
//!
//! An empty `ids` means every dead letter (of the binding, when named).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_catalog::RecordStore;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::LogicStore;
use sprig_logic::catalog::{DeadLetterRow, LogRow, ModuleRow};

use super::AdminService;

/// The longest page of log lines or dead letters.
const MAX_ROWS: u32 = 1000;

/// A dead-letter change's body.
#[derive(Debug, Default, Deserialize)]
pub struct DeadLetterChange {
    /// Only this binding's (`None`: any binding's).
    #[serde(default)]
    pub binding: Option<u64>,
    /// The dead letters; empty: all of them.
    #[serde(default)]
    pub ids: Vec<u64>,
}

/// `GET .../logic/logs`' query.
#[derive(Debug, Default, Deserialize)]
pub struct LogsQuery {
    /// One module's lines, by name.
    pub module: Option<String>,
    /// Lines after this id.
    pub after: Option<String>,
    /// At most this many (50).
    pub limit: Option<String>,
}

/// `GET .../logic/dead-letters`' query.
#[derive(Debug, Default, Deserialize)]
pub struct DeadLettersQuery {
    /// One binding's.
    pub binding: Option<String>,
    /// `dead`, `replaying`, `replayed` or `discarded`.
    pub state: Option<String>,
    /// At most this many (100).
    pub limit: Option<String>,
}

/// A number from a query string (`None` when absent).
///
/// # Errors
/// 39792 when it isn't one.
pub fn num<T: std::str::FromStr>(v: Option<&String>, name: &str) -> Result<Option<T>> {
    v.map(|s| {
        s.parse::<T>()
            .map_err(|_| super::invalid(format!("{name}: not a number")))
    })
    .transpose()
}

/// What a replay did.
#[derive(Debug, Serialize)]
pub struct Replayed {
    /// Queued for their trigger's job.
    pub replaying: u64,
    /// Dead letters of triggers that can't replay (left dead).
    pub skipped: u64,
}

impl AdminService {
    /// Serves the logic views from `records`.
    #[must_use]
    pub fn with_logic(mut self, records: Arc<dyn RecordStore>) -> Self {
        self.logic = Some(records);
        self
    }

    async fn logic_tenant(&self, tenant: &str) -> Result<(Arc<dyn RecordStore>, TenantId)> {
        let records = self.logic.clone().ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                "this controller has no logic views",
            )
        })?;
        let t = self.get_tenant(tenant).await?.tenant_id;
        Ok((records, t))
    }

    /// `sprigctl logic list`.
    ///
    /// # Errors
    /// An unknown tenant; the catalog records'.
    pub async fn logic_modules(&self, tenant: &str) -> Result<Vec<ModuleRow>> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let rows = LogicStore::default().modules(&mut *tx, None).await?;
        tx.rollback().await?;
        Ok(rows)
    }

    /// `sprigctl logic show <name>`: the first live module of that name,
    /// with its versions.
    ///
    /// # Errors
    /// 39860 for no such module; an unknown tenant; the catalog records'.
    pub async fn logic_module(&self, tenant: &str, name: &str) -> Result<Value> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let store = LogicStore::default();
        let module = store
            .modules(&mut *tx, None)
            .await?
            .into_iter()
            .find(|m| m.name == name)
            .ok_or_else(|| {
                SprigError::new(SprigCode::LogicNotFound, format!("no logic module {name}"))
            })?;
        let versions = store.versions(&mut *tx, module.module_id).await?;
        tx.rollback().await?;
        Ok(json!({"module": module, "versions": versions}))
    }

    /// `sprigctl logic logs`: lines after `after`, oldest first, of one
    /// module (by name) or of all.
    ///
    /// # Errors
    /// 39860 for no such module; an unknown tenant; the catalog records'.
    pub async fn logic_logs(
        &self,
        tenant: &str,
        module: Option<&str>,
        after: u64,
        limit: u32,
    ) -> Result<Vec<LogRow>> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let store = LogicStore::default();
        let module_id = match module {
            Some(n) => Some(
                store
                    .modules(&mut *tx, None)
                    .await?
                    .into_iter()
                    .find(|m| m.name == n)
                    .ok_or_else(|| {
                        SprigError::new(SprigCode::LogicNotFound, format!("no logic module {n}"))
                    })?
                    .module_id,
            ),
            None => None,
        };
        let rows = store
            .logs(&mut *tx, module_id, after, limit.clamp(1, MAX_ROWS))
            .await?;
        tx.rollback().await?;
        Ok(rows)
    }

    /// `sprigctl logic dead-letters list`.
    ///
    /// # Errors
    /// An unknown tenant; the catalog records'.
    pub async fn logic_dead_letters(
        &self,
        tenant: &str,
        binding: Option<u64>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<DeadLetterRow>> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let rows = LogicStore::default()
            .dead_letters(&mut *tx, binding, state, limit.clamp(1, MAX_ROWS))
            .await?;
        tx.rollback().await?;
        Ok(rows)
    }

    /// `sprigctl logic dead-letters replay`: back to their trigger's job,
    /// which runs them in order with the active version.
    ///
    /// # Errors
    /// An unknown tenant; the catalog records'.
    pub async fn logic_replay(&self, tenant: &str, c: &DeadLetterChange) -> Result<Replayed> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let (replaying, skipped) = LogicStore::default()
            .replay_dead_letters(&mut *tx, c.binding, &c.ids)
            .await?;
        tx.commit().await?;
        Ok(Replayed { replaying, skipped })
    }

    /// `sprigctl logic dead-letters discard`.
    ///
    /// # Errors
    /// An unknown tenant; the catalog records'.
    pub async fn logic_discard(&self, tenant: &str, c: &DeadLetterChange) -> Result<Value> {
        let (records, t) = self.logic_tenant(tenant).await?;
        let mut tx = records.begin(t).await?;
        let n = LogicStore::default()
            .set_dead_letters(&mut *tx, c.binding, &c.ids, "dead", "discarded")
            .await?;
        tx.commit().await?;
        Ok(json!({"discarded": n}))
    }
}
