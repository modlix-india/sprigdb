//! Contract migrations: the DC leader's pass that runs a catalog module's
//! contract migrations once nothing running needs what they remove, and
//! the Admin API's audited run before `catalog.contract_delay_days`.
//!
//! ```text
//! every PASS_INTERVAL, on the DC leader (and on POST .../contracts:run):
//!   modules = every module this build reads (readers::declared)
//!   pending = the modules with a contract migration
//!   none pending -> done (no catalog is read)
//!   scan = every DC's reader records and running processes (readers::scan)
//!   ceiling(m) = the oldest format a reader of m reads; 0 while a running
//!                process has no reader record
//!   runnable = pending modules with a contract at or under their ceiling
//!   per active tenant homed here (or the one named):
//!     read sprig.catalog_format; skip modules the catalog doesn't have,
//!     or has at the ceiling already
//!     run the migrator on those modules: expands as far as they go,
//!     contracts up to the ceiling, catalog.contract_delay_days (live) and
//!     the run's override, if any (the migrator records it)
//! ```
//!
//! The override only shortens the delay: a contract still never runs past
//! a registered reader's format, since that reader would then fail with
//! 39073. A catalog a mover's cutover holds is skipped this pass (39070).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_catalog::migrate::{
    self, ContractOverride, MigrateOpts, Module, Phase, contract_delay_of_days,
};
use sprig_catalog::readers::{self, ModuleCeiling, ReaderScan};
use sprig_catalog::{CatalogLocator, Migrator};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::time::Clock;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};
use crate::tree::{Directory, DirectoryEntry, TenantState};

/// How often the leader's pass runs.
pub const PASS_INTERVAL: Duration = Duration::from_secs(600);

/// Every catalog module the controller's crates know; the `sprigdb`
/// binary declares them with the gateway's at start
/// ([`declare_known`]).
#[must_use]
pub fn known_modules() -> Vec<Module> {
    vec![
        sprig_auth::store::AUTH_MODULE,
        sprig_auth::authz::AUTHZ_MODULE,
        sprig_auth::authz::rls::RLS_MODULE,
        sprig_inherit::store::INHERIT_MODULE,
        sprig_inherit::msg::store::MSG_MODULE,
        sprig_shard::groups::SHARDING_MODULE,
        sprig_ai::dashboards::DASHBOARD_MODULE,
        sprig_ai::audit::AI_MODULE,
        sprig_ai::glossary::GLOSSARY_MODULE,
        sprig_ai::digest::store::DIGEST_MODULE,
        sprig_ai::embedding::store::EMBEDDING_MODULE,
        sprig_ai::retrieval::store::RETRIEVAL_MODULE,
        sprig_ai::functions::cache::FN_MODULE,
        sprig_workload::store::WORKLOAD_MODULE,
        sprig_advisor::sharding::runs::ADVISOR_RUNS_MODULE,
        sprig_advisor::autoindex::store::AUTOINDEX_MODULE,
        sprig_logic::catalog::LOGIC_MODULE,
        sprig_queues::catalog::MQ_MODULE,
        sprig_approvals::catalog::APPROVALS_MODULE,
        sprig_balancer::decisions::DECISIONS_MODULE,
        sprig_sinks::store::SINKS_MODULE,
    ]
}

/// Declares [`known_modules`] as read by this process
/// ([`sprig_catalog::readers`]).
pub fn declare_known() {
    readers::declare(&[migrate::CORE_MODULE]);
    readers::declare(&known_modules());
}

/// Declares `modules` as read by this process, for crates that don't
/// depend on `sprig-catalog` (the `sprigdb` binary).
pub fn declare(modules: &[Module]) {
    readers::declare(modules);
}

/// `GET /admin/v1/catalog/contracts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractsView {
    /// `catalog.contract_delay_days` in force.
    pub contract_delay_days: u64,
    /// Every registered reader, as `<role> <node> (dc <dc>)`.
    pub readers: Vec<String>,
    /// Running processes without a reader record (they hold every
    /// contract back).
    pub unregistered: Vec<String>,
    /// Every module this build knows, with its contracts and ceiling.
    pub modules: Vec<ModuleCeiling>,
}

/// `POST /admin/v1/catalog/contracts:run`: the audited run before the
/// delay is over.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunContractsRequest {
    /// Why the delay is cut short (required).
    #[serde(default)]
    pub reason: String,
    /// Who decided, such as `op:alice` (required).
    #[serde(default)]
    pub operator: String,
    /// Only this tenant (by name); every tenant homed in this DC otherwise.
    #[serde(default)]
    pub tenant: Option<String>,
}

/// One tenant's part of a run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantContracts {
    /// The tenant.
    pub tenant: String,
    /// Migrations applied, as `<module> <format> <phase> <name>`.
    pub applied: Vec<String>,
    /// Contracts the delay held back, as `<module> <format> (due <time>)`.
    pub delayed: Vec<String>,
    /// Why the tenant was skipped this pass (a held catalog, an error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What a run did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractRunView {
    /// Per module, the format contracts may reach now (only modules with a
    /// contract a reader allows).
    pub runnable: BTreeMap<String, u32>,
    /// Tenants whose catalogs were looked at.
    pub tenants: Vec<TenantContracts>,
    /// Why nothing ran, when nothing could.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// The pass (module docs).
pub struct ContractPass {
    dir: Directory,
    locator: Arc<dyn CatalogLocator>,
    global: Arc<dyn MetaStore>,
    dc_store: Arc<dyn MetaStore>,
    dc: DcId,
    live: Option<LiveSettings>,
    clock: Arc<dyn Clock>,
    /// The modules to look at; `None`: every module the process declared.
    modules: Option<Vec<Module>>,
}

impl std::fmt::Debug for ContractPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContractPass")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

impl ContractPass {
    /// The pass over the tenants homed in `dc`.
    #[must_use]
    pub fn new(
        dir: Directory,
        locator: Arc<dyn CatalogLocator>,
        global: Arc<dyn MetaStore>,
        dc_store: Arc<dyn MetaStore>,
        dc: DcId,
        live: Option<LiveSettings>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            dir,
            locator,
            global,
            dc_store,
            dc,
            live,
            clock,
            modules: None,
        }
    }

    /// The same pass over `modules` only (tests).
    #[must_use]
    pub fn with_modules(mut self, modules: Vec<Module>) -> Self {
        self.modules = Some(modules);
        self
    }

    fn modules(&self) -> Vec<Module> {
        self.modules.clone().unwrap_or_else(readers::declared)
    }

    fn delay_days(&self) -> u64 {
        self.live
            .as_ref()
            .map_or(sprig_catalog::settings::CONTRACT_DELAY_DAYS.default, |l| {
                l.get(
                    &sprig_catalog::settings::CONTRACT_DELAY_DAYS,
                    &SettingCtx::process(&self.dc),
                )
            })
    }

    async fn scan(&self) -> Result<ReaderScan> {
        readers::scan(self.global.as_ref(), &self.dc_store).await
    }

    /// The readers and every module's ceiling.
    ///
    /// # Errors
    /// The stores' errors.
    pub async fn status(&self) -> Result<ContractsView> {
        let scan = self.scan().await?;
        Ok(ContractsView {
            contract_delay_days: self.delay_days(),
            readers: scan
                .readers
                .iter()
                .map(|(dc, r)| format!("{} {} (dc {dc})", r.role, r.node))
                .collect(),
            modules: readers::contract_ceilings(&scan, &self.modules()),
            unregistered: scan.unregistered,
        })
    }

    /// Runs the contracts the readers allow (module docs), with `over`
    /// letting them run before the delay is over; only `tenant` (by name)
    /// when given.
    ///
    /// # Errors
    /// The stores' errors; an unknown tenant. A tenant's failure is
    /// reported in its entry, not as the run's error.
    pub async fn run(
        &self,
        over: Option<&ContractOverride>,
        tenant: Option<&str>,
    ) -> Result<ContractRunView> {
        let modules = self.modules();
        if !modules
            .iter()
            .any(|m| m.migrations.iter().any(|x| x.phase == Phase::Contract))
        {
            return Ok(ContractRunView {
                note: Some("no catalog module of this build has a contract migration".into()),
                ..ContractRunView::default()
            });
        }
        let scan = self.scan().await?;
        let ceilings = readers::contract_ceilings(&scan, &modules);
        let runnable: BTreeMap<String, u32> = ceilings
            .iter()
            .filter(|c| c.contracts.iter().any(|f| *f <= c.contract_ceiling))
            .map(|c| (c.module.clone(), c.contract_ceiling))
            .collect();
        if runnable.is_empty() {
            let held: Vec<String> = ceilings
                .iter()
                .filter(|c| !c.contracts.is_empty())
                .map(|c| format!("{}: {}", c.module, c.held_by.join("; ")))
                .collect();
            return Ok(ContractRunView {
                note: Some(format!(
                    "no contract migration is allowed by the registered readers ({})",
                    held.join(" | ")
                )),
                ..ContractRunView::default()
            });
        }
        let entries: Vec<DirectoryEntry> = match tenant {
            Some(name) => vec![self.dir.require(name).await?.body().clone()],
            None => self
                .dir
                .list()
                .await?
                .iter()
                .map(|e| e.body().clone())
                .filter(|e| e.home_dc == self.dc && e.state == TenantState::Active)
                .collect(),
        };
        let mut out = ContractRunView {
            runnable: runnable.clone(),
            ..ContractRunView::default()
        };
        let delay = contract_delay_of_days(self.delay_days());
        for e in entries {
            let mut t = TenantContracts {
                tenant: e.name.clone(),
                ..TenantContracts::default()
            };
            if let Err(err) = self
                .tenant(e.tenant_id, &modules, &runnable, delay, over, &mut t)
                .await
            {
                tracing::warn!(tenant = %e.name, error = %err, "contract migrations failed for a tenant; next pass retries");
                t.error = Some(err.to_string());
            }
            out.tenants.push(t);
        }
        Ok(out)
    }

    async fn tenant(
        &self,
        id: TenantId,
        modules: &[Module],
        runnable: &BTreeMap<String, u32>,
        delay: Duration,
        over: Option<&ContractOverride>,
        t: &mut TenantContracts,
    ) -> Result<()> {
        crate::catalogs::not_held(&*self.locator, id).await?;
        let ep = self.locator.writer(id).await?;
        let schema = ep.store.schema().to_owned();
        let mut conn = crate::catalogs::acquire(&ep.pool, "contract migrations").await?;
        let state = migrate::read_state(&mut conn, &schema).await?;
        // Only modules the catalog has, below a contract their ceiling
        // allows: a module the tenant never used gets its tables when it
        // first needs them, as before.
        let due: Vec<Module> = modules
            .iter()
            .filter(|m| {
                let (Some(&ceiling), Some(have)) = (runnable.get(m.name), state.get(m.name)) else {
                    return false;
                };
                m.migrations.iter().any(|x| {
                    x.phase == Phase::Contract && x.format > have.format && x.format <= ceiling
                })
            })
            .copied()
            .collect();
        if due.is_empty() {
            return Ok(());
        }
        let opts = MigrateOpts {
            ceilings: due
                .iter()
                .map(|m| {
                    (
                        m.name.to_owned(),
                        readers::ceiling(runnable.get(m.name).copied().unwrap_or(0)),
                    )
                })
                .collect(),
            applied_by: format!("controller:{}/contracts", self.dc),
            contract_delay: delay,
            contract_override: over.cloned(),
            ..MigrateOpts::existing()
        };
        let report = Migrator::new(&due)?
            .with_clock(Arc::clone(&self.clock))
            .run(&mut conn, &schema, &opts)
            .await?;
        t.applied = report
            .applied
            .iter()
            .map(|r| format!("{} {} {:?} {}", r.module, r.format, r.phase, r.name).to_lowercase())
            .collect();
        t.delayed = report
            .delayed
            .iter()
            .map(|d| {
                format!(
                    "{} {} (due {})",
                    d.migration.module,
                    d.migration.format,
                    d.due
                        .as_deref()
                        .unwrap_or("unknown: no expand time recorded")
                )
            })
            .collect();
        if !t.applied.is_empty() {
            tracing::info!(tenant = %t.tenant, applied = ?t.applied, "contract migrations applied");
        }
        Ok(())
    }
}

/// [`ContractOverride::new`] for an Admin API request.
///
/// # Errors
/// 39084 for a blank reason or operator.
pub fn override_of(req: &RunContractsRequest) -> Result<ContractOverride> {
    ContractOverride::new(&req.reason, &req.operator).map_err(|e| {
        SprigError::new(
            SprigCode::SettingBadValue,
            format!("{}: pass reason and operator", e.message),
        )
    })
}

/// The leader's loop over the pass.
pub struct ContractLoop(pub Arc<ContractPass>);

impl std::fmt::Debug for ContractLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ContractLoop").finish()
    }
}

#[async_trait::async_trait]
impl Reconciler for ContractLoop {
    type Key = String;

    fn name(&self) -> &'static str {
        "catalog_contracts"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<String> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<String>> {
        Ok(vec!["contracts".to_owned()])
    }

    async fn reconcile(&self, _cx: &LoopCx, _key: &String) -> Result<Outcome> {
        let r = self.0.run(None, None).await?;
        for t in r.tenants.iter().filter(|t| t.error.is_some()) {
            tracing::debug!(tenant = %t.tenant, error = ?t.error, "contract pass skipped a tenant");
        }
        Ok(Outcome::RequeueAfter(PASS_INTERVAL))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn an_override_needs_a_reason_and_an_operator() {
        let mut req = RunContractsRequest {
            reason: "hotfix".into(),
            operator: " ".into(),
            tenant: None,
        };
        let e = override_of(&req).unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 39084, "{e}");
        req.operator = "op:alice".into();
        let o = override_of(&req).unwrap();
        assert_eq!((o.reason(), o.by()), ("hotfix", "op:alice"));
        req.reason = String::new();
        assert_eq!(override_of(&req).unwrap_err().protocol().mysql_errno, 39084);
    }

    #[test]
    fn known_modules_are_valid_and_distinct() {
        // Migrator::new refuses a repeated or malformed module.
        Migrator::new(&known_modules()).unwrap();
    }
}
