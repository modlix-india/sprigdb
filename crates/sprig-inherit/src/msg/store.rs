//! The `inherit_msg` catalog module, where definitions and instances live,
//! kept as the engine's catalog records, the snapshot loader
//! ([`MsgExtension`]) and the writes.
//!
//! ```text
//! m/inherit_msg/defs/<object_id u64>                                  Definition
//! m/inherit_msg/overrides/<def tenant u64><def object u64>
//!                         <target tenant u64><applies_to u8><what u8> Override
//! m/inherit_msg/instances/<def tenant u64><def object u64>            InstanceRow (not versioned)
//! ```
//!
//! Definitions and overrides are versioned with the catalog: they are
//! written inside a catalog change (the associated functions of
//! [`MsgStore`] on the change's records). Instance rows are written outside
//! one (they are not versioned), each in a short engine transaction of its
//! own ([`MsgStore`]'s methods); the member's catalog change that creates
//! the instance's physical object (a stream's row, a trigger's binding)
//! comes right after, so the next snapshot has both.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::model::{
    AppliesTo, DefKey, Definition, Instance, InstanceState, MsgState, Override, What, db_key,
};

/// The module name.
pub const MODULE: &str = "inherit_msg";

/// Definitions, in the declaring tenant.
pub const DEFS: RowTable = RowTable::new(MODULE, "defs");
/// Overrides, in a member or an intermediate tenant.
pub const OVERRIDES: RowTable = RowTable::new(MODULE, "overrides");
/// Instances, in the member; not versioned.
pub const INSTANCES: RowTable = RowTable::new(MODULE, "instances");

/// The module's MariaDB migrations: none, its rows are engine records.
pub static MSG_MIGRATIONS: &[Migration] = &[];

/// The `inherit_msg` module, for [`Migrator::new`].
pub const MSG_MODULE: Module = Module {
    name: MODULE,
    migrations: MSG_MIGRATIONS,
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

/// An instance row: the instance and when it was last used.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceRow {
    /// The instance.
    #[serde(flatten)]
    pub instance: Instance,
    /// Last used (ms since the epoch).
    pub last_used_at_ms: u64,
}

fn def_pk(def: DefKey) -> Pk {
    Pk::new().u64(def.tenant.0).u64(def.object)
}

fn applies_byte(a: AppliesTo) -> u8 {
    match a {
        AppliesTo::Own => 1,
        AppliesTo::Subtree => 2,
    }
}

fn what_byte(w: What) -> u8 {
    match w {
        What::Settings => 1,
        What::Body => 2,
        What::Options => 3,
        What::Credentials => 4,
        What::State => 5,
    }
}

/// Milliseconds since the epoch as MariaDB's `DATETIME(3)` text.
#[must_use]
pub fn datetime(ms: u64) -> String {
    let secs = i64::try_from(ms / 1000).unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rest = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant's algorithm).
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
        rest / 3600,
        rest / 60 % 60,
        rest % 60,
        ms % 1000
    )
}

/// Now, ms since the epoch.
#[must_use]
pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

/// The module's records of one tenant: versioned writes inside a catalog
/// change's records (associated functions), instance rows in short
/// transactions of their own (methods).
#[derive(Clone)]
pub struct MsgStore {
    records: Arc<dyn RecordStore>,
    tenant: TenantId,
}

impl std::fmt::Debug for MsgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsgStore")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

impl MsgStore {
    /// `tenant`'s records in `records`.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>, tenant: TenantId) -> Self {
        Self { records, tenant }
    }

    /// The whole state of tenant `tenant`.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx, tenant: TenantId) -> Result<MsgState> {
        let mut st = MsgState {
            migrated: true,
            ..MsgState::default()
        };
        for mut d in DEFS.rows::<Definition>(rec, &Pk::new()).await? {
            if d.state.as_str() == "dropped" {
                continue;
            }
            d.declarer = tenant;
            st.defs
                .insert((db_key(&d.db_name), d.kind, d.name.clone()), Arc::new(d));
        }
        for mut o in OVERRIDES.rows::<Override>(rec, &Pk::new()).await? {
            o.made_in = tenant;
            st.overrides.push(Arc::new(o));
        }
        for r in INSTANCES.rows::<InstanceRow>(rec, &Pk::new()).await? {
            st.instances.insert(r.instance.def, Arc::new(r.instance));
        }
        Ok(st)
    }

    /// Writes (or replaces) a definition, at catalog version `version`. As
    /// `REPLACE` did, a definition holding the same name (database, kind,
    /// name) under another id is replaced too.
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_def(rec: &mut dyn RecordTx, d: &Definition, version: u64) -> Result<()> {
        for (pk, o) in DEFS.scan::<Definition>(rec, &Pk::new()).await? {
            if o.object_id != d.object_id
                && o.db_name == d.db_name
                && o.kind == d.kind
                && o.name == d.name
            {
                DEFS.delete(rec, &pk).await?;
            }
        }
        let mut row = d.clone();
        row.changed_version = version;
        DEFS.put(rec, &Pk::new().u64(d.object_id), &row).await
    }

    /// Deletes a definition.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_def(rec: &mut dyn RecordTx, object_id: u64) -> Result<()> {
        DEFS.delete(rec, &Pk::new().u64(object_id)).await
    }

    /// Writes (or replaces) an override made in this tenant.
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_override(rec: &mut dyn RecordTx, o: &Override, _version: u64) -> Result<()> {
        let pk = def_pk(o.def)
            .u64(o.target.0)
            .u8(applies_byte(o.applies_to))
            .u8(what_byte(o.what));
        let mut row = o.clone();
        row.set_at_ms = now_ms();
        OVERRIDES.put(rec, &pk, &row).await
    }

    /// Deletes overrides made in this tenant: of `what` (every kind when
    /// `None`) for `target` and `applies_to`. Returns how many.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_override(
        rec: &mut dyn RecordTx,
        def: DefKey,
        target: TenantId,
        applies_to: AppliesTo,
        what: Option<What>,
    ) -> Result<u64> {
        let mut under = def_pk(def).u64(target.0).u8(applies_byte(applies_to));
        if let Some(w) = what {
            under = under.u8(what_byte(w));
        }
        let mut n = 0;
        for (pk, _) in OVERRIDES.scan::<Value>(rec, &under).await? {
            OVERRIDES.delete(rec, &pk).await?;
            n += 1;
        }
        Ok(n)
    }

    // ---- instances: each call one short transaction ----

    /// Every instance row.
    ///
    /// # Errors
    /// The record store's.
    pub async fn instances(&self) -> Result<Vec<Instance>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let r = INSTANCES.rows::<InstanceRow>(&mut *tx, &Pk::new()).await;
        let _ = tx.rollback().await;
        Ok(r?.into_iter().map(|r| r.instance).collect())
    }

    /// One instance row.
    ///
    /// # Errors
    /// The record store's.
    pub async fn instance(&self, def: DefKey) -> Result<Option<Instance>> {
        let mut tx = self.records.begin(self.tenant).await?;
        let r = INSTANCES.get::<InstanceRow>(&mut *tx, &def_pk(def)).await;
        let _ = tx.rollback().await;
        Ok(r?.map(|r| r.instance))
    }

    /// Claims an instance: writes its row (state `creating`) unless one
    /// exists for the definition or under the instance id (two gateways
    /// racing: one claim wins). Returns whether this call wrote it.
    ///
    /// # Errors
    /// The record store's.
    pub async fn claim(&self, i: &Instance) -> Result<bool> {
        let mut tx = self.records.begin(self.tenant).await?;
        let pk = def_pk(i.def);
        if INSTANCES
            .get_for_update::<InstanceRow>(&mut *tx, &pk)
            .await?
            .is_some()
            || INSTANCES
                .rows::<InstanceRow>(&mut *tx, &Pk::new())
                .await?
                .iter()
                .any(|r| r.instance.instance_id == i.instance_id)
        {
            let _ = tx.rollback().await;
            return Ok(false);
        }
        let now = now_ms();
        let mut row = InstanceRow {
            instance: i.clone(),
            last_used_at_ms: now,
        };
        row.instance.created_at_ms = now;
        INSTANCES.put(&mut *tx, &pk, &row).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Takes over a `creating` row nobody finished since `older_than_ms`
    /// (a gateway that died while creating): moves its creation time to now
    /// if it is still the old one. Returns whether this call took it.
    ///
    /// # Errors
    /// The record store's.
    pub async fn take_over(&self, def: DefKey, older_than_ms: u64) -> Result<bool> {
        let mut tx = self.records.begin(self.tenant).await?;
        let pk = def_pk(def);
        let now = now_ms();
        let Some(mut r) = INSTANCES
            .get_for_update::<InstanceRow>(&mut *tx, &pk)
            .await?
            .filter(|r| {
                r.instance.state == InstanceState::Creating
                    && r.instance.created_at_ms < now.saturating_sub(older_than_ms)
            })
        else {
            let _ = tx.rollback().await;
            return Ok(false);
        };
        r.instance.created_at_ms = now;
        INSTANCES.put(&mut *tx, &pk, &r).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Sets an instance's state, version and definition copy (and its
    /// physical object, when given).
    ///
    /// # Errors
    /// The record store's.
    pub async fn set_instance(
        &self,
        def: DefKey,
        state: InstanceState,
        def_version: u64,
        definition: &Value,
        physical: Option<&Value>,
    ) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        let pk = def_pk(def);
        let Some(mut r) = INSTANCES
            .get_for_update::<InstanceRow>(&mut *tx, &pk)
            .await?
        else {
            let _ = tx.rollback().await;
            return Ok(());
        };
        r.instance.state = state;
        r.instance.def_version = def_version;
        r.instance.definition = definition.clone();
        if let Some(p) = physical {
            r.instance.physical = Some(p.clone());
        }
        r.last_used_at_ms = now_ms();
        INSTANCES.put(&mut *tx, &pk, &r).await?;
        tx.commit().await
    }

    /// Sets (or clears, `None`) `blocked` in the physical object of the
    /// trigger instance whose binding is `binding`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn mark_blocked(&self, binding: u64, why: Option<&str>) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        for (pk, mut r) in INSTANCES.scan::<InstanceRow>(&mut *tx, &Pk::new()).await? {
            let hit = r.instance.kind.as_str() == "trigger"
                && r.instance
                    .physical
                    .as_ref()
                    .and_then(|p| p.get("binding"))
                    .and_then(|b| b.as_u64().or_else(|| b.as_str()?.parse().ok()))
                    == Some(binding);
            if !hit {
                continue;
            }
            if let Some(Value::Object(p)) = r.instance.physical.as_mut() {
                match why {
                    Some(w) => {
                        p.insert("blocked".to_owned(), Value::String(w.to_owned()));
                    }
                    None => {
                        p.remove("blocked");
                    }
                }
                INSTANCES.put(&mut *tx, &pk, &r).await?;
            }
        }
        tx.commit().await
    }

    /// Deletes an instance row.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_instance(&self, def: DefKey) -> Result<()> {
        let mut tx = self.records.begin(self.tenant).await?;
        INSTANCES.delete(&mut *tx, &def_pk(def)).await?;
        tx.commit().await
    }
}

/// The module in the catalog cache: [`MsgState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgExtension;

#[async_trait::async_trait]
impl CatalogExtension for MsgExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        MSG_MIGRATIONS
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
            return Ok(Arc::new(MsgState::default()));
        }
        let tenant = tenant_of_schema(tx.schema());
        Ok(Arc::new(MsgStore::load(tx.records()?, tenant).await?))
    }
}

/// `t<tenant>_sprig` → the tenant.
#[must_use]
pub fn tenant_of_schema(schema: &str) -> TenantId {
    TenantId(
        schema
            .strip_prefix('t')
            .and_then(|s| s.strip_suffix("_sprig"))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
    )
}

#[cfg(test)]
mod tests;
