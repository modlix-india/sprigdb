//! The catalog side of the DDL path: the rows a DDL reads and writes
//! inside a transaction holding the catalog version row ([`DdlTx`]), and
//! what the reconciler needs outside one ([`DdlBackend`]), over MariaDB
//! and the in-memory catalog of unit tests.

use std::time::SystemTime;

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::cache::mem::{MemSource, MemSourceTx};
use crate::pool_wait::Bounded as _;
use crate::records::RecordTx;
use crate::sql::{map_err, timed};
use crate::store::rec::CoreRecords;
use crate::store::{
    Counter, DatabaseRow, DbState, Namespace, ObjectRow, ObjectState, PurgeReport,
    SchemaVersionRow, SettingRow, SettingScope, Snapshot,
};
use crate::version::{Core, MariaBackend, MariaTx, VersionBackend, VersionKind};

/// Rows a DDL reads and writes inside an open change (or a locked
/// version row). Reads see the transaction's own writes.
#[async_trait::async_trait]
pub trait DdlTx: Send {
    /// The rows of a read-only query on the catalog schema, in which
    /// `{catalog}` stands for the schema's quoted name: other modules'
    /// tables, for [`PrepareCheck`](super::PrepareCheck)s. A missing table
    /// reads as no rows; `None` where the backend runs no SQL.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn query_rows(&mut self, _sql: &str) -> Result<Option<Vec<Vec<Option<String>>>>> {
        Ok(None)
    }

    /// The catalog's records inside this change (modules' rows on the
    /// catalog version), when the catalog is on records; `None` elsewhere
    /// (the in-memory catalog of unit tests, a MariaDB-only catalog).
    ///
    /// # Errors
    /// The store's.
    async fn catalog_records(&mut self) -> Result<Option<&mut dyn RecordTx>> {
        Ok(None)
    }

    /// One database by id, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn database(&mut self, db: DbId) -> Result<Option<DatabaseRow>>;

    /// The database holding the live name `name_key` (any state but
    /// `dropped`).
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn database_by_name(&mut self, name_key: &str) -> Result<Option<DatabaseRow>>;

    /// Every database, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn databases(&mut self) -> Result<Vec<DatabaseRow>>;

    /// One object by id, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn object(&mut self, object: ObjectId) -> Result<Option<ObjectRow>>;

    /// The object holding the live name `name_key` in `db` and
    /// `namespace`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn object_by_name(
        &mut self,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Result<Option<ObjectRow>>;

    /// Every object of `db`, tombstones included.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn objects_in(&mut self, db: DbId) -> Result<Vec<ObjectRow>>;

    /// Inserts a database; a live name in use is MariaDB's 1062.
    ///
    /// # Errors
    /// 1062, 39070 or another backend error.
    async fn insert_database(&mut self, r: &DatabaseRow) -> Result<()>;

    /// Replaces a database row.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn update_database(&mut self, r: &DatabaseRow) -> Result<()>;

    /// Deletes a database row that was never visible.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn delete_database(&mut self, db: DbId) -> Result<()>;

    /// Inserts an object; a live name in use is MariaDB's 1062.
    ///
    /// # Errors
    /// 1062, 39070 or another backend error.
    async fn insert_object(&mut self, r: &ObjectRow) -> Result<()>;

    /// Replaces an object row.
    ///
    /// # Errors
    /// 39070 or a backend error (1062 for a rename onto a live name).
    async fn update_object(&mut self, r: &ObjectRow) -> Result<()>;

    /// Deletes an object row that was never visible.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn delete_object(&mut self, object: ObjectId) -> Result<()>;

    /// The next id of `counter`, created at 1 if it is missing.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn next_id(&mut self, counter: Counter) -> Result<u64>;

    /// The newest schema version of an object.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn latest_schema_version(&mut self, object: ObjectId)
    -> Result<Option<SchemaVersionRow>>;

    /// Inserts a schema version.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn insert_schema_version(&mut self, r: &SchemaVersionRow) -> Result<()>;

    /// Every tenant-scope row whose key starts with `prefix`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn internal_rows(&mut self, prefix: &str) -> Result<Vec<SettingRow>>;

    /// Writes an internal tenant-scope row.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn put_internal(&mut self, row: &SettingRow) -> Result<()>;

    /// Deletes an internal tenant-scope row.
    ///
    /// # Errors
    /// 39070 or a backend error.
    async fn delete_internal(&mut self, key: &str) -> Result<()>;
}

/// A catalog backend the DDL path and the reconciler work through.
#[async_trait::async_trait]
pub trait DdlBackend: VersionBackend
where
    Self::Tx: DdlTx,
{
    /// Every row of `tenant`'s catalog (tombstones, transient rows and
    /// internal keys included) in one consistent snapshot.
    ///
    /// # Errors
    /// 39070 if the catalog is unreachable; 39072 if it isn't migrated.
    async fn full_snapshot(&self, tenant: TenantId) -> Result<Snapshot>;

    /// The committed catalog version and when its row was last written:
    /// a version committed no later than that time (the reconciler's
    /// mark for purging by age).
    ///
    /// # Errors
    /// As [`full_snapshot`](Self::full_snapshot).
    async fn version_mark(&self, tenant: TenantId) -> Result<(u64, SystemTime)>;

    /// Purges tombstones changed at or before `up_to` (clamped to the
    /// committed version), holding the version row; allocates nothing.
    ///
    /// # Errors
    /// As [`full_snapshot`](Self::full_snapshot); 39071 if the version
    /// row stays locked.
    async fn purge(&self, tenant: TenantId, up_to: u64) -> Result<PurgeReport>;
}

// ---- the catalog writer ----

/// One core call on a change's transaction: on records or on the catalog
/// schema.
macro_rules! core_call {
    ($tx:expr, |$r:ident| $rec:expr, |$s:ident, $q:ident| $sql:expr) => {
        match $tx.core() {
            Core::Rec($r) => $rec,
            Core::Sql($s, $q) => $sql,
        }
    };
}

#[async_trait::async_trait]
impl DdlTx for MariaTx {
    async fn query_rows(&mut self, sql: &str) -> Result<Option<Vec<Vec<Option<String>>>>> {
        use mysql_async::prelude::Queryable;
        let (st, q) = self.sql().await?;
        let sql = sql.replace("{catalog}", &sprig_common::names::quote(st.schema()));
        match q.query::<mysql_async::Row, _>(sql).await {
            Ok(rows) => Ok(Some(
                rows.into_iter()
                    .map(|r| {
                        r.unwrap()
                            .into_iter()
                            .map(|v| match v {
                                mysql_async::Value::NULL => None,
                                mysql_async::Value::Bytes(b) => {
                                    Some(String::from_utf8_lossy(&b).into_owned())
                                }
                                other => Some(other.as_sql(true)),
                            })
                            .collect()
                    })
                    .collect(),
            )),
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(Some(Vec::new())),
            Err(e) => Err(map_err(e)),
        }
    }

    async fn catalog_records(&mut self) -> Result<Option<&mut dyn RecordTx>> {
        if self.on_records() {
            self.records().await.map(Some)
        } else {
            Ok(None)
        }
    }

    async fn database(&mut self, db: DbId) -> Result<Option<DatabaseRow>> {
        core_call!(self, |r| CoreRecords::database(r, db).await, |s, q| s
            .database(q, db)
            .await)
    }

    async fn database_by_name(&mut self, name_key: &str) -> Result<Option<DatabaseRow>> {
        core_call!(
            self,
            |r| CoreRecords::database_by_name(r, name_key).await,
            |s, q| s.database_by_name(q, name_key).await
        )
    }

    async fn databases(&mut self) -> Result<Vec<DatabaseRow>> {
        core_call!(
            self,
            |r| CoreRecords::databases_since(r, None).await,
            |s, q| s.databases_since(q, None).await
        )
    }

    async fn object(&mut self, object: ObjectId) -> Result<Option<ObjectRow>> {
        core_call!(self, |r| CoreRecords::object(r, object).await, |s, q| s
            .object(q, object)
            .await)
    }

    async fn object_by_name(
        &mut self,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Result<Option<ObjectRow>> {
        core_call!(
            self,
            |r| CoreRecords::object_by_name(r, db, namespace, name_key).await,
            |s, q| s.object_by_name(q, db, namespace, name_key).await
        )
    }

    async fn objects_in(&mut self, db: DbId) -> Result<Vec<ObjectRow>> {
        core_call!(self, |r| CoreRecords::objects_in(r, db).await, |s, q| s
            .objects_in(q, db)
            .await)
    }

    async fn insert_database(&mut self, row: &DatabaseRow) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::insert_database(r, row).await,
            |s, q| s.insert_database(q, row).await
        )
    }

    async fn update_database(&mut self, row: &DatabaseRow) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::update_database(r, row).await.map(drop),
            |s, q| s.update_database(q, row).await.map(drop)
        )
    }

    async fn delete_database(&mut self, db: DbId) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::delete_database(r, db).await.map(drop),
            |s, q| s.delete_database(q, db).await.map(drop)
        )
    }

    async fn insert_object(&mut self, row: &ObjectRow) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::insert_object(r, row).await,
            |s, q| s.insert_object(q, row).await
        )
    }

    async fn update_object(&mut self, row: &ObjectRow) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::update_object(r, row).await.map(drop),
            |s, q| s.update_object(q, row).await.map(drop)
        )
    }

    async fn delete_object(&mut self, object: ObjectId) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::delete_object(r, object).await.map(drop),
            |s, q| s.delete_object(q, object).await.map(drop)
        )
    }

    async fn next_id(&mut self, counter: Counter) -> Result<u64> {
        match self.core() {
            Core::Rec(r) => CoreRecords::next_ids(r, counter, 1).await,
            Core::Sql(s, q) => match s.next_ids(q, counter, 1).await {
                Err(e) if e.code() == Some(SprigCode::TenantNotReady) => {
                    s.ensure_counter(q, counter).await?;
                    s.next_ids(q, counter, 1).await
                }
                r => r,
            },
        }
    }

    async fn latest_schema_version(
        &mut self,
        object: ObjectId,
    ) -> Result<Option<SchemaVersionRow>> {
        core_call!(
            self,
            |r| CoreRecords::latest_schema_version(r, object).await,
            |s, q| s.latest_schema_version(q, object).await
        )
    }

    async fn insert_schema_version(&mut self, row: &SchemaVersionRow) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::insert_schema_version(r, row).await,
            |s, q| s.insert_schema_version(q, row).await
        )
    }

    async fn internal_rows(&mut self, prefix: &str) -> Result<Vec<SettingRow>> {
        let rows = core_call!(
            self,
            |r| CoreRecords::settings_of(r, SettingScope::Tenant).await,
            |s, q| s.settings_of(q, SettingScope::Tenant).await
        )?;
        Ok(rows
            .into_iter()
            .filter(|r| r.key.starts_with(prefix))
            .collect())
    }

    async fn put_internal(&mut self, row: &SettingRow) -> Result<()> {
        core_call!(self, |r| CoreRecords::put_setting(r, row).await, |s, q| s
            .put_setting(q, row)
            .await)
    }

    async fn delete_internal(&mut self, key: &str) -> Result<()> {
        core_call!(
            self,
            |r| CoreRecords::delete_internal(r, key).await.map(drop),
            |s, q| s.delete_internal(q, key).await.map(drop)
        )
    }
}

#[async_trait::async_trait]
impl DdlBackend for MariaBackend {
    async fn full_snapshot(&self, tenant: TenantId) -> Result<Snapshot> {
        if let Some(r) = self.records() {
            let mut tx = r.begin(tenant).await?;
            let snap = CoreRecords::snapshot(&mut *tx, None).await;
            let _ = tx.rollback().await;
            return snap;
        }
        let ep = self.locator().writer(tenant).await?;
        timed(
            self.opts().statement_timeout * 4,
            "snapshot",
            ep.store.snapshot(&ep.pool, None),
        )
        .await
    }

    async fn version_mark(&self, tenant: TenantId) -> Result<(u64, SystemTime)> {
        if let Some(r) = self.records() {
            let mut tx = r.begin(tenant).await?;
            let mark = CoreRecords::version_mark(&mut *tx).await;
            let _ = tx.rollback().await;
            return mark;
        }
        let ep = self.locator().writer(tenant).await?;
        timed(self.opts().statement_timeout, "version mark", async {
            let mut conn = ep.pool.get_conn_bounded().await.map_err(map_err)?;
            let row = ep
                .store
                .setting(&mut conn, SettingScope::Tenant, VersionKind::Catalog.key())
                .await?
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::TenantNotReady,
                        "the tenant's catalog is not bootstrapped",
                    )
                })?;
            let v = row
                .value
                .as_ref()
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| SprigError::internal("sprig.catalog_version is not a number"))?;
            Ok((v, row.set_at))
        })
        .await
    }

    async fn purge(&self, tenant: TenantId, up_to: u64) -> Result<PurgeReport> {
        let (mut tx, committed) = self.begin_locked(tenant, VersionKind::Catalog).await?;
        let up_to = up_to.min(committed);
        let report = timed(self.opts().statement_timeout * 4, "purge", async {
            core_call!(tx, |r| CoreRecords::purge(r, up_to).await, |s, q| s
                .purge(q, up_to)
                .await)
        })
        .await;
        match report {
            Ok(r) => {
                // Committing at the same version allocates nothing.
                self.commit(tx, VersionKind::Catalog, committed).await?;
                Ok(r)
            }
            Err(e) => {
                let _ = self.rollback(tx).await;
                Err(e)
            }
        }
    }
}

// ---- in memory ----

fn live_db(d: &DatabaseRow) -> bool {
    d.state != DbState::Dropped
}

fn live_obj(o: &ObjectRow) -> bool {
    o.state != ObjectState::Dropped
}

fn duplicate(what: &str) -> SprigError {
    SprigError::backend(1062, *b"23000", format!("duplicate live name: {what}"))
}

#[async_trait::async_trait]
impl DdlTx for MemSourceTx {
    async fn database(&mut self, db: DbId) -> Result<Option<DatabaseRow>> {
        Ok(self.current().databases.get(&db).cloned())
    }

    async fn database_by_name(&mut self, name_key: &str) -> Result<Option<DatabaseRow>> {
        Ok(self
            .current()
            .databases
            .into_values()
            .find(|d| live_db(d) && d.name_key == name_key))
    }

    async fn databases(&mut self) -> Result<Vec<DatabaseRow>> {
        Ok(self.current().databases.into_values().collect())
    }

    async fn object(&mut self, object: ObjectId) -> Result<Option<ObjectRow>> {
        Ok(self.current().objects.get(&object).cloned())
    }

    async fn object_by_name(
        &mut self,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Result<Option<ObjectRow>> {
        Ok(self.current().objects.into_values().find(|o| {
            live_obj(o) && o.db_id == db && o.namespace == namespace && o.name_key == name_key
        }))
    }

    async fn objects_in(&mut self, db: DbId) -> Result<Vec<ObjectRow>> {
        Ok(self
            .current()
            .objects
            .into_values()
            .filter(|o| o.db_id == db)
            .collect())
    }

    async fn insert_database(&mut self, r: &DatabaseRow) -> Result<()> {
        let t = self.current();
        if t.databases.contains_key(&r.db_id) {
            return Err(duplicate("db_id"));
        }
        if live_db(r)
            && t.databases
                .values()
                .any(|d| live_db(d) && d.name_key == r.name_key)
        {
            return Err(duplicate(&r.name_key));
        }
        self.put_database(r.clone());
        Ok(())
    }

    async fn update_database(&mut self, r: &DatabaseRow) -> Result<()> {
        let t = self.current();
        if live_db(r)
            && t.databases
                .values()
                .any(|d| d.db_id != r.db_id && live_db(d) && d.name_key == r.name_key)
        {
            return Err(duplicate(&r.name_key));
        }
        self.put_database(r.clone());
        Ok(())
    }

    async fn delete_database(&mut self, db: DbId) -> Result<()> {
        MemSourceTx::delete_database(self, db);
        Ok(())
    }

    async fn insert_object(&mut self, r: &ObjectRow) -> Result<()> {
        if self.current().objects.contains_key(&r.object_id) {
            return Err(duplicate("object_id"));
        }
        self.update_object(r).await
    }

    async fn update_object(&mut self, r: &ObjectRow) -> Result<()> {
        let t = self.current();
        if live_obj(r)
            && t.objects.values().any(|o| {
                o.object_id != r.object_id
                    && live_obj(o)
                    && o.db_id == r.db_id
                    && o.namespace == r.namespace
                    && o.name_key == r.name_key
            })
        {
            return Err(duplicate(&r.name_key));
        }
        self.put_object(r.clone());
        Ok(())
    }

    async fn delete_object(&mut self, object: ObjectId) -> Result<()> {
        MemSourceTx::delete_object(self, object);
        Ok(())
    }

    async fn next_id(&mut self, counter: Counter) -> Result<u64> {
        let key = counter.key();
        let next = self.current().counters.get(&key).copied().unwrap_or(1);
        self.set_counter(&key, next + 1);
        Ok(next)
    }

    async fn latest_schema_version(
        &mut self,
        object: ObjectId,
    ) -> Result<Option<SchemaVersionRow>> {
        Ok(self
            .current()
            .schema_versions
            .range((object, 0)..=(object, u64::MAX))
            .next_back()
            .map(|(_, r)| r.clone()))
    }

    async fn insert_schema_version(&mut self, r: &SchemaVersionRow) -> Result<()> {
        if self
            .current()
            .schema_versions
            .contains_key(&(r.object_id, r.version))
        {
            return Err(duplicate("schema version"));
        }
        self.put_schema_version(r.clone());
        Ok(())
    }

    async fn internal_rows(&mut self, prefix: &str) -> Result<Vec<SettingRow>> {
        Ok(self
            .current()
            .settings
            .into_iter()
            .filter(|((s, k), _)| *s == SettingScope::Tenant && k.starts_with(prefix))
            .map(|(_, r)| r)
            .collect())
    }

    async fn put_internal(&mut self, row: &SettingRow) -> Result<()> {
        MemSourceTx::put_setting(self, row.clone());
        Ok(())
    }

    async fn delete_internal(&mut self, key: &str) -> Result<()> {
        self.delete_setting(SettingScope::Tenant, key);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DdlBackend for MemSource {
    async fn full_snapshot(&self, tenant: TenantId) -> Result<Snapshot> {
        let t = self.tables(tenant).ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("no catalog for tenant {tenant}"),
            )
        })?;
        Ok(t.snapshot(None))
    }

    async fn version_mark(&self, tenant: TenantId) -> Result<(u64, SystemTime)> {
        let t = self.tables(tenant).ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!("no catalog for tenant {tenant}"),
            )
        })?;
        Ok((
            t.versions.catalog,
            t.version_set_at.unwrap_or(SystemTime::UNIX_EPOCH),
        ))
    }

    async fn purge(&self, tenant: TenantId, up_to: u64) -> Result<PurgeReport> {
        let before = self.tables(tenant);
        MemSource::purge(self, tenant, up_to).await?;
        let after = self.tables(tenant);
        Ok(match (before, after) {
            (Some(b), Some(a)) => PurgeReport {
                databases: (b.databases.len() - a.databases.len()) as u64,
                objects: (b.objects.len() - a.objects.len()) as u64,
                settings: (b.settings.len() - a.settings.len()) as u64,
                schema_versions: (b.schema_versions.len() - a.schema_versions.len()) as u64,
            },
            _ => PurgeReport::default(),
        })
    }
}
