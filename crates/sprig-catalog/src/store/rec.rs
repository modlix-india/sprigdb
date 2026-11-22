//! The catalog's four core tables as engine catalog records
//! ([`crate::records`]): the same calls as [`Store`](super::Store), on a
//! [`RecordTx`] instead of a MariaDB connection.
//!
//! ```text
//! m/core/databases/<db u32>                          a database row (JSON)
//! m/core/dnames/<name key>                           a live database's id (u32 BE)
//! m/core/objects/<object u64>                        an object row (JSON)
//! m/core/onames/<db u32><namespace u8><name key>     a live object's id (u64 BE)
//! m/core/settings/<scope u8><scope id u64><key>      a settings row (JSON)
//! m/core/sv/<object u64><version u64>                a schema version (JSON)
//! chg/<version u64><relative key>                    the change index (empty value)
//! v/catalog/, v/auth/                                committed versions (records::version_key)
//! v/purged/                                          the purged version (u64 BE)
//! ctr/core.db/, ctr/core.object/, ctr/core.<name>/   id counters (records::next_ids)
//! ```
//!
//! Name keys are byte strings in the prefix-free form of
//! [`Pk::bytes`](crate::records::Pk::bytes), so a prefix scan returns rows
//! in key order. The rules MariaDB's keys enforced are kept here:
//! - **Live names are unique** (`databases.live_name`, `objects` by database,
//!   namespace and `live_name`): a live row's name is a record of its own,
//!   read for update before a row takes it; a taken name is 1062, as the
//!   unique key gave. A dropped row (a tombstone) holds no name.
//! - **Ids are never reused:** counters only move up; an insert of an id
//!   that has a row is 1062 (the primary key).
//! - **Incremental loads:** every write of a core row also writes
//!   `chg/<version><key>` for the version the row records. A load since `s`
//!   reads the index from `s + 1` and keeps the rows whose version is above
//!   `s`, which is what `WHERE changed_version > s` returned. [`purge`]
//!   removes index entries at or below the purged version, as loads from
//!   before it reload in full.
//!
//! Every writer of these rows holds the catalog version record
//! (`v/catalog/`, read for update by the change), so writes are serial as
//! they were under MariaDB's version-row lock; the name records are still
//! read for update, so a reader never needs the version lock to be exact.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sprig_common::ids::{DbId, GroupId, JobId, ObjectId, UserId};
use sprig_common::{Result, SprigError};
use sprig_settings::Bounds;

use super::{
    Counter, DatabaseRow, DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
    PurgeReport, SchemaVersionRow, SettingRow, SettingScope, Snapshot, Versions,
};
use crate::records::{self, Pk, RecordTx, prefix_end, scan_all};
use crate::version::VersionKind;

const DATABASES: &[u8] = b"m/core/databases/";
const DNAMES: &[u8] = b"m/core/dnames/";
const OBJECTS: &[u8] = b"m/core/objects/";
const ONAMES: &[u8] = b"m/core/onames/";
const SETTINGS: &[u8] = b"m/core/settings/";
const SCHEMA_VERSIONS: &[u8] = b"m/core/sv/";
const CHANGES: &[u8] = b"chg/";
const PURGED: &[u8] = b"v/purged/";

fn key(prefix: &[u8], pk: &Pk) -> Vec<u8> {
    let mut k = prefix.to_vec();
    k.extend_from_slice(&pk.0);
    k
}

fn db_key(db: DbId) -> Vec<u8> {
    key(DATABASES, &Pk::new().u32(db.0))
}

fn dname_key(name_key: &str) -> Vec<u8> {
    key(DNAMES, &Pk::new().bytes(name_key.as_bytes()))
}

fn obj_key(object: ObjectId) -> Vec<u8> {
    key(OBJECTS, &Pk::new().u64(object.0))
}

fn ns_code(ns: Namespace) -> u8 {
    Namespace::ALL
        .iter()
        .position(|n| *n == ns)
        .and_then(|i| u8::try_from(i).ok())
        .unwrap_or(u8::MAX)
}

fn oname_key(db: DbId, ns: Namespace, name_key: &str) -> Vec<u8> {
    key(
        ONAMES,
        &Pk::new()
            .u32(db.0)
            .u8(ns_code(ns))
            .bytes(name_key.as_bytes()),
    )
}

/// The scope's place in key order (tenant, database, object, ancestor:
/// the order of [`SettingScope`]).
fn scope_pk(scope: SettingScope) -> Pk {
    let (code, id) = match scope {
        SettingScope::Tenant => (0, 0),
        SettingScope::Database(d) => (1, u64::from(d.0)),
        SettingScope::Object(o) => (2, o.0),
        SettingScope::Ancestor(t) => (3, t.0),
    };
    Pk::new().u8(code).u64(id)
}

fn setting_key(scope: SettingScope, k: &str) -> Vec<u8> {
    key(SETTINGS, &scope_pk(scope).bytes(k.as_bytes()))
}

fn sv_key(object: ObjectId, version: u64) -> Vec<u8> {
    key(SCHEMA_VERSIONS, &Pk::new().u64(object.0).u64(version))
}

fn chg_key(version: u64, rel: &[u8]) -> Vec<u8> {
    let mut k = CHANGES.to_vec();
    k.extend_from_slice(&version.to_be_bytes());
    k.extend_from_slice(rel);
    k
}

fn counter_name(c: Counter) -> String {
    match c {
        Counter::Database => "core.db".to_owned(),
        Counter::Object => "core.object".to_owned(),
        Counter::Other(n) => format!("core.{n}"),
    }
}

fn micros(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

fn time(us: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us)
}

fn duplicate(what: &str) -> SprigError {
    SprigError::backend(1062, *b"23000", format!("Duplicate entry '{what}'"))
}

fn bad(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("catalog record {what} doesn't decode: {e}"))
}

fn be_u64(raw: &[u8]) -> Option<u64> {
    raw.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

fn encode<T: Serialize>(what: &str, row: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(row)
        .map_err(|e| SprigError::internal(format!("catalog record {what} doesn't encode: {e}")))
}

fn decode<T: for<'de> Deserialize<'de>>(what: &str, raw: &[u8]) -> Result<T> {
    serde_json::from_slice(raw).map_err(|e| bad(what, e))
}

// ---- rows as stored ----

#[derive(Serialize, Deserialize)]
struct DbRec {
    id: u32,
    name: String,
    key: String,
    charset: String,
    collation: String,
    group: u32,
    state: String,
    options: Json,
    created: u64,
    by: Option<u32>,
    v: u64,
}

impl DbRec {
    fn of(r: &DatabaseRow) -> Self {
        Self {
            id: r.db_id.0,
            name: r.name.clone(),
            key: r.name_key.clone(),
            charset: r.default_charset.clone(),
            collation: r.default_collation.clone(),
            group: r.default_group_id.0,
            state: r.state.as_str().to_owned(),
            options: r.options.clone(),
            created: micros(r.created_at),
            by: r.created_by.map(|u| u.0),
            v: r.changed_version,
        }
    }

    fn row(self) -> Result<DatabaseRow> {
        Ok(DatabaseRow {
            db_id: DbId(self.id),
            name: self.name,
            name_key: self.key,
            default_charset: self.charset,
            default_collation: self.collation,
            default_group_id: GroupId(self.group),
            state: DbState::parse(&self.state)?,
            options: self.options,
            created_at: time(self.created),
            created_by: self.by.map(UserId),
            changed_version: self.v,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct ObjRec {
    id: u64,
    db: u32,
    ns: String,
    name: String,
    key: String,
    kind: String,
    parent: Option<u64>,
    state: String,
    sv: u64,
    flags: String,
    job: Option<JobId>,
    created: u64,
    v: u64,
}

impl ObjRec {
    fn of(r: &ObjectRow) -> Self {
        Self {
            id: r.object_id.0,
            db: r.db_id.0,
            ns: r.namespace.as_str().to_owned(),
            name: r.name.clone(),
            key: r.name_key.clone(),
            kind: r.kind.as_str().to_owned(),
            parent: r.parent_id.map(|p| p.0),
            state: r.state.as_str().to_owned(),
            sv: r.schema_version,
            flags: r.flags.to_sql(),
            job: r.pending_job_id,
            created: micros(r.created_at),
            v: r.changed_version,
        }
    }

    fn row(self) -> Result<ObjectRow> {
        Ok(ObjectRow {
            object_id: ObjectId(self.id),
            db_id: DbId(self.db),
            namespace: Namespace::parse(&self.ns)?,
            name: self.name,
            name_key: self.key,
            kind: ObjectKind::parse(&self.kind)?,
            parent_id: self.parent.map(ObjectId),
            state: ObjectState::parse(&self.state)?,
            schema_version: self.sv,
            flags: ObjectFlags::parse(&self.flags)?,
            pending_job_id: self.job,
            created_at: time(self.created),
            changed_version: self.v,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct SetRec {
    scope: String,
    sid: u64,
    key: String,
    value: Option<Json>,
    bounds: Option<Bounds>,
    by: String,
    at: u64,
    v: u64,
}

impl SetRec {
    fn of(r: &SettingRow) -> Self {
        let (scope, sid) = r.scope.to_sql();
        Self {
            scope: scope.to_owned(),
            sid,
            key: r.key.clone(),
            value: r.value.clone(),
            bounds: r.bounds.clone(),
            by: r.set_by.clone(),
            at: micros(r.set_at),
            v: r.version,
        }
    }

    fn row(self) -> Result<SettingRow> {
        Ok(SettingRow {
            scope: SettingScope::parse(&self.scope, self.sid)?,
            key: self.key,
            value: self.value,
            bounds: self.bounds,
            set_by: self.by,
            set_at: time(self.at),
            version: self.v,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct SvRec {
    object: u64,
    version: u64,
    ddl: String,
    sql: Option<String>,
    hash: Option<String>,
    job: Option<JobId>,
    by: Option<u32>,
    at: u64,
    v: u64,
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        let pair = std::str::from_utf8(&b[i * 2..i * 2 + 2]).ok()?;
        *o = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

impl SvRec {
    fn of(r: &SchemaVersionRow) -> Self {
        Self {
            object: r.object_id.0,
            version: r.version,
            ddl: r.ddl.clone(),
            sql: r.create_sql.clone(),
            hash: r.create_sql_hash.map(|h| hex(&h)),
            job: r.ddl_job_id,
            by: r.applied_by.map(|u| u.0),
            at: micros(r.applied_at),
            v: r.changed_version,
        }
    }

    fn row(self) -> Result<SchemaVersionRow> {
        let hash = match self.hash {
            Some(h) => Some(unhex32(&h).ok_or_else(|| bad("schema version hash", &h))?),
            None => None,
        };
        Ok(SchemaVersionRow {
            object_id: ObjectId(self.object),
            version: self.version,
            ddl: self.ddl,
            create_sql: self.sql,
            create_sql_hash: hash,
            ddl_job_id: self.job,
            applied_by: self.by.map(UserId),
            applied_at: time(self.at),
            changed_version: self.v,
        })
    }
}

// ---- reads and writes ----

/// The core tables on a tenant's catalog records. Every call runs in the
/// [`RecordTx`] it is given; reads see that transaction's own writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoreRecords;

async fn put(tx: &mut dyn RecordTx, k: &[u8], v: &[u8], version: u64) -> Result<()> {
    tx.put(k, v).await?;
    tx.put(&chg_key(version, k), b"").await
}

async fn rows_under(tx: &mut dyn RecordTx, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    scan_all(tx, prefix, &prefix_end(prefix)).await
}

impl CoreRecords {
    // ---- versions and counters ----

    /// The committed catalog and auth versions and the purged version at
    /// the read view (0 for any never written: a catalog with no change
    /// yet).
    ///
    /// # Errors
    /// The store's; `Internal` for a record that doesn't decode.
    pub async fn versions(tx: &mut dyn RecordTx) -> Result<Versions> {
        Ok(Versions {
            catalog: records::read_version(tx, VersionKind::Catalog).await?,
            auth: records::read_version(tx, VersionKind::Auth).await?,
            purged: Self::purged(tx).await?,
        })
    }

    async fn purged(tx: &mut dyn RecordTx) -> Result<u64> {
        match tx.get(PURGED).await? {
            Some(raw) => be_u64(&raw).ok_or_else(|| bad("purged version", "short")),
            None => Ok(0),
        }
    }

    /// The committed catalog version and when it was committed (the
    /// reconciler's mark for purging by age); the epoch for a catalog with
    /// no change yet.
    ///
    /// # Errors
    /// The store's; `Internal` for a record that doesn't decode.
    pub async fn version_mark(tx: &mut dyn RecordTx) -> Result<(u64, SystemTime)> {
        let key = records::version_key(VersionKind::Catalog);
        let raw = match tx.get(&key).await? {
            Some(raw) => Some(raw),
            None => tx.get(key.strip_suffix(b"/").unwrap_or(&key)).await?,
        };
        let Some(raw) = raw else {
            return Ok((0, UNIX_EPOCH));
        };
        let v = be_u64(&raw).ok_or_else(|| bad("version", "short"))?;
        let at = raw.get(8..).and_then(be_u64).map_or(UNIX_EPOCH, time);
        Ok((v, at))
    }

    /// Reserves `n` ids from `counter`; the first of them (ids start at 1,
    /// never reused). The counter stays locked until the transaction ends.
    ///
    /// # Errors
    /// As [`records::next_ids`].
    pub async fn next_ids(tx: &mut dyn RecordTx, counter: Counter, n: u64) -> Result<u64> {
        if n == 0 {
            return Err(SprigError::internal("reserving zero ids"));
        }
        records::next_ids(tx, &counter_name(counter), n).await
    }

    // ---- databases ----

    /// One database by id, tombstones included.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn database(tx: &mut dyn RecordTx, db: DbId) -> Result<Option<DatabaseRow>> {
        match tx.get(&db_key(db)).await? {
            Some(raw) => decode::<DbRec>("database", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    /// The live (not dropped) database with this name key.
    ///
    /// # Errors
    /// As [`database`](Self::database).
    pub async fn database_by_name(
        tx: &mut dyn RecordTx,
        name_key: &str,
    ) -> Result<Option<DatabaseRow>> {
        let Some(raw) = tx.get(&dname_key(name_key)).await? else {
            return Ok(None);
        };
        let id = raw
            .get(..4)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
            .map(u32::from_be_bytes)
            .ok_or_else(|| bad("database name", "short"))?;
        Ok(Self::database(tx, DbId(id))
            .await?
            .filter(|d| d.state != DbState::Dropped && d.name_key == name_key))
    }

    /// Databases changed after `since` (every database for `None`), by id.
    ///
    /// # Errors
    /// As [`database`](Self::database).
    pub async fn databases_since(
        tx: &mut dyn RecordTx,
        since: Option<u64>,
    ) -> Result<Vec<DatabaseRow>> {
        let mut out = Vec::new();
        match since {
            None => {
                for (_, raw) in rows_under(tx, DATABASES).await? {
                    out.push(decode::<DbRec>("database", &raw)?.row()?);
                }
            }
            Some(s) => {
                for k in changed_keys(tx, s, DATABASES).await? {
                    if let Some(raw) = tx.get(&k).await? {
                        let r = decode::<DbRec>("database", &raw)?.row()?;
                        if r.changed_version > s {
                            out.push(r);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Inserts a database. A live name already in use, or an id with a
    /// row, is 1062.
    ///
    /// # Errors
    /// 1062; the store's.
    pub async fn insert_database(tx: &mut dyn RecordTx, r: &DatabaseRow) -> Result<()> {
        let k = db_key(r.db_id);
        if tx.get_for_update(&k).await?.is_some() {
            return Err(duplicate(&format!("{}' for key 'PRIMARY", r.db_id.0)));
        }
        if r.state != DbState::Dropped {
            Self::claim_dname(tx, &r.name_key, r.db_id).await?;
        }
        put(
            tx,
            &k,
            &encode("database", &DbRec::of(r))?,
            r.changed_version,
        )
        .await
    }

    async fn check_dname(tx: &mut dyn RecordTx, name_key: &str, db: DbId) -> Result<()> {
        if let Some(raw) = tx.get_for_update(&dname_key(name_key)).await?
            && raw.get(..4) != Some(&db.0.to_be_bytes()[..])
        {
            return Err(duplicate(&format!("{name_key}' for key 'live_name")));
        }
        Ok(())
    }

    async fn claim_dname(tx: &mut dyn RecordTx, name_key: &str, db: DbId) -> Result<()> {
        Self::check_dname(tx, name_key, db).await?;
        tx.put(&dname_key(name_key), &db.0.to_be_bytes()).await
    }

    async fn release_dname(tx: &mut dyn RecordTx, name_key: &str, db: DbId) -> Result<()> {
        let nk = dname_key(name_key);
        if let Some(raw) = tx.get_for_update(&nk).await?
            && raw.get(..4) == Some(&db.0.to_be_bytes()[..])
        {
            tx.delete(&nk).await?;
        }
        Ok(())
    }

    async fn write_database(
        tx: &mut dyn RecordTx,
        old: &DatabaseRow,
        new: DatabaseRow,
    ) -> Result<()> {
        let old_live = old.state != DbState::Dropped;
        let new_live = new.state != DbState::Dropped;
        // A refused write changes nothing (a failed statement's effect).
        if new_live {
            Self::check_dname(tx, &new.name_key, new.db_id).await?;
        }
        if old_live && (!new_live || old.name_key != new.name_key) {
            Self::release_dname(tx, &old.name_key, old.db_id).await?;
        }
        if new_live {
            Self::claim_dname(tx, &new.name_key, new.db_id).await?;
        }
        put(
            tx,
            &db_key(new.db_id),
            &encode("database", &DbRec::of(&new))?,
            new.changed_version,
        )
        .await
    }

    async fn database_for_update(tx: &mut dyn RecordTx, db: DbId) -> Result<Option<DatabaseRow>> {
        match tx.get_for_update(&db_key(db)).await? {
            Some(raw) => decode::<DbRec>("database", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    /// Updates every column of a database but its id and creation fields.
    /// Returns whether the row exists.
    ///
    /// # Errors
    /// 1062 for a live name in use; the store's.
    pub async fn update_database(tx: &mut dyn RecordTx, r: &DatabaseRow) -> Result<bool> {
        let Some(old) = Self::database_for_update(tx, r.db_id).await? else {
            return Ok(false);
        };
        let new = DatabaseRow {
            created_at: old.created_at,
            created_by: old.created_by,
            ..r.clone()
        };
        Self::write_database(tx, &old, new).await?;
        Ok(true)
    }

    /// Moves a database to `state` (a drop: `Dropped`, which frees the
    /// name). Returns whether the row exists.
    ///
    /// # Errors
    /// The store's.
    pub async fn set_database_state(
        tx: &mut dyn RecordTx,
        db: DbId,
        state: DbState,
        version: u64,
    ) -> Result<bool> {
        let Some(old) = Self::database_for_update(tx, db).await? else {
            return Ok(false);
        };
        let new = DatabaseRow {
            state,
            changed_version: version,
            ..old.clone()
        };
        Self::write_database(tx, &old, new).await?;
        Ok(true)
    }

    /// Deletes a database row that was never visible, with its settings
    /// rows. Returns whether it existed.
    ///
    /// # Errors
    /// The store's.
    pub async fn delete_database(tx: &mut dyn RecordTx, db: DbId) -> Result<bool> {
        Self::delete_scope(tx, SettingScope::Database(db)).await?;
        let Some(old) = Self::database_for_update(tx, db).await? else {
            return Ok(false);
        };
        if old.state != DbState::Dropped {
            Self::release_dname(tx, &old.name_key, db).await?;
        }
        tx.delete(&db_key(db)).await?;
        Ok(true)
    }

    // ---- objects ----

    /// One object by id, tombstones included.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn object(tx: &mut dyn RecordTx, object: ObjectId) -> Result<Option<ObjectRow>> {
        match tx.get(&obj_key(object)).await? {
            Some(raw) => decode::<ObjRec>("object", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    async fn object_for_update(
        tx: &mut dyn RecordTx,
        object: ObjectId,
    ) -> Result<Option<ObjectRow>> {
        match tx.get_for_update(&obj_key(object)).await? {
            Some(raw) => decode::<ObjRec>("object", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    /// The live object with this name key in `db` and `namespace`.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn object_by_name(
        tx: &mut dyn RecordTx,
        db: DbId,
        namespace: Namespace,
        name_key: &str,
    ) -> Result<Option<ObjectRow>> {
        let Some(raw) = tx.get(&oname_key(db, namespace, name_key)).await? else {
            return Ok(None);
        };
        let id = be_u64(&raw).ok_or_else(|| bad("object name", "short"))?;
        Ok(Self::object(tx, ObjectId(id)).await?.filter(|o| {
            o.state != ObjectState::Dropped
                && o.db_id == db
                && o.namespace == namespace
                && o.name_key == name_key
        }))
    }

    /// Objects changed after `since` (every object for `None`), by id.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn objects_since(
        tx: &mut dyn RecordTx,
        since: Option<u64>,
    ) -> Result<Vec<ObjectRow>> {
        let mut out = Vec::new();
        match since {
            None => {
                for (_, raw) in rows_under(tx, OBJECTS).await? {
                    out.push(decode::<ObjRec>("object", &raw)?.row()?);
                }
            }
            Some(s) => {
                for k in changed_keys(tx, s, OBJECTS).await? {
                    if let Some(raw) = tx.get(&k).await? {
                        let r = decode::<ObjRec>("object", &raw)?.row()?;
                        if r.changed_version > s {
                            out.push(r);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Every object of `db`, tombstones included, by id.
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn objects_in(tx: &mut dyn RecordTx, db: DbId) -> Result<Vec<ObjectRow>> {
        Ok(Self::objects_since(tx, None)
            .await?
            .into_iter()
            .filter(|o| o.db_id == db)
            .collect())
    }

    /// The objects among `ids`, tombstones included, by id (ids with no
    /// row are left out).
    ///
    /// # Errors
    /// As [`object`](Self::object).
    pub async fn objects_by_ids(tx: &mut dyn RecordTx, ids: &[ObjectId]) -> Result<Vec<ObjectRow>> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(o) = Self::object(tx, id).await? {
                out.push(o);
            }
        }
        Ok(out)
    }

    async fn check_oname(tx: &mut dyn RecordTx, o: &ObjectRow) -> Result<()> {
        let nk = oname_key(o.db_id, o.namespace, &o.name_key);
        if let Some(raw) = tx.get_for_update(&nk).await?
            && be_u64(&raw) != Some(o.object_id.0)
        {
            return Err(duplicate(&format!("{}' for key 'live_name", o.name_key)));
        }
        Ok(())
    }

    async fn claim_oname(tx: &mut dyn RecordTx, o: &ObjectRow) -> Result<()> {
        Self::check_oname(tx, o).await?;
        let nk = oname_key(o.db_id, o.namespace, &o.name_key);
        tx.put(&nk, &o.object_id.0.to_be_bytes()).await
    }

    async fn release_oname(tx: &mut dyn RecordTx, o: &ObjectRow) -> Result<()> {
        let nk = oname_key(o.db_id, o.namespace, &o.name_key);
        if let Some(raw) = tx.get_for_update(&nk).await?
            && be_u64(&raw) == Some(o.object_id.0)
        {
            tx.delete(&nk).await?;
        }
        Ok(())
    }

    async fn write_object(
        tx: &mut dyn RecordTx,
        old: Option<&ObjectRow>,
        new: &ObjectRow,
    ) -> Result<()> {
        let new_live = new.state != ObjectState::Dropped;
        // A refused write changes nothing (a failed statement's effect).
        if new_live {
            Self::check_oname(tx, new).await?;
        }
        if let Some(old) = old
            && old.state != ObjectState::Dropped
            && (!new_live
                || old.db_id != new.db_id
                || old.namespace != new.namespace
                || old.name_key != new.name_key)
        {
            Self::release_oname(tx, old).await?;
        }
        if new_live {
            Self::claim_oname(tx, new).await?;
        }
        put(
            tx,
            &obj_key(new.object_id),
            &encode("object", &ObjRec::of(new))?,
            new.changed_version,
        )
        .await
    }

    /// Inserts an object. A live name already in use in its database and
    /// namespace, or an id with a row, is 1062.
    ///
    /// # Errors
    /// `Internal` if `namespace` doesn't match `kind`; 1062; the store's.
    pub async fn insert_object(tx: &mut dyn RecordTx, r: &ObjectRow) -> Result<()> {
        super::check_namespace(r)?;
        if tx.get_for_update(&obj_key(r.object_id)).await?.is_some() {
            return Err(duplicate(&format!("{}' for key 'PRIMARY", r.object_id.0)));
        }
        Self::write_object(tx, None, r).await
    }

    /// Updates every column of an object but its id and creation time.
    /// Returns whether the row exists.
    ///
    /// # Errors
    /// As [`insert_object`](Self::insert_object).
    pub async fn update_object(tx: &mut dyn RecordTx, r: &ObjectRow) -> Result<bool> {
        super::check_namespace(r)?;
        let Some(old) = Self::object_for_update(tx, r.object_id).await? else {
            return Ok(false);
        };
        let new = ObjectRow {
            created_at: old.created_at,
            ..r.clone()
        };
        Self::write_object(tx, Some(&old), &new).await?;
        Ok(true)
    }

    /// Moves an object to `state` (a drop: `Dropped`). Returns whether the
    /// row exists.
    ///
    /// # Errors
    /// The store's.
    pub async fn set_object_state(
        tx: &mut dyn RecordTx,
        object: ObjectId,
        state: ObjectState,
        version: u64,
    ) -> Result<bool> {
        let Some(old) = Self::object_for_update(tx, object).await? else {
            return Ok(false);
        };
        let new = ObjectRow {
            state,
            changed_version: version,
            ..old.clone()
        };
        Self::write_object(tx, Some(&old), &new).await?;
        Ok(true)
    }

    /// Deletes an object row that was never visible, with its settings
    /// rows and schema versions. Returns whether it existed.
    ///
    /// # Errors
    /// The store's.
    pub async fn delete_object(tx: &mut dyn RecordTx, object: ObjectId) -> Result<bool> {
        Self::delete_scope(tx, SettingScope::Object(object)).await?;
        Self::delete_schema_versions(tx, object).await?;
        let Some(old) = Self::object_for_update(tx, object).await? else {
            return Ok(false);
        };
        if old.state != ObjectState::Dropped {
            Self::release_oname(tx, &old).await?;
        }
        tx.delete(&obj_key(object)).await?;
        Ok(true)
    }

    // ---- settings ----

    /// One settings row.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn setting(
        tx: &mut dyn RecordTx,
        scope: SettingScope,
        k: &str,
    ) -> Result<Option<SettingRow>> {
        match tx.get(&setting_key(scope, k)).await? {
            Some(raw) => decode::<SetRec>("setting", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    /// Inserts or replaces a settings row.
    ///
    /// # Errors
    /// `Internal` if `set_by` is longer than 288 characters; the store's.
    pub async fn put_setting(tx: &mut dyn RecordTx, r: &SettingRow) -> Result<()> {
        if r.set_by.chars().count() > 288 {
            return Err(SprigError::internal("set_by is longer than 288 characters"));
        }
        put(
            tx,
            &setting_key(r.scope, &r.key),
            &encode("setting", &SetRec::of(r))?,
            r.version,
        )
        .await
    }

    /// Resets a row: clears its value, its bounds, or both, leaving the row
    /// (empty, a tombstone) with `version`. Returns whether it existed.
    ///
    /// # Errors
    /// The store's.
    #[allow(clippy::too_many_arguments)] // one row's address and its change
    pub async fn reset_setting(
        tx: &mut dyn RecordTx,
        scope: SettingScope,
        k: &str,
        value: bool,
        bounds: bool,
        set_by: &str,
        at: SystemTime,
        version: u64,
    ) -> Result<bool> {
        let rk = setting_key(scope, k);
        let Some(raw) = tx.get_for_update(&rk).await? else {
            return Ok(false);
        };
        let mut row = decode::<SetRec>("setting", &raw)?.row()?;
        if value {
            row.value = None;
        }
        if bounds {
            row.bounds = None;
        }
        row.set_by = set_by.to_owned();
        row.set_at = at;
        row.version = version;
        Self::put_setting(tx, &row).await?;
        Ok(true)
    }

    /// Every row of one scope, by key.
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_of(
        tx: &mut dyn RecordTx,
        scope: SettingScope,
    ) -> Result<Vec<SettingRow>> {
        let prefix = key(SETTINGS, &scope_pk(scope));
        let mut out = Vec::new();
        for (_, raw) in rows_under(tx, &prefix).await? {
            out.push(decode::<SetRec>("setting", &raw)?.row()?);
        }
        Ok(out)
    }

    /// Settings rows changed after `since` (every row for `None`).
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_since(
        tx: &mut dyn RecordTx,
        since: Option<u64>,
    ) -> Result<Vec<SettingRow>> {
        let mut out = Vec::new();
        match since {
            None => {
                for (_, raw) in rows_under(tx, SETTINGS).await? {
                    out.push(decode::<SetRec>("setting", &raw)?.row()?);
                }
            }
            Some(s) => {
                for k in changed_keys(tx, s, SETTINGS).await? {
                    if let Some(raw) = tx.get(&k).await? {
                        let r = decode::<SetRec>("setting", &raw)?.row()?;
                        if r.version > s {
                            out.push(r);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Every row of `key`, at any scope (empty rows included), in scope
    /// order.
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn settings_for_key(tx: &mut dyn RecordTx, k: &str) -> Result<Vec<SettingRow>> {
        Ok(Self::settings_since(tx, None)
            .await?
            .into_iter()
            .filter(|r| r.key == k)
            .collect())
    }

    /// An internal (`sprig.`) key's value at tenant scope.
    ///
    /// # Errors
    /// As [`setting`](Self::setting).
    pub async fn internal(tx: &mut dyn RecordTx, k: &str) -> Result<Option<Json>> {
        Ok(Self::setting(tx, SettingScope::Tenant, k)
            .await?
            .and_then(|r| r.value))
    }

    /// Writes an internal (`sprig.`) key at tenant scope.
    ///
    /// # Errors
    /// `Internal` unless `key` starts with `sprig.`; the store's.
    pub async fn put_internal(
        tx: &mut dyn RecordTx,
        k: &str,
        value: &Json,
        version: u64,
        at: SystemTime,
    ) -> Result<()> {
        if !k.starts_with("sprig.") {
            return Err(SprigError::internal(format!("{k} is not an internal key")));
        }
        Self::put_setting(
            tx,
            &SettingRow {
                scope: SettingScope::Tenant,
                key: k.to_owned(),
                value: Some(value.clone()),
                bounds: None,
                set_by: "system".to_owned(),
                set_at: at,
                version,
            },
        )
        .await
    }

    /// Deletes an internal (`sprig.`) key at tenant scope. Returns whether
    /// it existed.
    ///
    /// # Errors
    /// `Internal` unless `key` starts with `sprig.`; the store's.
    pub async fn delete_internal(tx: &mut dyn RecordTx, k: &str) -> Result<bool> {
        if !k.starts_with("sprig.") {
            return Err(SprigError::internal(format!("{k} is not an internal key")));
        }
        let rk = setting_key(SettingScope::Tenant, k);
        if tx.get_for_update(&rk).await?.is_none() {
            return Ok(false);
        }
        tx.delete(&rk).await?;
        Ok(true)
    }

    async fn delete_scope(tx: &mut dyn RecordTx, scope: SettingScope) -> Result<u64> {
        let prefix = key(SETTINGS, &scope_pk(scope));
        let mut n = 0;
        for (k, _) in rows_under(tx, &prefix).await? {
            tx.delete(&k).await?;
            n += 1;
        }
        Ok(n)
    }

    // ---- schema versions ----

    /// Inserts a schema version (1062 if the version exists).
    ///
    /// # Errors
    /// 1062; the store's.
    pub async fn insert_schema_version(tx: &mut dyn RecordTx, r: &SchemaVersionRow) -> Result<()> {
        let k = sv_key(r.object_id, r.version);
        if tx.get_for_update(&k).await?.is_some() {
            return Err(duplicate(&format!(
                "{}-{}' for key 'PRIMARY",
                r.object_id.0, r.version
            )));
        }
        tx.put(&k, &encode("schema version", &SvRec::of(r))?).await
    }

    /// Every schema version of an object, oldest first.
    ///
    /// # Errors
    /// The store's; `Internal` for a row that doesn't decode.
    pub async fn schema_versions(
        tx: &mut dyn RecordTx,
        object: ObjectId,
    ) -> Result<Vec<SchemaVersionRow>> {
        let prefix = key(SCHEMA_VERSIONS, &Pk::new().u64(object.0));
        let mut out = Vec::new();
        for (_, raw) in rows_under(tx, &prefix).await? {
            out.push(decode::<SvRec>("schema version", &raw)?.row()?);
        }
        Ok(out)
    }

    /// One schema version of an object.
    ///
    /// # Errors
    /// As [`schema_versions`](Self::schema_versions).
    pub async fn schema_version(
        tx: &mut dyn RecordTx,
        object: ObjectId,
        version: u64,
    ) -> Result<Option<SchemaVersionRow>> {
        match tx.get(&sv_key(object, version)).await? {
            Some(raw) => decode::<SvRec>("schema version", &raw)?.row().map(Some),
            None => Ok(None),
        }
    }

    /// The newest schema version of an object.
    ///
    /// # Errors
    /// As [`schema_versions`](Self::schema_versions).
    pub async fn latest_schema_version(
        tx: &mut dyn RecordTx,
        object: ObjectId,
    ) -> Result<Option<SchemaVersionRow>> {
        Ok(Self::schema_versions(tx, object).await?.pop())
    }

    async fn delete_schema_versions(tx: &mut dyn RecordTx, object: ObjectId) -> Result<u64> {
        let prefix = key(SCHEMA_VERSIONS, &Pk::new().u64(object.0));
        let mut n = 0;
        for (k, _) in rows_under(tx, &prefix).await? {
            tx.delete(&k).await?;
            n += 1;
        }
        Ok(n)
    }

    // ---- tombstones ----

    /// Removes tombstones changed at or before `up_to` (dropped databases
    /// and objects with their settings rows and schema versions, and empty
    /// settings rows), the change index at or below it, and raises the
    /// purged version to `up_to`. Run it in a transaction holding the
    /// catalog version record, with `up_to` no newer than the committed
    /// version.
    ///
    /// # Errors
    /// The store's.
    pub async fn purge(tx: &mut dyn RecordTx, up_to: u64) -> Result<PurgeReport> {
        let mut r = PurgeReport::default();
        for o in Self::objects_since(tx, None).await? {
            if o.state == ObjectState::Dropped && o.changed_version <= up_to {
                r.settings += Self::delete_scope(tx, SettingScope::Object(o.object_id)).await?;
                r.schema_versions += Self::delete_schema_versions(tx, o.object_id).await?;
                tx.delete(&obj_key(o.object_id)).await?;
                r.objects += 1;
            }
        }
        for d in Self::databases_since(tx, None).await? {
            if d.state == DbState::Dropped && d.changed_version <= up_to {
                r.settings += Self::delete_scope(tx, SettingScope::Database(d.db_id)).await?;
                tx.delete(&db_key(d.db_id)).await?;
                r.databases += 1;
            }
        }
        for s in Self::settings_since(tx, None).await? {
            if s.is_empty() && !s.is_internal() && s.version <= up_to {
                tx.delete(&setting_key(s.scope, &s.key)).await?;
                r.settings += 1;
            }
        }
        let hi = chg_key(up_to.saturating_add(1), b"");
        for (k, _) in scan_all(tx, CHANGES, &hi).await? {
            tx.delete(&k).await?;
        }
        let purged = Self::purged(tx).await?;
        if up_to > purged {
            tx.put(PURGED, &up_to.to_be_bytes()).await?;
        }
        Ok(r)
    }

    // ---- snapshots ----

    /// The versions and every row changed after `since` (every row for
    /// `None`) at the transaction's read view. An incremental request from
    /// before the purged version becomes a full read.
    ///
    /// # Errors
    /// The store's.
    pub async fn snapshot(tx: &mut dyn RecordTx, since: Option<u64>) -> Result<Snapshot> {
        let versions = Self::versions(tx).await?;
        let since = since.filter(|&s| s >= versions.purged);
        Ok(Snapshot {
            versions,
            since,
            databases: Self::databases_since(tx, since).await?,
            objects: Self::objects_since(tx, since).await?,
            settings: Self::settings_since(tx, since).await?,
        })
    }
}

/// Reads of one tenant's core rows outside any change, each in a short
/// read-only record transaction of its own: for processes and loops that
/// read the catalog by id or name without a snapshot (the controller, the
/// workers, tools). They see the latest committed catalog.
#[derive(Clone)]
pub struct CoreReader {
    records: std::sync::Arc<dyn crate::records::RecordStore>,
    tenant: sprig_common::ids::TenantId,
}

impl std::fmt::Debug for CoreReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreReader")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

macro_rules! read_in {
    ($self:ident, |$tx:ident| $body:expr) => {{
        let mut t = $self.records.begin($self.tenant).await?;
        let $tx: &mut dyn RecordTx = &mut *t;
        let out = $body;
        let _ = t.rollback().await;
        out
    }};
}

impl CoreReader {
    /// Reads `tenant`'s core rows in `records`.
    #[must_use]
    pub fn new(
        records: std::sync::Arc<dyn crate::records::RecordStore>,
        tenant: sprig_common::ids::TenantId,
    ) -> Self {
        Self { records, tenant }
    }

    /// The committed versions.
    ///
    /// # Errors
    /// The store's (39070 when the engine node can't be reached).
    pub async fn versions(&self) -> Result<Versions> {
        read_in!(self, |tx| CoreRecords::versions(tx).await)
    }

    /// One database by id, tombstones included.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn database(&self, db: DbId) -> Result<Option<DatabaseRow>> {
        read_in!(self, |tx| CoreRecords::database(tx, db).await)
    }

    /// The live database with this name key.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn database_by_name(&self, name_key: &str) -> Result<Option<DatabaseRow>> {
        read_in!(self, |tx| CoreRecords::database_by_name(tx, name_key).await)
    }

    /// Every database, tombstones included, by id.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn databases(&self) -> Result<Vec<DatabaseRow>> {
        read_in!(self, |tx| CoreRecords::databases_since(tx, None).await)
    }

    /// One object by id, tombstones included.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn object(&self, object: ObjectId) -> Result<Option<ObjectRow>> {
        read_in!(self, |tx| CoreRecords::object(tx, object).await)
    }

    /// The objects among `ids`, tombstones included, by id.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn objects_by_ids(&self, ids: &[ObjectId]) -> Result<Vec<ObjectRow>> {
        read_in!(self, |tx| CoreRecords::objects_by_ids(tx, ids).await)
    }

    /// Every object, tombstones included, by id.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn objects(&self) -> Result<Vec<ObjectRow>> {
        read_in!(self, |tx| CoreRecords::objects_since(tx, None).await)
    }

    /// Every settings row of one scope, by key.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn settings_of(&self, scope: SettingScope) -> Result<Vec<SettingRow>> {
        read_in!(self, |tx| CoreRecords::settings_of(tx, scope).await)
    }

    /// Every settings row, with the committed versions they were read at.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn settings(&self) -> Result<(Versions, Vec<SettingRow>)> {
        read_in!(self, |tx| {
            let v = CoreRecords::versions(tx).await?;
            Ok((v, CoreRecords::settings_since(tx, None).await?))
        })
    }

    /// Every schema version of an object, oldest first.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn schema_versions(&self, object: ObjectId) -> Result<Vec<SchemaVersionRow>> {
        read_in!(self, |tx| CoreRecords::schema_versions(tx, object).await)
    }

    /// How many databases aren't dropped.
    ///
    /// # Errors
    /// As [`versions`](Self::versions).
    pub async fn live_databases(&self) -> Result<u64> {
        Ok(self
            .databases()
            .await?
            .iter()
            .filter(|d| d.state != DbState::Dropped)
            .count() as u64)
    }
}

/// The keys under `table` (a core table's prefix) written at versions
/// after `since`, each once, in key order (so rows come back ordered by
/// their primary key, as the SQL's `ORDER BY` did).
async fn changed_keys(tx: &mut dyn RecordTx, since: u64, table: &[u8]) -> Result<Vec<Vec<u8>>> {
    let lo = chg_key(since.saturating_add(1), b"");
    let mut keys: Vec<Vec<u8>> = scan_all(tx, &lo, &prefix_end(CHANGES))
        .await?
        .into_iter()
        .filter_map(|(k, _)| {
            let rel = k.get(CHANGES.len() + 8..)?;
            rel.starts_with(table).then(|| rel.to_vec())
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}

#[cfg(test)]
mod tests;
