//! The `inherit` catalog module: spaces, inheritable objects (with their
//! extension), shares and users' home spaces, kept as the engine's
//! catalog records on the catalog version, the snapshot loader
//! ([`InheritExtension`]) and the writes the extension statements make
//! ([`InheritChange`]).
//!
//! ```text
//! m/inherit/spaces/<space_id u64>                 SpaceRow
//! m/inherit/objects/<object_id u64>               ObjectRow (the inheritable object and its extension)
//! m/inherit/shares/<share_id u64><role u8>        ShareRow (role 1 granted, 2 received)
//! m/inherit/homes/<user_id u32>                   HomeRow
//! ```
//!
//! Like the type layer's records, the rows are versioned with the catalog
//! version (each keeps the version that last changed it) and reloaded
//! whole into every snapshot (they are few). Table names come from
//! `objects` at load time.

use std::collections::BTreeMap;
use std::sync::Arc;

use mysql_async::Conn;
use serde::{Deserialize, Serialize};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    InheritState, InheritableObject, MergeMode, ObjectExt, ROOT_SPACE, Share, ShareTarget, Space,
    SpaceId, Take, UniqueKey,
};

/// The module name.
pub const MODULE: &str = "inherit";

/// Spaces.
pub const SPACES: RowTable = RowTable::new(MODULE, "spaces");
/// Inheritable objects.
pub const OBJECTS: RowTable = RowTable::new(MODULE, "objects");
/// Shares granted and received.
pub const SHARES: RowTable = RowTable::new(MODULE, "shares");
/// Users' home spaces.
pub const HOMES: RowTable = RowTable::new(MODULE, "homes");

const GRANTED: u8 = 1;
const RECEIVED: u8 = 2;

/// The module's MariaDB migrations: none, its rows are engine records.
pub static INHERIT_MIGRATIONS: &[Migration] = &[];

/// The `inherit` module, for [`Migrator::new`].
pub const INHERIT_MODULE: Module = Module {
    name: MODULE,
    migrations: INHERIT_MIGRATIONS,
};

/// Makes sure catalog `schema` can take the module's records (its core
/// migrations, which create the record log).
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    Migrator::new(&[])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A space: what `spaces` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceRow {
    /// Its id.
    pub space_id: u64,
    /// Its name (unique in the tenant).
    pub name: String,
    /// Its parent (none for the root).
    pub parent_space_id: Option<u64>,
    /// 0 for the root.
    pub depth: u16,
    /// `/1/<id>/.../`.
    pub path: String,
    /// `active` (rows of dropped spaces are removed).
    pub state: String,
    /// Who made it.
    pub created_by: u32,
    /// When (ms since the epoch).
    pub created_at_ms: u64,
    /// The catalog version that last changed it.
    pub changed_version: u64,
}

/// An inheritable object: what `inheritable_objects` and `inherit_ext`
/// held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRow {
    /// The object.
    pub object_id: u64,
    /// `table`.
    pub model: String,
    /// The key fields.
    pub key_fields: Vec<String>,
    /// Field slots.
    pub field_slots: BTreeMap<String, u8>,
    /// The merged view's column order.
    pub columns: Vec<String>,
    /// The user `UNIQUE` keys as declared.
    pub unique_keys: Vec<UniqueKey>,
    /// The tag field.
    pub tag_field: Option<String>,
    /// `active`.
    pub state: String,
    /// Bumped by every change.
    pub version: u64,
    /// The extension (`AUTO_INCREMENT` column, foreign keys, adoption).
    pub ext: ObjectExt,
    /// When (ms since the epoch).
    pub created_at_ms: u64,
    /// The catalog version that last changed it.
    pub changed_version: u64,
}

/// A share as granted or received: what `shares` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRow {
    /// Its name (granted shares).
    pub name: String,
    /// The shared object.
    pub object_id: u64,
    /// The space it is shared from.
    pub from_space_id: u64,
    /// The receiving tenant (0: spaces of this tenant).
    pub peer_tenant_id: u64,
    /// The receiving space (0: every child).
    pub to_space_id: u64,
    /// The row filter.
    pub filter_sql: Option<String>,
    /// The shared columns.
    pub columns: Option<Vec<String>>,
    /// A received share's narrowing.
    pub take_filter_sql: Option<String>,
    /// Receivers may share it on.
    pub reshare: bool,
    /// `read_through`, `materialized` or `federated`.
    pub merge_mode: String,
    /// The share's version.
    pub version: u64,
    /// `active` or `dropping`.
    pub state: String,
    /// Who made it.
    pub created_by: u32,
    /// When (ms since the epoch).
    pub created_at_ms: u64,
    /// The catalog version that last changed it.
    pub changed_version: u64,
}

/// A user's home space: what `space_homes` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeRow {
    /// The space.
    pub space_id: u64,
    /// The catalog version that last changed it.
    pub changed_version: u64,
}

fn space_exists(what: &str) -> SprigError {
    SprigError::new(SprigCode::SpaceExists, what.to_owned())
}

/// The object id of a share key, and its role.
fn share_key(pk: &Pk) -> Option<(u64, u8)> {
    let id = u64::from_be_bytes(pk.0.get(..8)?.try_into().ok()?);
    Some((id, *pk.0.get(8)?))
}

/// A space to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSpace {
    /// The name.
    pub name: String,
    /// The parent space.
    pub parent: SpaceId,
    /// The creating user.
    pub created_by: u32,
}

/// What one extension statement writes, committed in one versioned change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InheritChange {
    /// Make sure the root space has a row, with this name.
    pub root_name: Option<String>,
    /// Spaces created (ids allocated on commit).
    pub add_spaces: Vec<NewSpace>,
    /// Spaces removed.
    pub drop_spaces: Vec<SpaceId>,
    /// Objects made inheritable or changed.
    pub upsert_objects: Vec<InheritableObject>,
    /// Objects no longer inheritable (or dropped).
    pub delete_objects: Vec<ObjectId>,
    /// Shares created (`id` 0: allocated on commit) or replaced.
    pub upsert_shares: Vec<Share>,
    /// Shares dropped: they stay as `dropping` rows, which no edge reads,
    /// until [`Self::purge_shares`] records their cleanup done.
    pub drop_shares: Vec<u64>,
    /// Dropped shares whose cleanup in the children is done: their rows
    /// go.
    pub purge_shares: Vec<u64>,
    /// Narrowings of received shares set or replaced.
    pub upsert_takes: Vec<Take>,
    /// Home spaces set (`None`: back to the root).
    pub homes: Vec<(u32, Option<SpaceId>)>,
}

impl InheritChange {
    /// Whether nothing is written.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// The live objects among `ids` with their database and name (from the
/// catalog's `objects`).
pub type ObjectNames = sprig_types::store::ObjectNames;

/// Reads and writes the module's records.
#[derive(Debug, Clone, Copy, Default)]
pub struct InheritStore;

impl InheritStore {
    /// The names of the active granted shares of `objects`, in key order;
    /// with `inheritable`, only of objects that are inheritable.
    ///
    /// # Errors
    /// The record store's.
    pub async fn granted_share_names(
        rec: &mut dyn RecordTx,
        objects: &[u64],
        inheritable: bool,
    ) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for (pk, row) in SHARES.scan::<ShareRow>(rec, &Pk::new()).await? {
            let Some((object, role)) = share_key(&pk) else {
                continue;
            };
            if role != GRANTED || row.state != "active" || !objects.contains(&object) {
                continue;
            }
            if inheritable
                && OBJECTS
                    .get::<serde_json::Value>(rec, &Pk::new().u64(object))
                    .await?
                    .is_none()
            {
                continue;
            }
            out.push(row.name);
        }
        Ok(out)
    }

    /// Every object id the state needs a name for.
    ///
    /// # Errors
    /// The record store's.
    pub async fn object_ids(rec: &mut dyn RecordTx) -> Result<Vec<ObjectId>> {
        Ok(OBJECTS
            .rows::<ObjectRow>(rec, &Pk::new())
            .await?
            .into_iter()
            .map(|o| ObjectId(o.object_id))
            .collect())
    }

    /// The whole state; objects whose table is gone (not in `names`) are
    /// left out.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx, names: &ObjectNames) -> Result<InheritState> {
        let spaces = SPACES
            .rows::<SpaceRow>(rec, &Pk::new())
            .await?
            .into_iter()
            .filter(|r| r.state == "active")
            .map(|r| Space {
                id: SpaceId(r.space_id),
                name: r.name,
                parent: r.parent_space_id.map(SpaceId),
                depth: r.depth,
            })
            .collect();
        let mut objects = Vec::new();
        for r in OBJECTS.rows::<ObjectRow>(rec, &Pk::new()).await? {
            let Some((db, table)) = names.get(&ObjectId(r.object_id)) else {
                continue;
            };
            objects.push(InheritableObject {
                object_id: ObjectId(r.object_id),
                db: *db,
                table: table.clone(),
                key: r.key_fields,
                slots: r.field_slots,
                columns: r.columns,
                uniques: r.unique_keys,
                tag_field: r.tag_field,
                version: r.version,
                auto: r.ext.auto,
                links: r.ext.links,
                adopted: r.ext.adopted,
                type_root: r.ext.type_root,
            });
        }
        let mut shares = Vec::new();
        let mut dropping = Vec::new();
        let mut takes = Vec::new();
        for (pk, r) in SHARES.scan::<ShareRow>(rec, &Pk::new()).await? {
            let (id, role) = share_key(&pk)
                .ok_or_else(|| SprigError::internal("inherit catalog: share key".to_owned()))?;
            let active = r.state == "active";
            if !active && (role != GRANTED || r.state != "dropping") {
                continue;
            }
            if role == RECEIVED {
                takes.push(Take {
                    share_id: id,
                    peer_tenant: TenantId(r.peer_tenant_id),
                    object_id: ObjectId(r.object_id),
                    take: r.take_filter_sql,
                });
                continue;
            }
            let to_list = if active { &mut shares } else { &mut dropping };
            to_list.push(Share {
                id,
                name: r.name,
                object_id: ObjectId(r.object_id),
                from: SpaceId(r.from_space_id),
                to: match (r.peer_tenant_id, r.to_space_id) {
                    (0, 0) => ShareTarget::AllChildren,
                    (0, s) => ShareTarget::Space(SpaceId(s)),
                    (t, _) => ShareTarget::Tenant(TenantId(t)),
                },
                filter: r.filter_sql,
                columns: r.columns,
                take: r.take_filter_sql,
                reshare: r.reshare,
                version: r.version,
                created_by: r.created_by,
                mode: MergeMode::parse(&r.merge_mode).unwrap_or_default(),
                via: None,
            });
        }
        let mut homes = Vec::new();
        for (pk, r) in HOMES.scan::<HomeRow>(rec, &Pk::new()).await? {
            let user =
                pk.0.get(..4)
                    .and_then(|b| b.try_into().ok())
                    .map(u32::from_be_bytes)
                    .ok_or_else(|| SprigError::internal("inherit catalog: home key".to_owned()))?;
            homes.push((user, SpaceId(r.space_id)));
        }
        let mut st = InheritState::new(spaces, objects, shares, takes, homes);
        st.ext_ready = true;
        st.set_dropping(dropping);
        Ok(st)
    }

    /// The state of the tenant whose catalog schema is `schema`, read the
    /// way an operator's tool can: the records (object names included)
    /// through the engine node's peer service (`SPRIGDB_CATALOG_NODE`).
    /// `_c` (a catalog connection) is no longer read.
    ///
    /// # Errors
    /// `Internal` when `schema` isn't `t<id>_sprig`; 39070 when the
    /// engine node can't be reached; the catalog's.
    pub async fn load_remote(_c: &mut Conn, schema: &str) -> Result<InheritState> {
        let tenant = schema
            .strip_prefix('t')
            .and_then(|s| s.strip_suffix("_sprig"))
            .and_then(|s| s.parse().ok())
            .map(TenantId)
            .ok_or_else(|| SprigError::internal(format!("{schema} is not a tenant catalog")))?;
        let store = sprig_catalog::records::remote::RemoteRecords::new(None, None);
        let mut tx = sprig_catalog::RecordStore::begin(&store, tenant).await?;
        let st = async {
            let ids = Self::object_ids(&mut *tx).await?;
            let names: sprig_types::store::ObjectNames =
                sprig_catalog::store::rec::CoreRecords::objects_by_ids(&mut *tx, &ids)
                    .await?
                    .into_iter()
                    .filter(|o| o.state != sprig_catalog::store::ObjectState::Dropped)
                    .map(|o| (o.object_id, (o.db_id, o.name)))
                    .collect();
            Self::load(&mut *tx, &names).await
        }
        .await;
        let _ = tx.rollback().await;
        st
    }

    /// `MAX(id) + 1` of a table keyed by a leading u64 (at least 2: the
    /// root space is 1), as the SQL handed out ids.
    async fn next_id(rec: &mut dyn RecordTx, t: RowTable) -> Result<u64> {
        let max = t
            .scan::<serde_json::Value>(rec, &Pk::new())
            .await?
            .iter()
            .filter_map(|(pk, _)| pk.0.get(..8)?.try_into().ok().map(u64::from_be_bytes))
            .max()
            .unwrap_or(1);
        Ok(max.max(1) + 1)
    }

    /// Applies `change` to the records of a catalog change at catalog
    /// version `version`.
    ///
    /// # Errors
    /// 39881 for a space name taken; the record store's.
    #[allow(clippy::too_many_lines)] // one block per kind of row
    pub async fn apply(rec: &mut dyn RecordTx, change: &InheritChange, version: u64) -> Result<()> {
        let now = now_ms();
        if let Some(name) = &change.root_name {
            let taken = SPACES
                .rows::<SpaceRow>(rec, &Pk::new())
                .await?
                .iter()
                .any(|s| s.space_id == ROOT_SPACE.0 || s.name == *name);
            if !taken {
                let row = SpaceRow {
                    space_id: ROOT_SPACE.0,
                    name: name.clone(),
                    parent_space_id: None,
                    depth: 0,
                    path: "/1/".to_owned(),
                    state: "active".to_owned(),
                    created_by: 0,
                    created_at_ms: now,
                    changed_version: version,
                };
                SPACES.put(rec, &Pk::new().u64(ROOT_SPACE.0), &row).await?;
            }
        }
        for s in &change.add_spaces {
            let all = SPACES.rows::<SpaceRow>(rec, &Pk::new()).await?;
            if all.iter().any(|o| o.name == s.name) {
                return Err(space_exists("the name is taken"));
            }
            let id = Self::next_id(rec, SPACES).await?;
            let (depth, mut path) = all
                .iter()
                .find(|o| o.space_id == s.parent.0 && o.state == "active")
                .map_or((0, "/1/".to_owned()), |p| (p.depth, p.path.clone()));
            path.push_str(&format!("{id}/"));
            let row = SpaceRow {
                space_id: id,
                name: s.name.clone(),
                parent_space_id: Some(s.parent.0),
                depth: depth.saturating_add(1),
                path,
                state: "active".to_owned(),
                created_by: s.created_by,
                created_at_ms: now,
                changed_version: version,
            };
            SPACES.put(rec, &Pk::new().u64(id), &row).await?;
        }
        for s in &change.drop_spaces {
            SPACES.delete(rec, &Pk::new().u64(s.0)).await?;
            for (pk, h) in HOMES.scan::<HomeRow>(rec, &Pk::new()).await? {
                if h.space_id == s.0 {
                    HOMES.delete(rec, &pk).await?;
                }
            }
            for (pk, r) in SHARES.scan::<ShareRow>(rec, &Pk::new()).await? {
                let granted = share_key(&pk).is_some_and(|(_, role)| role == GRANTED);
                if granted
                    && (r.from_space_id == s.0 || (r.peer_tenant_id == 0 && r.to_space_id == s.0))
                {
                    SHARES.delete(rec, &pk).await?;
                }
            }
        }
        for id in &change.delete_objects {
            OBJECTS.delete(rec, &Pk::new().u64(id.0)).await?;
            for (pk, r) in SHARES.scan::<ShareRow>(rec, &Pk::new()).await? {
                if r.object_id == id.0 {
                    SHARES.delete(rec, &pk).await?;
                }
            }
        }
        for o in &change.upsert_objects {
            let pk = Pk::new().u64(o.object_id.0);
            let old = OBJECTS.get::<ObjectRow>(rec, &pk).await?;
            let row = ObjectRow {
                object_id: o.object_id.0,
                model: "table".to_owned(),
                key_fields: o.key.clone(),
                field_slots: o.slots.clone(),
                columns: o.columns.clone(),
                unique_keys: o.uniques.clone(),
                tag_field: o.tag_field.clone(),
                state: "active".to_owned(),
                version: old.as_ref().map_or(1, |r| r.version + 1),
                ext: o.ext(),
                created_at_ms: old.as_ref().map_or(now, |r| r.created_at_ms),
                changed_version: version,
            };
            OBJECTS.put(rec, &pk, &row).await?;
        }
        for id in &change.drop_shares {
            let pk = Pk::new().u64(*id).u8(GRANTED);
            if let Some(mut r) = SHARES.get::<ShareRow>(rec, &pk).await? {
                "dropping".clone_into(&mut r.state);
                r.changed_version = version;
                SHARES.put(rec, &pk, &r).await?;
            }
        }
        for id in &change.purge_shares {
            let pk = Pk::new().u64(*id).u8(GRANTED);
            if SHARES
                .get::<ShareRow>(rec, &pk)
                .await?
                .is_some_and(|r| r.state == "dropping")
            {
                SHARES.delete(rec, &pk).await?;
            }
        }
        for s in &change.upsert_shares {
            let id = if s.id == 0 {
                Self::next_id(rec, SHARES).await?
            } else {
                s.id
            };
            let (peer, to) = match s.to {
                ShareTarget::AllChildren => (0, 0),
                ShareTarget::Space(c) => (0, c.0),
                ShareTarget::Tenant(t) => (t.0, 0),
            };
            let pk = Pk::new().u64(id).u8(GRANTED);
            let row = match SHARES.get::<ShareRow>(rec, &pk).await? {
                // As `ON DUPLICATE KEY UPDATE`: the definition's columns.
                Some(mut r) => {
                    r.filter_sql.clone_from(&s.filter);
                    r.columns.clone_from(&s.columns);
                    s.mode.as_str().clone_into(&mut r.merge_mode);
                    r.take_filter_sql.clone_from(&s.take);
                    r.reshare = s.reshare;
                    r.version = s.version;
                    r.changed_version = version;
                    r
                }
                None => ShareRow {
                    name: s.name.clone(),
                    object_id: s.object_id.0,
                    from_space_id: s.from.0,
                    peer_tenant_id: peer,
                    to_space_id: to,
                    filter_sql: s.filter.clone(),
                    columns: s.columns.clone(),
                    take_filter_sql: s.take.clone(),
                    reshare: s.reshare,
                    merge_mode: s.mode.as_str().to_owned(),
                    version: s.version,
                    state: "active".to_owned(),
                    created_by: s.created_by,
                    created_at_ms: now,
                    changed_version: version,
                },
            };
            SHARES.put(rec, &pk, &row).await?;
        }
        for t in &change.upsert_takes {
            let pk = Pk::new().u64(t.share_id).u8(RECEIVED);
            let row = match SHARES.get::<ShareRow>(rec, &pk).await? {
                Some(mut r) => {
                    r.take_filter_sql.clone_from(&t.take);
                    r.version += 1;
                    r.changed_version = version;
                    r
                }
                None => ShareRow {
                    name: String::new(),
                    object_id: t.object_id.0,
                    from_space_id: 0,
                    peer_tenant_id: t.peer_tenant.0,
                    to_space_id: ROOT_SPACE.0,
                    filter_sql: None,
                    columns: None,
                    take_filter_sql: t.take.clone(),
                    reshare: false,
                    merge_mode: MergeMode::default().as_str().to_owned(),
                    version: 1,
                    state: "active".to_owned(),
                    created_by: 0,
                    created_at_ms: now,
                    changed_version: version,
                },
            };
            SHARES.put(rec, &pk, &row).await?;
        }
        for (user, space) in &change.homes {
            let pk = Pk::new().u32(*user);
            match space {
                Some(s) => {
                    HOMES
                        .put(
                            rec,
                            &pk,
                            &HomeRow {
                                space_id: s.0,
                                changed_version: version,
                            },
                        )
                        .await?;
                }
                None => HOMES.delete(rec, &pk).await?,
            }
        }
        Ok(())
    }
}

impl ExtState for InheritState {
    fn bytes(&self) -> usize {
        self.spaces().count() * 128
            + self
                .objects()
                .map(|o| 256 + o.columns.len() * 64)
                .sum::<usize>()
            + self.shares().count() * 256
    }
}

/// The module in the catalog cache: [`InheritState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct InheritExtension;

#[async_trait::async_trait]
impl CatalogExtension for InheritExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        INHERIT_MIGRATIONS
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
            return Ok(Arc::new(InheritState::unmigrated()));
        }
        let ids = InheritStore::object_ids(tx.records()?).await?;
        let names = sprig_types::store::snapshot_object_names(tx, &ids).await?;
        Ok(Arc::new(InheritStore::load(tx.records()?, &names).await?))
    }
}

#[cfg(test)]
mod tests;
