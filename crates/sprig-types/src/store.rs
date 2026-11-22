//! The `types` catalog module: types, their fields, bindings and record
//! hierarchies, kept as the engine's catalog records on the catalog
//! version, the snapshot loader ([`TypesExtension`]) and the writes type
//! DDL makes ([`TypesChange`]).
//!
//! ```text
//! m/types/types/<type_id u64>                         TypeRow
//! m/types/type_fields/<type_id u64><ordinal u32>      FieldRow
//! m/types/type_bindings/<object_id u64>               (no writer yet)
//! m/types/hierarchies/<object_id u64>                 HierRow
//! ```
//!
//! Every type DDL commits its rows in one catalog change
//! ([`TypesStore::apply`] on the change's records), and every gateway's
//! snapshot reloads the rows whole (they are few). A catalog whose record
//! log isn't there yet loads as [`TypesState::unmigrated`]; the first type
//! DDL migrates it ([`migrate`]).
//!
//! Names are never stored here: a type's or hierarchy's table name comes
//! from its `objects` row at load time, so `objects` stays the one source of
//! names.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{MigrateOpts, Migration, Module};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, Migrator};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

use crate::model::{
    DiscValue, FieldDef, HierState, HierarchyDef, KeyClass, OnDelete, Storage, TypeDef, TypeState,
    TypesState,
};

/// The module name.
pub const MODULE: &str = "types";

/// Types.
pub const TYPES: RowTable = RowTable::new(MODULE, "types");
/// Types' own fields, by type and ordinal.
pub const TYPE_FIELDS: RowTable = RowTable::new(MODULE, "type_fields");
/// Bindings of types to other models' objects (written by the document,
/// key-value, vector and dimensional models, none built yet).
pub const TYPE_BINDINGS: RowTable = RowTable::new(MODULE, "type_bindings");
/// Record hierarchies.
pub const HIERARCHIES: RowTable = RowTable::new(MODULE, "hierarchies");

/// The module's MariaDB migrations: none, its rows are engine records.
pub static TYPES_MIGRATIONS: &[Migration] = &[];

/// The `types` module, for [`Migrator::new`].
pub const TYPES_MODULE: Module = Module {
    name: MODULE,
    migrations: TYPES_MIGRATIONS,
};

/// Makes sure catalog `schema` can take the module's records (its core
/// migrations, which create the record log), under the migrator's lock.
///
/// # Errors
/// The migrator's.
pub async fn migrate(c: &mut Conn, schema: &str) -> Result<()> {
    Migrator::new(&[])?
        .run(c, schema, &MigrateOpts::default())
        .await?;
    Ok(())
}

/// A type: what `types` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeRow {
    /// The type's object id.
    pub type_id: u64,
    /// Its database.
    pub db_id: u32,
    /// The parent type.
    pub parent_id: Option<u64>,
    /// The root of its tree.
    pub root_id: u64,
    /// 0 for a root.
    pub depth: u8,
    /// The path from the root (`asset.vehicle`).
    pub type_path: String,
    /// `entity` (composites are not built).
    pub kind: String,
    /// Abstract (no rows of its own).
    pub is_abstract: bool,
    /// `joined`, `single_table` or `none`.
    pub storage: String,
    /// A root's discriminator column or a subtype's value.
    pub discriminator: Option<serde_json::Value>,
    /// The key fields.
    pub key_fields: Vec<String>,
    /// The table holding its own fields.
    pub physical_table: Option<String>,
    /// `active`, `renaming` or `dropping`.
    pub state: String,
    /// Bumped by every change of the row.
    pub version: u64,
    /// Milliseconds since the epoch.
    pub created_at_ms: u64,
    /// Milliseconds since the epoch.
    pub updated_at_ms: u64,
}

/// A type's own field: what `type_fields` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRow {
    /// The type.
    pub type_id: u64,
    /// The field.
    pub field_name: String,
    /// 1, 2, ... in declaration order.
    pub ordinal: u16,
    /// The column type.
    pub data_type: String,
    /// Nullable.
    pub nullable: bool,
    /// The default, as SQL.
    pub default_expr: Option<String>,
    /// The table holding it.
    pub physical_table: Option<String>,
    /// Its column there.
    pub physical_column: Option<String>,
}

/// A record hierarchy: what `hierarchies` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HierRow {
    /// The table's object id.
    pub object_id: u64,
    /// `table` (collections are not built).
    pub model: String,
    /// `[{"name":..,"class":..,"type":..}]`.
    pub key_fields: serde_json::Value,
    /// The parent column(s).
    pub parent_fields: Vec<String>,
    /// `restrict` or `cascade`.
    pub on_delete: String,
    /// The deepest path.
    pub max_depth: u16,
    /// `backfilling`, `active`, `failed` or `dropping`.
    pub state: String,
    /// Milliseconds since the epoch.
    pub created_at_ms: u64,
    /// Milliseconds since the epoch.
    pub updated_at_ms: u64,
}

fn type_pk(id: ObjectId) -> Pk {
    Pk::new().u64(id.0)
}

fn field_pk(id: ObjectId, ordinal: u16) -> Pk {
    Pk::new().u64(id.0).u32(u32::from(ordinal))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("types catalog: {what}"))
}

/// A `types.discriminator` value: a root's column, or a subtype's value.
fn discriminator_json(t: &TypeDef) -> Option<serde_json::Value> {
    if let Some(c) = &t.discriminator {
        return Some(match &t.disc_operand {
            Some(o) => serde_json::json!({ "column": c, "operand": o }),
            None => serde_json::json!({ "column": c }),
        });
    }
    t.disc_value.as_ref().map(DiscValue::to_json)
}

/// Reads [`discriminator_json`]'s value back.
#[cfg(test)]
fn parse_discriminator(
    j: Option<&serde_json::Value>,
) -> Result<(Option<String>, Option<DiscValue>)> {
    parse_discriminator_full(j).map(|(c, v, _)| (c, v))
}

/// [`parse_discriminator`], with a root's operand.
fn parse_discriminator_full(
    v: Option<&serde_json::Value>,
) -> Result<(Option<String>, Option<DiscValue>, Option<String>)> {
    let Some(v) = v else {
        return Ok((None, None, None));
    };
    if !v.is_object() {
        return Err(bad("discriminator"));
    }
    if let Some(c) = v.get("column").and_then(serde_json::Value::as_str) {
        let o = v
            .get("operand")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        return Ok((Some(c.to_owned()), None, o));
    }
    if let Some(s) = v.get("value").and_then(serde_json::Value::as_str) {
        return Ok((None, Some(DiscValue::Str(s.to_owned())), None));
    }
    if let Some(n) = v.get("number").and_then(serde_json::Value::as_str) {
        return Ok((None, Some(DiscValue::Num(n.to_owned())), None));
    }
    Ok((None, None, None))
}

/// The JSON of a hierarchy's `key_fields`: `[{"name":..,"class":..,"type":..}]`.
fn key_fields_json(h: &HierarchyDef) -> serde_json::Value {
    serde_json::json!([{ "name": h.key, "class": h.key_class.as_str(), "type": h.key_type }])
}

/// A table's object as the loader needs it: its database and name.
pub type ObjectNames = HashMap<ObjectId, (DbId, String)>;

/// Reads and writes the module's records.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypesStore;

impl TypesStore {
    /// Every type row, field row and hierarchy row.
    ///
    /// # Errors
    /// The record store's; `Internal` for a row that doesn't decode.
    pub async fn rows(
        rec: &mut dyn RecordTx,
    ) -> Result<(Vec<TypeRow>, Vec<FieldRow>, Vec<HierRow>)> {
        let types = TYPES.rows(rec, &Pk::new()).await?;
        let fields = TYPE_FIELDS.rows(rec, &Pk::new()).await?;
        let hierarchies = HIERARCHIES.rows(rec, &Pk::new()).await?;
        Ok((types, fields, hierarchies))
    }

    /// The state from the rows, keeping entity types and table
    /// hierarchies whose object is live (in `names`).
    ///
    /// # Errors
    /// `Internal` for a malformed row.
    pub fn build(
        types: &[TypeRow],
        fields: &[FieldRow],
        hierarchies: &[HierRow],
        names: &ObjectNames,
    ) -> Result<TypesState> {
        let mut out = Vec::with_capacity(types.len());
        for r in types {
            if r.kind != "entity" {
                continue;
            }
            let id = ObjectId(r.type_id);
            let Some((_, name)) = names.get(&id) else {
                continue;
            };
            let (discriminator, disc_value, disc_operand) =
                parse_discriminator_full(r.discriminator.as_ref())?;
            out.push(TypeDef {
                id,
                db: DbId(r.db_id),
                parent: r.parent_id.map(ObjectId),
                root: ObjectId(r.root_id),
                depth: r.depth,
                path: r.type_path.clone(),
                is_abstract: r.is_abstract,
                storage: Storage::parse(&r.storage).ok_or_else(|| bad("storage"))?,
                key_fields: r.key_fields.clone(),
                own_fields: Vec::new(),
                state: TypeState::parse(&r.state).ok_or_else(|| bad("state"))?,
                version: r.version,
                name: name.clone(),
                discriminator,
                disc_value,
                disc_operand,
            });
        }
        // Field rows come in (type, ordinal) order.
        for f in fields {
            let id = ObjectId(f.type_id);
            let Some(t) = out.iter_mut().find(|t| t.id == id) else {
                continue;
            };
            t.own_fields.push(FieldDef {
                name: f.field_name.clone(),
                ordinal: f.ordinal,
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                default_expr: f.default_expr.clone(),
                physical_table: f.physical_table.clone().unwrap_or_else(|| t.name.clone()),
            });
        }
        let mut hs = Vec::with_capacity(hierarchies.len());
        for h in hierarchies {
            if h.model != "table" {
                continue;
            }
            let id = ObjectId(h.object_id);
            let Some((db, table)) = names.get(&id) else {
                continue;
            };
            let key = h.key_fields.get(0).ok_or_else(|| bad("key_fields"))?;
            hs.push(HierarchyDef {
                object_id: id,
                db: *db,
                table: table.clone(),
                key: key["name"].as_str().ok_or_else(|| bad("key"))?.to_owned(),
                key_class: KeyClass::parse(key["class"].as_str().unwrap_or(""))
                    .ok_or_else(|| bad("key class"))?,
                key_type: key["type"].as_str().unwrap_or("").to_owned(),
                parent: h
                    .parent_fields
                    .first()
                    .cloned()
                    .ok_or_else(|| bad("parent"))?,
                on_delete: if h.on_delete == "cascade" {
                    OnDelete::Cascade
                } else {
                    OnDelete::Restrict
                },
                max_depth: h.max_depth,
                state: HierState::parse(&h.state).ok_or_else(|| bad("hierarchy state"))?,
            });
        }
        Ok(TypesState::new(out, hs))
    }

    /// Applies `change` to the records of a catalog change (its
    /// `MariaTx::records`). A type path already taken in the database by
    /// another type is refused with 1062, as the unique key did.
    ///
    /// # Errors
    /// 1062 for a duplicate path; the record store's.
    pub async fn apply(rec: &mut dyn RecordTx, change: &TypesChange) -> Result<()> {
        let now = now_ms();
        for id in &change.delete_types {
            Self::delete_fields(rec, *id).await?;
            TYPES.delete(rec, &type_pk(*id)).await?;
        }
        for (id, state) in &change.type_states {
            if let Some(mut row) = TYPES.get::<TypeRow>(rec, &type_pk(*id)).await? {
                state.as_str().clone_into(&mut row.state);
                row.version += 1;
                row.updated_at_ms = now;
                TYPES.put(rec, &type_pk(*id), &row).await?;
            }
        }
        for t in &change.upsert_types {
            let taken = TYPES
                .rows::<TypeRow>(rec, &Pk::new())
                .await?
                .into_iter()
                .any(|r| r.db_id == t.db.0 && r.type_path == t.path && r.type_id != t.id.0);
            if taken {
                return Err(SprigError::backend(
                    1062,
                    *b"23000",
                    "catalog statement failed (1062)",
                ));
            }
            let old = TYPES.get::<TypeRow>(rec, &type_pk(t.id)).await?;
            let row = TypeRow {
                type_id: t.id.0,
                db_id: t.db.0,
                parent_id: t.parent.map(|p| p.0),
                root_id: t.root.0,
                depth: t.depth,
                type_path: t.path.clone(),
                kind: "entity".to_owned(),
                is_abstract: t.is_abstract,
                storage: t.storage.as_str().to_owned(),
                discriminator: discriminator_json(t),
                key_fields: t.key_fields.clone(),
                physical_table: Some(t.name.clone()),
                state: t.state.as_str().to_owned(),
                version: old.as_ref().map_or(1, |o| o.version + 1),
                created_at_ms: old.as_ref().map_or(now, |o| o.created_at_ms),
                updated_at_ms: now,
            };
            TYPES.put(rec, &type_pk(t.id), &row).await?;
            if let Some(fields) = change.fields_of(t.id) {
                Self::replace_fields(rec, t.id, fields).await?;
            }
        }
        for (id, fields) in &change.fields {
            if !change.upsert_types.iter().any(|t| t.id == *id) {
                Self::replace_fields(rec, *id, fields).await?;
            }
        }
        for id in &change.delete_hierarchies {
            HIERARCHIES.delete(rec, &type_pk(*id)).await?;
        }
        for h in &change.upsert_hierarchies {
            let old = HIERARCHIES
                .get::<HierRow>(rec, &type_pk(h.object_id))
                .await?;
            let row = HierRow {
                object_id: h.object_id.0,
                model: "table".to_owned(),
                key_fields: key_fields_json(h),
                parent_fields: vec![h.parent.clone()],
                on_delete: h.on_delete.as_str().to_owned(),
                max_depth: h.max_depth,
                state: h.state.as_str().to_owned(),
                created_at_ms: old.as_ref().map_or(now, |o| o.created_at_ms),
                updated_at_ms: now,
            };
            HIERARCHIES.put(rec, &type_pk(h.object_id), &row).await?;
        }
        Ok(())
    }

    async fn delete_fields(rec: &mut dyn RecordTx, id: ObjectId) -> Result<()> {
        for (pk, _) in TYPE_FIELDS
            .scan::<FieldRow>(rec, &Pk::new().u64(id.0))
            .await?
        {
            TYPE_FIELDS.delete(rec, &pk).await?;
        }
        Ok(())
    }

    async fn replace_fields(
        rec: &mut dyn RecordTx,
        id: ObjectId,
        fields: &[FieldDef],
    ) -> Result<()> {
        Self::delete_fields(rec, id).await?;
        for (i, f) in fields.iter().enumerate() {
            let ordinal = u16::try_from(i + 1).unwrap_or(u16::MAX);
            let row = FieldRow {
                type_id: id.0,
                field_name: f.name.clone(),
                ordinal,
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                default_expr: f.default_expr.clone(),
                physical_table: Some(f.physical_table.clone()),
                physical_column: Some(f.name.clone()),
            };
            TYPE_FIELDS.put(rec, &field_pk(id, ordinal), &row).await?;
        }
        Ok(())
    }
}

/// The live objects among `ids`, with their database and name, read from
/// the catalog's `objects` in `q` (the snapshot's transaction).
///
/// # Errors
/// 39070 or a backend error.
pub async fn object_names<Q: Queryable + Send>(
    q: &mut Q,
    schema: &str,
    ids: &[ObjectId],
) -> Result<ObjectNames> {
    let mut out = ObjectNames::new();
    for chunk in ids.chunks(500) {
        let list = chunk
            .iter()
            .map(|i| i.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let rows: Vec<(u64, u32, String)> = q
            .query(format!(
                "SELECT object_id, db_id, name FROM {}.objects \
                 WHERE state <> 'dropped' AND object_id IN ({list})",
                quote(schema)
            ))
            .await
            .map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable").with_source(e)
            })?;
        for (id, db, name) in rows {
            out.insert(ObjectId(id), (DbId(db), name));
        }
    }
    Ok(out)
}

/// What one type DDL statement writes to the module's tables, committed in
/// one versioned change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TypesChange {
    /// Types created or replaced (with their fields, if in `fields`).
    pub upsert_types: Vec<TypeDef>,
    /// Types whose own fields are replaced.
    pub fields: Vec<(ObjectId, Vec<FieldDef>)>,
    /// State changes.
    pub type_states: Vec<(ObjectId, TypeState)>,
    /// Types removed with their fields.
    pub delete_types: Vec<ObjectId>,
    /// Hierarchies declared or changed.
    pub upsert_hierarchies: Vec<HierarchyDef>,
    /// Hierarchies removed.
    pub delete_hierarchies: Vec<ObjectId>,
}

impl TypesChange {
    fn fields_of(&self, id: ObjectId) -> Option<&[FieldDef]> {
        self.fields
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, f)| f.as_slice())
    }

    /// Whether the change writes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &TypesChange::default()
    }
}

impl ExtState for TypesState {
    fn bytes(&self) -> usize {
        self.dbs()
            .map(|(_, d)| {
                d.iter()
                    .map(|t| 256 + t.own_fields.len() * 128)
                    .sum::<usize>()
                    + d.hierarchies().count() * 256
            })
            .sum()
    }
}

/// The module in the catalog cache: [`TypesState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct TypesExtension;

#[async_trait::async_trait]
impl CatalogExtension for TypesExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        TYPES_MIGRATIONS
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
            return Ok(Arc::new(TypesState::unmigrated()));
        }
        let (types, fields, hierarchies) = TypesStore::rows(tx.records()?).await?;
        let mut ids: Vec<ObjectId> = types.iter().map(|t| ObjectId(t.type_id)).collect();
        ids.extend(hierarchies.iter().map(|h| ObjectId(h.object_id)));
        let schema = tx.schema().to_owned();
        let names = object_names(tx.conn(), &schema, &ids).await?;
        Ok(Arc::new(TypesStore::build(
            &types,
            &fields,
            &hierarchies,
            &names,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_catalog::records::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    #[test]
    fn module_has_no_mariadb_tables() {
        Migrator::new(&[TYPES_MODULE]).unwrap();
        assert_eq!(TYPES_MODULE.newest(), 0);
    }

    #[test]
    fn discriminators_round_trip_as_json() {
        let mut t = (**crate::model::fixtures::assets()
            .get("asset", false)
            .unwrap())
        .clone();
        assert_eq!(discriminator_json(&t), None);
        t.discriminator = Some("vtype".into());
        let j = discriminator_json(&t);
        assert_eq!(
            parse_discriminator(j.as_ref()).unwrap(),
            (Some("vtype".into()), None)
        );
        t.discriminator = None;
        for v in [
            DiscValue::Str("TRUCK 'x'".into()),
            DiscValue::Num("-5".into()),
        ] {
            t.disc_value = Some(v.clone());
            assert_eq!(
                parse_discriminator(discriminator_json(&t).as_ref()).unwrap(),
                (None, Some(v))
            );
        }
    }

    #[test]
    fn hierarchy_keys_round_trip_as_json() {
        let db = crate::model::fixtures::assets();
        let h = db.hierarchy("org_unit", false).unwrap();
        let j = key_fields_json(h);
        assert_eq!(j[0]["name"], "id");
        assert_eq!(j[0]["class"], "int");
    }

    const T: TenantId = TenantId(11);

    /// Every type and hierarchy of the fixture, with names for all.
    fn fixture() -> (Vec<TypeDef>, Vec<HierarchyDef>, ObjectNames) {
        let db = crate::model::fixtures::assets();
        let types: Vec<TypeDef> = db.iter().map(|t| (**t).clone()).collect();
        let hs: Vec<HierarchyDef> = db.hierarchies().map(|h| (**h).clone()).collect();
        let mut names = ObjectNames::new();
        for t in &types {
            names.insert(t.id, (t.db, t.name.clone()));
        }
        for h in &hs {
            names.insert(h.object_id, (h.db, h.table.clone()));
        }
        (types, hs, names)
    }

    async fn commit(store: &MemRecords, change: &TypesChange) -> Result<()> {
        let mut tx = store.begin(T).await?;
        TypesStore::apply(&mut *tx, change).await?;
        tx.commit().await
    }

    async fn load(store: &MemRecords, names: &ObjectNames) -> TypesState {
        let mut tx = store.begin(T).await.unwrap();
        let (t, f, h) = TypesStore::rows(&mut *tx).await.unwrap();
        TypesStore::build(&t, &f, &h, names).unwrap()
    }

    fn same(a: &TypesState, b: &TypesState) {
        let mut x: Vec<_> = a
            .dbs()
            .flat_map(|(_, d)| d.iter().map(|t| (**t).clone()).collect::<Vec<_>>())
            .collect();
        let mut y: Vec<_> = b
            .dbs()
            .flat_map(|(_, d)| d.iter().map(|t| (**t).clone()).collect::<Vec<_>>())
            .collect();
        x.sort_by_key(|t| t.id);
        y.sort_by_key(|t| t.id);
        assert_eq!(x.len(), y.len());
        for (p, q) in x.iter().zip(&y) {
            assert_eq!(p.id, q.id);
            assert_eq!(p.path, q.path);
            // The store numbers fields 1, 2, ... in order, as the SQL did.
            let renumbered: Vec<FieldDef> = q
                .own_fields
                .iter()
                .enumerate()
                .map(|(i, f)| FieldDef {
                    ordinal: u16::try_from(i + 1).unwrap(),
                    ..f.clone()
                })
                .collect();
            assert_eq!(p.own_fields, renumbered);
            assert_eq!(p.discriminator, q.discriminator);
            assert_eq!(p.disc_value, q.disc_value);
            assert_eq!(p.storage, q.storage);
            assert_eq!(p.state, q.state);
            assert_eq!(p.key_fields, q.key_fields);
        }
        let mut hx: Vec<_> = a
            .dbs()
            .flat_map(|(_, d)| d.hierarchies().map(|h| (**h).clone()).collect::<Vec<_>>())
            .collect();
        let mut hy: Vec<_> = b
            .dbs()
            .flat_map(|(_, d)| d.hierarchies().map(|h| (**h).clone()).collect::<Vec<_>>())
            .collect();
        hx.sort_by_key(|h| h.object_id);
        hy.sort_by_key(|h| h.object_id);
        assert_eq!(hx, hy);
    }

    #[tokio::test]
    async fn a_change_round_trips_through_records() {
        let (types, hs, names) = fixture();
        let store = MemRecords::new();
        let change = TypesChange {
            fields: types.iter().map(|t| (t.id, t.own_fields.clone())).collect(),
            upsert_types: types.clone(),
            upsert_hierarchies: hs.clone(),
            ..TypesChange::default()
        };
        commit(&store, &change).await.unwrap();
        let got = load(&store, &names).await;
        same(&got, &TypesState::new(types.clone(), hs.clone()));
        // A type whose object is gone (dropped table) isn't loaded.
        let mut fewer = names.clone();
        fewer.remove(&types[0].id);
        let got = load(&store, &fewer).await;
        assert_eq!(
            got.dbs().map(|(_, d)| d.iter().count()).sum::<usize>(),
            types.len() - 1
        );
    }

    #[tokio::test]
    async fn states_fields_and_deletes_follow_the_sql_rules() {
        let (types, hs, names) = fixture();
        let store = MemRecords::new();
        commit(
            &store,
            &TypesChange {
                fields: types.iter().map(|t| (t.id, t.own_fields.clone())).collect(),
                upsert_types: types.clone(),
                upsert_hierarchies: hs.clone(),
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let t0 = types[0].id;
        // A state change bumps the row's version.
        commit(
            &store,
            &TypesChange {
                type_states: vec![(t0, TypeState::Dropping)],
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let mut tx = store.begin(T).await.unwrap();
        let row: TypeRow = TYPES.get(&mut *tx, &type_pk(t0)).await.unwrap().unwrap();
        assert_eq!(row.state, "dropping");
        assert_eq!(row.version, 2);
        drop(tx);
        // Fields replaced: fewer of them, ordinals renumbered from 1.
        let mut f = types[0].own_fields.clone();
        if !f.is_empty() {
            f.remove(0);
        }
        commit(
            &store,
            &TypesChange {
                fields: vec![(t0, f.clone())],
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let got = load(&store, &names).await;
        let t = got
            .dbs()
            .find_map(|(_, d)| d.iter().find(|t| t.id == t0).cloned())
            .unwrap();
        assert_eq!(t.own_fields.len(), f.len());
        for (i, x) in t.own_fields.iter().enumerate() {
            assert_eq!(usize::from(x.ordinal), i + 1);
        }
        // Delete the type and the hierarchies: no row of them is left.
        commit(
            &store,
            &TypesChange {
                delete_types: vec![t0],
                delete_hierarchies: hs.iter().map(|h| h.object_id).collect(),
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let mut tx = store.begin(T).await.unwrap();
        let (t, f, h) = TypesStore::rows(&mut *tx).await.unwrap();
        assert!(t.iter().all(|r| r.type_id != t0.0));
        assert!(f.iter().all(|r| r.type_id != t0.0));
        assert!(h.is_empty());
    }

    #[tokio::test]
    async fn a_taken_path_is_refused_like_the_unique_key() {
        let (types, _, _) = fixture();
        let store = MemRecords::new();
        commit(
            &store,
            &TypesChange {
                upsert_types: vec![types[0].clone()],
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let mut dup = types[0].clone();
        dup.id = ObjectId(dup.id.0 + 1000);
        let e = commit(
            &store,
            &TypesChange {
                upsert_types: vec![dup],
                ..TypesChange::default()
            },
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("1062"), "{e}");
        // Re-upserting the same type is fine and bumps its version.
        commit(
            &store,
            &TypesChange {
                upsert_types: vec![types[0].clone()],
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        let mut tx = store.begin(T).await.unwrap();
        let row: TypeRow = TYPES
            .get(&mut *tx, &type_pk(types[0].id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.version, 2);
    }

    /// A change cut off at any write leaves either nothing (the record
    /// transaction is never committed) or all of it.
    #[tokio::test]
    async fn an_uncommitted_change_leaves_nothing() {
        let (types, hs, _) = fixture();
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        TypesStore::apply(
            &mut *tx,
            &TypesChange {
                upsert_types: types,
                upsert_hierarchies: hs,
                ..TypesChange::default()
            },
        )
        .await
        .unwrap();
        drop(tx);
        assert!(store.dump(T).is_empty());
    }
}
