//! The tenant catalog's document module (`doc`) as the engine's catalog
//! records, shared by the gateway (collections, their indexes and array
//! paths), the worker (the TTL monitor) and the controller (automatic
//! document indexes):
//!
//! ```text
//! m/doc/colls/<collection u64>                         CollRow
//! m/doc/cnames/<db u32><name>                          collection id
//! m/doc/indexes/<collection u64><index u32>            IndexRow
//! m/doc/fields/<collection u64><path hash u64><class>  FieldRow
//! ```
//!
//! A collection's id is the `objects.object_id` of its table. The name
//! rows stand for the unique key on a database's collection names. Every
//! write goes through a versioned catalog change's records.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bson::Document;
use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_common::{Result, SprigError};

use crate::meta::{self, CollMeta, IndexDef};

/// The module name.
pub const MODULE: &str = "doc";

/// Collections by id.
pub const COLLS: RowTable = RowTable::new(MODULE, "colls");
/// Collection ids by database and name.
pub const CNAMES: RowTable = RowTable::new(MODULE, "cnames");
/// Indexes by collection and id.
pub const INDEXES: RowTable = RowTable::new(MODULE, "indexes");
/// Promoted fields by collection, path hash and class.
pub const FIELDS: RowTable = RowTable::new(MODULE, "fields");

/// A taken unique name, as MariaDB's unique key answered it (1062).
fn duplicate(key: &str, name: &str) -> SprigError {
    SprigError::backend(
        1062,
        *b"23000",
        format!("Duplicate entry '{name}' for key '{key}'"),
    )
}

fn bson_doc(b: &[u8]) -> Document {
    if b.is_empty() {
        return Document::new();
    }
    bson::from_slice::<Document>(b).unwrap_or_default()
}

fn bson_bytes(d: &Document) -> Vec<u8> {
    bson::to_vec(d).unwrap_or_default()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A collection: what `collections` held. Byte fields are BSON documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollRow {
    /// Its id (its table's object id).
    pub collection_id: u64,
    /// Its database.
    pub db_id: u32,
    /// Its MongoDB name.
    pub name: String,
    /// Its table.
    pub table_name: String,
    /// The collection's UUID.
    pub coll_uuid: Vec<u8>,
    /// Paths known to hold arrays.
    pub array_paths: Vec<String>,
    /// Every path may hold arrays.
    pub array_paths_full: bool,
    /// Paths registered but not yet confirmed by every gateway.
    pub array_paths_pending: Option<Vec<String>>,
    /// The collection's options (BSON).
    pub options: Vec<u8>,
    /// The next index id.
    pub next_index: u32,
    /// `creating`, `ready` or `dropping`.
    pub state: String,
    /// Ms since the epoch.
    pub created_ms: u64,
}

/// An index: what `doc_indexes` held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexRow {
    /// Its MongoDB name.
    pub name: String,
    /// The key pattern (BSON).
    pub key_pattern: Vec<u8>,
    /// How it is built (`column`).
    pub implementation: String,
    /// Its MariaDB index name.
    pub mariadb_name: Option<String>,
    /// The field columns' slugs, in key order.
    pub field_columns: Vec<String>,
    /// Unique.
    pub unique: bool,
    /// Sparse.
    pub sparse: bool,
    /// TTL seconds.
    pub expire_seconds: Option<i64>,
    /// `system`, `user`, `auto` or `migrated`.
    pub origin: String,
    /// Other options (BSON).
    pub options: Vec<u8>,
    /// The automatic index it came from.
    pub auto_index_id: Option<u64>,
    /// `building`, `ready`, ...
    pub state: String,
    /// Ms since the epoch.
    pub created_ms: u64,
}

/// A promoted field: what `promoted_fields` held besides its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRow {
    /// The path.
    pub path: String,
    /// Its column slug.
    pub slug: String,
    /// The column types.
    pub sql_type: String,
    /// `user`, `auto` or `migrated`.
    pub origin: String,
    /// `adding`, `ready` or `dropping`.
    pub state: String,
    /// Ms since the epoch.
    pub created_ms: u64,
}

fn cid_pk(cid: u64) -> Pk {
    Pk::new().u64(cid)
}

fn name_pk(db: u32, name: &str) -> Pk {
    Pk::new().u32(db).bytes(name.as_bytes())
}

/// Index row `r` (id `id`) as the document engine's definition.
#[must_use]
pub fn index_of(id: u32, r: IndexRow) -> IndexDef {
    // `sprig_dix_<id>` is implied; only another name is kept.
    let mariadb = r.mariadb_name.filter(|m| *m != format!("sprig_dix_{id}"));
    let k = bson_doc(&r.key_pattern);
    let key = k
        .iter()
        .map(|(p, v)| {
            let d = crate::filter::as_i64(v).unwrap_or(1);
            (p.clone(), if d < 0 { -1 } else { 1 })
        })
        .collect();
    IndexDef {
        id,
        name: r.name,
        key,
        unique: r.unique,
        sparse: r.sparse,
        expire_after: r.expire_seconds,
        origin: r.origin,
        extra: bson_doc(&r.options),
        mariadb,
        auto_index_id: r.auto_index_id,
    }
}

/// Collection row `r` as the document engine's metadata, without its
/// indexes.
#[must_use]
pub fn coll_of(r: CollRow) -> CollMeta {
    let mut uuid = [0u8; 16];
    if r.coll_uuid.len() == 16 {
        uuid.copy_from_slice(&r.coll_uuid);
    }
    CollMeta {
        name: r.name,
        table: r.table_name,
        uuid,
        array_paths: r.array_paths.into_iter().collect(),
        arrays_full: r.array_paths_full,
        pending_paths: r
            .array_paths_pending
            .unwrap_or_default()
            .into_iter()
            .collect(),
        indexes: Vec::new(),
        options: bson_doc(&r.options),
        next_index: r.next_index.max(1),
    }
}

fn last_u32(pk: &Pk) -> Option<u32> {
    let b = pk.0.get(pk.0.len().checked_sub(4)?..)?;
    <[u8; 4]>::try_from(b).ok().map(u32::from_be_bytes)
}

/// The module's rows, read and written as catalog records.
#[derive(Debug, Clone, Copy, Default)]
pub struct DocTables;

impl DocTables {
    /// Every registered collection whose state is `ready`, by id, with its
    /// database and its ready indexes; [`DocExtension`] leaves out those
    /// whose table is gone.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed row.
    pub async fn load(rec: &mut dyn RecordTx) -> Result<HashMap<u64, (u32, CollMeta)>> {
        let mut by_id: HashMap<u64, (u32, CollMeta)> = COLLS
            .rows::<CollRow>(rec, &Pk::new())
            .await?
            .into_iter()
            .filter(|r| r.state == "ready")
            .map(|r| (r.collection_id, (r.db_id, coll_of(r))))
            .collect();
        for (pk, r) in INDEXES.scan::<IndexRow>(rec, &Pk::new()).await? {
            if r.state != "ready" {
                continue;
            }
            let (Some(cid), Some(id)) = (be_u64(&pk.0), last_u32(&pk)) else {
                continue;
            };
            if let Some((_, m)) = by_id.get_mut(&cid) {
                m.indexes.push(index_of(id, r));
            }
        }
        Ok(by_id)
    }

    /// Every collection id (any state).
    ///
    /// # Errors
    /// The record store's.
    pub async fn ids(rec: &mut dyn RecordTx) -> Result<Vec<u64>> {
        Ok(COLLS
            .rows::<CollRow>(rec, &Pk::new())
            .await?
            .into_iter()
            .map(|r| r.collection_id)
            .collect())
    }

    /// The collection `(db, name)`, its id and its row.
    ///
    /// # Errors
    /// The record store's.
    pub async fn find(
        rec: &mut dyn RecordTx,
        db: u32,
        name: &str,
    ) -> Result<Option<(u64, CollMeta)>> {
        let Some(cid) = CNAMES.get::<u64>(rec, &name_pk(db, name)).await? else {
            return Ok(None);
        };
        let Some(row) = COLLS.get::<CollRow>(rec, &cid_pk(cid)).await? else {
            return Ok(None);
        };
        let mut m = coll_of(row);
        m.indexes = INDEXES
            .scan::<IndexRow>(rec, &cid_pk(cid))
            .await?
            .into_iter()
            .filter_map(|(pk, r)| last_u32(&pk).map(|id| index_of(id, r)))
            .collect();
        Ok(Some((cid, m)))
    }

    /// Deletes collection `cid`'s rows.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete(rec: &mut dyn RecordTx, cid: u64) -> Result<()> {
        Self::delete_indexes(rec, cid).await?;
        if let Some(r) = COLLS.get::<CollRow>(rec, &cid_pk(cid)).await? {
            let npk = name_pk(r.db_id, &r.name);
            if CNAMES.get::<u64>(rec, &npk).await? == Some(cid) {
                CNAMES.delete(rec, &npk).await?;
            }
            COLLS.delete(rec, &cid_pk(cid)).await?;
        }
        Ok(())
    }

    async fn delete_indexes(rec: &mut dyn RecordTx, cid: u64) -> Result<()> {
        for t in [FIELDS, INDEXES] {
            for (pk, _) in t.scan::<serde_json::Value>(rec, &cid_pk(cid)).await? {
                t.delete(rec, &pk).await?;
            }
        }
        Ok(())
    }

    /// Changes collection `cid`'s row with `f` (nothing if it's gone).
    ///
    /// # Errors
    /// The record store's.
    pub async fn update_row(
        rec: &mut dyn RecordTx,
        cid: u64,
        f: impl FnOnce(&mut CollRow) + Send,
    ) -> Result<()> {
        if let Some(mut r) = COLLS.get::<CollRow>(rec, &cid_pk(cid)).await? {
            f(&mut r);
            COLLS.put(rec, &cid_pk(cid), &r).await?;
        }
        Ok(())
    }

    /// Rewrites collection `cid`'s indexes and field-column rows.
    ///
    /// # Errors
    /// The record store's (a backend 1062 for two indexes of one name).
    pub async fn write_indexes(rec: &mut dyn RecordTx, cid: u64, m: &CollMeta) -> Result<()> {
        Self::delete_indexes(rec, cid).await?;
        let now = now_ms();
        let mut names = BTreeSet::new();
        let mut paths: BTreeMap<String, String> = BTreeMap::new();
        for i in &m.indexes {
            if !names.insert(i.name.as_str()) {
                return Err(duplicate("uk_index_name", &i.name));
            }
            let slugs: Vec<String> = i.key.iter().map(|(p, _)| meta::slug(p)).collect();
            for (p, _) in &i.key {
                paths.entry(p.clone()).or_insert_with(|| i.origin.clone());
            }
            let origin = match i.origin.as_str() {
                "migrated" | "auto" | "system" => i.origin.as_str(),
                _ => "user",
            };
            let row = IndexRow {
                name: i.name.clone(),
                key_pattern: bson_bytes(&i.key_doc()),
                implementation: "column".to_owned(),
                mariadb_name: Some(i.mariadb_name()),
                field_columns: slugs,
                unique: i.unique,
                sparse: i.sparse,
                expire_seconds: i.expire_after,
                origin: origin.to_owned(),
                options: bson_bytes(&i.extra),
                auto_index_id: i.auto_index_id,
                state: "ready".to_owned(),
                created_ms: now,
            };
            INDEXES
                .put(rec, &Pk::new().u64(cid).u32(i.id), &row)
                .await?;
        }
        for (p, origin) in paths {
            let origin = if matches!(origin.as_str(), "migrated" | "auto") {
                origin
            } else {
                "user".to_owned()
            };
            let row = FieldRow {
                slug: meta::slug(&p),
                sql_type: format!("TINYINT,DOUBLE,VARCHAR({})", meta::S_CHARS),
                path: p.clone(),
                origin,
                state: "ready".to_owned(),
                created_ms: now,
            };
            FIELDS
                .put(
                    rec,
                    &Pk::new()
                        .u64(cid)
                        .u64(meta::fnv64(p.as_bytes()))
                        .bytes(b"pair"),
                    &row,
                )
                .await?;
        }
        let (next, options) = (m.next_index, bson_bytes(&m.options));
        Self::update_row(rec, cid, move |r| {
            r.next_index = next;
            r.options = options;
        })
        .await
    }

    /// Inserts collection `cid` with all of `m`.
    ///
    /// # Errors
    /// The record store's (a backend 1062 for a name in use).
    pub async fn insert(rec: &mut dyn RecordTx, cid: u64, db: u32, m: &CollMeta) -> Result<()> {
        let npk = name_pk(db, &m.name);
        if CNAMES.get::<u64>(rec, &npk).await?.is_some() {
            return Err(duplicate("uk_coll_name", &m.name));
        }
        let row = CollRow {
            collection_id: cid,
            db_id: db,
            name: m.name.clone(),
            table_name: m.table.clone(),
            coll_uuid: m.uuid.to_vec(),
            array_paths: m.array_paths.iter().cloned().collect(),
            array_paths_full: m.arrays_full,
            array_paths_pending: None,
            options: bson_bytes(&m.options),
            next_index: m.next_index,
            state: "ready".to_owned(),
            created_ms: now_ms(),
        };
        COLLS.put(rec, &cid_pk(cid), &row).await?;
        CNAMES.put(rec, &npk, &cid).await?;
        Self::write_indexes(rec, cid, m).await
    }
}

impl DocTables {
    /// The automatic indexes registered: `(collection, index id, automatic
    /// index id)`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn auto_indexes(rec: &mut dyn RecordTx) -> Result<Vec<(u64, u32, Option<u64>)>> {
        let mut out = Vec::new();
        for (pk, r) in INDEXES.scan::<IndexRow>(rec, &Pk::new()).await? {
            if r.origin != "auto" {
                continue;
            }
            if let (Some(cid), Some(id)) = (be_u64(&pk.0), last_u32(&pk)) {
                out.push((cid, id, r.auto_index_id));
            }
        }
        Ok(out)
    }

    /// Deletes index `id` of collection `cid` (its row only, as the
    /// registry of automatic indexes did).
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_index(rec: &mut dyn RecordTx, cid: u64, id: u32) -> Result<()> {
        INDEXES.delete(rec, &Pk::new().u64(cid).u32(id)).await
    }

    /// Registers automatic index `auto_id` named `name` on collection
    /// `cid` over `paths` (each with its direction), with the collection's
    /// next index id, and its paths' field rows: that id, or `None` when
    /// `cid` is no collection.
    ///
    /// # Errors
    /// The record store's.
    pub async fn register_auto(
        rec: &mut dyn RecordTx,
        cid: u64,
        name: &str,
        auto_id: u64,
        paths: &[(String, i32)],
    ) -> Result<Option<u32>> {
        let Some(mut coll) = COLLS.get::<CollRow>(rec, &cid_pk(cid)).await? else {
            return Ok(None);
        };
        let next = coll.next_index;
        let mut key = Document::new();
        for (p, d) in paths {
            key.insert(p.clone(), *d);
        }
        let now = now_ms();
        let row = IndexRow {
            name: name.to_owned(),
            key_pattern: bson_bytes(&key),
            implementation: "column".to_owned(),
            mariadb_name: Some(name.to_owned()),
            field_columns: paths.iter().map(|(p, _)| meta::slug(p)).collect(),
            unique: false,
            sparse: false,
            expire_seconds: None,
            origin: "auto".to_owned(),
            options: bson_bytes(&Document::new()),
            auto_index_id: Some(auto_id),
            state: "ready".to_owned(),
            created_ms: now,
        };
        INDEXES
            .put(rec, &Pk::new().u64(cid).u32(next), &row)
            .await?;
        for (p, _) in paths {
            let pk = Pk::new()
                .u64(cid)
                .u64(meta::fnv64(p.as_bytes()))
                .bytes(b"pair");
            if FIELDS.get::<FieldRow>(rec, &pk).await?.is_some() {
                continue;
            }
            let f = FieldRow {
                path: p.clone(),
                slug: meta::slug(p),
                sql_type: format!("TINYINT,DOUBLE,VARCHAR({})", meta::S_CHARS),
                origin: "auto".to_owned(),
                state: "ready".to_owned(),
                created_ms: now,
            };
            FIELDS.put(rec, &pk, &f).await?;
        }
        coll.next_index = next + 1;
        COLLS.put(rec, &cid_pk(cid), &coll).await?;
        Ok(Some(next))
    }
}

fn be_u64(b: &[u8]) -> Option<u64> {
    b.get(..8)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .map(u64::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_catalog::records::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    const T: TenantId = TenantId(57_406_000_002);

    fn coll(name: &str, table: &str) -> CollMeta {
        CollMeta {
            name: name.into(),
            table: table.into(),
            uuid: [7; 16],
            array_paths: ["tags".to_owned()].into_iter().collect(),
            arrays_full: false,
            pending_paths: BTreeSet::new(),
            indexes: vec![idx(1, "a_1", "a"), idx(2, "b_-1", "b")],
            options: bson::doc! {"collation": {"locale": "fr"}},
            next_index: 3,
        }
    }

    fn idx(id: u32, name: &str, path: &str) -> IndexDef {
        IndexDef {
            id,
            name: name.into(),
            key: vec![(path.into(), if name.ends_with("-1") { -1 } else { 1 })],
            unique: id == 1,
            sparse: false,
            expire_after: (id == 2).then_some(60),
            origin: "user".into(),
            extra: Document::new(),
            mariadb: None,
            auto_index_id: None,
        }
    }

    #[tokio::test]
    async fn collections_round_trip_and_keep_their_rules() {
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        DocTables::insert(&mut *tx, 100, 1, &coll("users", "users"))
            .await
            .unwrap();
        DocTables::insert(&mut *tx, 101, 2, &coll("users", "users2"))
            .await
            .unwrap();
        // A database's collection names are unique (1062).
        let e = DocTables::insert(&mut *tx, 102, 1, &coll("users", "x"))
            .await
            .unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Backend { errno: 1062, .. }
        ));
        tx.commit().await.unwrap();

        let mut tx = store.begin(T).await.unwrap();
        let all = DocTables::load(&mut *tx).await.unwrap();
        assert_eq!(all.len(), 2);
        let (db, m) = &all[&100];
        assert_eq!(*db, 1);
        assert_eq!(m, &coll("users", "users"));
        let (cid, found) = DocTables::find(&mut *tx, 2, "users")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((cid, found.table.as_str()), (101, "users2"));
        assert!(
            DocTables::find(&mut *tx, 3, "users")
                .await
                .unwrap()
                .is_none()
        );

        // Indexes are rewritten whole; two of one name are refused.
        let mut m2 = found.clone();
        m2.indexes = vec![idx(4, "c_1", "c")];
        m2.next_index = 5;
        DocTables::write_indexes(&mut *tx, 101, &m2).await.unwrap();
        let mut dup = m2.clone();
        dup.indexes.push(idx(5, "c_1", "d"));
        assert!(DocTables::write_indexes(&mut *tx, 101, &dup).await.is_err());
        // Array paths and pending paths.
        DocTables::update_row(&mut *tx, 101, |r| {
            r.array_paths_full = true;
            r.array_paths.clear();
            r.array_paths_pending = Some(vec!["*".into()]);
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut tx = store.begin(T).await.unwrap();
        let (_, m) = DocTables::find(&mut *tx, 2, "users")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.indexes.len(), 1);
        assert_eq!((m.indexes[0].id, m.next_index), (4, 5));
        assert!(m.arrays_full && m.pending_paths.contains("*"));

        // A dropped collection frees its name and leaves no index or field.
        DocTables::delete(&mut *tx, 100).await.unwrap();
        DocTables::insert(&mut *tx, 103, 1, &coll("users", "users3"))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let mut tx = store.begin(T).await.unwrap();
        assert_eq!(DocTables::ids(&mut *tx).await.unwrap(), vec![101, 103]);
        assert!(
            INDEXES
                .scan::<IndexRow>(&mut *tx, &Pk::new().u64(100))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            FIELDS
                .scan::<FieldRow>(&mut *tx, &Pk::new().u64(100))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            FIELDS
                .scan::<FieldRow>(&mut *tx, &Pk::new().u64(103))
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn automatic_indexes_register_once_and_leave_with_their_row() {
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        DocTables::insert(&mut *tx, 200, 1, &coll("orders", "orders"))
            .await
            .unwrap();
        let paths = vec![("a".to_owned(), 1), ("z.y".to_owned(), -1)];
        assert_eq!(
            DocTables::register_auto(&mut *tx, 200, "sprig_auto_9", 9, &paths)
                .await
                .unwrap(),
            Some(3)
        );
        assert_eq!(
            DocTables::register_auto(&mut *tx, 999, "sprig_auto_8", 8, &paths)
                .await
                .unwrap(),
            None
        );
        tx.commit().await.unwrap();
        let mut tx = store.begin(T).await.unwrap();
        assert_eq!(
            DocTables::auto_indexes(&mut *tx).await.unwrap(),
            vec![(200, 3, Some(9))]
        );
        let (_, m) = DocTables::find(&mut *tx, 1, "orders")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m.next_index, 4);
        let auto = m.indexes.iter().find(|i| i.id == 3).unwrap();
        assert_eq!(auto.key, paths);
        assert_eq!(auto.mariadb.as_deref(), Some("sprig_auto_9"));
        DocTables::delete_index(&mut *tx, 200, 3).await.unwrap();
        assert!(DocTables::auto_indexes(&mut *tx).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn only_ready_rows_load() {
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        DocTables::insert(&mut *tx, 300, 1, &coll("a", "a"))
            .await
            .unwrap();
        DocTables::insert(&mut *tx, 301, 1, &coll("b", "b"))
            .await
            .unwrap();
        DocTables::update_row(&mut *tx, 301, |r| r.state = "creating".into())
            .await
            .unwrap();
        let pk = Pk::new().u64(300).u32(2);
        let mut r = INDEXES
            .get::<IndexRow>(&mut *tx, &pk)
            .await
            .unwrap()
            .unwrap();
        r.state = "building".into();
        INDEXES.put(&mut *tx, &pk, &r).await.unwrap();
        let all = DocTables::load(&mut *tx).await.unwrap();
        assert_eq!(all.keys().copied().collect::<Vec<_>>(), vec![300]);
        assert_eq!(all[&300].1.indexes.len(), 1);
    }
}
