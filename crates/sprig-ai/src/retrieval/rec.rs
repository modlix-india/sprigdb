//! Retrieval indexes of engine databases in the tenant's catalog records:
//! one JSON row per index under `m/ai_retrieval/indexes/<id>`, ids from
//! the records counter `ai_retrieval`, and each taken name under
//! `m/ai_retrieval/names/<db>/<name>`.
//!
//! The same [`RetrievalIndex`] as the MariaDB catalog's rows ([`super::store`]),
//! so the gateway's statements and searches read either. On the engine an
//! index has one chunk table (the source's database has no shards), and
//! `progress` holds the in-node job's state:
//!
//! ```text
//! {"fill": <last key filled, as text values> | "done",
//!  "token": <change-log resume token after the changes applied>,
//!  "since_ms": <creation time: the changes followed start there>}
//! ```
//!
//! A name is taken by locking its record, so of two creations of one name
//! at once the second finds it taken.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::records::{Pk, RecordTx, RowTable, next_ids};
use sprig_common::Result;

use super::store::{NewIndex, RetrievalIndex, State};
use super::{FilterColumn, KeyColumn};
use crate::embedding::ModelRef;

/// The records table.
pub const TABLE: RowTable = RowTable::new("ai_retrieval", "indexes");

/// The id counter.
pub const COUNTER: &str = "ai_retrieval";

/// An index as stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Row {
    id: u64,
    db_id: u32,
    name: String,
    object_id: u64,
    text_column: String,
    title_column: Option<String>,
    filters: Vec<FilterColumn>,
    key_columns: Vec<KeyColumn>,
    chunking: String,
    model: ModelRef,
    state: String,
    progress: Value,
    rows_indexed: u64,
    last_error: Option<String>,
    created_by: Option<u32>,
}

impl From<Row> for RetrievalIndex {
    fn from(r: Row) -> Self {
        RetrievalIndex {
            id: r.id,
            db_id: r.db_id,
            name: r.name,
            object_id: r.object_id,
            text_column: r.text_column,
            title_column: r.title_column,
            filters: r.filters,
            key_columns: r.key_columns,
            chunking: r.chunking,
            model: r.model,
            state: State::parse(&r.state),
            progress: r.progress,
            rows_indexed: r.rows_indexed,
            last_error: r.last_error,
            created_by: r.created_by,
        }
    }
}

impl From<&RetrievalIndex> for Row {
    fn from(r: &RetrievalIndex) -> Self {
        Row {
            id: r.id,
            db_id: r.db_id,
            name: r.name.clone(),
            object_id: r.object_id,
            text_column: r.text_column.clone(),
            title_column: r.title_column.clone(),
            filters: r.filters.clone(),
            key_columns: r.key_columns.clone(),
            chunking: r.chunking.clone(),
            model: r.model.clone(),
            state: r.state.as_str().to_owned(),
            progress: r.progress.clone(),
            rows_indexed: r.rows_indexed,
            last_error: r.last_error.clone(),
            created_by: r.created_by,
        }
    }
}

fn pk(id: u64) -> Pk {
    Pk::new().u64(id)
}

/// The names taken: (database, name without case) to the index's id.
const NAMES: RowTable = RowTable::new("ai_retrieval", "names");

fn name_pk(db: u32, name: &str) -> Pk {
    Pk::new().u32(db).bytes(name.to_lowercase().as_bytes())
}

/// Every index of the tenant, by id.
///
/// # Errors
/// The store's.
pub async fn all(tx: &mut dyn RecordTx) -> Result<Vec<RetrievalIndex>> {
    Ok(TABLE
        .rows::<Row>(tx, &Pk::new())
        .await?
        .into_iter()
        .map(Into::into)
        .collect())
}

/// The indexes of database `db_id`.
///
/// # Errors
/// The store's.
pub async fn of_db(tx: &mut dyn RecordTx, db_id: u32) -> Result<Vec<RetrievalIndex>> {
    Ok(all(tx)
        .await?
        .into_iter()
        .filter(|i| i.db_id == db_id)
        .collect())
}

/// The index of database `db_id` named `name` (names compare without
/// case).
///
/// # Errors
/// The store's.
pub async fn by_name(
    tx: &mut dyn RecordTx,
    db_id: u32,
    name: &str,
) -> Result<Option<RetrievalIndex>> {
    Ok(of_db(tx, db_id)
        .await?
        .into_iter()
        .find(|i| i.name.eq_ignore_ascii_case(name)))
}

/// Index `id`.
///
/// # Errors
/// The store's.
pub async fn get(tx: &mut dyn RecordTx, id: u64) -> Result<Option<RetrievalIndex>> {
    Ok(TABLE.get::<Row>(tx, &pk(id)).await?.map(Into::into))
}

/// Index `id` as it is now, locked until `tx` ends: what a write of the
/// index's state reads first, so a `DROP` in between is never undone.
///
/// # Errors
/// The store's (39071 after a lock wait).
pub async fn get_for_update(tx: &mut dyn RecordTx, id: u64) -> Result<Option<RetrievalIndex>> {
    Ok(TABLE
        .get_for_update::<Row>(tx, &pk(id))
        .await?
        .map(Into::into))
}

/// Records a new index (state `building`); its id, or `None` when the
/// database has an index of that name. Commit `tx` to keep it.
///
/// # Errors
/// The store's.
pub async fn insert(tx: &mut dyn RecordTx, n: &NewIndex) -> Result<Option<u64>> {
    // The name's record, locked and read as it is now: of two creations
    // of one name at once, the second waits and then sees the first's.
    let name_pk = name_pk(n.db_id, &n.name);
    if NAMES.get_for_update::<u64>(tx, &name_pk).await?.is_some() {
        return Ok(None);
    }
    let id = next_ids(tx, COUNTER, 1).await?;
    NAMES.put(tx, &name_pk, &id).await?;
    let row = Row {
        id,
        db_id: n.db_id,
        name: n.name.clone(),
        object_id: n.object_id,
        text_column: n.text_column.clone(),
        title_column: n.title_column.clone(),
        filters: n.filters.clone(),
        key_columns: n.key_columns.clone(),
        chunking: n.chunking.clone(),
        model: n.model.clone(),
        state: State::Building.as_str().to_owned(),
        progress: n.progress.clone(),
        rows_indexed: 0,
        last_error: None,
        created_by: n.created_by,
    };
    TABLE.put(tx, &pk(id), &row).await?;
    Ok(Some(id))
}

/// Writes `ix` back (state, progress, rows, error). Commit `tx` to keep
/// it.
///
/// # Errors
/// The store's.
pub async fn put(tx: &mut dyn RecordTx, ix: &RetrievalIndex) -> Result<()> {
    TABLE.put(tx, &pk(ix.id), &Row::from(ix)).await
}

/// Whether index `ix` is being dropped: its record stays until its chunk
/// table is gone, and nothing searches, lists or fills it meanwhile.
#[must_use]
pub fn dropping(ix: &RetrievalIndex) -> bool {
    ix.progress.get("dropping") == Some(&Value::Bool(true))
}

/// Marks index `id` as being dropped ([`dropping`]); `false` when it is
/// gone. Commit `tx` to keep it.
///
/// # Errors
/// The store's.
pub async fn mark_dropping(tx: &mut dyn RecordTx, id: u64) -> Result<bool> {
    let Some(mut ix) = get_for_update(tx, id).await? else {
        return Ok(false);
    };
    if let Some(o) = ix.progress.as_object_mut() {
        o.insert("dropping".into(), Value::Bool(true));
    } else {
        ix.progress = serde_json::json!({"dropping": true});
    }
    put(tx, &ix).await?;
    Ok(true)
}

/// Forgets every index of database `db_id` (a dropped database: its chunk
/// tables went with it); how many. Commit `tx` to keep it.
///
/// # Errors
/// The store's.
pub async fn delete_of_db(tx: &mut dyn RecordTx, db_id: u32) -> Result<u64> {
    let mut n = 0;
    for ix in of_db(tx, db_id).await? {
        delete(tx, ix.id).await?;
        n += 1;
    }
    Ok(n)
}

/// Forgets index `id`. Commit `tx` to keep it.
///
/// # Errors
/// The store's.
pub async fn delete(tx: &mut dyn RecordTx, id: u64) -> Result<()> {
    if let Some(ix) = get(tx, id).await? {
        NAMES.delete(tx, &name_pk(ix.db_id, &ix.name)).await?;
    }
    TABLE.delete(tx, &pk(id)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_catalog::records::RecordStore as _;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    fn new(name: &str, db: u32) -> NewIndex {
        NewIndex {
            db_id: db,
            name: name.into(),
            object_id: 7,
            text_column: "body".into(),
            title_column: Some("title".into()),
            filters: Vec::new(),
            key_columns: vec![KeyColumn {
                name: "id".into(),
                column_type: "int".into(),
                binary: false,
                numeric: true,
            }],
            chunking: "tokens:400".into(),
            model: ModelRef::local(),
            progress: serde_json::json!({}),
            created_by: Some(3),
        }
    }

    #[tokio::test]
    async fn rows_round_trip_and_names_are_unique() {
        let store = MemRecords::default();
        let t = TenantId(5);
        let mut tx = store.begin(t).await.unwrap();
        let a = insert(tx.as_mut(), &new("kb", 1)).await.unwrap().unwrap();
        assert!(insert(tx.as_mut(), &new("KB", 1)).await.unwrap().is_none());
        let b = insert(tx.as_mut(), &new("kb", 2)).await.unwrap().unwrap();
        assert_ne!(a, b);
        tx.commit().await.unwrap();
        let mut tx = store.begin(t).await.unwrap();
        let mut ix = by_name(tx.as_mut(), 1, "Kb").await.unwrap().unwrap();
        assert_eq!(ix.state, State::Building);
        assert_eq!(ix.key_columns[0].name, "id");
        ix.state = State::Ready;
        ix.progress = serde_json::json!({"fill": "done", "token": "ab"});
        ix.rows_indexed = 12;
        put(tx.as_mut(), &ix).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = store.begin(t).await.unwrap();
        let got = get(tx.as_mut(), a).await.unwrap().unwrap();
        assert_eq!(got, ix);
        assert_eq!(all(tx.as_mut()).await.unwrap().len(), 2);
        assert!(mark_dropping(tx.as_mut(), a).await.unwrap());
        assert!(dropping(&get(tx.as_mut(), a).await.unwrap().unwrap()));
        assert!(!dropping(&get(tx.as_mut(), b).await.unwrap().unwrap()));
        delete(tx.as_mut(), a).await.unwrap();
        assert!(get(tx.as_mut(), a).await.unwrap().is_none());
        assert_eq!(of_db(tx.as_mut(), 2).await.unwrap().len(), 1);
        assert_eq!(delete_of_db(tx.as_mut(), 2).await.unwrap(), 1);
        assert!(all(tx.as_mut()).await.unwrap().is_empty());
        // The name is free again.
        assert!(insert(tx.as_mut(), &new("kb", 2)).await.unwrap().is_some());
    }
}
