//! MongoDB collections read from SQL: a collection of an engine database
//! reads as a table, so a `SELECT` can filter, group and join it with
//! tables at one read view.
//!
//! ```text
//! a name in a FROM that is no table, view or sequence of an engine
//! database, but a collection of the MongoDB database of the same name
//!   → a table of three columns:
//!       _id     VARCHAR, utf8mb4 NO PAD binary: the _id as text
//!       doc     JSON: the whole document
//!       _types  JSON, invisible: the BSON types JSON can't show
//!   → read through the planner's storage (general::storage::coll)
//! ```
//!
//! - **The values.** `doc` is the document as the SQL document path of
//!   the earlier design rendered it (`sprig_doc::json::render`): `_id`
//!   first, the document's field order, `ObjectIds` as 24 hex digits, dates
//!   as ISO text, `NumberLong`/`NumberDecimal` as plain numbers, the rest
//!   in the table of `sprig_doc::json`. `_id` is the text `doc->>'$._id'`
//!   gives: a string as itself, any other value as its JSON. Fields are
//!   read by path with the JSON functions (`doc->>'$.a.b'`,
//!   `JSON_VALUE(doc, '$.n' RETURNING SIGNED)`), so a collection answers
//!   exactly as a table holding the same JSON would.
//! - **Keys.** The planner sees one unique key, on `_id`. An equality on
//!   `_id` (also as the inner side of a join) reads only the documents
//!   whose `_id` can print as that text: the stored keys of the values
//!   that print so ([`id_candidates`]), plus the few key classes whose
//!   values print without a way back. Every document read is checked
//!   against the text, so the key never changes an answer. Other ranges
//!   of `_id` read the collection and keep the rows in range, in `_id`
//!   order. A full scan reads in the store's `_id` order, which the
//!   planner doesn't rely on (the table has no primary key).
//! - **Writes** (`INSERT`, `REPLACE`, `UPDATE`, `DELETE`) run through the
//!   document store's write path in the SQL transaction
//!   ([`super::collwrite`]); a locking read locks the documents it reads.
//! - **Privileges** are the database's: a collection has no SQL grants of
//!   its own, so the session needs `SELECT` on the database (or wider).
//! - A MongoDB database is visible from SQL once the tenant has an SQL
//!   database of that name. MongoDB views aren't (they read as missing).

use std::sync::Arc;

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document};
use sprig_common::ids::{DbId, ObjectId};
use sprig_doc::docstore::layout;
use sprig_doc::json::{RenderOpts, ms_from_iso, render};
use sprig_doc::keyenc::{self, class};
use sprig_plan::catalog::TableDef as PlanTable;

use super::row::Datum;
use super::table::{self, TableDef};

/// The `_id` column.
pub(crate) const ID: usize = 0;
/// The `doc` column.
pub(crate) const DOC: usize = 1;
/// The `_types` column.
pub(crate) const TYPES: usize = 2;

/// A collection as a statement reads it.
#[derive(Debug)]
pub(crate) struct CollTable {
    /// The table it reads as (object: the collection's engine object).
    pub def: Arc<TableDef>,
    /// The tenant.
    pub tenant: u64,
    /// The MongoDB database.
    pub db: String,
    /// The collection's name.
    pub name: String,
    /// Its indexes a numeric condition on a field can read
    /// (`general::storage::coll`), each the planner's key after `_id_`.
    pub paths: Vec<PathIndex>,
}

/// A collection index the planner may read for a numeric condition on its
/// first field (`doc->>'$.f' = 7`, `JSON_EXTRACT(doc, '$.f') > 2`, ...): an
/// ascending index on a plain field path, neither sparse, partial, hidden,
/// hashed, wildcard nor being built, so every document has an entry.
#[derive(Debug, Clone)]
pub(crate) struct PathIndex {
    /// The index's number in the collection.
    pub id: u16,
    /// Unique (its entries are keyed without the `_id`).
    pub unique: bool,
    /// Its name.
    pub name: String,
    /// The JSON path of its first field (`$.a.b`).
    pub path: String,
}

/// The indexes of collection record `rec` SQL may read (see [`PathIndex`]).
fn path_indexes(rec: &Document, db: &str, name: &str) -> Vec<PathIndex> {
    use sprig_doc::docstore::catalog::{CollMeta, FieldKind};
    let Ok(meta) = CollMeta::from_record(db, name, rec) else {
        return Vec::new();
    };
    let plain = |f: &str| {
        !f.is_empty()
            && f.split('.').all(|seg| {
                seg.chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                    && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            })
    };
    meta.indexes
        .iter()
        .filter(|i| {
            !i.building && !i.hidden && !i.sparse && i.partial.is_none() && !i.is_wildcard()
        })
        .filter_map(|i| {
            let (f, kind) = i.fields.first()?;
            (*kind == FieldKind::Asc && f != "_id" && plain(f)).then(|| PathIndex {
                id: i.id,
                unique: i.unique,
                name: i.name.clone(),
                path: format!("$.{f}"),
            })
        })
        .collect()
}

/// The table the planner sees for collection `c`: [`plan_table`]'s, with
/// a key per [`PathIndex`] over `doc` at its path, its bounds as `DOUBLE`
/// (`KeyId::Index(1)` is the first).
pub(crate) fn plan_table_of(c: &CollTable, db_name: &str) -> PlanTable {
    use sprig_plan::catalog::{IndexDef, IndexPart, PathPart};
    let mut t = plan_table(&c.def, db_name);
    for p in &c.paths {
        t.indexes.push(
            IndexDef::new(&p.name, false, vec![IndexPart::asc(DOC)]).with_paths(vec![Some(
                PathPart {
                    path: p.path.clone(),
                    ty: sprig_engine::row::ColumnType::Double,
                },
            )]),
        );
    }
    t
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The table a collection reads as.
pub(crate) fn table_def(db: DbId, name: &str, n: u32) -> Option<Arc<TableDef>> {
    table_def_or(db, name, n).ok()
}

fn table_def_or(db: DbId, name: &str, n: u32) -> Result<Arc<TableDef>, String> {
    let text = format!(
        "CREATE TABLE {} (\n  `_id` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL,\n  `doc` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`doc`)),\n  `_types` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL INVISIBLE CHECK (json_valid(`_types`)),\n  UNIQUE KEY `_id_` (`_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_nopad_bin",
        quote(name)
    );
    let object = ObjectId(u64::from(layout::CATALOG) + u64::from(n));
    table::parse(object, 1, db, &text)
}

/// `def` (a collection's table) as the planner sees it: no primary key
/// (the store's order isn't `_id`'s text order), one unique key on `_id`.
pub(crate) fn plan_table(def: &TableDef, db_name: &str) -> PlanTable {
    let mut t = super::general::names::plan_table(def, db_name);
    if let Some(mut k) = t.primary.take() {
        "_id_".clone_into(&mut k.name);
        t.indexes.insert(0, k);
    }
    t
}

/// The record's collection number, unless it is a view.
fn record_n(v: &[u8]) -> Option<u32> {
    let rec = Document::from_reader(&mut &v[..]).ok()?;
    if rec
        .get_document("options")
        .is_ok_and(|o| o.contains_key("viewOn"))
    {
        return None;
    }
    u32::try_from(rec.get_i64("n").ok()?).ok()
}

/// The collection `name` of MongoDB database `db_name` (the SQL database
/// `db`'s name) of `tenant`, as the store holds it now; `None` when there
/// is none (or it is a view). `lower_case`: names compare without case
/// when no collection has the exact name.
pub(crate) fn find(
    eng: &sprig_engine::Engine,
    tenant: u64,
    db: DbId,
    db_name: &str,
    name: &str,
    lower_case: bool,
) -> Option<Arc<CollTable>> {
    let mut t = eng.begin(sprig_engine::txn::TxnOptions::default()).ok()?;
    let mut found = t
        .get_snapshot(&layout::coll_record(tenant, db_name, name))
        .ok()
        .flatten()
        .map(|v| (name.to_owned(), v));
    if found.is_none() && lower_case {
        let p = layout::coll_records(tenant, Some(db_name));
        let r = sprig_engine::keys::KeyRange::new(p.clone(), layout::after_prefix(&p));
        let rows = t
            .scan(
                &r,
                sprig_engine::txn::Dir::Forward,
                10_000,
                sprig_engine::txn::ReadMode::Snapshot,
            )
            .unwrap_or_default();
        let mut hits = rows.into_iter().filter_map(|(k, v)| {
            let (c, _) = layout::take_str(k.get(p.len()..)?)?;
            let c = String::from_utf8(c).ok()?;
            c.eq_ignore_ascii_case(name).then_some((c, v))
        });
        if let (Some(h), None) = (hits.next(), hits.next()) {
            found = Some(h);
        }
    }
    t.rollback();
    let (coll, v) = found?;
    let n = record_n(&v)?;
    let paths = Document::from_reader(&mut &v[..])
        .map(|rec| path_indexes(&rec, db_name, &coll))
        .unwrap_or_default();
    Some(Arc::new(CollTable {
        def: table_def(db, &coll, n)?,
        tenant,
        db: db_name.to_owned(),
        name: coll,
        paths,
    }))
}

/// The collection's keyspace now, from its record read at `get`'s view;
/// `None` when it no longer exists.
pub(crate) fn space_of(
    c: &CollTable,
    get: impl FnOnce(&[u8]) -> sprig_common::Result<Option<bytes::Bytes>>,
) -> sprig_common::Result<Option<layout::Space>> {
    let v = get(&layout::coll_record(c.tenant, &c.db, &c.name))?;
    Ok(v.as_deref()
        .and_then(record_n)
        .map(|n| layout::Space::new(c.tenant, n)))
}

/// The `_id` column's text of an `_id` value: what `doc->>'$._id'` gives.
pub(crate) fn id_text(v: &Bson) -> Option<String> {
    match v {
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => Some(s.clone()),
        _ => {
            let mut d = Document::new();
            d.insert("_id", v.clone());
            let (j, _) = render(&d, &RenderOpts { type_map: false }).ok()?;
            let inner = j.strip_prefix("{\"_id\":")?.strip_suffix('}')?;
            if inner.starts_with('"') {
                serde_json::from_str::<String>(inner).ok()
            } else {
                Some(inner.to_owned())
            }
        }
    }
}

/// A row of a collection's table from a stored document's value: only the
/// columns of `read` are computed, the others are `NULL`.
///
/// # Errors
/// The stored value isn't a document.
pub(crate) fn row(value: &[u8], read: &[bool]) -> Result<Vec<Datum>, String> {
    let d = Document::from_reader(&mut layout::doc_bson(value))
        .map_err(|e| format!("a stored document doesn't decode: {e}"))?;
    let wants = |c: usize| read.get(c).copied().unwrap_or(true);
    let mut out = vec![Datum::Null, Datum::Null, Datum::Null];
    if wants(ID) {
        let id = d.get("_id").and_then(id_text).unwrap_or_default();
        out[ID] = Datum::Str(bytes::Bytes::from(id));
    }
    if wants(DOC) || wants(TYPES) {
        let (j, types) = render(
            &d,
            &RenderOpts {
                type_map: wants(TYPES),
            },
        )
        .map_err(|e| e.to_string())?;
        if wants(DOC) {
            out[DOC] = Datum::Json(bytes::Bytes::from(j));
        }
        if let Some(t) = types.filter(|_| wants(TYPES)) {
            out[TYPES] = Datum::Json(bytes::Bytes::from(t));
        }
    }
    Ok(out)
}

/// The `_id` text of a row built by [`row`] (empty when it wasn't read).
pub(crate) fn row_id(r: &[Datum]) -> &[u8] {
    match r.get(ID) {
        Some(Datum::Str(s)) => s,
        _ => &[],
    }
}

/// Where the documents whose `_id` prints as `s` can be: the stored keys
/// (`keyenc`) of the values that print as `s`, and the key classes to
/// read whole, whose values print without a way back to the value.
pub(crate) fn id_candidates(s: &str) -> (Vec<Vec<u8>>, Vec<u8>) {
    let mut vals: Vec<Bson> = vec![Bson::String(s.to_owned())];
    let hex = |c: u8| c.is_ascii_digit() || (b'a'..=b'f').contains(&c);
    if s.len() == 24
        && s.bytes().all(hex)
        && let Ok(o) = bson::oid::ObjectId::parse_str(s)
    {
        vals.push(Bson::ObjectId(o));
    }
    if let Ok(i) = s.parse::<i64>() {
        vals.push(Bson::Int64(i));
        vals.push(Bson::DateTime(bson::DateTime::from_millis(i)));
    }
    if let Ok(f) = s.parse::<f64>() {
        vals.push(Bson::Double(f));
    }
    if let Ok(d) = s.parse::<bson::Decimal128>() {
        vals.push(Bson::Decimal128(d));
    }
    match s {
        "true" => vals.push(Bson::Boolean(true)),
        "false" => vals.push(Bson::Boolean(false)),
        "null" => vals.push(Bson::Null),
        "MinKey" => vals.push(Bson::MinKey),
        "MaxKey" => vals.push(Bson::MaxKey),
        "NaN" => vals.push(Bson::Double(f64::NAN)),
        "Infinity" => vals.push(Bson::Double(f64::INFINITY)),
        "-Infinity" => vals.push(Bson::Double(f64::NEG_INFINITY)),
        _ => {}
    }
    if let Some(ms) = ms_from_iso(s) {
        vals.push(Bson::DateTime(bson::DateTime::from_millis(ms)));
    }
    if let Some((t, i)) = s.split_once('.')
        && t.len() == 10
        && i.len() == 10
        && let (Ok(time), Ok(increment)) = (t.parse::<u32>(), i.parse::<u32>())
    {
        vals.push(Bson::Timestamp(bson::Timestamp { time, increment }));
    }
    let uuid = uuid_bytes(s);
    if let Some(b) = &uuid {
        vals.push(Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: b.clone(),
        }));
    }
    let mut keys: Vec<Vec<u8>> = vals.iter().map(keyenc::key).collect();
    keys.sort();
    keys.dedup();
    // Classes printed without a way back: code (as its text), documents
    // and code with scope (as JSON objects), other binary data (base64),
    // DBPointers (`ns:hex`).
    let mut classes = vec![class::JAVASCRIPT];
    if s.starts_with('{') {
        classes.extend([class::OBJECT, class::JAVASCRIPT_SCOPE]);
    }
    let b64 = |c: u8| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'/' | b'=');
    if uuid.is_none() && s.bytes().all(b64) {
        classes.push(class::BINARY);
    }
    if s.contains(':') {
        classes.push(class::DB_POINTER);
    }
    (keys, classes)
}

/// The 16 bytes of a UUID in its canonical text (lower-case hex).
fn uuid_bytes(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() != 36 || [8, 13, 18, 23].iter().any(|&i| b[i] != b'-') {
        return None;
    }
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32
        || !hex
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return None;
    }
    (0..16)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
