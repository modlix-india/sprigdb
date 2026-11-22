//! Writes: each document is changed under its key's lock, with its index
//! entries, in the caller's engine transaction. An update reads the
//! current version under the lock and rechecks the filter on it, so a
//! concurrent change is never overwritten blindly.

use bson::oid::ObjectId;
use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::catalog::IndexMeta;
use super::layout::{self, Space};
use super::read::decode;
use super::{Coll, DocStore, DocTxn};
use crate::filter::{self, Pred};
use crate::update::{self, ApplyCx, UpdateSpec};

/// The largest stored document.
pub const MAX_DOC: usize = 16 * 1024 * 1024;

/// The key pattern and value of a duplicate on `idx` for `doc`.
fn dup_error(ns: &str, idx: Option<&IndexMeta>, doc: &Document) -> MongoError {
    let (name, pattern) = match idx {
        Some(i) => (i.name.clone(), i.key()),
        None => ("_id_".to_owned(), doc! {"_id": 1}),
    };
    let mut value = Document::new();
    for (p, _) in &pattern {
        let v = update::get_path(doc, p).cloned().unwrap_or(Bson::Null);
        value.insert(p.clone(), v);
    }
    let shown = value
        .iter()
        .map(|(k, v)| format!("{k}: {}", super::cmd::shell(v)))
        .collect::<Vec<_>>()
        .join(", ");
    MongoError::new(
        code::DUPLICATE_KEY,
        format!("E11000 duplicate key error collection: {ns} index: {name} dup key: {{ {shown} }}"),
    )
    .with_extra(doc! {"keyPattern": pattern, "keyValue": value})
}

/// Writes the entries of one document in `idx`: unique entries checked
/// against other documents' under their lock.
///
/// # Errors
/// 11000 on a duplicate; a write conflict.
pub async fn put_entries(
    t: &mut DocTxn,
    space: &Space,
    idx: &IndexMeta,
    id: &[u8],
    entries: &[Vec<u8>],
    doc: &Document,
    ns: &str,
) -> Result<(), MongoError> {
    let base = space.index(idx.id, idx.unique);
    for e in entries {
        let mut k = base.clone();
        k.extend_from_slice(e);
        if idx.unique {
            t.lock_entry(&k).await?;
            if let Some(v) = t.get_current(&k)?
                && v.as_ref() != id
            {
                return Err(dup_error(ns, Some(idx), doc));
            }
        } else {
            k.extend_from_slice(id);
        }
        t.put(k, id.to_vec())?;
    }
    Ok(())
}

/// Deletes the entries of one document in `idx`.
///
/// # Errors
/// A write conflict.
pub async fn del_entries(
    t: &mut DocTxn,
    space: &Space,
    idx: &IndexMeta,
    id: &[u8],
    entries: &[Vec<u8>],
) -> Result<(), MongoError> {
    let base = space.index(idx.id, idx.unique);
    for e in entries {
        let mut k = base.clone();
        k.extend_from_slice(e);
        if idx.unique {
            t.lock_entry(&k).await?;
            // Another document's entry (never ours to delete).
            match t.get_current(&k)? {
                Some(v) if v.as_ref() == id => {}
                _ => continue,
            }
        } else {
            k.extend_from_slice(id);
        }
        t.delete(k)?;
    }
    Ok(())
}

/// `_id` first, generated when missing.
#[must_use]
pub fn with_id(d: Document) -> Document {
    match d.keys().next().map(String::as_str) {
        Some("_id") => d,
        _ => {
            let id = d
                .get("_id")
                .cloned()
                .unwrap_or_else(|| Bson::ObjectId(ObjectId::new()));
            let mut n = Document::new();
            n.insert("_id", id);
            for (k, v) in d {
                if k != "_id" {
                    n.insert(k, v);
                }
            }
            n
        }
    }
}

/// The `_id` key of a document about to be stored, after MongoDB's checks.
///
/// # Errors
/// 53 for a bad `_id`; 10334 past 16 MiB.
fn checked(d: &Document) -> Result<(Vec<u8>, Vec<u8>), MongoError> {
    let id = d
        .get("_id")
        .ok_or_else(|| MongoError::new(code::INVALID_ID_FIELD, "document has no _id"))?;
    if let Bson::Document(sub) = id
        && let Some(k) = sub.keys().find(|k| k.starts_with('$'))
    {
        return Err(MongoError::new(
            code::DOLLAR_PREFIXED_FIELD_NAME,
            format!(
                "_id fields may not contain '$'-prefixed fields: {k} is not valid for storage."
            ),
        ));
    }
    let idk = crate::json::id_key(id)?;
    let bytes = bson::to_vec(d).map_err(|e| MongoError::new(code::BAD_VALUE, e.to_string()))?;
    if bytes.len() > MAX_DOC {
        return Err(MongoError::new(
            code::BSON_OBJECT_TOO_LARGE,
            format!(
                "object to insert too large. size in bytes: {}, max size: {MAX_DOC}",
                bytes.len()
            ),
        ));
    }
    Ok((idk, bytes))
}

/// What one update statement did.
#[derive(Debug, Clone, Default)]
pub struct UpdateResult {
    /// Matched (`n`).
    pub n: u64,
    /// Changed (`nModified`).
    pub modified: u64,
    /// The `_id` an upsert inserted.
    pub upserted: Option<Bson>,
    /// The last document before the change.
    pub old: Option<Document>,
    /// The last document after it.
    pub new: Option<Document>,
}

/// One update statement.
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    /// `q`.
    pub q: Document,
    /// `u`.
    pub u: Bson,
    /// `multi`.
    pub multi: bool,
    /// `upsert`.
    pub upsert: bool,
    /// `arrayFilters`.
    pub array_filters: Vec<Document>,
    /// `hint`.
    pub hint: Option<Bson>,
    /// `sort` (`updateOne` and `findAndModify`).
    pub sort: Option<Document>,
}

/// Parses `arrayFilters`.
///
/// # Errors
/// 9 for a filter without a top-level field name; the filters' parse
/// errors.
pub fn parse_array_filters(afs: &[Document]) -> Result<Vec<(String, Pred)>, MongoError> {
    afs.iter()
        .map(|f| {
            let id = f
                .keys()
                .next()
                .and_then(|k| k.split('.').next())
                .ok_or_else(|| {
                    MongoError::new(
                        code::FAILED_TO_PARSE,
                        "Cannot use an expression without a top-level field name in arrayFilters",
                    )
                })?
                .to_owned();
            Ok((id, filter::parse(f)?))
        })
        .collect()
}

impl DocStore {
    /// Records that index `pos` of `c` has seen an array, in a transaction
    /// of its own committed at once: the fact must outlive a write that is
    /// rolled back or retried (a spurious fact only costs speed; a missing
    /// one would give duplicates and wrong orders).
    async fn note_multikey(&self, c: &Coll, pos: usize) -> Result<(), MongoError> {
        if c.is_multikey(pos) {
            return Ok(());
        }
        let k = c.space.multikey(c.meta.indexes[pos].id);
        let mut t = self.begin(false);
        t.lock_entry(&k).await?;
        t.put(k, vec![1])?;
        t.commit().await?;
        if let Some(b) = c.multikey.get(pos) {
            b.store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }

    /// Inserts `d` (with its `_id` first) into `c` in `t`.
    ///
    /// # Errors
    /// 11000 for a duplicate `_id` or unique key; 121 for a document the
    /// validator refuses; 53, 10334, 171, 16766.
    pub async fn insert_doc(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        d: Document,
    ) -> Result<Bson, MongoError> {
        let d = with_id(d);
        let ns = c.ns();
        if let Some(r) = crate::validate::Rules::of(&c.meta.options)? {
            r.check_insert(&ns, &d)?;
        }
        let (idk, bytes) = checked(&d)?;
        let key = c.space.doc(&idk);
        t.lock_doc(&key).await?;
        if t.get_current(&key)?.is_some() {
            return Err(dup_error(&ns, None, &d));
        }
        t.put(key, layout::doc_value(&bytes))?;
        for (pos, idx) in c.meta.indexes.iter().enumerate() {
            let (entries, multi) = super::keys::entries(&d, idx)?;
            if multi {
                self.note_multikey(c, pos).await?;
            }
            put_entries(t, &c.space, idx, &idk, &entries, &d, &ns).await?;
        }
        Ok(d.get("_id").cloned().unwrap_or(Bson::Null))
    }

    /// Replaces document `idk` of `c`, now `old`, with `new` in `t`.
    ///
    /// # Errors
    /// As for [`Self::insert_doc`].
    pub(crate) async fn replace_doc(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        idk: &[u8],
        old: &Document,
        new: &Document,
    ) -> Result<(), MongoError> {
        let ns = c.ns();
        if let Some(r) = crate::validate::Rules::of(&c.meta.options)? {
            r.check_update(&ns, old, new)?;
        }
        let (_, bytes) = checked(new)?;
        t.put(c.space.doc(idk), layout::doc_value(&bytes))?;
        for (pos, idx) in c.meta.indexes.iter().enumerate() {
            let (before, _) = super::keys::entries(old, idx)?;
            let (after, multi) = super::keys::entries(new, idx)?;
            if multi {
                self.note_multikey(c, pos).await?;
            }
            let gone: Vec<Vec<u8>> = before
                .iter()
                .filter(|k| after.binary_search(k).is_err())
                .cloned()
                .collect();
            let added: Vec<Vec<u8>> = after
                .iter()
                .filter(|k| before.binary_search(k).is_err())
                .cloned()
                .collect();
            del_entries(t, &c.space, idx, idk, &gone).await?;
            put_entries(t, &c.space, idx, idk, &added, new, &ns).await?;
        }
        Ok(())
    }

    /// Deletes document `idk` of `c`, now `old`, in `t`.
    ///
    /// # Errors
    /// A write conflict.
    pub async fn delete_doc(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        idk: &[u8],
        old: &Document,
    ) -> Result<(), MongoError> {
        t.delete(c.space.doc(idk))?;
        for idx in &c.meta.indexes {
            // A document stored before an index existed can't fail here
            // (its build indexed it); a damaged one is skipped.
            let Ok((entries, _)) = super::keys::entries(old, idx) else {
                continue;
            };
            del_entries(t, &c.space, idx, idk, &entries).await?;
        }
        Ok(())
    }

    /// The current version of document `idk` under its lock, if it still
    /// matches `pred`.
    pub(crate) async fn lock_current(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        idk: &[u8],
        pred: &Pred,
    ) -> Result<Option<Document>, MongoError> {
        let key = c.space.doc(idk);
        t.lock_doc(&key).await?;
        let Some(v) = t.get_current(&key)? else {
            return Ok(None);
        };
        let d = decode(&v)?;
        let cx = crate::agg::AggExpr { now: self.now() };
        Ok(pred.matches_with(&d, &cx).then_some(d))
    }

    /// Runs one update statement on `c` (`None`: the collection is missing
    /// and the statement doesn't upsert) in `t`.
    ///
    /// # Errors
    /// The update's parse and apply errors; the writes' errors.
    pub async fn update(
        &self,
        t: &mut DocTxn,
        c: Option<&Coll>,
        st: &UpdateStmt,
    ) -> Result<UpdateResult, MongoError> {
        let spec = update::parse(&st.u)?;
        if st.multi && matches!(spec, UpdateSpec::Replace(_)) {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "multi update is not supported for replacement-style update",
            ));
        }
        let pred = filter::parse(&st.q)?;
        let afs = parse_array_filters(&st.array_filters)?;
        let sort = st
            .sort
            .as_ref()
            .map(crate::agg::parse_sort)
            .transpose()?
            .unwrap_or_default();
        let now = self.now();
        let cx = ApplyCx {
            query: &pred,
            array_filters: &afs,
            inserting: false,
            now,
        };
        let mut out = UpdateResult::default();
        if let Some(c) = c {
            let limit = usize::from(!st.multi);
            // A single update whose target changed meanwhile looks again.
            for round in 0..4 {
                if round > 0 {
                    // Nothing written yet: a fresh view to look again.
                    t.begin_statement();
                }
                let targets = self.targets(t, c, &pred, &sort, st.hint.as_ref(), limit)?;
                let mut lost = false;
                for (idk, _) in targets {
                    let Some(old) = self.lock_current(t, c, &idk, &pred).await? else {
                        lost = true;
                        continue;
                    };
                    let mut new = old.clone();
                    let changed = update::apply(&mut new, &spec, &cx)?;
                    out.n += 1;
                    if changed {
                        let new = with_id(new);
                        self.replace_doc(t, c, &idk, &old, &new).await?;
                        out.modified += 1;
                        out.new = Some(new);
                    } else {
                        out.new = Some(new);
                    }
                    out.old = Some(old);
                }
                if st.multi || !lost || out.n > 0 {
                    break;
                }
            }
        }
        if out.n == 0 && st.upsert {
            let Some(c) = c else {
                return Err(MongoError::new(
                    code::INTERNAL_ERROR,
                    "upsert without a collection",
                ));
            };
            let mut d = match &spec {
                UpdateSpec::Replace(_) => {
                    let mut s = Document::new();
                    if let Some(ids) = pred.id_values()
                        && ids.len() == 1
                    {
                        s.insert("_id", ids[0].clone());
                    }
                    s
                }
                _ => update::upsert_seed(&pred)?,
            };
            let icx = ApplyCx {
                inserting: true,
                ..cx
            };
            update::apply(&mut d, &spec, &icx)?;
            let d = with_id(d);
            let id = self.insert_doc(t, c, d.clone()).await?;
            out.upserted = Some(id);
            out.new = Some(d);
        }
        Ok(out)
    }

    /// Runs one delete statement (`limit_one`: `limit: 1`) on `c` in `t`.
    /// Returns the documents deleted.
    ///
    /// # Errors
    /// Parse errors; a write conflict.
    pub async fn delete(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        q: &Document,
        limit_one: bool,
        hint: Option<&Bson>,
        sort: Option<&Document>,
    ) -> Result<Vec<Document>, MongoError> {
        let pred = filter::parse(q)?;
        let sort = sort
            .map(crate::agg::parse_sort)
            .transpose()?
            .unwrap_or_default();
        let mut gone = Vec::new();
        for round in 0..4 {
            if round > 0 {
                t.begin_statement();
            }
            let targets = self.targets(t, c, &pred, &sort, hint, usize::from(limit_one))?;
            let mut lost = false;
            for (idk, _) in targets {
                let Some(old) = self.lock_current(t, c, &idk, &pred).await? else {
                    lost = true;
                    continue;
                };
                self.delete_doc(t, c, &idk, &old).await?;
                gone.push(old);
            }
            if !limit_one || !lost || !gone.is_empty() {
                break;
            }
        }
        Ok(gone)
    }
}
