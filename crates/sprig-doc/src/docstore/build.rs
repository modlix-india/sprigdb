//! Online index builds: `createIndexes` no longer stops the tenant's
//! writers while it reads the collection.
//!
//! A build has three steps:
//! 1. Under the DDL lock (a moment): the new indexes enter the collection's
//!    record marked `building`. From then on every write keeps their
//!    entries, as it does for any index; no plan reads them, `listIndexes`
//!    and the statistics don't show them, and a `hint` can't name them.
//! 2. Without the DDL lock: the existing documents' entries are written,
//!    500 documents per engine transaction, each document locked and read
//!    again before its entries are written, so a concurrent write to it
//!    waits for its batch (never for the build). Each batch holds the
//!    writers' side of the DDL lock, so a drop or rename between batches
//!    is seen by the next one, which then stops the build.
//! 3. Under the DDL lock again: the `building` mark is removed. The
//!    command answers only then.
//!
//! A unique index builds online too, as MongoDB builds one: while it is
//! `building`, a write (or the backfill) that meets another document's
//! entry isn't refused; the document is noted among the index's duplicates
//! (`Space::dups`). In step 3, with no writer running, every noted
//! document that still holds the values must own their entry (an owner
//! that changed or left since frees it): two documents with the same
//! values fail the build with 11000, and the index is removed. A build cut
//! short by a crash leaves the index `building` and kept up to date by
//! writes; the same `createIndexes` again finishes it.

use std::collections::HashSet;
use std::sync::PoisonError;

use bson::Document;
use sprig_engine::txn::Dir;
use sprig_proto_mongo::errors::{MongoError, code};

use super::catalog::{self, Added, IndexMeta};
use super::{Coll, DocStore, Name, keys, layout, write};

/// Documents per backfill transaction.
const BATCH: usize = 500;
/// 276 `IndexBuildAborted`.
pub const INDEX_BUILD_ABORTED: i32 = 276;

/// The builds running in this process: `(tenant, collection number, index
/// number)`.
#[derive(Debug, Default)]
pub struct Builds(std::sync::Mutex<HashSet<(u64, u32, u16)>>);

impl Builds {
    fn set(&self) -> std::sync::MutexGuard<'_, HashSet<(u64, u32, u16)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn aborted(name: &Name, why: &str) -> MongoError {
    MongoError::new(
        INDEX_BUILD_ABORTED,
        format!("Index build aborted on {name}: {why}"),
    )
}

/// What step 1 decided.
enum Start {
    /// Nothing to build (`before`, `created`).
    Done(usize, usize, bool),
    /// Build these index numbers and wait for those (`before`, `created`).
    Build {
        c: std::sync::Arc<Coll>,
        mine: Vec<u16>,
        theirs: Vec<u16>,
        before: usize,
        created: bool,
    },
}

impl DocStore {
    /// `createIndexes` of `specs` on collection `name`, built online
    /// unless one of the new indexes is unique. Returns the numbers of
    /// indexes before and after, and whether the collection was created.
    ///
    /// # Errors
    /// The specification errors (67, 85, 86); a document that can't be
    /// indexed (171, 16766); 276 when the collection or index was dropped
    /// during the build.
    pub async fn create_indexes(
        &self,
        tenant: u64,
        name: &Name,
        specs: &[Document],
    ) -> Result<(usize, usize, bool), MongoError> {
        let start = {
            let _g = self.ddl_guard(tenant).await;
            Box::pin(self.start_build(tenant, name, specs)).await?
        };
        let (c, mine, theirs, before, created) = match start {
            Start::Done(before, after, created) => return Ok((before, after, created)),
            Start::Build {
                c,
                mine,
                theirs,
                before,
                created,
            } => (c, mine, theirs, before, created),
        };
        let n = c.meta.n;
        if !mine.is_empty() {
            let r = Box::pin(self.backfill(tenant, name, n, &mine)).await;
            let _g = self.ddl_guard(tenant).await;
            let r = match r {
                Ok(()) => self.publish(tenant, name, n, &mine).await,
                Err(e) => Err(e),
            };
            {
                let mut b = self.builds.set();
                for i in &mine {
                    b.remove(&(tenant, n, *i));
                }
            }
            if let Err(e) = r {
                self.abandon(tenant, name, n, &mine).await;
                return Err(e);
            }
        }
        // Builds another command started: answer when they end.
        loop {
            let Some(now) = self.coll(tenant, name)?.filter(|x| x.meta.n == n) else {
                return Err(aborted(name, "collection dropped"));
            };
            let pending: Vec<&catalog::IndexMeta> = now
                .meta
                .indexes
                .iter()
                .filter(|i| theirs.contains(&i.id))
                .collect();
            if pending.len() < theirs.len() {
                return Err(aborted(name, "index dropped"));
            }
            if pending.iter().all(|i| !i.building) {
                let after = now.meta.indexes.iter().filter(|i| !i.building).count() + 1;
                return Ok((before, after, created));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// `createIndexes` inside a multi-document transaction: allowed on a
    /// collection that is missing (created now, outside the transaction)
    /// or empty, so there is nothing to build; the indexes enter the
    /// record at once. The transaction holds the writers' side of the DDL
    /// lock, so this can't take it.
    ///
    /// # Errors
    /// 263 for a collection that has documents; the specification errors.
    pub async fn create_indexes_in_txn(
        &self,
        tenant: u64,
        name: &Name,
        specs: &[Document],
    ) -> Result<(usize, usize, bool), MongoError> {
        let (c, created) = match self.coll(tenant, name)? {
            Some(c) => (c, false),
            None => match self.create_locked(tenant, name, Document::new()).await? {
                Some(c) => (c, true),
                None => (
                    self.coll(tenant, name)?.ok_or_else(|| {
                        MongoError::new(code::INTERNAL_ERROR, "collection vanished")
                    })?,
                    false,
                ),
            },
        };
        let p = c.space.docs();
        let mut t = self.begin(false);
        let any = t.scan(&p, &layout::after_prefix(&p), Dir::Forward, 1);
        t.rollback();
        if !any?.is_empty() {
            return Err(MongoError::new(
                code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                format!(
                    "Cannot create new indexes on existing collection {name} in a multi-document transaction."
                ),
            ));
        }
        let before = c.meta.indexes.iter().filter(|i| !i.building).count() + 1;
        let mut meta = c.meta.clone();
        let mut fresh = Vec::new();
        for s in specs {
            let m = catalog::parse_index(&meta.index_spec(s), meta.next_index)?;
            if let Added::New = meta.add_index(m)? {
                fresh.push(meta.next_index);
                meta.next_index += 1;
            }
        }
        if fresh.is_empty() {
            return Ok((before, before, created));
        }
        // Entries a build cut short by a crash may have left under these
        // numbers.
        for i in &fresh {
            for unique in [false, true] {
                let p = c.space.index(*i, unique);
                self.delete_range(&p, &layout::after_prefix(&p)).await?;
            }
        }
        let after = meta.indexes.len() + 1;
        self.replace_meta(tenant, meta).await?;
        Ok((before, after, created))
    }

    /// Step 1, DDL lock held.
    async fn start_build(
        &self,
        tenant: u64,
        name: &Name,
        specs: &[Document],
    ) -> Result<Start, MongoError> {
        let (c, created) = match self.coll(tenant, name)? {
            Some(c) => (c, false),
            None => match self.create_locked(tenant, name, Document::new()).await? {
                Some(c) => (c, true),
                None => (
                    self.coll(tenant, name)?.ok_or_else(|| {
                        MongoError::new(code::INTERNAL_ERROR, "collection vanished")
                    })?,
                    false,
                ),
            },
        };
        let before = c.meta.indexes.iter().filter(|i| !i.building).count() + 1;
        let mut meta = c.meta.clone();
        let mut fresh = Vec::new();
        let mut theirs = Vec::new();
        let mut resume = Vec::new();
        for s in specs {
            let m = catalog::parse_index(&meta.index_spec(s), meta.next_index)?;
            let iname = m.name.clone();
            match meta.add_index(m)? {
                Added::New => {
                    fresh.push(meta.next_index);
                    meta.next_index += 1;
                }
                Added::Exists => {
                    if let Some(ex) = meta.indexes.iter().find(|i| i.name == iname && i.building) {
                        let running = self.builds.set().contains(&(tenant, meta.n, ex.id));
                        let id = ex.id;
                        if running {
                            if !theirs.contains(&id) {
                                theirs.push(id);
                            }
                        } else if !resume.contains(&id) {
                            // Left by a build that ended with its node: this
                            // command finishes it (its entries are kept up
                            // to date by writes, and writing them again is
                            // harmless).
                            resume.push(id);
                        }
                    }
                }
            }
        }
        if fresh.is_empty() && theirs.is_empty() && resume.is_empty() {
            return Ok(Start::Done(before, before, created));
        }
        // A build cut short by a crash may have left entries under the new
        // numbers (the record keeps the counter only once a build starts).
        for i in &fresh {
            for unique in [false, true] {
                let p = c.space.index(*i, unique);
                self.delete_range(&p, &layout::after_prefix(&p)).await?;
            }
            for p in [c.space.multikey(*i), c.space.dups(*i)] {
                self.delete_range(&p, &layout::after_prefix(&p)).await?;
            }
        }
        let mut mine = fresh.clone();
        mine.extend(resume.iter().copied());
        for i in &mut meta.indexes {
            if fresh.contains(&i.id) {
                i.building = true;
            }
        }
        {
            let mut b = self.builds.set();
            for i in &mine {
                b.insert((tenant, meta.n, *i));
            }
        }
        let c = if fresh.is_empty() {
            c
        } else {
            match self.replace_meta(tenant, meta).await {
                Ok(c) => c,
                Err(e) => {
                    let mut b = self.builds.set();
                    for i in &mine {
                        b.remove(&(tenant, c.meta.n, *i));
                    }
                    return Err(e);
                }
            }
        };
        Ok(Start::Build {
            c,
            mine,
            theirs,
            before,
            created,
        })
    }

    /// Step 2: every document's entries for indexes `ids` of collection
    /// number `n`, batch by batch.
    async fn backfill(
        &self,
        tenant: u64,
        name: &Name,
        n: u32,
        ids: &[u16],
    ) -> Result<(), MongoError> {
        let mut lo: Option<Vec<u8>> = None;
        loop {
            let mut attempt = 0u32;
            let next = loop {
                let w = self.write_guard(tenant).await;
                let Some(c) = self.coll(tenant, name)?.filter(|c| c.meta.n == n) else {
                    return Err(aborted(name, "collection dropped"));
                };
                if ids
                    .iter()
                    .any(|i| !c.meta.indexes.iter().any(|x| x.id == *i))
                {
                    return Err(aborted(name, "index dropped"));
                }
                match Box::pin(self.backfill_batch(&c, ids, lo.as_deref())).await {
                    Ok(next) => break next,
                    Err(e) if e.code == code::WRITE_CONFLICT && attempt < 50 => {
                        attempt += 1;
                        drop(w);
                        tokio::time::sleep(std::time::Duration::from_millis(u64::from(attempt)))
                            .await;
                    }
                    Err(e) => return Err(e),
                }
            };
            match next {
                Some(k) => lo = Some(k),
                None => return Ok(()),
            }
            // Let waiting DDL and writers in between batches.
            tokio::task::yield_now().await;
        }
    }

    /// One batch from `lo` (the start: `None`); the next batch's start, or
    /// `None` at the end.
    async fn backfill_batch(
        &self,
        c: &Coll,
        ids: &[u16],
        lo: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, MongoError> {
        let docs_p = c.space.docs();
        let end = layout::after_prefix(&docs_p);
        let from = lo.map_or_else(|| docs_p.clone(), <[u8]>::to_vec);
        let ns = c.ns();
        let mut t = self.begin(false);
        let rows = match t.scan(&from, &end, Dir::Forward, BATCH) {
            Ok(r) => r,
            Err(e) => {
                t.rollback();
                return Err(e);
            }
        };
        let Some((last, _)) = rows.last() else {
            t.rollback();
            return Ok(None);
        };
        let next = layout::after_prefix(last);
        let r = async {
            for (k, _) in &rows {
                // The document as it is now, under its lock: a write that
                // changed it after the scan wrote these entries itself.
                t.lock_doc(k).await?;
                let Some(v) = t.get_current(k)? else {
                    continue;
                };
                let d = super::read::decode(&v)?;
                let id = &k[docs_p.len()..];
                for (pos, idx) in c.meta.indexes.iter().enumerate() {
                    if !ids.contains(&idx.id) {
                        continue;
                    }
                    let (entries, multi) = keys::entries(&d, idx)?;
                    if multi {
                        self.note_multikey(c, pos).await?;
                    }
                    write::put_entries(&mut t, &c.space, idx, id, &entries, &d, &ns).await?;
                }
            }
            Ok::<(), MongoError>(())
        }
        .await;
        match r {
            Ok(()) => {
                t.commit().await?;
                Ok(Some(next))
            }
            Err(e) => {
                t.rollback();
                Err(e)
            }
        }
    }

    /// Step 3, DDL lock held: the indexes become visible.
    async fn publish(
        &self,
        tenant: u64,
        name: &Name,
        n: u32,
        ids: &[u16],
    ) -> Result<(), MongoError> {
        let Some(c) = self.coll(tenant, name)?.filter(|c| c.meta.n == n) else {
            return Err(aborted(name, "collection dropped"));
        };
        if ids
            .iter()
            .any(|i| !c.meta.indexes.iter().any(|x| x.id == *i))
        {
            return Err(aborted(name, "index dropped"));
        }
        for idx in c
            .meta
            .indexes
            .iter()
            .filter(|i| i.unique && ids.contains(&i.id))
        {
            self.check_dups(&c, idx).await?;
        }
        let mut meta = c.meta.clone();
        for i in &mut meta.indexes {
            if ids.contains(&i.id) {
                i.building = false;
            }
        }
        self.replace_meta(tenant, meta).await?;
        Ok(())
    }

    /// Step 3's check of unique index `idx` (DDL lock held: no write
    /// runs): each noted duplicate that still holds its values takes their
    /// entry if no one owns it now; if another document does, the build
    /// fails.
    ///
    /// # Errors
    /// 11000 for two documents with the same values; the store's.
    async fn check_dups(&self, c: &Coll, idx: &IndexMeta) -> Result<(), MongoError> {
        let dp = c.space.dups(idx.id);
        let end = layout::after_prefix(&dp);
        let base = c.space.index(idx.id, true);
        let ns = c.ns();
        loop {
            let mut t = self.begin(false);
            let rows = match t.scan(&dp, &end, Dir::Forward, BATCH) {
                Ok(r) => r,
                Err(e) => {
                    t.rollback();
                    return Err(e);
                }
            };
            if rows.is_empty() {
                t.rollback();
                return Ok(());
            }
            let r = async {
                for (k, v) in &rows {
                    let id: &[u8] = v;
                    let Some(e) = k.get(dp.len()..k.len().saturating_sub(id.len())) else {
                        continue;
                    };
                    t.lock_entry(k).await?;
                    t.delete(k.clone())?;
                    let Some(dv) = t.get_current(&c.space.doc(id))? else {
                        continue;
                    };
                    let d = super::read::decode(&dv)?;
                    let (entries, _) = keys::entries(&d, idx)?;
                    if !entries.iter().any(|x| x.as_slice() == e) {
                        continue;
                    }
                    let mut uk = base.clone();
                    uk.extend_from_slice(e);
                    t.lock_entry(&uk).await?;
                    match t.get_current(&uk)? {
                        Some(owner) if owner.as_ref() != id => {
                            return Err(write::duplicate(&ns, idx, &d));
                        }
                        Some(_) => {}
                        None => t.put(uk, id.to_vec())?,
                    }
                }
                Ok::<(), MongoError>(())
            }
            .await;
            match r {
                Ok(()) => t.commit().await?,
                Err(e) => {
                    t.rollback();
                    return Err(e);
                }
            }
        }
    }

    /// Marks index `index` of `db.coll` as `building`, as a node that died
    /// mid-build leaves it (tests only).
    #[doc(hidden)]
    pub async fn mark_building_for_tests(&self, tenant: u64, db: &str, coll: &str, index: &str) {
        let _g = self.ddl_guard(tenant).await;
        if let Ok(Some(c)) = self.coll(tenant, &Name::new(db, coll)) {
            let mut meta = c.meta.clone();
            for i in &mut meta.indexes {
                if i.name == index {
                    i.building = true;
                }
            }
            let _ = self.replace_meta(tenant, meta).await;
        }
    }

    /// After a failed build, DDL lock held: the indexes leave the record
    /// and their entries are deleted (best effort; a later build of the
    /// same number clears them first anyway).
    async fn abandon(&self, tenant: u64, name: &Name, n: u32, ids: &[u16]) {
        let Ok(Some(c)) = self.coll(tenant, name).map(|c| c.filter(|c| c.meta.n == n)) else {
            return;
        };
        let mut meta = c.meta.clone();
        meta.indexes.retain(|i| !ids.contains(&i.id));
        if meta.indexes.len() != c.meta.indexes.len()
            && self.replace_meta(tenant, meta).await.is_err()
        {
            return;
        }
        for i in ids {
            for unique in [false, true] {
                let p = c.space.index(*i, unique);
                let _ = self.delete_range(&p, &layout::after_prefix(&p)).await;
            }
            for p in [c.space.multikey(*i), c.space.dups(*i)] {
                let _ = self.delete_range(&p, &layout::after_prefix(&p)).await;
            }
        }
    }
}
