//! Upkeep of the vector side tables (see [`super::models`]).
//!
//! ```text
//! createSearchIndexes (vectorSearch), or a side table found missing:
//!   build, in the background when the gateway runs it (else here):
//!     a lease row in __sprig_doc_vx_jobs, so one process builds it
//!     <side>_b filled by _id, CHUNK documents per statement (small
//!       transactions: no long locks, no huge binlog group)
//!     still the same definition? replace <side> with it; else drop it
//!   $listSearchIndexes: READY and queryable only once <side> exists
//! each $vectorSearch, at most every vector_sync_ms per process, in the
//! background (the read never writes):
//!   sync: documents whose _v moved past the side table's, read without
//!     locks, upserted in batches; every 10 s the read reaches back
//!     SIDE_MARGIN for transactions that committed late
//!   purge: a sweep of PURGE_STEP side rows per sync, every minute, drops
//!     rows whose document is gone or has no vector any more
//! ```
//!
//! Every approximate result is rechecked against the document itself, so
//! a side table behind its collection costs recall, never a wrong result.
//! A failed upkeep step is logged and retried later; it never fails a read.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bson::Document;
use sprig_proto_mongo::errors::MongoError;

use super::models::{SIDE_MARGIN, VectorIndex, vector_side_table};
use super::{Engine, Ns, SqlRunner, mongo_error, text};
use crate::sql::{bin_lit, ident, str_lit};

/// Documents per build statement.
pub const BUILD_CHUNK: usize = 1000;
/// Documents per sync read (more wait for the next sync).
const SYNC_ROWS: usize = 2000;
/// Rows per upsert or delete statement.
const WRITE_BATCH: usize = 500;
/// Side rows one purge step looks at.
const PURGE_STEP: usize = 5000;
/// How often a sync reaches back [`SIDE_MARGIN`].
const MARGIN_EVERY: Duration = Duration::from_secs(10);
/// How often a purge sweep starts.
const PURGE_EVERY: Duration = Duration::from_secs(60);
/// A build lease not renewed for this long is taken over.
const LEASE_SECS: u64 = 120;
/// A missing side table is looked for again after this long.
const MISSING_FOR: Duration = Duration::from_secs(1);

/// The hidden table of build leases in each database.
const JOBS_TABLE: &str = "__sprig_doc_vx_jobs";

/// One side table's upkeep, asked of [`VectorSides`].
#[derive(Debug, Clone)]
pub struct VectorJob {
    /// The collection.
    pub ns: Ns,
    /// The vector index.
    pub name: String,
}

/// Runs side-table upkeep in the background, in a session of its own (the
/// gateway's): [`Engine::vector_side_work`].
pub trait VectorSides: Send + Sync + std::fmt::Debug {
    /// Starts `job` unless the same one runs; `false` when it can't run
    /// it in the background (the engine then does the work in `sql`).
    fn kick(&self, engine: &Engine, sql: &dyn SqlRunner, job: VectorJob) -> bool;
}

/// What this process knows of a side table.
#[derive(Debug, Clone, Default)]
struct Side {
    /// Vectors in it.
    count: u64,
    /// The newest `_v` copied.
    hwm: u64,
    /// The last sync.
    synced: Option<Instant>,
    /// The last sync that reached back [`SIDE_MARGIN`].
    margin_at: Option<Instant>,
    /// A purge sweep under way: the last `_id` it looked at.
    purge_from: Option<Vec<u8>>,
    /// The last sweep's end.
    purged: Option<Instant>,
}

/// Side tables by `db.side`.
static SIDES: LazyLock<Mutex<HashMap<String, Side>>> = LazyLock::new(Mutex::default);
/// Side tables found missing, and when.
static MISSING: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Mutex::default);
/// Side tables with upkeep under way in this process.
static BUSY: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(Mutex::default);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// This process's id in build leases.
fn me() -> &'static str {
    static ME: LazyLock<String> = LazyLock::new(|| {
        format!(
            "{:016x}",
            crate::meta::fnv64(
                format!("{}:{:?}", std::process::id(), std::time::SystemTime::now()).as_bytes()
            )
        )
    });
    &ME
}

/// Ends a [`BUSY`] entry.
struct Busy(String);

impl Drop for Busy {
    fn drop(&mut self) {
        lock(&BUSY).remove(&self.0);
    }
}

/// Forgets what this process knows of a side table (dropped).
pub(crate) fn forget_side(db: &str, side: &str) {
    let key = format!("{db}.{side}");
    lock(&SIDES).remove(&key);
    lock(&MISSING).remove(&key);
}

fn n_of(r: &[Option<Vec<u8>>], i: usize) -> u64 {
    text(r.get(i).and_then(Option::as_ref)).parse().unwrap_or(0)
}

impl Engine {
    /// The vectors in the side table of index `name`, `None` while it
    /// doesn't exist (being built): searches are exact meanwhile.
    ///
    /// # Errors
    /// SQL errors other than a missing table.
    pub(super) async fn side_count(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
        side: &str,
    ) -> Result<Option<u64>, MongoError> {
        let key = format!("{db}.{side}");
        if let Some(s) = lock(&SIDES).get(&key) {
            return Ok(Some(s.count));
        }
        if lock(&MISSING)
            .get(&key)
            .is_some_and(|t| t.elapsed() < MISSING_FOR)
        {
            return Ok(None);
        }
        match sql
            .query(&format!(
                "SELECT COUNT(*), COALESCE(MAX(_v), 0) FROM {}.{}",
                ident(db),
                ident(side)
            ))
            .await
        {
            Ok(rows) => {
                let r = rows.into_iter().next().unwrap_or_default();
                let (count, newest) = (n_of(&r, 0), n_of(&r, 1));
                lock(&MISSING).remove(&key);
                let mut g = lock(&SIDES);
                let s = g.entry(key).or_insert_with(|| Side {
                    count,
                    // Not known to this process: reach back from the newest.
                    hwm: newest.saturating_sub(SIDE_MARGIN),
                    ..Side::default()
                });
                Ok(Some(s.count))
            }
            Err(e) if e.is(1146) => {
                lock(&MISSING).insert(key, Instant::now());
                Ok(None)
            }
            Err(e) => Err(mongo_error(&e)),
        }
    }

    /// Asks for the upkeep of index `name`'s side table: in the
    /// background when the gateway runs it, else here (errors logged).
    /// `due_only`: not if it was synced within `vector_sync_ms`.
    pub(super) async fn side_upkeep(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        name: &str,
        due_only: bool,
    ) {
        let key = format!("{}.{}", ns.db, vector_side_table(table, name));
        if due_only
            && lock(&SIDES).get(&key).is_some_and(|s| {
                s.synced
                    .is_some_and(|t| t.elapsed() < Duration::from_millis(self.cfg.vector_sync_ms))
            })
        {
            return;
        }
        if lock(&BUSY).contains(&key) {
            return;
        }
        let job = VectorJob {
            ns: ns.clone(),
            name: name.to_owned(),
        };
        if let Some(v) = &self.vectors
            && v.kick(self, &*sql, job)
        {
            return;
        }
        if let Err(e) = Box::pin(self.vector_side_work(sql, ns, name)).await {
            tracing::debug!(ns = %ns, index = name, error = %e.message, "vector index upkeep failed; retried later");
        }
    }

    /// One round of upkeep of index `name`'s side table on `ns`: built
    /// when missing, else brought up to date and a purge step. Runs in a
    /// session of its own (the gateway's background task), or in the
    /// caller's when no [`VectorSides`] is set. Once at a time per side
    /// table and process.
    ///
    /// # Errors
    /// SQL errors (the caller logs them; the next round retries).
    pub async fn vector_side_work(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        name: &str,
    ) -> Result<(), MongoError> {
        let Some(m) = self.meta(sql, ns).await? else {
            return Ok(());
        };
        let side = vector_side_table(&m.table, name);
        let key = format!("{}.{side}", ns.db);
        if !lock(&BUSY).insert(key.clone()) {
            return Ok(());
        }
        let _busy = Busy(key.clone());
        let Some((_, _, _, def)) = self.search_rows(sql, ns, Some(name)).await?.pop() else {
            // The index is gone.
            self.drop_side_tables(sql, &ns.db, &side).await;
            return Ok(());
        };
        let vx = VectorIndex::parse(&def)?;
        lock(&MISSING).remove(&key);
        match self.side_count(sql, &ns.db, &side).await? {
            None => Box::pin(self.side_build(sql, ns, &m.table, name, &vx, &def)).await,
            Some(_) => Box::pin(self.side_sync(sql, ns, &m.table, &side, &vx)).await,
        }
    }

    /// Drops a side table and its build table.
    pub(super) async fn drop_side_tables(&self, sql: &mut dyn SqlRunner, db: &str, side: &str) {
        for t in [side.to_owned(), format!("{side}_b")] {
            let _ = sql
                .exec(&format!("DROP TABLE IF EXISTS {}.{}", ident(db), ident(&t)))
                .await;
        }
        forget_side(db, side);
    }

    /// Takes or renews the build lease of `side`; whether this process
    /// holds it.
    async fn side_lease(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
        side: &str,
    ) -> Result<bool, MongoError> {
        let jobs = format!("{}.{}", ident(db), ident(JOBS_TABLE));
        let run = |r: Result<super::Done, super::SqlError>| r.map_err(|e| mongo_error(&e));
        run(sql
            .exec(&format!(
                "CREATE TABLE IF NOT EXISTS {jobs} (side VARCHAR(64) NOT NULL PRIMARY KEY, \
                 owner VARCHAR(32) NOT NULL, beat BIGINT NOT NULL) ENGINE=InnoDB"
            ))
            .await)?;
        run(sql
            .exec(&format!(
                "INSERT INTO {jobs} (side, owner, beat) VALUES ({s}, {o}, UNIX_TIMESTAMP()) \
                 ON DUPLICATE KEY UPDATE \
                 owner = IF(beat < UNIX_TIMESTAMP() - {LEASE_SECS} OR owner = VALUES(owner), VALUES(owner), owner), \
                 beat = IF(owner = VALUES(owner), VALUES(beat), beat)",
                s = str_lit(side),
                o = str_lit(me())
            ))
            .await)?;
        let rows = sql
            .query(&format!(
                "SELECT owner FROM {jobs} WHERE side = {}",
                str_lit(side)
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        Ok(rows
            .first()
            .is_some_and(|r| text(r.first().and_then(Option::as_ref)) == me()))
    }

    async fn side_release(&self, sql: &mut dyn SqlRunner, db: &str, side: &str) {
        let _ = sql
            .exec(&format!(
                "DELETE FROM {}.{} WHERE side = {} AND owner = {}",
                ident(db),
                ident(JOBS_TABLE),
                str_lit(side),
                str_lit(me())
            ))
            .await;
    }

    /// Builds the side table of index `name` (see the module docs).
    async fn side_build(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        name: &str,
        vx: &VectorIndex,
        def: &Document,
    ) -> Result<(), MongoError> {
        let side = vector_side_table(table, name);
        if !self.side_lease(sql, &ns.db, &side).await? {
            // Another process builds it.
            return Ok(());
        }
        let r = self.side_fill(sql, ns, table, name, vx, def).await;
        self.side_release(sql, &ns.db, &side).await;
        r
    }

    async fn side_fill(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        name: &str,
        vx: &VectorIndex,
        def: &Document,
    ) -> Result<(), MongoError> {
        let started = Instant::now();
        let side = vector_side_table(table, name);
        let build = format!("{side}_b");
        let db = ident(&ns.db);
        let coll = format!("{db}.{}", ident(table));
        let bt = format!("{db}.{}", ident(&build));
        let run = |r: Result<super::Done, super::SqlError>| r.map_err(|e| mongo_error(&e));
        // The documents' `_v`, for the syncs' range reads.
        run(sql
            .exec(&format!(
                "ALTER TABLE {coll} ADD INDEX IF NOT EXISTS _sprig_dix_v (_v)"
            ))
            .await)?;
        let start_v = sql
            .query(&format!("SELECT COALESCE(MAX(_v), 0) FROM {coll}"))
            .await
            .map_err(|e| mongo_error(&e))?
            .first()
            .map_or(0, |r| n_of(r, 0));
        let (_, metric) = vx.side_distance();
        run(sql.exec(&format!("DROP TABLE IF EXISTS {bt}")).await)?;
        run(sql
            .exec(&format!(
                "CREATE TABLE {bt} (_id VARBINARY(254) NOT NULL, \
                 v VECTOR({}) NOT NULL, _v BIGINT UNSIGNED NOT NULL, PRIMARY KEY (_id), KEY by_v (_v), \
                 VECTOR INDEX (v) M=16 DISTANCE={metric}) ENGINE=InnoDB",
                vx.dims
            ))
            .await)?;
        let mut after: Option<Vec<u8>> = None;
        let mut n = 0u64;
        loop {
            let from = after
                .as_ref()
                .map_or_else(String::new, |k| format!(" AND _id > {}", bin_lit(k)));
            // The chunk's last key (none: the rest is the last chunk).
            let end = sql
                .query(&format!(
                    "SELECT _id FROM {coll} WHERE 1 = 1{from} ORDER BY _id LIMIT 1 OFFSET {}",
                    BUILD_CHUNK - 1
                ))
                .await
                .map_err(|e| mongo_error(&e))?
                .into_iter()
                .next()
                .and_then(|mut r| r.first_mut().and_then(Option::take));
            let upto = end
                .as_ref()
                .map_or_else(String::new, |k| format!(" AND _id <= {}", bin_lit(k)));
            let d = run(sql
                .exec(&format!(
                    "INSERT INTO {bt} (_id, v, _v) SELECT _id, VEC_FromText(JSON_EXTRACT(doc, {p})), _v \
                     FROM {coll} WHERE LENGTH(_id) <= 254 AND {valid}{from}{upto}",
                    p = vx.path_lit(),
                    valid = vx.valid_sql()
                ))
                .await)?;
            n += d.affected;
            let Some(k) = end else { break };
            after = Some(k);
            if !self.side_lease(sql, &ns.db, &side).await? {
                // Taken over (this build looked dead): the other one goes on.
                return Ok(());
            }
        }
        // Built for the definition the index still has?
        let now = self.search_rows(sql, ns, Some(name)).await?.pop();
        if now.as_ref().is_none_or(|(_, _, _, d)| d != def) {
            let _ = sql.exec(&format!("DROP TABLE IF EXISTS {bt}")).await;
            return Ok(());
        }
        let st = format!("{db}.{}", ident(&side));
        run(sql.exec(&format!("DROP TABLE IF EXISTS {st}")).await)?;
        run(sql.exec(&format!("RENAME TABLE {bt} TO {st}")).await)?;
        let key = format!("{}.{side}", ns.db);
        lock(&MISSING).remove(&key);
        lock(&SIDES).insert(
            key,
            Side {
                count: n,
                // Documents changed while it was built come with the next
                // sync.
                hwm: start_v.saturating_sub(SIDE_MARGIN),
                ..Side::default()
            },
        );
        tracing::info!(ns = %ns, index = name, vectors = n, ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), "vector index built");
        Ok(())
    }

    /// Brings a side table up to date, then a purge step (module docs).
    async fn side_sync(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        side: &str,
        vx: &VectorIndex,
    ) -> Result<(), MongoError> {
        let key = format!("{}.{side}", ns.db);
        let mut s = lock(&SIDES).get(&key).cloned().unwrap_or_default();
        let db = ident(&ns.db);
        let coll = format!("{db}.{}", ident(table));
        let st = format!("{db}.{}", ident(side));
        let margin = s.margin_at.is_none_or(|t| t.elapsed() >= MARGIN_EVERY);
        let lower = if margin {
            s.hwm.saturating_sub(SIDE_MARGIN)
        } else {
            s.hwm
        };
        // Documents new or changed since, read without locks.
        let rows = sql
            .query(&format!(
                "SELECT c._id, JSON_EXTRACT(c.doc, {p}), c._v, s._id IS NULL FROM {coll} AS c \
                 LEFT JOIN {st} AS s ON s._id = c._id \
                 WHERE c._v >= {lower} AND LENGTH(c._id) <= 254 AND {valid} \
                 AND (s._id IS NULL OR s._v <> c._v) ORDER BY c._v LIMIT {SYNC_ROWS}",
                p = vx.path_lit(),
                valid = vx.valid_sql()
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        let got = rows.len();
        let mut newest = s.hwm;
        let mut added = 0u64;
        for chunk in rows.chunks(WRITE_BATCH) {
            let mut values = Vec::with_capacity(chunk.len());
            for r in chunk {
                let id = r.first().cloned().flatten().unwrap_or_default();
                let v = text(r.get(1).and_then(Option::as_ref));
                let ver = n_of(r, 2);
                newest = newest.max(ver);
                added += n_of(r, 3);
                values.push(format!(
                    "({}, VEC_FromText({}), {ver})",
                    bin_lit(&id),
                    str_lit(&v)
                ));
            }
            sql.exec(&format!(
                "INSERT INTO {st} (_id, v, _v) VALUES {} ON DUPLICATE KEY UPDATE v = VALUES(v), _v = VALUES(_v)",
                values.join(", ")
            ))
            .await
            .map_err(|e| mongo_error(&e))?;
        }
        s.count += added;
        s.hwm = newest;
        if margin && got < SYNC_ROWS {
            s.margin_at = Some(Instant::now());
        }
        // A purge step.
        if s.purge_from.is_some() || s.purged.is_none_or(|t| t.elapsed() >= PURGE_EVERY) {
            let from = s.purge_from.take().unwrap_or_default();
            let ids = sql
                .query(&format!(
                    "SELECT s._id, c._id IS NULL FROM (SELECT _id FROM {st} WHERE _id > {} ORDER BY _id \
                     LIMIT {PURGE_STEP}) AS s LEFT JOIN {coll} AS c ON c._id = s._id AND {}",
                    bin_lit(&from),
                    vx.valid_sql()
                ))
                .await
                .map_err(|e| mongo_error(&e))?;
            let looked = ids.len();
            let mut last: Option<Vec<u8>> = None;
            let mut gone = Vec::new();
            for r in ids {
                let id = r.first().cloned().flatten().unwrap_or_default();
                if n_of(&r, 1) == 1 {
                    gone.push(bin_lit(&id));
                }
                if last.as_ref().is_none_or(|l| id > *l) {
                    last = Some(id);
                }
            }
            for chunk in gone.chunks(WRITE_BATCH) {
                let d = sql
                    .exec(&format!(
                        "DELETE FROM {st} WHERE _id IN ({})",
                        chunk.join(", ")
                    ))
                    .await
                    .map_err(|e| mongo_error(&e))?;
                s.count = s.count.saturating_sub(d.affected);
            }
            if looked < PURGE_STEP {
                s.purged = Some(Instant::now());
            } else {
                s.purge_from = last;
            }
        }
        s.synced = Some(Instant::now());
        lock(&SIDES).insert(key, s);
        Ok(())
    }

    /// Whether index `name`'s side table exists (`$listSearchIndexes`:
    /// queryable); a missing one is asked for.
    pub(super) async fn side_ready(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        table: &str,
        name: &str,
    ) -> Result<bool, MongoError> {
        let side = vector_side_table(table, name);
        let ready = self.side_count(sql, &ns.db, &side).await?.is_some();
        if !ready {
            self.side_upkeep(sql, ns, table, name, false).await;
            return Ok(self.side_count(sql, &ns.db, &side).await?.is_some());
        }
        Ok(true)
    }
}
