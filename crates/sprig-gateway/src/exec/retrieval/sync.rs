//! The in-node job that keeps engine databases' retrieval indexes in step
//! with their tables: an engine node has no worker and no binlog feed, so
//! the node that leads the tenant's unit does it.
//!
//! ```text
//! spawn: one task per process; every tick (1 s, SPRIGDB_RETRIEVAL_TICK_MS,
//!   or at once after a CREATE), for each tenant whose catalog this node has
//!   loaded and whose unit it leads (or no one alone leads):
//!     each index of an engine database, one round at a time, in a session
//!     of the user who created it (internal statements, exempt from
//!     row-level security: the chunks hold every row; searches check each
//!     hit through the searching user's session):
//!       source table or database gone: the index and its chunk table go
//!       fill (state building): the table's rows in key order, 64 at a time,
//!         from the key `progress.fill` names; "done": state ready
//!       changes: the table's changes in the node's change log after
//!         `progress.token` (or from `progress.since_ms`, the creation time):
//!         each changed key's row read again now
//!       a row: hash(text, title, filters) against chunk 0's src_hash;
//!         equal: nothing; text empty or the row gone: its chunks deleted;
//!         else cut (CHUNK BY), embedded (the bundled model, in process) and
//!         its chunks replaced in one transaction
//!       progress saved in the index's record after each step's writes
//! ```
//!
//! The work is state-based: a row is always made to match what it is when
//! it is read, so a change read twice (a restart between the write and the
//! saved token) or out of order changes nothing, and a change committed
//! while a row was embedded comes back as a later change. A change log
//! that no longer holds the token's place (retention) fills the index
//! again from the start (`rebuilding`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::{Value as J, json};
use sprig_ai::retrieval::rec;
use sprig_ai::retrieval::store::{RetrievalIndex, State};
use sprig_ai::retrieval::{self as rx, KeyColumn};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::names::quote;
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_doc::engine::{Param, SqlRunner as _};
use sprig_sql::retrieval::Chunking;
use tokio::sync::Notify;

use crate::mongo::sql::SqlSession;
use crate::pipeline::SharedExecutor;
use crate::table_changes::{self, Watched};
use crate::tenant::TenantServices;

/// The tick, in milliseconds (tests shorten it).
pub(crate) const TICK_VAR: &str = "SPRIGDB_RETRIEVAL_TICK_MS";
/// How often a tenant's chunk tables without an index are looked for.
const SWEEP_EVERY: Duration = Duration::from_secs(60);
/// How long a led tenant without retrieval indexes waits to be listed
/// again.
const LIST_IDLE: Duration = Duration::from_secs(30);
/// Rows read per fill step.
const FILL_ROWS: usize = 64;
/// How long one round of one index may work before it lets others go.
const ROUND: Duration = Duration::from_secs(2);
/// Change-log pages one round reads at most.
const PAGES: usize = 16;

fn wakeup() -> &'static Notify {
    static N: OnceLock<Notify> = OnceLock::new();
    N.get_or_init(Notify::new)
}

/// Wakes the job now (a new index).
pub(crate) fn wake() {
    wakeup().notify_one();
}

/// The indexes a round is running for.
fn running() -> &'static Mutex<HashSet<(u64, u64)>> {
    static R: OnceLock<Mutex<HashSet<(u64, u64)>>> = OnceLock::new();
    R.get_or_init(Mutex::default)
}

/// Starts the job.
#[allow(clippy::too_many_lines)] // the tick: led tenants, their indexes, the sweep
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    executor: SharedExecutor,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let tick = std::env::var(TICK_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(1), Duration::from_millis);
    let mut led = crate::led::Led::new(&svc);
    tokio::spawn(async move {
        // Per led tenant: when its indexes were listed, and them.
        let mut known: HashMap<u64, (Instant, Vec<(u64, u32)>)> = HashMap::new();
        // Per led tenant: when its orphan chunk tables were last looked for.
        let mut swept: HashMap<u64, Instant> = HashMap::new();
        let mut timer = tokio::time::interval(tick);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = led.changed() => continue,
                () = wakeup().notified() => known.clear(),
                _ = timer.tick() => {}
            }
            if led.take_lagged() {
                known.clear();
            }
            let tenants = led.tenants(&svc).await;
            let led_now: HashSet<u64> = tenants.iter().copied().collect();
            known.retain(|t, _| led_now.contains(t));
            for t in tenants {
                // A tenant with indexes is listed again each tick; one
                // without, every LIST_IDLE (a CREATE wakes the job).
                let fresh = known
                    .get(&t)
                    .is_some_and(|(at, l)| l.is_empty() && at.elapsed() < LIST_IDLE);
                if !fresh {
                    match list(&svc, TenantId(t)).await {
                        Ok(v) => {
                            known.insert(t, (Instant::now(), v));
                        }
                        Err(e) => {
                            tracing::debug!(tenant = t, error = %e, "retrieval job: indexes not read");
                            continue;
                        }
                    }
                }
                let Some((_, ids)) = known.get(&t) else {
                    continue;
                };
                if ids.is_empty() {
                    continue;
                }
                let ids = ids.clone();
                let Ok(cat) = svc
                    .cache
                    .get(
                        TenantId(t),
                        sprig_catalog::cache::Freshness::Cached { min_version: 0 },
                    )
                    .await
                else {
                    continue;
                };
                let Some(ecat) = crate::engine::catalog::of(&cat) else {
                    continue;
                };
                // Chunk tables no index points to (left by a failure),
                // dropped at most every SWEEP_EVERY.
                if swept
                    .get(&t)
                    .is_none_or(|at: &Instant| at.elapsed() >= SWEEP_EVERY)
                {
                    swept.insert(t, Instant::now());
                    let mine: HashSet<u64> = ids.iter().map(|(id, _)| *id).collect();
                    let orphans: Vec<(String, String)> = ecat
                        .by_name
                        .keys()
                        .filter_map(|(d, n)| {
                            let id: u64 = n.strip_prefix(rx::TABLE_PREFIX)?.parse().ok()?;
                            (!mine.contains(&id)).then_some((*d, n.clone()))
                        })
                        .filter_map(|(d, n)| Some((cat.databases.get(d)?.name.clone(), n)))
                        .collect();
                    if !orphans.is_empty() {
                        let job = Job {
                            svc: Arc::clone(&svc),
                            login: Arc::clone(&login),
                            executor: Arc::clone(&executor),
                            tenant: TenantId(t),
                            id: 0,
                        };
                        let cat = Arc::clone(&cat);
                        tokio::spawn(async move { job.sweep(&cat, orphans).await });
                    }
                }
                let mut gone_dbs: Vec<u32> = Vec::new();
                for (id, db) in ids {
                    if !ecat.dbs.contains(&DbId(db)) {
                        if !gone_dbs.contains(&db) {
                            gone_dbs.push(db);
                        }
                        continue;
                    }
                    if !running()
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert((t, id))
                    {
                        continue;
                    }
                    let job = Job {
                        svc: Arc::clone(&svc),
                        login: Arc::clone(&login),
                        executor: Arc::clone(&executor),
                        tenant: TenantId(t),
                        id,
                    };
                    tokio::spawn(async move {
                        let (t, id) = (job.tenant.0, job.id);
                        if let Err(e) = Box::pin(job.round()).await {
                            tracing::warn!(tenant = t, index = id, error = %e,
                                "retrieval index round failed");
                        }
                        running()
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&(t, id));
                    });
                }
                for db in gone_dbs {
                    match forget_database(&svc, TenantId(t), db).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(
                            tenant = t,
                            db,
                            indexes = n,
                            "retrieval indexes of a dropped database forgotten"
                        ),
                        Err(e) => tracing::debug!(tenant = t, db, error = %e,
                            "retrieval indexes of a dropped database not forgotten yet"),
                    }
                }
            }
        }
    })
}

/// Forgets the retrieval indexes of database `db` when the tenant's latest
/// catalog no longer has it (a `DROP DATABASE`: their chunk tables went
/// with it); how many. Callable on its own (`DROP DATABASE` may call it);
/// the job calls it for any index whose database it doesn't find.
///
/// # Errors
/// The catalog's or the records'.
pub(crate) async fn forget_database(
    svc: &TenantServices,
    tenant: TenantId,
    db: u32,
) -> Result<u64> {
    let cat = svc
        .cache
        .get(tenant, sprig_catalog::cache::Freshness::Latest)
        .await?;
    if cat
        .databases
        .get(DbId(db))
        .is_some_and(|d| d.state == sprig_catalog::store::DbState::Active)
    {
        return Ok(0);
    }
    let mut tx = super::engine::writers(svc).begin(tenant).await?;
    let n = rec::delete_of_db(tx.as_mut(), db).await?;
    tx.commit().await?;
    Ok(n)
}

/// Every index of the tenant: (id, database).
async fn list(svc: &TenantServices, tenant: TenantId) -> Result<Vec<(u64, u32)>> {
    let mut tx = super::engine::writers(svc).begin(tenant).await?;
    Ok(rec::all(tx.as_mut())
        .await?
        .into_iter()
        .map(|i| (i.id, i.db_id))
        .collect())
}

/// A failure of the job's own statements.
fn sql_failed(what: &str, e: &sprig_doc::engine::SqlError) -> SprigError {
    SprigError::new(
        SprigCode::EmbeddingFailed,
        format!(
            "retrieval index: {what}: {}{}",
            e.errno.map_or_else(String::new, |n| format!("{n} ")),
            e.message
        ),
    )
}

/// A row to embed: the row, its hash, its (heading, chunk) pairs.
type ToEmbed<'a> = (&'a SrcRow, [u8; 16], Vec<(String, String)>);

/// A row of the source table as read.
#[derive(Debug, Clone)]
pub(super) struct SrcRow {
    /// The key columns' values as the text protocol gives them.
    pub key: Vec<Option<Vec<u8>>>,
    /// The text.
    pub text: Option<String>,
    /// The title.
    pub title: Option<String>,
    /// The filters' values as text.
    pub filters: Vec<Option<String>>,
}

/// A row's text cut into (heading, chunk) pairs by `chunking`, at most
/// `max` of them; the heading is the title (and, by headings, the headings
/// above the chunk).
pub(super) fn chunk_row(
    chunking: Chunking,
    text: &str,
    title: Option<&str>,
    max: usize,
) -> Vec<(String, String)> {
    let title = title.unwrap_or("").trim();
    let mut out: Vec<(String, String)> = match chunking {
        Chunking::Headings => sprig_docs::split::by_headings(title, text)
            .into_iter()
            .map(|c| (c.heading, c.text))
            .collect(),
        Chunking::Tokens(n) => {
            sprig_docs::split::by_tokens(text, usize::try_from(n).unwrap_or(400))
                .into_iter()
                .map(|c| (title.to_owned(), c))
                .collect()
        }
    };
    out.truncate(max.max(1));
    out
}

/// `(k1, k2, ...) = (v1, v2, ...)` over `cols` (the column of each key
/// column) as a conjunction.
fn key_eq(
    keys: &[KeyColumn],
    cols: &dyn Fn(&KeyColumn) -> String,
    v: &[Option<Vec<u8>>],
) -> String {
    let eq: Vec<String> = keys
        .iter()
        .zip(v)
        .map(|(k, x)| match x {
            None => format!("{} IS NULL", cols(k)),
            Some(b) => format!("{} = {}", cols(k), k.literal(Some(b))),
        })
        .collect();
    format!("({})", eq.join(" AND "))
}

/// The keyset condition "after key `v`" in key order.
fn after(keys: &[KeyColumn], v: &[Option<Vec<u8>>]) -> String {
    let mut ors = Vec::new();
    for i in 0..keys.len() {
        let mut and: Vec<String> = Vec::new();
        for j in 0..i {
            and.push(format!(
                "{} = {}",
                quote(&keys[j].name),
                keys[j].literal(v[j].as_deref())
            ));
        }
        and.push(format!(
            "{} > {}",
            quote(&keys[i].name),
            keys[i].literal(v[i].as_deref())
        ));
        ors.push(format!("({})", and.join(" AND ")));
    }
    ors.join(" OR ")
}

/// A key value of a change event (its key JSON) as the text protocol
/// gives it: numbers as their text, binary strings decoded from base64.
fn key_bytes(k: &KeyColumn, v: &J) -> Option<Vec<u8>> {
    match v {
        J::Null => None,
        J::String(s) if k.binary => base64::engine::general_purpose::STANDARD
            .decode(s)
            .ok()
            .or_else(|| Some(s.clone().into_bytes())),
        J::String(s) => Some(s.clone().into_bytes()),
        J::Bool(b) => Some(if *b { b"1".to_vec() } else { b"0".to_vec() }),
        other => Some(other.to_string().into_bytes()),
    }
}

/// The key values of an event's key object, in key order.
fn event_key(keys: &[KeyColumn], key: &J) -> Option<Vec<Option<Vec<u8>>>> {
    let o = key.as_object()?;
    keys.iter()
        .map(|k| {
            o.iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&k.name))
                .map(|(_, v)| key_bytes(k, v))
        })
        .collect()
}

struct Job {
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    executor: SharedExecutor,
    tenant: TenantId,
    id: u64,
}

/// One round's state.
struct Round<'a> {
    sql: SqlSession,
    ix: RetrievalIndex,
    db: String,
    table: String,
    chunking: Chunking,
    max_chunks: usize,
    /// `ai.embedding.backfill_rows_per_s` and `ai.embedding.rows_per_s`.
    rates: (u64, u64),
    /// Rows that gained (+) or lost (-) their chunks since the last save.
    delta: i64,
    /// Count the rows with chunks again at the next save.
    recount: bool,
    /// Chunks written (+) or deleted (-) since the last save.
    chunk_delta: i64,
    model: Arc<sprig_embed::Embedder>,
    svc: &'a TenantServices,
    tenant: TenantId,
}

/// When each index's counts were last taken in full: again on the first
/// round of a process, after a failed round, and every [`RECOUNT_EVERY`].
fn recounts() -> &'static Mutex<HashMap<(u64, u64), Instant>> {
    static R: OnceLock<Mutex<HashMap<(u64, u64), Instant>>> = OnceLock::new();
    R.get_or_init(Mutex::default)
}

/// How often an index's counts are taken in full.
const RECOUNT_EVERY: Duration = Duration::from_secs(300);

fn recount_due(tenant: u64, id: u64) -> bool {
    recounts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&(tenant, id))
        .is_none_or(|at| at.elapsed() >= RECOUNT_EVERY)
}

fn recounted(tenant: u64, id: u64) {
    recounts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((tenant, id), Instant::now());
}

/// Takes `NO_BACKSLASH_ESCAPES` out of the session's `sql_mode` (the
/// job's literals escape with backslashes).
async fn clear_nbe(sql: &mut SqlSession) -> Result<()> {
    let rows = sql
        .query("SELECT @@SESSION.sql_mode")
        .await
        .map_err(|e| sql_failed("reading sql_mode", &e))?;
    let mode = rows
        .first()
        .and_then(|r| r.first())
        .and_then(Option::as_ref)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    if !mode.to_ascii_uppercase().contains("NO_BACKSLASH_ESCAPES") {
        return Ok(());
    }
    let kept: Vec<&str> = mode
        .split(',')
        .filter(|m| !m.trim().eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"))
        .filter(|m| m.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        .collect();
    sql.exec(&format!("SET SESSION sql_mode = '{}'", kept.join(",")))
        .await
        .map(|_| ())
        .map_err(|e| sql_failed("clearing NO_BACKSLASH_ESCAPES", &e))
}

/// A tenant user who may drop the tenant's tables (the first in id
/// order), to clean up after an index whose creator is gone.
fn admin_user(svc: &TenantServices, cat: &sprig_catalog::TenantCatalog) -> Option<u32> {
    let auth = sprig_auth::TenantAuth::of(cat)?;
    let mut ids: Vec<u32> = auth.users().map(|u| u.row.user_id.0).collect();
    ids.sort_unstable();
    ids.into_iter().find(|&u| {
        crate::tenant::authz::grants_of(svc, cat, Some(sprig_common::ids::UserId(u))).allows(
            sprig_auth::authz::Action::Drop,
            sprig_auth::authz::Target::Tenant,
        )
    })
}

/// Records `e` as index `id`'s last error (`SHOW RETRIEVAL INDEXES`).
async fn note(svc: &TenantServices, tenant: TenantId, id: u64, e: &str) {
    let Ok(mut tx) = super::engine::writers(svc).begin(tenant).await else {
        return;
    };
    if let Ok(Some(mut ix)) = rec::get_for_update(tx.as_mut(), id).await
        && ix.last_error.as_deref() != Some(e)
    {
        ix.last_error = Some(e.to_owned());
        if rec::put(tx.as_mut(), &ix).await.is_ok() {
            let _ = tx.commit().await;
        }
    }
}

impl Job {
    /// One round of the index; its error is also recorded on the index.
    async fn round(self) -> Result<()> {
        let (svc, tenant, id) = (Arc::clone(&self.svc), self.tenant, self.id);
        let r = Box::pin(self.round_inner()).await;
        if let Err(e) = &r {
            note(&svc, tenant, id, &e.to_string()).await;
            // The counts may have missed the failed step's writes.
            recounts()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&(tenant.0, id));
        }
        r
    }

    #[allow(clippy::too_many_lines)] // the round's steps, in order
    async fn round_inner(self) -> Result<()> {
        let svc = &*self.svc;
        let mut cat = svc
            .cache
            .get(
                self.tenant,
                sprig_catalog::cache::Freshness::Cached { min_version: 0 },
            )
            .await?;
        let mut tx = super::engine::writers(svc).begin(self.tenant).await?;
        let Some(ix) = rec::get(tx.as_mut(), self.id).await? else {
            return Ok(());
        };
        drop(tx);
        let source = |cat: &sprig_catalog::TenantCatalog| {
            let db = cat
                .databases
                .get(DbId(ix.db_id))
                .filter(|d| d.state == sprig_catalog::store::DbState::Active)
                .map(|d| d.name.clone());
            // A table renamed into another database is gone from this one.
            let table = cat
                .objects
                .get(ObjectId(ix.object_id))
                .filter(|o| {
                    o.state == sprig_catalog::store::ObjectState::Active && o.db_id.0 == ix.db_id
                })
                .map(|o| o.name.clone());
            let def = crate::engine::catalog::of(cat)
                .and_then(|e| e.tables.get(&ObjectId(ix.object_id)))
                .and_then(|p| p.as_ref().as_ref().ok())
                .cloned();
            (db, table, def)
        };
        let (mut db, mut table, mut def) = source(&cat);
        if db.is_none() || table.is_none() || def.is_none() {
            // Gone, or a snapshot older than the table: ask for the latest.
            cat = svc
                .cache
                .get(self.tenant, sprig_catalog::cache::Freshness::Latest)
                .await?;
            (db, table, def) = source(&cat);
        }
        if db.is_none() {
            // Its database is gone, chunk table and all.
            forget_database(svc, self.tenant, ix.db_id).await?;
            return Ok(());
        }
        if rec::dropping(&ix) || table.is_none() || def.is_none() {
            // A DROP RETRIEVAL INDEX that didn't finish, or a source table
            // that is gone: the index goes, as its creator or, when that
            // user is gone too, a tenant administrator.
            let sql = self.session(&cat, ix.created_by, true).await?;
            return Self::forget(svc, self.tenant, sql, &ix, db.as_deref()).await;
        }
        let (Some(db), Some(table), Some(def)) = (db, table, def) else {
            return Ok(());
        };
        let Some(user) = ix.created_by else {
            return Err(sprig_ai::embedding::failed(
                "retrieval index: it has no creator to run as; drop it and create it again",
            ));
        };
        // A filled index with no change since its token: nothing to do,
        // and no session is opened.
        if ix.progress.get("fill") == Some(&json!("done"))
            && ix.progress.get("table") == Some(&json!(true))
            && Self::idle(svc, self.tenant, &ix, &db, &def).await
        {
            return Ok(());
        }
        let mut sql = self.session(&cat, Some(user), false).await?;
        // The job's literals escape with backslashes.
        if let Err(e) = clear_nbe(&mut sql).await {
            sql.close().await;
            return Err(e);
        }
        if ix.progress.get("table") != Some(&json!(true))
            && let Err(e) = Self::ensure_table(svc, self.tenant, &mut sql, &ix, &db).await
        {
            sql.close().await;
            return Err(e);
        }
        let _exempt = crate::tenant::rls::exempt_internal(
            sql.session_id(),
            self.tenant.0,
            "retrieval index",
            &format!("{db}.{table}"),
        );
        let dir = crate::docs::service()
            .map(|d| d.model_dir())
            .ok_or_else(|| {
                sprig_ai::embedding::failed("retrieval index: this gateway has no embedding model")
            })?;
        let model = tokio::task::spawn_blocking(move || sprig_embed::local::shared(&dir))
            .await
            .map_err(|e| SprigError::internal(format!("model load: {e}")))?
            .map_err(sprig_ai::embedding::failed)?;
        let max_chunks = svc.settings.get(
            &rx::MAX_CHUNKS_PER_ROW,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        );
        let mut r = Round {
            sql,
            chunking: Chunking::from_catalog(&ix.chunking)
                .unwrap_or(Chunking::Tokens(sprig_sql::retrieval::DEFAULT_CHUNK_TOKENS)),
            ix,
            db: db.clone(),
            table: table.clone(),
            max_chunks: usize::try_from(max_chunks).unwrap_or(64),
            rates: (
                svc.settings.get(
                    &sprig_ai::embedding::BACKFILL_ROWS_PER_S,
                    &cat.setting_ctx(&svc.dc),
                    Some(&cat.settings),
                ),
                svc.settings.get(
                    &sprig_ai::embedding::ROWS_PER_S,
                    &cat.setting_ctx(&svc.dc),
                    Some(&cat.settings),
                ),
            ),
            model,
            svc,
            tenant: self.tenant,
            delta: 0,
            recount: recount_due(self.tenant.0, self.id),
            chunk_delta: 0,
        };
        let watched: HashMap<u32, Watched> = u32::try_from(r.ix.object_id)
            .ok()
            .map(|o| (o, Watched { def, db }))
            .into_iter()
            .collect();
        let res = Box::pin(r.work(&watched)).await;
        if let Err(e) = &res {
            let _ = r.note_error(&e.to_string()).await;
        }
        r.sql.close().await;
        res
    }

    /// Drops chunk tables (database, table) that no index points to, as a
    /// tenant administrator.
    async fn sweep(self, cat: &sprig_catalog::TenantCatalog, orphans: Vec<(String, String)>) {
        let mut sql = match self.session(cat, None, true).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(tenant = self.tenant.0, error = %e, "orphan chunk tables not swept");
                return;
            }
        };
        // The records as they are now: an index made since the listing
        // keeps its table.
        let Ok(now) = list(&self.svc, self.tenant).await else {
            sql.close().await;
            return;
        };
        let live: HashSet<u64> = now.iter().map(|(id, _)| *id).collect();
        for (db, table) in orphans {
            if table
                .strip_prefix(rx::TABLE_PREFIX)
                .and_then(|i| i.parse::<u64>().ok())
                .is_none_or(|id| live.contains(&id))
            {
                continue;
            }
            match sql
                .exec(&format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    quote(&db),
                    quote(&table)
                ))
                .await
            {
                Ok(_) => tracing::info!(event = "retrieval.chunks.swept", tenant = self.tenant.0,
                    db = %db, table = %table, "a chunk table no index points to was dropped"),
                Err(e) => tracing::warn!(tenant = self.tenant.0, db = %db, table = %table,
                    error = %e.message, "an orphan chunk table wasn't dropped"),
            }
        }
        sql.close().await;
    }

    /// A session for the index's job: as `user` (its creator); with
    /// `fallback`, when that user can't log in any more, as a tenant
    /// administrator (to drop what the index left; audited).
    async fn session(
        &self,
        cat: &sprig_catalog::TenantCatalog,
        user: Option<u32>,
        fallback: bool,
    ) -> Result<SqlSession> {
        let svc = &*self.svc;
        let name = crate::inherited::tenant_name(svc, self.tenant).await?;
        let mut why = "it has no creator".to_owned();
        if let Some(u) = user {
            match crate::triggers::principal_of(&self.login, self.tenant.0, &name, u).await {
                Ok(p) => return self.open(&p),
                Err(e) => why = format!("its creator (user {u}) can't run it: {e}"),
            }
        }
        if fallback && let Some(admin) = admin_user(svc, cat) {
            let p = crate::triggers::principal_of(&self.login, self.tenant.0, &name, admin).await?;
            tracing::info!(target: "sprig_audit", event = "retrieval.run_as_admin",
                tenant = self.tenant.0, index = self.id, user = admin, reason = %why,
                "a retrieval index is cleaned up as a tenant administrator");
            return self.open(&p);
        }
        Err(sprig_ai::embedding::failed(format!(
            "retrieval index: {why}"
        )))
    }

    fn open(&self, p: &crate::session::Principal) -> Result<SqlSession> {
        SqlSession::open(
            &self.svc.sessions,
            &self.executor,
            p,
            "retrieval",
            0,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            None,
            64 << 20,
        )
    }

    /// Makes the chunk table, if it was never made (a crash between the
    /// index's record and its CREATE), under the record's lock: a DROP
    /// waits for it, and an index being dropped never gets one again.
    async fn ensure_table(
        svc: &TenantServices,
        tenant: TenantId,
        sql: &mut SqlSession,
        ix: &RetrievalIndex,
        db: &str,
    ) -> Result<()> {
        let mut tx = super::engine::writers(svc).begin(tenant).await?;
        let Some(mut now) = rec::get_for_update(tx.as_mut(), ix.id).await? else {
            return Err(SprigError::internal("the retrieval index was dropped"));
        };
        if rec::dropping(&now) {
            return Err(SprigError::internal("the retrieval index is being dropped"));
        }
        if now.progress.get("table") == Some(&json!(true)) {
            return Ok(());
        }
        let ddl = rx::create_engine_table_sql(db, ix.id, &ix.key_columns, &ix.filters);
        sql.exec(&ddl)
            .await
            .map_err(|e| sql_failed("making the chunk table", &e))?;
        now.progress["table"] = json!(true);
        rec::put(tx.as_mut(), &now).await?;
        tx.commit().await
    }

    /// Whether the table has no change after the index's token (the
    /// token moved past other tables' changes is saved).
    async fn idle(
        svc: &TenantServices,
        tenant: TenantId,
        ix: &RetrievalIndex,
        db: &str,
        def: &Arc<crate::engine::table::TableDef>,
    ) -> bool {
        let (Some(log), Ok(eng), Ok(object)) = (
            crate::changelog::changes(),
            crate::engine::engine(),
            u32::try_from(ix.object_id),
        ) else {
            return false;
        };
        let watched: HashMap<u32, Watched> = [(
            object,
            Watched {
                def: Arc::clone(def),
                db: db.to_owned(),
            },
        )]
        .into_iter()
        .collect();
        let token = ix.progress.get("token").and_then(J::as_str);
        let since = ix
            .progress
            .get("since_ms")
            .and_then(J::as_i64)
            .unwrap_or(0)
            .max(1);
        let Ok(b) = table_changes::read(
            eng,
            &log,
            tenant.0,
            &watched,
            token,
            if token.is_some() { 0 } else { since },
            table_changes::PAGE_BYTES,
            Duration::ZERO,
            "",
        )
        .await
        else {
            return false;
        };
        if !b.events.is_empty() || !b.at_end {
            return false;
        }
        if token != Some(b.next.as_str())
            && let Ok(mut tx) = super::engine::writers(svc).begin(tenant).await
            && let Ok(Some(mut now)) = rec::get_for_update(tx.as_mut(), ix.id).await
            && now.progress.get("token") == ix.progress.get("token")
        {
            now.progress["token"] = json!(b.next);
            if rec::put(tx.as_mut(), &now).await.is_ok() {
                let _ = tx.commit().await;
            }
        }
        true
    }

    /// Drops an index whose source is gone (or whose `DROP` didn't finish):
    /// its chunk table first, then its record, so a failed drop keeps the
    /// record and is tried again.
    async fn forget(
        svc: &TenantServices,
        tenant: TenantId,
        mut sql: SqlSession,
        ix: &RetrievalIndex,
        db: Option<&str>,
    ) -> Result<()> {
        if let Some(db) = db {
            let r = sql
                .exec(&format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    quote(db),
                    quote(&rx::table_name(ix.id))
                ))
                .await;
            if let Err(e) = r {
                sql.close().await;
                return Err(sql_failed("dropping the chunk table", &e));
            }
        }
        sql.close().await;
        let mut tx = super::engine::writers(svc).begin(tenant).await?;
        rec::delete(tx.as_mut(), ix.id).await?;
        tx.commit().await?;
        tracing::info!(event = "retrieval.chunks.dropped", tenant = %tenant, db = db.unwrap_or(""),
            index = %ix.name, id = ix.id, "retrieval index dropped with its chunk table");
        Ok(())
    }
}

impl Round<'_> {
    fn source(&self) -> String {
        format!("{}.{}", quote(&self.db), quote(&self.table))
    }

    fn chunks(&self) -> String {
        format!("{}.{}", quote(&self.db), quote(&rx::table_name(self.ix.id)))
    }

    /// The columns read of a source row: keys, text, title, filters.
    fn select_list(&self) -> String {
        let mut cols: Vec<String> = self.ix.key_columns.iter().map(|k| quote(&k.name)).collect();
        cols.push(quote(&self.ix.text_column));
        cols.push(
            self.ix
                .title_column
                .as_deref()
                .map_or_else(|| "NULL".to_owned(), quote),
        );
        cols.extend(self.ix.filters.iter().map(|f| quote(&f.name)));
        cols.join(", ")
    }

    fn rows_of(&self, rows: Vec<Vec<Option<Vec<u8>>>>) -> Vec<SrcRow> {
        let nk = self.ix.key_columns.len();
        let s = |v: &Option<Vec<u8>>| v.as_ref().map(|b| String::from_utf8_lossy(b).into_owned());
        rows.into_iter()
            .map(|r| SrcRow {
                key: r[..nk].to_vec(),
                text: r.get(nk).and_then(s),
                title: r.get(nk + 1).and_then(s),
                filters: (0..self.ix.filters.len())
                    .map(|i| r.get(nk + 2 + i).and_then(s))
                    .collect(),
            })
            .collect()
    }

    async fn query(&mut self, what: &str, q: &str) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        self.sql.query(q).await.map_err(|e| sql_failed(what, &e))
    }

    async fn exec(&mut self, what: &str, q: &str) -> Result<()> {
        self.exec_n(what, q).await.map(|_| ())
    }

    /// `q`'s affected rows.
    async fn exec_n(&mut self, what: &str, q: &str) -> Result<u64> {
        self.sql
            .exec(q)
            .await
            .map(|d| d.affected)
            .map_err(|e| sql_failed(what, &e))
    }

    /// Fill steps, then changes, within one round's time.
    #[allow(clippy::too_many_lines)] // the fill, then the changes
    async fn work(&mut self, watched: &HashMap<u32, Watched>) -> Result<()> {
        let started = Instant::now();
        let mut fill_done = self.ix.progress.get("fill") == Some(&json!("done"));

        while !fill_done && started.elapsed() < ROUND {
            let from: Option<Vec<Option<Vec<u8>>>> =
                self.ix.progress.get("fill").and_then(J::as_array).map(|a| {
                    a.iter()
                        .map(|v| v.as_str().map(|s| s.as_bytes().to_vec()))
                        .collect()
                });
            let keys = self.ix.key_columns.clone();
            let order: Vec<String> = keys.iter().map(|k| quote(&k.name)).collect();
            let pred = from
                .as_ref()
                .filter(|f| f.len() == keys.len())
                .map_or_else(|| "TRUE".to_owned(), |f| after(&keys, &self.raw_keys(f)));
            let q = format!(
                "SELECT {} FROM {} WHERE {pred} ORDER BY {} LIMIT {FILL_ROWS}",
                self.select_list(),
                self.source(),
                order.join(", ")
            );
            let rows = self.query("reading the table", &q).await?;
            let n = rows.len();
            let rows = self.rows_of(rows);
            let last = rows.last().map(|r| r.key.clone());
            let step = Instant::now();
            self.apply(&rows, &[]).await?;
            pace(step, n, self.rates.0).await;
            if n < FILL_ROWS {
                fill_done = true;
                self.recount = true;
                self.ix.progress["fill"] = json!("done");
                if self.ix.state != State::Ready {
                    self.ix.state = State::Ready;
                }
            } else if let Some(k) = last {
                self.ix.progress["fill"] = J::Array(
                    k.iter()
                        .zip(&keys)
                        .map(|(v, c)| c.text(v.as_deref()).map_or(J::Null, J::String))
                        .collect(),
                );
            }
            self.count_and_save().await?;
        }
        // Changes since the token (or the index's creation).
        let Some(log) = crate::changelog::changes() else {
            return Err(sprig_ai::embedding::failed(
                "retrieval index: the node's change log is off (SPRIGDB_ENGINE_CHANGELOG)",
            ));
        };
        let eng = crate::engine::engine()?;
        let consumer = format!("rx:{}:{}", self.tenant.0, self.ix.id);
        for _ in 0..PAGES {
            if started.elapsed() >= ROUND * 2 {
                break;
            }
            let token = self
                .ix
                .progress
                .get("token")
                .and_then(J::as_str)
                .map(str::to_owned);
            let since = self
                .ix
                .progress
                .get("since_ms")
                .and_then(J::as_i64)
                .unwrap_or(0)
                .max(1);
            let b = match table_changes::read(
                eng,
                &log,
                self.tenant.0,
                watched,
                token.as_deref(),
                if token.is_some() { 0 } else { since },
                table_changes::PAGE_BYTES,
                Duration::ZERO,
                &consumer,
            )
            .await
            {
                Ok(b) => b,
                Err(e) if e.code() == Some(SprigCode::SinkSourceLost) => {
                    // The log no longer holds the place: fill again.
                    tracing::warn!(index = self.ix.id, error = %e,
                        "retrieval index: changes lost; filling again");
                    // Its old chunks go: a row deleted meanwhile has no
                    // change left to clear them.
                    self.exec(
                        "emptying the chunk table",
                        &format!("DELETE FROM {}", self.chunks()),
                    )
                    .await?;
                    self.ix.progress = json!({"since_ms": super::engine::now_ms(), "table": true});
                    self.ix.state = State::Rebuilding;
                    self.recount = true;
                    self.count_and_save().await?;
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            let keys = self.ix.key_columns.clone();
            // Each changed key once, and whether its last change deleted it.
            let mut changed: Vec<(Vec<Option<Vec<u8>>>, bool)> = Vec::new();
            let mut at: HashMap<String, usize> = HashMap::new();
            for ev in &b.events {
                let Some(k) = event_key(&keys, &ev.key) else {
                    continue;
                };
                let deleted = ev.op == "d";
                if let Some(&i) = at.get(&ev.key.to_string()) {
                    changed[i].1 = deleted;
                } else {
                    at.insert(ev.key.to_string(), changed.len());
                    changed.push((k, deleted));
                }
            }
            rx::metrics::backlog(u64::try_from(changed.len()).unwrap_or(0));
            if !changed.is_empty() {
                let step = Instant::now();
                let n = changed.len();
                let all: Vec<Vec<Option<Vec<u8>>>> = changed.iter().map(|c| c.0.clone()).collect();
                let rows = self.read_keys(&all).await?;
                // Gone: a key whose last change was a delete and which no
                // row has now, each checked on its own (a key's text in the
                // change event needn't be the row's: floats, exponents).
                let mut gone = Vec::new();
                for (k, deleted) in &changed {
                    if *deleted && !self.exists(k).await? {
                        gone.push(k.clone());
                    }
                }
                self.apply(&rows, &gone).await?;
                pace(step, n, self.rates.1).await;
            }
            let moved = token.as_deref() != Some(b.next.as_str());
            self.ix.progress["token"] = json!(b.next);
            if moved || !b.events.is_empty() {
                self.count_and_save().await?;
            }
            if b.at_end {
                break;
            }
        }
        rx::metrics::backlog(0);
        Ok(())
    }

    /// Text-protocol key bytes from the saved fill position (text; binary
    /// columns hexadecimal).
    fn raw_keys(&self, v: &[Option<Vec<u8>>]) -> Vec<Option<Vec<u8>>> {
        self.ix
            .key_columns
            .iter()
            .zip(v)
            .map(|(k, x)| {
                x.as_ref().map(|b| {
                    if k.binary {
                        unhex(b).unwrap_or_else(|| b.clone())
                    } else {
                        b.clone()
                    }
                })
            })
            .collect()
    }

    /// A key as text, numbers in one form (`1.50` and `1.5` are one
    /// key: a change event's JSON writes a decimal as the double it is).
    fn key_text(&self, v: &[Option<Vec<u8>>]) -> Vec<Option<String>> {
        self.ix
            .key_columns
            .iter()
            .zip(v)
            .map(|(k, x)| {
                k.text(x.as_deref())
                    .map(|t| if k.numeric { number_key(&t) } else { t })
            })
            .collect()
    }

    /// Whether the source has a row with key `k` now.
    async fn exists(&mut self, k: &[Option<Vec<u8>>]) -> Result<bool> {
        let cols = self.ix.key_columns.clone();
        let q = format!(
            "SELECT 1 FROM {} WHERE {} LIMIT 1",
            self.source(),
            key_eq(&cols, &|c| quote(&c.name), k)
        );
        Ok(!self.query("checking a deleted row", &q).await?.is_empty())
    }

    /// The current rows of `keys`.
    async fn read_keys(&mut self, keys: &[Vec<Option<Vec<u8>>>]) -> Result<Vec<SrcRow>> {
        let cols = self.ix.key_columns.clone();
        let mut out = Vec::new();
        for part in keys.chunks(200) {
            let pred: Vec<String> = part
                .iter()
                .map(|k| key_eq(&cols, &|c| quote(&c.name), k))
                .collect();
            let q = format!(
                "SELECT {} FROM {} WHERE {}",
                self.select_list(),
                self.source(),
                pred.join(" OR ")
            );
            let rows = self.query("reading changed rows", &q).await?;
            out.extend(self.rows_of(rows));
        }
        Ok(out)
    }

    /// Makes the chunks of `rows` match them, and removes the chunks of
    /// `gone` (rows deleted).
    #[allow(clippy::too_many_lines)] // hashes, embeddings, writes
    async fn apply(&mut self, rows: &[SrcRow], gone: &[Vec<Option<Vec<u8>>>]) -> Result<()> {
        let keys = self.ix.key_columns.clone();
        let kcol = |c: &KeyColumn| quote(&rx::key_column(&c.name));
        // Chunk 0's hash of each row now.
        let mut stored: HashMap<Vec<Option<String>>, Vec<u8>> = HashMap::new();
        for part in rows.chunks(200) {
            let pred: Vec<String> = part.iter().map(|r| key_eq(&keys, &kcol, &r.key)).collect();
            let kc: Vec<String> = keys.iter().map(kcol).collect();
            let q = format!(
                "SELECT {}, `src_hash` FROM {} WHERE `chunk_no` = 0 AND ({})",
                kc.join(", "),
                self.chunks(),
                pred.join(" OR ")
            );
            for r in self.query("reading chunk hashes", &q).await? {
                // In the same form as the rows' keys are looked up in.
                let k = self.key_text(&r[..keys.len()]);
                stored.insert(k, r[keys.len()].clone().unwrap_or_default());
            }
        }
        // What to embed.
        let mut work: Vec<ToEmbed<'_>> = Vec::new();
        let mut cleared: Vec<Vec<Option<Vec<u8>>>> = gone.to_vec();
        let mut skipped = 0u64;
        for r in rows {
            let had = stored.get(&self.key_text(&r.key));
            let text = r.text.as_deref().filter(|t| !t.trim().is_empty());
            let Some(text) = text else {
                if had.is_some() {
                    cleared.push(r.key.clone());
                }
                continue;
            };
            let hash = rx::row_hash(Some(text), r.title.as_deref(), &r.filters);
            if had.is_some_and(|h| h.as_slice() == hash) {
                skipped += 1;
                continue;
            }
            let chunks = chunk_row(self.chunking, text, r.title.as_deref(), self.max_chunks);
            if had.is_none() {
                self.delta += 1;
            }
            work.push((r, hash, chunks));
        }
        rx::metrics::rows("skipped", skipped);
        let inputs: Vec<String> = work
            .iter()
            .flat_map(|(_, _, ch)| ch.iter().map(|(h, t)| rx::embedding_input(h, t)))
            .collect();
        let vectors = if inputs.is_empty() {
            Vec::new()
        } else {
            let model = Arc::clone(&self.model);
            let started = Instant::now();
            let v = tokio::task::spawn_blocking(move || {
                let mut out = Vec::with_capacity(inputs.len());
                for part in inputs.chunks(16) {
                    let refs: Vec<&str> = part.iter().map(String::as_str).collect();
                    out.extend(
                        model
                            .embed_passages_truncated(&refs, sprig_embed::MAX_TOKENS)?
                            .into_iter()
                            .map(|v| v.to_vec()),
                    );
                }
                Ok::<_, sprig_embed::EmbedError>(out)
            })
            .await
            .map_err(|e| SprigError::internal(format!("the embedding task stopped: {e}")))?
            .map_err(|e| sprig_ai::embedding::failed(format!("the model failed: {e}")))?;
            sprig_ai::embedding::metrics::seconds(
                &self.ix.model.alias,
                started.elapsed().as_secs_f64(),
            );
            v
        };
        // Deleted rows first: a key read again under another spelling of
        // its number is never deleted after its new chunks are written.
        for part in cleared.chunks(200) {
            if part.is_empty() {
                continue;
            }
            let pred: Vec<String> = part.iter().map(|k| key_eq(&keys, &kcol, k)).collect();
            let had = self
                .query(
                    "counting chunks",
                    &format!(
                        "SELECT COUNT(*) FROM {} WHERE `chunk_no` = 0 AND ({})",
                        self.chunks(),
                        pred.join(" OR ")
                    ),
                )
                .await?;
            let had: i64 = had
                .first()
                .and_then(|r| r.first())
                .and_then(Option::as_ref)
                .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
                .unwrap_or(0);
            self.delta -= had;
            let n = self
                .exec_n(
                    "deleting chunks",
                    &format!("DELETE FROM {} WHERE {}", self.chunks(), pred.join(" OR ")),
                )
                .await?;
            self.chunk_delta -= i64::try_from(n).unwrap_or(0);
        }
        // Each row's chunks replaced in one transaction.
        let mut vi = 0usize;
        let mut cols: Vec<String> = keys.iter().map(kcol).collect();
        cols.extend(["`chunk_no`", "`title`", "`text`"].map(str::to_owned));
        cols.extend(
            self.ix
                .filters
                .iter()
                .map(|f| quote(&rx::filter_column(&f.name))),
        );
        cols.extend(["`src_hash`", "`embedding`"].map(str::to_owned));
        let (mut indexed, mut written) = (0u64, 0u64);
        for (r, hash, chunks) in &work {
            let lits: Vec<String> = keys
                .iter()
                .zip(&r.key)
                .map(|(k, v)| k.literal(v.as_deref()))
                .collect();
            let per = 3 + r.filters.len() + 2;
            let mut marks = Vec::new();
            let mut params: Vec<Param> = Vec::new();
            for (n, (heading, text)) in chunks.iter().enumerate() {
                let Some(v) = vectors.get(vi) else { break };
                vi += 1;
                let mut row = lits.clone();
                row.extend(std::iter::repeat_n("?".to_owned(), per));
                marks.push(format!("({})", row.join(", ")));
                params.push(Param::Int(i64::try_from(n).unwrap_or(0)));
                params.push(if heading.is_empty() {
                    Param::Null
                } else {
                    Param::Text(heading.clone())
                });
                params.push(Param::Text(text.clone()));
                for f in &r.filters {
                    params.push(f.clone().map_or(Param::Null, Param::Text));
                }
                params.push(Param::Bin(hash.to_vec()));
                params.push(Param::Bin(rx::vector_bytes(v)));
            }
            let del = format!(
                "DELETE FROM {} WHERE {}",
                self.chunks(),
                key_eq(&keys, &kcol, &r.key)
            );
            self.exec("starting a write", "START TRANSACTION").await?;
            let res = async {
                let gone = self.exec_n("deleting old chunks", &del).await?;
                if !marks.is_empty() {
                    let ins = format!(
                        "INSERT INTO {} ({}) VALUES {}",
                        self.chunks(),
                        cols.join(", "),
                        marks.join(", ")
                    );
                    self.sql
                        .exec_bound(&ins, &params)
                        .await
                        .map_err(|e| sql_failed("writing chunks", &e))?;
                }
                self.exec("committing chunks", "COMMIT").await?;
                Ok::<u64, SprigError>(gone)
            }
            .await;
            match res {
                Ok(gone) => {
                    self.chunk_delta +=
                        i64::try_from(marks.len()).unwrap_or(0) - i64::try_from(gone).unwrap_or(0);
                }
                Err(e) => {
                    let _ = self.sql.exec("ROLLBACK").await;
                    return Err(e);
                }
            }
            indexed += 1;
            written += u64::try_from(marks.len()).unwrap_or(0);
        }
        rx::metrics::rows("indexed", indexed);
        rx::metrics::rows("cleared", u64::try_from(cleared.len()).unwrap_or(0));
        rx::metrics::chunks(written);
        Ok(())
    }

    /// Counts the rows with chunks and saves the index's progress, state
    /// and count (the record re-read first: a dropped index stays dropped).
    async fn count_and_save(&mut self) -> Result<()> {
        if self.recount {
            let q = format!(
                "SELECT COUNT(*), SUM(CASE WHEN `chunk_no` = 0 THEN 1 ELSE 0 END) FROM {}",
                self.chunks()
            );
            let num = |r: &Vec<Option<Vec<u8>>>, i: usize| -> Option<u64> {
                r.get(i)?
                    .as_ref()
                    .and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
            };
            if let Ok(rows) = self.sql.query(&q).await
                && let Some(r) = rows.first()
                && let Some(chunks) = num(r, 0)
            {
                self.ix.rows_indexed = num(r, 1).unwrap_or(0);
                self.ix.progress["chunks"] = json!(chunks);
                self.delta = 0;
                self.chunk_delta = 0;
                self.recount = false;
                recounted(self.tenant.0, self.ix.id);
            }
        }
        let chunks = i64::try_from(
            self.ix
                .progress
                .get("chunks")
                .and_then(J::as_u64)
                .unwrap_or(0),
        )
        .unwrap_or(i64::MAX)
            + self.chunk_delta;
        self.ix.progress["chunks"] = json!(u64::try_from(chunks.max(0)).unwrap_or(0));
        self.chunk_delta = 0;
        let n = i64::try_from(self.ix.rows_indexed).unwrap_or(i64::MAX) + self.delta;
        self.ix.rows_indexed = u64::try_from(n.max(0)).unwrap_or(0);
        self.delta = 0;
        self.ix.last_error = None;
        let mut tx = super::engine::writers(self.svc).begin(self.tenant).await?;
        match rec::get_for_update(tx.as_mut(), self.ix.id).await? {
            None => return Err(SprigError::internal("the retrieval index was dropped")),
            // A DROP began: its mark stays.
            Some(now) if rec::dropping(&now) => {
                return Err(SprigError::internal("the retrieval index is being dropped"));
            }
            Some(_) => {}
        }
        rec::put(tx.as_mut(), &self.ix).await?;
        tx.commit().await
    }

    /// Records the round's error in the index (`SHOW RETRIEVAL INDEXES`).
    async fn note_error(&mut self, e: &str) -> Result<()> {
        let mut tx = super::engine::writers(self.svc).begin(self.tenant).await?;
        let Some(mut ix) = rec::get_for_update(tx.as_mut(), self.ix.id).await? else {
            return Ok(());
        };
        ix.last_error = Some(e.to_owned());
        rec::put(tx.as_mut(), &ix).await?;
        tx.commit().await
    }
}

/// A number's text in one form: no `+`, no trailing fraction zeros, no
/// trailing point, `-0` as `0`; text that isn't a plain decimal as it is.
pub(super) fn number_key(t: &str) -> String {
    let s = t.trim().trim_start_matches('+');
    let plain = !s.is_empty()
        && s.trim_start_matches('-')
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'.')
        && s.matches('.').count() <= 1;
    if !plain {
        return t.to_owned();
    }
    let (neg, digits) = s.strip_prefix('-').map_or((false, s), |d| (true, d));
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let body = if frac.is_empty() {
        int.to_owned()
    } else {
        format!("{int}.{frac}")
    };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// Waits so that `rows` rows handled since `since` stay within `per_s`
/// rows a second.
async fn pace(since: Instant, rows: usize, per_s: u64) {
    if per_s == 0 || rows == 0 {
        return;
    }
    #[allow(clippy::cast_precision_loss)]
    let want = Duration::from_secs_f64(rows as f64 / per_s as f64);
    if let Some(rest) = want.checked_sub(since.elapsed()) {
        tokio::time::sleep(rest).await;
    }
}

fn unhex(b: &[u8]) -> Option<Vec<u8>> {
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<KeyColumn> {
        vec![
            KeyColumn {
                name: "org".into(),
                column_type: "varchar(8)".into(),
                binary: false,
                numeric: false,
            },
            KeyColumn {
                name: "id".into(),
                column_type: "int".into(),
                binary: false,
                numeric: true,
            },
        ]
    }

    #[test]
    fn keysets_keys_and_chunks() {
        let k = keys();
        let v = vec![Some(b"a'b".to_vec()), Some(b"7".to_vec())];
        assert_eq!(
            after(&k, &v),
            "(`org` > 'a''b') OR (`org` = 'a''b' AND `id` > 7)"
        );
        assert_eq!(
            key_eq(&k, &|c| quote(&c.name), &v),
            "(`org` = 'a''b' AND `id` = 7)"
        );
        let ev = json!({"id": 7, "org": "a'b"});
        assert_eq!(event_key(&k, &ev).unwrap(), v);
        let bin = KeyColumn {
            name: "b".into(),
            column_type: "binary(2)".into(),
            binary: true,
            numeric: false,
        };
        assert_eq!(key_bytes(&bin, &json!("qwE=")).unwrap(), vec![0xab, 0x01]);
        assert_eq!(unhex(b"ab01").unwrap(), vec![0xab, 0x01]);
        for (a, b) in [
            ("1.50", "1.5"),
            ("1.5", "1.5"),
            ("10.00", "10"),
            ("007", "7"),
            ("-0.0", "0"),
            ("+2.10", "2.1"),
            ("0.25", "0.25"),
            ("1e3", "1e3"),
        ] {
            assert_eq!(number_key(a), b, "{a}");
        }
        let c = chunk_row(Chunking::Tokens(64), "one two three", Some(" T "), 4);
        assert_eq!(c, vec![("T".to_owned(), "one two three".to_owned())]);
    }
}
