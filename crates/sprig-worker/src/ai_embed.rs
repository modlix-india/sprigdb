//! `ai_embed` (embedded columns): one long-running job per embedded
//! column, submitted by the controller's `ai_embed` loop for the gateway's
//! feed subscription `emb:e<id>`, filling the column from the change feed
//! and backfilling the rows that were there before.
//!
//! ```text
//! serve(tenant, emb:e<id>):
//!   the catalog row (ai_embedded_columns); its table (objects); the local
//!   model (loaded once per process); gone → remove the row and the
//!   subscription, end
//!   tail the tenant's feed from the subscription's checkpoint, through the
//!   worker's shared reader
//!   loop:
//!     a round: the next feed item (up to 200 ms), then what is already read,
//!       up to ROUND items
//!     the round's inserts and updates of the table, by shard; an update
//!       that changed only the embedded column is this job's own write
//!       and is passed over; a DDL of the table reloads the column
//!     reconcile(keys) per shard (below), retried with backoff until done;
//!       the checkpoint moves after the round, at most every second
//!     backfill, between rounds: the next keyset chunk of one shard
//!       (ai.embedding.backfill_rows_per_s, at most 500 rows), reconciled
//!       the same way; its position is kept in the row (`progress`), so a
//!       new owner carries on; every shard done → ready
//!     reembedding: the shadow generation is backfilled the same way, then
//!       the cutover (every shard fresh, then one ALTER per shard renames the
//!       shadow into place) and the row's generation moves on
//!     every 5 s: the row again (a model change, a drop)
//!
//!     the stale-hash check, between rounds once ready: one keyset window
//!       of one shard (a tenth of ai.embedding.backfill_rows_per_s, at most
//!       500 rows; a minute's rest after each pass over every shard): the
//!       rows whose stored hash isn't their source's, reconciled
//!
//! reconcile(shard, keys or a keyset chunk):
//!   SELECT pk, src, UNHEX(MD5(src)), hash per generation filled; for a
//!     feed round's keys a causal read (`causal`): after a wsrep_sync_wait
//!     barrier on Galera, else LOCK IN SHARE MODE
//!   a row whose hash is its source's: nothing to do
//!   no source (NULL or blank): vector NULL, hash := the source's
//!   else: embedded in batches of ai.embedding.batch_rows with the model,
//!     each source cut to ai.embedding.max_input_bytes and the column's
//!     token cap
//!   one transaction per batch: UPDATE ... SET vec, hash WHERE pk AND
//!     UNHEX(MD5(src)) <=> the hash embedded (the guard); a row that
//!     changed meanwhile is left to its own change's event
//! ```
//!
//! **Exactly once.** A column's state is a function of its row: the vector
//! of the current source, marked by the source's hash. The checkpoint
//! moves only after a round's writes committed, so a job killed mid-round
//! reads the round again, and the rows it had written already match their
//! hash and are passed over; the guard keeps a slow write from replacing a
//! newer source's vector. No change is lost (the feed is read from the
//! last checkpoint) and no row is embedded twice for the same source.
//! A round's rows are read causally ([`causal`]): a read that returned a
//! version older than the round's events would take the row as done and
//! acknowledge past its change. The stale-hash check is the safety net
//! behind that: a row left stale by any miss is found within a pass and
//! embedded.
//!
//! **A graceful stop drains** (`sprig_feed::drain`, as the other feed
//! consumers): the round in flight is finished, and the feed is read and
//! handled on (no backfill steps) until its position has passed the
//! cluster's (read again once passed, until nothing more was written), at
//! most `STOP_DRAIN` after the stop; then the position is acknowledged, so
//! a binary log purge right after the stop takes nothing the restart
//! needs. The vector updates of a tenant on Sprig 2PC's relay path go
//! through the cluster's relay writer (`write_pool_of`).

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable as _;
use mysql_async::{Pool, TxOpts};
use serde_json::{Value, json};
use sprig_ai::embedding::store::{EmbeddedColumn, State, Store};
use sprig_ai::embedding::{self, metrics};
use sprig_catalog::CatalogLocator as _;
use sprig_catalog::pool_wait::Bounded as _;
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::dispatch::JobClass;
use sprig_feed::drain::{STOP_DRAIN, StopDrain};
use sprig_feed::names::CatalogResolver;
use sprig_feed::pump::Pump;
use sprig_feed::subs::Subscriptions;
use sprig_feed::{
    ChangeEvent, FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions, TenantTail,
};
use sprig_sql::embedcol;

use crate::logic::LogicEnv;
use crate::runtime::{JobCtx, JobHandler};

pub(crate) mod causal;

/// The job kind (the controller's `ai_embed_loop::JOB_KIND`).
pub const JOB_KIND: &str = "ai_embed";

/// Feed items in one round at most.
const ROUND: usize = 2048;

/// Rows of one backfill chunk at most.
const CHUNK: u64 = 500;

/// The stale-hash check reads this share of
/// `ai.embedding.backfill_rows_per_s` (one in ten).
const SCAN_SHARE: u64 = 10;

/// The stale-hash check's rest after each pass over every shard (and
/// before the first, after the job starts).
pub(crate) const SCAN_REST: Duration = Duration::from_secs(60);

/// The job's environment: the logic jobs' (catalogs, shard pools, the
/// shared reader), the shard maps, and where the local model is.
#[derive(Clone)]
pub struct EmbedEnv {
    /// Catalogs, `sprig_ctl` pools, the feed, live settings.
    pub logic: LogicEnv,
    /// This DC's shard maps.
    pub maps: sprig_shard::ShardMapCache,
}

impl std::fmt::Debug for EmbedEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbedEnv").finish_non_exhaustive()
    }
}

impl EmbedEnv {
    /// `docs.model_dir`, else the default next to the binary.
    pub(crate) fn model_dir(&self) -> PathBuf {
        let set = self.logic.settings.get(&sprig_docs::settings::MODEL_DIR);
        if set.trim().is_empty() {
            sprig_docs::settings::next_to_binary(sprig_docs::settings::DEFAULT_MODEL_DIR)
                .unwrap_or_else(|| PathBuf::from(sprig_docs::settings::DEFAULT_MODEL_DIR))
        } else {
            PathBuf::from(set.trim())
        }
    }
}

/// `ai_embed` (see the module docs).
#[derive(Debug, Clone)]
pub struct AiEmbed(pub EmbedEnv);

#[async_trait::async_trait]
impl JobHandler for AiEmbed {
    fn kind(&self) -> &'static str {
        JOB_KIND
    }

    fn drains(&self) -> bool {
        true
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let tenant = TenantId(cx.spec["tenant"].as_u64().ok_or("spec.tenant is missing")?);
        let name = cx.spec["name"]
            .as_str()
            .ok_or("spec.name is missing")?
            .to_owned();
        Box::pin(serve(&self.0, cx, tenant, &name))
            .await
            .map_err(|e| e.to_string())
    }
}

pub(crate) fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

pub(crate) fn backend(e: mysql_async::Error) -> SprigError {
    let retry = match &e {
        // A moved shard (its tables renamed away at a cutover), a lock wait,
        // a deadlock: transient.
        mysql_async::Error::Server(s) => matches!(s.code, 1146 | 1205 | 1213 | 1047 | 2013),
        _ => true,
    };
    let mut x = SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"));
    x.retryable = retry;
    x.with_source(e)
}

// ---- the parts the loop is made of -------------------------------------------

/// What a row needs for one generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Its hash is its source's.
    Skip,
    /// No source: the vector becomes `NULL`.
    Clear,
    /// Embed this text.
    Embed(String),
}

/// What a row needs, from its source, the source's hash and the stored
/// hash (all as MariaDB returned them).
#[must_use]
pub fn plan_row(src: Option<&[u8]>, src_hash: Option<&[u8]>, stored: Option<&[u8]>) -> Action {
    if src_hash == stored {
        return Action::Skip;
    }
    match src {
        Some(s) if !s.iter().all(u8::is_ascii_whitespace) => {
            Action::Embed(String::from_utf8_lossy(s).into_owned())
        }
        _ => Action::Clear,
    }
}

/// `text` cut to at most `max` bytes, at a character boundary.
#[must_use]
pub fn cut(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Whether feed event `e` asks for the column to be looked at again: an
/// insert, or an update that changed something besides `column` itself
/// (the job's own writes change only the column and its hidden ones,
/// which the feed doesn't show).
#[must_use]
pub fn wants(e: &ChangeEvent, column: &str) -> bool {
    wants_op(e.op, e.delta.as_deref(), column)
}

/// [`wants`] over an event's operation and changed columns.
#[must_use]
pub fn wants_op(op: Op, delta: Option<&[String]>, column: &str) -> bool {
    match op {
        Op::Insert => true,
        Op::Update => delta.is_none_or(|d| d.iter().any(|c| !c.eq_ignore_ascii_case(column))),
        _ => false,
    }
}

/// A token bucket: `rate` per second, at most a second's worth saved.
#[derive(Debug)]
pub struct Throttle {
    rate: f64,
    tokens: f64,
    at: Instant,
}

impl Throttle {
    /// Full, at `rate` per second.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn new(rate: u64) -> Self {
        let rate = rate.max(1) as f64;
        Self {
            rate,
            tokens: rate,
            at: Instant::now(),
        }
    }

    /// A new rate, keeping what is saved.
    #[allow(clippy::cast_precision_loss)]
    pub fn set_rate(&mut self, rate: u64) {
        self.rate = rate.max(1) as f64;
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.saturating_duration_since(self.at).as_secs_f64();
        self.tokens = (self.tokens + dt * self.rate).min(self.rate);
        self.at = now;
    }

    /// How many of `want` may go now (at least one when anything is saved).
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn take(&mut self, want: u64, now: Instant) -> u64 {
        self.refill(now);
        if self.tokens < 1.0 {
            return 0;
        }
        let n = (self.tokens.floor() as u64).min(want);
        self.tokens -= n as f64;
        n
    }

    /// Spends `n` (which may go below zero: a round's changes aren't
    /// held back, the next ones wait).
    #[allow(clippy::cast_precision_loss)]
    pub fn spend(&mut self, n: u64, now: Instant) {
        self.refill(now);
        self.tokens -= n as f64;
    }

    /// How long until one is saved.
    #[must_use]
    pub fn wait(&self) -> Duration {
        if self.tokens >= 1.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64((1.0 - self.tokens) / self.rate)
        }
    }
}

/// Where the stale-hash check is: a keyset walk over the shards, one
/// window at a time, throttled to [`SCAN_SHARE`] of the backfill's rate,
/// resting [`SCAN_REST`] between passes.
#[derive(Debug)]
pub(crate) struct Scan {
    /// The shard (its index in the job's sorted shard list) being walked.
    pub(crate) shard: usize,
    /// The last key read in it, as SQL literals.
    pub(crate) after: Option<Vec<String>>,
    /// Not before then.
    next: Instant,
    throttle: Throttle,
    /// Rows brought up to date in this pass.
    pub(crate) repaired: u64,
}

impl Scan {
    /// A check that starts [`SCAN_REST`] after `now`, at a share of
    /// `backfill_rate`.
    pub(crate) fn new(backfill_rate: u64, now: Instant) -> Self {
        Self {
            shard: 0,
            after: None,
            next: now + SCAN_REST,
            throttle: Throttle::new(scan_rate(backfill_rate)),
            repaired: 0,
        }
    }

    /// The backfill's rate changed.
    pub(crate) fn set_rate(&mut self, backfill_rate: u64) {
        self.throttle.set_rate(scan_rate(backfill_rate));
    }

    /// Rows the next window may read now, at most `max`: 0 while resting
    /// or throttled.
    pub(crate) fn take(&mut self, max: u64, now: Instant) -> u64 {
        if now < self.next {
            return 0;
        }
        self.throttle.take(max, now)
    }

    /// A window read `read` rows of the `asked`, the last key `last`: the
    /// walk goes on after it, or to the next shard at the end of this one.
    pub(crate) fn advance(&mut self, last: Option<Vec<String>>, read: u64, asked: u64) {
        if read >= asked && last.is_some() {
            self.after = last;
        } else {
            self.shard += 1;
            self.after = None;
        }
    }

    /// Every shard walked: back to the first after the rest; the rows
    /// this pass repaired.
    pub(crate) fn pass_done(&mut self, now: Instant) -> u64 {
        self.shard = 0;
        self.after = None;
        self.next = now + SCAN_REST;
        std::mem::take(&mut self.repaired)
    }
}

/// The stale-hash check's rows per second for a backfill rate.
#[must_use]
pub(crate) fn scan_rate(backfill_rate: u64) -> u64 {
    (backfill_rate / SCAN_SHARE).max(1)
}

/// Whether `v` (a column of a row read) is a true flag (`1`, as either
/// protocol returns it).
pub(crate) fn flag(v: Option<&mysql_async::Value>) -> bool {
    match v {
        Some(mysql_async::Value::Int(n)) => *n != 0,
        Some(mysql_async::Value::UInt(n)) => *n != 0,
        Some(mysql_async::Value::Bytes(b)) => b.as_slice() != b"0" && !b.is_empty(),
        _ => false,
    }
}

// ---- one column -----------------------------------------------------------------

/// One row's work for one generation: the target's index, the row's key,
/// what to do, and the source's hash.
type Work = (usize, Vec<mysql_async::Value>, Action, Option<Vec<u8>>);

/// The generation a reconcile fills: its vector, source and hash columns.
#[derive(Debug, Clone)]
struct Target {
    vec: String,
    src: String,
    hash: String,
    max_tokens: usize,
}

/// Where one shard of the table is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ShardLoc {
    group: u32,
    shard: u32,
}

/// The column the job keeps, as last read.
struct Column {
    row: EmbeddedColumn,
    db: String,
    table: String,
    targets: Vec<Target>,
}

impl Column {
    fn new(row: EmbeddedColumn, db: String, table: String) -> Self {
        let tokens = |n: Option<u32>| {
            n.map_or(sprig_embed::MAX_TOKENS, |n| {
                usize::try_from(n).unwrap_or(sprig_embed::MAX_TOKENS)
            })
        };
        let mut targets = vec![Target {
            vec: row.column.clone(),
            src: embedcol::src_column(&row.column, None),
            hash: embedcol::hash_column(&row.column, None),
            max_tokens: tokens(row.max_input_tokens),
        }];
        if let Some(p) = &row.pending {
            targets.push(Target {
                vec: embedcol::vector_column(&row.column, Some(p.generation)),
                src: embedcol::src_column(&row.column, p.new_source.then_some(p.generation)),
                hash: embedcol::hash_column(&row.column, Some(p.generation)),
                max_tokens: tokens(p.max_input_tokens),
            });
        }
        Self {
            row,
            db,
            table,
            targets,
        }
    }

    fn schema(&self, tenant: TenantId, s: &ShardLoc) -> String {
        SchemaName::shard(
            tenant,
            DbId(self.row.db_id),
            GroupId(s.group),
            ShardId(s.shard),
        )
        .to_string()
    }

    fn table_ref(&self, tenant: TenantId, s: &ShardLoc) -> String {
        format!("{}.{}", quote(&self.schema(tenant, s)), quote(&self.table))
    }
}

/// The job's state between rounds.
struct Job<'a> {
    env: &'a EmbedEnv,
    tenant: TenantId,
    col: Column,
    model: Arc<sprig_embed::Embedder>,
    /// Primary key columns per shard schema (they are the same on every
    /// shard; read once).
    pk: Option<Vec<String>>,
    batch: usize,
    max_bytes: usize,
    live: Throttle,
    backfill: Throttle,
    /// The backfill's position per shard schema: the last key, as SQL
    /// literals, or `done`.
    progress: BTreeMap<String, Value>,
    progress_dirty: u64,
    shards: Option<(Instant, Vec<(ShardLoc, ClusterId)>)>,
    embedded: u64,
    /// How each cluster is read causally.
    kinds: causal::Kinds,
    /// The stale-hash check.
    scan: Scan,
}

impl Job<'_> {
    fn model_label(&self) -> &str {
        &self.col.row.model.alias
    }

    async fn catalog(&self) -> Result<(mysql_async::Conn, Store, String)> {
        let ep = self.env.logic.locator.writer(self.tenant).await?;
        let conn = ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let schema = ep.store.schema().to_owned();
        Ok((conn, Store::new(&schema), schema))
    }

    /// The cluster holding shard `s` now.
    async fn cluster_of(&self, s: &ShardLoc) -> Result<ClusterId> {
        if let Ok(m) = self.env.maps.get(self.tenant, GroupId(s.group))
            && let Some(r) = m.shard(ShardId(s.shard))
        {
            return Ok(r.cluster.clone());
        }
        // A group without a map of its own sits with the catalog.
        self.env
            .logic
            .locator
            .cluster_of(self.tenant)
            .await?
            .ok_or_else(|| {
                let mut e = SprigError::new(
                    SprigCode::TenantNotReady,
                    format!("tenant {} has no shard map here", self.tenant),
                );
                e.retryable = true;
                e
            })
    }

    async fn pool_of(&self, s: &ShardLoc) -> Result<Pool> {
        let c = self.cluster_of(s).await?;
        self.env.logic.pools.writer(&c).await
    }

    /// The pool the vector `UPDATE`s of shard `s` take connections from:
    /// [`Self::pool_of`], or, for a tenant on the relay path, one through
    /// the cluster's relay writer, chosen again at each use. Its own
    /// transactions write the same rows, so a held branch can hold them;
    /// through the relay, the fence ends this connection with the tenant's
    /// others when a holder is lost. Reads of the catalog and
    /// `information_schema`, and the cutover's `ALTER` (DDL), stay direct.
    ///
    /// # Errors
    /// As [`Self::pool_of`]; a retryable 39030 while the cluster has relays
    /// but none is ready.
    async fn write_pool_of(&self, s: &ShardLoc) -> Result<Pool> {
        let c = self.cluster_of(s).await?;
        let direct = self.env.logic.pools.writer(&c).await?;
        crate::relay_pools::shard_pool(&self.env.logic.dc, self.tenant, &c, direct).await
    }

    /// The table's shards: every `t<tenant>_d<db>_g*_s*` schema holding it,
    /// on the cluster its map puts it on (a mover's copy elsewhere is left
    /// alone). Read again every minute.
    async fn shards(&mut self) -> Result<Vec<(ShardLoc, ClusterId)>> {
        if let Some((at, s)) = &self.shards
            && at.elapsed() < Duration::from_secs(60)
        {
            return Ok(s.clone());
        }
        let prefix = format!("t{}_d{}_g", self.tenant.0, self.col.row.db_id);
        let mut out = Vec::new();
        for cluster in self.env.logic.pools.sweepable().await? {
            let pool = self.env.logic.pools.writer(&cluster).await?;
            let mut c = pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
            })?;
            let schemas: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA LIKE CONCAT(REPLACE(?, '_', '\\_'), '%') AND TABLE_NAME = ?",
                    (prefix.as_str(), self.col.table.as_str()),
                )
                .await
                .map_err(backend)?;
            for s in schemas {
                let Ok(SchemaName::Shard {
                    tenant,
                    db,
                    group,
                    shard,
                }) = s.parse::<SchemaName>()
                else {
                    continue;
                };
                if tenant != self.tenant || db.0 != self.col.row.db_id {
                    continue;
                }
                let loc = ShardLoc {
                    group: group.0,
                    shard: shard.0,
                };
                if self.cluster_of(&loc).await.is_ok_and(|c| c == cluster) {
                    out.push((loc, cluster.clone()));
                }
            }
        }
        out.sort();
        self.shards = Some((Instant::now(), out.clone()));
        Ok(out)
    }

    /// The table's primary key columns (visible ones), read from a shard.
    async fn pk(&mut self, s: &ShardLoc) -> Result<Vec<String>> {
        if let Some(pk) = &self.pk {
            return Ok(pk.clone());
        }
        let pool = self.pool_of(s).await?;
        let mut c = pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
        })?;
        let cols: Vec<String> = c
            .exec(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX",
                (self.col.schema(self.tenant, s), self.col.table.as_str()),
            )
            .await
            .map_err(backend)?;
        let cols: Vec<String> = cols
            .into_iter()
            .filter(|c| !c.to_ascii_lowercase().starts_with("_sprig_"))
            .collect();
        if cols.is_empty() {
            return Err(embedding::failed(format!(
                "{}.{} has no primary key; an embedded column needs one",
                self.col.db, self.col.table
            )));
        }
        self.pk = Some(cols.clone());
        Ok(cols)
    }

    /// `SELECT pk, (src, md5, hash) per target FROM ... WHERE <pred>
    /// [ORDER BY pk LIMIT n]`.
    fn select_sql(&self, s: &ShardLoc, pk: &[String], pred: &str, order: Option<u64>) -> String {
        let mut cols: Vec<String> = pk.iter().map(|c| quote(c)).collect();
        for t in &self.col.targets {
            cols.push(quote(&t.src));
            cols.push(format!("UNHEX(MD5({}))", quote(&t.src)));
            cols.push(quote(&t.hash));
        }
        let mut sql = format!(
            "SELECT {} FROM {} WHERE {pred}",
            cols.join(", "),
            self.col.table_ref(self.tenant, s)
        );
        if let Some(n) = order {
            let keys: Vec<String> = pk.iter().map(|c| quote(c)).collect();
            let _ = write!(sql, " ORDER BY {} LIMIT {n}", keys.join(", "));
        }
        sql
    }

    /// Brings the rows `sql` selects up to date (see the module docs),
    /// reading them causally when `causal` (a feed round's keys); the last
    /// row's key as SQL literals, the number of rows read and the number
    /// written (embedded or cleared).
    #[allow(clippy::too_many_lines)] // the reconcile of the module docs
    async fn reconcile(
        &mut self,
        s: &ShardLoc,
        sql: &str,
        causal: bool,
        cancel: &sprig_common::CancellationToken,
    ) -> Result<(Option<Vec<String>>, u64, u64)> {
        let pk = self.pk(s).await?;
        let pool = self.write_pool_of(s).await?;
        let mut c = pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
        })?;
        let sql = if causal {
            let cluster = self.cluster_of(s).await?;
            let reads = self.kinds.of(&self.env.logic.registry, &cluster).await?;
            causal::read_sql(reads, &mut c, sql).await?
        } else {
            sql.to_owned()
        };
        let rows: Vec<mysql_async::Row> = c.query(sql).await.map_err(backend)?;
        let read = rows.len() as u64;
        let last = rows.last().map(|r| {
            (0..pk.len())
                .map(|i| {
                    r.as_ref(i)
                        .map_or_else(|| "NULL".to_owned(), |v| v.as_sql(false))
                })
                .collect::<Vec<_>>()
        });
        // (target, key values, action, source hash)
        let mut work: Vec<Work> = Vec::new();
        let (mut skipped, mut cleared) = (0u64, 0u64);
        for r in &rows {
            let key: Vec<mysql_async::Value> = (0..pk.len())
                .map(|i| r.as_ref(i).cloned().unwrap_or(mysql_async::Value::NULL))
                .collect();
            for (ti, _) in self.col.targets.iter().enumerate() {
                let base = pk.len() + ti * 3;
                let bytes = |i: usize| -> Option<Vec<u8>> {
                    match r.as_ref(i) {
                        Some(mysql_async::Value::Bytes(b)) => Some(b.clone()),
                        _ => None,
                    }
                };
                let (src, h, stored) = (bytes(base), bytes(base + 1), bytes(base + 2));
                match plan_row(src.as_deref(), h.as_deref(), stored.as_deref()) {
                    Action::Skip => skipped += 1,
                    a => work.push((ti, key.clone(), a, h)),
                }
            }
        }
        metrics::rows(self.model_label(), "skipped", skipped);
        if work.is_empty() {
            return Ok((last, read, 0));
        }
        // Embed in batches, each written in one transaction.
        let where_pk: Vec<String> = pk.iter().map(|c| format!("{} = ?", quote(c))).collect();
        let where_pk = where_pk.join(" AND ");
        let table = self.col.table_ref(self.tenant, s);
        let mut superseded = 0u64;
        let mut embedded = 0u64;
        for chunk in work.chunks(self.batch.max(1)) {
            if cancel.is_cancelled() {
                return Err(SprigError::internal("cancelled"));
            }
            // The texts of this batch, per target's token cap.
            let mut vectors: Vec<Option<Vec<f32>>> = vec![None; chunk.len()];
            let mut by_cap: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (i, (ti, _, a, _)) in chunk.iter().enumerate() {
                if matches!(a, Action::Embed(_)) {
                    by_cap
                        .entry(self.col.targets[*ti].max_tokens)
                        .or_default()
                        .push(i);
                }
            }
            for (cap, idx) in by_cap {
                let texts: Vec<String> = idx
                    .iter()
                    .map(|&i| match &chunk[i].2 {
                        Action::Embed(t) => cut(t, self.max_bytes).to_owned(),
                        _ => String::new(),
                    })
                    .collect();
                let model = Arc::clone(&self.model);
                let started = Instant::now();
                let out = tokio::task::spawn_blocking(move || {
                    let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
                    model.embed_passages_truncated(&refs, cap)
                })
                .await
                .map_err(|e| SprigError::internal(format!("the embedding task stopped: {e}")))?
                .map_err(|e| embedding::failed(format!("the model failed: {e}")))?;
                metrics::seconds(self.model_label(), started.elapsed().as_secs_f64());
                for (&i, v) in idx.iter().zip(out) {
                    vectors[i] = Some(v.to_vec());
                }
            }
            let mut tx = c
                .start_transaction(TxOpts::default())
                .await
                .map_err(backend)?;
            for ((ti, key, action, h), v) in chunk.iter().zip(&vectors) {
                let t = &self.col.targets[*ti];
                let mut params: Vec<mysql_async::Value> = Vec::with_capacity(key.len() + 3);
                let set = match (action, v) {
                    (Action::Embed(_), Some(v)) => {
                        params.push(vec_text(v).into());
                        format!("{} = VEC_FromText(?)", quote(&t.vec))
                    }
                    _ => format!("{} = NULL", quote(&t.vec)),
                };
                params.push(h.clone().into());
                params.extend(key.iter().cloned());
                params.push(h.clone().into());
                let stmt = format!(
                    "UPDATE {table} SET {set}, {} = ? WHERE {where_pk} AND UNHEX(MD5({})) <=> ?",
                    quote(&t.hash),
                    quote(&t.src)
                );
                tx.exec_drop(stmt, mysql_async::Params::Positional(params))
                    .await
                    .map_err(backend)?;
                if tx.affected_rows() == 0 {
                    superseded += 1;
                } else if matches!(action, Action::Embed(_)) {
                    embedded += 1;
                } else {
                    cleared += 1;
                }
            }
            tx.commit().await.map_err(backend)?;
        }
        let model = self.model_label().to_owned();
        metrics::rows(&model, "embedded", embedded);
        metrics::rows(&model, "cleared", cleared);
        metrics::rows(&model, "superseded", superseded);
        self.embedded += embedded;
        self.progress_dirty += embedded;
        Ok((last, read, embedded + cleared))
    }

    /// The rows of `keys` (feed keys of one shard) brought up to date.
    async fn reconcile_keys(
        &mut self,
        s: &ShardLoc,
        keys: &[Vec<(String, sprig_binlog::Value)>],
        cancel: &sprig_common::CancellationToken,
    ) -> Result<()> {
        let pk = self.pk(s).await?;
        for part in keys.chunks(500) {
            let tuples: Vec<String> = part
                .iter()
                .filter_map(|k| {
                    let vals: Option<Vec<String>> = pk
                        .iter()
                        .map(|c| {
                            k.iter()
                                .find(|(n, _)| n.eq_ignore_ascii_case(c))
                                .map(|(_, v)| sprig_logic::triggers::literal(v))
                        })
                        .collect();
                    vals.map(|v| format!("({})", v.join(", ")))
                })
                .collect();
            if tuples.is_empty() {
                continue;
            }
            let cols: Vec<String> = pk.iter().map(|c| quote(c)).collect();
            let pred = format!("({}) IN ({})", cols.join(", "), tuples.join(", "));
            let sql = self.select_sql(s, &pk, &pred, None);
            self.reconcile(s, &sql, true, cancel).await?;
        }
        Ok(())
    }

    /// One window of the stale-hash check (see the module docs): the next
    /// rows of the shard it is walking, read plainly; those whose stored
    /// hash isn't their source's are reconciled. A pass over every shard
    /// ends with a log line when it repaired rows.
    async fn scan_step(&mut self, cancel: &sprig_common::CancellationToken) -> Result<()> {
        let now = Instant::now();
        let n = self.scan.take(CHUNK, now);
        if n == 0 {
            return Ok(());
        }
        let shards = self.shards().await?;
        let Some((s, _)) = shards.get(self.scan.shard).cloned() else {
            let repaired = self.scan.pass_done(now);
            if repaired > 0 {
                tracing::warn!(tenant = %self.tenant, embedding = self.col.row.id, db = %self.col.db, table = %self.col.table, column = %self.col.row.column, repaired, "embedded column: the stale-hash check brought rows up to date that their changes had missed");
            }
            return Ok(());
        };
        let pk = self.pk(&s).await?;
        let sql = scan_sql(
            &self.col.table_ref(self.tenant, &s),
            &pk,
            &self.col.targets,
            self.scan.after.as_deref(),
            n,
        );
        let rows: Vec<mysql_async::Row> = {
            let pool = self.pool_of(&s).await?;
            let mut c = pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
            })?;
            c.query(sql).await.map_err(backend)?
        };
        let read = rows.len() as u64;
        let key = |r: &mysql_async::Row| -> Vec<String> {
            (0..pk.len())
                .map(|i| {
                    r.as_ref(i)
                        .map_or_else(|| "NULL".to_owned(), |v| v.as_sql(false))
                })
                .collect()
        };
        let last = rows.last().map(key);
        let stale: Vec<String> = rows
            .iter()
            .filter(|r| flag(r.as_ref(pk.len())))
            .map(|r| format!("({})", key(r).join(", ")))
            .collect();
        if !stale.is_empty() {
            let cols: Vec<String> = pk.iter().map(|c| quote(c)).collect();
            let pred = format!("({}) IN ({})", cols.join(", "), stale.join(", "));
            let sql = self.select_sql(&s, &pk, &pred, None);
            let (_, _, written) = self.reconcile(&s, &sql, false, cancel).await?;
            self.scan.repaired += written;
            self.live.spend(stale.len() as u64, now);
        }
        self.scan.advance(last, read, n);
        Ok(())
    }

    /// One backfill chunk of the first shard not done; whether every shard
    /// is done.
    async fn backfill_step(&mut self, cancel: &sprig_common::CancellationToken) -> Result<bool> {
        let shards = self.shards().await?;
        let Some((s, _)) = shards
            .iter()
            .find(|(s, _)| {
                self.progress.get(&self.col.schema(self.tenant, s)) != Some(&json!("done"))
            })
            .cloned()
        else {
            return Ok(true);
        };
        let n = self.backfill.take(CHUNK, Instant::now());
        if n == 0 {
            return Ok(false);
        }
        let pk = self.pk(&s).await?;
        let schema = self.col.schema(self.tenant, &s);
        let pred = match self.progress.get(&schema).and_then(Value::as_array) {
            Some(after) if after.len() == pk.len() => {
                let cols: Vec<String> = pk.iter().map(|c| quote(c)).collect();
                let vals: Vec<String> = after
                    .iter()
                    .map(|v| v.as_str().unwrap_or("NULL").to_owned())
                    .collect();
                format!("({}) > ({})", cols.join(", "), vals.join(", "))
            }
            _ => "TRUE".to_owned(),
        };
        let sql = self.select_sql(&s, &pk, &pred, Some(n));
        let (last, read, _) = self.reconcile(&s, &sql, false, cancel).await?;
        self.live.spend(read, Instant::now());
        let pos = match last {
            Some(l) if read >= n => json!(l),
            _ => json!("done"),
        };
        self.progress.insert(schema, pos);
        self.save_progress().await?;
        Ok(false)
    }

    async fn save_progress(&mut self) -> Result<()> {
        let (mut c, store, _) = self.catalog().await?;
        let p = Value::Object(self.progress.clone().into_iter().collect());
        store
            .set_progress(&mut c, self.col.row.id, &p, self.progress_dirty)
            .await?;
        self.progress_dirty = 0;
        Ok(())
    }

    /// The re-embedding's cutover: every shard's shadow fresh, then each
    /// shard's columns renamed, then the row. `false` when a shard isn't
    /// fresh yet (its backfill starts again).
    async fn cutover(&mut self) -> Result<bool> {
        let Some(p) = self.col.row.pending.clone() else {
            return Ok(true);
        };
        let shadow =
            self.col.targets.get(1).cloned().ok_or_else(|| {
                SprigError::internal("a re-embedding without its shadow generation")
            })?;
        let shards = self.shards().await?;
        let column = self.col.row.column.clone();
        // Fresh everywhere first.
        for (s, _) in &shards {
            if !self.has_column(s, &shadow.vec).await? {
                continue;
            }
            let pool = self.pool_of(s).await?;
            let mut c = pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
            })?;
            let stale: Option<u64> = c
                .query_first(format!(
                    "SELECT COUNT(*) FROM {} WHERE NOT ({} <=> UNHEX(MD5({})))",
                    self.col.table_ref(self.tenant, s),
                    quote(&shadow.hash),
                    quote(&shadow.src)
                ))
                .await
                .map_err(backend)?;
            if stale.unwrap_or(0) > 0 {
                self.progress.remove(&self.col.schema(self.tenant, s));
                self.save_progress().await?;
                return Ok(false);
            }
        }
        let meta = embedcol::Meta {
            column: column.clone(),
            model: p.model.alias.clone(),
            generation: p.generation,
            mode: embedcol::Mode::parse(&self.col.row.mode).unwrap_or(embedcol::Mode::Async),
            max_input_tokens: p.max_input_tokens,
            shadow: false,
        };
        for (s, _) in &shards {
            if !self.has_column(s, &shadow.vec).await? {
                // Cut over before a restart.
                continue;
            }
            let pool = self.pool_of(s).await?;
            let mut c = pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
            })?;
            let names: Vec<String> = c
                .exec(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                     AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                    (self.col.schema(self.tenant, s), self.col.table.as_str()),
                )
                .await
                .map_err(backend)?;
            let after = names
                .iter()
                .position(|n| n.eq_ignore_ascii_case(&column))
                .and_then(|i| i.checked_sub(1))
                .map(|i| names[i].clone());
            let sql = embedcol::cutover_sql(
                &self.col.table_ref(self.tenant, s),
                &column,
                p.generation,
                p.model.dimensions,
                after.as_deref(),
                &p.source,
                p.new_source,
                &meta,
            );
            if let Err(e) = c.query_drop(&sql).await {
                metrics::cutovers("failed");
                return Err(backend(e));
            }
        }
        let (mut c, store, _) = self.catalog().await?;
        store
            .finish_reembedding(&mut c, self.col.row.id, &p)
            .await?;
        metrics::cutovers("done");
        tracing::info!(tenant = %self.tenant, db = %self.col.db, table = %self.col.table, %column, generation = p.generation, model = %p.model.alias, "embedded column re-embedded: cutover done");
        Ok(true)
    }

    async fn has_column(&self, s: &ShardLoc, column: &str) -> Result<bool> {
        let pool = self.pool_of(s).await?;
        let mut c = pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::BackendUnavailable, format!("embedding: {e}"))
        })?;
        let n: Option<u64> = c
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (
                    self.col.schema(self.tenant, s),
                    self.col.table.as_str(),
                    column,
                ),
            )
            .await
            .map_err(backend)?;
        Ok(n.unwrap_or(0) > 0)
    }
}

/// One window of the stale-hash check over `table`: the next `n` rows
/// after key `after` (SQL literals; `None` from the start), in key order,
/// each row's key and whether a generation's stored hash isn't its
/// source's (1) or every one is (0). MariaDB hashes the sources; only keys
/// and flags come back.
fn scan_sql(
    table: &str,
    pk: &[String],
    targets: &[Target],
    after: Option<&[String]>,
    n: u64,
) -> String {
    let keys: Vec<String> = pk.iter().map(|c| quote(c)).collect();
    let keys = keys.join(", ");
    let stale: Vec<String> = targets
        .iter()
        .map(|t| format!("NOT ({} <=> UNHEX(MD5({})))", quote(&t.hash), quote(&t.src)))
        .collect();
    let pred = after.map_or_else(
        || "TRUE".to_owned(),
        |a| format!("({keys}) > ({})", a.join(", ")),
    );
    format!(
        "SELECT {keys}, ({}) FROM {table} WHERE {pred} ORDER BY {keys} LIMIT {n}",
        stale.join(" OR ")
    )
}

/// `[x,y,...]`, as `VEC_FromText` reads it.
fn vec_text(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 12);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&x.to_string());
    }
    s.push(']');
    s
}

/// The table of object `object`: (database, table), or `None` when it (or
/// its database) is gone.
async fn table_of(
    c: &mut mysql_async::Conn,
    schema: &str,
    object: u64,
) -> Result<Option<(String, String)>> {
    let r: Option<(String, String)> = c
        .exec_first(
            format!(
                "SELECT d.name, o.name FROM {s}.objects o JOIN {s}.`databases` d ON d.db_id = o.db_id \
                 WHERE o.object_id = ? AND o.state NOT IN ('dropping', 'dropped') \
                 AND d.state = 'active'",
                s = quote(schema)
            ),
            (object,),
        )
        .await
        .map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
    Ok(r)
}

/// The job's end when its column is gone: the row and the subscription go.
async fn forget(env: &EmbedEnv, tenant: TenantId, name: &str, id: u64) {
    if let Ok(ep) = env.logic.locator.writer(tenant).await
        && let Ok(mut c) = ep.pool.get_conn_bounded().await
    {
        let _ = Store::new(ep.store.schema()).delete(&mut c, id).await;
    }
    let _ = Subscriptions::new(Arc::clone(&env.logic.dc))
        .drop_subscription(tenant, name)
        .await;
}

/// The job's share of the process's backlog gauge.
fn backlog_set(job: &str, rows: u64) {
    use std::sync::{Mutex, OnceLock, PoisonError};
    static ALL: OnceLock<Mutex<BTreeMap<String, u64>>> = OnceLock::new();
    let m = ALL.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut m = m.lock().unwrap_or_else(PoisonError::into_inner);
    if rows == 0 {
        m.remove(job);
    } else {
        m.insert(job.to_owned(), rows);
    }
    metrics::backlog(m.values().sum());
}

/// Loads row `id` and its table: `Ok(None)` when either is gone.
async fn load(env: &EmbedEnv, tenant: TenantId, id: u64) -> Result<Option<Column>> {
    let ep = env.logic.locator.writer(tenant).await?;
    let mut c = ep.pool.get_conn_bounded().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    let schema = ep.store.schema().to_owned();
    let Some(row) = Store::new(&schema).get(&mut c, id).await? else {
        return Ok(None);
    };
    let Some((db, table)) = table_of(&mut c, &schema, row.object_id).await? else {
        return Ok(None);
    };
    Ok(Some(Column::new(row, db, table)))
}

/// Waits `d` unless cancelled first (`false`).
pub(crate) async fn pause(cancel: &sprig_common::CancellationToken, d: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => false,
        () = tokio::time::sleep(d) => true,
    }
}

#[allow(clippy::too_many_lines)] // the loop of the module docs
async fn serve(env: &EmbedEnv, cx: &JobCtx, tenant: TenantId, name: &str) -> Result<Value> {
    let subs = Subscriptions::new(Arc::clone(&env.logic.dc));
    let sub = match subs.get(tenant, name).await {
        Ok(s) => s,
        Err(e) if e.code() == Some(SprigCode::FeedSubscriptionNotFound) => {
            return Ok(json!({"stopped": "the subscription is gone"}));
        }
        Err(e) => return Err(e),
    };
    let owner: Value = serde_json::from_str(sub.owner_ref.as_deref().unwrap_or("{}"))
        .map_err(|e| SprigError::internal(format!("the subscription's owner_ref: {e}")))?;
    let id = owner["embedding"]
        .as_u64()
        .ok_or_else(|| SprigError::internal("owner_ref.embedding is missing"))?;
    let Some(col) = load(env, tenant, id).await? else {
        forget(env, tenant, name, id).await;
        return Ok(json!({"stopped": "the column or its table is gone"}));
    };
    // The model, loaded once per process; a model that doesn't load marks
    // the column failed and is tried again.
    let model = loop {
        if !col.row.model.is_local() {
            let msg = format!(
                "model '{}' is a provider's; embedded columns use local models for now",
                col.row.model.alias
            );
            let ep = env.logic.locator.writer(tenant).await?;
            if let Ok(mut c) = ep.pool.get_conn_bounded().await {
                let _ = Store::new(ep.store.schema())
                    .set_state(&mut c, id, State::Failed, Some(&msg))
                    .await;
            }
            if !pause(&cx.cancel, Duration::from_secs(60)).await {
                return Ok(json!({"stopped": "cancelled"}));
            }
            return Err(embedding::failed(msg));
        }
        let dir = env.model_dir();
        let loaded = tokio::task::spawn_blocking(move || sprig_embed::local::shared(&dir))
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r);
        match loaded {
            Ok(m) => break m,
            Err(why) => {
                let msg = format!("the local embedding model isn't available: {why}");
                tracing::warn!(%tenant, embedding = id, %msg, "embedded column waits for its model");
                let ep = env.logic.locator.writer(tenant).await?;
                if let Ok(mut c) = ep.pool.get_conn_bounded().await {
                    let _ = Store::new(ep.store.schema())
                        .set_state(&mut c, id, State::Failed, Some(&msg))
                        .await;
                }
                if !pause(&cx.cancel, Duration::from_secs(60)).await {
                    return Ok(json!({"stopped": "cancelled"}));
                }
            }
        }
    };
    let settings = &env.logic.settings;
    let progress: BTreeMap<String, Value> = col
        .row
        .progress
        .as_object()
        .map(|o| o.clone().into_iter().collect())
        .unwrap_or_default();
    let mut job = Job {
        env,
        tenant,
        model,
        pk: None,
        batch: usize::try_from(settings.get(&embedding::BATCH_ROWS)).unwrap_or(64),
        max_bytes: usize::try_from(settings.get(&embedding::MAX_INPUT_BYTES)).unwrap_or(65_536),
        live: Throttle::new(settings.tenant_get(tenant, &embedding::ROWS_PER_S)),
        backfill: Throttle::new(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S)),
        progress,
        progress_dirty: 0,
        shards: None,
        embedded: 0,
        kinds: causal::Kinds::default(),
        scan: Scan::new(
            settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S),
            Instant::now(),
        ),
        col,
    };
    // A failed column that loads again carries on.
    if job.col.row.state == State::Failed {
        let (mut c, store, _) = job.catalog().await?;
        let back = if job.col.row.pending.is_some() {
            State::Reembedding
        } else if job.progress.is_empty() && job.col.row.rows_embedded == 0 {
            State::Backfilling
        } else {
            State::Ready
        };
        store.set_state(&mut c, id, back, None).await?;
        job.col.row.state = back;
    }
    let start = match subs.checkpoint(tenant, name).await? {
        Some(cp) => StartAt::After(cp.resume_token()?),
        None => StartAt::Now,
    };
    let tail = TenantTail::start(
        tenant,
        start,
        Arc::clone(&env.logic.feed),
        Arc::new(MetaPlacement::new(Arc::clone(&env.logic.dc))),
        Arc::new(CatalogResolver::new(Arc::new(crate::logic::Catalogs(
            Arc::clone(&env.logic.locator),
        )))),
        TailOptions::default(),
    )
    .await?;
    let mut feed = Pump::start(tail, ROUND, "embedding worker");
    let job_key = format!("{tenant}/{name}");
    tracing::info!(%tenant, embedding = id, db = %job.col.db, table = %job.col.table, column = %job.col.row.column, state = job.col.row.state.as_str(), job = %cx.job_id, "embedding job started");
    let mut last_token = None;
    let mut pending_ack = 0u64;
    let mut acked = Instant::now();
    let mut checked = Instant::now();
    let mut reload = false;
    let mut attempt = 0u32;
    let object = job.col.row.object_id;
    // A graceful stop drains: the round in flight goes on, and the feed is
    // read and handled on, until `hard`, STOP_DRAIN after the cancel.
    let hard = sprig_common::CancellationToken::new();
    let _hard = hard.clone().drop_guard();
    {
        let (cancel, hard) = (cx.cancel.clone(), hard.clone());
        tokio::spawn(async move {
            tokio::select! {
                () = cancel.cancelled() => {}
                () = hard.cancelled() => return,
            }
            tokio::time::sleep(STOP_DRAIN).await;
            hard.cancel();
        });
    }
    let mut drain: Option<StopDrain> = None;
    // The position past every handled item (`last_token` is taken by acks).
    let mut at: Option<ResumeToken> = None;
    'serve: loop {
        if cx.cancel.is_cancelled() && drain.is_none() {
            let cluster = at.as_ref().map(|t| t.cluster.clone());
            drain =
                Some(StopDrain::begin(env.logic.feed.as_ref(), cluster.as_ref(), STOP_DRAIN).await);
        }
        if let Some(d) = drain.as_mut() {
            let finished = match &at {
                Some(t) => d.done(env.logic.feed.as_ref(), &t.cluster, &t.pos).await,
                None => true,
            };
            if finished || hard.is_cancelled() {
                tracing::debug!(%tenant, embedding = id, passed = d.reached(), "embedding job drained at its stop");
                break;
            }
        }
        // While draining, rounds stop at `hard`, not at the cancel.
        let stop = if drain.is_some() { &hard } else { &cx.cancel };
        // Every 5 s, or after a DDL of the table: the row again.
        if drain.is_none() && (reload || checked.elapsed() > Duration::from_secs(5)) {
            reload = false;
            checked = Instant::now();
            match load(env, tenant, id).await {
                Ok(None) => {
                    backlog_set(&job_key, 0);
                    forget(env, tenant, name, id).await;
                    tracing::info!(%tenant, embedding = id, "embedded column or its table is gone; job ends");
                    return Ok(
                        json!({"stopped": "the column or its table is gone", "embedded": job.embedded}),
                    );
                }
                Ok(Some(c)) => {
                    let new_gen = c.row.generation != job.col.row.generation
                        || c.row.pending != job.col.row.pending;
                    if new_gen {
                        job.progress = c
                            .row
                            .progress
                            .as_object()
                            .map(|o| o.clone().into_iter().collect())
                            .unwrap_or_default();
                    }
                    if c.table != job.col.table {
                        job.shards = None;
                    }
                    job.col = c;
                }
                Err(e) => {
                    tracing::warn!(%tenant, embedding = id, error = %e, "embedded column row unreadable");
                }
            }
            job.live
                .set_rate(settings.tenant_get(tenant, &embedding::ROWS_PER_S));
            job.backfill
                .set_rate(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S));
            job.scan
                .set_rate(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S));
            let _ = cx
                .progress(json!({"embedded": job.embedded, "state": job.col.row.state.as_str()}))
                .await;
        }
        if acked.elapsed() > Duration::from_secs(1)
            && let Some(t) = last_token.take()
        {
            subs.ack(tenant, name, &t, pending_ack, now_ms()).await?;
            pending_ack = 0;
            acked = Instant::now();
        }
        // A round of the feed.
        let backfilling = matches!(job.col.row.state, State::Backfilling | State::Reembedding);
        let wait = if backfilling && job.backfill.wait() == Duration::ZERO {
            Duration::from_millis(5)
        } else {
            Duration::from_millis(200)
        };
        let read = tokio::select! {
            biased;
            () = stop.cancelled() => None,
            r = tokio::time::timeout(wait, feed.next()) => Some(r),
        };
        let first = match read {
            // The stop: its drain starts (or ends) above.
            None => continue,
            Some(Ok(i)) => Some(i?),
            Some(Err(_)) => None,
        };
        let mut items = Vec::new();
        let mut ended = None;
        if let Some(i) = first {
            items.push(i);
            while items.len() < ROUND {
                match feed.try_next() {
                    Some(Ok(i)) => items.push(i),
                    Some(Err(e)) => {
                        ended = Some(e);
                        break;
                    }
                    None => break,
                }
            }
        }
        let mut keys: BTreeMap<ShardLoc, Vec<Vec<(String, sprig_binlog::Value)>>> = BTreeMap::new();
        let mut seen: HashSet<(ShardLoc, String)> = HashSet::new();
        for i in &items {
            let FeedItem::Event(e) = i else {
                continue;
            };
            if e.object != object {
                continue;
            }
            if matches!(e.op, Op::Ddl(_)) {
                reload = true;
                continue;
            }
            if !wants(e, &job.col.row.column) {
                continue;
            }
            let loc = ShardLoc {
                group: e.shard.group.0,
                shard: e.shard.shard.0,
            };
            let k = format!("{:?}", e.key.0);
            if seen.insert((loc.clone(), k)) {
                keys.entry(loc).or_default().push(e.key.0.clone());
            }
        }
        let n_keys: u64 = keys.values().map(|v| v.len() as u64).sum();
        backlog_set(&job_key, n_keys);
        // Reconcile the round's keys, retried until done.
        if n_keys > 0 {
            loop {
                let mut failed = None;
                for (s, ks) in &keys {
                    if let Err(e) = job.reconcile_keys(s, ks, stop).await {
                        failed = Some(e);
                        break;
                    }
                }
                let Some(e) = failed else {
                    attempt = 0;
                    break;
                };
                if stop.is_cancelled() {
                    // Out of time: this round stays after the checkpoint.
                    break 'serve;
                }
                attempt += 1;
                if e.code() == Some(SprigCode::EmbeddingFailed) || (!e.retryable && attempt > 5) {
                    let (mut c, store, _) = job.catalog().await?;
                    let _ = store
                        .set_state(&mut c, id, State::Failed, Some(&e.to_string()))
                        .await;
                    tracing::warn!(%tenant, embedding = id, error = %e, "embedded column failed; retrying in 30 s");
                    if !pause(stop, Duration::from_secs(30)).await {
                        break 'serve;
                    }
                    return Err(e);
                }
                job.shards = None;
                tracing::warn!(%tenant, embedding = id, error = %e, attempt, "embedding round failed; retrying");
                let wait = Duration::from_millis(200 * u64::from(attempt.min(25)));
                if !pause(stop, wait).await {
                    break 'serve;
                }
            }
            job.live.spend(n_keys, Instant::now());
            if job.progress_dirty > 0 {
                let _ = job.save_progress().await;
            }
            // The live rate: the next round waits for the bucket.
            let w = job.live.wait();
            if w > Duration::ZERO {
                // A stop cuts the wait short; the round's token is kept.
                let _ = pause(stop, w.min(Duration::from_secs(5))).await;
            }
        }
        if let Some(t) = items.last().map(|i| i.token().clone()) {
            pending_ack += n_keys;
            at = Some(t.clone());
            last_token = Some(t);
        }
        if let Some(e) = ended {
            if let Some(t) = last_token.take() {
                let _ = subs.ack(tenant, name, &t, pending_ack, now_ms()).await;
            }
            return Err(e);
        }
        // Backfill between rounds (not while draining).
        if backfilling && drain.is_none() {
            match job.backfill_step(&cx.cancel).await {
                Ok(true) => {
                    if job.col.row.state == State::Reembedding {
                        match job.cutover().await {
                            Ok(true) => reload = true,
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!(%tenant, embedding = id, error = %e, "re-embedding cutover failed; retrying");
                                // A stop cuts the wait short; its drain
                                // starts above.
                                let _ = pause(&cx.cancel, Duration::from_secs(2)).await;
                            }
                        }
                    } else {
                        let (mut c, store, _) = job.catalog().await?;
                        store.set_state(&mut c, id, State::Ready, None).await?;
                        job.col.row.state = State::Ready;
                        tracing::info!(%tenant, embedding = id, db = %job.col.db, table = %job.col.table, column = %job.col.row.column, embedded = job.embedded, "embedded column backfilled: ready");
                    }
                }
                Ok(false) => {}
                Err(e) => {
                    job.shards = None;
                    tracing::warn!(%tenant, embedding = id, error = %e, "backfill step failed; retrying");
                    if e.code() == Some(SprigCode::EmbeddingFailed) {
                        let (mut c, store, _) = job.catalog().await?;
                        let _ = store
                            .set_state(&mut c, id, State::Failed, Some(&e.to_string()))
                            .await;
                        if !pause(&cx.cancel, Duration::from_secs(30)).await {
                            continue;
                        }
                        return Err(e);
                    }
                    let _ = pause(&cx.cancel, Duration::from_secs(1)).await;
                }
            }
        } else if drain.is_none()
            && job.col.row.state == State::Ready
            && let Err(e) = job.scan_step(&cx.cancel).await
        {
            // The next window tries again; a failed column shows in the
            // rounds.
            job.shards = None;
            tracing::debug!(%tenant, embedding = id, error = %e, "stale-hash check step failed; retrying");
        }
    }
    backlog_set(&job_key, 0);
    if let Some(t) = last_token {
        let _ = subs.ack(tenant, name, &t, pending_ack, now_ms()).await;
    }
    Ok(json!({"stopped": "cancelled", "embedded": job.embedded}))
}

#[cfg(test)]
mod tests;
