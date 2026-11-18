//! `ai_retrieval` (retrieval indexes): one long-running job per index,
//! submitted by the controller's `ai_embed` loop for the gateway's feed
//! subscription `rx:i<id>`, keeping the index's chunk tables in step with
//! its source table. The machinery is the embedded columns' (`ai_embed`):
//! the same shared feed reader, throttles, model, batch sizes and
//! exactly-once rule (acknowledge only after the round's writes commit,
//! every write guarded).
//!
//! ```text
//! serve(tenant, rx:i<id>):
//!   the catalog row (ai_retrieval_indexes); its table (objects); the local
//!   model (loaded once per process); gone → drop the chunk tables, the
//!   row and the subscription, end
//!   tail the tenant's feed from the subscription's checkpoint
//!   loop:
//!     every 15 s, and when a shard map changes: the source's live shards
//!       (a data group's: those its map holds now, so a split's or move's
//!       new shards join at the cutover); a shard without the chunk table
//!       gets it (empty) and is filled again: state rebuilding; a shard
//!       that arrives with one (a split, merge or move copies the chunk
//!       tables with their rows, routed by `_sprig_rk`) is checked by the
//!       same fill, which embeds only what is missing or stale, and the
//!       state stays; chunks written while their row had no routing hash
//!       get it from the row; a chunk table whose schema has had no
//!       source table for a minute is dropped
//!     a round: the next feed item (up to 200 ms), then what is read,
//!       up to ROUND items: the table's inserts, updates of the indexed
//!       columns and deletes, keys by shard; a TRUNCATE sweeps the shard;
//!       a DDL reloads the row; keys of a shard that is no longer live
//!       (events from before a cutover) go to every live shard
//!     reconcile(keys) per shard, retried with backoff until done; the
//!       checkpoint moves after the round, at most every second
//!     between rounds, while a shard isn't filled: one keyset chunk of it
//!       (ai.embedding.backfill_rows_per_s, at most 200 rows), its last key
//!       saved in `progress`; a shard done is swept of chunks whose row is
//!       gone; every shard done → ready
//!     rows indexed (`rows_indexed`, SHOW's `Rows`): the rows with chunks
//!       per live shard, summed; each shard counted from its chunk table
//!       (its chunk 0s) when the job starts, when the live shards change
//!       (a split, merge, move or re-key), after a sweep and when its fill
//!       or check ends; in between each committed write adds a row that
//!       gained chunks and takes away one that lost them. Embeddings are
//!       counted apart: sprig_retrieval_rows_total{result="indexed"}
//!     the stale-hash check, between rounds once every shard is filled: one
//!       keyset window of one shard (a tenth of
//!       ai.embedding.backfill_rows_per_s, at most 200 rows; a minute's rest
//!       after each pass over every shard): the rows whose chunk 0's hash
//!       isn't theirs, or with text and no chunks, reconciled; a shard
//!       walked to its end is swept of chunks whose row is gone
//!   a graceful stop drains (sprig_feed::drain): the round in flight is
//!     finished, the feed read and handled on (no fill steps) until the
//!     position has passed the cluster's (read again once passed, until
//!     nothing more was written), at most STOP_DRAIN after the stop; then
//!     the position is acknowledged, so a binlog purge right after the
//!     stop takes nothing the restart needs
//!
//! reconcile(shard, rows):
//!   SELECT pk, text, title, filters, UNHEX(MD5(JSON_ARRAY(text, title,
//!     filters))) [, _sprig_rk] from the source, for a feed round's keys
//!     read causally (`ai_embed::causal`: after a wsrep_sync_wait barrier
//!     on Galera, else LOCK IN SHARE MODE); the stored hash (and routing
//!     hash) of each row's chunk 0
//!   same hash: nothing to do, or only the chunks' _sprig_rk set (guarded
//!     as below); no text: its chunks deleted
//!   else: cut into chunks (CHUNK BY), embedded in batches of
//!     ai.embedding.batch_rows, then per group of rows one transaction:
//!       the row's hash (and _sprig_rk) again, LOCK IN SHARE MODE (the
//!         guard): changed since → left to the newer change's event;
//!         gone → chunks deleted
//!       DELETE its chunks; INSERT the new ones, with the row's _sprig_rk
//!   keys the source no longer has: DELETE their chunks WHERE NOT EXISTS
//!     (the row) — the guard of a delete; on Galera after the barrier too
//!     (a round's TRUNCATE sweep likewise)
//! ```
//!
//! The work that locks the source's rows (the guard, and the clean-ups'
//! and routing hash catch-up's subqueries) goes, for a tenant on Sprig
//! 2PC's relay path, through the cluster's relay writer
//! (`locking_conn_of`); the chunk tables' DDL and plain reads stay direct.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable as _;
use mysql_async::{Pool, TxOpts};
use serde_json::{Value, json};
use sprig_ai::embedding;
use sprig_ai::retrieval::store::{RetrievalIndex, State, Store};
use sprig_ai::retrieval::{self, KeyColumn, metrics};
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
use sprig_feed::{FeedItem, MetaPlacement, Op, ResumeToken, StartAt, TailOptions, TenantTail};
use sprig_sql::retrieval::Chunking;

use crate::ai_embed::causal::{self, Reads};
use crate::ai_embed::{EmbedEnv, Scan, Throttle, backend, cut, flag, now_ms, pause};
use crate::runtime::{JobCtx, JobHandler};

/// The job kind (the controller loop's).
pub const JOB_KIND: &str = retrieval::KIND;

/// Feed items in one round at most.
const ROUND: usize = 2048;

/// Source rows in one backfill chunk at most (each may make many chunks).
const CHUNK: u64 = 200;

/// How often the source's shards are listed again.
const SHARDS_EVERY: Duration = Duration::from_secs(15);

/// Chunks given their row's routing hash per shard and listing at most.
const RK_CATCH_UP: u64 = 5000;

/// How long a chunk table stays in a schema without its source table
/// before it is dropped (the table moved away alone, as into its table
/// group; a cutover's renames take seconds).
const ORPHAN_AFTER: Duration = Duration::from_secs(60);

/// `ai_retrieval` (see the module docs).
#[derive(Debug, Clone)]
pub struct AiRetrieval(pub EmbedEnv);

#[async_trait::async_trait]
impl JobHandler for AiRetrieval {
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

// ---- the parts the loop is made of ------------------------------------------

/// A row's text cut into (heading, chunk) pairs by `chunking`, at most
/// `max` of them; the heading is the title (and, by headings, the headings
/// above the chunk).
#[must_use]
pub fn chunk_row(
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

/// Whether feed event op `op` with changed columns `delta` may change the
/// index: inserts, deletes and truncates do; an update when it changed an
/// indexed column or a key column (or the feed doesn't say which).
#[must_use]
pub fn wants(op: Op, delta: Option<&[String]>, columns: &[String]) -> bool {
    match op {
        Op::Insert | Op::Delete | Op::Truncate => true,
        Op::Update => delta.is_none_or(|d| {
            d.iter()
                .any(|c| columns.iter().any(|x| x.eq_ignore_ascii_case(c)))
        }),
        Op::Ddl(_) => false,
    }
}

/// Moves the keys and truncates of shards not in `live` to every live
/// shard.
fn spread(keys: &mut RoundKeys, truncated: &mut BTreeSet<ShardLoc>, live: &[ShardLoc]) {
    let stray: Vec<ShardLoc> = keys.keys().filter(|s| !live.contains(s)).cloned().collect();
    for s in stray {
        if let Some(ks) = keys.remove(&s) {
            for l in live {
                keys.entry(l.clone())
                    .or_default()
                    .extend(ks.iter().cloned());
            }
        }
    }
    if truncated.iter().any(|s| !live.contains(s)) {
        truncated.retain(|s| live.contains(s));
        truncated.extend(live.iter().cloned());
    }
}

/// One source row's work: its index in the read, and its (heading,
/// chunk) pairs.
type RowWork = (usize, Vec<(String, String)>);

/// Feed keys of one round, by shard.
type RoundKeys = BTreeMap<ShardLoc, Vec<Vec<(String, sprig_binlog::Value)>>>;

/// Where one shard of the table is.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ShardLoc {
    group: u32,
    shard: u32,
}

/// The index the job keeps, as last read.
struct Index {
    row: RetrievalIndex,
    db: String,
    table: String,
    chunking: Chunking,
}

impl Index {
    fn schema(&self, tenant: TenantId, s: &ShardLoc) -> String {
        SchemaName::shard(
            tenant,
            DbId(self.row.db_id),
            GroupId(s.group),
            ShardId(s.shard),
        )
        .to_string()
    }

    fn source(&self, schema: &str) -> String {
        format!("{}.{}", quote(schema), quote(&self.table))
    }

    fn chunks(&self, schema: &str) -> String {
        format!(
            "{}.{}",
            quote(schema),
            quote(&retrieval::table_name(self.row.id))
        )
    }

    /// The columns whose change matters: text, title, filters, key.
    fn watched(&self) -> Vec<String> {
        let mut v = vec![self.row.text_column.clone()];
        v.extend(self.row.title_column.clone());
        v.extend(self.row.filters.iter().map(|f| f.name.clone()));
        v.extend(self.row.key_columns.iter().map(|k| k.name.clone()));
        v
    }

    /// `pk1 = ? AND pk2 = ?` over `qualifier`'s columns (or the chunk
    /// table's key columns).
    fn key_match(&self, chunk_table: bool) -> String {
        self.row
            .key_columns
            .iter()
            .map(|k| {
                let c = if chunk_table {
                    retrieval::key_column(&k.name)
                } else {
                    k.name.clone()
                };
                format!("{} = ?", quote(&c))
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// `SELECT pk..., text, title, filters..., hash [, _sprig_rk] FROM
    /// source WHERE pred [ORDER BY pk LIMIT n]`.
    fn select_sql(&self, schema: &str, pred: &str, order: Option<u64>, rk: bool) -> String {
        let r = &self.row;
        let mut cols: Vec<String> = r.key_columns.iter().map(|k| quote(&k.name)).collect();
        cols.push(quote(&r.text_column));
        cols.push(
            r.title_column
                .as_deref()
                .map_or_else(|| "NULL".to_owned(), quote),
        );
        cols.extend(r.filters.iter().map(|f| quote(&f.name)));
        cols.push(retrieval::hash_expr(
            &r.text_column,
            r.title_column.as_deref(),
            &r.filters,
            None,
        ));
        if rk {
            cols.push(quote(retrieval::RK_COLUMN));
        }
        let mut sql = format!(
            "SELECT {} FROM {} WHERE {pred}",
            cols.join(", "),
            self.source(schema)
        );
        if let Some(n) = order {
            let keys: Vec<String> = r.key_columns.iter().map(|k| quote(&k.name)).collect();
            let _ = write!(sql, " ORDER BY {} LIMIT {n}", keys.join(", "));
        }
        sql
    }
}

/// One source row as read.
struct SrcRow {
    key: Vec<mysql_async::Value>,
    key_text: Vec<Option<String>>,
    text: Option<String>,
    title: Option<String>,
    filters: Vec<mysql_async::Value>,
    hash: Option<Vec<u8>>,
    /// Its routing hash, when the source has one.
    rk: Option<u64>,
}

fn bytes(r: &mysql_async::Row, i: usize) -> Option<Vec<u8>> {
    match r.as_ref(i) {
        Some(mysql_async::Value::Bytes(b)) => Some(b.clone()),
        Some(mysql_async::Value::NULL) | None => None,
        Some(v) => Some(v.as_sql(true).trim_matches('\'').as_bytes().to_vec()),
    }
}

fn text_of(r: &mysql_async::Row, i: usize) -> Option<String> {
    bytes(r, i).map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn u64_of(r: &mysql_async::Row, i: usize) -> Option<u64> {
    match r.as_ref(i) {
        Some(mysql_async::Value::UInt(u)) => Some(*u),
        Some(mysql_async::Value::Int(n)) => u64::try_from(*n).ok(),
        Some(mysql_async::Value::Bytes(b)) => std::str::from_utf8(b).ok()?.parse().ok(),
        _ => None,
    }
}

/// `[x,y,...]`, as `VEC_FromText` reads it.
fn vec_text(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(ToString::to_string).collect();
    format!("[{}]", parts.join(","))
}

/// The job's state between rounds.
struct Job<'a> {
    env: &'a EmbedEnv,
    tenant: TenantId,
    ix: Index,
    model: Arc<sprig_embed::Embedder>,
    batch: usize,
    max_bytes: usize,
    max_chunks: usize,
    live: Throttle,
    backfill: Throttle,
    /// The fill's position per shard schema: the last key, or `done`.
    progress: BTreeMap<String, Value>,
    /// Rows with chunks per live shard schema.
    rows: BTreeMap<String, u64>,
    /// The rows indexed last saved in the catalog.
    rows_saved: Option<u64>,
    shards: Option<(Instant, Vec<(ShardLoc, ClusterId)>)>,
    /// The live shard schemas whose source table has `_sprig_rk`.
    rk: HashSet<String>,
    /// Shard map changes: the shards are listed again on one.
    map_changes: tokio::sync::watch::Receiver<u64>,
    /// Chunk tables in schemas without the source table, since when.
    orphans: HashMap<String, Instant>,
    /// Rows embedded by this run of the job.
    indexed: u64,
    /// How each cluster is read causally.
    kinds: causal::Kinds,
    /// The stale-hash check.
    scan: Scan,
}

impl Job<'_> {
    async fn catalog(&self) -> Result<(mysql_async::Conn, Store)> {
        let ep = self.env.logic.locator.writer(self.tenant).await?;
        let conn = ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        Ok((conn, Store::new(ep.store.schema())))
    }

    async fn set_state(&mut self, state: State, error: Option<&str>) -> Result<()> {
        let (mut c, store) = self.catalog().await?;
        store
            .set_state(&mut c, self.ix.row.id, state, error)
            .await?;
        self.ix.row.state = state;
        Ok(())
    }

    /// The cluster holding shard `s` now: the group's map, else the
    /// catalog's cluster.
    async fn cluster_of(&self, s: &ShardLoc) -> Result<ClusterId> {
        if let Ok(m) = self.env.maps.get(self.tenant, GroupId(s.group))
            && let Some(r) = m.shard(ShardId(s.shard))
        {
            return Ok(r.cluster.clone());
        }
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

    /// A direct connection to shard `s`: chunk table DDL and plain reads.
    async fn conn_of(&self, s: &ShardLoc) -> Result<mysql_async::Conn> {
        let pool = self.pool_of(s).await?;
        pool.get_conn_bounded()
            .await
            .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("retrieval: {e}")))
    }

    /// A connection for the writes that lock the source's rows (the
    /// reconcile's guard, `LOCK IN SHARE MODE`, and the source subqueries
    /// of the clean-ups and the routing hash catch-up, shared locks under
    /// `REPEATABLE READ`): for a tenant on the relay path, through the
    /// cluster's relay writer, chosen again at each use, so the fence ends
    /// it with the tenant's others when a held branch's holder is lost.
    ///
    /// # Errors
    /// As [`Self::conn_of`]; a retryable 39030 while the cluster has
    /// relays but none is ready.
    async fn locking_conn_of(&self, s: &ShardLoc) -> Result<mysql_async::Conn> {
        let c = self.cluster_of(s).await?;
        let direct = self.env.logic.pools.writer(&c).await?;
        let pool =
            crate::relay_pools::shard_pool(&self.env.logic.dc, self.tenant, &c, direct).await?;
        pool.get_conn_bounded()
            .await
            .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("retrieval: {e}")))
    }

    /// How shard `s`'s cluster is read causally.
    async fn reads(&mut self, s: &ShardLoc) -> Result<Reads> {
        let cluster = self.cluster_of(s).await?;
        self.kinds.of(&self.env.logic.registry, &cluster).await
    }

    /// The table's live shards, each with its chunk table: a shard without
    /// one gets it and starts filling (state rebuilding once the index was
    /// ready); a shard that arrives with one (a split, a merge or a move
    /// carried it) is checked by the same fill, which embeds only rows
    /// whose chunks are missing or stale, and the state stays; a shard
    /// gone leaves `progress`. Listed again every [`SHARDS_EVERY`] and when
    /// a shard map changes.
    #[allow(clippy::too_many_lines)] // list, then make, then follow
    async fn shards(&mut self) -> Result<Vec<(ShardLoc, ClusterId)>> {
        let moved = self.map_changes.has_changed().unwrap_or(false);
        if moved {
            self.map_changes.mark_unchanged();
        }
        if !moved
            && let Some((at, s)) = &self.shards
            && at.elapsed() < SHARDS_EVERY
        {
            return Ok(s.clone());
        }
        let prefix = format!("t{}_d{}_g", self.tenant.0, self.ix.row.db_id);
        let chunk_table = retrieval::table_name(self.ix.row.id);
        let mut out = Vec::new();
        let mut have: HashSet<String> = HashSet::new();
        let mut rk_source: HashSet<String> = HashSet::new();
        let mut rk_chunks: HashSet<String> = HashSet::new();
        for cluster in self.env.logic.pools.sweepable().await? {
            let pool = self.env.logic.pools.writer(&cluster).await?;
            let mut c = pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}"))
            })?;
            let rows: Vec<(String, String)> = c
                .exec(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA LIKE CONCAT(REPLACE(?, '_', '\\_'), '%') \
                     AND TABLE_NAME IN (?, ?)",
                    (
                        prefix.as_str(),
                        self.ix.table.as_str(),
                        chunk_table.as_str(),
                    ),
                )
                .await
                .map_err(backend)?;
            let with_rk: Vec<(String, String)> = c
                .exec(
                    "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA LIKE CONCAT(REPLACE(?, '_', '\\_'), '%') \
                     AND TABLE_NAME IN (?, ?) AND COLUMN_NAME = ?",
                    (
                        prefix.as_str(),
                        self.ix.table.as_str(),
                        chunk_table.as_str(),
                        retrieval::RK_COLUMN,
                    ),
                )
                .await
                .map_err(backend)?;
            for (schema, table) in with_rk {
                if table == chunk_table {
                    rk_chunks.insert(schema);
                } else {
                    rk_source.insert(schema);
                }
            }
            let mut sources = BTreeSet::new();
            let mut chunks_here = Vec::new();
            for (schema, table) in rows {
                if table == chunk_table {
                    chunks_here.push(schema.clone());
                    have.insert(schema);
                } else {
                    sources.insert(schema);
                }
            }
            // A chunk table left without its source (the table was moved
            // into its group alone): dropped once that has lasted.
            for schema in chunks_here {
                if sources.contains(&schema) {
                    self.orphans.remove(&schema);
                    continue;
                }
                let since = *self
                    .orphans
                    .entry(schema.clone())
                    .or_insert_with(Instant::now);
                if since.elapsed() >= ORPHAN_AFTER {
                    c.query_drop(format!("USE {}", quote(&schema)))
                        .await
                        .map_err(backend)?;
                    c.query_drop(format!("DROP TABLE IF EXISTS {}", quote(&chunk_table)))
                        .await
                        .map_err(backend)?;
                    self.orphans.remove(&schema);
                    have.remove(&schema);
                    tracing::info!(tenant = %self.tenant, index = self.ix.row.id, %schema, "retrieval index: a chunk table without its table dropped");
                }
            }
            for s in sources {
                let Ok(SchemaName::Shard {
                    tenant,
                    db,
                    group,
                    shard,
                }) = s.parse::<SchemaName>()
                else {
                    continue;
                };
                if tenant != self.tenant || db.0 != self.ix.row.db_id {
                    continue;
                }
                let loc = ShardLoc {
                    group: group.0,
                    shard: shard.0,
                };
                let live = match self.env.maps.get(self.tenant, GroupId(loc.group)) {
                    // A data group's shards are those its map holds: a
                    // split's or move's new shards, being copied, join at
                    // the cutover.
                    Ok(m) => m
                        .shard(ShardId(loc.shard))
                        .is_some_and(|r| r.cluster == cluster),
                    // A database's default group: the tenant's cluster.
                    Err(_) => self.cluster_of(&loc).await.is_ok_and(|c| c == cluster),
                };
                if live {
                    out.push((loc, cluster.clone()));
                }
            }
        }
        out.sort();
        // A split, merge, move or re-key changes the live shards (or their
        // clusters): every shard's rows are counted again.
        let recount = self.shards.as_ref().is_none_or(|(_, v)| *v != out);
        // Chunk tables where they are missing; progress follows the shards.
        let mut changed = false;
        let mut created = false;
        let schemas: BTreeSet<String> = out
            .iter()
            .map(|(s, _)| self.ix.schema(self.tenant, s))
            .collect();
        for (loc, _) in &out {
            let schema = self.ix.schema(self.tenant, loc);
            if !have.contains(&schema) {
                let r = &self.ix.row;
                let sql = retrieval::create_table_sql(
                    &schema,
                    r.id,
                    r.model.dimensions,
                    &r.key_columns,
                    &r.filters,
                );
                let mut c = self.conn_of(loc).await?;
                // From the shard's schema, so a mover copying the shard
                // makes the table on its targets too.
                c.query_drop(format!("USE {}", quote(&schema)))
                    .await
                    .map_err(backend)?;
                c.query_drop(&sql).await.map_err(backend)?;
                self.rows.insert(schema.clone(), 0);
                self.progress.insert(schema, Value::Null);
                created = true;
                changed = true;
                continue;
            }
            if !rk_chunks.contains(&schema) {
                // A chunk table made before it carried the routing hash.
                let mut c = self.conn_of(loc).await?;
                c.query_drop(format!("USE {}", quote(&schema)))
                    .await
                    .map_err(backend)?;
                c.query_drop(retrieval::add_rk_sql(self.ix.row.id))
                    .await
                    .map_err(backend)?;
            }
            if recount || !self.rows.contains_key(&schema) {
                let n = self.count(loc, &schema).await?;
                if self.rows.insert(schema.clone(), n) != Some(n) {
                    changed = true;
                }
            }
            if let std::collections::btree_map::Entry::Vacant(e) = self.progress.entry(schema) {
                // Carried here with its rows: checked, not filled again.
                e.insert(Value::Null);
                changed = true;
            }
        }
        let gone: Vec<String> = self
            .progress
            .keys()
            .filter(|k| !schemas.contains(*k))
            .cloned()
            .collect();
        for g in gone {
            self.progress.remove(&g);
            changed = true;
        }
        self.rows.retain(|k, _| schemas.contains(k));
        if changed {
            self.save_progress().await?;
        }
        if created && self.ix.row.state == State::Ready {
            self.set_state(State::Rebuilding, None).await?;
            tracing::info!(tenant = %self.tenant, index = self.ix.row.id, "retrieval index: a shard is filled again (rebuilding)");
        }
        // Chunks written while their row had no routing hash yet (before
        // the table's group was sharded) get it from the row.
        rk_source.retain(|s| schemas.contains(s));
        self.rk = rk_source;
        for (loc, _) in &out {
            let schema = self.ix.schema(self.tenant, loc);
            if !self.rk.contains(&schema) {
                continue;
            }
            let sql = retrieval::rk_catch_up_sql(
                &self.ix.source(&schema),
                &self.ix.chunks(&schema),
                &self.ix.row.key_columns,
                RK_CATCH_UP,
            );
            let done = match self.locking_conn_of(loc).await {
                Ok(mut c) => c.query_drop(sql).await.map_err(backend),
                Err(e) => Err(e),
            };
            if let Err(e) = done {
                tracing::debug!(tenant = %self.tenant, index = self.ix.row.id, %schema, error = %e, "retrieval index: routing hashes not set yet");
            }
        }
        self.shards = Some((Instant::now(), out.clone()));
        Ok(out)
    }

    /// Whether `s` was live at the last listing.
    fn is_live(&self, s: &ShardLoc) -> bool {
        self.shards
            .as_ref()
            .is_some_and(|(_, v)| v.iter().any(|(l, _)| l == s))
    }

    /// Feed keys and truncates of shards that aren't live (events from
    /// before a split, merge or move, read after its cutover) moved to
    /// every live shard: each brings what it holds of those rows up to
    /// date, and the others find nothing to do.
    async fn relocate(
        &mut self,
        keys: &mut RoundKeys,
        truncated: &mut BTreeSet<ShardLoc>,
    ) -> Result<()> {
        if keys.keys().chain(truncated.iter()).all(|s| self.is_live(s)) {
            return Ok(());
        }
        self.shards = None;
        let live: Vec<ShardLoc> = self.shards().await?.into_iter().map(|(s, _)| s).collect();
        spread(keys, truncated, &live);
        Ok(())
    }

    fn all_done(&self) -> bool {
        self.progress.values().all(|p| p == "done")
    }

    /// The rows with chunks in shard `s` (schema `schema`): its chunk 0s.
    async fn count(&self, s: &ShardLoc, schema: &str) -> Result<u64> {
        let mut c = self.conn_of(s).await?;
        let n: Option<u64> = c
            .query_first(format!(
                "SELECT COUNT(*) FROM {} WHERE chunk_no = 0",
                self.ix.chunks(schema)
            ))
            .await
            .map_err(backend)?;
        Ok(n.unwrap_or(0))
    }

    /// Adds `gained` rows to shard `schema`'s count and takes `lost` away.
    fn adjust(&mut self, schema: &str, gained: u64, lost: u64) {
        if let Some(n) = self.rows.get_mut(schema) {
            *n = (*n + gained).saturating_sub(lost);
        }
    }

    /// The rows indexed: the rows with chunks over the live shards.
    fn rows_indexed(&self) -> u64 {
        self.rows.values().sum()
    }

    /// Whether the rows indexed changed since the last save.
    fn rows_unsaved(&self) -> bool {
        self.rows_saved != Some(self.rows_indexed())
    }

    async fn save_progress(&mut self) -> Result<()> {
        let (mut c, store) = self.catalog().await?;
        let p = Value::Object(self.progress.clone().into_iter().collect());
        let rows = self.rows_indexed();
        store.set_progress(&mut c, self.ix.row.id, &p, rows).await?;
        self.rows_saved = Some(rows);
        Ok(())
    }

    /// Embeds `inputs` with the model, in batches.
    async fn embed(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let model = Arc::clone(&self.model);
        let batch = self.batch.max(1);
        let started = Instant::now();
        let out = tokio::task::spawn_blocking(move || {
            let mut out = Vec::with_capacity(inputs.len());
            for part in inputs.chunks(batch) {
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
        .map_err(|e| embedding::failed(format!("the model failed: {e}")))?;
        embedding::metrics::seconds(&self.ix.row.model.alias, started.elapsed().as_secs_f64());
        Ok(out)
    }

    /// Brings the source rows `sql` selects up to date in the chunk table
    /// of `schema` (see the module docs), reading them causally when
    /// `causal` (a feed round's keys); the last row's key as SQL literals,
    /// the number of rows read and the number whose chunks were written or
    /// deleted.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the reconcile of the module docs
    async fn reconcile(
        &mut self,
        s: &ShardLoc,
        sql: &str,
        causal: bool,
        cancel: &sprig_common::CancellationToken,
    ) -> Result<(Option<Vec<String>>, u64, u64)> {
        let schema = self.ix.schema(self.tenant, s);
        let has_rk = self.rk.contains(&schema);
        let keys: Vec<KeyColumn> = self.ix.row.key_columns.clone();
        let nk = keys.len();
        let nf = self.ix.row.filters.len();
        let mut c = self.locking_conn_of(s).await?;
        let sql = if causal {
            let reads = self.reads(s).await?;
            causal::read_sql(reads, &mut c, sql).await?
        } else {
            sql.to_owned()
        };
        let rows: Vec<mysql_async::Row> = c.query(sql).await.map_err(|e| self.source_error(e))?;
        let read = rows.len() as u64;
        let src: Vec<SrcRow> = rows
            .iter()
            .map(|r| {
                let key: Vec<mysql_async::Value> = (0..nk)
                    .map(|i| r.as_ref(i).cloned().unwrap_or(mysql_async::Value::NULL))
                    .collect();
                let key_text = (0..nk)
                    .map(|i| keys[i].text(bytes(r, i).as_deref()))
                    .collect();
                SrcRow {
                    key,
                    key_text,
                    text: text_of(r, nk),
                    title: text_of(r, nk + 1),
                    filters: (0..nf)
                        .map(|i| {
                            r.as_ref(nk + 2 + i)
                                .cloned()
                                .unwrap_or(mysql_async::Value::NULL)
                        })
                        .collect(),
                    hash: bytes(r, nk + 2 + nf),
                    rk: if has_rk { u64_of(r, nk + 3 + nf) } else { None },
                }
            })
            .collect();
        let last = rows.last().map(|r| {
            (0..nk)
                .map(|i| keys[i].literal(bytes(r, i).as_deref()))
                .collect::<Vec<_>>()
        });
        if src.is_empty() {
            return Ok((last, read, 0));
        }
        // The stored hashes and routing hashes (chunk 0 of each row).
        let mut stored: HashMap<Vec<Option<String>>, (Vec<u8>, u64)> = HashMap::new();
        for part in src.chunks(500) {
            let pred: Vec<String> = part
                .iter()
                .map(|r| {
                    let parts: Vec<String> = keys
                        .iter()
                        .zip(&r.key)
                        .map(|(k, v)| {
                            let b = match v {
                                mysql_async::Value::Bytes(b) => Some(b.as_slice()),
                                _ => None,
                            };
                            format!(
                                "{} = {}",
                                quote(&retrieval::key_column(&k.name)),
                                k.literal(b)
                            )
                        })
                        .collect();
                    format!("({})", parts.join(" AND "))
                })
                .collect();
            let kcols: Vec<String> = keys
                .iter()
                .map(|k| quote(&retrieval::key_column(&k.name)))
                .collect();
            let q = format!(
                "SELECT {}, src_hash, {} FROM {} WHERE chunk_no = 0 AND ({})",
                kcols.join(", "),
                quote(retrieval::RK_COLUMN),
                self.ix.chunks(&schema),
                pred.join(" OR ")
            );
            let got: Vec<mysql_async::Row> = c.query(q).await.map_err(backend)?;
            for r in &got {
                let kt: Vec<Option<String>> = (0..nk)
                    .map(|i| keys[i].text(bytes(r, i).as_deref()))
                    .collect();
                if let Some(h) = bytes(r, nk) {
                    stored.insert(kt, (h, u64_of(r, nk + 1).unwrap_or(0)));
                }
            }
        }
        // What each row needs.
        let mut skipped = 0u64;
        let mut work: Vec<RowWork> = Vec::new();
        // Rows whose chunks are current but for their routing hash.
        let mut rk_only: Vec<usize> = Vec::new();
        for (i, r) in src.iter().enumerate() {
            let have = stored.get(&r.key_text);
            if let Some((h, rk)) = have
                && Some(h.as_slice()) == r.hash.as_deref()
            {
                if has_rk && *rk != r.rk.unwrap_or(0) {
                    rk_only.push(i);
                } else {
                    skipped += 1;
                }
                continue;
            }
            let text = r.text.as_deref().unwrap_or("");
            let chunks = if text.trim().is_empty() {
                Vec::new()
            } else {
                chunk_row(
                    self.ix.chunking,
                    cut(text, self.max_bytes),
                    r.title.as_deref(),
                    self.max_chunks,
                )
            };
            if chunks.is_empty() && have.is_none() {
                skipped += 1;
                continue;
            }
            work.push((i, chunks));
        }
        // Groups of rows, about a model batch of chunks each, one
        // transaction per group.
        let target = self.batch.max(1);
        let mut groups: Vec<Vec<RowWork>> = Vec::new();
        let mut cur: Vec<RowWork> = Vec::new();
        let mut n = 0;
        for w in work {
            n += w.1.len().max(1);
            cur.push(w);
            if n >= target {
                groups.push(std::mem::take(&mut cur));
                n = 0;
            }
        }
        if !cur.is_empty() {
            groups.push(cur);
        }
        let guard = format!(
            "SELECT {}{} FROM {} WHERE {} LOCK IN SHARE MODE",
            retrieval::hash_expr(
                &self.ix.row.text_column,
                self.ix.row.title_column.as_deref(),
                &self.ix.row.filters,
                None
            ),
            if has_rk {
                format!(", {}", quote(retrieval::RK_COLUMN))
            } else {
                String::new()
            },
            self.ix.source(&schema),
            self.ix.key_match(false)
        );
        let delete = format!(
            "DELETE FROM {} WHERE {}",
            self.ix.chunks(&schema),
            self.ix.key_match(true)
        );
        let mut insert_cols: Vec<String> = keys
            .iter()
            .map(|k| quote(&retrieval::key_column(&k.name)))
            .collect();
        insert_cols.extend(["`chunk_no`", "`title`", "`text`"].map(str::to_owned));
        insert_cols.extend(
            self.ix
                .row
                .filters
                .iter()
                .map(|f| quote(&retrieval::filter_column(&f.name))),
        );
        insert_cols.push(quote(retrieval::RK_COLUMN));
        insert_cols.extend(["`src_hash`", "`embedding`"].map(str::to_owned));
        let marks: Vec<&str> = std::iter::repeat_n("?", insert_cols.len() - 1)
            .chain(std::iter::once("VEC_FromText(?)"))
            .collect();
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.ix.chunks(&schema),
            insert_cols.join(", "),
            marks.join(", ")
        );
        let (mut indexed, mut cleared, mut superseded, mut written) = (0u64, 0u64, 0u64, 0u64);
        for group in groups {
            // Rows that gained chunks and rows that lost them, counted once
            // the group's transaction commits.
            let (mut gained, mut emptied) = (0u64, 0u64);
            if cancel.is_cancelled() {
                return Err(SprigError::internal("cancelled"));
            }
            let inputs: Vec<String> = group
                .iter()
                .flat_map(|(_, ch)| ch.iter().map(|(h, t)| retrieval::embedding_input(h, t)))
                .collect();
            let vectors = if inputs.is_empty() {
                Vec::new()
            } else {
                self.embed(inputs).await?
            };
            let mut vi = 0usize;
            let mut tx = c
                .start_transaction(TxOpts::default())
                .await
                .map_err(backend)?;
            for (i, chunks) in &group {
                let r = &src[*i];
                let had = stored.contains_key(&r.key_text);
                let now: Option<mysql_async::Row> = tx
                    .exec_first(&guard, mysql_async::Params::Positional(r.key.clone()))
                    .await
                    .map_err(backend)?;
                let mine = &r.hash;
                let gone = now.is_none();
                let now_rk = now.as_ref().and_then(|n| u64_of(n, 1)).unwrap_or(0);
                if let Some(n) = &now
                    && bytes(n, 0).as_ref() != mine.as_ref()
                {
                    superseded += 1;
                    vi += chunks.len();
                    continue;
                }
                tx.exec_drop(&delete, mysql_async::Params::Positional(r.key.clone()))
                    .await
                    .map_err(backend)?;
                if gone || chunks.is_empty() {
                    cleared += 1;
                    emptied += u64::from(had);
                    vi += chunks.len();
                    continue;
                }
                for (no, (heading, text)) in chunks.iter().enumerate() {
                    let v = &vectors[vi];
                    vi += 1;
                    let mut p: Vec<mysql_async::Value> = r.key.clone();
                    p.push(u16::try_from(no).unwrap_or(u16::MAX).into());
                    p.push(if heading.is_empty() {
                        mysql_async::Value::NULL
                    } else {
                        heading.as_str().into()
                    });
                    p.push(text.as_str().into());
                    p.extend(r.filters.iter().cloned());
                    p.push(now_rk.into());
                    p.push(mine.clone().into());
                    p.push(vec_text(v).into());
                    tx.exec_drop(&insert, mysql_async::Params::Positional(p))
                        .await
                        .map_err(backend)?;
                    written += 1;
                }
                indexed += 1;
                gained += u64::from(!had);
            }
            tx.commit().await.map_err(backend)?;
            self.adjust(&schema, gained, emptied);
        }
        // Current chunks given their row's routing hash, under the guard.
        if !rk_only.is_empty() {
            let set_rk = format!(
                "UPDATE {} SET {} = ? WHERE {}",
                self.ix.chunks(&schema),
                quote(retrieval::RK_COLUMN),
                self.ix.key_match(true)
            );
            for part in rk_only.chunks(200) {
                let mut tx = c
                    .start_transaction(TxOpts::default())
                    .await
                    .map_err(backend)?;
                for &i in part {
                    let r = &src[i];
                    let now: Option<mysql_async::Row> = tx
                        .exec_first(&guard, mysql_async::Params::Positional(r.key.clone()))
                        .await
                        .map_err(backend)?;
                    // Changed or gone since: the newer event's to handle.
                    let Some(now) = now.filter(|n| bytes(n, 0) == r.hash) else {
                        continue;
                    };
                    let mut p = vec![mysql_async::Value::from(u64_of(&now, 1).unwrap_or(0))];
                    p.extend(r.key.iter().cloned());
                    tx.exec_drop(&set_rk, mysql_async::Params::Positional(p))
                        .await
                        .map_err(backend)?;
                }
                tx.commit().await.map_err(backend)?;
            }
            skipped += rk_only.len() as u64;
        }
        metrics::rows("skipped", skipped);
        metrics::rows("indexed", indexed);
        metrics::rows("cleared", cleared);
        metrics::rows("superseded", superseded);
        metrics::chunks(written);
        self.indexed += indexed;
        Ok((last, read, indexed + cleared))
    }

    /// 1054 (a column the index reads is gone) and 1146 of the source as
    /// they are: the first fails the index, the second is retried.
    fn source_error(&self, e: mysql_async::Error) -> SprigError {
        if let mysql_async::Error::Server(s) = &e
            && s.code == 1054
        {
            return embedding::failed(format!(
                "retrieval index {}: {}.{} lost a column it reads: {}",
                self.ix.row.name, self.ix.db, self.ix.table, s.message
            ));
        }
        backend(e)
    }

    /// The rows of `keys` (feed keys of one shard) brought up to date;
    /// keys the source no longer has lose their chunks.
    async fn reconcile_keys(
        &mut self,
        s: &ShardLoc,
        keys: &[Vec<(String, sprig_binlog::Value)>],
        cancel: &sprig_common::CancellationToken,
    ) -> Result<()> {
        let schema = self.ix.schema(self.tenant, s);
        let kc = self.ix.row.key_columns.clone();
        for part in keys.chunks(200) {
            let lits: Vec<Vec<String>> = part
                .iter()
                .filter_map(|k| {
                    kc.iter()
                        .map(|c| {
                            k.iter()
                                .find(|(n, _)| n.eq_ignore_ascii_case(&c.name))
                                .map(|(_, v)| sprig_logic::triggers::literal(v))
                        })
                        .collect::<Option<Vec<String>>>()
                })
                .collect();
            if lits.is_empty() {
                continue;
            }
            let eq = |cols: &dyn Fn(&KeyColumn) -> String, l: &[String]| -> String {
                kc.iter()
                    .zip(l)
                    .map(|(c, v)| format!("{} = {v}", cols(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            let pred: Vec<String> = lits
                .iter()
                .map(|l| format!("({})", eq(&|c| quote(&c.name), l)))
                .collect();
            let rk = self.rk.contains(&schema);
            let sql = self.ix.select_sql(&schema, &pred.join(" OR "), None, rk);
            self.reconcile(s, &sql, true, cancel).await?;
            // The rows that are gone: their chunks, unless the row exists
            // (read causally: on Galera after the barrier; elsewhere the
            // subquery is a locking read).
            let reads = self.reads(s).await?;
            let mut c = self.locking_conn_of(s).await?;
            if reads == Reads::Barrier {
                causal::barrier(&mut c).await?;
            }
            let mut gone = 0u64;
            for l in &lits {
                let stmt = format!(
                    "DELETE FROM {} WHERE {} AND NOT EXISTS (SELECT 1 FROM {} WHERE {})",
                    self.ix.chunks(&schema),
                    eq(&|c| quote(&retrieval::key_column(&c.name)), l),
                    self.ix.source(&schema),
                    eq(&|c| quote(&c.name), l)
                );
                c.query_drop(stmt).await.map_err(backend)?;
                if c.affected_rows() > 0 {
                    gone += 1;
                }
            }
            self.adjust(&schema, 0, gone);
            metrics::rows("cleared", gone);
        }
        Ok(())
    }

    /// Deletes the chunks of shard `s` whose row is gone, then counts the
    /// shard's rows again; after a feed round's `TRUNCATE` (`causal`), on
    /// Galera after the barrier (elsewhere the subquery is a locking read).
    async fn sweep(&mut self, s: &ShardLoc, causal: bool) -> Result<u64> {
        let schema = self.ix.schema(self.tenant, s);
        // Single-table form: a multi-table DELETE with no current schema
        // is refused (1046).
        let chunks = self.ix.chunks(&schema);
        let on: Vec<String> = self
            .ix
            .row
            .key_columns
            .iter()
            .map(|k| {
                format!(
                    "s.{} = {chunks}.{}",
                    quote(&k.name),
                    quote(&retrieval::key_column(&k.name))
                )
            })
            .collect();
        let reads = if causal {
            Some(self.reads(s).await?)
        } else {
            None
        };
        let mut c = self.locking_conn_of(s).await?;
        if reads == Some(Reads::Barrier) {
            causal::barrier(&mut c).await?;
        }
        c.query_drop(format!(
            "DELETE FROM {chunks} WHERE NOT EXISTS (SELECT 1 FROM {} AS s WHERE {})",
            self.ix.source(&schema),
            on.join(" AND ")
        ))
        .await
        .map_err(backend)?;
        let swept = c.affected_rows();
        drop(c);
        let n = self.count(s, &schema).await?;
        self.rows.insert(schema, n);
        Ok(swept)
    }

    /// One window of the stale-hash check (see the module docs): the next
    /// rows of the shard it walks, read plainly with their chunk 0's hash;
    /// those whose chunks are stale, or missing while they have text, are
    /// reconciled. A shard walked to its end is swept of chunks whose row
    /// is gone. A pass over every shard ends with a log line when it
    /// repaired anything.
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
                tracing::warn!(tenant = %self.tenant, index = self.ix.row.id, db = %self.ix.db, table = %self.ix.table, repaired, "retrieval index: the stale-hash check brought rows' chunks up to date that their changes had missed");
            }
            return Ok(());
        };
        let schema = self.ix.schema(self.tenant, &s);
        let kc = self.ix.row.key_columns.clone();
        let nk = kc.len();
        let sql = {
            let r = &self.ix.row;
            let hash = retrieval::hash_expr(
                &r.text_column,
                r.title_column.as_deref(),
                &r.filters,
                Some("s"),
            );
            retrieval::stale_scan_sql(
                &self.ix.source(&schema),
                &self.ix.chunks(&schema),
                &kc,
                &r.text_column,
                &hash,
                self.scan.after.as_deref(),
                n,
            )
        };
        let rows: Vec<mysql_async::Row> = {
            let mut c = self.conn_of(&s).await?;
            c.query(sql).await.map_err(|e| self.source_error(e))?
        };
        let read = rows.len() as u64;
        let key = |r: &mysql_async::Row| -> Vec<String> {
            (0..nk)
                .map(|i| kc[i].literal(bytes(r, i).as_deref()))
                .collect()
        };
        let last = rows.last().map(key);
        let stale: Vec<String> = rows
            .iter()
            .filter(|r| flag(r.as_ref(nk)))
            .map(|r| {
                let eq: Vec<String> = kc
                    .iter()
                    .zip(key(r))
                    .map(|(c, v)| format!("{} = {v}", quote(&c.name)))
                    .collect();
                format!("({})", eq.join(" AND "))
            })
            .collect();
        if !stale.is_empty() {
            let rk = self.rk.contains(&schema);
            let sql = self.ix.select_sql(&schema, &stale.join(" OR "), None, rk);
            let (_, _, written) = self.reconcile(&s, &sql, false, cancel).await?;
            self.scan.repaired += written;
            self.live.spend(stale.len() as u64, now);
        }
        if read < n {
            // The shard's end: chunks whose row is gone.
            let swept = self.sweep(&s, false).await?;
            if swept > 0 {
                metrics::rows("cleared", swept);
                tracing::warn!(tenant = %self.tenant, index = self.ix.row.id, %schema, swept, "retrieval index: the stale-hash check swept chunks of deleted rows");
            }
        }
        self.scan.advance(last, read, n);
        Ok(())
    }

    /// One fill chunk of the first shard not done; whether every shard is
    /// done.
    async fn backfill_step(&mut self, cancel: &sprig_common::CancellationToken) -> Result<bool> {
        let shards = self.shards().await?;
        let Some((s, _)) = shards
            .iter()
            .find(|(s, _)| {
                self.progress.get(&self.ix.schema(self.tenant, s)) != Some(&json!("done"))
            })
            .cloned()
        else {
            return Ok(true);
        };
        let n = self.backfill.take(CHUNK, Instant::now());
        if n == 0 {
            return Ok(false);
        }
        let schema = self.ix.schema(self.tenant, &s);
        let kc = &self.ix.row.key_columns;
        let pred = match self.progress.get(&schema).and_then(Value::as_array) {
            Some(after) if after.len() == kc.len() => {
                let cols: Vec<String> = kc.iter().map(|c| quote(&c.name)).collect();
                let vals: Vec<String> = after
                    .iter()
                    .map(|v| v.as_str().unwrap_or("NULL").to_owned())
                    .collect();
                format!("({}) > ({})", cols.join(", "), vals.join(", "))
            }
            _ => "TRUE".to_owned(),
        };
        let rk = self.rk.contains(&schema);
        let sql = self.ix.select_sql(&schema, &pred, Some(n), rk);
        let (last, read, _) = self.reconcile(&s, &sql, false, cancel).await?;
        self.live.spend(read, Instant::now());
        let pos = match last {
            Some(l) if read >= n => json!(l),
            _ => {
                let swept = self.sweep(&s, false).await?;
                metrics::rows("cleared", swept);
                json!("done")
            }
        };
        self.progress.insert(schema, pos);
        self.save_progress().await?;
        Ok(false)
    }
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

/// Loads index `id` and its table: `Ok(None)` when either is gone.
async fn load(env: &EmbedEnv, tenant: TenantId, id: u64) -> Result<Option<Index>> {
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
    let chunking = Chunking::from_catalog(&row.chunking)
        .unwrap_or(Chunking::Tokens(sprig_sql::retrieval::DEFAULT_CHUNK_TOKENS));
    Ok(Some(Index {
        row,
        db,
        table,
        chunking,
    }))
}

/// The job's end when its index or table is gone: the chunk tables, the
/// row and the subscription go.
async fn forget(env: &EmbedEnv, tenant: TenantId, name: &str, id: u64) {
    let table = retrieval::table_name(id);
    let prefix = format!("t{}\\_d", tenant.0);
    if let Ok(clusters) = env.logic.pools.sweepable().await {
        for cluster in clusters {
            let Ok(pool) = env.logic.pools.writer(&cluster).await else {
                continue;
            };
            let Ok(mut c) = pool.get_conn_bounded().await else {
                continue;
            };
            let schemas: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE \
                     CONCAT(?, '%') AND TABLE_NAME = ?",
                    (prefix.as_str(), table.as_str()),
                )
                .await
                .unwrap_or_default();
            for s in schemas {
                let _ = c
                    .query_drop(format!(
                        "DROP TABLE IF EXISTS {}.{}",
                        quote(&s),
                        quote(&table)
                    ))
                    .await;
            }
        }
    }
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

async fn mark_failed(env: &EmbedEnv, tenant: TenantId, id: u64, msg: &str) {
    if let Ok(ep) = env.logic.locator.writer(tenant).await
        && let Ok(mut c) = ep.pool.get_conn_bounded().await
    {
        let _ = Store::new(ep.store.schema())
            .set_state(&mut c, id, State::Failed, Some(msg))
            .await;
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
    let id = owner["retrieval"]
        .as_u64()
        .ok_or_else(|| SprigError::internal("owner_ref.retrieval is missing"))?;
    let Some(ix) = load(env, tenant, id).await? else {
        forget(env, tenant, name, id).await;
        return Ok(json!({"stopped": "the index or its table is gone"}));
    };
    // The model, loaded once per process.
    let model = loop {
        if !ix.row.model.is_local() {
            let msg = format!(
                "model '{}' is a provider's; retrieval indexes use local models for now",
                ix.row.model.alias
            );
            mark_failed(env, tenant, id, &msg).await;
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
                tracing::warn!(%tenant, index = id, %msg, "retrieval index waits for its model");
                mark_failed(env, tenant, id, &msg).await;
                if !pause(&cx.cancel, Duration::from_secs(60)).await {
                    return Ok(json!({"stopped": "cancelled"}));
                }
            }
        }
    };
    let settings = &env.logic.settings;
    let progress: BTreeMap<String, Value> = ix
        .row
        .progress
        .as_object()
        .map(|o| o.clone().into_iter().collect())
        .unwrap_or_default();
    let mut job = Job {
        env,
        tenant,
        model,
        batch: usize::try_from(settings.get(&embedding::BATCH_ROWS)).unwrap_or(64),
        max_bytes: usize::try_from(settings.get(&embedding::MAX_INPUT_BYTES)).unwrap_or(65_536),
        max_chunks: usize::try_from(settings.get(&retrieval::MAX_CHUNKS_PER_ROW)).unwrap_or(64),
        live: Throttle::new(settings.tenant_get(tenant, &embedding::ROWS_PER_S)),
        backfill: Throttle::new(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S)),
        progress,
        rows: BTreeMap::new(),
        rows_saved: None,
        shards: None,
        rk: HashSet::new(),
        map_changes: env.maps.changes(),
        orphans: HashMap::new(),
        indexed: 0,
        kinds: causal::Kinds::default(),
        scan: Scan::new(
            settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S),
            Instant::now(),
        ),
        ix,
    };
    // A failed index that loads again carries on.
    if job.ix.row.state == State::Failed {
        let back = if job.progress.is_empty() || job.ix.row.rows_indexed == 0 {
            State::Building
        } else if job.all_done() {
            State::Ready
        } else {
            State::Rebuilding
        };
        job.set_state(back, None).await?;
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
    let mut feed = Pump::start(tail, ROUND, "retrieval worker");
    let job_key = format!("{tenant}/{name}");
    tracing::info!(%tenant, index = id, db = %job.ix.db, table = %job.ix.table, state = job.ix.row.state.as_str(), job = %cx.job_id, "retrieval index job started");
    let mut last_token = None;
    let mut pending_ack = 0u64;
    let mut acked = Instant::now();
    let mut checked = Instant::now();
    let mut reload = false;
    let mut attempt = 0u32;
    let object = job.ix.row.object_id;
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
                tracing::debug!(%tenant, index = id, passed = d.reached(), "retrieval index job drained at its stop");
                break;
            }
        }
        // While draining, rounds stop at `hard`, not at the cancel.
        let stop = if drain.is_some() { &hard } else { &cx.cancel };
        if drain.is_none() && (reload || checked.elapsed() > Duration::from_secs(5)) {
            reload = false;
            checked = Instant::now();
            match load(env, tenant, id).await {
                Ok(None) => {
                    backlog_set(&job_key, 0);
                    forget(env, tenant, name, id).await;
                    tracing::info!(%tenant, index = id, "retrieval index or its table is gone; job ends");
                    return Ok(
                        json!({"stopped": "the index or its table is gone", "indexed": job.indexed}),
                    );
                }
                Ok(Some(mut x)) => {
                    if x.table != job.ix.table {
                        job.shards = None;
                    }
                    // The job's own state and position are newer than the
                    // row's between saves.
                    x.row.state = job.ix.row.state;
                    job.ix = x;
                }
                Err(e) => {
                    tracing::warn!(%tenant, index = id, error = %e, "retrieval index row unreadable");
                }
            }
            job.live
                .set_rate(settings.tenant_get(tenant, &embedding::ROWS_PER_S));
            job.backfill
                .set_rate(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S));
            job.scan
                .set_rate(settings.tenant_get(tenant, &embedding::BACKFILL_ROWS_PER_S));
            job.max_chunks =
                usize::try_from(settings.get(&retrieval::MAX_CHUNKS_PER_ROW)).unwrap_or(64);
            let _ = cx
                .progress(json!({"indexed": job.indexed, "state": job.ix.row.state.as_str()}))
                .await;
        }
        if acked.elapsed() > Duration::from_secs(1)
            && let Some(t) = last_token.take()
        {
            subs.ack(tenant, name, &t, pending_ack, now_ms()).await?;
            pending_ack = 0;
            acked = Instant::now();
        }
        // The shards (and their chunk tables) first.
        if let Err(e) = job.shards().await {
            tracing::warn!(%tenant, index = id, error = %e, "retrieval index: listing shards failed; retrying");
            // A stop cuts the wait short; its drain starts (or ends) above.
            let _ = pause(stop, Duration::from_secs(1)).await;
            continue;
        }
        let filling = !job.all_done();
        let wait = if filling && job.backfill.wait() == Duration::ZERO {
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
        let watched = job.ix.watched();
        let mut keys: RoundKeys = BTreeMap::new();
        let mut truncated: BTreeSet<ShardLoc> = BTreeSet::new();
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
            if !wants(e.op, e.delta.as_deref(), &watched) {
                continue;
            }
            let loc = ShardLoc {
                group: e.shard.group.0,
                shard: e.shard.shard.0,
            };
            if e.op == Op::Truncate {
                truncated.insert(loc);
                continue;
            }
            let k = format!("{:?}", e.key.0);
            if seen.insert((loc.clone(), k)) {
                keys.entry(loc).or_default().push(e.key.0.clone());
            }
        }
        let n_keys: u64 = keys.values().map(|v| v.len() as u64).sum();
        backlog_set(&job_key, n_keys);
        if n_keys > 0 || !truncated.is_empty() {
            loop {
                let mut failed = job.relocate(&mut keys, &mut truncated).await.err();
                if failed.is_none() {
                    for s in &truncated {
                        if let Err(e) = job.sweep(s, true).await {
                            failed = Some(e);
                            break;
                        }
                    }
                }
                if failed.is_none() {
                    for (s, ks) in &keys {
                        if let Err(e) = job.reconcile_keys(s, ks, stop).await {
                            failed = Some(e);
                            break;
                        }
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
                    mark_failed(env, tenant, id, &e.to_string()).await;
                    tracing::warn!(%tenant, index = id, error = %e, "retrieval index failed; retrying in 30 s");
                    if !pause(stop, Duration::from_secs(30)).await {
                        break 'serve;
                    }
                    return Err(e);
                }
                job.shards = None;
                tracing::warn!(%tenant, index = id, error = %e, attempt, "retrieval round failed; retrying");
                let wait = Duration::from_millis(200 * u64::from(attempt.min(25)));
                if !pause(stop, wait).await {
                    break 'serve;
                }
            }
            job.live.spend(n_keys, Instant::now());
            if job.rows_unsaved() {
                let _ = job.save_progress().await;
            }
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
        // Fill between rounds (not while draining).
        if drain.is_some() {
            continue;
        }
        if filling {
            match job.backfill_step(&cx.cancel).await {
                Ok(true) => {
                    if job.ix.row.state != State::Ready {
                        job.set_state(State::Ready, None).await?;
                        tracing::info!(%tenant, index = id, db = %job.ix.db, table = %job.ix.table, indexed = job.indexed, "retrieval index filled: ready");
                    }
                }
                Ok(false) => {}
                Err(e) => {
                    job.shards = None;
                    tracing::warn!(%tenant, index = id, error = %e, "retrieval fill step failed; retrying");
                    if e.code() == Some(SprigCode::EmbeddingFailed) {
                        mark_failed(env, tenant, id, &e.to_string()).await;
                        if !pause(&cx.cancel, Duration::from_secs(30)).await {
                            // The stop: its drain starts above.
                            continue;
                        }
                        return Err(e);
                    }
                    // A stop cuts the wait short; its drain starts above.
                    let _ = pause(&cx.cancel, Duration::from_secs(1)).await;
                }
            }
        } else if job.ix.row.state != State::Ready {
            job.set_state(State::Ready, None).await?;
        } else if let Err(e) = job.scan_step(&cx.cancel).await {
            // The next window tries again; a failed index shows in the
            // rounds.
            job.shards = None;
            tracing::debug!(%tenant, index = id, error = %e, "stale-hash check step failed; retrying");
        }
    }
    backlog_set(&job_key, 0);
    if let Some(t) = last_token {
        let _ = subs.ack(tenant, name, &t, pending_ack, now_ms()).await;
    }
    Ok(json!({"stopped": "cancelled", "indexed": job.indexed}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_are_cut_by_the_index_chunking() {
        let text = "One sentence here. ".repeat(200);
        let c = chunk_row(Chunking::Tokens(100), &text, Some("Title"), 64);
        assert!(c.len() > 3);
        assert!(c.iter().all(|(h, _)| h == "Title"));
        assert_eq!(chunk_row(Chunking::Tokens(100), &text, None, 2).len(), 2);
        let md = "Intro paragraph that says something useful about passwords.\n\n## Reset\n\nOpen settings, then choose reset password and follow the mail.\n";
        let h = chunk_row(Chunking::Headings, md, Some("Help"), 64);
        assert!(h.iter().all(|(head, _)| head.starts_with("Help")));
        assert!(chunk_row(Chunking::Tokens(100), "  ", None, 64).is_empty());
    }

    #[test]
    fn the_feed_events_that_matter() {
        let cols = vec!["body".to_owned(), "id".to_owned()];
        assert!(wants(Op::Insert, None, &cols));
        assert!(wants(Op::Delete, None, &cols));
        assert!(wants(Op::Truncate, None, &cols));
        assert!(wants(Op::Update, None, &cols));
        assert!(wants(Op::Update, Some(&["BODY".to_owned()]), &cols));
        assert!(!wants(Op::Update, Some(&["price".to_owned()]), &cols));
        assert_eq!(vec_text(&[0.5, -1.0]), "[0.5,-1]");
    }

    #[test]
    fn keys_of_a_shard_gone_go_to_the_live_ones() {
        let loc = |group, shard| ShardLoc { group, shard };
        let key = |n: i64| vec![("id".to_owned(), sprig_binlog::Value::Int(n))];
        let mut keys: RoundKeys = BTreeMap::new();
        keys.insert(loc(3, 1), vec![key(1), key(2)]);
        keys.insert(loc(3, 4), vec![key(9)]);
        let mut truncated: BTreeSet<ShardLoc> = [loc(3, 1)].into();
        // Shard 1 was split into 4 and 5.
        let live = [loc(3, 4), loc(3, 5)];
        spread(&mut keys, &mut truncated, &live);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[&loc(3, 4)].len(), 3);
        assert_eq!(keys[&loc(3, 5)], vec![key(1), key(2)]);
        assert_eq!(truncated, live.clone().into());
        // Nothing stray: nothing moves.
        let mut t2 = BTreeSet::new();
        let before = keys.clone();
        spread(&mut keys, &mut t2, &live);
        assert_eq!(keys, before);
        assert!(t2.is_empty());
    }
}
