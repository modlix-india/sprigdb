//! The runner that fills embedded columns of engine tables, in the
//! gateway that holds the node's engine.
//!
//! ```text
//! spawn(services, login, sql engine): one task per process
//!   every tick (1 s, or at once after a declaration here), for each tenant
//!   whose catalog this process has loaded and whose unit is led here (or
//!   has no single leader): its EngineColumn records (read again every
//!   few seconds), and for each column one round:
//!     1. changes: the table's row changes in the node's change log after
//!        the column's token (from its declaration time at first), at most
//!        a few pages: the keys of inserted and updated rows (a deleted
//!        row's vector went with it)
//!     2. backfill, until done: the next page of keys after the last one,
//!        in key order
//!     3. stale check, once backfilled, between rounds and throttled: a
//!        window of keys whose hash isn't their source's
//!     4. those keys' rows read again (source, MD5 of the source, hash,
//!        vector is NULL), as the column's declarer, exempt from row-level
//!        security, in a fresh statement; a row whose hash is its source's
//!        is skipped; the rest embedded (the bundled model) and written in
//!        one transaction, each UPDATE guarded by the MD5 of the source it
//!        was made from, so a row changed meanwhile keeps the newer
//!        source's turn
//!     5. then, in one record transaction, the token after the changes
//!        read, the backfill's place, the counts and the state
//! ```
//!
//! **Why a read sees at least the change it follows.** The change log is
//! written by the engine's apply observer after the store applied the
//! entry, and the round's rows are read in a statement begun after the
//! page was read, so the read view holds that change or a newer one.
//! Nothing is acknowledged (the token kept) before the round's writes
//! committed; a crash repeats the round and the hash skips rows already
//! done. A change log place that is no longer held (retention) starts the
//! column over: changes from now, and a full backfill pass, which skips
//! every row already current.
//!
//! Failures: a source the table can no longer evaluate (a column it read
//! was dropped: 1054), a declarer without the privileges or gone, or no
//! model: the column is `failed` with the reason and tried again every
//! minute. A table that is gone (dropped) takes its records with it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use sprig_ai::embedding::store::{EngineColumn, EngineColumns, State};
use sprig_ai::embedding::{self, metrics};
use sprig_catalog::TenantCatalog;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_doc::engine::SqlRunner as _;
use sprig_sql::embedcol::{self, quote};
use tokio::sync::Notify;

use super::engine::table_by_object;
use crate::engine::table::{Family, TableDef};
use crate::http::sql::SqlEngine;
use crate::mongo::sql::SqlSession;
use crate::node_engine::{self, UnitLeader};
use crate::table_changes::{self, Watched};
use crate::tenant::TenantServices;

/// How long a tenant's list of columns, as read, stands.
const FRESH: Duration = Duration::from_secs(3);
/// Change-log pages one round reads at most.
const MAX_PAGES: usize = 8;
/// Keys one round handles at most (changes, backfill and stale check).
const MAX_KEYS: usize = 2_048;
/// A failed column is tried again after this.
const RETRY: Duration = Duration::from_secs(60);
/// A stale check's window of keys.
const STALE_WINDOW: usize = 500;
/// Rest between two stale-check passes over a table.
const STALE_REST: Duration = Duration::from_secs(60);
/// A column whose table isn't in the catalog for this long (and that was
/// declared at least this long ago) is taken as dropped.
const GONE_AFTER: Duration = Duration::from_secs(30);

fn notify() -> &'static Notify {
    static N: OnceLock<Notify> = OnceLock::new();
    N.get_or_init(Notify::new)
}

/// Starts a round soon (a column was declared in this process).
pub(crate) fn wake() {
    notify().notify_one();
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(i64::MAX)
}

/// A character string literal as its bytes in hexadecimal with the
/// connection's character set (`_utf8mb4 X'..'`): no quote or escape to
/// get wrong, whatever `sql_mode` says (`NO_BACKSLASH_ESCAPES`).
fn lit(s: &[u8]) -> String {
    format!("_utf8mb4 {}", hex_lit(s))
}

/// A number's text as a literal, when it is one (digits, a sign, a point
/// and an exponent only).
fn num_lit(s: &[u8]) -> Option<String> {
    let ok = !s.is_empty()
        && s.iter()
            .all(|b| matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'));
    ok.then(|| String::from_utf8_lossy(s).into_owned())
}

/// How a key column's values are written as literals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    /// Integers and decimals: as they are.
    Number,
    /// Binary strings: `X'..'`.
    Binary,
    /// Everything else (text, dates and times, `ENUM`): a character string.
    Text,
}

impl KeyKind {
    fn of(f: Family) -> Self {
        match f {
            Family::Integer | Family::Decimal => KeyKind::Number,
            Family::Binary => KeyKind::Binary,
            _ => KeyKind::Text,
        }
    }

    /// The literal of a value's text (as a statement returns it).
    fn literal(self, v: &[u8]) -> Option<String> {
        match self {
            KeyKind::Number => num_lit(v),
            KeyKind::Binary => Some(hex_lit(v)),
            KeyKind::Text => Some(lit(v)),
        }
    }
}

fn hex_lit(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s.push('\'');
    s
}

/// What the runner needs of a table: its names and key.
#[derive(Debug, Clone)]
struct Table {
    db: String,
    name: String,
    /// Key columns: name and how its values are written.
    key: Vec<(String, KeyKind)>,
    /// Every column's name.
    columns: Vec<String>,
    def: Option<Arc<TableDef>>,
}

impl Table {
    fn of(cat: &TenantCatalog, def: &Arc<TableDef>) -> Option<Self> {
        let db = cat.databases.get(def.db)?.name.clone();
        let key = def
            .pk
            .iter()
            .map(|&i| {
                let c = def.columns.get(i)?;
                Some((c.name.clone(), KeyKind::of(Family::of(c.ty))))
            })
            .collect::<Option<Vec<_>>>()?;
        if def.hidden || key.is_empty() {
            return None;
        }
        Some(Self {
            db,
            name: def.name.clone(),
            key,
            columns: def.columns.iter().map(|c| c.name.clone()).collect(),
            def: Some(Arc::clone(def)),
        })
    }

    fn qualified(&self) -> String {
        format!("{}.{}", quote(&self.db), quote(&self.name))
    }

    fn key_list(&self) -> String {
        self.key
            .iter()
            .map(|(n, _)| quote(n))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// A key's literals from a row's text values.
    fn literals(&self, vals: &[Option<Vec<u8>>]) -> Option<Vec<String>> {
        self.key
            .iter()
            .zip(vals)
            .map(|((_, kind), v)| kind.literal(v.as_deref()?))
            .collect()
    }

    /// A key's literals from a change event's key (JSON), `None` when it
    /// can't stand for the key (a key the change reader couldn't decode).
    fn literals_of_json(&self, key: &serde_json::Value) -> Option<Vec<String>> {
        let o = key.as_object()?;
        self.key
            .iter()
            .map(|(n, kind)| {
                let v = o.iter().find(|(k, _)| k.eq_ignore_ascii_case(n))?.1;
                match (v, kind) {
                    (serde_json::Value::Number(x), _) => num_lit(x.to_string().as_bytes()),
                    (serde_json::Value::String(s), KeyKind::Binary) => Some(hex_lit(
                        &base64::engine::general_purpose::STANDARD.decode(s).ok()?,
                    )),
                    (serde_json::Value::String(s), k) => k.literal(s.as_bytes()),
                    _ => None,
                }
            })
            .collect()
    }

    /// `(k1 = a AND k2 = b) OR ...` for `keys`.
    fn matching(&self, keys: &[Vec<String>]) -> String {
        keys.iter()
            .map(|k| self.equal(k))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn equal(&self, k: &[String]) -> String {
        let parts: Vec<String> = self
            .key
            .iter()
            .zip(k)
            .map(|((n, _), v)| format!("{} = {v}", quote(n)))
            .collect();
        format!("({})", parts.join(" AND "))
    }

    /// Rows after key `after` in key order: `(a > x) OR (a = x AND b > y)`.
    fn after(&self, after: &[String]) -> String {
        let mut ors = Vec::new();
        for i in 0..self.key.len().min(after.len()) {
            let mut ands: Vec<String> = (0..i)
                .map(|j| format!("{} = {}", quote(&self.key[j].0), after[j]))
                .collect();
            ands.push(format!("{} > {}", quote(&self.key[i].0), after[i]));
            ors.push(format!("({})", ands.join(" AND ")));
        }
        format!("({})", ors.join(" OR "))
    }

    fn order(&self) -> String {
        self.key_list()
    }

    /// Whether the table still has the column's three columns.
    fn has_columns(&self, column: &str) -> bool {
        let has = |n: &str| self.columns.iter().any(|c| c.eq_ignore_ascii_case(n));
        has(column)
            && has(&embedcol::src_column(column, None))
            && has(&embedcol::hash_column(column, None))
    }
}

/// The declarer's session and its row-level security exemption, which
/// lasts as long as the session.
struct Sess {
    sql: SqlSession,
    _exempt: crate::tenant::rls::ExemptGuard,
}

/// What the runner keeps of one column between rounds.
#[derive(Default)]
struct Live {
    session: Option<Sess>,
    failed_at: Option<Instant>,
    missing_since: Option<Instant>,
    /// The stale check's place, and when its last pass ended.
    stale_after: Option<Vec<String>>,
    stale_rest_from: Option<Instant>,
    /// The throttle: rows that may be handled now (`ai.embedding.rows_per_s`
    /// a second, at most one second's worth saved; below zero after a
    /// round that handled more), and when it was last topped up.
    allowance: Option<(Instant, f64)>,
    /// The vectors this runner wrote, by key (base64, as change events
    /// show them), until their change event is read: a write that only
    /// replaced a vector (same source, same hash) is the runner's own, not
    /// a user's.
    wrote: HashMap<Vec<String>, String>,
}

/// Vectors remembered per column at most ([`Live::wrote`]).
const WROTE_MAX: usize = 10_000;

/// The runner's state.
struct Runner {
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    live: HashMap<(u64, u64), Live>,
    known: HashMap<u64, (Instant, u64, Vec<EngineColumn>)>,
}

/// Starts the runner; it ends with `stop`.
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let mut loads = svc.cache.loads();
    tokio::spawn(async move {
        let mut r = Runner {
            svc,
            login,
            engine,
            live: HashMap::new(),
            known: HashMap::new(),
        };
        let mut tenants: BTreeSet<u64> = BTreeSet::new();
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                l = loads.recv() => {
                    if let Ok(l) = l {
                        tenants.insert(l.tenant.0);
                    }
                    continue;
                }
                () = notify().notified() => {
                    r.known.clear();
                }
                _ = timer.tick() => {}
            }
            let mut backlog = 0u64;
            for &t in &tenants {
                backlog += r.tenant(TenantId(t)).await;
            }
            metrics::backlog(backlog);
        }
    })
}

impl Runner {
    /// One look at tenant `t`'s columns; the keys still waiting.
    async fn tenant(&mut self, t: TenantId) -> u64 {
        if !matches!(
            node_engine::tenant_leader(t.0),
            UnitLeader::Here | UnitLeader::Any
        ) {
            self.known.remove(&t.0);
            self.live.retain(|(tt, _), _| *tt != t.0);
            return 0;
        }
        let Some(cat) = self.svc.cache.cached(t) else {
            return 0;
        };
        if crate::engine::catalog::of(&cat).is_none() {
            return 0;
        }
        let stale = self
            .known
            .get(&t.0)
            .is_none_or(|k| k.1 != cat.version || k.0.elapsed() >= FRESH);
        if stale {
            match self.read_columns(t).await {
                Ok(list) => {
                    self.known.insert(t.0, (Instant::now(), cat.version, list));
                }
                Err(e) => {
                    tracing::debug!(tenant = %t, error = %e, "embedded columns not read");
                    return 0;
                }
            }
        }
        let cols = self
            .known
            .get(&t.0)
            .map(|k| k.2.clone())
            .unwrap_or_default();
        let ids: HashSet<(u64, u64)> = cols.iter().map(|c| (t.0, c.id)).collect();
        self.live.retain(|k, _| k.0 != t.0 || ids.contains(k));
        let mut waiting = 0;
        for c in cols {
            waiting += self.column(t, &cat, c).await;
        }
        waiting
    }

    async fn read_columns(&mut self, t: TenantId) -> Result<Vec<EngineColumn>> {
        let mut tx = super::super::ai_fn::writers(&self.svc).begin(t).await?;
        let r = EngineColumns.all(&mut *tx).await;
        tx.rollback().await?;
        r
    }

    /// One round of column `c`; the keys still waiting.
    async fn column(&mut self, t: TenantId, cat: &TenantCatalog, c: EngineColumn) -> u64 {
        let key = (t.0, c.id);
        let live = self.live.entry(key).or_default();
        if c.state == State::Failed && live.failed_at.is_some_and(|f| f.elapsed() < RETRY) {
            return 0;
        }
        let table = table_by_object(cat, c.object_id)
            .and_then(|d| Table::of(cat, &d))
            .filter(|tb| tb.has_columns(&c.column));
        let Some(table) = table else {
            let since = *live.missing_since.get_or_insert_with(Instant::now);
            let old = now_ms().saturating_sub(c.from_ms)
                > i64::try_from(GONE_AFTER.as_millis()).unwrap_or(i64::MAX);
            if since.elapsed() >= GONE_AFTER && old {
                self.forget(t, &c).await;
            }
            return 0;
        };
        live.missing_since = None;
        match Box::pin(self.round(t, cat, &c, &table)).await {
            Ok(waiting) => waiting,
            Err(e) => {
                let retryable = e.retryable;
                if let Some(l) = self.live.get_mut(&key) {
                    l.session = None;
                    if !retryable {
                        l.failed_at = Some(Instant::now());
                    }
                }
                if retryable {
                    tracing::debug!(tenant = %t, column = %c.column, error = %e, "embedded column round failed; tried again");
                } else {
                    tracing::warn!(tenant = %t, column = %c.column, error = %e, "embedded column failed");
                    let _ = self.set_failed(t, c.id, &e.to_string()).await;
                }
                0
            }
        }
    }

    /// Removes the records of a column whose table is gone.
    async fn forget(&mut self, t: TenantId, c: &EngineColumn) {
        let r: Result<()> = async {
            let mut tx = super::super::ai_fn::writers(&self.svc).begin(t).await?;
            EngineColumns.delete(&mut *tx, c.id).await?;
            tx.commit().await
        }
        .await;
        match r {
            Ok(()) => {
                tracing::info!(tenant = %t, column = %c.column, embedding = c.id, "embedded column's table is gone; its record removed");
                self.live.remove(&(t.0, c.id));
                self.known.remove(&t.0);
            }
            Err(e) => {
                tracing::debug!(tenant = %t, error = %e, "embedded column record not removed");
            }
        }
    }

    async fn set_failed(&mut self, t: TenantId, id: u64, why: &str) -> Result<()> {
        let mut tx = super::super::ai_fn::writers(&self.svc).begin(t).await?;
        if let Some(mut c) = EngineColumns.get_for_update(&mut *tx, id).await? {
            c.state = State::Failed;
            c.last_error = Some(why.chars().take(1000).collect());
            EngineColumns.put(&mut *tx, &c).await?;
        }
        tx.commit().await
    }

    /// The declarer's session for column `c`, opened on first use.
    async fn session(&mut self, t: TenantId, c: &EngineColumn, table: &Table) -> Result<()> {
        let key = (t.0, c.id);
        if self.live.get(&key).is_some_and(|l| l.session.is_some()) {
            return Ok(());
        }
        let name = crate::inherited::tenant_name(&self.svc, t).await?;
        let principal =
            crate::triggers::principal_of(&self.login, t.0, &name, c.created_by).await?;
        let sql = SqlSession::open(
            &self.engine.sessions,
            &self.engine.executor,
            &principal,
            "embedding",
            0,
            SocketAddr::from(([127, 0, 0, 1], 0)),
            None,
            64 << 20,
        )?;
        // The column holds every row's vector; readers' own policies apply
        // when they read it.
        let exempt = crate::tenant::rls::exempt_internal(
            sql.session_id(),
            t.0,
            "embedded column",
            &format!("{}.{}", table.db, table.name),
        );
        let l = self.live.entry(key).or_default();
        l.session = Some(Sess {
            sql,
            _exempt: exempt,
        });
        Ok(())
    }

    async fn query(&mut self, key: (u64, u64), sql: &str) -> Result<sprig_doc::engine::Rows> {
        let s = self
            .live
            .get_mut(&key)
            .and_then(|l| l.session.as_mut().map(|s| &mut s.sql))
            .ok_or_else(|| SprigError::internal("the embedded column has no session"))?;
        s.query(sql).await.map_err(|e| sql_error(&e))
    }

    async fn exec(&mut self, key: (u64, u64), sql: &str) -> Result<u64> {
        let s = self
            .live
            .get_mut(&key)
            .and_then(|l| l.session.as_mut().map(|s| &mut s.sql))
            .ok_or_else(|| SprigError::internal("the embedded column has no session"))?;
        s.exec(sql)
            .await
            .map(|d| d.affected)
            .map_err(|e| sql_error(&e))
    }

    /// One round (see the module docs); the keys still waiting.
    #[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the round's steps in order
    async fn round(
        &mut self,
        t: TenantId,
        cat: &TenantCatalog,
        c: &EngineColumn,
        table: &Table,
    ) -> Result<u64> {
        let key = (t.0, c.id);
        // The column fills every row: its declarer must not be limited by
        // the table's row-level security (policies added later included).
        if super::engine::bound_by_rls(cat, c.created_by, c.object_id) {
            return Err(super::engine::rls_refused(&table.name));
        }
        let (rows_per_s, backfill_per_s, batch, max_bytes) = {
            let sctx = cat.setting_ctx(&self.svc.dc).with_db(DbId(c.db_id));
            let get = |s: &'static sprig_settings::Setting<u64>| {
                self.svc.settings.get(s, &sctx, Some(&cat.settings))
            };
            (
                get(&embedding::ROWS_PER_S).max(1),
                get(&embedding::BACKFILL_ROWS_PER_S).max(1),
                usize::try_from(get(&embedding::BATCH_ROWS))
                    .unwrap_or(64)
                    .max(1),
                usize::try_from(get(&embedding::MAX_INPUT_BYTES)).unwrap_or(65_536),
            )
        };
        // The throttle: a bucket of `rows_per_s` a second. A round always
        // handles every key it read (its place moves past them), so the
        // bucket may go below zero; the next rounds wait until it refills.
        #[allow(clippy::cast_precision_loss)] // rates far below 2^52
        let budget = {
            let rate = rows_per_s as f64;
            let l = self.live.entry(key).or_default();
            let (at, left) = l.allowance.unwrap_or((Instant::now(), rate));
            let left = (left + at.elapsed().as_secs_f64() * rate).min(rate);
            l.allowance = Some((Instant::now(), left));
            left
        };
        if budget < 1.0 {
            return Ok(1);
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // 1 ..= rows_per_s
        let budget = budget as u64;
        let mut keys: Vec<Vec<String>> = Vec::new();
        let mut seen: HashSet<Vec<String>> = HashSet::new();
        let mut push = |k: Vec<String>, keys: &mut Vec<Vec<String>>| {
            if seen.insert(k.clone()) {
                keys.push(k);
            }
        };
        // 1. Changes.
        let mut token = c.token.clone();
        // When a lost change-log place was found: changes from then on are
        // followed, and a full pass covers what came before.
        let mut restart: Option<i64> = None;
        // Rows whose vector a user wrote (the vector changed, its hash
        // didn't): embedded again whatever their hash says.
        let mut forced: HashSet<Vec<String>> = HashSet::new();
        let vec_col = c.column.clone();
        let hash_col = embedcol::hash_column(&c.column, None);
        let mut full_check = false;
        let mut caught_up = false;
        if let (Some(log), Ok(eng), Some(def)) = (
            crate::changelog::changes(),
            crate::engine::engine(),
            table.def.as_ref(),
        ) {
            let object = u32::try_from(c.object_id)
                .map_err(|_| SprigError::internal("an engine table's object id"))?;
            let tables: HashMap<u32, Watched> = std::iter::once((
                object,
                Watched {
                    def: Arc::clone(def),
                    db: table.db.clone(),
                },
            ))
            .collect();
            let consumer = format!("embedding:{}:{}", t.0, c.id);
            for page in 0..MAX_PAGES {
                let from_ms = if token.is_none() && page == 0 {
                    c.from_ms.max(1)
                } else {
                    0
                };
                match table_changes::read(
                    eng,
                    &log,
                    t.0,
                    &tables,
                    token.as_deref(),
                    from_ms,
                    table_changes::PAGE_BYTES,
                    Duration::ZERO,
                    &consumer,
                )
                .await
                {
                    Ok(b) => {
                        for e in &b.events {
                            if e.op == "d" {
                                continue;
                            }
                            match table.literals_of_json(&e.key) {
                                Some(k) => {
                                    let ours = self.live.get_mut(&key).and_then(|l| {
                                        let w = l.wrote.remove(&k)?;
                                        Some(after_vector(e, &vec_col) == Some(w))
                                    });
                                    if ours != Some(true)
                                        && user_wrote_vector(e, &vec_col, &hash_col)
                                    {
                                        forced.insert(k.clone());
                                    }
                                    push(k, &mut keys);
                                }
                                None => full_check = true,
                            }
                        }
                        token = Some(b.next);
                        if b.at_end {
                            caught_up = true;
                            break;
                        }
                        if keys.len() >= MAX_KEYS {
                            break;
                        }
                    }
                    Err(e) if e.code() == Some(SprigCode::SinkSourceLost) => {
                        tracing::warn!(tenant = %t, column = %c.column, error = %e, "the embedded column's change-log place is gone; starting over with a full pass");
                        restart = Some(now_ms());
                        token = None;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            // No change log here: only the stale check finds changes.
            full_check = true;
            caught_up = true;
        }
        // 2. Backfill.
        let mut backfilled = c.backfilled && restart.is_none();
        let mut backfill_after = if restart.is_some() {
            None
        } else {
            c.backfill_after.clone()
        };
        if !backfilled {
            self.session(t, c, table).await?;
            let room = usize::try_from(budget.min(backfill_per_s))
                .unwrap_or(batch)
                .saturating_sub(keys.len())
                .clamp(1, MAX_KEYS);
            let mut sql = format!("SELECT {} FROM {}", table.key_list(), table.qualified());
            if let Some(a) = &backfill_after {
                sql.push_str(" WHERE ");
                sql.push_str(&table.after(a));
            }
            let _ = write!(sql, " ORDER BY {} LIMIT {room}", table.order());
            let rows = self.query(key, &sql).await?;
            if rows.len() < room {
                backfilled = true;
            }
            for r in &rows {
                if let Some(k) = table.literals(r) {
                    backfill_after = Some(k.clone());
                    push(k, &mut keys);
                }
            }
        }
        // 3. Stale check, once backfilled and caught up with the changes
        //    (a row whose change is still to be handled isn't missed).
        if backfilled && caught_up && keys.is_empty() {
            let l = self.live.entry(key).or_default();
            let resting =
                l.stale_rest_from.is_some_and(|f| f.elapsed() < STALE_REST) && !full_check;
            if !resting {
                let after = l.stale_after.clone();
                self.session(t, c, table).await?;
                let window = usize::try_from((backfill_per_s / 10).max(1))
                    .unwrap_or(STALE_WINDOW)
                    .min(STALE_WINDOW);
                let mut sql = format!(
                    "SELECT {}, NOT ({} <=> UNHEX(MD5({}))) FROM {}",
                    table.key_list(),
                    quote(&embedcol::hash_column(&c.column, None)),
                    quote(&embedcol::src_column(&c.column, None)),
                    table.qualified()
                );
                if let Some(a) = &after {
                    sql.push_str(" WHERE ");
                    sql.push_str(&table.after(a));
                }
                let _ = write!(sql, " ORDER BY {} LIMIT {window}", table.order());
                let rows = self.query(key, &sql).await?;
                let n = table.key.len();
                let mut last = None;
                let mut repaired = 0u64;
                for r in &rows {
                    let Some(k) = table.literals(&r[..n.min(r.len())]) else {
                        continue;
                    };
                    last = Some(k.clone());
                    if r.get(n).and_then(Option::as_deref) == Some(&b"1"[..]) {
                        repaired += 1;
                        push(k, &mut keys);
                    }
                    if keys.len() >= MAX_KEYS {
                        break;
                    }
                }
                if repaired > 0 && c.state == State::Ready {
                    tracing::warn!(tenant = %t, column = %c.column, rows = repaired, "the stale-hash check found rows whose changes the embedded column had missed");
                }
                let l = self.live.entry(key).or_default();
                if rows.len() < window {
                    l.stale_after = None;
                    l.stale_rest_from = Some(Instant::now());
                } else {
                    l.stale_after = last;
                }
            }
        }
        // 4. Read again, embed, write: every key read, so the place kept
        //    below covers them all.
        let waiting = if caught_up { 0 } else { keys.len() as u64 };
        let mut embedded = 0u64;
        let mut row_failures = 0u64;
        let mut row_error: Option<String> = None;
        if !keys.is_empty() {
            self.session(t, c, table).await?;
            for chunk in keys.chunks(batch) {
                match self
                    .embed_rows(key, c, table, chunk, &forced, max_bytes)
                    .await
                {
                    Ok(n) => embedded += n,
                    // A lock wait, a lost session: the whole round again.
                    Err(e) if e.retryable => return Err(e),
                    // The model, the declarer's rights, a lost column: the
                    // column's problem, whatever the batch's size; the
                    // column fails and its place stays.
                    Err(e) if !is_row_error(&e) => return Err(e),
                    // Something about one row (a constraint, a value that
                    // doesn't fit): row by row, the failing rows passed
                    // over and counted.
                    Err(first) => {
                        let mut failed = 0u64;
                        let mut last = first;
                        for k in chunk {
                            match self
                                .embed_rows(
                                    key,
                                    c,
                                    table,
                                    std::slice::from_ref(k),
                                    &forced,
                                    max_bytes,
                                )
                                .await
                            {
                                Ok(n) => embedded += n,
                                Err(e) if e.retryable || !is_row_error(&e) => return Err(e),
                                Err(e) => {
                                    failed += 1;
                                    tracing::warn!(tenant = %t, column = %c.column, key = ?k, error = %e, "an embedded column's row failed and was passed over");
                                    last = e;
                                }
                            }
                        }
                        row_failures += failed;
                        if failed > 0 {
                            row_error = Some(last.to_string());
                        }
                    }
                }
            }
            #[allow(clippy::cast_precision_loss)] // at most a few thousand keys
            let n = keys.len() as f64;
            if let Some(l) = self.live.get_mut(&key)
                && let Some(a) = &mut l.allowance
            {
                a.1 -= n;
            }
        }
        // 5. The place, after the writes.
        let changed = token != c.token
            || backfilled != c.backfilled
            || backfill_after != c.backfill_after
            || embedded > 0
            || row_failures > 0
            || c.state == State::Failed
            || (backfilled && c.state != State::Ready);
        if changed {
            let mut tx = super::super::ai_fn::writers(&self.svc).begin(t).await?;
            if let Some(mut row) = EngineColumns.get_for_update(&mut *tx, c.id).await? {
                row.token = token;
                if let Some(at) = restart {
                    row.from_ms = at;
                }
                row.backfilled = backfilled;
                row.backfill_after = backfill_after;
                row.rows_embedded = row.rows_embedded.saturating_add(embedded);
                row.rows_failed = row.rows_failed.saturating_add(row_failures);
                row.state = if backfilled {
                    State::Ready
                } else {
                    State::Backfilling
                };
                // The last row's error, or none once a round passes over no
                // row.
                row.last_error = row_error.as_ref().map(|e| e.chars().take(1000).collect());
                EngineColumns.put(&mut *tx, &row).await?;
                tx.commit().await?;
                if let Some(k) = self.known.get_mut(&t.0)
                    && let Some(x) = k.2.iter_mut().find(|x| x.id == row.id)
                {
                    *x = row;
                }
            } else {
                tx.rollback().await?;
            }
            if let Some(l) = self.live.get_mut(&key) {
                l.failed_at = None;
            }
        }
        Ok(waiting)
    }

    /// Embeds the rows of `keys` whose vector isn't their source's; the
    /// rows written.
    #[allow(clippy::too_many_lines)] // read, embed, write
    async fn embed_rows(
        &mut self,
        key: (u64, u64),
        c: &EngineColumn,
        table: &Table,
        keys: &[Vec<String>],
        forced: &HashSet<Vec<String>>,
        max_bytes: usize,
    ) -> Result<u64> {
        let src = quote(&embedcol::src_column(&c.column, None));
        let hash = quote(&embedcol::hash_column(&c.column, None));
        let vec = quote(&c.column);
        let n = table.key.len();
        let sql = format!(
            "SELECT {}, {src}, MD5({src}), LOWER(HEX({hash})), {vec} IS NULL FROM {} WHERE {}",
            table.key_list(),
            table.qualified(),
            table.matching(keys)
        );
        let rows = self.query(key, &sql).await?;
        let model = c.model.alias.as_str();
        let mut texts: Vec<String> = Vec::new();
        let mut todo: Vec<(Vec<String>, Option<String>, usize)> = Vec::new();
        let mut skipped = 0u64;
        for r in rows {
            let Some(k) = table.literals(&r[..n.min(r.len())]) else {
                continue;
            };
            let source = r.get(n).cloned().flatten();
            let md5 = r
                .get(n + 1)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned());
            let have = r
                .get(n + 2)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned());
            let vec_null = r.get(n + 3).and_then(Option::as_deref) == Some(&b"1"[..]);
            let blank = source
                .as_deref()
                .is_none_or(|s| String::from_utf8_lossy(s).trim().is_empty());
            if have == md5 && (!blank || vec_null) && !forced.contains(&k) {
                skipped += 1;
                continue;
            }
            if blank {
                todo.push((k, md5, usize::MAX));
                continue;
            }
            let s = String::from_utf8_lossy(source.as_deref().unwrap_or_default()).into_owned();
            let mut end = s.len().min(max_bytes);
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            texts.push(s[..end].to_owned());
            todo.push((k, md5, texts.len() - 1));
        }
        metrics::rows(model, "skipped", skipped);
        if todo.is_empty() {
            return Ok(0);
        }
        let vectors = if texts.is_empty() {
            Vec::new()
        } else {
            let started = Instant::now();
            let v = embed_local(texts, c.max_input_tokens).await?;
            metrics::seconds(model, started.elapsed().as_secs_f64());
            v
        };
        let mut stmts = Vec::with_capacity(todo.len());
        let mut written_vecs: Vec<Option<String>> = Vec::with_capacity(todo.len());
        for (k, md5, i) in &todo {
            let guard = match md5 {
                Some(m) => format!("MD5({src}) = '{m}'"),
                None => format!("{src} IS NULL"),
            };
            let set_hash = match md5 {
                Some(m) => format!("UNHEX('{m}')"),
                None => "NULL".to_owned(),
            };
            let bytes = vectors
                .get(*i)
                .map(|v| sprig_ai::functions::vector_bytes(v));
            written_vecs.push(
                bytes
                    .as_ref()
                    .map(|b| base64::engine::general_purpose::STANDARD.encode(b)),
            );
            let value = bytes.as_deref().map_or_else(|| "NULL".to_owned(), hex_lit);
            stmts.push(format!(
                "UPDATE {} SET {vec} = {value}, {hash} = {set_hash} WHERE {} AND {guard}",
                table.qualified(),
                table.equal(k)
            ));
        }
        self.exec(key, "START TRANSACTION").await?;
        let mut written = 0u64;
        let mut cleared = 0u64;
        let mut ours: Vec<(Vec<String>, String)> = Vec::new();
        for ((s, (k, _, i)), w) in stmts.iter().zip(&todo).zip(&written_vecs) {
            // Only the vector and its hash, with no trigger, validation or
            // `ON UPDATE CURRENT_TIMESTAMP` (super::write).
            let cols = vec![c.column.clone(), embedcol::hash_column(&c.column, None)];
            match super::write::scope(cols, self.exec(key, s)).await {
                Ok(n) => {
                    if n > 0 {
                        if *i == usize::MAX {
                            cleared += 1;
                        } else {
                            written += 1;
                        }
                        if let Some(w) = w {
                            ours.push((k.clone(), w.clone()));
                        }
                    }
                }
                Err(e) => {
                    let _ = self.exec(key, "ROLLBACK").await;
                    return Err(e);
                }
            }
        }
        self.exec(key, "COMMIT").await?;
        if let Some(l) = self.live.get_mut(&key) {
            if l.wrote.len() + ours.len() > WROTE_MAX {
                l.wrote.clear();
            }
            l.wrote.extend(ours);
        }
        let superseded = todo.len() as u64 - written - cleared;
        metrics::rows(model, "embedded", written);
        metrics::rows(model, "cleared", cleared);
        metrics::rows(model, "superseded", superseded);
        Ok(written + cleared)
    }
}

/// Whether change `e` is a write to the vector itself (the vector changed,
/// its hash didn't): the runner's own writes change both.
fn user_wrote_vector(e: &table_changes::Event, vec_col: &str, hash_col: &str) -> bool {
    let (Some(b), Some(a)) = (e.before.as_ref(), e.after.as_ref()) else {
        return false;
    };
    let get = |r: &serde_json::Value, n: &str| r.get(n).cloned().unwrap_or(serde_json::Value::Null);
    get(b, vec_col) != get(a, vec_col) && get(b, hash_col) == get(a, hash_col)
}

/// The vector in change `e`'s after-image, as the event shows it.
fn after_vector(e: &table_changes::Event, vec_col: &str) -> Option<String> {
    e.after.as_ref()?.get(vec_col)?.as_str().map(str::to_owned)
}

/// The bundled model's passages, cut to `max_tokens` when set.
async fn embed_local(texts: Vec<String>, max_tokens: Option<u32>) -> Result<Vec<Vec<f32>>> {
    let dir = crate::docs::service()
        .ok_or_else(|| {
            let mut e =
                embedding::failed("this gateway has no tenant logins, so no embedding model");
            e.retryable = true;
            e
        })?
        .model_dir();
    tokio::task::spawn_blocking(move || {
        let model = sprig_embed::local::shared(&dir)?;
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let out = match max_tokens {
            Some(n) => model.embed_passages_truncated(&refs, usize::try_from(n).unwrap_or(512)),
            None => model.embed_passages(&refs),
        };
        out.map(|v| v.into_iter().map(|x| x.to_vec()).collect())
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| embedding::failed(format!("the embedding task stopped: {e}")))?
    .map_err(|why| embedding::failed(format!("the embedding model: {why}")))
}

/// A statement's error as a gateway error: lock waits, deadlocks and lost
/// connections are tried again; the rest fail the column.
fn sql_error(e: &sprig_doc::engine::SqlError) -> SprigError {
    let code = e.errno.unwrap_or(0);
    // The statement's own number, so a row's failure tells itself from the
    // column's ([`row_error`]).
    let mut out = SprigError::standard(
        code,
        *b"HY000",
        format!("embedded column: {} ({code})", e.message),
    );
    // 1146: the table is going (a DROP); its record goes after a while.
    out.retryable = matches!(code, 0 | 1146 | 1205 | 1213 | 2013 | 39070 | 39071);
    out
}

/// Errors one row's values cause (a constraint, a value that doesn't
/// fit): that row is passed over. Anything else (the model, the
/// declarer's rights, a column the source lost) is the column's.
const ROW_ERRNOS: &[u16] = &[
    1048, 1062, 1264, 1265, 1292, 1366, 1406, 1451, 1452, 3819, 4025,
];

/// Whether `e` is one row's failure ([`ROW_ERRNOS`]).
fn is_row_error(e: &SprigError) -> bool {
    !e.retryable && ROW_ERRNOS.contains(&e.protocol().mysql_errno)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(key: &[(&str, KeyKind)]) -> Table {
        Table {
            db: "d".into(),
            name: "t".into(),
            key: key.iter().map(|(n, k)| ((*n).to_owned(), *k)).collect(),
            columns: vec!["a".into(), "b".into()],
            def: None,
        }
    }

    #[test]
    fn keys_become_literals_and_conditions() {
        let tb = t(&[("a", KeyKind::Text), ("b", KeyKind::Binary)]);
        let k = tb
            .literals(&[Some(br"it's \ x".to_vec()), Some(vec![0, 255])])
            .unwrap();
        // No quote or backslash in what is written, whatever sql_mode says.
        assert_eq!(
            k,
            vec![
                "_utf8mb4 X'69742773205C2078'".to_owned(),
                "X'00FF'".to_owned()
            ]
        );
        assert_eq!(
            tb.equal(&k),
            "(`a` = _utf8mb4 X'69742773205C2078' AND `b` = X'00FF')"
        );
        assert_eq!(
            tb.after(&["1".into(), "2".into()]),
            "((`a` > 1) OR (`a` = 1 AND `b` > 2))"
        );
        let j = serde_json::json!({"A": "q", "b": "AP8="});
        assert_eq!(
            tb.literals_of_json(&j).unwrap(),
            vec!["_utf8mb4 X'71'".to_owned(), "X'00FF'".to_owned()]
        );
        assert!(
            tb.literals_of_json(&serde_json::json!({"_key": "x"}))
                .is_none()
        );
        assert!(tb.literals(&[None, Some(vec![1])]).is_none());
        // Numbers stay numbers; text that isn't one is refused.
        let n = t(&[("id", KeyKind::Number)]);
        assert_eq!(n.literals(&[Some(b"-12.5".to_vec())]).unwrap(), ["-12.5"]);
        assert_eq!(
            n.literals_of_json(&serde_json::json!({"id": 9_007_199_254_740_993_u64}))
                .unwrap(),
            ["9007199254740993"]
        );
        assert!(n.literals(&[Some(b"1 OR 1".to_vec())]).is_none());
    }

    #[test]
    fn statement_errors_say_whether_they_are_tried_again() {
        let e = |n| sprig_doc::engine::SqlError {
            errno: Some(n),
            message: "m".into(),
            mongo: None,
        };
        assert!(sql_error(&e(1205)).retryable);
        assert!(!sql_error(&e(1054)).retryable);
        assert_eq!(sql_error(&e(1054)).protocol().mysql_errno, 1054);
        // A constraint is one row's; a privilege or a lost column the column's.
        assert!(is_row_error(&sql_error(&e(4025))));
        assert!(is_row_error(&sql_error(&e(3819))));
        assert!(!is_row_error(&sql_error(&e(1142))));
        assert!(!is_row_error(&sql_error(&e(1054))));
        assert!(!is_row_error(&embedding::failed(
            "the embedding model: gone"
        )));
    }
}
