//! Definition changes on engine tables: what the engine does to a table's
//! rows, index entries and records when its definition changes.
//!
//! A change runs in three steps around the catalog's own:
//!
//! ```text
//! the statement runs on the MariaDB copy (no rows) ─► the new definition
//!   1. stage   the engine reads the new definition, prepares what it
//!              needs (converted copies of the rows, new index entries),
//!              and records the change as pending. An error here (a value
//!              that doesn't fit, a duplicate key) changes nothing: the
//!              copy is put back and the catalog never publishes.
//!   2. the catalog publishes the new definition
//!   3. apply   the pending change becomes the table's layout; what the
//!              old definition alone needed is deleted.
//! ```
//!
//! A crash between 1 and 3 leaves the pending change in the table's layout
//! record: the next statement that arrives with the new definition applies
//! it, and the next definition change that finds it unapplied discards it.
//!
//! Columns added, dropped, renamed, moved or widened touch no row (the row
//! record's versions read through them). Other changes copy the rows into
//! the table's next generation, each value converted as the servers
//! convert it, and switch to it in one write.
//!
//! While a change runs, no other statement uses the table: the change
//! waits for the statements and transactions that use it (up to the
//! session's `lock_wait_timeout`, then 1205), as it waits for its metadata
//! lock on the servers, and later statements wait for the change. A change
//! that touches no row is over at once; one that copies rows or builds an
//! index holds the others back for as long as that takes.

use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_engine::keys::kind;
use sprig_engine::txn::{LockMode, TxnOptions, Wait};

use super::alter::{self, Alter, Plan, Src};
use super::assign::{self, Env, Store};
use super::expr::{self, Args};
use super::indexes::{Entry, Record};
use super::keys::{self, Space};
use super::layout::{Layout, LayoutRecord, Pending};
use super::meta;
use super::row::{self, Datum};
use super::table::{ColDefault, ColType, Column, Family, TableDef};
use super::{Engine, Pace, WALK_BATCH, exec, not_on_engine, object_of, temporal};

tokio::task_local! {
    /// Set while automatic indexing adds an index online: a change that
    /// only adds `IGNORED` indexes named `sprig_auto_*` publishes them
    /// without entries (every later write keeps them), and
    /// [`Engine::backfill_index`] writes the entries of the rows there are,
    /// a batch at a time, without holding the table.
    pub(crate) static ONLINE_BUILD: ();
}

tokio::task_local! {
    /// Set while a user's statement adds plain indexes online
    /// (`exec::engine_seam::online_index`): the definition marks each new
    /// plain index `IGNORED` and records its name here, the change publishes
    /// them without entries (every later write keeps them), and the
    /// statement then writes the entries with [`Engine::backfill_index`]
    /// and makes the indexes public.
    pub(crate) static USER_ONLINE: Arc<std::sync::Mutex<Vec<String>>>;
}

/// Whether `name` is one of the user's online-built indexes of this task.
fn user_online(name: &str) -> bool {
    USER_ONLINE
        .try_with(|v| {
            v.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .any(|n| n.eq_ignore_ascii_case(name))
        })
        .unwrap_or(false)
}

/// Whether `name` is an automatic index's.
fn auto_named(name: &str) -> bool {
    name.len() >= 11 && name[..11].eq_ignore_ascii_case("sprig_auto_")
}

/// What a change needs from the session that asked for it.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    /// The session's rules for storing values.
    pub env: Env,
    /// How long the change waits for statements and transactions using
    /// the table (`lock_wait_timeout`).
    pub wait: Duration,
    /// The logical database name (messages).
    pub db: String,
    /// The session's connection id (the servers' temporary table names in
    /// messages carry it).
    pub conn: u32,
    /// The session's account (`user`, `host`): a view's definer when the
    /// statement names none.
    pub account: (String, String),
}

/// A staged change: the engine holds the table until it is applied or
/// abandoned.
pub(crate) struct Staged {
    _guard: tokio::sync::OwnedMutexGuard<()>,
    tenant: u64,
    object: u32,
    /// The rows the servers report as copied: `None` when they report
    /// none.
    pub rows: Option<u64>,
    /// The notes and warnings converting the rows raised.
    pub conds: Vec<assign::Condition>,
}

impl Staged {
    /// The table's object number.
    pub(crate) fn object(&self) -> u32 {
        self.object
    }
}

impl std::fmt::Debug for Staged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Staged")
            .field("tenant", &self.tenant)
            .field("object", &self.object)
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

/// 1412: the statement's definition of the table is no longer the table's.
fn definition_changed() -> SprigError {
    SprigError::standard(
        1412,
        *b"HY000",
        "Table definition has changed, please retry transaction",
    )
}

/// 1205 for a change that waited too long for the table's users.
fn lock_wait_timeout() -> SprigError {
    SprigError::standard(
        1205,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction",
    )
}

fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        ErrorKind::Standard { errno, .. } => Some(errno),
        _ => None,
    }
}

/// Whether a value of `from` is given to `to`'s store rules as it is (the
/// two are numbers, or byte strings, or dates and times); otherwise it is
/// given as its text.
fn direct(from: ColType, to: ColType) -> bool {
    let numeric = |f: Family| matches!(f, Family::Integer | Family::Decimal | Family::Float);
    let (a, b) = (Family::of(from), Family::of(to));
    (numeric(a) && numeric(b))
        || (a == Family::Binary && b == Family::Binary)
        || (a == Family::Temporal && b == Family::Temporal)
}

/// The statement's time as column `c` stores it.
fn now_for(c: &Column, now: (i64, u32), tz: i32) -> Datum {
    let cut = |fsp: u8| {
        let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
        sprig_engine::row::Timestamp::new(
            u32::try_from(now.0).unwrap_or(u32::MAX),
            now.1 - now.1 % unit,
        )
    };
    match c.ty {
        ColType::DateTime { fsp } => Datum::DateTime(temporal::from_timestamp(cut(fsp), tz)),
        ColType::Timestamp { fsp } => Datum::Timestamp(cut(fsp)),
        _ => c.implicit_default(),
    }
}

/// 1062 for a new key that two rows share, as each server names the key
/// (MySQL with the table's name before it).
fn duplicate(cv: &Convert<'_>, cols: &[usize], row: &[Datum], key: &str) -> SprigError {
    if cv.sess.env.mysql {
        exec::duplicate(cv.d1, cols, row, &format!("{}.{key}", cv.d1.name))
    } else {
        exec::duplicate(cv.d1, cols, row, key)
    }
}

/// Converting the rows of one change.
struct Convert<'a> {
    d0: &'a TableDef,
    d1: &'a TableDef,
    plan: &'a Plan,
    sess: &'a Session,
    /// Conditions raised so far (outside strict mode).
    conds: Vec<assign::Condition>,
    /// The `CHECK` constraints of `d1` every row must pass (positions in
    /// its list): the ones the statement added.
    validate: Vec<usize>,
    /// The statement's time.
    now: (i64, u32),
    /// Every column of the new definition is the old one at its place,
    /// with its type: rows are the same under both.
    unchanged: bool,
}

impl Convert<'_> {
    /// The row of the new definition for `old`, row number `n` (from 1).
    fn row(&mut self, old: &[Datum], n: u64) -> Result<Vec<Datum>> {
        let mut out = Vec::with_capacity(self.plan.src.len());
        let mut computed = false;
        for (j, s) in self.plan.src.iter().enumerate() {
            let c1 = &self.d1.columns[j];
            out.push(match *s {
                Src::Added => self.plan.ver.cols[j].missing.clone(),
                Src::Computed => {
                    computed = true;
                    Datum::Null
                }
                Src::Kept(i) => {
                    let v = old[i].clone();
                    if c1.ty.check(&v).is_ok() {
                        v
                    } else {
                        assign::constant(c1, v).ok_or_else(|| {
                            SprigError::internal("engine: a widened value doesn't fit its column")
                        })?
                    }
                }
                Src::Converted(i) => self.value(&self.d0.columns[i], c1, &old[i], n)?,
            });
        }
        if computed || !self.validate.is_empty() || !self.d1.generated_cols.is_empty() {
            let a = Args {
                params: &[],
                env: self.sess.env,
                now: std::sync::OnceLock::from(self.now),
                conds: crate::engine::expr::Conds::default(),
            };
            // New columns whose DEFAULT is an expression over the row, and
            // the generated columns (all of them: the columns they read
            // may have changed).
            for (j, s) in self.plan.src.iter().enumerate() {
                let c1 = &self.d1.columns[j];
                if *s != Src::Computed && c1.generated.is_none() {
                    continue;
                }
                let v = match (&c1.generated, &c1.default) {
                    (Some(g), _) => expr::eval(&g.expr, &out, &a)?,
                    (None, Some(ColDefault::Expr(d))) => expr::eval(&d.expr, &out, &a)?,
                    _ => now_for(c1, self.now, assign::zone(self.sess.env).unwrap_or(0)),
                };
                let mut cx = Store {
                    env: self.sess.env,
                    db: self.sess.db.as_bytes(),
                    table: &self.d1.name,
                    row: usize::try_from(n).unwrap_or(usize::MAX),
                    conds: &mut self.conds,
                };
                out[j] = assign::store(&mut cx, c1, v)?;
            }
            // The constraints the statement added.
            if !self.validate.is_empty()
                && let Some(ch) = self.d1.failed_check(&out, &a, Some(&self.validate))?
            {
                return Err(self.d1.check_error(ch, &self.sess.db, self.sess.env.mysql));
            }
        }
        Ok(out)
    }

    /// One value of column `c0` as column `c1` stores it.
    fn value(&mut self, c0: &Column, c1: &Column, v: &Datum, n: u64) -> Result<Datum> {
        let mysql = self.sess.env.mysql;
        if v.is_null() {
            if c1.nullable {
                return Ok(Datum::Null);
            }
            // NULL into a column that becomes NOT NULL.
            // (MySQL words it differently when it changes the table in
            // place.)
            return Err(if mysql && !self.plan.reports_rows {
                SprigError::standard(1138, *b"22004", "Invalid use of NULL value")
            } else {
                SprigError::standard(
                    1265,
                    *b"01000",
                    format!("Data truncated for column '{}' at row {n}", c1.name),
                )
            });
        }
        let utf8 = meta::Results {
            cs: Some(meta::ResultsCs { id: 224, mbmax: 4 }),
            mysql,
        };
        let text = meta::text_in(c0, v, &self.sess.env, utf8).unwrap_or_default();
        let input = if direct(c0.ty, c1.ty) {
            v.clone()
        } else {
            Datum::Str(text.clone())
        };
        let mut cx = Store {
            env: self.sess.env,
            db: self.sess.db.as_bytes(),
            table: &self.d1.name,
            row: usize::try_from(n).unwrap_or(usize::MAX),
            conds: &mut self.conds,
        };
        assign::store(&mut cx, c1, input).map_err(|e| {
            // The servers word some errors of a table copy differently
            // from an INSERT's.
            match errno(&e) {
                // Too long for a column of the same kind (VARCHAR to
                // VARCHAR, CHAR to CHAR, VARBINARY to VARBINARY).
                Some(1406)
                    if matches!(
                        (c0.ty, c1.ty),
                        (ColType::VarChar { .. }, ColType::VarChar { .. })
                            | (ColType::Char { .. }, ColType::Char { .. })
                            | (ColType::VarBinary { .. }, ColType::VarBinary { .. })
                            | (ColType::Binary { .. }, ColType::Binary { .. })
                    ) =>
                {
                    SprigError::standard(
                        1265,
                        *b"01000",
                        format!("Data truncated for column '{}' at row {n}", c1.name),
                    )
                }
                Some(1366) if !mysql => {
                    let what = match Family::of(c1.ty) {
                        Family::Integer => "INTEGER",
                        Family::Decimal => "DECIMAL",
                        Family::Float => "DOUBLE",
                        _ => return e,
                    };
                    SprigError::standard(
                        1292,
                        *b"22007",
                        format!(
                            "Truncated incorrect {what} value: '{}'",
                            String::from_utf8_lossy(&text)
                        ),
                    )
                }
                _ => e,
            }
        })
    }
}

impl Engine {
    fn read_layout(&self, tenant: u64, object: u32) -> Result<Option<LayoutRecord>> {
        let mut tx = self.begin(TxnOptions::default())?;
        match tx.get(&keys::layout_key(tenant, object))? {
            None => Ok(None),
            Some(raw) => LayoutRecord::decode(&raw).map(Some).ok_or_else(|| {
                SprigError::internal("engine: a table's layout record doesn't decode")
            }),
        }
    }

    async fn write_layout(&self, tenant: u64, object: u32, rec: &LayoutRecord) -> Result<()> {
        let mut tx = self.begin(TxnOptions::default())?;
        tx.put_new(Bytes::from(keys::layout_key(tenant, object)), rec.encode())
            .await?;
        tx.finish().await
    }

    /// Deletes every key starting with `prefix`, a batch per transaction.
    pub(super) async fn clear(&self, prefix: &[u8]) -> Result<()> {
        let end = keys::prefix_end(prefix);
        loop {
            let mut tx = self.begin(TxnOptions::default())?;
            let batch = tx.scan(prefix, &end, false, WALK_BATCH)?;
            let n = batch.len();
            for (k, _) in batch {
                tx.delete(k).await?;
            }
            tx.finish().await?;
            // The durability wait may be ready at once (syncs off).
            tokio::task::yield_now().await;
            if n < WALK_BATCH {
                return Ok(());
            }
        }
    }

    /// Deletes the record's garbage ranges and writes it without them.
    async fn collect(&self, tenant: u64, object: u32, rec: &mut LayoutRecord) -> Result<()> {
        if rec.garbage.is_empty() {
            return Ok(());
        }
        for (k, id) in rec.garbage.clone() {
            let mut p = keys::rows_prefix(tenant, object, id);
            p[16] = k;
            self.clear(&p).await?;
        }
        rec.garbage.clear();
        self.write_layout(tenant, object, rec).await
    }

    /// Forgets what the engine keeps in memory about a table.
    fn forget(&self, tenant: u64, object: u32) {
        self.spaces
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant, object));
        self.autoinc
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant, object));
    }

    /// The row layout of `def` (the caller holds the records' lock): from
    /// the table's layout record, after applying a pending change this
    /// definition is the result of.
    ///
    /// # Errors
    /// 1412 for a definition older than the table's; the store's.
    pub(super) async fn layout_of(
        &self,
        tenant: u64,
        object: u32,
        def: &TableDef,
    ) -> Result<Layout> {
        let Some(mut rec) = self.read_layout(tenant, object)? else {
            return Ok(Layout::first(def));
        };
        let promote = rec.pending.as_ref().is_some_and(|p| {
            def.version > p.base
                && p.ver.matches(def)
                && p.indexes.entries.len() == def.index_sigs.len()
                && def
                    .index_sigs
                    .iter()
                    .all(|s| p.indexes.entries.iter().any(|e| e.sig == *s))
        });
        if promote {
            if let Some(p) = rec.promote() {
                rec.at = def.version;
                let mut tx = self.begin(TxnOptions::default())?;
                tx.put_new(Bytes::from(keys::layout_key(tenant, object)), rec.encode())
                    .await?;
                tx.put_new(
                    Bytes::from(keys::index_record_key(tenant, object)),
                    p.indexes.encode(),
                )
                .await?;
                tx.finish().await?;
            }
            self.forget(tenant, object);
        }
        self.collect(tenant, object, &mut rec).await?;
        if def.version < rec.at {
            return Err(definition_changed());
        }
        if !rec.current.matches(def) {
            // A definition the engine wasn't told about: its columns are
            // matched by name, and must read through.
            let p = alter::plan(
                &rec.current,
                None,
                def,
                None,
                rec.next_col,
                &alter::plain_missing,
            );
            if p.convert {
                return Err(definition_changed());
            }
            let old = std::mem::replace(&mut rec.current, p.ver);
            if old.version != rec.current.version {
                rec.older.push(old);
            }
            rec.next_col = p.next_col;
            rec.at = def.version;
            self.write_layout(tenant, object, &rec).await?;
        }
        rec.layout(def.version)
    }

    /// Makes later statements on the table wait for the caller (which
    /// holds the table's lock), then waits until no statement or
    /// transaction uses the table through the keyspace `sp` was taken from.
    /// When they don't finish in time the table is left as it was.
    async fn quiesce(&self, sp: &Space, wait: Duration) -> Result<()> {
        let key = (sp.tenant, sp.object);
        let removed = self
            .spaces
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
        // The users of the keyspace `sp` was taken from, the caller's `sp`
        // among them: counted over the cache entry's index numbers and
        // their per-thread copies while this function holds the entry.
        let grouped = removed.as_ref().is_some_and(|c| c.holds(&sp.ids));
        let users = |removed: &Option<super::Cached>| match removed {
            Some(c) if grouped => c.users(),
            _ => Arc::strong_count(&sp.ids),
        };
        let deadline = Instant::now() + wait;
        while users(&removed) > 1 {
            if Instant::now() >= deadline {
                if let Some(c) = removed {
                    self.spaces
                        .write()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(key, c);
                }
                return Err(lock_wait_timeout());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }

    /// The table's rows: how many there are.
    async fn count_rows(&self, sp: &Space) -> Result<u64> {
        let p = sp.rows();
        let end = keys::prefix_end(&p);
        let mut lo = p;
        let mut n = 0u64;
        let mut tx = self.begin(TxnOptions::default())?;
        let mut pace = Pace::default();
        loop {
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            n += batch.len() as u64;
            pace.add(batch.len()).await;
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH => lo = keys::prefix_end(k),
                _ => return Ok(n),
            }
        }
    }

    /// Writes every row of `sp0` (definition `d0`) into `sp1` (definition
    /// `d1`) as `cv` converts it: with its entries in every index of `d1`
    /// when `copy`, else its entries in indexes `build` only (the rows stay
    /// where they are). Returns the number of rows.
    async fn carry(
        &self,
        sp0: &Space,
        sp1: &Space,
        cv: &mut Convert<'_>,
        copy: bool,
        build: &[usize],
    ) -> Result<u64> {
        let (d0, d1) = (cv.d0, cv.d1);
        let p = sp0.rows();
        let end = keys::prefix_end(&p);
        let mut lo = p;
        let mut n = 0u64;
        // One wait for durability, for the last batch.
        let last = loop {
            let mut tx = self.begin(TxnOptions::default())?;
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            for (k, v) in &batch {
                n += 1;
                let old = row::decode(d0, sp0, k, v)?;
                // A table keyed by a hidden row number keeps each row's
                // number; one that loses its primary key numbers its rows
                // in the old key's order.
                let hidden = d1.hidden.then(|| match old.get(d0.columns.len()) {
                    Some(h) if d0.hidden => h.clone(),
                    _ => Datum::UInt(n),
                });
                // A new index over unchanged columns takes the row as it is.
                let mut new = if cv.unchanged { old } else { cv.row(&old, n)? };
                new.truncate(d1.columns.len());
                if let Some(h) = hidden {
                    new.push(h);
                }
                if copy {
                    let key = keys::row_key_of(sp1, d1, &new).ok_or_else(|| {
                        SprigError::internal("engine: a key value of the wrong type")
                    })?;
                    if !tx
                        .insert(Bytes::from(key), row::encode(d1, sp1, &new)?, false)
                        .await?
                    {
                        return Err(duplicate(cv, &d1.pk, &new, &d1.pk_name));
                    }
                }
                for ix in 0..d1.indexes.len() {
                    if !copy && !build.contains(&ix) {
                        continue;
                    }
                    let x = &d1.indexes[ix];
                    let Some((k, val)) = keys::index_entry(sp1, d1, ix, &new) else {
                        return Err(SprigError::internal(
                            "engine: an index value of the wrong type",
                        ));
                    };
                    if x.unique && !x.cols.iter().any(|&c| new[c].is_null()) {
                        if !tx.insert(Bytes::from(k), val, false).await? {
                            return Err(duplicate(cv, &x.cols, &new, &x.name));
                        }
                    } else {
                        tx.put_new(Bytes::from(k), val).await?;
                    }
                }
            }
            let queued = tx.commit_queued()?;
            // Between batches the worker goes back to the runtime (see
            // `Engine::build`).
            tokio::task::yield_now().await;
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH => lo = keys::prefix_end(k),
                _ => break queued,
            }
        };
        last.wait_async().await?;
        Ok(n)
    }

    /// Step 1 of a definition change from `d0` to `d1` (what the MariaDB
    /// copy of the table now is), which statement `alter` made.
    ///
    /// # Errors
    /// The servers' errors for rows the new definition can't hold (1264,
    /// 1265, 1138, 1292, 1062 ...); 1205 when the table's users don't
    /// finish in time; 39960 for a change the engine doesn't make yet.
    /// Nothing is changed then.
    #[allow(clippy::too_many_lines)] // the steps of one change, in order
    pub(crate) async fn alter_stage(
        &self,
        tenant: u64,
        d0: &TableDef,
        d1: &TableDef,
        alter: &Alter,
        sess: &Session,
    ) -> Result<Staged> {
        let object = object_of(d0)?;
        let guard = self.table_lock(tenant, object).lock_owned().await;
        // The table as the old definition has it (an earlier change the
        // catalog published is applied first).
        let sp0 = match self.cached(tenant, object, d0) {
            Some(sp) => sp,
            None => self.space_locked(tenant, object, d0).await?,
        };
        // The change waits for the statements and transactions that use
        // the table, and later ones wait for it: no other statement uses
        // the table from here on.
        self.quiesce(&sp0, sess.wait).await?;
        let mut rec = self
            .read_layout(tenant, object)?
            .unwrap_or_else(|| LayoutRecord::first(d0));
        // An earlier change that was staged and never published.
        if rec.pending.is_some() {
            rec.discard();
            self.collect(tenant, object, &mut rec).await?;
        }
        let now = temporal::now();
        let tz = assign::zone(sess.env).unwrap_or(0);
        let env = sess.env;
        let mut plan = alter::plan(
            &rec.current,
            Some(d0),
            d1,
            Some(alter),
            rec.next_col,
            &|c| match &c.default {
                // The statement's time, the same for every row.
                Some(ColDefault::Now) => Some(now_for(c, now, tz)),
                // An expression with one value: worked out now.
                Some(ColDefault::Expr(d)) if d.constant => {
                    let a = Args {
                        params: &[],
                        env,
                        now: std::sync::OnceLock::from(now),
                        conds: crate::engine::expr::Conds::default(),
                    };
                    expr::eval(&d.expr, &[], &a)
                        .ok()
                        .and_then(|v| assign::constant(c, v))
                }
                _ => alter::plain_missing(c),
            },
        );
        // The `CHECK` constraints the statement adds must hold for the
        // rows there are. (MariaDB doesn't judge a column's own check
        // added by `MODIFY`; MySQL does.)
        let validate: Vec<usize> = d1
            .checks
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                !d0.checks
                    .iter()
                    .any(|o| o.name == c.name && o.text == c.text)
                    && (env.mysql || c.column.is_none())
            })
            .map(|(i, _)| i)
            .collect();
        // Both servers copy the table to add a constraint; MySQL also for
        // a new column whose DEFAULT is an expression.
        if alter.specs.contains(&alter::Spec::AddCheck)
            || (env.mysql
                && plan.src.iter().zip(&d1.columns).any(|(s, c)| {
                    *s == Src::Added && matches!(c.default, Some(ColDefault::Expr(_)))
                }))
        {
            plan.reports_rows = true;
        }
        // A table that loses its primary key (the hidden row number takes
        // over) is copied on both servers: nothing else can key it in place.
        if d1.hidden && !d0.hidden {
            plan.reports_rows = true;
        }
        // Partitioning made or removed: both servers copy the table (and
        // report its rows); a rehash (`COALESCE`, `ADD PARTITION
        // PARTITIONS n`) reports them on MariaDB only; dropping, adding or
        // reorganizing RANGE / LIST partitions doesn't.
        if alter.specs.iter().any(|s| {
            matches!(s, alter::Spec::Other(o)
                if o.starts_with("PARTITION BY")
                    || o.starts_with("REMOVE PARTITIONING")
                    || (!sess.env.mysql
                        && (o.starts_with("COALESCE PARTITION")
                            || o == "ADD PARTITION PARTITIONS")))
        }) {
            plan.reports_rows = true;
        }
        if sess.env.mysql {
            alter::mysql_clauses(alter, &plan)?;
        } else {
            let tightens = plan.src.iter().zip(&d1.columns).any(|(s, c)| match s {
                Src::Kept(i) | Src::Converted(i) => d0.columns[*i].nullable && !c.nullable,
                _ => false,
            });
            alter::mariadb_clauses(alter, &plan, tightens)?;
        }
        for (j, s) in plan.src.iter().enumerate() {
            let c = &d1.columns[j];
            if *s == Src::Added && c.auto_inc {
                return Err(not_on_engine(
                    "adding an AUTO_INCREMENT column to a table (its rows would need numbers)",
                ));
            }
        }
        let rekey = alter::rekeys(d0, &rec.current, d1, &plan);
        let copy = plan.convert || rekey || sp0.first_format;
        if copy && plan.ver.version == rec.current.version {
            plan.ver.version = rec.current.version.saturating_add(1);
        }
        let keep = if copy {
            vec![None; d1.indexes.len()]
        } else {
            alter::kept_indexes(d0, &rec.current, d1, &plan, Some(alter))
        };
        // Key index numbers: kept for the indexes whose entries stay, new
        // for the others.
        let rk = Bytes::from(keys::index_record_key(tenant, object));
        let mut tx = self.begin(TxnOptions::default())?;
        let irec = match tx.get(&rk)? {
            Some(raw) => Record::decode(&raw).ok_or_else(|| {
                SprigError::internal("engine: a table's index record doesn't decode")
            })?,
            None => Record::default(),
        };
        drop(tx);
        let mut next = irec.next.max(1);
        let mut entries = Vec::with_capacity(d1.indexes.len());
        let mut built = Vec::new();
        let mut fresh = Vec::new();
        for (q, sig) in d1.index_sigs.iter().enumerate() {
            let id = if let Some(o) = keep[q] {
                sp0.ids[o]
            } else {
                let id = next;
                next = next.saturating_add(1);
                built.push((id, d1.indexes[q].unique));
                fresh.push(q);
                id
            };
            entries.push(Entry {
                id,
                sig: sig.clone(),
            });
        }
        let dropped: Vec<(u16, bool)> = sp0
            .ids
            .iter()
            .zip(&d0.indexes)
            .filter(|(id, _)| !entries.iter().any(|e| e.id == **id))
            .map(|(id, x)| (*id, x.unique))
            .collect();
        let new_gen = copy.then_some(rec.next_gen);
        // What this step is about to write is garbage until the change is
        // recorded as pending (a crash in between leaves nothing behind
        // that the next change wouldn't delete).
        if !built.is_empty() || copy || !validate.is_empty() {
            let mut reserve = rec.clone();
            for (id, unique) in &built {
                reserve.garbage.push((
                    if *unique {
                        kind::UNIQUE
                    } else {
                        kind::SECONDARY
                    },
                    *id,
                ));
            }
            if let Some(g) = new_gen {
                reserve.garbage.push((kind::PRIMARY, g));
                reserve.next_gen = g.saturating_add(1);
                rec.next_gen = g.saturating_add(1);
            }
            let mut tx = self.begin(TxnOptions::default())?;
            tx.put_new(
                Bytes::from(keys::layout_key(tenant, object)),
                reserve.encode(),
            )
            .await?;
            tx.put_new(
                rk,
                Record {
                    next,
                    entries: irec.entries.clone(),
                }
                .encode(),
            )
            .await?;
            tx.finish().await?;
            let sp1 = Space {
                tenant,
                object,
                ids: Arc::from(entries.iter().map(|e| e.id).collect::<Vec<u16>>()),
                first_format: false,
                layout: Arc::new(Layout {
                    current: plan.ver.schema()?,
                    older: Vec::new(),
                    row_gen: new_gen.unwrap_or(rec.row_gen),
                    restart_counter: false,
                }),
            };
            let reports_rows = plan.reports_rows;
            let unchanged = validate.is_empty()
                && d1.generated_cols.is_empty()
                && d0.columns.len() == d1.columns.len()
                && plan.src.iter().enumerate().all(|(j, s)| *s == Src::Kept(j))
                && d0
                    .columns
                    .iter()
                    .zip(&d1.columns)
                    .all(|(a, b)| a.ty == b.ty);
            let mut cv = Convert {
                d0,
                d1,
                plan: &plan,
                sess,
                conds: Vec::new(),
                validate,
                now,
                unchanged,
            };
            // An online build adds its index without entries: writes keep
            // it from the publish on, and the backfill writes the rest.
            let auto = ONLINE_BUILD.try_with(|()| ()).is_ok();
            let online = (auto || USER_ONLINE.try_with(|_| ()).is_ok())
                && !copy
                && cv.validate.is_empty()
                && !fresh.is_empty()
                && fresh.iter().all(|&q| {
                    let x = &d1.indexes[q];
                    x.ignored
                        && !x.unique
                        && if auto {
                            auto_named(&x.name)
                        } else {
                            user_online(&x.name)
                        }
                });
            let carried = if online {
                Ok(0)
            } else {
                self.carry(&sp0, &sp1, &mut cv, copy, &fresh).await
            };
            let n = match carried {
                Ok(n) => n,
                Err(e) => {
                    self.collect(tenant, object, &mut reserve).await?;
                    return Err(e);
                }
            };
            let conds = std::mem::take(&mut cv.conds);
            rec.next_col = plan.next_col;
            rec.pending = Some(Pending {
                base: d0.version,
                ver: plan.ver,
                copied: new_gen,
                indexes: Record { next, entries },
                built,
                dropped,
            });
            self.write_layout(tenant, object, &rec).await?;
            return Ok(Staged {
                _guard: guard,
                tenant,
                object,
                rows: reports_rows.then_some(n),
                conds,
            });
        }
        // Nothing to write but the record.
        let rows = if plan.reports_rows {
            Some(self.count_rows(&sp0).await?)
        } else {
            None
        };
        rec.next_col = plan.next_col;
        rec.pending = Some(Pending {
            base: d0.version,
            ver: plan.ver,
            copied: None,
            indexes: Record { next, entries },
            built,
            dropped,
        });
        self.write_layout(tenant, object, &rec).await?;
        Ok(Staged {
            _guard: guard,
            tenant,
            object,
            rows,
            conds: Vec::new(),
        })
    }

    /// The backfill of an online index build: writes index `name`'s entry
    /// for every row of the table, a batch of `batch` rows per transaction,
    /// pausing `pause` between batches. Each batch reads its rows with
    /// shared locks, so a write to one of them waits for the batch (and the
    /// batch for a write in flight): an entry is always written from the
    /// row's latest committed value, and every write after the index was
    /// published keeps the entry itself. `current` gives the table's newest
    /// definition before each batch; when its version is no longer `def`'s,
    /// the backfill stops (the next build starts it again).
    ///
    /// # Errors
    /// 1412 when the definition changed; lock waits; the store's.
    pub(crate) async fn backfill_index(
        &self,
        tenant: u64,
        def: &TableDef,
        name: &str,
        batch: usize,
        pause: Duration,
        current: &(dyn Fn() -> Option<u64> + Send + Sync),
    ) -> Result<(u64, u64)> {
        let Some(ix) = def
            .indexes
            .iter()
            .position(|x| x.name.eq_ignore_ascii_case(name))
        else {
            return Err(SprigError::internal(format!(
                "engine: the table has no index {name} to build"
            )));
        };
        let batch = batch.max(1);
        let (mut rows, mut batches) = (0u64, 0u64);
        let mut lo: Option<Vec<u8>> = None;
        loop {
            if current() != Some(def.version) {
                return Err(definition_changed());
            }
            let sp = self.space(tenant, def).await?;
            let p = sp.rows();
            let end = keys::prefix_end(&p);
            let from = lo.clone().unwrap_or(p);
            let mut tx = self.begin(TxnOptions::default())?;
            let got = tx
                .scan_lock_entries(&from, &end, batch, LockMode::Shared, Wait::Block)
                .await?;
            for (k, v) in &got {
                let row = row::decode(def, &sp, k, v)?;
                let Some((ik, iv)) = keys::index_entry(&sp, def, ix, &row) else {
                    return Err(SprigError::internal(
                        "engine: an index value of the wrong type",
                    ));
                };
                tx.put(Bytes::from(ik), iv).await?;
            }
            tx.finish().await?;
            drop(sp);
            rows += got.len() as u64;
            batches += 1;
            match got.last() {
                Some((k, _)) if got.len() == batch => lo = Some(keys::prefix_end(k)),
                _ => return Ok((rows, batches)),
            }
            if pause.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(pause).await;
            }
        }
    }

    /// Step 3: the catalog published `def`, the definition the staged
    /// change prepared.
    ///
    /// # Errors
    /// The store's (the change then applies with the next statement on
    /// the table).
    pub(crate) async fn alter_apply(&self, staged: Staged, def: &TableDef) -> Result<()> {
        self.space_locked(staged.tenant, staged.object, def)
            .await
            .map(|_| ())
    }

    /// The catalog didn't publish the staged change: what it prepared is
    /// deleted.
    ///
    /// # Errors
    /// The store's (the next change of the table deletes it then).
    pub(crate) async fn alter_abandon(&self, staged: Staged) -> Result<()> {
        let (tenant, object) = (staged.tenant, staged.object);
        if let Some(mut rec) = self.read_layout(tenant, object)? {
            rec.discard();
            self.collect(tenant, object, &mut rec).await?;
        }
        self.forget(tenant, object);
        Ok(())
    }

    /// `TRUNCATE TABLE`: the table's rows and index entries are no longer
    /// part of it (they are deleted after the switch), and its
    /// `AUTO_INCREMENT` counter starts over.
    ///
    /// # Errors
    /// 1205 when the table's users don't finish in time; the store's.
    pub(crate) async fn truncate(&self, tenant: u64, def: &TableDef, sess: &Session) -> Result<()> {
        let object = object_of(def)?;
        let lock = self.table_lock(tenant, object);
        let _guard = lock.lock().await;
        let sp = match self.cached(tenant, object, def) {
            Some(sp) => sp,
            None => self.space_locked(tenant, object, def).await?,
        };
        self.quiesce(&sp, sess.wait).await?;
        let mut rec = self
            .read_layout(tenant, object)?
            .unwrap_or_else(|| LayoutRecord::first(def));
        rec.discard();
        // A new generation and new key index numbers: the old ones are
        // garbage from the moment the record says so.
        rec.garbage.push((kind::PRIMARY, rec.row_gen));
        rec.row_gen = rec.next_gen;
        rec.next_gen = rec.next_gen.saturating_add(1);
        rec.older.clear();
        rec.truncated = def.version;
        let rk = Bytes::from(keys::index_record_key(tenant, object));
        let mut tx = self.begin(TxnOptions::default())?;
        let mut irec = match tx.get(&rk)? {
            Some(raw) => Record::decode(&raw).ok_or_else(|| {
                SprigError::internal("engine: a table's index record doesn't decode")
            })?,
            None => Record::default(),
        };
        irec.next = irec.next.max(1);
        for e in &mut irec.entries {
            rec.garbage.push((
                if e.sig.unique {
                    kind::UNIQUE
                } else {
                    kind::SECONDARY
                },
                e.id,
            ));
            e.id = irec.next;
            irec.next = irec.next.saturating_add(1);
        }
        tx.put_new(Bytes::from(keys::layout_key(tenant, object)), rec.encode())
            .await?;
        tx.put_new(rk, irec.encode()).await?;
        // An old table's prototype-format marker goes with its rows.
        tx.delete(Bytes::from(keys::format_key(tenant, object)))
            .await?;
        tx.finish().await?;
        self.forget(tenant, object);
        self.collect(tenant, object, &mut rec).await
    }

    /// `DROP TABLE` (after the catalog published it): every key of the
    /// table is deleted.
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn drop_table(&self, tenant: u64, object: u32) -> Result<()> {
        let lock = self.table_lock(tenant, object);
        let _guard = lock.lock().await;
        self.forget(tenant, object);
        self.clear(&keys::object_prefix(tenant, object)).await?;
        let mut tx = self.begin(TxnOptions::default())?;
        tx.delete(Bytes::from(keys::drop_mark(tenant, object)))
            .await?;
        tx.finish().await?;
        self.tables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant, object));
        Ok(())
    }

    /// Marks tables `objects` of `tenant` as dropped, durably, before
    /// their keys are deleted in the background: a process that stops
    /// before the deletion ends finds the marks again ([`Self::sweep`]).
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn mark_dropped(&self, tenant: u64, objects: &[u32]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let mut tx = self.begin(TxnOptions::default())?;
        for o in objects {
            tx.put(Bytes::from(keys::drop_mark(tenant, *o)), Bytes::new())
                .await?;
        }
        tx.finish().await
    }

    /// Deletes what is left of tables of `tenant` that are no longer in
    /// its catalog: those marked dropped ([`Self::mark_dropped`]) and
    /// `tombstones` (tables the catalog records as dropped, or in a
    /// dropped database, whose
    /// deletion may have been cut short before the marks existed), except
    /// any in `live` (the catalog's tables: a mark whose drop was never
    /// published is left alone). Gives the number of tables whose keys
    /// were deleted.
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn sweep(
        &self,
        tenant: u64,
        live: &std::collections::HashSet<u32>,
        tombstones: &[u32],
    ) -> Result<usize> {
        let prefix = keys::drops_prefix(tenant);
        let marked: Vec<u32> = {
            let mut tx = self.begin(TxnOptions::default())?;
            let rows = tx.scan(&prefix, &keys::prefix_end(&prefix), false, 1 << 20)?;
            tx.rollback();
            rows.iter()
                // A session's temporary table is marked with its owner:
                // only a later process of the same node deletes it.
                .filter(|(_, v)| v.is_empty() || super::temp::orphaned(v))
                .filter_map(|(k, _)| {
                    let b = k.get(prefix.len()..prefix.len() + 4)?;
                    Some(u32::from_be_bytes(b.try_into().ok()?))
                })
                .collect()
        };
        let mut swept = 0;
        for o in marked {
            if live.contains(&o) {
                continue;
            }
            self.drop_table(tenant, o).await?;
            swept += 1;
        }
        for o in tombstones {
            if live.contains(o) || !self.has_keys(tenant, *o)? {
                continue;
            }
            self.drop_table(tenant, *o).await?;
            swept += 1;
        }
        Ok(swept)
    }

    /// Marks table `object` of `tenant` as one to delete when found by a
    /// later sweep, with `owner` as the mark's value (a session's
    /// temporary table: its node and process).
    ///
    /// # Errors
    /// The store's.
    pub(crate) async fn mark_owned(&self, tenant: u64, object: u32, owner: Vec<u8>) -> Result<()> {
        let mut tx = self.begin(TxnOptions::default())?;
        tx.put(
            Bytes::from(keys::drop_mark(tenant, object)),
            Bytes::from(owner),
        )
        .await?;
        tx.finish().await
    }

    /// Whether table `object` of `tenant` has any key left.
    pub(crate) fn has_keys(&self, tenant: u64, object: u32) -> Result<bool> {
        let p = keys::object_prefix(tenant, object);
        let mut tx = self.begin(TxnOptions::default())?;
        let any = !tx.scan(&p, &keys::prefix_end(&p), false, 1)?.is_empty();
        tx.rollback();
        Ok(any)
    }

    /// Runs [`Self::sweep`] for `tenant` once per process, in the
    /// background, with `cat`'s tables as the live ones.
    pub(crate) fn sweep_once(&'static self, cat: &sprig_catalog::TenantCatalog) {
        let tenant = cat.tenant.0;
        if !self
            .swept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant)
        {
            return;
        }
        // Live: an object not dropped, in a database not dropped (a
        // database's drop leaves its tables' rows as they were).
        let is_live = |o: &sprig_catalog::ObjectRow| {
            o.state != sprig_catalog::ObjectState::Dropped
                && cat
                    .databases
                    .get(o.db_id)
                    .is_some_and(|d| d.state != sprig_catalog::DbState::Dropped)
        };
        let live: std::collections::HashSet<u32> = cat
            .objects
            .iter()
            .filter(|o| is_live(o))
            .filter_map(|o| u32::try_from(o.object_id.0).ok())
            .collect();
        let tombstones: Vec<u32> = cat
            .objects
            .iter()
            .filter(|o| o.kind == sprig_catalog::ObjectKind::Table && !is_live(o))
            .filter_map(|o| u32::try_from(o.object_id.0).ok())
            .collect();
        tokio::spawn(async move {
            match self.sweep(tenant, &live, &tombstones).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(tenant, tables = n, "dropped tables' keys deleted"),
                Err(e) => {
                    tracing::warn!(tenant, error = %e, "dropped tables' keys not all deleted");
                }
            }
        });
    }
}
