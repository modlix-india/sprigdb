//! The in-process SQL backend (the storage engine's first prototype): engine-backed
//! databases served by the gateway itself, with no MariaDB on the data
//! path.
//!
//! ```text
//! client ─► session stack (login, persona, admission, SLIs, workload, span)
//!        ─► tenant layers (names, privileges, row-level security, ...)
//!        ─► forward / prepare / execute ── engine database? ──► this module
//!                                              │ no
//!                                              ▼
//!                                       MariaDB, as before
//! ```
//!
//! - **Definitions** ([`define`], [`catalog`]): every database is an
//!   engine database. The engine produces each table's definition itself
//!   and the catalog stores it as the table's `SHOW CREATE TABLE` text;
//!   [`ddlexec`] is the catalog's physical side for DDL.
//! - **Binding and plans** ([`plan`]): the statement the tenant layers
//!   produced is parsed once, bound to its table, and planned: a primary
//!   key lookup is a `Point` plan that needs no optimizer.
//! - **Execution** ([`exec`]), values ([`row`], [`expr`]), the rules for
//!   storing them ([`assign`]) and keys ([`keys`], [`indexes`]) over the
//!   engine (`sprig_engine`, through
//!   [`store`]), which the node opens once ([`crate::node_engine`]; the
//!   Redis front end takes the same handle).
//! - **Results** ([`meta`]): column definitions built from the table, as
//!   the persona's server reports them, and rows encoded straight from the
//!   values.
//! - **The seam** is `exec::engine_seam`: the hooks in the executor's
//!   forward, prepare and execute paths.
//!
//! A statement naming a table that the engine can't run fails with
//! `ER_SPRIG_NOT_ON_ENGINE` (39960); it never reaches MariaDB, which holds
//! no table.

// The tokenizer, binder and executor follow the SQL text closely: short
// names (`p` parser, `c` column, `e` expression, `k` key, `v` value) read
// better there than long ones.
#![allow(clippy::many_single_char_names)]

pub(crate) mod alter;
pub(crate) mod assign;
pub(crate) mod catalog;
pub(crate) mod control;
pub(crate) mod ddl;
pub(crate) mod ddlexec;
pub(crate) mod define;
pub(crate) mod exec;
pub(crate) mod expr;
pub(crate) mod fk;
pub(crate) mod general;
pub(crate) mod graphsql;
pub(crate) mod indexes;
pub(crate) mod infoschema;
pub(crate) mod keys;
pub(crate) mod layout;
pub(crate) mod locking;
pub(crate) mod meta;
pub(crate) mod multiwrite;
pub(crate) mod paramtype;
pub(crate) mod plan;
pub(crate) mod plancache;
pub(crate) mod row;
pub(crate) mod sequence;
pub(crate) mod session;
pub(crate) mod show;
pub(crate) mod store;
pub(crate) mod table;
pub(crate) mod temp;
pub(crate) mod temporal;
pub(crate) mod view;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::txn::TxnOptions;

use self::indexes::{Record, Sig};
use self::keys::Space;
use self::row::Datum;
use self::store::Tx;
use self::table::TableDef;

/// `ER_SPRIG_NOT_ON_ENGINE`.
#[cfg(test)]
pub(crate) const NOT_ON_ENGINE: u16 = 39960;

/// 39960: a statement on an engine table outside what the engine runs.
pub(crate) fn not_on_engine(what: &str) -> SprigError {
    SprigError::new(
        sprig_common::SprigCode::NotOnEngine,
        format!("This statement can't run on the storage engine yet: {what}"),
    )
}

/// One table's keyspace as the engine last worked it out.
pub(crate) struct Cached {
    /// The index identities the numbers were given for.
    sigs: Vec<Sig>,
    /// The catalog version of the definition it was worked out for.
    version: u64,
    /// The key index number of each secondary index (the keyspace built
    /// under the table's lock hands this copy out).
    ids: Arc<[u16]>,
    /// Copies of `ids`, one per thread slot, made when a slot first asks; a
    /// statement takes its thread's copy. `ids` and its copies together
    /// count the table's users: the cache's own reference to each, and one
    /// per statement and open transaction using the table (a definition
    /// change waits until only the cache's are left). A later entry with
    /// the same numbers takes these over, so the users stay counted.
    id_copies: Arc<[OnceLock<Arc<[u16]>>]>,
    /// The rows are in the first row format.
    first_format: bool,
    /// The row layout.
    layout: layout::Shared,
    /// Copies of the layout, one per thread slot, made when a slot first
    /// asks: a statement takes its thread's copy.
    copies: Box<[OnceLock<layout::Shared>]>,
}

/// Thread slots for the per-thread copies (more threads than slots share
/// some). A statement on a table clones and drops reference counts only in
/// its own thread's copies: sessions on other threads don't write the same
/// cache line, and don't read the layout or the index numbers from a line
/// another thread keeps writing.
const SLOTS: usize = 64;

thread_local! {
    /// This thread's slot.
    static SLOT: usize = {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % SLOTS
    };
}

/// Empty per-slot copies.
fn slots<T>() -> Box<[OnceLock<T>]> {
    (0..SLOTS).map(|_| OnceLock::new()).collect()
}

impl Cached {
    /// The calling thread's keyspace of the table.
    fn space(&self, tenant: u64, object: u32) -> Space {
        let slot = SLOT.with(|s| *s);
        Space {
            tenant,
            object,
            ids: Arc::clone(self.id_copies[slot].get_or_init(|| Arc::from(&self.ids[..]))),
            first_format: self.first_format,
            layout: Arc::clone(self.copies[slot].get_or_init(|| Arc::new((*self.layout).clone()))),
        }
    }

    /// Whether `ids` is this entry's list or one of its copies.
    fn holds(&self, ids: &Arc<[u16]>) -> bool {
        Arc::ptr_eq(&self.ids, ids)
            || self
                .id_copies
                .iter()
                .any(|c| c.get().is_some_and(|c| Arc::ptr_eq(c, ids)))
    }

    /// The statements and transactions using the table through this
    /// entry's index numbers (the entry's own references aside).
    fn users(&self) -> usize {
        Arc::strong_count(&self.ids).saturating_sub(1)
            + self
                .id_copies
                .iter()
                .filter_map(OnceLock::get)
                .map(|c| Arc::strong_count(c).saturating_sub(1))
                .sum::<usize>()
    }
}

/// Each table's keyspace per (tenant, object).
type Spaces = HashMap<(u64, u32), Cached>;

/// Each table's lock per (tenant, object).
type TableLocks = HashMap<(u64, u32), Arc<tokio::sync::Mutex<()>>>;

/// Rows read per store call when a whole table is walked.
const WALK_BATCH: usize = 1024;

/// Rows a long loop handles between two returns to the runtime.
const PACE_ROWS: usize = 1024;

/// Paces a loop over many rows: every [`PACE_ROWS`] rows the task goes
/// back to the runtime once. A loop whose awaits are all ready at once
/// (no lock waits, no durability wait) otherwise runs to its end in one
/// poll, holding a runtime worker; the stall watchdog stops a statement
/// whose one poll runs past its limit (1317), and the gateway's per-poll
/// timing can only move a heavy statement off the workers between polls.
#[derive(Default)]
pub(crate) struct Pace(usize);

impl Pace {
    /// Counts `rows` more rows handled, yielding once each time the
    /// count passes another [`PACE_ROWS`].
    pub(crate) async fn add(&mut self, rows: usize) {
        self.0 += rows;
        if self.0 >= PACE_ROWS {
            self.0 = 0;
            tokio::task::yield_now().await;
        }
    }
}

/// The SQL backend over the node's engine.
pub(crate) struct Engine {
    eng: sprig_engine::Engine,
    /// `AUTO_INCREMENT` counters per (tenant, object).
    autoinc: Mutex<HashMap<(u64, u32), AutoInc>>,
    /// Next hidden row number per (tenant, object, row generation), for
    /// tables without a primary key, with the leader epoch it was seeded
    /// under.
    rowids: Mutex<HashMap<RowIdKey, (u64, u64)>>,
    /// Each table's key index numbers, per (tenant, object), for the index
    /// identities they were given for.
    spaces: RwLock<Spaces>,
    /// One lock per table (tenant, object): it serializes changes of the
    /// table's records (its indexes, row format and layout), index builds
    /// and definition changes. A definition change holds it from before it
    /// reads the rows until its catalog change is published and applied,
    /// so statements that find no cached keyspace of the table wait for it.
    tables: Mutex<TableLocks>,
    /// Tenants whose dropped tables' keys were swept in this process
    /// (`Engine::sweep_once`).
    swept: Mutex<std::collections::HashSet<u64>>,
    /// This node's cache of each sequence, per (tenant, object).
    seqs: Mutex<HashMap<(u64, u32), sequence::Cache>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("engine", &self.eng)
            .finish_non_exhaustive()
    }
}

static ENGINE: OnceLock<std::result::Result<Engine, String>> = OnceLock::new();

/// The process's SQL backend, on the node's engine (opened at first use).
///
/// # Errors
/// The engine failed to open.
pub(crate) fn engine() -> Result<&'static Engine> {
    ENGINE
        .get_or_init(|| {
            crate::node_engine::engine()
                .map(Engine::new)
                .map_err(|e| e.message)
        })
        .as_ref()
        .map_err(|e| SprigError::internal(format!("the storage engine is not available: {e}")))
}

/// The object number of `def` in keys (engine object numbers are 32-bit).
fn object_of(def: &TableDef) -> Result<u32> {
    u32::try_from(def.object.0)
        .map_err(|_| not_on_engine("a table whose object id needs more than 32 bits"))
}

/// A hidden row number counter's key: (tenant, object, row generation).
type RowIdKey = (u64, u32, u16);

/// A table's `AUTO_INCREMENT` counter in memory.
#[derive(Debug, Clone, Copy)]
struct AutoInc {
    /// The next value to give.
    next: u64,
    /// The leader epoch of the tenant's unit the counter was loaded under:
    /// a counter from another epoch is loaded again from the stored rows.
    epoch: u64,
}

/// Where the `AUTO_INCREMENT` counter of an empty table starts: the
/// definition's `AUTO_INCREMENT=n` option, or 1 after a `TRUNCATE` under
/// this definition.
fn start_of(sp: &Space, def: &TableDef) -> u64 {
    if sp.layout.restart_counter {
        1
    } else {
        def.auto_inc_start
    }
}

impl Engine {
    /// The SQL backend over `eng`.
    pub(crate) fn new(eng: sprig_engine::Engine) -> Self {
        Self {
            eng,
            autoinc: Mutex::new(HashMap::new()),
            rowids: Mutex::new(HashMap::new()),
            spaces: RwLock::new(HashMap::new()),
            tables: Mutex::new(HashMap::new()),
            swept: Mutex::new(std::collections::HashSet::new()),
            seqs: Mutex::new(HashMap::new()),
        }
    }

    /// A transaction with `opts`.
    pub(crate) fn begin(&self, opts: TxnOptions) -> Result<Tx> {
        Tx::begin(&self.eng, opts)
    }

    /// A session's transaction with `opts`.
    pub(crate) fn begin_session(&self, opts: TxnOptions) -> Result<Tx> {
        Tx::begin_session(&self.eng, opts)
    }

    /// The leader epoch under which this node leads `tenant`'s unit.
    ///
    /// # Errors
    /// The node doesn't lead it (retryable: the tenant moved).
    fn epoch_of(&self, tenant: u64) -> Result<u64> {
        match self
            .eng
            .leader_epoch(sprig_engine::Engine::unit_of_tenant(tenant))
        {
            0 => {
                let mut e =
                    SprigError::internal(format!("this node doesn't lead tenant {tenant}'s data"));
                e.retryable = true;
                Err(e)
            }
            e => Ok(e),
        }
    }

    /// The counter of `def` as the stored rows have it: after the largest
    /// stored value, at or above the table's start.
    ///
    /// A node that takes the tenant's leadership loads it again (a new
    /// epoch). The old leader's open transactions end with its leadership,
    /// and the new one takes the leadership only once it has applied every
    /// entry the old one committed, so every value in a row that can still
    /// commit is in the store.
    fn load_autoinc(&self, sp: &Space, def: &TableDef, epoch: u64) -> Result<AutoInc> {
        // The table's `AUTO_INCREMENT=n` option is where an empty table
        // starts.
        let next = (self.max_autoinc(sp, def)? + 1).max(start_of(sp, def));
        Ok(AutoInc { next, epoch })
    }

    /// `c` from memory, loaded again when missing or of another epoch.
    fn autoinc_of<'a>(
        &self,
        m: &'a mut HashMap<(u64, u32), AutoInc>,
        sp: &Space,
        def: &TableDef,
    ) -> Result<&'a mut AutoInc> {
        let epoch = self.epoch_of(sp.tenant)?;
        let key = (sp.tenant, sp.object);
        if m.get(&key).is_none_or(|c| c.epoch != epoch) {
            let c = self.load_autoinc(sp, def, epoch)?;
            m.insert(key, c);
        }
        m.get_mut(&key)
            .ok_or_else(|| SprigError::internal("the AUTO_INCREMENT counter went missing"))
    }

    /// The next `AUTO_INCREMENT` value of `def`, reserved.
    fn next_autoinc(&self, sp: &Space, def: &TableDef) -> Result<u64> {
        let mut m = self.autoinc.lock().unwrap_or_else(PoisonError::into_inner);
        let c = self.autoinc_of(&mut m, sp, def)?;
        let id = c.next;
        c.next = id.saturating_add(1);
        Ok(id)
    }

    /// The next hidden row number of a table without a primary key,
    /// reserved. The first one a process gives follows the largest row
    /// number stored (the last row key); after that the counter lives in
    /// memory. Numbers are never given twice in a generation of the rows,
    /// so a rolled-back insert leaves a gap, as InnoDB's row ids do.
    ///
    /// A node that takes the tenant's leadership seeds again from the
    /// stored rows: the old leader's open transactions were ended with its
    /// leadership, so no number it gave is in a row still to commit. A
    /// number of a deleted last row may be given again; nothing outside
    /// the table ever sees these numbers.
    fn next_rowid(&self, sp: &Space) -> Result<u64> {
        let key = (sp.tenant, sp.object, sp.layout.row_gen);
        let epoch = self.epoch_of(sp.tenant)?;
        let mut m = self.rowids.lock().unwrap_or_else(PoisonError::into_inner);
        let next = if let Some((n, _)) = m.get(&key).filter(|(_, e)| *e == epoch) {
            *n
        } else {
            let mut tx = self.begin(TxnOptions::default())?;
            let p = sp.rows();
            let end = keys::prefix_end(&p);
            let last = tx.scan(&p, &end, true, 1)?;
            match last.first() {
                Some((k, _)) => match row::hidden_of(k)? {
                    Datum::UInt(n) => n + 1,
                    _ => 1,
                },
                None => 1,
            }
        };
        m.insert(key, (next + 1, epoch));
        Ok(next)
    }

    /// An explicit value moves the counter past it.
    ///
    /// # Errors
    /// Loading the counter failed.
    fn seen_autoinc(&self, sp: &Space, def: &TableDef, v: &Datum) -> Result<()> {
        let v = match v {
            Datum::Int(i) => match u64::try_from(*i) {
                Ok(u) if u > 0 => u,
                _ => return Ok(()),
            },
            Datum::UInt(u) => *u,
            _ => return Ok(()),
        };
        let mut m = self.autoinc.lock().unwrap_or_else(PoisonError::into_inner);
        let c = self.autoinc_of(&mut m, sp, def)?;
        c.next = c.next.max(v.saturating_add(1));
        Ok(())
    }

    /// The largest stored value of the `AUTO_INCREMENT` column (read once
    /// per table and process; the counter lives in memory after that).
    fn max_autoinc(&self, sp: &Space, def: &TableDef) -> Result<u64> {
        let Some(ac) = def.auto_inc else {
            return Ok(0);
        };
        let as_u64 = |d: &Datum| match d {
            Datum::Int(i) => u64::try_from(*i).unwrap_or(0),
            Datum::UInt(u) => *u,
            _ => 0,
        };
        let mut tx = self.begin(TxnOptions::default())?;
        let p = sp.rows();
        let end = keys::prefix_end(&p);
        // The key's only column: the last row has the largest value.
        if def.pk == [ac] {
            let last = tx.scan(&p, &end, true, 1)?;
            return Ok(last
                .first()
                .and_then(|(k, _)| keys::decode_int_pk(def, sp.first_format, &k[p.len()..]))
                .map_or(0, |pk| as_u64(&pk)));
        }
        let mut max = 0;
        let mut lo = p;
        loop {
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            for (k, v) in &batch {
                let r = row::decode(def, sp, k, v)?;
                max = max.max(as_u64(&r[ac]));
            }
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH => lo = keys::prefix_end(k),
                _ => break,
            }
        }
        Ok(max)
    }

    /// The next `AUTO_INCREMENT` value table `def` would give (what
    /// `SHOW CREATE TABLE` shows), without reserving it; `None` without an
    /// `AUTO_INCREMENT` column.
    pub(crate) async fn next_counter(&self, tenant: u64, def: &TableDef) -> Result<Option<u64>> {
        if def.auto_inc.is_none() {
            return Ok(None);
        }
        let sp = self.space(tenant, def).await?;
        let epoch = self.epoch_of(tenant)?;
        let known = self
            .autoinc
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(sp.tenant, sp.object))
            .filter(|c| c.epoch == epoch)
            .copied();
        Ok(Some(match known {
            Some(c) => c.next,
            None => self.load_autoinc(&sp, def, epoch)?.next,
        }))
    }

    /// How many rows table `def` has, counted up to `cap`.
    pub(crate) async fn row_count(&self, tenant: u64, def: &TableDef, cap: u64) -> Result<u64> {
        let sp = self.space(tenant, def).await?;
        let mut tx = self.begin(TxnOptions::default())?;
        let p = sp.rows();
        let end = keys::prefix_end(&p);
        let mut lo = p;
        let mut n: u64 = 0;
        let mut pace = Pace::default();
        loop {
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            n += batch.len() as u64;
            pace.add(batch.len()).await;
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH && n < cap => lo = keys::prefix_end(k),
                _ => break,
            }
        }
        Ok(n)
    }

    /// The lock of table `object` of `tenant`.
    fn table_lock(&self, tenant: u64, object: u32) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.tables
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry((tenant, object))
                .or_default(),
        )
    }

    /// The cached keyspace of `def`, when it was worked out for this
    /// definition.
    fn cached(&self, tenant: u64, object: u32, def: &TableDef) -> Option<Space> {
        self.spaces
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(tenant, object))
            .filter(|c| c.version == def.version && c.sigs == def.index_sigs)
            .map(|c| c.space(tenant, object))
    }

    /// The keyspace of `def` for `tenant`: its row format and layout, and
    /// its index numbers from the table's index record. An index the record doesn't have yet is given
    /// a number and, when the table has rows, built first (an index created
    /// after rows were inserted is built by the next statement on the
    /// table). A definition change the catalog published and the engine
    /// didn't apply yet is applied first.
    pub(crate) async fn space(&self, tenant: u64, def: &TableDef) -> Result<Space> {
        let object = object_of(def)?;
        if let Some(sp) = self.cached(tenant, object, def) {
            return Ok(sp);
        }
        let lock = self.table_lock(tenant, object);
        let _build = lock.lock().await;
        if let Some(sp) = self.cached(tenant, object, def) {
            return Ok(sp);
        }
        self.space_locked(tenant, object, def).await
    }

    /// [`Engine::space`] for a caller that holds the table's lock.
    async fn space_locked(&self, tenant: u64, object: u32, def: &TableDef) -> Result<Space> {
        let key = (tenant, object);
        let wanted = &def.index_sigs;
        let layout = Arc::new(self.layout_of(tenant, object, def).await?);
        let first_format = self.row_format(tenant, object, def, layout.row_gen)?;
        let rk = Bytes::from(keys::index_record_key(tenant, object));
        let mut tx = self.begin(TxnOptions::default())?;
        let record = match tx.get(&rk)? {
            Some(raw) => Record::decode(&raw).ok_or_else(|| {
                SprigError::internal("engine: a table's index record doesn't decode")
            })?,
            None => Record::default(),
        };
        drop(tx);
        let (ids, fresh, next) = record.assign(wanted);
        // The same numbers stay the same shared list: the statements and
        // transactions holding it stay counted as the table's users.
        let known = self
            .spaces
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .filter(|c| c.ids[..] == ids[..])
            .map(|c| (Arc::clone(&c.ids), Arc::clone(&c.id_copies)));
        let (ids, id_copies) = known.unwrap_or_else(|| (Arc::from(ids), slots().into()));
        let sp = Space {
            tenant,
            object,
            ids,
            first_format,
            layout,
        };
        if next != record {
            if !fresh.is_empty() {
                self.build(&sp, def, &fresh).await?;
            }
            let mut tx = self.begin(TxnOptions::default())?;
            tx.put_new(rk, next.encode()).await?;
            tx.finish().await?;
            // The entries of indexes the definition no longer has.
            for e in &record.entries {
                if !next.entries.iter().any(|n| n.id == e.id) {
                    self.clear(&sp.index_of(e.id, e.sig.unique)).await?;
                }
            }
        }
        self.spaces
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                key,
                Cached {
                    sigs: wanted.clone(),
                    version: def.version,
                    ids: Arc::clone(&sp.ids),
                    id_copies,
                    first_format,
                    layout: Arc::clone(&sp.layout),
                    copies: slots(),
                },
            );
        Ok(sp)
    }

    /// Refuses a table whose rows are in the first (prototype) row format:
    /// its marker says so. That format is no longer read (development
    /// stacks only); such a table is dropped and created again.
    fn row_format(&self, tenant: u64, object: u32, def: &TableDef, _row_gen: u16) -> Result<bool> {
        let fk = Bytes::from(keys::format_key(tenant, object));
        let mut tx = self.begin(TxnOptions::default())?;
        if let Some(raw) = tx.get(&fk)?
            && raw.first() == Some(&keys::FORMAT_FIRST)
        {
            return Err(SprigError::internal(format!(
                "engine: table {} holds rows in the prototype row format, which is no longer read: drop it and create it again",
                def.name
            )));
        }
        Ok(false)
    }

    /// Writes the entries of indexes `which` (positions in `def`) for every
    /// row, one transaction per batch of rows.
    async fn build(&self, sp: &Space, def: &TableDef, which: &[usize]) -> Result<()> {
        let p = sp.rows();
        let end = keys::prefix_end(&p);
        let mut lo = p;
        // One wait for durability, for the last batch.
        let last = loop {
            let mut tx = self.begin(TxnOptions::default())?;
            let batch = tx.scan(&lo, &end, false, WALK_BATCH)?;
            for (k, v) in &batch {
                let r = row::decode(def, sp, k, v)?;
                for &ix in which {
                    let Some((k, v)) = keys::index_entry(sp, def, ix, &r) else {
                        return Err(SprigError::internal(
                            "engine: an index value of the wrong type",
                        ));
                    };
                    let k = Bytes::from(k);
                    if def.indexes[ix].unique && !v.is_empty() {
                        if !tx.insert(k, v, false).await? {
                            let x = &def.indexes[ix];
                            return Err(exec::duplicate(def, &x.cols, &r, &x.name));
                        }
                    } else {
                        tx.put_new(k, v).await?;
                    }
                }
            }
            let queued = tx.commit_queued()?;
            // Between batches the worker goes back to the runtime: a long
            // build must not hold it (the stall watchdog stops a statement
            // whose one poll runs past its limit, with 1317).
            tokio::task::yield_now().await;
            match batch.last() {
                Some((k, _)) if batch.len() == WALK_BATCH => lo = keys::prefix_end(k),
                _ => break queued,
            }
        };
        last.wait_async().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
