//! `FULLTEXT` keys of engine tables: an apply-time index each
//! ([`super::applyidx`]), and `MATCH ... AGAINST` over them as `InnoDB`
//! answers it ([`sprig_search::innodb`]).
//!
//! ```text
//! <engine dir>/idx/u<unit>/t<object>/k<key name, hex>-g<generation>/
//!     def.json      what the index is of (so the node reopens it alone)
//!     Tantivy files (sprig_search::ftindex), payload = the log index covered
//! ```
//!
//! - **Definitions.** A `FULLTEXT` key is part of the table's definition
//!   (`TableDef::fulltext`); the index follows the table's rows under the
//!   row generation it was built for. Whenever a statement or a finished
//!   DDL statement sees a definition the index doesn't match (another row
//!   generation, other columns, another collation), the index is built
//!   again as a new generation; keys that went are dropped with their
//!   files and floors ([`Manager::sync_table`]). Word rules (token sizes,
//!   stop words) are fixed when the index is built, as `InnoDB` fixes
//!   them.
//! - **Words** compare by the first column's collation: a word's term is
//!   the hexadecimal of its collation weights ([`CollationFold`]), so
//!   `_ai_ci` folds case and accents and a prefix stays a prefix.
//! - **Reading.** `MATCH` sees every commit of the table's unit up to the
//!   statement's start (`latest`), never the statement's own uncommitted
//!   changes, as `InnoDB`. One search per statement and query text
//!   ([`StmtSearch`]); a row's score comes from its primary key.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};
use sprig_engine::UnitId;
use sprig_engine::keys::collation::Collation;
use sprig_engine::keys::typed;
use sprig_engine::row::record::{self, Column as RecordColumn, RowSchema};
use sprig_engine::txn::TxnOptions;
use sprig_exec::{FtMode, FtSpec, FullText, SqlError};
use sprig_search::ftindex::{FtStore, Hit, MIN_WRITER_BYTES};
use sprig_search::innodb::{self, Analyzer, COLUMN_GAP, Fold, Parser, Rules};

use super::applyidx::{self, Dispatcher, Handle, IndexStore, Phase};
use super::keys::{self, Space};
use super::layout::{Layout, LayoutRecord};
use super::row::{Datum, hidden_of};
use super::store::Tx;
use super::table::{FtKey, TableDef};

/// The rows of the first search whose words `WITH QUERY EXPANSION` adds (all
/// of them on both servers; a bound for safety).
const EXPANSION_ROWS: usize = 10_000;
/// How long a statement waits for an index being built.
const BUILD_WAIT: Duration = Duration::from_secs(60);
/// How long a statement waits for an index to cover the latest commits
/// (`search.wait_timeout_ms`).
const FRESH_WAIT: Duration = Duration::from_secs(2);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Words compared by a collation: a term is the hexadecimal of the word's
/// weights, level after level.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CollationFold {
    coll: Collation,
    /// Hex digits per weight.
    width: usize,
}

impl CollationFold {
    pub(crate) fn new(coll: Collation) -> Self {
        // Weights of the `_bin` collations of multi-byte character sets are
        // code points; every other collation's fit in 16 bits.
        let width = if coll.name().ends_with("_bin") { 6 } else { 4 };
        Self { coll, width }
    }
}

impl Fold for CollationFold {
    fn term(&self, word: &str) -> String {
        use std::fmt::Write as _;
        let mut out = String::with_capacity(word.len() * self.width);
        let mut w = Vec::with_capacity(word.len());
        for level in 0..self.coll.levels() {
            w.clear();
            self.coll.level_weights(word.as_bytes(), level, &mut w);
            if level > 0 {
                out.push('.');
            }
            for x in &w {
                let _ = write!(out, "{x:0width$x}", width = self.width);
            }
        }
        out
    }
}

/// What one index is of: written beside its files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FtDef {
    pub tenant: u64,
    pub object: u32,
    /// The row generation the index follows.
    pub row_gen: u16,
    /// The key's name.
    pub key: String,
    /// The key's columns, by permanent column id, in key order.
    pub cols: Vec<u32>,
    /// The primary key's columns, by permanent column id; empty with a
    /// hidden row number.
    pub pk: Vec<u32>,
    /// The words' collation.
    pub collation: String,
    pub min: usize,
    pub max: usize,
    pub stopwords: bool,
    /// `WITH PARSER ngram`: the gram size.
    pub ngram: Option<usize>,
    /// Raised at every rebuild.
    pub generation: u64,
    /// The table's layout record as a table whose columns never changed
    /// has it (hex): how rows are read when the table has no record.
    #[serde(default)]
    pub first: String,
}

impl FtDef {
    /// The same index (all but the generation).
    fn same(&self, o: &Self) -> bool {
        Self {
            generation: 0,
            first: String::new(),
            ..self.clone()
        } == Self {
            generation: 0,
            first: String::new(),
            ..o.clone()
        }
    }

    fn rules(&self) -> Rules {
        Rules {
            min: self.min,
            max: self.max,
            stopwords: if self.stopwords {
                Rules::default().stopwords
            } else {
                Vec::new()
            },
            parser: self.ngram.map_or(Parser::Words, Parser::Ngram),
        }
    }

    fn analyzer(&self) -> Result<Analyzer> {
        let coll = Collation::from_name(&self.collation).ok_or_else(|| {
            SprigError::internal(format!(
                "full-text index: unknown collation {}",
                self.collation
            ))
        })?;
        Ok(Analyzer::new(
            self.rules(),
            Arc::new(CollationFold::new(coll)),
        ))
    }

    fn unit(&self) -> UnitId {
        sprig_engine::Engine::unit_of_tenant(self.tenant)
    }

    fn dir(&self, root: &Path) -> PathBuf {
        root.join(format!("u{}", self.unit().0))
            .join(format!("t{}", self.object))
            .join(format!("k{}-g{}", hex(&self.key), self.generation))
    }

    fn floor_name(&self) -> String {
        format!(
            "ai/ft/{}/{}/{}/{}",
            self.tenant,
            self.object,
            hex(&self.key),
            self.generation
        )
    }

    fn prefix(&self) -> Vec<u8> {
        keys::rows_prefix(self.tenant, self.object, self.row_gen)
    }
}

fn hex(s: &str) -> String {
    use std::fmt::Write as _;
    let mut o = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        let _ = write!(o, "{b:02x}");
    }
    o
}

/// The definition `key` of `def` asks for, under layout `layout`.
fn wanted(tenant: u64, object: u32, def: &TableDef, layout: &Layout, key: &FtKey) -> Option<FtDef> {
    let cols = layout.current.columns();
    let id_of = |pos: usize| cols.get(pos).map(|c| c.id);
    let ids: Option<Vec<u32>> = key.cols.iter().map(|&p| id_of(p)).collect();
    let pk: Option<Vec<u32>> = if def.hidden {
        Some(Vec::new())
    } else {
        def.pk.iter().map(|&p| id_of(p)).collect()
    };
    let collation = key
        .cols
        .first()
        .and_then(|&p| def.columns.get(p))
        .and_then(|c| c.collation)?
        .name()
        .to_owned();
    let ngram = match key.parser.as_deref() {
        Some("ngram") => Some(2),
        _ => None,
    };
    Some(FtDef {
        tenant,
        object,
        row_gen: layout.row_gen,
        key: key.name.clone(),
        cols: ids?,
        pk: pk?,
        collation,
        min: innodb::MIN_TOKEN,
        max: innodb::MAX_TOKEN,
        stopwords: true,
        ngram,
        generation: 0,
        first: hex_bytes(&LayoutRecord::first(def).encode()),
    })
}

fn hex_bytes(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut o = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(o, "{x:02x}");
    }
    o
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| s.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
        .collect()
}

/// How rows of one layout are read for an index.
struct Extract {
    layout: Layout,
    /// Positions of the key's columns in the layout's current list.
    cols: Vec<usize>,
    /// Positions of the primary key's columns.
    pk: Vec<usize>,
    /// The primary key's values as `_pk` holds them.
    pk_schema: RowSchema,
}

fn extract_of(def: &FtDef, layout: Layout) -> Result<Extract> {
    let cols = layout.current.columns();
    let pos = |id: u32| {
        cols.iter()
            .position(|c| c.id == id)
            .ok_or_else(|| SprigError::internal("full-text index: a column of the key is gone"))
    };
    let ft = def
        .cols
        .iter()
        .map(|&i| pos(i))
        .collect::<Result<Vec<_>>>()?;
    let pk = def.pk.iter().map(|&i| pos(i)).collect::<Result<Vec<_>>>()?;
    let pk_cols: Vec<RecordColumn> = if def.pk.is_empty() {
        vec![RecordColumn::new(1, super::table::HIDDEN_TYPE)]
    } else {
        pk.iter()
            .enumerate()
            .map(|(i, &p)| RecordColumn::new(u32::try_from(i + 1).unwrap_or(u32::MAX), cols[p].ty))
            .collect()
    };
    let pk_schema = RowSchema::new(1, pk_cols)?;
    Ok(Extract {
        layout,
        cols: ft,
        pk,
        pk_schema,
    })
}

/// The kind of apply-time index of one `FULLTEXT` key.
pub(crate) struct FtKind {
    eng: sprig_engine::Engine,
    def: FtDef,
    store: FtStore,
    an: Analyzer,
    ex: Mutex<Extract>,
}

/// The table's layout as its record says now (`None`: it has none).
fn kind_layout(eng: &sprig_engine::Engine, def: &FtDef) -> Result<Option<Layout>> {
    let mut tx = Tx::begin(eng, TxnOptions::default())?;
    let raw = tx.get(&keys::layout_key(def.tenant, def.object))?;
    drop(tx);
    match raw {
        None => Ok(None),
        Some(raw) => LayoutRecord::decode(&raw)
            .ok_or_else(|| SprigError::internal("engine: a table's layout record doesn't decode"))?
            .layout(0)
            .map(Some),
    }
}

impl FtKind {
    /// The table's layout as its record says now.
    fn read_layout(&self, eng: &sprig_engine::Engine) -> Result<Option<Layout>> {
        kind_layout(eng, &self.def)
    }

    fn decode(&self, row: &Bytes) -> Result<Vec<Datum>> {
        let first = lock(&self.ex).layout.decode(row);
        match first {
            Ok(v) => Ok(v),
            Err(e) => {
                // A row of a column list this index hasn't seen: the
                // table's layout record has it.
                let Some(l) = self.read_layout(&self.eng)? else {
                    return Err(e);
                };
                let ex = extract_of(&self.def, l)?;
                let v = ex.layout.decode(row)?;
                *lock(&self.ex) = ex;
                Ok(v)
            }
        }
    }
}

impl FtKind {
    /// The indexed words of the rows of `keys` as they are now.
    fn words_of(&self, keys: &[Vec<u8>]) -> Result<Vec<String>> {
        let mut tx = Tx::begin(&self.eng, TxnOptions::default())?;
        let mut out = Vec::new();
        for k in keys {
            let mut full = self.def.prefix();
            full.extend_from_slice(k);
            let Some(row) = tx.get(&full)? else {
                continue;
            };
            let vals = self.decode(&row)?;
            let ex = lock(&self.ex);
            let mut toks = Vec::new();
            for &c in &ex.cols {
                if let Some(Datum::Str(b) | Datum::Bytes(b)) = vals.get(c) {
                    self.an.tokens(&String::from_utf8_lossy(b), 0, &mut toks);
                }
            }
            out.extend(toks.into_iter().map(|t| t.term));
        }
        Ok(out)
    }
}

impl IndexStore for FtKind {
    fn upsert(&self, key: &[u8], row: &Bytes, seq: u64) -> Result<()> {
        let vals = self.decode(row).map_err(|e| {
            SprigError::internal(format!(
                "{} (key {:02x?}, row {:02x?}, seq {seq})",
                e.message,
                &key[..key.len().min(12)],
                &row[..row.len().min(24)]
            ))
        })?;
        let ex = lock(&self.ex);
        let mut toks = Vec::new();
        let mut pos = 0u32;
        for &c in &ex.cols {
            match vals.get(c) {
                Some(Datum::Str(b) | Datum::Bytes(b)) => {
                    let text = String::from_utf8_lossy(b);
                    pos = self.an.tokens(&text, pos, &mut toks) + COLUMN_GAP;
                }
                _ => pos += COLUMN_GAP,
            }
        }
        let pk: Vec<Datum> = if self.def.pk.is_empty() {
            let mut full = self.def.prefix();
            full.extend_from_slice(key);
            vec![hidden_of(&full)?]
        } else {
            ex.pk
                .iter()
                .map(|&p| vals.get(p).cloned().unwrap_or(Datum::Null))
                .collect()
        };
        let mut pk_bytes = Vec::with_capacity(32);
        record::encode(&ex.pk_schema, &pk, None, &mut pk_bytes)?;
        drop(ex);
        self.store.upsert(key, &pk_bytes, seq, toks)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.store.delete(key)
    }

    fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    fn commit(&self, applied: u64) -> Result<()> {
        self.store.commit(applied)
    }
}

/// One index: its definition, files and handle.
pub(crate) struct FtIndex {
    def: FtDef,
    kind: Arc<FtKind>,
    handle: Arc<Handle>,
    dir: PathBuf,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for FtIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtIndex")
            .field("def", &self.def)
            .finish_non_exhaustive()
    }
}

struct TableIdx {
    /// The catalog version of the definition last synced (0: recovered
    /// from disk, not checked yet).
    version: u64,
    indexes: Vec<Arc<FtIndex>>,
}

/// The node's full-text indexes.
pub(crate) struct Manager {
    d: Arc<Dispatcher>,
    root: PathBuf,
    tables: Mutex<HashMap<(u64, u32), TableIdx>>,
}

static MANAGER: OnceLock<Arc<Manager>> = OnceLock::new();

/// The node's manager, once [`start`] ran.
pub(crate) fn manager() -> Option<&'static Arc<Manager>> {
    MANAGER.get()
}

/// Starts the apply-time indexes on `eng` (once per process): reopens the
/// full-text indexes on disk and replays what they miss.
pub(crate) fn start(eng: &sprig_engine::Engine) {
    if MANAGER.get().is_some() {
        return;
    }
    match Manager::open(eng, true) {
        Ok(m) => {
            let _ = MANAGER.set(m);
        }
        Err(e) => {
            tracing::error!(error = %e, "apply-time indexes didn't start; full-text searches fail until a restart");
        }
    }
}

fn remove_dir(dir: &Path) {
    if let Err(e) = std::fs::remove_dir_all(dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(dir = %dir.display(), error = %e, "full-text index files not removed");
    }
}

/// Every index directory under `root` with its definition.
fn on_disk(root: &Path) -> Vec<(FtDef, PathBuf)> {
    let mut out = Vec::new();
    let Ok(units) = std::fs::read_dir(root) else {
        return out;
    };
    for u in units.flatten() {
        let Ok(tables) = std::fs::read_dir(u.path()) else {
            continue;
        };
        for t in tables.flatten() {
            let Ok(idx) = std::fs::read_dir(t.path()) else {
                continue;
            };
            for i in idx.flatten() {
                let dir = i.path();
                match std::fs::read(dir.join("def.json"))
                    .ok()
                    .and_then(|b| serde_json::from_slice::<FtDef>(&b).ok())
                {
                    Some(def) => out.push((def, dir)),
                    None => remove_dir(&dir),
                }
            }
        }
    }
    out
}

/// Opens an index found on disk: its kind and the entry its files cover
/// (`None`: never committed, or the log no longer reaches back to it).
fn reopen(
    eng: &sprig_engine::Engine,
    def: &FtDef,
    dir: &Path,
) -> Result<Option<(Arc<FtKind>, u64)>> {
    let store = FtStore::open(Some(dir), MIN_WRITER_BYTES)?;
    let Some(payload) = store.applied()? else {
        return Ok(None);
    };
    let floor = eng.log_floor_of(def.unit(), &def.floor_name()).unwrap_or(0);
    let from = payload.max(floor);
    if from < eng.last_applied_of(def.unit())
        && eng
            .log_first_index_of(def.unit())
            .is_none_or(|f| f > from + 1)
    {
        let _ = eng.clear_log_floor(&def.floor_name());
        return Ok(None);
    }
    let kind = kind_of(eng, def, store, None)?;
    Ok(Some((kind, from)))
}

fn kind_of(
    eng: &sprig_engine::Engine,
    def: &FtDef,
    store: FtStore,
    layout: Option<Layout>,
) -> Result<Arc<FtKind>> {
    let layout = match layout {
        Some(l) => l,
        // Reopened: the table's layout record, or the first layout of a
        // table that has none.
        None => match kind_layout(eng, def)? {
            Some(l) => l,
            None => unhex(&def.first)
                .and_then(|b| LayoutRecord::decode(&b))
                .map(|r| r.layout(0))
                .transpose()?
                .ok_or_else(|| SprigError::internal("full-text index: no row layout"))?,
        },
    };
    let ex = match extract_of(def, layout.clone()) {
        Ok(ex) => ex,
        Err(_) => Extract {
            layout,
            cols: Vec::new(),
            pk: Vec::new(),
            pk_schema: RowSchema::new(1, Vec::new())?,
        },
    };
    Ok(Arc::new(FtKind {
        eng: eng.clone(),
        an: def.analyzer()?,
        def: def.clone(),
        store,
        ex: Mutex::new(ex),
    }))
}

impl Manager {
    /// The manager of `eng`'s full-text indexes (under its data
    /// directory): reopens the ones on disk and starts observing the
    /// engine's applies. `node`: the process's own dispatcher (the node
    /// engine); otherwise one of its own (tests).
    ///
    /// # Errors
    /// The observer couldn't start.
    pub(crate) fn open(eng: &sprig_engine::Engine, node: bool) -> Result<Arc<Self>> {
        let root = eng.data_dir().join("idx");
        let mut opened: Vec<Arc<FtIndex>> = Vec::new();
        // Registered before the observer replays the log, so the replay
        // reaches them.
        let recover = |d: &Arc<Dispatcher>| {
            let mut lows = Vec::new();
            for (def, dir) in on_disk(&root) {
                match reopen(eng, &def, &dir) {
                    Ok(Some((kind, from))) => {
                        lows.push((def.unit(), from));
                        let handle = d.register(
                            def.floor_name(),
                            def.unit(),
                            def.prefix(),
                            Arc::clone(&kind) as Arc<dyn IndexStore>,
                            Some(from),
                        );
                        opened.push(Arc::new(FtIndex {
                            def,
                            kind,
                            handle,
                            dir,
                        }));
                    }
                    Ok(None) => remove_dir(&dir),
                    Err(e) => {
                        tracing::warn!(dir = %dir.display(), error = %e, "full-text index not reopened; it is built again when used");
                        remove_dir(&dir);
                    }
                }
            }
            lows
        };
        let d = if node {
            Arc::clone(applyidx::start(eng, recover)?)
        } else {
            applyidx::attach(eng, recover)?
        };
        let m = Arc::new(Manager {
            d,
            root,
            tables: Mutex::new(HashMap::new()),
        });
        {
            let mut t = lock(&m.tables);
            for ix in opened {
                t.entry((ix.def.tenant, ix.def.object))
                    .or_insert_with(|| TableIdx {
                        version: 0,
                        indexes: Vec::new(),
                    })
                    .indexes
                    .push(ix);
            }
        }
        Ok(m)
    }

    /// The dispatcher.
    pub(crate) fn dispatcher(&self) -> &Arc<Dispatcher> {
        &self.d
    }

    fn drop_index(&self, ix: &FtIndex) {
        self.d.drop_index(&ix.handle);
        remove_dir(&ix.dir);
    }

    /// Brings `def`'s indexes in line with its `FULLTEXT` keys: builds the
    /// missing ones (started; not waited for), drops the ones that went.
    /// Returns them, in the definition's key order.
    ///
    /// # Errors
    /// An index couldn't be created.
    pub(crate) fn sync_table(
        &self,
        tenant: u64,
        def: &TableDef,
        sp: &Space,
    ) -> Result<Vec<Arc<FtIndex>>> {
        let object = sp.object;
        let mut tables = lock(&self.tables);
        if let Some(t) = tables.get(&(tenant, object))
            && t.version == def.version
            && t.indexes.len() == def.fulltext.len()
        {
            return Ok(t.indexes.clone());
        }
        let had = tables
            .remove(&(tenant, object))
            .map(|t| t.indexes)
            .unwrap_or_default();
        let mut keep: Vec<Arc<FtIndex>> = Vec::with_capacity(def.fulltext.len());
        let mut used = vec![false; had.len()];
        let mut out = Vec::with_capacity(def.fulltext.len());
        for key in &def.fulltext {
            let Some(mut want) = wanted(tenant, object, def, &sp.layout, key) else {
                return Err(SprigError::internal(
                    "full-text index: a key column without a collation",
                ));
            };
            if let Some(i) = had.iter().enumerate().position(|(i, x)| {
                !used[i]
                    && x.def.same(&want)
                    && !matches!(x.handle.phase(), Phase::Failed(_) | Phase::Stopped)
            }) {
                used[i] = true;
                // Rows of the current layout read through.
                if let Ok(ex) = extract_of(&had[i].def, (*sp.layout).clone()) {
                    *lock(&had[i].kind.ex) = ex;
                }
                keep.push(Arc::clone(&had[i]));
                out.push(Arc::clone(&had[i]));
                continue;
            }
            want.generation = had
                .iter()
                .filter(|x| x.def.key.eq_ignore_ascii_case(&want.key))
                .map(|x| x.def.generation + 1)
                .max()
                .unwrap_or(0);
            let ix = self.create(want, (*sp.layout).clone())?;
            keep.push(Arc::clone(&ix));
            out.push(ix);
        }
        for (i, x) in had.iter().enumerate() {
            if !used[i] {
                self.drop_index(x);
            }
        }
        if !keep.is_empty() {
            tables.insert(
                (tenant, object),
                TableIdx {
                    version: def.version,
                    indexes: keep,
                },
            );
        }
        Ok(out)
    }

    fn create(&self, def: FtDef, layout: Layout) -> Result<Arc<FtIndex>> {
        let dir = def.dir(&self.root);
        remove_dir(&dir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| SprigError::internal(format!("full-text index directory: {e}")))?;
        let json = serde_json::to_vec_pretty(&def)
            .map_err(|e| SprigError::internal(format!("full-text index definition: {e}")))?;
        std::fs::write(dir.join("def.json"), json)
            .map_err(|e| SprigError::internal(format!("full-text index definition: {e}")))?;
        let store = FtStore::open(Some(&dir), MIN_WRITER_BYTES)?;
        let kind = kind_of(self.d.engine(), &def, store, Some(layout))?;
        let handle = self.d.register(
            def.floor_name(),
            def.unit(),
            def.prefix(),
            Arc::clone(&kind) as Arc<dyn IndexStore>,
            None,
        );
        Ok(Arc::new(FtIndex {
            def,
            kind,
            handle,
            dir,
        }))
    }

    /// Stops every index's worker, keeping files and floors.
    #[cfg(test)]
    pub(crate) fn shutdown(&self) {
        let tables = std::mem::take(&mut *lock(&self.tables));
        for t in tables.into_values() {
            for ix in t.indexes {
                self.d.stop(&ix.handle);
            }
        }
    }

    /// Drops every index of `tenant`'s tables, with its files and floors,
    /// and whatever else is left under the tenant unit's directory: the
    /// tenant was deleted and its unit left this node.
    pub(crate) fn forget_tenant(&self, tenant: u64) {
        self.retain_tables(tenant, &|_| false);
        remove_dir(&self.root.join(format!("u{tenant}")));
    }

    /// Drops every index of tables of `tenant` that `keep` doesn't name
    /// (tables dropped, or with no `FULLTEXT` key any more).
    pub(crate) fn retain_tables(&self, tenant: u64, keep: &dyn Fn(u32) -> bool) {
        let mut tables = lock(&self.tables);
        let gone: Vec<(u64, u32)> = tables
            .keys()
            .filter(|(t, o)| *t == tenant && !keep(*o))
            .copied()
            .collect();
        for k in gone {
            if let Some(t) = tables.remove(&k) {
                for ix in &t.indexes {
                    self.drop_index(ix);
                }
            }
        }
    }
}

impl FtIndex {
    /// The key's name.
    pub(crate) fn name(&self) -> &str {
        &self.def.key
    }

    /// Waits until the index is built and covers every commit of its unit
    /// a statement starting now may see.
    fn ready(&self, d: &Dispatcher) -> Result<()> {
        if self.handle.phase() == Phase::Building {
            self.handle.wait_built(BUILD_WAIT)?;
        }
        let target = d.target(self.def.unit());
        self.handle.sync(d, target, FRESH_WAIT)
    }
}

/// Brings the `FULLTEXT` indexes of `tenant`'s engine tables in line with
/// the catalog `cat` after a DDL statement, and waits for the builds it
/// started (the servers build a `FULLTEXT` key before they answer).
pub(crate) async fn after_ddl(tenant: u64, cat: &super::catalog::EngineCatalog) {
    let Some(m) = manager() else {
        return;
    };
    let Ok(eng) = super::engine() else {
        return;
    };
    let mut with: Vec<u32> = Vec::new();
    let mut builds = Vec::new();
    for (obj, parsed) in &cat.tables {
        let Ok(def) = parsed.as_ref().as_ref() else {
            continue;
        };
        if def.fulltext.is_empty() {
            continue;
        }
        let Ok(object) = u32::try_from(obj.0) else {
            continue;
        };
        with.push(object);
        let sp = match eng.space(tenant, def).await {
            Ok(sp) => sp,
            Err(e) => {
                tracing::warn!(tenant, object, error = %e, "full-text indexes not checked after a DDL statement");
                continue;
            }
        };
        match m.sync_table(tenant, def, &sp) {
            Ok(ixs) => builds.extend(ixs),
            Err(e) => {
                tracing::warn!(tenant, object, error = %e, "full-text index not built");
            }
        }
    }
    m.retain_tables(tenant, &|o| with.contains(&o));
    for ix in builds {
        if ix.handle.phase() == Phase::Building {
            let h = Arc::clone(&ix.handle);
            let _ =
                tokio::task::spawn_blocking(move || h.wait_built(Duration::from_secs(3600))).await;
        }
    }
}

// ---------------------------------------------------------------------------
// One statement's searches.

/// A table a statement reads, as its `MATCH` calls name it.
#[derive(Debug, Clone)]
pub(crate) struct StmtTable {
    /// The planner's database and table names.
    pub schema: String,
    pub name: String,
    /// The tenant whose keyspace holds it.
    pub tenant: u64,
    pub def: Arc<TableDef>,
    pub sp: Space,
}

/// One search's answer.
struct Answer {
    index: String,
    /// Score per row key (after the prefix).
    scores: HashMap<Vec<u8>, f32>,
    /// The rows found, best first.
    hits: Vec<Hit>,
    pk_schema: RowSchema,
}

/// `MATCH ... AGAINST` for one statement ([`sprig_exec::FullText`]).
#[derive(Debug)]
pub(crate) struct StmtSearch {
    m: Arc<Manager>,
    tables: Vec<StmtTable>,
    answers: Mutex<Answers>,
}

/// One statement's searches already run, by `MATCH` and query text.
type Answers = HashMap<(FtSpec, Vec<u8>), Arc<AnswerBox>>;

#[derive(Debug)]
struct AnswerBox(Answer);

impl std::fmt::Debug for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Answer")
            .field("index", &self.index)
            .field("hits", &self.hits.len())
            .finish_non_exhaustive()
    }
}

fn sql_err(e: &SprigError) -> SqlError {
    let p = e.protocol();
    let state = match &p.sqlstate {
        b"42000" => "42000",
        b"40001" => "40001",
        _ => "HY000",
    };
    SqlError::new(p.mysql_errno, state, e.message.clone())
}

fn no_index() -> SqlError {
    SqlError::new(
        1191,
        "HY000",
        "Can't find FULLTEXT index matching the column list",
    )
}

impl StmtSearch {
    /// The searches of a statement reading `tables`; `None` when none of
    /// them has a `FULLTEXT` key.
    pub(crate) fn new(m: &Arc<Manager>, tables: Vec<StmtTable>) -> Option<Arc<Self>> {
        tables.iter().any(|t| !t.def.fulltext.is_empty()).then(|| {
            Arc::new(Self {
                m: Arc::clone(m),
                tables,
                answers: Mutex::new(HashMap::new()),
            })
        })
    }

    fn table(&self, spec: &FtSpec) -> Option<&StmtTable> {
        self.tables
            .iter()
            .find(|t| t.schema == spec.schema && t.name == spec.table)
    }

    /// The key of `t` covering exactly `spec`'s columns.
    fn key<'t>(t: &'t StmtTable, spec: &FtSpec) -> Option<&'t FtKey> {
        let mut want: Vec<usize> = spec
            .columns
            .iter()
            .map(|c| t.def.column(c))
            .collect::<Option<Vec<_>>>()?;
        want.sort_unstable();
        want.dedup();
        t.def.fulltext.iter().find(|k| {
            let mut have = k.cols.clone();
            have.sort_unstable();
            have == want
        })
    }

    fn answer(&self, spec: &FtSpec, query: &[u8]) -> Result<Arc<AnswerBox>, SqlError> {
        let k = (spec.clone(), query.to_vec());
        if let Some(a) = lock(&self.answers).get(&k) {
            return Ok(Arc::clone(a));
        }
        let t = self.table(spec).ok_or_else(no_index)?;
        let key = Self::key(t, spec).ok_or_else(no_index)?;
        let m = &self.m;
        let ixs = m
            .sync_table(t.tenant, &t.def, &t.sp)
            .map_err(|e| sql_err(&e))?;
        let ix = ixs
            .into_iter()
            .find(|x| x.def.key == key.name)
            .ok_or_else(no_index)?;
        let wait = || ix.ready(m.dispatcher());
        let on_runtime = tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
        if on_runtime {
            tokio::task::block_in_place(wait)
        } else {
            wait()
        }
        .map_err(|e| sql_err(&e))?;
        let text = String::from_utf8_lossy(query);
        let an = &ix.kind.an;
        let node = match spec.mode {
            FtMode::Boolean => {
                innodb::parse_boolean(&text, an).map_err(|e| SqlError::new(1064, "42000", e.0))?
            }
            FtMode::Natural | FtMode::Expansion => innodb::parse_natural(&text, an),
        };
        let snap = ix.kind.store.snapshot();
        let node = if spec.mode == FtMode::Expansion {
            // Blind expansion: the words of the first search's best rows
            // are added to the query, which runs again.
            let mut first = snap.hits(&innodb::rank(&node, &snap));
            first.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.seq.cmp(&b.seq)));
            first.truncate(EXPANSION_ROWS);
            let keys: Vec<Vec<u8>> = first.into_iter().map(|h| h.key).collect();
            let words = ix.kind.words_of(&keys).map_err(|e| sql_err(&e))?;
            innodb::expand(node, words)
        } else {
            node
        };
        let ranked = innodb::rank(&node, &snap);
        let mut hits = snap.hits(&ranked);
        hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.seq.cmp(&b.seq)));
        let scores = hits.iter().map(|h| (h.key.clone(), h.score)).collect();
        let pk_schema = lock(&ix.kind.ex).pk_schema.clone();
        let a = Arc::new(AnswerBox(Answer {
            index: ix.name().to_owned(),
            scores,
            hits,
            pk_schema,
        }));
        lock(&self.answers).insert(k, Arc::clone(&a));
        Ok(a)
    }
}

impl FullText for StmtSearch {
    fn check(&self, spec: &FtSpec) -> Result<(), SqlError> {
        let t = self.table(spec).ok_or_else(no_index)?;
        Self::key(t, spec).map(|_| ()).ok_or_else(no_index)
    }

    fn score(
        &self,
        spec: &FtSpec,
        query: &[u8],
        pk: &[sprig_engine::row::Value],
    ) -> Result<f64, SqlError> {
        let a = self.answer(spec, query)?;
        let t = self.table(spec).ok_or_else(no_index)?;
        let mut key = Vec::with_capacity(16);
        for (v, kc) in pk.iter().zip(t.def.pk_key(false)) {
            if typed::encode_column(&mut key, kc, v).is_err() {
                return Ok(0.0);
            }
        }
        Ok(a.0.scores.get(&key).map_or(0.0, |s| f64::from(*s)))
    }

    fn hits(
        &self,
        spec: &FtSpec,
        query: &[u8],
    ) -> Result<Option<(String, Vec<Vec<sprig_engine::row::Value>>)>, SqlError> {
        let a = self.answer(spec, query)?;
        let mut out = Vec::with_capacity(a.0.hits.len());
        for h in &a.0.hits {
            let Ok(v) = record::decode(&a.0.pk_schema, &Bytes::from(h.pk.clone())) else {
                return Ok(None);
            };
            out.push(v);
        }
        Ok(Some((a.0.index.clone(), out)))
    }
}

#[cfg(test)]
mod tests;
