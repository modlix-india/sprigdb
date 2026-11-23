//! Native graphs stored in SprigDB's own storage engine: [`EngineStore`],
//! a [`GraphStore`] whose reads and writes are engine reads and writes in
//! one engine transaction ([`GraphTxn`]), with no SQL.
//!
//! The layout is [`crate::engine_keys`]'s. What matters for speed and
//! correctness:
//! - **A hop** is one prefix seek per near node (and type) over its
//!   adjacency entries, then a sequential read. The far node's label set
//!   is in the entry, and so are the relationship's properties when they
//!   are small, so a hop that needs only labels or relationship
//!   properties reads no other record.
//! - **Both directions in one commit.** Creating a relationship writes its
//!   out-adjacency, in-adjacency, record and type-scan entries in the
//!   statement's transaction; deleting one deletes all four. There is no
//!   asynchronous mirror.
//! - **Names** (labels, relationship types, property keys, label sets) get
//!   ids from the graph's dictionary. A new name is committed in its own
//!   small transaction before the data that uses it, so a reader never
//!   meets an id it can't name; ids are never reused. Every process keeps
//!   the dictionary in memory ([`EngineGraph`]).
//! - **Ids** come in blocks of [`ID_BLOCK`] from a counter, also committed
//!   on their own; a crash loses the rest of a block, never reuses one.
//! - **Deleting a node** that still has relationships fails at commit
//!   ([`GraphTxn::commit`]), so `DELETE n` may come before deleting its
//!   relationships in the same transaction.
//! - **Indexes and uniqueness constraints** are transactional entries
//!   written in the element's transaction ([`crate::schema`]'s
//!   definitions): exact at every snapshot.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use crate::store::{CmpOp, LABEL_KEY};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value as Json;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::KeyRange;
use sprig_engine::txn::{
    CommitPath, CommitTicket, Dir as ScanDir, Isolation, LockRequest, ReadMode, Txn, TxnOptions,
};

use crate::ast::{Dir, Entity, IndexKind, Schema};
use crate::engine_keys::{
    self as k, GRAPH_OBJECT_BASE, GraphSpace, IN, MAX_GRAPH_ID, OUT, adj_parts, be32, be64,
};
use crate::mem::{duplicate, only, shard_of_rk, unique_key};
use crate::schema::{index_value, plan_create, plan_drop};
use crate::store::{
    EdgeKey, EdgeRow, FarLink, FarRead, GraphStore, IdKind, IndexDef, Link, NodeRow, Reach, Scan,
    SchemaCounts, StoreStats, not_implicit, rk_of,
};

/// Ids taken from a counter at a time.
pub const ID_BLOCK: u64 = 1000;

/// Dictionary kinds.
const DK_LABEL: u8 = b'l';
const DK_TYPE: u8 = b't';
const DK_KEY: u8 = b'k';
const DK_SET: u8 = b's';

/// Id counters.
const C_NODE: u8 = 0;
const C_EDGE: u8 = 1;
const C_INDEX: u8 = 2;

fn poisoned<T>(e: PoisonError<T>) -> T {
    e.into_inner()
}

/// Names and ids of one kind.
#[derive(Debug, Default, Clone)]
struct Names {
    by_name: HashMap<String, u32>,
    by_id: BTreeMap<u32, String>,
}

impl Names {
    fn add(&mut self, name: String, id: u32) {
        self.by_name.insert(name.clone(), id);
        self.by_id.insert(id, name);
    }
}

/// The graph's dictionary, in memory.
#[derive(Debug, Default, Clone)]
struct Dict {
    labels: Names,
    types: Names,
    keys: Names,
    /// Label set id by its sorted label ids (set 0: no labels).
    sets: HashMap<Vec<u32>, u32>,
    /// Label ids by label set id.
    set_labels: HashMap<u32, Vec<u32>>,
}

impl Dict {
    fn names(&self, dk: u8) -> &Names {
        match dk {
            DK_LABEL => &self.labels,
            DK_TYPE => &self.types,
            _ => &self.keys,
        }
    }

    fn learn(&mut self, dk: u8, name: &[u8], id: u32) {
        if dk == DK_SET {
            let ls: Vec<u32> = name
                .chunks_exact(4)
                .filter_map(|c| c.try_into().ok().map(u32::from_be_bytes))
                .collect();
            self.sets.insert(ls.clone(), id);
            self.set_labels.insert(id, ls);
            return;
        }
        let n = String::from_utf8_lossy(name).into_owned();
        match dk {
            DK_LABEL => self.labels.add(n, id),
            DK_TYPE => self.types.add(n, id),
            _ => self.keys.add(n, id),
        }
    }

    fn lookup(&self, dk: u8, name: &[u8]) -> Option<u32> {
        match dk {
            DK_SET => {
                let ls: Vec<u32> = name
                    .chunks_exact(4)
                    .filter_map(|c| c.try_into().ok().map(u32::from_be_bytes))
                    .collect();
                if ls.is_empty() {
                    return Some(0);
                }
                self.sets.get(&ls).copied()
            }
            _ => self
                .names(dk)
                .by_name
                .get(std::str::from_utf8(name).ok()?)
                .copied(),
        }
    }

    fn labels_of(&self, set: u32) -> Vec<u32> {
        self.set_labels.get(&set).cloned().unwrap_or_default()
    }

    /// The label sets holding `label`.
    fn sets_with(&self, label: u32) -> Vec<u32> {
        let mut v: Vec<u32> = self
            .set_labels
            .iter()
            .filter(|(_, ls)| ls.contains(&label))
            .map(|(s, _)| *s)
            .collect();
        v.sort_unstable();
        v
    }
}

fn set_name(labels: &[u32]) -> Vec<u8> {
    let mut ls = labels.to_vec();
    ls.sort_unstable();
    ls.dedup();
    ls.iter().flat_map(|l| l.to_be_bytes()).collect()
}

#[derive(Debug, Default, Clone, Copy)]
struct IdBlock {
    next: u64,
    end: u64,
    /// The leader epoch the block was taken under: a block of another
    /// epoch is dropped (its counter raise was not waited for, so a leader
    /// whose log tail was lost may see its ids handed out again).
    epoch: u64,
}

/// One native graph on the engine: where it lives, and what every
/// statement on it shares in memory (the dictionary, id blocks, index
/// definitions).
pub struct EngineGraph {
    path: Arc<dyn CommitPath>,
    space: GraphSpace,
    graph_id: u64,
    shards: usize,
    opts: TxnOptions,
    dict: RwLock<Dict>,
    ids: Mutex<[IdBlock; 2]>,
    indexes: RwLock<Vec<IndexDef>>,
    /// `DROP GRAPH` began: no transaction may write it any more.
    dropped: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for EngineGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineGraph")
            .field("space", &self.space)
            .field("graph_id", &self.graph_id)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

/// The transaction options graph statements run with: READ COMMITTED (a
/// fresh view per statement, as Neo4j reads), record locks only.
#[must_use]
pub fn graph_txn_options() -> TxnOptions {
    TxnOptions::mysql().with_isolation(Isolation::ReadCommitted)
}

impl EngineGraph {
    /// Opens graph `graph_id` of `tenant` on `path` with `shards` data
    /// shards, loading its dictionary and index definitions.
    ///
    /// # Errors
    /// A graph id outside the graph object range; the engine's.
    pub fn open(
        path: Arc<dyn CommitPath>,
        tenant: u64,
        graph_id: u64,
        shards: usize,
    ) -> Result<Arc<Self>> {
        if graph_id == 0 || graph_id > MAX_GRAPH_ID {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                format!("graph id {graph_id} is outside the storage engine's graph range"),
            ));
        }
        let object = GRAPH_OBJECT_BASE + u32::try_from(graph_id).unwrap_or(0);
        let g = Self {
            path,
            space: GraphSpace { tenant, object },
            graph_id,
            shards: shards.max(1),
            opts: graph_txn_options(),
            dict: RwLock::new(Dict::default()),
            ids: Mutex::new([IdBlock::default(); 2]),
            indexes: RwLock::new(Vec::new()),
            dropped: std::sync::atomic::AtomicBool::new(false),
        };
        g.reload()?;
        Ok(Arc::new(g))
    }

    /// Reads the dictionary and index definitions again.
    ///
    /// # Errors
    /// The engine's.
    pub fn reload(&self) -> Result<()> {
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        let p = self
            .space
            .prefix(0, sprig_engine::keys::kind::META, k::DICT_BY_ID);
        let rows = t.scan_current(&KeyRange::prefix(&p), ScanDir::Forward, usize::MAX)?;
        let mut d = Dict::default();
        for (key, v) in rows {
            let rest = &key[p.len()..];
            let (Some(&dk), Ok(id)) = (rest.first(), be32(rest, 1)) else {
                continue;
            };
            d.learn(dk, &v, id);
        }
        let p = self
            .space
            .prefix(0, sprig_engine::keys::kind::META, k::INDEX_DEFS);
        let rows = t.scan_current(&KeyRange::prefix(&p), ScanDir::Forward, usize::MAX)?;
        let mut defs = Vec::new();
        for (_, v) in rows {
            if let Some(def) = decode_def(&v) {
                defs.push(def);
            }
        }
        t.rollback();
        *self.dict.write().unwrap_or_else(poisoned) = d;
        *self.indexes.write().unwrap_or_else(poisoned) = defs;
        Ok(())
    }

    /// The graph's place in the keyspace.
    #[must_use]
    pub fn space(&self) -> GraphSpace {
        self.space
    }

    /// The catalog's graph id.
    #[must_use]
    pub fn graph_id(&self) -> u64 {
        self.graph_id
    }

    /// The commit path.
    #[must_use]
    pub fn path(&self) -> &Arc<dyn CommitPath> {
        &self.path
    }

    /// Starts a transaction on the graph; `implicit` for an auto-commit
    /// statement (which may commit batches of its own).
    #[must_use]
    pub fn begin(&self, implicit: bool) -> GraphTxn {
        GraphTxn {
            txn: Some(Txn::begin_on(Arc::clone(&self.path), self.opts)),
            path: Arc::clone(&self.path),
            opts: self.opts,
            deleted: BTreeSet::new(),
            implicit,
            tickets: Vec::new(),
            schema_shared: false,
        }
    }

    /// A statement's store over `tx`.
    #[must_use]
    pub fn store<'t>(self: &Arc<Self>, tx: &'t mut GraphTxn) -> EngineStore<'t> {
        if let Some(t) = tx.txn.as_mut() {
            t.begin_statement();
        }
        EngineStore {
            g: Arc::clone(self),
            tx: TxSlot::Borrowed(tx),
            stats: StoreStats::default(),
        }
    }

    /// The data shard (keyspace shard) and routing hash of node `id`.
    fn home(&self, id: u64) -> (u32, u64) {
        let rk = rk_of(id);
        (self.data_shard(rk), rk)
    }

    fn data_shard(&self, rk: u64) -> u32 {
        if self.shards == 1 {
            return 1;
        }
        1 + u32::try_from(shard_of_rk(rk, self.shards)).unwrap_or(0)
    }

    fn all_shards(&self) -> impl Iterator<Item = u32> + use<> {
        1..=u32::try_from(self.shards).unwrap_or(1)
    }

    fn dict(&self) -> std::sync::RwLockReadGuard<'_, Dict> {
        self.dict.read().unwrap_or_else(poisoned)
    }

    /// The ids of `names` of kind `dk`, committing the missing ones first
    /// (each in the same small transaction, before any data uses them).
    async fn ensure(&self, dk: u8, names: &[Vec<u8>]) -> Result<Vec<u32>> {
        let missing: BTreeSet<&Vec<u8>> = {
            let d = self.dict();
            names.iter().filter(|n| d.lookup(dk, n).is_none()).collect()
        };
        if !missing.is_empty() {
            let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
            let mut made: Vec<(Vec<u8>, u32)> = Vec::new();
            let r: Result<()> = async {
                // The kind's counter first: it serializes every new name of
                // the kind, so two statements making overlapping sets of
                // names never wait for each other in opposite orders.
                let ck = Bytes::from(self.space.dict_counter(dk));
                t.lock(&ck, LockRequest::write()).await?;
                for n in &missing {
                    let nk = Bytes::from(self.space.dict_name(dk, n));
                    t.lock(&nk, LockRequest::write()).await?;
                    if let Some(v) = t.read_current(&nk)? {
                        // Another statement made it first: its id.
                        made.push(((*n).clone(), be32(&v, 0)?));
                        continue;
                    }
                    let last = match t.read_current(&ck)? {
                        Some(v) => be32(&v, 0)?,
                        None => 0,
                    };
                    let id = last.checked_add(1).ok_or_else(|| {
                        SprigError::new(
                            SprigCode::GraphUnsupported,
                            "the graph has run out of name ids",
                        )
                    })?;
                    t.put(ck.to_vec(), id.to_be_bytes().to_vec())?;
                    t.put(nk.to_vec(), id.to_be_bytes().to_vec())?;
                    t.put(self.space.dict_id(dk, id), (*n).clone())?;
                    made.push(((*n).clone(), id));
                }
                Ok(())
            }
            .await;
            match r {
                Ok(()) => {
                    t.commit()?;
                }
                Err(e) => {
                    t.rollback();
                    return Err(e);
                }
            }
            let mut d = self.dict.write().unwrap_or_else(poisoned);
            for (n, id) in made {
                d.learn(dk, &n, id);
            }
        }
        let d = self.dict();
        names
            .iter()
            .map(|n| {
                d.lookup(dk, n)
                    .ok_or_else(|| SprigError::internal("graph storage: a name was not recorded"))
            })
            .collect()
    }

    /// The label set id of `labels`, made when new.
    async fn labelset(&self, labels: &[u32]) -> Result<u32> {
        if labels.is_empty() {
            return Ok(0);
        }
        Ok(self.ensure(DK_SET, &[set_name(labels)]).await?[0])
    }

    /// Ids for every key of `props` objects.
    async fn ensure_keys<'a>(&self, props: impl Iterator<Item = &'a Json>) -> Result<()> {
        let mut want: BTreeSet<Vec<u8>> = BTreeSet::new();
        {
            let d = self.dict();
            for p in props {
                if let Some(o) = p.as_object() {
                    for (key, v) in o {
                        if !v.is_null() && !d.keys.by_name.contains_key(key) {
                            want.insert(key.as_bytes().to_vec());
                        }
                    }
                }
            }
        }
        if !want.is_empty() {
            let v: Vec<Vec<u8>> = want.into_iter().collect();
            self.ensure(DK_KEY, &v).await?;
        }
        Ok(())
    }

    /// `n` new ids of `kind`.
    async fn alloc(&self, which: IdKind, n: usize) -> Result<Vec<u64>> {
        let slot = usize::from(which == IdKind::Edge);
        let n64 = u64::try_from(n).unwrap_or(u64::MAX);
        let mut out = Vec::with_capacity(n);
        let which_c = if slot == 0 { C_NODE } else { C_EDGE };
        let epoch = self
            .path
            .leader_epoch_of(self.path.unit_of(&self.space.id_counter(which_c)));
        loop {
            {
                let mut b = self.ids.lock().unwrap_or_else(poisoned);
                let blk = &mut b[slot];
                if blk.epoch != epoch {
                    *blk = IdBlock {
                        epoch,
                        ..IdBlock::default()
                    };
                }
                while blk.next < blk.end && out.len() < n {
                    out.push(blk.next);
                    blk.next += 1;
                }
            }
            if out.len() >= n {
                return Ok(out);
            }
            let want = (n64 - u64::try_from(out.len()).unwrap_or(0)).max(ID_BLOCK);
            let (start, end) = self.take_block(which_c, want).await?;
            let mut b = self.ids.lock().unwrap_or_else(poisoned);
            let blk = &mut b[slot];
            if blk.epoch != epoch || blk.next >= blk.end {
                *blk = IdBlock {
                    next: start,
                    end,
                    epoch,
                };
            } else {
                // Another statement refilled first: use this block now.
                while out.len() < n && blk.next < blk.end {
                    out.push(blk.next);
                    blk.next += 1;
                }
                for id in start..end {
                    if out.len() >= n {
                        break;
                    }
                    out.push(id);
                }
            }
        }
    }

    /// Raises counter `which` by `n`: the ids `[start, end)`.
    async fn take_block(&self, which: u8, n: u64) -> Result<(u64, u64)> {
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        let ck = Bytes::from(self.space.id_counter(which));
        let r: Result<(u64, u64)> = async {
            t.lock(&ck, LockRequest::write()).await?;
            let hw = match t.read_current(&ck)? {
                Some(v) => be64(&v, 0)?,
                None => 0,
            };
            let end = hw
                .checked_add(n)
                .ok_or_else(|| SprigError::internal("id counter overflow"))?;
            t.put(ck.to_vec(), end.to_be_bytes().to_vec())?;
            Ok((hw + 1, end + 1))
        }
        .await;
        match r {
            Ok(x) => {
                t.commit()?;
                Ok(x)
            }
            Err(e) => {
                t.rollback();
                Err(e)
            }
        }
    }

    fn record(&self, props: &Json, types: Option<&Json>) -> Result<Vec<u8>> {
        let d = self.dict();
        let mut out = Vec::new();
        k::put_record(&mut out, props, types, &|n| d.keys.by_name.get(n).copied())?;
        Ok(out)
    }

    /// Decodes a record, keeping `keys` (`None`: all).
    fn decode(&self, rec: &[u8], keys: Option<&[String]>) -> Result<(Json, Option<Json>)> {
        if keys.is_some_and(<[String]>::is_empty) || rec.is_empty() {
            return Ok((Json::Object(serde_json::Map::new()), None));
        }
        let d = self.dict();
        // A statement asks for a few keys: a list beats a hash set.
        let want: Option<Vec<u32>> = keys.map(|ks| {
            ks.iter()
                .filter_map(|n| d.keys.by_name.get(n).copied())
                .collect()
        });
        k::get_record(
            rec,
            &|id| want.as_ref().is_none_or(|w| w.contains(&id)),
            &|id| d.keys.by_id.get(&id).cloned(),
        )
    }

    fn labels_of(&self, set: u32) -> Vec<u32> {
        if set == 0 {
            return Vec::new();
        }
        self.dict().labels_of(set)
    }

    /// The graph's schema lock: every writing transaction holds it shared
    /// (taken at its first write), schema changes and `DROP GRAPH` hold it
    /// exclusively.
    fn schema_key(&self) -> Bytes {
        Bytes::from(
            self.space
                .prefix(0, sprig_engine::keys::kind::META, k::SCHEMA_LOCK),
        )
    }

    /// A transaction holding the schema lock exclusively: it waits for
    /// every writing transaction in flight, and later ones wait for it.
    async fn schema_exclusive(&self) -> Result<Txn> {
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        if let Err(e) = t.lock(&self.schema_key(), LockRequest::write()).await {
            t.rollback();
            return Err(e);
        }
        Ok(t)
    }

    /// Deletes every key under `prefix`, a batch per transaction, each key
    /// locked first.
    async fn delete_prefix(&self, prefix: &[u8], batch: usize) -> Result<u64> {
        let mut n = 0u64;
        let end = k::end_of(prefix);
        loop {
            let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
            let rows = t.scan_current(
                &KeyRange::new(prefix, end.as_slice()),
                ScanDir::Forward,
                batch.max(1),
            )?;
            if rows.is_empty() {
                t.rollback();
                return Ok(n);
            }
            let r: Result<()> = async {
                for (key, _) in &rows {
                    t.lock(key, LockRequest::write()).await?;
                    t.delete(key.to_vec())?;
                }
                Ok(())
            }
            .await;
            if let Err(e) = r {
                t.rollback();
                return Err(e);
            }
            n += rows.len() as u64;
            t.commit()?.wait_async().await?;
        }
    }

    /// Deletes the entries of index `d`.
    async fn delete_entries(&self, d: &IndexDef) -> Result<()> {
        let n = u16::try_from(d.id).unwrap_or(u16::MAX);
        self.delete_prefix(
            &self.space.prefix(0, sprig_engine::keys::kind::UNIQUE, n),
            1000,
        )
        .await?;
        for sh in self.all_shards() {
            self.delete_prefix(
                &self
                    .space
                    .prefix(sh, sprig_engine::keys::kind::SECONDARY, n),
                1000,
            )
            .await?;
        }
        Ok(())
    }

    /// Writes the entries of a published index `d` for the elements that
    /// exist, a page per transaction. Each element is share-locked and read
    /// at its latest state, so an element changed or deleted meanwhile (by
    /// a writer that already keeps `d`'s entries) is never indexed from a
    /// stale copy. A uniqueness clash fails the build.
    async fn backfill(&self, d: &IndexDef) -> Result<()> {
        const PAGE: usize = 1000;
        let n = u16::try_from(d.id).unwrap_or(u16::MAX);
        let prefixes: Vec<(u32, Vec<u8>)> = match d.entity {
            Entity::Node => {
                let sets = self.dict().sets_with(d.token_id);
                self.all_shards()
                    .flat_map(|sh| sets.iter().map(move |s2| (sh, *s2)))
                    .map(|(sh, s2)| (sh, self.space.label_prefix(sh, s2)))
                    .collect()
            }
            Entity::Relationship => self
                .all_shards()
                .map(|sh| {
                    let mut p =
                        self.space
                            .prefix(sh, sprig_engine::keys::kind::LABELS, k::TYPE_SCAN);
                    p.extend_from_slice(&d.token_id.to_be_bytes());
                    (sh, p)
                })
                .collect(),
        };
        for (sh, p) in prefixes {
            let end = k::end_of(&p);
            let mut from = p.clone();
            loop {
                let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
                let rows = t.scan_current(
                    &KeyRange::new(from.as_slice(), end.as_slice()),
                    ScanDir::Forward,
                    PAGE,
                )?;
                let Some((last, _)) = rows.last() else {
                    t.rollback();
                    break;
                };
                let last_id = be64(last, last.len() - 8)?;
                let r: Result<()> = async {
                    for (key, _) in &rows {
                        let id = be64(key, key.len() - 8)?;
                        let ek = Bytes::from(match d.entity {
                            Entity::Node => self.space.node(sh, id),
                            Entity::Relationship => self.space.edge(sh, id),
                        });
                        t.lock(&ek, LockRequest::for_share()).await?;
                        let Some(v) = t.read_current(&ek)? else {
                            continue;
                        };
                        let rec = match d.entity {
                            Entity::Node => {
                                let (ls, rec) = k::split_node(&v)?;
                                if !self.labels_of(ls).contains(&d.token_id) {
                                    continue;
                                }
                                rec
                            }
                            Entity::Relationship => k::split_edge(&v)?.3,
                        };
                        let (props, types) = self.decode(rec, Some(&d.props))?;
                        let Some(key) = unique_key(d, &props, types.as_ref()).map(|v2| k::index_key(&v2)) else {
                            continue;
                        };
                        if d.unique {
                            let uk = Bytes::from(self.space.unique(n, &key));
                            t.lock(&uk, LockRequest::write()).await?;
                            if let Some(o) = t.read_current(&uk)?
                                && be64(&o, 0)? != id
                            {
                                return Err(SprigError::new(
                                    SprigCode::GraphDefinitionInvalid,
                                    format!(
                                        "[Neo.ClientError.Schema.ConstraintCreationFailed] Unable to create constraint {}: existing data has duplicate values",
                                        d.name
                                    ),
                                ));
                            }
                            t.put(uk.to_vec(), id.to_be_bytes().to_vec())?;
                        } else {
                            let mut ik = self.space.index_prefix(sh, n, &key);
                            ik.extend_from_slice(&id.to_be_bytes());
                            let ik = Bytes::from(ik);
                            t.lock(&ik, LockRequest::write()).await?;
                            t.put(ik.to_vec(), Vec::new())?;
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = r {
                    t.rollback();
                    return Err(e);
                }
                t.commit()?.wait_async().await?;
                if rows.len() < PAGE || last_id == u64::MAX {
                    break;
                }
                // The next page starts after the last id (keys end with it).
                from.truncate(last.len() - 8);
                from.extend_from_slice(&(last_id + 1).to_be_bytes());
            }
        }
        Ok(())
    }

    fn defs(&self) -> Vec<IndexDef> {
        self.indexes.read().unwrap_or_else(poisoned).clone()
    }
}

/// An index definition as stored.
fn encode_def(d: &IndexDef) -> Vec<u8> {
    serde_json::json!({
        "id": d.id, "name": d.name, "unique": d.unique,
        "kind": if d.kind == IndexKind::Text { "text" } else { "range" },
        "entity": if d.entity == Entity::Node { "node" } else { "relationship" },
        "token": d.token, "token_id": d.token_id, "props": d.props, "state": d.state,
    })
    .to_string()
    .into_bytes()
}

fn decode_def(v: &[u8]) -> Option<IndexDef> {
    let j: Json = serde_json::from_slice(v).ok()?;
    Some(IndexDef {
        id: u32::try_from(j.get("id")?.as_u64()?).ok()?,
        name: j.get("name")?.as_str()?.to_owned(),
        unique: j.get("unique")?.as_bool()?,
        kind: if j.get("kind")?.as_str()? == "text" {
            IndexKind::Text
        } else {
            IndexKind::Range
        },
        entity: if j.get("entity")?.as_str()? == "node" {
            Entity::Node
        } else {
            Entity::Relationship
        },
        token: j.get("token")?.as_str()?.to_owned(),
        token_id: u32::try_from(j.get("token_id")?.as_u64()?).ok()?,
        props: j
            .get("props")?
            .as_array()?
            .iter()
            .filter_map(|p| p.as_str().map(str::to_owned))
            .collect(),
        state: j.get("state")?.as_str()?.to_owned(),
    })
}

/// One engine transaction over a graph: a Bolt transaction, or one
/// auto-commit statement (with its `CALL { } IN TRANSACTIONS` batches).
pub struct GraphTxn {
    txn: Option<Txn>,
    path: Arc<dyn CommitPath>,
    opts: TxnOptions,
    /// Nodes this transaction deleted: they must have no relationships
    /// left when it commits.
    deleted: BTreeSet<u64>,
    implicit: bool,
    /// Tickets of batches already committed.
    tickets: Vec<CommitTicket>,
    /// The current engine transaction holds the schema lock shared.
    schema_shared: bool,
}

impl std::fmt::Debug for GraphTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphTxn")
            .field("implicit", &self.implicit)
            .field("deleted", &self.deleted.len())
            .finish_non_exhaustive()
    }
}

fn gone() -> SprigError {
    SprigError::internal("graph transaction already ended")
}

impl GraphTxn {
    fn t(&mut self) -> Result<&mut Txn> {
        self.txn.as_mut().ok_or_else(gone)
    }

    /// The engine transaction.
    ///
    /// # Errors
    /// It has ended.
    pub fn txn_mut(&mut self) -> Result<&mut Txn> {
        self.t()
    }

    /// Fails with 39906 when a node this transaction deleted still has a
    /// relationship (in the latest committed state with this
    /// transaction's writes over it).
    ///
    /// # Errors
    /// 39906; the engine's.
    pub fn check_deleted(&mut self, g: &EngineGraph) -> Result<()> {
        let ids: Vec<u64> = self.deleted.iter().copied().collect();
        let t = self.t()?;
        for n in ids {
            let (d, _) = g.home(n);
            for dir in [OUT, IN] {
                let p = g.space.adj_prefix(d, dir, n, None);
                if !t
                    .scan_current(&KeyRange::prefix(&p), ScanDir::Forward, 1)?
                    .is_empty()
                {
                    return Err(SprigError::new(
                        SprigCode::GraphNodeHasRelationships,
                        format!(
                            "Cannot delete node<{n}>, because it still has relationships. To delete this node, you must first delete its relationships."
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Commits after the deleted-node check, and waits until the commit
    /// (and every batch before it) is durable. On an error nothing more
    /// is committed.
    ///
    /// # Errors
    /// 39906; the engine's (the transaction is then rolled back).
    pub async fn commit(mut self, g: &EngineGraph) -> Result<()> {
        if let Err(e) = self.check_deleted(g) {
            self.rollback();
            return Err(e);
        }
        let t = self.txn.take().ok_or_else(gone)?;
        let ticket = t.commit()?;
        self.tickets.push(ticket);
        for tk in &self.tickets {
            tk.wait_async().await?;
        }
        Ok(())
    }

    /// Rolls back.
    pub fn rollback(mut self) {
        if let Some(t) = self.txn.take() {
            t.rollback();
        }
    }
}

/// The transaction a store runs in: the caller's, or its own (tests).
enum TxSlot<'t> {
    Borrowed(&'t mut GraphTxn),
    Owned(Box<GraphTxn>),
}

impl std::ops::Deref for TxSlot<'_> {
    type Target = GraphTxn;
    fn deref(&self) -> &GraphTxn {
        match self {
            TxSlot::Borrowed(t) => t,
            TxSlot::Owned(t) => t,
        }
    }
}

impl std::ops::DerefMut for TxSlot<'_> {
    fn deref_mut(&mut self) -> &mut GraphTxn {
        match self {
            TxSlot::Borrowed(t) => t,
            TxSlot::Owned(t) => t,
        }
    }
}

/// A statement's view of a graph on the engine.
pub struct EngineStore<'t> {
    g: Arc<EngineGraph>,
    tx: TxSlot<'t>,
    stats: StoreStats,
}

impl std::fmt::Debug for EngineStore<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStore")
            .field("graph", &self.g)
            .finish_non_exhaustive()
    }
}

/// Numeric comparisons a node must pass (labels too), as the in-memory
/// store and MariaDB evaluate them: a superset the caller rechecks.
fn passes(labels: &[u32], props: &Json, filter: &[(String, CmpOp, f64)]) -> bool {
    filter.iter().all(|(key, op, x)| {
        if let Some(id) = key.strip_prefix(LABEL_KEY) {
            return id.parse::<u32>().is_ok_and(|id| labels.contains(&id));
        }
        let v = props.get(key).and_then(Json::as_f64);
        match op {
            CmpOp::NotNumBelow => v.is_none_or(|v| v >= *x),
            CmpOp::NotNumAbove => v.is_none_or(|v| v <= *x),
            CmpOp::Lt => v.is_some_and(|v| v < *x),
            CmpOp::Le => v.is_some_and(|v| v <= *x),
            CmpOp::Gt => v.is_some_and(|v| v > *x),
            CmpOp::Ge => v.is_some_and(|v| v >= *x),
            CmpOp::Eq => v.is_some_and(|v| (v - *x).abs() == 0.0),
        }
    })
}

/// One adjacency entry as read.
struct Adj {
    near: u64,
    /// The near node's routing hash (the same for all its entries).
    near_rk: u64,
    etype: u32,
    far: u64,
    edge: u64,
    /// Out-adjacency: true.
    out: bool,
    value: Bytes,
}

impl EngineStore<'static> {
    /// A store with a transaction of its own that is never committed: the
    /// executor's tests run on it as they run on the in-memory store
    /// (each statement sees the ones before it).
    #[must_use]
    pub fn standalone(g: &Arc<EngineGraph>) -> Self {
        let mut tx = g.begin(true);
        if let Some(t) = tx.txn.as_mut() {
            t.begin_statement();
        }
        EngineStore {
            g: Arc::clone(g),
            tx: TxSlot::Owned(Box::new(tx)),
            stats: StoreStats::default(),
        }
    }
}

impl EngineStore<'_> {
    /// Nodes and relationships this store's transaction sees.
    ///
    /// # Errors
    /// The engine's.
    pub fn element_counts(&mut self) -> Result<(usize, usize)> {
        let (mut n, mut e) = (0, 0);
        for sh in self.g.all_shards().collect::<Vec<_>>() {
            let p = self
                .g
                .space
                .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::NODE);
            n += self.scan_prefix(&p)?.len();
            let p = self
                .g
                .space
                .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::EDGE);
            e += self.scan_prefix(&p)?.len();
        }
        Ok((n, e))
    }

    /// The graph.
    #[must_use]
    pub fn graph(&self) -> &Arc<EngineGraph> {
        &self.g
    }

    fn t(&mut self) -> Result<&mut Txn> {
        self.tx.t()
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.t()?.get_snapshot(key)
    }

    fn scan_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let r = KeyRange::new(start, end);
        self.t()?
            .scan(&r, ScanDir::Forward, limit, ReadMode::Snapshot)
    }

    fn scan_prefix(&mut self, p: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let end = k::end_of(p);
        self.scan_range(p, &end, usize::MAX)
    }

    /// The latest committed version of a key under this transaction's
    /// writes (for keys it holds or is about to change).
    fn current(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        self.t()?.read_current(key)
    }

    fn scan_current(&mut self, p: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        self.t()?
            .scan_current(&KeyRange::prefix(p), ScanDir::Forward, usize::MAX)
    }

    async fn lock(&mut self, key: &[u8], exclusive: bool) -> Result<()> {
        let b = Bytes::copy_from_slice(key);
        let req = if exclusive {
            LockRequest::write()
        } else {
            LockRequest::for_share()
        };
        self.t()?.lock(&b, req).await?;
        Ok(())
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.lock(&key, true).await?;
        self.t()?.put(key, value)
    }

    async fn del(&mut self, key: Vec<u8>) -> Result<()> {
        self.lock(&key, true).await?;
        self.t()?.delete(key)
    }

    /// Before a transaction's first write: the schema lock, shared (so
    /// index definitions can't change under it, and `DROP GRAPH` waits for
    /// it), and the check that the graph still exists.
    async fn writing(&mut self) -> Result<()> {
        if !self.tx.schema_shared {
            let key = self.g.schema_key();
            self.t()?.lock(&key, LockRequest::for_share()).await?;
            self.tx.schema_shared = true;
        }
        if self.g.dropped.load(std::sync::atomic::Ordering::Acquire) {
            return Err(SprigError::new(
                SprigCode::GraphNotFound,
                format!("graph {} was dropped", self.g.graph_id),
            ));
        }
        Ok(())
    }

    fn node_row(&self, id: u64, rk: u64, v: &[u8], keys: Option<&[String]>) -> Result<NodeRow> {
        let (ls, rec) = k::split_node(v)?;
        let (props, types) = self.g.decode(rec, keys)?;
        Ok(NodeRow {
            id,
            rk,
            labels: self.g.labels_of(ls),
            props,
            types,
        })
    }

    /// Node `id` at the statement's view.
    fn read_node(&mut self, id: u64, keys: Option<&[String]>) -> Result<Option<NodeRow>> {
        let (d, rk) = self.g.home(id);
        let key = self.g.space.node(d, id);
        match self.get(&key)? {
            Some(v) => Ok(Some(self.node_row(id, rk, &v, keys)?)),
            None => Ok(None),
        }
    }

    /// The edge record of `id` on the shard of `src_rk` (`None`: probe
    /// every shard).
    fn read_edge(
        &mut self,
        id: u64,
        src_rk: Option<u64>,
        keys: Option<&[String]>,
    ) -> Result<Option<EdgeRow>> {
        let shards: Vec<u32> = match src_rk {
            Some(rk) => vec![self.g.data_shard(rk)],
            None => self.g.all_shards().collect(),
        };
        for d in shards {
            let key = self.g.space.edge(d, id);
            if let Some(v) = self.get(&key)? {
                return Ok(Some(self.edge_row(id, &v, keys)?));
            }
        }
        Ok(None)
    }

    fn edge_row(&self, id: u64, v: &[u8], keys: Option<&[String]>) -> Result<EdgeRow> {
        let (src, rtype, dst, rec) = k::split_edge(v)?;
        let (props, types) = self.g.decode(rec, keys)?;
        Ok(EdgeRow {
            id,
            src,
            dst,
            src_rk: rk_of(src),
            dst_rk: rk_of(dst),
            rtype,
            props,
            types,
        })
    }

    /// The adjacency entries of `from` over `types` (empty: any) in
    /// `dir`, in edge-id order (the order the other stores give).
    fn adjacency(&mut self, from: &[u64], types: &[u32], dir: Dir) -> Result<Vec<Adj>> {
        self.adjacency_at(from, types, dir, false)
    }

    /// [`Self::adjacency`] at the statement's view, or (`current`) at the
    /// latest committed state under this transaction's writes.
    fn adjacency_at(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        current: bool,
    ) -> Result<Vec<Adj>> {
        let dirs: &[u16] = match dir {
            Dir::Out => &[OUT],
            Dir::In => &[IN],
            Dir::Both => &[OUT, IN],
        };
        let mut out = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        for &near in from {
            if !seen.insert(near) {
                continue;
            }
            let (d, near_rk) = self.g.home(near);
            for &dr in dirs {
                let prefixes: Vec<Vec<u8>> = if types.is_empty() {
                    vec![self.g.space.adj_prefix(d, dr, near, None)]
                } else {
                    let mut ts = types.to_vec();
                    ts.sort_unstable();
                    ts.dedup();
                    ts.iter()
                        .map(|t| self.g.space.adj_prefix(d, dr, near, Some(*t)))
                        .collect()
                };
                for p in prefixes {
                    self.stats.shard_queries += 1;
                    let rows: Vec<(Vec<u8>, Bytes)> = if current {
                        self.scan_current(&p)?
                            .into_iter()
                            .map(|(k2, v)| (k2.to_vec(), v))
                            .collect()
                    } else {
                        self.scan_prefix(&p)?
                    };
                    for (key, value) in rows {
                        let (n, etype, far, edge) = adj_parts(&key)?;
                        out.push(Adj {
                            near: n,
                            near_rk,
                            etype,
                            far,
                            edge,
                            out: dr == OUT,
                            value,
                        });
                    }
                }
            }
        }
        self.stats.sql_rows += out.len() as u64;
        out.sort_by_key(|a| (a.edge, !a.out));
        Ok(out)
    }

    fn link_of(a: &Adj) -> Link {
        let far_rk = rk_of(a.far);
        let (src, dst, src_rk, dst_rk) = if a.out {
            (a.near, a.far, a.near_rk, far_rk)
        } else {
            (a.far, a.near, far_rk, a.near_rk)
        };
        Link {
            src,
            dst,
            src_rk,
            dst_rk,
            id: a.edge,
            rtype: a.etype,
        }
    }

    /// The relationship's properties from its out-adjacency value, or its
    /// record when they aren't inline.
    fn rel_props(
        &mut self,
        a: &Adj,
        keys: Option<&[String]>,
    ) -> Result<Option<(Json, Option<Json>)>> {
        if a.out {
            let (_, inline) = k::split_out(&a.value)?;
            if let Some(rec) = inline {
                return Ok(Some(self.g.decode(rec, keys)?));
            }
        }
        let src = if a.out { a.near } else { a.far };
        Ok(self
            .read_edge(a.edge, Some(rk_of(src)), keys)?
            .map(|e| (e.props, e.types)))
    }

    /// The far node's label set as the adjacency entry has it (out-
    /// adjacency: the destination's; in-adjacency: the source's).
    fn far_labelset(a: &Adj) -> Result<u32> {
        be32(&a.value, 0)
    }

    /// Index entries of `def` for an element (`None`: it has none).
    fn index_key_of(def: &IndexDef, props: &Json, types: Option<&Json>) -> Option<Vec<u8>> {
        unique_key(def, props, types).map(|vals| k::index_key(&vals))
    }

    fn applies(def: &IndexDef, labels: &[u32], rtype: Option<u32>) -> bool {
        match (def.entity, rtype) {
            (Entity::Node, None) => labels.contains(&def.token_id),
            (Entity::Relationship, Some(t)) => t == def.token_id,
            _ => false,
        }
    }

    /// The index entries (index id, key) an element has.
    fn entries(
        &self,
        labels: &[u32],
        rtype: Option<u32>,
        props: &Json,
        types: Option<&Json>,
    ) -> Vec<(u32, Vec<u8>)> {
        self.g
            .defs()
            .iter()
            .filter(|def| Self::applies(def, labels, rtype))
            .filter_map(|def| Self::index_key_of(def, props, types).map(|k2| (def.id, k2)))
            .collect()
    }

    /// The entries an element keeps through a change: neither dropped nor
    /// written again (nor locked).
    fn kept(
        &self,
        old: (&[u32], &Json, Option<&Json>),
        new: (&[u32], &Json, Option<&Json>),
        rtype: Option<u32>,
    ) -> Vec<(u32, Vec<u8>)> {
        let a = self.entries(old.0, rtype, old.1, old.2);
        let b = self.entries(new.0, rtype, new.1, new.2);
        a.into_iter().filter(|x| b.contains(x)).collect()
    }

    /// Writes the index and unique entries of an element on shard `d`,
    /// failing on a uniqueness clash (1062).
    #[allow(clippy::too_many_arguments)] // the element's parts, and what it keeps
    async fn add_index_entries(
        &mut self,
        d: u32,
        id: u64,
        labels: &[u32],
        rtype: Option<u32>,
        props: &Json,
        types: Option<&Json>,
        except: &[(u32, Vec<u8>)],
    ) -> Result<()> {
        for def in self.g.defs() {
            if !Self::applies(&def, labels, rtype) {
                continue;
            }
            let Some(key) = Self::index_key_of(&def, props, types) else {
                continue;
            };
            if except.iter().any(|(i, k2)| *i == def.id && *k2 == key) {
                continue;
            }
            let n = u16::try_from(def.id).unwrap_or(u16::MAX);
            if def.unique {
                let uk = self.g.space.unique(n, &key);
                self.lock(&uk, true).await?;
                if let Some(v) = self.current(&uk)?
                    && be64(&v, 0)? != id
                {
                    // The executor's `MERGE` matches again after a
                    // duplicate (the other writer has committed: this
                    // lock waited for it), so the rest of the statement
                    // reads from a fresh view, which sees it. The entry's
                    // lock goes back first: the match may lock the owner,
                    // whose writer may want the entry.
                    let t = self.t()?;
                    t.unlock(&Bytes::from(uk));
                    t.begin_statement();
                    let what = if rtype.is_none() {
                        "Node"
                    } else {
                        "Relationship"
                    };
                    return Err(duplicate(&def, what, id));
                }
                self.t()?.put(uk, id.to_be_bytes().to_vec())?;
            } else {
                let mut ik = self.g.space.index_prefix(d, n, &key);
                ik.extend_from_slice(&id.to_be_bytes());
                self.put(ik, Vec::new()).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // the element's parts, and what it keeps
    async fn drop_index_entries(
        &mut self,
        d: u32,
        id: u64,
        labels: &[u32],
        rtype: Option<u32>,
        props: &Json,
        types: Option<&Json>,
        except: &[(u32, Vec<u8>)],
    ) -> Result<()> {
        for def in self.g.defs() {
            if !Self::applies(&def, labels, rtype) {
                continue;
            }
            let Some(key) = Self::index_key_of(&def, props, types) else {
                continue;
            };
            if except.iter().any(|(i, k2)| *i == def.id && *k2 == key) {
                continue;
            }
            let n = u16::try_from(def.id).unwrap_or(u16::MAX);
            if def.unique {
                let uk = self.g.space.unique(n, &key);
                self.lock(&uk, true).await?;
                if let Some(v) = self.current(&uk)?
                    && be64(&v, 0)? == id
                {
                    self.t()?.delete(uk)?;
                }
            } else {
                let mut ik = self.g.space.index_prefix(d, n, &key);
                ik.extend_from_slice(&id.to_be_bytes());
                self.del(ik).await?;
            }
        }
        Ok(())
    }

    /// Node ids an index finds for `s`, when one serves it.
    fn index_ids(&mut self, s: &Scan) -> Result<Option<Vec<u64>>> {
        let Some(label) = s.label else {
            return Ok(None);
        };
        if s.props_eq.is_empty() {
            return Ok(None);
        }
        let defs = self.g.defs();
        let Some(def) = defs.iter().find(|d| {
            d.entity == Entity::Node
                && d.token_id == label
                && d.state == "online"
                && d.props
                    .iter()
                    .all(|p| s.props_eq.iter().any(|(k2, _)| k2 == p))
        }) else {
            return Ok(None);
        };
        // Every combination of the wanted values, as index keys.
        let mut keys: Vec<Vec<String>> = vec![Vec::new()];
        for p in &def.props {
            let Some((_, vals)) = s.props_eq.iter().find(|(k2, _)| k2 == p) else {
                return Ok(None);
            };
            let mut texts = Vec::new();
            for v in vals {
                let Some(t) = index_value(v, def.kind) else {
                    return Ok(None);
                };
                texts.push(t);
            }
            let mut next = Vec::new();
            for kk in &keys {
                for t in &texts {
                    let mut x = kk.clone();
                    x.push(t.clone());
                    next.push(x);
                }
            }
            keys = next;
            if keys.len() > 10_000 {
                return Ok(None);
            }
        }
        let n = u16::try_from(def.id).unwrap_or(u16::MAX);
        let mut ids = Vec::new();
        for vals in keys {
            let key = k::index_key(&vals);
            self.stats.shard_queries += 1;
            if def.unique {
                if let Some(v) = self.get(&self.g.space.unique(n, &key))? {
                    ids.push(be64(&v, 0)?);
                }
            } else {
                for d in self.g.all_shards().collect::<Vec<_>>() {
                    let p = self.g.space.index_prefix(d, n, &key);
                    for (ek, _) in self.scan_prefix(&p)? {
                        ids.push(be64(&ek, ek.len() - 8)?);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Some(ids))
    }

    /// The next ids of label `label` (`None`: every node) above `after`,
    /// at most `page` of them, in id order.
    fn next_ids(
        &mut self,
        label: Option<u32>,
        after: Option<u64>,
        page: usize,
    ) -> Result<Vec<u64>> {
        let from = after.map_or(0, |a| a.saturating_add(1));
        if after == Some(u64::MAX) {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        let sets: Vec<Option<u32>> = match label {
            None => vec![None],
            Some(l) => self.g.dict().sets_with(l).into_iter().map(Some).collect(),
        };
        for d in self.g.all_shards().collect::<Vec<_>>() {
            for set in &sets {
                let p = match set {
                    None => self
                        .g
                        .space
                        .prefix(d, sprig_engine::keys::kind::PRIMARY, k::NODE),
                    Some(s) => self.g.space.label_prefix(d, *s),
                };
                let mut start = p.clone();
                start.extend_from_slice(&from.to_be_bytes());
                let end = k::end_of(&p);
                self.stats.shard_queries += 1;
                for (key, _) in self.scan_range(&start, &end, page)? {
                    ids.push(be64(&key, key.len() - 8)?);
                }
            }
        }
        ids.sort_unstable();
        ids.truncate(page);
        Ok(ids)
    }

    /// Reads node values by id (the statement's view); missing ids are
    /// skipped.
    fn read_nodes(&mut self, ids: &[u64], keys: Option<&[String]>) -> Result<Vec<NodeRow>> {
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            if let Some(n) = self.read_node(id, keys)? {
                out.push(n);
            }
        }
        Ok(out)
    }

    /// Keys a read needs: `keys` plus the filter's.
    fn with_filter_keys(
        keys: Option<&[String]>,
        filter: &[(String, CmpOp, f64)],
    ) -> Option<Vec<String>> {
        keys.map(|ks| {
            let mut v = ks.to_vec();
            for (key, _, _) in filter {
                if !key.starts_with(LABEL_KEY) && !v.contains(key) {
                    v.push(key.clone());
                }
            }
            v
        })
    }

    /// Rewrites the label-set field of the adjacency entries naming node
    /// `n` (its neighbours' entries), after its labels changed.
    async fn relabel(&mut self, n: u64, ls: u32) -> Result<()> {
        let (d, _) = self.g.home(n);
        for dir in [OUT, IN] {
            let p = self.g.space.adj_prefix(d, dir, n, None);
            for (key, _) in self.scan_current(&p)? {
                let (_, etype, far, edge) = adj_parts(&key)?;
                let (fd, _) = self.g.home(far);
                // The far node's entry for the same edge, the other way.
                let other = if dir == OUT {
                    self.g.space.adj(fd, IN, far, etype, n, edge)
                } else {
                    self.g.space.adj(fd, OUT, far, etype, n, edge)
                };
                self.lock(&other, true).await?;
                if let Some(v) = self.current(&other)? {
                    let mut nv = v.to_vec();
                    if nv.len() >= 4 {
                        nv[..4].copy_from_slice(&ls.to_be_bytes());
                    }
                    self.t()?.put(other, nv)?;
                }
            }
        }
        Ok(())
    }

    /// Runs a schema command in its own transaction.
    async fn schema_cmd(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        // A store with its own transaction (tests) commits what it wrote so
        // far: the command runs outside any data transaction and must see
        // it.
        if let TxSlot::Owned(tx) = &mut self.tx {
            let t = tx.txn.take().ok_or_else(gone)?;
            t.commit()?.wait_async().await?;
            let mut nt = Txn::begin_on(Arc::clone(&tx.path), tx.opts);
            nt.begin_statement();
            tx.txn = Some(nt);
            tx.schema_shared = false;
        }
        // No writer is in flight while the schema lock is held: the
        // definition becomes visible to every later writer at once.
        let mut x = self.g.schema_exclusive().await?;
        let existing = self.g.defs();
        if let Schema::Drop { .. } = cmd {
            let d = match plan_drop(&existing, cmd) {
                Ok(Some(d)) => d,
                Ok(None) => {
                    x.rollback();
                    return Ok(vec![]);
                }
                Err(e) => {
                    x.rollback();
                    return Err(e);
                }
            };
            x.delete(self.g.space.index_def(d.id))?;
            // Out of memory while the lock is held: no writer keeps entries
            // of it once the lock goes (commit releases it at once).
            self.g
                .indexes
                .write()
                .unwrap_or_else(poisoned)
                .retain(|y| y.name != d.name);
            x.commit()?.wait_async().await?;
            self.g.delete_entries(&d).await?;
            return Ok(vec![(
                if d.unique {
                    "constraints-removed"
                } else {
                    "indexes-removed"
                },
                1,
            )]);
        }
        let mut d = match plan_create(&existing, cmd) {
            Ok(Some(d)) => d,
            Ok(None) => {
                x.rollback();
                return Ok(vec![]);
            }
            Err(e) => {
                x.rollback();
                return Err(e);
            }
        };
        let made: Result<()> = async {
            d.token_id = match d.entity {
                Entity::Node => self.label_id(&d.token, true).await?,
                Entity::Relationship => self.type_id(&d.token, true).await?,
            }
            .unwrap_or(0);
            let (start, _) = self.g.take_block(C_INDEX, 1).await?;
            d.id = u32::try_from(start)
                .ok()
                .filter(|i| u16::try_from(*i).is_ok())
                .ok_or_else(|| {
                    SprigError::new(
                        SprigCode::GraphUnsupported,
                        "the graph has used every index number",
                    )
                })?;
            d.state = "building".into();
            x.put(self.g.space.index_def(d.id), encode_def(&d))?;
            Ok(())
        }
        .await;
        if let Err(e) = made {
            x.rollback();
            return Err(e);
        }
        // Published while the lock is held (commit releases it at once):
        // every writer that gets the lock after it keeps the entries.
        self.g
            .indexes
            .write()
            .unwrap_or_else(poisoned)
            .push(d.clone());
        match x.commit() {
            Ok(tk) => tk.wait_async().await?,
            Err(e) => {
                self.g
                    .indexes
                    .write()
                    .unwrap_or_else(poisoned)
                    .retain(|y| y.name != d.name);
                return Err(e);
            }
        }
        if let Err(e) = self.g.backfill(&d).await {
            // Undone: the definition first, then its entries.
            let mut x = self.g.schema_exclusive().await?;
            x.delete(self.g.space.index_def(d.id))?;
            self.g
                .indexes
                .write()
                .unwrap_or_else(poisoned)
                .retain(|y| y.name != d.name);
            x.commit()?.wait_async().await?;
            self.g.delete_entries(&d).await?;
            return Err(e);
        }
        d.state = "online".into();
        let mut t = Txn::begin_on(Arc::clone(&self.g.path), self.g.opts);
        t.put(self.g.space.index_def(d.id), encode_def(&d))?;
        t.commit()?.wait_async().await?;
        let unique = d.unique;
        for y in self.g.indexes.write().unwrap_or_else(poisoned).iter_mut() {
            if y.id == d.id {
                y.state = "online".into();
            }
        }
        Ok(vec![(
            if unique {
                "constraints-added"
            } else {
                "indexes-added"
            },
            1,
        )])
    }
}

#[async_trait]
impl GraphStore for EngineStore<'_> {
    fn graph_id(&self) -> u64 {
        self.g.graph_id
    }

    fn shards(&self) -> usize {
        self.g.shards
    }

    fn shard_of(&self, rk: u64) -> usize {
        shard_of_rk(rk, self.g.shards)
    }

    fn stats(&self) -> StoreStats {
        self.stats
    }

    fn late_props(&self) -> bool {
        true
    }

    async fn scan(&mut self, s: &Scan) -> Result<Vec<NodeRow>> {
        let keys = s.keys.as_deref();
        // Read whole when the pushed equalities need keys outside `keys`.
        let read_keys: Option<Vec<String>> = keys.map(|ks| {
            let mut v = ks.to_vec();
            for (key, _) in &s.props_eq {
                if !v.contains(key) {
                    v.push(key.clone());
                }
            }
            v
        });
        let limit = s.limit.unwrap_or(usize::MAX);
        let keep = |n: &NodeRow| {
            s.label.is_none_or(|l| n.labels.contains(&l))
                && s.after.is_none_or(|a| n.id > a)
                && s.props_eq
                    .iter()
                    .all(|(key, v)| crate::mem::MemStore::json_matches(&n.props, key, v))
        };
        let trim = |mut n: NodeRow| {
            if let Some(ks) = keys
                && read_keys.as_ref().is_some_and(|r| r.len() != ks.len())
            {
                n.props = only(&n.props, ks);
            }
            n
        };
        // By id, or through an index.
        let fixed: Option<Vec<u64>> = match &s.ids {
            Some(ids) => {
                let mut v = ids.clone();
                v.sort_unstable();
                v.dedup();
                Some(v)
            }
            None => self.index_ids(s)?,
        };
        if let Some(ids) = fixed {
            self.stats.shard_queries += 1;
            let mut out = Vec::new();
            for id in ids {
                if s.after.is_some_and(|a| id <= a) {
                    continue;
                }
                if let Some(n) = self.read_node(id, read_keys.as_deref())?
                    && keep(&n)
                {
                    out.push(trim(n));
                    if out.len() >= limit {
                        break;
                    }
                }
            }
            return Ok(out);
        }
        // A label or full scan, page by page in id order.
        let eq_keys: Vec<String> = s.props_eq.iter().map(|(key, _)| key.clone()).collect();
        let page = if s.props_eq.is_empty() {
            limit
        } else {
            1024.min(limit.saturating_mul(4).max(64))
        };
        let mut out = Vec::new();
        let mut after = s.after;
        loop {
            let ids = self.next_ids(s.label, after, page)?;
            let Some(&last) = ids.last() else {
                break;
            };
            // Dense ids (most of a label's nodes made together): one range
            // read of the node family beats a point read per node.
            let mut dense: HashMap<u64, Bytes> = HashMap::new();
            if self.g.shards == 1 && ids.len() >= 16 && (last - ids[0]) / 4 < ids.len() as u64 {
                let start = self.g.space.node(1, ids[0]);
                let end = self.g.space.node(1, last.saturating_add(1));
                let want: HashSet<u64> = ids.iter().copied().collect();
                for (key, v) in self.scan_range(&start, &end, usize::MAX)? {
                    let id = be64(&key, key.len() - 8)?;
                    if want.contains(&id) {
                        dense.insert(id, v);
                    }
                }
            }
            for id in &ids {
                let (d, rk) = self.g.home(*id);
                let v = if dense.is_empty() {
                    self.get(&self.g.space.node(d, *id))?
                } else {
                    dense.remove(id)
                };
                let Some(v) = v else {
                    continue;
                };
                // The pushed equalities first, on just their keys: most
                // nodes of a label fail them, and decoding all of a node
                // costs more than reading it.
                if !eq_keys.is_empty() {
                    let (_, rec) = k::split_node(&v)?;
                    let (p, _) = self.g.decode(rec, Some(&eq_keys))?;
                    if !s
                        .props_eq
                        .iter()
                        .all(|(key, w)| crate::mem::MemStore::json_matches(&p, key, w))
                    {
                        continue;
                    }
                }
                let n = self.node_row(*id, rk, &v, read_keys.as_deref())?;
                if keep(&n) {
                    out.push(trim(n));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
            if ids.len() < page {
                break;
            }
            after = Some(last);
        }
        Ok(out)
    }

    async fn nodes(&mut self, ids: &[u64]) -> Result<Vec<NodeRow>> {
        self.stats.shard_queries += 1;
        self.read_nodes(ids, None)
    }

    async fn nodes_part(&mut self, ids: &[u64], keys: Option<&[String]>) -> Result<Vec<NodeRow>> {
        self.stats.shard_queries += 1;
        self.read_nodes(ids, keys)
    }

    async fn nodes_where(
        &mut self,
        ids: &[u64],
        keys: Option<&[String]>,
        filter: &[(String, CmpOp, f64)],
    ) -> Result<Vec<NodeRow>> {
        self.stats.shard_queries += 1;
        let rk = Self::with_filter_keys(keys, filter);
        let mut rows = self.read_nodes(ids, rk.as_deref())?;
        rows.retain(|n| passes(&n.labels, &n.props, filter));
        if let (Some(ks), Some(r)) = (keys, &rk)
            && r.len() != ks.len()
        {
            for n in &mut rows {
                n.props = only(&n.props, ks);
            }
        }
        Ok(rows)
    }

    async fn edges_part(
        &mut self,
        at: &[(u64, u64)],
        keys: Option<&[String]>,
    ) -> Result<Vec<EdgeRow>> {
        self.stats.shard_queries += 1;
        let mut out = Vec::with_capacity(at.len());
        for &(rk, id) in at {
            if let Some(e) = self.read_edge(id, Some(rk), keys)? {
                out.push(e);
            }
        }
        Ok(out)
    }

    async fn edges_at(&mut self, at: &[(u64, u64)]) -> Result<Vec<EdgeRow>> {
        self.edges_part(at, None).await
    }

    async fn expand(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
    ) -> Result<Vec<(EdgeRow, Option<NodeRow>)>> {
        if dir == Dir::Both {
            return Ok(Vec::new());
        }
        let adj = self.adjacency(from, types, dir)?;
        let mut out = Vec::with_capacity(adj.len());
        for a in &adj {
            let l = Self::link_of(a);
            let Some((props, types)) = self.rel_props(a, None)? else {
                continue;
            };
            let far = self.read_node(a.far, None)?;
            out.push((
                EdgeRow {
                    id: l.id,
                    src: l.src,
                    dst: l.dst,
                    src_rk: l.src_rk,
                    dst_rk: l.dst_rk,
                    rtype: l.rtype,
                    props,
                    types,
                },
                far,
            ));
        }
        Ok(out)
    }

    async fn links(&mut self, from: &[u64], types: &[u32], dir: Dir) -> Result<Vec<Link>> {
        if dir == Dir::Both {
            return Ok(Vec::new());
        }
        Ok(self
            .adjacency(from, types, dir)?
            .iter()
            .map(Self::link_of)
            .collect())
    }

    async fn links_both(&mut self, from: &[u64], types: &[u32]) -> Result<Vec<(Link, bool)>> {
        Ok(self
            .adjacency(from, types, Dir::Both)?
            .iter()
            .map(|a| (Self::link_of(a), a.out))
            .collect())
    }

    async fn links_far(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        far: &FarRead<'_>,
    ) -> Result<Vec<FarLink>> {
        if dir == Dir::Both {
            return Ok(Vec::new());
        }
        Ok(self
            .far_links(from, types, dir, far)?
            .into_iter()
            .map(|(l, _)| l)
            .collect())
    }

    async fn links_far_both(
        &mut self,
        from: &[u64],
        types: &[u32],
        far: &FarRead<'_>,
    ) -> Result<Vec<(FarLink, bool)>> {
        self.far_links(from, types, Dir::Both, far)
    }

    async fn reach(
        &mut self,
        shard: usize,
        seeds: &[(u64, u64)],
        types: &[u32],
        dir: Dir,
        hops: u32,
    ) -> Result<Vec<Reach>> {
        self.stats.shard_queries += 1;
        self.stats.cte_queries += 1;
        if dir == Dir::Both {
            return Ok(Vec::new());
        }
        let mut best: HashMap<(u64, u64), Reach> = HashMap::new();
        let mut cache: HashMap<u64, Vec<u64>> = HashMap::new();
        for &(origin, seed) in seeds {
            let mut q = VecDeque::from([(seed, 0u32)]);
            let mut seen: HashSet<u64> = HashSet::new();
            while let Some((x, dep)) = q.pop_front() {
                if dep >= hops {
                    continue;
                }
                let next = if let Some(v) = cache.get(&x) {
                    v.clone()
                } else {
                    let v: Vec<u64> = self
                        .adjacency(&[x], types, dir)?
                        .iter()
                        .map(|a| a.far)
                        .collect();
                    cache.insert(x, v.clone());
                    v
                };
                for y in next {
                    if !seen.insert(y) {
                        continue;
                    }
                    let yrk = rk_of(y);
                    best.entry((origin, y)).or_insert(Reach {
                        origin,
                        node: y,
                        rk: yrk,
                        depth: dep + 1,
                    });
                    if shard_of_rk(yrk, self.g.shards) == shard {
                        q.push_back((y, dep + 1));
                    }
                }
            }
        }
        Ok(best.into_values().collect())
    }

    async fn edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        self.stats.shard_queries += 1;
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            if let Some(e) = self.read_edge(id, None, None)? {
                out.push(e);
            }
        }
        Ok(out)
    }

    async fn edges_touching(&mut self, nodes: &[u64], limit: usize) -> Result<Vec<EdgeRow>> {
        self.stats.shard_queries += 1;
        // At the latest state: `DETACH DELETE` holds the nodes' exclusive
        // locks by now, so a relationship created after the statement's
        // view (its creator held a shared lock on the node) is seen and
        // deleted too.
        let adj = self.adjacency_at(nodes, &[], Dir::Both, true)?;
        let mut ids: Vec<(u64, u64)> = adj
            .iter()
            .map(|a| (a.edge, rk_of(if a.out { a.near } else { a.far })))
            .collect();
        ids.sort_unstable();
        ids.dedup_by_key(|x| x.0);
        ids.truncate(limit);
        let mut out = Vec::with_capacity(ids.len());
        for (id, rk) in ids {
            let key = self.g.space.edge(self.g.data_shard(rk), id);
            if let Some(v) = self.current(&key)? {
                out.push(self.edge_row(id, &v, None)?);
            }
        }
        Ok(out)
    }

    async fn lock_edges(&mut self, ids: &[u64]) -> Result<Vec<EdgeRow>> {
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let shards: Vec<u32> = self.g.all_shards().collect();
        let mut found: HashMap<u64, EdgeRow> = HashMap::new();
        for id in sorted {
            for &d in &shards {
                let key = self.g.space.edge(d, id);
                // The record is where its source lives; with one shard
                // there is nothing to probe.
                if shards.len() > 1 && self.get(&key)?.is_none() && self.current(&key)?.is_none() {
                    continue;
                }
                self.lock(&key, true).await?;
                if let Some(v) = self.current(&key)? {
                    found.insert(id, self.edge_row(id, &v, None)?);
                }
                break;
            }
        }
        Ok(ids.iter().filter_map(|i| found.remove(i)).collect())
    }

    async fn lock_nodes(&mut self, ids: &[u64], exclusive: bool) -> Result<Vec<NodeRow>> {
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        for &id in &sorted {
            let (d, _) = self.g.home(id);
            let key = self.g.space.node(d, id);
            self.lock(&key, exclusive).await?;
        }
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            let (d, rk) = self.g.home(id);
            if let Some(v) = self.current(&self.g.space.node(d, id))? {
                out.push(self.node_row(id, rk, &v, None)?);
            }
        }
        Ok(out)
    }

    async fn alloc_ids(&mut self, kind: IdKind, n: usize) -> Result<Vec<u64>> {
        self.g.alloc(kind, n).await
    }

    async fn insert_nodes(&mut self, rows: &[NodeRow]) -> Result<()> {
        self.writing().await?;
        self.g.ensure_keys(rows.iter().map(|r| &r.props)).await?;
        for r in rows {
            let ls = self.g.labelset(&r.labels).await?;
            let labels = self.g.labels_of(ls);
            let (d, _) = self.g.home(r.id);
            self.add_index_entries(d, r.id, &labels, None, &r.props, r.types.as_ref(), &[])
                .await?;
            let rec = self.g.record(&r.props, r.types.as_ref())?;
            self.put(self.g.space.node(d, r.id), k::node_value(ls, &rec))
                .await?;
            self.put(self.g.space.label_entry(d, ls, r.id), Vec::new())
                .await?;
        }
        Ok(())
    }

    async fn insert_edges(&mut self, rows: &[EdgeRow]) -> Result<()> {
        self.writing().await?;
        self.g.ensure_keys(rows.iter().map(|r| &r.props)).await?;
        for r in rows {
            let (sd, _) = self.g.home(r.src);
            let (dd, _) = self.g.home(r.dst);
            // Both ends share-locked (the executor already holds them for
            // nodes that existed): their label sets can't change until
            // this transaction ends.
            self.lock(&self.g.space.node(sd, r.src), false).await?;
            self.lock(&self.g.space.node(dd, r.dst), false).await?;
            let src_ls = match self.current(&self.g.space.node(sd, r.src))? {
                Some(v) => be32(&v, 0)?,
                None => 0,
            };
            let dst_ls = match self.current(&self.g.space.node(dd, r.dst))? {
                Some(v) => be32(&v, 0)?,
                None => 0,
            };
            self.add_index_entries(
                sd,
                r.id,
                &[],
                Some(r.rtype),
                &r.props,
                r.types.as_ref(),
                &[],
            )
            .await?;
            let rec = self.g.record(&r.props, r.types.as_ref())?;
            self.put(
                self.g.space.adj(sd, OUT, r.src, r.rtype, r.dst, r.id),
                k::out_value(dst_ls, &rec),
            )
            .await?;
            self.put(
                self.g.space.adj(dd, IN, r.dst, r.rtype, r.src, r.id),
                src_ls.to_be_bytes().to_vec(),
            )
            .await?;
            self.put(
                self.g.space.edge(sd, r.id),
                k::edge_value(r.src, r.rtype, r.dst, &rec),
            )
            .await?;
            let mut ts = Vec::with_capacity(16);
            ts.extend_from_slice(&r.src.to_be_bytes());
            ts.extend_from_slice(&r.dst.to_be_bytes());
            self.put(self.g.space.type_entry(sd, r.rtype, r.id), ts)
                .await?;
        }
        Ok(())
    }

    async fn update_node(&mut self, row: &NodeRow, _old_labels: &[u32]) -> Result<()> {
        self.writing().await?;
        self.g.ensure_keys(std::iter::once(&row.props)).await?;
        let (d, _) = self.g.home(row.id);
        let nk = self.g.space.node(d, row.id);
        self.lock(&nk, true).await?;
        let Some(old) = self.current(&nk)? else {
            // Deleted meanwhile: nothing to update.
            return Ok(());
        };
        let (old_ls, old_rec) = k::split_node(&old)?;
        let old_labels = self.g.labels_of(old_ls);
        let (old_props, old_types) = self.g.decode(old_rec, None)?;
        let ls = self.g.labelset(&row.labels).await?;
        let labels = self.g.labels_of(ls);
        // Index entries: the old ones out, the new ones in (a unique
        // entry this node already owns stays).
        let kept = self.kept(
            (&old_labels, &old_props, old_types.as_ref()),
            (&labels, &row.props, row.types.as_ref()),
            None,
        );
        self.drop_index_entries(
            d,
            row.id,
            &old_labels,
            None,
            &old_props,
            old_types.as_ref(),
            &kept,
        )
        .await?;
        self.add_index_entries(
            d,
            row.id,
            &labels,
            None,
            &row.props,
            row.types.as_ref(),
            &kept,
        )
        .await?;
        let rec = self.g.record(&row.props, row.types.as_ref())?;
        self.t()?.put(nk, k::node_value(ls, &rec))?;
        if ls != old_ls {
            self.del(self.g.space.label_entry(d, old_ls, row.id))
                .await?;
            self.put(self.g.space.label_entry(d, ls, row.id), Vec::new())
                .await?;
            self.relabel(row.id, ls).await?;
        }
        Ok(())
    }

    async fn update_edge(&mut self, row: &EdgeRow) -> Result<()> {
        self.writing().await?;
        self.g.ensure_keys(std::iter::once(&row.props)).await?;
        let (sd, _) = self.g.home(row.src);
        let ek = self.g.space.edge(sd, row.id);
        self.lock(&ek, true).await?;
        let Some(old) = self.current(&ek)? else {
            return Ok(());
        };
        let (src, rtype, dst, old_rec) = k::split_edge(&old)?;
        let (old_props, old_types) = self.g.decode(old_rec, None)?;
        let kept = self.kept(
            (&[], &old_props, old_types.as_ref()),
            (&[], &row.props, row.types.as_ref()),
            Some(rtype),
        );
        self.drop_index_entries(
            sd,
            row.id,
            &[],
            Some(rtype),
            &old_props,
            old_types.as_ref(),
            &kept,
        )
        .await?;
        self.add_index_entries(
            sd,
            row.id,
            &[],
            Some(rtype),
            &row.props,
            row.types.as_ref(),
            &kept,
        )
        .await?;
        let rec = self.g.record(&row.props, row.types.as_ref())?;
        self.t()?.put(ek, k::edge_value(src, rtype, dst, &rec))?;
        let ok = self.g.space.adj(sd, OUT, src, rtype, dst, row.id);
        self.lock(&ok, true).await?;
        let far_ls = match self.current(&ok)? {
            Some(v) => be32(&v, 0)?,
            None => 0,
        };
        self.t()?.put(ok, k::out_value(far_ls, &rec))?;
        Ok(())
    }

    async fn delete_edges(&mut self, edges: &[EdgeKey]) -> Result<u64> {
        self.writing().await?;
        let mut n = 0;
        for e in edges {
            let (sd, _) = self.g.home(e.src);
            let ek = self.g.space.edge(sd, e.id);
            self.lock(&ek, true).await?;
            let Some(v) = self.current(&ek)? else {
                continue;
            };
            let (src, rtype, dst, rec) = k::split_edge(&v)?;
            let (props, types) = self.g.decode(rec, None)?;
            self.drop_index_entries(sd, e.id, &[], Some(rtype), &props, types.as_ref(), &[])
                .await?;
            let (dd, _) = self.g.home(dst);
            self.t()?.delete(ek)?;
            self.del(self.g.space.adj(sd, OUT, src, rtype, dst, e.id))
                .await?;
            self.del(self.g.space.adj(dd, IN, dst, rtype, src, e.id))
                .await?;
            self.del(self.g.space.type_entry(sd, rtype, e.id)).await?;
            n += 1;
        }
        Ok(n)
    }

    async fn delete_nodes(&mut self, ids: &[u64]) -> Result<u64> {
        self.writing().await?;
        let mut n = 0;
        for &id in ids {
            let (d, _) = self.g.home(id);
            let nk = self.g.space.node(d, id);
            self.lock(&nk, true).await?;
            let Some(v) = self.current(&nk)? else {
                continue;
            };
            let (ls, rec) = k::split_node(&v)?;
            let labels = self.g.labels_of(ls);
            let (props, types) = self.g.decode(rec, None)?;
            self.drop_index_entries(d, id, &labels, None, &props, types.as_ref(), &[])
                .await?;
            self.t()?.delete(nk)?;
            self.del(self.g.space.label_entry(d, ls, id)).await?;
            self.tx.deleted.insert(id);
            n += 1;
        }
        Ok(n)
    }

    async fn label_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some(id) = self.g.dict().labels.by_name.get(name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        Ok(Some(
            self.g.ensure(DK_LABEL, &[name.as_bytes().to_vec()]).await?[0],
        ))
    }

    async fn type_id(&mut self, name: &str, create: bool) -> Result<Option<u32>> {
        if let Some(id) = self.g.dict().types.by_name.get(name) {
            return Ok(Some(*id));
        }
        if !create {
            return Ok(None);
        }
        Ok(Some(
            self.g.ensure(DK_TYPE, &[name.as_bytes().to_vec()]).await?[0],
        ))
    }

    fn label_name(&self, id: u32) -> Option<String> {
        self.g.dict().labels.by_id.get(&id).cloned()
    }

    fn type_name(&self, id: u32) -> Option<String> {
        self.g.dict().types.by_id.get(&id).cloned()
    }

    fn all_labels(&self) -> Vec<String> {
        self.g.dict().labels.by_id.values().cloned().collect()
    }

    fn all_types(&self) -> Vec<String> {
        self.g.dict().types.by_id.values().cloned().collect()
    }

    async fn batches(&mut self) -> Result<()> {
        if self.tx.implicit {
            Ok(())
        } else {
            Err(not_implicit())
        }
    }

    async fn end_batch(&mut self, ok: bool, deleted: &[u64]) -> Result<()> {
        if !self.tx.implicit {
            return Err(not_implicit());
        }
        self.tx.deleted.extend(deleted.iter().copied());
        let r = if ok {
            self.tx.check_deleted(&self.g)
        } else {
            Ok(())
        };
        let t = self.tx.txn.take().ok_or_else(gone)?;
        let r = match r {
            Ok(()) if ok => match t.commit() {
                Ok(tk) => {
                    self.tx.tickets.push(tk);
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Ok(()) => {
                t.rollback();
                Ok(())
            }
            Err(e) => {
                t.rollback();
                Err(e)
            }
        };
        self.tx.deleted.clear();
        self.tx.schema_shared = false;
        let mut nt = Txn::begin_on(Arc::clone(&self.tx.path), self.tx.opts);
        nt.begin_statement();
        self.tx.txn = Some(nt);
        r
    }

    fn indexes(&self) -> Vec<IndexDef> {
        self.g.defs()
    }

    async fn schema(&mut self, cmd: &Schema) -> Result<SchemaCounts> {
        self.schema_cmd(cmd).await
    }
}

impl EngineStore<'_> {
    /// [`GraphStore::links_far`] in one or both directions: each link with
    /// `true` when it goes out of its near node.
    fn far_links(
        &mut self,
        from: &[u64],
        types: &[u32],
        dir: Dir,
        far: &FarRead<'_>,
    ) -> Result<Vec<(FarLink, bool)>> {
        let adj = self.adjacency(from, types, dir)?;
        let only_set: HashSet<u64> = far.only.iter().copied().collect();
        let node_keys = if far.join {
            Self::with_filter_keys(far.keys, far.filter)
        } else {
            None
        };
        let rel_read_keys: Option<Option<Vec<String>>> = far.rel_keys.map(|ks| {
            if ks.is_empty() {
                None
            } else {
                Self::with_filter_keys(Some(ks), far.rel_filter)
            }
        });
        let mut out = Vec::with_capacity(adj.len());
        for a in &adj {
            if !only_set.is_empty() && !only_set.contains(&a.far) {
                continue;
            }
            if far.after.is_some_and(|x| a.edge <= x) {
                continue;
            }
            let l = Self::link_of(a);
            // The relationship's properties, when asked or filtered.
            let rel = if far.rel_keys.is_some() || !far.rel_filter.is_empty() {
                let ks: Option<Vec<String>> = match &rel_read_keys {
                    Some(k2) => k2.clone(),
                    None => Self::with_filter_keys(Some(&[]), far.rel_filter),
                };
                match self.rel_props(a, ks.as_deref())? {
                    Some((p, t)) => {
                        if !passes(&[], &p, far.rel_filter) {
                            continue;
                        }
                        let p = match far.rel_keys {
                            Some(ks) if !ks.is_empty() => only(&p, ks),
                            Some(_) => p,
                            None => Json::Object(serde_json::Map::new()),
                        };
                        far.rel_keys.map(|_| (p, t))
                    }
                    // Deleted in this transaction.
                    None => continue,
                }
            } else {
                None
            };
            let node = if far.join {
                if far.keys.is_some_and(<[String]>::is_empty)
                    && far
                        .filter
                        .iter()
                        .all(|(key, _, _)| key.starts_with(LABEL_KEY))
                    && !self.tx.deleted.contains(&a.far)
                {
                    // Labels only: from the adjacency entry.
                    let labels = self.g.labels_of(Self::far_labelset(a)?);
                    if !passes(&labels, &Json::Null, far.filter) {
                        continue;
                    }
                    Some(NodeRow {
                        id: a.far,
                        rk: rk_of(a.far),
                        labels,
                        props: Json::Object(serde_json::Map::new()),
                        types: None,
                    })
                } else {
                    match self.read_node(a.far, node_keys.as_deref())? {
                        Some(mut n) => {
                            if !passes(&n.labels, &n.props, far.filter) {
                                continue;
                            }
                            if let (Some(ks), Some(r)) = (far.keys, &node_keys)
                                && r.len() != ks.len()
                            {
                                n.props = only(&n.props, ks);
                            }
                            Some(n)
                        }
                        None => None,
                    }
                }
            } else {
                None
            };
            out.push(((l, node, rel), a.out));
        }
        if let Some(n) = far.limit {
            out.truncate(n);
        }
        Ok(out)
    }
}

/// Every key of a graph on the engine is under these prefixes: what a
/// `DROP GRAPH` deletes.
#[must_use]
pub fn graph_ranges(space: GraphSpace, shards: usize) -> Vec<KeyRange> {
    let mut v = Vec::new();
    for sh in 0..=u32::try_from(shards).unwrap_or(1) {
        let mut p = Vec::new();
        sprig_engine::keys::KeyPrefix {
            tenant: space.tenant,
            shard: sh,
            object: space.object,
            kind: 0,
            index: 0,
        }
        .encode_into(&mut p);
        // tenant | shard | object: the first 16 bytes.
        p.truncate(16);
        v.push(KeyRange::prefix(&p));
    }
    v
}

/// Deletes every key of a graph, in transactions of at most `batch` keys.
///
/// # Errors
/// The engine's.
pub async fn drop_graph(g: &EngineGraph, batch: usize) -> Result<u64> {
    // No transaction may write it from now on; the exclusive schema lock
    // waits for the ones writing it now.
    g.dropped.store(true, std::sync::atomic::Ordering::Release);
    g.schema_exclusive().await?.rollback();
    let mut n = 0u64;
    for r in graph_ranges(g.space, g.shards) {
        n += g.delete_prefix(&r.start, batch).await?;
    }
    *g.dict.write().unwrap_or_else(poisoned) = Dict::default();
    g.indexes.write().unwrap_or_else(poisoned).clear();
    *g.ids.lock().unwrap_or_else(poisoned) = [IdBlock::default(); 2];
    Ok(n)
}

/// The invariants of the stored graph at the latest committed state, for
/// tests and recovery checks: every edge has all four keys or none, no
/// adjacency entry names a missing node, every adjacency entry's label set
/// is its far node's, and every node has exactly one label-scan entry, for
/// its label set. The problems found, as text (empty: none).
///
/// # Errors
/// The engine's.
pub fn check_invariants(g: &EngineGraph) -> Result<Vec<String>> {
    let mut t = Txn::begin_on(Arc::clone(&g.path), g.opts);
    let mut bad = Vec::new();
    let mut nodes: HashMap<u64, u32> = HashMap::new();
    let mut edges: HashMap<u64, (u64, u32, u64)> = HashMap::new();
    let scan =
        |t: &mut Txn, p: &[u8]| t.scan_current(&KeyRange::prefix(p), ScanDir::Forward, usize::MAX);
    for sh in g.all_shards() {
        for (key, v) in scan(
            &mut t,
            &g.space
                .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::NODE),
        )? {
            let id = be64(&key, key.len() - 8)?;
            if g.home(id).0 != sh {
                bad.push(format!("node {id} on the wrong shard"));
            }
            nodes.insert(id, be32(&v, 0)?);
        }
        for (key, v) in scan(
            &mut t,
            &g.space
                .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::EDGE),
        )? {
            let id = be64(&key, key.len() - 8)?;
            let (s, ty, d, _) = k::split_edge(&v)?;
            edges.insert(id, (s, ty, d));
        }
    }
    let mut labels_seen: HashMap<u64, u32> = HashMap::new();
    let mut out_seen: HashSet<u64> = HashSet::new();
    let mut in_seen: HashSet<u64> = HashSet::new();
    let mut type_seen: HashSet<u64> = HashSet::new();
    for sh in g.all_shards() {
        for (key, _) in scan(
            &mut t,
            &g.space
                .prefix(sh, sprig_engine::keys::kind::LABELS, k::LABEL_SCAN),
        )? {
            let ls = be32(&key, sprig_engine::keys::PREFIX_LEN)?;
            let id = be64(&key, key.len() - 8)?;
            if labels_seen.insert(id, ls).is_some() {
                bad.push(format!("node {id} has two label-scan entries"));
            }
        }
        for (key, _) in scan(
            &mut t,
            &g.space
                .prefix(sh, sprig_engine::keys::kind::LABELS, k::TYPE_SCAN),
        )? {
            type_seen.insert(be64(&key, key.len() - 8)?);
        }
        for (dir, seen) in [(OUT, &mut out_seen), (IN, &mut in_seen)] {
            for (key, v) in scan(
                &mut t,
                &g.space.prefix(sh, sprig_engine::keys::kind::ADJACENCY, dir),
            )? {
                let (near, ty, far, edge) = adj_parts(&key)?;
                seen.insert(edge);
                let want = if dir == OUT {
                    (near, ty, far)
                } else {
                    (far, ty, near)
                };
                if edges.get(&edge) != Some(&want) {
                    bad.push(format!("adjacency of edge {edge} doesn't match its record"));
                }
                for n in [near, far] {
                    if !nodes.contains_key(&n) {
                        bad.push(format!("edge {edge} names missing node {n}"));
                    }
                }
                if let Some(ls) = nodes.get(&far)
                    && be32(&v, 0)? != *ls
                {
                    bad.push(format!("edge {edge}: stale label set of node {far}"));
                }
            }
        }
    }
    for (id, ls) in &nodes {
        if labels_seen.get(id) != Some(ls) {
            bad.push(format!("node {id}: label-scan entry missing or wrong"));
        }
    }
    for id in labels_seen.keys() {
        if !nodes.contains_key(id) {
            bad.push(format!("label-scan entry of missing node {id}"));
        }
    }
    for id in edges.keys() {
        if !(out_seen.contains(id) && in_seen.contains(id) && type_seen.contains(id)) {
            bad.push(format!("edge {id} lacks one of its keys"));
        }
    }
    for id in out_seen.iter().chain(&in_seen).chain(&type_seen) {
        if !edges.contains_key(id) {
            bad.push(format!("edge {id} has keys but no record"));
        }
    }
    t.rollback();
    bad.sort();
    bad.dedup();
    Ok(bad)
}

/// Counts of the stored graph (nodes, edges) at the latest committed
/// state.
///
/// # Errors
/// The engine's.
pub fn counts(g: &EngineGraph) -> Result<(usize, usize)> {
    let mut t = Txn::begin_on(Arc::clone(&g.path), g.opts);
    let (mut n, mut e) = (0, 0);
    for sh in g.all_shards() {
        let p = g
            .space
            .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::NODE);
        n += t
            .scan_current(&KeyRange::prefix(&p), ScanDir::Forward, usize::MAX)?
            .len();
        let p = g
            .space
            .prefix(sh, sprig_engine::keys::kind::PRIMARY, k::EDGE);
        e += t
            .scan_current(&KeyRange::prefix(&p), ScanDir::Forward, usize::MAX)?
            .len();
    }
    t.rollback();
    Ok((n, e))
}

/// The executor's tests' store: a graph on an in-memory engine path, in
/// one transaction of its own.
#[cfg(test)]
pub(crate) type EngineTestStore = EngineStore<'static>;

#[cfg(test)]
impl EngineStore<'static> {
    /// A fresh graph with `shards` data shards on an in-memory engine.
    pub(crate) fn new(shards: usize) -> Self {
        let path = sprig_engine::txn::testing::MemPath::start(
            sprig_engine::txn::UnitConfig::default(),
            std::time::Duration::ZERO,
        );
        let g = EngineGraph::open(path, 1, 1, shards).unwrap_or_else(|e| panic!("open: {e}"));
        Self::standalone(&g)
    }

    /// Hops already read properties late on the engine.
    pub(crate) fn with_late_reads(self) -> Self {
        self
    }

    pub(crate) fn node_count(&mut self) -> usize {
        self.element_counts().map_or(0, |c| c.0)
    }

    pub(crate) fn edge_count(&mut self) -> usize {
        self.element_counts().map_or(0, |c| c.1)
    }
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
