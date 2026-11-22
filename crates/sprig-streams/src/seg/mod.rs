//! Streams stored on the node's engine: one segment log per partition,
//! fed by the engine's commit log.
//!
//! ```text
//! <dir>/manifest.json                       what is durable, and the log floor
//! <dir>/t<tenant>/s<stream>-p<partition>/   one partition
//!     <base offset, 20 digits>.log          record batches v2, as received
//! ```
//!
//! **Produce.** A batch is proposed through the engine's log as a payload
//! note ([`note`]); one fsync of the log's group commit covers every
//! partition's batches. The apply step ([`Applier`]) checks idempotence,
//! takes the partition's next offset, writes it and the leader epoch into
//! the head (the CRC stays valid) and appends the bytes to the active
//! segment, unsynced. The producer is answered from the apply's outcome.
//!
//! **Durability.** The flusher syncs the segments written since its last
//! pass, writes the manifest (offsets, segment lengths, epochs, producer
//! state, at one applied index) and then raises the engine's log floor to
//! that index, so the log keeps every entry the files may lack. After a
//! crash the segments are cut back to the manifest's lengths and the log
//! is replayed from the floor: the same batches get the same offsets.
//!
//! **Fetch** reads whole stored batches from the batch holding the
//! offset, up to the high watermark (the apply step's next offset), and
//! waits for the apply step's signal when there is nothing new.
//!
//! **Retention** drops whole segments; **compaction** ([`compact`])
//! rewrites rolled segments of compacted streams keeping each key's
//! newest record.
//!
//! Consumer offsets and groups are engine records ([`records`]), so an
//! offset commit can be part of a SQL transaction.

pub mod batch;
pub mod compact;
pub mod kafka;
pub mod note;
pub mod part;
pub mod records;
pub mod segment;
pub mod sql;
pub mod testing;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};
use sprig_engine::log::{LogEntry, Payload};
use sprig_engine::{ApplyObserver, Durability, Engine};

use self::part::{Outcome, PartKey, PartState, ProducedBatch, Producer};
use self::segment::Segment;

pub use self::part::PartKey as Key;

/// The engine's name for this log's floor.
pub const FLOOR: &str = "streams";
const MANIFEST: &str = "manifest.json";

/// Settings.
#[derive(Debug, Clone)]
pub struct SegConfig {
    /// A segment rolls past this many bytes (`segment.bytes`).
    pub segment_bytes: u64,
    /// Bytes between sparse index entries (`index.interval.bytes`).
    pub index_interval: u64,
    /// How often the flusher runs.
    pub flush_every: Duration,
    /// How often retention and compaction run.
    pub maintain_every: Duration,
    /// The commit log's durability for appends.
    pub durability: Durability,
    /// Idle producers' state is dropped after this (ms).
    pub producer_expiry_ms: i64,
}

impl Default for SegConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 256 << 20,
            index_interval: 4096,
            flush_every: Duration::from_secs(1),
            maintain_every: Duration::from_secs(30),
            durability: Durability::Local,
            producer_expiry_ms: 7 * 24 * 3_600_000,
        }
    }
}

/// A stream's storage policy, set by whoever owns its definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Rolled segments older than this go (ms; negative: never).
    pub retention_ms: i64,
    /// The partition's bytes are cut to this by dropping whole segments
    /// (negative: no limit).
    pub retention_bytes: i64,
    /// `cleanup.policy=compact`.
    pub compact: bool,
    /// Tombstones are kept this long after compaction reaches them (ms).
    pub delete_retention_ms: i64,
    /// Stamp the broker's time (`message.timestamp.type=LogAppendTime`).
    pub log_append_time: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            retention_ms: 168 * 3_600_000,
            retention_bytes: -1,
            compact: false,
            delete_retention_ms: 24 * 3_600_000,
            log_append_time: false,
        }
    }
}

/// A partition's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Bounds {
    /// The first offset served.
    pub log_start: i64,
    /// The next offset.
    pub high_watermark: i64,
    /// The current leader epoch.
    pub epoch: i32,
}

/// A proposed append, to wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pending {
    /// The log entry.
    pub index: u64,
    /// The note's position in it.
    pub slot: u32,
}

/// A read outside `[log start, high watermark]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfRange(pub Bounds);

/// What the manifest keeps of one segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegMeta {
    base: i64,
    len: u64,
    created_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProdMeta {
    id: i64,
    epoch: i16,
    last_ms: i64,
    batches: Vec<ProducedBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartMeta {
    key: PartKey,
    next_offset: i64,
    log_start: i64,
    cleaned_to: i64,
    epochs: Vec<(i32, i64)>,
    segments: Vec<SegMeta>,
    producers: Vec<ProdMeta>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    floor: u64,
    epoch: i32,
    dropped: Vec<(u64, u64)>,
    parts: Vec<PartMeta>,
}

/// One partition.
#[derive(Debug)]
pub struct Part {
    st: Mutex<PartState>,
}

impl Part {
    fn lock(&self) -> MutexGuard<'_, PartState> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The apply step's own state.
#[derive(Debug, Default)]
struct ApplyState {
    /// The last log index handed to the applier.
    applied: u64,
    /// The node's leader epoch.
    epoch: i32,
    /// Dropped streams (ids are never reused).
    dropped: BTreeSet<(u64, u64)>,
    /// Changed since the last manifest, beyond partition state.
    dirty: bool,
    /// A segment write failed: appends are refused until a restart.
    broken: Option<String>,
}

struct Inner {
    engine: Engine,
    dir: PathBuf,
    cfg: SegConfig,
    apply: Mutex<ApplyState>,
    parts: RwLock<HashMap<PartKey, Arc<Part>>>,
    results: Mutex<BTreeMap<(u64, u32), Outcome>>,
    changed: tokio::sync::watch::Sender<u64>,
    flushing: Mutex<()>,
    policies: Mutex<HashMap<(u64, u64), Policy>>,
}

/// The node's stream log. Cheap to clone.
#[derive(Clone)]
pub struct SegLog {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SegLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegLog")
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn io_err(what: &str, e: &io::Error) -> SprigError {
    SprigError::internal(format!("stream log: {what}: {e}"))
}

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Results kept for producers that stopped waiting.
const RESULTS_KEPT: usize = 100_000;

/// The apply observer the engine calls.
struct Applier(Weak<Inner>);

impl ApplyObserver for Applier {
    fn applied(&self, unit: sprig_engine::UnitId, entries: &[LogEntry]) {
        if let Some(i) = self.0.upgrade() {
            // Stream appends ride on the engine's notes unit; its index
            // sequence is the one this log tracks.
            if unit == i.engine.notes_unit() {
                i.apply(entries);
            }
        }
    }
}

fn part_dir(root: &Path, k: &PartKey) -> PathBuf {
    root.join(format!("t{}", k.tenant))
        .join(format!("s{}-p{}", k.stream, k.partition))
}

fn sync_dir(p: &Path) {
    if let Ok(f) = fs::File::open(p) {
        let _ = f.sync_all();
    }
}

/// Syncs a directory's entries (a directory that is gone is fine: a dropped
/// stream's).
fn sync_dir_checked(p: &Path) -> io::Result<()> {
    match fs::File::open(p) {
        Ok(f) => f.sync_all(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

impl Inner {
    fn part(&self, k: PartKey) -> Arc<Part> {
        if let Some(p) = self
            .parts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&k)
        {
            return Arc::clone(p);
        }
        let mut w = self.parts.write().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(w.entry(k).or_insert_with(|| {
            Arc::new(Part {
                st: Mutex::new(PartState::new(part_dir(&self.dir, &k))),
            })
        }))
    }

    fn existing(&self, k: &PartKey) -> Option<Arc<Part>> {
        self.parts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(k)
            .cloned()
    }

    fn policy(&self, tenant: u64, stream: u64) -> Option<Policy> {
        lock(&self.policies).get(&(tenant, stream)).copied()
    }

    /// The apply step for a group of committed entries.
    fn apply(&self, entries: &[LogEntry]) {
        let Some(last) = entries.last() else {
            return;
        };
        let mut a = lock(&self.apply);
        let mut outs = Vec::new();
        for e in entries {
            if e.index <= a.applied {
                continue;
            }
            let Ok(notes) = sprig_engine::log::payload::notes(&e.data) else {
                continue;
            };
            for (slot, n) in notes.iter().enumerate() {
                let Some(op) = note::decode(n) else {
                    continue;
                };
                let slot = u32::try_from(slot).unwrap_or(u32::MAX);
                let note::Op::Append {
                    key,
                    now_ms,
                    log_append_time,
                    batch,
                } = op;
                let out = self.apply_append(&mut a, key, now_ms, log_append_time, batch);
                outs.push(((e.index, slot), out));
            }
        }
        a.applied = a.applied.max(last.index);
        drop(a);
        if !outs.is_empty() {
            let mut r = lock(&self.results);
            r.extend(outs);
            while r.len() > RESULTS_KEPT {
                r.pop_first();
            }
        }
        self.changed.send_replace(last.index);
    }

    fn apply_append(
        &self,
        a: &mut ApplyState,
        key: PartKey,
        now_ms: i64,
        log_append_time: bool,
        batch: &[u8],
    ) -> Outcome {
        if a.dropped.contains(&(key.tenant, key.stream)) {
            return Outcome::Refused {
                code: part::UNKNOWN_TOPIC_OR_PARTITION,
                message: "the stream was dropped".into(),
            };
        }
        if let Some(why) = &a.broken {
            return Outcome::Refused {
                code: part::STORAGE_ERROR,
                message: why.clone(),
            };
        }
        let p = self.part(key);
        let mut st = p.lock();
        let epoch = st.epoch(a.epoch);
        let mut bytes = batch.to_vec();
        match st.append(
            &mut bytes,
            epoch,
            now_ms,
            log_append_time,
            self.cfg.segment_bytes,
            self.cfg.index_interval,
        ) {
            Ok(o) => o,
            Err(e) => {
                let why = format!("stream segment write failed: {e}");
                tracing::error!(tenant = key.tenant, stream = key.stream, partition = key.partition, error = %e, "stream segment write failed; appends refused until restart");
                a.broken = Some(why.clone());
                Outcome::Refused {
                    code: part::STORAGE_ERROR,
                    message: why,
                }
            }
        }
    }
}

impl SegLog {
    /// Opens the stream log in `dir` on `engine`: reads the manifest, cuts
    /// segments back to what it knows, registers with the engine (which
    /// replays the log from the floor), then starts a new leader epoch.
    /// Background work starts with [`SegLog::spawn_background`].
    ///
    /// # Errors
    /// The files or the engine's log can't be read.
    pub fn open(engine: &Engine, dir: impl Into<PathBuf>, cfg: SegConfig) -> Result<SegLog> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| io_err("create dir", &e))?;
        let manifest = read_manifest(&dir)?;
        let (tx, _) = tokio::sync::watch::channel(0u64);
        let inner = Arc::new(Inner {
            engine: engine.clone(),
            dir: dir.clone(),
            cfg,
            apply: Mutex::new(ApplyState::default()),
            parts: RwLock::new(HashMap::new()),
            results: Mutex::new(BTreeMap::new()),
            changed: tx,
            flushing: Mutex::new(()),
            policies: Mutex::new(HashMap::new()),
        });
        let mut from = if let Some(m) = &manifest {
            restore(&inner, m)?;
            m.floor.max(engine.log_floor(FLOOR).unwrap_or(0))
        } else {
            clear_parts(&dir);
            engine.consensus().applied_index(engine.notes_unit())
        };
        if let Some(held) = catch_up_start(from, engine.log_first_index(), engine.last_applied()) {
            // The commit log was trimmed past the floor: a binary that
            // didn't keep observer floors ran on this directory. Catch up
            // from what the log still holds; appends in the gap (only a
            // crash before a floor flush leaves any) are gone.
            tracing::warn!(
                floor = from,
                resumed_at = held,
                parts = manifest.as_ref().map_or(0, |m| m.parts.len()),
                "stream log: the commit log no longer holds entries above the stream log's \
                 floor (trimmed by a binary that didn't keep it); catching up from the \
                 oldest entry it holds"
            );
            from = held;
        }
        lock(&inner.apply).applied = from;
        let top = engine.observe(
            Arc::new(Applier(Arc::downgrade(&inner))) as Arc<dyn ApplyObserver>,
            from,
        )?;
        let log = SegLog { inner };
        {
            let mut a = lock(&log.inner.apply);
            a.applied = a.applied.max(top);
            a.epoch += 1;
            a.dirty = true;
        }
        log.flush()?;
        Ok(log)
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Sets a stream's policy (retention, compaction, timestamps).
    pub fn set_policy(&self, tenant: u64, stream: u64, p: Policy) {
        lock(&self.inner.policies).insert((tenant, stream), p);
    }

    /// The node's current leader epoch.
    #[must_use]
    pub fn epoch(&self) -> i32 {
        lock(&self.inner.apply).epoch
    }

    /// Proposes one batch (a whole, checked magic-2 batch) for `key`.
    ///
    /// # Errors
    /// The log refused it (closed).
    pub fn propose(&self, key: PartKey, batch: &[u8]) -> Result<Pending> {
        let lat = self
            .inner
            .policy(key.tenant, key.stream)
            .is_some_and(|p| p.log_append_time);
        let mut p = Payload::with_capacity(batch.len() + 48);
        p.note(&note::encode_append(key, now_ms(), lat, batch));
        let t = self.inner.engine.consensus().propose(
            self.inner.engine.notes_unit(),
            p.into_bytes(),
            self.inner.cfg.durability,
        )?;
        Ok(Pending {
            index: t.index(),
            slot: 0,
        })
    }

    /// Proposes several batches in one log entry: they are in the log
    /// together or not at all (a multi-row SQL publish over several
    /// partitions). One pending append per batch, in order.
    ///
    /// # Errors
    /// The log refused the entry.
    pub fn propose_many(&self, batches: &[(PartKey, Vec<u8>)]) -> Result<Vec<Pending>> {
        let now = now_ms();
        let mut p = Payload::with_capacity(batches.iter().map(|b| b.1.len() + 48).sum());
        for (key, batch) in batches {
            let lat = self
                .inner
                .policy(key.tenant, key.stream)
                .is_some_and(|p| p.log_append_time);
            p.note(&note::encode_append(*key, now, lat, batch));
        }
        let t = self.inner.engine.consensus().propose(
            self.inner.engine.notes_unit(),
            p.into_bytes(),
            self.inner.cfg.durability,
        )?;
        Ok((0..batches.len())
            .map(|slot| Pending {
                index: t.index(),
                slot: u32::try_from(slot).unwrap_or(u32::MAX),
            })
            .collect())
    }

    /// Adds an append of `batch` to `txn`: it happens when (and if) the
    /// transaction commits, in commit order, atomically with its writes (a
    /// publish inside a SQL transaction). Returns the note's slot; after the
    /// commit, [`SegLog::outcome`] with the commit's log index and the slot
    /// gives the offsets.
    ///
    /// # Errors
    /// The transaction is not active.
    pub fn append_in(
        &self,
        txn: &mut sprig_engine::txn::Txn,
        key: PartKey,
        batch: &[u8],
    ) -> Result<u32> {
        let lat = self
            .inner
            .policy(key.tenant, key.stream)
            .is_some_and(|p| p.log_append_time);
        let slot = u32::try_from(txn.notes()).unwrap_or(u32::MAX);
        txn.add_note(note::encode_append(key, now_ms(), lat, batch))?;
        Ok(slot)
    }

    /// The outcome of a proposed append, waiting until it is applied or
    /// `deadline`; `None` at the deadline (it may still be appended: an
    /// idempotent retry then finds it).
    pub async fn outcome(&self, p: Pending, deadline: tokio::time::Instant) -> Option<Outcome> {
        let mut rx = self.inner.changed.subscribe();
        loop {
            if let Some(o) = lock(&self.inner.results).remove(&(p.index, p.slot)) {
                return Some(o);
            }
            if *rx.borrow_and_update() >= p.index {
                // Applied, but no outcome: the entry carried no note we
                // read (cannot happen for our own proposals).
                if let Some(o) = lock(&self.inner.results).remove(&(p.index, p.slot)) {
                    return Some(o);
                }
                return Some(Outcome::Refused {
                    code: part::STORAGE_ERROR,
                    message: "append lost".into(),
                });
            }
            if tokio::time::timeout_at(deadline, rx.changed())
                .await
                .is_err()
            {
                return None;
            }
        }
    }

    /// Proposes and waits (tests, SQL publishes).
    ///
    /// # Errors
    /// The log refused it, or it was not applied in `wait`.
    pub async fn append(&self, key: PartKey, batch: &[u8], wait: Duration) -> Result<Outcome> {
        let p = self.propose(key, batch)?;
        self.outcome(p, tokio::time::Instant::now() + wait)
            .await
            .ok_or_else(|| SprigError::internal("stream append: timed out waiting for the log"))
    }

    /// A partition's bounds (zeros for one never written).
    #[must_use]
    pub fn bounds(&self, key: PartKey) -> Bounds {
        let epoch = self.epoch();
        match self.inner.existing(&key) {
            None => Bounds {
                log_start: 0,
                high_watermark: 0,
                epoch,
            },
            Some(p) => {
                let st = p.lock();
                Bounds {
                    log_start: st.log_start,
                    high_watermark: st.next_offset,
                    epoch: st.epoch(epoch),
                }
            }
        }
    }

    /// Whole stored batches from the one holding `offset`, at most
    /// `max_bytes` (the first whatever its size).
    ///
    /// # Errors
    /// `Err(Ok(OutOfRange))` outside the bounds; `Err(Err(_))` when the
    /// files can't be read.
    pub fn read(
        &self,
        key: PartKey,
        offset: i64,
        max_bytes: usize,
    ) -> std::result::Result<(Bounds, Vec<u8>), std::result::Result<OutOfRange, SprigError>> {
        let epoch = self.epoch();
        let Some(p) = self.inner.existing(&key) else {
            let b = Bounds {
                log_start: 0,
                high_watermark: 0,
                epoch,
            };
            return if offset == 0 {
                Ok((b, Vec::new()))
            } else {
                Err(Ok(OutOfRange(b)))
            };
        };
        let (b, view) = {
            let st = p.lock();
            let b = Bounds {
                log_start: st.log_start,
                high_watermark: st.next_offset,
                epoch: st.epoch(epoch),
            };
            if offset < b.log_start || offset > b.high_watermark {
                return Err(Ok(OutOfRange(b)));
            }
            if offset == b.high_watermark {
                return Ok((b, Vec::new()));
            }
            let v = st.view(offset).map_err(|e| Err(io_err("read", &e)))?;
            (b, v)
        };
        match view {
            None => Ok((b, Vec::new())),
            Some(v) => v
                .read(max_bytes)
                .map(|bytes| (b, bytes))
                .map_err(|e| Err(io_err("read", &e))),
        }
    }

    /// Waits until a partition of `beyond` has a high watermark above its
    /// offset, or `deadline`.
    pub async fn wait(&self, beyond: &[(PartKey, i64)], deadline: tokio::time::Instant) {
        let mut rx = self.inner.changed.subscribe();
        loop {
            rx.borrow_and_update();
            if beyond
                .iter()
                .any(|(k, o)| self.bounds(*k).high_watermark > *o)
            {
                return;
            }
            if tokio::time::timeout_at(deadline, rx.changed())
                .await
                .is_err()
            {
                return;
            }
        }
    }

    /// The first record whose timestamp is at least `ts`: (offset,
    /// timestamp). `ts = -3`: the record with the largest timestamp.
    ///
    /// # Errors
    /// The files can't be read or hold a malformed batch.
    pub fn offset_for_time(&self, key: PartKey, ts: i64) -> Result<Option<(i64, i64)>> {
        let Some(p) = self.inner.existing(&key) else {
            return Ok(None);
        };
        let st = p.lock();
        let decode = |bytes: &[u8]| {
            sprig_proto_kafka::records::RecordBatch::decode(
                &mut sprig_proto_kafka::wire::Reader::new(bytes),
                256 << 20,
            )
            .map_err(|e| SprigError::internal(format!("stream log: stored batch: {e}")))
        };
        if ts == -3 {
            let mut best: Option<(i64, i64)> = None;
            for s in &st.segments {
                for (pos, h) in s.heads().map_err(|e| io_err("read", &e))? {
                    if h.next_offset() <= st.log_start || best.is_some_and(|b| b.1 >= h.max_ts) {
                        continue;
                    }
                    let b = decode(&s.batch_at(pos, &h).map_err(|e| io_err("read", &e))?)?;
                    for r in &b.records {
                        let o = b.base_offset + i64::from(r.offset_delta);
                        let t = if h.attributes & batch::ATTR_LOG_APPEND_TIME != 0 {
                            h.max_ts
                        } else {
                            b.base_timestamp + r.timestamp_delta
                        };
                        if o >= st.log_start && best.is_none_or(|x| t > x.1) {
                            best = Some((o, t));
                        }
                    }
                }
            }
            return Ok(best);
        }
        for s in &st.segments {
            let Some((v, h)) = s.view_at_time(ts).map_err(|e| io_err("read", &e))? else {
                continue;
            };
            let mut pos = v.pos;
            let mut h = h;
            loop {
                let b = decode(&s.batch_at(pos, &h).map_err(|e| io_err("read", &e))?)?;
                for r in &b.records {
                    let o = b.base_offset + i64::from(r.offset_delta);
                    let t = if h.attributes & batch::ATTR_LOG_APPEND_TIME != 0 {
                        h.max_ts
                    } else {
                        b.base_timestamp + r.timestamp_delta
                    };
                    if t >= ts && o >= st.log_start {
                        return Ok(Some((o, t)));
                    }
                }
                pos += h.total as u64;
                let next = s
                    .heads()
                    .map_err(|e| io_err("read", &e))?
                    .into_iter()
                    .find(|(p, _)| *p == pos);
                match next {
                    Some((_, nh)) => h = nh,
                    None => break,
                }
            }
        }
        Ok(None)
    }

    /// The largest epoch at most `epoch` and where its records end.
    #[must_use]
    pub fn epoch_end(&self, key: PartKey, epoch: i32) -> (i32, i64) {
        let current = self.epoch();
        match self.inner.existing(&key) {
            None => (current, 0),
            Some(p) => {
                let st = p.lock();
                let c = st.epoch(current);
                st.epoch_end(epoch, c)
            }
        }
    }

    /// Sets the log start of a partition (Kafka's `DeleteRecords`; -1: the
    /// high watermark). Returns the new log start.
    ///
    /// # Errors
    /// The manifest could not be written.
    pub fn delete_records(&self, key: PartKey, offset: i64) -> Result<i64> {
        let Some(p) = self.inner.existing(&key) else {
            return Ok(0);
        };
        let (start, gone) = {
            let mut st = p.lock();
            let to = if offset < 0 { st.next_offset } else { offset };
            let gone = st.truncate_below(to);
            (st.log_start, gone)
        };
        self.flush()?;
        for g in gone {
            let _ = fs::remove_file(g);
        }
        Ok(start)
    }

    /// Drops a stream: its partitions go, and appends still in the log for
    /// it are refused.
    ///
    /// # Errors
    /// The manifest could not be written.
    pub fn drop_stream(&self, tenant: u64, stream: u64) -> Result<()> {
        let dirs: Vec<PathBuf> = {
            let mut a = lock(&self.inner.apply);
            a.dropped.insert((tenant, stream));
            a.dirty = true;
            let mut w = self
                .inner
                .parts
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let keys: Vec<PartKey> = w
                .keys()
                .filter(|k| k.tenant == tenant && k.stream == stream)
                .copied()
                .collect();
            keys.iter()
                .filter_map(|k| w.remove(k).map(|p| p.lock().dir.clone()))
                .collect()
        };
        lock(&self.inner.policies).remove(&(tenant, stream));
        self.flush()?;
        for d in dirs {
            let _ = fs::remove_dir_all(d);
        }
        Ok(())
    }

    /// Tenants with partitions in the log.
    #[must_use]
    pub fn tenants(&self) -> BTreeSet<u64> {
        self.inner
            .parts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .map(|k| k.tenant)
            .collect()
    }

    /// Partitions of a stream that hold data, with their bounds.
    #[must_use]
    pub fn partitions(&self, tenant: u64, stream: u64) -> Vec<(u32, Bounds)> {
        let keys: Vec<PartKey> = self
            .inner
            .parts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .filter(|k| k.tenant == tenant && k.stream == stream)
            .copied()
            .collect();
        let mut out: Vec<(u32, Bounds)> = keys
            .into_iter()
            .map(|k| (k.partition, self.bounds(k)))
            .collect();
        out.sort_by_key(|x| x.0);
        out
    }

    /// Whether the commit log is durable through `index`, waiting up to
    /// two seconds for it.
    fn log_durable_through(&self, index: u64) -> bool {
        let e = &self.inner.engine;
        let c = e.consensus();
        for _ in 0..200 {
            if c.durable_index(e.notes_unit()) >= index {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// Makes everything applied so far durable in the segment files and the
    /// manifest, then raises the engine's log floor.
    ///
    /// # Errors
    /// A sync or the manifest write failed.
    pub fn flush(&self) -> Result<()> {
        let _one = lock(&self.inner.flushing);
        // What this pass takes from the in-memory state, to give back if a
        // sync or the manifest write fails: the next pass must redo it, and
        // the floor must not move past the last manifest written.
        let mut taken: Vec<(Arc<Part>, Vec<(i64, u64)>, bool)> = Vec::new();
        let (m, files, dirs) = {
            let mut a = lock(&self.inner.apply);
            let parts: Vec<(PartKey, Arc<Part>)> = self
                .inner
                .parts
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .map(|(k, p)| (*k, Arc::clone(p)))
                .collect();
            let any_dirty = a.dirty || parts.iter().any(|(_, p)| p.lock().dirty);
            if !any_dirty {
                let floor = a.applied;
                drop(a);
                // Never above what the log holds durably (see below).
                let e = &self.inner.engine;
                let floor = floor.min(e.consensus().durable_index(e.notes_unit()));
                if self.inner.engine.log_floor(FLOOR).is_none_or(|f| f < floor) {
                    self.inner.engine.set_log_floor(FLOOR, floor)?;
                }
                return Ok(());
            }
            let mut files = Vec::new();
            let mut dirs: BTreeSet<PathBuf> = BTreeSet::new();
            let mut metas = Vec::with_capacity(parts.len());
            for (k, p) in &parts {
                let mut st = p.lock();
                let mut synced = Vec::new();
                for s in &mut st.segments {
                    if s.synced < s.len {
                        files.push(s.handle());
                        synced.push((s.base, s.synced));
                        s.synced = s.len;
                    }
                }
                let new_files = std::mem::take(&mut st.new_files);
                if new_files {
                    dirs.insert(st.dir.clone());
                    if let Some(t) = st.dir.parent() {
                        dirs.insert(t.to_path_buf());
                    }
                    dirs.insert(self.inner.dir.clone());
                }
                if st.dirty || !synced.is_empty() || new_files {
                    taken.push((Arc::clone(p), synced, new_files));
                }
                st.dirty = false;
                metas.push(meta_of(*k, &st));
            }
            a.dirty = false;
            let m = Manifest {
                version: 1,
                floor: a.applied,
                epoch: a.epoch,
                dropped: a.dropped.iter().copied().collect(),
                parts: metas,
            };
            (m, files, dirs)
        };
        // The manifest's floor must be durable in the commit log: with
        // relaxed durability (or a log that syncs later) an entry can be
        // applied before it is durable, and a crash would leave files and a
        // floor above what the log kept. Wait for the log; if it doesn't
        // get there, this pass gives everything back for the next one.
        let durable = if self.log_durable_through(m.floor) {
            Ok(())
        } else {
            Err(SprigError::internal(
                "stream log: the commit log isn't durable through the floor yet",
            ))
        };
        let durable = durable.and_then(|()| {
            for f in files {
                f.sync_data().map_err(|e| io_err("sync segment", &e))?;
            }
            for d in &dirs {
                sync_dir_checked(d).map_err(|e| io_err("sync directory", &e))?;
            }
            write_manifest(&self.inner.dir, &m)
        });
        if let Err(e) = durable {
            lock(&self.inner.apply).dirty = true;
            for (p, synced, new_files) in taken {
                let mut st = p.lock();
                st.dirty = true;
                st.new_files |= new_files;
                for (base, old) in synced {
                    if let Some(s) = st.segments.iter_mut().find(|s| s.base == base) {
                        s.synced = s.synced.min(old);
                    }
                }
            }
            return Err(e);
        }
        self.inner.engine.set_log_floor(FLOOR, m.floor)?;
        Ok(())
    }

    /// One pass of retention, compaction and producer expiry.
    ///
    /// # Errors
    /// The manifest could not be written; a compaction failure is logged
    /// and skipped.
    pub fn maintain(&self) -> Result<()> {
        let now = now_ms();
        let parts: Vec<(PartKey, Arc<Part>)> = self
            .inner
            .parts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, p)| (*k, Arc::clone(p)))
            .collect();
        let mut gone = Vec::new();
        for (k, p) in &parts {
            let policy = self.inner.policy(k.tenant, k.stream);
            if let Some(pol) = policy.filter(|p| p.compact)
                && let Err(e) = compact::compact(p, &pol, now, self.inner.cfg.index_interval)
            {
                tracing::warn!(tenant = k.tenant, stream = k.stream, partition = k.partition, error = %e, "stream compaction failed");
            }
            let mut st = p.lock();
            st.expire_producers(now - self.inner.cfg.producer_expiry_ms);
            let Some(pol) = policy else { continue };
            let mut cut = st.log_start;
            if pol.retention_ms >= 0 {
                for w in st.segments.windows(2) {
                    if w[0].max_ts < now - pol.retention_ms && w[0].max_ts >= 0 {
                        cut = cut.max(w[1].base);
                    } else {
                        break;
                    }
                }
            }
            if pol.retention_bytes >= 0 {
                let mut total = st.bytes();
                for w in st.segments.windows(2) {
                    if total <= u64::try_from(pol.retention_bytes).unwrap_or(0) {
                        break;
                    }
                    total -= w[0].len;
                    cut = cut.max(w[1].base);
                }
            }
            if cut > st.log_start {
                gone.extend(st.truncate_below(cut));
            }
        }
        self.flush()?;
        for g in gone {
            let _ = fs::remove_file(g);
        }
        Ok(())
    }

    /// Starts the flusher and the maintenance pass on the current tokio
    /// runtime; both end when the log is dropped.
    pub fn spawn_background(&self) {
        let w = Arc::downgrade(&self.inner);
        let every = self.inner.cfg.flush_every;
        let maintain = self.inner.cfg.maintain_every;
        tokio::spawn(async move {
            let mut last_maintain = tokio::time::Instant::now();
            loop {
                tokio::time::sleep(every).await;
                let Some(inner) = w.upgrade() else { return };
                let log = SegLog { inner };
                let l = log.clone();
                let due = last_maintain.elapsed() >= maintain;
                let r =
                    tokio::task::spawn_blocking(move || if due { l.maintain() } else { l.flush() })
                        .await;
                if due {
                    last_maintain = tokio::time::Instant::now();
                }
                match r {
                    Ok(Err(e)) => tracing::warn!(error = %e, "stream log flush failed"),
                    Err(e) => tracing::warn!(error = %e, "stream log flush panicked"),
                    Ok(Ok(())) => {}
                }
            }
        });
    }
}

fn meta_of(k: PartKey, st: &PartState) -> PartMeta {
    PartMeta {
        key: k,
        next_offset: st.next_offset,
        log_start: st.log_start,
        cleaned_to: st.cleaned_to,
        epochs: st.epochs.clone(),
        segments: st
            .segments
            .iter()
            .map(|s| SegMeta {
                base: s.base,
                len: s.len,
                created_ms: s.created_ms,
            })
            .collect(),
        producers: st
            .producers
            .iter()
            .map(|(id, p)| ProdMeta {
                id: *id,
                epoch: p.epoch,
                last_ms: p.last_ms,
                batches: p.batches.iter().copied().collect(),
            })
            .collect(),
    }
}

fn read_manifest(dir: &Path) -> Result<Option<Manifest>> {
    match fs::read(dir.join(MANIFEST)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| SprigError::internal(format!("stream log: manifest is corrupt: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read manifest", &e)),
    }
}

fn write_manifest(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    let body = serde_json::to_vec(m)
        .map_err(|e| SprigError::internal(format!("stream log: manifest: {e}")))?;
    {
        let mut f = fs::File::create(&tmp).map_err(|e| io_err("write manifest", &e))?;
        f.write_all(&body)
            .and_then(|()| f.sync_all())
            .map_err(|e| io_err("write manifest", &e))?;
    }
    fs::rename(&tmp, dir.join(MANIFEST)).map_err(|e| io_err("write manifest", &e))?;
    sync_dir(dir);
    Ok(())
}

/// Removes every partition directory (a log without a manifest).
fn clear_parts(dir: &Path) {
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            if e.file_name().to_string_lossy().starts_with('t') && e.path().is_dir() {
                let _ = fs::remove_dir_all(e.path());
            }
        }
    }
}

/// Rebuilds the partitions the manifest lists, cutting each active segment
/// back to its recorded length and removing files it doesn't list.
/// Where catching up must start instead of `floor`, when the commit log no
/// longer holds the entry after it: just below the oldest entry held, or
/// at the last applied index when it holds none. `None`: `floor` is fine.
fn catch_up_start(floor: u64, first_held: Option<u64>, last_applied: u64) -> Option<u64> {
    if floor >= last_applied {
        return None;
    }
    match first_held {
        Some(f) if f > floor + 1 => Some((f - 1).min(last_applied)),
        Some(_) => None,
        None => Some(last_applied),
    }
}

fn restore(inner: &Inner, m: &Manifest) -> Result<()> {
    let mut listed: BTreeSet<PathBuf> = BTreeSet::new();
    let mut parts = HashMap::new();
    for pm in &m.parts {
        let dir = part_dir(&inner.dir, &pm.key);
        listed.insert(dir.clone());
        let mut st = PartState::new(dir.clone());
        st.next_offset = pm.next_offset;
        st.log_start = pm.log_start;
        st.cleaned_to = pm.cleaned_to;
        st.epochs.clone_from(&pm.epochs);
        st.producers = pm
            .producers
            .iter()
            .map(|p| {
                (
                    p.id,
                    Producer {
                        epoch: p.epoch,
                        batches: p.batches.iter().copied().collect(),
                        last_ms: p.last_ms,
                    },
                )
            })
            .collect();
        let names: BTreeSet<String> = pm
            .segments
            .iter()
            .map(|s| segment::file_name(s.base))
            .collect();
        if let Ok(rd) = fs::read_dir(&dir) {
            for e in rd.flatten() {
                if !names.contains(&*e.file_name().to_string_lossy()) {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        let n = pm.segments.len();
        for (i, sm) in pm.segments.iter().enumerate() {
            let path = dir.join(segment::file_name(sm.base));
            let active = i + 1 == n;
            match Segment::open(
                &path,
                sm.base,
                active.then_some(sm.len),
                sm.created_ms,
                inner.cfg.index_interval,
            ) {
                Ok(s) => st.segments.push(s),
                Err(e) if e.kind() == io::ErrorKind::NotFound && !active => {
                    // Retention removed it after the manifest was written.
                    if let Some(next) = pm.segments.get(i + 1) {
                        st.log_start = st.log_start.max(next.base);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir_all(&dir).map_err(|e| io_err("restore", &e))?;
                    st.segments.push(
                        Segment::create(&dir, sm.base, sm.created_ms, inner.cfg.index_interval)
                            .map_err(|e| io_err("restore", &e))?,
                    );
                }
                Err(e) => return Err(io_err("restore", &e)),
            }
        }
        // The active segment ends where the manifest's offsets do.
        if let Some(s) = st.segments.last()
            && s.next != pm.next_offset
            && !s.is_empty()
        {
            return Err(SprigError::internal(format!(
                "stream log: partition {}/{}/{} ends at {} but the manifest says {}",
                pm.key.tenant, pm.key.stream, pm.key.partition, s.next, pm.next_offset
            )));
        }
        st.dirty = false;
        st.new_files = false;
        parts.insert(pm.key, Arc::new(Part { st: Mutex::new(st) }));
    }
    // Partition directories the manifest doesn't know were created after
    // it: the replay creates them again.
    if let Ok(rd) = fs::read_dir(&inner.dir) {
        for t in rd.flatten().filter(|e| e.path().is_dir()) {
            if let Ok(rd2) = fs::read_dir(t.path()) {
                for d in rd2.flatten() {
                    if !listed.contains(&d.path()) {
                        let _ = fs::remove_dir_all(d.path());
                    }
                }
            }
        }
    }
    *inner.parts.write().unwrap_or_else(PoisonError::into_inner) = parts;
    let mut a = lock(&inner.apply);
    a.epoch = m.epoch;
    a.dropped = m.dropped.iter().copied().collect();
    Ok(())
}

#[cfg(test)]
mod tests;
