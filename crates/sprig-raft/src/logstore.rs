//! The node's one log for all its groups.
//!
//! The driver writes once per turn: a [`WriteBatch`] with every group's new
//! entries, hard state and snapshot records, then syncs once if any of them
//! needs it. A write is atomic: after a crash a batch is there whole or not
//! at all, and the batches that survive are a prefix of those written (all
//! synced ones, then perhaps some unsynced ones).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use raft::prelude::{Entry, HardState, Snapshot};
use raft_engine::{Command, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use sprig_engine::log::blobs::NoteBlobs;
use sprig_engine::log::{LogFs, SimDisk, SyncMode};

use crate::GroupId;
use crate::error::RaftError;

/// A snapshot record.
#[derive(Debug, Clone)]
pub enum SnapshotWrite {
    /// A follower installs a leader's snapshot: the group's log is replaced.
    Install(Snapshot),
    /// The state machine's own snapshot at an applied index: entries up to
    /// it are dropped, later ones kept.
    Checkpoint(Snapshot),
}

/// What one group writes in a batch, applied in field order.
#[derive(Debug, Clone, Default)]
pub struct GroupWrite {
    /// The group.
    pub group: GroupId,
    /// The group was removed from this node: its log is deleted and a
    /// tombstone with this index (the removal's) kept.
    pub remove: Option<u64>,
    /// A snapshot record.
    pub snapshot: Option<SnapshotWrite>,
    /// New entries; any held from the first one's index on are replaced.
    pub entries: Vec<Entry>,
    /// The new hard state.
    pub hard_state: Option<HardState>,
}

/// One atomic write.
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    /// Per group, in order.
    pub groups: Vec<GroupWrite>,
}

impl WriteBatch {
    /// Whether there is nothing to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// The group's part of the batch, added if missing.
    pub fn group(&mut self, group: GroupId) -> &mut GroupWrite {
        if let Some(i) = self.groups.iter().position(|g| g.group == group) {
            return &mut self.groups[i];
        }
        self.groups.push(GroupWrite {
            group,
            ..GroupWrite::default()
        });
        let last = self.groups.len() - 1;
        &mut self.groups[last]
    }
}

/// What the log holds for one group after a restart.
#[derive(Debug, Clone, Default)]
pub struct RecoveredGroup {
    /// The group.
    pub group: GroupId,
    /// Its hard state.
    pub hard_state: HardState,
    /// Its latest snapshot record (index 0: none).
    pub snapshot: Snapshot,
    /// Entries after the snapshot.
    pub entries: Vec<Entry>,
    /// Set when the group was removed from this node: the removal's index.
    pub tombstone: Option<u64>,
}

impl RecoveredGroup {
    fn apply(&mut self, w: &GroupWrite) {
        if let Some(t) = w.remove {
            *self = RecoveredGroup {
                group: self.group,
                tombstone: Some(t),
                ..RecoveredGroup::default()
            };
            return;
        }
        self.tombstone = None;
        match &w.snapshot {
            Some(SnapshotWrite::Install(s)) => {
                self.entries.clear();
                self.snapshot = s.clone();
            }
            Some(SnapshotWrite::Checkpoint(s)) => {
                let i = s.get_metadata().index;
                self.entries.retain(|e| e.index > i);
                self.snapshot = s.clone();
            }
            None => {}
        }
        if let Some(first) = w.entries.first() {
            self.entries.retain(|e| e.index < first.index);
            self.entries.extend(w.entries.iter().cloned());
        }
        if let Some(hs) = &w.hard_state {
            self.hard_state = hs.clone();
        }
    }
}

/// The node's log.
pub trait LogStore: Send + Sync {
    /// Writes `batch` atomically; with `sync`, makes it and everything
    /// written before it durable before returning. (One call: raft-engine's
    /// own `sync()` writes an empty batch, which it skips, so it never syncs
    /// earlier writes.)
    ///
    /// # Errors
    /// The write or sync failed. The driver stops: the batch may or may not
    /// be on disk.
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<(), RaftError>;

    /// Writes `batches` in order, syncing once after the last when `sync`
    /// (the log writer thread's queued jobs). On a failure, says how many
    /// were written before it.
    ///
    /// # Errors
    /// As [`LogStore::write`], with the count of batches written.
    fn write_all(&self, batches: &[&WriteBatch], sync: bool) -> Result<(), (usize, RaftError)> {
        let n = batches.len();
        for (i, b) in batches.iter().enumerate() {
            self.write(b, sync && i + 1 == n).map_err(|e| (i, e))?;
        }
        Ok(())
    }

    /// How many syncs the log writer thread may have running at once while
    /// it goes on writing ([`LogStore::write_unsynced`] then
    /// [`LogStore::sync_written`]); 0: each write is synced in the same
    /// call ([`LogStore::write_all`]), one after another.
    fn sync_pipeline(&self) -> usize {
        0
    }

    /// Writes `batches` in order without syncing them. `sync` says a sync
    /// ([`LogStore::sync_written`]) will make them durable before anyone
    /// counts them as persisted (a store may keep synced writes
    /// differently). On a failure, says how many were written before it.
    /// The default writes and syncs ([`LogStore::write_all`]).
    ///
    /// # Errors
    /// As [`LogStore::write_all`].
    fn write_unsynced(
        &self,
        batches: &[&WriteBatch],
        sync: bool,
    ) -> Result<(), (usize, RaftError)> {
        self.write_all(batches, sync)
    }

    /// Makes everything written before the call durable: when it returns,
    /// every write that finished before the call is on the disk. It may run
    /// alongside writes and other syncs. A call with nothing new to sync
    /// may return at once only when a finished sync already covers the last
    /// write; while the sync that covers it is still running (another
    /// caller's), the call waits for it.
    ///
    /// # Errors
    /// The sync failed. The driver stops.
    fn sync_written(&self) -> Result<(), RaftError> {
        Ok(())
    }

    /// Every group the log holds.
    ///
    /// # Errors
    /// The log could not be read.
    fn recover(&self) -> Result<Vec<RecoveredGroup>, RaftError>;

    /// Simulation only: how long the next sync takes.
    fn sync_delay(&self) -> Duration {
        Duration::ZERO
    }

    /// Housekeeping between turns (deleting trimmed files).
    fn maintain(&self) {}

    /// The lowest entry index of `group` the log holds. The default
    /// recovers the whole log (the simulated disk).
    fn first_index(&self, group: GroupId) -> Option<u64> {
        self.recover()
            .ok()?
            .into_iter()
            .find(|g| g.group == group)
            .and_then(|g| g.entries.first().map(|e| e.index))
    }

    /// The entries `from..to` of `group` the log holds, in order (fewer
    /// when trimmed or not yet written). Reads what was written; the
    /// caller asks only for committed indexes. The default recovers the
    /// whole log (the simulated disk).
    ///
    /// # Errors
    /// The log could not be read.
    fn entries(&self, group: GroupId, from: u64, to: u64) -> Result<Vec<Entry>, RaftError> {
        Ok(self
            .recover()?
            .into_iter()
            .find(|g| g.group == group)
            .map(|g| {
                g.entries
                    .into_iter()
                    .filter(|e| e.index >= from && e.index < to)
                    .collect()
            })
            .unwrap_or_default())
    }
}

#[derive(Default)]
struct MemDisk {
    base: BTreeMap<GroupId, RecoveredGroup>,
    /// Batches since `base`, each with whether it was synced.
    records: Vec<(WriteBatch, bool)>,
    /// Batches folded into `base`: `records[i]` is batch `folded + i`.
    folded: u64,
}

impl MemDisk {
    fn fold_synced(&mut self) {
        let n = self.records.iter().take_while(|(_, s)| *s).count();
        if n < 32 {
            return;
        }
        let folded: Vec<(WriteBatch, bool)> = self.records.drain(..n).collect();
        self.folded += n as u64;
        for (b, _) in &folded {
            apply_batch(&mut self.base, b);
        }
    }

    /// Batches written so far.
    fn written(&self) -> u64 {
        self.folded + self.records.len() as u64
    }

    /// Marks the first `upto` batches synced.
    fn sync_to(&mut self, upto: u64) {
        let n = usize::try_from(upto.saturating_sub(self.folded)).unwrap_or(usize::MAX);
        for r in self.records.iter_mut().take(n) {
            r.1 = true;
        }
        self.fold_synced();
    }
}

fn apply_batch(state: &mut BTreeMap<GroupId, RecoveredGroup>, b: &WriteBatch) {
    for w in &b.groups {
        state
            .entry(w.group)
            .or_insert_with(|| RecoveredGroup {
                group: w.group,
                ..RecoveredGroup::default()
            })
            .apply(w);
    }
}

type DelayFn = Box<dyn FnMut() -> Duration + Send>;

/// A simulated disk: batches are kept in memory with whether they were
/// synced, and [`MemLogStore::crash`] keeps every synced batch plus a
/// seed-chosen prefix of the unsynced ones (the next one is torn and lost).
#[derive(Default)]
pub struct MemLogStore {
    disk: Mutex<MemDisk>,
    delay: Mutex<Option<DelayFn>>,
    /// [`LogStore::sync_pipeline`].
    pipeline: AtomicUsize,
    /// Syncs running now, and the most ever at once.
    syncing: Mutex<(usize, usize)>,
}

impl std::fmt::Debug for MemLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemLogStore").finish_non_exhaustive()
    }
}

/// What a simulated crash lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrashLoss {
    /// Unsynced batches that were written.
    pub unsynced: usize,
    /// Of those, the ones that survived.
    pub kept: usize,
}

impl MemLogStore {
    /// An empty disk.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Sets how long each sync takes (slow disks).
    pub fn set_sync_delay(&self, f: impl FnMut() -> Duration + Send + 'static) {
        *self.delay.lock().unwrap_or_else(PoisonError::into_inner) = Some(Box::new(f));
    }

    /// A power loss: every synced batch stays, the unsynced ones keep a
    /// seed-chosen prefix, and the batch after that prefix is torn.
    pub fn crash(&self, seed: u64) -> CrashLoss {
        let mut d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        let synced = d.records.iter().take_while(|(_, s)| *s).count();
        let unsynced = d.records.len() - synced;
        let mut x = seed | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let kept = usize::try_from(x % (unsynced as u64 + 1)).unwrap_or(0);
        d.records.truncate(synced + kept);
        for r in &mut d.records {
            r.1 = true;
        }
        CrashLoss { unsynced, kept }
    }

    /// Batches written and not yet synced.
    #[must_use]
    pub fn unsynced(&self) -> usize {
        let d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        d.records.iter().filter(|(_, s)| !*s).count()
    }

    /// Lets the log writer thread run up to `n` syncs at once while it
    /// writes on ([`LogStore::sync_pipeline`]); each takes the sync delay.
    pub fn set_sync_pipeline(&self, n: usize) {
        self.pipeline.store(n, Ordering::Release);
    }

    /// The most syncs that ever ran at once.
    #[must_use]
    pub fn most_syncs_at_once(&self) -> usize {
        self.syncing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
    }
}

impl LogStore for MemLogStore {
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<(), RaftError> {
        let mut d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        d.records.push((batch.clone(), false));
        if sync {
            let n = d.written();
            d.sync_to(n);
        }
        Ok(())
    }

    fn sync_pipeline(&self) -> usize {
        self.pipeline.load(Ordering::Acquire)
    }

    fn write_unsynced(
        &self,
        batches: &[&WriteBatch],
        _sync: bool,
    ) -> Result<(), (usize, RaftError)> {
        let mut d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        for b in batches {
            d.records.push(((*b).clone(), false));
        }
        Ok(())
    }

    fn sync_written(&self) -> Result<(), RaftError> {
        // Covers what was written before it began, not what is written
        // while it runs.
        let upto = self
            .disk
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .written();
        {
            let mut s = self.syncing.lock().unwrap_or_else(PoisonError::into_inner);
            s.0 += 1;
            s.1 = s.1.max(s.0);
        }
        let d = self.sync_delay();
        if !d.is_zero() {
            std::thread::sleep(d);
        }
        self.syncing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0 -= 1;
        self.disk
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .sync_to(upto);
        Ok(())
    }

    fn recover(&self) -> Result<Vec<RecoveredGroup>, RaftError> {
        let d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = d.base.clone();
        for (b, _) in &d.records {
            apply_batch(&mut state, b);
        }
        Ok(state.into_values().collect())
    }

    fn sync_delay(&self) -> Duration {
        self.delay
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .map_or(Duration::ZERO, |f| f())
    }
}

/// raft-engine's view of raft-rs entries.
struct EntryExt;

impl MessageExt for EntryExt {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

const KEY_HS: &[u8] = b"hs";
const KEY_SNAP: &[u8] = b"snap";
const KEY_TOMB: &[u8] = b"tomb";

fn engine_err(what: &str, e: &raft_engine::Error) -> RaftError {
    RaftError::Storage(format!("{what}: {e}"))
}

/// The node's log on disk: raft-engine, one raft-engine group per Raft
/// group, entries as raft-rs writes them, the hard state, snapshot record
/// and tombstone as keys of the group. With note blobs
/// ([`EngineLogStore::set_note_blobs`]) the large notes of synced entries
/// (stream appends) are written once into blob files and the log keeps
/// references; every read puts them back.
pub struct EngineLogStore {
    engine: raft_engine::Engine<LogFs>,
    fs: Arc<LogFs>,
    dir: PathBuf,
    blobs: OnceLock<Arc<NoteBlobs>>,
    /// [`LogStore::sync_pipeline`].
    pipeline: AtomicUsize,
}

/// Syncs an [`EngineLogStore`] lets run at once unless told otherwise
/// ([`EngineLogStore::set_sync_pipeline`]).
pub const SYNC_PIPELINE: usize = 8;

impl std::fmt::Debug for EngineLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineLogStore")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl EngineLogStore {
    /// Opens (or creates) the log in `dir`. `sim` simulates crashes that
    /// lose unsynced bytes (tests).
    ///
    /// # Errors
    /// raft-engine failed to open.
    pub fn open(
        dir: impl AsRef<Path>,
        sync: SyncMode,
        sim: Option<Arc<SimDisk>>,
    ) -> Result<Arc<Self>, RaftError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .map_err(|e| RaftError::Storage(format!("create {}: {e}", dir.display())))?;
        let cfg = raft_engine::Config {
            dir: dir.to_string_lossy().into_owned(),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            batch_compression_threshold: ReadableSize::kb(4),
            target_file_size: ReadableSize::mb(32),
            purge_threshold: ReadableSize::mb(64),
            enable_log_recycle: false,
            ..raft_engine::Config::default()
        };
        let fs = Arc::new(LogFs::new(sync, sim));
        let opened = Arc::clone(&fs);
        // Opening reads the files; raft-engine's per-thread read cache must
        // start empty (several logs can live in one process).
        let engine = on_fresh_thread(move || {
            raft_engine::Engine::open_with_file_system(cfg, opened)
                .map_err(|e| engine_err("open", &e))
        })?;
        Ok(Arc::new(Self {
            engine,
            fs,
            dir,
            blobs: OnceLock::new(),
            pipeline: AtomicUsize::new(SYNC_PIPELINE),
        }))
    }

    /// How many syncs the log writer thread may run at once while it goes
    /// on writing ([`LogStore::sync_pipeline`]); 0 writes and syncs in one
    /// call, one after another. Set it before the node starts.
    pub fn set_sync_pipeline(&self, n: usize) {
        self.pipeline.store(n, Ordering::Release);
    }

    /// Keeps large notes in `blobs` (see the type's docs). Set it before the
    /// node opens: its recovery reads the log, and references must be put
    /// back. Only the first call counts.
    pub fn set_note_blobs(&self, blobs: Arc<NoteBlobs>) {
        let _ = self.blobs.set(blobs);
    }

    /// The note blobs, if set.
    #[must_use]
    pub fn note_blobs(&self) -> Option<&Arc<NoteBlobs>> {
        self.blobs.get()
    }

    /// One batch as raft-engine writes it, taking notes out into `take`.
    fn log_batch(batch: &WriteBatch, take: Option<&NoteBlobs>) -> Result<LogBatch, RaftError> {
        let mut lb = LogBatch::with_capacity(batch.groups.len());
        for w in &batch.groups {
            let g = w.group;
            let enc = |e: raft_engine::Error| engine_err("encode", &e);
            if let Some(t) = w.remove {
                lb.add_command(g, Command::Clean);
                lb.put(g, KEY_TOMB.to_vec(), t.to_le_bytes().to_vec())
                    .map_err(enc)?;
                continue;
            }
            match &w.snapshot {
                Some(SnapshotWrite::Install(s)) => {
                    lb.add_command(g, Command::Clean);
                    lb.put_message(g, KEY_SNAP.to_vec(), s).map_err(enc)?;
                }
                Some(SnapshotWrite::Checkpoint(s)) => {
                    lb.put_message(g, KEY_SNAP.to_vec(), s).map_err(enc)?;
                    lb.add_command(
                        g,
                        Command::Compact {
                            index: s.get_metadata().index + 1,
                        },
                    );
                }
                None => {}
            }
            if !w.entries.is_empty() {
                let entries = match take {
                    Some(b) => take_out(b, g, &w.entries)?,
                    None => Cow::Borrowed(&w.entries[..]),
                };
                lb.add_entries::<EntryExt>(g, &entries).map_err(enc)?;
            }
            if let Some(hs) = &w.hard_state {
                lb.put_message(g, KEY_HS.to_vec(), hs).map_err(enc)?;
            }
        }
        Ok(lb)
    }

    /// [`EngineLogStore::put_back`] at recovery: entries whose notes were
    /// in a blob file's unsynced tail when the node stopped (the log's last
    /// write, never reported persisted) are cut off with everything after
    /// them. Returns the index of the first entry cut.
    fn put_back_recovered(
        &self,
        group: GroupId,
        entries: &mut Vec<Entry>,
    ) -> Result<Option<u64>, RaftError> {
        let Some(b) = self.blobs.get() else {
            return Ok(None);
        };
        for i in 0..entries.len() {
            let e = &mut entries[i];
            match b.put_back(group, e.index, e.term, &e.data) {
                Ok(Some(d)) => e.data = d.into(),
                Ok(None) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    let cut = e.index;
                    tracing::warn!(group, index = cut, dropped = entries.len() - i, error = %err, "raft log: entries of the last write lost their notes in a crash; cut off (they were never reported persisted)");
                    entries.truncate(i);
                    return Ok(Some(cut));
                }
                Err(err) => return Err(RaftError::Storage(format!("note blobs: {err}"))),
            }
        }
        Ok(None)
    }

    /// Puts back the notes `entries` (read from the log) refer to.
    fn put_back(&self, group: GroupId, entries: &mut [Entry]) -> Result<(), RaftError> {
        let Some(b) = self.blobs.get() else {
            return Ok(());
        };
        for e in entries {
            if let Some(d) = b
                .put_back(group, e.index, e.term, &e.data)
                .map_err(|err| RaftError::Storage(format!("note blobs: {err}")))?
            {
                e.data = d.into();
            }
        }
        Ok(())
    }

    /// Bytes of log files on disk.
    #[must_use]
    pub fn disk_bytes(&self) -> u64 {
        self.engine.get_used_size() as u64
    }
}

/// Runs `f` on a new thread: raft-engine caches the last block it read in a
/// per-thread slot keyed by file position only, so reads of two logs on one
/// thread could mix.
fn on_fresh_thread<T: Send>(
    f: impl FnOnce() -> Result<T, RaftError> + Send,
) -> Result<T, RaftError> {
    std::thread::scope(|s| {
        std::thread::Builder::new()
            .name("sprig-raft-read".into())
            .spawn_scoped(s, f)
            .map_err(|e| RaftError::Storage(format!("spawn reader: {e}")))?
            .join()
            .unwrap_or_else(|p| std::panic::resume_unwind(p))
    })
}

impl EngineLogStore {
    /// Writes raft-engine batches in order, the sync with the last one that
    /// writes anything (raft-engine skips an empty one, sync and all).
    fn write_lbs(&self, lbs: Vec<LogBatch>, sync: bool) -> Result<(), (usize, RaftError)> {
        let last = lbs.iter().rposition(|lb| !lb.is_empty());
        for (i, mut lb) in lbs.into_iter().enumerate() {
            if lb.is_empty() {
                continue;
            }
            self.engine
                .write(&mut lb, sync && Some(i) == last)
                .map_err(|e| (i, engine_err("write", &e)))?;
        }
        Ok(())
    }
}

/// `entries` with the notes `blobs` picks taken out (borrowed when none
/// is).
fn take_out<'a>(
    blobs: &NoteBlobs,
    group: GroupId,
    entries: &'a [Entry],
) -> Result<Cow<'a, [Entry]>, RaftError> {
    let mut out: Option<Vec<Entry>> = None;
    for (i, e) in entries.iter().enumerate() {
        let taken = blobs
            .take_out(group, e.index, e.term, &e.data)
            .map_err(|err| RaftError::Storage(format!("note blobs: {err}")))?;
        if let Some(d) = taken {
            let v = out.get_or_insert_with(|| entries[..i].to_vec());
            let mut e = e.clone();
            e.data = d.into();
            v.push(e);
        } else if let Some(v) = out.as_mut() {
            v.push(e.clone());
        }
    }
    Ok(out.map_or(Cow::Borrowed(entries), Cow::Owned))
}

impl LogStore for EngineLogStore {
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<(), RaftError> {
        self.write_all(&[batch], sync).map_err(|(_, e)| e)
    }

    fn write_all(&self, batches: &[&WriteBatch], sync: bool) -> Result<(), (usize, RaftError)> {
        // Notes leave only synced writes. Their blobs are written before
        // the log and synced alongside its write and sync; the write counts
        // as persisted only once both are done (a crash between them cuts
        // the log there on recovery: see `NoteBlobs`).
        let take = self
            .blobs
            .get()
            .filter(|b| sync && b.takes())
            .map(Arc::as_ref);
        let mut lbs = Vec::with_capacity(batches.len());
        for b in batches {
            lbs.push(Self::log_batch(b, take).map_err(|e| (0, e))?);
        }
        let blob_err =
            |what: &str, e: &std::io::Error| RaftError::Storage(format!("note blobs: {what}: {e}"));
        let Some(b) = take else {
            return self.write_lbs(lbs, sync);
        };
        b.write_out().map_err(|e| (0, blob_err("write", &e)))?;
        std::thread::scope(|s| {
            let synced = std::thread::Builder::new()
                .name("sprig-raft-blob-sync".into())
                .spawn_scoped(s, || b.sync());
            let wrote = self.write_lbs(lbs, sync);
            let synced = match synced {
                Ok(t) => t.join().unwrap_or_else(|p| std::panic::resume_unwind(p)),
                // No thread: sync here.
                Err(_) => b.sync(),
            };
            wrote?;
            synced.map_err(|e| (0, blob_err("sync", &e)))
        })
    }

    fn sync_pipeline(&self) -> usize {
        self.pipeline.load(Ordering::Acquire)
    }

    fn write_unsynced(
        &self,
        batches: &[&WriteBatch],
        sync: bool,
    ) -> Result<(), (usize, RaftError)> {
        // As `write_all`, without the syncs: the blobs go out before the
        // log's write that refers to them, and one later sync covers both.
        let take = self
            .blobs
            .get()
            .filter(|b| sync && b.takes())
            .map(Arc::as_ref);
        let mut lbs = Vec::with_capacity(batches.len());
        for b in batches {
            lbs.push(Self::log_batch(b, take).map_err(|e| (0, e))?);
        }
        if let Some(b) = take {
            b.write_out()
                .map_err(|e| (0, RaftError::Storage(format!("note blobs: write: {e}"))))?;
        }
        self.write_lbs(lbs, false)
    }

    fn sync_written(&self) -> Result<(), RaftError> {
        let log = || {
            self.fs
                .sync_written()
                .map_err(|e| RaftError::Storage(format!("sync: {e}")))
        };
        let Some(b) = self.blobs.get() else {
            return log();
        };
        let blob = || {
            b.sync()
                .map_err(|e| RaftError::Storage(format!("note blobs: sync: {e}")))
        };
        // Both files' syncs at once: on a journaling file system both
        // files' size changes then ride one journal commit. Each returns
        // only once its file's writes made before this call are durable
        // (a sync still running for them is waited for).
        std::thread::scope(|s| {
            let synced = std::thread::Builder::new()
                .name("sprig-raft-blob-sync".into())
                .spawn_scoped(s, blob);
            let logged = log();
            let synced = match synced {
                Ok(t) => t.join().unwrap_or_else(|p| std::panic::resume_unwind(p)),
                Err(_) => blob(),
            };
            logged.and(synced)
        })
    }

    fn first_index(&self, group: GroupId) -> Option<u64> {
        self.engine.first_index(group)
    }

    fn entries(&self, group: GroupId, from: u64, to: u64) -> Result<Vec<Entry>, RaftError> {
        on_fresh_thread(|| {
            let Some(first) = self.engine.first_index(group) else {
                return Ok(Vec::new());
            };
            let last = self.engine.last_index(group).unwrap_or(0);
            let (lo, hi) = (from.max(first), to.min(last + 1));
            let mut out = Vec::new();
            if lo < hi {
                self.engine
                    .fetch_entries_to::<EntryExt>(group, lo, hi, None, &mut out)
                    .map_err(|e| engine_err("read", &e))?;
            }
            self.put_back(group, &mut out)?;
            Ok(out)
        })
    }

    fn recover(&self) -> Result<Vec<RecoveredGroup>, RaftError> {
        on_fresh_thread(|| {
            let mut out = Vec::new();
            let mut groups = self.engine.raft_groups();
            groups.sort_unstable();
            for g in groups {
                let read = |e: raft_engine::Error| engine_err("read", &e);
                let tomb = self.engine.get(g, KEY_TOMB);
                if let Some(t) = tomb {
                    let mut b = [0u8; 8];
                    let n = t.len().min(8);
                    b[..n].copy_from_slice(&t[..n]);
                    out.push(RecoveredGroup {
                        group: g,
                        tombstone: Some(u64::from_le_bytes(b)),
                        ..RecoveredGroup::default()
                    });
                    continue;
                }
                let hard_state = self
                    .engine
                    .get_message::<HardState>(g, KEY_HS)
                    .map_err(read)?
                    .unwrap_or_default();
                let snapshot = self
                    .engine
                    .get_message::<Snapshot>(g, KEY_SNAP)
                    .map_err(read)?
                    .unwrap_or_default();
                let mut entries = Vec::new();
                if let (Some(first), Some(last)) =
                    (self.engine.first_index(g), self.engine.last_index(g))
                {
                    self.engine
                        .fetch_entries_to::<EntryExt>(g, first, last + 1, None, &mut entries)
                        .map_err(read)?;
                }
                let si = snapshot.get_metadata().index;
                entries.retain(|e| e.index > si);
                let mut hard_state = hard_state;
                if let Some(cut) = self.put_back_recovered(g, &mut entries)? {
                    // Never reported persisted: raft-rs re-fetches them.
                    // The commit index learned with them may not pass the
                    // log's end.
                    hard_state.commit = hard_state.commit.min(cut - 1);
                }
                out.push(RecoveredGroup {
                    group: g,
                    hard_state,
                    snapshot,
                    entries,
                    tombstone: None,
                });
            }
            Ok(out)
        })
    }

    fn maintain(&self) {
        let _ = on_fresh_thread(|| {
            self.engine
                .purge_expired_files()
                .map(drop)
                .map_err(|e| engine_err("purge", &e))
        });
        // With a holder (the stream log) registered, its own maintenance
        // removes blob files: it knows which ones its segments refer to.
        if let Some(b) = self.blobs.get()
            && !b.has_holder()
        {
            b.collect(&|g| self.engine.first_index(g));
        }
    }
}

/// The encoded size of a message, for budgets and tests.
#[must_use]
pub fn encoded_len(m: &impl protobuf::Message) -> usize {
    m.compute_size() as usize
}

#[cfg(test)]
mod tests;
