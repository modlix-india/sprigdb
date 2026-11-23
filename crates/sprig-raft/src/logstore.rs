//! The node's one log for all its groups.
//!
//! The driver writes once per turn: a [`WriteBatch`] with every group's new
//! entries, hard state and snapshot records, then syncs once if any of them
//! needs it. A write is atomic: after a crash a batch is there whole or not
//! at all, and the batches that survive are a prefix of those written (all
//! synced ones, then perhaps some unsynced ones).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use raft::prelude::{Entry, HardState, Snapshot};
use raft_engine::{Command, LogBatch, MessageExt, ReadableSize, RecoveryMode};
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
}

impl MemDisk {
    fn fold_synced(&mut self) {
        let n = self.records.iter().take_while(|(_, s)| *s).count();
        if n < 32 {
            return;
        }
        let folded: Vec<(WriteBatch, bool)> = self.records.drain(..n).collect();
        for (b, _) in &folded {
            apply_batch(&mut self.base, b);
        }
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
}

impl LogStore for MemLogStore {
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<(), RaftError> {
        let mut d = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        d.records.push((batch.clone(), false));
        if sync {
            for r in &mut d.records {
                r.1 = true;
            }
            d.fold_synced();
        }
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
/// and tombstone as keys of the group.
pub struct EngineLogStore {
    engine: raft_engine::Engine<LogFs>,
    dir: PathBuf,
}

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
        // Opening reads the files; raft-engine's per-thread read cache must
        // start empty (several logs can live in one process).
        let engine = on_fresh_thread(move || {
            raft_engine::Engine::open_with_file_system(cfg, fs).map_err(|e| engine_err("open", &e))
        })?;
        Ok(Arc::new(Self { engine, dir }))
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

impl LogStore for EngineLogStore {
    fn write(&self, batch: &WriteBatch, sync: bool) -> Result<(), RaftError> {
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
                lb.add_entries::<EntryExt>(g, &w.entries).map_err(enc)?;
            }
            if let Some(hs) = &w.hard_state {
                lb.put_message(g, KEY_HS.to_vec(), hs).map_err(enc)?;
            }
        }
        if lb.is_empty() {
            return Ok(());
        }
        self.engine
            .write(&mut lb, sync)
            .map(drop)
            .map_err(|e| engine_err("write", &e))
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
    }
}

/// The encoded size of a message, for budgets and tests.
#[must_use]
pub fn encoded_len(m: &impl protobuf::Message) -> usize {
    m.compute_size() as usize
}

#[cfg(test)]
mod tests;
