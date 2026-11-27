//! raft-rs's view of one group's log: an in-memory copy of what the log
//! store holds for the group (entries since the last snapshot point, the
//! hard state, and the snapshot point itself).

use std::cell::Cell;

use raft::prelude::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};

/// One group's log as raft-rs reads it. The driver keeps it equal to what it
/// has written to the log store.
#[derive(Default)]
pub(crate) struct GroupStorage {
    hs: HardState,
    /// The conf state at the snapshot point: what raft-rs starts from.
    cs: ConfState,
    /// The snapshot point: entries at or below it are gone.
    snap: SnapshotMetadata,
    /// Entries from `snap.index + 1`.
    entries: Vec<Entry>,
    /// The newest snapshot of the state machine, for followers that need
    /// one.
    cached: Option<Snapshot>,
    /// raft-rs asked for a snapshot that isn't there yet.
    wanted: Cell<bool>,
}

impl GroupStorage {
    /// A log rebuilt from what the log store recovered.
    pub(crate) fn recovered(hs: HardState, snap: &Snapshot, entries: Vec<Entry>) -> Self {
        let meta = snap.get_metadata().clone();
        let entries = entries
            .into_iter()
            .filter(|e| e.index > meta.index)
            .collect();
        Self {
            hs,
            cs: meta.get_conf_state().clone(),
            snap: meta,
            entries,
            cached: None,
            wanted: Cell::new(false),
        }
    }

    pub(crate) fn hard_state(&self) -> &HardState {
        &self.hs
    }

    pub(crate) fn set_hard_state(&mut self, hs: HardState) {
        self.hs = hs;
    }

    pub(crate) fn snapshot_index(&self) -> u64 {
        self.snap.index
    }

    pub(crate) fn last(&self) -> u64 {
        self.snap.index + self.entries.len() as u64
    }

    /// The term of the entry at `index`, if the log holds it.
    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snap.index {
            return Some(self.snap.term);
        }
        if index < self.snap.index {
            return None;
        }
        let i = usize::try_from(index - self.snap.index - 1).ok()?;
        self.entries.get(i).map(|e| e.term)
    }

    /// Appends `ents`, replacing every entry from the first one's index on.
    pub(crate) fn append(&mut self, ents: &[Entry]) {
        let Some(first) = ents.first() else {
            return;
        };
        let keep = usize::try_from(first.index.saturating_sub(self.snap.index + 1)).unwrap_or(0);
        self.entries.truncate(keep.min(self.entries.len()));
        self.entries.extend_from_slice(ents);
    }

    /// Replaces the whole log with `snap` (a follower installing one).
    pub(crate) fn install(&mut self, snap: &Snapshot) {
        let meta = snap.get_metadata();
        self.entries.clear();
        self.snap = meta.clone();
        self.cs = meta.get_conf_state().clone();
        if self.hs.commit < meta.index {
            self.hs.commit = meta.index;
        }
        if self.hs.term < meta.term {
            self.hs.term = meta.term;
        }
        self.cached = None;
    }

    /// Records `snap` (taken at an applied index) as the snapshot point and
    /// drops the entries it covers.
    pub(crate) fn checkpoint(&mut self, snap: Snapshot) {
        let meta = snap.get_metadata();
        if meta.index <= self.snap.index {
            return;
        }
        let drop = usize::try_from(meta.index - self.snap.index).unwrap_or(usize::MAX);
        self.entries.drain(..drop.min(self.entries.len()));
        self.snap = meta.clone();
        self.cs = meta.get_conf_state().clone();
        self.cached = Some(snap);
    }

    /// Moves the snapshot point to `snap` (metadata only: a durable state
    /// machine's own store holds the state) and drops the entries it covers.
    pub(crate) fn trim(&mut self, snap: &Snapshot) {
        let cached = self.cached.take();
        self.checkpoint(snap.clone());
        // A metadata-only record is no snapshot to send: keep the older
        // one only if it still covers the log, else make a new one on
        // demand.
        self.cached = cached.filter(|c| c.get_metadata().index >= self.snap.index);
    }

    /// Offers `snap` to followers that need one, without trimming the log.
    /// The size of the snapshot held to send, in bytes (0: none).
    pub(crate) fn cached_len(&self) -> usize {
        self.cached.as_ref().map_or(0, |s| s.data.len())
    }

    pub(crate) fn set_cached(&mut self, snap: Snapshot) {
        self.cached = Some(snap);
    }

    /// Whether raft-rs asked for a snapshot since the last call.
    pub(crate) fn take_wanted(&self) -> bool {
        self.wanted.replace(false)
    }

    /// Bytes held in memory, roughly (for the idle-cost figures).
    pub(crate) fn mem_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.data.len() + 64)
            .sum::<usize>()
            + self.cached.as_ref().map_or(0, |s| s.data.len())
    }
}

fn store_err(e: StorageError) -> raft::Error {
    raft::Error::Store(e)
}

impl Storage for GroupStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hs.clone(),
            conf_state: self.cs.clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low <= self.snap.index {
            return Err(store_err(StorageError::Compacted));
        }
        let high = high.min(self.last() + 1);
        if low >= high {
            return Ok(Vec::new());
        }
        let off = self.snap.index + 1;
        let lo = usize::try_from(low - off).unwrap_or(usize::MAX);
        let hi = usize::try_from(high - off).unwrap_or(usize::MAX);
        let mut ents = self.entries[lo..hi].to_vec();
        raft::util::limit_size(&mut ents, max_size.into());
        Ok(ents)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx < self.snap.index {
            return Err(store_err(StorageError::Compacted));
        }
        self.term_at(idx)
            .ok_or_else(|| store_err(StorageError::Unavailable))
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snap.index + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last())
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        if let Some(s) = &self.cached {
            let m = s.get_metadata();
            let cs = m.get_conf_state();
            // A replica refuses a snapshot whose configuration doesn't
            // include it (one taken before it was added).
            let has_to = cs
                .voters
                .iter()
                .chain(&cs.learners)
                .chain(&cs.voters_outgoing)
                .chain(&cs.learners_next)
                .any(|v| *v == to);
            if m.index >= request_index && m.index >= self.snap.index && has_to {
                return Ok(s.clone());
            }
        }
        self.wanted.set(true);
        Err(store_err(StorageError::SnapshotTemporarilyUnavailable))
    }
}
