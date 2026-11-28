//! Snapshots that travel as files: built off the driver thread by a
//! [`crate::SnapshotExporter`], sent in chunks ahead of their `MsgSnapshot`,
//! paced per node, written to a file by the follower and restored from it.
//!
//! The snapshot raft-rs sees carries only a token (the file's id and
//! length) in its data; the bytes go beside it:
//!
//! 1. The leader's builder thread fixes the state's point under the group's
//!    lock (the group's applied index, term and membership), writes the
//!    state to `out-<group>-<id>.snap`, and offers the token snapshot to
//!    raft-rs as the group's cached snapshot.
//! 2. When raft-rs sends it, the driver hands the `MsgSnapshot` to the
//!    node's sender thread instead: it sends the file in 1 MiB chunks
//!    ([`crate::wire`] item `SnapChunk`), at most
//!    [`crate::RaftConfig::snapshot_bytes_per_second`] over all transfers,
//!    then the message itself on the same connection.
//! 3. The follower writes the chunks to `in-<group>-<id>.part`. A
//!    `MsgSnapshot` whose file isn't complete here is dropped (the leader
//!    sends again after its retry window); otherwise raft-rs takes it, and
//!    the driver restores the state from the file (durably) before the
//!    snapshot's record is written, then deletes the file.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use raft::prelude::Message;

use crate::{GroupId, NodeId};

/// The bytes sent per chunk.
pub(crate) const CHUNK: usize = 1 << 20;

const MAGIC: &[u8; 8] = b"SPRGSNAP";

/// The data of a snapshot whose bytes are file `id`, `len` bytes long.
pub(crate) fn token(id: u64, len: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(24);
    b.extend_from_slice(MAGIC);
    b.extend_from_slice(&id.to_le_bytes());
    b.extend_from_slice(&len.to_le_bytes());
    b
}

/// The file id and length a snapshot's data names, if it is a token.
pub(crate) fn parse_token(data: &[u8]) -> Option<(u64, u64)> {
    if data.len() != 24 || &data[..8] != MAGIC {
        return None;
    }
    let id = u64::from_le_bytes(data[8..16].try_into().ok()?);
    let len = u64::from_le_bytes(data[16..24].try_into().ok()?);
    Some((id, len))
}

/// How many bytes a snapshot with `data` carries (its file's, for a token).
pub(crate) fn size_of(data: &[u8]) -> usize {
    parse_token(data).map_or(data.len(), |(_, n)| {
        usize::try_from(n).unwrap_or(usize::MAX)
    })
}

/// A built snapshot's file; deleted when the last transfer reading it ends
/// and the group no longer offers it.
#[derive(Debug)]
pub(crate) struct SnapFile {
    pub(crate) path: PathBuf,
    pub(crate) len: u64,
}

impl Drop for SnapFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A snapshot being received for one group.
#[derive(Debug)]
pub(crate) struct Incoming {
    pub(crate) id: u64,
    pub(crate) from: NodeId,
    pub(crate) total: u64,
    pub(crate) written: u64,
    pub(crate) file: Option<File>,
    pub(crate) path: PathBuf,
    /// A chunk went missing or a write failed: the rest is ignored until
    /// the leader starts the transfer again.
    pub(crate) broken: bool,
}

impl Incoming {
    pub(crate) fn complete(&self, id: u64, len: u64) -> bool {
        !self.broken && self.id == id && self.total == len && self.written == len
    }
}

impl Drop for Incoming {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

/// One snapshot to send: the file's chunks, then `msg`.
pub(crate) struct Transfer {
    pub(crate) to: NodeId,
    pub(crate) group: GroupId,
    pub(crate) id: u64,
    /// The group's applied index when the message was made (the frame item
    /// carries it).
    pub(crate) applied: u64,
    pub(crate) msg: Message,
    pub(crate) file: Arc<SnapFile>,
}

/// Transfers waiting, and the (follower, group) pairs queued or being sent.
pub(crate) type Queue = (VecDeque<Transfer>, HashSet<(NodeId, GroupId)>);

/// A node's snapshot files and transfers.
#[derive(Default)]
pub(crate) struct Snaps {
    pub(crate) dir: PathBuf,
    /// Built here, by group: the file its cached snapshot names.
    pub(crate) outgoing: Mutex<HashMap<GroupId, (u64, Arc<SnapFile>)>>,
    /// Groups with a build running.
    pub(crate) building: Mutex<HashSet<GroupId>>,
    /// Being received (or received and not restored yet), by group and
    /// snapshot id: a complete file stays until it is restored or a newer
    /// one of its group is complete, whatever transfer starts meanwhile.
    pub(crate) incoming: Mutex<HashMap<(GroupId, u64), Incoming>>,
    /// Transfers waiting for the sender thread, and the (follower, group)
    /// pairs queued or being sent (the leader doesn't count those as lost).
    pub(crate) queue: Mutex<Queue>,
    pub(crate) cv: Condvar,
    /// The sender thread runs.
    pub(crate) sender: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Snaps {
    /// Snapshot files in `dir` (created; files a previous run left are
    /// deleted).
    pub(crate) fn open(dir: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&dir);
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for e in rd.flatten() {
                let n = e.file_name();
                let n = n.to_string_lossy();
                if n.starts_with("out-") || n.starts_with("in-") {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        Self {
            dir,
            ..Self::default()
        }
    }

    pub(crate) fn out_path(&self, group: GroupId, id: u64) -> PathBuf {
        self.dir.join(format!("out-{group}-{id}.snap"))
    }

    pub(crate) fn in_path(&self, group: GroupId, from: NodeId, id: u64) -> PathBuf {
        self.dir.join(format!("in-{group}-{from}-{id}.part"))
    }

    /// Takes chunk `data` of snapshot `id` of `group` from `from`.
    /// Returns the bytes written.
    pub(crate) fn receive(
        &self,
        from: NodeId,
        group: GroupId,
        id: u64,
        offset: u64,
        total: u64,
        data: &[u8],
    ) -> usize {
        let mut inc = crate::node::lock(&self.incoming);
        if offset == 0 {
            // A transfer (re)starts: transfers of the group that didn't
            // finish go; complete files stay until restored or replaced.
            inc.retain(|(g, i), x| *g != group || (*i != id && x.written >= x.total));
            let path = self.in_path(group, from, id);
            let file = File::create(&path).ok();
            let broken = file.is_none();
            inc.insert(
                (group, id),
                Incoming {
                    id,
                    from,
                    total,
                    written: 0,
                    file,
                    path,
                    broken,
                },
            );
        }
        let Some(i) = inc.get_mut(&(group, id)) else {
            return 0;
        };
        if i.from != from || i.broken || i.written != offset || i.total != total {
            i.broken = true;
            return 0;
        }
        let ok = i.file.as_mut().is_some_and(|f| f.write_all(data).is_ok());
        if !ok {
            i.broken = true;
            return 0;
        }
        i.written += data.len() as u64;
        if i.written >= i.total {
            // Complete: close it (the restore opens it again); older
            // complete files of the group go.
            drop(i.file.take());
            inc.retain(|(g, i), _| *g != group || *i >= id);
        }
        data.len()
    }

    /// Whether snapshot `id` (`len` bytes) of `group` is here in full.
    pub(crate) fn has(&self, group: GroupId, id: u64, len: u64) -> bool {
        crate::node::lock(&self.incoming)
            .get(&(group, id))
            .is_some_and(|i| i.complete(id, len))
    }

    /// The file of complete snapshot `id` of `group`, taken out of the
    /// incoming list (deleted when the returned value drops).
    pub(crate) fn take(&self, group: GroupId, id: u64, len: u64) -> Option<Incoming> {
        let mut inc = crate::node::lock(&self.incoming);
        if inc.get(&(group, id)).is_some_and(|i| i.complete(id, len)) {
            inc.remove(&(group, id))
        } else {
            None
        }
    }

    /// Forgets everything of `group` (its replica left this node).
    pub(crate) fn forget(&self, group: GroupId) {
        crate::node::lock(&self.incoming).retain(|(g, _), _| *g != group);
        crate::node::lock(&self.outgoing).remove(&group);
    }

    /// Whether a transfer of `group` to `to` is queued or being sent.
    pub(crate) fn sending(&self, to: NodeId, group: GroupId) -> bool {
        crate::node::lock(&self.queue).1.contains(&(to, group))
    }
}

/// Paces the sender: at most `rate` bytes a second over the transfers.
#[derive(Debug)]
pub(crate) struct Pace {
    start: std::time::Instant,
    sent: u64,
    rate: u64,
}

impl Pace {
    pub(crate) fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            sent: 0,
            rate: 0,
        }
    }

    /// Waits until `n` more bytes may go at `rate` bytes a second (0: no
    /// limit); a change of rate starts a new window.
    pub(crate) fn wait(&mut self, n: usize, rate: u64) {
        if rate != self.rate || self.start.elapsed() > std::time::Duration::from_secs(10) {
            // A new window: idle time isn't saved up for a burst.
            self.start = std::time::Instant::now();
            self.sent = 0;
            self.rate = rate;
        }
        self.sent += n as u64;
        if rate == 0 {
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let due = std::time::Duration::from_secs_f64(self.sent as f64 / rate as f64);
        let elapsed = self.start.elapsed();
        if due > elapsed {
            std::thread::sleep(due.saturating_sub(elapsed));
        }
    }
}

/// Removes `path`, ignoring a missing file.
pub(crate) fn remove(path: &Path) {
    let _ = std::fs::remove_file(path);
}
