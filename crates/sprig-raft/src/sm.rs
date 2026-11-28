//! What a group replicates into.

use std::io::{Read, Write};
use std::sync::Arc;

use raft::prelude::Entry;

use crate::GroupId;
use crate::error::RaftError;

/// One group's state machine, driven by the node's loop: committed entries
/// in index order, queries for reads, and snapshots both ways.
pub trait StateMachine: Send {
    /// Applies committed normal entries (contiguous, ascending, never
    /// empty-data). Conf changes and leaders' empty entries are not passed.
    ///
    /// # Errors
    /// The state could not be written. The driver stops.
    fn apply(&mut self, entries: &[Entry]) -> Result<(), RaftError>;

    /// Answers a read against the current state (the driver calls it once
    /// the read's index is applied).
    fn query(&self, q: &[u8]) -> Vec<u8>;

    /// The state at the last applied entry, for a follower that fell
    /// behind or for the log store.
    ///
    /// # Errors
    /// The state could not be read.
    fn snapshot(&self) -> Result<Vec<u8>, RaftError>;

    /// Replaces the state with a snapshot taken at `index`.
    ///
    /// # Errors
    /// The snapshot is malformed, or the state could not be written.
    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), RaftError>;

    /// Writes snapshots off the driver thread: `None` (the default) takes
    /// them on the driver with [`StateMachine::snapshot`], in memory.
    fn exporter(&self) -> Option<Arc<dyn SnapshotExporter>> {
        None
    }

    /// [`StateMachine::restore`] from a reader (a snapshot that came in
    /// chunks, from a file), made durable before it returns: the log
    /// store's record of the snapshot then holds no state. By default the
    /// whole snapshot is read into memory.
    ///
    /// # Errors
    /// As for [`StateMachine::restore`]; the reader failed.
    fn restore_from(&mut self, index: u64, input: &mut dyn Read) -> Result<(), RaftError> {
        let mut data = Vec::new();
        input
            .read_to_end(&mut data)
            .map_err(|e| RaftError::Storage(format!("read a snapshot: {e}")))?;
        self.restore(index, &data)
    }

    /// Whether the state survives a restart by itself (an engine with its
    /// own durable store). Volatile state machines (the default) are
    /// rebuilt from the snapshot record and the log, so the driver keeps a
    /// snapshot in the log store and trims the log below it.
    fn durable(&self) -> bool {
        false
    }

    /// For durable state machines: the highest index already applied when
    /// the group opens. Entries up to it are not applied again.
    fn applied_index(&self) -> u64 {
        0
    }

    /// This node's replica of the group is gone: a membership change
    /// without it was applied, or [`crate::Node::drop_group`] dropped it. A
    /// state machine with its own durable store drops what it holds of the
    /// group, so a replica added here again later starts empty (from the
    /// leader's snapshot), not on top of an old copy. Nothing by default.
    fn removed(&mut self) {}
}

/// Writes a group's snapshot on a thread other than the driver's (see
/// [`StateMachine::exporter`]).
pub trait SnapshotExporter: Send + Sync {
    /// Writes the state as of one point in time to `out`. Calls `fixed`
    /// exactly once, as soon as that point is fixed and before anything is
    /// read: the caller holds the group's lock until then, so no entry is
    /// applied in between and the point is the group's applied index.
    ///
    /// # Errors
    /// The state could not be read, or the writer failed.
    fn export(&self, fixed: &mut dyn FnMut(), out: &mut dyn Write) -> Result<(), RaftError>;
}

/// Makes each group's state machine when the group is created or the node
/// restarts.
pub trait StateMachineFactory: Send + Sync {
    /// The state machine of `group`.
    fn create(&self, group: GroupId) -> Box<dyn StateMachine>;
}

impl<F> StateMachineFactory for F
where
    F: Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync,
{
    fn create(&self, group: GroupId) -> Box<dyn StateMachine> {
        self(group)
    }
}
