//! What a group replicates into.

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
