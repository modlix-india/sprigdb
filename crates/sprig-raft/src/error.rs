//! The driver's errors.

use sprig_common::{SprigCode, SprigError};

use crate::{GroupId, NodeId};

/// Why a call to the driver failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RaftError {
    /// This replica doesn't lead the group: nothing was written. Retry on
    /// the leader, if one is known.
    #[error("not the group's leader (leader: {leader:?})")]
    NotLeader {
        /// The leader this replica knows of.
        leader: Option<NodeId>,
    },
    /// The entry entered this replica's log, but another leader's entry
    /// replaced it: it never committed. Safe to retry.
    #[error("the group's leader changed before the entry committed")]
    Dropped,
    /// The entry may or may not have committed: a snapshot replaced the log
    /// before its outcome was seen, or the node stopped.
    #[error("the entry's outcome is unknown")]
    Unknown,
    /// The node has no such group.
    #[error("no group {0} on this node")]
    NoGroup(GroupId),
    /// The request can't run now (another membership change is pending, a
    /// leader transfer is running). Retry later.
    #[error("busy: {0}")]
    Busy(&'static str),
    /// The request is not valid.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The node stopped.
    #[error("the node is stopped")]
    Closed,
    /// The log store failed.
    #[error("log store: {0}")]
    Storage(String),
    /// raft-rs refused the call.
    #[error("raft: {0}")]
    Raft(String),
}

impl RaftError {
    /// Whether the caller may simply retry: nothing was committed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            RaftError::NotLeader { .. } | RaftError::Dropped | RaftError::Busy(_)
        )
    }
}

impl From<raft::Error> for RaftError {
    fn from(e: raft::Error) -> Self {
        match e {
            raft::Error::ProposalDropped => RaftError::NotLeader { leader: None },
            e => RaftError::Raft(e.to_string()),
        }
    }
}

impl From<RaftError> for SprigError {
    fn from(e: RaftError) -> Self {
        let msg = e.to_string();
        match e {
            RaftError::NotLeader { .. } | RaftError::Dropped | RaftError::Busy(_) => {
                SprigError::new(SprigCode::ShardMoved, format!("{msg}; retry"))
            }
            RaftError::Unknown => SprigError::new(SprigCode::CommitOutcomeUnknown, msg),
            RaftError::NoGroup(_)
            | RaftError::Invalid(_)
            | RaftError::Closed
            | RaftError::Storage(_)
            | RaftError::Raft(_) => SprigError::internal(msg),
        }
    }
}
