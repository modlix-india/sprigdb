//! The multi-Raft driver: many raft-rs groups on one node.
//!
//! A node hosts one Raft group per unit (an unsharded tenant or one shard).
//! raft-rs does the protocol (elections with pre-vote and check-quorum,
//! replication, learners, joint consensus, leader transfer, `ReadIndex`); this
//! crate is the host around it:
//!
//! - **One shared log per node** ([`LogStore`]): every group's entries,
//!   hard state and snapshot records go into one write per driver turn, with
//!   one sync covering every group that needs it (group commit across
//!   groups). [`EngineLogStore`] is raft-engine on disk; [`MemLogStore`] is a
//!   simulated disk that loses unsynced writes and tears its tail on a crash.
//! - **One frame per node pair per turn** ([`Transport`]): the messages of
//!   every group bound for one node travel together, with the acks leases
//!   need. [`MemNetwork`] is in-process; [`tcp`] is TCP with optional mutual
//!   TLS.
//! - **Leader leases on monotonic clocks**: a leader answers reads alone
//!   while a quorum has accepted a frame it sent within the lease (900 ms,
//!   served for 800 ms after that send: a 100 ms margin for clock rate
//!   error). A follower that accepted that frame refuses votes for an
//!   election timeout (at least 1 s), so a new leader can't exist while the
//!   lease is used. Without a valid lease, reads use `ReadIndex`.
//! - **Quiescence**: a leader whose group is idle and fully replicated tells
//!   its followers to stop ticking; quiesced groups cost nothing per tick.
//!   Store liveness, one heartbeat per node pair, wakes the groups of a
//!   node that stops answering.
//! - **Snapshots through the state machine** ([`StateMachine::snapshot`] and
//!   [`StateMachine::restore`]): sent to followers that fell behind the
//!   log's compaction, and kept in the log store for volatile state
//!   machines.
//! - **Proposals with definite outcomes**: a proposal fails with
//!   [`RaftError::NotLeader`] before entering the log, with
//!   [`RaftError::Dropped`] when another leader's entry replaced it (never
//!   committed, safe to retry), and with [`RaftError::Unknown`] only when a
//!   snapshot or a crash hides what happened.
//!
//! [`RaftConsensus`] implements the engine's `Consensus` trait on top.
//!
//! The driver is one async loop per node ([`Node::run`]), single-threaded
//! over all its groups. In production it runs on its own thread
//! ([`Node::spawn`]); in simulation it runs on the simulator's paused-time
//! runtime with a simulated clock, disk and network.

// raft-rs messages are protobuf structs, built field by field.
#![allow(clippy::field_reassign_with_default)]

pub mod config;
pub mod consensus;
pub mod engine_node;
pub mod engine_snap;
pub mod error;
mod lease;
mod logger;
pub mod logstore;
pub mod node;
pub mod slot;
pub mod sm;
mod snapfile;
mod storage;
pub mod toy;
pub mod transport;
mod wire;

/// A Raft group: one unit (the engine's `UnitId`).
pub type GroupId = u64;
/// A node. Each node holds at most one replica of a group, so a group's
/// Raft peer ids are node ids.
pub type NodeId = u64;

pub use config::{PlacementLookup, RaftConfig, UnitPlacement};
pub use consensus::{RaftConsensus, UnitMachine, UnitSnapshots};
pub use engine_node::{EngineNode, EngineNodeOptions};
pub use engine_snap::EngineSnapshots;
pub use error::RaftError;
pub use logstore::{EngineLogStore, LogStore, MemLogStore};
pub use node::{GroupStatus, Node, NodeOptions, NodeStats, Observer, Waiter};
pub use raft::eraftpb;
pub use raft::prelude::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};
pub use slot::Slot;
pub use sm::{SnapshotExporter, StateMachine, StateMachineFactory};
pub use transport::{MemNetwork, MemTransport, Transport, tcp};

/// The index and term every group's log starts after: a new group's
/// replicas all begin from a snapshot at this point holding the initial
/// voters, and replicas added later always start from a snapshot.
pub const INIT_INDEX: u64 = 5;
/// See [`INIT_INDEX`].
pub const INIT_TERM: u64 = 5;
