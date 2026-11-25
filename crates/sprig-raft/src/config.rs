//! Driver settings.

use std::time::Duration;

/// How a node runs its groups. The defaults are the production values:
/// 100 ms ticks, elections after 1 to 2 s of silence, heartbeats every
/// 200 ms, a 900 ms lease used with a 100 ms margin.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// One Raft tick.
    pub tick: Duration,
    /// Ticks without a leader before a follower campaigns, at least; the
    /// randomized timeout is drawn from this to twice this. Followers also
    /// refuse votes for this long after hearing from their leader.
    pub election_ticks: usize,
    /// Ticks between a leader's heartbeats.
    pub heartbeat_ticks: usize,
    /// How long after a quorum accepted a frame a leader may lease reads.
    pub lease: Duration,
    /// Taken off the lease for clock rate error.
    pub lease_margin: Duration,
    /// Ticks a leader's group must be idle and fully replicated before it
    /// quiesces.
    pub quiesce_ticks: u32,
    /// Store liveness: a frame goes to every peer node at least this often.
    pub liveness_interval: Duration,
    /// Store liveness: a peer node silent this long is taken as down, and
    /// the quiesced groups it leads wake up.
    pub liveness_timeout: Duration,
    /// The most entry bytes in one append message.
    pub max_size_per_msg: u64,
    /// The most append messages in flight to one follower.
    pub max_inflight_msgs: usize,
    /// For state machines that don't keep their own state across a
    /// restart: once this many entries are applied past the last snapshot,
    /// the driver stores a new snapshot in the log store and trims the log
    /// below it.
    pub compact_after: u64,
    /// Ticks a follower may stay waiting for a snapshot before the leader
    /// sends it again (the first one was lost).
    pub snapshot_retry_ticks: u32,
    /// Create a group on the first message from its leader (a replica
    /// being added). Off: groups exist only through
    /// [`crate::Node::create_group`].
    pub auto_create: bool,
    /// The voters every group starts with on a cluster whose members are
    /// fixed (every node holds every unit, until placement decides per
    /// unit). Not empty: a member that hears about a group it doesn't hold
    /// (a candidate's vote request, a leader's append) creates it with
    /// these voters, exactly as [`crate::Node::create_group`] would, so a
    /// unit needs creating on one member only. A node not listed creates
    /// nothing this way.
    pub static_voters: Vec<crate::NodeId>,
    /// Log writes and syncs run on a thread of their own, started by
    /// [`crate::Node::spawn`]: the loop keeps stepping groups (and sending
    /// what doesn't wait for durability) while a sync runs, and writes
    /// queued meanwhile share the next sync. Off: the loop writes itself
    /// (a driver run with [`crate::Node::run`] always does).
    pub log_writer_thread: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(100),
            election_ticks: 10,
            heartbeat_ticks: 2,
            lease: Duration::from_millis(900),
            lease_margin: Duration::from_millis(100),
            quiesce_ticks: 20,
            liveness_interval: Duration::from_millis(300),
            liveness_timeout: Duration::from_millis(1500),
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            compact_after: 1024,
            snapshot_retry_ticks: 30,
            auto_create: true,
            static_voters: Vec::new(),
            log_writer_thread: false,
        }
    }
}

impl RaftConfig {
    /// The shortest election timeout: what a follower that heard from its
    /// leader waits before it votes for anyone else.
    #[must_use]
    pub fn min_election(&self) -> Duration {
        self.tick * u32::try_from(self.election_ticks).unwrap_or(u32::MAX)
    }

    /// The part of a lease a leader uses.
    #[must_use]
    pub fn usable_lease(&self) -> Duration {
        self.lease.saturating_sub(self.lease_margin)
    }
}
