//! Driver settings.

use std::path::PathBuf;
use std::sync::Arc;
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
    /// Placement per unit: where it is set, it decides which nodes a unit's
    /// group starts on, and `static_voters` serve only the units it answers
    /// [`UnitPlacement::Static`] for. A member the lookup lists creates the
    /// group with exactly those voters when it first hears of it (or on its
    /// first write), as every listed member would; a node it doesn't list
    /// creates nothing that way.
    pub placement: Option<PlacementLookup>,
    /// Log writes and syncs run on a thread of their own, started by
    /// [`crate::Node::spawn`]: the loop keeps stepping groups (and sending
    /// what doesn't wait for durability) while a sync runs, and writes
    /// queued meanwhile share the next sync. Off: the loop writes itself
    /// (a driver run with [`crate::Node::run`] always does).
    pub log_writer_thread: bool,
    /// Snapshots of state machines that can write them off the driver
    /// ([`crate::StateMachine::exporter`]: the engine's units) are built
    /// into a file on a thread of their own, and sent in chunks of 1 MiB
    /// that a follower writes to a file before it restores from it, paced
    /// by [`RaftConfig::snapshot_bytes_per_second`]. Off: built on the
    /// driver and sent in one message (the simulator, which steps every
    /// thread itself).
    pub snapshot_off_driver: bool,
    /// Where those snapshots' files are kept while they are built, sent or
    /// received (cleared when the node opens). `None`: a folder of its own
    /// under the system's temporary directory.
    pub snapshot_dir: Option<PathBuf>,
    /// The most snapshot bytes per second this node sends, over all its
    /// transfers together (0: no limit). [`crate::Node::set_snapshot_rate`]
    /// changes it while the node runs.
    pub snapshot_bytes_per_second: u64,
    /// The most bytes of messages one frame to a peer carries: a turn's
    /// messages to a peer that come to more go in several frames, in
    /// order (a follower catching up after a large snapshot gets up to
    /// `max_inflight_msgs` appends in one turn, each up to an entry of
    /// 16 MiB). A single message larger than this goes in a frame of its
    /// own. Far below the transport's frame limit (1 GiB).
    pub max_frame_bytes: usize,
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
            placement: None,
            log_writer_thread: false,
            snapshot_off_driver: true,
            snapshot_dir: None,
            snapshot_bytes_per_second: 0,
            max_frame_bytes: 64 << 20,
        }
    }
}

/// Where a unit's group starts, as the placement lookup knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitPlacement {
    /// The unit is placed on these voters (its descriptor's first
    /// membership).
    Voters(Vec<crate::NodeId>),
    /// Not placed per unit: the cluster's fixed voters
    /// ([`RaftConfig::static_voters`]) hold it.
    Static,
    /// Not known here (yet): nothing creates the group from a message or a
    /// write until it is; a leader may still add this node as a replica.
    Unknown,
}

/// The placement lookup of [`RaftConfig::placement`]: called on the
/// driver's thread, so it must answer from memory.
#[derive(Clone)]
pub struct PlacementLookup(pub Arc<dyn Fn(crate::GroupId) -> UnitPlacement + Send + Sync>);

impl PlacementLookup {
    /// A lookup from `f`.
    pub fn new(f: impl Fn(crate::GroupId) -> UnitPlacement + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Where `group` starts.
    #[must_use]
    pub fn of(&self, group: crate::GroupId) -> UnitPlacement {
        (self.0)(group)
    }
}

impl std::fmt::Debug for PlacementLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PlacementLookup")
    }
}

impl RaftConfig {
    /// The voters node `me` starts `group` with when it doesn't hold it
    /// yet: `None` when `me` isn't one of them or the unit's placement isn't
    /// known here.
    #[must_use]
    pub fn start_voters(
        &self,
        me: crate::NodeId,
        group: crate::GroupId,
    ) -> Option<Vec<crate::NodeId>> {
        let placed = match &self.placement {
            None => UnitPlacement::Static,
            Some(p) => p.of(group),
        };
        let voters = match placed {
            UnitPlacement::Voters(v) => v,
            UnitPlacement::Static => self.static_voters.clone(),
            UnitPlacement::Unknown => return None,
        };
        voters.contains(&me).then_some(voters)
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
