//! The node's storage engine: one [`Engine`] per process, shared by every
//! front end whose data lives on it (engine-backed SQL databases and Redis
//! databases).
//!
//! It opens on first use, from the environment:
//! - `SPRIGDB_ENGINE_DIR`: its directory (RocksDB and its commit log),
//!   default `/var/lib/sprigdb/engine`;
//! - `SPRIGDB_ENGINE_STORE`: `rocks` (default) or `mem` (an in-memory store
//!   rebuilt from the log at start; for tests);
//! - `SPRIGDB_ENGINE_MB`: its memory in MiB, default 1,024;
//! - `SPRIGDB_ENGINE_RAFT_ID` and `SPRIGDB_ENGINE_RAFT_PEERS`: when both are
//!   set, this node is one replica of several: every tenant's unit commits
//!   through Raft with the listed nodes as its voters (a majority synced),
//!   and the engine opens at start, not on first use, so the node takes
//!   part in replication without clients. `PEERS` lists every member,
//!   this one included: `1=10.0.0.11:26171,2=10.0.0.12:26171,...`.
//!   `SPRIGDB_ENGINE_RAFT_LISTEN` is the address the Raft listener binds
//!   (default: this node's own entry). Unset: one node, as before.
//! - `SPRIGDB_ENGINE_STREAM_GROUPS`: on a replicated node, the partition
//!   groups per tenant: a number, or `auto` (the default): one group per
//!   replica where the Raft log writer thread is on, else `0` (every
//!   partition in its tenant's unit). With `n` groups, partition `p` of a
//!   stream commits through its tenant's group unit `p % n`, each group's
//!   leader preferably on a different replica, so Kafka writes spread over
//!   the nodes. Groups pay only with the writer thread: without it each
//!   node's one Raft loop syncs for a group it leads and groups it follows
//!   in turn. A stream log keeps the groups it was written with. Every
//!   replica must use the same value (and the same writer-thread setting
//!   under `auto`).
//! - `SPRIGDB_ENGINE_RAFT_LOG_THREAD`: on a replicated node, the Raft
//!   log's writes and syncs run on a thread of their own, so the Raft loop
//!   keeps stepping groups (and answering) while a sync runs, and writes
//!   queued meanwhile share the next sync (default `true`; `false` writes
//!   on the loop, as before).
//! - `SPRIGDB_ENGINE_STREAM_WRITE_ONCE`: on a replicated node, each stream
//!   append's batch is written once: the Raft log keeps it in a note blob
//!   file (`<engine dir>/raft-notes`) with only a reference in the log, and
//!   the stream's segments refer to the same bytes instead of copying them
//!   (default `true`; `false` writes the batch into both, as before; blobs
//!   already written are still read).
//! - `SPRIGDB_ENGINE_STREAM_COMPRESS`: a stream batch that arrives
//!   uncompressed goes into the log LZ4-compressed when that makes it an
//!   eighth smaller (default `true`; `false` stores it as it arrives).
//!   Batches a client compressed are always stored as they arrive.
//! - `SPRIGDB_ENGINE_RAFT_JOIN=true`: this node replaces a lost replica.
//!   It starts with no units of its own and takes each unit only from the
//!   unit's leader, once an operator runs `sprigctl engine replace-replica`
//!   (the route [`REPLACE_PATH`] on a surviving gateway).
//!
//! If it can't open, every caller gets the same error, naming the
//! directory; nothing falls back to MariaDB.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use sprig_common::time::SystemClock;
use sprig_common::{Result, SprigError};
use sprig_engine::log::{Durability, SyncMode};
use sprig_engine::{Engine, EngineConfig, StoreKind};

/// The variable naming the engine's directory.
pub const DIR_VAR: &str = "SPRIGDB_ENGINE_DIR";
/// The variable choosing the engine's store (`rocks` or `mem`).
pub const STORE_VAR: &str = "SPRIGDB_ENGINE_STORE";
/// The variable giving the engine's memory, in MiB.
pub const MB_VAR: &str = "SPRIGDB_ENGINE_MB";
/// The variable giving this node's Raft id (1..N).
pub const RAFT_ID_VAR: &str = "SPRIGDB_ENGINE_RAFT_ID";
/// The variable listing every replica: `<id>=<host:port>,...`.
pub const RAFT_PEERS_VAR: &str = "SPRIGDB_ENGINE_RAFT_PEERS";
/// The variable giving the address this node's Raft listener binds.
pub const RAFT_LISTEN_VAR: &str = "SPRIGDB_ENGINE_RAFT_LISTEN";
/// The variable saying this node joins as a replacement replica.
pub const RAFT_JOIN_VAR: &str = "SPRIGDB_ENGINE_RAFT_JOIN";
/// The variable giving the partition groups per tenant on a replicated
/// node.
pub const STREAM_GROUPS_VAR: &str = "SPRIGDB_ENGINE_STREAM_GROUPS";
/// The variable that turns the Raft log writer thread off (`false`).
pub const RAFT_LOG_THREAD_VAR: &str = "SPRIGDB_ENGINE_RAFT_LOG_THREAD";
/// The variable that turns writing stream appends once off (`false`).
pub const STREAM_WRITE_ONCE_VAR: &str = "SPRIGDB_ENGINE_STREAM_WRITE_ONCE";
/// The variable that turns LZ4 on uncompressed stream batches off
/// (`false`).
pub const STREAM_COMPRESS_VAR: &str = "SPRIGDB_ENGINE_STREAM_COMPRESS";

/// Whether uncompressed stream batches go into the log LZ4-compressed
/// ([`STREAM_COMPRESS_VAR`], default on).
#[must_use]
pub fn stream_compress() -> bool {
    !std::env::var(STREAM_COMPRESS_VAR).is_ok_and(|v| v.trim().eq_ignore_ascii_case("false"))
}
/// The node-local Admin API route that replaces a lost replica.
pub const REPLACE_PATH: &str = "/admin/v1/engine/replicas:replace";

/// This node's place among the replicas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftSetup {
    /// This node's id.
    pub id: u64,
    /// Every member, this one included, by id.
    pub peers: Vec<(u64, SocketAddr)>,
    /// The address the Raft listener binds.
    pub listen: SocketAddr,
    /// A replacement replica: no fixed voters, units only from leaders.
    pub join: bool,
}

/// Whether this node is configured as one replica of several.
#[must_use]
pub fn replicated_from_env() -> bool {
    raft_from(|k| std::env::var(k).ok()).is_ok_and(|r| r.is_some())
}

/// This node's Raft id when it is one replica of several.
#[must_use]
pub fn raft_id_from_env() -> Option<u64> {
    raft_from(|k| std::env::var(k).ok())
        .ok()
        .flatten()
        .map(|r| r.id)
}

/// Who leads a tenant's engine unit, as this node knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnitLeader {
    /// No one alone: a one-node engine, or a unit no replica has started
    /// yet (the first write starts it where it lands).
    Any,
    /// This node.
    Here,
    /// Another replica, by Raft id.
    Node(u64),
    /// Not known now (an election is running, or replication is still
    /// starting).
    Unknown,
}

/// Who leads `tenant`'s unit (the unit that holds its records, streams and
/// queues), from this node's Raft state.
#[must_use]
pub fn tenant_leader(tenant: u64) -> UnitLeader {
    unit_leader(tenant)
}

/// Who leads engine unit `unit` (a tenant's, or a partition group's: see
/// [`stream_groups`]), from this node's Raft state. A partition group no
/// replica has started yet is named by its preferred leader, where its
/// first write starts it.
#[must_use]
pub fn unit_leader(unit: u64) -> UnitLeader {
    let Some(raft) = RAFT.get() else {
        return if replicated_from_env() {
            UnitLeader::Unknown
        } else {
            UnitLeader::Any
        };
    };
    match raft.node.node().status(unit) {
        None => match preferred_leader(&raft.voters, unit) {
            Some(p) if p == raft.node.node().id() => UnitLeader::Here,
            Some(p) => UnitLeader::Node(p),
            None => UnitLeader::Any,
        },
        Some(st) if st.is_leader => UnitLeader::Here,
        Some(st) => st.leader.map_or(UnitLeader::Unknown, UnitLeader::Node),
    }
}

/// The replica that should lead `unit`: for a tenant's partition group
/// `g`, the voters in id order, starting at the tenant's own offset, so
/// the groups of one tenant (and of tenants side by side) sit on
/// different replicas. `None` for other units (wherever elections put
/// them).
fn preferred_leader(voters: &[u64], unit: u64) -> Option<u64> {
    let (tenant, g) = sprig_streams::seg::group_of_unit(unit)?;
    let n = u64::try_from(voters.len()).ok().filter(|n| *n > 0)?;
    let at = (tenant % n + u64::from(g)) % n;
    voters.get(usize::try_from(at).ok()?).copied()
}

/// The partition groups per tenant for this node's stream log: 0 on one
/// node; on a replicated node [`STREAM_GROUPS_VAR`], where `auto` (also
/// when unset) is one group per replica with the Raft log writer thread on,
/// 0 with it off.
#[must_use]
pub fn stream_groups() -> u32 {
    groups_from(|k| std::env::var(k).ok())
}

fn groups_from(lookup: impl Fn(&str) -> Option<String>) -> u32 {
    let Ok(Some(r)) = raft_from(&lookup) else {
        return 0;
    };
    let auto = || {
        let thread =
            !lookup(RAFT_LOG_THREAD_VAR).is_some_and(|v| v.trim().eq_ignore_ascii_case("false"));
        if thread {
            u32::try_from(r.peers.len()).unwrap_or(0).min(255)
        } else {
            0
        }
    };
    let v = lookup(STREAM_GROUPS_VAR).map(|v| v.trim().to_owned());
    match v.as_deref() {
        None | Some("") => auto(),
        Some(a) if a.eq_ignore_ascii_case("auto") => auto(),
        Some(n) => n.parse::<u32>().map_or_else(
            |_| {
                tracing::warn!("{STREAM_GROUPS_VAR} isn't a number or auto; no partition groups");
                0
            },
            |n| n.min(255),
        ),
    }
}

/// The replication settings from `lookup`: `None` for one node.
fn raft_from(lookup: impl Fn(&str) -> Option<String>) -> Result<Option<RaftSetup>> {
    let var = |k: &str| lookup(k).filter(|v| !v.trim().is_empty());
    let bad = |m: String| SprigError::internal(m);
    let (id, peers) = match (var(RAFT_ID_VAR), var(RAFT_PEERS_VAR)) {
        (None, None) => return Ok(None),
        (Some(i), Some(p)) => (i, p),
        _ => {
            return Err(bad(format!(
                "{RAFT_ID_VAR} and {RAFT_PEERS_VAR} are set together or not at all"
            )));
        }
    };
    let id: u64 = id.trim().parse().ok().filter(|i| *i > 0).ok_or_else(|| {
        bad(format!(
            "{RAFT_ID_VAR} must be a node id above 0, not '{id}'"
        ))
    })?;
    let mut list = Vec::new();
    for item in peers.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let parsed = item.split_once('=').and_then(|(i, a)| {
            Some((
                i.trim().parse::<u64>().ok()?,
                a.trim().parse::<SocketAddr>().ok()?,
            ))
        });
        let Some((pid, addr)) = parsed.filter(|(pid, _)| *pid > 0) else {
            return Err(bad(format!(
                "{RAFT_PEERS_VAR}: '{item}' is not <id>=<host:port>"
            )));
        };
        if list.iter().any(|(p, _)| *p == pid) {
            return Err(bad(format!("{RAFT_PEERS_VAR} names node {pid} twice")));
        }
        list.push((pid, addr));
    }
    let Some(own) = list.iter().find(|(p, _)| *p == id).map(|(_, a)| *a) else {
        return Err(bad(format!(
            "{RAFT_PEERS_VAR} doesn't list this node ({id})"
        )));
    };
    let listen = match var(RAFT_LISTEN_VAR) {
        None => own,
        Some(l) => l
            .trim()
            .parse()
            .map_err(|_| bad(format!("{RAFT_LISTEN_VAR} must be host:port, not '{l}'")))?,
    };
    let join = match var(RAFT_JOIN_VAR).as_deref().map(str::trim) {
        None | Some("false" | "0" | "no") => false,
        Some("true" | "1" | "yes") => true,
        Some(other) => {
            return Err(bad(format!(
                "{RAFT_JOIN_VAR} must be true or false, not '{other}'"
            )));
        }
    };
    Ok(Some(RaftSetup {
        id,
        peers: list,
        listen,
        join,
    }))
}

/// The engine's settings from the environment.
///
/// # Errors
/// `SPRIGDB_ENGINE_STORE` or `SPRIGDB_ENGINE_MB` has a value it can't use.
pub fn config_from_env() -> Result<EngineConfig> {
    config_from(|k| std::env::var(k).ok())
}

/// The engine's settings from `lookup` (a variable's value by name).
fn config_from(lookup: impl Fn(&str) -> Option<String>) -> Result<EngineConfig> {
    let var = |k: &str| lookup(k).filter(|v| !v.trim().is_empty());
    let dir = var(DIR_VAR).map_or_else(|| PathBuf::from("/var/lib/sprigdb/engine"), PathBuf::from);
    let mut cfg = EngineConfig::new(dir);
    cfg.store = match var(STORE_VAR).as_deref().map(str::trim) {
        None | Some("rocks") => StoreKind::Rocks,
        Some("mem") => StoreKind::Mem,
        Some(other) => {
            return Err(SprigError::internal(format!(
                "{STORE_VAR} must be 'rocks' or 'mem', not '{other}'"
            )));
        }
    };
    if let Some(mb) = var(MB_VAR) {
        cfg.budget.engine_mb = mb.trim().parse().map_err(|_| {
            SprigError::internal(format!("{MB_VAR} must be a number of MiB, not '{mb}'"))
        })?;
    }
    Ok(cfg)
}

/// The node's engine, opened on the first call; every call returns the
/// same handle, or the same failure.
///
/// # Errors
/// The settings are malformed, or the engine failed to open in its
/// directory.
pub fn engine() -> Result<Engine> {
    static ENGINE: OnceLock<std::result::Result<Engine, String>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            let mut cfg = config_from_env().map_err(|e| e.message.clone())?;
            let raft = raft_from(|k| std::env::var(k).ok()).map_err(|e| e.message.clone())?;
            let dir = cfg.data_dir.display().to_string();
            if raft.is_some() {
                cfg.replicated = true;
                // A commit is acknowledged once a majority synced it.
                cfg.default_durability = Durability::Strict;
            }
            let raft_dir = cfg.data_dir.join("raft-repl");
            let opened = Engine::open(cfg).and_then(|e| match &raft {
                Some(r) => start_raft(&e, r, raft_dir).map(|()| e),
                None => Ok(e),
            });
            match opened {
                Ok(e) => {
                    tracing::info!(dir = %dir, replicated = raft.is_some(), "storage engine open");
                    crate::changelog::open_on(&e);
                    crate::backup::open_on(&e);
                    Ok(e)
                }
                Err(e) => {
                    tracing::error!(dir = %dir, error = %e, "storage engine failed to open");
                    Err(format!("storage engine in {dir}: {}", e.message))
                }
            }
        })
        .clone()
        .map_err(SprigError::internal)
}

/// Joins this node's engine to its replicas: the Raft log beside the
/// engine, the TCP transport on its own small runtime, the listener and the
/// driver thread. They live as long as the process.
fn start_raft(engine: &Engine, r: &RaftSetup, dir: PathBuf) -> Result<()> {
    let raft_err = |m: String| SprigError::internal(format!("engine replication: {m}"));
    // Runs once per process (inside `engine()`'s initialisation); the
    // runtime lives as long as the process, like the node.
    let rt: &'static tokio::runtime::Runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("sprig-raft-net")
            .enable_all()
            .build()
            .map_err(|e| raft_err(e.to_string()))?,
    ));
    let transport = sprig_raft::tcp::TcpTransport::new(r.id, rt.handle().clone(), None);
    for (pid, addr) in &r.peers {
        transport.set_peer(*pid, *addr);
    }
    let notes_dir = dir.with_file_name("raft-notes");
    let store = sprig_raft::EngineLogStore::open(dir, SyncMode::Full, None)
        .map_err(|e| raft_err(e.to_string()))?;
    // Before the node opens: its recovery reads the log, whose references
    // to note blobs must be put back. Opened even with writing once off, to
    // read the blobs an earlier run wrote.
    let write_once =
        !std::env::var(STREAM_WRITE_ONCE_VAR).is_ok_and(|v| v.trim().eq_ignore_ascii_case("false"));
    let blobs = sprig_engine::log::blobs::NoteBlobs::open(
        &notes_dir,
        SyncMode::Full,
        sprig_streams::seg::note::is_append,
        write_once,
    )
    .map_err(|e| raft_err(format!("note blobs in {}: {e}", notes_dir.display())))?;
    store.set_note_blobs(Arc::clone(&blobs));
    let _ = NOTE_BLOBS.set(blobs);
    let node = sprig_raft::EngineNode::open(
        engine,
        sprig_raft::EngineNodeOptions {
            id: r.id,
            config: sprig_raft::RaftConfig {
                // A replacement starts no unit itself: each comes from its
                // leader when the operator replaces the lost replica.
                static_voters: if r.join {
                    Vec::new()
                } else {
                    r.peers.iter().map(|(p, _)| *p).collect()
                },
                log_writer_thread: !std::env::var(RAFT_LOG_THREAD_VAR)
                    .is_ok_and(|v| v.trim().eq_ignore_ascii_case("false")),
                ..sprig_raft::RaftConfig::default()
            },
            store,
            transport: Arc::clone(&transport) as Arc<dyn sprig_raft::Transport>,
            clock: Arc::new(SystemClock),
            seed: r.id ^ u64::from(std::process::id()),
            observer: None,
            inline_roles: false,
        },
    )
    .map_err(|e| raft_err(e.to_string()))?;
    // The listener is async; bind it from a plain thread so this works
    // whether or not the caller runs inside a runtime.
    let (listen, raft_node, handle) = (r.listen, node.node().clone(), rt.handle().clone());
    let bound = std::thread::scope(|s| {
        s.spawn(|| handle.block_on(transport.listen(listen, raft_node)))
            .join()
            .map_err(|_| raft_err("the listener thread panicked".into()))
    })?
    .map_err(|e| raft_err(format!("raft listener on {listen}: {e}")))?;
    node.spawn().map_err(|e| raft_err(e.to_string()))?;
    let mut voters: Vec<u64> = r.peers.iter().map(|(p, _)| *p).collect();
    voters.sort_unstable();
    let prefer = voters.clone();
    node.prefer_leaders(
        Arc::new(move |u: sprig_engine::UnitId| preferred_leader(&prefer, u.0)),
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(10),
    )
    .map_err(|e| raft_err(e.to_string()))?;
    tracing::info!(id = r.id, raft = %bound, voters = r.peers.len(), join = r.join, stream_groups = groups_from(|k| std::env::var(k).ok()), stream_write_once = write_once, "engine units replicate through raft");
    let _ = RAFT.set(Raft {
        node,
        transport,
        voters,
    });
    Ok(())
}

/// The process's Raft node and transport, once started.
struct Raft {
    node: sprig_raft::EngineNode,
    transport: Arc<sprig_raft::tcp::TcpTransport>,
    /// Every member's id, in order.
    voters: Vec<u64>,
}

static RAFT: OnceLock<Raft> = OnceLock::new();

static NOTE_BLOBS: OnceLock<Arc<sprig_engine::log::blobs::NoteBlobs>> = OnceLock::new();

/// The Raft log's note blobs on a replicated node (after [`engine`]
/// opened), for the stream log's segments to refer to.
#[must_use]
pub fn note_blobs() -> Option<Arc<sprig_engine::log::blobs::NoteBlobs>> {
    NOTE_BLOBS.get().cloned()
}

/// What [`replace_replica`] did.
#[derive(Debug, Default, serde::Serialize)]
pub struct ReplaceReport {
    /// Units whose lost replica was replaced from here.
    pub replaced: Vec<u64>,
    /// Units that already had the new replica in place of the lost one.
    pub already: usize,
    /// Units that could not be replaced, with the reason.
    pub failed: Vec<(u64, String)>,
}

/// Replaces `dead`, a lost replica, with `new` (a node started with
/// `SPRIGDB_ENGINE_RAFT_JOIN=true`, reachable at `new_addr`) in every unit
/// this node holds: the unit's leadership moves here if it is elsewhere,
/// the new replica joins as a learner, gets the unit from a snapshot and
/// the log, and one joint change makes it a voter in place of `dead`.
/// Idempotent: run it on each surviving node (each learns the new node's
/// address); the first does the work. Blocks; waits up to `timeout` per
/// unit.
///
/// # Errors
/// This node doesn't replicate, or the arguments name the same node.
pub fn replace_replica(
    dead: u64,
    new: u64,
    new_addr: Option<SocketAddr>,
    timeout: std::time::Duration,
) -> Result<ReplaceReport> {
    let bad = |m: &str| SprigError::new(sprig_common::SprigCode::AdminRequestInvalid, m);
    let raft = RAFT
        .get()
        .ok_or_else(|| bad("this gateway's engine doesn't replicate"))?;
    let me = raft.node.node().id();
    if dead == new || dead == 0 || new == 0 || dead == me || new == me {
        return Err(bad(
            "dead and new must be two other nodes' ids, neither this node's",
        ));
    }
    if let Some(a) = new_addr {
        raft.transport.set_peer(new, a);
    }
    let engine = engine()?;
    let node = raft.node.node();
    let mut report = ReplaceReport::default();
    for g in node.groups() {
        let Some(st) = node.status(g) else { continue };
        let has_dead = st.voters.contains(&dead) || st.voters_outgoing.contains(&dead);
        if !has_dead {
            if st.voters.contains(&new) {
                report.already += 1;
            }
            continue;
        }
        let unit = sprig_engine::UnitId(g);
        let deadline = std::time::Instant::now() + timeout;
        // The unit's leader runs the change: move it here first.
        while engine.leader_epoch(unit) == 0 && std::time::Instant::now() < deadline {
            let _ = node.request_leadership(g);
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if engine.leader_epoch(unit) == 0 {
            report
                .failed
                .push((g, "its leadership didn't move here in time".to_owned()));
            continue;
        }
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match raft.node.replace_replica(unit, dead, new, left) {
            Ok(()) => report.replaced.push(g),
            Err(e) => report.failed.push((g, e.to_string())),
        }
    }
    tracing::info!(
        dead,
        new,
        replaced = report.replaced.len(),
        already = report.already,
        failed = report.failed.len(),
        "engine: replaced a lost replica"
    );
    Ok(report)
}

/// On one replica of several: drops a tenant's cached catalog whenever its
/// unit applies entries while this node doesn't lead it. A follower's
/// catalog records change through replication, not through its own
/// commits, so its cache would otherwise keep what it first loaded (a
/// tenant without the users added later, say) and serve that after it
/// takes the unit's leadership. Does nothing on a one-node engine.
///
/// # Errors
/// The engine failed to open.
pub fn follow_catalogs(cache: sprig_catalog::TenantCache) -> Result<()> {
    if !replicated_from_env() {
        return Ok(());
    }
    let engine = engine()?;
    let from = engine.last_applied();
    engine.observe(
        Arc::new(FollowCatalogs {
            cache,
            engine: engine.downgrade(),
        }),
        from,
    )?;
    Ok(())
}

struct FollowCatalogs {
    cache: sprig_catalog::TenantCache,
    engine: sprig_engine::WeakEngine,
}

impl sprig_engine::ApplyObserver for FollowCatalogs {
    fn applied(&self, unit: sprig_engine::UnitId, _entries: &[sprig_engine::log::LogEntry]) {
        // Partition groups carry stream appends only, no catalog rows.
        if unit == sprig_engine::NODE_UNIT || sprig_streams::seg::group_of_unit(unit.0).is_some() {
            return;
        }
        let Some(e) = self.engine.upgrade() else {
            return;
        };
        if e.leader_epoch(unit) == 0 {
            let _ = self.cache.evict(sprig_common::ids::TenantId(unit.0));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(vars: &[(&str, &str)]) -> Result<EngineConfig> {
        config_from(|k| {
            vars.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn settings_from_the_environment() {
        let d = cfg(&[]).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(d.data_dir, PathBuf::from("/var/lib/sprigdb/engine"));
        assert_eq!(d.store, StoreKind::Rocks);
        assert_eq!(d.budget.engine_mb, 1024);
        let c = cfg(&[(DIR_VAR, "/tmp/e"), (STORE_VAR, "mem"), (MB_VAR, " 256 ")])
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(c.data_dir, PathBuf::from("/tmp/e"));
        assert_eq!(c.store, StoreKind::Mem);
        assert_eq!(c.budget.engine_mb, 256);
        assert!(cfg(&[(STORE_VAR, "disk")]).is_err());
        assert!(cfg(&[(MB_VAR, "lots")]).is_err());
    }

    fn raft(vars: &[(&str, &str)]) -> Result<Option<RaftSetup>> {
        raft_from(|k| {
            vars.iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| (*v).to_owned())
        })
    }

    #[test]
    fn replication_settings() {
        assert_eq!(raft(&[]).unwrap_or_else(|e| panic!("{e}")), None);
        let peers = "1=127.0.0.1:26171, 2=127.0.0.1:26172,3=127.0.0.1:26173";
        let r = raft(&[(RAFT_ID_VAR, "2"), (RAFT_PEERS_VAR, peers)])
            .unwrap_or_else(|e| panic!("{e}"))
            .unwrap_or_else(|| panic!("replicated"));
        assert_eq!(r.id, 2);
        assert_eq!(r.peers.len(), 3);
        assert_eq!(r.listen.to_string(), "127.0.0.1:26172");
        let r = raft(&[
            (RAFT_ID_VAR, "1"),
            (RAFT_PEERS_VAR, peers),
            (RAFT_LISTEN_VAR, "0.0.0.0:26171"),
        ])
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("replicated"));
        assert_eq!(r.listen.to_string(), "0.0.0.0:26171");
        // Malformed: one without the other, an unlisted id, a duplicate,
        // a bad address, id 0.
        assert!(raft(&[(RAFT_ID_VAR, "1")]).is_err());
        assert!(raft(&[(RAFT_PEERS_VAR, peers)]).is_err());
        assert!(raft(&[(RAFT_ID_VAR, "4"), (RAFT_PEERS_VAR, peers)]).is_err());
        assert!(
            raft(&[
                (RAFT_ID_VAR, "1"),
                (RAFT_PEERS_VAR, "1=127.0.0.1:1,1=127.0.0.1:2")
            ])
            .is_err()
        );
        assert!(raft(&[(RAFT_ID_VAR, "1"), (RAFT_PEERS_VAR, "1=nowhere")]).is_err());
        assert!(raft(&[(RAFT_ID_VAR, "0"), (RAFT_PEERS_VAR, "0=127.0.0.1:1")]).is_err());
        // A replacement replica.
        let r = raft(&[
            (RAFT_ID_VAR, "2"),
            (RAFT_PEERS_VAR, peers),
            (RAFT_JOIN_VAR, "true"),
        ])
        .unwrap_or_else(|e| panic!("{e}"))
        .unwrap_or_else(|| panic!("replicated"));
        assert!(r.join);
        assert!(
            raft(&[
                (RAFT_ID_VAR, "2"),
                (RAFT_PEERS_VAR, peers),
                (RAFT_JOIN_VAR, "maybe")
            ])
            .is_err()
        );
    }

    #[test]
    fn partition_groups_and_their_preferred_leaders() {
        let peers = "1=127.0.0.1:26171,2=127.0.0.1:26172,3=127.0.0.1:26173";
        let groups = |vars: &[(&str, &str)]| {
            let vars: Vec<(String, String)> = vars
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            groups_from(|k| vars.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone()))
        };
        assert_eq!(groups(&[]), 0, "one node: no groups");
        let repl = [(RAFT_ID_VAR, "1"), (RAFT_PEERS_VAR, peers)];
        assert_eq!(
            groups(&repl),
            3,
            "by default one per replica (writer thread on)"
        );
        let mut set = repl.to_vec();
        set.push((RAFT_LOG_THREAD_VAR, "false"));
        assert_eq!(groups(&set), 0, "none without the writer thread");
        set.push((STREAM_GROUPS_VAR, "auto"));
        assert_eq!(groups(&set), 0);
        set.pop();
        set.pop();
        set.push((STREAM_GROUPS_VAR, "AUTO"));
        assert_eq!(groups(&set), 3);
        set.pop();
        set.push((STREAM_GROUPS_VAR, "0"));
        assert_eq!(groups(&set), 0);
        set.pop();
        set.push((STREAM_GROUPS_VAR, "6"));
        assert_eq!(groups(&set), 6);
        set.pop();
        set.push((STREAM_GROUPS_VAR, "three"));
        assert_eq!(groups(&set), 0, "malformed: none");
        // A tenant's groups land on different replicas; tenant units have
        // no preference.
        let voters = [1, 2, 3];
        let leads: Vec<Option<u64>> = (0..3)
            .map(|g| preferred_leader(&voters, sprig_streams::seg::group_unit(7, g)))
            .collect();
        assert_eq!(leads, vec![Some(2), Some(3), Some(1)]);
        assert_eq!(preferred_leader(&voters, 7), None);
    }
}
