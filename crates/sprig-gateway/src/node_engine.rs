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
//! - `SPRIGDB_ENGINE_PLACEMENT=on`: on a replicated node, each tenant's
//!   unit lives on the three nodes its unit descriptor names (the
//!   controller places it when it creates the tenant), not on every node:
//!   this node starts a unit's group only where the descriptor lists it,
//!   prefers the descriptor's home node as the unit's leader, and refuses
//!   reads and writes of units placed elsewhere as retryable (39110).
//!   `off` (the default): every node holds every tenant unit, as before.
//!   Every replica must use the same value.
//! - `SPRIGDB_ENGINE_FAILURE_DOMAIN`: this node's failure domain (rack or
//!   zone) for placement: a unit's voters sit in distinct domains. Unset:
//!   a domain of its own.
//! - `SPRIGDB_ENGINE_RAFT_TLS_DIR`: on a replicated node, a directory with
//!   the internal CA (`ca.pem`) and this node's certificate and key
//!   (`node.pem`, `node-key.pem`); the certificate names the node
//!   (subjectAltName `node-<raft id>`). Set, Raft traffic uses mutual TLS
//!   with node identity: a node dials only the node it means and accepts
//!   frames only from the node a peer's certificate names. Unset, a node
//!   that replicates refuses to start: without it Raft is plain TCP, and
//!   whoever reaches the port can send votes, appends and snapshots for
//!   any tenant's group.
//! - `SPRIGDB_ENGINE_INSECURE_CLUSTER=true`: development and test clusters
//!   only: lets a node replicate over that plain, unauthenticated TCP.
//! - `SPRIGDB_ENGINE_RAFT_TRANSPORT=peer`: Raft traffic arrives on the
//!   peer listener instead of a listener of its own; then
//!   `SPRIGDB_ENGINE_RAFT_PEERS` names every node's peer listener. A Raft
//!   connection offers its own ALPN protocol over TLS (or a preface in
//!   plaintext), and the peer listener hands it to the transport; the
//!   bytes, node identity and revocation are the same. `tcp` (the
//!   default): its own listener, as before.
//! - `SPRIGDB_ENGINE_RAFT_TLS_ISSUED=on`: the files in that directory come
//!   from the internal CA (`crate::node_certs`): the node makes its key,
//!   the controller issues and renews its certificate, and the transport
//!   reloads renewed ones and drops revoked peers without a restart.
//! - `SPRIGDB_ENGINE_RAFT_SYNC_PIPELINE`: with the log writer thread, how
//!   many syncs of the Raft log may run at once while the thread goes on
//!   writing (default 8; 0 on macOS, where each sync flushes the whole
//!   drive): a write counts as persisted once a sync that began after it
//!   has returned, and every earlier write counts too. `0` writes and syncs
//!   one after another, as before.
//! - `SPRIGDB_ENGINE_SIM_SYNC_US` (benchmarks only): every log sync in the
//!   process sleeps this many microseconds instead of reaching the disk, so
//!   nothing is durable; `SPRIGDB_ENGINE_SIM_SYNC_SERIAL=true` runs them one
//!   at a time, each covering every sync that waited for it (a journaling
//!   file system's commits).
//! - `SPRIGDB_ENGINE_RAFT_JOIN=true`: this node replaces a lost replica.
//!   It starts with no units of its own and takes each unit only from the
//!   unit's leader, once an operator runs `sprigctl engine replace-replica`
//!   (the route [`REPLACE_PATH`] on a surviving gateway).
//! - `SPRIGDB_ENGINE_RAFT_CUT_FILE`: fault tests only. A file this node
//!   reads every 100 ms: the Raft ids it lists (comma- or space-separated)
//!   are cut off, as a network partition would cut them: frames to them
//!   aren't sent and frames from them are dropped. An empty or missing
//!   file heals every link. Unset (always, outside fault tests): no cuts.
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
/// The variable that places each tenant's unit on the nodes its
/// descriptor names (`on`) instead of every node (`off`, the default).
pub const PLACEMENT_VAR: &str = "SPRIGDB_ENGINE_PLACEMENT";
/// The variable naming this node's failure domain.
pub const FAILURE_DOMAIN_VAR: &str = "SPRIGDB_ENGINE_FAILURE_DOMAIN";
/// The variable naming the directory of the Raft transport's certificates.
pub const RAFT_TLS_DIR_VAR: &str = "SPRIGDB_ENGINE_RAFT_TLS_DIR";
/// The variable choosing where Raft traffic arrives: `tcp` (the default,
/// its own listener) or `peer` (the peer listener).
pub const RAFT_TRANSPORT_VAR: &str = "SPRIGDB_ENGINE_RAFT_TRANSPORT";

/// `true` lets a replicated node run without mutual TLS on its Raft
/// traffic (development and test clusters).
pub const INSECURE_CLUSTER_VAR: &str = "SPRIGDB_ENGINE_INSECURE_CLUSTER";

/// Why a node with replication settings `r` may not start: it would take
/// Raft traffic over plain TCP from anyone who reaches its port (`tls`:
/// [`RAFT_TLS_DIR_VAR`] is set), and nobody asked for that by name.
/// `None` with mutual TLS, with [`INSECURE_CLUSTER_VAR`] set to `true`,
/// and for one node alone that listens on a loopback address.
fn plain_raft_refused(
    r: &RaftSetup,
    tls: bool,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<String>> {
    let insecure = match lookup(INSECURE_CLUSTER_VAR)
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("" | "false" | "0" | "no" | "off") => false,
        Some("true" | "1" | "yes" | "on") => true,
        Some(other) => {
            return Err(SprigError::internal(format!(
                "{INSECURE_CLUSTER_VAR} must be true or false, not '{other}'"
            )));
        }
    };
    if tls || insecure {
        return Ok(None);
    }
    let others = r.peers.iter().filter(|(p, _)| *p != r.id).count();
    if others == 0 && !r.join && r.listen.ip().is_loopback() {
        return Ok(None);
    }
    Ok(Some(format!(
        "this node would replicate with {others} other node(s), listening on {}, over plain \
         TCP that nothing authenticates: set {RAFT_TLS_DIR_VAR} (mutual TLS with node \
         certificates; SPRIGDB_ENGINE_RAFT_TLS_ISSUED=on has the controller's internal CA issue \
         them), or, on a development or test cluster only, {INSECURE_CLUSTER_VAR}=true",
        r.listen
    )))
}

/// Whether Raft traffic arrives on the peer listener ([`RAFT_TRANSPORT_VAR`]).
fn raft_on_peer_from(lookup: impl Fn(&str) -> Option<String>) -> Result<bool> {
    match lookup(RAFT_TRANSPORT_VAR)
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        None | Some("" | "tcp") => Ok(false),
        Some("peer") => Ok(true),
        Some(other) => Err(SprigError::internal(format!(
            "{RAFT_TRANSPORT_VAR} must be tcp or peer, not '{other}'"
        ))),
    }
}

/// The variable naming a fault test's file of Raft ids to cut off.
pub const RAFT_CUT_FILE_VAR: &str = "SPRIGDB_ENGINE_RAFT_CUT_FILE";

/// The Raft ids listed in a cut file's text.
fn cut_ids(text: &str) -> std::collections::BTreeSet<u64> {
    text.split(|c: char| c == ',' || c.is_whitespace())
        .filter_map(|t| t.trim().parse().ok())
        .collect()
}

/// For fault tests ([`RAFT_CUT_FILE_VAR`]): keeps `transport`'s cut links
/// equal to the file's list.
fn follow_cut_file(transport: Arc<sprig_raft::tcp::TcpTransport>) {
    let Ok(path) = std::env::var(RAFT_CUT_FILE_VAR) else {
        return;
    };
    let spawned = std::thread::Builder::new()
        .name("sprig-raft-cut".into())
        .spawn(move || {
            loop {
                let want = std::fs::read_to_string(&path)
                    .map(|t| cut_ids(&t))
                    .unwrap_or_default();
                if want != transport.cut() {
                    tracing::warn!(cut = ?want, "engine: fault test: raft links cut off");
                    transport.set_cut(want);
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
    if let Err(e) = spawned {
        tracing::warn!(error = %e, "engine: the raft cut file isn't followed");
    }
}

/// The Raft transport's mutual TLS for node `id` from
/// [`RAFT_TLS_DIR_VAR`]; `None` when it isn't set.
fn raft_tls(id: u64) -> Result<Option<sprig_raft::tcp::TlsConfig>> {
    let Some(dir) = std::env::var(RAFT_TLS_DIR_VAR)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
    else {
        return Ok(None);
    };
    let dir = PathBuf::from(dir);
    let read = |name: &str| {
        std::fs::read(dir.join(name)).map_err(|e| {
            SprigError::internal(format!(
                "{RAFT_TLS_DIR_VAR}: {}: {e}",
                dir.join(name).display()
            ))
        })
    };
    sprig_raft::tcp::TlsConfig::node_identity(
        id,
        &read("ca.pem")?,
        &read("node.pem")?,
        &read("node-key.pem")?,
    )
    .map(Some)
    .map_err(|e| SprigError::internal(format!("{RAFT_TLS_DIR_VAR}: {e}")))
}

/// Whether units are placed per tenant on a replicated node
/// ([`PLACEMENT_VAR`]).
#[must_use]
pub fn placement_from_env() -> bool {
    placement_from(|k| std::env::var(k).ok())
}

fn placement_from(lookup: impl Fn(&str) -> Option<String>) -> bool {
    matches!(raft_from(&lookup), Ok(Some(_)))
        && lookup(PLACEMENT_VAR).is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "on" | "true" | "1" | "yes"
            )
        })
}

/// This node's failure domain ([`FAILURE_DOMAIN_VAR`]), if set.
#[must_use]
pub fn failure_domain_from_env() -> Option<String> {
    std::env::var(FAILURE_DOMAIN_VAR)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// The unit descriptors this node follows, when units are placed per
/// tenant (made when the engine opens; filled by [`follow_units`]).
static UNITS: OnceLock<Arc<sprig_catalog::units::UnitDirectory>> = OnceLock::new();

/// The unit descriptors this node follows, when units are placed per
/// tenant.
#[must_use]
pub fn unit_directory() -> Option<Arc<sprig_catalog::units::UnitDirectory>> {
    UNITS.get().cloned()
}

/// On one replica of several: records this node in the DC tier's node
/// registry (`nodes/<raft id>`, retried until it lands), and with units
/// placed per tenant keeps this node's unit descriptors equal to the DC
/// tier's until `stop`, dropping this node's replicas of a tenant's units
/// once its descriptor is deleted (the tenant is gone; also, once at
/// start, those of tenants deleted while this node was down). Does nothing
/// on a one-node engine.
pub fn start_placement(
    dc: Arc<dyn sprig_meta::MetaStore>,
    global: Arc<dyn sprig_meta::MetaStore>,
    stop: sprig_common::CancellationToken,
    gateway_id: String,
    addr: String,
) {
    let Some(id) = raft_id_from_env() else {
        return;
    };
    let rec = sprig_catalog::units::NodeRecord {
        id,
        gateway_id,
        addr,
        failure_domain: failure_domain_from_env().unwrap_or_default(),
        placement: placement_from_env(),
        state: "active".to_owned(),
        version: crate::ops::node_version(),
        since_ms: 0,
        reason: String::new(),
    };
    crate::ops::start(id, &dc, stop.clone());
    let store = Arc::clone(&dc);
    let until = stop.clone();
    tokio::spawn(async move {
        let mut backoff = std::time::Duration::from_millis(200);
        while !until.is_cancelled() {
            // A draining or removed node keeps that state across its restart.
            match sprig_catalog::units::register_starting_node(store.as_ref(), &rec).await {
                Ok(r) => {
                    crate::ops::nodes::note_own(r);
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "node registry: this node's record wasn't written; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(5));
                }
            }
        }
    });
    if let Some(dir) = unit_directory() {
        let _ = DC_STORE.set(Arc::clone(&dc));
        dir.on_removed(|unit| {
            // Off the watch's task: dropping clears the units' keys.
            tokio::task::spawn_blocking(move || drop_tenant_units(tenant_of_unit(unit)));
        });
        tokio::spawn(sweep_deleted(
            Arc::clone(&dir),
            Arc::clone(&dc),
            global,
            stop.clone(),
        ));
        // Extra read copies: the leader keeps its units' learners equal to
        // their descriptors'.
        tokio::spawn(crate::read_copies::follow(
            Arc::clone(&dir),
            Arc::clone(&dc),
            stop.clone(),
        ));
        tokio::spawn(dir.follow(dc, stop));
    }
}

/// This node's Raft node, once replication started.
#[must_use]
pub fn raft_node() -> Option<sprig_raft::Node> {
    RAFT.get().map(|r| r.node.node().clone())
}

/// Drops this node's replicas of `tenant`'s units (its own and its
/// partition groups), whose descriptor was deleted with the tenant: each
/// group leaves this node with a tombstone, and the unit's keys here are
/// cleared. Returns how many it dropped.
fn drop_tenant_units(tenant: u64) -> usize {
    let (Some(raft), Ok(engine)) = (RAFT.get(), engine()) else {
        return 0;
    };
    if tenant == 0 {
        return 0;
    }
    let node = raft.node.node();
    let snaps = sprig_raft::EngineSnapshots::new(engine);
    let mut n = 0;
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 || tenant_of_unit(g) != tenant {
            continue;
        }
        match node.drop_group(g) {
            Ok(true) => {
                n += 1;
                if let Err(e) =
                    sprig_raft::UnitSnapshots::ingest(&snaps, sprig_engine::UnitId(g), 0, &[])
                {
                    tracing::warn!(unit = g, error = %e, "engine: a deleted tenant's unit keys weren't cleared");
                }
                crate::ops::replicate::forget_streams(g);
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(unit = g, error = %e, "engine: a deleted tenant's unit replica wasn't dropped");
            }
        }
    }
    if n > 0 {
        tracing::info!(
            tenant,
            units = n,
            "engine: dropped this node's replicas of a deleted tenant's units"
        );
    }
    n
}

/// Once the descriptors are listed: drops the units this node holds whose
/// tenant has neither a descriptor nor a directory entry any more (deleted
/// while this node was down, so no watch told it).
async fn sweep_deleted(
    dir: Arc<sprig_catalog::units::UnitDirectory>,
    dc: Arc<dyn sprig_meta::MetaStore>,
    global: Arc<dyn sprig_meta::MetaStore>,
    stop: sprig_common::CancellationToken,
) {
    while !dir.is_ready() {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
        }
    }
    let Some(raft) = RAFT.get() else {
        return;
    };
    let tenants: std::collections::BTreeSet<u64> = raft
        .node
        .node()
        .groups()
        .into_iter()
        .filter(|g| *g < sprig_engine::NODE_UNIT.0)
        .map(tenant_of_unit)
        .filter(|t| *t != 0 && dir.get(*t).is_none())
        .collect();
    for t in tenants {
        // Both read again, linearizably: a tenant being created has its
        // directory entry before its descriptor, and its descriptor
        // before any write to its unit.
        if !matches!(sprig_catalog::units::read(dc.as_ref(), t).await, Ok(None)) {
            continue;
        }
        let Ok(key) = global.tier().key(&format!("tenants/{t}")) else {
            continue;
        };
        if matches!(
            global
                .get(&key, sprig_meta::Consistency::Linearizable)
                .await,
            Ok(None)
        ) {
            let _ = tokio::task::spawn_blocking(move || drop_tenant_units(t)).await;
        }
    }
}

/// The peer service's client identity (`[tls]`), for catalog
/// transactions this node sends to another node's peer service.
static PEER_PEMS: OnceLock<Option<sprig_catalog::records::remote::PeerPems>> = OnceLock::new();

/// Records this process's peer identity (`None`: plaintext, development).
/// Call once at start, before the first catalog write.
pub fn set_peer_pems(p: Option<sprig_catalog::records::remote::PeerPems>) {
    let _ = PEER_PEMS.set(p);
}

/// The peer identity [`set_peer_pems`] recorded.
#[must_use]
pub fn peer_pems() -> Option<sprig_catalog::records::remote::PeerPems> {
    PEER_PEMS.get().cloned().flatten()
}

/// The DC tier, for reading a descriptor the watch hasn't delivered yet.
static DC_STORE: OnceLock<Arc<dyn sprig_meta::MetaStore>> = OnceLock::new();

/// Makes sure the directory knows `tenant`'s descriptor: when the watch
/// hasn't delivered it yet (a tenant created a moment ago), reads it from
/// the DC tier. Returns `Some(false)` only when that read found none.
async fn know_unit(tenant: u64) -> Option<bool> {
    let (Some(dir), Some(dc)) = (unit_directory(), DC_STORE.get()) else {
        return None;
    };
    if dir.get(tenant).is_some() {
        return Some(true);
    }
    match sprig_catalog::units::read(dc.as_ref(), tenant).await {
        Ok(Some(d)) => {
            dir.put(d);
            Some(true)
        }
        Ok(None) => Some(false),
        Err(e) => {
            tracing::debug!(tenant, error = %e, "unit descriptor unreadable");
            None
        }
    }
}

/// The tenant a unit belongs to: its own, or a partition group's tenant.
pub(crate) fn tenant_of_unit(unit: u64) -> u64 {
    sprig_streams::seg::group_of_unit(unit).map_or(unit, |(t, _)| t)
}

/// Where a unit starts, from the descriptors: its tenant's voters; tenant
/// 0's (data no tenant owns) on every node; anything else not known yet.
/// A unit not known yet has its descriptor read from the DC tier on
/// `rt` (once per tenant every 2 s at most): a new tenant's home node
/// campaigns as soon as the descriptor is written, which can be before
/// this node's watch delivers it, and its next try then finds this node
/// ready to vote.
fn placement_of(
    dir: &sprig_catalog::units::UnitDirectory,
    unit: u64,
    rt: &tokio::runtime::Handle,
) -> sprig_raft::UnitPlacement {
    let tenant = tenant_of_unit(unit);
    match dir.get(tenant) {
        Some(d) => sprig_raft::UnitPlacement::Voters(d.voters),
        None if tenant == 0 => sprig_raft::UnitPlacement::Static,
        None => {
            if unit < sprig_engine::NODE_UNIT.0 && fetch_due(tenant) {
                rt.spawn(async move {
                    know_unit(tenant).await;
                });
            }
            sprig_raft::UnitPlacement::Unknown
        }
    }
}

/// Whether to read `tenant`'s descriptor now: not within 2 s of the last
/// read started for it.
fn fetch_due(tenant: u64) -> bool {
    static LAST: OnceLock<std::sync::Mutex<std::collections::HashMap<u64, std::time::Instant>>> =
        OnceLock::new();
    let mut last = LAST
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let now = std::time::Instant::now();
    if last
        .get(&tenant)
        .is_some_and(|t| now.duration_since(*t) < std::time::Duration::from_secs(2))
    {
        return false;
    }
    if last.len() > 10_000 {
        last.retain(|_, t| now.duration_since(*t) < std::time::Duration::from_secs(2));
    }
    last.insert(tenant, now);
    true
}

/// Syncs of the Raft log that may run at once (`SPRIGDB_ENGINE_RAFT_SYNC_PIPELINE`).
pub const RAFT_SYNC_PIPELINE_VAR: &str = "SPRIGDB_ENGINE_RAFT_SYNC_PIPELINE";
/// Benchmarks only: simulated log syncs, in microseconds.
pub const SIM_SYNC_US_VAR: &str = "SPRIGDB_ENGINE_SIM_SYNC_US";
/// Benchmarks only: simulated syncs one at a time.
pub const SIM_SYNC_SERIAL_VAR: &str = "SPRIGDB_ENGINE_SIM_SYNC_SERIAL";

/// Syncs of the Raft log that may run at once by default. On macOS a sync
/// is a full flush of the whole drive (`F_FULLFSYNC`), so overlapping them
/// gains nothing there (measured 10% slower); everywhere else, and with
/// simulated syncs, [`sprig_raft::logstore::SYNC_PIPELINE`].
fn default_sync_pipeline() -> usize {
    if cfg!(target_os = "macos") && !sprig_engine::log::syncs_simulated() {
        0
    } else {
        sprig_raft::logstore::SYNC_PIPELINE
    }
}

/// Turns on simulated syncs when [`SIM_SYNC_US_VAR`] is set (benchmarks).
fn simulate_syncs_from_env() {
    let Some(us) = std::env::var(SIM_SYNC_US_VAR)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
    else {
        return;
    };
    let serial =
        std::env::var(SIM_SYNC_SERIAL_VAR).is_ok_and(|v| v.trim().eq_ignore_ascii_case("true"));
    if sprig_engine::log::simulate_syncs(sprig_engine::log::SimSyncs {
        delay: std::time::Duration::from_micros(us),
        serial,
    }) {
        tracing::warn!(
            us,
            serial,
            "engine log syncs are SIMULATED: nothing is durable (benchmarks only)"
        );
    }
}

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
    let me = raft.node.node().id();
    match raft.node.node().status(unit) {
        // Placed per tenant: a unit this node holds no replica of yet is
        // led where its descriptor says, or by no one known.
        None if raft.placed => match preferred(&raft.voters, unit) {
            Some(p) if p == me => UnitLeader::Here,
            Some(p) => UnitLeader::Node(p),
            None => match raft.node.node().start_voters(unit) {
                Some(_) => UnitLeader::Any,
                None => UnitLeader::Unknown,
            },
        },
        None => match preferred_leader(&raft.voters, unit) {
            Some(p) if p == me => UnitLeader::Here,
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

/// The replica that should lead `unit` on this node's cluster: with units
/// placed per tenant, a tenant's unit is led by its descriptor's home and
/// its partition groups spread over the tenant's own voters; otherwise as
/// [`preferred_leader`] over every member.
fn preferred(all: &[u64], unit: u64) -> Option<u64> {
    // A node that is draining, gone or out of clock bounds is nobody's
    // preferred leader: its units stay wherever their leadership went.
    // Nor is anyone while a voter is on a lost node: leadership stays put
    // until the unit's leader has replaced it (a transfer would stall the
    // membership change).
    if crate::ops::replicate::short_of_copies(unit) {
        return None;
    }
    preferred_any(all, unit).filter(|n| crate::ops::nodes::takes_leadership(*n))
}

fn preferred_any(all: &[u64], unit: u64) -> Option<u64> {
    if let Some(dir) = unit_directory() {
        let d = dir.get(tenant_of_unit(unit))?;
        if sprig_streams::seg::group_of_unit(unit).is_some() {
            let mut voters = d.voters;
            voters.sort_unstable();
            return preferred_leader(&voters, unit);
        }
        return Some(d.home);
    }
    preferred_leader(all, unit)
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
            simulate_syncs_from_env();
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
#[allow(clippy::too_many_lines)] // start-up, step by step
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
    let tls = raft_tls(r.id)?;
    let mtls = tls.is_some();
    // Before anything listens: plain Raft is asked for by name, or not run.
    if let Some(why) = plain_raft_refused(r, mtls, |k| std::env::var(k).ok())? {
        return Err(raft_err(why));
    }
    if !mtls {
        tracing::warn!(
            node = r.id,
            listen = %r.listen,
            "engine replication: Raft traffic is plain TCP that nothing authenticates \
             (development and test clusters only)"
        );
    }
    let on_peer = raft_on_peer_from(|k| std::env::var(k).ok())?;
    let transport = sprig_raft::tcp::TcpTransport::new(r.id, rt.handle().clone(), tls);
    transport.set_preface(on_peer);
    for (pid, addr) in &r.peers {
        transport.set_peer(*pid, *addr);
    }
    let notes_dir = dir.with_file_name("raft-notes");
    let store = sprig_raft::EngineLogStore::open(dir, SyncMode::Full, None)
        .map_err(|e| raft_err(e.to_string()))?;
    store.set_sync_pipeline(
        std::env::var(RAFT_SYNC_PIPELINE_VAR)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(default_sync_pipeline),
    );
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
    let placed = placement_from_env();
    let placement = if placed {
        let dir = Arc::clone(UNITS.get_or_init(sprig_catalog::units::UnitDirectory::new));
        let fetch_rt = rt.handle().clone();
        Some(sprig_raft::PlacementLookup::new(move |g| {
            placement_of(&dir, g, &fetch_rt)
        }))
    } else {
        None
    };
    let node = sprig_raft::EngineNode::open(
        engine,
        sprig_raft::EngineNodeOptions {
            id: r.id,
            config: sprig_raft::RaftConfig {
                placement,
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
    // On the peer listener, which binds later: peers retry until it does.
    let bound = if on_peer {
        "peer listener".to_owned()
    } else {
        std::thread::scope(|s| {
            s.spawn(|| handle.block_on(transport.listen(listen, raft_node)))
                .join()
                .map_err(|_| raft_err("the listener thread panicked".into()))
        })?
        .map_err(|e| raft_err(format!("raft listener on {listen}: {e}")))?
        .to_string()
    };
    node.spawn().map_err(|e| raft_err(e.to_string()))?;
    let mut voters: Vec<u64> = r.peers.iter().map(|(p, _)| *p).collect();
    voters.sort_unstable();
    let prefer = voters.clone();
    node.prefer_leaders(
        Arc::new(move |u: sprig_engine::UnitId| preferred(&prefer, u.0)),
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(10),
    )
    .map_err(|e| raft_err(e.to_string()))?;
    tracing::info!(id = r.id, raft = %bound, voters = r.peers.len(), join = r.join, placed, mtls, stream_groups = groups_from(|k| std::env::var(k).ok()), stream_write_once = write_once, "engine units replicate through raft");
    follow_cut_file(Arc::clone(&transport));
    let _ = RAFT.set(Raft {
        node,
        transport,
        voters,
        placed,
        on_peer,
    });
    Ok(())
}

/// Loads the Raft certificates again from [`RAFT_TLS_DIR_VAR`] (renewed
/// ones) into the running transport: new connections use them, and
/// outgoing ones reconnect with them. `Ok(false)` when Raft doesn't run
/// with TLS.
///
/// # Errors
/// The files don't load, or the certificate doesn't name this node.
pub fn reload_raft_tls() -> Result<bool> {
    let (Some(raft), Some(id)) = (RAFT.get(), raft_id_from_env()) else {
        return Ok(false);
    };
    let Some(tls) = raft_tls(id)? else {
        return Ok(false);
    };
    raft.transport
        .set_tls(tls)
        .map(|()| true)
        .map_err(|e| SprigError::internal(format!("{RAFT_TLS_DIR_VAR}: {e}")))
}

/// Revoked node certificates by serial number: the Raft transport refuses
/// peers presenting one and closes their connections.
pub fn set_raft_revoked(serials: std::collections::BTreeSet<Vec<u8>>) {
    if let Some(raft) = RAFT.get() {
        raft.transport.set_revoked(serials);
    }
}

/// The process's Raft node and transport, once started.
struct Raft {
    node: sprig_raft::EngineNode,
    transport: Arc<sprig_raft::tcp::TcpTransport>,
    /// Every member's id, in order.
    voters: Vec<u64>,
    /// Units are placed per tenant ([`PLACEMENT_VAR`]).
    placed: bool,
    /// Raft traffic arrives on the peer listener ([`RAFT_TRANSPORT_VAR`]).
    on_peer: bool,
}

/// With Raft on the peer listener: the transport and the node its
/// connections feed, for the peer listener to hand Raft connections to.
#[must_use]
pub fn raft_on_peer() -> Option<(Arc<sprig_raft::tcp::TcpTransport>, sprig_raft::Node)> {
    RAFT.get()
        .filter(|r| r.on_peer)
        .map(|r| (Arc::clone(&r.transport), r.node.node().clone()))
}

/// For the catalog peer service: `Ok` when this node may run `tenant`'s
/// catalog transactions, that is it leads the tenant's unit (or the engine
/// doesn't replicate). A member that holds no replica yet starts it and
/// stands for election; an election in progress is waited out for up to
/// 3 s. Otherwise 39110 naming the leader when known (`leader node <id>`),
/// so the caller goes there.
///
/// # Errors
/// 39110 (retryable) when another node leads the unit or none is known;
/// 39072 when units are placed per tenant and the tenant has no unit
/// descriptor (not placed yet, or deleted).
pub async fn lead_catalog(tenant: u64) -> Result<()> {
    let Some(raft) = RAFT.get() else {
        return Ok(());
    };
    let unit = sprig_engine::Engine::unit_of_tenant(tenant);
    // A tenant with no descriptor in the DC tier has no unit anywhere yet,
    // so no records: said at once (39072), not after waiting for a leader
    // nobody can elect.
    if raft.placed
        && tenant != 0
        && unit != sprig_engine::NODE_UNIT
        && know_unit(tenant).await == Some(false)
    {
        return Err(SprigError::new(
            sprig_common::SprigCode::TenantNotReady,
            format!("tenant {tenant}'s unit isn't placed on any node yet"),
        ));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let engine = engine()?;
    let node = raft.node.node();
    let mut started = false;
    let mut asked = std::time::Instant::now();
    loop {
        if engine.leader_epoch(unit) > 0 {
            return Ok(());
        }
        let leader = if let Some(st) = node.status(unit.0) {
            // A first election whose pre-votes found no voter ready (their
            // watch hadn't delivered the new unit's descriptor yet) is
            // asked again soon, rather than after a whole election timeout,
            // in which a member's own timer could win it first and the
            // leadership then move back here.
            if (started || preferred(&raft.voters, unit.0) == Some(node.id()))
                && st.leader.is_none()
                && st.term == sprig_raft::INIT_TERM
                && asked.elapsed() >= std::time::Duration::from_millis(150)
            {
                asked = std::time::Instant::now();
                let _ = node.campaign(unit.0);
            }
            st.leader.filter(|l| *l != node.id())
        } else {
            // A member starts the unit; a unit nobody started on a
            // one-cluster membership starts here too. Only the unit's home
            // (or, with no home, this node) campaigns at once: another
            // member points the caller at the home, so a new unit's first
            // election isn't won elsewhere and its leadership then moved
            // (a transfer during the tenant's bootstrap). Its own election
            // timer still elects a leader if the home is down.
            if !started && let Some(voters) = node.start_voters(unit.0) {
                started = true;
                let home = if raft.placed {
                    preferred(&raft.voters, unit.0).filter(|h| *h != node.id())
                } else {
                    None
                };
                if let Some(h) = home {
                    if let Err(e) = node.create_group(unit.0, &voters) {
                        tracing::debug!(tenant, error = %e, "catalog: could not start the tenant's unit");
                    }
                    return Err(SprigError::new(
                        sprig_common::SprigCode::ShardMoved,
                        format!(
                            "tenant {tenant}'s unit is new; its home, leader node {h}, leads it first; retry there"
                        ),
                    ));
                }
                if let Err(e) = raft.node.start_unit(unit, &voters) {
                    tracing::debug!(tenant, error = %e, "catalog: could not start the tenant's unit");
                }
                continue;
            }
            if raft.placed {
                preferred(&raft.voters, unit.0).filter(|p| *p != node.id())
            } else {
                None
            }
        };
        if let Some(l) = leader {
            return Err(SprigError::new(
                sprig_common::SprigCode::ShardMoved,
                format!("tenant {tenant}'s unit is led by leader node {l}; retry there"),
            ));
        }
        if std::time::Instant::now() >= deadline {
            return Err(SprigError::new(
                sprig_common::SprigCode::ShardMoved,
                format!("tenant {tenant}'s unit has no leader yet; retry"),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

static RAFT: OnceLock<Raft> = OnceLock::new();

/// This process's Raft node over the engine, once replication started.
pub(crate) fn engine_raft() -> Option<&'static sprig_raft::EngineNode> {
    RAFT.get().map(|r| &r.node)
}

/// Whether units are placed per tenant on this replicated node.
pub(crate) fn raft_placed() -> bool {
    RAFT.get().is_some_and(|r| r.placed)
}

static NOTE_BLOBS: OnceLock<Arc<sprig_engine::log::blobs::NoteBlobs>> = OnceLock::new();

/// The Raft log's note blobs on a replicated node (after [`engine`]
/// opened), for the stream log's segments to refer to.
#[must_use]
pub fn note_blobs() -> Option<Arc<sprig_engine::log::blobs::NoteBlobs>> {
    NOTE_BLOBS.get().cloned()
}

/// The node-local Admin API route that lists the engine units this node
/// holds ([`units_report`]).
pub const UNITS_PATH: &str = "/admin/v1/engine/units";

/// The units this node holds a replica of, with their membership and
/// leader as this node knows them: `{"node": id, "placed": bool,
/// "units": [{"unit", "home", "voters", "learners", "leader", "leading", "term",
/// "committed", "applied"}]}`. On a one-node engine, `{"node": null}`.
#[must_use]
pub fn units_report() -> serde_json::Value {
    let Some(raft) = RAFT.get() else {
        return serde_json::json!({"node": null, "placed": false, "units": []});
    };
    let node = raft.node.node();
    let mut groups = node.groups();
    groups.sort_unstable();
    let units: Vec<serde_json::Value> = groups
        .into_iter()
        .filter_map(|g| node.status(g))
        .map(|st| {
            let home = unit_directory()
                .and_then(|d| d.get(tenant_of_unit(st.group)))
                .map(|d| d.home);
            serde_json::json!({
                "unit": st.group,
                "home": home,
                "voters": st.voters,
                "learners": st.learners,
                "leader": st.leader,
                "leading": st.is_leader,
                "term": st.term,
                "committed": st.committed,
                "applied": st.applied,
            })
        })
        .collect();
    serde_json::json!({"node": node.id(), "placed": raft.placed, "units": units})
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
/// unit applies an entry that writes the tenant's catalog records while
/// this node doesn't lead it. A follower's catalog records change through
/// replication, not through its own commits, so its cache would otherwise
/// keep what it first loaded (a tenant without the users added later, say)
/// and serve that after it takes the unit's leadership. Entries that write
/// only data (rows, documents, stream appends) leave the cache alone. Does
/// nothing on a one-node engine.
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
    fn applied(&self, unit: sprig_engine::UnitId, entries: &[sprig_engine::log::LogEntry]) {
        // Partition groups carry stream appends only, no catalog rows.
        if unit == sprig_engine::NODE_UNIT || sprig_streams::seg::group_of_unit(unit.0).is_some() {
            return;
        }
        let Some(e) = self.engine.upgrade() else {
            return;
        };
        if e.leader_epoch(unit) == 0 && writes_catalog(unit.0, entries) {
            let _ = self.cache.evict(sprig_common::ids::TenantId(unit.0));
        }
    }
}

/// Whether any of `entries` writes `tenant`'s catalog records (an entry
/// that doesn't decode counts as one that does).
fn writes_catalog(tenant: u64, entries: &[sprig_engine::log::LogEntry]) -> bool {
    let prefix = crate::tenant::catalog_records::catalog_prefix(tenant);
    entries
        .iter()
        .filter(|e| !e.data.is_empty())
        .any(|e| sprig_engine::log::payload::writes_under(&e.data, &prefix).unwrap_or(true))
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

    /// A node that replicates never takes Raft traffic over plain TCP
    /// unless its operator asked for that by name.
    #[test]
    fn plain_raft_needs_the_insecure_switch() {
        let three = "1=10.0.0.1:26171,2=10.0.0.2:26171,3=10.0.0.3:26171";
        let local = "1=127.0.0.1:26171,2=127.0.0.1:26172,3=127.0.0.1:26173";
        let setup = |vars: &[(&str, &str)]| {
            raft(vars)
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("replicated"))
        };
        let check = |r: &RaftSetup, tls: bool, switch: Option<&str>| {
            plain_raft_refused(r, tls, |k| {
                (k == INSECURE_CLUSTER_VAR)
                    .then(|| switch.map(str::to_owned))
                    .flatten()
            })
        };
        for peers in [three, local] {
            let r = setup(&[(RAFT_ID_VAR, "2"), (RAFT_PEERS_VAR, peers)]);
            // No TLS, nothing said: refused, and the message names both
            // ways out.
            let why = check(&r, false, None)
                .unwrap_or_else(|e| panic!("{e}"))
                .unwrap_or_else(|| panic!("{peers}: a plain cluster started"));
            assert!(
                why.contains(RAFT_TLS_DIR_VAR) && why.contains(INSECURE_CLUSTER_VAR),
                "{why}"
            );
            for off in ["false", "0", "no", "off", "", " "] {
                assert!(
                    check(&r, false, Some(off))
                        .unwrap_or_else(|e| panic!("{e}"))
                        .is_some()
                );
            }
            // Mutual TLS, or the switch by name.
            assert_eq!(
                check(&r, true, None).unwrap_or_else(|e| panic!("{e}")),
                None
            );
            for on in ["true", "TRUE", "1", "yes", "on", " true "] {
                assert_eq!(
                    check(&r, false, Some(on)).unwrap_or_else(|e| panic!("{e}")),
                    None
                );
            }
            // A value that is neither isn't read as yes.
            assert!(check(&r, false, Some("maybe")).is_err());
        }
        // One node alone on loopback has nobody to listen to; on another
        // address, or joining a cluster, it does.
        let alone = setup(&[(RAFT_ID_VAR, "1"), (RAFT_PEERS_VAR, "1=127.0.0.1:26171")]);
        assert_eq!(
            check(&alone, false, None).unwrap_or_else(|e| panic!("{e}")),
            None
        );
        let open = setup(&[
            (RAFT_ID_VAR, "1"),
            (RAFT_PEERS_VAR, "1=127.0.0.1:26171"),
            (RAFT_LISTEN_VAR, "0.0.0.0:26171"),
        ]);
        assert!(
            check(&open, false, None)
                .unwrap_or_else(|e| panic!("{e}"))
                .is_some()
        );
        let joining = setup(&[
            (RAFT_ID_VAR, "1"),
            (RAFT_PEERS_VAR, "1=127.0.0.1:26171"),
            (RAFT_JOIN_VAR, "true"),
        ]);
        assert!(
            check(&joining, false, None)
                .unwrap_or_else(|e| panic!("{e}"))
                .is_some()
        );
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
    fn a_cut_file_lists_raft_ids() {
        assert_eq!(cut_ids("2, 3\n"), [2, 3].into());
        assert_eq!(cut_ids("4 5,x"), [4, 5].into());
        assert!(cut_ids("").is_empty());
    }

    #[test]
    fn only_catalog_writes_drop_a_followers_cached_catalog() {
        use sprig_engine::Cf;
        use sprig_engine::log::{LogEntry, Payload};
        let tenant = 28_306_000_001;
        let entry = |key: Vec<u8>| {
            let mut p = Payload::new();
            p.put(
                Cf::Data,
                &sprig_engine::keys::versioned(&key, sprig_engine::Ts(9)),
                b"v",
            );
            LogEntry::new(6, 7, p.into_bytes())
        };
        let mut cat = crate::tenant::catalog_records::catalog_prefix(tenant);
        cat.extend_from_slice(b"users/1");
        let mut row = sprig_engine::keys::KeyPrefix {
            tenant,
            shard: 0,
            object: 5,
            kind: sprig_engine::keys::kind::PRIMARY,
            index: 0,
        }
        .to_vec();
        row.push(1);
        let other = crate::tenant::catalog_records::catalog_prefix(tenant + 1);
        assert!(writes_catalog(
            tenant,
            &[entry(row.clone()), entry(cat.clone())]
        ));
        assert!(!writes_catalog(tenant, &[entry(row)]));
        assert!(!writes_catalog(tenant, &[entry(other)]));
        // An empty entry (a new leader's) writes nothing; a malformed one
        // counts as a catalog write.
        assert!(!writes_catalog(tenant, &[LogEntry::new(6, 8, Vec::new())]));
        assert!(writes_catalog(tenant, &[LogEntry::new(6, 9, vec![1, 9])]));
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
