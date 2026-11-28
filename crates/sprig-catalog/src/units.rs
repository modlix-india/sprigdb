//! Where each tenant's engine unit lives across nodes: unit descriptors.
//!
//! ```text
//! /sprig/dc/<dc_id>/units/<tenant_id>/<unit_id>   {voters, learners, home, generation}
//! ```
//!
//! The controller, as the placement driver, writes a tenant's descriptor
//! once (guarded by `Missing`) before anything is written to the unit, so
//! every rerun of a creation (after a crash, on another controller) finds
//! the same three nodes. Nodes follow the descriptors with a watch
//! ([`UnitDirectory::follow`]): a node a descriptor lists starts the unit's
//! Raft group with exactly those voters when it first hears of it, and
//! prefers the descriptor's home node as the unit's leader. The voters
//! listed are the unit's first membership; after that, Raft's own
//! configuration is the truth and the descriptor is a copy for discovery
//! (later moves raise `generation`).
//!
//! Which node leads a unit is never written here: callers keep a cache
//! corrected by "not the leader, try N" answers.
//!
//! A descriptor is deleted only with its tenant (after the tenant's catalog
//! records are gone): a node told of the deletion
//! ([`UnitDirectory::on_removed`]) drops its replicas of the tenant's units.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Consistency, Key, ListOpts, MetaStore, Op, Record, Versioned, WatchEvent};

/// A unit's placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitDescriptor {
    /// The tenant.
    pub tenant: u64,
    /// The unit (a whole tenant's unit is the tenant id).
    pub unit: u64,
    /// The voters' node ids (Raft ids), in order.
    pub voters: Vec<u64>,
    /// Non-voting copies.
    #[serde(default)]
    pub learners: Vec<u64>,
    /// The node that should lead the unit: where the tenant's sessions run.
    pub home: u64,
    /// Raised by every forced reconfiguration and move.
    #[serde(default)]
    pub generation: u64,
    /// A planned replica move in progress ([`start_move`]): the unit's
    /// leader copies it to `to` and then takes `from` out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moving: Option<UnitMove>,
    /// The last replica move that finished (`since_ms`: when it finished):
    /// the balancer neither moves the unit again within its cooldown nor
    /// back to the node it left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_move: Option<UnitMove>,
}

impl Versioned for UnitDescriptor {
    const V: u32 = 1;
}

/// A planned move of one replica of a unit from one node to another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnitMove {
    /// The voter that leaves.
    pub from: u64,
    /// The node that takes its place (a learner until it has caught up).
    pub to: u64,
    /// When the move was asked for (Unix milliseconds).
    #[serde(default)]
    pub since_ms: u64,
    /// The unit's home before the move (restored when the move is given up).
    #[serde(default)]
    pub home_was: u64,
    /// Who asked and why (`balancer: ...`, `operator: ...`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

/// A node of the DC's engine cluster: `/sprig/dc/<dc_id>/nodes/<raft id>`,
/// written by the node when it starts. Placement takes nodes that have
/// this record and a ready gateway registration with the same Raft id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    /// Its Raft id.
    pub id: u64,
    /// The gateway process running it (`[node] id`).
    pub gateway_id: String,
    /// The host other nodes reach it at.
    pub addr: String,
    /// Where other nodes reach its Raft traffic (`host:port`): nodes learn
    /// a node added after them from this. Empty: only the peers listed at
    /// start reach it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub raft_addr: String,
    /// Its failure domain (rack, zone); empty: a domain of its own.
    #[serde(default)]
    pub failure_domain: String,
    /// It places units per tenant (`SPRIGDB_ENGINE_PLACEMENT=on`).
    #[serde(default)]
    pub placement: bool,
    /// `active` (takes units); `draining` (keeps its replicas, leads
    /// none); `dead` (the controller found it gone for
    /// `replication.dead_node_minutes`: its units are re-replicated);
    /// `removed` (an operator removed it: re-replicated at once).
    pub state: String,
    /// Its binary's version, as it reported it at start.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    /// When the node last started or `state` last changed (Unix
    /// milliseconds; 0: unknown).
    #[serde(default)]
    pub since_ms: u64,
    /// Why `state` last changed (an operator's reason, or the loop's).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// Replication features its binary has ([`RAFT_SNAPSHOT_CHUNKS`]); a
    /// record without one is a node of an older build.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub raft_features: Vec<String>,
}

/// The node takes unit snapshots in chunks ahead of their message. Leaders
/// send a node without it each snapshot whole, inside its message, as
/// builds before chunked snapshots did (a rolling upgrade).
pub const RAFT_SNAPSHOT_CHUNKS: &str = "snapshot-chunks";

impl NodeRecord {
    /// Whether the node takes unit snapshots in chunks.
    #[must_use]
    pub fn takes_snapshot_chunks(&self) -> bool {
        self.raft_features.iter().any(|f| f == RAFT_SNAPSHOT_CHUNKS)
    }
}

impl Versioned for NodeRecord {
    const V: u32 = 1;
}

/// A node that takes units and leaderships.
pub const NODE_ACTIVE: &str = "active";
/// A node leaving its leaderships (a planned stop): keeps its replicas.
pub const NODE_DRAINING: &str = "draining";
/// A node gone for longer than `replication.dead_node_minutes`.
pub const NODE_DEAD: &str = "dead";
/// A node an operator removed.
pub const NODE_REMOVED: &str = "removed";

impl NodeRecord {
    /// Whether its replicas must be replaced elsewhere.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.state == NODE_DEAD || self.state == NODE_REMOVED
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Writes (or rewrites) a node's record.
///
/// # Errors
/// The store's.
pub async fn register_node(store: &dyn MetaStore, n: &NodeRecord) -> Result<()> {
    let key = store.tier().key(&format!("nodes/{}", n.id))?;
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: Record::new(n.clone()).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .map(|_| ())
}

/// Records a starting node: `n` as given, except that a node an operator
/// is draining or removed keeps that state (a restart in the middle of a
/// drain stays drained until undrained; a removed node never takes units
/// again). A `dead` node that starts is `active` again. Returns the record
/// stored.
///
/// # Errors
/// The store's.
pub async fn register_starting_node(store: &dyn MetaStore, n: &NodeRecord) -> Result<NodeRecord> {
    let key = store.tier().key(&format!("nodes/{}", n.id))?;
    for _ in 0..20 {
        let old = store.get(&key, Consistency::Linearizable).await?;
        let mut rec = n.clone();
        // `since_ms` is when the node last started or changed state (the
        // controller counts a node as seen then).
        rec.since_ms = now_ms();
        let guard = if let Some(kv) = &old {
            if let Ok(prev) = Record::<NodeRecord>::decode(&kv.value) {
                let prev = prev.body;
                if prev.state == NODE_DRAINING || prev.state == NODE_REMOVED {
                    rec.state = prev.state;
                    rec.reason = prev.reason;
                } else {
                    "started".clone_into(&mut rec.reason);
                }
            }
            Cmp::ModRev(key.clone(), kv.mod_revision)
        } else {
            Cmp::Missing(key.clone())
        };
        let r = store
            .txn(sprig_meta::Txn {
                when: vec![guard],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(rec);
        }
    }
    Err(SprigError::new(
        SprigCode::MetaUnavailable,
        "the node's record kept changing while it was written",
    ))
}

/// Moves node `id` to state `to` when its state is one of `from` (any
/// state when `from` is empty), compare-and-set on the record. Returns the
/// record now stored, or `None` when the node has no record; a node whose
/// state isn't in `from` is returned unchanged.
///
/// # Errors
/// The store's.
pub async fn set_node_state(
    store: &dyn MetaStore,
    id: u64,
    from: &[&str],
    to: &str,
    reason: &str,
) -> Result<Option<NodeRecord>> {
    let key = store.tier().key(&format!("nodes/{id}"))?;
    for _ in 0..20 {
        let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        let mut rec = Record::<NodeRecord>::decode(&kv.value)?.body;
        if rec.state == to || (!from.is_empty() && !from.contains(&rec.state.as_str())) {
            return Ok(Some(rec));
        }
        rec.state = to.to_owned();
        rec.reason = reason.to_owned();
        rec.since_ms = now_ms();
        let r = store
            .txn(sprig_meta::Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: Record::new(rec.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(Some(rec));
        }
    }
    Err(SprigError::new(
        SprigCode::MetaUnavailable,
        "the node's record kept changing while its state was set",
    ))
}

/// Rewrites `tenant`'s own unit's descriptor with `f`, compare-and-set,
/// raising its generation. `f` returns `false` to leave it as it is.
/// Returns the descriptor stored (or found), `None` when there is none.
///
/// # Errors
/// The store's.
pub async fn update(
    store: &dyn MetaStore,
    tenant: u64,
    mut f: impl FnMut(&mut UnitDescriptor) -> bool,
) -> Result<Option<UnitDescriptor>> {
    let key = unit_key(store, tenant, tenant)?;
    for _ in 0..20 {
        let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        let mut d = decode(&kv.value)?;
        if !f(&mut d) {
            return Ok(Some(d));
        }
        d.generation += 1;
        let r = store
            .txn(sprig_meta::Txn {
                when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: key.clone(),
                    value: Record::new(d.clone()).encode()?,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await?;
        if r.succeeded {
            return Ok(Some(d));
        }
    }
    Err(SprigError::new(
        SprigCode::MetaUnavailable,
        "the unit descriptor kept changing while it was rewritten",
    ))
}

/// Asks for a replica move of `tenant`'s unit from voter `from` to node
/// `to`, compare-and-set: the descriptor records the move, lists `to` as a
/// learner (so `to` keeps the replica it is given), and names `to` home if
/// `from` was. The unit's leader then does the work and ends the move
/// ([`end_move`]). Returns the descriptor stored.
///
/// # Errors
/// 39792 when the unit has no descriptor, `from` isn't a voter or `to`
/// already holds a copy; 39470 (retryable) while another move of the unit
/// is pending; the store's.
pub async fn start_move(
    store: &dyn MetaStore,
    tenant: u64,
    from: u64,
    to: u64,
    reason: &str,
) -> Result<UnitDescriptor> {
    let invalid = |m: String| SprigError::new(SprigCode::AdminRequestInvalid, m);
    let mut refused: Option<SprigError> = None;
    let r = update(store, tenant, |d| {
        refused = check_move(d, from, to).err();
        if refused.is_some() {
            return false;
        }
        d.moving = Some(UnitMove {
            from,
            to,
            since_ms: now_ms(),
            home_was: d.home,
            reason: reason.to_owned(),
        });
        d.learners.retain(|l| *l != to);
        d.learners.push(to);
        if d.home == from {
            d.home = to;
        }
        true
    })
    .await?;
    if let Some(e) = refused {
        return Err(e);
    }
    r.ok_or_else(|| invalid(format!("tenant {tenant}'s unit has no descriptor")))
}

/// Whether a move of `d`'s unit from `from` to `to` may start now.
///
/// # Errors
/// As [`start_move`].
pub fn check_move(d: &UnitDescriptor, from: u64, to: u64) -> Result<()> {
    let invalid = |m: String| SprigError::new(SprigCode::AdminRequestInvalid, m);
    if let Some(m) = &d.moving {
        return Err(SprigError::new(
            SprigCode::ShardBusy,
            format!(
                "unit {} is already moving from node {} to node {}",
                d.unit, m.from, m.to
            ),
        ));
    }
    if from == to || !d.voters.contains(&from) {
        return Err(invalid(format!(
            "node {from} isn't a voter of unit {} (voters {:?})",
            d.unit, d.voters
        )));
    }
    if d.voters.contains(&to) || d.learners.contains(&to) {
        return Err(invalid(format!(
            "node {to} already holds a copy of unit {}",
            d.unit
        )));
    }
    Ok(())
}

/// How a move ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveEnd {
    /// Raft's voters now (`to` in, `from` out): the descriptor takes them.
    Done(Vec<u64>),
    /// Given up: `to` leaves the learners and the old home comes back.
    Abandoned,
}

/// Ends the pending move of `tenant`'s unit (if it is still the one from
/// `from` to `to`), compare-and-set. Returns the descriptor stored.
///
/// # Errors
/// The store's.
pub async fn end_move(
    store: &dyn MetaStore,
    tenant: u64,
    from: u64,
    to: u64,
    how: &MoveEnd,
) -> Result<Option<UnitDescriptor>> {
    update(store, tenant, |d| {
        let Some(m) = d.moving.clone() else {
            return false;
        };
        if m.from != from || m.to != to {
            return false;
        }
        d.moving = None;
        d.learners.retain(|l| *l != to);
        match how {
            MoveEnd::Done(voters) => {
                let mut v = voters.clone();
                v.sort_unstable();
                d.voters = v;
                d.learners.retain(|l| !d.voters.contains(l));
                d.last_move = Some(UnitMove {
                    since_ms: now_ms(),
                    ..m.clone()
                });
            }
            MoveEnd::Abandoned => {
                if d.home == to && d.voters.contains(&m.home_was) {
                    d.home = m.home_was;
                }
            }
        }
        if !d.voters.contains(&d.home)
            && let Some(first) = d.voters.first()
        {
            d.home = *first;
        }
        true
    })
    .await
}

/// Names `home` the preferred leader of `tenant`'s unit (a leadership
/// move), compare-and-set; `home` must be one of its voters. Returns the
/// descriptor stored.
///
/// # Errors
/// 39792 when there is no descriptor or `home` isn't a voter; the store's.
pub async fn set_home(store: &dyn MetaStore, tenant: u64, home: u64) -> Result<UnitDescriptor> {
    let mut not_voter = false;
    let r = update(store, tenant, |d| {
        not_voter = !d.voters.contains(&home);
        if not_voter || d.home == home {
            return false;
        }
        d.home = home;
        true
    })
    .await?;
    match r {
        Some(_) if not_voter => Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("node {home} isn't a voter of tenant {tenant}'s unit"),
        )),
        Some(d) => Ok(d),
        None => Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("tenant {tenant}'s unit has no descriptor"),
        )),
    }
}

/// The DC's node records, kept in memory by a watch of `nodes/`.
#[derive(Default)]
pub struct NodeDirectory {
    nodes: RwLock<HashMap<u64, NodeRecord>>,
    ready: AtomicBool,
}

impl std::fmt::Debug for NodeDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDirectory")
            .field("nodes", &self.nodes)
            .finish_non_exhaustive()
    }
}

impl NodeDirectory {
    /// An empty directory; [`NodeDirectory::follow`] fills it.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Node `id`'s record.
    #[must_use]
    pub fn get(&self, id: u64) -> Option<NodeRecord> {
        self.nodes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// Every record held, by id.
    #[must_use]
    pub fn all(&self) -> BTreeMap<u64, NodeRecord> {
        self.nodes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Whether the first listing has been read.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Node `id`'s state; `None` when it has no record (or none read yet).
    #[must_use]
    pub fn state(&self, id: u64) -> Option<String> {
        self.get(id).map(|n| n.state)
    }

    /// Records a node (tests, and a node after writing its own).
    pub fn put(&self, n: NodeRecord) {
        self.nodes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(n.id, n);
    }

    /// Keeps the directory equal to `store`'s node records until `stop`.
    pub async fn follow(self: Arc<Self>, store: Arc<dyn MetaStore>, stop: CancellationToken) {
        let mut backoff = Duration::from_millis(100);
        while !stop.is_cancelled() {
            match self.follow_once(store.as_ref(), &stop).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(error = %e, "node records: watch failed; listing again");
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn follow_once(&self, store: &dyn MetaStore, stop: &CancellationToken) -> Result<()> {
        let prefix = store.tier().key("nodes/")?;
        let page = store
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let map: HashMap<u64, NodeRecord> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<NodeRecord>::decode(&kv.value).ok())
            .map(|r| (r.body.id, r.body))
            .collect();
        *self.nodes.write().unwrap_or_else(PoisonError::into_inner) = map;
        self.ready.store(true, Ordering::Release);
        let mut w = store.watch(&prefix, page.revision + 1).await?;
        loop {
            let ev = tokio::select! {
                () = stop.cancelled() => return Ok(()),
                ev = w.next() => ev,
            };
            match ev {
                None => return Err(SprigError::internal("the node record watch ended")),
                Some(Err(e)) => return Err(e),
                Some(Ok(WatchEvent::Put(kv))) => {
                    if let Ok(r) = Record::<NodeRecord>::decode(&kv.value) {
                        self.put(r.body);
                    }
                }
                Some(Ok(WatchEvent::Delete { key, .. })) => {
                    let s = key.as_str();
                    if let Some(id) = s
                        .rsplit_once('/')
                        .and_then(|(_, id)| id.parse::<u64>().ok())
                    {
                        self.nodes
                            .write()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&id);
                    }
                }
                Some(Ok(WatchEvent::Compacted { .. })) => {
                    return Err(SprigError::internal("the node record watch was compacted"));
                }
                Some(Ok(_)) => {}
            }
        }
    }
}

/// Every node record of the DC.
///
/// # Errors
/// The store's.
pub async fn nodes(store: &dyn MetaStore) -> Result<Vec<NodeRecord>> {
    let prefix = store.tier().key("nodes/")?;
    let page = store
        .list(
            &prefix,
            ListOpts {
                limit: 10_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<NodeRecord>::decode(&kv.value).ok())
        .map(|r| r.body)
        .collect())
}

/// The prefix of every descriptor in a DC.
///
/// # Errors
/// The tier's key error.
pub fn all_prefix(store: &dyn MetaStore) -> Result<Key> {
    store.tier().key("units/")
}

/// The prefix of `tenant`'s descriptors.
///
/// # Errors
/// The tier's key error.
pub fn tenant_prefix(store: &dyn MetaStore, tenant: u64) -> Result<Key> {
    store.tier().key(&format!("units/{tenant}/"))
}

/// The key of one descriptor.
///
/// # Errors
/// The tier's key error.
pub fn unit_key(store: &dyn MetaStore, tenant: u64, unit: u64) -> Result<Key> {
    store.tier().key(&format!("units/{tenant}/{unit}"))
}

fn decode(b: &[u8]) -> Result<UnitDescriptor> {
    Record::<UnitDescriptor>::decode(b).map(|r| r.body)
}

/// Writes `desc` unless the unit already has a descriptor, while `guard`
/// holds (the controller's leadership); returns the one stored (the
/// existing one wins, so every rerun of a creation agrees).
///
/// # Errors
/// The store's; 39504 when `guard` failed and nothing is stored.
pub async fn place_once(
    store: &dyn MetaStore,
    desc: &UnitDescriptor,
    guard: &[Cmp],
) -> Result<UnitDescriptor> {
    let key = unit_key(store, desc.tenant, desc.unit)?;
    let value = Record::new(desc.clone()).encode()?;
    let mut when = vec![Cmp::Missing(key.clone())];
    when.extend_from_slice(guard);
    let r = store
        .txn(sprig_meta::Txn {
            when,
            then: vec![Op::Put {
                key: key.clone(),
                value,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    if r.succeeded {
        return Ok(desc.clone());
    }
    match store.get(&key, Consistency::Linearizable).await? {
        Some(kv) => decode(&kv.value),
        None => Err(SprigError::new(
            SprigCode::NotLeader,
            "this controller no longer leads the DC",
        )),
    }
}

/// `tenant`'s own unit's descriptor.
///
/// # Errors
/// The store's.
pub async fn read(store: &dyn MetaStore, tenant: u64) -> Result<Option<UnitDescriptor>> {
    let key = unit_key(store, tenant, tenant)?;
    match store.get(&key, Consistency::Linearizable).await? {
        Some(kv) => decode(&kv.value).map(Some),
        None => Ok(None),
    }
}

/// Every descriptor in the DC, by tenant (its own unit's).
///
/// # Errors
/// The store's.
pub async fn list(store: &dyn MetaStore) -> Result<Vec<UnitDescriptor>> {
    let prefix = all_prefix(store)?;
    let mut out = Vec::new();
    let mut opts = ListOpts {
        limit: 5_000,
        ..ListOpts::default()
    };
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        for kv in &page.kvs {
            match decode(&kv.value) {
                Ok(d) => out.push(d),
                Err(e) => {
                    tracing::warn!(key = %kv.key.as_str(), error = %e, "a unit descriptor doesn't decode");
                }
            }
        }
        match (page.more, page.kvs.last()) {
            (true, Some(last)) => {
                opts.at = Some(page.revision);
                opts.after = Some(last.key.clone());
            }
            _ => return Ok(out),
        }
    }
}

/// Deletes `tenant`'s descriptors (its unit leaves every node).
///
/// # Errors
/// The store's.
pub async fn remove(store: &dyn MetaStore, tenant: u64) -> Result<()> {
    let prefix = tenant_prefix(store, tenant)?;
    let page = store.list(&prefix, ListOpts::default()).await?;
    if page.kvs.is_empty() {
        return Ok(());
    }
    store
        .txn(sprig_meta::Txn {
            when: Vec::new(),
            then: page.kvs.into_iter().map(|kv| Op::Delete(kv.key)).collect(),
            otherwise: Vec::new(),
        })
        .await
        .map(|_| ())
}

/// The tenant of a descriptor key `.../units/<tenant>/<unit>`.
fn tenant_of(key: &Key) -> Option<(u64, u64)> {
    let s = key.as_str();
    let rest = &s[s.rfind("units/")? + "units/".len()..];
    let (t, u) = rest.split_once('/')?;
    Some((t.parse().ok()?, u.parse().ok()?))
}

/// What a directory calls with a unit whose descriptor was deleted.
type RemovedHook = Arc<dyn Fn(u64) + Send + Sync>;

/// The descriptors of a DC, kept in memory by a watch: what a node asks on
/// its Raft thread (so it answers from memory only).
#[derive(Default)]
pub struct UnitDirectory {
    /// By unit id.
    units: RwLock<HashMap<u64, UnitDescriptor>>,
    /// The first full listing has been read.
    ready: AtomicBool,
    /// Called with each unit whose descriptor the watch saw deleted (or
    /// that a new listing no longer has).
    removed: RwLock<Option<RemovedHook>>,
}

impl std::fmt::Debug for UnitDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitDirectory")
            .field("units", &self.units)
            .field("ready", &self.ready)
            .finish_non_exhaustive()
    }
}

impl UnitDirectory {
    /// An empty directory; [`UnitDirectory::follow`] fills it.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The descriptor of `unit`.
    #[must_use]
    pub fn get(&self, unit: u64) -> Option<UnitDescriptor> {
        self.units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .cloned()
    }

    /// Whether the first listing has been read: before it, "not placed"
    /// means "not known yet".
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Records a descriptor (tests, and the controller after writing one).
    pub fn put(&self, d: UnitDescriptor) {
        self.units
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(d.unit, d);
    }

    /// Forgets a unit.
    pub fn forget(&self, unit: u64) {
        self.units
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&unit);
    }

    /// Every descriptor held.
    #[must_use]
    pub fn all(&self) -> Vec<UnitDescriptor> {
        self.units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    /// Per node id: how many units it holds and how many it is home to.
    #[must_use]
    pub fn counts(&self) -> BTreeMap<u64, (usize, usize)> {
        let mut c: BTreeMap<u64, (usize, usize)> = BTreeMap::new();
        for d in self
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            for v in &d.voters {
                c.entry(*v).or_default().0 += 1;
            }
            c.entry(d.home).or_default().1 += 1;
        }
        c
    }

    /// Calls `f` with each unit whose descriptor is deleted from now on
    /// (seen by the watch, or missing from a new listing of a directory
    /// that had it): the unit's tenant is gone.
    pub fn on_removed(&self, f: impl Fn(u64) + Send + Sync + 'static) {
        *self.removed.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(f));
    }

    fn removed(&self, unit: u64) {
        let hook = self
            .removed
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(f) = hook {
            f(unit);
        }
    }

    fn replace(&self, all: Vec<UnitDescriptor>) {
        let map: HashMap<u64, UnitDescriptor> = all.into_iter().map(|d| (d.unit, d)).collect();
        let gone: Vec<u64> = {
            let mut units = self.units.write().unwrap_or_else(PoisonError::into_inner);
            let gone = units
                .keys()
                .filter(|u| !map.contains_key(u))
                .copied()
                .collect();
            *units = map;
            gone
        };
        self.ready.store(true, Ordering::Release);
        for u in gone {
            self.removed(u);
        }
    }

    /// Keeps the directory equal to `store`'s descriptors until `stop`:
    /// lists them, then watches from the listing's revision, listing again
    /// after a compaction or an error.
    pub async fn follow(self: Arc<Self>, store: Arc<dyn MetaStore>, stop: CancellationToken) {
        let mut backoff = Duration::from_millis(100);
        while !stop.is_cancelled() {
            match self.follow_once(store.as_ref(), &stop).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(error = %e, "unit descriptors: watch failed; listing again");
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn follow_once(&self, store: &dyn MetaStore, stop: &CancellationToken) -> Result<()> {
        let prefix = all_prefix(store)?;
        let mut all = Vec::new();
        let mut opts = ListOpts {
            limit: 5_000,
            ..ListOpts::default()
        };
        let revision = loop {
            let page = store.list(&prefix, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(d) = decode(&kv.value) {
                    all.push(d);
                }
            }
            match (page.more, page.kvs.last()) {
                (true, Some(last)) => {
                    opts.at = Some(page.revision);
                    opts.after = Some(last.key.clone());
                }
                _ => break page.revision,
            }
        };
        self.replace(all);
        let mut w = store.watch(&prefix, revision + 1).await?;
        loop {
            let ev = tokio::select! {
                () = stop.cancelled() => return Ok(()),
                ev = w.next() => ev,
            };
            match ev {
                None => {
                    return Err(SprigError::internal("the descriptor watch ended"));
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(WatchEvent::Put(kv))) => match decode(&kv.value) {
                    Ok(d) => self.put(d),
                    Err(e) => tracing::warn!(error = %e, "a unit descriptor doesn't decode"),
                },
                Some(Ok(WatchEvent::Delete { key, .. })) => {
                    if let Some((_, unit)) = tenant_of(&key) {
                        self.forget(unit);
                        self.removed(unit);
                    }
                }
                Some(Ok(WatchEvent::Compacted { .. })) => {
                    return Err(SprigError::internal("the descriptor watch was compacted"));
                }
                Some(Ok(_)) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    fn store() -> Arc<dyn MetaStore> {
        Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-u").unwrap())))
    }

    fn desc(tenant: u64, voters: &[u64]) -> UnitDescriptor {
        UnitDescriptor {
            tenant,
            unit: tenant,
            voters: voters.to_vec(),
            learners: Vec::new(),
            home: voters[0],
            generation: 0,
            moving: None,
            last_move: None,
        }
    }

    #[tokio::test]
    async fn a_unit_is_placed_once_and_reruns_get_the_first_placement() {
        let s = store();
        let first = place_once(s.as_ref(), &desc(10, &[1, 2, 3]), &[])
            .await
            .unwrap();
        assert_eq!(first.voters, vec![1, 2, 3]);
        // A rerun with another choice gets the stored one.
        let again = place_once(s.as_ref(), &desc(10, &[3, 4, 5]), &[])
            .await
            .unwrap();
        assert_eq!(again, first);
        assert_eq!(read(s.as_ref(), 10).await.unwrap(), Some(first));
        assert_eq!(read(s.as_ref(), 11).await.unwrap(), None);
        place_once(s.as_ref(), &desc(11, &[2, 3, 4]), &[])
            .await
            .unwrap();
        assert_eq!(list(s.as_ref()).await.unwrap().len(), 2);
        remove(s.as_ref(), 10).await.unwrap();
        assert_eq!(read(s.as_ref(), 10).await.unwrap(), None);
        assert_eq!(list(s.as_ref()).await.unwrap().len(), 1);
        // Removing twice is fine.
        remove(s.as_ref(), 10).await.unwrap();
    }

    #[tokio::test]
    async fn the_directory_follows_creates_and_removals() {
        let s = store();
        place_once(s.as_ref(), &desc(20, &[1, 2, 3]), &[])
            .await
            .unwrap();
        let dir = UnitDirectory::new();
        let gone = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&gone);
        dir.on_removed(move |u| seen.lock().unwrap().push(u));
        let stop = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&dir).follow(Arc::clone(&s), stop.clone()));
        for _ in 0..500 {
            if dir.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(dir.is_ready());
        assert_eq!(dir.get(20).unwrap().voters, vec![1, 2, 3]);
        place_once(s.as_ref(), &desc(21, &[3, 4, 5]), &[])
            .await
            .unwrap();
        for _ in 0..500 {
            if dir.get(21).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(dir.get(21).unwrap().home, 3);
        let c = dir.counts();
        assert_eq!(c[&3], (2, 1));
        assert_eq!(c[&5], (1, 0));
        remove(s.as_ref(), 20).await.unwrap();
        for _ in 0..500 {
            if dir.get(20).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(dir.get(20).is_none());
        assert_eq!(*gone.lock().unwrap(), vec![20]);
        stop.cancel();
        task.await.unwrap();
        // A listing that no longer has a unit the directory held (deleted
        // while the watch was down) tells the hook too.
        dir.replace(Vec::new());
        assert_eq!(*gone.lock().unwrap(), vec![20, 21]);
    }

    #[test]
    fn descriptor_keys_name_their_tenant_and_unit() {
        let s = store();
        let k = unit_key(s.as_ref(), 28_306_000_001, 28_306_000_001).unwrap();
        assert_eq!(tenant_of(&k), Some((28_306_000_001, 28_306_000_001)));
    }
}
