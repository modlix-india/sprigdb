//! Re-replication, on each unit's leader, and stale replicas leaving.
//!
//! ```text
//! every 2 s, for each tenant unit led here (units placed per tenant):
//!   a voter on a lost node (dead or removed) → replace it:
//!     new := the descriptor's learner, if it is usable; else the usable
//!            node (active, places units, gateway ready, not a member) in a
//!            failure domain no other voter has, fewest units, ties by a
//!            hash of unit and node
//!     1. descriptor: learners += new                  (generation + 1)
//!     2. Raft: new joins as a learner, gets a snapshot and the log, then
//!        one joint change makes it a voter in place of the lost one
//!     3. descriptor: voters := Raft's voters, learners -= new,
//!        home := this node if the home was lost        (generation + 1)
//!   a learner on a lost node → removed from the group
//! a partition group led here takes the replacement its tenant's
//! descriptor names (no descriptor writes of its own)
//! at most REBUILDS units at once per node
//!
//! every 10 s: a unit held here whose descriptor (generation > 0) lists
//! this node neither as voter nor learner leaves this node (tombstone,
//! keys cleared): a node that was dead or removed while its units moved
//! ```
//!
//! The descriptor is written after Raft's change, never before it, so a
//! node never drops a replica Raft still counts; and the replacement is in
//! the descriptor (as a learner) before it joins, so it never drops the
//! replica it is being given.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::units::{self, NODE_ACTIVE, UnitDescriptor};
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};

use super::nodes;
use crate::node_engine;

/// Units one node re-replicates at once.
pub const REBUILDS: usize = 4;
/// How long one replacement may take (learner, snapshot, catch-up, change).
const REPLACE_TIMEOUT: Duration = Duration::from_secs(180);

/// Units being re-replicated from this node now.
static BUSY: Mutex<Option<HashSet<u64>>> = Mutex::new(None);

fn busy() -> std::sync::MutexGuard<'static, Option<HashSet<u64>>> {
    BUSY.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Replacements that failed: (unit, node) → when; that node isn't chosen
/// for that unit again for [`AVOID_FOR`].
static AVOID: Mutex<Option<HashMap<(u64, u64), Instant>>> = Mutex::new(None);
const AVOID_FOR: Duration = Duration::from_secs(300);

fn avoided(unit: u64, node: u64) -> bool {
    AVOID
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_ref()
        .and_then(|m| m.get(&(unit, node)))
        .is_some_and(|t| t.elapsed() < AVOID_FOR)
}

fn avoid(unit: u64, node: u64) {
    let mut g = AVOID.lock().unwrap_or_else(PoisonError::into_inner);
    let m = g.get_or_insert_with(HashMap::new);
    m.retain(|_, t| t.elapsed() < AVOID_FOR);
    m.insert((unit, node), Instant::now());
}

pub(super) fn start(dc: &Arc<dyn MetaStore>, stop: sprig_common::CancellationToken) {
    let store = Arc::clone(dc);
    let until = stop.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = until.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            if let Err(e) = pass(&store).await {
                tracing::debug!(error = %e, "re-replication pass failed");
            }
        }
    });
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            let _ = tokio::task::spawn_blocking(drop_stale).await;
        }
    });
}

/// A unit led here with a member on a lost node.
#[derive(Debug, Clone)]
struct Work {
    unit: u64,
    tenant: u64,
    /// Its tenant's own unit (else a partition group).
    own: bool,
    lost_voter: Option<u64>,
    lost_learners: Vec<u64>,
    voters: Vec<u64>,
    learners: Vec<u64>,
}

/// Whether `unit`'s descriptor (its tenant's) names a voter on a lost
/// node: its leader is (or will be) re-replicating it.
#[must_use]
pub fn short_of_copies(unit: u64) -> bool {
    node_engine::unit_directory()
        .and_then(|d| d.get(node_engine::tenant_of_unit(unit)))
        .is_some_and(|d| d.voters.iter().any(|v| nodes::is_lost(*v)))
}

/// The units led here that hold a member on a lost node.
fn work() -> Vec<Work> {
    let (Some(raft), Some(dir)) = (node_engine::engine_raft(), node_engine::unit_directory())
    else {
        return Vec::new();
    };
    if !node_engine::raft_placed() || nodes::directory().is_none() {
        return Vec::new();
    }
    let node = raft.node();
    let mut out = Vec::new();
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 {
            continue;
        }
        let tenant = node_engine::tenant_of_unit(g);
        if tenant == 0 || dir.get(tenant).is_none() {
            continue;
        }
        let Some(st) = node.status(g) else { continue };
        if !st.is_leader || !st.voters_outgoing.is_empty() {
            continue;
        }
        let lost_voter = st.voters.iter().copied().find(|v| nodes::is_lost(*v));
        let lost_learners: Vec<u64> = st
            .learners
            .iter()
            .copied()
            .filter(|l| nodes::is_lost(*l))
            .collect();
        if lost_voter.is_none() && lost_learners.is_empty() {
            // Nothing lost: a tenant's own descriptor's voters must match
            // Raft's (a replacement whose last write failed leaves the old
            // voters). Learners are read copies, kept by their own
            // reconciler from the descriptor; they are left alone here.
            // A planned move's leader writes the descriptor itself.
            if g == tenant
                && let Some(d) = dir.get(tenant)
                && d.moving.is_none()
            {
                let mut v = st.voters.clone();
                v.sort_unstable();
                let mut dv = d.voters.clone();
                dv.sort_unstable();
                if dv != v {
                    out.push(Work {
                        unit: g,
                        tenant,
                        own: true,
                        lost_voter: None,
                        lost_learners: Vec::new(),
                        voters: st.voters,
                        learners: st.learners,
                    });
                }
            }
            continue;
        }
        out.push(Work {
            unit: g,
            tenant,
            own: g == tenant,
            lost_voter,
            lost_learners,
            voters: st.voters,
            learners: st.learners,
        });
    }
    out
}

/// Raft ids whose gateway registration is ready now.
async fn ready_nodes(dc: &dyn MetaStore) -> sprig_common::Result<BTreeSet<u64>> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Gw {
        state: String,
        #[serde(default)]
        raft_node: Option<u64>,
    }
    impl Versioned for Gw {
        const V: u32 = 1;
    }
    let prefix = dc.tier().key("gateways/")?;
    let page = dc
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
        .filter_map(|kv| Record::<Gw>::decode(&kv.value).ok())
        .filter(|g| g.body.state == "ready")
        .filter_map(|g| g.body.raft_node)
        .collect())
}

/// A node usable as a new replica of a unit: what the choice sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// Its Raft id.
    pub id: u64,
    /// Its failure domain (empty: one of its own).
    pub failure_domain: String,
    /// Units it holds a copy of.
    pub units: usize,
}

fn mix(a: u64, b: u64) -> u64 {
    let mut x = a.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ b.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x
}

/// Chooses the node that replaces a lost voter of `unit`: among
/// `candidates` (already usable and not members), one in a failure domain
/// none of `kept_domains` has (if any such exists, else any), with the
/// fewest units, ties broken by a hash of the unit and the node (so the
/// leaders of many units replaced at once spread them).
#[must_use]
pub fn choose(unit: u64, candidates: &[Candidate], kept_domains: &[String]) -> Option<u64> {
    let domain = |c: &Candidate| {
        if c.failure_domain.is_empty() {
            format!("\u{0}node-{}", c.id)
        } else {
            c.failure_domain.clone()
        }
    };
    let fresh: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| !kept_domains.contains(&domain(c)))
        .collect();
    let pool: Vec<&Candidate> = if fresh.is_empty() {
        candidates.iter().collect()
    } else {
        fresh
    };
    pool.into_iter()
        .min_by_key(|c| (c.units, mix(unit, c.id)))
        .map(|c| c.id)
}

async fn pass(dc: &Arc<dyn MetaStore>) -> sprig_common::Result<()> {
    let work = tokio::task::spawn_blocking(work).await.unwrap_or_default();
    if work.is_empty() {
        return Ok(());
    }
    let (Some(ndir), Some(udir)) = (nodes::directory(), node_engine::unit_directory()) else {
        return Ok(());
    };
    let ready = ready_nodes(dc.as_ref()).await?;
    let all = ndir.all();
    let counts = udir.counts();
    let domain_of = |id: u64| {
        all.get(&id).map_or_else(
            || format!("\u{0}node-{id}"),
            |n| {
                if n.failure_domain.is_empty() {
                    format!("\u{0}node-{id}")
                } else {
                    n.failure_domain.clone()
                }
            },
        )
    };
    let usable = |id: u64| {
        ready.contains(&id)
            && all
                .get(&id)
                .is_some_and(|n| n.state == NODE_ACTIVE && n.placement)
    };
    for w in work {
        {
            let mut b = busy();
            let set = b.get_or_insert_with(HashSet::new);
            if set.len() >= REBUILDS || set.contains(&w.unit) {
                continue;
            }
            set.insert(w.unit);
        }
        let Some(desc) = udir.get(w.tenant) else {
            busy().get_or_insert_with(HashSet::new).remove(&w.unit);
            continue;
        };
        let members: HashSet<u64> = w.voters.iter().chain(&w.learners).copied().collect();
        let new = match w.lost_voter {
            None => None,
            Some(_) if !w.own => {
                // A partition group follows its tenant's descriptor.
                desc.voters
                    .iter()
                    .chain(&desc.learners)
                    .copied()
                    .find(|n| !w.voters.contains(n) && !nodes::is_lost(*n))
            }
            Some(_) => desc
                .learners
                .iter()
                .copied()
                .find(|n| !w.voters.contains(n) && usable(*n) && !avoided(w.unit, *n))
                .or_else(|| {
                    let kept: Vec<String> = w
                        .voters
                        .iter()
                        .filter(|v| !nodes::is_lost(**v))
                        .map(|v| domain_of(*v))
                        .collect();
                    let cands: Vec<Candidate> = all
                        .values()
                        .filter(|n| {
                            usable(n.id) && !members.contains(&n.id) && !avoided(w.unit, n.id)
                        })
                        .map(|n| Candidate {
                            id: n.id,
                            failure_domain: n.failure_domain.clone(),
                            units: counts.get(&n.id).map_or(0, |c| c.0),
                        })
                        .collect();
                    choose(w.unit, &cands, &kept)
                }),
        };
        let dc = Arc::clone(dc);
        tokio::spawn(async move {
            let unit = w.unit;
            let r = rebuild(&dc, &w, desc, new).await;
            busy().get_or_insert_with(HashSet::new).remove(&unit);
            if let Err(e) = r {
                metrics::counter!("sprig_node_rereplications_total", "result" => "failed")
                    .increment(1);
                tracing::warn!(unit, error = %e, "re-replication of a unit failed; retried");
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // one replacement, step by step
async fn rebuild(
    dc: &Arc<dyn MetaStore>,
    w: &Work,
    desc: UnitDescriptor,
    new: Option<u64>,
) -> sprig_common::Result<()> {
    let started = Instant::now();
    let me = nodes::me();
    // Learners on lost nodes leave the group first.
    for l in &w.lost_learners {
        let (unit, l) = (w.unit, *l);
        tokio::task::spawn_blocking(move || remove_learner(unit, l))
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))??;
        if w.own {
            units::update(dc.as_ref(), w.tenant, |d| {
                let had = d.learners.len();
                d.learners.retain(|x| *x != l);
                d.learners.len() != had
            })
            .await?;
        }
    }
    if w.lost_voter.is_none() && w.own {
        // Tidy: the descriptor takes Raft's voters and no learners.
        let mut v = w.voters.clone();
        v.sort_unstable();
        units::update(dc.as_ref(), w.tenant, |d| {
            let mut dv = d.voters.clone();
            dv.sort_unstable();
            if dv == v {
                return false;
            }
            d.voters.clone_from(&v);
            d.learners.retain(|l| !v.contains(l));
            if !d.voters.contains(&d.home) {
                d.home = me;
            }
            true
        })
        .await?;
        tracing::info!(unit = w.unit, voters = ?v, "unit descriptor brought in line with its Raft group");
        return Ok(());
    }
    let (Some(dead), Some(new)) = (w.lost_voter, new) else {
        if w.lost_voter.is_some() {
            tracing::debug!(
                unit = w.unit,
                "no usable node to re-replicate a unit to yet"
            );
        }
        return Ok(());
    };
    if w.own && !desc.learners.contains(&new) && !desc.voters.contains(&new) {
        units::update(dc.as_ref(), w.tenant, |d| {
            if d.learners.contains(&new) || d.voters.contains(&new) {
                return false;
            }
            d.learners.push(new);
            true
        })
        .await?;
        // The new node follows the descriptor by watch; give it a moment
        // to learn that it is the unit's learner before frames arrive.
    }
    tracing::info!(
        event = "decision",
        kind = "unit.rereplicate",
        unit = w.unit,
        tenant = w.tenant,
        lost = dead,
        new,
        "re-replicating a unit whose voter is on a lost node"
    );
    let unit = w.unit;
    let raft_r = tokio::task::spawn_blocking(move || {
        let raft = node_engine::engine_raft()
            .ok_or_else(|| sprig_common::SprigError::internal("the node doesn't replicate"))?;
        raft.replace_replica(sprig_engine::UnitId(unit), dead, new, REPLACE_TIMEOUT)
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        Ok::<_, sprig_common::SprigError>(
            raft.node()
                .status(unit)
                .map(|s| s.voters)
                .unwrap_or_default(),
        )
    })
    .await
    .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
    let voters = match raft_r {
        Ok(v) => v,
        Err(e) if e.message.contains("busy") => {
            // Another change or a leader transfer is running: try again on
            // the next pass, with the same node.
            return Err(e);
        }
        Err(e) => {
            // Give the node up for this unit for a while: take it out of
            // the group (if it is only a learner) and of the descriptor,
            // so the next pass chooses another.
            avoid(w.unit, new);
            let still_learner = tokio::task::spawn_blocking(move || {
                node_engine::engine_raft()
                    .and_then(|r| r.node().status(unit))
                    .is_some_and(|s| s.learners.contains(&new))
            })
            .await
            .unwrap_or(false);
            if still_learner {
                let _ = tokio::task::spawn_blocking(move || remove_learner(unit, new)).await;
            }
            if w.own {
                let _ = units::update(dc.as_ref(), w.tenant, |d| {
                    let had = d.learners.len();
                    d.learners.retain(|x| *x != new);
                    d.learners.len() != had
                })
                .await;
            }
            return Err(e);
        }
    };
    if w.own {
        let home_lost = nodes::is_lost(desc.home) || !voters.contains(&desc.home);
        units::update(dc.as_ref(), w.tenant, |d| {
            let mut v = voters.clone();
            v.sort_unstable();
            let changed = d.voters != v || d.learners.iter().any(|l| v.contains(l));
            d.voters = v;
            d.learners.retain(|l| !d.voters.contains(l));
            if home_lost || !d.voters.contains(&d.home) {
                d.home = me;
                return true;
            }
            changed
        })
        .await?;
    }
    metrics::counter!("sprig_node_rereplications_total", "result" => "ok").increment(1);
    metrics::histogram!("sprig_node_rereplication_seconds").record(started.elapsed().as_secs_f64());
    tracing::info!(
        event = "decision",
        kind = "unit.rereplicated",
        unit = w.unit,
        tenant = w.tenant,
        lost = dead,
        new,
        ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "a unit is back at full copies"
    );
    Ok(())
}

/// Removes learner `l` (on a lost node) from `unit`'s group; on its leader.
fn remove_learner(unit: u64, l: u64) -> sprig_common::Result<()> {
    use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
    let raft = node_engine::engine_raft()
        .ok_or_else(|| sprig_common::SprigError::internal("the node doesn't replicate"))?;
    let mut c = ConfChangeSingle::default();
    c.set_change_type(ConfChangeType::RemoveNode);
    c.node_id = l;
    let mut cc = ConfChangeV2::default();
    cc.mut_changes().push(c);
    let slot = raft
        .node()
        .change_membership(unit, cc)
        .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
    slot.wait_timeout(Duration::from_secs(10))
        .ok_or_else(|| sprig_common::SprigError::internal("removing a learner: no answer"))?
        .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
    tracing::info!(unit, learner = l, "removed a learner on a lost node");
    Ok(())
}

/// Drops the replicas this node holds that their unit's descriptor no
/// longer lists here (module docs). Returns how many it dropped.
pub fn drop_stale() -> usize {
    let (Some(raft), Some(dir)) = (node_engine::engine_raft(), node_engine::unit_directory())
    else {
        return 0;
    };
    if !node_engine::raft_placed() || !dir.is_ready() {
        return 0;
    }
    let Ok(engine) = node_engine::engine() else {
        return 0;
    };
    let me = nodes::me();
    let node = raft.node();
    let snaps = sprig_raft::EngineSnapshots::new(engine);
    let mut dropped = 0;
    let mut by_tenant: HashMap<u64, Option<UnitDescriptor>> = HashMap::new();
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 {
            continue;
        }
        let tenant = node_engine::tenant_of_unit(g);
        if tenant == 0 {
            continue;
        }
        let d = by_tenant.entry(tenant).or_insert_with(|| dir.get(tenant));
        let Some(d) = d else { continue };
        if d.generation == 0 || d.voters.contains(&me) || d.learners.contains(&me) {
            continue;
        }
        if node.status(g).is_some_and(|s| s.is_leader) {
            continue;
        }
        match node.drop_group(g) {
            Ok(true) => {
                dropped += 1;
                if let Err(e) =
                    sprig_raft::UnitSnapshots::ingest(&snaps, sprig_engine::UnitId(g), 0, &[])
                {
                    tracing::warn!(unit = g, error = %e, "engine: a stale replica's keys weren't cleared");
                }
                forget_streams(g);
                metrics::counter!("sprig_node_stale_replicas_dropped_total").increment(1);
                tracing::info!(
                    unit = g,
                    tenant,
                    generation = d.generation,
                    "engine: dropped a replica its unit's descriptor no longer lists on this node"
                );
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(unit = g, error = %e, "engine: a stale replica wasn't dropped");
            }
        }
    }
    // Units the stream log still keeps floors for but this node holds no
    // replica of and isn't placed on (dropped before the stream log forgot
    // them, e.g. by an older build): forget them too.
    if let Some(streams) = crate::kafka::engine::streams() {
        for u in streams.log().floored_units() {
            if u >= sprig_engine::NODE_UNIT.0 || node.status(u).is_some() {
                continue;
            }
            let tenant = node_engine::tenant_of_unit(u);
            let placed_here = dir
                .get(tenant)
                .is_some_and(|d| d.voters.contains(&me) || d.learners.contains(&me));
            if tenant != 0 && !placed_here {
                forget_streams(u);
            }
        }
    }
    dropped
}

/// The stream log forgets a unit that left this node: its partitions'
/// files go and it no longer waits for that unit's commit log (which this
/// node no longer has) before writing its manifest.
pub(crate) fn forget_streams(unit: u64) {
    if let Some(s) = crate::kafka::engine::streams() {
        let n = s.log().forget_unit(unit);
        if n > 0 {
            tracing::info!(
                unit,
                partitions = n,
                "stream log: dropped a departed unit's partitions"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(id: u64, fd: &str, units: usize) -> Candidate {
        Candidate {
            id,
            failure_domain: fd.into(),
            units,
        }
    }

    #[test]
    fn a_replacement_goes_to_a_fresh_domain_with_the_fewest_units() {
        let cands = [c(4, "a", 1), c(5, "b", 3), c(6, "c", 2)];
        // Domain a is kept by a surviving voter: 6 (domain c, 2 units)
        // beats 5 (domain b, 3 units); 4 is in a kept domain.
        assert_eq!(choose(9, &cands, &["a".into(), "x".into()]), Some(6));
        // Every candidate in a kept domain: the least loaded anyway.
        assert_eq!(
            choose(9, &cands, &["a".into(), "b".into(), "c".into()]),
            Some(4)
        );
        assert_eq!(choose(9, &[], &[]), None);
        // Nodes without a domain are each one of their own.
        let bare = [c(4, "", 0), c(5, "", 0)];
        let a = choose(1, &bare, &[]).unwrap();
        assert!(a == 4 || a == 5);
    }

    #[test]
    fn ties_spread_over_units() {
        let cands = [c(4, "", 0), c(5, "", 0), c(6, "", 0)];
        let picks: HashSet<u64> = (0..64).filter_map(|u| choose(u, &cands, &[])).collect();
        assert_eq!(picks.len(), 3, "equal nodes share the replacements");
    }
}
