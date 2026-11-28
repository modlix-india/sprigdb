//! Planned replica moves, run by each unit's leader.
//!
//! Whoever moves a unit (the controller's balancer, `sprigctl unit move`)
//! only writes the move into the unit's descriptor
//! (`sprig_catalog::units::start_move`: `moving {from, to}`, `to` listed as
//! a learner, `to` home if `from` was). The unit's leader does the work, so
//! a crash anywhere resumes from the descriptor:
//!
//! ```text
//! every second, for each unit led here (the tenant's own, or one of its
//! partition groups) whose tenant's descriptor has moving {from, to}:
//!   a joint configuration still open      → wait
//!   a member on a lost node               → own unit: give the move up
//!                                           (re-replication goes first)
//!   to a voter, from not                  → done: the own unit's leader
//!                                           writes the descriptor once every
//!                                           partition group held here has
//!                                           dropped `from` too
//!   to and from both voters (four)    → on `from`: leadership to `to` (one
//!                                           transfer, writes pause briefly);
//!                                           on any other leader: `from` leaves
//!   to a voter beside from (three),
//!   or from no longer a voter             → own unit: give the move up
//!   older than MOVE_TIMEOUT               → own unit: give the move up
//!   this node is `from`                   → `to` joins as a learner, gets a
//!                                           snapshot and the log, and becomes
//!                                           a fourth voter
//!   else                                  → `to` joins as a learner, gets a
//!                                           snapshot and the log, then one
//!                                           joint change puts it in `from`'s
//!                                           place
//! giving up: learner `to` leaves the group, the descriptor drops the move
//! and `to` (and gets its old home back); `to` is avoided for that unit
//! for a while
//! ```
//!
//! `from` drops its replica once the descriptor no longer lists it (the
//! stale-replica sweep of re-replication). A partition group with no move
//! pending follows its tenant's descriptor: a learner the descriptor
//! doesn't list leaves it, and a voter the descriptor doesn't list is
//! replaced by the one it lacks.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::units::{self, MoveEnd, UnitDescriptor, UnitMove};
use sprig_meta::MetaStore;

use super::nodes;
use crate::node_engine;

/// Moves one node runs at once (copies to new replicas).
pub const MOVES_AT_ONCE: usize = 2;
/// A move older than this is given up (its leader removes the learner).
pub const MOVE_TIMEOUT: Duration = Duration::from_secs(4 * 3600);
/// How long one copy step may run before the next pass tries again (the
/// learner keeps what it received).
const REPLACE_STEP: Duration = Duration::from_secs(600);
/// A target a move failed to is avoided for its unit this long.
const AVOID_FOR: Duration = Duration::from_secs(600);

static BUSY: Mutex<Option<HashSet<u64>>> = Mutex::new(None);
static AVOID: Mutex<Option<HashMap<(u64, u64), Instant>>> = Mutex::new(None);
static HANDED: Mutex<Option<HashMap<u64, Instant>>> = Mutex::new(None);
/// When each unit's target became a voter beside the source (on the
/// leaving leader): the hand-off waits [`WARM_FOR`] after it, so the target
/// has loaded the tenant's catalog before sessions arrive.
static PROMOTED: Mutex<Option<HashMap<u64, Instant>>> = Mutex::new(None);
/// Moves whose target loaded its tenant's catalog here: (unit, `since_ms`).
static WARMED: Mutex<Option<HashSet<(u64, u64)>>> = Mutex::new(None);
/// How long the target gets to warm up before the leadership comes.
const WARM_FOR: Duration = Duration::from_secs(2);

fn busy() -> std::sync::MutexGuard<'static, Option<HashSet<u64>>> {
    BUSY.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether a move of `unit` to `node` failed recently.
#[must_use]
pub fn avoided(unit: u64, node: u64) -> bool {
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

/// Whether `node` is the source or the target of a pending move of `unit`
/// (or its tenant's): it is then nobody's preferred leader for the unit or
/// its partition groups, so the move's leader alone decides when the
/// leadership goes to the target (once it has caught up and warmed up), and
/// leadership handed away from the source isn't pulled back.
#[must_use]
pub fn leaving(unit: u64, node: u64) -> bool {
    node_engine::unit_directory()
        .and_then(|d| d.get(node_engine::tenant_of_unit(unit)))
        .and_then(|d| d.moving)
        .is_some_and(|m| m.from == node || m.to == node)
}

pub(super) fn start(dc: &Arc<dyn MetaStore>, stop: sprig_common::CancellationToken) {
    let store = Arc::clone(dc);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
            pass(&store).await;
        }
    });
}

/// What the leader of a unit does next about its tenant's pending move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Nothing (yet).
    Wait,
    /// The move is done here: the descriptor takes these voters.
    Finish(Vec<u64>),
    /// Give the move up, for this reason.
    Abandon(&'static str),
    /// Hand this unit's leadership to `to`.
    HandOff(u64),
    /// Copy the unit to the target and swap it in for the source.
    Replace,
    /// Copy the unit to the target and make it a voter beside the source
    /// (on the leaving node, which then hands the leadership to it).
    Promote,
    /// Take the source out of a group the target already votes in.
    Remove,
}

/// What a unit's leader sees of one unit.
#[derive(Debug, Clone)]
pub struct Seen<'a> {
    /// This node.
    pub me: u64,
    /// The tenant's own unit (else a partition group).
    pub own: bool,
    /// Raft's voters (incoming half).
    pub voters: &'a [u64],
    /// Raft's outgoing voters (a joint configuration).
    pub outgoing: &'a [u64],
    /// How far each voter's log matches the leader's.
    pub matched: &'a HashMap<u64, u64>,
    /// The group's commit index.
    pub committed: u64,
    /// Every partition group held here has dropped the source.
    pub groups_done: bool,
    /// How long ago the move was asked for.
    pub age: Duration,
}

/// The next step for a unit's leader (module docs); `lost` says whether a
/// node is dead or removed, `leads` whether it may take leaderships.
pub fn decide(
    m: &UnitMove,
    s: &Seen<'_>,
    lost: impl Fn(u64) -> bool,
    leads: impl Fn(u64) -> bool,
) -> Step {
    if !s.outgoing.is_empty() {
        return Step::Wait;
    }
    let give_up = |why| {
        if s.own {
            Step::Abandon(why)
        } else {
            Step::Wait
        }
    };
    if s.voters.iter().any(|v| lost(*v)) || lost(m.to) {
        return give_up("a copy is on a lost node: re-replication goes first");
    }
    let to_in = s.voters.contains(&m.to);
    let from_in = s.voters.contains(&m.from);
    match (to_in, from_in) {
        (true, false) => {
            if s.own && s.groups_done {
                Step::Finish(s.voters.to_vec())
            } else {
                Step::Wait
            }
        }
        // The target was added beside the source (four voters): the
        // leaving node hands its leadership over, preferably to the
        // target; then the source leaves.
        (true, true) if s.voters.len() > 3 => {
            if s.me != m.from {
                return Step::Remove;
            }
            if leads(m.to) {
                // Only to a target that has the whole log: a transfer to a
                // lagging node holds proposals until it catches up.
                let caught_up = s.matched.get(&m.to).is_some_and(|x| *x >= s.committed);
                return if caught_up {
                    Step::HandOff(m.to)
                } else {
                    Step::Wait
                };
            }
            s.voters
                .iter()
                .copied()
                .filter(|v| *v != m.from && leads(*v))
                .max_by_key(|v| {
                    (
                        s.matched.get(v).copied().unwrap_or(0),
                        std::cmp::Reverse(*v),
                    )
                })
                .map_or(Step::Wait, Step::HandOff)
        }
        (true, true) => give_up("the target became a voter beside the source"),
        (false, false) => give_up("the source no longer holds a copy"),
        (false, true) if s.age > MOVE_TIMEOUT => give_up("the move took too long"),
        // On the leaving node: the target becomes a fourth voter first, so
        // the leadership goes to it in one transfer.
        (false, true) if s.me == m.from => Step::Promote,
        (false, true) => Step::Replace,
    }
}

/// A unit led here and what to do about it.
struct Work {
    unit: u64,
    tenant: u64,
    own: bool,
    desc: UnitDescriptor,
    step: Step,
    /// For a partition group without a move: (leaves, joins).
    follow: Option<(Option<u64>, Option<u64>)>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The units led here with a step to take.
fn work() -> Vec<Work> {
    let (Some(raft), Some(dir)) = (node_engine::engine_raft(), node_engine::unit_directory())
    else {
        return Vec::new();
    };
    if !node_engine::raft_placed() {
        return Vec::new();
    }
    let node = raft.node();
    let me = node.id();
    let groups = node.groups();
    let mut out = Vec::new();
    for &g in &groups {
        if g >= sprig_engine::NODE_UNIT.0 {
            continue;
        }
        let tenant = node_engine::tenant_of_unit(g);
        if tenant == 0 {
            continue;
        }
        let Some(desc) = dir.get(tenant) else {
            continue;
        };
        let own = g == tenant;
        let Some(st) = node.status(g) else { continue };
        if !st.is_leader {
            // The target of a move loads the tenant's catalog ahead of the
            // sessions the leadership will bring.
            // (Once it votes, so the catalog it loads is the latest.)
            if own
                && let Some(m) = &desc.moving
                && m.to == me
                && st.voters.contains(&me)
            {
                warm(tenant, m.since_ms);
            }
            continue;
        }
        let Some(m) = desc.moving.clone() else {
            if !own && st.voters_outgoing.is_empty() {
                let f = follow_descriptor(&desc, &st.voters, &st.learners);
                if f != (None, None) {
                    out.push(Work {
                        unit: g,
                        tenant,
                        own,
                        desc,
                        step: Step::Wait,
                        follow: Some(f),
                    });
                }
            }
            continue;
        };
        let matched: HashMap<u64, u64> = st
            .voters
            .iter()
            .map(|v| (*v, node.matched(g, *v).unwrap_or(0)))
            .collect();
        let groups_done = !own
            || groups.iter().all(|x| {
                *x == g
                    || node_engine::tenant_of_unit(*x) != tenant
                    || node.status(*x).is_some_and(|s| {
                        !s.voters.contains(&m.from) && s.voters_outgoing.is_empty()
                    })
            });
        let age = Duration::from_millis(now_ms().saturating_sub(m.since_ms));
        let step = decide(
            &m,
            &Seen {
                me,
                own,
                voters: &st.voters,
                outgoing: &st.voters_outgoing,
                matched: &matched,
                committed: st.committed,
                groups_done,
                age,
            },
            nodes::is_lost,
            nodes::takes_leadership,
        );
        if step != Step::Wait {
            out.push(Work {
                unit: g,
                tenant,
                own,
                desc,
                step,
                follow: None,
            });
        }
    }
    out
}

/// For a partition group with no move pending: a learner its tenant's
/// descriptor doesn't list (to remove), and a voter it has that the
/// descriptor doesn't list paired with one it lacks (to replace).
#[must_use]
pub fn follow_descriptor(
    d: &UnitDescriptor,
    voters: &[u64],
    learners: &[u64],
) -> (Option<u64>, Option<u64>) {
    if let Some(l) = learners
        .iter()
        .find(|l| !d.learners.contains(l) && !d.voters.contains(l))
    {
        return (Some(*l), None);
    }
    let extra: Vec<u64> = voters
        .iter()
        .copied()
        .filter(|v| !d.voters.contains(v))
        .collect();
    let lacking: Vec<u64> = d
        .voters
        .iter()
        .copied()
        .filter(|v| !voters.contains(v))
        .collect();
    match (extra.as_slice(), lacking.as_slice()) {
        ([e], [l]) => (Some(*e), Some(*l)),
        _ => (None, None),
    }
}

#[allow(clippy::too_many_lines)] // one arm per step
async fn pass(dc: &Arc<dyn MetaStore>) {
    let work = tokio::task::spawn_blocking(work).await.unwrap_or_default();
    for w in work {
        let step = w.step.clone();
        match (step, w.follow) {
            (_, Some((Some(leave), join))) => {
                if !claim(w.unit) {
                    continue;
                }
                tokio::spawn(async move {
                    let unit = w.unit;
                    let r = tokio::task::spawn_blocking(move || match join {
                        None => remove_learner(unit, leave),
                        Some(j) => replace(unit, leave, j),
                    })
                    .await;
                    release(unit);
                    if let Ok(Err(e)) = r {
                        tracing::debug!(unit, error = %e, "a partition group didn't follow its tenant's descriptor now");
                    }
                });
            }
            (Step::Finish(voters), _) => {
                let Some(m) = w.desc.moving.clone() else {
                    continue;
                };
                let how = MoveEnd::Done(voters);
                match units::end_move(dc.as_ref(), w.tenant, m.from, m.to, &how).await {
                    Ok(Some(d)) => {
                        if let Some(dir) = node_engine::unit_directory() {
                            dir.put(d.clone());
                        }
                        let secs = Duration::from_millis(now_ms().saturating_sub(m.since_ms))
                            .as_secs_f64();
                        metrics::counter!("sprig_unit_moves_total", "result" => "done")
                            .increment(1);
                        metrics::histogram!("sprig_unit_move_seconds").record(secs);
                        tracing::info!(
                            event = "decision",
                            kind = "unit.moved",
                            unit = w.unit,
                            tenant = w.tenant,
                            from = m.from,
                            to = m.to,
                            voters = ?d.voters,
                            home = d.home,
                            secs,
                            "a unit's replica moved"
                        );
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::debug!(tenant = w.tenant, error = %e, "a finished move's descriptor wasn't written now");
                    }
                }
            }
            (Step::Abandon(why), _) => {
                let Some(m) = w.desc.moving.clone() else {
                    continue;
                };
                abandon(dc, &w, &m, why).await;
            }
            (Step::HandOff(to), _) => {
                let unit = w.unit;
                // The target first gets WARM_FOR to load the tenant's catalog.
                let warm_since = PROMOTED
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get_or_insert_with(HashMap::new)
                    .entry(unit)
                    .or_insert_with(Instant::now)
                    .elapsed();
                if warm_since < WARM_FOR {
                    continue;
                }
                let recent = {
                    let mut g = HANDED.lock().unwrap_or_else(PoisonError::into_inner);
                    let h = g.get_or_insert_with(HashMap::new);
                    h.retain(|_, t| t.elapsed() < Duration::from_secs(3));
                    match h.entry(unit) {
                        std::collections::hash_map::Entry::Occupied(_) => true,
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert(Instant::now());
                            false
                        }
                    }
                };
                if !recent {
                    let _ = tokio::task::spawn_blocking(move || hand_off(unit, to)).await;
                    PROMOTED
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .get_or_insert_with(HashMap::new)
                        .remove(&unit);
                }
            }
            (s @ (Step::Replace | Step::Promote | Step::Remove), _) => {
                let Some(m) = w.desc.moving.clone() else {
                    continue;
                };
                if !claim(w.unit) {
                    continue;
                }
                let dc = Arc::clone(dc);
                tokio::spawn(async move {
                    let unit = w.unit;
                    let (from, to) = (m.from, m.to);
                    let started = Instant::now();
                    let r = tokio::task::spawn_blocking(move || match s {
                        Step::Promote => promote(unit, to),
                        Step::Remove => remove_voter(unit, from),
                        _ => replace(unit, from, to),
                    })
                    .await
                    .map_err(|e| sprig_common::SprigError::internal(e.to_string()))
                    .and_then(|r| r);
                    release(unit);
                    match r {
                        Ok(()) => tracing::info!(
                            unit,
                            from,
                            to,
                            ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                            "move: the target holds the unit and replaced the source in its group"
                        ),
                        Err(e) if e.message.contains("busy") || e.message.contains("timed out") => {
                            // Another change is running, or the copy goes on:
                            // the next pass tries again (the learner keeps
                            // what it received).
                            tracing::debug!(unit, error = %e, "move: not finished in this step");
                        }
                        Err(e) => {
                            // Give up only while this node still leads the unit
                            // and the target isn't a voter yet; otherwise the
                            // move went on elsewhere and the next pass sees it.
                            let still = tokio::task::spawn_blocking(move || {
                                node_engine::engine_raft()
                                    .and_then(|r| r.node().status(unit))
                                    .is_some_and(|st| st.is_leader && !st.voters.contains(&to))
                            })
                            .await
                            .unwrap_or(false);
                            if w.own && still {
                                tracing::warn!(unit, from, to, error = %e, "move: the copy failed; giving the move up");
                                abandon(&dc, &w, &m, "the copy failed").await;
                            } else {
                                tracing::info!(unit, from, to, error = %e, "move: a step ended early; the move goes on from the group's state");
                            }
                        }
                    }
                });
            }
            _ => {}
        }
    }
}

fn claim(unit: u64) -> bool {
    let mut b = busy();
    let set = b.get_or_insert_with(HashSet::new);
    if set.len() >= MOVES_AT_ONCE || set.contains(&unit) {
        return false;
    }
    set.insert(unit);
    true
}

fn release(unit: u64) {
    busy().get_or_insert_with(HashSet::new).remove(&unit);
}

async fn abandon(dc: &Arc<dyn MetaStore>, w: &Work, m: &UnitMove, why: &str) {
    let (unit, to) = (w.unit, m.to);
    let still_learner = tokio::task::spawn_blocking(move || {
        node_engine::engine_raft()
            .and_then(|r| r.node().status(unit))
            .is_some_and(|s| s.learners.contains(&to))
    })
    .await
    .unwrap_or(false);
    if still_learner {
        let _ = tokio::task::spawn_blocking(move || remove_learner(unit, to)).await;
    }
    avoid(w.unit, m.to);
    match units::end_move(dc.as_ref(), w.tenant, m.from, m.to, &MoveEnd::Abandoned).await {
        Ok(Some(d)) => {
            if let Some(dir) = node_engine::unit_directory() {
                dir.put(d);
            }
            metrics::counter!("sprig_unit_moves_total", "result" => "abandoned").increment(1);
            tracing::warn!(
                event = "decision",
                kind = "unit.move_abandoned",
                unit = w.unit,
                tenant = w.tenant,
                from = m.from,
                to = m.to,
                why,
                "a unit's replica move was given up"
            );
        }
        Ok(None) => {}
        Err(e) => tracing::debug!(tenant = w.tenant, error = %e, "a move wasn't given up now"),
    }
}

/// Loads `tenant`'s catalog into this node's cache once per move (keyed by
/// when the move was asked for), in the background.
fn warm(tenant: u64, since_ms: u64) {
    let fresh = {
        let mut g = WARMED.lock().unwrap_or_else(PoisonError::into_inner);
        let set = g.get_or_insert_with(HashSet::new);
        if set.len() > 10_000 {
            set.clear();
        }
        set.insert((tenant, since_ms))
    };
    if !fresh {
        return;
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    rt.spawn(async move {
        let started = Instant::now();
        let _ = crate::reader::tenant_value_latest(&crate::read_copies::READ_COPIES, tenant).await;
        tracing::info!(
            tenant,
            ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "move: the target loaded the tenant's catalog ahead of its leadership"
        );
    });
}

/// Hands `unit`'s leadership to `to` and waits briefly for it to land.
fn hand_off(unit: u64, to: u64) {
    let Some(raft) = node_engine::engine_raft() else {
        return;
    };
    let node = raft.node();
    let started = Instant::now();
    if let Err(e) = node.transfer_leader(unit, to) {
        tracing::debug!(unit, to, error = %e, "move: leadership transfer refused");
        return;
    }
    while started.elapsed() < Duration::from_secs(2) {
        if node.status(unit).is_some_and(|s| s.leader == Some(to)) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // The tenant's sessions here close now, not at the watcher's next look.
    crate::forward::watch::nudge();
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    metrics::counter!("sprig_node_leader_transfers_total", "reason" => "move").increment(1);
    tracing::info!(
        unit,
        to,
        ms,
        "move: the leaving node handed the unit's leadership over"
    );
}

/// `to` joins `unit`'s group and takes `from`'s place (on its leader).
fn replace(unit: u64, from: u64, to: u64) -> sprig_common::Result<()> {
    let raft = node_engine::engine_raft()
        .ok_or_else(|| sprig_common::SprigError::internal("the node doesn't replicate"))?;
    raft.replace_replica(sprig_engine::UnitId(unit), from, to, REPLACE_STEP)
        .map_err(|e| sprig_common::SprigError::internal(e.to_string()))
}

/// `to` joins `unit`'s group as a learner, catches up and becomes a voter
/// beside the others (on its leader).
fn promote(unit: u64, to: u64) -> sprig_common::Result<()> {
    use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
    let err = |m: String| sprig_common::SprigError::internal(m);
    let raft =
        node_engine::engine_raft().ok_or_else(|| err("the node doesn't replicate".into()))?;
    let node = raft.node();
    let deadline = Instant::now() + REPLACE_STEP;
    let change = |t: ConfChangeType| -> sprig_common::Result<()> {
        let mut c = ConfChangeSingle::default();
        c.set_change_type(t);
        c.node_id = to;
        let mut cc = ConfChangeV2::default();
        cc.mut_changes().push(c);
        let slot = node
            .change_membership(unit, cc)
            .map_err(|e| err(e.to_string()))?;
        slot.wait_timeout(Duration::from_secs(30))
            .ok_or_else(|| err("promoting the target: no answer".into()))?
            .map(|_| ())
            .map_err(|e| err(e.to_string()))
    };
    let st = node
        .status(unit)
        .ok_or_else(|| err(format!("unit {unit} isn't held here")))?;
    if !st.learners.contains(&to) && !st.voters.contains(&to) {
        change(ConfChangeType::AddLearnerNode)?;
    }
    loop {
        let st = node
            .status(unit)
            .ok_or_else(|| err(format!("unit {unit} isn't held here")))?;
        if node.matched(unit, to).is_some_and(|m| m >= st.committed) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(err(
                "promoting the target: timed out waiting for it to catch up".into(),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    change(ConfChangeType::AddNode)?;
    tracing::info!(
        unit,
        to,
        "move: the target caught up and votes beside the source"
    );
    Ok(())
}

/// Takes voter `from` out of `unit`'s group (on its leader, `from` not
/// leading).
fn remove_voter(unit: u64, from: u64) -> sprig_common::Result<()> {
    remove_learner(unit, from)
}

/// Removes learner `l` from `unit`'s group (on its leader).
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
    tracing::info!(
        unit,
        learner = l,
        "move: took a node out of the unit's group"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mv(from: u64, to: u64) -> UnitMove {
        UnitMove {
            from,
            to,
            since_ms: 0,
            home_was: from,
            reason: String::new(),
        }
    }

    fn seen<'a>(me: u64, voters: &'a [u64], matched: &'a HashMap<u64, u64>) -> Seen<'a> {
        Seen {
            me,
            own: true,
            voters,
            outgoing: &[],
            matched,
            committed: 10,
            groups_done: true,
            age: Duration::from_secs(5),
        }
    }

    #[test]
    fn the_leader_copies_then_the_descriptor_follows() {
        let m = mv(1, 4);
        let matched = HashMap::from([(1, 10), (2, 10), (3, 9), (4, 10)]);
        let none = |_| false;
        let all = |_| true;
        // A follower of the move leads: it copies.
        assert_eq!(
            decide(&m, &seen(2, &[1, 2, 3], &matched), none, all),
            Step::Replace
        );
        // The leaving node leads: the target becomes a fourth voter first,
        // then the leadership goes to it in one transfer ...
        assert_eq!(
            decide(&m, &seen(1, &[1, 2, 3], &matched), none, all),
            Step::Promote
        );
        assert_eq!(
            decide(&m, &seen(1, &[1, 2, 3, 4], &matched), none, all),
            Step::HandOff(4)
        );
        // ... not before the target has the whole log.
        let lagging = HashMap::from([(1, 10), (2, 10), (3, 9), (4, 7)]);
        assert_eq!(
            decide(&m, &seen(1, &[1, 2, 3, 4], &lagging), none, all),
            Step::Wait
        );
        // ... else to the most caught-up voter that may lead.
        assert_eq!(
            decide(&m, &seen(1, &[1, 2, 3, 4], &matched), none, |n| n == 3),
            Step::HandOff(3)
        );
        // The new leader takes the source out.
        assert_eq!(
            decide(&m, &seen(4, &[1, 2, 3, 4], &matched), none, all),
            Step::Remove
        );
        // Swapped in: finish, once the partition groups are done too.
        assert_eq!(
            decide(&m, &seen(2, &[2, 3, 4], &matched), none, all),
            Step::Finish(vec![2, 3, 4])
        );
        let mut s = seen(2, &[2, 3, 4], &matched);
        s.groups_done = false;
        assert_eq!(decide(&m, &s, none, all), Step::Wait);
        // A joint configuration still open: wait.
        let mut s = seen(2, &[2, 3, 4], &matched);
        s.outgoing = &[1, 2, 3];
        assert_eq!(decide(&m, &s, none, all), Step::Wait);
    }

    #[test]
    fn moves_give_way_to_re_replication_and_odd_states() {
        let m = mv(1, 4);
        let matched = HashMap::new();
        let all = |_| true;
        assert_eq!(
            decide(&m, &seen(2, &[1, 2, 3], &matched), |n| n == 3, all),
            Step::Abandon("a copy is on a lost node: re-replication goes first")
        );
        assert!(matches!(
            decide(&m, &seen(2, &[1, 2, 4], &matched), |_| false, all),
            Step::Abandon(_)
        ));
        assert!(matches!(
            decide(&m, &seen(2, &[2, 3, 5], &matched), |_| false, all),
            Step::Abandon(_)
        ));
        let mut s = seen(2, &[1, 2, 3], &matched);
        s.age = MOVE_TIMEOUT + Duration::from_secs(1);
        assert!(matches!(decide(&m, &s, |_| false, all), Step::Abandon(_)));
        // A partition group never gives a move up itself.
        let mut s = seen(2, &[1, 2, 4], &matched);
        s.own = false;
        assert_eq!(decide(&m, &s, |_| false, all), Step::Wait);
        // ... and finishes its own part without writing anything.
        let mut s = seen(2, &[2, 3, 4], &matched);
        s.own = false;
        assert_eq!(decide(&m, &s, |_| false, all), Step::Wait);
    }

    #[test]
    fn partition_groups_follow_their_tenants_descriptor() {
        let d = UnitDescriptor {
            tenant: 7,
            unit: 7,
            voters: vec![2, 3, 4],
            learners: vec![6],
            home: 2,
            generation: 3,
            moving: None,
            last_move: None,
        };
        assert_eq!(follow_descriptor(&d, &[2, 3, 4], &[6]), (None, None));
        assert_eq!(follow_descriptor(&d, &[2, 3, 4], &[5]), (Some(5), None));
        assert_eq!(follow_descriptor(&d, &[1, 2, 3], &[]), (Some(1), Some(4)));
        // Two voters off: left to re-replication and moves.
        assert_eq!(follow_descriptor(&d, &[1, 5, 3], &[]), (None, None));
    }
}
