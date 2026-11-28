//! Extra read-only copies of a tenant's unit: non-voting learners listed in
//! its unit descriptor (`learners`), which receive every change and serve
//! reader sessions ([`crate::reader`]) as voters do, without slowing
//! commits.
//!
//! The tenant's `placement.read_copies` says how many (`sprigctl unit
//! read-copies <tenant> <n>` sets it). The unit's leader keeps the
//! descriptor's learners at that number: it adds nodes that hold no
//! replica of the unit (active nodes placing units per tenant, in failure
//! domains the unit doesn't use yet when there are such, fewest units
//! first) and drops the last ones listed, rewriting the descriptor
//! compare-and-set with its generation raised.
//!
//! The descriptor says which nodes hold them; the unit's leader makes the
//! group's configuration match: it adds each listed learner that the group
//! lacks (the learner node creates its replica when the leader's first
//! message arrives and catches up from a snapshot and the log) and removes
//! learners the descriptor no longer lists. Voters are never changed here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::units::{NodeRecord, UnitDescriptor, UnitDirectory};
use sprig_common::{CancellationToken, Result};
use sprig_meta::{Cmp, Consistency, MetaStore, Op, Record};
use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// How often the leader compares its units' learners with the descriptors.
const EVERY: Duration = Duration::from_secs(1);

/// How often the leader compares its units' descriptors with
/// `placement.read_copies`.
const SETTING_EVERY: u32 = 5;

/// The most extra read copies a unit may have.
pub const MAX_READ_COPIES: u64 = 16;

/// `placement.read_copies`: extra read-only copies of a tenant's unit.
pub static READ_COPIES: Setting<u64> = Setting {
    key: "placement.read_copies",
    default: 0,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 16),
    apply: Apply::Live,
    operator_only: false,
    owner: 61,
    doc: "Extra read-only copies (non-voting learners) of each of the tenant's units, serving reads from copies",
};
sprig_settings::register!(READ_COPIES);

/// Keeps the learners of the units this node leads equal to their
/// descriptors', and the descriptors' learners at `placement.read_copies`,
/// until `stop`.
pub async fn follow(dir: Arc<UnitDirectory>, store: Arc<dyn MetaStore>, stop: CancellationToken) {
    let mut pass = 0u32;
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(EVERY) => {}
        }
        pass = pass.wrapping_add(1);
        if pass.is_multiple_of(SETTING_EVERY) {
            apply_settings(&dir, store.as_ref()).await;
        }
        let dir = Arc::clone(&dir);
        let _ = tokio::task::spawn_blocking(move || reconcile(&dir)).await;
        publish_read_counts();
    }
}

/// `tenant`'s `placement.read_copies`, from its latest catalog.
async fn wanted(tenant: u64) -> usize {
    let n = crate::reader::tenant_value_latest(&READ_COPIES, tenant).await;
    usize::try_from(n.min(MAX_READ_COPIES)).unwrap_or(0)
}

/// The units this node leads whose descriptor lists a different number of
/// learners than their tenant's `placement.read_copies`: their descriptors
/// rewritten.
async fn apply_settings(dir: &UnitDirectory, store: &dyn MetaStore) {
    let (Some(node), Ok(engine)) = (
        crate::node_engine::raft_node(),
        crate::node_engine::engine(),
    ) else {
        return;
    };
    let mut nodes: Option<Vec<NodeRecord>> = None;
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 || engine.leader_epoch(sprig_engine::UnitId(g)) == 0 {
            continue;
        }
        let Some(d) = dir.get(g) else { continue };
        if d.unit != d.tenant {
            continue;
        }
        // A voter on a lost node: the unit is being re-replicated, and its
        // learner is the replacement catching up, not a read copy.
        if crate::ops::replicate::short_of_copies(g) {
            continue;
        }
        // A replica move lists its target as a learner until it is done.
        if d.moving.is_some() {
            continue;
        }
        let want = wanted(d.tenant).await;
        if d.learners.len() == want {
            continue;
        }
        if nodes.is_none() {
            nodes = sprig_catalog::units::nodes(store).await.ok();
        }
        let Some(all) = nodes.as_deref() else { return };
        let counts = dir.counts();
        let r = set_learners(store, d.tenant, |d| {
            let next = choose(d, want, all, &counts);
            if next == d.learners {
                return false;
            }
            d.learners = next;
            true
        })
        .await;
        match r {
            Ok(Some(d)) => {
                dir.put(d.clone());
                tracing::info!(tenant = d.tenant, learners = ?d.learners, "read copies: the unit's descriptor follows placement.read_copies");
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(tenant = d.tenant, error = %e, "read copies: descriptor not rewritten now");
            }
        }
    }
}

/// The learners a unit should list for `want` copies: the ones it lists
/// (the first `want`), then active nodes placing units per tenant that
/// hold none of its replicas, in failure domains the unit doesn't use yet
/// first, fewest units first.
#[must_use]
pub fn choose(
    d: &UnitDescriptor,
    want: usize,
    nodes: &[NodeRecord],
    counts: &BTreeMap<u64, (usize, usize)>,
) -> Vec<u64> {
    let mut out: Vec<u64> = d.learners.iter().copied().take(want).collect();
    let domain = |id: u64| {
        nodes.iter().find(|n| n.id == id).map_or_else(
            || format!("node-{id}"),
            |n| {
                if n.failure_domain.is_empty() {
                    format!("node-{}", n.id)
                } else {
                    n.failure_domain.clone()
                }
            },
        )
    };
    let mut candidates: Vec<&NodeRecord> = nodes
        .iter()
        .filter(|n| n.state == "active" && n.placement)
        .filter(|n| !d.voters.contains(&n.id) && !out.contains(&n.id))
        .collect();
    while out.len() < want && !candidates.is_empty() {
        let used: Vec<String> = d
            .voters
            .iter()
            .chain(out.iter())
            .map(|n| domain(*n))
            .collect();
        let best = candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, n)| {
                let d = domain(n.id);
                (
                    used.contains(&d),
                    counts.get(&n.id).map_or(0, |c| c.0),
                    n.id,
                )
            })
            .map(|(i, _)| i);
        let Some(i) = best else { break };
        out.push(candidates.remove(i).id);
    }
    out
}

/// Rewrites `tenant`'s own unit's descriptor with `f`, compare-and-set,
/// raising its generation; `f` returns `false` to leave it. The descriptor
/// stored, `None` when there is none or `f` left it.
async fn set_learners(
    store: &dyn MetaStore,
    tenant: u64,
    mut f: impl FnMut(&mut UnitDescriptor) -> bool,
) -> Result<Option<UnitDescriptor>> {
    let key = sprig_catalog::units::unit_key(store, tenant, tenant)?;
    for _ in 0..5 {
        let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        let mut d = Record::<UnitDescriptor>::decode(&kv.value)?.body;
        if !f(&mut d) {
            return Ok(None);
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
    Ok(None)
}

/// Reads served on this node's copies, by how they were let through
/// (`local`: at once; `waited`: after a short wait; `barrier`: after the
/// leader confirmed its commits).
fn publish_read_counts() {
    let (local, waited, barrier) = sprig_engine::reads::stats().get();
    for (served, n) in [("local", local), ("waited", waited), ("barrier", barrier)] {
        metrics::counter!("sprig_unit_follower_reads_total", "served" => served).absolute(n);
    }
}

/// One pass: at most one change per unit (a change waits for the previous
/// one). Returns how many changes it proposed.
fn reconcile(dir: &UnitDirectory) -> usize {
    let (Some(node), Ok(engine)) = (
        crate::node_engine::raft_node(),
        crate::node_engine::engine(),
    ) else {
        return 0;
    };
    let mut n = 0;
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 || engine.leader_epoch(sprig_engine::UnitId(g)) == 0 {
            continue;
        }
        let Some(d) = dir.get(g) else { continue };
        // (Re-replication, or a replica move's leader, owns the unit's
        // learners meanwhile.)
        if d.unit != g || crate::ops::replicate::short_of_copies(g) || d.moving.is_some() {
            continue;
        }
        let Some(st) = node.status(g) else { continue };
        let single = |t: ConfChangeType, id: u64| {
            let mut c = ConfChangeSingle::default();
            c.set_change_type(t);
            c.node_id = id;
            c
        };
        let add = d
            .learners
            .iter()
            .find(|l| !st.learners.contains(l) && !st.voters.contains(l))
            .map(|l| single(ConfChangeType::AddLearnerNode, *l));
        let remove = st
            .learners
            .iter()
            .find(|l| !d.learners.contains(l))
            .map(|l| single(ConfChangeType::RemoveNode, *l));
        let Some(change) = add.or(remove) else {
            continue;
        };
        let what = (change.get_change_type(), change.node_id);
        let mut cc = ConfChangeV2::default();
        cc.mut_changes().push(change);
        match node.change_membership(g, cc) {
            Ok(_) => {
                n += 1;
                tracing::info!(unit = g, change = ?what.0, node = what.1, "read copies: changing the unit's learners to match its descriptor");
            }
            Err(e) => {
                tracing::debug!(unit = g, error = %e, "read copies: learner change not proposed now");
            }
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, domain: &str) -> NodeRecord {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "gateway_id": format!("gw{id}"),
            "addr": "127.0.0.1",
            "failure_domain": domain,
            "placement": true,
            "state": "active",
        }))
        .expect("a node record")
    }

    #[test]
    fn read_copies_go_to_new_domains_then_to_the_emptiest_nodes() {
        let d = UnitDescriptor {
            tenant: 7,
            unit: 7,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            home: 1,
            generation: 1,
            moving: None,
            last_move: None,
        };
        let nodes = [
            node(1, "a"),
            node(2, "b"),
            node(3, "c"),
            node(4, "a"),
            node(5, "d"),
            node(6, "e"),
        ];
        let counts = BTreeMap::from([(4, (0, 0)), (5, (9, 0)), (6, (2, 0))]);
        assert_eq!(choose(&d, 1, &nodes, &counts), vec![6]);
        assert_eq!(choose(&d, 2, &nodes, &counts), vec![6, 5]);
        assert_eq!(choose(&d, 3, &nodes, &counts), vec![6, 5, 4]);
        assert_eq!(choose(&d, 9, &nodes, &counts).len(), 3);
        let with = UnitDescriptor {
            learners: vec![5, 4],
            ..d
        };
        assert_eq!(choose(&with, 1, &nodes, &counts), vec![5]);
        assert!(choose(&with, 0, &nodes, &counts).is_empty());
    }
}
