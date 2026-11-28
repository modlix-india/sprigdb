//! Rebalancing engine units over nodes: replica moves and leadership
//! (home) moves, so units and leaders spread evenly, a new node takes its
//! share, and nothing moves back and forth.
//!
//! ```text
//! plan(nodes, units):
//!   usable := nodes that are active, place units and have a ready gateway
//!   counts : a pending move counts as done (its source -1, its target +1)
//!   leaders: while one usable node is home to >= 2 more units than another,
//!            a unit homed on the most-homed node gets a new home: one of its
//!            voters on a usable node with >= 2 fewer homes (fewest first)
//!   replicas, within max_pending pending moves and max_incoming per node:
//!            while one usable node holds >= 2 more units than another, move
//!            a unit from the fullest to the emptiest pair that has one:
//!            a voter on the source, no copy on the target, no move pending,
//!            not moved within the cooldown, never back to a node it left
//!            within the no-return window, the target's failure domain not
//!            used by its other voters; units homed on the source first when
//!            the source has more homes, then the smallest, ties by a hash
//! ```
//!
//! Every move lowers the sum of the squared counts by at least 2, so the
//! plans of successive passes end, and moving a unit back would need the
//! counts to tilt by 2 the other way first; the cooldown and the no-return
//! window guard that as well. Pure and deterministic over its inputs.

use std::collections::{BTreeMap, HashSet};

/// A node as the balancer sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Its Raft id.
    pub id: u64,
    /// Its failure domain; empty: a domain of its own.
    pub failure_domain: String,
    /// Active, places units, gateway ready: may give and take units.
    pub usable: bool,
}

/// A unit as the balancer sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit {
    /// Its tenant (the unit's id).
    pub tenant: u64,
    /// Its voters.
    pub voters: Vec<u64>,
    /// Its learners (read copies, a move's target).
    pub learners: Vec<u64>,
    /// Its preferred leader.
    pub home: u64,
    /// A move pending: (from, to).
    pub moving: Option<(u64, u64)>,
    /// The last finished move: (from, to, finished at, Unix ms).
    pub last_move: Option<(u64, u64, u64)>,
    /// Its size in bytes, when known (0: unknown).
    pub bytes: u64,
}

/// The balancer's limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rules {
    /// Replica moves pending at once in the DC.
    pub max_pending: usize,
    /// Replica moves pending at once into one node.
    pub max_incoming: usize,
    /// Home moves per pass.
    pub max_home_moves: usize,
    /// No replica move of a unit within this long of its last (ms).
    pub unit_cooldown_ms: u64,
    /// No move of a unit back to the node it left within this long (ms).
    pub no_return_ms: u64,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            max_pending: 4,
            max_incoming: 2,
            max_home_moves: 16,
            unit_cooldown_ms: 24 * 3_600_000,
            no_return_ms: 7 * 24 * 3_600_000,
        }
    }
}

/// One planned action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Move the unit's replica from `from` to `to`.
    Move {
        /// The unit's tenant.
        tenant: u64,
        /// The voter that leaves.
        from: u64,
        /// The node that joins.
        to: u64,
        /// Why, with the counts.
        reason: String,
    },
    /// Name `to` (a voter) the unit's home.
    Home {
        /// The unit's tenant.
        tenant: u64,
        /// Its home now.
        from: u64,
        /// Its new home.
        to: u64,
        /// Why, with the counts.
        reason: String,
    },
}

/// Units and homes per node, as [`plan`] counts them (pending moves as
/// done).
#[must_use]
pub fn counts(nodes: &[Node], units: &[Unit]) -> BTreeMap<u64, (usize, usize)> {
    let mut c: BTreeMap<u64, (usize, usize)> = nodes.iter().map(|n| (n.id, (0, 0))).collect();
    for u in units {
        for v in &u.voters {
            if u.moving.is_some_and(|(f, _)| f == *v) {
                continue;
            }
            c.entry(*v).or_default().0 += 1;
        }
        if let Some((_, to)) = u.moving
            && !u.voters.contains(&to)
        {
            c.entry(to).or_default().0 += 1;
        }
        c.entry(u.home).or_default().1 += 1;
    }
    c
}

fn mix(a: u64, b: u64) -> u64 {
    let mut x = a.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ b.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x
}

fn domain(nodes: &BTreeMap<u64, &Node>, id: u64) -> String {
    match nodes.get(&id) {
        Some(n) if !n.failure_domain.is_empty() => n.failure_domain.clone(),
        _ => format!("\u{0}node-{id}"),
    }
}

/// The actions of one pass (module docs).
#[must_use]
#[allow(clippy::too_many_lines)] // two phases, step by step
pub fn plan(nodes: &[Node], units: &[Unit], rules: &Rules, now_ms: u64) -> Vec<Action> {
    let by_id: BTreeMap<u64, &Node> = nodes.iter().map(|n| (n.id, n)).collect();
    let usable: Vec<u64> = nodes.iter().filter(|n| n.usable).map(|n| n.id).collect();
    if usable.len() < 2 {
        return Vec::new();
    }
    let mut c = counts(nodes, units);
    let mut out = Vec::new();
    let units_of = |c: &BTreeMap<u64, (usize, usize)>, n: u64| c.get(&n).map_or(0, |x| x.0);
    let homes_of = |c: &BTreeMap<u64, (usize, usize)>, n: u64| c.get(&n).map_or(0, |x| x.1);
    let mut changed: HashSet<u64> = HashSet::new();

    // Leaders: homes even out among usable nodes, without copying.
    for _ in 0..rules.max_home_moves {
        let mut by_homes = usable.clone();
        by_homes.sort_by_key(|n| (std::cmp::Reverse(homes_of(&c, *n)), *n));
        let mut done = false;
        for &hi in &by_homes {
            let h = homes_of(&c, hi);
            let best = units
                .iter()
                .filter(|u| u.home == hi && u.moving.is_none() && !changed.contains(&u.tenant))
                .filter_map(|u| {
                    u.voters
                        .iter()
                        .copied()
                        .filter(|v| *v != hi && usable.contains(v) && homes_of(&c, *v) + 2 <= h)
                        .min_by_key(|v| (homes_of(&c, *v), *v))
                        .map(|v| (u, v))
                })
                .min_by_key(|(u, v)| (homes_of(&c, *v), mix(u.tenant, *v)));
            if let Some((u, to)) = best {
                out.push(Action::Home {
                    tenant: u.tenant,
                    from: hi,
                    to,
                    reason: format!(
                        "node {hi} is home to {h} units, node {to} to {}",
                        homes_of(&c, to)
                    ),
                });
                changed.insert(u.tenant);
                if let Some(x) = c.get_mut(&hi) {
                    x.1 -= 1;
                }
                c.entry(to).or_default().1 += 1;
                done = true;
                break;
            }
        }
        if !done {
            break;
        }
    }

    // Replicas: units even out among usable nodes.
    let mut pending = units.iter().filter(|u| u.moving.is_some()).count();
    let mut incoming: BTreeMap<u64, usize> = BTreeMap::new();
    for u in units {
        if let Some((_, to)) = u.moving {
            *incoming.entry(to).or_default() += 1;
        }
    }
    while pending < rules.max_pending {
        let mut desc = usable.clone();
        desc.sort_by_key(|n| (std::cmp::Reverse(units_of(&c, *n)), *n));
        let mut asc = usable.clone();
        asc.sort_by_key(|n| (units_of(&c, *n), *n));
        let mut chosen: Option<(&Unit, u64, u64)> = None;
        'pairs: for &dst in &asc {
            if incoming.get(&dst).copied().unwrap_or(0) >= rules.max_incoming {
                continue;
            }
            for &src in &desc {
                if units_of(&c, src) < units_of(&c, dst) + 2 {
                    break;
                }
                let src_more_homes = homes_of(&c, src) > homes_of(&c, dst) + 1;
                let pick = units
                    .iter()
                    .filter(|u| {
                        u.voters.contains(&src)
                            && !u.voters.contains(&dst)
                            && !u.learners.contains(&dst)
                            && u.moving.is_none()
                            && !changed.contains(&u.tenant)
                    })
                    .filter(|u| {
                        u.last_move.is_none_or(|(from, _, at)| {
                            let since = now_ms.saturating_sub(at);
                            since >= rules.unit_cooldown_ms
                                && (from != dst || since >= rules.no_return_ms)
                        })
                    })
                    .filter(|u| {
                        let d = domain(&by_id, dst);
                        !u.voters
                            .iter()
                            .filter(|v| **v != src)
                            .any(|v| domain(&by_id, *v) == d)
                    })
                    .min_by_key(|u| {
                        (
                            !(src_more_homes && u.home == src),
                            u.bytes,
                            mix(u.tenant, dst),
                        )
                    });
                if let Some(u) = pick {
                    chosen = Some((u, src, dst));
                    break 'pairs;
                }
            }
        }
        let Some((u, src, dst)) = chosen else { break };
        out.push(Action::Move {
            tenant: u.tenant,
            from: src,
            to: dst,
            reason: format!(
                "node {src} holds {} units, node {dst} {}",
                units_of(&c, src),
                units_of(&c, dst)
            ),
        });
        changed.insert(u.tenant);
        pending += 1;
        *incoming.entry(dst).or_default() += 1;
        if let Some(x) = c.get_mut(&src) {
            x.0 -= 1;
        }
        c.entry(dst).or_default().0 += 1;
        if u.home == src {
            if let Some(x) = c.get_mut(&src) {
                x.1 -= 1;
            }
            c.entry(dst).or_default().1 += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64) -> Node {
        Node {
            id,
            failure_domain: String::new(),
            usable: true,
        }
    }

    fn unit(tenant: u64, voters: [u64; 3], home: u64) -> Unit {
        Unit {
            tenant,
            voters: voters.to_vec(),
            learners: Vec::new(),
            home,
            moving: None,
            last_move: None,
            bytes: 0,
        }
    }

    /// Applies a pass's actions as the cluster would once they finish.
    fn apply(units: &mut [Unit], actions: &[Action], now: u64) {
        for a in actions {
            match a {
                Action::Move {
                    tenant, from, to, ..
                } => {
                    let u = units.iter_mut().find(|u| u.tenant == *tenant).unwrap();
                    u.voters.retain(|v| v != from);
                    u.voters.push(*to);
                    u.voters.sort_unstable();
                    if u.home == *from {
                        u.home = *to;
                    }
                    u.last_move = Some((*from, *to, now));
                }
                Action::Home { tenant, to, .. } => {
                    units.iter_mut().find(|u| u.tenant == *tenant).unwrap().home = *to;
                }
            }
        }
    }

    /// 5 nodes, 100 units placed evenly (60 units, 20 homes per node).
    fn five_full() -> (Vec<Node>, Vec<Unit>) {
        let nodes: Vec<Node> = (1..=5).map(node).collect();
        let mut units = Vec::new();
        for t in 0..100u64 {
            let a = t % 5 + 1;
            let b = (t + 1) % 5 + 1;
            let c = (t + 2) % 5 + 1;
            let mut v = [a, b, c];
            v.sort_unstable();
            units.push(unit(1000 + t, v, a));
        }
        (nodes, units)
    }

    #[test]
    fn an_even_cluster_plans_nothing() {
        let (nodes, units) = five_full();
        let c = counts(&nodes, &units);
        assert!(c.values().all(|x| *x == (60, 20)), "{c:?}");
        assert!(plan(&nodes, &units, &Rules::default(), 0).is_empty());
    }

    #[test]
    fn a_sixth_node_takes_its_share_and_nothing_moves_back() {
        let (mut nodes, mut units) = five_full();
        nodes.push(node(6));
        let rules = Rules::default();
        let mut moved: Vec<(u64, u64, u64)> = Vec::new();
        let mut passes = 0;
        // Each pass's moves finish before the next (an hour apart).
        for pass in 1..=200u64 {
            let now = pass * 3_600_000;
            let a = plan(&nodes, &units, &rules, now);
            if a.is_empty() {
                break;
            }
            passes = pass;
            for x in &a {
                if let Action::Move {
                    tenant, from, to, ..
                } = x
                {
                    moved.push((*tenant, *from, *to));
                }
            }
            apply(&mut units, &a, now);
        }
        let c = counts(&nodes, &units);
        let (min_u, max_u) = (
            c.values().map(|x| x.0).min().unwrap(),
            c.values().map(|x| x.0).max().unwrap(),
        );
        let (min_h, max_h) = (
            c.values().map(|x| x.1).min().unwrap(),
            c.values().map(|x| x.1).max().unwrap(),
        );
        assert!(max_u - min_u <= 1, "units spread: {c:?}");
        assert!(max_h - min_h <= 1, "homes spread: {c:?}");
        assert_eq!(c[&6].0, 50, "300 copies over 6 nodes");
        // Only what the new node needs moved, and never back.
        assert_eq!(moved.len(), 50);
        assert!(moved.iter().all(|(_, _, to)| *to == 6), "{moved:?}");
        let units_moved: HashSet<u64> = moved.iter().map(|m| m.0).collect();
        assert_eq!(units_moved.len(), moved.len(), "each unit moved once");
        assert!(
            passes <= 26,
            "two copies into the new node per pass: 25 passes ({passes})"
        );
        // Settled: another pass much later plans nothing.
        assert!(plan(&nodes, &units, &rules, 10_000 * 3_600_000).is_empty());
    }

    #[test]
    fn limits_cooldowns_and_failure_domains_hold() {
        let (mut nodes, mut units) = five_full();
        nodes.push(node(6));
        let rules = Rules::default();
        let a = plan(&nodes, &units, &rules, 0);
        let moves = a
            .iter()
            .filter(|x| matches!(x, Action::Move { .. }))
            .count();
        assert_eq!(moves, rules.max_incoming, "two copies into node 6 at once");
        // Pending moves count as done and use the slots.
        for x in &a {
            if let Action::Move {
                tenant, from, to, ..
            } = x
            {
                units
                    .iter_mut()
                    .find(|u| u.tenant == *tenant)
                    .unwrap()
                    .moving = Some((*from, *to));
            }
        }
        assert!(
            plan(&nodes, &units, &rules, 0)
                .iter()
                .all(|x| !matches!(x, Action::Move { .. }))
        );
        // A unit just moved doesn't move again, nor back.
        let mut u = unit(1, [1, 2, 3], 1);
        u.last_move = Some((4, 1, 1_000));
        let nodes4: Vec<Node> = (1..=4).map(node).collect();
        let fill = vec![u, unit(2, [1, 2, 3], 2)];
        let p = plan(&nodes4, &fill, &rules, 2_000);
        assert!(
            p.iter()
                .all(|x| !matches!(x, Action::Move { tenant: 1, .. }))
        );
        // Failure domains: node 6 shares a domain with node 1, so a unit on
        // {1, ...} that moves off node 2 can't land there.
        let nodes_fd = vec![
            Node {
                id: 1,
                failure_domain: "a".into(),
                usable: true,
            },
            Node {
                id: 2,
                failure_domain: "b".into(),
                usable: true,
            },
            Node {
                id: 3,
                failure_domain: "c".into(),
                usable: true,
            },
            Node {
                id: 4,
                failure_domain: "a".into(),
                usable: true,
            },
        ];
        let us = vec![unit(1, [1, 2, 3], 1), unit(2, [1, 2, 3], 2)];
        for x in plan(&nodes_fd, &us, &rules, 0) {
            if let Action::Move { from, to, .. } = x {
                assert_eq!((from, to), (1, 4), "only node 1's copy may go to domain a");
            }
        }
    }

    #[test]
    fn homes_even_out_without_copies() {
        let nodes: Vec<Node> = (1..=3).map(node).collect();
        let units: Vec<Unit> = (0..9).map(|t| unit(t, [1, 2, 3], 1)).collect();
        let a = plan(&nodes, &units, &Rules::default(), 0);
        assert!(a.iter().all(|x| matches!(x, Action::Home { .. })));
        let mut u = units.clone();
        apply(&mut u, &a, 0);
        let c = counts(&nodes, &u);
        assert!(c.values().all(|x| x.1 == 3), "{c:?}");
        assert!(plan(&nodes, &u, &Rules::default(), 0).is_empty());
        // Unusable nodes neither give nor take.
        let mut n2 = nodes.clone();
        n2[2].usable = false;
        let a = plan(&n2, &units, &Rules::default(), 0);
        assert!(a.iter().all(|x| match x {
            Action::Home { to, .. } => *to != 3,
            Action::Move { .. } => false,
        }));
    }
}
