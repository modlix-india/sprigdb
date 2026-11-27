//! Placing a new tenant's engine unit on nodes: three voters in distinct
//! failure domains, the least loaded first, and a home node to lead it.
//!
//! ```text
//! place(nodes, copies):
//!   usable := nodes that are ready
//!   per failure domain, its node with the fewest units (ties by id);
//!     a node with no failure domain is a domain of its own
//!   take the `copies` domains whose best node has the fewest units
//!     (ties by id); fewer usable domains than copies: 39120
//!   home := the chosen node home to the fewest units (ties by id)
//! ```
//!
//! Pure and deterministic over what the caller counted, so a rerun with the
//! same counts chooses the same nodes; the controller stores the first
//! choice once, so a rerun after a crash keeps it whatever the counts are
//! then.

use std::collections::BTreeMap;

use sprig_common::{Result, SprigCode, SprigError};

/// A node as placement sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLoad {
    /// Its Raft id.
    pub id: u64,
    /// Its failure domain (rack, zone); empty: a domain of its own.
    pub failure_domain: String,
    /// Units it holds a copy of.
    pub units: usize,
    /// Units it is home to.
    pub homes: usize,
}

/// Where a new unit goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The voters, by id.
    pub voters: Vec<u64>,
    /// The voter that should lead it.
    pub home: u64,
}

/// Chooses `copies` voters in distinct failure domains and a home among
/// them (module docs).
///
/// # Errors
/// 39120 `ER_SPRIG_PLACEMENT_UNAVAILABLE` when fewer than `copies` failure
/// domains have a usable node.
pub fn place(nodes: &[NodeLoad], copies: usize) -> Result<Placement> {
    let mut best: BTreeMap<String, &NodeLoad> = BTreeMap::new();
    for n in nodes {
        let domain = if n.failure_domain.is_empty() {
            format!("\u{0}node-{}", n.id)
        } else {
            n.failure_domain.clone()
        };
        let better = best
            .get(&domain)
            .is_none_or(|b| (n.units, n.id) < (b.units, b.id));
        if better {
            best.insert(domain, n);
        }
    }
    let mut ranked: Vec<&NodeLoad> = best.into_values().collect();
    ranked.sort_by_key(|n| (n.units, n.id));
    if copies == 0 || ranked.len() < copies {
        return Err(SprigError::new(
            SprigCode::PlacementUnavailable,
            format!(
                "a unit needs {copies} nodes in distinct failure domains; {} are available",
                ranked.len()
            ),
        ));
    }
    let chosen = &ranked[..copies];
    let home = chosen
        .iter()
        .min_by_key(|n| (n.homes, n.id))
        .map_or(0, |n| n.id);
    let mut voters: Vec<u64> = chosen.iter().map(|n| n.id).collect();
    voters.sort_unstable();
    Ok(Placement { voters, home })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: u64, fd: &str, units: usize, homes: usize) -> NodeLoad {
        NodeLoad {
            id,
            failure_domain: fd.into(),
            units,
            homes,
        }
    }

    #[test]
    fn three_least_loaded_nodes_in_distinct_domains() {
        let nodes = [
            n(1, "a", 5, 2),
            n(2, "a", 1, 0),
            n(3, "b", 3, 1),
            n(4, "c", 2, 1),
            n(5, "c", 9, 4),
        ];
        let p = place(&nodes, 3).unwrap();
        // Domain a's best is 2, b's is 3, c's is 4.
        assert_eq!(p.voters, vec![2, 3, 4]);
        assert_eq!(p.home, 2);
    }

    #[test]
    fn nodes_without_a_domain_are_domains_of_their_own() {
        let nodes = [
            n(1, "", 0, 0),
            n(2, "", 0, 0),
            n(3, "", 0, 0),
            n(4, "", 0, 0),
            n(5, "", 0, 0),
        ];
        let p = place(&nodes, 3).unwrap();
        assert_eq!(p.voters, vec![1, 2, 3]);
        assert_eq!(p.home, 1);
        // Counted after that placement, the next one goes to the others.
        let nodes = [
            n(1, "", 1, 1),
            n(2, "", 1, 0),
            n(3, "", 1, 0),
            n(4, "", 0, 0),
            n(5, "", 0, 0),
        ];
        let p = place(&nodes, 3).unwrap();
        assert_eq!(p.voters, vec![1, 4, 5]);
        assert_eq!(p.home, 4);
    }

    #[test]
    fn too_few_domains_is_placement_unavailable() {
        let nodes = [n(1, "a", 0, 0), n(2, "a", 0, 0), n(3, "b", 0, 0)];
        let e = place(&nodes, 3).unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::PlacementUnavailable));
        assert!(place(&[], 3).is_err());
        assert!(place(&nodes, 0).is_err());
        assert_eq!(place(&nodes, 2).unwrap().voters, vec![1, 3]);
    }

    #[test]
    fn many_placements_spread_evenly() {
        let mut nodes: Vec<NodeLoad> = (1..=5).map(|i| n(i, "", 0, 0)).collect();
        for _ in 0..1_000 {
            let p = place(&nodes, 3).unwrap();
            for v in &p.voters {
                nodes.iter_mut().find(|x| x.id == *v).unwrap().units += 1;
            }
            nodes.iter_mut().find(|x| x.id == p.home).unwrap().homes += 1;
        }
        let units: Vec<usize> = nodes.iter().map(|x| x.units).collect();
        let homes: Vec<usize> = nodes.iter().map(|x| x.homes).collect();
        assert_eq!(units.iter().sum::<usize>(), 3_000);
        assert!(units.iter().all(|u| *u == 600), "{units:?}");
        assert!(homes.iter().all(|h| (199..=201).contains(h)), "{homes:?}");
    }
}
