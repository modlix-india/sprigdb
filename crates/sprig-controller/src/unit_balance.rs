//! The `unit_balance` loop: the balancer for engine units placed per
//! tenant. On the DC leader, every `balancer.interval_seconds`:
//!
//! ```text
//! nodes    := node records; usable = active, placing units, gateway ready
//! units    := unit descriptors (voters, learners, home, pending and last move)
//! actions  := sprig_balancer::nodes::plan(nodes, units)   (home moves first,
//!             then replica moves within balancer.max_concurrent_moves_per_dc)
//! balancer.mode auto:            a home move rewrites the descriptor's home
//!                                (the new home's node asks for the
//!                                leadership); a replica move writes the move
//!                                into the descriptor, and the unit's leader
//!                                carries it out
//! recommend, dry_run, approve:   the actions are logged as decisions only
//! off:                           nothing
//! ```
//!
//! Nothing here copies data or talks to nodes: the descriptors are the
//! interface, so a controller failover loses nothing (the next leader plans
//! again from them). A DC with no node placing units per tenant plans
//! nothing.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::Serialize;
use sprig_balancer::nodes::{self, Action, Node, Rules, Unit};
use sprig_catalog::units::{self, NODE_ACTIVE};
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_meta::MetaStore;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// A unit never goes back to a node it left within this long.
pub const NO_RETURN: Duration = Duration::from_secs(7 * 24 * 3600);

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BalanceTick;

impl fmt::Display for BalanceTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
pub struct UnitBalanceLoop {
    live: Option<(LiveSettings, DcId)>,
}

impl fmt::Debug for UnitBalanceLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnitBalanceLoop").finish_non_exhaustive()
    }
}

/// The balancer's settings for one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Knobs {
    /// `balancer.mode`.
    pub mode: String,
    /// `balancer.interval_seconds`.
    pub interval: Duration,
    /// The planner's limits.
    pub rules: Rules,
}

/// Reads the balancer's settings (defaults without `live`).
#[must_use]
pub fn knobs(live: Option<&(LiveSettings, DcId)>) -> Knobs {
    use sprig_balancer::settings as s;
    let (mode, interval, pending, cooldown_h) = match live {
        Some((l, dc)) => {
            let ctx = SettingCtx::process(dc);
            (
                l.get(&s::MODE, &ctx).into_owned(),
                l.get(&s::INTERVAL_SECONDS, &ctx),
                l.get(&s::MAX_CONCURRENT_MOVES_PER_DC, &ctx),
                l.get(&s::SHARD_COOLDOWN_HOURS, &ctx),
            )
        }
        None => (
            s::MODE.default.clone().into_owned(),
            s::INTERVAL_SECONDS.default,
            s::MAX_CONCURRENT_MOVES_PER_DC.default,
            s::SHARD_COOLDOWN_HOURS.default,
        ),
    };
    Knobs {
        mode,
        interval: Duration::from_secs(interval.max(1)),
        rules: Rules {
            max_pending: usize::try_from(pending).unwrap_or(usize::MAX),
            unit_cooldown_ms: cooldown_h.saturating_mul(3_600_000),
            no_return_ms: u64::try_from(NO_RETURN.as_millis()).unwrap_or(u64::MAX),
            ..Rules::default()
        },
    }
}

impl UnitBalanceLoop {
    /// The loop, reading its settings from `live` (defaults without).
    #[must_use]
    pub fn new(live: Option<(LiveSettings, DcId)>) -> Self {
        Self { live }
    }
}

/// What the balancer sees and would do now: `sprigctl balancer show`.
#[derive(Debug, Clone, Serialize)]
pub struct BalanceReport {
    /// `balancer.mode`.
    pub mode: String,
    /// Per node: state, usable, units held and homes (pending moves
    /// counted as done).
    pub nodes: Vec<NodeCount>,
    /// Replica moves pending: (tenant, from, to, since Unix ms).
    pub pending: Vec<(u64, u64, u64, u64)>,
    /// What the next pass would do.
    pub planned: Vec<PlannedAction>,
}

/// One node in a [`BalanceReport`].
#[derive(Debug, Clone, Serialize)]
pub struct NodeCount {
    /// Its Raft id.
    pub id: u64,
    /// Its registry state.
    pub state: String,
    /// It gives and takes units.
    pub usable: bool,
    /// Units it holds (as a voter, or will by a pending move).
    pub units: usize,
    /// Units it is home to.
    pub homes: usize,
}

/// One action in a [`BalanceReport`].
#[derive(Debug, Clone, Serialize)]
pub struct PlannedAction {
    /// `unit.move` or `leader.move`.
    pub kind: &'static str,
    /// The unit's tenant.
    pub tenant: u64,
    /// From node.
    pub from: u64,
    /// To node.
    pub to: u64,
    /// Why.
    pub reason: String,
}

impl From<&Action> for PlannedAction {
    fn from(a: &Action) -> Self {
        match a {
            Action::Move {
                tenant,
                from,
                to,
                reason,
            } => Self {
                kind: "unit.move",
                tenant: *tenant,
                from: *from,
                to: *to,
                reason: reason.clone(),
            },
            Action::Home {
                tenant,
                from,
                to,
                reason,
            } => Self {
                kind: "leader.move",
                tenant: *tenant,
                from: *from,
                to: *to,
                reason: reason.clone(),
            },
        }
    }
}

/// The balancer's inputs from the DC tier: the nodes (with whether they
/// are usable), the units, and every node record's state.
///
/// # Errors
/// The store's.
pub async fn view(dc: &dyn MetaStore) -> Result<(Vec<Node>, Vec<Unit>, BTreeMap<u64, String>)> {
    let records = units::nodes(dc).await?;
    let ready = crate::node_health::ready_gateways(dc).await?;
    let nodes: Vec<Node> = records
        .iter()
        .filter(|n| n.placement)
        .map(|n| Node {
            id: n.id,
            failure_domain: n.failure_domain.clone(),
            usable: n.state == NODE_ACTIVE && ready.contains(&n.gateway_id),
        })
        .collect();
    let states = records.into_iter().map(|n| (n.id, n.state)).collect();
    let descs = units::list(dc).await?;
    let us = descs
        .into_iter()
        .filter(|d| d.unit == d.tenant && d.tenant != 0)
        .map(|d| Unit {
            tenant: d.tenant,
            voters: d.voters,
            learners: d.learners,
            home: d.home,
            moving: d.moving.map(|m| (m.from, m.to)),
            last_move: d.last_move.map(|m| (m.from, m.to, m.since_ms)),
            bytes: 0,
        })
        .collect();
    Ok((nodes, us, states))
}

/// What the balancer sees and would do now, without doing it.
///
/// # Errors
/// The store's.
pub async fn report(dc: &dyn MetaStore, k: &Knobs) -> Result<BalanceReport> {
    let (ns, us, states) = view(dc).await?;
    let counts = nodes::counts(&ns, &us);
    let planned = nodes::plan(&ns, &us, &k.rules, now_ms());
    Ok(BalanceReport {
        mode: k.mode.clone(),
        nodes: ns
            .iter()
            .map(|n| {
                let (u, h) = counts.get(&n.id).copied().unwrap_or_default();
                NodeCount {
                    id: n.id,
                    state: states.get(&n.id).cloned().unwrap_or_default(),
                    usable: n.usable,
                    units: u,
                    homes: h,
                }
            })
            .collect(),
        pending: us
            .iter()
            .filter_map(|u| u.moving.map(|(f, t)| (u.tenant, f, t, 0)))
            .collect(),
        planned: planned.iter().map(PlannedAction::from).collect(),
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Carries out one action (mode `auto`).
async fn act(dc: &dyn MetaStore, a: &Action) -> Result<()> {
    match a {
        Action::Move {
            tenant,
            from,
            to,
            reason,
        } => {
            units::start_move(dc, *tenant, *from, *to, &format!("balancer: {reason}")).await?;
        }
        Action::Home { tenant, to, .. } => {
            units::set_home(dc, *tenant, *to).await?;
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl Reconciler for UnitBalanceLoop {
    type Key = BalanceTick;

    fn name(&self) -> &'static str {
        "unit_balance"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<BalanceTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<BalanceTick>> {
        Ok(vec![BalanceTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &BalanceTick) -> Result<Outcome> {
        cx.leader()?;
        let k = knobs(self.live.as_ref());
        if k.mode == "off" {
            return Ok(Outcome::RequeueAfter(k.interval));
        }
        let started = std::time::Instant::now();
        let (ns, us, _) = view(cx.dc.as_ref()).await?;
        if ns.iter().filter(|n| n.usable).count() < 2 {
            return Ok(Outcome::RequeueAfter(k.interval));
        }
        for (id, (u, h)) in nodes::counts(&ns, &us) {
            let node = id.to_string();
            let n = |x: usize| f64::from(u32::try_from(x).unwrap_or(u32::MAX));
            metrics::gauge!("sprig_balancer_units", "node" => node.clone(), "role" => "voter")
                .set(n(u));
            metrics::gauge!("sprig_balancer_units", "node" => node, "role" => "home").set(n(h));
        }
        let actions = nodes::plan(&ns, &us, &k.rules, now_ms());
        metrics::histogram!("sprig_balancer_plan_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        let apply = k.mode == "auto";
        for a in &actions {
            let p = PlannedAction::from(a);
            let outcome = if apply {
                match act(cx.dc.as_ref(), a).await {
                    Ok(()) => "scheduled",
                    Err(e) => {
                        tracing::debug!(tenant = p.tenant, error = %e, "balancer: an action wasn't taken now");
                        "failed"
                    }
                }
            } else {
                "recommended"
            };
            let kind = if p.kind == "unit.move" {
                "replica"
            } else {
                "leader"
            };
            metrics::counter!("sprig_balancer_unit_moves_total", "kind" => kind, "outcome" => outcome)
                .increment(1);
            tracing::info!(event = "decision", component = "balancer", kind = p.kind,
                tenant = p.tenant, from = p.from, to = p.to, mode = %k.mode, outcome,
                reason = %p.reason, "balancer: an engine unit or its leadership moves");
        }
        Ok(Outcome::RequeueAfter(k.interval))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_meta::{MemStore, Op, Record, Tier, Versioned};

    #[tokio::test]
    async fn the_view_reads_nodes_descriptors_and_readiness() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Gw {
            gateway_id: String,
            state: String,
        }
        impl Versioned for Gw {
            const V: u32 = 1;
        }
        let s = MemStore::new(Tier::Dc(DcId::new("dc-b").unwrap()));
        for id in 1..=4u64 {
            let rec: units::NodeRecord = serde_json::from_value(serde_json::json!({
                "id": id, "gateway_id": format!("gw{id}"), "addr": "127.0.0.1",
                "placement": true, "state": if id == 4 { "draining" } else { "active" },
            }))
            .unwrap();
            units::register_node(&s, &rec).await.unwrap();
            let key = s.tier().key(&format!("gateways/gw{id}")).unwrap();
            let v = Record::new(Gw {
                gateway_id: format!("gw{id}"),
                state: if id == 3 { "starting" } else { "ready" }.into(),
            })
            .encode()
            .unwrap();
            s.txn(sprig_meta::Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key,
                    value: v,
                    lease: None,
                }],
                otherwise: Vec::new(),
            })
            .await
            .unwrap();
        }
        let d = units::UnitDescriptor {
            tenant: 7,
            unit: 7,
            voters: vec![1, 2, 3],
            learners: Vec::new(),
            home: 1,
            generation: 0,
            moving: None,
            last_move: None,
        };
        units::place_once(&s, &d, &[]).await.unwrap();
        let (ns, us, states) = view(&s).await.unwrap();
        let usable: Vec<u64> = ns.iter().filter(|n| n.usable).map(|n| n.id).collect();
        assert_eq!(usable, vec![1, 2], "3 isn't ready, 4 is draining");
        assert_eq!(us.len(), 1);
        assert_eq!(states[&4], "draining");
        // A move written into the descriptor shows as pending.
        units::start_move(&s, 7, 1, 4, "test").await.unwrap();
        let r = report(&s, &knobs(None)).await.unwrap();
        assert_eq!(r.pending, vec![(7, 1, 4, 0)]);
        let n4 = r.nodes.iter().find(|n| n.id == 4).unwrap();
        assert_eq!(
            (n4.units, n4.homes),
            (1, 1),
            "the target holds it and is home"
        );
        // A second move of the same unit waits for the first.
        assert!(units::start_move(&s, 7, 2, 1, "test").await.is_err());
        let done = units::end_move(&s, 7, 1, 4, &units::MoveEnd::Done(vec![2, 3, 4]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(done.voters, vec![2, 3, 4]);
        assert_eq!(done.home, 4);
        assert!(done.learners.is_empty() && done.moving.is_none());
        assert_eq!(done.last_move.map(|m| (m.from, m.to)), Some((1, 4)));
    }
}
