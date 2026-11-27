//! The `node_health` loop: which engine nodes of the DC are gone. On the DC
//! leader, every five seconds:
//!
//! ```text
//! for each node record (nodes/<raft id>) that isn't removed:
//!   its gateway's registration (gateways/<gateway id>) is ready → seen now
//!   active or draining, not seen for replication.dead_node_minutes → dead
//!   dead, seen again → active
//! ```
//!
//! A node marked `dead` has its units re-replicated by each unit's leader
//! (a voter on a lost node is replaced on another node); a dead node that
//! comes back finds it no longer holds those units and drops them. The
//! time a node was last seen is kept in memory only, so a new DC leader
//! starts the clock again: a node is declared dead later, never earlier.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sprig_catalog::units::{self, NODE_ACTIVE, NODE_DEAD, NODE_DRAINING};
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx};

use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler};

/// `replication.dead_node_minutes`.
pub static DEAD_NODE_MINUTES: Setting<f64> = Setting {
    key: "replication.dead_node_minutes",
    default: 5.0,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 10_080),
    apply: Apply::Live,
    operator_only: true,
    owner: 61,
    doc: "Minutes a node may be gone before its units are re-replicated elsewhere",
};
sprig_settings::register!(DEAD_NODE_MINUTES);

/// The loop's one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HealthTick;

impl fmt::Display for HealthTick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tick")
    }
}

/// See the module docs.
pub struct NodeHealthLoop {
    live: Option<(LiveSettings, DcId)>,
    /// Raft id → when its gateway was last seen ready (by this leader).
    seen: Mutex<HashMap<u64, Instant>>,
}

impl fmt::Debug for NodeHealthLoop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHealthLoop").finish_non_exhaustive()
    }
}

impl NodeHealthLoop {
    /// The loop, reading `replication.dead_node_minutes` from `live`
    /// (its default without).
    #[must_use]
    pub fn new(live: Option<(LiveSettings, DcId)>) -> Self {
        Self {
            live,
            seen: Mutex::new(HashMap::new()),
        }
    }

    fn dead_after(&self) -> Duration {
        let m = match &self.live {
            Some((l, dc)) => l.get(&DEAD_NODE_MINUTES, &SettingCtx::process(dc)),
            None => DEAD_NODE_MINUTES.default,
        };
        Duration::from_secs_f64((m * 60.0).max(1.0))
    }
}

/// Gateway ids whose registration is ready, in this DC.
///
/// # Errors
/// The store's.
pub async fn ready_gateways(dc: &dyn MetaStore) -> Result<BTreeSet<String>> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Gw {
        gateway_id: String,
        state: String,
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
        .map(|g| g.body.gateway_id)
        .collect())
}

/// What one pass decides for a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Nothing to change.
    Keep,
    /// Mark it dead (gone this long).
    Dead(Duration),
    /// It is back: mark it active.
    Alive,
}

/// The pass's rule for one node: `state` its record's, `up` whether its
/// gateway is ready now, `gone` how long since it was last seen ready.
pub(crate) fn verdict(state: &str, up: bool, gone: Duration, dead_after: Duration) -> Verdict {
    match state {
        NODE_DEAD if up => Verdict::Alive,
        NODE_ACTIVE | NODE_DRAINING if !up && gone >= dead_after => Verdict::Dead(gone),
        _ => Verdict::Keep,
    }
}

#[async_trait::async_trait]
impl Reconciler for NodeHealthLoop {
    type Key = HealthTick;

    fn name(&self) -> &'static str {
        "node_health"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<HealthTick> {
        Vec::new()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<HealthTick>> {
        Ok(vec![HealthTick])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &HealthTick) -> Result<Outcome> {
        cx.leader()?;
        let nodes = units::nodes(cx.dc.as_ref()).await?;
        if nodes.is_empty() {
            return Ok(Outcome::RequeueAfter(Duration::from_secs(5)));
        }
        let ready = ready_gateways(cx.dc.as_ref()).await?;
        let dead_after = self.dead_after();
        let now = Instant::now();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let mut todo = Vec::new();
        {
            let mut seen = self
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for n in &nodes {
                let up = ready.contains(&n.gateway_id);
                let last = *seen.entry(n.id).or_insert(now);
                if up {
                    seen.insert(n.id, now);
                }
                // A node that has just restarted (its record's state
                // changed moments ago) counts as seen then, even before its
                // gateway is ready.
                let since = Duration::from_millis(now_ms.saturating_sub(n.since_ms));
                let gone = if up {
                    Duration::ZERO
                } else {
                    now.duration_since(last).min(since)
                };
                let v = verdict(&n.state, up, gone, dead_after);
                if v != Verdict::Keep {
                    todo.push((n.id, v));
                }
            }
        }
        for (id, v) in todo {
            match v {
                Verdict::Dead(gone) => {
                    let reason = format!(
                        "its gateway was not ready for {} s (replication.dead_node_minutes)",
                        gone.as_secs()
                    );
                    if let Some(r) = units::set_node_state(
                        cx.dc.as_ref(),
                        id,
                        &[NODE_ACTIVE, NODE_DRAINING],
                        NODE_DEAD,
                        &reason,
                    )
                    .await?
                        && r.state == NODE_DEAD
                    {
                        metrics::counter!("sprig_controller_nodes_declared_dead_total")
                            .increment(1);
                        tracing::warn!(event = "decision", component = "controller", kind = "node.dead",
                            node = id, gateway = %r.gateway_id, gone_s = gone.as_secs(),
                            "an engine node is gone: its units are re-replicated on other nodes");
                    }
                }
                Verdict::Alive => {
                    if let Some(r) = units::set_node_state(
                        cx.dc.as_ref(),
                        id,
                        &[NODE_DEAD],
                        NODE_ACTIVE,
                        "its gateway is ready again",
                    )
                    .await?
                        && r.state == NODE_ACTIVE
                    {
                        tracing::info!(event = "decision", component = "controller", kind = "node.alive",
                            node = id, gateway = %r.gateway_id,
                            "a dead engine node is back; it drops the units others took over");
                    }
                }
                Verdict::Keep => {}
            }
        }
        Ok(Outcome::RequeueAfter(Duration::from_secs(5)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_catalog::units::NODE_REMOVED;

    #[test]
    fn a_node_is_dead_only_after_the_timeout_and_alive_when_back() {
        let t = Duration::from_secs(300);
        assert_eq!(
            verdict(NODE_ACTIVE, false, Duration::from_secs(10), t),
            Verdict::Keep
        );
        assert_eq!(
            verdict(NODE_ACTIVE, false, Duration::from_secs(300), t),
            Verdict::Dead(Duration::from_secs(300))
        );
        assert_eq!(
            verdict(NODE_DRAINING, false, Duration::from_secs(301), t),
            Verdict::Dead(Duration::from_secs(301))
        );
        assert_eq!(verdict(NODE_ACTIVE, true, Duration::ZERO, t), Verdict::Keep);
        assert_eq!(verdict(NODE_DEAD, true, Duration::ZERO, t), Verdict::Alive);
        assert_eq!(
            verdict(NODE_DEAD, false, Duration::from_secs(900), t),
            Verdict::Keep
        );
        // An operator's removal stands, whatever the node does.
        assert_eq!(
            verdict(NODE_REMOVED, true, Duration::ZERO, t),
            Verdict::Keep
        );
        assert_eq!(
            verdict(NODE_REMOVED, false, Duration::from_secs(900), t),
            Verdict::Keep
        );
    }

    #[tokio::test]
    async fn ready_gateways_reads_registrations() {
        use sprig_meta::{MemStore, Op, Tier};
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Gw {
            gateway_id: String,
            state: String,
        }
        impl Versioned for Gw {
            const V: u32 = 1;
        }
        let s = MemStore::new(Tier::Dc(DcId::new("dc-h").unwrap()));
        for (id, state) in [("gw-1", "ready"), ("gw-2", "starting")] {
            let key = s.tier().key(&format!("gateways/{id}")).unwrap();
            let v = Record::new(Gw {
                gateway_id: id.to_owned(),
                state: state.to_owned(),
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
        let r = ready_gateways(&s).await.unwrap();
        assert_eq!(r.into_iter().collect::<Vec<_>>(), vec!["gw-1".to_owned()]);
    }
}
