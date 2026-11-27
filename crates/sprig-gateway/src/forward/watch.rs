//! Leadership events: when this node stops leading a tenant's unit, the
//! tenant's sessions here close at their next safe point, so their clients
//! reconnect (through any node) to where the tenant is now served.
//!
//! Sessions in the [`SessionTable`] (MySQL, MariaDB, Bolt) are marked with
//! [`CloseReason::Moved`]; front ends without table sessions (Redis,
//! MongoDB) hold a [`Watched`] and select on [`Watched::moved`] while they
//! wait for the client's next command. Kafka and AMQP connections are not
//! closed: Kafka clients follow `NOT_LEADER_OR_FOLLOWER` and Metadata, and
//! AMQP connections end with their queue owners.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::TenantId;

use crate::node_engine::{self, UnitLeader};
use crate::session::{CloseReason, SessionTable};

/// How often the watcher looks.
const EVERY: Duration = Duration::from_millis(200);

#[derive(Default)]
struct Registry {
    next: AtomicU64,
    open: Mutex<HashMap<u64, (u64, CancellationToken)>>,
}

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(Registry::default)
}

/// A connection of a tenant served here, outside the session table; closed
/// (cancelled) when this node stops leading the tenant's unit. Dropping it
/// unregisters the connection.
#[derive(Debug)]
pub struct Watched {
    id: u64,
    moved: CancellationToken,
}

impl Watched {
    /// Registers a connection of `tenant`.
    #[must_use]
    pub fn new(tenant: u64) -> Self {
        let r = registry();
        let id = r.next.fetch_add(1, Ordering::Relaxed);
        let moved = CancellationToken::new();
        r.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, (tenant, moved.clone()));
        Self { id, moved }
    }

    /// Resolves once the tenant's unit is led elsewhere.
    pub async fn moved(&self) {
        self.moved.cancelled().await;
    }

    /// Whether the tenant's unit is led elsewhere now.
    #[must_use]
    pub fn is_moved(&self) -> bool {
        self.moved.is_cancelled()
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        registry()
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.id);
    }
}

/// Whether this node has lost `tenant`'s unit to another node (an election
/// in progress, or the node not knowing yet, doesn't count).
fn led_elsewhere(tenant: u64, me: u64) -> bool {
    matches!(node_engine::tenant_leader(tenant), UnitLeader::Node(n) if n != me)
}

/// Starts the watcher on a replicated node (no-op otherwise).
pub fn start(sessions: Arc<SessionTable>, stop: CancellationToken) {
    let Some(me) = node_engine::raft_id_from_env() else {
        return;
    };
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(EVERY) => {}
            }
            let mut tenants: BTreeSet<u64> = sessions.tenants().into_iter().map(|t| t.0).collect();
            let open: Vec<(u64, CancellationToken)> = registry()
                .open
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .values()
                .filter(|(_, c)| !c.is_cancelled())
                .cloned()
                .collect();
            tenants.extend(open.iter().map(|(t, _)| *t));
            for t in tenants {
                if t == 0 || !led_elsewhere(t, me) {
                    continue;
                }
                let n = sessions.close_moved(TenantId(t));
                let mut m = 0;
                for (_, c) in open.iter().filter(|(ot, _)| *ot == t) {
                    c.cancel();
                    m += 1;
                }
                if n + m > 0 {
                    metrics::counter!("sprig_node_sessions_moved_total").increment((n + m) as u64);
                    tracing::info!(
                        tenant = t,
                        sessions = n + m,
                        "tenant's unit is led elsewhere now: its sessions here close at their next safe point"
                    );
                }
            }
        }
    });
}

/// The close reason sessions get (for front ends that print it).
#[must_use]
pub fn reason() -> CloseReason {
    CloseReason::Moved
}
