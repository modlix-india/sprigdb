//! Closing a vhost nobody uses, so a gateway's cost follows the vhosts
//! in use rather than every vhost it ever opened.
//!
//! A vhost stays open on a gateway once anything used it: its forwarder
//! polls the stores and tries for the forwarder lease, and every queue
//! owner it started keeps its lease, its head cache and a timer. With
//! thousands of tenants that is thousands of loops doing nothing.
//!
//! **When.** The forwarder closes its vhost after
//! `queue.vhost_idle_close_seconds` (300; 0 never) with:
//!
//! - no AMQP connection, consumer or temporary queue on this gateway, and
//!   no SQL statement, relay stream or forwarded row in that time;
//! - every queue slot quiet ([`crate::owner::Cmd::Quiet`]): no consumer,
//!   no delivery out (also SQL's leased `mq_get` and acknowledgements
//!   inside a transaction), no write in flight, no `x-expires` clock;
//! - as the forwarder lease's holder, an empty outbox.
//!
//! **Closing** removes the vhost from the engine (under the same lock
//! that hands it out, after checking nothing else holds it), stops its
//! slots (owners give back their leases; relayed slots end their
//! streams), stops the forwarder (it gives back the forwarder lease) and
//! tells the catalog ([`crate::engine::MqCatalog::closed`]), which drops
//! the vhost's store handle. The next use of the vhost opens it again.
//!
//! **What still has a time.** A delayed message's due time and the expiry
//! of a message at a queue's head need a forwarder or an owner when they
//! come. The closing gateway keeps one timer for the earliest; it opens
//! the vhost again then, and starts the owners of the queues whose head
//! expires, which dead-letter it. The vhost closes again once idle.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use sprig_common::CancellationToken;
use tokio::sync::oneshot;

use crate::engine::{EngineDeps, Inner, VShared, VhostRef, lock};
use crate::owner::{Cmd, OwnerHandle, Quiet};
use crate::store::MsgStore;

/// After a close was refused (something busy), the forwarder asks again
/// this much later at the soonest.
pub(crate) const RETRY_MS: i64 = 5_000;

/// How long a slot may take to say whether it is quiet.
const QUIET_WAIT: Duration = Duration::from_secs(2);

impl VShared {
    /// Counts as used at `now_ms` (the engine's clock).
    pub(crate) fn touch(&self, now_ms: i64) {
        self.last_used.fetch_max(now_ms, Ordering::Relaxed);
    }

    /// No connection, consumer or temporary queue here.
    pub(crate) fn unused(&self) -> bool {
        lock(&self.conns).is_empty()
            && lock(&self.consumers).is_empty()
            && lock(&self.temp).queues.is_empty()
    }

    /// Closes the vhost if nothing uses it (see the module); `true` when
    /// closed. `durable`: the store, when this gateway holds the
    /// forwarder lease.
    pub(crate) async fn try_close(
        self: &Arc<Self>,
        deps: &EngineDeps,
        durable: Option<&Arc<dyn MsgStore>>,
    ) -> bool {
        let seen = self.last_used.load(Ordering::Relaxed);
        let mut wake_at: Option<i64> = None;
        // The lease holder's outbox must be empty; delayed messages wait
        // for the wake-up.
        if let Some(s) = durable {
            if !matches!(s.outbox_head().await, Ok(None)) {
                return false;
            }
            match s.next_due().await {
                Ok(d) => wake_at = d,
                Err(_) => return false,
            }
        }
        let mem: Arc<dyn MsgStore> = Arc::clone(&self.mem) as Arc<dyn MsgStore>;
        if !matches!(mem.outbox_head().await, Ok(None)) || !matches!(mem.next_due().await, Ok(None))
        {
            return false;
        }
        let slots: Vec<OwnerHandle> = lock(&self.owners)
            .values()
            .filter(|h| h.alive())
            .cloned()
            .collect();
        let mut expiring = Vec::new();
        for h in &slots {
            let (tx, rx) = oneshot::channel();
            if !h.send(Cmd::Quiet(tx)) {
                return false;
            }
            match tokio::time::timeout(QUIET_WAIT, rx).await {
                Ok(Ok(Quiet::Idle { expires_at })) => {
                    if let Some(t) = expires_at {
                        expiring.push(h.queue);
                        wake_at = Some(wake_at.map_or(t, |w| w.min(t)));
                    }
                }
                _ => return false,
            }
        }
        let Some(e) = self.engine.upgrade() else {
            return false;
        };
        let key = (self.r.tenant, self.r.db_id);
        {
            let mut map = lock(&e.vhosts);
            if !map.get(&key).is_some_and(|v| Arc::ptr_eq(v, self))
                || self.last_used.load(Ordering::Relaxed) != seen
                || !self.unused()
            {
                return false;
            }
            let live = lock(&self.owners).values().filter(|h| h.alive()).count();
            // The map, the supervisor, this forwarder and one task per
            // slot hold it; anyone else (a connection's or a statement's
            // handle, a relay stream, a re-attachment) is a use.
            if live != slots.len() || Arc::strong_count(self) > 3 + live {
                return false;
            }
            map.remove(&key);
        }
        self.stop.cancel();
        for h in &slots {
            let _ = h.send(Cmd::Stop);
        }
        metrics::gauge!("sprig_queue_vhosts_open").decrement(1.0);
        metrics::counter!("sprig_queue_vhost_closes_total").increment(1);
        tracing::debug!(
            tenant = self.r.tenant,
            vhost = %self.r.name,
            slots = slots.len(),
            wake_at = ?wake_at,
            "idle vhost closed"
        );
        deps.catalog.closed(&self.r).await;
        if let Some(at) = wake_at {
            wake_later(
                Arc::downgrade(&e),
                e.stop.child_token(),
                self.r.clone(),
                at,
                expiring,
            );
        }
        true
    }
}

/// Opens a closed vhost again at `at_ms` (the engine's clock), so its
/// forwarder releases a delayed message that falls due and the owners of
/// `queues` dead-letter the message at their head that expires.
fn wake_later(
    engine: Weak<Inner>,
    stop: CancellationToken,
    r: VhostRef,
    at_ms: i64,
    queues: Vec<u64>,
) {
    tokio::spawn(async move {
        let wait = match engine.upgrade() {
            Some(e) => at_ms.saturating_sub(e.deps.clock.now_ms()),
            None => return,
        };
        let wait = Duration::from_millis(u64::try_from(wait).unwrap_or(0));
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
        }
        let Some(e) = engine.upgrade() else {
            return;
        };
        let v = e.vshared(&r);
        metrics::counter!("sprig_queue_vhost_wakeups_total").increment(1);
        if queues.is_empty() {
            return;
        }
        if let Err(err) = e.deps.catalog.refresh(&r).await {
            tracing::debug!(vhost = %r.name, error = %err, "vhost wake-up: catalog unavailable");
            return;
        }
        for id in queues {
            if let Some(q) = v.queue_by_id(id) {
                v.slot(&q);
            }
        }
    });
}
