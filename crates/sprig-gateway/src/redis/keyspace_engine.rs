//! Keyspace notifications of Redis databases on the engine.
//!
//! ```text
//! the tenant's flags: redis.notify_keyspace_events (tenant scope, written
//!   by CONFIG SET on any gateway), read every second for the tenants with a
//!   database open here; CONFIG SET here applies at once (and wins over an
//!   older catalog for PIN)
//! flags that send: the engine's writes record their events in the change
//!   log (sprig-redis-engine's notify), and this gateway reads the
//!   tenant's unit from the change log's head on:
//!     each event note (key notify::note_key) -> its events, in order
//!     published while this node leads the unit, through the pub/sub hubs
//!     (env.publish: every gateway's subscribers), under the flags now:
//!       K -> __keyspace@<db>__:<key> <event>, E -> __keyevent@<db>__:<event> <key>
//!     a leadership taken over mid-way: notes committed before it are the
//!     old leader's to publish (skipped)
//!   stops when the flags stop sending
//! keymiss (m): from the gateway that ran the read, straight to the hubs
//! ```
//!
//! Delivery is at most once, as in Redis: nothing is kept for a subscriber
//! that wasn't connected, and there is no replay. Events reach subscribers
//! after the commit is applied, not while the command runs.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::ids::TenantId;
use sprig_engine::txn::CommitPath;
use sprig_redis::keyspace::{flags_lossless, parse_flags, publishes, sends};
use sprig_redis_engine::notify;
use sprig_redis_engine::{Backend, EngineUnit};

/// How often the tenants' flags are read from their catalogs.
const REFRESH: Duration = Duration::from_secs(1);

/// A `CONFIG SET` here outweighs the catalog this long (the gateway's
/// catalog watch may not have delivered the change yet).
const PIN: Duration = Duration::from_secs(10);

/// Change-log bytes read per page.
const PAGE_BYTES: usize = 256 << 10;

/// Keyspace notifications of the engine's Redis databases on this gateway.
pub(crate) struct EngineKeyspace {
    env: Weak<sprig_redis::Env>,
    backend: Arc<Backend<EngineUnit>>,
    svc: Arc<crate::tenant::TenantServices>,
    /// Tenants whose change-log reader runs.
    readers: Mutex<HashSet<u64>>,
    /// Tenants whose flags `CONFIG SET` changed here, and when.
    pinned: Mutex<HashMap<u64, Instant>>,
    /// Tenants whose flags were read here at least once.
    seen: Mutex<HashSet<u64>>,
}

impl std::fmt::Debug for EngineKeyspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineKeyspace").finish_non_exhaustive()
    }
}

impl EngineKeyspace {
    /// Starts the notifications of `backend`'s databases: the flags'
    /// refresh, and the sink of `keymiss` events.
    pub(crate) fn start(
        env: &Arc<sprig_redis::Env>,
        backend: Arc<Backend<EngineUnit>>,
        svc: Arc<crate::tenant::TenantServices>,
    ) -> Arc<Self> {
        let me = Arc::new(Self {
            env: Arc::downgrade(env),
            backend,
            svc,
            readers: Mutex::default(),
            pinned: Mutex::default(),
            seen: Mutex::default(),
        });
        let sink_env = Arc::downgrade(env);
        me.backend.set_sink(Arc::new(move |tenant, events| {
            let Some(env) = sink_env.upgrade() else {
                return;
            };
            tokio::spawn(async move {
                let scope = tenant.to_string();
                let flags = parse_flags(env.keyspace_flags(&scope).as_bytes()).unwrap_or(0);
                for e in events {
                    publish(&env, &scope, flags, e.db, e.name, &e.key).await;
                }
            });
        }));
        let weak = Arc::downgrade(&me);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(REFRESH);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(me) = weak.upgrade() else {
                    return;
                };
                me.refresh().await;
            }
        });
        me
    }

    /// `CONFIG SET notify-keyspace-events` here: the flags apply at once,
    /// and a reader starts from the change log's head now, so every write
    /// acknowledged after this sends its events.
    pub(crate) fn config_set(self: &Arc<Self>, tenant: u64, flags: u16) {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant, Instant::now());
        self.apply(tenant, flags);
    }

    fn apply(self: &Arc<Self>, tenant: u64, flags: u16) {
        let n = self.backend.notifier(tenant);
        if n.flags.get() != flags {
            n.flags.set(flags);
            if let Some(env) = self.env.upgrade() {
                env.set_keyspace_flags(&tenant.to_string(), flags_lossless(flags));
            }
        }
        if sends(flags) {
            self.ensure_reader(tenant);
        }
    }

    /// A database of `tenant` is used here: the first time, its flags are
    /// read at once rather than at the next refresh, so its first writes
    /// already record their events.
    pub(crate) fn touch(self: &Arc<Self>, tenant: u64) {
        if !self
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant)
        {
            return;
        }
        let me = Arc::clone(self);
        tokio::spawn(async move { me.refresh_one(tenant).await });
    }

    /// Reads the flags of the tenants with a database open here.
    async fn refresh(self: &Arc<Self>) {
        for tenant in self.backend.tenants_open() {
            self.refresh_one(tenant).await;
        }
    }

    /// Reads `tenant`'s flags and applies them, unless a `CONFIG SET` here
    /// set others within [`PIN`].
    async fn refresh_one(self: &Arc<Self>, tenant: u64) {
        let Some(flags) = self.flags_of(tenant).await else {
            return;
        };
        let pinned = self
            .pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .is_some_and(|t| t.elapsed() < PIN);
        if pinned && flags != self.backend.notifier(tenant).flags.get() {
            return;
        }
        self.apply(tenant, flags);
    }

    /// Tenant `tenant`'s `redis.notify_keyspace_events` from its catalog.
    async fn flags_of(&self, tenant: u64) -> Option<u16> {
        let cat = match self
            .svc
            .cache
            .get(TenantId(tenant), sprig_catalog::Freshness::Latest)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(tenant, error = %e, "keyspace notifications: the tenant's settings weren't read");
                return None;
            }
        };
        let ctx = cat.setting_ctx(&self.svc.dc);
        let text = self
            .svc
            .settings
            .get(&super::NOTIFY_KEYSPACE_EVENTS, &ctx, Some(&cat.settings));
        Some(parse_flags(text.as_bytes()).unwrap_or(0))
    }

    fn ensure_reader(self: &Arc<Self>, tenant: u64) {
        let (Some(log), Ok(raw)) = (crate::changelog::changes(), crate::node_engine::engine())
        else {
            tracing::warn!(
                tenant,
                "keyspace notifications need the node's change log, which is not open"
            );
            return;
        };
        if !self
            .readers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant)
        {
            return;
        }
        let unit = sprig_engine::Engine::unit_of_tenant(tenant).0;
        // From now: the place is taken before CONFIG SET answers.
        let start = log.head(unit).0;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = me.read(&log, &raw, tenant, unit, start).await {
                tracing::warn!(tenant, error = %e, "keyspace notifications stopped");
            }
            me.readers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&tenant);
            // Turned on again meanwhile: start over from the head.
            if me.backend.notifier(tenant).flags.recording() != 0 {
                me.ensure_reader(tenant);
            }
        });
    }

    /// The reader of the module docs.
    async fn read(
        &self,
        log: &sprig_streams::changes::ChangeLog,
        raw: &sprig_engine::Engine,
        tenant: u64,
        unit: u64,
        start: sprig_streams::changes::Pos,
    ) -> sprig_common::Result<()> {
        let scope = tenant.to_string();
        let key = notify::note_key(tenant);
        let notifier = self.backend.notifier(tenant);
        let mut watch = log.watch(unit);
        let mut reader = Some(log.reader(unit, start, None)?);
        // The leader epoch this node publishes under, and the first commit
        // timestamp it publishes in it (0: everything from the start).
        let mut lead: Option<(u64, u64)> = None;
        tracing::info!(tenant, "keyspace notifications started");
        loop {
            if notifier.flags.recording() == 0 {
                tracing::info!(tenant, "keyspace notifications stopped: turned off");
                return Ok(());
            }
            let Some(env) = self.env.upgrade() else {
                return Ok(());
            };
            watch.borrow_and_update();
            let mut r = reader.take().ok_or_else(|| {
                sprig_common::SprigError::internal("keyspace notifications lost their reader")
            })?;
            let (r, page) = tokio::task::spawn_blocking(move || {
                let p = r.read(PAGE_BYTES);
                (r, p)
            })
            .await
            .map_err(|e| sprig_common::SprigError::internal(format!("change log read: {e}")))?;
            reader = Some(r);
            let page = match page {
                Ok(p) => p,
                Err(e) if sprig_streams::changes::is_history_lost(&e) => {
                    // Retention passed the reader by: go on from the head.
                    reader = Some(log.reader(unit, log.head(unit).0, None)?);
                    continue;
                }
                Err(e) => return Err(e),
            };
            let flags = parse_flags(env.keyspace_flags(&scope).as_bytes()).unwrap_or(0);
            for c in &page.changes {
                if c.key != key {
                    continue;
                }
                let epoch = raw.leader_epoch_of(unit);
                if epoch == 0 {
                    // A replica: the leader publishes.
                    lead = Some((0, lead.map_or(0, |(_, ts)| ts)));
                    continue;
                }
                let since = match lead {
                    Some((e, ts)) if e == epoch => ts,
                    // Leadership taken over: what the old leader committed
                    // was its to publish.
                    Some(_) => {
                        let ts = raw.txn_shared().visible_ts().0;
                        lead = Some((epoch, ts));
                        ts
                    }
                    None => {
                        lead = Some((epoch, 0));
                        0
                    }
                };
                if c.cts < since {
                    continue;
                }
                let Some(events) = c.value.as_deref().and_then(notify::decode) else {
                    tracing::warn!(tenant, "keyspace notifications: a malformed event note");
                    continue;
                };
                metrics::counter!("sprig_redis_keyspace_events_total")
                    .increment(events.len() as u64);
                for (db, name, k) in events {
                    publish(&env, &scope, flags, db, &name, &k).await;
                }
            }
            drop(env);
            if page.at_end {
                // Woken by the next apply, or after a second to look at the
                // flags again.
                let _ = tokio::time::timeout(Duration::from_secs(1), watch.changed()).await;
            }
        }
    }
}

/// Publishes one event under `flags` through the hubs.
async fn publish(
    env: &sprig_redis::Env,
    scope: &str,
    flags: u16,
    db: u32,
    name: &str,
    key: &Bytes,
) {
    for (channel, data) in publishes(flags, db, &[(name, notify::class(name), key.clone())]) {
        if let Err(e) = env.publish(scope, &channel, &data, false).await {
            tracing::debug!(error = %e, "keyspace notification not published");
        }
    }
}
