//! Which tenants' Redis databases live on the engine, and their sweeper.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use sprig_redis::engine::{EngineBackend, EngineDatabase};

use crate::db::{EngineDb, Uids, Versions, Waiters};
use crate::layout::Space;
use crate::notify::{Notifier, Sink};
use crate::store::Unit;

/// Which tenants are on the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tenants {
    /// Every tenant.
    All,
    /// These tenant ids (none: only databases placed on the engine).
    Only(BTreeSet<u64>),
}

impl Tenants {
    /// Parses `all` or a comma-separated list of tenant ids; `None` when
    /// empty or malformed.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("all") {
            return Some(Self::All);
        }
        let ids: Option<BTreeSet<u64>> = s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| p.parse().ok())
            .collect();
        ids.filter(|i| !i.is_empty()).map(Self::Only)
    }

    /// Whether tenant `t` is listed.
    #[must_use]
    pub fn has(&self, t: u64) -> bool {
        match self {
            Self::All => true,
            Self::Only(ids) => ids.contains(&t),
        }
    }
}

/// The databases opened so far, by tenant and index, and each tenant's
/// collection uids.
struct Open<U: Unit> {
    dbs: HashMap<(u64, u32), Arc<EngineDb<U>>>,
    uids: HashMap<u64, Arc<Uids<U>>>,
    waiters: HashMap<u64, Arc<Waiters>>,
    notify: HashMap<u64, Arc<Notifier>>,
}

/// The engine's Redis databases on one node.
pub struct Backend<U: Unit> {
    unit: Arc<U>,
    tenants: Tenants,
    databases: u32,
    open: Mutex<Open<U>>,
    versions: Arc<Versions>,
    /// Where keyspace events that change nothing go.
    sink: Arc<OnceLock<Sink>>,
}

impl<U: Unit> std::fmt::Debug for Backend<U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("tenants", &self.tenants)
            .field("databases", &self.databases)
            .finish_non_exhaustive()
    }
}

impl<U: Unit> Backend<U> {
    /// `tenants`' Redis databases (`0..databases`) on `unit`.
    #[must_use]
    pub fn new(unit: Arc<U>, tenants: Tenants, databases: u32) -> Self {
        Self {
            unit,
            tenants,
            databases: databases.max(1),
            open: Mutex::new(Open {
                dbs: HashMap::new(),
                uids: HashMap::new(),
                waiters: HashMap::new(),
                notify: HashMap::new(),
            }),
            versions: Versions::new(),
            sink: Arc::new(OnceLock::new()),
        }
    }

    /// Tenant `tenant`'s keyspace notifications on this node.
    pub fn notifier(&self, tenant: u64) -> Arc<Notifier> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            open.notify
                .entry(tenant)
                .or_insert_with(|| Arc::new(Notifier::new(Arc::clone(&self.sink)))),
        )
    }

    /// Sets tenant `tenant`'s keyspace notification flags on this node.
    pub fn set_flags(&self, tenant: u64, flags: u16) {
        self.notifier(tenant).flags.set(flags);
    }

    /// Installs the sink of events that change nothing (`keymiss`), once.
    pub fn set_sink(&self, sink: Sink) {
        let _ = self.sink.set(sink);
    }

    /// The tenants with a database opened on this node.
    #[must_use]
    pub fn tenants_open(&self) -> Vec<u64> {
        let open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let mut t: Vec<u64> = open.dbs.keys().map(|(t, _)| *t).collect();
        t.sort_unstable();
        t.dedup();
        t
    }

    /// Database `db` of `tenant`, when the backend holds the tenant.
    pub fn db(&self, tenant: u64, db: u32) -> Option<Arc<EngineDb<U>>> {
        self.placed_db(tenant, db, false)
    }

    fn open_db(&self, tenant: u64, db: u32) -> Option<Arc<EngineDb<U>>> {
        if db >= self.databases {
            return None;
        }
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let open = &mut *open;
        let uids = open
            .uids
            .entry(tenant)
            .or_insert_with(|| Uids::new(Arc::clone(&self.unit), tenant));
        let waiters = open.waiters.entry(tenant).or_default();
        let notifier = open
            .notify
            .entry(tenant)
            .or_insert_with(|| Arc::new(Notifier::new(Arc::clone(&self.sink))));
        let d = open.dbs.entry((tenant, db)).or_insert_with(|| {
            let all: Arc<[Space]> = (0..self.databases)
                .map(|i| Space::redis(tenant, i))
                .collect();
            let d = EngineDb::new(
                Arc::clone(&self.unit),
                Space::redis(tenant, db),
                all,
                Arc::clone(uids),
                Arc::clone(&self.versions),
                Arc::clone(waiters),
            );
            d.set_notify(Arc::clone(notifier));
            d
        });
        Some(Arc::clone(d))
    }

    /// The databases opened so far, with their tenants' notifications.
    fn spaces(&self) -> Vec<(Space, Arc<Notifier>)> {
        let open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        open.dbs
            .values()
            .map(|d| {
                let s = d.space();
                let n = open
                    .notify
                    .get(&s.tenant)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(Notifier::new(Arc::clone(&self.sink))));
                (s, n)
            })
            .collect()
    }

    /// Starts the sweeper: every `every` (Redis's 100 ms), each opened
    /// database's due keys are deleted. Stops when the backend is dropped.
    pub fn spawn_sweeper(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(me) = weak.upgrade() else {
                    return;
                };
                for (s, n) in me.spaces() {
                    if let Err(e) = crate::sweep::pass_notifying(
                        &*me.unit,
                        s,
                        crate::sweep::PASS_LIMIT,
                        &n.flags,
                    )
                    .await
                    {
                        tracing::warn!(error = %e, "expiry sweep failed");
                    }
                }
            }
        })
    }
}

impl<U: Unit> Backend<U> {
    /// Database `db` of `tenant` when the tenant's Redis databases are
    /// `placed` on the engine or the backend holds the whole tenant.
    pub fn placed_db(&self, tenant: u64, db: u32, placed: bool) -> Option<Arc<EngineDb<U>>> {
        if !placed && !self.tenants.has(tenant) {
            return None;
        }
        self.open_db(tenant, db)
    }
}

impl<U: Unit> EngineBackend for Backend<U> {
    fn database(&self, scope: &str, db: u32, placed: bool) -> Option<Arc<dyn EngineDatabase>> {
        let tenant: u64 = scope.parse().ok()?;
        self.placed_db(tenant, db, placed)
            .map(|d| d as Arc<dyn EngineDatabase>)
    }

    fn set_keyspace_flags(&self, scope: &str, flags: u16) {
        if let Ok(tenant) = scope.parse::<u64>() {
            self.set_flags(tenant, flags);
        }
    }
}
