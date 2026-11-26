//! Redis databases stored on SprigDB's own storage engine.
//!
//! Every tenant's Redis databases live on the engine: their data commands
//! run there (`sprig-redis-engine`), never on MariaDB.
//!
//! They run on the node's one engine ([`crate::node_engine`]), opened on
//! first use. If it can't open, those tenants' data commands are refused
//! with an error: their keys are never read from or written to MariaDB
//! instead.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_redis::engine::{EngineBackend, EngineDatabase, session_command};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

/// The node's Redis databases on the engine, started on first use.
#[derive(Debug)]
pub struct NodeEngine {
    tenants: Tenants,
    databases: u32,
    open: OnceLock<Option<Arc<Backend<EngineUnit>>>>,
    /// Keyspace notifications (installed with the tenants' services).
    keyspace: OnceLock<KeyspaceSetup>,
}

/// What keyspace notifications start from, once the engine opens.
struct KeyspaceSetup {
    env: std::sync::Weak<sprig_redis::Env>,
    svc: Arc<crate::tenant::TenantServices>,
    on: OnceLock<Arc<super::keyspace_engine::EngineKeyspace>>,
}

impl std::fmt::Debug for KeyspaceSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyspaceSetup").finish_non_exhaustive()
    }
}

impl NodeEngine {
    /// From the environment; `databases` is `redis.databases`.
    #[must_use]
    pub fn from_env(databases: u32) -> Self {
        // Every tenant: the engine is the only key-value store.
        let tenants = Tenants::All;
        Self {
            tenants,
            databases,
            open: OnceLock::new(),
            keyspace: OnceLock::new(),
        }
    }

    /// Keyspace notifications of the engine's databases, for the tenants
    /// of `svc` (they start with the engine).
    pub(crate) fn install_keyspace(
        &self,
        env: &Arc<sprig_redis::Env>,
        svc: Arc<crate::tenant::TenantServices>,
    ) {
        let _ = self.keyspace.set(KeyspaceSetup {
            env: Arc::downgrade(env),
            svc,
            on: OnceLock::new(),
        });
    }

    /// The running notifications, started on first use.
    fn keyspace(&self) -> Option<&Arc<super::keyspace_engine::EngineKeyspace>> {
        let k = self.keyspace.get()?;
        if let Some(on) = k.on.get() {
            return Some(on);
        }
        let backend = Arc::clone(self.backend()?);
        let env = k.env.upgrade()?;
        Some(k.on.get_or_init(|| {
            super::keyspace_engine::EngineKeyspace::start(&env, backend, Arc::clone(&k.svc))
        }))
    }

    fn backend(&self) -> Option<&Arc<Backend<EngineUnit>>> {
        self.open.get_or_init(|| self.start()).as_ref()
    }

    /// Takes the node's engine and starts the sweeper.
    fn start(&self) -> Option<Arc<Backend<EngineUnit>>> {
        match crate::node_engine::engine() {
            Ok(engine) => {
                let b = Arc::new(Backend::new(
                    Arc::new(EngineUnit::on_engine(engine)),
                    self.tenants.clone(),
                    self.databases,
                ));
                // Expired keys nobody touches again: Redis's 10 passes a
                // second.
                drop(b.spawn_sweeper(Duration::from_millis(100)));
                tracing::info!("redis databases on the storage engine");
                Some(b)
            }
            Err(e) => {
                tracing::error!(error = %e, "redis on the storage engine not started");
                None
            }
        }
    }
}

impl EngineBackend for NodeEngine {
    fn database(&self, scope: &str, db: u32, placed: bool) -> Option<Arc<dyn EngineDatabase>> {
        let tenant: u64 = scope.parse().ok()?;
        if !placed && !self.tenants.has(tenant) {
            return None;
        }
        match self.backend() {
            Some(b) => {
                // The tenant's keyspace notification flags.
                if let Some(k) = self.keyspace() {
                    k.touch(tenant);
                }
                b.placed_db(tenant, db, true)
                    .map(|d| d as Arc<dyn EngineDatabase>)
            }
            // The tenant's keys belong on the engine and it didn't open:
            // refuse, rather than let the session use the key-value tables.
            None => Some(Arc::new(Unavailable)),
        }
    }

    fn set_keyspace_flags(&self, scope: &str, flags: u16) {
        let Ok(tenant) = scope.parse::<u64>() else {
            return;
        };
        match self.keyspace() {
            Some(k) => k.config_set(tenant, flags),
            None => {
                if let Some(b) = self.backend() {
                    b.set_flags(tenant, flags);
                }
            }
        }
    }
}

/// A database of a tenant on the engine, on a node whose engine failed to
/// open: every data command is refused. The session still answers
/// connection, server and pub/sub commands.
#[derive(Debug)]
struct Unavailable;

#[async_trait]
impl EngineDatabase for Unavailable {
    fn runs(&self, name: &str) -> bool {
        !session_command(name)
    }

    async fn run(&self, cmds: &[&[Bytes]], _resp3: bool) -> Vec<Frame> {
        cmds.iter()
            .map(|_| {
                Frame::err(
                    "this database is stored on the SprigDB engine, which failed to start on this node; see the node's log",
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_node_without_its_engine_refuses_data_commands() {
        let d = Unavailable;
        assert!(d.runs("get") && d.runs("zadd") && d.runs("eval"));
        assert!(!d.runs("ping") && !d.runs("select") && !d.runs("subscribe"));
        let get = [Bytes::from_static(b"GET"), Bytes::from_static(b"k")];
        let replies = d.run(&[&get[..], &get[..]], false).await;
        assert_eq!(replies.len(), 2);
        for r in replies {
            assert!(
                matches!(&r, Frame::Error(e) if e.starts_with(b"ERR this database is stored on the SprigDB engine")),
                "{r:?}"
            );
        }
    }
}
