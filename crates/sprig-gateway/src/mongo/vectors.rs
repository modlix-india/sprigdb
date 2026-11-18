//! Background upkeep of vector side tables
//! ([`sprig_doc::engine::vectors`]): building one when its index is
//! created, and bringing it up to date after searches, in a session of
//! the index user's own, so a `$vectorSearch` never writes (nor waits on a
//! row lock) in the reader's session.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{LazyLock, Mutex, PoisonError};

use sprig_doc::engine::vectors::{VectorJob, VectorSides};
use sprig_doc::engine::{Engine, SqlRunner};

use super::sql::SqlSession;

/// A job: (tenant, database, collection, index).
type JobKey = (u64, String, String, String);

/// Jobs running.
static RUNNING: LazyLock<Mutex<HashSet<JobKey>>> = LazyLock::new(Mutex::default);

fn running() -> std::sync::MutexGuard<'static, HashSet<JobKey>> {
    RUNNING.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The gateway's [`VectorSides`]: a task per job, one at a time per index.
#[derive(Debug, Default)]
pub struct GatewaySides;

impl VectorSides for GatewaySides {
    fn kick(&self, engine: &Engine, sql: &dyn SqlRunner, job: VectorJob) -> bool {
        let Some(svc) = crate::search::service() else {
            return false;
        };
        let tenant = sql.tenant();
        let Some((tenant_name, user)) = sql.principal_hint() else {
            return false;
        };
        let key = (
            tenant,
            job.ns.db.clone(),
            job.ns.coll.clone(),
            job.name.clone(),
        );
        if !running().insert(key.clone()) {
            return true;
        }
        let engine = engine.clone();
        let svc = std::sync::Arc::clone(svc);
        tokio::spawn(async move {
            let r = async {
                let principal =
                    crate::triggers::principal_of(&svc.deps.login, tenant, &tenant_name, user)
                        .await
                        .map_err(|e| e.message)?;
                let mut s = SqlSession::open(
                    &svc.deps.sessions,
                    &svc.deps.executor,
                    &principal,
                    "vector",
                    0,
                    SocketAddr::from(([127, 0, 0, 1], 0)),
                    None,
                    64 << 20,
                )
                .map_err(|e| e.message)?;
                // The side table holds every document's vector; searches
                // read hits through the searching user's session and
                // policies.
                let exempt = crate::tenant::rls::exempt_internal(
                    s.session_id(),
                    tenant,
                    "vector index upkeep",
                    &format!("{}.{}", job.ns.db, job.ns.coll),
                );
                let r = Box::pin(engine.vector_side_work(&mut s, &job.ns, &job.name))
                    .await
                    .map_err(|e| e.message);
                drop(exempt);
                s.close().await;
                r
            }
            .await;
            running().remove(&key);
            if let Err(e) = r {
                tracing::warn!(tenant, ns = %job.ns, index = job.name, error = %e,
                    "vector index upkeep failed; retried on the next search");
            }
        });
        true
    }
}
