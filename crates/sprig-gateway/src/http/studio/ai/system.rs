//! The administration assistant's conversations, for platform operators:
//! `__sprig_ai` on the DC's system cluster ([`sprig_ai::audit::system`]).
//!
//! - **Which cluster.** The DC's lowest-numbered cluster that is active,
//!   draining or drained (`ClusterRegistry::system_cluster`), looked up
//!   again at most every [`CLUSTER_TTL`]. The controller creates the
//!   schema there; a gateway that gets there first creates it too.
//! - **How.** Through the gateway's `sprig_ctl` catalog pool to that
//!   cluster's writer, the path tenant catalogs take. No tenant account
//!   has a grant on the schema, and client statements can't name it.
//! - **Whose.** One operator's login (`tenant.user`, as
//!   `studio.operator_logins` lists it); the routes open it only for a
//!   caller who is a platform operator right now.
//! - **Ids.** Conversation and message ids from this store are shown with
//!   an `a` prefix (`a12`), so they never collide with a tenant's.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_ai::SystemAudit;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::ClusterId;
use sprig_common::{SprigCode, SprigError};

use super::super::super::Core;
use super::super::super::sql::Caller;

/// How long the system cluster's id is reused.
pub const CLUSTER_TTL: Duration = Duration::from_secs(30);

/// The prefix of this store's ids in the API.
pub const PREFIX: &str = "a";

static CLUSTER: Mutex<Option<(Instant, ClusterId)>> = Mutex::new(None);

/// `id` as the API shows it: `a<id>`.
#[must_use]
pub fn label(id: u64) -> String {
    format!("{PREFIX}{id}")
}

/// The id of an API id that names this store (`a<id>`), else `None`.
#[must_use]
pub fn parse(s: &str) -> Option<u64> {
    s.strip_prefix(PREFIX)?.parse().ok()
}

fn unavailable(why: impl Into<String>) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("administration conversations: {}", why.into()),
    )
}

async fn system_cluster(core: &Arc<Core>) -> Result<ClusterId, SprigError> {
    if let Some((at, c)) = &*CLUSTER.lock().unwrap_or_else(PoisonError::into_inner)
        && at.elapsed() < CLUSTER_TTL
    {
        return Ok(c.clone());
    }
    let Some(svc) = &core.studio.tenants else {
        return Err(unavailable("this gateway has no cluster registry"));
    };
    let registry = ClusterRegistry::new(Arc::clone(&svc.global), svc.locator.dc_store())?;
    let c = registry
        .system_cluster()
        .await?
        .ok_or_else(|| unavailable("the data center has no cluster in service yet"))?;
    *CLUSTER.lock().unwrap_or_else(PoisonError::into_inner) = Some((Instant::now(), c.clone()));
    Ok(c)
}

/// The store for `caller`, who must be a platform operator (the routes
/// check). Connecting happens on first use.
///
/// # Errors
/// 39070 without a registry or a cluster in service; the locator's.
pub async fn store(core: &Arc<Core>, caller: &Caller) -> Result<SystemAudit, SprigError> {
    let cluster = system_cluster(core).await?;
    let Some(svc) = &core.studio.tenants else {
        return Err(unavailable("this gateway has no cluster registry"));
    };
    let pool = svc.locator.catalog_pool(&cluster).await?;
    Ok(SystemAudit::new(pool, &super::super::ops::login_of(caller)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_carry_a_prefix() {
        assert_eq!(label(12), "a12");
        assert_eq!(parse("a12"), Some(12));
        assert_eq!(parse("12"), None);
        assert_eq!(parse("a"), None);
        assert_eq!(parse("ax"), None);
    }
}
