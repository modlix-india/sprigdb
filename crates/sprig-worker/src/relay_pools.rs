//! After-commit triggers of a tenant on Sprig 2PC's relay path reach its
//! shards through the cluster's relay writer, as the tenant's gateway
//! sessions do, so the relay fence ends their connections with the
//! tenant's others when a held branch's holder is lost. Without it, a
//! trigger's write waiting for a held branch's rows could take them in the
//! moment between the holder's end and the branch's replay.
//!
//! The route (the tenant's `'%'` accounts locked on the cluster, then the
//! relay lease) and the forwarder that sends the relay's preamble are
//! `sprig_cluster::failover::relay_pools`, shared with the mover, the
//! controller's system writers and the gateway's lookup builds.

use std::sync::Arc;

use mysql_async::Pool;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_meta::MetaStore;

/// The pool a trigger of `tenant` takes `sprig_ctl` connections to
/// `cluster` from: `direct`, or one through the cluster's relay writer for
/// a tenant on the relay path (see the module notes).
///
/// # Errors
/// Retryable 39030 while the cluster has relays but none is ready, or
/// when the cluster or the store can't be read.
pub(crate) async fn shard_pool(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: Pool,
) -> Result<Pool> {
    sprig_cluster::failover::relay_pools::tenant_pool(
        dc,
        tenant,
        cluster,
        direct,
        sprig_txn::fence::wire::preamble,
    )
    .await
}

/// Whether `tenant`'s `sprig_ctl` work on `cluster` goes through the
/// relay writer now: `Some` with the pool to take connections from, or
/// `None` to use `direct` (and a connection already taken from it).
///
/// # Errors
/// As [`shard_pool`].
pub(crate) async fn shard_route(
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    cluster: &ClusterId,
    direct: &Pool,
) -> Result<Option<Pool>> {
    Ok(sprig_cluster::failover::relay_pools::tenant_route(
        dc,
        tenant,
        cluster,
        direct,
        sprig_txn::fence::wire::preamble,
    )
    .await?
    .map(|r| r.pool))
}
