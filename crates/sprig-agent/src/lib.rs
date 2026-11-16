//! `sprig-agent`: the agent role, the node agent for VMs: MariaDB install,
//! settings, health, backups.
//!
//! Layer 3 (roles).
//!
//! What exists: [`fence`], the self-fence of the agent's MariaDB node:
//! the DC permit, the writer lease, `read_only` and session kills, and
//! the watchdog. It runs when `[agent] cluster` names the node's cluster.
//! Not yet: the provisioning plan engine and MariaDB supervision.

pub mod fence;
pub mod rebuild;
pub mod relay;
pub mod relay_role;

use std::sync::Arc;
use std::time::Duration;

use sprig_common::{Result, Role, RoleContext};
use sprig_meta::MetaStore;
use sprig_meta::etcd::{DEFAULT_REQUEST_TIMEOUT, EtcdTls, connect_tiers};

/// Runs the agent role until `ctx.shutdown` is cancelled.
///
/// The `sprigdb` binary has already bound the telemetry port, so
/// `/healthz` answers and `/readyz` reports `starting` until this marks the
/// process ready.
///
/// # Errors
/// 39998 for unreadable TLS files or an `[agent]` that names no node of
/// its cluster; 39500 if no etcd seed answers at start-up.
#[allow(clippy::too_many_lines)] // the role's start-up, step by step
pub async fn run(ctx: RoleContext) -> Result<()> {
    let role = Role::Agent;
    let cfg = &ctx.config;
    // A restart announcement (SIGUSR1) can come at any time: listen from
    // the start, so the signal never ends the process.
    let handover = Arc::new(std::sync::atomic::AtomicU64::new(0));
    #[cfg(unix)]
    listen_for_restart(Arc::clone(&handover), ctx.shutdown.clone());
    if cfg.agent.cluster.is_none() {
        tracing::warn!(%role, "no [agent] cluster is set: this agent has no node to manage or fence");
        ctx.health.set_ready();
        ctx.shutdown.cancelled().await;
        ctx.health.set_draining();
        return Ok(());
    }
    let tls = cfg.tls.as_ref().map(EtcdTls::from_files).transpose()?;
    let tiers = connect_tiers(
        cfg.node.dc.clone(),
        &cfg.meta,
        tls,
        DEFAULT_REQUEST_TIMEOUT,
        false,
    )
    .await?;
    let store: Arc<dyn MetaStore> = Arc::new(tiers.dc);
    // The cluster may be registered after the agent starts.
    let target = loop {
        match fence::target(&*store, &cfg.agent).await {
            Ok(t) => break t,
            Err(e) => {
                tracing::warn!(error = %e, "can't find the node to fence yet; retrying");
                tokio::select! {
                    () = ctx.shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    };
    // A Galera node with a relay: Sprig 2PC's relay writer election and
    // the relay itself. The primary + replicas fence below doesn't apply
    // (it would make every node but one read-only).
    if let Some(relay_addr) = cfg.agent.relay_addr
        && is_galera(&*store, &target.cluster).await?
    {
        return run_relay(&ctx, store, target, relay_addr).await;
    }
    let mut agent = fence::AgentFence::new(
        cfg.node.id(),
        cfg.node.dc.clone(),
        Arc::clone(&store),
        target.clone(),
    )
    .with_handover_flag(handover);
    // node_rebuild: the internal service (backup streams and
    // rebuilds), when `[agent] internal_addr` is set.
    let mut service_task = None;
    if let Some(addr) = cfg.agent.internal_addr {
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::ConfigInvalid,
                format!("[agent] internal_addr {addr}: {e}"),
            )
        })?;
        let bound = listener.local_addr().map_err(|e| {
            sprig_common::SprigError::internal(format!("[agent] internal_addr: {e}"))
        })?;
        // Mutual TLS with `[tls]` (the backup stream carries
        // tenant data); plain HTTP only without it, for development.
        let internal_tls = cfg
            .tls
            .as_ref()
            .map(sprig_cluster::failover::internal_tls::InternalTls::from_config)
            .transpose()?;
        let scheme = if internal_tls.is_some() {
            "https"
        } else {
            "http"
        };
        if internal_tls.is_none() {
            tracing::warn!(
                "no [tls]: the internal service (backup streams) is plain HTTP; development only"
            );
        }
        let url = cfg
            .agent
            .internal_advertise
            .clone()
            .unwrap_or_else(|| format!("{scheme}://{bound}"));
        let mut service = rebuild::RebuildService::new(
            cfg.agent.rebuild.clone(),
            store,
            target.cluster.clone(),
            target.node.clone(),
            target.endpoint.clone(),
            agent.hold(),
        );
        if let Some(t) = internal_tls {
            service = service.with_tls(t);
        }
        let service = Arc::new(service);
        service_task = Some(rebuild::serve(
            listener,
            Arc::clone(&service),
            ctx.shutdown.clone(),
        ));
        tracing::info!(%url, "agent internal service (node_rebuild) listening");
        agent = agent.with_rebuild(url, service);
    }
    ctx.health.set_ready();
    tracing::info!(%role, "ready");
    let r = agent
        .run(Duration::from_millis(1000), ctx.shutdown.clone())
        .await;
    if let Some(t) = service_task {
        let _ = t.await;
    }
    ctx.health.set_draining();
    r
}

/// SIGUSR1 announces a restart ([`fence::arm_handover`]): `sprig-vm.sh
/// upgrade` sends it before it replaces the agent.
#[cfg(unix)]
fn listen_for_restart(
    flag: Arc<std::sync::atomic::AtomicU64>,
    stop: sprig_common::CancellationToken,
) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut usr1 = match signal(SignalKind::user_defined1()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "can't listen for SIGUSR1: restarts won't be handed over");
            return;
        }
    };
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                r = usr1.recv() => {
                    if r.is_none() {
                        break;
                    }
                    fence::arm_handover(&flag);
                }
            }
        }
    });
}

async fn is_galera(store: &dyn MetaStore, cluster: &sprig_common::ids::ClusterId) -> Result<bool> {
    let key = store.tier().key(&format!("clusters/{cluster}"))?;
    let Some(kv) = store
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await?
    else {
        return Ok(false);
    };
    let rec = sprig_meta::Record::<sprig_cluster::record::ClusterRecord>::decode(&kv.value)?.body;
    Ok(rec.topology == sprig_cluster::record::TopologyKind::Galera)
}

/// The agent of a Galera node with `[agent] relay_addr` (module
/// [`relay_role`]).
async fn run_relay(
    ctx: &RoleContext,
    store: Arc<dyn MetaStore>,
    target: fence::Target,
    relay_addr: std::net::SocketAddr,
) -> Result<()> {
    let cfg = &ctx.config;
    let invalid =
        |m: String| sprig_common::SprigError::new(sprig_common::SprigCode::ConfigInvalid, m);
    let internal_addr = cfg.agent.internal_addr.ok_or_else(|| {
        invalid(
            "[agent] relay_addr needs [agent] internal_addr: gateways make the holder calls there"
                .into(),
        )
    })?;
    let relay_listener = tokio::net::TcpListener::bind(relay_addr)
        .await
        .map_err(|e| invalid(format!("[agent] relay_addr {relay_addr}: {e}")))?;
    let internal = tokio::net::TcpListener::bind(internal_addr)
        .await
        .map_err(|e| invalid(format!("[agent] internal_addr {internal_addr}: {e}")))?;
    let internal_tls = cfg
        .tls
        .as_ref()
        .map(sprig_cluster::failover::internal_tls::InternalTls::from_config)
        .transpose()?;
    if internal_tls.is_none() {
        tracing::warn!("no [tls]: the relay's holder calls are plain HTTP; development only");
    }
    let scheme = if internal_tls.is_some() {
        "https"
    } else {
        "http"
    };
    let bound_internal = internal
        .local_addr()
        .map_err(|e| sprig_common::SprigError::internal(format!("[agent] internal_addr: {e}")))?;
    let bound_relay = relay_listener
        .local_addr()
        .map_err(|e| sprig_common::SprigError::internal(format!("[agent] relay_addr: {e}")))?;
    let internal_url = cfg
        .agent
        .internal_advertise
        .clone()
        .unwrap_or_else(|| format!("{scheme}://{bound_internal}"));
    let advertised = cfg
        .agent
        .relay_advertise
        .clone()
        .unwrap_or_else(|| bound_relay.to_string());
    let role = relay_role::RelayRole::new(
        cfg.node.id(),
        store,
        target,
        advertised.clone(),
        internal_url.clone(),
    );
    let service =
        rebuild::serve_router(internal, role.routes(), internal_tls, ctx.shutdown.clone());
    tracing::info!(relay = %advertised, internal = %internal_url, "agent relay listening");
    ctx.health.set_ready();
    let r = role.run(relay_listener, ctx.shutdown.clone()).await;
    let _ = service.await;
    ctx.health.set_draining();
    r
}
