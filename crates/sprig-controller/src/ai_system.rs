//! The `ai_system` loop: the administration assistant's conversation
//! store, `__sprig_ai` on the DC's system cluster
//! ([`sprig_ai::audit::system`]).
//!
//! ```text
//! on the DC leader, at start, on every cluster record change and hourly:
//!   system := the lowest-numbered cluster that is active, draining or drained
//!             (ClusterRegistry::system_cluster); none yet: look again in a minute
//!   as sprig_ctl on its writer (the loop's own connection budget):
//!     CREATE the schema and tables IF NOT EXISTS
//!     at most hourly: drop payloads older than the global ai.audit_payload_days,
//!     keeping their hashes (sprig_ai_payloads_purged_total)
//! ```
//!
//! Without `sprig_ctl`'s grant on `__sprig_ai` the pass fails naming the
//! grant (the registration precheck lists it as an advisory too), and
//! administration turns run unstored until it is given.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use mysql_async::Conn;
use sprig_ai::audit::system::{ensure_schema, purge_payloads};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::Result;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::names::AI_SCHEMA;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::catalogs::{ClusterPools, RegistryPools, acquire};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};

/// How often payloads are purged, and the schema looked at.
pub const PASS_INTERVAL: Duration = Duration::from_secs(3600);
/// How soon a DC without a system cluster yet is looked at again.
const NO_CLUSTER_RECHECK: Duration = Duration::from_secs(60);
/// The loop's one key.
const KEY: &str = "system";

/// Where the system cluster is and how to reach it.
#[async_trait::async_trait]
pub trait SystemClusters: Send + Sync + 'static {
    /// The DC's system cluster, if it has one yet.
    async fn system(&self) -> Result<Option<ClusterId>>;
    /// A `sprig_ctl` session on its writer.
    async fn open(&self, cluster: &ClusterId) -> Result<Conn>;
}

/// [`SystemClusters`] over the DC's registry, with its own connection
/// budget.
pub struct RegistrySystem {
    registry: ClusterRegistry,
    pools: RegistryPools,
}

impl std::fmt::Debug for RegistrySystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrySystem").finish_non_exhaustive()
    }
}

impl RegistrySystem {
    /// Over `registry`, with `pools` (a budget of its own).
    #[must_use]
    pub fn new(registry: ClusterRegistry, pools: RegistryPools) -> Self {
        Self { registry, pools }
    }
}

#[async_trait::async_trait]
impl SystemClusters for RegistrySystem {
    async fn system(&self) -> Result<Option<ClusterId>> {
        self.registry.system_cluster().await
    }

    async fn open(&self, cluster: &ClusterId) -> Result<Conn> {
        let pool = self.pools.writer(cluster).await?;
        acquire(&pool, "administration conversations").await
    }
}

/// What one pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pass {
    /// The system cluster, when there is one.
    pub cluster: Option<ClusterId>,
    /// Payloads purged, when the purge ran.
    pub purged: Option<u64>,
}

/// The loop (see the module docs).
pub struct AiSystemLoop {
    clusters: Arc<dyn SystemClusters>,
    live: Option<(LiveSettings, DcId)>,
    schema: String,
    /// When the purge last ran, and on which cluster.
    last_purge: Mutex<Option<(ClusterId, Instant)>>,
}

impl std::fmt::Debug for AiSystemLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiSystemLoop")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl AiSystemLoop {
    /// The loop over `clusters`, reading `ai.audit_payload_days` from
    /// `live` (its default without).
    #[must_use]
    pub fn new(clusters: Arc<dyn SystemClusters>, live: Option<(LiveSettings, DcId)>) -> Self {
        Self {
            clusters,
            live,
            schema: AI_SCHEMA.to_owned(),
            last_purge: Mutex::new(None),
        }
    }

    /// The same over another schema (tests).
    #[must_use]
    pub fn in_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// The global (or DC) `ai.audit_payload_days`.
    #[must_use]
    pub fn days(&self) -> u64 {
        let key = &sprig_ai::settings::AUDIT_PAYLOAD_DAYS;
        match &self.live {
            Some((l, dc)) => l.get(key, &SettingCtx::process(dc)),
            None => key.default,
        }
    }

    /// One pass: the schema on the system cluster, and the purge when it
    /// is due.
    ///
    /// # Errors
    /// The registry's; the connection's; the schema's (a missing grant
    /// names it).
    pub async fn pass(&self) -> Result<Pass> {
        let Some(cluster) = self.clusters.system().await? else {
            return Ok(Pass {
                cluster: None,
                purged: None,
            });
        };
        let mut conn = self.clusters.open(&cluster).await?;
        ensure_schema(&mut conn, &self.schema).await?;
        let due = self
            .last_purge
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_none_or(|(c, at)| *c != cluster || at.elapsed() >= PASS_INTERVAL);
        let mut purged = None;
        if due {
            let days = self.days();
            let n = purge_payloads(&mut conn, &self.schema, days).await?;
            if n > 0 {
                sprig_ai::metrics::payloads_purged(n);
                tracing::debug!(%cluster, n, days,
                    "administration conversations' payloads past their retention purged");
            }
            *self
                .last_purge
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some((cluster.clone(), Instant::now()));
            purged = Some(n);
        }
        Ok(Pass {
            cluster: Some(cluster),
            purged,
        })
    }
}

#[async_trait::async_trait]
impl Reconciler for AiSystemLoop {
    type Key = String;

    fn name(&self) -> &'static str {
        "ai_system"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::dc("clusters/")]
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<String> {
        vec![KEY.to_owned()]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<String>> {
        Ok(vec![KEY.to_owned()])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &String) -> Result<Outcome> {
        cx.leader()?;
        let p = self.pass().await?;
        Ok(Outcome::RequeueAfter(if p.cluster.is_some() {
            PASS_INTERVAL
        } else {
            NO_CLUSTER_RECHECK
        }))
    }
}

#[cfg(test)]
mod tests {
    use mysql_async::prelude::Queryable as _;

    use super::*;

    /// One fixed cluster at `pool`, or none.
    struct Fixed(Option<(ClusterId, mysql_async::Pool)>);

    #[async_trait::async_trait]
    impl SystemClusters for Fixed {
        async fn system(&self) -> Result<Option<ClusterId>> {
            Ok(self.0.as_ref().map(|(c, _)| c.clone()))
        }
        async fn open(&self, _cluster: &ClusterId) -> Result<Conn> {
            let (_, pool) = self.0.as_ref().expect("a cluster");
            acquire(pool, "test").await
        }
    }

    #[tokio::test]
    async fn without_a_system_cluster_nothing_happens() {
        let l = AiSystemLoop::new(Arc::new(Fixed(None)), None);
        assert_eq!(
            l.pass().await.unwrap(),
            Pass {
                cluster: None,
                purged: None
            }
        );
        assert_eq!(l.days(), 30);
    }

    /// Live: `SPRIG_TEST_MARIADB_SINGLE`; works in `ai_sys_ctl_<pid>`,
    /// dropped afterwards.
    #[tokio::test]
    async fn live_the_pass_creates_the_schema_and_purges_hourly() {
        let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
            eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
            return;
        };
        let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
            .pool_opts(
                mysql_async::PoolOpts::default()
                    .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(30)),
            );
        let pool = mysql_async::Pool::new(opts);
        let schema = format!("ai_sys_ctl_{}", std::process::id());
        let cluster = ClusterId::new("dev1-c1").unwrap();
        let l = AiSystemLoop::new(Arc::new(Fixed(Some((cluster.clone(), pool.clone())))), None)
            .in_schema(schema.clone());
        let r = async {
            let p = l.pass().await?;
            assert_eq!(p.cluster, Some(cluster));
            assert_eq!(p.purged, Some(0));
            // The next pass within the hour only checks the schema.
            assert_eq!(l.pass().await?.purged, None);
            let mut c = acquire(&pool, "test").await?;
            let n: Option<u64> = c
                .exec_first(
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
                    (schema.as_str(),),
                )
                .await
                .map_err(|e| sprig_ai::audit::system::store_error("t", e))?;
            assert_eq!(n, Some(4));
            Ok::<(), sprig_common::SprigError>(())
        }
        .await;
        let mut c = pool.get_conn().await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            .await
            .unwrap();
        drop(c);
        pool.disconnect().await.unwrap();
        r.unwrap();
    }
}
