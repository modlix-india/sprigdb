//! Kafka topics stored on SprigDB's own storage engine.
//!
//! Every tenant's topics `<database>.<stream>` are stored on the engine,
//! in the node's stream log (`<engine dir>/streams`, segment files fed by
//! the engine's commit log); their offsets and groups are engine records.
//! They never touch MariaDB.
//!
//! The stream log opens with the front end when the node has an engine
//! (`SPRIGDB_ENGINE_DIR` set); if
//! it can't open, engine topics are not served and the node's log says
//! why.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use sprig_auth::authz::Action;
use sprig_common::{SprigCode, SprigError};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::topics::{Principal, TopicDefaults, TopicError};
use sprig_streams::seg::SegConfig;
use sprig_streams::seg::kafka::{EngineStreams, Gate, Need};

use super::data::GatewayData;

/// Which tenants are wholly on the engine: every one, or these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tenants {
    /// Every tenant.
    All,
    /// These tenants.
    Only(std::collections::BTreeSet<u64>),
}

impl Tenants {
    /// Parses `all` or a comma-separated id list (`None`: malformed).
    #[must_use]
    pub fn parse(v: &str) -> Option<Self> {
        let v = v.trim();
        if v.eq_ignore_ascii_case("all") {
            return Some(Self::All);
        }
        v.split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|x| x.trim().parse().ok())
            .collect::<Option<std::collections::BTreeSet<u64>>>()
            .map(Self::Only)
    }

    /// Whether `t` is one of them.
    #[must_use]
    pub fn has(&self, t: u64) -> bool {
        match self {
            Self::All => true,
            Self::Only(s) => s.contains(&t),
        }
    }
}

/// The tenants whose topics are on the engine: every one (the engine is
/// the only stream store).
#[must_use]
pub fn tenants() -> &'static Tenants {
    static T: Tenants = Tenants::All;
    &T
}

/// Whether the node has an engine for streams to be stored on.
fn configured() -> bool {
    let set = |k: &str| std::env::var(k).is_ok_and(|v| !v.trim().is_empty());
    set(crate::node_engine::DIR_VAR)
}

/// The node's engine-stored streams, opened on first use (with their
/// flusher and maintenance), or `None` when the engine failed to open.
pub fn streams() -> Option<Arc<EngineStreams>> {
    static S: OnceLock<Option<Arc<EngineStreams>>> = OnceLock::new();
    S.get_or_init(|| {
        let opened = crate::node_engine::engine().and_then(|e| {
            let dir = crate::node_engine::config_from_env()?
                .data_dir
                .join("streams");
            let cfg = SegConfig {
                groups: crate::node_engine::stream_groups(),
                blobs: crate::node_engine::note_blobs(),
                compress: crate::node_engine::stream_compress(),
                held: crate::node_engine::engine_raft().map(|_| {
                    sprig_streams::seg::HeldUnits(Arc::new(|u| {
                        crate::node_engine::engine_raft()
                            .is_some_and(|r| r.node().status(u).is_some())
                    }))
                }),
                ..SegConfig::default()
            };
            EngineStreams::open(&e, dir, cfg, TopicDefaults::default())
        });
        match opened {
            Ok(s) => {
                s.spawn_background();
                tracing::info!("kafka streams on the storage engine");
                Some(Arc::new(s))
            }
            Err(e) => {
                tracing::error!(error = %e, "kafka streams on the storage engine not started");
                None
            }
        }
    })
    .clone()
}

/// Where databases are and who may do what, from the tenant's catalog.
pub struct GatewayGate {
    data: Arc<GatewayData>,
}

impl std::fmt::Debug for GatewayGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayGate").finish_non_exhaustive()
    }
}

impl GatewayGate {
    /// Over the data plane's catalog and grants.
    #[must_use]
    pub fn new(data: Arc<GatewayData>) -> Self {
        Self { data }
    }
}

#[async_trait]
impl Gate for GatewayGate {
    async fn on_engine(&self, p: &Principal, db: &str) -> bool {
        if p.tenant_id == 0 {
            return false;
        }
        if tenants().has(p.tenant_id) {
            return true;
        }
        match self.data.catalog_of(p).await {
            Some(cat) => self.data.engine_database(&cat, db),
            None => false,
        }
    }

    fn tenant_on_engine(&self, tenant: u64) -> bool {
        tenant != 0 && tenants().has(tenant)
    }

    async fn allowed(&self, p: &Principal, db: &str, stream: &str, need: Need) -> bool {
        self.check(p, db, stream, &[need])
            .await
            .is_ok_and(|v| v.first() == Some(&true))
    }

    async fn check(
        &self,
        p: &Principal,
        db: &str,
        stream: &str,
        needs: &[Need],
    ) -> Result<Vec<bool>, TopicError> {
        let cat = match self.data.catalog_res(p).await {
            Ok(c) => c,
            Err(e) => return catalog_unreadable(&e, needs.len()),
        };
        Ok(needs
            .iter()
            .map(|n| self.data.allowed_named(p, &cat, db, stream, action_of(*n)))
            .collect())
    }

    async fn limits(&self, p: &Principal) -> sprig_proto_kafka::broker::data::Limits {
        self.data.limits(p).await
    }

    async fn fan_in(&self, p: &Principal) -> Vec<sprig_streams::seg::kafka::FanInTopic> {
        match self.data.catalog_res(p).await {
            Ok(cat) => (*self.data.fan_in_topics(p, &cat).await).clone(),
            Err(_) => Vec::new(),
        }
    }
}

/// The grant a stream need asks for.
fn action_of(need: Need) -> Action {
    match need {
        Need::Read => Action::Select,
        Need::Write => Action::Insert,
        Need::Create => Action::Create,
        Need::Alter => Action::Alter,
        Need::Drop => Action::Drop,
    }
}

/// The tenant's catalog couldn't be read. While it moves with its unit's
/// leader (a retryable error: the replica doesn't lead the unit, or can't
/// yet confirm what it holds) the client gets a retriable answer that sends
/// it back to Metadata; any other failure denies every need, as before.
fn catalog_unreadable(e: &SprigError, needs: usize) -> Result<Vec<bool>, TopicError> {
    if e.code() == Some(SprigCode::ShardMoved) {
        return Err(TopicError::new(err::NOT_LEADER_OR_FOLLOWER, e.to_string()));
    }
    if e.retryable {
        return Err(TopicError::new(err::LEADER_NOT_AVAILABLE, e.to_string()));
    }
    Ok(vec![false; needs])
}

/// Puts engine-stored topics in front of `d`'s stores when the node has an
/// engine; `d` unchanged otherwise.
pub fn wrap(d: &mut super::KafkaDeps) {
    let Some(data) = d.engine_gate.clone() else {
        return;
    };
    if !configured() {
        return;
    }
    let Some(eng) = streams() else {
        return;
    };
    let gate: Arc<dyn Gate> = Arc::new(GatewayGate::new(data));
    d.topics = Arc::new(sprig_streams::seg::kafka::RoutedTopics::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Arc::clone(&d.topics),
    ));
    d.groups = Arc::new(sprig_streams::seg::kafka::RoutedGroups::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Arc::clone(&d.groups),
    ));
    d.data = Some(Arc::new(sprig_streams::seg::kafka::RoutedData::new(
        eng,
        gate,
        d.data.take(),
    )));
}

#[cfg(test)]
mod tests {
    use super::Tenants;

    /// A catalog that can't be read while it moves with its unit's leader
    /// answers a retriable Kafka error, never a denial; other failures deny.
    #[test]
    fn an_unreadable_catalog_is_retriable_while_it_moves() {
        use sprig_common::{SprigCode, SprigError};
        use sprig_proto_kafka::protocol::err;
        let code = |e: SprigError| super::catalog_unreadable(&e, 2).map_err(|t| t.code);
        assert_eq!(
            code(SprigError::new(
                SprigCode::ShardMoved,
                "no leader reachable"
            )),
            Err(err::NOT_LEADER_OR_FOLLOWER)
        );
        assert_eq!(
            code(SprigError::new(
                SprigCode::CatalogUnavailable,
                "doesn't lead the unit"
            )),
            Err(err::LEADER_NOT_AVAILABLE)
        );
        assert_eq!(code(SprigError::internal("broken")), Ok(vec![false, false]));
    }

    #[test]
    fn tenant_lists_parse() {
        assert_eq!(Tenants::parse(" ALL "), Some(Tenants::All));
        let t = Tenants::parse("5, 7,").unwrap_or(Tenants::All);
        assert!(t.has(5) && t.has(7) && !t.has(6));
        assert_eq!(Tenants::parse("5,x"), None);
    }
}
