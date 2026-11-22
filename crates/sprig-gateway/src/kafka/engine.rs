//! Kafka topics stored on SprigDB's own storage engine.
//!
//! A topic `<database>.<stream>` is stored on the engine when its database
//! is an engine database of the tenant, or when the node puts whole
//! tenants there: `SPRIGDB_ENGINE_STREAMS_TENANTS` (`all`, or tenant ids
//! separated by commas). Such topics live in the node's stream log
//! (`<engine dir>/streams`, segment files fed by the engine's commit log)
//! and their offsets and groups are engine records; they never touch
//! MariaDB, the outbox or the sequencer. Every other topic is served as
//! before.
//!
//! The stream log opens with the front end when the node has an engine
//! (`SPRIGDB_ENGINE_DIR` set, or tenants named in the variable above); if
//! it can't open, engine topics are not served and the node's log says
//! why.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use sprig_auth::authz::Action;
use sprig_proto_kafka::topics::{Principal, TopicDefaults};
use sprig_streams::seg::SegConfig;
use sprig_streams::seg::kafka::{EngineStreams, Gate, Need};

use super::data::GatewayData;

/// The variable naming whole tenants whose streams are on the engine.
pub const TENANTS_VAR: &str = "SPRIGDB_ENGINE_STREAMS_TENANTS";

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

/// The tenants named in `SPRIGDB_ENGINE_STREAMS_TENANTS`.
pub fn tenants() -> &'static Tenants {
    static T: OnceLock<Tenants> = OnceLock::new();
    T.get_or_init(|| {
        std::env::var(TENANTS_VAR)
            .ok()
            .and_then(|v| Tenants::parse(&v))
            .unwrap_or_else(|| Tenants::Only(std::collections::BTreeSet::new()))
    })
}

/// Whether the node has an engine for streams to be stored on.
fn configured() -> bool {
    let set = |k: &str| std::env::var(k).is_ok_and(|v| !v.trim().is_empty());
    set(crate::node_engine::DIR_VAR) || set(TENANTS_VAR)
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
            EngineStreams::open(&e, dir, SegConfig::default(), TopicDefaults::default())
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
        let Some(cat) = self.data.catalog_of(p).await else {
            return false;
        };
        let action = match need {
            Need::Read => Action::Select,
            Need::Write => Action::Insert,
            Need::Create => Action::Create,
            Need::Alter => Action::Alter,
            Need::Drop => Action::Drop,
        };
        self.data.allowed_named(p, &cat, db, stream, action)
    }
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

    #[test]
    fn tenant_lists_parse() {
        assert_eq!(Tenants::parse(" ALL "), Some(Tenants::All));
        let t = Tenants::parse("5, 7,").unwrap_or(Tenants::All);
        assert!(t.has(5) && t.has(7) && !t.has(6));
        assert_eq!(Tenants::parse("5,x"), None);
    }
}
