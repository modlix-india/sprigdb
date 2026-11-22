//! What the engine's topic and group stores fall back to.
//!
//! Every stream is stored on the node's engine, and the engine router in
//! front of these stores ([`super::engine::wrap`]) serves every topic of an
//! engine database. What reaches these stores is the rest:
//!
//! - **Topics** ([`StreamTopics`]): a topic whose database isn't an engine
//!   database, which is a database that doesn't exist (every database is an
//!   engine database), so nothing is listed and changes answer
//!   `UNKNOWN_TOPIC_OR_PARTITION`. The defaults of new topics
//!   (`stream.partitions_default`) are read here for the router.
//! - **Groups** ([`CatalogGroups`]): consumer groups' rows, and offsets of
//!   stream ids the router doesn't hold, as the engine records of the
//!   node's streams (`sprig-streams`' `StreamRecords`).

use std::sync::Arc;

use async_trait::async_trait;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::SprigError;
use sprig_common::ids::TenantId;
use sprig_proto_kafka::group::{CommittedOffset, GroupState, GroupStore, StoreError, StoredGroup};
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::topics::{
    NewTopic, Principal, TopicConfigs, TopicDefaults, TopicError, TopicInfo, Topics,
};

use crate::tenant::TenantServices;

/// The topic store behind the engine router.
pub struct StreamTopics {
    svc: Arc<TenantServices>,
}

impl std::fmt::Debug for StreamTopics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTopics").finish_non_exhaustive()
    }
}

/// Splits `<database>.<stream>`.
fn split(name: &str) -> Result<(&str, &str), TopicError> {
    sprig_proto_kafka::mem::split_topic(name)
}

fn catalog_error(e: &SprigError) -> TopicError {
    TopicError::new(
        err::UNKNOWN_SERVER_ERROR,
        format!("{} {}", e.protocol().mysql_errno, e),
    )
}

/// A topic named in a database that isn't an engine database.
fn no_database(name: &str) -> TopicError {
    let db = split(name).map_or(name, |(d, _)| d);
    TopicError::new(
        err::UNKNOWN_TOPIC_OR_PARTITION,
        format!("1049 Unknown database '{db}': a topic is named <database>.<stream>"),
    )
}

impl StreamTopics {
    /// Topics of `svc`'s tenants.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self { svc }
    }

    async fn catalog(&self, p: &Principal) -> Result<Arc<TenantCatalog>, TopicError> {
        self.svc
            .cache
            .get(TenantId(p.tenant_id), Freshness::Latest)
            .await
            .map_err(|e| catalog_error(&e))
    }
}

#[async_trait]
impl Topics for StreamTopics {
    async fn list(&self, _p: &Principal) -> Result<Vec<TopicInfo>, TopicError> {
        Ok(Vec::new())
    }

    async fn get(&self, _p: &Principal, _name: &str) -> Result<Option<TopicInfo>, TopicError> {
        Ok(None)
    }

    async fn defaults(&self, p: &Principal, name: &str) -> Result<TopicDefaults, TopicError> {
        use sprig_streams::settings as s;
        let cat = self.catalog(p).await?;
        let (db, _) = split(name)?;
        let dc = &self.svc.dc;
        let mut ctx = cat.setting_ctx(dc);
        if let Some(d) = cat.databases.iter().find(|d| d.name == db) {
            ctx = ctx.with_db(d.db_id);
        }
        let get = |k: &'static sprig_settings::Setting<u64>| {
            self.svc.settings.get(k, &ctx, Some(&cat.settings))
        };
        Ok(TopicDefaults {
            partitions: i32::try_from(get(&s::PARTITIONS_DEFAULT)).unwrap_or(12),
            ..TopicDefaults::default()
        })
    }

    async fn create(
        &self,
        _p: &Principal,
        t: NewTopic,
        _validate_only: bool,
    ) -> Result<TopicInfo, TopicError> {
        Err(no_database(&t.name))
    }

    async fn delete(&self, _p: &Principal, name: &str) -> Result<TopicInfo, TopicError> {
        Err(no_database(name))
    }

    async fn grow(
        &self,
        _p: &Principal,
        name: &str,
        _partitions: i32,
        _validate_only: bool,
    ) -> Result<(), TopicError> {
        Err(no_database(name))
    }

    async fn set_configs(
        &self,
        _p: &Principal,
        name: &str,
        _configs: TopicConfigs,
        _validate_only: bool,
    ) -> Result<(), TopicError> {
        Err(no_database(name))
    }
}

/// Consumer groups and their committed offsets, as the engine records of
/// the node's streams (`sprig-streams`' `StreamRecords`), the same records
/// engine streams keep their groups and offsets in. In front of it the
/// engine router handles engine streams' offsets itself, so this store
/// keeps the groups' rows and answers the offsets of other stream ids.
pub struct CatalogGroups;

impl std::fmt::Debug for CatalogGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogGroups").finish_non_exhaustive()
    }
}

fn store_err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

impl CatalogGroups {
    /// The node's engine streams, whose records hold groups and offsets.
    fn eng() -> Result<Arc<sprig_streams::seg::kafka::EngineStreams>, StoreError> {
        super::engine::streams().ok_or_else(|| {
            StoreError(
                "consumer groups are stored on the SprigDB engine, which isn't running on this \
                 node"
                    .to_owned(),
            )
        })
    }
}

#[async_trait]
impl GroupStore for CatalogGroups {
    async fn commit(
        &self,
        tenant: u64,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), StoreError> {
        let on: Vec<(u64, CommittedOffset)> = offsets.iter().map(|o| (0, o.clone())).collect();
        Self::eng()?
            .records()
            .commit_offsets_on(tenant, group, &on, sprig_streams::seg::now_ms())
            .await
            .map_err(|e| store_err(&e))
    }

    async fn fetch(
        &self,
        tenant: u64,
        group: &str,
        only: Option<&[(u64, i32)]>,
    ) -> Result<Vec<CommittedOffset>, StoreError> {
        let rows = Self::eng()?
            .records()
            .offsets_on(tenant, group)
            .map_err(|e| store_err(&e))?;
        Ok(rows
            .into_iter()
            // Engine streams' offsets come from the router in front.
            .filter(|(instance, o)| {
                *instance == 0 && !sprig_streams::seg::kafka::is_engine_stream(o.stream_id)
            })
            .map(|(_, o)| o)
            .filter(|o| only.is_none_or(|k| k.contains(&(o.stream_id, o.partition))))
            .collect())
    }

    async fn save_group(
        &self,
        tenant: u64,
        g: &StoredGroup,
        state: GroupState,
    ) -> Result<(), StoreError> {
        Self::eng()?
            .records()
            .save_group(tenant, g, state)
            .await
            .map_err(|e| store_err(&e))
    }

    async fn load_group(
        &self,
        tenant: u64,
        group: &str,
    ) -> Result<Option<StoredGroup>, StoreError> {
        Self::eng()?
            .records()
            .load_group(tenant, group)
            .map_err(|e| store_err(&e))
    }

    async fn list_groups(&self, tenant: u64) -> Result<Vec<StoredGroup>, StoreError> {
        Self::eng()?
            .records()
            .groups(tenant)
            .map_err(|e| store_err(&e))
    }

    async fn delete_group(&self, tenant: u64, group: &str) -> Result<(), StoreError> {
        Self::eng()?
            .records()
            .delete_group(tenant, group)
            .await
            .map_err(|e| store_err(&e))
    }
}
