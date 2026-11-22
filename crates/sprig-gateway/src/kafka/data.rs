//! The tenant catalog and privileges the engine's Kafka data plane asks
//! about: [`GatewayData`] tells the engine router which databases are
//! engine databases, whether a principal may read, write or change a
//! stream, and the produce and fetch limits (`stream.produce_wait_ms`,
//! `kafka.fetch_max_wait_ms`). Produce, Fetch, ListOffsets and
//! OffsetForLeaderEpoch themselves run on the engine's stream log
//! (`sprig-streams`' `seg::kafka`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{TenantId, UserId};
use sprig_proto_kafka::broker::data::Limits;
use sprig_proto_kafka::topics::Principal;

use crate::tenant::TenantServices;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Compiled privileges by (tenant, user), with the catalog and auth
/// versions they were compiled at.
type GrantCache = HashMap<(u64, Option<u64>), (u64, u64, Arc<dyn SessionGrants>)>;

/// The tenant catalog and privileges of the Kafka front end.
pub struct GatewayData {
    svc: Arc<TenantServices>,
    grants: Mutex<GrantCache>,
}

impl std::fmt::Debug for GatewayData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayData").finish_non_exhaustive()
    }
}

impl GatewayData {
    /// Over `svc`'s tenants.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            grants: Mutex::new(HashMap::new()),
        }
    }

    fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    fn grants(&self, p: &Principal, cat: &TenantCatalog) -> Arc<dyn SessionGrants> {
        let key = (p.tenant_id, p.user_id);
        if let Some((v, a, g)) = lock(&self.grants).get(&key)
            && *v == cat.version
            && *a == cat.auth_version
        {
            return Arc::clone(g);
        }
        let user = p.user_id.and_then(|u| u32::try_from(u).ok()).map(UserId);
        let g = crate::tenant::authz::grants_of(&self.svc, cat, user);
        lock(&self.grants).insert(key, (cat.version, cat.auth_version, Arc::clone(&g)));
        g
    }

    /// Whether `p` may `action` the stream named `stream` in database
    /// `db` (by name or on the database).
    pub(crate) fn allowed_named(
        &self,
        p: &Principal,
        cat: &TenantCatalog,
        db: &str,
        stream: &str,
        action: Action,
    ) -> bool {
        let g = self.grants(p, cat);
        if g.unrestricted() {
            return true;
        }
        let lctn = self.lctn(cat);
        let db_key = database_key(db, lctn);
        let name_key = object_key(ObjectKind::Table, stream, lctn);
        g.allows(
            action,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || g.allows(action, Target::Database { db_key: &db_key })
    }

    /// The tenant's catalog.
    pub(crate) async fn catalog_of(&self, p: &Principal) -> Option<Arc<TenantCatalog>> {
        self.svc
            .cache
            .get(TenantId(p.tenant_id), Freshness::Latest)
            .await
            .ok()
    }

    /// Whether `p`'s tenant has a database `db` stored on the engine.
    pub(crate) fn engine_database(&self, cat: &TenantCatalog, db: &str) -> bool {
        crate::engine::catalog::of(cat).is_some_and(|eng| {
            cat.database(&database_key(db, self.lctn(cat)))
                .is_some_and(|d| eng.dbs.contains(&d.db_id))
        })
    }

    /// The produce and fetch limits for `p`.
    pub(crate) async fn limits(&self, p: &Principal) -> Limits {
        let Some(cat) = self.catalog_of(p).await else {
            return Limits::default();
        };
        let ctx = cat.setting_ctx(&self.svc.dc);
        Limits {
            produce_wait: Duration::from_millis(self.svc.settings.get(
                &sprig_streams::settings::PRODUCE_WAIT_MS,
                &ctx,
                Some(&cat.settings),
            )),
            fetch_max_wait: Duration::from_millis(self.svc.settings.get(
                &super::FETCH_MAX_WAIT_MS,
                &ctx,
                Some(&cat.settings),
            )),
        }
    }
}
