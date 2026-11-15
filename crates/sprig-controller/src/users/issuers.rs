//! A tenant's token issuers (for external tokens) through the Admin API:
//!
//! - `GET /admin/v1/tenants/{name}/issuers`: the issuers with their claim
//!   mappings;
//! - `POST /admin/v1/tenants/{name}/issuers`: creates or replaces an issuer
//!   (by `issuer`) with its mappings, or disables it (`"active": false`);
//! - `POST /admin/v1/tenants/{name}/users:setTokenLogin`: adds or removes
//!   an account's `token` method (`{"user", "host", "enabled"}`).
//!
//! Each write is one auth change in the tenant's catalog, so every
//! gateway follows within its watch's latency; the catalog's auth module
//! is brought to format 7 first. `sprigctl tenant issuer` and `tenant user
//! token-login` call them.

use serde::{Deserialize, Serialize};
use sprig_auth::oidc::IssuerSpec;
use sprig_auth::oidc::store as issuers;
use sprig_auth::store::{AUTH_MODULE, AuthStore};
use sprig_catalog::Migrator;
use sprig_common::Result;
use sprig_common::ids::{TenantId, UserId};

use super::TenantUsers;

/// `GET /admin/v1/tenants/{name}/issuers`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListIssuersResponse {
    /// The tenant.
    pub tenant: String,
    /// Its issuers, with their mappings.
    pub issuers: Vec<IssuerSpec>,
}

/// What `POST /admin/v1/tenants/{name}/issuers` did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutIssuerResponse {
    /// The tenant.
    pub tenant: String,
    /// The issuer's id.
    pub issuer_id: u32,
    /// The issuer.
    pub issuer: String,
    /// Whether it is active.
    pub active: bool,
    /// The mappings stored.
    pub mappings: usize,
    /// The auth version the change committed at.
    pub auth_version: u64,
}

/// `POST /admin/v1/tenants/{name}/users:setTokenLogin`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenLoginRequest {
    /// The user name, without the tenant prefix.
    pub user: String,
    /// The host pattern; `%` without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Add (`true`) or remove the `token` method.
    pub enabled: bool,
}

/// What `users:setTokenLogin` did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenLoginResponse {
    /// The tenant.
    pub tenant: String,
    /// The user.
    pub user: String,
    /// The host pattern.
    pub host: String,
    /// The user's id.
    pub user_id: UserId,
    /// The `token` method is on.
    pub enabled: bool,
    /// The auth version the change committed at.
    pub auth_version: u64,
}

impl TenantUsers {
    async fn migrated(&self, tenant: &str) -> Result<TenantId> {
        let id = self.active(tenant).await?;
        let ep = self.locator.writer(id).await?;
        let mut conn = crate::catalogs::acquire(&ep.pool, "auth tables").await?;
        crate::catalogs::migrate_catalog(
            &*self.locator,
            id,
            &mut conn,
            ep.store.schema(),
            &Migrator::new(&[AUTH_MODULE])?,
        )
        .await?;
        Ok(id)
    }

    /// The issuers of the tenant named `tenant`.
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; the catalog's errors.
    pub async fn list_issuers(&self, tenant: &str) -> Result<ListIssuersResponse> {
        let id = self.active(tenant).await?;
        let ep = self.locator.writer(id).await?;
        let mut conn = crate::catalogs::acquire(&ep.pool, "issuers").await?;
        let list = issuers::read_issuers(&AuthStore::new(ep.store.schema()), &mut conn).await?;
        Ok(ListIssuersResponse {
            tenant: tenant.to_owned(),
            issuers: list,
        })
    }

    /// Creates or replaces an issuer of the tenant named `tenant`.
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; 39970 while the audit spool
    /// is full; 39998 for a spec [`IssuerSpec::validate`] refuses; the
    /// catalog's errors.
    pub async fn put_issuer(&self, tenant: &str, spec: IssuerSpec) -> Result<PutIssuerResponse> {
        sprig_common::audit::check_available()?;
        spec.validate()?;
        let id = self.migrated(tenant).await?;
        let (issuer_id, auth_version) = issuers::put_issuer(&self.catalog, id, &spec).await?;
        sprig_common::audit::record(
            sprig_common::audit::AuditStream::Tenant(id),
            "admin.issuer_changed",
            serde_json::json!({"tenant": tenant, "issuer_id": issuer_id, "issuer": spec.issuer,
                "active": spec.active, "jit_users": spec.jit_users,
                "protocols": spec.protocols, "mappings": spec.mappings.len()}),
        );
        tracing::info!(event = "admin.issuer_changed", tenant, issuer = %spec.issuer, issuer_id, active = spec.active, "issuer changed");
        Ok(PutIssuerResponse {
            tenant: tenant.to_owned(),
            issuer_id,
            issuer: spec.issuer,
            active: spec.active,
            mappings: spec.mappings.len(),
            auth_version,
        })
    }

    /// Adds or removes an account's `token` method.
    ///
    /// # Errors
    /// 39072 for an unknown or inactive tenant; 39970 while the audit spool
    /// is full; 1396 for an unknown account; the catalog's errors.
    pub async fn set_token_login(
        &self,
        tenant: &str,
        req: TokenLoginRequest,
    ) -> Result<TokenLoginResponse> {
        sprig_common::audit::check_available()?;
        let id = self.migrated(tenant).await?;
        let host = req.host.clone().unwrap_or_else(|| "%".into());
        let c = issuers::set_token_method(&self.catalog, id, &req.user, &host, req.enabled).await?;
        sprig_common::audit::record(
            sprig_common::audit::AuditStream::Tenant(id),
            "admin.token_login_changed",
            serde_json::json!({"tenant": tenant, "user": req.user, "host": host,
                "user_id": c.user_id, "enabled": req.enabled}),
        );
        Ok(TokenLoginResponse {
            tenant: tenant.to_owned(),
            user: req.user,
            host,
            user_id: c.user_id,
            enabled: req.enabled,
            auth_version: c.auth_version,
        })
    }
}
