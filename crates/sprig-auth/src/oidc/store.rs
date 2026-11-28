//! `oidc_issuers` and `claim_mappings` as catalog records
//! (`m/auth/oidc_issuers/<issuer>`, `m/auth/claim_mappings/<issuer>/<mapping>`):
//! rows, reads for the cache, and the writes the Admin API makes (each one
//! auth change, so every gateway's `TenantAuth` follows at once).

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{self, Pk, RecordTx};
use sprig_catalog::{AuthCatalog, AuthTx, ChangeOpts, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};

use super::jwks::check_url;
use super::jwt::Alg;
use crate::admin::{Changed, MAX_NAME};
use crate::rec::{CLAIM_MAPPINGS, OIDC_ISSUERS, host_eq, is_duplicate};
use crate::store::{AuthMethods, AuthStore, UserRow, UserSource};

/// The protocols an issuer's tokens log in over (`oidc_issuers.protocols`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenProtocol {
    /// MySQL and MariaDB: the token as the clear-text password.
    Mysql,
    /// MongoDB: `MONGODB-OIDC`.
    Mongodb,
    /// Redis: the token as `AUTH`'s password.
    Redis,
    /// HTTP and gRPC: `Authorization: Bearer`, or the `Basic` password of
    /// an account with the `token` method.
    Http,
}

impl TokenProtocol {
    /// Every protocol.
    pub const ALL: [TokenProtocol; 4] = [
        TokenProtocol::Mysql,
        TokenProtocol::Mongodb,
        TokenProtocol::Redis,
        TokenProtocol::Http,
    ];

    /// The SQL `SET` member.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            TokenProtocol::Mysql => "mysql",
            TokenProtocol::Mongodb => "mongodb",
            TokenProtocol::Redis => "redis",
            TokenProtocol::Http => "http",
        }
    }
}

/// `oidc_issuers.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssuerKind {
    /// An OIDC provider.
    #[default]
    Oidc,
    /// The pluggable end-user layer: its tokens map to a service account
    /// per application, and their claims reach row-level security. It
    /// never creates accounts (`jit_users` is refused), and a login's audit
    /// names the end user (`sub`).
    EndUserLayer,
}

impl IssuerKind {
    /// The SQL text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            IssuerKind::Oidc => "oidc",
            IssuerKind::EndUserLayer => "end_user_layer",
        }
    }
}

/// `claim_mappings.kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MappingKind {
    /// Names the account: the first match by priority wins.
    User,
    /// Adds a role to the session (`token_roles`).
    Role,
}

/// `claim_mappings.match_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchKind {
    /// The claim is present (not null).
    #[default]
    Any,
    /// The claim (or one element of an array claim) equals `match_value`.
    Equals,
    /// The claim (or one element) matches `match_value` as a whole.
    Regex,
}

/// One claim mapping, as stored and as the Admin API takes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimMapping {
    /// Ascending; the first user mapping that matches names the account.
    pub priority: i16,
    /// User or role.
    pub kind: MappingKind,
    /// The claim: a top-level name, or a dotted path into objects
    /// (`realm_access.roles`) when no top-level claim has the name.
    pub claim: String,
    /// How the claim is matched.
    #[serde(default)]
    pub match_kind: MatchKind,
    /// The value or regular expression; none for `any`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_value: Option<String>,
    /// A user name template (`{email}`, `app-{azp}`) or a role name.
    pub target: String,
}

/// An issuer with its mappings: the Admin API's body and answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerSpec {
    /// The id (answers only; ignored in a body).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_id: Option<u32>,
    /// OIDC provider or end-user layer.
    #[serde(default)]
    pub kind: IssuerKind,
    /// The exact `iss` value; the key a write replaces by.
    pub issuer: String,
    /// The key set's URL; none: OIDC discovery from the issuer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_url: Option<String>,
    /// Accepted `aud` values (at least one).
    pub audiences: Vec<String>,
    /// Accepted algorithms (default `["RS256"]`).
    #[serde(default = "default_algorithms")]
    pub algorithms: Vec<String>,
    /// The protocols tokens log in over (default all four).
    #[serde(default = "default_protocols")]
    pub protocols: Vec<TokenProtocol>,
    /// `MONGODB-OIDC`'s human flow: the client id drivers ask the provider
    /// for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// `MONGODB-OIDC`'s human flow: scopes to request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_scopes: Option<Vec<String>>,
    /// Create the account a user mapping names when it doesn't exist.
    #[serde(default)]
    pub jit_users: bool,
    /// The longest token lifetime accepted (`exp - iat`), seconds.
    #[serde(default = "default_lifetime")]
    pub max_lifetime_s: u32,
    /// `false` disables the issuer (its tokens are refused).
    #[serde(default = "default_true")]
    pub active: bool,
    /// The mappings, replacing the issuer's previous ones.
    #[serde(default)]
    pub mappings: Vec<ClaimMapping>,
}

fn default_algorithms() -> Vec<String> {
    vec!["RS256".into()]
}
fn default_protocols() -> Vec<TokenProtocol> {
    TokenProtocol::ALL.to_vec()
}
const fn default_lifetime() -> u32 {
    86_400
}
const fn default_true() -> bool {
    true
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, m)
}

impl IssuerSpec {
    /// Checks the spec as a write would store it.
    ///
    /// # Errors
    /// 39998 naming the first problem: an empty or long issuer, a URL that
    /// isn't `https://` (or `http://` to a loopback address), no audience,
    /// an algorithm SprigDB doesn't verify (`none`, HMAC), no protocol, a
    /// lifetime of 0, `jit_users` on an `end_user_layer` issuer, a bad
    /// mapping (an empty claim or target, a value
    /// missing or present against its match kind, a regular expression that
    /// doesn't compile).
    pub fn validate(&self) -> Result<()> {
        if self.issuer.is_empty() || self.issuer.len() > 512 {
            return Err(invalid("issuer must be 1 to 512 characters"));
        }
        match &self.jwks_url {
            Some(u) => check_url(u).map_err(invalid)?,
            None => check_url(&self.issuer).map_err(|e| {
                invalid(format!(
                    "no jwks_url, so discovery reads the issuer's URL: {e}"
                ))
            })?,
        }
        if self.jwks_url.as_ref().is_some_and(|u| u.len() > 512) {
            return Err(invalid("jwks_url is longer than 512 characters"));
        }
        if self.audiences.is_empty() || self.audiences.iter().any(String::is_empty) {
            return Err(invalid("audiences: at least one, none empty"));
        }
        if self.algorithms.is_empty() {
            return Err(invalid("algorithms: at least one"));
        }
        for a in &self.algorithms {
            if Alg::parse(a).is_none() {
                return Err(invalid(format!(
                    "algorithm {a:?} isn't verified: use RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384 or EdDSA (never none or HMAC)"
                )));
            }
        }
        if self.protocols.is_empty() {
            return Err(invalid("protocols: at least one"));
        }
        if self.kind == IssuerKind::EndUserLayer && self.jit_users {
            return Err(invalid(
                "an end_user_layer issuer's tokens are end users': they log in as the service \
                 account a user mapping names, so jit_users must be false",
            ));
        }
        if self.max_lifetime_s == 0 {
            return Err(invalid("max_lifetime_s must be above 0"));
        }
        if self.client_id.as_ref().is_some_and(|c| c.len() > 255) {
            return Err(invalid("client_id is longer than 255 characters"));
        }
        for m in &self.mappings {
            if m.claim.is_empty() || m.claim.len() > 255 {
                return Err(invalid("a mapping's claim must be 1 to 255 characters"));
            }
            if m.target.is_empty() || m.target.len() > 255 {
                return Err(invalid("a mapping's target must be 1 to 255 characters"));
            }
            match (m.match_kind, &m.match_value) {
                (MatchKind::Any, Some(_)) => {
                    return Err(invalid("match_kind any takes no match_value"));
                }
                (MatchKind::Equals | MatchKind::Regex, None) => {
                    return Err(invalid("match_kind equals and regex need a match_value"));
                }
                (_, Some(v)) if v.len() > 512 => {
                    return Err(invalid("match_value is longer than 512 characters"));
                }
                (MatchKind::Regex, Some(v)) => {
                    super::mapping::compile(v).map_err(invalid)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A stored `oidc_issuers` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuerRow {
    /// The id.
    pub issuer_id: u32,
    /// OIDC provider or end-user layer.
    pub kind: IssuerKind,
    /// The exact `iss` value (unique).
    pub issuer: String,
    /// The key set's URL.
    pub jwks_url: Option<String>,
    /// Accepted `aud` values.
    pub audiences: Vec<String>,
    /// Accepted algorithms.
    pub algorithms: Vec<String>,
    /// The protocols, each once, in [`TokenProtocol::ALL`]'s order (as the
    /// `SET` column kept them).
    pub protocols: Vec<TokenProtocol>,
    /// `MONGODB-OIDC`'s client id.
    pub client_id: Option<String>,
    /// `MONGODB-OIDC`'s scopes.
    pub request_scopes: Option<Vec<String>>,
    /// Just-in-time accounts.
    pub jit_users: bool,
    /// The longest token lifetime accepted, seconds.
    pub max_lifetime_s: u32,
    /// `active` or `disabled`.
    pub state: String,
    /// The auth version of its last change.
    pub changed_version: u64,
}

impl IssuerRow {
    /// The spec it stores (no mappings).
    fn spec(&self) -> IssuerSpec {
        IssuerSpec {
            issuer_id: Some(self.issuer_id),
            kind: self.kind,
            issuer: self.issuer.clone(),
            jwks_url: self.jwks_url.clone(),
            audiences: self.audiences.clone(),
            algorithms: self.algorithms.clone(),
            protocols: self.protocols.clone(),
            client_id: self.client_id.clone(),
            request_scopes: self.request_scopes.clone(),
            jit_users: self.jit_users,
            max_lifetime_s: self.max_lifetime_s,
            active: self.state == "active",
            mappings: Vec::new(),
        }
    }
}

/// A stored `claim_mappings` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MappingRow {
    /// `mapping_id`.
    pub mapping_id: u32,
    /// Its issuer.
    pub issuer_id: u32,
    /// The mapping.
    pub mapping: ClaimMapping,
    /// The auth version of its last change.
    pub changed_version: u64,
}

/// The id counter of issuers.
const ISSUER_COUNTER: &str = "oidc_issuers";
/// The id counter of mappings.
const MAPPING_COUNTER: &str = "claim_mappings";

fn issuer_pk(id: u32) -> Pk {
    Pk::new().u32(id)
}

fn mapping_pk(issuer: u32, id: u32) -> Pk {
    Pk::new().u32(issuer).u32(id)
}

/// Whether two issuer URLs are the same key, as the column's `utf8mb4_bin`
/// unique key compared them: byte for byte, trailing spaces ignored.
fn same_issuer(a: &str, b: &str) -> bool {
    a.trim_end_matches(' ') == b.trim_end_matches(' ')
}

/// Every issuer with its mappings (in priority order), by issuer id.
///
/// # Errors
/// The store's; `Internal` for a malformed row.
pub async fn read_issuers(tx: &mut dyn RecordTx) -> Result<Vec<IssuerSpec>> {
    let rows: Vec<IssuerRow> = OIDC_ISSUERS.rows(tx, &Pk::new()).await?;
    let mut issuers: Vec<IssuerSpec> = rows.iter().map(IssuerRow::spec).collect();
    let mut maps: Vec<MappingRow> = CLAIM_MAPPINGS.rows(tx, &Pk::new()).await?;
    maps.sort_by_key(|m| (m.issuer_id, m.mapping.priority, m.mapping_id));
    for m in maps {
        if let Some(i) = issuers
            .iter_mut()
            .find(|i| i.issuer_id == Some(m.issuer_id))
        {
            i.mappings.push(m.mapping);
        }
    }
    Ok(issuers)
}

/// Whether any issuer or mapping changed after `since`.
///
/// # Errors
/// The store's; `Internal` for a malformed row.
pub async fn changed_since(tx: &mut dyn RecordTx, since: u64) -> Result<bool> {
    let issuers: Vec<IssuerRow> = OIDC_ISSUERS.rows(tx, &Pk::new()).await?;
    if issuers.iter().any(|i| i.changed_version > since) {
        return Ok(true);
    }
    let maps: Vec<MappingRow> = CLAIM_MAPPINGS.rows(tx, &Pk::new()).await?;
    Ok(maps.iter().any(|m| m.changed_version > since))
}

/// Nothing to do: the issuer tables are catalog records, which need no
/// migration. Kept for callers that still bring a tenant up to date first.
///
/// # Errors
/// None.
#[allow(clippy::unused_async)] // callers await it, as when it migrated
pub async fn migrate(_cat: &AuthCatalog, _tenant: TenantId) -> Result<()> {
    Ok(())
}

/// Creates or replaces (by `issuer`) an issuer and its mappings in one auth
/// change. Returns the issuer's id and the change's version.
///
/// # Errors
/// [`IssuerSpec::validate`]'s; the change's.
pub async fn put_issuer(
    cat: &AuthCatalog,
    tenant: TenantId,
    spec: &IssuerSpec,
) -> Result<(u32, u64)> {
    spec.validate()?;
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        let existing: Vec<IssuerRow> = OIDC_ISSUERS.rows(tx, &Pk::new()).await?;
        let id = if let Some(i) = existing
            .iter()
            .find(|i| same_issuer(&i.issuer, &spec.issuer))
        {
            i.issuer_id
        } else {
            let next = records::next_ids(tx, ISSUER_COUNTER, 1).await?;
            u32::try_from(next).map_err(|_| SprigError::internal("issuer ids exhausted"))?
        };
        let row = IssuerRow {
            issuer_id: id,
            kind: spec.kind,
            issuer: spec.issuer.clone(),
            jwks_url: spec.jwks_url.clone(),
            audiences: spec.audiences.clone(),
            algorithms: spec.algorithms.clone(),
            protocols: TokenProtocol::ALL
                .into_iter()
                .filter(|p| spec.protocols.contains(p))
                .collect(),
            client_id: spec.client_id.clone(),
            request_scopes: spec.request_scopes.clone(),
            jit_users: spec.jit_users,
            max_lifetime_s: spec.max_lifetime_s,
            state: if spec.active { "active" } else { "disabled" }.to_owned(),
            changed_version: version,
        };
        OIDC_ISSUERS.put(tx, &issuer_pk(id), &row).await?;
        for (k, _) in CLAIM_MAPPINGS
            .scan::<MappingRow>(tx, &Pk::new().u32(id))
            .await?
        {
            CLAIM_MAPPINGS.delete(tx, &k).await?;
        }
        if !spec.mappings.is_empty() {
            let n = u64::try_from(spec.mappings.len()).unwrap_or(u64::MAX);
            let first = records::next_ids(tx, MAPPING_COUNTER, n).await?;
            for (i, m) in spec.mappings.iter().enumerate() {
                let mapping_id = first
                    .checked_add(u64::try_from(i).unwrap_or(u64::MAX))
                    .and_then(|v| u32::try_from(v).ok())
                    .ok_or_else(|| SprigError::internal("claim mapping ids exhausted"))?;
                let row = MappingRow {
                    mapping_id,
                    issuer_id: id,
                    mapping: m.clone(),
                    changed_version: version,
                };
                CLAIM_MAPPINGS
                    .put(tx, &mapping_pk(id, mapping_id), &row)
                    .await?;
            }
        }
        Ok(id)
    }
    .await;
    finish(cat, tenant, c, r).await
}

async fn finish<T>(
    cat: &AuthCatalog,
    tenant: TenantId,
    c: AuthTx,
    r: Result<T>,
) -> Result<(T, u64)> {
    match r {
        Ok(v) => Ok((v, c.commit().await?)),
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

/// Adds or removes the `token` method of the live account
/// `'name'@'host'`: only accounts with it log in with an issuer's token
/// over MySQL and Redis (the token names the account over HTTP and
/// MongoDB, and is checked against it too). Returns the user's id and the
/// change's version.
///
/// # Errors
/// 1396 for an unknown account; the change's.
pub async fn set_token_method(
    cat: &AuthCatalog,
    tenant: TenantId,
    name: &str,
    host: &str,
    enabled: bool,
) -> Result<Changed> {
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        let store = AuthStore::new();
        let users = store.users_since(tx, None).await?;
        let Some(mut row) = users
            .into_iter()
            .find(|u| u.is_live() && u.name == name && host_eq(&u.host, host))
        else {
            return Err(SprigError::backend(
                1396,
                *b"HY000",
                format!("Operation ALTER USER failed for '{name}'@'{host}'"),
            ));
        };
        let mut m = AuthMethods::empty();
        for bit in [AuthMethods::PASSWORD, AuthMethods::API_KEY] {
            if row.auth_methods.contains(bit) {
                m = m.union(bit);
            }
        }
        if enabled {
            m = m.union(AuthMethods::TOKEN);
        }
        row.auth_methods = m;
        row.changed_version = version;
        store.update_user(tx, &row).await?;
        Ok(row.user_id)
    }
    .await;
    let (user_id, auth_version) = finish(cat, tenant, c, r).await?;
    Ok(Changed {
        user_id,
        auth_version,
    })
}

/// Creates `'name'@'%'` just in time for a verified token whose issuer has
/// `jit_users`: no password, only the `token` method, `source =
/// 'oidc_jit'`. A concurrent creation of the same account wins and is
/// returned as 1396.
///
/// # Errors
/// 39998 for a bad name; 1396 when the account appeared meanwhile; the
/// change's.
pub async fn create_jit_user(
    cat: &AuthCatalog,
    tenant: TenantId,
    name: &str,
    now: SystemTime,
) -> Result<Changed> {
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(invalid(format!(
            "a mapped user name must be 1 to {MAX_NAME} characters"
        )));
    }
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        let store = AuthStore::new();
        let id: UserId = store.next_user_id(tx).await?;
        let mut row = UserRow::new(id, name, "%", UserSource::OidcJit, now, version);
        row.auth_methods = AuthMethods::TOKEN;
        store.insert_user(tx, &row).await.map_err(|e| {
            if is_duplicate(&e) {
                SprigError::backend(
                    1396,
                    *b"HY000",
                    format!("Operation CREATE USER failed for '{name}'@'%'"),
                )
            } else {
                e
            }
        })?;
        Ok(id)
    }
    .await;
    let (user_id, auth_version) = finish(cat, tenant, c, r).await?;
    Ok(Changed {
        user_id,
        auth_version,
    })
}

#[cfg(test)]
mod tests;
