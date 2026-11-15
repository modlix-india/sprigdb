//! `oidc_issuers` and `claim_mappings`: the auth module's format 7, rows,
//! reads for the cache, and the writes the Admin API makes (each one auth
//! change, so every gateway's `TenantAuth` follows at once).

use std::time::SystemTime;

use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};
use serde::{Deserialize, Serialize};
use sprig_catalog::{ChangeOpts, MariaCatalog, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::names::quote;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};

use super::jwks::check_url;
use super::jwt::Alg;
use crate::admin::{Changed, MAX_NAME};
use crate::store::{AuthMethods, AuthStore, UserRow, UserSource, map_err};

/// The `oidc_issuers` table: one row per token issuer.
pub const DDL_OIDC_ISSUERS: &str = "CREATE TABLE IF NOT EXISTS oidc_issuers (
  issuer_id       INT UNSIGNED    NOT NULL,
  kind            ENUM('oidc','end_user_layer') NOT NULL DEFAULT 'oidc',
  issuer          VARCHAR(512)    NOT NULL,
  jwks_url        VARCHAR(512)    NULL,
  audiences       JSON            NOT NULL,
  algorithms      JSON            NOT NULL,
  protocols       SET('mysql','mongodb','redis','http') NOT NULL,
  client_id       VARCHAR(255)    NULL,
  request_scopes  JSON            NULL,
  jit_users       TINYINT(1)      NOT NULL DEFAULT 0,
  max_lifetime_s  INT UNSIGNED    NOT NULL DEFAULT 86400,
  state           ENUM('active','disabled') NOT NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (issuer_id),
  UNIQUE KEY uk_issuer (issuer)
) ENGINE=InnoDB";

/// `claim_mappings`.
pub const DDL_CLAIM_MAPPINGS: &str = "CREATE TABLE IF NOT EXISTS claim_mappings (
  mapping_id      INT UNSIGNED    NOT NULL,
  issuer_id       INT UNSIGNED    NOT NULL,
  priority        SMALLINT        NOT NULL,
  kind            ENUM('user','role') NOT NULL,
  claim           VARCHAR(255)    NOT NULL,
  match_kind      ENUM('any','equals','regex') NOT NULL,
  match_value     VARCHAR(512)    NULL,
  target          VARCHAR(255)    NOT NULL,
  changed_version BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (mapping_id),
  KEY k_issuer (issuer_id, priority)
) ENGINE=InnoDB";

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
    /// HTTP and gRPC: `Authorization: Bearer`.
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

    fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.as_str() == s)
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
    /// per application, and their claims reach row-level security.
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
    /// lifetime of 0, a bad mapping (an empty claim or target, a value
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

/// A stored issuer row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerRow {
    /// The spec (with `issuer_id`; mappings separate).
    pub spec: IssuerSpec,
    /// The auth version of its last change.
    pub changed_version: u64,
}

/// A stored mapping row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingRow {
    /// `mapping_id`.
    pub mapping_id: u32,
    /// Its issuer.
    pub issuer_id: u32,
    /// The mapping.
    pub mapping: ClaimMapping,
}

fn tables(store: &AuthStore) -> (String, String) {
    let q = quote(store.schema());
    (format!("{q}.oidc_issuers"), format!("{q}.claim_mappings"))
}

fn json_list(v: &[String]) -> Value {
    Value::from(serde_json::json!(v).to_string())
}

fn take_str(r: &mut Row, i: usize) -> Option<String> {
    match r.take::<Value, _>(i)? {
        Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into_owned()),
        Value::NULL => None,
        v => Some(mysql_async::from_value::<String>(v)),
    }
}

fn list_of(s: Option<String>) -> Vec<String> {
    s.and_then(|t| serde_json::from_str::<Vec<String>>(&t).ok())
        .unwrap_or_default()
}

const ISSUER_COLS: &str = "issuer_id, kind, issuer, jwks_url, audiences, algorithms, protocols, \
    client_id, request_scopes, jit_users, max_lifetime_s, state, changed_version";

fn issuer_row(mut r: Row) -> IssuerRow {
    let n = |r: &mut Row, i: usize| -> u64 { r.take::<Option<u64>, _>(i).flatten().unwrap_or(0) };
    let issuer_id = u32::try_from(n(&mut r, 0)).unwrap_or(0);
    let kind = match take_str(&mut r, 1).as_deref() {
        Some("end_user_layer") => IssuerKind::EndUserLayer,
        _ => IssuerKind::Oidc,
    };
    let issuer = take_str(&mut r, 2).unwrap_or_default();
    let jwks_url = take_str(&mut r, 3);
    let audiences = list_of(take_str(&mut r, 4));
    let algorithms = list_of(take_str(&mut r, 5));
    let protocols = take_str(&mut r, 6)
        .unwrap_or_default()
        .split(',')
        .filter_map(TokenProtocol::parse)
        .collect();
    let client_id = take_str(&mut r, 7);
    let request_scopes = take_str(&mut r, 8).map(|s| list_of(Some(s)));
    let jit_users = n(&mut r, 9) != 0;
    let max_lifetime_s = u32::try_from(n(&mut r, 10)).unwrap_or(u32::MAX);
    let active = take_str(&mut r, 11).as_deref() == Some("active");
    let changed_version = n(&mut r, 12);
    IssuerRow {
        spec: IssuerSpec {
            issuer_id: Some(issuer_id),
            kind,
            issuer,
            jwks_url,
            audiences,
            algorithms,
            protocols,
            client_id,
            request_scopes,
            jit_users,
            max_lifetime_s,
            active,
            mappings: Vec::new(),
        },
        changed_version,
    }
}

const MAPPING_COLS: &str =
    "mapping_id, issuer_id, priority, kind, claim, match_kind, match_value, target";

fn mapping_row(mut r: Row) -> MappingRow {
    let mapping_id = r.take::<Option<u32>, _>(0).flatten().unwrap_or(0);
    let issuer_id = r.take::<Option<u32>, _>(1).flatten().unwrap_or(0);
    let priority = r.take::<Option<i16>, _>(2).flatten().unwrap_or(0);
    let kind = match take_str(&mut r, 3).as_deref() {
        Some("role") => MappingKind::Role,
        _ => MappingKind::User,
    };
    let claim = take_str(&mut r, 4).unwrap_or_default();
    let match_kind = match take_str(&mut r, 5).as_deref() {
        Some("equals") => MatchKind::Equals,
        Some("regex") => MatchKind::Regex,
        _ => MatchKind::Any,
    };
    let match_value = take_str(&mut r, 6);
    let target = take_str(&mut r, 7).unwrap_or_default();
    MappingRow {
        mapping_id,
        issuer_id,
        mapping: ClaimMapping {
            priority,
            kind,
            claim,
            match_kind,
            match_value,
            target,
        },
    }
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

/// Every issuer with its mappings (in priority order); empty for a catalog
/// before format 7.
///
/// # Errors
/// A backend error.
pub async fn read_issuers<Q: Queryable + Send>(
    store: &AuthStore,
    q: &mut Q,
) -> Result<Vec<IssuerSpec>> {
    let (it, mt) = tables(store);
    let rows: Vec<Row> = match q
        .query(format!("SELECT {ISSUER_COLS} FROM {it} ORDER BY issuer_id"))
        .await
    {
        Ok(r) => r,
        Err(e) if missing_table(&e) => return Ok(Vec::new()),
        Err(e) => return Err(map_err(e)),
    };
    let mut issuers: Vec<IssuerSpec> = rows.into_iter().map(|r| issuer_row(r).spec).collect();
    let maps: Vec<Row> = match q
        .query(format!(
            "SELECT {MAPPING_COLS} FROM {mt} ORDER BY issuer_id, priority, mapping_id"
        ))
        .await
    {
        Ok(r) => r,
        Err(e) if missing_table(&e) => Vec::new(),
        Err(e) => return Err(map_err(e)),
    };
    for m in maps.into_iter().map(mapping_row) {
        if let Some(i) = issuers
            .iter_mut()
            .find(|i| i.issuer_id == Some(m.issuer_id))
        {
            i.mappings.push(m.mapping);
        }
    }
    Ok(issuers)
}

/// Whether any issuer or mapping changed after `since`; `false` for a
/// catalog before format 7.
///
/// # Errors
/// A backend error.
pub async fn changed_since<Q: Queryable + Send>(
    store: &AuthStore,
    q: &mut Q,
    since: u64,
) -> Result<bool> {
    let (it, mt) = tables(store);
    let sql = format!(
        "SELECT (SELECT COUNT(*) FROM {it} WHERE changed_version > ?) + \
         (SELECT COUNT(*) FROM {mt} WHERE changed_version > ?)"
    );
    match q.exec_first::<u64, _, _>(sql, (since, since)).await {
        Ok(n) => Ok(n.unwrap_or(0) > 0),
        Err(e) if missing_table(&e) => Ok(false),
        Err(e) => Err(map_err(e)),
    }
}

/// Brings `tenant`'s auth module to format 7 (outside any change: the
/// migration is DDL).
///
/// # Errors
/// The migrator's.
pub async fn migrate(cat: &MariaCatalog, tenant: TenantId) -> Result<()> {
    let ep = cat.backend().locator().writer(tenant).await?;
    let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
    sprig_catalog::Migrator::new(&[crate::store::AUTH_MODULE])?
        .run(
            &mut conn,
            ep.store.schema(),
            &sprig_catalog::migrate::MigrateOpts::default(),
        )
        .await?;
    Ok(())
}

/// Creates or replaces (by `issuer`) an issuer and its mappings in one auth
/// change. Returns the issuer's id and the change's version.
///
/// # Errors
/// [`IssuerSpec::validate`]'s; 39083 when the catalog has no issuer tables
/// yet (call [`migrate`] first); the change's.
pub async fn put_issuer(
    cat: &MariaCatalog,
    tenant: TenantId,
    spec: &IssuerSpec,
) -> Result<(u32, u64)> {
    spec.validate()?;
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let (it, mt) = tables(&store);
        let existing: Option<u32> = q
            .exec_first(
                format!("SELECT issuer_id FROM {it} WHERE issuer = ? FOR UPDATE"),
                (spec.issuer.as_str(),),
            )
            .await
            .map_err(map_err)?;
        let id = match existing {
            Some(id) => id,
            None => q
                .query_first::<u32, _>(format!(
                    "SELECT COALESCE(MAX(issuer_id), 0) + 1 FROM {it} FOR UPDATE"
                ))
                .await
                .map_err(map_err)?
                .unwrap_or(1),
        };
        let protocols: Vec<&str> = spec.protocols.iter().map(|p| p.as_str()).collect();
        let params = vec![
            Value::from(id),
            Value::from(spec.kind.as_str()),
            Value::from(spec.issuer.as_str()),
            spec.jwks_url.as_deref().map_or(Value::NULL, Value::from),
            json_list(&spec.audiences),
            json_list(&spec.algorithms),
            Value::from(protocols.join(",")),
            spec.client_id.as_deref().map_or(Value::NULL, Value::from),
            spec.request_scopes
                .as_deref()
                .map_or(Value::NULL, json_list),
            Value::from(u8::from(spec.jit_users)),
            Value::from(spec.max_lifetime_s),
            Value::from(if spec.active { "active" } else { "disabled" }),
            Value::from(version),
        ];
        q.exec_drop(
            format!(
                "REPLACE INTO {it} ({ISSUER_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            params,
        )
        .await
        .map_err(map_err)?;
        q.exec_drop(format!("DELETE FROM {mt} WHERE issuer_id = ?"), (id,))
            .await
            .map_err(map_err)?;
        let mut next: u32 = q
            .query_first::<u32, _>(format!(
                "SELECT COALESCE(MAX(mapping_id), 0) + 1 FROM {mt} FOR UPDATE"
            ))
            .await
            .map_err(map_err)?
            .unwrap_or(1);
        for m in &spec.mappings {
            q.exec_drop(
                format!(
                    "INSERT INTO {mt} ({MAPPING_COLS}, changed_version) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ),
                vec![
                    Value::from(next),
                    Value::from(id),
                    Value::from(m.priority),
                    Value::from(match m.kind {
                        MappingKind::User => "user",
                        MappingKind::Role => "role",
                    }),
                    Value::from(m.claim.as_str()),
                    Value::from(match m.match_kind {
                        MatchKind::Any => "any",
                        MatchKind::Equals => "equals",
                        MatchKind::Regex => "regex",
                    }),
                    m.match_value.as_deref().map_or(Value::NULL, Value::from),
                    Value::from(m.target.as_str()),
                    Value::from(version),
                ],
            )
            .await
            .map_err(map_err)?;
            next += 1;
        }
        Ok(id)
    }
    .await;
    finish(cat, tenant, c, r).await
}

async fn finish<T>(
    cat: &MariaCatalog,
    tenant: TenantId,
    c: sprig_catalog::CatalogTx,
    r: Result<T>,
) -> Result<(T, u64)> {
    match r {
        Ok(v) => Ok((v, c.commit().await?)),
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            if matches!(e.kind, ErrorKind::Backend { errno: 1146, .. }) {
                return Err(SprigError::new(
                    SprigCode::TenantNotReady,
                    "the tenant's catalog has no issuer tables yet: migrate its auth module",
                ));
            }
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
    cat: &MariaCatalog,
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
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let users = store.users_since(q, None).await?;
        let Some(mut row) = users
            .into_iter()
            .find(|u| u.is_live() && u.name == name && u.host.eq_ignore_ascii_case(host))
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
        store.update_user(q, &row).await?;
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
    cat: &MariaCatalog,
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
        let (core, q) = c.tx().parts();
        let store = AuthStore::of(core);
        let id: UserId = store.next_user_id(core, q).await?;
        let mut row = UserRow::new(id, name, "%", UserSource::OidcJit, now, version);
        row.auth_methods = AuthMethods::TOKEN;
        store.insert_user(q, &row).await.map_err(|e| {
            if matches!(e.kind, ErrorKind::Backend { errno: 1062, .. }) {
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
