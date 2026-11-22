//! API keys for HTTP on 8080 and 8443.
//!
//! A key is `sprig_<tenant>_<key_id>_<secret>`: the tenant name (a visible
//! prefix for secret scanners), a 12-character base32 id, and 256 bits of
//! secret as 64 hex digits. The catalog keeps only `SHA256(secret)`
//! (`api_keys.secret_sha256`); the secret is shown once, when the key is
//! created ([`create_api_key`]).
//!
//! `api_keys` rows are catalog records (`m/auth/api_keys/<key id>`),
//! versioned by the auth version (`changed_version`), so creating or
//! revoking a key is an auth change and every gateway's cache follows it.
//! A revoked key's row stays (revoked), never deleted.
//!
//! Verification ([`crate::http`]): the key's row in the cached
//! [`TenantAuth`](crate::cache::TenantAuth), `SHA256(secret)` compared in
//! constant time, not revoked, not expired, the client inside
//! `allowed_cidrs` when set, and the user live with the `api_key` method,
//! then the user's statuses as for a token (temporary lock, `ACCOUNT
//! LOCK`, TLS requirement, suspension). `scopes` limit what the key may do
//! on top of the user's grants; the authorization layer applies them.

use std::fmt::Write as _;
use std::net::IpAddr;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sprig_catalog::records::{Pk, RecordTx};
use sprig_catalog::{AuthCatalog, ChangeOpts, VersionKind};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use subtle::ConstantTimeEq as _;
use zeroize::{Zeroize as _, Zeroizing};

use crate::account::HostPattern;
use crate::rec::{API_KEYS, duplicate, to_micros};
use crate::store::{AuthMethods, AuthStore};

/// The visible id's length.
pub const KEY_ID_LEN: usize = 12;
/// The secret's length in bytes (256 bits).
pub const SECRET_LEN: usize = 32;
const BASE32: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// An `api_keys` row. `Debug` hides the secret's hash.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyRow {
    /// The visible id.
    pub key_id: String,
    /// The user the key logs in as.
    pub user_id: UserId,
    /// A name for people.
    pub name: String,
    /// `SHA256(secret)`.
    pub secret_sha256: [u8; 32],
    /// Limits on top of the user's grants, as stored.
    pub scopes: serde_json::Value,
    /// Client addresses allowed, as host patterns (`10.0.0.0/8`); `None`
    /// for any.
    pub allowed_cidrs: Option<Vec<String>>,
    /// When it stops working.
    #[serde(with = "crate::rec::micros_opt")]
    pub expires_at: Option<SystemTime>,
    /// When it was revoked.
    #[serde(with = "crate::rec::micros_opt")]
    pub revoked_at: Option<SystemTime>,
    /// Who created it.
    pub created_by: Option<UserId>,
    /// When.
    #[serde(with = "crate::rec::micros")]
    pub created_at: SystemTime,
    /// The auth version of its last change.
    pub changed_version: u64,
}

impl std::fmt::Debug for ApiKeyRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyRow")
            .field("key_id", &self.key_id)
            .field("user_id", &self.user_id)
            .field("name", &self.name)
            .field("revoked_at", &self.revoked_at)
            .finish_non_exhaustive()
    }
}

/// A key as the cache keeps it, with its parsed address patterns.
#[derive(Debug, Clone, PartialEq)]
pub struct ApiKey {
    /// The row.
    pub row: ApiKeyRow,
    /// `allowed_cidrs`, parsed.
    pub allowed: Option<Vec<HostPattern>>,
}

impl ApiKey {
    /// The cached form of `row`.
    #[must_use]
    pub fn new(row: ApiKeyRow) -> Self {
        let allowed = row
            .allowed_cidrs
            .as_ref()
            .map(|v| v.iter().map(|c| HostPattern::parse(c)).collect());
        Self { row, allowed }
    }

    /// Whether `secret` is this key's, compared in constant time.
    #[must_use]
    pub fn secret_matches(&self, secret: &[u8]) -> bool {
        let h: [u8; 32] = Sha256::digest(secret).into();
        bool::from(h.ct_eq(&self.row.secret_sha256))
    }

    /// Why the key can't be used now from `ip`, if it can't.
    #[must_use]
    pub fn unusable(&self, ip: IpAddr, now: SystemTime) -> Option<&'static str> {
        if self.row.revoked_at.is_some() {
            return Some("key_revoked");
        }
        if self.row.expires_at.is_some_and(|e| e <= now) {
            return Some("key_expired");
        }
        if self
            .allowed
            .as_ref()
            .is_some_and(|a| !a.iter().any(|p| p.matches(ip)))
        {
            return Some("key_address");
        }
        None
    }
}

/// A presented key, split.
#[derive(PartialEq, Eq)]
pub struct PresentedKey {
    /// The tenant name.
    pub tenant: String,
    /// The visible id.
    pub key_id: String,
    /// The secret's text (64 hex digits).
    pub secret: Zeroizing<String>,
}

impl std::fmt::Debug for PresentedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresentedKey")
            .field("tenant", &self.tenant)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// Splits `sprig_<tenant>_<key_id>_<secret>`; `None` if it isn't one.
#[must_use]
pub fn parse_key(s: &str) -> Option<PresentedKey> {
    let rest = s.strip_prefix("sprig_")?;
    let (tenant, rest) = rest.split_once('_')?;
    let (key_id, secret) = rest.split_once('_')?;
    let tenant_ok = sprig_common::names::is_tenant_name(tenant);
    let id_ok = key_id.len() == KEY_ID_LEN && key_id.bytes().all(|b| BASE32.contains(&b));
    let secret_ok = secret.len() == SECRET_LEN * 2 && secret.bytes().all(|b| b.is_ascii_hexdigit());
    (tenant_ok && id_ok && secret_ok).then(|| PresentedKey {
        tenant: tenant.to_owned(),
        key_id: key_id.to_owned(),
        secret: Zeroizing::new(secret.to_owned()),
    })
}

/// A new key's parts: the visible id, the secret's text and the full key.
fn generate(tenant: &str) -> Result<(String, Zeroizing<String>, Zeroizing<String>)> {
    let mut raw = [0_u8; KEY_ID_LEN + SECRET_LEN];
    getrandom::fill(&mut raw)
        .map_err(|e| SprigError::internal(format!("no randomness for an API key: {e}")))?;
    let key_id: String = raw[..KEY_ID_LEN]
        .iter()
        .map(|b| BASE32[usize::from(b & 31)] as char)
        .collect();
    let mut secret = Zeroizing::new(String::with_capacity(SECRET_LEN * 2));
    for b in &raw[KEY_ID_LEN..] {
        let _ = write!(secret, "{b:02x}");
    }
    raw.zeroize();
    let full = Zeroizing::new(format!("sprig_{tenant}_{key_id}_{}", secret.as_str()));
    Ok((key_id, secret, full))
}

// ---- store ----

/// The primary key of key `key_id`.
fn key_pk(key_id: &str) -> Pk {
    Pk::new().bytes(key_id.as_bytes())
}

/// Keys changed after `since` (every key with `None`), by key id.
///
/// # Errors
/// The store's; `Internal` for a malformed row.
pub async fn keys_since(tx: &mut dyn RecordTx, since: Option<u64>) -> Result<Vec<ApiKeyRow>> {
    let all: Vec<ApiKeyRow> = API_KEYS.rows(tx, &Pk::new()).await?;
    Ok(match since {
        Some(v) => all.into_iter().filter(|k| k.changed_version > v).collect(),
        None => all,
    })
}

/// Inserts a key; a key id in use is a duplicate key (1062).
async fn insert(tx: &mut dyn RecordTx, r: &ApiKeyRow) -> Result<()> {
    let pk = key_pk(&r.key_id);
    if API_KEYS.get::<ApiKeyRow>(tx, &pk).await?.is_some() {
        return Err(duplicate("api key id"));
    }
    let mut row = r.clone();
    row.expires_at = row.expires_at.map(to_micros);
    row.revoked_at = None;
    row.created_at = to_micros(row.created_at);
    API_KEYS.put(tx, &pk, &row).await
}

// ---- admin ----

/// A key to create.
#[derive(Debug, Clone, Default)]
pub struct NewApiKey {
    /// A name for people.
    pub name: String,
    /// Scopes on top of the user's grants; `[]` for none.
    pub scopes: Vec<String>,
    /// Client addresses allowed (host patterns, `10.0.0.0/8`); empty for
    /// any.
    pub allowed_cidrs: Vec<String>,
    /// When it stops working.
    pub expires_at: Option<SystemTime>,
    /// Who creates it.
    pub created_by: Option<UserId>,
}

/// A created key: the only time its secret is visible.
pub struct CreatedKey {
    /// The visible id.
    pub key_id: String,
    /// The whole key to hand to the client, `sprig_<tenant>_<id>_<secret>`.
    pub key: Zeroizing<String>,
    /// The auth version it committed at.
    pub auth_version: u64,
}

impl std::fmt::Debug for CreatedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedKey")
            .field("key_id", &self.key_id)
            .field("auth_version", &self.auth_version)
            .finish_non_exhaustive()
    }
}

/// Creates an API key for live user `user` of `tenant` (named
/// `tenant_name`, the key's prefix). The user needs the `api_key` method
/// (`users.auth_methods`); this call adds it, in the same auth change.
///
/// # Errors
/// 1396 if the user doesn't exist; 39998 for a bad name; the change's.
pub async fn create_api_key(
    cat: &AuthCatalog,
    tenant: TenantId,
    tenant_name: &str,
    user: UserId,
    new: &NewApiKey,
    now: SystemTime,
) -> Result<CreatedKey> {
    if new.name.is_empty() || new.name.chars().count() > 128 {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            "an API key's name must be 1 to 128 characters",
        ));
    }
    let (key_id, secret, full) = generate(tenant_name)?;
    let mut row = ApiKeyRow {
        key_id: key_id.clone(),
        user_id: user,
        name: new.name.clone(),
        secret_sha256: Sha256::digest(secret.as_bytes()).into(),
        scopes: serde_json::json!(new.scopes),
        allowed_cidrs: (!new.allowed_cidrs.is_empty()).then(|| new.allowed_cidrs.clone()),
        expires_at: new.expires_at,
        revoked_at: None,
        created_by: new.created_by,
        created_at: now,
        changed_version: 0,
    };
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        let store = AuthStore::new();
        let mut urow = match store.user(tx, user).await? {
            Some(r) if r.is_live() => r,
            _ => {
                return Err(SprigError::backend(
                    1396,
                    *b"HY000",
                    format!("Operation CREATE API KEY failed for '#{user}'@'%'"),
                ));
            }
        };
        row.changed_version = version;
        insert(tx, &row).await?;
        urow.auth_methods = urow.auth_methods.union(AuthMethods::API_KEY);
        urow.changed_version = version;
        store.update_user(tx, &urow).await?;
        Ok(())
    }
    .await;
    let version = match r {
        Ok(()) => c.commit().await?,
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            return Err(e);
        }
    };
    Ok(CreatedKey {
        key_id,
        key: full,
        auth_version: version,
    })
}

/// Revokes key `key_id` of `tenant` (the row stays, revoked). Returns
/// `false` if there is no such live key.
///
/// # Errors
/// The change's errors.
pub async fn revoke_api_key(
    cat: &AuthCatalog,
    tenant: TenantId,
    key_id: &str,
    now: SystemTime,
) -> Result<bool> {
    let mut c = cat
        .change_with(tenant, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let version = c.version();
    let r = async {
        let tx = c.tx().records();
        let pk = key_pk(key_id);
        let Some(mut row) = API_KEYS.get_for_update::<ApiKeyRow>(tx, &pk).await? else {
            return Ok(false);
        };
        if row.revoked_at.is_some() {
            return Ok(false);
        }
        row.revoked_at = Some(to_micros(now));
        row.changed_version = version;
        API_KEYS.put(tx, &pk, &row).await?;
        Ok(true)
    }
    .await;
    match r {
        Ok(true) => {
            c.commit().await?;
            Ok(true)
        }
        Ok(false) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Ok(false)
        }
        Err(e) => {
            let _ = c.rollback().await;
            let _ = cat.settle(tenant, VersionKind::Auth).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_catalog::records::RecordStore as _;

    use super::*;
    use crate::token::sessions::tests::{T, add_user, at, auth_version, catalog};

    fn new_key(name: &str) -> NewApiKey {
        NewApiKey {
            name: name.into(),
            scopes: vec!["read".into()],
            allowed_cidrs: vec!["10.0.0.0/8".into()],
            expires_at: Some(at(3600)),
            created_by: Some(UserId(1)),
        }
    }

    async fn all_keys(
        recs: &sprig_catalog::records::mem::MemRecords,
        since: Option<u64>,
    ) -> Vec<ApiKeyRow> {
        let mut tx = recs.begin(T).await.unwrap();
        keys_since(&mut *tx, since).await.unwrap()
    }

    #[tokio::test]
    async fn a_key_and_its_users_api_key_method_are_one_auth_change() {
        let (recs, cat) = catalog().await;
        add_user(&cat, 3, "ci").await;
        let created = create_api_key(&cat, T, "acme", UserId(3), &new_key("ci"), at(5))
            .await
            .unwrap();
        assert_eq!(auth_version(&recs).await, created.auth_version);
        let p = parse_key(&created.key).unwrap();
        assert_eq!(p.key_id, created.key_id);
        let keys = all_keys(&recs, None).await;
        assert_eq!(keys.len(), 1);
        let k = &keys[0];
        assert_eq!(k.key_id, created.key_id);
        assert_eq!(k.changed_version, created.auth_version);
        assert_eq!(k.created_at, crate::rec::to_micros(at(5)));
        assert_eq!(k.expires_at, Some(crate::rec::to_micros(at(3600))));
        assert_eq!(k.scopes, serde_json::json!(["read"]));
        assert_eq!(k.allowed_cidrs, Some(vec!["10.0.0.0/8".to_owned()]));
        assert!(ApiKey::new(k.clone()).secret_matches(p.secret.as_bytes()));
        let mut tx = recs.begin(T).await.unwrap();
        let u = AuthStore::new()
            .user(&mut *tx, UserId(3))
            .await
            .unwrap()
            .unwrap();
        assert!(u.auth_methods.contains(AuthMethods::API_KEY));
        assert!(u.auth_methods.contains(AuthMethods::PASSWORD));
        assert_eq!(u.changed_version, created.auth_version);
    }

    #[tokio::test]
    async fn a_key_for_no_live_user_is_1396_and_leaves_nothing() {
        let (recs, cat) = catalog().await;
        let e = create_api_key(&cat, T, "acme", UserId(3), &new_key("ci"), at(5))
            .await
            .unwrap_err();
        assert!(matches!(
            e.kind,
            sprig_common::ErrorKind::Backend { errno: 1396, .. }
        ));
        assert!(all_keys(&recs, None).await.is_empty());
        // A refused change rolls back and settles the version it was
        // given (a closed gap): no row moves to it.
        let e = create_api_key(&cat, T, "acme", UserId(3), &new_key(""), at(5))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::ConfigInvalid));
    }

    #[tokio::test]
    async fn a_key_id_in_use_is_a_duplicate() {
        let (recs, _cat) = catalog().await;
        let row = ApiKeyRow {
            key_id: "a".repeat(12),
            user_id: UserId(3),
            name: "ci".into(),
            secret_sha256: [0; 32],
            scopes: serde_json::json!([]),
            allowed_cidrs: None,
            expires_at: None,
            revoked_at: None,
            created_by: None,
            created_at: at(0),
            changed_version: 1,
        };
        let mut tx = recs.begin(T).await.unwrap();
        insert(&mut *tx, &row).await.unwrap();
        let e = insert(&mut *tx, &row).await.unwrap_err();
        assert!(crate::rec::is_duplicate(&e));
    }

    #[tokio::test]
    async fn a_revoke_is_soft_and_happens_once() {
        let (recs, cat) = catalog().await;
        add_user(&cat, 3, "ci").await;
        let a = create_api_key(&cat, T, "acme", UserId(3), &new_key("a"), at(5))
            .await
            .unwrap();
        let b = create_api_key(&cat, T, "acme", UserId(3), &new_key("b"), at(5))
            .await
            .unwrap();
        assert!(revoke_api_key(&cat, T, &a.key_id, at(9)).await.unwrap());
        let v = auth_version(&recs).await;
        assert!(v > b.auth_version);
        // Again, or an unknown key: false, and no version is used.
        assert!(!revoke_api_key(&cat, T, &a.key_id, at(10)).await.unwrap());
        assert!(
            !revoke_api_key(&cat, T, "zzzzzzzzzzzz", at(10))
                .await
                .unwrap()
        );
        // A refused change rolls back and settles the version it was
        // given (a closed gap): no row moves to it.
        // The row stays, revoked; the incremental load brings it alone.
        let since = all_keys(&recs, Some(b.auth_version)).await;
        assert_eq!(since.len(), 1);
        assert_eq!(since[0].key_id, a.key_id);
        assert_eq!(since[0].revoked_at, Some(crate::rec::to_micros(at(9))));
        assert_eq!(since[0].changed_version, v);
        // The full load: every key, by key id.
        let all = all_keys(&recs, None).await;
        let mut ids = vec![a.key_id.clone(), b.key_id.clone()];
        ids.sort();
        assert_eq!(
            all.iter().map(|k| k.key_id.clone()).collect::<Vec<_>>(),
            ids
        );
        assert!(all_keys(&recs, Some(v)).await.is_empty());
    }

    #[test]
    fn generated_keys_parse_back() {
        let (id, secret, full) = generate("acme-eu").unwrap();
        let p = parse_key(&full).unwrap();
        assert_eq!(p.tenant, "acme-eu");
        assert_eq!(p.key_id, id);
        assert_eq!(*p.secret, *secret);
        assert_eq!(full.len(), "sprig_acme-eu_".len() + 12 + 1 + 64);
        let (id2, _, _) = generate("acme-eu").unwrap();
        assert_ne!(id, id2);
    }

    #[test]
    fn malformed_keys_are_refused() {
        let good = format!("sprig_acme_{}_{}", "a".repeat(12), "0".repeat(64));
        assert!(parse_key(&good).is_some());
        for bad in [
            format!("sprig_acme_{}_{}", "a".repeat(11), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "a".repeat(12), "0".repeat(63)),
            format!("sprig_Acme_{}_{}", "a".repeat(12), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "A".repeat(12), "0".repeat(64)),
            format!("sprig_acme_{}_{}", "a".repeat(12), "g".repeat(64)),
            format!("token_acme_{}_{}", "a".repeat(12), "0".repeat(64)),
            "sprig_acme".to_owned(),
        ] {
            assert!(parse_key(&bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn usability_checks() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let secret = "0".repeat(64);
        let row = ApiKeyRow {
            key_id: "a".repeat(12),
            user_id: UserId(1),
            name: "ci".into(),
            secret_sha256: Sha256::digest(secret.as_bytes()).into(),
            scopes: serde_json::json!([]),
            allowed_cidrs: Some(vec!["10.0.0.0/8".into()]),
            expires_at: Some(now + std::time::Duration::from_secs(60)),
            revoked_at: None,
            created_by: None,
            created_at: now,
            changed_version: 1,
        };
        let k = ApiKey::new(row.clone());
        assert!(k.secret_matches(secret.as_bytes()));
        assert!(!k.secret_matches(b"nope"));
        let inside: IpAddr = "10.1.2.3".parse().unwrap();
        let outside: IpAddr = "192.0.2.1".parse().unwrap();
        assert_eq!(k.unusable(inside, now), None);
        assert_eq!(k.unusable(outside, now), Some("key_address"));
        assert_eq!(
            k.unusable(inside, now + std::time::Duration::from_secs(60)),
            Some("key_expired")
        );
        let mut r = row;
        r.revoked_at = Some(now);
        assert_eq!(ApiKey::new(r).unusable(inside, now), Some("key_revoked"));
    }
}
