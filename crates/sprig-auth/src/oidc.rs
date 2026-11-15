//! External tokens: JWTs from a tenant's registered issuers (OIDC
//! providers, and the pluggable end-user layer), verified against the
//! tenant's `oidc_issuers` and mapped to an account by its
//! `claim_mappings`.
//!
//! ```text
//! verify(token, tenant's issuers, protocol):
//!   parse (compact JWS, at most 16 KiB)                       else malformed
//!   iss → an active issuer of the tenant taking this protocol  else unknown issuer
//!   alg in the issuer's allow-list (never none or HMAC)        else algorithm
//!   JWKS key by kid (refetched at most once a minute)          else unknown key
//!   signature                                                  else signature
//!   exp (required), nbf, iat with auth.token_clock_skew_seconds else expired / not yet valid
//!   aud ∩ the issuer's audiences                               else audience
//!   exp - iat (or exp - now) ≤ max_lifetime_s                  else lifetime
//!   mappings → the account name and token roles; the claims, verified
//! ```
//!
//! Every refusal is one generic credential error to the client (1045, 401,
//! `AuthenticationFailed`); the reason goes to the audit event's detail.
//! The protocols ([`TokenProtocol`]): MySQL and MariaDB take the token as
//! the clear-text password of an account with the `token` method (the
//! login name names the account, and the token must map to it); Redis the
//! same in `AUTH`; HTTP and gRPC as `Authorization: Bearer`, where the
//! mapping names the account (created when the issuer has `jit_users`);
//! MongoDB as `MONGODB-OIDC`, like HTTP.
//!
//! The verified claims become the session's `SPRIG_CLAIM()` values
//! (chapter 29), unchanged.

pub mod jwks;
pub mod jwt;
pub mod login;
pub mod mapping;
pub mod store;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use sha2::{Digest, Sha256};
use sprig_common::ids::TenantId;
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

pub use self::jwks::{Fetcher, HttpFetcher, JwksCache, Source};
pub use self::jwt::{Alg, Jws};
pub use self::login::{TokenDenied, TokenLogin};
pub use self::mapping::Mapped;
pub use self::store::{
    ClaimMapping, IssuerKind, IssuerSpec, MappingKind, MatchKind, TokenProtocol,
};
use self::mapping::Mapping;

/// `auth.token_clock_skew_seconds`: clock skew allowed on token times.
pub static TOKEN_CLOCK_SKEW_SECONDS: Setting<u64> = Setting {
    key: "auth.token_clock_skew_seconds",
    default: 60,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 600),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Clock skew allowed on external tokens' exp, nbf and iat",
};
sprig_settings::register!(TOKEN_CLOCK_SKEW_SECONDS);

/// `auth.jwks_refresh_seconds`: how often an issuer's key set is
/// refreshed; a failing issuer's cached keys serve twice as long.
pub static JWKS_REFRESH_SECONDS: Setting<u64> = Setting {
    key: "auth.jwks_refresh_seconds",
    default: 3600,
    scopes: Scopes::TREE,
    hard: HardBounds::range(60, 604_800),
    apply: Apply::Live,
    operator_only: false,
    owner: 14,
    doc: "Refresh period of issuers' key sets; cached keys serve twice as long while the issuer fails",
};
sprig_settings::register!(JWKS_REFRESH_SECONDS);

/// A registered issuer, ready to verify.
#[derive(Debug, Clone)]
pub struct Issuer {
    /// As stored, with its mappings.
    pub spec: IssuerSpec,
    algs: Vec<Alg>,
    mappings: Vec<Mapping>,
    source: Source,
}

impl Issuer {
    /// Prepares `spec`. Unknown algorithms and mappings whose expression
    /// doesn't compile are left out (both are refused when written).
    #[must_use]
    pub fn new(spec: IssuerSpec) -> Self {
        let algs = spec.algorithms.iter().filter_map(|a| Alg::parse(a)).collect();
        let mappings = spec
            .mappings
            .iter()
            .cloned()
            .filter_map(Mapping::new)
            .collect();
        let source = match &spec.jwks_url {
            Some(u) => Source::Url(u.clone()),
            None => Source::Discovery(spec.issuer.clone()),
        };
        Self {
            spec,
            algs,
            mappings,
            source,
        }
    }

    /// Whether the issuer is active and takes `protocol`.
    #[must_use]
    pub fn serves(&self, protocol: TokenProtocol) -> bool {
        self.spec.active && self.spec.protocols.contains(&protocol)
    }
}

/// Why a token was refused (the audit detail; clients see one generic
/// error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// Not a compact JWS, too long, no `exp`, claims not an object.
    Malformed(String),
    /// No active issuer of the tenant has this `iss` for this protocol.
    UnknownIssuer,
    /// `alg` isn't in the issuer's allow-list.
    Algorithm,
    /// The issuer's key set can't be fetched and no usable copy is cached.
    KeysUnavailable(String),
    /// No key with the token's `kid` (after one refetch).
    UnknownKey,
    /// The signature doesn't verify.
    Signature,
    /// `exp` passed (beyond the skew).
    Expired,
    /// `nbf` or `iat` is in the future (beyond the skew).
    NotYetValid,
    /// No `aud` is one of the issuer's audiences.
    Audience,
    /// The token lives longer than the issuer's `max_lifetime_s`.
    Lifetime,
    /// No user mapping matched, or it names another account than the
    /// login's.
    NoUser,
    /// The mapped account doesn't exist and the issuer has no `jit_users`,
    /// or it lacks the `token` method.
    NoAccount,
}

impl Refusal {
    /// The audit detail.
    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Refusal::Malformed(m) => format!("token: malformed ({m})"),
            Refusal::UnknownIssuer => "token: unknown issuer".into(),
            Refusal::Algorithm => "token: algorithm not allowed".into(),
            Refusal::KeysUnavailable(m) => format!("token: issuer keys unavailable ({m})"),
            Refusal::UnknownKey => "token: unknown key id".into(),
            Refusal::Signature => "token: bad signature".into(),
            Refusal::Expired => "token: expired".into(),
            Refusal::NotYetValid => "token: not yet valid".into(),
            Refusal::Audience => "token: wrong audience".into(),
            Refusal::Lifetime => "token: lifetime above the issuer's maximum".into(),
            Refusal::NoUser => "token: no claim mapping names this account".into(),
            Refusal::NoAccount => "token: the mapped account doesn't take tokens".into(),
        }
    }

    /// A short label for metrics.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Refusal::Malformed(_) => "malformed",
            Refusal::UnknownIssuer => "issuer",
            Refusal::Algorithm => "algorithm",
            Refusal::KeysUnavailable(_) => "keys_unavailable",
            Refusal::UnknownKey => "unknown_key",
            Refusal::Signature => "signature",
            Refusal::Expired => "expired",
            Refusal::NotYetValid => "not_yet_valid",
            Refusal::Audience => "audience",
            Refusal::Lifetime => "lifetime",
            Refusal::NoUser => "no_user",
            Refusal::NoAccount => "no_account",
        }
    }
}

/// A verified token.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedToken {
    /// The issuer's id.
    pub issuer_id: u32,
    /// The issuer (`iss`).
    pub issuer: String,
    /// OIDC provider or end-user layer.
    pub kind: IssuerKind,
    /// Whether the issuer creates mapped accounts that don't exist.
    pub jit_users: bool,
    /// Every claim, as verified: the session's `SPRIG_CLAIM()` values.
    pub claims: Arc<Value>,
    /// The account name the user mappings give.
    pub user: Option<String>,
    /// The roles the role mappings add (`token_roles`).
    pub roles: Vec<String>,
    /// `exp`.
    pub expires: SystemTime,
}

/// What a verification runs against.
#[derive(Debug, Clone, Copy)]
pub struct Check<'a> {
    /// The tenant's issuers.
    pub issuers: &'a [Arc<Issuer>],
    /// The protocol the token arrived over.
    pub protocol: TokenProtocol,
    /// `auth.token_clock_skew_seconds`.
    pub skew: Duration,
    /// `auth.jwks_refresh_seconds`.
    pub jwks_refresh: Duration,
    /// The time of the check.
    pub now: SystemTime,
    /// The tenant and auth version, for the verified-token cache; `None`
    /// skips it.
    pub cache: Option<(TenantId, u64)>,
}

/// How long a verified token is reused without checking it again.
pub const TOKEN_CACHE_TTL: Duration = Duration::from_secs(60);
/// Verified tokens kept at most.
const TOKEN_CACHE_MAX: usize = 10_000;

struct Cached {
    token: VerifiedToken,
    until: Instant,
}

/// External-token verification for one process: the key-set cache and
/// the verified-token cache (by SHA-256, until the earlier of `exp` and
/// 60 s, per tenant and auth version).
pub struct ExternalTokens {
    jwks: JwksCache,
    cache: Mutex<HashMap<(TenantId, u64, [u8; 32], TokenProtocol), Cached>>,
    /// The catalog writer, for just-in-time accounts; `None`: an issuer's
    /// `jit_users` creates nothing (the login is refused).
    catalog: Option<sprig_catalog::MariaCatalog>,
}

impl std::fmt::Debug for ExternalTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTokens").finish_non_exhaustive()
    }
}

impl Default for ExternalTokens {
    fn default() -> Self {
        Self::new(JwksCache::new(Arc::new(HttpFetcher::default())))
    }
}

fn secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

impl ExternalTokens {
    /// Verification with key sets from `jwks`.
    #[must_use]
    pub fn new(jwks: JwksCache) -> Self {
        Self {
            jwks,
            cache: Mutex::new(HashMap::new()),
            catalog: None,
        }
    }

    /// The same verifier creating just-in-time accounts through `cat`.
    #[must_use]
    pub fn with_catalog(mut self, cat: sprig_catalog::MariaCatalog) -> Self {
        self.catalog = Some(cat);
        self
    }

    /// The catalog writer for just-in-time accounts, if any.
    #[must_use]
    pub fn catalog(&self) -> Option<&sprig_catalog::MariaCatalog> {
        self.catalog.as_ref()
    }

    /// Verifies `token` (see the module docs).
    ///
    /// # Errors
    /// The [`Refusal`].
    pub async fn verify(&self, token: &str, c: &Check<'_>) -> Result<VerifiedToken, Refusal> {
        let key = c.cache.map(|(t, v)| {
            let h: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            (t, v, h, c.protocol)
        });
        if let Some(k) = &key {
            let m = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(hit) = m.get(k)
                && hit.until > Instant::now()
                && hit.token.expires + c.skew > c.now
            {
                return Ok(hit.token.clone());
            }
        }
        let v = self.verify_now(token, c).await?;
        if let Some(k) = key {
            let mut m = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
            let now = Instant::now();
            if m.len() >= TOKEN_CACHE_MAX {
                m.retain(|_, e| e.until > now);
                if m.len() >= TOKEN_CACHE_MAX {
                    m.clear();
                }
            }
            let left = v
                .expires
                .duration_since(c.now)
                .unwrap_or_default()
                .min(TOKEN_CACHE_TTL);
            m.insert(
                k,
                Cached {
                    token: v.clone(),
                    until: now + left,
                },
            );
        }
        Ok(v)
    }

    async fn verify_now(&self, token: &str, c: &Check<'_>) -> Result<VerifiedToken, Refusal> {
        let jws = Jws::parse(token).map_err(Refusal::Malformed)?;
        let iss = jws.str_claim("iss").ok_or(Refusal::UnknownIssuer)?;
        let issuer = c
            .issuers
            .iter()
            .find(|i| i.spec.issuer == iss && i.serves(c.protocol))
            .ok_or(Refusal::UnknownIssuer)?;
        let alg = Alg::parse(&jws.alg)
            .filter(|a| issuer.algs.contains(a))
            .ok_or(Refusal::Algorithm)?;
        let keys = self
            .jwks
            .keys(&issuer.source, c.jwks_refresh, jws.kid.as_deref())
            .await
            .map_err(|u| Refusal::KeysUnavailable(u.0))?;
        let mut candidates = keys
            .iter()
            .filter(|k| match (&jws.kid, &k.kid) {
                (Some(want), Some(have)) => want == have,
                (Some(_), None) => false,
                (None, _) => true,
            })
            .filter(|k| k.key.fits(alg) && k.alg.as_deref().is_none_or(|a| a == alg.name()))
            .peekable();
        if candidates.peek().is_none() {
            return Err(Refusal::UnknownKey);
        }
        if !candidates.any(|k| k.key.verify(alg, &jws.signed, &jws.signature)) {
            return Err(Refusal::Signature);
        }
        let now = secs(c.now);
        let skew = i64::try_from(c.skew.as_secs()).unwrap_or(0);
        let exp = jws
            .time_claim("exp")
            .ok_or_else(|| Refusal::Malformed("no exp".into()))?;
        if now > exp.saturating_add(skew) {
            return Err(Refusal::Expired);
        }
        if jws.time_claim("nbf").is_some_and(|n| n > now.saturating_add(skew)) {
            return Err(Refusal::NotYetValid);
        }
        let iat = jws.time_claim("iat");
        if iat.is_some_and(|t| t > now.saturating_add(skew)) {
            return Err(Refusal::NotYetValid);
        }
        if !jws
            .audiences()
            .iter()
            .any(|a| issuer.spec.audiences.iter().any(|w| w == a))
        {
            return Err(Refusal::Audience);
        }
        let lifetime = exp.saturating_sub(iat.unwrap_or(now));
        if lifetime > i64::from(issuer.spec.max_lifetime_s) {
            return Err(Refusal::Lifetime);
        }
        let m = mapping::map(&issuer.mappings, &jws.claims);
        Ok(VerifiedToken {
            issuer_id: issuer.spec.issuer_id.unwrap_or(0),
            issuer: issuer.spec.issuer.clone(),
            kind: issuer.spec.kind,
            jit_users: issuer.spec.jit_users,
            claims: Arc::new(jws.claims),
            user: m.user,
            roles: m.roles,
            expires: UNIX_EPOCH + Duration::from_secs(u64::try_from(exp).unwrap_or(0)),
        })
    }
}
