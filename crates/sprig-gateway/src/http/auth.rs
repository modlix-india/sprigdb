//! Authentication of HTTP requests.
//!
//! The HTTP front end asks an [`HttpAuthenticator`] for the principal of
//! each data request. [`ChapterFourteen`] is the implementation:
//! `sprig_auth::http::HttpAuth` (HTTP Basic and `Bearer
//! sprig_<tenant>_<id>_<secret>` API keys), over the same `LoginService`
//! the MySQL listeners use, so a request is checked exactly like a MySQL
//! login of the same user. Its refusal (MySQL's 1045 text and the like)
//! becomes the error the REST and gRPC layers render: 401 with
//! `WWW-Authenticate`, 403 for a suspended tenant, 429 with `Retry-After`,
//! 503.

use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use sprig_auth::{LoginService, Principal};
use sprig_common::Result;

/// What a request carries that authentication may use.
#[derive(Debug, Clone)]
pub struct HttpCredentials {
    /// The `authorization` header.
    pub authorization: Option<String>,
    /// The `x-sprig-tenant` header.
    pub tenant_header: Option<String>,
    /// The `Host` header.
    pub host: Option<String>,
    /// The TLS SNI.
    pub sni: Option<String>,
    /// The client address, after the PROXY header.
    pub client_ip: IpAddr,
    /// The request arrived over TLS.
    pub tls: bool,
    /// The client presented a certificate the listener verified.
    pub client_cert: bool,
    /// That certificate's subject and issuer, for `REQUIRE SUBJECT` and
    /// `ISSUER`.
    pub client_dn: Option<sprig_auth::mysql::ClientDn>,
}

/// Decides who sent a request.
#[async_trait]
pub trait HttpAuthenticator: Send + Sync + 'static {
    /// The principal, or the error to answer with (1045 → 401).
    ///
    /// # Errors
    /// 1045 for missing or bad credentials; 39303 for a suspended tenant;
    /// 39300 when rate limited; 39301 when credentials can't be checked now.
    async fn authenticate(&self, creds: &HttpCredentials) -> Result<Principal>;

    /// [`Self::authenticate`], with the issuer's token a `Basic` password
    /// was verified as (an account with the `token` method), so the request
    /// gets its claims as a bearer token's request does.
    ///
    /// # Errors
    /// As [`Self::authenticate`].
    async fn authenticate_login(
        &self,
        creds: &HttpCredentials,
    ) -> Result<(Principal, Option<String>)> {
        self.authenticate(creds).await.map(|p| (p, None))
    }
}

/// The `authorization` a request is forwarded to another gateway with: an
/// issuer's bearer token becomes a `Basic` header naming the tenant and the
/// account (`<tenant>.<user>:<token>`), since the forwarded call carries no
/// `Host` or `x-sprig-tenant` to find the tenant by; the target checks the
/// same token for the same account. Anything else goes as it came.
#[must_use]
pub fn forwarded_authorization(authorization: Option<&str>, principal: &Principal) -> String {
    let Some(a) = authorization else {
        return String::new();
    };
    let bearer = a
        .trim()
        .split_once(' ')
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, t)| t.trim());
    match (bearer, principal.tenant_name.as_deref()) {
        (Some(t), Some(tenant)) if sprig_auth::oidc::Jws::looks_like(t.as_bytes()) => {
            use base64::Engine as _;
            let pair = format!("{tenant}.{}:{t}", principal.user);
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(pair.as_bytes())
            )
        }
        _ => a.to_owned(),
    }
}

/// The `WWW-Authenticate` value of a 401.
pub const WWW_AUTHENTICATE: &str = sprig_auth::http::WWW_AUTHENTICATE;

/// The HTTP authentication of `sprig_auth::http::HttpAuth`.
pub struct ChapterFourteen {
    inner: sprig_auth::http::HttpAuth,
}

impl std::fmt::Debug for ChapterFourteen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChapterFourteen").finish_non_exhaustive()
    }
}

impl ChapterFourteen {
    /// Requests checked by `svc`'s accounts and policies.
    #[must_use]
    pub fn new(svc: Arc<LoginService>) -> Self {
        Self {
            inner: sprig_auth::http::HttpAuth::new(svc),
        }
    }
}

#[async_trait]
impl HttpAuthenticator for ChapterFourteen {
    async fn authenticate(&self, creds: &HttpCredentials) -> Result<Principal> {
        let req = sprig_auth::http::HttpRequest {
            authorization: creds.authorization.as_deref(),
            host: creds.host.as_deref(),
            sni: creds.sni.as_deref(),
            tenant_header: creds.tenant_header.as_deref(),
            client_ip: creds.client_ip,
            tls: creds.tls,
            client_cert: creds.client_cert,
            client_dn: creds.client_dn.as_ref(),
        };
        self.inner.authenticate(&req).await.map_err(|e| {
            sprig_api::status::from_parts(e.error.code, &e.error.sqlstate, &e.error.message)
        })
    }

    async fn authenticate_login(
        &self,
        creds: &HttpCredentials,
    ) -> Result<(Principal, Option<String>)> {
        let req = sprig_auth::http::HttpRequest {
            authorization: creds.authorization.as_deref(),
            host: creds.host.as_deref(),
            sni: creds.sni.as_deref(),
            tenant_header: creds.tenant_header.as_deref(),
            client_ip: creds.client_ip,
            tls: creds.tls,
            client_cert: creds.client_cert,
            client_dn: creds.client_dn.as_ref(),
        };
        self.inner
            .authenticate_login(&req)
            .await
            .map(|l| (l.principal, l.basic_token))
            .map_err(|e| {
                sprig_api::status::from_parts(e.error.code, &e.error.sqlstate, &e.error.message)
            })
    }
}

/// Accepts every request as one principal (tests of the HTTP layer).
#[derive(Debug, Clone)]
pub struct FixedPrincipal(pub Principal);

#[async_trait]
impl HttpAuthenticator for FixedPrincipal {
    async fn authenticate(&self, _creds: &HttpCredentials) -> Result<Principal> {
        Ok(self.0.clone())
    }
}
