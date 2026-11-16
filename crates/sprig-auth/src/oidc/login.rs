//! Logins where the token names the account: HTTP and gRPC bearer tokens
//! and `MONGODB-OIDC`.
//!
//! ```text
//! token_login(tenant, token):
//!   the tenant takes logins                         else 1045 (39301 if unreadable)
//!   verify against its issuers for the protocol     else 1045 (detail: why)
//!   the first user mapping names the account; none  else 1045
//!   no such account: created when the issuer has jit_users (auth.user_created_jit)
//!   the account has the token method                else 1045
//!   temporary lock 3955, suspension 39303, ACCOUNT LOCK 3118, TLS and
//!   certificate requirements 1045, as for an API key
//!   the principal, with the verified token (its claims: SPRIG_CLAIM())
//! ```
//!
//! Refused tokens count toward the address's failure limit, not the
//! account's lockout (a token isn't a password, as an API key isn't).

use std::net::IpAddr;
use std::sync::Arc;

use sprig_common::{ErrorKind, SprigCode, SprigError};
use sprig_proto_mysql::{ErrPacket, Persona};

use super::{Check, Refusal, TokenProtocol, VerifiedToken};
use crate::audit::{self, AuditEvent};
use crate::cache::{AuthUser, TenantAuth};
use crate::handoff::{HandoffResult, HandoffStart, Route, remote_access_denied, route};
use crate::mysql::{
    ClientDn, LoginService, LoginSource, Principal, certificate_ok, locked, temp_locked,
};
use crate::store::{AuthMethods, RequireTls};
use zeroize::Zeroizing;

/// One token login's request.
#[derive(Debug, Clone, Copy)]
pub struct TokenLogin<'a> {
    /// The protocol: `Http` or `Mongodb`.
    pub protocol: TokenProtocol,
    /// The `protocol` label of metrics and audit events (`http`, `mongo`).
    pub label: &'static str,
    /// The tenant's name.
    pub tenant: &'a str,
    /// The token.
    pub token: &'a str,
    /// The client address.
    pub client_ip: IpAddr,
    /// TLS at the gateway.
    pub tls: bool,
    /// A client certificate the listener verified.
    pub client_cert: bool,
    /// Its names.
    pub client_dn: Option<&'a ClientDn>,
}

/// A refused token login: why (the audit reason) and the error to answer
/// with (MySQL's form; HTTP maps it to a status).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenDenied {
    /// `bad_credentials`, `temp_locked`, `suspended`, `locked`,
    /// `tls_required`, `unavailable`, `no_backend`.
    pub reason: &'static str,
    /// The error.
    pub error: ErrPacket,
}

fn denied(ip: IpAddr) -> ErrPacket {
    ErrPacket::access_denied(Persona::MySql84, "", &ip.to_string(), true)
}

fn unavailable() -> ErrPacket {
    ErrPacket::from_error(
        Persona::MySql84,
        &SprigError::new(
            SprigCode::AuthUnavailable,
            "authentication unavailable; retry",
        ),
    )
}

impl LoginService {
    /// The tenant a token login names: the TLS server name (or HTTP host)
    /// under `listener.tenant_sni_suffixes`, else `login_name`'s prefix
    /// (`acme.alice`); `None` when neither names one.
    #[must_use]
    pub fn tenant_named(&self, login_name: Option<&str>, sni: Option<&str>) -> Option<String> {
        let LoginSource::Catalog(cl) = self.source() else {
            return None;
        };
        let suffixes = cl.sni_suffixes();
        let from_sni = crate::account::parse_login("-", sni, &suffixes, crate::admin::MAX_NAME)
            .filter(|p| p.form == crate::store::LoginForm::Plain)
            .map(|p| p.tenant);
        from_sni.or_else(|| {
            crate::account::parse_login(login_name?, None, &[], crate::admin::MAX_NAME)
                .map(|p| p.tenant)
        })
    }

    /// The first active issuer of `tenant` that takes `MONGODB-OIDC`
    /// tokens: what the human flow's first step answers (`IdpInfo`).
    pub async fn mongo_issuer(&self, tenant: &str) -> Option<super::IssuerSpec> {
        let LoginSource::Catalog(cl) = self.source() else {
            return None;
        };
        // The issuer is public (any OIDC client of the tenant gets it), so
        // a DC the login will be handed from reads it here; one that may
        // not serve the tenant at all (`residency.deny_remote_access`)
        // refuses now, without loading the tenant's catalog.
        if self.token_route(tenant).await == Route::Deny {
            return None;
        }
        let t = cl.tenants().lookup(tenant).await.ok()??;
        if !t.can_log_in {
            return None;
        }
        let catalog = cl.catalogs().catalog(t.id).await.ok()?;
        TenantAuth::of(&catalog)?
            .issuers()
            .iter()
            .find(|i| i.serves(TokenProtocol::Mongodb))
            .map(|i| i.spec.clone())
    }

    /// Where a token login for `tenant` is decided: the tenant's residency
    /// as for a MySQL login (`Local` without a hand-off configuration, an
    /// unpinned tenant, or an unreadable directory).
    pub async fn token_route(&self, tenant: &str) -> Route {
        let (Some(h), LoginSource::Catalog(c)) = (self.handoff(), self.source()) else {
            return Route::Local;
        };
        match c.tenants().residency(tenant).await {
            Ok(Some(r)) if r.allowed_dcs.is_empty() => Route::Deny,
            Ok(Some(r)) => route(Some(&r.allowed_dcs), &h.here, r.deny_remote_access),
            Ok(None) => Route::Local,
            Err(e) => {
                tracing::warn!(error = %e, "residency unreadable; the token login decides locally");
                Route::Local
            }
        }
    }

    /// Logs in with an issuer's token that names its account (see the
    /// module docs), routed by the tenant's residency: decided here, handed
    /// to an allowed DC (whose verified token comes back with the
    /// principal), or refused with 39302. A login decided here emits
    /// `auth.login_succeeded` or `auth.login_failed` with mechanism `jwt`;
    /// a handed-off one is audited by the deciding DC.
    ///
    /// # Errors
    /// [`TokenDenied`].
    pub async fn token_login(
        &self,
        req: &TokenLogin<'_>,
    ) -> Result<(Principal, VerifiedToken), TokenDenied> {
        match self.token_route(req.tenant).await {
            Route::Local => self.token_login_here(req).await,
            Route::Deny => {
                metrics::counter!("sprig_auth_handoffs_total", "outcome" => "denied").increment(1);
                Err(TokenDenied {
                    reason: "remote_denied",
                    error: remote_access_denied(Persona::MySql84),
                })
            }
            Route::HandOff(dcs) => self.relay_token_login(req, &dcs).await,
        }
    }

    /// The source side of a token login's hand-off: each allowed DC in
    /// order until one answers; the principal gets this gateway's backend
    /// target.
    async fn relay_token_login(
        &self,
        req: &TokenLogin<'_>,
        dcs: &[sprig_common::ids::DcId],
    ) -> Result<(Principal, VerifiedToken), TokenDenied> {
        let (Some(handoff), LoginSource::Catalog(logins)) = (self.handoff(), self.source()) else {
            return Err(TokenDenied {
                reason: "unavailable",
                error: unavailable(),
            });
        };
        let start = HandoffStart::of_token(req, handoff.here.clone());
        for dc in dcs {
            let reply = handoff
                .peers
                .login(
                    dc,
                    start.clone(),
                    Zeroizing::new(req.token.as_bytes().to_vec()),
                )
                .await;
            match reply {
                Ok(HandoffResult::Accepted(mut principal, token)) => {
                    metrics::counter!("sprig_auth_handoffs_total", "outcome" => "decided")
                        .increment(1);
                    let Some(token) = token else {
                        // A target from before token hand-offs.
                        return Err(TokenDenied {
                            reason: "unavailable",
                            error: unavailable(),
                        });
                    };
                    match logins.local_backend(&principal) {
                        Some(Ok(b)) => principal.backend = b,
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "no backend for a handed-off token login");
                            return Err(TokenDenied {
                                reason: "no_backend",
                                error: ErrPacket::from_error(Persona::MySql84, &e),
                            });
                        }
                        None => {}
                    }
                    return Ok((*principal, *token));
                }
                Ok(HandoffResult::Refused(packet)) => {
                    metrics::counter!("sprig_auth_handoffs_total", "outcome" => "decided")
                        .increment(1);
                    return Err(TokenDenied {
                        reason: "bad_credentials",
                        error: packet,
                    });
                }
                Err(e) => {
                    metrics::counter!("sprig_auth_handoffs_total", "outcome" => "unreachable")
                        .increment(1);
                    tracing::warn!(%dc, error = %e, "token login handoff target unreachable; trying the next");
                }
            }
        }
        Err(TokenDenied {
            reason: "unavailable",
            error: unavailable(),
        })
    }

    /// [`Self::token_login`] decided here, whatever the residency (the
    /// hand-off target's side checks it first).
    ///
    /// # Errors
    /// [`TokenDenied`].
    pub async fn token_login_here(
        &self,
        req: &TokenLogin<'_>,
    ) -> Result<(Principal, VerifiedToken), TokenDenied> {
        let mut ev = AuditEvent::new(
            audit::events::LOGIN_SUCCEEDED,
            req.label,
            self.clock().wall(),
        );
        ev.client_ip = Some(req.client_ip);
        ev.mechanism = Some("jwt");
        ev.tenant_name = Some(req.tenant.to_owned());
        let mut ignored = None;
        let r = self.token_login_checked(req, &mut ev, &mut ignored).await;
        match &r {
            Ok(_) => {
                metrics::counter!("sprig_auth_logins_total", "protocol" => req.label,
                    "mechanism" => "jwt", "outcome" => "ok")
                .increment(1);
            }
            Err(d) => {
                ev.event = audit::events::LOGIN_FAILED;
                ev.outcome = "refused";
                ev.reason = Some(d.reason);
                metrics::counter!("sprig_auth_logins_total", "protocol" => req.label,
                    "mechanism" => "jwt", "outcome" => d.reason)
                .increment(1);
                if d.reason == "bad_credentials" {
                    self.ip_failure(req.client_ip, req.label);
                }
            }
        }
        self.audit().emit(&ev);
        if let Some(i) = ignored {
            self.audit().emit(&i);
        }
        r
    }

    #[allow(clippy::too_many_lines)] // the checks, in order
    async fn token_login_checked(
        &self,
        req: &TokenLogin<'_>,
        ev: &mut AuditEvent,
        ignored: &mut Option<AuditEvent>,
    ) -> Result<(Principal, VerifiedToken), TokenDenied> {
        let bad = |ev: &mut AuditEvent, r: Option<Refusal>| {
            if let Some(r) = r {
                ev.detail = Some(r.detail());
            }
            TokenDenied {
                reason: "bad_credentials",
                error: denied(req.client_ip),
            }
        };
        let gone = || TokenDenied {
            reason: "unavailable",
            error: unavailable(),
        };
        let (LoginSource::Catalog(cl), Some(tokens)) = (self.source(), self.external_tokens())
        else {
            return Err(bad(ev, None));
        };
        let tokens = Arc::clone(tokens);
        let tenant = match cl.tenants().lookup(req.tenant).await {
            Ok(Some(t)) if t.can_log_in => t,
            Ok(_) => return Err(bad(ev, None)),
            Err(e) => {
                tracing::warn!(tenant = %req.tenant, error = %e, "token login: tenant directory unreadable");
                return Err(gone());
            }
        };
        ev.tenant = Some(tenant.id);
        let mut catalog = cl.catalogs().catalog(tenant.id).await.map_err(|e| {
            tracing::warn!(tenant = %tenant.name, error = %e, "token login: catalog unreadable");
            gone()
        })?;
        let (skew, jwks_refresh) = cl.token_times(&catalog);
        let verified = {
            let Some(auth) = TenantAuth::of(&catalog) else {
                return Err(bad(ev, None));
            };
            let check = Check {
                issuers: auth.issuers(),
                protocol: req.protocol,
                skew,
                jwks_refresh,
                now: self.clock().wall(),
                cache: Some((tenant.id, catalog.auth_version)),
            };
            tokens
                .verify(req.token, &check)
                .await
                .map_err(|r| bad(ev, Some(r)))?
        };
        let Some(name) = verified.user.clone() else {
            return Err(bad(ev, Some(Refusal::NoUser)));
        };
        ev.user = Some(name.clone());
        let found =
            TenantAuth::of(&catalog).and_then(|a| a.match_account(&name, req.client_ip).cloned());
        let user: Arc<AuthUser> = if let Some(u) = found {
            u
        } else {
            let Some(cat) = tokens.catalog().filter(|_| verified.jit_users) else {
                return Err(bad(ev, Some(Refusal::NoAccount)));
            };
            match super::store::create_jit_user(cat, tenant.id, &name, self.clock().wall()).await {
                Ok(c) => {
                    let mut e = AuditEvent::new(
                        audit::events::USER_CREATED_JIT,
                        req.label,
                        self.clock().wall(),
                    );
                    e.tenant = Some(tenant.id);
                    e.tenant_name = Some(tenant.name.clone());
                    e.user = Some(name.clone());
                    e.host = Some("%".into());
                    e.user_id = Some(c.user_id);
                    e.client_ip = Some(req.client_ip);
                    e.detail = Some(format!("issuer {}", verified.issuer));
                    self.audit().emit(&e);
                }
                // Created meanwhile by another login: use it.
                Err(e) if matches!(e.kind, ErrorKind::Backend { errno: 1396, .. }) => {}
                Err(e) => {
                    tracing::warn!(tenant = %tenant.name, user = %name, error = %e, "just-in-time account not created");
                    return Err(gone());
                }
            }
            catalog = cl.catalogs().catalog(tenant.id).await.map_err(|_| gone())?;
            TenantAuth::of(&catalog)
                .and_then(|a| a.match_account(&name, req.client_ip).cloned())
                .ok_or_else(|| bad(ev, Some(Refusal::NoAccount)))?
        };
        let row = &user.row;
        ev.host = Some(row.host.clone());
        ev.user_id = Some(row.user_id);
        if !row.auth_methods.contains(AuthMethods::TOKEN) {
            return Err(bad(ev, Some(Refusal::NoAccount)));
        }
        let now = self.clock().wall();
        let auth = TenantAuth::of(&catalog);
        if let Some(lock) = auth
            .and_then(|a| a.temp_lock(row.user_id))
            .filter(|l| l.locked_until > now)
        {
            return Err(TokenDenied {
                reason: "temp_locked",
                error: temp_locked(Persona::MySql84, &row.name, &row.host, &lock, now),
            });
        }
        if tenant.suspended {
            return Err(TokenDenied {
                reason: "suspended",
                error: ErrPacket::from_error(
                    Persona::MySql84,
                    &SprigError::new(
                        SprigCode::TenantSuspended,
                        format!("tenant {} is suspended", tenant.name),
                    ),
                ),
            });
        }
        if row.account_locked {
            return Err(TokenDenied {
                reason: "locked",
                error: locked(Persona::MySql84, &row.name, &row.host),
            });
        }
        let need_tls = row.require_tls != RequireTls::None || cl.require_tls(&catalog);
        if (need_tls && !req.tls) || !certificate_ok(row, req.client_cert, req.client_dn) {
            return Err(TokenDenied {
                reason: "tls_required",
                error: ErrPacket::access_denied(
                    Persona::MySql84,
                    &row.name,
                    &req.client_ip.to_string(),
                    true,
                ),
            });
        }
        let backend = cl.backend_of(&tenant, &user).map_err(|e| {
            tracing::error!(tenant = %tenant.name, error = %e, "no backend for a verified token");
            TokenDenied {
                reason: "no_backend",
                error: ErrPacket::from_error(Persona::MySql84, &e),
            }
        })?;
        *ignored = super::roles::audit_login(ev, &verified, &catalog);
        Ok((
            Principal {
                tenant: Some(tenant.id),
                tenant_name: Some(tenant.name.clone()),
                user_id: Some(row.user_id),
                user: row.name.clone(),
                host: row.host.clone(),
                client_ip: req.client_ip,
                must_change_password: false,
                limits: row.limits,
                auth_version: catalog.auth_version,
                backend,
            },
            verified,
        ))
    }
}
