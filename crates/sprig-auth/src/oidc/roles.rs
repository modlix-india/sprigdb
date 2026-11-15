//! Token roles: the roles an issuer's role mappings give a token login
//! (chapter 29, "Token roles").
//!
//! ```text
//! a token's roles := every role mapping matching its claims, by the issuer
//!                    (claims' iss, active) in the session's snapshot
//!   each target names a role: `name` (host '%'), else `name@host`
//!   a name the tenant doesn't define: ignored (the login audits
//!   auth.token_roles_ignored); never created
//! the session: granted for its life, active from login with the default
//!   roles (A := defaults ∪ token roles); SET ROLE may name them, NONE
//!   drops them, DEFAULT and ALL include them
//! backend: a user with the token method holds the privileges of every role
//!   an active issuer's role mapping can name (a template: every role its
//!   pattern matches), as it holds those of granted roles it hasn't
//!   activated; the gateway enforces which are active
//! ```
//!
//! Sessions derive their token roles from the verified claims at every auth
//! version, so a changed mapping, a disabled issuer or a dropped role holds
//! from the next statement, as a `REVOKE` does.

use std::sync::Arc;

use regex::Regex;
use serde_json::Value;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::RoleId;

use super::mapping::{self, REGEX_SIZE_LIMIT};
use super::store::MappingKind;
use super::{Issuer, VerifiedToken};
use crate::audit::{self, AuditEvent};
use crate::authz::state::AuthzState;
use crate::cache::TenantAuth;

/// The role a mapping's target text names: `name` with host `%`, else
/// `name@host` split at the last `@`.
#[must_use]
pub fn resolve(state: &AuthzState, name: &str) -> Option<RoleId> {
    state.role_named(name, "%").or_else(|| {
        let (n, h) = name.rsplit_once('@')?;
        state.role_named(n, h)
    })
}

/// `names` as role ids, and the names the tenant doesn't define.
#[must_use]
pub fn of_names(state: &AuthzState, names: &[String]) -> (Vec<RoleId>, Vec<String>) {
    let mut ids = Vec::new();
    let mut unknown = Vec::new();
    for n in names {
        match resolve(state, n) {
            Some(id) if !ids.contains(&id) => ids.push(id),
            Some(_) => {}
            None => unknown.push(n.clone()),
        }
    }
    (ids, unknown)
}

/// The issuer of `claims` among `issuers`: the active one whose `issuer`
/// is the claims' `iss`. SprigDB's own session tokens have none.
fn issuer_of<'a>(issuers: &'a [Arc<Issuer>], claims: &Value) -> Option<&'a Issuer> {
    let iss = claims.get("iss")?.as_str()?;
    if iss == crate::token::ISSUER {
        return None;
    }
    issuers
        .iter()
        .find(|i| i.spec.active && i.spec.issuer == iss)
        .map(AsRef::as_ref)
}

/// The token roles of a session whose verified claims are `claims`, at
/// `cat`: the role mappings of the claims' issuer, evaluated now, as role
/// ids the tenant defines. Empty without a matching active issuer.
#[must_use]
pub fn session_roles(cat: &TenantCatalog, claims: &Value) -> Vec<RoleId> {
    let (Some(auth), Some(state)) = (TenantAuth::of(cat), AuthzState::of(cat)) else {
        return Vec::new();
    };
    let Some(issuer) = issuer_of(auth.issuers(), claims) else {
        return Vec::new();
    };
    of_names(state, &mapping::map(&issuer.mappings, claims).roles).0
}

/// A role mapping's target as an anchored pattern over role names: its
/// literal text, each `{claim}` any text. `None` for an unmatched brace.
fn template(target: &str) -> Option<Regex> {
    let mut pat = String::from("^");
    let mut rest = target;
    while let Some(open) = rest.find('{') {
        pat.push_str(&regex::escape(&rest[..open]));
        let close = rest[open + 1..].find('}')?;
        pat.push_str(".*");
        rest = &rest[open + 1 + close + 1..];
    }
    if rest.contains('}') {
        return None;
    }
    pat.push_str(&regex::escape(rest));
    pat.push('$');
    regex::RegexBuilder::new(&pat)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .ok()
}

/// Every role a token of an active issuer in `issuers` could be given: a
/// literal target's role, and for a template every role whose name (or
/// `name@host`) its pattern matches. What a token-taking user's backend
/// account holds beyond its granted roles.
#[must_use]
pub fn possible_roles(issuers: &[Arc<Issuer>], state: &AuthzState) -> Vec<RoleId> {
    let mut out: Vec<RoleId> = Vec::new();
    let mut add = |id: RoleId| {
        if !out.contains(&id) {
            out.push(id);
        }
    };
    for i in issuers.iter().filter(|i| i.spec.active) {
        for m in i
            .mappings
            .iter()
            .filter(|m| m.row.kind == MappingKind::Role)
        {
            let t = &m.row.target;
            if !t.contains('{') {
                if let Some(id) = resolve(state, t) {
                    add(id);
                }
                continue;
            }
            let Some(re) = template(t) else {
                continue;
            };
            for r in &state.rows().roles {
                let full = format!("{}@{}", r.name, r.host);
                if (r.host == "%" && re.is_match(&r.name)) || re.is_match(&full) {
                    add(r.role_id);
                }
            }
        }
    }
    out
}

/// The audit of an accepted token login's roles: sets `success`'s detail
/// (the issuer and the roles taken), and answers the
/// `auth.token_roles_ignored` event naming the roles the tenant doesn't
/// define, if any, to emit after it. Those roles are never created.
#[must_use]
pub fn audit_login(
    success: &mut AuditEvent,
    token: &VerifiedToken,
    cat: &TenantCatalog,
) -> Option<AuditEvent> {
    // A catalog without the authorization module has no roles to take.
    let Some(state) = AuthzState::of(cat).filter(|s| s.is_migrated()) else {
        success.detail = Some(format!("issuer {}", token.issuer));
        return None;
    };
    let unknown = of_names(state, &token.roles).1;
    let taken: Vec<&str> = token
        .roles
        .iter()
        .filter(|n| !unknown.contains(n))
        .map(String::as_str)
        .collect();
    success.detail = Some(if taken.is_empty() {
        format!("issuer {}", token.issuer)
    } else {
        format!("issuer {}; token roles {}", token.issuer, taken.join(", "))
    });
    if unknown.is_empty() {
        return None;
    }
    let mut e = success.clone();
    e.event = audit::events::TOKEN_ROLES_IGNORED;
    e.outcome = "ignored";
    e.reason = Some("undefined_role");
    e.detail = Some(format!(
        "issuer {}: roles the tenant doesn't define, ignored: {}",
        token.issuer,
        unknown.join(", ")
    ));
    Some(e)
}
