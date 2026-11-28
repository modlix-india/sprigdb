//! The administration assistant's conversations, for platform operators:
//! the DC's system catalog records ([`sprig_ai::audit::rec`], module
//! `ai_admin` under the reserved tenant id 0).
//!
//! - **Where.** The engine, as every tenant's catalog records, through the
//!   leader of tenant 0's unit ([`super::records`]); no cluster, schema or
//!   grant is needed. Tenant 0's records hold no table
//!   data and no tenant has that id, and client statements can't reach
//!   catalog records.
//! - **Whose.** One operator's login (`tenant.user`, as
//!   `studio.operator_logins` lists it); the routes open it only for a
//!   caller who is a platform operator right now.
//! - **Ids.** Conversation and message ids from this store are shown with
//!   an `a` prefix (`a12`), so they never collide with a tenant's.

use std::sync::Arc;

use sprig_ai::audit::rec::RecAudit;
use sprig_common::{SprigCode, SprigError};

use super::super::super::Core;
use super::super::super::sql::Caller;

/// The prefix of this store's ids in the API.
pub const PREFIX: &str = "a";

/// `id` as the API shows it: `a<id>`.
#[must_use]
pub fn label(id: u64) -> String {
    format!("{PREFIX}{id}")
}

/// The id of an API id that names this store (`a<id>`), else `None`.
#[must_use]
pub fn parse(s: &str) -> Option<u64> {
    s.strip_prefix(PREFIX)?.parse().ok()
}

fn unavailable(why: impl Into<String>) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("administration conversations: {}", why.into()),
    )
}

/// The store for `caller`, who must be a platform operator (the routes
/// check).
///
/// # Errors
/// 39070 when this gateway serves no tenants (no catalog records).
pub fn store(core: &Arc<Core>, caller: &Caller) -> Result<RecAudit, SprigError> {
    let Some(svc) = &core.studio.tenants else {
        return Err(unavailable("this gateway has no catalog records"));
    };
    Ok(RecAudit::system(
        super::records(svc),
        &super::super::ops::login_of(caller),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_carry_a_prefix() {
        assert_eq!(label(12), "a12");
        assert_eq!(parse("a12"), Some(12));
        assert_eq!(parse("12"), None);
        assert_eq!(parse("a"), None);
        assert_eq!(parse("ax"), None);
    }
}
