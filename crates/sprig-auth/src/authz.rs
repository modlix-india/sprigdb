//! Authorization: privileges, roles, the checks and backend grants.
//!
//! - [`authorizer`]: the interface the gateway calls for every decision
//!   ([`Authorizer`], [`SessionGrants`]), chosen per tenant by
//!   `auth.authorizer`, and the default engine, [`MysqlGrants`], which is
//!   the rest of this list;
//! - [`privilege`]: actions as stable bits, levels, the MySQL and MariaDB
//!   privilege names;
//! - [`store`]: the `authz` catalog module (`roles`, `role_edges`,
//!   `privileges`, as catalog records) with typed rows and writes;
//! - [`state`]: [`AuthzExtension`], loading [`AuthzState`] into every
//!   catalog snapshot at each auth version;
//! - [`compile`]: [`CompiledPrivileges`] and the checks;
//! - [`stmt`], [`account_sql`] and [`show`]: the account statements and
//!   `SHOW GRANTS` (with basic roles);
//! - [`backend`]: per-user MariaDB accounts granted only on the tenant's
//!   own shard schemas, and their reconciler;
//! - [`definers`]: stored code runs as a tenant account, never as
//!   `sprig_ctl`, and code created before that is moved;
//! - [`migrate`]: a no-op kept for its callers (the modules' rows are
//!   engine records, at the newest format from the start).
//!
//! The gateway glues these to statements (`sprig-gateway`'s
//! `tenant::authz`), and `sprig-sql` parses the statements
//! (`sprig_sql::account_stmt`) and finds what each statement touches
//! (`sprig_sql::access`).

pub mod account_sql;
pub mod authorizer;
pub mod backend;
pub mod compile;
pub mod definers;
pub mod engine_guard;
pub mod owner;
pub mod privilege;
// Row-level security.
pub mod rls;
pub mod settings;
pub mod show;
pub mod state;
pub mod stmt;
pub mod store;

pub use authorizer::{Authorizer, MysqlGrants, SessionGrants};
pub use compile::{CompiledPrivileges, Target};
pub use privilege::{Action, ActionSet, Level, ObjectType};
pub use state::{AuthzExtension, AuthzState};
pub use store::{AUTHZ_MODULE, Grantee, PrivTarget};

use sprig_catalog::AuthCatalog;
use sprig_common::Result;
use sprig_common::ids::TenantId;

/// Brings `tenant`'s catalog to the `auth` and `authz` modules: nothing to
/// do, since their rows are engine records that start at the newest
/// format. Kept for its callers; returns 0 (no auth change), as the
/// migration did for a catalog already current.
///
/// # Errors
/// None.
#[allow(clippy::unused_async, clippy::unnecessary_wraps)] // the callers await it, as they did the migration
pub async fn migrate(catalog: &AuthCatalog, tenant: TenantId) -> Result<u64> {
    let _ = (catalog, tenant);
    Ok(0)
}
