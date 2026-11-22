//! The gateway's side of hiding automatic indexes from metadata, next to
//! the hiding in `sprig-sql`:
//!
//! - [`session_flag`]: `SET @sprig_show_auto_indexes = 1|0` is kept in the
//!   session ([`TenantSession::show_auto_indexes`]) and answered with OK;
//!   it never reaches MariaDB. Plans then use
//!   [`TenantSession::visibility`], so `SHOW INDEX`, `SHOW CREATE TABLE`
//!   and `information_schema.STATISTICS` show automatic indexes, and the
//!   session may `DROP INDEX sprig_auto_...`.
//! - [`make_room`]: nothing now. It made room under MariaDB's 64-index
//!   limit by dropping automatic indexes on a table's MariaDB shard tables;
//!   automatic indexing is gone with those tables.

use mysql_async::Pool;
use sprig_common::Result;
use sprig_sql::lex::LexOptions;
use sprig_sql::names::DdlTarget;

use super::session::TenantSession;

/// `SET @sprig_show_auto_indexes = <v>`: applies it to `ts`. `None` when
/// the statement is anything else.
///
/// # Errors
/// 39051 for the variable set together with others or to a value that
/// isn't `1`, `0`, `ON`, `OFF`, `TRUE` or `FALSE`.
pub fn session_flag(ts: &mut TenantSession, text: &[u8], lex: &LexOptions) -> Option<Result<()>> {
    let r = sprig_sql::names::session_flag(text, lex)?;
    Some(match r {
        Ok(v) => {
            ts.show_auto_indexes = v;
            Ok(())
        }
        Err(e) => Err(e.into()),
    })
}

/// The 64-index rule for one DDL statement: no automatic indexes exist any
/// more, so none are dropped.
#[allow(clippy::unused_async)]
pub async fn make_room(
    _ts: &TenantSession,
    _ctl: &Pool,
    _target: &DdlTarget,
    _original: &[u8],
    _lex: &LexOptions,
) -> Vec<String> {
    Vec::new()
}
