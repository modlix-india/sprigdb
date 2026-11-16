//! `FT.*` (Redis Search's commands) on SprigDB's search indexes.
//!
//! The gateway installs the [`SearchCalls`] hook on the [`crate::Env`]: an
//! `FT.CREATE` becomes `CREATE SEARCH INDEX` on the selected database's
//! namespace, run on the login's own SQL session (so privileges and the
//! catalog are SQL's), and `FT.SEARCH` and `FT.AGGREGATE` query a copy of
//! the index and re-read the matching hashes through that session.
//! Without a hook every `FT.*` command answers that search isn't
//! available. They aren't allowed inside `MULTI` or scripts.

use async_trait::async_trait;
use bytes::Bytes;
use sprig_kv::SqlRunner;
use sprig_proto_redis::Frame;

/// One `FT.*` command.
#[derive(Debug, Clone)]
pub struct FtCall<'a> {
    /// The command in lower case (`ft.search`).
    pub command: &'a str,
    /// The arguments after the command name.
    pub args: &'a [Bytes],
    /// The login's tenant (its id as text).
    pub scope: &'a str,
    /// `redis.logical_database`: where the namespaces are.
    pub database: &'a str,
    /// The selected database (`SELECT`): the namespace `db<n>`.
    pub db: u32,
    /// RESP3 replies.
    pub resp3: bool,
}

/// Runs `FT.*` commands (the gateway's implementation).
#[async_trait]
pub trait SearchCalls: Send + Sync + std::fmt::Debug {
    /// Runs `call` with `sql`, the login's session.
    async fn ft(&self, sql: &mut dyn SqlRunner, call: FtCall<'_>) -> Frame;
}
