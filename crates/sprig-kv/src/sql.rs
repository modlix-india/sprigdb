//! The SQL a key-value front end runs, and literals for it.
//!
//! Front ends run their statements through a [`SqlRunner`]: in the gateway
//! that is the user's own executor session (name virtualization,
//! privileges, the router), in tests a plain MariaDB connection.
//! Every value is sent as a hex literal, so statements stay ASCII (the
//! gateway's parser works on UTF-8 text) and binary-safe.

use async_trait::async_trait;

/// One result row, text protocol: `None` is SQL `NULL`.
pub type Row = Vec<Option<Vec<u8>>>;

/// One result column of [`SqlRunner::query_columns`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The column's name in the result.
    pub name: String,
    /// The protocol's type code (`MYSQL_TYPE_LONGLONG` is 8, ...).
    pub ty: u8,
    /// `UNSIGNED`.
    pub unsigned: bool,
}

/// A statement's result with its columns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    /// The columns.
    pub columns: Vec<Column>,
    /// The rows, text protocol.
    pub rows: Vec<Row>,
}

/// A failed statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    /// The MySQL error number, when MariaDB or the gateway gave one.
    pub errno: Option<u16>,
    /// The message.
    pub message: String,
}

impl SqlError {
    /// A deadlock (1213) or lock wait timeout (1205): retry the whole unit.
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(self.errno, Some(1213 | 1205))
    }
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.errno {
            Some(n) => write!(f, "{n}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

/// Runs SQL for a front end.
#[async_trait]
pub trait SqlRunner: Send {
    /// A statement returning rows.
    async fn query(&mut self, sql: &str) -> Result<Vec<Row>, SqlError>;
    /// A statement returning an affected-row count.
    async fn exec(&mut self, sql: &str) -> Result<u64, SqlError>;
    /// Marks the writes that follow, until called with `false`, as a Redis
    /// command's whose keyspace notifications the command sends itself, so
    /// the notifications from the change feed pass over them. The gateway's
    /// runner prefixes each write statement with the comment `o=k`; by
    /// default nothing.
    fn notified(&mut self, _on: bool) {}
    /// The client's data center hint (the text after the last
    /// `~` of a Redis `CLIENT SETNAME` name; `None` clears it). The
    /// gateway's runner counts the session's statements as traffic from
    /// that DC; by default nothing.
    fn client_dc(&mut self, _raw: Option<&str>) {}
    /// Whether a row-level security policy on the object with this id
    /// binds the session's user (`None`: can't tell, read as bound).
    /// Search answers such a user from the rows it reads; by default
    /// `None`.
    fn row_policy_binds(&self, _object: u64) -> Option<bool> {
        None
    }
    /// Whether [`SqlRunner::query`] returns the rows of every result set
    /// a statement sends (a compound statement's several `SELECT`s), not
    /// only the first one's. The Redis front end groups a pipeline's
    /// commands into one statement only on such a runner; by default
    /// `false`.
    fn multi_results(&self) -> bool {
        false
    }
    /// Whether [`SqlRunner::query_bound`] runs prepared statements;
    /// callers otherwise send the statement with its values as literals.
    /// By default `false`.
    fn prepares(&self) -> bool {
        false
    }
    /// Whether the tenant's logical database `database` is placed on the
    /// storage engine (its `storage.placement` setting is `engine`); the
    /// Redis front end then runs its Redis databases there. By default
    /// `false`.
    fn engine_placed(&self, _database: &str) -> bool {
        false
    }
    /// `sql`, with `?` placeholders, prepared once per session and run
    /// with `params` bound as binary strings: its rows. Called only when
    /// [`SqlRunner::prepares`] says so; by default an error.
    async fn query_bound(&mut self, _sql: &str, _params: &[&[u8]]) -> Result<Vec<Row>, SqlError> {
        Err(SqlError {
            errno: None,
            message: "this runner doesn't prepare statements".into(),
        })
    }
    /// A client's own statement (no internal names allowed, the session's
    /// privileges and row-level security as for any client): its columns
    /// and rows. By default an error.
    async fn query_columns(&mut self, _sql: &str) -> Result<Table, SqlError> {
        Err(SqlError {
            errno: None,
            message: "this runner doesn't run client statements".into(),
        })
    }
}

/// `X'...'`, or `''` for empty bytes.
#[must_use]
pub fn lit(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    if b.is_empty() {
        return "''".into();
    }
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        s.push(char::from(HEX[usize::from(x >> 4)]));
        s.push(char::from(HEX[usize::from(x & 15)]));
    }
    s.push('\'');
    s
}

/// A backquoted identifier.
#[must_use]
pub fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// A cell as an `i64` (0 for `NULL` or text that isn't a number).
#[must_use]
pub fn cell_i64(c: Option<&Vec<u8>>) -> i64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// A cell as a `u64`.
#[must_use]
pub fn cell_u64(c: Option<&Vec<u8>>) -> u64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// A cell as an `f64`.
#[must_use]
pub fn cell_f64(c: Option<&Vec<u8>>) -> f64 {
    c.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals() {
        assert_eq!(lit(b""), "''");
        assert_eq!(lit(b"\x00a'"), "X'006127'");
        assert_eq!(ident("a`b"), "`a``b`");
    }
}
