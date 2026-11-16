//! The internal schema `__sprig_sys` and the cluster's own tables in it.
//! The mover owns three more.

use sprig_common::names::{SYS_SCHEMA, internal_object, quote};

use crate::sql::{ProbeError, ProbeSession};

/// `__sprig_sys.__sprig_probe`: XA round trips and other probe writes.
#[must_use]
pub fn probe_table() -> String {
    format!("{}.{}", quote(SYS_SCHEMA), quote(&internal_object("probe")))
}

/// `__sprig_sys.__sprig_heartbeat`: one row per writer node.
#[must_use]
pub fn heartbeat_table() -> String {
    format!(
        "{}.{}",
        quote(SYS_SCHEMA),
        quote(&internal_object("heartbeat"))
    )
}

/// `__sprig_sys.__sprig_feed_time`: the change feed's time index, one
/// binlog position per [`FEED_TIME_STEP_SECONDS`]: a change
/// stream that starts at a time reads from the last position written
/// before it.
#[must_use]
pub fn feed_time_table() -> String {
    format!(
        "{}.{}",
        quote(SYS_SCHEMA),
        quote(&internal_object("feed_time"))
    )
}

/// The time index's step: at most one row per this many seconds.
pub const FEED_TIME_STEP_SECONDS: u64 = 10;

/// How long the time index keeps a row (8 days, longer than the dev
/// clusters' binlog retention; a position whose binlog is gone fails the
/// stream that reads it).
pub const FEED_TIME_KEEP_SECONDS: u64 = 8 * 86_400;

/// `CREATE TABLE` of [`feed_time_table`].
#[must_use]
pub fn feed_time_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (bucket BIGINT UNSIGNED NOT NULL PRIMARY KEY, \
         pos VARCHAR(2048) CHARACTER SET ascii NOT NULL, written_at DATETIME(6) NOT NULL) \
         ENGINE=InnoDB",
        feed_time_table()
    )
}

/// Writes the time index's row for the current step on a writable node
/// (nothing on a read-only one, so nothing reaches its binlog): the
/// node's `@@gtid_binlog_pos` before the row, keyed by the server's
/// `UNIX_TIMESTAMP()` rounded down to the step. A row already there for
/// the step stays. Rows past [`FEED_TIME_KEEP_SECONDS`] go. Creates the
/// table on first use.
///
/// # Errors
/// The statements' (a missing privilege on `__sprig_sys`).
pub async fn feed_time_mark(session: &mut ProbeSession) -> Result<(), ProbeError> {
    let t = feed_time_table();
    let mark = format!(
        "INSERT IGNORE INTO {t} (bucket, pos, written_at) SELECT UNIX_TIMESTAMP() DIV \
         {FEED_TIME_STEP_SECONDS} * {FEED_TIME_STEP_SECONDS}, @@gtid_binlog_pos, \
         UTC_TIMESTAMP(6) FROM DUAL WHERE @@GLOBAL.read_only = 0"
    );
    match session.exec(&mark).await {
        Err(e) if e.errno == Some(1146) => {
            session.exec(&feed_time_ddl()).await?;
            session.exec(&mark).await?;
        }
        r => r?,
    }
    session
        .exec(&format!(
            "DELETE FROM {t} WHERE bucket < UNIX_TIMESTAMP() - {FEED_TIME_KEEP_SECONDS} \
             AND @@GLOBAL.read_only = 0 LIMIT 1000"
        ))
        .await
}

/// A scratch object of a feature probe: `__sprig_sys.__sprig_probe_<suffix>`.
#[must_use]
pub fn scratch(suffix: &str) -> String {
    format!(
        "{}.{}",
        quote(SYS_SCHEMA),
        quote(&internal_object(&format!("probe_{suffix}")))
    )
}

/// The statements that create the schema and the cluster's own tables.
/// Each is idempotent.
#[must_use]
pub fn ddl() -> [String; 3] {
    [
        format!(
            "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            quote(SYS_SCHEMA)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT UNSIGNED NOT NULL PRIMARY KEY, \
             v BIGINT UNSIGNED NOT NULL, written_at DATETIME(6) NOT NULL) ENGINE=InnoDB",
            probe_table()
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (writer_node VARCHAR(64) NOT NULL PRIMARY KEY, \
             seq BIGINT UNSIGNED NOT NULL, written_at DATETIME(6) NOT NULL) ENGINE=InnoDB",
            heartbeat_table()
        ),
    ]
}

/// Creates `__sprig_sys` and its tables if missing. Run on a writer: the
/// DDL replicates (or runs in TOI on Galera).
///
/// # Errors
/// A statement failed, typically for a missing privilege on `__sprig_sys`.
pub async fn ensure(session: &mut ProbeSession) -> Result<(), ProbeError> {
    for stmt in ddl() {
        session.exec(&stmt).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_come_from_sprig_common() {
        assert_eq!(probe_table(), "`__sprig_sys`.`__sprig_probe`");
        assert_eq!(heartbeat_table(), "`__sprig_sys`.`__sprig_heartbeat`");
        assert_eq!(feed_time_table(), "`__sprig_sys`.`__sprig_feed_time`");
        assert!(feed_time_ddl().contains("bucket BIGINT UNSIGNED NOT NULL PRIMARY KEY"));
        assert_eq!(scratch("f"), "`__sprig_sys`.`__sprig_probe_f`");
        assert!(ddl()[0].contains("utf8mb4_bin"));
    }
}
