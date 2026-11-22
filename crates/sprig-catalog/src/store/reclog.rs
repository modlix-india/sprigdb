//! The catalog's record log (`rec_log`): module record writes committed
//! with the catalog version, until the engine has applied them
//! (`crate::records::staged`).

use std::time::Duration;

use mysql_async::prelude::Queryable;

use sprig_common::Result;

use super::Store;
use crate::sql::map_err;

/// `rec_log`: one row per catalog change that wrote module records.
pub const DDL_REC_LOG: &str = "CREATE TABLE IF NOT EXISTS rec_log (
  seq         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  version     BIGINT UNSIGNED NOT NULL,
  ops         LONGBLOB        NOT NULL,
  created_at  DATETIME(6)     NOT NULL,
  PRIMARY KEY (seq)
) ENGINE=InnoDB";

/// The internal key holding the last log sequence number purged.
pub const PURGED_KEY: &str = "sprig.rec_log_purged";

/// How long an applied log row stays, so loads whose engine view is a
/// little older than the purge rarely have to load again.
pub const KEEP: Duration = Duration::from_secs(60);

/// What a snapshot sees of the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogView {
    /// Rows after the asked sequence number, in order.
    pub rows: Vec<(u64, Vec<u8>)>,
    /// The highest sequence number in the log (0 when empty).
    pub last: u64,
    /// The last sequence number purged.
    pub purged: u64,
    /// Whether the catalog has the log table.
    pub present: bool,
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

impl Store {
    fn rec_log(&self) -> String {
        format!("{}.`rec_log`", sprig_common::names::quote(&self.schema))
    }

    /// The log rows after `after` (and the log's marks), read in `q`.
    /// `locking` reads them with `FOR UPDATE` (inside a change, which
    /// holds the catalog version row: every row committed before it is
    /// then seen). A catalog without the log table reads as an empty log.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn rec_log_after<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        after: u64,
        locking: bool,
    ) -> Result<LogView> {
        let lock = if locking { " FOR UPDATE" } else { "" };
        let rows: Vec<(u64, Vec<u8>)> = match q
            .exec(
                format!(
                    "SELECT seq, ops FROM {} WHERE seq > ? ORDER BY seq{lock}",
                    self.rec_log()
                ),
                (after,),
            )
            .await
        {
            Ok(r) => r,
            Err(e) if missing_table(&e) => return Ok(LogView::default()),
            Err(e) => return Err(map_err(e)),
        };
        let last: Option<Option<u64>> = q
            .exec_first(format!("SELECT MAX(seq) FROM {}", self.rec_log()), ())
            .await
            .map_err(map_err)?;
        let last = last.flatten();
        let purged = match self.internal(q, PURGED_KEY).await? {
            Some(v) => v.as_u64().unwrap_or(0),
            None => 0,
        };
        Ok(LogView {
            rows,
            last: last.unwrap_or(0),
            purged,
            present: true,
        })
    }

    /// Appends one change's writes at `version`; returns the row's
    /// sequence number.
    ///
    /// # Errors
    /// 39070 or a backend error (1146 when the catalog has no log table:
    /// the module's migration didn't run).
    pub async fn rec_log_append<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        version: u64,
        ops: &[u8],
    ) -> Result<u64> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (version, ops, created_at) VALUES (?, ?, UTC_TIMESTAMP(6))",
                self.rec_log()
            ),
            (version, ops),
        )
        .await
        .map_err(map_err)?;
        let seq: Option<u64> = q
            .exec_first("SELECT LAST_INSERT_ID()", ())
            .await
            .map_err(map_err)?;
        Ok(seq.unwrap_or(0))
    }

    /// Removes log rows the engine has applied (`applied`) that are older
    /// than [`KEEP`], recording the last one removed. Returns it, if any.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn rec_log_purge<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        applied: u64,
        version: u64,
        at: std::time::SystemTime,
    ) -> Result<Option<u64>> {
        let upto: Option<Option<u64>> = q
            .exec_first(
                format!(
                    "SELECT MAX(seq) FROM {} WHERE seq <= ? AND created_at < \
                     UTC_TIMESTAMP(6) - INTERVAL {} SECOND",
                    self.rec_log(),
                    KEEP.as_secs()
                ),
                (applied,),
            )
            .await
            .map_err(map_err)?;
        let Some(upto) = upto.flatten() else {
            return Ok(None);
        };
        q.exec_drop(
            format!("DELETE FROM {} WHERE seq <= ?", self.rec_log()),
            (upto,),
        )
        .await
        .map_err(map_err)?;
        self.put_internal(q, PURGED_KEY, &serde_json::json!(upto), version, at)
            .await?;
        Ok(Some(upto))
    }
}
