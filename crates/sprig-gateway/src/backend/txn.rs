//! The session's backend connection as a Sprig 2PC transaction branch:
//! [`sprig_txn::BranchConn`] on [`BackendSession`], so the coordinator
//! sends its control statements (`START TRANSACTION`, `XA START`, `XA END;
//! XA PREPARE`, the decision insert and `COMMIT`, savepoints) through the
//! same pinned connection the router's statements use.
//!
//! Not yet: the session holding one `TxnCoordinator` with a
//! `BackendSession` per cluster (a gateway `TxnBackend`), and the router's
//! executor calling it.

use sprig_common::ErrorKind;
use sprig_proto_mysql::client::PacketKind;
use sprig_txn::settings::SESSION_LOCK_WAIT_DEFAULT;
use sprig_txn::{BranchConn, Ctl, DbError, DbResult, RedoStmt};

use super::pins::{Pins, TxnSetting};
use super::state::SessionVars;
use super::{BackendSession, Effect, StmtClass, StmtInfo};

/// Sprig 2PC: a held branch's record of its writes on a
/// [`BackendSession`]: each text `INSERT`, `UPDATE`, `DELETE` or `REPLACE`
/// that succeeded, with its replay context (the branch's fixed clock,
/// `insert_id`, `sql_mode`, `time_zone`, the client character set), and
/// the first write it can't replay (the determinism rule; a
/// binary-protocol write, whose values aren't text here).
#[derive(Debug, Default)]
pub(crate) struct Recording {
    timestamp_us: u64,
    stmts: Vec<RedoStmt>,
    savepoints: Vec<(String, usize)>,
    unsafe_why: Option<String>,
    /// The statement in flight, recorded when it succeeds.
    pub(super) pending: Option<String>,
    /// The next binary-protocol statement with its bound values written
    /// in: recorded like a text statement.
    pub(super) bound: Option<String>,
    /// The coordinator's own statement is running: nothing is recorded.
    /// Still set after that statement was cut short (its future dropped),
    /// which makes the record unusable.
    paused: bool,
}

fn first_word(sql: &[u8]) -> String {
    let s = String::from_utf8_lossy(sql);
    s.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

impl Recording {
    /// Before a text statement.
    pub(super) fn before(&mut self, info: &StmtInfo, sql: &[u8]) {
        self.pending = None;
        if self.paused {
            return;
        }
        let w = first_word(sql);
        if matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE") {
            let text = String::from_utf8_lossy(sql).into_owned();
            if self.unsafe_why.is_none()
                && let Some(why) = sprig_txn::redo::unsafe_reason(&text)
            {
                self.unsafe_why = Some(why.to_owned());
            }
            self.pending = Some(text);
        } else if info.class == StmtClass::Write
            && matches!(w.as_str(), "CALL" | "LOAD" | "SET" | "HANDLER")
            && self.unsafe_why.is_none()
        {
            self.unsafe_why = Some(format!("a {w} statement that writes"));
        }
    }

    /// Before a binary-protocol statement.
    pub(super) fn before_binary(&mut self, info: &StmtInfo, sql: &[u8]) {
        self.pending = None;
        if self.paused {
            return;
        }
        if let Some(text) = self.bound.take() {
            // The values are written in: the same rules as a text write.
            self.before(info, text.as_bytes());
            return;
        }
        let w = first_word(sql);
        if (info.class == StmtClass::Write
            && matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE"))
            && self.unsafe_why.is_none()
        {
            self.unsafe_why = Some("a prepared (binary protocol) write".to_owned());
        }
    }

    /// A recorded statement succeeded.
    pub(super) fn record(
        &mut self,
        sql: String,
        affected: u64,
        insert_id: u64,
        vars: &SessionVars,
    ) {
        let update = first_word(sql.as_bytes()) == "UPDATE";
        let var = |n: &str| vars.vars.get(n).map(|v| (n.to_owned(), v.clone()));
        self.stmts.push(RedoStmt {
            // UPDATE counts depend on CLIENT_FOUND_ROWS: not checked.
            affected: if update { u64::MAX } else { affected },
            insert_id,
            vars: ["sql_mode", "time_zone"]
                .into_iter()
                .filter_map(var)
                .collect(),
            charset: vars.vars.get("character_set_client").cloned(),
            timestamp_us: Some(self.timestamp_us),
            sql,
        });
    }
}

/// The statement for the coordinator's next control steps, the settings it
/// changes (`true`) or puts back (`false`), and how many steps it covers.
///
/// A branch's connection is a fresh session's, whose own lock wait is the
/// server's default: the coordinator's "session value"
/// ([`SESSION_LOCK_WAIT_DEFAULT`]) after it lowered the lock wait goes
/// back as `DEFAULT`, and so does the clock. Both put back together are
/// one statement. Once nothing the coordinator set is left, the connection
/// matches a new one and returns to the pool.
fn ctl_statement(ctl: &[Ctl], pins: &Pins) -> (String, Vec<(TxnSetting, bool)>, usize) {
    let restores_wait = |c: &Ctl| {
        matches!(c, Ctl::LockWaitTimeout(n) if *n == SESSION_LOCK_WAIT_DEFAULT)
            && pins.txn_setting(TxnSetting::LockWait)
    };
    match ctl {
        [w, Ctl::SetTimestamp(None), ..] if restores_wait(w) => (
            "SET @@session.innodb_lock_wait_timeout = DEFAULT, @@session.timestamp = DEFAULT"
                .to_owned(),
            vec![(TxnSetting::LockWait, false), (TxnSetting::Clock, false)],
            2,
        ),
        [w, ..] if restores_wait(w) => (
            "SET @@session.innodb_lock_wait_timeout = DEFAULT".to_owned(),
            vec![(TxnSetting::LockWait, false)],
            1,
        ),
        [c @ Ctl::LockWaitTimeout(_), ..] => (c.sql(), vec![(TxnSetting::LockWait, true)], 1),
        [c @ Ctl::SetTimestamp(t), ..] => (c.sql(), vec![(TxnSetting::Clock, t.is_some())], 1),
        [c, ..] => (c.sql(), Vec::new(), 1),
        [] => (String::new(), Vec::new(), 1),
    }
}

fn db_error(e: &sprig_common::SprigError) -> DbError {
    match e.kind {
        ErrorKind::Backend { errno, .. } => DbError::new(errno, e.message.clone()),
        // Anything else (lost connection, pool, protocol): the outcome of
        // the statement is unknown.
        _ => DbError::lost(e.to_string()),
    }
}

#[async_trait::async_trait]
impl BranchConn for BackendSession {
    fn thread_id(&self) -> u64 {
        BackendSession::thread_id(self).map_or(0, |(_, t)| t)
    }

    async fn control(&mut self, ctl: &[Ctl]) -> DbResult<()> {
        let mut at = 0;
        while at < ctl.len() {
            let (sql, settings, steps) = ctl_statement(&ctl[at..], self.pins());
            let mut info = StmtInfo::classify(sql.as_bytes());
            if !settings.is_empty() {
                // The coordinator's settings pin as `TxnSettings` until it
                // puts them back, not as `Untracked` for the session's life.
                info.effects.retain(|e| !matches!(e, Effect::Untracked(_)));
            }
            // The coordinator's own statements (`SET timestamp`, the lock
            // wait, savepoints, the redo marker) aren't the branch's writes:
            // the record pauses for them (found by the gateway's live test
            // of Sprig 2PC: a recorded `SET timestamp` made every held
            // branch unreplayable). The record stays in place, paused: if
            // this call is cut short, the pause stays and the record is
            // refused at prepare rather than missing statements.
            if let Some(r) = self.redo.as_mut() {
                r.paused = true;
            }
            self.txn_ctl = Some(settings);
            let ran = async {
                self.query(&info, sql.as_bytes())
                    .await
                    .map_err(|e| db_error(&e))?;
                let mut err = None;
                while let Some(p) = self.next().await.map_err(|e| db_error(&e))? {
                    if let PacketKind::Err(e) = p.kind {
                        err = Some(DbError::new(e.code, e.message.clone()));
                    }
                }
                self.finish().await.map_err(|e| db_error(&e))?;
                Ok::<Option<DbError>, DbError>(err)
            }
            .await;
            self.txn_ctl = None;
            if let Some(r) = self.redo.as_mut() {
                r.paused = false;
            }
            if let Some(e) = ran? {
                return Err(e);
            }
            if let Some(r) = self.redo.as_mut() {
                for c in &ctl[at..at + steps] {
                    match c {
                        Ctl::Savepoint(n) => {
                            r.savepoints.retain(|(x, _)| x != n);
                            r.savepoints.push((n.clone(), r.stmts.len()));
                        }
                        Ctl::RollbackTo(n) => {
                            if let Some(&(_, len)) = r.savepoints.iter().find(|(x, _)| x == n) {
                                r.stmts.truncate(len);
                            }
                        }
                        _ => {}
                    }
                }
            }
            at += steps;
        }
        Ok(())
    }

    fn start_redo(&mut self, timestamp_us: u64) -> bool {
        if self.redo_off {
            return false;
        }
        self.redo = Some(Recording {
            timestamp_us,
            ..Recording::default()
        });
        true
    }

    fn take_redo(&mut self) -> Result<Vec<RedoStmt>, String> {
        let Some(r) = self.redo.take() else {
            return Ok(Vec::new());
        };
        if r.paused && r.unsafe_why.is_none() {
            return Err("a statement of the coordinator's on this branch was cut short".to_owned());
        }
        match r.unsafe_why {
            Some(why) => Err(why),
            None => Ok(r.stmts),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_coordinators_settings_go_back_as_defaults_in_one_statement() {
        let mut pins = Pins::default();
        let (sql, set, n) = ctl_statement(&[Ctl::LockWaitTimeout(10)], &pins);
        assert_eq!(
            (sql.as_str(), set, n),
            (
                "SET SESSION innodb_lock_wait_timeout = 10",
                vec![(TxnSetting::LockWait, true)],
                1
            )
        );
        // The cap may equal the session's value: it is still a change.
        let (_, set, _) = ctl_statement(&[Ctl::LockWaitTimeout(SESSION_LOCK_WAIT_DEFAULT)], &pins);
        assert_eq!(set, vec![(TxnSetting::LockWait, true)]);
        let (sql, set, _) = ctl_statement(&[Ctl::SetTimestamp(Some(1_700_000_000_123_456))], &pins);
        assert_eq!(sql, "SET timestamp = 1700000000.123456");
        assert_eq!(set, vec![(TxnSetting::Clock, true)]);

        pins.set_txn_setting(TxnSetting::LockWait, true);
        pins.set_txn_setting(TxnSetting::Clock, true);
        let ctl = [
            Ctl::Commit,
            Ctl::LockWaitTimeout(SESSION_LOCK_WAIT_DEFAULT),
            Ctl::SetTimestamp(None),
        ];
        assert_eq!(ctl_statement(&ctl, &pins), ("COMMIT".to_owned(), vec![], 1));
        let (sql, set, n) = ctl_statement(&ctl[1..], &pins);
        assert_eq!(
            sql,
            "SET @@session.innodb_lock_wait_timeout = DEFAULT, @@session.timestamp = DEFAULT"
        );
        assert_eq!(
            set,
            vec![(TxnSetting::LockWait, false), (TxnSetting::Clock, false)]
        );
        assert_eq!(n, 2);
        let (sql, set, n) = ctl_statement(&ctl[1..2], &pins);
        assert_eq!(sql, "SET @@session.innodb_lock_wait_timeout = DEFAULT");
        assert_eq!((set, n), (vec![(TxnSetting::LockWait, false)], 1));
        let (sql, set, _) = ctl_statement(&ctl[2..], &pins);
        assert_eq!(sql, "SET timestamp = DEFAULT");
        assert_eq!(set, vec![(TxnSetting::Clock, false)]);
        // Lowered again inside a transaction: a change, not a put-back.
        let (_, set, _) = ctl_statement(&[Ctl::LockWaitTimeout(5)], &pins);
        assert_eq!(set, vec![(TxnSetting::LockWait, true)]);
    }

    #[test]
    fn a_paused_record_records_nothing() {
        let mut r = Recording::default();
        let sql = b"UPDATE acct SET bal = bal - 1 WHERE id = 1";
        let info = StmtInfo::classify(sql);
        r.paused = true;
        r.before(&info, sql);
        assert_eq!(r.pending, None, "a coordinator statement isn't recorded");
        r.paused = false;
        r.before(&info, sql);
        assert!(r.pending.is_some());
    }

    #[test]
    fn the_coordinators_set_statements_classify_as_writes() {
        // They go to the writer (the pinned connection), never a reader.
        for sql in [
            "SET SESSION innodb_lock_wait_timeout = 10",
            "SET timestamp = 1700000000.123456",
            "SET @@session.innodb_lock_wait_timeout = DEFAULT, @@session.timestamp = DEFAULT",
        ] {
            assert_eq!(
                StmtInfo::classify(sql.as_bytes()).class,
                StmtClass::Write,
                "{sql}"
            );
        }
    }
}
