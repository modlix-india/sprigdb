//! What the lock table remembers about the last deadlock it found, for the
//! `LATEST DETECTED DEADLOCK` section of the engine status.
//!
//! The table knows keys, not tables or indexes, so the report carries the
//! raw keys and the SQL layer may render them with names. [`DeadlockReport`]'s
//! `Display` is a plain rendering with keys in hex, in the same order of
//! sections clients know from `SHOW ENGINE INNODB STATUS`.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::{LockMode, TxnId};

/// One transaction in a deadlock cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockMember {
    /// The transaction.
    pub txn: TxnId,
    /// The key its pending request is for.
    pub waits_for_key: Bytes,
    /// The mode it asked for.
    pub mode: LockMode,
    /// Whether the request is an insert waiting for gap locks.
    pub insert_intention: bool,
    /// The transactions that request waits for.
    pub blocked_by: Vec<TxnId>,
    /// Record locks it held.
    pub locks_held: usize,
    /// Undo entries it had written (with `locks_held`, its weight).
    pub undo_entries: usize,
}

/// A deadlock: the cycle, in waits-for order, and who was rolled back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockReport {
    /// When it was found.
    pub at: SystemTime,
    /// The cycle's transactions; each waits for the next, the last for the
    /// first.
    pub members: Vec<DeadlockMember>,
    /// The transaction rolled back.
    pub victim: TxnId,
}

/// `(year, month, day)` of a day count since 1970-01-01.
fn civil(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + u64::from(m <= 2), m, d)
}

impl fmt::Display for DeadlockReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self
            .at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let (y, mo, d) = civil(secs / 86_400);
        let rem = secs % 86_400;
        writeln!(
            f,
            "{y:04}-{mo:02}-{d:02} {:02}:{:02}:{:02} UTC",
            rem / 3600,
            rem % 3600 / 60,
            rem % 60
        )?;
        for (i, m) in self.members.iter().enumerate() {
            let n = i + 1;
            writeln!(f, "*** ({n}) TRANSACTION:")?;
            writeln!(
                f,
                "TRANSACTION {}, {} row lock(s), undo log entries {}",
                m.txn, m.locks_held, m.undo_entries
            )?;
            writeln!(f, "*** ({n}) WAITING FOR THIS LOCK TO BE GRANTED:")?;
            let mode = match m.mode {
                LockMode::Shared => "S",
                LockMode::Exclusive => "X",
            };
            let kind = if m.insert_intention {
                "insert intention"
            } else {
                "locks rec but not gap"
            };
            write!(f, "RECORD LOCKS key 0x")?;
            for b in &m.waits_for_key {
                write!(f, "{b:02x}")?;
            }
            writeln!(f, " trx id {} lock_mode {mode} {kind} waiting", m.txn)?;
            write!(f, "*** ({n}) BLOCKED BY TRANSACTION")?;
            for (j, b) in m.blocked_by.iter().enumerate() {
                write!(f, "{} {b}", if j == 0 { "" } else { "," })?;
            }
            writeln!(f)?;
        }
        let pos = self
            .members
            .iter()
            .position(|m| m.txn == self.victim)
            .map_or(0, |p| p + 1);
        write!(f, "*** WE ROLL BACK TRANSACTION ({pos})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_become_calendar_dates() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(59), (1970, 3, 1));
        assert_eq!(civil(11_016), (2000, 2, 29));
        assert_eq!(civil(20_777), (2026, 11, 20));
    }

    #[test]
    fn the_report_names_each_wait_and_the_victim() {
        let r = DeadlockReport {
            at: UNIX_EPOCH + std::time::Duration::from_secs(20_777 * 86_400 + 3723),
            members: vec![
                DeadlockMember {
                    txn: 7,
                    waits_for_key: Bytes::from_static(b"\x01\xab"),
                    mode: LockMode::Exclusive,
                    insert_intention: false,
                    blocked_by: vec![9],
                    locks_held: 2,
                    undo_entries: 5,
                },
                DeadlockMember {
                    txn: 9,
                    waits_for_key: Bytes::from_static(b"k"),
                    mode: LockMode::Exclusive,
                    insert_intention: true,
                    blocked_by: vec![7, 8],
                    locks_held: 1,
                    undo_entries: 0,
                },
            ],
            victim: 9,
        };
        let text = r.to_string();
        assert!(text.starts_with("2026-11-20 01:02:03 UTC\n"), "{text}");
        assert!(
            text.contains("*** (1) TRANSACTION:\nTRANSACTION 7, 2 row lock(s), undo log entries 5")
        );
        assert!(text.contains(
            "RECORD LOCKS key 0x01ab trx id 7 lock_mode X locks rec but not gap waiting"
        ));
        assert!(
            text.contains("RECORD LOCKS key 0x6b trx id 9 lock_mode X insert intention waiting")
        );
        assert!(text.contains("*** (2) BLOCKED BY TRANSACTION 7, 8\n"));
        assert!(text.ends_with("*** WE ROLL BACK TRANSACTION (2)"));
    }
}
