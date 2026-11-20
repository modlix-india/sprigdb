//! The `InnoDB`-compatible errors the transaction layer raises, with the
//! numbers, SQLSTATEs and texts clients see.

use sprig_common::SprigError;

/// `ER_LOCK_WAIT_TIMEOUT`.
pub const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// `ER_LOCK_DEADLOCK`.
pub const ER_LOCK_DEADLOCK: u16 = 1213;
/// `ER_CHECKREAD`: a snapshot-isolation conflict (MariaDB 11.8).
pub const ER_CHECKREAD: u16 = 1020;
/// `ER_LOCK_TABLE_FULL`.
pub const ER_LOCK_TABLE_FULL: u16 = 1206;
/// `ER_LOCK_NOWAIT`: MySQL 8's error for `NOWAIT` (MariaDB uses 1205).
pub const ER_LOCK_NOWAIT: u16 = 3572;

/// 1205: lock wait timeout (also MariaDB's `NOWAIT`).
#[must_use]
pub fn lock_wait_timeout() -> SprigError {
    SprigError::standard(
        ER_LOCK_WAIT_TIMEOUT,
        *b"HY000",
        "Lock wait timeout exceeded; try restarting transaction",
    )
}

/// 3572: MySQL's `NOWAIT` refusal.
#[must_use]
pub fn lock_nowait() -> SprigError {
    SprigError::standard(
        ER_LOCK_NOWAIT,
        *b"HY000",
        "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.",
    )
}

/// 1213: deadlock victim.
#[must_use]
pub fn deadlock() -> SprigError {
    SprigError::standard(
        ER_LOCK_DEADLOCK,
        *b"40001",
        "Deadlock found when trying to get lock; try restarting transaction",
    )
}

/// 1020: the row changed since the transaction's snapshot. `table` is the
/// client-facing table name, or empty when the caller has none.
#[must_use]
pub fn check_read(table: &str) -> SprigError {
    SprigError::standard(
        ER_CHECKREAD,
        *b"HY000",
        format!("Record has changed since last read in table '{table}'"),
    )
}

/// 1206: the transaction's lock memory is exhausted.
#[must_use]
pub fn lock_table_full() -> SprigError {
    SprigError::standard(
        ER_LOCK_TABLE_FULL,
        *b"HY000",
        "The total number of locks exceeds the lock table size",
    )
}

/// The MySQL error number of `e`, if it is a standard or passed-through one.
#[must_use]
pub fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        sprig_common::ErrorKind::Standard { errno, .. }
        | sprig_common::ErrorKind::Backend { errno, .. } => Some(errno),
        sprig_common::ErrorKind::Sprig(_) => None,
    }
}

/// The log failed before the outcome was durable: the transaction did not
/// happen. Reported as 1213 so clients and ORMs retry it.
#[must_use]
pub fn outcome_lost() -> SprigError {
    deadlock()
}
