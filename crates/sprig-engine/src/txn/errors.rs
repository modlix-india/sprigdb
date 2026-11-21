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

/// `ER_QUERY_INTERRUPTED`: `KILL QUERY`.
pub const ER_QUERY_INTERRUPTED: u16 = 1317;
/// `ER_STATEMENT_TIMEOUT`: MariaDB's `max_statement_time`.
pub const ER_STATEMENT_TIMEOUT: u16 = 1969;
/// `ER_QUERY_TIMEOUT`: MySQL's `max_execution_time`.
pub const ER_QUERY_TIMEOUT: u16 = 3024;
/// `ER_XA_RBDEADLOCK`: a deadlock victim inside an XA transaction.
pub const ER_XA_RBDEADLOCK: u16 = 1614;
/// `ER_XA_RBTIMEOUT`: a lock wait timeout that rolled back an XA transaction.
pub const ER_XA_RBTIMEOUT: u16 = 1613;

/// 1317: the statement was killed while it waited.
#[must_use]
pub fn query_interrupted() -> SprigError {
    SprigError::standard(
        ER_QUERY_INTERRUPTED,
        *b"70100",
        "Query execution was interrupted",
    )
}

/// 1969 (MariaDB): the statement's time limit passed while it waited.
#[must_use]
pub fn statement_timeout_mariadb() -> SprigError {
    SprigError::standard(
        ER_STATEMENT_TIMEOUT,
        *b"70100",
        "Query execution was interrupted (max_statement_time exceeded)",
    )
}

/// 3024 (MySQL): the statement's time limit passed while it waited.
#[must_use]
pub fn statement_timeout_mysql() -> SprigError {
    SprigError::standard(
        ER_QUERY_TIMEOUT,
        *b"HY000",
        "Query execution was interrupted, maximum statement execution time exceeded",
    )
}

/// 1614: a deadlock victim inside an XA transaction.
#[must_use]
pub fn xa_deadlock() -> SprigError {
    SprigError::standard(
        ER_XA_RBDEADLOCK,
        *b"40001",
        "XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected",
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
