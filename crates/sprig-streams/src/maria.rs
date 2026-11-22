//! The MariaDB error mapper the remaining MariaDB-backed messaging code
//! (queue broadcasts, inherited messaging) shares.

use sprig_common::{SprigCode, SprigError};

/// A backend error as a [`SprigError`], retryable when the server or the
/// connection is the problem (and for 1146: a move's fence renames the
/// schema for up to the hold).
#[must_use]
pub fn map_err(e: mysql_async::Error) -> SprigError {
    if let mysql_async::Error::Server(s) = &e {
        let mut state = *b"HY000";
        if s.state.len() == 5 {
            state.copy_from_slice(s.state.as_bytes());
        }
        let mut out = SprigError::backend(s.code, state, s.message.clone());
        out.retryable = matches!(
            s.code,
            1205 | 1213 | 1146 | 1049 | 1927 | 1053 | 1290 | 1836
        );
        return out.with_source(e);
    }
    let mut out = SprigError::new(SprigCode::BackendUnavailable, format!("backend: {e}"));
    out.retryable = true;
    out.with_source(e)
}
