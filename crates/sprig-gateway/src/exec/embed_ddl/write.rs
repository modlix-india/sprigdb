//! The embedded-column runner's own writes: an `UPDATE` that sets only an
//! embedded column's vector and its hidden hash, run while [`scope`]
//! holds those two names.
//!
//! While it is held (only the runner sets it; no statement or variable
//! can), the engine's write path:
//!
//! - refuses an assignment to any other column ([`check_sets`]), so the
//!   runner's exemption from row-level security can never write a user
//!   column;
//! - fires no trigger and no validation (`engine_sql_triggers` passes the
//!   statement on), so user code never runs with that exemption;
//! - leaves `ON UPDATE CURRENT_TIMESTAMP` columns as they are, so filling
//!   a vector isn't a change of the row's data (and can't cause another
//!   embedding);
//! - checks `CHECK`, `NOT NULL`, foreign keys and unique keys as always.

use std::future::Future;

use sprig_common::{Result, SprigError};

tokio::task_local! {
    static COLUMNS: Vec<String>;
}

/// Runs `fut` as the runner's write of `columns` (an embedded column and
/// its hash).
pub(crate) async fn scope<F: Future>(columns: Vec<String>, fut: F) -> F::Output {
    COLUMNS.scope(columns, fut).await
}

/// Whether the running statement is the runner's write.
#[must_use]
pub(crate) fn active() -> bool {
    COLUMNS.try_with(|_| ()).is_ok()
}

/// The error of an assignment the runner's write may not make.
fn refused(column: &str) -> SprigError {
    SprigError::internal(format!(
        "the embedded-column runner may only write its column and hash, not {column}"
    ))
}

/// While the runner's write runs: every column `sets` names must be one
/// it holds. Outside it, any column may be set.
///
/// # Errors
/// An internal error naming the first column it may not write.
pub(crate) fn check_sets<'a>(sets: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let Ok(allowed) = COLUMNS.try_with(Clone::clone) else {
        return Ok(());
    };
    check_against(&allowed, sets)
}

/// While the runner's write runs, any statement but a single-table
/// `UPDATE` (an insert, a replace, a delete, a multiple-table write): an
/// error naming it. Outside it: nothing.
///
/// # Errors
/// An internal error naming `what`.
pub(crate) fn refuse(what: &str) -> Result<()> {
    if active() {
        return Err(SprigError::internal(format!(
            "the embedded-column runner may only run a single-table UPDATE, not {what}"
        )));
    }
    Ok(())
}

fn check_against<'a>(allowed: &[String], sets: impl IntoIterator<Item = &'a str>) -> Result<()> {
    for c in sets {
        if !allowed.iter().any(|a| a.eq_ignore_ascii_case(c)) {
            return Err(refused(c));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_the_embedded_column_and_its_hash_may_be_written() {
        // Outside the runner's write: anything, and nothing is held.
        assert!(!active());
        assert!(check_sets(["body", "secret"]).is_ok());
        let cols = vec!["emb".to_owned(), "_sprig_emb_emb_hash".to_owned()];
        scope(cols, async {
            assert!(active());
            assert!(check_sets(["emb", "_SPRIG_EMB_EMB_HASH"]).is_ok());
            let e = check_sets(["emb", "owner"]).unwrap_err();
            assert!(e.to_string().contains("owner"), "{e}");
            assert!(check_sets(["body"]).is_err());
            assert!(refuse("a DELETE").is_err());
        })
        .await;
        assert!(refuse("a DELETE").is_ok());
        assert!(!active());
    }
}
