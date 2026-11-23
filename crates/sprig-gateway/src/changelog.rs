//! The node's change log ([`sprig_streams::changes`]): every engine
//! unit's committed changes, written on the apply step beside the engine
//! (`<engine dir>/changes`), opened together with the engine so it sees
//! every entry from the start. Change consumers (MongoDB change streams on
//! engine collections) read it.
//!
//! `SPRIGDB_ENGINE_CHANGELOG=off` leaves it closed: those consumers then
//! refuse.

use std::sync::OnceLock;

use sprig_engine::Engine;
use sprig_streams::changes::{ChangeConfig, ChangeLog};

/// The variable that turns the change log off.
pub const OFF_VAR: &str = "SPRIGDB_ENGINE_CHANGELOG";

static LOG: OnceLock<Option<ChangeLog>> = OnceLock::new();

/// Opens the change log on the node's engine, once (the engine's
/// initialisation calls it).
pub(crate) fn open_on(e: &Engine) {
    LOG.get_or_init(|| {
        if std::env::var(OFF_VAR).is_ok_and(|v| v.eq_ignore_ascii_case("off")) {
            tracing::info!("change log off ({OFF_VAR}=off)");
            return None;
        }
        let dir = e.data_dir().join("changes");
        match ChangeLog::open(e, &dir, ChangeConfig::default()) {
            Ok(l) => {
                l.spawn_background();
                tracing::info!(dir = %dir.display(), "change log open");
                Some(l)
            }
            Err(err) => {
                tracing::error!(error = %err, "change log not started");
                None
            }
        }
    });
}

/// The node's change log, when the engine and the log are open.
#[must_use]
pub fn changes() -> Option<ChangeLog> {
    crate::node_engine::engine().ok()?;
    LOG.get().cloned().flatten()
}
