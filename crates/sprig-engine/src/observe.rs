//! Apply observers: consumers of committed log entries that keep their own
//! files beside the store (stream segment files).
//!
//! An observer sees every committed entry of every unit, in index order, on
//! the apply step, right after the store applied it. It writes what it
//! needs into its own files without syncing them; the log is its
//! write-ahead log. Its **log floor** is the highest index its files hold
//! durably: the engine's checkpoint never trims the log above a floor, so
//! after a crash the observer replays from its floor through
//! [`crate::Engine::observe`]. Floors are stored in the `local` column
//! family, synced, so they hold across restarts even before the observer
//! registers again.

use crate::log::{LogEntry, UnitId};

/// See the module docs.
pub trait ApplyObserver: Send + Sync {
    /// Called on the apply step with committed entries of `unit`,
    /// contiguous and in index order, after the store applied them. Every
    /// unit's entries come through here; each unit has its own index
    /// sequence. It must not wait for the log (the apply step is the log
    /// writer's thread). Failures are the observer's own to report to its
    /// callers; the engine goes on.
    fn applied(&self, unit: UnitId, entries: &[LogEntry]);
}
