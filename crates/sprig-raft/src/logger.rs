//! raft-rs logs through slog; this forwards its records to tracing.

use slog::{Drain, Level, OwnedKVList, Record};

/// A slog drain writing to tracing under the `raft` target: errors and
/// critical records as warnings, everything else at debug or trace.
struct ToTracing;

impl Drain for ToTracing {
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record<'_>, _values: &OwnedKVList) -> Result<(), slog::Never> {
        match record.level() {
            Level::Critical | Level::Error => {
                tracing::warn!(target: "raft", "{}", record.msg());
            }
            Level::Warning | Level::Info => {
                if tracing::enabled!(target: "raft", tracing::Level::DEBUG) {
                    tracing::debug!(target: "raft", "{}", record.msg());
                }
            }
            Level::Debug | Level::Trace => {
                if tracing::enabled!(target: "raft", tracing::Level::TRACE) {
                    tracing::trace!(target: "raft", "{}", record.msg());
                }
            }
        }
        Ok(())
    }
}

/// The logger handed to every raft-rs group of a node.
pub(crate) fn logger(node: u64) -> slog::Logger {
    slog::Logger::root(ToTracing, slog::o!("node" => node))
}
