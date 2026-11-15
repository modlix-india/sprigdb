//! The administration agent's error intake: counts in every process,
//! incidents on the DC leader, triage and advice.
//!
//! ```text
//! gateway, controller, worker:  sprig_common::incidents counts errors by (code, span)
//!     -> intake: DC tier ai/intake/<process>, every minute, under a lease
//! controller (DC leader), each pass, while ai.admin.enabled:
//!     intake::Merger: what each process reported since the last pass
//!     record: ai/incidents/<fingerprint>; new, rising or critical -> pending
//!     pending, within ai.admin.max_triage_hour and ai.admin.max_advice_hour:
//!         advise::Advisor: triage (noise | needs_advice), then advice
//!         -> an `ai_incident_advice` item in the approvals inbox (dc level,
//!            advisory, emailed); repeats only refresh its counts
//! ```
//!
//! What leaves for the provider is the intake text of [`brief`]: codes,
//! components, spans, counts, versions and a topology summary. Never a
//! tenant, database, table or user name, a statement, a value or an
//! address: the text holds none, and every name that could appear is
//! registered with the egress gate's scrubber anyway.
//!
//! - [`intake`]: the per-process records and the merge;
//! - [`record`]: incidents, fingerprints and the rule that escalates;
//! - [`brief`]: the intake text, names, prompts, and reading answers;
//! - [`advise`]: the triage and advice turns.

pub mod advise;
pub mod brief;
pub mod intake;
pub mod record;

#[cfg(test)]
mod tests;

pub use advise::{Advisor, Case, INCIDENTS, Model, Report};
pub use brief::{Advice, ClusterShape, Names, Topology, Verdict};
pub use intake::{Flusher, IntakeRecord, Merger};
pub use record::{IncidentRecord, IncidentStore, Status, Why, fingerprint};
