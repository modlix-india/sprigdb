//! The executor's tests (`exec/tests.rs`) run a second time, on the
//! storage engine's store: every answer must equal the in-memory store's.

pub(super) use crate::engine::EngineTestStore as MemStore;
// What the tests name through `super`.
pub(super) use crate::exec::TraversalTier;

// The same file as `exec::tests`, on purpose: each run names its store.
#[allow(clippy::duplicate_mod)]
#[path = "exec/tests.rs"]
mod tests;
