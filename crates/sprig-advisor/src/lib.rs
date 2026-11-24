//! `sprig-advisor`: the sharding advisor's and automatic indexing's
//! design logic.
//!
//! Layer: 2 operations.
//!
//! What exists: the sharding advisor's design logic over a workload and
//! schema input, [`sharding`] (kept for sharding big tenants across nodes),
//! and automatic indexing on the engine, [`autoindex`]: triggers,
//! candidates, the verdict over what-if plans, the cost rule, dampers,
//! watch and drop rules, its records and one pass of its loop (the gateway
//! hosting the engine runs it).

pub mod autoindex;
pub mod sharding;
