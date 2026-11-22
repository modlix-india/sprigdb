//! `sprig-advisor`: the sharding advisor's design logic.
//!
//! Layer: 2 operations.
//!
//! What exists: the sharding advisor's design logic over a workload and
//! schema input, [`sharding`] (kept for sharding big tenants across nodes),
//! and the two `auto_index.*` settings other code still reads,
//! [`autoindex`].

pub mod autoindex;
pub mod sharding;
