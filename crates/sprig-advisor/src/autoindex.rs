//! What is left of automatic indexing: two settings other code still reads.
//!
//! Automatic indexing built, validated and dropped MariaDB indexes on
//! tables' MariaDB shard tables. Engine tables have none, so the pipeline,
//! its store (`auto_indexes`, `auto_index_candidates`,
//! `auto_index_validations`, `index_origins`, `index_usage`) and its
//! `auto_index.*` keys are gone; [`settings`] keeps the job scheduler's
//! build-slot key and the sharding advisor's lookup threshold.

pub mod settings;
