//! Automatic indexing on the engine: the decision logic.
//!
//! ```text
//! every eval interval, per tenant (the gateway that hosts its engine
//! runs `pass::run` through its `Env`):
//!   1. watch     built indexes: used since built? the query patterns they
//!                serve faster? -> active, or dropped (unused, no_gain)
//!   2. trigger   the window's statement patterns that repeat and are slow
//!                or read far more rows than they return
//!   3. generate  candidate indexes from each pattern's shape: equality
//!                columns (most selective first), one range, the order or
//!                grouping columns, a covering variant
//!   4. validate  each pattern's recent statements planned without and
//!                with the candidate (a hypothetical index in the
//!                planner): kept only with a clear win and no regression
//!   5. cost      saved time against write cost; dampers (cooldown per
//!                pattern, daily budget, re-create block, one build per
//!                tenant, no builds while units move or fail over)
//!   6. act       mode auto: build online (absent, delete-only,
//!                write-only, backfill, public); mode recommend: an
//!                approval request, built once approved
//! ```
//!
//! - [`model`]: index specifications, their canonical form and
//!   `sprig_auto_<8 hex>` names, records and their states.
//! - [`candidates`]: triggers and candidate generation (pure).
//! - [`policy`]: the verdict over what-if estimates, the cost rule, the
//!   dampers and the watch rules (pure).
//! - [`store`]: the loop's records in the tenant catalog
//!   (`m/autoindex/...`).
//! - [`pass`]: one evaluation of one tenant over an [`pass::Env`].
//! - [`settings`]: the `auto_index.*` keys.
//!
//! The loop only ever creates, and only ever drops, indexes named
//! `sprig_auto_*` (origin `auto`); `user`, `system` and `migrated`
//! indexes are never dropped.

// Counters become estimates (far below 2^52); one pass and the generator
// follow the chapter's steps in order; recommendations are updated in place.
#![allow(
    clippy::cast_precision_loss,
    clippy::too_many_lines,
    clippy::needless_range_loop
)]

pub mod candidates;
pub mod model;
pub mod pass;
pub mod policy;
pub mod settings;
pub mod store;
