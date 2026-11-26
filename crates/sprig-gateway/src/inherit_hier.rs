//! Record hierarchies across tenants on engine databases: the copy task's
//! hierarchy pass ([`crate::inherit_orphans`]), what the copy worker's
//! hierarchy pass does on MariaDB databases.
//!
//! A child tenant's root space inherits its parent tenant's paths with the
//! records, and keeps path overrides only where its own tree differs (a
//! record it moved, hid or added). When a tenant above changes its tree,
//! those overrides (and the paths the child would inherit) have to follow.
//! The session that made the change checked the child tenants' structure
//! under their locks before it committed; this pass, run after it
//! committed, brings their paths up to date.
//!
//! ```text
//! pass(family = tree root, db, table; keys K or all), after the copy pass,
//! when the table carries a record hierarchy:
//!   each tenant x of the family that receives the table from its parent
//!   tenant (unbroken edges for the table up its line), parents first:
//!     the edge into x is materialized and its copy isn't serving, or a
//!     tenant above x was written in this pass (x's copy hasn't caught up
//!     with it yet): skipped; the next look does it (the writes above are
//!     changes, so their keys come back)
//!     x's hierarchy lock (the engine lock x's own structural changes take),
//!     waiting at most 5 s; then one engine transaction at READ COMMITTED:
//!       sprig_inherit::hier::reconcile_levels(K, or every row walked from
//!       each space's top rows): x's root space, then x's spaces below it
//!     committed; a conflict with x's own tree (39711-39715: a cycle or a
//!     missing parent the change's check couldn't see) is rolled back and
//!     leaves x's paths of K as they were, with a warning
//!   a lock that isn't free in time (or a deadlock) stops the pass: the
//!   keys are kept for the next look
//! ```
//!
//! Each tenant's paths are written in one transaction, so a process
//! stopped part-way leaves every tenant either reconciled or as it was; the
//! task's first look at a family after a start is a full pass, which walks
//! every tenant's whole tree.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::TenantId;
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_inherit::copy::{CopyKey, Family, KeyLit, UPSTREAM, load_family};
use sprig_inherit::model::MergeMode;
use sprig_inherit::plan::{Resolved, physical_schema, state_of, tenant_edge};
use sprig_inherit::view::q;

use crate::exec::inherit_tree::{
    EngineTx, copy_infos, current_copies, engine_copy_states, engine_hier_lock,
};
use crate::tenant::TenantServices;

/// How long the pass waits for a tenant's hierarchy lock (and for row
/// locks inside its transaction).
const LOCK_WAIT: Duration = Duration::from_secs(5);

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    /// Tenants whose paths were written.
    pub tenants: u64,
    /// Layer rows written (path overrides set or cleared).
    pub rows: u64,
    /// Tenants whose own tree conflicted with the change above.
    pub conflicts: u64,
    /// Tenants left for a later look (a copy behind).
    pub skipped: u64,
    /// A lock wasn't free in time: the keys stay for the next look.
    pub retry: bool,
}

/// Whether `e` is a lock wait that ran out or a deadlock.
fn lock_busy(e: &SprigError) -> bool {
    matches!(
        e.kind,
        ErrorKind::Standard {
            errno: 1205 | 1213,
            ..
        } | ErrorKind::Backend {
            errno: 1205 | 1213,
            ..
        }
    )
}

/// Whether some family tenant's object `db.table` carries a record
/// hierarchy's paths (by the catalogs this process holds).
fn has_hierarchy(
    svc: &TenantServices,
    tenants: &[TenantId],
    db: &str,
    table: &str,
    lctn: bool,
) -> bool {
    tenants.iter().any(|t| {
        svc.cache.cached(*t).is_some_and(|cat| {
            let Some(st) = state_of(&cat) else {
                return false;
            };
            cat.database(&database_key(db, lctn))
                .and_then(|d| st.object_named(d.db_id, table, lctn))
                .is_some_and(|o| sprig_inherit::hier::has_paths(o))
        })
    })
}

/// The catalogs from the top of `x`'s line of tenant edges for `db.table`
/// down to `x` (just `x` when it receives nothing from its parent).
fn line(
    family: &Family,
    x: TenantId,
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<Vec<Arc<TenantCatalog>>> {
    let mut cats = family.ancestors.get(&x).cloned().unwrap_or_default();
    let Some(me) = family.cats.get(&x) else {
        return Ok(Vec::new());
    };
    cats.push(Arc::clone(me));
    let mut j = cats.len() - 1;
    while j >= 1 && tenant_edge(&cats[j - 1], &cats[j], db, table, lctn)?.is_some() {
        j -= 1;
    }
    Ok(cats.split_off(j))
}

/// The hierarchy pass of one family (module docs). `tenants`: the tenants
/// holding the table; `keys`: the keys whose rows changed somewhere in the
/// family (`None`: every row).
///
/// # Errors
/// The catalog's, the records' and the engine's; the task's next look is
/// then a full pass.
#[allow(clippy::too_many_lines)] // the steps for one tenant, in order
pub(crate) async fn pass(
    svc: &Arc<TenantServices>,
    root: TenantId,
    tenants: Vec<TenantId>,
    db: &str,
    table: &str,
    lctn: bool,
    keys: Option<&[KeyLit]>,
) -> Result<Counts> {
    let mut counts = Counts::default();
    if keys.is_some_and(<[KeyLit]>::is_empty) || !has_hierarchy(svc, &tenants, db, table, lctn) {
        return Ok(counts);
    }
    let env = crate::inherit_copies::Env {
        svc: Arc::clone(svc),
        tenants,
    };
    let family = load_family(&env, root).await?;
    // The copies a reader may use now: a tenant reading its parent through
    // a serving copy reconciles from it, as its statements read.
    let mut rows = Vec::new();
    for t in &family.tenants {
        let Some(cat) = family.cats.get(t) else {
            continue;
        };
        let parent = family
            .ancestors
            .get(t)
            .and_then(|a| a.last())
            .map(AsRef::as_ref);
        rows.extend(current_copies(
            engine_copy_states(svc, *t).await?,
            cat,
            parent,
        ));
    }
    let copies = copy_infos(rows);
    let lits: Option<Vec<String>> = keys.map(|ks| ks.iter().map(KeyLit::tuple).collect());
    let mut order = family.tenants.clone();
    order.sort_by_key(|t| family.depth(*t));
    let mut written: BTreeSet<TenantId> = BTreeSet::new();
    for x in order {
        let line = line(&family, x, db, table, lctn)?;
        if line.len() < 2 {
            continue;
        }
        let Some(x_cat) = family.cats.get(&x) else {
            continue;
        };
        let Some(st) = state_of(x_cat) else {
            continue;
        };
        let Some(d) = x_cat.database(&database_key(db, lctn)) else {
            continue;
        };
        let Some(xo) = st.object_named(d.db_id, table, lctn).cloned() else {
            continue;
        };
        if !sprig_inherit::hier::has_paths(&xo) {
            continue;
        }
        let Some(up) = tenant_edge(&line[line.len() - 2], x_cat, db, table, lctn)? else {
            continue;
        };
        if up.share.mode == MergeMode::Materialized {
            let serving = copies
                .ready(
                    CopyKey {
                        tenant: x,
                        object: xo.object_id,
                        share: UPSTREAM,
                    },
                    xo.version,
                    up.share.version,
                )
                .is_some();
            if !serving || line.iter().any(|c| written.contains(&c.tenant)) {
                counts.skipped += 1;
                continue;
            }
        }
        // The share queries x's merged view reads up its line.
        let mut lineage = family.ancestors.get(&x).cloned().unwrap_or_default();
        lineage.push(Arc::clone(x_cat));
        sprig_inherit::upstream::engine_lineage(&lineage, db, table, lctn)?;
        let Some(mut ctx) = family.ctx(x, &copies) else {
            continue;
        };
        ctx.lctn = lctn;
        // Paths here must follow the committed change whatever its size.
        ctx.limits.max_move_rows = u64::MAX;
        let Some(h) = sprig_inherit::hier::hierarchy_of(&ctx, &xo) else {
            continue;
        };
        let schema = physical_schema(x_cat, &d.name, lctn)?;
        let res = Resolved {
            db: xo.db,
            db_name: d.name.clone(),
            layer: format!("{}.{}", q(&schema), q(&xo.table)),
            prefix: format!("{}.", q(&schema)),
            derived: ctx.derived(&xo),
            obj: Arc::clone(&xo),
        };
        let started = std::time::Instant::now();
        let lock = match engine_hier_lock(x, &schema, xo.object_id.0, LOCK_WAIT).await {
            Ok(l) => l,
            Err(e) if lock_busy(&e) => {
                tracing::debug!(tenant = %x, table, "a tenant's hierarchy lock is busy; its paths wait for the next look");
                counts.retry = true;
                break;
            }
            Err(e) => return Err(e),
        };
        let mut tx = EngineTx::begin(Arc::clone(x_cat), lctn, LOCK_WAIT)?;
        let outcome =
            sprig_inherit::hier::reconcile_levels(&ctx, &mut tx, &res, &h, lits.as_deref()).await;
        match outcome {
            Ok(n) => {
                tx.commit().await?;
                drop(lock);
                if n > 0 {
                    written.insert(x);
                    counts.tenants += 1;
                    counts.rows += n;
                    metrics::counter!("sprig_inherit_hierarchy_tenant_reconciles_total")
                        .increment(1);
                    tracing::info!(tenant = %x, table, rows = n, full = keys.is_none(), ms = started.elapsed().as_millis(), "child tenant's hierarchy paths reconciled");
                }
            }
            Err(e) if sprig_inherit::hier::tenants::is_conflict(&e) => {
                tx.rollback();
                drop(lock);
                counts.conflicts += 1;
                metrics::counter!("sprig_inherit_hierarchy_conflicts_total").increment(1);
                tracing::warn!(tenant = %x, table, error = %e.message,
                    "a change above conflicts with this tenant's own tree; its paths of these records stay as they were");
            }
            Err(e) if lock_busy(&e) => {
                tx.rollback();
                drop(lock);
                counts.retry = true;
                break;
            }
            Err(e) => {
                tx.rollback();
                drop(lock);
                return Err(e);
            }
        }
    }
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_waits_and_deadlocks_are_retried() {
        assert!(lock_busy(&SprigError::standard(1205, *b"HY000", "wait")));
        assert!(lock_busy(&SprigError::standard(
            1213, *b"40001", "deadlock"
        )));
        assert!(!lock_busy(&SprigError::standard(1062, *b"23000", "dup")));
        assert!(!lock_busy(&SprigError::internal("x")));
    }
}
