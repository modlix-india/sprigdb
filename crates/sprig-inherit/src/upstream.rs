//! Cross-tenant read-through edges.
//!
//! A tenant's root space hangs below its parent tenant's root. When the
//! parent shares object `db.t` with the child (`TO SPACE <child tenant>` or
//! `TO ALL CHILDREN`) and the child has declared its own `db.t`
//! inheritable with the same key, the child's top level reads the view
//!
//! ```sql
//! CREATE OR REPLACE DEFINER = 't<parent>_sprig_share'@'%'
//!   SQL SECURITY DEFINER VIEW `<child schema>`.`__sprig_share_<id>` AS
//!   SELECT <shared columns> FROM (<the parent root's merged view>) v
//!   WHERE <share filter>;
//! ```
//!
//! The view lives in the child's schema, so the child's accounts can
//! select it with the grants they already have, and runs as the parent's
//! **share definer** `t<parent>_sprig_share`: an account
//! SprigDB creates locked (nobody logs in as it) and grants `SELECT` only
//! on what the parent's share views read, the parent's layer table and,
//! in a chain, the parent's own share view of its parent. MariaDB itself
//! thus limits the view to the parent's shared tables; its text is the
//! share's subset. No user owns the account, so dropping or renaming the
//! user who made the share (once the definer, `t<parent>_u<id>`)
//! no longer breaks every child's view with 1449, and no policy of the
//! parent binds it. A client can't name the view (`__sprig_` objects are
//! reserved and hidden). The parent's merged view reads the parent's own
//! upstream view the same way, so a chain of tenants is a chain of views.
//!
//! The view is (re)created by the first statement that needs it on each
//! gateway, and again whenever its text changes (a filter, the columns),
//! so the parent never writes into the child's schema. Both tenants must
//! be on one cluster with single-shard databases; other placements need
//! the materialized or federated modes, which are not built (39889).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, OnceLock};

use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::names::share_definer_account;
use sprig_common::{Result, SprigCode, SprigError};

use crate::Runner;
use crate::model::{ROOT_SPACE, Share};
use crate::plan::{Ctx, Resolved, UpEdge, physical_schema, tenant_edge};
use crate::view::{Chain, Level, Top, ViewSql, q};

fn cache() -> &'static Mutex<HashMap<(u64, u64), u64>> {
    static C: OnceLock<Mutex<HashMap<(u64, u64), u64>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The view's name.
#[must_use]
pub fn view_name(share: &Share) -> String {
    format!("__sprig_share_{}", share.id)
}

/// The account the share views of `parent`'s shares run as
/// ([`sprig_common::names::share_definer_account`]), quoted for SQL.
#[must_use]
pub fn definer(parent: TenantId) -> String {
    format!("'{}'@'%'", share_definer_account(parent))
}

/// The statements making the share view of `up` in `lineage[j]`'s
/// schema, in order: the parent's share definer (created locked when
/// missing), its `SELECT` on what the view reads, the view.
///
/// # Errors
/// 39889 for sharded databases.
pub fn statements(
    lineage: &[&TenantCatalog],
    j: usize,
    db_name: &str,
    table: &str,
    up: &UpEdge,
    lctn: bool,
) -> Result<Vec<String>> {
    let (create, reads) = build(lineage, j, db_name, table, up, lctn)?;
    let who = definer(lineage[j - 1].tenant);
    let mut out = vec![format!("CREATE USER IF NOT EXISTS {who} ACCOUNT LOCK")];
    for (schema, name) in reads {
        out.push(format!(
            "GRANT SELECT ON {}.{} TO {who}",
            q(&schema),
            q(&name)
        ));
    }
    out.push(create);
    Ok(out)
}

/// The statement creating the share view of `up` in `child`'s schema
/// (without its definer's account and grants: [`statements`]).
///
/// # Errors
/// 39889 for sharded databases.
pub fn create_sql(
    lineage: &[&TenantCatalog],
    j: usize,
    db_name: &str,
    table: &str,
    up: &UpEdge,
    lctn: bool,
) -> Result<String> {
    build(lineage, j, db_name, table, up, lctn).map(|(sql, _)| sql)
}

/// The view's statement and the parent's relations it reads (schema,
/// name).
fn build(
    lineage: &[&TenantCatalog],
    j: usize,
    db_name: &str,
    table: &str,
    up: &UpEdge,
    lctn: bool,
) -> Result<(String, Vec<(String, String)>)> {
    let parent = lineage[j - 1];
    let ps = physical_schema(parent, db_name, lctn)?;
    let cs = physical_schema(lineage[j], db_name, lctn)?;
    let mut reads = vec![(ps.clone(), up.parent_obj.table.clone())];
    let top = if j >= 2
        && let Some(u2) = tenant_edge(lineage[j - 2], parent, db_name, table, lctn)?
    {
        reads.push((ps.clone(), view_name(&u2.share)));
        Top::Upstream {
            from: format!("{}.{}", q(&ps), q(&view_name(&u2.share))),
            columns: u2.columns,
            take: u2.take,
            origin: String::new(),
            filter: None,
        }
    } else {
        Top::Base
    };
    let chain = Chain {
        top,
        levels: vec![Level {
            space: ROOT_SPACE,
            name: String::new(),
            edge: None,
        }],
    };
    let body = ViewSql {
        o: &up.parent_obj,
        layer: format!("{}.{}", q(&ps), q(&up.parent_obj.table)),
        keys: None,
        origin: false,
        derived: Vec::new(),
    }
    .view(&chain);
    let cols = up
        .columns
        .iter()
        .map(|c| q(c))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "CREATE OR REPLACE DEFINER = {} SQL SECURITY DEFINER VIEW {}.{} AS SELECT {cols} FROM \
         ({body}) AS `__sprig_v` WHERE {}",
        definer(parent.tenant),
        q(&cs),
        q(&view_name(&up.share)),
        up.share
            .filter
            .as_deref()
            .map_or_else(|| "TRUE".to_owned(), |f| format!("({f})"))
    );
    Ok((sql, reads))
}

/// Makes sure every share view `r`'s merged view reads exists and is
/// current, top tenant first.
///
/// # Errors
/// 39889 for placements read-through can't serve; 39892; the backend's.
pub async fn ensure(ctx: &Ctx<'_>, run: &mut dyn Runner, r: &Resolved) -> Result<()> {
    if ctx.ancestors.is_empty() {
        return Ok(());
    }
    let mut lineage: Vec<&TenantCatalog> = ctx.ancestors.iter().map(AsRef::as_ref).collect();
    lineage.push(ctx.cat);
    let table = r.obj.table.clone();
    // Reads stop at a serving copy of the edge from the parent tenant: no
    // share view is read then, and across clusters none could be made
    // (the parent's schema is on another server).
    if let Some(up) = tenant_edge(
        lineage[lineage.len() - 2],
        ctx.cat,
        &r.db_name,
        &table,
        ctx.lctn,
    )? && up.share.mode == crate::model::MergeMode::Materialized
        && ctx
            .copies
            .ready(
                crate::copy::CopyKey {
                    tenant: ctx.cat.tenant,
                    object: r.obj.object_id,
                    share: crate::copy::UPSTREAM,
                },
                r.obj.version,
                up.share.version,
            )
            .is_some()
    {
        return Ok(());
    }
    // The contiguous edges ending at this tenant.
    let mut edges = Vec::new();
    let mut j = lineage.len() - 1;
    while j >= 1 {
        match tenant_edge(lineage[j - 1], lineage[j], &r.db_name, &table, ctx.lctn)? {
            Some(up) => edges.push((j, up)),
            None => break,
        }
        j -= 1;
    }
    for (j, up) in edges.into_iter().rev() {
        let sqls = statements(&lineage, j, &r.db_name, &table, &up, ctx.lctn)?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sqls.hash(&mut h);
        // The parent's catalog version too: a parent's `DROP SHARE` drops
        // the view on every child, and share ids are reused
        // (`MAX + 1`), so a view this gateway made for a dropped share of
        // the same id and text must be made again.
        lineage[j - 1].version.hash(&mut h);
        let sig = h.finish();
        let key = (lineage[j].tenant.0, up.share.id);
        let known = cache().lock().map_or(None, |c| c.get(&key).copied());
        if known == Some(sig) {
            continue;
        }
        for sql in &sqls {
            run.ctl(sql).await.map_err(|e| {
                SprigError::new(
                    SprigCode::ShareResidencyConflict,
                    format!(
                        "the share view of {}.{table} could not be created (are both tenants on \
                         one cluster?): {}",
                        r.db_name, e.message
                    ),
                )
            })?;
        }
        if let Ok(mut c) = cache().lock() {
            c.insert(key, sig);
        }
    }
    Ok(())
}

/// Forgets the views this gateway created for `tenant` (after a drop).
pub fn forget(tenant: u64) {
    if let Ok(mut c) = cache().lock() {
        c.retain(|(t, _), _| *t != tenant);
    }
}
