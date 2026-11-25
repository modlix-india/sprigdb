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
//! **On engine databases** there is no MariaDB view and no account: the
//! share's rows are read in process. [`ensure`] keeps, per (child tenant,
//! share), the view's query ([`EngineShare`]): the parent root's merged
//! view, cut to the shared columns and the share's filter, naming the
//! parent's layer table (and, in a chain, the parent's own share) by the
//! parent's physical schema. The engine expands a reference to
//! `__sprig_share_<id>` in the child's schema into that query as a derived
//! table, and resolves the parent's names inside it in the parent's
//! keyspace (and nowhere else), with the parent's snapshot kept here.
//!
//! The view is (re)created by the first statement that needs it on each
//! gateway, and again whenever its text changes (a filter, the columns),
//! so the parent never writes into the child's schema. Both tenants must
//! be on one cluster with single-shard databases; other placements need
//! the materialized or federated modes, which are not built (39889).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

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

/// A share read in process: the query standing for the share view
/// `__sprig_share_<id>` in a child tenant's schema.
#[derive(Debug, Clone)]
pub struct EngineShare {
    /// The query: the shared columns of the parent root's merged view, the
    /// share's filter applied, naming the parent's relations by their
    /// physical schema.
    pub sql: String,
    /// The parent tenant's snapshot the query was built from: the engine
    /// resolves the parent's tables in it.
    pub parent: Arc<TenantCatalog>,
}

/// The kept queries by (child tenant, share).
type EngineShares = Mutex<HashMap<(u64, u64), Arc<EngineShare>>>;

fn engine_cache() -> &'static EngineShares {
    static C: OnceLock<EngineShares> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The in-process query of share `share` as child tenant `child` reads it,
/// as the last [`ensure`] on this gateway made it.
#[must_use]
pub fn engine_share(child: TenantId, share: u64) -> Option<Arc<EngineShare>> {
    engine_cache()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&(child.0, share))
        .cloned()
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
    let (create, _, reads) = build(lineage, j, db_name, table, up, lctn)?;
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
    build(lineage, j, db_name, table, up, lctn).map(|(sql, _, _)| sql)
}

/// A share view's statement, its query alone, and the parent's relations
/// it reads (schema, name).
type Built = (String, String, Vec<(String, String)>);

/// The view's statement and the parent's relations it reads (schema,
/// name).
fn build(
    lineage: &[&TenantCatalog],
    j: usize,
    db_name: &str,
    table: &str,
    up: &UpEdge,
    lctn: bool,
) -> Result<Built> {
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
    let select = format!(
        "SELECT {cols} FROM ({body}) AS `__sprig_v` WHERE {}",
        up.share
            .filter
            .as_deref()
            .map_or_else(|| "TRUE".to_owned(), |f| format!("({f})"))
    );
    let sql = format!(
        "CREATE OR REPLACE DEFINER = {} SQL SECURITY DEFINER VIEW {}.{} AS {select}",
        definer(parent.tenant),
        q(&cs),
        q(&view_name(&up.share)),
    );
    Ok((sql, select, reads))
}

/// Makes the engine know the share queries of the contiguous tenant edges
/// of `db.table` ending at the last tenant of `lineage` (its catalogs, root
/// first), as [`ensure`] does for a session's read: what a read of that
/// tenant's merged view as the system (the copy pass's source reads) needs
/// when it reaches its parent through a share.
///
/// # Errors
/// 39892; 39889 for placements read-through can't serve.
pub fn engine_lineage(
    lineage: &[Arc<TenantCatalog>],
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<()> {
    let refs: Vec<&TenantCatalog> = lineage.iter().map(AsRef::as_ref).collect();
    let mut kept = Vec::new();
    let mut j = refs.len().saturating_sub(1);
    while j >= 1 {
        let Some(up) = tenant_edge(refs[j - 1], refs[j], db, table, lctn)? else {
            break;
        };
        let (_, select, _) = build(&refs, j, db, table, &up, lctn)?;
        kept.push((
            (refs[j].tenant.0, up.share.id),
            Arc::new(EngineShare {
                sql: scoped_ctes(&select, up.share.id),
                parent: Arc::clone(&lineage[j - 1]),
            }),
        ));
        j -= 1;
    }
    engine_cache()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(kept);
    Ok(())
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
    if run.engine_shares() {
        // Read in process: each edge's query, with the parent's snapshot.
        let mut kept = Vec::with_capacity(edges.len());
        for (j, up) in &edges {
            let (_, select, _) = build(&lineage, *j, &r.db_name, &table, up, ctx.lctn)?;
            // Its common table expressions under names of their own: the
            // query is read inside the child's view, whose levels have the
            // same names.
            let select = scoped_ctes(&select, up.share.id);
            kept.push((
                (lineage[*j].tenant.0, up.share.id),
                Arc::new(EngineShare {
                    sql: select,
                    parent: Arc::clone(&ctx.ancestors[*j - 1]),
                }),
            ));
        }
        engine_cache()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(kept);
        return Ok(());
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

/// `sql` with the merged view's level names (`` `__sprig_l<n>` ``,
/// `` `__sprig_p<n>` ``) made `` `__sprig_s<share>l<n>` `` and
/// `` `__sprig_s<share>p<n>` ``.
fn scoped_ctes(sql: &str, share: u64) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut rest = sql;
    while let Some(at) = rest.find("`__sprig_") {
        out.push_str(&rest[..at]);
        let tail = &rest[at + "`__sprig_".len()..];
        let mut chars = tail.chars();
        match (chars.next(), chars.next()) {
            (Some(k @ ('l' | 'p')), Some(d)) if d.is_ascii_digit() => {
                out.push_str(&format!("`__sprig_s{share}{k}"));
                rest = &tail[1..];
            }
            _ => {
                out.push_str("`__sprig_");
                rest = tail;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Forgets the views this gateway created for `tenant` (after a drop).
pub fn forget(tenant: u64) {
    if let Ok(mut c) = cache().lock() {
        c.retain(|(t, _), _| *t != tenant);
    }
    engine_cache()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|(t, _), _| *t != tenant);
}

#[cfg(test)]
mod tests {
    #[test]
    fn share_queries_name_their_levels_apart() {
        let sql = "WITH `__sprig_l0` AS (SELECT 1 FROM `t` `__sprig_lyr`), `__sprig_p1` AS \
                   (SELECT 1 FROM `__sprig_l0`) SELECT * FROM `__sprig_p1`, `__sprig_share_3`";
        assert_eq!(
            super::scoped_ctes(sql, 7),
            "WITH `__sprig_s7l0` AS (SELECT 1 FROM `t` `__sprig_lyr`), `__sprig_s7p1` AS \
             (SELECT 1 FROM `__sprig_s7l0`) SELECT * FROM `__sprig_s7p1`, `__sprig_share_3`"
        );
    }
}
