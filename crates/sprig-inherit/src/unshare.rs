//! What a parent tenant's `DROP SHARE` does to the children the share served.
//!
//! After the share's catalog change commits, for every tenant edge the
//! share carried (its table, and the tables of its foreign-key group
//! through derived shares):
//!
//! ```text
//! each direct child the share served:
//!   DROP VIEW __sprig_share_<id> in the child's schema
//!   no other share of the table serves it now →
//!     its overrides (`ovr` rows of its root space) become `orphan` rows
//!     with every bit set: the child's own records, as they were written
//! each tenant below such a child:
//!   its overrides of records nobody above it (from that child down) still
//!   holds become `orphan` rows too, as the copy worker's orphan pass does
//! ```
//!
//! With the overrides, the records a child had hidden (`hide` rows) go:
//! they hid records of a share that no longer exists, and a later share
//! of the table would otherwise come with them still hidden. Below a cut
//! child, a grandchild's hides of records nobody from that child down
//! still holds go too. A materialized edge's copy (`__sprig_ih_<object>`)
//! and its state row go with the view; the copy worker only emptied it.
//!
//! Before, a child's override was hidden from the moment of the drop (its
//! level-0 view no longer had an upstream, and reads `own` and `orphan`
//! rows only), which looked like data loss, and the share view stayed in
//! the child's schema, pointing at the parent's dropped table, until the
//! child dropped the database.
//!
//! The work runs as the provisioning account on each tenant's cluster.
//! The share is already dropped when it runs, so a failure isn't
//! returned: every step can run again, and the drop runs the whole
//! cleanup up to [`ATTEMPTS`] times ([`Retry`]) before it
//! logs and counts the failure; the overrides then stay hidden (the
//! behaviour before).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use sprig_catalog::ddl::database_key;
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};

use sprig_catalog::TenantCatalog;

use crate::Runner;
use crate::copy::{STATE_TABLE, UPSTREAM, copy_table, key_text};
use crate::ids::TreeTable;
use crate::model::{ALL_BITS, InheritState, ROOT_SPACE, Share, ShareTarget};
use crate::plan::{Ctx, state_of};
use crate::view::q;

/// Keys per `IN (...)` list.
pub(crate) const CHUNK: usize = 500;

/// How many times a drop runs its cleanup before it gives up.
pub const ATTEMPTS: usize = 3;

/// The pause before each attempt after the first.
const RETRY_PAUSE: [Duration; 2] = [Duration::from_millis(200), Duration::from_secs(1)];

/// The retry policy of a drop's cleanup: [`Retry::again`] after each
/// failed attempt says whether to run it again, and after which pause.
#[derive(Debug, Default)]
pub struct Retry {
    attempts: usize,
}

impl Retry {
    /// After a failed attempt: the pause before the next one, or `None`
    /// once [`ATTEMPTS`] have run.
    pub fn again(&mut self) -> Option<Duration> {
        self.attempts += 1;
        (self.attempts < ATTEMPTS)
            .then(|| RETRY_PAUSE[(self.attempts - 1).min(RETRY_PAUSE.len() - 1)])
    }

    /// Attempts that failed so far.
    #[must_use]
    pub fn failed(&self) -> usize {
        self.attempts
    }
}

/// One tenant edge a dropped share carried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Carried {
    /// The parent's object.
    pub object: ObjectId,
    /// The share's id (a derived share's for a foreign-key group table).
    pub share: u64,
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
}

/// What [`after_drop`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unshared {
    /// Share views dropped (`DROP VIEW IF EXISTS` statements run).
    pub views: u64,
    /// Overrides that became the tenants' own (`orphan`) rows.
    pub orphaned: u64,
    /// Hide rows removed.
    pub unhidden: u64,
    /// Copy tables dropped.
    pub copies: u64,
}

/// The parent tenant whose share went, as it read before the drop: its
/// snapshot, its state with the share, and `lower_case_table_names`.
#[derive(Clone, Copy)]
pub struct Before<'a> {
    /// The parent's snapshot.
    pub cat: &'a TenantCatalog,
    /// Its inheritance state with the dropped share in it.
    pub state: &'a InheritState,
    /// `lower_case_table_names`.
    pub lctn: bool,
}

impl<'a> Before<'a> {
    /// The parent's own statement's context, read before its drop.
    #[must_use]
    pub fn of(ctx: &'a Ctx<'_>) -> Self {
        Self {
            cat: ctx.cat,
            state: ctx.state,
            lctn: ctx.lctn,
        }
    }

    fn db_name(&self, db: sprig_common::ids::DbId) -> Option<String> {
        self.cat.databases.get(db).map(|d| d.name.clone())
    }
}

/// The tenant edges `sh`, and the shares derived from it, carry: read
/// from the parent's state before the drop commits.
#[must_use]
pub fn carried(ctx: Before<'_>, sh: &Share) -> Vec<Carried> {
    ctx.state
        .shares()
        .filter(|s| {
            (s.id == sh.id || s.via == Some(sh.id))
                && s.from == ROOT_SPACE
                && matches!(s.to, ShareTarget::Tenant(_) | ShareTarget::AllChildren)
        })
        .filter_map(|s| {
            let o = ctx.state.object(s.object_id)?;
            Some(Carried {
                object: s.object_id,
                share: s.id,
                db: ctx.db_name(o.db)?,
                table: o.table.clone(),
            })
        })
        .collect()
}

/// The children's side of a committed `DROP SHARE` (module docs).
/// `ctx` is the parent as it read before the drop.
///
/// # Errors
/// The directory's, the cache's and the backends'.
pub async fn after_drop(
    ctx: Before<'_>,
    r: &mut dyn Runner,
    carried: &[Carried],
) -> Result<Unshared> {
    let mut out = Unshared::default();
    if carried.is_empty() {
        return Ok(out);
    }
    let parent = ctx.cat.tenant;
    let fresh = r.tenant_catalog(parent).await?;
    for c in carried {
        let mut tree = r.tree_tables(parent, &c.db, &c.table).await?;
        tree.retain(|t| t.tenant != parent && t.ancestors.contains(&parent));
        // Top first: a child's overrides are its own before its children's
        // are compared with them.
        tree.sort_by_key(|t| t.ancestors.len());
        let at: BTreeMap<TenantId, TreeTable> =
            tree.iter().map(|t| (t.tenant, t.clone())).collect();
        let mut cut: BTreeSet<TenantId> = BTreeSet::new();
        for t in &tree {
            let Some((table, key, object)) = table_of(r, t, c, ctx.lctn).await? else {
                continue;
            };
            if t.ancestors.last() == Some(&parent) {
                // A direct child: did the dropped share serve it?
                let before = ctx.state.tenant_edge(c.object, t.tenant).map(|s| s.id);
                if before != Some(c.share) {
                    continue;
                }
                r.ctl_query(
                    t.tenant,
                    &format!(
                        "DROP VIEW IF EXISTS {}.{}",
                        q(&t.schema),
                        q(&format!("__sprig_share_{}", c.share))
                    ),
                )
                .await?;
                out.views += 1;
                if state_of(&fresh)
                    .and_then(|s| s.tenant_edge(c.object, t.tenant))
                    .is_some()
                {
                    // Another share of the table serves it now.
                    continue;
                }
                cut.insert(t.tenant);
                out.orphaned += orphan(r, t.tenant, &table, &key, None).await?;
                out.unhidden += unhide(r, t.tenant, &table, &key, None).await?;
                out.copies += drop_copy(r, t.tenant, &t.schema, object).await?;
            } else if let Some(i) = t.ancestors.iter().position(|a| cut.contains(a)) {
                // Below a cut child: the levels from that child down.
                let above: Vec<&TreeTable> =
                    t.ancestors[i..].iter().filter_map(|a| at.get(a)).collect();
                let lost =
                    lost_keys(r, t.tenant, &table, &key, &above, c, ctx.lctn, "ovr", None).await?;
                for part in lost.chunks(CHUNK) {
                    out.orphaned += orphan(r, t.tenant, &table, &key, Some(part)).await?;
                }
                let lost =
                    lost_keys(r, t.tenant, &table, &key, &above, c, ctx.lctn, "hide", None).await?;
                for part in lost.chunks(CHUNK) {
                    out.unhidden += unhide(r, t.tenant, &table, &key, Some(part)).await?;
                }
            }
        }
    }
    // The parent's share definer reads a table only while a share to a
    // child tenant carries it.
    let after = state_of(&fresh);
    for c in carried {
        let still = after.is_some_and(|st| {
            st.shares().any(|s| {
                s.object_id == c.object
                    && s.from == ROOT_SPACE
                    && matches!(s.to, ShareTarget::Tenant(_) | ShareTarget::AllChildren)
            })
        });
        if still {
            continue;
        }
        let Ok(schema) = crate::plan::physical_schema(ctx.cat, &c.db, ctx.lctn) else {
            continue;
        };
        if let Err(e) = r
            .ctl_query(
                parent,
                &format!(
                    "REVOKE SELECT ON {}.{} FROM {}",
                    q(&schema),
                    q(&c.table),
                    crate::upstream::definer(parent)
                ),
            )
            .await
        {
            // 1141/1147: never granted (no child read through it).
            tracing::debug!(tenant = %parent, table = %c.table, error = %e.message, "share definer's grant not revoked");
        }
    }
    if out.orphaned > 0 {
        metrics::counter!("sprig_inherit_orphans_total", "policy" => "keep", "path" => "unshare")
            .increment(out.orphaned);
    }
    Ok(out)
}

/// The orphan pass across tenants over the current state, for table
/// `db.table` of the tenants at or below `top`: each tenant below that
/// inherits the table through tenant edges has its overrides (`ovr` rows of
/// its root space) of records that no level above it still holds (ignoring
/// share filters; a hide above hides) turned into `orphan` rows with every
/// bit set, as the copy worker's orphan pass does (its "keep"). Parents
/// first, so a child's new orphans count for its children. `only`: just
/// the overrides of these keys (SQL literals of the key's columns), the
/// keys whose rows changed somewhere in the tree; `None`: every override.
///
/// As the copy worker's pass, for a table with foreign keys to inheritable
/// tables, the new orphans first take, for each foreign key they inherit,
/// the value their view showed when the record left
/// ([`crate::orphan_refs::fill`]), then claim what they reference in the
/// reference index ([`crate::orphan_refs::claim`]), so a handover no longer
/// has to stand in for them; a record hierarchy's path-only overrides go
/// with their records. Every step can run again. Returns how many rows
/// became orphans.
///
/// # Errors
/// The directory's, the cache's and the statements'.
pub async fn orphan_pass(
    r: &mut dyn Runner,
    top: TenantId,
    db: &str,
    table: &str,
    lctn: bool,
    only: Option<&[Vec<String>]>,
) -> Result<u64> {
    if only.is_some_and(<[Vec<String>]>::is_empty) {
        return Ok(0);
    }
    let mut tree = r.tree_tables(top, db, table).await?;
    tree.sort_by_key(|t| t.ancestors.len());
    let at: BTreeMap<TenantId, TreeTable> = tree.iter().map(|t| (t.tenant, t.clone())).collect();
    let mut cats: BTreeMap<TenantId, std::sync::Arc<TenantCatalog>> = BTreeMap::new();
    for t in &tree {
        cats.insert(t.tenant, r.tenant_catalog(t.tenant).await?);
    }
    let c = Carried {
        object: ObjectId(0),
        share: 0,
        db: db.to_owned(),
        table: table.to_owned(),
    };
    let mut n = 0;
    for t in &tree {
        // The levels its records come through: its tenant edges upwards.
        let mut above: Vec<&TreeTable> = Vec::new();
        let mut child = t.tenant;
        for a in t.ancestors.iter().rev() {
            let (Some(pc), Some(cc), Some(y)) = (cats.get(a), cats.get(&child), at.get(a)) else {
                break;
            };
            if crate::plan::tenant_edge(pc, cc, db, table, lctn)?.is_none() {
                break;
            }
            above.push(y);
            child = *a;
        }
        if above.is_empty() {
            continue;
        }
        above.reverse();
        let Some((xtable, key, xid)) = table_of(r, t, &c, lctn).await? else {
            continue;
        };
        let lost = lost_keys(r, t.tenant, &xtable, &key, &above, &c, lctn, "ovr", only).await?;
        if lost.is_empty() {
            continue;
        }
        let xo = cats
            .get(&t.tenant)
            .and_then(|cat| state_of(cat))
            .and_then(|st| st.object(xid).cloned());
        let links = xo.as_ref().is_some_and(|o| !o.links.is_empty());
        if links && let Some(xo) = &xo {
            crate::orphan_refs::fill(r, t, xo, db, &above, &xtable, &lost, lctn).await?;
        }
        if let Some(xo) = xo.as_ref().filter(|o| crate::hier::has_paths(o)) {
            // A record hierarchy's path-only overrides go with their records
            // (found by a read, deleted by key: the engine's single-table
            // writes take plain comparisons).
            let cols = key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
            for part in lost.chunks(CHUNK) {
                let only: Vec<Vec<String>> = r
                    .ctl_query(
                        t.tenant,
                        &format!(
                            "SELECT {cols} FROM {xtable} WHERE `_sprig_space` = {} AND \
                             `_sprig_state` = 'ovr' AND `_sprig_ovr` & {} = 0 AND {}",
                            ROOT_SPACE.0,
                            !crate::hier::path_bits(xo),
                            key_pred(&key, part)
                        ),
                    )
                    .await?
                    .iter()
                    .map(|row| key_lits(row))
                    .collect();
                if only.is_empty() {
                    continue;
                }
                r.ctl_query(
                    t.tenant,
                    &format!(
                        "DELETE FROM {xtable} WHERE `_sprig_space` = {} AND `_sprig_state` = \
                         'ovr' AND {}",
                        ROOT_SPACE.0,
                        key_pred(&key, &only)
                    ),
                )
                .await?;
            }
        }
        let mut made = 0;
        for part in lost.chunks(CHUNK) {
            made += orphan(r, t.tenant, &xtable, &key, Some(part)).await?;
        }
        n += made;
        if made > 0
            && links
            && let Some(xo) = &xo
        {
            crate::orphan_refs::claim(r, t, xo, db, &xtable, &lost, lctn).await?;
        }
    }
    if n > 0 {
        metrics::counter!("sprig_inherit_orphans_total", "policy" => "keep", "path" => "pass")
            .increment(n);
    }
    Ok(n)
}

/// A failed sweep of a dropped share is tried again after this long.
const SWEEP_RETRY: Duration = Duration::from_secs(60);

/// Dropped shares this process swept, by (parent, share): `None` once the
/// cleanup ran, else when it last failed.
type Swept = std::collections::HashMap<(TenantId, u64), Option<std::time::Instant>>;

fn swept() -> &'static std::sync::Mutex<Swept> {
    static S: std::sync::OnceLock<std::sync::Mutex<Swept>> = std::sync::OnceLock::new();
    S.get_or_init(Default::default)
}

/// Whether [`sweep`] has work for share `id` of `parent` now.
fn due(parent: TenantId, id: u64) -> bool {
    match swept()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&(parent, id))
    {
        None => true,
        Some(None) => false,
        Some(Some(failed)) => failed.elapsed() >= SWEEP_RETRY,
    }
}

/// Whether `ctx`'s tenant or an ancestor has a dropped share whose
/// cleanup isn't recorded done and this process has yet to run
/// ([`sweep`]).
#[must_use]
pub fn sweep_due(ctx: &Ctx<'_>) -> bool {
    std::iter::once(ctx.cat)
        .chain(ctx.ancestors.iter().map(AsRef::as_ref))
        .any(|cat| state_of(cat).is_some_and(|st| st.dropping().any(|sh| due(cat.tenant, sh.id))))
}

/// The second half of `DROP SHARE`s whose first half committed but whose
/// cleanup in the children never ran or kept failing (a gateway stopped
/// in between): such a share stays as a `dropping` row in its parent's
/// catalog ([`InheritState::dropping`]), which no edge reads. A statement
/// of the parent or of a tenant below it that reaches the inheritance
/// layer runs the cleanup once per process ([`after_drop`] as the drop
/// would have, from the parent's state with the share put back; a failure
/// is tried again a minute later), and the parent's own statement then
/// removes the row. Every step can run again. The tree's copy worker
/// does the same without waiting for a statement ([`crate::sweep`]), for
/// trees nobody uses.
///
/// # Errors
/// The cleanup's (the share stays `dropping`).
pub async fn sweep(ctx: &Ctx<'_>, r: &mut dyn Runner) -> Result<()> {
    let cats: Vec<&TenantCatalog> = std::iter::once(ctx.cat)
        .chain(ctx.ancestors.iter().map(AsRef::as_ref))
        .collect();
    for cat in cats {
        let Some(st) = state_of(cat) else {
            continue;
        };
        let pending: Vec<Share> = st
            .dropping()
            .filter(|sh| due(cat.tenant, sh.id))
            .map(|sh| (**sh).clone())
            .collect();
        for sh in pending {
            let mark = |v: Option<std::time::Instant>| {
                swept()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert((cat.tenant, sh.id), v);
            };
            let u = match finish_dropped(cat, st, &sh, ctx.lctn, r).await {
                Ok(u) => u,
                Err(e) => {
                    mark(Some(std::time::Instant::now()));
                    return Err(e);
                }
            };
            tracing::info!(tenant = %cat.tenant, share = %sh.name, views = u.views, orphaned = u.orphaned, unhidden = u.unhidden, copies = u.copies, "a dropped share's cleanup ran after its drop was cut off");
            metrics::counter!("sprig_inherit_unshare_swept_total").increment(1);
            if cat.tenant == ctx.cat.tenant
                && let Err(e) = r
                    .commit(&crate::store::InheritChange {
                        purge_shares: vec![sh.id],
                        ..crate::store::InheritChange::default()
                    })
                    .await
            {
                mark(Some(std::time::Instant::now()));
                return Err(e);
            }
            mark(None);
        }
    }
    Ok(())
}

/// The children's side of dropped share `sh` of `cat`'s tenant, whose
/// state `st` holds it as a `dropping` row: [`after_drop`] as the drop
/// would have run it, from `st` with the share put back. Every step can
/// run again, so a second run (another gateway's sweep, the copy
/// worker's) finds nothing left to do. The `dropping` row stays: the
/// caller removes it once this returns.
///
/// # Errors
/// The cleanup's.
pub async fn finish_dropped(
    cat: &TenantCatalog,
    st: &InheritState,
    sh: &Share,
    lctn: bool,
    r: &mut dyn Runner,
) -> Result<Unshared> {
    let before = st.with_share(sh);
    let b = Before {
        cat,
        state: &before,
        lctn,
    };
    let carried = carried(b, sh);
    after_drop(b, r, &carried).await
}

/// Tenant `t`'s inheritable table of `c` (`schema`.`table`), its key and
/// its object.
async fn table_of(
    r: &mut dyn Runner,
    t: &TreeTable,
    c: &Carried,
    lctn: bool,
) -> Result<Option<(String, Vec<String>, ObjectId)>> {
    let cat = r.tenant_catalog(t.tenant).await?;
    let Some(st) = state_of(&cat) else {
        return Ok(None);
    };
    let Some(d) = cat.database(&database_key(&c.db, lctn)) else {
        return Ok(None);
    };
    Ok(st.object_named(d.db_id, &c.table, lctn).map(|o| {
        (
            format!("{}.{}", q(&t.schema), q(&o.table)),
            o.key.clone(),
            o.object_id,
        )
    }))
}

/// Removes tenant `t`'s hide rows (root space): all of them, or those of
/// `keys`. Returns how many went.
async fn unhide(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    keys: Option<&[Vec<String>]>,
) -> Result<u64> {
    let mut cond = format!(
        "`_sprig_space` = {} AND `_sprig_state` = 'hide'",
        ROOT_SPACE.0
    );
    if let Some(k) = keys {
        if k.is_empty() {
            return Ok(0);
        }
        let _ = write!(cond, " AND {}", key_pred(key, k));
    }
    let n = count(r, t, &format!("SELECT COUNT(*) FROM {table} WHERE {cond}")).await?;
    if n > 0 {
        r.ctl_query(t, &format!("DELETE FROM {table} WHERE {cond}"))
            .await?;
    }
    Ok(n)
}

/// The first value of a `COUNT(*)`.
async fn count(r: &mut dyn Runner, t: TenantId, sql: &str) -> Result<u64> {
    Ok(r.ctl_query(t, sql)
        .await?
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .and_then(|b| String::from_utf8_lossy(&b).parse::<u64>().ok())
        .unwrap_or(0))
}

/// Removes the copy of tenant `t`'s `object` from its parent tenant (a
/// materialized edge), with its state row: the whole copy table when no
/// other copy of the object (a share to a space) is left in it. Returns 1
/// when the table went.
async fn drop_copy(r: &mut dyn Runner, t: TenantId, schema: &str, object: ObjectId) -> Result<u64> {
    let copy = copy_table(object);
    let have: BTreeSet<String> = r
        .ctl_query(
            t,
            &format!(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND \
                 TABLE_NAME IN ({}, {})",
                key_text(Some(schema)),
                key_text(Some(&copy)),
                key_text(Some(STATE_TABLE))
            ),
        )
        .await?
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect();
    if !have.contains(&copy) {
        return Ok(0);
    }
    let copy = format!("{}.{}", q(schema), q(&copy));
    let others = if have.contains(STATE_TABLE) {
        let state = format!("{}.{}", q(schema), q(STATE_TABLE));
        r.ctl_query(
            t,
            &format!(
                "DELETE FROM {state} WHERE object_id = {} AND share_key = {UPSTREAM}",
                object.0
            ),
        )
        .await?;
        count(
            r,
            t,
            &format!(
                "SELECT COUNT(*) FROM {state} WHERE object_id = {}",
                object.0
            ),
        )
        .await?
    } else {
        0
    };
    if others > 0 {
        r.ctl_query(
            t,
            &format!("DELETE FROM {copy} WHERE `_sprig_share` = {UPSTREAM}"),
        )
        .await?;
        return Ok(0);
    }
    r.ctl_query(t, &format!("DROP TABLE IF EXISTS {copy}"))
        .await?;
    Ok(1)
}

/// `(k1, k2) IN ((..), ..)`, or `k IN (..)`.
pub(crate) fn key_pred(key: &[String], keys: &[Vec<String>]) -> String {
    let tuple = |v: &[String]| {
        if v.len() == 1 {
            v[0].clone()
        } else {
            format!("({})", v.join(", "))
        }
    };
    let cols: Vec<String> = key.iter().map(|k| q(k)).collect();
    format!(
        "{} IN ({})",
        tuple(&cols),
        keys.iter().map(|k| tuple(k)).collect::<Vec<_>>().join(", ")
    )
}

/// A result row's values as key literals.
fn key_lits(row: &[Option<Vec<u8>>]) -> Vec<String> {
    row.iter()
        .map(|v| key_text(v.as_deref().map(String::from_utf8_lossy).as_deref()))
        .collect()
}

/// Turns tenant `t`'s overrides (root space) into `orphan` rows: all of
/// them, or those of `keys`. Returns how many there were.
async fn orphan(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    keys: Option<&[Vec<String>]>,
) -> Result<u64> {
    let mut cond = format!(
        "`_sprig_space` = {} AND `_sprig_state` = 'ovr'",
        ROOT_SPACE.0
    );
    if let Some(k) = keys {
        if k.is_empty() {
            return Ok(0);
        }
        let _ = write!(cond, " AND {}", key_pred(key, k));
    }
    let n = r
        .ctl_query(t, &format!("SELECT COUNT(*) FROM {table} WHERE {cond}"))
        .await?
        .first()
        .and_then(|row| row.first().cloned().flatten())
        .and_then(|b| String::from_utf8_lossy(&b).parse::<u64>().ok())
        .unwrap_or(0);
    if n == 0 {
        return Ok(0);
    }
    r.ctl_query(
        t,
        &format!(
            "UPDATE {table} SET `_sprig_state` = 'orphan', `_sprig_ovr` = {ALL_BITS} WHERE {cond}"
        ),
    )
    .await?;
    Ok(n)
}

/// Tenant `t`'s overrides (`state` `ovr`), or hides (`hide`), whose record
/// no level of `above` (top first) still holds, ignoring share filters:
/// the copy worker's orphan test.
#[allow(clippy::too_many_arguments)]
async fn lost_keys(
    r: &mut dyn Runner,
    t: TenantId,
    table: &str,
    key: &[String],
    above: &[&TreeTable],
    c: &Carried,
    lctn: bool,
    state: &str,
    only: Option<&[Vec<String>]>,
) -> Result<Vec<Vec<String>>> {
    let cols = key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
    let base = format!(
        "SELECT {cols} FROM {table} WHERE `_sprig_space` = {} AND `_sprig_state` = '{state}'",
        ROOT_SPACE.0
    );
    let mut ovr: Vec<Vec<String>> = Vec::new();
    match only {
        None => ovr.extend(r.ctl_query(t, &base).await?.iter().map(|row| key_lits(row))),
        Some(keys) => {
            for part in keys.chunks(CHUNK) {
                let sql = format!("{base} AND {}", key_pred(key, part));
                ovr.extend(r.ctl_query(t, &sql).await?.iter().map(|row| key_lits(row)));
            }
        }
    }
    if ovr.is_empty() {
        return Ok(Vec::new());
    }
    let mut exists: BTreeMap<Vec<String>, bool> = ovr.into_iter().map(|k| (k, false)).collect();
    for y in above {
        let Some((ytable, _, _)) = table_of(r, y, c, lctn).await? else {
            continue;
        };
        let keys: Vec<Vec<String>> = exists.keys().cloned().collect();
        for part in keys.chunks(CHUNK) {
            let rows = r
                .ctl_query(
                    y.tenant,
                    &format!(
                        "SELECT {cols}, CAST(`_sprig_state` AS CHAR) FROM {ytable} WHERE \
                         `_sprig_space` = {} AND {}",
                        ROOT_SPACE.0,
                        key_pred(key, part)
                    ),
                )
                .await?;
            for row in rows {
                let Some((st, k)) = row.split_last() else {
                    continue;
                };
                let st = st
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default();
                if let Some(e) = exists.get_mut(&key_lits(k)) {
                    match st.as_ref() {
                        "hide" => *e = false,
                        "own" | "orphan" => *e = true,
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(exists
        .into_iter()
        .filter(|(_, e)| !e)
        .map(|(k, _)| k)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: a failed cleanup runs again, [`ATTEMPTS`] times in all,
    /// with growing pauses.
    #[test]
    fn cleanup_retries() {
        let mut r = Retry::default();
        assert_eq!(r.again(), Some(Duration::from_millis(200)));
        assert_eq!(r.again(), Some(Duration::from_secs(1)));
        assert_eq!(r.again(), None);
        assert_eq!(r.failed(), ATTEMPTS);
    }

    #[test]
    fn key_predicates() {
        assert_eq!(
            key_pred(&["id".into()], &[vec!["'1'".into()], vec!["'2'".into()]]),
            "`id` IN ('1', '2')"
        );
        assert_eq!(
            key_pred(
                &["a".into(), "b".into()],
                &[vec!["'1'".into(), "'x'".into()]]
            ),
            "(`a`, `b`) IN (('1', 'x'))"
        );
        assert_eq!(
            key_lits(&[Some(b"7".to_vec()), Some(b"o'k".to_vec())]),
            vec!["'7'".to_owned(), "'o''k'".to_owned()]
        );
    }
}
