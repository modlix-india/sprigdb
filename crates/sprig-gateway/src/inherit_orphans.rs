//! Orphans across tenants and materialized copies on engine databases,
//! kept from the node's change log (what the copy worker's orphan and copy
//! passes do on MariaDB databases).
//!
//! ```text
//! spawn(services): one task per process
//!   every tick (1 s; SPRIGDB_INHERIT_ORPHANS_TICK_MS), for each tenant whose
//!   catalog this process has loaded, whose databases are all the engine's
//!   and whose unit is led here (or has no single leader):
//!     each inheritable table it shares to child tenants (from its root
//!     space, to a tenant or to all children), or shares materialized to
//!     anyone, names a family: the tree root, the database and the table
//!   each family's tenants holding the table (looked up again every 30 s,
//!   and after any catalog change this process loads)
//!   are watched: their table's changes in the change log since the last
//!   look (a reader per tenant and family, from where the task first saw it)
//!   the keys of the changed rows are gathered; a family seen for the first
//!   time, a reader whose place the log no longer holds, a key whose text
//!   can't stand for it, or a failed pass makes the next pass a full one
//!   a pass, as the system, in transactions of its own:
//!     the orphan pass (sprig_inherit::unshare::orphan_pass) over the
//!     gathered keys' overrides, or all of them
//!     the copy pass (crate::inherit_copies) when the family has a
//!     materialized share: copies seeded or brought up to their sources for
//!     the gathered keys, or all of them
//! ```
//!
//! Both passes recompute from the current state, so a change seen twice is
//! harmless, and one the task missed (a lost reader place, a process that
//! stopped) is caught by the full pass that follows. A key changed in any
//! tenant of the family is looked at in every tenant (a superset: the
//! passes only act where something is out of date). No feed subscription
//! and no worker job are involved: the gateway that hosts the tenants'
//! engine runs it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_inherit::RawRows;
use sprig_inherit::copy::KeyLit;
use sprig_inherit::ids::TreeTable;
use sprig_inherit::model::{MergeMode, ROOT_SPACE, ShareTarget};
use sprig_types::{Done, Rows, TypesChange};

use crate::node_engine::{self, UnitLeader};
use crate::table_changes::Watched;
use crate::tenant::TenantServices;

/// The tick, in milliseconds (tests shorten it).
const TICK_VAR: &str = "SPRIGDB_INHERIT_ORPHANS_TICK_MS";

/// How long a family's list of tenants is kept before it is looked up again.
const TREE_TTL: Duration = Duration::from_secs(30);

/// A family: the tree root, the logical database and the table.
type Family = (u64, String, String);

/// What the task knows of one family.
struct Known {
    /// The tenants holding the table, and when they were looked up.
    tree: Vec<TreeTable>,
    looked: Instant,
    /// Each watched tenant's place in the change log (a resume token).
    tokens: HashMap<u64, String>,
    /// A pass is due (first seen, a change, or the last pass failed).
    dirty: bool,
    /// The due pass is a full one (else it covers `keys`).
    full: bool,
    /// The keys of the rows changed since the last pass, as SQL literals
    /// of the key's columns.
    keys: BTreeSet<Vec<String>>,
}

/// Starts the task.
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    // This process's part of the copies' guard stamps: a later process's
    // writes win over a stale one's.
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() & 0xFFFF_FFFF);
    let tick = std::env::var(TICK_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(1), Duration::from_millis);
    let mut loads = svc.cache.loads();
    tokio::spawn(async move {
        let mut tenants: BTreeSet<u64> = BTreeSet::new();
        let mut known: BTreeMap<Family, Known> = BTreeMap::new();
        let mut last_load = Instant::now();
        let mut timer = tokio::time::interval(tick);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                l = loads.recv() => {
                    if let Ok(l) = l {
                        tenants.insert(l.tenant.0);
                    }
                    // A catalog changed: a tenant may have adopted a table
                    // (joined a family), so the families' tenants are
                    // looked up again.
                    last_load = Instant::now();
                    continue;
                }
                _ = timer.tick() => {}
            }
            let families = families(&svc, &tenants);
            known.retain(|f, _| families.contains(f));
            for f in families {
                if let Err(e) = Box::pin(look(&svc, &f, &mut known, epoch, last_load)).await {
                    tracing::warn!(tenant = f.0, db = %f.1, table = %f.2, error = %e, "orphans and copies across tenants not kept up this time");
                    if let Some(k) = known.get_mut(&f) {
                        k.dirty = true;
                        k.full = true;
                    }
                }
            }
        }
    })
}

/// The families of the loaded tenants led here.
fn families(svc: &TenantServices, tenants: &BTreeSet<u64>) -> BTreeSet<Family> {
    let mut out = BTreeSet::new();
    for &t in tenants {
        if !matches!(
            node_engine::tenant_leader(t),
            UnitLeader::Here | UnitLeader::Any
        ) {
            continue;
        }
        let Some(cat) = svc.cache.cached(TenantId(t)) else {
            continue;
        };
        if crate::engine::catalog::of(&cat).is_none()
            || !crate::exec::inherit_tree::all_engine(&cat)
        {
            continue;
        }
        let Some(st) = sprig_inherit::plan::state_of(&cat) else {
            continue;
        };
        let root = cat.ancestry.first().map_or(t, |a| a.id.0);
        for s in st.shares() {
            let across = s.from == ROOT_SPACE
                && matches!(s.to, ShareTarget::Tenant(_) | ShareTarget::AllChildren);
            if !across && s.mode != MergeMode::Materialized {
                continue;
            }
            let Some(o) = st.object(s.object_id) else {
                continue;
            };
            let Some(db) = cat.databases.get(o.db).map(|d| d.name.clone()) else {
                continue;
            };
            out.insert((root, db, o.table.clone()));
        }
    }
    out
}

/// One look at family `f`: its tenants' changes, then the passes when due.
#[allow(clippy::many_single_char_names, clippy::too_many_lines)] // f, k: the family and what is known of it; the steps in order
async fn look(
    svc: &Arc<TenantServices>,
    f: &Family,
    known: &mut BTreeMap<Family, Known>,
    epoch: u64,
    last_load: Instant,
) -> Result<()> {
    let root = TenantId(f.0);
    let root_cat = svc
        .cache
        .get(root, Freshness::Cached { min_version: 0 })
        .await?;
    let lctn = crate::inherited::lctn(svc, &root_cat);
    let stale = known
        .get(f)
        .is_none_or(|k| k.looked.elapsed() >= TREE_TTL || k.looked < last_load);
    if stale {
        let tree = crate::exec::inherit_tree::tree_tables(svc, root, &f.1, &f.2, lctn).await?;
        let k = known.entry(f.clone()).or_insert_with(|| Known {
            tree: Vec::new(),
            looked: Instant::now(),
            tokens: HashMap::new(),
            dirty: true,
            full: true,
            keys: BTreeSet::new(),
        });
        k.tree = tree;
        k.looked = Instant::now();
    }
    let Some(k) = known.get_mut(f) else {
        return Ok(());
    };
    if let (Some(log), Ok(eng)) = (crate::changelog::changes(), crate::engine::engine()) {
        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(i64::MAX);
        for tt in &k.tree {
            let Some(cat) = svc.cache.cached(tt.tenant) else {
                continue;
            };
            let Some(w) = watched(&cat, &f.1, &f.2, lctn) else {
                continue;
            };
            let key_cols = key_columns(&cat, &w.1, &f.1, &f.2, lctn);
            let tables: HashMap<u32, Watched> = std::iter::once(w).collect();
            let token = k.tokens.get(&tt.tenant.0).cloned();
            // A tenant first seen starts at now: the full pass due for a new
            // family (or tenant) covers what came before.
            if token.is_none() {
                k.dirty = true;
                k.full = true;
            }
            let consumer = format!("inherit-orphans:{}:{}:{}:{}", f.0, f.1, f.2, tt.tenant.0);
            let batch = crate::table_changes::read(
                eng,
                &log,
                tt.tenant.0,
                &tables,
                token.as_deref(),
                if token.is_none() { now_ms } else { 0 },
                crate::table_changes::PAGE_BYTES,
                Duration::ZERO,
                &consumer,
            )
            .await;
            match batch {
                Ok(b) => {
                    for e in &b.events {
                        k.dirty = true;
                        match key_columns_of(key_cols.as_deref(), e) {
                            Some(lits) => k.keys.extend(lits),
                            None => k.full = true,
                        }
                    }
                    k.tokens.insert(tt.tenant.0, b.next);
                }
                Err(e) => {
                    // A place the log no longer holds: start again from now,
                    // with a full pass.
                    tracing::debug!(tenant = %tt.tenant, error = %e, "change log place lost");
                    k.tokens.remove(&tt.tenant.0);
                    k.dirty = true;
                    k.full = true;
                }
            }
        }
    } else {
        // No change log on this node: every look is a full pass.
        k.dirty = true;
        k.full = true;
    }
    if !k.dirty {
        return Ok(());
    }
    let keys: Option<Vec<Vec<String>>> = (!k.full).then(|| k.keys.iter().cloned().collect());
    tracing::debug!(root = f.0, db = %f.1, table = %f.2, full = k.full, keys = k.keys.len(), "inheritance passes due");
    let mut tenants: Vec<TreeTable> = k.tree.clone();
    tenants.sort_by_key(|t| t.ancestors.len());
    let copies = tenants.iter().any(|tt| {
        svc.cache
            .cached(tt.tenant)
            .is_some_and(|cat| materialized(&cat, &f.1, &f.2, lctn))
    });
    let mut r = SysRunner {
        svc: Arc::clone(svc),
        lctn,
    };
    let n = sprig_inherit::unshare::orphan_pass(&mut r, root, &f.1, &f.2, lctn, keys.as_deref())
        .await?;
    if n > 0 {
        tracing::info!(root = %root, db = %f.1, table = %f.2, orphaned = n, "overrides of records gone above became orphans");
    }
    if copies {
        let lits: Option<Vec<KeyLit>> = keys
            .as_ref()
            .map(|ks| ks.iter().cloned().map(KeyLit).collect());
        let c = crate::inherit_copies::pass(
            svc,
            root,
            tenants.iter().map(|t| t.tenant).collect(),
            &f.1,
            &f.2,
            lctn,
            lits.as_deref(),
            epoch,
        )
        .await?;
        if c.seeds > 0 {
            tracing::debug!(root = %root, db = %f.1, table = %f.2, seeds = c.seeds, upserts = c.upserts, tombstones = c.tombstones, "copies kept");
        }
    }
    if let Some(k) = known.get_mut(f) {
        k.dirty = false;
        k.full = false;
        k.keys.clear();
    }
    Ok(())
}

/// Whether `cat`'s tenant shares its object `db.table` materialized (to a
/// space or a tenant): the family keeps copies.
fn materialized(cat: &TenantCatalog, db: &str, table: &str, lctn: bool) -> bool {
    let Some(st) = sprig_inherit::plan::state_of(cat) else {
        return false;
    };
    let Some(d) = cat.database(&sprig_catalog::ddl::database_key(db, lctn)) else {
        return false;
    };
    let Some(o) = st.object_named(d.db_id, table, lctn) else {
        return false;
    };
    st.shares_of(o.object_id)
        .any(|s| s.mode == MergeMode::Materialized)
}

/// The key columns of tenant `cat`'s inheritable table `db.table`, each
/// with whether its value in a change event (JSON) gives back its text:
/// integers, character strings, temporal values and `ENUM`/`SET` members do;
/// for others (decimals, floats, binary strings, `BIT`, `JSON`) a change
/// makes the next pass a full one. `None`: not an inheritable table here.
fn key_columns(
    cat: &TenantCatalog,
    def: &Watched,
    db: &str,
    table: &str,
    lctn: bool,
) -> Option<Vec<(String, bool)>> {
    use crate::engine::table::Family as F;
    let st = sprig_inherit::plan::state_of(cat)?;
    let d = cat.database(&sprig_catalog::ddl::database_key(db, lctn))?;
    let o = st.object_named(d.db_id, table, lctn)?;
    Some(
        o.key
            .iter()
            .map(|k| {
                let text = def
                    .def
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(k))
                    .is_some_and(|c| {
                        matches!(F::of(c.ty), F::Integer | F::Text | F::Temporal | F::Members)
                    });
                (k.clone(), text)
            })
            .collect(),
    )
}

/// The keys of change event `e`'s row before and after (one when they
/// agree), as SQL literals of their text (as the passes compare keys);
/// `None` when a key column's value can't stand for it (see
/// [`key_columns`]).
fn key_columns_of(
    cols: Option<&[(String, bool)]>,
    e: &crate::table_changes::Event,
) -> Option<Vec<Vec<String>>> {
    let cols = cols?;
    let key = |row: &serde_json::Value| -> Option<Vec<String>> {
        let row = row.as_object()?;
        cols.iter()
            .map(|(name, text)| {
                if !text {
                    return None;
                }
                let v = row
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)?;
                let s = match v {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => return None,
                };
                Some(sprig_inherit::copy::key_text(Some(&s)))
            })
            .collect()
    };
    let mut out = Vec::with_capacity(2);
    for row in [e.before.as_ref(), e.after.as_ref()].into_iter().flatten() {
        let k = key(row)?;
        if !out.contains(&k) {
            out.push(k);
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Tenant `cat`'s engine table `db.table`, as the change reader watches it.
fn watched(cat: &TenantCatalog, db: &str, table: &str, lctn: bool) -> Option<(u32, Watched)> {
    let d = cat.database(&sprig_catalog::ddl::database_key(db, lctn))?;
    let def = crate::engine::catalog::of(cat)?
        .table(d.db_id, table, lctn)?
        .as_ref()
        .as_ref()
        .ok()?
        .clone();
    let object = u32::try_from(def.object.0).ok()?;
    Some((
        object,
        Watched {
            def,
            db: d.name.clone(),
        },
    ))
}

fn no_session() -> SprigError {
    SprigError::internal("the orphan pass has no session")
}

/// The orphan pass's runner: the system's reads and writes on the
/// tenants' engine databases, the directory and the catalog cache; no
/// session.
struct SysRunner {
    svc: Arc<TenantServices>,
    lctn: bool,
}

#[async_trait::async_trait]
impl sprig_types::Runner for SysRunner {
    async fn query(&mut self, _sql: &str) -> Result<Rows> {
        Err(no_session())
    }

    async fn exec(&mut self, _sql: &str) -> Result<Done> {
        Err(no_session())
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn found_rows(&self) -> bool {
        false
    }

    async fn object_id(
        &mut self,
        _db: Option<&str>,
        _table: &str,
    ) -> Result<Option<(sprig_common::ids::DbId, ObjectId)>> {
        Err(no_session())
    }

    async fn commit_types(&mut self, _change: &TypesChange) -> Result<()> {
        Err(no_session())
    }
}

#[async_trait::async_trait]
impl sprig_inherit::Runner for SysRunner {
    async fn commit(&mut self, _change: &sprig_inherit::InheritChange) -> Result<()> {
        Err(no_session())
    }

    async fn ctl(&mut self, _sql: &str) -> Result<()> {
        Err(no_session())
    }

    async fn child_tenant(&mut self, _name: &str) -> Result<Option<TenantId>> {
        Ok(None)
    }

    fn user_id(&self, _name: &str) -> Option<u32> {
        None
    }

    async fn ctl_query(&mut self, t: TenantId, sql: &str) -> Result<RawRows> {
        crate::exec::inherit_tree::ctl_query(&self.svc, t, sql, self.lctn).await
    }

    async fn refs(
        &mut self,
        root: TenantId,
        op: &sprig_inherit::refs::RefsOp<'_>,
    ) -> Result<RawRows> {
        crate::exec::inherit_tree::refs(&self.svc, root, op).await
    }

    async fn tenant_catalog(&mut self, t: TenantId) -> Result<Arc<TenantCatalog>> {
        self.svc.cache.get(t, Freshness::Latest).await
    }

    async fn tree_tables(
        &mut self,
        root: TenantId,
        db: &str,
        table: &str,
    ) -> Result<Vec<TreeTable>> {
        crate::exec::inherit_tree::tree_tables(&self.svc, root, db, table, self.lctn).await
    }
}
