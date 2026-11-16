//! The copy worker's sweep of dropped shares whose cleanup was cut off.
//!
//! `DROP SHARE` commits the share as a `dropping` row, then cleans up the
//! children it served ([`crate::unshare::after_drop`]) and removes the
//! row. A gateway that stops in between leaves the row, and the children's
//! overrides stay hidden. A statement of the parent or of a tenant below
//! it finishes the work ([`crate::unshare::sweep`]), but a tree nobody uses
//! would wait for ever. So the tree's copy worker, which reloads the
//! family every couple of seconds anyway, finishes it too:
//!
//! ```text
//! each refresh, for each `dropping` share of a tenant of the family:
//!   first seen less than GRACE ago → leave it (the dropping gateway is
//!     probably still cleaning up; its cleanup waits on row locks)
//!   read the tenant's catalog from its writer again; the share is no
//!     longer `dropping` there (finished meanwhile) → forget it
//!   run the cleanup from that catalog (every step can run again)
//!   remove the row (only while it is still `dropping`)
//!   a failure → logged, tried again GRACE later
//! ```
//!
//! Running at the same time as a gateway's cleanup of the same share is
//! harmless: each step finds what the other left. A share created again
//! meanwhile has another id (an id isn't reused while its row is
//! `dropping`), and the cleanup leaves a child that a share of the table
//! serves again alone.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::{Done, Rows, TypesChange};

use crate::copy::{CopyEnv, Family};
use crate::ids::TreeTable;
use crate::plan::{physical_schema, state_of};
use crate::unshare::{Unshared, finish_dropped};
use crate::{InheritChange, RawRows, Runner};

/// How long a share must have been `dropping` before the worker cleans up
/// after it: past the dropping gateway's own cleanup, its retries and a
/// lock wait or two.
pub const GRACE_MS: u64 = 60_000;

/// Dropped shares the worker has seen, by (tenant, share id): when it
/// first saw each one, or last failed to clean up after it (ms).
pub type Seen = BTreeMap<(TenantId, u64), u64>;

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Shares whose cleanup ran and whose row went.
    pub shares: u64,
    /// The cleanups' work, summed.
    pub done: Unshared,
    /// Shares whose cleanup failed (tried again later).
    pub failed: u64,
}

/// Runs the cleanup of every `dropping` share of `family` that has been
/// waiting [`GRACE_MS`] or more (module docs). `seen` is the caller's
/// memory across passes. A share's failure is logged and counted, never
/// returned, so the copies' refresh goes on.
pub async fn sweep_family(env: &dyn CopyEnv, family: &Family, seen: &mut Seen) -> Swept {
    let now = env.now_ms();
    let mut out = Swept::default();
    let mut live: BTreeSet<(TenantId, u64)> = BTreeSet::new();
    for t in &family.tenants {
        let Some(st) = family.cats.get(t).and_then(|c| state_of(c)) else {
            continue;
        };
        for sh in st.dropping() {
            let key = (*t, sh.id);
            live.insert(key);
            let first = *seen.entry(key).or_insert(now);
            if now.saturating_sub(first) < GRACE_MS {
                continue;
            }
            match finish(env, family, *t, sh.id).await {
                Ok(None) => {
                    seen.remove(&key);
                }
                Ok(Some(u)) => {
                    seen.remove(&key);
                    out.shares += 1;
                    out.done.views += u.views;
                    out.done.orphaned += u.orphaned;
                    out.done.unhidden += u.unhidden;
                    out.done.copies += u.copies;
                    tracing::info!(tenant = %t, share = %sh.name, views = u.views, orphaned = u.orphaned, unhidden = u.unhidden, copies = u.copies, "the copy worker finished a dropped share's cleanup that its drop didn't");
                    metrics::counter!("sprig_inherit_unshare_swept_total").increment(1);
                }
                Err(e) => {
                    seen.insert(key, now);
                    out.failed += 1;
                    tracing::warn!(tenant = %t, share = %sh.name, error = %e, "a dropped share's cleanup failed in the copy worker; tried again later");
                }
            }
        }
    }
    // Shares no longer `dropping` (finished elsewhere) are forgotten.
    seen.retain(|k, _| live.contains(k));
    out
}

/// One share: `None` when it is no longer `dropping` in the catalog read
/// now.
async fn finish(
    env: &dyn CopyEnv,
    family: &Family,
    t: TenantId,
    id: u64,
) -> Result<Option<Unshared>> {
    let cat = env.catalog(t).await?;
    let Some(st) = state_of(&cat) else {
        return Ok(None);
    };
    let Some(sh) = st.dropping().find(|s| s.id == id).map(|s| (**s).clone()) else {
        return Ok(None);
    };
    let mut r = EnvRunner { env, family };
    let u = finish_dropped(&cat, st, &sh, false, &mut r).await?;
    env.purge_shares(t, &[id]).await?;
    Ok(Some(u))
}

/// The cleanup's statements, run by the worker as the provisioning
/// account through `env`; the family stands in for the tenant directory.
struct EnvRunner<'a> {
    env: &'a dyn CopyEnv,
    family: &'a Family,
}

fn no_session() -> SprigError {
    SprigError::internal("the copy worker's sweep has no session")
}

#[allow(clippy::needless_pass_by_value)] // `map_err(server_err)`
fn server_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            let mut x = SprigError::backend(s.code, state, format!("{} ({})", s.message, s.code));
            x.retryable = matches!(s.code, 1205 | 1213);
            x
        }
        other => {
            let mut x = SprigError::new(SprigCode::BackendUnavailable, other.to_string());
            x.retryable = true;
            x
        }
    }
}

fn bytes_of(v: mysql_async::Value) -> Option<Vec<u8>> {
    match v {
        mysql_async::Value::NULL => None,
        mysql_async::Value::Bytes(b) => Some(b),
        mysql_async::Value::Int(i) => Some(i.to_string().into_bytes()),
        mysql_async::Value::UInt(u) => Some(u.to_string().into_bytes()),
        other => Some(other.as_sql(true).trim_matches('\'').as_bytes().to_vec()),
    }
}

#[async_trait::async_trait]
impl sprig_types::Runner for EnvRunner<'_> {
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
impl Runner for EnvRunner<'_> {
    async fn commit(&mut self, _change: &InheritChange) -> Result<()> {
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
        let mut c = self.env.conn(t).await?;
        let rows: Vec<mysql_async::Row> = c.query(sql).await.map_err(server_err)?;
        Ok(rows
            .into_iter()
            .map(|r| r.unwrap().into_iter().map(bytes_of).collect())
            .collect())
    }

    async fn tenant_catalog(&mut self, t: TenantId) -> Result<Arc<TenantCatalog>> {
        self.env.catalog(t).await
    }

    async fn tree_tables(
        &mut self,
        root: TenantId,
        db: &str,
        table: &str,
    ) -> Result<Vec<TreeTable>> {
        Ok(tree_tables(self.family, root, db, table))
    }
}

/// The tenants of `family` at or below `root` holding `db.table`, as the
/// gateway's directory lookup finds them.
fn tree_tables(family: &Family, root: TenantId, db: &str, table: &str) -> Vec<TreeTable> {
    let mut out = Vec::new();
    for t in &family.tenants {
        let Some(cat) = family.cats.get(t) else {
            continue;
        };
        let ancestors: Vec<TenantId> = cat.ancestry.iter().map(|a| a.id).collect();
        if *t != root && !ancestors.contains(&root) {
            continue;
        }
        let Some(d) = cat.database(&database_key(db, false)) else {
            continue;
        };
        let key = object_key(ObjectKind::Table, table, false);
        if cat.object(d.db_id, Namespace::Relation, &key).is_none() {
            continue;
        }
        let Ok(schema) = physical_schema(cat, db, false) else {
            continue;
        };
        out.push(TreeTable {
            tenant: *t,
            name: t.to_string(),
            ancestors,
            schema,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use sprig_catalog::TenantCatalog;
    use sprig_settings::Registry;

    use super::*;
    use crate::model::{InheritState, MergeMode, ROOT_SPACE, Share, ShareTarget};

    const PARENT: TenantId = TenantId(7);

    fn share(id: u64) -> Share {
        Share {
            id,
            name: format!("s{id}"),
            // No object of the state: the cleanup carries nothing, so it
            // needs no connection.
            object_id: ObjectId(99),
            from: ROOT_SPACE,
            to: ShareTarget::AllChildren,
            filter: None,
            columns: None,
            take: None,
            reshare: false,
            version: 1,
            created_by: 0,
            mode: MergeMode::ReadThrough,
            via: None,
        }
    }

    fn catalog(dropping: &[u64]) -> Arc<TenantCatalog> {
        let mut st = InheritState::new(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        st.set_dropping(dropping.iter().map(|id| share(*id)));
        let mut c = TenantCatalog::empty(Registry::global().unwrap(), PARENT);
        c.ext = c
            .ext
            .with(vec![(crate::store::MODULE, Arc::new(st) as Arc<_>)]);
        Arc::new(c)
    }

    fn family(dropping: &[u64]) -> Family {
        Family {
            tenants: vec![PARENT],
            cats: BTreeMap::from([(PARENT, catalog(dropping))]),
            ..Family::default()
        }
    }

    /// The catalog the worker reads again, as its writer has it now.
    struct Env {
        now: AtomicU64,
        writer: Mutex<Option<Arc<TenantCatalog>>>,
        reads: AtomicU64,
        purged: Mutex<Vec<(TenantId, Vec<u64>)>>,
    }

    impl Env {
        fn new(writer: Option<Arc<TenantCatalog>>) -> Self {
            Self {
                now: AtomicU64::new(1_000_000),
                writer: Mutex::new(writer),
                reads: AtomicU64::new(0),
                purged: Mutex::new(Vec::new()),
            }
        }

        fn later(&self, ms: u64) {
            self.now.fetch_add(ms, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl CopyEnv for Env {
        async fn family(&self, _root: TenantId) -> Result<Vec<TenantId>> {
            Ok(vec![PARENT])
        }

        async fn catalog(&self, _tenant: TenantId) -> Result<Arc<TenantCatalog>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.writer
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| SprigError::new(SprigCode::CatalogUnavailable, "unreachable"))
        }

        async fn conn(&self, _tenant: TenantId) -> Result<mysql_async::Conn> {
            Err(SprigError::internal("no connections in this test"))
        }

        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::Relaxed)
        }

        async fn purge_shares(&self, tenant: TenantId, ids: &[u64]) -> Result<()> {
            self.purged.lock().unwrap().push((tenant, ids.to_vec()));
            Ok(())
        }
    }

    /// A dropped share is left alone for the grace period (its gateway may
    /// still be cleaning up), then cleaned up and its row removed.
    #[tokio::test]
    async fn waits_out_the_grace_then_finishes_and_purges() {
        let f = family(&[5]);
        let env = Env::new(Some(catalog(&[5])));
        let mut seen = Seen::new();
        assert_eq!(sweep_family(&env, &f, &mut seen).await, Swept::default());
        env.later(GRACE_MS - 1);
        assert_eq!(sweep_family(&env, &f, &mut seen).await, Swept::default());
        assert_eq!(
            env.reads.load(Ordering::Relaxed),
            0,
            "read before the grace"
        );
        env.later(1);
        let s = sweep_family(&env, &f, &mut seen).await;
        assert_eq!(s.shares, 1);
        assert_eq!(s.failed, 0);
        assert_eq!(*env.purged.lock().unwrap(), vec![(PARENT, vec![5])]);
        assert!(seen.is_empty());
    }

    /// A share finished elsewhere meanwhile (a statement's sweep, the drop
    /// itself) is forgotten, and nothing is purged.
    #[tokio::test]
    async fn a_share_finished_meanwhile_is_left_alone() {
        let f = family(&[5]);
        let env = Env::new(Some(catalog(&[])));
        let mut seen = Seen::new();
        sweep_family(&env, &f, &mut seen).await;
        env.later(GRACE_MS);
        let s = sweep_family(&env, &f, &mut seen).await;
        assert_eq!(s, Swept::default());
        assert_eq!(env.reads.load(Ordering::Relaxed), 1);
        assert!(env.purged.lock().unwrap().is_empty());
        assert!(seen.is_empty());
        // And a family where it no longer shows forgets it too.
        seen.insert((PARENT, 6), 0);
        sweep_family(&env, &family(&[]), &mut seen).await;
        assert!(seen.is_empty());
    }

    /// A failure is counted and tried again a grace period later, not at
    /// the next refresh.
    #[tokio::test]
    async fn a_failure_is_tried_again_after_the_grace() {
        let f = family(&[5]);
        let env = Env::new(None);
        let mut seen = Seen::new();
        sweep_family(&env, &f, &mut seen).await;
        env.later(GRACE_MS);
        assert_eq!(sweep_family(&env, &f, &mut seen).await.failed, 1);
        env.later(2_000);
        assert_eq!(sweep_family(&env, &f, &mut seen).await.failed, 0);
        assert_eq!(env.reads.load(Ordering::Relaxed), 1);
        *env.writer.lock().unwrap() = Some(catalog(&[5]));
        env.later(GRACE_MS);
        assert_eq!(sweep_family(&env, &f, &mut seen).await.shares, 1);
        assert_eq!(*env.purged.lock().unwrap(), vec![(PARENT, vec![5])]);
    }
}
