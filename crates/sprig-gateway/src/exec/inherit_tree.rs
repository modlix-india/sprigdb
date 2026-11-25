//! What the inheritance layer needs beyond the session: the tree-wide id
//! sequences, the reference index, reads as the provisioning account, the
//! tenants of a tree, and bringing a child tenant's tables up to its
//! parent's ([`adopt`]).
//!
//! ```text
//! tree sequence  the root's catalog records (m/sharding/sequences), seq_id = root object | 1<<62,
//!                global ID blocks (one allocator per root tenant
//!                in this gateway); created at first use above every
//!                tenant's MAX(id) and AUTO_INCREMENT in the root's subtree
//! inherit_refs   the root's catalog records (m/inherit/refs), or
//!                t<root>_sprig.inherit_refs (created at first use) for a
//!                root whose catalog schema is on MariaDB
//! adopt          before a tenant statement: for each tenant on the
//!                session's line, top first, what sprig_inherit::adopt::
//!                wanted says is missing is created or added, as the
//!                provisioning account through the DDL engine (staged),
//!                then the tables, the child's inherit records and its
//!                types are published in one catalog version
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use mysql_async::prelude::Queryable;
use sprig_catalog::ddl::{
    DdlRequest, DdlStatement, MariaExecutor, ObjectName, PublishWith, Stage, Staged, database_key,
};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::version::MariaTx;
use sprig_catalog::{CatalogLocator, ExtState, Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_inherit::ids::{TreeSeq, TreeTable};
use sprig_inherit::model::InheritableObject;
use sprig_inherit::{InheritChange, InheritStore, RawRows};
use sprig_meta::{ListOpts, Record};
use sprig_shard::ids::records::RecordSequences;
use sprig_shard::ids::{
    AutoIncSession, BLOCK_LEASE, BlockAllocator, BlockConfig, Explicit, IdAllocator, Ordering,
    SeqId, SequenceSource,
};
use sprig_types::{TypesChange, TypesStore};

use crate::inherited::DirEntry;
use crate::tenant::TenantServices;

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn server_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, state, format!("{} ({})", s.message, s.code))
        }
        other => SprigError::new(SprigCode::BackendUnavailable, other.to_string()),
    }
    .with_source(e)
}

fn bytes_of(v: mysql_async::Value) -> Option<Vec<u8>> {
    match v {
        mysql_async::Value::NULL => None,
        mysql_async::Value::Bytes(b) => Some(b),
        mysql_async::Value::Int(i) => Some(i.to_string().into_bytes()),
        mysql_async::Value::UInt(u) => Some(u.to_string().into_bytes()),
        mysql_async::Value::Float(f) => Some(f.to_string().into_bytes()),
        mysql_async::Value::Double(f) => Some(f.to_string().into_bytes()),
        other => Some(other.as_sql(true).into_bytes()),
    }
}

/// Runs `sql` as the provisioning account on tenant `t`'s cluster: a
/// dropped share's clean-up of `t`'s rows, link checks locking them. For
/// a tenant on Sprig 2PC's relay path, through the relay writer, since
/// `t`'s own transactions write the same rows. On a tenant whose
/// databases are all the engine's, in process instead ([`engine_ctl`]).
///
/// # Errors
/// The backend's (server errors keep their number).
pub(crate) async fn ctl_query(
    svc: &TenantServices,
    t: TenantId,
    sql: &str,
    lctn: bool,
) -> Result<RawRows> {
    // A tenant whose databases are all the engine's has no MariaDB to ask.
    let cat = svc.cache.get(t, Freshness::Latest).await?;
    if all_engine(&cat) {
        return engine_ctl(&cat, sql, lctn).await;
    }
    maria_query(svc, t, sql).await
}

fn refs_ready() -> &'static Mutex<HashSet<u64>> {
    static R: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Runs `op` on tree root `root`'s reference index: its catalog records
/// when its catalog is records only (no catalog schema on MariaDB), else
/// the `inherit_refs` table of its catalog schema, created when this
/// gateway hasn't seen it yet.
///
/// # Errors
/// The catalog's.
pub(crate) async fn refs(
    svc: &TenantServices,
    root: TenantId,
    op: &sprig_inherit::refs::RefsOp<'_>,
) -> Result<RawRows> {
    let schema = SchemaName::catalog(root).to_string();
    if sprig_catalog::migrate::records_catalog(&schema).await? {
        let mut tx = svc.records.begin(root).await?;
        let rows = match sprig_inherit::refs::run(&mut *tx, op).await {
            Ok(rows) => rows,
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
            }
        };
        if op.reads() {
            let _ = tx.rollback().await;
        } else {
            tx.commit().await?;
        }
        return Ok(rows);
    }
    let table = format!("{}.`inherit_refs`", q(&schema));
    let ready = refs_ready()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&root.0);
    if !ready {
        maria_query(
            svc,
            root,
            &sprig_inherit::links::DDL_REFS.replace("{refs}", &table),
        )
        .await?;
        // Tables made before handovers lack the index by referencing row.
        maria_query(
            svc,
            root,
            &sprig_inherit::links::DDL_REFS_BY_FROM.replace("{refs}", &table),
        )
        .await?;
        refs_ready()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(root.0);
    }
    maria_query(svc, root, &op.sql().replace("{refs}", &table)).await
}

/// The record-hierarchy lock of `object` (physical schema `schema`) on a
/// tenant whose databases are all the engine's: an engine transaction of
/// its own holding the exclusive lock of the catalog-records key
/// `lk/inherit/hier/<schema>/<object>/` of tenant `t` (no record is
/// written there), waiting at most `wait`. The caller keeps the
/// transaction until the session's transaction ends; dropping it releases
/// the lock. As the MariaDB lock row, it serializes structural changes of
/// one hierarchy whatever the user's grants.
///
/// # Errors
/// 1205 after `wait` (another structural change of the hierarchy holds
/// it); 1213 for a deadlock; 39070 when the engine isn't available.
pub(crate) async fn engine_hier_lock(
    t: TenantId,
    schema: &str,
    object: u64,
    wait: std::time::Duration,
) -> Result<sprig_engine::txn::Txn> {
    use sprig_engine::txn::{LockMode, LockRequest, TxnOptions, Wait};
    let eng = crate::node_engine::engine()?;
    let mut txn = eng.begin(TxnOptions {
        lock_wait_timeout: Some(wait),
        ..TxnOptions::default()
    })?;
    let mut key = Vec::with_capacity(64);
    sprig_engine::keys::KeyPrefix {
        tenant: t.0,
        shard: 0,
        object: 0,
        kind: sprig_engine::keys::kind::META,
        index: 0,
    }
    .encode_into(&mut key);
    key.extend_from_slice(format!("lk/inherit/hier/{schema}/{object}/").as_bytes());
    let req = LockRequest {
        mode: LockMode::Exclusive,
        wait: Wait::Block,
        check_snapshot: false,
    };
    match txn.lock(&bytes::Bytes::from(key), req).await {
        Ok(_) => Ok(txn),
        Err(e)
            if matches!(
                e.kind,
                sprig_common::ErrorKind::Standard { errno: 1205, .. }
                    | sprig_common::ErrorKind::Backend { errno: 1205, .. }
            ) =>
        {
            Err(SprigError::standard(
                1205,
                *b"HY000",
                "Lock wait timeout exceeded; another structural change of this record \
             hierarchy is running: try restarting transaction",
            ))
        }
        Err(e) => Err(e),
    }
}

/// Runs `sql` as the provisioning account on tenant `t`'s cluster.
async fn maria_query(svc: &TenantServices, t: TenantId, sql: &str) -> Result<RawRows> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.tenant_ctl_pool(t, &cluster).await?;
    let mut conn = pool.get_conn().await.map_err(server_err)?;
    let rows: Vec<mysql_async::Row> = conn.query(sql).await.map_err(server_err)?;
    Ok(rows
        .into_iter()
        .map(|r| r.unwrap().into_iter().map(bytes_of).collect())
        .collect())
}

/// Whether every database of `cat`'s tenant is the engine's (a tenant
/// with no database yet counts: each new database is the engine's).
pub(crate) fn all_engine(cat: &TenantCatalog) -> bool {
    let engine = crate::engine::catalog::of(cat);
    cat.databases
        .iter()
        .all(|d| engine.is_some_and(|e| e.dbs.contains(&d.db_id)))
}

/// A trailing locking clause of `sql`, cut off.
fn strip_lock(sql: &str) -> (&str, Option<crate::engine::plan::RowLock>) {
    use sprig_engine::txn::Wait;
    let s = sql.trim_end().trim_end_matches(';').trim_end();
    let up = s.to_ascii_uppercase();
    for (suffix, exclusive, wait) in [
        (" LOCK IN SHARE MODE NOWAIT", false, Wait::NoWait),
        (" FOR SHARE NOWAIT", false, Wait::NoWait),
        (" FOR UPDATE NOWAIT", true, Wait::NoWait),
        (" LOCK IN SHARE MODE", false, Wait::Block),
        (" FOR SHARE", false, Wait::Block),
        (" FOR UPDATE", true, Wait::Block),
    ] {
        if up.ends_with(suffix) {
            return (
                &s[..s.len() - suffix.len()],
                Some(crate::engine::plan::RowLock { exclusive, wait }),
            );
        }
    }
    (s, None)
}

/// [`ctl_query`] on a tenant whose databases are all the engine's: the
/// statement (physical names, as the inheritance layer writes them) runs
/// in process on the tenant's catalog snapshot `cat`, in a transaction of
/// its own, as the system (no privileges checked): a `SELECT` through the
/// general planner (its rows as text, a trailing locking clause kept, so a
/// `NOWAIT` read of a row another transaction holds fails with 1205), an
/// `INSERT` (the copy pass's upserts), `UPDATE` or `DELETE` of one table
/// through the single-table executor.
/// What exists only on MariaDB answers nothing: share views and their
/// definer's grants (`DROP VIEW`, `REVOKE`), copies (`DROP TABLE`, their
/// `information_schema` lookups) and the catalog schema's tables.
#[allow(clippy::too_many_lines)] // the statement kinds, in order
async fn engine_ctl(cat: &TenantCatalog, sql: &str, lctn: bool) -> Result<RawRows> {
    use sprig_engine::txn::TxnOptions;
    use sqlparser::ast::Statement;

    use crate::engine::assign::Env;
    use crate::engine::general::{self, RunArgs, Seed};
    use crate::engine::meta::{Results, ResultsCs};
    use crate::engine::plan::{BindCtx, CatalogNames};

    enum Bound {
        Read(Box<general::General>),
        Write(Box<crate::engine::plan::Plan>),
    }

    let s = sql.trim();
    let lower = s.to_ascii_lowercase();
    let mut words = lower.split_whitespace();
    let head = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    if matches!(
        head,
        ("drop", "view" | "table") | ("revoke" | "grant" | "create", _)
    ) || lower.contains("information_schema.")
        || lower.contains("_sprig`.")
    {
        return Ok(Vec::new());
    }
    let (text, lock) = strip_lock(s);
    // A layer row's state is an `ENUM`, which the planner reads as its
    // member's text only as a plain result column.
    let text = text.replace("CAST(`_sprig_state` AS CHAR)", "`_sprig_state`");
    let mut stmts = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &text)
        .map_err(|e| {
        SprigError::internal(format!(
            "engine: an inheritance statement doesn't parse: {e}"
        ))
    })?;
    let (Some(stmt), true) = (stmts.pop(), stmts.is_empty()) else {
        return Err(SprigError::internal(
            "engine: an inheritance statement isn't one statement",
        ));
    };
    let eng = crate::engine::engine()?;
    let Some(ecat) = crate::engine::catalog::of(cat) else {
        // No engine database at all: nothing to read or change.
        return Ok(Vec::new());
    };
    // Bound before any wait: the names aren't `Send`.
    let bound = {
        let names = CatalogNames {
            cat,
            eng: ecat,
            lower_case: lctn,
        };
        let cx = BindCtx {
            tenant: cat.tenant,
            names: &names,
            current: None,
            backslash: true,
        };
        match &stmt {
            Statement::Query(_) => {
                general::bind(&cx, &stmt, Some(&text), lock)?.map(|g| Bound::Read(Box::new(g)))
            }
            Statement::Update { .. } | Statement::Delete(_) | Statement::Insert(_) => {
                Some(Bound::Write(Box::new(
                    crate::engine::plan::bind(&cx, &stmt)?.ok_or_else(|| {
                        crate::engine::not_on_engine(
                            "an inheritance write outside the engine's tables",
                        )
                    })?,
                )))
            }
            _ => {
                return Err(crate::engine::not_on_engine(
                    "an inheritance statement other than SELECT, INSERT, UPDATE or DELETE",
                ));
            }
        }
    };
    match bound {
        None => Ok(Vec::new()),
        Some(Bound::Read(g)) => {
            let seed = Seed::from_vars(false, &Env::STRICT, |_| None);
            let a = RunArgs {
                tenant: cat.tenant.0,
                params: &[],
                seed: &seed,
                deadline: None,
                stop: None,
            };
            let ran = eng
                .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
                .await?;
            let results = Results {
                cs: Some(ResultsCs { id: 224, mbmax: 4 }),
                mysql: false,
            };
            let layout = general::results::layout(&g, &ran.columns, results);
            Ok(ran
                .rows
                .iter()
                .map(|r| {
                    r.iter()
                        .zip(&layout.shapes)
                        .map(|(v, shape)| {
                            general::results::text(&g, shape, v, &Env::STRICT, results)
                                .map(|b| b.to_vec())
                        })
                        .collect()
                })
                .collect())
        }
        Some(Bound::Write(plan)) => {
            eng.run_env(
                cat.tenant.0,
                &plan,
                &[],
                false,
                TxnOptions::default(),
                Env::STRICT,
            )
            .await?;
            Ok(Vec::new())
        }
    }
}

/// Runs one DDL statement as the system on tenant `t`, whose databases
/// are all the engine's (the copy pass's copy tables): staged by the DDL
/// engine with the engine's hook, nothing on MariaDB, then published on
/// its own and applied by the engine. `sql` names physical schemas,
/// `logical` the logical database `db`.
///
/// # Errors
/// The DDL engine's and the engine's.
pub(crate) async fn engine_ddl(
    svc: &Arc<TenantServices>,
    t: TenantId,
    db: &str,
    statement: DdlStatement,
    sql: &str,
    logical: &str,
    lctn: bool,
) -> Result<()> {
    let mut hooks = Vec::new();
    let staged = match stage(svc, t, statement, sql, logical, lctn, db, &mut hooks).await {
        Ok(Stage::Staged(s)) => *s,
        Ok(Stage::Done(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    let none = Records {
        inherit: InheritChange::default(),
        types: TypesChange::default(),
    };
    let r = svc.ddl.publish(t, vec![staged], &none).await;
    let fresh = match &r {
        Ok(v) => svc
            .cache
            .get(t, Freshness::Cached { min_version: *v })
            .await
            .ok(),
        Err(_) => None,
    };
    for h in hooks {
        let outcome = match (&fresh, &r) {
            (Some(c), _) => crate::engine::ddlexec::Outcome::Published(c),
            (None, Err(_)) => crate::engine::ddlexec::Outcome::Failed,
            (None, Ok(_)) => crate::engine::ddlexec::Outcome::Unknown,
        };
        let _ = h.done(outcome).await;
    }
    r.map(|_| ())
}

/// The states of a tenant's copies on engine databases: unversioned rows
/// of its catalog records (as the reference index's), one per copy.
///
/// ```text
/// m/inherit/copies/<object u64><share key u64>
///     {"serving", "object_version", "share_version", "epoch", "seeded_ms", "caught_up_ms", "source_tenant"}
/// ```
pub(crate) const COPIES: sprig_catalog::records::RowTable =
    sprig_catalog::records::RowTable::new("inherit", "copies");

/// A copy's state row in the records ([`COPIES`]).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CopyRow {
    pub serving: bool,
    pub object_version: u64,
    pub share_version: u64,
    pub epoch: u64,
    pub seeded_ms: u64,
    pub caught_up_ms: u64,
    pub source_tenant: u64,
}

fn copy_pk(object: ObjectId, share: u64) -> sprig_catalog::records::Pk {
    sprig_catalog::records::Pk::new().u64(object.0).u64(share)
}

/// Tenant `t`'s copy states on engine databases (none when it has none).
///
/// # Errors
/// The records'.
pub(crate) async fn engine_copy_states(
    svc: &TenantServices,
    t: TenantId,
) -> Result<Vec<(sprig_inherit::copy::CopyKey, CopyRow)>> {
    let mut tx = svc.records.begin(t).await?;
    let rows = COPIES
        .scan::<CopyRow>(&mut *tx, &sprig_catalog::records::Pk::new())
        .await;
    let _ = tx.rollback().await;
    Ok(rows?
        .into_iter()
        .filter_map(|(pk, row)| {
            let b = pk.0.get(..16)?;
            let object = u64::from_be_bytes(b[..8].try_into().ok()?);
            let share = u64::from_be_bytes(b[8..16].try_into().ok()?);
            Some((
                sprig_inherit::copy::CopyKey {
                    tenant: t,
                    object: ObjectId(object),
                    share,
                },
                row,
            ))
        })
        .collect())
}

/// [`engine_copy_states`] as the read path's [`CopyStates`]: a serving copy
/// is its subscribers' source from the start (no change streams of copies
/// on engine databases).
///
/// [`CopyStates`]: sprig_inherit::copy::CopyStates
pub(crate) fn copy_infos(
    rows: Vec<(sprig_inherit::copy::CopyKey, CopyRow)>,
) -> sprig_inherit::copy::CopyStates {
    sprig_inherit::copy::CopyStates(
        rows.into_iter()
            .map(|(k, r)| {
                (
                    k,
                    sprig_inherit::copy::CopyInfo {
                        serving: r.serving,
                        object_version: r.object_version,
                        share_version: r.share_version,
                        caught_up_ms: r.caught_up_ms,
                        seeded_ms: r.seeded_ms,
                        handover: if r.serving {
                            sprig_inherit::handover::Handover::Always
                        } else {
                            sprig_inherit::handover::Handover::Pending
                        },
                    },
                )
            })
            .collect(),
    )
}

/// Writes (`Some`) or removes (`None`) copy `k`'s state row.
///
/// # Errors
/// The records'.
pub(crate) async fn set_engine_copy_state(
    svc: &TenantServices,
    k: sprig_inherit::copy::CopyKey,
    row: Option<&CopyRow>,
) -> Result<()> {
    let mut tx = svc.records.begin(k.tenant).await?;
    let pk = copy_pk(k.object, k.share);
    let r = match row {
        Some(row) => COPIES.put(&mut *tx, &pk, row).await,
        None => COPIES.delete(&mut *tx, &pk).await,
    };
    match r {
        Ok(()) => tx.commit().await,
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}

/// Every active tenant of the directory, by id.
async fn directory(svc: &TenantServices) -> Result<BTreeMap<u64, DirEntry>> {
    let prefix = svc.global.tier().key("tenants/")?;
    let page = svc
        .global
        .list(
            &prefix,
            ListOpts {
                limit: 10_000_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<DirEntry>::decode(&kv.value).ok().map(|r| r.body))
        .filter(|e| e.state == "active" && e.tenant_id != 0)
        .map(|e| (e.tenant_id, e))
        .collect())
}

/// Every tenant in `root`'s subtree holding `db.table`.
///
/// # Errors
/// The directory's and the cache's.
pub(crate) async fn tree_tables(
    svc: &TenantServices,
    root: TenantId,
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<Vec<TreeTable>> {
    let mut out = Vec::new();
    for (id, e) in directory(svc).await? {
        let path = e.path_ids();
        if !path.contains(&root.0) {
            continue;
        }
        let t = TenantId(id);
        let Ok(cat) = svc.cache.get(t, Freshness::Latest).await else {
            continue;
        };
        let Some(d) = cat.database(&database_key(db, lctn)) else {
            continue;
        };
        let key = sprig_catalog::ddl::object_key(ObjectKind::Table, table, lctn);
        if cat
            .object(d.db_id, sprig_catalog::store::Namespace::Relation, &key)
            .is_none()
        {
            continue;
        }
        let Ok(schema) = sprig_inherit::plan::physical_schema(&cat, db, lctn) else {
            continue;
        };
        out.push(TreeTable {
            tenant: t,
            name: e.name.clone(),
            ancestors: path
                .iter()
                .filter(|x| **x != id)
                .map(|x| TenantId(*x))
                .collect(),
            schema,
        });
    }
    Ok(out)
}

// ------------------------------------------------------- tree sequences

/// The sequences of one root tenant's catalog.
struct TreeSource {
    svc: Arc<TenantServices>,
    root: TenantId,
    holder: String,
}

impl TreeSource {
    #[allow(clippy::unused_async)] // as when it looked the catalog's pool up
    async fn catalog(&self) -> Result<RecordSequences> {
        Ok(
            RecordSequences::new(Arc::clone(&self.svc.records), self.root)
                .held_by(self.holder.clone(), BLOCK_LEASE),
        )
    }
}

#[async_trait::async_trait]
impl SequenceSource for TreeSource {
    async fn fetch(&self, seq: SeqId, size: u128) -> Result<(i128, i128)> {
        let r = self.catalog().await?.fetch(seq, size).await;
        if r.is_ok() {
            metrics::counter!("sprig_ids_blocks_fetched_total", "kind" => "inherit_tree")
                .increment(1);
        }
        r
    }

    async fn raise(&self, seq: SeqId, v: i128) -> Result<()> {
        self.catalog().await?.raise(seq, v).await
    }

    async fn ordering(&self, seq: SeqId) -> Result<Ordering> {
        self.catalog().await?.ordering(seq).await
    }

    fn lease(&self) -> Option<std::time::Duration> {
        Some(BLOCK_LEASE)
    }

    async fn explicit(&self, seq: SeqId, v: i128) -> Result<Explicit> {
        self.catalog().await?.explicit(seq, v).await
    }
}

struct RootIds {
    alloc: BlockAllocator<TreeSource>,
    ready: Mutex<HashSet<u64>>,
    creating: tokio::sync::Mutex<()>,
}

fn roots() -> &'static Mutex<HashMap<u64, Arc<RootIds>>> {
    static R: OnceLock<Mutex<HashMap<u64, Arc<RootIds>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn root_ids(svc: &Arc<TenantServices>, root: TenantId) -> Arc<RootIds> {
    let mut m = roots().lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(m.entry(root.0).or_insert_with(|| {
        Arc::new(RootIds {
            alloc: BlockAllocator::new(
                TreeSource {
                    svc: Arc::clone(svc),
                    root,
                    holder: format!("gw-inherit-{}", std::process::id()),
                },
                BlockConfig::default(),
            ),
            ready: Mutex::new(HashSet::new()),
            creating: tokio::sync::Mutex::new(()),
        })
    }))
}

/// The first value of a new tree sequence: above every value and
/// `AUTO_INCREMENT` counter of the table in the root's subtree. Ids that
/// more than one tenant holds as its own are counted and logged (`SPRIG
/// CHECK IDS` lists them); none is renumbered.
async fn first_value(svc: &TenantServices, seq: &TreeSeq, lctn: bool) -> Result<i128> {
    let mut next: i128 = 1;
    let mut holders: HashMap<String, usize> = HashMap::new();
    for tt in tree_tables(svc, seq.root, &seq.db, &seq.table, lctn).await? {
        // An engine table has no `information_schema` counter to read:
        // above its largest value.
        let engine = all_engine(&*svc.cache.get(tt.tenant, Freshness::Latest).await?);
        let sql = if engine {
            format!(
                "SELECT CAST(COALESCE(MAX({col}), 0) + 1 AS CHAR) FROM {s}.{t}",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
            )
        } else {
            format!(
                "SELECT CAST(GREATEST(COALESCE((SELECT MAX({col}) FROM {s}.{t}), 0) + 1, \
                 COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE \
                 TABLE_SCHEMA = '{sn}' AND TABLE_NAME = '{tn}'), 1)) AS CHAR)",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
                sn = tt.schema.replace('\'', "''"),
                tn = seq.table.replace('\'', "''"),
            )
        };
        let rows = ctl_query(svc, tt.tenant, &sql, lctn).await?;
        let v: i128 = rows
            .first()
            .and_then(|r| r.first().cloned().flatten())
            .and_then(|b| String::from_utf8_lossy(&b).parse().ok())
            .unwrap_or(1);
        next = next.max(v);
        let own = ctl_query(
            svc,
            tt.tenant,
            &format!(
                "SELECT DISTINCT CAST({col} AS CHAR) FROM {s}.{t} WHERE `_sprig_state` IN \
                 ('own', 'orphan')",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
            ),
            lctn,
        )
        .await
        .unwrap_or_default();
        for r in own {
            if let Some(Some(b)) = r.first() {
                *holders
                    .entry(String::from_utf8_lossy(b).into_owned())
                    .or_default() += 1;
            }
        }
    }
    let collisions = holders.values().filter(|n| **n > 1).count();
    if collisions > 0 {
        metrics::counter!("sprig_inherit_id_collisions_total").increment(collisions as u64);
        tracing::warn!(
            root = %seq.root,
            table = %seq.table,
            collisions,
            "ids held by more than one tenant of the tree (SPRIG CHECK IDS lists them; none \
             renumbered)"
        );
    }
    Ok(next)
}

/// `n` values of `seq`, after moving it past `explicit`.
///
/// # Errors
/// 39460, 39463, 1467.
pub(crate) async fn tree_ids(
    svc: &Arc<TenantServices>,
    seq: &TreeSeq,
    n: u64,
    explicit: Option<i128>,
    s: &AutoIncSession,
    lctn: bool,
) -> Result<Option<(i128, i128)>> {
    let ids = root_ids(svc, seq.root);
    let known = |x: &RootIds| {
        x.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&seq.seq)
    };
    if !known(&ids) {
        let _one = ids.creating.lock().await;
        if !known(&ids) {
            let cat = ids.alloc.source().catalog().await?;
            if !cat.exists(SeqId(seq.seq)).await? {
                let next = first_value(svc, seq, lctn).await?;
                match cat
                    .create(
                        SeqId(seq.seq),
                        seq.root_object.0,
                        Some(&seq.column),
                        next,
                        Ordering::Blocks,
                    )
                    .await
                {
                    Ok(()) => tracing::info!(
                        root = %seq.root,
                        table = %seq.table,
                        next,
                        "tree id sequence created"
                    ),
                    Err(e) if matches!(e.kind, ErrorKind::Backend { errno: 1062, .. }) => {}
                    Err(e) => return Err(e),
                }
            }
            ids.ready
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(seq.seq);
        }
    }
    if let Some(v) = explicit {
        ids.alloc.explicit(SeqId(seq.seq), v).await?;
    }
    if n == 0 {
        return Ok(Some((0, 1)));
    }
    let run = ids.alloc.take(SeqId(seq.seq), n, s).await?;
    Ok(Some((run.first, run.step)))
}

// ------------------------------------------------------------- adoption

/// One adoption at a time for each child tenant in this gateway. Each
/// adoption publishes in one catalog version ([`sync_one`]), so this turn
/// isn't needed for what readers see; it keeps two sessions of a tenant
/// from running the same statements at once (the second would wait for
/// the first's held tables, up to `catalog.ddl_wait_ms`), and a statement
/// that waited takes the newer snapshot the other adoption left.
fn turn(tenant: TenantId) -> Arc<tokio::sync::Mutex<()>> {
    static T: OnceLock<Mutex<HashMap<u64, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let mut m = T
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(m.entry(tenant.0).or_default())
}

/// What this gateway last found up to date: tenant → (its version, its
/// parent's version).
fn synced() -> &'static Mutex<HashMap<u64, (u64, u64)>> {
    static S: OnceLock<Mutex<HashMap<u64, (u64, u64)>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The executor of a statement whose physical side is the engine's alone
/// (an engine tenant's adoption): a database exists only in the catalog,
/// tables are the engine hook's ([`crate::engine::ddlexec::Hook`]), and
/// nothing runs on MariaDB.
#[derive(Default)]
pub(crate) struct NoMaria {
    databases: Mutex<HashMap<SchemaName, sprig_catalog::ddl::SchemaInfo>>,
}

#[async_trait::async_trait]
impl sprig_catalog::ddl::DdlExecutor for NoMaria {
    async fn execute(&self, stmt: &sprig_catalog::ddl::PhysicalDdl) -> Result<()> {
        use sprig_catalog::ddl::Effect;
        if matches!(stmt.effect, Effect::CreateSchema | Effect::AlterSchema) {
            let info = crate::tenant::ddl::database_charset(&stmt.sql)?;
            self.databases
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(stmt.schema, info);
            return Ok(());
        }
        // Nor the decision table of transactions on MariaDB a new database
        // gets (no transaction of an engine database uses it).
        if matches!(&stmt.effect, Effect::Create { kind: ObjectKind::Table, name }
            if name == sprig_catalog::ddl::DTX_TABLE)
        {
            return Ok(());
        }
        Err(SprigError::internal(
            "inheritance: this statement has no physical side outside the engine",
        ))
    }
    async fn schema(&self, schema: &SchemaName) -> Result<Option<sprig_catalog::ddl::SchemaInfo>> {
        Ok(self
            .databases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(schema)
            .cloned())
    }
    async fn show_create(
        &self,
        _schema: &SchemaName,
        _kind: ObjectKind,
        _name: &str,
    ) -> Result<Option<String>> {
        Ok(None)
    }
    async fn objects(
        &self,
        _schema: &SchemaName,
    ) -> Result<Vec<sprig_catalog::ddl::PhysicalObject>> {
        Ok(Vec::new())
    }
    async fn tenant_schemas(&self, _tenant: TenantId) -> Result<Vec<SchemaName>> {
        Ok(Vec::new())
    }
}

/// Runs phases 1 and 2 of one adoption statement on tenant `t`: the
/// physical change is made, and its catalog change waits for the
/// adoption's one publish.
///
/// On a tenant with engine databases the engine takes part (its tables
/// are the engine's: nothing of them runs on MariaDB): the statement's
/// engine state is kept in `hooks` until the adoption's publish.
#[allow(clippy::too_many_arguments)] // the statement's parts and the hooks kept
async fn stage(
    svc: &Arc<TenantServices>,
    t: TenantId,
    statement: DdlStatement,
    sql: &str,
    logical: &str,
    lctn: bool,
    db: &str,
    hooks: &mut Vec<crate::engine::ddlexec::Hook>,
) -> Result<Stage> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let creates = matches!(statement, DdlStatement::CreateObject { .. });
    let req = DdlRequest {
        tenant: t,
        statement,
        sql,
        logical_sql: logical,
        current_db: None,
        user: None,
        lower_case_table_names: lctn,
        wait: None,
    };
    let maria = MariaExecutor::new(pool);
    let cat = svc.cache.get(t, Freshness::Latest).await?;
    if crate::engine::catalog::of(&cat).is_some()
        && let Ok(eng) = crate::engine::engine()
    {
        let none = NoMaria::default();
        let hook = crate::engine::ddlexec::Hook::new(
            eng,
            t,
            Arc::clone(&cat),
            lctn,
            crate::engine::ddl::Session {
                env: crate::engine::assign::Env::STRICT,
                wait: std::time::Duration::from_secs(30),
                db: db.to_owned(),
                conn: 0,
                account: ("sprig_inherit".into(), "localhost".into()),
            },
            creates,
        )
        .with_services(Arc::clone(svc));
        let r = svc.ddl.stage(&req, &hook.over(&none)).await;
        match &r {
            Ok(Stage::Staged(_)) => hooks.push(hook),
            Ok(Stage::Done(_)) => {
                let _ = hook.done(crate::engine::ddlexec::Outcome::Unknown).await;
            }
            Err(_) => {
                let _ = hook.done(crate::engine::ddlexec::Outcome::Failed).await;
            }
        }
        return r;
    }
    svc.ddl.stage(&req, &maria).await
}

async fn show_create(
    svc: &TenantServices,
    t: TenantId,
    schema: &str,
    table: &str,
) -> Result<String> {
    // An engine table: the engine's own text (no MariaDB holds it).
    if let Ok(sn) = schema.parse::<SchemaName>()
        && let Some(db) = sn.db()
        && let Ok(cat) = svc.cache.get(t, Freshness::Latest).await
        && let Some(text) = crate::engine::show::stored_create(&cat, db, table)
    {
        return Ok(text);
    }
    let rows = ctl_query(
        svc,
        t,
        &format!("SHOW CREATE TABLE {}.{}", q(schema), q(table)),
        false,
    )
    .await?;
    rows.first()
        .and_then(|r| r.get(1).cloned().flatten())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .ok_or_else(|| SprigError::internal(format!("no SHOW CREATE TABLE for {table}")))
}

/// The inheritance records and the types an adoption writes, in the
/// catalog version that publishes its tables.
struct Records {
    inherit: InheritChange,
    types: TypesChange,
}

#[async_trait::async_trait]
impl PublishWith<MariaTx> for Records {
    async fn write(&self, tx: &mut MariaTx, version: u64) -> Result<()> {
        if !self.inherit.is_empty() {
            InheritStore::apply(tx.records().await?, &self.inherit, version).await?;
        }
        if !self.types.is_empty() {
            TypesStore::apply(tx.records().await?, &self.types).await?;
        }
        Ok(())
    }
}

/// Makes sure tenant `t`'s catalog has the module tables `records`
/// writes. Each migration takes its own catalog connection and gives it
/// back before the publish takes the version lock.
async fn migrate_for(svc: &TenantServices, t: TenantId, records: &Records) -> Result<()> {
    if records.inherit.is_empty() && records.types.is_empty() {
        return Ok(());
    }
    let ep = svc.locator.writer(t).await?;
    // A catalog of engine records only needs no MariaDB run.
    if sprig_catalog::migrate::records_catalog(ep.store.schema()).await? {
        return Ok(());
    }
    let mut conn = ep.pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
    })?;
    if !records.inherit.is_empty() {
        sprig_inherit::store::migrate(&mut conn, ep.store.schema()).await?;
    }
    if !records.types.is_empty() {
        sprig_types::store::migrate(&mut conn, ep.store.schema()).await?;
    }
    Ok(())
}

/// The table's key in an adoption's maps (database and table, case
/// folded as the inheritance layer compares them).
fn table_key(db: &str, table: &str) -> (String, String) {
    (db.to_lowercase(), table.to_lowercase())
}

/// Brings `child` up to its parent: its tables (new ones, parent columns
/// it lacks, types the parent widened), their inheritance records and the
/// types of inherited type trees. Every catalog row of it is published in
/// ONE catalog version: a reader on any gateway sees the child either
/// before or after, never a new table without its record or its types
/// (which it would plan as a plain table: 1054 on an inherited type's
/// parent columns). The physical statements run first, each staged by
/// the DDL engine; the records are then worked out from the newest
/// snapshot and published with them. The version when anything changed.
async fn sync_one(
    svc: &Arc<TenantServices>,
    parent: &TenantCatalog,
    child: &Arc<TenantCatalog>,
    lctn: bool,
) -> Result<Option<u64>> {
    let mut hooks = Vec::new();
    let r = sync_staged(svc, parent, child, lctn, &mut hooks).await;
    if hooks.is_empty() {
        return r;
    }
    // The engine applies what was published, or gives it up.
    let fresh = match &r {
        Ok(Some(v)) => svc
            .cache
            .get(child.tenant, Freshness::Cached { min_version: *v })
            .await
            .ok(),
        _ => None,
    };
    for h in hooks {
        let outcome = match (&fresh, &r) {
            (Some(c), _) => crate::engine::ddlexec::Outcome::Published(c),
            (None, Err(_)) => crate::engine::ddlexec::Outcome::Failed,
            (None, Ok(_)) => crate::engine::ddlexec::Outcome::Unknown,
        };
        let _ = h.done(outcome).await;
    }
    r
}

/// [`sync_one`]'s steps; `hooks`: the engine's part of each staged
/// statement, for its outcome.
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the steps in order
async fn sync_staged(
    svc: &Arc<TenantServices>,
    parent: &TenantCatalog,
    child: &Arc<TenantCatalog>,
    lctn: bool,
    hooks: &mut Vec<crate::engine::ddlexec::Hook>,
) -> Result<Option<u64>> {
    let t = child.tenant;
    let wants = sprig_inherit::adopt::wanted(parent, child, lctn);
    let widen = widenings(svc, parent, child, &wants, lctn).await?;
    let widened: HashSet<(String, String)> = widen
        .iter()
        .map(|(i, _, _)| table_key(&wants[*i].db, &wants[*i].table))
        .collect();
    // Newest version seen: a database made here, or another adoption's
    // tables a statement found.
    let mut seen = child.version;
    let mut staged: Vec<Staged> = Vec::new();
    // New tables' object ids, before they are published.
    let mut made: HashMap<(String, String), ObjectId> = HashMap::new();
    let mut new_tables = 0_u64;
    for (i, w) in wants.iter().enumerate() {
        let clauses: Vec<String> = widen
            .iter()
            .filter(|(j, _, _)| *j == i)
            .flat_map(|(_, c, _)| c.iter().cloned())
            .collect();
        if w.done() && clauses.is_empty() {
            continue;
        }
        let pschema = sprig_inherit::plan::physical_schema(parent, &w.db, lctn)?;
        let name = ObjectName {
            db: Some(w.db.clone()),
            name: w.table.clone(),
        };
        if w.child.is_none() {
            // A database whose creation stopped half-way (left `creating`
            // in the catalog, which only a cluster's reconciler clears)
            // can't take the table: the table waits, and the child's
            // sessions go on.
            if let Some(d) = child.database(&sprig_catalog::ddl::database_key(&w.db, lctn))
                && d.state != sprig_catalog::store::DbState::Active
            {
                tracing::warn!(tenant = %t, db = %w.db, table = %w.table, state = ?d.state, "inherited table not created: the child's database isn't active");
                continue;
            }
            // A database alone changes nothing anyone reads: it is
            // published on its own.
            let cat = crate::inherited::ensure_database(svc, t, &w.db).await?;
            seen = seen.max(cat.version);
            let text = show_create(svc, parent.tenant, &pschema, &w.table).await?;
            let cschema = sprig_inherit::plan::physical_schema(&cat, &w.db, lctn)?;
            let target = format!("{}.{}", q(&cschema), q(&w.table));
            let sql = sprig_inherit::adopt::child_create(&text, &target);
            let logical =
                sprig_inherit::adopt::child_create(&text, &format!("{}.{}", q(&w.db), q(&w.table)));
            let st = DdlStatement::CreateObject {
                kind: ObjectKind::Table,
                name,
                if_not_exists: true,
                or_replace: false,
                parent: None,
            };
            match stage(svc, t, st, &sql, &logical, lctn, &w.db, hooks).await? {
                Stage::Staged(s) => {
                    if let Some(id) = s.objects().first() {
                        made.insert(table_key(&w.db, &w.table), *id);
                    }
                    staged.push(*s);
                    new_tables += 1;
                    tracing::info!(tenant = %t, db = %w.db, table = %w.table, "inherited table created");
                }
                // Another adoption made it meanwhile.
                Stage::Done(o) => seen = seen.max(o.seen.or(o.version).unwrap_or(0)),
            }
            continue;
        }
        // Types the parent widened (MODIFY COLUMN) and the parent's
        // columns the child lacks, in one ALTER.
        let mut clauses = clauses;
        if !w.missing.is_empty() {
            let text = show_create(svc, parent.tenant, &pschema, &w.table).await?;
            clauses.extend(
                w.missing
                    .iter()
                    .filter_map(|c| sprig_inherit::adopt::column_def(&text, c))
                    .map(|d| format!("ADD COLUMN IF NOT EXISTS {d}")),
            );
        }
        if clauses.is_empty() {
            continue;
        }
        let cschema = sprig_inherit::plan::physical_schema(child, &w.db, lctn)?;
        let sql = format!(
            "ALTER TABLE {}.{} {}",
            q(&cschema),
            q(&w.table),
            clauses.join(", ")
        );
        let logical = format!(
            "ALTER TABLE {}.{} {}",
            q(&w.db),
            q(&w.table),
            clauses.join(", ")
        );
        let st = DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name,
            if_exists: false,
            rename_to: None,
        };
        match stage(svc, t, st, &sql, &logical, lctn, &w.db, hooks).await? {
            Stage::Staged(s) => staged.push(*s),
            Stage::Done(o) => seen = seen.max(o.seen.or(o.version).unwrap_or(0)),
        }
        tracing::info!(tenant = %t, table = %w.table, clauses = ?clauses, "inherited table altered");
    }
    // The records, against the newest snapshot (with any database made
    // above, and any adoption another gateway published meanwhile).
    let cur = if seen > child.version {
        svc.cache
            .get(t, Freshness::Cached { min_version: seen })
            .await?
    } else {
        Arc::clone(child)
    };
    let mut change = InheritChange {
        root_name: None,
        ..InheritChange::default()
    };
    // A widened table's object is written again, unchanged: its version
    // moves, so materialized copies of it are seeded again with the new
    // types.
    for w in sprig_inherit::adopt::wanted(parent, &cur, lctn) {
        let key = table_key(&w.db, &w.table);
        if w.done() && !widened.contains(&key) {
            continue;
        }
        let Some(d) = cur.database(&database_key(&w.db, lctn)) else {
            continue;
        };
        let okey = sprig_catalog::ddl::object_key(ObjectKind::Table, &w.table, lctn);
        let Some(id) = cur
            .object(d.db_id, sprig_catalog::store::Namespace::Relation, &okey)
            .map(|r| ObjectId(r.object_id.0))
            .or_else(|| made.get(&key).copied())
        else {
            continue;
        };
        let p = &w.parent;
        #[allow(clippy::single_match_else)] // two ways to the same object
        let o = match &w.child {
            Some(c) => {
                let mut o = (**c).clone();
                let mut cols = o.columns.clone();
                for m in &w.missing {
                    if let Some(pc) = p.column(m) {
                        cols.push(pc.to_owned());
                    }
                }
                if let Some(n) = o.with_slots_for(&cols) {
                    o = n;
                }
                o.links = sprig_inherit::adopt::merged_links(&p.links, &o.links);
                if o.auto.is_none() {
                    o.auto.clone_from(&p.auto);
                }
                if o.type_root.is_none() {
                    o.type_root.clone_from(&p.type_root);
                }
                o
            }
            None => {
                let empty = InheritableObject {
                    object_id: id,
                    db: d.db_id,
                    table: w.table.clone(),
                    key: p.key.clone(),
                    columns: Vec::new(),
                    slots: std::collections::BTreeMap::new(),
                    uniques: p.uniques.clone(),
                    tag_field: p.tag_field.clone(),
                    version: 1,
                    auto: p.auto.clone(),
                    links: p.links.clone(),
                    adopted: true,
                    type_root: p.type_root.clone(),
                };
                empty.with_slots_for(&p.columns).unwrap_or(empty)
            }
        };
        change.upsert_objects.push(o);
    }
    // The types of inherited type trees (typed tables in groups), worked
    // out as the child reads once these records are in.
    let state = sprig_inherit::plan::state_of(&cur)
        .cloned()
        .unwrap_or_default()
        .with_objects(change.upsert_objects.iter().cloned());
    let mut next = (*cur).clone();
    next.ext = cur.ext.with(vec![(
        sprig_inherit::store::MODULE,
        Arc::new(state) as Arc<dyn ExtState>,
    )]);
    let (mut types, follow) = super::inherit_types::wanted_types(parent, &next, lctn);
    let mut followed = Vec::new();
    Box::pin(super::inherit_types::follow_discriminators(
        svc,
        &next,
        follow,
        lctn,
        &mut types,
        &mut followed,
    ))
    .await?;
    for o in followed {
        change.upsert_objects.retain(|x| x.object_id != o.object_id);
        change.upsert_objects.push(o);
    }
    if staged.is_empty() && change.upsert_objects.is_empty() && types.is_empty() {
        remember_widened(t, &widen);
        return Ok((cur.version > child.version).then_some(cur.version));
    }
    if !change.upsert_objects.is_empty()
        && sprig_inherit::plan::state_of(&cur).is_none_or(|s| !s.migrated)
    {
        let name = crate::inherited::tenant_name(svc, t)
            .await
            .unwrap_or_else(|_| t.to_string());
        change.root_name = Some(name);
    }
    let records = Records {
        inherit: change,
        types,
    };
    migrate_for(svc, t, &records).await?;
    let v = svc.ddl.publish(t, staged, &records).await?;
    remember_widened(t, &widen);
    metrics::counter!("sprig_inherit_adopted_tables_total").increment(new_tables);
    let widened_count: usize = widen.iter().map(|(_, c, _)| c.len()).sum();
    metrics::counter!("sprig_inherit_widened_columns_total").increment(widened_count as u64);
    if !records.types.is_empty() {
        metrics::counter!("sprig_inherit_adopted_types_total")
            .increment(records.types.upsert_types.len() as u64);
        tracing::info!(tenant = %t, types = records.types.upsert_types.len(), "inherited types adopted");
    }
    Ok(Some(v))
}

/// What this gateway last compared for widened types: (child tenant,
/// parent object) → the parent object's schema version.
fn compared() -> &'static Mutex<HashMap<(u64, u64), u64>> {
    static C: OnceLock<Mutex<HashMap<(u64, u64), u64>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn remember_widened(t: TenantId, widen: &[(usize, Vec<String>, (u64, u64))]) {
    let mut m = compared().lock().unwrap_or_else(PoisonError::into_inner);
    for (_, _, (obj, v)) in widen {
        m.insert((t.0, *obj), *v);
    }
}

/// The `MODIFY COLUMN` clauses giving `child`'s inherited tables the
/// types their parent widened since: per want, its
/// clauses and (parent object, schema version) to remember once applied.
/// Only tables whose parent definition changed since this gateway last
/// compared them are read (two `SHOW CREATE TABLE`s).
async fn widenings(
    svc: &TenantServices,
    parent: &TenantCatalog,
    child: &TenantCatalog,
    wants: &[sprig_inherit::adopt::Want],
    lctn: bool,
) -> Result<Vec<(usize, Vec<String>, (u64, u64))>> {
    let mut out = Vec::new();
    for (i, w) in wants.iter().enumerate() {
        let Some(co) = &w.child else { continue };
        if w.plain {
            continue;
        }
        let Some(pd) = parent.database(&database_key(&w.db, lctn)) else {
            continue;
        };
        let key = sprig_catalog::ddl::object_key(ObjectKind::Table, &w.table, lctn);
        let Some(prow) = parent.object(pd.db_id, sprig_catalog::store::Namespace::Relation, &key)
        else {
            continue;
        };
        let memo = (child.tenant.0, prow.object_id.0);
        let seen = compared()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&memo)
            .copied();
        if seen == Some(prow.schema_version) {
            continue;
        }
        let pschema = sprig_inherit::plan::physical_schema(parent, &w.db, lctn)?;
        let cschema = sprig_inherit::plan::physical_schema(child, &w.db, lctn)?;
        let ptext = show_create(svc, parent.tenant, &pschema, &w.table).await?;
        let ctext = show_create(svc, child.tenant, &cschema, &w.table).await?;
        let cols: Vec<String> = w
            .parent
            .columns
            .iter()
            .filter(|c| co.column(c).is_some())
            .cloned()
            .collect();
        let clauses = sprig_inherit::adopt::widened(&ptext, &ctext, &cols);
        if clauses.is_empty() {
            compared()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(memo, prow.schema_version);
        } else {
            out.push((i, clauses, (prow.object_id.0, prow.schema_version)));
        }
    }
    Ok(out)
}

/// Brings the session tenant (`cat`) and each tenant above it up to its
/// parent, top first, each in one catalog version ([`sync_one`]), one
/// adoption per child tenant at a time in this gateway ([`turn`]);
/// the session tenant's fresh snapshot when anything changed or a newer one
/// than `cat` was found.
///
/// # Errors
/// The steps'.
pub(crate) async fn adopt(
    svc: &Arc<TenantServices>,
    cat: &Arc<TenantCatalog>,
    lctn: bool,
) -> Result<Option<Arc<TenantCatalog>>> {
    if cat.ancestry.is_empty() {
        return Ok(None);
    }
    let mut line: Vec<Arc<TenantCatalog>> = Vec::with_capacity(cat.ancestry.len() + 1);
    for a in cat.ancestry.iter() {
        line.push(svc.cache.get(a.id, Freshness::Latest).await?);
    }
    line.push(Arc::clone(cat));
    // Nothing shared to anyone on the line: nothing to do.
    if line[..line.len() - 1]
        .iter()
        .all(|c| sprig_inherit::plan::state_of(c).is_none_or(|s| s.shares().next().is_none()))
    {
        return Ok(None);
    }
    let mut changed = false;
    for j in 1..line.len() {
        // The child's turn ([`turn`]); the newest snapshot of it this
        // gateway holds, which another adoption may have just written.
        let turn = turn(line[j].tenant);
        let _turn = turn.lock().await;
        if let Some(c) = svc.cache.cached(line[j].tenant)
            && c.version > line[j].version
        {
            line[j] = c;
        }
        let child = Arc::clone(&line[j]);
        let key = (child.version, line[j - 1].version);
        let known = synced()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&child.tenant.0)
            .copied();
        if known == Some(key) {
            continue;
        }
        let now = match sync_one(svc, &line[j - 1], &child, lctn).await? {
            Some(v) => {
                changed = true;
                let fresh = svc
                    .cache
                    .get(child.tenant, Freshness::Cached { min_version: v })
                    .await?;
                line[j] = Arc::clone(&fresh);
                fresh
            }
            None => child,
        };
        synced()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(now.tenant.0, (now.version, line[j - 1].version));
    }
    let last = Arc::clone(&line[line.len() - 1]);
    Ok((changed || last.version != cat.version).then_some(last))
}

#[cfg(test)]
mod tests {
    use super::strip_lock;

    #[test]
    fn a_trailing_locking_clause_is_cut_off() {
        let (s, l) = strip_lock("SELECT 1 FROM t WHERE a = 1 LIMIT 1 LOCK IN SHARE MODE NOWAIT");
        assert_eq!(s, "SELECT 1 FROM t WHERE a = 1 LIMIT 1");
        let l = l.expect("a lock");
        assert!(!l.exclusive);
        assert_eq!(l.wait, sprig_engine::txn::Wait::NoWait);
        let (s, l) = strip_lock("SELECT a FROM t for update;");
        assert_eq!(s, "SELECT a FROM t");
        assert!(l.is_some_and(|l| l.exclusive && l.wait == sprig_engine::txn::Wait::Block));
        let (s, l) = strip_lock("SELECT 'FOR UPDATE x' FROM t");
        assert_eq!(s, "SELECT 'FOR UPDATE x' FROM t");
        assert!(l.is_none());
    }
}
