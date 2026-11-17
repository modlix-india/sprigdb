//! What the inheritance layer needs beyond the session: the tree-wide id
//! sequences, the reference index, reads as the provisioning account, the
//! tenants of a tree, and bringing a child tenant's tables up to its
//! parent's ([`adopt`]).
//!
//! ```text
//! tree sequence  t<root>_sprig.sequences, seq_id = root object | 1<<62,
//!                global ID blocks (one allocator per root tenant
//!                in this gateway); created at first use above every
//!                tenant's MAX(id) and AUTO_INCREMENT in the root's subtree
//! inherit_refs   t<root>_sprig.inherit_refs (created at first use)
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
use sprig_shard::ids::{
    AutoIncSession, BLOCK_LEASE, BlockAllocator, BlockConfig, CatalogSequences, Explicit,
    IdAllocator, Ordering, SeqId, SequenceSource,
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
/// `t`'s own transactions write the same rows.
///
/// # Errors
/// The backend's (server errors keep their number).
pub(crate) async fn ctl_query(svc: &TenantServices, t: TenantId, sql: &str) -> Result<RawRows> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.tenant_ctl_pool(t, &cluster).await?;
    let mut conn = pool.get_conn().await.map_err(server_err)?;
    let rows: Vec<mysql_async::Row> = conn.query(sql).await.map_err(server_err)?;
    Ok(rows
        .into_iter()
        .map(|r| r.unwrap().into_iter().map(bytes_of).collect())
        .collect())
}

fn refs_ready() -> &'static Mutex<HashSet<u64>> {
    static R: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Runs `sql` on `inherit_refs` in tenant `root`'s catalog, creating the
/// table when this gateway hasn't seen it yet.
///
/// # Errors
/// The catalog's.
pub(crate) async fn refs(svc: &TenantServices, root: TenantId, sql: &str) -> Result<RawRows> {
    let table = format!(
        "{}.`inherit_refs`",
        q(&SchemaName::catalog(root).to_string())
    );
    let ready = refs_ready()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&root.0);
    if !ready {
        ctl_query(
            svc,
            root,
            &sprig_inherit::links::DDL_REFS.replace("{refs}", &table),
        )
        .await?;
        // Tables made before handovers lack the index by referencing row.
        ctl_query(
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
    ctl_query(svc, root, &sql.replace("{refs}", &table)).await
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
    async fn catalog(&self) -> Result<CatalogSequences> {
        let cluster = self.svc.locator.cluster(self.root).await?;
        let pool = self.svc.locator.ctl_pool(&cluster).await?;
        Ok(
            CatalogSequences::new(pool, SchemaName::catalog(self.root).to_string())
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
        let rows = ctl_query(
            svc,
            tt.tenant,
            &format!(
                "SELECT CAST(GREATEST(COALESCE((SELECT MAX({col}) FROM {s}.{t}), 0) + 1, \
                 COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE \
                 TABLE_SCHEMA = '{sn}' AND TABLE_NAME = '{tn}'), 1)) AS CHAR)",
                col = q(&seq.column),
                s = q(&tt.schema),
                t = q(&seq.table),
                sn = tt.schema.replace('\'', "''"),
                tn = seq.table.replace('\'', "''"),
            ),
        )
        .await?;
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
            cat.ensure_tables().await?;
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

/// Runs phases 1 and 2 of one adoption statement on tenant `t`: the
/// physical change is made, and its catalog change waits for the
/// adoption's one publish.
async fn stage(
    svc: &TenantServices,
    t: TenantId,
    statement: DdlStatement,
    sql: &str,
    logical: &str,
    lctn: bool,
) -> Result<Stage> {
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
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
    svc.ddl.stage(&req, &MariaExecutor::new(pool)).await
}

async fn show_create(
    svc: &TenantServices,
    t: TenantId,
    schema: &str,
    table: &str,
) -> Result<String> {
    let rows = ctl_query(
        svc,
        t,
        &format!("SHOW CREATE TABLE {}.{}", q(schema), q(table)),
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
        let (store, q) = tx.parts();
        if !self.inherit.is_empty() {
            InheritStore::new(store.schema())
                .apply(q, &self.inherit, version)
                .await?;
        }
        if !self.types.is_empty() {
            TypesStore::new(store.schema())
                .apply(q, &self.types)
                .await?;
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
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // the steps in order
async fn sync_one(
    svc: &Arc<TenantServices>,
    parent: &TenantCatalog,
    child: &Arc<TenantCatalog>,
    lctn: bool,
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
            // A database alone changes nothing anyone reads: it is
            // published on its own.
            let cat = crate::inherited::streams::ensure_database(svc, t, &w.db).await?;
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
            match stage(svc, t, st, &sql, &logical, lctn).await? {
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
        match stage(svc, t, st, &sql, &logical, lctn).await? {
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
