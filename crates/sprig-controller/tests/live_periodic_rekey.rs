//! The advisor's periodic run and its `Rekey` verdict, live, on a group
//! whose root owns a global lookup index:
//!
//! 1. group 7 (`orders`, `order_items`, `events`, keyed by `store_id`) on 3
//!    shards of one cluster, rows written with `_sprig_rk`; `orders.ref`
//!    has a unique lookup (`uk_ref`), built by the lookup jobs;
//! 2. a workload that reads and writes by `customer_id` is recorded in
//!    the tenant's workload store;
//! 3. `ShardingLoop::tick` with `advisor.rekey_sustain_runs = 2`: the first
//!    periodic run proposes `customer_id` and is held (one run so far); the
//!    second, after `advisor.interval_hours`, starts the re-key;
//! 4. while the group is re-keyed, a lookup rebuild is refused (39470);
//!    after the cutover and before the rebuild, a claim of a taken `ref`
//!    by another row still raises 1062 (the owner check reads every shard),
//!    where an owner check at the old hash alone would take the value over;
//! 5. the next ticks rebuild the lookup and finish the catalog: every
//!    lookup row names its order by `customer_id`'s hash; every row is on
//!    the shard its `customer_id` routes to; a third periodic run doesn't
//!    re-key again (the key it proposed is the group's now).
//!
//! Needs a MariaDB with the binary log on (row format) and an etcd of the
//! test's own:
//!
//! ```sh
//! SPRIG_TEST_PERIODIC_URL=mysql://root:sprig-dev-root@127.0.0.1:35313 \
//! SPRIG_TEST_ETCD=http://127.0.0.1:35379 \
//!   cargo test -p sprig-controller --test live_periodic_rekey -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use sprig_balancer::decisions::{DecisionLog, DecisionStore, NewDecision, Outcome};
use sprig_catalog::placement::ShardMap as TenantMap;
use sprig_catalog::records::RecordStore as _;
use sprig_cluster::TopologyKind;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, NodeId, ObjectId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Result as SResult};
use sprig_controller::sharding::{ClusterPools, FixedSettings, ShardingLoop, TenantSettings};
use sprig_meta::{EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::mysql::{ClusterAccess, group_env};
use sprig_mover::{
    EnvFactory, JobEnv, JobRecord, JobRuntime, JobStore, MoverSettings, ShardRoute, WorkerOpts,
};
use sprig_shard::lookup::admin::{Admin, LookupHost, NewLookup};
use sprig_shard::lookup::direct::{DirectTxn, Pools as LkPools};
use sprig_shard::lookup::{ClaimResult, LookupCatalog, LookupOrigin, LookupState, OwnerRef};
use sprig_shard::{KeyClass, KeySpec, KeyValueOwned, RoutingHash, ShardMap, ShardMapStore};
use sprig_workload::fingerprint::{FrontEnd, SqlCtx, SqlFingerprinter};
use sprig_workload::recorder::{ExecSummary, Recorder, RecorderConfig};
use sprig_workload::shape::{ResolvedTable, TableResolver};
use sprig_workload::store::WorkloadTables;

/// Inside the private deployment's range (port 35313 × 1,000,000).
const TENANT: TenantId = TenantId(35_313_000_901);
const DB: DbId = DbId(1);
const GROUP: GroupId = GroupId(7);
const TABLES: [(&str, u64); 3] = [("orders", 1), ("order_items", 2), ("events", 3)];
const ORDERS: i64 = 600;

fn url() -> Option<String> {
    std::env::var("SPRIG_TEST_PERIODIC_URL").ok()
}

fn pool(url: &str) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 16).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    )
}

fn catalog() -> String {
    SchemaName::catalog(TENANT).to_string()
}

fn schema(shard: u32) -> String {
    sprig_shard::shard_schema(TENANT, DB, GROUP, ShardId(shard))
}

fn int() -> KeySpec {
    KeySpec::new(vec![KeyClass::Numeric]).unwrap()
}

fn rk(v: i64) -> u64 {
    sprig_shard::rk::hash_values(&int(), &[Value::Int(v)]).unwrap()
}

struct Pools(Pool);

#[async_trait::async_trait]
impl ClusterPools for Pools {
    async fn pool(&self, _cluster: &ClusterId) -> SResult<Pool> {
        Ok(self.0.clone())
    }
}

struct CatalogLog(Pool);

#[async_trait::async_trait]
impl DecisionLog for CatalogLog {
    async fn record(&self, tenant: TenantId, d: &NewDecision) -> SResult<u64> {
        // Decisions are the engine's catalog records now; this test keeps
        // them in memory.
        let _ = &self.0;
        let mut tx = decision_records().begin(tenant).await?;
        let id = DecisionStore::insert(&mut *tx, d).await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> SResult<()> {
        let mut tx = decision_records().begin(tenant).await?;
        DecisionStore::set_outcome(&mut *tx, id, o, None, at_us).await?;
        tx.commit().await
    }
}

/// The decision records of this test process.
fn decision_records() -> &'static sprig_catalog::records::mem::MemRecords {
    static M: std::sync::OnceLock<sprig_catalog::records::mem::MemRecords> =
        std::sync::OnceLock::new();
    M.get_or_init(sprig_catalog::records::mem::MemRecords::new)
}

struct Factory {
    access: ClusterAccess,
    meta: Arc<dyn MetaStore>,
    settings: MoverSettings,
}

#[async_trait::async_trait]
impl EnvFactory for Factory {
    async fn env(&self, rec: &JobRecord) -> SResult<JobEnv> {
        let route = ShardRoute::from_spec(&rec.spec)?
            .ok_or_else(|| sprig_common::SprigError::internal("not a hash-range job"))?;
        group_env(
            self.access.clone(),
            self.access.clone(),
            rec,
            route,
            self.meta.clone(),
            self.settings.server_id_base,
        )
    }

    fn settings(&self, rec: &JobRecord) -> MoverSettings {
        let mut s = self.settings.clone();
        if let Some(ms) = rec.spec.retain_ms {
            s.source_retain = Duration::from_millis(ms);
        }
        s
    }
}

fn access(cluster: &ClusterId, url: &str) -> ClusterAccess {
    let ep = sprig_cluster::NodeEndpoint::from_url(NodeId::new("r3s-n1").unwrap(), url).unwrap();
    ClusterAccess {
        cluster: cluster.clone(),
        topology: TopologyKind::SingleNode,
        writer: ep.clone(),
        nodes: vec![ep],
        repl: sprig_binlog::BinlogSource::from_url(url).unwrap(),
        domain: None,
    }
}

/// The lookup jobs' host: the catalog and every cluster on the test's
/// one server; publishing is a no-op (no gateways).
struct Host {
    pool: Pool,
    cluster: ClusterId,
}

#[async_trait::async_trait]
impl LookupHost for Host {
    async fn catalog(&self) -> SResult<Conn> {
        let mut c = self
            .pool
            .get_conn()
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        c.query_drop(format!("USE `{}`", catalog()))
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        Ok(c)
    }
    async fn pools(&self, _clusters: &[ClusterId]) -> SResult<LkPools> {
        let mut m = std::collections::HashMap::new();
        m.insert(self.cluster.clone(), self.pool.clone());
        Ok(Arc::new(m))
    }
    async fn publish(&self) -> SResult<()> {
        Ok(())
    }
}

struct Resolver;

impl TableResolver for Resolver {
    fn resolve(&self, db: Option<&str>, table: &str) -> Option<ResolvedTable> {
        if db != Some("shop") {
            return None;
        }
        let id = TABLES.iter().find(|(n, _)| *n == table)?.1;
        Some(ResolvedTable {
            object_id: ObjectId(id),
            db_id: DB,
            physical: None,
        })
    }
}

/// Records `sql` `n` times in the tenant's workload store.
async fn record(p: &Pool, stmts: &[(&str, u32)]) {
    let fps = SqlFingerprinter::new(10_000);
    let rec = Recorder::new(RecorderConfig::default(), Arc::new(SystemClock));
    for (sql, n) in stmts {
        let ctx = SqlCtx {
            lex: sprig_sql::lex::LexOptions::mariadb(),
            current_db: Some("shop"),
            db_id: DB,
            front_end: FrontEnd::Mariadb,
            caller: None,
            lower_case_table_names: false,
            max_text_bytes: 4096,
        };
        let fp = fps.sql(TENANT, sql.as_bytes(), &ctx, &Resolver);
        for _ in 0..*n {
            rec.record(
                TENANT,
                &fp,
                &ExecSummary {
                    latency_us: 800,
                    error: false,
                    rows_returned: 1,
                    rows_affected: 1,
                    shards: 3,
                    clusters: 1,
                    scatter: true,
                    in_list_max: 0,
                    examined: None,
                    session: 1,
                },
            );
        }
    }
    // The workload store is the catalog records the controller reads.
    let _ = p;
    let records = sprig_controller::catalog_records();
    let mut tx = records.begin(TENANT).await.unwrap();
    WorkloadTables::insert_fingerprints(&mut *tx, &rec.drain_new())
        .await
        .unwrap();
    WorkloadTables::replace_stats(&mut *tx, 1, &rec.snapshot())
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// Drops this tenant's schemas (by the exact tenant id) and the private
/// tier's keys.
async fn cleanup(p: &Pool, dc: &Arc<dyn MetaStore>) {
    let mut c = p.get_conn().await.unwrap();
    let names: Vec<String> = c
        .query("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
        .await
        .unwrap();
    for n in names {
        if n.parse::<SchemaName>().is_ok_and(|s| s.tenant() == TENANT) {
            c.query_drop(format!("DROP DATABASE IF EXISTS `{n}`"))
                .await
                .unwrap();
        }
    }
    let mut keys = Vec::new();
    for pre in ["shardmaps/", "mover/", "feed/", "tenantmaps/", "tenants/"] {
        let Ok(prefix) = dc.tier().key(pre) else {
            continue;
        };
        let mut opts = ListOpts::default();
        loop {
            let page = dc.list(&prefix, opts.clone()).await.unwrap();
            keys.extend(page.kvs.iter().map(|kv| kv.key.clone()));
            if !page.more {
                break;
            }
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
    for k in keys {
        let _ = dc
            .txn(Txn {
                when: vec![],
                then: vec![Op::Delete(k)],
                otherwise: vec![],
            })
            .await;
    }
}

const DDL: [&str; 3] = [
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, store_id BIGINT NOT NULL, \
     customer_id BIGINT NOT NULL, ref VARCHAR(40) NOT NULL, amount INT NOT NULL, \
     _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), KEY (customer_id), \
     KEY (store_id), UNIQUE KEY uk_ref (ref))",
    "CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, \
     store_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, qty INT NOT NULL, \
     _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), KEY (order_id), \
     KEY (customer_id), KEY (store_id))",
    "CREATE TABLE events (id BIGINT PRIMARY KEY, store_id BIGINT NOT NULL, \
     customer_id BIGINT NOT NULL, kind VARCHAR(20) NOT NULL, \
     _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), KEY (customer_id), \
     KEY (store_id))",
];

fn store_of(i: i64) -> i64 {
    1 + i % 4
}

fn customer_of(i: i64) -> i64 {
    1 + (i * 7919) % 300
}

async fn setup(p: &Pool, dc: &Arc<dyn MetaStore>, cluster: &ClusterId) {
    let mut c = p.get_conn().await.unwrap();
    let cat = catalog();
    c.query_drop(format!("CREATE DATABASE `{cat}`"))
        .await
        .unwrap();
    sprig_catalog::Migrator::new(&[
        sprig_workload::store::WORKLOAD_MODULE,
        sprig_balancer::DECISIONS_MODULE,
        sprig_shard::groups::SHARDING_MODULE,
    ])
    .unwrap()
    .run(
        &mut c,
        &cat,
        &sprig_catalog::migrate::MigrateOpts::default(),
    )
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{cat}`.`databases` (db_id, name, name_key, default_charset, default_collation, \
         default_group_id, state, options, created_at, changed_version) VALUES \
         (1, 'shop', 'shop', 'utf8mb4', 'utf8mb4_uca1400_ai_ci', 1, 'active', '{{}}', UTC_TIMESTAMP(6), 1)"
    ))
    .await
    .unwrap();
    for (name, id) in TABLES {
        c.exec_drop(
            format!(
                "INSERT INTO `{cat}`.objects (object_id, db_id, namespace, name, name_key, kind, state, \
                 schema_version, created_at, changed_version) VALUES (?, 1, 'relation', ?, ?, 'table', \
                 'active', 1, UTC_TIMESTAMP(6), 1)"
            ),
            (id, name, name),
        )
        .await
        .unwrap();
        c.exec_drop(
            format!(
                "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, key_columns, \
                 rk_state, joined_at) VALUES (?, ?, 1, 'columns', '[\"store_id\"]', 'ready', \
                 UTC_TIMESTAMP(6))"
            ),
            (id, GROUP.0),
        )
        .await
        .unwrap();
    }
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, state, \
             origin, created_at, updated_at) VALUES (?, 'sharded', 'orders', ?, ?, 'active', 'advisor', \
             UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        ),
        (GROUP.0, int().to_json().to_string(), int().digest),
    )
    .await
    .unwrap();
    for s in 1..=3 {
        let sc = schema(s);
        c.query_drop(format!("CREATE DATABASE `{sc}` CHARACTER SET utf8mb4"))
            .await
            .unwrap();
        c.query_drop(format!("USE `{sc}`")).await.unwrap();
        for d in DDL {
            c.query_drop(d).await.unwrap();
        }
    }
    let key = TenantMap::catalog_key(dc.tier(), TENANT).unwrap();
    dc.txn(Txn {
        when: vec![],
        then: vec![Op::Put {
            key,
            value: Record::new(TenantMap::catalog(TENANT, cluster.clone(), 1))
                .encode()
                .unwrap(),
            lease: None,
        }],
        otherwise: vec![],
    })
    .await
    .unwrap();
    let mut m = ShardMap::presplit(
        TENANT,
        GROUP,
        int().digest,
        3,
        std::slice::from_ref(cluster),
    )
    .unwrap();
    m.tenant_epoch = 1;
    ShardMapStore::new(dc.clone()).create(&m).await.unwrap();
    // Rows by store: each on its store's shard, with its hash.
    let shard_of = |store: i64| m.route(RoutingHash(rk(store))).shard.0;
    for i in 0..ORDERS {
        let (s, cu) = (store_of(i), customer_of(i));
        let sc = schema(shard_of(s));
        c.exec_drop(
            format!(
                "INSERT INTO `{sc}`.orders (id, store_id, customer_id, ref, amount, _sprig_rk) \
                 VALUES (?, ?, ?, ?, ?, ?)"
            ),
            (i, s, cu, format!("ref-{i}"), i % 97, rk(s)),
        )
        .await
        .unwrap();
        c.exec_drop(
            format!(
                "INSERT INTO `{sc}`.order_items (id, order_id, store_id, customer_id, qty, _sprig_rk) \
                 VALUES (?, ?, ?, ?, 1, ?)"
            ),
            (i, i, s, cu, rk(s)),
        )
        .await
        .unwrap();
        c.exec_drop(
            format!(
                "INSERT INTO `{sc}`.events (id, store_id, customer_id, kind, _sprig_rk) \
                 VALUES (?, ?, ?, 'view', ?)"
            ),
            (i, s, cu, rk(s)),
        )
        .await
        .unwrap();
    }
    for s in 1..=3 {
        for (t, _) in TABLES {
            c.query_drop(format!("ANALYZE TABLE `{}`.`{t}`", schema(s)))
                .await
                .unwrap();
        }
    }
}

/// The advisor runs of the tenant: id, cause, verdict, evidence.
async fn runs(p: &Pool) -> Vec<(u64, String, String, String)> {
    let mut c = p.get_conn().await.unwrap();
    c.query(format!(
        "SELECT run_id, cause, COALESCE(verdict, ''), CAST(evidence AS CHAR) FROM `{}`.advisor_runs \
         ORDER BY run_id",
        catalog()
    ))
    .await
    .unwrap_or_default()
}

async fn group_state(p: &Pool) -> (String, String) {
    let mut c = p.get_conn().await.unwrap();
    let state: Option<String> = c
        .query_first(format!(
            "SELECT state FROM `{}`.table_groups WHERE group_id = {}",
            catalog(),
            GROUP.0
        ))
        .await
        .unwrap();
    let cols: Option<String> = c
        .query_first(format!(
            "SELECT CAST(key_columns AS CHAR) FROM `{}`.group_members WHERE object_id = 1",
            catalog()
        ))
        .await
        .unwrap();
    (state.unwrap_or_default(), cols.unwrap_or_default())
}

#[tokio::test]
async fn a_periodic_run_rekeys_a_group_with_a_lookup() {
    let Some(url) = url() else {
        eprintln!(
            "skipped: set SPRIG_TEST_PERIODIC_URL and SPRIG_TEST_ETCD (a private MariaDB and etcd)"
        );
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "sprig_mover=warn,sprig_controller=info,sprig_shard=info,warn".into()
        }))
        .with_test_writer()
        .try_init();
    let etcd = std::env::var("SPRIG_TEST_ETCD").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Dc(DcId::new("r3smallrk").unwrap()),
            &EtcdConfig::new(vec![etcd]),
        )
        .await
        .unwrap(),
    );
    let cluster = ClusterId::new("r3smallrk-c1").unwrap();
    let p = pool(&url);
    cleanup(&p, &dc).await;
    setup(&p, &dc, &cluster).await;
    let t0 = Instant::now();

    // The lookup on orders.ref.
    let maps = ShardMapStore::new(dc.clone());
    let host = Host {
        pool: p.clone(),
        cluster: cluster.clone(),
    };
    let admin = Admin {
        tenant: TENANT,
        maps: &maps,
        host: &host,
        chunk: 200,
        lease: Duration::from_secs(30),
    };
    let (owner_map, _) = maps.load(TENANT, GROUP).await.unwrap();
    let (l, rep) = admin
        .create(&NewLookup {
            db: DB,
            owner: ObjectId(1),
            owner_table: "orders".into(),
            owner_map: Arc::new(owner_map),
            columns: vec!["ref".into()],
            key: Some("uk_ref".into()),
            origin: LookupOrigin::UniqueKey,
        })
        .await
        .unwrap();
    eprintln!(
        "[{:>4}s] lookup {} built: {} rows",
        t0.elapsed().as_secs(),
        l.index_name,
        rep.rows
    );

    // The workload: by customer, a little by store.
    record(
        &p,
        &[
            ("SELECT * FROM orders WHERE customer_id = 5", 500),
            ("SELECT * FROM order_items WHERE customer_id = 7", 300),
            ("UPDATE events SET kind = 'buy' WHERE customer_id = 9", 300),
            ("SELECT * FROM orders WHERE store_id = 2", 20),
        ],
    )
    .await;

    // The mover worker.
    let ms = MoverSettings {
        copy_chunk_rows: 200,
        ready_sustain: Duration::from_secs(1),
        cutover_retry: Duration::from_secs(1),
        server_id_base: 353_130_000,
        job_lease: Duration::from_secs(10),
        ..MoverSettings::default()
    };
    let factory = Arc::new(Factory {
        access: access(&cluster, &url),
        meta: dc.clone(),
        settings: ms.clone(),
    });
    let stop = CancellationToken::new();
    let mut opts = WorkerOpts::new("r3smallrk-worker");
    opts.scan_interval = Duration::from_millis(200);
    opts.lease = ms.job_lease;
    let rt = JobRuntime::new(
        JobStore::new(dc.clone(), Arc::new(SystemClock)),
        factory,
        opts,
    );
    let worker = tokio::spawn(rt.run(stop.child_token()));

    let mut ts = TenantSettings::default();
    ts.sharding.retire = Duration::from_secs(5);
    ts.sharding.merge_bytes = 0.0;
    ts.advisor.min_ndv_per_shard = 10;
    ts.rekey.sustain_runs = 2;
    ts.rekey.every = Duration::from_secs(2);
    let lp = Arc::new(ShardingLoop::new(
        dc.clone(),
        Arc::new(SystemClock),
        Arc::new(Pools(p.clone())),
        Arc::new(CatalogLog(p.clone())),
        Arc::new(FixedSettings(Arc::new(Mutex::new(ts)))),
        "r3smallrk-controller",
    ));

    // 1. The first periodic run: proposed, held for the second.
    let r1 = lp.tick(TENANT).await.unwrap();
    let all = runs(&p).await;
    eprintln!(
        "[{:>4}s] tick 1: {:?}; runs {:?}",
        t0.elapsed().as_secs(),
        r1.advisor,
        all.iter().map(|r| (r.0, &r.1, &r.2)).collect::<Vec<_>>()
    );
    let first = all.iter().find(|r| r.1 == "periodic").unwrap();
    assert_eq!(first.2, "recommend", "{}", first.3);
    let ev: serde_json::Value = serde_json::from_str(&first.3).unwrap();
    let best = &ev["rekey_proposals"][0];
    assert_eq!(best["column"], "customer_id", "{ev}");
    assert_eq!(best["qualifies"], true, "{ev}");
    eprintln!(
        "        proposal: gain {:.1} points, confidence {}, held: {}",
        best["gain_pts"].as_f64().unwrap(),
        best["confidence"],
        ev["held"]
    );
    assert_eq!(group_state(&p).await.0, "active");

    // 2. The second, past the interval: the re-key starts.
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let r2 = lp.tick(TENANT).await.unwrap();
    eprintln!("[{:>4}s] tick 2: {:?}", t0.elapsed().as_secs(), r2.advisor);
    assert_eq!(r2.advisor.map(|a| a.1), Some("apply"));
    assert_eq!(group_state(&p).await.0, "rekeying");
    // A rebuild meanwhile is refused.
    let e = admin
        .rebuild(ObjectId(1), "orders", "uk_ref")
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(sprig_common::SprigCode::ShardBusy), "{e}");

    // 3. Wait for the cutover (every range a new shard), without ticking.
    let started = Instant::now();
    loop {
        assert!(started.elapsed() < Duration::from_secs(300), "no cutover");
        let (m, _) = maps.load(TENANT, GROUP).await.unwrap();
        if m.ranges.iter().all(|r| r.shard.0 > 3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    eprintln!(
        "[{:>4}s] cut over after {:.1}s",
        t0.elapsed().as_secs(),
        started.elapsed().as_secs_f64()
    );
    // A claim of ref-10 by another row, as a gateway would after the
    // cutover: the lookup row still names order 10 by its store's hash.
    {
        let mut cat = host.catalog().await.unwrap();
        let rec = admin
            .find(&mut cat, ObjectId(1), "uk_ref")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rec.state, LookupState::Public);
        let (l, placement, pools) = admin.load(&mut cat, &rec).await.unwrap();
        assert!(l.rehashing, "the catalog says the group is being re-keyed");
        // An order whose old hash and new hash fall on different shards.
        let shard = |h: u64| placement.owner_map.route(RoutingHash(h)).shard;
        let i = (0..ORDERS)
            .find(|i| shard(rk(store_of(*i))) != shard(rk(customer_of(*i))))
            .unwrap();
        let value = vec![KeyValueOwned::Str {
            bytes: format!("ref-{i}").into_bytes(),
            charset: sprig_shard::collation::Charset::Utf8mb4,
        }];
        let owner = OwnerRef {
            pk: vec![KeyValueOwned::Int(1_000_000)],
            rk: RoutingHash(rk(customer_of(i))),
        };
        let mut t = DirectTxn::new(Arc::clone(&pools), 5);
        let r = sprig_shard::lookup::claim(&mut t, &placement, &l, &value, &owner).await;
        t.rollback().await;
        let e = r.unwrap_err();
        assert_eq!(sprig_shard::lookup::errno(&e), Some(1062), "{e}");
        eprintln!("        claim of ref-{i} (taken) after the cutover: {e}");
        // Before this round, the owner check read only the old hash's
        // shard, which no longer holds order i: the value was taken over.
        let mut stale = l.clone();
        stale.rehashing = false;
        let mut t = DirectTxn::new(Arc::clone(&pools), 5);
        let r = sprig_shard::lookup::claim(&mut t, &placement, &stale, &value, &owner).await;
        t.rollback().await;
        eprintln!("        the same claim with the old owner check: {r:?} (rolled back)");
        assert!(
            matches!(r, Ok(ClaimResult::Reclaimed)),
            "a stale owner check takes the value over: {r:?}"
        );
    }

    // 4. Ticks: the rebuild, then the catalog.
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "the re-key didn't finish"
        );
        let _ = lp.tick(TENANT).await;
        let (state, cols) = group_state(&p).await;
        if state == "active" && cols.contains("customer_id") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[{:>4}s] re-key finished", t0.elapsed().as_secs());

    // Checks: rows where customer_id routes them; lookup rows by the new hash.
    let (m, _) = maps.load(TENANT, GROUP).await.unwrap();
    let mut c = p.get_conn().await.unwrap();
    let mut rows = 0;
    for r in &m.ranges {
        let sc = sprig_shard::shard_schema(TENANT, DB, GROUP, r.shard);
        for (t, _) in TABLES {
            let got: Vec<(i64, u64)> = c
                .query(format!("SELECT customer_id, _sprig_rk FROM `{sc}`.`{t}`"))
                .await
                .unwrap();
            for (cu, h) in got {
                assert_eq!(h, rk(cu), "{t}: a row with a stale hash");
                assert_eq!(
                    m.route(RoutingHash(h)).shard,
                    r.shard,
                    "{t}: a misplaced row"
                );
                rows += 1;
            }
        }
    }
    assert_eq!(rows, ORDERS * 3);
    let mut cat = host.catalog().await.unwrap();
    let rec = admin
        .find(&mut cat, ObjectId(1), "uk_ref")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rec.state, LookupState::Public);
    let (_, placement, _) = admin.load(&mut cat, &rec).await.unwrap();
    let mut lk = 0;
    for t in sprig_shard::lookup::LookupPlacement::lookup_shards(&placement) {
        let got: Vec<(String, i64, u64)> = c
            .query(format!(
                "SELECT ref, owner_id, owner_rk FROM `{}`.`{}`",
                t.schema, rec.physical_name
            ))
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        for (r, id, h) in got {
            assert_eq!(r, format!("ref-{id}"));
            assert_eq!(h, rk(customer_of(id)), "lookup row {r}: the old hash");
            lk += 1;
        }
    }
    assert_eq!(lk, ORDERS, "one lookup row per order");
    eprintln!("        {rows} rows and {lk} lookup rows checked");
    let n_lookups = LookupCatalog.rows(&mut cat).await.unwrap().len();
    assert_eq!(n_lookups, 1);
    drop(cat);

    // 5. Another periodic run doesn't re-key again.
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let r3 = lp.tick(TENANT).await.unwrap();
    eprintln!(
        "[{:>4}s] tick after: {:?}",
        t0.elapsed().as_secs(),
        r3.advisor
    );
    assert_ne!(r3.advisor.map(|a| a.1), Some("apply"));
    assert_eq!(group_state(&p).await.0, "active");
    let rekeys: u64 = c
        .query_first(format!(
            "SELECT COUNT(*) FROM `{}`.decisions WHERE kind = 'group.rekey' AND reason_code = 'periodic'",
            catalog()
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rekeys, 1);
    drop(c);

    stop.cancel();
    let _ = worker.await;
    if std::env::var("SPRIG_TEST_PERIODIC_KEEP").is_err() {
        cleanup(&p, &dc).await;
    }
    p.disconnect().await.unwrap();
}
