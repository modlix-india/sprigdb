//! The orphanage sweep of the sharding loop, live:
//!
//! 1. group 7: `invoices` keyed by `cid` and `invoice_lines` following it
//!    by `fk_il_inv (iid)`, on 4 shards of one cluster; the key's
//!    `orphanage` is present;
//! 2. 121 invoices' lines wait on the shard holding hash 0 with
//!    `_sprig_rk = 0` (as a restore with `foreign_key_checks = 0` leaves
//!    them), and the invoices of 120 of them arrive by a path that adopts
//!    nothing (written on their shards directly);
//! 3. one `ShardingLoop::tick` takes every line to its invoice's shard
//!    with its hash; the line whose invoice never came keeps waiting and
//!    the flag stays present;
//! 4. its invoice arrives: the next tick adopts it and clears the flag;
//! 5. a line written as an orphan after that (a gateway that hadn't seen
//!    the change): the next tick sets the flag present again.
//!
//! Needs a MariaDB and an etcd of the test's own:
//!
//! ```sh
//! SPRIG_TEST_ORPH_SWEEP_URL=mysql://root:<password>@127.0.0.1:43311 \
//! SPRIG_TEST_ETCD=http://127.0.0.1:43379 \
//!   cargo test -p sprig-controller --test live_orphanage_sweep -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use sprig_balancer::decisions::{DecisionLog, DecisionStore, NewDecision, Outcome};
use sprig_catalog::placement::ShardMap as TenantMap;
use sprig_common::Result as SResult;
use sprig_common::ids::{ClusterId, DbId, DcId, GroupId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::time::SystemClock;
use sprig_controller::sharding::{ClusterPools, FixedSettings, ShardingLoop, TenantSettings};
use sprig_meta::{EtcdConfig, EtcdStore, ListOpts, MetaStore, Op, Record, Tier, Txn};
use sprig_shard::{KeyClass, KeySpec, RoutingHash, ShardMap, ShardMapStore};

/// Inside the private deployment's range (port 43311 × 1,000,000).
const TENANT: TenantId = TenantId(43_311_000_901);
const DB: DbId = DbId(1);
const GROUP: GroupId = GroupId(7);
const INVOICES: i64 = 120;

fn pool(url: &str) -> Pool {
    Pool::new(
        OptsBuilder::from_opts(Opts::from_url(url).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 8).unwrap())
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

fn cid_of(iid: i64) -> i64 {
    1 + (iid * 31) % 50
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
        let mut c = self
            .0
            .get_conn()
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        DecisionStore::for_tenant(tenant).insert(&mut c, d).await
    }
    async fn update_outcome(
        &self,
        tenant: TenantId,
        id: u64,
        o: Outcome,
        at_us: i64,
    ) -> SResult<()> {
        let mut c = self
            .0
            .get_conn()
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        DecisionStore::for_tenant(tenant)
            .set_outcome(&mut c, id, o, None, at_us)
            .await
    }
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
    let prefix = dc.tier().key("shardmaps/").unwrap();
    let mut keys = Vec::new();
    for pre in [prefix, dc.tier().key("tenantmaps/").unwrap()] {
        let page = dc.list(&pre, ListOpts::default()).await.unwrap();
        keys.extend(page.kvs.iter().map(|kv| kv.key.clone()));
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

const DDL: [&str; 2] = [
    "CREATE TABLE invoices (iid BIGINT PRIMARY KEY, cid BIGINT NOT NULL, \
     total DECIMAL(10,2) NOT NULL, _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk))",
    "CREATE TABLE invoice_lines (iid BIGINT NOT NULL, line_no INT NOT NULL, \
     sku VARCHAR(20) NOT NULL, _sprig_rk BIGINT UNSIGNED NULL INVISIBLE, KEY (_sprig_rk), \
     PRIMARY KEY (iid, line_no), CONSTRAINT fk_il_inv FOREIGN KEY (iid) \
     REFERENCES invoices (iid) ON DELETE CASCADE)",
];

async fn setup(p: &Pool, dc: &Arc<dyn MetaStore>, cluster: &ClusterId) -> ShardMap {
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
    for (id, name) in [(1u64, "invoices"), (2, "invoice_lines")] {
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
    }
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, key_columns, \
             rk_state, joined_at) VALUES (1, ?, 1, 'columns', '[\"cid\"]', 'ready', UTC_TIMESTAMP(6))"
        ),
        (GROUP.0,),
    )
    .await
    .unwrap();
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.group_members (object_id, group_id, db_id, routing, \
             parent_object_id, parent_fk, rk_state, joined_at) VALUES (2, ?, 1, 'parent', 1, \
             'fk_il_inv', 'ready', UTC_TIMESTAMP(6))"
        ),
        (GROUP.0,),
    )
    .await
    .unwrap();
    c.exec_drop(
        format!(
            "INSERT INTO `{cat}`.table_groups (group_id, kind, name, key_spec, key_spec_digest, state, \
             origin, created_at, updated_at) VALUES (?, 'sharded', 'invoices', ?, ?, 'active', \
             'advisor', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))"
        ),
        (GROUP.0, int().to_json().to_string(), int().digest),
    )
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{cat}`.foreign_keys (fk_id, name, child_object_id, child_columns, \
         parent_object_id, parent_columns, on_delete, on_update, enforcement, child_lookup_id, \
         orphanage, state, updated_at) VALUES (1, 'fk_il_inv', 2, '[\"iid\"]', 1, '[\"iid\"]', \
         'cascade', 'restrict', 'native', NULL, 'present', 'enforced', UTC_TIMESTAMP(6))"
    ))
    .await
    .unwrap();
    for s in 1..=4 {
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
        4,
        std::slice::from_ref(cluster),
    )
    .unwrap();
    m.tenant_epoch = 1;
    ShardMapStore::new(dc.clone()).create(&m).await.unwrap();
    m
}

/// Every line: `(iid, line_no) → (shard schema, _sprig_rk)`.
async fn lines(p: &Pool) -> BTreeMap<(i64, i64), (String, u64)> {
    let mut c = p.get_conn().await.unwrap();
    let mut out = BTreeMap::new();
    for s in 1..=4 {
        let rows: Vec<(i64, i64, u64)> = c
            .query(format!(
                "SELECT iid, line_no, _sprig_rk FROM `{}`.invoice_lines",
                schema(s)
            ))
            .await
            .unwrap();
        for (i, l, h) in rows {
            assert!(
                out.insert((i, l), (schema(s), h)).is_none(),
                "line ({i}, {l}) twice"
            );
        }
    }
    out
}

async fn flag(p: &Pool) -> String {
    let mut c = p.get_conn().await.unwrap();
    c.query_first::<String, _>(format!(
        "SELECT orphanage FROM `{}`.foreign_keys WHERE fk_id = 1",
        catalog()
    ))
    .await
    .unwrap()
    .unwrap_or_default()
}

#[tokio::test]
async fn the_sweep_adopts_rows_whose_parent_came_another_way() {
    let Ok(url) = std::env::var("SPRIG_TEST_ORPH_SWEEP_URL") else {
        eprintln!("skipped: set SPRIG_TEST_ORPH_SWEEP_URL and SPRIG_TEST_ETCD");
        return;
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "sprig_controller=info,sprig_shard=info,warn".into()),
        )
        .with_test_writer()
        .try_init();
    let etcd = std::env::var("SPRIG_TEST_ETCD").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(
            Tier::Dc(DcId::new("smallfix6").unwrap()),
            &EtcdConfig::new(vec![etcd]),
        )
        .await
        .unwrap(),
    );
    let cluster = ClusterId::new("smallfix6-c1").unwrap();
    let p = pool(&url);
    cleanup(&p, &dc).await;
    let map = setup(&p, &dc, &cluster).await;
    let shard_of = |cid: i64| map.route(RoutingHash(rk(cid))).shard.0;
    let s0 = map.ranges[0].shard.0;
    let t0 = Instant::now();

    // 2. Lines waiting on the hash-0 shard; invoices written on their
    // shards directly (a path that adopts nothing).
    let mut c = p.get_conn().await.unwrap();
    c.query_drop("SET SESSION foreign_key_checks = 0")
        .await
        .unwrap();
    let mut rows = Vec::new();
    for iid in 100..100 + INVOICES {
        for l in 1..=3 {
            rows.push(format!("({iid}, {l}, 'sku{iid}-{l}', 0)"));
        }
    }
    rows.push("(9000, 1, 'lost', 0)".into());
    for chunk in rows.chunks(100) {
        c.query_drop(format!(
            "INSERT INTO `{}`.invoice_lines (iid, line_no, sku, _sprig_rk) VALUES {}",
            schema(s0),
            chunk.join(", ")
        ))
        .await
        .unwrap();
    }
    c.query_drop("SET SESSION foreign_key_checks = 1")
        .await
        .unwrap();
    for iid in 100..100 + INVOICES {
        let cid = cid_of(iid);
        c.exec_drop(
            format!(
                "INSERT INTO `{}`.invoices (iid, cid, total, _sprig_rk) VALUES (?, ?, 1.5, ?)",
                schema(shard_of(cid))
            ),
            (iid, cid, rk(cid)),
        )
        .await
        .unwrap();
    }
    let before = lines(&p).await;
    assert_eq!(before.values().filter(|v| v.1 == 0).count(), rows.len());
    let elsewhere = (100..100 + INVOICES)
        .filter(|i| shard_of(cid_of(*i)) != s0)
        .count();
    eprintln!(
        "[{:?}] {} lines waiting on {}; {INVOICES} invoices written directly, {elsewhere} on \
         other shards",
        t0.elapsed(),
        rows.len(),
        schema(s0)
    );

    let mut ts = TenantSettings::default();
    ts.advisor.mode = sprig_advisor::sharding::AdvisorMode::Off;
    ts.sharding.merge_bytes = 0.0;
    let lp = ShardingLoop::new(
        dc.clone(),
        Arc::new(SystemClock),
        Arc::new(Pools(p.clone())),
        Arc::new(CatalogLog(p.clone())),
        Arc::new(FixedSettings(Arc::new(Mutex::new(ts)))),
        "smallfix6-controller",
    );

    // 3. One tick adopts every line whose invoice exists.
    let started = Instant::now();
    lp.tick(TENANT).await.unwrap();
    let took = started.elapsed();
    let after = lines(&p).await;
    assert_eq!(after.len(), before.len(), "no line lost or doubled");
    for ((iid, l), (s, h)) in &after {
        if *iid == 9000 {
            assert_eq!((s.as_str(), *h), (schema(s0).as_str(), 0), "still waiting");
            continue;
        }
        let cid = cid_of(*iid);
        assert_eq!(
            (s.clone(), *h),
            (schema(shard_of(cid)), rk(cid)),
            "line ({iid}, {l}) not with its invoice"
        );
    }
    assert_eq!(flag(&p).await, "present", "a line still waits");
    eprintln!(
        "[{:?}] tick 1 ({took:?}): {} lines adopted, 1 waiting, flag present",
        t0.elapsed(),
        INVOICES * 3
    );

    // 4. Its invoice arrives: adopted, and the flag goes.
    c.exec_drop(
        format!(
            "INSERT INTO `{}`.invoices (iid, cid, total, _sprig_rk) VALUES (9000, 7, 1, ?)",
            schema(shard_of(7))
        ),
        (rk(7),),
    )
    .await
    .unwrap();
    lp.tick(TENANT).await.unwrap();
    let after = lines(&p).await;
    assert_eq!(
        after.get(&(9000, 1)),
        Some(&(schema(shard_of(7)), rk(7))),
        "the last line adopted"
    );
    assert_eq!(flag(&p).await, "none", "nothing waits: the flag is cleared");
    eprintln!("[{:?}] tick 2: last line adopted, flag none", t0.elapsed());

    // 5. A line written as an orphan by a gateway that hadn't seen the
    // change: the flag comes back.
    c.query_drop("SET SESSION foreign_key_checks = 0")
        .await
        .unwrap();
    c.query_drop(format!(
        "INSERT INTO `{}`.invoice_lines (iid, line_no, sku, _sprig_rk) VALUES (9100, 1, 'late', 0)",
        schema(s0)
    ))
    .await
    .unwrap();
    c.query_drop("SET SESSION foreign_key_checks = 1")
        .await
        .unwrap();
    lp.tick(TENANT).await.unwrap();
    assert_eq!(flag(&p).await, "present", "a waiting row opens it again");
    eprintln!(
        "[{:?}] tick 3: a late orphan, flag present again",
        t0.elapsed()
    );
    drop(c);
    cleanup(&p, &dc).await;
    p.disconnect().await.unwrap();
}
