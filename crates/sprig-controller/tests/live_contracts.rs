//! The contract pass against a real catalog on MariaDB: a module's
//! contract migration runs only once every registered reader reads its
//! format, after `catalog.contract_delay_days`, or earlier with an
//! override whose reason and operator land in the migration's history.
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the MariaDB;
//! without it the test is skipped. The etcd tiers are in memory. The
//! catalog is a schema of its own, dropped afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::migrate::{ContractOverride, MigrateOpts, Migration, Module, Phase};
use sprig_catalog::readers::{ReaderRecord, key_of};
use sprig_catalog::{CatalogEndpoint, Migrator, StaticLocator, Store};
use sprig_common::ids::DcId;
use sprig_common::time::SystemClock;
use sprig_controller::contracts::ContractPass;
use sprig_controller::tree::{Directory, IdRange, NewTenant, TenantState};
use sprig_meta::{MemStore, MetaStore, Op, Record, Tier, Txn};

static EXT: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "contract test table",
        statements: &["CREATE TABLE IF NOT EXISTS ct_items (id INT PRIMARY KEY, a INT)"],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "column b",
        statements: &["ALTER TABLE ct_items ADD COLUMN IF NOT EXISTS b INT"],
    },
    Migration {
        format: 3,
        phase: Phase::Contract,
        name: "drop column a",
        statements: &["ALTER TABLE ct_items DROP COLUMN IF EXISTS a"],
    },
];

const EXT_MODULE: Module = Module {
    name: "contract_test",
    migrations: EXT,
};

async fn put(store: &Arc<dyn MetaStore>, rel: &str, value: Vec<u8>) {
    store
        .txn(Txn {
            when: vec![],
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value,
                lease: None,
            }],
            otherwise: vec![],
        })
        .await
        .unwrap();
}

async fn reader(dc: &Arc<dyn MetaStore>, role: &str, node: &str, format: u32) {
    let rec = ReaderRecord {
        role: role.into(),
        node: node.into(),
        build: "test".into(),
        formats: [(EXT_MODULE.name.to_owned(), format)].into(),
    };
    put(dc, &key_of(role, node), Record::new(rec).encode().unwrap()).await;
}

#[tokio::test]
async fn contracts_wait_for_readers_and_the_delay_unless_overridden() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    // One connection, kept while idle (connections are reused).
    let pool = Pool::new(
        OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 1).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    );
    let schema = format!("__sprig_test_contracts_{}", std::process::id());
    let r = AssertUnwindSafe(body(pool.clone(), schema.clone()))
        .catch_unwind()
        .await;
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    if let Err(p) = r {
        std::panic::resume_unwind(p);
    }
}

async fn body(pool: Pool, schema: String) {
    let dc_id = DcId::new("dc-t").unwrap();
    let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc_id.clone())));
    let dir = Directory::new(Arc::clone(&global));
    let t = dir
        .create_in(
            &NewTenant {
                name: "ctacme".into(),
                parent: None,
                home_dc: dc_id.clone(),
                placement_hint: None,
            },
            8,
            IdRange::ALL,
            SystemTime::now(),
        )
        .await
        .unwrap();
    let id = t.body().tenant_id;
    dir.update(id, |e| {
        e.state = TenantState::Active;
        Ok(true)
    })
    .await
    .unwrap();

    // The catalog with the module's expands, as the processes' own runs
    // leave it: the contract is held back by its ceiling.
    let mut conn = pool.get_conn().await.unwrap();
    let r = Migrator::new(&[EXT_MODULE])
        .unwrap()
        .run(&mut conn, &schema, &MigrateOpts::default())
        .await
        .unwrap();
    assert_eq!(
        r.applied
            .iter()
            .filter(|a| a.module == EXT_MODULE.name)
            .count(),
        2,
        "{r:?}"
    );
    assert_eq!(r.state[EXT_MODULE.name].format, 2);
    drop(conn);

    let locator = StaticLocator::new()
        .with(
            id,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema.clone()),
            },
        )
        .shared();
    let pass = ContractPass::new(
        dir,
        locator,
        Arc::clone(&global),
        Arc::clone(&dc),
        dc_id.clone(),
        None,
        Arc::new(SystemClock),
    )
    .with_modules(vec![EXT_MODULE]);
    let over = ContractOverride::new("hotfix; no rollback planned", "op:smallfix").unwrap();

    // 1. A gateway that reads format 2: nothing runs, even with an
    //    override, and the status names it.
    put(&dc, "gateways/g1", b"{}".to_vec()).await;
    reader(&dc, "gateway", "g1", 2).await;
    let run = pass.run(Some(&over), None).await.unwrap();
    assert!(run.runnable.is_empty() && run.tenants.is_empty(), "{run:?}");
    assert!(
        run.note
            .as_deref()
            .unwrap()
            .contains("gateway g1 (dc dc-t) reads 2"),
        "{run:?}"
    );
    let st = pass.status().await.unwrap();
    assert_eq!(st.modules[0].contract_ceiling, 2);

    // 2. The gateway reads 3, but a worker without a reader record (an
    //    older build) holds every contract back.
    reader(&dc, "gateway", "g1", 3).await;
    put(&dc, "workers/w1", b"{}".to_vec()).await;
    let run = pass.run(Some(&over), None).await.unwrap();
    assert!(run.runnable.is_empty(), "{run:?}");
    assert!(
        run.note.as_deref().unwrap().contains("worker w1"),
        "{run:?}"
    );

    // 3. Every reader reads 3: the pass may run it, but the delay (7
    //    days after the expand, just recorded) holds it back.
    reader(&dc, "worker", "w1", 3).await;
    let run = pass.run(None, None).await.unwrap();
    assert_eq!(run.runnable.get(EXT_MODULE.name), Some(&3), "{run:?}");
    assert_eq!(run.tenants.len(), 1);
    assert!(run.tenants[0].applied.is_empty(), "{run:?}");
    assert_eq!(run.tenants[0].delayed.len(), 1, "{run:?}");
    assert!(run.tenants[0].delayed[0].starts_with("contract_test 3 (due "));

    // 4. With the override it runs, recorded with the reason and operator.
    let run = pass.run(Some(&over), Some("ctacme")).await.unwrap();
    assert_eq!(
        run.tenants[0].applied,
        vec!["contract_test 3 contract drop column a"],
        "{run:?}"
    );
    let mut conn = pool.get_conn().await.unwrap();
    let cols: Vec<String> = conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
             AND TABLE_NAME = 'ct_items' ORDER BY ORDINAL_POSITION",
            (&schema,),
        )
        .await
        .unwrap();
    assert_eq!(cols, ["id", "b"]);
    let hist = sprig_catalog::migrate::read_history(&mut conn, &schema, EXT_MODULE.name)
        .await
        .unwrap();
    let last = hist.history.last().unwrap();
    assert_eq!((last.format, last.phase), (3, Phase::Contract));
    assert_eq!(last.by, "controller:dc-t/contracts");
    let o = last.delay_override.as_ref().unwrap();
    assert_eq!(
        (o.reason.as_str(), o.by.as_str(), o.delay_secs),
        ("hotfix; no rollback planned", "op:smallfix", 7 * 86_400)
    );
    assert!(o.expand_at.is_some());
    assert!(hist.history[..2].iter().all(|h| h.delay_override.is_none()));
    drop(conn);

    // 5. Nothing is left to run.
    let run = pass.run(None, None).await.unwrap();
    assert!(run.tenants[0].applied.is_empty() && run.tenants[0].delayed.is_empty());
}
