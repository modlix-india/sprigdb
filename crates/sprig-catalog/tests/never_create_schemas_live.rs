//! `migrate::never_create_schemas`: once a process calls it (gateways and
//! workers do at start), no migration run there creates a missing schema,
//! whatever its options say. Its own test binary, since the switch is for
//! the whole process.
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`) names the MariaDB; without
//! it the test is skipped with a message. It works in its own schema,
//! `__sprig_test_nocreate_<pid>`, dropped afterwards.

#![allow(clippy::unwrap_used, missing_docs)]

use mysql_async::prelude::Queryable;
use mysql_async::{Opts, Pool};
use sprig_catalog::Migrator;
use sprig_catalog::migrate::{self, MigrateOpts};
use sprig_common::SprigCode;

async fn exists(pool: &Pool, schema: &str) -> bool {
    let mut conn = pool.get_conn().await.unwrap();
    let n: Option<i64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
            (schema,),
        )
        .await
        .unwrap();
    n == Some(1)
}

#[ignore = "live: needs SPRIG_TEST_MARIADB_SINGLE; tools/live/README.md"]
#[tokio::test]
async fn a_process_that_never_creates_schemas_refuses_a_missing_one() {
    let Some(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let pool = Pool::new(Opts::from_url(&url).unwrap());
    let schema = format!("__sprig_test_nocreate_{}", std::process::id());
    let a = format!("{schema}_a");
    let mut conn = pool.get_conn().await.unwrap();
    for s in [&schema, &a] {
        conn.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }

    // Before the switch, the defaults still make a missing schema.
    assert!(migrate::schemas_created_here());
    Migrator::new(&[])
        .unwrap()
        .run(&mut conn, &a, &MigrateOpts::default())
        .await
        .unwrap();
    assert!(exists(&pool, &a).await, "made before the switch");

    migrate::never_create_schemas();
    assert!(!migrate::schemas_created_here());
    // The defaults (create_schema: true) no longer make it: 39072, nothing made.
    let e = Migrator::new(&[])
        .unwrap()
        .run(&mut conn, &schema, &MigrateOpts::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady), "{e}");
    assert!(e.message.contains("migrations don't create it"), "{e}");
    assert!(!exists(&pool, &schema).await, "the schema wasn't made");

    // A schema that exists is migrated as before, and again.
    conn.query_drop(format!("CREATE DATABASE `{schema}`"))
        .await
        .unwrap();
    for _ in 0..2 {
        Migrator::new(&[])
            .unwrap()
            .run(&mut conn, &schema, &MigrateOpts::default())
            .await
            .unwrap();
    }
    let tables: Option<i64> = conn
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?",
            (&schema,),
        )
        .await
        .unwrap();
    assert!(tables.unwrap_or(0) >= 4, "the core tables: {tables:?}");

    // A catalog dropped under a migrating loop (a tenant's deletion) stays
    // gone.
    conn.query_drop(format!("DROP DATABASE `{a}`"))
        .await
        .unwrap();
    let e = Migrator::new(&[])
        .unwrap()
        .run(&mut conn, &a, &MigrateOpts::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::TenantNotReady), "{e}");
    assert!(
        !exists(&pool, &a).await,
        "a dropped catalog isn't made again"
    );

    conn.query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
        .await
        .unwrap();
    drop(conn);
    pool.disconnect().await.unwrap();
}
