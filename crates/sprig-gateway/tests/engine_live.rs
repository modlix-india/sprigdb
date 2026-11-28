//! Storage engines other than `InnoDB` through a running deployment
//! (`compat.engine_substitution`):
//!
//! - 3306: `CREATE TABLE ... ENGINE=MyISAM` makes an `InnoDB` table with
//!   MySQL's warnings 3161 and 1266; `ALTER TABLE ... ENGINE=MEMORY` keeps
//!   the engine with 1286 and `Warnings: 1` in the info line; the session's
//!   `default_storage_engine` counts; a temporary table keeps its engine;
//!   `BLACKHOLE` fails with 3161; a failed statement leaves no gateway
//!   warning behind;
//! - 3307: MariaDB's note 1266 for `CREATE` and `ALTER`, which becomes
//!   `ENGINE=InnoDB`;
//! - `reject` at tenant scope: 3161 on 3306 and 1290 on 3307, temporary
//!   tables still allowed.
//!
//! ```sh
//! export SPRIG_TEST_ENGINE_PORTS=47306,47307   # a private deployment's mysql, mariadb ports
//! cargo test -p sprig-gateway --test engine_live -- --nocapture
//! ```
//!
//! Logs in as `acme.app` (gateway-dev.sh's tenant), over TLS. Skipped
//! without `SPRIG_TEST_ENGINE_PORTS`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    missing_docs
)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

const H: &str = "127.0.0.1";
const DB: &str = "engine_live";

async fn login(port: u16) -> Conn {
    let o = OptsBuilder::default()
        .ip_or_hostname(H)
        .tcp_port(port)
        .user(Some("acme.app"))
        .pass(Some("sprig-dev-app"))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ))
        .prefer_socket(false);
    Conn::new(o).await.expect("login")
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        _ => 0,
    }
}

/// `SHOW WARNINGS` as `level code message`.
async fn warnings(c: &mut Conn) -> Vec<String> {
    let rows: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap();
    rows.into_iter()
        .map(|(l, n, m)| format!("{l} {n} {m}"))
        .collect()
}

async fn engine_of(c: &mut Conn, table: &str) -> Option<String> {
    c.query_first(format!(
        "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{DB}' AND TABLE_NAME = '{table}'"
    ))
    .await
    .unwrap()
}

#[ignore = "live: needs SPRIG_TEST_ENGINE_PORTS; tools/live/README.md"]
#[tokio::test]
async fn engines_become_innodb_live() {
    let Some(ports) = std::env::var("SPRIG_TEST_ENGINE_PORTS").ok() else {
        eprintln!("SPRIG_TEST_ENGINE_PORTS not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let ports: Vec<u16> = ports
        .split(',')
        .map(|p| p.trim().parse().unwrap())
        .collect();
    let (mysql, mariadb) = (ports[0], ports[1]);

    let mut my = login(mysql).await;
    my.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    my.query_drop(format!("CREATE DATABASE {DB}"))
        .await
        .unwrap();
    my.query_drop(format!("USE {DB}")).await.unwrap();
    // A setting another run left behind.
    my.query_drop("SPRIG RESET compat.engine_substitution")
        .await
        .unwrap();

    // 3306: CREATE TABLE.
    my.query_drop("CREATE TABLE m1 (a int, KEY k (a DESC)) ENGINE=MyISAM")
        .await
        .unwrap();
    assert_eq!(my.get_warnings(), 2);
    assert_eq!(
        warnings(&mut my).await,
        [
            "Warning 3161 Storage engine MyISAM is disabled (Table creation is disallowed).",
            "Warning 1266 Using storage engine InnoDB for table 'm1'",
        ]
    );
    assert_eq!(engine_of(&mut my, "m1").await.as_deref(), Some("InnoDB"));
    let (_, create): (String, String) = my
        .query_first("SHOW CREATE TABLE m1")
        .await
        .unwrap()
        .unwrap();
    assert!(create.contains(") ENGINE=InnoDB "), "{create}");
    assert!(create.contains("KEY `k` (`a` DESC)"), "{create}");

    // ALTER TABLE ... ENGINE= keeps the engine.
    my.query_drop("INSERT INTO m1 VALUES (1), (2)")
        .await
        .unwrap();
    my.query_drop("ALTER TABLE m1 ADD COLUMN b int, ENGINE=MEMORY")
        .await
        .unwrap();
    assert_eq!(my.get_warnings(), 1);
    assert_eq!(my.info(), "Records: 0  Duplicates: 0  Warnings: 1");
    assert_eq!(
        warnings(&mut my).await,
        ["Warning 1286 Unknown storage engine 'MEMORY'"]
    );
    my.query_drop("ALTER TABLE m1 ENGINE=MyISAM").await.unwrap();
    assert_eq!(my.get_warnings(), 1);
    assert_eq!(engine_of(&mut my, "m1").await.as_deref(), Some("InnoDB"));

    // The session's default engine.
    my.query_drop("SET default_storage_engine = MyISAM")
        .await
        .unwrap();
    my.query_drop("CREATE TABLE m2 (a int)").await.unwrap();
    assert_eq!(my.get_warnings(), 2);
    assert_eq!(engine_of(&mut my, "m2").await.as_deref(), Some("InnoDB"));
    my.query_drop("SET default_storage_engine = InnoDB")
        .await
        .unwrap();

    // A temporary table keeps its engine, without a warning.
    my.query_drop("CREATE TEMPORARY TABLE t1 (a int) ENGINE=MEMORY")
        .await
        .unwrap();
    assert_eq!(my.get_warnings(), 0);
    assert_eq!(engine_of(&mut my, "t1").await.as_deref(), Some("MEMORY"));
    my.query_drop("DROP TEMPORARY TABLE t1").await.unwrap();

    // Refused engines.
    let r = my
        .query_drop("CREATE TABLE b1 (a int) ENGINE=BLACKHOLE")
        .await;
    assert_eq!(r.as_ref().map_err(code), Err(3161), "{r:?}");

    // A statement that fails after the engine step: no gateway warning
    // is left for SHOW WARNINGS.
    let r = my.query_drop("CREATE TABLE m1 (a int) ENGINE=MyISAM").await;
    assert_eq!(r.as_ref().map_err(code), Err(1050), "{r:?}");
    let w = warnings(&mut my).await;
    assert!(!w.iter().any(|l| l.contains(" 1266 ")), "{w:?}");

    // 3307: MariaDB's note.
    let mut ma = login(mariadb).await;
    ma.query_drop(format!("USE {DB}")).await.unwrap();
    ma.query_drop("CREATE TABLE a1 (a int) ENGINE=Aria")
        .await
        .unwrap();
    assert_eq!(ma.get_warnings(), 1);
    assert_eq!(
        warnings(&mut ma).await,
        ["Note 1266 Using storage engine InnoDB for table 'a1'"]
    );
    assert_eq!(engine_of(&mut ma, "a1").await.as_deref(), Some("InnoDB"));
    ma.query_drop("ALTER TABLE a1 ENGINE=MyISAM").await.unwrap();
    assert_eq!(ma.info(), "Records: 0  Duplicates: 0  Warnings: 1");
    assert_eq!(
        warnings(&mut ma).await,
        ["Note 1266 Using storage engine InnoDB for table 'a1'"]
    );
    assert_eq!(engine_of(&mut ma, "a1").await.as_deref(), Some("InnoDB"));

    // reject, at tenant scope.
    my.query_drop("SPRIG SET compat.engine_substitution = 'reject'")
        .await
        .unwrap();
    let r = my.query_drop("CREATE TABLE r1 (a int) ENGINE=MyISAM").await;
    assert_eq!(r.as_ref().map_err(code), Err(3161), "{r:?}");
    let r = my.query_drop("ALTER TABLE m1 ENGINE=MyISAM").await;
    assert_eq!(r.as_ref().map_err(code), Err(3161), "{r:?}");
    let r = ma.query_drop("CREATE TABLE r1 (a int) ENGINE=MyISAM").await;
    assert_eq!(r.as_ref().map_err(code), Err(1290), "{r:?}");
    my.query_drop("CREATE TEMPORARY TABLE t2 (a int) ENGINE=MyISAM")
        .await
        .unwrap();
    my.query_drop("CREATE TABLE i1 (a int) ENGINE=InnoDB")
        .await
        .unwrap();
    assert_eq!(my.get_warnings(), 0);
    my.query_drop("SPRIG RESET compat.engine_substitution")
        .await
        .unwrap();

    my.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    drop(ma.disconnect().await);
    drop(my.disconnect().await);
}
