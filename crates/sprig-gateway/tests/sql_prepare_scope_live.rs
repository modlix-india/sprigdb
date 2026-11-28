//! Two statement paths of a tenant session, live:
//!
//! - **SQL-level `PREPARE` of a publish into a stream.** `PREPARE pub FROM
//!   'INSERT INTO order_events (key, value) VALUES (?, ?)'` is held by the
//!   gateway, as `COM_STMT_PREPARE` of the same text is (MariaDB has no
//!   table to prepare it on: 1146 before); `EXECUTE pub USING ...` publishes
//!   each record, `EXECUTE IMMEDIATE` too, `DEALLOCATE` forgets it, and a
//!   `PREPARE` of an ordinary table still works.
//! - **`SHOW INDEX` and `SHOW TABLE STATUS` under row-level security.** A
//!   session bound by a policy on `db_x.notes` (here through `FORCE ROW
//!   LEVEL SECURITY`) is refused (39366) only for what reaches that table:
//!   the index of another table, or the status of another database, is
//!   answered. `TRUNCATE` and `DROP TABLE` of the forced table work for
//!   its bound manager (they read no row), and a new table of the same name
//!   isn't bound by the old one's policy.
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! cargo test -p sprig-gateway --test sql_prepare_scope_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the databases `sps_pub`, `sps_x` and `sps_y`, dropped
//! afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";

async fn connect(test: &str) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(std::env::var("SPRIG_TEST_LOGIC_USER").ok())
        .pass(std::env::var("SPRIG_TEST_LOGIC_PASSWORD").ok())
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Some(Conn::new(opts).await.unwrap())
}

async fn must(c: &mut Conn, q: &str) {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
}

struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            println!("ok   {name}: {detail}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

fn code(e: &mysql_async::Error) -> u16 {
    match e {
        mysql_async::Error::Server(s) => s.code,
        _ => 0,
    }
}

/// The stream's records in offset order, once there are `n` (or after
/// 20 s).
async fn records(c: &mut Conn, n: usize) -> Vec<(String, String)> {
    let t0 = std::time::Instant::now();
    loop {
        let rows: Vec<(String, String)> = c
            .query("SELECT `key`, value FROM order_events ORDER BY offset_no")
            .await
            .unwrap_or_default();
        if rows.len() >= n || t0.elapsed() > std::time::Duration::from_secs(20) {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_prepare_publishes_into_a_stream() {
    let Some(mut c) = connect("sql_prepare_publishes_into_a_stream").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut ck = Checks { failed: Vec::new() };
    must(&mut c, "DROP DATABASE IF EXISTS sps_pub").await;
    must(&mut c, "CREATE DATABASE sps_pub").await;
    must(&mut c, "USE sps_pub").await;
    must(&mut c, "CREATE STREAM order_events PARTITIONS 1").await;
    must(
        &mut c,
        "CREATE TABLE plain (id INT PRIMARY KEY, v VARCHAR(10))",
    )
    .await;

    let r = c
        .query_drop("PREPARE pub FROM 'INSERT INTO order_events (`key`, value) VALUES (?, ?)'")
        .await;
    ck.check(
        "PREPARE of a publish into a stream",
        r.is_ok(),
        format!("{r:?}"),
    );
    must(&mut c, "SET @k = 'k1', @v = 'v1'").await;
    let r = c.query_drop("EXECUTE pub USING @k, @v").await;
    ck.check("EXECUTE with variables", r.is_ok(), format!("{r:?}"));
    let r = c.query_drop("EXECUTE pub USING 'k2', CONCAT('v', 2)").await;
    ck.check(
        "EXECUTE with a literal and an expression",
        r.is_ok(),
        format!("{r:?}"),
    );
    let r = c
        .query_drop(
            "EXECUTE IMMEDIATE 'INSERT INTO order_events (`key`, value) VALUES (?, ?)' \
             USING 'k3', 'v3'",
        )
        .await;
    ck.check(
        "EXECUTE IMMEDIATE of a publish",
        r.is_ok(),
        format!("{r:?}"),
    );
    // The stream's sequencer gives records their offsets a moment after
    // the commit.
    let want: Vec<(String, String)> = (1..=3)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    let rows = records(&mut c, want.len()).await;
    ck.check(
        "each record once, in order",
        rows == want,
        format!("{rows:?}"),
    );
    let r = c.query_drop("EXECUTE pub USING 'k4'").await;
    ck.check(
        "the wrong number of values: 1210",
        r.as_ref().is_err_and(|e| code(e) == 1210),
        format!("{r:?}"),
    );
    must(&mut c, "DEALLOCATE PREPARE pub").await;
    let r = c.query_drop("EXECUTE pub USING 'k5', 'v5'").await;
    ck.check(
        "after DEALLOCATE: 1243",
        r.as_ref().is_err_and(|e| code(e) == 1243),
        format!("{r:?}"),
    );
    // An ordinary table: MariaDB's own statement, as before.
    let r = c
        .query_drop("PREPARE ins FROM 'INSERT INTO plain (id, v) VALUES (?, ?)'")
        .await;
    ck.check("PREPARE of an ordinary table", r.is_ok(), format!("{r:?}"));
    let r = c.query_drop("EXECUTE ins USING 1, 'a'").await;
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM plain")
        .await
        .unwrap_or(None);
    ck.check(
        "its EXECUTE inserts the row",
        r.is_ok() && n == Some(1),
        format!("{r:?}, {n:?} rows"),
    );
    must(&mut c, "DEALLOCATE PREPARE ins").await;
    // A name used for an ordinary statement, then for a publish.
    must(&mut c, "PREPARE twice FROM 'SELECT 1'").await;
    let r = c
        .query_drop("PREPARE twice FROM 'INSERT INTO order_events (`key`, value) VALUES (?, ?)'")
        .await;
    let r2 = c.query_drop("EXECUTE twice USING 'k6', 'v6'").await;
    let n = records(&mut c, 4).await.len();
    ck.check(
        "a name MariaDB held, prepared again as a publish",
        r.is_ok() && r2.is_ok() && n == 4,
        format!("{r:?} {r2:?}, {n} records"),
    );
    must(&mut c, "DEALLOCATE PREPARE twice").await;
    must(&mut c, "DROP DATABASE sps_pub").await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_refusals_reach_only_protected_tables() {
    let Some(mut c) = connect("metadata_refusals_reach_only_protected_tables").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut ck = Checks { failed: Vec::new() };
    for q in [
        "DROP DATABASE IF EXISTS sps_x",
        "DROP DATABASE IF EXISTS sps_y",
        "CREATE DATABASE sps_x",
        "CREATE DATABASE sps_y",
        "CREATE TABLE sps_x.notes (id INT PRIMARY KEY, owner VARCHAR(64), KEY (owner))",
        "INSERT INTO sps_x.notes VALUES (1, 'nobody'), (2, 'nobody')",
        "CREATE TABLE sps_y.fx (id INT PRIMARY KEY, k INT, KEY (k))",
        "INSERT INTO sps_y.fx VALUES (1, 1), (2, 2)",
        "CREATE POLICY own ON sps_x.notes USING (owner = CURRENT_USER())",
        "ALTER TABLE sps_x.notes ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE sps_x.notes FORCE ROW LEVEL SECURITY",
    ] {
        must(&mut c, q).await;
    }
    // A new session: bound by the forced policy.
    drop(c);
    let mut c = connect("metadata_refusals_reach_only_protected_tables")
        .await
        .unwrap();
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM sps_x.notes")
        .await
        .unwrap_or(None);
    ck.check(
        "the session is bound: no row of notes",
        n == Some(0),
        format!("{n:?}"),
    );
    must(&mut c, "USE sps_y").await;
    for (q, refused) in [
        ("SHOW INDEX FROM fx", false),
        ("SHOW KEYS FROM fx IN sps_y", false),
        ("SHOW TABLE STATUS", false),
        ("SHOW TABLE STATUS FROM sps_y LIKE 'f%'", false),
        ("SHOW TABLE STATUS FROM sps_x LIKE 'zz%'", false),
        ("SHOW INDEX FROM sps_x.notes", true),
        ("SHOW INDEXES FROM notes IN sps_x", true),
        ("SHOW TABLE STATUS FROM sps_x", true),
        ("SHOW TABLE STATUS IN sps_x LIKE 'no%'", true),
    ] {
        let r = c.query_iter(q).await.map(drop);
        let r = match r {
            Ok(()) => Ok(()),
            Err(e) => Err(code(&e)),
        };
        let ok = if refused { r == Err(39366) } else { r.is_ok() };
        ck.check(
            &format!(
                "{q}: {}",
                if refused {
                    "refused (39366)"
                } else {
                    "answered"
                }
            ),
            ok,
            format!("{r:?}"),
        );
    }
    must(&mut c, "USE sps_x").await;
    let r = c
        .query_drop("SHOW TABLE STATUS")
        .await
        .map_err(|e| code(&e));
    ck.check(
        "SHOW TABLE STATUS in the protected table's database: refused",
        r == Err(39366),
        format!("{r:?}"),
    );
    // DDL that takes the table whole reads no row: the bound manager may
    // truncate and drop it (39366 before).
    let r = c
        .query_drop("TRUNCATE TABLE sps_x.notes")
        .await
        .map_err(|e| code(&e));
    ck.check("TRUNCATE of the forced table", r.is_ok(), format!("{r:?}"));
    let r = c
        .query_drop("DROP TABLE sps_x.notes")
        .await
        .map_err(|e| code(&e));
    ck.check(
        "DROP TABLE of the forced table",
        r.is_ok(),
        format!("{r:?}"),
    );
    // A new table of the same name isn't bound by the old one's policy.
    let r = c
        .query_drop("CREATE TABLE sps_x.notes (id INT PRIMARY KEY, owner VARCHAR(64))")
        .await
        .map_err(|e| code(&e));
    let _ = c
        .query_drop("INSERT INTO sps_x.notes VALUES (1, 'nobody')")
        .await;
    let n: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM sps_x.notes")
        .await
        .unwrap_or(None);
    ck.check(
        "a new table of that name is unprotected",
        r.is_ok() && n == Some(1),
        format!("{r:?}, {n:?} rows"),
    );
    drop(c);
    let mut c = connect("metadata_refusals_reach_only_protected_tables")
        .await
        .unwrap();
    // FORCE binds the administrator too: the policy goes first.
    let _ = c.query_drop("DROP POLICY own ON sps_x.notes").await;
    let _ = c
        .query_drop("ALTER TABLE sps_x.notes NO FORCE ROW LEVEL SECURITY")
        .await;
    must(&mut c, "DROP DATABASE sps_x").await;
    must(&mut c, "DROP DATABASE sps_y").await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
