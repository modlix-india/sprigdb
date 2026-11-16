//! `stream.max_message_bytes` on every SQL publish path, through a live
//! gateway: each literal form (strings with escapes, `N'..'`, `X'..'`,
//! `0x..`, `B'..'`, `_charset` introducers, `CONCAT` of literals), values
//! MariaDB evaluates (functions, variables, `INSERT ... SELECT`), the
//! headers, a prepared statement's parameters, the stream's own value of
//! the setting, and `amqp.max_message_bytes` on `sprig.mq_publish`. The
//! records that pass are read back from the stream.
//!
//! Needs a gateway with a worker (the stream's sequencer runs there) and a
//! tenant user who may create databases and streams and set the tenant's
//! settings:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:46306
//! export SPRIG_TEST_LOGIC_USER=sf7acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! cargo test -p sprig-gateway --test stream_publish_size_live -- --nocapture
//! ```
//!
//! Works in the database `sf7_size`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "sf7_size";

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

type Rows = Vec<Vec<Option<Vec<u8>>>>;

async fn sql(c: &mut Conn, q: &str) -> Result<Rows, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(b),
                        other => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) -> Rows {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn check(&mut self, what: &str, ok: bool, detail: impl std::fmt::Display) {
        self.n += 1;
        if ok {
            println!("ok   {what}");
        } else {
            println!("FAIL {what}: {detail}");
            self.failed.push(format!("{what}: {detail}"));
        }
    }
}

fn hex(n: usize) -> String {
    "41".repeat(n)
}

#[tokio::test]
async fn every_publish_is_measured() {
    let Some(mut c) = connect("every_publish_is_measured").await else {
        return;
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    must(&mut c, "CREATE STREAM s PARTITIONS 1").await;
    must(&mut c, "CREATE STREAM latest PARTITIONS 1 CLEANUP compact").await;
    must(
        &mut c,
        "CREATE TABLE src (id INT PRIMARY KEY, body LONGBLOB)",
    )
    .await;
    must(
        &mut c,
        "INSERT INTO src VALUES (1, REPEAT('b', 99)), (2, REPEAT('b', 100))",
    )
    .await;
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (stream.max_message_bytes = 100)"),
    )
    .await;

    // Each case: a statement, whether it passes. A record is key, value
    // and headers together; 100 bytes fit.
    let x = |n: usize| "x".repeat(n);
    let cases: Vec<(String, bool)> = vec![
        (
            format!("INSERT INTO s (key, value) VALUES ('k', '{}')", x(99)),
            true,
        ),
        (
            format!("INSERT INTO s (key, value) VALUES ('k', '{}')", x(100)),
            false,
        ),
        (
            format!("INSERT INTO s (key, value) VALUES ('k', 'it''s{}')", x(95)),
            true,
        ),
        (
            format!(r"INSERT INTO s (key, value) VALUES ('k', '\n{}')", x(98)),
            true,
        ),
        (
            format!(r"INSERT INTO s (key, value) VALUES ('k', '\n{}')", x(99)),
            false,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', _utf8mb4 X'{}')",
                hex(99)
            ),
            true,
        ),
        // The engine tier's form: before, any size went through.
        (
            format!(
                "INSERT INTO s (key, value) VALUES (_utf8mb4 X'6B', _utf8mb4 X'{}')",
                hex(100)
            ),
            false,
        ),
        (
            format!("INSERT INTO s (key, value) VALUES ('k', 0x{})", hex(100)),
            false,
        ),
        (
            format!("INSERT INTO s (key, value) VALUES ('k', X'{}')", hex(99)),
            true,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', B'{}')",
                "1".repeat(800)
            ),
            false,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', _binary'{}')",
                x(100)
            ),
            false,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', N'{}')",
                "é".repeat(50)
            ),
            false,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', CONCAT('{}', '{}'))",
                x(50),
                x(50)
            ),
            false,
        ),
        (
            format!(
                "INSERT INTO s (key, value) VALUES ('k', '{}' '{}')",
                x(50),
                x(49)
            ),
            true,
        ),
        (
            format!(
                "INSERT INTO s VALUES ('k', 'v', '{{\"h\": \"{}\"}}')",
                x(89)
            ),
            true,
        ),
        (
            format!(
                "INSERT INTO s VALUES ('k', 'v', '{{\"h\": \"{}\"}}')",
                x(90)
            ),
            false,
        ),
        // Evaluated by MariaDB, then measured.
        (
            "INSERT INTO s (key, value) VALUES ('k', REPEAT('x', 99))".into(),
            true,
        ),
        (
            "INSERT INTO s (key, value) VALUES ('k', REPEAT('x', 100))".into(),
            false,
        ),
        (
            "INSERT INTO s (key, value) VALUES ('k', CONCAT('a', REPEAT('x', 99)))".into(),
            false,
        ),
        (
            "INSERT INTO s VALUES ('k', 'v', JSON_OBJECT('h', REPEAT('a', 89)))".into(),
            true,
        ),
        (
            "INSERT INTO s VALUES ('k', 'v', JSON_OBJECT('h', REPEAT('a', 90)))".into(),
            false,
        ),
        (
            "INSERT INTO s (key, value) VALUES ('a', 'b'), ('k', REPEAT('x', 100))".into(),
            false,
        ),
        (
            "INSERT INTO s (key, value) SELECT NULL, body FROM src WHERE id = 2".into(),
            true,
        ),
        (
            "INSERT INTO s (key, value) SELECT 'k', body FROM src WHERE id = 2".into(),
            false,
        ),
        (
            "INSERT INTO s (key, value) SELECT 'k', body FROM src WHERE id = 1".into(),
            true,
        ),
        (
            "INSERT INTO s (key, value) SELECT 'k', body FROM src WHERE id = 99".into(),
            true,
        ),
        (
            "INSERT INTO s (key, value) VALUES (REPEAT('k', 4097), 'v')".into(),
            false,
        ),
    ];
    let mut published = 0usize;
    for (q, ok) in &cases {
        let r = sql(&mut c, q).await;
        let short: String = q.chars().take(90).collect();
        match (ok, &r) {
            (true, Ok(_)) => {
                published += usize::from(!q.ends_with("id = 99"));
                ck.check(&format!("passes: {short}"), true, "");
            }
            (false, Err((39944, m))) => ck.check(&format!("39944: {short}"), true, m),
            _ => ck.check(&short, false, format!("{r:?}")),
        }
    }

    // A variable's value is measured too.
    must(&mut c, "SET @big = REPEAT('x', 100)").await;
    let r = sql(&mut c, "INSERT INTO s (key, value) VALUES ('k', @big)").await;
    ck.check(
        "a user variable's value is measured",
        matches!(&r, Err((39944, _))),
        format!("{r:?}"),
    );
    // On a compacted stream, an evaluated record without a key is 39955.
    let r = sql(
        &mut c,
        "INSERT INTO latest (key, value) VALUES (NULL, REPEAT('x', 3))",
    )
    .await;
    ck.check(
        "an evaluated keyless record on a compacted stream is 39955",
        matches!(&r, Err((39955, _))),
        format!("{r:?}"),
    );
    // In a transaction, a refused row leaves the earlier ones to the
    // client's ROLLBACK or COMMIT, as any failed statement does.
    must(&mut c, "START TRANSACTION").await;
    must(
        &mut c,
        "INSERT INTO s (key, value) VALUES ('t', REPEAT('t', 10))",
    )
    .await;
    let r = sql(
        &mut c,
        "INSERT INTO s (key, value) VALUES ('t', REPEAT('t', 100))",
    )
    .await;
    must(&mut c, "ROLLBACK").await;
    ck.check(
        "refused inside a transaction",
        matches!(&r, Err((39944, _))),
        format!("{r:?}"),
    );

    // The stream's own value wins over the database's.
    must(&mut c, "ALTER STREAM s SET (stream.max_message_bytes = 50)").await;
    let r1 = sql(
        &mut c,
        &format!("INSERT INTO s (key, value) VALUES ('k', '{}')", x(49)),
    )
    .await;
    let r2 = sql(
        &mut c,
        &format!("INSERT INTO s (key, value) VALUES ('k', '{}')", x(50)),
    )
    .await;
    ck.check(
        "ALTER STREAM ... SET (stream.max_message_bytes = 50) applies to SQL",
        r1.is_ok() && matches!(&r2, Err((39944, _))),
        format!("{r1:?} / {r2:?}"),
    );
    published += usize::from(r1.is_ok());
    must(&mut c, "ALTER STREAM s RESET (stream.max_message_bytes)").await;

    // Prepared, with parameters: measured at each execute.
    // Not UTF-8: written in as X'..'.
    let bin: Vec<u8> = (157..=255u8).collect();
    let r1 = c
        .exec_drop(
            "INSERT INTO s (key, value) VALUES (?, ?)",
            ("p", bin.clone()),
        )
        .await;
    let r2 = c
        .exec_drop(
            "INSERT INTO s (key, value) VALUES (?, ?)",
            ("pp", bin.clone()),
        )
        .await;
    let r3 = c
        .exec_drop(
            "INSERT INTO s (key, value, headers) VALUES (?, ?, ?)",
            ("p", "v", format!("{{\"h\": \"{}\"}}", x(80))),
        )
        .await;
    ck.check(
        "prepared: 100 bytes pass, 101 are 39944",
        r1.is_ok()
            && matches!(&r2, Err(mysql_async::Error::Server(e)) if e.code == 39944)
            && r3.is_ok(),
        format!("{r1:?} / {r2:?} / {r3:?}"),
    );
    published += usize::from(r1.is_ok()) + usize::from(r3.is_ok());

    // Everything that passed is in the stream, byte for byte.
    let deadline = Instant::now() + Duration::from_secs(40);
    let rows = loop {
        let rows = must(&mut c, "SELECT key, value FROM s ORDER BY offset_no").await;
        if rows.len() >= published || Instant::now() > deadline {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    ck.check(
        &format!("the stream holds the {published} records that passed"),
        rows.len() == published,
        format!("{} rows", rows.len()),
    );
    ck.check(
        "a prepared binary value reads back as sent",
        rows.iter()
            .any(|r| r[0].as_deref() == Some(b"p") && r[1].as_deref() == Some(&bin[..])),
        "missing",
    );
    ck.check(
        "an INSERT ... SELECT value reads back as the table holds it",
        rows.iter()
            .any(|r| r[0].is_none() && r[1].as_deref() == Some(&[b'b'; 100][..])),
        "missing",
    );

    // `amqp.max_message_bytes` on the SQL publish into a queue.
    let set = sql(
        &mut c,
        "ALTER TENANT SPRIG SET (amqp.max_message_bytes = 100)",
    )
    .await;
    if set.is_ok() {
        let r1 = sql(
            &mut c,
            "CALL sprig.mq_publish('', 'sf7_nowhere', REPEAT('x', 100), NULL, 0, 'sf7_size')",
        )
        .await;
        let r2 = sql(
            &mut c,
            "CALL sprig.mq_publish('', 'sf7_nowhere', REPEAT('x', 101), NULL, 0, 'sf7_size')",
        )
        .await;
        ck.check(
            "sprig.mq_publish: 100 bytes route, 101 are 39257",
            r1.is_ok() && matches!(&r2, Err((39257, _))),
            format!("{r1:?} / {r2:?}"),
        );
        must(&mut c, "ALTER TENANT SPRIG RESET (amqp.max_message_bytes)").await;
    } else {
        println!("skipped the queue check: {set:?}");
    }

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!("{} checks, {} failed", ck.n, ck.failed.len());
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
