//! Row-level security decides alike on a table and on what is read from
//! somewhere else, live: change records (an `ENUM`'s index, a `SET`'s
//! bitmap, a `TIMESTAMP` in UTC, a large `DECIMAL`, binary strings as
//! base64, a `JSON` column as text), and MariaDB's side (policy views
//! through stored code, change topic filter views) against the gateway's
//! rewrite, with user attributes of every JSON type (a boolean, a number,
//! an array).
//!
//! The trap behind it: MariaDB 11.8's `JSON_VALUE` reads JSON `true` as
//! `1`, and a value read in another type than the table's compares
//! differently (`'2' <> 'secret'` shows a secret row; `07 = '7'` as a
//! number but not as text), so a policy can show on one path a row it
//! hides on another.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:41306
//! export SPRIG_TEST_RLS_TENANT=acme
//! cargo test -p sprig-gateway --test rls_coerce_live -- --nocapture --test-threads 1
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;
use std::time::Duration;

use mysql_async::prelude::Queryable;

const PW: &str = "Rlsy-suite-pw-1";
const DB: &str = "rlsy_suite";
const ALICE: &str = "rlsy_alice";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if !cond {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

async fn login(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn ids(c: &mut mysql_async::Conn, q: &str) -> Result<BTreeSet<i64>, String> {
    c.query::<Option<i64>, _>(q)
        .await
        .map(|v| v.into_iter().flatten().collect())
        .map_err(|e| e.to_string())
}

/// The ids of the change records' after images (inserts) a read of `from`
/// returns.
async fn record_ids(c: &mut mysql_async::Conn, from: &str) -> Result<BTreeSet<i64>, String> {
    ids(
        c,
        &format!(
            "SELECT CAST(JSON_VALUE(CAST(value AS CHAR), '$.after.id') AS SIGNED) FROM {from} \
             WHERE JSON_VALUE(CAST(value AS CHAR), '$.op') = 'c'"
        ),
    )
    .await
}

/// What every path must show alice: her rows, minus each one a
/// restrictive policy hides on the table.
const SHOWN: [i64; 2] = [1, 9];

#[ignore = "live: needs SPRIG_TEST_RLS_SQL; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_path_decides_as_the_table() {
    let Ok(addr) = std::env::var("SPRIG_TEST_RLS_SQL") else {
        eprintln!("skipped: set SPRIG_TEST_RLS_SQL");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut admin = login(&addr, "app", "sprig-dev-app").await;
    // Rows, by what should hide them from alice:
    //  1 nothing                      2 st = 'secret' (an ENUM, index 2 in records)
    //  3 tags has 'secret' (a SET)    4 doc.deleted is JSON true
    //  5 amount past the cap (a DECIMAL a double can't hold)
    //  6 not alice's                  7 ts after 2030 at +05:30 but not in UTC (records carry UTC)
    //  8 b = 'secret' (VARBINARY, base64 in records)
    //  9 code '07': not her number 7 as text  10 code '7': her number
    let rows = [
        "(1, 'alice', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k1', 'x')",
        "(2, 'alice', 'secret', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k2', 'x')",
        "(3, 'alice', 'public', 'secret,b', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k3', 'x')",
        "(4, 'alice', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": true}', 'k4', 'x')",
        "(5, 'alice', 'public', 'a', '2029-12-30 00:00:00', 12345678901234567890.1234567891, '{\"deleted\": false}', 'k5', 'x')",
        "(6, 'bob', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k6', 'x')",
        "(7, 'alice', 'public', 'a', '2029-12-31 20:00:00', 1, '{\"deleted\": false}', 'k7', 'x')",
        "(8, 'alice', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'secret', 'x')",
        "(9, 'alice', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k9', '07')",
        "(10, 'alice', 'public', 'a', '2029-12-30 00:00:00', 1, '{\"deleted\": false}', 'k10', '7')",
    ];
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("DROP USER IF EXISTS '{ALICE}'@'%'"),
        format!("CREATE DATABASE {DB}"),
        format!(
            "CREATE TABLE {DB}.t (id INT PRIMARY KEY, owner VARCHAR(16) NOT NULL, \
             st ENUM('public','secret','draft') NOT NULL, tags SET('a','secret','b') NOT NULL, \
             ts TIMESTAMP(3) NULL, amount DECIMAL(30,10) NOT NULL, doc JSON NOT NULL, \
             b VARBINARY(16) NOT NULL, code VARCHAR(8) NOT NULL)"
        ),
        format!("ALTER TABLE {DB}.t SPRIG SET (stream.change_topic = on)"),
        format!(
            "CREATE USER '{ALICE}'@'%' IDENTIFIED BY '{PW}' ATTRIBUTE \
             '{{\"num\": 7, \"vip\": true, \"groups\": [\"g1\", \"g2\"]}}'"
        ),
        format!("GRANT SELECT ON {DB}.* TO '{ALICE}'@'%'"),
        // Her rows, for a member of g1 who is a vip.
        format!(
            "CREATE POLICY mine ON {DB}.t USING (owner = 'alice' AND \
             JSON_CONTAINS(SPRIG_USER_ATTR('groups'), '\"g1\"') AND SPRIG_USER_ATTR('vip') = '1')"
        ),
        format!("CREATE POLICY no_secret ON {DB}.t AS RESTRICTIVE USING (st <> 'secret')"),
        format!(
            "CREATE POLICY no_secret_tag ON {DB}.t AS RESTRICTIVE USING \
             (FIND_IN_SET('secret', tags) = 0)"
        ),
        format!(
            "CREATE POLICY not_deleted ON {DB}.t AS RESTRICTIVE USING \
             (NOT (JSON_VALUE(doc, '$.deleted') <=> 1))"
        ),
        format!(
            "CREATE POLICY capped ON {DB}.t AS RESTRICTIVE USING \
             (amount < 12345678901234567000.5)"
        ),
        // Published by the start of 2030 in the reader's time zone.
        format!(
            "CREATE POLICY published ON {DB}.t AS RESTRICTIVE USING \
             (ts <= TIMESTAMP '2030-01-01 00:00:00')"
        ),
        format!("CREATE POLICY no_secret_key ON {DB}.t AS RESTRICTIVE USING (b <> 'secret')"),
        format!(
            "CREATE POLICY not_my_number ON {DB}.t AS RESTRICTIVE USING \
             (code <> SPRIG_USER_ATTR('num'))"
        ),
        format!("ALTER TABLE {DB}.t ENABLE ROW LEVEL SECURITY"),
        // Stored code reads the policy views: MariaDB's side decides.
        format!("CREATE SQL SECURITY INVOKER VIEW {DB}.v AS SELECT id FROM {DB}.t"),
        format!("CREATE VIEW {DB}.vch AS SELECT * FROM {DB}.`t.changes`"),
        format!("GRANT SELECT ON {DB}.vch TO '{ALICE}'@'%'"),
    ] {
        let r = admin.query_drop(&q).await;
        ck.ok(&format!("setup: {q}"), r.is_ok(), || format!("{r:?}"));
    }
    // Inserted after the change topic is on: one record each, written in
    // UTC. Alice reads at +05:30.
    admin.query_drop("SET time_zone = '+00:00'").await.unwrap();
    let r = admin
        .query_drop(format!(
            "INSERT INTO {DB}.t (id, owner, st, tags, ts, amount, doc, b, code) VALUES {}",
            rows.join(", ")
        ))
        .await;
    ck.ok("insert rows", r.is_ok(), || format!("{r:?}"));
    let mut alice = login(&addr, ALICE, PW).await;
    alice.query_drop("SET time_zone = '+05:30'").await.unwrap();
    let want: BTreeSet<i64> = SHOWN.into_iter().collect();

    // 1. The gateway's rewrite (and the policy view behind it).
    let got = ids(&mut alice, &format!("SELECT id FROM {DB}.t")).await;
    ck.ok("rewrite: SELECT", got.as_ref() == Ok(&want), || {
        format!("{got:?}, want {want:?}")
    });
    // 2. MariaDB's side: stored code over the policy views.
    let got = ids(&mut alice, &format!("SELECT id FROM {DB}.v")).await;
    ck.ok(
        "MariaDB's side: a view over the table",
        got.as_ref() == Ok(&want),
        || format!("{got:?}, want {want:?}"),
    );
    // 3. Change records through the rewrite (the topic read as SQL), once
    // the sequencer copied them in.
    let mut got = Err(String::new());
    let mut raw = 0usize;
    for _ in 0..60 {
        raw = admin
            .query::<i64, _>(format!("SELECT COUNT(*) FROM {DB}.`t.changes`"))
            .await
            .ok()
            .and_then(|v| v.first().copied())
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        if raw >= rows.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.ok(
        "the change topic has every record",
        raw >= rows.len(),
        || format!("{raw} records"),
    );
    if raw >= rows.len() {
        got = record_ids(&mut alice, &format!("{DB}.`t.changes`")).await;
    }
    ck.ok(
        "change records through the rewrite",
        got.as_ref() == Ok(&want),
        || format!("{got:?}, want {want:?}"),
    );
    // 4. Change records through MariaDB's side: the topic's filter view
    // under a view.
    let got = record_ids(&mut alice, &format!("{DB}.vch")).await;
    ck.ok(
        "change records through the filter view",
        got.as_ref() == Ok(&want),
        || format!("{got:?}, want {want:?}"),
    );
    // A record carries a DECIMAL a double can't hold as its exact text.
    let amount: Option<String> = admin
        .query_first(format!(
            "SELECT JSON_EXTRACT(CAST(value AS CHAR), '$.after.amount') FROM {DB}.`t.changes` \
             WHERE JSON_VALUE(CAST(value AS CHAR), '$.after.id') = '5'"
        ))
        .await
        .ok()
        .flatten();
    ck.ok(
        "a large DECIMAL keeps its digits in the record",
        amount.as_deref() == Some("\"12345678901234567890.1234567891\""),
        || format!("{amount:?}"),
    );

    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("DROP USER IF EXISTS '{ALICE}'@'%'"),
    ] {
        let _ = admin.query_drop(&q).await;
    }
    eprintln!(
        "type coercion across paths: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
