//! The wall budget of nested logic calls, through a live gateway: a caller
//! compiled into MariaDB and its engine-tier twin each call a compiled
//! callee that sleeps, under `logic.function_budget_ms = 1000` for the
//! database. In both tiers the caller's budget covers its nested calls:
//! calls that fit run, calls that don't are stopped near the budget.
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:46306
//! export SPRIG_TEST_LOGIC_USER=sf7acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! cargo test -p sprig-gateway --test logic_call_budget_live -- --nocapture
//! ```
//!
//! Works in the database `sf7_budget`, dropped afterwards.

#![allow(clippy::unwrap_used, missing_docs)]

use std::time::Instant;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "sf7_budget";

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

async fn run(c: &mut Conn, q: &str) -> Result<(), (u16, String)> {
    match c.query_drop(q).await {
        Ok(()) => Ok(()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) {
    run(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

#[tokio::test]
async fn nested_calls_run_inside_the_callers_wall_budget() {
    let Some(mut c) = connect("nested_calls_run_inside_the_callers_wall_budget").await else {
        return;
    };
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    // Read at deploy by the compiled tier, at each call by the engine tier.
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (logic.function_budget_ms = 1000)"),
    )
    .await;
    must(
        &mut c,
        "CREATE FUNCTION nap(ms INT) LANGUAGE javascript AS $$
           const r = db.sqlOne('SELECT SLEEP(?) AS s', [ms / 1000]);
           return { s: r.s };
         $$",
    )
    .await;
    let body = "let t = 0;
       for (let i = 0; i < n; i++) { db.call('nap', 300); t = t + 1; }
       return { t };";
    must(
        &mut c,
        &format!("CREATE PROCEDURE naps_c(n INT) LANGUAGE javascript AS $${body}$$"),
    )
    .await;
    must(
        &mut c,
        &format!(
            "CREATE PROCEDURE naps_e(n INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $${body}$$"
        ),
    )
    .await;
    let mut failed = Vec::new();
    for tier in ["c", "e"] {
        for (n, fits) in [(2, true), (6, false)] {
            let t0 = Instant::now();
            let r = run(&mut c, &format!("CALL naps_{tier}({n})")).await;
            let ms = t0.elapsed().as_millis();
            println!("naps_{tier}({n}): {ms} ms, {r:?}");
            let ok = if fits {
                r.is_ok()
            } else {
                // Stopped near the budget (one nested statement may finish
                // past it in the engine tier), never after all 1.8 s.
                matches!(&r, Err((1969 | 39864, _))) && ms < 1700
            };
            if !ok {
                failed.push(format!("naps_{tier}({n}): {ms} ms, {r:?}"));
            }
        }
    }
    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    assert!(failed.is_empty(), "{failed:#?}");
}
