//! `MATCH ... AGAINST` on a search index through SQL's prepared paths and
//! `EXPLAIN`, live, for a tenant without sharded tables:
//!
//! - `PREPARE ... FROM` a text with `MATCH ... AGAINST (?)` or a literal
//!   (from a string or a variable), `EXECUTE ... USING`, `EXECUTE
//!   IMMEDIATE` and the driver's `COM_STMT_PREPARE` answer what the plain
//!   statement answers (before, the SQL-level ones reached MariaDB as
//!   written and failed with 1191: it has no `FULLTEXT` index there);
//!   `DEALLOCATE` forgets the statement; a `PREPARE` without a search
//!   still runs on MariaDB.
//! - With `search.max_window` below the matches, a top-results statement
//!   (`ORDER BY score DESC LIMIT n`) runs by reading the search a page at
//!   a time, and its `EXPLAIN` now does too (it failed with 39827); a
//!   statement that needs every hit fails, and so does its `EXPLAIN`.
//!
//! ```sh
//! export SPRIG_TEST_SQLS_SQL=127.0.0.1:49506
//! export SPRIG_TEST_SQLS_TENANT=acme              # user app
//! export SPRIG_TEST_SQLS_SETTING='<script>'       # <script> <key> <json value> | <script> <key> -
//! cargo test -p sprig-gateway --test search_sql_paths_live -- --nocapture
//! ```
//!
//! Works in the database `search_sql_live`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "search_sql_live";
const ROWS: u32 = 300;

struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl std::fmt::Display) {
        if cond {
            println!("ok   {what}: {detail}");
        } else {
            println!("FAIL {what}: {detail}");
            self.failed.push(format!("{what}: {detail}"));
        }
    }
}

async fn setting(key: &str, value: &str) -> bool {
    let Ok(script) = std::env::var("SPRIG_TEST_SQLS_SETTING") else {
        return false;
    };
    let ok = std::process::Command::new(&script)
        .args([key, value])
        .status()
        .is_ok_and(|s| s.success());
    tokio::time::sleep(Duration::from_millis(1500)).await;
    ok
}

async fn ints(c: &mut mysql_async::Conn, q: &str) -> Result<Vec<i64>, String> {
    c.query::<i64, _>(q).await.map_err(|e| e.to_string())
}

/// Every cell of every row, as text.
async fn cells(c: &mut mysql_async::Conn, q: &str) -> Result<Vec<String>, String> {
    let rows: Vec<mysql_async::Row> = c.query(q).await.map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .flat_map(|r| {
            (0..r.len())
                .map(|i| {
                    r.get_opt::<Option<String>, _>(i)
                        .and_then(Result::ok)
                        .flatten()
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_through_prepare_and_explain() {
    let Ok(addr) = std::env::var("SPRIG_TEST_SQLS_SQL") else {
        eprintln!("skipped: set SPRIG_TEST_SQLS_SQL");
        return;
    };
    let tenant = std::env::var("SPRIG_TEST_SQLS_TENANT").unwrap_or_else(|_| "acme".into());
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    let mut c = mysql_async::Conn::new(opts).await.unwrap();
    let mut ck = Checks { failed: Vec::new() };
    c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
    c.query_drop(format!("USE {DB}")).await.unwrap();
    c.query_drop("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT NOT NULL)")
        .await
        .unwrap();
    let values: Vec<String> = (1..=ROWS)
        .map(|i| {
            let colour = if i % 3 == 0 { "red" } else { "blue" };
            let extra = "shoe ".repeat(1 + (i % 7) as usize);
            format!("({i}, '{colour} {extra}number {i}')")
        })
        .collect();
    c.query_drop(format!("INSERT INTO docs VALUES {}", values.join(", ")))
        .await
        .unwrap();
    c.query_drop("CREATE SEARCH INDEX docs_ix ON docs (body TEXT)")
        .await
        .unwrap();
    let t = Instant::now();
    let plain_count = "SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST ('red')";
    let want_red = u64::from(ROWS / 3);
    loop {
        let n = ints(&mut c, plain_count).await.unwrap_or_default();
        if n.first().copied() == Some(i64::try_from(want_red).unwrap())
            || t.elapsed() > Duration::from_secs(60)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let plain = ints(&mut c, plain_count).await;
    ck.ok(
        "the plain statement finds every red row",
        plain.as_deref() == Ok(&[i64::try_from(want_red).unwrap()][..]),
        format!("{plain:?} after {:.1} s", t.elapsed().as_secs_f64()),
    );
    let plain_ids = ints(
        &mut c,
        "SELECT id FROM docs WHERE MATCH (body) AGAINST ('red') ORDER BY id LIMIT 5",
    )
    .await
    .unwrap_or_default();

    // ---- PREPARE / EXECUTE / EXECUTE IMMEDIATE / COM_STMT_PREPARE
    c.query_drop(
        "PREPARE p1 FROM 'SELECT id FROM docs WHERE MATCH (body) AGAINST (?) ORDER BY id LIMIT 5'",
    )
    .await
    .unwrap_or_else(|e| println!("     PREPARE p1: {e}"));
    c.query_drop("SET @q = 'red'").await.unwrap();
    let r = ints(&mut c, "EXECUTE p1 USING @q").await;
    ck.ok(
        "PREPARE with AGAINST (?), EXECUTE USING a variable",
        r.as_ref().is_ok_and(|v| *v == plain_ids),
        format!("{r:?}, want {plain_ids:?}"),
    );
    let r = ints(&mut c, "EXECUTE p1 USING 'red'").await;
    ck.ok(
        "EXECUTE USING a literal",
        r.as_ref().is_ok_and(|v| *v == plain_ids),
        format!("{r:?}"),
    );
    let r = match c
        .query_drop(
            "PREPARE p2 FROM 'SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST (''red'')'",
        )
        .await
    {
        Ok(()) => ints(&mut c, "EXECUTE p2").await,
        Err(e) => Err(e.to_string()),
    };
    ck.ok(
        "PREPARE with a literal AGAINST, EXECUTE",
        r == plain,
        format!("{r:?}"),
    );
    c.query_drop(
        "SET @s = 'SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST (?) AND id > 150'",
    )
    .await
    .unwrap();
    let r = match c.query_drop("PREPARE p3 FROM @s").await {
        Ok(()) => ints(&mut c, "EXECUTE p3 USING 'red'").await,
        Err(e) => Err(e.to_string()),
    };
    let want3 = ints(
        &mut c,
        "SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST ('red') AND id > 150",
    )
    .await;
    ck.ok(
        "PREPARE FROM a variable",
        r.is_ok() && r == want3,
        format!("{r:?}, want {want3:?}"),
    );
    let r = ints(
        &mut c,
        "EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST (?)' USING 'red'",
    )
    .await;
    ck.ok("EXECUTE IMMEDIATE with USING", r == plain, format!("{r:?}"));
    let r = ints(
        &mut c,
        "EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST (''red'')'",
    )
    .await;
    ck.ok(
        "EXECUTE IMMEDIATE of a literal",
        r == plain,
        format!("{r:?}"),
    );
    let r: Result<Vec<i64>, String> = c
        .exec(
            "SELECT COUNT(*) FROM docs WHERE MATCH (body) AGAINST (?)",
            ("red",),
        )
        .await
        .map_err(|e| e.to_string());
    ck.ok(
        "the driver's COM_STMT_PREPARE with AGAINST (?)",
        r == plain,
        format!("{r:?}"),
    );
    c.query_drop("DEALLOCATE PREPARE p1").await.unwrap();
    let r = ints(&mut c, "EXECUTE p1 USING @q").await;
    ck.ok(
        "EXECUTE after DEALLOCATE: 1243",
        r.as_ref().is_err_and(|e| e.contains("1243")),
        format!("{r:?}"),
    );
    let r = match c
        .query_drop("PREPARE p4 FROM 'SELECT COUNT(*) FROM docs'")
        .await
    {
        Ok(()) => ints(&mut c, "EXECUTE p4").await,
        Err(e) => Err(e.to_string()),
    };
    ck.ok(
        "a PREPARE without a search runs as before",
        r == Ok(vec![i64::from(ROWS)]),
        format!("{r:?}"),
    );
    for p in ["p2", "p3", "p4"] {
        let _ = c.query_drop(format!("DEALLOCATE PREPARE {p}")).await;
    }

    // ---- EXPLAIN of a top-results statement past search.max_window
    if setting("search.max_window", "100").await {
        let top = "SELECT id, MATCH (body) AGAINST ('shoe') AS s FROM docs ORDER BY s DESC LIMIT 3";
        let direct = cells(&mut c, top).await;
        ck.ok(
            "the top-results statement runs past the window",
            direct.as_ref().is_ok_and(|v| v.len() == 6),
            format!("{direct:?}"),
        );
        let e = cells(&mut c, &format!("EXPLAIN {top}")).await;
        ck.ok(
            "its EXPLAIN runs too, with the search's row",
            e.as_ref()
                .is_ok_and(|v| v.iter().any(|x| x.contains("docs_ix"))),
            format!("{e:?}"),
        );
        let e = cells(&mut c, &format!("EXPLAIN FORMAT=JSON {top}")).await;
        ck.ok(
            "EXPLAIN FORMAT=JSON too",
            e.as_ref()
                .is_ok_and(|v| v.iter().any(|x| x.contains("docs_ix"))),
            format!(
                "{:?}",
                e.map(|v| v.join(" ").chars().take(300).collect::<String>())
            ),
        );
        let every = "SELECT id FROM docs WHERE MATCH (body) AGAINST ('shoe')";
        let s = cells(&mut c, every).await;
        let e = cells(&mut c, &format!("EXPLAIN {every}")).await;
        ck.ok(
            "a statement that needs every hit, and its EXPLAIN, both fail past the window",
            s.as_ref().is_err_and(|x| x.contains("39827"))
                && e.as_ref().is_err_and(|x| x.contains("39827")),
            format!("{s:?} / {e:?}"),
        );
        setting("search.max_window", "-").await;
    } else {
        println!("     (no SPRIG_TEST_SQLS_SETTING: the EXPLAIN part isn't run)");
    }

    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
