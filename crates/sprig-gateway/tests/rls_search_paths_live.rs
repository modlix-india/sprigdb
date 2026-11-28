//! Search read paths under row policies, live: each path a user a policy
//! binds may take must show only rows it may read, and paths built for
//! unbound callers must not open to bound ones.
//!
//! The administrator (`<tenant>.app`) makes users `rlsp_alice`,
//! `rlsp_bob` (both bound by a policy showing each its own rows) and
//! `rlsp_carol` (no grants), and in each model rows owned by alice or bob:
//!
//! ```text
//! SQL table   rlsp_suite.items   owner = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)
//!             search index items_ix (name TEXT, body TEXT, name SUBSTRING)
//! collection  rlsp_suite.mdocs   {owner: "$$USER"}, text index on title
//! Redis       redis.db0          k LIKE CONCAT('rlsp:', <user>, ':%')
//! ```
//!
//! Checked:
//!
//! - **Top-k `MATCH`** (`... ORDER BY score DESC LIMIT n`): with
//!   `search.max_window` below the number of matches, the administrator's
//!   statement pages through the copy and answers as with a large window;
//!   alice's (bound) takes the old path: only her rows, refused past the
//!   window as before; carol gets a privilege error, not rows.
//! - **`LIKE` acceleration** (`search.like_acceleration`): the
//!   administrator's narrowed `LIKE` answers exactly what the plain scan
//!   does, including a row written just before; alice's `LIKE` is never
//!   narrowed and shows only her rows; carol is refused.
//! - **Redis `HIGHLIGHT`, `SUMMARIZE` and `WITHCURSOR`**: alice's replies
//!   hold only her keys and her values; her cursor is hers alone (bob and
//!   the administrator get "Cursor not found").
//! - **`explain` of `findAndModify`**: alice's counts only her documents,
//!   MariaDB's plan is refused to her, and nothing is written.
//! - **`textScore`**: MongoDB's formula, from the document alone: alice's
//!   score of her document equals the administrator's and doesn't move when
//!   bob adds rows.
//! - **Change-stream counters**: `serverStatus`' `docsReturned` and
//!   `docsExamined` grow by the events alice's stream delivers, not by
//!   bob's events she can't see.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:36306
//! export SPRIG_TEST_RLS_MONGO=127.0.0.1:36737
//! export SPRIG_TEST_RLS_REDIS=127.0.0.1:36380
//! export SPRIG_TEST_RLS_TENANT=rs3a
//! export SPRIG_TEST_RLS_SETTING='<script>'   # <script> <key> <json value> | <script> <key> -
//! cargo test -p sprig-gateway --test rls_search_paths_live -- --nocapture
//! ```
//!
//! Everything it makes is dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::BTreeSet;
use std::time::Duration;

use futures::StreamExt;
use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "Rlsp-suite-pw-1";
const DB: &str = "rlsp_suite";
const ALICE: &str = "rlsp_alice";
const BOB: &str = "rlsp_bob";
const CAROL: &str = "rlsp_carol";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            println!("ok {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_RLS_TENANT").unwrap_or_else(|_| "acme".into())
}

fn admin_pw() -> String {
    std::env::var("SPRIG_TEST_RLS_ADMIN_PW").unwrap_or_else(|_| "sprig-dev-app".into())
}

async fn sql(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
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

async fn redis(addr: &str, user: &str, pw: &str) -> redis::aio::MultiplexedConnection {
    let url = format!("redis://{}.{user}:{pw}@{addr}", tenant());
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn mongo(addr: &str, user: &str, pw: &str) -> mongodb::Client {
    let uri = format!(
        "mongodb://{}.{user}:{pw}@{addr}/?authSource=admin&directConnection=true",
        tenant()
    );
    mongodb::Client::with_uri_str(&uri).await.unwrap()
}

async fn cmd(
    c: &mut redis::aio::MultiplexedConnection,
    args: &[&str],
) -> Result<redis::Value, String> {
    let mut k = redis::cmd(args[0]);
    for a in &args[1..] {
        k.arg(*a);
    }
    k.query_async(c).await.map_err(|e| e.to_string())
}

fn s(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::Int(i) => Some(i.to_string()),
        redis::Value::Double(d) => Some(d.to_string()),
        _ => None,
    }
}

/// Every string anywhere in a reply.
fn strings(v: &redis::Value, out: &mut Vec<String>) {
    match v {
        redis::Value::Array(a) | redis::Value::Set(a) => {
            for x in a {
                strings(x, out);
            }
        }
        redis::Value::Map(m) => {
            for (k, x) in m {
                strings(k, out);
                strings(x, out);
            }
        }
        other => out.extend(s(other)),
    }
}

fn all_strings(v: &redis::Value) -> Vec<String> {
    let mut out = Vec::new();
    strings(v, &mut out);
    out
}

/// Sets (or, with `-`, removes) a dc-scope setting through the script in
/// `SPRIG_TEST_RLS_SETTING`, then gives the gateway a moment to see it.
async fn setting(key: &str, value: &str) -> bool {
    let Ok(script) = std::env::var("SPRIG_TEST_RLS_SETTING") else {
        return false;
    };
    let ok = std::process::Command::new(&script)
        .args([key, value])
        .status()
        .is_ok_and(|s| s.success());
    tokio::time::sleep(Duration::from_millis(1500)).await;
    ok
}

/// The `sprig_search_like_total` and `sprig_search_top_k_total` counters of
/// the gateway's metrics page (`SPRIG_TEST_RLS_METRICS`, a URL), by label.
async fn metric(name: &str, label: &str) -> Option<f64> {
    let url = std::env::var("SPRIG_TEST_RLS_METRICS").ok()?;
    let (host, path) = url
        .strip_prefix("http://")?
        .split_once('/')
        .map(|(h, p)| (h.to_owned(), format!("/{p}")))?;
    let mut s = tokio::net::TcpStream::connect(&host).await.ok()?;
    s.write_all(format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n").as_bytes())
        .await
        .ok()?;
    let mut body = String::new();
    s.read_to_string(&mut body).await.ok()?;
    let want = format!("{name}{{result=\"{label}\"}}");
    Some(
        body.lines()
            .filter(|l| l.starts_with(&want))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum(),
    )
}

/// A SQL error's number, or 0 for success.
fn errno<T>(r: &Result<T, mysql_async::Error>) -> u16 {
    match r {
        Ok(_) => 0,
        Err(mysql_async::Error::Server(e)) => e.code,
        Err(_) => 1,
    }
}

fn owner_of(id: i64) -> &'static str {
    if id < 1000 { ALICE } else { BOB }
}

/// Alice's rows 1..=6; bob's 1001..=1120: every name holds "common"
/// (bob's twice, so his rows rank first), with lengths that differ.
fn name_of(id: i64) -> String {
    if id < 1000 {
        format!("red widget {id} common")
    } else {
        format!(
            "blue gadget {id} common common{}",
            " pad".repeat(((id - 1000) % 40) as usize)
        )
    }
}

/// A top-k answer's scores, rounded: rows that tie may come in any order.
fn scores_of(v: &[(i64, f64)]) -> Vec<i64> {
    v.iter().map(|x| (x.1 * 1e6).round() as i64).collect()
}

#[ignore = "live: needs SPRIG_TEST_RLS_SQL, SPRIG_TEST_RLS_MONGO, SPRIG_TEST_RLS_REDIS; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_paths_under_row_policies() {
    let (Ok(sql_addr), Ok(mongo_addr), Ok(redis_addr)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_MONGO"),
        std::env::var("SPRIG_TEST_RLS_REDIS"),
    ) else {
        eprintln!("SPRIG_TEST_RLS_SQL, _MONGO or _REDIS not set; skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let apw = admin_pw();
    let mut admin = sql(&sql_addr, "app", &apw).await;
    let am = mongo(&mongo_addr, "app", &apw).await;
    let adb = am.database(DB);
    let mut ar = redis(&redis_addr, "app", &apw).await;

    // Leftovers of an earlier run.
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    let _ = cmd(&mut ar, &["FT.DROPINDEX", "rlsp_idx", "DD"]).await;
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsp_own ON redis.db0")
        .await;
    for u in [ALICE, BOB, CAROL] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }

    // ---- Setup ----
    for st in [
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, owner VARCHAR(64), name VARCHAR(200), \
         body VARCHAR(200))"
            .to_owned(),
    ] {
        admin.query_drop(st).await.unwrap();
    }
    let ids: Vec<i64> = (1..=6).chain(1001..=1120).collect();
    let rows: Vec<String> = ids
        .iter()
        .map(|&i| format!("({i}, '{}', '{}', 'body {i}')", owner_of(i), name_of(i)))
        .collect();
    admin
        .query_drop(format!("INSERT INTO items VALUES {}", rows.join(", ")))
        .await
        .unwrap();
    admin
        .query_drop("INSERT INTO items VALUES (7, 'rlsp_alice', NULL, 'no name')")
        .await
        .unwrap();
    admin
        .query_drop("CREATE SEARCH INDEX items_ix ON items (name TEXT, body TEXT, name SUBSTRING)")
        .await
        .unwrap();
    for st in [
        "CREATE POLICY rlsp_own ON items USING (owner = SUBSTRING_INDEX(CURRENT_USER(), '@', 1))",
        "ALTER TABLE items ENABLE ROW LEVEL SECURITY",
    ] {
        admin.query_drop(st).await.unwrap();
    }
    let coll = adb.collection::<Document>("mdocs");
    coll.insert_many([
        doc! {"_id": 1, "owner": format!("{ALICE}@%"), "title": "apple note one", "n": 1},
        doc! {"_id": 2, "owner": format!("{ALICE}@%"), "title": "apple", "n": 2},
        doc! {"_id": 1001, "owner": format!("{BOB}@%"), "title": "apple apple pie", "n": 3},
    ])
    .await
    .unwrap();
    adb.run_command(doc! {"createIndexes": "mdocs", "indexes": [{"key": {"title": "text"}, "name": "title_text"}]})
        .await
        .unwrap();
    adb.run_command(
        doc! {"sprigCreatePolicy": "mdocs", "name": "own", "using": {"owner": "$$USER"}},
    )
    .await
    .unwrap();
    adb.run_command(doc! {"sprigRowSecurity": "mdocs", "enable": true})
        .await
        .unwrap();
    for (k, owner, title) in [
        ("rlsp:rlsp_alice:1", ALICE, "red shoe for running"),
        ("rlsp:rlsp_alice:2", ALICE, "a shoe"),
        ("rlsp:rlsp_bob:1", BOB, "secret shoe of bob"),
        ("rlsp:rlsp_bob:2", BOB, "bobs other shoe"),
    ] {
        cmd(&mut ar, &["HSET", k, "title", title, "owner", owner])
            .await
            .unwrap();
    }
    cmd(
        &mut ar,
        &[
            "FT.CREATE",
            "rlsp_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "rlsp:",
            "SCHEMA",
            "title",
            "TEXT",
            "owner",
            "TAG",
        ],
    )
    .await
    .unwrap();
    for st in [
        "CREATE POLICY rlsp_own ON redis.db0 USING (k LIKE CONCAT('rlsp:', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), ':%'))",
        "ALTER TABLE redis.db0 ENABLE ROW LEVEL SECURITY",
    ] {
        admin.query_drop(st).await.unwrap();
    }
    for u in [ALICE, BOB, CAROL] {
        admin
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
    }
    for u in [ALICE, BOB] {
        admin
            .query_drop(format!(
                "GRANT SELECT, INSERT, UPDATE, DELETE, WATCH ON {DB}.* TO '{u}'@'%'"
            ))
            .await
            .unwrap();
        admin
            .query_drop(format!("GRANT SELECT ON redis.* TO '{u}'@'%'"))
            .await
            .unwrap();
    }
    // The copies have every row.
    for _ in 0..120 {
        let n: Vec<i64> = admin
            .query("SELECT COUNT(*) FROM items WHERE MATCH (name) AGAINST ('common')")
            .await
            .unwrap_or_default();
        if n.first() == Some(&126) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let mut al = sql(&sql_addr, ALICE, PW).await;
    al.query_drop(format!("USE {DB}")).await.unwrap();
    let mut carol = sql(&sql_addr, CAROL, PW).await;
    let have_settings = std::env::var("SPRIG_TEST_RLS_SETTING").is_ok();

    // ---- Top-k MATCH pages through the copy ----
    let top = "SELECT id, MATCH (name) AGAINST ('common') AS s FROM items \
               WHERE MATCH (name) AGAINST ('common') ORDER BY s DESC LIMIT 5";
    let top_filtered = "SELECT id, MATCH (name) AGAINST ('common') AS s FROM items \
               WHERE MATCH (name) AGAINST ('common') AND id % 3 = 0 ORDER BY s DESC LIMIT 4";
    let full: Vec<(i64, f64)> = admin.query(top).await.unwrap();
    let full_f: Vec<(i64, f64)> = admin.query(top_filtered).await.unwrap();
    let alice_full: Vec<(i64, f64)> = al.query(top).await.unwrap();
    ck.ok(
        "alice's top-k: her rows only",
        !alice_full.is_empty() && alice_full.iter().all(|r| r.0 < 1000),
        || format!("{alice_full:?}"),
    );
    if have_settings {
        assert!(setting("search.max_window", "60").await);
        let paged_before = metric("sprig_search_top_k_total", "paged").await;
        let r: Result<Vec<(i64, f64)>, _> = admin.query(top).await;
        ck.ok(
            "admin top-k past the window: answered, same rows as with the whole window",
            r.as_ref()
                .is_ok_and(|v| v.len() == 5 && scores_of(v) == scores_of(&full)),
            || format!("{r:?} vs {full:?}"),
        );
        let r: Result<Vec<(i64, f64)>, _> = admin.query(top_filtered).await;
        ck.ok(
            "admin top-k with a residual filter past the window: several pages, same rows",
            r.as_ref().is_ok_and(|v| {
                v.len() == 4 && scores_of(v) == scores_of(&full_f) && v.iter().all(|x| x.0 % 3 == 0)
            }),
            || format!("{r:?} vs {full_f:?}"),
        );
        if let (Some(a), Some(b)) = (
            paged_before,
            metric("sprig_search_top_k_total", "paged").await,
        ) {
            ck.ok("top-k answered from pages (metric)", b >= a + 2.0, || {
                format!("{a} -> {b}")
            });
        }
        // Alice is bound: the old path, refused past the window, never
        // bob's rows.
        let r: Result<Vec<(i64, f64)>, _> = al.query(top).await;
        ck.ok(
            "alice's top-k past the window: refused (39827), no rows",
            errno(&r) == 39827,
            || format!("{r:?}"),
        );
        let r: Result<Vec<(i64, f64)>, _> = carol
            .query(format!(
                "SELECT id, MATCH (name) AGAINST ('common') AS s FROM {DB}.items ORDER BY s DESC LIMIT 5"
            ))
            .await;
        ck.ok(
            "carol's top-k: a privilege error",
            matches!(errno(&r), 1142 | 1044),
            || format!("{r:?}"),
        );
        assert!(setting("search.max_window", "-").await);
    } else {
        eprintln!("SPRIG_TEST_RLS_SETTING not set: the window and LIKE checks skipped");
    }

    // ---- LIKE acceleration ----
    let likes = [
        "SELECT id FROM items WHERE name LIKE '%idget 3%' ORDER BY id",
        "SELECT id FROM items WHERE name LIKE '%GADGET 10%' ORDER BY id",
        "SELECT id FROM items WHERE (name LIKE '%red%' OR id = 7) ORDER BY id",
        "SELECT id FROM items WHERE name LIKE 'blue%' AND name LIKE '%11_ common%' ORDER BY id",
        "SELECT id FROM items WHERE name LIKE '%ge\\_t%' ESCAPE '\\\\' ORDER BY id",
        "SELECT id FROM items WHERE name LIKE '%zz%' ORDER BY id",
    ];
    let mut plain = Vec::new();
    for q in likes {
        let v: Vec<i64> = admin.query(q).await.unwrap();
        plain.push(v);
    }
    let alice_plain: Vec<i64> = al
        .query("SELECT id FROM items WHERE name LIKE '%dget%' ORDER BY id")
        .await
        .unwrap();
    if have_settings {
        assert!(setting("search.like_acceleration", "true").await);
        let narrowed_before = metric("sprig_search_like_total", "narrowed").await;
        for (q, want) in likes.iter().zip(&plain) {
            let got: Result<Vec<i64>, _> = admin.query(*q).await;
            ck.ok(
                &format!("admin LIKE narrowed equals the scan: {q}"),
                got.as_ref().is_ok_and(|g| g == want),
                || format!("{got:?} vs {want:?}"),
            );
        }
        if let (Some(a), Some(b)) = (
            narrowed_before,
            metric("sprig_search_like_total", "narrowed").await,
        ) {
            ck.ok(
                "LIKE narrowed by the search index (metric)",
                b >= a + 4.0,
                || format!("{a} -> {b}"),
            );
        }
        // A row written just before is found (latest consistency).
        admin
            .query_drop("INSERT INTO items VALUES (8, 'rlsp_alice', 'fresh wqxz zqx', 'x')")
            .await
            .unwrap();
        let got: Vec<i64> = admin
            .query("SELECT id FROM items WHERE name LIKE '%wqxz zqx%'")
            .await
            .unwrap();
        ck.ok(
            "admin LIKE finds a row written just before",
            got == vec![8],
            || format!("{got:?}"),
        );
        // Alice: never narrowed, her rows only.
        let scan_before = metric("sprig_search_like_total", "scan").await;
        let got: Result<Vec<i64>, _> = al
            .query("SELECT id FROM items WHERE name LIKE '%dget%' ORDER BY id")
            .await;
        ck.ok(
            "alice's LIKE: her rows only, as the scan",
            got.as_ref()
                .is_ok_and(|g| g == &alice_plain && g.iter().all(|i| *i < 1000)),
            || format!("{got:?} vs {alice_plain:?}"),
        );
        let got: Result<Vec<i64>, _> = al
            .query("SELECT id FROM items WHERE name LIKE '%gadget 10%'")
            .await;
        ck.ok(
            "alice's LIKE on bob's words: nothing",
            got.as_ref().is_ok_and(Vec::is_empty),
            || format!("{got:?}"),
        );
        if let (Some(a), Some(b)) = (scan_before, metric("sprig_search_like_total", "scan").await) {
            ck.ok("alice's LIKE took the scan (metric)", b >= a + 2.0, || {
                format!("{a} -> {b}")
            });
        }
        let r: Result<Vec<i64>, _> = carol
            .query(format!(
                "SELECT id FROM {DB}.items WHERE name LIKE '%gadget%'"
            ))
            .await;
        ck.ok(
            "carol's LIKE: a privilege error",
            matches!(errno(&r), 1142 | 1044),
            || format!("{r:?}"),
        );
        assert!(setting("search.like_acceleration", "-").await);
    }

    // ---- Redis HIGHLIGHT, SUMMARIZE, WITHCURSOR ----
    let mut alr = redis(&redis_addr, ALICE, PW).await;
    let mut bobr = redis(&redis_addr, BOB, PW).await;
    let r = cmd(
        &mut alr,
        &[
            "FT.SEARCH",
            "rlsp_idx",
            "shoe",
            "HIGHLIGHT",
            "SUMMARIZE",
            "LEN",
            "3",
        ],
    )
    .await;
    let all = r.as_ref().map(all_strings).unwrap_or_default();
    ck.ok(
        "alice's HIGHLIGHT/SUMMARIZE: her keys and values only",
        r.is_ok()
            && all.iter().any(|x| x == "rlsp:rlsp_alice:1")
            && all.iter().any(|x| x.contains("<b>shoe</b>"))
            && !all.iter().any(|x| x.contains("bob")),
        || format!("{r:?}"),
    );
    let r = cmd(
        &mut alr,
        &[
            "FT.AGGREGATE",
            "rlsp_idx",
            "shoe",
            "LOAD",
            "1",
            "@title",
            "WITHCURSOR",
            "COUNT",
            "1",
        ],
    )
    .await;
    let (first, id) = match &r {
        Ok(redis::Value::Array(p)) if p.len() == 2 => {
            (all_strings(&p[0]), s(&p[1]).unwrap_or_default())
        }
        _ => (Vec::new(), String::new()),
    };
    ck.ok(
        "alice's WITHCURSOR: a cursor, her rows only",
        !id.is_empty() && id != "0" && !first.iter().any(|x| x.contains("bob")),
        || format!("{r:?}"),
    );
    for (who, c) in [("bob", &mut bobr), ("the administrator", &mut ar)] {
        let r = cmd(c, &["FT.CURSOR", "READ", "rlsp_idx", &id]).await;
        ck.ok(
            &format!("{who} can't read alice's cursor"),
            r.as_ref().is_err_and(|e| e.contains("Cursor not found")),
            || format!("{r:?}"),
        );
        let r = cmd(c, &["FT.CURSOR", "DEL", "rlsp_idx", &id]).await;
        ck.ok(
            &format!("{who} can't delete alice's cursor"),
            r.is_err(),
            || format!("{r:?}"),
        );
    }
    let r = cmd(&mut alr, &["FT.CURSOR", "READ", "rlsp_idx", &id]).await;
    let rest = r.as_ref().map(all_strings).unwrap_or_default();
    ck.ok(
        "alice reads the rest of her cursor: her rows only",
        r.is_ok()
            && rest.iter().any(|x| x.contains("shoe"))
            && !rest.iter().any(|x| x.contains("bob")),
        || format!("{r:?}"),
    );

    // ---- explain of findAndModify ----
    let alm = mongo(&mongo_addr, ALICE, PW).await;
    let aldb = alm.database(DB);
    let ex = |q: Document| {
        let aldb = aldb.clone();
        async move {
            aldb.run_command(doc! {"explain": {"findAndModify": "mdocs", "query": q, "update": {"$set": {"n": 99}}}, "verbosity": "executionStats"})
                .await
        }
    };
    let r = ex(doc! {"_id": 1001}).await;
    let es = r
        .as_ref()
        .ok()
        .and_then(|d| d.get_document("executionStats").ok().cloned())
        .unwrap_or_default();
    ck.ok(
        "alice's explain of findAndModify on bob's document: nothing matched",
        es.get_i64("nMatched").ok() == Some(0) && es.get_i64("nReturned").ok() == Some(0),
        || format!("{r:?}"),
    );
    let refused = r.as_ref().is_ok_and(|d| {
        d.get_document("queryPlanner")
            .and_then(|q| q.get_document("sprig"))
            .and_then(|s| s.get_array("statements"))
            .is_ok_and(|st| {
                st.iter().all(|x| {
                    x.as_document()
                        .and_then(|x| x.get_document("mariadb").ok())
                        .is_some_and(|m| m.contains_key("refused"))
                })
            })
    });
    ck.ok(
        "alice's explain of findAndModify: MariaDB's plan refused to her",
        refused,
        || format!("{r:?}"),
    );
    let r = ex(doc! {"_id": 1}).await;
    ck.ok(
        "alice's explain of findAndModify on her document: one matched",
        r.as_ref().is_ok_and(|d| {
            d.get_document("executionStats")
                .is_ok_and(|e| e.get_i64("nMatched").ok() == Some(1))
        }),
        || format!("{r:?}"),
    );
    let n99 = coll.count_documents(doc! {"n": 99}).await.unwrap();
    ck.ok("explain of findAndModify wrote nothing", n99 == 0, || {
        format!("{n99} documents changed")
    });

    // ---- textScore from the document alone ----
    let scores = |db: mongodb::Database| async move {
        let mut out = Vec::new();
        let mut cur = db
            .collection::<Document>("mdocs")
            .find(doc! {"$text": {"$search": "apple"}})
            .projection(doc! {"s": {"$meta": "textScore"}})
            .sort(doc! {"_id": 1})
            .await
            .unwrap();
        while let Some(d) = cur.next().await {
            let d = d.unwrap();
            out.push((
                d.get("_id").and_then(Bson::as_i32).map_or(-1, i64::from),
                d.get_f64("s").unwrap_or(f64::NAN),
            ));
        }
        out
    };
    let a1 = scores(aldb.clone()).await;
    let adm = scores(adb.clone()).await;
    // MongoDB 8.0.34: "apple note one" 0.6667, "apple" 1 (its term is
    // "appl", so the whole-string bonus doesn't apply).
    ck.ok(
        "alice's textScore: MongoDB's values for her documents",
        a1.len() == 2 && (a1[0].1 - 2.0 / 3.0).abs() < 1e-9 && (a1[1].1 - 1.0).abs() < 1e-9,
        || format!("{a1:?}"),
    );
    ck.ok(
        "the administrator's textScore of alice's documents is the same",
        adm.iter()
            .filter(|x| x.0 < 1000)
            .copied()
            .collect::<Vec<_>>()
            == a1
            && adm
                .iter()
                .any(|x| x.0 == 1001 && (x.1 - 1.5 * (0.5 * 2.0 / 3.0 + 0.5)).abs() < 1e-9),
        || format!("{adm:?}"),
    );
    coll.insert_many(
        (2001..2021).map(
            |i| doc! {"_id": i, "owner": format!("{BOB}@%"), "title": "apple apple apple apple"},
        ),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let a2 = scores(aldb.clone()).await;
    ck.ok(
        "alice's textScore doesn't move when bob adds documents",
        a2 == a1,
        || format!("{a1:?} -> {a2:?}"),
    );

    // ---- Change-stream counters ----
    let status = |db: mongodb::Database| async move {
        let r = db.run_command(doc! {"serverStatus": 1}).await.unwrap();
        let c = r
            .get_document("metrics")
            .and_then(|m| m.get_document("changeStreams"))
            .and_then(|c| c.get_document("cursor"))
            .cloned()
            .unwrap_or_default();
        (
            c.get_i64("docsReturned").unwrap_or(-1),
            c.get_i64("docsExamined").unwrap_or(-1),
            c.get_i64("batchesReturned").unwrap_or(-1),
        )
    };
    let mut stream = aldb.collection::<Document>("mdocs").watch().await.unwrap();
    let before = status(adb.clone()).await;
    coll.insert_many([
        doc! {"_id": 3001, "owner": format!("{BOB}@%"), "title": "hidden"},
        doc! {"_id": 3, "owner": format!("{ALICE}@%"), "title": "visible"},
        doc! {"_id": 3002, "owner": format!("{BOB}@%"), "title": "hidden"},
    ])
    .await
    .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .ok()
        .flatten()
        .and_then(Result::ok);
    let got = ev
        .as_ref()
        .and_then(|e| e.document_key.as_ref())
        .and_then(|k| k.get("_id").and_then(Bson::as_i32));
    ck.ok("alice's stream: her insert", got == Some(3), || {
        format!("{ev:?}")
    });
    drop(stream);
    let after = status(adb.clone()).await;
    ck.ok(
        "serverStatus counts alice's one event, not bob's hidden ones",
        after.0 - before.0 == 1 && after.1 - before.1 == 1 && after.2 > before.2,
        || format!("{before:?} -> {after:?}"),
    );

    // ---- Cleanup ----
    drop(al);
    drop(carol);
    let _ = cmd(&mut ar, &["FT.DROPINDEX", "rlsp_idx", "DD"]).await;
    let left: BTreeSet<String> = cmd(&mut ar, &["KEYS", "rlsp:*"])
        .await
        .map(|v| all_strings(&v).into_iter().collect())
        .unwrap_or_default();
    for k in &left {
        let _ = cmd(&mut ar, &["DEL", k]).await;
    }
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsp_own ON redis.db0")
        .await;
    let _ = admin
        .query_drop("ALTER TABLE redis.db0 DISABLE ROW LEVEL SECURITY")
        .await;
    let _ = adb.drop().await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    for u in [ALICE, BOB, CAROL] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    println!(
        "rls_search_paths_live: {} checks, {} failures",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
