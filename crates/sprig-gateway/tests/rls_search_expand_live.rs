//! Row-level security on prefix and fuzzy search terms, live: for a user
//! a row policy binds, `zorb*` and `kayak~1` expand to terms chosen among
//! the rows the user may see, through SQL (`MATCH ... AGAINST` in boolean
//! mode, `sprig_search()`), the REST Search API, MongoDB (`$search` with
//! `fuzzy`, `$searchMeta`) and Redis (`FT.SEARCH` with `zorb*` and
//! `%kayak%`). MongoDB's `explain` shows the scoring the user gets
//! (`matched`; SQL `EXPLAIN` over the protected table stays refused).
//!
//! A prefix or fuzzy term becomes its `search.max_expansions` (50) most
//! frequent terms. Alice's two rows hold `zorbint` and `kayak`; bob's
//! rows hold 60 other `zorb...` words and 60 other words one edit from
//! `kayak`, each three times, so over every row bob's words fill the 50
//! and alice's rows match nothing. For alice every interface must:
//!
//! - find her two rows, and none of bob's;
//! - count two (`FT.SEARCH` totals, `$searchMeta`);
//! - give the same rows and scores after the administrator adds 102 more
//!   of bob's rows: 17 new `zorb...` and `kayak`-like words, six times
//!   each (over every row they would now be the most frequent).
//!
//! The unbound administrator's search still takes the 50 most frequent
//! words of every row (a control: it misses alice's rows).
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:57306
//! export SPRIG_TEST_RLS_HTTP=127.0.0.1:57080
//! export SPRIG_TEST_RLS_MONGO=127.0.0.1:57737
//! export SPRIG_TEST_RLS_REDIS=127.0.0.1:57380
//! export SPRIG_TEST_RLS_TENANT=sf11t
//! cargo test -p sprig-gateway --test rls_search_expand_live -- --nocapture
//! ```
//!
//! The database, the Redis keys, the policy and the users are dropped at
//! the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::similar_names
)]

use std::collections::BTreeSet;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use futures::TryStreamExt;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};

const PW: &str = "Rlsx-suite-pw-1";
const DB: &str = "rlsx_suite";
const ALICE: &str = "rlsx_alice";
const BOB: &str = "rlsx_bob";
const IX: &str = "xnotes_ix";
const FT: &str = "rlsx_idx";

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

fn strings(v: &redis::Value, out: &mut Vec<String>) {
    match v {
        redis::Value::Array(a) | redis::Value::Set(a) => {
            for x in a {
                strings(x, out);
            }
        }
        other => out.extend(s(other)),
    }
}

/// A RESP2 `FT.SEARCH ... NOCONTENT WITHSCORES` reply: total, then
/// (key, score), keys sorted.
fn ft_hits(v: &redis::Value) -> (i64, Vec<(String, i64)>) {
    let redis::Value::Array(a) = v else {
        return (-1, Vec::new());
    };
    let total = match a.first() {
        Some(redis::Value::Int(n)) => *n,
        _ => -1,
    };
    let mut out = Vec::new();
    let mut i = 1;
    while i + 1 < a.len() {
        let key = s(&a[i]).unwrap_or_default();
        let score: f64 = s(&a[i + 1])
            .and_then(|x| x.parse().ok())
            .unwrap_or(f64::NAN);
        out.push((key, (score * 1e5).round() as i64));
        i += 2;
    }
    out.sort();
    (total, out)
}

async fn post(addr: &str, user: &str, pw: &str, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{pw}", tenant()))
    );
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", addr)
        .header("authorization", auth)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// REST hits as (id, rounded score), by id.
fn rest_hits(v: &Value) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = v["hits"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|h| {
                    let k: Value = serde_json::from_str(h["key"].as_str().unwrap_or("{}"))
                        .unwrap_or(Value::Null);
                    (
                        k["id"].as_i64().unwrap_or(-1),
                        (h["score"].as_f64().unwrap_or(f64::NAN) * 1e5).round() as i64,
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_unstable();
    out
}

/// Letters no stemmer or analyzer changes at the end of a word.
const LETTERS: &[u8] = b"bcdfghjmnpqrtvwxz";

/// The `n`th word other than alice's that `zorb*` matches (`round` 0 or
/// 1 gives disjoint sets).
fn zorb_word(round: usize, n: usize) -> String {
    let a = LETTERS[(n / LETTERS.len() + round * 8) % LETTERS.len()] as char;
    let b = LETTERS[n % LETTERS.len()] as char;
    format!("zorb{a}{b}")
}

/// The `n`th word one edit from `kayak` (`round` 0 or 1 gives disjoint
/// sets): one letter of positions 1-4 (round 0) or 5 (round 1) replaced.
fn kayak_word(round: usize, n: usize) -> String {
    let c = LETTERS[n % LETTERS.len()] as char;
    let pos = if round == 0 { n / LETTERS.len() } else { 4 };
    let mut w: Vec<char> = "kayak".chars().collect();
    w[pos] = c;
    w.into_iter().collect()
}

/// Bob's rows of one round: 60 words of each kind, each three times.
fn bob_rows(round: usize, first_id: i64) -> Vec<(i64, String)> {
    let words = if round == 0 { 60 } else { 17 };
    let copies = if round == 0 { 3 } else { 6 };
    let mut out = Vec::new();
    let mut id = first_id;
    for n in 0..words {
        for _ in 0..copies {
            out.push((
                id,
                format!("{} {} bob", zorb_word(round, n), kayak_word(round, n)),
            ));
            id += 1;
        }
    }
    out
}

fn alice_rows() -> Vec<(i64, String)> {
    vec![
        (1, "zorbint kayak first".to_owned()),
        (2, "zorbint kayak second".to_owned()),
    ]
}

/// Inserts rows owned by `owner` into the table, the collection and Redis.
async fn insert(
    admin: &mut mysql_async::Conn,
    adb: &mongodb::Database,
    ar: &mut redis::aio::MultiplexedConnection,
    owner: &str,
    rows: &[(i64, String)],
) {
    let values: Vec<String> = rows
        .iter()
        .map(|(id, t)| format!("({id}, '{owner}', '{t}')"))
        .collect();
    admin
        .query_drop(format!(
            "INSERT INTO {DB}.xnotes VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();
    let docs: Vec<Document> = rows
        .iter()
        .map(|(id, t)| doc! {"_id": *id, "owner": format!("{owner}@%"), "title": t})
        .collect();
    adb.collection::<Document>("xdocs")
        .insert_many(docs)
        .await
        .unwrap();
    for (id, t) in rows {
        cmd(ar, &["HSET", &format!("rlsx:{owner}:{id}"), "title", t])
            .await
            .unwrap();
    }
}

/// Waits until the administrator's search of `bob` finds `n` rows,
/// documents and hashes (the copies applied the writes).
async fn wait_copies(
    admin: &mut mysql_async::Conn,
    adb: &mongodb::Database,
    ar: &mut redis::aio::MultiplexedConnection,
    n: usize,
) {
    for _ in 0..240 {
        let a: Vec<i64> = admin
            .query(format!(
                "SELECT COUNT(*) FROM {DB}.xnotes WHERE MATCH (title) AGAINST ('bob')"
            ))
            .await
            .unwrap_or_default();
        let m: Vec<Document> = match adb
            .collection::<Document>("xdocs")
            .aggregate([
                doc! {"$searchMeta": {"index": "default", "consistency": "latest",
                "count": {"type": "total"}, "text": {"query": "bob", "path": "title"}}},
            ])
            .await
        {
            Ok(c) => c.try_collect().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let m = m
            .first()
            .and_then(|d| d.get_document("count").ok())
            .and_then(|c| c.get("total").cloned());
        let m = match m {
            Some(Bson::Int64(x)) => x,
            Some(Bson::Int32(x)) => i64::from(x),
            _ => -1,
        };
        let r = cmd(
            ar,
            &["FT.SEARCH", FT, "bob", "NOCONTENT", "LIMIT", "0", "0"],
        )
        .await
        .map(|v| match v {
            redis::Value::Array(a) => match a.first() {
                Some(redis::Value::Int(t)) => *t,
                _ => -1,
            },
            _ => -1,
        })
        .unwrap_or(-1);
        if a.first().copied() == Some(n as i64) && m == n as i64 && r == n as i64 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("the search copies never showed {n} of bob's rows");
}

/// What alice gets for `zorb*` and `kayak~1` on every interface: the rows
/// (with rounded scores where the interface gives them).
async fn alice_view(
    ck: &mut Checks,
    al: &mut mysql_async::Conn,
    http: &str,
    amdb: &mongodb::Database,
    alr: &mut redis::aio::MultiplexedConnection,
    tag: &str,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mine = |ids: &[i64]| ids == [1, 2];
    // SQL MATCH, boolean mode prefix.
    let r: Result<Vec<i64>, _> = al
        .query("SELECT id FROM xnotes WHERE MATCH (title) AGAINST ('zorb*' IN BOOLEAN MODE) ORDER BY id")
        .await;
    ck.ok(
        &format!("sql MATCH zorb*: alice's two rows ({tag})"),
        r.as_ref().is_ok_and(|v| mine(v)),
        || format!("{r:?}"),
    );
    out.push(("sql match".into(), format!("{r:?}")));
    for q in ["zorb*", "kayak~1"] {
        let r: Result<Vec<(i64, f64)>, _> = al
            .query(format!(
                "SELECT s.id, s.score FROM sprig_search('{IX}', '{q}', '{{\"limit\": 10, \"consistency\": \"latest\"}}') AS s ORDER BY s.id"
            ))
            .await;
        let ids: Vec<i64> = r
            .as_ref()
            .map(|v| v.iter().map(|x| x.0).collect())
            .unwrap_or_default();
        ck.ok(
            &format!("sprig_search {q}: alice's two rows ({tag})"),
            mine(&ids),
            || format!("{r:?}"),
        );
        let rounded: Vec<(i64, i64)> = r
            .unwrap_or_default()
            .iter()
            .map(|(i, s)| (*i, (s * 1e5).round() as i64))
            .collect();
        out.push((format!("sprig_search {q}"), format!("{rounded:?}")));
        let (st, v) = post(
            http,
            ALICE,
            PW,
            &format!("/v1/databases/{DB}/search/{IX}:query"),
            &json!({"query_string": q, "limit": 10, "consistency": "latest"}),
        )
        .await;
        let hits = rest_hits(&v);
        let ids: Vec<i64> = hits.iter().map(|x| x.0).collect();
        ck.ok(
            &format!("REST {q}: alice's two rows ({tag})"),
            st == 200 && mine(&ids),
            || format!("{st} {v}"),
        );
        out.push((format!("rest {q}"), format!("{hits:?}")));
    }
    // MongoDB `$search` with fuzzy, and its count.
    let fuzzy = doc! {"query": "kayak", "path": "title", "fuzzy": {"maxEdits": 1}};
    let docs: Vec<Document> = amdb
        .collection::<Document>("xdocs")
        .aggregate([
            doc! {"$search": {"index": "default", "consistency": "latest", "text": fuzzy.clone()}},
            doc! {"$project": {"_id": 1, "score": {"$meta": "searchScore"}}},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap_or_default();
    let mut hits: Vec<(i64, i64)> = docs
        .iter()
        .map(|d| {
            (
                d.get_i64("_id").unwrap_or(-1),
                (d.get_f64("score").unwrap_or(f64::NAN) * 1e5).round() as i64,
            )
        })
        .collect();
    hits.sort_unstable();
    let ids: Vec<i64> = hits.iter().map(|x| x.0).collect();
    ck.ok(
        &format!("$search fuzzy kayak: alice's two documents ({tag})"),
        mine(&ids),
        || format!("{hits:?}"),
    );
    out.push(("mongo $search".into(), format!("{hits:?}")));
    let meta: Vec<Document> = amdb
        .collection::<Document>("xdocs")
        .aggregate([
            doc! {"$searchMeta": {"index": "default", "consistency": "latest",
            "count": {"type": "total"}, "text": fuzzy}},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap_or_default();
    let total = meta
        .first()
        .and_then(|d| d.get_document("count").ok())
        .and_then(|c| c.get("total").cloned());
    ck.ok(
        &format!("$searchMeta fuzzy kayak: total 2 ({tag})"),
        matches!(total, Some(Bson::Int64(2) | Bson::Int32(2))),
        || format!("{meta:?}"),
    );
    // Redis prefix and fuzzy.
    for q in ["zorb*", "%kayak%"] {
        let v = cmd(alr, &["FT.SEARCH", FT, q, "NOCONTENT", "WITHSCORES"]).await;
        let (total, hits) = v.as_ref().map(ft_hits).unwrap_or((-1, Vec::new()));
        let keys: Vec<&str> = hits.iter().map(|x| x.0.as_str()).collect();
        ck.ok(
            &format!("FT.SEARCH {q}: alice's two keys, total 2 ({tag})"),
            total == 2 && keys == [format!("rlsx:{ALICE}:1"), format!("rlsx:{ALICE}:2")],
            || format!("{v:?}"),
        );
        out.push((format!("FT.SEARCH {q}"), format!("{total} {hits:?}")));
    }
    out
}

/// `scoring` of MongoDB's `explain` of a `$search` with fuzzy.
async fn mongo_scoring(db: &mongodb::Database) -> String {
    let r = db
        .run_command(doc! {
            "explain": {"aggregate": "xdocs", "pipeline": [
                {"$search": {"index": "default", "text": {"query": "kayak", "path": "title", "fuzzy": {"maxEdits": 1}}}}
            ], "cursor": {}},
            "verbosity": "queryPlanner",
        })
        .await;
    match r {
        Ok(d) => d
            .get_document("queryPlanner")
            .and_then(|p| p.get_document("winningPlan"))
            .and_then(|w| w.get_document("sprigSearch"))
            .and_then(|s| s.get_str("scoring"))
            .map_or_else(|_| format!("{d}"), str::to_owned),
        Err(e) => e.to_string(),
    }
}

/// The `Extra` of SQL `EXPLAIN` of a `MATCH` search.
async fn sql_extra(c: &mut mysql_async::Conn) -> String {
    let r: Result<Vec<mysql_async::Row>, _> = c
        .query(format!(
            "EXPLAIN SELECT id FROM {DB}.xnotes WHERE MATCH (title) AGAINST ('zorb*' IN BOOLEAN MODE)"
        ))
        .await;
    match r {
        Ok(rows) => rows
            .iter()
            .filter_map(|row| row.get_opt::<Option<String>, _>("Extra")?.ok().flatten())
            .find(|x| x.starts_with("SearchScan"))
            .unwrap_or_else(|| format!("{} rows, no SearchScan", rows.len())),
        Err(e) => e.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefix_and_fuzzy_terms_come_from_visible_rows() {
    let (Ok(sql_addr), Ok(http), Ok(mongo_addr), Ok(redis_addr)) = (
        std::env::var("SPRIG_TEST_RLS_SQL"),
        std::env::var("SPRIG_TEST_RLS_HTTP"),
        std::env::var("SPRIG_TEST_RLS_MONGO"),
        std::env::var("SPRIG_TEST_RLS_REDIS"),
    ) else {
        eprintln!("SPRIG_TEST_RLS_SQL, _HTTP, _MONGO or _REDIS not set; skipped");
        return;
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
    let _ = cmd(&mut ar, &["FT.DROPINDEX", FT, "DD"]).await;
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsx_own ON redis.db0")
        .await;
    for u in [ALICE, BOB] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }

    // The table, the collection and the Redis hashes, their search
    // indexes and policies showing each user its own rows.
    for st in [
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE xnotes (id BIGINT PRIMARY KEY, owner VARCHAR(64), title VARCHAR(200))"
            .to_owned(),
    ] {
        admin.query_drop(st).await.unwrap();
    }
    insert(&mut admin, &adb, &mut ar, ALICE, &alice_rows()).await;
    let bob = bob_rows(0, 1001);
    insert(&mut admin, &adb, &mut ar, BOB, &bob).await;
    admin
        .query_drop(format!("CREATE SEARCH INDEX {IX} ON xnotes (title TEXT)"))
        .await
        .unwrap();
    for st in [
        "CREATE POLICY rlsx_own ON xnotes USING (owner = SUBSTRING_INDEX(CURRENT_USER(), '@', 1))",
        "ALTER TABLE xnotes ENABLE ROW LEVEL SECURITY",
    ] {
        admin.query_drop(st).await.unwrap();
    }
    adb.run_command(
        doc! {"createSearchIndexes": "xdocs", "indexes": [{"name": "default", "definition": {
        "mappings": {"dynamic": false, "fields": {"title": {"type": "string"}}}}}]},
    )
    .await
    .unwrap();
    adb.run_command(
        doc! {"sprigCreatePolicy": "xdocs", "name": "own", "using": {"owner": "$$USER"}},
    )
    .await
    .unwrap();
    adb.run_command(doc! {"sprigRowSecurity": "xdocs", "enable": true})
        .await
        .unwrap();
    cmd(
        &mut ar,
        &[
            "FT.CREATE",
            FT,
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "rlsx:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
    )
    .await
    .unwrap();
    for st in [
        "CREATE POLICY rlsx_own ON redis.db0 USING (k LIKE CONCAT('rlsx:', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), ':%'))",
        "ALTER TABLE redis.db0 ENABLE ROW LEVEL SECURITY",
    ] {
        admin.query_drop(st).await.unwrap();
    }
    for u in [ALICE, BOB] {
        admin
            .query_drop(format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'"))
            .await
            .unwrap();
        for db in [DB, "redis"] {
            admin
                .query_drop(format!("GRANT SELECT ON {db}.* TO '{u}'@'%'"))
                .await
                .unwrap();
        }
    }
    wait_copies(&mut admin, &adb, &mut ar, bob.len()).await;

    // ---- The control: the administrator (no policy binds) ----
    // Bob's 60 words fill the 50 terms over every row; alice's are left out.
    let r: Vec<i64> = admin
        .query(format!(
            "SELECT s.id FROM sprig_search('{IX}', 'zorb*', '{{\"limit\": 500, \"consistency\": \"latest\"}}') AS s"
        ))
        .await
        .unwrap();
    ck.ok(
        "control: the administrator's zorb* takes bob's 50 most frequent words, not alice's",
        r.len() == 150 && !r.contains(&1) && !r.contains(&2),
        || format!("{} hits, alice's: {}", r.len(), r.contains(&1)),
    );
    let extra = sql_extra(&mut admin).await;
    ck.ok(
        "control: the administrator's EXPLAIN shows scoring global",
        extra.contains("scoring global"),
        || extra.clone(),
    );
    let sc = mongo_scoring(&adb).await;
    ck.ok(
        "control: the administrator's explain shows scoring global",
        sc == "global",
        || sc.clone(),
    );

    // ---- Alice ----
    let mut al = sql(&sql_addr, ALICE, PW).await;
    al.query_drop(format!("USE {DB}")).await.unwrap();
    let am_alice = mongo(&mongo_addr, ALICE, PW).await;
    let amdb = am_alice.database(DB);
    let mut alr = redis(&redis_addr, ALICE, PW).await;
    let before = alice_view(&mut ck, &mut al, &http, &amdb, &mut alr, "before").await;
    // SQL `EXPLAIN` over a protected table stays refused to a bound user.
    let extra = sql_extra(&mut al).await;
    ck.ok(
        "EXPLAIN stays refused to alice",
        extra.contains("39366"),
        || extra.clone(),
    );
    let sc = mongo_scoring(&amdb).await;
    ck.ok(
        "explain shows alice scoring matched",
        sc == "matched",
        || sc.clone(),
    );

    // More of bob's rows with new words of both kinds: alice's terms,
    // rows and scores stay the same.
    let more = bob_rows(1, 5001);
    insert(&mut admin, &adb, &mut ar, BOB, &more).await;
    wait_copies(&mut admin, &adb, &mut ar, bob.len() + more.len()).await;
    let after = alice_view(&mut ck, &mut al, &http, &amdb, &mut alr, "after").await;
    for ((k, b), (_, a)) in before.iter().zip(&after) {
        ck.ok(
            &format!("{k}: alice's rows and scores unchanged by bob's new rows"),
            a == b,
            || format!("before {b}, after {a}"),
        );
    }

    // ---- Cleanup ----
    drop(al);
    let _ = cmd(&mut ar, &["FT.DROPINDEX", FT, "DD"]).await;
    let leftover: BTreeSet<String> = match cmd(&mut ar, &["KEYS", "rlsx:*"]).await {
        Ok(v) => {
            let mut k = Vec::new();
            strings(&v, &mut k);
            k.into_iter().collect()
        }
        Err(_) => BTreeSet::new(),
    };
    for k in &leftover {
        let _ = cmd(&mut ar, &["DEL", k]).await;
    }
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlsx_own ON redis.db0")
        .await;
    let _ = admin
        .query_drop("ALTER TABLE redis.db0 DISABLE ROW LEVEL SECURITY")
        .await;
    let _ = admin
        .query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
        .await;
    for u in [ALICE, BOB] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }
    println!(
        "rls_search_expand_live: {} checks, {} failures",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
