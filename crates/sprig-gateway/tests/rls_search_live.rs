//! Row-level security on full-text search, live: a user a policy binds
//! finds, counts, facets and scores only the rows it may see, through
//! SQL (`MATCH ... AGAINST`, `sprig_search()`), the REST Search API,
//! MongoDB (`$text`, `$search`, `$searchMeta`) and Redis (`FT.SEARCH`,
//! `FT.AGGREGATE`, `FT.TAGVALS`, `FT.INFO`).
//!
//! The administrator (`<tenant>.app`) makes users `rlss_alice` and
//! `rlss_bob`, and in each model rows owned by one or the other, with a
//! policy showing each user its own:
//!
//! ```text
//! SQL table   rlss_suite.notes   owner = SUBSTRING_INDEX(CURRENT_USER(), '@', 1)
//! collection  rlss_suite.mnotes  {owner: "$$USER"}
//! Redis       redis.db0          k LIKE CONCAT('rlss:', <user>, ':%')
//! ```
//!
//! Alice's rows say "apple"; bob's say "apple" (more often, so they rank
//! above hers) and "zebra", and carry tag `bobtag`. For alice every
//! interface must:
//!
//! - find no `zebra` row and none of bob's `apple` rows;
//! - count and facet her own rows only (`FT.SEARCH` totals, `FT.TAGVALS`,
//!   `FT.AGGREGATE` groups, `$searchMeta` counts and facets, REST facets),
//!   and show no document count in `FT.INFO`;
//! - fill a `LIMIT k` with her own rows when she has `k` matches;
//! - give the same scores before and after the administrator adds 30
//!   more of bob's rows full of "apple": scores computed over every row
//!   would move with rows she can't see (their term statistics).
//!
//! With `SPRIG_TEST_RLS_SETTING` (a script setting a dc-scope key:
//! `<script> search.max_window 5`, `-` to remove it), the window part
//! checks a search matching more rows than `search.max_window`: alice's
//! ranked results still hold only her rows, and her counts, facets and
//! tag values are refused (39827) without the number of matches.
//!
//! ```sh
//! export SPRIG_TEST_RLS_SQL=127.0.0.1:44306
//! export SPRIG_TEST_RLS_HTTP=127.0.0.1:44080
//! export SPRIG_TEST_RLS_MONGO=127.0.0.1:44737
//! export SPRIG_TEST_RLS_REDIS=127.0.0.1:44380
//! export SPRIG_TEST_RLS_TENANT=r13a
//! cargo test -p sprig-gateway --test rls_search_live -- --nocapture
//! ```
//!
//! The database, the Redis keys, the policies and the users are dropped
//! at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::similar_names,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use futures::TryStreamExt;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mongodb::bson::{Bson, Document, doc};
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};

const PW: &str = "Rlss-suite-pw-1";
const DB: &str = "rlss_suite";
const ALICE: &str = "rlss_alice";
const BOB: &str = "rlss_bob";

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

/// Every string anywhere in a reply (keys, field names, values).
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

/// A RESP2 `FT.SEARCH ... NOCONTENT [WITHSCORES]` reply: total, then keys
/// (with scores).
fn ft_hits(v: &redis::Value, scores: bool) -> (i64, Vec<(String, f64)>) {
    let redis::Value::Array(a) = v else {
        return (-1, Vec::new());
    };
    let total = match a.first() {
        Some(redis::Value::Int(n)) => *n,
        _ => -1,
    };
    let mut out = Vec::new();
    let mut i = 1;
    while i < a.len() {
        let key = s(&a[i]).unwrap_or_default();
        let score = if scores {
            i += 1;
            a.get(i)
                .and_then(s)
                .and_then(|x| x.parse().ok())
                .unwrap_or(f64::NAN)
        } else {
            0.0
        };
        out.push((key, score));
        i += 1;
    }
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

/// REST hits as (id, score).
fn rest_hits(v: &Value) -> Vec<(i64, f64)> {
    v["hits"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|h| {
                    let k: Value = serde_json::from_str(h["key"].as_str().unwrap_or("{}"))
                        .unwrap_or(Value::Null);
                    (
                        k["id"].as_i64().unwrap_or(-1),
                        h["score"].as_f64().unwrap_or(f64::NAN),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Alice's ids 1..=4; bob's 7001..=7004 (and 7101.. added later).
fn owner_of(id: i64) -> &'static str {
    if id < 1000 { ALICE } else { BOB }
}

fn title_of(id: i64) -> String {
    if id < 1000 {
        format!("apple note {id}")
    } else {
        format!("apple apple apple zebra note {id}")
    }
}

fn round(v: &[(i64, f64)]) -> Vec<(i64, i64)> {
    v.iter()
        .map(|(i, s)| (*i, (s * 1e5).round() as i64))
        .collect()
}

fn round_s(v: &[(String, f64)]) -> Vec<(String, i64)> {
    v.iter()
        .map(|(i, s)| (i.clone(), (s * 1e5).round() as i64))
        .collect()
}

/// Waits until the administrator's search of `apple` over every model
/// finds `n` rows, documents and hashes (the copies applied the writes).
async fn wait_copies(
    admin: &mut mysql_async::Conn,
    adb: &mongodb::Database,
    ar: &mut redis::aio::MultiplexedConnection,
    n: usize,
) {
    for _ in 0..120 {
        let a: Vec<i64> = admin
            .query("SELECT id FROM notes WHERE MATCH (title, body) AGAINST ('apple')")
            .await
            .unwrap_or_default();
        let m = adb
            .collection::<Document>("mnotes")
            .count_documents(doc! {"$text": {"$search": "apple"}})
            .await
            .unwrap_or(0);
        let r = cmd(
            ar,
            &[
                "FT.SEARCH",
                "rlss_idx",
                "apple",
                "NOCONTENT",
                "LIMIT",
                "0",
                "0",
            ],
        )
        .await
        .map(|v| ft_hits(&v, false).0)
        .unwrap_or(-1);
        if a.len() == n && m as usize == n && r == n as i64 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("the search copies never showed {n} rows");
}

/// Alice's scores on every interface, by row: SQL `MATCH` and
/// `sprig_search()`, REST, `$text`, `$search` and `FT.SEARCH`.
async fn alice_scores(
    ck: &mut Checks,
    al: &mut mysql_async::Conn,
    http: &str,
    amdb: &mongodb::Database,
    alr: &mut redis::aio::MultiplexedConnection,
    tag: &str,
) -> BTreeMap<&'static str, Vec<(i64, i64)>> {
    let mut out = BTreeMap::new();
    let r: Vec<(i64, f64)> = al
        .query(
            "SELECT id, MATCH (title, body) AGAINST ('apple') FROM notes \
             WHERE MATCH (title, body) AGAINST ('apple') ORDER BY id",
        )
        .await
        .unwrap();
    ck.ok(
        &format!("sql MATCH scores: alice's rows only ({tag})"),
        r.iter().all(|x| x.0 < 1000) && r.len() == 4,
        || format!("{r:?}"),
    );
    out.insert("sql match", round(&r));
    let r: Vec<(i64, f64)> = al
        .query(
            "SELECT n.id, s.score FROM sprig_search('notes_ix', 'apple', '{\"limit\": 10, \"consistency\": \"latest\"}') AS s \
             JOIN notes AS n ON n.id = s.id ORDER BY n.id",
        )
        .await
        .unwrap();
    ck.ok(
        &format!("sprig_search scores: alice's rows only ({tag})"),
        r.iter().all(|x| x.0 < 1000) && r.len() == 4,
        || format!("{r:?}"),
    );
    out.insert("sql sprig_search", round(&r));
    let (st, v) = post(
        http,
        ALICE,
        PW,
        &format!("/v1/databases/{DB}/search/notes_ix:query"),
        &json!({"query_string": "apple", "limit": 10, "consistency": "latest"}),
    )
    .await;
    let mut r = rest_hits(&v);
    r.sort_by_key(|x| x.0);
    ck.ok(
        &format!("REST scores: alice's rows only ({tag})"),
        st == 200 && r.iter().all(|x| x.0 < 1000) && r.len() == 4,
        || format!("{st} {v}"),
    );
    out.insert("rest", round(&r));
    let docs: Vec<Document> = amdb
        .collection::<Document>("mnotes")
        .find(doc! {"$text": {"$search": "apple"}})
        .projection(doc! {"score": {"$meta": "textScore"}})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut r: Vec<(i64, f64)> = docs
        .iter()
        .map(|d| {
            (
                d.get_i64("_id").unwrap_or(-1),
                d.get_f64("score").unwrap_or(f64::NAN),
            )
        })
        .collect();
    r.sort_by_key(|x| x.0);
    ck.ok(
        &format!("$text scores: alice's documents only ({tag})"),
        r.iter().all(|x| x.0 < 1000) && r.len() == 4,
        || format!("{r:?}"),
    );
    out.insert("mongo $text", round(&r));
    let docs: Vec<Document> = amdb
        .collection::<Document>("mnotes")
        .aggregate([
            doc! {"$search": {"index": "default", "consistency": "latest", "text": {"query": "apple", "path": "title"}}},
            doc! {"$project": {"_id": 1, "score": {"$meta": "searchScore"}}},
        ])
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut r: Vec<(i64, f64)> = docs
        .iter()
        .map(|d| {
            (
                d.get_i64("_id").unwrap_or(-1),
                d.get_f64("score").unwrap_or(f64::NAN),
            )
        })
        .collect();
    r.sort_by_key(|x| x.0);
    ck.ok(
        &format!("$search scores: alice's documents only ({tag})"),
        r.iter().all(|x| x.0 < 1000) && r.len() == 4,
        || format!("{r:?}"),
    );
    out.insert("mongo $search", round(&r));
    let v = cmd(
        alr,
        &[
            "FT.SEARCH",
            "rlss_idx",
            "apple",
            "NOCONTENT",
            "WITHSCORES",
            "LIMIT",
            "0",
            "100",
        ],
    )
    .await;
    let (total, mut hits) = v
        .as_ref()
        .map(|v| ft_hits(v, true))
        .unwrap_or((-1, Vec::new()));
    hits.sort_by(|a, b| a.0.cmp(&b.0));
    ck.ok(
        &format!("FT.SEARCH scores: alice's keys only, total 4 ({tag})"),
        total == 4 && hits.len() == 4 && hits.iter().all(|h| h.0.starts_with("rlss:rlss_alice:")),
        || format!("{v:?}"),
    );
    let r: Vec<(i64, i64)> = round_s(&hits)
        .into_iter()
        .enumerate()
        .map(|(i, (_, s))| (i as i64, s))
        .collect();
    out.insert("redis FT.SEARCH", r);
    out
}

/// Runs the deployment's setting script (`<script> <key> <json value>`,
/// `-` to remove).
fn set(script: &str, key: &str, value: &str) {
    let st = std::process::Command::new(script)
        .args([key, value])
        .status()
        .unwrap();
    assert!(st.success(), "{script} {key} {value}");
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => 0,
    }
}

async fn past_the_window(
    ck: &mut Checks,
    script: &str,
    al: &mut mysql_async::Conn,
    http: &str,
    amdb: &mongodb::Database,
    alr: &mut redis::aio::MultiplexedConnection,
    ar: &mut redis::aio::MultiplexedConnection,
) {
    set(script, "search.max_window", "5");
    // The setting reaches the gateway through its watch.
    let mut applied = false;
    for _ in 0..40 {
        let v = cmd(
            ar,
            &["FT.AGGREGATE", "rlss_idx", "apple", "LIMIT", "0", "1"],
        )
        .await;
        if v.is_err_and(|e| e.contains("39827") || e.contains("max_window")) {
            applied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    ck.ok(
        "window: search.max_window = 5 applied",
        applied,
        String::new,
    );
    let v = cmd(alr, &["FT.SEARCH", "rlss_idx", "apple", "NOCONTENT"]).await;
    ck.ok(
        "window: FT.SEARCH refused to alice, without a count",
        v.as_ref()
            .is_err_and(|e| e.contains("max_window") && !e.contains("38")),
        || format!("{v:?}"),
    );
    let v = cmd(alr, &["FT.TAGVALS", "rlss_idx", "tag"]).await;
    ck.ok(
        "window: FT.TAGVALS refused to alice",
        v.as_ref().is_err_and(|e| e.contains("max_window")),
        || format!("{v:?}"),
    );
    let r: Result<Vec<(i64, f64)>, _> = al
        .query(
            "SELECT id, score FROM sprig_search('notes_ix', 'apple', '{\"limit\": 20}') AS s ORDER BY id",
        )
        .await;
    ck.ok(
        "window: sprig_search answers alice's rows only",
        r.as_ref().is_ok_and(|v| v.iter().all(|x| x.0 < 1000)),
        || format!("{r:?}"),
    );
    let path = format!("/v1/databases/{DB}/search/notes_ix:query");
    let (st, v) = post(
        http,
        ALICE,
        PW,
        &path,
        &json!({"query_string": "apple", "limit": 3}),
    )
    .await;
    ck.ok(
        "window: REST hits are alice's only",
        st == 200 && rest_hits(&v).iter().all(|h| h.0 < 1000),
        || format!("{st} {v}"),
    );
    let (st, v) = post(
        http,
        ALICE,
        PW,
        &path,
        &json!({"query_string": "apple", "limit": 3,
                "facets": r#"{"tags": {"type": "string", "path": "tag"}}"#}),
    )
    .await;
    ck.ok(
        "window: REST facets refused to alice, without a count",
        st != 200 && v.to_string().contains("max_window") && !v.to_string().contains("38"),
        || format!("{st} {v}"),
    );
    let got: Result<Vec<Document>, _> = async {
        amdb.collection::<Document>("mnotes")
            .aggregate([
                doc! {"$search": {"index": "default", "text": {"query": "apple", "path": "title"}}},
            ])
            .await?
            .try_collect()
            .await
    }
    .await;
    ck.ok(
        "window: $search answers alice's documents only",
        got.as_ref()
            .is_ok_and(|v| v.iter().all(|d| d.get_i64("_id").is_ok_and(|i| i < 1000))),
        || format!("{got:?}"),
    );
    let got: Result<Vec<Document>, _> = async {
        amdb.collection::<Document>("mnotes")
            .aggregate([
                doc! {"$searchMeta": {"index": "default", "count": {"type": "total"},
                "text": {"query": "apple", "path": "title"}}},
            ])
            .await?
            .try_collect()
            .await
    }
    .await;
    ck.ok(
        "window: $searchMeta refused to alice",
        got.as_ref().is_err_and(|e| code_of(e) == 39827),
        || format!("{got:?}"),
    );
    set(script, "search.max_window", "-");
    for _ in 0..40 {
        if cmd(
            ar,
            &["FT.AGGREGATE", "rlss_idx", "apple", "LIMIT", "0", "1"],
        )
        .await
        .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_respects_row_policies() {
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
    let _ = cmd(&mut ar, &["FT.DROPINDEX", "rlss_idx", "DD"]).await;
    let _ = admin
        .query_drop("DROP POLICY IF EXISTS rlss_own ON redis.db0")
        .await;
    for u in [ALICE, BOB] {
        let _ = admin
            .query_drop(format!("DROP USER IF EXISTS '{u}'@'%'"))
            .await;
    }

    // The SQL table, its search index and policy.
    for st in [
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE notes (id BIGINT PRIMARY KEY, owner VARCHAR(64), title VARCHAR(200), \
         body VARCHAR(200), tag VARCHAR(20))"
            .to_owned(),
    ] {
        admin.query_drop(st).await.unwrap();
    }
    let ids: Vec<i64> = (1..=4).chain(7001..=7004).collect();
    let rows: Vec<String> = ids
        .iter()
        .map(|&i| {
            let tag = if i < 1000 { "alicetag" } else { "bobtag" };
            format!(
                "({i}, '{}', '{}', 'body {i}', '{tag}')",
                owner_of(i),
                title_of(i)
            )
        })
        .collect();
    admin
        .query_drop(format!("INSERT INTO notes VALUES {}", rows.join(", ")))
        .await
        .unwrap();
    admin
        .query_drop(
            "CREATE SEARCH INDEX notes_ix ON notes (title TEXT, body TEXT, \
             tag KEYWORD WITH (facet = true, filter = true))",
        )
        .await
        .unwrap();
    for st in [
        "CREATE POLICY rlss_own ON notes USING (owner = SUBSTRING_INDEX(CURRENT_USER(), '@', 1))",
        "ALTER TABLE notes ENABLE ROW LEVEL SECURITY",
    ] {
        admin.query_drop(st).await.unwrap();
    }

    // The collection, its text and search indexes and policy.
    let coll = adb.collection::<Document>("mnotes");
    let docs: Vec<Document> = ids
        .iter()
        .map(|&i| {
            let tag = if i < 1000 { "alicetag" } else { "bobtag" };
            doc! {"_id": i, "owner": format!("{}@%", owner_of(i)), "title": title_of(i), "tag": tag}
        })
        .collect();
    coll.insert_many(docs).await.unwrap();
    adb.run_command(doc! {"createIndexes": "mnotes", "indexes": [{"key": {"title": "text"}, "name": "title_text"}]})
        .await
        .unwrap();
    adb.run_command(
        doc! {"createSearchIndexes": "mnotes", "indexes": [{"name": "default", "definition": {
        "mappings": {"dynamic": false, "fields": {"title": {"type": "string"},
            "tag": [{"type": "token"}, {"type": "stringFacet"}]}}}}]},
    )
    .await
    .unwrap();
    adb.run_command(
        doc! {"sprigCreatePolicy": "mnotes", "name": "own", "using": {"owner": "$$USER"}},
    )
    .await
    .unwrap();
    adb.run_command(doc! {"sprigRowSecurity": "mnotes", "enable": true})
        .await
        .unwrap();

    // The Redis hashes, their index and the key policy.
    for &i in &ids {
        let tag = if i < 1000 { "alicetag" } else { "bobtag" };
        let key = format!("rlss:{}:{i}", owner_of(i));
        cmd(&mut ar, &["HSET", &key, "title", &title_of(i), "tag", tag])
            .await
            .unwrap();
    }
    cmd(
        &mut ar,
        &[
            "FT.CREATE",
            "rlss_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "rlss:",
            "SCHEMA",
            "title",
            "TEXT",
            "tag",
            "TAG",
        ],
    )
    .await
    .unwrap();
    for st in [
        "CREATE POLICY rlss_own ON redis.db0 USING (k LIKE CONCAT('rlss:', SUBSTRING_INDEX(CURRENT_USER(), '@', 1), ':%'))",
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
    wait_copies(&mut admin, &adb, &mut ar, 8).await;

    let mut al = sql(&sql_addr, ALICE, PW).await;
    al.query_drop(format!("USE {DB}")).await.unwrap();
    let am_alice = mongo(&mongo_addr, ALICE, PW).await;
    let amdb = am_alice.database(DB);
    let mut alr = redis(&redis_addr, ALICE, PW).await;

    // ---- SQL ----
    let r: Result<Vec<i64>, _> = al
        .query("SELECT id FROM notes WHERE MATCH (title, body) AGAINST ('zebra')")
        .await;
    ck.ok(
        "sql MATCH zebra: nothing",
        r.as_ref().is_ok_and(Vec::is_empty),
        || format!("{r:?}"),
    );
    let r: Result<Vec<i64>, _> = al
        .query("SELECT COUNT(*) FROM notes WHERE MATCH (title, body) AGAINST ('apple')")
        .await;
    ck.ok(
        "sql MATCH count: 4",
        r.as_ref().is_ok_and(|v| v == &[4]),
        || format!("{r:?}"),
    );
    // Bob's rows rank above hers; a limit of 3 still gives her 3 rows.
    let r: Result<Vec<i64>, _> = al
        .query(
            "SELECT id FROM sprig_search('notes_ix', 'apple', '{\"limit\": 3}') AS s ORDER BY id",
        )
        .await;
    ck.ok(
        "sprig_search limit 3: three of alice's rows",
        r.as_ref()
            .is_ok_and(|v| v.len() == 3 && v.iter().all(|i| *i < 1000)),
        || format!("{r:?}"),
    );
    let r: Result<Vec<i64>, _> = al
        .query("SELECT id FROM sprig_search('notes_ix', 'zebra') AS s")
        .await;
    ck.ok(
        "sprig_search zebra: nothing",
        r.as_ref().is_ok_and(Vec::is_empty),
        || format!("{r:?}"),
    );
    // A statement MariaDB refuses must not echo bob's keys.
    let e = al
        .query_drop("SELECT id FROM notes WHERE id = = 1 OR MATCH (title, body) AGAINST ('zebra')")
        .await
        .map_err(|e| e.to_string());
    ck.ok(
        "sql error text names none of bob's keys",
        e.as_ref()
            .is_err_and(|m| !m.contains("7001") && !m.contains("7002")),
        || format!("{e:?}"),
    );

    // ---- REST ----
    let path = format!("/v1/databases/{DB}/search/notes_ix:query");
    let (st, v) = post(
        &http,
        ALICE,
        PW,
        &path,
        &json!({"query_string": "zebra", "consistency": "latest"}),
    )
    .await;
    ck.ok(
        "REST zebra: nothing",
        st == 200 && rest_hits(&v).is_empty(),
        || format!("{st} {v}"),
    );
    let (st, v) = post(
        &http,
        ALICE,
        PW,
        &path,
        &json!({"query_string": "apple", "limit": 3, "consistency": "latest",
                "facets": r#"{"tags": {"type": "string", "path": "tag", "numBuckets": 5}}"#}),
    )
    .await;
    let hits = rest_hits(&v);
    ck.ok(
        "REST limit 3: three of alice's rows",
        st == 200 && hits.len() == 3 && hits.iter().all(|h| h.0 < 1000),
        || format!("{st} {v}"),
    );
    let facets = v["facets"].as_str().unwrap_or_default().to_owned();
    ck.ok(
        "REST facets: alicetag 4, no bobtag",
        facets.contains("alicetag") && !facets.contains("bobtag"),
        || facets.clone(),
    );

    // ---- MongoDB ----
    let mc = amdb.collection::<Document>("mnotes");
    let n = mc
        .count_documents(doc! {"$text": {"$search": "zebra"}})
        .await;
    ck.ok(
        "$text zebra: nothing",
        n.as_ref().is_ok_and(|n| *n == 0),
        || format!("{n:?}"),
    );
    let n = mc
        .count_documents(doc! {"$text": {"$search": "apple"}})
        .await;
    ck.ok("$text apple: 4", n.as_ref().is_ok_and(|n| *n == 4), || {
        format!("{n:?}")
    });
    let got: Result<Vec<Document>, _> = async {
        mc.aggregate([doc! {"$search": {"index": "default", "consistency": "latest", "text": {"query": "zebra", "path": "title"}}}])
            .await?
            .try_collect()
            .await
    }
    .await;
    ck.ok(
        "$search zebra: nothing",
        got.as_ref().is_ok_and(Vec::is_empty),
        || format!("{got:?}"),
    );
    let got: Result<Vec<Document>, _> = async {
        mc.aggregate([
            doc! {"$search": {"index": "default", "consistency": "latest", "text": {"query": "apple", "path": "title"}}},
            doc! {"$limit": 3},
        ])
        .await?
        .try_collect()
        .await
    }
    .await;
    ck.ok(
        "$search limit 3: three of alice's documents",
        got.as_ref().is_ok_and(|v| {
            v.len() == 3 && v.iter().all(|d| d.get_i64("_id").is_ok_and(|i| i < 1000))
        }),
        || format!("{got:?}"),
    );
    let got: Result<Vec<Document>, _> = async {
        mc.aggregate([doc! {"$searchMeta": {"index": "default", "consistency": "latest", "count": {"type": "total"},
            "facet": {"operator": {"text": {"query": "apple", "path": "title"}},
                      "facets": {"tags": {"type": "string", "path": "tag"}}}}}])
            .await?
            .try_collect()
            .await
    }
    .await;
    let meta = format!("{got:?}");
    ck.ok(
        "$searchMeta: total 4, alicetag only",
        got.as_ref().is_ok_and(|v| {
            v.first()
                .and_then(|d| d.get_document("count").ok())
                .and_then(|c| c.get("total"))
                .is_some_and(|t| matches!(t, Bson::Int64(4) | Bson::Int32(4)))
        }) && meta.contains("alicetag")
            && !meta.contains("bobtag"),
        || meta.clone(),
    );

    // ---- Redis ----
    let v = cmd(&mut alr, &["FT.SEARCH", "rlss_idx", "zebra", "NOCONTENT"]).await;
    ck.ok(
        "FT.SEARCH zebra: total 0, no key",
        v.as_ref()
            .is_ok_and(|v| ft_hits(v, false) == (0, Vec::new())),
        || format!("{v:?}"),
    );
    let v = cmd(
        &mut alr,
        &[
            "FT.SEARCH",
            "rlss_idx",
            "apple",
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
        ],
    )
    .await;
    ck.ok(
        "FT.SEARCH LIMIT 0 0: total 4",
        v.as_ref().is_ok_and(|v| ft_hits(v, false).0 == 4),
        || format!("{v:?}"),
    );
    let v = cmd(
        &mut alr,
        &[
            "FT.SEARCH",
            "rlss_idx",
            "apple",
            "NOCONTENT",
            "LIMIT",
            "0",
            "3",
        ],
    )
    .await;
    ck.ok(
        "FT.SEARCH LIMIT 0 3: three of alice's keys",
        v.as_ref().is_ok_and(|v| {
            let (_, h) = ft_hits(v, false);
            h.len() == 3 && h.iter().all(|x| x.0.starts_with("rlss:rlss_alice:"))
        }),
        || format!("{v:?}"),
    );
    let v = cmd(
        &mut alr,
        &[
            "FT.SEARCH",
            "rlss_idx",
            "*",
            "SORTBY",
            "title",
            "LIMIT",
            "0",
            "100",
        ],
    )
    .await;
    let mut all = Vec::new();
    if let Ok(v) = &v {
        strings(v, &mut all);
    }
    ck.ok(
        "FT.SEARCH SORTBY: total 4, nothing of bob's",
        v.as_ref().is_ok_and(|v| ft_hits(v, false).0 == 4)
            && !all
                .iter()
                .any(|x| x.contains("zebra") || x.contains("rlss_bob") || x == "bobtag"),
        || format!("{v:?}"),
    );
    let v = cmd(&mut alr, &["FT.TAGVALS", "rlss_idx", "tag"]).await;
    let mut tv = Vec::new();
    if let Ok(v) = &v {
        strings(v, &mut tv);
    }
    ck.ok("FT.TAGVALS: alicetag only", tv == ["alicetag"], || {
        format!("{v:?}")
    });
    let v = cmd(
        &mut alr,
        &[
            "FT.AGGREGATE",
            "rlss_idx",
            "*",
            "GROUPBY",
            "1",
            "@tag",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "n",
        ],
    )
    .await;
    let mut ag = Vec::new();
    if let Ok(v) = &v {
        strings(v, &mut ag);
    }
    ck.ok(
        "FT.AGGREGATE groups: alicetag 4 only",
        ag.contains(&"alicetag".to_owned())
            && !ag.contains(&"bobtag".to_owned())
            && ag.contains(&"4".to_owned()),
        || format!("{v:?}"),
    );
    let v = cmd(&mut alr, &["FT.INFO", "rlss_idx"]).await;
    let num_docs = match &v {
        Ok(redis::Value::Array(a)) => a
            .chunks(2)
            .find(|p| s(&p[0]).as_deref() == Some("num_docs"))
            .map(|p| p[1].clone()),
        Ok(redis::Value::Map(m)) => m
            .iter()
            .find(|(k, _)| s(k).as_deref() == Some("num_docs"))
            .map(|(_, x)| x.clone()),
        _ => None,
    };
    ck.ok(
        "FT.INFO: no document count",
        matches!(num_docs, Some(redis::Value::Nil)),
        || format!("{num_docs:?} in {v:?}"),
    );
    // The administrator, bound by nothing, sees all 8.
    let v = cmd(
        &mut ar,
        &[
            "FT.SEARCH",
            "rlss_idx",
            "apple",
            "NOCONTENT",
            "LIMIT",
            "0",
            "0",
        ],
    )
    .await;
    ck.ok(
        "FT.SEARCH as the administrator: total 8",
        v.as_ref().is_ok_and(|v| ft_hits(v, false).0 == 8),
        || format!("{v:?}"),
    );

    // ---- Scores don't move with rows alice can't see ----
    let before = alice_scores(&mut ck, &mut al, &http, &amdb, &mut alr, "before").await;
    let more: Vec<i64> = (7101..=7130).collect();
    let rows: Vec<String> = more
        .iter()
        .map(|&i| format!("({i}, '{BOB}', 'apple apple apple apple', 'apple {i}', 'bobtag')"))
        .collect();
    admin
        .query_drop(format!("INSERT INTO notes VALUES {}", rows.join(", ")))
        .await
        .unwrap();
    coll.insert_many(more.iter().map(|&i| {
        doc! {"_id": i, "owner": format!("{BOB}@%"), "title": "apple apple apple apple", "tag": "bobtag"}
    }))
    .await
    .unwrap();
    for &i in &more {
        let key = format!("rlss:{BOB}:{i}");
        cmd(
            &mut ar,
            &[
                "HSET",
                &key,
                "title",
                "apple apple apple apple",
                "tag",
                "bobtag",
            ],
        )
        .await
        .unwrap();
    }
    wait_copies(&mut admin, &adb, &mut ar, 8 + more.len()).await;
    let after = alice_scores(&mut ck, &mut al, &http, &amdb, &mut alr, "after").await;
    for (k, b) in &before {
        let a = after.get(k).cloned().unwrap_or_default();
        ck.ok(
            &format!("{k}: alice's scores unchanged by bob's new rows"),
            &a == b,
            || format!("before {b:?}, after {a:?}"),
        );
    }

    // ---- Past search.max_window ----
    // With a window of 5 and 38 matches, alice's ranked results use plain
    // scores and leave out bob's rows; counts and facets are refused.
    if let Ok(script) = std::env::var("SPRIG_TEST_RLS_SETTING") {
        past_the_window(&mut ck, &script, &mut al, &http, &amdb, &mut alr, &mut ar).await;
    } else {
        eprintln!("SPRIG_TEST_RLS_SETTING not set: the window checks skipped");
    }

    // ---- Cleanup ----
    drop(al);
    let _ = cmd(&mut ar, &["FT.DROPINDEX", "rlss_idx", "DD"]).await;
    let leftover: BTreeSet<String> = match cmd(&mut ar, &["KEYS", "rlss:*"]).await {
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
        .query_drop("DROP POLICY IF EXISTS rlss_own ON redis.db0")
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
        "rls_search_live: {} checks, {} failures",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
