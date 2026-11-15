//! Row-level security on inherited rows:
//! a tenant's policies filter every row it reads of an inheritable table,
//! its own and those it inherits from parent spaces and parent tenants,
//! on every read path, evaluated on the merged records.
//!
//! Tenants `a > b > c`, and `b`'s space `b1`; table `docs` (a record
//! hierarchy) made in `a` and shared `a > b > (b1, c)`, read through or
//! materialized (`SPRIG_TEST_RLSI_MODE`). Every tenant has its own
//! policies on its `docs`, permissive and restrictive, and users `alice`,
//! `bob` and `eve` (no permissive policy names `eve`); `app` manages the
//! table and is bound by nothing. Overrides at `b`, `b1` and `c` change
//! what the policies decide: a record made invisible, a record given to
//! another owner, a hidden record, a field changed in a space.
//!
//! [`PATHS`] is the regression table: each read path is run for every
//! user at every level, and **no path may return a record the level's
//! policies hide**. Most must also return exactly what they show. A new
//! read path belongs in the table.
//!
//! ```sh
//! export SPRIG_TEST_RLSI_MYSQL=127.0.0.1:38306
//! export SPRIG_TEST_RLSI_MARIADB=127.0.0.1:38307
//! export SPRIG_TEST_RLSI_HTTP=127.0.0.1:38080
//! export SPRIG_TEST_RLSI_TENANTS=rlsia,rlsib,rlsic
//! export SPRIG_TEST_RLSI_MODE=read_through   # or materialized
//! cargo test -p sprig-gateway --test rls_inherit_live -- --nocapture --test-threads 1
//! ```
//!
//! Optional: `SPRIG_TEST_RLSI_WORKER_PID` (materialized mode: the worker
//! is stopped for a few seconds, so a copy lags; RL69), and
//! `SPRIG_TEST_RLSI_RESTART`, a command taking `kill9`, `term` or `all`
//! that restarts the gateway (`kill -9`, a clean stop) or every process,
//! for [`share_views_survive_restarts`] (RL68), with
//! `SPRIG_TEST_RLSI_RAW` (`host:port` of the tenants' MariaDB, user
//! `root`, password `SPRIG_TEST_RLSI_RAW_PW`) and
//! `SPRIG_TEST_RLSI_TENANT_IDS` (`a,b,c`) to check the views' definers and
//! repair one an older build moved. The restart test stops the gateway:
//! run the tests one at a time (`--test-threads 1`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "sprig-dev-app";
const USERS: [&str; 3] = ["alice", "bob", "eve"];
/// `a`'s root, `b`'s root, `b`'s space `b1`, `c`'s root.
const LEVELS: usize = 4;
const TENANT_OF: [usize; LEVELS] = [0, 1, 1, 2];
const LEVEL_NAME: [&str; LEVELS] = ["a", "b", "b1", "c"];

struct Env {
    mysql: String,
    mariadb: String,
    http: String,
    t: Vec<String>,
    materialized: bool,
}

fn env() -> Option<Env> {
    let (Ok(mysql), Ok(mariadb), Ok(http), Ok(t)) = (
        std::env::var("SPRIG_TEST_RLSI_MYSQL"),
        std::env::var("SPRIG_TEST_RLSI_MARIADB"),
        std::env::var("SPRIG_TEST_RLSI_HTTP"),
        std::env::var("SPRIG_TEST_RLSI_TENANTS"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RLSI_MYSQL, SPRIG_TEST_RLSI_MARIADB, SPRIG_TEST_RLSI_HTTP \
             and SPRIG_TEST_RLSI_TENANTS"
        );
        return None;
    };
    let t: Vec<String> = t.split(',').map(|x| x.trim().to_owned()).collect();
    assert_eq!(t.len(), 3, "three tenants, a > b > c");
    Some(Env {
        mysql,
        mariadb,
        http,
        t,
        materialized: std::env::var("SPRIG_TEST_RLSI_MODE").ok().as_deref() == Some("materialized"),
    })
}

async fn conn(addr: &str, tenant: &str, user: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.{user}")))
        .pass(Some(PW))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts)
        .await
        .unwrap_or_else(|e| panic!("login {tenant}.{user} at {addr}: {e}"))
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

async fn try_exec(c: &mut mysql_async::Conn, s: &str) -> Result<(), String> {
    c.query_drop(s).await.map_err(|e| e.to_string())
}

async fn ids(c: &mut mysql_async::Conn, s: &str) -> Result<BTreeSet<i64>, String> {
    let rows: Vec<mysql_async::Row> = c.query(s).await.map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get_opt::<i64, _>(0).and_then(Result::ok))
        .collect())
}

/// One record of the model.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rec {
    owner: String,
    visible: i64,
    tier: i64,
    parent: Option<i64>,
}

type View = BTreeMap<i64, Rec>;

/// The merged views the writes below make, per level.
fn model() -> [View; LEVELS] {
    let owners = ["alice", "bob", "carol"];
    let mut a = View::new();
    for id in 1..=12i64 {
        a.insert(
            id,
            Rec {
                owner: owners[usize::try_from((id - 1) % 3).unwrap()].to_owned(),
                visible: i64::from(!matches!(id, 5 | 6)),
                tier: id % 4,
                parent: (id != 1).then_some(1),
            },
        );
    }
    let mut b = a.clone();
    b.get_mut(&1).unwrap().visible = 0; // alice's record made invisible
    b.get_mut(&4).unwrap().owner = "bob".into(); // alice's record given to bob
    b.remove(&7); // a hidden record
    b.insert(1_000_001, rec("alice", 1, 1));
    let mut b1 = b.clone();
    b1.get_mut(&10).unwrap().tier = 0; // alice's record, a field changed
    b1.insert(1_500_001, rec("alice", 1, 2));
    let mut c = b.clone();
    c.get_mut(&2).unwrap().owner = "alice".into(); // bob's record given to alice
    c.insert(2_000_001, rec("bob", 1, 1));
    [a, b, b1, c]
}

fn rec(owner: &str, visible: i64, tier: i64) -> Rec {
    Rec {
        owner: owner.into(),
        visible,
        tier,
        parent: None,
    }
}

/// The writes of [`model`], per level (`app`, bound by nothing).
fn writes(level: usize) -> &'static [&'static str] {
    match level {
        1 => &[
            "UPDATE docs SET visible = 0 WHERE id = 1",
            "UPDATE docs SET owner = 'bob' WHERE id = 4",
            "DELETE FROM docs WHERE id = 7",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (1000001, NULL, \
             'alice', 1, 1)",
        ],
        2 => &[
            "UPDATE docs SET tier = 0 WHERE id = 10",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (1500001, NULL, \
             'alice', 1, 2)",
        ],
        3 => &[
            "UPDATE docs SET owner = 'alice' WHERE id = 2",
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (2000001, NULL, 'bob', \
             1, 1)",
        ],
        _ => &[],
    }
}

/// The policies of tenant `t` (0, 1, 2) on its `docs`.
fn policies(t: usize) -> Vec<&'static str> {
    let mut v = vec![
        "CREATE POLICY p_alice ON docs FOR SELECT, UPDATE, DELETE TO alice USING (owner = 'alice')",
        // No INSERT policy: an UPDATE or DELETE of an inherited record
        // writes an override or a hide marker, which MariaDB's triggers
        // check with UPDATE's and DELETE's policies (RL65).
        "CREATE POLICY p_bob ON docs FOR SELECT, UPDATE, DELETE TO bob USING (owner = 'bob')",
    ];
    match t {
        0 => v.push(
            "CREATE POLICY p_tier ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (tier <= 2)",
        ),
        1 => v.push(
            "CREATE POLICY p_vis ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (visible = 1)",
        ),
        _ => {
            v.push("CREATE POLICY p_t3 ON docs FOR SELECT TO alice USING (tier = 3)");
            v.push("CREATE POLICY p_nz ON docs AS RESTRICTIVE FOR ALL TO PUBLIC USING (tier <> 0)");
        }
    }
    v
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmd {
    Select,
    Update,
    Delete,
}

/// Whether tenant `t`'s policies show `r` to `user` for `cmd` (the
/// write's policies with `SELECT`'s, as the gateway applies them).
fn shows(t: usize, user: &str, r: &Rec, cmd: Cmd) -> bool {
    if user == "app" {
        return true;
    }
    let own = (user == "alice" && r.owner == "alice") || (user == "bob" && r.owner == "bob");
    let restrictive = match t {
        0 => r.tier <= 2,
        1 => r.visible == 1,
        _ => r.tier != 0,
    };
    let select = (own || (t == 2 && user == "alice" && r.tier == 3)) && restrictive;
    match cmd {
        Cmd::Select => select,
        Cmd::Update | Cmd::Delete => select && own && restrictive,
    }
}

fn expected(view: &View, t: usize, user: &str, cmd: Cmd) -> BTreeSet<i64> {
    view.iter()
        .filter(|(_, r)| shows(t, user, r, cmd))
        .map(|(i, _)| *i)
        .collect()
}

/// What a read path answered.
#[derive(Debug)]
enum Got {
    Ids(BTreeSet<i64>),
    Count(u64),
    /// Refused (allowed only for [`Must::SubsetOrRefused`] paths).
    Refused(String),
}

/// What a read path must answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Must {
    /// Exactly the records the policies show (`SELECT`'s).
    Exact,
    /// The number of records the policies show for the command.
    Count(Cmd),
    /// Records the policies show, never another (the hierarchy functions,
    /// whose answer depends on the hierarchy too); an error is allowed.
    SubsetOrRefused,
}

/// The read paths (the regression table).
#[derive(Debug, Clone, Copy)]
enum Path {
    SelectMysql,
    SelectMariadb,
    PointReads,
    InList,
    SelfJoin,
    Derived,
    InSubquery,
    Union,
    TopK,
    Origin,
    Http,
    CountStar,
    UpdateMatched,
    DeleteAffected,
    Subtree,
    Prepared,
    Graph,
    Search,
}

const PATHS: &[(Path, Must)] = &[
    (Path::SelectMysql, Must::Exact),
    (Path::SelectMariadb, Must::Exact),
    (Path::PointReads, Must::Exact),
    (Path::InList, Must::Exact),
    (Path::SelfJoin, Must::Exact),
    (Path::Derived, Must::Exact),
    (Path::InSubquery, Must::Exact),
    (Path::Union, Must::Exact),
    (Path::TopK, Must::Exact),
    (Path::Origin, Must::Exact),
    (Path::Http, Must::Exact),
    (Path::CountStar, Must::Count(Cmd::Select)),
    (Path::UpdateMatched, Must::Count(Cmd::Update)),
    (Path::DeleteAffected, Must::Count(Cmd::Delete)),
    (Path::Subtree, Must::SubsetOrRefused),
    (Path::Prepared, Must::SubsetOrRefused),
    (Path::Graph, Must::SubsetOrRefused),
    (Path::Search, Must::SubsetOrRefused),
];

struct Fixture<'e> {
    e: &'e Env,
    db: String,
    b1: String,
}

impl Fixture<'_> {
    async fn session(&self, addr: &str, level: usize, user: &str) -> mysql_async::Conn {
        let mut c = conn(addr, &self.e.t[TENANT_OF[level]], user).await;
        must(&mut c, &format!("USE `{}`", self.db)).await;
        if level == 2 {
            must(&mut c, &format!("USE SPACE `{}`", self.b1)).await;
        }
        c
    }

    /// One path for `user` at `level`; `all`: every id the model knows.
    async fn run(&self, p: Path, level: usize, user: &str, all: &BTreeSet<i64>) -> Got {
        let answer = |r: Result<BTreeSet<i64>, String>| match r {
            Ok(s) => Got::Ids(s),
            Err(e) => Got::Refused(e),
        };
        let mysql = &self.e.mysql;
        match p {
            Path::SelectMysql => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id FROM docs").await)
            }
            Path::SelectMariadb => {
                let mut c = self.session(&self.e.mariadb, level, user).await;
                answer(ids(&mut c, "SELECT * FROM docs").await)
            }
            Path::PointReads => {
                let mut c = self.session(mysql, level, user).await;
                let mut out = BTreeSet::new();
                for i in all {
                    match ids(&mut c, &format!("SELECT id FROM docs WHERE id = {i}")).await {
                        Ok(s) => out.extend(s),
                        Err(e) => return Got::Refused(e),
                    }
                }
                Got::Ids(out)
            }
            Path::InList => {
                let mut c = self.session(mysql, level, user).await;
                let list: Vec<String> = all.iter().map(ToString::to_string).collect();
                answer(
                    ids(
                        &mut c,
                        &format!("SELECT id FROM docs WHERE id IN ({})", list.join(", ")),
                    )
                    .await,
                )
            }
            Path::SelfJoin => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT d.id FROM docs d LEFT JOIN docs e ON e.id = d.parent_id",
                    )
                    .await,
                )
            }
            Path::Derived => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT x.id FROM (SELECT * FROM docs) AS x").await)
            }
            Path::InSubquery => {
                let mut c = self.session(&self.e.mariadb, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE id IN (SELECT id FROM docs WHERE owner IS NOT \
                         NULL)",
                    )
                    .await,
                )
            }
            Path::Union => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE tier < 2 UNION SELECT id FROM docs WHERE tier \
                         >= 2",
                    )
                    .await,
                )
            }
            // The shape of a vector search: ORDER BY a distance, LIMIT k.
            Path::TopK => {
                let mut c = self.session(&self.e.mariadb, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs ORDER BY ABS(tier - 1), id LIMIT 1000",
                    )
                    .await,
                )
            }
            Path::Origin => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id, SPRIG_ORIGIN() FROM docs").await)
            }
            Path::Http => match self.http_ids(level, user).await {
                Ok(s) => Got::Ids(s),
                Err(e) => Got::Refused(e),
            },
            Path::CountStar => {
                let mut c = self.session(mysql, level, user).await;
                match c.query_first::<u64, _>("SELECT COUNT(*) FROM docs").await {
                    Ok(n) => Got::Count(n.unwrap_or(0)),
                    Err(e) => Got::Refused(e.to_string()),
                }
            }
            Path::UpdateMatched | Path::DeleteAffected => {
                let mut c = self.session(mysql, level, user).await;
                must(&mut c, "BEGIN").await;
                let sql = if matches!(p, Path::UpdateMatched) {
                    "UPDATE docs SET body = 'rlsi'"
                } else {
                    // The root has children: a record hierarchy refuses its
                    // delete.
                    "DELETE FROM docs WHERE id <> 1"
                };
                let r = c.query_drop(sql).await;
                let got = match r {
                    Ok(()) => {
                        let info = c.info().into_owned();
                        let matched = info
                            .split("Rows matched: ")
                            .nth(1)
                            .and_then(|x| x.split_whitespace().next())
                            .and_then(|x| x.parse().ok());
                        Got::Count(matched.unwrap_or(c.affected_rows()))
                    }
                    Err(e) => Got::Refused(e.to_string()),
                };
                let _ = c.query_drop("ROLLBACK").await;
                got
            }
            Path::Subtree => {
                let mut c = self.session(mysql, level, user).await;
                answer(ids(&mut c, "SELECT id FROM docs WHERE SPRIG_SUBTREE(docs, 1)").await)
            }
            Path::Prepared => {
                let mut c = self.session(mysql, level, user).await;
                match c
                    .exec::<i64, _, _>("SELECT id FROM docs WHERE id > ?", (0,))
                    .await
                {
                    Ok(v) => Got::Ids(v.into_iter().collect()),
                    Err(e) => Got::Refused(e.to_string()),
                }
            }
            Path::Graph => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM GRAPH_TABLE (rlsi_g MATCH (d:Doc) COLUMNS (d.id AS id)) \
                         AS g",
                    )
                    .await,
                )
            }
            Path::Search => {
                let mut c = self.session(mysql, level, user).await;
                answer(
                    ids(
                        &mut c,
                        "SELECT id FROM docs WHERE MATCH (body) AGAINST ('searchable')",
                    )
                    .await,
                )
            }
        }
    }

    async fn http_ids(&self, level: usize, user: &str) -> Result<BTreeSet<i64>, String> {
        let tcp = tokio::net::TcpStream::connect(&self.e.http)
            .await
            .map_err(|e| e.to_string())?;
        let auth = base64::engine::general_purpose::STANDARD
            .encode(format!("{}.{user}:{PW}", self.e.t[TENANT_OF[level]]));
        let body = serde_json::json!({ "sql": "SELECT id FROM docs" }).to_string();
        let mut req = hyper::Request::builder()
            .method("POST")
            .uri(format!("/v1/databases/{}/sql:execute", self.db))
            .header("host", &self.e.http)
            .header("authorization", format!("Basic {auth}"))
            .header("content-type", "application/json");
        if level == 2 {
            req = req.header("sprig-space", &self.b1);
        }
        let req = req.body(Full::new(Bytes::from(body))).unwrap();
        let (mut tx, c) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(c);
        let resp = tx.send_request(req).await.map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let b = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| e.to_string())?
            .to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&b).map_err(|e| e.to_string())?;
        if status >= 400 {
            return Err(v.to_string());
        }
        let r = sprig_api::results::execute_from_json(&v).map_err(|e| e.to_string())?;
        let mut out = BTreeSet::new();
        for res in &r.results {
            for row in &res.rows {
                if let Some(cell) = row.values.first()
                    && let Some(b) = sprig_api::results::cell_bytes(cell, false)
                    && let Ok(i) = String::from_utf8_lossy(&b).parse::<i64>()
                {
                    out.insert(i);
                }
            }
        }
        Ok(out)
    }
}

/// An NDJSON change stream of `docs`.
async fn open_stream(e: &Env, tenant: &str, user: &str, db: &str) -> tokio::net::TcpStream {
    open_stream_on(e, tenant, user, db, "docs").await
}

/// An NDJSON change stream of `objects`.
async fn open_stream_on(
    e: &Env,
    tenant: &str,
    user: &str,
    db: &str,
    objects: &str,
) -> tokio::net::TcpStream {
    let mut s = tokio::net::TcpStream::connect(&e.http).await.unwrap();
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.{user}:{PW}"));
    let req = format!(
        "GET /v1/databases/{db}/changes?objects={objects} HTTP/1.1\r\nhost: {}\r\n\
         authorization: Basic {auth}\r\n\r\n",
        e.http
    );
    s.write_all(req.as_bytes()).await.unwrap();
    s
}

/// The keys (`id`) of the events a stream got, heartbeats left out.
async fn stream_keys(s: &mut tokio::net::TcpStream, secs: u64) -> Vec<(String, i64)> {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut buf = Vec::new();
    let mut chunk = vec![0u8; 65536];
    loop {
        match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .filter(|v| v.is_object() && v["op"] != "heartbeat")
        .filter_map(|v| {
            let key: serde_json::Value =
                serde_json::from_str(v["key"].as_str().unwrap_or("null")).ok()?;
            Some((v["op"].as_str()?.to_owned(), key["id"].as_i64()?))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_read_path_returns_an_inherited_row_a_policy_hides() {
    let Some(e) = env() else {
        return;
    };
    let pid = std::process::id() % 10_000;
    let tag = if e.materialized { "m" } else { "r" };
    let db = format!("rlsi_{tag}{pid}");
    let b1 = format!("rlsi_{tag}{pid}_b1");
    let f = Fixture {
        e: &e,
        db: db.clone(),
        b1: b1.clone(),
    };
    let mut app: Vec<mysql_async::Conn> = Vec::new();
    for t in &e.t {
        let mut c = conn(&e.mysql, t, "app").await;
        must(&mut c, &format!("USE SPACE `{t}`")).await;
        app.push(c);
    }
    // Setup: the table in a, shared a > b > (b1, c).
    let _ = try_exec(&mut app[1], &format!("DROP SPACE IF EXISTS `{b1}` CASCADE")).await;
    must(&mut app[1], &format!("CREATE SPACE `{b1}`")).await;
    must(&mut app[0], &format!("CREATE DATABASE `{db}`")).await;
    must(&mut app[0], &format!("USE `{db}`")).await;
    must(
        &mut app[0],
        "CREATE TABLE docs (id INT PRIMARY KEY, parent_id INT NULL, owner VARCHAR(20) NOT NULL, \
         visible INT NOT NULL DEFAULT 1, tier INT NOT NULL DEFAULT 0, body VARCHAR(40) NOT NULL \
         DEFAULT 'searchable')",
    )
    .await;
    must(&mut app[0], "ALTER TABLE docs ADD HIERARCHY (parent_id)").await;
    must(&mut app[0], "ALTER TABLE docs SPRIG INHERITABLE KEY (id)").await;
    let base = &model()[0];
    for (id, r) in base {
        must(
            &mut app[0],
            &format!(
                "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES ({id}, {}, '{}', \
                 {}, {})",
                r.parent.map_or("NULL".to_owned(), |p| p.to_string()),
                r.owner,
                r.visible,
                r.tier
            ),
        )
        .await;
    }
    if e.materialized {
        must(&mut app[0], "SPRIG SET inherit.merge = 'materialized'").await;
    }
    must(
        &mut app[0],
        &format!(
            "CREATE SHARE rlsi_{tag}{pid}_ab ON docs TO SPACE `{}` WITH RESHARE",
            e.t[1]
        ),
    )
    .await;
    if e.materialized {
        must(&mut app[0], "SPRIG RESET inherit.merge").await;
    }
    must(&mut app[1], &format!("USE `{db}`")).await;
    must(
        &mut app[1],
        &format!("CREATE SHARE rlsi_{tag}{pid}_bb ON docs TO SPACE `{b1}`"),
    )
    .await;
    if e.materialized {
        must(&mut app[1], "SPRIG SET inherit.merge = 'materialized'").await;
    }
    must(
        &mut app[1],
        &format!(
            "CREATE SHARE rlsi_{tag}{pid}_bc ON docs TO SPACE `{}`",
            e.t[2]
        ),
    )
    .await;
    if e.materialized {
        must(&mut app[1], "SPRIG RESET inherit.merge").await;
    }
    must(&mut app[2], &format!("USE `{db}`")).await;
    // Users and policies in every tenant.
    for (t, c) in app.iter_mut().enumerate() {
        for u in USERS {
            let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
            must(c, &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{PW}'")).await;
            must(
                c,
                &format!("GRANT SELECT, INSERT, UPDATE, DELETE, WATCH ON `{db}`.* TO '{u}'@'%'"),
            )
            .await;
        }
        must(c, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
        for p in policies(t) {
            must(c, p).await;
        }
    }
    // The overrides, hides and own records below a.
    for level in 1..LEVELS {
        let mut c = f.session(&e.mysql, level, "app").await;
        for w in writes(level) {
            must(&mut c, w).await;
        }
    }
    // A mapped graph and a full-text index, where they can be made (their
    // refusal is a pass: the path then has no rows to leak).
    for (t, c) in app.iter_mut().enumerate() {
        let g = try_exec(
            c,
            "CREATE PROPERTY GRAPH rlsi_g VERTEX TABLES (docs KEY (id) LABEL Doc PROPERTIES (id, \
             owner))",
        )
        .await;
        eprintln!("graph in tenant {t}: {g:?}");
    }
    let ft = try_exec(&mut app[0], "CREATE FULLTEXT INDEX rlsi_ft ON docs (body)").await;
    eprintln!("full-text index in a: {ft:?}");

    // The oracle: app, bound by nothing, sees the model at every level
    // (materialized copies converge first).
    let views = model();
    for level in 0..LEVELS {
        let want: BTreeSet<i64> = views[level].keys().copied().collect();
        let mut got = Err(String::new());
        for _ in 0..60 {
            let mut c = f.session(&e.mysql, level, "app").await;
            got = ids(&mut c, "SELECT id FROM docs").await;
            if got.as_ref() == Ok(&want) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(got, Ok(want), "app's view at {}", LEVEL_NAME[level]);
    }
    let all: BTreeSet<i64> = views
        .iter()
        .flat_map(|v| v.keys().copied())
        .chain([7])
        .collect();

    // The matrix: every path, user and level.
    let mut leaks: Vec<String> = Vec::new();
    let mut wrong: Vec<String> = Vec::new();
    let mut checks = 0u32;
    let mut refused: BTreeMap<String, u32> = BTreeMap::new();
    for level in 0..LEVELS {
        let t = TENANT_OF[level];
        let view = &views[level];
        for user in USERS.iter().copied().chain(["app"]) {
            for (p, must) in PATHS {
                // The REST API doesn't take a space (chapter 42's
                // `Sprig-Space` isn't built): it reads the home space's
                // view, `b`'s root at `b1`.
                let view = if matches!(p, Path::Http) && level == 2 {
                    &views[1]
                } else {
                    view
                };
                let shown = expected(view, t, user, Cmd::Select);
                checks += 1;
                let got = f.run(*p, level, user, &all).await;
                let at = format!("{p:?} {user}@{}", LEVEL_NAME[level]);
                match (&got, must) {
                    (Got::Ids(s), _) => {
                        let hidden: Vec<i64> = s.difference(&shown).copied().collect();
                        if !hidden.is_empty() {
                            leaks.push(format!("{at}: returned hidden {hidden:?}"));
                        }
                        if *must == Must::Exact && *s != shown {
                            wrong.push(format!("{at}: {s:?}, want {shown:?}"));
                        }
                    }
                    (Got::Count(n), Must::Count(cmd)) => {
                        let mut want = expected(view, t, user, *cmd);
                        if *cmd == Cmd::Delete {
                            want.remove(&1);
                        }
                        let w = want.len() as u64;
                        if *n > w {
                            leaks.push(format!("{at}: counted {n}, policies show {w}"));
                        } else if *n != w {
                            wrong.push(format!("{at}: counted {n}, want {w}"));
                        }
                    }
                    (Got::Refused(msg), Must::SubsetOrRefused) => {
                        *refused
                            .entry(format!(
                                "{p:?}: {}",
                                msg.chars().take(90).collect::<String>()
                            ))
                            .or_default() += 1;
                    }
                    (other, _) => wrong.push(format!("{at}: {other:?}")),
                }
            }
        }
    }
    eprintln!("{checks} path checks; refused (allowed): {refused:#?}");
    assert!(
        leaks.is_empty(),
        "hidden records returned:\n{}",
        leaks.join("\n")
    );
    assert!(wrong.is_empty(), "wrong answers:\n{}", wrong.join("\n"));

    // RL66: a DELETE inside a transaction on the shared record hierarchy
    // (it failed with 1305, `SAVEPOINT sprig_types does not exist`, with or
    // without policies).
    for (level, user, id) in [
        (3, "app", 12),
        (1, "app", 12),
        (2, "alice", 10),
        (3, "bob", 2),
    ] {
        let mut c = f.session(&e.mysql, level, user).await;
        must(&mut c, "BEGIN").await;
        let r = try_exec(&mut c, &format!("DELETE FROM docs WHERE id = {id}")).await;
        let n = c.affected_rows();
        must(&mut c, "ROLLBACK").await;
        assert!(
            r.is_ok(),
            "DELETE in a transaction, {user}@{}: {r:?}",
            LEVEL_NAME[level]
        );
        eprintln!(
            "DELETE in a transaction, {user}@{}: {n} row(s)",
            LEVEL_NAME[level]
        );
    }

    // RL65: an UPDATE or DELETE of an inherited record is governed by the
    // UPDATE and DELETE policies, never INSERT's. `wdocs` in b, shared to
    // b1; alice writes at b1, where every record is inherited.
    write_combinations(&f, &mut app[1], &format!("rlsi_{tag}{pid}_wb")).await;

    // Vector search over an inherited table (read through from a), where
    // the inheritance layer takes a VECTOR column.
    let vsetup: Result<(), String> = async {
        for q in [
            "CREATE TABLE vdocs (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, emb VECTOR(2) \
             NOT NULL)",
            "ALTER TABLE vdocs SPRIG INHERITABLE KEY (id)",
            "INSERT INTO vdocs VALUES (1, 'alice', VEC_FromText('[1,0]')), (2, 'bob', \
             VEC_FromText('[0,1]')), (3, 'alice', VEC_FromText('[2,2]'))",
        ] {
            try_exec(&mut app[0], q).await?;
        }
        let q = format!(
            "CREATE SHARE rlsi_{tag}{pid}_vab ON vdocs TO SPACE `{}` WITH RESHARE",
            e.t[1]
        );
        try_exec(&mut app[0], &q).await?;
        try_exec(&mut app[1], "SELECT 1").await?;
        let q = format!(
            "CREATE SHARE rlsi_{tag}{pid}_vbc ON vdocs TO SPACE `{}`",
            e.t[2]
        );
        try_exec(&mut app[1], &q).await?;
        try_exec(&mut app[2], "SELECT 1").await?;
        try_exec(&mut app[2], "ALTER TABLE vdocs ENABLE ROW LEVEL SECURITY").await?;
        try_exec(
            &mut app[2],
            "CREATE POLICY p_va ON vdocs FOR SELECT TO alice USING (owner = 'alice')",
        )
        .await
    }
    .await;
    match vsetup {
        Ok(()) => {
            let mut c = f.session(&e.mysql, 3, "alice").await;
            let got = ids(
                &mut c,
                "SELECT id FROM vdocs ORDER BY VEC_DISTANCE_EUCLIDEAN(emb, VEC_FromText('[0,0]')) \
                 LIMIT 10",
            )
            .await;
            eprintln!("vector search alice@c: {got:?}");
            if let Ok(s) = &got {
                assert_eq!(*s, BTreeSet::from([1, 3]), "vector search alice@c");
            }
        }
        Err(e) => eprintln!("vector table: not available: {e}"),
    }

    // Change streams: c's alice gets a's changes only for records c's
    // policies show her; eve, whom no policy names, gets nothing.
    let mut s_alice = open_stream(&e, &e.t[2], "alice", &db).await;
    let mut s_eve = open_stream(&e, &e.t[2], "eve", &db).await;
    let mut s_bob_b = open_stream(&e, &e.t[1], "bob", &db).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    for w in [
        // alice's: shown to alice at c, not to bob at b.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (50, 1, 'alice', 1, 1)",
        // bob's, visible at b: bob at b sees it; alice at c doesn't.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (51, 1, 'bob', 1, 1)",
        // bob's, invisible: hidden by b's restrictive policy.
        "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (52, 1, 'bob', 0, 1)",
        "UPDATE docs SET body = 'changed' WHERE id IN (50, 51, 52)",
        // carol's tier-3 record 3: c's p_t3 shows it to alice.
        "UPDATE docs SET body = 'changed' WHERE id = 3",
        // carol's record 6: nobody's.
        "UPDATE docs SET body = 'changed' WHERE id = 6",
        // alice's record 1: b's override made it invisible, which b's
        // policies hide; c's policies don't read `visible` (a parent's
        // policies don't bind a child's readers), so alice at c sees it.
        "UPDATE docs SET body = 'changed' WHERE id = 1",
    ] {
        must(&mut app[0], w).await;
    }
    let ka = stream_keys(&mut s_alice, 12).await;
    let ke = stream_keys(&mut s_eve, 1).await;
    let kb = stream_keys(&mut s_bob_b, 1).await;
    eprintln!("streams: alice@c {ka:?}; eve@c {ke:?}; bob@b {kb:?}");
    let keys = |v: &[(String, i64)]| -> BTreeSet<i64> { v.iter().map(|(_, k)| *k).collect() };
    let leak_a: Vec<i64> = keys(&ka)
        .into_iter()
        .filter(|k| ![50, 3, 1].contains(k))
        .collect();
    assert!(
        leak_a.is_empty(),
        "alice@c got hidden records' events: {leak_a:?}"
    );
    assert!(
        keys(&ka).contains(&50),
        "alice@c missed her record's insert: {ka:?}"
    );
    assert!(ke.is_empty(), "eve@c got events: {ke:?}");
    let leak_b: Vec<i64> = keys(&kb).into_iter().filter(|k| *k != 51).collect();
    assert!(
        leak_b.is_empty(),
        "bob@b got hidden records' events: {leak_b:?}"
    );
    assert!(keys(&kb).contains(&51), "bob@b missed his record: {kb:?}");

    drop((s_alice, s_eve, s_bob_b));

    // RL64: a delete reaches a subscriber only for a record it could see.
    // In a, alice's records 60-63; c gives 60 to bob and hides 61, then a
    // deletes 60, 61 and 62 and c gives 63 to bob. alice at c saw 62 and 63
    // only, so she gets their deletes and nothing of 60 or 61 (before,
    // a's row passing c's policies sent her a delete of each).
    for id in 60..=63 {
        must(
            &mut app[0],
            &format!(
                "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES ({id}, 1, \
                 'alice', 1, 1)"
            ),
        )
        .await;
    }
    let want_c: BTreeSet<i64> = [60, 61, 62, 63].into();
    let mut seen_c = BTreeSet::new();
    for _ in 0..60 {
        let mut c = f.session(&e.mysql, 3, "app").await;
        seen_c = ids(&mut c, "SELECT id FROM docs WHERE id BETWEEN 60 AND 63")
            .await
            .unwrap_or_default();
        if seen_c == want_c {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(seen_c, want_c, "60-63 at c");
    {
        let mut c = f.session(&e.mysql, 3, "app").await;
        must(&mut c, "UPDATE docs SET owner = 'bob' WHERE id = 60").await;
        must(&mut c, "DELETE FROM docs WHERE id = 61").await;
    }
    let mut s_alice = open_stream(&e, &e.t[2], "alice", &db).await;
    let mut s_eve = open_stream(&e, &e.t[2], "eve", &db).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    must(&mut app[0], "DELETE FROM docs WHERE id IN (60, 61, 62)").await;
    {
        let mut c = f.session(&e.mysql, 3, "app").await;
        must(&mut c, "UPDATE docs SET owner = 'bob' WHERE id = 63").await;
    }
    let ka = stream_keys(&mut s_alice, 12).await;
    let ke = stream_keys(&mut s_eve, 1).await;
    eprintln!("delete streams: alice@c {ka:?}; eve@c {ke:?}");
    let deleted: BTreeSet<i64> = ka
        .iter()
        .filter(|(op, _)| op == "delete")
        .map(|(_, k)| *k)
        .collect();
    let leaked: Vec<i64> = keys(&ka)
        .into_iter()
        .filter(|k| [60, 61].contains(k))
        .collect();
    assert!(
        leaked.is_empty(),
        "alice@c got events of records she never saw: {ka:?}"
    );
    // Materialized, c reads a's records through its copy: the delete of
    // 62 comes from the copy's tombstone (RL69), a's own event while the
    // copy serves sending nothing.
    assert_eq!(
        deleted,
        BTreeSet::from([62, 63]),
        "alice@c's deletes: {ka:?}"
    );
    assert!(ke.is_empty(), "eve@c got events: {ke:?}");
    drop((s_alice, s_eve));

    // RL69: with materialized edges, the copy's changes reach change
    // streams, and a's own changes, which c's view doesn't read while the
    // copy serves, send nothing. The worker (which keeps the copies) is
    // stopped while a deletes 70, updates 71 and inserts 73, so the copy
    // lags: alice at c gets nothing then (before, the delete came as an
    // `update` and its delete never came), and exactly a delete of 70, an
    // update of 71 and an insert of 73 once it catches up.
    if e.materialized {
        lagging_copy(&f, &mut app[0]).await;
        flips(&f, &mut app).await;
    }

    // Cleanup, best effort: rows below a, shares bottom first, users, the
    // space, the databases.
    for level in (1..LEVELS).rev() {
        let mut c = f.session(&e.mysql, level, "app").await;
        // Own records first (a DELETE of an inherited one hides it), then
        // overrides and hide markers.
        let _ = try_exec(&mut c, "DELETE FROM docs WHERE id > 1000000").await;
        let _ = try_exec(&mut c, "SPRIG RESET docs").await;
        let _ = try_exec(&mut c, "SPRIG RESET vdocs").await;
    }
    for (lv, sh) in [
        (1, "fbc"),
        (0, "fab"),
        (1, "wb"),
        (1, "vbc"),
        (0, "vab"),
        (1, "bc"),
        (1, "bb"),
        (0, "ab"),
    ] {
        let q = format!("DROP SHARE IF EXISTS rlsi_{tag}{pid}_{sh}");
        let _ = try_exec(&mut app[lv], &q).await;
    }
    let _ = try_exec(&mut app[1], &format!("DROP SPACE IF EXISTS `{b1}` CASCADE")).await;
    for c in app.iter_mut().rev() {
        let _ = try_exec(c, "DROP GRAPH IF EXISTS rlsi_g").await;
        for u in USERS {
            let _ = try_exec(c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
        }
        let _ = try_exec(c, &format!("DROP DATABASE IF EXISTS `{db}`")).await;
    }
}

/// The ops a stream got per key.
fn ops_by_key(v: &[(String, i64)]) -> BTreeMap<i64, Vec<String>> {
    let mut out: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for (op, k) in v {
        out.entry(*k).or_default().push(op.clone());
    }
    out
}

/// RL69's lagging copy (materialized mode); needs the worker's pid in
/// `SPRIG_TEST_RLSI_WORKER_PID` (it is stopped with `SIGSTOP` for a few
/// seconds), else skipped.
async fn lagging_copy(f: &Fixture<'_>, app_a: &mut mysql_async::Conn) {
    let e = f.e;
    let Ok(worker) = std::env::var("SPRIG_TEST_RLSI_WORKER_PID") else {
        eprintln!("lagging copy: skipped (set SPRIG_TEST_RLSI_WORKER_PID)");
        return;
    };
    let signal = |sig: &str| {
        let ok = std::process::Command::new("kill")
            .args([sig, worker.as_str()])
            .status()
            .is_ok_and(|s| s.success());
        assert!(ok, "kill {sig} {worker}");
    };
    for id in 70..=72 {
        must(
            app_a,
            &format!(
                "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES ({id}, 1,                  'alice', 1, 1)"
            ),
        )
        .await;
    }
    let want: BTreeSet<i64> = [70, 71, 72].into();
    let mut seen = BTreeSet::new();
    for _ in 0..60 {
        let mut c = f.session(&e.mysql, 3, "app").await;
        seen = ids(&mut c, "SELECT id FROM docs WHERE id BETWEEN 70 AND 73")
            .await
            .unwrap_or_default();
        if seen == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(seen, want, "70-72 at c");
    let mut s_alice = open_stream(e, &e.t[2], "alice", &f.db).await;
    let mut s_eve = open_stream(e, &e.t[2], "eve", &f.db).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    signal("-STOP");
    let lag = async {
        must(app_a, "DELETE FROM docs WHERE id = 70").await;
        must(app_a, "UPDATE docs SET body = 'lagged' WHERE id = 71").await;
        must(
            app_a,
            "INSERT INTO docs (id, parent_id, owner, visible, tier) VALUES (73, 1, 'alice', 1, 1)",
        )
        .await;
        let mut c = f.session(&e.mysql, 3, "app").await;
        let at_c = ids(&mut c, "SELECT id FROM docs WHERE id BETWEEN 70 AND 73").await;
        (stream_keys(&mut s_alice, 6).await, at_c)
    }
    .await;
    signal("-CONT");
    let (during, at_c) = lag;
    eprintln!("lagging copy: c read {at_c:?} while the worker was stopped");
    let after = stream_keys(&mut s_alice, 20).await;
    let ke = stream_keys(&mut s_eve, 1).await;
    eprintln!("lagging copy: alice@c while stopped {during:?}; after {after:?}; eve@c {ke:?}");
    assert_eq!(
        at_c.unwrap_or_default(),
        want,
        "the copy didn't lag (the worker kept writing it)"
    );
    assert!(
        during.is_empty(),
        "alice@c got a's own changes while the copy lagged: {during:?}"
    );
    let got = ops_by_key(&after);
    let wanted: BTreeMap<i64, Vec<String>> = [
        (70, vec!["delete".to_owned()]),
        (71, vec!["update".to_owned()]),
        (73, vec!["insert".to_owned()]),
    ]
    .into();
    assert_eq!(got, wanted, "alice@c's events once the copy caught up");
    assert!(ke.is_empty(), "eve@c got events: {ke:?}");
}

/// Runs `sql` (a `CREATE SHARE`) with `inherit.merge = 'materialized'`.
async fn materialized(c: &mut mysql_async::Conn, sql: &str) {
    must(c, "SPRIG SET inherit.merge = 'materialized'").await;
    must(c, sql).await;
    must(c, "SPRIG RESET inherit.merge").await;
}

/// RL71: exactly once across a copy's flips. Table `fdocs` is shared
/// `a > b > c`, both edges materialized, and `alice` at `c` watches it.
/// Each cycle flips a copy while `a` and `b` write, every 200 ms for 4 s:
/// `a` inserts a new record, updates one of its first 20 and deletes one
/// it inserted in the cycle before; `b` inserts one of its own and deletes
/// one from the cycle before. The cycles alternate: `b` drops and makes
/// again its share to `c` (a first seed of `c`'s copy), `b` changes that
/// share's filter (a reseed of `c`'s copy), `a` changes its share's filter
/// (a reseed of `b`'s copy). Every change must reach `alice` exactly once:
/// each new record exactly an `insert` then a `delete`, each of the first
/// 20 exactly one `update` per cycle. `SPRIG_TEST_RLSI_FLIPS` cycles (9
/// by default).
async fn flips(f: &Fixture<'_>, app: &mut [mysql_async::Conn]) {
    let e = f.e;
    let db = &f.db;
    let cycles: usize = std::env::var("SPRIG_TEST_RLSI_FLIPS")
        .ok()
        .map_or(9, |v| v.parse().unwrap());
    let (fab, fbc) = (format!("{db}_fab"), format!("{db}_fbc"));
    must(
        &mut app[0],
        "CREATE TABLE fdocs (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, body VARCHAR(40) \
         NOT NULL DEFAULT 'x')",
    )
    .await;
    must(&mut app[0], "ALTER TABLE fdocs SPRIG INHERITABLE KEY (id)").await;
    for id in 1..=20 {
        must(
            &mut app[0],
            &format!("INSERT INTO fdocs (id, owner) VALUES ({id}, 'alice')"),
        )
        .await;
    }
    let share = |name: &str, to: &str, reshare: bool| {
        format!(
            "CREATE SHARE {name} ON fdocs TO SPACE `{to}`{}",
            if reshare { " WITH RESHARE" } else { "" }
        )
    };
    materialized(&mut app[0], &share(&fab, &e.t[1], true)).await;
    let make_bc = share(&fbc, &e.t[2], false);
    materialized(&mut app[1], &make_bc).await;
    // Both copies serve: c reads the 20 records.
    let base: BTreeSet<i64> = (1..=20).collect();
    let mut at_c = BTreeSet::new();
    for _ in 0..60 {
        let mut c = f.session(&e.mysql, 3, "app").await;
        at_c = ids(&mut c, "SELECT id FROM fdocs")
            .await
            .unwrap_or_default();
        if at_c == base {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(at_c, base, "fdocs at c");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut s = open_stream_on(e, &e.t[2], "alice", db, "fdocs").await;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let mut chunk = vec![0u8; 65536];
        tokio::pin!(stopped);
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                r = s.read(&mut chunk) => match r {
                    Ok(n) if n > 0 => buf.extend_from_slice(&chunk[..n]),
                    _ => break,
                },
            }
        }
        String::from_utf8_lossy(&buf)
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
            .filter(|v| v.is_object() && v["op"] != "heartbeat")
            .filter_map(|v| {
                let key: serde_json::Value =
                    serde_json::from_str(v["key"].as_str().unwrap_or("null")).ok()?;
                Some((v["op"].as_str()?.to_owned(), key["id"].as_i64()?))
            })
            .collect::<Vec<_>>()
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut want: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let push = |want: &mut BTreeMap<i64, Vec<String>>, k: i64, op: &str| {
        want.entry(k).or_default().push(op.to_owned());
    };
    let new_a = |cycle: usize, i: usize| i64::try_from(100_000 + cycle * 1000 + i).unwrap();
    let new_b = |cycle: usize, i: usize| i64::try_from(200_000 + cycle * 1000 + i).unwrap();
    for cycle in 0..=cycles {
        let kind = cycle % 3;
        let last = cycle == cycles;
        let flip = std::time::Instant::now();
        if !last {
            match kind {
                // A first seed of c's copy.
                0 => {
                    must(&mut app[1], &format!("DROP SHARE {fbc}")).await;
                    materialized(&mut app[1], &make_bc).await;
                }
                // A reseed of c's copy.
                1 => {
                    must(
                        &mut app[1],
                        &format!("ALTER SHARE {fbc} WHERE id > -{}", cycle + 1),
                    )
                    .await;
                }
                // A reseed of b's copy.
                _ => {
                    must(
                        &mut app[0],
                        &format!("ALTER SHARE {fab} WHERE id > -{}", cycle + 1),
                    )
                    .await;
                }
            }
        }
        for i in 0..20 {
            if !last {
                let k = new_a(cycle, i);
                must(
                    &mut app[0],
                    &format!("INSERT INTO fdocs (id, owner) VALUES ({k}, 'alice')"),
                )
                .await;
                push(&mut want, k, "insert");
                let k = i64::try_from(i + 1).unwrap();
                must(
                    &mut app[0],
                    &format!("UPDATE fdocs SET body = 'c{cycle}' WHERE id = {k}"),
                )
                .await;
                push(&mut want, k, "update");
                let k = new_b(cycle, i);
                must(
                    &mut app[1],
                    &format!("INSERT INTO fdocs (id, owner) VALUES ({k}, 'alice')"),
                )
                .await;
                push(&mut want, k, "insert");
            }
            if cycle > 0 {
                let k = new_a(cycle - 1, i);
                must(&mut app[0], &format!("DELETE FROM fdocs WHERE id = {k}")).await;
                push(&mut want, k, "delete");
                let k = new_b(cycle - 1, i);
                must(&mut app[1], &format!("DELETE FROM fdocs WHERE id = {k}")).await;
                push(&mut want, k, "delete");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        eprintln!(
            "flips: cycle {cycle} (kind {kind}) written in {:.1} s",
            flip.elapsed().as_secs_f64()
        );
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _ = stop.send(());
    let got = reader.await.unwrap();
    let got = ops_by_key(&got);
    let mut wrong = Vec::new();
    for (k, w) in &want {
        let g = got.get(k).cloned().unwrap_or_default();
        if &g != w {
            wrong.push(format!("{k}: want {w:?}, got {g:?}"));
        }
    }
    for (k, g) in &got {
        if !want.contains_key(k) {
            wrong.push(format!("{k}: unexpected {g:?}"));
        }
    }
    let changes: usize = want.values().map(Vec::len).sum();
    eprintln!(
        "flips: {cycles} flips, {changes} changes, {} keys, {} wrong",
        want.len(),
        wrong.len()
    );
    assert!(
        wrong.is_empty(),
        "alice@c didn't get every change exactly once across the flips:\n{}",
        wrong.join("\n")
    );
}

/// RL65's policy combinations: `alice` at `b1` updates and deletes records
/// she inherits from `b`, under each set of policies on `wdocs`.
async fn write_combinations(f: &Fixture<'_>, app_b: &mut mysql_async::Conn, share: &str) {
    for q in [
        "CREATE TABLE wdocs (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, body VARCHAR(20) \
         NOT NULL DEFAULT 'x')",
        "ALTER TABLE wdocs SPRIG INHERITABLE KEY (id)",
        "INSERT INTO wdocs (id, owner) VALUES (1, 'alice'), (2, 'alice'), (3, 'alice'), (4, \
         'alice'), (5, 'bob'), (6, 'alice')",
    ] {
        must(app_b, q).await;
    }
    must(
        app_b,
        &format!("CREATE SHARE {share} ON wdocs TO SPACE `{}`", f.b1),
    )
    .await;
    must(app_b, "ALTER TABLE wdocs ENABLE ROW LEVEL SECURITY").await;
    let cases: [(&str, &[&str]); 4] = [
        (
            "SELECT and UPDATE",
            &[
                "CREATE POLICY w_s ON wdocs FOR SELECT TO alice USING (owner = 'alice')",
                "CREATE POLICY w_u ON wdocs FOR UPDATE TO alice USING (owner = 'alice')",
            ],
        ),
        (
            "SELECT and UPDATE WITH CHECK",
            &[
                "CREATE POLICY w_s ON wdocs FOR SELECT TO alice USING (owner = 'alice')",
                "CREATE POLICY w_u ON wdocs FOR UPDATE TO alice USING (owner = 'alice') WITH \
                 CHECK (owner = 'alice')",
            ],
        ),
        (
            "SELECT and DELETE",
            &[
                "CREATE POLICY w_s ON wdocs FOR SELECT TO alice USING (owner = 'alice')",
                "CREATE POLICY w_d ON wdocs FOR DELETE TO alice USING (owner = 'alice')",
            ],
        ),
        (
            "SELECT and INSERT",
            &[
                "CREATE POLICY w_s ON wdocs FOR SELECT TO alice USING (owner = 'alice')",
                "CREATE POLICY w_i ON wdocs FOR INSERT TO alice WITH CHECK (TRUE)",
            ],
        ),
    ];
    // (statement, what it must do): Ok(n) rows affected, or Err(code).
    let steps: [&[(&str, Result<u64, &str>)]; 4] = [
        &[
            // An override, with no INSERT policy; then the override again.
            ("UPDATE wdocs SET body = 'u1' WHERE id = 1", Ok(1)),
            ("UPDATE wdocs SET body = 'u2' WHERE id = 1", Ok(1)),
            // No DELETE policy: nothing to delete.
            ("DELETE FROM wdocs WHERE id = 2", Ok(0)),
        ],
        &[
            // WITH CHECK, checked by MariaDB on the override.
            ("UPDATE wdocs SET owner = 'bob' WHERE id = 3", Err("39363")),
            ("UPDATE wdocs SET body = 'c2' WHERE id = 3", Ok(1)),
        ],
        &[
            // A hide marker, and a hide marker over an override.
            ("DELETE FROM wdocs WHERE id = 4", Ok(1)),
            ("DELETE FROM wdocs WHERE id = 1", Ok(1)),
            // No UPDATE policy: nothing to update.
            ("UPDATE wdocs SET body = 'no' WHERE id = 6", Ok(0)),
        ],
        &[
            // An INSERT policy alone gives neither.
            ("UPDATE wdocs SET body = 'i' WHERE id = 6", Ok(0)),
            ("DELETE FROM wdocs WHERE id = 6", Ok(0)),
        ],
    ];
    let mut failures = Vec::new();
    for ((name, pols), steps) in cases.iter().zip(steps) {
        for p in ["w_s", "w_u", "w_d", "w_i"] {
            let _ = try_exec(app_b, &format!("DROP POLICY IF EXISTS {p} ON wdocs")).await;
        }
        for p in *pols {
            must(app_b, p).await;
        }
        let mut c = f.session(&f.e.mysql, 2, "alice").await;
        for (sql, want) in steps {
            let r = c.query_drop(*sql).await;
            let got = match r {
                Ok(()) => Ok(c.affected_rows()),
                Err(e) => Err(e.to_string()),
            };
            let ok = match (&got, want) {
                (Ok(n), Ok(w)) => n == w,
                (Err(msg), Err(code)) => msg.contains(code),
                _ => false,
            };
            eprintln!("write combination {name}: {sql}: {got:?}");
            if !ok {
                failures.push(format!("{name}: {sql}: {got:?}, want {want:?}"));
            }
        }
    }
    // What the writes left: at b1, 1 and 4 hidden, 3's body changed; at b,
    // nothing.
    let mut app_b1 = f.session(&f.e.mysql, 2, "app").await;
    let at_b1 = ids(&mut app_b1, "SELECT id FROM wdocs").await;
    let body3: Option<String> = app_b1
        .query_first("SELECT body FROM wdocs WHERE id = 3")
        .await
        .unwrap_or(None);
    let at_b = ids(app_b, "SELECT id FROM wdocs").await;
    if at_b1 != Ok(BTreeSet::from([2, 3, 5, 6])) || body3.as_deref() != Some("c2") {
        failures.push(format!(
            "b1 after the writes: {at_b1:?}, body of 3 {body3:?}"
        ));
    }
    if at_b != Ok(BTreeSet::from([1, 2, 3, 4, 5, 6])) {
        failures.push(format!("b after the writes: {at_b:?}"));
    }
    assert!(
        failures.is_empty(),
        "writes to inherited records:\n{}",
        failures.join("\n")
    );
}

/// Raw MariaDB access for [`share_views_survive_restarts`].
struct Raw {
    conn: mysql_async::Conn,
    ids: Vec<u64>,
}

impl Raw {
    async fn open() -> Option<Self> {
        let addr = std::env::var("SPRIG_TEST_RLSI_RAW").ok()?;
        let ids: Vec<u64> = std::env::var("SPRIG_TEST_RLSI_TENANT_IDS")
            .ok()?
            .split(',')
            .map(|x| x.trim().parse().unwrap())
            .collect();
        assert_eq!(ids.len(), 3, "SPRIG_TEST_RLSI_TENANT_IDS: a,b,c");
        let pw =
            std::env::var("SPRIG_TEST_RLSI_RAW_PW").unwrap_or_else(|_| "sprig-dev-root".into());
        let (host, port) = addr.split_once(':').unwrap();
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some("root"))
            .pass(Some(pw))
            .prefer_socket(false);
        let conn = mysql_async::Conn::new(opts).await.unwrap();
        Some(Self { conn, ids })
    }

    /// Tenant `t`'s share views on `db`'s schemas: (schema, name, definer).
    async fn share_views(&mut self, t: usize) -> Vec<(String, String, String)> {
        let schemas: Vec<String> = self
            .conn
            .exec(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE ?",
                (format!("t{}\\_d%", self.ids[t]),),
            )
            .await
            .unwrap();
        let mut out = Vec::new();
        for sc in schemas {
            let rows: Vec<(String, String, String)> = self
                .conn
                .exec(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, DEFINER FROM information_schema.VIEWS \
                     WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE '\\_\\_sprig\\_share\\_%'",
                    (sc,),
                )
                .await
                .unwrap();
            out.extend(rows);
        }
        out
    }

    /// Every share view of `b` and `c` runs as its parent's share definer
    /// (RL70), which may read only the parent's relations.
    async fn check_definers(&mut self, when: &str) {
        for t in [1, 2] {
            let views = self.share_views(t).await;
            eprintln!("share views of tenant {t} {when}: {views:?}");
            assert!(!views.is_empty(), "tenant {t} has no share view {when}");
            let parent = format!("t{}_sprig_share@%", self.ids[t - 1]);
            for (sc, name, definer) in views {
                assert_eq!(
                    definer,
                    parent,
                    "{sc}.{name} runs as {definer} {when}, not as tenant {}'s share definer",
                    t - 1
                );
            }
            let grants: Vec<(String, String, String)> = self
                .conn
                .exec(
                    "SELECT Db, Table_name, Table_priv FROM mysql.tables_priv WHERE User = ? AND \
                     Host = '%'",
                    (format!("t{}_sprig_share", self.ids[t - 1]),),
                )
                .await
                .unwrap();
            let schemas: Vec<String> = self
                .conn
                .exec(
                    "SELECT Db FROM mysql.db WHERE User = ?",
                    (format!("t{}_sprig_share", self.ids[t - 1]),),
                )
                .await
                .unwrap();
            eprintln!("tenant {}'s share definer {when}: {grants:?}", t - 1);
            assert!(schemas.is_empty(), "no database-level grant: {schemas:?}");
            for (db, table, privs) in &grants {
                assert!(
                    db.starts_with(&format!("t{}_d", self.ids[t - 1])),
                    "the share definer reads only its tenant's schemas: {db}.{table}"
                );
                assert_eq!(privs, "Select", "{db}.{table}: SELECT only");
            }
        }
    }

    /// What a build before RL68 did: `c`'s share views run as one of `c`'s
    /// accounts.
    async fn move_to_child(&mut self) {
        let child: String = self
            .conn
            .exec_first(
                "SELECT User FROM mysql.user WHERE User LIKE ? ORDER BY User LIMIT 1",
                (format!("t{}\\_u%", self.ids[2]),),
            )
            .await
            .unwrap()
            .expect("c has a backend account");
        self.move_c_to(&child).await;
    }

    /// `c`'s share views run as `account` (host `%`).
    async fn move_c_to(&mut self, child: &str) {
        for (sc, name, _) in self.share_views(2).await {
            let row: mysql_async::Row = self
                .conn
                .query_first(format!("SHOW CREATE VIEW `{sc}`.`{name}`"))
                .await
                .unwrap()
                .unwrap();
            let create: String = row.get("Create View").unwrap();
            let at = create.find("DEFINER=`").unwrap();
            let end = at + create[at..].find(" SQL SECURITY").unwrap();
            let moved = format!(
                "CREATE OR REPLACE {}DEFINER=`{child}`@`%`{}",
                &create["CREATE ".len()..at],
                &create[end..]
            );
            self.conn.query_drop(moved).await.unwrap();
        }
    }
}

/// Reads `docs` at `b` and `c` (as `app`, and as `alice` bound by `c`'s
/// policy) until they answer, up to 30 s; `Err` with every error seen
/// when they never do, or when one was a 1356, a 1449 or a 1045 (a share
/// view's definer).
async fn read_inherited(e: &Env, db: &str) -> Result<(), Vec<String>> {
    let mut errs = Vec::new();
    for _ in 0..60 {
        let mut ok = true;
        for (t, user, want) in [
            (1, "app", vec![1, 2, 3, 4]),
            (2, "app", vec![1, 2, 3, 4]),
            (2, "alice", vec![1, 3]),
        ] {
            let got = async {
                let (host, port) = e.mysql.split_once(':').unwrap();
                let opts = mysql_async::OptsBuilder::default()
                    .ip_or_hostname(host)
                    .tcp_port(port.parse().unwrap())
                    .user(Some(format!("{}.{user}", e.t[t])))
                    .pass(Some(PW))
                    .db_name(Some(db))
                    .prefer_socket(false)
                    .ssl_opts(
                        mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true),
                    );
                let mut c = mysql_async::Conn::new(opts)
                    .await
                    .map_err(|x| x.to_string())?;
                let r = ids(&mut c, "SELECT id FROM docs").await;
                let _ = c.disconnect().await;
                r
            }
            .await;
            match got {
                Ok(g) if g == want.iter().copied().collect::<BTreeSet<i64>>() => {}
                Ok(g) => {
                    ok = false;
                    errs.push(format!("{user}@{t}: {g:?}"));
                }
                Err(x) => {
                    ok = false;
                    errs.push(format!("{user}@{t}: {x}"));
                }
            }
        }
        if ok {
            if !errs.is_empty() {
                eprintln!("reads answered after: {errs:?}");
            }
            return if errs
                .iter()
                .any(|x| x.contains("1356") || x.contains("1449") || x.contains("1045"))
            {
                Err(errs)
            } else {
                Ok(())
            };
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(errs)
}

/// RL68 and RL70: read-through share views keep working across gateway
/// restarts (`kill -9`, a clean stop) and a restart of every process, and
/// when the users who made the shares are dropped: they run as the
/// parent's share definer, never a user's account. A share view an older
/// build left with another definer (the share's dropped creator: every
/// read failing with 1449; the child's account: 1356) is re-pointed at the
/// gateway's start, and by the drift sweep while it runs
/// (`SPRIG_TEST_RLSI_SWEEP_SECS`: the deployment's
/// `auth.reconcile_interval_seconds`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn share_views_survive_restarts() {
    let Some(e) = env() else {
        return;
    };
    let Ok(restart) = std::env::var("SPRIG_TEST_RLSI_RESTART") else {
        eprintln!("skipped: set SPRIG_TEST_RLSI_RESTART");
        return;
    };
    let pid = std::process::id() % 10_000;
    let db = format!("rlsr_{pid}");
    let mut app: Vec<mysql_async::Conn> = Vec::new();
    for t in &e.t {
        let mut c = conn(&e.mysql, t, "app").await;
        must(&mut c, &format!("USE SPACE `{t}`")).await;
        app.push(c);
    }
    must(&mut app[0], &format!("CREATE DATABASE `{db}`")).await;
    must(&mut app[0], &format!("USE `{db}`")).await;
    must(
        &mut app[0],
        "CREATE TABLE docs (id INT PRIMARY KEY, owner VARCHAR(20) NOT NULL, body VARCHAR(40) NOT \
         NULL DEFAULT 'x')",
    )
    .await;
    must(&mut app[0], "ALTER TABLE docs SPRIG INHERITABLE KEY (id)").await;
    must(
        &mut app[0],
        "INSERT INTO docs (id, owner) VALUES (1, 'alice'), (2, 'bob'), (3, 'alice'), (4, 'bob')",
    )
    .await;
    // The shares are made by users `sharer` of `a` and `b`, dropped below
    // (RL70).
    for (t, c) in app.iter_mut().enumerate().take(2) {
        let _ = try_exec(c, "DROP USER IF EXISTS 'sharer'@'%'").await;
        must(c, &format!("CREATE USER 'sharer'@'%' IDENTIFIED BY '{PW}'")).await;
        must(c, &format!("GRANT ALL ON `{db}`.* TO 'sharer'@'%'")).await;
        let mut sh = conn(&e.mysql, &e.t[t], "sharer").await;
        must(&mut sh, &format!("USE `{db}`")).await;
        let to = if t == 0 {
            format!(
                "CREATE SHARE {db}_ab ON docs TO SPACE `{}` WITH RESHARE",
                e.t[1]
            )
        } else {
            format!("CREATE SHARE {db}_bc ON docs TO SPACE `{}`", e.t[2])
        };
        must(&mut sh, &to).await;
        drop(sh);
    }
    must(&mut app[1], &format!("USE `{db}`")).await;
    must(&mut app[2], &format!("USE `{db}`")).await;
    let _ = try_exec(&mut app[2], "DROP USER IF EXISTS 'alice'@'%'").await;
    must(
        &mut app[2],
        &format!("CREATE USER 'alice'@'%' IDENTIFIED BY '{PW}'"),
    )
    .await;
    must(
        &mut app[2],
        &format!("GRANT SELECT ON `{db}`.* TO 'alice'@'%'"),
    )
    .await;
    must(&mut app[2], "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;
    must(
        &mut app[2],
        "CREATE POLICY p_alice ON docs FOR SELECT TO alice USING (owner = 'alice')",
    )
    .await;
    drop(app);
    let mut raw = Raw::open().await;
    if raw.is_none() {
        eprintln!("definers: not checked (set SPRIG_TEST_RLSI_RAW and SPRIG_TEST_RLSI_TENANT_IDS)");
    }
    let r = read_inherited(&e, &db).await;
    assert_eq!(r, Ok(()), "reads before any restart");
    if let Some(raw) = &mut raw {
        raw.check_definers("before any restart").await;
    }
    // The users who made the shares go (RL70): the children's reads don't
    // depend on them.
    {
        let mut a = conn(&e.mysql, &e.t[0], "app").await;
        let mut b = conn(&e.mysql, &e.t[1], "app").await;
        must(&mut a, "DROP USER 'sharer'@'%'").await;
        must(&mut b, "DROP USER 'sharer'@'%'").await;
    }
    let r = read_inherited(&e, &db).await;
    assert_eq!(r, Ok(()), "reads after the shares' creators were dropped");
    if let Some(raw) = &mut raw {
        raw.check_definers("after the shares' creators were dropped")
            .await;
    }
    // A user id no tenant has: what a view made before RL70 runs as once
    // the user who made its share is dropped.
    let gone = raw.as_ref().map(|r| format!("t{}_u999999", r.ids[1]));
    for how in ["kill9", "term", "all"] {
        if how == "kill9"
            && let (Some(raw), Some(gone)) = (&mut raw, &gone)
        {
            // A view made before RL70 whose share's creator was dropped:
            // reads fail until the gateway starts: 1449, or 1045 naming the
            // reader (MariaDB's answer to an account without `SET USER`).
            raw.move_c_to(gone).await;
            let mut c = conn(&e.mysql, &e.t[2], "app").await;
            must(&mut c, &format!("USE `{db}`")).await;
            let r = ids(&mut c, "SELECT id FROM docs").await;
            eprintln!("c's share views run as a dropped user: {r:?}");
            assert!(
                r.as_ref()
                    .is_err_and(|x| x.contains("1449") || x.contains("1045")),
                "a share view running as a dropped user must fail with 1449 or 1045: {r:?}"
            );
        }
        if how == "term"
            && let Some(raw) = &mut raw
        {
            // An older build's damage, on a running gateway: reads fail.
            raw.move_to_child().await;
            let mut c = conn(&e.mysql, &e.t[2], "app").await;
            must(&mut c, &format!("USE `{db}`")).await;
            let r = ids(&mut c, "SELECT id FROM docs").await;
            eprintln!("c's share views moved to c's account: {r:?}");
            assert!(
                r.as_ref().is_err_and(|x| x.contains("1356")),
                "a share view running as the child's account must fail with 1356: {r:?}"
            );
        }
        let out = std::process::Command::new("sh")
            .args(["-c", &format!("{restart} {how}")])
            .output()
            .unwrap();
        eprintln!(
            "restart {how}: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.status.success(), "restart {how}");
        // Twice: the first statements reconcile the tenants' grants and
        // make the views; nothing may move them afterwards.
        for pass in 0..2 {
            let r = read_inherited(&e, &db).await;
            assert_eq!(r, Ok(()), "reads after restart {how}, pass {pass}");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        if let Some(raw) = &mut raw {
            raw.check_definers(&format!("after restart {how}")).await;
        }
    }
    // The drift sweep re-points a view while the gateway runs.
    if let (Some(raw), Some(gone), Ok(secs)) = (
        &mut raw,
        &gone,
        std::env::var("SPRIG_TEST_RLSI_SWEEP_SECS").map(|s| s.parse::<u64>().unwrap()),
    ) {
        raw.move_c_to(gone).await;
        let started = std::time::Instant::now();
        let mut last = Err(String::new());
        while started.elapsed() < Duration::from_secs(secs * 2 + 30) {
            let mut c = conn(&e.mysql, &e.t[2], "app").await;
            must(&mut c, &format!("USE `{db}`")).await;
            last = ids(&mut c, "SELECT id FROM docs").await;
            if last.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        eprintln!(
            "drift sweep: c read {last:?} {:.1} s after its views were moved to a dropped user",
            started.elapsed().as_secs_f64()
        );
        assert!(
            last.is_ok(),
            "the drift sweep didn't repair c's share views: {last:?}"
        );
        assert_eq!(
            read_inherited(&e, &db).await,
            Ok(()),
            "reads after the sweep"
        );
        raw.check_definers("after the drift sweep").await;
    }
    // Cleanup by exact name.
    let mut a = conn(&e.mysql, &e.t[0], "app").await;
    let mut b = conn(&e.mysql, &e.t[1], "app").await;
    let mut c = conn(&e.mysql, &e.t[2], "app").await;
    for x in [&mut a, &mut b, &mut c] {
        let _ = try_exec(x, &format!("USE `{db}`")).await;
    }
    let _ = try_exec(&mut c, "DROP USER IF EXISTS 'alice'@'%'").await;
    let _ = try_exec(&mut b, &format!("DROP SHARE IF EXISTS {db}_bc")).await;
    let _ = try_exec(&mut a, &format!("DROP SHARE IF EXISTS {db}_ab")).await;
    for x in [&mut c, &mut b, &mut a] {
        let _ = try_exec(x, &format!("DROP DATABASE IF EXISTS `{db}`")).await;
    }
}
