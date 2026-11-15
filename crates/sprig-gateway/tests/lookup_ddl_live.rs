//! Global lookup indexes through the gateway (the DDL path, `ADD UNIQUE`
//! on a sharded table), live on a private deployment:
//!
//! 1. `customers (id PK, email, name)` with 20,000 rows, pinned
//!    `SHARD KEY (id)`, made a group by the advisor and split into 4 shards.
//!    A point read by `email` goes to every shard (the gateway's
//!    `sprig_router_fanout_shards` histogram: 4 per read).
//! 2. `ALTER TABLE customers ADD UNIQUE KEY uk_email (email)` while 4
//!    writers insert, change emails and delete. Afterwards the same reads go
//!    to 1 shard each, `SPRIG SHOW LOOKUP INDEXES` and
//!    `information_schema.SPRIG_LOOKUP_INDEXES` show it `public`, and every
//!    row has exactly its lookup row (checked on the lookup shards).
//! 3. Uniqueness across shards: a duplicate insert and an update to a taken
//!    email fail with 1062; an update to a new email moves the row's
//!    lookup row; a delete releases it.
//! 4. A split of one shard while point reads run: reads stay right and
//!    narrow.
//! 5. `kill -9` of the gateway while a rebuilt lookup back-fills (drop,
//!    40,000 more rows, `ADD UNIQUE` again): the controller takes the
//!    stopped build over after `BUILD_LEASE` and makes it public; the rows
//!    and lookup rows match again.
//!
//! ```sh
//! source target/agent-lookupddl/env.sh   # a private deployment (gateway-dev.sh up)
//! export SPRIG_TEST_LKD_GATEWAY=127.0.0.1:46306 SPRIG_TEST_LKD_METRICS=127.0.0.1:46190
//! # a tenant without table groups yet: the advisor designs only a tenant's first groups
//! export SPRIG_TEST_LKD_TENANT=globex SPRIG_TEST_LKD_TENANT_ID=<id>
//! export SPRIG_TEST_LKD_CLUSTERS=127.0.0.1:13301,127.0.0.1:13311   # root, every cluster
//! export SPRIG_TEST_LKD_SPRIGCTL=target/agent-lookupddl/debug/sprigctl
//! export SPRIG_TEST_LKD_GW_PIDFILE=target/agent-lookupddl/gwdev/gateway.pid
//! export SPRIG_TEST_LKD_GW_RESTART='deploy/compose/gateway-dev.sh restart'
//! cargo test -p sprig-gateway --test lookup_ddl_live -- --nocapture --test-threads 1
//! ```
//!
//! Without `SPRIG_TEST_LKD_GATEWAY` the test says what it needs and passes.
//!
//! `lookup_followups_live` needs a fresh tenant too
//! (`SPRIG_TEST_LKD_TENANT`, `_TENANT_ID`) and a second gateway on the same
//! deployment (`SPRIG_TEST_LKD_GW2=127.0.0.1:<port>`,
//! `SPRIG_TEST_LKD_GW2_PIDFILE`):
//!
//! 1. Prepared `SPRIG SHOW LOOKUP INDEXES`, through `COM_STMT_PREPARE` and
//!    SQL `PREPARE`.
//! 2. A unique key of two columns without the distribution key: built,
//!    routed (1 shard per read), unique across shards, updates of one part.
//! 3. `ADD UNIQUE` over values that repeat across shards: 1062, and an
//!    `advisor.lookup` approvals item naming 10 of the 12 values, raised
//!    through the controller so that it notifies (with
//!    `SPRIG_TEST_LKD_CTL_METRICS=127.0.0.1:<controller telemetry port>`,
//!    the controller's `sprig_approvals_notifications_total` must grow).
//! 4. A user with table-level grants only: claims, releases and routed
//!    reads work; its backend account holds that table's lookup tables only.
//! 5. `DROP TABLE` and `DROP DATABASE` remove their tables' lookups (rows
//!    and tables).
//! 6. Gateway acknowledgements: with both gateways up a build's publishes
//!    take milliseconds; with the second paused (SIGSTOP) the build waits
//!    `lookup.publish_timeout_ms`, fences it, and goes on; resumed, the
//!    second gateway's old session reloads its catalog first, so it claims
//!    (a duplicate fails with 1062). A `kill -9` of the first gateway while
//!    a `DROP TABLE`'s lookup is `dropping`: the controller finishes it.
//! 7. `lookup.lock_wait_seconds` at a table's scope applies to that table's
//!    claims only; the controller's scheduled orphan scan
//!    (`lookup.orphan_scan_hours`) removes planted orphans once due.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row, SslOpts};

fn tls() -> SslOpts {
    SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true)
}

const DB: &str = "lookupddl_db";

struct Env {
    gateway: String,
    metrics: String,
    tenant: String,
    tenant_id: u64,
    clusters: Vec<String>,
    sprigctl: String,
    pidfile: String,
    restart: String,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

impl Env {
    fn load() -> Option<Self> {
        Some(Self {
            gateway: var("SPRIG_TEST_LKD_GATEWAY")?,
            metrics: var("SPRIG_TEST_LKD_METRICS")?,
            tenant: var("SPRIG_TEST_LKD_TENANT").unwrap_or_else(|| "acme".into()),
            tenant_id: var("SPRIG_TEST_LKD_TENANT_ID")?.parse().ok()?,
            clusters: var("SPRIG_TEST_LKD_CLUSTERS")?
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect(),
            sprigctl: var("SPRIG_TEST_LKD_SPRIGCTL")?,
            pidfile: var("SPRIG_TEST_LKD_GW_PIDFILE")?,
            restart: var("SPRIG_TEST_LKD_GW_RESTART")?,
        })
    }

    fn app(&self) -> Pool {
        let (h, p) = self.gateway.split_once(':').unwrap();
        let opts = OptsBuilder::default()
            .ip_or_hostname(h)
            .tcp_port(p.parse().unwrap())
            .user(Some(format!("{}.app", self.tenant)))
            .pass(Some("sprig-dev-app"))
            .ssl_opts(tls())
            .db_name(Some(DB))
            .pool_opts(
                PoolOpts::default()
                    .with_constraints(PoolConstraints::new(0, 16).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            );
        Pool::new(opts)
    }

    async fn root(&self, i: usize) -> Conn {
        let (h, p) = self.clusters[i].split_once(':').unwrap();
        Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(h)
                .tcp_port(p.parse().unwrap())
                .user(Some("root"))
                .pass(Some("sprig-dev-root")),
        )
        .await
        .unwrap()
    }

    fn sprigctl(&self, args: &[&str]) -> String {
        let out = std::process::Command::new(&self.sprigctl)
            .args(args)
            .output()
            .expect("sprigctl");
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            out.status.success(),
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        );
        text
    }
}

/// `sprig_router_fanout_shards` (sum, count) from the gateway's metrics.
fn fanout(addr: &str) -> (f64, f64) {
    let mut s = std::net::TcpStream::connect(addr).expect("metrics");
    write!(
        s,
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).unwrap();
    let (mut sum, mut count) = (0.0, 0.0);
    for line in body.lines() {
        let v = |l: &str| {
            l.rsplit(' ')
                .next()
                .and_then(|x| x.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        if line.starts_with("sprig_router_fanout_shards_sum") {
            sum += v(line);
        } else if line.starts_with("sprig_router_fanout_shards_count") {
            count += v(line);
        }
    }
    (sum, count)
}

/// Shards per point read by email over `n` reads.
async fn shards_per_read(e: &Env, c: &mut Conn, ids: &[i64]) -> f64 {
    let (s0, n0) = fanout(&e.metrics);
    for id in ids {
        let r: Option<i64> = c
            .exec_first(
                "SELECT id FROM customers WHERE email = ?",
                (format!("person{id}@x.org"),),
            )
            .await
            .unwrap();
        assert_eq!(r, Some(*id), "point read of person{id}");
    }
    let (s1, n1) = fanout(&e.metrics);
    (s1 - s0) / (n1 - n0).max(1.0)
}

fn errno_of<T>(r: mysql_async::Result<T>) -> Option<u16> {
    match r {
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        _ => None,
    }
}

/// Every lookup row of `uk_email`: email (lowercase) → owner ids.
async fn lookup_rows(e: &Env) -> BTreeMap<String, Vec<i64>> {
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for i in 0..e.clusters.len() {
        let mut r = e.root(i).await;
        let schemas: Vec<String> = r
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = \
                 '__sprig_lk_customers__uk_email' AND TABLE_SCHEMA LIKE ?",
                (format!("t{}\\_d%", e.tenant_id),),
            )
            .await
            .unwrap();
        for s in schemas {
            let rows: Vec<(String, i64)> = r
                .query(format!(
                    "SELECT email, owner_id FROM `{s}`.`__sprig_lk_customers__uk_email`"
                ))
                .await
                .unwrap();
            for (em, id) in rows {
                out.entry(em.to_lowercase()).or_default().push(id);
            }
        }
        r.disconnect().await.unwrap();
    }
    out
}

/// Every row's (email, id) through the gateway; the uniqueness check.
async fn owners(c: &mut Conn) -> BTreeMap<String, i64> {
    let rows: Vec<(Option<String>, i64)> =
        c.query("SELECT email, id FROM customers").await.unwrap();
    let mut out = BTreeMap::new();
    for (em, id) in rows {
        let Some(em) = em else { continue };
        let prev = out.insert(em.to_lowercase(), id);
        assert!(prev.is_none(), "email {em} held by {id} and {prev:?}");
    }
    out
}

/// Every owner has its lookup row; returns orphans (lookup rows whose
/// owner doesn't hold the value).
async fn check_consistent(e: &Env, c: &mut Conn, label: &str) -> usize {
    let own = owners(c).await;
    let lk = lookup_rows(e).await;
    for (em, id) in &own {
        let held = lk.get(em).cloned().unwrap_or_default();
        assert!(
            held.contains(id),
            "{label}: row {id} ({em}) has no lookup row (lookup holds {held:?})"
        );
    }
    let orphans = lk
        .iter()
        .filter(|(em, ids)| ids.iter().any(|i| own.get(*em) != Some(i)))
        .count();
    println!(
        "{label}: {} rows, {} lookup rows, {orphans} orphans",
        own.len(),
        lk.values().map(Vec::len).sum::<usize>()
    );
    orphans
}

async fn state_of(c: &mut Conn) -> Option<String> {
    let rows: Vec<Row> = c
        .query("SPRIG SHOW LOOKUP INDEXES FROM customers")
        .await
        .unwrap_or_default();
    rows.first().and_then(|r| r.get::<String, _>(6))
}

/// Writers changing `customers` until `stop`: inserts of new ids, email
/// changes and deletes. Returns (statements, errors).
fn writers(
    pool: &Pool,
    stop: &Arc<AtomicBool>,
    next: &Arc<AtomicI64>,
    n: usize,
) -> Vec<tokio::task::JoinHandle<(u64, u64)>> {
    (0..n)
        .map(|w| {
            let pool = pool.clone();
            let stop = Arc::clone(stop);
            let next = Arc::clone(next);
            tokio::spawn(async move {
                let (mut ok, mut err) = (0u64, 0u64);
                let mut k = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    k += 1;
                    let Ok(mut c) = pool.get_conn().await else {
                        err += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    };
                    let id = next.fetch_add(1, Ordering::Relaxed);
                    let r = match k % 3 {
                        0 => {
                            c.exec_drop(
                                "INSERT INTO customers (id, email, name) VALUES (?, ?, 'w')",
                                (id, format!("person{id}@x.org")),
                            )
                            .await
                        }
                        1 => {
                            let victim = 1 + (id * 7919 + i64::try_from(w).unwrap_or(0)) % 20_000;
                            c.exec_drop(
                                "UPDATE customers SET email = ? WHERE id = ?",
                                (format!("moved{id}@x.org"), victim),
                            )
                            .await
                        }
                        _ => {
                            let victim =
                                1 + (id * 104_729 + i64::try_from(w).unwrap_or(0)) % 20_000;
                            c.exec_drop("DELETE FROM customers WHERE id = ?", (victim,))
                                .await
                        }
                    };
                    if r.is_ok() {
                        ok += 1;
                    } else {
                        err += 1;
                    }
                }
                (ok, err)
            })
        })
        .collect()
}

async fn stop_writers(
    stop: &AtomicBool,
    ws: Vec<tokio::task::JoinHandle<(u64, u64)>>,
) -> (u64, u64) {
    stop.store(true, Ordering::Relaxed);
    let mut t = (0, 0);
    for w in ws {
        let (a, b) = w.await.unwrap();
        t.0 += a;
        t.1 += b;
    }
    t
}

/// The ids still holding their seeded email.
async fn seeded_ids(c: &mut Conn, n: usize) -> Vec<i64> {
    c.query(format!(
        "SELECT id FROM customers WHERE email LIKE 'person%' AND id <= 20000 ORDER BY id LIMIT {n}"
    ))
    .await
    .unwrap()
}

async fn group_of(cat: &mut Conn, tenant_id: u64) -> Option<u32> {
    cat.query_first(format!(
        "SELECT m.group_id FROM t{tenant_id}_sprig.group_members m JOIN t{tenant_id}_sprig.objects o \
         ON o.object_id = m.object_id WHERE o.name = 'customers' AND o.state = 'active'"
    ))
    .await
    .unwrap()
}

/// The group's shard ids once it has at least `n`.
async fn shard_count(e: &Env, group: u32, n: usize) -> Vec<u64> {
    let started = Instant::now();
    loop {
        let out = e.sprigctl(&["shard", "list", &e.tenant, "-o", "json"]);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap_or_default();
        let shards: Vec<u64> = v
            .as_array()
            .into_iter()
            .flatten()
            .filter(|g| g["group"].as_u64() == Some(u64::from(group)))
            .flat_map(|g| g["shards"].as_array().cloned().unwrap_or_default())
            .filter_map(|s| s["shard"].as_u64())
            .collect();
        if shards.len() >= n {
            return shards;
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "group {group} never reached {n} shards: {out}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lookup_ddl_live() {
    let Some(e) = Env::load() else {
        eprintln!(
            "skipped: set SPRIG_TEST_LKD_GATEWAY, _METRICS, _TENANT_ID, _CLUSTERS, _SPRIGCTL, \
             _GW_PIDFILE and _GW_RESTART (module docs)"
        );
        return;
    };
    let t0 = Instant::now();
    // ---- 1. a table in 4 shards ------------------------------------------
    {
        let (h, p) = e.gateway.split_once(':').unwrap();
        let mut c = Conn::new(
            OptsBuilder::default()
                .ip_or_hostname(h)
                .tcp_port(p.parse().unwrap())
                .user(Some(format!("{}.app", e.tenant)))
                .pass(Some("sprig-dev-app"))
                .ssl_opts(tls()),
        )
        .await
        .unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.disconnect().await.unwrap();
    }
    let pool = e.app();
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(
        "CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR(100) COLLATE \
         utf8mb4_uca1400_ai_ci, name VARCHAR(40))",
    )
    .await
    .unwrap();
    for b in 0..20 {
        let vals: Vec<String> = (b * 1000 + 1..=b * 1000 + 1000)
            .map(|i| format!("({i}, 'person{i}@x.org', 'n{i}')"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO customers (id, email, name) VALUES {}",
            vals.join(", ")
        ))
        .await
        .unwrap();
    }
    c.query_drop("SPRIG PIN customers SHARD KEY (id)")
        .await
        .unwrap();
    e.sprigctl(&["advisor", "run", &e.tenant, "-o", "json", "--yes"]);
    let mut cat = e.root(0).await;
    let run: Option<u64> = cat
        .query_first(format!(
            "SELECT MAX(run_id) FROM t{}_sprig.advisor_runs",
            e.tenant_id
        ))
        .await
        .unwrap();
    e.sprigctl(&[
        "advisor",
        "apply",
        &e.tenant,
        &run.unwrap().to_string(),
        "-o",
        "json",
        "--yes",
    ]);
    let started = Instant::now();
    let group = loop {
        if let Some(g) = group_of(&mut cat, e.tenant_id).await {
            break g;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "never grouped"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let first = shard_count(&e, group, 1).await;
    e.sprigctl(&[
        "shard",
        "split",
        &e.tenant,
        &group.to_string(),
        &first[0].to_string(),
        "--ways",
        "4",
        "--yes",
    ]);
    let four = shard_count(&e, group, 4).await;
    // The router has the 4-shard map once a scatter fans out to 4.
    let ids = seeded_ids(&mut c, 50).await;
    let started = Instant::now();
    let before = loop {
        let f = shards_per_read(&e, &mut c, &ids).await;
        if f >= 3.99 {
            break f;
        }
        assert!(started.elapsed() < Duration::from_secs(120), "fan-out {f}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    println!(
        "[{:?}] group {group}, shards {four:?}; a point read by email visits {before:.2} shards",
        t0.elapsed()
    );

    // ---- 2. ADD UNIQUE under writes --------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicI64::new(100_001));
    let ws = writers(&pool, &stop, &next, 4);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ddl_t = Instant::now();
    c.query_drop("ALTER TABLE customers ADD UNIQUE KEY uk_email (email)")
        .await
        .unwrap();
    let ddl_s = ddl_t.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (ok, err) = stop_writers(&stop, ws).await;
    println!(
        "[{:?}] ADD UNIQUE took {ddl_s:?} under {ok} writes ({err} failed)",
        t0.elapsed()
    );
    assert_eq!(state_of(&mut c).await.as_deref(), Some("public"));
    let view: Vec<(String, String, String)> = c
        .query(
            "SELECT TABLE_NAME, INDEX_NAME, STATE FROM information_schema.SPRIG_LOOKUP_INDEXES \
             WHERE TABLE_SCHEMA = 'lookupddl_db'",
        )
        .await
        .unwrap();
    assert_eq!(
        view,
        vec![("customers".into(), "uk_email".into(), "public".into())]
    );
    let shown: Vec<Row> = c.query("SHOW INDEX FROM customers").await.unwrap();
    assert!(
        shown
            .iter()
            .any(|r| r.get::<String, _>(2).as_deref() == Some("uk_email")),
        "SHOW INDEX lists uk_email"
    );
    check_consistent(&e, &mut c, "after ADD UNIQUE under writes").await;
    let ids = seeded_ids(&mut c, 200).await;
    let after = shards_per_read(&e, &mut c, &ids).await;
    println!("a point read by email now visits {after:.2} shards (was {before:.2})");
    assert!(after <= 1.01, "{after}");

    // ---- 3. uniqueness across shards -------------------------------------
    let a = ids[0];
    let b = *ids
        .iter()
        .find(|i| **i % 4 != a % 4 && **i != a)
        .unwrap_or(&ids[1]);
    let dup = c
        .exec_drop(
            "INSERT INTO customers (id, email, name) VALUES (?, ?, 'dup')",
            (900_001, format!("PERSON{a}@X.ORG")),
        )
        .await;
    assert_eq!(errno_of(dup), Some(1062), "a duplicate insert");
    let dup = c
        .exec_drop(
            "UPDATE customers SET email = ? WHERE id = ?",
            (format!("person{a}@x.org"), b),
        )
        .await;
    assert_eq!(errno_of(dup), Some(1062), "an update to a taken email");
    c.exec_drop(
        "UPDATE customers SET email = ? WHERE id = ?",
        ("renamed@x.org", b),
    )
    .await
    .unwrap();
    let r: Option<i64> = c
        .query_first("SELECT id FROM customers WHERE email = 'RENAMED@x.org'")
        .await
        .unwrap();
    assert_eq!(r, Some(b));
    let r: Option<i64> = c
        .exec_first(
            "SELECT id FROM customers WHERE email = ?",
            (format!("person{b}@x.org"),),
        )
        .await
        .unwrap();
    assert_eq!(r, None, "the old email finds nothing");
    // The freed email can be taken by another row now.
    c.exec_drop(
        "INSERT INTO customers (id, email, name) VALUES (?, ?, 'reuse')",
        (900_002, format!("person{b}@x.org")),
    )
    .await
    .unwrap();
    c.exec_drop("DELETE FROM customers WHERE id = ?", (900_002,))
        .await
        .unwrap();
    c.exec_drop(
        "INSERT INTO customers (id, email, name) VALUES (?, ?, 'again')",
        (900_003, format!("person{b}@x.org")),
    )
    .await
    .unwrap();
    // Multi-row: one statement with two equal emails.
    let dup = c
        .query_drop(
            "INSERT INTO customers (id, email) VALUES (900004, 'same@x.org'), (900005, 'SAME@x.org')",
        )
        .await;
    assert_eq!(errno_of(dup), Some(1062), "two equal emails in one insert");
    check_consistent(&e, &mut c, "after the uniqueness checks").await;
    // The seeded rows phase 3 left alone.
    let ids: Vec<i64> = ids.into_iter().filter(|i| *i != a && *i != b).collect();
    println!("[{:?}] uniqueness across shards holds", t0.elapsed());

    // ---- 4. a split while point reads run --------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let reads = AtomicU64::new(0);
    let reader = {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let ids = ids.clone();
        tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            let mut n = 0u64;
            let mut bad = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                for id in ids.iter().take(20) {
                    let r: mysql_async::Result<Option<i64>> = c
                        .exec_first(
                            "SELECT id FROM customers WHERE email = ?",
                            (format!("person{id}@x.org"),),
                        )
                        .await;
                    match r {
                        Ok(Some(x)) if x == *id => n += 1,
                        Ok(other) => bad.push((*id, format!("{other:?}"))),
                        Err(_) => {} // a held range's wait, retried next round
                    }
                }
            }
            (n, bad)
        })
    };
    let shard_to_split = four[1];
    e.sprigctl(&[
        "shard",
        "split",
        &e.tenant,
        &group.to_string(),
        &shard_to_split.to_string(),
        "--yes",
    ]);
    let five = shard_count(&e, group, 5).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::Relaxed);
    let (n, bad) = reader.await.unwrap();
    reads.fetch_add(n, Ordering::Relaxed);
    assert!(
        bad.is_empty(),
        "wrong point reads during the split: {bad:?}"
    );
    let f = shards_per_read(&e, &mut c, &ids[..100]).await;
    assert!(f <= 1.01, "after the split: {f}");
    check_consistent(&e, &mut c, "after the split").await;
    println!(
        "[{:?}] split shard {shard_to_split}: shards {five:?}; {n} point reads right during it, {f:.2} shards per read after",
        t0.elapsed()
    );

    // ---- 5. kill -9 during the back-fill ---------------------------------
    c.query_drop("ALTER TABLE customers DROP INDEX uk_email")
        .await
        .unwrap();
    assert_eq!(state_of(&mut c).await, None, "the lookup is gone");
    for b in 0..40 {
        let vals: Vec<String> = (200_001 + b * 1000..200_001 + b * 1000 + 1000)
            .map(|i| format!("({i}, 'bulk{i}@x.org', 'b')"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO customers (id, email, name) VALUES {}",
            vals.join(", ")
        ))
        .await
        .unwrap();
    }
    let pid: i32 = std::fs::read_to_string(&e.pidfile)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let ddl = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut c = pool.get_conn().await.unwrap();
            c.query_drop("ALTER TABLE customers ADD UNIQUE KEY uk_email (email)")
                .await
        })
    };
    // Kill as soon as the back-fill runs.
    let mut cat = e.root(0).await;
    let started = Instant::now();
    loop {
        let st: Option<String> = cat
            .query_first(format!(
                "SELECT state FROM t{}_sprig.lookup_indexes WHERE index_name = 'uk_email'",
                e.tenant_id
            ))
            .await
            .unwrap();
        if st.as_deref() == Some("backfilling") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "no back-fill: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let killed_at = Instant::now();
    let st = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(st.success());
    assert!(ddl.await.unwrap().is_err(), "the client lost its gateway");
    println!(
        "[{:?}] killed the gateway (pid {pid}) during the back-fill",
        t0.elapsed()
    );
    let st = std::process::Command::new("sh")
        .args(["-c", &e.restart])
        .status()
        .unwrap();
    assert!(st.success(), "restart");
    drop(pool);
    let pool = e.app();
    let started = Instant::now();
    let mut c = loop {
        if let Ok(c) = pool.get_conn().await {
            break c;
        }
        assert!(started.elapsed() < Duration::from_secs(120));
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    // Writes go on while the controller takes the build over.
    let stop = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicI64::new(300_001));
    let ws = writers(&pool, &stop, &next, 2);
    let started = Instant::now();
    loop {
        let st = state_of(&mut c).await;
        if st.as_deref() == Some("public") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "the stopped build was never taken over: {st:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let resumed = killed_at.elapsed();
    let (ok, err) = stop_writers(&stop, ws).await;
    println!(
        "[{:?}] the controller finished the build {resumed:?} after the kill ({ok} writes meanwhile, {err} failed)",
        t0.elapsed()
    );
    check_consistent(&e, &mut c, "after kill -9 and the take-over").await;
    let ids2: Vec<i64> = c
        .query("SELECT id FROM customers WHERE email LIKE 'bulk%' ORDER BY id LIMIT 100")
        .await
        .unwrap();
    for id in &ids2 {
        let r: Option<i64> = c
            .exec_first(
                "SELECT id FROM customers WHERE email = ?",
                (format!("bulk{id}@x.org"),),
            )
            .await
            .unwrap();
        assert_eq!(r, Some(*id));
    }
    // A rebuild removes the orphans the kill and the failed writes left.
    c.query_drop("SPRIG REBUILD LOOKUP INDEX uk_email ON customers")
        .await
        .unwrap();
    let orphans = check_consistent(&e, &mut c, "after SPRIG REBUILD").await;
    assert_eq!(orphans, 0);
    let names: BTreeSet<String> = lookup_rows(&e).await.into_keys().collect();
    assert_eq!(names.len(), owners(&mut c).await.len());
    println!(
        "[{:?}] done; {} reads during the split",
        t0.elapsed(),
        reads.load(Ordering::Relaxed)
    );
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
}

// ---- the follow-ups -----------------------------------------------------

const DB2: &str = "lk2_db";
const DB3: &str = "lk2_db2";

/// A session on `addr` as `<tenant>.<user>`, in `db` when given.
async fn login(
    addr: &str,
    tenant: &str,
    user: &str,
    pw: &str,
    db: Option<&str>,
) -> mysql_async::Result<Conn> {
    let (h, p) = addr.split_once(':').unwrap();
    Conn::new(
        OptsBuilder::default()
            .ip_or_hostname(h)
            .tcp_port(p.parse().unwrap())
            .user(Some(format!("{tenant}.{user}")))
            .pass(Some(pw))
            .ssl_opts(tls())
            .db_name(db),
    )
    .await
}

/// A counter or histogram's total (`_count` for histograms) in `addr`'s metrics.
fn metric(addr: &str, name: &str) -> f64 {
    let mut s = std::net::TcpStream::connect(addr).expect("metrics");
    write!(
        s,
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).unwrap();
    body.lines()
        .filter(|l| l.starts_with(name) && !l.starts_with('#'))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// Shards per run of `sql` (one row expected) over `n` runs, from gateway 1's metrics.
async fn fanout_of(e: &Env, c: &mut Conn, sql: &str, n: usize) -> f64 {
    let (s0, n0) = fanout(&e.metrics);
    for _ in 0..n {
        let r: Vec<Row> = c.query(sql).await.unwrap();
        assert_eq!(r.len(), 1, "{sql}");
    }
    let (s1, n1) = fanout(&e.metrics);
    (s1 - s0) / (n1 - n0).max(1.0)
}

/// Lookup tables named `name` on every cluster, and their rows.
async fn lookup_tables(e: &Env, name: &str) -> (usize, u64) {
    let (mut tables, mut rows) = (0, 0);
    for i in 0..e.clusters.len() {
        let mut r = e.root(i).await;
        let schemas: Vec<String> = r
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ? \
                 AND TABLE_SCHEMA LIKE ?",
                (name, format!("t{}\\_d%", e.tenant_id)),
            )
            .await
            .unwrap();
        for s in schemas {
            tables += 1;
            let n: Option<u64> = r
                .query_first(format!("SELECT COUNT(*) FROM `{s}`.`{name}`"))
                .await
                .unwrap();
            rows += n.unwrap_or(0);
        }
        r.disconnect().await.unwrap();
    }
    (tables, rows)
}

/// `lookup_indexes` rows of the tenant named `name`.
async fn catalog_lookups(e: &Env, name: &str) -> Vec<(String, String)> {
    let mut cat = e.root(0).await;
    let v = cat
        .exec(
            format!(
                "SELECT index_name, state FROM t{}_sprig.lookup_indexes WHERE index_name = ?",
                e.tenant_id
            ),
            (name,),
        )
        .await
        .unwrap();
    cat.disconnect().await.unwrap();
    v
}

async fn wait_state(c: &mut Conn, table: &str, want: Option<&str>, secs: u64) {
    let started = Instant::now();
    loop {
        let rows: Vec<Row> = c
            .query(format!("SPRIG SHOW LOOKUP INDEXES FROM {table}"))
            .await
            .unwrap_or_default();
        let st = rows.first().and_then(|r| r.get::<String, _>(6));
        if st.as_deref() == want {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(secs),
            "{table}: {st:?}, want {want:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn signal(pidfile: &str, sig: &str) {
    let pid = std::fs::read_to_string(pidfile).unwrap().trim().to_owned();
    let st = std::process::Command::new("kill")
        .args([sig, &pid])
        .status()
        .unwrap();
    assert!(st.success(), "kill {sig} {pid}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lookup_followups_live() {
    let (Some(e), Some(gw2), Some(gw2_pid)) = (
        Env::load(),
        var("SPRIG_TEST_LKD_GW2"),
        var("SPRIG_TEST_LKD_GW2_PIDFILE"),
    ) else {
        eprintln!(
            "skipped: set the SPRIG_TEST_LKD_* variables and _GW2, _GW2_PIDFILE (module docs)"
        );
        return;
    };
    let t0 = Instant::now();
    let app = |db: Option<&'static str>| login(&e.gateway, &e.tenant, "app", "sprig-dev-app", db);
    // ---- set-up: five tables in two databases, grouped, 4 shards ---------
    {
        let mut c = app(None).await.unwrap();
        for d in [DB2, DB3] {
            c.query_drop(format!("DROP DATABASE IF EXISTS {d}"))
                .await
                .unwrap();
            c.query_drop(format!("CREATE DATABASE {d}")).await.unwrap();
        }
        c.query_drop(format!("USE {DB2}")).await.unwrap();
        for ddl in [
            "CREATE TABLE members (id BIGINT PRIMARY KEY, email VARCHAR(100), name VARCHAR(40))",
            "CREATE TABLE accounts (id BIGINT PRIMARY KEY, region INT, handle VARCHAR(40), note VARCHAR(40))",
            "CREATE TABLE dups (id BIGINT PRIMARY KEY, code VARCHAR(40))",
            "CREATE TABLE t7 (id BIGINT PRIMARY KEY, code VARCHAR(40))",
        ] {
            c.query_drop(ddl).await.unwrap();
        }
        let rows = |f: &dyn Fn(i64) -> String, n: i64| -> String {
            (1..=n).map(f).collect::<Vec<_>>().join(", ")
        };
        c.query_drop(format!(
            "INSERT INTO members VALUES {}",
            rows(&|i| format!("({i}, 'm{i}@x.org', 'n{i}')"), 2000)
        ))
        .await
        .unwrap();
        // (region, handle) unique; each handle in 5 regions.
        c.query_drop(format!(
            "INSERT INTO accounts VALUES {}",
            rows(&|i| format!("({i}, {}, 'h{}', 'a')", i % 5, i / 5), 2000)
        ))
        .await
        .unwrap();
        // Unique codes for now; 12 of them repeat across shards once split.
        c.query_drop(format!(
            "INSERT INTO dups VALUES {}",
            rows(&|i| format!("({i}, 'u{i}')"), 1000)
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO t7 VALUES {}",
            rows(&|i| format!("({i}, 'k{i}')"), 500)
        ))
        .await
        .unwrap();
        for t in ["members", "accounts", "dups", "t7"] {
            c.query_drop(format!("SPRIG PIN {t} SHARD KEY (id)"))
                .await
                .unwrap();
        }
        c.query_drop(format!("USE {DB3}")).await.unwrap();
        c.query_drop("CREATE TABLE items (id BIGINT PRIMARY KEY, sku VARCHAR(40))")
            .await
            .unwrap();
        c.query_drop(format!(
            "INSERT INTO items VALUES {}",
            rows(&|i| format!("({i}, 's{i}')"), 500)
        ))
        .await
        .unwrap();
        c.query_drop("SPRIG PIN items SHARD KEY (id)")
            .await
            .unwrap();
        c.disconnect().await.unwrap();
    }
    e.sprigctl(&["advisor", "run", &e.tenant, "-o", "json", "--yes"]);
    let mut cat = e.root(0).await;
    let run: Option<u64> = cat
        .query_first(format!(
            "SELECT MAX(run_id) FROM t{}_sprig.advisor_runs",
            e.tenant_id
        ))
        .await
        .unwrap();
    e.sprigctl(&[
        "advisor",
        "apply",
        &e.tenant,
        &run.unwrap().to_string(),
        "-o",
        "json",
        "--yes",
    ]);
    let started = Instant::now();
    let groups: Vec<u32> = loop {
        let g: Vec<u32> = cat
            .query(format!(
                "SELECT DISTINCT m.group_id FROM t{0}_sprig.group_members m JOIN t{0}_sprig.objects o \
                 ON o.object_id = m.object_id WHERE o.name IN ('members','accounts','dups','t7','items') \
                 AND o.state = 'active'",
                e.tenant_id
            ))
            .await
            .unwrap();
        let members: Option<u64> = cat
            .query_first(format!(
                "SELECT COUNT(*) FROM t{0}_sprig.group_members m JOIN t{0}_sprig.objects o \
                 ON o.object_id = m.object_id WHERE o.name IN ('members','accounts','dups','t7','items') \
                 AND o.state = 'active'",
                e.tenant_id
            ))
            .await
            .unwrap();
        if members == Some(5) {
            break g;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "never grouped: {g:?}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    for g in &groups {
        let first = shard_count(&e, *g, 1).await;
        if first.len() < 4 {
            e.sprigctl(&[
                "shard",
                "split",
                &e.tenant,
                &g.to_string(),
                &first[0].to_string(),
                "--ways",
                "4",
                "--yes",
            ]);
        }
        shard_count(&e, *g, 4).await;
    }
    let mut c = app(Some(DB2)).await.unwrap();
    let started = Instant::now();
    loop {
        let f = fanout_of(
            &e,
            &mut c,
            "SELECT id FROM members WHERE email = 'm7@x.org'",
            5,
        )
        .await;
        if f >= 3.99 {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(120), "fan-out {f}");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!("[{:?}] groups {groups:?} in 4 shards", t0.elapsed());
    // Codes c1..c12, each on two rows of two different shards (one shard's
    // own unique index would refuse a pair inside it first).
    let mut per_shard: Vec<Vec<i64>> = Vec::new();
    for i in 0..e.clusters.len() {
        let mut r = e.root(i).await;
        let schemas: Vec<String> = r
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = 'dups' \
                 AND TABLE_SCHEMA LIKE ?",
                (format!("t{}\\_d%", e.tenant_id),),
            )
            .await
            .unwrap();
        for sc in schemas {
            let ids: Vec<i64> = r
                .query(format!("SELECT id FROM `{sc}`.dups ORDER BY id LIMIT 12"))
                .await
                .unwrap();
            if ids.len() == 12 {
                per_shard.push(ids);
            }
        }
        r.disconnect().await.unwrap();
    }
    assert!(per_shard.len() >= 2, "dups in two shards: {per_shard:?}");
    let dup_b = per_shard[1].clone();
    for k in 0..12 {
        c.query_drop(format!(
            "UPDATE dups SET code = 'c{}' WHERE id IN ({}, {})",
            k + 1,
            per_shard[0][k],
            dup_b[k]
        ))
        .await
        .unwrap();
    }

    // ---- 1. prepared SPRIG SHOW LOOKUP INDEXES ---------------------------
    c.query_drop("ALTER TABLE members ADD UNIQUE KEY uk_email (email)")
        .await
        .unwrap();
    let prepared: Vec<Row> = c
        .exec("SPRIG SHOW LOOKUP INDEXES FROM members", ())
        .await
        .unwrap();
    assert_eq!(prepared.len(), 1);
    assert_eq!(prepared[0].get::<String, _>(2).as_deref(), Some("uk_email"));
    assert_eq!(prepared[0].get::<String, _>(6).as_deref(), Some("public"));
    let all: Vec<Row> = c.exec("SPRIG SHOW LOOKUP INDEXES", ()).await.unwrap();
    assert!(!all.is_empty());
    c.query_drop("PREPARE s FROM 'SPRIG SHOW LOOKUP INDEXES FROM members'")
        .await
        .unwrap();
    let viasql: Vec<Row> = c.query("EXECUTE s").await.unwrap();
    assert_eq!(viasql.len(), 1);
    assert_eq!(viasql[0].get::<String, _>(2).as_deref(), Some("uk_email"));
    c.query_drop("DEALLOCATE PREPARE s").await.unwrap();
    let f = fanout_of(
        &e,
        &mut c,
        "SELECT id FROM members WHERE email = 'm9@x.org'",
        20,
    )
    .await;
    assert!(f <= 1.01, "{f}");
    println!(
        "[{:?}] 1. prepared SHOW: COM_STMT_PREPARE and SQL PREPARE answer",
        t0.elapsed()
    );

    // ---- 2. a unique key of two columns ----------------------------------
    let before = fanout_of(
        &e,
        &mut c,
        "SELECT id FROM accounts WHERE region = 3 AND handle = 'h17'",
        10,
    )
    .await;
    let t = Instant::now();
    c.query_drop("ALTER TABLE accounts ADD UNIQUE KEY uk_rh (region, handle)")
        .await
        .unwrap();
    let took = t.elapsed();
    wait_state(&mut c, "accounts", Some("public"), 10).await;
    let after = fanout_of(
        &e,
        &mut c,
        "SELECT id FROM accounts WHERE region = 3 AND handle = 'h17'",
        20,
    )
    .await;
    let tuple = fanout_of(
        &e,
        &mut c,
        "SELECT id FROM accounts WHERE (handle, region) IN (('h17', 3))",
        10,
    )
    .await;
    let one_part = fanout_of(
        &e,
        &mut c,
        "SELECT id FROM accounts WHERE region = 3 AND id = 88",
        1,
    )
    .await;
    println!(
        "2. ADD UNIQUE (region, handle) took {took:?}; reads visit {before:.2} -> {after:.2} shards ({tuple:.2} by tuple)"
    );
    assert!(
        after <= 1.01 && tuple <= 1.01 && before >= 3.99,
        "{before} {after} {tuple} {one_part}"
    );
    let rows: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM accounts")
        .await
        .unwrap();
    let (tables, lk) = lookup_tables(&e, "__sprig_lk_accounts__uk_rh").await;
    assert_eq!(
        Some(lk),
        rows,
        "one lookup row per row ({tables} lookup tables)"
    );
    let dup = c
        .query_drop("INSERT INTO accounts VALUES (90001, 3, 'h17', 'dup')")
        .await;
    assert_eq!(errno_of(dup), Some(1062), "a duplicate pair");
    c.query_drop("INSERT INTO accounts VALUES (90002, 9, 'h17', 'new region')")
        .await
        .unwrap();
    // Row 18 is (3, 'h3'); moving it to region 2 collides with row 17 (2, 'h3').
    let dup = c
        .query_drop("UPDATE accounts SET region = 2 WHERE id = 18")
        .await;
    assert_eq!(
        errno_of(dup),
        Some(1062),
        "an update of one part to a taken pair"
    );
    c.query_drop("UPDATE accounts SET region = 7 WHERE id = 18")
        .await
        .unwrap();
    let r: Option<i64> = c
        .query_first("SELECT id FROM accounts WHERE region = 7 AND handle = 'h3'")
        .await
        .unwrap();
    assert_eq!(r, Some(18));
    let r: Option<i64> = c
        .query_first("SELECT id FROM accounts WHERE region = 3 AND handle = 'h3'")
        .await
        .unwrap();
    assert_eq!(r, None, "the old pair is free");
    c.query_drop("INSERT INTO accounts VALUES (90003, 3, 'h3', 'reuse')")
        .await
        .unwrap();
    c.query_drop("DELETE FROM accounts WHERE id IN (90002, 90003)")
        .await
        .unwrap();
    let rows: Option<u64> = c
        .query_first("SELECT COUNT(*) FROM accounts")
        .await
        .unwrap();
    let (_, lk) = lookup_tables(&e, "__sprig_lk_accounts__uk_rh").await;
    assert_eq!(
        Some(lk),
        rows,
        "lookup rows follow inserts, updates and deletes"
    );
    println!(
        "[{:?}] 2. two-column lookup: built, routed, unique, maintained",
        t0.elapsed()
    );

    // ---- 3. duplicates: 1062 and an approvals item ----------------------
    // Raised through the controller, the item notifies: its hook counts
    // every channel it tries (email is `skipped` without SMTP).
    let ctl_metrics = var("SPRIG_TEST_LKD_CTL_METRICS");
    let notified = |addr: &str| metric(addr, "sprig_approvals_notifications_total");
    let notified0 = ctl_metrics.as_deref().map(notified);
    let r = c
        .query_drop("ALTER TABLE dups ADD UNIQUE KEY uk_code (code)")
        .await;
    let msg = match &r {
        Err(mysql_async::Error::Server(s)) => {
            assert_eq!(s.code, 1062, "{s:?}");
            s.message.clone()
        }
        other => panic!("ADD UNIQUE over duplicates: {other:?}"),
    };
    println!("3. ADD UNIQUE over duplicates: {msg}");
    assert!(msg.contains("11 more values"), "{msg}");
    wait_state(&mut c, "dups", None, 5).await;
    let idx: Vec<Row> = c.query("SHOW INDEX FROM dups").await.unwrap();
    assert!(
        !idx.iter()
            .any(|r| r.get::<String, _>(2).as_deref() == Some("uk_code")),
        "the shards' key is undone"
    );
    let recs: Vec<Row> = c.query("SHOW SPRIG RECOMMENDATIONS").await.unwrap();
    let item = recs
        .iter()
        .find(|r| {
            r.get::<String, _>(1).as_deref() == Some("advisor.lookup")
                && r.get::<String, _>(5).is_some_and(|s| s.contains("uk_code"))
        })
        .unwrap_or_else(|| panic!("no duplicates item: {recs:?}"));
    let summary: String = item.get(5).unwrap();
    let action: String = item.get(6).unwrap();
    println!("   item: {summary} / {action}");
    assert!(summary.contains("12 values repeat in 12 rows"), "{summary}");
    let sampled = (1..=12)
        .filter(|i| summary.contains(&format!("'c{i}'")))
        .count();
    assert_eq!(sampled, 10, "{summary}");
    assert!(
        action.starts_with("ALTER TABLE `lk2_db`.`dups` ADD UNIQUE KEY `uk_code`"),
        "{action}"
    );
    if let (Some(addr), Some(n0)) = (ctl_metrics.as_deref(), notified0) {
        let mut n = notified(addr);
        for _ in 0..20 {
            if n > n0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
            n = notified(addr);
        }
        println!("   notifications tried by the controller: {n0} -> {n}");
        assert!(n > n0, "the gateway's duplicates item didn't notify");
    }
    // Again, still duplicated: the same item is refreshed, not a second one.
    let _ = c
        .query_drop("ALTER TABLE dups ADD UNIQUE KEY uk_code (code)")
        .await;
    let recs: Vec<Row> = c.query("SHOW SPRIG RECOMMENDATIONS").await.unwrap();
    let n = recs
        .iter()
        .filter(|r| r.get::<String, _>(5).is_some_and(|s| s.contains("uk_code")))
        .count();
    assert_eq!(n, 1);
    // Fixed: the statement succeeds.
    c.query_drop(format!(
        "DELETE FROM dups WHERE id IN ({})",
        dup_b
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .await
    .unwrap();
    c.query_drop("ALTER TABLE dups ADD UNIQUE KEY uk_code (code)")
        .await
        .unwrap();
    wait_state(&mut c, "dups", Some("public"), 10).await;
    println!(
        "[{:?}] 3. duplicates reported (10 of 12 values), fixed, built",
        t0.elapsed()
    );

    // ---- 4. table-level grants -------------------------------------------
    for sql in [
        "DROP USER IF EXISTS tbl, ro",
        "CREATE USER tbl IDENTIFIED BY 'pw-tbl-1'",
        "CREATE USER ro IDENTIFIED BY 'pw-ro-1'",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON lk2_db.members TO tbl",
        "GRANT SELECT ON lk2_db.accounts TO ro",
    ] {
        c.query_drop(sql)
            .await
            .unwrap_or_else(|er| panic!("{sql}: {er}"));
    }
    let mut u = login(&e.gateway, &e.tenant, "tbl", "pw-tbl-1", Some(DB2))
        .await
        .unwrap();
    // The reconcile runs on the session's first statement; give it a moment.
    let started = Instant::now();
    loop {
        match u
            .query_drop("INSERT INTO members VALUES (5001, 'tbl1@x.org', 't')")
            .await
        {
            Ok(()) => break,
            Err(er) => {
                assert!(
                    started.elapsed() < Duration::from_secs(30),
                    "table-level insert: {er}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    let dup = u
        .query_drop("INSERT INTO members VALUES (5002, 'TBL1@x.org', 't')")
        .await;
    assert_eq!(errno_of(dup), Some(1062), "a table-level user's duplicate");
    u.query_drop("UPDATE members SET email = 'tbl2@x.org' WHERE id = 5001")
        .await
        .unwrap();
    let r: Option<i64> = u
        .query_first("SELECT id FROM members WHERE email = 'tbl2@x.org'")
        .await
        .unwrap();
    assert_eq!(r, Some(5001));
    let f = fanout_of(
        &e,
        &mut u,
        "SELECT id FROM members WHERE email = 'm11@x.org'",
        10,
    )
    .await;
    assert!(f <= 1.01, "{f}");
    u.query_drop("DELETE FROM members WHERE id = 5001")
        .await
        .unwrap();
    let denied = u.query_drop("SELECT * FROM accounts LIMIT 1").await;
    assert_eq!(errno_of(denied), Some(1142));
    let mut ro = login(&e.gateway, &e.tenant, "ro", "pw-ro-1", Some(DB2))
        .await
        .unwrap();
    let r: Option<i64> = ro
        .query_first("SELECT id FROM accounts WHERE region = 1 AND handle = 'h2'")
        .await
        .unwrap();
    assert_eq!(r, Some(11));
    assert_eq!(
        errno_of(ro.query_drop("SELECT * FROM members LIMIT 1").await),
        Some(1142)
    );
    // MariaDB's grants on lookup tables, per backend account.
    let mut root = e.root(0).await;
    let tp: Vec<(String, String, String)> = root
        .exec(
            "SELECT User, Table_name, Table_priv FROM mysql.tables_priv WHERE User LIKE ? \
             AND Table_name LIKE '\\_\\_sprig\\_lk\\_%' ORDER BY User, Table_name",
            (format!("t{}\\_u%", e.tenant_id),),
        )
        .await
        .unwrap();
    println!("4. lookup-table grants: {tp:?}");
    let by: BTreeMap<String, BTreeSet<String>> =
        tp.iter().fold(BTreeMap::new(), |mut m, (u, t, _)| {
            m.entry(u.clone()).or_default().insert(t.clone());
            m
        });
    assert_eq!(by.len(), 2, "two table-level accounts: {by:?}");
    let tables: BTreeSet<BTreeSet<String>> = by.values().cloned().collect();
    assert!(
        tables.contains(&BTreeSet::from(["__sprig_lk_members__uk_email".to_owned()])),
        "{by:?}"
    );
    assert!(
        tables.contains(&BTreeSet::from(["__sprig_lk_accounts__uk_rh".to_owned()])),
        "{by:?}"
    );
    assert!(
        tp.iter().all(|(_, t, p)| if t.contains("accounts") {
            p == "Select"
        } else {
            p.contains("Insert") && p.contains("Delete")
        }),
        "{tp:?}"
    );
    root.disconnect().await.unwrap();
    u.disconnect().await.unwrap();
    ro.disconnect().await.unwrap();
    println!(
        "[{:?}] 4. table-level grants reach their lookups only",
        t0.elapsed()
    );

    // ---- 5. DROP TABLE and DROP DATABASE ---------------------------------
    c.query_drop("DROP TABLE members").await.unwrap();
    assert!(
        catalog_lookups(&e, "uk_email").await.is_empty(),
        "the lookup row went with its table"
    );
    assert_eq!(lookup_tables(&e, "__sprig_lk_members__uk_email").await.0, 0);
    {
        let mut d = app(Some(DB3)).await.unwrap();
        d.query_drop("ALTER TABLE items ADD UNIQUE KEY uk_sku (sku)")
            .await
            .unwrap();
        assert!(lookup_tables(&e, "__sprig_lk_items__uk_sku").await.0 > 0);
        d.query_drop(format!("DROP DATABASE {DB3}")).await.unwrap();
        d.disconnect().await.unwrap();
    }
    assert!(catalog_lookups(&e, "uk_sku").await.is_empty());
    assert_eq!(lookup_tables(&e, "__sprig_lk_items__uk_sku").await.0, 0);
    println!(
        "[{:?}] 5. DROP TABLE and DROP DATABASE drop their lookups",
        t0.elapsed()
    );

    // ---- 6. acknowledgements, a paused gateway, a crash mid-drop ---------
    e.sprigctl(&[
        "settings",
        "set",
        "lookup.publish_timeout_ms",
        "3000",
        "--scope",
        &format!("tenant:{}", e.tenant),
        "--reason",
        "lookup follow-ups test",
    ]);
    // Both up: a build's publishes are acknowledged at once.
    let mut c2 = login(&gw2, &e.tenant, "app", "sprig-dev-app", Some(DB2))
        .await
        .unwrap();
    let _: Vec<Row> = c2.query("SELECT * FROM t7 WHERE id = 1").await.unwrap();
    let pubs0 = metric(&e.metrics, "sprig_lookup_publish_seconds_count");
    let t = Instant::now();
    c.query_drop("ALTER TABLE t7 ADD UNIQUE KEY uk_k (code)")
        .await
        .unwrap();
    let both_up = t.elapsed();
    let pubs = metric(&e.metrics, "sprig_lookup_publish_seconds_count") - pubs0;
    c.query_drop("ALTER TABLE t7 DROP INDEX uk_k")
        .await
        .unwrap();
    println!("6. ADD UNIQUE with both gateways up: {both_up:?} ({pubs} publishes acknowledged)");
    assert!(pubs >= 2.0, "{pubs}");
    // gw-2's session holds a snapshot without the lookup; pause gw-2.
    c2.query_drop("INSERT INTO t7 VALUES (601, 'k601')")
        .await
        .unwrap();
    signal(&gw2_pid, "-STOP");
    let fenced0 = metric(&e.metrics, "sprig_lookup_publish_fenced_total");
    let t = Instant::now();
    c.query_drop("ALTER TABLE t7 ADD UNIQUE KEY uk_k (code)")
        .await
        .unwrap();
    let paused = t.elapsed();
    let fenced = metric(&e.metrics, "sprig_lookup_publish_fenced_total") - fenced0;
    println!("   ... with gw-2 paused: {paused:?}, {fenced} gateway(s) fenced");
    assert!(
        paused >= Duration::from_secs(3),
        "the build waited for gw-2"
    );
    assert!(fenced >= 1.0, "gw-2 was fenced");
    c.query_drop("INSERT INTO t7 VALUES (602, 'fresh1')")
        .await
        .unwrap();
    signal(&gw2_pid, "-CONT");
    // gw-2's old session: its first statement reloads, so it claims.
    let dup = c2.query_drop("INSERT INTO t7 VALUES (603, 'FRESH1')").await;
    assert_eq!(
        errno_of(dup),
        Some(1062),
        "the resumed gateway enforces the new lookup"
    );
    c2.query_drop("INSERT INTO t7 VALUES (604, 'fresh2')")
        .await
        .unwrap();
    let (_, lk) = lookup_tables(&e, "__sprig_lk_t7__uk_k").await;
    let rows: Option<u64> = c.query_first("SELECT COUNT(*) FROM t7").await.unwrap();
    assert_eq!(Some(lk), rows, "gw-2's insert claimed its lookup row");
    c2.disconnect().await.unwrap();
    println!(
        "[{:?}] 6. paused gateway fenced; resumed, it reloaded and claims",
        t0.elapsed()
    );
    // A crash while DROP TABLE's lookup is `dropping`: pause gw-2 so the
    // publish waits, kill -9 gw-1 then, and let the controller finish.
    tokio::time::sleep(Duration::from_secs(12)).await; // gw-2 re-registers
    signal(&gw2_pid, "-STOP");
    let drop_task = {
        let gw = e.gateway.clone();
        let tenant = e.tenant.clone();
        tokio::spawn(async move {
            let mut d = login(&gw, &tenant, "app", "sprig-dev-app", Some(DB2))
                .await
                .unwrap();
            d.query_drop("DROP TABLE t7").await
        })
    };
    let started = Instant::now();
    loop {
        let st = catalog_lookups(&e, "uk_k").await;
        if st.iter().any(|(_, s)| s == "dropping") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "never dropping: {st:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    signal(&e.pidfile, "-9");
    let _ = drop_task.await;
    signal(&gw2_pid, "-CONT");
    let killed = Instant::now();
    let st = std::process::Command::new("sh")
        .args(["-c", &e.restart])
        .status()
        .unwrap();
    assert!(st.success(), "restart");
    loop {
        if catalog_lookups(&e, "uk_k").await.is_empty() {
            break;
        }
        assert!(
            killed.elapsed() < Duration::from_secs(300),
            "the stopped drop was never finished"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(lookup_tables(&e, "__sprig_lk_t7__uk_k").await.0, 0);
    println!(
        "[{:?}] 6. the controller finished the drop {:?} after the kill",
        t0.elapsed(),
        killed.elapsed()
    );
    e.sprigctl(&[
        "settings",
        "reset",
        "lookup.publish_timeout_ms",
        "--scope",
        &format!("tenant:{}", e.tenant),
        "--reason",
        "lookup follow-ups test",
    ]);
    let started = Instant::now();
    let mut c = loop {
        if let Ok(c) = app(None).await {
            break c;
        }
        assert!(started.elapsed() < Duration::from_secs(120));
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    lock_wait_and_orphan_scan(&e, ctl_metrics.as_deref()).await;
    c.query_drop(format!("DROP DATABASE {DB2}")).await.unwrap();
    println!("[{:?}] done", t0.elapsed());
}

/// How long `waiter` on `b` waited before its 1205, while `holder` on `a`
/// held the same value's claim in an open transaction.
async fn blocked_claim(a: &mut Conn, b: &mut Conn, holder: &str, waiter: &str) -> Duration {
    a.query_drop("BEGIN").await.unwrap();
    a.query_drop(holder).await.unwrap();
    let started = Instant::now();
    let r = b.query_drop(waiter).await;
    let waited = started.elapsed();
    a.query_drop("ROLLBACK").await.unwrap();
    assert_eq!(
        errno_of(r),
        Some(1205),
        "{waiter}: a claim blocked by another"
    );
    waited
}

/// `lookup_followups_live`'s step 7:
///
/// - `lookup.lock_wait_seconds` at a table's scope: 1 s on `dups` with the
///   tenant at 6 s. A claim blocked by another session's uncommitted claim
///   of the same value fails with 1205 after about 1 s on `dups`, and after
///   about 6 s on `accounts` (its two-column key).
/// - The controller's scheduled orphan scan (`lookup.orphan_scan_hours` 1):
///   orphans planted in `uk_code`'s lookup table, with its last scan moved
///   two hours back, are removed at a sharding tick, and `last_scan_at`
///   moves to now.
async fn lock_wait_and_orphan_scan(e: &Env, ctl_metrics: Option<&str>) {
    let t0 = Instant::now();
    let set = |key: &str, value: &str, scope: &str| {
        e.sprigctl(&[
            "settings",
            "set",
            key,
            value,
            "--scope",
            scope,
            "--reason",
            "lookup lock wait test",
        ]);
    };
    let reset = |key: &str, scope: &str| {
        e.sprigctl(&[
            "settings",
            "reset",
            key,
            "--scope",
            scope,
            "--reason",
            "lookup lock wait test",
        ]);
    };
    let tenant_scope = format!("tenant:{}", e.tenant);
    let dups_scope = format!("object:{}.{DB2}.dups", e.tenant);
    set("lookup.lock_wait_seconds", "6", &tenant_scope);
    set("lookup.lock_wait_seconds", "1", &dups_scope);
    // The gateway reads the rows with its next catalog snapshot.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let app = || login(&e.gateway, &e.tenant, "app", "sprig-dev-app", Some(DB2));
    let mut a = app().await.unwrap();
    let mut b = app().await.unwrap();
    let on_dups = blocked_claim(
        &mut a,
        &mut b,
        "INSERT INTO dups VALUES (9001, 'lw1')",
        "INSERT INTO dups VALUES (9002, 'lw1')",
    )
    .await;
    let on_accounts = blocked_claim(
        &mut a,
        &mut b,
        "INSERT INTO accounts VALUES (9001, 1, 'lwx', 'a')",
        "INSERT INTO accounts VALUES (9002, 1, 'LWX', 'b')",
    )
    .await;
    println!(
        "7. a blocked claim waited {on_dups:?} on dups (table: 1 s), {on_accounts:?} on accounts (tenant: 6 s)"
    );
    assert!(
        on_dups >= Duration::from_millis(800) && on_dups < Duration::from_secs(4),
        "{on_dups:?}"
    );
    assert!(
        on_accounts >= Duration::from_secs(5) && on_accounts < Duration::from_secs(12),
        "{on_accounts:?}"
    );
    reset("lookup.lock_wait_seconds", &dups_scope);
    reset("lookup.lock_wait_seconds", &tenant_scope);

    // ---- the scheduled orphan scan ----
    set("lookup.orphan_scan_hours", "1", &tenant_scope);
    let name = "__sprig_lk_dups__uk_code";
    let (_, before) = lookup_tables(e, name).await;
    let mut planted = 0;
    for i in 0..e.clusters.len() {
        let mut r = e.root(i).await;
        let schemas: Vec<String> = r
            .exec(
                "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ? \
                 AND TABLE_SCHEMA LIKE ?",
                (name, format!("t{}\\_d%", e.tenant_id)),
            )
            .await
            .unwrap();
        for s in schemas {
            planted += 1;
            r.query_drop(format!(
                "INSERT INTO `{s}`.`{name}` (code, owner_id, owner_rk) \
                 VALUES ('orphan-{planted}', {}, {})",
                990_000 + planted,
                planted * 7919
            ))
            .await
            .unwrap();
        }
        r.disconnect().await.unwrap();
    }
    assert!(planted > 0);
    let removed0 = ctl_metrics.map(|m| metric(m, "sprig_lookup_orphans_removed_total"));
    let mut cat = e.root(0).await;
    cat.query_drop(format!(
        "UPDATE t{}_sprig.lookup_indexes SET last_scan_at = UTC_TIMESTAMP(6) - INTERVAL 2 HOUR \
         WHERE index_name = 'uk_code'",
        e.tenant_id
    ))
    .await
    .unwrap();
    let started = Instant::now();
    loop {
        let (_, rows) = lookup_tables(e, name).await;
        let recent: Option<i64> = cat
            .query_first(format!(
                "SELECT TIMESTAMPDIFF(SECOND, last_scan_at, UTC_TIMESTAMP(6)) \
                 FROM t{}_sprig.lookup_indexes WHERE index_name = 'uk_code'",
                e.tenant_id
            ))
            .await
            .unwrap();
        if rows == before && recent.is_some_and(|s| s < 600) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(240),
            "the orphan scan never ran: {rows} lookup rows (want {before}), last scan {recent:?} s ago"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "7. the controller's orphan scan removed {planted} planted orphans {:?} after they were due",
        started.elapsed()
    );
    if let (Some(m), Some(r0)) = (ctl_metrics, removed0) {
        // Counted when the scan ends, after its last delete.
        let mut r1 = metric(m, "sprig_lookup_orphans_removed_total");
        for _ in 0..30 {
            if r1 - r0 >= f64::from(planted) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            r1 = metric(m, "sprig_lookup_orphans_removed_total");
        }
        println!("   sprig_lookup_orphans_removed_total: {r0} -> {r1}");
        assert!(r1 - r0 >= f64::from(planted), "{r0} -> {r1}");
    }
    cat.disconnect().await.unwrap();
    reset("lookup.orphan_scan_hours", &tenant_scope);
    println!(
        "[{:?}] 7. per-table lock wait and the scheduled orphan scan",
        t0.elapsed()
    );
}
