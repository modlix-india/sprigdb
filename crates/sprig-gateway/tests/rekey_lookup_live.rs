//! An online re-key of a table group whose member owns a unique global
//! lookup index, under write load through the gateway, live on a private
//! deployment:
//!
//! 1. `orders (oid PK, store_id, customer_id, ref, amount)` pinned
//!    `SHARD KEY (store_id)`, made a group by the advisor and split into 3
//!    shards, then unpinned (the periodic run leaves pinned groups alone);
//!    then `ALTER TABLE orders ADD UNIQUE KEY uk_ref (ref)` builds
//!    the lookup index across the shards.
//! 2. Four writers, each on its own connection and its own `oid` range,
//!    insert orders with new `ref`s, insert orders with a `ref` another
//!    order holds (each must fail with 1062), change amounts, change `ref`s,
//!    delete their orders by `ref` (routed through the lookup) and read
//!    their orders back by `ref`. A read must return the order with its
//!    last acknowledged amount, at every moment.
//! 3. Before they start, statements by `customer_id` are recorded, and the
//!    tenant gets `advisor.mode = approve`, `advisor.rekey_sustain_runs = 1`,
//!    `advisor.rekey_cooldown_days = 0` and a short `advisor.interval_hours`.
//!    While they write, the controller's periodic run proposes `customer_id`
//!    and raises an `advisor.rekey` approvals item; the test approves it
//!    (`sprigctl recommendations approve`), which starts the re-key onto as
//!    many new shards. (`SPRIG_TEST_RKL_MANUAL=1` starts it with
//!    `sprigctl shard rekey` instead.) The test waits until the catalog has
//!    the new key, lets the writers go on for a few seconds, and stops them.
//! 4. Afterwards, read on the cluster itself: every order is on exactly one
//!    of the group's shards, with `_sprig_rk` the hash of its
//!    `customer_id`, inside that shard's range; every acknowledged insert,
//!    amount and `ref` is there and every acknowledged delete is gone; no
//!    `ref` is held twice; every order with a `ref` has exactly its lookup
//!    row, naming it and its new hash. Through the gateway, reads by `ref`
//!    find every sampled order (on one shard each, when the gateway's
//!    metrics address is given), and the lookup is `public`.
//!
//! ```sh
//! # a private deployment (controller, worker, gateway on your own etcd and MariaDB)
//! export SPRIGCTL_ENDPOINT=http://127.0.0.1:<admin port>
//! export SPRIG_TEST_RKL_GATEWAY=127.0.0.1:<mysql port> SPRIG_TEST_RKL_TENANT=<tenant> \
//!   SPRIG_TEST_RKL_TENANT_ID=<id> SPRIG_TEST_RKL_CLUSTER=127.0.0.1:<MariaDB port> \
//!   SPRIG_TEST_RKL_SPRIGCTL=<path to sprigctl> \
//!   SPRIG_TEST_RKL_METRICS=127.0.0.1:<gateway telemetry port>   # optional
//! cargo test -p sprig-gateway --test rekey_lookup_live -- --nocapture
//! ```
//!
//! The tenant must have no table groups yet. The cluster connection only
//! reads (shard contents, lookup rows, the catalog). Without
//! `SPRIG_TEST_RKL_GATEWAY` the test says what it needs and passes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::type_complexity,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value};
use sprig_shard::{KeyClass, KeySpec};

const DB: &str = "r3s_rekey_db";
const SEED: i64 = 4_000;
const WRITERS: i64 = 4;

fn tls() -> SslOpts {
    SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true)
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

struct Env {
    gateway: String,
    metrics: Option<String>,
    tenant: String,
    tenant_id: u64,
    cluster: String,
    sprigctl: String,
}

impl Env {
    fn load() -> Option<Self> {
        Some(Self {
            gateway: var("SPRIG_TEST_RKL_GATEWAY")?,
            metrics: var("SPRIG_TEST_RKL_METRICS"),
            tenant: var("SPRIG_TEST_RKL_TENANT")?,
            tenant_id: var("SPRIG_TEST_RKL_TENANT_ID")?.parse().ok()?,
            cluster: var("SPRIG_TEST_RKL_CLUSTER")?,
            sprigctl: var("SPRIG_TEST_RKL_SPRIGCTL")?,
        })
    }

    fn opts(&self, db: Option<&str>) -> OptsBuilder {
        let (h, p) = self.gateway.split_once(':').unwrap();
        OptsBuilder::default()
            .ip_or_hostname(h)
            .tcp_port(p.parse().unwrap())
            .user(Some(format!("{}.app", self.tenant)))
            .pass(Some("sprig-dev-app"))
            .ssl_opts(tls())
            .db_name(db)
    }

    async fn app(&self) -> Conn {
        Conn::new(self.opts(Some(DB))).await.unwrap()
    }

    /// A read-only look at the cluster (shard contents, the catalog).
    async fn look(&self) -> Conn {
        let (h, p) = self.cluster.split_once(':').unwrap();
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

    /// The group's current shards: (shard, lo, hi exclusive or `None` for
    /// 2^64, state), and the group's state.
    fn shards(&self, group: u32) -> (Vec<(u32, u64, Option<u64>, String)>, String) {
        let v: serde_json::Value =
            serde_json::from_str(&self.sprigctl(&["shard", "list", &self.tenant, "-o", "json"]))
                .unwrap_or_default();
        let hex = |s: &str| u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap();
        for g in v.as_array().into_iter().flatten() {
            if g["group"].as_u64() != Some(u64::from(group)) {
                continue;
            }
            let shards = g["shards"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|s| {
                    (
                        s["shard"].as_u64().unwrap() as u32,
                        hex(s["lo"].as_str().unwrap()),
                        s["hi"].as_str().map(hex),
                        s["state"].as_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            return (shards, g["state"].as_str().unwrap_or_default().to_owned());
        }
        (Vec::new(), String::new())
    }
}

fn errno<T>(r: &mysql_async::Result<T>) -> Option<u16> {
    match r {
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        _ => None,
    }
}

fn store_of(oid: i64) -> i64 {
    1 + oid % 50
}

fn customer_of(oid: i64) -> i64 {
    1 + (oid * 37) % 997
}

fn rk(customer: i64) -> u64 {
    let spec = KeySpec::new(vec![KeyClass::Numeric]).unwrap();
    sprig_shard::rk::hash_values(&spec, &[Value::Int(customer)]).unwrap()
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

/// The lookup's state in `SPRIG SHOW LOOKUP INDEXES`.
async fn lookup_state(c: &mut Conn) -> Option<String> {
    let rows: Vec<Row> = c
        .query("SPRIG SHOW LOOKUP INDEXES FROM orders")
        .await
        .unwrap_or_default();
    let r = rows.first()?;
    let i = r
        .columns_ref()
        .iter()
        .position(|c| c.name_str().eq_ignore_ascii_case("state"))?;
    r.get::<String, _>(i)
}

/// The group of `orders` and its member's routing and key columns.
async fn member(look: &mut Conn, tenant_id: u64) -> Option<(u32, String, String)> {
    look.query_first(format!(
        "SELECT m.group_id, m.routing, COALESCE(CAST(m.key_columns AS CHAR), '') \
         FROM t{tenant_id}_sprig.group_members m JOIN t{tenant_id}_sprig.objects o \
         ON o.object_id = m.object_id WHERE o.name = 'orders' AND o.state = 'active'"
    ))
    .await
    .unwrap()
}

/// One writer's view of its own orders.
#[derive(Default)]
struct Book {
    /// Acknowledged, not deleted: oid → (ref, amount).
    live: BTreeMap<i64, (String, i64)>,
    /// Acknowledged deletes.
    gone: BTreeSet<i64>,
    /// Orders whose last write failed without an answer we can trust.
    unknown: BTreeSet<i64>,
    ok: u64,
    errors: BTreeMap<String, u64>,
    /// Inserts of a taken `ref` that succeeded (must stay empty).
    duplicates_accepted: Vec<String>,
    /// Duplicate inserts refused with 1062.
    duplicates_refused: u64,
    /// Reads that returned the wrong answer (must stay empty).
    wrong_reads: Vec<String>,
    reads: u64,
    slowest: Duration,
}

impl Book {
    fn err(&mut self, what: &str, e: &mysql_async::Error) {
        let code = match e {
            mysql_async::Error::Server(s) => s.code.to_string(),
            _ => "client".to_owned(),
        };
        *self.errors.entry(format!("{what} {code}")).or_default() += 1;
    }
}

async fn writer(e: Arc<Env>, w: i64, stop: Arc<AtomicBool>) -> Book {
    let mut b = Book::default();
    let mut c = e.app().await;
    let base = 1_000_000 * (w + 1);
    let mut k: i64 = 0;
    while !stop.load(Ordering::Relaxed) {
        k += 1;
        let pick = |b: &Book, salt: i64| -> Option<i64> {
            let n = b.live.len();
            if n == 0 {
                return None;
            }
            let i = ((k * 7919 + salt) as usize) % n;
            b.live.keys().nth(i).copied()
        };
        let t = Instant::now();
        let mut reconnect = false;
        match k % 10 {
            // A new order with a new ref.
            0..=3 => {
                let oid = base + k;
                let r = format!("W{w}-{k}");
                let res = c
                    .exec_drop(
                        "INSERT INTO orders (oid, store_id, customer_id, ref, amount) \
                         VALUES (?, ?, ?, ?, ?)",
                        (oid, store_of(oid), customer_of(oid), &r, k),
                    )
                    .await;
                match res {
                    Ok(()) => {
                        b.ok += 1;
                        b.live.insert(oid, (r, k));
                    }
                    Err(err) => {
                        reconnect = !matches!(err, mysql_async::Error::Server(_));
                        b.err("insert", &err);
                        b.unknown.insert(oid);
                    }
                }
            }
            // A new order with a ref a seeded order holds.
            4 => {
                let oid = base + k;
                let taken = format!("R-{}", 1 + (k * 31 + w) % SEED);
                let res = c
                    .exec_drop(
                        "INSERT INTO orders (oid, store_id, customer_id, ref, amount) \
                         VALUES (?, ?, ?, ?, ?)",
                        (oid, store_of(oid), customer_of(oid), &taken, k),
                    )
                    .await;
                match errno(&res) {
                    Some(1062) => b.duplicates_refused += 1,
                    _ => match res {
                        Ok(()) => {
                            b.duplicates_accepted.push(format!("{oid} took {taken}"));
                            b.unknown.insert(oid);
                        }
                        Err(err) => {
                            reconnect = !matches!(err, mysql_async::Error::Server(_));
                            b.err("duplicate", &err);
                            b.unknown.insert(oid);
                        }
                    },
                }
            }
            // A new amount.
            5 => {
                if let Some(oid) = pick(&b, 1) {
                    let res = c
                        .exec_drop("UPDATE orders SET amount = ? WHERE oid = ?", (k, oid))
                        .await;
                    match res {
                        Ok(()) => {
                            b.ok += 1;
                            b.live.get_mut(&oid).unwrap().1 = k;
                        }
                        Err(err) => {
                            reconnect = !matches!(err, mysql_async::Error::Server(_));
                            b.err("amount", &err);
                            b.live.remove(&oid);
                            b.unknown.insert(oid);
                        }
                    }
                }
            }
            // A new ref.
            6 => {
                if let Some(oid) = pick(&b, 2) {
                    let r = format!("W{w}-{k}-x");
                    let res = c
                        .exec_drop("UPDATE orders SET ref = ? WHERE oid = ?", (&r, oid))
                        .await;
                    match res {
                        Ok(()) => {
                            b.ok += 1;
                            b.live.get_mut(&oid).unwrap().0 = r;
                        }
                        Err(err) => {
                            reconnect = !matches!(err, mysql_async::Error::Server(_));
                            b.err("ref", &err);
                            b.live.remove(&oid);
                            b.unknown.insert(oid);
                        }
                    }
                }
            }
            // A delete by ref, routed through the lookup.
            7 => {
                if let Some(oid) = pick(&b, 3) {
                    let r = b.live[&oid].0.clone();
                    let res = c.exec_drop("DELETE FROM orders WHERE ref = ?", (&r,)).await;
                    match res {
                        Ok(()) => {
                            b.ok += 1;
                            if c.affected_rows() == 1 {
                                b.live.remove(&oid);
                                b.gone.insert(oid);
                            } else {
                                b.wrong_reads.push(format!(
                                    "delete of {oid} by {r} removed {} rows",
                                    c.affected_rows()
                                ));
                                b.live.remove(&oid);
                                b.unknown.insert(oid);
                            }
                        }
                        Err(err) => {
                            reconnect = !matches!(err, mysql_async::Error::Server(_));
                            b.err("delete", &err);
                            b.live.remove(&oid);
                            b.unknown.insert(oid);
                        }
                    }
                }
            }
            // Reads by ref: one of ours, and a seeded order.
            _ => {
                let mut asks = Vec::new();
                if let Some(oid) = pick(&b, 4) {
                    let (r, a) = b.live[&oid].clone();
                    asks.push((r, oid, a));
                }
                let s = 1 + (k * 13 + w) % SEED;
                asks.push((format!("R-{s}"), s, s));
                for (r, oid, amount) in asks {
                    let res: mysql_async::Result<Vec<(i64, i64)>> = c
                        .exec("SELECT oid, amount FROM orders WHERE ref = ?", (&r,))
                        .await;
                    match res {
                        Ok(rows) => {
                            b.reads += 1;
                            if rows != vec![(oid, amount)] {
                                b.wrong_reads.push(format!(
                                    "read of {r}: {rows:?}, expected [({oid}, {amount})]"
                                ));
                            }
                        }
                        Err(err) => {
                            reconnect |= !matches!(err, mysql_async::Error::Server(_));
                            b.err("read", &err);
                        }
                    }
                }
            }
        }
        b.slowest = b.slowest.max(t.elapsed());
        // A paced load: a debug build's mover must keep up with it.
        tokio::time::sleep(Duration::from_millis(5)).await;
        if reconnect {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Ok(n) = Conn::new(e.opts(Some(DB))).await {
                c = n;
            }
        }
    }
    let _ = c.disconnect().await;
    b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_group_with_a_lookup_is_rekeyed_under_writes() {
    let Some(e) = Env::load() else {
        eprintln!(
            "skipped: set SPRIG_TEST_RKL_GATEWAY, _TENANT, _TENANT_ID, _CLUSTER and _SPRIGCTL \
             (module docs)"
        );
        return;
    };
    let e = Arc::new(e);
    let t0 = Instant::now();
    {
        let mut c = Conn::new(e.opts(None)).await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.disconnect().await.unwrap();
    }
    let mut c = e.app().await;
    c.query_drop(
        "CREATE TABLE orders (oid BIGINT PRIMARY KEY, store_id BIGINT NOT NULL, \
         customer_id BIGINT NOT NULL, ref VARCHAR(40) NULL, amount BIGINT NOT NULL)",
    )
    .await
    .unwrap();
    for chunk in (1..=SEED).collect::<Vec<_>>().chunks(500) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|&i| format!("({i}, {}, {}, 'R-{i}', {i})", store_of(i), customer_of(i)))
            .collect();
        c.query_drop(format!("INSERT INTO orders VALUES {}", vals.join(", ")))
            .await
            .unwrap();
    }
    // Statements by store, so the advisor sees the table used.
    for s in 1..=20 {
        let _: Vec<i64> = c
            .exec("SELECT oid FROM orders WHERE store_id = ?", (s,))
            .await
            .unwrap();
    }
    c.query_drop("SPRIG PIN orders SHARD KEY (store_id)")
        .await
        .unwrap();
    e.sprigctl(&["advisor", "run", &e.tenant, "-o", "json", "--yes"]);
    let mut look = e.look().await;
    let run: Option<u64> = look
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
        if let Some((g, routing, _)) = member(&mut look, e.tenant_id).await {
            assert_eq!(routing, "columns");
            break g;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "never grouped"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let first = e.shards(group).0;
    e.sprigctl(&[
        "shard",
        "split",
        &e.tenant,
        &group.to_string(),
        &first[0].0.to_string(),
        "--ways",
        "3",
        "--yes",
    ]);
    let started = Instant::now();
    while e.shards(group).0.len() < 3 {
        assert!(started.elapsed() < Duration::from_secs(600), "no 3 shards");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let old: BTreeSet<u32> = e.shards(group).0.iter().map(|s| s.0).collect();
    // The periodic run leaves groups with a pinned member alone.
    c.query_drop("SPRIG UNPIN orders").await.unwrap();
    eprintln!("[{:?}] group {group} in shards {old:?}", t0.elapsed());

    c.query_drop("ALTER TABLE orders ADD UNIQUE KEY uk_ref (ref)")
        .await
        .unwrap();
    let started = Instant::now();
    while lookup_state(&mut c).await.as_deref() != Some("public") {
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "uk_ref never public: {:?}",
            lookup_state(&mut c).await
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let r = c
        .query_drop("INSERT INTO orders VALUES (999999, 1, 1, 'R-17', 0)")
        .await;
    assert_eq!(errno(&r), Some(1062), "a taken ref across shards");
    eprintln!("[{:?}] uk_ref public", t0.elapsed());

    // ---- a workload by customer, and the advisor's periodic settings -------
    for i in 0..1_500 {
        let cust = customer_of(1 + (i * 7) % SEED);
        let _: Vec<(i64, i64)> = c
            .exec(
                "SELECT oid, amount FROM orders WHERE customer_id = ?",
                (cust,),
            )
            .await
            .unwrap();
    }
    for i in 0..300 {
        let oid = 1 + (i * 13) % SEED;
        c.exec_drop(
            "UPDATE orders SET amount = amount WHERE customer_id = ? AND oid = ?",
            (customer_of(oid), oid),
        )
        .await
        .unwrap();
    }
    let periodic = [
        ("advisor.mode", "approve"),
        ("advisor.rekey_sustain_runs", "1"),
        ("advisor.rekey_cooldown_days", "0"),
        ("advisor.interval_hours", "0.003"),
    ];
    if var("SPRIG_TEST_RKL_MANUAL").is_none() {
        for (k, v) in periodic {
            e.sprigctl(&[
                "settings",
                "set",
                k,
                v,
                "--scope",
                "tenant",
                "--tenant",
                &e.tenant,
                "--reason",
                "re-key test",
                "--yes",
            ]);
        }
    }
    eprintln!("[{:?}] workload by customer recorded", t0.elapsed());

    // ---- writers, and the re-key while they write --------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let ws: Vec<_> = (0..WRITERS)
        .map(|w| tokio::spawn(writer(Arc::clone(&e), w, Arc::clone(&stop))))
        .collect();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let rk_started = Instant::now();
    if var("SPRIG_TEST_RKL_MANUAL").is_some() {
        let out = e.sprigctl(&[
            "shard",
            "rekey",
            &e.tenant,
            &group.to_string(),
            "--key",
            "customer_id",
            "-o",
            "json",
            "--yes",
        ]);
        eprintln!(
            "[{:?}] re-key started by hand: {}",
            t0.elapsed(),
            out.trim()
        );
    } else {
        // The periodic run proposes customer_id and, under `approve`,
        // raises an advisor.rekey item; approving it starts the re-key.
        let id = loop {
            let v: serde_json::Value = serde_json::from_str(&e.sprigctl(&[
                "recommendations",
                "list",
                "--tenant",
                &e.tenant,
                "--kind",
                "advisor.rekey",
                "-o",
                "json",
            ]))
            .unwrap_or_default();
            let item = v["items"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|i| i["state"].as_str() == Some("open"))
                .cloned();
            if let Some(i) = item {
                eprintln!(
                    "[{:?}] approvals item {}: {} ({})",
                    t0.elapsed(),
                    i["id"],
                    i["summary"].as_str().unwrap_or_default(),
                    i["action"]["text"].as_str().unwrap_or_default()
                );
                assert!(
                    i["action"]["text"]
                        .as_str()
                        .is_some_and(|t| t.contains("--key customer_id")),
                    "the item proposes customer_id: {i}"
                );
                break match &i["id"] {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
            }
            if rk_started.elapsed() > Duration::from_secs(600) {
                let runs: Vec<(u64, String, String, String)> = look
                    .query(format!(
                        "SELECT run_id, cause, verdict, status FROM t{}_sprig.advisor_runs \
                         ORDER BY run_id",
                        e.tenant_id
                    ))
                    .await
                    .unwrap_or_default();
                panic!("no advisor.rekey item; advisor runs {runs:?}");
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        };
        e.sprigctl(&[
            "recommendations",
            "approve",
            &id,
            "--reason",
            "re-key under writes test",
        ]);
        eprintln!("[{:?}] approved {id}", t0.elapsed());
    }
    let mut seen_rekeying = false;
    loop {
        let (_, state) = e.shards(group);
        seen_rekeying |= state == "rekeying";
        let m = member(&mut look, e.tenant_id).await.unwrap();
        if state == "active" && m.2.contains("customer_id") {
            break;
        }
        assert!(
            rk_started.elapsed() < Duration::from_secs(900),
            "re-key not finished: group {state}, member {m:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let rekey_took = rk_started.elapsed();
    eprintln!(
        "[{:?}] re-key finished in {rekey_took:?} (rekeying seen: {seen_rekeying})",
        t0.elapsed()
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    let mut books = Vec::new();
    for w in ws {
        books.push(w.await.unwrap());
    }
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    for b in &books {
        for (k, v) in &b.errors {
            *errors.entry(k.clone()).or_default() += v;
        }
    }
    eprintln!(
        "writers: {} acknowledged writes, {} reads, {} duplicates refused, {} live, {} deleted, \
         {} unknown; slowest statement {:?}; errors {errors:?}",
        books.iter().map(|b| b.ok).sum::<u64>(),
        books.iter().map(|b| b.reads).sum::<u64>(),
        books.iter().map(|b| b.duplicates_refused).sum::<u64>(),
        books.iter().map(|b| b.live.len()).sum::<usize>(),
        books.iter().map(|b| b.gone.len()).sum::<usize>(),
        books.iter().map(|b| b.unknown.len()).sum::<usize>(),
        books.iter().map(|b| b.slowest).max().unwrap_or_default(),
    );
    for b in &books {
        assert!(
            b.duplicates_accepted.is_empty(),
            "duplicate refs accepted: {:?}",
            b.duplicates_accepted
        );
        assert!(
            b.wrong_reads.is_empty(),
            "wrong answers: {:?}",
            b.wrong_reads
        );
    }
    assert!(seen_rekeying, "the group was never seen rekeying");
    for k in ["insert 1062", "ref 1062"] {
        assert!(
            !errors.contains_key(k),
            "a new ref refused as taken: {errors:?}"
        );
    }

    // ---- the rows, on the cluster ----------------------------------------
    let (shards, state) = e.shards(group);
    assert_eq!(state, "active");
    assert_eq!(shards.len(), old.len(), "as many new shards: {shards:?}");
    assert!(
        shards.iter().all(|s| !old.contains(&s.0)),
        "the shards are new: {shards:?} (old {old:?})"
    );
    let mut rows: BTreeMap<i64, (i64, Option<String>, i64, u64, u32)> = BTreeMap::new();
    for (shard, lo, hi, _) in &shards {
        let schema: String = look
            .query_first(format!(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
                 WHERE SCHEMA_NAME LIKE 't{}\\_d%\\_g{group}\\_s{shard}'",
                e.tenant_id
            ))
            .await
            .unwrap()
            .expect("shard schema");
        let got: Vec<(i64, i64, Option<String>, i64, u64)> = look
            .query(format!(
                "SELECT oid, customer_id, ref, amount, _sprig_rk FROM `{schema}`.orders"
            ))
            .await
            .unwrap();
        for (oid, cust, r, amount, h) in got {
            assert_eq!(
                h,
                rk(cust),
                "order {oid}: _sprig_rk is customer {cust}'s hash"
            );
            assert!(
                h >= *lo && hi.is_none_or(|hi| h < hi),
                "order {oid} (hash {h:#x}) outside shard {shard}"
            );
            let prev = rows.insert(oid, (cust, r, amount, h, *shard));
            assert!(prev.is_none(), "order {oid} on two shards");
        }
    }
    let mut checked = 0usize;
    for b in &books {
        for (oid, (r, amount)) in &b.live {
            let got = rows.get(oid).unwrap_or_else(|| panic!("order {oid} lost"));
            assert_eq!(got.1.as_deref(), Some(r.as_str()), "order {oid}'s ref");
            assert_eq!(got.2, *amount, "order {oid}'s amount");
            checked += 1;
        }
        for oid in &b.gone {
            assert!(!rows.contains_key(oid), "deleted order {oid} is back");
        }
    }
    for s in 1..=SEED {
        let got = rows
            .get(&s)
            .unwrap_or_else(|| panic!("seeded order {s} lost"));
        assert_eq!(got.1.as_deref(), Some(format!("R-{s}").as_str()));
    }
    let mut holders: BTreeMap<String, i64> = BTreeMap::new();
    for (oid, (_, r, _, _, _)) in &rows {
        if let Some(r) = r {
            let prev = holders.insert(r.to_lowercase(), *oid);
            assert!(prev.is_none(), "ref {r} held by {oid} and {prev:?}");
        }
    }

    // ---- the lookup rows ---------------------------------------------------
    let schemas: Vec<String> = look
        .exec(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = \
             '__sprig_lk_orders__uk_ref' AND TABLE_SCHEMA LIKE ?",
            (format!("t{}\\_d%", e.tenant_id),),
        )
        .await
        .unwrap();
    assert!(!schemas.is_empty(), "no lookup tables");
    let mut lk: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
    for s in &schemas {
        let got: Vec<(String, i64, u64)> = look
            .query(format!(
                "SELECT ref, owner_oid, owner_rk FROM `{s}`.`__sprig_lk_orders__uk_ref`"
            ))
            .await
            .unwrap();
        for (r, oid, h) in got {
            lk.entry(r.to_lowercase()).or_default().push((oid, h));
        }
    }
    for (r, oid) in &holders {
        let (_, _, _, h, _) = rows[oid];
        let held = lk.get(r).cloned().unwrap_or_default();
        assert_eq!(
            held,
            vec![(*oid, h)],
            "ref {r}: order {oid} with hash {h:#x}, lookup rows {held:?}"
        );
    }
    let orphans = lk.keys().filter(|r| !holders.contains_key(*r)).count();
    eprintln!(
        "[{:?}] {} orders on {} shards ({checked} acknowledged writers' orders checked), {} lookup \
         rows, {orphans} orphans",
        t0.elapsed(),
        rows.len(),
        shards.len(),
        lk.values().map(Vec::len).sum::<usize>()
    );

    // ---- reads by ref through the gateway ----------------------------------
    assert_eq!(lookup_state(&mut c).await.as_deref(), Some("public"));
    let sample: Vec<(String, i64)> = holders
        .iter()
        .step_by((holders.len() / 300).max(1))
        .map(|(r, o)| (r.clone(), *o))
        .collect();
    let f0 = e.metrics.as_deref().map(fanout);
    for (r, oid) in &sample {
        let got: Vec<i64> = c
            .exec("SELECT oid FROM orders WHERE ref = ?", (r,))
            .await
            .unwrap();
        assert_eq!(got, vec![*oid], "read of {r}");
    }
    if let (Some(addr), Some((s0, n0))) = (e.metrics.as_deref(), f0) {
        let (s1, n1) = fanout(addr);
        let per = (s1 - s0) / (n1 - n0).max(1.0);
        eprintln!("{} reads by ref: {per:.2} shards each", sample.len());
        assert!(per < 1.01, "reads by ref fan out to {per} shards");
    }
    // The key is the group's: a read by customer goes to one shard.
    let cust = customer_of(1);
    let got: Vec<i64> = c
        .exec(
            "SELECT oid FROM orders WHERE customer_id = ? ORDER BY oid",
            (cust,),
        )
        .await
        .unwrap();
    let want: Vec<i64> = rows
        .iter()
        .filter(|(_, v)| v.0 == cust)
        .map(|(o, _)| *o)
        .collect();
    assert_eq!(got, want, "orders of customer {cust}");
    if var("SPRIG_TEST_RKL_MANUAL").is_none() {
        for (k, _) in periodic {
            e.sprigctl(&[
                "settings", "reset", k, "--scope", "tenant", "--tenant", &e.tenant, "--yes",
            ]);
        }
    }
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    c.disconnect().await.unwrap();
    eprintln!("[{:?}] done", t0.elapsed());
}
