//! Catalog records, background runners and tenant deletion on a cluster
//! of replicated gateways with units placed per tenant: what a node that
//! doesn't lead a tenant's unit, or leads it without any client of the
//! tenant ever reaching it, does for the tenant.
//!
//! Needs the local stack of `tools/live/stack.sh` with three gateways and
//! placement on, and a tenant made by `stack.sh tenant`:
//!
//! ```sh
//! SPRIG_LIVE_PLACEMENT=on SPRIGDB_ENGINE_INSECURE_CLUSTER=true tools/live/stack.sh up 3
//! tools/live/stack.sh tenant live
//! eval "$(tools/live/stack.sh env live | sed 's/^/export /')"
//! cargo test -p sprig-gateway --test multinode_records_live -- --ignored --test-threads 1
//! ```
//!
//! The variables are `stack.sh env`'s: `SPRIG_LIVE_ETCD`, `SPRIG_LIVE_TENANT`,
//! `SPRIG_LIVE_USER`, `SPRIG_LIVE_PASSWORD` and `SPRIG_LIVE_GW<n>_MYSQL` /
//! `_HTTP` for gateways 1 to 3 (gateway n is Raft node n). Each test works
//! in databases of its own (`nrec_*`), dropped at the end. The tests move
//! the tenant's leadership by rewriting its unit descriptor's home, as the
//! leader-preference thread follows it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::print_stdout,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const IGNORE: &str = "live: needs SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, \
     SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md";

struct Env {
    global: Arc<dyn MetaStore>,
    dc: Arc<dyn MetaStore>,
    tenant: String,
    user: String,
    password: String,
    /// Gateway n's (MySQL, HTTP) addresses, by Raft id.
    gw: BTreeMap<u64, (String, String)>,
}

fn var(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set ({IGNORE})"))
}

async fn env() -> Env {
    let cfg = EtcdConfig::new(vec![var("SPRIG_LIVE_ETCD")]);
    let dc_id = sprig_common::ids::DcId::new(
        std::env::var("SPRIG_LIVE_DC").unwrap_or_else(|_| "dc-a".into()),
    )
    .unwrap();
    let global: Arc<dyn MetaStore> =
        Arc::new(EtcdStore::connect(Tier::Global, &cfg).await.unwrap());
    let dc: Arc<dyn MetaStore> = Arc::new(EtcdStore::connect(Tier::Dc(dc_id), &cfg).await.unwrap());
    let mut gw = BTreeMap::new();
    for n in 1..=3u64 {
        gw.insert(
            n,
            (
                var(&format!("SPRIG_LIVE_GW{n}_MYSQL")),
                var(&format!("SPRIG_LIVE_GW{n}_HTTP")),
            ),
        );
    }
    Env {
        global,
        dc,
        tenant: var("SPRIG_LIVE_TENANT"),
        user: var("SPRIG_LIVE_USER"),
        password: var("SPRIG_LIVE_PASSWORD"),
        gw,
    }
}

impl Env {
    /// The tenant's id, from the directory.
    async fn tenant_id(&self) -> u64 {
        let prefix = self.global.tier().key("tenants/").unwrap();
        let page = self
            .global
            .list(
                &prefix,
                sprig_meta::ListOpts {
                    limit: 10_000,
                    ..sprig_meta::ListOpts::default()
                },
            )
            .await
            .unwrap();
        for kv in page.kvs {
            if let Ok(r) =
                sprig_meta::Record::<sprig_gateway::inherited::DirEntry>::decode(&kv.value)
                && r.body.name == self.tenant
            {
                return r.body.tenant_id;
            }
        }
        panic!("no tenant {} in the directory", self.tenant);
    }

    async fn descriptor(&self, tenant: u64) -> sprig_catalog::units::UnitDescriptor {
        sprig_catalog::units::read(self.dc.as_ref(), tenant)
            .await
            .unwrap()
            .expect("the tenant's unit descriptor")
    }

    /// Who node `n` says leads `unit` (`None`: no leader known there).
    async fn leader_seen_by(&self, n: u64, unit: u64) -> Option<u64> {
        let v = http_get(&self.gw[&n].1, "/admin/v1/engine/units").await;
        v["units"]
            .as_array()?
            .iter()
            .find(|u| u["unit"].as_u64() == Some(unit))
            .and_then(|u| u["leader"].as_u64())
    }

    /// Waits until every voter says `to` leads `unit`.
    async fn wait_leader(&self, unit: u64, voters: &[u64], to: u64) {
        let t0 = Instant::now();
        loop {
            let mut all = true;
            for &n in voters {
                if self.leader_seen_by(n, unit).await != Some(to) {
                    all = false;
                }
            }
            if all {
                return;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "node {to} didn't take unit {unit}'s leadership in 30 s"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Moves `tenant`'s leadership to `to` (the descriptor's home) and
    /// waits until the voters agree.
    async fn move_leader(&self, tenant: u64, to: u64) {
        let d = sprig_catalog::units::update(self.dc.as_ref(), tenant, |d| {
            if d.home == to {
                return false;
            }
            d.home = to;
            true
        })
        .await
        .unwrap()
        .unwrap();
        self.wait_leader(tenant, &d.voters, to).await;
    }

    async fn conn(&self, n: u64, db: Option<&str>) -> Conn {
        let (host, port) = self.gw[&n].0.rsplit_once(':').unwrap();
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(&self.user))
            .pass(Some(&self.password))
            .db_name(db)
            .ssl_opts(Some(
                SslOpts::default().with_danger_accept_invalid_certs(true),
            ));
        Conn::new(opts).await.unwrap()
    }

    /// `POST <path>` on gateway `n`'s HTTP listener as the tenant's user:
    /// (status, body).
    async fn post(&self, n: u64, path: &str, body: &Value) -> (u16, Value) {
        http_post(&self.gw[&n].1, path, &self.user, &self.password, body).await
    }
}

async fn http_get(addr: &str, path: &str) -> Value {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    serde_json::from_str(body).unwrap_or(Value::Null)
}

async fn http_post(
    addr: &str,
    path: &str,
    user: &str,
    password: &str,
    body: &Value,
) -> (u16, Value) {
    use base64::Engine as _;
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    let b = body.to_string();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST {path} HTTP/1.0\r\nHost: {addr}\r\nAuthorization: Basic {auth}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}",
        b.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8_lossy(&out);
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn must(c: &mut Conn, q: &str) -> Vec<mysql_async::Row> {
    c.query::<mysql_async::Row, _>(q)
        .await
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn unix_now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

/// Sleeps until the wall clock reaches `at` (Unix seconds).
async fn sleep_until(at: i64) {
    let now = unix_now();
    if at > now {
        tokio::time::sleep(Duration::from_secs(u64::try_from(at - now).unwrap())).await;
    }
}

/// A voter of `d` other than `not`.
fn other_voter(d: &sprig_catalog::units::UnitDescriptor, not: u64) -> u64 {
    *d.voters.iter().find(|v| **v != not).unwrap()
}

/// A schedule keeps firing once a minute, exactly once per minute, on a
/// leader that no client of the tenant ever reached: before, the runner
/// acted only for tenants whose catalog its process had loaded, so after
/// a leadership move nothing fired until a client came.
#[ignore = "live: needs SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_fires_once_a_minute_on_a_leader_no_client_reached() {
    let e = env().await;
    let t = e.tenant_id().await;
    let d = e.descriptor(t).await;
    e.wait_leader(t, &d.voters, d.home).await;
    let a = d.home;
    // A client on another node: forwarded to the leader, which loads the
    // tenant's catalog.
    let entry = other_voter(&d, a);
    let mut c = e.conn(entry, None).await;
    must(&mut c, "DROP DATABASE IF EXISTS nrec_sched").await;
    must(&mut c, "CREATE DATABASE nrec_sched").await;
    must(&mut c, "USE nrec_sched").await;
    must(
        &mut c,
        "CREATE TABLE fires (id BIGINT AUTO_INCREMENT PRIMARY KEY, at BIGINT NOT NULL)",
    )
    .await;
    must(
        &mut c,
        "CREATE PROCEDURE tick() INSERT INTO fires (at) VALUES (UNIX_TIMESTAMP())",
    )
    .await;
    must(
        &mut c,
        "CREATE SCHEDULE every_minute CRON '* * * * *' DO CALL tick()",
    )
    .await;
    // The first fire, by the node leading the unit now.
    let t0 = Instant::now();
    loop {
        let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM fires").await.unwrap();
        if n.unwrap_or(0) > 0 {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(90), "no fire in 90 s");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    drop(c);
    // Leadership to a node no client of the tenant reached.
    let b = (1..=3u64).find(|n| *n != a && *n != entry).unwrap();
    e.move_leader(t, b).await;
    let moved = unix_now();
    println!("leader moved from node {a} to node {b} at {moved}");
    // Two whole minutes after the move, with no client anywhere.
    let first = (moved / 60 + 1) * 60;
    sleep_until(first + 2 * 60 + 15).await;
    let mut c = e.conn(entry, Some("nrec_sched")).await;
    let read_at = unix_now();
    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, at FROM fires ORDER BY id")
        .await
        .unwrap();
    let mut per_minute: BTreeMap<i64, usize> = BTreeMap::new();
    for (_, at) in &rows {
        *per_minute.entry(at / 60).or_default() += 1;
    }
    println!("fires per minute: {per_minute:?} (read at {read_at})");
    for (m, n) in &per_minute {
        assert_eq!(*n, 1, "minute {m} fired {n} times: {rows:?}");
    }
    for m in [first / 60, first / 60 + 1] {
        assert!(
            per_minute.contains_key(&m),
            "minute {m} (after the move to node {b}, before any client came) didn't fire: {per_minute:?}"
        );
    }
    must(&mut c, "DROP SCHEDULE nrec_sched.every_minute").await;
    must(&mut c, "DROP DATABASE nrec_sched").await;
    // Back where it was, for the next test.
    e.move_leader(t, a).await;
}

/// The node that leads the tenant's unit now, by its own report.
async fn current_leader(e: &Env, unit: u64, up: &[u64]) -> Option<u64> {
    for &n in up {
        let v = http_get(&e.gw[&n].1, "/admin/v1/engine/units").await;
        if v["units"].as_array().is_some_and(|a| {
            a.iter()
                .any(|u| u["unit"].as_u64() == Some(unit) && u["leading"].as_bool() == Some(true))
        }) {
            return Some(n);
        }
    }
    None
}

/// `SPRIG_LIVE_STACK <cmd> gw<n>`: stops or starts one gateway of the
/// stack (`tools/live/stack.sh`, or a wrapper setting its variables).
fn stack(cmd: &str, n: u64) {
    let script = var("SPRIG_LIVE_STACK");
    let ok = std::process::Command::new(&script)
        .args([cmd, &format!("gw{n}")])
        .status()
        .unwrap()
        .success();
    assert!(ok, "{script} {cmd} gw{n}");
}

/// A schedule keeps firing, once a minute, after the node leading its
/// tenant stops: the node elected next never had a client of the tenant.
/// Works with or without placement (without it, nothing else loads the
/// tenant's catalog on that node, so the runner did nothing before).
#[ignore = "live: needs SPRIG_LIVE_STACK, SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_fires_once_a_minute_after_its_leader_stops() {
    let e = env().await;
    let t = e.tenant_id().await;
    let all = [1u64, 2, 3];
    let mut c = e.conn(1, None).await;
    must(&mut c, "DROP DATABASE IF EXISTS nrec_stop").await;
    must(&mut c, "CREATE DATABASE nrec_stop").await;
    must(&mut c, "USE nrec_stop").await;
    must(
        &mut c,
        "CREATE TABLE fires (id BIGINT AUTO_INCREMENT PRIMARY KEY, at BIGINT NOT NULL)",
    )
    .await;
    must(
        &mut c,
        "CREATE PROCEDURE tick() INSERT INTO fires (at) VALUES (UNIX_TIMESTAMP())",
    )
    .await;
    must(
        &mut c,
        "CREATE SCHEDULE every_minute CRON '* * * * *' DO CALL tick()",
    )
    .await;
    let t0 = Instant::now();
    loop {
        let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM fires").await.unwrap();
        if n.unwrap_or(0) > 0 {
            break;
        }
        assert!(t0.elapsed() < Duration::from_secs(90), "no fire in 90 s");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    drop(c);
    let a = current_leader(&e, t, &all).await.expect("a leader");
    stack("stop", a);
    let stopped = unix_now();
    let rest: Vec<u64> = all.iter().copied().filter(|n| *n != a).collect();
    let t0 = Instant::now();
    let b = loop {
        if let Some(b) = current_leader(&e, t, &rest).await {
            break b;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "no new leader in 30 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!("node {a} stopped at {stopped}; node {b} leads");
    let first = (stopped / 60 + 1) * 60;
    sleep_until(first + 2 * 60 + 15).await;
    let other = *rest.iter().find(|n| **n != b).unwrap();
    let mut c = e.conn(other, Some("nrec_stop")).await;
    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, at FROM fires ORDER BY id")
        .await
        .unwrap();
    stack("start", a);
    let mut per_minute: BTreeMap<i64, usize> = BTreeMap::new();
    for (_, at) in &rows {
        *per_minute.entry(at / 60).or_default() += 1;
    }
    println!("fires per minute: {per_minute:?}");
    must(&mut c, "DROP SCHEDULE nrec_stop.every_minute").await;
    must(&mut c, "DROP DATABASE nrec_stop").await;
    for (m, n) in &per_minute {
        assert_eq!(*n, 1, "minute {m} fired {n} times: {rows:?}");
    }
    for m in [first / 60, first / 60 + 1] {
        assert!(
            per_minute.contains_key(&m),
            "minute {m} (node {b} leading, no client) didn't fire: {per_minute:?}"
        );
    }
}

/// SQL events run on a new leader no client reached, from its first tick.
#[ignore = "live: needs SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_run_on_a_leader_no_client_reached() {
    let e = env().await;
    let t = e.tenant_id().await;
    let d = e.descriptor(t).await;
    e.wait_leader(t, &d.voters, d.home).await;
    let a = d.home;
    let entry = other_voter(&d, a);
    let mut c = e.conn(entry, None).await;
    must(&mut c, "DROP DATABASE IF EXISTS nrec_events").await;
    must(&mut c, "CREATE DATABASE nrec_events").await;
    must(&mut c, "USE nrec_events").await;
    must(
        &mut c,
        "CREATE TABLE ticks (id BIGINT AUTO_INCREMENT PRIMARY KEY, at BIGINT NOT NULL)",
    )
    .await;
    must(
        &mut c,
        "CREATE EVENT every_second ON SCHEDULE EVERY 1 SECOND \
         DO INSERT INTO ticks (at) VALUES (UNIX_TIMESTAMP())",
    )
    .await;
    let t0 = Instant::now();
    loop {
        let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM ticks").await.unwrap();
        if n.unwrap_or(0) > 0 {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "no event run in 30 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    drop(c);
    let b = (1..=3u64).find(|n| *n != a && *n != entry).unwrap();
    e.move_leader(t, b).await;
    let moved = unix_now();
    tokio::time::sleep(Duration::from_secs(20)).await;
    let mut c = e.conn(entry, Some("nrec_events")).await;
    let after: Option<i64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM ticks WHERE at >= {} AND at < {}",
            moved + 5,
            moved + 18
        ))
        .await
        .unwrap();
    let after = after.unwrap_or(0);
    println!("event runs on node {b} between 5 s and 18 s after the move: {after}");
    must(&mut c, "DROP EVENT nrec_events.every_second").await;
    must(&mut c, "DROP DATABASE nrec_events").await;
    e.move_leader(t, a).await;
    assert!(
        after >= 10,
        "the new leader ran the event {after} times in 13 s with no client"
    );
}

/// Writes through the HTTP API on a node that doesn't lead the tenant's
/// unit: DDL (catalog changes and catalog records), rows, `DROP DATABASE`
/// (its sinks and logic records after the catalog change) and a
/// transaction. Before, the node ran them itself and they failed with
/// 39070 (the catalog change) or 39110 (rows).
#[ignore = "live: needs SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_writes_on_a_node_that_doesnt_lead_the_tenant() {
    let e = env().await;
    let t = e.tenant_id().await;
    let d = e.descriptor(t).await;
    e.wait_leader(t, &d.voters, d.home).await;
    let f = other_voter(&d, d.home);
    let sql = |db: &str| format!("/v1/databases/{db}/sql:execute");
    let mut c = e.conn(d.home, None).await;
    must(&mut c, "DROP DATABASE IF EXISTS nrec_http").await;
    must(&mut c, "CREATE DATABASE nrec_http").await;
    drop(c);
    let steps = [
        "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))",
        "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
        "UPDATE t SET v = 'uno' WHERE id = 1",
        "CREATE PROCEDURE p() INSERT INTO t VALUES (3, 'three')",
        "CALL p()",
        "CREATE SCHEDULE s CRON '0 3 * * *' DO CALL p()",
        "DROP SCHEDULE s",
    ];
    for q in steps {
        let (status, body) = e
            .post(f, &sql("nrec_http"), &serde_json::json!({ "sql": q }))
            .await;
        assert_eq!(status, 200, "{q} on node {f}: {body}");
    }
    let (status, body) = e
        .post(
            f,
            &sql("nrec_http"),
            &serde_json::json!({ "sql": "SELECT COUNT(*) AS n FROM t" }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.to_string().contains('3'), "three rows: {body}");
    // A transaction begun on the node: its handle names the leader's
    // gateway, and the calls naming it follow.
    let (status, txn) = e
        .post(
            f,
            "/v1/databases/nrec_http/transactions",
            &serde_json::json!({}),
        )
        .await;
    assert_eq!(status, 200, "begin on node {f}: {txn}");
    let name = txn["name"].as_str().unwrap().to_owned();
    let (status, body) = e
        .post(
            f,
            &sql("nrec_http"),
            &serde_json::json!({ "sql": "INSERT INTO t VALUES (4, 'four')", "transaction": name }),
        )
        .await;
    assert_eq!(status, 200, "insert in the transaction: {body}");
    let (status, body) = e
        .post(f, &format!("/v1/{name}:commit"), &serde_json::json!({}))
        .await;
    assert_eq!(status, 200, "commit: {body}");
    let (status, body) = e
        .post(
            f,
            &sql("nrec_http"),
            &serde_json::json!({ "sql": "DROP DATABASE nrec_http" }),
        )
        .await;
    assert_eq!(status, 200, "DROP DATABASE on node {f}: {body}");
}

/// A deleted tenant leaves nothing behind on the cluster: its directory
/// entry and name go (before, a cleanup that dropped databases found its
/// own entry changed, failed, and every later pass stopped at the unit it
/// had already removed: the tenant stayed `deleting` for good), its keys
/// in the DC tier (the DDL barrier's acknowledgements too), its units on
/// every node and, when `SPRIG_LIVE_STATE` names the stack's state
/// directory, its full-text index files on every node.
#[ignore = "live: needs SPRIG_LIVE_ETCD, SPRIG_LIVE_SPRIGCTL, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_tenant_leaves_nothing_behind() {
    let mut e = env().await;
    let ctl: Vec<String> = var("SPRIG_LIVE_SPRIGCTL")
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let sprigctl = |args: &[&str]| {
        let out = std::process::Command::new(&ctl[0])
            .args(&ctl[1..])
            .args(args)
            .output()
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr),
        )
    };
    let name = format!("nrecdrop{}", unix_now() % 100_000);
    let (ok, out) = sprigctl(&["tenant", "create", &name, "--yes"]);
    assert!(ok, "tenant create: {out}");
    let mut added = false;
    for _ in 0..20 {
        let (ok, _) = sprigctl(&[
            "tenant",
            "user",
            "add",
            &name,
            "app",
            "--password",
            "sprig-drop-app",
            "--native",
            "--if-not-exists",
            "--yes",
        ]);
        if ok {
            added = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    assert!(added, "tenant user add");
    e.tenant.clone_from(&name);
    e.user = format!("{name}.app");
    e.password = "sprig-drop-app".into();
    let t = e.tenant_id().await;
    let mut c = e.conn(1, None).await;
    must(&mut c, "CREATE DATABASE nrec_gone").await;
    must(&mut c, "USE nrec_gone").await;
    must(
        &mut c,
        "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT, FULLTEXT KEY ft (body))",
    )
    .await;
    must(
        &mut c,
        "INSERT INTO notes VALUES (1, 'hello world'), (2, 'sprig engine')",
    )
    .await;
    let hits: Vec<i64> = c
        .query("SELECT id FROM notes WHERE MATCH(body) AGAINST('hello')")
        .await
        .unwrap();
    assert_eq!(hits, [1]);
    must(&mut c, "CREATE TABLE plain (id INT PRIMARY KEY)").await;
    must(&mut c, "INSERT INTO plain VALUES (1), (2), (3)").await;
    drop(c);
    let state = std::env::var("SPRIG_LIVE_STATE")
        .ok()
        .map(std::path::PathBuf::from);
    let idx = |n: u64| {
        state.as_ref().map(|s| {
            s.join(format!("engine-{n}"))
                .join("idx")
                .join(format!("u{t}"))
        })
    };
    // With or without placement: the node leading the unit has built it.
    let leader = current_leader(&e, t, &[1, 2, 3]).await.expect("a leader");
    if let Some(p) = idx(leader) {
        assert!(p.exists(), "the full-text index's files at {}", p.display());
    }
    let (ok, out) = sprigctl(&["tenant", "delete", &name, "--yes"]);
    assert!(ok, "tenant delete: {out}");
    // The directory entry goes once cleanup finished.
    let t0 = Instant::now();
    let key = e.global.tier().key(&format!("tenants/{t}")).unwrap();
    while e
        .global
        .get(&key, sprig_meta::Consistency::Linearizable)
        .await
        .unwrap()
        .is_some()
    {
        assert!(
            t0.elapsed() < Duration::from_secs(90),
            "tenant {name} ({t}) still in the directory 90 s after its deletion: {}",
            sprigctl(&["tenant", "show", &name]).1
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // Its keys in the DC tier.
    let root = e.dc.tier().root();
    let mut left = Vec::new();
    let mut opts = sprig_meta::ListOpts::default();
    loop {
        let page = e.dc.list(&root, opts.clone()).await.unwrap();
        left.extend(
            page.kvs
                .iter()
                .map(|kv| kv.key.as_str().to_owned())
                .filter(|k| k.split('/').any(|part| part == t.to_string())),
        );
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
    }
    assert!(
        left.is_empty(),
        "the deleted tenant's keys left in the DC tier: {left:?}"
    );
    // Its units and index files on every node.
    let t0 = Instant::now();
    for n in 1..=3u64 {
        loop {
            let v = http_get(&e.gw[&n].1, "/admin/v1/engine/units").await;
            let held = v["units"].as_array().is_some_and(|a| {
                a.iter().any(|u| {
                    u["unit"].as_u64().is_some_and(|x| {
                        x == t || sprig_streams::seg::group_of_unit(x).is_some_and(|(g, _)| g == t)
                    })
                })
            });
            let files = idx(n).is_some_and(|p| p.exists());
            if !held && !files {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(30),
                "node {n} still holds the deleted tenant's unit ({held}) or index files ({files})"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}
