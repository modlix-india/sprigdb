//! Retrieval indexes on a cluster of three replicated gateways: the in-node
//! job keeps an index in step after the node leading its tenant stops, on
//! the node elected next (which no client of the tenant reached).
//!
//! Needs the local stack of `tools/live/stack.sh` with three gateways (no
//! Raft TLS: `SPRIGDB_ENGINE_INSECURE_CLUSTER=true`), the docs model on
//! every node (`docs.model_dir`), and a tenant made by `stack.sh tenant`:
//!
//! ```sh
//! SPRIGDB_ENGINE_INSECURE_CLUSTER=true tools/live/stack.sh up 3
//! tools/live/stack.sh tenant live
//! eval "$(tools/live/stack.sh env live | sed 's/^/export /')"
//! export SPRIG_LIVE_STACK=tools/live/stack.sh
//! cargo test -p sprig-gateway --test retrieval_multinode_live -- --ignored --test-threads 1
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::print_stdout,
    missing_docs,
    dead_code
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;
use sprig_meta::{EtcdConfig, EtcdStore, MetaStore, Tier};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const IGNORE: &str = "live: needs SPRIG_LIVE_STACK, SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, \
     SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md";

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

/// The ids `RETRIEVE()` finds for `query` through gateway `n`.
async fn hits(env: &Env, n: u64, query: &str) -> Vec<i64> {
    let mut conn = env.conn(n, Some("rxm_live")).await;
    let ids: Vec<i64> = conn
        .query(format!(
            "SELECT id FROM RETRIEVE(kb, '{}', 3) ORDER BY `rank`",
            query.replace('\'', "''")
        ))
        .await
        .unwrap_or_default();
    let _ = conn.disconnect().await;
    ids
}

/// An index keeps following its table's changes after the node leading the
/// tenant stops: the next leader's job takes over from the saved token.
#[ignore = "live: needs SPRIG_LIVE_STACK, SPRIG_LIVE_ETCD, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_index_follows_changes_after_its_leader_stops() {
    let env = env().await;
    let tid = env.tenant_id().await;
    let all = [1u64, 2, 3];
    let mut conn = env.conn(1, None).await;
    must(&mut conn, "DROP DATABASE IF EXISTS rxm_live").await;
    must(&mut conn, "CREATE DATABASE rxm_live").await;
    must(&mut conn, "USE rxm_live").await;
    must(
        &mut conn,
        "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)",
    )
    .await;
    must(
        &mut conn,
        "INSERT INTO notes VALUES (1, 'Reset your password from the account settings page.'), \
         (2, 'Bananas grow in bunches on tall plants.'), \
         (3, 'Invoices are sent by email at the start of each month.')",
    )
    .await;
    must(&mut conn, "CREATE RETRIEVAL INDEX kb ON notes (body)").await;
    let t0 = Instant::now();
    loop {
        let rows: Vec<mysql_async::Row> = conn.query("SHOW RETRIEVAL INDEXES").await.unwrap();
        let ready = rows.iter().any(|r| {
            r.get::<String, _>(4).as_deref() == Some("ready") && r.get::<u64, _>(5) == Some(3)
        });
        if ready {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(120),
            "not ready in 120 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    drop(conn);
    let down = current_leader(&env, tid, &all).await.expect("a leader");
    stack("stop", down);
    let rest: Vec<u64> = all.iter().copied().filter(|n| *n != down).collect();
    let t0 = Instant::now();
    let next = loop {
        if let Some(next) = current_leader(&env, tid, &rest).await {
            break next;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "no new leader in 30 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    println!("node {down} stopped; node {next} leads");
    let other = *rest.iter().find(|n| **n != next).unwrap();
    // A change written through the node that doesn't lead: the new
    // leader's job indexes it.
    let mut conn = env.conn(other, Some("rxm_live")).await;
    must(
        &mut conn,
        "INSERT INTO notes VALUES (4, 'Connect your Outlook calendar under Integrations.')",
    )
    .await;
    must(
        &mut conn,
        "UPDATE notes SET body = 'Bake bread with a sourdough starter.' WHERE id = 2",
    )
    .await;
    let t0 = Instant::now();
    let mut found = (Vec::new(), Vec::new());
    while t0.elapsed() < Duration::from_secs(60) {
        found = (
            hits(&env, other, "link my outlook calendar").await,
            hits(&env, other, "sourdough starter").await,
        );
        if found.0.first() == Some(&4) && found.1.first() == Some(&2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let took = t0.elapsed();
    stack("start", down);
    must(&mut conn, "DROP DATABASE rxm_live").await;
    assert!(
        found.0.first() == Some(&4) && found.1.first() == Some(&2),
        "after node {down} stopped, node {next}'s job didn't index the insert and the update: {found:?}"
    );
    println!(
        "insert and update found {} ms after node {down} stopped",
        took.as_millis()
    );
}
