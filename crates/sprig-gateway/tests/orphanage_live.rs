//! The foreign-key orphanage, live through the gateway:
//!
//! 1. `invoices (iid PK, cid, total)` pinned `SHARD KEY (cid)` and
//!    `invoice_lines (iid, line_no, sku, FOREIGN KEY (iid) REFERENCES
//!    invoices (iid) ON DELETE CASCADE)`, made a group by the advisor
//!    (`invoice_lines` a parent member) and split into 4 shards;
//! 2. a dump restore's order: `SET foreign_key_checks = 0`, every line,
//!    then every invoice, then `SET foreign_key_checks = 1`. The lines
//!    without their invoice wait on the shard holding routing hash 0
//!    (`_sprig_rk = 0`); each invoice's insert takes its lines to its own
//!    shard. Afterwards every line is on its invoice's shard with its
//!    invoice's hash, joins narrowed by `cid` find every line, and a line
//!    whose invoice never came stays readable;
//! 3. with checks on, a line without its invoice still fails with 1452;
//!    `DELETE` of an invoice cascades to its adopted lines.
//!
//! ```sh
//! source target/agent-r3small/env.sh   # a private deployment (gateway-dev.sh up)
//! export SPRIG_TEST_ORPH_GATEWAY=127.0.0.1:35306 SPRIG_TEST_ORPH_TENANT=<tenant> \
//!   SPRIG_TEST_ORPH_TENANT_ID=<id> SPRIG_TEST_ORPH_CLUSTER=127.0.0.1:13301 \
//!   SPRIG_TEST_ORPH_SPRIGCTL=target/agent-r3small/debug/sprigctl
//! cargo test -p sprig-gateway --test orphanage_live -- --nocapture
//! ```
//!
//! The cluster connection only reads (shard contents, the catalog).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts, SslOpts};

const DB: &str = "r3s_orph_db";
const INVOICES: i64 = 120;

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
    tenant: String,
    tenant_id: u64,
    cluster: String,
    sprigctl: String,
}

impl Env {
    fn load() -> Option<Self> {
        Some(Self {
            gateway: var("SPRIG_TEST_ORPH_GATEWAY")?,
            tenant: var("SPRIG_TEST_ORPH_TENANT")?,
            tenant_id: var("SPRIG_TEST_ORPH_TENANT_ID")?.parse().ok()?,
            cluster: var("SPRIG_TEST_ORPH_CLUSTER")?,
            sprigctl: var("SPRIG_TEST_ORPH_SPRIGCTL")?,
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

    fn app(&self) -> Pool {
        Pool::new(
            self.opts(Some(DB)).pool_opts(
                PoolOpts::default()
                    .with_constraints(PoolConstraints::new(0, 4).unwrap())
                    .with_inactive_connection_ttl(Duration::from_secs(60)),
            ),
        )
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
}

fn errno_of<T>(r: mysql_async::Result<T>) -> Option<u16> {
    match r {
        Err(mysql_async::Error::Server(s)) => Some(s.code),
        _ => None,
    }
}

fn cid_of(iid: i64) -> i64 {
    1 + (iid * 31) % 50
}

/// Every line on the shards: `iid → (shard schema, _sprig_rk)`, per line.
async fn placed(look: &mut Conn, tenant_id: u64, group: u32) -> Vec<(i64, i64, String, u64)> {
    let schemas: Vec<String> = look
        .query(format!(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
             WHERE SCHEMA_NAME LIKE 't{tenant_id}\\_d%\\_g{group}\\_s%'"
        ))
        .await
        .unwrap();
    let mut out = Vec::new();
    for s in schemas {
        let rows: Vec<(i64, i64, u64)> = look
            .query(format!(
                "SELECT iid, line_no, _sprig_rk FROM `{s}`.invoice_lines"
            ))
            .await
            .unwrap_or_default();
        out.extend(rows.into_iter().map(|(i, l, h)| (i, l, s.clone(), h)));
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::many_single_char_names)] // short names for the env, connection and rows
async fn rows_without_their_parent_wait_and_are_adopted() {
    let Some(e) = Env::load() else {
        eprintln!(
            "skipped: set SPRIG_TEST_ORPH_GATEWAY, _TENANT, _TENANT_ID, _CLUSTER and _SPRIGCTL"
        );
        return;
    };
    let t0 = Instant::now();
    {
        let mut c = Conn::new(e.opts(None)).await.unwrap();
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.disconnect().await.unwrap();
    }
    let pool = e.app();
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(
        "CREATE TABLE invoices (iid BIGINT PRIMARY KEY, cid BIGINT NOT NULL, \
         total DECIMAL(10,2) NOT NULL)",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE invoice_lines (iid BIGINT NOT NULL, line_no INT NOT NULL, \
         sku VARCHAR(20) NOT NULL, PRIMARY KEY (iid, line_no), \
         CONSTRAINT fk_il_inv FOREIGN KEY (iid) REFERENCES invoices (iid) ON DELETE CASCADE)",
    )
    .await
    .unwrap();
    // A first invoice and line so the advisor sees the tables used.
    c.query_drop("INSERT INTO invoices VALUES (1, 1, 1.00)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO invoice_lines VALUES (1, 1, 'seed')")
        .await
        .unwrap();
    c.query_drop("SPRIG PIN invoices SHARD KEY (cid)")
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
    let group_of = |name: &'static str| {
        format!(
            "SELECT m.group_id, m.routing FROM t{0}_sprig.group_members m \
             JOIN t{0}_sprig.objects o ON o.object_id = m.object_id \
             WHERE o.name = '{name}' AND o.state = 'active'",
            e.tenant_id
        )
    };
    let started = Instant::now();
    let group = loop {
        let inv: Option<(u32, String)> = look.query_first(group_of("invoices")).await.unwrap();
        let lines: Option<(u32, String)> =
            look.query_first(group_of("invoice_lines")).await.unwrap();
        if let (Some(a), Some(b)) = (inv, lines) {
            assert_eq!(a.0, b.0, "one group");
            assert_eq!(b.1, "parent", "invoice_lines follows its invoice");
            break a.0;
        }
        assert!(
            started.elapsed() < Duration::from_secs(300),
            "never grouped"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let shards = || {
        let v: serde_json::Value =
            serde_json::from_str(&e.sprigctl(&["shard", "list", &e.tenant, "-o", "json"]))
                .unwrap_or_default();
        v.as_array()
            .into_iter()
            .flatten()
            .filter(|g| g["group"].as_u64() == Some(u64::from(group)))
            .flat_map(|g| g["shards"].as_array().cloned().unwrap_or_default())
            .filter_map(|s| s["shard"].as_u64())
            .collect::<Vec<u64>>()
    };
    let first = shards();
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
    let started = Instant::now();
    while shards().len() < 4 {
        assert!(started.elapsed() < Duration::from_secs(600), "no 4 shards");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // The gateway routes over 4 shards once an insert of lines follows
    // parents (planned as a write per shard).
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("[{:?}] group {group} in 4 shards", t0.elapsed());

    // ---- with checks on, a missing invoice is MariaDB's 1452 -------------
    let r = c
        .query_drop("INSERT INTO invoice_lines VALUES (99999, 1, 'none')")
        .await;
    assert_eq!(errno_of(r), Some(1452));

    // ---- a restore: lines first, then invoices ---------------------------
    c.query_drop("SET foreign_key_checks = 0").await.unwrap();
    let mut lines = Vec::new();
    for iid in 100..100 + INVOICES {
        for l in 1..=3 {
            lines.push(format!("({iid}, {l}, 'sku{iid}-{l}')"));
        }
    }
    // Lines of an invoice that never comes.
    lines.push("(9000, 1, 'lost')".into());
    for chunk in lines.chunks(50) {
        c.query_drop(format!(
            "INSERT INTO invoice_lines VALUES {}",
            chunk.join(", ")
        ))
        .await
        .unwrap_or_else(|err| panic!("lines while checks are off: {err}"));
    }
    let open: Option<String> = look
        .query_first(format!(
            "SELECT orphanage FROM t{}_sprig.foreign_keys WHERE name = 'fk_il_inv'",
            e.tenant_id
        ))
        .await
        .unwrap();
    assert_eq!(open.as_deref(), Some("present"));
    let waiting = placed(&mut look, e.tenant_id, group).await;
    let at_zero = waiting.iter().filter(|r| r.3 == 0).count();
    assert_eq!(usize::try_from(INVOICES).unwrap() * 3 + 1, at_zero);
    let zero_schema = waiting.iter().find(|r| r.3 == 0).unwrap().2.clone();
    assert!(
        waiting
            .iter()
            .filter(|r| r.3 == 0)
            .all(|r| r.2 == zero_schema),
        "every waiting line on one shard"
    );
    // They are readable meanwhile.
    let n: u64 = c
        .query_first("SELECT COUNT(*) FROM invoice_lines WHERE iid >= 100")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, u64::try_from(INVOICES).unwrap() * 3 + 1);
    let invoices: Vec<String> = (100..100 + INVOICES)
        .map(|iid| format!("({iid}, {}, {}.50)", cid_of(iid), iid % 90))
        .collect();
    for chunk in invoices.chunks(20) {
        c.query_drop(format!("INSERT INTO invoices VALUES {}", chunk.join(", ")))
            .await
            .unwrap_or_else(|err| panic!("invoices: {err}"));
    }
    c.query_drop("SET foreign_key_checks = 1").await.unwrap();
    eprintln!(
        "[{:?}] restored {} lines and {INVOICES} invoices",
        t0.elapsed(),
        lines.len()
    );

    // ---- every line on its invoice's shard, with its hash ---------------
    let inv_at: BTreeMap<i64, (String, u64)> = {
        let schemas: Vec<String> = look
            .query(format!(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA \
                 WHERE SCHEMA_NAME LIKE 't{}\\_d%\\_g{group}\\_s%'",
                e.tenant_id
            ))
            .await
            .unwrap();
        let mut m = BTreeMap::new();
        for s in schemas {
            let rows: Vec<(i64, u64)> = look
                .query(format!("SELECT iid, _sprig_rk FROM `{s}`.invoices"))
                .await
                .unwrap_or_default();
            for (i, h) in rows {
                m.insert(i, (s.clone(), h));
            }
        }
        m
    };
    let after = placed(&mut look, e.tenant_id, group).await;
    let mut adopted = 0;
    for (iid, _, schema, h) in &after {
        if *iid == 9000 {
            assert_eq!(*h, 0, "a line without its invoice keeps waiting");
            continue;
        }
        let (s, ih) = inv_at.get(iid).unwrap();
        assert_eq!((schema, h), (s, ih), "line of {iid} not with its invoice");
        if *iid >= 100 {
            adopted += 1;
        }
    }
    assert_eq!(adopted, INVOICES * 3);
    // Joins narrowed by cid find every line.
    let mut joined = 0u64;
    for cid in 1..=50 {
        let n: u64 = c
            .query_first(format!(
                "SELECT COUNT(*) FROM invoices i JOIN invoice_lines l ON l.iid = i.iid \
                 WHERE i.cid = {cid} AND i.iid >= 100"
            ))
            .await
            .unwrap()
            .unwrap();
        joined += n;
    }
    assert_eq!(joined, u64::try_from(INVOICES * 3).unwrap());
    let lost: Vec<(i64, String)> = c
        .query("SELECT iid, sku FROM invoice_lines WHERE iid = 9000")
        .await
        .unwrap();
    assert_eq!(lost, vec![(9000, "lost".to_owned())]);
    // A cascade reaches adopted lines.
    c.query_drop("DELETE FROM invoices WHERE iid = 100")
        .await
        .unwrap();
    let left: u64 = c
        .query_first("SELECT COUNT(*) FROM invoice_lines WHERE iid = 100")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(left, 0);
    eprintln!(
        "[{:?}] {adopted} lines adopted onto their invoices' shards, {joined} joined by cid, \
         1 still waiting; cascade ok",
        t0.elapsed()
    );
    c.query_drop(format!("DROP DATABASE {DB}")).await.unwrap();
    drop(c);
    drop(look);
    pool.disconnect().await.unwrap();
}
