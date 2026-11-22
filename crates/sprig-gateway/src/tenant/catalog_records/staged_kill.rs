//! kill -9 of a process writing module records on the catalog version
//! (the record log bridge) on a real engine and a real MariaDB catalog.
//!
//! A child process (this test binary again) commits changes in a loop,
//! each writing two records tagged with its version and deleting one of
//! two versions back, and prints every acknowledged version. The parent
//! kills it at a random moment, reopens the engine, and checks that a
//! snapshot load sees exactly the records of the catalog's committed
//! version (every acknowledged change included), and that the next change
//! catches the engine up to the log.
//!
//! `SPRIG_TEST_MARIADB_SINGLE` (`mysql://root:...@127.0.0.1:<port>`) names
//! the MariaDB; without it the test reports what it needs and passes.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde_json::json;
use sprig_catalog::cache::{
    CatalogExtension, CatalogSource, ExtState, ExtensionStates, MariaSource, Node, SnapshotTx,
};
use sprig_catalog::migrate::{MigrateOpts, Migration};
use sprig_catalog::records::staged::applied;
use sprig_catalog::records::{RecordStore, prefix_end, scan_all};
use sprig_catalog::store::{CatalogEndpoint, CatalogLocator, StaticLocator, Store};
use sprig_catalog::{
    DirectoryAllocator, MariaBackend, MariaCatalog, Migrator, VersionKind, Versioner,
};
use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_engine::{EngineConfig, StoreKind};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::EngineRecords;

const T: TenantId = TenantId(57_306_000_901);
const MODULE: &str = "staged_kill";
const PREFIX: &[u8] = b"m/staged_kill/";
const CHILD: &str = "SPRIG_STAGED_KILL_CHILD";

#[derive(Debug, Default)]
struct Rows(BTreeMap<Vec<u8>, Vec<u8>>);

impl ExtState for Rows {
    fn bytes(&self) -> usize {
        self.0.len()
    }
}

struct Ext;

#[async_trait::async_trait]
impl CatalogExtension for Ext {
    fn module(&self) -> &'static str {
        MODULE
    }
    fn migrations(&self) -> &'static [Migration] {
        &[]
    }
    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }
    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let rows = scan_all(tx.records()?, PREFIX, &prefix_end(PREFIX)).await?;
        Ok(Arc::new(Rows(rows.into_iter().collect())))
    }
}

fn key(v: u64, part: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(format!("{v:08}/{part}").as_bytes());
    k
}

fn expected(at: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for v in 1..=at {
        m.insert(key(v, "a"), v.to_be_bytes().to_vec());
        m.insert(key(v, "b"), v.to_be_bytes().to_vec());
        if v > 2 {
            m.remove(&key(v - 2, "a"));
        }
    }
    m
}

fn open(dir: &Path) -> sprig_engine::Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.store = StoreKind::Rocks;
    sprig_engine::Engine::open(cfg).unwrap()
}

fn locator(pool: &mysql_async::Pool, schema: &str) -> Arc<dyn CatalogLocator> {
    StaticLocator::new()
        .with(
            T,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema),
            },
        )
        .shared()
}

/// A directory whose catalog version starts at the catalog's.
async fn directory(at: u64) -> Arc<dyn MetaStore> {
    let g = Arc::new(MemStore::new(Tier::Global));
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = json!({"v": 1, "tenant_id": T.0, "name": "kill", "state": "active",
                      "home_dc": "dc-a", "catalog_version": at, "auth_version": 0});
    g.txn(Txn {
        then: vec![Op::Put {
            key,
            value: serde_json::to_vec(&body).unwrap(),
            lease: None,
        }],
        ..Txn::default()
    })
    .await
    .unwrap();
    g
}

async fn catalog_version(pool: &mysql_async::Pool, schema: &str) -> u64 {
    let mut c = pool.get_conn().await.unwrap();
    Store::new(schema).versions(&mut c).await.unwrap().catalog
}

async fn change(cat: &MariaCatalog) -> u64 {
    let mut c = cat.change(T, VersionKind::Catalog).await.unwrap();
    let v = c.version();
    {
        let rec = c.tx().records().await.unwrap();
        rec.put(&key(v, "a"), &v.to_be_bytes()).await.unwrap();
        rec.put(&key(v, "b"), &v.to_be_bytes()).await.unwrap();
        if v > 2 {
            rec.delete(&key(v - 2, "a")).await.unwrap();
        }
    }
    c.commit().await.unwrap()
}

/// The child: changes until killed, printing each acknowledged version.
async fn child(url: &str, schema: &str, dir: &Path) {
    let pool = mysql_async::Pool::new(url);
    let at = catalog_version(&pool, schema).await;
    let records: Arc<dyn RecordStore> = Arc::new(EngineRecords::on(open(dir)));
    let cat: MariaCatalog = Versioner::new(
        MariaBackend::new(locator(&pool, schema)).with_records(records),
        Arc::new(DirectoryAllocator::new(directory(at).await)),
    );
    println!("ready");
    loop {
        let v = change(&cat).await;
        println!("ok {v}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_module_records_survive_kill_9() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let schema = format!("__sprig_test_staged_kill_{}", std::process::id());
    let dir = std::env::temp_dir().join(format!("sprig-staged-kill-{}", std::process::id()));
    if let Ok(c) = std::env::var(CHILD) {
        let (schema, dir) = c.split_once('|').unwrap();
        child(&url, schema, Path::new(dir)).await;
        return;
    }
    let pool = mysql_async::Pool::new(url.as_str());
    {
        let mut c = pool.get_conn().await.unwrap();
        Migrator::new(&[])
            .unwrap()
            .run(&mut c, &schema, &MigrateOpts::default())
            .await
            .unwrap();
    }
    std::fs::create_dir_all(&dir).unwrap();
    let rounds: u64 = std::env::var("SPRIG_STAGED_KILL_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(8);
    let mut acked_total = 0;
    let mut report = Vec::new();
    for round in 0..rounds {
        let exe = std::env::current_exe().unwrap();
        let mut ch = Command::new(exe)
            .args([
                "--exact",
                "tenant::catalog_records::staged_kill::live_module_records_survive_kill_9",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env(CHILD, format!("{schema}|{}", dir.display()))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let out = ch.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(std::io::Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        // Wait until it's writing, then let it run a while.
        loop {
            let l = rx.recv_timeout(Duration::from_secs(60)).unwrap();
            if l.ends_with("ready") {
                break;
            }
        }
        let wait = 100 + (round * 137) % 700;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        ch.kill().unwrap(); // SIGKILL
        let _ = ch.wait();
        reader.join().unwrap();
        let acked: Vec<u64> = rx
            .try_iter()
            .filter_map(|l| l.strip_prefix("ok ").and_then(|v| v.parse().ok()))
            .collect();
        let last_acked = acked.last().copied().unwrap_or(0);
        acked_total += acked.len();

        // Reopen and check.
        let eng = open(&dir);
        let records: Arc<dyn RecordStore> = Arc::new(EngineRecords::on(eng.clone()));
        let loc = locator(&pool, &schema);
        let source = MariaSource::new(Arc::clone(&loc), vec![Arc::new(Ext)])
            .unwrap()
            .with_records(Arc::clone(&records));
        let l = source.load(T, None, Node::Writer).await.unwrap();
        let at = l.snapshot.versions.catalog;
        let states = ExtensionStates::default().with(l.ext);
        let rows = states.get::<Rows>(MODULE).unwrap().0.clone();
        assert!(
            at >= last_acked,
            "round {round}: acknowledged v{last_acked} lost (at v{at})"
        );
        assert_eq!(rows, expected(at), "round {round}: load at v{at}");
        let mut c = pool.get_conn().await.unwrap();
        let pending: u64 = c
            .query_first(format!(
                "SELECT COUNT(*) FROM `{schema}`.rec_log WHERE seq > {}",
                {
                    let mut t = records.begin(T).await.unwrap();
                    applied(&mut *t).await.unwrap()
                }
            ))
            .await
            .unwrap()
            .unwrap();
        // The next change catches the engine up.
        let cat: MariaCatalog = Versioner::new(
            MariaBackend::new(Arc::clone(&loc)).with_records(Arc::clone(&records)),
            Arc::new(DirectoryAllocator::new(directory(at).await)),
        );
        let v = change(&cat).await;
        let mut t = records.begin(T).await.unwrap();
        let engine: BTreeMap<_, _> = scan_all(&mut *t, PREFIX, &prefix_end(PREFIX))
            .await
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(engine, expected(v), "round {round}: engine after catch-up");
        drop(t);
        report.push(format!(
            "round {round}: killed after {wait} ms, {} acked (last v{last_acked}), committed v{at}, {pending} log rows unapplied",
            acked.len()
        ));
        drop(source);
        drop(records);
        drop(cat);
        drop(eng);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for r in &report {
        eprintln!("{r}");
    }
    eprintln!("{rounds} rounds, {acked_total} acknowledged changes, 0 inconsistencies");
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE `{schema}`"))
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
