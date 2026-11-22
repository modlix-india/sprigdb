//! Live tests of module records on the catalog version (the record log
//! bridge) against MariaDB, with the engine's records in memory.
//! `SPRIG_TEST_MARIADB_SINGLE` names the MariaDB (see `crate::live_tests`).

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::similar_names)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::mem::MemRecords;
use super::staged::{APPLIED_KEY, applied};
use super::{RecordStore, RecordTx, prefix_end, scan_all};
use crate::cache::{
    CatalogExtension, CatalogSource, ExtState, ExtensionStates, MariaSource, Node, SnapshotTx,
};
use crate::live_tests::{migrate, seed_directory, unique_tenant, with_global, with_schema};
use crate::migrate::Migration;
use crate::store::{CatalogEndpoint, StaticLocator, Store};
use crate::version::{DirectoryAllocator, MariaBackend, MariaCatalog, VersionKind, Versioner};

const MODULE: &str = "staged_live";
const PREFIX: &[u8] = b"m/staged_live/";

/// The module's rows as loaded.
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
        assert!(tx.catalog_records());
        let rows = scan_all(tx.records()?, PREFIX, &prefix_end(PREFIX)).await?;
        Ok(Arc::new(Rows(rows.into_iter().collect())))
    }
}

/// Engine records whose applies can be made to fail (a crash between
/// MariaDB's commit and the engine's apply).
#[derive(Clone)]
struct Flaky {
    inner: MemRecords,
    fail_apply: Arc<AtomicBool>,
}

struct FlakyTx {
    inner: Box<dyn RecordTx>,
    fail_apply: Arc<AtomicBool>,
    marks: bool,
}

#[async_trait::async_trait]
impl RecordStore for Flaky {
    async fn begin(&self, tenant: TenantId) -> Result<Box<dyn RecordTx>> {
        Ok(Box::new(FlakyTx {
            inner: self.inner.begin(tenant).await?,
            fail_apply: Arc::clone(&self.fail_apply),
            marks: false,
        }))
    }
}

#[async_trait::async_trait]
impl RecordTx for FlakyTx {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_for_update(key).await
    }
    async fn scan(
        &mut self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan(lo, hi, limit).await
    }
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.marks |= key == APPLIED_KEY;
        self.inner.put(key, value).await
    }
    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key).await
    }
    async fn commit(self: Box<Self>) -> Result<()> {
        if self.marks && self.fail_apply.load(Ordering::SeqCst) {
            return Err(super::unavailable(
                "injected: the engine died before applying",
            ));
        }
        self.inner.commit().await
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        self.inner.rollback().await
    }
}

fn locator(pool: &Pool, schema: &str, tenant: TenantId) -> Arc<dyn crate::store::CatalogLocator> {
    StaticLocator::new()
        .with(
            tenant,
            CatalogEndpoint {
                pool: pool.clone(),
                store: Store::new(schema),
            },
        )
        .shared()
}

async fn load(source: &MariaSource, tenant: TenantId) -> (u64, BTreeMap<Vec<u8>, Vec<u8>>) {
    let l = source.load(tenant, None, Node::Writer).await.unwrap();
    let states = ExtensionStates::default().with(l.ext);
    let rows = states.get::<Rows>(MODULE).unwrap();
    (l.snapshot.versions.catalog, rows.0.clone())
}

fn key(v: u64, part: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(format!("{v:08}/{part}").as_bytes());
    k
}

/// One change writing two rows tagged with its version and deleting the
/// rows two versions back.
async fn change(cat: &MariaCatalog, tenant: TenantId) -> u64 {
    let mut c = cat.change(tenant, VersionKind::Catalog).await.unwrap();
    let v = c.version();
    {
        let rec = c.tx().records().await.unwrap();
        // Reads see earlier changes' rows (applied or still in the log).
        if v > 1 {
            let before = scan_all(rec, PREFIX, &prefix_end(PREFIX)).await.unwrap();
            assert!(!before.is_empty() || v <= 2, "v{v}: earlier rows missing");
        }
        rec.put(&key(v, "a"), &v.to_be_bytes()).await.unwrap();
        rec.put(&key(v, "b"), &v.to_be_bytes()).await.unwrap();
        if v > 2 {
            rec.delete(&key(v - 2, "a")).await.unwrap();
        }
        // A read of its own write.
        assert_eq!(
            rec.get(&key(v, "a")).await.unwrap(),
            Some(v.to_be_bytes().to_vec())
        );
    }
    c.commit().await.unwrap()
}

/// The rows a load at `version` must see, from the changes' rule.
fn expected(versions: &[u64], at: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut m = BTreeMap::new();
    for &v in versions.iter().filter(|&&v| v <= at) {
        m.insert(key(v, "a"), v.to_be_bytes().to_vec());
        m.insert(key(v, "b"), v.to_be_bytes().to_vec());
        if v > 2 {
            m.remove(&key(v - 2, "a"));
        }
    }
    m
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_module_records_follow_the_catalog_version_through_crashes() {
    with_schema("staged", |pool, schema| async move {
        migrate(&pool, &schema).await;
        with_global(|g| async move {
            let tenant = unique_tenant();
            seed_directory(&*g, tenant).await;
            let mem = MemRecords::new();
            let fail_apply = Arc::new(AtomicBool::new(false));
            let records: Arc<dyn RecordStore> = Arc::new(Flaky {
                inner: mem.clone(),
                fail_apply: Arc::clone(&fail_apply),
            });
            let loc = locator(&pool, &schema, tenant);
            let cat: MariaCatalog = Versioner::new(
                MariaBackend::new(Arc::clone(&loc)).with_records(Arc::clone(&records)),
                Arc::new(DirectoryAllocator::new(Arc::clone(&g))),
            );
            let source = Arc::new(
                MariaSource::new(Arc::clone(&loc), vec![Arc::new(Ext)])
                    .unwrap()
                    .with_records(Arc::clone(&records)),
            );

            // Changes, some with the engine's apply failing (as a crash
            // right after MariaDB's commit), and a reader checking every
            // load sees exactly the rows of its version.
            let stop = Arc::new(AtomicBool::new(false));
            let committed = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
            let reader = {
                let source = Arc::clone(&source);
                let stop = Arc::clone(&stop);
                let committed = Arc::clone(&committed);
                tokio::spawn(async move {
                    let mut loads = 0;
                    while !stop.load(Ordering::Relaxed) {
                        let (at, rows) = load(&source, tenant).await;
                        let known: Vec<u64> = committed.lock().unwrap().clone();
                        // Changes still committing are above `known`; the
                        // rule only checks versions the writer confirmed or
                        // the load's own versions all being consistent.
                        let mine: Vec<u64> = (1..=at).collect();
                        let want = expected(&mine, at);
                        assert_eq!(rows, want, "load at v{at} (known {})", known.len());
                        loads += 1;
                    }
                    loads
                })
            };
            let mut versions = Vec::new();
            for i in 0..60 {
                fail_apply.store(i % 4 == 1, Ordering::SeqCst);
                let v = change(&cat, tenant).await;
                versions.push(v);
                committed.lock().unwrap().push(v);
            }
            fail_apply.store(false, Ordering::SeqCst);
            stop.store(true, Ordering::Relaxed);
            let loads = reader.await.unwrap();
            assert!(loads > 0);
            assert_eq!(versions, (1..=60).collect::<Vec<_>>());

            // The last change's apply succeeded; the engine holds exactly
            // the rows of version 60, its mark at the log's last row.
            let mut tx = mem.begin(tenant).await.unwrap();
            let engine: BTreeMap<_, _> = scan_all(&mut *tx, PREFIX, &prefix_end(PREFIX))
                .await
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(engine, expected(&versions, 60));
            let mark = applied(&mut *tx).await.unwrap();
            drop(tx);
            let mut c = pool.get_conn().await.unwrap();
            let last: Option<u64> = c
                .query_first(format!("SELECT MAX(seq) FROM `{schema}`.rec_log"))
                .await
                .unwrap();
            assert_eq!(Some(mark), last);

            // A crash after the last apply failed: the loader overlays
            // the log row, the engine lacks it until the next change.
            fail_apply.store(true, Ordering::SeqCst);
            let v61 = change(&cat, tenant).await;
            fail_apply.store(false, Ordering::SeqCst);
            versions.push(v61);
            let (at, rows) = load(&source, tenant).await;
            assert_eq!(at, v61);
            assert_eq!(rows, expected(&versions, v61));
            let mut tx = mem.begin(tenant).await.unwrap();
            assert_eq!(tx.get(&key(v61, "a")).await.unwrap(), None);
            drop(tx);

            // A change cut off before MariaDB's commit (failpoint as a
            // crash): no log row, no engine rows, a gap that settles.
            let mut c = cat.change(tenant, VersionKind::Catalog).await.unwrap();
            let lost = c.version();
            c.tx()
                .records()
                .await
                .unwrap()
                .put(&key(lost, "a"), b"x")
                .await
                .unwrap();
            // The engine caught up with v61 when the change began.
            let mut tx = mem.begin(tenant).await.unwrap();
            assert!(tx.get(&key(v61, "a")).await.unwrap().is_some());
            drop(tx);
            c.abandon().await.unwrap();
            cat.settle(tenant, VersionKind::Catalog).await.unwrap();
            let (at, rows) = load(&source, tenant).await;
            assert_eq!(at, lost);
            assert_eq!(rows, expected(&versions, v61));

            // Purge: rows applied and old enough go; the purge mark moves;
            // loads stay exact.
            let mut q = pool.get_conn().await.unwrap();
            q.query_drop(format!(
                "UPDATE `{schema}`.rec_log SET created_at = created_at - INTERVAL 1 HOUR"
            ))
            .await
            .unwrap();
            drop(q);
            let v = change(&cat, tenant).await;
            versions.push(v);
            let mut q = pool.get_conn().await.unwrap();
            let left: u64 = q
                .query_first(format!("SELECT COUNT(*) FROM `{schema}`.rec_log"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(left, 1, "only the newest row stays");
            let (at, rows) = load(&source, tenant).await;
            assert_eq!(rows, expected(&versions, at));
        })
        .await;
    })
    .await;
}
