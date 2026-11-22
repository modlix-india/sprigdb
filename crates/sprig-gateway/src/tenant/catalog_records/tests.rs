//! Catalog records on a real engine: locks, read views, versioned changes
//! (one engine transaction with their version bump, durable across a
//! restart, nothing left by a transaction that never committed), and the
//! same through the peer service from another "process".

#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_catalog::records::remote::RemoteRecords;
use sprig_catalog::records::{
    AuthCatalog, Pk, RecBackend, RecordStore, RowTable, next_ids, read_version,
};
use sprig_catalog::{DirectoryAllocator, VersionKind, Versioner};
use sprig_common::ids::TenantId;
use sprig_engine::{EngineConfig, StoreKind};
use sprig_meta::{MemStore, MetaStore, Op, Tier, Txn};

use super::*;

const T: TenantId = TenantId(61_306_000_901);

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!(
            "sprig-catrec-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(dir: &Path, store: StoreKind) -> sprig_engine::Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.store = store;
    cfg.checkpoint_interval = Duration::ZERO;
    sprig_engine::Engine::open(cfg).unwrap()
}

async fn directory() -> Arc<MemStore> {
    let g = Arc::new(MemStore::new(Tier::Global));
    let key = Tier::Global.key(&format!("tenants/{T}")).unwrap();
    let body = json!({"v": 1, "tenant_id": T.0, "name": "s2", "state": "active",
                      "home_dc": "dc-a", "catalog_version": 0, "auth_version": 0});
    let r = g
        .txn(Txn {
            then: vec![Op::Put {
                key,
                value: serde_json::to_vec(&body).unwrap(),
                lease: None,
            }],
            ..Txn::default()
        })
        .await
        .unwrap();
    assert!(r.succeeded);
    g
}

fn catalog(g: &Arc<MemStore>, store: Arc<dyn RecordStore>) -> AuthCatalog {
    let alloc = DirectoryAllocator::new(Arc::clone(g) as Arc<dyn MetaStore>)
        .with_timeout(Duration::from_millis(500));
    Versioner::new(RecBackend::new(store), Arc::new(alloc))
}

const ROWS: RowTable = RowTable::new("test", "rows");

#[tokio::test]
async fn first_writers_of_a_missing_record_queue_instead_of_deadlocking() {
    let d = TempDir::new("queue");
    let recs = EngineRecords::on(open(&d.0, StoreKind::Mem));
    let mut a = recs.begin(T).await.unwrap();
    assert_eq!(a.get_for_update(b"v/auth").await.unwrap(), None);
    let recs2 = recs.clone();
    let b = tokio::spawn(async move {
        let mut b = recs2.begin(T).await.unwrap();
        let v = b.get_for_update(b"v/auth").await.unwrap();
        b.put(b"v/auth", b"2").await.unwrap();
        b.commit().await.unwrap();
        v
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!b.is_finished(), "the second writer waits for the lock");
    a.put(b"v/auth", b"1").await.unwrap();
    a.commit().await.unwrap();
    assert_eq!(b.await.unwrap().as_deref(), Some(&b"1"[..]));
    let mut c = recs.begin(T).await.unwrap();
    assert_eq!(c.get(b"v/auth").await.unwrap().as_deref(), Some(&b"2"[..]));
}

#[tokio::test]
async fn a_read_view_is_stable_and_tenants_are_apart() {
    let d = TempDir::new("view");
    let recs = EngineRecords::on(open(&d.0, StoreKind::Mem));
    let mut w = recs.begin(T).await.unwrap();
    w.put(b"m/x/1", b"a").await.unwrap();
    w.commit().await.unwrap();
    let mut r = recs.begin(T).await.unwrap();
    assert_eq!(r.get(b"m/x/1").await.unwrap().as_deref(), Some(&b"a"[..]));
    let mut w = recs.begin(T).await.unwrap();
    w.put(b"m/x/2", b"b").await.unwrap();
    w.commit().await.unwrap();
    let rows = r.scan(b"m/x/", b"m/x0", 100).await.unwrap();
    assert_eq!(rows.len(), 1, "the reader's view predates the second write");
    let mut other = recs.begin(TenantId(T.0 + 1)).await.unwrap();
    assert!(other.scan(b"", b"\xff", 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_change_survives_a_restart_and_an_uncommitted_one_leaves_nothing() {
    let d = TempDir::new("restart");
    let g = directory().await;
    let v = {
        let eng = open(&d.0, StoreKind::Rocks);
        let cat = catalog(&g, Arc::new(EngineRecords::on(eng.clone())));
        let mut ch = cat.change(T, VersionKind::Auth).await.unwrap();
        let v = ch.version();
        let id = next_ids(ch.tx().records(), "row", 1).await.unwrap();
        ROWS.put(ch.tx().records(), &Pk::new().u64(id), &json!({"v": v}))
            .await
            .unwrap();
        ch.commit().await.unwrap();
        // A second change is written and dropped without a commit (as a
        // crashed caller's): rolled back, its version a gap.
        let mut lost = cat.change(T, VersionKind::Auth).await.unwrap();
        ROWS.put(
            lost.tx().records(),
            &Pk::new().u64(99),
            &json!({"lost": true}),
        )
        .await
        .unwrap();
        drop(lost);
        drop(cat);
        drop(eng);
        v
    };
    let eng = open(&d.0, StoreKind::Rocks);
    let recs = EngineRecords::on(eng);
    let mut r = recs.begin(T).await.unwrap();
    assert_eq!(read_version(&mut *r, VersionKind::Auth).await.unwrap(), v);
    let rows: Vec<serde_json::Value> = ROWS.rows(&mut *r, &Pk::new()).await.unwrap();
    assert_eq!(rows, vec![json!({"v": v})]);
    // The gap the lost change left closes, and changes go on above it.
    let cat = catalog(&g, Arc::new(recs));
    let settled = cat.settle(T, VersionKind::Auth).await.unwrap();
    assert!(settled > v);
    let ch = cat.change(T, VersionKind::Auth).await.unwrap();
    assert!(ch.version() > settled);
    ch.commit().await.unwrap();
}

async fn serve(svc: CatalogService) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let routes = tonic::service::Routes::new(CatalogPeerServer::new(svc)).prepare();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let routes = routes.clone();
            tokio::spawn(async move {
                let svc = hyper_util::service::TowerToHyperService::new(routes);
                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .http2_only()
                .serve_connection(hyper_util::rt::TokioIo::new(sock), svc)
                .await;
            });
        }
    });
    addr.to_string()
}

#[tokio::test]
async fn remote_changes_through_the_peer_service() {
    let d = TempDir::new("remote");
    let recs = EngineRecords::on(open(&d.0, StoreKind::Mem));
    let addr = serve(CatalogService::on(recs.clone())).await;
    let remote: Arc<dyn RecordStore> = Arc::new(RemoteRecords::at(addr, None));
    let g = directory().await;
    let cat = catalog(&g, Arc::clone(&remote));
    let mut ch = cat.change(T, VersionKind::Auth).await.unwrap();
    let v = ch.version();
    ROWS.put(ch.tx().records(), &Pk::new().u32(1), &json!({"n": 1}))
        .await
        .unwrap();
    ch.commit().await.unwrap();
    // Seen locally, at the same version.
    let mut l = recs.begin(T).await.unwrap();
    assert_eq!(read_version(&mut *l, VersionKind::Auth).await.unwrap(), v);
    // A remote transaction dropped without a commit (its caller died)
    // leaves nothing and releases its locks.
    {
        let mut ch = cat.change(T, VersionKind::Auth).await.unwrap();
        ROWS.put(ch.tx().records(), &Pk::new().u32(2), &json!({"n": 2}))
            .await
            .unwrap();
        drop(ch);
    }
    let ch = tokio::time::timeout(Duration::from_secs(5), cat.change(T, VersionKind::Auth))
        .await
        .expect("the dropped stream's lock was released")
        .unwrap();
    ch.commit().await.unwrap();
    let mut r = remote.begin(T).await.unwrap();
    let rows: Vec<serde_json::Value> = ROWS.rows(&mut *r, &Pk::new()).await.unwrap();
    assert_eq!(rows, vec![json!({"n": 1})]);
    // A lock held by another transaction answers 39071 after the wait,
    // and the remote transaction stays usable.
    let mut holder = recs.begin(T).await.unwrap();
    holder.get_for_update(b"ctr/x").await.unwrap();
    assert_eq!(r.get(b"ctr/none").await.unwrap(), None);
    drop(holder);
}

/// A scan whose start falls between two records begins at the next record
/// (the overlay of unapplied log rows pages through the base this way),
/// on both stores.
#[tokio::test]
async fn a_scan_from_between_two_records_starts_after_them() {
    for (name, kind) in [
        ("between-mem", StoreKind::Mem),
        ("between-rocks", StoreKind::Rocks),
    ] {
        let d = TempDir::new(name);
        let recs = EngineRecords::on(open(&d.0, kind));
        let mut w = recs.begin(T).await.unwrap();
        for k in [b"m/g/1".as_slice(), b"m/g/2", b"m/g/3"] {
            w.put(k, b"v").await.unwrap();
        }
        w.commit().await.unwrap();
        // Read once committed, and again from a fresh transaction.
        let mut r = recs.begin(T).await.unwrap();
        let all = r.scan(b"m/g/", b"m/g0", 100).await.unwrap();
        assert_eq!(all.len(), 3, "{name}");
        let after = r.scan(b"m/g/3\0", b"m/g0", 100).await.unwrap();
        assert!(after.is_empty(), "{name}: {after:?}");
        let mid = r.scan(b"m/g/1\0", b"m/g0", 100).await.unwrap();
        assert_eq!(
            mid.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
            vec![b"m/g/2".to_vec(), b"m/g/3".to_vec()],
            "{name}"
        );
    }
}
