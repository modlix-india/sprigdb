//! Shared helpers for the backup tests.

#![allow(dead_code, clippy::unwrap_used, clippy::unused_self, unreachable_pub)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sprig_backup::archive::ArchiveConfig;
use sprig_backup::crypt::{DataKey, KeyProvider, derive};
use sprig_backup::service::{Policy, ServiceConfig};
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{ReadMode, TxnOptions};
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};

/// A temporary directory, removed when dropped.
pub struct Dir(pub PathBuf);

impl Dir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-backup-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        if std::env::var_os("KEEP_TEST_DIRS").is_none() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// A fixed test key.
pub struct TestKeys;

impl TestKeys {
    pub fn key(&self, tenant: u64) -> DataKey {
        derive(&[1u8; 32], tenant).unwrap()
    }
}

impl KeyProvider for TestKeys {
    fn key_for(&self, tenant: u64) -> sprig_common::Result<DataKey> {
        derive(&[1u8; 32], tenant)
    }
}

pub fn engine_cfg(dir: &Path, store: StoreKind) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.store = store;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    c
}

pub fn open_engine(dir: &Path, store: StoreKind) -> Engine {
    Engine::open(engine_cfg(dir, store)).unwrap()
}

/// The service settings for tests: backups under `<dir>/backups`.
pub fn backup_cfg(d: &Dir) -> ServiceConfig {
    backup_cfg_at(&d.0)
}

/// [`backup_cfg`] for a path.
pub fn backup_cfg_at(dir: &Path) -> ServiceConfig {
    ServiceConfig {
        work_dir: dir.join("work"),
        default_location: dir.join("backups").display().to_string(),
        tick: Duration::from_millis(50),
        archive: ArchiveConfig::default(),
        policy: Arc::new(|_| Policy::default()),
        daily_hour: Arc::new(|| 0),
        rate_mb_per_sec: Arc::new(|| 0),
    }
}

/// A primary key of `tenant`.
pub fn key(tenant: u64, id: u32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 3,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// One transaction: puts (`Some`) and deletes (`None`).
pub fn commit(e: &Engine, ops: &[(Vec<u8>, Option<String>)]) {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    for (k, v) in ops {
        match v {
            Some(v) => t
                .put(k.clone(), Bytes::copy_from_slice(v.as_bytes()))
                .unwrap(),
            None => t.delete(k.clone()).unwrap(),
        }
    }
    t.commit().unwrap().wait().unwrap();
}

pub fn get(e: &Engine, k: &[u8]) -> Option<Bytes> {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(k, ReadMode::Snapshot)
        .unwrap()
        .expect_ready()
        .unwrap()
}

/// Waits until the store applied everything durable of `tenant`'s unit.
pub fn settle(e: &Engine, tenant: u64) {
    let u = UnitId(tenant);
    let c = e.consensus();
    while c.applied_index(u) < c.durable_index(u) {
        std::thread::sleep(Duration::from_millis(1));
    }
}
