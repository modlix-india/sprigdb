//! Shared setup for the transaction tests.
#![allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::missing_panics_doc
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{CommitPath, Txn, TxnOptions, TxnShared, UnitConfig};
use sprig_engine::{Engine, EngineConfig, StoreKind};

/// A transaction layer over some commit path.
#[derive(Clone)]
pub struct Bed {
    pub path: Arc<dyn CommitPath>,
    pub mem: Option<Arc<MemPath>>,
}

impl Bed {
    pub fn begin(&self, opts: TxnOptions) -> Txn {
        Txn::begin_on(self.path.clone(), opts)
    }

    pub fn shared(&self) -> &TxnShared {
        self.path.shared()
    }

    pub fn locks(&self) -> &Arc<sprig_engine::locks::LockTable> {
        self.path.shared().locks()
    }

    pub fn mem(&self) -> &MemPath {
        self.mem.as_ref().unwrap()
    }
}

/// The in-memory path, with syncs taking `delay`.
pub fn mem_bed_with(cfg: UnitConfig, delay: Duration) -> Bed {
    let mem = MemPath::start(cfg, delay);
    Bed {
        path: mem.clone(),
        mem: Some(mem),
    }
}

pub fn bed() -> Bed {
    mem_bed_with(UnitConfig::default(), Duration::ZERO)
}

/// A directory for one engine, removed on drop.
pub struct Dir(pub PathBuf);

impl Dir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-txn-{tag}-{}-{}",
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
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Engine settings for tests: no background checkpoints, small budgets.
pub fn engine_cfg(dir: &Dir, store: StoreKind, sync: SyncMode) -> EngineConfig {
    let mut c = EngineConfig::new(&dir.0);
    c.store = store;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = sync;
    c.budget.engine_mb = 64;
    c
}

/// The real engine (RocksDB or `MemStore`, raft-engine log).
pub fn engine_bed(dir: &Dir, store: StoreKind, sync: SyncMode) -> (Engine, Bed) {
    let e = Engine::open(engine_cfg(dir, store, sync)).unwrap();
    let bed = Bed {
        path: Arc::new(e.clone()),
        mem: None,
    };
    (e, bed)
}

/// A prefix-free logical key, with the head every engine key has (tenant,
/// shard, object, kind, index). The engine reads a version's timestamp only
/// from keys with that head, to start its clock above every recovered
/// commit; a shorter key would be invisible after a restart that lands in
/// the same millisecond as its commit.
pub fn k(s: &str) -> Bytes {
    let mut v = Vec::with_capacity(sprig_engine::keys::PREFIX_LEN + s.len() + 1);
    sprig_engine::keys::KeyPrefix {
        tenant: 7,
        shard: 0,
        object: 1,
        kind: sprig_engine::keys::kind::PRIMARY,
        index: 0,
    }
    .encode_into(&mut v);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    Bytes::from(v)
}

pub fn v(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

pub fn errno(e: &sprig_common::SprigError) -> u16 {
    sprig_engine::txn::errors::errno(e).unwrap_or(0)
}

pub fn sqlstate(e: &sprig_common::SprigError) -> [u8; 5] {
    e.protocol().sqlstate
}
