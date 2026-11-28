//! The engine document store on a replicated engine: three engines over
//! Raft (an in-process network). A tenant's MongoDB writes touch only its
//! own unit: creating its first collection must not also write a key in
//! unit 0 (another unit, led by another node or by none), and the TTL
//! sweeper still finds the tenant on a node that never served it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use sprig_common::time::SystemClock;
use sprig_doc::docstore::DocStore;
use sprig_doc::docstore::cmd::Reply;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::CommitPath;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};

const TENANT: u64 = 5151;
const VOTERS: [u64; 3] = [1, 2, 3];
const WAIT: Duration = Duration::from_secs(20);

struct Replica {
    engine: Engine,
    node: EngineNode,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Cluster {
    dir: PathBuf,
    net: Arc<MemNetwork>,
    replicas: BTreeMap<u64, Replica>,
}

fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

impl Cluster {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("sprig-docrepl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
        };
        for id in VOTERS {
            let d = c.dir.join(format!("n{id}"));
            let mut cfg = EngineConfig::new(d.join("engine"));
            cfg.store = StoreKind::Rocks;
            cfg.checkpoint_interval = Duration::ZERO;
            cfg.log.sync = SyncMode::Off;
            cfg.budget.engine_mb = 64;
            cfg.replicated = true;
            let engine = Engine::open(cfg).unwrap();
            let store = EngineLogStore::open(d.join("raft"), SyncMode::Off, None).unwrap();
            let node = EngineNode::open(
                &engine,
                EngineNodeOptions {
                    id,
                    config: RaftConfig {
                        tick: Duration::from_millis(10),
                        lease: Duration::from_millis(90),
                        lease_margin: Duration::from_millis(10),
                        liveness_interval: Duration::from_millis(30),
                        liveness_timeout: Duration::from_millis(150),
                        static_voters: VOTERS.to_vec(),
                        ..RaftConfig::default()
                    },
                    store,
                    transport: c.net.transport(id),
                    clock: Arc::new(SystemClock),
                    seed: id,
                    observer: None,
                    inline_roles: false,
                },
            )
            .unwrap();
            c.net.register(node.node());
            let thread = Some(node.spawn().unwrap());
            c.replicas.insert(
                id,
                Replica {
                    engine,
                    node,
                    thread,
                },
            );
        }
        c
    }

    /// The replica whose engine leads `unit`.
    fn leader(&self, unit: UnitId) -> u64 {
        until("a leader", || {
            self.replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(unit) > 0)
                .map(|(id, _)| *id)
        })
    }

    /// Moves `unit`'s leadership to `to` and waits until its engine leads.
    fn move_to(&self, unit: UnitId, to: u64) {
        until("the leadership to move", || {
            if self.replicas[&to].engine.leader_epoch(unit) > 0 {
                return Some(());
            }
            let l = self.leader(unit);
            if l != to {
                let _ = self.replicas[&l].node.node().transfer_leader(unit.0, to);
            }
            std::thread::sleep(Duration::from_millis(20));
            None
        });
    }

    fn store(&self, id: u64) -> DocStore {
        DocStore::new(Arc::new(self.replicas[&id].engine.clone()) as Arc<dyn CommitPath>)
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, mut r) in std::mem::take(&mut self.replicas) {
            r.node.close();
            if let Some(t) = r.thread.take() {
                t.join().unwrap();
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Runs a command; an error comes back as MongoDB's error reply.
async fn run(s: &DocStore, body: Document) -> Document {
    let name = body.keys().next().cloned().unwrap();
    let mut body = body;
    body.insert("$db", "d");
    match Box::pin(s.command(TENANT, "u", "d", &name, &body)).await {
        Ok(Reply::Doc(d)) => d,
        Ok(Reply::Cursor { first, .. }) => doc! {"ok": 1.0, "firstBatch": first},
        Err(e) => e.to_doc(),
    }
}

fn ok(r: &Document) -> bool {
    r.get_f64("ok").is_ok_and(|x| x > 0.5) && !r.contains_key("writeErrors")
}

/// A tenant's first collection, on the node leading the tenant's unit while
/// another node leads unit 0: created on the first try, and unit 0's
/// leadership stays where it was (before the fix, the collection's
/// creation wrote a registry key in unit 0 too, a commit across two units
/// refused as "led elsewhere" and pulling unit 0's leadership over).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenants_first_collection_touches_only_its_own_unit() {
    let c = Cluster::new("first");
    let (u0, ut) = (UnitId(0), Engine::unit_of_tenant(TENANT));
    for u in [u0, ut] {
        c.replicas[&1].node.start_unit(u, &VOTERS).unwrap();
    }
    c.move_to(u0, 1);
    c.move_to(ut, 2);
    let s = c.store(2);
    let r = run(
        &s,
        doc! {"insert": "c", "documents": [{"_id": 1, "v": "a"}, {"_id": 2, "v": "b"}]},
    )
    .await;
    assert!(ok(&r), "the first insert (creating the collection): {r}");
    let r = run(&s, doc! {"create": "made"}).await;
    assert!(ok(&r), "an explicit create: {r}");
    let r = run(&s, doc! {"find": "c", "filter": {}}).await;
    assert_eq!(
        r.get_array("firstBatch").map(Vec::len).unwrap_or(0),
        2,
        "{r}"
    );
    // Nothing asked for unit 0's leadership.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(c.leader(u0), 1, "unit 0's leadership moved");
    assert_eq!(c.leader(ut), 2);
    // The tenant's other replicas hold the documents.
    for id in VOTERS {
        until("the collection on every replica", || {
            let n = c.replicas[&id]
                .engine
                .unit_checksum(ut)
                .ok()
                .map(|s| s.entries)?;
            (n > 0).then_some(())
        });
    }
}

/// The TTL sweeper of a node that takes over a tenant's unit but never
/// served the tenant finds it (no registry key on a replicated engine: the
/// tenants are those of the units the node holds) and deletes its expired
/// documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ttl_sweeper_finds_tenants_from_the_units_held() {
    let c = Cluster::new("ttl");
    let (u0, ut) = (UnitId(0), Engine::unit_of_tenant(TENANT));
    for u in [u0, ut] {
        c.replicas[&1].node.start_unit(u, &VOTERS).unwrap();
    }
    c.move_to(u0, 1);
    c.move_to(ut, 2);
    let s = c.store(2);
    s.set_clock(Some(1_000_000));
    let r = run(
        &s,
        doc! {"createIndexes": "t", "indexes": [{"key": {"at": 1}, "name": "at", "expireAfterSeconds": 10}]},
    )
    .await;
    assert!(ok(&r), "{r}");
    let at = |ms: i64| Bson::DateTime(bson::DateTime::from_millis(ms));
    let r = run(
        &s,
        doc! {"insert": "t", "documents": [{"_id": 1, "at": at(900_000)}, {"_id": 2, "at": at(995_000)}]},
    )
    .await;
    assert!(ok(&r), "{r}");
    // Node 3 takes the tenant's unit; its store has served nobody.
    c.move_to(ut, 3);
    let s3 = c.store(3);
    s3.set_clock(Some(1_000_000));
    assert!(s3.tenants().contains(&TENANT), "{:?}", s3.tenants());
    let deadline = Instant::now() + WAIT;
    let n = loop {
        let n = s3.sweep_ttl(100).await.unwrap_or(0);
        if n > 0 {
            break n;
        }
        assert!(Instant::now() < deadline, "the sweep on the new leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(n, 1);
    let r = run(&s3, doc! {"find": "t", "filter": {}}).await;
    let left: Vec<i32> = r
        .get_array("firstBatch")
        .unwrap()
        .iter()
        .map(|d| d.as_document().unwrap().get_i32("_id").unwrap())
        .collect();
    assert_eq!(left, vec![2], "{r}");
    assert_eq!(c.leader(u0), 1, "unit 0's leadership moved");
}
