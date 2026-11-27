//! Reads on copies: three engines over an in-process network, reads on the
//! replicas that don't lead the unit. Exact reads share read barriers;
//! bounded reads are served without asking the leader while the copy is
//! fresh enough, and never miss the session's token.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::time::SystemClock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::reads::{self, ReadMode, ReadPolicy};
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};

const TENANT: u64 = 5151;
const VOTERS: [u64; 3] = [1, 2, 3];
const WAIT: Duration = Duration::from_secs(15);

fn fast() -> RaftConfig {
    RaftConfig {
        tick: Duration::from_millis(10),
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        static_voters: VOTERS.to_vec(),
        ..RaftConfig::default()
    }
}

fn key(tenant: u64, i: u64) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

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
        let dir = std::env::temp_dir().join(format!("sprig-fr-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
        };
        for id in VOTERS {
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: u64) {
        let d = self.dir.join(format!("n{id}"));
        let mut cfg = EngineConfig::new(d.join("engine"));
        cfg.store = StoreKind::Rocks;
        cfg.checkpoint_interval = Duration::ZERO;
        cfg.log.sync = SyncMode::Off;
        cfg.replicated = true;
        let engine = Engine::open(cfg).unwrap();
        let store = EngineLogStore::open(d.join("raft"), SyncMode::Off, None).unwrap();
        let node = EngineNode::open(
            &engine,
            EngineNodeOptions {
                id,
                config: fast(),
                store,
                transport: self.net.transport(id),
                clock: Arc::new(SystemClock),
                seed: id,
                observer: None,
                inline_roles: false,
            },
        )
        .unwrap();
        self.net.register(node.node());
        let thread = Some(node.spawn().unwrap());
        self.replicas.insert(
            id,
            Replica {
                engine,
                node,
                thread,
            },
        );
    }

    fn kill(&mut self, id: u64) {
        let mut r = self.replicas.remove(&id).unwrap();
        self.net.unregister(id);
        r.node.close();
        if let Some(t) = r.thread.take() {
            t.join().unwrap();
        }
        drop(r);
    }

    fn leader(&self, unit: UnitId) -> u64 {
        until("a leader", || {
            self.replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(unit) > 0)
                .map(|(id, _)| *id)
        })
    }

    /// Commits `k -> v` on the unit's leader; returns the commit timestamp.
    fn put(&self, unit: UnitId, k: &[u8], v: &[u8]) -> u64 {
        until("an acknowledged commit", || {
            let l = self.leader(unit);
            let e = &self.replicas[&l].engine;
            let mut t = e.begin(TxnOptions::default()).ok()?;
            t.put(k.to_vec(), Bytes::copy_from_slice(v)).ok()?;
            let c = t.commit().ok()?;
            let ts = c.cts().0;
            c.wait().ok()?;
            Some(ts)
        })
    }

    fn index_reads(&self, id: u64) -> u64 {
        self.replicas[&id].node.node().stats().index_reads
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let ids: Vec<u64> = self.replicas.keys().copied().collect();
        for id in ids {
            self.kill(id);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn bounded(max_lag_ms: u64, wait_ms: u64, token: u64) -> ReadPolicy {
    ReadPolicy::new(
        ReadMode::Bounded {
            max_lag: Duration::from_millis(max_lag_ms),
        },
        Duration::from_millis(wait_ms),
        token,
    )
}

/// A snapshot read of `k` on `e` under `policy`.
fn read(e: &Engine, policy: &ReadPolicy, k: &[u8]) -> sprig_common::Result<Option<Bytes>> {
    reads::sync_scope(policy.clone(), || {
        let mut t = e.begin(TxnOptions::default())?;
        t.get_snapshot(k)
    })
}

#[test]
fn bounded_reads_on_a_fresh_copy_need_no_round_trip() {
    let c = Cluster::new("bounded-local");
    let unit = Engine::unit_of_tenant(TENANT + 1);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let k = key(TENANT + 1, 1);
    let ts = c.put(unit, &k, b"v1");
    let e = &c.replicas[&f].engine;
    let p = bounded(10_000, 200, ts);
    // The first read asks the leader once (nothing marks the copy fresh
    // yet); the next ones are served here.
    assert_eq!(read(e, &p, &k).unwrap().as_deref(), Some(&b"v1"[..]));
    let before = c.index_reads(f);
    for _ in 0..200 {
        assert_eq!(read(e, &p, &k).unwrap().as_deref(), Some(&b"v1"[..]));
    }
    assert_eq!(c.index_reads(f), before, "bounded reads asked the leader");
    assert!(p.token() >= ts, "the token never moves back");
    // Exact reads still ask (one barrier per read view, coalesced).
    let exact = ReadPolicy::new(ReadMode::Exact, Duration::ZERO, 0);
    for _ in 0..5 {
        assert_eq!(read(e, &exact, &k).unwrap().as_deref(), Some(&b"v1"[..]));
    }
    assert!(c.index_reads(f) >= before + 5);
}

#[test]
fn bounded_reads_never_miss_the_token() {
    let c = Cluster::new("bounded-token");
    let unit = Engine::unit_of_tenant(TENANT + 2);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let followers: Vec<u64> = VOTERS.into_iter().filter(|id| *id != l).collect();
    // Mark the copies fresh, so reads take the no-round-trip path whenever
    // they hold the token.
    let warm = key(TENANT + 2, 0);
    c.put(unit, &warm, b"w");
    for f in &followers {
        read(&c.replicas[f].engine, &bounded(60_000, 100, 0), &warm).unwrap();
    }
    let mut served_before = reads::stats().get();
    for i in 1..=400u64 {
        let k = key(TENANT + 2, i % 37);
        let v = format!("v{i}");
        let ts = c.put(unit, &k, v.as_bytes());
        // At once, on a copy, with the writer's token.
        let f = followers[usize::try_from(i).unwrap() % followers.len()];
        let p = bounded(60_000, 100, ts);
        let got = read(&c.replicas[&f].engine, &p, &k).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(v.as_bytes()),
            "copy {f} read an older value than the token {ts} (write {i})"
        );
        assert!(p.token() >= ts);
    }
    let served = reads::stats().get();
    served_before = (
        served.0 - served_before.0,
        served.1 - served_before.1,
        served.2 - served_before.2,
    );
    eprintln!("served (local, waited, barrier) = {served_before:?}");
    // Most reads found their write applied already or after a short wait,
    // not through a barrier.
    assert!(
        served_before.0 + served_before.1 > served_before.2,
        "{served_before:?}"
    );
}

#[test]
fn concurrent_exact_reads_share_barriers() {
    let c = Cluster::new("coalesce");
    let unit = Engine::unit_of_tenant(TENANT + 3);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let k = key(TENANT + 3, 1);
    c.put(unit, &k, b"x");
    let e = c.replicas[&f].engine.clone();
    let before = c.index_reads(f);
    let threads = 16;
    let per = 50;
    let hs: Vec<_> = (0..threads)
        .map(|_| {
            let (e, k) = (e.clone(), k.clone());
            std::thread::spawn(move || {
                for _ in 0..per {
                    let mut t = e.begin(TxnOptions::default()).unwrap();
                    assert_eq!(t.get_snapshot(&k).unwrap().as_deref(), Some(&b"x"[..]));
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let asked = c.index_reads(f) - before;
    eprintln!("{} exact reads, {asked} read barriers", threads * per);
    assert!(asked < threads * per / 2, "{asked} barriers");
}

#[test]
fn a_writer_and_readers_on_copies_never_see_less_than_their_token() {
    let c = Cluster::new("rw-random");
    let unit = Engine::unit_of_tenant(TENANT + 4);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let followers: Vec<u64> = VOTERS.into_iter().filter(|id| *id != l).collect();
    let stop = Arc::new(AtomicBool::new(false));
    // A counter key the writer bumps; readers check each value they see
    // against the token they hold (their own last view and the writer's
    // published timestamps).
    let k = key(TENANT + 4, 1);
    c.put(unit, &k, &0u64.to_be_bytes());
    let published = Arc::new(AtomicU64::new(0));
    let published_ts = Arc::new(AtomicU64::new(0));
    let le = c.replicas[&l].engine.clone();
    let writer = {
        let (stop, published, published_ts, k) = (
            Arc::clone(&stop),
            Arc::clone(&published),
            Arc::clone(&published_ts),
            k.clone(),
        );
        std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let mut t = le.begin(TxnOptions::default()).unwrap();
                t.put(k.clone(), Bytes::copy_from_slice(&n.to_be_bytes()))
                    .unwrap();
                let ct = t.commit().unwrap();
                let ts = ct.cts().0;
                ct.wait().unwrap();
                // Acknowledged: from now on any reader holding ts must see n.
                published_ts.store(ts, Ordering::Release);
                published.store(n, Ordering::Release);
            }
            n
        })
    };
    let mut readers = Vec::new();
    for (i, f) in followers.iter().enumerate() {
        for j in 0..3 {
            let e = c.replicas[f].engine.clone();
            let (stop, published, published_ts, k) = (
                Arc::clone(&stop),
                Arc::clone(&published),
                Arc::clone(&published_ts),
                k.clone(),
            );
            let lag = if (i + j) % 2 == 0 { 60_000 } else { 50 };
            readers.push(std::thread::spawn(move || {
                let p = bounded(lag, 20, 0);
                let mut last_seen = 0u64;
                let mut reads_done = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // Take the writer's latest acknowledged write as this
                    // read's token now and then, as a client passing a
                    // token from one connection to another would.
                    let (n, ts) = (
                        published.load(Ordering::Acquire),
                        published_ts.load(Ordering::Acquire),
                    );
                    let must = if reads_done.is_multiple_of(3) {
                        p.raise(ts);
                        n
                    } else {
                        0
                    };
                    let Ok(v) = read(&e, &p, &k) else {
                        continue;
                    };
                    let got = u64::from_be_bytes(v.unwrap().as_ref().try_into().unwrap());
                    assert!(got >= must, "read {got} with a token for {must}");
                    assert!(got >= last_seen, "went back from {last_seen} to {got}");
                    last_seen = got;
                    reads_done += 1;
                }
                reads_done
            }));
        }
    }
    std::thread::sleep(Duration::from_secs(4));
    stop.store(true, Ordering::Relaxed);
    let wrote = writer.join().unwrap();
    let copy_reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    eprintln!("{wrote} writes, {copy_reads} reads on copies");
    assert!(wrote > 50 && copy_reads > 100);
}

#[test]
fn an_isolated_copy_serves_bounded_reads_only_within_the_bound() {
    let c = Cluster::new("isolated-bound");
    let unit = Engine::unit_of_tenant(TENANT + 5);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let k = key(TENANT + 5, 1);
    c.put(unit, &k, b"old");
    let e = c.replicas[&f].engine.clone();
    let p = bounded(400, 20, 0);
    assert_eq!(read(&e, &p, &k).unwrap().as_deref(), Some(&b"old"[..]));
    c.net.isolate(f, true);
    // Inside the bound: served from what the copy holds.
    assert_eq!(read(&e, &p, &k).unwrap().as_deref(), Some(&b"old"[..]));
    std::thread::sleep(Duration::from_millis(450));
    // Past it, the copy can't confirm it is fresh: retryable error, not
    // an old answer.
    let err = read(&e, &p, &k).expect_err("a copy cut off past its bound");
    assert_eq!(
        err.code(),
        Some(sprig_common::SprigCode::ShardMoved),
        "{err}"
    );
    c.net.isolate(f, false);
    until("bounded reads again", || read(&e, &p, &k).ok());
}

#[test]
fn a_restarted_copy_serves_bounded_reads_of_an_idle_unit_without_round_trips() {
    let mut c = Cluster::new("restarted-idle");
    let unit = Engine::unit_of_tenant(TENANT + 6);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let k = key(TENANT + 6, 1);
    c.put(unit, &k, b"v");
    until("the copy applied it", || {
        (c.replicas[&f].engine.unit_readable_ts(unit) > 0).then_some(())
    });
    // Restarted, the copy has applied nothing of the unit since it opened.
    c.kill(f);
    c.start(f);
    let e = c.replicas[&f].engine.clone();
    let p = bounded(10_000, 200, 0);
    until("a first bounded read on the restarted copy", || {
        read(&e, &p, &k).ok()
    });
    let before = c.index_reads(f);
    for _ in 0..100 {
        assert_eq!(read(&e, &p, &k).unwrap().as_deref(), Some(&b"v"[..]));
    }
    assert_eq!(
        c.index_reads(f),
        before,
        "an idle unit's bounded reads asked the leader every time"
    );
}
