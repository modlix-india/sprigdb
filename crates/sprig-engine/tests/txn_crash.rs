//! Crash test on the real engine (raft-engine log, RocksDB or `MemStore`),
//! with the store side's crash harness: clients write and read under early
//! lock release while the log is halted at a random point and cut back to
//! its synced bytes plus a random part of the unsynced tail; RocksDB loses
//! its memtables (WAL off, no flush on close). After recovery:
//! - no acknowledged write is lost;
//! - no acknowledged read returned data that was lost;
//! - no transaction is partly recovered.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

mod txn_common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use sprig_engine::keys::{self, KeyRange};
use sprig_engine::log::{SimDisk, SyncMode};
use sprig_engine::store::{Cf, IterOpts, SortedStore};
use sprig_engine::txn::mvcc::decode_value;
use sprig_engine::txn::{LockRequest, Locked, Ts, Txn, TxnOptions};
use sprig_engine::{Engine, StoreKind};
use txn_common::{Dir, engine_cfg, k};

const KEYS: usize = 4;

#[derive(Default)]
struct Acks {
    /// Acknowledged writes: keys, value, commit timestamp, log index.
    writes: Vec<(Vec<usize>, Bytes, Ts, u64)>,
    /// Acknowledged reads: key and the value returned.
    reads: Vec<(usize, Bytes)>,
}

fn key(i: usize) -> Bytes {
    k(&format!("k{i}"))
}

/// Every version of `logical` in `store`.
fn versions(store: &dyn SortedStore, logical: &[u8]) -> Vec<(Ts, Option<Bytes>)> {
    let mut it = store.iter(Cf::Data, IterOpts::range(&KeyRange::prefix(logical)));
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        if let Some((lk, ts)) = keys::split_version(it.key())
            && lk == logical
        {
            out.push((ts, decode_value(it.value())));
        }
        it.next();
    }
    out
}

fn client(engine: &Engine, acks: &Mutex<Acks>, stop: &AtomicBool, seed: u64, tid: u64) {
    let path: Arc<dyn sprig_engine::txn::CommitPath> = Arc::new(engine.clone());
    let mut x = seed * 1_000_003 + tid * 7 + 1;
    let mut rnd = move |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        usize::try_from(x % n).unwrap()
    };
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let mut t = Txn::begin_on(path.clone(), TxnOptions::mariadb());
        if rnd(3) == 0 {
            // Plain reads of two keys; the reply waits on the gate.
            let mut got = Vec::new();
            for _ in 0..2 {
                let ki = rnd(KEYS as u64);
                match t.get_snapshot(&key(ki)) {
                    Ok(Some(v)) => got.push((ki, v)),
                    Ok(None) => {}
                    Err(_) => return,
                }
            }
            let Ok(ticket) = t.commit() else { return };
            if ticket.wait().is_err() {
                return;
            }
            acks.lock().unwrap().reads.extend(got);
        } else {
            // A locking read then a write of one or two keys, one unique
            // value: the locking read may return a pending version.
            let a = rnd(KEYS as u64);
            let mut ks = vec![a];
            if rnd(2) == 0 {
                ks.push((a + 1 + rnd(KEYS as u64 - 1)) % KEYS);
            }
            ks.sort_unstable();
            let val = Bytes::from(format!("w{tid}.{n}"));
            let mut seen = Vec::new();
            for &ki in &ks {
                match t.lock_get_blocking(&key(ki), LockRequest::for_update()) {
                    Ok(Locked::Row(Some(v))) => seen.push((ki, v)),
                    Ok(_) => {}
                    Err(_) => return,
                }
                if t.put(key(ki), val.clone()).is_err() {
                    return;
                }
            }
            let Ok(ticket) = t.commit() else { return };
            if ticket.wait().is_err() {
                return;
            }
            let index = ticket
                .log_ticket()
                .map_or(0, sprig_engine::log::Ticket::index);
            let mut g = acks.lock().unwrap();
            g.writes.push((ks, val, ticket.commit_ts().unwrap(), index));
            // The locking reads' values were part of the acknowledged reply.
            g.reads.extend(seen);
        }
    }
}

fn crash_round(store: StoreKind, seed: u64, elr: bool) -> (usize, usize) {
    let dir = Dir::new("crash");
    let sim = SimDisk::new();
    let mut c = engine_cfg(&dir, store, SyncMode::Off);
    c.log.sim = Some(sim.clone());
    c.rocks.avoid_flush_on_close = true;
    let acks = Arc::new(Mutex::new(Acks::default()));
    let halt_ms = 5 + seed % 20;
    let durable_at_halt;
    {
        let e = Engine::open(c.clone()).unwrap();
        e.txn_shared().set_early_lock_release(elr);
        let stop = Arc::new(AtomicBool::new(false));
        let threads: Vec<_> = (0..6u64)
            .map(|tid| {
                let (e, acks, stop) = (e.clone(), acks.clone(), stop.clone());
                thread::spawn(move || client(&e, &acks, &stop, seed, tid))
            })
            .collect();
        thread::sleep(Duration::from_millis(halt_ms));
        e.log().halt();
        stop.store(true, Ordering::Relaxed);
        for t in threads {
            t.join().unwrap();
        }
        let s = e.stats();
        durable_at_halt = (s.durable_index, s.applied_index, s.log.syncs);
        // Before the crash, every acknowledged write must be in the store
        // (everything durable is applied once the writer has stopped).
        let st = e.store().as_ref();
        for (ks, val, cts, index) in &acks.lock().unwrap().writes {
            for ki in ks {
                let vs = versions(st, &key(*ki));
                assert!(
                    vs.iter()
                        .any(|(ts, v)| ts == cts && v.as_ref() == Some(val)),
                    "{store:?} seed {seed} elr {elr} dir {}: before the crash, acknowledged write {val:?} (log index {index}, cts {cts:?}) is not in the store; versions of k{ki}: {vs:?}",
                    dir.0.display()
                );
            }
        }
    }
    let cut = sim.crash(&dir.0.join("raft"), seed).unwrap();
    c.log.sim = None;
    let e = Engine::open(c).unwrap();
    let rec = e.stats();
    let st = e.store().as_ref();
    let all: HashMap<usize, Vec<(Ts, Option<Bytes>)>> =
        (0..KEYS).map(|i| (i, versions(st, &key(i)))).collect();
    let acks = acks.lock().unwrap();
    let max_acked = acks.writes.iter().map(|w| w.3).max().unwrap_or(0);
    let round = format!(
        "{store:?} seed {seed} elr {elr} in {}: halted after {halt_ms} ms; {} acked writes (highest log index {max_acked}), {} acked reads; at halt durable {} applied {} syncs {}; crash cut {cut} bytes; reopen replayed {} entries, applied index {}",
        dir.0.display(),
        acks.writes.len(),
        acks.reads.len(),
        durable_at_halt.0,
        durable_at_halt.1,
        durable_at_halt.2,
        rec.log.replayed,
        rec.applied_index,
    );
    eprintln!("{round}");
    for (ks, val, cts, index) in &acks.writes {
        for ki in ks {
            let found = all[ki]
                .iter()
                .any(|(ts, v)| ts == cts && v.as_ref() == Some(val));
            if !found {
                // Which later acknowledged entries survived tells a hole in
                // the log from a lost tail.
                let survivors: Vec<u64> = acks
                    .writes
                    .iter()
                    .filter(|w| w.3 > *index)
                    .filter(|w| {
                        w.0.iter().all(|k2| {
                            all[k2]
                                .iter()
                                .any(|(ts, v)| *ts == w.2 && v.as_ref() == Some(&w.1))
                        })
                    })
                    .map(|w| w.3)
                    .collect();
                // With SPRIG_TXN_CRASH_KEEP set, the failed round's files are
                // copied there for a post-mortem.
                let keep = std::env::var_os("SPRIG_TXN_CRASH_KEEP").map(|d| {
                    let to = std::path::PathBuf::from(d)
                        .join(format!("crash-{}-{seed}-{elr}", std::process::id()));
                    let _ = std::process::Command::new("cp")
                        .arg("-R")
                        .arg(&dir.0)
                        .arg(&to)
                        .status();
                    to
                });
                panic!(
                    "{round}\ndir {} (copied to {:?})\nall versions: {all:?}\nacknowledged write {val:?} (log index {index}, cts {cts:?}) to k{ki} lost; versions of k{ki} after recovery: {:?}; later acknowledged entries that survived: {survivors:?}",
                    dir.0.display(),
                    keep,
                    all[ki]
                );
            }
        }
    }
    for (ki, val) in &acks.reads {
        assert!(
            all[ki].iter().any(|(_, v)| v.as_ref() == Some(val)),
            "seed {seed}: a client read {val:?} from k{ki}, which the crash lost"
        );
    }
    // Atomicity: a recovered value is on every key its commit wrote, at one
    // timestamp.
    let mut by_val: HashMap<Bytes, Vec<(usize, Ts)>> = HashMap::new();
    for (ki, vs) in &all {
        for (ts, v) in vs {
            if let Some(v) = v {
                by_val.entry(v.clone()).or_default().push((*ki, *ts));
            }
        }
    }
    for (ks, val, _, _) in &acks.writes {
        if let Some(found) = by_val.get(val) {
            assert_eq!(
                found.len(),
                ks.len(),
                "seed {seed}: partial transaction {val:?}"
            );
        }
    }
    for found in by_val.values() {
        assert!(found.windows(2).all(|w| w[0].1 == w[1].1));
    }
    (acks.writes.len(), acks.reads.len())
}

fn run(store: StoreKind, rounds: u64) {
    let (mut w, mut r) = (0, 0);
    for seed in 1..=rounds {
        for elr in [true, false] {
            let (a, b) = crash_round(store, seed, elr);
            w += a;
            r += b;
        }
    }
    eprintln!(
        "{store:?}: {} crash points, {w} acknowledged writes and {r} acknowledged reads all survived",
        rounds * 2
    );
    assert!(w > 100 && r > 100);
}

#[test]
fn engine_crash_mem_store() {
    run(StoreKind::Mem, 25);
}

#[test]
fn engine_crash_rocks() {
    run(StoreKind::Rocks, 25);
}
