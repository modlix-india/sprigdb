//! Model tests against `MemStore`: a snapshot-isolation and lock-order
//! checker over random concurrent histories, and a linearizability checker
//! for autocommit reads and writes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation,
    clippy::items_after_statements
)]

mod txn_common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::StoreKind;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{Isolation, LockRequest, Locked, Ts, TxnOptions, UnitConfig, Wait};
use txn_common::{Bed, Dir, engine_bed, k, mem_bed_with};

/// A tiny deterministic generator (no extra dependencies).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, Clone)]
enum Op {
    /// A plain read at `rts` returned the value.
    Snapshot {
        key: usize,
        rts: Ts,
        got: Option<Bytes>,
        own: bool,
    },
    /// A locking read returned the latest value.
    Current {
        key: usize,
        got: Option<Bytes>,
        own: bool,
        view: Option<Ts>,
    },
    /// A write after an X lock.
    Write { key: usize, val: Option<Bytes> },
}

#[derive(Debug, Clone)]
struct TxnRecord {
    si: bool,
    ops: Vec<Op>,
    cts: Option<Ts>,
}

const KEYS: usize = 6;

fn key(i: usize) -> Bytes {
    k(&format!("key{i}"))
}

fn run_txn(unit: &Bed, rng: &mut Rng, tid: usize, seq: &mut u64) -> TxnRecord {
    let base = if rng.below(2) == 0 {
        TxnOptions::mariadb()
    } else {
        TxnOptions::mysql()
    };
    let iso = if rng.below(3) == 0 {
        Isolation::ReadCommitted
    } else {
        Isolation::RepeatableRead
    };
    let opts = base.with_isolation(iso).with_lock_wait_seconds(10);
    let si = opts.snapshot_isolation && iso == Isolation::RepeatableRead;
    let mut t = unit.begin(opts);
    let mut rec = TxnRecord {
        si,
        ops: Vec::new(),
        cts: None,
    };
    let mut own: HashSet<usize> = HashSet::new();
    let n = 1 + rng.below(4);
    for _ in 0..n {
        t.begin_statement();
        let ki = usize::try_from(rng.below(KEYS as u64)).unwrap();
        match rng.below(4) {
            0 => {
                let got = t.get_snapshot(&key(ki)).unwrap();
                rec.ops.push(Op::Snapshot {
                    key: ki,
                    rts: t.read_view().unwrap_or(Ts::MIN),
                    got,
                    own: own.contains(&ki),
                });
            }
            1 => {
                let view = t.read_view();
                match t.lock_get_blocking(&key(ki), LockRequest::for_share()) {
                    Ok(Locked::Row(got)) => rec.ops.push(Op::Current {
                        key: ki,
                        got,
                        own: own.contains(&ki),
                        view,
                    }),
                    Ok(Locked::Skipped) => unreachable!(),
                    Err(_) => return rec,
                }
            }
            _ => {
                let view = t.read_view();
                match t.lock_get_blocking(&key(ki), LockRequest::for_update()) {
                    Ok(Locked::Row(got)) => rec.ops.push(Op::Current {
                        key: ki,
                        got,
                        own: own.contains(&ki),
                        view,
                    }),
                    Ok(Locked::Skipped) => unreachable!(),
                    Err(_) => return rec,
                }
                *seq += 1;
                let val = if rng.below(8) == 0 {
                    None
                } else {
                    Some(Bytes::from(format!("t{tid}.{seq}")))
                };
                match &val {
                    Some(v) => t.put(key(ki), v.clone()).unwrap(),
                    None => t.delete(key(ki)).unwrap(),
                }
                own.insert(ki);
                rec.ops.push(Op::Write { key: ki, val });
            }
        }
    }
    if rng.below(10) == 0 {
        t.rollback();
        return rec;
    }
    let ticket = t.commit().unwrap();
    ticket.wait().unwrap();
    rec.cts = ticket.commit_ts();
    rec
}

/// The newest committed version of `key` with `cts <= at` (or `< at`).
fn version_at(
    versions: &BTreeMap<usize, Vec<(Ts, Option<Bytes>)>>,
    key: usize,
    at: Ts,
    inclusive: bool,
) -> Option<Bytes> {
    versions.get(&key).and_then(|vs| {
        vs.iter()
            .rev()
            .find(|(ts, _)| if inclusive { *ts <= at } else { *ts < at })
            .and_then(|(_, v)| v.clone())
    })
}

fn check_history(history: &[TxnRecord]) -> (usize, usize, usize) {
    // Committed versions per key, in commit-timestamp order.
    let mut versions: BTreeMap<usize, Vec<(Ts, Option<Bytes>)>> = BTreeMap::new();
    let mut seen_cts = HashSet::new();
    for r in history {
        let Some(cts) = r.cts else { continue };
        assert!(seen_cts.insert(cts), "duplicate commit timestamp");
        let mut last: HashMap<usize, Option<Bytes>> = HashMap::new();
        for op in &r.ops {
            if let Op::Write { key, val } = op {
                last.insert(*key, val.clone());
            }
        }
        for (key, val) in last {
            versions.entry(key).or_default().push((cts, val));
        }
    }
    for vs in versions.values_mut() {
        vs.sort_by_key(|(ts, _)| *ts);
    }
    let (mut snapshot_reads, mut current_reads, mut si_checks) = (0, 0, 0);
    for r in history {
        let mut own: HashMap<usize, Option<Bytes>> = HashMap::new();
        for op in &r.ops {
            match op {
                Op::Write { key, val } => {
                    own.insert(*key, val.clone());
                }
                Op::Snapshot {
                    key,
                    rts,
                    got,
                    own: is_own,
                } => {
                    let want = if *is_own {
                        own.get(key).cloned().flatten()
                    } else {
                        version_at(&versions, *key, *rts, true)
                    };
                    assert_eq!(got, &want, "snapshot read of key{key} at {rts:?}");
                    snapshot_reads += 1;
                }
                Op::Current {
                    key,
                    got,
                    own: is_own,
                    view,
                } => {
                    if *is_own {
                        assert_eq!(got, &own.get(key).cloned().flatten());
                    } else if let Some(cts) = r.cts {
                        // Held until commit: nobody committed the key between
                        // this read and our commit.
                        let want = version_at(&versions, *key, cts, false);
                        assert_eq!(got, &want, "current read of key{key} before {cts:?}");
                        current_reads += 1;
                        // First committer wins: no foreign version in
                        // (view, cts) under MariaDB's snapshot isolation.
                        if let (true, Some(rts)) = (r.si, view) {
                            let foreign =
                                versions[key].iter().any(|(ts, _)| *ts > *rts && *ts < cts);
                            assert!(!foreign, "1020 should have fired for key{key}");
                            si_checks += 1;
                        }
                    }
                }
            }
        }
    }
    (snapshot_reads, current_reads, si_checks)
}

#[derive(Debug, Clone, Copy)]
enum Variant {
    /// The in-memory path with this sync delay.
    Mem(Duration),
    /// The engine: raft-engine log (no syncs) over this store.
    Engine(StoreKind),
}

fn run_history(seed: u64, variant: Variant, elr: bool) -> Vec<TxnRecord> {
    let cfg = UnitConfig {
        early_lock_release: elr,
        ..UnitConfig::default()
    };
    let dir = Dir::new("model");
    let (_engine, b) = match variant {
        Variant::Mem(d) => (None, mem_bed_with(cfg, d)),
        Variant::Engine(store) => {
            let (e, b) = engine_bed(&dir, store, SyncMode::Off);
            b.shared().set_early_lock_release(elr);
            (Some(e), b)
        }
    };
    let history = Arc::new(Mutex::new(Vec::new()));
    let threads: Vec<_> = (0..6)
        .map(|tid| {
            let unit = b.clone();
            let history = history.clone();
            thread::spawn(move || {
                let mut rng = Rng(seed * 7919 + tid as u64 * 104_729 + 1);
                let mut seq = 0;
                for _ in 0..150 {
                    let rec = run_txn(&unit, &mut rng, tid, &mut seq);
                    history.lock().unwrap().push(rec);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    // Final state equals the last committed version of each key.
    let h = history.lock().unwrap().clone();
    let mut last: BTreeMap<usize, (Ts, Option<Bytes>)> = BTreeMap::new();
    for r in &h {
        let Some(cts) = r.cts else { continue };
        for op in &r.ops {
            if let Op::Write { key, val } = op {
                let e = last.entry(*key).or_insert((cts, None));
                if cts >= e.0 {
                    *e = (cts, val.clone());
                }
            }
        }
    }
    let mut r = b.begin(TxnOptions::mariadb());
    for ki in 0..KEYS {
        assert_eq!(
            r.get_snapshot(&key(ki)).unwrap(),
            last.get(&ki).and_then(|e| e.1.clone())
        );
    }
    assert_eq!(b.locks().key_count(), 0, "lock leak");
    h
}

#[test]
fn random_histories_are_snapshot_consistent_and_lock_ordered() {
    let mut totals = (0, 0, 0, 0, 0);
    for seed in 1..=6 {
        for (variant, elr) in [
            (Variant::Mem(Duration::ZERO), true),
            (Variant::Mem(Duration::from_micros(300)), true),
            (Variant::Mem(Duration::from_micros(300)), false),
            (Variant::Engine(StoreKind::Mem), true),
            (Variant::Engine(StoreKind::Rocks), true),
            (Variant::Engine(StoreKind::Rocks), false),
        ] {
            let h = run_history(seed, variant, elr);
            let (s, c, si) = check_history(&h);
            totals.0 += s;
            totals.1 += c;
            totals.2 += si;
            totals.3 += h.iter().filter(|r| r.cts.is_some()).count();
            totals.4 += h.len();
        }
    }
    eprintln!(
        "checked {} snapshot reads, {} current reads, {} first-committer checks; {} of {} transactions committed",
        totals.0, totals.1, totals.2, totals.3, totals.4
    );
    assert!(totals.0 > 1000 && totals.1 > 1000 && totals.2 > 100);
}

// ----- linearizability ----------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum RegOp {
    Write(u64),
    Read(u64),
}

#[derive(Debug, Clone, Copy)]
struct Timed {
    op: RegOp,
    invoke: Instant,
    response: Instant,
}

/// Wing & Gong's search with memoisation, for one register starting at 0.
fn linearizable(ops: &[Timed]) -> bool {
    assert!(ops.len() <= 64);
    fn go(ops: &[Timed], done: u64, val: u64, seen: &mut HashSet<(u64, u64)>) -> bool {
        if done.count_ones() as usize == ops.len() {
            return true;
        }
        if !seen.insert((done, val)) {
            return false;
        }
        let pending = |i: usize| done & (1 << i) == 0;
        let min_resp = (0..ops.len())
            .filter(|&i| pending(i))
            .map(|i| ops[i].response)
            .min()
            .unwrap();
        for i in 0..ops.len() {
            if !pending(i) || ops[i].invoke > min_resp {
                continue;
            }
            let next = match ops[i].op {
                RegOp::Write(v) => v,
                RegOp::Read(v) if v == val => val,
                RegOp::Read(_) => continue,
            };
            if go(ops, done | (1 << i), next, seen) {
                return true;
            }
        }
        false
    }
    go(ops, 0, 0, &mut HashSet::new())
}

#[test]
fn the_checker_rejects_a_stale_read() {
    let t0 = Instant::now();
    let at = |ms: u64| t0 + Duration::from_millis(ms);
    let ok = [
        Timed {
            op: RegOp::Write(1),
            invoke: at(0),
            response: at(10),
        },
        Timed {
            op: RegOp::Read(1),
            invoke: at(5),
            response: at(6),
        },
    ];
    assert!(linearizable(&ok));
    let stale = [
        Timed {
            op: RegOp::Write(1),
            invoke: at(0),
            response: at(10),
        },
        Timed {
            op: RegOp::Read(0),
            invoke: at(20),
            response: at(21),
        },
    ];
    assert!(!linearizable(&stale));
}

#[test]
fn autocommit_reads_and_writes_are_linearizable() {
    for (variant, elr) in [
        (Variant::Mem(Duration::from_micros(500)), true),
        (Variant::Mem(Duration::from_micros(500)), false),
        (Variant::Mem(Duration::ZERO), true),
        (Variant::Engine(StoreKind::Rocks), true),
    ] {
        for round in 0..10u64 {
            let dir = Dir::new("lin");
            let cfg = UnitConfig {
                early_lock_release: elr,
                ..UnitConfig::default()
            };
            let (_engine, b) = match variant {
                Variant::Mem(d) => (None, mem_bed_with(cfg, d)),
                Variant::Engine(store) => {
                    let (e, b) = engine_bed(&dir, store, SyncMode::Off);
                    (Some(e), b)
                }
            };
            let ops = Arc::new(Mutex::new(Vec::new()));
            let threads: Vec<_> = (0..5u64)
                .map(|tid| {
                    let unit = b.clone();
                    let ops = ops.clone();
                    thread::spawn(move || {
                        let mut rng = Rng(round * 31 + tid * 7 + 3);
                        for i in 0..10u64 {
                            let invoke = Instant::now();
                            let op = if rng.below(2) == 0 {
                                let val = tid * 1000 + i + 1;
                                let mut t = unit.begin(TxnOptions::mariadb());
                                t.lock_blocking(
                                    &k("r"),
                                    LockRequest::write().with_wait(Wait::Block),
                                )
                                .unwrap();
                                t.put(k("r"), Bytes::copy_from_slice(&val.to_be_bytes()))
                                    .unwrap();
                                t.commit().unwrap().wait().unwrap();
                                RegOp::Write(val)
                            } else {
                                let mut t = unit.begin(TxnOptions::mariadb());
                                let got = t
                                    .get_snapshot(&k("r"))
                                    .unwrap()
                                    .map_or(0, |b| u64::from_be_bytes(b[..].try_into().unwrap()));
                                t.commit().unwrap().wait().unwrap();
                                RegOp::Read(got)
                            };
                            ops.lock().unwrap().push(Timed {
                                op,
                                invoke,
                                response: Instant::now(),
                            });
                        }
                    })
                })
                .collect();
            for t in threads {
                t.join().unwrap();
            }
            let ops = ops.lock().unwrap().clone();
            assert!(
                linearizable(&ops),
                "not linearizable (round {round}): {ops:?}"
            );
        }
    }
}
