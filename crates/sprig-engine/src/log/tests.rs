use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::testutil::TempDir;

/// A state machine that remembers what was applied.
#[derive(Default)]
struct Recorder {
    applied: Mutex<BTreeMap<UnitId, Vec<Applied>>>,
}

/// An applied entry: its index and data.
type Applied = (u64, Vec<u8>);

impl StateMachine for Recorder {
    fn applied_index(&self, unit: UnitId) -> Result<u64> {
        Ok(self
            .applied
            .lock()
            .unwrap()
            .get(&unit)
            .and_then(|v| v.last())
            .map_or(0, |e| e.0))
    }

    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()> {
        let mut a = self.applied.lock().unwrap();
        let v = a.entry(unit).or_default();
        for e in entries {
            let expect = v.last().map_or(e.index, |l| l.0 + 1);
            assert_eq!(e.index, expect, "applied out of order");
            v.push((e.index, e.data.clone()));
        }
        Ok(())
    }
}

fn cfg() -> LogConfig {
    LogConfig {
        sync: SyncMode::Off,
        ..LogConfig::default()
    }
}

#[test]
fn propose_wait_apply_and_replay() {
    let dir = TempDir::new("log");
    let sm = Arc::new(Recorder::default());
    {
        let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
        let mut tickets = Vec::new();
        for i in 0..100u32 {
            let unit = UnitId(u64::from(i % 3) + 1);
            tickets.push(
                log.propose(unit, i.to_be_bytes().to_vec(), Durability::Local)
                    .unwrap(),
            );
        }
        for t in &tickets {
            t.wait().unwrap();
            t.wait_applied().unwrap();
        }
        assert_eq!(log.applied_index(UnitId(1)), 34);
        assert_eq!(log.durable_index(UnitId(2)), 33);
        // Every index handed out is covered; a unit never written has none.
        assert_eq!(log.proposed_index(UnitId(1)), 34);
        assert_eq!(log.proposed_index(UnitId(3)), 33);
        assert_eq!(log.proposed_index(UnitId(9)), 0);
    }
    // A fresh state machine gets every entry back on reopen.
    let fresh = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), cfg(), fresh.clone()).unwrap();
    assert_eq!(log.stats().replayed, 100);
    assert_eq!(*fresh.applied.lock().unwrap(), *sm.applied.lock().unwrap());
    // New proposals continue after the replayed ones.
    let t = log
        .propose(UnitId(1), b"next".to_vec(), Durability::Local)
        .unwrap();
    assert_eq!(t.index(), 35);
    assert_eq!(log.proposed_index(UnitId(1)), 35);
    assert_eq!(log.proposed_index(UnitId(2)), 33);
    t.wait_applied().unwrap();
}

#[test]
fn concurrent_proposals_group() {
    let dir = TempDir::new("group");
    let sm = Arc::new(Recorder::default());
    let log = Arc::new(LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let log = Arc::clone(&log);
            std::thread::spawn(move || {
                for i in 0..500u32 {
                    let tk = log
                        .propose(
                            UnitId(1),
                            [t, i].iter().flat_map(|x: &u32| x.to_be_bytes()).collect(),
                            Durability::Local,
                        )
                        .unwrap();
                    tk.wait().unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    log.wait_applied(UnitId(1), 4000).unwrap();
    let s = log.stats();
    assert_eq!(s.entries, 4000);
    assert!(s.groups < 4000, "concurrent proposals share groups: {s:?}");
    assert_eq!(sm.applied.lock().unwrap()[&UnitId(1)].len(), 4000);
}

#[test]
fn relaxed_entries_are_synced_by_the_timer() {
    let dir = TempDir::new("relaxed");
    let c = LogConfig {
        relaxed_sync: Duration::from_millis(30),
        ..cfg()
    };
    let log = LocalLog::open(dir.path(), c, Arc::new(Recorder::default())).unwrap();
    let t = log
        .propose(UnitId(1), b"x".to_vec(), Durability::LocalRelaxed)
        .unwrap();
    t.wait().unwrap();
    let before = log.stats().syncs;
    std::thread::sleep(Duration::from_millis(120));
    assert!(log.stats().syncs > before, "timer sync");
    assert_eq!(log.durable_index(UnitId(1)), 1);
}

#[tokio::test]
async fn async_wait() {
    let dir = TempDir::new("async");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    let t = log
        .propose(UnitId(7), b"a".to_vec(), Durability::Strict)
        .unwrap();
    t.wait_async().await.unwrap();
    t.wait_applied_async().await.unwrap();
    assert!(t.is_durable());
}

#[test]
fn closed_log_refuses() {
    let dir = TempDir::new("closed");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    log.close();
    assert!(log.propose(UnitId(1), vec![1], Durability::Local).is_err());
}

#[test]
fn checkpoint_trims_and_replay_resumes_after_it() {
    let dir = TempDir::new("ckpt");
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
    for i in 0..50u8 {
        log.propose(UnitId(1), vec![i], Durability::Local).unwrap();
    }
    log.wait_applied(UnitId(1), 50).unwrap();
    let trimmed = log.checkpoint(&|| Ok(())).unwrap();
    assert_eq!(trimmed, 50);
    for i in 50..60u8 {
        log.propose(UnitId(1), vec![i], Durability::Local).unwrap();
    }
    log.wait_applied(UnitId(1), 60).unwrap();
    drop(log);
    // The state machine kept 1..=60, so only nothing is replayed; a state
    // machine that "lost" 51..=60 gets exactly those back.
    {
        let mut a = sm.applied.lock().unwrap();
        a.get_mut(&UnitId(1)).unwrap().truncate(50);
    }
    let log = LocalLog::open(dir.path(), cfg(), sm.clone()).unwrap();
    assert_eq!(log.stats().replayed, 10);
    assert_eq!(sm.applied.lock().unwrap()[&UnitId(1)].len(), 60);
    drop(log);
    // A state machine behind the trim point is an error, not silent loss.
    let behind = Arc::new(Recorder::default());
    assert!(LocalLog::open(dir.path(), cfg(), behind).is_err());
}

/// Bytes that do not compress (a fixed xorshift stream).
fn noise(seed: u64, n: usize) -> Vec<u8> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
        })
        .collect()
}

fn log_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|f| {
            let name = f.as_ref().unwrap().file_name();
            name.to_string_lossy().ends_with(".raftlog")
        })
        .count()
}

/// A checkpoint deletes the files it trimmed, down to the one being
/// written, so the log on disk stays near one file however much went
/// through it; what was written after the checkpoint still replays.
#[test]
fn checkpoint_deletes_trimmed_files() {
    let dir = TempDir::new("purge");
    let c = LogConfig {
        target_file_size: 1 << 20,
        ..cfg()
    };
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), c.clone(), sm.clone()).unwrap();
    let mut index = 0;
    for round in 0..3u64 {
        // One entry per group: a group is written to one file whole.
        for i in 0..40 {
            log.propose(
                UnitId(1),
                noise(round * 100 + i, 128 << 10),
                Durability::Local,
            )
            .unwrap()
            .wait()
            .unwrap();
        }
        index += 40;
        log.wait_applied(UnitId(1), index).unwrap();
        assert!(
            log_files(dir.path()) >= 4,
            "5 MiB of entries span several 1 MiB files"
        );
        assert_eq!(log.checkpoint(&|| Ok(())).unwrap(), 40);
        assert!(
            log_files(dir.path()) <= 2,
            "round {round}: {} files left after the checkpoint",
            log_files(dir.path())
        );
        assert!(log.disk_bytes() <= 3 << 20, "{}", log.disk_bytes());
    }
    for i in 0..5 {
        log.propose(UnitId(1), noise(1000 + i, 128 << 10), Durability::Local)
            .unwrap();
    }
    log.wait_applied(UnitId(1), index + 5).unwrap();
    drop(log);
    {
        let mut a = sm.applied.lock().unwrap();
        a.get_mut(&UnitId(1)).unwrap().truncate(120);
    }
    let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
    assert_eq!(log.stats().replayed, 5);
    let a = sm.applied.lock().unwrap();
    for (i, (idx, data)) in a[&UnitId(1)][120..].iter().enumerate() {
        assert_eq!(*idx, 121 + i as u64);
        assert_eq!(data, &noise(1000 + i as u64, 128 << 10));
    }
}

/// Groups at or above `compress_min_bytes` are compressed in the log: the
/// file is much smaller than what went in, and replay returns the same
/// bytes. With compression off the file holds every byte.
#[test]
fn large_groups_are_compressed_and_replay_unchanged() {
    let on_disk = |compress_min_bytes: u64| {
        let dir = TempDir::new("lz4");
        let c = LogConfig {
            compress_min_bytes,
            ..cfg()
        };
        let sm = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), c.clone(), sm.clone()).unwrap();
        let payload = |i: u8| {
            let mut v = noise(u64::from(i), 16);
            v.extend(std::iter::repeat_n(b'x', 32 << 10));
            v
        };
        for i in 0..32u8 {
            log.propose(UnitId(1), payload(i), Durability::Local)
                .unwrap();
        }
        log.wait_applied(UnitId(1), 32).unwrap();
        drop(log);
        let bytes: u64 = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|f| f.unwrap().metadata().unwrap().len())
            .sum();
        sm.applied.lock().unwrap().clear();
        let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
        assert_eq!(log.stats().replayed, 32);
        let a = sm.applied.lock().unwrap();
        for (i, (idx, data)) in a[&UnitId(1)].iter().enumerate() {
            assert_eq!(*idx, 1 + i as u64);
            assert_eq!(data, &payload(u8::try_from(i).unwrap()));
        }
        bytes
    };
    let plain = on_disk(0);
    let packed = on_disk(4 << 10);
    assert!(plain >= 1 << 20, "uncompressed: {plain} bytes");
    assert!(packed < plain / 8, "compressed: {packed} of {plain} bytes");
}

/// Under the crash simulation, every acknowledged `Local` entry survives,
/// and replay never yields a gap.
#[test]
fn sim_crash_keeps_acknowledged_entries() {
    let mut lost_total = 0;
    for seed in 1..=40u64 {
        let dir = TempDir::new("simcrash");
        let sim = SimDisk::new();
        let c = LogConfig {
            sim: Some(sim.clone()),
            ..cfg()
        };
        let sm = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), c.clone(), sm).unwrap();
        let mut acked = 0;
        for i in 0..(20 + seed * 3) {
            let d = if i % 3 == 0 {
                Durability::LocalRelaxed
            } else {
                Durability::Local
            };
            let t = log.propose(UnitId(1), i.to_be_bytes().to_vec(), d).unwrap();
            if d.needs_sync() {
                t.wait().unwrap();
                acked = t.index();
            }
        }
        // Simulate a power loss: stop without the closing sync, then cut
        // unsynced tails.
        log.halt();
        drop(log);
        lost_total += sim.crash(dir.path(), seed).unwrap();
        let fresh = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), LogConfig { sim: None, ..c }, fresh.clone()).unwrap();
        let got = fresh
            .applied
            .lock()
            .unwrap()
            .get(&UnitId(1))
            .cloned()
            .unwrap_or_default();
        assert!(
            got.len() as u64 >= acked,
            "seed {seed}: {} < acked {acked}",
            got.len()
        );
        for (n, (i, data)) in got.iter().enumerate() {
            assert_eq!(*i, n as u64 + 1);
            assert_eq!(data, &((n as u64).to_be_bytes().to_vec()));
        }
        drop(log);
    }
    // The simulation must actually lose unsynced bytes; raft-engine's sync
    // when the log is dropped comes after the power cut and keeps nothing.
    assert!(lost_total > 0, "the crash simulation lost nothing");
}

/// Polls until `unit`'s durable index reaches `index` (the sync timer ran).
fn wait_durable(log: &LocalLog, unit: UnitId, index: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while log.durable_index(unit) < index {
        assert!(
            std::time::Instant::now() < deadline,
            "the sync timer never reached {index}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// What a fresh state machine gets back from the log after a power cut:
/// every entry of unit 1, in order, with no gap.
fn after_power_cut(log: LocalLog, sim: &SimDisk, dir: &std::path::Path, seed: u64) -> Vec<Applied> {
    log.halt();
    drop(log);
    sim.crash(dir, seed).unwrap();
    let fresh = Arc::new(Recorder::default());
    let log = LocalLog::open(dir, cfg(), fresh.clone()).unwrap();
    drop(log);
    fresh
        .applied
        .lock()
        .unwrap()
        .get(&UnitId(1))
        .cloned()
        .unwrap_or_default()
}

/// Entries acknowledged with a relaxed durability class are synced by the
/// timer: once it has run (the durable index covers them), a power cut
/// that drops every unsynced byte keeps them all.
#[test]
fn relaxed_entries_survive_a_power_cut_after_the_timer() {
    for d in [Durability::LocalRelaxed, Durability::Replicated] {
        for seed in 1..=8u64 {
            let dir = TempDir::new("relaxcut");
            let sim = SimDisk::new();
            let c = LogConfig {
                relaxed_sync: Duration::from_millis(20),
                replicated_sync: Duration::from_millis(20),
                sim: Some(sim.clone()),
                ..cfg()
            };
            let log = LocalLog::open(dir.path(), c, Arc::new(Recorder::default())).unwrap();
            for i in 0..40u64 {
                log.propose(UnitId(1), noise(i, 512), d)
                    .unwrap()
                    .wait()
                    .unwrap();
            }
            wait_durable(&log, UnitId(1), 40);
            let got = after_power_cut(log, &sim, dir.path(), seed);
            assert_eq!(
                got.len(),
                40,
                "{d:?} seed {seed}: the timer's sync kept nothing"
            );
            for (n, (i, data)) in got.iter().enumerate() {
                assert_eq!(*i, n as u64 + 1);
                assert_eq!(data, &noise(n as u64, 512));
            }
        }
    }
}

/// A checkpoint syncs the log before it flushes the store: entries it
/// keeps (here an apply observer still needs all of them) survive a power
/// cut, even when they were written relaxed and the timer hasn't run.
#[test]
fn checkpoint_syncs_the_entries_it_keeps() {
    for seed in 1..=8u64 {
        let dir = TempDir::new("ckptcut");
        let sim = SimDisk::new();
        let c = LogConfig {
            // The timer must not do the checkpoint's work.
            relaxed_sync: Duration::from_secs(3600),
            sim: Some(sim.clone()),
            ..cfg()
        };
        let sm = Arc::new(Recorder::default());
        let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
        for i in 0..40u64 {
            log.propose(UnitId(1), noise(i, 512), Durability::LocalRelaxed)
                .unwrap();
        }
        log.wait_applied(UnitId(1), 40).unwrap();
        let flushed: Mutex<BTreeMap<UnitId, Vec<Applied>>> = Mutex::default();
        let trimmed = log
            .checkpoint_capped(
                &|| {
                    *flushed.lock().unwrap() = sm.applied.lock().unwrap().clone();
                    Ok(())
                },
                &|_| Some(0),
            )
            .unwrap();
        assert_eq!(trimmed, 0);
        log.halt();
        drop(log);
        sim.crash(dir.path(), seed).unwrap();
        // The store comes back as the checkpoint flushed it; the log must
        // still hold every entry the observer needs.
        let restored = Arc::new(Recorder {
            applied: Mutex::new(flushed.into_inner().unwrap()),
        });
        let log = LocalLog::open(dir.path(), cfg(), restored).unwrap();
        assert_eq!(log.first_index(UnitId(1)), Some(1), "seed {seed}");
        let got = log.entries(UnitId(1), 1, 41).unwrap();
        assert_eq!(
            got.len(),
            40,
            "seed {seed}: the checkpoint's sync kept nothing"
        );
        for (n, e) in got.iter().enumerate() {
            assert_eq!((e.index, &e.data), (n as u64 + 1, &noise(n as u64, 512)));
        }
    }
}

/// The group that carries sync markers is no unit: proposing to it fails,
/// replay doesn't treat it as one, and many syncs and checkpoints still let
/// the log delete the files it trimmed.
#[test]
fn sync_markers_stay_out_of_units_and_purges() {
    let dir = TempDir::new("syncgroup");
    let c = LogConfig {
        target_file_size: 1 << 20,
        relaxed_sync: Duration::from_millis(1),
        ..cfg()
    };
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(dir.path(), c.clone(), sm.clone()).unwrap();
    assert!(
        log.propose(UnitId(u64::MAX), vec![1], Durability::Local)
            .is_err()
    );
    let mut index = 0;
    for round in 0..3u64 {
        for i in 0..40 {
            log.propose(
                UnitId(1),
                noise(round * 100 + i, 128 << 10),
                Durability::LocalRelaxed,
            )
            .unwrap();
            index += 1;
            // Let the timer write a marker between most entries.
            wait_durable(&log, UnitId(1), index);
        }
        log.wait_applied(UnitId(1), index).unwrap();
        assert_eq!(log.checkpoint(&|| Ok(())).unwrap(), 40);
        assert!(
            log_files(dir.path()) <= 2,
            "round {round}: {} files left after the checkpoint",
            log_files(dir.path())
        );
    }
    assert!(log.stats().syncs >= 100, "{:?}", log.stats());
    drop(log);
    let log = LocalLog::open(dir.path(), c, sm.clone()).unwrap();
    assert_eq!(log.applied(), vec![(UnitId(1), index)]);
    assert_eq!(log.stats().replayed, 0);
    let t = log
        .propose(UnitId(1), b"next".to_vec(), Durability::Local)
        .unwrap();
    assert_eq!(t.index(), index + 1);
    t.wait_applied().unwrap();
}

/// `entry` reads back what was written, and nothing past the end.
#[test]
fn entry_reads_one_index() {
    let dir = TempDir::new("entry");
    let log = LocalLog::open(dir.path(), cfg(), Arc::new(Recorder::default())).unwrap();
    for i in 0..3u64 {
        log.propose(UnitId(1), i.to_be_bytes().to_vec(), Durability::Local)
            .unwrap()
            .wait()
            .unwrap();
    }
    let e = log.entry(UnitId(1), 2).unwrap().unwrap();
    assert_eq!((e.index, e.data.as_slice()), (2, &1u64.to_be_bytes()[..]));
    assert!(log.entry(UnitId(1), 9).unwrap().is_none());
}

/// The parts of an atomic write across units go into one group (one
/// raft-engine batch) even when they pass the group's byte limit, each unit
/// numbering its part in its own sequence.
#[test]
fn an_atomic_write_across_units_is_one_group() {
    let dir = TempDir::new("log-many");
    let sm = Arc::new(Recorder::default());
    let log = LocalLog::open(
        dir.path(),
        LogConfig {
            group_max_bytes: 16,
            ..cfg()
        },
        sm.clone(),
    )
    .unwrap();
    log.propose(UnitId(2), vec![0; 8], Durability::Local)
        .unwrap()
        .wait_applied()
        .unwrap();
    let before = log.stats().groups;
    let tickets = log
        .propose_many(
            vec![
                (UnitId(1), vec![1; 40]),
                (UnitId(2), vec![2; 40]),
                (UnitId(3), vec![3; 40]),
            ],
            Durability::Local,
        )
        .unwrap();
    let got: Vec<(UnitId, u64)> = tickets.iter().map(|t| (t.unit(), t.index())).collect();
    assert_eq!(got, vec![(UnitId(1), 1), (UnitId(2), 2), (UnitId(3), 1)]);
    for t in &tickets {
        t.wait_applied().unwrap();
    }
    assert_eq!(log.stats().groups, before + 1);
    assert!(
        log.propose_many(
            vec![(UnitId(4), vec![]), (UnitId(4), vec![])],
            Durability::Local
        )
        .is_err()
    );
    assert!(
        log.propose_many(
            vec![(UnitId(5), vec![]), (UnitId(u64::MAX), vec![])],
            Durability::Local
        )
        .is_err()
    );
    // The refused write took no index.
    let t = log.propose(UnitId(5), vec![], Durability::Local).unwrap();
    assert_eq!(t.index(), 1);
}

/// After a power cut, an atomic write across units is in the log whole or
/// not at all.
#[test]
fn an_atomic_write_across_units_survives_a_crash_whole_or_not_at_all() {
    for seed in 1..=40u64 {
        let dir = TempDir::new("log-many-crash");
        let sim = SimDisk::new();
        {
            let sm = Arc::new(Recorder::default());
            let log = LocalLog::open(
                dir.path(),
                LogConfig {
                    sim: Some(sim.clone()),
                    ..cfg()
                },
                sm,
            )
            .unwrap();
            for i in 0..30u64 {
                let d = if i % 3 == 0 {
                    Durability::Local
                } else {
                    Durability::LocalRelaxed
                };
                let ts = log
                    .propose_many(
                        vec![
                            (UnitId(1), i.to_be_bytes().to_vec()),
                            (UnitId(2), i.to_be_bytes().to_vec()),
                        ],
                        d,
                    )
                    .unwrap();
                if d.needs_sync() {
                    for t in &ts {
                        t.wait().unwrap();
                    }
                }
            }
            log.halt();
        }
        sim.crash(dir.path(), seed).unwrap();
        let fresh = Arc::new(Recorder::default());
        let _log = LocalLog::open(dir.path(), cfg(), fresh.clone()).unwrap();
        let a = fresh.applied.lock().unwrap();
        let one = a.get(&UnitId(1)).cloned().unwrap_or_default();
        let two = a.get(&UnitId(2)).cloned().unwrap_or_default();
        assert_eq!(one, two, "seed {seed}: a write across units was cut");
        assert!(
            one.len() >= 28,
            "seed {seed}: synced writes lost ({})",
            one.len()
        );
    }
}
