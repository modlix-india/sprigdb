use raft::prelude::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};

use super::*;

fn ent(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        data: index.to_le_bytes().to_vec().into(),
        ..Entry::default()
    }
}

fn snap(index: u64, term: u64, data: &[u8]) -> Snapshot {
    let mut cs = ConfState::default();
    cs.voters = vec![1, 2, 3];
    let mut meta = SnapshotMetadata::default();
    meta.index = index;
    meta.term = term;
    meta.set_conf_state(cs);
    let mut s = Snapshot::default();
    s.set_metadata(meta);
    s.data = data.to_vec().into();
    s
}

fn hs(term: u64, commit: u64) -> HardState {
    let mut h = HardState::default();
    h.term = term;
    h.commit = commit;
    h
}

fn batch(writes: Vec<GroupWrite>) -> WriteBatch {
    WriteBatch { groups: writes }
}

/// The same scenario against any log store; returns what it recovers.
fn scenario(s: &dyn LogStore) -> Vec<RecoveredGroup> {
    s.write(
        &batch(vec![GroupWrite {
            group: 1,
            snapshot: Some(SnapshotWrite::Install(snap(5, 5, b"init"))),
            entries: (6..=10).map(|i| ent(i, 6)).collect(),
            hard_state: Some(hs(6, 8)),
            ..GroupWrite::default()
        }]),
        false,
    )
    .unwrap();
    // A new leader's entries replace 9 and 10.
    s.write(
        &batch(vec![
            GroupWrite {
                group: 1,
                entries: vec![ent(9, 7), ent(10, 7), ent(11, 7)],
                hard_state: Some(hs(7, 10)),
                ..GroupWrite::default()
            },
            GroupWrite {
                group: 2,
                entries: (1..=3).map(|i| ent(i, 1)).collect(),
                ..GroupWrite::default()
            },
        ]),
        false,
    )
    .unwrap();
    // A checkpoint at 8 keeps 9..=11; group 2 is removed.
    s.write(
        &batch(vec![
            GroupWrite {
                group: 1,
                snapshot: Some(SnapshotWrite::Checkpoint(snap(8, 6, b"at8"))),
                ..GroupWrite::default()
            },
            GroupWrite {
                group: 2,
                remove: Some(3),
                ..GroupWrite::default()
            },
        ]),
        true,
    )
    .unwrap();
    s.recover().unwrap()
}

fn check(r: &[RecoveredGroup]) {
    assert_eq!(r.len(), 2);
    let g1 = &r[0];
    assert_eq!(g1.group, 1);
    assert_eq!(g1.snapshot.get_metadata().index, 8);
    assert_eq!(&g1.snapshot.data[..], b"at8");
    let idx: Vec<(u64, u64)> = g1.entries.iter().map(|e| (e.index, e.term)).collect();
    assert_eq!(idx, vec![(9, 7), (10, 7), (11, 7)]);
    assert_eq!((g1.hard_state.term, g1.hard_state.commit), (7, 10));
    assert_eq!(r[1].group, 2);
    assert_eq!(r[1].tombstone, Some(3));
}

#[test]
fn the_simulated_disk_folds_batches_like_a_log() {
    let s = MemLogStore::new();
    check(&scenario(&*s));
    // An install replaces the whole log.
    s.write(
        &batch(vec![GroupWrite {
            group: 1,
            snapshot: Some(SnapshotWrite::Install(snap(20, 9, b"at20"))),
            ..GroupWrite::default()
        }]),
        true,
    )
    .unwrap();
    let r = s.recover().unwrap();
    assert!(r[0].entries.is_empty());
    assert_eq!(r[0].snapshot.get_metadata().index, 20);
}

#[test]
fn a_crash_keeps_synced_batches_and_a_prefix_of_the_rest() {
    let mut kept_counts = std::collections::BTreeSet::new();
    for seed in 0..64 {
        let s = MemLogStore::new();
        s.write(
            &batch(vec![GroupWrite {
                group: 1,
                entries: vec![ent(1, 1)],
                ..GroupWrite::default()
            }]),
            true,
        )
        .unwrap();
        for i in 2..=5 {
            s.write(
                &batch(vec![GroupWrite {
                    group: 1,
                    entries: vec![ent(i, 1)],
                    ..GroupWrite::default()
                }]),
                false,
            )
            .unwrap();
        }
        let loss = s.crash(seed);
        assert_eq!(loss.unsynced, 4);
        let r = s.recover().unwrap();
        let last = r[0].entries.last().unwrap().index;
        // Entry 1 was synced; the rest survive as a prefix.
        assert_eq!(last, 1 + loss.kept as u64);
        let idx: Vec<u64> = r[0].entries.iter().map(|e| e.index).collect();
        assert_eq!(idx, (1..=last).collect::<Vec<_>>());
        kept_counts.insert(loss.kept);
    }
    // Seeds reach every outcome from losing all to keeping all.
    assert_eq!(kept_counts.len(), 5);
}

#[test]
fn raft_engine_holds_the_same_log() {
    let dir = std::env::temp_dir().join(format!("sprig-raft-logstore-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let s = EngineLogStore::open(&dir, SyncMode::Full, None).unwrap();
        check(&scenario(&*s));
    }
    // And after reopening.
    let s = EngineLogStore::open(&dir, SyncMode::Full, None).unwrap();
    check(&s.recover().unwrap());
    // An install replaces the whole log, including entries past it.
    s.write(
        &batch(vec![GroupWrite {
            group: 1,
            snapshot: Some(SnapshotWrite::Install(snap(9, 7, b"at9"))),
            hard_state: Some(hs(7, 9)),
            ..GroupWrite::default()
        }]),
        true,
    )
    .unwrap();
    drop(s);
    let s = EngineLogStore::open(&dir, SyncMode::Full, None).unwrap();
    let r = s.recover().unwrap();
    assert!(r[0].entries.is_empty(), "{:?}", r[0].entries);
    assert_eq!(r[0].snapshot.get_metadata().index, 9);
    drop(s);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn raft_engine_after_a_power_cut_keeps_a_prefix() {
    for seed in [1u64, 7, 42, 99, 1234] {
        let dir = std::env::temp_dir().join(format!(
            "sprig-raft-logstore-cut-{}-{seed}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let sim = SimDisk::new();
        {
            let s = EngineLogStore::open(&dir, SyncMode::Full, Some(Arc::clone(&sim))).unwrap();
            for i in 1..=20 {
                s.write(
                    &batch(vec![GroupWrite {
                        group: 1,
                        entries: vec![ent(i, 1)],
                        hard_state: Some(hs(1, i)),
                        ..GroupWrite::default()
                    }]),
                    i == 10,
                )
                .unwrap();
            }
            sim.power_cut();
        }
        sim.crash(&dir, seed).unwrap();
        let s = EngineLogStore::open(&dir, SyncMode::Full, None).unwrap();
        let r = s.recover().unwrap();
        let idx: Vec<u64> = r[0].entries.iter().map(|e| e.index).collect();
        let last = *idx.last().unwrap();
        // Everything synced survives, then a gap-free prefix of the rest,
        // and the hard state is never ahead of the entries.
        assert!(last >= 10, "seed {seed}: lost synced entries: {idx:?}");
        assert_eq!(idx, (1..=last).collect::<Vec<_>>(), "seed {seed}");
        assert!(r[0].hard_state.commit <= last, "seed {seed}");
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
