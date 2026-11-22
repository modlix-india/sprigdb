use std::path::Path;
use std::time::Duration;

use sprig_engine::log::SyncMode;
use sprig_engine::{Engine, EngineConfig, StoreKind};

use super::testing::{TempDir, batch_bytes, batch_with, records_of};
use super::*;

fn engine(dir: &Path) -> Engine {
    let mut c = EngineConfig::new(dir.join("engine"));
    c.store = StoreKind::Rocks;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    Engine::open(c).unwrap()
}

fn cfg() -> SegConfig {
    SegConfig {
        segment_bytes: 4096,
        index_interval: 512,
        ..SegConfig::default()
    }
}

fn open(dir: &Path) -> SegLog {
    SegLog::open(&engine(dir), dir.join("streams"), cfg()).unwrap()
}

fn key(p: u32) -> PartKey {
    PartKey {
        tenant: 5,
        stream: 1,
        partition: p,
    }
}

const W: Duration = Duration::from_secs(10);

async fn put(l: &SegLog, k: PartKey, b: &[u8]) -> Outcome {
    l.append(k, b, W).await.unwrap()
}

fn base(o: &Outcome) -> i64 {
    match o {
        Outcome::Appended { base, .. } | Outcome::Duplicate { base, .. } => *base,
        Outcome::Refused { code, message } => panic!("refused {code}: {message}"),
    }
}

fn all(l: &SegLog, k: PartKey) -> Vec<(i64, Option<Vec<u8>>, Option<Vec<u8>>)> {
    let b = l.bounds(k);
    let mut out = Vec::new();
    let mut at = b.log_start;
    while at < b.high_watermark {
        let (_, bytes) = l
            .read(k, at, 1 << 20)
            .unwrap_or_else(|_| panic!("read {at}"));
        if bytes.is_empty() {
            break;
        }
        let mut last = at;
        for r in records_of(&bytes) {
            if r.0 >= at {
                last = r.0;
                out.push(r);
            }
        }
        // Next batch: after the last whole batch returned.
        let mut pos = 0;
        let mut next = at;
        while let Some(h) = super::batch::Head::parse(&bytes[pos..]) {
            next = h.next_offset();
            pos += h.total;
            if pos >= bytes.len() {
                break;
            }
        }
        at = next.max(last + 1);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_get_offsets_and_read_back_byte_for_byte() {
    let d = TempDir::new("basic");
    let l = open(d.path());
    let b1 = batch_bytes(3, 1_000, 10);
    let o = put(&l, key(0), &b1).await;
    assert_eq!(base(&o), 0);
    assert_eq!(base(&put(&l, key(0), &batch_bytes(2, 2_000, 10)).await), 3);
    assert_eq!(base(&put(&l, key(1), &batch_bytes(4, 3_000, 10)).await), 0);
    let b = l.bounds(key(0));
    assert_eq!((b.log_start, b.high_watermark, b.epoch), (0, 5, 1));
    let (_, bytes) = l.read(key(0), 0, 1).unwrap();
    // Stored as received apart from the base offset and the epoch.
    assert_eq!(bytes.len(), b1.len());
    assert_eq!(&bytes[16..], &b1[16..]);
    assert!(super::batch::crc_ok(&bytes));
    assert_eq!(all(&l, key(0)).len(), 5);
    assert!(matches!(l.read(key(0), 6, 100), Err(Ok(OutOfRange(_)))));
    assert_eq!(l.read(key(0), 5, 100).unwrap().1.len(), 0);
    assert_eq!(l.offset_for_time(key(0), 2_001).unwrap(), Some((4, 2_001)));
    assert_eq!(l.offset_for_time(key(1), -3).unwrap(), Some((3, 3_003)));
    assert_eq!(l.offset_for_time(key(0), 9_999).unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_producers_dedupe_and_fence() {
    let d = TempDir::new("idem");
    let l = open(d.path());
    let rec = |n: usize| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
        (0..n)
            .map(|_| (Some(b"k".to_vec()), Some(b"v".to_vec())))
            .collect()
    };
    let b0 = batch_with(&rec(2), 1, Some((77, 0, 0)));
    assert_eq!(base(&put(&l, key(0), &b0).await), 0);
    // A retry appends nothing and answers the first offsets.
    assert!(matches!(
        put(&l, key(0), &b0).await,
        Outcome::Duplicate { base: 0, .. }
    ));
    // A gap.
    let gap = batch_with(&rec(1), 1, Some((77, 0, 5)));
    assert!(matches!(
        put(&l, key(0), &gap).await,
        Outcome::Refused { code: 45, .. }
    ));
    let b1 = batch_with(&rec(1), 1, Some((77, 0, 2)));
    assert_eq!(base(&put(&l, key(0), &b1).await), 2);
    // A new epoch must start at 0; an older one is fenced.
    let e1 = batch_with(&rec(1), 1, Some((77, 1, 3)));
    assert!(matches!(
        put(&l, key(0), &e1).await,
        Outcome::Refused { code: 45, .. }
    ));
    let e1 = batch_with(&rec(1), 1, Some((77, 1, 0)));
    assert_eq!(base(&put(&l, key(0), &e1).await), 3);
    assert!(matches!(
        put(&l, key(0), &b1).await,
        Outcome::Refused { code: 47, .. }
    ));
    // Producer state survives a restart (manifest plus replay).
    l.flush().unwrap();
    let e2 = batch_with(&rec(1), 1, Some((77, 1, 1)));
    assert_eq!(base(&put(&l, key(0), &e2).await), 4);
    drop(l);
    let l = open(d.path());
    assert!(matches!(
        put(&l, key(0), &e2).await,
        Outcome::Duplicate { base: 4, .. }
    ));
    assert!(matches!(
        put(&l, key(0), &e1).await,
        Outcome::Duplicate { base: 3, .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_replays_unsynced_appends_from_the_log() {
    let d = TempDir::new("crash");
    let mut expect = Vec::new();
    let root = d.path().join("streams");
    // Every segment file and its length at the last flush.
    let mut synced: Vec<(std::path::PathBuf, u64)> = Vec::new();
    {
        let l = open(d.path());
        for i in 0..40 {
            let b = batch_bytes(1 + (i % 3), 10_000 + i64::from(i), 100);
            put(&l, key(u32::try_from(i % 2).unwrap()), &b).await;
            if i == 15 {
                l.flush().unwrap();
                for k in [key(0), key(1)] {
                    for e in std::fs::read_dir(part_dir(&root, &k)).unwrap().flatten() {
                        synced.push((e.path(), e.metadata().unwrap().len()));
                    }
                }
            }
        }
        expect.push(all(&l, key(0)));
        expect.push(all(&l, key(1)));
        drop(l);
    }
    // The crash: everything written after the flush is lost (files cut
    // back to their synced length; newer files torn in half).
    for k in [key(0), key(1)] {
        for e in std::fs::read_dir(part_dir(&root, &k)).unwrap().flatten() {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(e.path())
                .unwrap();
            let n = f.metadata().unwrap().len();
            let keep = synced
                .iter()
                .find(|(p, _)| *p == e.path())
                .map_or(n / 2, |(_, l)| *l);
            f.set_len(keep).unwrap();
        }
    }
    let l = open(d.path());
    assert_eq!(all(&l, key(0)), expect[0]);
    assert_eq!(all(&l, key(1)), expect[1]);
    // A new leader epoch after the restart, recorded at the next append.
    assert_eq!(l.epoch(), 2);
    let o = put(&l, key(0), &batch_bytes(1, 99_999, 1)).await;
    let (e, end) = l.epoch_end(key(0), 1);
    assert_eq!((e, end), (1, base(&o)));
    assert_eq!(l.epoch_end(key(0), 2), (2, base(&o) + 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_drops_whole_segments_and_delete_records_moves_the_start() {
    let d = TempDir::new("retention");
    let l = open(d.path());
    for i in 0..30 {
        put(&l, key(0), &batch_bytes(2, i64::from(i), 200)).await;
    }
    let (segs, total, first) = {
        let p = l.inner.existing(&key(0)).unwrap();
        let st = p.lock();
        (st.segments.len(), st.bytes(), st.segments[0].len)
    };
    assert!(segs > 3, "{segs} segments");
    // Exactly the first segment is over the limit.
    l.set_policy(
        5,
        1,
        Policy {
            retention_bytes: i64::try_from(total - first).unwrap(),
            retention_ms: -1,
            ..Policy::default()
        },
    );
    l.maintain().unwrap();
    let b = l.bounds(key(0));
    assert!(b.log_start > 0 && b.high_watermark == 60, "{b:?}");
    assert_eq!(
        l.inner.existing(&key(0)).unwrap().lock().segments.len(),
        segs - 1
    );
    assert!(matches!(l.read(key(0), 0, 10), Err(Ok(OutOfRange(_)))));
    assert_eq!(all(&l, key(0)).first().map(|r| r.0), Some(b.log_start));
    // Time retention: everything rolled is older than a millisecond.
    l.set_policy(
        5,
        1,
        Policy {
            retention_ms: 1,
            ..Policy::default()
        },
    );
    l.maintain().unwrap();
    let start = l.bounds(key(0)).log_start;
    assert_eq!(l.inner.existing(&key(0)).unwrap().lock().segments.len(), 1);
    assert!(start > b.log_start);
    assert_eq!(l.delete_records(key(0), -1).unwrap(), 60);
    drop(l);
    let l = open(d.path());
    assert_eq!(l.bounds(key(0)).log_start, 60);
    // Dropping the stream refuses later appends and removes its files.
    l.drop_stream(5, 1).unwrap();
    assert!(matches!(
        put(&l, key(0), &batch_bytes(1, 0, 1)).await,
        Outcome::Refused { code: 3, .. }
    ));
    assert!(!part_dir(&d.path().join("streams"), &key(0)).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_keeps_each_keys_newest_record() {
    let d = TempDir::new("compact");
    let l = open(d.path());
    l.set_policy(
        5,
        1,
        Policy {
            compact: true,
            delete_retention_ms: 0,
            retention_ms: -1,
            ..Policy::default()
        },
    );
    for round in 0..10u8 {
        let recs: Vec<_> = (0..4u8)
            .map(|k| {
                let v = if round == 9 && k == 0 {
                    None
                } else {
                    Some(vec![round; 300])
                };
                (Some(vec![b'a' + k]), v)
            })
            .collect();
        put(&l, key(0), &batch_with(&recs, 1, None)).await;
    }
    let before = all(&l, key(0)).len();
    l.maintain().unwrap();
    let after = all(&l, key(0));
    assert!(after.len() < before);
    // Every key once at most in the rolled part, newest value; offsets keep
    // their gaps (all ascending).
    assert!(after.windows(2).all(|w| w[0].0 < w[1].0));
    for k in 1..4u8 {
        let last = after
            .iter()
            .rev()
            .find(|r| r.1 == Some(vec![b'a' + k]))
            .unwrap();
        assert_eq!(last.2, Some(vec![9; 300]));
    }
    assert_eq!(l.bounds(key(0)).high_watermark, 40);
    drop(l);
    let l = open(d.path());
    assert_eq!(all(&l, key(0)), after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_waits_wake_on_append() {
    let d = TempDir::new("wait");
    let l = open(d.path());
    let l2 = l.clone();
    let w = tokio::spawn(async move {
        let t = tokio::time::Instant::now();
        l2.wait(&[(key(3), 0)], t + Duration::from_secs(5)).await;
        t.elapsed()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    put(&l, key(3), &batch_bytes(1, 0, 1)).await;
    let took = w.await.unwrap();
    assert!(took < Duration::from_secs(4), "{took:?}");
    // Nothing new: the wait ends at the deadline.
    let t = tokio::time::Instant::now();
    l.wait(&[(key(3), 1)], t + Duration::from_millis(100)).await;
    assert!(t.elapsed() >= Duration::from_millis(90));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offsets_and_groups_are_engine_records() {
    use sprig_proto_kafka::group::{CommittedOffset, GroupState, StoredGroup};
    let d = TempDir::new("records");
    let e = engine(d.path());
    let r = records::StreamRecords::new(e.clone());
    let t = records::TopicRecord {
        name: "db.s".into(),
        stream_id: 0,
        partitions: 3,
        retention_ms: None,
        retention_bytes: None,
        compact: Some(true),
        max_message_bytes: None,
        created_ms: 1,
    };
    let made = r.create(5, t.clone()).await.unwrap().unwrap();
    assert_eq!(made.stream_id, 1);
    assert!(r.create(5, t.clone()).await.unwrap().is_none());
    let mut t2 = t.clone();
    t2.name = "db.t".into();
    assert_eq!(r.create(5, t2).await.unwrap().unwrap().stream_id, 2);
    assert_eq!(r.topics(5).unwrap().len(), 2);
    assert!(r.topics(6).unwrap().is_empty());
    let off = |s, p, o| CommittedOffset {
        stream_id: s,
        partition: p,
        offset: o,
        leader_epoch: Some(1),
        metadata: None,
    };
    r.commit_offsets(5, "g", &[off(1, 0, 10), off(1, 1, 20)], 0)
        .await
        .unwrap();
    r.commit_offsets(5, "g", &[off(1, 0, 11)], 0).await.unwrap();
    let got = r.offsets(5, "g", None).unwrap();
    assert_eq!(got, vec![off(1, 0, 11), off(1, 1, 20)]);
    assert_eq!(
        r.offsets(5, "g", Some(&[(1, 1)])).unwrap(),
        vec![off(1, 1, 20)]
    );
    // An offset commit inside another transaction is part of it.
    let mut txn = e.begin(sprig_engine::txn::TxnOptions::mysql()).unwrap();
    records::put_offsets(&mut txn, 5, "g", 0, &[off(1, 0, 99)], 0)
        .await
        .unwrap();
    txn.rollback();
    assert_eq!(r.offsets(5, "g", Some(&[(1, 0)])).unwrap()[0].offset, 11);
    let g = StoredGroup {
        group: "g".into(),
        protocol_type: "consumer".into(),
        generation: 4,
    };
    r.save_group(5, &g, GroupState::Stable).await.unwrap();
    assert_eq!(r.load_group(5, "g").unwrap(), Some(g.clone()));
    assert_eq!(r.groups(5).unwrap(), vec![g]);
    r.delete_group(5, "g").await.unwrap();
    assert!(r.load_group(5, "g").unwrap().is_none());
    assert!(r.offsets(5, "g", None).unwrap().is_empty());
    assert_eq!(
        r.delete(5, "db.s").await.unwrap().map(|x| x.stream_id),
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_inside_a_transaction_commit_with_it_or_not_at_all() {
    use sprig_engine::txn::TxnOptions;
    let d = TempDir::new("txn");
    let l = open(d.path());
    let e = l.engine().clone();
    // Rolled back: nothing appended.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    t.put(
        b"\x00\x00\x00\x00\x00\x00\x00\x05row".to_vec(),
        b"v".to_vec(),
    )
    .unwrap();
    l.append_in(&mut t, key(0), &batch_bytes(2, 1, 1)).unwrap();
    t.rollback();
    // A statement rolled back inside a committed transaction: its append goes.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    t.begin_statement();
    l.append_in(&mut t, key(0), &batch_bytes(9, 1, 1)).unwrap();
    t.rollback_statement();
    t.begin_statement();
    let s0 = l.append_in(&mut t, key(0), &batch_bytes(2, 1, 1)).unwrap();
    let s1 = l.append_in(&mut t, key(1), &batch_bytes(3, 1, 1)).unwrap();
    let c = t.commit().unwrap();
    c.wait_async().await.unwrap();
    let index = c.log_ticket().unwrap().index();
    let dl = tokio::time::Instant::now() + W;
    assert_eq!(
        base(&l.outcome(Pending { index, slot: s0 }, dl).await.unwrap()),
        0
    );
    assert_eq!(
        base(&l.outcome(Pending { index, slot: s1 }, dl).await.unwrap()),
        0
    );
    assert_eq!(l.bounds(key(0)).high_watermark, 2);
    assert_eq!(l.bounds(key(1)).high_watermark, 3);
}

/// Exactly-once consume-transform-produce on engine transactions: the
/// consumer's offset commit and the produced records commit together or
/// not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_offset_commit_and_appends_share_one_transaction() {
    use sprig_engine::txn::TxnOptions;
    use sprig_proto_kafka::group::CommittedOffset;
    let d = TempDir::new("eos");
    let l = open(d.path());
    let e = l.engine().clone();
    let r = records::StreamRecords::new(e.clone());
    let offset = |o: i64| CommittedOffset {
        stream_id: 77,
        partition: 0,
        offset: o,
        leader_epoch: None,
        metadata: None,
    };
    // Rolled back: neither the offset nor the output.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    records::put_offsets(&mut t, key(0).tenant, "g", 0, &[offset(5)], 1)
        .await
        .unwrap();
    l.append_in(&mut t, key(0), &batch_bytes(3, 1, 1)).unwrap();
    t.rollback();
    assert!(r.offsets(key(0).tenant, "g", None).unwrap().is_empty());
    assert_eq!(l.bounds(key(0)).high_watermark, 0);
    // Committed: both, at the same commit.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    records::put_offsets(&mut t, key(0).tenant, "g", 0, &[offset(5)], 1)
        .await
        .unwrap();
    let slot = l.append_in(&mut t, key(0), &batch_bytes(3, 1, 1)).unwrap();
    let c = t.commit().unwrap();
    c.wait_async().await.unwrap();
    let index = c.log_ticket().unwrap().index();
    let out = l
        .outcome(Pending { index, slot }, tokio::time::Instant::now() + W)
        .await
        .unwrap();
    assert_eq!(base(&out), 0);
    let got = r.offsets(key(0).tenant, "g", None).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].offset, 5);
    assert_eq!(l.bounds(key(0)).high_watermark, 3);
    // After a restart both are still there.
    drop((l, r, e));
    let l = open(d.path());
    assert_eq!(l.bounds(key(0)).high_watermark, 3);
    let r = records::StreamRecords::new(l.engine().clone());
    assert_eq!(r.offsets(key(0).tenant, "g", None).unwrap()[0].offset, 5);
}

/// A manifest write that fails leaves the floor where the last manifest
/// put it and the state to redo: the next flush writes it, and a crash in
/// between loses nothing that was acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_keeps_the_floor_and_redoes_the_work() {
    use std::os::unix::fs::PermissionsExt;
    let d = TempDir::new("flushfail");
    let l = open(d.path());
    put(&l, key(0), &batch_bytes(3, 1, 10)).await;
    l.flush().unwrap();
    let floor = l.engine().log_floor(FLOOR).unwrap();
    put(&l, key(0), &batch_bytes(2, 1, 10)).await;
    put(&l, key(1), &batch_bytes(4, 1, 10)).await;
    // The manifest can't be written (its temporary file can't be made).
    let dir = d.path().join("streams");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
    let failed = l.flush();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(failed.is_err());
    assert_eq!(l.engine().log_floor(FLOOR), Some(floor));
    // Nothing looks clean that isn't: a second, plain pass doesn't move the
    // floor without a manifest either; it writes one.
    l.flush().unwrap();
    assert!(l.engine().log_floor(FLOOR).unwrap() > floor);
    let m = read_manifest(&dir).unwrap().unwrap();
    let p1 = m.parts.iter().find(|p| p.key == key(1)).unwrap();
    assert_eq!(p1.next_offset, 4);
    drop(l);
    let l = open(d.path());
    assert_eq!(l.bounds(key(0)).high_watermark, 5);
    assert_eq!(l.bounds(key(1)).high_watermark, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opens_after_a_binary_without_floors_trimmed_the_log() {
    let d = TempDir::new("trimmed");
    {
        let l = open(d.path());
        put(&l, key(0), &batch_bytes(3, 1, 10)).await;
        l.flush().unwrap();
    }
    // An older binary: it doesn't know the stream log's floor, writes
    // engine records and trims the commit log past the floor.
    {
        let e = engine(d.path());
        let floor = e.log_floor(FLOOR).unwrap();
        e.clear_log_floor(FLOOR).unwrap();
        let r = records::StreamRecords::new(e.clone());
        for i in 0..20 {
            let t = records::TopicRecord {
                name: format!("db.s{i}"),
                stream_id: 0,
                partitions: 1,
                retention_ms: None,
                retention_bytes: None,
                compact: None,
                max_message_bytes: None,
                created_ms: 1,
            };
            r.create(5, t).await.unwrap();
        }
        e.checkpoint().unwrap();
        assert!(e.last_applied() > floor + 1);
        assert!(e.log_first_index().is_none_or(|f| f > floor + 1));
        // The engine itself refuses to hand the observer what it lost.
        let err = e
            .observe(
                std::sync::Arc::new(NoObserver)
                    as std::sync::Arc<dyn sprig_engine::observe::ApplyObserver>,
                floor,
            )
            .unwrap_err();
        assert!(err.message.contains("no longer holds"), "{}", err.message);
    }
    let l = open(d.path());
    assert_eq!(all(&l, key(0)).len(), 3);
    let o = put(&l, key(0), &batch_bytes(2, 1, 10)).await;
    assert_eq!(base(&o), 3);
    l.flush().unwrap();
    drop(l);
    // And again, now that the floor is kept.
    let l = open(d.path());
    assert_eq!(l.bounds(key(0)).high_watermark, 5);
}

struct NoObserver;

impl sprig_engine::observe::ApplyObserver for NoObserver {
    fn applied(&self, _: &[sprig_engine::log::LogEntry]) {}
}

#[test]
fn catching_up_starts_where_the_log_still_holds_entries() {
    // Floor kept: nothing changes.
    assert_eq!(catch_up_start(10, Some(5), 20), None);
    assert_eq!(catch_up_start(10, Some(11), 20), None);
    // Caught up already.
    assert_eq!(catch_up_start(20, None, 20), None);
    // Trimmed past the floor: just below the oldest held entry.
    assert_eq!(catch_up_start(10, Some(15), 20), Some(14));
    // Trimmed through the end: from the last applied entry.
    assert_eq!(catch_up_start(10, None, 20), Some(20));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_floor_never_passes_what_the_commit_log_holds_durably() {
    use sprig_engine::Durability;
    let d = TempDir::new("relaxed");
    let mut c = EngineConfig::new(d.path().join("engine"));
    c.store = StoreKind::Rocks;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    // Relaxed appends are synced only after a minute.
    c.log.relaxed_sync = Duration::from_secs(60);
    c.budget.engine_mb = 64;
    let e = Engine::open(c).unwrap();
    let l = SegLog::open(
        &e,
        d.path().join("streams"),
        SegConfig {
            durability: Durability::LocalRelaxed,
            ..cfg()
        },
    )
    .unwrap();
    put(&l, key(0), &batch_bytes(3, 1, 10)).await;
    // Applied, not yet durable: the flush doesn't write a manifest or
    // raise the floor past the durable index.
    assert!(l.flush().is_err());
    let durable = e.consensus().durable_index(e.unit());
    assert!(e.log_floor(FLOOR).unwrap_or(0) <= durable);
    assert!(
        read_manifest(&d.path().join("streams"))
            .unwrap()
            .unwrap()
            .floor
            <= durable
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offsets_on_instances_are_kept_apart() {
    use sprig_proto_kafka::group::CommittedOffset;
    let d = TempDir::new("records-on");
    let e = engine(d.path());
    let r = records::StreamRecords::new(e.clone());
    let o = |stream: u64, partition: i32, offset: i64| CommittedOffset {
        stream_id: stream,
        partition,
        offset,
        leader_epoch: None,
        metadata: Some("m".into()),
    };
    // The tenant's own stream and the same stream id on a member's
    // instance (a fan-in reader's position), in one commit.
    r.commit_offsets_on(5, "g", &[(0, o(7, 0, 10)), (99, o(7, 0, 20))], 1)
        .await
        .unwrap();
    r.commit_offsets_on(5, "g", &[(99, o(7, 1, 21))], 2)
        .await
        .unwrap();
    let mut all = r.offsets_on(5, "g").unwrap();
    all.sort_by_key(|(i, c)| (*i, c.stream_id, c.partition));
    assert_eq!(all.len(), 3);
    assert_eq!((all[0].0, all[0].1.offset), (0, 10));
    assert_eq!((all[1].0, all[1].1.offset), (99, 20));
    assert_eq!((all[2].0, all[2].1.partition, all[2].1.offset), (99, 1, 21));
    // The plain read keeps to the tenant's own instance.
    let own = r.offsets(5, "g", None).unwrap();
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].offset, 10);
    // Deleting the group removes every instance's offsets.
    r.delete_group(5, "g").await.unwrap();
    assert!(r.offsets_on(5, "g").unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_mark_moves_once() {
    use sprig_engine::txn::TxnOptions;
    let d = TempDir::new("records-mark");
    let e = engine(d.path());
    let r = records::StreamRecords::new(e.clone());
    // The first run marks it, in its transaction; a rolled-back run
    // leaves it unmarked.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    assert!(
        records::move_offset(&mut t, 5, "replay", 41, 0, 0, 1, 1)
            .await
            .unwrap()
    );
    t.rollback();
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    assert!(
        records::move_offset(&mut t, 5, "replay", 41, 0, 0, 1, 1)
            .await
            .unwrap()
    );
    t.commit().unwrap().wait_async().await.unwrap();
    // A repeat finds it moved, and writes nothing.
    let mut t = e.begin(TxnOptions::mysql()).unwrap();
    assert!(
        !records::move_offset(&mut t, 5, "replay", 41, 0, 0, 1, 2)
            .await
            .unwrap()
    );
    t.rollback();
    let got = r.offsets(5, "replay", None).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].stream_id, got[0].offset), (41, 1));
}
