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
    // A test that reopens its directory can find the engine it dropped
    // still closing: the stream log's apply observer may hold the last
    // handle on the log writer's thread, and the store's lock is free only
    // when that thread lets it go.
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match Engine::open(c.clone()) {
            Ok(e) => return e,
            Err(e)
                if e.message.contains("lock hold by current process")
                    && std::time::Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("{e}"),
        }
    }
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

/// The unit `key(_)`'s appends ride on: tenant 5's.
const U5: sprig_engine::UnitId = sprig_engine::UnitId(5);

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

/// A producer's later batch that arrives before its first (the first
/// went to a former leader) waits for it; a retry of a batch older than
/// the ones remembered is a duplicate, never out of order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_producers_batches_stay_in_order_across_a_leader_move() {
    let d = TempDir::new("idem-move");
    let l = open(d.path());
    let rec = |n: usize| -> Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> {
        (0..n)
            .map(|_| (Some(b"k".to_vec()), Some(b"v".to_vec())))
            .collect()
    };
    let at = |seq: i32, n: usize| batch_with(&rec(n), 1, Some((88, 0, seq)));
    // The second batch comes first: refused, and nothing appended.
    assert!(matches!(
        put(&l, key(0), &at(5, 5)).await,
        Outcome::Refused { code: 45, .. }
    ));
    assert_eq!(l.bounds(key(0)).high_watermark, 0);
    // The first batch's retry, then the second's.
    assert_eq!(base(&put(&l, key(0), &at(0, 5)).await), 0);
    assert_eq!(base(&put(&l, key(0), &at(5, 5)).await), 5);
    // Five more batches: the first ones are no longer remembered.
    for i in 0..5 {
        assert_eq!(
            base(&put(&l, key(0), &at(10 + i, 1)).await),
            10 + i64::from(i)
        );
    }
    // A late retry of the first batch, and of the second: duplicates,
    // offsets no longer known.
    for seq in [0, 5] {
        assert!(matches!(
            put(&l, key(0), &at(seq, 5)).await,
            Outcome::Duplicate { base: -1, .. }
        ));
    }
    assert!(matches!(
        put(&l, key(0), &at(14, 1)).await,
        Outcome::Duplicate { base: 14, .. }
    ));
    // A batch reaching past the last one taken, or a gap: out of order.
    assert!(matches!(
        put(&l, key(0), &at(13, 3)).await,
        Outcome::Refused { code: 45, .. }
    ));
    assert!(matches!(
        put(&l, key(0), &at(20, 1)).await,
        Outcome::Refused { code: 45, .. }
    ));
    assert_eq!(l.bounds(key(0)).high_watermark, 15);
    // A producer that comes back after its state expired starts a new
    // epoch at 0.
    assert!(matches!(
        put(&l, key(0), &batch_with(&rec(1), 1, Some((89, 0, 7)))).await,
        Outcome::Refused { code: 45, .. }
    ));
    assert_eq!(
        base(&put(&l, key(0), &batch_with(&rec(1), 1, Some((89, 1, 0)))).await),
        15
    );
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
        delete_retention_ms: Some(5 * 3_600_000),
        created_ms: 1,
    };
    let made = r.create(5, t.clone()).await.unwrap().unwrap();
    assert_eq!(made.stream_id, 1);
    // A compacted stream's tombstone time is kept with it.
    assert_eq!(
        r.topics(5).unwrap()[0].delete_retention_ms,
        Some(5 * 3_600_000)
    );
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
    let unit = sprig_engine::Engine::unit_of_tenant(key(0).tenant).0;
    let index = c.log_ticket_of(unit).unwrap().index();
    let dl = tokio::time::Instant::now() + W;
    assert_eq!(
        base(
            &l.outcome(
                Pending {
                    unit,
                    index,
                    slot: s0
                },
                dl
            )
            .await
            .unwrap()
        ),
        0
    );
    assert_eq!(
        base(
            &l.outcome(
                Pending {
                    unit,
                    index,
                    slot: s1
                },
                dl
            )
            .await
            .unwrap()
        ),
        0
    );
    assert_eq!(l.bounds(key(0)).high_watermark, 2);
    assert_eq!(l.bounds(key(1)).high_watermark, 3);
}

/// A transaction of one tenant publishing into another tenant's stream
/// commits across the two units: the records appear exactly when it
/// commits (with its rows), never when it rolls back, and survive a
/// restart; with and without waits between the protocol's steps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_into_another_tenants_stream_commits_with_the_transaction() {
    use sprig_engine::txn::TxnOptions;
    for wait in [false, true] {
        let d = TempDir::new("xtenant");
        let l = open(d.path());
        let e = l.engine().clone();
        e.txn_shared().set_cross_unit_wait(wait);
        let other = PartKey {
            tenant: 9,
            stream: 1,
            partition: 0,
        };
        let row = |i: u8| {
            let mut k = 3u64.to_be_bytes().to_vec();
            k.extend_from_slice(b"row");
            k.push(i);
            k
        };
        // Rolled back: no record.
        let mut t = e.begin(TxnOptions::mysql()).unwrap();
        t.put(row(0), b"v".to_vec()).unwrap();
        l.append_in(&mut t, other, &batch_bytes(2, 1, 1)).unwrap();
        t.rollback();
        assert_eq!(l.bounds(other).high_watermark, 0);
        // Committed: the records and the rows, together.
        let mut t = e.begin(TxnOptions::mysql()).unwrap();
        t.put(row(1), b"v".to_vec()).unwrap();
        t.put(row(2), b"v".to_vec()).unwrap();
        let slot = l.append_in(&mut t, other, &batch_bytes(3, 1, 1)).unwrap();
        assert_eq!(
            l.bounds(other).high_watermark,
            0,
            "nothing before the commit"
        );
        let c = t.commit().unwrap();
        c.wait_async().await.unwrap();
        let unit = sprig_engine::Engine::unit_of_tenant(other.tenant).0;
        let index = c.log_ticket_of(unit).unwrap().index();
        let out = l
            .outcome(
                Pending { unit, index, slot },
                tokio::time::Instant::now() + W,
            )
            .await
            .unwrap();
        assert_eq!(base(&out), 0, "wait={wait}");
        assert_eq!(l.bounds(other).high_watermark, 3);
        let mut r = e.begin(TxnOptions::mysql()).unwrap();
        assert!(r.get_snapshot(&row(2)).unwrap().is_some());
        drop(r);
        drop((l, e));
        let l = open(d.path());
        assert_eq!(l.bounds(other).high_watermark, 3, "after a restart");
    }
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
    let unit = sprig_engine::Engine::unit_of_tenant(key(0).tenant).0;
    let index = c.log_ticket_of(unit).unwrap().index();
    let out = l
        .outcome(
            Pending { unit, index, slot },
            tokio::time::Instant::now() + W,
        )
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
    let floor = l.engine().log_floor_of(U5, FLOOR).unwrap();
    put(&l, key(0), &batch_bytes(2, 1, 10)).await;
    put(&l, key(1), &batch_bytes(4, 1, 10)).await;
    // The manifest can't be written (its temporary file can't be made).
    let dir = d.path().join("streams");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
    let failed = l.flush();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(failed.is_err());
    assert_eq!(l.engine().log_floor_of(U5, FLOOR), Some(floor));
    // Nothing looks clean that isn't: a second, plain pass doesn't move the
    // floor without a manifest either; it writes one.
    l.flush().unwrap();
    assert!(l.engine().log_floor_of(U5, FLOOR).unwrap() > floor);
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
    // more entries on the log the stream log rides on, and trims that log
    // past the floor.
    {
        let e = engine(d.path());
        let floor = e.log_floor_of(U5, FLOOR).unwrap();
        e.clear_log_floor(FLOOR).unwrap();
        for _ in 0..20 {
            e.consensus()
                .propose(
                    U5,
                    sprig_engine::log::Payload::new().into_bytes(),
                    sprig_engine::Durability::Local,
                )
                .unwrap()
                .wait_applied()
                .unwrap();
        }
        e.checkpoint().unwrap();
        assert!(e.last_applied_of(U5) > floor + 1);
        assert!(e.log_first_index_of(U5).is_none_or(|f| f > floor + 1));
        // The engine itself refuses to hand the observer what it lost.
        let err = e
            .observe_units(
                std::sync::Arc::new(NoObserver)
                    as std::sync::Arc<dyn sprig_engine::observe::ApplyObserver>,
                &[(U5, floor)],
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
    fn applied(&self, _: sprig_engine::UnitId, _: &[sprig_engine::log::LogEntry]) {}
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
    let durable = e.consensus().durable_index(U5);
    assert!(e.log_floor_of(U5, FLOOR).unwrap_or(0) <= durable);
    assert!(
        read_manifest(&d.path().join("streams"))
            .unwrap()
            .unwrap()
            .floors
            .iter()
            .all(|(u, f)| *u != U5.0 || *f <= durable)
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

/// A stream append is an entry of its tenant's unit, not of the node
/// unit, and the stream log keeps that unit's log above its floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_ride_on_the_tenants_unit() {
    let d = TempDir::new("tenant-unit");
    let l = open(d.path());
    let e = l.engine().clone();
    let c = e.consensus();
    let (node0, t0) = (c.applied_index(e.notes_unit()), c.applied_index(U5));
    put(&l, key(0), &batch_bytes(3, 1, 10)).await;
    let other = PartKey {
        tenant: 6,
        stream: 1,
        partition: 0,
    };
    let pending = l
        .propose_many(&[
            (key(1), batch_bytes(2, 1, 10)),
            (other, batch_bytes(4, 1, 10)),
        ])
        .unwrap();
    assert_eq!(
        pending.iter().map(|p| p.unit).collect::<Vec<_>>(),
        vec![5, 6]
    );
    for p in pending {
        l.outcome(p, tokio::time::Instant::now() + W).await.unwrap();
    }
    assert_eq!(c.applied_index(e.notes_unit()), node0);
    // The stream log resolves an append from its apply observer, which runs
    // before the engine publishes the unit's new applied index: wait for it.
    let until = std::time::Instant::now() + W;
    while c.applied_index(U5) < t0 + 2 && std::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(c.applied_index(U5) >= t0 + 2);
    l.flush().unwrap();
    assert!(e.log_floor_of(U5, FLOOR).unwrap() >= t0 + 2);
    assert!(e.log_floor_of(sprig_engine::UnitId(6), FLOOR).is_some());
    drop(e);
    drop(l);
    let l = open(d.path());
    assert_eq!(l.bounds(key(0)).high_watermark, 3);
    assert_eq!(l.bounds(key(1)).high_watermark, 2);
    assert_eq!(l.bounds(other).high_watermark, 4);
}

/// A node that takes a tenant's leadership gives its partitions a new
/// leader epoch, above every epoch before, also after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taking_a_tenants_leadership_starts_a_new_epoch() {
    let d = TempDir::new("unit-epoch");
    let l = open(d.path());
    let e = l.engine().clone();
    put(&l, key(0), &batch_bytes(3, 1, 10)).await;
    let first = l.epoch_of(5);
    assert_eq!(l.epoch_of(6), first);
    e.lead(U5);
    let second = l.epoch_of(5);
    assert!(second > first);
    assert_eq!(l.epoch_of(6), first);
    put(&l, key(0), &batch_bytes(2, 1, 10)).await;
    assert_eq!(l.bounds(key(0)).epoch, second);
    // The first epoch's records end where the second's begin.
    assert_eq!(l.epoch_end(key(0), first), (first, 3));
    l.flush().unwrap();
    drop(l);
    drop(e);
    let l = open(d.path());
    assert!(l.epoch_of(5) > second);
    assert_eq!(l.bounds(key(0)).high_watermark, 5);
}

/// A data directory moved from one unit to units: the node unit carries
/// the old unit's applied index but its log holds none of those entries.
/// The stream log, whose manifest floor is on the node unit, still opens
/// (the node unit counts what it applied as durable) and serves appends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opens_on_a_directory_moved_to_units() {
    use sprig_engine::store::{StoreBatch, WalMode};
    let d = TempDir::new("moved");
    {
        let l = open(d.path());
        put(&l, key(0), &batch_bytes(3, 1, 10)).await;
        l.flush().unwrap();
    }
    // What the move leaves: the node unit applied through 50 in the store,
    // with no entries of it in the commit log.
    {
        let e = engine(d.path());
        let mut b = StoreBatch::new();
        b.put(
            sprig_engine::Cf::Data,
            sprig_engine::keys::unit_meta_key(sprig_engine::NODE_UNIT.0, b"applied"),
            50u64.to_be_bytes(),
        );
        e.store().write(&b, WalMode::Sync).unwrap();
    }
    let e = engine(d.path());
    assert_eq!(e.consensus().applied_index(sprig_engine::NODE_UNIT), 50);
    assert_eq!(e.consensus().durable_index(sprig_engine::NODE_UNIT), 50);
    let l = SegLog::open(&e, d.path().join("streams"), cfg()).unwrap();
    assert_eq!(l.bounds(key(0)).high_watermark, 3);
    let o = put(&l, key(0), &batch_bytes(2, 1, 10)).await;
    assert_eq!(base(&o), 3);
    l.flush().unwrap();
    assert!(e.log_floor(FLOOR).unwrap() >= 50);
}

/// A unit snapshot carries its tenant's partitions: the target node gets
/// the same records at the same offsets, appends continue after them, and
/// they survive a restart there. Both forms: the stream and the files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unit_snapshot_carries_the_tenants_streams() {
    for files in [false, true] {
        let (da, db) = (TempDir::new("snap-src"), TempDir::new("snap-dst"));
        let a = open(da.path());
        for i in 0..20 {
            put(&a, key(i % 2), &batch_bytes(3, i64::from(i), 10)).await;
        }
        let other = PartKey {
            tenant: 6,
            stream: 1,
            partition: 0,
        };
        put(&a, other, &batch_bytes(2, 1, 10)).await;
        let want: Vec<_> = (0..2).map(|p| all(&a, key(p))).collect();
        let b = open(db.path());
        let eb = b.engine().clone();
        if files {
            let x = a
                .engine()
                .export_unit(U5, &da.path().join("export"), 1 << 20)
                .unwrap();
            assert!(x.files.iter().any(|f| f.ends_with("parts.snap")));
            eb.ingest_unit(U5, &x.files).unwrap();
        } else {
            let mut bytes = Vec::new();
            a.engine().export_unit_to(U5, &mut bytes).unwrap();
            eb.ingest_unit_from(U5, &mut bytes.as_slice()).unwrap();
        }
        for p in 0..2 {
            let (x, y) = (b.bounds(key(p)), a.bounds(key(p)));
            assert_eq!(
                (x.log_start, x.high_watermark),
                (y.log_start, y.high_watermark),
                "files {files}"
            );
            assert_eq!(all(&b, key(p)), want[p as usize]);
        }
        // Only tenant 5's unit came over.
        assert_eq!(b.bounds(other).high_watermark, 0);
        let o = put(&b, key(0), &batch_bytes(2, 1, 99)).await;
        assert_eq!(base(&o), 30);
        b.flush().unwrap();
        drop(eb);
        drop(b);
        let b = open(db.path());
        assert_eq!(b.bounds(key(0)).high_watermark, 32);
        assert_eq!(b.bounds(key(1)).high_watermark, 30);
    }
}

/// Partition groups: partition `p` rides on its tenant's group unit
/// `p % n`, and a log holding partitions keeps the groups it was written
/// with when reopened with another setting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_groups_put_partitions_in_units_of_their_own() {
    assert_eq!(group_of_unit(group_unit(5, 2)), Some((5, 2)));
    assert_eq!(group_of_unit(5), None);
    assert_eq!(group_of_unit(Engine::unit_of_tenant(u64::MAX).0), None);
    let d = TempDir::new("groups");
    let grouped = SegConfig { groups: 2, ..cfg() };
    {
        let l = SegLog::open(&engine(d.path()), d.path().join("seg"), grouped.clone()).unwrap();
        assert_eq!(l.unit_of_part(key(0)), group_unit(5, 0));
        assert_eq!(l.unit_of_part(key(3)), group_unit(5, 1));
        for p in 0..4 {
            assert_eq!(base(&put(&l, key(p), &batch_bytes(2, 1_000, 10)).await), 0);
        }
        assert_eq!(base(&put(&l, key(3), &batch_bytes(1, 1_000, 10)).await), 2);
        let e = l.engine();
        assert!(
            e.consensus()
                .applied_index(sprig_engine::UnitId(group_unit(5, 1)))
                >= 3
        );
        l.flush().unwrap();
    }
    let l = SegLog::open(
        &engine(d.path()),
        d.path().join("seg"),
        SegConfig { groups: 0, ..cfg() },
    )
    .unwrap();
    assert_eq!(
        l.groups(),
        2,
        "kept the groups the partitions were written with"
    );
    assert_eq!(base(&put(&l, key(3), &batch_bytes(1, 1_000, 10)).await), 3);
    assert_eq!(all(&l, key(3)).len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unclaimed_outcomes_of_other_units_never_push_out_a_fresh_one() {
    // A replica records the outcome of every append it applies, also the
    // ones other nodes proposed (nobody here waits for those). Past the
    // cap, the oldest go: never the fresh outcome of the lowest unit, a
    // partition group led here whose producer is about to read it.
    let d = TempDir::new("results-cap");
    let grouped = SegConfig { groups: 3, ..cfg() };
    let l = SegLog::open(&engine(d.path()), d.path().join("seg"), grouped).unwrap();
    lock(&l.inner.results).kept = 8;
    let mut unclaimed = Vec::new();
    for _ in 0..20 {
        unclaimed.push(l.propose(key(2), &batch_bytes(1, 1_000, 10)).unwrap());
    }
    let last = unclaimed.last().unwrap().index;
    let until = std::time::Instant::now() + W;
    while l
        .engine()
        .consensus()
        .applied_index(sprig_engine::UnitId(group_unit(5, 2)))
        < last
    {
        assert!(std::time::Instant::now() < until, "group 2 not applied");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for i in 0..3 {
        assert_eq!(
            base(&put(&l, key(0), &batch_bytes(1, 1_000, 10)).await),
            i,
            "group 0's own append {i} kept its outcome"
        );
    }
    // The oldest unclaimed ones went; the newest are still there.
    let newest = *unclaimed.last().unwrap();
    assert!(
        lock(&l.inner.results)
            .take(&(newest.unit, newest.index, newest.slot))
            .is_some()
    );
    let oldest = unclaimed[0];
    assert!(
        lock(&l.inner.results)
            .take(&(oldest.unit, oldest.index, oldest.slot))
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_topic_ids_are_given_once_and_kept() {
    let d = TempDir::new("fanin-ids");
    let e = engine(d.path());
    let r = records::StreamRecords::new(e.clone());
    let a = r.fan_in_id(7, 70, 1 << 48).await.unwrap();
    let b = r.fan_in_id(7, 71, 1 << 48).await.unwrap();
    assert!(records::is_fan_in(a) && records::is_fan_in(b) && a != b);
    assert_eq!(r.fan_in_id(7, 70, 1 << 48).await.unwrap(), a);
    assert_eq!(r.fan_in_target(7, a).unwrap(), Some((70, 1 << 48)));
    assert_eq!(r.fan_in_target(7, b).unwrap(), Some((71, 1 << 48)));
    // Another reader's ids are its own.
    assert_eq!(r.fan_in_target(8, a).unwrap(), None);
    assert_eq!(r.fan_in_id(8, 70, 1 << 48).await.unwrap(), a);
}
