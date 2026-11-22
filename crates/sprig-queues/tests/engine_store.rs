//! The engine-backed message store against the in-memory one: the same
//! rules (fence, `uk_msg`, ordering, outbox, delays, ledger), a random
//! model comparison, and recovery after a restart.

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    missing_docs
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proptest::prelude::*;
use sprig_engine::log::SyncMode;
use sprig_engine::{Durability, Engine, EngineConfig, StoreKind};
use sprig_queues::enginestore::EngineQueues;
use sprig_queues::memstore::MemStore;
use sprig_queues::store::{
    DelayedRow, LaneState, MsgId, MsgRef, MsgStore, OutDest, OutKind, OutboxRow, StoreError,
    StoredMsg, WriteBatch,
};

struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-queues-store-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        Self(p)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn engine(dir: &Path) -> Engine {
    let mut c = EngineConfig::new(dir);
    c.store = StoreKind::Rocks;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    Engine::open(c).unwrap()
}

fn id(n: u64) -> MsgId {
    let mut a = [0u8; 16];
    a[..8].copy_from_slice(&n.to_be_bytes());
    a[6] = 0x70 | (a[6] & 0x0F);
    a
}

fn msg(lane: u16, pclass: u8, seq: u64, n: u64) -> StoredMsg {
    StoredMsg {
        lane,
        pclass,
        seq,
        msg_id: id(n),
        fmt: 0,
        exchange: "ex".into(),
        routing_key: format!("rk{n}"),
        props: Bytes::from_static(b"props"),
        body: Bytes::from(vec![
            u8::try_from(n % 251).unwrap();
            10 + usize::try_from(n % 50).unwrap()
        ]),
        expires_at_ms: n.is_multiple_of(3).then_some(i64::try_from(n).unwrap()),
        delivery_count: 0,
        deaths: n.is_multiple_of(5).then(|| Bytes::from_static(b"death")),
    }
}

fn r(m: &StoredMsg) -> MsgRef {
    MsgRef {
        lane: m.lane,
        pclass: m.pclass,
        seq: m.seq,
        msg_id: m.msg_id,
    }
}

fn out(n: u64, kind: OutKind) -> OutboxRow {
    OutboxRow {
        out_id: id(1_000_000 + n),
        kind,
        dest: OutDest::Queues(vec![7, 8]),
        msg_id: id(n),
        fmt: 1,
        exchange: "dlx".into(),
        routing_key: "k".into(),
        props: Bytes::from_static(b"p"),
        body: Bytes::from_static(b"b"),
        deaths: Some(Bytes::from_static(b"x-death")),
    }
}

async fn both_rules(s: &dyn MsgStore) {
    let c = s.claim(7, 0).await.unwrap();
    assert_eq!((c.epoch, c.state.next_seq, c.count), (1, 1, 0));
    let ms: Vec<StoredMsg> = (1..=10)
        .map(|n| msg(0, u8::from(n % 2 == 0), n, n))
        .collect();
    let state = LaneState {
        next_seq: 11,
        delivered_hwm: 0,
        ready_count: 10,
        ready_bytes: 100,
    };
    s.write(&WriteBatch {
        queue: 7,
        lane: 0,
        epoch: 1,
        inserts: ms.clone(),
        forwarded: vec![id(500)],
        state,
        ..WriteBatch::default()
    })
    .await
    .unwrap();
    // Priority class first, then sequence.
    let all = s.load(7, 0, None, 100).await.unwrap();
    let order: Vec<u64> = all.iter().map(|m| m.seq).collect();
    assert_eq!(order, vec![1, 3, 5, 7, 9, 2, 4, 6, 8, 10]);
    assert_eq!(all[0], ms[0]);
    let after = s.load(7, 0, Some((0, 5)), 2).await.unwrap();
    assert_eq!(after.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![7, 9]);
    assert_eq!(
        s.existing(7, &[id(3), id(99), id(500)]).await.unwrap(),
        vec![id(3), id(500)]
    );
    // uk_msg: a duplicate fails the whole batch.
    let dup = s
        .write(&WriteBatch {
            queue: 7,
            lane: 0,
            epoch: 1,
            inserts: vec![msg(0, 0, 11, 3)],
            state,
            ..WriteBatch::default()
        })
        .await;
    assert!(matches!(dup, Err(StoreError::Failed(_))));
    // Acks (a contiguous run and singles), a requeue count, a dead letter.
    s.write(&WriteBatch {
        queue: 7,
        lane: 0,
        epoch: 1,
        deletes: vec![r(&ms[0]), r(&ms[2]), r(&ms[4]), r(&ms[1])],
        counts: vec![(r(&ms[5]), 3)],
        outbox: vec![out(1, OutKind::DeadLetter)],
        state,
        ..WriteBatch::default()
    })
    .await
    .unwrap();
    let left = s.load(7, 0, None, 100).await.unwrap();
    assert_eq!(
        left.iter().map(|m| m.seq).collect::<Vec<_>>(),
        vec![7, 9, 4, 6, 8, 10]
    );
    assert_eq!(left.iter().find(|m| m.seq == 6).unwrap().delivery_count, 3);
    assert_eq!(s.stored(7).await.unwrap(), 6);
    // A delete whose id doesn't match does nothing.
    s.write(&WriteBatch {
        queue: 7,
        lane: 0,
        epoch: 1,
        deletes: vec![MsgRef {
            msg_id: id(4242),
            ..r(&ms[6])
        }],
        state,
        ..WriteBatch::default()
    })
    .await
    .unwrap();
    assert_eq!(s.stored(7).await.unwrap(), 6);
    // A takeover fences the old owner.
    let c2 = s.claim(7, 0).await.unwrap();
    assert_eq!((c2.epoch, c2.count, c2.state), (2, 6, state));
    let fenced = s
        .write(&WriteBatch {
            queue: 7,
            lane: 0,
            epoch: 1,
            state,
            ..WriteBatch::default()
        })
        .await;
    assert_eq!(fenced, Err(StoreError::Fenced));
    assert_eq!(s.purge(7, 0, 1, &[]).await, Err(StoreError::Fenced));
    assert_eq!(s.purge(7, 0, 2, &[9, 10]).await.unwrap(), 4);
    assert_eq!(
        s.load(7, 0, None, 100)
            .await
            .unwrap()
            .iter()
            .map(|m| m.seq)
            .collect::<Vec<_>>(),
        vec![9, 10]
    );
    // Outbox.
    let ob = s.outbox(10).await.unwrap();
    assert_eq!(ob, vec![out(1, OutKind::DeadLetter)]);
    assert_eq!(s.outbox_rows(&[id(1_000_001)]).await.unwrap().len(), 1);
    s.outbox_done(&[id(1_000_001)]).await.unwrap();
    assert!(s.outbox(10).await.unwrap().is_empty());
    // Delays.
    for (due, n) in [(300, 1u64), (100, 2), (200, 3)] {
        s.delay(&DelayedRow {
            exchange_id: 9,
            exchange: "dx".into(),
            due_ms: due,
            msg_id: id(n),
            routing_key: "rk".into(),
            fmt: 0,
            props: Bytes::from_static(b"p"),
            body: Bytes::from_static(b"b"),
        })
        .await
        .unwrap();
    }
    assert_eq!(s.next_due().await.unwrap(), Some(100));
    assert_eq!(s.release_due(250, 10).await.unwrap(), 2);
    assert_eq!(s.next_due().await.unwrap(), Some(300));
    let due = s.outbox(10).await.unwrap();
    assert_eq!(due.len(), 2);
    assert!(due.iter().all(|o| o.kind == OutKind::Delayed));
    let mut ids: Vec<MsgId> = due.iter().map(|o| o.msg_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![id(2), id(3)]);
    // Ledger purge.
    assert_eq!(s.purge_ledger(i64::MAX, i64::MAX).await.unwrap(), 1);
    assert!(s.existing(7, &[id(500)]).await.unwrap().is_empty());
    s.drop_queue(7).await.unwrap();
    assert_eq!(s.stored(7).await.unwrap(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_store_keeps_the_memory_stores_rules() {
    both_rules(&MemStore::new()).await;
    let d = Dir::new("rules");
    let q = EngineQueues::new(engine(&d.0), Durability::Local);
    both_rules(q.store(42, 1).as_ref()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_lanes_and_outbox_survive_a_restart() {
    let d = Dir::new("restart");
    let before;
    {
        let q = EngineQueues::new(engine(&d.0), Durability::Local);
        let s = q.store(42, 1);
        s.claim(3, 1).await.unwrap();
        s.write(&WriteBatch {
            queue: 3,
            lane: 1,
            epoch: 1,
            inserts: (1..=500).map(|n| msg(1, 0, n, n)).collect(),
            outbox: vec![out(9, OutKind::Publish)],
            state: LaneState {
                next_seq: 501,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await
        .unwrap();
        let acks: Vec<MsgRef> = (1..=200).map(|n| r(&msg(1, 0, n, n))).collect();
        s.write(&WriteBatch {
            queue: 3,
            lane: 1,
            epoch: 1,
            deletes: acks,
            state: LaneState {
                next_seq: 501,
                delivered_hwm: 200,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await
        .unwrap();
        before = s.load(3, 1, None, 10_000).await.unwrap();
        assert_eq!(before.len(), 300);
    }
    let q = EngineQueues::new(engine(&d.0), Durability::Local);
    let s = q.store(42, 1);
    assert_eq!(s.load(3, 1, None, 10_000).await.unwrap(), before);
    let c = s.claim(3, 1).await.unwrap();
    assert_eq!((c.epoch, c.count, c.state.delivered_hwm), (2, 300, 200));
    assert_eq!(s.outbox(10).await.unwrap().len(), 1);
    // Another vhost of the same tenant sees nothing of it.
    let other = q.store(42, 2);
    assert!(other.load(3, 1, None, 10).await.unwrap().is_empty());
    assert!(other.outbox(10).await.unwrap().is_empty());
}

#[derive(Debug, Clone)]
enum Step {
    Publish(u8, u8),
    Ack(Vec<usize>),
    Count(usize, u32),
    Claim,
    Purge(Vec<usize>),
    StaleWrite,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (1u8..6, 0u8..3).prop_map(|(n, p)| Step::Publish(n, p)),
        3 => proptest::collection::vec(0usize..64, 0..8).prop_map(Step::Ack),
        1 => (0usize..64, 0u32..9).prop_map(|(i, n)| Step::Count(i, n)),
        1 => Just(Step::Claim),
        1 => proptest::collection::vec(0usize..64, 0..4).prop_map(Step::Purge),
        1 => Just(Step::StaleWrite),
    ]
}

async fn run_model(steps: Vec<Step>, e: &EngineQueues, scope: u32) {
    let mem = MemStore::new();
    let eng = e.store(1, scope);
    let stores: [&dyn MsgStore; 2] = [&mem, eng.as_ref()];
    let mut epoch = 0;
    for s in &stores {
        epoch = s.claim(1, 0).await.unwrap().epoch;
    }
    let mut next = 1u64;
    let mut n = 0u64;
    for st in steps {
        let live = mem.load(1, 0, None, 10_000).await.unwrap();
        let pick = |i: &usize| live.get(i % live.len().max(1)).cloned();
        let state = LaneState {
            next_seq: next,
            ..LaneState::default()
        };
        let mut b = WriteBatch {
            queue: 1,
            lane: 0,
            epoch,
            state,
            ..WriteBatch::default()
        };
        match st {
            Step::Publish(k, p) => {
                for _ in 0..k {
                    n += 1;
                    b.inserts.push(msg(0, p, next, n));
                    next += 1;
                }
                b.state.next_seq = next;
            }
            Step::Ack(is) => b.deletes = is.iter().filter_map(pick).map(|m| r(&m)).collect(),
            Step::Count(i, c) => b.counts = pick(&i).map(|m| (r(&m), c)).into_iter().collect(),
            Step::Claim => {
                let mut got = Vec::new();
                for s in &stores {
                    let c = s.claim(1, 0).await.unwrap();
                    got.push((c.epoch, c.count, c.bytes));
                }
                assert_eq!(got[0], got[1]);
                epoch = got[0].0;
                continue;
            }
            Step::Purge(keep) => {
                let keep: Vec<u64> = keep.iter().filter_map(pick).map(|m| m.seq).collect();
                let a = mem.purge(1, 0, epoch, &keep).await;
                let z = eng.purge(1, 0, epoch, &keep).await;
                assert_eq!(a, z);
                continue;
            }
            Step::StaleWrite => b.epoch = epoch.saturating_sub(1),
        }
        let a = mem.write(&b).await;
        let z = eng.write(&b).await;
        assert_eq!(a, z, "{b:?}");
        let x = mem.load(1, 0, None, 10_000).await.unwrap();
        let y = eng.load(1, 0, None, 10_000).await.unwrap();
        assert_eq!(x, y);
        assert_eq!(mem.stored(1).await.unwrap(), eng.stored(1).await.unwrap());
    }
}

#[test]
fn random_owner_batches_match_the_memory_store() {
    let d = Dir::new("model");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let e = Arc::new(EngineQueues::new(engine(&d.0), Durability::Local));
    let scope = std::sync::atomic::AtomicU32::new(0);
    let cases = std::env::var("SPRIG_MODEL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    proptest!(ProptestConfig::with_cases(cases), |(steps in proptest::collection::vec(step(), 1..60))| {
        let s = scope.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        rt.block_on(run_model(steps, &e, s));
    });
}

/// A requeued message (its delivery count raised) that is acknowledged
/// before the owner's batch is written: the count and the delete are in
/// one batch, and the message stays deleted, after a restart too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_count_and_a_delete_of_one_message_in_one_batch_delete_it() {
    let d = Dir::new("count-delete");
    {
        let q = EngineQueues::new(engine(&d.0), Durability::Local);
        let s = q.store(42, 1);
        let c = s.claim(5, 0).await.unwrap();
        let m = msg(0, 0, 1, 1);
        s.write(&WriteBatch {
            queue: 5,
            lane: 0,
            epoch: c.epoch,
            inserts: vec![m.clone()],
            state: LaneState {
                next_seq: 2,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await
        .unwrap();
        s.write(&WriteBatch {
            queue: 5,
            lane: 0,
            epoch: c.epoch,
            counts: vec![(r(&m), 1)],
            deletes: vec![r(&m)],
            state: LaneState {
                next_seq: 2,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await
        .unwrap();
        assert!(s.load(5, 0, None, 10).await.unwrap().is_empty());
        assert!(s.existing(5, &[m.msg_id]).await.unwrap().is_empty());
    }
    let q = EngineQueues::new(engine(&d.0), Durability::Local);
    let s = q.store(42, 1);
    s.claim(5, 0).await.unwrap();
    assert!(s.load(5, 0, None, 10).await.unwrap().is_empty());
}

/// A claim while the old owner's write is in flight starts from that
/// write's lane state (no sequence number is given twice), and the old
/// owner's next write is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_starts_from_the_newest_written_state_and_fences() {
    let d = Dir::new("claim-newest");
    let q = EngineQueues::new(engine(&d.0), Durability::Local);
    let s = q.store(42, 1);
    let c1 = s.claim(6, 0).await.unwrap();
    s.write(&WriteBatch {
        queue: 6,
        lane: 0,
        epoch: c1.epoch,
        inserts: (1..=10).map(|n| msg(0, 0, n, n)).collect(),
        state: LaneState {
            next_seq: 11,
            ..LaneState::default()
        },
        ..WriteBatch::default()
    })
    .await
    .unwrap();
    let c2 = s.claim(6, 0).await.unwrap();
    assert_eq!(c2.epoch, c1.epoch + 1);
    assert_eq!(c2.state.next_seq, 11);
    assert_eq!(c2.count, 10);
    let stale = s
        .write(&WriteBatch {
            queue: 6,
            lane: 0,
            epoch: c1.epoch,
            inserts: vec![msg(0, 0, 11, 11)],
            state: LaneState {
                next_seq: 12,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await;
    assert_eq!(stale, Err(StoreError::Fenced));
}

/// The forward ledger: a purge past the general cutoff takes only the rows
/// of other ids; change ids (UUID version 8) stay until their own cutoff,
/// in both stores.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn change_ids_outlive_other_ledger_rows_in_both_stores() {
    let d = Dir::new("ledger");
    let q = EngineQueues::new(engine(&d.0), Durability::Local);
    let eng = q.store(42, 1);
    let mem = MemStore::new();
    let id = |change: bool, i: u8| -> MsgId {
        let mut b = [i; 16];
        b[6] = if change { 0x80 } else { 0x70 } | (i & 0x0f);
        b
    };
    for s in [&mem as &dyn MsgStore, eng.as_ref()] {
        let c = s.claim(8, 0).await.unwrap();
        s.write(&WriteBatch {
            queue: 8,
            lane: 0,
            epoch: c.epoch,
            forwarded: (0..6u8).map(|i| id(i % 2 == 0, i)).collect(),
            state: LaneState {
                next_seq: 1,
                ..LaneState::default()
            },
            ..WriteBatch::default()
        })
        .await
        .unwrap();
        let all: Vec<MsgId> = (0..6u8).map(|i| id(i % 2 == 0, i)).collect();
        assert_eq!(s.existing(8, &all).await.unwrap().len(), 6);
        assert_eq!(s.purge_ledger(i64::MAX, 0).await.unwrap(), 3);
        let left = s.existing(8, &all).await.unwrap();
        assert_eq!(left.len(), 3);
        assert!(left.iter().all(sprig_queues::store::is_change_id));
        assert_eq!(s.purge_ledger(i64::MAX, i64::MAX).await.unwrap(), 3);
    }
}
