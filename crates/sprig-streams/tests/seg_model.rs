//! The engine-stored stream log against a model of Kafka partitions:
//! random appends (plain and idempotent, with retries), reads at random
//! offsets, DeleteRecords, flushes, clean restarts and crashes that lose
//! everything written after the last flush. Every acknowledged append
//! must survive, with the same offsets and bytes.

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::doc_markdown,
    missing_docs
)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use proptest::prelude::*;
use sprig_engine::log::SyncMode;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_streams::seg::part::{Outcome, PartKey};
use sprig_streams::seg::testing::{TempDir, batch_with, records_of};
use sprig_streams::seg::{SegConfig, SegLog, batch::Head};

type Rec = (i64, Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Debug, Clone)]
enum Op {
    Append(u32, u8),
    Idem(u8, u32, u8),
    Retry(u8, u32, u8),
    Read(u32, u16),
    DeleteRecords(u32, u16),
    Flush,
    Restart,
    Crash,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0u32..3, 1u8..5).prop_map(|(p, n)| Op::Append(p, n)),
        4 => (0u8..3, 0u32..3, 1u8..4).prop_map(|(w, p, n)| Op::Idem(w, p, n)),
        2 => (0u8..3, 0u32..3, 0u8..5).prop_map(|(w, p, k)| Op::Retry(w, p, k)),
        3 => (0u32..3, any::<u16>()).prop_map(|(p, o)| Op::Read(p, o)),
        1 => (0u32..3, any::<u16>()).prop_map(|(p, o)| Op::DeleteRecords(p, o)),
        2 => Just(Op::Flush),
        1 => Just(Op::Restart),
        2 => Just(Op::Crash),
    ]
}

fn key(p: u32) -> PartKey {
    PartKey {
        tenant: 3,
        stream: 9,
        partition: p,
    }
}

fn engine(dir: &Path) -> Engine {
    let mut c = EngineConfig::new(dir.join("engine"));
    c.store = StoreKind::Rocks;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    // The engine of the run before can still be closing: the stream log's
    // apply observer may hold the last handle on the log writer's thread,
    // and the store's lock is free only when that thread lets it go.
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

fn open(dir: &Path) -> SegLog {
    SegLog::open(
        &engine(dir),
        dir.join("streams"),
        SegConfig {
            segment_bytes: 2048,
            index_interval: 256,
            ..SegConfig::default()
        },
    )
    .unwrap()
}

fn files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for p in 0..3 {
        let d = dir.join("streams").join("t3").join(format!("s9-p{p}"));
        if let Ok(rd) = std::fs::read_dir(&d) {
            for e in rd.flatten() {
                out.push((e.path(), e.metadata().unwrap().len()));
            }
        }
    }
    out
}

#[derive(Default)]
struct Model {
    parts: HashMap<u32, (Vec<Rec>, i64, i64)>,
    /// (producer, partition) -> (next sequence, last batches: (first seq, records, base))
    producers: HashMap<
        (u8, u32),
        (
            i32,
            Vec<(i32, Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>, i64)>,
        ),
    >,
}

fn read_all(l: &SegLog, k: PartKey) -> Vec<Rec> {
    let b = l.bounds(k);
    let mut out = Vec::new();
    let mut at = b.log_start;
    while at < b.high_watermark {
        let (_, bytes) = l.read(k, at, 4096).unwrap();
        assert!(!bytes.is_empty(), "no bytes at {at} < {}", b.high_watermark);
        let mut pos = 0;
        let mut next = at;
        while let Some(h) = Head::parse(&bytes[pos..]) {
            next = h.next_offset();
            pos += h.total;
            if pos >= bytes.len() {
                break;
            }
        }
        out.extend(records_of(&bytes).into_iter().filter(|r| r.0 >= at));
        at = next;
    }
    out
}

async fn run(ops: Vec<Op>) {
    let d = TempDir::new("model");
    let mut l = open(d.path());
    let mut m = Model::default();
    let mut synced = files(d.path());
    let mut n = 0u32;
    let w = Duration::from_secs(10);
    for op in ops {
        match op {
            Op::Append(p, k) => {
                let recs: Vec<_> = (0..k)
                    .map(|_| {
                        n += 1;
                        (
                            Some(format!("k{}", n % 7).into_bytes()),
                            Some(n.to_be_bytes().to_vec()),
                        )
                    })
                    .collect();
                let o = l
                    .append(key(p), &batch_with(&recs, 1, None), w)
                    .await
                    .unwrap();
                let Outcome::Appended { base, .. } = o else {
                    panic!("{o:?}")
                };
                let e = m.parts.entry(p).or_default();
                assert_eq!(base, e.1);
                for (i, (k, v)) in recs.into_iter().enumerate() {
                    e.0.push((base + i64::try_from(i).unwrap(), k, v));
                }
                e.1 += i64::from(k);
            }
            Op::Idem(wr, p, k) => {
                let recs: Vec<_> = (0..k)
                    .map(|_| {
                        n += 1;
                        (Some(b"i".to_vec()), Some(n.to_be_bytes().to_vec()))
                    })
                    .collect();
                let st = m.producers.entry((wr, p)).or_default();
                let seq = st.0;
                let b = batch_with(&recs, 1, Some((100 + i64::from(wr), 0, seq)));
                let o = l.append(key(p), &b, w).await.unwrap();
                let Outcome::Appended { base, .. } = o else {
                    panic!("{o:?}")
                };
                st.0 += i32::from(k);
                st.1.push((seq, recs.clone(), base));
                if st.1.len() > 5 {
                    st.1.remove(0);
                }
                let e = m.parts.entry(p).or_default();
                assert_eq!(base, e.1);
                for (i, (k, v)) in recs.into_iter().enumerate() {
                    e.0.push((base + i64::try_from(i).unwrap(), k, v));
                }
                e.1 += i64::from(k);
            }
            Op::Retry(wr, p, k) => {
                let Some(st) = m.producers.get(&(wr, p)) else {
                    continue;
                };
                if st.1.is_empty() {
                    continue;
                }
                let (seq, recs, base) = st.1[usize::from(k) % st.1.len()].clone();
                let b = batch_with(&recs, 1, Some((100 + i64::from(wr), 0, seq)));
                let o = l.append(key(p), &b, w).await.unwrap();
                let start = m.parts.get(&p).map_or(0, |e| e.2);
                let want = if base < start { -1 } else { base };
                assert!(
                    matches!(o, Outcome::Duplicate { base: b, .. } if b == want),
                    "{o:?} want {want}"
                );
            }
            Op::Read(p, o) => {
                let e = m.parts.entry(p).or_default();
                let span = (e.1 - e.2 + 1).max(1);
                let at = e.2 + i64::from(o) % span;
                let (b, bytes) = l.read(key(p), at, 300).unwrap();
                assert_eq!((b.log_start, b.high_watermark), (e.2, e.1));
                let got: Vec<Rec> = records_of(&bytes)
                    .into_iter()
                    .filter(|r| r.0 >= at)
                    .collect();
                let want: Vec<Rec> =
                    e.0.iter()
                        .filter(|r| r.0 >= at)
                        .take(got.len())
                        .cloned()
                        .collect();
                assert_eq!(got, want);
                assert_eq!(got.is_empty(), at == e.1);
            }
            Op::DeleteRecords(p, o) => {
                let e = m.parts.entry(p).or_default();
                let span = (e.1 - e.2 + 1).max(1);
                let to = e.2 + i64::from(o) % span;
                if l.bounds(key(p)).high_watermark == 0 {
                    continue;
                }
                assert_eq!(l.delete_records(key(p), to).unwrap(), to.max(e.2));
                e.2 = to.max(e.2);
                e.0.retain(|r| r.0 >= to);
                synced = files(d.path());
            }
            Op::Flush => {
                l.flush().unwrap();
                synced = files(d.path());
            }
            Op::Restart => {
                drop(l);
                l = open(d.path());
                synced = files(d.path());
            }
            Op::Crash => {
                drop(l);
                for (path, len) in files(d.path()) {
                    let keep = synced
                        .iter()
                        .find(|(p, _)| *p == path)
                        .map_or(len / 2, |(_, s)| (*s).min(len));
                    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                    f.set_len(keep).unwrap();
                }
                l = open(d.path());
                synced = files(d.path());
            }
        }
        for (p, e) in &m.parts {
            let b = l.bounds(key(*p));
            assert_eq!((b.log_start, b.high_watermark), (e.2, e.1), "partition {p}");
        }
    }
    for (p, e) in &m.parts {
        assert_eq!(read_all(&l, key(*p)), e.0, "partition {p}");
    }
}

#[test]
fn random_histories_match_the_model() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let cases = std::env::var("SPRIG_MODEL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    proptest!(ProptestConfig::with_cases(cases), |(ops in proptest::collection::vec(op(), 1..50))| {
        rt.block_on(run(ops));
    });
}
