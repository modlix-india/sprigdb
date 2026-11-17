#![allow(clippy::unwrap_used, clippy::cast_possible_truncation)]

use std::sync::atomic::AtomicUsize;
use std::sync::{Mutex, PoisonError};

use sprig_binlog::{Gtid, Value};
use sprig_common::ids::{DbId, GroupId, ShardId, TenantId};
use sprig_feed::{EventId, Image, ShardRef};

use super::*;
use crate::memlog::{Fault, MemLog};

const T: TenantId = TenantId(7);

fn c1() -> ClusterId {
    ClusterId::new("dc-a-c1").unwrap()
}

fn token(seq: u64) -> ResumeToken {
    ResumeToken {
        tenant: T,
        clamp: seq,
        cluster: c1(),
        segment: 0,
        pos: GtidPos::from_gtids([Gtid {
            domain: 1,
            server: 1,
            seq,
        }]),
        until: None,
        progress: false,
    }
}

/// An outbox insert of event `n` with key `key`.
fn outbox_event(seq: u64, row: u32, n: u8, key: &str) -> ChangeEvent {
    let mut id = [0u8; 16];
    id[0] = n;
    id[1] = (seq % 256) as u8;
    id[2] = row as u8;
    ChangeEvent {
        id: EventId {
            cluster: c1(),
            gtid: Gtid {
                domain: 1,
                server: 1,
                seq,
            },
            row,
        },
        token: token(seq),
        tenant: T,
        db: DbId(1),
        db_name: "shop".into(),
        object: 0,
        table: "__sprig_eo_5".into(),
        shard: ShardRef {
            group: GroupId(1),
            shard: ShardId(1),
        },
        key: Image(vec![("event_id".into(), Value::Bytes(id.to_vec()))]),
        op: Op::Insert,
        before: None,
        after: Some(Image(vec![
            ("event_id".into(), Value::Bytes(id.to_vec())),
            ("partition_no".into(), Value::Null),
            ("k".into(), Value::Bytes(key.as_bytes().to_vec())),
            (
                "v".into(),
                Value::Bytes(format!("v{seq}.{row}").into_bytes()),
            ),
            ("headers".into(), Value::Null),
        ])),
        delta: None,
        hlc: seq,
        txn: Gtid {
            domain: 1,
            server: 1,
            seq,
        },
        ddl: None,
        hint: None,
    }
}

/// A binlog of transactions; seq is the index + 1.
#[derive(Clone)]
struct Script(Arc<Vec<Vec<ChangeEvent>>>);

struct ScriptFeed {
    txns: Arc<Vec<Vec<ChangeEvent>>>,
    next: usize,
    out: std::collections::VecDeque<FeedItem>,
    /// A wait after taking a transaction and before emitting it, as the
    /// tail's catalog lookups: `next` isn't cancel-safe then.
    lookup: Option<Duration>,
    /// The binlog's head when the log's own writes follow the script
    /// ([`OwnLog`]): progress reaches it.
    head: Option<Arc<AtomicU64>>,
}

#[async_trait::async_trait]
impl FeedSource for ScriptFeed {
    async fn next(&mut self) -> Result<FeedItem> {
        loop {
            if let Some(i) = self.out.pop_front() {
                return Ok(i);
            }
            if let Some(t) = self.txns.get(self.next) {
                self.next += 1;
                let seq = self.next as u64;
                if let Some(d) = self.lookup {
                    tokio::time::sleep(d).await;
                }
                for e in t {
                    self.out.push_back(FeedItem::Event(Box::new(e.clone())));
                }
                self.out.push_back(FeedItem::TxnEnd { token: token(seq) });
                continue;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            let head = self.head.as_ref().map_or(0, |h| h.load(Ordering::SeqCst));
            return Ok(FeedItem::Progress {
                token: token((self.txns.len() as u64).max(head)),
            });
        }
    }
}

#[async_trait::async_trait]
impl FeedOpener for Script {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let next = match start {
            Start::After(t) => t.pos.get(1).map_or(0, |g| g.seq as usize),
            Start::At { .. } => 0,
        };
        Ok(Box::new(ScriptFeed {
            txns: Arc::clone(&self.0),
            next,
            out: std::collections::VecDeque::new(),
            lookup: None,
            head: None,
        }))
    }
}

/// [`Script`] whose feed waits 20 ms between taking a transaction and
/// emitting it.
#[derive(Clone)]
struct SlowScript(Script);

#[async_trait::async_trait]
impl FeedOpener for SlowScript {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let next = match start {
            Start::After(t) => t.pos.get(1).map_or(0, |g| g.seq as usize),
            Start::At { .. } => 0,
        };
        Ok(Box::new(ScriptFeed {
            txns: Arc::clone(&self.0.0),
            next,
            out: std::collections::VecDeque::new(),
            lookup: Some(Duration::from_millis(20)),
            head: None,
        }))
    }
}

/// Regression (seen in live runs): the run loop's deadlines must not drop
/// a feed read half-way. A tail takes a group from its source and then
/// awaits catalog lookups; a batch or idle-checkpoint deadline that
/// cancelled that read lost the group (6 new instances lost their first
/// record).
#[tokio::test(start_paused = true)]
async fn deadlines_never_drop_a_half_read_feed_item() {
    let feed = script(40);
    let log = MemLog::new(&start());
    let mut s = sequencer(&log, &feed);
    s.feed = Arc::new(SlowScript(feed.clone()));
    s.opts.batch_window = Duration::from_millis(1);
    s.opts.idle_checkpoint = Duration::from_millis(5);
    assert_eq!(run_for(&s, Duration::from_secs(10)).await, RunEnd::Stopped);
    check(&log, &feed);
}

/// `stream.sequencer_batch_ms` as the job resolved it (the live window):
/// a short window appends each transaction as it arrives, a long one
/// gathers the whole script into one append.
#[tokio::test(start_paused = true)]
async fn the_live_batch_window_sets_how_records_are_batched() {
    let mut batches = Vec::new();
    for ms in [1, 5000] {
        let feed = script(20);
        let log = MemLog::new(&start());
        let mut s = sequencer(&log, &feed);
        s.feed = Arc::new(SlowScript(feed.clone()));
        s.opts.batch_window = Duration::from_secs(3600);
        s.opts.live_window = Some(Arc::new(AtomicU64::new(ms)));
        s.opts.idle_checkpoint = Duration::from_secs(60);
        assert_eq!(run_for(&s, Duration::from_secs(10)).await, RunEnd::Stopped);
        check(&log, &feed);
        batches.push(s.stats.batches.load(Ordering::Relaxed));
    }
    assert!(batches[0] >= 20, "{batches:?}");
    assert_eq!(batches[1], 1, "{batches:?}");
}

fn script(n: u64) -> Script {
    let keys = ["a", "b", "c", "d", "e"];
    let txns = (1..=n)
        .map(|seq| {
            (0..(seq % 3 + 1) as u32)
                .map(|row| outbox_event(seq, row, 9, keys[((seq + u64::from(row)) % 5) as usize]))
                .collect()
        })
        .collect();
    Script(Arc::new(txns))
}

fn sequencer(log: &MemLog, feed: &Script) -> Sequencer {
    Sequencer {
        filter: Filter::Outbox {
            db: DbId(1),
            table: "__sprig_eo_5".into(),
            partitions: 4,
        },
        owned: Vec::new(),
        store: Arc::new(log.clone()),
        feed: Arc::new(feed.clone()),
        marks: Arc::new(NoWatermarks),
        opts: SeqOpts::default(),
        stats: Arc::default(),
    }
}

fn start() -> Start {
    Start::At {
        cluster: c1(),
        pos: GtidPos::new(),
    }
}

/// Every record of the script once, in feed order per partition, with
/// offsets from 0 without gaps.
fn check(log: &MemLog, feed: &Script) {
    let mut want: BTreeMap<PartitionNo, Vec<Source>> = BTreeMap::new();
    for t in feed.0.iter() {
        for e in t {
            let (r, _) = outbox_record(e, 4).unwrap();
            want.entry(r.partition).or_default().push(r.source);
        }
    }
    for (p, sources) in want {
        let got = log.records(p);
        let offsets: Vec<u64> = got.iter().map(|s| s.offset.0).collect();
        assert_eq!(offsets, (0..sources.len() as u64).collect::<Vec<_>>());
        let got: Vec<Source> = got.into_iter().map(|s| s.record.source).collect();
        assert_eq!(got, sources, "partition {p:?} in feed order");
    }
}

async fn run_for(seq: &Sequencer, d: Duration) -> RunEnd {
    let stop = CancellationToken::new();
    let s2 = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(d).await;
        s2.cancel();
    });
    seq.run(&stop).await.unwrap()
}

#[tokio::test(start_paused = true)]
async fn appends_in_feed_order_and_resumes() {
    let feed = script(40);
    let log = MemLog::new(&start());
    let s = sequencer(&log, &feed);
    assert_eq!(run_for(&s, Duration::from_secs(5)).await, RunEnd::Stopped);
    check(&log, &feed);
    let n = s.stats.appended.load(Ordering::Relaxed);
    assert_eq!(n, feed.0.iter().map(|t| t.len() as u64).sum::<u64>());
    // A restart resumes after the last batch: nothing appended twice.
    let s2 = sequencer(&log, &feed);
    run_for(&s2, Duration::from_secs(5)).await;
    assert_eq!(s2.stats.appended.load(Ordering::Relaxed), 0);
    assert_eq!(s2.stats.duplicates.load(Ordering::Relaxed), 0);
    check(&log, &feed);
    assert_eq!(log.lock().epoch, LeaderEpoch(2));
}

#[tokio::test(start_paused = true)]
async fn lost_answers_are_replayed_without_duplicates() {
    let feed = script(60);
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = Arc::clone(&n);
    let log = MemLog::new(&start()).with_faults(Arc::new(move || {
        match n2.fetch_add(1, Ordering::Relaxed) % 4 {
            1 => Fault::After,
            2 => Fault::Before,
            _ => Fault::None,
        }
    }));
    let mut s = sequencer(&log, &feed);
    s.opts.max_batch = 4;
    run_for(&s, Duration::from_secs(20)).await;
    check(&log, &feed);
    assert!(
        s.stats.duplicates.load(Ordering::Relaxed) > 0,
        "a replay happened"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stale_sequencer_is_fenced() {
    let feed = script(10);
    let log = MemLog::new(&start());
    let old = sequencer(&log, &feed);
    let stop = CancellationToken::new();
    let old_stop = stop.clone();
    // The old sequencer acquires, then a new one takes over before it
    // writes anything.
    let old_run = tokio::spawn(async move { old.run(&old_stop).await });
    tokio::task::yield_now().await;
    let new = sequencer(&log, &feed);
    let new_end = run_for(&new, Duration::from_secs(3)).await;
    assert_eq!(new_end, RunEnd::Stopped);
    let end = tokio::time::timeout(Duration::from_secs(5), old_run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(end, RunEnd::Fenced);
    stop.cancel();
    check(&log, &feed);
}

#[tokio::test(start_paused = true)]
async fn owned_partitions_only() {
    let feed = script(30);
    let log = MemLog::new(&start());
    let mut s = sequencer(&log, &feed);
    s.owned = vec![PartitionNo(1)];
    run_for(&s, Duration::from_secs(5)).await;
    let st = log.lock();
    assert!(st.parts.keys().all(|p| *p == PartitionNo(1)));
}

#[test]
fn starts_round_trip() {
    let a = start();
    assert_eq!(Start::decode(&a.encode()).unwrap(), a);
    let b = Start::After(token(9));
    assert_eq!(Start::decode(&b.encode()).unwrap(), b);
    assert!(Start::decode(b"").is_err());
}

/// A binlog where the script comes first and the log's own writes follow
/// (`head`), with everything up to `purged` purged: a dump from before it
/// is refused, as a node refuses a position its binary logs lack.
#[derive(Clone)]
struct Binlog {
    script: Script,
    head: Arc<AtomicU64>,
    purged: Arc<AtomicU64>,
}

impl Binlog {
    fn new(script: Script) -> Self {
        let n = script.0.len() as u64;
        Self {
            script,
            head: Arc::new(AtomicU64::new(n)),
            purged: Arc::new(AtomicU64::new(0)),
        }
    }

    fn write(&self) -> Gtid {
        Gtid {
            domain: 1,
            server: 1,
            seq: self.head.fetch_add(1, Ordering::SeqCst) + 1,
        }
    }
}

#[async_trait::async_trait]
impl FeedOpener for Binlog {
    async fn open(&self, start: &Start) -> Result<Box<dyn FeedSource>> {
        let from = match start {
            Start::After(t) => t.pos.get(1).map_or(0, |g| g.seq),
            Start::At { .. } => 0,
        };
        let purged = self.purged.load(Ordering::SeqCst);
        if from < purged {
            return Err(SprigError::new(
                SprigCode::FeedHistoryLost,
                format!("binlog events up to {purged} are purged; the feed starts after {from}"),
            ));
        }
        let n = self.script.0.len();
        Ok(Box::new(ScriptFeed {
            txns: Arc::clone(&self.script.0),
            next: usize::try_from(from).unwrap().min(n),
            out: std::collections::VecDeque::new(),
            lookup: None,
            head: Some(Arc::clone(&self.head)),
        }))
    }
}

/// A [`MemLog`] whose every commit (an acquisition, an append, a
/// clean-up) is a write of its own in [`Binlog`], with its GTID.
struct OwnLog {
    log: MemLog,
    binlog: Binlog,
    own: Mutex<Vec<Gtid>>,
}

impl OwnLog {
    fn wrote(&self) {
        let g = self.binlog.write();
        self.own
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(g);
    }
}

#[async_trait::async_trait]
impl LogStore for OwnLog {
    async fn acquire(&self) -> Result<Acquired> {
        let a = self.log.acquire().await?;
        self.wrote();
        Ok(a)
    }

    async fn append(&self, epoch: LeaderEpoch, batch: &Batch) -> Result<AppendOutcome> {
        let o = self.log.append(epoch, batch).await?;
        if matches!(o, AppendOutcome::Appended(_)) {
            self.wrote();
        }
        Ok(o)
    }

    async fn cleanup(&self, outbox_ids: &[[u8; 16]]) -> Result<()> {
        self.log.cleanup(outbox_ids).await?;
        self.wrote();
        Ok(())
    }

    fn take_own(&self) -> Vec<Gtid> {
        std::mem::take(&mut *self.own.lock().unwrap_or_else(PoisonError::into_inner))
    }

    async fn position(&self) -> Result<Option<GtidPos>> {
        Ok(Some(token(self.binlog.head.load(Ordering::SeqCst)).pos))
    }
}

/// Positions saved outside the log, in memory.
#[derive(Default)]
struct SavedPositions {
    saved: Mutex<Option<Vec<u8>>>,
    saves: AtomicUsize,
}

#[async_trait::async_trait]
impl Watermarks for SavedPositions {
    async fn publish(&self, _epoch: LeaderEpoch, _marks: &BTreeMap<PartitionNo, Offset>) {}

    async fn save_position(&self, _epoch: LeaderEpoch, start: &[u8]) -> bool {
        *self.saved.lock().unwrap_or_else(PoisonError::into_inner) = Some(start.to_vec());
        self.saves.fetch_add(1, Ordering::SeqCst);
        true
    }

    async fn saved_position(&self) -> Option<Vec<u8>> {
        self.saved
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn own_sequencer(log: &MemLog, binlog: &Binlog, marks: &Arc<SavedPositions>) -> Sequencer {
    let mut s = sequencer(log, &binlog.script);
    s.store = Arc::new(OwnLog {
        log: log.clone(),
        binlog: binlog.clone(),
        own: Mutex::new(Vec::new()),
    });
    s.feed = Arc::new(binlog.clone());
    s.marks = Arc::clone(marks) as Arc<dyn Watermarks>;
    s
}

fn saved_seq(marks: &SavedPositions) -> Option<u64> {
    let b = marks.saved.lock().unwrap().clone()?;
    match Start::decode(&b).unwrap() {
        Start::After(t) => t.pos.get(1).map(|g| g.seq),
        Start::At { .. } => None,
    }
}

/// The shard row's position lies one of the sequencer's own writes behind
/// (its last append, and the outbox clean-up after it). A stop saves the
/// position past them outside the log; a purge of every binlog right after
/// the stop then loses nothing on the restart, which starts there and
/// handles nothing twice.
#[tokio::test(start_paused = true)]
async fn a_purge_right_after_a_stop_loses_nothing() {
    let binlog = Binlog::new(script(30));
    let log = MemLog::new(&start());
    let marks = Arc::new(SavedPositions::default());
    let s = own_sequencer(&log, &binlog, &marks);
    // Stopped soon after its last append.
    assert_eq!(
        run_for(&s, Duration::from_millis(400)).await,
        RunEnd::Stopped
    );
    check(&log, &binlog.script);
    let head = binlog.head.load(Ordering::SeqCst);
    let row = match Start::decode(&log.lock().resume).unwrap() {
        Start::After(t) => t.pos.get(1).unwrap().seq,
        Start::At { .. } => 0,
    };
    assert!(
        row < head,
        "the row's position {row} lies behind the sequencer's own writes up to {head}"
    );
    assert_eq!(
        saved_seq(&marks),
        Some(head),
        "the saved position covers its own last writes"
    );
    // Every binlog purged; the restart starts past them.
    binlog.purged.store(head, Ordering::SeqCst);
    let s2 = own_sequencer(&log, &binlog, &marks);
    assert_eq!(run_for(&s2, Duration::from_secs(3)).await, RunEnd::Stopped);
    assert_eq!(s2.stats.appended.load(Ordering::Relaxed), 0);
    assert_eq!(s2.stats.duplicates.load(Ordering::Relaxed), 0);
    check(&log, &binlog.script);
    // Without the saved position, the row's alone is refused.
    let bare = MemLog::new(&Start::decode(&log.lock().resume).unwrap());
    let s3 = own_sequencer(&bare, &binlog, &Arc::new(SavedPositions::default()));
    let e = run_for_err(&s3, Duration::from_secs(1)).await;
    assert_eq!(e.code(), Some(SprigCode::FeedHistoryLost), "{e}");
}

/// Once the feed has moved over nothing but its own writes, an idle
/// sequencer saves its position outside the log and writes nothing more:
/// a checkpoint in the log would itself be a new write to move over.
#[tokio::test(start_paused = true)]
async fn an_idle_sequencer_stops_writing_its_own_checkpoints() {
    let binlog = Binlog::new(script(20));
    let log = MemLog::new(&start());
    let marks = Arc::new(SavedPositions::default());
    let mut s = own_sequencer(&log, &binlog, &marks);
    s.opts.idle_checkpoint = Duration::from_millis(500);
    let stop = CancellationToken::new();
    let s = Arc::new(s);
    let (s2, stop2) = (Arc::clone(&s), stop.clone());
    let run = tokio::spawn(async move { s2.run(&stop2).await });
    tokio::time::sleep(Duration::from_secs(10)).await;
    let (commits, head, saves) = (
        log.lock().commits,
        binlog.head.load(Ordering::SeqCst),
        marks.saves.load(Ordering::SeqCst),
    );
    assert!(saves >= 1);
    assert_eq!(saved_seq(&marks), Some(head));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        log.lock().commits,
        commits,
        "no checkpoint written while idle"
    );
    assert_eq!(
        binlog.head.load(Ordering::SeqCst),
        head,
        "no write of its own while idle"
    );
    assert_eq!(
        marks.saves.load(Ordering::SeqCst),
        saves,
        "nothing new to save"
    );
    stop.cancel();
    assert_eq!(run.await.unwrap().unwrap(), RunEnd::Stopped);
    check(&log, &binlog.script);
}

#[test]
fn the_saved_position_is_used_only_when_it_is_later_on_the_same_feed() {
    let row = Start::After(token(10));
    let later = Start::After(token(12)).encode();
    let earlier = Start::After(token(8)).encode();
    assert_eq!(
        later_start(row.clone(), Some(&later)),
        Start::After(token(12))
    );
    assert_eq!(later_start(row.clone(), Some(&earlier)), row);
    assert_eq!(later_start(row.clone(), None), row);
    let mut other = token(12);
    other.segment = 3;
    assert_eq!(
        later_start(row.clone(), Some(&Start::After(other).encode())),
        row
    );
    let mut moved = token(12);
    moved.cluster = ClusterId::new("dc-a-c2").unwrap();
    assert_eq!(
        later_start(row.clone(), Some(&Start::After(moved).encode())),
        row
    );
    assert_eq!(later_start(start(), Some(&later)), Start::After(token(12)));
    let base = token(10).pos;
    let own = [
        Gtid {
            domain: 1,
            server: 1,
            seq: 12,
        },
        Gtid {
            domain: 1,
            server: 1,
            seq: 11,
        },
    ];
    assert_eq!(past_own(&base, &own), token(12).pos);
    // A gap (someone else's write at 11) stops it there.
    assert_eq!(past_own(&base, &own[..1]), base);
}

async fn run_for_err(seq: &Sequencer, d: Duration) -> SprigError {
    let stop = CancellationToken::new();
    let s2 = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(d).await;
        s2.cancel();
    });
    seq.run(&stop).await.unwrap_err()
}
