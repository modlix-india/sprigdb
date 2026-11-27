//! Model tests of the commands, each on three units: the in-memory unit,
//! the engine's transactions on the transaction layer's in-memory commit
//! path (a group-commit log with a simulated sync), and the engine itself
//! on RocksDB with real syncs: replies, inline and split collections,
//! expiry, the sweeper, and concurrent batches on one key. Then the
//! engine's recovery of acknowledged writes after a restart.

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::cast_precision_loss
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{CommitPath, UnitConfig};
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_proto_redis::Frame;
use sprig_redis_engine::layout::{KIND_ELEM, KIND_EXPIRY, Space};
use sprig_redis_engine::mem::MemUnit;
use sprig_redis_engine::store::Ticket;
use sprig_redis_engine::{Backend, Batch, EngineDb, EngineUnit, Tenants, Unit};

const T: u64 = 42;

/// A unit under test, with a clock tests can fix.
trait Bed: Unit {
    fn fix_clock(&self, ms: Option<i64>);
}

impl Bed for MemUnit {
    fn fix_clock(&self, ms: Option<i64>) {
        self.set_clock(ms);
    }
}

impl Bed for EngineUnit {
    fn fix_clock(&self, ms: Option<i64>) {
        self.set_clock(ms);
    }
}

fn mem() -> Arc<MemUnit> {
    Arc::new(MemUnit::new())
}

/// The transaction layer on its in-memory path, 300 µs per log sync.
fn path() -> Arc<EngineUnit> {
    let p = MemPath::start(UnitConfig::default(), Duration::from_micros(300));
    Arc::new(EngineUnit::new(p as Arc<dyn CommitPath>))
}

/// A fresh directory for an engine, under the build's temporary folder.
fn dir(tag: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("e0-redis-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(d: &std::path::Path) -> EngineConfig {
    let mut c = EngineConfig::new(d);
    c.store = StoreKind::Rocks;
    c.budget.engine_mb = 64;
    c
}

/// The engine on RocksDB, with real syncs.
fn rocks(d: &std::path::Path) -> Arc<EngineUnit> {
    Arc::new(EngineUnit::on_engine(Engine::open(cfg(d)).unwrap()))
}

/// Runs `$f` on the three units.
macro_rules! three {
    ($mem:ident, $path:ident, $rocks:ident, $f:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn $mem() {
            $f(mem()).await;
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn $path() {
            $f(path()).await;
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
        async fn $rocks() {
            let d = dir(stringify!($rocks));
            $f(rocks(&d)).await;
            let _ = std::fs::remove_dir_all(&d);
        }
    };
}

three!(
    strings_mem,
    strings_path,
    strings_rocks,
    strings_and_options
);
three!(
    collections_mem,
    collections_path,
    collections_rocks,
    hashes_and_lists_inline_then_split
);
three!(expiry_mem, expiry_path, expiry_rocks, expiry_lazy_and_swept);
three!(
    pipeline_mem,
    pipeline_path,
    pipeline_rocks,
    pipeline_is_one_batch_with_in_order_replies
);
three!(
    concurrent_mem,
    concurrent_path,
    concurrent_rocks,
    concurrent_batches_on_one_key_lose_nothing
);
three!(flush_mem, flush_path, flush_rocks, flush_and_databases);
three!(
    hot_mem,
    hot_path,
    hot_rocks,
    hot_key_batches_are_combined_in_order
);
three!(index_mem, index_path, index_rocks, scan_index_and_uids);

fn db<U: Unit>(unit: &Arc<U>) -> Arc<EngineDb<U>> {
    Backend::new(Arc::clone(unit), Tenants::All, 16)
        .db(T, 0)
        .unwrap()
}

fn args(line: &str) -> Vec<Bytes> {
    line.split(' ')
        .map(|s| Bytes::copy_from_slice(s.as_bytes()))
        .collect()
}

async fn one<U: Unit>(d: &EngineDb<U>, line: &str) -> Frame {
    let a = args(line);
    d.run_batch(&[&a[..]]).await.pop().unwrap()
}

fn bulk(s: &str) -> Frame {
    Frame::Bulk(Bytes::copy_from_slice(s.as_bytes()))
}

async fn count<U: Unit>(unit: &U, kind: u8) -> usize {
    let s = Space::redis(T, 0);
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let n = b
        .scan(&s.prefix(kind), &s.prefix_end(kind), usize::MAX / 2)
        .unwrap()
        .len();
    b.commit().unwrap().durable().await.unwrap();
    n
}

async fn scan_entries<U: Unit>(unit: &U) -> usize {
    let (lo, hi) = Space::redis(T, 0).scan_index_range();
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let n = b.scan(&lo, &hi, usize::MAX / 2).unwrap().len();
    b.commit().unwrap().durable().await.unwrap();
    n
}

/// The uids of every split collection, from their elements.
async fn uids<U: Unit>(unit: &U) -> Vec<u64> {
    let s = Space::redis(T, 0);
    let p = s.prefix(KIND_ELEM).len();
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let mut u: Vec<u64> = b
        .scan(
            &s.prefix(KIND_ELEM),
            &s.prefix_end(KIND_ELEM),
            usize::MAX / 2,
        )
        .unwrap()
        .iter()
        .map(|(k, _)| u64::from_be_bytes(k[p..p + 8].try_into().unwrap()))
        .collect();
    b.commit().unwrap().durable().await.unwrap();
    u.dedup();
    u
}

/// One scan-index entry per key, through creates, overwrites, expiry and
/// deletes; every split collection gets a fresh uid, never reused.
async fn scan_index_and_uids<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    unit.fix_clock(Some(2_000_000));
    one(&d, "SET a 1").await;
    one(&d, "SET a 2").await;
    one(&d, "INCR n").await;
    one(&d, "SET e v PX 100").await;
    assert_eq!(scan_entries(&*unit).await, 3);
    let big: Vec<String> = (0..130).map(|i| format!("f{i} v")).collect();
    one(&d, &format!("HSET h {}", big.join(" "))).await;
    let items: Vec<String> = (0..130).map(|i| i.to_string()).collect();
    one(&d, &format!("LPUSH l {}", items.join(" "))).await;
    assert_eq!(scan_entries(&*unit).await, 5);
    let first = uids(&*unit).await;
    assert_eq!(first.len(), 2);
    // A replaced collection's elements go, and its successor's uid is new.
    one(&d, "DEL h").await;
    one(&d, &format!("HSET h {}", big.join(" "))).await;
    let second = uids(&*unit).await;
    assert_eq!(second.len(), 2);
    assert!(second.iter().all(|u| *u > 0));
    assert!(second.iter().any(|u| !first.contains(u)));
    assert_eq!(one(&d, "HGET h f77").await, bulk("v"));
    // An expired key written again keeps its one entry; DEL and the
    // sweeper remove entries.
    unit.fix_clock(Some(2_001_000));
    one(&d, "SET e w").await;
    assert_eq!(scan_entries(&*unit).await, 5);
    one(&d, "SET x v PX 10").await;
    unit.fix_clock(Some(2_002_000));
    assert_eq!(scan_entries(&*unit).await, 6);
    sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 100)
        .await
        .unwrap();
    assert_eq!(scan_entries(&*unit).await, 5);
    assert_eq!(one(&d, "DEL a n e h l").await, Frame::Integer(5));
    assert_eq!(scan_entries(&*unit).await, 0);
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
}

async fn strings_and_options<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    assert_eq!(one(&d, "SET a 1").await, Frame::ok());
    assert_eq!(one(&d, "GET a").await, bulk("1"));
    assert_eq!(one(&d, "SET a 2 NX").await, Frame::null());
    assert_eq!(one(&d, "SET b 2 XX").await, Frame::null());
    assert_eq!(one(&d, "SET a 3 XX GET").await, bulk("1"));
    assert_eq!(one(&d, "INCR a").await, Frame::Integer(4));
    assert_eq!(one(&d, "INCR new").await, Frame::Integer(1));
    assert_eq!(one(&d, "SET a x").await, Frame::ok());
    assert!(one(&d, "INCR a").await.is_error());
    assert!(one(&d, "SET a v NX XX").await.is_error());
    assert!(one(&d, "SET a v EX 0").await.is_error());
    assert_eq!(one(&d, "MSET a 1 b 2 a 3").await, Frame::ok());
    assert_eq!(
        one(&d, "MGET a b c").await,
        Frame::Array(vec![bulk("3"), bulk("2"), Frame::null()])
    );
    assert_eq!(one(&d, "DEL a a b c").await, Frame::Integer(2));
    assert_eq!(one(&d, "GET a").await, Frame::null());
}

async fn hashes_and_lists_inline_then_split<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    assert_eq!(one(&d, "HSET h f1 v1 f2 v2").await, Frame::Integer(2));
    assert_eq!(one(&d, "HSET h f1 x f3 v3").await, Frame::Integer(1));
    assert_eq!(one(&d, "HGET h f1").await, bulk("x"));
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    // Past 128 fields the hash moves to element entries.
    for i in 0..200 {
        one(&d, &format!("HSET h k{i} v{i}")).await;
    }
    assert_eq!(count(&*unit, KIND_ELEM).await, 203);
    assert_eq!(one(&d, "HGET h k150").await, bulk("v150"));
    assert_eq!(one(&d, "HGET h f1").await, bulk("x"));
    assert_eq!(one(&d, "HSET h k150 w f9 z").await, Frame::Integer(1));
    assert_eq!(one(&d, "HGET h k150").await, bulk("w"));
    assert_eq!(one(&d, "GET h").await, sprig_redis::reply::wrongtype());

    assert_eq!(one(&d, "LPUSH l a b c").await, Frame::Integer(3));
    let long = "x".repeat(100);
    assert_eq!(one(&d, &format!("LPUSH l {long}")).await, Frame::Integer(4));
    assert_eq!(one(&d, "LPUSH l d").await, Frame::Integer(5));
    assert_eq!(one(&d, "HGET l f").await, sprig_redis::reply::wrongtype());
    // DEL takes the elements with it.
    assert_eq!(one(&d, "DEL h l").await, Frame::Integer(2));
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    // SET over a collection replaces it.
    for i in 0..130 {
        one(&d, &format!("LPUSH l2 {i}")).await;
    }
    assert!(count(&*unit, KIND_ELEM).await > 0);
    assert_eq!(one(&d, "SET l2 s").await, Frame::ok());
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
}

async fn expiry_lazy_and_swept<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    unit.fix_clock(Some(1_000_000));
    assert_eq!(one(&d, "SET a v EX 10").await, Frame::ok());
    assert_eq!(one(&d, "TTL a").await, Frame::Integer(10));
    assert_eq!(one(&d, "TTL nokey").await, Frame::Integer(-2));
    assert_eq!(one(&d, "SET b v PX 2500").await, Frame::ok());
    assert_eq!(one(&d, "TTL b").await, Frame::Integer(3));
    assert_eq!(one(&d, "SET c v").await, Frame::ok());
    assert_eq!(one(&d, "TTL c").await, Frame::Integer(-1));
    assert_eq!(one(&d, "EXPIRE c 100 XX").await, Frame::Integer(0));
    assert_eq!(one(&d, "EXPIRE c 100").await, Frame::Integer(1));
    assert_eq!(one(&d, "EXPIRE c 50 GT").await, Frame::Integer(0));
    assert_eq!(one(&d, "EXPIRE c 50 LT").await, Frame::Integer(1));
    assert_eq!(count(&*unit, KIND_EXPIRY).await, 3);
    // INCR and KEEPTTL keep the expiry; plain SET drops it.
    assert_eq!(one(&d, "SET n 1 EX 100").await, Frame::ok());
    assert_eq!(one(&d, "INCR n").await, Frame::Integer(2));
    assert_eq!(one(&d, "TTL n").await, Frame::Integer(100));
    assert_eq!(one(&d, "SET n 5").await, Frame::ok());
    assert_eq!(one(&d, "TTL n").await, Frame::Integer(-1));
    assert_eq!(count(&*unit, KIND_EXPIRY).await, 3);
    // Time passes: a and b read as gone at once, the sweeper deletes them.
    unit.fix_clock(Some(1_011_000));
    assert_eq!(one(&d, "GET a").await, Frame::null());
    assert_eq!(one(&d, "EXPIRE a 10").await, Frame::Integer(0));
    let n = sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 100)
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(count(&*unit, KIND_EXPIRY).await, 1);
    assert_eq!(one(&d, "TTL c").await, Frame::Integer(39));
    // A negative expiry deletes.
    assert_eq!(one(&d, "EXPIRE c -1").await, Frame::Integer(1));
    assert_eq!(one(&d, "GET c").await, Frame::null());
    assert_eq!(count(&*unit, KIND_EXPIRY).await, 0);
}

async fn pipeline_is_one_batch_with_in_order_replies<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    let lines = [
        "SET k 1",
        "INCR k",
        "HSET k f v",
        "INCR k",
        "LPUSH q a",
        "GET k",
        "MSET k 10 q2 x",
        "INCR k",
    ];
    let a: Vec<Vec<Bytes>> = lines.iter().map(|l| args(l)).collect();
    let refs: Vec<&[Bytes]> = a.iter().map(|v| &v[..]).collect();
    let r = d.run_batch(&refs).await;
    assert_eq!(r[0], Frame::ok());
    assert_eq!(r[1], Frame::Integer(2));
    assert_eq!(r[2], sprig_redis::reply::wrongtype());
    assert_eq!(r[3], Frame::Integer(3));
    assert_eq!(r[4], Frame::Integer(1));
    assert_eq!(r[5], bulk("3"));
    assert_eq!(r[6], Frame::ok());
    assert_eq!(r[7], Frame::Integer(11));
}

async fn concurrent_batches_on_one_key_lose_nothing<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    let mut tasks = Vec::new();
    for c in 0..32 {
        let d = Arc::clone(&d);
        tasks.push(tokio::spawn(async move {
            for _ in 0..50 {
                // Pipelines touching the hot key and the client's own,
                // in different orders.
                let x = args("INCR hot");
                let own = args(&format!("INCR own{c}"));
                let push = args("LPUSH list e");
                let r = d.run_batch(&[&own[..], &x[..], &push[..], &x[..]]).await;
                assert!(r.iter().all(|f| !f.is_error()), "{r:?}");
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(one(&d, "GET hot").await, bulk("3200"));
    assert_eq!(one(&d, "GET own7").await, bulk("50"));
    assert_eq!(one(&d, "LPUSH list e").await, Frame::Integer(1601));
}

async fn flush_and_databases<U: Bed>(unit: Arc<U>) {
    let b = Backend::new(Arc::clone(&unit), Tenants::parse("42, 43").unwrap(), 16);
    let d0 = b.db(T, 0).unwrap();
    let d1 = b.db(T, 1).unwrap();
    let other = b.db(43, 0).unwrap();
    assert!(b.db(44, 0).is_none());
    assert!(b.db(T, 16).is_none());
    one(&d0, "SET a 1").await;
    one(&d1, "SET a 2").await;
    one(&other, "SET a 3").await;
    assert_eq!(one(&d1, "GET a").await, bulk("2"));
    assert_eq!(one(&d1, "FLUSHDB").await, Frame::ok());
    assert_eq!(one(&d0, "GET a").await, bulk("1"));
    assert_eq!(one(&d1, "GET a").await, Frame::null());
    one(&d1, "SET a 2 EX 100").await;
    assert_eq!(one(&d0, "FLUSHALL").await, Frame::ok());
    assert_eq!(one(&d0, "GET a").await, Frame::null());
    assert_eq!(one(&d1, "GET a").await, Frame::null());
    assert_eq!(one(&other, "GET a").await, bulk("3"));
    assert!(one(&d0, "FLUSHDB now").await.is_error());
}

/// Acknowledged writes survive a restart in which RocksDB's memtables are
/// lost (no flush on close, as in a crash): the engine replays its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_recovers_acknowledged_writes() {
    let d = dir("recover");
    let crashy = |d: &std::path::Path| {
        let mut c = cfg(d);
        c.rocks.avoid_flush_on_close = true;
        c.checkpoint_interval = Duration::ZERO;
        Arc::new(EngineUnit::on_engine(Engine::open(c).unwrap()))
    };
    {
        let unit = crashy(&d);
        let db0 = db(&unit);
        one(&db0, "SET a 1").await;
        one(&db0, "INCR a").await;
        one(&db0, "HSET h f v").await;
        for i in 0..200 {
            one(&db0, &format!("LPUSH l {i}")).await;
        }
        one(&db0, "SET gone x").await;
        one(&db0, "DEL gone").await;
        one(&db0, "SET t v EX 1000").await;
        let big: Vec<String> = (0..130).map(|i| format!("f{i} v")).collect();
        one(&db0, &format!("HSET h2 {}", big.join(" "))).await;
    }
    let unit = crashy(&d);
    let db0 = db(&unit);
    assert_eq!(one(&db0, "GET a").await, bulk("2"));
    assert_eq!(one(&db0, "HGET h f").await, bulk("v"));
    assert_eq!(one(&db0, "GET gone").await, Frame::null());
    assert_eq!(one(&db0, "LPUSH l x").await, Frame::Integer(201));
    assert!(matches!(one(&db0, "TTL t").await, Frame::Integer(n) if n > 990));
    // A collection made after the restart never takes a uid used before.
    let big: Vec<String> = (0..130).map(|i| format!("f{i} w")).collect();
    one(&db0, &format!("HSET h3 {}", big.join(" "))).await;
    assert_eq!(one(&db0, "HGET h2 f5").await, bulk("v"));
    assert_eq!(one(&db0, "HGET h3 f5").await, bulk("w"));
    // Writes after recovery recover again, after a checkpoint too.
    one(&db0, "SET b 7").await;
    unit.engine().unwrap().checkpoint().unwrap();
    one(&db0, "SET c 8").await;
    drop(db0);
    drop(unit);
    let unit = crashy(&d);
    let db0 = db(&unit);
    assert_eq!(one(&db0, "GET b").await, bulk("7"));
    assert_eq!(one(&db0, "GET c").await, bulk("8"));
    assert_eq!(one(&db0, "LPUSH l y").await, Frame::Integer(202));
    drop(db0);
    drop(unit);
    let _ = std::fs::remove_dir_all(&d);
}

/// Single-key batches on one hot key (the combined path): every INCR reply
/// is unique, and the last equals the count.
async fn hot_key_batches_are_combined_in_order<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    let mut tasks = Vec::new();
    for _ in 0..40 {
        let d = Arc::clone(&d);
        tasks.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..100 {
                let incr = args("INCR hot");
                let get = args("GET hot");
                let r = d.run_batch(&[&incr[..], &get[..]]).await;
                let Frame::Integer(n) = r[0] else {
                    panic!("{r:?}");
                };
                // The GET in the same batch sees at least its own INCR.
                let Frame::Bulk(g) = &r[1] else {
                    panic!("{r:?}");
                };
                assert!(std::str::from_utf8(g).unwrap().parse::<i64>().unwrap() >= n);
                seen.push(n);
            }
            seen
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort_unstable();
    assert_eq!(all, (1..=4000).collect::<Vec<i64>>());
    assert_eq!(one(&d, "GET hot").await, bulk("4000"));
}

three!(paging_mem, paging_path, paging_rocks, paged_scans_end);

/// Paged scans of the scan index and the metadata end (DBSIZE, KEYS),
/// with expiring keys among them.
async fn paged_scans_end<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    for i in 0..10 {
        one(&d, &format!("SET k{i} v")).await;
    }
    one(&d, "SET e v EX 1000").await;
    assert_eq!(one(&d, "DBSIZE").await, Frame::Integer(11));
    let s = Space::redis(T, 0);
    let (lo, hi) = s.scan_index_range();
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let mut start = lo.clone();
    let mut n = 0;
    for _ in 0..100 {
        let got = b.scan(&start, &hi, 3).unwrap();
        let Some((last, _)) = got.last() else { break };
        assert!(got.iter().all(|(k, _)| k.as_ref() >= start.as_slice()));
        start = sprig_redis_engine::layout::after(last);
        n += got.len();
    }
    b.commit().unwrap().durable().await.unwrap();
    assert_eq!(n, 11);
    match one(&d, "KEYS *").await {
        Frame::Array(a) => assert_eq!(a.len(), 11),
        f => panic!("{f:?}"),
    }
}

three!(
    types_mem,
    types_path,
    types_rocks,
    every_type_split_and_emptied
);
three!(moves_mem, moves_path, moves_rocks, copies_renames_and_moves);

fn int(f: &Frame) -> i64 {
    match f {
        Frame::Integer(i) => *i,
        f => panic!("not an integer: {f:?}"),
    }
}

fn len(f: &Frame) -> usize {
    match f {
        Frame::Array(a) => a.len(),
        f => panic!("not an array: {f:?}"),
    }
}

/// Each collection type: small ones inline, big ones split into elements
/// (sorted sets also into score-index entries), and emptied ones gone with
/// every element.
async fn every_type_split_and_emptied<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    let n = 200;
    let words = |p: &str| {
        (0..n)
            .map(|i| format!("{p}{i}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let pairs = (0..n)
        .map(|i| format!("{} m{i}", i % 13))
        .collect::<Vec<_>>()
        .join(" ");
    let fields = (0..n)
        .map(|i| format!("f{i} v{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    one(&d, "SADD small a b").await;
    one(&d, "ZADD zsmall 1 a").await;
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(int(&one(&d, &format!("SADD s {}", words("m"))).await), n);
    assert_eq!(int(&one(&d, &format!("ZADD z {pairs}")).await), n);
    assert_eq!(int(&one(&d, &format!("RPUSH l {}", words("i"))).await), n);
    assert_eq!(int(&one(&d, &format!("HSET h {fields}")).await), n);
    assert_eq!(count(&*unit, KIND_ELEM).await, 4 * n as usize);
    assert_eq!(
        count(&*unit, sprig_redis_engine::layout::KIND_ZSCORE).await,
        n as usize
    );
    assert_eq!(one(&d, "OBJECT ENCODING z").await, bulk("skiplist"));
    assert_eq!(one(&d, "OBJECT ENCODING s").await, bulk("hashtable"));
    assert_eq!(one(&d, "ZRANK z m13").await, Frame::Integer(3));
    assert_eq!(len(&one(&d, "ZRANGE z 0 -1").await), n as usize);
    assert_eq!(len(&one(&d, "ZRANGE z (3 5 BYSCORE").await), 31);
    assert_eq!(one(&d, "LINDEX l 150").await, bulk("i150"));
    // Scans page through split collections and end.
    for cmd in ["SSCAN s", "HSCAN h", "ZSCAN z"] {
        let mut cursor = "0".to_owned();
        let mut seen = 0;
        loop {
            let Frame::Array(r) = one(&d, &format!("{cmd} {cursor} COUNT 17")).await else {
                panic!()
            };
            let Frame::Bulk(c) = &r[0] else { panic!() };
            seen += len(&r[1]);
            cursor = String::from_utf8_lossy(c).into_owned();
            if cursor == "0" {
                break;
            }
        }
        let per = if cmd == "SSCAN s" { 1 } else { 2 };
        assert_eq!(seen, per * n as usize, "{cmd}");
    }
    // Emptied by removal: the key and every element go.
    one(&d, &format!("SREM s {}", words("m"))).await;
    one(&d, &format!("ZREM z {}", words("m"))).await;
    assert_eq!(len(&one(&d, &format!("LPOP l {n}")).await), n as usize);
    one(&d, &format!("HDEL h {}", words("f"))).await;
    for k in ["s", "z", "l", "h"] {
        assert_eq!(
            one(&d, &format!("EXISTS {k}")).await,
            Frame::Integer(0),
            "{k}"
        );
    }
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(
        count(&*unit, sprig_redis_engine::layout::KIND_ZSCORE).await,
        0
    );
    assert_eq!(scan_entries(&*unit).await, 2);
    // Stores choose their encoding again; an empty result deletes.
    one(&d, &format!("SADD s {}", words("m"))).await;
    assert_eq!(one(&d, "SINTERSTORE t s small").await, Frame::Integer(0));
    assert_eq!(int(&one(&d, "SUNIONSTORE t s small").await), n + 2);
    assert_eq!(one(&d, "SDIFFSTORE t small s").await, Frame::Integer(2));
    assert_eq!(one(&d, "OBJECT ENCODING t").await, bulk("listpack"));
    one(&d, "DEL s t small zsmall").await;
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(scan_entries(&*unit).await, 0);
}

/// Copies get their own elements; renames keep them; moves take them to
/// the other database.
async fn copies_renames_and_moves<U: Bed>(unit: Arc<U>) {
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    let d1 = b.db(T, 1).unwrap();
    let pairs = (0..150)
        .map(|i| format!("{i} m{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    one(&d, &format!("ZADD z {pairs}")).await;
    assert_eq!(one(&d, "COPY z z2").await, Frame::Integer(1));
    assert_eq!(count(&*unit, KIND_ELEM).await, 300);
    assert_eq!(one(&d, "RENAME z2 z3").await, Frame::ok());
    assert_eq!(count(&*unit, KIND_ELEM).await, 300);
    assert_eq!(one(&d, "ZSCORE z3 m99").await, Frame::Double(99.0));
    assert_eq!(one(&d, "MOVE z3 1").await, Frame::Integer(1));
    assert_eq!(count(&*unit, KIND_ELEM).await, 150);
    assert_eq!(one(&d1, "ZSCORE z3 m98").await, Frame::Double(98.0));
    assert_eq!(one(&d1, "ZCARD z3").await, Frame::Integer(150));
    assert_eq!(one(&d, "COPY z z3 DB 1").await, Frame::Integer(0));
    assert_eq!(one(&d, "COPY z z3 DB 1 REPLACE").await, Frame::Integer(1));
    assert_eq!(
        one(&d1, "ZRANGE z3 0 0").await,
        Frame::Array(vec![bulk("m0")])
    );
    assert_eq!(one(&d, "RENAME z z").await, Frame::ok());
    one(&d, "SET plain v").await;
    assert_eq!(one(&d, "RENAME z plain").await, Frame::ok());
    assert_eq!(one(&d, "TYPE plain").await, Frame::simple("zset"));
    assert_eq!(one(&d, "DEL plain").await, Frame::Integer(1));
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(one(&d1, "FLUSHDB").await, Frame::ok());
}

three!(block_mem, block_path, block_rocks, blocked_clients_wake);

/// A blocked command wakes when another batch pushes to its key.
async fn blocked_clients_wake<U: Bed>(unit: Arc<U>) {
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    for (blocking, push, want) in [
        ("BLMOVE b1 b3 RIGHT LEFT 5", "RPUSH b1 p q", bulk("q")),
        (
            "BLPOP b1 b2 5",
            "RPUSH b2 x",
            Frame::Array(vec![bulk("b2"), bulk("x")]),
        ),
        (
            "BZPOPMIN z1 z2 5",
            "ZADD z2 1 a",
            Frame::Array(vec![bulk("z2"), bulk("a"), Frame::Double(1.0)]),
        ),
    ] {
        one(&d, "FLUSHALL").await;
        let d2 = Arc::clone(&d);
        let a = args(blocking);
        let t0 = std::time::Instant::now();
        let w = tokio::spawn(async move { d2.run_batch(&[&a[..]]).await.pop().unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        one(&d, push).await;
        let got = tokio::time::timeout(Duration::from_secs(3), w)
            .await
            .expect("still blocked")
            .unwrap();
        assert_eq!(got, want, "{blocking}");
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "{blocking} took {:?}",
            t0.elapsed()
        );
    }
}

three!(
    fexp_mem,
    fexp_path,
    fexp_rocks,
    hash_fields_expire_and_are_swept
);

/// Every entry of `kind` with index `index` in database 0.
async fn count_at<U: Unit>(unit: &U, kind: u8, index: u16) -> usize {
    let s = Space::redis(T, 0);
    let mut lo = s.prefix(kind);
    let n = lo.len();
    lo[n - 2..].copy_from_slice(&index.to_be_bytes());
    let mut hi = lo.clone();
    hi[n - 2..].copy_from_slice(&(index + 1).to_be_bytes());
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let got = b.scan(&lo, &hi, usize::MAX / 2).unwrap().len();
    b.commit().unwrap().durable().await.unwrap();
    got
}

/// Field lifetimes: due fields read as absent at once, the sweeper removes
/// them with their index entries, and a hash whose last field goes is
/// deleted.
async fn hash_fields_expire_and_are_swept<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    unit.fix_clock(Some(5_000_000));
    one(&d, "HSET h a 1 b 2 c 3").await;
    one(&d, "HSET g x 1").await;
    let set = one(&d, "HPEXPIRE h 100 FIELDS 2 a b").await;
    assert_eq!(
        set,
        Frame::Array(vec![Frame::Integer(1), Frame::Integer(1)])
    );
    one(&d, "HPEXPIRE g 200 FIELDS 1 x").await;
    assert_eq!(count_at(&*unit, KIND_ELEM, 1).await, 3);
    assert_eq!(count_at(&*unit, KIND_EXPIRY, 1).await, 2);
    unit.fix_clock(Some(5_000_150));
    assert_eq!(one(&d, "HLEN h").await, Frame::Integer(1));
    assert_eq!(one(&d, "HGET h a").await, Frame::null());
    assert_eq!(one(&d, "EXISTS g").await, Frame::Integer(1));
    sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 100)
        .await
        .unwrap();
    assert_eq!(count_at(&*unit, KIND_ELEM, 1).await, 1);
    assert_eq!(count_at(&*unit, KIND_EXPIRY, 1).await, 1);
    unit.fix_clock(Some(5_000_300));
    assert_eq!(one(&d, "EXISTS g").await, Frame::Integer(0));
    sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 100)
        .await
        .unwrap();
    assert_eq!(count_at(&*unit, KIND_ELEM, 1).await, 0);
    assert_eq!(count_at(&*unit, KIND_EXPIRY, 1).await, 0);
    assert_eq!(scan_entries(&*unit).await, 1);
    assert_eq!(
        one(&d, "HGETALL h").await,
        Frame::Map(vec![(bulk("c"), bulk("3"))])
    );
    // A field written again loses its lifetime; HINCRBY keeps it.
    one(&d, "HPEXPIRE h 1000 FIELDS 1 c").await;
    one(&d, "HINCRBY h c 1").await;
    assert_eq!(
        one(&d, "HPTTL h FIELDS 1 c").await,
        Frame::Array(vec![Frame::Integer(1000)])
    );
    one(&d, "HSET h c 9").await;
    assert_eq!(
        one(&d, "HPTTL h FIELDS 1 c").await,
        Frame::Array(vec![Frame::Integer(-1)])
    );
    assert_eq!(count_at(&*unit, KIND_ELEM, 1).await, 0);
    assert_eq!(count_at(&*unit, KIND_EXPIRY, 1).await, 0);
    one(&d, "DEL h").await;
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
}

/// Every type, its indexes and a committed EXEC come back after a restart
/// that loses RocksDB's memtables; the restored data is consistent (counts,
/// ranks, score order, field lifetimes, a second database).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_recovers_every_type() {
    let d = dir("recover-types");
    let crashy = |d: &std::path::Path| {
        let mut c = cfg(d);
        c.rocks.avoid_flush_on_close = true;
        c.checkpoint_interval = Duration::ZERO;
        Arc::new(EngineUnit::on_engine(Engine::open(c).unwrap()))
    };
    let members = (0..300)
        .map(|i| format!("m{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    let pairs = (0..300)
        .map(|i| format!("{} z{i}", i % 17))
        .collect::<Vec<_>>()
        .join(" ");
    {
        let unit = crashy(&d);
        let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
        let d0 = b.db(T, 0).unwrap();
        let d1 = b.db(T, 1).unwrap();
        one(&d0, &format!("SADD s {members}")).await;
        one(&d0, "SADD small 1 2 3").await;
        one(&d0, &format!("ZADD z {pairs}")).await;
        one(&d0, "ZINCRBY z 100 z5").await;
        one(&d0, "ZREM z z6").await;
        one(&d0, &format!("RPUSH l {members}")).await;
        one(&d0, "LTRIM l 10 -11").await;
        one(&d0, "HSET h a 1 b 2 c 3").await;
        one(&d0, "HEXPIRE h 100000 FIELDS 1 a").await;
        one(&d0, "PFADD hll a b c d e").await;
        one(&d0, "SETBIT bits 1000 1").await;
        let tx = [args("SET tx 1"), args("INCR tx"), args("LPUSH txl q")];
        let refs: Vec<&[Bytes]> = tx.iter().map(Vec::as_slice).collect();
        assert!(d0.exec_watched(&refs, &[], false).await.is_some());
        one(&d0, "COPY z z2 DB 1").await;
        one(&d1, "ZADD z2 1000 top").await;
    }
    let unit = crashy(&d);
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d0 = b.db(T, 0).unwrap();
    let d1 = b.db(T, 1).unwrap();
    assert_eq!(one(&d0, "SCARD s").await, Frame::Integer(300));
    assert_eq!(one(&d0, "SISMEMBER s m299").await, Frame::Integer(1));
    assert_eq!(one(&d0, "OBJECT ENCODING small").await, bulk("intset"));
    assert_eq!(one(&d0, "ZCARD z").await, Frame::Integer(299));
    assert_eq!(one(&d0, "ZSCORE z z5").await, Frame::Double(105.0));
    assert_eq!(
        one(&d0, "ZRANGE z -1 -1").await,
        Frame::Array(vec![bulk("z5")])
    );
    assert_eq!(one(&d0, "ZRANK z z0").await, Frame::Integer(0));
    assert_eq!(one(&d0, "LLEN l").await, Frame::Integer(280));
    assert_eq!(one(&d0, "LINDEX l 0").await, bulk("m10"));
    assert_eq!(one(&d0, "HLEN h").await, Frame::Integer(3));
    assert!(
        matches!(one(&d0, "HTTL h FIELDS 1 a").await, Frame::Array(v) if v[0] != Frame::Integer(-1))
    );
    assert_eq!(one(&d0, "PFCOUNT hll").await, Frame::Integer(5));
    assert_eq!(one(&d0, "GETBIT bits 1000").await, Frame::Integer(1));
    assert_eq!(one(&d0, "GET tx").await, bulk("2"));
    assert_eq!(
        one(&d0, "LRANGE txl 0 -1").await,
        Frame::Array(vec![bulk("q")])
    );
    assert_eq!(one(&d1, "ZCARD z2").await, Frame::Integer(300));
    assert_eq!(
        one(&d1, "ZRANGE z2 -1 -1").await,
        Frame::Array(vec![bulk("top")])
    );
    // New collections after the restart never reuse a uid.
    one(&d0, &format!("SADD s2 {members}")).await;
    assert_eq!(one(&d0, "SCARD s").await, Frame::Integer(300));
    assert_eq!(one(&d0, "SINTERCARD 2 s s2").await, Frame::Integer(300));
    drop((d0, d1, b));
    drop(unit);
    let _ = std::fs::remove_dir_all(&d);
}

/// Random argument lists for every command the engine runs (arity as the
/// command table says, tokens chosen to hit options, ranges and limits):
/// no command panics, and the database stays readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_arguments_never_panic() {
    const TOKENS: &[&str] = &[
        "",
        "0",
        "1",
        "-1",
        "2",
        "-2",
        "3",
        "7",
        "10",
        "100",
        "-100",
        "9223372036854775807",
        "-9223372036854775808",
        "18446744073709551616",
        "1.5",
        "-inf",
        "+inf",
        "nan",
        "(1",
        "[a",
        "(b",
        "-",
        "+",
        "a",
        "b",
        "k1",
        "k2",
        "k3",
        "#",
        "*",
        "k*",
        "FIELDS",
        "COUNT",
        "LIMIT",
        "WITHSCORES",
        "WITHSCORE",
        "REV",
        "BYSCORE",
        "BYLEX",
        "NX",
        "XX",
        "GT",
        "LT",
        "CH",
        "INCR",
        "EX",
        "PX",
        "EXAT",
        "PXAT",
        "KEEPTTL",
        "PERSIST",
        "GET",
        "MATCH",
        "TYPE",
        "LEFT",
        "RIGHT",
        "MIN",
        "MAX",
        "BEFORE",
        "AFTER",
        "STORE",
        "STOREDIST",
        "BY",
        "ALPHA",
        "DESC",
        "ASC",
        "WEIGHTS",
        "AGGREGATE",
        "SUM",
        "u8",
        "i64",
        "u63",
        "#3",
        "OVERFLOW",
        "SAT",
        "FAIL",
        "WRAP",
        "INCRBY",
        "SET",
        "BIT",
        "BYTE",
        "IDX",
        "LEN",
        "MINMATCHLEN",
        "WITHMATCHLEN",
        "FROMLONLAT",
        "FROMMEMBER",
        "BYRADIUS",
        "BYBOX",
        "km",
        "WITHCOORD",
        "WITHDIST",
        "WITHHASH",
        "ANY",
        "DB",
        "REPLACE",
        "NOVALUES",
        "RANK",
        "MAXLEN",
        "4294967296",
        "536870912",
        "65536",
        "15",
        "16",
        "ENCODING",
        "REFCOUNT",
        "HELP",
        "AND",
        "OR",
        "NOT",
        "XOR",
        "h1",
        "z1",
        "k1z",
        "h1",
        "z1",
        "k1z",
        "f",
        "m5",
        "m77",
    ];
    let unit = mem();
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    let mut seed: u64 = std::env::var("MREDIS_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_261_121);
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed >> 33
    };
    // Some keys of every type, small and split.
    for line in [
        "SET k1 hello",
        "RPUSH k2 a b c 1 2 3",
        "SADD k3 1 2 3",
        "HSET h1 a 1 b 2",
        "ZADD z1 1 a 2 b 3 c",
    ] {
        one(&d, line).await;
    }
    let big = (0..140)
        .map(|i| format!("{i} m{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    one(&d, &format!("ZADD k1z {big}")).await;
    let cmds = sprig_redis_engine::exec::COMMANDS;
    let mut ran = 0;
    for _ in 0..60_000 {
        let name = cmds[(next() % cmds.len() as u64) as usize];
        let Some(spec) = sprig_redis::engine::lookup(name) else {
            continue;
        };
        let argc = if spec.arity >= 0 {
            spec.arity as usize
        } else {
            spec.arity.unsigned_abs() as usize + (next() % 7) as usize
        };
        let mut a = vec![Bytes::from(name.to_ascii_uppercase())];
        for _ in 1..argc {
            a.push(Bytes::from_static(
                TOKENS[(next() % TOKENS.len() as u64) as usize].as_bytes(),
            ));
        }
        if matches!(name, "flushall" | "flushdb") && !next().is_multiple_of(50) {
            continue;
        }
        // One attempt each, as inside EXEC (blocking commands don't wait).
        let r = d.exec_watched(&[&a[..]], &[], false).await;
        assert!(r.is_some_and(|r| r.len() == 1), "{a:?}");
        ran += 1;
        if ran % 5_000 == 0 {
            for db in 0..16 {
                check_invariants(&*unit, Space::redis(T, db), &format!("after {a:?}")).await;
            }
        }
    }
    assert!(ran > 50_000);
    assert!(matches!(one(&d, "DBSIZE").await, Frame::Integer(_)));
}

/// The raw keyspace of `space` is consistent: every split collection's
/// count matches its entries (lists: dense positions `head..tail`), every
/// element, score-index, field-expiry and hash-due entry belongs to a live
/// collection, and scan-index and expiry entries match the metadata.
async fn check_invariants<U: Unit>(unit: &U, space: Space, ctx: &str) {
    use sprig_redis_engine::layout::{Body, HashBody, ListBody, Meta, SetBody, ZBody, after};
    use std::collections::{BTreeMap, BTreeSet};
    let mut b = unit.begin(Vec::new()).await.unwrap();
    let all = |b: &mut <U as Unit>::Batch, lo: Vec<u8>, hi: Vec<u8>| {
        let mut out = Vec::new();
        let mut from = lo;
        loop {
            let got = b.scan(&from, &hi, 4096).unwrap();
            let n = got.len();
            if let Some((k, _)) = got.last() {
                from = after(k);
            }
            out.extend(got);
            if n < 4096 {
                return out;
            }
        }
    };
    let (si_lo, si_hi) = space.scan_index_range();
    let metas = all(
        &mut b,
        space.prefix(sprig_redis_engine::layout::KIND_META),
        si_lo.clone(),
    );
    let index = all(&mut b, si_lo, si_hi);
    assert_eq!(metas.len(), index.len(), "scan index vs metadata {ctx}");
    let mut uids: BTreeMap<u64, (Vec<u8>, Meta)> = BTreeMap::new();
    let mut exps = BTreeSet::new();
    let mut ttl_hashes = BTreeMap::new();
    for (k, v) in &metas {
        let key = space.meta_user_key(k).expect("meta key");
        let m = Meta::decode(v).expect("meta");
        assert!(
            index.iter().any(|(e, _)| *e == space.scan_entry(&key)),
            "no scan entry {ctx}"
        );
        if let Some(e) = m.exp {
            exps.insert(space.expiry(e, &key).to_vec());
        }
        if let Some(u) = m.uid() {
            assert!(
                uids.insert(u, (key.clone(), m.clone())).is_none(),
                "uid shared {ctx}"
            );
        }
        if let Body::Hash(HashBody::Split { uid, ttl: true, .. }) = m.body {
            ttl_hashes.insert(uid, key);
        }
    }
    let (x_lo, _) = space.expiry_due(i64::MAX / 2);
    let x_hi = {
        let mut v = space.prefix(sprig_redis_engine::layout::KIND_EXPIRY);
        let n = v.len();
        v[n - 2..].copy_from_slice(&1u16.to_be_bytes());
        v
    };
    let stored_exps: BTreeSet<Vec<u8>> = all(&mut b, x_lo, x_hi)
        .into_iter()
        .map(|(k, _)| k.to_vec())
        .collect();
    assert_eq!(stored_exps, exps, "expiry entries {ctx}");
    // Elements by uid.
    let elems = all(&mut b, space.prefix(KIND_ELEM), {
        let mut v = space.prefix(KIND_ELEM);
        let n = v.len();
        v[n - 2..].copy_from_slice(&1u16.to_be_bytes());
        v
    });
    // Collections deleted but not yet collected by the sweeper.
    let (d_lo, d_hi) = space.dead_range();
    let dead: BTreeSet<u64> = all(&mut b, d_lo, d_hi)
        .iter()
        .map(|(k, _)| u64::from_be_bytes(k[19..27].try_into().unwrap()))
        .collect();
    for u in &dead {
        assert!(!uids.contains_key(u), "a live value's uid is dead {ctx}");
    }
    let mut per: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
    for (k, _) in &elems {
        let uid = u64::from_be_bytes(k[19..27].try_into().unwrap());
        if !dead.contains(&uid) {
            per.entry(uid).or_default().push(k.to_vec());
        }
    }
    for (uid, ks) in &per {
        let (key, m) = uids
            .get(uid)
            .unwrap_or_else(|| panic!("orphan elements of uid {uid} {ctx}"));
        let n = ks.len() as u64;
        match &m.body {
            Body::Hash(HashBody::Split { count, .. })
            | Body::Set(SetBody::Split { count, .. })
            | Body::ZSet(ZBody::Split { count, .. }) => {
                assert_eq!(n, *count, "count of {key:?} {ctx}");
            }
            Body::List(ListBody::Split { head, tail, .. }) => {
                assert_eq!(n as i64, tail - head, "length of {key:?} {ctx}");
                for (i, k) in ks.iter().enumerate() {
                    assert_eq!(
                        Space::parse_pos(k),
                        Some(head + i as i64),
                        "positions of {key:?} {ctx}"
                    );
                }
            }
            Body::Chunked { len, .. } => {
                let chunks = len.div_ceil(sprig_redis_engine::layout::CHUNK as u64);
                for k in ks {
                    let c = Space::parse_chunk(k).expect("chunk key");
                    assert!(u64::from(c) < chunks, "chunk past the end of {key:?} {ctx}");
                }
            }
            Body::Stream(st) => {
                let entries = ks.iter().filter(|k| k.get(27) == Some(&b'e')).count() as u64;
                assert_eq!(entries, st.length, "length of stream {key:?} {ctx}");
            }
            other => panic!("elements under a non-split value {other:?} {ctx}"),
        }
    }
    for (uid, (key, m)) in &uids {
        let has = per.get(uid).map_or(0, Vec::len);
        // A chunked string's chunks of zeros aren't stored.
        let chunked = matches!(m.body, Body::Chunked { .. } | Body::Stream(_));
        assert!(
            has > 0 || chunked,
            "split value {key:?} without elements {m:?} {ctx}"
        );
    }
    // Score index: one entry per sorted-set member.
    let zs = all(
        &mut b,
        space.prefix(sprig_redis_engine::layout::KIND_ZSCORE),
        space.prefix_end(sprig_redis_engine::layout::KIND_ZSCORE),
    );
    let mut zper: BTreeMap<u64, u64> = BTreeMap::new();
    for (k, _) in &zs {
        *zper
            .entry(u64::from_be_bytes(k[19..27].try_into().unwrap()))
            .or_default() += 1;
    }
    for (uid, n) in zper.iter().filter(|(u, _)| !dead.contains(u)) {
        match uids.get(uid) {
            Some((
                _,
                Meta {
                    body: Body::ZSet(ZBody::Split { count, .. }),
                    ..
                },
            )) => {
                assert_eq!(n, count, "score index count {ctx}");
            }
            _ => panic!("orphan score index of uid {uid} {ctx}"),
        }
    }
    // Field lifetimes: index entries only under hashes flagged for them.
    let fx = all(
        &mut b,
        {
            let mut v = space.prefix(KIND_ELEM);
            let n = v.len();
            v[n - 2..].copy_from_slice(&1u16.to_be_bytes());
            v
        },
        space.prefix_end(KIND_ELEM),
    );
    for (k, _) in &fx {
        let uid = u64::from_be_bytes(k[19..27].try_into().unwrap());
        assert!(
            ttl_hashes.contains_key(&uid) || dead.contains(&uid),
            "orphan field-expiry entry {ctx}"
        );
    }
    let hd = all(
        &mut b,
        x_hi_of(space),
        space.prefix_end(sprig_redis_engine::layout::KIND_EXPIRY),
    );
    let mut due_of: BTreeMap<Vec<u8>, Vec<i64>> = BTreeMap::new();
    for (k, _) in &hd {
        let (at, key) = space.parse_hdue(k).expect("hash-due entry");
        assert!(
            ttl_hashes.values().any(|h| *h == key),
            "orphan hash-due entry {ctx}"
        );
        due_of.entry(key).or_default().push(at);
    }
    // Every hash with field lifetimes has one hash-due entry, at its
    // earliest field expiry.
    for (uid, key) in &ttl_hashes {
        let first = fx
            .iter()
            .filter(|(k, _)| u64::from_be_bytes(k[19..27].try_into().unwrap()) == *uid)
            .filter_map(|(k, _)| Space::parse_fexp(k))
            .map(|(at, _)| at)
            .min();
        let want: Vec<i64> = first.into_iter().collect();
        assert_eq!(
            due_of.get(key).cloned().unwrap_or_default(),
            want,
            "hash-due entry of {key:?} {ctx}"
        );
    }
    b.commit().unwrap().durable().await.unwrap();
}

fn x_hi_of(space: Space) -> Vec<u8> {
    let mut v = space.prefix(sprig_redis_engine::layout::KIND_EXPIRY);
    let n = v.len();
    v[n - 2..].copy_from_slice(&1u16.to_be_bytes());
    v
}

three!(
    churn_mem,
    churn_path,
    churn_rocks,
    collection_churn_keeps_invariants
);

/// Many writes to a few keys of every type, growing past the inline limits
/// and shrinking, renamed, copied, moved, stored over, popped, trimmed and
/// given field lifetimes: the keyspace stays consistent throughout.
async fn collection_churn_keeps_invariants<U: Bed>(unit: Arc<U>) {
    unit.fix_clock(Some(9_000_000));
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    let mut seed: u64 = 77;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed >> 33
    };
    let keys = ["a", "b", "c", "d"];
    for step in 0..4_000u32 {
        let k = keys[(next() % 4) as usize];
        let k2 = keys[(next() % 4) as usize];
        let n = 1 + next() % 60;
        let base = next() % 300;
        let many = |f: &dyn Fn(u64) -> String| {
            (0..n)
                .map(|i| f((base + i * 7) % 300))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let line = match next() % 24 {
            0 => format!("HSET {k} {}", many(&|i| format!("f{i} v{i}"))),
            1 => format!("HDEL {k} {}", many(&|i| format!("f{i}"))),
            2 => format!("SADD {k} {}", many(&|i| format!("m{i}"))),
            3 => format!("SADD {k} {}", many(&|i| i.to_string())),
            4 => format!("SREM {k} {}", many(&|i| format!("m{i}"))),
            5 => format!("ZADD {k} {}", many(&|i| format!("{} m{i}", i % 9))),
            6 => format!("ZREM {k} {}", many(&|i| format!("m{i}"))),
            7 => format!("ZINCRBY {k} 3 m{base}"),
            8 => format!("RPUSH {k} {}", many(&|i| format!("x{i}"))),
            9 => format!("LPOP {k} {n}"),
            10 => format!("LTRIM {k} {} -{}", next() % 10, 1 + next() % 10),
            11 => format!("LREM {k} 0 x{base}"),
            12 => format!("LINSERT {k} BEFORE x{base} ins"),
            13 => format!("RENAME {k} {k2}"),
            14 => format!("COPY {k} {k2} REPLACE"),
            15 => format!("MOVE {k} 1"),
            16 => format!("SUNIONSTORE {k} {k} {k2}"),
            17 => format!("ZUNIONSTORE {k} 2 {k} {k2}"),
            18 => format!("ZREMRANGEBYRANK {k} 0 {}", next() % 50),
            19 => format!(
                "HPEXPIRE {k} {} FIELDS 2 f{base} f{}",
                1 + next() % 200,
                (base + 7) % 300
            ),
            20 => format!("SPOP {k} {}", next() % 40),
            21 => format!("DEL {k}"),
            22 => format!("LSET {k} {} y", next() % 20),
            _ => format!("SET {k} plain"),
        };
        one(&d, &line).await;
        if step % 50 == 0 {
            unit.fix_clock(Some(9_000_000 + i64::from(step) * 10));
            sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 1000)
                .await
                .unwrap();
        }
        if step % 400 == 0 {
            for db in [0, 1] {
                check_invariants(&*unit, Space::redis(T, db), &format!("step {step}: {line}"))
                    .await;
            }
        }
    }
    for db in [0, 1] {
        check_invariants(&*unit, Space::redis(T, db), "end").await;
    }
}

three!(
    chunks_mem,
    chunks_path,
    chunks_rocks,
    chunked_strings_match_a_byte_model
);

/// Long strings changed in place are held in chunks: every read and write
/// agrees with a plain byte vector, only touched chunks are stored, and
/// a whole-value write makes the value whole again.
async fn chunked_strings_match_a_byte_model<U: Bed>(unit: Arc<U>) {
    use sprig_redis_engine::layout::CHUNK;
    let d = db(&unit);
    let mut model: Vec<u8> = Vec::new();
    let set_at = |m: &mut Vec<u8>, off: usize, data: &[u8]| {
        if m.len() < off + data.len() {
            m.resize(off + data.len(), 0);
        }
        m[off..off + data.len()].copy_from_slice(data);
    };
    // A bitmap far past the threshold: one chunk stored.
    assert_eq!(one(&d, "SETBIT bm 1000000 1").await, Frame::Integer(0));
    set_at(&mut model, 125_000, &[0x80]);
    assert_eq!(one(&d, "OBJECT ENCODING bm").await, bulk("raw"));
    assert_eq!(count(&*unit, KIND_ELEM).await, 1);
    assert_eq!(one(&d, "STRLEN bm").await, Frame::Integer(125_001));
    let mut seed = 5u64;
    for _ in 0..400 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let r = (seed >> 33) as usize;
        match r % 5 {
            0 => {
                let off = r % 300_000;
                let data = format!("x{}", r % 1000);
                one(&d, &format!("SETRANGE bm {off} {data}")).await;
                set_at(&mut model, off, data.as_bytes());
            }
            1 => {
                let bit = r % 2_400_000;
                let v = r % 2;
                let old = model.get(bit / 8).map_or(0, |b| (b >> (7 - bit % 8)) & 1);
                assert_eq!(
                    one(&d, &format!("SETBIT bm {bit} {v}")).await,
                    Frame::Integer(i64::from(old))
                );
                let mut byte = model.get(bit / 8).copied().unwrap_or(0);
                byte = (byte & !(1 << (7 - bit % 8))) | ((v as u8) << (7 - bit % 8));
                set_at(&mut model, bit / 8, &[byte]);
            }
            2 => {
                let data = format!("tail{}", r % 97);
                one(&d, &format!("APPEND bm {data}")).await;
                let at = model.len();
                set_at(&mut model, at, data.as_bytes());
            }
            3 => {
                let off = (r % 2_000_000) as u64;
                one(
                    &d,
                    &format!("BITFIELD bm SET u16 {off} 4660 INCRBY i8 {} 3", off + 40),
                )
                .await;
                let reread = one(&d, &format!("GETRANGE bm {} {}", off / 8, (off + 48) / 8)).await;
                let Frame::Bulk(b) = reread else { panic!() };
                set_at(&mut model, (off / 8) as usize, &b);
            }
            _ => {
                let s = r % model.len();
                let e = s + r % 40_000;
                let want = &model[s..=e.min(model.len() - 1)];
                assert_eq!(
                    one(&d, &format!("GETRANGE bm {s} {e}")).await,
                    Frame::Bulk(Bytes::copy_from_slice(want))
                );
            }
        }
    }
    assert_eq!(
        one(&d, "STRLEN bm").await,
        Frame::Integer(model.len() as i64)
    );
    assert_eq!(
        one(&d, "GET bm").await,
        Frame::Bulk(Bytes::from(model.clone()))
    );
    let ones: i64 = model.iter().map(|b| i64::from(b.count_ones())).sum();
    assert_eq!(one(&d, "BITCOUNT bm").await, Frame::Integer(ones));
    assert!(count(&*unit, KIND_ELEM).await <= model.len().div_ceil(CHUNK));
    // Copies and renames keep it; a whole-value write ends the chunks.
    assert_eq!(one(&d, "COPY bm bm2").await, Frame::Integer(1));
    assert_eq!(one(&d, "RENAME bm2 bm3").await, Frame::ok());
    assert_eq!(
        one(&d, "GET bm3").await,
        Frame::Bulk(Bytes::from(model.clone()))
    );
    one(&d, "SET bm3 small").await;
    one(&d, "DEL bm").await;
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(one(&d, "GET bm3").await, bulk("small"));
    // APPEND past the threshold, then reads across chunk edges.
    let piece = "y".repeat(1000);
    for _ in 0..70 {
        one(&d, &format!("APPEND ap {piece}")).await;
    }
    assert_eq!(one(&d, "STRLEN ap").await, Frame::Integer(70_000));
    assert_eq!(one(&d, "OBJECT ENCODING ap").await, bulk("raw"));
    let edge = CHUNK as i64;
    let got = one(&d, &format!("GETRANGE ap {} {}", edge - 2, edge + 2)).await;
    assert_eq!(got, bulk("yyyyy"));
    check_invariants(&*unit, Space::redis(T, 0), "chunked").await;
}

three!(
    rand_mem,
    rand_path,
    rand_rocks,
    random_picks_on_big_collections
);

fn bulks(f: Frame) -> Vec<Bytes> {
    match f {
        Frame::Array(a) | Frame::Set(a) => a
            .into_iter()
            .map(|x| match x {
                Frame::Bulk(b) => b,
                o => panic!("{o:?}"),
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

/// Random picks on collections of 10,000 entries seek windows instead of
/// reading everything: answers are members, distinct when asked, spread
/// over the collection, and `SPOP` removes exactly what it returns.
async fn random_picks_on_big_collections<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    for chunk in 0..10 {
        let ms = (0..1000)
            .map(|i| format!("m{}", chunk * 1000 + i))
            .collect::<Vec<_>>()
            .join(" ");
        let fs = (0..1000)
            .map(|i| format!("m{} v{}", chunk * 1000 + i, chunk * 1000 + i))
            .collect::<Vec<_>>()
            .join(" ");
        let zs = (0..1000)
            .map(|i| format!("{} m{}", chunk * 1000 + i, chunk * 1000 + i))
            .collect::<Vec<_>>()
            .join(" ");
        one(&d, &format!("SADD s {ms}")).await;
        one(&d, &format!("HSET h {fs}")).await;
        one(&d, &format!("ZADD z {zs}")).await;
    }
    let member = |b: &Bytes| {
        let n: usize = std::str::from_utf8(&b[1..]).unwrap().parse().unwrap();
        assert!(b[0] == b'm' && n < 10_000, "{b:?}");
        n
    };
    let mut seen = std::collections::HashSet::new();
    for _ in 0..300 {
        let Frame::Bulk(b) = one(&d, "SRANDMEMBER s").await else {
            panic!()
        };
        seen.insert(member(&b));
    }
    assert!(seen.len() > 250, "{}", seen.len());
    let ten = bulks(one(&d, "SRANDMEMBER s 10").await);
    assert_eq!(
        ten.iter().collect::<std::collections::HashSet<_>>().len(),
        10
    );
    assert_eq!(bulks(one(&d, "SRANDMEMBER s -30").await).len(), 30);
    let popped = bulks(one(&d, "SPOP s 7").await);
    assert_eq!(popped.len(), 7);
    assert_eq!(one(&d, "SCARD s").await, Frame::Integer(9_993));
    for p in &popped {
        member(p);
        assert_eq!(
            one(&d, &format!("SISMEMBER s {}", String::from_utf8_lossy(p))).await,
            Frame::Integer(0)
        );
    }
    let hv = bulks(one(&d, "HRANDFIELD h 5 WITHVALUES").await);
    assert_eq!(hv.len(), 10);
    for pair in hv.chunks(2) {
        let n = member(&pair[0]);
        assert_eq!(pair[1], Bytes::from(format!("v{n}")));
    }
    let zv = one(&d, "ZRANDMEMBER z -4 WITHSCORES").await;
    let Frame::Array(zv) = zv else { panic!() };
    for pair in zv.chunks(2) {
        let Frame::Bulk(m) = &pair[0] else { panic!() };
        assert_eq!(pair[1], Frame::Double(member(m) as f64));
    }
    // Most of the set asked for: read whole, still distinct.
    assert_eq!(bulks(one(&d, "SRANDMEMBER s 6000").await).len(), 6000);
    check_invariants(&*unit, Space::redis(T, 0), "random picks").await;
}

three!(
    fifo_mem,
    fifo_path,
    fifo_rocks,
    blocked_clients_served_in_order
);

/// Blocked clients get pushed items in the order they blocked, and one
/// that went away (its future dropped) doesn't hold up the others.
async fn blocked_clients_served_in_order<U: Bed>(unit: Arc<U>) {
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    // A client that goes away while blocked.
    let gone = {
        let d2 = Arc::clone(&d);
        tokio::spawn(async move { d2.run_batch(&[&args("BLPOP q 30")[..]]).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    gone.abort();
    let _ = gone.await;
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let d2 = Arc::clone(&d);
        waiters.push(tokio::spawn(async move {
            d2.run_batch(&[&args("BLPOP q 10")[..]])
                .await
                .pop()
                .unwrap()
        }));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let t0 = std::time::Instant::now();
    for item in ["a", "b", "c"] {
        one(&d, &format!("RPUSH q {item}")).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for (w, item) in waiters.into_iter().zip(["a", "b", "c"]) {
        let got = tokio::time::timeout(Duration::from_secs(3), w)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, Frame::Array(vec![bulk("q"), bulk(item)]));
    }
    assert!(
        t0.elapsed() < Duration::from_millis(800),
        "{:?}",
        t0.elapsed()
    );
    // Several items at once reach several clients without waiting for the
    // once-a-second retry.
    let mut waiters = Vec::new();
    for _ in 0..3 {
        let d2 = Arc::clone(&d);
        waiters.push(tokio::spawn(async move {
            d2.run_batch(&[&args("BRPOP q2 10")[..]])
                .await
                .pop()
                .unwrap()
        }));
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    let t0 = std::time::Instant::now();
    one(&d, "LPUSH q2 x y z").await;
    for w in waiters {
        tokio::time::timeout(Duration::from_secs(3), w)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "{:?}",
        t0.elapsed()
    );
}

three!(
    lead_mem,
    lead_path,
    lead_rocks,
    a_cancelled_hot_key_leader_leaves_no_queue
);

/// A client that leads a hot key's round and goes away while the round
/// waits for the key: the batches queued behind it still run, and the key
/// takes writes afterwards.
async fn a_cancelled_hot_key_leader_leaves_no_queue<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    one(&d, "SET k 0").await;
    // Something else holds the key for a while.
    let holder = unit
        .begin(vec![Space::redis(T, 0).meta(b"k")])
        .await
        .unwrap();
    let lead = {
        let d2 = Arc::clone(&d);
        tokio::spawn(async move { d2.run_batch(&[&args("INCR k")[..]]).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let behind = {
        let d2 = Arc::clone(&d);
        tokio::spawn(async move { d2.run_batch(&[&args("INCR k")[..]]).await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    lead.abort();
    let _ = lead.await;
    holder.commit().unwrap().durable().await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(3), behind)
        .await
        .expect("queued batch stuck")
        .unwrap();
    assert!(matches!(got[0], Frame::Integer(_)), "{got:?}");
    let after = tokio::time::timeout(Duration::from_secs(3), one(&d, "INCR k"))
        .await
        .expect("key stuck");
    assert!(matches!(after, Frame::Integer(n) if n >= 2), "{after:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_pages_through_keys_sharing_a_hash_tag() {
    let unit = mem();
    let d = db(&unit);
    let keys = (0..1100)
        .map(|i| format!("{{t}}:{i} v"))
        .collect::<Vec<_>>()
        .join(" ");
    one(&d, &format!("MSET {keys}")).await;
    one(&d, "SET other v").await;
    let mut cursor = "0".to_owned();
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..100 {
        let Frame::Array(r) = one(&d, &format!("SCAN {cursor} COUNT 10")).await else {
            panic!()
        };
        let Frame::Bulk(c) = &r[0] else { panic!() };
        seen.extend(bulks(r[1].clone()));
        cursor = String::from_utf8_lossy(c).into_owned();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(cursor, "0");
    assert_eq!(seen.len(), 1101);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn huge_random_counts_are_refused() {
    let unit = mem();
    let d = db(&unit);
    one(&d, "HSET h f v").await;
    one(&d, "SADD s a").await;
    for cmd in [
        "HRANDFIELD h -9223372036854775807",
        "SRANDMEMBER s -2000000",
        "ZRANDMEMBER s -9223372036854775807",
    ] {
        assert!(one(&d, cmd).await.is_error(), "{cmd}");
    }
    assert_eq!(bulks(one(&d, "SRANDMEMBER s -1000").await).len(), 1000);
}

three!(
    mixed_mem,
    mixed_path,
    mixed_rocks,
    waiters_of_other_types_pass_the_turn
);

/// A client blocked for a sorted set on a key that became a list doesn't
/// hold up a list client blocked behind it.
async fn waiters_of_other_types_pass_the_turn<U: Bed>(unit: Arc<U>) {
    let b = Backend::new(Arc::clone(&unit), Tenants::All, 16);
    let d = b.db(T, 0).unwrap();
    let z = {
        let d2 = Arc::clone(&d);
        tokio::spawn(async move { d2.run_batch(&[&args("BZPOPMIN k 3")[..]]).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let l = {
        let d2 = Arc::clone(&d);
        tokio::spawn(async move { d2.run_batch(&[&args("BLPOP k 3")[..]]).await.pop().unwrap() })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t0 = std::time::Instant::now();
    one(&d, "RPUSH k x").await;
    let got = tokio::time::timeout(Duration::from_secs(3), l)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, Frame::Array(vec![bulk("k"), bulk("x")]));
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "{:?}",
        t0.elapsed()
    );
    z.abort();
}

three!(
    dead_mem,
    dead_path,
    dead_rocks,
    big_collections_are_collected_later
);

/// Deleting a big collection is one metadata change; the sweeper removes
/// its entries afterwards, in bounded commits.
async fn big_collections_are_collected_later<U: Bed>(unit: Arc<U>) {
    let d = db(&unit);
    for chunk in 0..10 {
        let zs = (0..1000)
            .map(|i| format!("{i} m{}", chunk * 1000 + i))
            .collect::<Vec<_>>()
            .join(" ");
        one(&d, &format!("ZADD z {zs}")).await;
    }
    one(&d, "SADD small a b").await;
    let fields = (0..5000)
        .map(|i| format!("f{i} v"))
        .collect::<Vec<_>>()
        .join(" ");
    one(&d, &format!("HSET h {fields}")).await;
    one(&d, "HEXPIRE h 1000 FIELDS 2 f1 f2").await;
    assert_eq!(one(&d, "DEL z h").await, Frame::Integer(2));
    assert_eq!(one(&d, "EXISTS z").await, Frame::Integer(0));
    assert_eq!(count(&*unit, KIND_ELEM).await, 15_002);
    check_invariants(&*unit, Space::redis(T, 0), "after DEL").await;
    // A new value under the same name is independent of the old entries.
    one(&d, "ZADD z 1 fresh").await;
    assert_eq!(one(&d, "ZCARD z").await, Frame::Integer(1));
    sprig_redis_engine::sweep::pass(&*unit, Space::redis(T, 0), 100)
        .await
        .unwrap();
    assert_eq!(count(&*unit, KIND_ELEM).await, 0);
    assert_eq!(
        count(&*unit, sprig_redis_engine::layout::KIND_ZSCORE).await,
        0
    );
    check_invariants(&*unit, Space::redis(T, 0), "after the sweep").await;
    let (lo, hi) = Space::redis(T, 0).dead_range();
    let mut b = unit.begin(Vec::new()).await.unwrap();
    assert!(b.scan(&lo, &hi, 10).unwrap().is_empty());
    b.commit().unwrap().durable().await.unwrap();
}
