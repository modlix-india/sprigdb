//! Two tenants on one node: whatever one of them does, the other's keys are
//! never changed and never come back in its replies.
//!
//! One engine (the in-memory store) serves both tenants through the entry
//! points the gateway uses: SQL tables (this module's backend), documents
//! (`sprig_doc`'s store) and Redis databases (`sprig_redis_engine`). The
//! two tenants use the same database, table, collection and key names and
//! the same object ids, so only the tenant in the key keeps them apart.
//! Each run draws random operations for both tenants from a seed. After
//! every operation the whole store is read back raw:
//!
//! - no key under the other tenant's prefix was added, changed or removed;
//! - the reply holds nothing the other tenant wrote (every value a tenant
//!   writes carries that tenant's mark).
//!
//! A failure names the seed and the operation, and running the test with
//! `SPRIG_TENANT_WALL_SEEDS=<n>` tries more seeds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bson::{Document, doc};
use bytes::Bytes;
use sprig_common::ids::TenantId;
use sprig_engine::store::{Cf, IterOpts};
use sprig_engine::txn::{CommitPath, TxnOptions};
use sprig_engine::{EngineConfig, StoreKind};
use sprig_redis::engine::EngineDatabase;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::super::plan::{self, BindCtx};
use super::{DB, Engine, Fake, TempDir};

/// The node under test: one engine, three front doors.
struct Node {
    raw: sprig_engine::Engine,
    sql: Engine,
    names: Fake,
    docs: sprig_doc::docstore::DocStore,
    redis: sprig_redis_engine::Backend<sprig_redis_engine::EngineUnit>,
    _dir: TempDir,
}

impl Node {
    fn open() -> Self {
        let dir = TempDir::new("wall");
        let mut cfg = EngineConfig::new(&dir.0);
        cfg.store = StoreKind::Mem;
        cfg.log.sync = sprig_engine::log::SyncMode::Off;
        cfg.checkpoint_interval = Duration::ZERO;
        let raw = sprig_engine::Engine::open(cfg).unwrap();
        Self {
            sql: Engine::new(raw.clone()),
            names: Fake::new(),
            docs: sprig_doc::docstore::DocStore::new(Arc::new(raw.clone()) as Arc<dyn CommitPath>),
            redis: sprig_redis_engine::Backend::new(
                Arc::new(sprig_redis_engine::EngineUnit::on_engine(raw.clone())),
                sprig_redis_engine::Tenants::All,
                16,
            ),
            raw,
            _dir: dir,
        }
    }

    /// Every key and value of the store's two column families, as stored.
    fn dump(&self) -> BTreeMap<(u8, Vec<u8>), Vec<u8>> {
        let mut out = BTreeMap::new();
        for (n, cf) in [(0u8, Cf::Data), (1u8, Cf::Local)] {
            let mut it = self.raw.store().iter(cf, IterOpts::default());
            it.seek_to_first();
            while it.valid() {
                out.insert((n, it.key().to_vec()), it.value().to_vec());
                it.next();
            }
        }
        out
    }

    /// The store once nothing is still being applied: two equal reads in a
    /// row (a commit is acknowledged when durable; its apply may trail).
    async fn settled(&self) -> BTreeMap<(u8, Vec<u8>), Vec<u8>> {
        let mut last = self.dump();
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(2)).await;
            let now = self.dump();
            if now == last {
                return now;
            }
            last = now;
        }
        panic!("the store never settled");
    }

    /// One SQL statement as `tenant`; the reply as text.
    async fn sql(&self, tenant: u64, sql: &str) -> String {
        let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        let cx = BindCtx {
            tenant: TenantId(tenant),
            names: &self.names,
            current: Some(DB),
            backslash: true,
        };
        let plan = match plan::bind(&cx, &stmt) {
            Ok(Some(p)) => p,
            Ok(None) => return "not an engine statement".into(),
            Err(e) => return format!("error: {e}"),
        };
        match self
            .sql
            .run(tenant, &plan, &[], false, TxnOptions::default())
            .await
        {
            Ok(o) => format!("{:?} affected {}", o.rows, o.affected),
            Err(e) => format!("error: {e}"),
        }
    }

    /// One MongoDB command as `tenant`; the reply as text.
    async fn doc(&self, tenant: u64, body: Document) -> String {
        let name = body.keys().next().cloned().unwrap();
        let mut body = body;
        body.insert("$db", "d");
        let coll = sprig_doc::collation::Collation::of(&body).unwrap();
        let r = sprig_doc::collation::scope(
            coll,
            Box::pin(self.docs.command(tenant, "u", "d", &name, &body)),
        )
        .await;
        match r {
            Ok(sprig_doc::docstore::cmd::Reply::Doc(d)) => format!("{d:?}"),
            Ok(sprig_doc::docstore::cmd::Reply::Cursor { first, .. }) => format!("{first:?}"),
            Err(e) => format!("error: {:?}", e.to_doc()),
        }
    }

    /// One Redis command as `tenant` in database `db`; the reply as text.
    async fn redis(&self, tenant: u64, db: u32, cmd: &[String]) -> String {
        let d = self.redis.placed_db(tenant, db, true).unwrap();
        let args: Vec<Bytes> = cmd.iter().map(|a| Bytes::from(a.clone())).collect();
        format!("{:?}", d.run(&[&args[..]], true).await)
    }
}

/// The tenant id in a stored key: its first eight bytes.
fn tenant_of(key: &[u8]) -> Option<u64> {
    key.get(..8)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

/// The keys of `tenant` in a dump.
fn keys_of(
    dump: &BTreeMap<(u8, Vec<u8>), Vec<u8>>,
    tenant: u64,
) -> BTreeMap<(u8, Vec<u8>), Vec<u8>> {
    dump.iter()
        .filter(|((_, k), _)| tenant_of(k) == Some(tenant))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// A small generator (xorshift): the same seed draws the same operations.
struct Draw(u64);

impl Draw {
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

/// What a tenant writes into every value, so that a reply can be checked
/// for the other tenant's data. Letters and digits only: it survives SQL,
/// BSON and Redis framing unchanged.
fn mark(tenant: u64) -> String {
    format!("MARKOF{tenant}X")
}

/// One operation, as (what to show on a failure, the reply).
#[allow(clippy::too_many_lines, clippy::many_single_char_names)] // one arm per kind of operation
async fn step(n: &Node, tenant: u64, d: &mut Draw) -> (String, String) {
    let m = mark(tenant);
    let id = d.below(12) + 1;
    let hi = id + d.below(6);
    let k = format!("k{}", d.below(6));
    let db = u32::try_from(d.below(2)).unwrap();
    let other = 1 - db;
    let v = format!("{m}{}", d.below(1000));
    let r = |c: &[&str]| c.iter().map(|s| (*s).to_owned()).collect::<Vec<String>>();
    match d.below(40) {
        // SQL tables.
        0..=2 => {
            let q = format!(
                "INSERT INTO sbtest1(k, c, pad) VALUES ({}, '{v}', '{m}')",
                d.below(50)
            );
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        3 => {
            let q = format!("INSERT INTO owned VALUES ({id}, '{m}', {})", d.below(1000));
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        4 => {
            // Half the time every row the tenant has.
            let (id, hi) = if d.below(2) == 0 {
                (1, 100_000)
            } else {
                (id, hi)
            };
            let q = format!("SELECT id, k, c, pad FROM sbtest1 WHERE id BETWEEN {id} AND {hi}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        5 => {
            let q = format!("SELECT c FROM sbtest1 WHERE id={id}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        6 => {
            let q = format!("SELECT id, owner, v FROM owned WHERE id BETWEEN {id} AND {hi}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        7 => {
            let q = format!("UPDATE sbtest1 SET c='{v}' WHERE id BETWEEN {id} AND {hi}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        8 => {
            let q = format!("UPDATE sbtest1 SET k=k+1 WHERE id={id}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        9 => {
            let q = format!("DELETE FROM sbtest1 WHERE id BETWEEN {id} AND {hi}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        10 => {
            let q = format!("DELETE FROM owned WHERE id={id}");
            let out = n.sql(tenant, &q).await;
            (q, out)
        }
        // Documents.
        11..=13 => {
            let b = doc! {"insert": "c", "documents": [{"n": i64::try_from(id).unwrap(), "v": &v}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        14 => {
            let b =
                doc! {"insert": "o", "documents": [{"ref": i64::try_from(id).unwrap(), "v": &v}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        15 => {
            let b = doc! {"find": "c"};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        16 => {
            let b = doc! {"find": "c", "filter": {"n": {"$gte": i64::try_from(id).unwrap()}}};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        17 => {
            let b = doc! {"update": "c", "updates": [
            {"q": {"n": {"$lte": i64::try_from(hi).unwrap()}}, "u": {"$set": {"v": &v}}, "multi": true}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        18 => {
            let b = doc! {"delete": "c", "deletes": [
            {"q": {"n": i64::try_from(id).unwrap()}, "limit": 0}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        19 => {
            let b = doc! {"aggregate": "o", "cursor": {}, "pipeline": [
            {"$lookup": {"from": "c", "as": "j", "pipeline": [{"$unionWith": "o"}]}},
            {"$unionWith": {"coll": "c", "pipeline": [{"$match": {}}]}}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        20 => {
            let b = doc! {"count": "c"};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        21 => {
            let b = doc! {"listCollections": 1};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        22 => {
            let b = doc! {"createIndexes": "c", "indexes": [{"key": {"n": 1}, "name": "n_1"}]};
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        23 => {
            let b = if d.below(4) == 0 {
                doc! {"dropDatabase": 1}
            } else {
                doc! {"drop": "c"}
            };
            let out = n.doc(tenant, b.clone()).await;
            (format!("{b}"), out)
        }
        // Redis databases.
        24..=26 => {
            let c = r(&["SET", &k, &v]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        27 => {
            let c = r(&["GET", &k]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        28 => {
            let c = r(&["RPUSH", "list", &v]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        29 => {
            let c = r(&["LRANGE", "list", "0", "-1"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        30 => {
            let c = r(&["HSET", "hash", &k, &v]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        31 => {
            let c = r(&["HGETALL", "hash"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        32 => {
            let c = r(&["KEYS", "*"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        33 => {
            let c = r(&["SCAN", "0", "COUNT", "100"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        34 => {
            let c = r(&["DEL", &k, "list"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        35 => {
            let c = r(&["RENAME", &k, "k0"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        36 => {
            let c = r(&["COPY", &k, "copied", "DB", &other.to_string(), "REPLACE"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        37 => {
            let c = r(&["DBSIZE"]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        38 => {
            let c = if d.below(3) == 0 {
                r(&["FLUSHALL"])
            } else {
                r(&["FLUSHDB"])
            };
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
        _ => {
            let c = r(&["MOVE", &k, &other.to_string()]);
            (format!("db{db} {c:?}"), n.redis(tenant, db, &c).await)
        }
    }
}

/// Pairs of tenant ids, the close ones first: neighbours, ids that differ
/// in one byte of the key, and ids whose bytes are each other's prefix.
const PAIRS: [(u64, u64); 6] = [
    (61_306_000_101, 61_306_000_102),
    (1, 2),
    (255, 256),
    (0x0000_0001_0000_0000, 0x0000_0001_0000_0001),
    (0x0100_0000_0000_0000, 0x0001_0000_0000_0000),
    (7, 0x0700_0000_0000_0000),
];

#[allow(clippy::many_single_char_names)]
async fn run_seed(seed: u64, ops: usize) {
    let (a, b) = PAIRS[usize::try_from(seed % 6).unwrap()];
    let n = Node::open();
    let mut d = Draw(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut history: Vec<String> = Vec::new();
    let mut touched = [false, false];
    for i in 0..ops {
        let (actor, other) = if d.below(2) == 0 { (a, b) } else { (b, a) };
        let before = keys_of(&n.settled().await, other);
        let (what, reply) = step(&n, actor, &mut d).await;
        history.push(format!("{i}: tenant {actor}: {what} -> {reply}"));
        let after = keys_of(&n.settled().await, other);
        let trail = || history.join("\n");
        assert!(
            before == after,
            "seed {seed}: tenant {actor}'s operation changed tenant {other}'s keys \
             ({} before, {} after)\n{}",
            before.len(),
            after.len(),
            trail()
        );
        assert!(
            !reply.contains(&mark(other)),
            "seed {seed}: tenant {actor} was shown tenant {other}'s data\n{}",
            trail()
        );
        touched[usize::from(actor == b)] |= !keys_of(&n.dump(), actor).is_empty();
    }
    // Each tenant, last: it sees what it wrote through every door, and
    // nothing of the other's.
    for (me, other) in [(a, b), (b, a)] {
        let m = mark(me);
        n.sql(
            me,
            &format!("INSERT INTO sbtest1(k, c, pad) VALUES (1, '{m}', '{m}')"),
        )
        .await;
        n.doc(me, doc! {"insert": "c", "documents": [{"n": 1, "v": &m}]})
            .await;
        let write_last = vec!["SET".to_owned(), "last".to_owned(), m.clone()];
        n.redis(me, 0, &write_last).await;
        let read_last = vec!["GET".to_owned(), "last".to_owned()];
        let replies = [
            n.sql(
                me,
                "SELECT id, k, c, pad FROM sbtest1 WHERE id BETWEEN 1 AND 100000",
            )
            .await,
            n.doc(me, doc! {"find": "c"}).await,
            n.redis(me, 0, &read_last).await,
        ];
        for reply in replies {
            history.push(format!("last: tenant {me}: {reply}"));
            assert!(
                reply.contains(&m),
                "seed {seed}: tenant {me} doesn't see its own data\n{}",
                history.join("\n")
            );
            assert!(
                !reply.contains(&mark(other)),
                "seed {seed}: tenant {me} was shown tenant {other}'s data\n{}",
                history.join("\n")
            );
        }
    }
    if std::env::var("SPRIG_TENANT_WALL_TRACE").is_ok() {
        println!("seed {seed}, tenants {a} and {b}:\n{}", history.join("\n"));
    }
    // The run did write under both tenants' prefixes (the checks above
    // would pass on an empty store).
    assert!(
        touched[0] && touched[1],
        "seed {seed}: a tenant stored nothing\n{}",
        history.join("\n")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_tenants_on_one_node_never_read_or_change_each_others_keys() {
    let seeds: u64 = std::env::var("SPRIG_TENANT_WALL_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    for seed in 1..=seeds {
        run_seed(seed, 80).await;
    }
}

/// The check itself notices a key written across the wall: a raw write
/// under the other tenant's prefix, as a bug in a key builder would make.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_check_sees_a_write_under_another_tenants_prefix() {
    let (a, b) = PAIRS[0];
    let n = Node::open();
    let c = vec!["SET".to_owned(), "k".to_owned(), mark(b)];
    n.redis(b, 0, &c).await;
    let before = keys_of(&n.settled().await, b);
    assert!(!before.is_empty());
    // Tenant a's transaction, with a key that carries tenant b's id.
    let mut key = b.to_be_bytes().to_vec();
    key.extend_from_slice(b"\0\0\0\0\0\0\0\x01\x01\0\0stray");
    let mut t = n.raw.begin(TxnOptions::default()).unwrap();
    t.put(key, Bytes::from(mark(a))).unwrap();
    let _ = t.commit().unwrap();
    let after = keys_of(&n.settled().await, b);
    assert!(
        before != after,
        "a stray write under tenant {b} went unseen"
    );
}
