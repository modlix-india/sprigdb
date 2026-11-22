//! Binding and execution on the engine (its in-memory store and RocksDB),
//! from SQL text.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::exec::Outcome;
use super::plan::{self, Access, BindCtx, Names, Plan};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{EngineConfig, StoreKind};

use super::row::Datum;
use super::table::{self, Parsed};
use super::{Engine, NOT_ON_ENGINE};

/// A directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-gw-engine-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(dir: &Path, store: StoreKind, sync: SyncMode) -> Engine {
    let mut cfg = EngineConfig::new(dir);
    cfg.store = store;
    cfg.log.sync = sync;
    cfg.checkpoint_interval = Duration::ZERO;
    Engine::new(sprig_engine::Engine::open(cfg).unwrap())
}

/// An engine on the in-memory store (syncs off: these tests check SQL).
fn mem() -> (Engine, TempDir) {
    let d = TempDir::new("mem");
    (open(&d.0, StoreKind::Mem, SyncMode::Off), d)
}

const TENANT: TenantId = TenantId(61_306_000_001);
const DB: DbId = DbId(1);

const SBTEST: &str = "CREATE TABLE `sbtest1` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `k` int(11) NOT NULL DEFAULT 0,\n  `c` char(120) NOT NULL DEFAULT '',\n  `pad` char(60) NOT NULL DEFAULT '',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const WITH_K: &str = "CREATE TABLE `sbtest1` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `k` int(11) NOT NULL DEFAULT 0,\n  `c` char(120) NOT NULL DEFAULT '',\n  `pad` char(60) NOT NULL DEFAULT '',\n  PRIMARY KEY (`id`),\n  KEY `k_1` (`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const NAMED: &str = "CREATE TABLE `named` (\n  `name` varchar(20) NOT NULL,\n  `k` int(11) DEFAULT NULL,\n  `tag` varchar(10) DEFAULT NULL,\n  PRIMARY KEY (`name`),\n  KEY `tag` (`tag`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

const OWNED: &str = "CREATE TABLE `owned` (\n  `id` bigint(20) unsigned NOT NULL,\n  `owner` varchar(32) NOT NULL,\n  `v` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `v` (`v`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

struct Fake {
    tables: HashMap<String, Arc<Parsed>>,
}

impl Fake {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "sbtest1".into(),
            Arc::new(table::parse(ObjectId(10), 1, DB, SBTEST)),
        );
        tables.insert(
            "owned".into(),
            Arc::new(table::parse(ObjectId(11), 1, DB, OWNED)),
        );
        tables.insert(
            "named".into(),
            Arc::new(table::parse(ObjectId(12), 1, DB, NAMED)),
        );
        Self { tables }
    }
}

impl Names for Fake {
    fn engine_db(&self, db: DbId) -> bool {
        db == DB
    }
    fn table(&self, _db: DbId, name: &str) -> Option<Arc<Parsed>> {
        self.tables.get(&name.to_lowercase()).cloned()
    }
    fn other_relation(&self, _db: DbId, name: &str) -> bool {
        name == "v_view"
    }
    fn db_name(&self, _db: DbId) -> String {
        "sbtest".into()
    }
    fn db_by_name(&self, name: &str) -> Option<DbId> {
        (name == "sbtest")
            .then_some(DB)
            .or((name == "other").then_some(DbId(2)))
    }
    fn version(&self) -> u64 {
        1
    }
    fn referencing(&self, _db: DbId, name: &str) -> Vec<(Arc<table::TableDef>, usize)> {
        let mut out = Vec::new();
        for p in self.tables.values() {
            if let Ok(d) = &**p {
                for (i, f) in d.fks.iter().enumerate() {
                    if f.ref_table.eq_ignore_ascii_case(name) {
                        out.push((Arc::clone(d), i));
                    }
                }
            }
        }
        out.sort_by_key(|(d, i)| (d.object, *i));
        out
    }
}

fn bind(names: &Fake, sql: &str) -> sprig_common::Result<Option<Plan>> {
    let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
    let cx = BindCtx {
        tenant: TENANT,
        names,
        current: Some(DB),
        backslash: true,
    };
    plan::bind(&cx, &stmt)
}

async fn run(
    e: &Engine,
    names: &Fake,
    sql: &str,
    params: &[Datum],
) -> sprig_common::Result<Outcome> {
    let p = bind(names, sql)?.expect("an engine statement");
    e.run(TENANT.0, &p, params, false, TxnOptions::default())
        .await
}

async fn exec(e: &Engine, p: &Plan, params: &[Datum]) -> sprig_common::Result<Outcome> {
    e.run(TENANT.0, p, params, false, TxnOptions::default())
        .await
}

fn s(v: &str) -> Datum {
    Datum::Str(Bytes::copy_from_slice(v.as_bytes()))
}

fn code(r: sprig_common::Result<Outcome>) -> u16 {
    r.err().map_or(0, |e| e.protocol().mysql_errno)
}

async fn sysbench(n: i64) -> (Engine, Fake, TempDir) {
    let (e, d) = mem();
    let f = Fake::new();
    load(&e, &f, n).await;
    (e, f, d)
}

async fn load(e: &Engine, f: &Fake, n: i64) {
    let mut sql = String::from("INSERT INTO sbtest1(k, c, pad) VALUES ");
    for i in 1..=n {
        if i > 1 {
            sql.push(',');
        }
        let _ = write!(sql, "({}, 'c-{i}', 'pad-{i}')", i * 10);
    }
    let o = run(e, f, &sql, &[]).await.unwrap();
    assert_eq!(o.affected, u64::try_from(n).unwrap());
    assert_eq!(o.last_insert_id, 1);
}

#[tokio::test]
async fn point_select_skips_the_optimizer() {
    let (e, f, _d) = sysbench(20).await;
    let p = bind(&f, "SELECT c FROM sbtest1 WHERE id=?")
        .unwrap()
        .unwrap();
    assert!(matches!(p.access, Access::Point(_)));
    assert!(p.filter.is_none());
    let o = exec(&e, &p, &[Datum::Int(7)]).await.unwrap();
    assert_eq!(o.rows, vec![vec![s("c-7")]]);
    assert_eq!(o.examined, 1);
    // A missing key, a string key, a fractional key.
    assert!(
        exec(&e, &p, &[Datum::Int(99)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(exec(&e, &p, &[s("7")]).await.unwrap().rows.len(), 1);
    assert!(
        exec(&e, &p, &[Datum::Double(7.5)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn row_security_predicates_stay_as_a_filter() {
    let (e, _d) = mem();
    let f = Fake::new();
    run(
        &e,
        &f,
        "INSERT INTO owned VALUES (1, 'alice', 5), (2, 'bob', 6)",
        &[],
    )
    .await
    .unwrap();
    // The rewrite's shape: the policy first, the client's condition after.
    let p = bind(
        &f,
        "SELECT id, v FROM owned WHERE (owner = 'ALICE') AND (id = ?)",
    )
    .unwrap()
    .unwrap();
    assert!(matches!(p.access, Access::Point(_)));
    assert!(p.filter.is_some());
    // The collation is case-insensitive: 'ALICE' matches 'alice'.
    let one = exec(&e, &p, &[Datum::UInt(1)]).await.unwrap().rows;
    assert_eq!(one, vec![vec![Datum::UInt(1), Datum::Int(5)]]);
    assert!(
        exec(&e, &p, &[Datum::UInt(2)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
}

#[tokio::test]
async fn ranges_scans_and_late_indexes() {
    let (e, mut f, _d) = sysbench(600).await;
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 WHERE id BETWEEN 10 AND 12",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.rows.len(), 3);
    assert!(!o.no_index);
    let o = run(&e, &f, "SELECT id FROM sbtest1 WHERE pad = 'pad-300'", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(300)]]);
    assert!(o.no_index);
    assert_eq!(o.examined, 600);
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 ORDER BY id DESC LIMIT 2",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(600)], vec![Datum::Int(599)]]);
    // CREATE INDEX after the rows: the next statement builds it.
    f.tables.insert(
        "sbtest1".into(),
        Arc::new(table::parse(ObjectId(10), 2, DB, WITH_K)),
    );
    let p = bind(&f, "SELECT id FROM sbtest1 WHERE k = ?")
        .unwrap()
        .unwrap();
    assert!(matches!(p.access, Access::IndexEq { .. }));
    let o = exec(&e, &p, &[Datum::Int(4200)]).await.unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(420)]]);
    assert_eq!(o.examined, 1);
    // Index entries follow updates.
    let u = run(&e, &f, "UPDATE sbtest1 SET k=k+1 WHERE id=420", &[])
        .await
        .unwrap();
    assert_eq!(
        u.info.as_deref(),
        Some("Rows matched: 1  Changed: 1  Warnings: 0")
    );
    assert!(
        exec(&e, &p, &[Datum::Int(4200)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        exec(&e, &p, &[Datum::Int(4201)]).await.unwrap().rows.len(),
        1
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one table, every strict-mode rule
async fn writes_follow_strict_mode() {
    let (e, f, _d) = sysbench(3).await;
    let dup = run(
        &e,
        &f,
        "INSERT INTO sbtest1(id,k,c,pad) VALUES (1,1,'x','y')",
        &[],
    )
    .await;
    let err = dup.unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1062);
    assert_eq!(err.message, "Duplicate entry '1' for key 'PRIMARY'");
    assert_eq!(
        code(
            run(
                &e,
                &f,
                "INSERT INTO sbtest1(k,c,pad) VALUES (NULL,'a','b')",
                &[]
            )
            .await
        ),
        1048
    );
    assert_eq!(
        code(
            run(
                &e,
                &f,
                "INSERT INTO sbtest1(k,c,pad) VALUES (99999999999,'a','b')",
                &[]
            )
            .await
        ),
        1264
    );
    assert_eq!(
        code(
            run(
                &e,
                &f,
                "INSERT INTO sbtest1(k,c,pad) VALUES ('abc','a','b')",
                &[]
            )
            .await
        ),
        1366
    );
    assert_eq!(
        code(
            run(
                &e,
                &f,
                "INSERT INTO sbtest1(k,c,pad) VALUES ('12x','a','b')",
                &[]
            )
            .await
        ),
        1265
    );
    let long = format!(
        "INSERT INTO sbtest1(k,c,pad) VALUES (1,'a','{}')",
        "p".repeat(61)
    );
    assert_eq!(code(run(&e, &f, &long, &[]).await), 1406);
    assert_eq!(
        code(run(&e, &f, "INSERT INTO owned(id) VALUES (9)", &[]).await),
        1364
    );
    // A failed multi-row insert leaves nothing.
    let bad = run(
        &e,
        &f,
        "INSERT INTO sbtest1(id,k,c,pad) VALUES (50,1,'a','b'),(1,1,'a','b')",
        &[],
    )
    .await;
    assert_eq!(code(bad), 1062);
    assert!(
        run(&e, &f, "SELECT id FROM sbtest1 WHERE id=50", &[])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    // Explicit ids move the counter.
    run(
        &e,
        &f,
        "INSERT INTO sbtest1(id,k,c,pad) VALUES (100,1,'a','b')",
        &[],
    )
    .await
    .unwrap();
    let o = run(
        &e,
        &f,
        "INSERT INTO sbtest1(k,c,pad) VALUES (1,'a','b')",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.last_insert_id, 101);
    // Unique secondary keys.
    run(&e, &f, "INSERT INTO owned VALUES (1, 'a', 5)", &[])
        .await
        .unwrap();
    let u = run(&e, &f, "INSERT INTO owned VALUES (2, 'b', 5)", &[])
        .await
        .unwrap_err();
    assert_eq!(u.message, "Duplicate entry '5' for key 'v'");
    let d = run(&e, &f, "DELETE FROM sbtest1 WHERE id > 2", &[])
        .await
        .unwrap();
    assert_eq!(d.affected, 3);
    // Trailing spaces of CHAR go; the value reads back without them.
    run(
        &e,
        &f,
        "INSERT INTO sbtest1(id,k,c,pad) VALUES (7,1,'x  ','y')",
        &[],
    )
    .await
    .unwrap();
    let o = run(&e, &f, "SELECT c FROM sbtest1 WHERE id=7", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![s("x")]]);
}

#[test]
fn what_e0_doesnt_run_is_refused_and_other_databases_pass() {
    let f = Fake::new();
    let refused = |sql: &str| bind(&f, sql).err().map_or(0, |e| e.protocol().mysql_errno);
    assert_eq!(refused("SELECT COUNT(*) FROM sbtest1"), NOT_ON_ENGINE);
    assert_eq!(
        refused("SELECT c FROM sbtest1 a JOIN sbtest1 b ON a.id=b.id"),
        NOT_ON_ENGINE
    );
    assert_eq!(
        refused("SELECT c FROM sbtest1 WHERE id=1 FOR UPDATE"),
        NOT_ON_ENGINE
    );
    assert_eq!(refused("SELECT DISTINCT c FROM sbtest1"), NOT_ON_ENGINE);
    assert_eq!(
        refused("INSERT INTO sbtest1 SELECT * FROM sbtest1"),
        NOT_ON_ENGINE
    );
    assert_eq!(
        refused("REPLACE INTO sbtest1(id) VALUES (1)"),
        NOT_ON_ENGINE
    );
    assert_eq!(refused("SELECT * FROM v_view"), NOT_ON_ENGINE);
    assert_eq!(refused("SELECT * FROM nosuch"), 1146);
    assert_eq!(refused("SELECT zz FROM sbtest1"), 1054);
    assert_eq!(refused("SELECT c FROM sbtest1 WHERE zz = 1"), 1054);
    assert_eq!(
        refused("SELECT c FROM sbtest1 UNION SELECT c FROM sbtest1"),
        NOT_ON_ENGINE
    );
    // Not the engine's: another database, no table.
    assert!(bind(&f, "SELECT c FROM other.t").unwrap().is_none());
    assert!(bind(&f, "SELECT 1").unwrap().is_none());
    assert!(
        bind(
            &f,
            "SELECT c FROM t61306000001_d1_g1_s1.sbtest1 WHERE id = 1"
        )
        .unwrap()
        .is_some()
    );
}

/// Statements [`plan::bind`] leaves to MariaDB are still found when they
/// name an engine table somewhere the binder doesn't look: MariaDB's copy
/// of the table is empty, so they must be refused, not forwarded.
#[test]
fn engine_tables_are_found_wherever_a_statement_names_them() {
    let f = Fake::new();
    let names = |sql: &str| {
        let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        let cx = BindCtx {
            tenant: TENANT,
            names: &f,
            current: Some(DB),
            backslash: true,
        };
        // Refused by the binder itself, or passed on and found here; never
        // bound as one engine table's statement.
        match plan::bind(&cx, &stmt) {
            Ok(None) => plan::names_engine_table(&cx, &stmt),
            Ok(Some(_)) => panic!("bound: {sql}"),
            Err(e) => e.protocol().mysql_errno == NOT_ON_ENGINE,
        }
    };
    for sql in [
        "SELECT * FROM (SELECT * FROM sbtest1) x",
        "DELETE FROM other.a WHERE id NOT IN (SELECT id FROM sbtest1)",
        "UPDATE other.a SET v = (SELECT k FROM sbtest.sbtest1 WHERE id = 1)",
        "INSERT INTO other.a SELECT id FROM t61306000001_d1_g1_s1.sbtest1",
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM named)",
        "WITH w AS (SELECT id FROM sbtest1) SELECT * FROM other.a JOIN w ON w.id = a.id",
        "SET @v = (SELECT k FROM sbtest1 WHERE id = 1)",
    ] {
        assert!(names(sql), "not found: {sql}");
    }
    for sql in [
        "SELECT c FROM other.t",
        "SELECT 1",
        // A common table expression's own name is no engine table.
        "WITH w AS (SELECT 1 AS id) SELECT * FROM w",
        "DELETE FROM other.a WHERE id IN (SELECT id FROM other.b)",
    ] {
        assert!(!names(sql), "found: {sql}");
    }
}

/// `UPDATE` assigns left to right: a later assignment reads what an
/// earlier one stored, as MariaDB does.
#[tokio::test]
async fn update_assigns_left_to_right() {
    let (e, f, _d) = sysbench(3).await;
    run(
        &e,
        &f,
        "UPDATE sbtest1 SET k = k + 1, c = k WHERE id = 2",
        &[],
    )
    .await
    .unwrap();
    let o = run(&e, &f, "SELECT k, c FROM sbtest1 WHERE id = 2", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(21), s("21")]]);
}

#[tokio::test]
async fn string_keys_follow_the_collation() {
    let (e, _d) = mem();
    let f = Fake::new();
    run(
        &e,
        &f,
        "INSERT INTO named VALUES ('Alice', 1, 'x'), ('bob', 2, 'X'), ('carol', 3, NULL)",
        &[],
    )
    .await
    .unwrap();
    // Case and trailing spaces don't count for the key, as in MariaDB.
    let dup = run(&e, &f, "INSERT INTO named VALUES ('ALICE ', 9, NULL)", &[])
        .await
        .unwrap_err();
    assert_eq!(dup.message, "Duplicate entry 'ALICE ' for key 'PRIMARY'");
    let p = bind(&f, "SELECT k FROM named WHERE name = ?")
        .unwrap()
        .unwrap();
    assert!(matches!(p.access, Access::Point(_)));
    assert_eq!(
        exec(&e, &p, &[s("alice")]).await.unwrap().rows,
        vec![vec![Datum::Int(1)]]
    );
    let t = bind(&f, "SELECT name FROM named WHERE tag = 'x'")
        .unwrap()
        .unwrap();
    assert!(matches!(t.access, Access::IndexEq { .. }));
    let o = exec(&e, &t, &[]).await.unwrap();
    assert_eq!(o.rows, vec![vec![s("Alice")], vec![s("bob")]]);
    let o = run(
        &e,
        &f,
        "SELECT name FROM named WHERE name > 'b' ORDER BY name",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.rows, vec![vec![s("bob")], vec![s("carol")]]);
    // An update moving the key and the index entry.
    run(
        &e,
        &f,
        "UPDATE named SET name = 'dave', tag = 'y' WHERE name = 'BOB'",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        exec(&e, &t, &[]).await.unwrap().rows,
        vec![vec![s("Alice")]]
    );
    assert_eq!(
        exec(&e, &p, &[s("DAVE")]).await.unwrap().rows,
        vec![vec![Datum::Int(2)]]
    );
    assert!(exec(&e, &p, &[s("bob")]).await.unwrap().rows.is_empty());
}

#[tokio::test]
async fn index_numbers_survive_a_new_index_listed_first() {
    let (e, mut f, _d) = sysbench(50).await;
    f.tables.insert(
        "sbtest1".into(),
        Arc::new(table::parse(ObjectId(10), 2, DB, WITH_K)),
    );
    let k = bind(&f, "SELECT id FROM sbtest1 WHERE k = ?")
        .unwrap()
        .unwrap();
    assert_eq!(
        exec(&e, &k, &[Datum::Int(300)]).await.unwrap().rows,
        vec![vec![Datum::Int(30)]]
    );
    // MariaDB lists a unique index before the others.
    let with_u = WITH_K.replace(
        "  KEY `k_1` (`k`)",
        "  UNIQUE KEY `c` (`c`),\n  KEY `k_1` (`k`)",
    );
    f.tables.insert(
        "sbtest1".into(),
        Arc::new(table::parse(ObjectId(10), 3, DB, &with_u)),
    );
    let k = bind(&f, "SELECT id FROM sbtest1 WHERE k = ?")
        .unwrap()
        .unwrap();
    assert_eq!(
        exec(&e, &k, &[Datum::Int(300)]).await.unwrap().rows,
        vec![vec![Datum::Int(30)]]
    );
    let c = bind(&f, "SELECT id FROM sbtest1 WHERE c = ?")
        .unwrap()
        .unwrap();
    assert!(matches!(c.access, Access::IndexEq { .. }));
    assert_eq!(
        exec(&e, &c, &[s("c-7")]).await.unwrap().rows,
        vec![vec![Datum::Int(7)]]
    );
    let dup = run(
        &e,
        &f,
        "INSERT INTO sbtest1(k,c,pad) VALUES (1,'c-7','p')",
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(dup.message, "Duplicate entry 'c-7' for key 'c'");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_of_one_row_lose_nothing() {
    let (e, f, _d) = sysbench(5).await;
    let e = Arc::new(e);
    let f = Arc::new(f);
    let p = Arc::new(
        bind(&f, "UPDATE sbtest1 SET k = k + 1 WHERE id = ?")
            .unwrap()
            .unwrap(),
    );
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (e, p) = (Arc::clone(&e), Arc::clone(&p));
        tasks.push(tokio::spawn(async move {
            for _ in 0..25 {
                let o = exec(&e, &p, &[Datum::Int(3)]).await.unwrap();
                assert_eq!(o.affected, 1);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let o = run(&e, &f, "SELECT k FROM sbtest1 WHERE id = 3", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(30 + 200)]]);
}

#[tokio::test]
async fn committed_rows_survive_a_restart_on_rocksdb() {
    let d = TempDir::new("rocks");
    let mut f = Fake::new();
    f.tables.insert(
        "sbtest1".into(),
        Arc::new(table::parse(ObjectId(10), 2, DB, WITH_K)),
    );
    {
        let e = open(&d.0, StoreKind::Rocks, SyncMode::Full);
        load(&e, &f, 300).await;
        run(&e, &f, "UPDATE sbtest1 SET c = 'changed' WHERE id = 5", &[])
            .await
            .unwrap();
        run(&e, &f, "DELETE FROM sbtest1 WHERE id = 6", &[])
            .await
            .unwrap();
    }
    let e = open(&d.0, StoreKind::Rocks, SyncMode::Full);
    let o = run(&e, &f, "SELECT c FROM sbtest1 WHERE id = 5", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![s("changed")]]);
    assert!(
        run(&e, &f, "SELECT c FROM sbtest1 WHERE id = 6", &[])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    let o = run(&e, &f, "SELECT id FROM sbtest1 WHERE id > 290", &[])
        .await
        .unwrap();
    assert_eq!(o.rows.len(), 10);
    // The index record came back: the index answers without a rebuild.
    let o = run(&e, &f, "SELECT id FROM sbtest1 WHERE k = 1230", &[])
        .await
        .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(123)]]);
    // AUTO_INCREMENT continues after the largest stored value.
    let o = run(
        &e,
        &f,
        "INSERT INTO sbtest1(k,c,pad) VALUES (1,'a','b')",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.last_insert_id, 301);
}

// ----- transactions ---------------------------------------------------------

use super::control::{self, Control, Level, LockWait, Scope, Switch};
use super::session::{SessionTxn, Start};
use sprig_engine::txn::Isolation;

/// One session: its transaction and the options its transactions start
/// with.
struct Sess {
    txn: SessionTxn,
    opts: TxnOptions,
}

impl Sess {
    fn mariadb() -> Self {
        Self {
            txn: SessionTxn::default(),
            opts: TxnOptions::mariadb().with_lock_wait_seconds(1),
        }
    }

    fn mysql() -> Self {
        Self {
            txn: SessionTxn::default(),
            opts: TxnOptions::mysql().with_lock_wait_seconds(1),
        }
    }

    fn level(mut self, l: Isolation) -> Self {
        self.opts = self.opts.with_isolation(l);
        self
    }

    fn begin(&mut self, e: &Engine) {
        self.txn
            .begin(e, self.opts, false, Start::default())
            .unwrap();
    }

    /// Runs `sql` in the open transaction, or in its own when none is.
    async fn run(&mut self, e: &Engine, f: &Fake, sql: &str) -> sprig_common::Result<Outcome> {
        let p = bind(f, sql)?.expect("an engine statement");
        let Some(tx) = self.txn.statement() else {
            return e.run(TENANT.0, &p, &[], false, self.opts).await;
        };
        let r = e.run_in(TENANT.0, &p, &[], false, tx).await;
        self.txn.statement_done();
        r
    }

    async fn k(&mut self, e: &Engine, f: &Fake, id: i64) -> Option<i64> {
        let o = self
            .run(e, f, &format!("SELECT k FROM sbtest1 WHERE id = {id}"))
            .await
            .unwrap();
        o.rows.first().map(|r| match r[0] {
            Datum::Int(v) => v,
            _ => panic!("k is an integer"),
        })
    }
}

#[test]
#[allow(clippy::too_many_lines)] // one line per statement form
fn control_statements_are_read() {
    let p = |t: &str| control::parse(t.as_bytes());
    let begin = |read_only, snapshot| {
        Some(Control::Begin {
            read_only,
            snapshot,
        })
    };
    assert_eq!(p("BEGIN"), begin(None, false));
    assert_eq!(p("  begin work ;"), begin(None, false));
    assert_eq!(p("/* app */ START TRANSACTION"), begin(None, false));
    assert_eq!(p("start transaction read only"), begin(Some(true), false));
    assert_eq!(
        p("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ WRITE"),
        begin(Some(false), true)
    );
    // Not transaction control: a compound statement, a malformed start,
    // an executable comment (MariaDB reads and answers those).
    assert_eq!(p("BEGIN NOT ATOMIC SELECT 1; END"), None);
    assert_eq!(p("START TRANSACTION READ ONLY, READ WRITE"), None);
    assert_eq!(p("/*!40101 COMMIT */"), None);
    assert_eq!(p("SELECT 1"), None);
    assert_eq!(p("BEGINX"), None);
    assert_eq!(
        p("COMMIT"),
        Some(Control::Commit {
            chain: None,
            release: None
        })
    );
    assert_eq!(
        p("commit work and chain no release"),
        Some(Control::Commit {
            chain: Some(true),
            release: Some(false)
        })
    );
    assert_eq!(
        p("ROLLBACK AND NO CHAIN RELEASE"),
        Some(Control::Rollback {
            chain: Some(false),
            release: Some(true)
        })
    );
    assert_eq!(p("COMMIT AND"), None);
    assert_eq!(p("SAVEPOINT Sp1"), Some(Control::Savepoint("Sp1".into())));
    assert_eq!(
        p("savepoint `my sp`"),
        Some(Control::Savepoint("my sp".into()))
    );
    assert_eq!(
        p("ROLLBACK TO SAVEPOINT sp1"),
        Some(Control::RollbackTo("sp1".into()))
    );
    assert_eq!(
        p("rollback work to sp1"),
        Some(Control::RollbackTo("sp1".into()))
    );
    assert_eq!(
        p("RELEASE SAVEPOINT sp1"),
        Some(Control::Release("sp1".into()))
    );
    assert_eq!(
        p("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
        Some(Control::SetTransaction {
            scope: Scope::Next,
            isolation: Some(Level::ReadCommitted),
            read_only: None
        })
    );
    assert_eq!(
        p("set session transaction read only, isolation level serializable"),
        Some(Control::SetTransaction {
            scope: Scope::Session,
            isolation: Some(Level::Serializable),
            read_only: Some(true)
        })
    );
    assert_eq!(
        p("SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
        Some(Control::SetTransaction {
            scope: Scope::Global,
            isolation: Some(Level::RepeatableRead),
            read_only: None
        })
    );
    assert_eq!(p("SET TRANSACTION ISOLATION LEVEL SNAPSHOT"), None);
    let set = |autocommit, lock_wait| {
        Some(Control::Set {
            autocommit,
            lock_wait,
            results_charset: false,
        })
    };
    let charset = Some(Control::Set {
        autocommit: None,
        lock_wait: None,
        results_charset: true,
    });
    assert_eq!(p("SET NAMES utf8mb4 COLLATE utf8mb4_bin"), charset);
    assert_eq!(p("set character set latin1"), charset);
    assert_eq!(p("SET character_set_results = utf8mb4"), charset);
    assert_eq!(p("SET collation_connection = utf8mb4_bin"), None);
    assert_eq!(p("SET autocommit = 0"), set(Some(Switch::Off), None));
    assert_eq!(
        p("SET @@session.autocommit := ON, sql_mode = 'a''b'"),
        set(Some(Switch::On), None)
    );
    assert_eq!(p("SET @autocommit = 1"), None, "a user variable");
    assert_eq!(p("SET GLOBAL autocommit = 1"), None);
    assert_eq!(p("SET @@global.autocommit = 1"), None);
    assert_eq!(p("SET sql_mode = 'autocommit = 1'"), None);
    assert_eq!(
        p("set sql_safe_updates = 1, AUTOCOMMIT=1"),
        set(Some(Switch::On), None)
    );
    assert_eq!(
        p("SET autocommit = DEFAULT"),
        set(Some(Switch::Unknown), None)
    );
    assert_eq!(
        p("SET autocommit = 1 - 1"),
        set(Some(Switch::Unknown), None)
    );
    // The lock wait: MariaDB reports that it changed, not its value.
    assert_eq!(
        p("SET innodb_lock_wait_timeout = 3"),
        set(None, Some(LockWait::Seconds(3)))
    );
    assert_eq!(
        p("set session innodb_lock_wait_timeout=DEFAULT, autocommit=0"),
        set(Some(Switch::Off), Some(LockWait::Default))
    );
    assert_eq!(
        p("SET @@innodb_lock_wait_timeout = @w"),
        set(None, Some(LockWait::Unknown))
    );
    assert_eq!(p("SET GLOBAL innodb_lock_wait_timeout = 3"), None);
    assert_eq!(
        p("SET STATEMENT innodb_lock_wait_timeout = 1 FOR SELECT 1"),
        None
    );
    assert_eq!(p("SET sql_notes = 1"), None);
    let xid = |g: &str, b: &str, format: u32| control::Xid {
        gtrid: g.as_bytes().to_vec(),
        bqual: b.as_bytes().to_vec(),
        format,
    };
    assert_eq!(
        p("XA START 'x'"),
        Some(Control::Xa(control::Xa::Start {
            xid: xid("x", "", 1),
            join: false
        }))
    );
    assert_eq!(
        p("xa begin 0x7476, X'62', 7 JOIN"),
        Some(Control::Xa(control::Xa::Start {
            xid: xid("tv", "b", 7),
            join: true
        }))
    );
    assert_eq!(
        p("XA END 'it''s', \"b\""),
        Some(Control::Xa(control::Xa::End {
            xid: xid("it's", "b", 1),
            suspend: false
        }))
    );
    assert_eq!(
        p("XA PREPARE 'x'"),
        Some(Control::Xa(control::Xa::Prepare(xid("x", "", 1))))
    );
    assert_eq!(
        p("XA COMMIT 'x' ONE PHASE"),
        Some(Control::Xa(control::Xa::Commit {
            xid: xid("x", "", 1),
            one_phase: true
        }))
    );
    assert_eq!(
        p("XA ROLLBACK 'x','y'"),
        Some(Control::Xa(control::Xa::Rollback(xid("x", "y", 1))))
    );
    assert_eq!(p("XA RECOVER"), Some(Control::Xa(control::Xa::Recover)));
    // A prepared `SET` that gives autocommit a parameter: which one.
    let ap = |t: &str| control::autocommit_param(t.as_bytes());
    assert_eq!(ap("SET autocommit = ?"), Some(0));
    assert_eq!(ap("set @a = ?, @@session.autocommit := ?"), Some(1));
    assert_eq!(ap("SET autocommit = 1"), None);
    assert_eq!(ap("SET @autocommit = ?"), None);
    assert_eq!(ap("SELECT ?"), None);
    // Forms this layer doesn't read: an option, a part longer than 64
    // bytes, a missing xid.
    assert_eq!(
        p("XA RECOVER FORMAT = 'SQL'"),
        Some(Control::Xa(control::Xa::RecoverWith))
    );
    for other in [
        format!("XA START '{}'", "a".repeat(65)),
        "XA START".to_owned(),
        "XA COMMIT 'x' ONE".to_owned(),
    ] {
        assert_eq!(p(&other), Some(Control::Xa(control::Xa::Other)), "{other}");
    }
    // An xid's engine key names its tenant; another tenant's key isn't read.
    let x = xid("gt", "bq", 9);
    let key = x.engine_key(7);
    assert_eq!(control::Xid::from_engine_key(7, &key), Some(x));
    assert_eq!(control::Xid::from_engine_key(8, &key), None);
    assert_eq!(p("SAVEPOINT 'x"), None, "an unterminated quote");
    assert_eq!(
        p("LOCK TABLES t1 READ, d.t2 AS x WRITE, `a b`.`c` LOW_PRIORITY WRITE"),
        Some(Control::LockTables(vec![
            (None, "t1".to_owned()),
            (Some("d".to_owned()), "t2".to_owned()),
            (Some("a b".to_owned()), "c".to_owned()),
        ]))
    );
    assert_eq!(p("UNLOCK TABLES"), Some(Control::UnlockTables));
    assert_eq!(
        Level::from_value("READ-COMMITTED"),
        Some(Level::ReadCommitted)
    );
    assert_eq!(
        Level::from_value("'repeatable read'"),
        Some(Level::RepeatableRead)
    );
    assert_eq!(Level::from_value("x"), None);
    assert_eq!(Level::ReadUncommitted.engine(), Isolation::ReadCommitted);
}

#[tokio::test]
async fn a_transaction_commits_or_rolls_back_as_a_whole() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mariadb();
    let mut b = Sess::mariadb();
    a.begin(&e);
    assert!(a.txn.is_open() && !a.txn.read_only());
    a.run(&e, &f, "UPDATE sbtest1 SET k = 11 WHERE id = 1")
        .await
        .unwrap();
    a.run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (9, 90)")
        .await
        .unwrap();
    // Its own writes, and nobody else's view of them.
    assert_eq!(a.k(&e, &f, 1).await, Some(11));
    assert_eq!(a.k(&e, &f, 9).await, Some(90));
    assert_eq!(b.k(&e, &f, 1).await, Some(10));
    assert_eq!(b.k(&e, &f, 9).await, None);
    a.txn.commit().await.unwrap();
    assert!(!a.txn.is_open());
    assert_eq!(b.k(&e, &f, 1).await, Some(11));
    assert_eq!(b.k(&e, &f, 9).await, Some(90));

    a.begin(&e);
    a.run(&e, &f, "DELETE FROM sbtest1 WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(a.k(&e, &f, 2).await, None);
    assert!(a.txn.rollback().is_some());
    assert!(!a.txn.is_open());
    assert_eq!(b.k(&e, &f, 2).await, Some(20));
    // Nothing open: both are no-ops.
    assert!(a.txn.commit().await.unwrap().is_none());
    assert!(a.txn.rollback().is_none());
}

#[tokio::test]
async fn a_failed_statement_undoes_itself_and_keeps_the_transaction() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mariadb();
    a.begin(&e);
    a.run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (7, 70)")
        .await
        .unwrap();
    // The second row is a duplicate: the first row of the statement goes
    // with it, the earlier statement's row stays.
    let r = a
        .run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (8, 80), (7, 71)")
        .await;
    assert_eq!(code(r), 1062);
    assert!(a.txn.is_open());
    assert_eq!(a.k(&e, &f, 8).await, None);
    assert_eq!(a.k(&e, &f, 7).await, Some(70));
    a.txn.commit().await.unwrap();
    let mut b = Sess::mariadb();
    assert_eq!(b.k(&e, &f, 7).await, Some(70));
    assert_eq!(b.k(&e, &f, 8).await, None);
}

#[tokio::test]
async fn savepoints_undo_back_to_a_name() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mariadb();
    // Outside a transaction there is nothing to name.
    assert!(!a.txn.savepoint("s1"));
    // Under `autocommit = 0` a savepoint opens a transaction nobody is
    // told about until a statement touches a table.
    a.txn.begin_quiet(&e, a.opts, false).unwrap();
    assert!(a.txn.is_open() && !a.txn.reported());
    assert!(a.txn.savepoint("b1"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 500 WHERE id = 3")
        .await
        .unwrap();
    assert!(a.txn.reported());
    a.txn.rollback_to(&e, "b1").unwrap();
    assert_eq!(a.k(&e, &f, 3).await, Some(30));
    a.txn.rollback();
    assert_eq!(
        a.txn
            .rollback_to(&e, "s1")
            .unwrap_err()
            .protocol()
            .mysql_errno,
        1305
    );
    a.begin(&e);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 1 WHERE id = 1")
        .await
        .unwrap();
    assert!(a.txn.savepoint("s1"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 2 WHERE id = 1")
        .await
        .unwrap();
    assert!(a.txn.savepoint("s2"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 3 WHERE id = 1")
        .await
        .unwrap();
    // Names compare without case; the savepoint stays after a rollback
    // to it, later ones are forgotten.
    a.txn.rollback_to(&e, "S1").unwrap();
    assert_eq!(a.k(&e, &f, 1).await, Some(1));
    let gone = a.txn.rollback_to(&e, "s2").unwrap_err();
    assert_eq!(gone.protocol().mysql_errno, 1305);
    assert_eq!(gone.message, "SAVEPOINT s2 does not exist");
    a.run(&e, &f, "UPDATE sbtest1 SET k = 4 WHERE id = 1")
        .await
        .unwrap();
    a.txn.rollback_to(&e, "s1").unwrap();
    assert_eq!(a.k(&e, &f, 1).await, Some(1));
    // Setting a name again moves it.
    a.run(&e, &f, "UPDATE sbtest1 SET k = 5 WHERE id = 1")
        .await
        .unwrap();
    assert!(a.txn.savepoint("s1"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 6 WHERE id = 1")
        .await
        .unwrap();
    a.txn.rollback_to(&e, "s1").unwrap();
    assert_eq!(a.k(&e, &f, 1).await, Some(5));
    // Release forgets the name and undoes nothing.
    a.txn.release("s1").unwrap();
    assert_eq!(
        a.txn.release("s1").unwrap_err().protocol().mysql_errno,
        1305
    );
    assert_eq!(a.k(&e, &f, 1).await, Some(5));
    a.txn.commit().await.unwrap();
    // Locks taken after a savepoint stay after a rollback to it, unless
    // the savepoint was set before the transaction did anything: then
    // they all go, as on both servers.
    let mut b = Sess::mariadb();
    a.begin(&e);
    assert!(a.txn.savepoint("first"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 91 WHERE id = 2")
        .await
        .unwrap();
    assert!(a.txn.savepoint("second"));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 92 WHERE id = 3")
        .await
        .unwrap();
    a.txn.rollback_to(&e, "second").unwrap();
    let r = b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 3").await;
    assert_eq!(code(r), 1205, "row 3 is still locked");
    a.txn.rollback_to(&e, "first").unwrap();
    assert!(a.txn.is_open());
    b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 2")
        .await
        .unwrap();
    b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 3")
        .await
        .unwrap();
    // The names survive, and the transaction goes on.
    a.run(&e, &f, "UPDATE sbtest1 SET k = 93 WHERE id = 2")
        .await
        .unwrap();
    a.txn.rollback_to(&e, "FIRST").unwrap();
    a.txn.commit().await.unwrap();
    assert_eq!(b.k(&e, &f, 2).await, Some(0));
    // A new transaction starts without the old names.
    a.begin(&e);
    assert_eq!(
        a.txn
            .rollback_to(&e, "s1")
            .unwrap_err()
            .protocol()
            .mysql_errno,
        1305
    );
    a.txn.rollback();
    let mut b = Sess::mariadb();
    assert_eq!(b.k(&e, &f, 1).await, Some(5));
}

#[tokio::test]
async fn repeatable_read_keeps_its_view_and_read_committed_does_not() {
    let (e, f, _d) = sysbench(3).await;
    let mut w = Sess::mariadb();
    let mut rr = Sess::mariadb();
    let mut rc = Sess::mariadb().level(Isolation::ReadCommitted);
    rr.begin(&e);
    rc.begin(&e);
    assert_eq!(rr.txn.isolation(), Some(Isolation::RepeatableRead));
    assert_eq!(rc.txn.isolation(), Some(Isolation::ReadCommitted));
    assert_eq!(rr.k(&e, &f, 1).await, Some(10));
    assert_eq!(rc.k(&e, &f, 1).await, Some(10));
    w.run(&e, &f, "UPDATE sbtest1 SET k = 99 WHERE id = 1")
        .await
        .unwrap();
    w.run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (50, 500)")
        .await
        .unwrap();
    assert_eq!(rr.k(&e, &f, 1).await, Some(10));
    assert_eq!(rr.k(&e, &f, 50).await, None);
    assert_eq!(rc.k(&e, &f, 1).await, Some(99));
    assert_eq!(rc.k(&e, &f, 50).await, Some(500));
    rr.txn.commit().await.unwrap();
    rc.txn.commit().await.unwrap();
    assert_eq!(rr.k(&e, &f, 1).await, Some(99));
}

#[tokio::test]
async fn a_write_over_a_newer_commit_fails_on_mariadb_only() {
    let (e, f, _d) = sysbench(3).await;
    let mut w = Sess::mariadb();
    // MariaDB's port: the row changed since the transaction's snapshot,
    // so the update fails with 1020 and the transaction is gone.
    let mut a = Sess::mariadb();
    a.begin(&e);
    assert_eq!(a.k(&e, &f, 1).await, Some(10));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 5 WHERE id = 2")
        .await
        .unwrap();
    // MySQL's port: no such check; the update works on the latest row.
    let mut m = Sess::mysql();
    m.begin(&e);
    assert_eq!(m.k(&e, &f, 1).await, Some(10));
    w.run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 1")
        .await
        .unwrap();
    let err = a
        .run(&e, &f, "UPDATE sbtest1 SET k = k + 100 WHERE id = 1")
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1020);
    assert_eq!(
        err.message,
        "Record has changed since last read in table 'sbtest1'; try restarting transaction"
    );
    assert!(!a.txn.is_open(), "1020 rolls the whole transaction back");
    assert_eq!(w.k(&e, &f, 2).await, Some(20), "its earlier update too");
    let o = m
        .run(&e, &f, "UPDATE sbtest1 SET k = k + 100 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(o.affected, 1);
    assert!(m.txn.is_open());
    m.txn.commit().await.unwrap();
    assert_eq!(w.k(&e, &f, 1).await, Some(111));
    // A transaction that never read plainly has no snapshot to conflict
    // with, on MariaDB's port too.
    a.begin(&e);
    w.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 3")
        .await
        .unwrap();
    a.run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 3")
        .await
        .unwrap();
    a.txn.commit().await.unwrap();
    assert_eq!(w.k(&e, &f, 3).await, Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lock_wait_times_out_and_the_transaction_stays() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mariadb();
    let mut b = Sess::mariadb();
    a.begin(&e);
    b.begin(&e);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 1 WHERE id = 1")
        .await
        .unwrap();
    b.run(&e, &f, "UPDATE sbtest1 SET k = 2 WHERE id = 2")
        .await
        .unwrap();
    // b waits one second for a's row, then gets 1205; only that statement
    // is undone.
    let r = b.run(&e, &f, "UPDATE sbtest1 SET k = 3 WHERE id = 1").await;
    assert_eq!(code(r), 1205);
    assert!(b.txn.is_open());
    a.txn.commit().await.unwrap();
    // The row is free now.
    b.run(&e, &f, "UPDATE sbtest1 SET k = 3 WHERE id = 1")
        .await
        .unwrap();
    b.txn.commit().await.unwrap();
    assert_eq!(a.k(&e, &f, 1).await, Some(3));
    assert_eq!(a.k(&e, &f, 2).await, Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadlock_rolls_one_transaction_back() {
    let (e, f, _d) = sysbench(3).await;
    let e = Arc::new(e);
    let f = Arc::new(f);
    let mut a = Sess::mariadb();
    let mut b = Sess::mariadb();
    a.opts = a.opts.with_lock_wait_seconds(20);
    b.opts = b.opts.with_lock_wait_seconds(20);
    a.begin(&e);
    b.begin(&e);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 1 WHERE id = 1")
        .await
        .unwrap();
    b.run(&e, &f, "UPDATE sbtest1 SET k = 2 WHERE id = 2")
        .await
        .unwrap();
    // a waits for b's row; then b asks for a's: one of them is the victim.
    let (e2, f2) = (Arc::clone(&e), Arc::clone(&f));
    let waiter = tokio::spawn(async move {
        let r = a
            .run(&e2, &f2, "UPDATE sbtest1 SET k = 11 WHERE id = 2")
            .await;
        (a, r)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rb = b
        .run(&e, &f, "UPDATE sbtest1 SET k = 22 WHERE id = 1")
        .await;
    let (mut a, ra) = waiter.await.unwrap();
    let (ca, cb) = (code(ra), code(rb));
    assert!(
        (ca == 1213 && cb == 0) || (ca == 0 && cb == 1213),
        "one victim: a {ca}, b {cb}"
    );
    // The victim is outside a transaction; the other commits both rows.
    let (victim, winner, want) = if ca == 1213 {
        (&mut a, &mut b, (Some(22), Some(2)))
    } else {
        (&mut b, &mut a, (Some(1), Some(11)))
    };
    assert!(!victim.txn.is_open());
    assert!(winner.txn.is_open());
    winner.txn.commit().await.unwrap();
    assert_eq!(victim.k(&e, &f, 1).await, want.0);
    assert_eq!(victim.k(&e, &f, 2).await, want.1);
}

#[tokio::test]
async fn a_dropped_statement_is_undone_and_set_transaction_applies_once() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mariadb();
    a.begin(&e);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 1 WHERE id = 1")
        .await
        .unwrap();
    // A statement whose request was dropped half-way: its writes go, the
    // transaction stays.
    {
        let p = bind(&f, "UPDATE sbtest1 SET k = 2 WHERE id = 2")
            .unwrap()
            .unwrap();
        let tx = a.txn.statement().unwrap();
        e.run_in(TENANT.0, &p, &[], false, tx).await.unwrap();
        // No `statement_done`: the request never got that far.
    }
    a.txn.abandon_statement();
    assert!(a.txn.is_open());
    assert_eq!(a.k(&e, &f, 2).await, Some(20));
    assert_eq!(a.k(&e, &f, 1).await, Some(1));
    // Characteristics can't change inside a transaction.
    let err = a
        .txn
        .set_next(Some(Level::ReadCommitted), None)
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1568);
    a.txn.commit().await.unwrap();
    // The next transaction takes them, once.
    a.txn
        .set_next(Some(Level::ReadCommitted), Some(true))
        .unwrap();
    a.begin(&e);
    assert_eq!(a.txn.isolation(), Some(Isolation::ReadCommitted));
    assert!(a.txn.read_only());
    // `AND CHAIN` keeps them for the transaction that follows.
    let ended = a.txn.commit().await.unwrap().unwrap();
    a.txn.chain(&e, Some(ended), a.opts, false).unwrap();
    assert_eq!(a.txn.isolation(), Some(Isolation::ReadCommitted));
    assert!(a.txn.read_only());
    a.txn.rollback();
    a.begin(&e);
    assert_eq!(a.txn.isolation(), Some(Isolation::RepeatableRead));
    assert!(!a.txn.read_only());
    // `START TRANSACTION READ ONLY` beats the session's access mode.
    a.txn.rollback();
    a.txn
        .begin(
            &e,
            a.opts,
            false,
            Start {
                read_only: Some(true),
                snapshot: true,
            },
        )
        .unwrap();
    assert!(a.txn.read_only());
}

use super::locking::{self, Form};
use sprig_engine::txn::Wait;

#[test]
fn locking_clauses_are_split_off_a_select() {
    let sp =
        |t: &'static str| locking::split(t).map(|(rest, c)| (rest, c.form, c.wait, c.wait_n, c.of));
    assert_eq!(
        sp("SELECT c FROM t WHERE id = 1 FOR UPDATE"),
        Some((
            "SELECT c FROM t WHERE id = 1 ",
            Form::ForUpdate,
            Wait::Block,
            false,
            vec![]
        ))
    );
    assert_eq!(
        sp("select c from t where id=1 lock in share mode;"),
        Some((
            "select c from t where id=1 ",
            Form::ShareMode,
            Wait::Block,
            false,
            vec![]
        ))
    );
    assert_eq!(
        sp("SELECT c FROM t FOR SHARE NOWAIT"),
        Some((
            "SELECT c FROM t ",
            Form::ForShare,
            Wait::NoWait,
            false,
            vec![]
        ))
    );
    assert_eq!(
        sp("SELECT c FROM t FOR UPDATE SKIP LOCKED"),
        Some((
            "SELECT c FROM t ",
            Form::ForUpdate,
            Wait::SkipLocked,
            false,
            vec![]
        ))
    );
    assert_eq!(
        sp("SELECT c FROM t WHERE id IN (1, 2) FOR UPDATE WAIT 3"),
        Some((
            "SELECT c FROM t WHERE id IN (1, 2) ",
            Form::ForUpdate,
            Wait::Seconds(3),
            true,
            vec![]
        ))
    );
    assert_eq!(
        sp("SELECT c FROM t AS x WHERE id = ? FOR UPDATE OF x SKIP LOCKED"),
        Some((
            "SELECT c FROM t AS x WHERE id = ? ",
            Form::ForUpdate,
            Wait::SkipLocked,
            false,
            vec!["x".to_owned()]
        ))
    );
    // Not a locking clause: other statements, the words inside a literal
    // or a subquery, a clause that isn't last.
    assert_eq!(sp("UPDATE t SET c = 'FOR UPDATE'"), None);
    assert_eq!(sp("SELECT 'x FOR UPDATE'"), None);
    assert_eq!(sp("SELECT c FROM t WHERE id = 1"), None);
    assert_eq!(sp("SELECT c FROM t WHERE c = 'for update'"), None);
    assert_eq!(
        sp("SELECT c FROM t WHERE id IN (SELECT id FROM u FOR UPDATE)"),
        None
    );
    assert_eq!(sp("SELECT c FROM t FOR UPDATE LIMIT 1"), None);
    assert_eq!(sp("FOR UPDATE"), None);
    // Where each part starts, for a port's syntax error.
    let t = "SELECT c FROM t FOR SHARE OF t NOWAIT";
    let (_, c) = locking::split(t).unwrap();
    assert_eq!(&t[c.at.lock_word..], "SHARE OF t NOWAIT");
    assert_eq!(c.at.of.map(|i| &t[i..]), Some("OF t NOWAIT"));
    assert_eq!(c.at.wait.map(|i| &t[i..]), Some("NOWAIT"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // one scene: who holds what, then each wait option
async fn locking_reads_lock_wait_skip_or_refuse() {
    let (e, f, _d) = sysbench(4).await;
    let lock = |sql: &str, exclusive: bool, wait: Wait| {
        let mut p = bind(&f, sql).unwrap().unwrap();
        p.lock = Some(plan::RowLock { exclusive, wait });
        p
    };
    let ids = |o: &Outcome| -> Vec<i64> {
        o.rows
            .iter()
            .map(|r| match r[0] {
                Datum::Int(v) => v,
                _ => panic!("an id"),
            })
            .collect()
    };
    let mut a = Sess::mariadb();
    let mut b = Sess::mariadb();
    let mut m = Sess::mysql();
    a.begin(&e);
    b.begin(&e);
    m.begin(&e);
    // a holds row 2 exclusively.
    let p = lock("SELECT id FROM sbtest1 WHERE id = 2", true, Wait::Block);
    let o = e
        .run_in(TENANT.0, &p, &[], false, a.txn.statement().unwrap())
        .await
        .unwrap();
    a.txn.statement_done();
    assert_eq!(ids(&o), [2]);
    // A plain read doesn't care.
    assert_eq!(b.k(&e, &f, 2).await, Some(20));
    // SKIP LOCKED leaves the row out; NOWAIT fails at once with each
    // port's error; WAIT 1 and the session's timeout give 1205.
    let all = "SELECT id FROM sbtest1 WHERE id BETWEEN 1 AND 4";
    let p = lock(all, true, Wait::SkipLocked);
    let mut d = Sess::mariadb();
    d.begin(&e);
    let o = e
        .run_in(TENANT.0, &p, &[], false, d.txn.statement().unwrap())
        .await
        .unwrap();
    d.txn.statement_done();
    assert_eq!(ids(&o), [1, 3, 4]);
    d.txn.rollback();
    let one = "SELECT id FROM sbtest1 WHERE id = 2";
    for (mysql, wait, want) in [
        (false, Wait::NoWait, 1205),
        (true, Wait::NoWait, 3572),
        (false, Wait::Seconds(1), 1205),
        (false, Wait::Block, 1205),
    ] {
        let sess = if mysql { &mut m } else { &mut b };
        let p = lock(one, false, wait);
        let r = e
            .run_in(TENANT.0, &p, &[], false, sess.txn.statement().unwrap())
            .await;
        sess.txn.statement_done();
        assert_eq!(code(r), want, "{wait:?}");
        assert!(sess.txn.is_open(), "the statement alone fails");
    }
    // Shared locks go together; an exclusive one waits for them.
    let mut c = Sess::mariadb();
    c.begin(&e);
    for sess in [&mut b, &mut c] {
        let p = lock("SELECT id FROM sbtest1 WHERE id = 4", false, Wait::Block);
        let o = e
            .run_in(TENANT.0, &p, &[], false, sess.txn.statement().unwrap())
            .await
            .unwrap();
        sess.txn.statement_done();
        assert_eq!(ids(&o), [4]);
    }
    let r = a.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 4").await;
    assert_eq!(code(r), 1205);
    // A locking read sees the latest committed row, not the snapshot, and
    // (with LIMIT) stops at the rows it returns.
    a.txn.rollback();
    b.txn.rollback();
    c.txn.rollback();
    m.txn.rollback();
    m.begin(&e);
    assert_eq!(m.k(&e, &f, 1).await, Some(10));
    a.run(&e, &f, "UPDATE sbtest1 SET k = 77 WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(m.k(&e, &f, 1).await, Some(10));
    let mut p = lock(
        "SELECT k FROM sbtest1 WHERE id BETWEEN 1 AND 3 LIMIT 2",
        true,
        Wait::Block,
    );
    let o = e
        .run_in(TENANT.0, &p, &[], false, m.txn.statement().unwrap())
        .await
        .unwrap();
    m.txn.statement_done();
    assert_eq!(ids(&o), [77, 20]);
    // Row 3 was past the limit: not locked.
    a.run(&e, &f, "UPDATE sbtest1 SET k = 1 WHERE id = 3")
        .await
        .unwrap();
    // Autocommit: the lock ends with the statement.
    p.limit = None;
    let o = e.run(TENANT.0, &p, &[], false, a.opts).await;
    assert_eq!(code(o), 1205, "m still holds rows 1 and 2");
    m.txn.rollback();
    let o = e.run(TENANT.0, &p, &[], false, a.opts).await.unwrap();
    assert_eq!(ids(&o), [77, 20, 1]);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 2 WHERE id = 2")
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_reads_see_own_writes_and_serializable_reads_lock() {
    let (e, f, _d) = sysbench(3).await;
    let mut a = Sess::mysql();
    let mut b = Sess::mysql();
    a.begin(&e);
    // The transaction's own insert, update and delete, in a range it then
    // changes: found without opening its read view.
    a.run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (10, 100)")
        .await
        .unwrap();
    a.run(&e, &f, "DELETE FROM sbtest1 WHERE id = 2")
        .await
        .unwrap();
    b.run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (11, 110)")
        .await
        .unwrap();
    let o = a
        .run(
            &e,
            &f,
            "UPDATE sbtest1 SET k = k + 1 WHERE id BETWEEN 1 AND 20",
        )
        .await
        .unwrap();
    assert_eq!(o.affected, 4, "rows 1, 3, 10 and b's 11");
    assert_eq!(a.k(&e, &f, 10).await, Some(101));
    assert_eq!(a.k(&e, &f, 11).await, Some(111));
    assert_eq!(a.k(&e, &f, 2).await, None);
    a.txn.commit().await.unwrap();
    assert_eq!(b.k(&e, &f, 11).await, Some(111));
    // SERIALIZABLE: a plain read inside a transaction holds a shared lock.
    let mut s1 = Sess::mysql().level(Isolation::Serializable);
    s1.begin(&e);
    assert_eq!(s1.k(&e, &f, 1).await, Some(11));
    let r = b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 1").await;
    assert_eq!(code(r), 1205);
    // Outside a transaction it is a consistent read: no lock.
    let mut s2 = Sess::mysql().level(Isolation::Serializable);
    assert_eq!(s2.k(&e, &f, 3).await, Some(31));
    s1.txn.commit().await.unwrap();
    b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 1")
        .await
        .unwrap();
}

/// `plan` with `cond` as its row-level security condition on new rows.
fn with_check(mut p: Plan, cond: &str) -> Plan {
    let e = Parser::new(&MySqlDialect {})
        .try_with_sql(cond)
        .unwrap()
        .parse_expr()
        .unwrap();
    let mut c = super::expr::Compiler {
        def: &p.table,
        quals: vec!["owned".into()],
        params: 0,
        read: vec![false; p.table.columns.len()],
        backslash: true,
        hints: Vec::new(),
    };
    let compiled = c.compile(&e).unwrap();
    p.check = Some(compiled);
    p
}

#[tokio::test]
async fn new_rows_must_meet_the_row_security_check() {
    let (e, _d) = mem();
    let f = Fake::new();
    let ins = |sql: &str| with_check(bind(&f, sql).unwrap().unwrap(), "`owned`.`owner` = 'me'");
    let o = exec(&e, &ins("INSERT INTO owned VALUES (1, 'me', 1)"), &[])
        .await
        .unwrap();
    assert_eq!(o.affected, 1);
    let err = exec(&e, &ins("INSERT INTO owned VALUES (2, 'you', 2)"), &[])
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(sprig_common::SprigCode::RlsCheckFailed),
        "{err}"
    );
    assert!(
        err.message
            .contains("new row violates row-level security policy for table 'owned'")
    );
    // One bad row fails the whole statement.
    let r = exec(
        &e,
        &ins("INSERT INTO owned VALUES (3, 'me', 3), (4, 'you', 4)"),
        &[],
    )
    .await;
    assert!(r.is_err());
    let all = run(&e, &f, "SELECT id FROM owned", &[]).await.unwrap();
    assert_eq!(all.rows, vec![vec![Datum::UInt(1)]]);
    // An UPDATE's new version is checked, whatever it changes.
    let r = exec(&e, &ins("UPDATE owned SET owner = 'you' WHERE id = 1"), &[]).await;
    assert_eq!(
        r.unwrap_err().code(),
        Some(sprig_common::SprigCode::RlsCheckFailed)
    );
    let o = exec(&e, &ins("UPDATE owned SET v = 5 WHERE id = 1"), &[])
        .await
        .unwrap();
    assert_eq!(o.affected, 1);
    // A condition that is NULL fails, as in MariaDB's triggers.
    let null = with_check(
        bind(&f, "UPDATE owned SET v = NULL WHERE id = 1")
            .unwrap()
            .unwrap(),
        "v > 0",
    );
    assert!(exec(&e, &null, &[]).await.is_err());
    // Inside a transaction the failed statement alone is undone.
    let mut a = Sess::mariadb();
    a.begin(&e);
    a.run(&e, &f, "INSERT INTO owned VALUES (7, 'x', 7)")
        .await
        .unwrap();
    let tx = a.txn.statement().unwrap();
    let r = e
        .run_in(
            TENANT.0,
            &ins("INSERT INTO owned VALUES (8, 'you', 8)"),
            &[],
            false,
            tx,
        )
        .await;
    a.txn.statement_done();
    assert!(r.is_err() && a.txn.is_open());
    a.txn.commit().await.unwrap();
    let all = run(&e, &f, "SELECT id FROM owned", &[]).await.unwrap();
    assert_eq!(all.rows, vec![vec![Datum::UInt(1)], vec![Datum::UInt(7)]]);
}

// ----- lock rules found by the two-server differential ------------------------

/// Runs a locking `SELECT` (`FOR UPDATE`) in `s`'s open transaction.
async fn for_update(s: &mut Sess, e: &Engine, f: &Fake, sql: &str) -> Outcome {
    let mut p = bind(f, sql).unwrap().unwrap();
    p.lock = Some(plan::RowLock {
        exclusive: true,
        wait: Wait::Block,
    });
    let o = e
        .run_in(TENANT.0, &p, &[], false, s.txn.statement().unwrap())
        .await
        .unwrap();
    s.txn.statement_done();
    o
}

/// Whether another session can change `owned` row `id` right now (`false`:
/// it waits for a lock and times out).
async fn owned_free(e: &Engine, f: &Fake, id: u64) -> bool {
    let mut w = Sess::mariadb().level(Isolation::ReadCommitted);
    let r = w
        .run(
            e,
            f,
            &format!("UPDATE owned SET owner = owner WHERE id = {id}"),
        )
        .await;
    match r {
        Ok(_) => true,
        Err(err) => {
            assert_eq!(err.protocol().mysql_errno, 1205, "{err:?}");
            false
        }
    }
}

#[tokio::test]
async fn read_committed_keeps_an_unmatched_row_where_the_servers_do() {
    let (e, d) = mem();
    let f = Fake::new();
    run(
        &e,
        &f,
        "INSERT INTO owned VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)",
        &[],
    )
    .await
    .unwrap();
    // (statement, kept on MariaDB, kept on MySQL): every one finds row 2
    // and its `owner = 'zz'` refuses it.
    let cases = [
        // By the whole primary key: a locking SELECT keeps the row on both;
        // UPDATE and DELETE give it back on both.
        (
            "S:SELECT id FROM owned WHERE id = 2 AND owner = 'zz'",
            true,
            true,
        ),
        (
            "UPDATE owned SET owner = 'q' WHERE id = 2 AND owner = 'zz'",
            false,
            false,
        ),
        (
            "DELETE FROM owned WHERE id = 2 AND owner = 'zz'",
            false,
            false,
        ),
        // A primary-key range: given back on both.
        (
            "S:SELECT id FROM owned WHERE id BETWEEN 2 AND 2 AND owner = 'zz'",
            false,
            false,
        ),
        (
            "UPDATE owned SET owner = 'q' WHERE id BETWEEN 2 AND 2 AND owner = 'zz'",
            false,
            false,
        ),
        // Through the unique index: MariaDB keeps it whatever the statement;
        // MySQL keeps it for the locking SELECT only.
        (
            "S:SELECT id FROM owned WHERE v = 20 AND owner = 'zz'",
            true,
            true,
        ),
        (
            "UPDATE owned SET owner = 'q' WHERE v = 20 AND owner = 'zz'",
            true,
            false,
        ),
        (
            "DELETE FROM owned WHERE v = 20 AND owner = 'zz'",
            true,
            false,
        ),
    ];
    for (sql, maria, mysql) in cases {
        for (is_mysql, kept) in [(false, maria), (true, mysql)] {
            let mut a = if is_mysql {
                Sess::mysql()
            } else {
                Sess::mariadb()
            }
            .level(Isolation::ReadCommitted);
            a.begin(&e);
            if let Some(q) = sql.strip_prefix("S:") {
                assert!(for_update(&mut a, &e, &f, q).await.rows.is_empty());
            } else {
                assert_eq!(a.run(&e, &f, sql).await.unwrap().affected, 0);
            }
            assert_eq!(
                !owned_free(&e, &f, 2).await,
                kept,
                "{sql} (mysql: {is_mysql})"
            );
            // A neighbour the statement never matched is free either way.
            assert!(owned_free(&e, &f, 3).await);
            a.txn.rollback();
        }
    }
    // At REPEATABLE READ every row read stays locked, as before.
    let mut a = Sess::mysql();
    a.begin(&e);
    a.run(
        &e,
        &f,
        "UPDATE owned SET owner = 'q' WHERE id = 2 AND owner = 'zz'",
    )
    .await
    .unwrap();
    assert!(!owned_free(&e, &f, 2).await);
    a.txn.rollback();
    drop(d);
}

#[tokio::test]
async fn a_read_committed_delete_waits_for_a_locked_row_and_an_update_may_skip_it() {
    let (e, f, _d) = sysbench(3).await;
    // Another transaction holds row 1; neither statement below matches it.
    let mut holder = Sess::mysql();
    holder.begin(&e);
    holder
        .run(&e, &f, "UPDATE sbtest1 SET c = 'held' WHERE id = 1")
        .await
        .unwrap();
    let mut m = Sess::mysql().level(Isolation::ReadCommitted);
    m.begin(&e);
    // MySQL's UPDATE judges the locked row on its committed version and
    // skips it (a semi-consistent read)...
    let o = m
        .run(&e, &f, "UPDATE sbtest1 SET c = 'x' WHERE k = 30")
        .await
        .unwrap();
    assert_eq!(o.affected, 1);
    // ... its DELETE waits for the row, as both servers' DELETE does.
    let err = m
        .run(&e, &f, "DELETE FROM sbtest1 WHERE k = 999")
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1205);
    assert!(m.txn.is_open());
    m.txn.rollback();
    holder.txn.rollback();
}

#[tokio::test]
async fn serializable_opens_its_view_at_the_first_statement_on_mariadb() {
    let (e, f, _d) = sysbench(3).await;
    let e = Arc::new(e);
    let mut w = Sess::mariadb();
    // BEGIN alone opens no view: a commit before the first statement is no
    // conflict.
    let mut a = Sess::mariadb().level(Isolation::Serializable);
    a.begin(&e);
    w.run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 3")
        .await
        .unwrap();
    a.run(&e, &f, "UPDATE sbtest1 SET k = k + 100 WHERE id = 3")
        .await
        .unwrap();
    // From the first statement on, a row someone else changed and committed
    // is a snapshot conflict (1020), also when that statement was a write.
    w.run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 2")
        .await
        .unwrap();
    let err = a
        .run(&e, &f, "UPDATE sbtest1 SET k = k + 100 WHERE id = 2")
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1020);
    assert!(!a.txn.is_open());
    assert_eq!(
        w.k(&e, &f, 3).await,
        Some(31),
        "the transaction's update went too"
    );
    // An autocommit statement at SERIALIZABLE that waited for a row which
    // was then changed: 1020 as well (it is its own one-statement
    // transaction with a view from its start).
    let mut holder = Sess::mariadb();
    holder.begin(&e);
    holder
        .run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 1")
        .await
        .unwrap();
    let e2 = Arc::clone(&e);
    let waiter = tokio::spawn(async move {
        let f = Fake::new();
        let mut s = Sess::mariadb().level(Isolation::Serializable);
        s.opts = s.opts.with_lock_wait_seconds(20);
        s.run(&e2, &f, "UPDATE sbtest1 SET k = k + 100 WHERE id = 1")
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    holder.txn.commit().await.unwrap();
    let err = waiter.await.unwrap().unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1020);
    assert_eq!(w.k(&e, &f, 1).await, Some(11));
    // MySQL has no such check at any level.
    let mut m = Sess::mysql().level(Isolation::Serializable);
    m.begin(&e);
    m.run(&e, &f, "UPDATE sbtest1 SET k = k WHERE id = 3")
        .await
        .unwrap();
    w.run(&e, &f, "UPDATE sbtest1 SET k = 7 WHERE id = 2")
        .await
        .unwrap();
    m.run(&e, &f, "UPDATE sbtest1 SET k = k + 1 WHERE id = 2")
        .await
        .unwrap();
    m.txn.commit().await.unwrap();
    assert_eq!(w.k(&e, &f, 2).await, Some(8));
}

#[tokio::test]
async fn an_undone_insert_frees_its_key_on_mariadb_and_closes_its_place_on_mysql() {
    let (e, f, _d) = sysbench(3).await;
    for (is_mysql, level, free) in [
        (false, Isolation::RepeatableRead, true),
        (false, Isolation::ReadCommitted, true),
        (true, Isolation::RepeatableRead, false),
        (true, Isolation::ReadCommitted, true),
    ] {
        let mut a = if is_mysql {
            Sess::mysql()
        } else {
            Sess::mariadb()
        }
        .level(level);
        a.begin(&e);
        // Row 50 goes in, row 1 is a duplicate: the statement fails and
        // row 50 is undone.
        let r = a
            .run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (50, 1), (1, 1)")
            .await;
        assert_eq!(code(r), 1062);
        assert!(a.txn.is_open());
        let mut b = Sess::mariadb();
        let r = b
            .run(&e, &f, "INSERT INTO sbtest1(id, k) VALUES (50, 2)")
            .await;
        assert_eq!(r.is_ok(), free, "mysql: {is_mysql}, {level:?}");
        if !free {
            assert_eq!(code(r), 1205);
        }
        // The duplicate's lock stays on both.
        let r = b.run(&e, &f, "UPDATE sbtest1 SET k = k WHERE id = 1").await;
        assert_eq!(code(r), 1205);
        a.txn.rollback();
        b.run(&e, &f, "DELETE FROM sbtest1 WHERE id = 50")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_write_that_read_the_whole_table_says_so() {
    let (e, f, _d) = sysbench(3).await;
    let o = run(&e, &f, "UPDATE sbtest1 SET c = 'x' WHERE k = 20", &[])
        .await
        .unwrap();
    assert!(o.no_index && o.affected == 1);
    let o = run(&e, &f, "DELETE FROM sbtest1 WHERE k = 999", &[])
        .await
        .unwrap();
    assert!(o.no_index && o.affected == 0);
    let o = run(&e, &f, "UPDATE sbtest1 SET c = 'y' WHERE id = 2", &[])
        .await
        .unwrap();
    assert!(!o.no_index);
}

/// Column types: definitions, storage, assignment rules, keys.
mod ddl;
mod fk;
mod hidden;
mod types;

#[tokio::test]
async fn an_xa_branch_is_prepared_listed_and_finished_by_its_xid() {
    use super::session::{self, XaPhase};
    let (e, f, _d) = sysbench(3).await;
    let xid = |g: &str| control::Xid {
        gtrid: g.as_bytes().to_vec(),
        bqual: Vec::new(),
        format: 1,
    };
    let tenant = TENANT.0;
    // MariaDB's port: the prepared branch stays with its session.
    let x1 = xid("unit-xa-1");
    let k1 = x1.engine_key(tenant);
    let mut a = Sess::mariadb();
    assert!(!session::xa_taken(&e, &k1));
    a.txn
        .xa_start(&e, a.opts, false, x1.clone(), k1.clone())
        .unwrap();
    assert_eq!(a.txn.xa_phase(), Some(XaPhase::Active));
    assert!(a.txn.xa_is(&x1) && a.txn.reported());
    // The xid is taken for every other session from now on.
    assert!(session::xa_taken(&e, &k1));
    let mut b = Sess::mariadb();
    let err = b
        .txn
        .xa_start(&e, b.opts, false, x1.clone(), k1.clone())
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1440);
    a.run(&e, &f, "UPDATE sbtest1 SET k = 111 WHERE id = 1")
        .await
        .unwrap();
    a.txn.xa_end();
    assert_eq!(a.txn.xa_phase(), Some(XaPhase::Idle));
    a.txn.xa_prepare(true).await.unwrap();
    assert_eq!(a.txn.xa_phase(), Some(XaPhase::Prepared));
    assert!(!a.txn.is_open() && a.txn.reported());
    assert_eq!(session::xa_recover(&e, tenant), vec![x1.clone()]);
    assert!(
        session::xa_recover(&e, tenant + 1).is_empty(),
        "another tenant sees none"
    );
    // Its row is locked and its change invisible; another session can't
    // finish a branch its session still holds.
    assert_eq!(b.k(&e, &f, 1).await, Some(10));
    let r = b.run(&e, &f, "UPDATE sbtest1 SET k = 0 WHERE id = 1").await;
    assert_eq!(code(r), 1205);
    let err = session::xa_finish(&e, &k1, true).await.unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1397);
    a.txn.xa_finish_own(&e, true).await.unwrap();
    assert_eq!(a.txn.xa_phase(), None);
    assert!(!session::xa_taken(&e, &k1));
    assert_eq!(b.k(&e, &f, 1).await, Some(111));
    // MySQL's port: after the prepare the session is free and any session
    // finishes the branch by its xid.
    let x2 = xid("unit-xa-2");
    let k2 = x2.engine_key(tenant);
    let mut m = Sess::mysql();
    m.txn
        .xa_start(&e, m.opts, false, x2.clone(), k2.clone())
        .unwrap();
    m.run(&e, &f, "UPDATE sbtest1 SET k = 222 WHERE id = 2")
        .await
        .unwrap();
    m.txn.xa_end();
    m.txn.xa_prepare(false).await.unwrap();
    assert_eq!(m.txn.xa_phase(), None);
    assert!(!m.txn.reported());
    assert_eq!(session::xa_recover(&e, tenant), vec![x2.clone()]);
    session::xa_finish(&e, &k2, false).await.unwrap();
    assert_eq!(b.k(&e, &f, 2).await, Some(20));
    let err = session::xa_finish(&e, &k2, false).await.unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1397);
    // A deadlock (here: the engine ending the transaction) leaves the
    // branch with nothing but XA ROLLBACK; a session that leaves with an
    // active branch frees its xid.
    let x3 = xid("unit-xa-3");
    let k3 = x3.engine_key(tenant);
    let mut c = Sess::mariadb();
    c.txn.xa_start(&e, c.opts, false, x3, k3.clone()).unwrap();
    c.txn.rollback();
    c.txn.xa_leave();
    assert!(!session::xa_taken(&e, &k3));
    assert!(session::xa_recover(&e, tenant).is_empty());
}

#[tokio::test]
async fn null_bounds_and_descending_order_through_an_index() {
    let (e, mut f, _d) = sysbench(40).await;
    // `x NOT BETWEEN NULL AND h` is true where `x > h` (the other half is
    // false whatever the NULL bound is), and NULL elsewhere.
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 WHERE id NOT BETWEEN NULL AND 37",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        o.rows,
        vec![
            vec![Datum::Int(38)],
            vec![Datum::Int(39)],
            vec![Datum::Int(40)]
        ]
    );
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 WHERE k NOT BETWEEN 15 AND NULL",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(o.rows, vec![vec![Datum::Int(1)]]);
    assert!(
        run(
            &e,
            &f,
            "SELECT id FROM sbtest1 WHERE id BETWEEN NULL AND 37",
            &[]
        )
        .await
        .unwrap()
        .rows
        .is_empty()
    );
    // Equal index values come back in primary-key order, descending when
    // asked.
    f.tables.insert(
        "sbtest1".into(),
        Arc::new(table::parse(ObjectId(10), 2, DB, WITH_K)),
    );
    let p = bind(
        &f,
        "SELECT id FROM sbtest1 WHERE k = 7 ORDER BY id DESC LIMIT 3",
    )
    .unwrap()
    .unwrap();
    assert!(matches!(p.access, Access::IndexEq { .. }) && p.reverse);
    run(
        &e,
        &f,
        "UPDATE sbtest1 SET k = 7 WHERE id IN (3, 9, 21, 30)",
        &[],
    )
    .await
    .unwrap();
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 WHERE k = 7 ORDER BY id DESC LIMIT 3",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        o.rows,
        vec![
            vec![Datum::Int(30)],
            vec![Datum::Int(21)],
            vec![Datum::Int(9)]
        ]
    );
    let o = run(
        &e,
        &f,
        "SELECT id FROM sbtest1 WHERE k = 7 ORDER BY id LIMIT 3",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        o.rows,
        vec![
            vec![Datum::Int(3)],
            vec![Datum::Int(9)],
            vec![Datum::Int(21)]
        ]
    );
}
