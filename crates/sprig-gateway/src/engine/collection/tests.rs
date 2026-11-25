//! MongoDB collections read from SQL: documents written through the
//! document store, read through the planner's path on the same engine,
//! and compared with the same JSON held in a table.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bson::{Bson, doc};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_doc::docstore::DocStore;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{EngineConfig, StoreKind};
use sqlparser::parser::Parser;

use super::{CollTable, find, id_candidates, id_text};
use crate::engine::assign::Env;
use crate::engine::general::{General, RunArgs, Seed, bind, results};
use crate::engine::meta::Results;
use crate::engine::plan::{self, BindCtx, Names, RowLock};
use crate::engine::table::{self, Parsed};
use crate::engine::{Engine, NOT_ON_ENGINE};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-gw-coll-{}-{}",
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

const TENANT: TenantId = TenantId(61_306_000_560);
const DB: DbId = DbId(1);

struct Fake {
    eng: sprig_engine::Engine,
    tables: HashMap<String, Arc<Parsed>>,
}

impl Names for Fake {
    fn engine_db(&self, db: DbId) -> bool {
        db == DB
    }
    fn table(&self, _db: DbId, name: &str) -> Option<Arc<Parsed>> {
        self.tables.get(&name.to_lowercase()).cloned()
    }
    fn other_relation(&self, _db: DbId, _name: &str) -> bool {
        false
    }
    fn db_name(&self, _db: DbId) -> String {
        "shop".into()
    }
    fn db_by_name(&self, name: &str) -> Option<DbId> {
        (name == "shop").then_some(DB)
    }
    fn version(&self) -> u64 {
        1
    }
    fn collection(&self, db: DbId, name: &str) -> Option<Arc<CollTable>> {
        find(&self.eng, TENANT.0, db, "shop", name, false)
    }
}

fn cx(f: &Fake) -> BindCtx<'_> {
    BindCtx {
        tenant: TENANT,
        names: f,
        current: Some(DB),
        backslash: true,
    }
}

fn parse(sql: &str) -> sqlparser::ast::Statement {
    Parser::parse_sql(&sprig_plan::dialect::SprigMySql, sql)
        .unwrap()
        .remove(0)
}

const TAIL: &str = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

async fn setup() -> (Engine, Fake, TempDir) {
    let d = TempDir::new();
    let mut cfg = EngineConfig::new(&d.0);
    cfg.store = StoreKind::Mem;
    cfg.log.sync = SyncMode::Off;
    cfg.checkpoint_interval = Duration::ZERO;
    let raw = sprig_engine::Engine::open(cfg).unwrap();
    let e = Engine::new(raw.clone());
    let defs = [
        (
            "customers",
            "CREATE TABLE `customers` (\n  `id` int(11) NOT NULL,\n  `name` varchar(40) NOT NULL,\n  PRIMARY KEY (`id`)",
        ),
        (
            "refs",
            "CREATE TABLE `refs` (\n  `id` int(11) NOT NULL,\n  `ev` varchar(40) NOT NULL,\n  PRIMARY KEY (`id`)",
        ),
        (
            "ev_json",
            "CREATE TABLE `ev_json` (\n  `_id` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin NOT NULL,\n  `doc` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`doc`)),\n  PRIMARY KEY (`_id`)",
        ),
    ];
    let mut tables = HashMap::new();
    for (i, (name, body)) in defs.iter().enumerate() {
        tables.insert(
            (*name).to_owned(),
            Arc::new(table::parse(
                ObjectId(20 + i as u64),
                1,
                DB,
                &format!("{body}\n{TAIL}"),
            )),
        );
    }
    let f = Fake {
        eng: raw.clone(),
        tables,
    };
    // The documents, through the document store on the same engine.
    let store = DocStore::new(Arc::new(raw));
    let docs = vec![
        doc! {"_id": 1, "customer_id": 3, "kind": "view", "n": 5i64},
        doc! {"_id": bson::oid::ObjectId::parse_str("650000000000000000000001").unwrap(),
        "customer_id": 4, "kind": "buy", "amount": 12.5},
        doc! {"_id": "e3", "customer_id": 3, "kind": "buy", "tags": ["a", "b"],
        "at": bson::DateTime::from_millis(1_700_000_000_000)},
        doc! {"_id": 2.5, "customer_id": 99},
        doc! {"_id": {"a": 1}, "customer_id": 4, "kind": "view"},
        doc! {"_id": 7i64, "customer_id": 3i64, "kind": "buy", "n": -2},
    ];
    store
        .command(
            TENANT.0,
            "app",
            "shop",
            "insert",
            &doc! {"insert": "events", "documents": docs.into_iter().map(Bson::Document).collect::<Vec<_>>()},
        )
        .await
        .unwrap();
    for sql in [
        "INSERT INTO customers (id, name) VALUES (3, 'Asha'), (4, 'Ben'), (5, 'Chen')",
        "INSERT INTO refs (id, ev) VALUES (1, 'e3'), (2, '1'), (3, 'nope'), (4, '650000000000000000000001')",
    ] {
        let p = plan::bind(&cx(&f), &parse(sql)).unwrap().unwrap();
        e.run(TENANT.0, &p, &[], false, TxnOptions::default())
            .await
            .unwrap();
    }
    (e, f, d)
}

fn general(f: &Fake, sql: &str, lock: Option<RowLock>) -> sprig_common::Result<General> {
    Ok(bind(&cx(f), &parse(sql), Some(sql), lock)?.expect("a general statement"))
}

async fn run(e: &Engine, g: &General) -> sprig_common::Result<Vec<String>> {
    let s = Seed::of(false);
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: None,
        stop: None,
    };
    let rows = e
        .general_run(g, &a, TxnOptions::default(), Env::STRICT)
        .await?;
    let r = Results {
        cs: None,
        mysql: false,
    };
    let l = results::layout(g, &rows.columns, r);
    Ok(rows
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(&l.shapes)
                .map(|(v, s)| {
                    results::text(g, s, v, &Env::STRICT, r).map_or_else(
                        || "NULL".to_owned(),
                        |b| String::from_utf8_lossy(&b).into_owned(),
                    )
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect())
}

async fn select(e: &Engine, f: &Fake, sql: &str) -> Vec<String> {
    let g = general(f, sql, None).unwrap_or_else(|x| panic!("{sql}: {x}"));
    run(e, &g).await.unwrap_or_else(|x| panic!("{sql}: {x}"))
}

#[test]
fn the_table_parses() {
    let d = super::table_def_or(DB, "events", 3).unwrap();
    assert_eq!(d.columns.len(), 3);
    assert!(d.columns[2].invisible);
    assert_eq!(d.object, ObjectId(0x4D00_0003));
}

#[tokio::test]
async fn the_collection_is_found() {
    let (_e, f, _d) = setup().await;
    assert!(f.collection(DB, "events").is_some());
    assert!(f.collection(DB, "nothing").is_none());
}

#[test]
fn id_text_is_what_the_arrow_operator_gives() {
    assert_eq!(id_text(&Bson::String("e3".into())).as_deref(), Some("e3"));
    assert_eq!(id_text(&Bson::Int32(1)).as_deref(), Some("1"));
    assert_eq!(id_text(&Bson::Double(2.5)).as_deref(), Some("2.5"));
    assert_eq!(id_text(&Bson::Double(3.0)).as_deref(), Some("3.0"));
    assert_eq!(
        id_text(&Bson::Document(doc! {"a": 1})).as_deref(),
        Some("{\"a\":1}")
    );
    assert_eq!(id_text(&Bson::Null).as_deref(), Some("null"));
    let o = bson::oid::ObjectId::parse_str("650000000000000000000001").unwrap();
    assert_eq!(
        id_text(&Bson::ObjectId(o)).as_deref(),
        Some("650000000000000000000001")
    );
    // Every value prints back to a candidate key of its own text, or to a
    // class read whole.
    for v in [
        Bson::String("x y".into()),
        Bson::Int64(42),
        Bson::Double(0.1),
        Bson::Double(3.0),
        Bson::Decimal128("1.50".parse().unwrap()),
        Bson::ObjectId(o),
        Bson::Boolean(true),
        Bson::Null,
        Bson::MinKey,
        Bson::DateTime(bson::DateTime::from_millis(1_700_000_000_000)),
        Bson::DateTime(bson::DateTime::from_millis(-99_999_999_999_999)),
        Bson::Timestamp(bson::Timestamp {
            time: 5,
            increment: 6,
        }),
        Bson::Document(doc! {"k": "v"}),
        Bson::JavaScriptCode("f()".into()),
        Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: vec![1, 2, 3],
        }),
        Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Uuid,
            bytes: (0u8..16).collect(),
        }),
    ] {
        let text = id_text(&v).unwrap();
        let (keys, classes) = id_candidates(&text);
        let k = sprig_doc::keyenc::key(&v);
        assert!(
            keys.contains(&k) || classes.contains(&k[0]),
            "{v:?} prints as {text:?}, which finds neither its key nor its class"
        );
    }
}

#[tokio::test]
async fn a_collection_reads_as_a_table() {
    let (e, f, _d) = setup().await;
    // `*`: `_id` and `doc`; `_types` is invisible.
    let all = select(&e, &f, "SELECT * FROM events ORDER BY _id").await;
    assert_eq!(all.len(), 6, "{all:?}");
    assert_eq!(
        all[0],
        "1|{\"_id\":1,\"customer_id\":3,\"kind\":\"view\",\"n\":5}"
    );
    let ids = select(&e, &f, "SELECT _id FROM events ORDER BY _id").await;
    assert_eq!(
        ids,
        [
            "1",
            "2.5",
            "650000000000000000000001",
            "7",
            "e3",
            "{\"a\":1}"
        ]
    );
    let desc = select(
        &e,
        &f,
        "SELECT _id FROM shop.events WHERE _id > '2' ORDER BY _id DESC LIMIT 2",
    )
    .await;
    assert_eq!(desc, ["{\"a\":1}", "e3"]);
    // Point reads by `_id`, whatever the BSON type behind the text.
    for (id, want) in [
        ("e3", "buy"),
        ("650000000000000000000001", "buy"),
        ("2.5", "NULL"),
        ("{\"a\":1}", "view"),
        ("7", "buy"),
    ] {
        let got = select(
            &e,
            &f,
            &format!("SELECT doc->>'$.kind' FROM events WHERE _id = '{id}'"),
        )
        .await;
        assert_eq!(got, [want], "_id {id}");
    }
    assert!(
        select(&e, &f, "SELECT _id FROM events WHERE _id = '2.50'")
            .await
            .is_empty()
    );
    assert!(
        select(&e, &f, "SELECT _id FROM events WHERE _id = 'E3'")
            .await
            .is_empty()
    );
    // `_types` on request.
    assert_eq!(
        select(
            &e,
            &f,
            "SELECT _types FROM events WHERE _id = '650000000000000000000001'"
        )
        .await,
        ["{\"_id\":\"objectId\"}"]
    );
    // Fields by path, typed.
    assert_eq!(
        select(
            &e,
            &f,
            "SELECT _id, CAST(doc->>'$.n' AS SIGNED) * 2 FROM events WHERE doc->>'$.n' IS NOT NULL ORDER BY 2"
        )
        .await,
        ["7|-4", "1|10"]
    );
    assert_eq!(
        select(
            &e,
            &f,
            "SELECT doc->>'$.kind' AS k, COUNT(*) FROM events GROUP BY k ORDER BY k"
        )
        .await,
        ["NULL|1", "buy|3", "view|2"]
    );
}

#[tokio::test]
async fn a_collection_joins_tables() {
    let (e, f, _d) = setup().await;
    // A field joined to a table's key.
    let got = select(
        &e,
        &f,
        "SELECT c.name, e._id FROM customers c JOIN events e ON e.doc->>'$.customer_id' = c.id ORDER BY c.name, e._id",
    )
    .await;
    assert_eq!(
        got,
        [
            "Asha|1",
            "Asha|7",
            "Asha|e3",
            "Ben|650000000000000000000001",
            "Ben|{\"a\":1}"
        ]
    );
    // A table's column joined to `_id`: the inner side read by key.
    let sql =
        "SELECT r.id, e.doc->>'$.kind' FROM refs r JOIN events e ON e._id = r.ev ORDER BY r.id";
    assert_eq!(select(&e, &f, sql).await, ["1|buy", "2|view", "4|buy"]);
    let plan = select(
        &e,
        &f,
        "EXPLAIN SELECT doc FROM events WHERE _id = 'e3' OR _id = '1'",
    )
    .await;
    assert!(plan.iter().any(|l| l.contains("_id_")), "{plan:?}");
    // Outer join, aggregate over both.
    assert_eq!(
        select(
            &e,
            &f,
            "SELECT c.name, COUNT(e._id) FROM customers c LEFT JOIN events e ON JSON_EXTRACT(e.doc, '$.customer_id') = c.id GROUP BY c.name ORDER BY c.name"
        )
        .await,
        ["Asha|3", "Ben|2", "Chen|0"]
    );
    // A subquery over the collection.
    assert_eq!(
        select(
            &e,
            &f,
            "SELECT name FROM customers WHERE id IN (SELECT doc->>'$.customer_id' FROM events WHERE doc->>'$.kind' = 'buy') ORDER BY name"
        )
        .await,
        ["Asha", "Ben"]
    );
}

#[tokio::test]
async fn a_collection_answers_as_the_same_json_in_a_table() {
    let (e, f, _d) = setup().await;
    // Copy the collection into a table of the same shape.
    let rows = select(&e, &f, "SELECT _id, doc FROM events").await;
    for r in rows {
        let (id, doc) = r.split_once('|').unwrap();
        let sql = format!(
            "INSERT INTO ev_json (_id, doc) VALUES ('{}', '{}')",
            id.replace('\'', "''"),
            doc.replace('\'', "''")
        );
        let p = plan::bind(&cx(&f), &parse(&sql)).unwrap().unwrap();
        e.run(TENANT.0, &p, &[], false, TxnOptions::default())
            .await
            .unwrap();
    }
    for q in [
        "SELECT _id, doc FROM {} ORDER BY _id",
        "SELECT _id FROM {} WHERE _id BETWEEN '2' AND 'f' ORDER BY _id DESC",
        "SELECT doc->'$.tags', JSON_LENGTH(doc), JSON_TYPE(doc->'$.customer_id') FROM {} ORDER BY _id",
        "SELECT doc->>'$.kind', SUM(doc->'$.customer_id') FROM {} GROUP BY 1 ORDER BY 1",
        "SELECT JSON_EXTRACT(doc, '$.at'), JSON_UNQUOTE(doc->'$.amount') FROM {} WHERE JSON_CONTAINS(doc, '\"buy\"', '$.kind') ORDER BY _id",
        "SELECT c.name, t._id FROM customers c JOIN {} t ON t.doc->>'$.customer_id' = c.id ORDER BY 1, 2",
        "SELECT r.id, t._id FROM refs r LEFT JOIN {} t ON t._id = r.ev ORDER BY 1",
    ] {
        let a = select(&e, &f, &q.replace("{}", "events")).await;
        let b = select(&e, &f, &q.replace("{}", "ev_json")).await;
        assert_eq!(a, b, "{q}");
        assert!(!a.is_empty(), "{q}");
    }
}

#[tokio::test]
async fn collections_are_read_only_from_sql() {
    let (e, f, _d) = setup().await;
    for sql in [
        "UPDATE events SET doc = '{}' WHERE _id = 'e3'",
        "DELETE FROM events",
        "INSERT INTO events (_id, doc) VALUES ('x', '{}')",
    ] {
        let err = plan::bind(&cx(&f), &parse(sql)).unwrap_err();
        assert_eq!(err.protocol().mysql_errno, NOT_ON_ENGINE, "{sql}: {err}");
    }
    let lock = RowLock {
        exclusive: true,
        wait: sprig_engine::txn::Wait::Block,
    };
    let g = general(&f, "SELECT _id FROM events", Some(lock)).unwrap();
    let err = run(&e, &g).await.unwrap_err();
    assert_eq!(err.protocol().mysql_errno, NOT_ON_ENGINE, "{err}");
    // An unknown name is still unknown.
    let err = bind(
        &cx(&f),
        &parse("SELECT * FROM nothing JOIN customers"),
        None,
        None,
    )
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1146);
    // A table copies a collection (INSERT ... SELECT reads it unlocked).
    let g = crate::engine::general::bind_view(
        &cx(&f),
        &parse("SELECT _id, doc FROM events WHERE doc->>'$.kind' = 'view'"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(run(&e, &g).await.unwrap().len(), 2);
}
