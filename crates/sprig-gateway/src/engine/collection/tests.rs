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
async fn locking_reads_lock_documents() {
    let (e, f, _d) = setup().await;
    // The single-table binder leaves every collection statement to the
    // planner's path and `collwrite`.
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
    assert_eq!(run(&e, &g).await.unwrap().len(), 6);
    let g = general(&f, "SELECT _id FROM events WHERE _id = 'e3'", Some(lock)).unwrap();
    assert_eq!(run(&e, &g).await.unwrap(), ["e3"]);
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

/// Runs SQL write `sql` of a collection in a transaction of its own, on
/// document store `store`: (affected, info, warnings) or the error.
async fn write(
    e: &Engine,
    f: &Fake,
    store: &DocStore,
    sql: &str,
) -> sprig_common::Result<(u64, Option<String>, usize)> {
    use crate::engine::session::{SessionTxn, Start};
    let w = crate::engine::collwrite::bind(
        &cx(f),
        &parse(sql),
        sql,
        sprig_sql::lex::LexOptions::mariadb(),
    )?
    .unwrap_or_else(|| panic!("{sql}: not a collection write"));
    let s = Seed::of(false);
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: None,
        stop: None,
    };
    let mut own = SessionTxn::default();
    own.begin(e, TxnOptions::default(), false, Start::default())?;
    let r = e.coll_write_with(store, &w, &a, &mut own, false).await;
    own.statement_done();
    match r {
        Ok(o) => {
            own.commit().await?;
            Ok((o.affected, o.info, o.conds.len()))
        }
        Err(err) => {
            own.rollback();
            Err(err)
        }
    }
}

/// Document `id` (a string `_id`) as the store holds it.
async fn stored(store: &DocStore, id: Bson) -> Option<bson::Document> {
    let r = store
        .command(
            TENANT.0,
            "app",
            "shop",
            "find",
            &doc! {"find": "events", "filter": {"_id": id}},
        )
        .await
        .unwrap();
    let d = r.into_doc(0);
    d.get_document("cursor")
        .ok()?
        .get_array("firstBatch")
        .ok()?
        .first()?
        .as_document()
        .cloned()
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one statement after the other, each checked
async fn sql_writes_run_through_the_document_store() {
    let (e, f, _d) = setup().await;
    let store = DocStore::new(Arc::new(f.eng.clone()));
    // INSERT: `_id` from the document, from the column, or made.
    let (n, info, _) = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"_id":"n1","k":1}'), (JSON_OBJECT('k', 2, 'big', 5000000000))"#,
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        info.as_deref(),
        Some("Records: 2  Duplicates: 0  Warnings: 0")
    );
    let n1 = stored(&store, Bson::String("n1".into())).await.unwrap();
    assert_eq!(n1.get("k"), Some(&Bson::Int32(1)));
    let made = select(
        &e,
        &f,
        "SELECT _id, doc->>'$.big' FROM events WHERE doc->>'$.k' = 2",
    )
    .await;
    assert_eq!(made.len(), 1);
    assert_eq!(made[0].len(), 24 + 1 + 10, "{made:?}");
    write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (_id, doc) VALUES ('n2', '{"v":true}')"#,
    )
    .await
    .unwrap();
    assert_eq!(
        stored(&store, Bson::String("n2".into())).await,
        Some(doc! {"_id": "n2", "v": true})
    );
    // A duplicate `_id`: 1062, or a warning with IGNORE.
    let err = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"_id":"n1"}')"#,
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1062, "{err}");
    let (n, _, warns) = write(
        &e,
        &f,
        &store,
        r#"INSERT IGNORE INTO events (doc) VALUES ('{"_id":"n1"}'), ('{"_id":"n3"}')"#,
    )
    .await
    .unwrap();
    assert_eq!((n, warns), (1, 1));
    // Disagreeing `_id`s, and a document that isn't one.
    for sql in [
        r#"INSERT INTO events (_id, doc) VALUES ('a', '{"_id":"b"}')"#,
        "INSERT INTO events (doc) VALUES ('[1,2]')",
    ] {
        let err = write(&e, &f, &store, sql).await.unwrap_err();
        assert_eq!(err.protocol().mysql_errno, 39667, "{sql}: {err}");
    }
    // UPDATE keeps the types of what it leaves alone (`n` is a long, `at`
    // a date), and `_id`.
    let (n, info, _) = write(
        &e,
        &f,
        &store,
        "UPDATE events SET doc = JSON_SET(doc, '$.n', 6, '$.kind', 'sell') WHERE _id IN ('1', 'e3')",
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        info.as_deref(),
        Some("Rows matched: 2  Changed: 2  Warnings: 0")
    );
    let one = stored(&store, Bson::Int32(1)).await.unwrap();
    assert_eq!(one.get("n"), Some(&Bson::Int64(6)));
    assert_eq!(one.get_str("kind").unwrap(), "sell");
    let e3 = stored(&store, Bson::String("e3".into())).await.unwrap();
    assert!(matches!(e3.get("at"), Some(Bson::DateTime(_))), "{e3:?}");
    assert_eq!(e3.get_str("kind").unwrap(), "sell");
    // An unchanged row is matched, not changed.
    let (_, info, _) = write(
        &e,
        &f,
        &store,
        "UPDATE events SET doc = doc WHERE _id = 'e3'",
    )
    .await
    .unwrap();
    assert_eq!(
        info.as_deref(),
        Some("Rows matched: 1  Changed: 0  Warnings: 0")
    );
    // `_id` can't change.
    for sql in [
        "UPDATE events SET doc = JSON_SET(doc, '$._id', 'zz') WHERE _id = 'e3'",
        "UPDATE events SET _id = 'zz' WHERE _id = 'e3'",
    ] {
        let err = write(&e, &f, &store, sql).await.unwrap_err();
        assert_eq!(err.protocol().mysql_errno, 39668, "{sql}: {err}");
    }
    // A new document without `_id` keeps the old one.
    write(
        &e,
        &f,
        &store,
        r#"UPDATE events SET doc = '{"fresh":1}' WHERE _id = 'n3'"#,
    )
    .await
    .unwrap();
    assert_eq!(
        stored(&store, Bson::String("n3".into())).await,
        Some(doc! {"_id": "n3", "fresh": 1})
    );
    // A join with a table finds the rows to change.
    let (n, _, _) = write(
        &e,
        &f,
        &store,
        "UPDATE events t JOIN customers c ON c.id = t.doc->>'$.customer_id' SET t.doc = JSON_SET(t.doc, '$.who', c.name) WHERE c.name = 'Ben'",
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    let who = select(
        &e,
        &f,
        "SELECT doc->>'$.who' FROM events WHERE doc->>'$.who' IS NOT NULL",
    )
    .await;
    assert_eq!(who, ["Ben", "Ben"]);
    // REPLACE: the document of the same `_id` is replaced.
    let (n, _, _) = write(
        &e,
        &f,
        &store,
        r#"REPLACE INTO events (doc) VALUES ('{"_id":"n2","v":2}')"#,
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        stored(&store, Bson::String("n2".into())).await,
        Some(doc! {"_id": "n2", "v": 2})
    );
    // INSERT ... SELECT from a table.
    let (n, info, _) = write(
        &e,
        &f,
        &store,
        "INSERT INTO events (_id, doc) SELECT CONCAT('c', id), JSON_OBJECT('name', name) FROM customers",
    )
    .await
    .unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        info.as_deref(),
        Some("Records: 3  Duplicates: 0  Warnings: 0")
    );
    // DELETE.
    let (n, _, _) = write(
        &e,
        &f,
        &store,
        "DELETE FROM events WHERE _id LIKE 'c%' OR _id = 'n1'",
    )
    .await
    .unwrap();
    assert_eq!(n, 4);
    assert!(stored(&store, Bson::String("n1".into())).await.is_none());
    // A form not built: two collections written by one statement.
    let sql = "UPDATE events a JOIN events b ON a._id = b._id SET a.doc = '{}', b.doc = '{}'";
    let err = crate::engine::collwrite::bind(
        &cx(&f),
        &parse(sql),
        sql,
        sprig_sql::lex::LexOptions::mariadb(),
    )
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, NOT_ON_ENGINE);
}

#[tokio::test]
async fn a_unique_index_and_a_validator_hold_for_sql_writes() {
    let (e, f, _d) = setup().await;
    let store = DocStore::new(Arc::new(f.eng.clone()));
    store
        .command(
            TENANT.0,
            "app",
            "shop",
            "createIndexes",
            &doc! {"createIndexes": "events", "indexes": [{"key": {"n": 1}, "name": "kn", "unique": true, "sparse": true}]},
        )
        .await
        .unwrap();
    let err = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"n":5}')"#,
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1062, "{err}");
    assert!(err.message.contains("for key 'kn'"), "{err}");
    // The index follows an UPDATE: the old entry goes, the new one is found.
    write(
        &e,
        &f,
        &store,
        "UPDATE events SET doc = JSON_SET(doc, '$.n', 8) WHERE _id = '1'",
    )
    .await
    .unwrap();
    write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"n":5}')"#,
    )
    .await
    .unwrap();
    let r = store
        .command(
            TENANT.0,
            "app",
            "shop",
            "find",
            &doc! {"find": "events", "filter": {"n": 8}, "hint": "kn"},
        )
        .await
        .unwrap()
        .into_doc(0);
    let batch = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "{r:?}");
    store
        .command(
            TENANT.0,
            "app",
            "shop",
            "collMod",
            &doc! {"collMod": "events", "validator": {"customer_id": {"$type": "number"}}},
        )
        .await
        .unwrap();
    let err = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"customer_id":"three"}')"#,
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 39667, "{err}");
}

#[tokio::test]
async fn on_duplicate_key_update_changes_the_conflicting_document() {
    let (e, f, _d) = setup().await;
    let store = DocStore::new(Arc::new(f.eng.clone()));
    // A new `_id`: inserted (1); an existing one: its document updated (2),
    // the assignment reading both the old document and the new row.
    let (n, info, _) = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"_id":"u1","hits":1}'), ('{"_id":"e3","hits":1}') ON DUPLICATE KEY UPDATE doc = JSON_SET(doc, '$.hits', JSON_EXTRACT(VALUES(doc), '$.hits') + 10)"#,
    )
    .await
    .unwrap();
    assert_eq!(n, 3);
    assert_eq!(
        info.as_deref(),
        Some("Records: 2  Duplicates: 1  Warnings: 0")
    );
    let e3 = stored(&store, Bson::String("e3".into())).await.unwrap();
    assert_eq!(e3.get("hits"), Some(&Bson::Int32(11)), "{e3:?}");
    // The rest of the document kept its types.
    assert_eq!(
        e3.get("at"),
        Some(&Bson::DateTime(bson::DateTime::from_millis(
            1_700_000_000_000
        )))
    );
    // Nothing changes: 0 rows.
    let (n, _, _) = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"_id":"e3"}') ON DUPLICATE KEY UPDATE doc = doc"#,
    )
    .await
    .unwrap();
    assert_eq!(n, 0);
    // `_id` stays.
    let err = write(
        &e,
        &f,
        &store,
        r#"INSERT INTO events (doc) VALUES ('{"_id":"e3"}') ON DUPLICATE KEY UPDATE _id = 'zz'"#,
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 39668, "{err}");
}

#[tokio::test]
async fn one_statement_writes_a_table_and_a_collection() {
    let (e, f, _d) = setup().await;
    let store = DocStore::new(Arc::new(f.eng.clone()));
    let (n, info, _) = write(
        &e,
        &f,
        &store,
        "UPDATE customers c JOIN events ev ON ev._id = 'e3' AND c.id = 3 SET c.name = 'Asha B', ev.doc = JSON_SET(ev.doc, '$.who', c.name)",
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        info.as_deref(),
        Some("Rows matched: 2  Changed: 2  Warnings: 0")
    );
    let e3 = stored(&store, Bson::String("e3".into())).await.unwrap();
    // The value worked out from the rows as they were before the statement.
    assert_eq!(e3.get("who"), Some(&Bson::String("Asha".into())));
    assert_eq!(
        select(&e, &f, "SELECT name FROM customers WHERE id = 3").await,
        vec!["Asha B".to_owned()]
    );
    let (n, _, _) = write(
        &e,
        &f,
        &store,
        "DELETE c, ev FROM customers c JOIN events ev ON ev._id = 'e3' AND c.id = 3",
    )
    .await
    .unwrap();
    assert_eq!(n, 2);
    assert!(stored(&store, Bson::String("e3".into())).await.is_none());
    assert!(
        select(&e, &f, "SELECT name FROM customers WHERE id = 3")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn a_field_index_answers_as_a_scan_does() {
    let (e, f, _d) = setup().await;
    let store = DocStore::new(Arc::new(f.eng.clone()));
    let queries = [
        "SELECT _id FROM events WHERE doc->>'$.customer_id' = 3 ORDER BY _id",
        "SELECT _id FROM events WHERE JSON_EXTRACT(doc, '$.customer_id') = 4 ORDER BY _id",
        "SELECT _id FROM events WHERE JSON_VALUE(doc, '$.customer_id') > 3.5 ORDER BY _id",
        "SELECT _id FROM events WHERE doc->>'$.customer_id' BETWEEN 3 AND 4 ORDER BY _id",
        "SELECT _id FROM events WHERE doc->>'$.customer_id' IN (99, 5) ORDER BY _id",
        "SELECT _id FROM events WHERE doc->>'$.customer_id' IS NULL ORDER BY _id",
    ];
    let mut before = Vec::new();
    for q in queries {
        before.push(select(&e, &f, q).await);
    }
    store
        .command(
            TENANT.0,
            "app",
            "shop",
            "createIndexes",
            &doc! {"createIndexes": "events", "indexes": [{"key": {"customer_id": 1}, "name": "cust_1"}]},
        )
        .await
        .unwrap();
    // A document whose field is text: SQL compares it as a number too.
    store
        .command(
            TENANT.0,
            "app",
            "shop",
            "insert",
            &doc! {"insert": "events", "documents": [{"_id": "t1", "customer_id": "3"}]},
        )
        .await
        .unwrap();
    for (i, q) in queries.iter().enumerate() {
        let mut want = before[i].clone();
        let got = select(&e, &f, q).await;
        if i == 0 || i == 3 {
            want.push("t1".to_owned());
            want.sort();
        }
        assert_eq!(got, want, "{q}");
    }
    // The planner reads the index.
    let c = f.collection(DB, "events").unwrap();
    assert_eq!(c.paths.len(), 1);
    assert_eq!(c.paths[0].path, "$.customer_id");
}
