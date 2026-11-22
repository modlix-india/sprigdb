//! General `SELECT`s on engine tables: what a statement may read, the
//! refusals, and the planner's answers over the engine's storage against
//! its answers over the same rows held in memory.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{Isolation, TxnOptions};
use sprig_engine::{EngineConfig, StoreKind};
use sprig_exec::Ctx;
use sprig_plan::Planner;
use sprig_plan::mem::MemDb;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::results::{self, Shape};
use super::{General, RunArgs, Seed, bind, takes};
use crate::engine::assign::Env;
use crate::engine::meta::Results;
use crate::engine::plan::{self, BindCtx, Names};
use crate::engine::row::Datum;
use crate::engine::session::{SessionTxn, Start};
use crate::engine::table::{self, Parsed};
use crate::engine::{Engine, NOT_ON_ENGINE};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "sprig-gw-general-{}-{}",
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

fn mem() -> (Engine, TempDir) {
    let d = TempDir::new();
    let mut cfg = EngineConfig::new(&d.0);
    cfg.store = StoreKind::Mem;
    cfg.log.sync = SyncMode::Off;
    cfg.checkpoint_interval = Duration::ZERO;
    (Engine::new(sprig_engine::Engine::open(cfg).unwrap()), d)
}

const TENANT: TenantId = TenantId(61_306_000_002);
const DB: DbId = DbId(1);
const TAIL: &str = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

fn create(body: &str) -> String {
    format!("{body}\n{TAIL}")
}

struct Fake {
    tables: HashMap<String, Arc<Parsed>>,
}

impl Fake {
    fn new() -> Self {
        let defs = [
            (
                "customers",
                "CREATE TABLE `customers` (\n  `id` int(11) NOT NULL,\n  `name` varchar(40) NOT NULL,\n  `city` varchar(20) DEFAULT NULL,\n  `tier` enum('gold','silver','none') NOT NULL DEFAULT 'none',\n  `joined` datetime DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `city` (`city`)",
            ),
            (
                "orders",
                "CREATE TABLE `orders` (\n  `id` bigint(20) NOT NULL,\n  `customer_id` int(11) NOT NULL,\n  `amount` decimal(10,2) NOT NULL,\n  `placed` date DEFAULT NULL,\n  `status` varchar(10) DEFAULT NULL,\n  `ref` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `ref` (`ref`),\n  KEY `cust` (`customer_id`),\n  KEY `placed` (`placed`,`customer_id`)",
            ),
            (
                "items",
                "CREATE TABLE `items` (\n  `order_id` bigint(20) NOT NULL,\n  `line` int(11) NOT NULL,\n  `sku` varchar(20) NOT NULL,\n  `qty` int(11) DEFAULT NULL,\n  `price` decimal(8,2) DEFAULT NULL,\n  PRIMARY KEY (`order_id`,`line`),\n  KEY `sku` (`sku`(3)),\n  KEY `qty` (`qty` DESC)",
            ),
        ];
        let mut tables = HashMap::new();
        for (i, (name, body)) in defs.iter().enumerate() {
            tables.insert(
                (*name).to_owned(),
                Arc::new(table::parse(ObjectId(20 + i as u64), 1, DB, &create(body))),
            );
        }
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
        name == "v_orders"
    }
    fn db_name(&self, _db: DbId) -> String {
        "shop".into()
    }
    fn db_by_name(&self, name: &str) -> Option<DbId> {
        (name == "shop")
            .then_some(DB)
            .or((name == "legacy").then_some(DbId(2)))
    }
    fn version(&self) -> u64 {
        1
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
    Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0)
}

fn general(f: &Fake, sql: &str) -> sprig_common::Result<Option<General>> {
    bind(&cx(f), &parse(sql), Some(sql), None)
}

fn refused(f: &Fake, sql: &str) -> (u16, String) {
    match general(f, sql) {
        Err(e) => (e.protocol().mysql_errno, e.message),
        Ok(Some(_)) => (0, "bound".into()),
        Ok(None) => (0, "not the engine's".into()),
    }
}

async fn write(e: &Engine, f: &Fake, sql: &str) {
    let p = plan::bind(&cx(f), &parse(sql))
        .unwrap()
        .expect("a single-table statement");
    e.run(TENANT.0, &p, &[], false, TxnOptions::default())
        .await
        .unwrap();
}

async fn shop() -> (Engine, Fake, TempDir) {
    let (e, d) = mem();
    let f = Fake::new();
    let cities = ["Pune", "Oslo", "pune", "Lima"];
    let mut sql = String::from("INSERT INTO customers (id, name, city, tier, joined) VALUES ");
    for i in 1..=24 {
        if i > 1 {
            sql.push(',');
        }
        let city = if i % 7 == 0 {
            "NULL".to_owned()
        } else {
            format!("'{}'", cities[i % 4])
        };
        let tier = ["gold", "silver", "none"][i % 3];
        let _ = write!(
            sql,
            "({i}, 'name{:02}', {city}, '{tier}', '2024-0{}-1{} 10:00:0{}')",
            i,
            1 + i % 9,
            i % 10,
            i % 10
        );
    }
    write(&e, &f, &sql).await;
    let mut sql =
        String::from("INSERT INTO orders (id, customer_id, amount, placed, status, ref) VALUES ");
    for i in 1..=120i64 {
        if i > 1 {
            sql.push(',');
        }
        let placed = if i % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("'2024-{:02}-{:02}'", 1 + i % 12, 1 + i % 28)
        };
        let status = ["new", "paid", "sent", "NEW"][usize::try_from(i % 4).unwrap()];
        let r = if i % 5 == 0 {
            "NULL".to_owned()
        } else {
            (1000 - i).to_string()
        };
        let _ = write!(
            sql,
            "({i}, {}, {}.{:02}, {placed}, '{status}', {r})",
            1 + (i * 7) % 30,
            (i * 37) % 500,
            i % 100
        );
    }
    write(&e, &f, &sql).await;
    let mut sql = String::from("INSERT INTO items (order_id, line, sku, qty, price) VALUES ");
    let mut first = true;
    for o in 1..=120i64 {
        for line in 1..=(1 + o % 3) {
            if !first {
                sql.push(',');
            }
            first = false;
            let qty = if (o + line) % 9 == 0 {
                "NULL".to_owned()
            } else {
                ((o * line) % 7).to_string()
            };
            let _ = write!(
                sql,
                "({o}, {line}, 'sku-{:03}', {qty}, {}.50)",
                (o * 3 + line) % 40,
                (o + line) % 30
            );
        }
    }
    write(&e, &f, &sql).await;
    (e, f, d)
}

fn seed() -> Seed {
    Seed::of(false)
}

fn show(g: &General, cols: &[sprig_plan::Column], rows: &[Vec<Datum>]) -> Vec<String> {
    let results = Results {
        cs: None,
        mysql: false,
    };
    let l = results::layout(g, cols, results);
    rows.iter()
        .map(|r| {
            r.iter()
                .zip(&l.shapes)
                .map(|(v, s)| {
                    results::text(g, s, v, &Env::STRICT, results).map_or_else(
                        || "NULL".to_owned(),
                        |b| String::from_utf8_lossy(&b).into_owned(),
                    )
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn select(
    e: &Engine,
    f: &Fake,
    sql: &str,
    params: &[Datum],
) -> sprig_common::Result<Vec<String>> {
    let g = general(f, sql)?.expect("a general statement");
    let s = seed();
    let a = RunArgs {
        tenant: TENANT.0,
        params,
        seed: &s,
        deadline: None,
        stop: None,
    };
    let rows = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await?;
    Ok(show(&g, &rows.columns, &rows.rows))
}

/// The same tables in memory: every row read back through the
/// single-table path.
async fn in_memory(e: &Engine, f: &Fake) -> Arc<MemDb> {
    let mut db = MemDb::new("shop");
    for name in ["customers", "orders", "items"] {
        let g = general(f, &format!("SELECT * FROM {name} t JOIN {name} u ON 1 = 0"))
            .unwrap()
            .unwrap();
        db.create_table((*g.tables[0].plan).clone());
        let p = plan::bind(&cx(f), &parse(&format!("SELECT * FROM {name}")))
            .unwrap()
            .unwrap();
        let o = e
            .run(TENANT.0, &p, &[], false, TxnOptions::default())
            .await
            .unwrap();
        for r in o.rows {
            db.insert(name, r).unwrap();
        }
    }
    Arc::new(db)
}

/// The planner's answer over the in-memory tables, values printed as the
/// expression engine prints them.
fn reference(db: &Arc<MemDb>, sql: &str, params: &[Datum]) -> Result<Vec<String>, String> {
    let session = seed().session;
    let planner = Planner::new(&**db, db.clone(), &session);
    let mut plan = planner
        .plan(&parse(sql), params)
        .map_err(|e| format!("{}: {}", e.code, e.message))?;
    let mut ctx = Ctx::with_params(&session, params);
    let mut out = Vec::new();
    while let Some(r) = plan
        .source
        .next(&mut ctx)
        .map_err(|e| format!("{}: {}", e.code, e.message))?
    {
        out.push(
            r.iter()
                .zip(&plan.columns)
                .map(|(v, c)| {
                    sprig_exec::value_text(v, &c.ty, session.persona).map_or_else(
                        || "NULL".to_owned(),
                        |b| String::from_utf8_lossy(&b).into_owned(),
                    )
                })
                .collect::<Vec<_>>()
                .join("|"),
        );
    }
    Ok(out)
}

/// Statements without `tier` (an `ENUM`, which the in-memory reference
/// would print as a number): every feature the planner has.
const STATEMENTS: &[&str] = &[
    // joins
    "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id < 20",
    "SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id ORDER BY o.id",
    "SELECT c.id, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.amount > 300",
    "SELECT c.name, i.sku, i.qty FROM customers c JOIN orders o ON o.customer_id = c.id JOIN items i ON i.order_id = o.id WHERE c.city = 'Oslo' AND i.qty > 3",
    "SELECT o.id, i.line FROM orders o, items i WHERE i.order_id = o.id AND o.status = 'paid' AND i.line = 2",
    "SELECT a.id, b.id FROM orders a JOIN orders b ON b.customer_id = a.customer_id AND b.id > a.id WHERE a.id <= 12",
    "SELECT o.id, i.sku FROM orders o JOIN items i USING (order_id, line) WHERE 1 = 0",
    "SELECT COUNT(*) FROM orders o JOIN items i ON i.order_id = o.id",
    "SELECT o.id FROM orders o STRAIGHT_JOIN customers c ON c.id = o.customer_id WHERE c.city IS NULL",
    // grouping
    "SELECT customer_id, COUNT(*), SUM(amount), MIN(placed), MAX(placed) FROM orders GROUP BY customer_id",
    "SELECT status, COUNT(*) AS n FROM orders GROUP BY status HAVING n > 20 ORDER BY n DESC, status",
    "SELECT c.city, COUNT(o.id), AVG(o.amount) FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.city",
    "SELECT COUNT(*), COUNT(placed), COUNT(DISTINCT customer_id), SUM(amount) FROM orders",
    "SELECT order_id, MIN(sku), MAX(sku), COUNT(qty) FROM items WHERE order_id < 9 GROUP BY order_id",
    "SELECT MIN(id), MAX(id) FROM orders",
    "SELECT MAX(qty) FROM items",
    "SELECT customer_id, SUM(amount) FROM orders WHERE placed >= '2024-06-01' GROUP BY customer_id ORDER BY 2 DESC LIMIT 5",
    // DISTINCT, ordering off the key, LIMIT
    "SELECT DISTINCT status FROM orders",
    "SELECT DISTINCT city FROM customers ORDER BY city",
    "SELECT DISTINCT customer_id, status FROM orders WHERE id BETWEEN 10 AND 60 ORDER BY customer_id, status",
    "SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 7",
    "SELECT id, amount FROM orders ORDER BY amount, id LIMIT 5 OFFSET 10",
    "SELECT name FROM customers ORDER BY city, name DESC",
    "SELECT id FROM orders ORDER BY placed DESC, id LIMIT 9",
    // expressions and functions
    "SELECT id, amount * 2, amount + customer_id, -amount FROM orders WHERE id <= 5",
    "SELECT id, CONCAT(name, '/', COALESCE(city, '?')), UPPER(name), LENGTH(name) FROM customers WHERE id < 8",
    "SELECT id, CASE WHEN amount > 250 THEN 'big' WHEN amount > 100 THEN 'mid' ELSE 'small' END FROM orders WHERE id < 30",
    "SELECT id, YEAR(placed), MONTH(placed), DATEDIFF(placed, '2024-01-01') FROM orders WHERE id < 15",
    "SELECT id, amount DIV 7, amount % 7, ROUND(amount / 3, 1) FROM orders WHERE id < 10",
    "SELECT id, IF(ref IS NULL, 0, ref), IFNULL(status, 'x') FROM orders WHERE id < 12",
    "SELECT SUM(amount) FROM orders WHERE id BETWEEN 5 AND 104",
    "SELECT status FROM orders WHERE id BETWEEN 5 AND 104 ORDER BY status",
    "SELECT DISTINCT status FROM orders WHERE id BETWEEN 5 AND 104 ORDER BY status",
    "SELECT id FROM orders WHERE status LIKE 'n%' AND amount NOT BETWEEN 100 AND 400",
    "SELECT id, joined, DATE(joined), HOUR(joined) FROM customers WHERE id < 6",
    "SELECT 1 + 1, id FROM customers WHERE id = 3",
    // key ranges and index paths
    "SELECT id, ref FROM orders WHERE ref IN (991, 986, 12, 994) ORDER BY ref",
    "SELECT id FROM orders WHERE ref > 950 ORDER BY ref DESC",
    "SELECT id FROM orders WHERE ref IS NULL AND id < 40",
    "SELECT id, placed FROM orders WHERE placed BETWEEN '2024-03-01' AND '2024-03-31' ORDER BY placed, id",
    "SELECT placed, customer_id FROM orders WHERE placed = '2024-06-06'",
    "SELECT customer_id FROM orders WHERE customer_id IN (3, 5, 8) ORDER BY customer_id",
    "SELECT order_id, line FROM items WHERE order_id BETWEEN 10 AND 14 ORDER BY order_id DESC, line DESC",
    "SELECT order_id, line FROM items WHERE (order_id = 7 AND line >= 2) OR order_id = 9",
    "SELECT order_id, sku FROM items WHERE sku LIKE 'sku-01%'",
    "SELECT order_id, qty FROM items WHERE qty >= 5 ORDER BY qty DESC, order_id, line",
    "SELECT order_id, qty FROM items WHERE qty IS NULL",
    "SELECT qty, COUNT(*) FROM items GROUP BY qty",
    "SELECT id, city FROM customers WHERE city = 'PUNE' ORDER BY id",
    "SELECT id FROM customers WHERE city > 'M' ORDER BY city, id",
    "SELECT id FROM orders WHERE customer_id = 9 AND id > 50",
    "SELECT id FROM orders WHERE ref = 991 AND id >= 9",
    "SELECT id FROM orders WHERE ref BETWEEN 900 AND 991 AND id IN (9, 11, 12, 99)",
    // set operations, derived tables, common table expressions
    "SELECT id FROM orders WHERE id < 4 UNION SELECT id FROM customers WHERE id < 6",
    "SELECT status FROM orders UNION ALL SELECT city FROM customers ORDER BY 1 LIMIT 12",
    "SELECT d.customer_id, d.total FROM (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) d WHERE d.total > 800 ORDER BY d.total",
    "SELECT d.id, d.name FROM (SELECT id, name FROM customers WHERE city = 'Lima') d",
    "SELECT c.name, d.n FROM customers c JOIN (SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id) d ON d.customer_id = c.id ORDER BY d.n DESC, c.name",
    "WITH big AS (SELECT id, customer_id FROM orders WHERE amount > 400) SELECT c.name, big.id FROM big JOIN customers c ON c.id = big.customer_id ORDER BY big.id",
    "WITH t AS (SELECT customer_id, COUNT(*) n FROM orders GROUP BY customer_id) SELECT MAX(n), MIN(n) FROM t",
    // subqueries
    "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 450)",
    "SELECT id FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)",
    "SELECT id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'sent')",
    "SELECT id FROM customers c WHERE NOT EXISTS (SELECT * FROM orders o WHERE o.customer_id = c.id)",
    "SELECT id, (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) FROM customers c ORDER BY id",
    "SELECT id FROM orders WHERE amount > (SELECT AVG(amount) FROM orders) ORDER BY id LIMIT 10",
    "SELECT id, (SELECT name FROM customers c WHERE c.id = o.customer_id) FROM orders o WHERE o.id < 10",
    "SELECT o.id FROM orders o WHERE o.id IN (SELECT i.order_id FROM items i WHERE i.qty = 6) AND o.status <> 'new'",
    // names
    "SELECT shop.orders.id, shop.orders.status FROM shop.orders WHERE shop.orders.id = 4 OR orders.id = 6",
    "SELECT o.* FROM orders o WHERE o.id IN (1, 2)",
    "SELECT * FROM items WHERE order_id = 3",
    "SELECT i.*, o.status FROM items i JOIN orders o ON o.id = i.order_id WHERE o.id = 30",
];

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[tokio::test]
async fn the_engines_storage_answers_as_the_in_memory_tables() {
    let (e, f, _d) = shop().await;
    let db = in_memory(&e, &f).await;
    let mut failed = Vec::new();
    for sql in STATEMENTS {
        let want = reference(&db, sql, &[]);
        let got = select(&e, &f, sql, &[])
            .await
            .map_err(|e| format!("{}: {}", e.protocol().mysql_errno, e.message));
        // A statement with ORDER BY on every column compares in order;
        // the others as sets of rows.
        let same = match (&want, &got) {
            (Ok(w), Ok(g)) => sorted(w.clone()) == sorted(g.clone()),
            (Err(w), Err(g)) => w.split(':').next() == g.split(':').next(),
            _ => false,
        };
        if !same {
            failed.push(format!("{sql}\n  memory: {want:?}\n  engine: {got:?}"));
        }
        // A statement that selects nothing proves nothing.
        if let (Ok(w), Ok(_)) = (&want, &got)
            && w.is_empty()
            && !sql.contains("1 = 0")
            && !sql.contains("NOT IN")
            && !sql.contains("NOT EXISTS")
        {
            failed.push(format!("{sql}\n  selects nothing"));
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

#[tokio::test]
async fn ordered_statements_keep_their_order() {
    let (e, f, _d) = shop().await;
    let db = in_memory(&e, &f).await;
    for sql in [
        "SELECT id, amount FROM orders ORDER BY amount DESC, id",
        "SELECT order_id, line FROM items WHERE order_id BETWEEN 10 AND 30 ORDER BY order_id DESC, line DESC",
        "SELECT id FROM orders WHERE ref IS NOT NULL ORDER BY ref DESC",
        "SELECT order_id, line, qty FROM items WHERE qty IS NOT NULL ORDER BY qty DESC, order_id, line",
        "SELECT id, placed FROM orders WHERE placed IS NOT NULL ORDER BY placed, customer_id, id",
        "SELECT id FROM orders ORDER BY id DESC LIMIT 5",
        "SELECT customer_id, id FROM orders WHERE customer_id IN (4, 9) ORDER BY customer_id DESC, id DESC",
    ] {
        let want = reference(&db, sql, &[]).unwrap();
        let got = select(&e, &f, sql, &[]).await.unwrap();
        assert_eq!(want, got, "{sql}");
        assert!(got.len() > 3, "{sql}");
    }
}

#[tokio::test]
async fn parameters_are_bound_at_each_run() {
    let (e, f, _d) = shop().await;
    let db = in_memory(&e, &f).await;
    let sql = "SELECT customer_id, SUM(amount) FROM orders WHERE id BETWEEN ? AND ? AND status = ? GROUP BY customer_id";
    for (lo, hi, st) in [(1, 40, "paid"), (30, 120, "new"), (7, 7, "sent")] {
        let params = [
            Datum::Int(lo),
            Datum::Int(hi),
            Datum::Str(bytes::Bytes::copy_from_slice(st.as_bytes())),
        ];
        let want = sorted(reference(&db, sql, &params).unwrap());
        let got = sorted(select(&e, &f, sql, &params).await.unwrap());
        assert_eq!(want, got);
    }
    let g = general(&f, sql).unwrap().unwrap();
    assert_eq!(g.params, 3);
    // Described without a run: the columns, no row read.
    let cols = Engine::general_describe(&g, &seed()).unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].name, "customer_id");
}

#[tokio::test]
async fn a_statement_reads_only_the_columns_it_names() {
    let f = Fake::new();
    let read = |sql: &str| -> Vec<(String, Vec<String>)> {
        let g = general(&f, sql).unwrap().unwrap();
        g.tables
            .iter()
            .map(|t| {
                (
                    t.def.name.clone(),
                    t.def
                        .columns
                        .iter()
                        .zip(&t.read)
                        .filter(|(_, r)| **r)
                        .map(|(c, _)| c.name.clone())
                        .collect(),
                )
            })
            .collect()
    };
    let names = |v: &[&str]| v.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    assert_eq!(
        read("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id"),
        vec![
            ("orders".to_owned(), names(&["id", "customer_id"])),
            ("customers".to_owned(), names(&["id", "name"])),
        ]
    );
    // An unqualified name marks every table that has it.
    assert_eq!(
        read("SELECT status FROM orders WHERE amount > (SELECT MAX(price) FROM items)"),
        vec![
            ("orders".to_owned(), names(&["amount", "status"])),
            ("items".to_owned(), names(&["price"])),
        ]
    );
    // `*` reads every column of that FROM's tables only.
    assert_eq!(
        read("SELECT * FROM items WHERE order_id IN (SELECT id FROM orders WHERE ref = 1)"),
        vec![
            (
                "items".to_owned(),
                names(&["order_id", "line", "sku", "qty", "price"])
            ),
            ("orders".to_owned(), names(&["id", "ref"])),
        ]
    );
    assert_eq!(
        read("SELECT c.* FROM customers c JOIN orders o ON o.customer_id = c.id")[1],
        ("orders".to_owned(), names(&["customer_id"]))
    );
    // USING names its columns; COUNT(*) names none.
    assert_eq!(
        read("SELECT COUNT(*) FROM items JOIN orders USING (id)"),
        vec![
            ("items".to_owned(), names(&[])),
            ("orders".to_owned(), names(&["id"])),
        ]
    );
    // A derived table's and a common table expression's inner names.
    assert_eq!(
        read(
            "WITH t AS (SELECT customer_id AS c, amount FROM orders) SELECT d.c FROM (SELECT c FROM t) d"
        ),
        vec![("orders".to_owned(), names(&["customer_id", "amount"]))]
    );
    // The order a missing privilege is reported in: as named.
    let g = general(&f, "SELECT status, id FROM orders WHERE amount > 1")
        .unwrap()
        .unwrap();
    let order: Vec<&str> = g.tables[0]
        .order
        .iter()
        .map(|c| g.tables[0].def.columns[*c].name.as_str())
        .collect();
    assert_eq!(order, ["status", "id", "amount"]);
    // A select list's `*` is told apart (a missing privilege under it
    // denies the table); a NATURAL join's columns and `COUNT(*)` aren't.
    let starred = |sql: &str| -> Vec<bool> {
        general(&f, sql)
            .unwrap()
            .unwrap()
            .tables
            .iter()
            .map(|t| t.starred.iter().any(|s| *s))
            .collect()
    };
    assert_eq!(
        starred("SELECT c.*, o.id FROM customers c JOIN orders o ON o.customer_id = c.id"),
        [true, false]
    );
    assert_eq!(
        starred("SELECT COUNT(*), MAX(o.ref) FROM items i NATURAL JOIN orders o"),
        [false, false]
    );
}

#[tokio::test]
async fn columns_a_statement_does_not_name_never_leave_storage() {
    let (e, f, _d) = shop().await;
    // The statement as bound, then with one of its columns taken off the
    // list storage serves: the value is gone from the result, whatever the
    // plan does.
    let sql = "SELECT o.id, o.status, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = 5";
    let mut g = general(&f, sql).unwrap().unwrap();
    let s = seed();
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: None,
        stop: None,
    };
    let rows = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await
        .unwrap();
    assert_eq!(show(&g, &rows.columns, &rows.rows), ["5|paid|name06"]);
    let status = g.tables[0].def.column("status").unwrap();
    let name = g.tables[1].def.column("name").unwrap();
    g.tables[0].read[status] = false;
    g.tables[1].read[name] = false;
    let rows = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await
        .unwrap();
    assert_eq!(show(&g, &rows.columns, &rows.rows), ["5|NULL|NULL"]);
    // A table the statement wasn't bound to can't be read at all.
    g.tables.pop();
    g.origins.clear();
    let err = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1146, "{}", err.message);
}

#[tokio::test]
async fn what_the_planner_does_not_run_is_refused() {
    let f = Fake::new();
    // Tables elsewhere, views, locking reads, other EXPLAIN forms.
    for sql in [
        "SELECT o.id FROM orders o JOIN legacy.old l ON l.id = o.id",
        "SELECT o.id FROM orders o JOIN v_orders v ON v.id = o.id",
        "SELECT o.id FROM orders o WHERE o.id IN (SELECT id FROM legacy.old)",
        "SELECT status, COUNT(*) FROM orders GROUP BY status FOR UPDATE",
        "EXPLAIN ANALYZE SELECT COUNT(*) FROM orders",
    ] {
        let (code, msg) = refused(&f, sql);
        assert_eq!(code, NOT_ON_ENGINE, "{sql}: {msg}");
    }
    // A locking clause the seam split off the text: the statement locks
    // what it reads.
    let stmt = parse("SELECT status, COUNT(*) FROM orders GROUP BY status");
    let lock = crate::engine::plan::RowLock {
        exclusive: true,
        wait: sprig_engine::txn::Wait::Block,
    };
    let g = bind(&cx(&f), &stmt, None, Some(lock)).unwrap().unwrap();
    assert_eq!(g.lock, Some(lock));
    // A missing table of an engine database is the servers' error.
    assert_eq!(
        refused(&f, "SELECT o.id FROM orders o JOIN nope n ON n.id = o.id").0,
        1146
    );
    // Not the engine's at all.
    assert!(general(&f, "SELECT 1 + 1").unwrap().is_none());
    assert!(general(&f, "SELECT id FROM legacy.old").unwrap().is_none());
    assert!(
        general(&f, "UPDATE orders SET status = 'x'")
            .unwrap()
            .is_none()
    );
    // An ENUM passes through as a plain column of the outermost select
    // list; computed with, it is refused.
    assert!(
        general(
            &f,
            "SELECT c.id, c.tier FROM customers c JOIN orders o ON o.customer_id = c.id"
        )
        .unwrap()
        .is_some()
    );
    assert!(
        general(&f, "SELECT DISTINCT * FROM customers ORDER BY name")
            .unwrap()
            .is_some()
    );
    for sql in [
        "SELECT tier, COUNT(*) FROM customers GROUP BY tier",
        "SELECT id FROM customers WHERE tier = 'gold' ORDER BY name",
        "SELECT CONCAT(tier, '!') FROM customers",
        "SELECT d.tier FROM (SELECT tier FROM customers) d",
        "SELECT tier FROM customers UNION SELECT status FROM orders",
    ] {
        let (code, msg) = refused(&f, sql);
        assert_eq!(code, NOT_ON_ENGINE, "{sql}: {msg}");
        assert!(msg.contains("ENUM"), "{msg}");
    }
    // What the planner itself doesn't cover yet is the engine's refusal
    // too, with the planner's words.
    let (e, f2, _d) = shop().await;
    for sql in [
        "SELECT order_id, GROUP_CONCAT(sku ORDER BY line) FROM items GROUP BY order_id",
        "SELECT id, DATE_ADD(placed, INTERVAL 3 DAY) FROM orders WHERE id < 15 ORDER BY placed",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY amount) FROM orders",
        "SELECT id, NO_SUCH_FUNCTION(amount) FROM orders ORDER BY amount",
    ] {
        let err = select(&e, &f2, sql, &[]).await.unwrap_err();
        assert_eq!(
            err.protocol().mysql_errno,
            NOT_ON_ENGINE,
            "{sql}: {}",
            err.message
        );
    }
    // Errors of the statement itself are the servers'.
    let err = select(
        &e,
        &f2,
        "SELECT nope, COUNT(*) FROM orders GROUP BY status",
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1054, "{}", err.message);
    // The single-table binder's own refusals hand a SELECT over; its
    // other errors stand.
    let stmt = parse("SELECT status, COUNT(*) FROM orders GROUP BY status");
    let e = plan::bind(&cx(&f), &stmt).unwrap_err();
    assert!(takes(&stmt, &e));
    let stmt = parse("SELECT nope FROM orders");
    let e = plan::bind(&cx(&f), &stmt).unwrap_err();
    assert!(!takes(&stmt, &e));
}

#[tokio::test]
async fn result_columns_know_their_tables() {
    let (e, f, _d) = shop().await;
    let sql = "SELECT c.id, c.tier, o.amount AS a, o.amount + 1, c.joined FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id IN (2, 30) ORDER BY o.id LIMIT 2";
    let g = general(&f, sql).unwrap().unwrap();
    let s = seed();
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: None,
        stop: None,
    };
    let rows = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await
        .unwrap();
    let results = Results {
        cs: None,
        mysql: false,
    };
    let l = results::layout(&g, &rows.columns, results);
    assert!(matches!(
        l.shapes[0],
        Shape::Column {
            table: 0,
            column: 0
        }
    ));
    assert!(matches!(l.shapes[1], Shape::Column { table: 0, .. }));
    assert!(matches!(l.shapes[2], Shape::Column { table: 1, .. }));
    assert!(matches!(l.shapes[3], Shape::Value(_)));
    let d = &l.defs;
    assert_eq!(&d[0].table[..], b"c");
    assert_eq!(&d[0].org_table[..], b"customers");
    assert_eq!(&d[0].schema[..], b"shop");
    assert_eq!(&d[2].name[..], b"a");
    assert_eq!(&d[2].org_name[..], b"amount");
    assert_eq!(&d[2].table[..], b"o");
    assert!(d[3].table.is_empty() && d[3].org_name.is_empty());
    // A computed column is named by its text as written.
    assert_eq!(&d[3].name[..], b"o.amount + 1");
    let g2 = general(
        &f,
        "SELECT  count(*),sum( amount )+1, 'lit', MAX(id) AS m, id FROM orders GROUP BY id",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        g2.written,
        vec![
            Some("count(*)".to_owned()),
            Some("sum( amount )+1".to_owned()),
            Some("lit".to_owned()),
            None,
            None
        ]
    );
    // The inner side of the outer join can be NULL; the outer side's key
    // can't.
    let not_null = sprig_proto_mysql::resultset::ColumnFlags::NOT_NULL.bits();
    assert_ne!(d[0].flags & not_null, 0);
    assert_eq!(d[2].flags & not_null, 0);
    // The ENUM prints as its member, through the table's own encoder.
    let shown = show(&g, &rows.columns, &rows.rows);
    assert_eq!(shown.len(), 2);
    assert!(shown[0].starts_with("2|none|"), "{shown:?}");
    // `*` over a join that merges columns: the shared column once, then
    // the left side's others and the right table's (a RIGHT JOIN the other
    // way round, its shared column from the right table).
    for (sql, id_from, second, orders_outer) in [
        (
            "SELECT * FROM orders JOIN customers USING (id)",
            "orders",
            "customer_id",
            false,
        ),
        (
            "SELECT * FROM orders RIGHT JOIN customers USING (id)",
            "customers",
            "name",
            true,
        ),
    ] {
        let g = general(&f, sql).unwrap().unwrap();
        let rows = e
            .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
            .await
            .unwrap();
        let l = results::layout(&g, &rows.columns, results);
        assert!(
            l.shapes.iter().all(|s| matches!(s, Shape::Column { .. })),
            "{sql}"
        );
        let names: Vec<&[u8]> = l.defs.iter().map(|d| &d.org_name[..]).collect();
        assert_eq!(names.len(), 10, "{sql}");
        assert_eq!(names[0], b"id");
        assert_eq!(names[1], second.as_bytes(), "{sql}");
        assert_eq!(&l.defs[0].org_table[..], id_from.as_bytes(), "{sql}");
        let cust = names.iter().position(|n| *n == b"customer_id").unwrap();
        assert_eq!(l.defs[cust].flags & not_null == 0, orders_outer, "{sql}");
    }
}

#[tokio::test]
async fn explain_is_the_planners() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "EXPLAIN SELECT c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = 7",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(got.iter().any(|r| r.contains("PRIMARY")), "{got:?}");
    assert!(got.iter().all(|r| r.starts_with("1|SIMPLE|")), "{got:?}");
}

#[tokio::test]
async fn statements_of_a_transaction_read_its_view_and_its_writes() {
    let (e, f, _d) = shop().await;
    let sql = "SELECT status, COUNT(*) FROM orders WHERE id > 100 GROUP BY status ORDER BY status";
    let g = general(&f, sql).unwrap().unwrap();
    let s = seed();
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: None,
        stop: None,
    };
    let before = select(&e, &f, sql, &[]).await.unwrap();
    let mut txn = SessionTxn::default();
    let mut opts = TxnOptions::mariadb();
    opts.isolation = Isolation::RepeatableRead;
    txn.begin(&e, opts, false, Start::default()).unwrap();
    let rows = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    assert_eq!(show(&g, &rows.columns, &rows.rows), before);
    // Another session commits a row: the transaction's view doesn't move.
    write(
        &e,
        &f,
        "INSERT INTO orders (id, customer_id, amount, status) VALUES (500, 1, 1.00, 'zzz')",
    )
    .await;
    let rows = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    assert_eq!(show(&g, &rows.columns, &rows.rows), before);
    // Its own write is in its next statement.
    let ins = plan::bind(
        &cx(&f),
        &parse("INSERT INTO orders (id, customer_id, amount, status) VALUES (501, 1, 1.00, 'own')"),
    )
    .unwrap()
    .unwrap();
    e.run_in(TENANT.0, &ins, &[], false, txn.statement().unwrap())
        .await
        .unwrap();
    txn.statement_done();
    let rows = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    let shown = show(&g, &rows.columns, &rows.rows);
    assert!(shown.contains(&"own|1".to_owned()), "{shown:?}");
    assert!(!shown.contains(&"zzz|1".to_owned()), "{shown:?}");
    assert!(txn.is_open());
    txn.rollback();
    // Outside it: the committed row, not the rolled-back one.
    let after = select(&e, &f, sql, &[]).await.unwrap();
    assert!(after.contains(&"zzz|1".to_owned()) && !after.contains(&"own|1".to_owned()));

    // READ COMMITTED: each statement sees what is committed when it starts.
    let mut txn = SessionTxn::default();
    opts.isolation = Isolation::ReadCommitted;
    txn.begin(&e, opts, false, Start::default()).unwrap();
    let rows = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    assert_eq!(show(&g, &rows.columns, &rows.rows), after);
    write(
        &e,
        &f,
        "INSERT INTO orders (id, customer_id, amount, status) VALUES (502, 1, 1.00, 'zzz')",
    )
    .await;
    let rows = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    assert!(show(&g, &rows.columns, &rows.rows).contains(&"zzz|2".to_owned()));
    txn.rollback();

    // SERIALIZABLE inside a transaction locks what it reads, and answers
    // as a plain read would.
    let mut txn = SessionTxn::default();
    opts.isolation = Isolation::Serializable;
    txn.begin(&e, opts, false, Start::default()).unwrap();
    let locked = e.general_run_in(&g, &a, &mut txn).await.unwrap();
    txn.statement_done();
    assert_eq!(locked.rows.len(), rows.rows.len());
    assert!(txn.is_open());
    txn.rollback();
}

#[tokio::test]
async fn a_statement_past_its_time_limit_ends_with_the_limits_error() {
    let (e, f, _d) = shop().await;
    let g = general(
        &f,
        "SELECT COUNT(*) FROM orders a JOIN orders b ON b.amount > a.amount JOIN items i ON i.qty <> b.customer_id",
    )
    .unwrap()
    .unwrap();
    let s = seed();
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: &s,
        deadline: Some((
            Instant::now(),
            sprig_engine::txn::errors::statement_timeout_mariadb,
        )),
        stop: None,
    };
    let err = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await
        .unwrap_err();
    assert_eq!(
        err.protocol().mysql_errno,
        sprig_engine::txn::errors::statement_timeout_mariadb()
            .protocol()
            .mysql_errno
    );
}

#[tokio::test]
async fn physical_database_names_read_as_the_logical_ones() {
    let (e, f, _d) = shop().await;
    let p = format!("t{}_d1_g0_s0", TENANT.0);
    let sql = format!(
        "SELECT `{p}`.`orders`.`id`, c.name FROM `{p}`.`orders` JOIN `{p}`.`customers` c ON c.id = `{p}`.`orders`.customer_id WHERE `{p}`.`orders`.id = 3"
    );
    assert_eq!(select(&e, &f, &sql, &[]).await.unwrap(), ["3|name22"]);
}
