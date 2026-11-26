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
    fn view(
        &self,
        _db: DbId,
        name: &str,
    ) -> Option<Arc<std::result::Result<crate::engine::view::ViewDef, String>>> {
        let text = match name {
            "big_orders" => {
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `big_orders` AS select `orders`.`id` AS `id`,`orders`.`customer_id` AS `cust`,`orders`.`amount` * 2 AS `twice` from `orders` where `orders`.`amount` > 50 WITH CASCADED CHECK OPTION"
            }
            "per_customer" => {
                "CREATE ALGORITHM=TEMPTABLE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `per_customer` (`cust`,`n`) AS select `big_orders`.`cust` AS `cust`,count(0) AS `n` from `big_orders` group by `big_orders`.`cust`"
            }
            "loop_a" => {
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `loop_a` AS select `loop_b`.`x` AS `x` from `loop_b`"
            }
            "loop_b" => {
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `loop_b` AS select `loop_a`.`x` AS `x` from `loop_a`"
            }
            _ => return None,
        };
        Some(Arc::new(
            crate::engine::view::load(ObjectId(90), DB, "shop", text).map_err(|e| e.message),
        ))
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
        "EXPLAIN FORMAT=TREE SELECT COUNT(*) FROM orders",
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
    // list, and as its member text in a set operation; computed with, it
    // is refused.
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
        // (`tier = 'gold'` runs: a member-number comparison.)
        "SELECT id FROM customers WHERE tier > 'gold' ORDER BY name",
        "SELECT CONCAT(tier, '!') FROM customers",
    ] {
        let (code, msg) = refused(&f, sql);
        assert_eq!(code, NOT_ON_ENGINE, "{sql}: {msg}");
        assert!(msg.contains("ENUM"), "{msg}");
    }
    // Read through a derived table, it is the member text.
    assert!(
        general(&f, "SELECT d.tier FROM (SELECT tier FROM customers) d")
            .unwrap()
            .is_some()
    );
    // What the planner itself doesn't cover yet is the engine's refusal
    // too, with the planner's words.
    let (e, f2, _d) = shop().await;
    for sql in [
        "SELECT order_id, VARIANCE(line) OVER () FROM items",
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
async fn subqueries_that_read_the_outer_query_run_per_outer_row() {
    let (e, f, _d) = shop().await;
    let db = in_memory(&e, &f).await;
    for sql in [
        // A condition other than an equality.
        "SELECT id FROM orders o WHERE NOT EXISTS (SELECT 1 FROM orders p \
         WHERE p.customer_id = o.customer_id AND p.amount > o.amount) ORDER BY id",
        // Correlated IN, in WHERE and in the select list.
        "SELECT id FROM customers c WHERE id IN (SELECT customer_id FROM orders o \
         WHERE o.amount > c.id * 10) ORDER BY id",
        "SELECT id, id IN (SELECT customer_id FROM orders o WHERE o.status = c.city OR o.amount > c.id * 30) FROM customers c ORDER BY id",
        // A scalar subquery over a join, and a grouped one with HAVING.
        "SELECT id, (SELECT COUNT(*) FROM orders o JOIN items i ON i.order_id = o.id \
         WHERE o.customer_id = c.id) FROM customers c ORDER BY id",
        "SELECT id, (SELECT MAX(n) FROM (SELECT 1 AS n) d WHERE c.id > 3) FROM customers c ORDER BY id",
        "SELECT id, (SELECT SUM(amount) FROM orders o WHERE o.customer_id <= c.id GROUP BY o.customer_id \
         HAVING SUM(amount) > 0 ORDER BY o.customer_id DESC LIMIT 1) FROM customers c ORDER BY id",
        // EXISTS with a non-equality in the select list.
        "SELECT id, EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.placed > c.joined) \
         FROM customers c ORDER BY id",
        // Nested: the inner subquery reads both outer queries.
        "SELECT id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id \
         AND EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id AND i.qty > c.id)) ORDER BY id",
    ] {
        let want = reference(&db, sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got = select(&e, &f, sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {}", e.message));
        assert_eq!(got, want, "{sql}");
        assert!(!got.is_empty(), "{sql}");
    }
    // More than one row for a scalar subquery is the servers' 1242.
    let err = select(
        &e,
        &f,
        "SELECT id, (SELECT id FROM orders o WHERE o.customer_id > c.id) FROM customers c",
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1242);
}

#[tokio::test]
async fn window_functions_run_on_the_engine() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "SELECT id, ROW_NUMBER() OVER w, RANK() OVER w, DENSE_RANK() OVER w, \
         SUM(amount) OVER w, LAG(id) OVER w, LEAD(id, 2) OVER w, COUNT(*) OVER (PARTITION BY customer_id), \
         FIRST_VALUE(id) OVER (PARTITION BY customer_id ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
         FROM orders WHERE customer_id = 3 WINDOW w AS (PARTITION BY customer_id ORDER BY id) ORDER BY id",
        &[],
    )
    .await
    .unwrap();
    assert!(!got.is_empty());
    let first: Vec<&str> = got[0].split('|').collect();
    assert_eq!(&first[1..4], ["1", "1", "1"], "{got:?}");
    assert_eq!(first[5], "NULL", "{got:?}");
    let db = in_memory(&e, &f).await;
    for sql in [
        "SELECT customer_id, COUNT(*), RANK() OVER (ORDER BY COUNT(*) DESC) FROM orders GROUP BY customer_id ORDER BY customer_id",
        "SELECT id, NTILE(4) OVER (ORDER BY amount, id), PERCENT_RANK() OVER (ORDER BY amount), CUME_DIST() OVER (ORDER BY amount) FROM orders ORDER BY id",
        "SELECT id, AVG(amount) OVER (PARTITION BY status ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM orders ORDER BY id",
        "SELECT id, LAST_VALUE(id) OVER (ORDER BY placed RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), NTH_VALUE(id, 2) OVER (ORDER BY id) FROM orders ORDER BY id",
    ] {
        let want = reference(&db, sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let got = select(&e, &f, sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {}", e.message));
        assert_eq!(got, want, "{sql}");
    }
    // Not in WHERE.
    let err = select(
        &e,
        &f,
        "SELECT id FROM orders WHERE ROW_NUMBER() OVER () > 1",
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 4015);
}

#[tokio::test]
async fn date_arithmetic_runs_on_the_engine() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "SELECT id, DATE_ADD(placed, INTERVAL 3 DAY), placed - INTERVAL 1 MONTH, \
         INTERVAL 2 DAY + placed, EXTRACT(YEAR_MONTH FROM placed), \
         TIMESTAMPDIFF(DAY, placed, '2025-01-01'), TIMESTAMPADD(HOUR, 5, placed), \
         placed + INTERVAL '1:30' HOUR_MINUTE, ADDDATE(placed, 1) \
         FROM orders WHERE id IN (1, 11) ORDER BY id",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        got,
        [
            "1|2024-02-05|2024-01-02|2024-02-04|202402|334|2024-02-02 05:00:00|2024-02-02 01:30:00|2024-02-03",
            "11|NULL|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
        ]
    );
    // In WHERE, against the statement's clock.
    let got = select(
        &e,
        &f,
        "SELECT COUNT(*) FROM orders WHERE placed > NOW() - INTERVAL 100 YEAR",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(got, ["110"]);
    // A unit neither server has is a syntax error.
    let err = select(&e, &f, "SELECT placed + INTERVAL 1 DAYS FROM orders", &[])
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1064);
}

#[tokio::test]
async fn row_comparisons_run_on_the_engine() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "SELECT (1, 2) < (1, 3), (1, NULL) < (2, 0), (NULL, 1) < (1, 2), (1, 2) <=> (1, 2), \
         (1, 2) IN ((1, 2), (3, 4)), (1, NULL) IN ((1, 2)), ((1, 2), 3) < ((1, 3), 0), \
         (id, 2) >= (1, 2) FROM orders WHERE id = 1",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(got, ["1|1|NULL|1|1|NULL|1|1"]);
    // Keyset pagination: `(a, b) < (x, y)` is `a < x OR (a = x AND b < y)`.
    let rows = |sql: &'static str| {
        let (e, f) = (&e, &f);
        async move { select(e, f, sql, &[]).await.unwrap() }
    };
    assert_eq!(
        rows("SELECT id FROM orders WHERE (customer_id, id) < (3, 20) ORDER BY id").await,
        rows("SELECT id FROM orders WHERE customer_id < 3 OR (customer_id = 3 AND id < 20) ORDER BY id")
            .await
    );
    // A row where one value belongs is 1241 on both servers.
    let err = select(&e, &f, "SELECT (1, 2) FROM orders", &[])
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1241);
}

#[tokio::test]
async fn enum_columns_compare_with_their_members_on_the_engine() {
    let (e, f, _d) = shop().await;
    let ids = |rows: Vec<String>| rows.join(",");
    // `tier` is ENUM('gold', 'silver', 'none'): gold for every third id.
    let got = select(
        &e,
        &f,
        "SELECT c.id, c.tier FROM customers c JOIN customers d ON d.id = c.id \
         WHERE c.tier IN ('GOLD', 'nope') AND d.tier <> 'silver' AND c.tier IS NOT NULL \
         ORDER BY c.id",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(
        ids(got),
        "3|gold,6|gold,9|gold,12|gold,15|gold,18|gold,21|gold,24|gold"
    );
    let none = select(
        &e,
        &f,
        "SELECT c.id FROM customers c JOIN customers d ON d.id = c.id WHERE c.tier = 'nope' \
         OR 'x' = d.tier",
        &[],
    )
    .await
    .unwrap();
    assert!(none.is_empty(), "{none:?}");
    // Computing with the member text is still refused.
    let (code, msg) = refused(
        &f,
        "SELECT CONCAT(c.tier, 'x') FROM customers c JOIN customers d ON d.id = c.id",
    );
    assert_eq!(code, NOT_ON_ENGINE, "{msg}");
    let (code, msg) = refused(
        &f,
        "SELECT c.id FROM customers c JOIN customers d ON d.id = c.id WHERE c.tier < 'none'",
    );
    assert_eq!(code, NOT_ON_ENGINE, "{msg}");
}

#[tokio::test]
async fn ordered_group_concat_and_bit_aggregates_run_on_the_engine() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "SELECT GROUP_CONCAT(id ORDER BY id DESC), GROUP_CONCAT(id, '-', id ORDER BY id SEPARATOR ';'), \
         BIT_OR(id), BIT_AND(id), BIT_XOR(id) FROM orders WHERE id IN (1, 2, 3)",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(got, ["3,2,1|1-1;2-2;3-3|3|0|0"]);
}

#[tokio::test]
async fn group_by_with_rollup_runs_on_the_engine() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "SELECT customer_id, id, COUNT(*) FROM orders WHERE id <= 4 GROUP BY customer_id, id WITH ROLLUP",
        &[],
    )
    .await
    .unwrap();
    // Four orders, a total per customer after its orders, the grand total
    // last.
    assert_eq!(got.last().map(String::as_str), Some("NULL|NULL|4"));
    let totals: usize = got
        .iter()
        .filter(|r| r.contains("|NULL|") && !r.starts_with("NULL"))
        .map(|r| r.rsplit('|').next().unwrap().parse::<usize>().unwrap())
        .sum();
    assert_eq!(totals, 4);
    // MariaDB doesn't order a rollup.
    let err = select(
        &e,
        &f,
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WITH ROLLUP ORDER BY customer_id",
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1221);
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

#[tokio::test(flavor = "multi_thread")]
async fn views_read_as_their_select() {
    let (e, f, _d) = shop().await;
    for (through, direct) in [
        (
            "SELECT * FROM big_orders ORDER BY id",
            "SELECT id, customer_id, amount * 2 FROM orders WHERE amount > 50 ORDER BY id",
        ),
        (
            "SELECT b.cust, c.name FROM big_orders b JOIN customers c ON c.id = b.cust WHERE b.twice > 300 ORDER BY b.id",
            "SELECT o.customer_id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.amount > 50 AND o.amount * 2 > 300 ORDER BY o.id",
        ),
        (
            "SELECT * FROM per_customer ORDER BY cust",
            "SELECT customer_id, COUNT(*) FROM orders WHERE amount > 50 GROUP BY customer_id ORDER BY customer_id",
        ),
        (
            "SELECT id FROM customers WHERE id IN (SELECT cust FROM big_orders) ORDER BY id",
            "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE amount > 50) ORDER BY id",
        ),
    ] {
        let got = select(&e, &f, through, &[])
            .await
            .unwrap_or_else(|e| panic!("{through}: {}", e.message));
        let want = select(&e, &f, direct, &[])
            .await
            .unwrap_or_else(|e| panic!("{direct}: {}", e.message));
        assert_eq!(
            got.iter().skip(1).collect::<Vec<_>>(),
            want.iter().skip(1).collect::<Vec<_>>(),
            "{through}"
        );
    }
    let err = select(&e, &f, "SELECT * FROM loop_a", &[])
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1462);
}

/// Runs `sql` under `seed`, giving the rows as text and the
/// `SQL_CALC_FOUND_ROWS` count.
async fn select_with(
    e: &Engine,
    f: &Fake,
    s: &Seed,
    sql: &str,
) -> sprig_common::Result<(Vec<String>, Option<u64>)> {
    // A query of common table expressions alone names no stored table.
    let stmt = parse(sql);
    let g = match general(f, sql)? {
        Some(g) => g,
        None => super::bind_no_tables(&cx(f), &stmt, Some(sql))?.expect("a general statement"),
    };
    let a = RunArgs {
        tenant: TENANT.0,
        params: &[],
        seed: s,
        deadline: None,
        stop: None,
    };
    let rows = e
        .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
        .await?;
    Ok((show(&g, &rows.columns, &rows.rows), rows.found))
}

#[tokio::test]
async fn recursive_common_table_expressions_run_on_the_engine() {
    let (e, f, _d) = shop().await;
    let maria = seed();
    let one = |sql: &'static str| {
        let (e, f, s) = (&e, &f, &maria);
        async move { select_with(e, f, s, sql).await.map(|(rows, _)| rows) }
    };
    let got = one("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT SUM(n), COUNT(*) FROM c").await.unwrap();
    assert_eq!(got, ["15|5"]);
    // A chain through a table, by key.
    let got = one("WITH RECURSIVE c(id, d) AS (SELECT id, 0 FROM customers WHERE id = 1 UNION ALL SELECT x.id, c.d + 1 FROM c JOIN customers x ON x.id = c.id * 2) SELECT GROUP_CONCAT(id ORDER BY id), MAX(d) FROM c").await.unwrap();
    assert_eq!(got, ["1,2,4,8,16|4"]);
    // UNION DISTINCT ends a cycle; MariaDB stops after 1000 passes.
    let got = one(
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n % 3 + 1 FROM c) SELECT COUNT(*) FROM c",
    )
    .await
    .unwrap();
    assert_eq!(got, ["3"]);
    let got = one("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c) SELECT COUNT(*), MAX(n) FROM c").await.unwrap();
    assert_eq!(got, ["1001|1001"]);
    // Two references, and a reference in a subquery of the main query.
    let got = one("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 3) SELECT a.n FROM c a JOIN c b ON a.n = b.n WHERE a.n > (SELECT MIN(n) FROM c) ORDER BY a.n").await.unwrap();
    assert_eq!(got, ["2", "3"]);
    // Strings are stored into the anchor's column type.
    let err = one("WITH RECURSIVE c(s) AS (SELECT 'a' UNION ALL SELECT CONCAT(s, 'a') FROM c WHERE LENGTH(s) < 3) SELECT * FROM c").await.unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1406);
    for (sql, code) in [
        (
            "WITH RECURSIVE c(n) AS (SELECT n FROM c) SELECT * FROM c",
            4005,
        ),
        (
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT COUNT(*) FROM c) SELECT * FROM c",
            4008,
        ),
        (
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT a.n FROM c a JOIN c b) SELECT * FROM c",
            4008,
        ),
        (
            "WITH RECURSIVE c(n, m) AS (SELECT 1 UNION ALL SELECT n FROM c) SELECT * FROM c",
            4002,
        ),
        (
            "WITH c AS (SELECT 1), c AS (SELECT 2) SELECT * FROM c",
            4004,
        ),
    ] {
        let err = one(sql).await.unwrap_err();
        assert_eq!(err.protocol().mysql_errno, code, "{sql}");
    }
    // Without RECURSIVE, an expression's own name is the stored table.
    let got = one("WITH customers AS (SELECT id, name FROM customers WHERE id < 3) SELECT COUNT(*) FROM customers").await.unwrap();
    assert_eq!(got, ["2"]);
    // MySQL: pass 1001 fails; a LIMIT on the whole stops the passes.
    let my = Seed::of(true);
    let err = select_with(
        &e,
        &f,
        &my,
        "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c) SELECT COUNT(*) FROM c",
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 3636);
    let (got, _) = select_with(&e, &f, &my, "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c LIMIT 2, 3) SELECT * FROM c").await.unwrap();
    assert_eq!(got, ["3", "4", "5"]);
    let err = select_with(
        &e,
        &f,
        &my,
        "WITH RECURSIVE c(n) AS (SELECT 1 FROM c UNION ALL SELECT 1) SELECT * FROM c",
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 3574);
}

#[tokio::test]
async fn sql_calc_found_rows_counts_past_the_limit() {
    let (e, f, _d) = shop().await;
    let s = seed();
    let (got, found) = select_with(
        &e,
        &f,
        &s,
        "SELECT SQL_CALC_FOUND_ROWS id FROM customers ORDER BY id LIMIT 1, 2",
    )
    .await
    .unwrap();
    assert_eq!(got, ["2", "3"]);
    assert_eq!(found, Some(24));
    let (_, found) = select_with(&e, &f, &s, "SELECT id FROM customers LIMIT 2")
        .await
        .unwrap();
    assert_eq!(found, None);
    let err = select_with(
        &e,
        &f,
        &s,
        "SELECT id FROM customers WHERE id IN (SELECT SQL_CALC_FOUND_ROWS id FROM customers)",
    )
    .await
    .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 1234);
}

#[tokio::test]
async fn json_table_runs_on_the_engine() {
    let (e, f, _d) = shop().await;
    let s = seed();
    let run = |sql: &'static str| {
        let (e, f, s) = (&e, &f, &s);
        async move { select_with(e, f, s, sql).await.map(|(rows, _)| rows) }
    };
    let got = run(r#"SELECT * FROM JSON_TABLE('[{"a":1,"b":"x"},{"a":2},{"c":3}]', '$[*]' COLUMNS (rn FOR ORDINALITY, a INT PATH '$.a', b VARCHAR(5) PATH '$.b', e INT EXISTS PATH '$.c')) AS jt"#).await.unwrap();
    assert_eq!(got, ["1|1|x|0", "2|2|NULL|0", "3|NULL|NULL|1"]);
    let got = run(r#"SELECT * FROM JSON_TABLE('{"x":[1,2],"y":[{"z":5}]}', '$' COLUMNS (NESTED PATH '$.x[*]' COLUMNS (x INT PATH '$'), NESTED PATH '$.y[*]' COLUMNS (z INT PATH '$.z'))) jt"#).await.unwrap();
    assert_eq!(got, ["1|NULL", "2|NULL", "NULL|5"]);
    // Lateral: the document reads the item before it.
    let got = run("SELECT c.id, j.v FROM customers c, JSON_TABLE(CONCAT('[', c.id, ',', c.id + 100, ']'), '$[*]' COLUMNS (v INT PATH '$')) AS j WHERE c.id <= 2 ORDER BY c.id, j.v").await.unwrap();
    assert_eq!(got, ["1|1", "1|101", "2|2", "2|102"]);
    let got = run("SELECT c.id, j.v FROM customers c LEFT JOIN JSON_TABLE(IF(c.id = 1, '[7]', '[]'), '$[*]' COLUMNS (v INT PATH '$')) AS j ON 1 WHERE c.id <= 2 ORDER BY c.id").await.unwrap();
    assert_eq!(got, ["1|7", "2|NULL"]);
    let err = run("SELECT * FROM JSON_TABLE('[1]', '$[*]' COLUMNS (a INT PATH '$'))")
        .await
        .unwrap_err();
    assert_eq!(err.protocol().mysql_errno, 4177);
}

/// The session's user variables for a test: a plain map.
#[derive(Debug, Default)]
struct TestVars(
    std::sync::Mutex<HashMap<String, (sprig_engine::row::Value, sprig_exec::ExprType)>>,
    std::sync::atomic::AtomicBool,
);

impl sprig_exec::UserVars for TestVars {
    fn get(&self, name: &str) -> Option<(sprig_engine::row::Value, sprig_exec::ExprType)> {
        self.0.lock().unwrap().get(name).cloned()
    }

    fn set(&self, name: &str, value: sprig_engine::row::Value, ty: sprig_exec::ExprType) {
        self.0.lock().unwrap().insert(name.to_owned(), (value, ty));
    }

    fn first_assignment(&self) -> bool {
        !self.1.swap(true, Ordering::Relaxed)
    }
}

#[tokio::test]
async fn user_variables_are_read_and_assigned_per_row() {
    use sprig_engine::row::Value;
    let (e, f, _d) = shop().await;
    let mut s = seed();
    let vars = Arc::new(TestVars::default());
    sprig_exec::UserVars::set(
        &*vars,
        "r",
        Value::Int(0),
        sprig_exec::ExprType::int(21, false),
    );
    s.session.info.user_vars = Some(vars.clone());
    let (got, _) = select_with(
        &e,
        &f,
        &s,
        "SELECT c.id, @r := @r + 1 AS rn FROM customers c WHERE c.id <= 3 ORDER BY c.id",
    )
    .await
    .unwrap();
    assert_eq!(got, ["1|1", "2|2", "3|3"]);
    assert_eq!(
        sprig_exec::UserVars::get(&*vars, "r").unwrap().0,
        Value::Int(3)
    );
    // A read has the type the variable had when the statement was planned.
    let (got, _) = select_with(&e, &f, &s, "SELECT @r := 'x', @r")
        .await
        .unwrap();
    assert_eq!(got, ["x|0"]);
    // A variable never set reads as NULL.
    let (got, _) = select_with(
        &e,
        &f,
        &s,
        "SELECT c.id FROM customers c WHERE c.id = @nope",
    )
    .await
    .unwrap();
    assert!(got.is_empty());
}

/// Shapes the change streams' inheritance probes use: an `ENUM` column
/// in a set operation (its member text), `FROM DUAL` inside a union and a
/// subquery, and a `JSON_TABLE` whose document reads the outer row.
#[tokio::test]
async fn enum_set_operations_dual_and_outer_json_table_run_on_the_engine() {
    let (e, f, _d) = shop().await;
    let run = |sql: &'static str| {
        let (e, f) = (&e, &f);
        async move { select(e, f, sql, &[]).await }
    };
    // `tier` is ENUM('gold', 'silver', 'none').
    let got = run("SELECT tier FROM customers WHERE id IN (1, 3) UNION ALL SELECT 'x' ORDER BY 1")
        .await
        .unwrap();
    assert_eq!(got, ["gold", "silver", "x"]);
    let got = run("SELECT id FROM customers WHERE id = 1 UNION ALL SELECT 7 FROM DUAL")
        .await
        .unwrap();
    assert_eq!(got, ["1", "7"]);
    let got = run("SELECT id FROM customers WHERE id IN (SELECT 2 FROM DUAL)")
        .await
        .unwrap();
    assert_eq!(got, ["2"]);
    let got = run(
        "SELECT c.id FROM customers c WHERE c.id <= 3 AND EXISTS (SELECT 1 FROM \
         JSON_TABLE(CONCAT('[', c.id, ']'), '$[*]' COLUMNS (v INT PATH '$')) AS j WHERE j.v = 2)",
    )
    .await
    .unwrap();
    assert_eq!(got, ["2"]);
}

/// An `ENUM` column a derived table or a common table expression reads,
/// computed with outside it, is read as its member text.
#[tokio::test]
async fn enum_columns_read_through_derived_tables_compare_as_text() {
    let (e, f, _d) = shop().await;
    let run = |sql: &'static str| {
        let (e, f) = (&e, &f);
        async move { select(e, f, sql, &[]).await }
    };
    // `tier` is ENUM('gold', 'silver', 'none'): ids 1..6 are silver,
    // none, gold, silver, none, gold.
    let got = run(
        "SELECT x.id FROM (SELECT id, tier FROM customers WHERE id <= 6) x \
         WHERE x.tier <> 'gold' ORDER BY x.id",
    )
    .await
    .unwrap();
    assert_eq!(got, ["1", "2", "4", "5"]);
    let got = run(
        "WITH c AS (SELECT c.id, c.tier FROM customers c WHERE c.id <= 6) \
         SELECT c.id FROM c WHERE c.tier IN ('none') ORDER BY 1",
    )
    .await
    .unwrap();
    assert_eq!(got, ["2", "5"]);
    // The derived table takes the name of the base table inside it.
    let got = run(
        "SELECT COUNT(*) FROM (SELECT c.id, c.tier FROM customers AS c WHERE c.id <= 6 \
         UNION ALL SELECT 99, 'ovr') c WHERE c.tier IS NULL OR c.tier <> 'silver'",
    )
    .await
    .unwrap();
    assert_eq!(got, ["5"]);
    // Passed straight through, the column is still the table's.
    let got = run("SELECT x.tier FROM (SELECT id, tier FROM customers) x WHERE x.id = 3")
        .await
        .unwrap();
    assert_eq!(got, ["gold"]);
    // Unqualified in a `FROM` of base tables, the name is the table's
    // even when a derived table elsewhere names a column the same.
    let got = run(
        "SELECT x.id FROM (SELECT id FROM customers WHERE tier <> 'gold' AND id <= 6) x \
         JOIN (SELECT id, tier FROM customers) y ON y.id = x.id ORDER BY x.id",
    )
    .await
    .unwrap();
    assert_eq!(got, ["1", "2", "4", "5"]);
}

/// `ANALYZE` runs the statement and answers its plan with what it read.
#[tokio::test]
async fn analyze_answers_the_plan_with_the_rows_read() {
    let (e, f, _d) = shop().await;
    let got = select(
        &e,
        &f,
        "EXPLAIN ANALYZE SELECT * FROM customers WHERE id <= 3",
        &[],
    )
    .await
    .unwrap();
    assert_eq!(got.len(), 1, "{got:?}");
    let cols: Vec<&str> = got[0].split('|').collect();
    assert_eq!(cols.len(), 13, "{got:?}");
    // `rows`, `r_rows`, `filtered`, `r_filtered`.
    assert_eq!(cols[9], "3.00", "{got:?}");
    assert_eq!(cols[11], "100.00", "{got:?}");
    let got = select(
        &e,
        &f,
        "EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM customers WHERE id <= 3",
        &[],
    )
    .await
    .unwrap();
    assert!(got[0].contains("\"r_rows\": 3"), "{got:?}");
    assert!(got[0].starts_with("{\n  \"query_optimization\""), "{got:?}");
}
