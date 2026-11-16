//! Analytic reads against a real MariaDB: two shard schemas, a scratch
//! connection that really runs the probe and the finishing step, and an
//! unsharded reference schema with the same rows; every answer (values
//! and column types) must equal the reference's.
//!
//! ```sh
//! SPRIG_TEST_ANALYTICS_URL=mysql://root:sprig-dev-root@127.0.0.1:35313 \
//!   cargo test -p sprig-router --lib analytics::live_tests -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::panic, clippy::too_many_lines)]

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_proto_mysql::ColumnDef;

use super::*;
use crate::exec::{ShardOk, ShardRows};
use crate::gather::{GatherLimits, GatherTable, Scratch};
use crate::plan::{ShardAt, Target};
use crate::reject::{Reason, Rejection};

/// The schema the statements name (empty; the scratch connection's
/// temporary tables live in it), the shards and the reference.
const VIEW: &str = "r3s_an_v";
const SHARDS: [&str; 2] = ["r3s_an_s1", "r3s_an_s2"];
const REFERENCE: &str = "r3s_an_ref";

fn column(c: &mysql_async::Column) -> ColumnDef {
    ColumnDef {
        name: Bytes::copy_from_slice(c.name_ref()),
        org_name: Bytes::copy_from_slice(c.org_name_ref()),
        charset: c.character_set(),
        length: c.column_length(),
        col_type: c.column_type() as u8,
        flags: c.flags().bits(),
        decimals: c.decimals(),
        ..ColumnDef::default()
    }
}

/// Runs `sql` in the text protocol: column definitions and rows as bytes
/// (`None` for an OK packet).
async fn text(c: &mut Conn, sql: &str) -> Result<Option<(Vec<ColumnDef>, Vec<Row>)>> {
    let mut r = c
        .query_iter(sql)
        .await
        .map_err(|e| SprigError::internal(e.to_string()))?;
    let cols: Vec<ColumnDef> = r
        .columns()
        .map(|cs| cs.iter().map(column).collect())
        .unwrap_or_default();
    let rows: Vec<mysql_async::Row> = r
        .collect()
        .await
        .map_err(|e| SprigError::internal(e.to_string()))?;
    if cols.is_empty() {
        return Ok(None);
    }
    let rows = rows
        .into_iter()
        .map(|row| {
            row.unwrap()
                .into_iter()
                .map(|v| match v {
                    mysql_async::Value::NULL => None,
                    mysql_async::Value::Bytes(b) => Some(Bytes::from(b)),
                    other => Some(Bytes::from(other.as_sql(true).into_bytes())),
                })
                .collect()
        })
        .collect();
    Ok(Some((cols, rows)))
}

fn outcome(r: Option<(Vec<ColumnDef>, Vec<Row>)>) -> ShardOutcome {
    match r {
        Some((columns, rows)) => ShardOutcome::Rows(ShardRows {
            columns,
            rows,
            warnings: 0,
        }),
        None => ShardOutcome::Ok(ShardOk::default()),
    }
}

/// One connection's buffered answer, for streams and the scratch.
struct Live {
    c: Conn,
    rows: std::collections::VecDeque<Row>,
    warnings: u16,
}

#[async_trait::async_trait]
impl ShardStream for Live {
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        let r = text(&mut self.c, sql).await?;
        self.warnings = self.c.get_warnings();
        Ok(r.map(|(cols, rows)| {
            self.rows = rows.into();
            cols
        }))
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        Ok(self.rows.pop_front())
    }
    fn warnings(&self) -> u16 {
        self.warnings
    }
}

#[async_trait::async_trait]
impl Scratch for Live {
    fn cluster(&self) -> &'static str {
        "c1"
    }
    async fn run(&mut self, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        Ok(outcome(text(&mut self.c, sql).await?))
    }
    async fn start(&mut self, sql: &str) -> Result<Option<Vec<ColumnDef>>> {
        ShardStream::start(self, sql).await
    }
    async fn next_row(&mut self) -> Result<Option<Row>> {
        Ok(self.rows.pop_front())
    }
    fn warnings(&self) -> u16 {
        self.warnings
    }
    async fn finish(self: Box<Self>, _clean: bool) {}
}

struct LiveBackend {
    pool: Pool,
    scratches: usize,
}

impl LiveBackend {
    async fn conn(&self, db: &str) -> Conn {
        let mut c = self.pool.get_conn().await.unwrap();
        c.query_drop(format!("USE `{db}`")).await.unwrap();
        c.query_drop("SET NAMES utf8mb4").await.unwrap();
        c
    }
}

#[async_trait::async_trait]
impl ShardBackend for LiveBackend {
    async fn run(&mut self, t: &Target, sql: &str, _b: &mut Budget) -> Result<ShardOutcome> {
        let mut c = self.conn(&t.at.schema).await;
        Ok(outcome(text(&mut c, sql).await?))
    }
    fn in_transaction(&self) -> bool {
        false
    }
    async fn open_stream(&mut self, _cluster: &str) -> Result<Option<Box<dyn ShardStream>>> {
        Ok(Some(Box::new(Live {
            c: self.conn(VIEW).await,
            rows: std::collections::VecDeque::new(),
            warnings: 0,
        })))
    }
    async fn open_scratch(&mut self) -> Result<Option<Box<dyn Scratch>>> {
        self.scratches += 1;
        Ok(Some(Box::new(Live {
            c: self.conn(VIEW).await,
            rows: std::collections::VecDeque::new(),
            warnings: 0,
        })))
    }
    fn gather_limits(&self) -> GatherLimits {
        GatherLimits {
            max_rows: 100,
            max_concurrent: 2,
            analytics: AnalyticsLimits {
                mode: AnalyticsMode::Prefer,
                ..AnalyticsLimits::default()
            },
        }
    }
}

const DDL: [&str; 2] = [
    "CREATE TABLE customers (cid INT NOT NULL PRIMARY KEY, name VARCHAR(20) NULL, \
     region VARCHAR(20) COLLATE utf8mb4_general_ci NULL)",
    "CREATE TABLE orders (oid INT NOT NULL PRIMARY KEY, cid INT NOT NULL, \
     amount DECIMAL(10,2) NULL, qty INT NULL)",
];

async fn setup(p: &Pool) {
    let mut c = p.get_conn().await.unwrap();
    for s in [VIEW, REFERENCE, SHARDS[0], SHARDS[1]] {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE `{s}` CHARACTER SET utf8mb4"))
            .await
            .unwrap();
    }
    for s in [REFERENCE, SHARDS[0], SHARDS[1]] {
        for d in DDL {
            c.query_drop(format!("USE `{s}`")).await.unwrap();
            c.query_drop(d).await.unwrap();
        }
    }
    let regions = ["north", "south", "east", "west", "up", "down"];
    for cid in 1..=60i64 {
        let shard = SHARDS[usize::try_from(cid % 2).unwrap()];
        let region = regions[usize::try_from(cid % 6).unwrap()];
        let name = if cid % 7 == 0 {
            "NULL".to_owned()
        } else {
            format!("'n{cid}'")
        };
        for s in [REFERENCE, shard] {
            c.query_drop(format!(
                "INSERT INTO `{s}`.customers VALUES ({cid}, {name}, '{region}')"
            ))
            .await
            .unwrap();
        }
    }
    for oid in 1..=400i64 {
        let cid = 1 + (oid * 37) % 60;
        let shard = SHARDS[usize::try_from(oid % 2).unwrap()];
        let amount = if oid % 11 == 0 {
            "NULL".to_owned()
        } else {
            format!("{}.{:02}", (oid * 13) % 500 - 100, oid % 100)
        };
        let qty = (oid * 7) % 9;
        for s in [REFERENCE, shard] {
            c.query_drop(format!(
                "INSERT INTO `{s}`.orders VALUES ({oid}, {cid}, {amount}, {qty})"
            ))
            .await
            .unwrap();
        }
    }
}

fn plan(sql: &str) -> GatherPlan {
    let src = |i: u32, s: &str| Target {
        cluster: "c1".into(),
        shard: i,
        sql: String::new(),
        at: ShardAt {
            group: 1,
            db: 1,
            lo: 0,
            schema: s.into(),
        },
    };
    let t = |name: &str| GatherTable {
        schema: VIEW.into(),
        name: name.into(),
        sources: vec![src(1, SHARDS[0]), src(2, SHARDS[1])],
        alias: name.into(),
        filter: None,
        shards_total: 2,
    };
    GatherPlan {
        tables: vec![t("customers"), t("orders")],
        sql: sql.into(),
        reads_unsharded: false,
        fallback: Rejection::new(Reason::JoinCondition, "test"),
    }
}

/// Types and printed values, rows sorted unless `ordered`.
type Answer = (Vec<(u8, u8)>, Vec<Vec<Option<Vec<u8>>>>);

fn normal(cols: &[ColumnDef], rows: Vec<Row>, ordered: bool) -> Answer {
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = rows
        .into_iter()
        .map(|r| r.into_iter().map(|v| v.map(|b| b.to_vec())).collect())
        .collect();
    if !ordered {
        rows.sort();
    }
    (
        cols.iter().map(|c| (c.col_type, c.decimals)).collect(),
        rows,
    )
}

#[tokio::test]
async fn finished_statements_match_mariadb() {
    let Ok(url) = std::env::var("SPRIG_TEST_ANALYTICS_URL") else {
        eprintln!("skipped: set SPRIG_TEST_ANALYTICS_URL to a MariaDB of the test's own");
        return;
    };
    let pool = Pool::new(
        OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(0, 8).unwrap())
                .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
        ),
    );
    setup(&pool).await;
    // (statement, ordered): HAVING, expressions over aggregates and grouped
    // columns, aliases in HAVING and ORDER BY, functions, CASE, LIMIT.
    let corpus: Vec<(&str, bool)> = vec![
        (
            "SELECT c.region, COUNT(*) FROM customers c JOIN orders o ON o.cid = c.cid \
             GROUP BY c.region HAVING COUNT(*) > 60",
            false,
        ),
        (
            "SELECT c.region, SUM(o.amount) * 2 AS dbl, COUNT(*) + 1, ROUND(AVG(o.qty), 1) \
             FROM customers c JOIN orders o ON o.cid = c.cid WHERE o.qty > 1 \
             GROUP BY c.region HAVING dbl > 10 AND COUNT(*) >= 2 \
             ORDER BY SUM(o.amount) DESC LIMIT 3",
            true,
        ),
        (
            "SELECT o.cid, SUM(o.amount) / COUNT(*) AS per, MAX(o.qty) - MIN(o.qty) AS spread \
             FROM orders o GROUP BY o.cid HAVING MAX(o.qty) > 6 ORDER BY per DESC, o.cid",
            true,
        ),
        (
            "SELECT c.region, CASE WHEN SUM(o.qty) > 300 THEN 'big' ELSE 'small' END AS size, \
             CONCAT(c.region, ':', COUNT(DISTINCT o.cid)) FROM customers c \
             LEFT JOIN orders o ON o.cid = c.cid GROUP BY c.region ORDER BY 1",
            false,
        ),
        (
            "SELECT SUM(o.amount) * 100 / SUM(o.qty), COUNT(o.amount) FROM orders o \
             JOIN customers c ON c.cid = o.cid WHERE c.region IN ('north', 'south')",
            false,
        ),
        (
            "SELECT c.name, COUNT(*) AS n FROM customers c JOIN orders o ON o.cid = c.cid \
             GROUP BY c.name HAVING n BETWEEN 5 AND 8 AND MIN(o.oid) < 200 ORDER BY n DESC, c.name",
            true,
        ),
    ];
    let mut be = LiveBackend {
        pool: pool.clone(),
        scratches: 0,
    };
    let mut reference = pool.get_conn().await.unwrap();
    reference
        .query_drop(format!("USE `{REFERENCE}`"))
        .await
        .unwrap();
    reference.query_drop("SET NAMES utf8mb4").await.unwrap();
    let mut finished = 0;
    for (sql, ordered) in &corpus {
        let q = translate::translate(
            sql,
            &[
                TableCols {
                    schema: VIEW.into(),
                    name: "customers".into(),
                    columns: vec!["cid".into(), "name".into(), "region".into()],
                },
                TableCols {
                    schema: VIEW.into(),
                    name: "orders".into(),
                    columns: vec!["oid".into(), "cid".into(), "amount".into(), "qty".into()],
                },
            ],
        )
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(q.finish.is_some(), "{sql}: not a finished statement");
        let mut budget = Budget::mb(16);
        let got = execute(&plan(sql), &mut be, &mut budget, None)
            .await
            .unwrap();
        let Outcome::Done(Output::Rows { columns, rows, .. }) = got else {
            panic!("{sql}: {got:?}");
        };
        let (rc, rr) = text(&mut reference, sql).await.unwrap().unwrap();
        let a = normal(&columns, rows, *ordered);
        let b = normal(&rc, rr, *ordered);
        assert_eq!(a, b, "{sql}");
        eprintln!("ok ({} rows): {sql}", a.1.len());
        finished += 1;
    }
    // A shape the finishing step refuses still falls back.
    let mut budget = Budget::mb(16);
    let r = execute(
        &plan("SELECT c.region, SUM(o.amount * o.qty) FROM customers c JOIN orders o ON o.cid = c.cid GROUP BY c.region"),
        &mut be,
        &mut budget,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(r, Outcome::Refused(_)), "{r:?}");
    eprintln!("{finished} finished statements equal MariaDB's answers");
    drop(reference);
    let mut c = pool.get_conn().await.unwrap();
    for s in [VIEW, REFERENCE, SHARDS[0], SHARDS[1]] {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{s}`"))
            .await
            .unwrap();
    }
    drop(c);
    pool.disconnect().await.unwrap();
}
