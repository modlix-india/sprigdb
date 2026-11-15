use std::collections::HashSet;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use serde_json::json;

use super::*;

fn col(name: &str, ty: &str) -> GenColumn {
    GenColumn {
        name: name.into(),
        data_type: ty.split(['(', ' ']).next().unwrap().into(),
        column_type: ty.into(),
        ..GenColumn::default()
    }
}

const TODAY: i64 = 20_772; // 2026-11-15

fn orders() -> GenTable {
    GenTable {
        db: "shop".into(),
        table: "orders".into(),
        columns: vec![
            GenColumn {
                auto_increment: true,
                unique: true,
                ..col("id", "bigint")
            },
            col("customer_id", "bigint"),
            GenColumn {
                unique: true,
                ..col("ref", "varchar(12)")
            },
            col("qty", "int"),
            col("total", "decimal(10,2)"),
            col("status", "enum('new','paid','shipped')"),
            GenColumn {
                nullable: true,
                ..col("note", "text")
            },
            col("placed_at", "datetime"),
            GenColumn {
                generated: true,
                ..col("total_cents", "bigint")
            },
        ],
        unique_keys: vec![],
        foreign_keys: vec![ForeignKey {
            columns: vec!["customer_id".into()],
            parent: "customers".into(),
            keys: vec![vec![json!("3")], vec![json!(5)], vec![json!(8)]],
        }],
        checks: vec![
            "`qty` > 0 and `qty` <= 9".into(),
            "`total` between 10 and 20".into(),
            "weird_fn(`note`)".into(),
        ],
    }
}

#[test]
fn rows_follow_the_schema() {
    let t = orders();
    let g = generate(&t, 400, &[], 7, TODAY).unwrap();
    assert_eq!(
        g.columns,
        [
            "customer_id",
            "ref",
            "qty",
            "total",
            "status",
            "note",
            "placed_at"
        ]
    );
    assert_eq!(g.rows.len(), 400);
    let refs: HashSet<&String> = g.rows.iter().map(|r| &r[1]).collect();
    assert_eq!(refs.len(), 400, "unique refs");
    for r in &g.rows {
        assert!(["3", "5", "8"].contains(&r[0].as_str()), "fk {}", r[0]);
        assert!(r[1].len() <= 14, "ref within varchar(12): {}", r[1]);
        let q: i64 = r[2].parse().unwrap();
        assert!((1..=9).contains(&q), "qty {q}");
        let tot: f64 = r[3].parse().unwrap();
        assert!((10.0..=20.0).contains(&tot), "total {tot}");
        assert!(["'new'", "'paid'", "'shipped'"].contains(&r[4].as_str()));
        assert!(
            r[6].as_str() >= "'2024-11-15" && r[6].as_str() <= "'2026-11-16",
            "{}",
            r[6]
        );
    }
    assert_eq!(g.checks_used.len(), 2);
    assert_eq!(g.checks_unknown.len(), 1);
    assert_eq!(g.skipped.len(), 2);
    // Same seed, same rows.
    assert_eq!(g, generate(&t, 400, &[], 7, TODAY).unwrap());
    let st = g.statements("shop", "orders");
    assert_eq!(st.len(), 1);
    assert!(st[0].starts_with("INSERT INTO `shop`.`orders` (`customer_id`, `ref`"));
}

#[test]
fn hints_and_names_choose_values() {
    let t = GenTable {
        db: "crm".into(),
        table: "customers".into(),
        columns: vec![
            GenColumn {
                unique: true,
                ..col("email", "varchar(255)")
            },
            col("first_name", "varchar(40)"),
            col("country", "char(2)"),
            col("tier", "varchar(10)"),
            GenColumn {
                nullable: true,
                ..col("phone", "varchar(32)")
            },
            col("dob", "date"),
        ],
        ..GenTable::default()
    };
    let hints = hints_of(Some(&json!([
        "tier: one_of, values=free|pro",
        "phone: , nulls=100",
        {"column": "email", "domain": "test.example"},
    ])));
    assert_eq!(hints[0].values, ["free", "pro"]);
    assert_eq!(hints[1].null_percent, Some(100));
    assert!(check_hints(Some(&json!(["total: decimal, min=5, max=500"]))).is_ok());
    assert!(check_hints(Some(&json!(["total decimal"]))).is_err());
    assert!(check_hints(Some(&json!(["total: decimal, colour=red"]))).is_err());
    assert!(check_hints(Some(&json!(["total: banana"]))).is_err());
    let g = generate(&t, 50, &hints, 1, TODAY).unwrap();
    let mails: HashSet<&String> = g.rows.iter().map(|r| &r[0]).collect();
    assert_eq!(mails.len(), 50);
    for r in &g.rows {
        assert!(r[0].ends_with("@test.example'"), "{}", r[0]);
        assert_eq!(r[2].len(), 4, "two letters: {}", r[2]);
        assert!(r[3] == "'free'" || r[3] == "'pro'");
        assert_eq!(r[4], "NULL");
        assert!(
            r[5].as_str() < "'2007" && r[5].as_str() > "'1949",
            "{}",
            r[5]
        );
    }
    let s = g.summary(&t, 1, 100).to_string();
    for r in &g.rows {
        for v in r {
            if v.len() > 6 {
                assert!(!s.contains(v.trim_matches('\'')), "{v} is in the summary");
            }
        }
    }
}

#[test]
fn impossible_requests_are_refused() {
    let mut t = orders();
    t.foreign_keys[0].keys.clear();
    let e = generate(&t, 5, &[], 1, TODAY).unwrap_err();
    assert!(e.contains("generate rows for customers first"), "{e}");
    let t = GenTable {
        db: "d".into(),
        table: "t".into(),
        columns: vec![GenColumn {
            unique: true,
            ..col("level", "enum('a','b')")
        }],
        ..GenTable::default()
    };
    assert!(
        generate(&t, 3, &[], 1, TODAY)
            .unwrap_err()
            .contains("only 2")
    );
    assert!(generate(&t, MAX_ROWS + 1, &[], 1, TODAY).is_err());
    assert!(check_hints(Some(&json!([{"column": "x", "kind": "one_of"}]))).is_err());
    assert!(check_hints(Some(&json!([{"column": "x", "null_percent": 101}]))).is_err());
}

#[test]
fn dates_round_trip() {
    for d in [-1000, 0, 365, TODAY, 40_000] {
        let (y, m, dd) = civil_from_days(d);
        assert_eq!(days_from_civil(y, m, dd), d);
    }
    assert_eq!(date_text(TODAY), "2026-11-15");
    assert_eq!(members("enum('a','it''s')"), ["'a'", "'it''s'"]);
}

// ---- live ----

fn rows_of(rs: Vec<mysql_async::Row>) -> Vec<Row> {
    rs.into_iter()
        .map(|r| {
            let cols = r.columns();
            let mut m = Row::new();
            for (i, c) in cols.iter().enumerate() {
                let v: Option<String> = r.get_opt(i).and_then(Result::ok).flatten();
                m.insert(
                    c.name_str().to_ascii_lowercase(),
                    v.map_or(Value::Null, Value::String),
                );
            }
            m
        })
        .collect()
}

async fn shape(conn: &mut mysql_async::Conn, db: &str, table: &str) -> GenTable {
    let q = shape_queries(db, table);
    let mut parts = Vec::new();
    for s in &q {
        let rs: Vec<mysql_async::Row> = conn.query(s.as_str()).await.unwrap();
        parts.push(rows_of(rs));
    }
    let (mut t, fks) =
        GenTable::from_parts(db, table, &parts[0], &parts[1], &parts[2], &parts[3]).unwrap();
    for (i, f) in fks.iter().enumerate() {
        let rs: Vec<mysql_async::Row> = conn.query(f.keys_query()).await.unwrap();
        t.foreign_keys[i].keys = rows_of(rs)
            .into_iter()
            .map(|m| {
                f.ref_columns
                    .iter()
                    .map(|c| m[&c.to_ascii_lowercase()].clone())
                    .collect()
            })
            .collect();
    }
    if let Some(q) = t.max_query() {
        let rs: Vec<mysql_async::Row> = conn.query(q).await.unwrap();
        let row = rs.into_iter().next().unwrap();
        for (i, c) in t.unique_ints().iter().enumerate() {
            let v: Option<i64> = row.get_opt(i).and_then(Result::ok).flatten();
            if let Some(col) = t.columns.iter_mut().find(|x| &x.name == c) {
                col.max_existing = v;
            }
        }
    }
    t
}

/// Live: `SPRIG_TEST_MARIADB_SINGLE` (for example
/// `mysql://root:sprig-dev-root@127.0.0.1:13301`). Generated rows go into
/// tables with foreign keys, unique keys and CHECKs in the schema
/// `ai_dba2_gen_<pid>`, which MariaDB enforces; dropped afterwards.
#[tokio::test]
async fn live_generated_rows_satisfy_mariadb() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
        .pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 2).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        );
    let pool = mysql_async::Pool::new(opts);
    let db = format!("ai_dba2_gen_{}", std::process::id());
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {db}")).await.unwrap();
    let run = async {
        c.query_drop(format!(
            "CREATE TABLE {db}.customers (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
             email VARCHAR(255) NOT NULL UNIQUE, full_name VARCHAR(120) NOT NULL, \
             country CHAR(2) NOT NULL, tier ENUM('free','pro') NOT NULL, \
             credit INT NOT NULL CHECK (credit BETWEEN 0 AND 500), \
             created_at DATETIME NOT NULL)"
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "CREATE TABLE {db}.orders (id BIGINT PRIMARY KEY, \
             customer_id BIGINT NOT NULL, ref VARCHAR(20) NOT NULL, \
             qty INT NOT NULL CHECK (qty > 0 AND qty < 10), total DECIMAL(10,2) NOT NULL, \
             status VARCHAR(16) NULL, UNIQUE KEY by_ref (customer_id, ref), \
             FOREIGN KEY (customer_id) REFERENCES {db}.customers (id))"
        ))
        .await
        .unwrap();
        // A row already there: generated keys must not collide with it.
        c.query_drop(format!(
            "INSERT INTO {db}.customers VALUES (1, 'x@example.com', 'X', 'GB', 'free', 1, NOW())"
        ))
        .await
        .unwrap();
        c.query_drop(format!(
            "INSERT INTO {db}.orders VALUES (900, 1, 'r', 1, 1, NULL)"
        ))
        .await
        .unwrap();

        let t = shape(&mut c, &db, "customers").await;
        assert!(t.columns[0].auto_increment && t.columns[1].unique);
        assert_eq!(t.checks.len(), 1, "{:?}", t.checks);
        let g = generate(&t, 300, &[], 11, TODAY).unwrap();
        for s in g.statements(&db, "customers") {
            c.query_drop(s).await.unwrap();
        }
        let t = shape(&mut c, &db, "orders").await;
        assert_eq!(t.foreign_keys[0].keys.len(), 301);
        assert_eq!(
            t.unique_keys,
            vec![vec!["customer_id".to_owned(), "ref".to_owned()]]
        );
        assert_eq!(t.columns[0].max_existing, Some(900));
        let g = generate(&t, 2000, &[], 12, TODAY).unwrap();
        assert_eq!(g.statements(&db, "orders").len(), 4);
        for s in g.statements(&db, "orders") {
            c.query_drop(s).await.unwrap();
        }
        let n: Option<(u64, u64, u64)> = c
            .query_first(format!(
                "SELECT (SELECT COUNT(*) FROM {db}.customers), (SELECT COUNT(*) FROM {db}.orders), \
                 (SELECT COUNT(*) FROM {db}.orders o LEFT JOIN {db}.customers k ON k.id = o.customer_id WHERE k.id IS NULL)"
            ))
            .await
            .unwrap();
        assert_eq!(n, Some((301, 2001, 0)));
    };
    let done = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(run)).await;
    c.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
    if let Err(p) = done {
        std::panic::resume_unwind(p);
    }
}

// ---- through a turn ----

mod turn {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::{Value, json};
    use sprig_common::ids::{TenantId, UserId};
    use sprig_common::{Result, SprigError};

    use super::{TODAY, orders};
    use crate::audit::{AuditStore, ConvId, MemoryAudit};
    use crate::budget::Budget;
    use crate::provider::scripted::{Reply, ScriptedProvider};
    use crate::provider::{Effort, Provider, Usage};
    use crate::tools::testdata::generate;
    use crate::tools::{ProposalCheck, ToolHost, ToolOutput, ToolRegistry, ToolSpec};
    use crate::{Agent, Role, TurnConfig, TurnRequest};

    /// Generates rows as the gateway does; refuses logic that "doesn't
    /// compile".
    struct GenHost;

    #[async_trait]
    impl ToolHost for GenHost {
        async fn call(&self, _spec: &ToolSpec, _input: &Value) -> Result<ToolOutput> {
            Err(SprigError::internal("no reads here"))
        }

        async fn check_proposal(&self, spec: &ToolSpec, input: &Value) -> Result<ProposalCheck> {
            match spec.name {
                "generate_rows" => {
                    let t = orders();
                    let n = usize::try_from(input["n"].as_u64().unwrap()).unwrap();
                    let g = generate(&t, n, &[], 3, TODAY).unwrap();
                    let st = g.statements("shop", "orders");
                    let sql = st.join(";\n");
                    Ok(ProposalCheck {
                        checks: Some(g.summary(&t, st.len(), sql.len())),
                        generated: Some(json!({"sql": sql})),
                    })
                }
                "propose_logic" => Err(SprigError::internal(
                    "it doesn't compile: Arguments.qty is not a declared parameter",
                )),
                _ => Ok(ProposalCheck::default()),
            }
        }
    }

    #[tokio::test]
    async fn generated_rows_reach_the_user_never_the_provider() {
        let usage = Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        let p = Arc::new(ScriptedProvider::new(
            [
                Reply::Tools(
                    String::new(),
                    vec![
                        (
                            "g1".into(),
                            "generate_rows".into(),
                            json!({"db": "shop", "table": "orders", "n": 300, "rules": "", "columns": []}),
                        ),
                        (
                            "l1".into(),
                            "propose_logic".into(),
                            json!({"db": "shop", "sql": "CREATE FUNCTION f LANGUAGE kirun AS '{}'", "why": "x"}),
                        ),
                    ],
                ),
                Reply::Text("Proposed 300 rows; they have not run.".into()),
            ],
            usage,
        ));
        let audit = Arc::new(MemoryAudit::default());
        let agent = Agent::new(
            Arc::clone(&p) as Arc<dyn Provider>,
            ToolRegistry::builtin(),
            Arc::clone(&audit) as Arc<dyn AuditStore>,
        );
        let cfg = TurnConfig {
            role: Role::Dba,
            enabled: true,
            model: "m".into(),
            effort: Some(Effort::Medium),
            max_output_tokens: 1000,
            max_tool_rounds: 4,
            timeout: std::time::Duration::from_secs(30),
            budget: Budget {
                tenant_tokens_day: 1_000_000,
                user_tokens_day: 1_000_000,
            },
            share_data: false,
            keep_payloads: true,
        };
        let req = TurnRequest {
            tenant: Some(TenantId(5)),
            user: UserId(9),
            conv: Some(ConvId(1)),
            system: crate::prompt::base(Role::Dba),
            history: Vec::new(),
            message: "300 test orders please".into(),
            host: Arc::new(GenHost),
            scrubber: None,
            cancel: None,
        };
        let mut ev = Vec::new();
        let r = agent.run(&cfg, req, &mut ev).await.unwrap();
        // One proposal: the rows; the logic that failed its check went back.
        assert_eq!(r.proposals.len(), 1);
        let gp = &r.proposals[0];
        let sql = gp.generated.as_ref().unwrap()["sql"].as_str().unwrap();
        assert!(sql.starts_with("INSERT INTO `shop`.`orders`"));
        assert_eq!(gp.checks.as_ref().unwrap()["rows"], 300);
        // What the provider got: the summary and the compile error, never
        // a generated value.
        let sent: String = p
            .requests()
            .iter()
            .map(|q| serde_json::to_string(&q.messages).unwrap())
            .collect();
        assert!(sent.contains("existing keys of customers"));
        assert!(sent.contains("is not a declared parameter"));
        let g = generate(&orders(), 300, &[], 3, TODAY).unwrap();
        let mut checked = 0;
        for row in &g.rows {
            for v in &row[1..] {
                let v = v.trim_matches('\'');
                if v.len() >= 6 && !v.chars().all(|c| c.is_ascii_digit() || c == '.') {
                    assert!(!sent.contains(v), "{v} reached the provider");
                    checked += 1;
                }
            }
        }
        assert!(checked > 300, "{checked}");
    }
}
