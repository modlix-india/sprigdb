//! The data specialist's turns, checked on what was sent.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::json;

use super::*;
use crate::audit::MemoryAudit;
use crate::eval::world::Recording;
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::tools::{Column, ProposalKind, ResultSet, ToolBody, ToolOutput, ToolSpec};

/// Rows only this test knows.
const SECRET_A: &str = "carol.secret@example.com";
const SECRET_B: &str = "182340.55";

#[derive(Default)]
struct Host {
    calls: Mutex<Vec<(String, Value)>>,
}

#[async_trait]
impl ToolHost for Host {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        self.calls
            .lock()
            .unwrap()
            .push((spec.name.to_owned(), input.clone()));
        match spec.name {
            "run_query" => Ok(ToolOutput {
                body: ToolBody::Rows(ResultSet {
                    columns: vec![
                        Column {
                            name: "email".into(),
                            ty: "varchar".into(),
                        },
                        Column {
                            name: "revenue".into(),
                            ty: "decimal".into(),
                        },
                    ],
                    rows: vec![vec![json!(SECRET_A), json!(SECRET_B)]],
                    row_count: 1,
                    truncated: false,
                }),
                sources: Vec::new(),
            }),
            "describe" => Err(SprigError::new(
                sprig_common::SprigCode::AdminRequestInvalid,
                "there is no table shop.orderz that the user can see",
            )),
            _ => Err(SprigError::internal("not in this test")),
        }
    }
}

fn cfg(share_data: bool) -> TurnConfig {
    TurnConfig {
        role: Role::Data,
        enabled: true,
        model: "m".into(),
        effort: None,
        max_output_tokens: 4000,
        max_tool_rounds: 12,
        timeout: Duration::from_secs(30),
        budget: crate::budget::Budget {
            tenant_tokens_day: 1_000_000,
            user_tokens_day: 100_000,
        },
        share_data,
        keep_payloads: false,
    }
}

fn usage() -> Usage {
    Usage {
        input_tokens: 10,
        output_tokens: 5,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

async fn turn(
    replies: Vec<Reply>,
    share_data: bool,
    system: Vec<Block>,
) -> (Arc<Recording>, Arc<Host>, Vec<TurnEvent>, TurnResult) {
    let p = Arc::new(Recording::new(Arc::new(ScriptedProvider::new(
        replies,
        usage(),
    ))));
    let host = Arc::new(Host::default());
    let agent = Agent::new(
        p.clone() as Arc<dyn Provider>,
        ToolRegistry::builtin(),
        Arc::new(MemoryAudit::default()) as Arc<dyn AuditStore>,
    )
    .showing_results();
    let mut ev = Vec::new();
    let r = agent
        .run(
            &cfg(share_data),
            TurnRequest {
                tenant: Some(TenantId(5)),
                user: UserId(9),
                conv: None,
                system,
                history: Vec::new(),
                message: "Revenue per customer?".into(),
                host: host.clone(),
                scrubber: None,
                cancel: None,
            },
            &mut ev,
        )
        .await
        .unwrap();
    (p, host, ev, r)
}

fn run_query(sql: &str) -> Reply {
    Reply::Tools(
        String::new(),
        vec![(
            "q1".into(),
            "run_query".into(),
            json!({"db": "shop", "sql": sql}),
        )],
    )
}

fn sent(p: &Recording) -> String {
    p.bodies()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn sharing_off_sends_the_shape_and_shows_the_user_every_row() {
    let (p, _, ev, _) = turn(
        vec![
            run_query("SELECT email, revenue FROM t"),
            Reply::Text("One row.".into()),
        ],
        false,
        crate::prompt::base(Role::Data),
    )
    .await;
    let body = sent(&p);
    assert!(
        !body.contains(SECRET_A) && !body.contains(SECRET_B),
        "{body}"
    );
    assert!(body.contains("rows_withheld") && body.contains("row_count"));
    assert!(
        body.contains("the user sees the whole result"),
        "the model is told the user has the rows"
    );
    // The user got the rows in full.
    let shown = ev.iter().find_map(|e| match e {
        TurnEvent::Result { result, .. } => Some(result.clone()),
        _ => None,
    });
    let shown = shown.expect("a result event");
    assert_eq!(shown.rows[0][0], json!(SECRET_A));
}

#[tokio::test]
async fn sharing_on_sends_the_rows() {
    let (p, _, ev, _) = turn(
        vec![
            run_query("SELECT email, revenue FROM t"),
            Reply::Text("One row.".into()),
        ],
        true,
        crate::prompt::base(Role::Data),
    )
    .await;
    let body = sent(&p);
    assert!(body.contains(SECRET_A) && body.contains(SECRET_B), "{body}");
    assert!(!body.contains("rows_withheld"));
    assert!(ev.iter().any(|e| matches!(e, TurnEvent::Result { .. })));
}

#[tokio::test]
async fn a_write_in_run_query_is_refused_before_the_host() {
    let (p, host, ev, _) = turn(
        vec![
            run_query("DELETE FROM orders WHERE id = 1"),
            run_query("UPDATE orders SET total = 0"),
            Reply::Text("I can't change data.".into()),
        ],
        true,
        crate::prompt::base(Role::Data),
    )
    .await;
    assert!(host.calls.lock().unwrap().is_empty(), "nothing ran");
    assert!(sent(&p).contains("propose changes with propose_sql"));
    assert!(
        ev.iter().any(
            |e| matches!(e, TurnEvent::ToolEnd { name, ok: false, .. } if name == "run_query")
        )
    );
    assert!(!ev.iter().any(|e| matches!(e, TurnEvent::Result { .. })));
}

#[tokio::test]
async fn a_tile_proposal_never_executes() {
    let chart =
        r#"{"mark":"bar","columns":[{"field":"region"}],"rows":[{"field":"revenue","agg":"sum"}]}"#;
    let (p, host, ev, r) = turn(
        vec![
            Reply::Tools(
                String::new(),
                vec![(
                    "t1".into(),
                    "propose_tile".into(),
                    json!({"title": "Revenue by region", "db": "shop",
                           "sql": "SELECT region, SUM(total) AS revenue FROM orders GROUP BY region",
                           "chart": chart}),
                )],
            ),
            Reply::Text("I proposed a bar chart; it hasn't been added.".into()),
        ],
        false,
        crate::prompt::base(Role::Data),
    )
    .await;
    assert!(
        host.calls.lock().unwrap().is_empty(),
        "the tile's SQL never ran"
    );
    assert_eq!(r.proposals.len(), 1);
    assert_eq!(r.proposals[0].kind, ProposalKind::Tile);
    assert!(!ev.iter().any(|e| matches!(e, TurnEvent::Result { .. })));
    assert!(sent(&p).contains("has not run"));
}

#[tokio::test]
async fn the_glossary_reaches_the_model_and_errors_say_what_to_do() {
    let mut system = crate::prompt::base(Role::Data);
    let terms = vec![crate::glossary::Entry {
        term: "revenue".into(),
        status: crate::glossary::Status::Accepted,
        definition: "Order totals without refunds".into(),
        sql: Some("SUM(total) WHERE status <> 'refunded'".into()),
        tables: vec!["orders".into()],
        author_id: 1,
        author: "ann".into(),
        accepted_by: Some(1),
        proposal_id: None,
        updated_ms: 0,
    }];
    system.push(Block::text(crate::glossary::prompt(&terms).unwrap()));
    let (p, _, _, _) = turn(
        vec![
            Reply::Tools(
                String::new(),
                vec![(
                    "d1".into(),
                    "describe".into(),
                    json!({"db": "shop", "table": "orderz"}),
                )],
            ),
            Reply::Text("Which table?".into()),
        ],
        false,
        system,
    )
    .await;
    let body = sent(&p);
    assert!(body.contains("SUM(total) WHERE status <> 'refunded'"));
    assert!(body.contains("relationships"), "the data prompt names it");
    // The failed describe tells the model how to recover.
    assert!(
        body.contains("What to do: call catalog with the database"),
        "{body}"
    );
}
