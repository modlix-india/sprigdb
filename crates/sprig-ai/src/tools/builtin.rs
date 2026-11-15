//! The built-in tools.
//!
//! | Tool | Roles | Effect | Shares |
//! |---|---|---|---|
//! | `search_docs` | admin, dba, data | read | docs |
//! | `catalog`, `describe` | dba, data | read | schema |
//! | `workload`, `fingerprint` | dba | read | stats |
//! | `explain` | dba, data | read | stats |
//! | `indexes`, `auto_indexes` | dba | read | schema |
//! | `settings` | dba, admin | read | schema |
//! | `shards`, `advisor_runs` | dba | read | stats |
//! | `approvals` | admin, dba, data | read | schema |
//! | `column_stats` | dba, data | read | stats |
//! | `run_query` | data | read | rows |
//! | `test_policy` | dba | read | stats |
//! | `compile_logic` | dba | read | schema |
//! | `sensitive_columns` | dba | read | stats (pattern counts only with sharing) |
//! | `check_ddl`, `migration_check` | dba | read | schema |
//! | `decision_log` | admin, dba | read | stats |
//! | `metrics_trend` | admin | read | stats |
//! | `propose_sql` | dba, data | propose | |
//! | `propose_recommendation` | dba, admin | propose | |
//! | `propose_tile` | data, dba | propose | |
//! | `propose_glossary` | data, dba | propose | |
//! | `generate_rows` | dba | propose | |
//! | `propose_policy`, `propose_logic` | dba | propose | |
//!
//! The triage role has no tools.

use serde_json::{Map, Value, json};

use super::{Effect, ProposalKind, Shares, ToolSpec};
use crate::role::Role;
use crate::sql_guard;

const ALL: &[Role] = &[Role::Admin, Role::Dba, Role::Data];
const DBA_DATA: &[Role] = &[Role::Dba, Role::Data];
const DBA: &[Role] = &[Role::Dba];
const DATA: &[Role] = &[Role::Data];
const DBA_ADMIN: &[Role] = &[Role::Dba, Role::Admin];
const ADMIN: &[Role] = &[Role::Admin];

/// The longest SQL or source text a tool takes.
pub const MAX_TEXT: usize = 64 * 1024;

/// The longest schema `migration_check` takes in one call.
pub const MAX_MIGRATION_TEXT: usize = 512 * 1024;

/// The chart marks a tile may use.
pub const MARKS: &[&str] = &[
    "bar",
    "line",
    "area",
    "point",
    "pie",
    "donut",
    "waterfall",
    "histogram",
    "box",
    "heatmap",
    "highlight-table",
    "treemap",
    "funnel",
    "radar",
    "sankey",
    "kpi",
    "gauge",
    "bullet",
    "gantt",
    "text-table",
    "pivot",
    "map-point",
    "map-filled",
];

fn obj(props: &[(&str, Value)], required: &[&str]) -> Value {
    let mut p = Map::new();
    for (k, v) in props {
        p.insert((*k).to_owned(), v.clone());
    }
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": Value::Object(p),
        "required": required,
    })
}

fn s(d: &str) -> Value {
    json!({"type": "string", "description": d})
}

fn int(d: &str) -> Value {
    json!({"type": "integer", "description": d})
}

fn one_of(vals: &[&str], d: &str) -> Value {
    json!({"type": "string", "enum": vals, "description": d})
}

fn strs(d: &str) -> Value {
    json!({"type": "array", "items": {"type": "string"}, "description": d})
}

fn db() -> Value {
    s("The database (schema) name")
}

fn spec(
    name: &'static str,
    description: &'static str,
    input: Value,
    effect: Effect,
    roles: &'static [Role],
    shares: Shares,
) -> ToolSpec {
    ToolSpec {
        name,
        description,
        input,
        effect,
        roles,
        shares,
    }
}

/// Every built-in tool.
#[must_use]
#[allow(clippy::too_many_lines)] // one table
pub fn specs() -> Vec<ToolSpec> {
    use Effect::{Propose, Read};
    vec![
        spec(
            "search_docs",
            "Search SprigDB's own documentation for this release. Call it before answering any question about how SprigDB works, its settings, errors, SQL extensions or operations, and cite the pages you use.",
            obj(
                &[
                    ("query", s("What to look for, in words")),
                    (
                        "audience",
                        one_of(&["user", "dba", "operator"], "Only pages for this audience"),
                    ),
                ],
                &["query"],
            ),
            Read,
            ALL,
            Shares::Docs,
        ),
        spec(
            "catalog",
            "List the databases the user can see or, with db, that database's tables, views and collections with their kinds and approximate sizes. Call it to find what exists before writing a query.",
            obj(&[("db", db())], &[]),
            Read,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "describe",
            "Describe one table: columns, types, keys, indexes, distribution and comments. Call it before writing SQL that uses the table.",
            obj(&[("db", db()), ("table", s("The table"))], &["db", "table"]),
            Read,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "workload",
            "The tenant's heaviest statement fingerprints over the last hours (1 to 168): calls, latency percentiles, rows examined. Call it when asked what is slow or busy.",
            obj(
                &[("db", db()), ("hours", int("Hours to look back, 1 to 168"))],
                &["hours"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "fingerprint",
            "One statement fingerprint's normalized text, plans seen and latency history. Call it after workload to look closer at one statement.",
            obj(
                &[("id", s("The fingerprint id, as workload returns it"))],
                &["id"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "explain",
            "The plan of a statement (EXPLAIN FORMAT=JSON), run as the user without executing it. Call it to judge a query's cost or an index's use.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT, INSERT, UPDATE or DELETE")),
                ],
                &["db", "sql"],
            ),
            Read,
            DBA_DATA,
            Shares::Stats,
        ),
        spec(
            "indexes",
            "A table's indexes, with their columns, uniqueness and whether automatic indexing made them.",
            obj(&[("db", db()), ("table", s("The table"))], &["db", "table"]),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "auto_indexes",
            "Automatic indexing's candidates, active indexes and recent drops, with the reasons.",
            obj(&[("db", db())], &[]),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "settings",
            "SprigDB settings with their values, where each value comes from, and what they mean. With db (and table), the values as they resolve for that database (or table), and where each comes from. Call it before suggesting a setting change.",
            obj(
                &[
                    (
                        "key",
                        s("A key or a prefix such as ai. or studio.; all when absent"),
                    ),
                    (
                        "scope",
                        one_of(
                            &["global", "dc", "tenant", "database"],
                            "Resolve at this scope",
                        ),
                    ),
                    ("db", s("Resolve for this database")),
                    ("table", s("Resolve for this table of db")),
                ],
                &[],
            ),
            Read,
            DBA_ADMIN,
            Shares::Schema,
        ),
        spec(
            "shards",
            "How the tenant's sharded tables are split and placed, with shard sizes.",
            obj(&[("db", db())], &[]),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "advisor_runs",
            "Recent runs of the shard and index advisors, with their verdicts.",
            obj(&[], &[]),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "approvals",
            "Recommendations waiting in the approvals inbox, or decided ones, that the user may see.",
            obj(
                &[
                    (
                        "state",
                        one_of(&["open", "decided", "all"], "Which items; open when absent"),
                    ),
                    ("kind", s("Only this recommendation kind")),
                ],
                &[],
            ),
            Read,
            ALL,
            Shares::Schema,
        ),
        spec(
            "column_stats",
            "Statistics of up to 50 columns: counts, null fractions and distinct estimates; minimum, maximum and common values only when data sharing is on.",
            obj(
                &[
                    ("db", db()),
                    ("table", s("The table")),
                    ("cols", strs("The columns, 1 to 50")),
                ],
                &["db", "table", "cols"],
            ),
            Read,
            DBA_DATA,
            Shares::Stats,
        ),
        spec(
            "run_query",
            "Run one read-only SELECT as the user, in a read-only transaction, returning at most limit rows (1 to 1000). Rows reach you only when data sharing is on; otherwise you get the columns and the row count, and should offer the query to the user with propose_sql.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT")),
                    ("limit", int("Rows to return, 1 to 1000")),
                ],
                &["db", "sql"],
            ),
            Read,
            DATA,
            Shares::Rows,
        ),
        spec(
            "test_policy",
            "Test row-level security as another user: how many rows of a SELECT they would see (counts, not values), and the policies applied. With policy, it also tries a CREATE POLICY that doesn't exist yet, without creating it: how many of the rows the user sees now the new policy would admit, whether it applies to the user, and which attributes or claims it reads that the user lacks.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One SELECT")),
                    ("as_user", s("The user to test as")),
                    (
                        "policy",
                        s("A CREATE POLICY statement to try without creating it"),
                    ),
                    (
                        "attributes",
                        s(
                            "A JSON object standing in for the user's attributes (SPRIG_USER_ATTR) during the test",
                        ),
                    ),
                    (
                        "claims",
                        s(
                            "A JSON object standing in for the user's token claims (SPRIG_CLAIM) during the test",
                        ),
                    ),
                ],
                &["db", "sql", "as_user"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "compile_logic",
            "Compile logic without deploying it, returning its diagnostics: a CREATE FUNCTION, PROCEDURE, VALIDATION or TRIGGER ... AFTER COMMIT statement in KIRun or JavaScript (validations and after-commit triggers are checked against their table as a deploy would), or a KIRun JSON definition alone (checked as a function).",
            obj(
                &[
                    ("db", db()),
                    (
                        "source",
                        s("The CREATE statement, or a KIRun JSON definition"),
                    ),
                ],
                &["db", "source"],
            ),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "check_ddl",
            "Check schema statements (CREATE TABLE, ALTER TABLE, CREATE INDEX, CREATE VIEW, ...) against SprigDB without running them: each must parse with SprigDB's parser and pass its rules (reserved and long names, storage engines, definers, FULLTEXT, distribution). Call it on every schema change before you propose it, fix what it reports, and propose only statements it passes.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("One or more statements, separated by semicolons")),
                ],
                &["sql"],
            ),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "migration_check",
            "Check a schema coming from MySQL, MariaDB or MongoDB against SprigDB, deterministically: what won't work and why, what SprigDB changes on the way in, suggested table groups and distribution keys (as SPRIG PIN statements), and an import plan. Pass the whole schema the user gave you in schema, unchanged. Explain the report; don't change its findings.",
            obj(
                &[
                    (
                        "source",
                        one_of(
                            &["mysql", "mariadb", "mongodb"],
                            "Where the schema comes from",
                        ),
                    ),
                    ("db", s("The SprigDB database it will go into")),
                    (
                        "schema",
                        s(
                            "The schema, as text: for mysql and mariadb the dump's DDL statements (mysqldump --no-data); for mongodb a JSON array of {name, options, indexes}, as db.getCollectionInfos() and getIndexes() print them",
                        ),
                    ),
                ],
                &["source", "schema"],
            ),
            Read,
            DBA,
            Shares::Schema,
        ),
        spec(
            "decision_log",
            "SprigDB's own decision log, with each decision's reason and evidence: shard splits, merges and moves, index changes and placement for a tenant; cluster and DC failovers, incidents, health findings and write-ups for operators. Call it to explain why SprigDB did something, and cite the entries (their ids) you use.",
            obj(
                &[
                    (
                        "subject",
                        s(
                            "A decision id, or what the decisions are about: a table, a shard such as t42/g1/s3, a cluster, an incident fingerprint, a kind such as shard.move or failover, or recent for the newest",
                        ),
                    ),
                    ("days", int("Days to look back, 1 to 90; 30 when absent")),
                ],
                &["subject"],
            ),
            Read,
            DBA_ADMIN,
            Shares::Stats,
        ),
        spec(
            "metrics_trend",
            "A series from the controller's metric history: daily mean, min and max for up to 30 days, and the last 24 hourly points. It keeps sprig_shard_split_ratio, sprig_balancer_cluster_full_ratio, sprig_backend_replica_lag_seconds, sprig_cluster_health_state, sprig_gateway_connections, sprig_backup_last_success_timestamp_seconds, sprig_mover_jobs_waiting and sprig_mover_oldest_waiting_seconds.",
            obj(
                &[
                    ("name", s("The series, one of those listed")),
                    ("labels", strs("label=value filters, such as cluster=<id>")),
                    ("days", int("Days to look back, 1 to 30")),
                ],
                &["name", "days"],
            ),
            Read,
            ADMIN,
            Shares::Stats,
        ),
        spec(
            "propose_sql",
            "Propose a statement for the user to review and run. Use it for anything that changes data or schema, and for queries the user should run. It is shown to the user and never run by you.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("The statement")),
                    ("why", s("Why, in a sentence or two")),
                    ("estimate", s("What it will do: rows touched, time, locks")),
                ],
                &["db", "sql", "why"],
            ),
            Propose,
            DBA_DATA,
            Shares::Schema,
        ),
        spec(
            "propose_recommendation",
            "Propose a recommendation for the approvals inbox, for changes people decide on later.",
            obj(
                &[
                    ("kind", s("The recommendation kind")),
                    ("subject", s("What it is about")),
                    ("body", s("What to do and why")),
                ],
                &["kind", "subject", "body"],
            ),
            Propose,
            DBA_ADMIN,
            Shares::Schema,
        ),
        spec(
            "propose_tile",
            "Propose a dashboard tile: a query and a chart spec (JSON with a mark and shelves). The user previews it and decides whether to add it.",
            obj(
                &[
                    ("dashboard", s("The dashboard; a new one when absent")),
                    ("title", s("The tile's title")),
                    ("db", db()),
                    ("sql", s("The tile's query, returning rows")),
                    (
                        "chart",
                        s("The chart spec as JSON: mark, columns, rows, color, ..."),
                    ),
                ],
                &["title", "sql", "chart"],
            ),
            Propose,
            &[Role::Data, Role::Dba],
            Shares::Schema,
        ),
        spec(
            "propose_glossary",
            "Propose a glossary entry: a business term, its definition and optionally its SQL. A tenant admin accepts it.",
            obj(
                &[
                    ("term", s("The term")),
                    ("definition", s("What it means here")),
                    ("sql", s("An SQL expression for it")),
                ],
                &["term", "definition"],
            ),
            Propose,
            &[Role::Data, Role::Dba],
            Shares::Schema,
        ),
        spec(
            "generate_rows",
            "Propose test rows for a table (1 to 10000). SprigDB generates them itself from the table's schema: types, NOT NULL, unique keys, foreign keys (existing parent keys), enums and simple CHECK constraints, plus your column hints. You never see the rows; you get a summary, and the user gets an INSERT batch to review and run.",
            obj(
                &[
                    ("db", db()),
                    ("table", s("The table")),
                    ("n", int("Rows, 1 to 10000")),
                    (
                        "rules",
                        s("Rules for the values, in words, for the user; empty when none"),
                    ),
                    (
                        "columns",
                        strs(
                            "Hints, one per column: \"<column>: <kind>\" then any of \", values=a|b|c\", \", min=..\", \", max=..\", \", domain=..\", \", nulls=NN\" (percent). Examples: \"status: one_of, values=paid|paid|paid|new\", \"total: decimal, min=5, max=500\", \"email: email, domain=test.example\". kind is one_of, sequence, email, phone, first_name, last_name, full_name, company, street_address, city, postcode, country_code, currency, url, ip_address, uuid, word, sentence, integer, decimal, date, datetime, boolean or null; min and max are numbers or YYYY-MM-DD. Columns without a hint are generated from their name and type; an empty list when none",
                        ),
                    ),
                ],
                &["db", "table", "n", "rules", "columns"],
            ),
            Propose,
            DBA,
            Shares::Schema,
        ),
        spec(
            "sensitive_columns",
            "Find columns that look like personal or sensitive data in a database (or one table): emails, phone numbers, birth dates, national ids, card and bank numbers, addresses, names, IP addresses, locations, secrets, special categories and free text. Each finding has a category, a confidence and the reasons (name, type, comment, constraint). With data sharing on, sampled values are also matched against patterns, and only the counts come back. It also says which tables have row-level security on. Call it before suggesting policies, grants or residency.",
            obj(
                &[
                    ("db", db()),
                    (
                        "table",
                        s("Only this table; the whole database when absent"),
                    ),
                ],
                &["db"],
            ),
            Read,
            DBA,
            Shares::Stats,
        ),
        spec(
            "propose_policy",
            "Propose row-level security for the user to review and run: CREATE POLICY statements, with ALTER TABLE ... ENABLE ROW LEVEL SECURITY where it is off. Before it is shown, SprigDB checks every policy and tries it as each test user without creating it; the proposal carries those counts, and a policy that fails the check comes back to you to fix. Try policies with test_policy first.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("The statements, separated by semicolons")),
                    ("why", s("Why, in a sentence or two")),
                    (
                        "test_as",
                        strs(
                            "Users to try the policies as: at least one who should see rows and one who shouldn't",
                        ),
                    ),
                ],
                &["db", "sql", "why", "test_as"],
            ),
            Propose,
            DBA,
            Shares::Schema,
        ),
        spec(
            "propose_logic",
            "Propose logic for the user to review and run: one CREATE [OR REPLACE] FUNCTION, PROCEDURE, VALIDATION or TRIGGER statement in KIRun or JavaScript. Before it is shown, SprigDB compiles it without deploying it; compile errors come back to you to fix and propose again. Check drafts with compile_logic.",
            obj(
                &[
                    ("db", db()),
                    ("sql", s("The CREATE statement")),
                    ("why", s("What it does, in a sentence or two")),
                ],
                &["db", "sql", "why"],
            ),
            Propose,
            DBA,
            Shares::Schema,
        ),
    ]
}

/// The proposal kind of a proposal tool.
#[must_use]
pub fn proposal_kind(name: &str) -> Option<ProposalKind> {
    Some(match name {
        // Policies and logic are statements the user runs as any other SQL
        // proposal; only the checks before they are shown differ.
        "propose_sql" | "propose_policy" | "propose_logic" => ProposalKind::Sql,
        "propose_recommendation" => ProposalKind::Recommendation,
        "propose_tile" => ProposalKind::Tile,
        "propose_glossary" => ProposalKind::Glossary,
        "generate_rows" => ProposalKind::GenerateRows,
        _ => return None,
    })
}

fn range(input: &Value, k: &str, lo: i64, hi: i64) -> Result<(), String> {
    match input.get(k).and_then(Value::as_i64) {
        Some(n) if !(lo..=hi).contains(&n) => Err(format!("{k} must be {lo} to {hi}")),
        _ => Ok(()),
    }
}

/// The checks a schema can't say: ranges, lengths, SQL that must be a
/// read, a chart spec.
///
/// # Errors
/// What is wrong, for the model.
#[allow(clippy::too_many_lines)] // one arm per tool
pub fn check_input(name: &str, input: &Value) -> Result<(), String> {
    let text = |k: &str| input.get(k).and_then(Value::as_str).unwrap_or_default();
    for k in ["sql", "source", "body", "definition", "rules", "chart"] {
        if text(k).len() > MAX_TEXT {
            return Err(format!("{k} is longer than {MAX_TEXT} bytes"));
        }
    }
    match name {
        "migration_check" => {
            let schema = text("schema");
            if schema.len() > MAX_MIGRATION_TEXT {
                return Err(format!(
                    "schema is longer than {MAX_MIGRATION_TEXT} bytes; check it in parts"
                ));
            }
            if schema.trim().is_empty() {
                return Err("schema is empty: pass the schema the user gave you".to_owned());
            }
            if text("source") == "mongodb" {
                match serde_json::from_str::<Value>(schema) {
                    Ok(v) if v.is_array() => Ok(()),
                    Ok(_) => Err("for mongodb, schema is a JSON array of collections".to_owned()),
                    Err(e) => Err(format!("for mongodb, schema isn't JSON: {e}")),
                }
            } else {
                Ok(())
            }
        }
        "workload" => range(input, "hours", 1, 168),
        "metrics_trend" => range(input, "days", 1, 30),
        "decision_log" => range(input, "days", 1, 90),
        "generate_rows" => {
            range(
                input,
                "n",
                1,
                i64::try_from(super::testdata::MAX_ROWS).unwrap_or(i64::MAX),
            )?;
            super::testdata::check_hints(input.get("columns"))
        }
        "test_policy" if !text("policy").trim().is_empty() => {
            sql_guard::read_only(text("sql"))?;
            if text("policy").len() > MAX_TEXT {
                return Err(format!("policy is longer than {MAX_TEXT} bytes"));
            }
            if !text("policy")
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("CREATE POLICY")
            {
                return Err("policy is one CREATE POLICY statement".to_owned());
            }
            for k in ["attributes", "claims"] {
                if !text(k).trim().is_empty() {
                    match serde_json::from_str::<Value>(text(k)) {
                        Ok(Value::Object(_)) => {}
                        _ => return Err(format!("{k} is a JSON object")),
                    }
                }
            }
            Ok(())
        }
        "propose_policy" => {
            let n = input
                .get("test_as")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if !(1..=5).contains(&n) {
                return Err("test_as names 1 to 5 users".to_owned());
            }
            if !text("sql").to_ascii_uppercase().contains("CREATE POLICY") {
                return Err("propose_policy needs at least one CREATE POLICY".to_owned());
            }
            Ok(())
        }
        "run_query" => {
            range(input, "limit", 1, 1000)?;
            sql_guard::read_only(text("sql"))
        }
        "test_policy" => sql_guard::read_only(text("sql")),
        "explain" => sql_guard::explainable(text("sql")),
        "column_stats" => {
            let n = input
                .get("cols")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if (1..=50).contains(&n) {
                Ok(())
            } else {
                Err("cols must name 1 to 50 columns".to_owned())
            }
        }
        "propose_tile" => {
            let chart: Value = serde_json::from_str(text("chart"))
                .map_err(|e| format!("chart isn't JSON: {e}"))?;
            match chart.get("mark").and_then(Value::as_str) {
                Some(m) if MARKS.contains(&m) => Ok(()),
                Some(m) => Err(format!("unknown mark {m}; use one of {}", MARKS.join(", "))),
                None => Err("chart needs a mark".to_owned()),
            }
        }
        _ => Ok(()),
    }
}
