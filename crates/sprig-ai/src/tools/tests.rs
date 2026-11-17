use serde_json::json;

use super::*;

#[test]
fn builtin_tools_are_valid_and_split_by_role() {
    let r = ToolRegistry::builtin();
    assert_eq!(r.all().count(), builtin::specs().len());
    assert!(r.for_role(Role::Triage).is_empty());
    let names = |role| -> Vec<&str> { r.for_role(role).iter().map(|s| s.name).collect() };
    assert!(names(Role::Data).contains(&"run_query"));
    assert!(!names(Role::Dba).contains(&"run_query"));
    assert!(names(Role::Admin).contains(&"metrics_trend"));
    assert!(!names(Role::Admin).contains(&"describe"));
    assert!(!names(Role::Admin).contains(&"propose_sql"));
    for s in r.all() {
        // Every proposal tool maps to a proposal kind, and only those.
        assert_eq!(
            builtin::proposal_kind(s.name).is_some(),
            s.effect == Effect::Propose,
            "{}",
            s.name
        );
        assert!(s.description.len() > 20, "{}", s.name);
        let def = s.def();
        assert!(def.strict);
        assert_eq!(def.input_schema["type"], "object");
    }
}

#[test]
fn calls_are_checked_before_anything_runs() {
    let r = ToolRegistry::builtin();
    let ok = r.check(
        Role::Data,
        "run_query",
        &json!({"db": "shop", "sql": "SELECT 1"}),
    );
    assert!(ok.is_ok());
    let deny = |role, name, input| r.check(role, name, &input).unwrap_err();
    let e = deny(
        Role::Dba,
        "run_query",
        json!({"db": "shop", "sql": "SELECT 1"}),
    );
    assert!(e.denied && e.message.contains("may not call"));
    let e = deny(Role::Data, "nope", json!({}));
    assert!(e.message.contains("no tool"));
    let e = deny(Role::Data, "run_query", json!({"db": "shop"}));
    assert!(e.message.contains("sql is required"));
    let e = deny(
        Role::Data,
        "run_query",
        json!({"db": "shop", "sql": "DELETE FROM t"}),
    );
    assert!(e.message.contains("propose_sql"), "{}", e.message);
    let e = deny(
        Role::Data,
        "run_query",
        json!({"db": "s", "sql": "SELECT 1", "limit": 5000}),
    );
    assert!(e.message.contains("limit"));
    let e = deny(
        Role::Dba,
        "explain",
        json!({"db": "s", "sql": "EXPLAIN ANALYZE SELECT 1"}),
    );
    assert!(e.denied);
    let e = deny(Role::Dba, "workload", json!({"hours": 0}));
    assert!(e.message.contains("hours"));
    let e = deny(
        Role::Data,
        "propose_tile",
        json!({"title": "t", "db": "s", "sql": "SELECT 1", "chart": "{\"mark\":\"spiral\"}"}),
    );
    assert!(e.message.contains("unknown mark"));
    // A tile's query is a read, and its fields are the query's columns.
    let tile = |sql: &str, chart: &str| {
        r.check(
            Role::Data,
            "propose_tile",
            &json!({"title": "t", "db": "s", "sql": sql, "chart": chart}),
        )
    };
    let e = tile("DELETE FROM orders", r#"{"mark":"bar"}"#).unwrap_err();
    assert!(e.message.contains("one SELECT"), "{}", e.message);
    let ok_sql = "SELECT region, SUM(total) AS revenue FROM orders GROUP BY region";
    assert!(
        tile(
            ok_sql,
            r#"{"mark":"bar","columns":[{"field":"region"}],"rows":[{"field":"revenue","agg":"sum"}],"sort":{"by":"revenue","dir":"desc"},"top":10}"#
        )
        .is_ok()
    );
    let e = tile(ok_sql, r#"{"mark":"bar","columns":[{"field":"country"}]}"#).unwrap_err();
    assert!(e.message.contains("country"), "{}", e.message);
    let e = tile(
        ok_sql,
        r#"{"mark":"bar","rows":[{"field":"revenue","agg":"total"}]}"#,
    )
    .unwrap_err();
    assert!(e.message.contains("unknown agg"), "{}", e.message);
    let e = tile(ok_sql, r#"{"mark":"bar","x":"region"}"#).unwrap_err();
    assert!(e.message.contains("no key x"), "{}", e.message);
    // SELECT * returns columns the text doesn't name.
    assert!(
        tile(
            "SELECT * FROM v",
            r#"{"mark":"line","columns":[{"field":"week"}],"rows":[{"field":"n","agg":"sum"}]}"#
        )
        .is_ok()
    );
    // The mark must fit the shelves (Show Me's rules): a line needs a
    // measure, a pie one dimension.
    let e = tile(
        "SELECT * FROM v",
        r#"{"mark":"line","columns":[{"field":"week","datePart":"week"}]}"#,
    )
    .unwrap_err();
    assert!(e.message.contains("marks that fit"), "{}", e.message);
    let e = deny(
        Role::Data,
        "describe",
        json!({"db": "s", "table": "t", "extra": 1}),
    );
    assert!(e.message.contains("unknown field extra"));
    assert!(
        r.check(
            Role::Data,
            "propose_tile",
            &json!({"title": "t", "db": "s", "sql": "SELECT 1 AS n", "chart": "{\"mark\":\"kpi\",\"rows\":[{\"field\":\"n\"}]}"})
        )
        .is_ok()
    );
}

#[test]
fn proposals_never_reach_a_host() {
    let r = ToolRegistry::builtin();
    let input = json!({"db": "shop", "sql": "DROP TABLE orders", "why": "unused"});
    let spec = r.check(Role::Dba, "propose_sql", &input).unwrap();
    let p = ToolRegistry::propose(&spec, "toolu_1", &input).unwrap();
    assert_eq!(p.kind, ProposalKind::Sql);
    assert_eq!(p.body, input);
    assert_eq!(p.tool_use_id, "toolu_1");
    let note = ToolRegistry::proposal_note(&p);
    assert!(note["status"].as_str().unwrap().contains("not run"));
    // A read tool can't be turned into a proposal.
    let read = r.get("describe").unwrap();
    assert!(ToolRegistry::propose(read, "x", &json!({})).is_err());
}

#[test]
fn registries_refuse_bad_specs() {
    let mut s = builtin::specs();
    s.push(s[0].clone());
    assert!(ToolRegistry::new(s).is_err());
    let bad = ToolSpec {
        name: "open",
        description: "an open schema",
        input: json!({"type": "object"}),
        effect: Effect::Read,
        roles: &[Role::Data],
        shares: Shares::Schema,
    };
    assert!(ToolRegistry::new([bad]).is_err());
}

/// How deep a JSON schema nests: an object's properties and an array's
/// items each add a level.
fn schema_depth(v: &serde_json::Value) -> usize {
    let props = v["properties"]
        .as_object()
        .map_or(0, |p| p.values().map(schema_depth).max().unwrap_or(0));
    let items = if v.get("items").is_some() {
        schema_depth(&v["items"])
    } else {
        0
    };
    1 + props.max(items)
}

fn enums(v: &serde_json::Value, out: &mut Vec<usize>) {
    if let Some(e) = v["enum"].as_array() {
        out.push(e.len());
    }
    if let Some(p) = v["properties"].as_object() {
        p.values().for_each(|x| enums(x, out));
    }
    if v.get("items").is_some() {
        enums(&v["items"], out);
    }
}

/// The Anthropic API compiles strict tool schemas into one grammar per
/// request and refuses a request whose grammar is too large (HTTP 400,
/// before any tokens). Nested objects in arrays and long enums are what
/// grow it, so every role's strict tools stay flat and within a budget:
/// structured input goes in a JSON-text string, checked by
/// `builtin::check_input`.
#[test]
fn strict_schemas_stay_small_enough_to_compile() {
    /// The schema text one role's strict tools may total (about 6 KiB in
    /// November 2026, when the DBA role's requests compiled live).
    const ROLE_BUDGET: usize = 8 * 1024;
    let r = ToolRegistry::builtin();
    for role in [Role::Admin, Role::Dba, Role::Data] {
        let strict: Vec<_> = r
            .for_role(role)
            .into_iter()
            .take(crate::provider::anthropic::MAX_STRICT_TOOLS)
            .collect();
        let mut total = 0;
        for s in &strict {
            let text = s.input.to_string();
            total += text.len();
            assert!(
                schema_depth(&s.input) <= 3,
                "{}: nested schema; take a JSON-text string instead",
                s.name
            );
            let mut e = Vec::new();
            enums(&s.input, &mut e);
            assert!(
                e.iter().all(|n| *n <= 8),
                "{}: an enum of more than 8 values; take a string checked in check_input",
                s.name
            );
        }
        eprintln!(
            "{role}: {} strict tools, {total} bytes of schema",
            strict.len()
        );
        assert!(
            total <= ROLE_BUDGET,
            "{role}: {total} bytes of strict schemas"
        );
    }
}
