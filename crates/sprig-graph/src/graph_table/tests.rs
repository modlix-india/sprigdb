#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;

const CHAPTER: &str = "SELECT g.name, SUM(g.total) AS spent
FROM GRAPH_TABLE (shop
       MATCH (c IS Person)-[o IS BOUGHT]->(p IS Product)
       WHERE c.email = 'alice@example.com'
       COLUMNS (p.name AS name, o.qty * p.price AS total)) AS g
GROUP BY g.name";

#[test]
fn finds_and_translates_the_chapters_example() {
    let v = find(CHAPTER).expect("ok");
    assert_eq!(v.len(), 1);
    let g = &v[0];
    assert_eq!(g.graph, "shop");
    assert_eq!(g.columns, ["name", "total"]);
    assert_eq!(
        g.cypher,
        "MATCH (c:Person)-[o:BOUGHT]->(p:Product) WHERE c.email = 'alice@example.com' RETURN p.name AS `name`, o.qty * p.price AS `total`"
    );
    assert!(CHAPTER[g.span.0..g.span.1].starts_with("GRAPH_TABLE"));
    assert!(CHAPTER[g.span.1..].trim_start().starts_with("AS g"));
    // The keyword inside a string is not a pattern word.
    assert_eq!(
        pattern_to_cypher("(a IS L {n: 'x IS y'})"),
        "(a:L {n: 'x IS y'})"
    );
    assert!(find("SELECT 1").expect("ok").is_empty());
    assert!(find("SELECT * FROM GRAPH_TABLE (g COLUMNS (a))").is_err());
    assert!(mentions(CHAPTER) && !mentions("SELECT 1"));
}

#[test]
fn rows_are_one_flat_value_list() {
    let cols = vec!["a".to_owned(), "b".to_owned()];
    let s = rows_sql(
        &cols,
        &[
            vec![GraphLit::Int(1), GraphLit::Str("it's".into())],
            vec![GraphLit::Null, GraphLit::Other("2.5e0".into())],
            vec![GraphLit::Int(-3), GraphLit::Str("a\\b".into())],
        ],
    );
    assert_eq!(
        s,
        "SELECT 1 AS `a`, 'it''s' AS `b` UNION ALL VALUES (NULL, 2.5e0), (-3, 'a\\\\b')"
    );
    assert_eq!(
        rows_sql(&cols, &[]),
        "SELECT NULL AS `a`, NULL AS `b` FROM DUAL WHERE FALSE"
    );
    // MySQL's table value constructor names each row.
    assert_eq!(
        rows_sql_in(
            &cols,
            &[
                vec![GraphLit::Int(1), GraphLit::Int(2)],
                vec![GraphLit::Int(3), GraphLit::Int(4)],
                vec![GraphLit::Int(5), GraphLit::Null],
            ],
            true,
        ),
        "SELECT 1 AS `a`, 2 AS `b` UNION ALL VALUES ROW(3, 4), ROW(5, NULL)"
    );
}

/// A prepared statement's values go in as SQL literals
/// outside the calls and as Cypher literals inside them.
#[test]
fn parameters_bind_as_sql_or_cypher_literals() {
    let sql = "SELECT x.a, '?' FROM GRAPH_TABLE (g MATCH (p IS P) WHERE p.id < ? AND p.n = ? \
               COLUMNS (p.id AS a)) AS x JOIN t ON t.k = x.a WHERE t.v > ? -- ?\n AND t.w = ? /* ? */";
    assert_eq!(param_spans(sql).len(), 4);
    let sql_l = [
        "1".to_owned(),
        "'s'".into(),
        "3".into(),
        "_utf8mb4'x'".into(),
    ];
    let cy_l = ["1".to_owned(), "'s\\\\'".into(), "3".into(), "'x'".into()];
    let b = bind_params(sql, &sql_l, &cy_l).unwrap();
    assert!(
        b.contains("WHERE p.id < 1 AND p.n = 's\\\\' COLUMNS"),
        "{b}"
    );
    assert!(
        b.contains("t.v > 3 -- ?\n AND t.w = _utf8mb4'x' /* ? */"),
        "{b}"
    );
    assert!(b.contains("SELECT x.a, '?' FROM"), "{b}");
    assert!(bind_params(sql, &sql_l[..3], &cy_l[..3]).is_none());
    let found = find(&b).unwrap();
    assert_eq!(
        found[0].cypher,
        "MATCH (p:P) WHERE p.id < 1 AND p.n = 's\\\\' RETURN p.id AS `a`"
    );
}
