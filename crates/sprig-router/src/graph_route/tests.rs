#![allow(clippy::unwrap_used, clippy::panic)]

use super::*;
use crate::topology::{KeyClass, ShardLoc, even_ranges};

/// A graph's group: four shards over two clusters, keyed by `part`.
fn group(held: Option<u64>) -> Arc<Group> {
    let shards = even_ranges(4)
        .into_iter()
        .enumerate()
        .map(|(i, lo)| ShardLoc {
            shard: u32::try_from(i + 1).unwrap(),
            schema: format!("t1_d2_g30_s{}", i + 1),
            cluster: if i < 2 { "c1" } else { "c2" }.to_owned(),
            lo,
            epoch: 3,
            held_until_ms: (i == 2).then_some(held).flatten(),
        })
        .collect();
    Arc::new(Group {
        id: 30,
        db: 2,
        version: 9,
        key_class: KeyClass::Int,
        spec: sprig_shard::KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap(),
        shards,
    })
}

fn spec(g: &Arc<Group>, op: HopOp, targets: Targets) -> RouteSpec {
    RouteSpec {
        group: Arc::clone(g),
        graph: 77,
        op,
        targets,
    }
}

#[test]
fn a_hop_by_key_reads_only_the_frontiers_shards_each_with_its_own_ids() {
    let g = group(None);
    let ids: Vec<u64> = (1..=40).collect();
    let r = plan_route(&spec(
        &g,
        HopOp::Expand {
            out: true,
            types: vec![4],
        },
        Targets::ByKey(ids.clone()),
    ));
    let ts = r.targets();
    assert!(!ts.is_empty() && ts.len() <= 4);
    let mut seen = Vec::new();
    for t in ts {
        let i = g.shards.iter().position(|s| s.shard == t.shard).unwrap();
        assert_eq!(t.cluster, g.shards[i].cluster);
        assert_eq!(t.at.schema, g.shards[i].schema);
        assert_eq!(t.at.group, 30);
        assert!(
            t.sql
                .contains(&format!("`{}`.`__sprig_ge_77`", g.shards[i].schema))
        );
        assert!(t.sql.contains("LEFT JOIN"));
        assert!(t.sql.ends_with(" AND e.type IN (4)"));
        // Each id in the statement of the shard its hash routes to.
        let list = t.sql.split("e.src IN (").nth(1).unwrap();
        let list = list.split(')').next().unwrap();
        for id in list.split(',') {
            let id: u64 = id.parse().unwrap();
            assert_eq!(g.route(node_hash(&g, id).unwrap()), i);
            seen.push(id);
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, ids);
    // One id: a single-shard pass.
    let one = plan_route(&spec(&g, HopOp::Nodes, Targets::ByKey(vec![5])));
    assert!(matches!(one.plan, Plan::Pass(_)));
    assert!(
        one.line()
            .starts_with("GraphRoute nodes: pass over 1 statement(s) on s")
    );
}

#[test]
fn incoming_without_mirrors_scatters_and_edges_by_hash_follow_the_hash() {
    let g = group(None);
    let r = plan_route(&spec(
        &g,
        HopOp::Expand {
            out: false,
            types: vec![],
        },
        Targets::All(vec![1, 2]),
    ));
    assert!(
        matches!(&r.plan, Plan::Read(p) if p.kind == ReadKind::Scatter && p.targets.len() == 4)
    );
    assert!(r.targets().iter().all(|t| t.sql.contains("e.dst IN (1,2)")));
    let h = g.shards[3].lo + 5;
    let e = plan_route(&spec(
        &g,
        HopOp::EdgesWithSrc,
        Targets::ByHash(vec![(h, 900)]),
    ));
    let [t] = e.targets() else { panic!() };
    assert_eq!(t.shard, 4);
    assert!(t.sql.contains("e.id IN (900)"));
    // Large lists come in batches on the same shard.
    let many: Vec<(u64, u64)> = (0..2500).map(|i| (h, i)).collect();
    let e = plan_route(&spec(&g, HopOp::EdgesWithSrc, Targets::ByHash(many)));
    assert_eq!(e.targets().len(), 3);
}

#[test]
fn scans_write_values_as_hex_and_skip_shards_that_match_nothing() {
    let g = group(None);
    let r = plan_route(&spec(
        &g,
        HopOp::Scan(ScanOp {
            label: Some(3),
            props: vec![(
                "name".into(),
                vec![Lit::Str("O'Brien \\ x".into()), Lit::Int(-2)],
            )],
            limit: Some(11),
            ..ScanOp::default()
        }),
        Targets::All(vec![]),
    ));
    assert_eq!(r.targets().len(), 4);
    let t = &r.targets()[0].sql;
    assert!(t.contains("JOIN `t1_d2_g30_s1`.`__sprig_gl_77` l ON l.id = n.id AND l.label = 3"));
    assert!(
        t.contains("JSON_VALUE(n.props, '$.\"name\"') IN (_utf8mb4 X'4F27427269656E205C2078', -2)")
    );
    assert!(t.ends_with(" LIMIT 11"));
    assert!(!t.contains("n.id IN"));
    // An index with no value can match nothing: no statement at all.
    let none = plan_route(&spec(
        &g,
        HopOp::Scan(ScanOp {
            label: Some(3),
            index: Some(("_sprig_i1_0".into(), vec![])),
            ..ScanOp::default()
        }),
        Targets::All(vec![]),
    ));
    assert!(none.targets().is_empty());
    assert!(matches!(none.plan, Plan::Read(_)));
}

#[test]
fn a_reach_runs_on_one_shard_inside_its_range_and_reports_holds() {
    let g = group(Some(1_234));
    let r = plan_route(&spec(
        &g,
        HopOp::Reach {
            seeds: vec![(1, 1), (1, 2)],
            types: vec![],
            out: false,
            hops: 3,
        },
        Targets::Shard(2),
    ));
    let [t] = r.targets() else { panic!() };
    assert_eq!(t.shard, 3);
    assert!(t.sql.contains("`t1_d2_g30_s3`.`__sprig_gm_77`"));
    assert!(t.sql.contains(&format!(
        "AND r.rk >= {} AND r.rk < {}",
        g.shards[2].lo, g.shards[3].lo
    )));
    assert!(
        t.sql
            .starts_with("SET STATEMENT max_recursive_iterations = 4 FOR")
    );
    assert_eq!(r.held_until_ms, Some(1_234));
    // A route that doesn't touch the held shard reports no hold.
    let other = plan_route(&spec(&g, HopOp::Edges, Targets::Shard(0)));
    assert_eq!(other.held_until_ms, None);
    let lines = explain_lines(&g, true);
    assert!(lines[0].contains("group 30 (map version 9), 4 shard(s)"));
    assert!(lines[3].ends_with("(held)"));
    assert!(
        lines
            .iter()
            .any(|l| l.contains("when the mirrors are fresh"))
    );
}

#[test]
fn a_top_k_hop_orders_by_cypher_classes_and_keeps_ties() {
    let g = group(None);
    let far = |top: Option<(String, bool, usize)>| FarNodes {
        keys: Some(vec!["cd".into()]),
        filter: vec![],
        only: vec![],
        join: true,
        rel_keys: None,
        rel_filter: vec![],
        top,
        limit: None,
        deep: false,
        near_scan: None,
        after: None,
        ordered: false,
        join_max: None,
        next: None,
    };
    let route = |top| {
        plan_route(&spec(
            &g,
            HopOp::Links {
                out: true,
                mirror: false,
                types: vec![4],
                far: Some(far(top)),
            },
            Targets::ByKey(vec![1]),
        ))
    };
    let r = route(Some(("cd".into(), false, 10)));
    let [t] = r.targets() else { panic!() };
    // Ranked links, then every link the statement can't rank.
    let (top, rest) = t.sql.split_once(") UNION ALL (").unwrap();
    assert!(top.starts_with("(SELECT "), "{}", t.sql);
    assert!(
        top.ends_with("DESC FETCH FIRST 10 ROWS WITH TIES"),
        "{}",
        t.sql
    );
    assert!(top.contains("IS NOT NULL AND NOT (JSON_TYPE("), "{}", t.sql);
    // Descending: missing values first, numbers by value, then the
    // other kinds tied.
    assert!(top.contains("IS NULL THEN 0 WHEN"), "{}", t.sql);
    assert!(top.contains("+ 0e0 END DESC"), "{}", t.sql);
    assert!(rest.ends_with("<=> 'NULL'))"), "{}", t.sql);
    let asc = route(Some(("cd".into(), true, 3)));
    let [t] = asc.targets() else { panic!() };
    assert!(t.sql.contains("THEN 2 WHEN JSON_TYPE("), "{}", t.sql);
    assert!(
        t.sql.contains("ASC FETCH FIRST 3 ROWS WITH TIES)"),
        "{}",
        t.sql
    );
    // Without a join there are no far node columns to order by.
    let plain = plan_route(&spec(
        &g,
        HopOp::Links {
            out: true,
            mirror: false,
            types: vec![4],
            far: Some(FarNodes {
                join: false,
                ..far(Some(("cd".into(), false, 10)))
            }),
        },
        Targets::ByKey(vec![1]),
    ));
    let [t] = plain.targets() else { panic!() };
    assert!(!t.sql.contains("FETCH"), "{}", t.sql);
    assert_eq!(top_parts("n.id", "n.props", "a\"b", true, 3), None);
}

#[test]
fn links_over_a_scan_read_each_shards_own_start_nodes() {
    let g = group(None);
    let scan = ScanOp {
        label: Some(3),
        ..ScanOp::default()
    };
    let r = plan_route(&spec(
        &g,
        HopOp::Links {
            out: true,
            mirror: false,
            types: vec![4],
            far: Some(FarNodes {
                keys: Some(vec![]),
                filter: vec![],
                only: vec![],
                join: false,
                rel_keys: None,
                rel_filter: vec![],
                top: None,
                limit: Some(100),
                deep: false,
                near_scan: Some(Box::new(scan)),
                after: None,
                ordered: true,
                join_max: None,
                next: None,
            }),
        },
        Targets::All(vec![]),
    ));
    let ts = r.targets();
    assert_eq!(ts.len(), 4);
    for t in ts {
        let s = &t.at.schema;
        assert!(
            t.sql.contains(&format!(
                "e.src IN (SELECT n.id FROM `{s}`.`__sprig_gn_77` n JOIN `{s}`.`__sprig_gl_77` l"
            )),
            "{}",
            t.sql
        );
        assert!(t.sql.ends_with(" ORDER BY e.id LIMIT 100"), "{}", t.sql);
    }
}

#[test]
fn a_joined_hop_past_join_max_gives_its_links_unjoined_in_the_same_statement() {
    let g = group(None);
    let r = plan_route(&spec(
        &g,
        HopOp::Links {
            out: true,
            mirror: false,
            types: vec![4],
            far: Some(FarNodes {
                keys: Some(vec!["cd".into()]),
                filter: vec![(format!("{LABEL_KEY}3"), CmpOp::Eq, "0".into())],
                only: vec![9],
                join: true,
                rel_keys: Some(vec![]),
                rel_filter: vec![],
                top: None,
                limit: Some(2000),
                deep: false,
                near_scan: None,
                after: None,
                ordered: false,
                join_max: Some(160),
                next: None,
            }),
        },
        Targets::ByKey(vec![1]),
    ));
    let [t] = r.targets() else { panic!() };
    let (joined, plain) = t.sql.split_once(") UNION ALL (").unwrap();
    // One part runs: the count of the near nodes' links decides which.
    assert!(joined.starts_with("(SELECT "), "{}", t.sql);
    assert!(
        joined.ends_with(" LIMIT 160, 1) IS NULL LIMIT 160"),
        "{}",
        t.sql
    );
    assert!(joined.contains("JSON_CONTAINS("), "{}", t.sql);
    assert!(
        plain.ends_with(" LIMIT 160, 1) IS NOT NULL LIMIT 2000)"),
        "{}",
        t.sql
    );
    assert!(
        plain.starts_with("SELECT e.src, e.dst, e._sprig_rk, e.dst_rk, e.id, e.type, NULL, NULL, NULL, NULL, NULL, e.props, e._types FROM "),
        "{}",
        t.sql
    );
    // The far node's filters stay out of the unjoined part; `only` stays.
    assert!(!plain.contains("JSON_CONTAINS("), "{}", t.sql);
    assert!(plain.contains("e.dst IN (9)"), "{}", t.sql);
    assert!(!plain.contains("LEFT JOIN"), "{}", t.sql);
    // Tagging marks both parts, not the count's subqueries.
    let tagged_sql = tagged(&t.sql, 1).unwrap();
    assert_eq!(tagged_sql.matches("1 AS q, ").count(), 2, "{tagged_sql}");
    assert!(tagged_sql.contains(" AND (SELECT 1 FROM "), "{tagged_sql}");
    assert_eq!(
        tagged("SELECT a FROM b", 0).unwrap(),
        "(SELECT 0 AS q, a FROM b)"
    );
    assert_eq!(tagged("SET STATEMENT x = 1 FOR SELECT 1", 0), None);
}

#[test]
fn a_walk_reads_ahead_in_one_recursive_statement_on_one_cluster() {
    let two = group(None);
    assert_eq!(
        walk_statement(&two, 77, &[1, 2], &[6], 15, None, None, 100, 100),
        None
    );
    let mut one = (*two).clone();
    for s in &mut one.shards {
        s.cluster = "c1".into();
    }
    let sql = walk_statement(
        &one,
        77,
        &[1, 2, 3],
        &[6],
        15,
        Some(&["id".into()]),
        None,
        4097,
        100,
    )
    .unwrap();
    assert!(
        sql.starts_with("SET STATEMENT max_recursive_iterations = 16, max_statement_time = 0.100 FOR WITH RECURSIVE w (src, dst, src_rk, dst_rk, id, type, d) AS (SELECT "),
        "{sql}"
    );
    // A recursive part per shard, each following links into its range.
    assert_eq!(sql.matches(" FROM w JOIN ").count(), 4, "{sql}");
    assert!(
        sql.contains("WHERE w.d < 15 AND w.dst_rk >= 0 AND w.dst_rk < "),
        "{sql}"
    );
    assert!(sql.contains(" AND e.type IN (6) WHERE w.d < 15"), "{sql}");
    assert!(sql.contains(") CYCLE id RESTRICT SELECT w.src, "), "{sql}");
    assert!(
        sql.contains(
            " LEFT JOIN `t1_d2_g30_s4`.`__sprig_gn_77` n3 ON n3.id = w.dst AND w.dst_rk >= "
        ),
        "{sql}"
    );
    assert!(sql.ends_with(" LIMIT 4097"), "{sql}");
    assert_eq!(
        walk_statement(&one, 77, &[], &[6], 15, None, None, 1, 100),
        None
    );
    // The clause's next hop from every node the walk is at.
    let nx = NextHop {
        types: vec![5],
        keys: Some(vec!["firstName".into()]),
    };
    let sql = walk_statement(&one, 77, &[1, 2], &[6], 15, Some(&[]), Some(&nx), 99, 100).unwrap();
    assert!(sql.contains(" SELECT 0, w.src, "), "{sql}");
    assert!(sql.contains(" UNION ALL SELECT 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, z.n, "), "{sql}");
    assert!(
        sql.contains("FROM (SELECT dst AS n, dst_rk AS rk FROM w UNION SELECT 1, "),
        "{sql}"
    );
    assert!(
        sql.contains(
            " LEFT JOIN `t1_d2_g30_s1`.`__sprig_ge_77` x0 ON x0.src = z.n AND z.rk >= 0 AND z.rk < "
        ),
        "{sql}"
    );
    assert!(sql.contains(" AND x0.type IN (5)"), "{sql}");
    assert!(sql.ends_with(" LIMIT 99"), "{sql}");
}
