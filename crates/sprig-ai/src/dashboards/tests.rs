//! Dashboards over the memory store: who sees and changes what, tiles run
//! as the viewer, filters bound as literals, the assistant's tiles
//! accepted.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};
use sprig_common::ids::UserId;
use sprig_common::{Result, SprigError};

use super::*;
use crate::tools::{Column, ResultSet};

fn who(id: u32, name: &str, admin: bool) -> Who {
    Who {
        user: UserId(id),
        name: name.to_owned(),
        admin,
    }
}

/// A runner for one user: answers with that user's name, and records the
/// SQL it was given.
struct Runner {
    user: String,
    ran: Mutex<Vec<(String, String)>>,
}

impl Runner {
    fn for_user(w: &Who) -> Self {
        Self {
            user: w.name.clone(),
            ran: Mutex::new(Vec::new()),
        }
    }
    fn ran(&self) -> Vec<(String, String)> {
        self.ran.lock().unwrap().clone()
    }
}

#[async_trait]
impl TileRunner for Runner {
    async fn run(&self, db: &str, sql: &str) -> Result<ResultSet> {
        self.ran
            .lock()
            .unwrap()
            .push((db.to_owned(), sql.to_owned()));
        Ok(ResultSet {
            columns: vec![Column {
                name: "seen_by".into(),
                ty: "VARCHAR".into(),
            }],
            rows: vec![vec![json!(self.user)]],
            row_count: 1,
            truncated: false,
        })
    }
}

fn tile(sql: &str) -> Value {
    json!({
        "title": "Revenue by region", "database": "shop", "sql": sql,
        "chart": {"mark": "bar", "columns": [{"field": "region"}], "rows": [{"field": "revenue", "agg": "sum"}]},
    })
}

/// Each store the routes may use: in memory, and on catalog records.
fn stores() -> Vec<Box<dyn DashStore>> {
    vec![
        Box::new(MemoryDashboards::default()),
        Box::new(RecordsDashboards::new(
            std::sync::Arc::new(sprig_catalog::records::mem::MemRecords::new()),
            sprig_common::ids::TenantId(7),
        )),
    ]
}

fn code(e: &SprigError) -> u16 {
    e.protocol().mysql_errno
}

async fn private_dashboards_are_their_owners_and_shared_ones_everyones_on(s: &dyn DashStore) {
    let alice = who(1, "alice", false);
    let bob = who(2, "bob", false);
    let admin = who(3, "carol", true);
    let d = create(s, &alice, &json!({"name": "Weekly sales"}))
        .await
        .unwrap();
    let id = id_of(&d["id"]).unwrap();
    assert_eq!(d["mine"], true);
    assert_eq!(d["can_edit"], true);
    add_tile(s, &alice, id, &tile("SELECT region, revenue FROM sales"))
        .await
        .unwrap();

    // Private: nobody else sees it, not even an administrator.
    for w in [&bob, &admin] {
        assert!(
            list(s, w).await.unwrap()["shared"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert!(get(s, w, id).await.is_err());
        assert!(delete(s, w, id).await.is_err());
        let r = Runner::for_user(w);
        let t = get(s, &alice, id).await.unwrap()["tiles"][0]["id"].clone();
        assert!(
            run_tile(s, w, &r, id, id_of(&t).unwrap(), &json!({}))
                .await
                .is_err()
        );
        assert!(r.ran().is_empty());
    }

    // Only the owner shares it.
    assert_eq!(code(&share(s, &admin, id, true).await.unwrap_err()), 39792);
    let d = share(s, &alice, id, true).await.unwrap();
    assert_eq!(d["shared"], true);

    // Shared: everyone sees it; bob can't change it, the administrator can.
    let l = list(s, &bob).await.unwrap();
    assert_eq!(l["shared"][0]["name"], "Weekly sales");
    assert_eq!(l["shared"][0]["can_edit"], false);
    assert_eq!(l["shared"][0]["owner"], "alice");
    assert_eq!(get(s, &bob, id).await.unwrap()["can_edit"], false);
    let e = update(s, &bob, id, &json!({"name": "Mine now"}))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 1227);
    assert_eq!(
        code(
            &add_tile(s, &bob, id, &tile("SELECT region, revenue FROM t"))
                .await
                .unwrap_err()
        ),
        1227
    );
    assert_eq!(code(&delete(s, &bob, id).await.unwrap_err()), 1227);
    assert_eq!(get(s, &admin, id).await.unwrap()["can_edit"], true);
    update(
        s,
        &admin,
        id,
        &json!({"name": "Weekly sales (all regions)"}),
    )
    .await
    .unwrap();
    // The administrator can't unshare it either: that is the owner's call.
    assert_eq!(code(&share(s, &admin, id, false).await.unwrap_err()), 1227);
    // The owner unshares it, and it is hers alone again.
    share(s, &alice, id, false).await.unwrap();
    assert!(get(s, &bob, id).await.is_err());
    // Shared again, the administrator may delete it.
    share(s, &alice, id, true).await.unwrap();
    delete(s, &admin, id).await.unwrap();
    assert!(get(s, &alice, id).await.is_err());
}

#[tokio::test]
async fn private_dashboards_are_their_owners_and_shared_ones_everyones() {
    for s in stores() {
        private_dashboards_are_their_owners_and_shared_ones_everyones_on(&*s).await;
    }
}

async fn a_viewer_runs_tiles_with_their_own_rights_on(s: &dyn DashStore) {
    let alice = who(1, "alice", false);
    let bob = who(2, "bob", false);
    let d = create(s, &alice, &json!({"name": "Ops", "shared": true}))
        .await
        .unwrap();
    let id = id_of(&d["id"]).unwrap();
    let r = add_tile(s, &alice, id, &tile("SELECT region, revenue FROM sales"))
        .await
        .unwrap();
    let t = id_of(&r["tile"]).unwrap();

    let as_bob = Runner::for_user(&bob);
    let out = run_tile(s, &bob, &as_bob, id, t, &json!({})).await.unwrap();
    // The runner is bob's: his rights, his rows.
    assert_eq!(out["rows"][0][0], "bob");
    assert_eq!(out["columns"][0]["name"], "seen_by");
    assert_eq!(
        as_bob.ran(),
        [(
            "shop".to_owned(),
            "SELECT region, revenue FROM sales".to_owned()
        )]
    );
    let as_alice = Runner::for_user(&alice);
    let out = run_tile(s, &alice, &as_alice, id, t, &json!({}))
        .await
        .unwrap();
    assert_eq!(out["rows"][0][0], "alice");
}

#[tokio::test]
async fn a_viewer_runs_tiles_with_their_own_rights() {
    for s in stores() {
        a_viewer_runs_tiles_with_their_own_rights_on(&*s).await;
    }
}

#[test]
fn filter_values_become_literals_never_sql() {
    let f = |name: &str, kind: &str, value: Value| Filter {
        name: name.into(),
        label: name.into(),
        kind: kind.into(),
        value,
    };
    let filters = vec![
        f("region", "text", json!("North")),
        f("min", "number", json!(10)),
        f("from", "date", json!("2026-07-01")),
        f("status", "list", json!(["paid", "shipped"])),
    ];
    let p = Map::new();
    let sql = "SELECT * FROM o WHERE region = {{region}} AND total >= {{ min }} \
               AND day >= {{from}} AND status IN {{status}} -- {{nope}}";
    assert_eq!(
        bind(sql, &filters, &p).unwrap(),
        "SELECT * FROM o WHERE region = 'North' AND total >= 10 \
         AND day >= '2026-07-01' AND status IN ('paid', 'shipped') -- {{nope}}"
    );

    // A viewer's values: quotes doubled, backslashes doubled, so the
    // statement stays one read.
    let mut p = Map::new();
    p.insert("region".into(), json!("x' OR '1'='1"));
    p.insert("min".into(), json!("5"));
    p.insert("from".into(), json!("2026-07-01T10:30"));
    p.insert("status".into(), json!(["a'); DROP TABLE o; --", 3]));
    let bound = bind(sql, &filters, &p).unwrap();
    assert!(bound.contains("region = 'x'' OR ''1''=''1'"), "{bound}");
    assert!(
        bound.contains("status IN ('a''); DROP TABLE o; --', 3)"),
        "{bound}"
    );
    assert!(bound.contains("day >= '2026-07-01 10:30'"), "{bound}");
    crate::sql_guard::read_only(&bound).unwrap();
    let mut p = Map::new();
    p.insert("region".into(), json!("\\' OR 1=1 -- "));
    let bound = bind("SELECT * FROM o WHERE region = {{region}}", &filters, &p).unwrap();
    assert_eq!(bound, "SELECT * FROM o WHERE region = '\\\\'' OR 1=1 -- '");
    crate::sql_guard::read_only(&bound).unwrap();

    // Values of the wrong kind are refused, not passed on.
    for (k, v) in [
        ("min", json!("1; DROP TABLE o")),
        ("min", json!("1e999")),
        ("from", json!("2026-07-01' OR 1=1")),
        ("from", json!("yesterday")),
        ("status", json!("paid")),
        ("status", json!([{"x": 1}])),
    ] {
        let mut p = Map::new();
        p.insert(k.into(), v.clone());
        assert!(bind(sql, &filters, &p).is_err(), "{k} = {v}");
    }
    // NULL for a filter without a value; (NULL) for an empty list.
    let empty = vec![
        f("region", "text", Value::Null),
        f("status", "list", json!([])),
    ];
    assert_eq!(
        bind(
            "SELECT 1 FROM o WHERE r = {{region}} OR s IN {{status}}",
            &empty,
            &Map::new()
        )
        .unwrap(),
        "SELECT 1 FROM o WHERE r = NULL OR s IN (NULL)"
    );
    // A filter inside quotes, or one the dashboard doesn't have, is an error.
    assert!(
        bind(
            "SELECT * FROM o WHERE r = '{{region}}'",
            &filters,
            &Map::new()
        )
        .is_err()
    );
    assert!(
        bind(
            "SELECT * FROM o WHERE `{{region}}` = 1",
            &filters,
            &Map::new()
        )
        .is_err()
    );
    assert!(bind("SELECT * FROM o WHERE r = {{other}}", &filters, &Map::new()).is_err());
    // Text that only looks like a filter is left alone.
    assert_eq!(
        bind(
            "SELECT '{{ not a name }}' /* {{region}} */",
            &filters,
            &Map::new()
        )
        .unwrap(),
        "SELECT '{{ not a name }}' /* {{region}} */"
    );
    assert_eq!(
        placeholders("SELECT {{a}}, {{b}}, {{a}}").unwrap(),
        ["a", "b"]
    );
}

#[allow(clippy::many_single_char_names)]
async fn tiles_and_filters_are_checked_when_saved_on(s: &dyn DashStore) {
    let alice = who(1, "alice", false);
    let d = create(s, &alice, &json!({"name": "Checks"})).await.unwrap();
    let id = id_of(&d["id"]).unwrap();
    // Writes, two statements, a chart naming a column the query lacks, a
    // mark that doesn't fit, a bad refresh or place.
    for (sql, chart) in [
        ("DELETE FROM sales", None),
        ("SELECT region, revenue FROM sales; SELECT 1", None),
        ("SELECT region, total FROM sales", None),
        (
            "SELECT region, revenue FROM sales",
            Some(
                json!({"mark": "pie", "columns": [{"field": "region"}, {"field": "revenue"}], "rows": [{"field": "revenue", "agg": "sum"}], "color": {"field": "region"}}),
            ),
        ),
    ] {
        let mut t = tile(sql);
        if let Some(c) = chart {
            t["chart"] = c;
        }
        assert!(add_tile(s, &alice, id, &t).await.is_err(), "{sql}");
    }
    let mut t = tile("SELECT region, revenue FROM sales");
    t["refresh_seconds"] = json!(5);
    assert!(add_tile(s, &alice, id, &t).await.is_err());
    t["refresh_seconds"] = json!(300);
    t["pos"] = json!({"x": 8, "y": 0, "w": 6, "h": 4});
    assert!(add_tile(s, &alice, id, &t).await.is_err());
    // A filter the dashboard doesn't have yet.
    let f = tile("SELECT region, revenue FROM sales WHERE day >= {{from}}");
    assert!(add_tile(s, &alice, id, &f).await.is_err());
    update(
        s,
        &alice,
        id,
        &json!({"filters": [{"name": "from", "label": "From", "kind": "date", "value": "2026-07-01"}]}),
    )
    .await
    .unwrap();
    let r = add_tile(s, &alice, id, &f).await.unwrap();
    assert_eq!(
        r["dashboard"]["tiles"][0]["pos"],
        json!({"x": 0, "y": 0, "w": 6, "h": 4})
    );
    // The chart spec may come as JSON text, as the assistant writes it.
    let mut t2 = tile("SELECT region, revenue FROM sales");
    t2["chart"] = json!(t2["chart"].to_string());
    let r = add_tile(s, &alice, id, &t2).await.unwrap();
    // The second tile goes beside the first.
    assert_eq!(
        r["dashboard"]["tiles"][1]["pos"],
        json!({"x": 6, "y": 0, "w": 6, "h": 4})
    );
    // Bad filters.
    for fl in [
        json!([{"name": "1x"}]),
        json!([{"name": "a"}, {"name": "A"}]),
        json!([{"name": "a", "kind": "money"}]),
        json!([{"name": "a", "kind": "number", "value": "ten"}]),
    ] {
        assert!(
            update(s, &alice, id, &json!({"filters": fl}))
                .await
                .is_err(),
            "{fl}"
        );
    }
    // Moving tiles.
    let t1 = r["dashboard"]["tiles"][0]["id"].clone();
    let d = update(
        s,
        &alice,
        id,
        &json!({"positions": [{"tile": t1, "pos": {"x": 0, "y": 4, "w": 12, "h": 6}}]}),
    )
    .await
    .unwrap();
    let moved = d["tiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == t1)
        .unwrap();
    assert_eq!(moved["pos"]["w"], 12);
    // Unsaved queries run read-only too.
    let r = Runner::for_user(&alice);
    assert!(
        run_unsaved(
            &r,
            &json!({"database": "shop", "sql": "UPDATE t SET a = 1"})
        )
        .await
        .is_err()
    );
    let out = run_unsaved(
        &r,
        &json!({"database": "shop", "sql": "SELECT {{n}} AS n",
                "filters": [{"name": "n", "kind": "number", "value": 1}], "params": {"n": 2}}),
    )
    .await
    .unwrap();
    assert_eq!(out["row_count"], 1);
    assert_eq!(r.ran(), [("shop".to_owned(), "SELECT 2 AS n".to_owned())]);
}

#[tokio::test]
async fn tiles_and_filters_are_checked_when_saved() {
    for s in stores() {
        tiles_and_filters_are_checked_when_saved_on(&*s).await;
    }
}

async fn the_assistants_tiles_are_accepted_into_a_dashboard_on(s: &dyn DashStore) {
    let alice = who(1, "alice", false);
    let bob = who(2, "bob", false);
    let proposal = json!({
        "dashboard": "Weekly sales", "title": "Average order value by region", "db": "shop",
        "sql": "SELECT region, AVG(total) AS aov FROM orders GROUP BY region",
        "chart": r#"{"mark": "bar", "columns": ["region"], "rows": ["aov"]}"#,
    });
    // No choice: the dashboard the assistant named, made for the caller.
    let (d1, t1) = accept_tile(s, &alice, &proposal, &json!({})).await.unwrap();
    let d = get(s, &alice, d1).await.unwrap();
    assert_eq!(d["name"], "Weekly sales");
    assert_eq!(d["shared"], false);
    assert_eq!(d["tiles"][0]["id"], t1.to_string());
    assert_eq!(d["tiles"][0]["chart"]["mark"], "bar");
    // Again: the same dashboard, found by name.
    let (d2, _) = accept_tile(s, &alice, &proposal, &json!({})).await.unwrap();
    assert_eq!(d1, d2);
    // Into a new one, with the user's own title.
    let (d3, _) = accept_tile(
        s,
        &alice,
        &proposal,
        &json!({"new_dashboard": "Board", "title": "AOV"}),
    )
    .await
    .unwrap();
    assert_ne!(d3, d1);
    assert_eq!(
        get(s, &alice, d3).await.unwrap()["tiles"][0]["title"],
        "AOV"
    );
    // Into someone else's shared dashboard: refused for bob.
    share(s, &alice, d1, true).await.unwrap();
    let e = accept_tile(s, &bob, &proposal, &json!({"dashboard": d1.to_string()}))
        .await
        .unwrap_err();
    assert_eq!(code(&e), 1227);
    // A chart the user changed to one that doesn't fit is refused.
    assert!(
        accept_tile(
            s,
            &alice,
            &proposal,
            &json!({"dashboard": d1, "chart": {"mark": "sankey", "columns": [{"field": "region"}], "rows": [{"field": "aov", "agg": "avg"}]}})
        )
        .await
        .is_err()
    );
    // A proposal with no dashboard and no choice goes to the caller's own
    // default dashboard, made the first time.
    let mut bare = proposal.clone();
    bare.as_object_mut().unwrap().remove("dashboard");
    let (d4, _) = accept_tile(s, &bob, &bare, &json!({})).await.unwrap();
    let (d5, _) = accept_tile(s, &bob, &bare, &json!({})).await.unwrap();
    assert_eq!(d4, d5);
    let d = get(s, &bob, d4).await.unwrap();
    assert_eq!(
        (d["name"].as_str(), d["mine"].as_bool()),
        (Some(DEFAULT_DASHBOARD), Some(true))
    );
}

#[tokio::test]
async fn the_assistants_tiles_are_accepted_into_a_dashboard() {
    for s in stores() {
        the_assistants_tiles_are_accepted_into_a_dashboard_on(&*s).await;
    }
}

/// The store checks a tile change with the dashboard held: a replaced tile
/// must still be there (a delete in between isn't undone), and the tile cap
/// holds for callers that checked an older copy of the dashboard.
#[tokio::test]
async fn the_store_checks_tiles_with_the_dashboard_held() {
    for s in stores() {
        let s = &*s;
        let id = s
            .create(UserId(1), "alice", "Race", false, &[])
            .await
            .unwrap();
        let t = TileReq::from_json(&tile("SELECT region, revenue FROM sales")).unwrap();
        let pos = Pos {
            x: 0,
            y: 0,
            w: 6,
            h: 4,
        };
        let tid = s.put_tile(id, None, &t, pos).await.unwrap();
        assert!(s.delete_tile(id, tid).await.unwrap());
        let e = s.put_tile(id, Some(tid), &t, pos).await.unwrap_err();
        assert!(format!("{e}").contains("has no tile"), "{e}");
        assert!(s.get(id).await.unwrap().unwrap().tiles.is_empty());
        for _ in 0..MAX_TILES {
            s.put_tile(id, None, &t, pos).await.unwrap();
        }
        let e = s.put_tile(id, None, &t, pos).await.unwrap_err();
        assert!(format!("{e}").contains("at most"), "{e}");
        assert_eq!(s.get(id).await.unwrap().unwrap().tiles.len(), MAX_TILES);
    }
}
