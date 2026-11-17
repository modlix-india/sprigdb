//! The metric history, the health rules, upgrade readiness and
//! write-ups, with a memory etcd and the scripted provider.

#![allow(clippy::float_cmp)] // exact values, written and read back

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proptest::prelude::*;
use serde_json::{Value, json};
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_meta::{MemStore, MetaStore, Tier};

use super::history::{DAY_MS, HOUR_MS, Point, day_text, series_id, trend_of};
use super::upgrade::{Catalog, Impact, SetKey, SettingInfo, Usage, diff, report};
use super::writeup::{Event, Evidence, Seen, brief};
use super::*;
use crate::MemoryAudit;
use crate::egress::NameKind;
use crate::incidents::{Advisor, ClusterShape, Model, Names, Writing};
use crate::provider::Usage as Tokens;
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::role::Role;
use crate::settings::{Defaults, TurnSettings};
use crate::tools::{Source, ToolHost, ToolOutput, ToolSpec};

const T0: u64 = 20_000 * DAY_MS;

fn dc() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("dev1").unwrap())))
}

/// A series with hourly means `f(hours ago)` over the last `hours`.
fn series(name: &str, labels: &[(&str, &str)], hours: u64, f: impl Fn(u64) -> f64) -> SeriesRecord {
    let s = Sample::new(name, labels, 0.0);
    let mut r = SeriesRecord::new(&s.name, &s.labels);
    let now_h = T0 / HOUR_MS;
    for ago in (0..hours).rev() {
        let v = f(ago);
        r.set_hour(Point::of(now_h - ago, v), T0);
    }
    r
}

#[tokio::test]
async fn samples_become_hours_days_and_trends() {
    let h = History::new(dc());
    let lbl = [("cluster", "zqc1")];
    // Two samples this hour, one the next.
    h.record(
        &[Sample::new("sprig_backend_replica_lag_seconds", &lbl, 2.0)],
        T0,
    );
    h.record(
        &[Sample::new("sprig_backend_replica_lag_seconds", &lbl, 4.0)],
        T0 + 600_000,
    );
    assert!(h.flush_due(T0 + 600_000));
    assert_eq!(h.flush(T0 + 600_000).await.unwrap(), 1);
    // Flushing again writes the same point, not twice the samples.
    assert_eq!(h.flush(T0 + 700_000).await.unwrap(), 1);
    h.record(
        &[Sample::new("sprig_backend_replica_lag_seconds", &lbl, 9.0)],
        T0 + HOUR_MS,
    );
    let all = h.series(T0 + HOUR_MS).await.unwrap();
    assert_eq!(all.len(), 1);
    let r = &all[0];
    assert_eq!(r.hours.len(), 2);
    assert_eq!(
        (r.hours[0].n, r.hours[0].mean, r.hours[0].max),
        (2, 3.0, 4.0)
    );
    assert_eq!(r.hours[1].last, 9.0);
    assert_eq!(r.days.len(), 1);
    assert_eq!(r.days[0].max, 9.0);
    assert_eq!(r.id(), series_id(&r.name, &r.labels));

    let t = h
        .trend(
            &TrendQuery::from_input(&json!({"name": "sprig_backend_replica_lag_seconds",
                "labels": ["cluster=zqc1"], "days": 7})),
            T0 + HOUR_MS,
        )
        .await
        .unwrap();
    assert_eq!(t["series"][0]["labels"]["cluster"], "zqc1");
    assert_eq!(t["series"][0]["days"][0]["day"], day_text(T0 / DAY_MS));
    assert_eq!(t["series"][0]["days"][0]["max"], 9);
    assert_eq!(t["series"][0]["last_24_hours"].as_array().unwrap().len(), 2);
    let none = h
        .trend(
            &TrendQuery::from_input(&json!({"name": "sprig_backend_replica_lag_seconds",
                "labels": ["cluster=other"], "days": 7})),
            T0,
        )
        .await
        .unwrap();
    assert!(none["series"].as_array().unwrap().is_empty());
    // A series the history doesn't keep names the ones it does.
    let e = trend_of(
        &all,
        &TrendQuery {
            name: "sprig_made_up".into(),
            labels: Vec::new(),
            days: 1,
        },
        T0,
    )
    .unwrap_err();
    assert!(e.message.contains("sprig_gateway_connections"), "{e}");

    // Two weeks later the record is pruned.
    assert_eq!(h.prune(T0 + 15 * DAY_MS).await.unwrap(), 1);
}

#[test]
fn hours_and_days_stay_in_their_windows() {
    let r = series(
        "sprig_gateway_connections",
        &[("gateway", "g1")],
        20 * 24,
        |_| 1.0,
    );
    assert_eq!(r.hours.len() as u64, history::HOURS_KEPT);
    assert!(r.days.len() as u64 <= history::DAYS_KEPT);
    assert_eq!(day_text(0), "1970-01-01");
    assert_eq!(day_text(20_407), "2025-11-15");
    assert_eq!(history::hour_text(20_407 * 24 + 5), "2025-11-15T05:00Z");
}

#[test]
#[allow(clippy::too_many_lines)] // two sets of series and their checks
fn rules_fire_on_trends_and_stay_quiet_on_flat_series() {
    let lim = Limits::default();
    // Flat, healthy series: nothing.
    let flat = vec![
        series(
            "sprig_balancer_cluster_full_ratio",
            &[("cluster", "c1"), ("dimension", "storage")],
            72,
            |_| 0.5,
        ),
        series(
            "sprig_shard_split_ratio",
            &[("tenant", "t42"), ("group", "2")],
            72,
            |_| 0.4,
        ),
        series(
            "sprig_backend_replica_lag_seconds",
            &[("cluster", "c1")],
            72,
            |_| 0.3,
        ),
        series(
            "sprig_cluster_health_state",
            &[("cluster", "c1")],
            72,
            |_| 0.0,
        ),
        series(
            "sprig_gateway_connections",
            &[("gateway", "g1")],
            72,
            |_| 500.0,
        ),
        #[allow(clippy::cast_precision_loss)]
        series(
            "sprig_backup_last_success_timestamp_seconds",
            &[("cluster", "c1"), ("kind", "full")],
            72,
            |ago| (T0 - ago * HOUR_MS) as f64 / 1000.0 - 3600.0,
        ),
        series("sprig_mover_oldest_waiting_seconds", &[], 72, |_| 60.0),
    ];
    assert!(
        rules::evaluate(&flat, &lim, T0).is_empty(),
        "{:?}",
        rules::evaluate(&flat, &lim, T0)
    );

    // Rising: storage from 0.70 to 0.85 over 3 days -> 0.9 within a day
    // or two; a shard at 0.8 growing 5% a day; lag peaks rising; a backup
    // 30 hours old; a waiting move; a gateway near its limit.
    #[allow(clippy::cast_precision_loss)]
    let rising = vec![
        series(
            "sprig_balancer_cluster_full_ratio",
            &[("cluster", "c1"), ("dimension", "storage")],
            72,
            |ago| 0.85 - 0.15 * ago as f64 / 72.0,
        ),
        series(
            "sprig_shard_split_ratio",
            &[("tenant", "t42"), ("group", "2")],
            72,
            |ago| 0.8 - 0.05 * ago as f64 / 24.0,
        ),
        series(
            "sprig_backend_replica_lag_seconds",
            &[("cluster", "c1")],
            72,
            |ago| {
                if ago % 24 == 3 {
                    30.0 - (ago / 24) as f64 * 6.0
                } else {
                    0.5
                }
            },
        ),
        series(
            "sprig_backup_last_success_timestamp_seconds",
            &[("cluster", "c1"), ("kind", "full")],
            72,
            |_| (T0 as f64 / 1000.0) - 30.0 * 3600.0,
        ),
        series("sprig_mover_oldest_waiting_seconds", &[], 72, |_| {
            7.0 * 3600.0
        }),
        series(
            "sprig_gateway_connections",
            &[("gateway", "g1")],
            72,
            |ago| if ago == 2 { 17_000.0 } else { 9_000.0 },
        ),
        series(
            "sprig_cluster_health_state",
            &[("cluster", "c2")],
            72,
            |ago| if ago < 4 { 1.0 } else { 0.0 },
        ),
    ];
    let found = rules::evaluate(&rising, &lim, T0);
    let rules_found: Vec<&str> = found.iter().map(|f| f.rule.as_str()).collect();
    for r in [
        "cluster_full",
        "shard_split",
        "replica_lag",
        "backup_age",
        "mover_backlog",
        "connections",
        "cluster_health",
    ] {
        assert!(rules_found.contains(&r), "{r} missing from {found:#?}");
    }
    // Highest severity first; projections carry their days.
    assert_eq!(found[0].severity, "high");
    let full = found.iter().find(|f| f.rule == "cluster_full").unwrap();
    assert_eq!(full.severity, "medium");
    assert!(full.days.is_some_and(|d| d < 2.0), "{full:?}");
    assert!(full.text.contains("c1"), "{}", full.text);
    let split = found.iter().find(|f| f.rule == "shard_split").unwrap();
    assert_eq!(split.severity, "low");
    assert!(split.text.contains("splits by itself"));
    // Keys are stable per rule and series.
    assert_eq!(rules::evaluate(&rising, &lim, T0)[0].key, found[0].key);

    // A stale series (nothing for a day) says nothing.
    let stale = vec![series(
        "sprig_mover_oldest_waiting_seconds",
        &[],
        72,
        |_| 9.0 * 3600.0,
    )];
    assert!(rules::evaluate(&stale, &lim, T0 + DAY_MS).is_empty());
}

#[test]
fn findings_warn_once_then_after_a_quiet_day_or_a_rise() {
    let f = rules::evaluate(
        &[series("sprig_mover_oldest_waiting_seconds", &[], 3, |_| {
            7.0 * 3600.0
        })],
        &Limits::default(),
        T0,
    )
    .remove(0);
    let mut r = FindingRecord::new(f, T0);
    assert!(r.due(false, T0));
    r.warned_ms = Some(T0);
    r.warned_severity = Some("medium".into());
    assert!(!r.due(true, T0 + 2 * DAY_MS), "never while its item waits");
    assert!(!r.due(false, T0 + HOUR_MS));
    assert!(r.due(false, T0 + DAY_MS));
    r.finding.severity = "high".into();
    assert!(r.due(false, T0 + HOUR_MS), "a rise warns again");
}

fn settings_page(rows: &[(&str, &str, &str, &str)]) -> String {
    let mut t = "## `x`\n\n| Key | Type | Default | Scopes | Bounds | Applies | Description |\n|---|---|---|---|---|---|---|\n".to_owned();
    for (k, kind, d, doc) in rows {
        let _ = writeln!(
            t,
            "| `{k}` | {kind} | `{d}` | dc, global (operator only) | - | live | {doc} |"
        );
    }
    t
}

fn info(kind: &str, d: &str, doc: &str) -> SettingInfo {
    SettingInfo {
        kind: kind.into(),
        default: d.into(),
        scopes: vec!["dc".into(), "global".into()],
        apply: "live".into(),
        doc: doc.into(),
    }
}

#[test]
fn upgrade_check_flags_a_renamed_setting_against_the_next_pack() {
    let mut running = Catalog {
        version: "1.4.2".into(),
        ..Catalog::default()
    };
    running.settings.insert(
        "gateway.legacy_auth".into(),
        info("bool", "false", "Allow the old password hash"),
    );
    running.settings.insert(
        "backend.pool_mode".into(),
        info("text", "session", "Pooling"),
    );
    running.settings.insert(
        "mover.max_lag".into(),
        info("int", "10", "Lag before a move waits"),
    );
    running.settings.insert(
        "kafka.retention_hours".into(),
        info("int", "24", "Retention"),
    );
    running.codes.insert(39051, "ER_SPRIG_OLD_NAME".into());
    running.codes.insert(39052, "ER_SPRIG_GONE".into());

    // The next version's pack, as its reference pages read: a rename, a
    // changed default, a removal, a renamed code and a new one.
    let next_settings = settings_page(&[
        (
            "gateway.legacy_password_auth",
            "bool",
            "false",
            "Allow the old password hash",
        ),
        ("backend.pool_mode", "text", "transaction", "Pooling"),
        ("kafka.retention_hours", "int", "24", "Retention"),
        ("ai.admin.check_interval", "text", "1h", "Health checks"),
    ]);
    let next_errors = "| Code | Name | SQLSTATE | Retry | HTTP | gRPC | Meaning |\n|---|---|---|---|---|---|---|\n\
| 39051 | `ER_SPRIG_NEW_NAME`<br>`SprigNewName` | HY000 | no | 400 | Internal | x |\n\
| 39060 | `ER_SPRIG_ADDED`<br>`SprigAdded` | HY000 | no | 400 | Internal | y \\| z |\n";
    let target = Catalog::from_reference("1.5.0", &[next_errors, &next_settings]);
    assert_eq!(target.settings.len(), 4);
    assert_eq!(
        target.codes.get(&39051).map(String::as_str),
        Some("ER_SPRIG_NEW_NAME")
    );
    assert_eq!(target.settings["backend.pool_mode"].default, "transaction");
    assert_eq!(
        target.settings["backend.pool_mode"].scopes,
        ["dc", "global"]
    );

    let d = diff(&running, &target);
    assert_eq!(
        d.renamed,
        [(
            "gateway.legacy_auth".to_owned(),
            "gateway.legacy_password_auth".to_owned()
        )]
    );
    assert_eq!(d.removed, ["mover.max_lag"]);
    assert_eq!(d.added, ["ai.admin.check_interval"]);
    assert_eq!(d.changed.len(), 1);
    assert_eq!(d.codes_renamed.len(), 1);
    assert_eq!(d.codes_removed, [(39052, "ER_SPRIG_GONE".to_owned())]);
    assert_eq!(d.codes_added, [39060]);

    let usage = Usage {
        settings_set: vec![SetKey {
            key: "gateway.legacy_auth".into(),
            scope: "global".into(),
        }],
        statements: BTreeMap::from([("mysql sql SELECT".to_owned(), 40)]),
        ..Usage::default()
    };
    let rep = report(&d, &usage);
    assert_eq!(rep.items[0].impact, Impact::Breaks);
    assert!(
        rep.items[0].text.contains("gateway.legacy_password_auth"),
        "{rep:#?}"
    );
    // The pool default changes and the deployment uses MySQL at its default.
    let pool = rep
        .items
        .iter()
        .find(|i| i.subject == "backend.pool_mode")
        .unwrap();
    assert_eq!(pool.impact, Impact::Changes);
    // Removed but not set here: information only.
    let gone = rep
        .items
        .iter()
        .find(|i| i.subject == "mover.max_lag")
        .unwrap();
    assert_eq!(gone.impact, Impact::Info);
    assert!(
        rep.items
            .iter()
            .any(|i| i.subject == "39051" && i.impact == Impact::Changes)
    );
    let text = rep.text();
    assert!(
        text.starts_with("Upgrade from 1.4.2 to 1.5.0: 1 that break, 3 that change"),
        "{text}"
    );
    // Nothing changes: an empty report.
    assert!(diff(&running, &running).is_empty());
}

#[test]
fn a_writeup_brief_has_the_timeline_errors_and_metrics() {
    let at = T0 - HOUR_MS / 2;
    let lag = series(
        "sprig_backend_replica_lag_seconds",
        &[("cluster", "zqc1")],
        4,
        |ago| {
            if ago == 1 { 48.0 } else { 0.5 }
        },
    );
    let e = Evidence {
        event: Event {
            id: "cf-zqc1-7".into(),
            kind: "cluster_failover".into(),
            at_ms: at,
            cluster: Some("zqc1".into()),
            facts: vec![
                (
                    Some(at - 60_000),
                    "the writer zqn1 stopped answering probes".into(),
                ),
                (
                    Some(at),
                    "promoted zqn2 (writer epoch 7); expected loss 1000 ms".into(),
                ),
            ],
        },
        incidents: vec![Seen {
            first_ms: at - 30_000,
            last_ms: at + 120_000,
            text: "39100 (ER_SPRIG_CLUSTER_UNAVAILABLE) in gateway mysql.query: 120 in the window"
                .into(),
        }],
        series: vec![lag],
    };
    let b = brief(&e);
    assert!(
        b.starts_with("Write-up of cluster failover cf-zqc1-7"),
        "{b}"
    );
    let i = b.find("stopped answering").unwrap();
    assert!(i < b.find("promoted").unwrap(), "facts in time order");
    assert!(b.contains("39100"));
    assert!(b.contains("sprig_backend_replica_lag_seconds{cluster=zqc1}"));
    assert!(b.contains("48"), "{b}");
}

/// Answers every read tool the agent's turns offer with text that carries
/// the names given.
struct Tools {
    text: String,
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl ToolHost for Tools {
    async fn call(&self, spec: &ToolSpec, _input: &Value) -> Result<ToolOutput> {
        self.calls.lock().unwrap().push(spec.name.to_owned());
        let mut out = match spec.name {
            "metrics_trend" => ToolOutput::json(
                json!({"series": [{"labels": {"cluster": self.text}, "days": []}]}),
            ),
            "decision_log" => {
                ToolOutput::json(json!({"entries": [{"id": "cf-1", "summary": self.text}]}))
            }
            _ => ToolOutput::json(
                json!({"hits": [{"page": "operate/monitoring", "text": self.text}]}),
            ),
        };
        out.sources.push(Source {
            kind: "docs".into(),
            title: "Monitoring".into(),
            reference: "operate/monitoring".into(),
        });
        Ok(out)
    }
}

fn tokens() -> Tokens {
    Tokens {
        input_tokens: 50,
        output_tokens: 10,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn model(p: Arc<ScriptedProvider>) -> Model {
    Model {
        provider: p,
        model: "claude-opus-5-5".into(),
        settings: TurnSettings::read(&Defaults, Role::Admin),
    }
}

fn topology() -> crate::incidents::Topology {
    crate::incidents::Topology {
        dcs: 1,
        gateways: 2,
        workers: 1,
        clusters: vec![ClusterShape {
            kind: "galera".into(),
            nodes: 3,
            server: None,
        }],
    }
}

fn ident() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{3,10}".prop_map(|s| format!("zq{s}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    /// A health warning and a write-up send no registered name, address or
    /// tenant id, even when the series' labels and the tools' results
    /// carry them.
    #[test]
    fn health_warnings_and_writeups_send_no_names(
        cluster in ident(), gateway in ident(), host in ident(),
        tenant in 1000u64..10_000_000,
        ip in (1u8..255, 0u8..255, 0u8..255, 1u8..255), port in 1024u16..65000,
    ) {
        let addr = format!("{}.{}.{}.{}:{port}", ip.0, ip.1, ip.2, ip.3);
        let mut names = Names::new();
        names.add(NameKind::Host, &cluster);
        names.add(NameKind::Host, &gateway);
        names.add(NameKind::Address, &format!("{host}.internal:{port}"));
        let tid = format!("t{tenant}");
        #[allow(clippy::cast_precision_loss)]
        let found = rules::evaluate(&[
            series("sprig_balancer_cluster_full_ratio", &[("cluster", &cluster), ("dimension", "storage")], 72, |_| 0.95),
            series("sprig_shard_split_ratio", &[("tenant", &tid), ("group", "2")], 24, |_| 1.2),
            series("sprig_gateway_connections", &[("gateway", &gateway)], 24, |_| 19_000.0),
        ], &Limits::default(), T0);
        prop_assert_eq!(found.len(), 3);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for f in &found {
            let p = Arc::new(ScriptedProvider::new(vec![
                Reply::Tools(String::new(), vec![
                    ("c1".into(), "metrics_trend".into(), json!({"name": f.series, "days": 7})),
                    ("c2".into(), "decision_log".into(), json!({"subject": "recent"})),
                    ("c3".into(), "search_docs".into(), json!({"query": "cluster full", "audience": "operator"})),
                ]),
                Reply::Text("Severity: high\n**Finding:** it fills.\n**Steps:**\n1. Add a cluster.".into()),
            ], tokens()));
            let tools = Tools {
                text: format!("{cluster} {gateway} {tid} {host}.internal {addr}"),
                calls: Mutex::new(Vec::new()),
            };
            let a = Advisor::new(Arc::new(MemoryAudit::system()), Arc::new(tools), true);
            let w = Writing {
                title: format!("Health warning {}", f.key),
                text: health_brief(f, &topology(), T0),
                rules: HEALTH_RULES,
                tools: agent_tools(),
                names: &names,
                restore: f.labels.values().cloned().collect(),
            };
            let out = rt.block_on(a.write(&w, &model(Arc::clone(&p)))).unwrap();
            prop_assert!(out.text.contains("Add a cluster"));
            prop_assert!(out.conversation.is_some());
            let reqs = p.requests();
            prop_assert_eq!(reqs.len(), 2);
            let secrets = [cluster.clone(), gateway.clone(), tid.clone(), host.clone(), addr.clone()];
            for req in &reqs {
                let body = serde_json::to_string(&json!({
                    "system": req.system, "messages": req.messages,
                })).unwrap();
                for s in &secrets {
                    prop_assert!(!body.contains(s.as_str()), "{} reached the provider: {}", s, body);
                }
            }
        }
    }
}

#[test]
fn agent_turns_offer_reads_only() {
    let t = agent_tools();
    let names: Vec<&str> = t.all().map(|s| s.name).collect();
    assert_eq!(names, ["decision_log", "metrics_trend", "search_docs"]);
    // The metrics_trend tool names every series the history keeps.
    let mt = crate::tools::ToolRegistry::builtin();
    let d = mt.get("metrics_trend").unwrap().description;
    for (n, _) in history::SERIES {
        assert!(d.contains(n), "{n}");
    }
}
