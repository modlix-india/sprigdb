//! The intake, incidents, the intake text and the advisor, with a memory
//! etcd and the scripted provider.

#![allow(clippy::many_single_char_names)] // p, a, r, m: provider, advisor, record, model

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proptest::prelude::*;
use serde_json::{Value, json};
use sprig_common::ids::DcId;
use sprig_common::incidents::{Count, Snapshot};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{MemStore, MetaStore, Tier};

use super::*;
use crate::MemoryAudit;
use crate::audit::{AuditStore, MsgKind};
use crate::egress::NameKind;
use crate::provider::Usage;
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::role::Role;
use crate::settings::{Defaults, TurnSettings};
use crate::tools::{Source, ToolHost, ToolOutput, ToolSpec};

const H: u64 = 3_600_000;

fn dc() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("dev1").unwrap())))
}

fn snap(started_ms: u64, counts: &[(u16, &str, u64)]) -> Snapshot {
    Snapshot {
        started_ms,
        counts: counts
            .iter()
            .map(|(code, span, n)| Count {
                code: *code,
                span: (*span).to_owned(),
                n: *n,
            })
            .collect(),
        dropped: 0,
    }
}

#[tokio::test]
async fn processes_flush_counts_and_the_leader_merges_new_ones() {
    let store = dc();
    let mut gw = Flusher::new(Arc::clone(&store), "gateway-GW 1", "gateway");
    // Nothing counted: nothing written.
    assert!(!gw.flush(snap(1000, &[])).await.unwrap());
    assert!(
        gw.flush(snap(1000, &[(1205, "mysql.query", 3)]))
            .await
            .unwrap()
    );
    // Unchanged: the lease is kept, nothing written.
    assert!(
        !gw.flush(snap(1000, &[(1205, "mysql.query", 3)]))
            .await
            .unwrap()
    );
    let recs = intake::list(&*store).await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].process, "gateway-gw_1");
    assert_eq!(recs[0].component, "gateway");
    assert_eq!(recs[0].counts[0].n, 3);

    // A leader that started before the process counts it all.
    let mut m = Merger::new(500);
    let d = m.merge(&recs);
    assert_eq!(d[&(1205, "gateway".into(), "mysql.query".into())].n, 3);
    // The next pass: only what is new.
    gw.flush(snap(
        1000,
        &[(1205, "mysql.query", 5), (39100, "loop.x", 1)],
    ))
    .await
    .unwrap();
    let d = m.merge(&intake::list(&*store).await.unwrap());
    assert_eq!(d[&(1205, "gateway".into(), "mysql.query".into())].n, 2);
    assert_eq!(d[&(39100, "gateway".into(), "loop.x".into())].n, 1);
    assert_eq!(m.merge(&intake::list(&*store).await.unwrap()).len(), 0);
    // A restart: everything the new process reports is new.
    gw.flush(snap(2000, &[(1205, "mysql.query", 1)]))
        .await
        .unwrap();
    let d = m.merge(&intake::list(&*store).await.unwrap());
    assert_eq!(d[&(1205, "gateway".into(), "mysql.query".into())].n, 1);

    // A leader that started after the process takes its counts as a
    // baseline.
    let mut late = Merger::new(5000);
    assert!(late.merge(&intake::list(&*store).await.unwrap()).is_empty());
}

fn delta(n: u64) -> intake::Delta {
    intake::Delta {
        n,
        versions: ["0.9.0".to_owned()].into(),
        processes: 2,
    }
}

#[test]
fn fingerprints_baselines_and_the_escalation_rule() {
    assert_eq!(fingerprint(1205, "gateway", "mysql.query").len(), 12);
    assert_eq!(
        fingerprint(1205, "gateway", "mysql.query"),
        fingerprint(1205, "gateway", "mysql.query")
    );
    assert_ne!(
        fingerprint(1205, "gateway", "mysql.query"),
        fingerprint(1205, "worker", "mysql.query")
    );
    let t0 = 1000 * H;
    let mut r = IncidentRecord::new(1062, "gateway", "mysql.query", t0);
    assert_eq!(r.code_name, "");
    // New: goes further.
    r.add(&delta(10), t0);
    assert_eq!(r.goes_further(true, t0, false), Some(Why::New));
    // Steady 10 an hour for three days: not rising.
    for h in 1..72 {
        r.add(&delta(10), t0 + h * H);
    }
    let now = t0 + 72 * H;
    r.add(&delta(10), now);
    assert!((r.baseline(now) - 10.0).abs() < 0.01, "{}", r.baseline(now));
    assert_eq!(r.goes_further(false, now, false), None);
    // A spike to 50 in the hour: rising.
    r.add(&delta(40), now + 1);
    assert_eq!(r.goes_further(false, now + 1, false), Some(Why::Rising));
    // ...but not while cooling down or while its advice is open.
    r.escalated_ms = Some(now);
    assert_eq!(r.goes_further(false, now + 1, false), None);
    r.escalated_ms = None;
    assert_eq!(r.goes_further(false, now + 1, true), None);
    // Buckets stay bounded.
    for h in 0..400 {
        r.add(&delta(1), now + h * H);
    }
    assert!(r.hours.len() <= record::MAX_HOURS);
    // Critical codes always go.
    let mut c = IncidentRecord::new(39442, "gateway", "dtx.commit", t0);
    c.add(&delta(1), t0);
    assert_eq!(c.code_name, "ER_SPRIG_PARTIAL_COMMIT");
    assert_eq!(c.goes_further(false, t0, false), Some(Why::Critical));
    // Pending a day: stale.
    c.pend(Why::Critical, t0);
    assert!(!c.stale_pending(t0 + H));
    assert!(c.stale_pending(t0 + 25 * H));
}

#[tokio::test]
async fn incident_records_are_written_once_per_revision() {
    let s = IncidentStore::new(dc());
    let r = IncidentRecord::new(39100, "controller", "loop.balancer", 5 * H);
    assert!(s.put(&r, None).await.unwrap());
    assert!(!s.put(&r, None).await.unwrap(), "created once");
    let (got, rev) = s.get(&r.fingerprint).await.unwrap().unwrap();
    assert_eq!(got, r);
    assert!(s.put(&got, Some(rev)).await.unwrap());
    assert!(!s.put(&got, Some(rev)).await.unwrap(), "a stale revision");
    assert_eq!(s.list().await.unwrap().len(), 1);
}

fn topology() -> Topology {
    Topology {
        dcs: 2,
        gateways: 6,
        workers: 1,
        clusters: vec![
            ClusterShape {
                kind: "primary_replica".into(),
                nodes: 3,
                server: Some("11.8.2".into()),
            },
            ClusterShape {
                kind: "primary_replica".into(),
                nodes: 3,
                server: Some("11.8.2".into()),
            },
            ClusterShape {
                kind: "galera".into(),
                nodes: 3,
                server: None,
            },
        ],
    }
}

#[test]
fn the_intake_text_has_the_eval_sets_shape() {
    let t0 = 1000 * H;
    let mut r = IncidentRecord::new(39100, "controller", "loop.balancer", t0);
    r.add(&delta(2_500_000), t0 + 10);
    let text = brief::intake(&r, Why::New, &topology(), t0 + 20);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        format!(
            "Incident fingerprint {}: code 39100 (ER_SPRIG_CLUSTER_UNAVAILABLE), component controller, span loop.balancer.",
            r.fingerprint
        )
    );
    assert!(lines[1].starts_with("Count: 2,500,000 in the last hour; 7-day baseline 0 per hour."));
    assert!(lines[1].ends_with("from 2 processes."), "{}", lines[1]);
    assert_eq!(lines[2], "New fingerprint.");
    assert_eq!(
        lines[3],
        "Topology: 2 DCs; this DC has 6 gateways, 1 worker and 3 clusters (1 galera of 3 nodes, 2 primary_replica of 3 nodes)."
    );
    assert_eq!(lines[4], "Versions: SprigDB 0.9.0; MariaDB 11.8.2.");
    // The scrubber leaves it as it is: no names, no values.
    let s = Names::new().scrubber();
    assert_eq!(s.text(&text), text);
    let m = IncidentRecord::new(1205, "gateway", "mysql.query", t0);
    assert!(brief::intake(&m, Why::Rising, &topology(), t0).contains("(a MariaDB error)"));
}

#[test]
fn answers_are_read() {
    assert_eq!(Verdict::read("noise"), Verdict::Noise);
    assert_eq!(Verdict::read(" needs_advice\n"), Verdict::NeedsAdvice);
    assert_eq!(
        Verdict::read("Not noise: needs advice"),
        Verdict::NeedsAdvice
    );
    assert_eq!(Verdict::read("I'm not sure"), Verdict::NeedsAdvice);
    let a = Advice::read(
        "Severity: **High**\n\n**Cause:** the primary was saturated.\n\n**Steps:**\n1. Check it.",
        false,
    );
    assert_eq!(a.severity, "high");
    assert_eq!(a.cause, "the primary was saturated.");
    let b = Advice::read("The pool is too small.\n1. Raise it.", true);
    assert_eq!(
        (b.severity.as_str(), b.cause.as_str()),
        ("high", "The pool is too small.")
    );
    assert_eq!(Advice::read("x", false).severity, "medium");
    assert_eq!(brief::count(999_999), "999999");
    assert_eq!(brief::count(1_234_567), "1,234,567");
}

/// Docs hits that carry every name, to check the gate scrubs tool
/// results too.
struct Docs {
    text: String,
    calls: Mutex<u32>,
}

#[async_trait]
impl ToolHost for Docs {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        assert_eq!(spec.name, "search_docs");
        assert_eq!(input["audience"], "operator");
        *self.calls.lock().unwrap() += 1;
        let mut out = ToolOutput::json(json!({"hits": [{
            "page": "troubleshooting/failover", "title": "Cluster unavailable", "text": self.text}]}));
        out.sources.push(Source {
            kind: "docs".into(),
            title: "Cluster unavailable".into(),
            reference: "troubleshooting/failover".into(),
        });
        Ok(out)
    }
}

fn usage() -> Usage {
    Usage {
        input_tokens: 50,
        output_tokens: 10,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn model(p: Arc<ScriptedProvider>, role: Role) -> Model {
    Model {
        provider: p,
        model: "claude-opus-5-5".into(),
        settings: TurnSettings::read(&Defaults, role),
    }
}

fn advice_replies() -> Vec<Reply> {
    vec![
        Reply::Tools(
            String::new(),
            vec![(
                "c1".into(),
                "search_docs".into(),
                json!({"query": "cluster unavailable", "audience": "operator"}),
            )],
        ),
        Reply::Text(
            "Severity: high\n**Cause:** the cluster's primary stopped answering probes.\n**Steps:**\n1. Check the primary.\n**Docs:** troubleshooting/failover"
                .into(),
        ),
    ]
}

#[tokio::test]
async fn advice_is_stored_with_what_was_sent() {
    let p = Arc::new(ScriptedProvider::new(
        std::iter::once(Reply::Text("needs_advice".into())).chain(advice_replies()),
        usage(),
    ));
    let audit = Arc::new(MemoryAudit::system());
    let docs = Arc::new(Docs {
        text: "Check the cluster's probes.".into(),
        calls: Mutex::new(0),
    });
    let a = Advisor::new(audit.clone(), docs.clone(), true);
    let t0 = 1000 * H;
    let mut r = IncidentRecord::new(39100, "controller", "loop.balancer", t0);
    r.add(&delta(12), t0);
    let names = Names::new();
    let topo = topology();
    let case = Case {
        incident: &r,
        why: Why::New,
        topology: &topo,
        names: &names,
        now_ms: t0,
    };
    let rep = a
        .advise(&case, None, &model(Arc::clone(&p), Role::Admin))
        .await
        .unwrap();
    assert_eq!(rep.verdict, Some(Verdict::NeedsAdvice));
    let adv = rep.advice.unwrap();
    assert_eq!(adv.severity, "high");
    assert!(adv.cause.contains("primary"));
    assert_eq!(rep.sources.len(), 1);
    assert_eq!(rep.sent.len(), 3, "triage, then two advice calls");
    assert_eq!(*docs.calls.lock().unwrap(), 1);
    // Triage offered no tools; advice only the docs search.
    let reqs = p.requests();
    assert!(reqs[0].tools.is_empty());
    let tools: Vec<&str> = reqs[1].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(tools, ["search_docs"]);
    assert!(
        reqs[0].messages[0]
            .text()
            .starts_with("Incident fingerprint")
    );
    // The conversation keeps every payload.
    let conv = rep.conversation.unwrap();
    let msgs = audit.messages(conv).await.unwrap();
    let payloads = msgs
        .iter()
        .filter(|m| m.kind == MsgKind::SystemNote && m.body.get("payload").is_some())
        .count();
    assert_eq!(payloads, 3);
}

#[tokio::test]
async fn noise_stops_after_triage_and_critical_codes_skip_it() {
    let p = Arc::new(ScriptedProvider::new(
        [Reply::Text("noise".into())],
        usage(),
    ));
    let a = Advisor::new(
        Arc::new(MemoryAudit::system()),
        Arc::new(Docs {
            text: String::new(),
            calls: Mutex::new(0),
        }),
        true,
    );
    let t0 = 1000 * H;
    let mut r = IncidentRecord::new(1062, "gateway", "mysql.query", t0);
    r.add(&delta(900), t0);
    let (names, topo) = (Names::new(), topology());
    let case = Case {
        incident: &r,
        why: Why::New,
        topology: &topo,
        names: &names,
        now_ms: t0,
    };
    let rep = a
        .advise(
            &case,
            Some(&model(Arc::clone(&p), Role::Triage)),
            &model(Arc::clone(&p), Role::Admin),
        )
        .await
        .unwrap();
    assert_eq!(rep.verdict, Some(Verdict::Noise));
    assert!(!rep.advised());
    assert_eq!(p.requests().len(), 1);

    let p = Arc::new(ScriptedProvider::new(advice_replies(), usage()));
    let mut c = IncidentRecord::new(39032, "gateway", "txn.commit", t0);
    c.add(&delta(1), t0);
    let case = Case {
        incident: &c,
        why: Why::Critical,
        topology: &topo,
        names: &names,
        now_ms: t0,
    };
    let rep = a
        .advise(
            &case,
            Some(&model(Arc::clone(&p), Role::Triage)),
            &model(Arc::clone(&p), Role::Admin),
        )
        .await
        .unwrap();
    assert_eq!(rep.verdict, None, "triage skipped");
    assert!(rep.advised());
    assert_eq!(p.requests().len(), 2);
}

#[tokio::test]
async fn a_spent_budget_stops_before_anything_is_sent() {
    let p = Arc::new(ScriptedProvider::new(
        [Reply::Text("noise".into())],
        usage(),
    ));
    let a = Advisor::new(
        Arc::new(MemoryAudit::system()),
        Arc::new(Docs {
            text: String::new(),
            calls: Mutex::new(0),
        }),
        true,
    );
    let t0 = 1000 * H;
    let mut r = IncidentRecord::new(39100, "controller", "loop.x", t0);
    r.add(&delta(1), t0);
    let (names, topo) = (Names::new(), topology());
    let case = Case {
        incident: &r,
        why: Why::New,
        topology: &topo,
        names: &names,
        now_ms: t0,
    };
    let mut m = model(Arc::clone(&p), Role::Admin);
    m.settings.budget.user_tokens_day = 0;
    let e = a.advise(&case, None, &m).await.unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiBudgetExceeded));
    assert!(p.requests().is_empty());
}

fn ident() -> impl Strategy<Value = String> {
    // `zq` keeps them apart from words in the prompts and tool schemas.
    "[a-z][a-z0-9_]{3,10}".prop_map(|s| format!("zq{s}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// No registered name, no value and no address reaches the provider,
    /// even when one finds its way into a span, a version or a docs hit.
    #[test]
    fn nothing_registered_reaches_the_provider(
        tenant in ident(), db in ident(), table in ident(), host in ident(), cluster in ident(),
        ip in (1u8..255, 0u8..255, 0u8..255, 1u8..255), port in 1024u16..65000,
        value in 1_000_000u64..u64::MAX / 2,
    ) {
        let addr = format!("{}.{}.{}.{}:{port}", ip.0, ip.1, ip.2, ip.3);
        let fqdn = format!("{host}.example.internal");
        let mut names = Names::new();
        names.add(NameKind::Tenant, &tenant);
        names.add(NameKind::Database, &db);
        names.add(NameKind::Table, &table);
        names.add(NameKind::Host, &format!("{fqdn}:{port}"));
        names.add(NameKind::Name, &cluster);
        let t0 = 1000 * H;
        let span = format!("loop.{cluster}/{tenant}.{db}");
        let mut r = IncidentRecord::new(39100, "controller", &span, t0);
        r.add(&delta(3), t0);
        let docs = format!(
            "Tenant {tenant} on {fqdn} ({addr}), host {host}: SELECT * FROM {db}.{table} WHERE id = {value}; cluster {cluster} id {value}"
        );
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let p = Arc::new(ScriptedProvider::new(
            std::iter::once(Reply::Text("needs_advice".into())).chain(advice_replies()),
            usage(),
        ));
        let a = Advisor::new(
            Arc::new(MemoryAudit::system()),
            Arc::new(Docs { text: docs, calls: Mutex::new(0) }),
            true,
        );
        let topo = topology();
        let case = Case { incident: &r, why: Why::New, topology: &topo, names: &names, now_ms: t0 };
        let rep = rt.block_on(a.advise(&case, None, &model(Arc::clone(&p), Role::Admin))).unwrap();
        prop_assert!(rep.advised());
        let reqs = p.requests();
        prop_assert_eq!(reqs.len(), 3);
        let secrets = [
            tenant.clone(), db.clone(), table.clone(), host.clone(), cluster.clone(),
            fqdn.clone(), addr.clone(), value.to_string(), format!("{}.{}.{}.{}", ip.0, ip.1, ip.2, ip.3),
        ];
        for req in &reqs {
            let body = serde_json::to_string(&json!({
                "system": req.system, "messages": req.messages, "tools": req.tools,
            })).unwrap();
            for s in &secrets {
                prop_assert!(!body.contains(s.as_str()), "{} reached the provider: {}", s, body);
            }
        }
        prop_assert!(!rep.intake.contains(&tenant));
    }
}

#[test]
fn names_register_host_parts() {
    let mut n = Names::new();
    n.add(NameKind::Host, "db-eu-2.internal:3306");
    let names: Vec<&str> = n.iter().map(|(_, s)| s.as_str()).collect();
    assert!(names.contains(&"db-eu-2.internal"));
    assert!(!names.contains(&"3306"));
    let s = n.scrubber();
    assert!(!s.text("node db-eu-2.internal is down").contains("db-eu-2"));
    let _ = SprigError::internal("unused");
}
