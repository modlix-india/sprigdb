//! The health loop end to end over a memory etcd, with a scripted
//! provider: warnings filed once and refreshed, quiet on flat series, off
//! by default, a failover written up, decisions explained, and an upgrade
//! check.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

use sprig_ai::admin::history::HOUR_MS;
use sprig_ai::incidents::{ClusterShape, Topology};
use sprig_ai::provider::Usage as Tokens;
use sprig_ai::provider::scripted::{Reply, ScriptedProvider};
use sprig_ai::settings::Lookup;
use sprig_ai::{MemoryAudit, Provider};
use sprig_approvals::MemStores;
use sprig_approvals::model::Level;
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, Tier};
use sprig_settings::Setting;

use super::*;

#[derive(Default)]
struct Values(HashMap<&'static str, String>);

impl Values {
    fn on(model: bool) -> Self {
        let mut v = Self::default();
        v.0.insert("ai.admin.enabled", "true".into());
        if model {
            v.0.insert("ai.model.admin", "scripted/claude-opus-5-5".into());
        }
        v
    }
}

impl Lookup for Values {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        self.0.get(s.key).map_or(s.default, |v| v == "true")
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        self.0
            .get(s.key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(s.default)
    }
    fn text(&self, s: &'static Setting<Cow<'static, str>>) -> String {
        self.0
            .get(s.key)
            .cloned()
            .unwrap_or_else(|| s.default.to_string())
    }
}

struct Docs;

#[async_trait::async_trait]
impl ToolHost for Docs {
    async fn call(&self, _spec: &ToolSpec, _input: &Value) -> Result<ToolOutput> {
        let mut out = ToolOutput::json(json!({"hits": [{"page": "operate/clusters",
            "text": "Add a cluster or move tenants off a full one; zqc1 is an example."}]}));
        out.sources.push(Source {
            kind: "docs".into(),
            title: "Capacity".into(),
            reference: "operate/clusters".into(),
        });
        Ok(out)
    }
}

struct Env {
    provider: Arc<ScriptedProvider>,
}

#[async_trait::async_trait]
impl IncidentEnv for Env {
    async fn model(&self, role: Role, setting: &str) -> Result<(Arc<dyn Provider>, String)> {
        let rm = sprig_ai::providers::RoleModel::parse(role, setting)?;
        Ok((Arc::clone(&self.provider) as Arc<dyn Provider>, rm.model))
    }
    async fn audit(&self) -> Result<Arc<dyn AuditStore>> {
        Ok(Arc::new(MemoryAudit::system()))
    }
    async fn topology(&self) -> Result<(Topology, Names)> {
        let mut n = Names::new();
        n.add(NameKind::Host, "zqc1");
        n.add(NameKind::Host, "zqn1");
        n.add(NameKind::Host, "zqn2");
        Ok((
            Topology {
                dcs: 1,
                gateways: 1,
                workers: 1,
                clusters: vec![ClusterShape {
                    kind: "primary_replica".into(),
                    nodes: 3,
                    server: None,
                }],
            },
            n,
        ))
    }
    fn docs(&self) -> Arc<dyn ToolHost> {
        Arc::new(Docs)
    }
}

/// A controller with one cluster whose fullness is `full`, and the events
/// and target catalog given.
#[derive(Default)]
struct Fake {
    full: StdMutex<f64>,
    events: StdMutex<Vec<Event>>,
    samples: StdMutex<u32>,
    target: StdMutex<Option<Catalog>>,
    usage: StdMutex<Usage>,
}

#[async_trait::async_trait]
impl HealthEnv for Fake {
    async fn sample(&self, _now_ms: u64) -> Result<Vec<Sample>> {
        *self.samples.lock().unwrap() += 1;
        Ok(vec![
            Sample::new(
                "sprig_balancer_cluster_full_ratio",
                &[("cluster", "zqc1"), ("dimension", "storage")],
                *self.full.lock().unwrap(),
            ),
            Sample::new(
                "sprig_backend_replica_lag_seconds",
                &[("cluster", "zqc1")],
                0.4,
            ),
        ])
    }
    async fn limits(&self) -> Limits {
        Limits::default()
    }
    async fn events(&self, since_ms: u64) -> Result<Vec<Event>> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.at_ms >= since_ms)
            .cloned()
            .collect())
    }
    async fn entries(&self, since_ms: u64) -> Result<Vec<Entry>> {
        Ok(self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.at_ms >= since_ms)
            .map(|e| Entry {
                id: e.id.clone(),
                at_ms: Some(e.at_ms),
                kind: "cluster.failover.promote".into(),
                subject: format!("cluster {}", e.cluster.clone().unwrap_or_default()),
                summary: e
                    .facts
                    .iter()
                    .map(|(_, f)| f.as_str())
                    .collect::<Vec<_>>()
                    .join("; "),
            })
            .collect())
    }
    async fn usage(&self) -> Result<Usage> {
        Ok(self.usage.lock().unwrap().clone())
    }
    async fn target(&self, _version: &str) -> Result<Option<Catalog>> {
        Ok(self.target.lock().unwrap().clone())
    }
}

fn tokens() -> Tokens {
    Tokens {
        input_tokens: 40,
        output_tokens: 10,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

struct Rig {
    hub: Arc<Approvals>,
    provider: Arc<ScriptedProvider>,
    fake: Arc<Fake>,
    l: HealthLoop,
}

fn rig(values: Values, replies: Vec<Reply>) -> Rig {
    let dc_id = DcId::new("dev1").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc_id.clone())));
    let hub = Arc::new(Approvals::new(
        Arc::new(MemStores::new()),
        Arc::new(SystemClock),
    ));
    let provider = Arc::new(ScriptedProvider::new(replies, tokens()));
    let fake = Arc::new(Fake::default());
    let l = HealthLoop::new(
        dc_id,
        &dc,
        Knobs::Fixed(Arc::new(values)),
        Arc::clone(&hub),
        Arc::new(Env {
            provider: Arc::clone(&provider),
        }),
        Arc::clone(&fake) as Arc<dyn HealthEnv>,
    );
    Rig {
        hub,
        provider,
        fake,
        l,
    }
}

fn warning_replies() -> Vec<Reply> {
    vec![
        Reply::Tools(
            String::new(),
            vec![
                (
                    "c1".into(),
                    "metrics_trend".into(),
                    json!({"name": "sprig_balancer_cluster_full_ratio", "days": 7}),
                ),
                (
                    "c2".into(),
                    "search_docs".into(),
                    json!({"query": "cluster full storage", "audience": "operator"}),
                ),
            ],
        ),
        Reply::Text(
            "Severity: high\n**Finding:** cluster zqc1 is 95% full.\n**Steps:**\n1. Add a cluster.\n**Docs:** operate/clusters".into(),
        ),
    ]
}

const T0: u64 = 20_000 * 24 * HOUR_MS;

#[tokio::test]
async fn a_full_cluster_is_warned_about_once_then_refreshed() {
    let rg = rig(Values::on(true), warning_replies());
    *rg.fake.full.lock().unwrap() = 0.95;
    let r = rg.l.pass(T0).await.unwrap();
    assert!(r.checked);
    assert_eq!(r.findings.len(), 1, "{r:?}");
    assert_eq!(r.findings[0].rule, "cluster_full");
    assert_eq!(r.filed.len(), 1, "{r:?}");
    let item = rg
        .hub
        .get(&RecId::parse(&r.filed[0]).unwrap())
        .await
        .unwrap();
    assert_eq!(item.kind, Kind::AiHealthWarning);
    assert_eq!(item.level, Level::Dc);
    assert!(item.advisory);
    assert!(item.action.text.contains("Add a cluster"));
    assert!(item.action.text.contains("Found by the health rules"));
    // The operators' copy says zqc1; the provider never saw it.
    assert!(item.summary.contains("zqc1"), "{}", item.summary);
    let reqs = rg.provider.requests();
    assert_eq!(reqs.len(), 2);
    for req in &reqs {
        let body = serde_json::to_string(&json!({"s": req.system, "m": req.messages})).unwrap();
        assert!(!body.contains("zqc1"), "{body}");
    }
    let tools: Vec<&str> = reqs[0].tools.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(tools, ["decision_log", "metrics_trend", "search_docs"]);

    // Within the interval nothing is checked; at the next check the item
    // still waits, so it is refreshed and nothing is sent.
    let r = rg.l.pass(T0 + 300_000).await.unwrap();
    assert!(!r.checked);
    let r = rg.l.pass(T0 + HOUR_MS).await.unwrap();
    assert!(r.checked);
    assert_eq!((r.filed.len(), r.refreshed), (0, 1), "{r:?}");
    assert_eq!(rg.provider.requests().len(), 2);
    let list = rg.l.parts().findings.list().await.unwrap();
    let (rec, _) = list.values().next().unwrap();
    assert_eq!((rec.status.as_str(), rec.seen), ("warned", 2));
    assert!(rec.intake.as_deref().is_some_and(|t| !t.contains("zqc1")));
}

#[tokio::test]
async fn flat_series_stay_quiet_and_it_is_off_by_default() {
    let rg = rig(Values::default(), Vec::new());
    let r = rg.l.pass(T0).await.unwrap();
    assert_eq!(r, HealthReport::default());
    assert_eq!(*rg.fake.samples.lock().unwrap(), 0, "off: nothing sampled");

    let rg = rig(Values::on(true), Vec::new());
    *rg.fake.full.lock().unwrap() = 0.5;
    for h in 0..30 {
        let r = rg.l.pass(T0 + h * HOUR_MS).await.unwrap();
        assert!(r.findings.is_empty() && r.filed.is_empty(), "{r:?}");
    }
    assert!(rg.provider.requests().is_empty());
    // The history kept the samples.
    let t =
        rg.l.parts()
            .history
            .trend(
                &TrendQuery {
                    name: "sprig_balancer_cluster_full_ratio".into(),
                    labels: vec![("cluster".into(), "zqc1".into())],
                    days: 7,
                },
                T0 + 29 * HOUR_MS,
            )
            .await
            .unwrap();
    assert_eq!(t["series"][0]["days"][0]["mean"], 0.5);
}

#[tokio::test]
async fn without_a_model_the_rules_words_are_filed() {
    let rg = rig(Values::on(false), Vec::new());
    *rg.fake.full.lock().unwrap() = 0.97;
    let r = rg.l.pass(T0).await.unwrap();
    assert_eq!(r.filed.len(), 1);
    let item = rg
        .hub
        .get(&RecId::parse(&r.filed[0]).unwrap())
        .await
        .unwrap();
    assert!(
        item.action
            .text
            .contains("Not worded by a model: ai.model.admin is empty")
    );
    assert!(rg.provider.requests().is_empty());
}

fn failover(at: u64) -> Event {
    Event {
        id: "cf-1a2b3c4d-7".into(),
        kind: "cluster_failover".into(),
        at_ms: at,
        cluster: Some("zqc1".into()),
        facts: vec![
            (Some(at - 9_000), "the writer zqn1 of cluster zqc1 stopped answering (sources: probes)".into()),
            (Some(at), "promoted replica zqn2 became the writer (writer epoch 7); recovery took 9000 ms from the first failed probe; expected loss up to 1000 ms of writes".into()),
        ],
    }
}

#[tokio::test]
async fn a_forced_failover_yields_a_writeup() {
    let rg = rig(
        Values::on(true),
        vec![
            Reply::Tools(
                String::new(),
                vec![("c1".into(), "decision_log".into(), json!({"subject": "failover"}))],
            ),
            Reply::Text(
                "**Summary:** cluster host#x lost its writer and SprigDB promoted a replica in 9 s.\n**Timeline:** ...\n**Cause:** unknown.\n**Effects:** writes failed for 9 s.\n**Follow-ups:**\n1. Check the old writer.".into(),
            ),
        ],
    );
    *rg.fake.full.lock().unwrap() = 0.5;
    let at = T0 - 20 * 60_000;
    // Too fresh: it waits for the hour after it.
    rg.fake.events.lock().unwrap().push(failover(T0 - 60_000));
    let r = rg.l.pass(T0).await.unwrap();
    assert!(r.writeups.is_empty());
    rg.fake.events.lock().unwrap()[0] = failover(at);
    let r = rg.l.pass(T0 + 60_000).await.unwrap();
    assert_eq!(r.writeups, ["cf-1a2b3c4d-7"], "{r:?}");
    let w =
        rg.l.parts()
            .writeups
            .get("cf-1a2b3c4d-7")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(w.status, "drafted");
    assert!(w.text.contains("**Follow-ups:**"));
    assert!(w.intake.contains("Write-up of cluster failover"));
    assert!(
        !w.intake.contains("zqc1") && !w.intake.contains("zqn2"),
        "{}",
        w.intake
    );
    let item = rg
        .hub
        .get(&RecId::parse(w.advice_id.as_deref().unwrap()).unwrap())
        .await
        .unwrap();
    assert_eq!(item.kind, Kind::AiIncidentAdvice);
    assert!(
        item.summary
            .starts_with("Write-up of cluster failover cf-1a2b3c4d-7"),
        "{}",
        item.summary
    );
    // Written once.
    let r = rg.l.pass(T0 + 120_000).await.unwrap();
    assert!(r.writeups.is_empty());
    assert_eq!(rg.provider.requests().len(), 2);
}

#[tokio::test]
async fn explanations_cite_the_decision_entry() {
    let rg = rig(Values::on(true), Vec::new());
    rg.fake.events.lock().unwrap().push(failover(T0 - HOUR_MS));
    let parts = rg.l.parts();
    let v = parts
        .decisions("why did cluster zqc1 fail over", 7, T0)
        .await
        .unwrap();
    assert_eq!(v["entries"][0]["id"], "cf-1a2b3c4d-7", "{v}");
    assert_eq!(v["note"], "");
    assert_eq!(cluster_tag("zqc1").len(), 8);
    // Words that match nothing still get the newest entries, with a note.
    let v = parts.decisions("catalog shards", 7, T0).await.unwrap();
    assert_eq!(v["entries"][0]["id"], "cf-1a2b3c4d-7");
    assert!(v["note"].as_str().unwrap().starts_with("nothing matched"));

    // A turn that calls decision_log cites the entry it read.
    // (The host reads the log at the real time.)
    rg.fake.events.lock().unwrap()[0] = failover(now_ms() - HOUR_MS);
    let provider = Arc::new(ScriptedProvider::new(
        vec![
            Reply::Tools(
                String::new(),
                vec![(
                    "c1".into(),
                    "decision_log".into(),
                    json!({"subject": "failover"}),
                )],
            ),
            Reply::Text(
                "SprigDB promoted a replica because the writer stopped answering (cf-1a2b3c4d-7)."
                    .into(),
            ),
        ],
        tokens(),
    ));
    let host = Arc::new(AdminHost::new(parts, Arc::new(Docs)));
    let a = Advisor::new(Arc::new(MemoryAudit::system()), host, true);
    let names = Env {
        provider: Arc::clone(&provider),
    }
    .topology()
    .await
    .unwrap()
    .1;
    let m = Model {
        provider: provider.clone(),
        model: "claude-opus-5-5".into(),
        settings: TurnSettings::read(&sprig_ai::settings::Defaults, Role::Admin),
    };
    let out = a
        .write(
            &Writing {
                title: "Why".into(),
                text: "Why did cluster zqc1 fail over?".into(),
                rules: "Explain from the decision log.",
                tools: admin::agent_tools(),
                names: &names,
                restore: vec!["zqc1".into()],
            },
            &m,
        )
        .await
        .unwrap();
    assert!(out.text.contains("cf-1a2b3c4d-7"));
    assert!(
        out.sources
            .iter()
            .any(|s| s.kind == "decision" && s.reference == "cf-1a2b3c4d-7"),
        "{:?}",
        out.sources
    );
    for req in provider.requests() {
        let body = serde_json::to_string(&json!({"m": req.messages})).unwrap();
        assert!(!body.contains("zqc1") && !body.contains("zqn2"), "{body}");
    }
}

#[tokio::test]
async fn an_upgrade_check_flags_a_renamed_key_set_here() {
    let rg = rig(Values::on(false), Vec::new());
    let mut next = current_catalog();
    let old = next.settings.remove("ai.admin.max_advice_hour").unwrap();
    next.settings.insert("ai.admin.advice_per_hour".into(), old);
    next.settings
        .get_mut("ai.admin.max_triage_hour")
        .unwrap()
        .default = "30".into();
    *rg.fake.target.lock().unwrap() = Some(next);
    *rg.fake.usage.lock().unwrap() = Usage {
        settings_set: vec![
            SetKey {
                key: "ai.admin.max_advice_hour".into(),
                scope: "global".into(),
            },
            SetKey {
                key: "ai.admin.enabled".into(),
                scope: "global".into(),
            },
        ],
        ..Usage::default()
    };
    let v =
        rg.l.upgrade_check(
            UpgradeRequest {
                to: "9.9.9".into(),
                catalog: None,
                explain: true,
            },
            T0,
        )
        .await
        .unwrap();
    assert_eq!(v["items"][0]["impact"], "breaks", "{v:#}");
    assert!(
        v["items"][0]["text"]
            .as_str()
            .unwrap()
            .contains("ai.admin.advice_per_hour")
    );
    assert!(
        v["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["subject"] == "ai.admin.max_triage_hour" && i["impact"] == "changes")
    );
    assert_eq!(v["explanation_unavailable"], "ai.model.admin is empty");
    // No pack: the error says how to pass one.
    *rg.fake.target.lock().unwrap() = None;
    let e =
        rg.l.upgrade_check(
            UpgradeRequest {
                to: "9.9.9".into(),
                ..UpgradeRequest::default()
            },
            T0,
        )
        .await
        .unwrap_err();
    assert!(e.message.contains("--pack"), "{e}");
}

#[test]
fn times_and_queries_parse() {
    assert_eq!(rfc3339_ms("1970-01-01T00:00:01Z"), Some(1000));
    assert_eq!(
        rfc3339_ms("2026-11-14T09:02:00.250Z"),
        Some(1_794_646_920_250)
    );
    assert_eq!(rfc3339_ms("nonsense"), None);
    let q = query(
        &"/x?name=a%2Eb&label=cluster%3Dzq&days=3"
            .parse::<Uri>()
            .unwrap(),
    );
    assert_eq!(q[0], ("name".into(), "a.b".into()));
    assert_eq!(q[1], ("label".into(), "cluster=zq".into()));
    let s = ShardSizes::default();
    s.record(TenantId(42), 3, 900.0, 1000.0);
    let v = s.samples(now_ms());
    assert_eq!(v[0].labels["tenant"], "t42");
    assert!((v[0].value - 0.9).abs() < 1e-9);
}
