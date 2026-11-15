//! The intake loop end to end: a forced error in a process's counts
//! becomes an `ai_incident_advice` item, with a scripted provider.

use std::borrow::Cow;
use std::collections::HashMap;

use sprig_ai::incidents::Flusher;
use sprig_ai::provider::Usage;
use sprig_ai::provider::scripted::{Reply, ScriptedProvider};
use sprig_ai::settings::Defaults;
use sprig_approvals::MemStores;
use sprig_common::incidents::Recorder;
use sprig_common::time::SystemClock;
use sprig_meta::{MemStore, Tier};
use sprig_settings::Setting;

use super::*;

/// Fixed `ai.*` values over the defaults.
#[derive(Default)]
struct Values(HashMap<&'static str, String>);

impl Values {
    fn on() -> Self {
        let mut v = Self::default();
        v.0.insert("ai.admin.enabled", "true".into());
        v.0.insert("ai.model.admin", "scripted/claude-opus-5-5".into());
        v
    }
    fn set(mut self, k: &'static str, v: &str) -> Self {
        self.0.insert(k, v.into());
        self
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
        let mut out = ToolOutput::json(json!({"hits": [{"page": "troubleshooting/failover",
            "text": "A cluster is unavailable when its probes fail; zqshop-eu-1 was one."}]}));
        out.sources.push(Source {
            kind: "docs".into(),
            title: "Cluster unavailable".into(),
            reference: "troubleshooting/failover".into(),
        });
        Ok(out)
    }
}

struct Env {
    provider: Arc<ScriptedProvider>,
    audit: Arc<MemoryAudit>,
}

#[async_trait::async_trait]
impl IncidentEnv for Env {
    async fn model(&self, role: Role, setting: &str) -> Result<(Arc<dyn Provider>, String)> {
        let rm = sprig_ai::providers::RoleModel::parse(role, setting)?;
        Ok((Arc::clone(&self.provider) as Arc<dyn Provider>, rm.model))
    }
    async fn audit(&self) -> Result<Arc<dyn AuditStore>> {
        Ok(Arc::clone(&self.audit) as Arc<dyn AuditStore>)
    }
    async fn topology(&self) -> Result<(Topology, Names)> {
        let mut n = Names::new();
        n.add(NameKind::Host, "zqshop-eu-1");
        n.add(NameKind::Address, "10.9.8.7:3306");
        n.add(NameKind::Tenant, "zqacme");
        Ok((
            Topology {
                dcs: 1,
                gateways: 2,
                workers: 1,
                clusters: vec![ClusterShape {
                    kind: "primary_replica".into(),
                    nodes: 3,
                    server: Some("11.8.2".into()),
                }],
            },
            n,
        ))
    }
    fn docs(&self) -> Arc<dyn ToolHost> {
        Arc::new(Docs)
    }
}

fn usage() -> Usage {
    Usage {
        input_tokens: 40,
        output_tokens: 10,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

fn replies() -> Vec<Reply> {
    vec![
        Reply::Text("needs_advice".into()),
        Reply::Tools(
            String::new(),
            vec![(
                "c1".into(),
                "search_docs".into(),
                json!({"query": "cluster unavailable", "audience": "operator"}),
            )],
        ),
        Reply::Text(
            "Severity: high\n**Cause:** the cluster's writer stopped answering probes.\n**Steps:**\n1. Check the writer.\n**Docs:** troubleshooting/failover"
                .into(),
        ),
    ]
}

struct Rig {
    dc: Arc<dyn MetaStore>,
    hub: Arc<Approvals>,
    provider: Arc<ScriptedProvider>,
    audit: Arc<MemoryAudit>,
    l: IncidentLoop,
}

fn rig(values: Values, replies: Vec<Reply>) -> Rig {
    let dc_id = DcId::new("dev1").unwrap();
    let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc_id.clone())));
    let hub = Arc::new(Approvals::new(
        Arc::new(MemStores::new()),
        Arc::new(SystemClock),
    ));
    let provider = Arc::new(ScriptedProvider::new(replies, usage()));
    let audit = Arc::new(MemoryAudit::system());
    let l = IncidentLoop::new(
        dc_id,
        Arc::clone(&dc),
        Knobs::Fixed(Arc::new(values)),
        Arc::clone(&hub),
        Arc::new(Env {
            provider: Arc::clone(&provider),
            audit: Arc::clone(&audit),
        }),
    );
    Rig {
        dc,
        hub,
        provider,
        audit,
        l,
    }
}

/// A process that counted `n` errors of 39100 at `loop.balancer`.
async fn flush(f: &mut Flusher, r: &Recorder, n: u64) {
    let e = SprigError::new(
        SprigCode::ClusterUnavailable,
        "cluster zqshop-eu-1 of tenant zqacme",
    );
    for _ in 0..n {
        r.record(&e, "loop.balancer");
    }
    f.flush(r.snapshot()).await.unwrap();
}

#[tokio::test]
async fn a_forced_error_becomes_advice_in_the_inbox() {
    let rg = rig(Values::on(), replies());
    let rec = Recorder::new();
    let mut f = Flusher::new(Arc::clone(&rg.dc), "controller-c1", "controller");
    flush(&mut f, &rec, 2).await;
    let t = intake::now_ms() + 1000;
    // The first pass after turning on takes what was counted as a
    // baseline.
    let r = rg.l.pass(t).await.unwrap();
    assert_eq!(r.counted, 0);
    flush(&mut f, &rec, 5).await;
    let r = rg.l.pass(t + 60_000).await.unwrap();
    assert_eq!(
        (r.counted, r.escalated, r.advised.len()),
        (1, 1, 1),
        "{r:?}"
    );
    let id = RecId::parse(&r.advised[0]).unwrap();
    assert!(r.advised[0].starts_with("dc.dev1."));
    let item = rg.hub.get(&id).await.unwrap();
    assert_eq!(item.kind, Kind::AiIncidentAdvice);
    assert_eq!(item.level, Level::Dc);
    assert!(item.advisory, "advice only");
    assert_eq!(item.state, RecState::Open);
    assert!(item.auto_apply_at_us.is_none());
    assert!(
        item.summary
            .starts_with("ER_SPRIG_CLUSTER_UNAVAILABLE in controller loop.balancer:"),
        "{}",
        item.summary
    );
    assert!(item.action.text.contains("Check the writer"));
    assert!(item.action.text.contains("--payload"));

    // What was sent: three calls, no name, no address.
    let reqs = rg.provider.requests();
    assert_eq!(reqs.len(), 3);
    for q in &reqs {
        let body = serde_json::to_string(&json!({"s": q.system, "m": q.messages})).unwrap();
        for secret in ["zqshop", "zqacme", "10.9.8.7"] {
            assert!(!body.contains(secret), "{secret} was sent: {body}");
        }
    }
    let store = IncidentStore::new(Arc::clone(&rg.dc));
    let all = store.list().await.unwrap();
    assert_eq!(all.len(), 1);
    let inc = &all[0];
    assert_eq!(inc.status, Status::Advised);
    assert_eq!(inc.total, 5);
    assert_eq!(inc.verdict.as_deref(), Some("needs_advice"));
    assert_eq!(inc.severity.as_deref(), Some("high"));
    assert_eq!(inc.sent.len(), 3);
    assert!(
        inc.intake
            .as_deref()
            .unwrap()
            .starts_with("Incident fingerprint")
    );

    // The routes show it, with the payloads on request.
    let api = IncidentsApi::new(
        Arc::clone(&rg.dc),
        Arc::new(Env {
            provider: Arc::clone(&rg.provider),
            audit: Arc::clone(&rg.audit),
        }),
    );
    let list = api.list().await.unwrap();
    assert_eq!(list["incidents"][0]["status"], "advised");
    let shown = api.show(&inc.fingerprint, true).await.unwrap();
    let msgs = shown["conversation"]["messages"].as_array().unwrap();
    assert_eq!(
        msgs.iter()
            .filter(|m| m["body"].get("payload").is_some())
            .count(),
        3
    );
    let bare = api.show(&inc.fingerprint, false).await.unwrap();
    assert!(!bare.to_string().contains("\"payload\""));
    assert!(api.show("nothex!", false).await.is_err());

    // Repeats only refresh the item's counts.
    flush(&mut f, &rec, 4).await;
    let r = rg.l.pass(t + 120_000).await.unwrap();
    assert_eq!((r.counted, r.escalated), (1, 0));
    assert_eq!(rg.provider.requests().len(), 3);
    let item = rg.hub.get(&id).await.unwrap();
    assert_eq!(item.refreshes, 1);
    assert_eq!(item.evidence["total"], 9);

    // Approving it acknowledges it; nothing is applied.
    let a = Acknowledge;
    assert!(a.progress(&item).await.unwrap().unwrap().0);
}

#[tokio::test]
async fn the_intake_is_off_by_default() {
    let mut rg = rig(Values::on(), replies());
    rg.l.knobs = Knobs::Fixed(Arc::new(Defaults));
    let rec = Recorder::new();
    let mut f = Flusher::new(Arc::clone(&rg.dc), "gateway-g1", "gateway");
    flush(&mut f, &rec, 3).await;
    let t = intake::now_ms() + 1000;
    for i in 0..3 {
        assert_eq!(
            rg.l.pass(t + i * 60_000).await.unwrap(),
            PassReport::default()
        );
    }
    assert!(
        IncidentStore::new(Arc::clone(&rg.dc))
            .list()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(rg.provider.requests().is_empty());
}

async fn one_incident(rg: &Rig) -> PassReport {
    let rec = Recorder::new();
    let mut f = Flusher::new(Arc::clone(&rg.dc), "worker-w1", "worker");
    flush(&mut f, &rec, 1).await;
    let t = intake::now_ms() + 1000;
    rg.l.pass(t).await.unwrap();
    flush(&mut f, &rec, 3).await;
    rg.l.pass(t + 60_000).await.unwrap()
}

#[tokio::test]
async fn caps_budgets_and_a_missing_model_hold_incidents() {
    let rg = rig(Values::on().set("ai.admin.max_advice_hour", "0"), replies());
    let r = one_incident(&rg).await;
    assert_eq!(r.held.get("advice_cap"), Some(&1), "{r:?}");
    assert!(rg.provider.requests().is_empty());
    let inc = &IncidentStore::new(Arc::clone(&rg.dc)).list().await.unwrap()[0];
    assert_eq!(inc.status, Status::Pending);
    assert_eq!(inc.held.as_deref(), Some("advice_cap"));

    let rg = rig(Values::on().set("ai.admin.max_triage_hour", "0"), replies());
    let r = one_incident(&rg).await;
    assert_eq!(r.held.get("triage_cap"), Some(&1), "{r:?}");

    let rg = rig(Values::on().set("ai.model.admin", ""), replies());
    assert_eq!(one_incident(&rg).await.held.get("no_model"), Some(&1));

    // A spent budget: nothing is sent, and the intake waits for 00:00 UTC.
    let rg = rig(
        Values::on().set("ai.budget.user_tokens_day", "0"),
        replies(),
    );
    let r = one_incident(&rg).await;
    assert_eq!(r.held.get("budget"), Some(&1), "{r:?}");
    assert!(rg.provider.requests().is_empty());
    let r = rg.l.pass(intake::now_ms() + 120_000).await.unwrap();
    assert_eq!(r.escalated, 0);
}

#[tokio::test]
async fn noise_files_nothing() {
    let rg = rig(Values::on(), vec![Reply::Text("noise".into())]);
    let r = one_incident(&rg).await;
    assert_eq!((r.escalated, r.advised.len()), (1, 0));
    let inc = &IncidentStore::new(Arc::clone(&rg.dc)).list().await.unwrap()[0];
    assert_eq!(inc.status, Status::Noise);
    assert_eq!(inc.verdict.as_deref(), Some("noise"));
}

#[test]
fn docs_search_keeps_operator_pages() {
    let chunk = |page: &str, audience: &str, text: &str| sprig_docs::pack::Chunk {
        chunk_id: [0; 16],
        version: "0.9.0".into(),
        page: page.into(),
        title: page.into(),
        heading_path: String::new(),
        audience: vec![audience.into()],
        text: text.into(),
        tokens: 10,
        terms: Vec::new(),
        embedding: Vec::new(),
    };
    let ix = DocsIndex::of(sprig_docs::pack::Pack {
        version: "0.9.0".into(),
        model_id: String::new(),
        model_sha256: [0; 32],
        chunks: vec![
            chunk(
                "ops/failover",
                "operator",
                "A cluster is unavailable when probes fail.",
            ),
            chunk(
                "sql/select",
                "user",
                "A cluster of rows is unavailable to SELECT.",
            ),
        ],
    });
    let hits = ix.search("cluster unavailable", 5);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].page, "ops/failover");
}
