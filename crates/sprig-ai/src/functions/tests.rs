#![allow(clippy::unwrap_used)]

use std::sync::Mutex;
use std::time::Duration;

use serde_json::json;
use sprig_common::SprigCode;

use super::*;
use crate::provider::{Reply, ScriptedProvider};
use crate::settings::Defaults;

/// A scripted surrounding: one provider, a memory cache, a usage counter.
struct Env {
    provider: Arc<ScriptedProvider>,
    cache: Arc<MemoryFnCache>,
    used: Mutex<u64>,
    embedded: Mutex<Vec<String>>,
    /// The next this many usage writes fail, with this code.
    usage_fails: Mutex<(u32, SprigCode)>,
}

impl Env {
    fn new(replies: Vec<Reply>) -> Self {
        Self {
            provider: Arc::new(ScriptedProvider::new(
                replies,
                Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                    ..Usage::default()
                },
            )),
            cache: Arc::new(MemoryFnCache::default()),
            used: Mutex::new(0),
            embedded: Mutex::new(Vec::new()),
            usage_fails: Mutex::new((0, SprigCode::CatalogUnavailable)),
        }
    }

    /// The text of every request body sent.
    fn sent(&self) -> String {
        self.provider
            .requests()
            .iter()
            .fold(String::new(), |mut a, r| {
                a.push_str(&serde_json::to_string(&r.system).unwrap());
                a.push_str(&serde_json::to_string(&r.messages).unwrap());
                a
            })
    }
}

#[async_trait]
impl FnEnv for Env {
    fn tenant(&self) -> Option<TenantId> {
        Some(TenantId(7))
    }
    fn user(&self) -> UserId {
        UserId(3)
    }
    async fn provider(&self, model: &str) -> Result<(Arc<dyn Provider>, String)> {
        let (_, m) = model.split_once('/').unwrap();
        Ok((
            Arc::clone(&self.provider) as Arc<dyn Provider>,
            m.to_owned(),
        ))
    }
    async fn embed_local(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let mut e = self.embedded.lock().unwrap();
        let out = texts
            .iter()
            .map(|t| {
                let mut v = vec![0f32; LOCAL_DIM];
                v[t.len() % LOCAL_DIM] = 1.0;
                v
            })
            .collect();
        e.extend(texts);
        Ok(out)
    }
    async fn cache(&self) -> Result<Arc<dyn FnCache>> {
        Ok(Arc::clone(&self.cache) as Arc<dyn FnCache>)
    }
    async fn usage_today(&self) -> Result<UsageToday> {
        let u = *self.used.lock().unwrap();
        Ok(UsageToday { tenant: u, user: u })
    }
    async fn add_usage(&self, usage: &Usage) -> Result<()> {
        {
            let mut f = self.usage_fails.lock().unwrap();
            if f.0 > 0 {
                f.0 -= 1;
                return Err(SprigError::new(f.1, "usage store down"));
            }
        }
        *self.used.lock().unwrap() += usage.billable();
        Ok(())
    }
}

fn settings() -> FnSettings {
    let mut s = FnSettings::read(&Defaults, true);
    s.enabled = true;
    s.model = "anthropic/claude-haiku".into();
    s
}

fn inputs(v: &[Option<&str>]) -> Vec<Option<String>> {
    v.iter().map(|x| x.map(str::to_owned)).collect()
}

fn classify_call() -> Call {
    Call::from_args(
        AiFn::Classify,
        &inputs(&[Some("bug"), Some("feature")]),
        &[],
    )
    .unwrap()
}

fn tool(name: &str, input: Value) -> Reply {
    Reply::Tools(String::new(), vec![("t1".into(), name.into(), input)])
}

#[allow(clippy::ref_option)] // called on slice elements
fn text(o: &Option<Output>) -> Option<&str> {
    match o {
        Some(Output::Text(t) | Output::Json(t)) => Some(t),
        _ => None,
    }
}

#[test]
fn defaults_are_off_and_registered() {
    let s = FnSettings::read(&Defaults, false);
    assert!(!s.enabled);
    assert_eq!(s.cache_days, 30);
    assert_eq!(s.on_budget, OnBudget::Error);
    assert!(s.model.is_empty());
    let reg = sprig_settings::Registry::global().unwrap();
    for k in [
        "ai.functions.enabled",
        "ai.functions.model",
        "ai.functions.models",
        "ai.functions.cache_days",
        "ai.functions.cache_max_entries",
        "ai.functions.on_budget",
        "ai.functions.max_output_tokens",
        "ai.functions.max_inputs",
    ] {
        assert!(reg.get(k).is_some(), "{k}");
    }
}

#[test]
fn arguments_are_checked() {
    let err = |f, a: &[Option<&str>]| {
        Call::from_args(f, &inputs(a), &["anthropic/claude-haiku".into()])
            .unwrap_err()
            .protocol()
            .mysql_errno
    };
    assert_eq!(err(AiFn::Classify, &[Some("only")]), 1582);
    assert_eq!(err(AiFn::Classify, &[Some("a"), Some("A")]), 1210);
    assert_eq!(err(AiFn::Classify, &[Some("a"), None]), 1210);
    assert_eq!(err(AiFn::Extract, &[Some("[1]")]), 1210);
    assert_eq!(err(AiFn::Extract, &[Some("{not json")]), 1210);
    assert_eq!(err(AiFn::Extract, &[]), 1582);
    assert_eq!(err(AiFn::Summarize, &[Some("0")]), 1210);
    assert_eq!(err(AiFn::Summarize, &[Some("ten")]), 1210);
    assert_eq!(err(AiFn::Embed, &[Some("a"), Some("b")]), 1582);
    assert_eq!(err(AiFn::Complete, &[Some("a"), Some("b")]), 1582);
    // A last argument naming a known model is the model; else a label.
    let known = vec!["anthropic/claude-haiku".to_owned()];
    let c = Call::from_args(
        AiFn::Classify,
        &inputs(&[Some("a"), Some("b"), Some("anthropic/claude-haiku")]),
        &known,
    )
    .unwrap();
    assert_eq!(c.labels, ["a", "b"]);
    assert_eq!(c.model.as_deref(), Some("anthropic/claude-haiku"));
    let c = Call::from_args(AiFn::Classify, &inputs(&[Some("n/a"), Some("b")]), &known).unwrap();
    assert_eq!(c.labels, ["n/a", "b"]);
    assert_eq!(c.model, None);
    let c = Call::from_args(AiFn::Summarize, &inputs(&[Some("25"), None]), &known).unwrap();
    assert_eq!((c.max_words, c.model), (Some(25), None));
    // An unknown model is refused at evaluation with 39990.
    let c = Call::from_args(AiFn::Complete, &inputs(&[Some("x/y")]), &known).unwrap();
    let e = model_of(&c, &settings()).unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::AiNotConfigured));
    // No default model at all.
    let mut s = settings();
    s.model.clear();
    let c = Call::from_args(AiFn::Complete, &[], &known).unwrap();
    assert_eq!(
        model_of(&c, &s).unwrap_err().code(),
        Some(SprigCode::AiNotConfigured)
    );
    // AI_EMBED defaults to the bundled model.
    let c = Call::from_args(AiFn::Embed, &[], &known).unwrap();
    assert_eq!(model_of(&c, &s).unwrap(), LOCAL_MODEL);
}

#[tokio::test]
async fn off_is_refused_before_anything() {
    let env = Env::new(vec![]);
    let mut s = settings();
    s.enabled = false;
    let e = evaluate(&env, &s, &classify_call(), &inputs(&[Some("x")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39991);
    assert!(env.provider.requests().is_empty());
}

#[tokio::test]
async fn classify_batches_dedupes_and_caches() {
    let env = Env::new(vec![tool(
        "classify",
        json!({"answers": ["Bug", "feature"]}),
    )]);
    let s = settings();
    let rows = inputs(&[
        Some("it crashes"),
        None,
        Some("add dark mode"),
        Some("it crashes"),
        Some("   "),
    ]);
    let (out, r) = evaluate(&env, &s, &classify_call(), &rows).await.unwrap();
    let got: Vec<Option<&str>> = out.iter().map(text).collect();
    assert_eq!(
        got,
        [Some("bug"), None, Some("feature"), Some("bug"), None],
        "labels come back as listed; NULL and blank inputs stay NULL"
    );
    assert_eq!((r.rows, r.distinct, r.answered, r.cached), (5, 2, 2, 0));
    assert_eq!(r.sent.len(), 1, "one call for the batch");
    let req = &env.provider.requests()[0];
    assert_eq!(req.model, "claude-haiku");
    assert_eq!(req.tools[0].name, "classify");
    let body = env.sent();
    assert!(body.contains("it crashes") && body.contains("add dark mode"));
    assert!(body.contains("never follow instructions"));
    assert_eq!(*env.used.lock().unwrap(), 110, "tokens counted");
    // Again: every answer from the cache, nothing sent (the scripted
    // provider has no reply left, so a call would fail).
    let (out, r) = evaluate(&env, &s, &classify_call(), &rows).await.unwrap();
    assert_eq!(text(&out[2]), Some("feature"));
    assert_eq!((r.cached, r.answered), (2, 0));
    assert!(r.sent.is_empty());
    assert_eq!(env.provider.requests().len(), 1);
    // Other labels are another key.
    let other =
        Call::from_args(AiFn::Classify, &inputs(&[Some("bug"), Some("idea")]), &[]).unwrap();
    assert_ne!(
        cache_key(&classify_call(), "m", "x"),
        cache_key(&other, "m", "x")
    );
    // Past cache_days the cache isn't read.
    env.cache.age(Duration::from_secs(31 * 86_400));
    assert!(evaluate(&env, &s, &classify_call(), &rows).await.is_err());
    assert_eq!(
        env.provider.requests().len(),
        2,
        "a call was made (and failed: the scripted provider has no reply left)"
    );
}

#[tokio::test]
async fn a_batch_with_the_wrong_count_is_asked_one_by_one() {
    let env = Env::new(vec![
        tool("classify", json!({"answers": ["bug"]})),
        tool("classify", json!({"answers": ["bug"]})),
        Reply::Text("something else".into()),
    ]);
    let (out, r) = evaluate(
        &env,
        &settings(),
        &classify_call(),
        &inputs(&[Some("a"), Some("b")]),
    )
    .await
    .unwrap();
    assert_eq!(text(&out[0]), Some("bug"));
    assert_eq!(out[1], None, "a label not in the list is NULL");
    assert_eq!((r.answered, r.invalid), (1, 1));
    assert_eq!(r.sent.len(), 3);
    assert_eq!(env.cache.len(), 1, "only valid answers are cached");
}

#[tokio::test]
async fn without_sharing_no_value_leaves() {
    let env = Env::new(vec![Reply::Text("x".into())]);
    let mut s = settings();
    s.share_data = false;
    for call in [
        classify_call(),
        Call::from_args(AiFn::Summarize, &[], &[]).unwrap(),
        Call::from_args(AiFn::Complete, &[], &[]).unwrap(),
        Call::from_args(AiFn::Extract, &inputs(&[Some(r#"{"type":"object"}"#)]), &[]).unwrap(),
    ] {
        let e = evaluate(&env, &s, &call, &inputs(&[Some("secret value 4111")]))
            .await
            .unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 39995, "{:?}", call.f);
    }
    assert!(env.provider.requests().is_empty());
    assert!(!env.sent().contains("4111"));
    // NULL inputs need nothing at all.
    let (out, _) = evaluate(&env, &s, &classify_call(), &[None]).await.unwrap();
    assert_eq!(out, [None]);
}

#[tokio::test]
async fn the_bundled_model_needs_no_sharing_and_sends_nothing() {
    let env = Env::new(vec![]);
    let mut s = settings();
    s.share_data = false;
    let call = Call::from_args(AiFn::Embed, &[], &[]).unwrap();
    let (out, r) = evaluate(
        &env,
        &s,
        &call,
        &inputs(&[Some("red shoes"), None, Some("red shoes")]),
    )
    .await
    .unwrap();
    match &out[0] {
        Some(Output::Vector(v)) => assert_eq!(v.len(), LOCAL_DIM),
        o => panic!("{o:?}"),
    }
    assert_eq!(out[0], out[2]);
    assert_eq!(out[1], None);
    assert_eq!((r.local, r.distinct), (1, 1));
    assert_eq!(*env.embedded.lock().unwrap(), ["red shoes"]);
    assert!(env.provider.requests().is_empty());
    assert!(env.cache.is_empty(), "local answers aren't cached");
    // An alias ai.embedding_models doesn't list: 39997.
    let call = Call::from_args(AiFn::Embed, &inputs(&[Some("openai/x")]), &[]).unwrap();
    let e = evaluate(&env, &s, &call, &inputs(&[Some("a")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39997);
    // A provider's embedding model it lists: not run by AI_EMBED yet (39990).
    s.embedding_models = r#"{"local:bge-small":{"provider":"local","model":"BAAI/bge-small-en-v1.5","dimensions":384},
        "small3":{"provider":"openai","model":"text-embedding-3-small","dimensions":1536},
        "mine":{"provider":"local","model":"BAAI/bge-small-en-v1.5","dimensions":384}}"#
        .into();
    assert!(s.known_models(AiFn::Embed).contains(&"small3".to_owned()));
    let call = Call::from_args(AiFn::Embed, &inputs(&[Some("small3")]), &[]).unwrap();
    let e = evaluate(&env, &s, &call, &inputs(&[Some("a")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39990);
    // Another alias of the bundled model works.
    let call = Call::from_args(AiFn::Embed, &inputs(&[Some("mine")]), &[]).unwrap();
    assert!(
        evaluate(&env, &s, &call, &inputs(&[Some("a")]))
            .await
            .is_ok()
    );
    // Bytes as MariaDB stores a VECTOR.
    let b = vector_bytes(&[1.0, 2.0]);
    assert_eq!(b, [0, 0, 0x80, 0x3f, 0, 0, 0, 0x40]);
    assert_eq!(vector_of(&b).unwrap(), [1.0, 2.0]);
}

#[tokio::test]
async fn budgets_stop_or_null() {
    let env = Env::new(vec![Reply::Text("short".into())]);
    let mut s = settings();
    s.budget.user_tokens_day = 50;
    *env.used.lock().unwrap() = 50;
    let call = Call::from_args(AiFn::Summarize, &[], &[]).unwrap();
    let e = evaluate(&env, &s, &call, &inputs(&[Some("a"), Some("b")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39992);
    assert!(env.provider.requests().is_empty());
    s.on_budget = OnBudget::Null;
    let (out, r) = evaluate(&env, &s, &call, &inputs(&[Some("a"), Some("b")]))
        .await
        .unwrap();
    assert_eq!(out, [None, None]);
    assert_eq!(r.budget_nulls, 2);
    // The budget runs out mid-statement: waves after it stop.
    *env.used.lock().unwrap() = 0;
    s.budget.user_tokens_day = 100;
    let many: Vec<Option<String>> = (0..6).map(|i| Some(format!("text {i}"))).collect();
    let env = Env::new((0..6).map(|i| Reply::Text(format!("sum {i}"))).collect());
    let (out, r) = evaluate(&env, &s, &call, &many).await.unwrap();
    assert_eq!(r.answered, PARALLEL_CALLS, "the first wave ran");
    assert_eq!(r.budget_nulls, 6 - PARALLEL_CALLS);
    assert!(out[5].is_none());
    // Too many inputs for one statement: refused before sending.
    let env = Env::new(vec![]);
    s.max_inputs = 3;
    let e = evaluate(&env, &s, &call, &many).await.unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39992);
    assert!(env.provider.requests().is_empty());
}

#[tokio::test]
async fn extract_summarize_complete_and_refusals() {
    let schema = r#"{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}"#;
    let extract = Call::from_args(AiFn::Extract, &inputs(&[Some(schema)]), &[]).unwrap();
    let env = Env::new(vec![
        tool("record", json!({"value": {"city": "Pune"}})),
        tool("record", json!({"value": {"town": "x"}})),
    ]);
    let (out, r) = evaluate(
        &env,
        &settings(),
        &extract,
        &inputs(&[Some("ship to Pune"), Some("no city")]),
    )
    .await
    .unwrap();
    assert_eq!(out[0], Some(Output::Json(r#"{"city":"Pune"}"#.into())));
    assert_eq!(out[1], None, "JSON that doesn't fit the schema is NULL");
    assert_eq!(r.invalid, 1);
    let req = &env.provider.requests()[0];
    assert_eq!(
        req.tools[0].input_schema["properties"]["value"]["required"][0],
        "city"
    );
    assert!(!req.tools[0].strict);

    let env = Env::new(vec![
        Reply::Text("  A short summary. ".into()),
        Reply::MaxTokens("cut".into()),
    ]);
    let sum = Call::from_args(AiFn::Summarize, &inputs(&[Some("12")]), &[]).unwrap();
    let (out, _) = evaluate(
        &env,
        &settings(),
        &sum,
        &inputs(&[Some("long"), Some("longer")]),
    )
    .await
    .unwrap();
    assert_eq!(text(&out[0]), Some("A short summary."));
    assert_eq!(text(&out[1]), Some("cut"));
    assert_eq!(env.cache.len(), 1, "a cut-off answer isn't cached");
    assert!(env.sent().contains("at most 12 words"));

    let env = Env::new(vec![Reply::Refuse("no".into())]);
    let c = Call::from_args(AiFn::Complete, &[], &[]).unwrap();
    let e = evaluate(&env, &settings(), &c, &inputs(&[Some("write")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39994);
    let env = Env::new(vec![Reply::Fail("down".into(), true)]);
    let e = evaluate(&env, &settings(), &c, &inputs(&[Some("write")]))
        .await
        .unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39993);
}

#[test]
fn json_answers_in_words_and_schema_checks() {
    assert_eq!(
        json_in_text("```json\n{\"a\":1}\n```"),
        Some(json!({"a": 1}))
    );
    assert!(fits(&json!(3), Some(&json!({"type": "integer"}))));
    assert!(!fits(
        &json!("3"),
        Some(&json!({"type": ["integer", "null"]}))
    ));
    assert_eq!(label_of(&["Bug".into()], " \"bug\". "), Some("Bug"));
    assert_eq!(cut("héllo", 2), "h");
}

#[tokio::test]
async fn tokens_that_cant_be_counted_fail_the_statement() {
    // A lock conflict (nothing written) is tried again and counted.
    let env = Env::new(vec![tool("classify", json!({"answers": ["bug"]}))]);
    *env.usage_fails.lock().unwrap() = (1, SprigCode::CatalogConflict);
    let (out, _) = evaluate(
        &env,
        &settings(),
        &classify_call(),
        &inputs(&[Some("it crashes")]),
    )
    .await
    .unwrap();
    assert_eq!(text(&out[0]), Some("bug"));
    assert_eq!(*env.used.lock().unwrap(), 110);
    // Any other failure may have committed: no second write, the
    // statement fails rather than spend uncounted tokens.
    let env = Env::new(vec![tool("classify", json!({"answers": ["bug"]}))]);
    *env.usage_fails.lock().unwrap() = (1, SprigCode::CatalogUnavailable);
    let e = evaluate(
        &env,
        &settings(),
        &classify_call(),
        &inputs(&[Some("it crashes")]),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogUnavailable));
    assert_eq!(
        *env.usage_fails.lock().unwrap(),
        (0, SprigCode::CatalogUnavailable)
    );
    // Two conflicts in a row: it fails too.
    let env = Env::new(vec![tool("classify", json!({"answers": ["bug"]}))]);
    *env.usage_fails.lock().unwrap() = (2, SprigCode::CatalogConflict);
    let e = evaluate(
        &env,
        &settings(),
        &classify_call(),
        &inputs(&[Some("it crashes")]),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Some(SprigCode::CatalogConflict));
}
