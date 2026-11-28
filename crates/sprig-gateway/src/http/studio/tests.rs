use super::*;

#[test]
fn bearer_tokens() {
    assert_eq!(bearer(Some("Bearer abc")), Some("abc"));
    assert_eq!(bearer(Some("bearer  abc ")), Some("abc"));
    assert_eq!(bearer(Some("Basic abc")), None);
    assert!(is_session_token(Some("Bearer sprigat_x")));
    assert!(!is_session_token(Some("Bearer sprig_acme_k_s")));
}

#[test]
fn literals_are_quoted() {
    assert_eq!(quote_literal("a'b\\c"), "'a''b\\\\c'");
    assert_eq!(quote_literal("x\0"), "'x\\0'");
}

#[test]
fn origin_checks() {
    let mut cfg = StudioConfig::default();
    let host = Some("127.0.0.1:28080");
    // A same-origin fetch, with or without the headers.
    assert!(
        same_origin(
            &cfg,
            false,
            host,
            Some("http://127.0.0.1:28080"),
            Some("same-origin")
        )
        .is_ok()
    );
    assert!(same_origin(&cfg, false, host, None, None).is_ok());
    // Another site.
    let e = same_origin(&cfg, false, host, Some("https://evil.example"), None).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39922);
    let e = same_origin(&cfg, false, host, None, Some("cross-site")).unwrap_err();
    assert_eq!(e.protocol().mysql_errno, 39922);
    assert!(same_origin(&cfg, false, host, None, Some("same-site")).is_err());
    // The scheme matters: an https page isn't this plain-HTTP origin...
    assert!(same_origin(&cfg, false, host, Some("https://127.0.0.1:28080"), None).is_err());
    // ...unless a TLS proxy in front is listed.
    cfg.public_origins = vec!["https://db.example.com".into()];
    assert!(same_origin(&cfg, false, host, Some("https://db.example.com"), None).is_ok());
    // Studio's export form: Origin null, vouched for by Sec-Fetch-Site.
    assert!(same_origin(&cfg, false, host, Some("null"), Some("same-origin")).is_ok());
    assert!(same_origin(&cfg, false, host, Some("null"), None).is_err());
}

/// The shards view was MariaDB's (table groups, shard maps, the advisor)
/// and its routes are gone: neither Studio's routes nor the data API's
/// route table claims `/v1/shards`, so the gateway answers 404 "no route".
#[test]
fn the_shards_view_is_retired() {
    for p in ["/v1/shards", "/v1/shards/advisor", "/v1/shards/advisor/3"] {
        assert!(sprig_api::rest::find("GET", p).is_none(), "{p}");
        assert!(!sprig_api::rest::path_known(p), "{p}");
    }
    let src = include_str!("mod.rs");
    assert!(
        !src.contains("\"/v1/shards"),
        "Studio's routes still serve /v1/shards"
    );
}

/// Which client a session token belongs to: what decides whether a
/// statement is recorded with caller studio (`Sql.Execute` and `Sql.Query`
/// alike, through `SqlEngine::run_capped`). A Studio login's token says
/// `studio`, a login with `client: "api"` says `api`, and Basic or an API
/// key say nothing.
#[test]
fn session_tokens_name_their_client() {
    use base64::Engine as _;
    let token = |cli: &str| {
        let b64 = |v: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(v);
        format!(
            "Bearer {ACCESS_PREFIX}{}.{}.sig",
            b64(r#"{"alg":"EdDSA"}"#),
            b64(&format!(r#"{{"sub":"7:1","cli":"{cli}"}}"#))
        )
    };
    assert_eq!(
        session_client(Some(&token("studio"))).as_deref(),
        Some("studio")
    );
    assert_eq!(session_client(Some(&token("api"))).as_deref(), Some("api"));
    assert_eq!(session_client(Some("Basic YTpi")), None);
    assert_eq!(session_client(Some("Bearer sprig_acme_k_s")), None);
    assert_eq!(session_client(None), None);
}

/// A setting read from the catalog `load_tenant` returned is the tenant's
/// even when this gateway's cache no longer holds the tenant (a replicated
/// change dropped the slot between the load and the read): reading the
/// cache again would fall back to this gateway's scopes, where the
/// tenant's `ai.enabled` is off.
#[tokio::test]
async fn a_loaded_catalog_keeps_the_tenants_settings_when_the_cache_drops_it() {
    use sprig_common::ids::TenantId;
    use sprig_meta::{MemStore, Tier};
    use sprig_settings::{Entry, Registry, SettingsStores, TenantScope, TenantSettings};
    let registry = Registry::global().unwrap();
    let dc = DcId::new("dc-a").unwrap();
    let dc_store = Arc::new(MemStore::new(Tier::Dc(dc.clone())));
    let global = Arc::new(dc_store.with_tier(Tier::Global));
    let stores = SettingsStores::new(global, dc_store).unwrap();
    let live = LiveSettings::start(registry, stores).unwrap();
    live.ready().await;
    // No tenant services: like a cache whose slot for the tenant is gone.
    let st = StudioState::new(StudioSource::Live { live, dc }, None, None);
    let tid = TenantId(7);
    let mut rows = TenantSettings::builder(registry, tid, 3);
    rows.put(
        TenantScope::Tenant,
        "ai.enabled",
        Entry {
            value: Some(sprig_settings::Value::Bool(true)),
            bounds: None,
        },
    );
    let mut cat = sprig_catalog::TenantCatalog::empty(registry, tid);
    cat.settings = Arc::new(rows.build());
    let enabled = &sprig_ai::settings::ENABLED;
    assert!(
        !st.tenant_get(enabled, Some(tid), None),
        "the cache read falls back"
    );
    assert!(
        st.tenant_get_in(enabled, Some(&cat), None),
        "the loaded catalog answers"
    );
    assert!(!st.tenant_get_in(enabled, None, None));
}
