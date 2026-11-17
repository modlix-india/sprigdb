//! The worker's settings follow etcd: a dc-scope change reaches what jobs
//! resolve within 2 s.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::CancellationToken;
use sprig_common::ids::DcId;
use sprig_meta::{MemStore, MetaStore, Op, Record, Tier, Txn};
use sprig_mover::MoverSettings;
use sprig_settings::etcd::SettingRecord;
use sprig_settings::{SettingCtx, Value};

use super::WorkerSettings;

/// A settings change reaches every gateway within 2 s; workers too.
const DELAY: Duration = Duration::from_secs(2);

async fn put(store: &dyn MetaStore, rel: &str, v: i64) {
    let rec = Record::new(SettingRecord {
        value: Some(Value::Int(v)),
        set_by: "op:test".into(),
        ..SettingRecord::default()
    });
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key: store.tier().key(rel).unwrap(),
                value: rec.encode().unwrap(),
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
}

async fn within<T: PartialEq + std::fmt::Debug>(want: &T, f: impl Fn() -> T) -> Duration {
    let t0 = Instant::now();
    loop {
        if f() == *want {
            return t0.elapsed();
        }
        assert!(
            t0.elapsed() < DELAY,
            "not applied within {DELAY:?}: {:?}",
            f()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn dc_scope_changes_reach_job_settings_within_two_seconds() {
    let dc = DcId::new("mum1").unwrap();
    let mem = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(mem.with_tier(Tier::Global));
    let dc_store: Arc<dyn MetaStore> = Arc::new(mem);
    let stop = CancellationToken::new();
    let s = WorkerSettings::start(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        dc.clone(),
        &stop,
    )
    .await
    .unwrap()
    .unwrap();
    let ctx = || {
        let snap = s.snapshot();
        MoverSettings::resolve(&snap, &SettingCtx::process(&dc))
    };
    assert_eq!(ctx().source_retain, Duration::from_secs(24 * 3600));

    // The bug: `mover.source_retain_hours = 0` at dc scope.
    put(&*dc_store, "settings/mover.source_retain_hours", 0).await;
    let took = within(&Duration::ZERO, || ctx().source_retain).await;
    eprintln!("dc-scope mover.source_retain_hours reached the worker in {took:?}");

    // Global under dc: the dc value still wins; a later global change of
    // another key arrives too.
    put(&*global, "settings/mover.copy_chunk_rows", 77).await;
    within(&77, || ctx().copy_chunk_rows).await;
    assert_eq!(ctx().source_retain, Duration::ZERO);

    // Trigger settings, re-read at every failure through `RunOpts::live`.
    let opts = s.run_opts();
    assert_eq!(opts.max_attempts, 10);
    put(&*global, "settings/logic.trigger_max_attempts", 3).await;
    let took = within(&3, || opts.current().max_attempts).await;
    eprintln!("global logic.trigger_max_attempts reached a running trigger in {took:?}");

    // kv.* for the sweeper's next pass.
    put(&*dc_store, "settings/kv.purge_interval_seconds", 2).await;
    within(&2, || {
        s.get(&sprig_controller::kv_sweep_loop::PURGE_INTERVAL_SECONDS)
    })
    .await;
    stop.cancel();
}

#[tokio::test]
async fn defaults_without_mirrors() {
    let s = WorkerSettings::defaults(DcId::new("mum1").unwrap()).unwrap();
    assert!(s.live().is_none());
    assert_eq!(s.trigger_budget(), Duration::from_secs(30));
    assert_eq!(s.job_budget(), Duration::from_secs(300));
    assert_eq!(s.run_opts().lanes, 8);
}

/// A tenant's own catalog rows win over the etcd
/// scopes for its jobs: trigger options and the mover's cutover hold.
#[tokio::test]
async fn tenant_rows_win_for_that_tenants_jobs() {
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_common::ids::TenantId;
    use sprig_controller::tenant_scopes::{TenantScopes, build_rows};

    let dc = DcId::new("mum1").unwrap();
    let mem = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(mem.with_tier(Tier::Global));
    let dc_store: Arc<dyn MetaStore> = Arc::new(mem);
    let stop = CancellationToken::new();
    let scopes = TenantScopes::new(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        Arc::new(sprig_controller::catalogs::StaticPools::new()),
    )
    .unwrap();
    let s = WorkerSettings::start(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        dc.clone(),
        &stop,
    )
    .await
    .unwrap()
    .unwrap()
    .with_tenant_scopes(Arc::clone(&scopes));
    put(&*global, "settings/logic.trigger_max_attempts", 3).await;
    put(&*dc_store, "settings/mover.cutover_max_hold_ms", 4000).await;
    within(&3, || s.run_opts().max_attempts).await;
    let t = TenantId(77);
    let row = |key: &str, v: serde_json::Value| SettingRow {
        scope: SettingScope::Tenant,
        key: key.into(),
        value: Some(v),
        bounds: None,
        set_by: "op:test".into(),
        set_at: std::time::SystemTime::UNIX_EPOCH,
        version: 2,
    };
    scopes.seed(
        t,
        2,
        build_rows(
            sprig_settings::Registry::global().unwrap(),
            t,
            2,
            vec![
                row("logic.trigger_max_attempts", serde_json::json!(7)),
                row("mover.cutover_max_hold_ms", serde_json::json!(900)),
                row("logic.job_budget_ms", serde_json::json!(1500)),
            ],
        ),
    );
    // The tenant's trigger: its own value; any other tenant: the global.
    assert_eq!(s.run_opts_for(t).max_attempts, 7);
    assert_eq!(s.run_opts_for(TenantId(78)).max_attempts, 3);
    // A scheduled run's budget, read per tenant as declared.
    assert_eq!(s.job_budget_for(t), Duration::from_millis(1500));
    assert_eq!(s.job_budget_for(TenantId(78)), Duration::from_secs(300));
    // The mover: the tenant's value over the dc's.
    let snap = s.snapshot();
    let rows = scopes.cached(t);
    let ctx = SettingCtx::process(&dc).for_tenant(t, &[]);
    assert_eq!(
        MoverSettings::resolve_with(&snap, rows.as_deref(), &ctx).cutover_max_hold,
        Duration::from_millis(900)
    );
    assert_eq!(
        MoverSettings::resolve(&snap, &ctx).cutover_max_hold,
        Duration::from_millis(4000)
    );
    stop.cancel();
}

/// Worker settings over a fresh mirror, with `rows` seeded as tenant
/// `t`'s catalog rows.
async fn seeded(
    t: sprig_common::ids::TenantId,
    rows: Vec<sprig_catalog::SettingRow>,
) -> (WorkerSettings, CancellationToken) {
    use sprig_controller::tenant_scopes::{TenantScopes, build_rows};
    let dc = DcId::new("mum1").unwrap();
    let mem = MemStore::new(Tier::Dc(dc.clone()));
    let global: Arc<dyn MetaStore> = Arc::new(mem.with_tier(Tier::Global));
    let dc_store: Arc<dyn MetaStore> = Arc::new(mem);
    let stop = CancellationToken::new();
    let scopes = TenantScopes::new(
        Arc::clone(&global),
        Arc::clone(&dc_store),
        Arc::new(sprig_controller::catalogs::StaticPools::new()),
    )
    .unwrap();
    let s = WorkerSettings::start(global, dc_store, dc, &stop)
        .await
        .unwrap()
        .unwrap()
        .with_tenant_scopes(Arc::clone(&scopes));
    scopes.seed(
        t,
        2,
        build_rows(sprig_settings::Registry::global().unwrap(), t, 2, rows),
    );
    (s, stop)
}

/// Worker settings seeded with tenant 79's `logic.*` rows at tenant,
/// database 5 and table 11 scope.
async fn seeded_trigger_rows() -> (WorkerSettings, CancellationToken) {
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_common::ids::{DbId, ObjectId, TenantId};

    let row = |scope: SettingScope, key: &str, v: serde_json::Value| SettingRow {
        scope,
        key: key.into(),
        value: Some(v),
        bounds: None,
        set_by: "op:test".into(),
        set_at: std::time::SystemTime::UNIX_EPOCH,
        version: 2,
    };
    let db = SettingScope::Database(DbId(5));
    let table = SettingScope::Object(ObjectId(11));
    seeded(
        TenantId(79),
        vec![
            row(
                SettingScope::Tenant,
                "logic.trigger_max_attempts",
                serde_json::json!(7),
            ),
            row(db, "logic.trigger_max_attempts", serde_json::json!(2)),
            row(db, "logic.trigger_retry_initial_ms", serde_json::json!(100)),
            row(db, "logic.trigger_budget_ms", serde_json::json!(4000)),
            row(db, "logic.job_budget_ms", serde_json::json!(9000)),
            row(table, "logic.trigger_max_attempts", serde_json::json!(4)),
            row(table, "logic.trigger_lanes", serde_json::json!(3)),
        ],
    )
    .await
}

/// A trigger reads its table's and its database's `logic.trigger_*` rows
/// over its tenant's: a value set with `ALTER DATABASE … SPRIG SET` was
/// accepted but ignored before.
#[tokio::test]
async fn database_and_table_rows_reach_a_trigger() {
    use sprig_common::ids::TenantId;

    use super::LogicScope;

    let t = TenantId(79);
    let (s, stop) = seeded_trigger_rows().await;
    // The tenant alone, another database, the database, its table.
    assert_eq!(s.run_opts_for(t).max_attempts, 7);
    assert_eq!(s.run_opts_for(LogicScope::db(t, 6)).max_attempts, 7);
    let in_db = s.run_opts_for(LogicScope::db(t, 5));
    assert_eq!(in_db.max_attempts, 2);
    assert_eq!(in_db.retry_initial, Duration::from_millis(100));
    assert_eq!(in_db.lanes, 8);
    let on_table = s.run_opts_for(LogicScope::db(t, 5).with_object(Some(11)));
    assert_eq!(on_table.max_attempts, 4);
    assert_eq!(on_table.retry_initial, Duration::from_millis(100));
    assert_eq!(on_table.lanes, 3);
    // A stream's or queue's trigger has no table.
    assert_eq!(
        s.run_opts_for(LogicScope::db(t, 5).with_object(Some(0)))
            .max_attempts,
        2
    );
    // Read again at each failure.
    assert_eq!(
        s.run_opts_for(LogicScope::db(t, 5)).current().max_attempts,
        2
    );
    stop.cancel();
}

/// Trigger and job budgets follow the database's rows too.
#[tokio::test]
async fn database_rows_reach_trigger_and_job_budgets() {
    use sprig_common::ids::TenantId;

    use super::LogicScope;

    let t = TenantId(79);
    let (s, stop) = seeded_trigger_rows().await;
    assert_eq!(
        s.trigger_budget_for(LogicScope::db(t, 5)),
        Duration::from_secs(4)
    );
    assert_eq!(s.trigger_budget_for(t), Duration::from_secs(30));
    assert_eq!(
        s.job_budget_for(LogicScope::db(t, 5)),
        Duration::from_secs(9)
    );
    stop.cancel();
}

/// A running trigger job follows its budget: the value it reads at each
/// run changes when the database's row does, without a new job.
#[tokio::test]
async fn a_running_trigger_follows_its_budget() {
    use sprig_catalog::{SettingRow, SettingScope};
    use sprig_common::ids::{DbId, TenantId};
    use sprig_controller::tenant_scopes::build_rows;

    use super::LogicScope;

    let t = TenantId(79);
    let (s, stop) = seeded_trigger_rows().await;
    let live = s.live_trigger_budget(LogicScope::db(t, 5));
    assert_eq!((live.0)(), Duration::from_secs(4));
    let row = SettingRow {
        scope: SettingScope::Database(DbId(5)),
        key: "logic.trigger_budget_ms".into(),
        value: Some(serde_json::json!(300)),
        bounds: None,
        set_by: "op:test".into(),
        set_at: std::time::SystemTime::UNIX_EPOCH,
        version: 3,
    };
    s.tenant_scopes().unwrap().seed(
        t,
        3,
        build_rows(sprig_settings::Registry::global().unwrap(), t, 3, vec![row]),
    );
    assert_eq!((live.0)(), Duration::from_millis(300));
    // Without tenant scopes (tests' workers), refreshing is a no-op.
    WorkerSettings::defaults(DcId::new("mum1").unwrap())
        .unwrap()
        .refresh_tenant(t)
        .await;
    stop.cancel();
}
