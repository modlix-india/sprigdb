//! Live: `SPRIG_TEST_MARIADB_SINGLE` (for example
//! `mysql://root:sprig-dev-root@127.0.0.1:13301`). Builds a catalog-shaped
//! schema `ai_digest_live_<pid>` with a week of statistics, reads the
//! facts, stores a worded digest and reads it back; the schema is dropped
//! afterwards.

use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_catalog::migrate::{MigrateOpts, Migrator};

use super::*;
use crate::audit::{CatalogAudit, MsgKind};
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::settings::Defaults;

/// The columns of `auto_indexes` and `recommendations` the facts read
/// (their modules live in crates this one doesn't depend on).
const AUTO_INDEXES: &str = "CREATE TABLE auto_indexes (index_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
    object_id BIGINT UNSIGNED NOT NULL, name VARCHAR(64) NOT NULL, definition JSON NOT NULL, \
    state VARCHAR(16) NOT NULL, state_changed_at DATETIME(6) NOT NULL)";
const RECOMMENDATIONS: &str = "CREATE TABLE recommendations (rec_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, \
    kind VARCHAR(32) NOT NULL, target VARCHAR(255) NOT NULL, state VARCHAR(16) NOT NULL, \
    created_at_us BIGINT NOT NULL)";

#[tokio::test]
#[allow(clippy::too_many_lines)] // one schema, one digest, end to end
async fn live_digest_from_a_catalog() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: set SPRIG_TEST_MARIADB_SINGLE to run it");
        return;
    };
    let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
        .pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        );
    let pool = mysql_async::Pool::new(opts);
    let schema = format!("ai_digest_live_{}", std::process::id());
    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE IF EXISTS {schema}"))
        .await
        .unwrap();
    c.query_drop(format!("CREATE DATABASE {schema}"))
        .await
        .unwrap();
    Migrator::new(&[])
        .unwrap()
        .run(&mut c, &schema, &MigrateOpts::default())
        .await
        .unwrap();
    c.query_drop(format!("USE {schema}")).await.unwrap();
    for ddl in [
        sprig_workload::store::DDL_FINGERPRINTS,
        sprig_workload::store::DDL_STATS,
        AUTO_INDEXES,
        RECOMMENDATIONS,
    ] {
        c.query_drop(ddl).await.unwrap();
    }
    // The week of Monday 2 November 2026, read on Wednesday the 11th.
    let now = 1_794_405_791_000_i64;
    let (from, to) = Cadence::Weekly.period(now);
    let day = |n: i64| {
        let ms = from + n * 86_400_000;
        format!("{} 10:00:00", date(ms))
    };
    c.query_drop(format!(
        "INSERT INTO `databases` (db_id, name, name_key, default_charset, default_collation, \
         default_group_id, state, created_at, changed_version) \
         VALUES (1, 'shop', 'shop', 'utf8mb4', 'utf8mb4_general_ci', 1, 'active', '{}', 1)",
        day(-30)
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO objects (object_id, db_id, namespace, name, name_key, kind, state, \
         schema_version, created_at, changed_version) \
         VALUES (10, 1, 'relation', 'orders', 'orders', 'table', 'active', 2, '{}', 2)",
        day(-30)
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO schema_versions (object_id, version, ddl, applied_at, changed_version) VALUES \
         (10, 1, 'CREATE TABLE orders (id INT, note TEXT DEFAULT ''carol@example.com'')', '{}', 1), \
         (10, 2, 'ALTER TABLE orders ADD COLUMN region VARCHAR(8) DEFAULT ''EMEA-SECRET''', '{}', 2)",
        day(-30),
        day(1)
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO workload_fingerprints (fingerprint_id, kind, front_end, db_id, stmt_type, \
         normalized_text, shape, object_ids, shape_version, flags, first_seen, last_seen) VALUES \
         (77, 'sql', 'mysql', 1, 'select', 'SELECT * FROM orders WHERE note = ''carol@example.com''', \
          '{{}}', '[]', 1, 'unparsed', '{0}', '{0}')",
        day(0)
    ))
    .await
    .unwrap();
    for (d, n, us) in [(-3, 1000_u64, 2_000_000_u64), (2, 3000, 30_000_000)] {
        c.query_drop(format!(
            "INSERT INTO workload_stats (fingerprint_id, bucket_start, gateway_id, part, exec_count, \
             error_count, total_us, max_us, latency_hist, rows_returned, rows_affected, \
             examined_samples, rows_examined, returned_sampled, shards_sum, shards_max, \
             scatter_count, cross_cluster, in_list_max, scan_runs, scan_sessions) \
             VALUES (77, '{}', 1, 0, {n}, 2, {us}, 90000, '', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '')",
            day(d)
        ))
        .await
        .unwrap();
    }
    c.query_drop(format!(
        "INSERT INTO auto_indexes (object_id, name, definition, state, state_changed_at) VALUES \
         (10, 'sprig_ai_orders_note', '{{\"parts\": [{{\"column\": \"note\"}}]}}', 'active', '{}')",
        day(3)
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO recommendations (kind, target, state, created_at_us) VALUES \
         ('ai_dba_advice', 'shop.orders where region = ''EMEA-SECRET''', 'open', {})",
        (from + 86_400_000) * 1000
    ))
    .await
    .unwrap();
    drop(c);

    let mut facts = Facts::new(Cadence::Weekly, from, to);
    {
        let mut c = pool.get_conn().await.unwrap();
        gather::catalog(&mut c, &schema, &mut facts, now)
            .await
            .unwrap();
    }
    assert!(facts.missing.is_empty(), "{:?}", facts.missing);
    assert_eq!(facts.schema_change_count, 1, "only the period's change");
    assert_eq!(facts.schema_changes[0].verb, "ALTER");
    assert_eq!(facts.schema_changes[0].object, "orders");
    assert_eq!(facts.workload.statements, 3000);
    assert_eq!(facts.workload_before.unwrap().statements, 1000);
    assert_eq!(facts.slowest[0].fingerprint, format!("{:016x}", 77));
    assert_eq!(facts.slowest[0].db.as_deref(), Some("shop"));
    assert_eq!(facts.auto_indexes[0].columns, vec!["note".to_owned()]);
    assert_eq!(facts.waiting_count, 1);

    // Worded and stored, with sharing off: no value in what was sent or kept.
    let rec = Arc::new(crate::eval::world::Recording::new(Arc::new(
        ScriptedProvider::new(
            [Reply::Text(
                "Statements tripled; look at the note lookup.".into(),
            )],
            Usage::default(),
        ),
    )));
    let mut settings = TurnSettings::read(&Defaults, Role::Data);
    settings.enabled = true;
    let store = CatalogDigests::new(pool.clone(), schema.clone());
    assert!(!store.exists().await.unwrap());
    let audit = Arc::new(CatalogAudit::new(pool.clone(), schema.clone()));
    let (d, new) = build(
        &store,
        audit.clone(),
        Ok((rec.clone() as Arc<dyn Provider>, "m".into())),
        &settings,
        TenantId(7),
        facts.clone(),
    )
    .await
    .unwrap();
    assert!(new);
    assert_eq!(d.status, Status::Worded);
    let sent: String = rec.bodies().iter().map(ToString::to_string).collect();
    for v in ["carol@example.com", "EMEA-SECRET"] {
        assert!(!sent.contains(v), "{v} was sent");
        assert!(
            !serde_json::to_string(&d).unwrap().contains(v),
            "{v} was kept"
        );
    }
    // Read back: once per period, newest first, with its conversation.
    let (again, new) = build(
        &store,
        audit.clone(),
        Err(crate::error::not_configured(Role::Data, "unused")),
        &settings,
        TenantId(7),
        facts,
    )
    .await
    .unwrap();
    assert!(!new);
    assert_eq!(again.id, d.id);
    assert_eq!(store.list(5).await.unwrap().len(), 1);
    let got = store.get(d.id).await.unwrap().unwrap();
    assert_eq!(got.to_ms, to);
    assert_eq!(
        got.text.as_deref(),
        Some("Statements tripled; look at the note lookup.")
    );
    store.emailed(d.id).await.unwrap();
    assert!(store.get(d.id).await.unwrap().unwrap().emailed_ms.is_some());
    let msgs = audit
        .messages(ConvId(got.conversation.unwrap()))
        .await
        .unwrap();
    assert!(msgs.iter().any(|m| m.kind == MsgKind::SystemNote));

    let mut c = pool.get_conn().await.unwrap();
    c.query_drop(format!("DROP DATABASE {schema}"))
        .await
        .unwrap();
    drop(c);
    pool.disconnect().await.unwrap();
}
