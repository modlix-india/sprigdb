//! Digests from synthetic statistics, worded by the scripted provider.

use std::sync::Arc;

use sprig_common::SprigError;

use super::*;
use crate::audit::MemoryAudit;
use crate::eval::world::Recording;
use crate::provider::scripted::{Reply, ScriptedProvider};
use crate::settings::{Defaults, Lookup};

const DAY: i64 = 86_400_000;
/// Wednesday 11 November 2026, 14:03 UTC.
const NOW: i64 = 1_794_405_791_000;

fn settings(share_data: bool, tenant_tokens_day: u64) -> TurnSettings {
    let mut s = TurnSettings::read(&Defaults, Role::Data);
    s.enabled = true;
    s.share_data = share_data;
    s.budget.tenant_tokens_day = tenant_tokens_day;
    s
}

fn usage() -> Usage {
    Usage {
        input_tokens: 800,
        output_tokens: 120,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

/// Synthetic statistics, with `planted` hidden in every free-text place a
/// value could slip into: statement text and recommendation targets.
fn facts(planted: &[String]) -> Facts {
    let (from, to) = Cadence::Weekly.period(NOW);
    let mut f = Facts::new(Cadence::Weekly, from, to);
    f.schema_changes = vec![SchemaChange {
        db: "shop".into(),
        object: "orders".into(),
        kind: "table".into(),
        verb: "ALTER".into(),
        at_ms: from + DAY,
    }];
    f.schema_change_count = 1;
    f.volumes = vec![
        Volume {
            db: "shop".into(),
            table: "orders".into(),
            rows: 1_900_000,
            bytes: 900_000_000,
            rows_before: None,
            bytes_before: None,
        },
        Volume {
            db: "shop".into(),
            table: "customers".into(),
            rows: 40_000,
            bytes: 12_000_000,
            rows_before: None,
            bytes_before: None,
        },
    ];
    f.workload = Workload {
        statements: 120_000,
        errors: 40,
        total_ms: 600_000.0,
    };
    f.workload_before = Some(Workload {
        statements: 80_000,
        errors: 10,
        total_ms: 200_000.0,
    });
    f.slowest = planted
        .iter()
        .enumerate()
        .map(|(i, v)| Statement {
            fingerprint: format!("{i:016x}"),
            db: Some("shop".into()),
            text: format!(
                "SELECT * FROM orders WHERE note = '{v}' AND total > {}",
                7_000 + i
            ),
            calls: 500,
            errors: 0,
            total_ms: 9000.0,
            max_ms: 400.0,
        })
        .collect();
    f.auto_indexes = vec![AutoIndex {
        db: "shop".into(),
        table: "orders".into(),
        name: "sprig_ai_orders_status".into(),
        columns: vec!["status".into(), "created_at".into()],
        state: "active".into(),
    }];
    f.waiting = planted
        .iter()
        .map(|v| Waiting {
            kind: "ai_dba_advice".into(),
            target: format!("orders where region = '{v}'"),
            state: "open".into(),
            age_hours: 30.0,
        })
        .collect();
    f.waiting_count = f.waiting.len() as u64;
    f
}

fn bodies(rec: &Recording) -> String {
    rec.bodies()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn periods_are_whole_utc_days_and_weeks() {
    let (from, to) = Cadence::Daily.period(NOW);
    assert_eq!(
        (date(from), date(to)),
        ("2026-11-10".into(), "2026-11-11".into())
    );
    let (from, to) = Cadence::Weekly.period(NOW);
    // The week of Monday 2 November, ending Monday 9 November.
    assert_eq!(
        (date(from), date(to)),
        ("2026-11-02".into(), "2026-11-09".into())
    );
    assert_eq!(to - from, 7 * DAY);
    assert_eq!(Cadence::parse("off"), None);
    // Off by default.
    assert_eq!(
        Cadence::parse(&Defaults.text(&crate::settings::DIGEST)),
        None
    );
}

#[test]
fn literals_become_placeholders_and_names_stay() {
    assert_eq!(
        strip_literals("SELECT a1, `t 2` FROM t3 WHERE x = 'it''s' AND y > 42.5 AND z = \"q\""),
        "SELECT a1, `t 2` FROM t3 WHERE x = ? AND y > ? AND z = ?"
    );
    assert_eq!(strip_literals("IN (?, ?)"), "IN (?, ?)");
    assert_eq!(
        strip_literals("orders_2024 LIMIT 10"),
        "orders_2024 LIMIT ?"
    );
}

#[test]
fn notable_movements_follow_fixed_thresholds() {
    let mut f = facts(&[]);
    f.compare_volumes(&[
        Volume {
            db: "shop".into(),
            table: "orders".into(),
            rows: 1_000_000,
            bytes: 500_000_000,
            rows_before: None,
            bytes_before: None,
        },
        Volume {
            db: "shop".into(),
            table: "customers".into(),
            rows: 39_900,
            bytes: 11_900_000,
            rows_before: None,
            bytes_before: None,
        },
    ]);
    let n = f.notable();
    assert!(n.iter().any(|s| s.starts_with("Statements +50%")), "{n:?}");
    assert!(
        n.iter()
            .any(|s| s.starts_with("Failed statements 10 to 40")),
        "{n:?}"
    );
    assert!(
        n.iter().any(|s| s.starts_with("Mean statement time")),
        "{n:?}"
    );
    assert!(n.iter().any(|s| s.starts_with("shop.orders")), "{n:?}");
    assert!(!n.iter().any(|s| s.starts_with("shop.customers")), "{n:?}");
    let t = f.text();
    assert!(
        t.contains("## Weekly digest, 2026-11-02 to 2026-11-08"),
        "{t}"
    );
    assert!(t.contains("ALTER table shop.orders"));
    assert!(t.contains("sprig_ai_orders_status on shop.orders (status, created_at): active"));
}

/// Values of every kind a data value could take, planted in statement
/// text and targets.
fn planted(seed: u64) -> Vec<String> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    vec![
        format!("carol{}@example.com", next() % 100_000),
        format!("ZEBRA-{:x}", next()),
        format!("{}", 4_000_000_000_000_000 + next() % 1_000_000_000_000),
        format!("Brightwave {}", next() % 1000),
    ]
}

#[tokio::test]
async fn with_sharing_off_no_value_reaches_the_provider_or_the_digest() {
    for seed in 1..=24 {
        let values = planted(seed);
        let p = Arc::new(Recording::new(Arc::new(ScriptedProvider::new(
            [Reply::Text(
                "Orders grew fast; look at the slow note lookup.".into(),
            )],
            usage(),
        ))));
        let store = MemoryDigests::default();
        let audit = Arc::new(MemoryAudit::default());
        let (d, new) = build(
            &store,
            audit.clone(),
            Ok((p.clone() as Arc<dyn Provider>, "claude-opus-5-5".into())),
            &settings(false, 1_000_000),
            TenantId(7),
            facts(&values),
        )
        .await
        .unwrap();
        assert!(new);
        assert_eq!(d.status, Status::Worded);
        let sent = bodies(&p);
        assert!(!sent.is_empty(), "the model was called");
        let stored = serde_json::to_string(&d).unwrap();
        for v in &values {
            assert!(!sent.contains(v.as_str()), "seed {seed}: {v} was sent");
            assert!(!stored.contains(v.as_str()), "seed {seed}: {v} was stored");
            assert!(!d.body().contains(v.as_str()));
        }
        // The statistics themselves still went: names, counts.
        assert!(sent.contains("shop.orders") && sent.contains("120000"));
        assert!(d.body().contains("## In short"));
    }
}

#[tokio::test]
async fn with_sharing_on_statement_text_is_kept() {
    let values = planted(99);
    let p = Arc::new(Recording::new(Arc::new(ScriptedProvider::new(
        [Reply::Text("ok".into())],
        usage(),
    ))));
    let (_, _) = build(
        &MemoryDigests::default(),
        Arc::new(MemoryAudit::default()),
        Ok((p.clone() as Arc<dyn Provider>, "m".into())),
        &settings(true, 1_000_000),
        TenantId(7),
        facts(&values),
    )
    .await
    .unwrap();
    assert!(bodies(&p).contains(&values[1]));
}

#[tokio::test]
async fn a_spent_budget_or_no_model_keeps_the_facts_alone() {
    // A spent tenant budget: the provider is never called.
    let scripted = Arc::new(ScriptedProvider::new([Reply::Text("x".into())], usage()));
    let store = MemoryDigests::default();
    let (d, _) = build(
        &store,
        Arc::new(MemoryAudit::default()),
        Ok((scripted.clone() as Arc<dyn Provider>, "m".into())),
        &settings(false, 0),
        TenantId(7),
        facts(&[]),
    )
    .await
    .unwrap();
    assert_eq!(d.status, Status::Facts);
    assert!(
        d.note.as_deref().unwrap().contains("budget"),
        "{:?}",
        d.note
    );
    assert_eq!(scripted.remaining(), 1, "no call was made");
    assert!(d.body().contains("## Weekly digest"));

    // No model.
    let store = MemoryDigests::default();
    let (d, _) = build(
        &store,
        Arc::new(MemoryAudit::default()),
        Err(SprigError::new(
            SprigCode::AiNotConfigured,
            "ai.model.data is empty",
        )),
        &settings(false, 1_000_000),
        TenantId(7),
        facts(&[]),
    )
    .await
    .unwrap();
    assert_eq!(d.status, Status::Facts);
    assert!(d.note.as_deref().unwrap().contains("ai.model.data"));
}

#[tokio::test]
async fn one_digest_per_period_and_volumes_compare_with_the_last() {
    let store = MemoryDigests::default();
    let audit = Arc::new(MemoryAudit::default());
    let run = |f: Facts| {
        let p = Arc::new(ScriptedProvider::new([Reply::Text("ok".into())], usage()));
        let store = &store;
        let audit = audit.clone();
        async move {
            build(
                store,
                audit,
                Ok((p as Arc<dyn Provider>, "m".into())),
                &settings(false, 1_000_000),
                TenantId(7),
                f,
            )
            .await
            .unwrap()
        }
    };
    let first = facts(&[]);
    let (a, new) = run(first.clone()).await;
    assert!(new);
    let (b, new) = run(first.clone()).await;
    assert!(!new, "the same period isn't built twice");
    assert_eq!(a.id, b.id);
    // The next week: orders before = this week's.
    let mut next = first;
    next.from_ms += 7 * DAY;
    next.to_ms += 7 * DAY;
    next.volumes[0].rows = 3_000_000;
    let (c, new) = run(next).await;
    assert!(new);
    assert_eq!(c.facts.volumes[0].rows_before, Some(1_900_000));
    assert_eq!(store.list(10).await.unwrap()[0].id, c.id, "newest first");
    // The wording turn was stored and budgeted under the digest's user.
    let used = audit.usage_today(DIGEST_USER).await.unwrap();
    assert!(used.tenant > 0);
}
