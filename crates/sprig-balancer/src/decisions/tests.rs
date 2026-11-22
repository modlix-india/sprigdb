#![allow(clippy::unwrap_used)]

use super::*;

fn decision(at_us: i64, subject: &str) -> NewDecision {
    NewDecision {
        decided_at_us: at_us,
        component: "balancer".to_owned(),
        kind: "shard.move".to_owned(),
        subject_type: "shard".to_owned(),
        subject_id: subject.to_owned(),
        mode: Mode::Applied,
        outcome: Outcome::Scheduled,
        reason_code: "cluster_hot".to_owned(),
        summary: "x".repeat(600),
        evidence: serde_json::json!({"gain": 0.28, "from": "mum1-c7"}),
        alternatives: Some(serde_json::json!([{"cluster": "mum1-c9", "full_after": 0.5}])),
        job_id: None,
        parent_id: None,
        actor: "ctl-1".to_owned(),
        dc_id: "mum1".to_owned(),
    }
}

#[test]
fn enums_round_trip() {
    for m in [
        Mode::Applied,
        Mode::Recommended,
        Mode::DryRun,
        Mode::Operator,
    ] {
        assert_eq!(Mode::parse(m.as_str()).unwrap(), m);
    }
    assert!(Outcome::parse("bogus").is_err());
    assert!(Outcome::Succeeded.is_final());
    assert!(!Outcome::Running.is_final());
    assert_eq!(DECISIONS_MODULE.newest(), 0);
}

#[tokio::test]
async fn mem_log_records_updates_and_purges() {
    let log = MemDecisionLog::new();
    let t = TenantId(42);
    let a = log
        .record(t, &decision(1_000_000, "t42/g1/s1"))
        .await
        .unwrap();
    let b = log
        .record(t, &decision(9_000_000, "t42/g1/s2"))
        .await
        .unwrap();
    assert_ne!(a, b);
    log.update_outcome(t, a, Outcome::Succeeded, 2_000_000)
        .await
        .unwrap();
    assert_eq!(log.of(t)[0].d.outcome, Outcome::Succeeded);
    assert_eq!(log.of(t)[0].updated_at_us, 2_000_000);
    assert_eq!(log.purge_before(5_000_000), 1);
    assert_eq!(log.of(t).len(), 1);
}

/// The record store: ids, outcomes, subject and time reads, search and
/// the retention purge by key range.
#[tokio::test]
async fn records_round_trip_and_retention() {
    use sprig_catalog::records::mem::MemRecords;
    let store: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
    let log = CatalogDecisionLog::new(Arc::clone(&store));
    let t = TenantId(57_406_000_005);
    let old = log
        .record(t, &decision(1_000_000, "t42/g1/s1"))
        .await
        .unwrap();
    let mut d = decision(1_700_000_000_000_000, "t42/g1/s1");
    d.job_id = Some([7; 16]);
    d.parent_id = Some(old);
    let new = log.record(t, &d).await.unwrap();
    let other = log
        .record(t, &decision(1_700_000_000_000_001, "t42/g1/s2"))
        .await
        .unwrap();
    assert!(old < new && new < other);
    log.update_outcome(t, new, Outcome::Succeeded, 1_700_000_000_500_000)
        .await
        .unwrap();
    let got = log.get(t, new).await.unwrap().unwrap();
    assert_eq!(got.d.outcome, Outcome::Succeeded);
    assert_eq!(got.d.job_id, Some([7; 16]));
    assert_eq!(got.d.parent_id, Some(old));
    assert_eq!(got.d.summary.chars().count(), 512);
    assert_eq!(got.d.evidence["gain"], 0.28);
    assert_eq!(got.d.decided_at_us, 1_700_000_000_000_000);
    assert_eq!(got.updated_at_us, 1_700_000_000_500_000);
    let by = log.by_subject(t, "shard", "t42/g1/s1", 10).await.unwrap();
    assert_eq!(
        by.iter().map(|r| r.decision_id).collect::<Vec<_>>(),
        vec![new, old]
    );
    let mut tx = store.begin(t).await.unwrap();
    let recent = DecisionStore::recent(&mut *tx, "balancer", 1_600_000_000_000_000, 10)
        .await
        .unwrap();
    assert_eq!(
        recent.iter().map(|r| r.decision_id).collect::<Vec<_>>(),
        vec![other, new]
    );
    assert!(
        DecisionStore::recent(&mut *tx, "advisor", 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
    drop(tx);
    let found = log.search(t, &["G1/S2".to_owned()], 0, 10).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].decision_id, other);
    assert_eq!(log.search(t, &[], 0, 2).await.unwrap().len(), 2);
    // Retention: a key range of the time index, in batches.
    assert_eq!(
        log.purge(t, 1, 1_000_001 + 86_400_000_000).await.unwrap(),
        1
    );
    assert!(log.get(t, old).await.unwrap().is_none());
    assert_eq!(
        log.by_subject(t, "shard", "t42/g1/s1", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    // Ids are never reused.
    let next = log.record(t, &decision(5, "x")).await.unwrap();
    assert!(next > other);
}

#[tokio::test]
async fn the_purge_works_in_batches() {
    use sprig_catalog::records::mem::MemRecords;
    let store: Arc<dyn RecordStore> = Arc::new(MemRecords::new());
    let log = CatalogDecisionLog::new(Arc::clone(&store));
    let t = TenantId(57_406_000_006);
    let n = u64::from(PURGE_BATCH) + 7;
    for i in 0..n {
        log.record(t, &decision(i64::try_from(i).unwrap(), "s"))
            .await
            .unwrap();
    }
    log.record(t, &decision(i64::MAX / 2, "s")).await.unwrap();
    assert_eq!(log.purge(t, 0, i64::MAX / 4).await.unwrap(), n);
    assert_eq!(log.search(t, &[], i64::MIN, 10).await.unwrap().len(), 1);
}
