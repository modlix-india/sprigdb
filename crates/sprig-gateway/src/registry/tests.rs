use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::DcId;
use sprig_common::time::{Clock, SystemClock};
use sprig_meta::{MemStore, MetaStore, Tier};

use super::*;

fn record(id: &str) -> GatewayRecord {
    GatewayRecord {
        gateway_id: id.to_owned(),
        slot: 0,
        version: "test".into(),
        addr: "127.0.0.1".into(),
        listeners: BTreeMap::from([("mysql".to_owned(), 3306)]),
        state: GatewayState::Starting,
        catalog_formats: vec![],
        authorizers: vec![],
        load: Load::default(),
        kafka_host: None,
        degraded: std::collections::BTreeMap::new(),
    }
}

fn store() -> Arc<dyn MetaStore> {
    Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-t").unwrap())))
}

#[tokio::test]
async fn gateways_get_distinct_slots_and_records() {
    let store = store();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let mut tasks = Vec::new();
    let mut tables = Vec::new();
    for id in ["gw-a", "gw-b", "gw-c"] {
        let r = Registrar::new(record(id));
        let sessions = Arc::new(SessionTable::new(clock.clone()));
        tasks.push(
            r.start(
                store.clone(),
                clock.clone(),
                sessions.clone(),
                || Duration::from_secs(5),
                stop.clone(),
            )
            .await,
        );
        tables.push(sessions);
    }
    let slots: std::collections::BTreeSet<u16> = tables.iter().map(|t| t.slot()).collect();
    assert_eq!(slots.len(), 3, "distinct slots: {slots:?}");
    assert!(!slots.contains(&0));
    let all = list(store.as_ref()).await.unwrap();
    assert_eq!(all.len(), 3);
    let b = get(store.as_ref(), "gw-b").await.unwrap().unwrap();
    assert_eq!(b.slot, tables[1].slot());
    let holder = slot_holder(store.as_ref(), tables[2].slot())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(holder.gateway_id, "gw-c");
    stop.cancel();
    for t in tasks {
        t.await.unwrap();
    }
    // Stopping gives the records and slots back.
    assert!(list(store.as_ref()).await.unwrap().is_empty());
}

#[tokio::test]
async fn state_changes_are_written_at_once() {
    let store = store();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let r = Registrar::new(record("gw-a"));
    let sessions = Arc::new(SessionTable::new(clock.clone()));
    let task = r
        .start(
            store.clone(),
            clock,
            sessions,
            || Duration::from_secs(5),
            stop.clone(),
        )
        .await;
    r.set_state(GatewayState::Draining);
    let mut seen = None;
    for _ in 0..50 {
        seen = get(store.as_ref(), "gw-a").await.unwrap().map(|g| g.state);
        if seen == Some(GatewayState::Draining) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen, Some(GatewayState::Draining));
    stop.cancel();
    task.await.unwrap();
}

/// Regression: a gateway writing to many shards made a load report over
/// the 64 KiB record limit, and every registration write failed with it.
#[tokio::test]
async fn a_large_load_report_never_fails_the_registration() {
    use sprig_balancer::load::{LoadReport, report_key};
    use sprig_balancer::model::ShardKey;
    use sprig_balancer::shards::HashRange;
    use sprig_common::ids::ClusterId;

    // 64 shards in each of the last completed minutes, hashes spread
    // over the whole range: about twice the record limit unbounded.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let now = u64::try_from(now).unwrap();
    let c = ClusterId::new("dc-t-c1").unwrap();
    let range = HashRange::new(0, None);
    for back in 1..=3u64 {
        for s in 0..64u32 {
            let key = ShardKey::new(4242, 1, s);
            let writes: Vec<_> = (0..40u64)
                .map(|i| {
                    let h = (i + 1)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add(u64::from(s));
                    (key, &c, Some((range, h)))
                })
                .collect();
            crate::write_load::accumulator()
                .sketches()
                .record(now - back * 60_000, writes);
        }
    }
    let unbounded = crate::write_load::accumulator().report("gw-big", now);
    assert!(
        Record::new(unbounded).encode().is_err(),
        "the workload must exceed the record limit"
    );

    let store = store();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let r = Registrar::new(record("gw-big"));
    let sessions = Arc::new(SessionTable::new(clock.clone()));
    let task = r
        .start(
            store.clone(),
            clock.clone(),
            sessions,
            || Duration::from_secs(5),
            stop.clone(),
        )
        .await;
    // Registered, and the report written beside the record, within the limit.
    assert!(get(store.as_ref(), "gw-big").await.unwrap().is_some());
    let key = store.tier().key(&report_key("gw-big")).unwrap();
    let kv = store
        .get(&key, Consistency::Linearizable)
        .await
        .unwrap()
        .expect("load report written");
    assert!(kv.value.len() <= sprig_meta::record::MAX_VALUE_BYTES);
    let report = Record::<LoadReport>::decode(&kv.value).unwrap().body;
    assert!(report.sketches.len() >= 64, "{}", report.sketches.len());
    // Refreshes keep succeeding: a state change reaches the record.
    r.set_state(GatewayState::Ready);
    let mut ok = false;
    for _ in 0..50 {
        if get(store.as_ref(), "gw-big")
            .await
            .unwrap()
            .map(|g| g.state)
            == Some(GatewayState::Ready)
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(ok, "the record was refreshed");
    stop.cancel();
    task.await.unwrap();
}
