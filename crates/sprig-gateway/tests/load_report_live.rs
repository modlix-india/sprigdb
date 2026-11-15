//! The gateway's registration against a real etcd while its write-load
//! report is far over the 64 KiB record limit (a regression: the
//! report reached 83,638 bytes and every registration write failed, every
//! 5 s, for hours).
//!
//! The gateway's accumulator gets the writes of 120 shards a minute for
//! the last three minutes, each with keyed rows spread over the hash range
//! (full buckets and top-16), then a registrar runs for a while. The test
//! checks:
//!
//! - every periodic refresh of `gateways/<id>` lands (its revision moves
//!   at least once per `LOAD_PERIOD`, 5 s), and a state change shows at once;
//! - `gateway-load/<id>` is written, within the limit, and read back as the
//!   DC leader reads it (`SketchCollector`), every shard keeps its write
//!   share, and the hottest shard keeps its histogram.
//!
//! ```sh
//! export SPRIG_TEST_ETCD=http://127.0.0.1:41379   # a private etcd, never the shared one
//! cargo test -p sprig-gateway --test load_report_live -- --nocapture
//! ```
//!
//! Without `SPRIG_TEST_ETCD` the test reports what it needs and passes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    missing_docs
)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sprig_balancer::load::{LoadReport, report_key};
use sprig_balancer::model::ShardKey;
use sprig_balancer::shards::{HashRange, SketchCollector};
use sprig_common::CancellationToken;
use sprig_common::ids::{ClusterId, DcId};
use sprig_common::time::{Clock, SystemClock};
use sprig_gateway::registry::{self, GatewayRecord, GatewayState, LOAD_PERIOD, Load, Registrar};
use sprig_gateway::session::SessionTable;
use sprig_meta::{Consistency, EtcdConfig, EtcdStore, MetaStore, Record, Tier};

const SHARDS: u32 = 120;
const MINUTE: u64 = 60_000;

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

#[tokio::test]
async fn registration_keeps_refreshing_with_a_huge_load_report() {
    let Ok(etcd) = std::env::var("SPRIG_TEST_ETCD") else {
        eprintln!("load_report_live: set SPRIG_TEST_ETCD to a private etcd to run");
        return;
    };
    let dc = DcId::new("r1fix-dc").unwrap();
    let store: Arc<dyn MetaStore> = Arc::new(
        EtcdStore::connect(Tier::Dc(dc), &EtcdConfig::new(vec![etcd]))
            .await
            .unwrap(),
    );
    let id = format!("r1fix-gw-{}", std::process::id());

    // The workload: SHARDS shards a minute over the last three minutes.
    let now = now_ms();
    let c = ClusterId::new("r1fix-c1").unwrap();
    let range = HashRange::new(0, None);
    for back in 1..=3u64 {
        for s in 0..SHARDS {
            let key = ShardKey::new(4_130_600_001, 1, s);
            // Shard 0 is the hottest.
            let n = 30 + u64::from(SHARDS - s);
            let writes: Vec<_> = (0..n)
                .map(|i| {
                    let h = (i + 1)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add(u64::from(s));
                    (key, &c, Some((range, h)))
                })
                .collect();
            sprig_gateway::write_load::accumulator()
                .sketches()
                .record(now - back * MINUTE, writes);
        }
    }
    let unbounded = sprig_gateway::write_load::accumulator().report(&id, now);
    let unbounded_len = serde_json::to_vec(&unbounded).unwrap().len();
    eprintln!(
        "load_report_live: unbounded report {} sketches, {unbounded_len} bytes",
        unbounded.sketches.len()
    );
    assert!(unbounded_len > sprig_meta::record::MAX_VALUE_BYTES);

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let stop = CancellationToken::new();
    let r = Registrar::new(GatewayRecord {
        gateway_id: id.clone(),
        slot: 0,
        version: "r1fix-live".into(),
        addr: "127.0.0.1".into(),
        listeners: std::collections::BTreeMap::new(),
        state: GatewayState::Starting,
        catalog_formats: vec![],
        authorizers: vec![],
        load: Load::default(),
        kafka_host: None,
    });
    let task = r
        .start(
            Arc::clone(&store),
            clock.clone(),
            Arc::new(SessionTable::new(clock)),
            || Duration::from_secs(10),
            stop.clone(),
        )
        .await;
    r.set_state(GatewayState::Ready);

    let gw_key = store.tier().key(&format!("gateways/{id}")).unwrap();
    let load_key = store.tier().key(&report_key(&id)).unwrap();
    // Watch the record for six refresh periods.
    let started = Instant::now();
    let mut revs = Vec::new();
    let mut last = 0;
    while started.elapsed() < LOAD_PERIOD * 6 + Duration::from_secs(1) {
        let kv = store
            .get(&gw_key, Consistency::Linearizable)
            .await
            .unwrap()
            .expect("the gateway record");
        if kv.mod_revision != last {
            revs.push((started.elapsed().as_millis(), kv.mod_revision));
            last = kv.mod_revision;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("load_report_live: record revisions (ms, rev): {revs:?}");
    // The initial write, the state change, then one per period.
    assert!(revs.len() >= 6, "refreshes: {revs:?}");
    for w in revs.windows(2).skip(1) {
        let gap = w[1].0 - w[0].0;
        assert!(
            gap <= LOAD_PERIOD.as_millis() + 1_500,
            "a refresh was missed: {revs:?}"
        );
    }
    let rec = registry::get(store.as_ref(), &id).await.unwrap().unwrap();
    assert_eq!(rec.state, GatewayState::Ready);

    // The report, read as the DC leader reads it.
    let kv = store
        .get(&load_key, Consistency::Linearizable)
        .await
        .unwrap()
        .expect("the load report");
    eprintln!("load_report_live: stored report {} bytes", kv.value.len());
    assert!(kv.value.len() <= sprig_meta::record::MAX_VALUE_BYTES);
    let got = Record::<LoadReport>::decode(&kv.value).unwrap().body;
    let detailed = got
        .sketches
        .iter()
        .filter(|s| !s.buckets.is_empty())
        .count();
    eprintln!(
        "load_report_live: {} sketches, {detailed} with buckets and top",
        got.sketches.len()
    );
    let (mut whole, mut read) = (SketchCollector::new(), SketchCollector::new());
    whole.ingest(&sprig_gateway::write_load::accumulator().report(&id, got.at_ms));
    read.ingest(&got);
    let at = now_ms() + MINUTE;
    for s in 0..64 {
        // The report holds the 64 most written shards of each minute.
        let k = ShardKey::new(4_130_600_001, 1, s);
        assert_eq!(
            whole.write_shares(&k, None, at),
            read.write_shares(&k, None, at),
            "shard {s}"
        );
    }
    let k = ShardKey::new(4_130_600_001, 1, 0);
    assert!(
        read.write_histogram(&k, range, Duration::from_secs(180), at)
            .is_some(),
        "the hottest shard keeps its histogram"
    );

    stop.cancel();
    task.await.unwrap();
    // Stopping gives the record and the report back (the lease ends).
    assert!(
        store
            .get(&gw_key, Consistency::Linearizable)
            .await
            .unwrap()
            .is_none()
    );
}
