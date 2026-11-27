//! Clock monitoring: this node's wall-clock offset from the other nodes.
//!
//! ```text
//! every 5 s, for each other node with a ready gateway:
//!   t0 := now; GET http://<its http>/admin/v1/engine/clock → its time t1;
//!   t3 := now; offset := t1 - (t0 + t3) / 2, uncertainty (t3 - t0) / 2
//!   per peer, the median of its last 5 offsets
//! this node's offset := the median of the peers' offsets (when this
//!   node's clock is off, every peer sees it off the same way)
//! sprig_node_clock_offset_seconds := |that|
//! above 50% of replication.max_offset_ms: warn (ClockOffsetHigh alert)
//! above 80% against a majority of the peers: lead nothing (hand every
//!   unit away, never ask for leadership) until back under 50%
//! ```
//!
//! Reads at a timestamp across shards will exit the node instead; until then the
//! node's own commits only need its clock to move forward (the HLC), so
//! stepping down is enough to keep leases honest.
//!
//! `SPRIGDB_ENGINE_CLOCK_SKEW_FILE` (fault tests only) names a file holding a
//! number of milliseconds added to this node's clock as the watch reads it
//! (what it reports to others and what it compares their answers with),
//! read at every use: the watch then behaves as with a real clock error
//! (the engine's own time and the HLC are not changed).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use super::nodes;

/// `replication.max_offset_ms`.
pub static MAX_OFFSET_MS: Setting<u64> = Setting {
    key: "replication.max_offset_ms",
    default: 250,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(10, 10_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 61,
    doc: "Largest clock offset between nodes; above half a node warns, above 80% it leads nothing",
};
sprig_settings::register!(MAX_OFFSET_MS);

/// The node-local route that tells this node's clock.
pub const CLOCK_PATH: &str = "/admin/v1/engine/clock";
/// Fault tests only: a file with milliseconds added to the reported time.
pub const SKEW_FILE_VAR: &str = "SPRIGDB_ENGINE_CLOCK_SKEW_FILE";

fn unix_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

fn skew_us() -> i64 {
    let Ok(path) = std::env::var(SKEW_FILE_VAR) else {
        return 0;
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map_or(0, |ms| {
            #[allow(clippy::cast_possible_truncation)]
            let us = (ms * 1000.0) as i64;
            us
        })
}

/// The route's answer: this node's Raft id and its time.
#[must_use]
pub fn report() -> serde_json::Value {
    serde_json::json!({"node": nodes::me(), "unix_us": local_us()})
}

/// This node's clock as it reads it (with the fault-test skew).
fn local_us() -> i64 {
    unix_us() + skew_us()
}

/// Offsets seen per peer, newest last.
static SAMPLES: Mutex<BTreeMap<u64, VecDeque<i64>>> = Mutex::new(BTreeMap::new());

fn median(v: &mut [i64]) -> Option<i64> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    Some(v[v.len() / 2])
}

/// What one round concludes from the peers' offsets (µs, each peer's time
/// minus this node's): this node's offset, and whether a majority of the
/// peers find it beyond `bad_us`.
#[must_use]
pub fn judge(peer_offsets: &[i64], bad_us: i64) -> (i64, bool) {
    let mut v = peer_offsets.to_vec();
    let Some(m) = median(&mut v) else {
        return (0, false);
    };
    // A peer `p` ahead of us by `o` means we are behind by `o`: our offset
    // is `-o`.
    let mine = -m;
    let beyond = peer_offsets
        .iter()
        .filter(|o| o.signum() == m.signum() && o.abs() > bad_us)
        .count();
    (mine, beyond * 2 > peer_offsets.len())
}

/// Other nodes' HTTP endpoints from their ready registrations.
async fn peers(dc: &dyn MetaStore) -> sprig_common::Result<BTreeMap<u64, String>> {
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Gw {
        state: String,
        #[serde(default)]
        addr: String,
        #[serde(default)]
        listeners: BTreeMap<String, u16>,
        #[serde(default)]
        raft_node: Option<u64>,
    }
    impl Versioned for Gw {
        const V: u32 = 1;
    }
    let me = nodes::me();
    let prefix = dc.tier().key("gateways/")?;
    let page = dc
        .list(
            &prefix,
            ListOpts {
                limit: 10_000,
                ..ListOpts::default()
            },
        )
        .await?;
    Ok(page
        .kvs
        .iter()
        .filter_map(|kv| Record::<Gw>::decode(&kv.value).ok())
        .filter(|g| g.body.state == "ready")
        .filter_map(|g| {
            let id = g.body.raft_node?;
            let port = g.body.listeners.get("http")?;
            (id != me).then(|| (id, format!("http://{}:{port}{CLOCK_PATH}", g.body.addr)))
        })
        .collect())
}

type HttpClient = Client<HttpConnector, Empty<Bytes>>;

async fn probe(client: &HttpClient, url: &str) -> Option<i64> {
    let uri: hyper::Uri = url.parse().ok()?;
    let t0 = local_us();
    let res = tokio::time::timeout(Duration::from_secs(2), client.get(uri))
        .await
        .ok()?
        .ok()?;
    let body = tokio::time::timeout(Duration::from_secs(2), res.into_body().collect())
        .await
        .ok()?
        .ok()?
        .to_bytes();
    let t3 = local_us();
    let v: serde_json::Value = serde_json::from_slice(&body).ok()?;
    let t1 = v.get("unix_us")?.as_i64()?;
    // A round trip slower than 200 ms says little about the offset.
    if t3 - t0 > 200_000 {
        return None;
    }
    Some(t1 - i64::midpoint(t0, t3))
}

pub(super) fn start(dc: std::sync::Arc<dyn MetaStore>, stop: sprig_common::CancellationToken) {
    tokio::spawn(async move {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(Duration::from_secs(1)));
        let client: HttpClient = Client::builder(TokioExecutor::new()).build(http);
        let mut endpoints: BTreeMap<u64, String> = BTreeMap::new();
        let mut listed: Option<Instant> = None;
        let mut warned: Option<Instant> = None;
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            if listed.is_none_or(|t| t.elapsed() >= Duration::from_secs(30)) {
                if let Ok(p) = peers(dc.as_ref()).await {
                    endpoints = p;
                }
                listed = Some(Instant::now());
            }
            let mut offsets = Vec::new();
            for (id, url) in &endpoints {
                let Some(o) = probe(&client, url).await else {
                    continue;
                };
                let mut s = SAMPLES.lock().unwrap_or_else(PoisonError::into_inner);
                let q = s.entry(*id).or_default();
                q.push_back(o);
                while q.len() > 5 {
                    q.pop_front();
                }
                let mut v: Vec<i64> = q.iter().copied().collect();
                if let Some(m) = median(&mut v) {
                    offsets.push(m);
                }
            }
            if offsets.is_empty() {
                continue;
            }
            let max_us = i64::try_from(super::setting(&MAX_OFFSET_MS)).unwrap_or(250) * 1000;
            let (mine, beyond) = judge(&offsets, max_us * 8 / 10);
            #[allow(clippy::cast_precision_loss)]
            let secs = mine.abs() as f64 / 1e6;
            metrics::gauge!("sprig_node_clock_offset_seconds").set(secs);
            let high = mine.abs() > max_us / 2;
            if high && warned.is_none_or(|t| t.elapsed() >= Duration::from_secs(60)) {
                warned = Some(Instant::now());
                tracing::warn!(
                    offset_ms = mine / 1000,
                    max_offset_ms = max_us / 1000,
                    "this node's clock is off from the other nodes' by more than half the allowed offset; check its time sync"
                );
            }
            // Out of bounds against a majority: lead nothing. Back in
            // under half the bound: lead again.
            let bad = if nodes::clock_bad() { high } else { beyond };
            if nodes::set_clock_bad(bad) {
                if bad {
                    tracing::error!(
                        offset_ms = mine / 1000,
                        max_offset_ms = max_us / 1000,
                        "this node's clock is beyond 80% of the allowed offset: it leads no units until its clock is back"
                    );
                } else {
                    tracing::warn!(
                        offset_ms = mine / 1000,
                        "this node's clock is back within bounds: it leads units again"
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_majority_beyond_the_bound_judges_this_node_off() {
        // Every peer is 300 ms ahead: this node is 300 ms behind.
        let (mine, beyond) = judge(&[300_000, 301_000, 299_000, 300_500], 200_000);
        assert_eq!(mine, -300_500);
        assert!(beyond);
        // One peer off (its own clock): not this node's problem.
        let (mine, beyond) = judge(&[300_000, 1_000, -2_000, 500], 200_000);
        assert!(mine.abs() < 10_000);
        assert!(!beyond);
        assert_eq!(judge(&[], 200_000), (0, false));
    }
}
