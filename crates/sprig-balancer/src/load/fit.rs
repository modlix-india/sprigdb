//! Fitting a [`LoadReport`] into one etcd record.
//!
//! A report carries up to [`REPORT_MINUTES`](crate::shards::sketch::REPORT_MINUTES)
//! minutes of up to 64 sketches each, about 1 KB a sketch with its buckets
//! and top-16, so a gateway writing to more than about 20 shards a minute
//! passed the 64 KiB record limit, and its registration write failed. The
//! report now gives up detail, least needed first, until it fits:
//!
//! ```text
//! priority of a sketch: newest minute first, then most writes
//! 1. slim the lowest-priority sketches: keep shard, cluster, minute and
//!    writes, drop range, buckets and top (shares stay exact; only the
//!    histogram of a little-written shard is lost)
//! 2. drop the lowest-priority slim sketches (oldest minutes first)
//! 3. drop the per-shard counters (`shards`) with the least busy time
//! cluster_writes (the share denominators) are never trimmed
//! ```
//!
//! Each step keeps the most it can (a binary search over how many entries
//! keep their detail). The older minutes are there only for a controller
//! that missed a refresh, so they go first.

use sprig_common::{Result, SprigError};
use sprig_meta::Record;

use super::LoadReport;
use crate::shards::sketch::{SpaceSaving, WriteSketch};

/// The most a report's record may take: the record limit less room for
/// fields a newer build adds.
pub const REPORT_MAX_BYTES: usize = sprig_meta::record::MAX_VALUE_BYTES - 2048;

/// What [`LoadReport::encode_within`] gave up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Trimmed {
    /// Sketches sent without range, buckets and top.
    pub slimmed: usize,
    /// Sketches left out.
    pub dropped: usize,
    /// Per-shard counters left out.
    pub shards_dropped: usize,
}

impl Trimmed {
    /// Whether anything was given up.
    #[must_use]
    pub fn any(&self) -> bool {
        self.slimmed + self.dropped + self.shards_dropped > 0
    }
}

fn slim(s: &WriteSketch) -> WriteSketch {
    WriteSketch {
        range: None,
        buckets: Vec::new(),
        top: SpaceSaving::default(),
        ..s.clone()
    }
}

fn encoded(r: &LoadReport, max: usize) -> Option<Vec<u8>> {
    Record::new(r.clone())
        .encode()
        .ok()
        .filter(|b| b.len() <= max)
}

/// The largest `k` in `0..=n` for which `fits(k)` holds, given that it
/// holds for every smaller `k` once it holds for one; `None` when it
/// doesn't hold even for 0.
fn largest(n: usize, mut fits: impl FnMut(usize) -> Option<Vec<u8>>) -> Option<(usize, Vec<u8>)> {
    let mut best = (0, fits(0)?);
    let (mut lo, mut hi) = (1, n);
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        if let Some(b) = fits(mid) {
            best = (mid, b);
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    Some(best)
}

impl LoadReport {
    /// The report as an etcd record of at most `max` bytes, with what had
    /// to be given up to fit (module docs).
    ///
    /// # Errors
    /// `Internal` when even the cluster totals alone don't fit, or the
    /// record can't be encoded.
    pub fn encode_within(&self, max: usize) -> Result<(Vec<u8>, Trimmed)> {
        if let Some(b) = encoded(self, max) {
            return Ok((b, Trimmed::default()));
        }
        // Sketch indexes by priority: newest minute, then most writes.
        let mut order: Vec<usize> = (0..self.sketches.len()).collect();
        order.sort_by(|&a, &b| {
            let (x, y) = (&self.sketches[a], &self.sketches[b]);
            y.minute
                .cmp(&x.minute)
                .then(y.writes.cmp(&x.writes))
                .then(x.shard.cmp(&y.shard))
        });
        let n = order.len();
        // `keep` sketches in priority order, the first `full` with detail.
        let with = |full: usize, keep: usize, shards: &[super::ShardLoad]| {
            let mut rank = vec![usize::MAX; n];
            for (r, &i) in order.iter().enumerate() {
                rank[i] = r;
            }
            let sketches = self
                .sketches
                .iter()
                .enumerate()
                .filter(|(i, _)| rank[*i] < keep)
                .map(|(i, s)| if rank[i] < full { s.clone() } else { slim(s) })
                .collect();
            LoadReport {
                gateway: self.gateway.clone(),
                at_ms: self.at_ms,
                shards: shards.to_vec(),
                sketches,
                cluster_writes: self.cluster_writes.clone(),
            }
        };
        // 1. Slim from the end.
        if let Some((full, b)) = largest(n, |k| encoded(&with(k, n, &self.shards), max)) {
            return Ok((
                b,
                Trimmed {
                    slimmed: n - full,
                    ..Trimmed::default()
                },
            ));
        }
        // 2. All slim; drop from the end.
        if let Some((keep, b)) = largest(n, |k| encoded(&with(0, k, &self.shards), max)) {
            return Ok((
                b,
                Trimmed {
                    slimmed: keep,
                    dropped: n - keep,
                    shards_dropped: 0,
                },
            ));
        }
        // 3. No sketches; keep the busiest shards' counters.
        let mut shards = self.shards.clone();
        shards.sort_by(|a, b| b.busy_us.cmp(&a.busy_us).then(a.shard.cmp(&b.shard)));
        let m = shards.len();
        if let Some((keep, b)) = largest(m, |k| encoded(&with(0, 0, &shards[..k]), max)) {
            return Ok((
                b,
                Trimmed {
                    slimmed: 0,
                    dropped: n,
                    shards_dropped: m - keep,
                },
            ));
        }
        Err(SprigError::internal(format!(
            "load report of gateway {} doesn't fit in {max} bytes even without sketches",
            self.gateway
        )))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::many_single_char_names)]

    use sprig_common::ids::ClusterId;

    use super::*;
    use crate::load::{LoadAccumulator, ShardLoad};
    use crate::model::ShardKey;
    use crate::shards::HashRange;
    use crate::shards::sketch::SketchCollector;

    const MINUTE: u64 = 60_000;

    /// A gateway that wrote to `shards` shards a minute for 3 minutes, each
    /// with spread-out hashes (full buckets and top-16).
    fn busy(shards: u32) -> (LoadAccumulator, u64) {
        let acc = LoadAccumulator::new();
        let c = ClusterId::new("mum1-c1").unwrap();
        let t0 = 29_000_000 * MINUTE;
        let range = HashRange::new(0, None);
        for m in 0..3u64 {
            for s in 0..shards {
                let key = ShardKey::new(7, 1, s);
                // More writes to lower shard numbers.
                let n = 40 + u64::from(shards - s);
                let writes: Vec<_> = (0..n)
                    .map(|i| {
                        let h = (i + 1)
                            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                            .wrapping_add(u64::from(s));
                        (key, &c, Some((range, h)))
                    })
                    .collect();
                acc.sketches().record(t0 + m * MINUTE + 1_000, writes);
            }
        }
        for s in 0..shards {
            acc.record(
                ShardKey::new(7, 1, s),
                &c,
                std::time::Duration::from_millis(u64::from(s) + 1),
                1,
                0,
            );
        }
        (acc, t0 + 3 * MINUTE + 1_000)
    }

    #[test]
    fn a_small_report_is_sent_whole() {
        let (acc, now) = busy(5);
        let r = acc.report("gw-1", now);
        let (b, t) = r.encode_within(REPORT_MAX_BYTES).unwrap();
        assert!(!t.any());
        assert_eq!(Record::<LoadReport>::decode(&b).unwrap().body, r);
    }

    #[test]
    fn a_report_of_many_shards_fits_and_keeps_every_share() {
        // 64 shards a minute, well over 64 KiB unbounded (a live gateway
        // once sent a report of 83,638 bytes).
        let (acc, now) = busy(64);
        let r = acc.report("gw-1", now);
        assert_eq!(r.sketches.len(), 192);
        assert!(Record::new(r.clone()).encode().is_err(), "too big before");
        let (b, t) = r.encode_within(REPORT_MAX_BYTES).unwrap();
        assert!(b.len() <= REPORT_MAX_BYTES, "{}", b.len());
        assert!(t.slimmed > 0 && t.dropped == 0, "{t:?}");
        let got = Record::<LoadReport>::decode(&b).unwrap().body;
        // Every sketch is still there, with its writes; the cluster
        // totals are whole.
        assert_eq!(got.sketches.len(), r.sketches.len());
        assert_eq!(got.cluster_writes, r.cluster_writes);
        assert_eq!(got.shards, r.shards);
        let writes = |x: &LoadReport| x.sketches.iter().map(|s| s.writes).sum::<u64>();
        assert_eq!(writes(&got), writes(&r));
        // The newest minute's most written shards keep their detail.
        let newest = r.sketches.iter().map(|s| s.minute).max().unwrap();
        let top = got
            .sketches
            .iter()
            .find(|s| s.minute == newest && s.shard == ShardKey::new(7, 1, 0))
            .unwrap();
        assert_eq!(top.buckets.len(), 64);
        assert!(!top.top.is_empty());
        // Detail goes from the oldest minute first.
        let oldest = r.sketches.iter().map(|s| s.minute).min().unwrap();
        assert!(
            got.sketches
                .iter()
                .filter(|s| s.minute == oldest)
                .all(|s| s.buckets.is_empty())
        );
        // The controller reads shares exactly as from the whole report.
        let (mut a, mut z) = (SketchCollector::new(), SketchCollector::new());
        a.ingest(&r);
        z.ingest(&got);
        let at = now + MINUTE;
        for s in 0..64 {
            let k = ShardKey::new(7, 1, s);
            assert_eq!(a.write_shares(&k, None, at), z.write_shares(&k, None, at));
        }
        // And the hottest shard's histogram of the newest minute.
        let range = HashRange::new(0, None);
        let w = std::time::Duration::from_secs(60);
        let k = ShardKey::new(7, 1, 0);
        assert_eq!(
            a.write_histogram(&k, range, w, at),
            z.write_histogram(&k, range, w, at)
        );
    }

    #[test]
    fn a_tiny_limit_drops_sketches_then_counters() {
        let (acc, now) = busy(64);
        let r = acc.report("gw-1", now);
        // Room for the counters and some slim sketches only.
        let (b, t) = r.encode_within(24 * 1024).unwrap();
        assert!(b.len() <= 24 * 1024);
        assert!(t.dropped > 0 && t.slimmed > 0, "{t:?}");
        let got = Record::<LoadReport>::decode(&b).unwrap().body;
        assert!(got.sketches.iter().all(|s| s.buckets.is_empty()));
        // The newest minute is kept before the older ones.
        let newest = r.sketches.iter().map(|s| s.minute).max().unwrap();
        assert!(got.sketches.iter().any(|s| s.minute == newest));
        assert_eq!(got.cluster_writes, r.cluster_writes);
        // Smaller still: no sketches, the busiest counters.
        let (b, t) = r.encode_within(1500).unwrap();
        assert!(b.len() <= 1500);
        assert_eq!(t.dropped, 192);
        assert!(t.shards_dropped > 0);
        let got = Record::<LoadReport>::decode(&b).unwrap().body;
        // The busiest counters are the ones kept.
        let kept_min = got.shards.iter().map(|s| s.busy_us).min().unwrap();
        let dropped_max = r
            .shards
            .iter()
            .filter(|s| !got.shards.contains(s))
            .map(|s| s.busy_us)
            .max()
            .unwrap();
        assert!(dropped_max < kept_min);
        // Nothing fits: an error, never a record over the limit.
        assert!(r.encode_within(64).is_err());
    }

    #[test]
    fn many_counters_alone_are_trimmed_too() {
        let acc = LoadAccumulator::new();
        let c = ClusterId::new("mum1-c1").unwrap();
        for s in 0..2_000u32 {
            acc.record(
                ShardKey::new(9, 1, s),
                &c,
                std::time::Duration::from_micros(u64::from(s)),
                0,
                0,
            );
        }
        let r = acc.report("gw-1", 1);
        let (b, t) = r.encode_within(REPORT_MAX_BYTES).unwrap();
        assert!(b.len() <= REPORT_MAX_BYTES);
        assert!(t.shards_dropped > 0);
        let _: ShardLoad = Record::<LoadReport>::decode(&b).unwrap().body.shards[0].clone();
    }
}
