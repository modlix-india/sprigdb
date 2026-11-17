//! A feed consumer's drain at a graceful stop.
//!
//! A consumer that keeps its checkpoint outside the binlog it reads (a
//! subscription checkpoint in etcd, a job's progress) acknowledges at most
//! about once a second. If it stopped where it was, the writes of the last
//! second before the stop, its own and other consumers' (a sequencer's
//! last append, a trigger's last procedure), would lie after its saved
//! position, and a binlog purge right after the stop would remove history
//! its restart needs: the node refuses the dump (39800), though the
//! consumer missed nothing. So at a graceful stop it drains:
//!
//! ```text
//! stop:
//!   target := the cluster's binlog position now
//!   go on: finish the work in flight, read on and handle what comes
//!     (nothing is skipped: the checkpoint still moves only past what
//!     was handled)
//!   the position past every handled item has passed target, read at
//!     least QUIET ago → read the cluster's position again: nothing
//!     written since → done; else it is the new target (others wrote
//!     while stopping: every job of the worker stops at once, and their
//!     last writes come over the first moments of the stop)
//!   done, or STOP_DRAIN is up → acknowledge exactly that position, end
//! ```
//!
//! A position passes a target when it is at or after it in every GTID
//! domain both name. A domain only the target names (the local domain of
//! another Galera node, read through the writer pool) isn't in this feed.
//! Without a position to wait for (it can't be read), the drain only
//! finishes the work in flight. After `kill -9` there is no drain: a purge
//! right after a kill can still remove history a consumer needs, and that
//! consumer alone fails (39800).

use std::time::Duration;

use sprig_binlog::GtidPos;
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use tokio::time::Instant;

use crate::source::Clusters;

/// How long a stopping consumer reads on, at most: an idle position
/// reaches each subscription of the shared reader at least every second,
/// so this leaves room for two rounds (others' writes while stopping).
pub const STOP_DRAIN: Duration = Duration::from_secs(3);

/// How long the cluster's position must stand still for a drain to end:
/// the other consumers' last writes of a stop come within it.
pub const QUIET: Duration = Duration::from_millis(500);

/// Whether `pos` has passed `target` in every domain both name.
#[must_use]
pub fn passed(pos: &GtidPos, target: &GtidPos) -> bool {
    target
        .iter()
        .all(|g| pos.get(g.domain).is_none_or(|x| x.seq >= g.seq))
}

/// Where a stopping consumer reads the cluster's binlog position.
#[async_trait::async_trait]
pub trait ClusterPosition: Send + Sync {
    /// `@@gtid_binlog_pos` of `cluster` now.
    ///
    /// # Errors
    /// Connection errors.
    async fn cluster_position(&self, cluster: &ClusterId) -> Result<GtidPos>;
}

#[async_trait::async_trait]
impl<T: Clusters + ?Sized> ClusterPosition for T {
    async fn cluster_position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        self.drain_position(cluster).await
    }
}

/// No position to read: the drain only finishes the work in flight.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoPosition;

#[async_trait::async_trait]
impl ClusterPosition for NoPosition {
    async fn cluster_position(&self, cluster: &ClusterId) -> Result<GtidPos> {
        Err(sprig_common::SprigError::internal(format!(
            "no position source for {cluster}"
        )))
    }
}

/// One consumer's drain (see the module docs).
#[derive(Debug)]
pub struct StopDrain {
    until: Instant,
    /// The cluster and position to pass; `None`: nothing to wait for.
    target: Option<(ClusterId, GtidPos)>,
    /// When the target was read.
    read_at: Instant,
    /// Positions read so far (for tests and logs).
    reads: u32,
    reached: bool,
}

impl StopDrain {
    /// Starts a drain of at most `budget` on `cluster` (the cluster of the
    /// consumer's newest position; `None` when it has none yet).
    pub async fn begin<P: ClusterPosition + ?Sized>(
        src: &P,
        cluster: Option<&ClusterId>,
        budget: Duration,
    ) -> Self {
        let mut d = Self {
            until: Instant::now() + budget,
            target: None,
            read_at: Instant::now(),
            reads: 0,
            reached: false,
        };
        if let Some(c) = cluster {
            d.read(src, c).await;
        }
        d
    }

    async fn read<P: ClusterPosition + ?Sized>(&mut self, src: &P, cluster: &ClusterId) {
        self.reads += 1;
        self.read_at = Instant::now();
        match src.cluster_position(cluster).await {
            Ok(p) => self.target = Some((cluster.clone(), p)),
            Err(e) => {
                tracing::debug!(%cluster, error = %e, "stopping consumer can't read the cluster's position; it only finishes its work");
                self.target = None;
            }
        }
    }

    /// When the drain ends at the latest.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.until
    }

    /// Whether the time is up.
    #[must_use]
    pub fn expired(&self) -> bool {
        Instant::now() >= self.until
    }

    /// Whether the last check found the position passed and nothing
    /// written since.
    #[must_use]
    pub fn reached(&self) -> bool {
        self.reached
    }

    /// The position read last, and its cluster.
    #[must_use]
    pub fn target(&self) -> Option<&(ClusterId, GtidPos)> {
        self.target.as_ref()
    }

    /// Positions of the cluster read so far.
    #[must_use]
    pub fn reads(&self) -> u32 {
        self.reads
    }

    /// Whether the consumer, with everything before `at` (on `cluster`)
    /// handled, may stop: `at` has passed the target and the cluster wrote
    /// nothing since (its position read again, at least [`QUIET`] after
    /// the target). A consumer that moved to
    /// another cluster waits for that cluster's position. Without a target
    /// it may stop at once.
    pub async fn done<P: ClusterPosition + ?Sized>(
        &mut self,
        src: &P,
        cluster: &ClusterId,
        at: &GtidPos,
    ) -> bool {
        let Some((c, target)) = &self.target else {
            self.reached = true;
            return true;
        };
        if c != cluster {
            self.read(src, cluster).await;
            return false;
        }
        if !passed(at, target) || self.read_at.elapsed() < QUIET {
            return false;
        }
        let before = target.clone();
        self.read(src, cluster).await;
        match &self.target {
            // Nothing new (or the position can't be read again): done.
            None => {
                self.target = Some((cluster.clone(), before));
                self.reached = true;
                true
            }
            Some((_, now)) if passed(at, now) => {
                self.reached = true;
                true
            }
            Some((_, now)) => {
                tracing::debug!(%cluster, from = %before, to = %now, at = %at, "stopping consumer: the cluster wrote more; reading on");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// A cluster whose position the test moves.
    struct Moving(Mutex<Vec<GtidPos>>);

    #[async_trait::async_trait]
    impl ClusterPosition for Moving {
        async fn cluster_position(&self, _: &ClusterId) -> Result<GtidPos> {
            let mut v = self.0.lock().unwrap();
            Ok(if v.len() > 1 {
                v.remove(0)
            } else {
                v[0].clone()
            })
        }
    }

    fn p(s: &str) -> GtidPos {
        s.parse().unwrap()
    }

    fn c() -> ClusterId {
        ClusterId::new("c1").unwrap()
    }

    #[test]
    fn a_domain_only_the_target_names_is_not_waited_for() {
        assert!(passed(&p("1-1-10"), &p("1-1-10,4902-4902-7")));
        assert!(!passed(&p("1-1-9"), &p("1-1-10")));
        assert!(passed(&p("1-1-11,4901-4901-3"), &p("1-1-10")));
        assert!(!passed(&p("1-1-11,4901-4901-3"), &p("1-1-10,4901-4901-4")));
    }

    #[tokio::test(start_paused = true)]
    async fn the_drain_waits_for_writes_made_while_stopping() {
        // At the stop the cluster is at 10; once the consumer passes it,
        // others have written up to 12; then nothing more.
        let src = Moving(Mutex::new(vec![p("1-1-10"), p("1-1-12")]));
        let mut d = StopDrain::begin(&src, Some(&c()), STOP_DRAIN).await;
        assert!(!d.done(&src, &c(), &p("1-1-9")).await);
        assert!(
            !d.done(&src, &c(), &p("1-1-10")).await,
            "the position stood still for less than QUIET"
        );
        tokio::time::advance(QUIET).await;
        assert!(
            !d.done(&src, &c(), &p("1-1-10")).await,
            "12 was written since"
        );
        assert_eq!(d.target().unwrap().1, p("1-1-12"));
        assert!(!d.done(&src, &c(), &p("1-1-11")).await);
        tokio::time::advance(QUIET).await;
        assert!(d.done(&src, &c(), &p("1-1-12")).await);
        assert!(d.reached());
        assert_eq!(d.reads(), 3);
    }

    #[tokio::test]
    async fn without_a_position_the_drain_ends_at_once() {
        let mut d = StopDrain::begin(&NoPosition, Some(&c()), STOP_DRAIN).await;
        assert!(d.done(&NoPosition, &c(), &p("1-1-1")).await);
        let mut d = StopDrain::begin(&NoPosition, None, STOP_DRAIN).await;
        assert!(d.done(&NoPosition, &c(), &p("1-1-1")).await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_consumer_on_another_cluster_waits_for_that_one() {
        let src = Moving(Mutex::new(vec![p("1-1-10"), p("2-2-5")]));
        let mut d = StopDrain::begin(&src, Some(&c()), STOP_DRAIN).await;
        let c2 = ClusterId::new("c2").unwrap();
        assert!(!d.done(&src, &c2, &p("2-2-4")).await);
        assert_eq!(d.target().unwrap(), &(c2.clone(), p("2-2-5")));
        tokio::time::advance(QUIET).await;
        assert!(d.done(&src, &c2, &p("2-2-5")).await);
    }

    #[tokio::test(start_paused = true)]
    async fn the_drain_is_bounded() {
        let src = Moving(Mutex::new(vec![p("1-1-10")]));
        let d = StopDrain::begin(&src, Some(&c()), Duration::from_secs(3)).await;
        assert!(!d.expired());
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(d.expired());
    }
}
