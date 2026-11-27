//! Reads on a copy: the rules a statement reads under on a replica that
//! doesn't lead the data's unit.
//!
//! A session served on a copy (a reader session) runs each statement inside
//! [`scope`] with its [`ReadPolicy`]. The engine's read path for a unit led
//! elsewhere ([`crate::Engine::follower_read`]) looks the policy up:
//!
//! - [`ReadMode::Exact`] (and no policy at all): a read barrier per read
//!   view, so the read sees every commit acknowledged before it started,
//!   exactly as on the leader;
//! - [`ReadMode::Bounded`]: served at once when this copy holds the
//!   session's token and is known to be at most `max_lag` behind; otherwise
//!   it waits up to [`ReadPolicy::wait`] for both, then runs a read barrier.
//!
//! The token is an HLC timestamp: the session's last commit, raised to every
//! read view it took, so its reads never go back in time and always see its
//! own writes. A copy holds a token `t` for a unit once it has applied every
//! commit of that unit at or below `t` (timestamps follow log order within a
//! unit).

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// How fresh a read on a copy must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// As fresh as the leader: a read barrier per read view.
    Exact,
    /// At most `max_lag` old, and never older than the session's token.
    Bounded {
        /// The largest staleness allowed.
        max_lag: Duration,
    },
}

/// The rules a session's statement reads under on a copy.
#[derive(Debug, Clone)]
pub struct ReadPolicy {
    /// How fresh the read must be.
    pub mode: ReadMode,
    /// How long a bounded read waits for this copy to catch up before it
    /// runs a read barrier.
    pub wait: Duration,
    /// The session's causal token (an HLC timestamp; 0: none). Reads raise
    /// it to the timestamp of every view they open.
    pub token: Arc<AtomicU64>,
}

impl ReadPolicy {
    /// A policy with its own token, starting at `token`.
    #[must_use]
    pub fn new(mode: ReadMode, wait: Duration, token: u64) -> Self {
        Self {
            mode,
            wait,
            token: Arc::new(AtomicU64::new(token)),
        }
    }

    /// The token now.
    #[must_use]
    pub fn token(&self) -> u64 {
        self.token.load(Ordering::Acquire)
    }

    /// Raises the token to `ts` (never lowers it).
    pub fn raise(&self, ts: u64) {
        self.token.fetch_max(ts, Ordering::AcqRel);
    }
}

tokio::task_local! {
    static POLICY: ReadPolicy;
}

/// Runs `f` with `policy` as the rules of every read it makes on a copy.
pub async fn scope<F: Future>(policy: ReadPolicy, f: F) -> F::Output {
    POLICY.scope(policy, f).await
}

/// [`scope`] for synchronous code.
pub fn sync_scope<R>(policy: ReadPolicy, f: impl FnOnce() -> R) -> R {
    POLICY.sync_scope(policy, f)
}

/// The policy of the running statement, if it set one.
#[must_use]
pub fn current() -> Option<ReadPolicy> {
    POLICY.try_with(Clone::clone).ok()
}

tokio::task_local! {
    static SESSION: Arc<AtomicU64>;
}

/// Runs `f` (a whole client connection) with `token` as its session's
/// causal token: every commit `f` makes raises it to the commit's
/// timestamp, so it is the session's own last commit, not every commit the
/// node made since.
pub async fn session_scope<F: Future>(token: Arc<AtomicU64>, f: F) -> F::Output {
    SESSION.scope(token, f).await
}

/// The running session's token, if it has a [`session_scope`].
#[must_use]
pub fn session_token() -> Option<u64> {
    SESSION.try_with(|t| t.load(Ordering::Acquire)).ok()
}

/// The running session's token cell, if it has a [`session_scope`].
#[must_use]
pub fn session_cell() -> Option<Arc<AtomicU64>> {
    SESSION.try_with(Arc::clone).ok()
}

/// Raises the running session's token to `ts` (a commit timestamp; called
/// by every commit). Nothing outside a [`session_scope`].
pub fn note_commit(ts: u64) {
    let _ = SESSION.try_with(|t| t.fetch_max(ts, Ordering::AcqRel));
}

/// How a read on a copy was let through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Served {
    /// At once: the copy held the token and was fresh enough.
    Local,
    /// After a short wait for the copy to catch up.
    Waited,
    /// After a read barrier (the leader confirmed what it had committed).
    Barrier,
}

/// Counts of reads on copies, by how they were let through, since the
/// process started.
#[derive(Debug, Default)]
pub struct ReadStats {
    local: AtomicU64,
    waited: AtomicU64,
    barrier: AtomicU64,
}

impl ReadStats {
    /// `(local, waited, barrier)`.
    #[must_use]
    pub fn get(&self) -> (u64, u64, u64) {
        (
            self.local.load(Ordering::Relaxed),
            self.waited.load(Ordering::Relaxed),
            self.barrier.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn count(&self, s: Served) {
        let c = match s {
            Served::Local => &self.local,
            Served::Waited => &self.waited,
            Served::Barrier => &self.barrier,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }
}

/// The process's counts of reads on copies.
#[must_use]
pub fn stats() -> &'static ReadStats {
    static S: std::sync::OnceLock<ReadStats> = std::sync::OnceLock::new();
    S.get_or_init(ReadStats::default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_policy_is_seen_only_inside_its_scope() {
        assert!(current().is_none());
        let p = ReadPolicy::new(
            ReadMode::Bounded {
                max_lag: Duration::from_secs(1),
            },
            Duration::from_millis(5),
            7,
        );
        let token = Arc::clone(&p.token);
        sync_scope(p, || {
            let c = current().expect("policy in scope");
            assert_eq!(c.token(), 7);
            c.raise(5);
            c.raise(9);
        });
        assert!(current().is_none());
        assert_eq!(token.load(Ordering::Acquire), 9);
    }

    #[tokio::test]
    async fn commits_raise_only_their_own_sessions_token() {
        note_commit(50);
        assert_eq!(session_token(), None);
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        session_scope(Arc::clone(&a), async {
            note_commit(10);
            note_commit(7);
            assert_eq!(session_token(), Some(10));
        })
        .await;
        session_scope(Arc::clone(&b), async { note_commit(3) }).await;
        assert_eq!(a.load(Ordering::Acquire), 10);
        assert_eq!(b.load(Ordering::Acquire), 3);
    }
}
