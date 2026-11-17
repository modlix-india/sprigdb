//! Bounded waits for pooled `sprig_ctl` connections.
//!
//! `mysql_async`'s pool has no wait limit: a `get_conn` waits until a
//! connection comes back, however long that is. A task that holds a pooled
//! connection while it asks the same pool for another waits for itself once
//! the others are busy, and with an unbounded wait it hangs for good,
//! silently.
//!
//! Whoever makes a pool gives it a limit with [`limit`] (the gateway's
//! locator for its catalog, `sprig_ctl` and holders' pools; the
//! controller's and the worker's cluster pools). Acquire sites call
//! [`Bounded::get_conn_bounded`] or [`Bounded::start_transaction_bounded`]
//! instead of the driver's methods: a wait past the pool's limit fails with
//! [`PoolWaitTimeout`] (as `mysql_async::Error::Other`, so the site's own
//! error mapping still applies), which names the pool and the cluster; it
//! is logged once at `warn` and counted in
//! `sprig_pool_wait_timeouts_total{pool, cluster}`. Every bounded
//! acquire is timed in `sprig_pool_wait_seconds{pool, cluster}`. On a pool
//! without a limit the methods are the driver's.
//!
//! The limit is found from the pool itself (its counters' allocation, which
//! clones share), so a pool keeps its limit whatever trait or crate it is
//! passed through.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use mysql_async::{Conn, Metrics, Pool, Transaction, TxOpts};

/// One pool's limit and names.
#[derive(Debug)]
struct Limit {
    /// Keeps the counters' allocation, so its address isn't reused while
    /// the entry exists; `None` from `upgrade` once the pool is gone.
    pool_ref: Weak<Metrics>,
    pool: &'static str,
    cluster: String,
    max: usize,
    after: Duration,
}

/// Limits by the address of a pool's counters.
static LIMITS: LazyLock<Mutex<HashMap<usize, Arc<Limit>>>> = LazyLock::new(Mutex::default);

fn key(m: &Arc<Metrics>) -> usize {
    Arc::as_ptr(m) as usize
}

/// Gives `pool` (and every clone of it) a wait limit: `after`, named
/// `pool_name` on `cluster` in errors and metrics; `max` is its size.
pub fn limit(pool: &Pool, pool_name: &'static str, cluster: &str, max: usize, after: Duration) {
    let m = pool.metrics();
    let mut g = LIMITS.lock().unwrap_or_else(PoisonError::into_inner);
    // Pools replaced since (a writer moved, a credential changed).
    g.retain(|_, l| l.pool_ref.strong_count() > 0);
    g.insert(
        key(&m),
        Arc::new(Limit {
            pool_ref: Arc::downgrade(&m),
            pool: pool_name,
            cluster: cluster.to_owned(),
            max,
            after,
        }),
    );
}

/// `pool`'s wait limit, if it has one.
#[must_use]
pub fn limit_of(pool: &Pool) -> Option<Duration> {
    find(pool).map(|l| l.after)
}

fn find(pool: &Pool) -> Option<Arc<Limit>> {
    let m = pool.metrics();
    LIMITS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key(&m))
        .cloned()
}

/// The bounded acquires (see the module docs).
pub trait Bounded {
    /// A connection, within the pool's wait limit.
    ///
    /// # Errors
    /// [`PoolWaitTimeout`] (as `mysql_async::Error::Other`) past the
    /// limit; the driver's.
    fn get_conn_bounded(
        &self,
    ) -> impl Future<Output = Result<Conn, mysql_async::Error>> + Send + 'static;

    /// A transaction on a connection of the pool; the limit covers the wait
    /// and its `START TRANSACTION`.
    ///
    /// # Errors
    /// As [`Self::get_conn_bounded`]; the server's.
    fn start_transaction_bounded(
        &self,
        opts: TxOpts,
    ) -> impl Future<Output = Result<Transaction<'static>, mysql_async::Error>> + Send + 'static;
}

impl Bounded for Pool {
    fn get_conn_bounded(
        &self,
    ) -> impl Future<Output = Result<Conn, mysql_async::Error>> + Send + 'static {
        let fut = self.get_conn();
        let pool = self.clone();
        async move { within(&pool, fut).await }
    }

    fn start_transaction_bounded(
        &self,
        opts: TxOpts,
    ) -> impl Future<Output = Result<Transaction<'static>, mysql_async::Error>> + Send + 'static
    {
        let pool = self.clone();
        async move {
            let p = pool.clone();
            within(&pool, async move { p.start_transaction(opts).await }).await
        }
    }
}

async fn within<T>(
    pool: &Pool,
    fut: impl Future<Output = Result<T, mysql_async::Error>>,
) -> Result<T, mysql_async::Error> {
    let Some(l) = find(pool) else {
        return fut.await;
    };
    let t0 = Instant::now();
    let r = tokio::time::timeout(l.after, fut).await;
    let waited = t0.elapsed();
    metrics::histogram!("sprig_pool_wait_seconds",
        "pool" => l.pool, "cluster" => l.cluster.clone())
    .record(waited.as_secs_f64());
    match r {
        Ok(r) => r,
        Err(_) => Err(mysql_async::Error::Other(Box::new(timed_out(
            pool, &l, waited,
        )))),
    }
}

fn timed_out(pool: &Pool, l: &Limit, waited: Duration) -> PoolWaitTimeout {
    let m = pool.metrics();
    let e = PoolWaitTimeout {
        pool: l.pool,
        cluster: l.cluster.clone(),
        waited,
        in_use: m.connections_in_use.load(Relaxed),
        max: l.max,
        waiting: m.active_wait_requests.load(Relaxed),
    };
    metrics::counter!("sprig_pool_wait_timeouts_total",
        "pool" => l.pool, "cluster" => l.cluster.clone())
    .increment(1);
    tracing::warn!(pool = e.pool, cluster = %e.cluster, waited_ms = e.waited.as_millis(),
        in_use = e.in_use, max = e.max, waiting = e.waiting,
        "gave up waiting for a pooled connection: all of the pool's connections are in use \
         (a task holding one while it asks the same pool for another waits for itself)");
    e
}

/// A wait for a pooled connection that passed the pool's limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolWaitTimeout {
    /// The pool's name (`catalog`, `sprig_ctl`, a controller budget...).
    pub pool: &'static str,
    /// Its cluster.
    pub cluster: String,
    /// How long the task waited.
    pub waited: Duration,
    /// Connections in use when it gave up.
    pub in_use: usize,
    /// The pool's size.
    pub max: usize,
    /// Tasks waiting when it gave up, itself included.
    pub waiting: usize,
}

impl PoolWaitTimeout {
    /// The timeout `e` carries, if it is one.
    #[must_use]
    pub fn of(e: &mysql_async::Error) -> Option<&PoolWaitTimeout> {
        match e {
            mysql_async::Error::Other(b) => b.downcast_ref::<PoolWaitTimeout>(),
            _ => None,
        }
    }
}

impl std::fmt::Display for PoolWaitTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no connection from the {} pool of cluster {} within {} ms ({} of {} in use, {} \
             waiting)",
            self.pool,
            self.cluster,
            self.waited.as_millis(),
            self.in_use,
            self.max,
            self.waiting
        )
    }
}

impl std::error::Error for PoolWaitTimeout {}

#[cfg(test)]
mod tests;
