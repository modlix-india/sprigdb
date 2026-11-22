//! The reply gate: waiting until a log entry is durable.
//!
//! A [`DurableTicket`] is what a reply waits on. On the engine it wraps the
//! commit log's own ticket; the in-memory test path ([`super::testing`])
//! uses a [`DurableWatch`] it can pause and crash.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use sprig_common::Result;

use crate::log::Ticket;

/// A durable index and who waits on it (the test path's log).
#[derive(Debug, Default)]
pub struct DurableWatch {
    durable: AtomicU64,
    failed: AtomicBool,
    waiters: Mutex<Vec<(u64, Waker)>>,
    cv: Condvar,
}

impl DurableWatch {
    fn guard(&self) -> MutexGuard<'_, Vec<(u64, Waker)>> {
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The highest durable index.
    #[must_use]
    pub fn durable(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    /// Whether the log failed: nothing more becomes durable.
    #[must_use]
    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Every index up to `upto` is durable.
    pub fn advance(&self, upto: u64) {
        self.durable.fetch_max(upto, Ordering::AcqRel);
        self.wake(|i| i <= upto);
    }

    /// The log failed: every waiter gets an error.
    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.wake(|_| true);
    }

    fn wake(&self, pred: impl Fn(u64) -> bool) {
        let ready: Vec<Waker> = {
            let mut g = self.guard();
            let mut out = Vec::new();
            g.retain(|(i, w)| {
                if pred(*i) {
                    out.push(w.clone());
                    false
                } else {
                    true
                }
            });
            out
        };
        self.cv.notify_all();
        for w in ready {
            w.wake();
        }
    }

    fn check(&self, index: u64) -> Option<Result<()>> {
        if self.durable() >= index {
            Some(Ok(()))
        } else if self.failed() {
            Some(Err(super::errors::outcome_lost()))
        } else {
            None
        }
    }

    fn wait_blocking(&self, index: u64) -> Result<()> {
        let mut g = self.guard();
        loop {
            if let Some(r) = self.check(index) {
                return r;
            }
            g = self
                .cv
                .wait(g)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct WatchFuture<'a> {
    watch: &'a DurableWatch,
    index: u64,
}

impl Future for WatchFuture<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Some(r) = self.watch.check(self.index) {
            return Poll::Ready(r);
        }
        let mut g = self.watch.guard();
        // Re-check under the latch, so an advance in between can't be missed.
        if let Some(r) = self.watch.check(self.index) {
            return Poll::Ready(r);
        }
        g.push((self.index, cx.waker().clone()));
        Poll::Pending
    }
}

/// What a reply waits for before the client may see it.
#[derive(Debug, Clone, Default)]
pub enum DurableTicket {
    /// Nothing to wait for.
    #[default]
    Ready,
    /// A commit log entry.
    Log(Ticket),
    /// An index of the test path's log.
    Watch(Arc<DurableWatch>, u64),
    /// Entries of several units (a commit across units, or reads of
    /// pending versions on several units): every one must be durable.
    /// Never empty, and never holds another `Many`.
    Many(Arc<[DurableTicket]>),
}

impl DurableTicket {
    /// The log index waited for (0 for [`DurableTicket::Ready`]; the
    /// first unit's for [`DurableTicket::Many`]).
    #[must_use]
    pub fn index(&self) -> u64 {
        match self {
            DurableTicket::Ready => 0,
            DurableTicket::Log(t) => t.index(),
            DurableTicket::Watch(_, i) => *i,
            DurableTicket::Many(ts) => ts.first().map_or(0, DurableTicket::index),
        }
    }

    /// The unit whose log the ticket waits on (`None`: nothing to wait for,
    /// or several units). The test path's log counts as one unit.
    pub(crate) fn unit_key(&self) -> Option<u64> {
        match self {
            DurableTicket::Log(t) => Some(t.unit().0),
            DurableTicket::Watch(..) => Some(u64::MAX),
            DurableTicket::Ready | DurableTicket::Many(_) => None,
        }
    }

    /// Both tickets at once: the later of the two on one unit (its log is
    /// in index order), every unit's latest across units.
    #[must_use]
    pub fn max(self, other: DurableTicket) -> DurableTicket {
        match (&self, &other) {
            (DurableTicket::Ready, _) => return other,
            (_, DurableTicket::Ready) => return self,
            _ => {}
        }
        if let (Some(a), Some(b)) = (self.unit_key(), other.unit_key())
            && a == b
        {
            return if other.index() > self.index() {
                other
            } else {
                self
            };
        }
        let mut parts: Vec<DurableTicket> = Vec::new();
        for t in [self, other] {
            match t {
                DurableTicket::Many(ts) => {
                    for x in ts.iter() {
                        merge_part(&mut parts, x.clone());
                    }
                }
                t => merge_part(&mut parts, t),
            }
        }
        if parts.len() == 1 {
            parts.pop().unwrap_or_default()
        } else {
            DurableTicket::Many(parts.into())
        }
    }

    /// The tickets of a commit across units, in order.
    #[must_use]
    pub fn all(tickets: Vec<DurableTicket>) -> DurableTicket {
        tickets
            .into_iter()
            .fold(DurableTicket::Ready, DurableTicket::max)
    }

    /// The first log ticket (for [`DurableTicket::Many`], its first part's).
    #[must_use]
    pub fn first_log(&self) -> Option<&Ticket> {
        match self {
            DurableTicket::Log(t) => Some(t),
            DurableTicket::Many(ts) => ts.iter().find_map(DurableTicket::first_log),
            _ => None,
        }
    }

    /// Whether the client may be answered now.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        match self {
            DurableTicket::Ready => true,
            DurableTicket::Log(t) => t.is_durable(),
            DurableTicket::Watch(w, i) => w.durable() >= *i,
            DurableTicket::Many(ts) => ts.iter().all(DurableTicket::is_durable),
        }
    }

    /// Blocks until the client may be answered.
    ///
    /// # Errors
    /// The log failed first: the outcome is lost and must not be reported
    /// as success.
    pub fn wait(&self) -> Result<()> {
        match self {
            DurableTicket::Ready => Ok(()),
            DurableTicket::Log(t) => t.wait().map_err(|_| super::errors::outcome_lost()),
            DurableTicket::Watch(w, i) => w.wait_blocking(*i),
            DurableTicket::Many(ts) => ts.iter().try_for_each(DurableTicket::wait),
        }
    }

    /// The async form of [`DurableTicket::wait`].
    ///
    /// # Errors
    /// As for [`DurableTicket::wait`].
    pub async fn wait_async(&self) -> Result<()> {
        match self {
            DurableTicket::Ready => Ok(()),
            DurableTicket::Log(t) => {
                if t.is_durable() {
                    return Ok(());
                }
                t.wait_async()
                    .await
                    .map_err(|_| super::errors::outcome_lost())
            }
            DurableTicket::Watch(w, i) => {
                WatchFuture {
                    watch: w,
                    index: *i,
                }
                .await
            }
            DurableTicket::Many(ts) => {
                for t in ts.iter() {
                    Box::pin(t.wait_async()).await?;
                }
                Ok(())
            }
        }
    }
}

/// Adds `t` to `parts`, keeping one ticket per unit (the later one).
fn merge_part(parts: &mut Vec<DurableTicket>, t: DurableTicket) {
    if matches!(t, DurableTicket::Ready) {
        return;
    }
    if let Some(k) = t.unit_key()
        && let Some(slot) = parts.iter_mut().find(|p| p.unit_key() == Some(k))
    {
        if t.index() > slot.index() {
            *slot = t;
        }
        return;
    }
    parts.push(t);
}
