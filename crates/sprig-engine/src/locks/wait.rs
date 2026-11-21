//! A queued lock request's wait: async for sessions on tokio workers,
//! blocking for threads.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::{Interrupt, LockTable, TxnId};

/// Where a queued request stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotState {
    Waiting,
    Granted,
    Deadlock,
    Interrupted(Interrupt),
}

/// How a wait ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    /// The lock is held.
    Granted,
    /// The wait limit passed; the request was withdrawn (1205).
    TimedOut,
    /// Chosen as a deadlock victim while waiting (1213).
    Deadlock,
    /// Ended from outside: `KILL QUERY` or a statement time limit. The
    /// request was withdrawn.
    Interrupted(Interrupt),
}

#[derive(Debug)]
pub(crate) struct Slot {
    inner: Mutex<(SlotState, Option<Waker>)>,
    cv: Condvar,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            inner: Mutex::new((SlotState::Waiting, None)),
            cv: Condvar::new(),
        }
    }
}

impl Slot {
    fn guard(&self) -> MutexGuard<'_, (SlotState, Option<Waker>)> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn get(&self) -> SlotState {
        self.guard().0
    }

    pub(crate) fn set(&self, s: SlotState) {
        let waker = {
            let mut g = self.guard();
            g.0 = s;
            g.1.take()
        };
        self.cv.notify_all();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

struct SlotFuture<'a>(&'a Slot);

impl Future for SlotFuture<'_> {
    type Output = SlotState;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<SlotState> {
        let mut g = self.0.guard();
        if g.0 == SlotState::Waiting {
            match &g.1 {
                Some(w) if w.will_wake(cx.waker()) => {}
                _ => g.1 = Some(cx.waker().clone()),
            }
            Poll::Pending
        } else {
            Poll::Ready(g.0)
        }
    }
}

/// A queued request. Dropping it unfinished withdraws the request.
#[derive(Debug)]
pub struct LockWait {
    table: Arc<LockTable>,
    txn: TxnId,
    key: Bytes,
    insert: bool,
    upgrade: bool,
    slot: Arc<Slot>,
    started: Instant,
    done: bool,
}

impl LockWait {
    pub(crate) fn new(
        table: Arc<LockTable>,
        txn: TxnId,
        key: Bytes,
        insert: bool,
        upgrade: bool,
        slot: Arc<Slot>,
    ) -> Self {
        Self {
            table,
            txn,
            key,
            insert,
            upgrade,
            slot,
            started: Instant::now(),
            done: false,
        }
    }

    fn outcome(s: SlotState) -> WaitOutcome {
        match s {
            SlotState::Granted => WaitOutcome::Granted,
            SlotState::Deadlock => WaitOutcome::Deadlock,
            SlotState::Interrupted(why) => WaitOutcome::Interrupted(why),
            SlotState::Waiting => WaitOutcome::TimedOut,
        }
    }

    /// The key the request is for.
    #[must_use]
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Whether this is an insert waiting for gap locks (true) or a record
    /// lock request (false).
    #[must_use]
    pub fn is_insert_intention(&self) -> bool {
        self.insert
    }

    /// Whether the request upgrades a shared lock the transaction already
    /// holds.
    #[must_use]
    pub fn is_upgrade(&self) -> bool {
        self.upgrade
    }

    fn finish(mut self, out: WaitOutcome) -> WaitOutcome {
        self.done = true;
        self.table.note_wait(self.started.elapsed());
        out
    }

    /// Waits up to `limit` (`None`: forever) on a tokio runtime.
    pub async fn wait(self, limit: Option<Duration>) -> WaitOutcome {
        let state = match limit {
            None => SlotFuture(&self.slot).await,
            Some(d) => {
                let Ok(s) = tokio::time::timeout(d, SlotFuture(&self.slot)).await else {
                    let out = self.table.cancel_wait(self.txn, &self.slot);
                    return self.finish(out);
                };
                s
            }
        };
        self.finish(Self::outcome(state))
    }

    /// Waits up to `limit` (`None`: forever), blocking the thread.
    #[must_use]
    pub fn wait_blocking(self, limit: Option<Duration>) -> WaitOutcome {
        let deadline = limit.map(|d| Instant::now() + d);
        let state = {
            let mut g = self.slot.guard();
            loop {
                if g.0 != SlotState::Waiting {
                    break Some(g.0);
                }
                match deadline {
                    None => {
                        g = self
                            .slot
                            .cv
                            .wait(g)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    Some(dl) => {
                        let now = Instant::now();
                        if now >= dl {
                            break None;
                        }
                        g = self
                            .slot
                            .cv
                            .wait_timeout(g, dl - now)
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .0;
                    }
                }
            }
        };
        let out = match state {
            Some(s) => Self::outcome(s),
            None => self.table.cancel_wait(self.txn, &self.slot),
        };
        self.finish(out)
    }
}

impl Drop for LockWait {
    fn drop(&mut self) {
        if !self.done {
            // Abandoned (for example a killed query): withdraw the request.
            // A grant that raced in is kept until the transaction ends, like
            // any lock a failed statement took.
            let _ = self.table.cancel_wait(self.txn, &self.slot);
        }
    }
}
