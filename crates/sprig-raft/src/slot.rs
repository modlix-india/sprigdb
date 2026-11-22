//! A one-shot result slot that both threads and async tasks can wait on.

use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

/// A value set once by the driver and taken once by the caller. Clones
/// share the slot.
#[derive(Debug)]
pub struct Slot<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug)]
struct Inner<T> {
    value: Mutex<Option<T>>,
    cv: Condvar,
    notify: tokio::sync::Notify,
}

impl<T> Clone for Slot<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Slot<T> {
    /// An empty slot.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                value: Mutex::new(None),
                cv: Condvar::new(),
                notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// A slot that already holds `v`.
    #[must_use]
    pub fn ready(v: T) -> Self {
        let s = Self::new();
        s.set(v);
        s
    }

    /// Sets the value and wakes every waiter. Only the first value counts.
    pub fn set(&self, v: T) {
        {
            let mut g = self
                .inner
                .value
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if g.is_some() {
                return;
            }
            *g = Some(v);
        }
        self.inner.cv.notify_all();
        self.inner.notify.notify_waiters();
    }

    /// Takes the value if it is there.
    pub fn try_take(&self) -> Option<T> {
        self.inner
            .value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Whether a value is waiting.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.inner
            .value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Blocks until the value is set, and takes it. Not for async code.
    pub fn wait(&self) -> T {
        let mut g = self
            .inner
            .value
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(v) = g.take() {
                return v;
            }
            g = self
                .inner
                .cv
                .wait(g)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Blocks for at most `d`; `None` if the value didn't come.
    pub fn wait_timeout(&self, d: Duration) -> Option<T> {
        let deadline = std::time::Instant::now() + d;
        let mut g = self
            .inner
            .value
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(v) = g.take() {
                return Some(v);
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return None;
            }
            g = self
                .inner
                .cv
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Waits for the value in async code, and takes it.
    pub async fn wait_async(&self) -> T {
        loop {
            let n = self.inner.notify.notified();
            let mut n = std::pin::pin!(n);
            n.as_mut().enable();
            if let Some(v) = self.try_take() {
                return v;
            }
            n.await;
        }
    }
}
