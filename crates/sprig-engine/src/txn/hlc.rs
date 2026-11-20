//! The hybrid logical clock and its timestamps.
//!
//! A timestamp is 48 bits of wall-clock milliseconds and 16 bits of logical
//! counter: `ms << 16 | logical`. Timestamps from one clock strictly increase,
//! even when the wall clock stands still or steps back.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub use crate::keys::Ts;

/// MongoDB's `Timestamp(secs, inc)` for a timestamp: order-preserving.
#[must_use]
pub const fn to_mongo(ts: Ts) -> (u32, u32) {
    let ms = ts.physical_ms();
    #[allow(clippy::cast_possible_truncation)]
    let secs = (ms / 1000) as u32;
    #[allow(clippy::cast_possible_truncation)]
    let inc = (((ms % 1000) << 16) as u32) | ts.logical() as u32;
    (secs, inc)
}

/// A wall clock in milliseconds since the Unix epoch. The simulator injects
/// its own.
pub trait Clock: Send + Sync {
    /// Milliseconds since the epoch.
    fn now_ms(&self) -> u64;
}

/// The system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// A clock tests move by hand.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// A clock reading `ms`.
    #[must_use]
    pub fn new(ms: u64) -> Self {
        Self(AtomicU64::new(ms))
    }

    /// Sets the reading.
    pub fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A hybrid logical clock.
pub struct Hlc {
    clock: Arc<dyn Clock>,
    last: AtomicU64,
}

impl fmt::Debug for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hlc")
            .field("last", &self.last.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Default for Hlc {
    fn default() -> Self {
        Self::new(Arc::new(SystemClock))
    }
}

impl Hlc {
    /// A clock over `clock`.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            last: AtomicU64::new(0),
        }
    }

    /// A new timestamp above every one this clock has issued or observed.
    pub fn now(&self) -> Ts {
        let wall = Ts::from_parts(self.clock.now_ms() & ((1 << 48) - 1), 0).0;
        let mut cur = self.last.load(Ordering::Relaxed);
        loop {
            // Wall time ahead: take it. Otherwise tick the logical part; a full
            // counter carries into the millisecond, which keeps order.
            let next = if wall > cur { wall } else { cur + 1 };
            match self
                .last
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Ts(next),
                Err(seen) => cur = seen,
            }
        }
    }

    /// Folds in a timestamp seen elsewhere, so later `now()`s are above it.
    pub fn observe(&self, ts: Ts) {
        self.last.fetch_max(ts.0, Ordering::AcqRel);
    }

    /// The last timestamp issued or observed, without issuing a new one.
    #[must_use]
    pub fn last(&self) -> Ts {
        Ts(self.last.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_and_mongo_conversion_keep_order() {
        let a = Ts::from_parts(1_700_000_000_123, 5);
        assert_eq!(a.physical_ms(), 1_700_000_000_123);
        assert_eq!(a.logical(), 5);
        let b = Ts::from_parts(1_700_000_000_123, 6);
        let c = Ts::from_parts(1_700_000_001_000, 0);
        assert!(to_mongo(a) < to_mongo(b) && to_mongo(b) < to_mongo(c));
        assert_eq!(to_mongo(c), (1_700_000_001, 0));
    }

    #[test]
    fn monotonic_when_the_wall_clock_stalls_or_steps_back() {
        let clock = Arc::new(ManualClock::new(1000));
        let hlc = Hlc::new(clock.clone());
        let a = hlc.now();
        let b = hlc.now();
        assert_eq!(a, Ts::from_parts(1000, 0));
        assert_eq!(b, Ts::from_parts(1000, 1));
        clock.set(900);
        let c = hlc.now();
        assert!(c > b);
        clock.set(2000);
        assert_eq!(hlc.now(), Ts::from_parts(2000, 0));
        hlc.observe(Ts::from_parts(5000, 7));
        assert_eq!(hlc.now(), Ts::from_parts(5000, 8));
    }

    #[test]
    fn logical_overflow_carries_into_the_millisecond() {
        let clock = Arc::new(ManualClock::new(10));
        let hlc = Hlc::new(clock);
        hlc.observe(Ts::from_parts(10, u16::MAX));
        assert_eq!(hlc.now(), Ts::from_parts(11, 0));
    }

    #[test]
    fn concurrent_callers_get_distinct_increasing_values() {
        let hlc = Arc::new(Hlc::new(Arc::new(ManualClock::new(1))));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let h = hlc.clone();
                std::thread::spawn(move || (0..10_000).map(|_| h.now()).collect::<Vec<_>>())
            })
            .collect();
        let mut all = Vec::new();
        for h in handles {
            let v = h.join().unwrap();
            assert!(v.windows(2).all(|w| w[0] < w[1]));
            all.extend(v);
        }
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 40_000);
    }
}
