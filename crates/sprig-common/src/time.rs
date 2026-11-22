//! Clocks.
//!
//! Every component reads time through [`Clock`], never `Instant::now()`
//! directly, so deterministic simulation can swap in a virtual clock.
//! Durations use the monotonic clock; wall-clock time is for logs, audit
//! events, catalog timestamps and statistics buckets only.

use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

/// A source of monotonic and wall-clock time.
pub trait Clock: Send + Sync + 'static {
    /// Monotonic time, for timeouts, leases and cooldowns.
    fn now(&self) -> Instant;
    /// Wall-clock time (UTC), for people and records only.
    fn wall(&self) -> SystemTime;
    /// Wall-clock time at the monotonic reading `at` (one already taken),
    /// for records kept to the second, such as statistics buckets: the
    /// real clocks derive it without reading the wall clock each time.
    fn wall_at(&self, at: Instant) -> SystemTime {
        let _ = at;
        self.wall()
    }
}

/// The real clocks of the operating system.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
    fn wall_at(&self, at: Instant) -> SystemTime {
        poll::wall_at(at)
    }
}

/// A clock that moves only when told to, for tests.
#[derive(Debug)]
pub struct ManualClock {
    state: Mutex<(Instant, SystemTime)>,
}

impl ManualClock {
    /// A clock starting at the current real time.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new((Instant::now(), SystemTime::now())),
        }
    }

    /// Moves both clocks forward by `d`.
    pub fn advance(&self, d: Duration) {
        let mut s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.0 += d;
        s.1 += d;
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }
    fn wall(&self) -> SystemTime {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1
    }
}

/// Readings of the monotonic clock shared by the layers one request passes
/// through within one poll of its future.
///
/// A short statement passes about a dozen layers (statement timing,
/// admission, catalog and lease freshness, workload capture, tracing),
/// and each used to read the clock itself, at about a fifth of a
/// microsecond a reading on a laptop. The gateway's poll guard reads the
/// clock once when a request's poll starts ([`enter`]); inside that poll
/// the layers ask [`now`] or [`start`] for the time instead, and the first
/// layer that needs the time a statement ended reads the clock once more
/// ([`end`]), which the outer layers reuse.
///
/// What a caller gets is never later than the real time, and earlier by
/// at most the work done since the reading: the poll's start, or the end
/// of the last statement in the poll. Outside a poll that called [`enter`]
/// (tests, background tasks) every call reads the clock.
pub mod poll {
    use std::cell::Cell;
    use std::time::{Duration, Instant, SystemTime};

    thread_local! {
        static START: Cell<Option<Instant>> = const { Cell::new(None) };
        static END: Cell<Option<Instant>> = const { Cell::new(None) };
        static STARTED: Cell<bool> = const { Cell::new(false) };
        static ANCHOR: Cell<Option<(Instant, SystemTime)>> = const { Cell::new(None) };
    }

    /// How old a thread's pair of readings for [`wall_at`] may get.
    const ANCHOR_AGE: Duration = Duration::from_secs(1);

    /// What [`enter`] replaced, for [`leave`] (polls nest).
    #[derive(Debug, Clone, Copy)]
    pub struct Saved {
        start: Option<Instant>,
        end: Option<Instant>,
        started: bool,
    }

    /// A poll starts at `start` (a reading the caller took).
    #[must_use]
    pub fn enter(start: Instant) -> Saved {
        Saved {
            start: START.replace(Some(start)),
            end: END.replace(None),
            started: STARTED.replace(false),
        }
    }

    /// The poll ended: restores what [`enter`] replaced, and returns the end
    /// reading a layer took during the poll, if any and if nothing started
    /// after it.
    #[must_use]
    pub fn leave(saved: Saved) -> Option<Instant> {
        START.set(saved.start);
        let open = STARTED.replace(saved.started);
        let end = END.replace(saved.end);
        if open { None } else { end }
    }

    /// The time now, for a check that tolerates the poll's age: the latest
    /// reading of this poll.
    #[must_use]
    pub fn now() -> Instant {
        match START.get() {
            Some(s) => END.get().unwrap_or(s),
            None => Instant::now(),
        }
    }

    /// The time a statement starts: [`now`], noted so that the next
    /// [`end`] reads the clock again.
    #[must_use]
    pub fn start() -> Instant {
        if START.get().is_none() {
            return Instant::now();
        }
        STARTED.set(true);
        now()
    }

    /// The wall-clock time at `at`, from a pair of readings of both clocks
    /// that each thread takes again once its pair is a second old: a
    /// wall time to well within a second without reading the wall clock
    /// for every statement.
    #[must_use]
    pub fn wall_at(at: Instant) -> SystemTime {
        ANCHOR.with(|a| {
            let (mono, wall) = match a.get() {
                Some((m, w))
                    if at.saturating_duration_since(m) < ANCHOR_AGE
                        && m.saturating_duration_since(at) < ANCHOR_AGE =>
                {
                    (m, w)
                }
                _ => {
                    let fresh = (Instant::now(), SystemTime::now());
                    a.set(Some(fresh));
                    fresh
                }
            };
            match at.checked_duration_since(mono) {
                Some(d) => wall + d,
                None => wall - mono.saturating_duration_since(at),
            }
        })
    }

    /// The time a statement ended: one reading per statement, reused by
    /// every caller until something [`start`]s again.
    #[must_use]
    pub fn end() -> Instant {
        if START.get().is_none() {
            return Instant::now();
        }
        if !STARTED.get()
            && let Some(e) = END.get()
        {
            return e;
        }
        let e = Instant::now();
        END.set(Some(e));
        STARTED.set(false);
        e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_readings_are_shared_within_a_poll() {
        // Outside a poll every call reads the clock.
        let a = poll::now();
        assert!(poll::end() >= a);
        let t0 = Instant::now();
        let saved = poll::enter(t0);
        assert_eq!(poll::now(), t0);
        let s = poll::start();
        assert_eq!(s, t0);
        std::thread::sleep(Duration::from_millis(2));
        let e1 = poll::end();
        assert!(e1 >= t0 + Duration::from_millis(2));
        // The outer layers reuse the end; plain reads see it.
        assert_eq!(poll::end(), e1);
        assert_eq!(poll::now(), e1);
        // A second statement in the same poll starts at the first's end
        // and gets its own end.
        assert_eq!(poll::start(), e1);
        std::thread::sleep(Duration::from_millis(2));
        let e2 = poll::end();
        assert!(e2 >= e1 + Duration::from_millis(2));
        // A nested poll restores the outer one.
        let inner = poll::enter(e2);
        assert_eq!(poll::leave(inner), None);
        assert_eq!(poll::now(), e2);
        assert_eq!(poll::leave(saved), Some(e2));
        assert!(poll::now() >= e2);
    }

    #[test]
    fn wall_at_follows_the_wall_clock() {
        let c = SystemClock;
        let (m, w) = (Instant::now(), SystemTime::now());
        let d = |a: SystemTime, b: SystemTime| a.duration_since(b).unwrap_or_else(|e| e.duration());
        assert!(d(c.wall_at(m), w) < Duration::from_millis(50));
        let later = m + Duration::from_millis(300);
        assert!(d(c.wall_at(later), w + Duration::from_millis(300)) < Duration::from_millis(50));
        if let Some(earlier) = m.checked_sub(Duration::from_secs(5)) {
            assert!(d(c.wall_at(earlier), w - Duration::from_secs(5)) < Duration::from_millis(50));
        }
        // A manual clock keeps its own wall time.
        let mc = ManualClock::new();
        assert_eq!(mc.wall_at(m), mc.wall());
    }

    #[test]
    fn manual_clock_moves_only_when_advanced() {
        let c = ManualClock::new();
        let t0 = c.now();
        assert_eq!(c.now(), t0);
        c.advance(Duration::from_secs(5));
        assert_eq!(c.now() - t0, Duration::from_secs(5));
    }
}
