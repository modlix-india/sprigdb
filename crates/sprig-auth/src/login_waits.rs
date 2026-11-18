//! The time a login spends waiting on the server's side, so a front end's
//! handshake deadline counts only the client's time.
//!
//! A front end whose connection phase has a deadline (the gateway's
//! `gateway.handshake_timeout_ms`) runs the login inside [`LOGIN_WAITS`]'s
//! scope. Waits of the server's own ([`LoginWaits::begin`]: a failure delay,
//! the RSA key the first plain-TCP login needs) don't count toward the
//! deadline, and once a refusal's delay has begun ([`LoginWaits::refusing`])
//! the refusal gets a whole handshake timeout after its delay to reach the
//! client as its ERR packet. Before, the deadline moved back by the delay
//! alone, so a refusal whose delay started late in the phase, or a first
//! login waiting for the key, could end in a dropped connection.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

tokio::task_local! {
    /// The server-side waits of the logins run in this scope.
    pub static LOGIN_WAITS: Arc<LoginWaits>;
}

/// Server-side waits of one connection's login.
#[derive(Debug, Default)]
pub struct LoginWaits {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    /// Waits in progress.
    active: u32,
    /// When the waits in progress began (the first of them).
    since: Option<Instant>,
    /// Waits that have ended, added up.
    total: Duration,
    /// When the last wait ended.
    last_end: Option<Instant>,
    /// A refusal's delay has begun: the login ends in an ERR packet.
    refusing: bool,
}

/// One wait in progress; it ends when dropped.
#[derive(Debug)]
pub struct LoginWait {
    waits: Arc<LoginWaits>,
}

impl Drop for LoginWait {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut i = self.waits.lock();
        i.active = i.active.saturating_sub(1);
        if i.active == 0 {
            if let Some(since) = i.since.take() {
                i.total += now.saturating_duration_since(since);
            }
            i.last_end = Some(now);
        }
    }
}

impl LoginWaits {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a server-side wait, which lasts until the guard is dropped.
    #[must_use]
    pub fn begin(self: &Arc<Self>) -> LoginWait {
        let mut i = self.lock();
        if i.active == 0 {
            i.since = Some(Instant::now());
        }
        i.active += 1;
        drop(i);
        LoginWait {
            waits: Arc::clone(self),
        }
    }

    /// Marks the login as refused: its failure delay begins.
    pub fn refusing(&self) {
        self.lock().refusing = true;
    }

    /// When a connection phase that started at `start` with a deadline of
    /// `timeout` times out, as of `now`: `start + timeout` moved back by the
    /// server's waits (those in progress up to `now`); for a refused login,
    /// not before `timeout` after its last wait ended. While a wait is in
    /// progress the deadline moves with `now`, so it stays what the client
    /// has left ahead of `now`.
    #[must_use]
    pub fn deadline(&self, start: Instant, timeout: Duration, now: Instant) -> Instant {
        let i = self.lock();
        let running = i
            .since
            .filter(|_| i.active > 0)
            .map_or(Duration::ZERO, |s| now.saturating_duration_since(s));
        let client = start + timeout + i.total + running;
        match (i.refusing, i.active, i.last_end) {
            (true, 0, Some(end)) => client.max(end + timeout),
            _ => client,
        }
    }

    /// Whether a wait is in progress now.
    #[must_use]
    pub fn waiting(&self) -> bool {
        self.lock().active > 0
    }
}

/// Starts a server-side wait of the login running in [`LOGIN_WAITS`]'s
/// scope; `None` outside it.
#[must_use]
pub fn begin_wait() -> Option<LoginWait> {
    LOGIN_WAITS.try_with(LoginWaits::begin).ok()
}

/// Runs `phase`, a connection phase started now, with a deadline of
/// `timeout` of the client's time ([`LoginWaits::deadline`]): its output,
/// or `None` when the deadline passed first.
pub async fn with_deadline<F: std::future::Future>(
    timeout: Duration,
    phase: F,
) -> Option<F::Output> {
    let waits = Arc::new(LoginWaits::default());
    let start = Instant::now();
    let phase = LOGIN_WAITS.scope(Arc::clone(&waits), phase);
    tokio::pin!(phase);
    loop {
        let at = waits.deadline(start, timeout, Instant::now());
        tokio::select! {
            r = &mut phase => return Some(r),
            () = tokio::time::sleep_until(at) => {
                let now = Instant::now();
                if !waits.waiting() && now >= waits.deadline(start, timeout, now) {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Duration = Duration::from_millis(1500);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A refusal: `before` of the server's own work, then a failure delay
    /// of `delay`, then `after` until the ERR packet is written.
    async fn refusal(before: Duration, delay: Duration, after: Duration) -> &'static str {
        tokio::time::sleep(before).await;
        LOGIN_WAITS.with(|w| w.refusing());
        {
            let _w = begin_wait();
            tokio::time::sleep(delay).await;
        }
        tokio::time::sleep(after).await;
        "ERR"
    }

    #[tokio::test(start_paused = true)]
    async fn a_delay_past_the_timeout_still_ends_with_its_refusal() {
        assert_eq!(
            with_deadline(T, refusal(ms(10), ms(2000), ms(10))).await,
            Some("ERR")
        );
        // The delay begins just before the deadline and the packet takes a
        // while after it: moving the deadline back by the delay alone cut
        // this one off at 3.5 s.
        assert_eq!(
            with_deadline(T, refusal(ms(1400), ms(2000), ms(300))).await,
            Some("ERR")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_servers_waits_dont_count_but_the_clients_time_does() {
        // A first login waiting 3 s for the RSA key, then 1 s of the
        // client's: within 1.5 s of the client's time.
        let key_wait = async {
            {
                let _w = begin_wait();
                tokio::time::sleep(ms(3000)).await;
            }
            tokio::time::sleep(ms(1000)).await;
            "OK"
        };
        assert_eq!(with_deadline(T, key_wait).await, Some("OK"));
        // The same with 1.6 s of the client's: timed out.
        let slow = async {
            {
                let _w = begin_wait();
                tokio::time::sleep(ms(3000)).await;
            }
            tokio::time::sleep(ms(1600)).await;
            "OK"
        };
        let t = Instant::now();
        assert_eq!(with_deadline(T, slow).await, None);
        assert_eq!(t.elapsed(), ms(4500));
        // A silent client: timed out at the deadline.
        let t = Instant::now();
        assert_eq!(with_deadline(T, tokio::time::sleep(ms(10_000))).await, None);
        assert_eq!(t.elapsed(), T);
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_waits_count_once() {
        let w = Arc::new(LoginWaits::default());
        let start = Instant::now();
        let a = w.begin();
        tokio::time::sleep(ms(500)).await;
        let b = w.begin();
        tokio::time::sleep(ms(500)).await;
        drop(a);
        tokio::time::sleep(ms(500)).await;
        drop(b);
        // 1.5 s of waiting in all, none of it the client's.
        assert_eq!(w.deadline(start, T, Instant::now()), start + T + ms(1500));
    }
}
