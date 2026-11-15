//! Error counts for the administration agent's intake.
//!
//! Every process counts the errors it reports, by (code, span), and
//! nothing else: no message text, no names, no values. The process's
//! component (`gateway`, `controller`, `worker`) and its version are the
//! same for every count, so they travel once with the flushed record.
//!
//! - **Where.** The gateway counts each statement that fails (every
//!   front end), the controller each loop pass that fails, the worker
//!   each job and mover step that fails.
//! - **Span.** A short static path naming where, such as `mysql.query`
//!   or `loop.balancer`: lower case, `[a-z0-9._:/-]`, at most
//!   [`MAX_SPAN`] bytes, runs of four or more digits replaced by `#` so an
//!   id can never become part of one.
//! - **Bounded.** At most [`MAX_KEYS`] distinct (code, span) pairs; later
//!   new pairs only raise `dropped`.
//! - **Cheap.** A count of a pair already seen takes one lock and no
//!   allocation.
//!
//! Counts are cumulative since the process started; the AI crate flushes
//! them to etcd, and the controller turns them into rates.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex, OnceLock, PoisonError};

use crate::error::{ErrorKind, SprigError};

/// Distinct (code, span) pairs one process keeps.
pub const MAX_KEYS: usize = 256;
/// The longest span kept, in bytes.
pub const MAX_SPAN: usize = 96;

/// One count.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Count {
    /// The error's number: a SprigDB code, or the MySQL error number;
    /// 0 when the error had none.
    pub code: u16,
    /// Where.
    pub span: String,
    /// Errors since the process started.
    pub n: u64,
}

/// The counts at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// When the recorder started, ms since the Unix epoch.
    pub started_ms: u64,
    /// Every pair, by code then span.
    pub counts: Vec<Count>,
    /// Errors not counted because [`MAX_KEYS`] pairs were already kept.
    pub dropped: u64,
}

/// A process's error counts (module docs).
#[derive(Debug)]
pub struct Recorder {
    started_ms: u64,
    counts: Mutex<HashMap<u16, HashMap<String, u64>>>,
    keys: AtomicU64,
    dropped: AtomicU64,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl Recorder {
    /// An empty recorder, started now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started_ms: now_ms(),
            counts: Mutex::new(HashMap::new()),
            keys: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// Counts one error `code` at `span`.
    pub fn record_code(&self, code: u16, span: &str) {
        let mut m = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        let spans = m.entry(code).or_default();
        if let Some(n) = spans.get_mut(span) {
            *n += 1;
            return;
        }
        let clean = sanitize_span(span);
        if let Some(n) = spans.get_mut(&clean) {
            *n += 1;
            return;
        }
        if self.keys.load(Ordering::Relaxed) >= MAX_KEYS as u64 {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.keys.fetch_add(1, Ordering::Relaxed);
        spans.insert(clean, 1);
    }

    /// Counts `e` at `span`.
    pub fn record(&self, e: &SprigError, span: &str) {
        self.record_code(code_of(e), span);
    }

    /// The counts now.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let m = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        let mut counts: Vec<Count> = m
            .iter()
            .flat_map(|(code, spans)| {
                spans.iter().map(|(span, n)| Count {
                    code: *code,
                    span: span.clone(),
                    n: *n,
                })
            })
            .collect();
        counts.sort();
        Snapshot {
            started_ms: self.started_ms,
            counts,
            dropped: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// The number an error is counted under: a SprigDB code, or the MySQL
/// error number of a backend or standard error.
#[must_use]
pub fn code_of(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

/// The code at the start of an error's text, as [`SprigError`]'s
/// `Display` writes it (`39100 ER_SPRIG_...: ...`, `1205 (HY000): ...`);
/// 0 when it starts with no number. For errors that reach a counting
/// point as text (a job's failure).
#[must_use]
pub fn code_in(text: &str) -> u16 {
    let t = text.trim_start();
    let digits: &str = &t[..t.bytes().take_while(u8::is_ascii_digit).count()];
    let next = t[digits.len()..].chars().next();
    if digits.is_empty() || !next.is_none_or(|c| c == ' ' || c == ':') {
        return 0;
    }
    digits.parse().unwrap_or(0)
}

/// `span` as it is kept (module docs); `unknown` when nothing is left.
#[must_use]
pub fn sanitize_span(span: &str) -> String {
    let mut out = String::with_capacity(span.len().min(MAX_SPAN));
    let mut digits = 0usize;
    for c in span.trim().chars() {
        let c = c.to_ascii_lowercase();
        let c = if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '/' | '-') {
            c
        } else {
            '_'
        };
        if c.is_ascii_digit() {
            digits += 1;
            if digits == 4 {
                // The run is an id: the three digits already kept go too.
                out.truncate(out.len() - 3);
                out.push('#');
            }
            if digits >= 4 {
                continue;
            }
        } else {
            digits = 0;
        }
        out.push(c);
    }
    while out.len() > MAX_SPAN {
        out.pop();
    }
    if out.is_empty() {
        "unknown".to_owned()
    } else {
        out
    }
}

static GLOBAL: LazyLock<Recorder> = LazyLock::new(Recorder::new);
static COMPONENT: OnceLock<&'static str> = OnceLock::new();

/// Names this process's component (`gateway`, `controller`, `worker`),
/// once, when the role starts.
pub fn set_component(component: &'static str) {
    let _ = COMPONENT.set(component);
    LazyLock::force(&GLOBAL);
}

/// This process's component; `unknown` before [`set_component`].
#[must_use]
pub fn component() -> &'static str {
    COMPONENT.get().copied().unwrap_or("unknown")
}

/// Counts `e` at `span` in the process's recorder.
pub fn record(e: &SprigError, span: &str) {
    GLOBAL.record(e, span);
}

/// Counts error `code` at `span` in the process's recorder.
pub fn record_code(code: u16, span: &str) {
    GLOBAL.record_code(code, span);
}

/// The process's counts now.
#[must_use]
pub fn snapshot() -> Snapshot {
    GLOBAL.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SprigCode;

    #[test]
    fn counts_by_code_and_span_only() {
        let r = Recorder::new();
        let e = SprigError::new(
            SprigCode::ClusterUnavailable,
            "cluster shop-eu-2 for tenant acme",
        );
        r.record(&e, "loop.balancer");
        r.record(&e, "loop.balancer");
        r.record(
            &SprigError::backend(1205, *b"HY000", "lock wait"),
            "mysql.query",
        );
        let s = r.snapshot();
        assert_eq!(
            s.counts,
            vec![
                Count {
                    code: 1205,
                    span: "mysql.query".into(),
                    n: 1
                },
                Count {
                    code: 39100,
                    span: "loop.balancer".into(),
                    n: 2
                },
            ]
        );
        // The message never reaches the counts.
        assert!(!format!("{s:?}").contains("acme"));
        assert_eq!(s.dropped, 0);
    }

    #[test]
    fn spans_are_cleaned_and_bounded() {
        assert_eq!(sanitize_span("Mysql.Query"), "mysql.query");
        assert_eq!(sanitize_span("job.t1234567_x"), "job.t#_x");
        assert_eq!(sanitize_span("mover.copy 12"), "mover.copy_12");
        assert_eq!(sanitize_span("a'b\"c d"), "a_b_c_d");
        assert_eq!(sanitize_span(""), "unknown");
        assert_eq!(sanitize_span(&"x".repeat(500)).len(), MAX_SPAN);
        let r = Recorder::new();
        r.record_code(1, "Job.99999");
        r.record_code(1, "job.88888");
        let s = r.snapshot();
        assert_eq!(s.counts.len(), 1);
        assert_eq!(s.counts[0].span, "job.#");
        assert_eq!(s.counts[0].n, 2);
    }

    #[test]
    fn at_most_max_keys_pairs() {
        let r = Recorder::new();
        for i in 0..(MAX_KEYS + 10) {
            r.record_code(u16::try_from(i).unwrap(), "x");
        }
        r.record_code(0, "x");
        let s = r.snapshot();
        assert_eq!(s.counts.len(), MAX_KEYS);
        assert_eq!(s.dropped, 10);
        assert_eq!(s.counts[0].n, 2);
    }

    #[test]
    fn codes_from_text() {
        let e = SprigError::new(SprigCode::ClusterUnavailable, "x");
        assert_eq!(code_in(&e.to_string()), 39100);
        assert_eq!(code_in("1205 (HY000): lock"), 1205);
        assert_eq!(code_in("no handler"), 0);
        assert_eq!(code_in("12ab"), 0);
        assert_eq!(code_in("99999999 x"), 0);
    }
}
