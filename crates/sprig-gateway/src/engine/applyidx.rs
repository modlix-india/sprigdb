//! Apply-time indexes: indexes every replica keeps of its units' rows
//! while it applies their commits (full text first; vectors, geo cells and
//! the other models' search indexes are the same kind of thing).
//!
//! ```text
//! apply step (log writer's thread)      one worker thread per index
//!   Dispatcher::applied(unit, entries)    loop:
//!     each versioned put of a followed      Put / Del / Clear -> the index
//!     row prefix -> Put or Del message      Mark(i): what it has seen
//!     a range delete over it -> Clear       every 200 ms, 10,000 changes,
//!     the unit's dispatched index           or a Flush: commit(applied),
//!     (and a Mark, at most every 50 ms        then the log floor
//!     or when a reader waits)
//! ```
//!
//! - **Never blocking the apply.** The dispatcher only walks payloads and
//!   queues messages; decoding rows and writing the index are the worker's.
//! - **Floors.** Each commit records the last log index the index's files
//!   hold (the index's payload) and raises the engine's log floor
//!   `ai/<name>` on the unit to it, so the commit log keeps every entry a
//!   replay needs. After a restart an index replays from its payload.
//! - **Online builds.** A new index is registered first (from then on
//!   every applied change is queued for it), then its worker writes every
//!   row of the table as a fresh read finds it, then the queue: a row seen
//!   both ways is written twice, which is harmless (every write replaces
//!   the row's entry). Writers never wait for a build.
//! - **Waiting for commits.** A reader that must see the unit's commits up
//!   to an index waits until the dispatcher has passed it, then sends a
//!   `Flush` through the worker's queue and waits for that commit
//!   ([`Handle::sync`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys;
use sprig_engine::log::LogEntry;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{ApplyObserver, Cf, UnitId};

use super::store::Tx;

/// How often a worker commits while changes come in.
pub(crate) const COMMIT_EVERY: Duration = Duration::from_millis(200);
/// Changes held before a commit regardless of the time.
const COMMIT_PENDING: u64 = 10_000;
/// How often the dispatcher tells an idle-looking index how far it got.
const MARK_EVERY: Duration = Duration::from_millis(50);
/// How often an index with no changes of its own raises its floor.
const FLOOR_EVERY: Duration = Duration::from_secs(1);
/// Rows a build reads per transaction.
const BUILD_BATCH: usize = 1024;
/// The age of every change that came through the log is above every
/// row a build wrote.
const SEQ_LOG: u64 = 1 << 40;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The age of the change at position `op` of log entry `index`: a later
/// change has a higher number, and every change from the log is above
/// every row a build wrote. (Ties in `WHERE MATCH` come back oldest
/// first, as the servers order documents.)
#[must_use]
pub(crate) fn seq_of(index: u64, op: u64) -> u64 {
    SEQ_LOG + ((index << 24) | op.min((1 << 24) - 1))
}

/// What one kind of index does with its rows.
pub(crate) trait IndexStore: Send + Sync + 'static {
    /// The row of `key` (the row key after its 19-byte prefix) is now
    /// `row` (its stored record); `seq` is its age.
    ///
    /// # Errors
    /// The row doesn't decode, or the index failed.
    fn upsert(&self, key: &[u8], row: &Bytes, seq: u64) -> Result<()>;
    /// The row of `key` went.
    ///
    /// # Errors
    /// The index failed.
    fn delete(&self, key: &[u8]) -> Result<()>;
    /// Every row went.
    ///
    /// # Errors
    /// The index failed.
    fn clear(&self) -> Result<()>;
    /// Makes what came so far durable and readable, as covering log
    /// index `applied`.
    ///
    /// # Errors
    /// The index failed.
    fn commit(&self, applied: u64) -> Result<()>;
}

/// Where an index is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Writing the table's rows.
    Building,
    /// Following the log.
    Ready,
    /// Stopped by an error (its message).
    Failed(String),
    /// Dropped.
    Stopped,
}

enum Msg {
    Put(Vec<u8>, Bytes, u64),
    Del(Vec<u8>),
    Clear,
    /// The dispatcher has handed over every change up to this index.
    Mark(u64),
    /// Commit now, covering at least this index.
    Flush(u64),
    Stop,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    /// The last log index the committed files cover.
    committed: u64,
    /// Rows the build wrote.
    built: u64,
}

/// One registered index.
pub(crate) struct Handle {
    /// The engine's floor name (`ai/...`).
    name: String,
    unit: UnitId,
    /// The rows' prefix (`tenant | 0 | object | P | generation`).
    prefix: Vec<u8>,
    /// Entries at or below this are in the index already (its files, or
    /// the build's read).
    start: u64,
    tx: Mutex<Sender<Msg>>,
    state: Mutex<State>,
    changed: Condvar,
    /// The index a reader waits for (the dispatcher marks it at once).
    wanted: AtomicU64,
    /// The last index the dispatcher marked.
    marked: AtomicU64,
    /// The worker thread.
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("name", &self.name)
            .field("unit", &self.unit)
            .finish_non_exhaustive()
    }
}

impl Handle {
    fn send(&self, m: Msg) {
        let _ = lock(&self.tx).send(m);
    }

    /// Where the index is.
    pub(crate) fn phase(&self) -> Phase {
        lock(&self.state).phase.clone()
    }

    /// The last log index the committed files cover.
    pub(crate) fn committed(&self) -> u64 {
        lock(&self.state).committed
    }

    /// Waits until the build is done (or failed), up to `timeout`.
    ///
    /// # Errors
    /// The build failed, or it is still running after `timeout` (39824).
    pub(crate) fn wait_built(&self, timeout: Duration) -> Result<()> {
        let until = Instant::now() + timeout;
        let mut st = lock(&self.state);
        loop {
            match &st.phase {
                Phase::Ready => return Ok(()),
                Phase::Failed(m) => return Err(failed(m)),
                Phase::Stopped => return Err(failed("the index was dropped")),
                Phase::Building => {}
            }
            let now = Instant::now();
            if now >= until {
                return Err(SprigError::new(
                    SprigCode::SearchIndexNotReady,
                    format!(
                        "the index is still being built ({} rows so far); try again",
                        st.built
                    ),
                ));
            }
            st = self
                .changed
                .wait_timeout(st, until - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Waits until the committed files cover every commit of the unit up
    /// to `target` (a reader's view), committing at once if need be.
    ///
    /// # Errors
    /// The index failed, or `timeout` passed (39826, retryable).
    pub(crate) fn sync(&self, d: &Dispatcher, target: u64, timeout: Duration) -> Result<()> {
        if self.committed() >= target {
            return Ok(());
        }
        let until = Instant::now() + timeout;
        self.wanted.fetch_max(target, Ordering::AcqRel);
        // Every change up to `target` handed to the worker, then a flush
        // behind them.
        d.flush_after(self, target, until)?;
        let mut st = lock(&self.state);
        loop {
            if st.committed >= target {
                return Ok(());
            }
            if let Phase::Failed(m) = &st.phase {
                return Err(failed(m));
            }
            let now = Instant::now();
            if now >= until {
                return Err(timed_out());
            }
            st = self
                .changed
                .wait_timeout(st, until - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

fn failed(m: &str) -> SprigError {
    SprigError::new(
        SprigCode::SearchUnavailable,
        format!("the index can't be read: {m}"),
    )
}

fn timed_out() -> SprigError {
    let mut e = SprigError::new(
        SprigCode::SearchWaitTimeout,
        "the index didn't catch up with the latest commits in time",
    );
    e.retryable = true;
    e
}

/// What the dispatcher knows of one unit.
#[derive(Default)]
struct UnitState {
    /// Every change of entries up to here was handed to the workers.
    dispatched: u64,
    last_mark: Option<Instant>,
    handles: Vec<Arc<Handle>>,
}

/// The node's apply observer for every apply-time index.
pub(crate) struct Dispatcher {
    eng: sprig_engine::Engine,
    units: Mutex<HashMap<UnitId, UnitState>>,
    progress: Condvar,
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher").finish_non_exhaustive()
    }
}

/// One operation of a log payload, borrowed.
enum PayloadOp<'a> {
    Put(u8, &'a [u8], &'a [u8]),
    DeleteRange(u8, &'a [u8], &'a [u8]),
    Other,
}

/// Walks a payload's operations (`sprig_engine::log::payload`'s format)
/// without copying; false when malformed.
fn walk_payload<'a>(data: &'a [u8], mut f: impl FnMut(PayloadOp<'a>)) -> bool {
    fn varint(b: &[u8], at: &mut usize) -> Option<usize> {
        let mut v = 0usize;
        for shift in (0..64).step_by(7) {
            let x = *b.get(*at)?;
            *at += 1;
            v |= usize::from(x & 0x7F) << shift;
            if x < 0x80 {
                return Some(v);
            }
        }
        None
    }
    fn slice<'a>(b: &'a [u8], at: &mut usize) -> Option<&'a [u8]> {
        let n = varint(b, at)?;
        let s = b.get(*at..*at + n)?;
        *at += n;
        Some(s)
    }
    let mut at = 1usize;
    if data.first() != Some(&1) {
        return false;
    }
    while at < data.len() {
        let code = data[at];
        at += 1;
        let ok = (|| -> Option<PayloadOp<'a>> {
            Some(match code {
                1 => {
                    let cf = *data.get(at)?;
                    at += 1;
                    let k = slice(data, &mut at)?;
                    let v = slice(data, &mut at)?;
                    PayloadOp::Put(cf, k, v)
                }
                2 => {
                    at += 1;
                    slice(data, &mut at)?;
                    PayloadOp::Other
                }
                3 => {
                    let cf = *data.get(at)?;
                    at += 1;
                    let s = slice(data, &mut at)?;
                    let e = slice(data, &mut at)?;
                    PayloadOp::DeleteRange(cf, s, e)
                }
                4 => {
                    slice(data, &mut at)?;
                    PayloadOp::Other
                }
                _ => return None,
            })
        })();
        match ok {
            Some(op) => f(op),
            None => return false,
        }
    }
    true
}

impl Dispatcher {
    fn new(eng: sprig_engine::Engine) -> Self {
        Self {
            eng,
            units: Mutex::new(HashMap::new()),
            progress: Condvar::new(),
        }
    }

    /// The engine.
    pub(crate) fn engine(&self) -> &sprig_engine::Engine {
        &self.eng
    }

    fn dispatch(&self, unit: UnitId, entries: &[LogEntry]) {
        let Some(last) = entries.last().map(|e| e.index) else {
            return;
        };
        let mut units = lock(&self.units);
        let us = units.entry(unit).or_default();
        if !us.handles.is_empty() {
            let data = Cf::Data.code();
            for e in entries {
                let mut op = 0u64;
                walk_payload(&e.data, |o| {
                    op += 1;
                    match o {
                        PayloadOp::Put(cf, key, value) if cf == data => {
                            if key.len() < keys::PREFIX_LEN + keys::VERSION_LEN
                                || keys::is_unversioned(key)
                            {
                                return;
                            }
                            let Some((logical, _)) = keys::split_version(key) else {
                                return;
                            };
                            let head = &logical[..keys::PREFIX_LEN];
                            for h in &us.handles {
                                if h.prefix != head || e.index <= h.start {
                                    continue;
                                }
                                let k = logical[keys::PREFIX_LEN..].to_vec();
                                match value.split_first() {
                                    Some((&keys::value::PUT, row)) => h.send(Msg::Put(
                                        k,
                                        Bytes::copy_from_slice(row),
                                        seq_of(e.index, op),
                                    )),
                                    _ => h.send(Msg::Del(k)),
                                }
                            }
                        }
                        PayloadOp::DeleteRange(cf, s, end) if cf == data => {
                            for h in &us.handles {
                                if e.index <= h.start {
                                    continue;
                                }
                                let hi = keys::prefix_end(&h.prefix);
                                let covers = s <= h.prefix.as_slice()
                                    && hi.as_deref().is_none_or(|hi| end >= hi);
                                if covers {
                                    h.send(Msg::Clear);
                                }
                            }
                        }
                        _ => {}
                    }
                });
            }
            let now = Instant::now();
            let due = us
                .last_mark
                .is_none_or(|t| now.duration_since(t) >= MARK_EVERY);
            for h in &us.handles {
                if due || h.wanted.load(Ordering::Acquire) > h.marked.load(Ordering::Acquire) {
                    h.marked.store(last, Ordering::Release);
                    h.send(Msg::Mark(last));
                }
            }
            if due {
                us.last_mark = Some(now);
            }
        }
        us.dispatched = us.dispatched.max(last);
        drop(units);
        self.progress.notify_all();
    }

    /// Waits until every change of `h`'s unit up to `target` was handed to
    /// it, then queues a flush behind them.
    fn flush_after(&self, h: &Handle, target: u64, until: Instant) -> Result<()> {
        let mut units = lock(&self.units);
        loop {
            let d = units.get(&h.unit).map_or(0, |u| u.dispatched);
            if d >= target {
                h.send(Msg::Flush(d));
                return Ok(());
            }
            let now = Instant::now();
            if now >= until {
                return Err(timed_out());
            }
            units = self
                .progress
                .wait_timeout(units, until - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// The unit's latest commit a reader may see now: what an index must
    /// cover for `latest` freshness.
    pub(crate) fn target(&self, unit: UnitId) -> u64 {
        self.eng.consensus().proposed_index(unit)
    }

    /// Registers an index of `prefix`'s rows on `unit`. `from`: the last
    /// entry its files already hold (`None`: none; it is built from the
    /// table's rows first). Returns its handle; its worker runs until
    /// [`Dispatcher::drop_index`].
    pub(crate) fn register(
        self: &Arc<Self>,
        name: String,
        unit: UnitId,
        prefix: Vec<u8>,
        store: Arc<dyn IndexStore>,
        from: Option<u64>,
    ) -> Arc<Handle> {
        let (tx, rx) = mpsc::channel();
        let mut units = lock(&self.units);
        let us = units.entry(unit).or_default();
        // A new index: the rows it reads hold every entry handed over so
        // far; the queue holds every later one.
        let start = match from {
            Some(f) => f,
            None => us.dispatched.max(self.eng.last_applied_of(unit)),
        };
        let h = Arc::new(Handle {
            name,
            unit,
            prefix,
            start,
            tx: Mutex::new(tx),
            state: Mutex::new(State {
                phase: if from.is_some() {
                    Phase::Ready
                } else {
                    Phase::Building
                },
                committed: from.unwrap_or(0),
                built: 0,
            }),
            changed: Condvar::new(),
            wanted: AtomicU64::new(0),
            marked: AtomicU64::new(0),
            worker: Mutex::new(None),
        });
        us.handles.push(Arc::clone(&h));
        drop(units);
        // The log keeps what the index may still need.
        if let Err(e) = self.eng.set_log_floors(&h.name, &[(unit, start)]) {
            tracing::warn!(index = %h.name, error = %e, "apply-time index: its log floor wasn't set");
        }
        let worker = Worker {
            d: Arc::downgrade(self),
            h: Arc::clone(&h),
            store,
            build: from.is_none(),
        };
        let spawned = std::thread::Builder::new()
            .name(format!("sprig-ai-{}", h.unit.0))
            .spawn(move || worker.run(&rx));
        match spawned {
            Ok(j) => *lock(&h.worker) = Some(j),
            Err(e) => {
                let mut st = lock(&h.state);
                st.phase = Phase::Failed(format!("no worker thread: {e}"));
            }
        }
        h
    }

    /// Stops `h`'s worker and forgets the index; its floor goes. The
    /// caller removes its files.
    pub(crate) fn drop_index(&self, h: &Arc<Handle>) {
        {
            let mut units = lock(&self.units);
            if let Some(us) = units.get_mut(&h.unit) {
                us.handles.retain(|x| !Arc::ptr_eq(x, h));
            }
        }
        h.send(Msg::Stop);
        {
            let mut st = lock(&h.state);
            st.phase = Phase::Stopped;
        }
        h.changed.notify_all();
        if let Err(e) = self.eng.clear_log_floor(&h.name) {
            tracing::warn!(index = %h.name, error = %e, "apply-time index: its log floor wasn't cleared");
        }
    }

    /// Stops `h`'s worker and waits for it, keeping its floor and files
    /// (tests; a process that stops simply exits).
    #[cfg(test)]
    pub(crate) fn stop(&self, h: &Arc<Handle>) {
        {
            let mut units = lock(&self.units);
            if let Some(us) = units.get_mut(&h.unit) {
                us.handles.retain(|x| !Arc::ptr_eq(x, h));
            }
        }
        h.send(Msg::Stop);
        if let Some(j) = lock(&h.worker).take() {
            let _ = j.join();
        }
    }

    /// Records where the observer started on each unit.
    fn note_tops(&self, tops: &[(UnitId, u64)]) {
        let mut units = lock(&self.units);
        for (u, top) in tops {
            let us = units.entry(*u).or_default();
            us.dispatched = us.dispatched.max(*top);
        }
    }
}

struct Observer(Weak<Dispatcher>);

impl ApplyObserver for Observer {
    fn applied(&self, unit: UnitId, entries: &[LogEntry]) {
        if let Some(d) = self.0.upgrade() {
            d.dispatch(unit, entries);
        }
    }
}

static DISPATCHER: OnceLock<Arc<Dispatcher>> = OnceLock::new();

/// A dispatcher on `eng`, observing its applies: `recover` registers the
/// indexes on disk and says, per unit, the lowest entry they hold; the
/// observer replays each unit from there.
///
/// # Errors
/// The log no longer holds an entry the indexes need.
pub(crate) fn attach(
    eng: &sprig_engine::Engine,
    recover: impl FnOnce(&Arc<Dispatcher>) -> Vec<(UnitId, u64)>,
) -> Result<Arc<Dispatcher>> {
    let d = Arc::new(Dispatcher::new(eng.clone()));
    let lows = recover(&d);
    // Every unit the log knows, from where the indexes need it (or its
    // applied index): the observer then knows exactly what it was handed.
    let mut from: HashMap<UnitId, u64> = eng.units().into_iter().collect();
    for (u, low) in lows {
        let f = from.entry(u).or_insert(low);
        *f = (*f).min(low);
    }
    let from: Vec<(UnitId, u64)> = from.into_iter().collect();
    let tops = eng.observe_units(Arc::new(Observer(Arc::downgrade(&d))), &from)?;
    d.note_tops(&tops);
    Ok(d)
}

/// [`attach`] for the node's engine, once per process
/// (later calls give the same one).
///
/// # Errors
/// As [`attach`].
pub(crate) fn start(
    eng: &sprig_engine::Engine,
    recover: impl FnOnce(&Arc<Dispatcher>) -> Vec<(UnitId, u64)>,
) -> Result<&'static Arc<Dispatcher>> {
    if let Some(d) = DISPATCHER.get() {
        return Ok(d);
    }
    let d = attach(eng, recover)?;
    let _ = DISPATCHER.set(d);
    DISPATCHER
        .get()
        .ok_or_else(|| SprigError::internal("apply-time indexes: no dispatcher"))
}

struct Worker {
    d: Weak<Dispatcher>,
    h: Arc<Handle>,
    store: Arc<dyn IndexStore>,
    build: bool,
}

impl Worker {
    fn set(&self, f: impl FnOnce(&mut State)) {
        f(&mut lock(&self.h.state));
        self.h.changed.notify_all();
    }

    fn fail(&self, e: &SprigError) {
        tracing::error!(index = %self.h.name, error = %e, "apply-time index failed; it is rebuilt when next used");
        self.set(|s| s.phase = Phase::Failed(e.message.clone()));
    }

    /// Writes every row of the table as a fresh read finds it.
    fn build_rows(&self) -> Result<u64> {
        let Some(d) = self.d.upgrade() else {
            return Ok(0);
        };
        let hi = keys::prefix_end(&self.h.prefix).unwrap_or_else(|| vec![0xFF; 20]);
        let mut lo = self.h.prefix.clone();
        let mut n = 0u64;
        loop {
            let mut tx = Tx::begin(&d.eng, TxnOptions::default())?;
            let batch = tx.scan(&lo, &hi, false, BUILD_BATCH)?;
            drop(tx);
            for (k, v) in &batch {
                n += 1;
                self.store.upsert(&k[keys::PREFIX_LEN..], v, n)?;
            }
            if n % 65_536 < BUILD_BATCH as u64 {
                self.set(|s| s.built = n);
            }
            match batch.last() {
                Some((k, _)) if batch.len() == BUILD_BATCH => {
                    lo = keys::prefix_end(k).unwrap_or_else(|| hi.clone());
                }
                _ => break,
            }
        }
        Ok(n)
    }

    /// Raises the log floor to `applied`, which the files cover.
    fn floor(&self, applied: u64) {
        if let Some(d) = self.d.upgrade()
            && let Err(e) = d
                .eng
                .set_log_floors(&self.h.name, &[(self.h.unit, applied)])
        {
            tracing::warn!(index = %self.h.name, error = %e, "apply-time index: its log floor wasn't raised");
        }
    }

    /// Commits the files as covering `applied`.
    fn commit(&self, applied: u64) -> Result<()> {
        self.store.commit(applied)?;
        self.floor(applied);
        self.set(|s| s.committed = s.committed.max(applied));
        Ok(())
    }

    fn run(self, rx: &Receiver<Msg>) {
        let mut seen = self.h.start;
        if self.build {
            let r = self.build_rows().and_then(|n| {
                self.set(|s| s.built = n);
                self.commit(seen)
            });
            if let Err(e) = r {
                self.fail(&e);
                return;
            }
            self.set(|s| s.phase = Phase::Ready);
        }
        // Row changes since the last commit, and when the first came.
        let mut pending = 0u64;
        let mut first: Option<Instant> = None;
        // When the floor last moved without a commit.
        let mut floored = Instant::now();
        loop {
            let wait = first.map_or(FLOOR_EVERY, |t| COMMIT_EVERY.saturating_sub(t.elapsed()));
            let m = match rx.recv_timeout(wait) {
                Ok(m) => Some(m),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            let mut flush = false;
            let r = match m {
                Some(Msg::Put(k, v, seq)) => self.store.upsert(&k, &v, seq).map(|()| 1),
                Some(Msg::Del(k)) => self.store.delete(&k).map(|()| 1),
                Some(Msg::Clear) => self.store.clear().map(|()| 1),
                Some(Msg::Mark(i)) => {
                    seen = seen.max(i);
                    Ok(0)
                }
                Some(Msg::Flush(i)) => {
                    seen = seen.max(i);
                    flush = true;
                    Ok(0)
                }
                Some(Msg::Stop) => return,
                None => Ok(0),
            };
            match r {
                Ok(n) => {
                    if n > 0 {
                        pending += n;
                        first.get_or_insert_with(Instant::now);
                    }
                }
                Err(e) => {
                    self.fail(&e);
                    return;
                }
            }
            let due = flush
                || pending >= COMMIT_PENDING
                || first.is_some_and(|t| t.elapsed() >= COMMIT_EVERY);
            if !due {
                continue;
            }
            if pending > 0 {
                if let Err(e) = self.commit(seen) {
                    self.fail(&e);
                    return;
                }
                pending = 0;
                first = None;
                floored = Instant::now();
            } else if seen > self.h.committed() {
                // Nothing of this index changed since its last commit: its
                // files already cover `seen`. The floor follows now and
                // then (a restart replays from the higher of the files'
                // position and the floor).
                if flush || floored.elapsed() >= FLOOR_EVERY {
                    self.floor(seen);
                    floored = Instant::now();
                }
                self.set(|s| s.committed = s.committed.max(seen));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_order_log_changes_after_built_rows() {
        assert!(seq_of(1, 0) > 1_000_000_000);
        assert!(seq_of(2, 0) > seq_of(1, 5));
        assert!(seq_of(1, 6) > seq_of(1, 5));
    }

    #[test]
    fn payload_walk_sees_puts_and_range_deletes() {
        let mut p = sprig_engine::log::payload::Payload::new();
        p.put(Cf::Data, b"k1", b"v1");
        p.delete(Cf::Data, b"k2");
        p.delete_range(Cf::Data, b"a", b"z");
        p.note(b"note");
        let data = p.into_bytes();
        let mut seen = Vec::new();
        assert!(walk_payload(&data, |o| seen.push(match o {
            PayloadOp::Put(_, k, v) => format!("put {k:?} {v:?}"),
            PayloadOp::DeleteRange(_, s, e) => format!("range {s:?} {e:?}"),
            PayloadOp::Other => "other".into(),
        })));
        assert_eq!(seen.len(), 4);
        assert!(seen[0].starts_with("put"));
        assert!(seen[2].starts_with("range"));
    }
}
