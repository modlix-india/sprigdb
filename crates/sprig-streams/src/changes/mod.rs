//! The change log: every unit's committed changes, written by an apply
//! observer as segment files beside the store, read by change consumers
//! (MongoDB change streams first).
//!
//! ```text
//! <dir>/manifest.json                  what is durable, per unit
//! <dir>/u<unit, 20 digits>/<first entry index, 20 digits>.chg
//! ```
//!
//! **Writing.** On the apply step the observer turns each log entry's
//! versioned puts of primary records and metadata (key kinds `P` and `M`,
//! not engine bookkeeping) into [`Change`] records, in the entry's order,
//! and appends them to the unit's active segment, unsynced. The commit
//! log is the change log's write-ahead log: the flusher syncs the
//! segments, writes the manifest (lengths and each unit's last entry) and
//! then raises the engine's log floor `changes` for each unit, so the log
//! keeps every entry the files may lack. After a crash the segments are
//! cut back to the manifest, records of entries the commit log no longer
//! has are cut too, and the engine replays from the floor: the same
//! entries give the same records. Followers of a replicated unit run the
//! same observer on the same entries, so every replica writes the same
//! change log; a unit snapshot carries the unit's change log files.
//!
//! **Hints** ([`record::encode_hint`]) are payload notes a writer adds
//! beside a write, attached to the record of the same key.
//!
//! **Reading.** A [`Reader`] reads a unit's records after a [`Pos`], from
//! disk, in pages bounded by bytes, so a transaction of any size is never
//! held in memory. Its position is a resume token ([`Token`], version 2).
//! A reader holds MVCC versions from its position on (the engine's version
//! holds), so a consumer can read a change's before-image as the key's
//! version just below the change's commit timestamp
//! ([`ChangeLog::before_image`]).
//!
//! **Resolved timestamps.** Each unit's resolved timestamp is the highest
//! commit timestamp its change log holds; it never goes back (it is kept in
//! the manifest). On one node a unit's commits reach its log in timestamp
//! order, except a commit across units, whose part lands when it is
//! resolved.
//!
//! **Retention** drops whole segments older than `retention_ms`; a reader
//! whose position is below the oldest record kept gets "history lost".

pub mod record;

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::{self, KeyPrefix};
use sprig_engine::log::LogEntry;
use sprig_engine::{ApplyObserver, Cf, Engine, Ts, UnitId};

pub use self::record::{Change, HintOp, Pos, Token, decode_hint, encode_hint};

/// The engine's name for this log's floor.
pub const FLOOR: &str = "changes";
const MANIFEST: &str = "manifest.json";
/// Units first seen after the last manifest: `unit from` lines.
const UNITS: &str = "units.log";
/// Consumers' version holds that outlive a restart ([`ChangeLog::hold_for`]).
const HOLDS: &str = "holds.json";
const EXT: &str = "chg";

/// Settings.
#[derive(Debug, Clone)]
pub struct ChangeConfig {
    /// A unit's segment rolls past this many bytes (at an entry boundary).
    pub segment_bytes: u64,
    /// Bytes between sparse index entries.
    pub index_interval: u64,
    /// How often the flusher runs.
    pub flush_every: Duration,
    /// Rolled segments older than this go (`feed.retention_hours`; ms,
    /// negative: never).
    pub retention_ms: i64,
    /// How often retention runs.
    pub maintain_every: Duration,
}

impl Default for ChangeConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            index_interval: 64 << 10,
            flush_every: Duration::from_secs(1),
            retention_ms: 168 * 3_600_000,
            maintain_every: Duration::from_secs(60),
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn io_err(what: &str, e: &io::Error) -> SprigError {
    SprigError::internal(format!("change log: {what}: {e}"))
}

/// "The change log no longer holds that place" (MongoDB's
/// `ChangeStreamHistoryLost`, 286, at the consumer).
#[must_use]
pub fn history_lost(unit: u64, pos: Pos) -> SprigError {
    SprigError::new(
        SprigCode::FeedHistoryLost,
        format!(
            "change log: unit {unit} no longer holds the changes after entry {} (retention, or a \
             gap after a restore)",
            pos.index
        ),
    )
}

/// Whether `e` is [`history_lost`].
#[must_use]
pub fn is_history_lost(e: &SprigError) -> bool {
    e.code() == Some(SprigCode::FeedHistoryLost)
}

/// Milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// One segment file.
#[derive(Debug)]
struct Seg {
    /// The first entry it may hold (its name).
    base: u64,
    path: PathBuf,
    file: Arc<File>,
    len: u64,
    synced: u64,
    created_ms: i64,
    /// Sparse index: a record's place and its file position.
    index: Vec<(Pos, u64)>,
    since: u64,
}

impl Seg {
    fn name(base: u64) -> String {
        format!("{base:020}.{EXT}")
    }

    fn create(dir: &Path, base: u64) -> io::Result<Seg> {
        fs::create_dir_all(dir)?;
        let path = dir.join(Self::name(base));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Seg {
            base,
            path,
            file: Arc::new(file),
            len: 0,
            synced: 0,
            created_ms: now_ms(),
            index: Vec::new(),
            since: u64::MAX,
        })
    }

    /// Opens a listed segment, cut back to `len`, and rebuilds its index
    /// (a torn record at the end is cut too).
    fn open(path: &Path, base: u64, len: u64, created_ms: i64, interval: u64) -> io::Result<Seg> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let have = file.metadata()?.len();
        let len = len.min(have);
        if have != len {
            file.set_len(len)?;
        }
        let mut index = Vec::new();
        let mut since = u64::MAX;
        let ok_to = scan(&file, 0, len, |p, at, n| {
            if since >= interval {
                index.push((p, at));
                since = 0;
            }
            since = since.saturating_add(n);
            true
        })?;
        if ok_to < len {
            file.set_len(ok_to)?;
        }
        Ok(Seg {
            base,
            path: path.to_path_buf(),
            file: Arc::new(file),
            len: ok_to,
            synced: ok_to,
            created_ms,
            index,
            since,
        })
    }

    /// The file position of the first record after `after` (or the end).
    fn seek(&self, after: Pos) -> io::Result<u64> {
        let start = match self.index.partition_point(|(p, _)| *p <= after) {
            0 => 0,
            i => self.index[i - 1].1,
        };
        scan(&self.file, start, self.len, |p, _, _| p <= after)
    }

    /// Cuts every record of an entry above `top`.
    fn cut_above(&mut self, top: u64) -> io::Result<()> {
        let at = self.seek(Pos::after_entry(top))?;
        if at < self.len {
            self.file.set_len(at)?;
            self.len = at;
            self.synced = self.synced.min(at);
            self.index.retain(|(_, p)| *p < at);
        }
        Ok(())
    }
}

/// Reads records from `from` to `to`, calling `f(pos, file position,
/// record bytes)` until it returns false. Returns where it stopped: at the
/// record `f` refused, at a torn or damaged record, or at `to`.
fn scan(
    file: &File,
    from: u64,
    to: u64,
    mut f: impl FnMut(Pos, u64, u64) -> bool,
) -> io::Result<u64> {
    let mut at = from;
    let mut buf: Vec<u8> = Vec::new();
    let mut buf_at = from;
    let load = |buf: &mut Vec<u8>, buf_at: &mut u64, at: u64, n: usize| -> io::Result<()> {
        if at >= *buf_at && at + n as u64 <= *buf_at + buf.len() as u64 {
            return Ok(());
        }
        let want = usize::try_from((to - at).min((n.max(256 << 10)) as u64)).unwrap_or(n);
        buf.resize(want, 0);
        file.read_exact_at(buf, at)?;
        *buf_at = at;
        Ok(())
    };
    loop {
        if at + record::HEAD as u64 > to {
            return Ok(at);
        }
        load(&mut buf, &mut buf_at, at, record::HEAD)?;
        let o = usize::try_from(at - buf_at).unwrap_or(0);
        let Some((len, crc)) = record::parse_head(&buf[o..]) else {
            return Ok(at);
        };
        let n = record::HEAD + len;
        if at + n as u64 > to {
            return Ok(at);
        }
        load(&mut buf, &mut buf_at, at, n)?;
        let o = usize::try_from(at - buf_at).unwrap_or(0);
        let body = &buf[o + record::HEAD..o + n];
        if !record::crc_ok(body, crc) {
            return Ok(at);
        }
        let Some((p, _)) = record::peek(body) else {
            return Ok(at);
        };
        if !f(p, at, n as u64) {
            return Ok(at);
        }
        at += n as u64;
    }
}

/// One unit's change log.
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)] // independent flags of one unit
struct UnitState {
    dir: PathBuf,
    segs: Vec<Seg>,
    /// The last entry seen (with or without records).
    applied: u64,
    /// The highest commit timestamp held (the resolved timestamp).
    max_cts: u64,
    /// Readers asking for places below this get "history lost".
    lost_before: u64,
    dirty: bool,
    /// A write failed: nothing more is taken until a restart replays.
    failed: bool,
    /// The engine holds a floor for this unit.
    floored: bool,
    /// First seen since the log opened (not in the manifest).
    fresh: bool,
}

impl UnitState {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            segs: Vec::new(),
            applied: 0,
            max_cts: 0,
            lost_before: 0,
            dirty: true,
            failed: false,
            floored: false,
            fresh: true,
        }
    }
}

type Unit = Arc<Mutex<UnitState>>;

struct Inner {
    engine: Engine,
    dir: PathBuf,
    cfg: ChangeConfig,
    units: RwLock<HashMap<u64, Unit>>,
    /// Each unit's last entry seen, for waiting readers.
    watches: Mutex<HashMap<u64, tokio::sync::watch::Sender<u64>>>,
    flushing: Mutex<()>,
    units_file: Mutex<()>,
    holds: Mutex<Holds>,
}

/// Durable consumers' version holds: by name, the commit timestamp whose
/// changes (and later) still need their before-images, and when it was
/// set. The engine holds versions from the lowest.
#[derive(Debug, Default)]
struct Holds {
    by: BTreeMap<String, (u64, i64)>,
    held: Option<u64>,
    written_ms: i64,
}

impl Holds {
    /// Reads the file, leaving out holds not renewed within `keep_ms`.
    fn load(dir: &Path, keep_ms: i64) -> Self {
        let now = now_ms();
        let by: BTreeMap<String, (u64, i64)> = fs::read(dir.join(HOLDS))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        Self {
            by: by
                .into_iter()
                .filter(|(_, (_, at))| now.saturating_sub(*at) < keep_ms)
                .collect(),
            held: None,
            written_ms: 0,
        }
    }

    /// Moves the engine's hold to the lowest timestamp kept.
    fn apply(&mut self, engine: &Engine) {
        let want = self.by.values().map(|(c, _)| c.saturating_sub(1)).min();
        if want == self.held {
            return;
        }
        let sh = engine.txn_shared();
        if let Some(w) = want {
            sh.hold_versions(Ts(w));
        }
        if let Some(old) = self.held {
            sh.release_versions(Ts(old));
        }
        self.held = want;
    }

    fn write(&mut self, dir: &Path) {
        let tmp = dir.join(format!("{HOLDS}.tmp"));
        let ok = serde_json::to_vec(&self.by).ok().is_some_and(|b| {
            fs::write(&tmp, b).is_ok() && fs::rename(&tmp, dir.join(HOLDS)).is_ok()
        });
        if ok {
            self.written_ms = now_ms();
        } else {
            tracing::warn!("change log: consumers' version holds not written");
        }
    }
}

/// The node's change log. Cheap to clone.
#[derive(Clone)]
pub struct ChangeLog {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ChangeLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeLog")
            .field("dir", &self.inner.dir)
            .finish_non_exhaustive()
    }
}

/// What the manifest keeps of a unit.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct UnitMeta {
    applied: u64,
    max_cts: u64,
    #[serde(default)]
    lost_before: u64,
    segs: Vec<SegMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegMeta {
    base: u64,
    len: u64,
    created_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Manifest {
    version: u32,
    units: BTreeMap<u64, UnitMeta>,
}

fn read_manifest(dir: &Path) -> Result<Option<Manifest>> {
    match fs::read(dir.join(MANIFEST)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| SprigError::internal(format!("change log: manifest is corrupt: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read manifest", &e)),
    }
}

fn sync_dir(p: &Path) -> io::Result<()> {
    match File::open(p) {
        Ok(f) => f.sync_all(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn write_manifest(dir: &Path, m: &Manifest) -> Result<()> {
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    let body = serde_json::to_vec(m)
        .map_err(|e| SprigError::internal(format!("change log: manifest: {e}")))?;
    {
        let mut f = File::create(&tmp).map_err(|e| io_err("write manifest", &e))?;
        f.write_all(&body)
            .and_then(|()| f.sync_all())
            .map_err(|e| io_err("write manifest", &e))?;
    }
    fs::rename(&tmp, dir.join(MANIFEST)).map_err(|e| io_err("write manifest", &e))?;
    sync_dir(dir).map_err(|e| io_err("sync dir", &e))
}

fn unit_dir(dir: &Path, unit: u64) -> PathBuf {
    dir.join(format!("u{unit:020}"))
}

/// One operation of a log payload (`sprig_engine::log::payload`'s
/// format), borrowed.
enum PayloadOp<'a> {
    Put(u8, &'a [u8], &'a [u8]),
    Note(&'a [u8]),
    Other,
}

/// Walks a payload's operations without copying; false when malformed.
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
                    at += 1;
                    slice(data, &mut at)?;
                    slice(data, &mut at)?;
                    PayloadOp::Other
                }
                4 => PayloadOp::Note(slice(data, &mut at)?),
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

/// Calls `f` with each change of one log entry, in order, without
/// holding the entry's changes in memory. `f` sees `last` set on the last.
/// The entry's event notes come after its changes, as records of their
/// own keys, at the entry's commit timestamp.
pub fn for_each_change(e: &LogEntry, f: impl FnMut(&Change)) {
    for_each_change_at(e, None, f);
}

/// [`for_each_change`]; an entry with event notes and no versioned write
/// (a command that changed nothing, such as `LTRIM l 0 -1`) gives its
/// events `fallback_cts` (the unit's highest commit timestamp so far, the
/// same on every replica), or none without one.
fn for_each_change_at(e: &LogEntry, fallback_cts: Option<u64>, mut f: impl FnMut(&Change)) {
    let data_cf = Cf::Data.code();
    let mut hints: HashMap<&[u8], (HintOp, &[u8])> = HashMap::new();
    let mut events: Vec<(&[u8], &[u8])> = Vec::new();
    let mut entry_ts = None;
    let ok = walk_payload(&e.data, |op| match op {
        PayloadOp::Note(n) => {
            if let Some((k, op, d)) = record::decode_hint(n) {
                hints.insert(k, (op, d));
            } else if let Some(kv) = sprig_engine::observe::event::decode(n) {
                events.push(kv);
            }
        }
        PayloadOp::Put(cf, key, _) if entry_ts.is_none() => {
            if cf == data_cf
                && key.len() >= keys::PREFIX_LEN + keys::VERSION_LEN
                && !keys::is_unversioned(key)
                && let Some((_, ts)) = keys::split_version(key)
            {
                entry_ts = Some(ts.0);
            }
        }
        _ => {}
    });
    if !ok {
        return;
    }
    let mut held: Option<Change> = None;
    let mut seq = 0u32;
    walk_payload(&e.data, |op| {
        let PayloadOp::Put(cf, key, value) = op else {
            return;
        };
        if cf != data_cf
            || key.len() < keys::PREFIX_LEN + keys::VERSION_LEN
            || keys::is_unversioned(key)
        {
            return;
        }
        let Some((logical, ts)) = keys::split_version(key) else {
            return;
        };
        let Some((p, _)) = KeyPrefix::decode(logical) else {
            return;
        };
        // Redis databases' entries aren't changes anyone reads: their
        // keyspace events are recorded as event notes instead.
        if p.tenant == keys::META_TENANT
            || (p.kind != keys::kind::PRIMARY && p.kind != keys::kind::META)
            || keys::REDIS_OBJECTS.contains(&p.object)
        {
            return;
        }
        if let Some(h) = held.take() {
            f(&h);
        }
        held = Some(Change {
            index: e.index,
            seq,
            cts: ts.0,
            last: false,
            key: logical.to_vec(),
            value: match value.split_first() {
                Some((&keys::value::PUT, v)) => Some(v.to_vec()),
                _ => None,
            },
            hint: hints.get(logical).map(|(op, d)| (*op, d.to_vec())),
        });
        seq = seq.saturating_add(1);
    });
    // Event notes take the entry's commit timestamp, or the fallback when
    // the entry wrote nothing versioned.
    if let Some(cts) = entry_ts.or(fallback_cts) {
        for (key, value) in events {
            if let Some(h) = held.take() {
                f(&h);
            }
            held = Some(Change {
                index: e.index,
                seq,
                cts,
                last: false,
                key: key.to_vec(),
                value: Some(value.to_vec()),
                hint: None,
            });
            seq = seq.saturating_add(1);
        }
    }
    if let Some(mut h) = held {
        h.last = true;
        f(&h);
    }
}

/// The changes of one log entry (tests and small entries).
#[must_use]
pub fn changes_of(e: &LogEntry) -> Vec<Change> {
    let mut out = Vec::new();
    for_each_change(e, |c| out.push(c.clone()));
    out
}

struct Applier(Weak<Inner>);

impl ApplyObserver for Applier {
    fn applied(&self, unit: UnitId, entries: &[LogEntry]) {
        if let Some(inner) = self.0.upgrade() {
            inner.apply(unit.0, entries);
        }
    }
}

impl Inner {
    fn unit(&self, unit: u64) -> Unit {
        if let Some(u) = self
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
        {
            return Arc::clone(u);
        }
        let mut w = self.units.write().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            w.entry(unit)
                .or_insert_with(|| Arc::new(Mutex::new(UnitState::new(unit_dir(&self.dir, unit))))),
        )
    }

    fn existing(&self, unit: u64) -> Option<Unit> {
        self.units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .cloned()
    }

    fn apply(&self, unit: u64, entries: &[LogEntry]) {
        let u = self.unit(unit);
        let applied = {
            let mut st = lock(&u);
            if st.failed {
                return;
            }
            let mut buf = Vec::new();
            for e in entries {
                if e.index <= st.applied {
                    continue;
                }
                if st.fresh {
                    // History before the log first saw the unit isn't here;
                    // from here on the commit log keeps what the files may
                    // lack, even before the unit's first manifest.
                    // A unit the log didn't know when it opened: every
                    // entry of it comes here (the first may be later than
                    // 1 on a replicated unit, whose first entries carry no
                    // payload), so nothing of it is lost.
                    st.fresh = false;
                    st.lost_before = 0;
                    if let Err(err) = self.note_unit(unit, e.index - 1) {
                        tracing::error!(unit, error = %err,
                            "change log: recording a new unit failed; the unit's change log stops until a restart");
                        st.failed = true;
                        return;
                    }
                    if !st.floored {
                        match self
                            .engine
                            .set_log_floors(FLOOR, &[(UnitId(unit), e.index - 1)])
                        {
                            Ok(()) => st.floored = true,
                            Err(err) => {
                                tracing::warn!(unit, error = %err, "change log: setting a new unit's floor failed");
                            }
                        }
                    }
                }
                if let Err(err) = self.append(&mut st, e, &mut buf) {
                    tracing::error!(unit, index = e.index, error = %err,
                        "change log: a write failed; the unit's change log stops until a restart");
                    st.failed = true;
                    return;
                }
                st.applied = e.index;
                st.dirty = true;
            }
            st.applied
        };
        let w = lock(&self.watches);
        if let Some(tx) = w.get(&unit) {
            tx.send_replace(applied);
        }
    }

    /// Records, synced, that the log took `unit` from after entry `from`,
    /// before any of its records: a crash before the unit's first manifest
    /// then replays it from there.
    fn note_unit(&self, unit: u64, from: u64) -> io::Result<()> {
        self.note_units(&[(unit, from, 0)])
    }

    /// Appends `unit from lost_before` lines, synced.
    fn note_units(&self, units: &[(u64, u64, u64)]) -> io::Result<()> {
        if units.is_empty() {
            return Ok(());
        }
        let _one = lock(&self.units_file);
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(UNITS))?;
        let mut b = String::new();
        for (u, from, lost) in units {
            b.push_str(&format!("{u} {from} {lost}\n"));
        }
        f.write_all(b.as_bytes())?;
        f.sync_data()
    }

    /// Writes the changes of entry `e`, in 1 MiB writes.
    fn append(&self, st: &mut UnitState, e: &LogEntry, buf: &mut Vec<u8>) -> io::Result<()> {
        const WRITE: usize = 1 << 20;
        let interval = self.cfg.index_interval;
        let roll = st
            .segs
            .last()
            .is_none_or(|s| s.len >= self.cfg.segment_bytes);
        let mut created = false;
        let mut res: io::Result<()> = Ok(());
        let mut cts = 0;
        buf.clear();
        let dir = st.dir.clone();
        let fallback = Some(st.max_cts);
        let segs = &mut st.segs;
        for_each_change_at(e, fallback, |r| {
            if res.is_err() {
                return;
            }
            if roll && !created {
                match Seg::create(&dir, e.index) {
                    Ok(s) => segs.push(s),
                    Err(x) => {
                        res = Err(x);
                        return;
                    }
                }
                created = true;
            }
            let seg = segs.last_mut().expect("a segment");
            if seg.since >= interval {
                seg.index.push((r.pos(), seg.len + buf.len() as u64));
                seg.since = 0;
            }
            let before = buf.len();
            r.encode_into(buf);
            seg.since += (buf.len() - before) as u64;
            cts = cts.max(r.cts);
            if buf.len() >= WRITE {
                res = seg.file.write_all_at(buf, seg.len);
                seg.len += buf.len() as u64;
                buf.clear();
            }
        });
        res?;
        if let Some(seg) = st.segs.last_mut()
            && !buf.is_empty()
        {
            seg.file.write_all_at(buf, seg.len)?;
            seg.len += buf.len() as u64;
            buf.clear();
        }
        if buf.capacity() > 2 * WRITE {
            *buf = Vec::new();
        }
        st.max_cts = st.max_cts.max(cts);
        Ok(())
    }
}

impl ChangeLog {
    /// Opens (or creates) the change log in `dir` and registers it with
    /// the engine, which hands it every entry above its floors.
    ///
    /// # Errors
    /// The files or the engine's log can't be read.
    #[allow(clippy::too_many_lines)] // recovery, step by step
    pub fn open(engine: &Engine, dir: impl Into<PathBuf>, cfg: ChangeConfig) -> Result<ChangeLog> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| io_err("create dir", &e))?;
        let manifest = read_manifest(&dir)?.unwrap_or_default();
        // Held before anything reads or collects versions.
        let mut holds = Holds::load(&dir, cfg.retention_ms);
        holds.apply(engine);
        let inner = Arc::new(Inner {
            engine: engine.clone(),
            dir: dir.clone(),
            cfg,
            units: RwLock::new(HashMap::new()),
            watches: Mutex::new(HashMap::new()),
            flushing: Mutex::new(()),
            units_file: Mutex::new(()),
            holds: Mutex::new(holds),
        });
        // Units first seen after the last manifest, from where they start.
        let mut seen: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
        if let Ok(b) = fs::read_to_string(dir.join(UNITS)) {
            for l in b.lines() {
                let mut it = l.split(' ').map(str::parse::<u64>);
                if let (Some(Ok(u)), Some(Ok(f))) = (it.next(), it.next()) {
                    let lost = match it.next() {
                        Some(Ok(x)) => x,
                        _ => f + 1,
                    };
                    seen.entry(u).or_insert((f, lost));
                }
            }
        }
        // Files the manifest doesn't list are from after its last write.
        if let Ok(rd) = fs::read_dir(&dir) {
            for d in rd.flatten() {
                let name = d.file_name();
                let Some(u) = name
                    .to_str()
                    .and_then(|n| n.strip_prefix('u'))
                    .and_then(|n| n.parse::<u64>().ok())
                else {
                    continue;
                };
                if !manifest.units.contains_key(&u) {
                    let _ = fs::remove_dir_all(d.path());
                }
            }
        }
        let mut from = Vec::new();
        let floors: HashMap<u64, u64> = engine
            .log_floors(FLOOR)
            .into_iter()
            .map(|(u, f)| (u.0, f))
            .collect();
        // Units the log first saw after the last manifest still have the
        // floor it set when it first saw them.
        let mut units = manifest.units.clone();
        let floor_only = floors.iter().map(|(u, f)| (*u, (*f, f + 1)));
        for (u, (f, lost)) in seen.clone().into_iter().chain(floor_only) {
            units.entry(u).or_insert_with(|| UnitMeta {
                applied: f,
                max_cts: 0,
                lost_before: lost,
                segs: Vec::new(),
            });
        }
        // Units the engine holds that the log never saw: from their
        // applied index on (what came before isn't in the log). Recorded
        // before the log reads them, so a crash before the first manifest
        // keeps that boundary.
        let mut first_seen = Vec::new();
        for (u, a) in engine.units() {
            if let std::collections::btree_map::Entry::Vacant(v) = units.entry(u.0) {
                let lost = if a == 0 { 0 } else { a + 1 };
                first_seen.push((u.0, a, lost));
                v.insert(UnitMeta {
                    applied: a,
                    max_cts: 0,
                    lost_before: lost,
                    segs: Vec::new(),
                });
            }
        }
        inner
            .note_units(&first_seen)
            .map_err(|e| io_err("record units", &e))?;
        for (u, m) in &units {
            let mut st = UnitState::new(unit_dir(&dir, *u));
            st.fresh = false;
            st.max_cts = m.max_cts;
            st.lost_before = m.lost_before;
            let listed: std::collections::BTreeSet<u64> = m.segs.iter().map(|s| s.base).collect();
            if let Ok(rd) = fs::read_dir(&st.dir) {
                for d in rd.flatten() {
                    let base = d
                        .file_name()
                        .to_str()
                        .and_then(|n| n.strip_suffix(".chg"))
                        .and_then(|n| n.parse::<u64>().ok());
                    if base.is_none_or(|b| !listed.contains(&b)) {
                        let _ = fs::remove_file(d.path());
                    }
                }
            }
            for sm in &m.segs {
                let p = st.dir.join(Seg::name(sm.base));
                let s = Seg::open(&p, sm.base, sm.len, sm.created_ms, inner.cfg.index_interval)
                    .map_err(|e| io_err("open segment", &e))?;
                st.segs.push(s);
            }
            // Records of entries the commit log lost (a relaxed commit's
            // tail) go: the log may give those indexes to other entries.
            let unit = UnitId(*u);
            let top = engine.last_applied_of(unit);
            let mut applied = m.applied.min(top);
            if top < m.applied {
                for s in &mut st.segs {
                    s.cut_above(top).map_err(|e| io_err("cut segment", &e))?;
                }
            }
            // The log was trimmed past what the files hold: a gap.
            if applied < top
                && engine
                    .log_first_index_of(unit)
                    .is_none_or(|f| f > applied + 1)
            {
                let held = engine
                    .log_first_index_of(unit)
                    .map_or(top, |f| f - 1)
                    .min(top);
                tracing::warn!(
                    unit = *u,
                    floor = applied,
                    resumed_at = held,
                    "change log: the commit log no longer holds the entries above the change log's floor; \
                     readers before the gap get history lost"
                );
                st.lost_before = held + 1;
                applied = held;
            }
            st.applied = applied;
            st.floored = floors.contains_key(u);
            from.push((unit, applied));
            inner
                .units
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(*u, Arc::new(Mutex::new(st)));
        }
        let tops = engine.observe_units(
            Arc::new(Applier(Arc::downgrade(&inner))) as Arc<dyn ApplyObserver>,
            &from,
        )?;
        for (u, top) in tops {
            if let Some(st) = inner.existing(u.0) {
                let mut st = lock(&st);
                st.applied = st.applied.max(top);
            }
        }
        engine.add_snapshot_part(Arc::new(SnapshotPart(Arc::downgrade(&inner))));
        let log = ChangeLog { inner };
        log.flush()?;
        Ok(log)
    }

    /// The engine.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Syncs what was written since the last pass, writes the manifest and
    /// raises the engine's floors.
    ///
    /// # Errors
    /// A sync or the manifest write failed (the next pass retries).
    #[allow(clippy::too_many_lines)] // sync, manifest, floors, in order
    pub fn flush(&self) -> Result<()> {
        let _one = lock(&self.inner.flushing);
        let units: Vec<(u64, Unit)> = self
            .inner
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect();
        let mut m = Manifest {
            version: 1,
            units: BTreeMap::new(),
        };
        let mut files: Vec<Arc<File>> = Vec::new();
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut taken: Vec<(Unit, Vec<(u64, u64)>)> = Vec::new();
        let mut floors: Vec<(UnitId, u64)> = Vec::new();
        let mut any = false;
        for (u, st) in &units {
            let mut s = lock(st);
            if s.dirty || !s.floored {
                any = true;
            }
            let mut synced = Vec::new();
            let dir = s.dir.clone();
            for seg in &mut s.segs {
                if seg.synced < seg.len {
                    files.push(Arc::clone(&seg.file));
                    if seg.synced == 0 {
                        dirs.push(dir.clone());
                    }
                    synced.push((seg.base, seg.len));
                }
            }
            s.dirty = false;
            m.units.insert(
                *u,
                UnitMeta {
                    applied: s.applied,
                    max_cts: s.max_cts,
                    lost_before: s.lost_before,
                    segs: s
                        .segs
                        .iter()
                        .map(|g| SegMeta {
                            base: g.base,
                            len: g.len,
                            created_ms: g.created_ms,
                        })
                        .collect(),
                },
            );
            let durable = self.inner.engine.consensus().durable_index(UnitId(*u));
            floors.push((UnitId(*u), s.applied.min(durable)));
            taken.push((Arc::clone(st), synced));
        }
        let e = &self.inner.engine;
        let raise = |floors: Vec<(UnitId, u64)>| {
            let raise: Vec<(UnitId, u64)> = floors
                .into_iter()
                .filter(|(u, f)| e.log_floor_of(*u, FLOOR).is_none_or(|x| x < *f))
                .collect();
            e.set_log_floors(FLOOR, &raise)
        };
        if !any && files.is_empty() {
            return raise(floors);
        }
        let restore = |taken: &[(Unit, Vec<(u64, u64)>)]| {
            for (st, _) in taken {
                lock(st).dirty = true;
            }
        };
        let r = (|| -> io::Result<()> {
            for f in &files {
                f.sync_data()?;
            }
            dirs.sort();
            dirs.dedup();
            for d in &dirs {
                sync_dir(d)?;
            }
            if !dirs.is_empty() {
                sync_dir(&self.inner.dir)?;
            }
            Ok(())
        })();
        if let Err(e) = r {
            restore(&taken);
            return Err(io_err("sync", &e));
        }
        {
            // Units seen meanwhile are in this manifest: the list can go,
            // unless a unit was first seen after the snapshot above.
            let _one = lock(&self.inner.units_file);
            if let Err(e) = write_manifest(&self.inner.dir, &m) {
                restore(&taken);
                return Err(e);
            }
            let all_in = fs::read_to_string(self.inner.dir.join(UNITS)).map_or(true, |b| {
                b.lines().all(|l| {
                    l.split(' ')
                        .next()
                        .and_then(|u| u.parse::<u64>().ok())
                        .is_some_and(|u| m.units.contains_key(&u))
                })
            });
            if all_in {
                let _ = fs::remove_file(self.inner.dir.join(UNITS));
            }
        }
        for (st, synced) in &taken {
            let mut s = lock(st);
            for (base, len) in synced {
                if let Some(g) = s.segs.iter_mut().find(|g| g.base == *base) {
                    g.synced = g.synced.max(*len);
                }
            }
            s.floored = true;
        }
        raise(floors)
    }

    /// Drops whole segments past retention (never a unit's last one).
    ///
    /// # Errors
    /// The manifest could not be written.
    pub fn maintain(&self) -> Result<()> {
        let keep = self.inner.cfg.retention_ms;
        if keep < 0 {
            return Ok(());
        }
        let cut = now_ms() - keep;
        let units: Vec<Unit> = self
            .inner
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        let mut gone = Vec::new();
        for st in units {
            let mut s = lock(&st);
            while s.segs.len() > 1 && s.segs[1].created_ms < cut {
                let g = s.segs.remove(0);
                s.lost_before = s.lost_before.max(s.segs[0].base);
                s.dirty = true;
                gone.push(g.path);
            }
        }
        if gone.is_empty() {
            return Ok(());
        }
        self.flush()?;
        for p in gone {
            let _ = fs::remove_file(p);
        }
        Ok(())
    }

    /// Runs the flusher and retention on a thread of their own, until the
    /// log is dropped.
    pub fn spawn_background(&self) {
        let w = Arc::downgrade(&self.inner);
        let every = self.inner.cfg.flush_every;
        let maintain = self.inner.cfg.maintain_every;
        let spawned = std::thread::Builder::new()
            .name("change-log-flush".into())
            .spawn(move || {
                let mut last = std::time::Instant::now();
                loop {
                    std::thread::sleep(every);
                    let Some(inner) = w.upgrade() else { return };
                    let log = ChangeLog { inner };
                    let r = if last.elapsed() >= maintain {
                        last = std::time::Instant::now();
                        log.maintain().and_then(|()| log.flush())
                    } else {
                        log.flush()
                    };
                    if let Err(e) = r {
                        tracing::warn!(error = %e, "change log flush failed");
                    }
                }
            });
        if let Err(e) = spawned {
            tracing::error!(error = %e, "change log: the flusher thread did not start");
        }
    }

    /// The unit's newest place: after its last entry seen, and its
    /// resolved timestamp.
    #[must_use]
    pub fn head(&self, unit: u64) -> (Pos, u64) {
        match self.inner.existing(unit) {
            Some(st) => {
                let s = lock(&st);
                (Pos::after_entry(s.applied), s.max_cts)
            }
            None => (
                Pos::after_entry(self.inner.engine.last_applied_of(UnitId(unit))),
                0,
            ),
        }
    }

    /// The oldest place a reader of `unit` may start from: before the
    /// oldest change the log still holds.
    #[must_use]
    pub fn oldest(&self, unit: u64) -> Pos {
        match self.inner.existing(unit) {
            Some(st) => {
                let s = lock(&st);
                match s.lost_before {
                    0 | 1 => Pos::default(),
                    n => Pos::after_entry(n - 1),
                }
            }
            None => Pos::after_entry(self.inner.engine.last_applied_of(UnitId(unit))),
        }
    }

    /// A place to start reading `unit` from so that every change with a
    /// commit timestamp at or above `cts` comes after it (readers leave
    /// out the earlier ones): before the segment ahead of the last segment
    /// whose first change is older than `cts` (one segment of margin for
    /// the part of a commit across units, which lands in the log later
    /// than commits with higher timestamps), or [`ChangeLog::oldest`].
    #[must_use]
    pub fn place_at(&self, unit: u64, cts: u64) -> Pos {
        let oldest = self.oldest(unit);
        let Some(st) = self.inner.existing(unit) else {
            return oldest;
        };
        let firsts: Vec<(u64, Option<u64>)> = {
            let s = lock(&st);
            s.segs
                .iter()
                .map(|g| {
                    let mut head = [0u8; record::HEAD + 21];
                    let c = (g.len >= head.len() as u64)
                        .then(|| g.file.read_exact_at(&mut head, 0).ok())
                        .flatten()
                        .and_then(|()| record::peek(&head[record::HEAD..]))
                        .map(|(_, c)| c);
                    (g.base, c)
                })
                .collect()
        };
        let older = firsts.iter().rposition(|(_, c)| c.is_some_and(|c| c < cts));
        match older {
            Some(i) if i >= 1 => {
                let base = firsts[i - 1].0;
                oldest.max(Pos::after_entry(base.saturating_sub(1)))
            }
            _ => oldest,
        }
    }

    /// The unit's resolved timestamp: no change with a lower or equal
    /// commit timestamp will still come (on one node; see the module
    /// docs for commits across units). Never goes back.
    #[must_use]
    pub fn resolved(&self, unit: u64) -> u64 {
        self.head(unit).1
    }

    /// A receiver of the unit's last entry seen, changed on every apply.
    #[must_use]
    pub fn watch(&self, unit: u64) -> tokio::sync::watch::Receiver<u64> {
        let mut w = lock(&self.inner.watches);
        if let Some(tx) = w.get(&unit) {
            return tx.subscribe();
        }
        let now = self.head(unit).0.index;
        let (tx, rx) = tokio::sync::watch::channel(now);
        w.insert(unit, tx);
        rx
    }

    /// A reader of `unit` after `after`, holding MVCC versions from
    /// `hold_ts` on (the commit timestamp of `after`, or `None` for now).
    ///
    /// # Errors
    /// [`history_lost`] when the log no longer holds the changes after
    /// `after`.
    pub fn reader(&self, unit: u64, after: Pos, hold_ts: Option<u64>) -> Result<Reader> {
        if let Some(st) = self.inner.existing(unit) {
            let s = lock(&st);
            if after.index.saturating_add(1) < s.lost_before {
                return Err(history_lost(unit, after));
            }
        }
        let shared = self.inner.engine.txn_shared();
        let hold = Ts(hold_ts
            .unwrap_or_else(|| shared.visible_ts().0)
            .saturating_sub(1));
        shared.hold_versions(hold);
        Ok(Reader {
            log: self.clone(),
            unit,
            after,
            cursor: None,
            hold,
            last_cts: hold.0,
        })
    }

    /// Keeps the MVCC versions the changes from commit timestamp `cts` on
    /// need for their before-images, for consumer `name` (an after-commit
    /// trigger: the end of what it read), across restarts of the node;
    /// `None` forgets the consumer. Forward only; a hold not renewed within
    /// the log's retention is dropped at the next open. Written at most
    /// once a second (an older value on disk holds more, never less).
    pub fn hold_for(&self, name: &str, cts: Option<u64>) {
        let mut h = lock(&self.inner.holds);
        let now = now_ms();
        let changed = match cts {
            Some(c) => match h.by.get(name) {
                Some((old, _)) if *old >= c => false,
                had => {
                    let new = had.is_none();
                    h.by.insert(name.to_owned(), (c, now));
                    // A new consumer is written at once; a moved one at
                    // most once a second.
                    new || now.saturating_sub(h.written_ms) >= 1000
                }
            },
            None => h.by.remove(name).is_some(),
        };
        h.apply(&self.inner.engine);
        if changed {
            h.write(&self.inner.dir);
        }
    }

    /// The consumers holding versions ([`ChangeLog::hold_for`]).
    #[must_use]
    pub fn held(&self) -> Vec<String> {
        lock(&self.inner.holds).by.keys().cloned().collect()
    }

    /// The version of `c`'s key just below `c` (its before-image), `None`
    /// when there was none (or it was garbage-collected). Its reader must
    /// still be open.
    ///
    /// # Errors
    /// Store failures.
    pub fn before_image(&self, c: &Change) -> Result<Option<Vec<u8>>> {
        let store = self.inner.engine.store();
        let v =
            sprig_engine::txn::mvcc::read_at(store.as_ref(), &c.key, Ts(c.cts.saturating_sub(1)))?;
        Ok(v.and_then(|v| v.value.map(|b| b.to_vec())))
    }
}

/// A reader of one unit's change log; see [`ChangeLog::reader`].
pub struct Reader {
    log: ChangeLog,
    unit: u64,
    after: Pos,
    /// The segment base and file position of the next record.
    cursor: Option<(u64, u64)>,
    hold: Ts,
    last_cts: u64,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("unit", &self.unit)
            .field("after", &self.after)
            .finish_non_exhaustive()
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.log
            .inner
            .engine
            .txn_shared()
            .release_versions(self.hold);
    }
}

/// What one [`Reader::read`] gives.
#[derive(Debug, Default)]
pub struct Page {
    /// Changes in log order.
    pub changes: Vec<Change>,
    /// The reader's place after the page: past every change given, and,
    /// when it reached the end, past every entry seen.
    pub pos: Pos,
    /// The commit timestamp of that place (the resolved timestamp when it
    /// reached the end).
    pub cts: u64,
    /// It read to the end of what the log holds.
    pub at_end: bool,
}

impl Reader {
    /// The unit.
    #[must_use]
    pub fn unit(&self) -> u64 {
        self.unit
    }

    /// Its place.
    #[must_use]
    pub fn pos(&self) -> Pos {
        self.after
    }

    /// The next changes after its place, up to about `max_bytes` (at least
    /// one change when there is one), from disk.
    ///
    /// # Errors
    /// [`history_lost`] when retention dropped its place; read failures.
    #[allow(clippy::too_many_lines)] // one pass over the pages of a read
    pub fn read(&mut self, max_bytes: usize) -> Result<Page> {
        // Before-images of the last page are no longer needed.
        let want = Ts(self.last_cts.saturating_sub(1));
        if want > self.hold {
            let sh = self.log.inner.engine.txn_shared();
            sh.hold_versions(want);
            sh.release_versions(self.hold);
            self.hold = want;
        }
        // The log hasn't seen the unit yet: nothing to read, and the place
        // stays (the unit's first entry may be applied and not yet seen).
        let Some(st) = self.log.inner.existing(self.unit) else {
            return Ok(Page {
                changes: Vec::new(),
                pos: self.after,
                cts: 0,
                at_end: true,
            });
        };
        // What is readable now, and where the reader is.
        let (segs, applied, max_cts) = {
            let s = lock(&st);
            if s.failed {
                return Err(SprigError::internal(
                    "change log: a write failed on this node; it resumes after a restart",
                ));
            }
            if self.after.index.saturating_add(1) < s.lost_before {
                return Err(history_lost(self.unit, self.after));
            }
            let mut out: Vec<(u64, Arc<File>, u64, u64)> = Vec::new();
            match self.cursor {
                Some((base, at)) if s.segs.iter().any(|g| g.base == base) => {
                    for g in s.segs.iter().filter(|g| g.base >= base) {
                        let from = if g.base == base { at } else { 0 };
                        out.push((g.base, Arc::clone(&g.file), from, g.len));
                    }
                }
                _ => {
                    let i = s
                        .segs
                        .partition_point(|g| g.base <= self.after.index)
                        .saturating_sub(1);
                    for (n, g) in s.segs.iter().enumerate().skip(i) {
                        let from = if n == i {
                            g.seek(self.after).map_err(|e| io_err("seek", &e))?
                        } else {
                            0
                        };
                        out.push((g.base, Arc::clone(&g.file), from, g.len));
                    }
                }
            }
            (out, s.applied, s.max_cts)
        };
        let mut page = Page::default();
        let mut bytes = 0usize;
        let mut full = false;
        for (base, file, from, len) in segs {
            let mut at = from;
            let mut buf = Vec::new();
            while at < len && !full {
                let want = usize::try_from((len - at).min(1 << 20)).unwrap_or(1 << 20);
                buf.resize(want, 0);
                file.read_exact_at(&mut buf, at)
                    .map_err(|e| io_err("read", &e))?;
                let mut off = 0usize;
                while off + record::HEAD <= buf.len() {
                    let Some((n, crc)) = record::parse_head(&buf[off..]) else {
                        break;
                    };
                    if off + record::HEAD + n > buf.len() {
                        if off == 0 {
                            // One record bigger than the window.
                            let mut big = vec![0u8; record::HEAD + n];
                            file.read_exact_at(&mut big, at)
                                .map_err(|e| io_err("read", &e))?;
                            buf = big;
                            continue;
                        }
                        break;
                    }
                    let body = &buf[off + record::HEAD..off + record::HEAD + n];
                    if !record::crc_ok(body, crc) {
                        return Err(SprigError::internal(format!(
                            "change log: unit {} has a damaged record at {base}:{}",
                            self.unit,
                            at + off as u64
                        )));
                    }
                    let c = Change::decode_body(body)
                        .ok_or_else(|| SprigError::internal("change log: a record is malformed"))?;
                    off += record::HEAD + n;
                    if c.pos() <= self.after {
                        continue;
                    }
                    bytes += record::HEAD + n;
                    self.after = c.pos();
                    if c.last {
                        self.after = Pos::after_entry(c.index);
                    }
                    page.cts = c.cts;
                    self.last_cts = self.last_cts.max(c.cts);
                    page.changes.push(c);
                    if bytes >= max_bytes {
                        full = true;
                        break;
                    }
                }
                at += off as u64;
                self.cursor = Some((base, at));
                if off == 0 {
                    break;
                }
            }
            if full {
                break;
            }
        }
        if !full {
            page.at_end = true;
            self.after = self.after.max(Pos::after_entry(applied));
            page.cts = page.cts.max(max_cts);
        }
        page.pos = self.after;
        Ok(page)
    }
}

/// The change log's part of a unit snapshot: the unit's segment files.
struct SnapshotPart(Weak<Inner>);

impl sprig_engine::snapshot::UnitSnapshotPart for SnapshotPart {
    fn name(&self) -> &'static str {
        "changes"
    }

    fn export(&self, unit: UnitId, _index: u64, out: &mut dyn io::Write) -> Result<()> {
        let Some(inner) = self.0.upgrade() else {
            return Ok(());
        };
        let Some(st) = inner.existing(unit.0) else {
            out.write_all(&0u64.to_be_bytes())
                .map_err(|e| io_err("snapshot", &e))?;
            return Ok(());
        };
        let s = lock(&st);
        let w = |out: &mut dyn io::Write, b: &[u8]| {
            out.write_all(b).map_err(|e| io_err("snapshot", &e))
        };
        w(out, &(s.segs.len() as u64).to_be_bytes())?;
        w(out, &s.applied.to_be_bytes())?;
        w(out, &s.max_cts.to_be_bytes())?;
        w(out, &s.lost_before.to_be_bytes())?;
        for g in &s.segs {
            w(out, &g.base.to_be_bytes())?;
            w(out, &g.len.to_be_bytes())?;
            w(out, &g.created_ms.to_be_bytes())?;
            let mut at = 0u64;
            let mut buf = vec![0u8; 1 << 20];
            while at < g.len {
                let n = usize::try_from((g.len - at).min(buf.len() as u64)).unwrap_or(0);
                g.file
                    .read_exact_at(&mut buf[..n], at)
                    .map_err(|e| io_err("snapshot", &e))?;
                w(out, &buf[..n])?;
                at += n as u64;
            }
        }
        Ok(())
    }

    fn ingest(&self, unit: UnitId, index: u64, input: &mut dyn io::Read) -> Result<()> {
        let Some(inner) = self.0.upgrade() else {
            return Ok(());
        };
        let u64_of = |input: &mut dyn io::Read| -> Result<u64> {
            let mut b = [0u8; 8];
            input
                .read_exact(&mut b)
                .map_err(|e| io_err("snapshot", &e))?;
            Ok(u64::from_be_bytes(b))
        };
        let n = u64_of(input)?;
        let st = inner.unit(unit.0);
        let mut s = lock(&st);
        let _ = fs::remove_dir_all(&s.dir);
        s.segs.clear();
        s.fresh = false;
        s.failed = false;
        s.dirty = true;
        if n == 0 {
            s.applied = index;
            s.lost_before = index + 1;
            return Ok(());
        }
        let applied = u64_of(input)?;
        s.max_cts = s.max_cts.max(u64_of(input)?);
        s.lost_before = u64_of(input)?;
        fs::create_dir_all(&s.dir).map_err(|e| io_err("snapshot", &e))?;
        for _ in 0..n {
            let base = u64_of(input)?;
            let len = u64_of(input)?;
            let created = i64::try_from(u64_of(input)?).unwrap_or(0);
            let p = s.dir.join(Seg::name(base));
            {
                let mut f = File::create(&p).map_err(|e| io_err("snapshot", &e))?;
                let mut left = len;
                let mut buf = vec![0u8; 1 << 20];
                while left > 0 {
                    let k = usize::try_from(left.min(buf.len() as u64)).unwrap_or(0);
                    input
                        .read_exact(&mut buf[..k])
                        .map_err(|e| io_err("snapshot", &e))?;
                    f.write_all(&buf[..k]).map_err(|e| io_err("snapshot", &e))?;
                    left -= k as u64;
                }
            }
            let mut g = Seg::open(&p, base, len, created, inner.cfg.index_interval)
                .map_err(|e| io_err("snapshot", &e))?;
            g.synced = 0;
            s.segs.push(g);
        }
        // Entries between the exporter's place and the snapshot's index
        // come again from the log only above `index`: anything the files
        // lack up to it is a gap.
        if applied < index {
            s.lost_before = index + 1;
        }
        s.applied = index.max(applied);
        let top = s.applied;
        if let Some(g) = s.segs.last_mut() {
            g.cut_above(top).map_err(|e| io_err("snapshot", &e))?;
        }
        Ok(())
    }
}

/// Reads the whole file (tests).
#[doc(hidden)]
pub fn read_all(path: &Path) -> io::Result<Vec<u8>> {
    let mut v = Vec::new();
    File::open(path)?.read_to_end(&mut v)?;
    Ok(v)
}

#[cfg(test)]
mod tests;
