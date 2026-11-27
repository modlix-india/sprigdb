//! Large notes written once, beside a replicated log rather than in it.
//!
//! A stream append rides in its log entry as a payload note of up to a few
//! MiB. Kept in the Raft log and then copied into the stream's segment
//! file, every byte reached each node's disk twice. With note blobs the log
//! store writes such a note once, into a blob file, and keeps only a
//! reference in the log ([`NoteBlobs::take_out`]); whoever reads the log
//! gets the note back ([`NoteBlobs::put_back`]). The stream log keeps a
//! reference to the same bytes in its segments instead of writing them
//! again ([`NoteBlobs::take_located`]).
//!
//! ```text
//! <dir>/<id, 16 hex digits>.blob   records, back to back
//! record := "SPNB" | crc32 u32 | len u32 | 0 u32 | unit u64 | index u64 | note
//! <dir>/<id>.sum                    a rolled file's units, each with its highest index
//! ```
//!
//! **Durability.** The log store writes a batch's blobs before the batch,
//! and when the batch is synced, syncs them alongside (both syncs done
//! before the write counts as persisted). Notes are taken out only of
//! synced writes. A crash between the two syncs can leave the log's last
//! write whole while the newest blob file lost its tail: those entries
//! were never reported persisted, so [`NoteBlobs::put_back`] answers
//! `UnexpectedEof` for them and the log store cuts them off, as it does a
//! torn write of its own. Anything else that doesn't read back is an error.
//! On macOS, where a sync (`F_FULLFSYNC`) flushes the whole drive cache, a
//! blob file is written with `O_DSYNC` instead (each write reaches the
//! drive) and the log's own full sync, in the same write, makes it durable:
//! one drive flush per write, not two. That needs the blob directory on
//! the log's drive (it sits beside it).
//!
//! **Removal** ([`NoteBlobs::collect`]). A blob file goes when it isn't the
//! one being written, the log holds none of its entries any more, and no
//! holder (the stream log's segments) refers to it. Until a holder has
//! registered in this process only files this process wrote may go: a
//! holder that opens later may still refer to older ones.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use super::SyncMode;
use super::fs::SyncGate;
use super::payload::{self, BLOB, FORMAT, NOTE};

/// Which notes go to blob files (the stream log's appends).
pub type NoteFilter = fn(&[u8]) -> bool;

const MAGIC: &[u8; 4] = b"SPNB";
/// A record's head.
const HEAD: usize = 32;
/// A reference in the log: file, position, length, crc.
const REF_LEN: usize = 24;
/// A blob file rolls past this many bytes.
const FILE_BYTES: u64 = 64 << 20;
/// Notes shorter than this stay in the log (a reference costs 24 bytes).
pub const MIN_NOTE: usize = 256;
/// Locations kept for the apply step at most (the oldest go first).
const LOCATED_MAX: usize = 1 << 20;
/// How often [`NoteBlobs::collect`] looks for files to remove.
const COLLECT_EVERY: Duration = Duration::from_secs(10);
/// `O_DSYNC` on macOS (see the module docs).
#[cfg(target_os = "macos")]
const O_DSYNC: i32 = 0x0040_0000;

/// How long a file this process finished stays at least: the log write of
/// its last entries may still be under way (a group whose log held nothing
/// yet looks as if it held none of them).
const SEALED_GRACE: Duration = Duration::from_secs(30);

/// Where a note is: in blob file `file`, `len` bytes from `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobLoc {
    /// The blob file's id.
    pub file: u64,
    /// Where the note starts in it.
    pub at: u64,
    /// Its length.
    pub len: u32,
}

/// One blob file, open for reading.
#[derive(Debug)]
pub struct BlobFile {
    id: u64,
    file: File,
}

impl BlobFile {
    /// Its id.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Fills `buf` from position `pos`.
    ///
    /// # Errors
    /// The file system's, or the file ends first.
    pub fn read_exact_at(&self, buf: &mut [u8], pos: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, pos)
    }
}

/// Whoever keeps references to blob files beyond the log (the stream log's
/// segments).
pub trait BlobHolder: Send + Sync {
    /// The ids of the blob files it refers to now.
    fn in_use(&self) -> BTreeSet<u64>;
}

/// The file being written.
struct Active {
    file: Arc<BlobFile>,
    /// Bytes in the file.
    len: u64,
    /// Records taken out since the last write, to go at `len`.
    pending: Vec<u8>,
    /// Which of its writes finished syncs cover.
    gate: Arc<SyncGate>,
}

/// What a file holds: per unit, its highest entry index.
#[derive(Debug, Default, Clone)]
struct Summary {
    units: BTreeMap<u64, u64>,
    /// Written by this process.
    this_run: bool,
    /// When this process finished writing it.
    sealed: Option<Instant>,
}

/// Locations of notes taken out or put back, by (unit, index): the entry's
/// term and its notes' (slot, location).
type Located = BTreeMap<(u64, u64), (u64, Vec<(u32, BlobLoc)>)>;

/// See the module docs.
pub struct NoteBlobs {
    dir: PathBuf,
    sync: SyncMode,
    filter: NoteFilter,
    take: AtomicBool,
    active: Mutex<Option<Active>>,
    files: RwLock<BTreeMap<u64, Arc<BlobFile>>>,
    sums: Mutex<BTreeMap<u64, Summary>>,
    located: Mutex<Located>,
    holder: Mutex<Option<Weak<dyn BlobHolder>>>,
    next_collect: Mutex<Option<Instant>>,
    grace: Mutex<Duration>,
    /// The newest file found at open and where its whole, checked records
    /// end: past that, a crash cut its unsynced tail.
    tail: Option<(u64, u64)>,
    /// Tests: called at the start of every sync of a blob file.
    #[cfg(test)]
    sync_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for NoteBlobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoteBlobs")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn crc32(b: &[u8]) -> u32 {
    let mut c = flate2::Crc::new();
    c.update(b);
    c.sum()
}

fn blob_name(id: u64) -> String {
    format!("{id:016x}.blob")
}

fn sum_name(id: u64) -> String {
    format!("{id:016x}.sum")
}

fn parse_id(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)
        .filter(|n| n.len() == 16)
        .and_then(|n| u64::from_str_radix(n, 16).ok())
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(a)
}

fn corrupt(what: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// The units and highest indexes of a file's records, read from their
/// heads (up to the first malformed or incomplete one), and where its whole
/// records end; with `check`, a record must also match its checksum.
fn scan(f: &File, check: bool) -> io::Result<(BTreeMap<u64, u64>, u64)> {
    let size = f.metadata()?.len();
    let mut units = BTreeMap::new();
    let mut pos = 0u64;
    let mut h = [0u8; HEAD];
    let mut body = Vec::new();
    while pos + HEAD as u64 <= size {
        f.read_exact_at(&mut h, pos)?;
        if &h[..4] != MAGIC {
            break;
        }
        let len = u64::from(u32_at(&h, 8));
        if pos + HEAD as u64 + len > size {
            break;
        }
        if check {
            body.resize(usize::try_from(len).unwrap_or(0), 0);
            f.read_exact_at(&mut body, pos + HEAD as u64)?;
            if crc32(&body) != u32_at(&h, 4) {
                break;
            }
        }
        let e = units.entry(u64_at(&h, 16)).or_insert(0);
        *e = (*e).max(u64_at(&h, 24));
        pos += HEAD as u64 + len;
    }
    Ok((units, pos))
}

fn read_sum(path: &Path) -> Option<BTreeMap<u64, u64>> {
    let b = fs::read(path).ok()?;
    if b.len() % 16 != 0 {
        return None;
    }
    Some(
        b.chunks_exact(16)
            .map(|c| (u64_at(c, 0), u64_at(c, 8)))
            .collect(),
    )
}

fn write_sum(path: &Path, units: &BTreeMap<u64, u64>) -> io::Result<()> {
    let mut b = Vec::with_capacity(units.len() * 16);
    for (u, i) in units {
        b.extend_from_slice(&u.to_le_bytes());
        b.extend_from_slice(&i.to_le_bytes());
    }
    let tmp = path.with_extension("sum.tmp");
    fs::write(&tmp, &b)?;
    fs::rename(&tmp, path)
}

impl NoteBlobs {
    /// Opens the blob files in `dir` (created if missing). `filter` picks
    /// the notes [`NoteBlobs::take_out`] moves into blob files; with `take`
    /// off nothing new is taken out (references already in the log are
    /// still put back).
    ///
    /// # Errors
    /// The directory or a file can't be read.
    pub fn open(
        dir: impl Into<PathBuf>,
        sync: SyncMode,
        filter: NoteFilter,
        take: bool,
    ) -> io::Result<Arc<NoteBlobs>> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut files = BTreeMap::new();
        let mut sums = BTreeMap::new();
        let mut sum_files = BTreeSet::new();
        for e in fs::read_dir(&dir)?.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(id) = parse_id(&name, ".blob") {
                let f = File::open(e.path())?;
                files.insert(id, Arc::new(BlobFile { id, file: f }));
            } else if let Some(id) = parse_id(&name, ".sum") {
                sum_files.insert(id);
            }
        }
        let newest = files.keys().next_back().copied();
        let mut tail = None;
        for (id, f) in &files {
            let units = if Some(*id) == newest {
                // The file being written at a crash: every record checked.
                let (u, end) = scan(&f.file, true)?;
                tail = Some((*id, end));
                u
            } else {
                match sum_files
                    .contains(id)
                    .then(|| read_sum(&dir.join(sum_name(*id))))
                    .flatten()
                {
                    Some(u) => u,
                    None => scan(&f.file, false)?.0,
                }
            };
            sums.insert(
                *id,
                Summary {
                    units,
                    this_run: false,
                    sealed: None,
                },
            );
        }
        for id in sum_files.difference(&files.keys().copied().collect()) {
            let _ = fs::remove_file(dir.join(sum_name(*id)));
        }
        Ok(Arc::new(NoteBlobs {
            dir,
            sync,
            filter,
            take: AtomicBool::new(take),
            active: Mutex::new(None),
            files: RwLock::new(files),
            sums: Mutex::new(sums),
            located: Mutex::new(BTreeMap::new()),
            holder: Mutex::new(None),
            next_collect: Mutex::new(None),
            grace: Mutex::new(SEALED_GRACE),
            tail,
            #[cfg(test)]
            sync_hook: Mutex::new(None),
        }))
    }

    /// Whether new notes are taken out.
    #[must_use]
    pub fn takes(&self) -> bool {
        self.take.load(Ordering::Relaxed)
    }

    /// The blob file `id`, if it is still there.
    #[must_use]
    pub fn file(&self, id: u64) -> Option<Arc<BlobFile>> {
        self.files
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// Registers the holder whose references keep files ([`BlobHolder`]).
    pub fn set_holder(&self, h: Weak<dyn BlobHolder>) {
        *lock(&self.holder) = Some(h);
    }

    /// Whether a live holder is registered.
    #[must_use]
    pub fn has_holder(&self) -> bool {
        lock(&self.holder)
            .as_ref()
            .is_some_and(|w| w.strong_count() > 0)
    }

    /// A new file to write, after the highest id there is.
    fn new_active(&self) -> io::Result<Active> {
        let id = self
            .files
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .next_back()
            .map_or(1, |i| i + 1);
        let path = self.dir.join(blob_name(id));
        let mut o = OpenOptions::new();
        o.read(true).write(true).create(true).truncate(true);
        #[cfg(target_os = "macos")]
        if self.sync == SyncMode::Full && !super::syncs_simulated() {
            use std::os::unix::fs::OpenOptionsExt;
            o.custom_flags(O_DSYNC);
        }
        let w = o.open(&path)?;
        if self.sync == SyncMode::Full
            && let Ok(d) = File::open(&self.dir)
        {
            d.sync_all()?;
        }
        let file = Arc::new(BlobFile { id, file: w });
        self.files
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, Arc::clone(&file));
        lock(&self.sums).insert(
            id,
            Summary {
                units: BTreeMap::new(),
                this_run: true,
                sealed: None,
            },
        );
        Ok(Active {
            file,
            len: 0,
            pending: Vec::new(),
            gate: Arc::default(),
        })
    }

    /// One sync of `file`. `whole`: the file's own full sync (a file that
    /// rolls); otherwise the sync that runs alongside the log's, which on
    /// macOS is the log's (module docs).
    fn sync_file(&self, file: &BlobFile, whole: bool) -> io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = lock(&self.sync_hook).clone() {
            hook();
        }
        if self.sync == SyncMode::Full
            && !super::simulated_sync()
            && (whole || !cfg!(target_os = "macos"))
        {
            file.file.sync_data()?;
        }
        Ok(())
    }

    /// Writes `a`'s pending records, synced when asked.
    fn write_pending(&self, a: &mut Active, sync: bool) -> io::Result<()> {
        if !a.pending.is_empty() {
            a.file.file.write_all_at(&a.pending, a.len)?;
            super::fs::note_write();
            a.len += a.pending.len() as u64;
            a.pending.clear();
            a.gate.note_write();
        }
        if sync {
            a.gate.sync_with(|| self.sync_file(&a.file, true))?;
        }
        Ok(())
    }

    /// Finishes the active file (written, synced, summary saved) and starts
    /// the next.
    fn roll(&self, a: &mut Active) -> io::Result<()> {
        self.write_pending(a, true)?;
        if let Some(s) = lock(&self.sums).get_mut(&a.file.id) {
            s.sealed = Some(Instant::now());
            let _ = write_sum(&self.dir.join(sum_name(a.file.id)), &s.units);
        }
        *a = self.new_active()?;
        Ok(())
    }

    /// Moves the notes of `data` (entry `index` of `unit`, term `term`)
    /// that the filter picks into the blob file, unwritten until
    /// [`NoteBlobs::persist`]; returns the payload to store in the log
    /// instead, or `None` to store `data` as it is.
    ///
    /// # Errors
    /// Rolling to a new file failed.
    pub fn take_out(
        &self,
        unit: u64,
        index: u64,
        term: u64,
        data: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        if !self.takes() || data.len() < MIN_NOTE {
            return Ok(None);
        }
        let Ok(ops) = payload::split(data) else {
            return Ok(None);
        };
        let pick =
            |code: u8, body: &[u8]| code == NOTE && body.len() >= MIN_NOTE && (self.filter)(body);
        if !ops.iter().any(|(c, _, b)| pick(*c, b)) {
            return Ok(None);
        }
        let mut guard = lock(&self.active);
        if guard.is_none() {
            *guard = Some(self.new_active()?);
        }
        let Some(a) = guard.as_mut() else {
            return Ok(None);
        };
        let mut out = Vec::with_capacity(64 + ops.len() * (REF_LEN + 2));
        out.push(FORMAT);
        let mut locs = Vec::new();
        let mut slot = 0u32;
        for (code, whole, body) in ops {
            if code != NOTE {
                out.extend_from_slice(whole);
                continue;
            }
            if pick(code, body) {
                let need = (HEAD + body.len()) as u64;
                if a.len + a.pending.len() as u64 > 0
                    && a.len + a.pending.len() as u64 + need > FILE_BYTES
                {
                    self.roll(a)?;
                }
                let at = a.len + a.pending.len() as u64 + HEAD as u64;
                let len =
                    u32::try_from(body.len()).map_err(|_| corrupt("note too large".into()))?;
                let crc = crc32(body);
                a.pending.extend_from_slice(MAGIC);
                a.pending.extend_from_slice(&crc.to_le_bytes());
                a.pending.extend_from_slice(&len.to_le_bytes());
                a.pending.extend_from_slice(&0u32.to_le_bytes());
                a.pending.extend_from_slice(&unit.to_le_bytes());
                a.pending.extend_from_slice(&index.to_le_bytes());
                a.pending.extend_from_slice(body);
                let loc = BlobLoc {
                    file: a.file.id,
                    at,
                    len,
                };
                let mut r = [0u8; REF_LEN];
                r[..8].copy_from_slice(&loc.file.to_le_bytes());
                r[8..16].copy_from_slice(&loc.at.to_le_bytes());
                r[16..20].copy_from_slice(&len.to_le_bytes());
                r[20..24].copy_from_slice(&crc.to_le_bytes());
                payload::push_op(&mut out, BLOB, &r);
                locs.push((slot, loc));
                let mut sums = lock(&self.sums);
                if let Some(s) = sums.get_mut(&a.file.id) {
                    let e = s.units.entry(unit).or_insert(index);
                    *e = (*e).max(index);
                }
            } else {
                out.extend_from_slice(whole);
            }
            slot += 1;
        }
        drop(guard);
        self.locate(unit, index, term, locs);
        Ok(Some(out))
    }

    /// Writes what [`NoteBlobs::take_out`] took out since the last call,
    /// and syncs the file when `sync`.
    ///
    /// # Errors
    /// The write or the sync failed.
    pub fn persist(&self, sync: bool) -> io::Result<()> {
        self.write_out()?;
        if sync { self.sync() } else { Ok(()) }
    }

    /// Writes what [`NoteBlobs::take_out`] took out since the last call
    /// (before the log's write that refers to it).
    ///
    /// # Errors
    /// The write failed.
    pub fn write_out(&self) -> io::Result<()> {
        match lock(&self.active).as_mut() {
            Some(a) => self.write_pending(a, false),
            None => Ok(()),
        }
    }

    /// Syncs what was written (alongside the log's own sync): everything
    /// written before the call is durable when it returns. The file is
    /// synced outside the lock, so writes go on meanwhile; several syncs
    /// may run at once (a log that overlaps its syncs). A call with nothing
    /// new to sync returns at once only when a finished sync covers the
    /// file's last write; while the sync covering it still runs, the call
    /// waits for it. A file that rolled was synced when it rolled.
    ///
    /// # Errors
    /// The sync failed.
    pub fn sync(&self) -> io::Result<()> {
        let (file, gate) = match lock(&self.active).as_ref() {
            Some(a) => (Arc::clone(&a.file), Arc::clone(&a.gate)),
            None => return Ok(()),
        };
        // macOS: written with O_DSYNC; the log's full sync flushes the
        // drive (module docs).
        gate.sync_with(|| self.sync_file(&file, false))
    }

    /// Whether `loc` lies where a crash cut the newest file's unsynced
    /// tail (see the module docs).
    fn torn(&self, loc: &BlobLoc) -> bool {
        self.tail
            .is_some_and(|(id, end)| id == loc.file && loc.at + u64::from(loc.len) > end)
    }

    /// Puts the notes `data` refers to back (entry `index` of `unit`, term
    /// `term`, as the log store read it); `None` when it refers to none.
    ///
    /// # Errors
    /// A blob file is gone, or a note doesn't match its checksum.
    pub fn put_back(
        &self,
        unit: u64,
        index: u64,
        term: u64,
        data: &[u8],
    ) -> io::Result<Option<Vec<u8>>> {
        // Quick look: a payload without a reference has no BLOB code at an
        // op start; only a split tells for sure, so split when it might.
        if !data.contains(&BLOB) {
            return Ok(None);
        }
        let Ok(ops) = payload::split(data) else {
            return Ok(None);
        };
        if !ops.iter().any(|(c, ..)| *c == BLOB) {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(data.len() + 64);
        out.push(FORMAT);
        let mut locs = Vec::new();
        let mut slot = 0u32;
        for (code, whole, body) in ops {
            match code {
                NOTE => {
                    out.extend_from_slice(whole);
                    slot += 1;
                }
                BLOB => {
                    if body.len() != REF_LEN {
                        return Err(corrupt(format!(
                            "log entry {index} of unit {unit}: malformed note reference"
                        )));
                    }
                    let loc = BlobLoc {
                        file: u64_at(body, 0),
                        at: u64_at(body, 8),
                        len: u32_at(body, 16),
                    };
                    if self.torn(&loc) {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "log entry {index} of unit {unit}: its note was in the unsynced \
                                 tail of blob file {} when the node stopped",
                                loc.file
                            ),
                        ));
                    }
                    let f = self.file(loc.file).ok_or_else(|| {
                        corrupt(format!(
                            "log entry {index} of unit {unit}: note blob file {} is gone",
                            loc.file
                        ))
                    })?;
                    let mut note = vec![0u8; loc.len as usize];
                    f.read_exact_at(&mut note, loc.at).map_err(|e| {
                        corrupt(format!(
                            "log entry {index} of unit {unit}: blob file {} at {}: {e}",
                            loc.file, loc.at
                        ))
                    })?;
                    if crc32(&note) != u32_at(body, 20) {
                        return Err(corrupt(format!(
                            "log entry {index} of unit {unit}: note in blob file {} at {} \
                             doesn't match its checksum",
                            loc.file, loc.at
                        )));
                    }
                    payload::push_op(&mut out, NOTE, &note);
                    locs.push((slot, loc));
                    slot += 1;
                }
                _ => out.extend_from_slice(whole),
            }
        }
        self.locate(unit, index, term, locs);
        Ok(Some(out))
    }

    fn locate(&self, unit: u64, index: u64, term: u64, locs: Vec<(u32, BlobLoc)>) {
        if locs.is_empty() {
            return;
        }
        let mut l = lock(&self.located);
        l.insert((unit, index), (term, locs));
        while l.len() > LOCATED_MAX {
            l.pop_first();
        }
    }

    /// The (slot, location) of the notes of entry `index` of `unit` that
    /// this node keeps in blob files, for its apply step: empty when the
    /// entry wasn't taken out here (or with another term). Forgets every
    /// location of `unit` up to `index` (entries apply in order).
    pub fn take_located(&self, unit: u64, index: u64, term: u64) -> Vec<(u32, BlobLoc)> {
        let mut l = lock(&self.located);
        let mut got = Vec::new();
        let keys: Vec<(u64, u64)> = l
            .range((unit, 0)..=(unit, index))
            .map(|(k, _)| *k)
            .collect();
        for k in keys {
            if let Some((t, locs)) = l.remove(&k)
                && k.1 == index
                && t == term
            {
                got = locs;
            }
        }
        got
    }

    /// Removes the blob files nothing needs any more (see the module docs).
    /// `held(unit)` is the first index the log still holds of `unit`.
    /// Runs at most every ten seconds; returns the files removed.
    pub fn collect(&self, held: &dyn Fn(u64) -> Option<u64>) -> usize {
        {
            let mut next = lock(&self.next_collect);
            let now = Instant::now();
            if next.is_some_and(|n| now < n) {
                return 0;
            }
            *next = Some(now + COLLECT_EVERY);
        }
        let active = lock(&self.active).as_ref().map(|a| a.file.id);
        let grace = *lock(&self.grace);
        // The log first: a file whose entries it no longer holds gains no
        // new reference (only applying an entry makes one), so a holder
        // asked after this sees every reference such a file still has.
        let free: Vec<u64> = lock(&self.sums)
            .iter()
            .filter(|(id, s)| {
                Some(**id) != active
                    && (!s.this_run || s.sealed.is_some_and(|t| t.elapsed() >= grace))
                    && s.units
                        .iter()
                        .all(|(u, max)| held(*u).is_none_or(|first| first > *max))
            })
            .map(|(id, _)| *id)
            .collect();
        if free.is_empty() {
            return 0;
        }
        let holder = lock(&self.holder).as_ref().and_then(Weak::upgrade);
        let in_use = holder.map(|h| h.in_use());
        let mut gone = 0;
        for id in free {
            let keep = match &in_use {
                Some(u) => u.contains(&id),
                None => lock(&self.sums).get(&id).is_none_or(|s| !s.this_run),
            };
            if keep {
                continue;
            }
            lock(&self.sums).remove(&id);
            self.files
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
            let _ = fs::remove_file(self.dir.join(blob_name(id)));
            let _ = fs::remove_file(self.dir.join(sum_name(id)));
            gone += 1;
        }
        gone
    }

    /// Bytes in blob files (tests, figures).
    #[must_use]
    pub fn disk_bytes(&self) -> u64 {
        fs::read_dir(&self.dir)
            .map(|rd| {
                rd.flatten()
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }

    /// The ids of the blob files there are (tests).
    #[must_use]
    pub fn file_ids(&self) -> Vec<u64> {
        self.files
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Payload;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("sprig-blobs-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn pick(n: &[u8]) -> bool {
        n.first() == Some(&b'K')
    }

    fn entry(notes: &[&[u8]]) -> Vec<u8> {
        let mut p = Payload::new();
        p.put(crate::store::Cf::Data, b"k", b"v");
        for n in notes {
            p.note(n);
        }
        p.into_bytes()
    }

    /// Notes go out and come back byte for byte; small and unpicked notes
    /// stay; locations are found once; a reopened store reads them too.
    #[test]
    fn notes_go_out_and_come_back() {
        let d = dir("round");
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        let a = [b'K'; 1000];
        let small = [b'K'; 10];
        let other = [b'x'; 2000];
        let data = entry(&[&a, &small, &other, &[b'K'; 3000]]);
        let stored = b.take_out(5, 9, 2, &data).unwrap().unwrap();
        assert!(
            stored.len() < 2200,
            "only the two picked notes left: {}",
            stored.len()
        );
        assert!(
            payload::notes(&stored).is_err(),
            "readers refuse references"
        );
        b.persist(true).unwrap();
        let locs = b.take_located(5, 9, 2);
        assert_eq!(locs.iter().map(|l| l.0).collect::<Vec<_>>(), [0, 3]);
        assert!(b.take_located(5, 9, 2).is_empty(), "taken once");
        let back = b.put_back(5, 9, 2, &stored).unwrap().unwrap();
        assert_eq!(back, data);
        assert_eq!(b.take_located(5, 9, 3), Vec::new(), "another term: nothing");
        assert_eq!(
            b.put_back(5, 9, 2, &data).unwrap(),
            None,
            "nothing to put back"
        );
        // The note's bytes are where the location says.
        let mut buf = vec![0u8; 1000];
        b.file(locs[0].1.file)
            .unwrap()
            .read_exact_at(&mut buf, locs[0].1.at)
            .unwrap();
        assert_eq!(buf, a);
        drop(b);
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        assert_eq!(b.put_back(5, 9, 2, &stored).unwrap().unwrap(), data);
        assert_eq!(b.take_located(5, 9, 2).len(), 2);
        // A damaged note is refused, never handed out.
        let f = OpenOptions::new()
            .write(true)
            .open(d.join(blob_name(locs[0].1.file)))
            .unwrap();
        f.write_all_at(b"Z", locs[0].1.at + 5).unwrap();
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        assert_eq!(
            b.put_back(5, 9, 2, &stored).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof,
            "a damaged record in the newest file's tail: cut there"
        );
        // A damaged note in a file that isn't the newest is corruption.
        b.take_out(5, 10, 2, &entry(&[&a])).unwrap();
        b.persist(true).unwrap();
        drop(b);
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        assert_eq!(
            b.put_back(5, 9, 2, &stored).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let _ = fs::remove_dir_all(&d);
    }

    /// Two overlapped syncs of the blob file (a log that overlaps its
    /// syncs runs one beside each of the log's): the second finds nothing
    /// new to sync while the first, which covers the note, still runs. It
    /// waits for the first instead of reporting the note durable.
    #[test]
    fn a_sync_waits_for_the_running_sync_that_covers_its_note() {
        use std::sync::mpsc;
        let d = dir("overlap");
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        b.take_out(5, 9, 2, &entry(&[&[b'K'; 1000]]))
            .unwrap()
            .unwrap();
        b.write_out().unwrap();
        let (began_tx, began) = mpsc::channel::<()>();
        let (finish, finish_rx) = mpsc::channel::<()>();
        let finish_rx = Mutex::new(finish_rx);
        let first = AtomicBool::new(true);
        *lock(&b.sync_hook) = Some(Arc::new(move || {
            // The first sync is the slow one.
            if first.swap(false, Ordering::AcqRel) {
                began_tx.send(()).unwrap();
                lock(&finish_rx).recv().unwrap();
            }
        }));
        let returned = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| b.sync().unwrap());
            began.recv().unwrap();
            s.spawn(|| {
                b.sync().unwrap();
                returned.store(true, Ordering::Release);
            });
            std::thread::sleep(Duration::from_millis(150));
            assert!(
                !returned.load(Ordering::Acquire),
                "a sync returned while the sync covering the note was still running"
            );
            finish.send(()).unwrap();
        });
        assert!(returned.load(Ordering::Acquire));
        // A later note needs a sync of its own, which doesn't wait.
        b.take_out(5, 10, 2, &entry(&[&[b'K'; 1000]]))
            .unwrap()
            .unwrap();
        b.persist(true).unwrap();
        let _ = fs::remove_dir_all(&d);
    }

    struct Holds(Mutex<BTreeSet<u64>>);

    impl BlobHolder for Holds {
        fn in_use(&self) -> BTreeSet<u64> {
            lock(&self.0).clone()
        }
    }

    /// Files roll; a file goes only when the log holds none of its entries
    /// and no holder refers to it; the active file stays; before a holder
    /// registers, only this run's files may go.
    #[test]
    fn files_go_when_nothing_needs_them() {
        let d = dir("collect");
        let note = vec![b'K'; 1 << 20];
        let fill = |b: &NoteBlobs, from: u64, to: u64| {
            for i in from..=to {
                b.take_out(7, i, 1, &entry(&[&note])).unwrap().unwrap();
                b.persist(true).unwrap();
            }
        };
        let again = |b: &NoteBlobs| *lock(&b.next_collect) = None;
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        fill(&b, 1, 200);
        // Just finished: kept for a while whatever the log holds.
        assert_eq!(b.collect(&|_| None), 0);
        *lock(&b.grace) = Duration::ZERO;
        again(&b);
        let ids = b.file_ids();
        assert!(ids.len() >= 3, "rolled: {ids:?}");
        // The log holds every entry: nothing goes.
        assert_eq!(b.collect(&|_| Some(1)), 0);
        // It holds entries from 100 on: files of older entries only go.
        again(&b);
        assert!(b.collect(&|_| Some(100)) >= 1);
        assert!(b.file_ids().len() >= 2);
        // It holds none: every file but the active one goes.
        again(&b);
        b.collect(&|_| None);
        let last1 = *ids.last().unwrap();
        assert_eq!(b.file_ids(), vec![last1]);
        drop(b);
        let b = NoteBlobs::open(&d, SyncMode::Off, pick, true).unwrap();
        *lock(&b.grace) = Duration::ZERO;
        fill(&b, 201, 300);
        again(&b);
        b.collect(&|_| None);
        let now = b.file_ids();
        assert!(
            now.contains(&last1),
            "an earlier run's file stays without a holder"
        );
        assert_eq!(
            now.len(),
            2,
            "this run's files went but the active one: {now:?}"
        );
        // A holder that refers to it keeps it; once it doesn't, it goes.
        let holds = Arc::new(Holds(Mutex::new(BTreeSet::from([last1]))));
        let h: Arc<dyn BlobHolder> = holds.clone();
        b.set_holder(Arc::downgrade(&h));
        assert!(b.has_holder());
        again(&b);
        b.collect(&|_| None);
        assert!(b.file_ids().contains(&last1), "held");
        lock(&holds.0).clear();
        again(&b);
        b.collect(&|_| None);
        assert_eq!(b.file_ids().len(), 1, "only the active file is left");
        assert!(!b.file_ids().contains(&last1));
        let _ = fs::remove_dir_all(&d);
    }
}
