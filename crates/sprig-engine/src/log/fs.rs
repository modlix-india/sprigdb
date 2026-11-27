//! raft-engine's file seam.
//!
//! raft-engine reaches the disk only through its `FileSystem` trait, and
//! syncs through `Handle::sync`. This implementation:
//! - syncs with `File::sync_data`, which is `fdatasync` on Linux and
//!   `fcntl(F_FULLFSYNC)` on macOS (Rust's standard library issues the full
//!   flush on Apple platforms; raft-engine's own file system calls plain
//!   `fsync` there, which leaves data in the drive's cache);
//! - can skip syncs entirely ([`SyncMode::Off`], for benchmarks that say so);
//! - can simulate a crash ([`SimDisk`]): it remembers how much of each file
//!   was synced, and [`SimDisk::crash`] cuts every file back to its synced
//!   length plus a seed-chosen part of the unsynced tail, the way a power
//!   loss can keep some unsynced pages and lose others. The power goes at
//!   [`SimDisk::power_cut`]: syncs after it (raft-engine's own closing sync
//!   when the log is dropped, for one) don't count.
//!
//! Directory entries (creates, renames, deletes) are not simulated: the engine runs
//! raft-engine without log recycling, so it only creates and deletes files.
//!
//! **Syncs outside raft-engine.** raft-engine syncs only inside a write
//! (its own `sync()` writes an empty batch, which it skips). A log that
//! overlaps its syncs writes without syncing and then calls
//! [`LogFs::sync_written`], from any thread, while later writes go on: it
//! syncs every append file written since its last sync, so everything
//! written before the call is durable when it returns. Files raft-engine
//! rotates away are synced by raft-engine itself when it closes them.
//!
//! **Simulated syncs** ([`simulate_syncs`], benchmarks only): every sync of
//! a log in the process sleeps instead of reaching the disk, either all
//! side by side (a device that runs syncs in parallel) or one after
//! another with each covering all that waited for it (a file system whose
//! journal commits one at a time).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use raft_engine::env::{FileSystem, Handle, Permission, WriteExt};

/// How syncs reach the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// `fdatasync` on Linux, `F_FULLFSYNC` on macOS.
    #[default]
    Full,
    /// No syncs at all. Only for benchmarks that report it.
    Off,
}

/// How simulated syncs behave ([`simulate_syncs`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimSyncs {
    /// How long one sync takes.
    pub delay: Duration,
    /// Syncs run one at a time, each covering every sync that waited for
    /// it (a journal's commits); otherwise side by side.
    pub serial: bool,
}

static SIM_SYNCS: OnceLock<SimSyncs> = OnceLock::new();

/// One journal commit at a time ([`SimSyncs::serial`]).
struct Journal {
    /// Commits started.
    started: u64,
    /// The last commit finished.
    done: u64,
    running: bool,
    /// [`SIM_WRITES`] when the running commit started.
    writes_at_start: u64,
}

static JOURNAL: (Mutex<Journal>, Condvar) = (
    Mutex::new(Journal {
        started: 0,
        done: 0,
        running: false,
        writes_at_start: 0,
    }),
    Condvar::new(),
);

/// Writes to logs while syncs are simulated ([`note_write`]).
static SIM_WRITES: AtomicU64 = AtomicU64::new(0);

/// A write to a log file, for simulated serial syncs: a commit covers the
/// writes made before it started.
pub(super) fn note_write() {
    if SIM_SYNCS.get().is_some_and(|s| s.serial) {
        SIM_WRITES.fetch_add(1, Ordering::AcqRel);
    }
}

/// Benchmarks only: from now on every sync of a log in this process (the
/// Raft log, its note blobs, the engine's own log) sleeps `s.delay` instead
/// of reaching the disk, which makes nothing durable. For measuring how the
/// write path behaves on slower disks than the machine's. Only the first
/// call counts; returns whether it was the first.
pub fn simulate_syncs(s: SimSyncs) -> bool {
    SIM_SYNCS.set(s).is_ok()
}

/// Whether syncs are simulated ([`simulate_syncs`]).
#[must_use]
pub fn syncs_simulated() -> bool {
    SIM_SYNCS.get().is_some()
}

/// When syncs are simulated, takes one simulated sync and returns true;
/// otherwise false (the caller syncs for real).
#[must_use]
pub fn simulated_sync() -> bool {
    let Some(s) = SIM_SYNCS.get() else {
        return false;
    };
    if !s.serial {
        std::thread::sleep(s.delay);
        return true;
    }
    let (m, cv) = &JOURNAL;
    let mut j = m.lock().unwrap_or_else(PoisonError::into_inner);
    // The running commit covers this sync if nothing was written since it
    // started; otherwise the next one does.
    let need = if j.running && SIM_WRITES.load(Ordering::Acquire) == j.writes_at_start {
        j.started
    } else {
        j.started + 1
    };
    while j.done < need {
        if j.running {
            j = cv.wait(j).unwrap_or_else(PoisonError::into_inner);
            continue;
        }
        j.running = true;
        j.started += 1;
        j.writes_at_start = SIM_WRITES.load(Ordering::Acquire);
        let id = j.started;
        drop(j);
        std::thread::sleep(s.delay);
        j = m.lock().unwrap_or_else(PoisonError::into_inner);
        j.done = id;
        j.running = false;
        cv.notify_all();
    }
    true
}

/// Crash simulation state shared by every file of one log.
#[derive(Debug, Default)]
pub struct SimDisk {
    synced: Mutex<HashMap<PathBuf, u64>>,
    /// The power is off: syncs no longer make anything durable.
    cut: AtomicBool,
}

impl SimDisk {
    /// A fresh simulation.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn note_sync(&self, path: &Path, len: u64) {
        let mut synced = self.synced.lock().unwrap_or_else(PoisonError::into_inner);
        // Checked under the lock, so a sync either counts before the cut
        // or not at all.
        if self.cut.load(Ordering::Acquire) {
            return;
        }
        // Two syncs of one file can race (the writer's and a checkpoint's):
        // the synced length only grows.
        let e = synced.entry(path.to_path_buf()).or_insert(0);
        *e = (*e).max(len);
    }

    /// The power goes off now: later syncs don't count. Call it once nothing
    /// more can be acknowledged (the log's writer has stopped), then drop the
    /// log and call [`SimDisk::crash`].
    pub fn power_cut(&self) {
        let _g = self.synced.lock().unwrap_or_else(PoisonError::into_inner);
        self.cut.store(true, Ordering::Release);
    }

    /// Whether [`SimDisk::power_cut`] was called since the last crash.
    #[must_use]
    pub fn is_cut(&self) -> bool {
        self.cut.load(Ordering::Acquire)
    }

    /// Simulates a power loss after the log was dropped: each file keeps its
    /// synced bytes plus a seed-chosen prefix of its unsynced tail. What
    /// survives counts as synced afterwards, so the log can be reopened on
    /// the same simulation and crashed again. Files a simulation never saw
    /// count as unsynced: reopen a crashed log on the simulation that
    /// crashed it. Returns how many unsynced bytes were lost in total.
    ///
    /// # Errors
    /// A file could not be truncated.
    pub fn crash(&self, dir: &Path, seed: u64) -> IoResult<u64> {
        let synced = self
            .synced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut lost = 0;
        let mut rng = seed | 1;
        // What survives the power loss is on the disk: a later crash (the
        // same log reopened on this simulation) keeps it.
        let mut survived = HashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let len = std::fs::metadata(&path)?.len();
            let keep_synced = synced.get(&path).copied().unwrap_or(0).min(len);
            let tail = len - keep_synced;
            if tail == 0 {
                survived.insert(path, len);
                continue;
            }
            // xorshift: deterministic per seed.
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let keep = rng % (tail + 1);
            let new_len = keep_synced + keep;
            lost += len - new_len;
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(new_len)?;
            survived.insert(path, new_len);
        }
        *self.synced.lock().unwrap_or_else(PoisonError::into_inner) = survived;
        self.cut.store(false, Ordering::Release);
        Ok(lost)
    }
}

/// The file system handed to raft-engine.
#[derive(Debug, Clone, Default)]
pub struct LogFs {
    mode: SyncMode,
    sim: Option<Arc<SimDisk>>,
    /// raft-engine's append files opened for writing, for
    /// [`LogFs::sync_written`].
    append: Arc<Mutex<Vec<Weak<FileCore>>>>,
}

impl LogFs {
    /// Real files with the given sync mode, optionally under a crash
    /// simulation.
    #[must_use]
    pub fn new(mode: SyncMode, sim: Option<Arc<SimDisk>>) -> Self {
        Self {
            mode,
            sim,
            append: Arc::default(),
        }
    }

    /// Syncs every append file written since its last sync: everything
    /// written to the log before the call is durable when it returns (see
    /// the module docs). Safe alongside writes and other syncs.
    ///
    /// # Errors
    /// A sync failed; the file counts as unsynced again.
    pub fn sync_written(&self) -> IoResult<()> {
        let files: Vec<Arc<FileCore>> = {
            let mut a = self.append.lock().unwrap_or_else(PoisonError::into_inner);
            a.retain(|w| w.strong_count() > 0);
            a.iter().filter_map(Weak::upgrade).collect()
        };
        for f in files {
            if f.dirty.swap(false, Ordering::AcqRel)
                && let Err(e) = f.sync()
            {
                f.dirty.store(true, Ordering::Release);
                return Err(e);
            }
        }
        Ok(())
    }
}

/// An open log file, shared by its handle and [`LogFs::sync_written`].
#[derive(Debug)]
struct FileCore {
    file: File,
    path: PathBuf,
    mode: SyncMode,
    sim: Option<Arc<SimDisk>>,
    /// Written since the last sync began.
    dirty: AtomicBool,
}

impl FileCore {
    fn sync(&self) -> IoResult<()> {
        // What the simulation counts as synced is what was there before
        // the sync began: writes alongside it may not be covered.
        let len = match &self.sim {
            Some(_) => self.file.metadata()?.len(),
            None => 0,
        };
        if self.mode == SyncMode::Full && !simulated_sync() {
            self.file.sync_data()?;
        }
        if let Some(sim) = &self.sim {
            sim.note_sync(&self.path, len);
        }
        Ok(())
    }
}

/// An open log file.
#[derive(Debug)]
pub struct LogHandle {
    core: Arc<FileCore>,
}

impl Handle for LogHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.core.file.set_len(offset as u64)
    }

    fn file_size(&self) -> IoResult<usize> {
        usize::try_from(self.core.file.metadata()?.len()).map_err(std::io::Error::other)
    }

    fn sync(&self) -> IoResult<()> {
        self.core.dirty.store(false, Ordering::Release);
        self.core.sync().inspect_err(|_| {
            self.core.dirty.store(true, Ordering::Release);
        })
    }
}

/// A reader or writer at an offset of a [`LogHandle`].
#[derive(Debug)]
pub struct LogIo {
    h: Arc<LogHandle>,
    offset: u64,
}

impl Read for LogIo {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let n = self.h.core.file.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl Write for LogIo {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let n = self.h.core.file.write_at(buf, self.offset)?;
        self.offset += n as u64;
        self.h.core.dirty.store(true, Ordering::Release);
        note_write();
        Ok(n)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Seek for LogIo {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let to = match pos {
            SeekFrom::Start(o) => Some(o),
            SeekFrom::Current(d) => self.offset.checked_add_signed(d),
            SeekFrom::End(d) => self.h.core.file.metadata()?.len().checked_add_signed(d),
        };
        self.offset = to.ok_or_else(|| std::io::Error::other("seek out of range"))?;
        Ok(self.offset)
    }
}

impl WriteExt for LogIo {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.h.core.file.set_len(offset as u64)?;
        self.offset = offset as u64;
        Ok(())
    }

    fn allocate(&mut self, _offset: usize, _size: usize) -> IoResult<()> {
        Ok(())
    }
}

/// raft-engine's append files end so (its rewrite files, `.rewrite`, are
/// written and synced by its own purge).
const APPEND_SUFFIX: &str = ".raftlog";

impl LogFs {
    fn handle(&self, file: File, path: &Path, writable: bool) -> LogHandle {
        let core = Arc::new(FileCore {
            file,
            path: path.to_path_buf(),
            mode: self.mode,
            sim: self.sim.clone(),
            dirty: AtomicBool::new(false),
        });
        if writable && path.to_string_lossy().ends_with(APPEND_SUFFIX) {
            self.append
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Arc::downgrade(&core));
        }
        LogHandle { core }
    }
}

impl FileSystem for LogFs {
    type Handle = LogHandle;
    type Reader = LogIo;
    type Writer = LogIo;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<LogHandle> {
        let p = path.as_ref();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(p)?;
        Ok(self.handle(f, p, true))
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> IoResult<LogHandle> {
        let p = path.as_ref();
        let f = OpenOptions::new()
            .read(true)
            .write(perm == Permission::ReadWrite)
            .open(p)?;
        Ok(self.handle(f, p, perm == Permission::ReadWrite))
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        std::fs::remove_file(path)
    }

    fn rename<P: AsRef<Path>>(&self, src: P, dst: P) -> IoResult<()> {
        std::fs::rename(src, dst)
    }

    fn new_reader(&self, h: Arc<LogHandle>) -> IoResult<LogIo> {
        Ok(LogIo { h, offset: 0 })
    }

    fn new_writer(&self, h: Arc<LogHandle>) -> IoResult<LogIo> {
        Ok(LogIo { h, offset: 0 })
    }
}
