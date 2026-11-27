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
//! [`LogFs::sync_written`], from any thread, while later writes go on:
//! everything written before the call is durable when it returns. Each
//! file counts its writes ([`SyncGate`]): a sync covers the writes made
//! before it began, so a caller returns only once a sync that began after
//! the file's last write before the call has finished, whether it ran that
//! sync itself or waited for another caller's. (A flag saying "written
//! since the last sync began" is not enough: a second caller would find it
//! clear and return while the first caller's sync, the one covering the
//! bytes, was still running.) Files raft-engine rotates away are synced by
//! raft-engine itself when it closes them, through the same gate.
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
    /// How long each sync takes, in microseconds ([`SimDisk::set_sync_delay`]).
    delay_micros: AtomicU64,
    /// Syncs begun ([`SimDisk::syncs_begun`]).
    begun: AtomicU64,
}

impl SimDisk {
    /// A fresh simulation.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Every sync under this simulation takes `d` from now on (a slow
    /// disk): what it covers counts as synced only once that time has
    /// passed, so a power cut meanwhile loses it.
    pub fn set_sync_delay(&self, d: Duration) {
        self.delay_micros.store(
            u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    /// How many syncs have begun under this simulation.
    #[must_use]
    pub fn syncs_begun(&self) -> u64 {
        self.begun.load(Ordering::Acquire)
    }

    /// A sync begins: counted, then it takes the sync delay.
    fn begin_sync(&self) {
        self.begun.fetch_add(1, Ordering::AcqRel);
        let d = self.delay_micros.load(Ordering::Acquire);
        if d > 0 {
            std::thread::sleep(Duration::from_micros(d));
        }
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
        self.crash_keeping(dir, Some(seed))
    }

    /// [`SimDisk::crash`] at its worst: every file keeps its synced bytes
    /// and nothing of its unsynced tail.
    ///
    /// # Errors
    /// A file could not be truncated.
    pub fn crash_losing_unsynced(&self, dir: &Path) -> IoResult<u64> {
        self.crash_keeping(dir, None)
    }

    /// The crash: with a seed, a seed-chosen prefix of each unsynced tail
    /// survives; without one, none of it.
    fn crash_keeping(&self, dir: &Path, seed: Option<u64>) -> IoResult<u64> {
        let synced = self
            .synced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut lost = 0;
        let mut rng = seed.unwrap_or(0) | 1;
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
            let keep = match seed {
                Some(_) => rng % (tail + 1),
                None => 0,
            };
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

/// Which writes of one file finished syncs cover.
///
/// Writes are counted after they reach the file ([`SyncGate::note_write`]).
/// A sync covers every write counted before it began, so
/// [`SyncGate::sync_with`] returns only once a sync that began at or after
/// the count it saw on entry has finished: it runs one, or waits for one
/// that is already running if that one began late enough. Syncs that began
/// earlier may still be running beside it; they don't cover the newest
/// writes, so they are not waited for and another sync starts at once.
#[derive(Debug, Default)]
pub(super) struct SyncGate {
    /// Writes to the file so far.
    written: AtomicU64,
    /// Every write up to this count is covered by a sync that finished
    /// (kept beside `state` so a caller with nothing to sync takes no lock).
    durable: AtomicU64,
    /// `written` when each running sync began.
    state: Mutex<Vec<u64>>,
    done: Condvar,
}

impl SyncGate {
    /// A write (or another change a sync must cover) reached the file.
    /// Call it after the write returns: a sync that then reads the count
    /// began after the write.
    pub(super) fn note_write(&self) {
        self.written.fetch_add(1, Ordering::AcqRel);
    }

    /// Returns once everything written before the call is covered by a
    /// finished `sync`. Safe alongside writes and other calls.
    ///
    /// # Errors
    /// The sync this call ran failed; what it would have covered stays
    /// uncovered, and whoever waited for it runs its own.
    pub(super) fn sync_with(&self, sync: impl Fn() -> IoResult<()>) -> IoResult<()> {
        let target = self.written.load(Ordering::Acquire);
        if self.durable.load(Ordering::Acquire) >= target {
            return Ok(());
        }
        let mut running = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if self.durable.load(Ordering::Acquire) >= target {
                return Ok(());
            }
            // A running sync that began after the last write this call must
            // cover: wait for it rather than start another.
            if running.iter().any(|began| *began >= target) {
                running = self
                    .done
                    .wait(running)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            // Read under the lock, before the sync starts: every write
            // counted by now is in the file.
            let began = self.written.load(Ordering::Acquire);
            running.push(began);
            drop(running);
            let res = sync();
            running = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(i) = running.iter().position(|b| *b == began) {
                running.swap_remove(i);
            }
            if res.is_ok() {
                self.durable.fetch_max(began, Ordering::AcqRel);
            }
            self.done.notify_all();
            res?;
        }
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

    /// Makes every append file's writes durable: everything written to the
    /// log before the call is durable when it returns (see the module
    /// docs). A file with nothing new is skipped only when a finished sync
    /// covers its last write; while the sync covering it still runs, the
    /// call waits for that sync. Safe alongside writes and other syncs.
    ///
    /// # Errors
    /// A sync failed; what it would have covered counts as unsynced.
    pub fn sync_written(&self) -> IoResult<()> {
        let files: Vec<Arc<FileCore>> = {
            let mut a = self.append.lock().unwrap_or_else(PoisonError::into_inner);
            a.retain(|w| w.strong_count() > 0);
            a.iter().filter_map(Weak::upgrade).collect()
        };
        for f in files {
            f.sync_covering()?;
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
    /// Which of its writes finished syncs cover.
    gate: SyncGate,
}

impl FileCore {
    /// Returns once everything written to the file before the call is
    /// durable ([`SyncGate::sync_with`]).
    fn sync_covering(&self) -> IoResult<()> {
        self.gate.sync_with(|| self.sync())
    }

    fn sync(&self) -> IoResult<()> {
        // What the simulation counts as synced is what was there before
        // the sync began: writes alongside it may not be covered.
        let len = match &self.sim {
            Some(_) => self.file.metadata()?.len(),
            None => 0,
        };
        if let Some(sim) = &self.sim {
            sim.begin_sync();
        }
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
        self.core.file.set_len(offset as u64)?;
        self.core.gate.note_write();
        Ok(())
    }

    fn file_size(&self) -> IoResult<usize> {
        usize::try_from(self.core.file.metadata()?.len()).map_err(std::io::Error::other)
    }

    fn sync(&self) -> IoResult<()> {
        // raft-engine's own syncs (a write it was asked to sync, a file it
        // closes or has just created) go through the gate too: a
        // `sync_written` beside one must not take the file for durable
        // before it has finished. They always sync, as raft-engine expects,
        // even with no write counted since the last sync.
        self.core.gate.note_write();
        self.core.sync_covering()
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
        self.h.core.gate.note_write();
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
        self.h.core.gate.note_write();
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
            gate: SyncGate::default(),
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;

    use super::*;

    /// A sync that blocks until told to finish, and says when it began.
    struct Slow {
        began: mpsc::Sender<()>,
        finish: Mutex<mpsc::Receiver<()>>,
    }

    impl Slow {
        fn new() -> (Slow, mpsc::Receiver<()>, mpsc::Sender<()>) {
            let (began, began_rx) = mpsc::channel();
            let (finish_tx, finish) = mpsc::channel();
            (
                Slow {
                    began,
                    finish: Mutex::new(finish),
                },
                began_rx,
                finish_tx,
            )
        }

        fn run(&self) {
            self.began.send(()).unwrap();
            self.finish.lock().unwrap().recv().unwrap();
        }
    }

    /// The interleaving of two overlapped sync workers: the write a caller
    /// must cover is covered by another caller's sync that is still
    /// running. The caller waits for it; it neither returns early nor
    /// starts a sync of its own.
    #[test]
    fn a_sync_waits_for_the_running_sync_that_covers_its_writes() {
        let gate = SyncGate::default();
        let (slow, began, finish) = Slow::new();
        let own = AtomicUsize::new(0);
        let returned = AtomicBool::new(false);
        gate.note_write();
        gate.note_write();
        std::thread::scope(|s| {
            s.spawn(|| {
                gate.sync_with(|| {
                    slow.run();
                    Ok(())
                })
                .unwrap();
            });
            began.recv().unwrap();
            s.spawn(|| {
                gate.sync_with(|| {
                    own.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
                .unwrap();
                returned.store(true, Ordering::Release);
            });
            std::thread::sleep(Duration::from_millis(150));
            assert!(
                !returned.load(Ordering::Acquire),
                "returned while the sync covering its writes was still running"
            );
            finish.send(()).unwrap();
        });
        assert!(returned.load(Ordering::Acquire));
        assert_eq!(
            own.load(Ordering::Acquire),
            0,
            "the running sync covered it"
        );
        // Nothing new: no sync at all.
        gate.sync_with(|| panic!("nothing to sync")).unwrap();
    }

    /// Syncs still overlap: a running sync that began before a write does
    /// not cover it, so the next caller starts its own beside it and does
    /// not wait for the first.
    #[test]
    fn a_sync_runs_beside_one_that_began_before_its_write() {
        let gate = SyncGate::default();
        let (slow, began, finish) = Slow::new();
        gate.note_write();
        std::thread::scope(|s| {
            let first = s.spawn(|| {
                gate.sync_with(|| {
                    slow.run();
                    Ok(())
                })
                .unwrap();
            });
            began.recv().unwrap();
            gate.note_write();
            let own = AtomicUsize::new(0);
            gate.sync_with(|| {
                own.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })
            .unwrap();
            assert_eq!(own.load(Ordering::Acquire), 1);
            assert!(!first.is_finished(), "the earlier sync is still running");
            finish.send(()).unwrap();
        });
        gate.sync_with(|| panic!("nothing to sync")).unwrap();
    }

    /// A failed sync covers nothing: its caller gets the error, and the
    /// next call (or whoever waited for it) syncs again.
    #[test]
    fn a_failed_sync_covers_nothing() {
        let gate = SyncGate::default();
        gate.note_write();
        let err = gate
            .sync_with(|| Err(std::io::Error::other("disk")))
            .unwrap_err();
        assert_eq!(err.to_string(), "disk");
        let ran = AtomicUsize::new(0);
        gate.sync_with(|| {
            ran.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
        .unwrap();
        assert_eq!(ran.load(Ordering::Acquire), 1);

        // A waiter of a sync that fails runs its own.
        let (slow, began, finish) = Slow::new();
        gate.note_write();
        std::thread::scope(|s| {
            let first = s.spawn(|| {
                gate.sync_with(|| {
                    slow.run();
                    Err(std::io::Error::other("disk"))
                })
            });
            began.recv().unwrap();
            let waiter = s.spawn(|| {
                gate.sync_with(|| {
                    ran.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                })
            });
            std::thread::sleep(Duration::from_millis(50));
            finish.send(()).unwrap();
            assert!(first.join().unwrap().is_err());
            waiter.join().unwrap().unwrap();
        });
        assert_eq!(ran.load(Ordering::Acquire), 2);
    }

    /// Many writers and syncers: whenever a sync call returns, a sync that
    /// began after every write counted before the call has finished.
    #[test]
    fn every_returned_sync_covers_what_was_written_before_it() {
        let gate = SyncGate::default();
        // The write count the latest finished sync began at, as the "disk"
        // sees it.
        let disk = AtomicU64::new(0);
        let stop = AtomicBool::new(false);
        std::thread::scope(|s| {
            for t in 0..4u64 {
                let (gate, disk, stop) = (&gate, &disk, &stop);
                s.spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        let before = gate.written.load(Ordering::Acquire);
                        gate.sync_with(|| {
                            let at = gate.written.load(Ordering::Acquire);
                            std::thread::sleep(Duration::from_micros(200 + 100 * t));
                            disk.fetch_max(at, Ordering::AcqRel);
                            Ok(())
                        })
                        .unwrap();
                        assert!(
                            disk.load(Ordering::Acquire) >= before,
                            "a sync returned before its writes were covered"
                        );
                    }
                });
            }
            for _ in 0..2000 {
                gate.note_write();
                std::thread::yield_now();
            }
            stop.store(true, Ordering::Release);
        });
    }
}
