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

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

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
}

impl LogFs {
    /// Real files with the given sync mode, optionally under a crash
    /// simulation.
    #[must_use]
    pub fn new(mode: SyncMode, sim: Option<Arc<SimDisk>>) -> Self {
        Self { mode, sim }
    }
}

/// An open log file.
#[derive(Debug)]
pub struct LogHandle {
    file: File,
    path: PathBuf,
    mode: SyncMode,
    sim: Option<Arc<SimDisk>>,
}

impl Handle for LogHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.file.set_len(offset as u64)
    }

    fn file_size(&self) -> IoResult<usize> {
        usize::try_from(self.file.metadata()?.len()).map_err(std::io::Error::other)
    }

    fn sync(&self) -> IoResult<()> {
        if self.mode == SyncMode::Full {
            self.file.sync_data()?;
        }
        if let Some(sim) = &self.sim {
            sim.note_sync(&self.path, self.file.metadata()?.len());
        }
        Ok(())
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
        let n = self.h.file.read_at(buf, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl Write for LogIo {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let n = self.h.file.write_at(buf, self.offset)?;
        self.offset += n as u64;
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
            SeekFrom::End(d) => self.h.file.metadata()?.len().checked_add_signed(d),
        };
        self.offset = to.ok_or_else(|| std::io::Error::other("seek out of range"))?;
        Ok(self.offset)
    }
}

impl WriteExt for LogIo {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.h.file.set_len(offset as u64)?;
        self.offset = offset as u64;
        Ok(())
    }

    fn allocate(&mut self, _offset: usize, _size: usize) -> IoResult<()> {
        Ok(())
    }
}

impl LogFs {
    fn handle(&self, file: File, path: &Path) -> LogHandle {
        LogHandle {
            file,
            path: path.to_path_buf(),
            mode: self.mode,
            sim: self.sim.clone(),
        }
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
        Ok(self.handle(f, p))
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> IoResult<LogHandle> {
        let p = path.as_ref();
        let f = OpenOptions::new()
            .read(true)
            .write(perm == Permission::ReadWrite)
            .open(p)?;
        Ok(self.handle(f, p))
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
