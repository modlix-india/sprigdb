//! One segment file of a partition: record batches as stored, back to
//! back, named by the first offset it may hold (`<base>.log`, 20 digits).
//!
//! The offset and time index is sparse and lives in memory: one entry at
//! the segment's first batch and then at least every `index_interval`
//! bytes, rebuilt from the batch heads when the file is opened. A lookup
//! starts at the floor entry and reads heads forward from there.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::batch::{HEAD, Head};

/// One sparse index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    /// The base offset of the batch at `pos`.
    offset: i64,
    /// Its position.
    pos: u64,
    /// The largest timestamp of every batch before `pos` (-1: none).
    max_ts_before: i64,
}

/// See the module docs.
#[derive(Debug)]
pub struct Segment {
    /// The first offset it may hold.
    pub base: i64,
    /// Its file.
    pub path: PathBuf,
    file: Arc<File>,
    /// Bytes written.
    pub len: u64,
    /// Bytes known synced.
    pub synced: u64,
    /// The offset after its last batch (`base` while empty).
    pub next: i64,
    /// The largest batch timestamp (-1 while empty).
    pub max_ts: i64,
    /// When it was created (ms).
    pub created_ms: i64,
    index: Vec<Entry>,
    since_entry: u64,
    interval: u64,
}

/// A read-only view of a segment for reading outside the partition lock.
#[derive(Debug, Clone)]
pub struct View {
    file: Arc<File>,
    /// Readable bytes.
    pub len: u64,
    /// Where to start (a batch head).
    pub pos: u64,
}

/// The name of the segment starting at `base`.
#[must_use]
pub fn file_name(base: i64) -> String {
    format!("{base:020}.log")
}

/// The base offset a segment file name gives.
#[must_use]
pub fn parse_name(name: &str) -> Option<i64> {
    name.strip_suffix(".log")
        .filter(|n| n.len() == 20)
        .and_then(|n| n.parse().ok())
}

fn read_head(file: &File, pos: u64) -> io::Result<Option<Head>> {
    let mut h = [0u8; HEAD];
    let mut got = 0;
    while got < HEAD {
        match file.read_at(&mut h[got..], pos + got as u64)? {
            0 => return Ok(None),
            n => got += n,
        }
    }
    Ok(Head::parse(&h))
}

impl Segment {
    /// Creates an empty segment file (replacing a stale one).
    ///
    /// # Errors
    /// The file system's.
    pub fn create(dir: &Path, base: i64, now_ms: i64, interval: u64) -> io::Result<Segment> {
        let path = dir.join(file_name(base));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Segment {
            base,
            path,
            file: Arc::new(file),
            len: 0,
            synced: 0,
            next: base,
            max_ts: -1,
            created_ms: now_ms,
            index: Vec::new(),
            since_entry: 0,
            interval: interval.max(1),
        })
    }

    /// Opens a segment, reading every batch head to rebuild the index.
    /// The file is cut at `keep` bytes when given (what the manifest knows
    /// was written), and at the first incomplete or malformed batch.
    ///
    /// # Errors
    /// The file system's.
    pub fn open(
        path: &Path,
        base: i64,
        keep: Option<u64>,
        created_ms: i64,
        interval: u64,
    ) -> io::Result<Segment> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let size = file.metadata()?.len();
        let s = Self::scan(
            file,
            path,
            base,
            keep.map_or(size, |k| k.min(size)),
            created_ms,
            interval,
        )?;
        if size != s.len {
            s.file.set_len(s.len)?;
        }
        Ok(s)
    }

    /// Opens a segment for reading only, as far as it is written now
    /// (another holder may be appending to it).
    ///
    /// # Errors
    /// The file system's.
    pub fn open_read(path: &Path, base: i64, interval: u64) -> io::Result<Segment> {
        let file = OpenOptions::new().read(true).open(path)?;
        let size = file.metadata()?.len();
        Self::scan(file, path, base, size, 0, interval)
    }

    fn scan(
        file: File,
        path: &Path,
        base: i64,
        limit: u64,
        created_ms: i64,
        interval: u64,
    ) -> io::Result<Segment> {
        let mut s = Segment {
            base,
            path: path.to_path_buf(),
            file: Arc::new(file),
            len: 0,
            synced: 0,
            next: base,
            max_ts: -1,
            created_ms,
            index: Vec::new(),
            since_entry: 0,
            interval: interval.max(1),
        };
        let mut pos = 0u64;
        while pos + HEAD as u64 <= limit {
            let Some(h) = read_head(&s.file, pos)? else {
                break;
            };
            if pos + h.total as u64 > limit || h.base_offset < s.next {
                break;
            }
            s.note(pos, &h);
            pos += h.total as u64;
        }
        s.len = pos;
        s.synced = pos;
        Ok(s)
    }

    fn note(&mut self, pos: u64, h: &Head) {
        if self.index.is_empty() || self.since_entry >= self.interval {
            self.index.push(Entry {
                offset: h.base_offset,
                pos,
                max_ts_before: self.max_ts,
            });
            self.since_entry = 0;
        }
        self.since_entry += h.total as u64;
        self.next = h.next_offset();
        self.max_ts = self.max_ts.max(h.max_ts);
    }

    /// Appends one batch whose head is `h` (already patched).
    ///
    /// # Errors
    /// The file system's.
    pub fn append(&mut self, bytes: &[u8], h: &Head) -> io::Result<()> {
        self.file.write_all_at(bytes, self.len)?;
        let pos = self.len;
        self.note(pos, h);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Syncs the file; returns the length synced.
    ///
    /// # Errors
    /// The file system's.
    pub fn sync(&self) -> io::Result<u64> {
        let len = self.len;
        self.file.sync_data()?;
        Ok(len)
    }

    /// The file handle (for syncing outside the partition lock).
    #[must_use]
    pub fn handle(&self) -> Arc<File> {
        Arc::clone(&self.file)
    }

    /// Whether it holds no batch.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Where reading for `offset` starts: the floor index entry.
    fn floor(&self, offset: i64) -> u64 {
        let i = self.index.partition_point(|e| e.offset <= offset);
        if i == 0 { 0 } else { self.index[i - 1].pos }
    }

    /// A view from the batch holding `offset` (or the first batch after
    /// it, across a compaction gap); `None` when nothing at or after
    /// `offset` is in this segment.
    ///
    /// # Errors
    /// The file system's.
    pub fn view_at(&self, offset: i64) -> io::Result<Option<View>> {
        if offset >= self.next {
            return Ok(None);
        }
        let mut pos = self.floor(offset);
        while pos < self.len {
            let Some(h) = read_head(&self.file, pos)? else {
                break;
            };
            if h.next_offset() > offset {
                return Ok(Some(View {
                    file: Arc::clone(&self.file),
                    len: self.len,
                    pos,
                }));
            }
            pos += h.total as u64;
        }
        Ok(None)
    }

    /// A view of the first batch whose max timestamp is at least `ts`.
    ///
    /// # Errors
    /// The file system's.
    pub fn view_at_time(&self, ts: i64) -> io::Result<Option<(View, Head)>> {
        if self.max_ts < ts {
            return Ok(None);
        }
        let i = self.index.partition_point(|e| e.max_ts_before < ts);
        let mut pos = if i == 0 { 0 } else { self.index[i - 1].pos };
        while pos < self.len {
            let Some(h) = read_head(&self.file, pos)? else {
                break;
            };
            if h.max_ts >= ts {
                return Ok(Some((
                    View {
                        file: Arc::clone(&self.file),
                        len: self.len,
                        pos,
                    },
                    h,
                )));
            }
            pos += h.total as u64;
        }
        Ok(None)
    }

    /// Every batch head with its position (compaction, the largest
    /// timestamp, tests).
    ///
    /// # Errors
    /// The file system's.
    pub fn heads(&self) -> io::Result<Vec<(u64, Head)>> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < self.len {
            let Some(h) = read_head(&self.file, pos)? else {
                break;
            };
            out.push((pos, h));
            pos += h.total as u64;
        }
        Ok(out)
    }

    /// The bytes of the batch at `pos` (its head says how many).
    ///
    /// # Errors
    /// The file system's.
    pub fn batch_at(&self, pos: u64, h: &Head) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; h.total];
        self.file.read_exact_at(&mut buf, pos)?;
        Ok(buf)
    }
}

impl View {
    /// Whole batches from the view's position, at most `max_bytes` of them
    /// except that the first batch comes whatever its size.
    ///
    /// # Errors
    /// The file system's.
    pub fn read(&self, max_bytes: usize) -> io::Result<Vec<u8>> {
        let avail = self.len.saturating_sub(self.pos);
        let want = (max_bytes as u64).min(avail).max(HEAD as u64).min(avail);
        let mut buf = vec![0u8; usize::try_from(want).unwrap_or(usize::MAX)];
        self.file.read_exact_at(&mut buf, self.pos)?;
        // Keep whole batches; the first one whole whatever its size.
        let mut end = 0usize;
        loop {
            let Some(h) = Head::parse(&buf[end..]) else {
                break;
            };
            if end + h.total > buf.len() {
                if end == 0 {
                    let mut first = vec![0u8; h.total];
                    self.file.read_exact_at(&mut first, self.pos)?;
                    return Ok(first);
                }
                break;
            }
            end += h.total;
            if end + HEAD > buf.len() {
                break;
            }
        }
        buf.truncate(end);
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seg::batch::set_offset_and_epoch;
    use crate::seg::testing::{TempDir, batch_bytes};

    #[test]
    fn append_find_reopen_and_cut() {
        let d = TempDir::new("segment");
        let mut s = Segment::create(d.path(), 100, 0, 600).unwrap();
        let mut off = 100;
        for i in 0..20 {
            let mut b = batch_bytes(3, 1_000 + i, 50);
            set_offset_and_epoch(&mut b, off, 1);
            let h = Head::parse(&b).unwrap();
            s.append(&b, &h).unwrap();
            off = h.next_offset();
        }
        assert_eq!(s.next, 160);
        assert!(s.index.len() > 2 && s.index.len() < 20);
        let v = s.view_at(131).unwrap().unwrap();
        let got = v.read(1).unwrap();
        let h = Head::parse(&got).unwrap();
        assert_eq!((h.base_offset, got.len()), (130, h.total));
        assert!(s.view_at(160).unwrap().is_none());
        // Batch i holds timestamps 1000 + i .. 1000 + i + 2.
        let (_, h) = s.view_at_time(1_010).unwrap().unwrap();
        assert_eq!(h.base_offset, 124);
        // Reopen with a torn tail: it is cut at the last whole batch.
        let full = s.len;
        let path = s.path.clone();
        drop(s);
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 5).unwrap();
        let s = Segment::open(&path, 100, None, 0, 600).unwrap();
        assert_eq!(s.next, 157);
        let s = Segment::open(&path, 100, Some(0), 0, 600).unwrap();
        assert_eq!((s.next, s.len), (100, 0));
    }
}
