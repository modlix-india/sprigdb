//! One segment file of a partition: record batches as stored, back to
//! back, named by the first offset it may hold (`<base>.log`, 20 digits).
//!
//! The offset and time index is sparse and lives in memory: one entry at
//! the segment's first batch and then at least every `index_interval`
//! bytes, rebuilt from the batch heads when the file is opened. A lookup
//! starts at the floor entry and reads heads forward from there.
//!
//! **Write-behind.** An append keeps the batch in memory and returns; the
//! stream log's writer moves pending batches into the file in one write
//! per segment, off the apply path (which runs on the replication loop).
//! Readers see pending batches as if they were in the file. Whoever syncs
//! the file or reads it directly writes the pending bytes out first
//! ([`Segment::write_out`]). A pending batch lost in a crash is no worse
//! than an unsynced one: the commit log keeps every entry above the
//! durable floor and the log replays them.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::batch::{HEAD, Head};

/// Pending bytes past which an append writes them out itself (bounds the
/// memory a slow or failing writer can pin).
pub const PENDING_MAX: u64 = 4 << 20;

/// Batches not in the file yet: (position, bytes), back to back.
type Chunks = Vec<(u64, Arc<[u8]>)>;

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
    /// Bytes appended (in the file or pending).
    pub len: u64,
    /// Bytes known synced.
    pub synced: u64,
    /// Bytes in the file; `pending` holds the rest up to `len`.
    written: u64,
    pending: Chunks,
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
    /// Bytes in the file when the view was taken; the rest is in `tail`.
    written: u64,
    tail: Chunks,
}

/// A segment's pending batches, to write outside the partition lock
/// ([`Segment::pending_write`], then [`Segment::written_through`]).
#[derive(Debug)]
pub struct PendingWrite {
    file: Arc<File>,
    /// The segment's base offset.
    pub base: i64,
    /// Where the bytes go.
    pub at: u64,
    chunks: Chunks,
}

impl PendingWrite {
    /// The position after the last pending byte.
    #[must_use]
    pub fn end(&self) -> u64 {
        self.chunks
            .last()
            .map_or(self.at, |(p, b)| p + b.len() as u64)
    }

    /// Writes the bytes into the file (one write).
    ///
    /// # Errors
    /// The file system's.
    pub fn write(&self) -> io::Result<()> {
        write_chunks(&self.file, self.at, &self.chunks)
    }
}

fn write_chunks(file: &File, at: u64, chunks: &[(u64, Arc<[u8]>)]) -> io::Result<()> {
    match chunks {
        [] => Ok(()),
        [(_, b)] => file.write_all_at(b, at),
        _ => {
            let n: usize = chunks.iter().map(|(_, b)| b.len()).sum();
            let mut buf = Vec::with_capacity(n);
            for (_, b) in chunks {
                buf.extend_from_slice(b);
            }
            file.write_all_at(&buf, at)
        }
    }
}

/// Fills `buf` from position `pos`: from the file below `written`, from
/// `tail` (pending batches) from there on.
fn fill(file: &File, written: u64, tail: &[(u64, Arc<[u8]>)], buf: &mut [u8], pos: u64) -> io::Result<()> {
    let mut at = pos;
    let mut done = 0usize;
    if at < written {
        let n = usize::try_from((written - at).min(buf.len() as u64)).unwrap_or(buf.len());
        file.read_exact_at(&mut buf[..n], at)?;
        done = n;
        at += n as u64;
    }
    while done < buf.len() {
        let i = tail.partition_point(|(p, b)| p + b.len() as u64 <= at);
        let Some((p, b)) = tail.get(i).filter(|(p, _)| *p <= at) else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past a segment's end",
            ));
        };
        let off = usize::try_from(at - p).unwrap_or(usize::MAX);
        let n = (b.len() - off).min(buf.len() - done);
        buf[done..done + n].copy_from_slice(&b[off..off + n]);
        done += n;
        at += n as u64;
    }
    Ok(())
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
            written: 0,
            pending: Vec::new(),
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
            written: 0,
            pending: Vec::new(),
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
        s.written = pos;
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

    /// Appends one batch whose head is `h` (already patched). It stays
    /// pending (in memory) until a writer moves it into the file; past
    /// [`PENDING_MAX`] pending bytes the append writes them out first.
    ///
    /// # Errors
    /// The file system's (the batch is then not appended).
    pub fn append(&mut self, bytes: &[u8], h: &Head) -> io::Result<()> {
        if self.len - self.written + bytes.len() as u64 > PENDING_MAX {
            self.write_out()?;
        }
        let pos = self.len;
        self.pending.push((pos, Arc::from(bytes)));
        self.note(pos, h);
        self.len += bytes.len() as u64;
        Ok(())
    }

    /// Writes every pending batch into the file.
    ///
    /// # Errors
    /// The file system's (the batches stay pending).
    pub fn write_out(&mut self) -> io::Result<()> {
        write_chunks(&self.file, self.written, &self.pending)?;
        self.written = self.len;
        self.pending.clear();
        Ok(())
    }

    /// Pending bytes (appended, not in the file yet).
    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.len - self.written
    }

    /// The pending batches, to write outside the partition lock; `None`
    /// when there are none.
    #[must_use]
    pub fn pending_write(&self) -> Option<PendingWrite> {
        (!self.pending.is_empty()).then(|| PendingWrite {
            file: Arc::clone(&self.file),
            base: self.base,
            at: self.written,
            chunks: self.pending.clone(),
        })
    }

    /// Records that a [`PendingWrite`] taken at `at` put everything before
    /// `end` in the file (ignored when another writer got there first).
    pub fn written_through(&mut self, at: u64, end: u64) {
        if self.written != at || end > self.len {
            return;
        }
        let k = self.pending.partition_point(|(p, _)| *p < end);
        self.pending.drain(..k);
        self.written = end;
    }

    /// Writes pending batches out, then syncs the file; returns the length
    /// synced.
    ///
    /// # Errors
    /// The file system's.
    pub fn sync(&mut self) -> io::Result<u64> {
        self.write_out()?;
        let len = self.len;
        self.file.sync_data()?;
        Ok(len)
    }

    /// The file handle (for syncing or reading outside the partition
    /// lock). The file holds only what was written: call
    /// [`Segment::write_out`] first.
    #[must_use]
    pub fn handle(&self) -> Arc<File> {
        Arc::clone(&self.file)
    }

    /// The batch head at `pos`, from the file or the pending batches.
    fn head_at(&self, pos: u64) -> io::Result<Option<Head>> {
        if pos + HEAD as u64 > self.len {
            return Ok(None);
        }
        if pos < self.written {
            return read_head(&self.file, pos);
        }
        let mut h = [0u8; HEAD];
        fill(&self.file, self.written, &self.pending, &mut h, pos)?;
        Ok(Head::parse(&h))
    }

    /// A view from `pos` (a batch head).
    fn view_from(&self, pos: u64) -> View {
        let from = self.pending.partition_point(|(p, b)| p + b.len() as u64 <= pos);
        View {
            file: Arc::clone(&self.file),
            len: self.len,
            pos,
            written: self.written,
            tail: self.pending[from..].to_vec(),
        }
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
            let Some(h) = self.head_at(pos)? else {
                break;
            };
            if h.next_offset() > offset {
                return Ok(Some(self.view_from(pos)));
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
            let Some(h) = self.head_at(pos)? else {
                break;
            };
            if h.max_ts >= ts {
                return Ok(Some((self.view_from(pos), h)));
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
            let Some(h) = self.head_at(pos)? else {
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
        fill(&self.file, self.written, &self.pending, &mut buf, pos)?;
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
        fill(&self.file, self.written, &self.tail, &mut buf, self.pos)?;
        // Keep whole batches; the first one whole whatever its size.
        let mut end = 0usize;
        loop {
            let Some(h) = Head::parse(&buf[end..]) else {
                break;
            };
            if end + h.total > buf.len() {
                if end == 0 {
                    let mut first = vec![0u8; h.total];
                    fill(&self.file, self.written, &self.tail, &mut first, self.pos)?;
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
        assert_eq!(s.pending_bytes(), s.len, "appends stay pending until written");
        s.write_out().unwrap();
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

    /// Reads see pending batches, across the boundary with written ones,
    /// and a view keeps reading what it saw after a writer moved it.
    #[test]
    fn pending_batches_read_like_written_ones() {
        let d = TempDir::new("segment-pending");
        let mut s = Segment::create(d.path(), 0, 0, 100).unwrap();
        let mut off = 0;
        let mut all = Vec::new();
        let mut append = |s: &mut Segment, i: i64| {
            let mut b = batch_bytes(2, 500 + i, 40);
            set_offset_and_epoch(&mut b, off, 1);
            let h = Head::parse(&b).unwrap();
            s.append(&b, &h).unwrap();
            off = h.next_offset();
            all.extend_from_slice(&b);
        };
        for i in 0..5 {
            append(&mut s, i);
        }
        // Half written by a background writer, the rest pending.
        let w = s.pending_write().unwrap();
        w.write().unwrap();
        s.written_through(w.at, w.end());
        assert_eq!(s.pending_bytes(), 0);
        for i in 5..10 {
            append(&mut s, i);
        }
        assert_eq!(std::fs::metadata(&s.path).unwrap().len(), w.end());
        let v = s.view_at(0).unwrap().unwrap();
        assert_eq!(v.read(usize::MAX >> 1).unwrap(), all);
        let heads = s.heads().unwrap();
        assert_eq!(heads.len(), 10);
        for (pos, h) in &heads {
            let p = usize::try_from(*pos).unwrap();
            assert_eq!(s.batch_at(*pos, h).unwrap(), all[p..p + h.total]);
        }
        let (tv, th) = s.view_at_time(507).unwrap().unwrap();
        assert_eq!(th.base_offset, 12);
        // A writer that lost the race to `write_out` changes nothing.
        let late = s.pending_write().unwrap();
        s.write_out().unwrap();
        late.write().unwrap();
        s.written_through(late.at, late.end());
        assert_eq!(s.pending_bytes(), 0);
        assert_eq!(std::fs::read(&s.path).unwrap(), all);
        // Views taken before the write still read their bytes.
        assert_eq!(v.read(usize::MAX >> 1).unwrap(), all);
        let p = usize::try_from(tv.pos).unwrap();
        assert_eq!(tv.read(usize::MAX >> 1).unwrap(), all[p..]);
        // Past the cap an append writes what is pending itself.
        let big = usize::try_from(PENDING_MAX / 3).unwrap();
        for _ in 0..4 {
            let mut b = batch_bytes(1, 900, big);
            set_offset_and_epoch(&mut b, off, 1);
            let h = Head::parse(&b).unwrap();
            s.append(&b, &h).unwrap();
            off = h.next_offset();
        }
        assert!(s.pending_bytes() <= PENDING_MAX);
        assert!(std::fs::metadata(&s.path).unwrap().len() > all.len() as u64);
    }
}
