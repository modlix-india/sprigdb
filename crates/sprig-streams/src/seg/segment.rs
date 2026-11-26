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
//!
//! **Blob segments** (`<base>.ref`). On a replicated node the log store
//! already wrote each append's batch once, into a note blob file
//! ([`sprig_engine::log::blobs`]). A blob segment keeps, per batch, only
//! its head as stored (offset and epoch written in) and where the batch
//! is in the blob file; its file holds those records
//! (`head | blob file id u64 | position u64`, little-endian). Readers see
//! the same bytes as from a plain segment: positions, lengths and the
//! index count the batches' bytes, not the records'.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sprig_engine::log::blobs::BlobFile;

use super::batch::{HEAD, Head};

/// Pending bytes past which an append writes them out itself (bounds the
/// memory a slow or failing writer can pin).
pub const PENDING_MAX: u64 = 4 << 20;

/// Batches not in the file yet: (position, bytes), back to back.
type Chunks = Vec<(u64, Arc<[u8]>)>;

/// A blob segment's record: the stored head, the blob file, the position.
const REF_REC: usize = HEAD + 16;

/// A view of a blob segment carries batches up to this many bytes (a
/// read returns fewer than it may when asked for more).
const VIEW_REFS_BYTES: u64 = 16 << 20;

/// Where a batch's bytes are when a note blob holds them: the batch, as
/// the producer sent it, starts at `at` in `file`.
#[derive(Debug, Clone)]
pub struct BlobBatch {
    /// The blob file.
    pub file: Arc<BlobFile>,
    /// Where the batch starts in it.
    pub at: u64,
}

/// One batch of a blob segment.
#[derive(Debug, Clone)]
struct RefEntry {
    /// Its position among the segment's batches.
    pos: u64,
    /// Its length.
    total: u64,
    /// Its head as stored.
    head: [u8; HEAD],
    /// Where the rest is.
    blob: BlobBatch,
}

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

/// See the module docs. A clone reads what the segment held when it was
/// taken ([`Segment::read_copy`]); only the original appends.
#[derive(Debug, Clone)]
pub struct Segment {
    /// The first offset it may hold.
    pub base: i64,
    /// Its file.
    pub path: PathBuf,
    file: Arc<File>,
    /// Bytes of its batches (in the file, pending, or in blob files).
    pub len: u64,
    /// Bytes of its file, pending ones included (`len` for a plain
    /// segment; records for a blob segment).
    pub flen: u64,
    /// File bytes known synced.
    pub synced: u64,
    /// File bytes written; `pending` holds the rest up to `flen`.
    written: u64,
    pending: Chunks,
    /// A blob segment's batches, in order.
    refs: Option<Vec<RefEntry>>,
    /// The blob files a blob segment refers to.
    blob_ids: BTreeSet<u64>,
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

/// Where a view's bytes come from.
#[derive(Debug, Clone)]
enum Src {
    /// A plain segment's file below `written`, its pending batches from
    /// there on.
    Data {
        file: Arc<File>,
        written: u64,
        tail: Chunks,
    },
    /// A blob segment's batches from the view's position.
    Refs(Vec<RefEntry>),
}

impl Src {
    fn fill(&self, buf: &mut [u8], pos: u64) -> io::Result<()> {
        match self {
            Src::Data {
                file,
                written,
                tail,
            } => fill(file, *written, tail, buf, pos),
            Src::Refs(r) => fill_refs(r, buf, pos),
        }
    }
}

/// A read-only view of a segment for reading outside the partition lock.
#[derive(Debug, Clone)]
pub struct View {
    src: Src,
    /// Readable bytes.
    pub len: u64,
    /// Where to start (a batch head).
    pub pos: u64,
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
fn fill(
    file: &File,
    written: u64,
    tail: &[(u64, Arc<[u8]>)],
    buf: &mut [u8],
    pos: u64,
) -> io::Result<()> {
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

/// Fills `buf` from position `pos` of a blob segment's batches: heads from
/// the records, the rest from the blob files.
fn fill_refs(refs: &[RefEntry], buf: &mut [u8], pos: u64) -> io::Result<()> {
    let mut i = refs.partition_point(|e| e.pos + e.total <= pos);
    let mut at = pos;
    let mut done = 0usize;
    while done < buf.len() {
        let Some(e) = refs.get(i).filter(|e| e.pos <= at) else {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past a segment's end",
            ));
        };
        let mut off = at - e.pos;
        let end = (e.total - off).min((buf.len() - done) as u64);
        let stop = off + end;
        if off < HEAD as u64 {
            let h = usize::try_from(stop.min(HEAD as u64)).unwrap_or(HEAD);
            let o = usize::try_from(off).unwrap_or(0);
            buf[done..done + h - o].copy_from_slice(&e.head[o..h]);
            done += h - o;
            off = h as u64;
        }
        if off < stop {
            let n = usize::try_from(stop - off).unwrap_or(0);
            e.blob
                .file
                .read_exact_at(&mut buf[done..done + n], e.blob.at + off)?;
            done += n;
        }
        at = e.pos + stop;
        if stop == e.total {
            i += 1;
        }
    }
    Ok(())
}

/// The name of the segment starting at `base`.
#[must_use]
pub fn file_name(base: i64) -> String {
    format!("{base:020}.log")
}

/// The name of the blob segment starting at `base`.
#[must_use]
pub fn ref_file_name(base: i64) -> String {
    format!("{base:020}.ref")
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
        Self::create_kind(dir, base, now_ms, interval, false)
    }

    /// Creates an empty blob segment (replacing a stale one).
    ///
    /// # Errors
    /// The file system's.
    pub fn create_refs(dir: &Path, base: i64, now_ms: i64, interval: u64) -> io::Result<Segment> {
        Self::create_kind(dir, base, now_ms, interval, true)
    }

    fn create_kind(
        dir: &Path,
        base: i64,
        now_ms: i64,
        interval: u64,
        refs: bool,
    ) -> io::Result<Segment> {
        let path = dir.join(if refs {
            ref_file_name(base)
        } else {
            file_name(base)
        });
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
            flen: 0,
            synced: 0,
            written: 0,
            pending: Vec::new(),
            refs: refs.then(Vec::new),
            blob_ids: BTreeSet::new(),
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

    /// Opens a blob segment: reads its records (cut at `keep` bytes when
    /// given, and at the first incomplete or malformed one), finding each
    /// one's blob file with `blob`.
    ///
    /// # Errors
    /// The file system's; `InvalidData` when a blob file it refers to is
    /// gone.
    pub fn open_refs(
        path: &Path,
        base: i64,
        keep: Option<u64>,
        created_ms: i64,
        interval: u64,
        blob: &dyn Fn(u64) -> Option<Arc<BlobFile>>,
    ) -> io::Result<Segment> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let size = file.metadata()?.len();
        let limit = keep.map_or(size, |k| k.min(size));
        let mut s = Segment {
            base,
            path: path.to_path_buf(),
            file: Arc::new(file),
            len: 0,
            flen: 0,
            synced: 0,
            written: 0,
            pending: Vec::new(),
            refs: Some(Vec::new()),
            blob_ids: BTreeSet::new(),
            next: base,
            max_ts: -1,
            created_ms,
            index: Vec::new(),
            since_entry: 0,
            interval: interval.max(1),
        };
        let mut body = vec![0u8; usize::try_from(limit).unwrap_or(0)];
        s.file.read_exact_at(&mut body, 0)?;
        for rec in body.chunks_exact(REF_REC) {
            let Some(h) = Head::parse(&rec[..HEAD]) else {
                break;
            };
            if h.base_offset < s.next {
                break;
            }
            let id = u64::from_le_bytes(rec[HEAD..HEAD + 8].try_into().unwrap_or([0; 8]));
            let at = u64::from_le_bytes(rec[HEAD + 8..].try_into().unwrap_or([0; 8]));
            let file = blob(id).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: note blob file {id} is gone", path.display()),
                )
            })?;
            let mut head = [0u8; HEAD];
            head.copy_from_slice(&rec[..HEAD]);
            s.push_ref(head, &h, BlobBatch { file, at });
            s.flen += REF_REC as u64;
        }
        s.synced = s.flen;
        s.written = s.flen;
        if size != s.flen {
            s.file.set_len(s.flen)?;
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
            flen: 0,
            synced: 0,
            written: 0,
            pending: Vec::new(),
            refs: None,
            blob_ids: BTreeSet::new(),
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
        s.flen = pos;
        s.synced = pos;
        s.written = pos;
        Ok(s)
    }

    /// Whether it is a blob segment.
    #[must_use]
    pub fn is_refs(&self) -> bool {
        self.refs.is_some()
    }

    /// The blob files a blob segment refers to.
    #[must_use]
    pub fn blob_ids(&self) -> &BTreeSet<u64> {
        &self.blob_ids
    }

    /// A copy that reads what the segment holds now, outside the
    /// partition lock (a plain segment's pending batches included).
    #[must_use]
    pub fn read_copy(&self) -> Segment {
        self.clone()
    }

    fn push_ref(&mut self, head: [u8; HEAD], h: &Head, blob: BlobBatch) {
        let pos = self.len;
        self.blob_ids.insert(blob.file.id());
        if let Some(r) = self.refs.as_mut() {
            r.push(RefEntry {
                pos,
                total: h.total as u64,
                head,
                blob,
            });
        }
        self.note(pos, h);
        self.len += h.total as u64;
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
        if self.is_refs() {
            return Err(io::Error::other(
                "a batch's bytes appended to a blob segment",
            ));
        }
        if self.flen - self.written + bytes.len() as u64 > PENDING_MAX {
            self.write_out()?;
        }
        let pos = self.len;
        self.pending.push((pos, Arc::from(bytes)));
        self.note(pos, h);
        self.len += bytes.len() as u64;
        self.flen = self.len;
        Ok(())
    }

    /// Appends one batch whose bytes `blob` holds, with `head` its stored
    /// head (already patched; `h` parsed from it). Only its record goes in
    /// the file, pending like a plain segment's batches.
    ///
    /// # Errors
    /// The file system's (the batch is then not appended).
    pub fn append_ref(&mut self, head: &[u8], h: &Head, blob: BlobBatch) -> io::Result<()> {
        if !self.is_refs() || head.len() < HEAD {
            return Err(io::Error::other(
                "a blob reference appended to a plain segment",
            ));
        }
        if self.flen - self.written + REF_REC as u64 > PENDING_MAX {
            self.write_out()?;
        }
        let mut rec = Vec::with_capacity(REF_REC);
        rec.extend_from_slice(&head[..HEAD]);
        rec.extend_from_slice(&blob.file.id().to_le_bytes());
        rec.extend_from_slice(&blob.at.to_le_bytes());
        self.pending.push((self.flen, Arc::from(rec)));
        let mut hb = [0u8; HEAD];
        hb.copy_from_slice(&head[..HEAD]);
        self.push_ref(hb, h, blob);
        self.flen += REF_REC as u64;
        Ok(())
    }

    /// Writes every pending batch (or record) into the file.
    ///
    /// # Errors
    /// The file system's (the batches stay pending).
    pub fn write_out(&mut self) -> io::Result<()> {
        write_chunks(&self.file, self.written, &self.pending)?;
        self.written = self.flen;
        self.pending.clear();
        Ok(())
    }

    /// Pending file bytes (appended, not in the file yet).
    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.flen - self.written
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
        if self.written != at || end > self.flen {
            return;
        }
        let k = self.pending.partition_point(|(p, _)| *p < end);
        self.pending.drain(..k);
        self.written = end;
    }

    /// Writes pending batches out, then syncs the file; returns the file
    /// length synced.
    ///
    /// # Errors
    /// The file system's.
    pub fn sync(&mut self) -> io::Result<u64> {
        self.write_out()?;
        let len = self.flen;
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
        if let Some(r) = &self.refs {
            let i = r.partition_point(|e| e.pos < pos);
            return Ok(r
                .get(i)
                .filter(|e| e.pos == pos)
                .and_then(|e| Head::parse(&e.head)));
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
        if let Some(r) = &self.refs {
            let from = r.partition_point(|e| e.pos < pos);
            let mut to = from;
            while to < r.len() && (to == from || r[to].pos - pos < VIEW_REFS_BYTES) {
                to += 1;
            }
            let len = r.get(to).map_or(self.len, |e| e.pos);
            return View {
                src: Src::Refs(r[from..to].to_vec()),
                len,
                pos,
            };
        }
        let from = self
            .pending
            .partition_point(|(p, b)| p + b.len() as u64 <= pos);
        View {
            src: Src::Data {
                file: Arc::clone(&self.file),
                written: self.written,
                tail: self.pending[from..].to_vec(),
            },
            len: self.len,
            pos,
        }
    }

    /// Fills `buf` with the batches' bytes from `pos`.
    ///
    /// # Errors
    /// The file system's, or `pos` and `buf` reach past the end.
    pub fn read_bytes(&self, buf: &mut [u8], pos: u64) -> io::Result<()> {
        match &self.refs {
            Some(r) => fill_refs(r, buf, pos),
            None => fill(&self.file, self.written, &self.pending, buf, pos),
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
        self.read_bytes(&mut buf, pos)?;
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
        self.src.fill(&mut buf, self.pos)?;
        // Keep whole batches; the first one whole whatever its size.
        let mut end = 0usize;
        loop {
            let Some(h) = Head::parse(&buf[end..]) else {
                break;
            };
            if end + h.total > buf.len() {
                if end == 0 {
                    let mut first = vec![0u8; h.total];
                    self.src.fill(&mut first, self.pos)?;
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
        assert_eq!(
            s.pending_bytes(),
            s.len,
            "appends stay pending until written"
        );
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

    /// A blob segment reads exactly like a plain one holding the same
    /// batches (views, heads, times, copies), its file holds only records,
    /// and it reopens (cut at a recorded length) from its records.
    #[test]
    #[allow(clippy::too_many_lines)] // one scenario, step by step
    fn blob_segments_read_like_plain_ones() {
        use sprig_engine::log::Payload;
        use sprig_engine::log::SyncMode;
        use sprig_engine::log::blobs::NoteBlobs;

        use crate::seg::note;
        use crate::seg::part::PartKey;

        let d = TempDir::new("segment-blobs");
        let blobs =
            NoteBlobs::open(d.path().join("notes"), SyncMode::Off, note::is_append, true).unwrap();
        let key = PartKey {
            tenant: 1,
            stream: 2,
            partition: 0,
        };
        let mut plain = Segment::create(d.path(), 0, 0, 700).unwrap();
        let mut refs = Segment::create_refs(d.path(), 0, 0, 700).unwrap();
        assert!(refs.is_refs() && !plain.is_refs());
        let mut off = 0;
        let mut ends = Vec::new();
        for i in 0..20u64 {
            let mut b = batch_bytes(3, 1_000 + i64::try_from(i).unwrap(), 100);
            let n = note::encode_append(key, 0, false, &b);
            let mut p = Payload::new();
            p.note(&n);
            let data = p.into_bytes();
            assert!(blobs.take_out(9, i + 1, 1, &data).unwrap().is_some());
            blobs.persist(true).unwrap();
            let loc = blobs.take_located(9, i + 1, 1)[0].1;
            let blob = BlobBatch {
                file: blobs.file(loc.file).unwrap(),
                at: loc.at + (n.len() - b.len()) as u64,
            };
            set_offset_and_epoch(&mut b, off, 4);
            let h = Head::parse(&b).unwrap();
            plain.append(&b, &h).unwrap();
            refs.append_ref(&b, &h, blob).unwrap();
            off = h.next_offset();
            ends.push(refs.flen);
        }
        let h0 = plain.heads().unwrap()[0].1;
        let b0 = plain.batch_at(0, &h0).unwrap();
        assert!(
            refs.append(&b0, &h0).is_err(),
            "bytes never go in a blob segment"
        );
        assert_eq!(
            (refs.len, refs.next, refs.max_ts),
            (plain.len, plain.next, plain.max_ts)
        );
        assert_eq!(refs.flen, 20 * REF_REC as u64, "only records in its file");
        assert_eq!(refs.blob_ids().len(), 1);
        let all = |s: &Segment| {
            s.view_at(0)
                .unwrap()
                .unwrap()
                .read(usize::MAX >> 1)
                .unwrap()
        };
        assert_eq!(all(&refs), all(&plain));
        for o in [0, 7, 31, 59] {
            let (a, b) = (
                refs.view_at(o).unwrap().unwrap(),
                plain.view_at(o).unwrap().unwrap(),
            );
            assert_eq!(a.pos, b.pos);
            for max in [1, 300, 1_000, 5_000] {
                assert_eq!(
                    a.read(max).unwrap(),
                    b.read(max).unwrap(),
                    "offset {o}, {max} bytes"
                );
            }
        }
        assert_eq!(refs.heads().unwrap(), plain.heads().unwrap());
        let (rv, rh) = refs.view_at_time(1_010).unwrap().unwrap();
        let (pv, ph) = plain.view_at_time(1_010).unwrap().unwrap();
        assert_eq!((rv.pos, rh), (pv.pos, ph));
        let mut buf = vec![0u8; 500];
        let mut want = vec![0u8; 500];
        refs.read_bytes(&mut buf, 333).unwrap();
        plain.read_bytes(&mut want, 333).unwrap();
        assert_eq!(buf, want, "a range across batches and heads");
        // A copy keeps reading what it saw.
        let copy = refs.read_copy();
        assert_eq!(all(&copy), all(&plain));
        // Reopened from its records, cut at a recorded length, and at a
        // torn record.
        refs.sync().unwrap();
        let path = refs.path.clone();
        assert!(path.to_string_lossy().ends_with(".ref"));
        drop(refs);
        let find = |id: u64| blobs.file(id);
        let again = Segment::open_refs(&path, 0, None, 0, 700, &find).unwrap();
        assert_eq!(all(&again), all(&plain));
        let cut = Segment::open_refs(&path, 0, Some(ends[9] + 5), 0, 700, &find).unwrap();
        assert_eq!((cut.next, cut.flen), (30, ends[9]));
        let p = cut
            .view_at(0)
            .unwrap()
            .unwrap()
            .read(usize::MAX >> 1)
            .unwrap();
        assert_eq!(p[..], all(&plain)[..usize::try_from(cut.len).unwrap()]);
        // A blob file that is gone is an error, never an empty segment.
        assert_eq!(
            Segment::open_refs(&path, 0, None, 0, 700, &|_| None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
