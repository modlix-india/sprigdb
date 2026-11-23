//! Unit snapshots: a unit's keys at one applied index, exported and
//! ingested elsewhere.
//!
//! A unit is its tenant's key range plus its bookkeeping under the
//! reserved tenant (applied index, highest timestamp, XA prepare
//! records). Both are read through one store iterator, so they come from
//! one point in time; the applied index read there is the snapshot's
//! index, because the apply step writes a group's data and the index in
//! one batch.
//!
//! Two forms:
//! - **Files** ([`Engine::export_unit`], [`Engine::ingest_unit`]): on
//!   RocksDB, sorted SST files the target ingests without rewriting them,
//!   for large units; on the in-memory store, the stream format in a file.
//! - **A stream** ([`Engine::export_unit_to`], [`Engine::ingest_unit_from`]):
//!   `magic | (u32 key len | key | u32 value len | value)* | u32 0xFFFF_FFFF
//!   | u64 entries | u32 crc`, for replicating small units in one message.
//!
//! [`Engine::unit_checksum`] gives the span checksum both sides compare.
//!
//! **Parts** ([`UnitSnapshotPart`]): state kept beside the store that a
//! unit owns (its tenant's stream files) rides after the keys, in both
//! forms: `(u32 name len | name | (u32 chunk len | chunk)* | u32 0)* |
//! u32 0xFFFF_FFFF`, in the stream after its trailer, in a `parts.snap`
//! file beside the SST files. A part exports its state at or after the
//! snapshot's index; its ingest runs after the keys are in.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use sprig_common::{Result, SprigError};

use crate::Engine;
use crate::keys;
use crate::log::UnitId;
use crate::store::{Cf, IterOpts, StoreBatch, WalMode};

const MAGIC: &[u8; 8] = b"SPRIGUS1";
const END: u32 = u32::MAX;
/// The file of a file-form snapshot's parts.
const PARTS_FILE: &str = "parts.snap";
/// Chunk size of a part's bytes.
const CHUNK: usize = 1 << 20;

/// State a unit owns outside the store (a tenant's stream files), carried
/// by unit snapshots after the unit's keys.
pub trait UnitSnapshotPart: Send + Sync {
    /// The part's name, the same on every node.
    fn name(&self) -> &'static str;
    /// Writes the part's state of `unit` as of applied index `index` or
    /// later (entries up to the later point are then skipped when the
    /// unit's log delivers them again).
    ///
    /// # Errors
    /// The part's state can't be read, or the writer failed.
    fn export(&self, unit: UnitId, index: u64, out: &mut dyn Write) -> Result<()>;
    /// Replaces the part's state of `unit` with `input` (what [`export`]
    /// wrote). The unit's keys are already in, at applied index `index`.
    ///
    /// # Errors
    /// The bytes are malformed, or the part's files can't be written.
    ///
    /// [`export`]: UnitSnapshotPart::export
    fn ingest(&self, unit: UnitId, index: u64, input: &mut dyn Read) -> Result<()>;
}

/// A part's bytes from an ingested snapshot, kept until the part registers:
/// its name, the unit, the applied index and the bytes.
pub(crate) type PendingPart = (String, UnitId, u64, Vec<u8>);

/// Frames what a part writes as chunks.
struct Chunks<'a> {
    out: &'a mut dyn Write,
    buf: Vec<u8>,
}

impl Write for Chunks<'_> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(b);
        if self.buf.len() >= CHUNK {
            self.flush()?;
        }
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.out.write_all(&len32(self.buf.len()).to_be_bytes())?;
            self.out.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

/// Reads one part's chunks; at its end it reads nothing more.
struct Unchunk<'a> {
    input: &'a mut dyn Read,
    left: usize,
    done: bool,
}

impl Read for Unchunk<'_> {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        while self.left == 0 {
            if self.done {
                return Ok(0);
            }
            let mut n = [0u8; 4];
            self.input.read_exact(&mut n)?;
            self.left = u32::from_be_bytes(n) as usize;
            if self.left == 0 {
                self.done = true;
            }
        }
        let k = b.len().min(self.left);
        let got = self.input.read(&mut b[..k])?;
        if got == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        self.left -= got;
        Ok(got)
    }
}

/// What an export wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitExport {
    /// The unit.
    pub unit: UnitId,
    /// Its applied index at the export.
    pub index: u64,
    /// Entries exported (data and bookkeeping).
    pub entries: u64,
    /// Bytes of keys and values.
    pub bytes: u64,
    /// The files written, in key order.
    pub files: Vec<PathBuf>,
    /// The span checksum of the unit's data at the export.
    pub checksum: SpanChecksum,
}

/// A checksum over a unit's data keys and values, in key order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpanChecksum {
    /// Entries.
    pub entries: u64,
    /// Bytes of keys and values.
    pub bytes: u64,
    /// CRC-32 over every `u32 key len | key | u32 value len | value`.
    pub crc: u32,
}

impl SpanChecksum {
    fn add(&mut self, crc: &mut flate2::Crc, k: &[u8], v: &[u8]) {
        crc.update(&len32(k.len()).to_be_bytes());
        crc.update(k);
        crc.update(&len32(v.len()).to_be_bytes());
        crc.update(v);
        self.entries += 1;
        self.bytes += (k.len() + v.len()) as u64;
    }
}

fn len32(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

fn io(what: &str, e: std::io::Error) -> SprigError {
    SprigError::internal(format!("unit snapshot: {what}")).with_source(e)
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("unit snapshot: {what}"))
}

/// The key range of a unit's data, and of its bookkeeping.
fn ranges(unit: UnitId) -> Result<(keys::KeyRange, keys::KeyRange)> {
    if unit == crate::engine::NODE_UNIT || unit.0 == keys::META_TENANT {
        return Err(bad("the node unit has no snapshot"));
    }
    let meta = keys::unit_meta_key(unit.0, b"");
    Ok((keys::tenant_range(unit.0), keys::KeyRange::prefix(&meta)))
}

/// Called for each entry of a unit walk: (bookkeeping entry, key, value).
type Visit<'a> = dyn FnMut(bool, &[u8], &[u8]) -> Result<()> + 'a;

impl Engine {
    /// Walks the unit's entries (data, then bookkeeping) from one point in
    /// time. Returns the applied index read at that point.
    fn walk_unit(&self, unit: UnitId, f: &mut Visit<'_>) -> Result<u64> {
        let (data, meta) = ranges(unit)?;
        let store = self.store();
        let mut it = store.iter(
            Cf::Data,
            IterOpts {
                lower: Some(data.start.clone()),
                ..IterOpts::default()
            },
        );
        it.seek(&data.start);
        while it.valid() && data.contains(it.key()) {
            f(true, it.key(), it.value())?;
            it.next();
        }
        let applied_key = keys::unit_meta_key(unit.0, b"applied");
        let mut applied = 0;
        it.seek(&meta.start);
        while it.valid() && meta.contains(it.key()) {
            if it.key() == applied_key.as_slice() {
                let b: [u8; 8] = it
                    .value()
                    .try_into()
                    .map_err(|_| bad("the applied index is corrupt"))?;
                applied = u64::from_be_bytes(b);
            }
            f(false, it.key(), it.value())?;
            it.next();
        }
        it.status()?.expect_ready()?;
        Ok(applied)
    }

    /// The span checksum of the unit's data (its tenant's range).
    ///
    /// # Errors
    /// The store failed, or `unit` is the node unit.
    pub fn unit_checksum(&self, unit: UnitId) -> Result<SpanChecksum> {
        let mut sum = SpanChecksum::default();
        let mut crc = flate2::Crc::new();
        self.walk_unit(unit, &mut |data, k, v| {
            if data {
                sum.add(&mut crc, k, v);
            }
            Ok(())
        })?;
        sum.crc = crc.sum();
        Ok(sum)
    }

    /// Writes the unit's state at its applied index as a stream (see the
    /// module docs). Returns the export's index and counts.
    ///
    /// # Errors
    /// The store or the writer failed.
    pub fn export_unit_to(&self, unit: UnitId, out: &mut dyn Write) -> Result<UnitExport> {
        out.write_all(MAGIC).map_err(|e| io("write", e))?;
        let mut all = SpanChecksum::default();
        let mut all_crc = flate2::Crc::new();
        let mut sum = SpanChecksum::default();
        let mut crc = flate2::Crc::new();
        let index = self.walk_unit(unit, &mut |data, k, v| {
            out.write_all(&len32(k.len()).to_be_bytes())
                .and_then(|()| out.write_all(k))
                .and_then(|()| out.write_all(&len32(v.len()).to_be_bytes()))
                .and_then(|()| out.write_all(v))
                .map_err(|e| io("write", e))?;
            all.add(&mut all_crc, k, v);
            if data {
                sum.add(&mut crc, k, v);
            }
            Ok(())
        })?;
        out.write_all(&END.to_be_bytes())
            .and_then(|()| out.write_all(&all.entries.to_be_bytes()))
            .and_then(|()| out.write_all(&all_crc.sum().to_be_bytes()))
            .map_err(|e| io("write", e))?;
        self.export_parts(unit, index, out)?;
        sum.crc = crc.sum();
        Ok(UnitExport {
            unit,
            index,
            entries: all.entries,
            bytes: all.bytes,
            files: Vec::new(),
            checksum: sum,
        })
    }

    /// Replaces the unit's state with a stream written by
    /// [`Engine::export_unit_to`]. The unit must hold nothing here yet
    /// (no keys, no log entries): a replica being created, or a move's
    /// target. Returns the snapshot's applied index.
    ///
    /// # Errors
    /// The stream is malformed or for another unit, the unit isn't empty
    /// here, or the store failed.
    pub fn ingest_unit_from(&self, unit: UnitId, input: &mut dyn Read) -> Result<u64> {
        self.check_empty(unit)?;
        self.load_stream(unit, input)
    }

    /// Replaces whatever the unit holds here with a stream written by
    /// [`Engine::export_unit_to`]: a replicated unit's follower restoring
    /// a snapshot (its log is the replication log's, not this node's).
    /// Returns the snapshot's applied index.
    ///
    /// # Errors
    /// The stream is malformed or for another unit, or the store failed.
    pub fn replace_unit_from(&self, unit: UnitId, input: &mut dyn Read) -> Result<u64> {
        let (data, meta) = ranges(unit)?;
        let mut clear = StoreBatch::new();
        clear.delete_range(
            Cf::Data,
            data.start.clone(),
            data.end.clone().unwrap_or_default(),
        );
        clear.delete_range(
            Cf::Data,
            meta.start.clone(),
            meta.end.clone().unwrap_or_default(),
        );
        self.store().write(&clear, WalMode::Off)?;
        self.load_stream(unit, input)
    }

    fn load_stream(&self, unit: UnitId, input: &mut dyn Read) -> Result<u64> {
        let (data, meta) = ranges(unit)?;
        let mut magic = [0u8; 8];
        input.read_exact(&mut magic).map_err(|e| io("read", e))?;
        if &magic != MAGIC {
            return Err(bad("not a unit snapshot"));
        }
        let mut all = SpanChecksum::default();
        let mut crc = flate2::Crc::new();
        let mut batch = StoreBatch::new();
        let mut u32b = [0u8; 4];
        loop {
            input.read_exact(&mut u32b).map_err(|e| io("read", e))?;
            let kl = u32::from_be_bytes(u32b);
            if kl == END {
                break;
            }
            let mut k = vec![0; kl as usize];
            input.read_exact(&mut k).map_err(|e| io("read", e))?;
            input.read_exact(&mut u32b).map_err(|e| io("read", e))?;
            let mut v = vec![0; u32::from_be_bytes(u32b) as usize];
            input.read_exact(&mut v).map_err(|e| io("read", e))?;
            if !data.contains(&k) && !meta.contains(&k) {
                return Err(bad("an entry outside the unit"));
            }
            all.add(&mut crc, &k, &v);
            batch.put(Cf::Data, k, v);
            if batch.bytes() >= 4 << 20 {
                self.store().write(&batch, WalMode::Off)?;
                batch = StoreBatch::new();
            }
        }
        let mut n = [0u8; 8];
        input.read_exact(&mut n).map_err(|e| io("read", e))?;
        input.read_exact(&mut u32b).map_err(|e| io("read", e))?;
        if u64::from_be_bytes(n) != all.entries || u32::from_be_bytes(u32b) != crc.sum() {
            // Undo what was written: the unit was empty.
            let mut undo = StoreBatch::new();
            undo.delete_range(
                Cf::Data,
                data.start.clone(),
                data.end.clone().unwrap_or_default(),
            );
            undo.delete_range(
                Cf::Data,
                meta.start.clone(),
                meta.end.clone().unwrap_or_default(),
            );
            self.store().write(&undo, WalMode::Off)?;
            return Err(bad("the snapshot's checksum doesn't match"));
        }
        self.store().write(&batch, WalMode::Off)?;
        let index = self.finish_ingest(unit)?;
        self.ingest_parts(unit, index, input)?;
        Ok(index)
    }

    /// Registers a part unit snapshots carry.
    /// A snapshot ingested before the part registered left its bytes
    /// aside: they are ingested now, in the order they came.
    pub fn add_snapshot_part(&self, p: std::sync::Arc<dyn UnitSnapshotPart>) {
        let mut parts = self
            .snapshot_parts()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let waiting: Vec<PendingPart> = {
            let mut pending = self
                .pending_parts()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let (mine, rest) = std::mem::take(&mut *pending)
                .into_iter()
                .partition(|(n, ..)| n == p.name());
            *pending = rest;
            mine
        };
        for (name, unit, index, bytes) in waiting {
            if let Err(e) = p.ingest(unit, index, &mut bytes.as_slice()) {
                tracing::error!(part = %name, unit = unit.0, error = %e, "a snapshot part left aside could not be ingested");
            }
        }
        parts.push(p);
    }

    /// Writes every registered part of `unit` (see the module docs).
    fn export_parts(&self, unit: UnitId, index: u64, out: &mut dyn Write) -> Result<()> {
        let parts = self
            .snapshot_parts()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for p in parts {
            let name = p.name().as_bytes();
            out.write_all(&len32(name.len()).to_be_bytes())
                .and_then(|()| out.write_all(name))
                .map_err(|e| io("write", e))?;
            let mut c = Chunks {
                out: &mut *out,
                buf: Vec::with_capacity(CHUNK),
            };
            p.export(unit, index, &mut c)?;
            c.flush().map_err(|e| io("write", e))?;
            out.write_all(&0u32.to_be_bytes())
                .map_err(|e| io("write", e))?;
        }
        out.write_all(&END.to_be_bytes())
            .map_err(|e| io("write", e))
    }

    /// Reads the parts after a snapshot's keys and hands each to the part
    /// registered under its name.
    fn ingest_parts(&self, unit: UnitId, index: u64, input: &mut dyn Read) -> Result<()> {
        let parts = self
            .snapshot_parts()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut u32b = [0u8; 4];
        let mut first = true;
        loop {
            match input.read_exact(&mut u32b) {
                // A snapshot written before parts existed ends here.
                Err(e) if first && e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                r => r.map_err(|e| io("read", e))?,
            }
            first = false;
            let n = u32::from_be_bytes(u32b);
            if n == END {
                return Ok(());
            }
            let mut name = vec![0; n as usize];
            input.read_exact(&mut name).map_err(|e| io("read", e))?;
            let name = String::from_utf8_lossy(&name).into_owned();
            let Some(p) = parts.iter().find(|p| p.name() == name) else {
                // Not registered yet (the stream log starts after the
                // engine opens): keep the bytes for its registration.
                let mut r = Unchunk {
                    input: &mut *input,
                    left: 0,
                    done: false,
                };
                let mut bytes = Vec::new();
                r.read_to_end(&mut bytes).map_err(|e| io("read", e))?;
                let mut pending = self
                    .pending_parts()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                pending.retain(|(n, u, ..)| !(*n == name && *u == unit));
                pending.push((name, unit, index, bytes));
                continue;
            };
            let mut r = Unchunk {
                input: &mut *input,
                left: 0,
                done: false,
            };
            p.ingest(unit, index, &mut r)?;
            // Whatever the part didn't read of its own bytes.
            std::io::copy(&mut r, &mut std::io::sink()).map_err(|e| io("read", e))?;
        }
    }

    /// Exports the unit at its applied index into files in `dir` (created;
    /// it must be empty or missing). On RocksDB the data is sorted SST
    /// files of at most `file_bytes` each.
    ///
    /// # Errors
    /// The store or the file system failed.
    pub fn export_unit(&self, unit: UnitId, dir: &Path, file_bytes: u64) -> Result<UnitExport> {
        std::fs::create_dir_all(dir).map_err(|e| io("create the directory", e))?;
        let Some(rocks) = self.rocks() else {
            let path = dir.join("unit.snap");
            let f = std::fs::File::create(&path).map_err(|e| io("create", e))?;
            let mut w = std::io::BufWriter::new(f);
            let mut x = self.export_unit_to(unit, &mut w)?;
            w.flush().map_err(|e| io("write", e))?;
            x.files.push(path);
            return Ok(x);
        };
        let mut files = Vec::new();
        let mut buf: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut in_buf = 0u64;
        let mut all = SpanChecksum::default();
        let mut all_crc = flate2::Crc::new();
        let mut sum = SpanChecksum::default();
        let mut crc = flate2::Crc::new();
        let flush = |buf: &mut Vec<(Vec<u8>, Vec<u8>)>, files: &mut Vec<PathBuf>| -> Result<()> {
            if buf.is_empty() {
                return Ok(());
            }
            let path = dir.join(format!("{:06}.sst", files.len()));
            rocks.write_sst(&path, buf)?;
            files.push(path);
            buf.clear();
            Ok(())
        };
        let index = self.walk_unit(unit, &mut |data, k, v| {
            buf.push((k.to_vec(), v.to_vec()));
            in_buf += (k.len() + v.len()) as u64;
            if in_buf >= file_bytes {
                flush(&mut buf, &mut files)?;
                in_buf = 0;
            }
            all.add(&mut all_crc, k, v);
            if data {
                sum.add(&mut crc, k, v);
            }
            Ok(())
        })?;
        flush(&mut buf, &mut files)?;
        let parts = dir.join(PARTS_FILE);
        {
            let f = std::fs::File::create(&parts).map_err(|e| io("create", e))?;
            let mut w = std::io::BufWriter::new(f);
            self.export_parts(unit, index, &mut w)?;
            w.flush().map_err(|e| io("write", e))?;
        }
        files.push(parts);
        sum.crc = crc.sum();
        Ok(UnitExport {
            unit,
            index,
            entries: all.entries,
            bytes: all.bytes,
            files,
            checksum: sum,
        })
    }

    /// Ingests files written by [`Engine::export_unit`] (moved into the
    /// store on RocksDB). The unit must hold nothing here yet. Returns the
    /// snapshot's applied index.
    ///
    /// # Errors
    /// The unit isn't empty here, the files don't match the store kind, or
    /// the store failed.
    pub fn ingest_unit(&self, unit: UnitId, files: &[PathBuf]) -> Result<u64> {
        let Some(rocks) = self.rocks() else {
            self.check_empty(unit)?;
            let [path] = files else {
                return Err(bad("expected one snapshot file"));
            };
            let f = std::fs::File::open(path).map_err(|e| io("open", e))?;
            return self.ingest_unit_from(unit, &mut std::io::BufReader::new(f));
        };
        self.check_empty(unit)?;
        let (parts, ssts): (Vec<PathBuf>, Vec<PathBuf>) = files
            .iter()
            .cloned()
            .partition(|f| f.file_name().is_some_and(|n| n == PARTS_FILE));
        if !ssts.is_empty() {
            rocks.ingest_sst(Cf::Data, &ssts)?;
        }
        let index = self.finish_ingest(unit)?;
        if let Some(p) = parts.first() {
            let f = std::fs::File::open(p).map_err(|e| io("open", e))?;
            self.ingest_parts(unit, index, &mut std::io::BufReader::new(f))?;
        }
        Ok(index)
    }

    /// Refuses a unit that already holds keys or log entries here.
    fn check_empty(&self, unit: UnitId) -> Result<()> {
        let (data, meta) = ranges(unit)?;
        for r in [&data, &meta] {
            let mut it = self.store().iter(Cf::Data, IterOpts::range(r));
            it.seek(&r.start);
            if it.valid() {
                return Err(bad("the unit already holds data here"));
            }
            it.status()?.expect_ready()?;
        }
        if self.log().first_index(unit).is_some() || self.consensus().applied_index(unit) > 0 {
            return Err(bad("the unit already has a log here"));
        }
        Ok(())
    }

    /// After an ingest: the clock moves above the unit's versions, and the
    /// unit's log continues after the snapshot's index.
    fn finish_ingest(&self, unit: UnitId) -> Result<u64> {
        let read = |field: &[u8]| -> Result<u64> {
            match self
                .store()
                .get(
                    Cf::Data,
                    &keys::unit_meta_key(unit.0, field),
                    crate::store::ReadTier::All,
                )?
                .expect_ready()?
            {
                None => Ok(0),
                Some(b) => Ok(u64::from_be_bytes(
                    b.as_ref()
                        .try_into()
                        .map_err(|_| bad("bookkeeping is corrupt"))?,
                )),
            }
        };
        let index = read(b"applied")?;
        let max_ts = read(b"max_ts")?;
        self.txn_shared().observe(keys::Ts(max_ts));
        self.note_ingested(unit, index, max_ts);
        Ok(index)
    }
}
