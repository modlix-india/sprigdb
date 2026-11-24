//! Tenant backups: every unit of a tenant exported from one point of its
//! commit log, cut into content-named chunks, sealed and uploaded. Chunks
//! a previous backup already stored are referenced, not copied.
//!
//! A unit is exported in the engine's stream form
//! ([`sprig_engine::Engine::export_unit_to`]), read as it is written:
//! entries (`u32 key len | key | u32 value len | value`) go into chunks cut
//! at content-defined boundaries (after at least [`MIN_CHUNK`] bytes, at a
//! key whose hash ends in eight zero bits, or at [`MAX_CHUNK`]), so a
//! change in one key range changes only the chunks around it; the parts
//! after the keys (the tenant's stream and change-log files) go into
//! fixed-size chunks. The manifest, written last, is the backup's commit
//! point: a backup that stopped half-way left only unreferenced chunks,
//! which retention removes.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigError};
use sprig_engine::keys;
use sprig_engine::{Engine, UnitId};

use crate::crypt::DataKey;
use crate::target::BackupTarget;
use crate::{chunk_name, manifest_name, now_ms};

/// The smallest key chunk, before a boundary may cut it.
pub const MIN_CHUNK: usize = 256 << 10;
/// The largest key chunk.
pub const MAX_CHUNK: usize = 8 << 20;
/// The size of a parts chunk.
pub const PARTS_CHUNK: usize = 8 << 20;
const MAGIC: &[u8; 8] = b"SPRIGUS1";
const END: u32 = u32::MAX;

/// One unit in a backup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitBackup {
    /// The unit.
    pub unit: u64,
    /// Its applied log index at the export.
    pub index: u64,
    /// Its highest version timestamp at the export.
    pub max_ts: u64,
    /// Entries in the stream (data and bookkeeping).
    pub entries: u64,
    /// CRC-32 over every framed entry (the stream's trailer).
    pub crc: u32,
    /// The span checksum of its data: entries, bytes, CRC-32.
    pub data_entries: u64,
    /// Bytes of keys and values in its data.
    pub data_bytes: u64,
    /// CRC-32 over its data entries.
    pub data_crc: u32,
    /// Key chunks, in order.
    pub chunks: Vec<String>,
    /// Parts chunks, in order.
    pub parts: Vec<String>,
}

/// A backup's manifest, `t<tenant>/backups/<taken ms>.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupManifest {
    /// Format version (1).
    pub version: u32,
    /// The tenant.
    pub tenant: u64,
    /// When it was taken, ms since the epoch: its id.
    pub taken_ms: u64,
    /// The commit timestamp it stands at (every unit's state includes
    /// every commit at or below its own `max_ts`).
    pub ts: u64,
    /// Its units.
    pub units: Vec<UnitBackup>,
    /// Chunks this backup uploaded (the rest were already stored).
    pub new_chunks: u64,
    /// Bytes of the chunks it uploaded, before sealing.
    pub new_bytes: u64,
    /// Bytes of every chunk it references, before sealing.
    pub total_bytes: u64,
}

fn err(m: impl Into<String>) -> SprigError {
    SprigError::internal(format!("backup: {}", m.into()))
}

/// Cuts a unit's export stream into chunks as it is written.
struct Chunker<'a> {
    key: &'a DataKey,
    stage: PathBuf,
    known: &'a BTreeSet<String>,
    max_ts_key: Vec<u8>,
    // Parsing.
    pending: Vec<u8>,
    state: State,
    // Output.
    cur: Vec<u8>,
    chunks: Vec<String>,
    staged: Vec<(String, PathBuf)>,
    parts_buf: Vec<u8>,
    parts: Vec<String>,
    entries: u64,
    trailer: Option<(u64, u32)>,
    max_ts: u64,
    total: u64,
    io_error: Option<std::io::Error>,
    /// Bytes per second the export may read (0: no limit).
    rate: u64,
    started: std::time::Instant,
    read: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Magic,
    Entries,
    Parts,
}

fn boundary(key: &[u8]) -> bool {
    // FNV-1a over the key without its version, so a key's new versions
    // keep the same boundary.
    let logical = if key.len() >= keys::PREFIX_LEN + keys::VERSION_LEN {
        keys::strip_version(key)
    } else {
        key
    };
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in logical {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h.trailing_zeros() >= 8
}

impl Chunker<'_> {
    fn emit(&mut self, parts: bool) -> std::io::Result<()> {
        let buf = if parts {
            std::mem::take(&mut self.parts_buf)
        } else {
            std::mem::take(&mut self.cur)
        };
        if buf.is_empty() {
            return Ok(());
        }
        self.total += buf.len() as u64;
        let name = self.key.chunk_name(&buf);
        if !self.known.contains(&name) && !self.staged.iter().any(|(n, _)| *n == name) {
            let p = self.stage.join(&name);
            std::fs::write(&p, &buf)?;
            self.staged.push((name.clone(), p));
        }
        if parts {
            self.parts.push(name);
        } else {
            self.chunks.push(name);
        }
        Ok(())
    }

    /// Parses what it can of `pending`.
    fn parse(&mut self) -> std::io::Result<()> {
        let mut at = 0usize;
        loop {
            let rest = &self.pending[at..];
            match self.state {
                State::Magic => {
                    if rest.len() < 8 {
                        break;
                    }
                    if &rest[..8] != MAGIC {
                        return Err(std::io::Error::other("not a unit snapshot stream"));
                    }
                    at += 8;
                    self.state = State::Entries;
                }
                State::Entries => {
                    if rest.len() < 4 {
                        break;
                    }
                    let kl = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
                    if kl == END {
                        if rest.len() < 16 {
                            break;
                        }
                        let n = u64::from_be_bytes(rest[4..12].try_into().unwrap_or_default());
                        let c = u32::from_be_bytes(rest[12..16].try_into().unwrap_or_default());
                        self.trailer = Some((n, c));
                        at += 16;
                        self.emit(false)?;
                        self.state = State::Parts;
                        continue;
                    }
                    let kl = kl as usize;
                    if rest.len() < 4 + kl + 4 {
                        break;
                    }
                    let vl_at = 4 + kl;
                    let vl =
                        u32::from_be_bytes(rest[vl_at..vl_at + 4].try_into().unwrap_or_default())
                            as usize;
                    let total = 4 + kl + 4 + vl;
                    if rest.len() < total {
                        break;
                    }
                    let key = &rest[4..4 + kl];
                    if key == self.max_ts_key.as_slice() && vl == 8 {
                        self.max_ts = u64::from_be_bytes(
                            rest[vl_at + 4..vl_at + 12].try_into().unwrap_or_default(),
                        );
                    }
                    let cut = boundary(key);
                    self.cur.extend_from_slice(&rest[..total]);
                    self.entries += 1;
                    at += total;
                    if (self.cur.len() >= MIN_CHUNK && cut) || self.cur.len() >= MAX_CHUNK {
                        self.emit(false)?;
                    }
                }
                State::Parts => {
                    self.parts_buf.extend_from_slice(rest);
                    at = self.pending.len();
                    while self.parts_buf.len() >= PARTS_CHUNK {
                        let tail = self.parts_buf.split_off(PARTS_CHUNK);
                        self.emit(true)?;
                        self.parts_buf = tail;
                    }
                    break;
                }
            }
        }
        self.pending.drain(..at);
        Ok(())
    }
}

impl Write for Chunker<'_> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.pending.extend_from_slice(b);
        self.read += b.len() as u64;
        if self.rate > 0 && self.read % (1 << 20) < b.len() as u64 {
            // Keep to the rate: the live load comes first.
            let due = std::time::Duration::from_millis(
                u64::try_from(u128::from(self.read) * 1000 / u128::from(self.rate))
                    .unwrap_or(u64::MAX),
            );
            if let Some(wait) = due.checked_sub(self.started.elapsed()) {
                std::thread::sleep(wait);
            }
        }
        if (self.pending.len() >= 64 << 10 || self.state == State::Parts)
            && let Err(e) = self.parse()
        {
            self.io_error = Some(std::io::Error::new(e.kind(), e.to_string()));
            return Err(e);
        }
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What a backup did.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupReport {
    /// The manifest written.
    pub manifest: BackupManifest,
    /// Milliseconds it took.
    pub took_ms: u64,
}

/// Every chunk name stored for `tenant`.
///
/// # Errors
/// The storage failed.
pub fn stored_chunks(target: &dyn BackupTarget, tenant: u64) -> Result<BTreeSet<String>> {
    let prefix = format!("t{tenant}/chunks/");
    Ok(target
        .list(&prefix)?
        .into_iter()
        .filter_map(|n| n.strip_prefix(&prefix).map(str::to_owned))
        .collect())
}

/// Takes a backup of `tenant` (its unit) into `target`. `work` is a
/// local directory for staging chunks (emptied afterwards). The export
/// reads at most `rate` bytes per second (0: no limit).
///
/// # Errors
/// The engine, the staging directory or the storage failed. Nothing a
/// restore would use is left behind.
pub fn take(
    engine: &Engine,
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    work: &Path,
    rate: u64,
) -> Result<BackupReport> {
    let started = std::time::Instant::now();
    let taken_ms = now_ms();
    let known = stored_chunks(target, tenant)?;
    let stage = work.join(format!("stage-t{tenant}-{taken_ms}"));
    let _ = std::fs::remove_dir_all(&stage);
    std::fs::create_dir_all(&stage).map_err(|e| err(format!("create {}: {e}", stage.display())))?;
    let r = take_into(engine, target, key, tenant, taken_ms, &known, &stage, rate);
    let _ = std::fs::remove_dir_all(&stage);
    let manifest = r?;
    Ok(BackupReport {
        manifest,
        took_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

#[allow(clippy::too_many_arguments)]
fn take_into(
    engine: &Engine,
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    taken_ms: u64,
    known: &BTreeSet<String>,
    stage: &Path,
    rate: u64,
) -> Result<BackupManifest> {
    if tenant == keys::META_TENANT || UnitId(tenant) == sprig_engine::NODE_UNIT {
        return Err(err("the engine's own unit has no backup"));
    }
    let unit = Engine::unit_of_tenant(tenant);
    if engine.last_applied_of(unit) == 0 && engine.log_first_index_of(unit).is_none() {
        return Err(SprigError::new(
            sprig_common::SprigCode::AdminRequestInvalid,
            format!("backup: tenant {tenant} has no data on this node's engine"),
        ));
    }
    let mut c = Chunker {
        key,
        stage: stage.to_path_buf(),
        known,
        max_ts_key: keys::unit_meta_key(unit.0, b"max_ts"),
        pending: Vec::new(),
        state: State::Magic,
        cur: Vec::new(),
        chunks: Vec::new(),
        staged: Vec::new(),
        parts_buf: Vec::new(),
        parts: Vec::new(),
        entries: 0,
        trailer: None,
        max_ts: 0,
        total: 0,
        io_error: None,
        rate,
        started: std::time::Instant::now(),
        read: 0,
    };
    let x = engine.export_unit_to(unit, &mut c);
    if let Some(e) = c.io_error.take() {
        return Err(err(format!("staging a chunk: {e}")));
    }
    let x = x?;
    c.parse()
        .map_err(|e| err(format!("staging a chunk: {e}")))?;
    c.emit(true)
        .map_err(|e| err(format!("staging a chunk: {e}")))?;
    let Some((n, crc)) = c.trailer else {
        return Err(err("the export stream ended early"));
    };
    if n != c.entries || n != x.entries {
        return Err(err(format!(
            "the export stream holds {} entries, its trailer says {n}",
            c.entries
        )));
    }
    let mut new_bytes = 0u64;
    for (name, path) in &c.staged {
        let b = std::fs::read(path).map_err(|e| err(format!("read {}: {e}", path.display())))?;
        new_bytes += b.len() as u64;
        let obj = chunk_name(tenant, name);
        target.put(&obj, &key.seal(&obj, &b)?)?;
    }
    let m = BackupManifest {
        version: 1,
        tenant,
        taken_ms,
        ts: c.max_ts,
        units: vec![UnitBackup {
            unit: unit.0,
            index: x.index,
            max_ts: c.max_ts,
            entries: n,
            crc,
            data_entries: x.checksum.entries,
            data_bytes: x.checksum.bytes,
            data_crc: x.checksum.crc,
            chunks: c.chunks,
            parts: c.parts,
        }],
        new_chunks: c.staged.len() as u64,
        new_bytes,
        total_bytes: c.total,
    };
    let obj = manifest_name(tenant, taken_ms);
    let body = serde_json::to_vec_pretty(&m).map_err(|e| err(format!("manifest: {e}")))?;
    target.put(&obj, &key.seal(&obj, &body)?)?;
    tracing::info!(
        tenant,
        index = x.index,
        chunks = m.units[0].chunks.len(),
        new_chunks = m.new_chunks,
        new_bytes,
        "backup taken"
    );
    Ok(m)
}

/// Every backup of `tenant`, oldest first.
///
/// # Errors
/// The storage failed, or a manifest doesn't open.
pub fn list(target: &dyn BackupTarget, key: &DataKey, tenant: u64) -> Result<Vec<BackupManifest>> {
    let prefix = format!("t{tenant}/backups/");
    let mut out = Vec::new();
    for name in target.list(&prefix)? {
        if !std::path::Path::new(&name)
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("json"))
        {
            continue;
        }
        let Some(b) = target.get(&name)? else {
            continue;
        };
        let plain = key.open(&name, &b)?;
        let m: BackupManifest =
            serde_json::from_slice(&plain).map_err(|e| err(format!("{name}: {e}")))?;
        out.push(m);
    }
    out.sort_by_key(|m| m.taken_ms);
    Ok(out)
}
