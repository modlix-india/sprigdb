//! Restore to a moment: the newest backup at or before `T`, then the
//! archived log entries up to `T`.
//!
//! The state is rebuilt in a scratch engine of its own (a directory under
//! the work directory), never in place: the backup's chunks are ingested
//! there in the stream form, then archived entries are applied in index
//! order through the scratch engine's state machine, exactly as the live
//! engine applied them, up to the first entry whose commit timestamp is
//! above `T`. A prefix of a unit's log is always a state the unit was in,
//! so the result is the unit as it stood after its last commit at or
//! before `T`.
//!
//! **Into another tenant** ([`restore_into`]): keys are moved to the
//! target tenant id as they're read. Then, in one of two ways:
//! - the target holds nothing on the engine (no keys, no log): the scratch
//!   unit is exported as files and ingested in one step (on RocksDB, one
//!   atomic SST ingest), with the tenant's stream and change-log files;
//! - the target already exists (a tenant just created for the restore,
//!   whose catalog bootstrap wrote into it; `replace` must be set): the
//!   rebuilt keys go through the target unit's own commit log, as entries
//!   of raw store writes: the first removes the target's data range, the
//!   rest put every key with its versions as rebuilt. The unit's log and
//!   applied index stay its own. Stream files are not carried this way.
//!
//! A marker file names the target while either runs; after a crash,
//! [`recover`] empties the half-restored target before anything else uses
//! it (a retried restore then finishes).
//!
//! **A check** ([`verify`]): the same rebuild under the tenant's own id,
//! returning the restored span checksum, for scheduled restore tests and
//! the gates.

use std::io::{Read, Write as _};
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::keys::{self, Ts};
use sprig_engine::log::{LogEntry, Payload, SyncMode};
use sprig_engine::snapshot::SpanChecksum;
use sprig_engine::store::{Cf, StoreBatch, WalMode};
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};

use crate::archive::{decode_segment, segments};
use crate::backup::{BackupManifest, UnitBackup, list};
use crate::crypt::DataKey;
use crate::payload::{Rekey, rekey_payload};
use crate::target::BackupTarget;
use crate::{chunk_name, now_ms};

const MAGIC: &[u8; 8] = b"SPRIGUS1";
const END: u32 = u32::MAX;

fn err(m: impl Into<String>) -> SprigError {
    SprigError::internal(format!("restore: {}", m.into()))
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(
        SprigCode::AdminRequestInvalid,
        format!("restore: {}", m.into()),
    )
}

/// What a restore did.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreReport {
    /// The tenant restored from.
    pub tenant: u64,
    /// The tenant restored into.
    pub into: u64,
    /// The moment asked for, ms since the epoch (`None`: the newest state
    /// the archive holds).
    pub to_ms: Option<u64>,
    /// The backup it started from (its `taken_ms`).
    pub backup: u64,
    /// The backup's log index.
    pub backup_index: u64,
    /// Archived entries applied after the backup.
    pub replayed: u64,
    /// The log index the restored state stands at.
    pub index: u64,
    /// The commit timestamp of the last change in it (physical ms in the
    /// high 48 bits).
    pub ts: u64,
    /// Whether the archive reached past `to_ms` (an entry after it was
    /// seen). `false`: the archive ends earlier, so the state is the
    /// newest one archived; changes after that weren't archived yet.
    pub reached: bool,
    /// Span checksum of the restored data: entries.
    pub entries: u64,
    /// ... bytes.
    pub bytes: u64,
    /// ... CRC-32.
    pub crc: u32,
    /// Milliseconds it took.
    pub took_ms: u64,
    /// How the target was written: `ingest` (one step, into a target that
    /// held nothing) or `log` (through the target's commit log, replacing
    /// what it held).
    #[serde(default)]
    pub mode: String,
}

/// The newest backup of `tenant` whose state is at or before `to_ts`.
///
/// # Errors
/// None qualifies, or the storage failed.
pub fn choose(
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    to_ts: u64,
) -> Result<BackupManifest> {
    let all = list(target, key, tenant)?;
    if all.is_empty() {
        return Err(invalid(format!(
            "tenant {tenant} has no backups in {}",
            target.describe()
        )));
    }
    all.into_iter()
        .rev()
        .find(|m| m.ts <= to_ts)
        .ok_or_else(|| {
            invalid(format!(
                "tenant {tenant} has no backup at or before that moment"
            ))
        })
}

/// Builds the stream form of a backed-up unit, moving keys to `rekey`'s
/// tenant, and checking every chunk's name and the stream's checksum.
struct Assembly<'a> {
    target: &'a dyn BackupTarget,
    key: &'a DataKey,
    tenant: u64,
    unit: &'a UnitBackup,
    rekey: Option<&'a Rekey>,
    next: usize,
    out: Vec<u8>,
    pos: usize,
    entries: u64,
    crc_in: flate2::Crc,
    crc_out: flate2::Crc,
    done: bool,
}

fn len32(n: usize) -> [u8; 4] {
    u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes()
}

impl Assembly<'_> {
    fn fill(&mut self) -> Result<()> {
        self.out.clear();
        self.pos = 0;
        if self.next == 0 && self.entries == 0 && !self.done && self.crc_in.amount() == 0 {
            self.out.extend_from_slice(MAGIC);
        }
        let Some(name) = self.unit.chunks.get(self.next) else {
            if !self.done {
                if self.entries != self.unit.entries || self.crc_in.sum() != self.unit.crc {
                    return Err(err(format!(
                        "the backup of unit {} doesn't add up: {} entries (expected {})",
                        self.unit.unit, self.entries, self.unit.entries
                    )));
                }
                self.out.extend_from_slice(&END.to_be_bytes());
                self.out.extend_from_slice(&self.entries.to_be_bytes());
                self.out
                    .extend_from_slice(&self.crc_out.sum().to_be_bytes());
                // No parts here: they are carried separately.
                self.out.extend_from_slice(&END.to_be_bytes());
                self.done = true;
            }
            return Ok(());
        };
        self.next += 1;
        let plain = fetch_chunk(self.target, self.key, self.tenant, name)?;
        let mut at = 0usize;
        while at < plain.len() {
            let h = |i: usize| -> Result<usize> {
                plain
                    .get(i..i + 4)
                    .and_then(|b| b.try_into().ok())
                    .map(|b: [u8; 4]| u32::from_be_bytes(b) as usize)
                    .ok_or_else(|| err(format!("chunk {name} is malformed")))
            };
            let kl = h(at)?;
            let vl = h(at + 4 + kl)?;
            let end = at + 8 + kl + vl;
            if end > plain.len() {
                return Err(err(format!("chunk {name} is malformed")));
            }
            let frame = &plain[at..end];
            self.crc_in.update(frame);
            self.entries += 1;
            match self.rekey {
                None => {
                    self.crc_out.update(frame);
                    self.out.extend_from_slice(frame);
                }
                Some(r) => {
                    let k = r.key(&frame[4..4 + kl]);
                    let s = self.out.len();
                    self.out.extend_from_slice(&len32(k.len()));
                    self.out.extend_from_slice(&k);
                    self.out.extend_from_slice(&frame[4 + kl..]);
                    let (_, new) = self.out.split_at(s);
                    self.crc_out.update(new);
                }
            }
            at = end;
        }
        Ok(())
    }
}

impl Read for Assembly<'_> {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.out.len() {
            if self.done {
                return Ok(0);
            }
            self.fill()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        let n = b.len().min(self.out.len() - self.pos);
        b[..n].copy_from_slice(&self.out[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Fetches and opens a chunk, checking it is the content its name says.
fn fetch_chunk(
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    name: &str,
) -> Result<Vec<u8>> {
    let obj = chunk_name(tenant, name);
    let sealed = target
        .get(&obj)?
        .ok_or_else(|| err(format!("chunk {obj} is missing from the backup storage")))?;
    let plain = key.open(&obj, &sealed)?;
    if key.chunk_name(&plain) != name {
        return Err(err(format!("chunk {obj} doesn't hold what its name says")));
    }
    Ok(plain)
}

/// A rebuilt unit in a scratch engine.
pub struct Rebuilt {
    /// The scratch engine (closed when dropped).
    pub engine: Engine,
    /// The unit in it.
    pub unit: UnitId,
    /// The parts section of the backup (`(name ...)* END`).
    pub parts: Vec<u8>,
    /// The report so far (checksum filled in).
    pub report: RestoreReport,
}

impl std::fmt::Debug for Rebuilt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rebuilt")
            .field("report", &self.report)
            .finish_non_exhaustive()
    }
}

/// The scratch engine's settings.
fn scratch_cfg(dir: &Path, store: StoreKind) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.store = store;
    c.checkpoint_interval = std::time::Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 256;
    c
}

/// The commit timestamp bound of a moment in ms: every commit in that
/// millisecond is included.
#[must_use]
pub fn ts_of_ms(ms: u64) -> u64 {
    Ts::from_parts(ms, u16::MAX).0
}

/// Rebuilds `tenant`'s unit as of `to_ms` (the newest archived state when
/// `None`) in a scratch engine at `dir`, under tenant id `into`.
///
/// # Errors
/// No backup qualifies, the storage failed, a chunk or segment is damaged,
/// or the archive skips entries.
#[allow(clippy::too_many_lines)]
pub fn rebuild(
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    into: u64,
    to_ms: Option<u64>,
    store: StoreKind,
    dir: &Path,
) -> Result<Rebuilt> {
    let started = Instant::now();
    let bound = to_ms.map_or(u64::MAX, ts_of_ms);
    let m = choose(target, key, tenant, bound)?;
    let unit_id = Engine::unit_of_tenant(tenant).0;
    let ub = m
        .units
        .iter()
        .find(|u| u.unit == unit_id)
        .ok_or_else(|| err(format!("backup {} has no unit {unit_id}", m.taken_ms)))?
        .clone();
    let rekey = Rekey::new(tenant, into);
    let into_unit = Engine::unit_of_tenant(into);
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).map_err(|e| err(format!("create {}: {e}", dir.display())))?;
    let engine = Engine::open(scratch_cfg(dir, store))?;
    let mut asm = Assembly {
        target,
        key,
        tenant,
        unit: &ub,
        rekey: rekey.as_ref(),
        next: 0,
        out: Vec::new(),
        pos: 0,
        entries: 0,
        crc_in: flate2::Crc::new(),
        crc_out: flate2::Crc::new(),
        done: false,
    };
    let index = engine.ingest_unit_from(
        into_unit,
        &mut std::io::BufReader::with_capacity(1 << 20, &mut asm),
    )?;
    if index != ub.index {
        return Err(err(format!(
            "the backup's unit stands at index {index}, its manifest says {}",
            ub.index
        )));
    }
    let mut parts = Vec::new();
    for p in &ub.parts {
        parts.extend_from_slice(&fetch_chunk(target, key, tenant, p)?);
    }
    // Replay. An entry without versioned writes (queue messages only) has
    // no commit timestamp of its own: it is held and applied with the next
    // versioned entry at or before the bound, and dropped if none follows
    // (so a moment never takes changes that may come after it).
    let mut need = ub.index + 1;
    let mut applied_to = ub.index;
    let mut last_ts = ub.max_ts;
    let mut reached = false;
    let mut gap: Option<u64> = None;
    let mut replayed = 0u64;
    let mut batch: Vec<LogEntry> = Vec::new();
    let mut held: Vec<LogEntry> = Vec::new();
    let mut batch_bytes = 0usize;
    let flush = |batch: &mut Vec<LogEntry>, bytes: &mut usize| -> Result<()> {
        if !batch.is_empty() {
            engine.state_machine().apply(into_unit, batch)?;
            batch.clear();
            *bytes = 0;
        }
        Ok(())
    };
    'segs: for (first, last, name) in segments(target, tenant, unit_id)? {
        if last < need {
            continue;
        }
        if first > need {
            // The archive holds nothing for `need`, but later entries.
            gap = Some(need);
            break;
        }
        let sealed = target
            .get(&name)?
            .ok_or_else(|| err(format!("segment {name} vanished")))?;
        for a in decode_segment(unit_id, &key.open(&name, &sealed)?)? {
            if a.index < need {
                continue;
            }
            if a.index > need {
                gap = Some(need);
                break 'segs;
            }
            if a.ts > bound {
                reached = true;
                break 'segs;
            }
            let data = match &rekey {
                None => a.data,
                Some(r) => rekey_payload(&a.data, r)
                    .ok_or_else(|| err(format!("entry {} in {name} is malformed", a.index)))?,
            };
            need += 1;
            let e = LogEntry::new(1, a.index, data);
            if a.ts == 0 && to_ms.is_some() {
                held.push(e);
                continue;
            }
            if a.ts > 0 {
                last_ts = last_ts.max(a.ts);
            }
            for h in held.drain(..).chain(std::iter::once(e)) {
                batch_bytes += h.data.len();
                applied_to = h.index;
                batch.push(h);
                replayed += 1;
            }
            if batch.len() >= 512 || batch_bytes >= 8 << 20 {
                flush(&mut batch, &mut batch_bytes)?;
            }
        }
    }
    if let Some(at) = gap
        && !reached
    {
        return Err(err(format!(
            "the archive of tenant {tenant} has a gap at log entry {at} (retention, or archiving was off or failed), so that moment can't be rebuilt from backup {}; choose a moment before the gap or at a later backup",
            m.taken_ms
        )));
    }
    let need = applied_to + 1;
    flush(&mut batch, &mut batch_bytes)?;
    let sum: SpanChecksum = engine.unit_checksum(into_unit)?;
    Ok(Rebuilt {
        report: RestoreReport {
            tenant,
            into,
            to_ms,
            backup: m.taken_ms,
            backup_index: ub.index,
            replayed,
            index: need - 1,
            ts: last_ts,
            reached,
            entries: sum.entries,
            bytes: sum.bytes,
            crc: sum.crc,
            took_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            mode: String::new(),
        },
        engine,
        unit: into_unit,
        parts,
    })
}

/// A restore check: rebuilds `tenant` as of `to_ms` in a scratch engine
/// under its own id and returns the report with the span checksum. The
/// scratch directory is removed afterwards.
///
/// # Errors
/// As [`rebuild`].
pub fn verify(
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    to_ms: Option<u64>,
    work: &Path,
) -> Result<RestoreReport> {
    let dir = work.join(format!("verify-t{tenant}-{}", now_ms()));
    let r = rebuild(target, key, tenant, tenant, to_ms, StoreKind::Rocks, &dir);
    let out = r.map(|b| b.report);
    let _ = std::fs::remove_dir_all(&dir);
    out
}

fn marker(work: &Path, into: u64) -> PathBuf {
    work.join(format!("restoring-t{into}.json"))
}

/// Whether `unit` holds nothing on `engine`: no keys, no log, no applied
/// index.
fn unit_is_empty(engine: &Engine, unit: UnitId) -> Result<bool> {
    if engine.log_first_index_of(unit).is_some() || engine.last_applied_of(unit) > 0 {
        return Ok(false);
    }
    Ok(engine.unit_checksum(unit)?.entries == 0)
}

/// Restores `tenant` as of `to_ms` into tenant `into` on `engine`. When
/// `into` holds nothing on the engine it is ingested in one step;
/// otherwise `replace` must be set, and what `into` held is replaced
/// through its commit log (see the module docs). `work` holds the scratch
/// engine, the exported files and the marker; everything but the result
/// is removed afterwards.
///
/// # Errors
/// `into` holds data and `replace` isn't set, or as [`rebuild`]. On an
/// error the target is left empty.
#[allow(clippy::too_many_arguments)]
pub fn restore_into(
    engine: &Engine,
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    into: u64,
    to_ms: Option<u64>,
    replace: bool,
    work: &Path,
) -> Result<RestoreReport> {
    if into == keys::META_TENANT || Engine::unit_of_tenant(into) == sprig_engine::NODE_UNIT {
        return Err(invalid("can't restore into the engine's own unit"));
    }
    if into == tenant {
        return Err(invalid(
            "restore into another tenant, not the tenant itself",
        ));
    }
    let into_unit = Engine::unit_of_tenant(into);
    let empty = unit_is_empty(engine, into_unit)?;
    if !empty && !replace {
        return Err(invalid(format!(
            "tenant {into} already holds data on this engine (replace it explicitly)"
        )));
    }
    let mode = if empty { "ingest" } else { "log" };
    std::fs::create_dir_all(work).map_err(|e| err(format!("create {}: {e}", work.display())))?;
    let mark = marker(work, into);
    let stamp = now_ms();
    let scratch = work.join(format!("scratch-t{into}-{stamp}"));
    let export = work.join(format!("export-t{into}-{stamp}"));
    // The rebuild touches only the scratch engine: an error there (no
    // backup at that moment, a gap) leaves the target as it was.
    let built = match rebuild(
        target,
        key,
        tenant,
        into,
        to_ms,
        engine.config().store,
        &scratch,
    ) {
        Ok(b) => b,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&scratch);
            return Err(e);
        }
    };
    write_synced(
        &mark,
        &serde_json::json!({"tenant": tenant, "into": into, "to_ms": to_ms, "started_ms": stamp, "mode": mode})
            .to_string(),
    )?;
    let (scratch_in, export_in) = (scratch.clone(), export.clone());
    let r = (move || -> Result<RestoreReport> {
        let (scratch, export) = (scratch_in, export_in);
        let mut report = built.report.clone();
        mode.clone_into(&mut report.mode);
        let started = Instant::now();
        if empty {
            ingest_files(engine, built, &export)?;
        } else {
            replace_through_log(engine, &built)?;
            drop(built);
        }
        let _ = std::fs::remove_dir_all(&scratch);
        let live = engine.unit_checksum(into_unit)?;
        let same = if empty {
            (live.entries, live.bytes, live.crc) == (report.entries, report.bytes, report.crc)
        } else {
            // GC may already have dropped old versions the rebuild kept.
            live.entries <= report.entries && live.entries > 0 || report.entries == 0
        };
        if !same {
            return Err(err(format!(
                "tenant {into} after the restore doesn't match the rebuilt state ({} entries, expected {})",
                live.entries, report.entries
            )));
        }
        report.took_ms += u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(report)
    })();
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&export);
    match &r {
        Ok(_) => {
            let _ = std::fs::remove_file(&mark);
        }
        Err(_) => {
            // Undo whatever reached the live engine, then drop the marker.
            if empty_target(engine, into_unit, mode).is_ok() {
                let _ = std::fs::remove_file(&mark);
            }
        }
    }
    r
}

/// The one-step form: export the rebuilt unit as files, put the backup's
/// parts in, ingest.
fn ingest_files(engine: &Engine, built: Rebuilt, export: &Path) -> Result<()> {
    let x = built.engine.export_unit(built.unit, export, 64 << 20)?;
    drop(built.engine);
    // The backup's parts replace the scratch engine's (it runs none).
    let mut files = Vec::new();
    for f in x.files {
        let n = f
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned();
        if n == "parts.snap" {
            std::fs::write(&f, &built.parts)
                .map_err(|e| err(format!("write {}: {e}", f.display())))?;
        } else if n == "unit.snap" {
            // The stream form ends with an empty parts section.
            let mut b = std::fs::read(&f).map_err(|e| err(format!("read {}: {e}", f.display())))?;
            if b.len() < 4 || b[b.len() - 4..] != END.to_be_bytes() {
                return Err(err("the scratch export doesn't end as expected"));
            }
            b.truncate(b.len() - 4);
            b.extend_from_slice(&built.parts);
            std::fs::write(&f, &b).map_err(|e| err(format!("write {}: {e}", f.display())))?;
        }
        files.push(f);
    }
    engine.ingest_unit(built.unit, &files)?;
    engine.checkpoint()?;
    Ok(())
}

/// Bytes of raw writes per log entry of a replace.
const ENTRY_BYTES: usize = 4 << 20;

/// Reads a unit's export stream and proposes its data keys to `unit`'s
/// log as raw writes, in entries of about [`ENTRY_BYTES`].
struct LogWriter<'a> {
    engine: &'a Engine,
    unit: UnitId,
    meta: Vec<u8>,
    pending: Vec<u8>,
    started: bool,
    ended: bool,
    payload: Payload,
    first: bool,
    tickets: Vec<sprig_engine::log::Ticket>,
    error: Option<SprigError>,
}

impl LogWriter<'_> {
    fn propose(&mut self) -> Result<()> {
        if self.payload.is_empty() && !self.first {
            return Ok(());
        }
        if self.first {
            // The first entry removes what the target held.
            let r = keys::tenant_range(self.unit.0);
            let mut p = Payload::new();
            p.delete_range(Cf::Data, &r.start, r.end.as_deref().unwrap_or_default());
            let body = std::mem::replace(&mut self.payload, Payload::new()).into_bytes();
            // Append the puts gathered so far after the range delete.
            let mut bytes = p.into_bytes();
            bytes.extend_from_slice(&body[1..]);
            self.first = false;
            return self.send(bytes);
        }
        let body = std::mem::replace(&mut self.payload, Payload::new()).into_bytes();
        self.send(body)
    }

    fn send(&mut self, bytes: Vec<u8>) -> Result<()> {
        let t =
            self.engine
                .consensus()
                .propose(self.unit, bytes, sprig_engine::Durability::Local)?;
        self.tickets.push(t);
        if self.tickets.len() >= 8 {
            for t in self.tickets.drain(..) {
                t.wait_applied()?;
            }
        }
        Ok(())
    }

    fn parse(&mut self) -> Result<()> {
        let mut at = 0usize;
        if !self.started {
            if self.pending.len() < 8 {
                return Ok(());
            }
            if &self.pending[..8] != MAGIC {
                return Err(err("not a unit snapshot stream"));
            }
            at = 8;
            self.started = true;
        }
        while !self.ended {
            let rest = &self.pending[at..];
            if rest.len() < 4 {
                break;
            }
            let kl = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
            if kl == END {
                self.ended = true;
                break;
            }
            let kl = kl as usize;
            if rest.len() < 8 + kl {
                break;
            }
            let vl =
                u32::from_be_bytes(rest[4 + kl..8 + kl].try_into().unwrap_or_default()) as usize;
            if rest.len() < 8 + kl + vl {
                break;
            }
            let (k, v) = (&rest[4..4 + kl], &rest[8 + kl..8 + kl + vl]);
            if !k.starts_with(&self.meta) {
                self.payload.put(Cf::Data, k, v);
            }
            at += 8 + kl + vl;
            if self.payload.len() >= ENTRY_BYTES {
                self.pending.drain(..at);
                at = 0;
                self.propose()?;
            }
        }
        if self.ended {
            self.pending.clear();
        } else {
            self.pending.drain(..at);
        }
        Ok(())
    }
}

impl std::io::Write for LogWriter<'_> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.ended {
            return Ok(b.len());
        }
        self.pending.extend_from_slice(b);
        if self.pending.len() >= 1 << 20
            && let Err(e) = self.parse()
        {
            let m = e.to_string();
            self.error = Some(e);
            return Err(std::io::Error::other(m));
        }
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The replacing form: the rebuilt keys through the target's own log.
fn replace_through_log(engine: &Engine, built: &Rebuilt) -> Result<()> {
    let mut w = LogWriter {
        engine,
        unit: built.unit,
        meta: keys::unit_meta_key(built.unit.0, b""),
        pending: Vec::new(),
        started: false,
        ended: false,
        payload: Payload::new(),
        first: true,
        tickets: Vec::new(),
        error: None,
    };
    let r = built.engine.export_unit_to(built.unit, &mut w);
    if let Some(e) = w.error.take() {
        return Err(e);
    }
    r?;
    w.parse()?;
    w.propose()?;
    for t in w.tickets.drain(..) {
        t.wait_applied()?;
    }
    Ok(())
}

/// Empties a half-restored target: raw for a target that held nothing
/// before (`ingest`), through its log for one that did (`log`).
fn empty_target(engine: &Engine, unit: UnitId, mode: &str) -> Result<()> {
    if mode == "log" {
        let r = keys::tenant_range(unit.0);
        let mut p = Payload::new();
        p.delete_range(Cf::Data, &r.start, r.end.as_deref().unwrap_or_default());
        engine
            .consensus()
            .propose(unit, p.into_bytes(), sprig_engine::Durability::Local)?
            .wait_applied()
    } else {
        clear_unit(engine, unit)
    }
}

fn write_synced(path: &Path, text: &str) -> Result<()> {
    let mut f =
        std::fs::File::create(path).map_err(|e| err(format!("create {}: {e}", path.display())))?;
    f.write_all(text.as_bytes())
        .and_then(|()| f.sync_all())
        .map_err(|e| err(format!("write {}: {e}", path.display())))?;
    if let Some(d) = path.parent()
        && let Ok(d) = std::fs::File::open(d)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Removes every key of `unit` (data and bookkeeping), synced.
fn clear_unit(engine: &Engine, unit: UnitId) -> Result<()> {
    let data = keys::tenant_range(unit.0);
    let meta = keys::KeyRange::prefix(&keys::unit_meta_key(unit.0, b""));
    let mut b = StoreBatch::new();
    for r in [data, meta] {
        b.delete_range(Cf::Data, r.start.clone(), r.end.clone().unwrap_or_default());
    }
    engine.store().write(&b, WalMode::Sync)
}

/// After a restart: a restore the process didn't finish left its marker.
/// The tenant it was restoring into is emptied (it holds all of the
/// restore or none, but its parts may be missing) and the marker and
/// leftover scratch directories are removed. Returns the tenants emptied.
///
/// # Errors
/// The work directory can't be read, or the store failed.
pub fn recover(engine: &Engine, work: &Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(work) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(err(format!("read {}: {e}", work.display()))),
    };
    for ent in rd.flatten() {
        let n = ent.file_name().to_string_lossy().into_owned();
        if let Some(t) = n
            .strip_prefix("restoring-t")
            .and_then(|s| s.strip_suffix(".json"))
            && let Ok(into) = t.parse::<u64>()
        {
            let mode = std::fs::read(ent.path())
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .and_then(|v| v.get("mode").and_then(|m| m.as_str()).map(str::to_owned))
                .unwrap_or_else(|| "ingest".to_owned());
            empty_target(engine, Engine::unit_of_tenant(into), &mode)?;
            let _ = std::fs::remove_file(ent.path());
            tracing::warn!(
                tenant = into,
                "a restore stopped half-way; its tenant was emptied"
            );
            out.push(into);
        } else if ["scratch-", "export-", "verify-", "stage-"]
            .iter()
            .any(|p| n.starts_with(p))
        {
            let _ = std::fs::remove_dir_all(ent.path());
        }
    }
    Ok(out)
}
