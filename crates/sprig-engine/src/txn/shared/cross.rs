//! Commits across units: prepare records in the participants, a
//! transaction record in the anchor unit as the commit point, and
//! resolution afterwards.
//!
//! A transaction whose writes (or notes) fall in several units commits in
//! steps, each a log entry of one unit, so the protocol works on any
//! [`crate::log::Consensus`], replicated or not:
//!
//! 1. **Prepare.** Each participant (every unit but the anchor) gets a
//!    prepare record: its writes, notes and plain writes, and the other keys
//!    of the unit the transaction holds exclusively. The anchor is the unit
//!    with the most writes, so the fewest bytes are written twice.
//! 2. **Commit point.** The anchor's entry carries the anchor's own writes
//!    at the commit timestamp and the transaction record
//!    `COMMITTED(cts)`. Once that entry is durable the transaction has
//!    committed; until then it hasn't.
//! 3. **Resolve.** Each participant's entry applies its writes at the same
//!    commit timestamp and deletes its prepare record. When every
//!    participant is resolved, the record is deleted.
//!
//! **Waiting between steps.** On a log that makes entries durable in the
//! order they were proposed across units (the single-node log), the steps
//! are queued at once: a crash keeps a prefix, and every prefix recovers to
//! all or nothing. Otherwise the coordinator first writes the record as
//! `PENDING`, waits until it and every prepare record are durable, together
//! with every pending version the transaction read on another unit (the
//! commit dependency of early lock release), and only then writes the
//! commit point; participants are resolved once the commit point is
//! durable.
//!
//! **Readers.** Before the commit point a reader's view is older than any
//! commit timestamp the transaction can still take, so it simply doesn't see
//! the prepared writes; a locking reader or writer waits on the
//! transaction's exclusive locks, which it keeps until every participant is
//! resolved. From the commit point on, the participants' writes sit in the
//! pending overlay at the commit timestamp, tied to the commit point's
//! entry, until their own entries replace them.
//!
//! **Stale transactions.** A `PENDING` transaction whose coordinator is gone,
//! or silent for too long, is aborted by compare-and-set on its record,
//! under the anchor unit's commit latch, where the coordinator's own
//! commit-point step checks the record too: exactly one of them wins. After
//! a restart every unfinished transaction is resolved before the first new
//! one: a committed record finishes its participants; a pending or missing
//! one aborts them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use super::{TxnShared, lock_or_recover};
use crate::keys::{self, KeyRange};
use crate::locks::TxnId;
use crate::log::{Durability, Payload};
use crate::store::{Cf, IterOpts};
use crate::txn::CommitPath;
use crate::txn::durable::DurableTicket;
use crate::txn::hlc::Ts;
use crate::txn::mvcc::encode_value_into;
use crate::txn::transaction::Extra;

/// How long a `PENDING` transaction may go without reaching its commit
/// point before a resolver aborts it (unless the stale time is set).
const STALE_MS: u64 = 30_000;

const RECORD: &[u8] = b"tr:";
const PREPARE: &[u8] = b"tp:";
const FORMAT: u8 = 1;

/// A point in the protocol where a test can stop the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossStage {
    /// The prepare records are queued (durable, when the log needs waits).
    Prepared,
    /// The commit point is queued.
    CommitPoint,
    /// About to resolve the participant unit.
    Resolve(u64),
}

/// What a test makes happen at a [`CrossStage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossFault {
    /// Nothing: the coordinator goes on.
    Go,
    /// The coordinator stops here as if its process had died: the commit
    /// call fails, and what it left is finished by a resolver or by
    /// recovery.
    CoordinatorLost,
    /// The participant's resolution is lost on the way (only at
    /// [`CrossStage::Resolve`]); a resolver redoes it later.
    Drop,
}

/// A test's choice of fault per stage.
pub type CrossFaults = Arc<dyn Fn(CrossStage) -> CrossFault + Send + Sync>;

/// The transaction record's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pending,
    Committed(Ts),
    Aborted,
}

/// One unit's share of a commit across units.
#[derive(Debug, Clone, Default)]
pub(super) struct Share {
    pub(super) writes: Vec<(Bytes, Option<Bytes>)>,
    pub(super) extras: Vec<Extra>,
    pub(super) delete: Option<Vec<u8>>,
    /// Other keys of the unit the transaction holds exclusively.
    pub(super) locks: Vec<Bytes>,
}

impl Share {
    fn size(&self) -> usize {
        self.writes
            .iter()
            .map(|(k, v)| k.len() + 24 + v.as_ref().map_or(0, Bytes::len))
            .sum::<usize>()
            + self
                .extras
                .iter()
                .map(|e| match e {
                    Extra::Note(_, b) => b.len() + 8,
                    Extra::Put(_, k, v) => k.len() + v.len() + 8,
                    Extra::Delete(_, k) => k.len() + 8,
                })
                .sum::<usize>()
    }

    /// The entry that applies this share at `cts`.
    fn payload(&self, cts: Ts) -> Payload {
        let mut p = Payload::with_capacity(self.size() + 64);
        let mut val = Vec::new();
        for (k, v) in &self.writes {
            val.clear();
            encode_value_into(&mut val, v.as_deref());
            p.put(Cf::Data, &keys::versioned(k, cts), &val);
        }
        if let Some(k) = &self.delete {
            p.delete(Cf::Data, k);
        }
        for n in &self.extras {
            match n {
                Extra::Note(_, b) => p.note(b),
                Extra::Put(cf, k, v) => p.put(*cf, k, v),
                Extra::Delete(cf, k) => p.delete(*cf, k),
            }
        }
        p
    }
}

/// A commit across units the node knows of: its coordinator's, or one
/// recovery found unfinished.
#[derive(Debug)]
struct Cross {
    /// The lock owner.
    txn: TxnId,
    outcome: Outcome,
    units: Vec<u64>,
    /// Participants not yet resolved (or, after an abort, whose prepare
    /// record isn't deleted yet).
    unresolved: BTreeMap<u64, Share>,
    /// The commit point's entry (`Ready` once known durable).
    point: DurableTicket,
    /// The entry that wrote `ABORTED` into the record.
    abort: DurableTicket,
    /// The participants' finishing entries.
    finished: Vec<DurableTicket>,
    /// A live coordinator is driving it; resolvers leave it alone (except
    /// to abort it when it is stale).
    coordinated: bool,
    started_ms: u64,
    /// Whether the anchor holds a record for it.
    has_record: bool,
    /// Whether its locks were released.
    released: bool,
    durability: Option<Durability>,
}

/// The node's commits across units in progress, and the test switches.
pub(crate) struct CrossTable {
    /// Ordered, so a resolver's proposals come in the same order on every
    /// run (the simulator compares runs).
    map: Mutex<BTreeMap<(u64, u64), Cross>>,
    open: AtomicUsize,
    wait: AtomicBool,
    stale_ms: AtomicU64,
    faults: Mutex<Option<CrossFaults>>,
}

impl Default for CrossTable {
    fn default() -> Self {
        Self {
            map: Mutex::new(BTreeMap::new()),
            open: AtomicUsize::new(0),
            wait: AtomicBool::new(false),
            stale_ms: AtomicU64::new(STALE_MS),
            faults: Mutex::new(None),
        }
    }
}

impl std::fmt::Debug for CrossTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossTable")
            .field("open", &self.open.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// What a commit across units leaves the caller.
pub(crate) struct Committed {
    /// The commit timestamp.
    pub(crate) cts: Ts,
    /// What the reply waits for.
    pub(crate) ticket: DurableTicket,
    /// Entries queued after the reply's (the participants' resolutions on a
    /// log that needs waits): their tickets name each unit's entry.
    pub(crate) later: DurableTicket,
}

// ----- records ---------------------------------------------------------------

fn record_key(anchor: u64, txid: u64) -> Vec<u8> {
    let mut f = Vec::with_capacity(RECORD.len() + 8);
    f.extend_from_slice(RECORD);
    f.extend_from_slice(&txid.to_be_bytes());
    keys::unit_meta_key(anchor, &f)
}

fn prepare_key(unit: u64, anchor: u64, txid: u64) -> Vec<u8> {
    let mut f = Vec::with_capacity(PREPARE.len() + 16);
    f.extend_from_slice(PREPARE);
    f.extend_from_slice(&anchor.to_be_bytes());
    f.extend_from_slice(&txid.to_be_bytes());
    keys::unit_meta_key(unit, &f)
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_len(out, b.len());
    out.extend_from_slice(b);
}

/// ```text
/// record := 0x01 | state u8 (0 pending, 1 committed, 2 aborted) | cts u64
///           | u32 units | unit u64*
/// ```
fn encode_record(outcome: Outcome, units: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(14 + 8 * units.len());
    out.push(FORMAT);
    let (state, ts) = match outcome {
        Outcome::Pending => (0, 0),
        Outcome::Committed(ts) => (1, ts.0),
        Outcome::Aborted => (2, 0),
    };
    out.push(state);
    out.extend_from_slice(&ts.to_be_bytes());
    put_len(&mut out, units.len());
    for u in units {
        out.extend_from_slice(&u.to_be_bytes());
    }
    out
}

/// ```text
/// prepare := 0x01 | u32 writes | write* | u32 extras | extra*
///            | 0x00 or 0x01 key (a reserved key the commit deletes)
///            | u32 locks | (u32 len | key)*
/// write   := u32 len | key | 0x00 u32 len value (put) or 0x01 (delete)
/// extra   := 0x00 tenant-flag u8 tenant u64 u32 len note
///          | 0x01 cf u8 key value (plain put) | 0x02 cf u8 key (plain delete)
/// ```
fn encode_prepare(s: &Share) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.size() + 32);
    out.push(FORMAT);
    put_len(&mut out, s.writes.len());
    for (k, v) in &s.writes {
        put_bytes(&mut out, k);
        match v {
            Some(v) => {
                out.push(0);
                put_bytes(&mut out, v);
            }
            None => out.push(1),
        }
    }
    put_len(&mut out, s.extras.len());
    let cf = |c: &Cf| match c {
        Cf::Data => 0u8,
        Cf::Local => 1u8,
    };
    for e in &s.extras {
        match e {
            Extra::Note(t, b) => {
                out.push(0);
                out.push(u8::from(t.is_some()));
                out.extend_from_slice(&t.unwrap_or(0).to_be_bytes());
                put_bytes(&mut out, b);
            }
            Extra::Put(c, k, v) => {
                out.push(1);
                out.push(cf(c));
                put_bytes(&mut out, k);
                put_bytes(&mut out, v);
            }
            Extra::Delete(c, k) => {
                out.push(2);
                out.push(cf(c));
                put_bytes(&mut out, k);
            }
        }
    }
    match &s.delete {
        Some(k) => {
            out.push(1);
            put_bytes(&mut out, k);
        }
        None => out.push(0),
    }
    put_len(&mut out, s.locks.len());
    for k in &s.locks {
        put_bytes(&mut out, k);
    }
    out
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn bad() -> SprigError {
        SprigError::internal("malformed cross-unit commit record")
    }

    fn take(&mut self, n: usize) -> Result<&[u8]> {
        let (head, rest) = self.0.split_at_checked(n).ok_or_else(Self::bad)?;
        self.0 = rest;
        Ok(head)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        let mut b = [0; 8];
        b.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(b))
    }

    fn len(&mut self) -> Result<usize> {
        let mut b = [0; 4];
        b.copy_from_slice(self.take(4)?);
        Ok(u32::from_be_bytes(b) as usize)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.len()?;
        Ok(self.take(n)?.to_vec())
    }

    fn cf(&mut self) -> Result<Cf> {
        match self.byte()? {
            0 => Ok(Cf::Data),
            1 => Ok(Cf::Local),
            _ => Err(Self::bad()),
        }
    }

    fn done(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Self::bad())
        }
    }
}

/// A transaction record: its outcome and its units.
type RecordBody = (Outcome, Vec<u64>);

fn decode_record(raw: &[u8]) -> Result<RecordBody> {
    let mut r = Reader(raw);
    if r.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    let state = r.byte()?;
    let ts = r.u64()?;
    let outcome = match state {
        0 => Outcome::Pending,
        1 => Outcome::Committed(Ts(ts)),
        2 => Outcome::Aborted,
        _ => return Err(Reader::bad()),
    };
    let n = r.len()?;
    let mut units = Vec::with_capacity(n.min(1 << 10));
    for _ in 0..n {
        units.push(r.u64()?);
    }
    r.done()?;
    Ok((outcome, units))
}

fn decode_prepare(raw: &[u8]) -> Result<Share> {
    let mut rd = Reader(raw);
    if rd.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    let mut out = Share::default();
    let n = rd.len()?;
    for _ in 0..n {
        let k = Bytes::from(rd.bytes()?);
        let v = match rd.byte()? {
            0 => Some(Bytes::from(rd.bytes()?)),
            1 => None,
            _ => return Err(Reader::bad()),
        };
        out.writes.push((k, v));
    }
    let n = rd.len()?;
    for _ in 0..n {
        let e = match rd.byte()? {
            0 => {
                let has = rd.byte()? != 0;
                let t = rd.u64()?;
                Extra::Note(has.then_some(t), rd.bytes()?)
            }
            1 => {
                let cf = rd.cf()?;
                let k = rd.bytes()?;
                Extra::Put(cf, k, rd.bytes()?)
            }
            2 => {
                let cf = rd.cf()?;
                Extra::Delete(cf, rd.bytes()?)
            }
            _ => return Err(Reader::bad()),
        };
        out.extras.push(e);
    }
    out.delete = match rd.byte()? {
        0 => None,
        1 => Some(rd.bytes()?),
        _ => return Err(Reader::bad()),
    };
    let n = rd.len()?;
    for _ in 0..n {
        out.locks.push(Bytes::from(rd.bytes()?));
    }
    rd.done()?;
    Ok(out)
}

/// The anchor: the unit with the most writes (the lowest on a tie), so the
/// fewest writes go into prepare records.
fn anchor_of(shares: &BTreeMap<u64, Share>) -> u64 {
    let mut best: Option<(usize, u64)> = None;
    for (u, s) in shares {
        if best.is_none_or(|(n, _)| s.writes.len() > n) {
            best = Some((s.writes.len(), *u));
        }
    }
    best.map_or(0, |b| b.1)
}

/// Proposes `payload` to `unit` alone.
fn propose_one(
    path: &dyn CommitPath,
    unit: u64,
    payload: Vec<u8>,
    durability: Option<Durability>,
) -> Result<DurableTicket> {
    path.propose_many(vec![(unit, payload)], durability)?
        .pop()
        .ok_or_else(|| SprigError::internal("the log returned no ticket"))
}

fn put_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = Payload::with_capacity(key.len() + value.len() + 16);
    p.put(Cf::Data, key, value);
    p.into_bytes()
}

fn delete_entry(key: &[u8]) -> Vec<u8> {
    let mut p = Payload::with_capacity(key.len() + 16);
    p.delete(Cf::Data, key);
    p.into_bytes()
}

/// Records `ABORTED` in the anchor's record, if it has one.
fn write_aborted(path: &dyn CommitPath, key: (u64, u64), c: &mut Cross) {
    if c.has_record {
        match propose_one(
            path,
            key.0,
            put_entry(
                &record_key(key.0, key.1),
                &encode_record(Outcome::Aborted, &c.units),
            ),
            c.durability,
        ) {
            Ok(t) => c.abort = t,
            // The log is closed: recovery finds the record pending and
            // aborts it.
            Err(_) => c.abort = DurableTicket::Ready,
        }
    }
}

impl TxnShared {
    // ----- switches -----------------------------------------------------------

    /// Makes commits across units wait between their steps even on a log
    /// that makes entries durable in order (tests run the replicated
    /// protocol on one node this way).
    pub fn set_cross_unit_wait(&self, on: bool) {
        self.cross.wait.store(on, Ordering::Relaxed);
    }

    /// How long a pending commit across units may go without reaching its
    /// commit point before a resolver aborts it.
    pub fn set_cross_unit_stale_ms(&self, ms: u64) {
        self.cross.stale_ms.store(ms, Ordering::Relaxed);
    }

    /// Sets (or clears) a test's faults for commits across units.
    pub fn set_cross_unit_faults(&self, faults: Option<CrossFaults>) {
        *lock_or_recover(&self.cross.faults) = faults;
    }

    /// Commits across units not yet finished (records not yet deleted).
    #[must_use]
    pub fn cross_unit_open(&self) -> usize {
        self.cross.open.load(Ordering::Acquire)
    }

    fn fault(&self, stage: CrossStage) -> CrossFault {
        let f = lock_or_recover(&self.cross.faults).clone();
        f.map_or(CrossFault::Go, |f| f(stage))
    }

    /// Whether the protocol's steps may be queued together.
    pub(super) fn ordered(&self, path: &dyn CommitPath) -> bool {
        path.durable_in_order() && !self.cross.wait.load(Ordering::Relaxed)
    }

    /// Whether a commit across units that `txn` coordinated still holds
    /// `txn`'s locks (its coordinator stopped part way; a resolver releases
    /// them).
    pub(crate) fn cross_holds_locks(&self, txn: TxnId) -> bool {
        self.cross.open.load(Ordering::Acquire) > 0
            && lock_or_recover(&self.cross.map)
                .values()
                .any(|c| c.txn == txn && !c.released)
    }

    // ----- the coordinator ------------------------------------------------------

    /// Commits `shares` (one per unit, two or more) atomically.
    pub(super) fn commit_across(
        &self,
        path: &dyn CommitPath,
        txn: TxnId,
        mut shares: BTreeMap<u64, Share>,
        durability: Option<Durability>,
        first: Option<u64>,
        dep: &DurableTicket,
    ) -> Result<Committed> {
        // Locks and commit points live on each unit's leader, so every unit
        // must be led where the transaction runs. Units led by other nodes
        // are asked to move their leadership here, and the commit fails as
        // retryable meanwhile; a node that leads none of them only refuses
        // (its client belongs on another node).
        let away: Vec<u64> = shares
            .keys()
            .copied()
            .filter(|u| path.leader_epoch_of(*u) == 0)
            .collect();
        if !away.is_empty() {
            if away.len() < shares.len() {
                for u in &away {
                    path.request_lead(*u);
                }
            }
            return Err(SprigError::new(
                sprig_common::SprigCode::ShardMoved,
                format!(
                    "the transaction's units are led by different nodes (units {away:?} are led \
                     elsewhere); their leadership is moving to one node; retry"
                ),
            ));
        }
        let ordered = self.ordered(path);
        // A reserved key the commit deletes (an XA transaction's record)
        // goes with the commit point, so a crash can't leave it behind a
        // commit: its unit anchors. Otherwise the unit with most writes.
        let anchor = shares
            .iter()
            .find(|(_, s)| s.delete.is_some())
            .map_or_else(|| anchor_of(&shares), |(u, _)| *u);
        let units: Vec<u64> = shares.keys().copied().collect();
        // Every other key the transaction holds exclusively goes into its
        // unit's prepare record, so a participant that learns the outcome
        // late keeps blocking writers of those keys too.
        for k in self.locks.exclusive_keys(txn) {
            let u = path.unit_of(&k);
            if u != anchor
                && let Some(s) = shares.get_mut(&u)
                && s.writes
                    .binary_search_by(|(w, _)| w.as_ref().cmp(k.as_ref()))
                    .is_err()
            {
                s.locks.push(k);
            }
        }
        let mine = shares.remove(&anchor).unwrap_or_default();
        let others = shares;
        // The record's id: a fresh timestamp of the node's clock, unique
        // here and above every id taken before a restart.
        let key = (anchor, self.hlc.now().0);
        let mut m = lock_or_recover(&self.cross.map);
        m.insert(
            key,
            Cross {
                txn,
                outcome: Outcome::Pending,
                units,
                unresolved: others.clone(),
                point: DurableTicket::Ready,
                abort: DurableTicket::Ready,
                finished: Vec::new(),
                coordinated: true,
                started_ms: self.hlc.physical_ms(),
                has_record: !ordered,
                released: false,
                durability,
            },
        );
        self.cross.open.fetch_add(1, Ordering::AcqRel);
        drop(m);
        let done = if ordered {
            self.across_ordered(path, key, &mine, &others, durability, first)
        } else {
            self.across_waiting(path, key, &mine, &others, durability, dep)
        }?;
        // Waiters for its locks must open views that see it.
        self.wait_visible(done.cts);
        self.finish_cross(path, key, &done.ticket.clone().max(done.later.clone()));
        Ok(done)
    }

    /// The steps queued at once (a log durable in proposal order), under
    /// every unit's commit latch so each unit's entries keep timestamp order.
    fn across_ordered(
        &self,
        path: &dyn CommitPath,
        key: (u64, u64),
        mine: &Share,
        others: &BTreeMap<u64, Share>,
        durability: Option<Durability>,
        first: Option<u64>,
    ) -> Result<Committed> {
        let (anchor, txid) = key;
        let mut all: Vec<u64> = others.keys().copied().collect();
        all.push(anchor);
        all.sort_unstable();
        let latches: Vec<_> = all.iter().map(|u| self.latch(*u)).collect();
        let held: Vec<_> = latches.iter().map(|l| lock_or_recover(l)).collect();
        // A resolver may have found it stale before the latches were taken.
        if !self.cross_is(key, Outcome::Pending) {
            drop(held);
            return Err(self.abort_own(path, key, super::super::errors::deadlock()));
        }
        for (u, s) in others {
            if let Err(e) = propose_one(
                path,
                *u,
                put_entry(&prepare_key(*u, anchor, txid), &encode_prepare(s)),
                durability,
            ) {
                drop(held);
                return Err(self.abort_own(path, key, e));
            }
        }
        if self.fault(CrossStage::Prepared) == CrossFault::CoordinatorLost {
            drop(held);
            return Err(self.lose(key));
        }
        let (cts, point) = match self.commit_point(path, key, mine, others, durability) {
            Ok(x) => x,
            Err(e) => {
                drop(held);
                return Err(self.abort_own(path, key, e));
            }
        };
        let mut tickets = vec![(anchor, point.clone())];
        let mut lost = self.fault(CrossStage::CommitPoint) == CrossFault::CoordinatorLost;
        if !lost {
            lost = self.resolve_all(path, key, cts, &point, others, durability, &mut tickets);
        }
        self.commit_seq.fetch_add(1, Ordering::AcqRel);
        self.publish_commit_ts(cts);
        drop(held);
        if lost {
            return Err(self.lose(key));
        }
        // The notes' unit first: observers report outcomes under its entry.
        if let Some(f) = first
            && let Some(i) = tickets.iter().position(|(u, _)| *u == f)
        {
            let t = tickets.remove(i);
            tickets.insert(0, t);
        }
        Ok(Committed {
            cts,
            ticket: DurableTicket::all(tickets.into_iter().map(|(_, t)| t).collect()),
            later: DurableTicket::Ready,
        })
    }

    /// The steps with waits (a log whose units become durable
    /// independently).
    fn across_waiting(
        &self,
        path: &dyn CommitPath,
        key: (u64, u64),
        mine: &Share,
        others: &BTreeMap<u64, Share>,
        durability: Option<Durability>,
        dep: &DurableTicket,
    ) -> Result<Committed> {
        let (anchor, txid) = key;
        let units = self.cross_units(key);
        let mut waits = match propose_one(
            path,
            anchor,
            put_entry(
                &record_key(anchor, txid),
                &encode_record(Outcome::Pending, &units),
            ),
            durability,
        ) {
            Ok(t) => t,
            Err(e) => return Err(self.abort_own(path, key, e)),
        };
        for (u, s) in others {
            match propose_one(
                path,
                *u,
                put_entry(&prepare_key(*u, anchor, txid), &encode_prepare(s)),
                durability,
            ) {
                Ok(t) => waits = waits.max(t),
                Err(e) => return Err(self.abort_own(path, key, e)),
            }
        }
        // Whatever the transaction read on other units must be durable
        // before its commit point is.
        waits = waits.max(dep.clone());
        if let Err(e) = path.settle(&waits) {
            return Err(self.abort_own(path, key, e));
        }
        if self.fault(CrossStage::Prepared) == CrossFault::CoordinatorLost {
            return Err(self.lose(key));
        }
        let (cts, point) = {
            let latch = self.latch(anchor);
            let _g = lock_or_recover(&latch);
            // Compare-and-set: a resolver may have aborted it meanwhile.
            if !self.cross_is(key, Outcome::Pending) {
                return Err(self.abort_own(path, key, super::super::errors::deadlock()));
            }
            let (cts, point) = match self.commit_point(path, key, mine, others, durability) {
                Ok(x) => x,
                Err(e) => return Err(self.abort_own(path, key, e)),
            };
            self.commit_seq.fetch_add(1, Ordering::AcqRel);
            self.publish_commit_ts(cts);
            (cts, point)
        };
        if self.fault(CrossStage::CommitPoint) == CrossFault::CoordinatorLost {
            return Err(self.lose(key));
        }
        // No participant applies before the commit point is durable.
        if let Err(e) = path.settle(&point) {
            self.lose(key);
            return Err(e);
        }
        let mut tickets = Vec::new();
        if self.resolve_all(path, key, cts, &point, others, durability, &mut tickets) {
            return Err(self.lose(key));
        }
        Ok(Committed {
            cts,
            ticket: point,
            later: DurableTicket::all(tickets.into_iter().map(|(_, t)| t).collect()),
        })
    }

    /// Writes the commit point: the anchor's writes at a fresh commit
    /// timestamp and the record `COMMITTED(cts)`. Its versions, and the
    /// participants' (tied to this entry until they are resolved), enter
    /// the overlay. The caller holds the anchor's latch and publishes `cts`.
    fn commit_point(
        &self,
        path: &dyn CommitPath,
        key: (u64, u64),
        mine: &Share,
        others: &BTreeMap<u64, Share>,
        durability: Option<Durability>,
    ) -> Result<(Ts, DurableTicket)> {
        let (anchor, txid) = key;
        let units = self.cross_units(key);
        let cts = self.take_commit_ts();
        let mut p = mine.payload(cts);
        p.put(
            Cf::Data,
            &record_key(anchor, txid),
            &encode_record(Outcome::Committed(cts), &units),
        );
        let point = match propose_one(path, anchor, p.into_bytes(), durability) {
            Ok(t) => t,
            Err(e) => {
                self.publish_commit_ts(cts);
                return Err(e);
            }
        };
        if let Some(c) = lock_or_recover(&self.cross.map).get_mut(&key) {
            c.outcome = Outcome::Committed(cts);
            c.point = point.clone();
            c.has_record = true;
        }
        self.pend(anchor, &mine.writes, cts, &point);
        for s in others.values() {
            if !s.writes.is_empty() {
                self.overlay
                    .insert(cts, &point, s.writes.iter().map(|(k, v)| (k, v)));
            }
        }
        Ok((cts, point))
    }

    /// Resolves every participant in `others`; true if a fault stopped the
    /// coordinator. A dropped or refused resolution stays for a resolver.
    #[allow(clippy::too_many_arguments)]
    fn resolve_all(
        &self,
        path: &dyn CommitPath,
        key: (u64, u64),
        cts: Ts,
        point: &DurableTicket,
        others: &BTreeMap<u64, Share>,
        durability: Option<Durability>,
        tickets: &mut Vec<(u64, DurableTicket)>,
    ) -> bool {
        for (u, s) in others {
            match self.fault(CrossStage::Resolve(*u)) {
                CrossFault::CoordinatorLost => return true,
                CrossFault::Drop => continue,
                CrossFault::Go => {}
            }
            if let Ok(t) = self.resolve_share(path, key, cts, point, *u, s, durability) {
                if let Some(c) = lock_or_recover(&self.cross.map).get_mut(&key) {
                    c.unresolved.remove(u);
                    c.finished.push(t.clone());
                }
                tickets.push((*u, t));
            }
        }
        false
    }

    /// Queues participant `unit`'s resolution: its writes at `cts`, its
    /// notes and plain writes, and the deletion of its prepare record. Its
    /// versions in the overlay move from the commit point's entry to this
    /// one (or enter it, after a restart).
    #[allow(clippy::too_many_arguments)]
    fn resolve_share(
        &self,
        path: &dyn CommitPath,
        key: (u64, u64),
        cts: Ts,
        point: &DurableTicket,
        unit: u64,
        share: &Share,
        durability: Option<Durability>,
    ) -> Result<DurableTicket> {
        let mut p = share.payload(cts);
        p.delete(Cf::Data, &prepare_key(unit, key.0, key.1));
        let t = propose_one(path, unit, p.into_bytes(), durability)?;
        if !share.writes.is_empty() {
            let moved = self
                .overlay
                .retarget(share.writes.iter().map(|(k, _)| k), cts, point, &t);
            if moved == 0 {
                self.overlay
                    .insert(cts, &t, share.writes.iter().map(|(k, v)| (k, v)));
            }
            lock_or_recover(&self.unapplied).push_back((
                t.unit_key().unwrap_or(u64::MAX),
                t.index(),
                share.writes.iter().map(|(k, _)| k.clone()).collect(),
            ));
        }
        Ok(t)
    }

    /// Adds one unit's committed versions to the overlay.
    fn pend(&self, _unit: u64, writes: &[(Bytes, Option<Bytes>)], cts: Ts, t: &DurableTicket) {
        if writes.is_empty() {
            return;
        }
        self.overlay
            .insert(cts, t, writes.iter().map(|(k, v)| (k, v)));
        lock_or_recover(&self.unapplied).push_back((
            t.unit_key().unwrap_or(u64::MAX),
            t.index(),
            writes.iter().map(|(k, _)| k.clone()).collect(),
        ));
    }

    fn cross_units(&self, key: (u64, u64)) -> Vec<u64> {
        lock_or_recover(&self.cross.map)
            .get(&key)
            .map(|c| c.units.clone())
            .unwrap_or_default()
    }

    fn cross_is(&self, key: (u64, u64), outcome: Outcome) -> bool {
        lock_or_recover(&self.cross.map)
            .get(&key)
            .is_some_and(|c| c.outcome == outcome)
    }

    /// The coordinator stops (a fault, or an outcome it can't learn):
    /// resolvers take over; the locks stay until they finish.
    fn lose(&self, key: (u64, u64)) -> SprigError {
        if let Some(c) = lock_or_recover(&self.cross.map).get_mut(&key) {
            c.coordinated = false;
        }
        super::super::errors::outcome_lost()
    }

    /// The coordinator aborts its own transaction before the commit point.
    fn abort_own(&self, path: &dyn CommitPath, key: (u64, u64), e: SprigError) -> SprigError {
        {
            let mut m = lock_or_recover(&self.cross.map);
            if let Some(c) = m.get_mut(&key) {
                if c.outcome == Outcome::Pending {
                    c.outcome = Outcome::Aborted;
                    write_aborted(path, key, c);
                }
                c.coordinated = false;
            }
        }
        self.drive_entry(path, key);
        e
    }

    /// After the coordinator's last step: releases the locks once every
    /// participant is resolved, and leaves the rest to the resolver.
    fn finish_cross(&self, path: &dyn CommitPath, key: (u64, u64), ticket: &DurableTicket) {
        let (all, txn) = {
            let mut m = lock_or_recover(&self.cross.map);
            let Some(c) = m.get_mut(&key) else { return };
            c.coordinated = false;
            let all = c.unresolved.is_empty();
            if all {
                c.released = true;
            }
            (all, c.txn)
        };
        if all {
            if self.early_lock_release() {
                self.locks.release_all(txn);
            } else {
                self.hold_until_durable(ticket.clone(), txn);
            }
        }
        // Every participant resolved: the record can go (on a log durable
        // in order, at once). A lost resolution waits for a resolver.
        if all {
            self.drive_entry(path, key);
        }
    }

    // ----- the resolver ---------------------------------------------------------

    /// Moves every unfinished commit across units on: resolves participants
    /// whose commit point is durable, aborts stale pending transactions by
    /// compare-and-set, deletes records once their participants are done.
    /// Never blocks on the log.
    pub(crate) fn drive_cross(&self, path: &dyn CommitPath) {
        if self.cross.open.load(Ordering::Acquire) == 0 {
            return;
        }
        let keys: Vec<(u64, u64)> = lock_or_recover(&self.cross.map).keys().copied().collect();
        for k in keys {
            self.drive_entry(path, k);
        }
    }

    fn drive_entry(&self, path: &dyn CommitPath, key: (u64, u64)) {
        let ordered = self.ordered(path);
        let now = self.hlc.physical_ms();
        let stale_ms = self.cross.stale_ms.load(Ordering::Relaxed);
        let stale = |c: &Cross| {
            c.outcome == Outcome::Pending
                && (!c.coordinated || now.saturating_sub(c.started_ms) >= stale_ms)
        };
        if lock_or_recover(&self.cross.map)
            .get(&key)
            .is_some_and(&stale)
        {
            // Compare-and-set under the anchor's latch, where the
            // coordinator checks the record before its commit point.
            let latch = self.latch(key.0);
            let _g = lock_or_recover(&latch);
            let mut m = lock_or_recover(&self.cross.map);
            if let Some(c) = m.get_mut(&key)
                && stale(c)
            {
                c.outcome = Outcome::Aborted;
                c.coordinated = false;
                write_aborted(path, key, c);
            }
        }
        let mut m = lock_or_recover(&self.cross.map);
        let Some(c) = m.get_mut(&key) else { return };
        if c.coordinated {
            return;
        }
        match c.outcome {
            Outcome::Pending => return,
            Outcome::Committed(cts) => {
                if !c.unresolved.is_empty() && (ordered || c.point.is_durable()) {
                    for (u, s) in std::mem::take(&mut c.unresolved) {
                        match self.resolve_share(path, key, cts, &c.point, u, &s, c.durability) {
                            Ok(t) => c.finished.push(t),
                            Err(_) => {
                                c.unresolved.insert(u, s);
                            }
                        }
                    }
                }
            }
            Outcome::Aborted => {
                // Prepare records go only once `ABORTED` is durable, so a
                // coordinator that took over can't still commit.
                if !c.unresolved.is_empty() && (ordered || !c.has_record || c.abort.is_durable()) {
                    for (u, s) in std::mem::take(&mut c.unresolved) {
                        match propose_one(
                            path,
                            u,
                            delete_entry(&prepare_key(u, key.0, key.1)),
                            c.durability,
                        ) {
                            Ok(t) => c.finished.push(t),
                            Err(_) => {
                                c.unresolved.insert(u, s);
                            }
                        }
                    }
                }
            }
        }
        let aborted = c.outcome == Outcome::Aborted;
        if c.unresolved.is_empty() || aborted {
            // Aborted: nothing of it is visible; committed: every
            // participant's versions are in the overlay.
            if !c.released {
                c.released = true;
                if aborted || self.early_lock_release() {
                    self.locks.release_all(c.txn);
                } else {
                    self.hold_until_durable(DurableTicket::all(c.finished.clone()), c.txn);
                }
            }
        }
        if c.unresolved.is_empty() && (ordered || c.finished.iter().all(DurableTicket::is_durable))
        {
            if c.has_record
                && propose_one(
                    path,
                    key.0,
                    delete_entry(&record_key(key.0, key.1)),
                    c.durability,
                )
                .is_err()
            {
                return;
            }
            m.remove(&key);
            self.cross.open.fetch_sub(1, Ordering::AcqRel);
        }
    }

    // ----- recovery -------------------------------------------------------------

    /// Reads back unfinished commits across units after a restart (or a
    /// unit's snapshot ingest: `only` that unit's records) and resolves
    /// them before any new transaction runs.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn load_cross(&self, path: &dyn CommitPath, only: Option<u64>) {
        let all = keys::unit_meta_key(0, b"");
        let prefix = &all[..all.len() - 8];
        let mut records: Vec<((u64, u64), Result<RecordBody>)> = Vec::new();
        let mut prepares: Vec<(u64, (u64, u64), Result<Share>)> = Vec::new();
        {
            let mut it = path
                .store()
                .iter(Cf::Data, IterOpts::range(&KeyRange::prefix(prefix)));
            it.seek_to_first();
            while it.valid() {
                let rest = &it.key()[prefix.len()..];
                if rest.len() >= 8 {
                    let unit = u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8]));
                    let field = &rest[8..];
                    let be = |b: &[u8]| u64::from_be_bytes(b.try_into().unwrap_or([0; 8]));
                    // Every record is read (a participant's anchor may be
                    // another unit); prepares of the unit asked for, or
                    // anchored there.
                    if let Some(id) = field.strip_prefix(RECORD)
                        && id.len() == 8
                    {
                        records.push(((unit, be(id)), decode_record(it.value())));
                    } else if let Some(id) = field.strip_prefix(PREPARE)
                        && id.len() == 16
                        && only.is_none_or(|u| u == unit || u == be(&id[..8]))
                    {
                        prepares.push((
                            unit,
                            (be(&id[..8]), be(&id[8..])),
                            decode_prepare(it.value()),
                        ));
                    }
                }
                it.next();
            }
            if let Err(e) = it.status().and_then(crate::store::Read::expect_ready) {
                tracing::error!(error = %e, "reading cross-unit commit records failed; they are left for the next start");
                return;
            }
        }
        // Only the records of the unit asked for, or of a prepare read.
        if let Some(u) = only {
            let wanted: std::collections::BTreeSet<(u64, u64)> =
                prepares.iter().map(|(_, k, _)| *k).collect();
            records.retain(|(k, _)| k.0 == u || wanted.contains(k));
        }
        if records.is_empty() && prepares.is_empty() {
            return;
        }
        let mut found: BTreeMap<(u64, u64), Cross> = BTreeMap::new();
        let fresh = |s: &Self, outcome: Outcome, units: Vec<u64>, has_record: bool| Cross {
            txn: s.fresh_txn_id(),
            outcome,
            units,
            unresolved: BTreeMap::new(),
            point: DurableTicket::Ready,
            abort: DurableTicket::Ready,
            finished: Vec::new(),
            coordinated: false,
            started_ms: 0,
            has_record,
            released: false,
            durability: None,
        };
        // A transaction with an unreadable record is left alone (its
        // participants stay prepared, holding nothing in memory) rather
        // than guessed at.
        let mut damaged = std::collections::BTreeSet::new();
        let mut committed_ts = Vec::new();
        for (key, r) in records {
            match r {
                Ok((outcome, units)) => {
                    if let Outcome::Committed(ts) = outcome {
                        committed_ts.push(ts);
                    }
                    found.insert(key, fresh(self, outcome, units, true));
                }
                Err(e) => {
                    tracing::error!(anchor = key.0, txid = key.1, error = %e, "a cross-unit transaction record can't be read; its participants are left prepared");
                    damaged.insert(key);
                }
            }
        }
        for (unit, key, r) in prepares {
            match r {
                Ok(share) if !damaged.contains(&key) => {
                    // No record: the commit point was never written, so it
                    // aborts.
                    found
                        .entry(key)
                        .or_insert_with(|| fresh(self, Outcome::Aborted, Vec::new(), false))
                        .unresolved
                        .insert(unit, share);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(unit, anchor = key.0, txid = key.1, error = %e, "a cross-unit prepare record can't be read; its transaction is left unresolved");
                    damaged.insert(key);
                }
            }
        }
        let mut keys = Vec::new();
        {
            let mut m = lock_or_recover(&self.cross.map);
            for (k, c) in found {
                if damaged.contains(&k) {
                    continue;
                }
                if let std::collections::btree_map::Entry::Vacant(v) = m.entry(k) {
                    // Its participants hold their keys until resolved.
                    for share in c.unresolved.values() {
                        let weight = Arc::new(AtomicUsize::new(share.writes.len()));
                        for key in share.writes.iter().map(|(k, _)| k).chain(&share.locks) {
                            self.locks.restore_exclusive(c.txn, &weight, key);
                        }
                    }
                    v.insert(c);
                    keys.push(k);
                }
            }
            self.cross.open.fetch_add(keys.len(), Ordering::AcqRel);
        }
        for k in keys {
            self.drive_entry(path, k);
        }
        // The clock moves above the recovered commits once their
        // participants' versions are in the overlay.
        for ts in committed_ts {
            self.observe(ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn records_round_trip() {
        for o in [
            Outcome::Pending,
            Outcome::Committed(Ts(77)),
            Outcome::Aborted,
        ] {
            let raw = encode_record(o, &[3, 9]);
            assert_eq!(decode_record(&raw).unwrap(), (o, vec![3, 9]));
            for cut in 0..raw.len() {
                assert!(decode_record(&raw[..cut]).is_err());
            }
        }
        let s = Share {
            writes: vec![(b("a"), Some(b("1"))), (b("b"), None)],
            extras: vec![
                Extra::Note(Some(5), b"n".to_vec()),
                Extra::Note(None, vec![]),
                Extra::Put(Cf::Local, b"k".to_vec(), b"v".to_vec()),
                Extra::Delete(Cf::Data, b"d".to_vec()),
            ],
            delete: Some(b"x".to_vec()),
            locks: vec![b("l")],
        };
        let raw = encode_prepare(&s);
        let back = decode_prepare(&raw).unwrap();
        assert_eq!(back.writes, s.writes);
        assert_eq!(back.delete, s.delete);
        assert_eq!(back.locks, s.locks);
        assert_eq!(format!("{:?}", back.extras), format!("{:?}", s.extras));
        for cut in 0..raw.len() {
            assert!(decode_prepare(&raw[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn the_anchor_is_the_unit_with_most_writes() {
        let mut m = BTreeMap::new();
        let w = |n: usize| Share {
            writes: (0..n).map(|i| (b(&i.to_string()), None)).collect(),
            ..Share::default()
        };
        m.insert(4, w(1));
        m.insert(7, w(3));
        m.insert(9, w(3));
        assert_eq!(anchor_of(&m), 7);
        let mut n = BTreeMap::new();
        n.insert(5, w(0));
        n.insert(2, w(0));
        assert_eq!(anchor_of(&n), 2);
    }

    #[test]
    fn records_are_kept_per_unit() {
        assert!(record_key(3, 1).starts_with(&keys::unit_meta_key(3, RECORD)));
        assert!(prepare_key(4, 3, 1).starts_with(&keys::unit_meta_key(4, PREPARE)));
        assert_ne!(prepare_key(4, 3, 1), prepare_key(4, 3, 2));
    }
}
