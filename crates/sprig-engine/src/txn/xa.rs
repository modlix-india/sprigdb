//! XA on one unit: `XA PREPARE`, `XA COMMIT`, `XA ROLLBACK`, `XA RECOVER`.
//!
//! **Prepare** makes the transaction's outcome independent of its session
//! and of a crash. Its buffered writes (the intents) and the keys it holds
//! exclusively (the lock list) go into the log as one record, stored under
//! a reserved key of the unit; the client is answered once that entry is
//! durable. The transaction then leaves its session and waits in the unit's
//! list of prepared transactions, where any session can finish it by its
//! XID. At prepare it gives up its shared record locks and its gap locks,
//! so a prepared transaction never blocks an insert or a reader's shared
//! lock; it keeps its exclusive record locks. (At SERIALIZABLE it keeps
//! everything, as `InnoDB` does.)
//!
//! **Commit** writes the versions at a fresh commit timestamp and deletes
//! the prepare record in the same log entry, so after a crash the
//! transaction is either still prepared or committed, never both and never
//! neither. **Rollback** deletes the record and releases the locks.
//!
//! **Recovery.** Before the unit's first transaction after a restart, the
//! prepare records in the store are read back: each becomes a prepared
//! transaction again, holding its exclusive locks, listed by `XA RECOVER`.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use super::durable::DurableTicket;
use super::shared::PreparedTxn;
use super::transaction::{AbortReason, CommitTicket, Phase};
use super::{CommitPath, Isolation, Persona, Txn, TxnShared};
use crate::keys::{self, KeyRange};
use crate::log::Payload;
use crate::store::{Cf, IterOpts};

/// `ER_XAER_NOTA`: no prepared transaction has this XID.
pub const ER_XAER_NOTA: u16 = 1397;
/// `ER_XAER_DUPID`: a prepared transaction already has this XID.
pub const ER_XAER_DUPID: u16 = 1440;

/// The longest XID the engine stores, in bytes (a format id, and a global
/// and a branch part of up to 64 bytes each, however the caller packs them).
pub const MAX_XID_LEN: usize = 255;

/// 1397: unknown XID.
#[must_use]
pub fn xaer_nota() -> SprigError {
    SprigError::standard(ER_XAER_NOTA, *b"XAE04", "XAER_NOTA: Unknown XID")
}

/// 1440: the XID is taken.
#[must_use]
pub fn xaer_dupid() -> SprigError {
    SprigError::standard(
        ER_XAER_DUPID,
        *b"XAE08",
        "XAER_DUPID: The XID already exists",
    )
}

/// The field name of prepare records among a unit's bookkeeping keys.
const FIELD: &[u8] = b"xa:";
const RECORD_FORMAT: u8 = 1;

/// The reserved key of `xid`'s prepare record.
fn record_key(unit: u64, xid: &[u8]) -> Vec<u8> {
    let mut field = Vec::with_capacity(FIELD.len() + xid.len());
    field.extend_from_slice(FIELD);
    field.extend_from_slice(xid);
    keys::unit_meta_key(unit, &field)
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes());
}

/// A prepare record: the intents, then the other exclusively locked keys.
///
/// ```text
/// record := 0x01 | u32 writes | write* | u32 locks | (u32 len | key)*
/// write  := u32 len | key | 0x00 (put) u32 len value | 0x01 (delete)
/// ```
fn encode(writes: &[(Bytes, Option<Bytes>)], locks: &[Bytes]) -> Vec<u8> {
    let mut out = vec![RECORD_FORMAT];
    put_len(&mut out, writes.len());
    for (k, v) in writes {
        put_len(&mut out, k.len());
        out.extend_from_slice(k);
        match v {
            Some(v) => {
                out.push(0);
                put_len(&mut out, v.len());
                out.extend_from_slice(v);
            }
            None => out.push(1),
        }
    }
    put_len(&mut out, locks.len());
    for k in locks {
        put_len(&mut out, k.len());
        out.extend_from_slice(k);
    }
    out
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn bad() -> SprigError {
        SprigError::internal("malformed XA prepare record")
    }

    fn byte(&mut self) -> Result<u8> {
        let (&b, rest) = self.0.split_first().ok_or_else(Self::bad)?;
        self.0 = rest;
        Ok(b)
    }

    fn len(&mut self) -> Result<usize> {
        let (head, rest) = self.0.split_at_checked(4).ok_or_else(Self::bad)?;
        self.0 = rest;
        let mut b = [0; 4];
        b.copy_from_slice(head);
        Ok(u32::from_be_bytes(b) as usize)
    }

    fn bytes(&mut self) -> Result<Bytes> {
        let n = self.len()?;
        let (head, rest) = self.0.split_at_checked(n).ok_or_else(Self::bad)?;
        self.0 = rest;
        Ok(Bytes::copy_from_slice(head))
    }
}

type Record = (Vec<(Bytes, Option<Bytes>)>, Vec<Bytes>);

fn decode(raw: &[u8]) -> Result<Record> {
    let mut r = Reader(raw);
    if r.byte()? != RECORD_FORMAT {
        return Err(Reader::bad());
    }
    let n = r.len()?;
    let mut writes = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        let k = r.bytes()?;
        let v = match r.byte()? {
            0 => Some(r.bytes()?),
            1 => None,
            _ => return Err(Reader::bad()),
        };
        writes.push((k, v));
    }
    let n = r.len()?;
    let mut locks = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        locks.push(r.bytes()?);
    }
    Ok((writes, locks))
}

impl Txn {
    /// `XA PREPARE`: makes the transaction's writes and exclusive locks
    /// durable under `xid` and hands the transaction to the unit, where
    /// [`Txn::xa_commit`] or [`Txn::xa_rollback`] finishes it from any
    /// session, also after a restart. Answer the client once the ticket is
    /// durable. Afterwards this handle is finished (`state().is_open` is
    /// false) and can be dropped; the locks stay with the unit.
    ///
    /// The transaction's shared record locks and gap locks are released
    /// (not at SERIALIZABLE); its exclusive record locks stay.
    ///
    /// # Errors
    /// 1440 if `xid` is already prepared (the transaction stays open and
    /// unchanged); the transaction is not active; `xid` is too long; or the
    /// log refused the record (the transaction is then rolled back, 1213).
    pub fn xa_prepare(&mut self, xid: &[u8]) -> Result<DurableTicket> {
        self.ensure_active()?;
        if xid.len() > MAX_XID_LEN {
            return Err(SprigError::internal("XID too long"));
        }
        let path = self.path.clone();
        let shared = path.shared();
        shared.recover_once(&*path);
        if shared.xa_known(xid) {
            return Err(xaer_dupid());
        }
        let writes = self.writes.writes();
        let mut locks = shared.locks.exclusive_keys(self.id);
        locks.retain(|k| writes.binary_search_by(|(w, _)| w.cmp(k)).is_err());
        let record = encode(&writes, &locks);
        let key = record_key(path.unit_id(), xid);
        let mut p = Payload::with_capacity(key.len() + record.len() + 16);
        p.put(Cf::Data, &key, &record);
        let durability = self.opts.durability;
        let prepared = PreparedTxn {
            id: self.id,
            writes,
            ticket: DurableTicket::Ready,
            durability,
            finishing: false,
        };
        let queued = shared.xa_register(xid, prepared, || path.propose(p.into_bytes(), durability));
        let ticket = match queued {
            Ok(t) => t,
            // Another session prepared the same XID in between.
            Err(e) if super::errors::errno(&e) == Some(ER_XAER_DUPID) => return Err(e),
            Err(e) => return Err(self.abort(AbortReason::LeaderChange, e)),
        };
        // MariaDB gives up the shared and gap locks at prepare, except at
        // SERIALIZABLE. MySQL 8.4 keeps every lock until the branch is
        // finished (both were probed).
        if self.opts.persona == Persona::MariaDb && self.opts.isolation != Isolation::Serializable {
            shared.locks.release_shared_and_gaps(self.id);
        }
        // The session's handle ends here; the locks stay with the unit.
        self.detach();
        let dep = std::mem::take(&mut self.dep);
        self.finish(Phase::Done);
        Ok(ticket.max(dep))
    }

    /// Whether a prepared transaction has `xid` (`XA START` refuses a
    /// taken XID with 1440).
    #[must_use]
    pub fn xa_exists(path: &Arc<dyn CommitPath>, xid: &[u8]) -> bool {
        let shared = path.shared();
        shared.recover_once(&**path);
        shared.xa_known(xid)
    }

    /// `XA COMMIT` of a prepared transaction, from any session. Answer the
    /// client once the ticket is durable.
    ///
    /// # Errors
    /// 1397 if no prepared transaction has `xid`; or the log refused the
    /// commit (the transaction stays prepared).
    pub fn xa_commit(path: &Arc<dyn CommitPath>, xid: &[u8]) -> Result<CommitTicket> {
        let shared = path.shared();
        shared.recover_once(&**path);
        let p = shared.xa_claim(xid).ok_or_else(xaer_nota)?;
        let key = record_key(path.unit_id(), xid);
        let done = shared.commit_writes(&**path, p.id, p.writes, p.durability, Some(&key));
        shared.xa_finished(xid, done.is_ok());
        let (cts, ticket) = done?;
        Ok(CommitTicket::new(Some(cts), cts, ticket.max(p.ticket)))
    }

    /// `XA ROLLBACK` of a prepared transaction, from any session. The locks
    /// are released at once; answer the client once the ticket is durable.
    ///
    /// # Errors
    /// 1397 if no prepared transaction has `xid`; or the log refused the
    /// entry (the transaction stays prepared).
    pub fn xa_rollback(path: &Arc<dyn CommitPath>, xid: &[u8]) -> Result<DurableTicket> {
        let shared = path.shared();
        shared.recover_once(&**path);
        let p = shared.xa_claim(xid).ok_or_else(xaer_nota)?;
        let key = record_key(path.unit_id(), xid);
        let mut payload = Payload::with_capacity(key.len() + 8);
        payload.delete(Cf::Data, &key);
        let queued = path.propose(payload.into_bytes(), p.durability);
        shared.xa_finished(xid, queued.is_ok());
        let ticket = queued?;
        shared.locks.release_all(p.id);
        shared.note_rollback();
        Ok(ticket.max(p.ticket))
    }

    /// `XA RECOVER`: the XIDs of the unit's prepared transactions, sorted.
    #[must_use]
    pub fn xa_recover(path: &Arc<dyn CommitPath>) -> Vec<Vec<u8>> {
        let shared = path.shared();
        shared.recover_once(&**path);
        shared.xa_list()
    }
}

impl TxnShared {
    /// Reads the prepare records back from the store after a restart: each
    /// becomes a prepared transaction again, holding its exclusive locks.
    pub(crate) fn load_prepared(&self, path: &dyn CommitPath) {
        let prefix = keys::unit_meta_key(path.unit_id(), FIELD);
        let mut found = Vec::new();
        {
            let mut it = path
                .store()
                .iter(Cf::Data, IterOpts::range(&KeyRange::prefix(&prefix)));
            it.seek_to_first();
            while it.valid() {
                if let Some(xid) = it.key().strip_prefix(&prefix[..]) {
                    found.push((xid.to_vec(), decode(it.value())));
                }
                it.next();
            }
        }
        for (xid, record) in found {
            let (writes, locks) = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(xid = ?xid, error = %e, "skipping a prepared transaction: its record can't be read");
                    continue;
                }
            };
            let id = self.fresh_txn_id();
            let weight = Arc::new(AtomicUsize::new(writes.len()));
            for k in writes.iter().map(|(k, _)| k).chain(&locks) {
                self.locks.restore_exclusive(id, &weight, k);
            }
            self.xa_restore(
                &xid,
                PreparedTxn {
                    id,
                    writes,
                    ticket: DurableTicket::Ready,
                    durability: None,
                    finishing: false,
                },
            );
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
    fn a_prepare_record_round_trips() {
        let writes = vec![
            (b("a"), Some(b("1"))),
            (b("b"), None),
            (b("c"), Some(Bytes::new())),
        ];
        let locks = vec![b("x"), b("y")];
        let raw = encode(&writes, &locks);
        assert_eq!(decode(&raw).unwrap(), (writes, locks));
        assert_eq!(decode(&encode(&[], &[])).unwrap(), (vec![], vec![]));
    }

    #[test]
    fn a_damaged_record_is_refused() {
        let raw = encode(&[(b("a"), Some(b("1")))], &[b("x")]);
        for cut in 0..raw.len() {
            assert!(decode(&raw[..cut]).is_err(), "cut at {cut}");
        }
        let mut wrong = raw.clone();
        wrong[0] = 9;
        assert!(decode(&wrong).is_err());
    }

    #[test]
    fn prepare_records_sort_under_one_prefix_per_unit() {
        let k = record_key(3, b"xid-1");
        assert!(k.starts_with(&keys::unit_meta_key(3, FIELD)));
        assert!(!k.starts_with(&keys::unit_meta_key(4, FIELD)));
        assert!(k.starts_with(&keys::META_TENANT.to_be_bytes()));
    }
}
