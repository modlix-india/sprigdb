//! What a log entry carries: the store writes a commit makes, in order.
//!
//! ```text
//! payload := 0x01 (format) | op*
//! op      := 0x01 put    | cf u8 | varint klen | key | varint vlen | value
//!          | 0x02 delete | cf u8 | varint klen | key
//!          | 0x03 range  | cf u8 | varint slen | start | varint elen | end
//!          | 0x04 note   | varint len | bytes          (ignored by apply)
//! ```
//!
//! The transaction layer builds one payload per commit (versioned keys at
//! the commit timestamp, already encoded); the apply step turns the entries
//! of a commit group into one store batch. Notes carry what later consumers
//! (the change log) need without the store seeing it.

use sprig_common::{Result, SprigError};

use crate::store::{Cf, StoreBatch};

pub(crate) const FORMAT: u8 = 1;
const PUT: u8 = 1;
const DELETE: u8 = 2;
const RANGE: u8 = 3;
pub(crate) const NOTE: u8 = 4;
/// A note kept in a blob file ([`super::blobs`]): only the log store's own
/// copy of an entry holds it, never what readers of the log get (they see
/// the note again), so the readers below refuse it as corrupt.
pub(crate) const BLOB: u8 = 5;

/// A payload under construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    buf: Vec<u8>,
}

impl Default for Payload {
    fn default() -> Self {
        Self::new()
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        #[allow(clippy::cast_possible_truncation)]
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    #[allow(clippy::cast_possible_truncation)]
    out.push(v as u8);
}

fn put_slice(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len());
    out.extend_from_slice(b);
}

impl Payload {
    /// An empty payload.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    /// An empty payload with room for `n` bytes.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        let mut buf = Vec::with_capacity(n + 1);
        buf.push(FORMAT);
        Self { buf }
    }

    /// Adds a put.
    pub fn put(&mut self, cf: Cf, key: &[u8], value: &[u8]) {
        self.buf.extend_from_slice(&[PUT, cf.code()]);
        put_slice(&mut self.buf, key);
        put_slice(&mut self.buf, value);
    }

    /// Adds a delete.
    pub fn delete(&mut self, cf: Cf, key: &[u8]) {
        self.buf.extend_from_slice(&[DELETE, cf.code()]);
        put_slice(&mut self.buf, key);
    }

    /// Adds a range delete of `[start, end)`.
    pub fn delete_range(&mut self, cf: Cf, start: &[u8], end: &[u8]) {
        self.buf.extend_from_slice(&[RANGE, cf.code()]);
        put_slice(&mut self.buf, start);
        put_slice(&mut self.buf, end);
    }

    /// Adds opaque bytes the apply step skips.
    pub fn note(&mut self, bytes: &[u8]) {
        self.buf.push(NOTE);
        put_slice(&mut self.buf, bytes);
    }

    /// Encoded length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether no operation was added.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.len() <= 1
    }

    /// The encoded bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bad() -> SprigError {
        SprigError::internal("log payload is corrupt")
    }

    fn byte(&mut self) -> Result<u8> {
        let (&b, rest) = self.0.split_first().ok_or_else(Self::bad)?;
        self.0 = rest;
        Ok(b)
    }

    fn varint(&mut self) -> Result<usize> {
        let mut v = 0usize;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= usize::from(b & 0x7F) << shift;
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err(Self::bad())
    }

    fn slice(&mut self) -> Result<&'a [u8]> {
        let n = self.varint()?;
        if self.0.len() < n {
            return Err(Self::bad());
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }

    fn cf(&mut self) -> Result<Cf> {
        Cf::from_code(self.byte()?).ok_or_else(Self::bad)
    }
}

/// Appends the store operations of one payload to `batch`.
///
/// # Errors
/// The payload is malformed.
pub fn decode_into(data: &[u8], batch: &mut StoreBatch) -> Result<()> {
    let mut r = Reader(data);
    if r.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    while !r.0.is_empty() {
        match r.byte()? {
            PUT => {
                let cf = r.cf()?;
                let k = r.slice()?;
                let v = r.slice()?;
                batch.put(cf, k, v);
            }
            DELETE => {
                let cf = r.cf()?;
                batch.delete(cf, r.slice()?);
            }
            RANGE => {
                let cf = r.cf()?;
                let s = r.slice()?;
                let e = r.slice()?;
                batch.delete_range(cf, s, e);
            }
            NOTE => {
                r.slice()?;
            }
            _ => return Err(Reader::bad()),
        }
    }
    Ok(())
}

/// One operation of a payload as stored: its code, all its bytes, and for a
/// note or a blob reference the bytes it carries.
pub(crate) type RawOp<'a> = (u8, &'a [u8], &'a [u8]);

/// Splits a payload into its operations, in order (blob references
/// included).
///
/// # Errors
/// The payload is malformed.
pub(crate) fn split(data: &[u8]) -> Result<Vec<RawOp<'_>>> {
    let mut r = Reader(data);
    if r.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    let mut out = Vec::new();
    while !r.0.is_empty() {
        let start = r.0;
        let code = r.byte()?;
        let body: &[u8] = match code {
            PUT | RANGE => {
                r.cf()?;
                r.slice()?;
                r.slice()?;
                &[]
            }
            DELETE => {
                r.cf()?;
                r.slice()?;
                &[]
            }
            NOTE | BLOB => r.slice()?,
            _ => return Err(Reader::bad()),
        };
        let used = start.len() - r.0.len();
        out.push((code, &start[..used], body));
    }
    Ok(out)
}

/// Appends a note or a blob reference carrying `bytes` to an encoded
/// payload.
pub(crate) fn push_op(out: &mut Vec<u8>, code: u8, bytes: &[u8]) {
    out.push(code);
    put_slice(out, bytes);
}

/// Whether one payload writes under `prefix`: a put or a delete of a key
/// that starts with it, or a range delete that overlaps the keys that do.
/// Notes (and blob references) write nothing to the store.
///
/// # Errors
/// The payload is malformed.
pub fn writes_under(data: &[u8], prefix: &[u8]) -> Result<bool> {
    let mut r = Reader(data);
    if r.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    while !r.0.is_empty() {
        match r.byte()? {
            PUT => {
                r.cf()?;
                let k = r.slice()?;
                r.slice()?;
                if k.starts_with(prefix) {
                    return Ok(true);
                }
            }
            DELETE => {
                r.cf()?;
                if r.slice()?.starts_with(prefix) {
                    return Ok(true);
                }
            }
            RANGE => {
                r.cf()?;
                let s = r.slice()?;
                let e = r.slice()?;
                if s.starts_with(prefix) || (s < prefix && e > prefix) {
                    return Ok(true);
                }
            }
            NOTE | BLOB => {
                r.slice()?;
            }
            _ => return Err(Reader::bad()),
        }
    }
    Ok(false)
}

/// The notes of one payload, in order (what [`Payload::note`] added).
///
/// # Errors
/// The payload is malformed.
pub fn notes(data: &[u8]) -> Result<Vec<&[u8]>> {
    let mut r = Reader(data);
    if r.byte()? != FORMAT {
        return Err(Reader::bad());
    }
    let mut out = Vec::new();
    while !r.0.is_empty() {
        match r.byte()? {
            PUT | RANGE => {
                r.cf()?;
                r.slice()?;
                r.slice()?;
            }
            DELETE => {
                r.cf()?;
                r.slice()?;
            }
            NOTE => out.push(r.slice()?),
            _ => return Err(Reader::bad()),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::BatchOp;

    #[test]
    fn round_trip() {
        let mut p = Payload::new();
        p.put(Cf::Data, b"k1", &vec![7u8; 300]);
        p.note(b"txn 9");
        p.delete(Cf::Local, b"k2");
        p.delete_range(Cf::Data, b"a", b"b");
        let mut b = StoreBatch::new();
        decode_into(&p.into_bytes(), &mut b).unwrap();
        assert_eq!(
            b.ops(),
            &[
                BatchOp::Put {
                    cf: Cf::Data,
                    key: b"k1".to_vec(),
                    value: vec![7u8; 300]
                },
                BatchOp::Delete {
                    cf: Cf::Local,
                    key: b"k2".to_vec()
                },
                BatchOp::DeleteRange {
                    cf: Cf::Data,
                    start: b"a".to_vec(),
                    end: b"b".to_vec()
                },
            ]
        );
        assert!(decode_into(&[1, 9], &mut StoreBatch::new()).is_err());
        let mut p = Payload::new();
        p.put(Cf::Data, b"k", b"v");
        p.note(b"one");
        p.delete_range(Cf::Data, b"a", b"b");
        p.note(b"two");
        let bytes = p.into_bytes();
        assert_eq!(notes(&bytes).unwrap(), vec![&b"one"[..], &b"two"[..]]);
        assert!(decode_into(&[1, 1, 0, 5, b'x'], &mut StoreBatch::new()).is_err());
    }

    #[test]
    fn writes_under_a_prefix() {
        let one = |f: &dyn Fn(&mut Payload)| {
            let mut p = Payload::new();
            p.note(b"n");
            f(&mut p);
            p.into_bytes()
        };
        let put = one(&|p| p.put(Cf::Data, b"pq1", b"v"));
        assert!(writes_under(&put, b"pq").unwrap());
        assert!(!writes_under(&put, b"pr").unwrap());
        let del = one(&|p| p.delete(Cf::Data, b"pq1"));
        assert!(writes_under(&del, b"pq").unwrap());
        // A range over the prefix, one inside it, and one beside it.
        let over = one(&|p| p.delete_range(Cf::Data, b"a", b"z"));
        assert!(writes_under(&over, b"pq").unwrap());
        let inside = one(&|p| p.delete_range(Cf::Data, b"pq1", b"pq9"));
        assert!(writes_under(&inside, b"pq").unwrap());
        let beside = one(&|p| p.delete_range(Cf::Data, b"a", b"pp"));
        assert!(!writes_under(&beside, b"pq").unwrap());
        let notes_only = one(&|_| {});
        assert!(!writes_under(&notes_only, b"").unwrap());
        assert!(writes_under(&[1, 9], b"pq").is_err());
    }
}
