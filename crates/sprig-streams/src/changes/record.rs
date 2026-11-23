//! The change log's records, change hints and resume tokens (version 2).
//!
//! ```text
//! record := len u32 | crc32c u32 (of body) | body
//! body   := index u64 | seq u32 | cts u64 | flags u8
//!         | varint klen | logical key
//!         | [varint vlen | value]                (flags & PUT)
//!         | [op u8 | varint dlen | description]  (flags & HINT)
//! flags  := 1 PUT (else a delete) | 2 LAST (the entry's last record) | 4 HINT
//! ```
//!
//! `index` is the log entry's index in its unit's log and `seq` the
//! record's place in the entry, so `(index, seq)` orders a unit's change
//! log; `cts` is the commit timestamp of the entry's versions.
//!
//! A **hint** is a payload note a writer adds beside a versioned write
//! ([`encode_hint`]): what the write was in its model's terms (an insert,
//! an update, a replacement or a delete) and a description (for a
//! MongoDB update, its `updateDescription` as BSON). The change log
//! attaches it to the record of the same logical key in the same entry
//! (the codec is `sprig_engine::observe::hint`, so writers need no
//! dependency on this crate).

use sprig_engine::keys::KeyPrefix;

/// Bytes before a record's body.
pub const HEAD: usize = 8;
const PUT: u8 = 1;
const LAST: u8 = 2;
const HINT: u8 = 4;

pub use sprig_engine::observe::hint::HintOp;

fn put_varint(out: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        #[allow(clippy::cast_possible_truncation)]
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    #[allow(clippy::cast_possible_truncation)]
    out.push(v as u8);
}

struct Rd<'a>(&'a [u8]);

impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Some(h)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn varint(&mut self) -> Option<usize> {
        let mut v = 0usize;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            v |= usize::from(b & 0x7F) << shift;
            if b < 0x80 {
                return Some(v);
            }
        }
        None
    }

    fn slice(&mut self) -> Option<&'a [u8]> {
        let n = self.varint()?;
        self.take(n)
    }
}

/// A hint note for the write of `key` (a logical key, without version).
#[must_use]
pub fn encode_hint(key: &[u8], op: HintOp, desc: &[u8]) -> Vec<u8> {
    sprig_engine::observe::hint::encode(key, op, desc)
}

/// The parts of a hint note: key, operation, description.
#[must_use]
pub fn decode_hint(note: &[u8]) -> Option<(&[u8], HintOp, &[u8])> {
    sprig_engine::observe::hint::decode(note)
}

/// A place in a unit's change log: after the record `seq` of entry
/// `index`. `seq == u32::MAX` is after the whole entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Pos {
    /// The log entry.
    pub index: u64,
    /// The record in it.
    pub seq: u32,
}

impl Pos {
    /// After every record of entry `index`.
    #[must_use]
    pub fn after_entry(index: u64) -> Self {
        Self {
            index,
            seq: u32::MAX,
        }
    }
}

/// One committed change of one logical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// The log entry's index in its unit's log.
    pub index: u64,
    /// The record's place in the entry.
    pub seq: u32,
    /// The commit timestamp.
    pub cts: u64,
    /// The entry's last record.
    pub last: bool,
    /// The logical key (no version).
    pub key: Vec<u8>,
    /// The value written; `None` for a delete.
    pub value: Option<Vec<u8>>,
    /// The writer's hint, when it gave one.
    pub hint: Option<(HintOp, Vec<u8>)>,
}

impl Change {
    /// Its place.
    #[must_use]
    pub fn pos(&self) -> Pos {
        Pos {
            index: self.index,
            seq: self.seq,
        }
    }

    /// The key's prefix and user key.
    #[must_use]
    pub fn prefix(&self) -> Option<(KeyPrefix, &[u8])> {
        KeyPrefix::decode(&self.key)
    }

    /// Encoded size.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        HEAD + 21
            + 10
            + self.key.len()
            + self.value.as_ref().map_or(0, |v| v.len() + 10)
            + self.hint.as_ref().map_or(0, |(_, d)| d.len() + 11)
    }

    /// Appends the record.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        out.extend_from_slice(&[0; HEAD]);
        out.extend_from_slice(&self.index.to_be_bytes());
        out.extend_from_slice(&self.seq.to_be_bytes());
        out.extend_from_slice(&self.cts.to_be_bytes());
        let mut flags = 0;
        if self.value.is_some() {
            flags |= PUT;
        }
        if self.last {
            flags |= LAST;
        }
        if self.hint.is_some() {
            flags |= HINT;
        }
        out.push(flags);
        put_varint(out, self.key.len());
        out.extend_from_slice(&self.key);
        if let Some(v) = &self.value {
            put_varint(out, v.len());
            out.extend_from_slice(v);
        }
        if let Some((op, d)) = &self.hint {
            out.push(op.code());
            put_varint(out, d.len());
            out.extend_from_slice(d);
        }
        let body = &out[start + HEAD..];
        let len = u32::try_from(body.len()).unwrap_or(u32::MAX);
        let crc = sprig_proto_kafka::crc32c::crc32c(body);
        out[start..start + 4].copy_from_slice(&len.to_be_bytes());
        out[start + 4..start + 8].copy_from_slice(&crc.to_be_bytes());
    }

    /// Decodes a record's body.
    #[must_use]
    pub fn decode_body(body: &[u8]) -> Option<Change> {
        let mut r = Rd(body);
        let index = r.u64()?;
        let seq = r.u32()?;
        let cts = r.u64()?;
        let flags = r.u8()?;
        let key = r.slice()?.to_vec();
        let value = if flags & PUT != 0 {
            Some(r.slice()?.to_vec())
        } else {
            None
        };
        let hint = if flags & HINT != 0 {
            let op = HintOp::of(r.u8()?)?;
            Some((op, r.slice()?.to_vec()))
        } else {
            None
        };
        Some(Change {
            index,
            seq,
            cts,
            last: flags & LAST != 0,
            key,
            value,
            hint,
        })
    }
}

/// A record head: body length and checksum. `None` for a torn head.
#[must_use]
pub fn parse_head(h: &[u8]) -> Option<(usize, u32)> {
    if h.len() < HEAD {
        return None;
    }
    let len = u32::from_be_bytes(h[0..4].try_into().ok()?) as usize;
    let crc = u32::from_be_bytes(h[4..8].try_into().ok()?);
    Some((len, crc))
}

/// The body's index, seq and cts without decoding the rest.
#[must_use]
pub fn peek(body: &[u8]) -> Option<(Pos, u64)> {
    let mut r = Rd(body);
    let index = r.u64()?;
    let seq = r.u32()?;
    let cts = r.u64()?;
    Some((Pos { index, seq }, cts))
}

/// Whether a body passes its checksum.
#[must_use]
pub fn crc_ok(body: &[u8], crc: u32) -> bool {
    sprig_proto_kafka::crc32c::crc32c(body) == crc
}

/// A resume token, version 2:
/// `2 | flags u8 | tenant u64 | cts u64 | txn u128 | seq u32`, in hex.
/// `txn` is the log entry's index in the tenant's unit; a resume is
/// everything after `(txn, seq)`. Flag 1: a progress token (no event at
/// this place; `seq` is `u32::MAX`, after the whole entry).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    /// The tenant.
    pub tenant: u64,
    /// The commit timestamp of the place.
    pub cts: u64,
    /// The log entry.
    pub txn: u128,
    /// The record in it.
    pub seq: u32,
    /// Flags (1: progress).
    pub flags: u8,
}

/// The version byte of [`Token`].
pub const TOKEN_V2: u8 = 2;
/// [`Token::flags`]: no event at this place.
pub const TOKEN_PROGRESS: u8 = 1;

impl Token {
    /// The token of a place.
    #[must_use]
    pub fn at(tenant: u64, cts: u64, pos: Pos, progress: bool) -> Self {
        Self {
            tenant,
            cts,
            txn: u128::from(pos.index),
            seq: pos.seq,
            flags: if progress { TOKEN_PROGRESS } else { 0 },
        }
    }

    /// The place it names.
    #[must_use]
    pub fn pos(&self) -> Pos {
        Pos {
            index: u64::try_from(self.txn).unwrap_or(u64::MAX),
            seq: self.seq,
        }
    }

    /// The 38 bytes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(38);
        b.push(TOKEN_V2);
        b.push(self.flags);
        b.extend_from_slice(&self.tenant.to_be_bytes());
        b.extend_from_slice(&self.cts.to_be_bytes());
        b.extend_from_slice(&self.txn.to_be_bytes());
        b.extend_from_slice(&self.seq.to_be_bytes());
        b
    }

    /// Upper-case hex of [`Token::to_bytes`] (MongoDB's tokens are hex).
    #[must_use]
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(76);
        for b in self.to_bytes() {
            let _ = write!(s, "{b:02X}");
        }
        s
    }

    /// Parses [`Token::to_bytes`]. `None`: not a version-2 token.
    #[must_use]
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() != 38 || b[0] != TOKEN_V2 {
            return None;
        }
        let mut r = Rd(&b[1..]);
        let flags = r.u8()?;
        let tenant = r.u64()?;
        let cts = r.u64()?;
        let txn = u128::from_be_bytes(r.take(16)?.try_into().ok()?);
        let seq = r.u32()?;
        Some(Self {
            tenant,
            cts,
            txn,
            seq,
            flags,
        })
    }

    /// Parses [`Token::to_hex`].
    #[must_use]
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 76 || !s.is_ascii() {
            return None;
        }
        let b: Option<Vec<u8>> = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
            .collect();
        Self::from_bytes(&b?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip() {
        let c = Change {
            index: 7,
            seq: 2,
            cts: 99,
            last: true,
            key: b"0123456789012345678key".to_vec(),
            value: Some(vec![1, 2, 3]),
            hint: Some((HintOp::Replace, b"d".to_vec())),
        };
        let mut b = Vec::new();
        c.encode_into(&mut b);
        assert!(b.len() <= c.encoded_len());
        let (len, crc) = parse_head(&b).unwrap();
        assert_eq!(len, b.len() - HEAD);
        assert!(crc_ok(&b[HEAD..], crc));
        assert_eq!(Change::decode_body(&b[HEAD..]).unwrap(), c);
        assert_eq!(peek(&b[HEAD..]).unwrap(), (c.pos(), 99));
        let d = Change {
            value: None,
            hint: None,
            last: false,
            ..c
        };
        let mut b = Vec::new();
        d.encode_into(&mut b);
        assert_eq!(Change::decode_body(&b[HEAD..]).unwrap(), d);
    }

    #[test]
    fn hints_and_tokens() {
        let n = encode_hint(b"key", HintOp::Update, b"desc");
        assert_eq!(
            decode_hint(&n),
            Some((&b"key"[..], HintOp::Update, &b"desc"[..]))
        );
        assert_eq!(decode_hint(b"other"), None);
        let t = Token::at(5, 77, Pos { index: 9, seq: 3 }, false);
        assert_eq!(Token::from_hex(&t.to_hex()), Some(t));
        assert_eq!(t.pos(), Pos { index: 9, seq: 3 });
        assert_eq!(Token::from_hex("82AB"), None);
        assert!(Pos::after_entry(4) > Pos { index: 4, seq: 9 });
        assert!(Pos::after_entry(4) < Pos { index: 5, seq: 0 });
    }
}
