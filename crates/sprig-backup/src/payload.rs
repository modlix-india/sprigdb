//! Reading and rewriting commit-log payloads (`sprig_engine::log::payload`'s
//! format) and the stream form of unit snapshots, for restoring a tenant
//! under another tenant id.
//!
//! A tenant's keys start with its 8-byte id; its unit's bookkeeping keys
//! are the reserved tenant's metadata prefix followed by the unit id.
//! Rewriting swaps both and leaves every other byte alone.

use sprig_engine::Cf;
use sprig_engine::keys::{self, PREFIX_LEN, VERSION_LEN};

/// Swaps one tenant id for another in keys.
#[derive(Debug, Clone)]
pub struct Rekey {
    from: [u8; 8],
    to: [u8; 8],
    meta_from: Vec<u8>,
    meta_to: Vec<u8>,
    /// Range ends just past each prefix: (old, new).
    ends: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Rekey {
    /// From tenant `from` to tenant `to` (`None` when they're equal).
    #[must_use]
    pub fn new(from: u64, to: u64) -> Option<Rekey> {
        (from != to).then(|| {
            let (meta_from, meta_to) =
                (keys::unit_meta_key(from, b""), keys::unit_meta_key(to, b""));
            let end = |r: keys::KeyRange| r.end.unwrap_or_default();
            Rekey {
                from: from.to_be_bytes(),
                to: to.to_be_bytes(),
                ends: vec![
                    (end(keys::tenant_range(from)), end(keys::tenant_range(to))),
                    (
                        end(keys::KeyRange::prefix(&meta_from)),
                        end(keys::KeyRange::prefix(&meta_to)),
                    ),
                ],
                meta_from,
                meta_to,
            }
        })
    }

    /// The end of a range: the old tenant's range end (just past its
    /// prefix) becomes the new one's; other keys as [`Rekey::key`].
    #[must_use]
    pub fn range_end(&self, key: &[u8]) -> Vec<u8> {
        for (old, new) in &self.ends {
            if !old.is_empty() && key == old.as_slice() {
                return new.clone();
            }
        }
        self.key(key)
    }

    /// `key` under the new tenant (unchanged when it isn't the old one's).
    #[must_use]
    pub fn key(&self, key: &[u8]) -> Vec<u8> {
        if key.starts_with(&self.meta_from) {
            let mut k = self.meta_to.clone();
            k.extend_from_slice(&key[self.meta_from.len()..]);
            k
        } else if key.starts_with(&self.from) {
            let mut k = self.to.to_vec();
            k.extend_from_slice(&key[8..]);
            k
        } else {
            key.to_vec()
        }
    }
}

fn varint(b: &[u8], at: &mut usize) -> Option<usize> {
    let mut v = 0usize;
    for shift in (0..64).step_by(7) {
        let x = *b.get(*at)?;
        *at += 1;
        v |= usize::from(x & 0x7F) << shift;
        if x < 0x80 {
            return Some(v);
        }
    }
    None
}

fn slice<'a>(b: &'a [u8], at: &mut usize) -> Option<&'a [u8]> {
    let n = varint(b, at)?;
    let s = b.get(*at..*at + n)?;
    *at += n;
    Some(s)
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

fn put_slice(out: &mut Vec<u8>, s: &[u8]) {
    put_varint(out, s.len());
    out.extend_from_slice(s);
}

/// One payload operation, borrowed.
enum Op<'a> {
    Put(u8, &'a [u8], &'a [u8]),
    Delete(u8, &'a [u8]),
    Range(u8, &'a [u8], &'a [u8]),
    Note(&'a [u8]),
}

/// Walks a payload's operations; false when malformed.
fn walk<'a>(data: &'a [u8], mut f: impl FnMut(Op<'a>)) -> bool {
    if data.first() != Some(&1) {
        return false;
    }
    let mut at = 1usize;
    while at < data.len() {
        let code = data[at];
        at += 1;
        let op = (|| -> Option<Op<'a>> {
            Some(match code {
                1 => {
                    let cf = *data.get(at)?;
                    at += 1;
                    let k = slice(data, &mut at)?;
                    Op::Put(cf, k, slice(data, &mut at)?)
                }
                2 => {
                    let cf = *data.get(at)?;
                    at += 1;
                    Op::Delete(cf, slice(data, &mut at)?)
                }
                3 => {
                    let cf = *data.get(at)?;
                    at += 1;
                    let s = slice(data, &mut at)?;
                    Op::Range(cf, s, slice(data, &mut at)?)
                }
                4 => Op::Note(slice(data, &mut at)?),
                _ => return None,
            })
        })();
        match op {
            Some(op) => f(op),
            None => return false,
        }
    }
    true
}

/// The commit timestamp of an entry: the highest version timestamp among
/// its versioned writes, 0 when it has none (unversioned writes only, or
/// an empty entry).
#[must_use]
pub fn entry_ts(data: &[u8]) -> u64 {
    let data_cf = Cf::Data.code();
    let mut ts = 0u64;
    walk(data, |op| {
        if let Op::Put(cf, k, _) | Op::Delete(cf, k) = op
            && cf == data_cf
            && k.len() >= PREFIX_LEN + VERSION_LEN
            && !keys::is_unversioned(k)
            && let Some((_, t)) = keys::split_version(k)
        {
            ts = ts.max(t.0);
        }
    });
    ts
}

/// The payload with every key moved to the new tenant. `None` when the
/// payload is malformed.
#[must_use]
pub fn rekey_payload(data: &[u8], r: &Rekey) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() + 16);
    out.push(1);
    let ok = walk(data, |op| match op {
        Op::Put(cf, k, v) => {
            out.push(1);
            out.push(cf);
            put_slice(&mut out, &r.key(k));
            put_slice(&mut out, v);
        }
        Op::Delete(cf, k) => {
            out.push(2);
            out.push(cf);
            put_slice(&mut out, &r.key(k));
        }
        Op::Range(cf, s, e) => {
            out.push(3);
            out.push(cf);
            put_slice(&mut out, &r.key(s));
            put_slice(&mut out, &r.range_end(e));
        }
        Op::Note(n) => {
            out.push(4);
            put_slice(&mut out, n);
        }
    });
    ok.then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_engine::log::payload::Payload;

    #[test]
    fn rekey_and_ts() {
        let mut k = keys::KeyPrefix {
            tenant: 7,
            shard: 0,
            object: 3,
            kind: keys::kind::PRIMARY,
            index: 0,
        }
        .to_vec();
        k.extend_from_slice(b"row");
        let mut versioned = k.clone();
        keys::push_version(&mut versioned, keys::Ts(12345));
        let meta = keys::unit_meta_key(7, b"xa");
        let mut p = Payload::new();
        p.put(Cf::Data, &versioned, b"v");
        p.put(Cf::Data, &meta, b"m");
        p.delete(Cf::Data, &k);
        let data = p.into_bytes();
        assert_eq!(entry_ts(&data), 12345);
        let r = Rekey::new(7, 9).unwrap();
        let out = rekey_payload(&data, &r).unwrap();
        let mut keys_seen = Vec::new();
        assert!(walk(&out, |op| {
            if let Op::Put(_, k, _) | Op::Delete(_, k) = op {
                keys_seen.push(k.to_vec());
            }
        }));
        assert_eq!(keys_seen.len(), 3);
        assert!(keys_seen[0].starts_with(&9u64.to_be_bytes()));
        assert_eq!(keys_seen[1], keys::unit_meta_key(9, b"xa"));
        assert_eq!(&keys_seen[2][8..], &k[8..]);
        assert_eq!(entry_ts(&out), 12345);
        assert!(Rekey::new(3, 3).is_none());
        // A range over the whole tenant keeps covering the whole tenant.
        let t7 = keys::tenant_range(7);
        let t9 = keys::tenant_range(9);
        assert_eq!(r.key(&t7.start), t9.start);
        assert_eq!(r.range_end(t7.end.as_deref().unwrap()), t9.end.unwrap());
    }
}
