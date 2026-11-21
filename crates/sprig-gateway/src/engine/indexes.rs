//! A table's index record: which key index number each secondary index
//! has, and which indexes have entries for every row.
//!
//! The definition (`SHOW CREATE TABLE` text) lists indexes in an order the
//! server chooses, and that order shifts when an index is added or dropped.
//! So an index's number in keys is given once, when the engine first sees
//! the index, and kept in this record (key kind `M` of the table's object):
//! an index is matched by its name, columns (with a part's prefix length
//! and direction) and uniqueness. Numbers are never reused. An index first seen on a table that already has rows is
//! built (its entries written for every row) before the record names it.
//!
//! ```text
//! u16 next number | u16 count | per index:
//!   u16 number | u8 unique | u16 name length, name | u16 column count,
//!   per column: u16 length, name                       (names lower case)
//! ```

use bytes::{BufMut, Bytes, BytesMut};

use super::table::TableDef;

/// One index as the record keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    /// Its number in keys.
    pub id: u16,
    /// What identifies it: name, columns and uniqueness.
    pub sig: Sig,
}

/// An index's identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Sig {
    /// The name, lower case.
    pub name: String,
    /// The columns' names, lower case, in order; a prefix length and a
    /// descending direction are part of a column's entry.
    pub cols: Vec<String>,
    /// `UNIQUE`.
    pub unique: bool,
}

/// The identities of `def`'s secondary indexes, in definition order.
pub(crate) fn sigs(def: &TableDef) -> Vec<Sig> {
    def.indexes
        .iter()
        .map(|x| Sig {
            name: x.name.to_lowercase(),
            cols: x
                .cols
                .iter()
                .zip(&x.parts)
                .map(|(&c, kc)| {
                    let mut n = def.columns[c].name.to_lowercase();
                    // A prefix or descending part makes another index than
                    // the plain one: marked after a NUL, which no column
                    // name holds (a plain part's identity is its name, as
                    // the first records wrote it).
                    if let Some(p) = kc.prefix {
                        n.push('\0');
                        n.push_str(&p.to_string());
                    }
                    if kc.desc {
                        n.push_str("\0d");
                    }
                    n
                })
                .collect(),
            unique: x.unique,
        })
        .collect()
}

/// The record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Record {
    /// The next number to give (numbers start at 1; 0 is the rows).
    pub next: u16,
    /// The indexes that have entries for every row.
    pub entries: Vec<Entry>,
}

impl Record {
    /// Encodes the record.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(64);
        b.put_u16_le(self.next);
        b.put_u16_le(u16::try_from(self.entries.len()).unwrap_or(u16::MAX));
        let put_str = |b: &mut BytesMut, s: &str| {
            b.put_u16_le(u16::try_from(s.len()).unwrap_or(u16::MAX));
            b.put_slice(s.as_bytes());
        };
        for e in &self.entries {
            b.put_u16_le(e.id);
            b.put_u8(u8::from(e.sig.unique));
            put_str(&mut b, &e.sig.name);
            b.put_u16_le(u16::try_from(e.sig.cols.len()).unwrap_or(u16::MAX));
            for c in &e.sig.cols {
                put_str(&mut b, c);
            }
        }
        b.freeze()
    }

    /// Decodes a record; `None` when it doesn't decode.
    pub(crate) fn decode(raw: &[u8]) -> Option<Self> {
        let mut r = Reader(raw);
        let next = r.u16()?;
        let n = r.u16()?;
        let mut entries = Vec::with_capacity(usize::from(n));
        for _ in 0..n {
            let id = r.u16()?;
            let unique = r.u8()? != 0;
            let name = r.str()?;
            let nc = r.u16()?;
            let mut cols = Vec::with_capacity(usize::from(nc));
            for _ in 0..nc {
                cols.push(r.str()?);
            }
            entries.push(Entry {
                id,
                sig: Sig { name, cols, unique },
            });
        }
        r.0.is_empty().then_some(Self { next, entries })
    }

    /// The numbers for `wanted` (definition order): kept for indexes the
    /// record has, new for the others. Returns the numbers, the positions
    /// that are new (they need building when the table has rows), and the
    /// record that names exactly `wanted`.
    pub(crate) fn assign(&self, wanted: &[Sig]) -> (Vec<u16>, Vec<usize>, Record) {
        let mut next = self.next.max(1);
        let mut ids = Vec::with_capacity(wanted.len());
        let mut fresh = Vec::new();
        let mut entries = Vec::with_capacity(wanted.len());
        for (pos, sig) in wanted.iter().enumerate() {
            let id = if let Some(e) = self.entries.iter().find(|e| e.sig == *sig) {
                e.id
            } else {
                let id = next;
                next = next.saturating_add(1);
                fresh.push(pos);
                id
            };
            ids.push(id);
            entries.push(Entry {
                id,
                sig: sig.clone(),
            });
        }
        (ids, fresh, Record { next, entries })
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let (h, t) = (self.0.get(..n)?, self.0.get(n..)?);
        self.0 = t;
        Some(h)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes([b[0], b[1]]))
    }
    fn str(&mut self) -> Option<String> {
        let n = usize::from(self.u16()?);
        self.take(n)
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(name: &str, unique: bool) -> Sig {
        Sig {
            name: name.into(),
            cols: vec![name.into()],
            unique,
        }
    }

    #[test]
    fn numbers_survive_reordering_and_drops() {
        let r0 = Record::default();
        let (ids, fresh, r1) = r0.assign(&[sig("k", false), sig("u", true)]);
        assert_eq!((ids, fresh), (vec![1, 2], vec![0, 1]));
        assert_eq!(Record::decode(&r1.encode()), Some(r1.clone()));
        // A new unique index listed first; `u` dropped: `k` keeps 1, and 2
        // is never given again.
        let (ids, fresh, r2) = r1.assign(&[sig("v", true), sig("k", false)]);
        assert_eq!((ids, fresh), (vec![3, 1], vec![0]));
        assert_eq!(r2.next, 4);
        // The same name on other columns is another index.
        let other = Sig {
            cols: vec!["z".into()],
            ..sig("k", false)
        };
        let (ids, fresh, _) = r2.assign(&[other]);
        assert_eq!((ids, fresh), (vec![4], vec![0]));
        assert_eq!(Record::decode(b"\x01"), None);
    }
}
