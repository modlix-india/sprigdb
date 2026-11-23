//! Where a tenant's MongoDB collections live in the engine's keyspace.
//!
//! ```text
//! catalog object (one per tenant): object CATALOG
//!   prefix(M, 0) | str(db) | str(coll)          -> collection record (BSON)
//!   prefix(M, 1) | str(lsid) | txnNumber u64    -> retryable-write record (BSON)
//!   prefix(C, 0)                                -> next collection number u32
//! collection object: object CATALOG + n (n >= 1, never reused)
//!   prefix(P, 0) | keyenc(_id)                  -> header u8 | BSON as stored
//!   prefix(S, i) | k_1 .. k_m | keyenc(_id)     -> keyenc(_id)
//!   prefix(U, i) | k_1 .. k_m                   -> keyenc(_id)
//!   prefix(M, 2) | i u16                        -> 1 (index i has seen an array)
//! prefix(kind, index) = tenant u64 | shard u32 (0) | object u32 | kind u8 | index u16
//! ```
//!
//! `k_j` is the order-preserving key of the j-th indexed value
//! ([`crate::keyenc`]), its bytes complemented for a descending field.
//! Every key encoding is prefix-free, so the concatenation sorts by
//! component and the `_id` that ends a secondary entry never bleeds into
//! the indexed values.

use sprig_engine::keys::{KeyPrefix, kind};

/// The catalog object of a tenant's collections; collection `n` is
/// object `CATALOG + n`.
pub const CATALOG: u32 = 0x4D00_0000;
/// The largest collection number.
pub const MAX_COLLECTION: u32 = 0x00FF_FFFF;

/// Collection records.
const CAT_COLLECTIONS: u16 = 0;
/// Retryable-write records.
const CAT_RETRY: u16 = 1;
/// Multikey facts under a collection's metadata kind.
const MULTIKEY: u16 = 2;
/// Database drop markers: a delete written when `dropDatabase` dropped a
/// database's collections, so the change log shows the database's drop.
const CAT_DB_DROPS: u16 = 3;

/// The header byte of a stored document: format 1, no field directory.
pub const DOC_FORMAT_1: u8 = 1;

fn prefix(tenant: u64, object: u32, kind: u8, index: u16) -> Vec<u8> {
    let mut k = Vec::with_capacity(64);
    KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind,
        index,
    }
    .encode_into(&mut k);
    k
}

/// Appends a memcomparable string: `00` escaped as `00 FF`, ended by
/// `00 00`.
pub fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    for b in s {
        out.push(*b);
        if *b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// The key of collection `db.coll`'s record.
#[must_use]
pub fn coll_record(tenant: u64, db: &str, coll: &str) -> Vec<u8> {
    let mut k = prefix(tenant, CATALOG, kind::META, CAT_COLLECTIONS);
    put_str(&mut k, db.as_bytes());
    put_str(&mut k, coll.as_bytes());
    k
}

/// The prefix of every collection record of database `db` (`None`: of
/// every database).
#[must_use]
pub fn coll_records(tenant: u64, db: Option<&str>) -> Vec<u8> {
    let mut k = prefix(tenant, CATALOG, kind::META, CAT_COLLECTIONS);
    if let Some(db) = db {
        put_str(&mut k, db.as_bytes());
    }
    k
}

/// The tenant's collection-number counter.
#[must_use]
pub fn coll_counter(tenant: u64) -> Vec<u8> {
    prefix(tenant, CATALOG, kind::COUNTERS, 0)
}

/// The registry of tenants holding collections on this unit (the TTL
/// sweeper's list after a restart): under tenant 0's catalog object.
const CAT_TENANTS: u16 = 9;

/// The registry entry of `tenant`.
#[must_use]
pub fn tenant_entry(tenant: u64) -> Vec<u8> {
    let mut k = prefix(0, CATALOG, kind::META, CAT_TENANTS);
    k.extend_from_slice(&tenant.to_be_bytes());
    k
}

/// The prefix of every registry entry.
#[must_use]
pub fn tenant_entries() -> Vec<u8> {
    prefix(0, CATALOG, kind::META, CAT_TENANTS)
}

/// The key of a retryable write's record.
#[must_use]
pub fn retry_record(tenant: u64, lsid: &[u8], txn_number: i64) -> Vec<u8> {
    let mut k = prefix(tenant, CATALOG, kind::META, CAT_RETRY);
    put_str(&mut k, lsid);
    k.extend_from_slice(&(txn_number.cast_unsigned() ^ (1 << 63)).to_be_bytes());
    k
}

/// The prefix of every retryable-write record of the tenant.
#[must_use]
pub fn retry_records(tenant: u64) -> Vec<u8> {
    prefix(tenant, CATALOG, kind::META, CAT_RETRY)
}

/// The marker key of database `db`'s drop (only ever deleted).
#[must_use]
pub fn db_drop_marker(tenant: u64, db: &str) -> Vec<u8> {
    let mut k = prefix(tenant, CATALOG, kind::META, CAT_DB_DROPS);
    put_str(&mut k, db.as_bytes());
    k
}

/// Reads one [`put_str`] string: the string and what follows.
#[must_use]
pub fn take_str(b: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == 0 {
            match b.get(i + 1)? {
                0 => return Some((out, &b[i + 2..])),
                0xFF => {
                    out.push(0);
                    i += 2;
                    continue;
                }
                _ => return None,
            }
        }
        out.push(b[i]);
        i += 1;
    }
    None
}

/// What a key of the catalog object's metadata is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogKey {
    /// Collection `coll` of `db`'s record.
    Coll {
        /// The database.
        db: String,
        /// The collection.
        coll: String,
    },
    /// Database `db`'s drop marker.
    DbDrop(String),
    /// Something else.
    Other,
}

/// What the catalog metadata key `index | user_key` names.
#[must_use]
pub fn catalog_key(index: u16, user_key: &[u8]) -> CatalogKey {
    let s = |b: Vec<u8>| String::from_utf8(b).ok();
    match index {
        CAT_COLLECTIONS => {
            let Some((db, rest)) = take_str(user_key) else {
                return CatalogKey::Other;
            };
            let Some((coll, _)) = take_str(rest) else {
                return CatalogKey::Other;
            };
            match (s(db), s(coll)) {
                (Some(db), Some(coll)) => CatalogKey::Coll { db, coll },
                _ => CatalogKey::Other,
            }
        }
        CAT_DB_DROPS => take_str(user_key)
            .and_then(|(db, _)| s(db))
            .map_or(CatalogKey::Other, CatalogKey::DbDrop),
        _ => CatalogKey::Other,
    }
}

/// The keyspace of one collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Space {
    /// The tenant.
    pub tenant: u64,
    /// The collection's object id.
    pub object: u32,
}

impl Space {
    /// Collection number `n` of `tenant`.
    #[must_use]
    pub const fn new(tenant: u64, n: u32) -> Self {
        Self {
            tenant,
            object: CATALOG + n,
        }
    }

    /// The prefix of the documents.
    #[must_use]
    pub fn docs(&self) -> Vec<u8> {
        prefix(self.tenant, self.object, kind::PRIMARY, 0)
    }

    /// The key of the document whose `_id` key is `id`.
    #[must_use]
    pub fn doc(&self, id: &[u8]) -> Vec<u8> {
        let mut k = self.docs();
        k.extend_from_slice(id);
        k
    }

    /// The prefix of index `i`'s entries.
    #[must_use]
    pub fn index(&self, i: u16, unique: bool) -> Vec<u8> {
        prefix(
            self.tenant,
            self.object,
            if unique {
                kind::UNIQUE
            } else {
                kind::SECONDARY
            },
            i,
        )
    }

    /// The multikey fact of index `i`.
    #[must_use]
    pub fn multikey(&self, i: u16) -> Vec<u8> {
        let mut k = prefix(self.tenant, self.object, kind::META, MULTIKEY);
        k.extend_from_slice(&i.to_be_bytes());
        k
    }

    /// The prefix of every key of the collection.
    #[must_use]
    pub fn all(&self) -> Vec<u8> {
        let mut k = self.docs();
        k.truncate(16);
        k
    }
}

/// The first key after every key that starts with `p`.
#[must_use]
pub fn after_prefix(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(last) = e.last_mut() {
        if *last == 0xFF {
            e.pop();
        } else {
            *last += 1;
            return e;
        }
    }
    vec![0xFF; p.len() + 1]
}

/// A stored document's value: the header, then the BSON.
#[must_use]
pub fn doc_value(bson: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(bson.len() + 1);
    v.push(DOC_FORMAT_1);
    v.extend_from_slice(bson);
    v
}

/// The BSON of a stored document's value.
#[must_use]
pub fn doc_bson(value: &[u8]) -> &[u8] {
    value.get(1..).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_nest_and_sort() {
        let s = Space::new(7, 1);
        assert!(s.doc(b"\x1e\x04").starts_with(&s.docs()));
        assert!(s.docs().starts_with(&s.all()));
        assert!(s.index(1, false).starts_with(&s.all()));
        let a = coll_record(7, "a", "b");
        let b = coll_record(7, "a", "bb");
        let c = coll_record(7, "ab", "");
        assert!(a < b && b < c);
        assert!(a.starts_with(&coll_records(7, Some("a"))));
        assert!(!c.starts_with(&coll_records(7, Some("a"))));
        assert_eq!(after_prefix(&[1, 0xFF]), vec![2]);
        assert!(retry_record(7, b"x", -1) < retry_record(7, b"x", 0));
        let k = coll_record(7, "d\0b", "c");
        let (_, user) = sprig_engine::keys::KeyPrefix::decode(&k).unwrap();
        assert_eq!(
            catalog_key(0, user),
            CatalogKey::Coll {
                db: "d\0b".into(),
                coll: "c".into()
            }
        );
        let k = db_drop_marker(7, "x");
        let (p, user) = sprig_engine::keys::KeyPrefix::decode(&k).unwrap();
        assert_eq!(catalog_key(p.index, user), CatalogKey::DbDrop("x".into()));
    }
}
