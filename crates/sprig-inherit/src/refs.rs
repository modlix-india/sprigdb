//! The reference index's operations ([`crate::links`]), and the index as
//! records of the tree root's catalog.
//!
//! A tenant whose catalog is records only (no `t<id>_sprig` schema) keeps
//! the index as unversioned rows of its catalog records, each operation in
//! a short engine transaction of its own, outside catalog versions (as the
//! `inherit_refs` table's statements were autocommitted):
//!
//! ```text
//! m/inherit/refs/<ref_obj u64><kind u8><ref_key><tenant u64><space u64><from_obj u64><from_key><link>
//!     {"at_us": <when it was written, µs since the epoch>}
//! kind: 0 a claim ('ref'), 1 a deletion marker ('del'); byte strings escaped (prefix-free)
//! ```
//!
//! The primary key is the table's, so a claim or marker written again only
//! moves its time, and the entries of one referenced key are one range.
//! A handover claim (its link starts with [`crate::links::HANDOVER`]) is
//! also kept by the row it hands over, so the orphan pass finds the values
//! a referencing row's handovers hold ([`RefsOp::Handed`]) without reading
//! every claim of the referenced object (the table's `by_from` index):
//!
//! ```text
//! m/inherit/handed/<ref_obj u64><link><tenant u64><from_obj u64><from_key><ref_key>
//!     {"at_us": ...}
//! ```
//! Tenants that still have the catalog schema keep the table; [`RefsOp::sql`]
//! is each operation's statement there.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_common::Result;
use sprig_common::ids::ObjectId;

use crate::RawRows;
use crate::links::{At, Entry, Ref};

/// The index's rows in the tree root's catalog records.
pub const REFS: RowTable = RowTable::new("inherit", "refs");

/// Handover claims by the row they hand over.
pub const HANDED: RowTable = RowTable::new("inherit", "handed");

/// One operation on the reference index of a tree root.
#[derive(Debug, Clone, Copy)]
pub enum RefsOp<'a> {
    /// Claims (kind `ref`) of `refs` made at `here` by rows of `from_obj`
    /// through `link`, written again when they exist.
    Claim {
        /// The referenced object (in the tree root).
        ref_obj: ObjectId,
        /// Where the referencing rows are.
        here: At,
        /// The referencing object.
        from_obj: ObjectId,
        /// The link's name (a handover's starts with [`crate::links::HANDOVER`]).
        link: &'a str,
        /// The references.
        refs: &'a [Ref],
    },
    /// Deletion markers (kind `del`) of keys `keys` made at `here`; `what`
    /// is `hide` or `drop`.
    Mark {
        /// The referenced object.
        ref_obj: ObjectId,
        /// The deleting level.
        here: At,
        /// `hide` or `drop`.
        what: &'a str,
        /// The keys, as SQL hex literals.
        keys: &'a [String],
    },
    /// The entries of `kind` (`ref` or `del`) for keys `keys`, as
    /// [`crate::links::parse_entries`] reads them.
    Entries {
        /// The referenced object.
        ref_obj: ObjectId,
        /// `ref` or `del`.
        kind: &'a str,
        /// The keys, as SQL hex literals.
        keys: &'a [String],
    },
    /// Removes one entry of `kind`.
    Forget {
        /// The referenced object.
        ref_obj: ObjectId,
        /// `ref` or `del`.
        kind: &'a str,
        /// The entry.
        entry: &'a Entry,
    },
    /// Removes the deletion markers of `keys` made at `here`.
    Unmark {
        /// The referenced object.
        ref_obj: ObjectId,
        /// The level.
        here: At,
        /// The keys, as SQL hex literals.
        keys: &'a [String],
    },
    /// The handover claims of link `link` (its name with the
    /// [`crate::links::HANDOVER`] prefix) made by rows `from_keys` (SQL hex
    /// literals) of the (tenant, object) pairs `pairs`, oldest first: rows
    /// of tenant, HEX(from key), HEX(referenced key).
    Handed {
        /// The referenced object.
        ref_obj: ObjectId,
        /// The handover's link name.
        link: &'a str,
        /// The levels' (tenant, referencing object).
        pairs: &'a [(u64, u64)],
        /// The referencing rows' keys.
        from_keys: &'a [String],
    },
}

impl RefsOp<'_> {
    /// The operation as a statement on the `inherit_refs` table (`{refs}`
    /// stands for it).
    #[must_use]
    pub fn sql(&self) -> String {
        use crate::links::{claims_sql, entries_sql, forget_sql, markers_sql, unmark_sql};
        match *self {
            Self::Claim {
                ref_obj,
                here,
                from_obj,
                link,
                refs,
            } => claims_sql(ref_obj, here, from_obj, link, refs),
            Self::Mark {
                ref_obj,
                here,
                what,
                keys,
            } => markers_sql(ref_obj, here, what, keys),
            Self::Entries {
                ref_obj,
                kind,
                keys,
            } => entries_sql(ref_obj, kind, keys),
            Self::Forget {
                ref_obj,
                kind,
                entry,
            } => forget_sql(ref_obj, kind, entry),
            Self::Unmark {
                ref_obj,
                here,
                keys,
            } => unmark_sql(ref_obj, here, keys),
            Self::Handed {
                ref_obj,
                link,
                pairs,
                from_keys,
            } => format!(
                "SELECT tenant, HEX(from_key), HEX(ref_key) FROM {{refs}} WHERE ref_obj = {} AND \
                 kind = 'ref' AND (tenant, from_obj) IN ({}) AND link = {} AND from_key IN ({}) \
                 ORDER BY at",
                ref_obj.0,
                pairs
                    .iter()
                    .map(|(t, o)| format!("({t}, {o})"))
                    .collect::<Vec<_>>()
                    .join(", "),
                crate::view::lit(link),
                from_keys.join(", ")
            ),
        }
    }

    /// Whether the operation only reads.
    #[must_use]
    pub fn reads(&self) -> bool {
        matches!(self, Self::Entries { .. } | Self::Handed { .. })
    }
}

/// A row's value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RefRow {
    /// When it was written (µs since the epoch).
    at_us: u64,
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

fn kind_byte(kind: &str) -> u8 {
    u8::from(kind == "del")
}

/// The bytes of a key as the statements write it: `X'..'` or `''`.
fn unhex(lit: &str) -> Vec<u8> {
    let h = lit.trim().trim_start_matches(['X', 'x']).trim_matches('\'');
    (0..h.len() / 2)
        .filter_map(|i| u8::from_str_radix(h.get(i * 2..i * 2 + 2)?, 16).ok())
        .collect()
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

/// Reads one escaped byte string of a primary key (see [`Pk::bytes`]) at
/// `*at`, moving past it.
fn read_bytes(k: &[u8], at: &mut usize) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let b = *k.get(*at)?;
        *at += 1;
        if b == 0 {
            match *k.get(*at)? {
                0 => {
                    *at += 1;
                    return Some(out);
                }
                0xFF => {
                    *at += 1;
                    out.push(0);
                }
                _ => return None,
            }
        } else {
            out.push(b);
        }
    }
}

fn read_u64(k: &[u8], at: &mut usize) -> Option<u64> {
    let v = u64::from_be_bytes(k.get(*at..*at + 8)?.try_into().ok()?);
    *at += 8;
    Some(v)
}

/// The prefix of the entries of one referenced key.
fn key_pk(ref_obj: ObjectId, kind: &str, key: &[u8]) -> Pk {
    Pk::new().u64(ref_obj.0).u8(kind_byte(kind)).bytes(key)
}

#[allow(clippy::too_many_arguments)]
fn entry_pk(
    ref_obj: ObjectId,
    kind: &str,
    key: &[u8],
    at: At,
    from_obj: u64,
    from_key: &[u8],
    link: &str,
) -> Pk {
    key_pk(ref_obj, kind, key)
        .u64(at.tenant.0)
        .u64(at.space.0)
        .u64(from_obj)
        .bytes(from_key)
        .bytes(link.as_bytes())
}

/// A handover's row by the row it hands over (see [`HANDED`]).
fn handed_pk(ref_obj: ObjectId, link: &str, tenant: u64, from_obj: u64, from_key: &[u8]) -> Pk {
    Pk::new()
        .u64(ref_obj.0)
        .bytes(link.as_bytes())
        .u64(tenant)
        .u64(from_obj)
        .bytes(from_key)
}

/// One stored entry, decoded from the part of its primary key after the
/// referenced key.
struct Stored {
    tenant: u64,
    space: u64,
    from_obj: u64,
    from_key: Vec<u8>,
    link: String,
}

fn decode_rest(k: &[u8], mut at: usize) -> Option<Stored> {
    let tenant = read_u64(k, &mut at)?;
    let space = read_u64(k, &mut at)?;
    let from_obj = read_u64(k, &mut at)?;
    let from_key = read_bytes(k, &mut at)?;
    let link = String::from_utf8(read_bytes(k, &mut at)?).ok()?;
    Some(Stored {
        tenant,
        space,
        from_obj,
        from_key,
        link,
    })
}

/// Runs `op` on the index in `tx` (the tree root's records; the caller
/// commits). Reads return [`RefsOp::Entries`]' rows as the statement's
/// text rows: tenant, space, from object, HEX(from key), HEX(referenced
/// key), link.
///
/// # Errors
/// The store's.
pub async fn run(tx: &mut dyn RecordTx, op: &RefsOp<'_>) -> Result<RawRows> {
    let row = RefRow { at_us: now_us() };
    match *op {
        RefsOp::Claim {
            ref_obj,
            here,
            from_obj,
            link,
            refs,
        } => {
            for r in refs {
                let pk = entry_pk(
                    ref_obj,
                    "ref",
                    &unhex(&r.ref_hex),
                    here,
                    from_obj.0,
                    &unhex(&r.from_hex),
                    link,
                );
                REFS.put(tx, &pk, &row).await?;
                if link.starts_with(crate::links::HANDOVER) {
                    let by = handed_pk(
                        ref_obj,
                        link,
                        here.tenant.0,
                        from_obj.0,
                        &unhex(&r.from_hex),
                    )
                    .bytes(&unhex(&r.ref_hex));
                    HANDED.put(tx, &by, &row).await?;
                }
            }
            Ok(Vec::new())
        }
        RefsOp::Mark {
            ref_obj,
            here,
            what,
            keys,
        } => {
            // The marker's link (what the delete did) is part of the
            // table's primary key but is rewritten on a repeat: the other
            // one goes first.
            for k in keys {
                let key = unhex(k);
                for other in ["hide", "drop"] {
                    if other != what {
                        let pk = entry_pk(ref_obj, "del", &key, here, 0, &[], other);
                        REFS.delete(tx, &pk).await?;
                    }
                }
                let pk = entry_pk(ref_obj, "del", &key, here, 0, &[], what);
                REFS.put(tx, &pk, &row).await?;
            }
            Ok(Vec::new())
        }
        RefsOp::Entries {
            ref_obj,
            kind,
            keys,
        } => {
            let mut out = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            for k in keys {
                let key = unhex(k);
                if !seen.insert(key.clone()) {
                    continue;
                }
                let under = key_pk(ref_obj, kind, &key);
                for (pk, _) in REFS.scan::<RefRow>(tx, &under).await? {
                    let Some(s) = decode_rest(&pk.0, under.0.len()) else {
                        continue;
                    };
                    out.push(vec![
                        Some(s.tenant.to_string().into_bytes()),
                        Some(s.space.to_string().into_bytes()),
                        Some(s.from_obj.to_string().into_bytes()),
                        Some(hex(&s.from_key).into_bytes()),
                        Some(hex(&key).into_bytes()),
                        Some(s.link.into_bytes()),
                    ]);
                }
            }
            Ok(out)
        }
        RefsOp::Forget {
            ref_obj,
            kind,
            entry,
        } => {
            let pk = entry_pk(
                ref_obj,
                kind,
                &unhex(&entry.ref_hex),
                entry.at,
                entry.from_obj.0,
                &unhex(&entry.from_hex),
                &entry.link,
            );
            REFS.delete(tx, &pk).await?;
            if kind == "ref" && entry.link.starts_with(crate::links::HANDOVER) {
                let by = handed_pk(
                    ref_obj,
                    &entry.link,
                    entry.at.tenant.0,
                    entry.from_obj.0,
                    &unhex(&entry.from_hex),
                )
                .bytes(&unhex(&entry.ref_hex));
                HANDED.delete(tx, &by).await?;
            }
            Ok(Vec::new())
        }
        RefsOp::Handed {
            ref_obj,
            link,
            pairs,
            from_keys,
        } => {
            let mut found: Vec<(u64, u64, Vec<u8>, Vec<u8>)> = Vec::new();
            for &(tenant, from_obj) in pairs {
                for k in from_keys {
                    let from = unhex(k);
                    let under = handed_pk(ref_obj, link, tenant, from_obj, &from);
                    for (pk, v) in HANDED.scan::<RefRow>(tx, &under).await? {
                        let mut at = under.0.len();
                        let Some(to) = read_bytes(&pk.0, &mut at) else {
                            continue;
                        };
                        found.push((v.at_us, tenant, from.clone(), to));
                    }
                }
            }
            found.sort();
            Ok(found
                .into_iter()
                .map(|(_, t, from, to)| {
                    vec![
                        Some(t.to_string().into_bytes()),
                        Some(hex(&from).into_bytes()),
                        Some(hex(&to).into_bytes()),
                    ]
                })
                .collect())
        }
        RefsOp::Unmark {
            ref_obj,
            here,
            keys,
        } => {
            for k in keys {
                let key = unhex(k);
                let under = key_pk(ref_obj, "del", &key)
                    .u64(here.tenant.0)
                    .u64(here.space.0);
                for (pk, _) in REFS.scan::<RefRow>(tx, &under).await? {
                    REFS.delete(tx, &pk).await?;
                }
            }
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sprig_catalog::records::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_common::ids::TenantId;

    use super::*;
    use crate::links::{key_hex, parse_entries};
    use crate::model::SpaceId;

    fn at(t: u64, s: u64) -> At {
        At {
            tenant: TenantId(t),
            space: SpaceId(s),
        }
    }

    async fn op(store: &Arc<MemRecords>, o: RefsOp<'_>) -> RawRows {
        let mut tx = store.begin(TenantId(1)).await.unwrap();
        let rows = run(&mut *tx, &o).await.unwrap();
        tx.commit().await.unwrap();
        rows
    }

    #[tokio::test]
    async fn claims_markers_and_their_removal_round_trip() {
        let store = Arc::new(MemRecords::default());
        let obj = ObjectId(7);
        let k1 = key_hex(&[b"1".to_vec()]);
        let k2 = key_hex(&[b"a\0b".to_vec(), b"x".to_vec()]);
        let refs = vec![
            Ref {
                from_hex: key_hex(&[b"10".to_vec()]),
                ref_hex: k1.clone(),
                ref_sql: String::new(),
            },
            Ref {
                from_hex: key_hex(&[b"11".to_vec()]),
                ref_hex: k2.clone(),
                ref_sql: String::new(),
            },
        ];
        let claim = RefsOp::Claim {
            ref_obj: obj,
            here: at(5, 1),
            from_obj: ObjectId(9),
            link: "fk1",
            refs: &refs,
        };
        op(&store, claim).await;
        // Written again: still one claim per row.
        op(&store, claim).await;
        let keys = vec![k1.clone(), k2.clone(), k1.clone()];
        let got = parse_entries(
            &op(
                &store,
                RefsOp::Entries {
                    ref_obj: obj,
                    kind: "ref",
                    keys: &keys,
                },
            )
            .await,
        );
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].ref_hex, k1);
        assert_eq!(got[0].from_hex, key_hex(&[b"10".to_vec()]));
        assert_eq!(got[1].ref_hex, k2);
        assert_eq!(got[1].at, at(5, 1));
        assert_eq!(got[1].from_obj, ObjectId(9));
        assert_eq!(got[1].link, "fk1");
        // Another object's or kind's entries are apart.
        let none = op(
            &store,
            RefsOp::Entries {
                ref_obj: ObjectId(8),
                kind: "ref",
                keys: &keys,
            },
        )
        .await;
        assert!(none.is_empty());
        // Markers: a repeat with another action replaces the first.
        let mk = vec![k1.clone()];
        for what in ["hide", "drop"] {
            op(
                &store,
                RefsOp::Mark {
                    ref_obj: obj,
                    here: at(5, 2),
                    what,
                    keys: &mk,
                },
            )
            .await;
        }
        let markers = parse_entries(
            &op(
                &store,
                RefsOp::Entries {
                    ref_obj: obj,
                    kind: "del",
                    keys: &mk,
                },
            )
            .await,
        );
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].link, "drop");
        assert_eq!(markers[0].from_hex, "''");
        op(
            &store,
            RefsOp::Unmark {
                ref_obj: obj,
                here: at(5, 2),
                keys: &mk,
            },
        )
        .await;
        assert!(
            op(
                &store,
                RefsOp::Entries {
                    ref_obj: obj,
                    kind: "del",
                    keys: &mk,
                },
            )
            .await
            .is_empty()
        );
        op(
            &store,
            RefsOp::Forget {
                ref_obj: obj,
                kind: "ref",
                entry: &got[1],
            },
        )
        .await;
        let left = parse_entries(
            &op(
                &store,
                RefsOp::Entries {
                    ref_obj: obj,
                    kind: "ref",
                    keys: &keys,
                },
            )
            .await,
        );
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].ref_hex, k1);
    }

    /// Handover claims are found by the row they hand over, and go with
    /// their claim.
    #[tokio::test]
    async fn handovers_are_found_by_the_row_handed_over() {
        let store = Arc::new(MemRecords::default());
        let obj = ObjectId(7);
        let from = key_hex(&[b"10".to_vec()]);
        let refs = vec![Ref {
            from_hex: from.clone(),
            ref_hex: key_hex(&[b"3".to_vec()]),
            ref_sql: String::new(),
        }];
        for (link, t) in [("~fk1", 5), ("fk1", 5), ("~fk1", 6)] {
            op(
                &store,
                RefsOp::Claim {
                    ref_obj: obj,
                    here: at(t, 1),
                    from_obj: ObjectId(9),
                    link,
                    refs: &refs,
                },
            )
            .await;
        }
        let froms = vec![from.clone()];
        let handed = |pairs: &'static [(u64, u64)]| RefsOp::Handed {
            ref_obj: obj,
            link: "~fk1",
            pairs,
            from_keys: &[],
        };
        assert!(op(&store, handed(&[(5, 9)])).await.is_empty());
        let got = op(
            &store,
            RefsOp::Handed {
                ref_obj: obj,
                link: "~fk1",
                pairs: &[(5, 9), (6, 9)],
                from_keys: &froms,
            },
        )
        .await;
        let text: Vec<Vec<String>> = got
            .iter()
            .map(|r| {
                r.iter()
                    .map(|v| String::from_utf8_lossy(v.as_deref().unwrap_or_default()).into())
                    .collect()
            })
            .collect();
        assert_eq!(
            text,
            [["5", "3130", "33"], ["6", "3130", "33"]].map(|r| r.map(String::from).to_vec())
        );
        let entry = Entry {
            at: at(5, 1),
            from_obj: ObjectId(9),
            from_hex: from.clone(),
            ref_hex: key_hex(&[b"3".to_vec()]),
            link: "~fk1".into(),
        };
        op(
            &store,
            RefsOp::Forget {
                ref_obj: obj,
                kind: "ref",
                entry: &entry,
            },
        )
        .await;
        let left = op(
            &store,
            RefsOp::Handed {
                ref_obj: obj,
                link: "~fk1",
                pairs: &[(5, 9), (6, 9)],
                from_keys: &froms,
            },
        )
        .await;
        assert_eq!(left.len(), 1);
    }
}
