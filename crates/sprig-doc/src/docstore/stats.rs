//! Collection statistics on the store: the `$collStats`, `$indexStats` and
//! `$planCacheStats` stages, and the `collStats` and `dbStats` commands.
//!
//! Counts and data sizes are exact: they come from one read of the
//! collection's documents (the stored BSON bytes). Index sizes are the
//! bytes of the index's entries, keys and values, read the same way up
//! to [`SCAN_LIMIT`] entries per index; past that the engine's estimate of
//! the range's size on disk is used. `_id_` is the documents' own keys.
//! Fields that have no meaning on the engine are 0 (`numOrphanDocs`,
//! `freeStorageSize`) or empty (`indexDetails`, `indexBuilds` unless a
//! build is running).

use bson::{Bson, DateTime, Document, doc};
use sprig_engine::txn::Dir;
use sprig_proto_mongo::errors::{MongoError, code};

use super::{Coll, DocStore, Name, layout};

/// Index entries read exactly before the estimate takes over.
pub const SCAN_LIMIT: usize = 200_000;

/// The sizes of one collection.
#[derive(Debug, Clone, Default)]
pub struct Sizes {
    /// Documents.
    pub count: i64,
    /// Their BSON bytes.
    pub bytes: i64,
    /// Per index, by name (`_id_` first): bytes.
    pub indexes: Vec<(String, i64)>,
}

impl Sizes {
    /// Every index's bytes.
    #[must_use]
    pub fn index_total(&self) -> i64 {
        self.indexes.iter().map(|(_, b)| *b).sum()
    }
}

fn unknown(stage: &str, k: &str) -> MongoError {
    MongoError::new(
        40415,
        format!("BSON field '{stage}.{k}' is an unknown field."),
    )
}

fn as_len(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// A size or count as MongoDB appends it: an int when it fits, else a
/// long.
fn num(n: i64) -> Bson {
    i32::try_from(n).map_or(Bson::Int64(n), Bson::Int32)
}

fn host() -> String {
    "sprigdb".to_owned()
}

impl DocStore {
    /// The bytes and entries of `[lo, hi)`, read exactly up to `limit`
    /// entries (then `None`).
    fn range_bytes(
        &self,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Option<(i64, i64, i64)>, MongoError> {
        let (mut n, mut keys, mut vals) = (0usize, 0usize, 0usize);
        let mut from = lo.to_vec();
        let mut t = self.begin(false);
        loop {
            let rows = t.scan(&from, hi, Dir::Forward, 1000)?;
            let Some((last, _)) = rows.last() else {
                break;
            };
            from = layout::after_prefix(last);
            for (k, v) in &rows {
                n += 1;
                keys += k.len();
                vals += v.len();
            }
            if n >= limit {
                t.rollback();
                return Ok(None);
            }
        }
        t.rollback();
        Ok(Some((as_len(n), as_len(keys), as_len(vals))))
    }

    fn estimate(&self, lo: &[u8], hi: &[u8]) -> i64 {
        let r = sprig_engine::keys::KeyRange::new(lo, hi);
        i64::try_from(
            self.path
                .store()
                .approx_size(sprig_engine::store::Cf::Data, &r),
        )
        .unwrap_or(i64::MAX)
    }

    /// The sizes of `c`.
    ///
    /// # Errors
    /// The store failed.
    pub fn sizes(&self, c: &Coll) -> Result<Sizes, MongoError> {
        let p = c.space.docs();
        let end = layout::after_prefix(&p);
        // Documents: always exact (the value is a 1-byte header and the
        // BSON).
        let (count, keys, vals) = self.range_bytes(&p, &end, usize::MAX)?.unwrap_or_default();
        let bytes = vals - count;
        let mut indexes = vec![("_id_".to_owned(), keys)];
        for i in &c.meta.indexes {
            if i.building {
                continue;
            }
            let p = c.space.index(i.id, i.unique);
            let end = layout::after_prefix(&p);
            let b = match self.range_bytes(&p, &end, SCAN_LIMIT)? {
                Some((_, k, v)) => k + v,
                None => self.estimate(&p, &end),
            };
            indexes.push((i.name.clone(), b));
        }
        Ok(Sizes {
            count,
            bytes,
            indexes,
        })
    }

    /// `storageStats` of `c`, sizes divided by `scale`.
    fn storage_stats(&self, c: &Coll, scale: i64) -> Result<Document, MongoError> {
        let s = self.sizes(c)?;
        let est = self.estimate(&c.space.docs(), &layout::after_prefix(&c.space.docs()));
        let storage = s.bytes.max(est);
        let idx_total = s.index_total();
        let mut sizes = Document::new();
        for (n, b) in &s.indexes {
            sizes.insert(n.clone(), num(b / scale));
        }
        let building: Vec<Bson> = c
            .meta
            .indexes
            .iter()
            .filter(|i| i.building)
            .map(|i| Bson::String(i.name.clone()))
            .collect();
        Ok(doc! {
            "size": num(s.bytes / scale),
            "count": num(s.count),
            "avgObjSize": num(if s.count > 0 { s.bytes / s.count } else { 0 }),
            "numOrphanDocs": 0,
            "storageSize": num(storage / scale),
            "freeStorageSize": 0,
            "capped": false,
            "nindexes": i32::try_from(s.indexes.len()).unwrap_or(i32::MAX),
            "indexDetails": Document::new(),
            "indexBuilds": building,
            "totalIndexSize": num(idx_total / scale),
            "totalSize": num((storage + idx_total) / scale),
            "indexSizes": sizes,
            "scaleFactor": i32::try_from(scale).unwrap_or(i32::MAX),
        })
    }

    fn scale_of(spec: &Document) -> Result<i64, MongoError> {
        match spec.get("scale") {
            None => Ok(1),
            Some(v) => match crate::filter::as_i64(v) {
                Some(n) if n >= 1 => Ok(n),
                Some(_) => Err(MongoError::new(
                    code::BAD_VALUE,
                    "BSON field 'scale' value must be >= 1, actual value '0'",
                )),
                None => Err(MongoError::new(
                    code::TYPE_MISMATCH,
                    "BSON field 'scale' is the wrong type, expected a number",
                )),
            },
        }
    }

    /// The documents of a statistics stage that opens a pipeline on
    /// `db.coll`: `None` for any other stage.
    ///
    /// # Errors
    /// 40415 for unknown fields; 28803 for `$indexStats` with fields; 26
    /// for `count` or `storageStats` of a missing collection.
    pub fn stats_stage(
        &self,
        tenant: u64,
        db: &str,
        coll: &str,
        stage: &Document,
    ) -> Result<Option<Vec<Document>>, MongoError> {
        let Some((name, arg)) = stage.iter().next() else {
            return Ok(None);
        };
        let ns = Name::new(db, coll);
        let obj = |arg: &Bson| -> Result<Document, MongoError> {
            arg.as_document().cloned().ok_or_else(|| {
                MongoError::new(
                    code::TYPE_MISMATCH,
                    format!(
                        "BSON field '{name}' is the wrong type '{}', expected type 'object'",
                        crate::update::type_name(arg)
                    ),
                )
            })
        };
        Ok(Some(match name.as_str() {
            "$collStats" => {
                let spec = obj(arg)?;
                for k in spec.keys() {
                    if !matches!(
                        k.as_str(),
                        "latencyStats" | "storageStats" | "count" | "queryExecStats"
                    ) {
                        return Err(unknown("$collStats", k));
                    }
                }
                let mut out =
                    doc! {"ns": ns.to_string(), "host": host(), "localTime": DateTime::now()};
                let c = self.coll(tenant, &ns)?.filter(|c| c.meta.view().is_none());
                if let Ok(l) = spec.get_document("latencyStats") {
                    out.insert(
                        "latencyStats",
                        crate::usage::latency_stats(
                            tenant,
                            db,
                            coll,
                            l.get_bool("histograms").unwrap_or(false),
                        ),
                    );
                }
                let missing = |what: &str| {
                    MongoError::new(
                        code::NAMESPACE_NOT_FOUND,
                        format!(
                            "Unable to retrieve {what} in $collStats stage :: caused by :: Collection [{ns}] not found."
                        ),
                    )
                };
                if let Ok(s) = spec.get_document("storageStats") {
                    let c = c.as_ref().ok_or_else(|| missing("storageStats"))?;
                    out.insert("storageStats", self.storage_stats(c, Self::scale_of(s)?)?);
                }
                if spec.contains_key("count") {
                    let c = c.as_ref().ok_or_else(|| missing("count"))?;
                    let p = c.space.docs();
                    let (n, _, _) = self
                        .range_bytes(&p, &layout::after_prefix(&p), usize::MAX)?
                        .unwrap_or_default();
                    out.insert("count", num(n));
                }
                if spec.contains_key("queryExecStats") {
                    out.insert(
                        "queryExecStats",
                        doc! {"collectionScans": {"total": 0i64, "nonTailable": 0i64}},
                    );
                }
                vec![out]
            }
            "$indexStats" => {
                if !obj(arg)?.is_empty() {
                    return Err(MongoError::new(
                        28803,
                        "The $indexStats stage specification must be an empty object",
                    ));
                }
                let Some(c) = self.coll(tenant, &ns)? else {
                    return Ok(Some(Vec::new()));
                };
                let mut out = Vec::new();
                let mut specs = vec![doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"}];
                specs.extend(
                    c.meta
                        .indexes
                        .iter()
                        .filter(|i| !i.building)
                        .map(|i| i.spec.clone()),
                );
                for spec in specs {
                    let name = spec.get_str("name").unwrap_or_default().to_owned();
                    let (ops, since) = crate::usage::index_accesses(tenant, db, coll, &name);
                    out.push(doc! {
                        "name": name,
                        "key": spec.get("key").cloned().unwrap_or(Bson::Null),
                        "host": host(),
                        "accesses": {"ops": ops, "since": since},
                        "spec": spec,
                    });
                }
                out
            }
            "$planCacheStats" => {
                for k in obj(arg)?.keys() {
                    if k != "allHosts" {
                        return Err(unknown("$planCacheStats", k));
                    }
                }
                // The store's planner is rule-based and keeps no plan
                // cache: an empty cache is the answer.
                Vec::new()
            }
            _ => return Ok(None),
        }))
    }

    /// The `collStats` command (deprecated in MongoDB, still sent by
    /// shells and tools): `$collStats`'s `storageStats` with `ns`.
    ///
    /// # Errors
    /// 26 for a missing collection.
    pub fn coll_stats_command(
        &self,
        tenant: u64,
        db: &str,
        coll: &str,
        body: &Document,
    ) -> Result<Document, MongoError> {
        let ns = Name::new(db, coll);
        let Some(c) = self.coll(tenant, &ns)?.filter(|c| c.meta.view().is_none()) else {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("Collection [{ns}] not found."),
            ));
        };
        let mut out = doc! {"ns": ns.to_string()};
        for (k, v) in self.storage_stats(&c, Self::scale_of(body)?)? {
            out.insert(k, v);
        }
        out.insert("ok", 1.0);
        Ok(out)
    }

    /// The `dbStats` command for `db`: collections, views, documents and
    /// sizes, divided by `scale`.
    ///
    /// # Errors
    /// The store failed.
    #[allow(clippy::cast_precision_loss)] // sizes far below 2^52
    pub fn db_stats(&self, tenant: u64, db: &str, body: &Document) -> Result<Document, MongoError> {
        let scale = Self::scale_of(body)?;
        let (mut colls, mut views, mut objects, mut data, mut storage, mut nidx, mut isize) =
            (0i64, 0i64, 0i64, 0i64, 0i64, 0i64, 0i64);
        for c in self.list(tenant, Some(db))? {
            if c.meta.view().is_some() {
                views += 1;
                continue;
            }
            colls += 1;
            let s = self.sizes(&c)?;
            let est = self.estimate(&c.space.docs(), &layout::after_prefix(&c.space.docs()));
            objects += s.count;
            data += s.bytes;
            storage += s.bytes.max(est);
            nidx += as_len(s.indexes.len());
            isize += s.index_total();
        }
        Ok(doc! {
            "db": db,
            "collections": colls,
            "views": views,
            "objects": objects,
            "avgObjSize": if objects > 0 { data as f64 / objects as f64 } else { 0.0 },
            "dataSize": (data / scale) as f64,
            "storageSize": (storage / scale) as f64,
            "indexes": nidx,
            "indexSize": (isize / scale) as f64,
            "totalSize": ((storage + isize) / scale) as f64,
            "scaleFactor": scale,
            "ok": 1.0,
        })
    }
}
