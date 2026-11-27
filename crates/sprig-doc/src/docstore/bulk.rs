//! MongoDB 8.0's client-level `bulkWrite` over the store: `ops` across the
//! collections named in `nsInfo`, run as one command.
//!
//! Each op becomes a one-statement `insert`, `update` or `delete` of its
//! collection and runs in the command's engine transaction, so the whole
//! `bulkWrite` is one engine transaction outside a multi-document
//! transaction (rerun whole after a write conflict, its reply stored for
//! retries) and part of the session's transaction inside one. An op that
//! fails is undone alone and reported in the cursor; an ordered command
//! stops there.

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::cmd::Reply;
use super::{DocStore, DocTxn, Name};

/// 16 `InvalidLength`.
const INVALID_LENGTH: i32 = 16;
/// 40414 `IDLFailedToParse`.
const IDL_FAILED_TO_PARSE: i32 = 40414;
/// 40415 `IDLUnknownField`.
const IDL_UNKNOWN_FIELD: i32 = 40415;
/// MongoDB's `maxWriteBatchSize`.
const MAX_OPS: usize = 100_000;

/// One parsed op: its kind, its collection and the one-statement command
/// that runs it.
#[derive(Debug, Clone)]
pub struct BulkOp {
    /// `insert`, `update` or `delete`.
    pub kind: &'static str,
    /// The collection.
    pub ns: Name,
    /// The command (`insert`, `update` or `delete` with one statement).
    pub body: Document,
}

fn required(field: &str) -> MongoError {
    MongoError::new(
        IDL_FAILED_TO_PARSE,
        format!("BSON field 'bulkWrite.ops.{field}' is missing but a required field"),
    )
}

/// `nsInfo`'s namespaces, by index.
///
/// # Errors
/// 73 for a namespace without a database and a collection.
pub fn namespaces(body: &Document) -> Result<Vec<Name>, MongoError> {
    body.get_array("nsInfo")
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(Bson::as_document)
        .map(|d| {
            let ns = d.get_str("ns").unwrap_or_default();
            match ns.split_once('.') {
                Some((db, coll)) if !db.is_empty() && !coll.is_empty() => Ok(Name::new(db, coll)),
                _ => Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    format!("Invalid namespace specified for bulkWrite: '{ns}'"),
                )),
            }
        })
        .collect()
}

/// Parses every op before anything runs, as MongoDB does: a bad op fails
/// the whole command.
///
/// # Errors
/// 16 for no ops or more than 100,000; 2 for a bad `nsInfo` index; 14 for
/// an index that isn't a number; 40414 and 40415 for missing and unknown
/// fields; 73 for a bad namespace.
pub fn parse(body: &Document) -> Result<Vec<BulkOp>, MongoError> {
    let nss = namespaces(body)?;
    let ops: Vec<&Document> = body
        .get_array("ops")
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(Bson::as_document)
        .collect();
    if ops.is_empty() || ops.len() > MAX_OPS {
        return Err(MongoError::new(
            INVALID_LENGTH,
            format!(
                "Write batch sizes must be between 1 and {MAX_OPS}. Got {} operations.",
                ops.len()
            ),
        ));
    }
    let lets = body.get_document("let").ok();
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        let Some((first, ix)) = op.iter().next() else {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "Unrecognized bulkWrite op {}",
            ));
        };
        let (kind, allowed): (&'static str, &[&str]) = match first.as_str() {
            "insert" => ("insert", &["insert", "document"]),
            "update" => (
                "update",
                &[
                    "update",
                    "filter",
                    "updateMods",
                    "arrayFilters",
                    "multi",
                    "hint",
                    "upsert",
                    "collation",
                    "sort",
                    "constants",
                ],
            ),
            "delete" => (
                "delete",
                &["delete", "filter", "multi", "hint", "collation"],
            ),
            other => {
                return Err(MongoError::new(
                    IDL_UNKNOWN_FIELD,
                    format!("BSON field 'bulkWrite.{other}' is an unknown field."),
                ));
            }
        };
        if let Some(k) = op.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(MongoError::new(
                IDL_UNKNOWN_FIELD,
                format!("BSON field 'bulkWrite.ops.{k}' is an unknown field."),
            ));
        }
        let Some(i) = crate::filter::as_i64(ix) else {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                format!(
                    "BSON field 'bulkWrite.ops.{kind}' is the wrong type '{}', expected types '[long, double, int, decimal']",
                    crate::update::type_name(ix)
                ),
            ));
        };
        let Some(ns) = usize::try_from(i).ok().and_then(|i| nss.get(i)) else {
            return Err(MongoError::new(
                code::BAD_VALUE,
                format!(
                    "BulkWrite ops entry {} has an invalid nsInfo index.",
                    super::cmd::shell(&Bson::Document(op.clone()))
                ),
            ));
        };
        let copy = |from: &Document, to: &mut Document, keys: &[&str]| {
            for k in keys {
                if let Some(v) = from.get(*k) {
                    to.insert(*k, v.clone());
                }
            }
        };
        let mut sub = match kind {
            "insert" => {
                let d = op
                    .get_document("document")
                    .map_err(|_| required("document"))?;
                doc! {"insert": ns.coll.clone(), "documents": [d.clone()]}
            }
            "update" => {
                let u = op.get("updateMods").ok_or_else(|| required("updateMods"))?;
                let q = op.get_document("filter").map_err(|_| required("filter"))?;
                let mut s = doc! {"q": q.clone(), "u": u.clone()};
                copy(
                    op,
                    &mut s,
                    &[
                        "multi",
                        "upsert",
                        "arrayFilters",
                        "hint",
                        "collation",
                        "sort",
                    ],
                );
                if let Some(c) = op.get("constants") {
                    s.insert("c", c.clone());
                }
                doc! {"update": ns.coll.clone(), "updates": [s]}
            }
            _ => {
                let q = op.get_document("filter").map_err(|_| required("filter"))?;
                let multi = op.get_bool("multi").unwrap_or(false);
                let mut s = doc! {"q": q.clone(), "limit": i32::from(!multi)};
                copy(op, &mut s, &["hint", "collation"]);
                doc! {"delete": ns.coll.clone(), "deletes": [s]}
            }
        };
        if let Some(l) = lets {
            sub.insert("let", l.clone());
        }
        out.push(BulkOp {
            kind,
            ns: ns.clone(),
            body: sub,
        });
    }
    Ok(out)
}

/// The collections the command may create (inserts and upserts), once
/// each.
#[must_use]
pub fn creates(ops: &[BulkOp]) -> Vec<Name> {
    let mut out: Vec<Name> = Vec::new();
    for op in ops {
        let upsert = op.kind == "update"
            && op
                .body
                .get_array("updates")
                .ok()
                .and_then(|a| a.first())
                .and_then(Bson::as_document)
                .is_some_and(|s| s.get_bool("upsert") == Ok(true));
        if (op.kind == "insert" || upsert) && !out.contains(&op.ns) {
            out.push(op.ns.clone());
        }
    }
    out
}

impl DocStore {
    /// Runs the parsed ops in `t`. An op's failure is a result in the
    /// cursor (its writes undone alone); a write conflict, or any failure
    /// inside a multi-document transaction, fails the command.
    pub(super) async fn bulk_body(
        &self,
        tenant: u64,
        body: &Document,
        ops: &[BulkOp],
        t: &mut DocTxn,
        multi: bool,
    ) -> Result<Reply, MongoError> {
        let ordered = body.get_bool("ordered").unwrap_or(true);
        let errors_only = body.get_bool("errorsOnly").unwrap_or(false);
        let (mut ins, mut matched, mut modified, mut ups, mut del, mut errs) =
            (0i64, 0i64, 0i64, 0i64, 0i64, 0i32);
        let mut batch = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            let idx = i32::try_from(i).unwrap_or(i32::MAX);
            if op.kind == "update" && sorted_multi(&op.body) {
                errs += 1;
                batch.push(Bson::Document(doc! {
                    "ok": 0.0, "idx": idx, "code": code::FAILED_TO_PARSE,
                    "errmsg": SORT_WITH_MULTI, "n": 0, "nModified": 0,
                }));
                if ordered {
                    break;
                }
                continue;
            }
            let r = match Box::pin(self.write_body(tenant, &op.ns.db, op.kind, &op.body, t, multi))
                .await?
            {
                Reply::Doc(d) => d,
                Reply::Cursor { .. } => {
                    return Err(MongoError::new(
                        code::INTERNAL_ERROR,
                        "bulkWrite op answered a cursor",
                    ));
                }
            };
            let n = r.get("n").and_then(crate::filter::as_i64).unwrap_or(0);
            if let Some(e) = r
                .get_array("writeErrors")
                .ok()
                .and_then(|a| a.first())
                .and_then(Bson::as_document)
            {
                errs += 1;
                let mut d = doc! {"ok": 0.0, "idx": idx};
                for (k, v) in e {
                    if k != "index" && k != "codeName" {
                        d.insert(k.clone(), v.clone());
                    }
                }
                d.insert("n", 0);
                if op.kind == "update" {
                    d.insert("nModified", 0);
                }
                batch.push(Bson::Document(d));
                if ordered {
                    break;
                }
                continue;
            }
            let mut d = doc! {"ok": 1.0, "idx": idx, "n": int(n)};
            match op.kind {
                "insert" => ins += n,
                "update" => {
                    let m = r
                        .get("nModified")
                        .and_then(crate::filter::as_i64)
                        .unwrap_or(0);
                    d.insert("nModified", int(m));
                    if let Some(u) = r
                        .get_array("upserted")
                        .ok()
                        .and_then(|a| a.first())
                        .and_then(Bson::as_document)
                    {
                        ups += 1;
                        d.insert(
                            "upserted",
                            doc! {"_id": u.get("_id").cloned().unwrap_or(Bson::Null)},
                        );
                    } else {
                        matched += n;
                        modified += m;
                    }
                }
                _ => del += n,
            }
            if !errors_only {
                batch.push(Bson::Document(d));
            }
        }
        Ok(Reply::Doc(doc! {
            "cursor": {"id": 0i64, "firstBatch": batch, "ns": "admin.$cmd.bulkWrite"},
            "nErrors": errs,
            "nInserted": int(ins),
            "nMatched": int(matched),
            "nModified": int(modified),
            "nUpserted": int(ups),
            "nDeleted": int(del),
            "ok": 1.0,
        }))
    }
}

/// MongoDB's answer to an update with both `sort` and `multi: true`.
pub const SORT_WITH_MULTI: &str = "Cannot specify sort with multi=true";

/// Whether an `update` command's statement asks for `sort` with
/// `multi: true`.
#[must_use]
pub fn sorted_multi(update: &Document) -> bool {
    update
        .get_array("updates")
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(Bson::as_document)
        .any(|s| {
            s.get_bool("multi") == Ok(true) && s.get_document("sort").is_ok_and(|d| !d.is_empty())
        })
}

fn int(n: i64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ops_parse_into_one_statement_commands() {
        let body = doc! {
            "bulkWrite": 1,
            "ops": [
                {"insert": 0, "document": {"_id": 1}},
                {"update": 1, "filter": {"_id": 1}, "updateMods": {"$set": {"a": 1}}, "upsert": true},
                {"delete": 0, "filter": {}, "multi": true},
            ],
            "nsInfo": [{"ns": "d.a"}, {"ns": "d.b"}],
        };
        let ops = parse(&body).unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[1].ns, Name::new("d", "b"));
        assert_eq!(
            ops[2].body,
            doc! {"delete": "a", "deletes": [{"q": {}, "limit": 0}]}
        );
        assert_eq!(
            creates(&ops),
            vec![Name::new("d", "a"), Name::new("d", "b")]
        );
    }

    #[test]
    fn bad_ops_fail_the_command() {
        let e = |b: Document| parse(&b).unwrap_err().code;
        assert_eq!(e(doc! {"ops": [], "nsInfo": [{"ns": "d.a"}]}), 16);
        assert_eq!(
            e(doc! {"ops": [{"insert": 3, "document": {}}], "nsInfo": [{"ns": "d.a"}]}),
            2
        );
        assert_eq!(
            e(doc! {"ops": [{"insert": 0}], "nsInfo": [{"ns": "d.a"}]}),
            40414
        );
        assert_eq!(
            e(doc! {"ops": [{"foo": 0}], "nsInfo": [{"ns": "d.a"}]}),
            40415
        );
        assert_eq!(
            e(doc! {"ops": [{"insert": 0, "document": {}, "x": 1}], "nsInfo": [{"ns": "d.a"}]}),
            40415
        );
        assert_eq!(
            e(doc! {"ops": [{"insert": "0", "document": {}}], "nsInfo": [{"ns": "d.a"}]}),
            14
        );
        assert_eq!(
            e(doc! {"ops": [{"insert": 0, "document": {}}], "nsInfo": [{"ns": "d"}]}),
            73
        );
    }
}
