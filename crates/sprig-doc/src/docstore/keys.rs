//! The index entries of a document: one per distinct combination of the
//! indexed values, arrays expanded one entry per element (multikey).

use std::collections::BTreeSet;
use std::hash::Hasher;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::MongoError;

use super::catalog::{FieldKind, IndexMeta};
use crate::keyenc;

/// The values at one indexed path.
#[derive(Debug, Default)]
struct Found {
    vals: Vec<Bson>,
    /// Some value was really there (a stored null counts); otherwise the
    /// path is missing and `vals` is `[null]`.
    present: bool,
    /// The path of the first array met, when one was.
    array_at: Option<String>,
}

fn is_index(c: &str) -> Option<usize> {
    if c.is_empty() || (c.len() > 1 && c.starts_with('0')) {
        return None;
    }
    c.parse().ok()
}

fn walk(v: &Bson, comps: &[&str], in_array: bool, f: &mut Found, so_far: &str) {
    let Some((head, rest)) = comps.split_first() else {
        match v {
            Bson::Array(a) => {
                f.present = true;
                if f.array_at.is_none() {
                    f.array_at = Some(so_far.to_owned());
                }
                if a.is_empty() {
                    f.vals.push(Bson::Undefined);
                } else {
                    f.vals.extend(a.iter().cloned());
                }
            }
            other => {
                f.present = true;
                f.vals.push(other.clone());
            }
        }
        return;
    };
    let path = if so_far.is_empty() {
        (*head).to_owned()
    } else {
        format!("{so_far}.{head}")
    };
    match v {
        Bson::Document(d) => {
            if let Some(x) = d.get(*head) {
                walk(x, rest, false, f, &path);
            } else if in_array {
                // An element without the field indexes as null.
                f.vals.push(Bson::Null);
            }
        }
        Bson::Array(a) => {
            if f.array_at.is_none() {
                f.array_at = Some(so_far.to_owned());
            }
            if let Some(i) = is_index(head) {
                if let Some(x) = a.get(i) {
                    walk(x, rest, false, f, &path);
                }
                return;
            }
            for e in a {
                if let Bson::Document(_) = e {
                    walk(e, comps, true, f, so_far);
                }
            }
        }
        _ => {}
    }
}

fn values(doc: &Document, path: &str) -> Found {
    let comps: Vec<&str> = path.split('.').collect();
    let mut f = Found::default();
    let (head, rest) = comps.split_first().map_or(("", &[][..]), |(h, r)| (*h, r));
    if let Some(v) = doc.get(head) {
        walk(v, rest, false, &mut f, head);
    }
    if f.vals.is_empty() {
        f.vals.push(Bson::Null);
    }
    f
}

/// The hashed key of a value: a 64-bit hash of its equality key.
#[must_use]
pub fn hashed(v: &Bson) -> Bson {
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(&keyenc::eq_key(v));
    Bson::Int64(h.finish().cast_signed())
}

fn component(v: &Bson, kind: FieldKind) -> Vec<u8> {
    let mut k = match kind {
        FieldKind::Hashed => keyenc::key(&hashed(v)),
        _ => keyenc::key(v),
    };
    if kind == FieldKind::Desc {
        keyenc::complement(&mut k);
    }
    k
}

/// The entries of `doc` in `idx`: each the indexed values' keys, without
/// the `_id`. Also whether some field yielded an array.
///
/// # Errors
/// 171 for two different arrays in one compound key; 16766 for an array
/// under a hashed field.
pub fn entries(doc: &Document, idx: &IndexMeta) -> Result<(Vec<Vec<u8>>, bool), MongoError> {
    if let Some(p) = &idx.partial
        && !p.matches(doc)
    {
        return Ok((Vec::new(), false));
    }
    if idx.is_wildcard() {
        return Ok(wildcard_entries(doc, idx));
    }
    let mut per_field = Vec::with_capacity(idx.fields.len());
    let mut any_present = false;
    let mut arrays: Vec<(String, String)> = Vec::new();
    for (path, kind) in &idx.fields {
        let f = values(doc, path);
        any_present |= f.present;
        if let Some(at) = &f.array_at {
            if *kind == FieldKind::Hashed {
                return Err(MongoError::new(
                    16766,
                    format!(
                        "Error: hashed indexes do not currently support array values. Found array at path: {at}"
                    ),
                ));
            }
            if let Some((other, _)) = arrays.iter().find(|(a, _)| a != at) {
                return Err(MongoError::new(
                    171,
                    format!(
                        "cannot index parallel arrays [{}] [{}]",
                        at.rsplit('.').next().unwrap_or(at),
                        other.rsplit('.').next().unwrap_or(other)
                    ),
                ));
            }
            arrays.push((at.clone(), path.clone()));
        }
        let mut comps: Vec<Vec<u8>> = f.vals.iter().map(|v| component(v, *kind)).collect();
        comps.sort();
        comps.dedup();
        per_field.push(comps);
    }
    if idx.sparse && !any_present {
        return Ok((Vec::new(), false));
    }
    let mut out: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut cur: Vec<Vec<u8>> = vec![Vec::new()];
    for comps in &per_field {
        let mut next = Vec::with_capacity(cur.len() * comps.len());
        for c in &cur {
            for k in comps {
                let mut n = c.clone();
                n.extend_from_slice(k);
                next.push(n);
            }
        }
        cur = next;
    }
    out.extend(cur);
    Ok((out.into_iter().collect(), !arrays.is_empty()))
}

/// Whether `path` is kept by a wildcard index rooted at `root` (empty: the
/// whole document) with `projection`.
fn wildcard_keeps(path: &str, root: &str, projection: Option<&Document>) -> bool {
    if !root.is_empty() {
        return path == root || path.starts_with(&format!("{root}."));
    }
    let Some(p) = projection else {
        return path != "_id" && !path.starts_with("_id.");
    };
    let mut include = None;
    let mut hit = None;
    for (k, v) in p {
        let on = crate::filter::truthy(v);
        if k != "_id" {
            include = Some(on);
        }
        if path == k || path.starts_with(&format!("{k}.")) {
            hit = Some(on);
        }
    }
    match (hit, include) {
        (Some(on), _) => on,
        (None, Some(true)) => false,
        _ => path != "_id" && !path.starts_with("_id."),
    }
}

fn wild_walk(v: &Bson, path: &str, out: &mut Vec<(String, Bson)>, multi: &mut bool) {
    match v {
        Bson::Document(d) if !d.is_empty() => {
            for (k, x) in d {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                wild_walk(x, &p, out, multi);
            }
        }
        Bson::Array(a) if !a.is_empty() => {
            *multi = true;
            for e in a {
                match e {
                    Bson::Document(d) if !d.is_empty() => wild_walk(e, path, out, multi),
                    // Nested arrays are values, not expanded.
                    other => out.push((path.to_owned(), other.clone())),
                }
            }
        }
        Bson::Array(_) => out.push((path.to_owned(), Bson::Undefined)),
        other => out.push((path.to_owned(), other.clone())),
    }
}

fn wildcard_entries(doc: &Document, idx: &IndexMeta) -> (Vec<Vec<u8>>, bool) {
    let field = idx
        .fields
        .iter()
        .find(|f| f.1 == FieldKind::Wildcard)
        .map_or("$**", |f| f.0.as_str());
    let root = field
        .strip_suffix("$**")
        .unwrap_or("")
        .trim_end_matches('.');
    let mut pairs = Vec::new();
    let mut multi = false;
    wild_walk(&Bson::Document(doc.clone()), "", &mut pairs, &mut multi);
    let mut out = BTreeSet::new();
    for (p, v) in pairs {
        if !wildcard_keeps(&p, root, idx.wildcard_projection.as_ref()) {
            continue;
        }
        out.insert(wildcard_key(&p, &v));
    }
    (out.into_iter().collect(), multi)
}

/// A wildcard entry's key: the path, then the value.
#[must_use]
pub fn wildcard_key(path: &str, v: &Bson) -> Vec<u8> {
    let mut k = keyenc::key(&Bson::String(path.to_owned()));
    keyenc::encode_key(v, &mut k);
    k
}

/// The prefix of every wildcard entry of `path`.
#[must_use]
pub fn wildcard_path(path: &str) -> Vec<u8> {
    keyenc::key(&Bson::String(path.to_owned()))
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::super::catalog::parse_index;
    use super::*;

    #[allow(clippy::needless_pass_by_value)]
    fn n(d: Document, spec: Document) -> Result<usize, i32> {
        let i = parse_index(&spec, 1).unwrap();
        entries(&d, &i).map(|(e, _)| e.len()).map_err(|e| e.code)
    }

    #[test]
    fn multikey_sparse_partial_parallel() {
        assert_eq!(n(doc! {"a": [1, 2, 2]}, doc! {"key": {"a": 1}}), Ok(2));
        assert_eq!(n(doc! {"a": []}, doc! {"key": {"a": 1}}), Ok(1));
        assert_eq!(n(doc! {}, doc! {"key": {"a": 1}}), Ok(1));
        assert_eq!(n(doc! {}, doc! {"key": {"a": 1}, "sparse": true}), Ok(0));
        assert_eq!(
            n(doc! {"a": null}, doc! {"key": {"a": 1}, "sparse": true}),
            Ok(1)
        );
        assert_eq!(
            n(
                doc! {"a": 1},
                doc! {"key": {"a": 1}, "partialFilterExpression": {"a": {"$gt": 5}}}
            ),
            Ok(0)
        );
        assert_eq!(
            n(doc! {"a": [1, 2], "b": [3]}, doc! {"key": {"a": 1, "b": 1}}),
            Err(171)
        );
        assert_eq!(
            n(
                doc! {"a": [{"b": 1, "c": 2}, {"b": 3}]},
                doc! {"key": {"a.b": 1}}
            ),
            Ok(2)
        );
        assert_eq!(
            n(doc! {"a": [1]}, doc! {"key": {"a": "hashed"}}),
            Err(16766)
        );
        assert_eq!(
            n(
                doc! {"_id": 1, "a": {"b": 1, "c": [1, 2]}, "d": {}},
                doc! {"key": {"$**": 1}}
            ),
            Ok(4)
        );
        assert_eq!(
            n(doc! {"a": {"b": 1}, "x": 1}, doc! {"key": {"a.$**": 1}}),
            Ok(1)
        );
    }

    #[test]
    fn descending_reverses_order() {
        let i = parse_index(&doc! {"key": {"a": -1}}, 1).unwrap();
        let (k1, _) = entries(&doc! {"a": 1}, &i).unwrap();
        let (k2, _) = entries(&doc! {"a": 2}, &i).unwrap();
        assert!(k1[0] > k2[0]);
    }
}
