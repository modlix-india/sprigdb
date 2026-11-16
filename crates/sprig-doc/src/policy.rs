//! Row-level security policies on collections in MongoDB's filter form
//! (`sprigCreatePolicy`).
//!
//! A policy written as a MongoDB filter becomes an SQL expression over the
//! collection's table (`doc`, `_types`), stored as an ordinary policy
//! with the filter kept beside it (`using_mongo`, `check_mongo`). Every
//! read and write of the document engine is SQL through the user's
//! session, so the gateway's rewrite, the policy views and the write
//! triggers apply it as they do to tables; change streams evaluate the
//! filter itself ([`Pred::matches`](crate::filter::Pred::matches)).
//!
//! The SQL is never wider than MongoDB's match (a document the filter
//! doesn't match is never visible); where MongoDB's semantics need more
//! than a JSON path can say, it is narrower and the document stays hidden:
//!
//! ```text
//! {f: v}, {f: {$eq: v}}  v a string, number or boolean of the same type at f (not
//!                        ObjectId, date or other typed values), or an element of
//!                        an array at f whose elements are plain JSON
//! {f: {$in: [v, ...]}}   any of them
//! {f: {$gt|$gte|$lt|$lte: v}}  the same type at f, compared (strings binary)
//! {f: {$ne|$nin: v}}     f missing, null, or a plain scalar not equal to v
//!                        (arrays, and a missing f under a path that crosses
//!                        an array: hidden)
//! {f: {$exists: true}}   f present
//! $and, $or              combined; $nor, $not, $expr, $where, $regex, null,
//!                        $exists: false and other operators: refused (39364)
//! "$$USER", "$$USER_ID", "$$TENANT_ID", "$$HOME_TENANT_ID", "$$SESSION_SPACE",
//! "$$CLAIM.<k>", "$$ATTR.<k>"   the session functions (strings, ids as numbers)
//! ```
//!
//! A path through an array (`a.b` where `a` is an array) reads nothing, so
//! such documents stay hidden too.
//!
//! [`partial_to_sql`] turns an index's `partialFilterExpression` into SQL
//! the same way, for the column that limits a partial unique index to the
//! documents the filter matches (see [`crate::meta`]). There, strings are
//! never session values, `$type` is allowed, and what MongoDB refuses in a
//! partial filter (`$ne`, `$nin`, `$not`, `$exists: false`, ...) is 67
//! `CannotCreateIndex`.

use std::fmt::Write as _;

use bson::{Bson, Document};
use sprig_proto_mongo::errors::MongoError;

use crate::sql::{json_path, str_lit, types_path, valid_path};

/// 39364 `ER_SPRIG_RLS_POLICY_INVALID`.
pub const POLICY_INVALID: i32 = 39364;

fn invalid(why: impl std::fmt::Display) -> MongoError {
    MongoError::new(
        POLICY_INVALID,
        format!("ER_SPRIG_RLS_POLICY_INVALID: {why}"),
    )
}

/// What a filter becomes SQL for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Use {
    /// A row-level security policy.
    Policy,
    /// An index's `partialFilterExpression`.
    Partial,
}

impl Use {
    /// The error for `why`. `refused` marks what MongoDB itself refuses
    /// in a partial filter (67); the rest is what SprigDB can't express
    /// there (39662).
    fn err(self, refused: bool, why: impl std::fmt::Display) -> MongoError {
        match (self, refused) {
            (Self::Policy, _) => invalid(why),
            (Self::Partial, true) => MongoError::new(
                sprig_proto_mongo::errors::code::CANNOT_CREATE_INDEX,
                format!("Expression not supported in partial index: {why}"),
            ),
            (Self::Partial, false) => MongoError::new(
                sprig_proto_mongo::errors::code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                format!("partialFilterExpression: {why} is not supported in this version"),
            ),
        }
    }
}

/// A value a condition compares with.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    /// A string literal or a string session function.
    Str(String),
    /// A number literal or a numeric session function.
    Num(String),
    /// A boolean.
    Bool(bool),
}

fn session(s: &str) -> Option<Val> {
    let rest = s.strip_prefix("$$")?;
    Some(match rest {
        "USER" => Val::Str("CURRENT_USER()".into()),
        "USER_ID" => Val::Num("SPRIG_USER_ID()".into()),
        "TENANT_ID" => Val::Num("SPRIG_TENANT_ID()".into()),
        "HOME_TENANT_ID" => Val::Num("SPRIG_HOME_TENANT_ID()".into()),
        "SESSION_SPACE" => Val::Num("SPRIG_SESSION_SPACE()".into()),
        _ => {
            if let Some(k) = rest.strip_prefix("CLAIM.") {
                Val::Str(format!("SPRIG_CLAIM({})", str_lit(k)))
            } else if let Some(k) = rest.strip_prefix("ATTR.") {
                Val::Str(format!("SPRIG_USER_ATTR({})", str_lit(k)))
            } else {
                return None;
            }
        }
    })
}

fn value(v: &Bson, u: Use) -> Result<Val, MongoError> {
    Ok(match v {
        Bson::String(s) if s.starts_with("$$") && u == Use::Policy => {
            session(s).ok_or_else(|| invalid(format!("unknown session value '{s}'")))?
        }
        Bson::String(s) => Val::Str(str_lit(s)),
        Bson::Int32(i) => Val::Num(i.to_string()),
        Bson::Int64(i) => Val::Num(i.to_string()),
        Bson::Double(f) if f.is_finite() => Val::Num(crate::sql::f64_lit(*f)),
        Bson::Boolean(b) => Val::Bool(*b),
        Bson::Null => {
            return Err(u.err(
                false,
                "null in a policy filter (missing fields can't be told from fields under arrays)",
            ));
        }
        other => {
            return Err(u.err(
                false,
                format!(
                    "a {:?} value in a policy filter (strings, numbers, booleans and session values only)",
                    other.element_type()
                ),
            ));
        }
    })
}

/// SQL pieces for one field path.
struct Path {
    /// `JSON_EXTRACT(doc, '$."a"."b"')`.
    extract: String,
    /// `JSON_VALUE(doc, ...)`.
    value: String,
    /// `JSON_VALUE(_types, '$."a.b"')`.
    typed: String,
    /// The `_types` key prefix of elements under the path (`a.b.`), as a
    /// `LIKE` pattern.
    elements: String,
    /// `JSON_EXTRACT` of each proper prefix (`a` of `a.b`).
    prefixes: Vec<String>,
}

impl Path {
    fn new(path: &str, u: Use) -> Result<Self, MongoError> {
        if !valid_path(path) {
            return Err(u.err(false, format!("field path '{path}'")));
        }
        let parts: Vec<&str> = path.split('.').collect();
        let p = str_lit(&json_path(&parts, &[]));
        let t = str_lit(&types_path(path));
        let key: Vec<String> = path.split('.').map(crate::json::escape_name).collect();
        let like = key
            .join(".")
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let prefixes = (1..parts.len())
            .map(|n| {
                format!(
                    "JSON_EXTRACT(doc, {})",
                    str_lit(&json_path(&parts[..n], &[]))
                )
            })
            .collect();
        Ok(Self {
            extract: format!("JSON_EXTRACT(doc, {p})"),
            value: format!("JSON_VALUE(doc, {p})"),
            typed: format!("JSON_VALUE(_types, {t})"),
            elements: str_lit(&format!("{like}.%")),
            prefixes,
        })
    }

    /// Nothing at the path, and no array on the way to it: MongoDB's
    /// "missing" for certain (under an array, a path may still match
    /// elements' fields).
    fn missing(&self) -> String {
        let mut s = format!("({} IS NULL", self.extract);
        for p in &self.prefixes {
            let _ = write!(s, " AND COALESCE(JSON_TYPE({p}), '') <> 'ARRAY'");
        }
        s.push(')');
        s
    }

    fn jtype(&self) -> String {
        format!("JSON_TYPE({})", self.extract)
    }

    /// The value at the path is a plain string, number or boolean of
    /// `v`'s kind (no `_types` entry beyond numbers' own).
    fn same_kind(&self, v: &Val) -> String {
        match v {
            Val::Str(_) => format!("{} = 'STRING' AND {} IS NULL", self.jtype(), self.typed),
            Val::Num(_) => format!(
                "{} IN ('INTEGER', 'DOUBLE') AND COALESCE({}, 'long') IN ('long', 'double', 'decimal')",
                self.jtype(),
                self.typed
            ),
            Val::Bool(_) => format!("{} = 'BOOLEAN'", self.jtype()),
        }
    }

    fn compare(&self, op: &str, v: &Val) -> String {
        match v {
            Val::Str(s) => format!("{} COLLATE utf8mb4_bin {op} CAST({s} AS CHAR)", self.value),
            Val::Num(n) => format!("CAST({} AS DECIMAL(65, 30)) {op} {n}", self.value),
            // MariaDB 11.8's `JSON_VALUE` reads `true` as `1` (other
            // versions as `true`); only `=` reaches here for booleans.
            Val::Bool(b) => {
                debug_assert_eq!(op, "=");
                format!("{} IN ({})", self.value, crate::sql::json_bool_texts(*b))
            }
        }
    }

    /// An array at the path whose elements are plain JSON contains `v`.
    fn array_has(&self, v: &Val) -> String {
        let probe = match v {
            Val::Str(s) => format!("JSON_QUOTE({s})"),
            Val::Num(n) => format!("CAST({n} AS CHAR)"),
            Val::Bool(b) => format!("'{b}'"),
        };
        format!(
            "({} = 'ARRAY' AND (_types IS NULL OR JSON_SEARCH(JSON_KEYS(_types), 'one', {}) IS NULL) \
             AND JSON_CONTAINS({}, {probe}))",
            self.jtype(),
            self.elements,
            self.extract
        )
    }

    fn eq(&self, v: &Val) -> String {
        let scalar = format!("({} AND {})", self.same_kind(v), self.compare("=", v));
        format!("({scalar} OR {})", self.array_has(v))
    }

    /// Missing (for certain), null, or a plain scalar other than `v`
    /// (narrower than `$ne`: arrays and typed values stay hidden).
    fn ne(&self, v: &Val) -> String {
        let plain = format!(
            "{} IN ('STRING', 'INTEGER', 'DOUBLE', 'BOOLEAN', 'NULL') AND (_types IS NULL OR {} IS NULL)",
            self.jtype(),
            self.typed
        );
        format!(
            "(({plain} AND NOT ({} AND {})) OR {})",
            self.same_kind(v),
            self.compare("=", v),
            self.missing()
        )
    }

    /// `$type` with one alias or number, for a partial filter: the value
    /// at the path itself has the type (an array's elements aren't
    /// looked at, so this is narrower than MongoDB for arrays).
    fn type_is(&self, t: &Bson) -> Result<String, MongoError> {
        let jt = self.jtype();
        let ty = format!("SUBSTRING_INDEX({}, ':', 1)", self.typed);
        let typed = |names: &str| format!("({ty} IN ({names}))");
        let plain = |json: &str| format!("({jt} IN ({json}) AND {} IS NULL)", self.typed);
        let int32 = format!(
            "CAST({} AS DECIMAL(65, 0)) BETWEEN -2147483648 AND 2147483647",
            self.value
        );
        let name = match t {
            Bson::String(s) => s.clone(),
            other => match crate::filter::as_i64(other) {
                Some(1) => "double".into(),
                Some(2) => "string".into(),
                Some(3) => "object".into(),
                Some(4) => "array".into(),
                Some(5) => "binData".into(),
                Some(7) => "objectId".into(),
                Some(8) => "bool".into(),
                Some(9) => "date".into(),
                Some(10) => "null".into(),
                Some(11) => "regex".into(),
                Some(13) => "javascript".into(),
                Some(14) => "symbol".into(),
                Some(16) => "int".into(),
                Some(17) => "timestamp".into(),
                Some(18) => "long".into(),
                Some(19) => "decimal".into(),
                Some(-1) => "minKey".into(),
                Some(127) => "maxKey".into(),
                _ => {
                    return Err(Use::Partial.err(false, format!("$type {other}")));
                }
            },
        };
        Ok(match name.as_str() {
            "string" => plain("'STRING'"),
            "bool" => format!("({jt} = 'BOOLEAN')"),
            "null" => plain("'NULL'"),
            "object" => plain("'OBJECT'"),
            "array" => format!("({jt} = 'ARRAY')"),
            "int" => format!("({} AND {int32})", plain("'INTEGER'")),
            "long" => format!(
                "({} OR ({} AND NOT ({int32})))",
                typed("'long'"),
                plain("'INTEGER'")
            ),
            "double" => format!("({} OR {})", plain("'DOUBLE'"), typed("'double'")),
            "decimal" => typed("'decimal'"),
            "number" => format!(
                "({jt} IN ('INTEGER', 'DOUBLE') AND ({} IS NULL OR {ty} IN ('long', 'double', 'decimal')))",
                self.typed
            ),
            "binData" => typed("'binData', 'uuid'"),
            "objectId"
            | "date"
            | "regex"
            | "javascript"
            | "javascriptWithScope"
            | "symbol"
            | "timestamp"
            | "minKey"
            | "maxKey"
            | "dbPointer"
            | "undefined" => typed(&str_lit(&name)),
            other => {
                return Err(MongoError::new(
                    sprig_proto_mongo::errors::code::BAD_VALUE,
                    format!("Unknown type name alias: {other}"),
                ));
            }
        })
    }
}

fn field(path: &str, cond: &Bson, u: Use) -> Result<String, MongoError> {
    let p = Path::new(path, u)?;
    let ops = match cond {
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => d,
        v => return Ok(p.eq(&value(v, u)?)),
    };
    let mut out = Vec::new();
    for (op, v) in ops {
        if u == Use::Partial && matches!(op.as_str(), "$ne" | "$nin") {
            return Err(u.err(true, op));
        }
        out.push(match op.as_str() {
            "$eq" => p.eq(&value(v, u)?),
            "$ne" => p.ne(&value(v, u)?),
            "$type" if u == Use::Partial => match v {
                Bson::Array(a) if !a.is_empty() => {
                    let parts = a
                        .iter()
                        .map(|t| p.type_is(t))
                        .collect::<Result<Vec<_>, MongoError>>()?;
                    format!("({})", parts.join(" OR "))
                }
                Bson::Array(_) => {
                    return Err(MongoError::new(
                        sprig_proto_mongo::errors::code::FAILED_TO_PARSE,
                        format!("{path} must match at least one type"),
                    ));
                }
                t => p.type_is(t)?,
            },
            "$gt" | "$gte" | "$lt" | "$lte" => {
                let v = value(v, u)?;
                if matches!(v, Val::Bool(_)) {
                    return Err(u.err(false, format!("{op} on a boolean")));
                }
                let sql = match op.as_str() {
                    "$gt" => ">",
                    "$gte" => ">=",
                    "$lt" => "<",
                    _ => "<=",
                };
                format!("({} AND {})", p.same_kind(&v), p.compare(sql, &v))
            }
            "$in" | "$nin" => {
                let Bson::Array(a) = v else {
                    return Err(u.err(false, format!("{op} needs an array")));
                };
                if a.is_empty() {
                    if op == "$in" {
                        "FALSE".to_owned()
                    } else {
                        return Err(invalid("$nin with an empty array"));
                    }
                } else {
                    let parts = a
                        .iter()
                        .map(|x| {
                            let x = value(x, u)?;
                            Ok(if op == "$in" { p.eq(&x) } else { p.ne(&x) })
                        })
                        .collect::<Result<Vec<_>, MongoError>>()?;
                    let join = if op == "$in" { " OR " } else { " AND " };
                    format!("({})", parts.join(join))
                }
            }
            "$exists" => match v {
                Bson::Boolean(true) | Bson::Int32(1) | Bson::Int64(1) => {
                    format!("({} IS NOT NULL)", p.extract)
                }
                _ if u == Use::Partial => return Err(u.err(true, "$exists: false")),
                _ => {
                    return Err(invalid(
                        "$exists: false (a field under an array can't be told from a missing one)",
                    ));
                }
            },
            other if u == Use::Partial => return Err(u.err(true, other)),
            other => return Err(invalid(format!("{other} in a policy filter"))),
        });
    }
    if out.is_empty() {
        return Err(u.err(false, format!("an empty condition on '{path}'")));
    }
    Ok(format!("({})", out.join(" AND ")))
}

/// The SQL expression of policy filter `filter` over a collection's table
/// (see the module docs). An empty filter is `TRUE`.
///
/// # Errors
/// 39364 for what a policy filter can't hold.
pub fn to_sql(filter: &Document) -> Result<String, MongoError> {
    filter_sql(filter, Use::Policy)
}

/// The SQL a stored MongoDB form (`using_mongo`, `check_mongo`: the
/// filter as relaxed extended JSON) compiles to with this translator.
/// `None` when the text isn't a filter document or the filter is one a
/// policy can't hold; the caller then hides every row.
///
/// The catalog keeps the SQL a policy was created with. Readers compile
/// the stored filter again with this function, so a fix to the
/// translator reaches policies created before it.
#[must_use]
pub fn stored_sql(mongo_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(mongo_json).ok()?;
    match Bson::try_from(v).ok()? {
        Bson::Document(d) => to_sql(&d).ok(),
        _ => None,
    }
}

/// The SQL expression of an index's `partialFilterExpression` over a
/// collection's table: true for the documents the filter matches. It
/// holds no session function, so a generated column may use it. It is
/// exact for strings, numbers and booleans compared with plain values,
/// `$exists: true` and `$type`; where a path crosses an array it is
/// narrower than MongoDB (see the module docs).
///
/// # Errors
/// 67 for what MongoDB refuses in a partial filter; 39662 for what this
/// release can't express (null, ObjectId, date and other typed values).
pub fn partial_to_sql(filter: &Document) -> Result<String, MongoError> {
    filter_sql(filter, Use::Partial)
}

fn filter_sql(filter: &Document, u: Use) -> Result<String, MongoError> {
    let mut out = Vec::new();
    for (k, v) in filter {
        out.push(match k.as_str() {
            "$and" | "$or" => {
                let Bson::Array(a) = v else {
                    return Err(u.err(false, format!("{k} needs an array")));
                };
                if a.is_empty() {
                    return Err(u.err(false, format!("{k} with an empty array")));
                }
                let parts = a
                    .iter()
                    .map(|x| match x {
                        Bson::Document(d) => filter_sql(d, u),
                        _ => Err(u.err(false, format!("{k} needs documents"))),
                    })
                    .collect::<Result<Vec<_>, MongoError>>()?;
                let join = if k == "$and" { " AND " } else { " OR " };
                format!("({})", parts.join(join))
            }
            other if other.starts_with('$') && u == Use::Partial => {
                return Err(u.err(true, other));
            }
            other if other.starts_with('$') => {
                return Err(invalid(format!("{other} in a policy filter")));
            }
            path => field(path, v, u)?,
        });
    }
    if out.is_empty() {
        return Ok("TRUE".to_owned());
    }
    Ok(out.join(" AND "))
}

/// Policy filter `filter` with its session values bound, for the
/// MongoDB matcher (change streams): `"$$USER"` and the others become the
/// session's values that `lookup` answers. `None` when one of them has no
/// value (`NULL` in SQL): the policy then shows nothing, which is never
/// wider than the SQL form.
#[must_use]
pub fn bind(filter: &Document, lookup: &dyn Fn(&str) -> Option<Bson>) -> Option<Document> {
    fn walk(v: &Bson, lookup: &dyn Fn(&str) -> Option<Bson>) -> Option<Bson> {
        Some(match v {
            Bson::String(s) if s.starts_with("$$") => lookup(&s[2..])?,
            Bson::Document(d) => Bson::Document(
                d.iter()
                    .map(|(k, x)| Some((k.clone(), walk(x, lookup)?)))
                    .collect::<Option<Document>>()?,
            ),
            Bson::Array(a) => Bson::Array(
                a.iter()
                    .map(|x| walk(x, lookup))
                    .collect::<Option<Vec<_>>>()?,
            ),
            other => other.clone(),
        })
    }
    filter
        .iter()
        .map(|(k, x)| Some((k.clone(), walk(x, lookup)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn stored_filters_compile_again() {
        // As `sprigCreatePolicy` stores it (relaxed extended JSON).
        let stored = Bson::Document(doc! {"deleted": {"$ne": true}, "n": {"$gte": 2_i64}})
            .into_relaxed_extjson()
            .to_string();
        assert_eq!(
            stored_sql(&stored),
            Some(to_sql(&doc! {"deleted": {"$ne": true}, "n": {"$gte": 2_i64}}).unwrap())
        );
        // What a policy can't hold, or not a filter document: nothing.
        assert_eq!(stored_sql(r#"{"a": {"$where": "1"}}"#), None);
        assert_eq!(stored_sql("[1]"), None);
        assert_eq!(stored_sql("not json"), None);
    }

    #[test]
    fn filters_become_sql() {
        let s = to_sql(&doc! {"owner": "$$USER"}).unwrap();
        assert!(
            s.contains("JSON_TYPE(JSON_EXTRACT(doc, '$.\"owner\"')) = 'STRING'"),
            "{s}"
        );
        assert!(
            s.contains("COLLATE utf8mb4_bin = CAST(CURRENT_USER() AS CHAR)"),
            "{s}"
        );
        assert!(
            s.contains(
                "JSON_CONTAINS(JSON_EXTRACT(doc, '$.\"owner\"'), JSON_QUOTE(CURRENT_USER()))"
            ),
            "{s}"
        );
        let s = to_sql(
            &doc! {"$or": [{"team": {"$in": [1, 2]}}, {"public": true}], "level": {"$lt": 5}},
        )
        .unwrap();
        assert!(
            s.starts_with("(((") && s.contains(" OR ") && !s.contains("<= 5"),
            "{s}"
        );
        assert!(
            s.contains("CAST(JSON_VALUE(doc, '$.\"level\"') AS DECIMAL(65, 30)) < 5"),
            "{s}"
        );
        let s = to_sql(&doc! {"tenant": "$$CLAIM.org", "n": {"$ne": "x"}}).unwrap();
        assert!(
            s.contains("SPRIG_CLAIM('org')") && s.contains("NOT ("),
            "{s}"
        );
        assert_eq!(to_sql(&doc! {}).unwrap(), "TRUE");
        for bad in [
            doc! {"a": null},
            doc! {"a": {"$exists": false}},
            doc! {"$nor": [{"a": 1}]},
            doc! {"a": {"$regex": "x"}},
            doc! {"a": "$$NOPE"},
            doc! {"$where": "1"},
            doc! {"a": {"$gt": true}},
            doc! {"a": bson::oid::ObjectId::new()},
        ] {
            assert_eq!(to_sql(&bad).unwrap_err().code, POLICY_INVALID, "{bad}");
        }
    }

    #[test]
    fn booleans_compare_both_texts() {
        // MariaDB 11.8's `JSON_VALUE` answers `1` for `true`: a policy
        // comparing with `'true'` alone showed `{deleted: true}` rows to
        // `{deleted: {$ne: true}}` and hid every row from `{active: true}`.
        let s = to_sql(&doc! {"deleted": {"$ne": true}}).unwrap();
        assert!(
            s.contains("JSON_VALUE(doc, '$.\"deleted\"') IN ('1', 'true')"),
            "{s}"
        );
        assert!(!s.contains("= 'true'"), "{s}");
        let s = to_sql(&doc! {"active": false}).unwrap();
        assert!(s.contains("IN ('0', 'false')"), "{s}");
        let s = to_sql(&doc! {"flags.hidden": {"$in": [true]}}).unwrap();
        assert!(
            s.contains("JSON_VALUE(doc, '$.\"flags\".\"hidden\"') IN ('1', 'true')"),
            "{s}"
        );
    }

    #[test]
    fn ne_shows_missing_fields_unless_an_array_is_on_the_path() {
        let s = to_sql(&doc! {"deleted": {"$ne": true}}).unwrap();
        assert!(
            s.contains("OR (JSON_EXTRACT(doc, '$.\"deleted\"') IS NULL)"),
            "{s}"
        );
        assert!(s.contains("'BOOLEAN', 'NULL')"), "{s}");
        let s = to_sql(&doc! {"a.b": {"$nin": [1, 2]}}).unwrap();
        assert!(
            s.contains(
                "(JSON_EXTRACT(doc, '$.\"a\".\"b\"') IS NULL AND COALESCE(JSON_TYPE(JSON_EXTRACT(doc, '$.\"a\"')), '') <> 'ARRAY')"
            ),
            "{s}"
        );
    }

    #[test]
    fn partial_filters_become_sql() {
        let s = partial_to_sql(&doc! {"kind": "Device"}).unwrap();
        assert!(s.contains("CAST('Device' AS CHAR)"), "{s}");
        // No session values in a partial filter: "$$USER" is a string.
        let s = partial_to_sql(&doc! {"owner": "$$USER"}).unwrap();
        assert!(s.contains("'$$USER'") && !s.contains("CURRENT_USER"), "{s}");
        let s = partial_to_sql(
            &doc! {"email": {"$exists": true, "$type": "string"}, "n": {"$gte": 5}, "$or": [{"a": true}, {"b": {"$in": ["x", 2]}}]},
        )
        .unwrap();
        assert!(s.contains("IS NOT NULL") && s.contains(">= 5"), "{s}");
        let s = partial_to_sql(&doc! {"x": {"$type": ["int", 18, "number"]}}).unwrap();
        assert!(s.contains("BETWEEN -2147483648 AND 2147483647"), "{s}");
        assert!(s.contains("IN ('long')"), "{s}");
        assert_eq!(partial_to_sql(&doc! {}).unwrap(), "TRUE");
        for bad in [
            doc! {"a": {"$ne": 1}},
            doc! {"a": {"$nin": [1]}},
            doc! {"a": {"$exists": false}},
            doc! {"a": {"$regex": "x"}},
            doc! {"a": {"$not": {"$eq": 1}}},
            doc! {"$nor": [{"a": 1}]},
        ] {
            let e = partial_to_sql(&bad).unwrap_err();
            assert_eq!(e.code, 67, "{bad}");
            assert!(
                e.message
                    .starts_with("Expression not supported in partial index"),
                "{e:?}"
            );
        }
        for bad in [doc! {"a": null}, doc! {"a": bson::oid::ObjectId::new()}] {
            assert_eq!(
                partial_to_sql(&bad).unwrap_err().code,
                sprig_proto_mongo::errors::code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                "{bad}"
            );
        }
        assert_eq!(
            partial_to_sql(&doc! {"a": {"$type": "nope"}})
                .unwrap_err()
                .code,
            sprig_proto_mongo::errors::code::BAD_VALUE
        );
    }

    #[test]
    fn session_values_bind_for_the_matcher() {
        let f = doc! {"owner": "$$USER", "n": {"$in": ["$$CLAIM.x", 3]}};
        let b = bind(&f, &|k| match k {
            "USER" => Some(Bson::String("a@%".into())),
            "CLAIM.x" => Some(Bson::Int32(9)),
            _ => None,
        });
        assert_eq!(b, Some(doc! {"owner": "a@%", "n": {"$in": [9, 3]}}));
        assert_eq!(bind(&f, &|_| None), None);
    }
}
