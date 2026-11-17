//! Values between an element's stored form (`props` and `_types`, as the
//! executor has them) and typed columns.
//!
//! - A scalar goes to its key's column of its value type
//!   ([`super::Code`]); a list goes to the values table, one row per item
//!   (an empty list as one row with `ord = u32::MAX`); a scalar whose
//!   column the table lacks goes there too (`ord = u32::MAX - 1`).
//! - Temporal values keep their stored text; bytes are raw bytes; points
//!   are `srid;x;y[;z]`.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use mysql_async::Value as MyValue;
use serde_json::{Map, Value as Json};

use super::{Code, TypedKind};

/// `ord` of an empty list's row.
pub const EMPTY_LIST: u32 = u32::MAX;
/// `ord` of a scalar kept outside the columns.
pub const SCALAR: u32 = u32::MAX - 1;

/// A typed value.
#[derive(Debug, Clone, PartialEq)]
pub enum Val {
    /// Integer.
    Int(i64),
    /// Float.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// Text (strings, temporal texts, points, non-finite floats).
    Text(String),
    /// Bytes.
    Bytes(Vec<u8>),
}

impl Val {
    /// As a statement parameter.
    #[must_use]
    pub fn my(&self) -> MyValue {
        match self {
            Self::Int(i) => MyValue::Int(*i),
            Self::Float(f) => MyValue::Double(*f),
            Self::Bool(b) => MyValue::Int(i64::from(*b)),
            Self::Text(s) => MyValue::Bytes(s.clone().into_bytes()),
            Self::Bytes(b) => MyValue::Bytes(b.clone()),
        }
    }
}

/// One row of the values table.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemRow {
    /// Property key.
    pub key: String,
    /// Position in the list, or [`EMPTY_LIST`] / [`SCALAR`].
    pub ord: u32,
    /// The value's type (`None` for an empty list's row).
    pub code: Option<Code>,
    /// The value (`None` for an empty list's row).
    pub val: Option<Val>,
}

/// An element's properties split for its table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Encoded {
    /// Scalars by (key, code).
    pub scalars: BTreeMap<(String, Code), Val>,
    /// Lists and scalars kept aside.
    pub items: Vec<ItemRow>,
}

/// The type and value of a stored scalar at `path` (`None` for `null`
/// and for maps, which properties can't hold).
#[must_use]
pub fn scalar(j: &Json, tag: Option<&str>) -> Option<(Code, Val)> {
    if let Some(kind) = tag.and_then(TypedKind::of) {
        return Some((Code::Typed(kind), typed_val(kind, j)?));
    }
    let float = tag == Some("float");
    Some(match j {
        Json::Bool(b) => (Code::Bool, Val::Bool(*b)),
        Json::Number(n) => match (n.as_i64(), float) {
            (Some(i), false) => (Code::Int, Val::Int(i)),
            _ => (Code::Float, Val::Float(n.as_f64()?)),
        },
        Json::String(s) if float => (Code::FloatX, Val::Text(s.clone())),
        Json::String(s) => (Code::Str, Val::Text(s.clone())),
        _ => return None,
    })
}

fn typed_val(kind: TypedKind, j: &Json) -> Option<Val> {
    match (kind, j) {
        (TypedKind::Bytes, Json::String(s)) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .ok()
            .map(Val::Bytes),
        (TypedKind::Point, Json::Object(o)) => {
            let f = |k: &str| o.get(k).and_then(Json::as_f64);
            let mut t = format!("{};{};{}", o.get("srid")?.as_u64()?, f("x")?, f("y")?);
            if let Some(z) = f("z") {
                t.push(';');
                t.push_str(&z.to_string());
            }
            Some(Val::Text(t))
        }
        (_, Json::String(s)) => Some(Val::Text(s.clone())),
        _ => None,
    }
}

/// The stored JSON (and whether it needs a `_types` entry) of a typed
/// value read back.
#[must_use]
pub fn unscalar(code: Code, v: &Val) -> Option<(Json, Option<&'static str>)> {
    Some(match (code, v) {
        (Code::Int, Val::Int(i)) => (Json::from(*i), None),
        (Code::Float, Val::Float(f)) => {
            let n = serde_json::Number::from_f64(*f)?;
            // Integral floats need the `float` entry to stay floats.
            (Json::Number(n), (f.fract() == 0.0).then_some("float"))
        }
        (Code::FloatX, Val::Text(s)) => (Json::String(s.clone()), Some("float")),
        (Code::Bool, Val::Bool(b)) => (Json::Bool(*b), None),
        (Code::Bool, Val::Int(i)) => (Json::Bool(*i != 0), None),
        (Code::Str, Val::Text(s)) => (Json::String(s.clone()), None),
        (Code::Typed(TypedKind::Bytes), Val::Bytes(b)) => (
            Json::String(base64::engine::general_purpose::STANDARD.encode(b)),
            Some("bytes"),
        ),
        (Code::Typed(TypedKind::Point), Val::Text(s)) => {
            let p: Vec<&str> = s.split(';').collect();
            let mut m = Map::new();
            m.insert("srid".into(), Json::from(p.first()?.parse::<u64>().ok()?));
            m.insert(
                "x".into(),
                serde_json::json!(p.get(1)?.parse::<f64>().ok()?),
            );
            m.insert(
                "y".into(),
                serde_json::json!(p.get(2)?.parse::<f64>().ok()?),
            );
            if let Some(z) = p.get(3) {
                m.insert("z".into(), serde_json::json!(z.parse::<f64>().ok()?));
            }
            (Json::Object(m), Some("point"))
        }
        (Code::Typed(k), Val::Text(s)) => (Json::String(s.clone()), Some(k.name())),
        _ => return None,
    })
}

/// Splits an element's `props` (and `_types`) for a table whose columns
/// are `have`: scalars with a column there go to [`Encoded::scalars`] when
/// `fits` says so (a column the table has, or one that may still be
/// added), the rest to [`Encoded::items`].
#[must_use]
pub fn encode(props: &Json, types: Option<&Json>, fits: impl Fn(&str, Code) -> bool) -> Encoded {
    let mut out = Encoded::default();
    let Json::Object(m) = props else {
        return out;
    };
    let tag = |path: &str| {
        types
            .and_then(|t| t.get(path))
            .and_then(Json::as_str)
            .map(str::to_owned)
    };
    for (k, v) in m {
        match v {
            Json::Null => {}
            Json::Array(a) => {
                if a.is_empty() {
                    out.items.push(ItemRow {
                        key: k.clone(),
                        ord: EMPTY_LIST,
                        code: None,
                        val: None,
                    });
                }
                for (i, x) in a.iter().enumerate() {
                    let t = tag(&format!("{k}[{i}]"));
                    if let Some((code, val)) = scalar(x, t.as_deref()) {
                        out.items.push(ItemRow {
                            key: k.clone(),
                            ord: u32::try_from(i).unwrap_or(SCALAR - 1),
                            code: Some(code),
                            val: Some(val),
                        });
                    }
                }
            }
            _ => {
                let t = tag(k);
                if let Some((code, val)) = scalar(v, t.as_deref()) {
                    if fits(k, code) {
                        out.scalars.insert((k.clone(), code), val);
                    } else {
                        out.items.push(ItemRow {
                            key: k.clone(),
                            ord: SCALAR,
                            code: Some(code),
                            val: Some(val),
                        });
                    }
                }
            }
        }
    }
    out
}

/// Every (key, code) an element's scalars need, and whether it has lists.
#[must_use]
pub fn needs(props: &Json, types: Option<&Json>) -> (BTreeSet<(String, Code)>, bool) {
    let e = encode(props, types, |_, _| true);
    let lists = !e.items.is_empty();
    (e.scalars.into_keys().collect(), lists)
}

/// Rebuilds `props` and `_types` from the scalars read and the values
/// table's rows of one element.
#[must_use]
pub fn decode(scalars: Vec<(String, Code, Val)>, items: Vec<ItemRow>) -> (Json, Option<Json>) {
    let mut props = Map::new();
    let mut types = Map::new();
    for (k, code, v) in scalars {
        if let Some((j, t)) = unscalar(code, &v) {
            if let Some(t) = t {
                types.insert(k.clone(), Json::from(t));
            }
            props.insert(k, j);
        }
    }
    let mut lists: BTreeMap<String, Vec<ListItem>> = BTreeMap::new();
    for it in items {
        if it.ord == SCALAR {
            if let (Some(c), Some(v)) = (it.code, &it.val)
                && let Some((j, t)) = unscalar(c, v)
            {
                if let Some(t) = t {
                    types.insert(it.key.clone(), Json::from(t));
                }
                props.insert(it.key, j);
            }
            continue;
        }
        let e = lists.entry(it.key).or_default();
        if it.ord != EMPTY_LIST
            && let (Some(c), Some(v)) = (it.code, &it.val)
        {
            e.push((it.ord, unscalar(c, v)));
        }
    }
    for (k, mut v) in lists {
        v.sort_by_key(|x| x.0);
        let mut a = Vec::with_capacity(v.len());
        for (i, (_, x)) in v.into_iter().enumerate() {
            let (j, t) = x.unwrap_or((Json::Null, None));
            if let Some(t) = t {
                types.insert(format!("{k}[{i}]"), Json::from(t));
            }
            a.push(j);
        }
        props.insert(k, Json::Array(a));
    }
    let t = (!types.is_empty()).then_some(Json::Object(types));
    (Json::Object(props), t)
}

/// A list item while decoding: its position, its value and type name.
type ListItem = (u32, Option<(Json, Option<&'static str>)>);

/// A column value as read (`None`: `NULL`).
#[must_use]
pub fn read_val(code: Code, v: MyValue) -> Option<Val> {
    match v {
        MyValue::Int(i) => Some(if code == Code::Bool {
            Val::Bool(i != 0)
        } else {
            Val::Int(i)
        }),
        MyValue::UInt(u) => Some(Val::Int(i64::try_from(u).ok()?)),
        MyValue::Double(f) => Some(Val::Float(f)),
        MyValue::Float(f) => Some(Val::Float(f64::from(f))),
        MyValue::Bytes(b) => Some(match code {
            Code::Int => Val::Int(std::str::from_utf8(&b).ok()?.parse().ok()?),
            Code::Float => Val::Float(std::str::from_utf8(&b).ok()?.parse().ok()?),
            Code::Bool => Val::Bool(std::str::from_utf8(&b).ok()? != "0"),
            Code::Typed(TypedKind::Bytes) => Val::Bytes(b),
            _ => Val::Text(String::from_utf8(b).ok()?),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip() {
        let props = serde_json::json!({
            "i": 7, "f": 2.5, "g": 3.0, "b": true, "s": "x",
            "d": "2020-01-02", "by": "AAEC", "pt": {"srid": 7203, "x": 1.0, "y": 2.5},
            "l": ["a", "b"], "e": [], "dl": ["2020-01-01", "2021-02-03"],
        });
        let types = serde_json::json!({
            "g": "float", "d": "date", "by": "bytes", "pt": "point",
            "dl[0]": "date", "dl[1]": "date",
        });
        // `s` has no column: it goes aside.
        let e = encode(&props, Some(&types), |k, _| k != "s");
        assert_eq!(e.scalars.len(), 7);
        assert!(e.items.iter().any(|r| r.key == "s" && r.ord == SCALAR));
        assert!(e.items.iter().any(|r| r.key == "e" && r.ord == EMPTY_LIST));
        let sc: Vec<(String, Code, Val)> = e
            .scalars
            .iter()
            .map(|((k, c), v)| (k.clone(), *c, v.clone()))
            .collect();
        let (p, t) = decode(sc, e.items);
        assert_eq!(p, props);
        assert_eq!(t, Some(types));
        let (n, lists) = needs(&props, None);
        assert!(lists && n.contains(&("i".to_owned(), Code::Int)));
    }

    #[test]
    fn numbers_keep_their_kind() {
        assert_eq!(
            scalar(&serde_json::json!(1), None),
            Some((Code::Int, Val::Int(1)))
        );
        assert_eq!(
            scalar(&serde_json::json!(1.0), Some("float")),
            Some((Code::Float, Val::Float(1.0)))
        );
        assert_eq!(
            unscalar(Code::Float, &Val::Float(1.0)),
            Some((serde_json::json!(1.0), Some("float")))
        );
        assert_eq!(read_val(Code::Bool, MyValue::Int(1)), Some(Val::Bool(true)));
        assert_eq!(
            read_val(Code::Int, MyValue::Bytes(b"42".to_vec())),
            Some(Val::Int(42))
        );
    }
}
