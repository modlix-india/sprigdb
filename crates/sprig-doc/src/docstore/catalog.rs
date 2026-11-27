//! Collection records: a collection's number, options and indexes, kept
//! in the tenant's catalog object as one BSON document per collection.

use bson::spec::BinarySubtype;
use bson::{Binary, Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::collation::Collation;
use crate::filter::{self, Pred};

/// How one field of an index orders its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
    /// Hashed.
    Hashed,
    /// Wildcard (`$**` or `path.$**`).
    Wildcard,
}

/// One index of a collection.
#[allow(clippy::struct_excessive_bools)] // unique, sparse, hidden, building: options, not states
#[derive(Debug, Clone)]
pub struct IndexMeta {
    /// The key index number (never reused in the collection).
    pub id: u16,
    /// The name.
    pub name: String,
    /// The fields in pattern order.
    pub fields: Vec<(String, FieldKind)>,
    /// Unique.
    pub unique: bool,
    /// Sparse.
    pub sparse: bool,
    /// `partialFilterExpression`, parsed.
    pub partial: Option<Pred>,
    /// `expireAfterSeconds`.
    pub expire_after: Option<i64>,
    /// Hidden from the planner.
    pub hidden: bool,
    /// `wildcardProjection`.
    pub wildcard_projection: Option<Document>,
    /// The specification `listIndexes` shows.
    pub spec: Document,
    /// Being built online: writes keep its entries, but no read uses it
    /// and `listIndexes` doesn't show it yet.
    pub building: bool,
    /// The index's collation (`None`: simple): its string keys are sort
    /// keys under it.
    pub collation: Option<Collation>,
}

impl IndexMeta {
    /// The key pattern.
    #[must_use]
    pub fn key(&self) -> Document {
        self.spec.get_document("key").cloned().unwrap_or_default()
    }

    /// Whether this index has a wildcard field.
    #[must_use]
    pub fn is_wildcard(&self) -> bool {
        self.fields.iter().any(|f| f.1 == FieldKind::Wildcard)
    }

    /// Whether this index has a hashed field.
    #[must_use]
    pub fn is_hashed(&self) -> bool {
        self.fields.iter().any(|f| f.1 == FieldKind::Hashed)
    }

    /// The same index: key pattern, and the options that make two
    /// indexes on one key different indexes (unique, sparse, partial
    /// filter). `hidden` and TTL don't.
    fn same_identity(&self, other: &IndexMeta) -> bool {
        self.fields == other.fields
            && self.unique == other.unique
            && self.sparse == other.sparse
            && self.spec.get(PARTIAL) == other.spec.get(PARTIAL)
            && self.spec.get("collation") == other.spec.get("collation")
    }

    /// Every option the same too.
    fn same_spec(&self, other: &IndexMeta) -> bool {
        self.same_identity(other)
            && self.expire_after == other.expire_after
            && self.wildcard_projection == other.wildcard_projection
            && self.hidden == other.hidden
    }
}

const PARTIAL: &str = "partialFilterExpression";

fn cannot(m: impl Into<String>) -> MongoError {
    MongoError::new(code::CANNOT_CREATE_INDEX, m)
}

/// The default name of an index on `key`: `a_1_b_-1`.
#[must_use]
pub fn default_name(key: &Document) -> String {
    key.iter()
        .map(|(k, v)| {
            let v = match v {
                Bson::Int32(n) => n.to_string(),
                Bson::Int64(n) => n.to_string(),
                Bson::Double(f) if f.fract() == 0.0 && f.abs() < 1e15 => format!("{f:.0}"),
                Bson::Double(f) => f.to_string(),
                Bson::String(s) => s.clone(),
                other => other.to_string(),
            };
            format!("{k}_{v}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

/// Parses and checks one `createIndexes` specification. `id` is the key
/// index number it would get.
///
/// # Errors
/// MongoDB's: 9 for a missing key, 67 for a bad pattern or options; 115 for
/// index kinds the engine doesn't store (text, geo); 2 for a bad
/// collation.
#[allow(clippy::too_many_lines)]
pub fn parse_index(s: &Document, id: u16) -> Result<IndexMeta, MongoError> {
    let key = match s.get("key") {
        Some(Bson::Document(k)) => k.clone(),
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "The field 'key' must be an object",
            ));
        }
        None => {
            return Err(MongoError::new(
                code::FAILED_TO_PARSE,
                "The 'key' field is a required property of an index specification",
            ));
        }
    };
    if key.is_empty() {
        return Err(cannot("Index keys cannot be empty."));
    }
    if key.len() > 32 {
        return Err(cannot(format!(
            "Index key pattern has {} fields; the limit is 32",
            key.len()
        )));
    }
    let mut fields = Vec::with_capacity(key.len());
    for (p, v) in &key {
        let kind = match v {
            Bson::String(t) if t == "hashed" => FieldKind::Hashed,
            Bson::String(t) if matches!(t.as_str(), "text" | "2dsphere" | "2d" | "geoHaystack") => {
                return Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    format!(
                        "'{t}' indexes are not supported on collections stored on the SprigDB engine yet"
                    ),
                ));
            }
            Bson::String(t) => {
                return Err(cannot(format!("Unknown index plugin '{t}'")));
            }
            other => match filter::as_i64(other) {
                _ if p == "$**" || p.ends_with(".$**") => FieldKind::Wildcard,
                Some(n) if n > 0 => FieldKind::Asc,
                Some(n) if n < 0 => FieldKind::Desc,
                _ => match other {
                    Bson::Double(f) if *f > 0.0 => FieldKind::Asc,
                    Bson::Double(f) if *f < 0.0 => FieldKind::Desc,
                    _ => {
                        return Err(cannot(format!(
                            "Values in the index key pattern can't be 0: {key}"
                        )));
                    }
                },
            },
        };
        if p.is_empty() || p.split('.').any(str::is_empty) {
            return Err(cannot(format!(
                "Index keys cannot be an empty field: {key}"
            )));
        }
        if p.split('.').any(|c| c.starts_with('$') && c != "$**") {
            return Err(cannot(format!(
                "Index key contains an illegal field name: field name starts with '$'. :: {key}"
            )));
        }
        fields.push((p.clone(), kind));
    }
    if fields.iter().filter(|f| f.1 == FieldKind::Hashed).count() > 1 {
        return Err(cannot(format!(
            "A maximum of one index field is allowed to be hashed but found 2 for 'key' {key}"
        )));
    }
    let wild = fields.iter().filter(|f| f.1 == FieldKind::Wildcard).count();
    if wild > 1 {
        return Err(cannot("A wildcard index may have only one wildcard field"));
    }
    if wild > 0 && fields.len() > 1 {
        return Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "compound wildcard indexes are not supported on collections stored on the SprigDB engine yet",
        ));
    }
    let name = match s.get("name") {
        Some(Bson::String(n)) => n.clone(),
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "The field 'name' must be a string",
            ));
        }
        None => default_name(&key),
    };
    if name.is_empty() {
        return Err(cannot("index name cannot be empty"));
    }
    let flag = |k: &str| -> Result<bool, MongoError> {
        match s.get(k) {
            None => Ok(false),
            Some(v) => Ok(crate::filter::truthy(v)),
        }
    };
    let unique = flag("unique")?;
    let sparse = flag("sparse")?;
    let hidden = flag("hidden")?;
    let (collation, collation_spec) = match s.get("collation") {
        None => (None, None),
        Some(Bson::Document(c)) => (Collation::parse(c)?, Collation::normalize(c)?),
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "The field 'collation' must be an object",
            ));
        }
    };
    let partial = match s.get(PARTIAL) {
        None => None,
        Some(Bson::Document(f)) => Some(filter::parse(f)?),
        Some(_) => {
            return Err(MongoError::new(
                code::TYPE_MISMATCH,
                "The field 'partialFilterExpression' must be an object",
            ));
        }
    };
    if sparse && partial.is_some() {
        return Err(cannot(
            "cannot mix \"partialFilterExpression\" and \"sparse\" options",
        ));
    }
    let expire_after = match s.get("expireAfterSeconds") {
        None => None,
        Some(v) => {
            let n = match v {
                Bson::Int32(n) => i64::from(*n),
                Bson::Int64(n) => *n,
                #[allow(clippy::cast_possible_truncation)]
                Bson::Double(f) if f.is_finite() => *f as i64,
                _ => {
                    return Err(cannot(
                        "TTL index 'expireAfterSeconds' option must be numeric",
                    ));
                }
            };
            if !(0..=i64::from(i32::MAX)).contains(&n) {
                return Err(cannot(format!(
                    "TTL index 'expireAfterSeconds' option must be within an acceptable range, try a lower number: {n}"
                )));
            }
            if fields.len() > 1 {
                return Err(cannot(
                    "TTL indexes are single-field indexes, compound indexes do not support TTL.",
                ));
            }
            Some(n)
        }
    };
    if unique && (wild > 0 || fields.iter().any(|f| f.1 == FieldKind::Hashed)) {
        return Err(if wild > 0 {
            cannot("Index type 'wildcard' does not support the unique option")
        } else {
            MongoError::new(
                16764,
                "Currently hashed indexes cannot guarantee uniqueness. Use a regular index.",
            )
        });
    }
    let wildcard_projection = match s.get("wildcardProjection") {
        None => None,
        Some(Bson::Document(d)) if wild > 0 => Some(d.clone()),
        Some(_) => {
            return Err(cannot(
                "The field 'wildcardProjection' is only allowed in an index with a wildcard key",
            ));
        }
    };
    // The specification as listIndexes shows it: v, key, name, then the
    // options as sent.
    let mut spec = doc! {"v": 2, "key": key, "name": name.clone()};
    for (k, v) in s {
        if matches!(k.as_str(), "v" | "key" | "name" | "ns" | "commitQuorum") {
            continue;
        }
        // MongoDB 8.0 leaves out `unique: false` and `hidden: false`, and
        // keeps `sparse: false` and `background` (as booleans).
        if matches!(k.as_str(), "unique" | "hidden") && !crate::filter::truthy(v) {
            continue;
        }
        // A collation is shown with every field; the simple one not at all.
        if k == "collation" {
            if let Some(c) = &collation_spec {
                spec.insert("collation", c.clone());
            }
            continue;
        }
        let v = match k.as_str() {
            "unique" | "sparse" | "hidden" | "background" => {
                Bson::Boolean(crate::filter::truthy(v))
            }
            "expireAfterSeconds" => match v {
                Bson::Double(f) => Bson::Int32(f64_to_i32(*f)),
                Bson::Int64(n) => Bson::Int32(i32::try_from(*n).unwrap_or(i32::MAX)),
                other => other.clone(),
            },
            _ => v.clone(),
        };
        spec.insert(k.clone(), v);
    }
    Ok(IndexMeta {
        id,
        name,
        fields,
        unique,
        sparse,
        partial,
        expire_after,
        hidden,
        wildcard_projection,
        spec,
        building: false,
        collation,
    })
}

/// The default collation of a collection's `create` options.
fn default_collation(options: &Document) -> Option<Collation> {
    options
        .get_document("collation")
        .ok()
        .and_then(|c| Collation::parse(c).ok().flatten())
}

#[allow(clippy::cast_possible_truncation)]
fn f64_to_i32(f: f64) -> i32 {
    f.clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32
}

/// What adding an index to a collection does.
#[derive(Debug)]
pub enum Added {
    /// An identical index exists: nothing to do.
    Exists,
    /// A new index.
    New,
}

/// A collection.
#[derive(Debug, Clone)]
pub struct CollMeta {
    /// The database.
    pub db: String,
    /// The name.
    pub name: String,
    /// The collection number (object `CATALOG + n`).
    pub n: u32,
    /// The UUID.
    pub uuid: [u8; 16],
    /// The `create` options as sent.
    pub options: Document,
    /// The secondary indexes.
    pub indexes: Vec<IndexMeta>,
    /// The next index number.
    pub next_index: u16,
    /// The validator, parsed.
    pub validator: Option<Pred>,
    /// The default collation (`create`'s `collation`): reads and writes
    /// that name none use it, and new indexes that name none get it.
    pub collation: Option<Collation>,
}

impl CollMeta {
    /// A new collection.
    #[must_use]
    pub fn new(db: &str, name: &str, n: u32, options: Document) -> Self {
        // An ObjectId's 12 bytes (unique in the process) and the clock's
        // nanoseconds.
        let mut uuid = [0u8; 16];
        uuid[..12].copy_from_slice(&bson::oid::ObjectId::new().bytes());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        uuid[12..].copy_from_slice(&nanos.to_be_bytes());
        uuid[6] = (uuid[6] & 0x0F) | 0x40;
        uuid[8] = (uuid[8] & 0x3F) | 0x80;
        let validator = options
            .get_document("validator")
            .ok()
            .and_then(|v| filter::parse(v).ok());
        let collation = default_collation(&options);
        Self {
            db: db.to_owned(),
            name: name.to_owned(),
            n,
            uuid,
            options,
            indexes: Vec::new(),
            next_index: 1,
            validator,
            collation,
        }
    }

    /// Index specification `s` with the collection's default collation
    /// added when it names none (`{locale: "simple"}` keeps the simple
    /// one), as MongoDB builds it.
    #[must_use]
    pub fn index_spec(&self, s: &Document) -> Document {
        match self.options.get_document("collation") {
            Ok(c) if !s.contains_key("collation") => {
                let mut s = s.clone();
                s.insert("collation", c.clone());
                s
            }
            _ => s.clone(),
        }
    }

    /// The collation a read or write of this collection uses when it names
    /// none in `body` (its `collation` field).
    #[must_use]
    pub fn collation_for(&self, body: &Document) -> Option<Collation> {
        if body.contains_key("collation") {
            None
        } else {
            self.collation
        }
    }

    /// A view's source collection and pipeline.
    #[must_use]
    pub fn view(&self) -> Option<(String, Vec<Document>)> {
        let on = self.options.get_str("viewOn").ok()?;
        let p = self
            .options
            .get_array("pipeline")
            .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
            .unwrap_or_default();
        Some((on.to_owned(), p))
    }

    /// The UUID as BSON.
    #[must_use]
    pub fn uuid_bson(&self) -> Bson {
        Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: self.uuid.to_vec(),
        })
    }

    /// The record stored in the catalog.
    #[must_use]
    pub fn to_record(&self) -> Document {
        doc! {
            "n": i64::from(self.n),
            "uuid": self.uuid_bson(),
            "options": self.options.clone(),
            "next_index": i32::from(self.next_index),
            "indexes": self.indexes.iter().map(|i| {
                let mut d = doc! {"id": i32::from(i.id), "spec": i.spec.clone()};
                if i.building {
                    d.insert("building", true);
                }
                Bson::Document(d)
            }).collect::<Vec<_>>(),
        }
    }

    /// From a stored record.
    ///
    /// # Errors
    /// The record is damaged.
    pub fn from_record(db: &str, name: &str, r: &Document) -> Result<Self, MongoError> {
        let damaged = || MongoError::new(code::INTERNAL_ERROR, "damaged collection record");
        let n = u32::try_from(r.get_i64("n").map_err(|_| damaged())?).map_err(|_| damaged())?;
        let uuid: [u8; 16] = match r.get("uuid") {
            Some(Bson::Binary(b)) => b.bytes.as_slice().try_into().map_err(|_| damaged())?,
            _ => return Err(damaged()),
        };
        let options = r.get_document("options").cloned().unwrap_or_default();
        let next_index = u16::try_from(r.get_i32("next_index").map_err(|_| damaged())?)
            .map_err(|_| damaged())?;
        let mut indexes = Vec::new();
        for i in r.get_array("indexes").map_err(|_| damaged())? {
            let d = i.as_document().ok_or_else(damaged)?;
            let id =
                u16::try_from(d.get_i32("id").map_err(|_| damaged())?).map_err(|_| damaged())?;
            let spec = d.get_document("spec").map_err(|_| damaged())?;
            let mut m = parse_index(spec, id)?;
            m.spec = spec.clone();
            m.building = d.get_bool("building").unwrap_or(false);
            indexes.push(m);
        }
        let validator = options
            .get_document("validator")
            .ok()
            .and_then(|v| filter::parse(v).ok());
        let collation = default_collation(&options);
        Ok(Self {
            db: db.to_owned(),
            name: name.to_owned(),
            n,
            uuid,
            options,
            indexes,
            next_index,
            validator,
            collation,
        })
    }

    /// Adds index `m` (its number already taken from `next_index`).
    ///
    /// # Errors
    /// 85 for the same key with other options under another name; 86 for
    /// the same name with another key; 67 past 64 indexes.
    pub fn add_index(&mut self, m: IndexMeta) -> Result<Added, MongoError> {
        if m.name == "_id_" {
            if m.fields == [("_id".to_owned(), FieldKind::Asc)] {
                return Ok(Added::Exists);
            }
            return Err(MongoError::new(
                code::INDEX_KEY_SPECS_CONFLICT,
                format!(
                    "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {}, existing index: {{ v: 2, key: {{ _id: 1 }}, name: \"_id_\" }}",
                    m.spec
                ),
            ));
        }
        if m.fields == [("_id".to_owned(), FieldKind::Asc)] && !m.unique && m.partial.is_none() {
            return Ok(Added::Exists);
        }
        if let Some(ex) = self.indexes.iter().find(|i| i.name == m.name) {
            if ex.same_spec(&m) {
                return Ok(Added::Exists);
            }
            return Err(MongoError::new(
                code::INDEX_KEY_SPECS_CONFLICT,
                format!(
                    "An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {}, existing index: {}",
                    m.spec, ex.spec
                ),
            ));
        }
        if let Some(ex) = self.indexes.iter().find(|i| i.same_identity(&m)) {
            return Err(MongoError::new(
                code::INDEX_OPTIONS_CONFLICT,
                format!(
                    "Index already exists with a different name: {}. Requested index: {}",
                    ex.name, m.spec
                ),
            ));
        }
        if self.indexes.len() + 1 >= 64 {
            return Err(cannot(format!(
                "add index fails, too many indexes for {}.{} key:{}",
                self.db,
                self.name,
                m.key()
            )));
        }
        self.indexes.push(m);
        Ok(Added::New)
    }

    /// The `_id_` index as `listIndexes` shows it: with the collection's
    /// default collation, if it has one.
    #[must_use]
    pub fn id_index_spec(&self) -> Document {
        let mut d = doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"};
        if let Ok(c) = self.options.get_document("collation") {
            d.insert("collation", c.clone());
        }
        d
    }

    /// The `listIndexes` entries: `_id_` first.
    #[must_use]
    pub fn index_specs(&self) -> Vec<Document> {
        let mut v = vec![self.id_index_spec()];
        v.extend(
            self.indexes
                .iter()
                .filter(|i| !i.building)
                .map(|i| i.spec.clone()),
        );
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specs_parse_and_conflict() {
        let a = parse_index(&doc! {"key": {"a": 1, "b": -1}, "name": "ab"}, 1).unwrap();
        assert_eq!(a.fields[1].1, FieldKind::Desc);
        assert_eq!(default_name(&doc! {"a": 1, "b": -1}), "a_1_b_-1");
        let mut c = CollMeta::new("d", "c", 1, Document::new());
        assert!(matches!(c.add_index(a.clone()), Ok(Added::New)));
        assert!(matches!(c.add_index(a), Ok(Added::Exists)));
        let other = parse_index(&doc! {"key": {"z": 1}, "name": "ab"}, 2).unwrap();
        assert_eq!(c.add_index(other).unwrap_err().code, 86);
        let opt = parse_index(
            &doc! {"key": {"a": 1, "b": -1}, "name": "ab", "unique": true},
            3,
        )
        .unwrap();
        assert_eq!(c.add_index(opt).unwrap_err().code, 86);
        let other = parse_index(
            &doc! {"key": {"a": 1, "b": -1}, "name": "ab2", "hidden": true},
            3,
        )
        .unwrap();
        assert_eq!(c.add_index(other).unwrap_err().code, 85);
        assert_eq!(
            parse_index(&doc! {"key": {"a": 0}}, 1).unwrap_err().code,
            67
        );
        assert_eq!(
            parse_index(&doc! {"key": {"t": "text"}}, 1)
                .unwrap_err()
                .code,
            code::SPRIG_DOC_FEATURE_UNSUPPORTED
        );
        let r = CollMeta::from_record("d", "c", &c.to_record()).unwrap();
        assert_eq!(r.indexes.len(), 1);
        assert_eq!(r.uuid, c.uuid);
    }

    #[test]
    fn collated_indexes() {
        let ci = parse_index(
            &doc! {"key": {"a": 1}, "name": "a_ci", "collation": {"locale": "en", "strength": 2}},
            1,
        )
        .unwrap();
        assert_eq!(ci.collation.unwrap().strength, 2);
        let shown = ci.spec.get_document("collation").unwrap();
        assert_eq!(shown.get_str("caseFirst"), Ok("off"));
        assert_eq!(shown.get_str("alternate"), Ok("non-ignorable"));
        let plain = parse_index(
            &doc! {"key": {"a": 1}, "name": "a_1", "collation": {"locale": "simple"}},
            2,
        )
        .unwrap();
        assert!(plain.collation.is_none() && !plain.spec.contains_key("collation"));
        let fr = Collation::normalize(&doc! {"locale": "fr"})
            .unwrap()
            .unwrap();
        let mut c = CollMeta::new("d", "c", 1, doc! {"collation": fr});
        assert!(c.collation.is_some());
        assert!(matches!(c.add_index(ci), Ok(Added::New)));
        assert!(matches!(c.add_index(plain), Ok(Added::New)));
        let inherited = parse_index(&c.index_spec(&doc! {"key": {"b": 1}}), 3).unwrap();
        let shown = inherited.spec.get_document("collation").unwrap();
        assert_eq!(shown.get_str("locale"), Ok("fr"));
        let r = CollMeta::from_record("d", "c", &c.to_record()).unwrap();
        assert_eq!(r.indexes[0].collation, c.indexes[0].collation);
        assert!(
            c.collation_for(&doc! {"collation": {"locale": "simple"}})
                .is_none()
        );
        assert!(c.collation_for(&doc! {}).is_some());
        assert_eq!(
            parse_index(&doc! {"key": {"a": 1}, "collation": 1}, 1)
                .unwrap_err()
                .code,
            14
        );
    }
}
