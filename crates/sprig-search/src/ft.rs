//! Redis `FT.*` commands' arguments: `FT.CREATE` and `FT.ALTER` become
//! search DDL ([`crate::ddl`]), so an index made from Redis is an
//! ordinary search index on the Redis database's namespace, checked and
//! created as `CREATE SEARCH INDEX` is; `FT.SEARCH` options are parsed
//! here for the gateway.
//!
//! ```text
//! FT.CREATE idx [ON HASH] [PREFIX n p...] [LANGUAGE l] [STOPWORDS 0] ...
//!   SCHEMA field [AS alias] TEXT [WEIGHT w] [NOSTEM] [SORTABLE]
//!                         | TAG [SEPARATOR c] [CASESENSITIVE] [SORTABLE]
//!                         | NUMERIC [SORTABLE] ...
//! → CREATE SEARCH INDEX `idx` ON `db`.`table` (
//!     VALUE FIELD 'field' AS 'alias' TEXT WITH (analyzer = 'english', boost = w),
//!     VALUE FIELD 'tag' KEYWORD WITH (separator = ',', filter = true, facet = true),
//!     VALUE FIELD 'n' NUMBER WITH (filter = true, sort = true))
//!   WITH (prefixes = ('p', ...), analyzer = 'english')
//! ```
//!
//! A `TEXT ... SORTABLE` field also gets a keyword field of the same name
//! for sorting. `GEO`, `VECTOR` and `GEOSHAPE` fields, `ON JSON`,
//! `FILTER`, `PHONETIC`, custom stop-word lists and `TEMPORARY` indexes
//! answer 39823.

use sprig_common::{Result, SprigCode, SprigError};

/// A Redis argument as text (lossy for non-UTF-8 bytes).
fn text(a: &[u8]) -> String {
    String::from_utf8_lossy(a).into_owned()
}

fn upper(a: &[u8]) -> String {
    text(a).to_ascii_uppercase()
}

/// A Redis Search-style argument error (the reply is `-ERR <message>`).
#[must_use]
pub fn arg_error(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchUnsupported, msg)
}

/// A schema field's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtType {
    /// `TEXT`.
    Text,
    /// `TAG`.
    Tag,
    /// `NUMERIC`.
    Numeric,
}

impl FtType {
    /// The name `FT.INFO` shows.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "TEXT",
            Self::Tag => "TAG",
            Self::Numeric => "NUMERIC",
        }
    }
}

/// One `SCHEMA` field.
#[derive(Debug, Clone, PartialEq)]
pub struct FtField {
    /// The hash field.
    pub field: String,
    /// The name in queries (`AS`), else the field.
    pub alias: String,
    /// The type.
    pub ty: FtType,
    /// `WEIGHT` (text).
    pub weight: f64,
    /// `NOSTEM` (text).
    pub nostem: bool,
    /// `SORTABLE`.
    pub sortable: bool,
    /// `SEPARATOR` (tags; `,` by default).
    pub separator: char,
    /// `CASESENSITIVE` (tags).
    pub case_sensitive: bool,
}

/// `FT.CREATE`'s arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct FtCreate {
    /// The index name.
    pub name: String,
    /// `PREFIX`.
    pub prefixes: Vec<String>,
    /// `LANGUAGE` (lower-case), `english` by default.
    pub language: String,
    /// `STOPWORDS 0`.
    pub no_stopwords: bool,
    /// The fields.
    pub fields: Vec<FtField>,
}

/// SQL string literal.
fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// SQL identifier.
fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Languages Redis Search stems that the analyzers have.
const LANGUAGES: &[&str] = &[
    "arabic",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "greek",
    "hungarian",
    "italian",
    "norwegian",
    "portuguese",
    "romanian",
    "russian",
    "spanish",
    "swedish",
    "tamil",
    "turkish",
];

/// Parses the schema fields from `a` (after `SCHEMA` or `SCHEMA ADD`).
///
/// # Errors
/// 39822 for malformed fields, 39823 for types and options not offered.
pub fn parse_fields(a: &[&[u8]]) -> Result<Vec<FtField>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < a.len() {
        let field = text(a[i]);
        i += 1;
        let mut alias = field.clone();
        if a.get(i).is_some_and(|x| upper(x) == "AS") {
            alias = a
                .get(i + 1)
                .map(|x| text(x))
                .ok_or_else(|| arg_error("Missing argument after AS"))?;
            i += 2;
        }
        let ty = match a.get(i).map(|x| upper(x)).as_deref() {
            Some("TEXT") => FtType::Text,
            Some("TAG") => FtType::Tag,
            Some("NUMERIC") => FtType::Numeric,
            Some(t @ ("GEO" | "VECTOR" | "GEOSHAPE")) => {
                return Err(unsupported(format!(
                    "{t} fields are not supported in FT.CREATE"
                )));
            }
            Some(t) => {
                return Err(arg_error(format!(
                    "Invalid field type for field `{field}`: {t}"
                )));
            }
            None => return Err(arg_error(format!("Field `{field}` has no type"))),
        };
        i += 1;
        let mut f = FtField {
            field,
            alias,
            ty,
            weight: 1.0,
            nostem: false,
            sortable: false,
            separator: ',',
            case_sensitive: false,
        };
        while let Some(opt) = a.get(i).map(|x| upper(x)) {
            match opt.as_str() {
                "WEIGHT" if ty == FtType::Text => {
                    f.weight = a
                        .get(i + 1)
                        .and_then(|x| text(x).parse::<f64>().ok())
                        .filter(|w| w.is_finite() && *w > 0.0)
                        .ok_or_else(|| arg_error("Could not parse field spec: bad WEIGHT"))?;
                    i += 2;
                }
                "NOSTEM" if ty == FtType::Text => {
                    f.nostem = true;
                    i += 1;
                }
                "SEPARATOR" if ty == FtType::Tag => {
                    let s = a.get(i + 1).map(|x| text(x)).unwrap_or_default();
                    let mut cs = s.chars();
                    match (cs.next(), cs.next()) {
                        (Some(c), None) => f.separator = c,
                        _ => {
                            return Err(arg_error(
                                "Tag separator must be a single character. Got `".to_owned()
                                    + &s
                                    + "`",
                            ));
                        }
                    }
                    i += 2;
                }
                "CASESENSITIVE" if ty == FtType::Tag => {
                    f.case_sensitive = true;
                    i += 1;
                }
                "SORTABLE" => {
                    f.sortable = true;
                    i += 1;
                    if a.get(i).is_some_and(|x| upper(x) == "UNF") {
                        i += 1;
                    }
                }
                // Accepted and ignored: they change memory use or
                // reporting in Redis, not results here.
                "NOINDEX" | "WITHSUFFIXTRIE" | "INDEXEMPTY" | "INDEXMISSING" => i += 1,
                "PHONETIC" => {
                    return Err(unsupported("PHONETIC text fields are not supported"));
                }
                _ => break,
            }
        }
        out.push(f);
    }
    if out.is_empty() {
        return Err(arg_error("Fields arguments are missing"));
    }
    Ok(out)
}

/// Parses `FT.CREATE`'s arguments (after the command name).
///
/// # Errors
/// 39822 for malformed arguments, 39823 for options not offered.
pub fn parse_create(a: &[&[u8]]) -> Result<FtCreate> {
    let Some(name) = a.first() else {
        return Err(arg_error(
            "wrong number of arguments for 'FT.CREATE' command",
        ));
    };
    let mut c = FtCreate {
        name: text(name),
        prefixes: Vec::new(),
        language: "english".to_owned(),
        no_stopwords: false,
        fields: Vec::new(),
    };
    let mut i = 1;
    let count = |i: usize| -> Result<usize> {
        a.get(i)
            .and_then(|x| text(x).parse::<usize>().ok())
            .ok_or_else(|| arg_error("Bad arguments: expected a count"))
    };
    while i < a.len() {
        let opt = upper(a[i]);
        match opt.as_str() {
            "ON" => {
                match a.get(i + 1).map(|x| upper(x)).as_deref() {
                    Some("HASH") => {}
                    Some("JSON") => {
                        return Err(unsupported(
                            "FT.CREATE ON JSON is not supported (no JSON keys)",
                        ));
                    }
                    _ => return Err(arg_error("Unknown index type")),
                }
                i += 2;
            }
            "PREFIX" => {
                let n = count(i + 1)?;
                if i + 2 + n > a.len() {
                    return Err(arg_error("Bad arguments for PREFIX"));
                }
                c.prefixes
                    .extend(a[i + 2..i + 2 + n].iter().map(|x| text(x)));
                i += 2 + n;
            }
            "LANGUAGE" => {
                let l = a.get(i + 1).map(|x| text(x).to_ascii_lowercase());
                match l {
                    Some(l) if LANGUAGES.contains(&l.as_str()) => c.language = l,
                    Some(l) => {
                        return Err(unsupported(format!("Invalid language: {l}")));
                    }
                    None => return Err(arg_error("Bad arguments for LANGUAGE")),
                }
                i += 2;
            }
            "STOPWORDS" => {
                let n = count(i + 1)?;
                if n != 0 {
                    return Err(unsupported(
                        "custom stop-word lists are not supported; STOPWORDS 0 is",
                    ));
                }
                c.no_stopwords = true;
                i += 2;
            }
            "SCORE" => {
                // The default score of documents: scores here are BM25's.
                i += 2;
            }
            "MAXTEXTFIELDS" | "NOOFFSETS" | "NOHL" | "NOFIELDS" | "NOFREQS" | "SKIPINITIALSCAN" => {
                i += 1;
            }
            "FILTER" | "LANGUAGE_FIELD" | "SCORE_FIELD" | "PAYLOAD_FIELD" | "TEMPORARY" => {
                return Err(unsupported(format!("FT.CREATE {opt} is not supported")));
            }
            "SCHEMA" => {
                c.fields = parse_fields(&a[i + 1..])?;
                i = a.len();
            }
            other => {
                return Err(arg_error(format!("Unknown argument `{other}`")));
            }
        }
    }
    if c.fields.is_empty() {
        return Err(arg_error("No schema found"));
    }
    let mut seen = std::collections::HashSet::new();
    for f in &c.fields {
        if !seen.insert(f.alias.to_ascii_lowercase()) {
            return Err(arg_error(format!(
                "Duplicate field in schema - {}",
                f.alias
            )));
        }
    }
    Ok(c)
}

/// The analyzer of text fields: the language's, without stemming or stop
/// words as asked.
#[must_use]
pub fn text_analyzer(language: &str, nostem: bool, no_stopwords: bool) -> String {
    match (nostem, no_stopwords) {
        (false, false) => language.to_owned(),
        (true, false) => format!("{language}_nostem"),
        (false, true) => format!("{language}_nostop"),
        (true, true) => "standard".to_owned(),
    }
}

/// One field's DDL items (a sortable text field has a keyword sibling).
fn field_ddl(f: &FtField, language: &str, no_stopwords: bool) -> Vec<String> {
    let head = format!("VALUE FIELD {} AS {}", lit(&f.field), lit(&f.alias));
    match f.ty {
        FtType::Text => {
            let mut v = vec![format!(
                "{head} TEXT WITH (analyzer = {}, boost = {})",
                lit(&text_analyzer(language, f.nostem, no_stopwords)),
                f.weight
            )];
            if f.sortable {
                v.push(format!("{head} KEYWORD WITH (sort = true)"));
            }
            v
        }
        FtType::Tag => vec![format!(
            "{head} KEYWORD WITH (separator = {}, case_sensitive = {}, filter = true, facet = true, sort = true)",
            lit(&f.separator.to_string()),
            f.case_sensitive
        )],
        FtType::Numeric => vec![format!("{head} NUMBER WITH (filter = true, sort = true)")],
    }
}

impl FtCreate {
    /// The `CREATE SEARCH INDEX` statement on namespace `table` of
    /// database `db`.
    #[must_use]
    pub fn ddl(&self, db: &str, table: &str) -> String {
        let items: Vec<String> = self
            .fields
            .iter()
            .flat_map(|f| field_ddl(f, &self.language, self.no_stopwords))
            .collect();
        let mut opts = vec![format!(
            "analyzer = {}",
            lit(&text_analyzer(&self.language, false, self.no_stopwords))
        )];
        if !self.prefixes.is_empty() {
            let p: Vec<String> = self.prefixes.iter().map(|p| lit(p)).collect();
            opts.push(format!("prefixes = ({})", p.join(", ")));
        }
        format!(
            "CREATE SEARCH INDEX {} ON {}.{} ({}) WITH ({})",
            ident(&self.name),
            ident(db),
            ident(table),
            items.join(", "),
            opts.join(", ")
        )
    }
}

/// `FT.ALTER idx [SKIPINITIALSCAN] SCHEMA ADD field ...` as one `ALTER
/// SEARCH INDEX ... ADD FIELD` statement per field item, for an index of
/// `language`.
///
/// # Errors
/// As [`parse_fields`]; 39822 without `SCHEMA ADD`.
pub fn alter_ddl(a: &[&[u8]], db: &str, language: &str) -> Result<(String, Vec<String>)> {
    let Some(name) = a.first() else {
        return Err(arg_error(
            "wrong number of arguments for 'FT.ALTER' command",
        ));
    };
    let mut i = 1;
    if a.get(i).is_some_and(|x| upper(x) == "SKIPINITIALSCAN") {
        i += 1;
    }
    if a.get(i).map(|x| upper(x)).as_deref() != Some("SCHEMA")
        || a.get(i + 1).map(|x| upper(x)).as_deref() != Some("ADD")
    {
        return Err(arg_error("Unknown action passed to ALTER SCHEMA"));
    }
    let fields = parse_fields(&a[i + 2..])?;
    let name = text(name);
    let stmts = fields
        .iter()
        .flat_map(|f| field_ddl(f, language, false))
        .map(|item| {
            format!(
                "ALTER SEARCH INDEX {}.{} ADD FIELD {item}",
                ident(db),
                ident(&name)
            )
        })
        .collect();
    Ok((name, stmts))
}

/// `FT.SEARCH`'s options.
// Each flag is an option of the command.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SearchOpts {
    /// `NOCONTENT`.
    pub nocontent: bool,
    /// `VERBATIM` (accepted; stemming still applies).
    pub verbatim: bool,
    /// `WITHSCORES`.
    pub withscores: bool,
    /// `WITHSORTKEYS`.
    pub withsortkeys: bool,
    /// `FILTER field min max` (numeric).
    pub filters: Vec<(String, String, String)>,
    /// `INKEYS`.
    pub inkeys: Option<Vec<Vec<u8>>>,
    /// `INFIELDS`.
    pub infields: Option<Vec<String>>,
    /// `RETURN` fields with their names in the reply.
    pub returns: Option<Vec<(String, String)>>,
    /// `SORTBY field [ASC|DESC]` (true: descending).
    pub sortby: Option<(String, bool)>,
    /// `LIMIT offset num` (`0 10` by default).
    pub offset: usize,
    /// `LIMIT`'s count.
    pub num: usize,
    /// `PARAMS`.
    pub params: Vec<(String, String)>,
    /// `DIALECT` (1 by default).
    pub dialect: u32,
    /// `SLOP`.
    pub slop: Option<u32>,
    /// `INORDER` (accepted; phrase order is kept anyway).
    pub inorder: bool,
}

/// Parses `FT.SEARCH`'s options (after the index and the query).
///
/// # Errors
/// 39822 for malformed options, 39823 for those not offered.
pub fn parse_search_opts(a: &[&[u8]]) -> Result<SearchOpts> {
    let mut o = SearchOpts {
        num: 10,
        dialect: 1,
        ..SearchOpts::default()
    };
    let mut i = 0;
    let num = |i: usize, what: &str| -> Result<usize> {
        a.get(i)
            .and_then(|x| text(x).parse::<usize>().ok())
            .ok_or_else(|| arg_error(format!("Bad arguments for {what}: expected a number")))
    };
    let list = |i: usize, what: &str| -> Result<(usize, Vec<Vec<u8>>)> {
        let n = num(i, what)?;
        if i + 1 + n > a.len() {
            return Err(arg_error(format!("Bad arguments for {what}")));
        }
        Ok((n, a[i + 1..i + 1 + n].iter().map(|x| x.to_vec()).collect()))
    };
    while i < a.len() {
        let opt = upper(a[i]);
        match opt.as_str() {
            "NOCONTENT" => o.nocontent = true,
            "VERBATIM" => o.verbatim = true,
            "NOSTOPWORDS" | "EXPLAINSCORE" | "WITHPAYLOADS" => {}
            "WITHSCORES" => o.withscores = true,
            "WITHSORTKEYS" => o.withsortkeys = true,
            "INORDER" => o.inorder = true,
            "FILTER" => {
                if a.get(i + 3).is_none() {
                    return Err(arg_error("Bad arguments for FILTER"));
                }
                o.filters
                    .push((text(a[i + 1]), text(a[i + 2]), text(a[i + 3])));
                i += 3;
            }
            "INKEYS" => {
                let (n, v) = list(i + 1, "INKEYS")?;
                o.inkeys.get_or_insert_with(Vec::new).extend(v);
                i += 1 + n;
            }
            "INFIELDS" => {
                let (n, v) = list(i + 1, "INFIELDS")?;
                o.infields
                    .get_or_insert_with(Vec::new)
                    .extend(v.iter().map(|x| text(x)));
                i += 1 + n;
            }
            "RETURN" => {
                let n = num(i + 1, "RETURN")?;
                if i + 2 + n > a.len() {
                    return Err(arg_error("Bad arguments for RETURN"));
                }
                let mut v = Vec::new();
                let mut j = i + 2;
                let end = i + 2 + n;
                while j < end {
                    let f = text(a[j]).trim_start_matches('@').to_owned();
                    if j + 2 < end && upper(a[j + 1]) == "AS" {
                        v.push((f, text(a[j + 2])));
                        j += 3;
                    } else {
                        v.push((f.clone(), f));
                        j += 1;
                    }
                }
                o.returns = Some(v);
                i += 1 + n;
            }
            "SORTBY" => {
                let f = a
                    .get(i + 1)
                    .map(|x| text(x).trim_start_matches('@').to_owned())
                    .ok_or_else(|| arg_error("Bad arguments for SORTBY"))?;
                let mut desc = false;
                match a.get(i + 2).map(|x| upper(x)).as_deref() {
                    Some("ASC") => i += 1,
                    Some("DESC") => {
                        desc = true;
                        i += 1;
                    }
                    _ => {}
                }
                if a.get(i + 2).is_some_and(|x| upper(x) == "WITHCOUNT") {
                    i += 1;
                }
                o.sortby = Some((f, desc));
                i += 1;
            }
            "LIMIT" => {
                o.offset = num(i + 1, "LIMIT")?;
                o.num = num(i + 2, "LIMIT")?;
                i += 2;
            }
            "PARAMS" => {
                let n = num(i + 1, "PARAMS")?;
                if n % 2 != 0 || i + 2 + n > a.len() {
                    return Err(arg_error("Parameters must be name value pairs"));
                }
                for p in a[i + 2..i + 2 + n].chunks(2) {
                    o.params.push((text(p[0]), text(p[1])));
                }
                i += 1 + n;
            }
            "DIALECT" => {
                let d = num(i + 1, "DIALECT")?;
                if !(1..=4).contains(&d) {
                    return Err(arg_error(
                        "DIALECT requires a non negative integer >=1 and <= 4",
                    ));
                }
                o.dialect = u32::try_from(d).unwrap_or(1);
                i += 1;
            }
            "SLOP" => {
                o.slop = Some(u32::try_from(num(i + 1, "SLOP")?).unwrap_or(u32::MAX));
                i += 1;
            }
            "TIMEOUT" | "LANGUAGE" | "EXPANDER" | "SCORER" | "PAYLOAD" => i += 1,
            "SUMMARIZE" | "HIGHLIGHT" | "GEOFILTER" => {
                return Err(unsupported(format!("FT.SEARCH {opt} is not supported")));
            }
            other => {
                return Err(arg_error(format!("Unknown argument `{other}`")));
            }
        }
        i += 1;
    }
    Ok(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<Vec<u8>> {
        s.split_whitespace()
            .map(|x| x.as_bytes().to_vec())
            .collect()
    }

    fn refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
        v.iter().map(Vec::as_slice).collect()
    }

    #[test]
    fn create_becomes_ddl() {
        let a = args(
            "idx ON HASH PREFIX 2 doc: p: SCHEMA title TEXT WEIGHT 5.0 SORTABLE body AS b TEXT NOSTEM price NUMERIC SORTABLE tags TAG SEPARATOR ; CASESENSITIVE",
        );
        let c = parse_create(&refs(&a)).unwrap();
        assert_eq!(c.prefixes, vec!["doc:", "p:"]);
        assert_eq!(c.fields.len(), 4);
        assert_eq!(c.fields[1].alias, "b");
        let ddl = c.ddl("redis", "db0");
        assert_eq!(
            ddl,
            "CREATE SEARCH INDEX `idx` ON `redis`.`db0` (\
             VALUE FIELD 'title' AS 'title' TEXT WITH (analyzer = 'english', boost = 5), \
             VALUE FIELD 'title' AS 'title' KEYWORD WITH (sort = true), \
             VALUE FIELD 'body' AS 'b' TEXT WITH (analyzer = 'english_nostem', boost = 1), \
             VALUE FIELD 'price' AS 'price' NUMBER WITH (filter = true, sort = true), \
             VALUE FIELD 'tags' AS 'tags' KEYWORD WITH (separator = ';', case_sensitive = true, filter = true, facet = true, sort = true)) \
             WITH (analyzer = 'english', prefixes = ('doc:', 'p:'))"
        );
        // The DDL parses back to hash fields.
        let Some(crate::ddl::SearchDdl::Create { def, .. }) = crate::ddl::parse(&ddl).unwrap()
        else {
            panic!("not a create");
        };
        assert_eq!(def.model, crate::def::Model::Kv);
        assert!(def.has_hash_fields());
        assert_eq!(def.key_prefixes, vec!["doc:", "p:"]);
        let b = &def.fields[2];
        assert_eq!((b.name.as_str(), b.source_ref.as_str()), ("b", "body"));
        let tags = &def.fields[4];
        assert_eq!(tags.separator.as_deref(), Some(";"));
        assert!(tags.case_sensitive);
        assert_eq!(
            tags.split_value(" A;b ;; c"),
            vec!["A".to_owned(), "b".into(), "c".into()]
        );
    }

    #[test]
    fn create_refusals() {
        let bad = |s: &str| parse_create(&refs(&args(s))).unwrap_err().code();
        assert_eq!(
            bad("i ON JSON SCHEMA a TEXT"),
            Some(SprigCode::SearchUnsupported)
        );
        assert_eq!(bad("i SCHEMA g GEO"), Some(SprigCode::SearchUnsupported));
        assert_eq!(bad("i SCHEMA a BLOB"), Some(SprigCode::SearchQuerySyntax));
        assert_eq!(bad("i PREFIX 1"), Some(SprigCode::SearchQuerySyntax));
        assert_eq!(
            bad("i STOPWORDS 1 a SCHEMA a TEXT"),
            Some(SprigCode::SearchUnsupported)
        );
        assert_eq!(
            bad("i SCHEMA a TEXT A TAG"),
            Some(SprigCode::SearchQuerySyntax)
        );
    }

    #[test]
    fn alter_adds_fields() {
        let a = args("idx SCHEMA ADD n NUMERIC");
        let (name, stmts) = alter_ddl(&refs(&a), "redis", "english").unwrap();
        assert_eq!(name, "idx");
        assert_eq!(
            stmts,
            vec![
                "ALTER SEARCH INDEX `redis`.`idx` ADD FIELD VALUE FIELD 'n' AS 'n' NUMBER WITH (filter = true, sort = true)"
            ]
        );
        assert!(crate::ddl::parse(&stmts[0]).unwrap().is_some());
    }

    #[test]
    fn search_options() {
        let a = args(
            "NOCONTENT WITHSCORES FILTER price 1 (5 INKEYS 2 a b RETURN 4 title AS t body SORTBY price DESC LIMIT 5 20 PARAMS 2 x hello DIALECT 2",
        );
        let o = parse_search_opts(&refs(&a)).unwrap();
        assert!(o.nocontent && o.withscores);
        assert_eq!(o.filters, vec![("price".into(), "1".into(), "(5".into())]);
        assert_eq!(o.inkeys, Some(vec![b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(
            o.returns,
            Some(vec![
                ("title".into(), "t".into()),
                ("body".into(), "body".into())
            ])
        );
        assert_eq!(o.sortby, Some(("price".into(), true)));
        assert_eq!((o.offset, o.num, o.dialect), (5, 20, 2));
        assert_eq!(o.params, vec![("x".into(), "hello".into())]);
        assert!(parse_search_opts(&refs(&args("LIMIT 0"))).is_err());
        assert!(parse_search_opts(&refs(&args("BOGUS"))).is_err());
    }
}
