//! Row-level security for paths that stream a table's rows out of the
//! gateway's session rewrite.
//!
//! ```text
//! change topic t.changes read by a principal (SQL, Kafka, a stream trigger,
//! a sink FROM STREAM t.changes):
//!   SELECT on the source table t itself (not only on the name "t.changes")
//!   policies on t bind the principal → 39366 (the topic's records are the
//!     table's rows; they aren't filtered per record)
//! ```
//!
//! The checks are the same at creation (a trigger's or sink's) and at each
//! run, so a policy, role or grant change after creation stops delivery.
//!
//! **Per-record filtering.** A session the table's policies bind reads the
//! topic through [`record_filter`]: its `SELECT` policies, rendered with
//! its own session values exactly as the rewrite renders them for the
//! table, are evaluated by MariaDB over each record's `before` and `after`
//! images (`JSON_TABLE` with the table's column types and collations,
//! [`filter_columns`]):
//!
//! ```text
//! c: after visible        d: before visible       ddl, t: always
//! u: both → u; before only → d (after null); after only → c (before null)
//! anything else is left out (an offset gap; nothing is renumbered)
//! ```
//!
//! Records carry some columns in another form than the table (binary
//! strings as base64, an `ENUM`'s index, a `SET`'s bitmap, a `TIMESTAMP`
//! in UTC): [`decode`] reads them back as the table shows them, and a
//! policy reading a column that can't be read back (bit, spatial, vector,
//! an `ENUM` or `SET` used other than against string literals) hides
//! every image of the table.

use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_catalog::pool_wait::Bounded as _;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_streams::model::{StreamDef, StreamKind};

/// Whether `grants` may read change topic `def`'s records: `Ok` for an
/// event stream, or a change topic whose source table the grants may
/// `SELECT` and whose policies don't bind them. 1142 without `SELECT` on
/// the table, 39366 when its policies bind.
///
/// # Errors
/// As above.
pub fn change_topic_readable(
    grants: &dyn SessionGrants,
    cat: &TenantCatalog,
    def: &StreamDef,
    lctn: bool,
    user: &str,
) -> Result<()> {
    if def.kind != StreamKind::Changes {
        return Ok(());
    }
    if grants.unrestricted() {
        return Ok(());
    }
    let table = def.source_name.clone().unwrap_or_default();
    let (Some(object), Some(db)) = (def.source_object, cat.databases.get(def.db)) else {
        // A change topic without its table: nothing to read it by.
        return Err(denied(user, "?", &table));
    };
    let db_key = database_key(&db.name, lctn);
    let on = Target::Object {
        db_key: &db_key,
        db: def.db,
        object,
    };
    if !grants.allows(Action::Select, on) {
        return Err(denied(user, &db.name, &table));
    }
    if grants.row_filter(on, Action::Select).is_some() {
        return Err(bound(&table, &def.name));
    }
    Ok(())
}

/// How a principal may read a change topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicAccess {
    /// Every record as stored.
    All,
    /// The records the table's policies show it ([`record_filter`]).
    Filtered,
}

/// Whether `grants` may read change topic `def`, and how: `SELECT` on its
/// source table (1142 without), its records filtered when the table's
/// policies bind the grants ([`change_topic_readable`] refuses those).
///
/// # Errors
/// 1142.
pub fn change_topic_access(
    grants: &dyn SessionGrants,
    cat: &TenantCatalog,
    def: &StreamDef,
    lctn: bool,
    user: &str,
) -> Result<TopicAccess> {
    match change_topic_readable(grants, cat, def, lctn, user) {
        Ok(()) => Ok(TopicAccess::All),
        Err(e) if e.code() == Some(SprigCode::RlsUnsupported) => Ok(TopicAccess::Filtered),
        Err(e) => Err(e),
    }
}

/// 39366 for change topic `topic` of table `table`.
#[must_use]
pub fn bound(table: &str, topic: &str) -> SprigError {
    SprigError::new(
        SprigCode::RlsUnsupported,
        format!(
            "ER_SPRIG_RLS_UNSUPPORTED: row-level security on '{table}' binds this user, and \
             change topic '{topic}' carries the table's rows unfiltered"
        ),
    )
}

fn denied(user: &str, db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("SELECT command denied to user '{user}' for table `{db}`.`{table}`"),
    )
}

/// A column of a protected table as a change record's image types it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterColumn {
    /// The name.
    pub name: String,
    /// Its `JSON_TABLE` type (with character set and collation).
    pub ty: String,
    /// The record carries it in another form (base64, a JSON array): a
    /// policy reading it can't be evaluated on the image.
    pub opaque: bool,
    /// A binary string column's own type, for images that carry its bytes
    /// as a plain JSON string (keyspace notifications' keys).
    pub plain: Option<String>,
    /// How the record carries the value when it isn't as the table types
    /// it (an `ENUM`'s index, a `SET`'s bitmap, a `TIMESTAMP` in UTC).
    pub carried: Carried,
}

/// How a change record carries a column the table types otherwise
/// ([`decode`] reads it back as the table shows it).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Carried {
    /// As the table types it (or base64 for binary strings, see
    /// [`FilterColumn::plain`]).
    #[default]
    AsIs,
    /// An `ENUM`: its 1-based member index (0: the empty error value),
    /// with the members and the column's `CHARACTER SET .. COLLATE ..`.
    Enum(Vec<String>, String),
    /// A `SET`: its bitmap, with the members and character set.
    Set(Vec<String>, String),
    /// A `TIMESTAMP`: its UTC date and time.
    Utc,
}

/// The `JSON_TABLE` type of a column from `information_schema.COLUMNS`
/// (`DATA_TYPE`, `COLUMN_TYPE`, `CHARACTER_SET_NAME`, `COLLATION_NAME`),
/// and whether it is opaque ([`FilterColumn::opaque`]).
#[must_use]
pub fn json_table_type(
    data_type: &str,
    column_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> (String, bool) {
    let dt = data_type.to_ascii_lowercase();
    let text_of = |base: &str| match (charset, collation) {
        (Some(cs), Some(co)) => format!("{base} CHARACTER SET {cs} COLLATE {co}"),
        (Some(cs), None) => format!("{base} CHARACTER SET {cs}"),
        _ => base.to_owned(),
    };
    match dt.as_str() {
        // Records carry a TIMESTAMP in UTC: read as a DATETIME, which
        // [`decode`] moves to the session's time zone.
        "timestamp" => (
            column_type
                .to_ascii_lowercase()
                .replace("timestamp", "datetime"),
            false,
        ),
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "decimal"
        | "numeric" | "float" | "double" | "real" | "date" | "datetime" | "time" | "year" => (
            column_type.to_ascii_lowercase().replace(" zerofill", ""),
            false,
        ),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => {
            (text_of(&column_type.to_ascii_lowercase()), false)
        }
        // JSON_TABLE takes no ENUM or SET; records carry an index or a
        // bitmap, which [`decode`] turns into the members' text.
        "enum" | "set" => ("bigint unsigned".to_owned(), false),
        "uuid" | "inet4" | "inet6" => ("varchar(64) character set ascii".to_owned(), false),
        _ => ("longtext".to_owned(), true),
    }
}

/// How a record carries a column of `DATA_TYPE` `data_type` and
/// `COLUMN_TYPE` `column_type` ([`Carried`]).
#[must_use]
pub fn carried_of(
    data_type: &str,
    column_type: &str,
    charset: Option<&str>,
    collation: Option<&str>,
) -> Carried {
    let text = match (charset, collation) {
        (Some(cs), Some(co)) => format!("{cs} COLLATE {co}"),
        (Some(cs), None) => cs.to_owned(),
        _ => "utf8mb4".to_owned(),
    };
    match data_type.to_ascii_lowercase().as_str() {
        "enum" => Carried::Enum(members(column_type), text),
        "set" => Carried::Set(members(column_type), text),
        "timestamp" => Carried::Utc,
        _ => Carried::AsIs,
    }
}

/// The members of `enum('a','b')` or `set(...)`, as `COLUMN_TYPE` spells
/// them.
fn members(column_type: &str) -> Vec<String> {
    let opts = sprig_sql::lex::LexOptions::mariadb();
    let src = column_type.as_bytes();
    sprig_sql::lex::lex(src, &opts)
        .unwrap_or_default()
        .iter()
        .filter(|t| t.kind == sprig_sql::lex::TokenKind::String)
        .filter_map(|t| sprig_sql::lex::string_value(t.bytes(src), false))
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect()
}

/// An `information_schema.COLUMNS` row as [`filter_columns`] reads it.
type ColumnRow = (String, String, String, Option<String>, Option<String>);

/// Columns by (tenant, table, schema version).
type ColumnCache =
    std::sync::Mutex<std::collections::HashMap<(u64, u64, u64), std::sync::Arc<Vec<FilterColumn>>>>;

fn column_cache() -> &'static ColumnCache {
    static C: std::sync::OnceLock<ColumnCache> = std::sync::OnceLock::new();
    C.get_or_init(Default::default)
}

/// Table `table`'s columns in shard schema `schema` of `cluster` as
/// [`FilterColumn`]s, cached by the table's schema version. Internal
/// columns (`_sprig…`, `__sprig…`) are left out: records never carry them.
///
/// # Errors
/// The cluster's.
pub async fn filter_columns(
    svc: &super::TenantServices,
    tenant: u64,
    object: &sprig_catalog::store::ObjectRow,
    cluster: &sprig_common::ids::ClusterId,
    schema: &str,
) -> Result<std::sync::Arc<Vec<FilterColumn>>> {
    use mysql_async::prelude::Queryable;
    let key = (tenant, object.object_id.0, object.schema_version);
    if let Some(c) = column_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
    {
        return Ok(std::sync::Arc::clone(c));
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn_bounded().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let rows: Vec<ColumnRow> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, COLLATION_NAME \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (schema, object.name.as_str()),
        )
        .await
        .map_err(|e| SprigError::new(SprigCode::BackendUnavailable, format!("{cluster}: {e}")))?;
    drop(conn);
    if rows.is_empty() {
        return Err(SprigError::internal(format!(
            "table {} has no columns in {schema}",
            object.name
        )));
    }
    let cols: Vec<FilterColumn> = rows
        .into_iter()
        .filter(|(n, ..)| !n.starts_with("_sprig") && !n.starts_with("__sprig"))
        .map(|(name, dt, ct, cs, co)| {
            let (ty, opaque) = json_table_type(&dt, &ct, cs.as_deref(), co.as_deref());
            // A name a JSON path can't spell plainly is never read.
            let odd = name.contains(['"', '\\', '\'']);
            let binary = matches!(
                dt.to_ascii_lowercase().as_str(),
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            );
            FilterColumn {
                name,
                ty,
                opaque: opaque || odd,
                plain: (binary && !odd).then(|| ct.to_ascii_lowercase()),
                carried: carried_of(&dt, &ct, cs.as_deref(), co.as_deref()),
            }
        })
        .collect();
    let cols = std::sync::Arc::new(cols);
    column_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(key, std::sync::Arc::clone(&cols));
    Ok(cols)
}

/// The `JSON_TABLE` column list of `columns`.
#[must_use]
pub fn json_table_columns(columns: &[FilterColumn]) -> String {
    columns
        .iter()
        .map(|c| {
            let path = if c.opaque {
                "$.__sprig_none".to_owned()
            } else {
                format!("$.\"{}\"", c.name)
            };
            format!(
                "{} {} PATH {}",
                sprig_sql::rls::quote(&c.name),
                c.ty,
                sprig_sql::rls::sql_string(&path, false)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// How session `ts` reads change topic `def` of table `object` (database
/// `db_name`): `None` when the table's policies don't bind it, else its
/// `SELECT` policies rendered with its session values over
/// [`sprig_streams::read::FILTER_QUAL`].
///
/// # Errors
/// The rewrite's (39364, 39366 for a subtree above
/// `rls.inline_subtree_max`).
#[allow(clippy::too_many_arguments)]
pub async fn record_filter(
    ts: &super::session::TenantSession,
    cat: &TenantCatalog,
    db_name: &str,
    object: sprig_common::ids::ObjectId,
    columns: &[FilterColumn],
    lex: sprig_sql::lex::LexOptions,
    current: Option<&str>,
) -> Result<Option<sprig_streams::read::RecordFilter>> {
    use sprig_sql::rls::Resolver as _;
    let grants = super::authz::privileges(ts, cat);
    if grants.unrestricted() {
        return Ok(None);
    }
    let Some(rls) = grants.row_policies() else {
        return Ok(None);
    };
    if !rls.tables.contains_key(&object) {
        return Ok(None);
    }
    let Some(obj) = cat.objects.get(object) else {
        return Err(SprigError::internal("a change topic's table is gone"));
    };
    let mut values = super::rls::SessionValues::of(ts, cat);
    super::rls::bind_tree(ts, cat, &rls, &mut values).await?;
    let lctn = ts.lower_case_table_names();
    let tainted = std::collections::HashSet::new();
    let temp = |_: Option<&str>, _: &str| false;
    let qual = sprig_streams::read::FILTER_QUAL;
    let predicate = {
        let r = super::rls::Resolve::new(cat, &rls, current, lctn, &temp, &values, lex, &tainted);
        let key = r
            .protected(Some(db_name), &obj.name)
            .ok_or_else(|| SprigError::internal("a protected table the resolver can't find"))?;
        r.predicate(key, &[sprig_sql::rls::Cmd::Select], qual)?
    };
    Ok(Some(decode(columns, &predicate, qual, lex)))
}

/// `predicate` (rendered over `qual`) as it must read change records'
/// images, with the `JSON_TABLE` columns to read them: each reference
/// `qual.c` becomes the value the table itself would give, or the whole
/// predicate is `FALSE` (fail closed) when that can't be had.
///
/// ```text
/// binary string (BINARY, VARBINARY, BLOBs)  base64 in the record → FROM_BASE64(ref)
/// ENUM, SET (index, bitmap in the record)   → the members' text (ELT, MAKE_SET)
///   in the column's character set and collation, where the column is
///   compared with string literals only (=, <>, <, IN, LIKE, IS NULL,
///   FIND_IN_SET); any other use (arithmetic, a number, another column,
///   a function) could read the index on the table → FALSE
/// TIMESTAMP (UTC text in the record)         → the session's time zone (FROM_UNIXTIME)
/// bit, spatial, vector, odd names            → FALSE
/// ```
#[must_use]
pub fn decode(
    columns: &[FilterColumn],
    predicate: &str,
    qual: &str,
    lex: sprig_sql::lex::LexOptions,
) -> sprig_streams::read::RecordFilter {
    use sprig_sql::lex::TokenKind;
    let closed = || sprig_streams::read::RecordFilter {
        columns: json_table_columns(columns),
        predicate: "FALSE".to_owned(),
    };
    let src = predicate.as_bytes();
    let Ok(all) = sprig_sql::lex::lex(src, &lex) else {
        return closed();
    };
    if all
        .iter()
        .any(|t| matches!(t.kind, TokenKind::Unterminated | TokenKind::Other))
    {
        return closed();
    }
    let nt: Vec<sprig_sql::lex::Token> = all.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let toks = Toks { nt: &nt, src };
    let qname = qual.trim_matches('`').to_ascii_lowercase();
    let mut out = columns.to_vec();
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    let mut i = 0;
    while i + 2 < nt.len() {
        if !(toks.ident(i).as_deref() == Some(qname.as_str())
            && toks.is(i + 1, TokenKind::Punct, ".")
            && nt[i + 2].kind.is_ident_like())
        {
            i += 1;
            continue;
        }
        let name = toks.ident(i + 2).unwrap_or_default();
        let range = nt[i].start as usize..nt[i + 2].end as usize;
        let r = &predicate[range.clone()];
        if let Some(at) = columns.iter().position(|c| c.name.to_lowercase() == name) {
            let c = &columns[at];
            let text = if c.opaque {
                if c.plain.is_none() {
                    return closed();
                }
                out[at].opaque = false;
                "longtext CHARACTER SET ascii".clone_into(&mut out[at].ty);
                format!("FROM_BASE64({r})")
            } else {
                match &c.carried {
                    Carried::AsIs => r.to_owned(),
                    Carried::Utc => format!(
                        "COALESCE(FROM_UNIXTIME(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', {r})) \
                         + INTERVAL MICROSECOND({r}) MICROSECOND, {r})"
                    ),
                    Carried::Enum(m, cs) | Carried::Set(m, cs) => {
                        if !toks.compared_as_text(i, i + 2) {
                            return closed();
                        }
                        let list = m
                            .iter()
                            .map(|x| sprig_sql::rls::sql_string(x, lex.no_backslash_escapes))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let (cs, co) = cs.split_once(" COLLATE ").unwrap_or((cs.as_str(), ""));
                        let inner = if matches!(c.carried, Carried::Enum(..)) {
                            format!("ELT({r} + 1, '', {list})")
                        } else if m.is_empty() {
                            "''".to_owned()
                        } else {
                            format!("MAKE_SET({r}, {list})")
                        };
                        if co.is_empty() {
                            format!("CONVERT({inner} USING {cs})")
                        } else {
                            format!("CONVERT({inner} USING {cs}) COLLATE {co}")
                        }
                    }
                }
            };
            edits.push((range, text));
        }
        i += 3;
    }
    let mut s = String::with_capacity(predicate.len() + 64 * edits.len());
    let mut at = 0;
    for (range, text) in edits {
        s.push_str(&predicate[at..range.start]);
        s.push_str(&text);
        at = range.end;
    }
    s.push_str(&predicate[at..]);
    sprig_streams::read::RecordFilter {
        columns: json_table_columns(&out),
        predicate: s,
    }
}

/// A predicate's tokens without trivia, for [`decode`].
struct Toks<'a> {
    nt: &'a [sprig_sql::lex::Token],
    src: &'a [u8],
}

impl Toks<'_> {
    fn text(&self, i: usize) -> Option<&str> {
        self.nt
            .get(i)
            .and_then(|t| std::str::from_utf8(t.bytes(self.src)).ok())
    }

    fn is(&self, i: usize, kind: sprig_sql::lex::TokenKind, text: &str) -> bool {
        self.nt.get(i).is_some_and(|t| t.kind == kind)
            && self.text(i).is_some_and(|x| x.eq_ignore_ascii_case(text))
    }

    fn word(&self, i: usize, w: &str) -> bool {
        self.is(i, sprig_sql::lex::TokenKind::Word, w)
    }

    /// An identifier's value, lower case.
    fn ident(&self, i: usize) -> Option<String> {
        let t = self.nt.get(i).filter(|t| t.kind.is_ident_like())?;
        let v = sprig_sql::lex::ident_value(t.bytes(self.src));
        Some(String::from_utf8_lossy(&v).to_lowercase())
    }

    /// Where an expression may start: nothing, `(`, or a logical keyword.
    fn opens(&self, i: Option<usize>) -> bool {
        let Some(i) = i else { return true };
        self.is(i, sprig_sql::lex::TokenKind::Punct, "(")
            || ["AND", "OR", "XOR", "WHEN", "THEN", "ELSE", "WHERE", "ON"]
                .iter()
                .any(|w| self.word(i, w))
    }

    /// Where an expression may end: nothing, `)`, or a logical keyword.
    fn closes(&self, i: usize) -> bool {
        i >= self.nt.len()
            || self.is(i, sprig_sql::lex::TokenKind::Punct, ")")
            || ["AND", "OR", "XOR", "THEN", "ELSE", "END", "WHEN"]
                .iter()
                .any(|w| self.word(i, w))
    }

    fn comparison(&self, i: usize) -> bool {
        self.nt
            .get(i)
            .is_some_and(|t| t.kind == sprig_sql::lex::TokenKind::Op)
            && self
                .text(i)
                .is_some_and(|x| matches!(x, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" | "<=>"))
    }

    /// The `)` closing the `(` at `open`.
    fn close_of(&self, open: usize) -> Option<usize> {
        use sprig_sql::lex::TokenKind;
        let mut depth = 0usize;
        for k in open..self.nt.len() {
            if self.is(k, TokenKind::Punct, "(") {
                depth += 1;
            } else if self.is(k, TokenKind::Punct, ")") {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    /// A call at `i` whose value is always a string: `JSON_VALUE(..)` or
    /// `JSON_QUERY(..)` (how MariaDB's side reads session values), or
    /// `COALESCE` of such calls and string literals. The index after it.
    fn string_call(&self, i: usize) -> Option<usize> {
        use sprig_sql::lex::TokenKind;
        if !self.is(i + 1, TokenKind::Punct, "(") {
            return None;
        }
        let close = self.close_of(i + 1)?;
        if self.word(i, "JSON_VALUE") || self.word(i, "JSON_QUERY") {
            return Some(close + 1);
        }
        if !self.word(i, "COALESCE") {
            return None;
        }
        let mut k = i + 2;
        loop {
            k = self.literal(k)?;
            if k == close {
                return Some(close + 1);
            }
            if !self.is(k, TokenKind::Punct, ",") {
                return None;
            }
            k += 1;
        }
    }

    /// A string literal at `i` (an introducer, the string, a `COLLATE`),
    /// or a [`Self::string_call`]: the index after it.
    fn literal(&self, mut i: usize) -> Option<usize> {
        use sprig_sql::lex::TokenKind;
        if let Some(e) = self.string_call(i) {
            return Some(e);
        }
        if self.nt.get(i).is_some_and(|t| t.kind == TokenKind::Word)
            && self.text(i).is_some_and(|x| x.starts_with('_'))
        {
            i += 1;
        }
        if !self
            .nt
            .get(i)
            .is_some_and(|t| matches!(t.kind, TokenKind::String | TokenKind::NationalString))
        {
            return None;
        }
        i += 1;
        if self.word(i, "COLLATE") && self.nt.get(i + 1).is_some_and(|t| t.kind.is_ident_like()) {
            i += 2;
        }
        Some(i)
    }

    /// A string literal ending at `end` (inclusive): the index of its
    /// first token.
    fn literal_back(&self, mut end: usize) -> Option<usize> {
        use sprig_sql::lex::TokenKind;
        if self.is(end, TokenKind::Punct, ")") {
            // A string call ending here.
            return (0..end)
                .rev()
                .find(|&k| self.string_call(k) == Some(end + 1));
        }
        if end >= 2 && self.nt[end].kind.is_ident_like() && self.word(end - 1, "COLLATE") {
            end -= 2;
        }
        if !matches!(
            self.nt.get(end)?.kind,
            TokenKind::String | TokenKind::NationalString
        ) {
            return None;
        }
        if end >= 1
            && self.nt[end - 1].kind == TokenKind::Word
            && self.text(end - 1).is_some_and(|x| x.starts_with('_'))
        {
            return Some(end - 1);
        }
        Some(end)
    }

    /// Whether the reference at tokens `a..=b` is compared with string
    /// literals only, so that its text decides as the column does on the
    /// table: `ref op 'x'`, `'x' op ref`, `ref [NOT] IN ('x', ...)`,
    /// `ref [NOT] LIKE 'x' [ESCAPE 'y']`, `ref IS [NOT] NULL`,
    /// `FIND_IN_SET('x', ref)`.
    fn compared_as_text(&self, first: usize, last: usize) -> bool {
        use sprig_sql::lex::TokenKind;
        let before = first.checked_sub(1);
        let after = last + 1;
        // FIND_IN_SET('x', ref)
        if let Some(comma) = before
            && self.is(comma, TokenKind::Punct, ",")
            && self.is(after, TokenKind::Punct, ")")
            && comma >= 1
            && let Some(lit) = self.literal_back(comma - 1)
            && lit >= 2
            && self.is(lit - 1, TokenKind::Punct, "(")
            && self.word(lit - 2, "FIND_IN_SET")
        {
            return true;
        }
        // 'x' op ref
        if let Some(op) = before
            && self.comparison(op)
            && op >= 1
            && let Some(lit) = self.literal_back(op - 1)
        {
            return self.opens(lit.checked_sub(1)) && self.closes(after);
        }
        if !self.opens(before) {
            return false;
        }
        let mut pos = after;
        // ref op 'x'
        if self.comparison(pos) {
            return self.literal(pos + 1).is_some_and(|end| self.closes(end));
        }
        if self.word(pos, "IS") {
            pos += 1;
            if self.word(pos, "NOT") {
                pos += 1;
            }
            return self.word(pos, "NULL") && self.closes(pos + 1);
        }
        if self.word(pos, "NOT") {
            pos += 1;
        }
        if self.word(pos, "LIKE") {
            let Some(mut end) = self.literal(pos + 1) else {
                return false;
            };
            if self.word(end, "ESCAPE") {
                let Some(escape) = self.literal(end + 1) else {
                    return false;
                };
                end = escape;
            }
            return self.closes(end);
        }
        if self.word(pos, "IN") && self.is(pos + 1, TokenKind::Punct, "(") {
            let mut at = pos + 2;
            loop {
                let Some(next) = self.literal(at) else {
                    return false;
                };
                if self.is(next, TokenKind::Punct, ",") {
                    at = next + 1;
                    continue;
                }
                return self.is(next, TokenKind::Punct, ")") && self.closes(next + 1);
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: &str = "`__sprig_ci`";

    fn lex() -> sprig_sql::lex::LexOptions {
        sprig_sql::lex::LexOptions::mariadb()
    }

    fn col(name: &str, ty: &str) -> FilterColumn {
        FilterColumn {
            name: name.into(),
            ty: ty.into(),
            opaque: false,
            plain: None,
            carried: Carried::AsIs,
        }
    }

    fn cols() -> Vec<FilterColumn> {
        let (ty, opaque) = json_table_type("varbinary", "varbinary(16)", None, None);
        vec![
            col("owner", "varchar(64)"),
            FilterColumn {
                opaque,
                plain: Some("varbinary(16)".into()),
                ..col("b", &ty)
            },
            FilterColumn {
                carried: carried_of(
                    "enum",
                    "enum('public','secret','it''s')",
                    Some("utf8mb4"),
                    Some("utf8mb4_general_ci"),
                ),
                ..col("st", "bigint unsigned")
            },
            FilterColumn {
                carried: carried_of("set", "set('a','secret')", Some("utf8mb4"), None),
                ..col("tags", "bigint unsigned")
            },
            FilterColumn {
                carried: carried_of("timestamp", "timestamp(3)", None, None),
                ..col("ts", "datetime(3)")
            },
            FilterColumn {
                opaque: true,
                ..col("g", "longtext")
            },
        ]
    }

    #[test]
    fn column_types() {
        assert_eq!(
            json_table_type(
                "varchar",
                "varchar(64)",
                Some("utf8mb4"),
                Some("utf8mb4_general_ci")
            ),
            (
                "varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci".to_owned(),
                false
            )
        );
        assert_eq!(
            json_table_type("int", "int(10) unsigned zerofill", None, None),
            ("int(10) unsigned".to_owned(), false)
        );
        // Records carry an ENUM's index and a SET's bitmap.
        assert_eq!(
            json_table_type(
                "enum",
                "enum('a','b')",
                Some("utf8mb4"),
                Some("utf8mb4_bin")
            )
            .0,
            "bigint unsigned"
        );
        assert_eq!(
            json_table_type("timestamp", "timestamp(3)", None, None).0,
            "datetime(3)"
        );
        assert!(json_table_type("varbinary", "varbinary(16)", None, None).1);
        assert!(json_table_type("point", "point", None, None).1);
        assert_eq!(
            carried_of(
                "enum",
                "enum('a','it''s')",
                Some("utf8mb4"),
                Some("utf8mb4_bin")
            ),
            Carried::Enum(
                vec!["a".into(), "it's".into()],
                "utf8mb4 COLLATE utf8mb4_bin".into()
            )
        );
    }

    #[test]
    fn binary_columns_are_decoded_from_base64() {
        let f = decode(
            &cols(),
            "(`__sprig_ci`.b LIKE 'a:%') AND (`__sprig_ci`.`B` <> `__sprig_ci`.owner)",
            Q,
            lex(),
        );
        assert_eq!(
            f.predicate,
            "(FROM_BASE64(`__sprig_ci`.b) LIKE 'a:%') AND (FROM_BASE64(`__sprig_ci`.`B`) <> `__sprig_ci`.owner)"
        );
        assert!(
            f.columns
                .contains("`b` longtext CHARACTER SET ascii PATH '$.\"b\"'")
        );
        // Not read: stays unread.
        let f = decode(&cols(), "`__sprig_ci`.owner = 'x'", Q, lex());
        assert!(f.columns.contains("`b` longtext PATH '$.__sprig_none'"));
        assert_eq!(f.predicate, "`__sprig_ci`.owner = 'x'");
        // A string literal naming the qualifier is left alone.
        let f = decode(&cols(), "`__sprig_ci`.owner = '`__sprig_ci`.b'", Q, lex());
        assert_eq!(f.predicate, "`__sprig_ci`.owner = '`__sprig_ci`.b'");
    }

    #[test]
    fn other_opaque_columns_hide_everything() {
        let f = decode(&cols(), "`__sprig_ci`.g IS NOT NULL OR TRUE", Q, lex());
        assert_eq!(f.predicate, "FALSE");
    }

    #[test]
    fn enums_and_sets_read_as_their_members_text() {
        let f = decode(&cols(), "(`__sprig_ci`.st <> 'secret')", Q, lex());
        assert_eq!(
            f.predicate,
            "(CONVERT(ELT(`__sprig_ci`.st + 1, '', 'public', 'secret', 'it''s') USING utf8mb4) \
             COLLATE utf8mb4_general_ci <> 'secret')"
        );
        let f = decode(
            &cols(),
            "FIND_IN_SET('secret', `__sprig_ci`.tags) = 0",
            Q,
            lex(),
        );
        assert_eq!(
            f.predicate,
            "FIND_IN_SET('secret', CONVERT(MAKE_SET(`__sprig_ci`.tags, 'a', 'secret') USING utf8mb4)) = 0"
        );
        for ok in [
            "'public' = `__sprig_ci`.st",
            "`__sprig_ci`.st IN ('public', _utf8mb4'x')",
            "`__sprig_ci`.st NOT IN ('secret')",
            "`__sprig_ci`.st LIKE 'p%' ESCAPE '!'",
            "`__sprig_ci`.st IS NOT NULL AND `__sprig_ci`.owner = 'a'",
            "NOT (`__sprig_ci`.st = 'secret' COLLATE utf8mb4_bin)",
            "CASE WHEN `__sprig_ci`.st = 'a' THEN 1 ELSE 0 END",
            "`__sprig_ci`.st = JSON_VALUE(`__sprig_rls_ctx`(), '$.s.user')",
            "COALESCE(JSON_VALUE(@c, '$.a'), JSON_QUERY(@c, '$.a')) <> `__sprig_ci`.st",
        ] {
            assert_ne!(decode(&cols(), ok, Q, lex()).predicate, "FALSE", "{ok}");
        }
        // Where the table could compare the index, nothing shows.
        for bad in [
            "`__sprig_ci`.st <> 1",
            "`__sprig_ci`.st + 0 = 2",
            "`__sprig_ci`.st = 'a' + 0",
            "`__sprig_ci`.st",
            "NOT `__sprig_ci`.st = 'a'",
            "`__sprig_ci`.st = `__sprig_ci`.owner",
            "`__sprig_ci`.st IN (1, 'a')",
            "`__sprig_ci`.st IS TRUE",
            "CONCAT(`__sprig_ci`.st, '') = 'a'",
            "-`__sprig_ci`.st < 'a'",
            "`__sprig_ci`.tags & 2",
            "`__sprig_ci`.st = COALESCE(JSON_VALUE(@c, '$.a'), 1)",
            "`__sprig_ci`.st = ABS(JSON_VALUE(@c, '$.a'))",
        ] {
            assert_eq!(decode(&cols(), bad, Q, lex()).predicate, "FALSE", "{bad}");
        }
    }

    #[test]
    fn timestamps_move_to_the_session_time_zone() {
        let f = decode(&cols(), "`__sprig_ci`.ts <= NOW()", Q, lex());
        assert_eq!(
            f.predicate,
            "COALESCE(FROM_UNIXTIME(TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', `__sprig_ci`.ts)) \
             + INTERVAL MICROSECOND(`__sprig_ci`.ts) MICROSECOND, `__sprig_ci`.ts) <= NOW()"
        );
        assert!(f.columns.contains("`ts` datetime(3) PATH '$.\"ts\"'"));
    }
}
