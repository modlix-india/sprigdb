//! Retrieval indexes, the statement side (text only):
//!
//! ```sql
//! CREATE RETRIEVAL INDEX [IF NOT EXISTS] kb ON [db.]articles (body)
//!   [TITLE title] [FILTERS (product, lang)]
//!   [CHUNK BY HEADINGS | CHUNK BY TOKENS n]
//!   [EMBED WITH MODEL 'local:bge-small'];
//! DROP RETRIEVAL INDEX [IF EXISTS] [db.]kb;
//! SHOW CREATE RETRIEVAL INDEX [db.]kb;
//! SHOW RETRIEVAL INDEXES [FROM | IN db];
//! SELECT * FROM RETRIEVE([db.]kb, 'reset my password', 8 [, 'product = ''app''']);
//! ```
//!
//! - [`parse`] reads the statements; [`render`] writes the `CREATE` back
//!   in its one canonical form (`SHOW CREATE RETRIEVAL INDEX`).
//! - [`calls`] finds `RETRIEVE(...)` table functions in a statement's
//!   `FROM` lists, with their arguments, which must be literals.
//! - [`compile_filter`] checks a filter and writes it again over other
//!   column names: only the index's `FILTERS` columns, literals,
//!   comparisons, `IN`, `BETWEEN`, `LIKE`, `IS [NOT] NULL`, `AND`, `OR`,
//!   `NOT` and parentheses are allowed, so the same filter can run on the
//!   index's own table and on the source table.

use std::ops::Range;

use crate::lex::{self, LexOptions, Token, TokenKind};

/// The longest name an index or its columns may have.
pub const MAX_NAME: usize = 64;

/// The most columns `FILTERS (...)` may name.
pub const MAX_FILTERS: usize = 16;

/// `CHUNK BY TOKENS n` bounds: the embedding model reads 512 tokens, the
/// title included.
pub const MIN_CHUNK_TOKENS: u32 = 32;
/// See [`MIN_CHUNK_TOKENS`].
pub const MAX_CHUNK_TOKENS: u32 = 480;
/// The chunk size without `CHUNK BY`.
pub const DEFAULT_CHUNK_TOKENS: u32 = 400;

/// The most hits one `RETRIEVE()` may ask for.
pub const MAX_LIMIT: u32 = 100;

/// The longest query text, in characters.
pub const MAX_QUERY: usize = 2000;

/// The longest filter text, in bytes.
pub const MAX_FILTER: usize = 4096;

/// How the source text is cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chunking {
    /// At Markdown headings, long sections cut again between blocks.
    Headings,
    /// Into pieces of about `n` tokens, between paragraphs and sentences.
    Tokens(u32),
}

impl Chunking {
    /// The catalog form: `headings` or `tokens:<n>`.
    #[must_use]
    pub fn as_catalog(self) -> String {
        match self {
            Chunking::Headings => "headings".to_owned(),
            Chunking::Tokens(n) => format!("tokens:{n}"),
        }
    }

    /// From [`Chunking::as_catalog`].
    #[must_use]
    pub fn from_catalog(s: &str) -> Option<Self> {
        if s == "headings" {
            return Some(Chunking::Headings);
        }
        s.strip_prefix("tokens:")?.parse().ok().map(Chunking::Tokens)
    }
}

/// `CREATE RETRIEVAL INDEX`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Create {
    /// `IF NOT EXISTS`.
    pub if_not_exists: bool,
    /// The index.
    pub name: String,
    /// The source table's database, when named.
    pub db: Option<String>,
    /// The source table.
    pub table: String,
    /// The text column.
    pub column: String,
    /// `TITLE`.
    pub title: Option<String>,
    /// `FILTERS (...)`.
    pub filters: Vec<String>,
    /// `CHUNK BY`.
    pub chunking: Chunking,
    /// `EMBED WITH MODEL`.
    pub model: Option<String>,
}

/// A retrieval index statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stmt {
    /// `CREATE RETRIEVAL INDEX`.
    Create(Create),
    /// `DROP RETRIEVAL INDEX [IF EXISTS] [db.]name`.
    Drop {
        /// The database, when named.
        db: Option<String>,
        /// The index.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
    /// `SHOW CREATE RETRIEVAL INDEX [db.]name`.
    ShowCreate {
        /// The database, when named.
        db: Option<String>,
        /// The index.
        name: String,
    },
    /// `SHOW RETRIEVAL INDEXES [FROM | IN db]`.
    Show {
        /// The database, when named.
        db: Option<String>,
    },
}

/// Tokens without trivia, over the statement's bytes.
struct Toks<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
    nbe: bool,
}

impl<'a> Toks<'a> {
    fn new(sql: &'a [u8], opts: &LexOptions) -> Option<Self> {
        let all = lex::lex(sql, opts).ok()?;
        Some(Self {
            sql,
            t: all.into_iter().filter(|k| !k.kind.is_trivia()).collect(),
            nbe: opts.no_backslash_escapes,
        })
    }

    fn word(&self, j: usize, w: &str) -> bool {
        self.t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w.as_bytes())
        })
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t.get(j).is_some_and(|tk| {
            matches!(tk.kind, TokenKind::Punct | TokenKind::Op) && tk.bytes(self.sql) == [c]
        })
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    fn string(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind == TokenKind::String)
            .and_then(|tk| lex::string_value(tk.bytes(self.sql), self.nbe))
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    fn number(&self, j: usize) -> Option<u64> {
        self.t
            .get(j)
            .filter(|tk| tk.kind == TokenKind::Number)
            .and_then(|tk| std::str::from_utf8(tk.bytes(self.sql)).ok()?.parse().ok())
    }

    /// `[db.]name` at `j`: (db, name, next token).
    fn name(&self, j: usize) -> Option<(Option<String>, String, usize)> {
        let first = self.ident(j)?;
        if self.punct(j + 1, b'.') {
            let second = self.ident(j + 2)?;
            Some((Some(first), second, j + 3))
        } else {
            Some((None, first, j + 1))
        }
    }

    /// The `)` matching the `(` at `from`.
    fn matching(&self, from: usize) -> Option<usize> {
        let mut d = 0i32;
        for k in from..self.t.len() {
            if self.punct(k, b'(') {
                d += 1;
            } else if self.punct(k, b')') {
                d -= 1;
                if d == 0 {
                    return Some(k);
                }
            }
        }
        None
    }
}

fn check_name(what: &str, n: &str) -> Result<(), String> {
    if n.is_empty() || n.chars().count() > MAX_NAME {
        return Err(format!("{what} needs a name of 1 to {MAX_NAME} characters"));
    }
    if n.to_ascii_lowercase().starts_with("_sprig_") || n.starts_with("__sprig_") {
        return Err(format!("{what} '{n}' uses the reserved prefix _sprig_"));
    }
    Ok(())
}

/// Whether `sql` may be a retrieval index statement (a cheap test before
/// [`parse`]).
#[must_use]
pub fn mentions_ddl(sql: &[u8]) -> bool {
    sql.windows(9)
        .any(|w| w.eq_ignore_ascii_case(b"RETRIEVAL"))
}

/// The statement in `sql`: `None` when it isn't a retrieval index
/// statement, `Some(Err(why))` when it is one written wrongly.
#[must_use]
pub fn parse(sql: &[u8], opts: &LexOptions) -> Option<Result<Stmt, String>> {
    if !mentions_ddl(sql) {
        return None;
    }
    let t = Toks::new(sql, opts)?;
    let mut end = t.t.len();
    if end > 0 && t.punct(end - 1, b';') {
        end -= 1;
    }
    if (0..end).any(|j| t.punct(j, b';')) {
        return None;
    }
    if t.word(0, "CREATE") && t.word(1, "RETRIEVAL") && t.word(2, "INDEX") {
        return Some(create(&t, 3, end).map(Stmt::Create));
    }
    if t.word(0, "DROP") && t.word(1, "RETRIEVAL") && t.word(2, "INDEX") {
        let mut j = 3;
        let if_exists = t.word(j, "IF") && t.word(j + 1, "EXISTS");
        if if_exists {
            j += 2;
        }
        return Some(match t.name(j) {
            Some((db, name, next)) if next == end => Ok(Stmt::Drop {
                db,
                name,
                if_exists,
            }),
            _ => Err("DROP RETRIEVAL INDEX [IF EXISTS] [db.]name".into()),
        });
    }
    if t.word(0, "SHOW") && t.word(1, "CREATE") && t.word(2, "RETRIEVAL") && t.word(3, "INDEX") {
        return Some(match t.name(4) {
            Some((db, name, next)) if next == end => Ok(Stmt::ShowCreate { db, name }),
            _ => Err("SHOW CREATE RETRIEVAL INDEX [db.]name".into()),
        });
    }
    if t.word(0, "SHOW") && t.word(1, "RETRIEVAL") && t.word(2, "INDEXES") {
        if end == 3 {
            return Some(Ok(Stmt::Show { db: None }));
        }
        return Some(
            if (t.word(3, "FROM") || t.word(3, "IN")) && end == 5 {
                t.ident(4)
                    .map(|db| Stmt::Show { db: Some(db) })
                    .ok_or_else(|| "SHOW RETRIEVAL INDEXES [FROM db]".to_owned())
            } else {
                Err("SHOW RETRIEVAL INDEXES [FROM db]".into())
            },
        );
    }
    None
}

#[allow(clippy::too_many_lines)] // one step per clause
fn create(t: &Toks<'_>, mut j: usize, end: usize) -> Result<Create, String> {
    let shape = "CREATE RETRIEVAL INDEX name ON [db.]table (column) [TITLE column] \
                 [FILTERS (column, ...)] [CHUNK BY HEADINGS | CHUNK BY TOKENS n] \
                 [EMBED WITH MODEL 'alias']";
    let if_not_exists = t.word(j, "IF") && t.word(j + 1, "NOT") && t.word(j + 2, "EXISTS");
    if if_not_exists {
        j += 3;
    }
    let name = t.ident(j).ok_or_else(|| shape.to_owned())?;
    check_name("a retrieval index", &name)?;
    if t.punct(j + 1, b'.') {
        return Err(
            "a retrieval index belongs to its table's database: name the database on the table"
                .into(),
        );
    }
    j += 1;
    if !t.word(j, "ON") {
        return Err(shape.into());
    }
    let (db, table, next) = t.name(j + 1).ok_or_else(|| shape.to_owned())?;
    j = next;
    if !t.punct(j, b'(') {
        return Err(shape.into());
    }
    let column = t.ident(j + 1).ok_or_else(|| shape.to_owned())?;
    if !t.punct(j + 2, b')') {
        return Err("a retrieval index reads one text column: ON table (column)".into());
    }
    j += 3;
    let mut c = Create {
        if_not_exists,
        name,
        db,
        table,
        column,
        title: None,
        filters: Vec::new(),
        chunking: Chunking::Tokens(DEFAULT_CHUNK_TOKENS),
        model: None,
    };
    let mut seen: Vec<&str> = Vec::new();
    while j < end {
        let clause = if t.word(j, "TITLE") {
            "TITLE"
        } else if t.word(j, "FILTERS") {
            "FILTERS"
        } else if t.word(j, "CHUNK") {
            "CHUNK"
        } else if t.word(j, "EMBED") {
            "EMBED"
        } else {
            return Err(shape.into());
        };
        if seen.contains(&clause) {
            return Err(format!("{clause} is given twice"));
        }
        seen.push(clause);
        match clause {
            "TITLE" => {
                c.title = Some(t.ident(j + 1).ok_or_else(|| "TITLE column".to_owned())?);
                j += 2;
            }
            "FILTERS" => {
                if !t.punct(j + 1, b'(') {
                    return Err("FILTERS (column, ...)".into());
                }
                let close = t
                    .matching(j + 1)
                    .filter(|&k| k <= end)
                    .ok_or_else(|| "FILTERS (column, ...)".to_owned())?;
                let mut k = j + 2;
                while k < close {
                    let col = t
                        .ident(k)
                        .ok_or_else(|| "FILTERS (column, ...)".to_owned())?;
                    if c.filters.iter().any(|f| f.eq_ignore_ascii_case(&col)) {
                        return Err(format!("FILTERS names {col} twice"));
                    }
                    c.filters.push(col);
                    k += 1;
                    if t.punct(k, b',') {
                        k += 1;
                    } else if k != close {
                        return Err("FILTERS (column, ...)".into());
                    }
                }
                if c.filters.is_empty() {
                    return Err("FILTERS needs at least one column".into());
                }
                if c.filters.len() > MAX_FILTERS {
                    return Err(format!("FILTERS takes at most {MAX_FILTERS} columns"));
                }
                j = close + 1;
            }
            "CHUNK" => {
                if !t.word(j + 1, "BY") {
                    return Err("CHUNK BY HEADINGS | CHUNK BY TOKENS n".into());
                }
                if t.word(j + 2, "HEADINGS") {
                    c.chunking = Chunking::Headings;
                    j += 3;
                } else if t.word(j + 2, "TOKENS") {
                    let n = t
                        .number(j + 3)
                        .and_then(|n| u32::try_from(n).ok())
                        .filter(|n| (MIN_CHUNK_TOKENS..=MAX_CHUNK_TOKENS).contains(n))
                        .ok_or_else(|| {
                            format!(
                                "CHUNK BY TOKENS takes {MIN_CHUNK_TOKENS} to {MAX_CHUNK_TOKENS}"
                            )
                        })?;
                    c.chunking = Chunking::Tokens(n);
                    j += 4;
                } else {
                    return Err("CHUNK BY HEADINGS | CHUNK BY TOKENS n".into());
                }
            }
            _ => {
                if !(t.word(j + 1, "WITH") && t.word(j + 2, "MODEL")) {
                    return Err("EMBED WITH MODEL 'alias'".into());
                }
                c.model = Some(
                    t.string(j + 3)
                        .filter(|m| !m.trim().is_empty())
                        .ok_or_else(|| "EMBED WITH MODEL 'alias'".to_owned())?,
                );
                j += 4;
            }
        }
    }
    for (what, col) in std::iter::once(("the text column", &c.column))
        .chain(c.title.iter().map(|t| ("the title column", t)))
        .chain(c.filters.iter().map(|f| ("a filter column", f)))
    {
        check_name(what, col)?;
    }
    if c.title
        .as_deref()
        .is_some_and(|t| t.eq_ignore_ascii_case(&c.column))
    {
        return Err("TITLE must be another column than the text".into());
    }
    Ok(c)
}

/// `name` quoted as an identifier.
#[must_use]
pub fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// What [`render`] prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shape<'a> {
    /// The index.
    pub name: &'a str,
    /// Its database.
    pub db: &'a str,
    /// The source table.
    pub table: &'a str,
    /// The text column.
    pub column: &'a str,
    /// The title column.
    pub title: Option<&'a str>,
    /// The filter columns.
    pub filters: &'a [String],
    /// The chunking.
    pub chunking: Chunking,
    /// The model alias.
    pub model: &'a str,
}

/// The canonical `CREATE RETRIEVAL INDEX` of an index (every clause, so a
/// reader sees the defaults it got).
#[must_use]
pub fn render(s: &Shape<'_>) -> String {
    let mut out = format!(
        "CREATE RETRIEVAL INDEX {} ON {}.{} ({})",
        quote(s.name),
        quote(s.db),
        quote(s.table),
        quote(s.column)
    );
    if let Some(t) = s.title {
        out.push_str(" TITLE ");
        out.push_str(&quote(t));
    }
    if !s.filters.is_empty() {
        let f: Vec<String> = s.filters.iter().map(|f| quote(f)).collect();
        out.push_str(&format!(" FILTERS ({})", f.join(", ")));
    }
    match s.chunking {
        Chunking::Headings => out.push_str(" CHUNK BY HEADINGS"),
        Chunking::Tokens(n) => out.push_str(&format!(" CHUNK BY TOKENS {n}")),
    }
    out.push_str(&format!(
        " EMBED WITH MODEL '{}'",
        s.model.replace('\\', "\\\\").replace('\'', "''")
    ));
    out
}

// ---- RETRIEVE() ----------------------------------------------------------------

/// One `RETRIEVE(...)` in a statement's `FROM` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The bytes from `RETRIEVE` to its `)`.
    pub span: Range<usize>,
    /// The index's database, when named.
    pub db: Option<String>,
    /// The index.
    pub index: String,
    /// The query text.
    pub query: String,
    /// Hits wanted.
    pub limit: u32,
    /// The filter, when given (and not blank).
    pub filter: Option<String>,
    /// Whether an alias follows the call.
    pub aliased: bool,
}

/// A call written wrongly: a MariaDB error number and its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    /// 1582 (argument count), 1210 (an argument), 1235 (where it stands).
    pub code: u16,
    /// What is wrong.
    pub message: String,
}

impl CallError {
    fn new(code: u16, m: impl Into<String>) -> Self {
        Self {
            code,
            message: m.into(),
        }
    }
}

/// Whether `sql` may call `RETRIEVE` (a cheap test before [`calls`]).
#[must_use]
pub fn mentions(sql: &[u8]) -> bool {
    sql.windows(8).any(|w| w.eq_ignore_ascii_case(b"RETRIEVE"))
}

/// Words after which a word is no alias.
const NOT_ALIAS: &[&str] = &[
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "STRAIGHT_JOIN",
    "FULL",
    "ON",
    "USING",
    "GROUP",
    "ORDER",
    "HAVING",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "FOR",
    "LOCK",
    "INTO",
    "PROCEDURE",
    "OFFSET",
];

/// Words that end a `FROM` list, looking back from a call.
const CLAUSE_WORDS: &[&str] = &[
    "SELECT", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "ON", "USING", "SET", "VALUES",
    "WINDOW",
];

/// Whether the call at token `j` stands in a `FROM` list: after `FROM`,
/// `JOIN`, or a comma of a `FROM` list.
fn in_from(t: &Toks<'_>, j: usize) -> bool {
    let Some(p) = j.checked_sub(1) else {
        return false;
    };
    if t.word(p, "FROM") || t.word(p, "JOIN") || t.word(p, "STRAIGHT_JOIN") {
        return true;
    }
    if !t.punct(p, b',') {
        return false;
    }
    // Back at the same depth to the clause the comma is in.
    let mut d = 0i32;
    let mut k = p;
    while k > 0 {
        k -= 1;
        if t.punct(k, b')') {
            d += 1;
        } else if t.punct(k, b'(') {
            if d == 0 {
                return false;
            }
            d -= 1;
        } else if d == 0 {
            if t.word(k, "FROM") {
                return true;
            }
            if CLAUSE_WORDS.iter().any(|w| t.word(k, w)) {
                return false;
            }
        }
    }
    false
}

/// The `RETRIEVE(...)` calls standing in `FROM` lists, in text order.
/// A `RETRIEVE(` anywhere else is left alone (a stored function of that
/// name, qualified or not, keeps working).
///
/// # Errors
/// [`CallError`] for a call in a `FROM` list written wrongly.
pub fn calls(sql: &[u8], opts: &LexOptions) -> Result<Vec<Call>, CallError> {
    if !mentions(sql) {
        return Ok(Vec::new());
    }
    let Some(t) = Toks::new(sql, opts) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut j = 0;
    while j < t.t.len() {
        if !(t.word(j, "RETRIEVE") && t.punct(j + 1, b'(')) || (j > 0 && t.punct(j - 1, b'.')) {
            j += 1;
            continue;
        }
        if !in_from(&t, j) {
            j += 1;
            continue;
        }
        let close = t
            .matching(j + 1)
            .ok_or_else(|| CallError::new(1064, "RETRIEVE( has no closing parenthesis"))?;
        // Arguments split at top-level commas.
        let mut args: Vec<(usize, usize)> = Vec::new();
        let mut start = j + 2;
        let mut d = 0i32;
        for k in j + 2..close {
            if t.punct(k, b'(') {
                d += 1;
            } else if t.punct(k, b')') {
                d -= 1;
            } else if d == 0 && t.punct(k, b',') {
                args.push((start, k));
                start = k + 1;
            }
        }
        if start < close || !args.is_empty() {
            args.push((start, close));
        }
        if !(3..=4).contains(&args.len()) {
            return Err(CallError::new(
                1582,
                "Incorrect parameter count in the call to native function 'RETRIEVE': \
                 RETRIEVE(index, 'query', limit [, 'filter'])",
            ));
        }
        let one = |(a, b): (usize, usize)| b == a + 1;
        let (db, index) = match args[0] {
            (a, b) if b == a + 1 => (None, t.ident(a)),
            (a, b) if b == a + 3 && t.punct(a + 1, b'.') => (t.ident(a), t.ident(a + 2)),
            _ => (None, None),
        };
        let index = index.ok_or_else(|| {
            CallError::new(
                1210,
                "RETRIEVE's first argument names a retrieval index: RETRIEVE(kb, ...)",
            )
        })?;
        let query = one(args[1])
            .then(|| t.string(args[1].0))
            .flatten()
            .ok_or_else(|| {
                CallError::new(1210, "RETRIEVE's query must be a string literal")
            })?;
        if query.trim().is_empty() {
            return Err(CallError::new(1210, "RETRIEVE's query is empty"));
        }
        if query.chars().count() > MAX_QUERY {
            return Err(CallError::new(
                1210,
                format!("RETRIEVE's query is longer than {MAX_QUERY} characters"),
            ));
        }
        let limit = one(args[2])
            .then(|| t.number(args[2].0))
            .flatten()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| {
                CallError::new(
                    1210,
                    format!("RETRIEVE's limit must be a number from 1 to {MAX_LIMIT}"),
                )
            })?;
        let filter = match args.get(3) {
            None => None,
            Some(&a) if one(a) && t.t[a.0].kind == TokenKind::String => t
                .string(a.0)
                .filter(|f| !f.trim().is_empty()),
            Some(&a) if one(a) && t.word(a.0, "NULL") => None,
            Some(_) => {
                return Err(CallError::new(
                    1210,
                    "RETRIEVE's filter must be a string literal",
                ));
            }
        };
        let aliased = t.word(close + 1, "AS")
            || (t.t.get(close + 1).is_some_and(|tk| tk.kind.is_ident_like())
                && !NOT_ALIAS.iter().any(|w| t.word(close + 1, w)));
        out.push(Call {
            span: t.t[j].start as usize..t.t[close].end as usize,
            db,
            index,
            query,
            limit,
            filter,
            aliased,
        });
        j = close + 1;
    }
    Ok(out)
}

/// Words a filter may use besides column names.
const FILTER_WORDS: &[&str] = &[
    "AND", "OR", "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE", "TRUE", "FALSE",
];

/// Operators a filter may use.
const FILTER_OPS: &[&str] = &[
    "=", "<>", "!=", "<", ">", "<=", ">=", "<=>", "+", "-", "*", "/", "%",
];

/// Checks `filter` (lexed with `opts`) and writes it again with each
/// column name `c` (one of `columns`, any case) as `rename(c)` and string
/// literals escaped for a session whose `NO_BACKSLASH_ESCAPES` is
/// `out_nbe`.
///
/// # Errors
/// What isn't allowed: another column, a function, a subquery, a variable,
/// a placeholder, unbalanced parentheses.
pub fn compile_filter(
    filter: &str,
    opts: &LexOptions,
    columns: &[String],
    rename: &dyn Fn(&str) -> String,
    out_nbe: bool,
) -> Result<String, String> {
    if filter.len() > MAX_FILTER {
        return Err(format!("a filter is longer than {MAX_FILTER} bytes"));
    }
    let t = Toks::new(filter.as_bytes(), opts).ok_or_else(|| "a filter that can't be read".to_owned())?;
    let mut out: Vec<String> = Vec::with_capacity(t.t.len());
    let mut depth = 0i32;
    for (j, tk) in t.t.iter().enumerate() {
        let b = tk.bytes(t.sql);
        let s = String::from_utf8_lossy(b);
        match tk.kind {
            TokenKind::Word | TokenKind::QuotedIdent => {
                let upper = s.to_ascii_uppercase();
                if tk.kind == TokenKind::Word && FILTER_WORDS.contains(&upper.as_str()) {
                    out.push(upper);
                    continue;
                }
                let name = t.ident(j).unwrap_or_default();
                if t.punct(j + 1, b'(') {
                    return Err(format!(
                        "a filter can't call functions ({name}); it compares FILTERS columns with values"
                    ));
                }
                let Some(c) = columns.iter().find(|c| c.eq_ignore_ascii_case(&name)) else {
                    return Err(format!(
                        "Unknown column '{name}' in the filter: a filter names the index's FILTERS columns{}",
                        if columns.is_empty() {
                            " (it has none)".to_owned()
                        } else {
                            format!(" ({})", columns.join(", "))
                        }
                    ));
                };
                out.push(rename(c));
            }
            TokenKind::String => {
                let v = t.string(j).ok_or_else(|| "a filter string that can't be read".to_owned())?;
                out.push(crate::ai_fn::string_literal(&v, out_nbe));
            }
            TokenKind::Number | TokenKind::HexNumber | TokenKind::HexString => {
                out.push(s.into_owned());
            }
            TokenKind::Punct | TokenKind::Op => {
                match &*s {
                    "(" => depth += 1,
                    ")" => {
                        depth -= 1;
                        if depth < 0 {
                            return Err("a filter has unbalanced parentheses".into());
                        }
                    }
                    "," => {}
                    o if FILTER_OPS.contains(&o) => {}
                    o => return Err(format!("a filter can't use '{o}'")),
                }
                out.push(s.into_owned());
            }
            _ => return Err(format!("a filter can't use '{s}'")),
        }
    }
    if depth != 0 {
        return Err("a filter has unbalanced parentheses".into());
    }
    if out.is_empty() {
        return Err("the filter is empty".into());
    }
    Ok(out.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Option<Result<Stmt, String>> {
        parse(s.as_bytes(), &LexOptions::mariadb())
    }

    #[test]
    fn create_parses_every_clause_and_renders_back() {
        let Some(Ok(Stmt::Create(c))) = p(
            "create retrieval index kb on support.articles (body) TITLE title \
             FILTERS (product, `lang`) CHUNK BY TOKENS 300 EMBED WITH MODEL 'local:bge-small';",
        ) else {
            panic!("not parsed");
        };
        assert_eq!(c.name, "kb");
        assert_eq!(c.db.as_deref(), Some("support"));
        assert_eq!(c.table, "articles");
        assert_eq!(c.column, "body");
        assert_eq!(c.title.as_deref(), Some("title"));
        assert_eq!(c.filters, ["product", "lang"]);
        assert_eq!(c.chunking, Chunking::Tokens(300));
        assert_eq!(c.model.as_deref(), Some("local:bge-small"));
        let shown = render(&Shape {
            name: &c.name,
            db: "support",
            table: &c.table,
            column: &c.column,
            title: c.title.as_deref(),
            filters: &c.filters,
            chunking: c.chunking,
            model: "local:bge-small",
        });
        assert_eq!(
            shown,
            "CREATE RETRIEVAL INDEX `kb` ON `support`.`articles` (`body`) TITLE `title` \
             FILTERS (`product`, `lang`) CHUNK BY TOKENS 300 EMBED WITH MODEL 'local:bge-small'"
        );
        // The rendering parses to the same index.
        let Some(Ok(Stmt::Create(again))) = p(&shown) else {
            panic!("rendering not parsed");
        };
        assert_eq!(
            Create {
                if_not_exists: false,
                ..c
            },
            again
        );
    }

    #[test]
    fn create_defaults_and_refusals() {
        let Some(Ok(Stmt::Create(c))) = p("CREATE RETRIEVAL INDEX IF NOT EXISTS kb ON a (b)") else {
            panic!();
        };
        assert!(c.if_not_exists);
        assert_eq!(c.chunking, Chunking::Tokens(DEFAULT_CHUNK_TOKENS));
        assert!(c.model.is_none() && c.title.is_none() && c.filters.is_empty());
        let Some(Ok(Stmt::Create(h))) = p("CREATE RETRIEVAL INDEX kb ON a (b) CHUNK BY HEADINGS")
        else {
            panic!();
        };
        assert_eq!(h.chunking, Chunking::Headings);
        for bad in [
            "CREATE RETRIEVAL INDEX kb ON a (b, c)",
            "CREATE RETRIEVAL INDEX kb ON a",
            "CREATE RETRIEVAL INDEX db.kb ON a (b)",
            "CREATE RETRIEVAL INDEX kb ON a (b) CHUNK BY TOKENS 5000",
            "CREATE RETRIEVAL INDEX kb ON a (b) CHUNK BY TOKENS 10",
            "CREATE RETRIEVAL INDEX kb ON a (b) TITLE b",
            "CREATE RETRIEVAL INDEX kb ON a (b) TITLE t TITLE u",
            "CREATE RETRIEVAL INDEX kb ON a (b) FILTERS ()",
            "CREATE RETRIEVAL INDEX kb ON a (b) FILTERS (x, x)",
            "CREATE RETRIEVAL INDEX kb ON a (b) EMBED WITH MODEL ''",
            "CREATE RETRIEVAL INDEX _sprig_x ON a (b)",
            "CREATE RETRIEVAL INDEX kb ON a (b) WHATEVER",
        ] {
            assert!(matches!(p(bad), Some(Err(_))), "{bad}");
        }
        assert!(p("CREATE TABLE retrieval (a INT)").is_none());
    }

    #[test]
    fn drop_show_and_show_create() {
        assert_eq!(
            p("DROP RETRIEVAL INDEX IF EXISTS d.kb").unwrap().unwrap(),
            Stmt::Drop {
                db: Some("d".into()),
                name: "kb".into(),
                if_exists: true
            }
        );
        assert_eq!(
            p("show create retrieval index kb").unwrap().unwrap(),
            Stmt::ShowCreate {
                db: None,
                name: "kb".into()
            }
        );
        assert_eq!(
            p("SHOW RETRIEVAL INDEXES FROM shop").unwrap().unwrap(),
            Stmt::Show {
                db: Some("shop".into())
            }
        );
        assert_eq!(
            p("SHOW RETRIEVAL INDEXES").unwrap().unwrap(),
            Stmt::Show { db: None }
        );
        assert!(matches!(p("DROP RETRIEVAL INDEX"), Some(Err(_))));
    }

    #[test]
    fn retrieve_calls_in_from_lists() {
        let o = LexOptions::mariadb();
        let sql = "SELECT r.id, a.lang FROM RETRIEVE(kb, 'reset my password', 8, \
                   'product = ''app''') AS r JOIN articles a USING (id)";
        let c = calls(sql.as_bytes(), &o).unwrap();
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].index, "kb");
        assert_eq!(c[0].query, "reset my password");
        assert_eq!(c[0].limit, 8);
        assert_eq!(c[0].filter.as_deref(), Some("product = 'app'"));
        assert!(c[0].aliased);
        assert_eq!(
            &sql[c[0].span.clone()],
            "RETRIEVE(kb, 'reset my password', 8, 'product = ''app''')"
        );
        let c = calls(b"select * from retrieve(d.kb, 'x', 3) where score > 0", &o).unwrap();
        assert_eq!(c[0].db.as_deref(), Some("d"));
        assert!(!c[0].aliased);
        let c = calls(b"SELECT * FROM t, RETRIEVE(kb, 'x', 3, NULL) x", &o).unwrap();
        assert_eq!(c.len(), 1);
        assert!(c[0].filter.is_none());
        // Not in a FROM list, or qualified: not ours.
        assert!(calls(b"SELECT RETRIEVE(kb, 'x', 3)", &o).unwrap().is_empty());
        assert!(calls(b"SELECT * FROM t WHERE a IN (1, RETRIEVE(1))", &o).unwrap().is_empty());
        assert!(calls(b"SELECT * FROM db.retrieve(1)", &o).unwrap().is_empty());
        for (bad, code) in [
            ("SELECT * FROM RETRIEVE(kb, 'x')", 1582),
            ("SELECT * FROM RETRIEVE(kb, 'x', 0)", 1210),
            ("SELECT * FROM RETRIEVE(kb, 'x', 101)", 1210),
            ("SELECT * FROM RETRIEVE(kb, CONCAT('x'), 3)", 1210),
            ("SELECT * FROM RETRIEVE(kb, ?, 3)", 1210),
            ("SELECT * FROM RETRIEVE('kb', 'x', 3)", 1210),
            ("SELECT * FROM RETRIEVE(kb, '  ', 3)", 1210),
            ("SELECT * FROM RETRIEVE(kb, 'x', 3, 7)", 1210),
        ] {
            assert_eq!(calls(bad.as_bytes(), &o).unwrap_err().code, code, "{bad}");
        }
    }

    #[test]
    fn filters_allow_comparisons_over_their_columns_only() {
        let o = LexOptions::mariadb();
        let cols = vec!["product".to_owned(), "lang".to_owned()];
        let f = |s: &str| compile_filter(s, &o, &cols, &|c| quote(&format!("f_{c}")), false);
        assert_eq!(
            f("Product = 'app' AND (lang IN ('en', 'de') OR lang IS NULL)").unwrap(),
            "`f_product` = 'app' AND ( `f_lang` IN ( 'en' , 'de' ) OR `f_lang` IS NULL )"
        );
        assert_eq!(
            f("product LIKE 'a\\'b%' /* c */").unwrap(),
            "`f_product` LIKE 'a''b%'"
        );
        assert_eq!(f("NOT lang <=> 'x'").unwrap(), "NOT `f_lang` <=> 'x'");
        for bad in [
            "price > 3",
            "lang = SLEEP(1)",
            "lang = (SELECT 1)",
            "lang = @x",
            "lang = ?",
            "lang = 'x'; DROP TABLE t",
            "(lang = 'x'",
            "lang = 'x')",
            "t.lang = 'x'",
            "",
        ] {
            assert!(f(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn chunking_catalog_form() {
        for c in [Chunking::Headings, Chunking::Tokens(400)] {
            assert_eq!(Chunking::from_catalog(&c.as_catalog()), Some(c));
        }
        assert_eq!(Chunking::from_catalog("tokens:x"), None);
    }
}
