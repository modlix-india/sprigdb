//! `ANALYZE TABLE` on engine tables: the forms each server takes after the
//! table list, and the answer rows each gives (taken from both servers'
//! answers, black box).
//!
//! ```text
//! both      ANALYZE [NO_WRITE_TO_BINLOG|LOCAL] TABLE t[, ...]
//!             collects; per table: status OK
//! MariaDB   ... PERSISTENT FOR ALL
//!           ... PERSISTENT FOR COLUMNS (c, ... | ALL) INDEXES (x, ... | ALL)
//!             collects; status "Engine-independent statistics collected",
//!             for ALL columns a Warning per BLOB or TEXT column ("are not
//!             collected for column"), then status OK; a column or index
//!             the table hasn't: error "Invalid argument" instead of OK
//!           UPDATE/DROP HISTOGRAM: 1064
//! MySQL     ... UPDATE HISTOGRAM ON c, ... [WITH n BUCKETS] [MANUAL|AUTO UPDATE]
//!             one table only; Op "histogram"; per column, by name:
//!             "Histogram statistics created for column 'c'." or an Error
//!             (no such column, unsupported type, covered by a single-part
//!             unique index); n outside 1..=1024: 1690; a column twice: 1060
//!           ... DROP HISTOGRAM ON c, ...
//!             "Histogram statistics removed for column 'c'." or Error "No
//!             histogram statistics found for column 'c'."
//!           ... USING DATA '...': 1235 (not taken)
//!           PERSISTENT FOR: 1064
//! ```
//!
//! The engine always keeps figures and histograms of every column it can
//! order; the MySQL forms set a column's bucket count and whether a
//! histogram is kept at all, and `UPDATE` makes the figures again at once.

use sprig_common::{Result, SprigError};
use sprig_plan::stats::{MAX_BUCKETS, hist_column};
use sprig_sql::lex::{Token, TokenKind};

use super::super::table::TableDef;
use super::collect::{self, Ask};

/// What follows the table list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Form {
    /// Nothing.
    Plain,
    /// MariaDB's `PERSISTENT FOR ...`: `None` for `ALL`.
    Persistent {
        columns: Option<Vec<String>>,
        indexes: Option<Vec<String>>,
    },
    /// MySQL's `UPDATE HISTOGRAM`.
    Update {
        columns: Vec<String>,
        buckets: u16,
        manual: bool,
    },
    /// MySQL's `DROP HISTOGRAM`.
    Drop { columns: Vec<String> },
}

fn word(toks: &[Token], sql: &[u8], i: usize) -> Option<Vec<u8>> {
    toks.get(i)
        .filter(|t| t.kind == TokenKind::Word)
        .map(|t| t.bytes(sql).to_ascii_uppercase())
}

fn punct(toks: &[Token], sql: &[u8], i: usize, c: &[u8]) -> bool {
    toks.get(i)
        .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == c)
}

fn ident(t: &Token, sql: &[u8]) -> Option<String> {
    let b = t.bytes(sql);
    match t.kind {
        TokenKind::Word => Some(String::from_utf8_lossy(b).into_owned()),
        TokenKind::QuotedIdent if b.len() >= 2 => {
            Some(String::from_utf8_lossy(&b[1..b.len() - 1]).replace("``", "`"))
        }
        _ => None,
    }
}

/// The servers' syntax error, near the token at `i` (or the end).
fn syntax(toks: &[Token], sql: &[u8], i: usize, mysql: bool) -> SprigError {
    let at = toks.get(i).map_or(sql.len(), |t| t.start as usize);
    let rest = String::from_utf8_lossy(sql.get(at..).unwrap_or_default()).into_owned();
    let rest = rest.trim_end().trim_end_matches(';').trim_end();
    let shown: String = rest.chars().take(80).collect();
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             {} server version for the right syntax to use near '{shown}' at line 1",
            if mysql { "MySQL" } else { "MariaDB" }
        ),
    )
}

/// Names separated by commas from `i`; the position after them.
fn names(toks: &[Token], sql: &[u8], mut i: usize) -> Option<(Vec<String>, usize)> {
    let mut out = Vec::new();
    loop {
        out.push(ident(toks.get(i)?, sql)?);
        i += 1;
        if !punct(toks, sql, i, b",") {
            return Some((out, i));
        }
        i += 1;
    }
}

/// `( name, ... )`, `( )` or `ALL` from `i`: `None` for `ALL`.
fn list(toks: &[Token], sql: &[u8], i: usize) -> Option<(Option<Vec<String>>, usize)> {
    if word(toks, sql, i).as_deref() == Some(b"ALL") {
        return Some((None, i + 1));
    }
    if !punct(toks, sql, i, b"(") {
        return None;
    }
    if punct(toks, sql, i + 1, b")") {
        return Some((Some(Vec::new()), i + 2));
    }
    let (n, j) = names(toks, sql, i + 1)?;
    punct(toks, sql, j, b")").then_some((Some(n), j + 1))
}

fn at_end(toks: &[Token], sql: &[u8], i: usize) -> bool {
    toks.get(i).is_none() || (punct(toks, sql, i, b";") && toks.get(i + 1).is_none())
}

impl Form {
    /// The form of the words from `i` (after the table list) of an
    /// `ANALYZE TABLE` on the port of `mysql`. `None`: not a form read
    /// here (the statement goes on as before); an error: the server's
    /// error for it.
    #[allow(clippy::too_many_lines)] // one arm per form, as each server reads it
    pub(crate) fn parse(toks: &[Token], sql: &[u8], i: usize, mysql: bool) -> Option<Result<Self>> {
        if at_end(toks, sql, i) {
            return Some(Ok(Self::Plain));
        }
        let w = word(toks, sql, i)?;
        match (w.as_slice(), mysql) {
            (b"PERSISTENT", false) => {
                if word(toks, sql, i + 1).as_deref() != Some(b"FOR") {
                    return Some(Err(syntax(toks, sql, i + 1, mysql)));
                }
                let mut j = i + 2;
                let mut columns = None;
                let mut indexes = None;
                if word(toks, sql, j).as_deref() == Some(b"ALL") {
                    j += 1;
                } else {
                    // `COLUMNS (...)` and then `INDEXES (...)`, both.
                    if word(toks, sql, j).as_deref() != Some(b"COLUMNS") {
                        return Some(Err(syntax(toks, sql, j, mysql)));
                    }
                    let Some((c, k)) = list(toks, sql, j + 1) else {
                        return Some(Err(syntax(toks, sql, j + 1, mysql)));
                    };
                    columns = c;
                    j = k;
                    if word(toks, sql, j).as_deref() != Some(b"INDEXES") {
                        return Some(Err(syntax(toks, sql, j, mysql)));
                    }
                    let Some((x, k)) = list(toks, sql, j + 1) else {
                        return Some(Err(syntax(toks, sql, j + 1, mysql)));
                    };
                    indexes = x;
                    j = k;
                }
                if !at_end(toks, sql, j) {
                    return Some(Err(syntax(toks, sql, j, mysql)));
                }
                Some(Ok(Self::Persistent { columns, indexes }))
            }
            (b"UPDATE" | b"DROP", true)
                if word(toks, sql, i + 1).as_deref() == Some(b"HISTOGRAM")
                    && word(toks, sql, i + 2).as_deref() == Some(b"ON") =>
            {
                let Some((columns, mut j)) = names(toks, sql, i + 3) else {
                    return Some(Err(syntax(toks, sql, i + 3, mysql)));
                };
                let mut seen: Vec<String> = Vec::new();
                for c in &columns {
                    if seen.iter().any(|s| s.eq_ignore_ascii_case(c)) {
                        return Some(Err(SprigError::standard(
                            1060,
                            *b"42S21",
                            format!("Duplicate column name '{c}'"),
                        )));
                    }
                    seen.push(c.clone());
                }
                if w.as_slice() == b"DROP" {
                    if !at_end(toks, sql, j) {
                        return Some(Err(syntax(toks, sql, j, mysql)));
                    }
                    return Some(Ok(Self::Drop { columns }));
                }
                let mut buckets = 0u16;
                let mut manual = false;
                if word(toks, sql, j).as_deref() == Some(b"USING") {
                    return Some(Err(SprigError::standard(
                        1235,
                        *b"42000",
                        "This version of SprigDB doesn't yet support 'ANALYZE TABLE ... UPDATE \
                         HISTOGRAM ... USING DATA'",
                    )));
                }
                if word(toks, sql, j).as_deref() == Some(b"WITH") {
                    let n = toks
                        .get(j + 1)
                        .filter(|t| t.kind == TokenKind::Number)
                        .and_then(|t| std::str::from_utf8(t.bytes(sql)).ok())
                        .and_then(|s| s.parse::<u64>().ok());
                    let Some(n) = n else {
                        return Some(Err(syntax(toks, sql, j + 1, mysql)));
                    };
                    if word(toks, sql, j + 2).as_deref() != Some(b"BUCKETS") {
                        return Some(Err(syntax(toks, sql, j + 2, mysql)));
                    }
                    if n == 0 || n > MAX_BUCKETS as u64 {
                        return Some(Err(SprigError::standard(
                            1690,
                            *b"22003",
                            "Number of buckets value is out of range in 'ANALYZE TABLE'",
                        )));
                    }
                    buckets = u16::try_from(n).unwrap_or(u16::MAX);
                    j += 3;
                }
                match (
                    word(toks, sql, j).as_deref(),
                    word(toks, sql, j + 1).as_deref(),
                ) {
                    (Some(b"MANUAL"), Some(b"UPDATE")) => {
                        manual = true;
                        j += 2;
                    }
                    (Some(b"AUTO"), Some(b"UPDATE")) => j += 2,
                    _ => {}
                }
                if !at_end(toks, sql, j) {
                    return Some(Err(syntax(toks, sql, j, mysql)));
                }
                Some(Ok(Self::Update {
                    columns,
                    buckets,
                    manual,
                }))
            }
            (b"PERSISTENT", true) | (b"UPDATE" | b"DROP", false) => {
                Some(Err(syntax(toks, sql, i, mysql)))
            }
            _ => None,
        }
    }

    /// The answer's `Op` column.
    pub(crate) fn op(&self) -> &'static str {
        if self.histogram() {
            "histogram"
        } else {
            "analyze"
        }
    }

    /// A MySQL histogram form: one table only, its own answers for views
    /// and missing tables.
    pub(crate) fn histogram(&self) -> bool {
        matches!(self, Self::Update { .. } | Self::Drop { .. })
    }

    /// Runs the form on one engine table; the answer rows (`Msg_type`,
    /// `Msg_text`).
    #[allow(clippy::too_many_lines)] // one arm per form, with its answer rows
    pub(crate) async fn run(&self, tenant: u64, def: &TableDef) -> Vec<(&'static str, String)> {
        let failed = |e: &SprigError| {
            vec![
                ("Error", e.to_string()),
                ("status", "Operation failed".to_owned()),
            ]
        };
        match self {
            Self::Plain => match collect::analyze(tenant, def, &Ask::default()).await {
                Ok(_) => vec![("status", "OK".to_owned())],
                Err(e) => failed(&e),
            },
            Self::Persistent { columns, indexes } => {
                if let Err(e) = collect::analyze(tenant, def, &Ask::default()).await {
                    return failed(&e);
                }
                let mut out = vec![(
                    "status",
                    "Engine-independent statistics collected".to_owned(),
                )];
                let unknown_col = columns.iter().flatten().any(|c| def.column(c).is_none());
                let unknown_ix = indexes.iter().flatten().any(|x| {
                    !x.eq_ignore_ascii_case("PRIMARY")
                        && !def.indexes.iter().any(|i| i.name.eq_ignore_ascii_case(x))
                });
                if unknown_col || unknown_ix {
                    out.push(("error", "Invalid argument".to_owned()));
                    return out;
                }
                for c in &def.columns {
                    let named = columns
                        .as_ref()
                        .is_none_or(|l| l.iter().any(|n| n.eq_ignore_ascii_case(&c.name)));
                    if named && is_lob(c.ty) {
                        out.push((
                            "Warning",
                            format!(
                                "Engine-independent statistics are not collected for column \
                                     '{}'",
                                c.name
                            ),
                        ));
                    }
                }
                out.push(("status", "OK".to_owned()));
                out
            }
            Self::Update {
                columns,
                buckets,
                manual,
            } => {
                let mut rows: Vec<(String, &'static str, String)> = Vec::new();
                let mut ask = Ask::default();
                for c in columns {
                    let msg = match def.column(c) {
                        None => Some(format!("The column '{c}' does not exist.")),
                        Some(i) if hist_column(&def.columns[i].ty).is_none() => {
                            Some(format!("The column '{c}' has an unsupported data type."))
                        }
                        Some(i) if single_unique(def, i) => Some(format!(
                            "The column '{c}' is covered by a single-part unique index."
                        )),
                        Some(_) => None,
                    };
                    match (msg, def.column(c)) {
                        (Some(m), _) => rows.push((c.to_lowercase(), "Error", m)),
                        // Named as defined from here on.
                        (None, Some(i)) => {
                            ask.histograms
                                .push((def.columns[i].name.clone(), *buckets, *manual));
                        }
                        (None, None) => {}
                    }
                }
                if !ask.histograms.is_empty() {
                    match collect::analyze(tenant, def, &ask).await {
                        Ok(_) => {
                            for (c, _, _) in &ask.histograms {
                                rows.push((
                                    c.to_lowercase(),
                                    "status",
                                    format!("Histogram statistics created for column '{c}'."),
                                ));
                            }
                        }
                        Err(e) => rows.push((String::new(), "Error", e.to_string())),
                    }
                }
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows.into_iter().map(|(_, t, m)| (t, m)).collect()
            }
            Self::Drop { columns } => match collect::drop_histograms(tenant, def, columns).await {
                Ok(had) => {
                    let mut rows: Vec<(String, &'static str, String)> = columns
                        .iter()
                        .zip(had)
                        .map(|(c, had)| {
                            if had {
                                (
                                    c.to_lowercase(),
                                    "status",
                                    format!("Histogram statistics removed for column '{c}'."),
                                )
                            } else {
                                (
                                    c.to_lowercase(),
                                    "Error",
                                    format!("No histogram statistics found for column '{c}'."),
                                )
                            }
                        })
                        .collect();
                    rows.sort_by(|a, b| a.0.cmp(&b.0));
                    rows.into_iter().map(|(_, t, m)| (t, m)).collect()
                }
                Err(e) => vec![("Error", e.to_string())],
            },
        }
    }
}

/// MySQL's answer to a histogram form naming several tables.
pub(crate) const ONE_TABLE: &str =
    "Only one table can be specified while modifying histogram statistics.";
/// MySQL's answer to a histogram form on a view.
pub(crate) const NOT_A_VIEW: &str = "Cannot create histogram statistics for a view.";

fn is_lob(ty: sprig_engine::row::ColumnType) -> bool {
    use sprig_engine::row::ColumnType as T;
    matches!(ty, T::Text { .. } | T::Blob { .. } | T::Json)
}

/// Whether column `i` alone is a unique key (the primary key included).
fn single_unique(def: &TableDef, i: usize) -> bool {
    (!def.hidden && def.pk == [i])
        || def.indexes.iter().any(|x| {
            x.unique && x.cols == [i] && x.parts.first().is_some_and(|p| p.prefix.is_none())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_sql::lex::{LexOptions, lex};

    fn form(sql: &str, mysql: bool) -> Option<std::result::Result<Form, u16>> {
        let opts = if mysql {
            LexOptions::mysql84()
        } else {
            LexOptions::mariadb()
        };
        let toks: Vec<_> = lex(sql.as_bytes(), &opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        // After `ANALYZE TABLE t`.
        Form::parse(&toks, sql.as_bytes(), 3, mysql)
            .map(|r| r.map_err(|e| e.protocol().mysql_errno))
    }

    #[test]
    fn each_port_reads_its_own_forms() {
        assert_eq!(form("ANALYZE TABLE t", false), Some(Ok(Form::Plain)));
        assert_eq!(form("ANALYZE TABLE t;", true), Some(Ok(Form::Plain)));
        assert_eq!(
            form("ANALYZE TABLE t PERSISTENT FOR ALL", false),
            Some(Ok(Form::Persistent {
                columns: None,
                indexes: None
            }))
        );
        assert_eq!(
            form(
                "ANALYZE TABLE t PERSISTENT FOR COLUMNS (a,`b`) INDEXES ()",
                false
            ),
            Some(Ok(Form::Persistent {
                columns: Some(vec!["a".into(), "b".into()]),
                indexes: Some(vec![])
            }))
        );
        assert_eq!(
            form("ANALYZE TABLE t PERSISTENT FOR ALL", true),
            Some(Err(1064))
        );
        assert_eq!(
            form(
                "ANALYZE TABLE t UPDATE HISTOGRAM ON k, s WITH 4 BUCKETS MANUAL UPDATE",
                true
            ),
            Some(Ok(Form::Update {
                columns: vec!["k".into(), "s".into()],
                buckets: 4,
                manual: true
            }))
        );
        assert_eq!(
            form("ANALYZE TABLE t UPDATE HISTOGRAM ON k", false),
            Some(Err(1064))
        );
        assert_eq!(
            form("ANALYZE TABLE t UPDATE HISTOGRAM ON k WITH 0 BUCKETS", true),
            Some(Err(1690))
        );
        assert_eq!(
            form(
                "ANALYZE TABLE t UPDATE HISTOGRAM ON k WITH 1025 BUCKETS",
                true
            ),
            Some(Err(1690))
        );
        assert_eq!(
            form("ANALYZE TABLE t UPDATE HISTOGRAM ON k, K", true),
            Some(Err(1060))
        );
        assert_eq!(
            form("ANALYZE TABLE t DROP HISTOGRAM ON k", true),
            Some(Ok(Form::Drop {
                columns: vec!["k".into()]
            }))
        );
        assert_eq!(form("ANALYZE TABLE t QUICK", true), None);
    }
}
