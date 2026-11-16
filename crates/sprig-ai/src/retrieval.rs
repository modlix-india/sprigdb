//! Retrieval indexes, the parts every role shares: the catalog rows
//! ([`store`]), the hidden chunk table each shard keeps, the SQL both the
//! worker and the gateway write over it, the `ai.retrieval.*` keys and the
//! metrics.
//!
//! A retrieval index keeps a source table's text column cut into chunks,
//! each chunk with an embedding and a full-text index, in a hidden table
//! `__sprig_rx_<id>` in every shard schema of the source table, so each
//! shard keeps the chunks of its own rows:
//!
//! ```sql
//! CREATE TABLE __sprig_rx_<id> (
//!   k_<col>   <the column's type> NOT NULL, -- the row's primary key, copied
//!   chunk_no  SMALLINT UNSIGNED NOT NULL,
//!   title     TEXT NULL,                    -- the row's title (and headings)
//!   text      MEDIUMTEXT NOT NULL,          -- the chunk
//!   f_<col>   <the column's type> NULL, ... -- FILTERS columns, copied
//!   src_hash  BINARY(16) NOT NULL,          -- MD5 of the row's indexed values
//!   embedding VECTOR(n) NOT NULL,
//!   PRIMARY KEY (k_<col>..., chunk_no),
//!   FULLTEXT KEY ft (text, title),
//!   VECTOR KEY v (embedding) DISTANCE=cosine)
//! ```
//!
//! The key columns have the source's types, so a key compares exactly as
//! in the source table. Rows are written only once embedded, so the vector column can be
//! `NOT NULL` and carry MariaDB's HNSW index. The worker's
//! `ai_retrieval` job keeps the table in step with the source from the
//! change feed; the gateway searches it (`RETRIEVE()` and REST) and checks
//! every hit against the source table through the caller's session.
//!
//! | Key | Default | Scopes |
//! |---|---|---|
//! | `ai.retrieval.candidates` | `40` | global |
//! | `ai.retrieval.max_chunks_per_row` | `64` | global |
//!
//! Throttles, batches and input cuts are the embedded columns'
//! (`ai.embedding.*`), and so are the model aliases.

pub mod store;

use serde::{Deserialize, Serialize};
use sprig_common::names::quote;
use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// Global only.
const GLOBAL: Scopes = Scopes(0b10_0000);

/// The feed subscription kind the gateways register, and the worker's job
/// kind.
pub const KIND: &str = "ai_retrieval";

/// At most this many hits of one source row in a search's answer.
pub const MAX_PER_ROW: usize = 2;

/// The prefix of the hidden chunk tables.
pub const TABLE_PREFIX: &str = "__sprig_rx_";

/// The prefix of a filter column in the chunk table.
pub const FILTER_PREFIX: &str = "f_";

/// The prefix of a key column in the chunk table.
pub const KEY_PREFIX: &str = "k_";

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $scopes:expr, $hard:expr, $op:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: $hard,
            apply: Apply::Live,
            operator_only: $op,
            owner: 56,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key!(
    /// `ai.retrieval.candidates`.
    CANDIDATES: u64 = "ai.retrieval.candidates", 40, GLOBAL, HardBounds::range(5, 1000), true,
    "Hits a retrieval index search takes from each shard's full-text list and vector list before fusing them"
);
key!(
    /// `ai.retrieval.max_chunks_per_row`.
    MAX_CHUNKS_PER_ROW: u64 = "ai.retrieval.max_chunks_per_row", 64, GLOBAL,
    HardBounds::range(1, 10_000), true,
    "Chunks a retrieval index keeps of one row at most; the rest of a longer text isn't indexed"
);

/// The chunk table of index `id`.
#[must_use]
pub fn table_name(id: u64) -> String {
    format!("{TABLE_PREFIX}{id}")
}

/// The feed subscription of index `id`.
#[must_use]
pub fn sub_name(id: u64) -> String {
    format!("rx:i{id}")
}

/// A column of the source table's primary key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyColumn {
    /// Its name.
    pub name: String,
    /// Its `COLUMN_TYPE`, which its copy in the chunk table takes.
    pub column_type: String,
    /// A binary string type: written as `X'..'` and shown as hexadecimal.
    #[serde(default)]
    pub binary: bool,
    /// A numeric type: written without quotes.
    #[serde(default)]
    pub numeric: bool,
}

impl KeyColumn {
    /// A value of this column (as the text protocol returns it) as a SQL
    /// literal; binary columns' values given as their raw bytes.
    #[must_use]
    pub fn literal(&self, v: Option<&[u8]>) -> String {
        match v {
            None => "NULL".to_owned(),
            Some(b) if self.binary => hex_literal(b),
            Some(b) => match std::str::from_utf8(b) {
                Ok(s) if self.numeric && is_number(s) => s.to_owned(),
                Ok(s) => string_literal(s),
                Err(_) => hex_literal(b),
            },
        }
    }

    /// A value of this column as text: binary columns' as hexadecimal.
    #[must_use]
    pub fn text(&self, v: Option<&[u8]>) -> Option<String> {
        v.map(|b| {
            if self.binary {
                hex(b)
            } else {
                String::from_utf8_lossy(b).into_owned()
            }
        })
    }
}

/// The chunk table's column for key column `name`.
#[must_use]
pub fn key_column(name: &str) -> String {
    format!("{KEY_PREFIX}{name}")
}

fn is_number(s: &str) -> bool {
    s.bytes().any(|b| b.is_ascii_digit())
        && s.bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
        && s.parse::<f64>().is_ok()
}

/// Lower-case hexadecimal.
#[must_use]
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

fn hex_literal(b: &[u8]) -> String {
    format!("X'{}'", hex(b))
}

/// A `FILTERS` column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterColumn {
    /// Its name in the source table.
    pub name: String,
    /// Its `COLUMN_TYPE` there, which its copy in the chunk table takes.
    pub column_type: String,
}

/// The chunk table's column for filter `name`.
#[must_use]
pub fn filter_column(name: &str) -> String {
    format!("{FILTER_PREFIX}{name}")
}

/// `CREATE TABLE IF NOT EXISTS` of index `id`'s chunk table in `schema`.
#[must_use]
pub fn create_table_sql(
    schema: &str,
    id: u64,
    dims: u32,
    keys: &[KeyColumn],
    filters: &[FilterColumn],
) -> String {
    let mut cols: Vec<String> = keys
        .iter()
        .map(|k| format!("{} {} NOT NULL", quote(&key_column(&k.name)), k.column_type))
        .collect();
    cols.extend([
        "`chunk_no` SMALLINT UNSIGNED NOT NULL".to_owned(),
        "`title` TEXT NULL".to_owned(),
        "`text` MEDIUMTEXT NOT NULL".to_owned(),
    ]);
    for f in filters {
        cols.push(format!(
            "{} {} NULL",
            quote(&filter_column(&f.name)),
            f.column_type
        ));
    }
    cols.push("`src_hash` BINARY(16) NOT NULL".to_owned());
    cols.push(format!("`embedding` VECTOR({dims}) NOT NULL"));
    let mut pk: Vec<String> = keys.iter().map(|k| quote(&key_column(&k.name))).collect();
    pk.push("`chunk_no`".to_owned());
    cols.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    cols.push("FULLTEXT KEY `ft` (`text`, `title`)".to_owned());
    cols.push("VECTOR KEY `v` (`embedding`) DISTANCE=cosine".to_owned());
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        quote(schema),
        quote(&table_name(id)),
        cols.join(", ")
    )
}

fn col(qualifier: Option<&str>, name: &str) -> String {
    match qualifier {
        Some(q) => format!("{}.{}", quote(q), quote(name)),
        None => quote(name),
    }
}

/// What a row's chunks were made from, hashed: `UNHEX(MD5(JSON_ARRAY(text,
/// title, filters...)))` over the source row.
#[must_use]
pub fn hash_expr(
    text: &str,
    title: Option<&str>,
    filters: &[FilterColumn],
    qualifier: Option<&str>,
) -> String {
    let mut parts = vec![col(qualifier, text)];
    parts.push(title.map_or_else(|| "NULL".to_owned(), |t| col(qualifier, t)));
    for f in filters {
        parts.push(col(qualifier, &f.name));
    }
    format!("UNHEX(MD5(JSON_ARRAY({})))", parts.join(", "))
}

/// A string as a SQL literal (backslashes and quotes escaped).
fn string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// The text a chunk is embedded from: its heading (the title and, with
/// `CHUNK BY HEADINGS`, the headings above it), a blank line, the chunk,
/// as the docs pack embeds its chunks.
#[must_use]
pub fn embedding_input(heading: &str, text: &str) -> String {
    if heading.trim().is_empty() {
        text.to_owned()
    } else {
        format!("{heading}\n\n{text}")
    }
}

/// Whether a `COLUMN_TYPE` can hold an indexed text or title.
#[must_use]
pub fn is_text_type(column_type: &str) -> bool {
    let t = column_type.to_ascii_lowercase();
    [
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
    ]
    .iter()
    .any(|p| t == *p || t.starts_with(&format!("{p}(")))
}

/// Whether a `COLUMN_TYPE` can be a `FILTERS` column (not a binary
/// string, a large object, a vector or a spatial type).
#[must_use]
pub fn is_filter_type(column_type: &str) -> bool {
    let t = column_type.to_ascii_lowercase();
    let bad = [
        "binary",
        "varbinary",
        "blob",
        "tinyblob",
        "mediumblob",
        "longblob",
        "text",
        "tinytext",
        "mediumtext",
        "longtext",
        "vector",
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "json",
    ];
    !bad.iter()
        .any(|p| t == *p || t.starts_with(&format!("{p}(")) || t.starts_with(&format!("{p} ")))
}

/// Whether a `COLUMN_TYPE` is a binary string (its key part goes in as
/// hexadecimal).
#[must_use]
pub fn is_binary_type(column_type: &str) -> bool {
    let t = column_type.to_ascii_lowercase();
    t.starts_with("binary") || t.starts_with("varbinary") || t.ends_with("blob")
}

/// Whether a `COLUMN_TYPE` is numeric.
#[must_use]
pub fn is_numeric_type(column_type: &str) -> bool {
    let t = column_type.to_ascii_lowercase();
    [
        "tinyint",
        "smallint",
        "mediumint",
        "int",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "bit",
        "year",
    ]
    .iter()
    .any(|p| t == *p || t.starts_with(&format!("{p}(")) || t.starts_with(&format!("{p} ")))
}

/// The metrics.
pub mod metrics {
    /// Source rows the process's retrieval jobs have read from the feed
    /// and not yet written.
    pub fn backlog(rows: u64) {
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!("sprig_retrieval_backlog").set(rows as f64);
    }

    /// Source rows handled: `result` is `indexed` (chunks written),
    /// `cleared` (chunks removed: the row is gone or has no text),
    /// `skipped` (already current) or `superseded` (the row changed again
    /// first).
    pub fn rows(result: &'static str, n: u64) {
        if n > 0 {
            metrics::counter!("sprig_retrieval_rows_total", "result" => result).increment(n);
        }
    }

    /// Chunks written.
    pub fn chunks(n: u64) {
        if n > 0 {
            metrics::counter!("sprig_retrieval_chunks_total").increment(n);
        }
    }

    /// One search's time: `mode` is `hybrid`, or `keyword` without a query
    /// embedding.
    pub fn search_seconds(mode: &'static str, secs: f64) {
        metrics::histogram!("sprig_retrieval_search_seconds", "mode" => mode).record(secs);
    }

    /// Searches: `result` is `ok`, `empty` or `error`.
    pub fn searches(result: &'static str) {
        metrics::counter!("sprig_retrieval_searches_total", "result" => result).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<KeyColumn> {
        vec![
            KeyColumn {
                name: "org".into(),
                column_type: "varchar(20)".into(),
                binary: false,
                numeric: false,
            },
            KeyColumn {
                name: "id".into(),
                column_type: "binary(2)".into(),
                binary: true,
                numeric: false,
            },
            KeyColumn {
                name: "n".into(),
                column_type: "int(11)".into(),
                binary: false,
                numeric: true,
            },
        ]
    }

    #[test]
    fn hash_expressions() {
        let f = vec![FilterColumn {
            name: "lang".into(),
            column_type: "varchar(8)".into(),
        }];
        assert_eq!(
            hash_expr("body", Some("title"), &f, None),
            "UNHEX(MD5(JSON_ARRAY(`body`, `title`, `lang`)))"
        );
        assert_eq!(
            hash_expr("body", None, &[], Some("s")),
            "UNHEX(MD5(JSON_ARRAY(`s`.`body`, NULL)))"
        );
    }

    #[test]
    fn key_values_as_literals_and_text() {
        let k = keys();
        assert_eq!(k[0].literal(Some(b"it's")), "'it''s'");
        assert_eq!(k[1].literal(Some(&[10, 11])), "X'0a0b'");
        assert_eq!(k[1].text(Some(&[10, 11])).as_deref(), Some("0a0b"));
        assert_eq!(k[2].literal(Some(b"-42")), "-42");
        assert_eq!(k[2].literal(Some(b"1; DROP")), "'1; DROP'");
        assert_eq!(k[0].literal(None), "NULL");
        assert_eq!(key_column("id"), "k_id");
    }

    #[test]
    fn the_chunk_table() {
        let f = vec![FilterColumn {
            name: "lang".into(),
            column_type: "varchar(8)".into(),
        }];
        let sql = create_table_sql("t1_d1_g0_s0", 7, 384, &keys()[2..], &f);
        assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `t1_d1_g0_s0`.`__sprig_rx_7` ("));
        assert!(sql.contains("`f_lang` varchar(8) NULL"));
        assert!(sql.contains("`k_n` int(11) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (`k_n`, `chunk_no`)"));
        assert!(sql.contains("`embedding` VECTOR(384) NOT NULL"));
        assert!(sql.contains("VECTOR KEY `v` (`embedding`) DISTANCE=cosine"));
        assert_eq!(sub_name(7), "rx:i7");
    }

    #[test]
    fn column_types() {
        assert!(is_text_type("varchar(200)") && is_text_type("mediumtext"));
        assert!(!is_text_type("int(11)") && !is_text_type("blob"));
        assert!(is_filter_type("varchar(8)") && is_filter_type("int(11)"));
        assert!(is_filter_type("enum('a','b')") && is_filter_type("date"));
        assert!(!is_filter_type("text") && !is_filter_type("vector(384)"));
        assert!(is_binary_type("binary(16)") && !is_binary_type("char(36)"));
        assert!(is_numeric_type("bigint(20) unsigned") && !is_numeric_type("char(3)"));
        assert_eq!(embedding_input("", "x"), "x");
        assert_eq!(embedding_input("T", "x"), "T\n\nx");
    }
}
