//! Small helpers for reading streams and change records: quoting the
//! `key` and `value` column words of a statement that reads a stream, and
//! the row-security filter of change records' images.

use sprig_sql::lex::{LexOptions, TokenKind, lex};

/// Quotes bare `key` and `value` words (not index hints such as `USE KEY`
/// or `PRIMARY KEY`).
#[must_use]
pub fn quote_columns(sql: &str, lex_opts: &LexOptions) -> String {
    let Ok(toks) = lex(sql.as_bytes(), lex_opts) else {
        return sql.to_owned();
    };
    let mut out = String::with_capacity(sql.len() + 8);
    let mut last = 0;
    let mut prev_word: Option<String> = None;
    for t in &toks {
        if t.kind.is_trivia() {
            continue;
        }
        let text = &sql[t.range()];
        if t.kind == TokenKind::Word
            && (text.eq_ignore_ascii_case("key") || text.eq_ignore_ascii_case("value"))
            && !prev_word.as_deref().is_some_and(|p| {
                matches!(
                    p,
                    "USE" | "FORCE" | "IGNORE" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "DUPLICATE"
                )
            })
        {
            out.push_str(&sql[last..t.start as usize]);
            out.push('`');
            out.push_str(&text.to_ascii_lowercase());
            out.push('`');
            last = t.end as usize;
        }
        prev_word = (t.kind == TokenKind::Word).then(|| text.to_ascii_uppercase());
    }
    out.push_str(&sql[last..]);
    out
}

/// The alias a change record's image has in a [`RecordFilter`]'s
/// predicate (`` `__sprig_ci`.`col` ``).
pub const FILTER_QUAL: &str = "`__sprig_ci`";

/// A change topic's records as one reader may see them (row-level
/// security for change feed consumers): each record's
/// `before` and `after` images are read as rows of the table
/// (`JSON_TABLE` with the table's column types and collations) and the
/// reader's `SELECT` policies, rendered with its session values over
/// [`FILTER_QUAL`], decide whether each image is visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordFilter {
    /// `JSON_TABLE`'s column list: `` `owner` VARCHAR(64) CHARACTER SET
    /// utf8mb4 COLLATE utf8mb4_general_ci PATH '$."owner"', ... ``.
    pub columns: String,
    /// The predicate over [`FILTER_QUAL`] (`FALSE` hides every image).
    pub predicate: String,
}

/// Whether image `path` (`'$.before'` or `'$.after'`) of the JSON in
/// column `v` of the row `` `__sprig_cl` `` passes `f`: `FALSE` for a
/// missing image.
#[must_use]
pub fn image_visible(f: &RecordFilter, path: &str) -> String {
    let v = "CONVERT(`__sprig_cl`.v USING utf8mb4)";
    format!(
        "COALESCE(JSON_TYPE(JSON_EXTRACT({v}, '{path}')) = 'OBJECT' AND EXISTS (SELECT 1 FROM \
         JSON_TABLE({v}, '{path}' COLUMNS ({cols})) AS {q} WHERE {pred}), FALSE)",
        cols = f.columns,
        q = FILTER_QUAL,
        pred = f.predicate,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_quoted() {
        let o = LexOptions::mariadb();
        assert_eq!(
            quote_columns("SELECT key, value FROM s USE KEY (x)", &o),
            "SELECT `key`, `value` FROM s USE KEY (x)"
        );
        assert_eq!(
            quote_columns("SELECT 'key' FROM s", &o),
            "SELECT 'key' FROM s"
        );
    }
}
