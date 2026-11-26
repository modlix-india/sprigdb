//! MySQL 8.4's deprecation warnings for the `utf8mb3` character set.
//!
//! MySQL warns while it parses a statement, once for each place the text
//! names the character set or one of its collations:
//!
//! ```text
//! utf8 as a character set     3719  'utf8' is currently an alias for the character set UTF8MB3, ...
//! utf8mb3 as a character set  1287  'utf8mb3' is deprecated and will be removed in a future release. ...
//! a utf8mb3 collation         3778  'utf8mb3_bin' is a collation of the deprecated character set UTF8MB3. ...
//! ```
//!
//! A character set is named after `CHARACTER SET`, `CHARSET`, `NAMES`,
//! `USING` and as an introducer (`_utf8'x'`); a collation after `COLLATE`
//! in a definition (`CREATE`, `ALTER`) or in `SET NAMES cs COLLATE c`. A
//! `COLLATE` in an expression (`SELECT x COLLATE utf8mb3_bin`) gives none.
//! `SET character_set_client = utf8` and `SET collation_connection =
//! utf8_bin` name them as a variable's value. A collation named `utf8_*`
//! is reported by its `utf8mb3_*` name. Names inside strings elsewhere
//! (`WHERE CHARACTER_SET_NAME = 'utf8'`) are values, not names. MariaDB
//! gives none of these warnings.

use crate::lex::{LexOptions, Token, TokenKind, lex};

/// `'utf8'` named as a character set.
pub const UTF8_ALIAS: u16 = 3719;
/// `'utf8mb3'` named as a character set (MySQL's general deprecation code).
pub const DEPRECATED: u16 = 1287;
/// A `utf8mb3` collation named.
pub const DEPRECATED_COLLATION: u16 = 3778;

/// The warnings MySQL 8.4 gives `sql` for naming `utf8mb3`, in the order
/// the names appear: `(code, message)`.
#[must_use]
pub fn warnings(sql: &[u8], opts: &LexOptions) -> Vec<(u16, String)> {
    // Cheap test first: most statements never name the character set.
    if !contains_ci(sql, b"utf8") {
        return Vec::new();
    }
    let Ok(tokens) = lex(sql, opts) else {
        return Vec::new();
    };
    let sig: Vec<Token> = tokens.into_iter().filter(|t| !t.kind.is_trivia()).collect();
    let word = |i: usize, w: &str| {
        sig.get(i).is_some_and(|t| {
            t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(w.as_bytes())
        })
    };
    let eq = |i: usize| {
        sig.get(i)
            .is_some_and(|t| t.kind == TokenKind::Op && t.bytes(sql) == b"=")
    };
    // `COLLATE` names a deprecated collation in definitions only.
    let ddl = word(0, "CREATE") || word(0, "ALTER");
    let mut out = Vec::new();
    let mut i = 0;
    while i < sig.len() {
        let t = sig[i];
        // An introducer: `_utf8'x'`, `_utf8mb3 X'78'`.
        if t.kind == TokenKind::Word
            && let Some(cs) = t.bytes(sql).strip_prefix(b"_")
            && sig.get(i + 1).is_some_and(|n| {
                matches!(
                    n.kind,
                    TokenKind::String | TokenKind::HexString | TokenKind::BitString
                )
            })
        {
            charset_warning(cs, &mut out);
            i += 1;
            continue;
        }
        // The position of the name a keyword introduces.
        let name_at = if word(i, "CHARACTER") && word(i + 1, "SET") {
            Some((i + 2, false))
        } else if word(i, "CHARSET") || word(i, "NAMES") || word(i, "USING") {
            Some((i + 1, false))
        } else if word(i, "COLLATE") && (ddl || (i >= 2 && word(i - 2, "NAMES"))) {
            Some((i + 1, true))
        } else if let Some(v) = variable(&sig[i], sql)
            && eq(i + 1)
        {
            Some((i + 2, v))
        } else {
            None
        };
        if let Some((mut at, collation)) = name_at {
            if eq(at) {
                at += 1;
            }
            if let Some(n) = sig.get(at).and_then(|n| name_text(n, sql)) {
                if collation {
                    collation_warning(&n, &mut out);
                } else {
                    charset_warning(n.as_bytes(), &mut out);
                }
                i = at + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// `Some(true)` for a collation variable (`collation_connection`, ...),
/// `Some(false)` for a character-set one, as a `SET` target.
fn variable(t: &Token, sql: &[u8]) -> Option<bool> {
    let raw = match t.kind {
        TokenKind::Word => t.bytes(sql),
        TokenKind::SysVar => {
            let b = t.bytes(sql).trim_ascii_start();
            let b = b.strip_prefix(b"@@").unwrap_or(b);
            let lower = b.to_ascii_lowercase();
            let rest = ["session.", "local.", "global."]
                .iter()
                .find_map(|p| lower.strip_prefix(p.as_bytes()).map(<[u8]>::len))
                .unwrap_or(b.len());
            &b[b.len() - rest..]
        }
        _ => return None,
    };
    let n = raw.to_ascii_lowercase();
    if n.starts_with(b"collation_") {
        Some(true)
    } else if n.starts_with(b"character_set_") && n != b"character_set_collations" {
        Some(false)
    } else {
        None
    }
}

/// A name as written: a word, a quoted identifier or a string.
fn name_text(t: &Token, sql: &[u8]) -> Option<String> {
    let b = t.bytes(sql);
    let inner = match t.kind {
        TokenKind::Word => b,
        TokenKind::QuotedIdent | TokenKind::String if b.len() >= 2 => &b[1..b.len() - 1],
        _ => return None,
    };
    std::str::from_utf8(inner).ok().map(str::to_ascii_lowercase)
}

fn charset_warning(cs: &[u8], out: &mut Vec<(u16, String)>) {
    if cs.eq_ignore_ascii_case(b"utf8") {
        out.push((
            UTF8_ALIAS,
            "'utf8' is currently an alias for the character set UTF8MB3, but will be an alias \
             for UTF8MB4 in a future release. Please consider using UTF8MB4 in order to be \
             unambiguous."
                .to_owned(),
        ));
    } else if cs.eq_ignore_ascii_case(b"utf8mb3") {
        out.push((
            DEPRECATED,
            "'utf8mb3' is deprecated and will be removed in a future release. Please use \
             utf8mb4 instead"
                .to_owned(),
        ));
    }
}

fn collation_warning(name: &str, out: &mut Vec<(u16, String)>) {
    let rest = name
        .strip_prefix("utf8mb3_")
        .or_else(|| name.strip_prefix("utf8_"));
    if let Some(rest) = rest {
        out.push((
            DEPRECATED_COLLATION,
            format!(
                "'utf8mb3_{rest}' is a collation of the deprecated character set UTF8MB3. \
                 Please consider using UTF8MB4 with an appropriate collation instead."
            ),
        ));
    }
}

fn contains_ci(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(sql: &str) -> Vec<u16> {
        warnings(sql.as_bytes(), &LexOptions::mysql84())
            .into_iter()
            .map(|(c, _)| c)
            .collect()
    }

    #[test]
    fn each_named_utf8mb3_warns_once_in_order() {
        assert_eq!(codes("SET NAMES utf8mb3"), [1287]);
        assert_eq!(codes("SET NAMES utf8"), [3719]);
        assert_eq!(codes("SET NAMES 'utf8' COLLATE 'utf8_bin'"), [3719, 3778]);
        assert_eq!(
            codes("CREATE TABLE t (a VARCHAR(5) CHARSET utf8mb3 COLLATE utf8mb3_unicode_ci)"),
            [1287, 3778]
        );
        assert_eq!(codes("CREATE TABLE t (a INT) DEFAULT CHARSET=utf8"), [3719]);
        assert_eq!(
            codes("CREATE TABLE t (a VARCHAR(5) COLLATE utf8_bin)"),
            [3778]
        );
        assert_eq!(
            codes("SELECT _utf8mb3'a', COLLATION(_utf8mb3'a')"),
            [1287, 1287]
        );
        assert_eq!(codes("SELECT _utf8mb3'x' COLLATE utf8mb3_bin"), [1287]);
        assert!(codes("SELECT a COLLATE utf8_bin FROM t").is_empty());
        assert_eq!(codes("CREATE DATABASE d COLLATE utf8_bin"), [3778]);
        assert_eq!(codes("SELECT CONVERT('a' USING utf8)"), [3719]);
        assert_eq!(
            codes("SELECT CAST('a' AS CHAR CHARACTER SET utf8mb3)"),
            [1287]
        );
        assert_eq!(codes("SET character_set_client = utf8"), [3719]);
        assert_eq!(
            codes("SET @@session.collation_connection = utf8_general_ci"),
            [3778]
        );
        assert_eq!(codes("ALTER TABLE t CONVERT TO CHARACTER SET utf8"), [3719]);
    }

    #[test]
    fn values_and_other_character_sets_do_not_warn() {
        assert!(codes("SET NAMES utf8mb4").is_empty());
        assert!(codes("SELECT * FROM t WHERE cs = 'utf8mb3'").is_empty());
        assert!(codes("SHOW CHARACTER SET LIKE 'utf8%'").is_empty());
        assert!(codes("SELECT 'utf8' COLLATE utf8mb4_bin").is_empty());
        assert!(codes("SELECT utf8mb3_col FROM t").is_empty());
        assert!(codes("SET character_set_collations = 'utf8mb3=utf8mb3_bin'").is_empty());
    }
}
