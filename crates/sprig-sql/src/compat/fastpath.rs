//! Statements simple enough to skip the gateway's text passes: a plain
//! `INSERT ... VALUES` of literals, and `SELECT`, `UPDATE` and `DELETE` of
//! one table whose `WHERE` compares columns with literals.
//!
//! Every statement a client sends goes through a series of text passes
//! before the storage engine sees it (the MySQL port's rewrites, the name
//! layer, the stream, typed-table, inheritance and search layers, the
//! privilege check's parse). Each pass scans the whole text; for a
//! `sysbench prepare` batch of 100 rows that is about 20 KB, scanned a
//! dozen times. [`classify`] reads the text once and says whether it has
//! one of the shapes below; the gateway then decides, once per shape (the
//! table, the statement kind, the session's context), whether the passes
//! leave such a statement unchanged, and if so skips them for the next
//! statements of the same shape.
//!
//! The shapes (anything else, including any comment, hint, variable,
//! placeholder, function call or qualified name, is `None`):
//!
//! ```text
//! INSERT [IGNORE] [INTO] t [(c, ...)] VALUES|VALUE (v, ...)[, (v, ...)]...
//! SELECT * | c, ... FROM t WHERE c = v [AND c = v]... [LIMIT n]
//! UPDATE [IGNORE] t SET c = w[, c = w]... WHERE c = v [AND c = v]...
//! DELETE FROM t WHERE c = v [AND c = v]...
//! ```
//!
//! where `v` is a literal (a number with an optional sign, a string, `NULL`,
//! `TRUE`, `FALSE`; `DEFAULT` in `VALUES`), and `w` is `v` or `c [+|-]
//! number`. Nothing may follow, not even `;` (a text of several statements
//! is split first).

use sprig_auth::authz::{Action, ActionSet};

use crate::access::Need;
use crate::lex::{self, LexOptions, Token, TokenKind};

/// The statement kinds [`classify`] recognises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FastKind {
    /// `INSERT ... VALUES`.
    Insert,
    /// A `SELECT` of one table by column values.
    Select,
    /// An `UPDATE` of one table by column values.
    Update,
    /// A `DELETE` from one table by column values.
    Delete,
}

/// A statement of one of the shapes in the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastStmt {
    /// Its kind.
    pub kind: FastKind,
    /// The table, as an identifier's value (unquoted), unqualified: it is
    /// in the session's current database.
    pub table: String,
}

impl FastStmt {
    /// What the privilege check needs for it: the same list the general
    /// check derives from a full parse of these shapes.
    #[must_use]
    pub fn needs(&self) -> Vec<Need> {
        let action = match self.kind {
            FastKind::Insert => Action::Insert,
            FastKind::Select => Action::Select,
            FastKind::Update => Action::Update,
            FastKind::Delete => Action::Delete,
        };
        vec![Need::Table {
            db: None,
            name: self.table.clone(),
            any: ActionSet::of(&[action]),
        }]
    }

    /// The table MySQL names in a duplicate-key error (`INSERT` and
    /// `UPDATE`).
    #[must_use]
    pub fn dup_key_table(&self) -> Option<&str> {
        matches!(self.kind, FastKind::Insert | FastKind::Update).then_some(self.table.as_str())
    }
}

/// Words that read like a column but are values (niladic functions and
/// constants the name layer or the engine evaluates).
const NOT_COLUMNS: &[&str] = &[
    "NULL",
    "TRUE",
    "FALSE",
    "DEFAULT",
    "UNKNOWN",
    "CURRENT_USER",
    "CURRENT_ROLE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "SYSDATE",
    "ROWNUM",
    "SYSTIMESTAMP",
];

/// Words that end a list or start a clause: never a name here.
const CLAUSES: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "SET",
    "VALUES",
    "VALUE",
    "LIMIT",
    "ORDER",
    "GROUP",
    "HAVING",
    "UNION",
    "INTO",
    "IGNORE",
    "FOR",
    "LOCK",
    "JOIN",
    "ON",
    "USING",
    "AS",
    "WINDOW",
    "PARTITION",
    "RETURNING",
    "ROW",
    "DISTINCT",
    "ALL",
    "XOR",
    "IS",
    "IN",
    "LIKE",
    "BETWEEN",
    "DUPLICATE",
    "KEY",
    "INTERSECT",
    "EXCEPT",
    "WITH",
];

struct P<'a> {
    src: &'a [u8],
    t: Vec<Token>,
    i: usize,
}

impl P<'_> {
    fn peek(&self) -> Option<&Token> {
        self.t.get(self.i)
    }

    fn word(&mut self, w: &str) -> bool {
        let hit = self.peek().is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.src).eq_ignore_ascii_case(w.as_bytes())
        });
        if hit {
            self.i += 1;
        }
        hit
    }

    fn punct(&mut self, p: u8) -> bool {
        let hit = self
            .peek()
            .is_some_and(|tk| tk.kind == TokenKind::Punct && tk.bytes(self.src) == [p]);
        if hit {
            self.i += 1;
        }
        hit
    }

    fn op(&mut self, o: &[u8]) -> bool {
        let hit = self
            .peek()
            .is_some_and(|tk| tk.kind == TokenKind::Op && tk.bytes(self.src) == o);
        if hit {
            self.i += 1;
        }
        hit
    }

    /// An unqualified name (table or column), as its value.
    fn name(&mut self) -> Option<String> {
        self.name_then(false)
    }

    /// [`Self::name`]; `paren`: a `(` may follow (an `INSERT`'s column
    /// list), else it makes the name a call.
    fn name_then(&mut self, paren: bool) -> Option<String> {
        let tk = *self.peek()?;
        let b = tk.bytes(self.src);
        let ok = match tk.kind {
            TokenKind::QuotedIdent => true,
            TokenKind::Word => {
                let up = |l: &[&str]| l.iter().any(|w| b.eq_ignore_ascii_case(w.as_bytes()));
                !up(NOT_COLUMNS) && !up(CLAUSES)
            }
            _ => false,
        };
        if !ok {
            return None;
        }
        self.i += 1;
        // A qualified name, or a call.
        if self.peek().is_some_and(|n| {
            n.kind == TokenKind::Punct
                && (n.bytes(self.src) == b"." || (!paren && n.bytes(self.src) == b"("))
        }) {
            return None;
        }
        let v = lex::ident_value(b);
        if v.is_empty() {
            return None;
        }
        Some(String::from_utf8_lossy(&v).into_owned())
    }

    /// A literal; `DEFAULT` too when `default` is set.
    fn literal(&mut self, default: bool) -> bool {
        let start = self.i;
        let signed = self.op(b"-") || self.op(b"+");
        let Some(tk) = self.peek().copied() else {
            self.i = start;
            return false;
        };
        let ok = match tk.kind {
            TokenKind::Number => true,
            TokenKind::String if !signed => true,
            TokenKind::Word if !signed => {
                let b = tk.bytes(self.src);
                ["NULL", "TRUE", "FALSE"]
                    .iter()
                    .any(|w| b.eq_ignore_ascii_case(w.as_bytes()))
                    || (default && b.eq_ignore_ascii_case(b"DEFAULT"))
            }
            _ => false,
        };
        if !ok {
            self.i = start;
            return false;
        }
        self.i += 1;
        // Adjacent strings concatenate (`'a' 'b'`): not a plain literal.
        if tk.kind == TokenKind::String
            && self
                .peek()
                .is_some_and(|n| matches!(n.kind, TokenKind::String))
        {
            self.i = start;
            return false;
        }
        true
    }

    /// `c = v [AND c = v]...`
    fn conds(&mut self) -> bool {
        loop {
            if self.name().is_none() || !self.op(b"=") || !self.literal(false) {
                return false;
            }
            if !self.word("AND") {
                return true;
            }
        }
    }

    /// The end of the text.
    fn end(&self) -> bool {
        self.i == self.t.len()
    }
}

/// The statement's shape, when it is one of the module docs' shapes.
#[must_use]
#[allow(clippy::too_many_lines)] // one arm per shape, read in one pass
pub fn classify(sql: &[u8], lex_opts: &LexOptions) -> Option<FastStmt> {
    let head = sql.trim_ascii_start();
    let starts = |w: &[u8]| head.len() > w.len() && head[..w.len()].eq_ignore_ascii_case(w);
    if !(starts(b"INSERT") || starts(b"SELECT") || starts(b"UPDATE") || starts(b"DELETE")) {
        return None;
    }
    let toks = lex::lex(sql, lex_opts).ok()?;
    // Whitespace only: a comment, a hint or an executable comment may
    // carry anything, and the passes read them.
    if toks.iter().any(|t| {
        t.kind.is_trivia() && t.kind != TokenKind::Whitespace || t.kind == TokenKind::Unterminated
    }) {
        return None;
    }
    let t: Vec<Token> = toks
        .into_iter()
        .filter(|t| t.kind != TokenKind::Whitespace)
        .collect();
    let mut p = P { src: sql, t, i: 0 };
    if p.word("INSERT") {
        p.word("IGNORE");
        p.word("INTO");
        let table = p.name_then(true)?;
        if p.punct(b'(') && !p.punct(b')') {
            loop {
                p.name()?;
                if p.punct(b')') {
                    break;
                }
                if !p.punct(b',') {
                    return None;
                }
            }
        }
        if !(p.word("VALUES") || p.word("VALUE")) {
            return None;
        }
        loop {
            if !p.punct(b'(') {
                return None;
            }
            if !p.punct(b')') {
                loop {
                    if !p.literal(true) {
                        return None;
                    }
                    if p.punct(b')') {
                        break;
                    }
                    if !p.punct(b',') {
                        return None;
                    }
                }
            }
            if !p.punct(b',') {
                break;
            }
        }
        return p.end().then_some(FastStmt {
            kind: FastKind::Insert,
            table,
        });
    }
    if p.word("SELECT") {
        if !p.op(b"*") {
            loop {
                p.name()?;
                if !p.punct(b',') {
                    break;
                }
            }
        }
        if !p.word("FROM") {
            return None;
        }
        let table = p.name()?;
        if !p.word("WHERE") || !p.conds() {
            return None;
        }
        if p.word("LIMIT") {
            if !p.peek().is_some_and(|tk| tk.kind == TokenKind::Number) {
                return None;
            }
            p.i += 1;
        }
        return p.end().then_some(FastStmt {
            kind: FastKind::Select,
            table,
        });
    }
    if p.word("UPDATE") {
        p.word("IGNORE");
        let table = p.name()?;
        if !p.word("SET") {
            return None;
        }
        loop {
            p.name()?;
            if !p.op(b"=") {
                return None;
            }
            if !p.literal(false) {
                p.name()?;
                if p.op(b"+") || p.op(b"-") {
                    if !p.peek().is_some_and(|tk| tk.kind == TokenKind::Number) {
                        return None;
                    }
                    p.i += 1;
                }
            }
            if !p.punct(b',') {
                break;
            }
        }
        if !p.word("WHERE") || !p.conds() {
            return None;
        }
        return p.end().then_some(FastStmt {
            kind: FastKind::Update,
            table,
        });
    }
    if p.word("DELETE") {
        if !p.word("FROM") {
            return None;
        }
        let table = p.name()?;
        if !p.word("WHERE") || !p.conds() {
            return None;
        }
        return p.end().then_some(FastStmt {
            kind: FastKind::Delete,
            table,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access;

    fn c(sql: &str) -> Option<FastStmt> {
        classify(sql.as_bytes(), &LexOptions::mysql84())
    }

    #[test]
    fn plain_shapes_are_recognised() {
        let ins = c(
            "INSERT INTO sbtest1 (id, k, c, pad) VALUES (1, -5, 'a\\'b', 'x'), (2, 3.5e2, NULL, DEFAULT)",
        );
        assert_eq!(
            ins,
            Some(FastStmt {
                kind: FastKind::Insert,
                table: "sbtest1".into()
            })
        );
        assert_eq!(c("insert ignore `t t` value ()").unwrap().table, "t t");
        assert_eq!(c("INSERT t VALUES (1)").unwrap().kind, FastKind::Insert);
        assert_eq!(
            c("SELECT c FROM sbtest1 WHERE id=10").unwrap().kind,
            FastKind::Select
        );
        assert_eq!(
            c("SELECT * FROM t WHERE a = 'x' AND b = 2 LIMIT 1")
                .unwrap()
                .kind,
            FastKind::Select
        );
        assert_eq!(
            c("UPDATE sbtest1 SET k=k+1 WHERE id=5").unwrap().kind,
            FastKind::Update
        );
        assert_eq!(
            c("UPDATE t SET c='x', d = NULL WHERE id = -1")
                .unwrap()
                .kind,
            FastKind::Update
        );
        assert_eq!(
            c("DELETE FROM sbtest1 WHERE id=7").unwrap().kind,
            FastKind::Delete
        );
    }

    #[test]
    fn anything_else_is_left_to_the_passes() {
        for s in [
            "INSERT INTO db.t VALUES (1)",
            "INSERT INTO t VALUES (NOW())",
            "INSERT INTO t VALUES (?)",
            "INSERT INTO t VALUES (@a)",
            "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE a = 2",
            "INSERT INTO t VALUES (1) AS new",
            "INSERT INTO t SELECT * FROM u",
            "INSERT INTO t SET a = 1",
            "INSERT INTO t VALUES ROW(1)",
            "INSERT INTO t VALUES ('a' 'b')",
            "INSERT INTO t VALUES (_utf8mb4'a')",
            "INSERT INTO t VALUES (1) /* c */",
            "INSERT /*+ hint */ INTO t VALUES (1)",
            "/*!80000 INSERT INTO t VALUES (1) */",
            "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
            "INSERT INTO t VALUES (1);",
            "INSERT INTO t VALUES (1), (2",
            "INSERT INTO t VALUES (CURRENT_TIMESTAMP)",
            "SELECT c FROM t",
            "SELECT c FROM t WHERE id > 1",
            "SELECT c FROM t WHERE id = 1 OR id = 2",
            "SELECT COUNT(*) FROM t WHERE id = 1",
            "SELECT CURRENT_USER FROM t WHERE id = 1",
            "SELECT c FROM t WHERE id = 1 FOR UPDATE",
            "SELECT c FROM t, u WHERE id = 1",
            "SELECT c FROM t WHERE id = 1 ORDER BY c",
            "SELECT c FROM t WHERE id = (SELECT 1)",
            "UPDATE t SET a = a * 2 WHERE id = 1",
            "UPDATE t, u SET a = 1 WHERE id = 1",
            "UPDATE t SET a = 1",
            "UPDATE t SET a = CURRENT_TIMESTAMP WHERE id = 1",
            "DELETE FROM t",
            "DELETE t FROM t WHERE id = 1",
            "DELETE FROM t WHERE id = 1 LIMIT 1",
            "REPLACE INTO t VALUES (1)",
        ] {
            assert_eq!(c(s), None, "{s}");
        }
    }

    /// The privilege needs are the general check's for the same text.
    #[test]
    fn needs_match_the_general_check() {
        for s in [
            "INSERT INTO t (a) VALUES (1)",
            "INSERT IGNORE INTO t VALUES (1, 'x')",
            "SELECT a FROM t WHERE id = 1",
            "UPDATE t SET a = a + 1 WHERE id = 1",
            "DELETE FROM t WHERE id = 1",
        ] {
            let f = c(s).unwrap();
            let general = access::requirements(s.as_bytes(), &LexOptions::mysql84());
            assert_eq!(Some(f.needs()), general, "{s}");
        }
    }
}
