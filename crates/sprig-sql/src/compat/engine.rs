//! Storage engines other than `InnoDB` in `CREATE TABLE` and `ALTER TABLE`.
//!
//! Galera replicates only `InnoDB` tables, and every SprigDB table is moved,
//! copied and restored as `InnoDB`, so a client's table is always `InnoDB`.
//! What a client asks for is answered the way MySQL and MariaDB answer when
//! an operator turned the engine off and engine substitution is allowed:
//!
//! - **Substituted engines** (`MyISAM`, `MEMORY`/`HEAP`, `Aria`, `CSV`,
//!   `ARCHIVE`). With [`Mode::Innodb`] (the default), `CREATE TABLE` makes
//!   an `InnoDB` table. On 3306 it warns as MySQL 8.4 does with the engine in
//!   `disabled_storage_engines` and without `NO_ENGINE_SUBSTITUTION`:
//!   warning 3161 (`Storage engine MyISAM is disabled (Table creation is
//!   disallowed).`) and warning 1266 (`Using storage engine InnoDB for table
//!   't'`). On 3307 it gives MariaDB's note 1266, as with
//!   `enforce_storage_engine=InnoDB`. `ALTER TABLE ... ENGINE=MyISAM` on 3306
//!   leaves the engine as it is, with MySQL's warning 1286 (`Unknown storage
//!   engine 'MyISAM'`); on 3307 it becomes `ENGINE=InnoDB` with note 1266, as
//!   MariaDB does. A partition's engine becomes `InnoDB` without a warning of
//!   its own. With [`Mode::Reject`], the statement fails: 3161 on 3306,
//!   MariaDB's 1290 on 3307.
//! - **Refused engines** (`MRG_MYISAM`, `BLACKHOLE`, `FEDERATED`,
//!   `Columnstore`, `SPIDER`, `CONNECT`, `S3`, `SEQUENCE`, ...): their
//!   tables don't hold their rows in `InnoDB`, so they always fail: 3161 on
//!   3306, 1286 on 3307.
//! - **`InnoDB` and unknown names** are left to the backend.
//!
//! The session's `default_storage_engine` counts for a `CREATE TABLE`
//! without `ENGINE`. Temporary tables keep the engine the client named: no
//! one replicates or moves them. The engine is read from the statement's
//! text: the table options of `CREATE TABLE` (after the column list, before
//! a `SELECT`), the `ALTER TABLE` clauses outside parentheses, and the
//! partition definitions of either.

use sprig_proto_mysql::packets::Persona;

use super::errmap::WireError;
use crate::lex::{self, LexOptions, Token, TokenKind};

/// What happens to a substituted engine (`compat.engine_substitution`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// The table is `InnoDB`, with the server's warning.
    Innodb,
    /// The statement fails.
    Reject,
}

impl Mode {
    /// The setting's value: `reject`, otherwise [`Mode::Innodb`].
    #[must_use]
    pub fn parse(v: &str) -> Self {
        if v.trim().eq_ignore_ascii_case("reject") {
            Mode::Reject
        } else {
            Mode::Innodb
        }
    }
}

/// What an engine name stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    /// `InnoDB`.
    InnoDb,
    /// Created as `InnoDB`, or refused with [`Mode::Reject`]; the server's
    /// name for it.
    Substituted(&'static str),
    /// Always refused; the server's name for it.
    Refused(&'static str),
    /// A name neither server has: the backend answers (1286).
    Unknown,
}

/// The engines stored as `InnoDB`: (lower-case name as written, the name the
/// servers print).
const SUBSTITUTED: &[(&str, &str)] = &[
    ("myisam", "MyISAM"),
    ("memory", "MEMORY"),
    ("heap", "MEMORY"),
    ("aria", "Aria"),
    ("csv", "CSV"),
    ("archive", "ARCHIVE"),
];

/// The engines that are never created.
const REFUSED: &[(&str, &str)] = &[
    ("mrg_myisam", "MRG_MYISAM"),
    ("merge", "MRG_MYISAM"),
    ("blackhole", "BLACKHOLE"),
    ("federated", "FEDERATED"),
    ("columnstore", "Columnstore"),
    ("spider", "SPIDER"),
    ("connect", "CONNECT"),
    ("s3", "S3"),
    ("oqgraph", "OQGRAPH"),
    ("sequence", "SEQUENCE"),
    ("rocksdb", "ROCKSDB"),
    ("mroonga", "Mroonga"),
    ("tokudb", "TokuDB"),
    ("ndbcluster", "ndbcluster"),
    ("ndb", "ndbcluster"),
    ("ndbinfo", "ndbinfo"),
    ("performance_schema", "PERFORMANCE_SCHEMA"),
];

/// The class of `name` (as written, any case).
#[must_use]
pub fn classify(name: &str) -> Engine {
    let n = name.trim().to_ascii_lowercase();
    if n == "innodb" {
        return Engine::InnoDb;
    }
    if let Some((_, c)) = SUBSTITUTED.iter().find(|(k, _)| *k == n) {
        return Engine::Substituted(c);
    }
    if let Some((_, c)) = REFUSED.iter().find(|(k, _)| *k == n) {
        return Engine::Refused(c);
    }
    Engine::Unknown
}

/// A warning or note the statement answers with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineNote {
    /// `Warning` or `Note`.
    pub level: &'static str,
    /// The code.
    pub code: u16,
    /// The message.
    pub message: String,
}

/// What [`Plan::apply`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing to change.
    Unchanged,
    /// Run this text, and answer with these notes.
    Rewritten {
        /// The statement to run.
        sql: Vec<u8>,
        /// Its warnings or notes, in order.
        notes: Vec<EngineNote>,
    },
    /// The statement fails.
    Refused(WireError),
}

/// MySQL's 3161 for an engine an operator turned off.
#[must_use]
pub fn disabled(engine: &str) -> WireError {
    WireError::new(
        3161,
        *b"HY000",
        format!("Storage engine {engine} is disabled (Table creation is disallowed)."),
    )
}

/// MariaDB's 1290 for an enforced engine under `NO_ENGINE_SUBSTITUTION`.
#[must_use]
pub fn no_substitution() -> WireError {
    WireError::new(
        1290,
        *b"HY000",
        "The MariaDB server is running with the NO_ENGINE_SUBSTITUTION option so it cannot execute this statement",
    )
}

/// 1286, `Unknown storage engine 'x'`.
#[must_use]
pub fn unknown(engine: &str) -> WireError {
    WireError::new(
        1286,
        *b"42000",
        format!("Unknown storage engine '{engine}'"),
    )
}

/// 1266's message for `table`.
#[must_use]
pub fn using_innodb(table: &str) -> String {
    format!("Using storage engine InnoDB for table '{table}'")
}

/// Which statement a [`Plan`] is of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `CREATE [OR REPLACE] [TEMPORARY] TABLE`.
    Create,
    /// `ALTER [ONLINE] [IGNORE] TABLE`.
    Alter,
}

/// One `[STORAGE] ENGINE [=] name` in the text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Clause {
    engine: Engine,
    /// Bytes of the whole clause.
    start: usize,
    end: usize,
    /// Bytes of the name.
    name: (usize, usize),
    /// Among the table's options, not a partition's.
    table_level: bool,
    /// Bytes to remove when the clause goes (with an adjoining comma when
    /// it is a clause of its own).
    removal: (usize, usize),
}

/// The engine clauses of a `CREATE TABLE` or `ALTER TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The statement.
    pub kind: Kind,
    /// `CREATE TEMPORARY TABLE`.
    pub temporary: bool,
    /// The database, when the name is qualified.
    pub db: Option<String>,
    /// The table's name.
    pub table: String,
    clauses: Vec<Clause>,
    /// Where `ENGINE=InnoDB` goes in a `CREATE TABLE` that names no engine.
    insert_at: Option<usize>,
}

/// Whether `sql` may hold an engine clause (or be a `CREATE TABLE`, which
/// the session's default engine reaches): a cheap test before lexing.
#[must_use]
pub fn may_apply(sql: &[u8], session_default: bool) -> bool {
    let lead = sql
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map_or(&sql[..0], |i| &sql[i..]);
    let starts = |w: &[u8]| lead.len() >= w.len() && lead[..w.len()].eq_ignore_ascii_case(w);
    if !(starts(b"CREATE") || starts(b"ALTER")) {
        return false;
    }
    session_default || sql.windows(6).any(|w| w.eq_ignore_ascii_case(b"ENGINE"))
}

struct Toks<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl Toks<'_> {
    fn word(&self, j: usize, w: &[u8]) -> bool {
        self.t.get(j).is_some_and(|tk| {
            tk.kind == TokenKind::Word && tk.bytes(self.sql).eq_ignore_ascii_case(w)
        })
    }

    fn any(&self, j: usize, ws: &[&[u8]]) -> bool {
        ws.iter().any(|w| self.word(j, w))
    }

    fn punct(&self, j: usize, c: u8) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Punct && self.sql[tk.start as usize] == c)
    }

    fn op(&self, j: usize, s: &[u8]) -> bool {
        self.t
            .get(j)
            .is_some_and(|tk| tk.kind == TokenKind::Op && tk.bytes(self.sql) == s)
    }

    fn ident(&self, j: usize) -> Option<String> {
        self.t
            .get(j)
            .filter(|tk| tk.kind.is_ident_like())
            .map(|tk| String::from_utf8_lossy(&lex::ident_value(tk.bytes(self.sql))).into_owned())
    }

    fn start(&self, j: usize) -> usize {
        self.t[j].start as usize
    }

    fn end(&self, j: usize) -> usize {
        self.t[j].end as usize
    }

    /// The index of the `)` closing the `(` at `open`.
    fn matching(&self, open: usize) -> Option<usize> {
        let mut depth = 0usize;
        for k in open..self.t.len() {
            if self.punct(k, b'(') {
                depth += 1;
            } else if self.punct(k, b')') {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(k);
                }
            }
        }
        None
    }

    /// An engine clause starting at `k` (`ENGINE`, or `STORAGE ENGINE`):
    /// the engine and the name's token, the clause's last.
    fn clause_at(&self, k: usize, opts: LexOptions) -> Option<(Engine, usize)> {
        let e = if self.word(k, b"STORAGE") && self.word(k + 1, b"ENGINE") {
            k + 1
        } else if self.word(k, b"ENGINE") {
            k
        } else {
            return None;
        };
        let v = if self.op(e + 1, b"=") { e + 2 } else { e + 1 };
        let tk = self.t.get(v)?;
        let name = match tk.kind {
            TokenKind::Word | TokenKind::QuotedIdent => self.ident(v)?,
            TokenKind::String => String::from_utf8_lossy(&lex::string_value(
                tk.bytes(self.sql),
                opts.no_backslash_escapes,
            )?)
            .into_owned(),
            _ => return None,
        };
        Some((classify(&name), v))
    }
}

/// The engine clauses of `sql`, a `CREATE TABLE` or `ALTER TABLE`. `None`
/// for any other statement and for `CREATE TABLE ... LIKE`.
#[must_use]
#[allow(clippy::too_many_lines)] // one pass over the statement's clauses
pub fn plan(sql: &[u8], opts: &LexOptions) -> Option<Plan> {
    let tokens: Vec<Token> = lex::lex(sql, opts)
        .ok()?
        .into_iter()
        .filter(|tk| !tk.kind.is_trivia())
        .collect();
    let s = Toks { sql, t: tokens };
    let n = s.t.len();
    let (kind, mut j) = if s.word(0, b"CREATE") {
        (Kind::Create, 1)
    } else if s.word(0, b"ALTER") {
        (Kind::Alter, 1)
    } else {
        return None;
    };
    let mut temporary = false;
    match kind {
        Kind::Create => {
            if s.word(j, b"OR") && s.word(j + 1, b"REPLACE") {
                j += 2;
            }
            if s.word(j, b"TEMPORARY") {
                temporary = true;
                j += 1;
            }
        }
        Kind::Alter => {
            while s.any(j, &[b"ONLINE", b"OFFLINE", b"IGNORE"]) {
                j += 1;
            }
        }
    }
    if !s.word(j, b"TABLE") {
        return None;
    }
    j += 1;
    if s.word(j, b"IF") && s.word(j + 1, b"NOT") && s.word(j + 2, b"EXISTS") {
        j += 3;
    } else if s.word(j, b"IF") && s.word(j + 1, b"EXISTS") {
        j += 2;
    }
    let mut parts = vec![s.ident(j)?];
    let mut k = j + 1;
    while s.punct(k, b'.')
        && let Some(p) = s.ident(k + 1)
    {
        parts.push(p);
        k += 2;
    }
    let name_end = s.end(k - 1);
    let table = parts.pop()?;
    let db = parts.pop();
    let mut insert_at = None;
    if kind == Kind::Create {
        if s.word(k, b"LIKE") || (s.punct(k, b'(') && s.word(k + 1, b"LIKE")) {
            return None;
        }
        if s.punct(k, b'(') && !s.any(k + 1, &[b"SELECT", b"WITH"]) && !s.punct(k + 1, b'(') {
            let close = s.matching(k)?;
            insert_at = Some(s.end(close));
            k = close + 1;
        } else {
            insert_at = Some(name_end);
        }
    }
    let mut clauses = Vec::new();
    let mut depth = 0usize;
    let mut partition = false;
    // The token before the current clause of an `ALTER TABLE` (a `,` or
    // the name).
    let mut spec_start = k;
    while k < n {
        if s.punct(k, b'(') {
            if depth == 0 && kind == Kind::Create && s.any(k + 1, &[b"SELECT", b"WITH"]) {
                break;
            }
            depth += 1;
        } else if s.punct(k, b')') {
            depth = depth.saturating_sub(1);
        } else if s.punct(k, b';') {
            break;
        } else if depth == 0 && s.punct(k, b',') {
            spec_start = k + 1;
        } else if depth == 0
            && kind == Kind::Create
            && s.any(
                k,
                &[
                    b"SELECT", b"WITH", b"VALUES", b"TABLE", b"AS", b"IGNORE", b"REPLACE",
                ],
            )
        {
            break;
        } else if depth == 0 && s.any(k, &[b"PARTITION", b"PARTITIONS"]) {
            partition = true;
        } else if (depth == 0 || partition)
            && let Some((engine, last)) = s.clause_at(k, *opts)
        {
            let start = s.start(k);
            let end = s.end(last);
            let table_level = depth == 0;
            // A clause of its own takes one adjoining comma with it.
            let own_start = k == spec_start;
            let own_end = last + 1 >= n || s.punct(last + 1, b',') || s.punct(last + 1, b';');
            let removal = if kind == Kind::Alter && own_start && own_end {
                if k > 0 && s.punct(k - 1, b',') {
                    (s.start(k - 1), end)
                } else if s.punct(last + 1, b',') {
                    (start, s.end(last + 1))
                } else {
                    (start, end)
                }
            } else {
                (start, end)
            };
            clauses.push(Clause {
                engine,
                start,
                end,
                name: (s.start(last), s.end(last)),
                table_level,
                removal,
            });
            k = last + 1;
            continue;
        }
        k += 1;
    }
    Some(Plan {
        kind,
        temporary,
        db,
        table,
        clauses,
        insert_at,
    })
}

impl Plan {
    /// What `sql` (the text [`plan`] read) becomes for `persona` under
    /// `mode`. `session_default`: the session's `default_storage_engine`,
    /// when it set one.
    #[must_use]
    #[allow(clippy::too_many_lines)] // the outcomes per statement and persona
    pub fn apply(
        &self,
        sql: &[u8],
        persona: Persona,
        mode: Mode,
        session_default: Option<&str>,
    ) -> Outcome {
        if self.temporary {
            return Outcome::Unchanged;
        }
        let mysql = persona == Persona::MySql84;
        let refuse = |e: Engine| match (e, mysql) {
            (Engine::Refused(name), true) => Some(disabled(name)),
            (Engine::Refused(name), false) => Some(unknown(name)),
            (Engine::Substituted(name), true) if mode == Mode::Reject => Some(disabled(name)),
            (Engine::Substituted(_), false) if mode == Mode::Reject => Some(no_substitution()),
            _ => None,
        };
        let has_table_engine = self.clauses.iter().any(|c| c.table_level);
        // The session's default engine counts for a CREATE TABLE that names
        // none.
        let default = match (self.kind, has_table_engine, session_default.map(classify)) {
            (Kind::Create, false, Some(e @ (Engine::Substituted(_) | Engine::Refused(_)))) => {
                Some(e)
            }
            _ => None,
        };
        let mut first = None;
        for e in self
            .clauses
            .iter()
            .map(|c| c.engine)
            .chain(default)
            .filter(|e| matches!(e, Engine::Substituted(_) | Engine::Refused(_)))
        {
            if let Some(err) = refuse(e) {
                return Outcome::Refused(err);
            }
            first.get_or_insert(e);
        }
        let Some(Engine::Substituted(engine)) = first else {
            return Outcome::Unchanged;
        };
        // (from, to, replacement), applied from the end.
        let mut edits: Vec<(usize, usize, &str)> = Vec::new();
        let mut notes = Vec::new();
        let created = || {
            if mysql {
                vec![
                    EngineNote {
                        level: "Warning",
                        code: 3161,
                        message: disabled(engine).message,
                    },
                    EngineNote {
                        level: "Warning",
                        code: 1266,
                        message: using_innodb(&self.table),
                    },
                ]
            } else {
                vec![EngineNote {
                    level: "Note",
                    code: 1266,
                    message: using_innodb(&self.table),
                }]
            }
        };
        for c in &self.clauses {
            if !matches!(c.engine, Engine::Substituted(_)) {
                continue;
            }
            if self.kind == Kind::Alter && mysql && c.table_level {
                // MySQL leaves the table's engine as it is.
                edits.push((c.removal.0, c.removal.1, ""));
            } else {
                edits.push((c.name.0, c.name.1, "InnoDB"));
            }
        }
        match (self.kind, mysql) {
            (Kind::Create, _) => {
                if default.is_some()
                    && let Some(at) = self.insert_at
                {
                    edits.push((at, at, " ENGINE=InnoDB"));
                }
                notes = created();
            }
            (Kind::Alter, true) => {
                if self
                    .clauses
                    .iter()
                    .any(|c| c.table_level && matches!(c.engine, Engine::Substituted(_)))
                {
                    notes.push(EngineNote {
                        level: "Warning",
                        code: 1286,
                        message: unknown(engine).message,
                    });
                }
            }
            (Kind::Alter, false) => notes = created(),
        }
        if edits.is_empty() {
            return Outcome::Unchanged;
        }
        // Overlapping removals (two clauses sharing a comma) merge.
        edits.sort_by_key(|e| (e.0, e.1));
        let mut merged: Vec<(usize, usize, &str)> = Vec::new();
        for e in edits {
            match merged.last_mut() {
                Some(last) if e.0 < last.1 && last.2.is_empty() && e.2.is_empty() => {
                    last.1 = last.1.max(e.1);
                }
                _ => merged.push(e),
            }
        }
        let mut out = Vec::with_capacity(sql.len() + 16);
        let mut at = 0;
        for (from, to, rep) in merged {
            if from < at {
                continue;
            }
            out.extend_from_slice(&sql[at..from]);
            out.extend_from_slice(rep.as_bytes());
            at = to;
        }
        out.extend_from_slice(&sql[at..]);
        Outcome::Rewritten { sql: out, notes }
    }

    /// The bytes of each engine clause (for tests and logs).
    #[must_use]
    pub fn clause_spans(&self) -> Vec<(usize, usize)> {
        self.clauses.iter().map(|c| (c.start, c.end)).collect()
    }
}

/// `Records: R  Duplicates: D  Warnings: W` with `n` more warnings.
#[must_use]
pub fn info_with_warnings(info: &str, n: usize) -> String {
    match info.rsplit_once("Warnings: ") {
        Some((head, w)) => match w.trim().parse::<usize>() {
            Ok(w) => format!("{head}Warnings: {}", w + n),
            Err(_) => info.to_owned(),
        },
        None => info.to_owned(),
    }
}

#[cfg(test)]
mod tests;
