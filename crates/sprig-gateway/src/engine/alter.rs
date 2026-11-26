//! What an `ALTER TABLE` (or `CREATE INDEX`, `DROP INDEX`) changes, as far
//! as the engine has to know it, and the plan for carrying the change over
//! to the table's rows.
//!
//! The new definition itself is not worked out here: the statement runs on
//! the database's MariaDB copy of the table (which has no rows), and the
//! engine reads the definition that results. The statement's text is read
//! only for what the two definitions can't tell:
//!
//! - which new column continues which old one (`CHANGE a b ...`, `RENAME
//!   COLUMN a TO b`), and which is new although an old one had its name
//!   (`DROP a, ADD a ...`);
//! - which index continues which (`RENAME INDEX`);
//! - the `ALGORITHM=` and `LOCK=` clauses.
//!
//! [`plan`] then compares the two definitions column by column:
//!
//! | Old to new | Rows |
//! |---|---|
//! | new column | untouched: older rows show its `DEFAULT` |
//! | dropped, renamed, moved | untouched |
//! | a widening (`record::reads_as`), `NOT NULL` to `NULL` | untouched |
//! | anything else: narrowing, another type family or character set, `NULL` to `NOT NULL`, other `ENUM` members | copied, each value converted |
//! | another primary key | copied under the new keys |

use std::collections::{HashMap, HashSet};

use sprig_common::SprigError;
use sprig_engine::keys::collation::Charset;
use sprig_engine::row::record;

use super::layout::{Ver, VerCol, members_print};
use super::row::Datum;
use super::table::{self, ColDefault, ColType, Column, TableDef, Tok};

/// One change of an `ALTER TABLE`'s list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Spec {
    /// `ADD [COLUMN] name ...` (one per column of `ADD (a ..., b ...)`).
    AddColumn(String),
    /// `DROP [COLUMN] name`.
    DropColumn(String),
    /// `CHANGE [COLUMN] old new ...`, `RENAME COLUMN old TO new`.
    Rename {
        /// The column's name before.
        old: String,
        /// Its name after.
        new: String,
    },
    /// `MODIFY [COLUMN] name ...`.
    Modify(String),
    /// `ALTER [COLUMN] name SET DEFAULT ... | DROP DEFAULT`.
    ColumnDefault(String),
    /// `ADD INDEX`, `ADD UNIQUE`, `CREATE INDEX`.
    AddIndex,
    /// `ADD PRIMARY KEY`.
    AddPrimary,
    /// `DROP INDEX name`.
    DropIndex(String),
    /// `DROP PRIMARY KEY`.
    DropPrimary,
    /// `RENAME INDEX old TO new`.
    RenameIndex {
        /// The index's name before.
        old: String,
        /// Its name after.
        new: String,
    },
    /// `AUTO_INCREMENT = n`.
    AutoIncrement,
    /// `FORCE`, `ENGINE = x`: the servers rebuild the table.
    Rebuild,
    /// `ADD [CONSTRAINT name] CHECK (...)`.
    AddCheck,
    /// `DROP CONSTRAINT name`, `DROP CHECK name`.
    DropConstraint(String),
    /// `ADD ... FOREIGN KEY`, `DROP FOREIGN KEY`.
    ForeignKey,
    /// Anything else, by its first words.
    Other(String),
}

/// An `ALTER TABLE` as the engine reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Alter {
    /// The changes, in order.
    pub specs: Vec<Spec>,
    /// `ALGORITHM = x`, upper case.
    pub algorithm: Option<String>,
    /// `LOCK = x`, upper case.
    pub lock: Option<String>,
}

fn word(t: Option<&Tok>) -> Option<&str> {
    match t {
        Some(Tok::Word(w)) => Some(w.as_str()),
        _ => None,
    }
}

fn name(t: Option<&Tok>) -> Option<String> {
    match t {
        Some(Tok::Ident(s) | Tok::Word(s)) => Some(s.to_lowercase()),
        _ => None,
    }
}

/// `toks` split at its commas outside parentheses.
fn split(toks: &[Tok]) -> Vec<&[Tok]> {
    let mut out = Vec::new();
    let (mut depth, mut start) = (0usize, 0usize);
    for (i, t) in toks.iter().enumerate() {
        match t {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth = depth.saturating_sub(1),
            Tok::Punct(',') if depth == 0 => {
                out.push(&toks[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < toks.len() {
        out.push(&toks[start..]);
    }
    out
}

/// Skips `IF [NOT] EXISTS` at `i`.
fn skip_if(s: &[Tok], mut i: usize) -> usize {
    if word(s.get(i)) == Some("IF") {
        i += 1;
        if word(s.get(i)) == Some("NOT") {
            i += 1;
        }
        if word(s.get(i)) == Some("EXISTS") {
            i += 1;
        }
    }
    i
}

/// The value of `KEYWORD [=] value`.
fn option_value(s: &[Tok]) -> Option<String> {
    let mut i = 1;
    if s.get(i) == Some(&Tok::Punct('=')) {
        i += 1;
    }
    match s.get(i) {
        Some(Tok::Word(w) | Tok::Ident(w) | Tok::Str(w)) => Some(w.to_uppercase()),
        _ => None,
    }
}

const KEY_WORDS: &[&str] = &[
    "INDEX",
    "KEY",
    "UNIQUE",
    "PRIMARY",
    "FULLTEXT",
    "SPATIAL",
    "VECTOR",
    "CONSTRAINT",
    "FOREIGN",
    "CHECK",
    "PERIOD",
    "PARTITION",
    "SYSTEM",
];

fn other(s: &[Tok]) -> Spec {
    let w: Vec<&str> = s.iter().take(3).filter_map(|t| word(Some(t))).collect();
    Spec::Other(w.join(" "))
}

fn add(s: &[Tok], out: &mut Vec<Spec>) {
    let mut i = 1;
    let column = word(s.get(i)) == Some("COLUMN");
    if column {
        i += 1;
    }
    i = skip_if(s, i);
    let unquoted_key =
        !column && matches!(s.get(i), Some(Tok::Word(w)) if KEY_WORDS.contains(&w.as_str()));
    if !unquoted_key {
        // `ADD (a INT, b INT)`: one column per element.
        if s.get(i) == Some(&Tok::Punct('(')) {
            let inner = &s[i + 1..s.len().saturating_sub(1)];
            for e in split(inner) {
                if let Some(n) = name(e.first()) {
                    out.push(Spec::AddColumn(n));
                }
            }
            return;
        }
        match name(s.get(i)) {
            Some(n) => out.push(Spec::AddColumn(n)),
            None => out.push(other(s)),
        }
        return;
    }
    // `[CONSTRAINT [name]]` first.
    if word(s.get(i)) == Some("CONSTRAINT") {
        i += 1;
        i = skip_if(s, i);
        if !matches!(
            word(s.get(i)),
            Some("UNIQUE" | "PRIMARY" | "FOREIGN" | "CHECK")
        ) {
            i += 1;
        }
    }
    out.push(match word(s.get(i)) {
        Some("INDEX" | "KEY" | "UNIQUE") => Spec::AddIndex,
        Some("PRIMARY") => Spec::AddPrimary,
        Some("FOREIGN") => Spec::ForeignKey,
        Some("CHECK") => Spec::AddCheck,
        _ => other(s),
    });
}

fn drop(s: &[Tok]) -> Spec {
    let mut i = 1;
    match word(s.get(i)) {
        Some("PRIMARY") if word(s.get(i + 1)) == Some("KEY") => return Spec::DropPrimary,
        Some("INDEX" | "KEY") => {
            i = skip_if(s, i + 1);
            return name(s.get(i)).map_or_else(|| other(s), Spec::DropIndex);
        }
        Some("FOREIGN") => return Spec::ForeignKey,
        Some("CONSTRAINT" | "CHECK") => {
            i = skip_if(s, i + 1);
            return name(s.get(i)).map_or_else(|| other(s), Spec::DropConstraint);
        }
        Some("PARTITION" | "SYSTEM" | "PERIOD") => return other(s),
        Some("COLUMN") => i += 1,
        _ => {}
    }
    i = skip_if(s, i);
    name(s.get(i)).map_or_else(|| other(s), Spec::DropColumn)
}

fn spec(s: &[Tok], a: &mut Alter) {
    let Some(first) = word(s.first()) else {
        a.specs.push(other(s));
        return;
    };
    match first {
        "ADD" => add(s, &mut a.specs),
        "DROP" => a.specs.push(drop(s)),
        "MODIFY" => {
            let mut i = 1;
            if word(s.get(i)) == Some("COLUMN") {
                i += 1;
            }
            i = skip_if(s, i);
            a.specs
                .push(name(s.get(i)).map_or_else(|| other(s), Spec::Modify));
        }
        "CHANGE" => {
            let mut i = 1;
            if word(s.get(i)) == Some("COLUMN") {
                i += 1;
            }
            i = skip_if(s, i);
            a.specs.push(match (name(s.get(i)), name(s.get(i + 1))) {
                (Some(old), Some(new)) => Spec::Rename { old, new },
                _ => other(s),
            });
        }
        "RENAME" => match word(s.get(1)) {
            Some("COLUMN") => {
                let i = skip_if(s, 2);
                let to = usize::from(word(s.get(i + 1)) == Some("TO"));
                a.specs
                    .push(match (name(s.get(i)), name(s.get(i + 1 + to))) {
                        (Some(old), Some(new)) => Spec::Rename { old, new },
                        _ => other(s),
                    });
            }
            Some("INDEX" | "KEY") => {
                let i = skip_if(s, 2);
                let to = usize::from(word(s.get(i + 1)) == Some("TO"));
                a.specs
                    .push(match (name(s.get(i)), name(s.get(i + 1 + to))) {
                        (Some(old), Some(new)) => Spec::RenameIndex { old, new },
                        _ => other(s),
                    });
            }
            _ => a.specs.push(Spec::Other("RENAME".into())),
        },
        "ALTER" => {
            let mut i = 1;
            match word(s.get(i)) {
                Some("INDEX" | "KEY" | "CHECK" | "CONSTRAINT") => {
                    a.specs.push(other(s));
                    return;
                }
                Some("COLUMN") => i += 1,
                _ => {}
            }
            i = skip_if(s, i);
            a.specs
                .push(name(s.get(i)).map_or_else(|| other(s), Spec::ColumnDefault));
        }
        "ALGORITHM" => a.algorithm = option_value(s),
        "LOCK" => a.lock = option_value(s),
        "AUTO_INCREMENT" => a.specs.push(Spec::AutoIncrement),
        "FORCE" | "ENGINE" => a.specs.push(Spec::Rebuild),
        _ => a.specs.push(other(s)),
    }
}

/// Reads an `ALTER TABLE`, `CREATE INDEX` or `DROP INDEX` statement.
/// `None` when it is none of them.
pub(crate) fn read(text: &str) -> Option<Alter> {
    let t = table::tokens(text)?;
    let mut a = Alter::default();
    let w = |i: usize| word(t.get(i));
    match w(0)? {
        "ALTER" => {
            let mut i = 1;
            while matches!(w(i), Some("ONLINE" | "IGNORE")) {
                i += 1;
            }
            if w(i) != Some("TABLE") {
                return None;
            }
            i = skip_if(&t, i + 1);
            // The table's name: `t` or `db.t`.
            i += 1;
            while t.get(i) == Some(&Tok::Punct('.')) {
                i += 2;
            }
            // `WAIT n`, `NOWAIT`.
            match w(i) {
                Some("WAIT") => i += 2,
                Some("NOWAIT") => i += 1,
                _ => {}
            }
            for s in split(t.get(i..)?) {
                spec(s, &mut a);
            }
        }
        "CREATE" => {
            // `CREATE [OR REPLACE] [UNIQUE|...] INDEX ... ON t (...) [options]`.
            if !t.iter().take(6).any(|x| word(Some(x)) == Some("INDEX")) {
                return None;
            }
            a.specs.push(Spec::AddIndex);
            index_options(&t, &mut a);
        }
        "DROP" => {
            if w(1) != Some("INDEX") {
                return None;
            }
            let i = skip_if(&t, 2);
            a.specs.push(Spec::DropIndex(name(t.get(i))?));
            index_options(&t, &mut a);
        }
        _ => return None,
    }
    Some(a)
}

/// Whether `text` only adds plain (non-unique) secondary indexes, as
/// `ALTER TABLE t ADD INDEX ...[, ADD KEY ...]` or `CREATE INDEX ... ON t`,
/// with no `ALGORITHM = COPY` and no `LOCK = SHARED | EXCLUSIVE`, and none
/// of them `IGNORED` / `INVISIBLE`: such a change is built online (the
/// table stays writable while the entries are written).
pub(crate) fn adds_plain_indexes_only(text: &str) -> bool {
    let Some(a) = read(text) else {
        return false;
    };
    if a.specs.is_empty() || a.specs.iter().any(|s| *s != Spec::AddIndex) {
        return false;
    }
    if a.algorithm.as_deref() == Some("COPY")
        || matches!(a.lock.as_deref(), Some("SHARED" | "EXCLUSIVE"))
    {
        return false;
    }
    let Some(t) = table::tokens(text) else {
        return false;
    };
    !t.iter().any(|x| {
        matches!(
            word(Some(x)),
            Some(
                "UNIQUE"
                    | "FULLTEXT"
                    | "SPATIAL"
                    | "VECTOR"
                    | "PRIMARY"
                    | "FOREIGN"
                    | "CONSTRAINT"
                    | "CHECK"
                    | "IGNORED"
                    | "INVISIBLE"
                    | "COLUMN"
                    | "IF"
                    | "COPY"
                    | "SHARED"
                    | "EXCLUSIVE"
            )
        )
    })
}

/// The `ALGORITHM` and `LOCK` options at the end of `CREATE INDEX` and
/// `DROP INDEX`.
fn index_options(t: &[Tok], a: &mut Alter) {
    let mut depth = 0usize;
    for (i, x) in t.iter().enumerate() {
        match x {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth = depth.saturating_sub(1),
            Tok::Word(w) if depth == 0 && w == "ALGORITHM" => {
                a.algorithm = option_value(&t[i..]);
            }
            Tok::Word(w) if depth == 0 && w == "LOCK" => a.lock = option_value(&t[i..]),
            _ => {}
        }
    }
}

/// Where a column of the new definition gets its value in a row written
/// under the old one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Src {
    /// A new column: its missing value.
    Added,
    /// A new column whose `DEFAULT` expression is worked out for each row.
    Computed,
    /// The old column at this position, read as it is (or widened).
    Kept(usize),
    /// The old column at this position, converted.
    Converted(usize),
}

/// The plan for one definition change.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)] // independent facts about one change
pub(crate) struct Plan {
    /// The new definition's column list.
    pub ver: Ver,
    /// Per column of the new definition, where its value comes from.
    pub src: Vec<Src>,
    /// A column must be converted: every row is copied.
    pub convert: bool,
    /// The servers report the rows as copied (`Records: n`) for this
    /// change.
    pub reports_rows: bool,
    /// A column's type changes in a way both servers copy the table for.
    pub copies_type: bool,
    /// A column's type or nullability changes at all.
    pub changes_column: bool,
    /// The next column id to give after this change.
    pub next_col: u32,
}

/// Whether both servers change `from` into `to` without copying the table:
/// a longer `VARCHAR` or `VARBINARY` whose length still takes as many
/// length bytes, or more `ENUM` / `SET` members in as many bytes.
fn extends_in_place(from: &Column, to: &Column) -> bool {
    let class = |bytes: u64| bytes > 255;
    match (from.ty, to.ty) {
        (
            ColType::VarChar {
                chars: a,
                collation: ca,
            },
            ColType::VarChar {
                chars: b,
                collation: cb,
            },
        ) => {
            let mb = match ca.charset() {
                Charset::Utf8mb4 if from.mb3 => 3,
                Charset::Utf8mb4 => 4,
                Charset::Latin1 | Charset::Ascii | Charset::Binary => 1,
            };
            ca == cb
                && from.mb3 == to.mb3
                && b >= a
                && class(u64::from(a) * mb) == class(u64::from(b) * mb)
        }
        (ColType::VarBinary { bytes: a }, ColType::VarBinary { bytes: b }) => {
            b >= a && class(u64::from(a)) == class(u64::from(b))
        }
        (ColType::Enum { members: a }, ColType::Enum { members: b }) => {
            b >= a && (a > 255) == (b > 255) && members_prefix(from, to)
        }
        (ColType::Set { members: a }, ColType::Set { members: b }) => {
            b >= a && a.div_ceil(8) == b.div_ceil(8) && members_prefix(from, to)
        }
        _ => false,
    }
}

/// Whether `to`'s members start with `from`'s.
fn members_prefix(from: &Column, to: &Column) -> bool {
    from.members.len() <= to.members.len()
        && from
            .members
            .iter()
            .zip(to.members.iter())
            .all(|(a, b)| a == b)
}

/// The fingerprint of the first `n` members of `c`.
fn print_of_first(c: &Column, n: usize) -> u64 {
    let mut head = c.clone();
    head.members = std::sync::Arc::from(c.members.iter().take(n).cloned().collect::<Vec<_>>());
    members_print(&head)
}

/// What rows written before a new column existed show for it, when that
/// is one value: its constant `DEFAULT`, else NULL or the type's own
/// default. `None` for a `DEFAULT` that has to be worked out
/// (`CURRENT_TIMESTAMP`, an expression).
pub(crate) fn plain_missing(c: &Column) -> Option<Datum> {
    if c.generated.is_some() {
        return None;
    }
    match &c.default {
        Some(ColDefault::Value(v)) if !v.is_null() => Some(v.clone()),
        Some(ColDefault::Now | ColDefault::Expr(_)) => None,
        _ if c.nullable => Some(Datum::Null),
        _ => Some(c.implicit_default()),
    }
}

/// Plans the change from the column list `old` (of definition `d0`, when
/// the engine has it) to definition `d1`. `alter`: the statement, when the
/// engine saw it. `missing`: what older rows show for a new column, or
/// `None` when its `DEFAULT` is worked out row by row.
#[allow(clippy::too_many_lines)] // one pass over the new definition's columns
pub(crate) fn plan(
    old: &Ver,
    d0: Option<&TableDef>,
    d1: &TableDef,
    alter: Option<&Alter>,
    next_col: u32,
    missing: &dyn Fn(&Column) -> Option<Datum>,
) -> Plan {
    let mut renamed: HashMap<&str, &str> = HashMap::new();
    let mut renamed_away: HashSet<&str> = HashSet::new();
    let mut dropped: HashSet<&str> = HashSet::new();
    let mut added: HashSet<&str> = HashSet::new();
    let mut copy_asked = false;
    if let Some(a) = alter {
        for s in &a.specs {
            match s {
                Spec::Rename { old, new } if old != new => {
                    renamed.insert(new.as_str(), old.as_str());
                    renamed_away.insert(old.as_str());
                }
                Spec::DropColumn(n) => {
                    dropped.insert(n.as_str());
                }
                Spec::AddColumn(n) => {
                    added.insert(n.as_str());
                }
                _ => {}
            }
        }
        copy_asked = a.algorithm.as_deref() == Some("COPY");
    }
    let position = |n: &str| old.cols.iter().position(|c| c.name == n);
    let mut next = next_col;
    let mut cols = Vec::with_capacity(d1.columns.len());
    let mut src = Vec::with_capacity(d1.columns.len());
    let mut convert = false;
    let mut copies_type = false;
    let mut changes_column = false;
    for c1 in &d1.columns {
        let n = c1.name.to_lowercase();
        let from = match renamed.get(n.as_str()) {
            Some(o) => position(o),
            None if added.contains(n.as_str())
                && (dropped.contains(n.as_str()) || renamed_away.contains(n.as_str())) =>
            {
                None
            }
            None if renamed_away.contains(n.as_str()) => None,
            None => position(&n),
        };
        let Some(i) = from else {
            let known = missing(c1);
            cols.push(VerCol {
                id: next,
                name: n,
                ty: c1.ty,
                nullable: c1.nullable,
                missing: known.clone().unwrap_or_else(|| {
                    if c1.nullable {
                        Datum::Null
                    } else {
                        c1.implicit_default()
                    }
                }),
                members: members_print(c1),
            });
            next = next.saturating_add(1);
            if known.is_some() {
                src.push(Src::Added);
            } else {
                // Every row gets its own value: the rows are copied, on
                // the servers too (which add a VIRTUAL column without).
                src.push(Src::Computed);
                convert = true;
                copies_type |= c1.generated.as_ref().is_none_or(|g| g.stored);
            }
            continue;
        };
        let o = &old.cols[i];
        let c0 = d0.and_then(|d| d.columns.get(i));
        // A generated column whose expression changed (or a column that
        // becomes generated) is worked out again for every row.
        let regenerate = c1.generated.as_ref().is_some_and(|g| {
            c0.and_then(|c| c.generated.as_ref())
                .is_none_or(|g0| g0.text != g.text)
        });
        // The members a row's number means must stay what they were.
        let members_ok = match c1.ty {
            ColType::Enum { .. } | ColType::Set { .. } => {
                let had = match o.ty {
                    ColType::Enum { members } => usize::from(members),
                    ColType::Set { members } => usize::from(members),
                    _ => 0,
                };
                o.members == print_of_first(c1, had)
            }
            _ => true,
        };
        let readable =
            (c1.nullable || !o.nullable) && members_ok && record::reads_as(&o.ty, &c1.ty);
        if o.ty != c1.ty || o.members != members_print(c1) {
            copies_type |= !c0.is_some_and(|c0| extends_in_place(c0, c1));
            changes_column = true;
        }
        changes_column |= o.nullable != c1.nullable;
        // The missing value in the new type (an unsigned column widened to
        // a signed one holds it as a signed number).
        let missing = if !readable {
            if c1.nullable {
                Datum::Null
            } else {
                c1.implicit_default()
            }
        } else if c1.ty.check(&o.missing).is_ok() {
            o.missing.clone()
        } else {
            super::assign::constant(c1, o.missing.clone()).unwrap_or_else(|| {
                if c1.nullable {
                    Datum::Null
                } else {
                    c1.implicit_default()
                }
            })
        };
        cols.push(VerCol {
            id: o.id,
            name: n,
            ty: c1.ty,
            nullable: c1.nullable,
            missing,
            members: members_print(c1),
        });
        if regenerate {
            convert = true;
            copies_type = true;
            src.push(Src::Computed);
        } else if readable {
            src.push(Src::Kept(i));
        } else {
            convert = true;
            src.push(Src::Converted(i));
        }
    }
    // The same list keeps its number: rows need no other reader.
    let same = cols.len() == old.cols.len()
        && cols.iter().zip(&old.cols).all(|(a, b)| {
            a.id == b.id
                && a.name == b.name
                && a.ty == b.ty
                && a.nullable == b.nullable
                && a.members == b.members
        });
    let ver = if same {
        old.clone()
    } else {
        Ver {
            version: old.version.saturating_add(1),
            cols,
        }
    };
    Plan {
        ver,
        src,
        convert,
        reports_rows: copy_asked || copies_type,
        copies_type,
        changes_column,
        next_col: next,
    }
}

fn unsupported(errno: u16, text: String) -> SprigError {
    SprigError::standard(errno, *b"0A000", text)
}

/// MySQL 8.4's answer to the statement's `ALGORITHM` and `LOCK` clauses
/// (on its port the clauses are taken out of the statement the MariaDB
/// copy runs, whose rules are MariaDB's).
///
/// # Errors
/// 1800, 1801 for a value MySQL doesn't know; 1845, 1846 for a clause it
/// doesn't support for this change.
pub(crate) fn mysql_clauses(a: &Alter, p: &Plan) -> Result<(), SprigError> {
    let algorithm = a.algorithm.as_deref().unwrap_or("DEFAULT");
    let lock = a.lock.as_deref().unwrap_or("DEFAULT");
    if !matches!(algorithm, "DEFAULT" | "INSTANT" | "INPLACE" | "COPY") {
        return Err(SprigError::standard(
            1800,
            *b"HY000",
            format!("Unknown ALGORITHM '{algorithm}'"),
        ));
    }
    if !matches!(lock, "DEFAULT" | "NONE" | "SHARED" | "EXCLUSIVE") {
        return Err(SprigError::standard(
            1801,
            *b"HY000",
            format!("Unknown LOCK type '{lock}'"),
        ));
    }
    let drops_pk_only =
        a.specs.contains(&Spec::DropPrimary) && !a.specs.contains(&Spec::AddPrimary);
    match algorithm {
        "INSTANT" => {
            if p.copies_type {
                return Err(unsupported(1846, "ALGORITHM=INSTANT is not supported. Reason: Need to rebuild the table to change column type. Try ALGORITHM=COPY/INPLACE.".into()));
            }
            let instant = !p.changes_column
                && a.specs.iter().all(|s| {
                    matches!(
                        s,
                        Spec::AddColumn(_)
                            | Spec::DropColumn(_)
                            | Spec::Rename { .. }
                            | Spec::Modify(_)
                            | Spec::ColumnDefault(_)
                    )
                });
            if !instant {
                return Err(unsupported(1845, "ALGORITHM=INSTANT is not supported for this operation. Try ALGORITHM=COPY/INPLACE.".into()));
            }
        }
        "INPLACE" => {
            if a.specs.contains(&Spec::AddCheck) {
                return Err(unsupported(
                    1845,
                    "ALGORITHM=INPLACE is not supported for this operation. Try ALGORITHM=COPY."
                        .into(),
                ));
            }
            if p.copies_type {
                return Err(unsupported(1846, "ALGORITHM=INPLACE is not supported. Reason: Cannot change column type INPLACE. Try ALGORITHM=COPY.".into()));
            }
            if drops_pk_only {
                return Err(unsupported(1846, "ALGORITHM=INPLACE is not supported. Reason: Dropping a primary key is not allowed without also adding a new primary key. Try ALGORITHM=COPY.".into()));
            }
        }
        _ => {}
    }
    if lock == "NONE" {
        if algorithm == "COPY" {
            return Err(unsupported(1846, "LOCK=NONE is not supported. Reason: COPY algorithm requires a lock. Try LOCK=SHARED.".into()));
        }
        if p.copies_type {
            return Err(unsupported(1846, "LOCK=NONE is not supported. Reason: Cannot change column type INPLACE. Try LOCK=SHARED.".into()));
        }
    }
    Ok(())
}

/// MariaDB 11.8's rules for `ALGORITHM` and `LOCK` (observed): `NOCOPY` is
/// an algorithm too; a column type the table must be copied for refuses
/// every algorithm but `COPY` (1846), making a column `NOT NULL` isn't
/// `INSTANT` (1845), a new `CHECK` isn't `INPLACE`, `NOCOPY` or `INSTANT`
/// (1845), a new index isn't `INSTANT` (1845); `LOCK=NONE` is accepted
/// with every algorithm.
pub(crate) fn mariadb_clauses(a: &Alter, p: &Plan, tightens: bool) -> Result<(), SprigError> {
    let algorithm = a.algorithm.as_deref().unwrap_or("DEFAULT");
    let lock = a.lock.as_deref().unwrap_or("DEFAULT");
    if !matches!(
        algorithm,
        "DEFAULT" | "INSTANT" | "INPLACE" | "NOCOPY" | "COPY"
    ) {
        return Err(SprigError::standard(
            1800,
            *b"HY000",
            format!("Unknown ALGORITHM '{algorithm}'"),
        ));
    }
    if !matches!(lock, "DEFAULT" | "NONE" | "SHARED" | "EXCLUSIVE") {
        return Err(SprigError::standard(
            1801,
            *b"HY000",
            format!("Unknown LOCK type '{lock}'"),
        ));
    }
    if matches!(algorithm, "DEFAULT" | "COPY") {
        return Ok(());
    }
    if p.copies_type {
        return Err(unsupported(
            1846,
            format!(
                "ALGORITHM={algorithm} is not supported. Reason: Cannot change column type. Try ALGORITHM=COPY"
            ),
        ));
    }
    if a.specs.contains(&Spec::AddCheck) {
        return Err(unsupported(
            1845,
            format!(
                "ALGORITHM={algorithm} is not supported for this operation. Try ALGORITHM=COPY"
            ),
        ));
    }
    if algorithm == "INSTANT" {
        if tightens {
            return Err(unsupported(
                1845,
                "ALGORITHM=INSTANT is not supported for this operation. Try ALGORITHM=INPLACE"
                    .into(),
            ));
        }
        let reason = a.specs.iter().find_map(|s| match s {
            Spec::AddIndex | Spec::AddPrimary => Some("ADD INDEX"),
            Spec::DropIndex(_) | Spec::DropPrimary => Some("DROP INDEX"),
            _ => None,
        });
        if let Some(r) = reason {
            return Err(unsupported(
                1846,
                format!("ALGORITHM=INSTANT is not supported. Reason: {r}. Try ALGORITHM=NOCOPY"),
            ));
        }
    }
    Ok(())
}

/// Whether the statement drops a column of the primary key, or changes
/// the primary key (the rows are keyed anew).
pub(crate) fn rekeys(d0: &TableDef, old: &Ver, d1: &TableDef, p: &Plan) -> bool {
    if d0.pk.len() != d1.pk.len() {
        return true;
    }
    d0.pk
        .iter()
        .zip(&d1.pk)
        .zip(d0.pk_parts.iter().zip(&d1.pk_parts))
        .any(|((&a, &b), (ka, kb))| {
            old.cols.get(a).map(|c| c.id) != p.ver.cols.get(b).map(|c| c.id) || ka != kb
        })
}

/// Per index of `d1`, the index of `d0` whose entries it keeps: the same
/// name (or the name it was renamed from), uniqueness, columns and parts.
pub(crate) fn kept_indexes(
    d0: &TableDef,
    old: &Ver,
    d1: &TableDef,
    p: &Plan,
    alter: Option<&Alter>,
) -> Vec<Option<usize>> {
    let mut from: HashMap<String, String> = HashMap::new();
    if let Some(a) = alter {
        for s in &a.specs {
            if let Spec::RenameIndex { old, new } = s {
                from.insert(new.clone(), old.clone());
            }
        }
    }
    d1.indexes
        .iter()
        .map(|x1| {
            let n = x1.name.to_lowercase();
            let was = from.get(&n).cloned().unwrap_or(n);
            let i = d0
                .indexes
                .iter()
                .position(|x0| x0.name.to_lowercase() == was)?;
            let x0 = &d0.indexes[i];
            let same = x0.unique == x1.unique
                && x0.cols.len() == x1.cols.len()
                && x0
                    .cols
                    .iter()
                    .zip(&x1.cols)
                    .zip(x0.parts.iter().zip(&x1.parts))
                    .all(|((&a, &b), (ka, kb))| {
                        old.cols.get(a).map(|c| c.id) == p.ver.cols.get(b).map(|c| c.id)
                            && ka == kb
                            && matches!(p.src.get(b), Some(Src::Kept(_)))
                    });
            same.then_some(i)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use sprig_common::ids::{DbId, ObjectId};

    use super::*;

    fn specs(sql: &str) -> Vec<Spec> {
        read(sql).unwrap().specs
    }

    fn n(s: &str) -> String {
        s.into()
    }

    #[test]
    fn column_changes_are_read() {
        assert_eq!(
            specs(
                "ALTER TABLE t ADD COLUMN b INT, ADD c VARCHAR(8) NOT NULL DEFAULT 'a,b' AFTER id, ADD (d INT, `e` DECIMAL(6,2))"
            ),
            vec![
                Spec::AddColumn(n("b")),
                Spec::AddColumn(n("c")),
                Spec::AddColumn(n("d")),
                Spec::AddColumn(n("e"))
            ]
        );
        assert_eq!(
            specs(
                "alter table `db`.`t` drop column a, DROP b, drop column if exists c, MODIFY d BIGINT, modify column e int first"
            ),
            vec![
                Spec::DropColumn(n("a")),
                Spec::DropColumn(n("b")),
                Spec::DropColumn(n("c")),
                Spec::Modify(n("d")),
                Spec::Modify(n("e"))
            ]
        );
        assert_eq!(
            specs(
                "ALTER TABLE t CHANGE a A2 BIGINT, CHANGE COLUMN `s` s2 VARCHAR(30) DEFAULT 'zz', RENAME COLUMN b TO b2, ALTER COLUMN n SET DEFAULT 9, ALTER m DROP DEFAULT"
            ),
            vec![
                Spec::Rename {
                    old: n("a"),
                    new: n("a2")
                },
                Spec::Rename {
                    old: n("s"),
                    new: n("s2")
                },
                Spec::Rename {
                    old: n("b"),
                    new: n("b2")
                },
                Spec::ColumnDefault(n("n")),
                Spec::ColumnDefault(n("m"))
            ]
        );
    }

    #[test]
    fn key_changes_and_clauses_are_read() {
        assert!(adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX k (a, b), ADD KEY (c(4) DESC)"
        ));
        assert!(adds_plain_indexes_only(
            "CREATE INDEX k ON db.t (a) LOCK = NONE"
        ));
        assert!(adds_plain_indexes_only(
            "alter table `t` add index `unique` (a) algorithm=inplace"
        ));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD UNIQUE KEY k (a)"
        ));
        assert!(!adds_plain_indexes_only("CREATE UNIQUE INDEX k ON t (a)"));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX k (a), ADD COLUMN c INT"
        ));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX k (a) ALGORITHM = COPY"
        ));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX k (a), LOCK = SHARED"
        ));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX k (a) IGNORED"
        ));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD FULLTEXT INDEX k (a)"
        ));
        assert!(!adds_plain_indexes_only("ALTER TABLE t DROP INDEX k"));
        assert!(!adds_plain_indexes_only(
            "ALTER TABLE t ADD INDEX IF NOT EXISTS k (a)"
        ));
        let a = read("ALTER TABLE t ADD INDEX k (a, b), ADD UNIQUE KEY u (c), ADD CONSTRAINT x UNIQUE (d), ADD PRIMARY KEY (id), DROP INDEX k2, DROP KEY k3, DROP PRIMARY KEY, RENAME INDEX a TO b, RENAME KEY c TO d, ALGORITHM=inplace, LOCK = NONE").unwrap();
        assert_eq!(
            a.specs,
            vec![
                Spec::AddIndex,
                Spec::AddIndex,
                Spec::AddIndex,
                Spec::AddPrimary,
                Spec::DropIndex(n("k2")),
                Spec::DropIndex(n("k3")),
                Spec::DropPrimary,
                Spec::RenameIndex {
                    old: n("a"),
                    new: n("b")
                },
                Spec::RenameIndex {
                    old: n("c"),
                    new: n("d")
                },
            ]
        );
        assert_eq!(a.algorithm.as_deref(), Some("INPLACE"));
        assert_eq!(a.lock.as_deref(), Some("NONE"));
        assert_eq!(
            specs(
                "ALTER TABLE t AUTO_INCREMENT = 100, FORCE, ENGINE=InnoDB, ADD CONSTRAINT c CHECK (a > 0), ADD CHECK (b < 3), DROP CONSTRAINT c, ADD FOREIGN KEY (a) REFERENCES p (id), RENAME TO u, COMMENT = 'x'"
            ),
            vec![
                Spec::AutoIncrement,
                Spec::Rebuild,
                Spec::Rebuild,
                Spec::AddCheck,
                Spec::AddCheck,
                Spec::DropConstraint(n("c")),
                Spec::ForeignKey,
                Spec::Other(n("RENAME")),
                Spec::Other(n("COMMENT"))
            ]
        );
        let c = read("CREATE UNIQUE INDEX i ON t (a, b) ALGORITHM=INPLACE LOCK=SHARED").unwrap();
        assert_eq!(c.specs, vec![Spec::AddIndex]);
        assert_eq!(
            (c.algorithm.as_deref(), c.lock.as_deref()),
            (Some("INPLACE"), Some("SHARED"))
        );
        assert_eq!(specs("DROP INDEX `i` ON t"), vec![Spec::DropIndex(n("i"))]);
        assert!(read("SELECT 1").is_none());
        assert!(read("ALTER VIEW v AS SELECT 1").is_none());
    }

    fn def(sql: &str) -> std::sync::Arc<TableDef> {
        table::parse(ObjectId(3), 1, DbId(1), sql).unwrap()
    }

    const T0: &str = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

    fn planned(d1: &str, sql: &str) -> Plan {
        let d0 = def(T0);
        let a = read(sql).unwrap();
        plan(
            &Ver::first(&d0),
            Some(&d0),
            &def(d1),
            Some(&a),
            5,
            &plain_missing,
        )
    }

    #[test]
    fn columns_keep_their_ids_through_renames_moves_and_drops() {
        // `a` renamed and widened, `s` dropped, a new `s` added, `b` added first.
        let p = planned(
            "CREATE TABLE `t` (\n  `b` int(11) NOT NULL DEFAULT 7,\n  `id` int(11) NOT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  `a2` bigint(20) DEFAULT NULL,\n  `s` int(11) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t ADD b INT NOT NULL DEFAULT 7 FIRST, CHANGE a a2 BIGINT AFTER n, DROP s, ADD s INT",
        );
        let ids: Vec<u32> = p.ver.cols.iter().map(|c| c.id).collect();
        assert_eq!(ids, vec![5, 1, 4, 2, 6]);
        assert_eq!(
            p.src,
            vec![
                Src::Added,
                Src::Kept(0),
                Src::Kept(3),
                Src::Kept(1),
                Src::Added
            ]
        );
        assert_eq!(p.ver.cols[0].missing, Datum::Int(7));
        assert_eq!(p.ver.cols[4].missing, Datum::Null);
        assert_eq!((p.ver.version, p.convert, p.next_col), (2, false, 7));
        // int to bigint: the servers copy the table; the engine reads through.
        assert!(p.reports_rows);
    }

    #[test]
    fn narrowing_and_not_null_convert_and_in_place_changes_report_nothing() {
        let p = planned(
            "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` smallint(6) NOT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t MODIFY a SMALLINT NOT NULL",
        );
        assert_eq!(p.src[1], Src::Converted(1));
        assert!(p.convert && p.reports_rows);
        // NULL to NOT NULL alone: converted, but the servers report no rows.
        let p = planned(
            "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) NOT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t MODIFY a INT NOT NULL",
        );
        assert!(p.convert && !p.reports_rows);
        // A longer VARCHAR within one length byte, a default: nothing.
        let p = planned(
            "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(20) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 9,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t MODIFY s VARCHAR(20), ALTER n SET DEFAULT 9",
        );
        assert!(!p.convert && !p.reports_rows);
        assert_eq!(p.ver.version, 2);
        // Past 255 bytes the servers copy; the engine still reads through.
        let p = planned(
            "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL,\n  `s` varchar(64) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t MODIFY s VARCHAR(64)",
        );
        assert!(!p.convert && p.reports_rows);
        // Only a default changed: the same list, the same number.
        let p = planned(T0, "ALTER TABLE t ALTER n SET DEFAULT 3, ALGORITHM=COPY");
        assert_eq!(p.ver.version, 1);
        assert!(p.reports_rows);
    }

    #[test]
    fn indexes_keep_their_entries_unless_their_columns_change() {
        let d0 = def(T0);
        let v0 = Ver::first(&d0);
        let go = |d1: &str, sql: &str| {
            let d1 = def(d1);
            let a = read(sql).unwrap();
            let p = plan(&v0, Some(&d0), &d1, Some(&a), 5, &plain_missing);
            (
                kept_indexes(&d0, &v0, &d1, &p, Some(&a)),
                rekeys(&d0, &v0, &d1, &p),
            )
        };
        // Renamed index, renamed column: the entries stay.
        let (k, r) = go(
            "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `a9` int(11) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `kb` (`a9`),\n  KEY `ks` (`s`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t RENAME COLUMN a TO a9, RENAME INDEX ka TO kb, ADD INDEX ks (s)",
        );
        assert_eq!((k, r), (vec![Some(0), None], false));
        // The column's type changed: the index is built anew.
        let (k, r) = go(
            "CREATE TABLE `t` (\n  `id` bigint(20) NOT NULL,\n  `a` bigint(20) DEFAULT NULL,\n  `s` varchar(10) DEFAULT NULL,\n  `n` int(11) NOT NULL DEFAULT 3,\n  PRIMARY KEY (`id`),\n  KEY `ka` (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
            "ALTER TABLE t MODIFY a BIGINT, MODIFY id BIGINT NOT NULL",
        );
        assert_eq!((k, r), (vec![None], true));
    }
}
