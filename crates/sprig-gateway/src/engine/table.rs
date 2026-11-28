//! An engine table's definition, read from the catalog's `SHOW CREATE
//! TABLE` text of its current schema version (MariaDB's wording: the
//! catalog's own server printed it).
//!
//! | In the text | Read as |
//! |---|---|
//! | `tinyint(4)` … `bigint(20) [unsigned]` | integers, with their display width |
//! | `bit(n)`, `decimal(m,d)`, `float[(m,d)]`, `double[(m,d)]` | numbers |
//! | `date`, `time(f)`, `datetime(f)`, `timestamp(f)`, `year(4)` | temporals |
//! | `char(n)`, `varchar(n)`, `tinytext` … `longtext` | character strings, in the column's collation |
//! | `binary(n)`, `varbinary(n)`, `tinyblob` … `longblob` | byte strings |
//! | `enum(...)`, `set(...)` | members kept in the column's character set |
//! | `longtext ... CHECK (json_valid(col))` | `JSON` |
//!
//! A collation is the engine's (`sprig_engine::keys::collation`), looked
//! up by the name in the text. The `utf8mb4_0900_*` names mean MySQL 8.4's
//! collations (the name is what a table keeps of where it came from);
//! every other name means MariaDB 11.8's.
//!
//! A table with anything the engine doesn't serve yet (a type family not
//! yet proven against the servers, an unknown collation, a generated
//! column, a foreign key, another storage engine, partitions) is kept with
//! the reason, and its statements fail with `ER_SPRIG_NOT_ON_ENGINE`.
//! [`check_create`] refuses the common cases before the table is created.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::{DbId, ObjectId};
use sprig_engine::keys::collation::{Charset, Collation, Flavor};
use sprig_engine::keys::typed::KeyColumn;
use sprig_engine::row::record::{Column as RecordColumn, RowSchema};
use sprig_engine::row::{Date, DateTime, Decimal, Fbt, LobSize, Time, Timestamp};
use sprig_proto_mysql::resultset::ColumnFlags;

use super::expr::{self, CExpr, Compiler};
use super::row::Datum;

/// A column's type: the engine's.
pub(crate) use sprig_engine::row::ColumnType as ColType;

/// The type families, in the order they were proven against the servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Family {
    /// `TINYINT` to `BIGINT`, `BOOL`.
    Integer,
    /// `CHAR`, `VARCHAR`, the `TEXT` types.
    Text,
    /// `DECIMAL`.
    Decimal,
    /// `FLOAT`, `DOUBLE`.
    Float,
    /// `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR`.
    Temporal,
    /// `BINARY`, `VARBINARY`, the `BLOB` types.
    Binary,
    /// `ENUM`, `SET`.
    Members,
    /// `BIT`.
    Bit,
    /// `JSON`.
    Json,
    /// MariaDB's `UUID`, `INET4`, `INET6`.
    Fixed,
}

impl Family {
    /// The family of `ty`.
    pub(crate) fn of(ty: ColType) -> Self {
        match ty {
            ColType::Int { .. } => Family::Integer,
            ColType::Bit { .. } => Family::Bit,
            ColType::Decimal { .. } => Family::Decimal,
            ColType::Float | ColType::Double => Family::Float,
            ColType::Date
            | ColType::Time { .. }
            | ColType::DateTime { .. }
            | ColType::Timestamp { .. }
            | ColType::Year => Family::Temporal,
            ColType::Char { .. } | ColType::VarChar { .. } | ColType::Text { .. } => Family::Text,
            ColType::Binary { .. } | ColType::VarBinary { .. } | ColType::Blob { .. } => {
                Family::Binary
            }
            ColType::Enum { .. } | ColType::Set { .. } => Family::Members,
            ColType::Json => Family::Json,
            ColType::Fbt(_) => Family::Fixed,
        }
    }

    /// Whether tables with columns of this family are served: its
    /// definitions, storage, assignment rules and results are checked
    /// against both servers.
    pub(crate) fn served(self) -> bool {
        SERVED.contains(&self)
    }

    fn what(self) -> &'static str {
        match self {
            Family::Integer => "integer columns",
            Family::Text => "character string columns",
            Family::Decimal => "DECIMAL columns",
            Family::Float => "FLOAT and DOUBLE columns",
            Family::Temporal => "date and time columns",
            Family::Binary => "BINARY, VARBINARY and BLOB columns",
            Family::Members => "ENUM and SET columns",
            Family::Bit => "BIT columns",
            Family::Json => "JSON columns",
            Family::Fixed => "UUID, INET4 and INET6 columns",
        }
    }
}

/// The families served so far.
const SERVED: &[Family] = &[
    Family::Integer,
    Family::Text,
    Family::Decimal,
    Family::Float,
    Family::Temporal,
    Family::Binary,
    Family::Members,
    Family::Bit,
    Family::Json,
    Family::Fixed,
];

/// A column's `DEFAULT`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColDefault {
    /// A constant, in the column's stored form.
    Value(Datum),
    /// `CURRENT_TIMESTAMP`, at the column's precision.
    Now,
    /// An expression (`DEFAULT (a + 1)`).
    Expr(DefaultExpr),
}

/// A `DEFAULT` expression, compiled against its table.
#[derive(Debug, Clone)]
pub(crate) struct DefaultExpr {
    /// The expression.
    pub expr: Arc<CExpr>,
    /// It reads no column and no clock: one value for every row.
    pub constant: bool,
}

impl PartialEq for DefaultExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.expr, &other.expr)
    }
}

/// A generated column's expression (`GENERATED ALWAYS AS (...)`). The
/// engine works the value out when a row is written and stores it, for
/// `VIRTUAL` columns too: reads and indexes then need nothing special.
#[derive(Debug, Clone)]
pub(crate) struct Generated {
    /// The expression.
    pub expr: Arc<CExpr>,
    /// `STORED` (MariaDB's `PERSISTENT`); `VIRTUAL` otherwise.
    pub stored: bool,
    /// The expression's text, as the definition has it.
    pub text: String,
}

/// A `CHECK` constraint.
#[derive(Debug, Clone)]
pub(crate) struct Check {
    /// Its name as MariaDB's error names it: the constraint's name, or
    /// `table.column` for a column's own check.
    pub name: String,
    /// Its name as MySQL's error names it: the constraint's name, or
    /// `table_chk_n` for one the client didn't name.
    pub mysql_name: String,
    /// The column it was declared on; `None` for a table constraint.
    pub column: Option<usize>,
    /// Its expression's text, as the definition has it.
    pub text: String,
    /// The expression: a row passes unless it is false.
    pub expr: CExpr,
}

/// One column.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // a column definition's independent attributes
pub(crate) struct Column {
    /// The name as defined.
    pub name: String,
    /// The type.
    pub ty: ColType,
    /// `NULL` allowed.
    pub nullable: bool,
    /// The `DEFAULT` clause; `None` without one.
    pub default: Option<ColDefault>,
    /// `ON UPDATE CURRENT_TIMESTAMP`.
    pub on_update_now: bool,
    /// `AUTO_INCREMENT`.
    pub auto_inc: bool,
    /// An integer's display width as declared (`int(11)`); 0 for other
    /// types.
    pub width: u32,
    /// `UNSIGNED` on a `DECIMAL`, `FLOAT` or `DOUBLE` (an integer's is in
    /// its type).
    pub unsigned: bool,
    /// `FLOAT(M,D)` or `DOUBLE(M,D)`.
    pub float_md: Option<(u8, u8)>,
    /// The members of an `ENUM` or `SET`, in the column's character set.
    pub members: Arc<[Bytes]>,
    /// The collation of a character string, `ENUM` or `SET` column.
    pub collation: Option<Collation>,
    /// The column's character set is `utf8mb3`: characters outside the
    /// Basic Multilingual Plane can't be stored.
    pub mb3: bool,
    /// Result-metadata flags of the base column as MariaDB reports them
    /// (keys included).
    pub flags: u16,
    /// The column's value is worked out from the row's other columns.
    pub generated: Option<Generated>,
    /// `INVISIBLE`: left out of `SELECT *` and of an `INSERT` without a
    /// column list (both servers).
    pub invisible: bool,
}

impl Column {
    /// The stored value of a `DEFAULT` constant, if the column has one.
    #[cfg(test)]
    pub(crate) fn default_value(&self) -> Option<&Datum> {
        match &self.default {
            Some(ColDefault::Value(v)) => Some(v),
            _ => None,
        }
    }

    /// What the column holds when a statement gives it nothing and it has
    /// no `DEFAULT`: zero, the empty string, the zero date, an `ENUM`'s
    /// first member.
    pub(crate) fn implicit_default(&self) -> Datum {
        implicit_default(self.ty)
    }
}

fn implicit_default(ty: ColType) -> Datum {
    match ty {
        ColType::Int {
            unsigned: false, ..
        } => Datum::Int(0),
        ColType::Int { unsigned: true, .. } => Datum::UInt(0),
        ColType::Bit { .. } => Datum::Bit(0),
        ColType::Decimal { scale, .. } => {
            Datum::Decimal(Decimal::zero().with_scale(scale).unwrap_or_default())
        }
        ColType::Float => Datum::Float(0.0),
        ColType::Double => Datum::Double(0.0),
        ColType::Date => Datum::Date(Date::default()),
        ColType::Time { .. } => Datum::Time(Time::default()),
        ColType::DateTime { .. } => Datum::DateTime(DateTime::default()),
        ColType::Timestamp { .. } => Datum::Timestamp(Timestamp::default()),
        ColType::Year => Datum::Year(0),
        ColType::Char { .. } | ColType::VarChar { .. } | ColType::Text { .. } => {
            Datum::Str(Bytes::new())
        }
        ColType::Binary { bytes } => Datum::Bytes(Bytes::from(vec![0u8; usize::from(bytes)])),
        ColType::VarBinary { .. } | ColType::Blob { .. } => Datum::Bytes(Bytes::new()),
        ColType::Enum { .. } => Datum::Enum(1),
        ColType::Set { .. } => Datum::Set(0),
        ColType::Json => Datum::Json(Bytes::from_static(b"null")),
        ColType::Fbt(f) => Datum::Bytes(Bytes::from(vec![0u8; f.bytes()])),
    }
}

/// A secondary index.
#[derive(Debug, Clone)]
pub(crate) struct Index {
    /// Its name.
    pub name: String,
    /// Key columns, in order.
    pub cols: Vec<usize>,
    /// How each key column is encoded: its type, prefix length and
    /// direction. Index parts always carry a NULL marker.
    pub parts: Vec<KeyColumn>,
    /// `UNIQUE`.
    pub unique: bool,
    /// MariaDB's `IGNORED`: every write keeps its entries, no plan reads
    /// them (an index being built online, or one an operator set aside).
    pub ignored: bool,
}

/// What a foreign key does when its parent row is deleted or its key
/// changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FkAction {
    /// `RESTRICT`.
    Restrict,
    /// `NO ACTION` (the same as `RESTRICT`: checked at once).
    NoAction,
    /// `CASCADE`.
    Cascade,
    /// `SET NULL`.
    SetNull,
    /// `SET DEFAULT`: both servers accept it and act as `RESTRICT`.
    SetDefault,
}

impl FkAction {
    fn read(p: &mut P) -> Option<Self> {
        Some(if p.word("CASCADE") {
            Self::Cascade
        } else if p.word("RESTRICT") {
            Self::Restrict
        } else if p.word("SET") {
            if p.word("NULL") {
                Self::SetNull
            } else if p.word("DEFAULT") {
                Self::SetDefault
            } else {
                return None;
            }
        } else if p.word("NO") && p.word("ACTION") {
            Self::NoAction
        } else {
            return None;
        })
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Restrict => "RESTRICT",
            Self::NoAction => "NO ACTION",
            Self::Cascade => "CASCADE",
            Self::SetNull => "SET NULL",
            Self::SetDefault => "SET DEFAULT",
        }
    }

    /// Whether the action changes the child rows (otherwise it refuses
    /// the parent's change while they exist).
    pub(crate) fn acts(self) -> bool {
        matches!(self, Self::Cascade | Self::SetNull)
    }
}

/// A foreign key of the table (the child).
#[derive(Debug, Clone)]
pub(crate) struct ForeignKey {
    /// The constraint's name.
    pub name: String,
    /// The child's columns, in the key's order.
    pub cols: Vec<usize>,
    /// The parent's database as the definition names it (`None`: the
    /// child's own).
    pub ref_db: Option<String>,
    /// The parent table's name.
    pub ref_table: String,
    /// The parent's columns, by name, in the key's order.
    pub ref_cols: Vec<String>,
    /// `ON DELETE`, as written (`None`: not written, `RESTRICT`).
    pub on_delete: Option<FkAction>,
    /// `ON UPDATE`, as written.
    pub on_update: Option<FkAction>,
}

impl ForeignKey {
    /// What deleting the parent row does.
    pub(crate) fn delete_action(&self) -> FkAction {
        self.on_delete.unwrap_or(FkAction::Restrict)
    }

    /// What changing the parent's key does.
    pub(crate) fn update_action(&self) -> FkAction {
        self.on_update.unwrap_or(FkAction::Restrict)
    }

    /// The constraint as 1451 and 1452 quote it: its definition as the
    /// session's server prints it, without `SET DEFAULT` (which neither
    /// server keeps), `RESTRICT` printed by MySQL only and `NO ACTION` by
    /// MariaDB only.
    pub(crate) fn text(&self, def: &TableDef, mysql: bool) -> String {
        let q = |n: &str| format!("`{}`", n.replace('`', "``"));
        let cols: Vec<String> = self.cols.iter().map(|&c| q(&def.columns[c].name)).collect();
        let refs: Vec<String> = self.ref_cols.iter().map(|c| q(c)).collect();
        let mut s = format!(
            "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}{} ({})",
            q(&self.name),
            cols.join(", "),
            self.ref_db
                .as_deref()
                .map(|d| format!("{}.", q(d)))
                .unwrap_or_default(),
            q(&self.ref_table),
            refs.join(", ")
        );
        let shown = |a: FkAction| match a {
            FkAction::SetDefault => false,
            FkAction::Restrict => mysql,
            FkAction::NoAction => !mysql,
            FkAction::Cascade | FkAction::SetNull => true,
        };
        for (what, a) in [("DELETE", self.on_delete), ("UPDATE", self.on_update)] {
            if let Some(a) = a.filter(|a| shown(*a)) {
                let _ = write!(s, " ON {what} {}", a.sql());
            }
        }
        s
    }
}

/// A table as the engine serves it.
#[derive(Debug, Clone)]
pub(crate) struct TableDef {
    /// The object.
    pub object: ObjectId,
    /// Its schema version.
    pub version: u64,
    /// Its database.
    pub db: DbId,
    /// The table's name.
    pub name: String,
    /// Columns in definition order.
    pub columns: Vec<Column>,
    /// The primary key's columns.
    pub pk: Vec<usize>,
    /// How each primary key column is encoded (no NULL marker).
    pub pk_parts: Vec<KeyColumn>,
    /// The primary key's name in errors: `PRIMARY`, or the unique key the
    /// servers take as the primary key of a table without one.
    pub pk_name: String,
    /// The table has neither a primary key nor a unique key that can
    /// stand for one: its rows are keyed by a hidden row number, which
    /// `pk` names as the column after the last ([`TableDef::columns`]'s
    /// length) and which a row read from the table carries as one more
    /// value at that place.
    pub hidden: bool,
    /// Secondary indexes.
    pub indexes: Vec<Index>,
    /// The `AUTO_INCREMENT` column.
    pub auto_inc: Option<usize>,
    /// The table's `AUTO_INCREMENT=n` option: the first value to give.
    pub auto_inc_start: u64,
    /// The secondary indexes' identities, in definition order (what the
    /// table's index record matches them by).
    pub index_sigs: Vec<super::indexes::Sig>,
    /// The row record's column list.
    pub schema: RowSchema,
    /// The `CHECK` constraints, in the order MariaDB evaluates them:
    /// columns' own first, then the table's.
    pub checks: Vec<Check>,
    /// The same in the order MySQL evaluates them: by name.
    pub checks_mysql: Vec<usize>,
    /// The generated columns, in definition order.
    pub generated_cols: Vec<usize>,
    /// The foreign keys, in definition order.
    pub fks: Vec<ForeignKey>,
    /// `PARTITION BY ...`: what a row may hold, and which rows a partition
    /// has.
    pub partition: Option<Arc<super::partition::Rule>>,
    /// The `FULLTEXT` keys: no entries in the keyspace, an apply-time
    /// full-text index each (`super::fulltext`).
    pub fulltext: Vec<FtKey>,
}

/// A `FULLTEXT` key of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FtKey {
    /// The key's name.
    pub name: String,
    /// Its columns' positions in the definition, in key order.
    pub cols: Vec<usize>,
    /// `WITH PARSER name` (MySQL's `ngram`), lower case.
    pub parser: Option<String>,
}

impl TableDef {
    /// The column named `name` (case-insensitive, as MySQL compares
    /// column names).
    pub(crate) fn column(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The primary key's parts as the table's keys encode them.
    pub(crate) fn pk_key(&self, _first_format: bool) -> &[KeyColumn] {
        &self.pk_parts
    }

    /// The first `CHECK` constraint (of `only`, positions in
    /// [`TableDef::checks`], when given) that `row` fails, in the order the
    /// session's server evaluates them. A constraint whose value is NULL
    /// passes.
    ///
    /// # Errors
    /// The expression's.
    pub(crate) fn failed_check(
        &self,
        row: &[Datum],
        a: &expr::Args<'_>,
        only: Option<&[usize]>,
    ) -> sprig_common::Result<Option<&Check>> {
        let mysql = a.env.mysql;
        for k in 0..self.checks.len() {
            let i = if mysql { self.checks_mysql[k] } else { k };
            if only.is_some_and(|o| !o.contains(&i)) {
                continue;
            }
            let ch = &self.checks[i];
            if super::row::truth(&expr::eval(&ch.expr, row, a)?) == Some(false) {
                return Ok(Some(ch));
            }
        }
        Ok(None)
    }

    /// The error for a row that fails `ch`: 4025 as MariaDB words it, 3819
    /// as MySQL does. `db`: the logical database name.
    pub(crate) fn check_error(
        &self,
        ch: &Check,
        db: &str,
        mysql: bool,
    ) -> sprig_common::SprigError {
        // The hidden check of a functional key part: MySQL's 3751 naming
        // the index whose part it guards.
        if mysql
            && let Some(col) = sprig_sql::compat::fxindex::fx_column_of_check(&ch.name)
            && let Some(ix) = self.indexes.iter().find(|ix| {
                ix.cols.iter().any(|&c| {
                    self.columns
                        .get(c)
                        .is_some_and(|k| k.name.eq_ignore_ascii_case(&col))
                })
            })
        {
            return sprig_common::SprigError::standard(
                3751,
                *b"01000",
                format!("Data truncated for functional index '{}' at row 1", ix.name),
            );
        }
        if mysql {
            sprig_common::SprigError::standard(
                3819,
                *b"HY000",
                format!("Check constraint '{}' is violated.", ch.mysql_name),
            )
        } else {
            sprig_common::SprigError::standard(
                4025,
                *b"23000",
                format!("CONSTRAINT `{}` failed for `{db}`.`{}`", ch.name, self.name),
            )
        }
    }

    /// Index `ix`'s parts as the table's keys encode them.
    pub(crate) fn index_key(&self, ix: usize, _first_format: bool) -> &[KeyColumn] {
        &self.indexes[ix].parts
    }
}

/// The hidden row number's type in keys: an unsigned 8-byte integer.
pub(crate) const HIDDEN_TYPE: ColType = ColType::Int {
    bytes: 8,
    unsigned: true,
};

/// A token of `SHOW CREATE TABLE` text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Tok {
    Ident(String),
    Word(String),
    Str(String),
    Num(String),
    Punct(char),
}

pub(super) fn tokens(s: &str) -> Option<Vec<Tok>> {
    let b: Vec<char> = s.chars().collect();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '`' {
            let mut v = String::new();
            i += 1;
            loop {
                let ch = *b.get(i)?;
                if ch == '`' {
                    if b.get(i + 1) == Some(&'`') {
                        v.push('`');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(ch);
                i += 1;
            }
            out.push(Tok::Ident(v));
        } else if c == '\'' {
            let mut v = String::new();
            i += 1;
            loop {
                let ch = *b.get(i)?;
                if ch == '\\' {
                    let n = *b.get(i + 1)?;
                    v.push(match n {
                        'n' => '\n',
                        't' => '\t',
                        '0' => '\0',
                        'r' => '\r',
                        'b' => '\u{8}',
                        'Z' => '\u{1a}',
                        o => o,
                    });
                    i += 2;
                    continue;
                }
                if ch == '\'' {
                    if b.get(i + 1) == Some(&'\'') {
                        v.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(ch);
                i += 1;
            }
            out.push(Tok::Str(v));
        } else if c.is_ascii_digit()
            || ((c == '-' || c == '.') && b.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            i += 1;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == '.') {
                i += 1;
            }
            // An exponent: `1e30`, `2.5e-3`.
            if i < b.len() && (b[i] == 'e' || b[i] == 'E') {
                let mut j = i + 1;
                if j < b.len() && (b[j] == '+' || b[j] == '-') {
                    j += 1;
                }
                if j < b.len() && b[j].is_ascii_digit() {
                    while j < b.len() && b[j].is_ascii_digit() {
                        j += 1;
                    }
                    i = j;
                }
            }
            out.push(Tok::Num(b[start..i].iter().collect()));
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_') {
                i += 1;
            }
            out.push(Tok::Word(
                b[start..i].iter().collect::<String>().to_ascii_uppercase(),
            ));
        } else {
            out.push(Tok::Punct(c));
            i += 1;
        }
    }
    Some(out)
}

struct P {
    t: Vec<Tok>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        self.i += 1;
        t
    }
    fn word(&mut self, w: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Word(x)) if x == w) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn punct(&mut self, c: char) -> bool {
        if self.peek() == Some(&Tok::Punct(c)) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Option<String> {
        match self.next()? {
            Tok::Ident(s) | Tok::Word(s) => Some(s),
            _ => None,
        }
    }
    fn num(&mut self) -> Option<u32> {
        match self.next()? {
            Tok::Num(n) => n.parse().ok(),
            _ => None,
        }
    }
    /// `( a [(n)] [ASC|DESC], ... )`: key parts.
    fn key_parts(&mut self) -> Option<Vec<RawPart>> {
        if !self.punct('(') {
            return None;
        }
        let mut v = Vec::new();
        loop {
            let name = self.ident()?;
            let mut prefix = None;
            if self.punct('(') {
                prefix = Some(self.num()?);
                if !self.punct(')') {
                    return None;
                }
            }
            let desc = self.word("DESC");
            if !desc {
                let _ = self.word("ASC");
            }
            v.push(RawPart { name, prefix, desc });
            if self.punct(')') {
                return Some(v);
            }
            if !self.punct(',') {
                return None;
            }
        }
    }
    /// The tokens of a bracketed group (which may hold others), from its
    /// `(` to its `)`, without the two.
    fn group(&mut self) -> Option<Vec<Tok>> {
        if !self.punct('(') {
            return None;
        }
        let mut depth = 1usize;
        let mut v = Vec::new();
        loop {
            match self.next()? {
                Tok::Punct('(') => {
                    depth += 1;
                    v.push(Tok::Punct('('));
                }
                Tok::Punct(')') => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(v);
                    }
                    v.push(Tok::Punct(')'));
                }
                t => v.push(t),
            }
        }
    }
    /// The tokens of a parenthesized list, after its `(`, up to its `)`.
    fn args(&mut self) -> Option<Vec<Tok>> {
        let mut v = Vec::new();
        loop {
            match self.next()? {
                Tok::Punct(')') => return Some(v),
                Tok::Punct('(') => return None,
                t => v.push(t),
            }
        }
    }
}

/// A foreign key as the definition's text has it.
struct RawFk {
    name: String,
    cols: Vec<String>,
    ref_db: Option<String>,
    ref_table: String,
    ref_cols: Vec<String>,
    on_delete: Option<FkAction>,
    on_update: Option<FkAction>,
}

/// `KEY (cols) REFERENCES [db.]table (cols) [ON DELETE a] [ON UPDATE a]`,
/// after `CONSTRAINT name FOREIGN`.
fn raw_fk(p: &mut P, name: String) -> Option<RawFk> {
    let names = |p: &mut P| -> Option<Vec<String>> {
        if !p.punct('(') {
            return None;
        }
        let mut v = Vec::new();
        loop {
            v.push(p.ident()?);
            if p.punct(')') {
                return Some(v);
            }
            if !p.punct(',') {
                return None;
            }
        }
    };
    if !p.word("KEY") {
        return None;
    }
    let cols = names(p)?;
    if !p.word("REFERENCES") {
        return None;
    }
    let first = p.ident()?;
    let (ref_db, ref_table) = if p.punct('.') {
        (Some(first), p.ident()?)
    } else {
        (None, first)
    };
    let ref_cols = names(p)?;
    let (mut on_delete, mut on_update) = (None, None);
    while p.word("ON") {
        if p.word("DELETE") {
            on_delete = Some(FkAction::read(p)?);
        } else if p.word("UPDATE") {
            on_update = Some(FkAction::read(p)?);
        } else {
            return None;
        }
    }
    (cols.len() == ref_cols.len() && !cols.is_empty()).then_some(RawFk {
        name,
        cols,
        ref_db,
        ref_table,
        ref_cols,
        on_delete,
        on_update,
    })
}

/// Tokens as SQL text again: an expression of the definition, for the
/// expression compiler.
fn sql_of(toks: &[Tok]) -> String {
    let mut s = String::new();
    let mut spaced = false;
    for t in toks {
        let wordy = !matches!(t, Tok::Punct(_));
        // A space unless both this token and the one before are symbols
        // (`<>`, `>=`, `((`).
        if !s.is_empty() && (wordy || spaced) {
            s.push(' ');
        }
        match t {
            Tok::Ident(i) => {
                s.push('`');
                s.push_str(&i.replace('`', "``"));
                s.push('`');
            }
            Tok::Word(w) | Tok::Num(w) => s.push_str(w),
            Tok::Str(v) => {
                s.push('\'');
                for c in v.chars() {
                    match c {
                        '\'' => s.push_str("''"),
                        '\\' => s.push_str("\\\\"),
                        '\0' => s.push_str("\\0"),
                        '\n' => s.push_str("\\n"),
                        '\r' => s.push_str("\\r"),
                        '\u{1a}' => s.push_str("\\Z"),
                        o => s.push(o),
                    }
                }
                s.push('\'');
            }
            Tok::Punct(c) => s.push(*c),
        }
        spaced = wordy;
    }
    s
}

/// Compiles expression `text` of `def`'s definition (a `CHECK`, a
/// `DEFAULT`).
fn compile_expr(def: &TableDef, text: &str) -> std::result::Result<CExpr, String> {
    use sqlparser::parser::Parser;
    let e = Parser::new(&sprig_plan::dialect::SprigMySql)
        .try_with_sql(text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|_| format!("the expression {text}"))?;
    let mut c = Compiler {
        def,
        quals: vec![def.name.clone()],
        params: 0,
        read: vec![false; def.columns.len()],
        backslash: true,
        hints: Vec::new(),
    };
    match c.compile(&e) {
        Ok(x) => Ok(x),
        // What the single-table compiler doesn't have, the expression
        // engine may.
        Err(_) => exec_expr(def, &e).map_err(|why| format!("the expression {text} ({why})")),
    }
}

/// `e`, an expression of `def`'s own columns, compiled by the expression
/// engine for both ports. Columns whose stored values it would misread
/// (`ENUM`, `SET`, `BIT`, `JSON`) are refused.
fn exec_expr(def: &TableDef, e: &sqlparser::ast::Expr) -> std::result::Result<CExpr, String> {
    exec_expr_in(def, e, false)
}

/// `e`, a value that reads no column (`NOW() + INTERVAL 7 DAY`,
/// `DATE_ADD(CURDATE(), INTERVAL 1 MONTH)`, `CONCAT('a', 'b')`), compiled
/// by the expression engine, for an `INSERT`'s `VALUES`: evaluated at the
/// statement's time in the session's time zone and `sql_mode`.
pub(crate) fn value_expr(
    def: &TableDef,
    e: &sqlparser::ast::Expr,
    seqs: &SeqLookup<'_>,
) -> std::result::Result<CExpr, String> {
    exec_expr_with(def, e, true, true, Some(seqs))
}

/// `e`, an expression of a row's columns that the single-table compiler
/// doesn't have (a function call, `CASE`, date arithmetic), compiled by
/// the expression engine and evaluated at the statement's time in the
/// session's time zone and `sql_mode`: an `ON DUPLICATE KEY UPDATE`
/// assignment, over the old row and the new one (`def` lists both).
pub(crate) fn row_expr(
    def: &TableDef,
    e: &sqlparser::ast::Expr,
) -> std::result::Result<CExpr, String> {
    exec_expr_with(def, e, false, true, None)
}

/// Resolves a sequence's name (`schema` empty: the current database) to
/// the identity the expression engine's sequence functions take.
pub(crate) type SeqLookup<'a> =
    dyn Fn(&str, &str) -> std::result::Result<String, sprig_exec::SqlError> + 'a;

/// An empty catalog that knows the statement's sequences.
struct WithSequences<'a> {
    inner: &'a sprig_plan::mem::MemDb,
    seqs: Option<&'a SeqLookup<'a>>,
}

impl sprig_plan::catalog::Catalog for WithSequences<'_> {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<sprig_plan::catalog::TableDef>> {
        sprig_plan::catalog::Catalog::table(self.inner, schema, name)
    }
    fn default_schema(&self) -> Option<String> {
        Some(String::new())
    }
    fn sequence(
        &self,
        schema: &str,
        name: &str,
    ) -> std::result::Result<String, sprig_exec::SqlError> {
        match self.seqs {
            Some(f) => f(schema, name),
            None => Err(sprig_exec::SqlError::new(
                4091,
                "42S02",
                format!("Unknown SEQUENCE: '{name}'"),
            )),
        }
    }
}

fn exec_expr_in(
    def: &TableDef,
    e: &sqlparser::ast::Expr,
    value: bool,
) -> std::result::Result<CExpr, String> {
    exec_expr_with(def, e, value, value, None)
}

/// 1305 for a function only MariaDB has, named as the statement writes it
/// (the expression engine names it in upper case), as MySQL's message
/// does; other errors as they are.
fn as_written(mut err: sprig_exec::SqlError, e: &sqlparser::ast::Expr) -> sprig_exec::SqlError {
    let Some(name) = err
        .message
        .strip_prefix("FUNCTION ")
        .and_then(|m| m.strip_suffix(" does not exist"))
        .map(str::to_owned)
    else {
        return err;
    };
    let mut written = None;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if let sqlparser::ast::Expr::Function(f) = x
            && let [sqlparser::ast::ObjectNamePart::Identifier(i)] = f.name.0.as_slice()
            && i.value.eq_ignore_ascii_case(&name)
        {
            written = Some(i.value.clone());
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    if let Some(w) = written {
        err.message = format!("FUNCTION {w} does not exist");
    }
    err
}

fn exec_expr_with(
    def: &TableDef,
    e: &sqlparser::ast::Expr,
    value: bool,
    clock: bool,
    seqs: Option<&SeqLookup<'_>>,
) -> std::result::Result<CExpr, String> {
    let table = super::general::names::plan_table(def, "");
    let mut out: Vec<(sprig_exec::Session, sprig_exec::Compiled)> = Vec::with_capacity(2);
    let mut mysql_refused = None;
    for persona in [sprig_exec::Persona::MariaDb, sprig_exec::Persona::MySql] {
        let session = sprig_exec::Session::new(persona);
        let db = sprig_plan::mem::MemDb::new("");
        let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new(db.clone());
        let cat = WithSequences { inner: &db, seqs };
        let planner = sprig_plan::Planner::new(&cat, storage, &session);
        let (x, read) = match planner.scalar(&table, e) {
            Ok(r) => r,
            // A sequence function, MariaDB's alone: the MySQL form is the
            // MariaDB one (the MySQL port serves no sequence).
            Err(err) if err.code == 1305 && seqs.is_some() && !out.is_empty() => {
                let first = out[0].clone();
                out.push(first);
                continue;
            }
            // A value only MariaDB has (`CAST(.. AS INET4)`, `UUID_V7()`):
            // a MySQL session's statement gets the MySQL form's refusal.
            Err(err) if value && matches!(err.code, 1064 | 1305) && !out.is_empty() => {
                let first = out[0].clone();
                out.push(first);
                mysql_refused = Some(as_written(err, e));
                continue;
            }
            Err(err) => return Err(err.message),
        };
        if value && !read.is_empty() {
            return Err("it reads a column".into());
        }
        if let Some(t) = read
            .iter()
            .filter_map(|&c| def.columns.get(c))
            .find_map(|c| super::general::names::opaque(c.ty))
        {
            return Err(format!("it reads a {t} column"));
        }
        let c = match sprig_exec::compile(&x, &session) {
            Ok(c) => c,
            // As above, for a function only MariaDB has.
            Err(err) if value && matches!(err.code, 1064 | 1305) && !out.is_empty() => {
                let first = out[0].clone();
                out.push(first);
                mysql_refused = Some(as_written(err, e));
                continue;
            }
            Err(err) => return Err(err.message),
        };
        out.push((session, c));
    }
    let mysql = out.pop().ok_or("no MySQL form")?;
    let maria = out.pop().ok_or("no MariaDB form")?;
    Ok(CExpr::Exec(Arc::new(super::expr::ExecExpr {
        maria,
        mysql,
        mysql_refused,
        session_clock: clock,
    })))
}

/// What [`parse`] found: the definition, or why the engine can't serve
/// the table.
pub(crate) type Parsed = std::result::Result<Arc<TableDef>, String>;

/// A key part as the definition text states it.
struct RawPart {
    name: String,
    prefix: Option<u32>,
    desc: bool,
}

/// A column's type before its collation is known.
#[derive(Debug, Clone, PartialEq)]
enum Base {
    Int(u8),
    Bit(u8),
    Decimal(u8, u8),
    Float,
    Double,
    Date,
    Time(u8),
    DateTime(u8),
    Timestamp(u8),
    Year,
    Char(u32),
    VarChar(u32),
    Text(LobSize),
    Binary(u32),
    VarBinary(u32),
    Blob(LobSize),
    Enum(Vec<String>),
    Set(Vec<String>),
    Fbt(Fbt),
}

/// A `DEFAULT` as the text states it.
#[derive(Debug, Clone, PartialEq)]
enum RawDefault {
    Null,
    Lit(Datum),
    Now,
    Expr(String),
}

/// A column as the definition text states it.
#[allow(clippy::struct_excessive_bools)] // a column definition's independent attributes
struct RawCol {
    name: String,
    base: Base,
    unsigned: bool,
    width: u32,
    float_md: Option<(u8, u8)>,
    nullable: bool,
    default: Option<RawDefault>,
    on_update_now: bool,
    auto_inc: bool,
    charset: Option<String>,
    collation: Option<String>,
    json: bool,
    /// `GENERATED ALWAYS AS (text)`, and whether it is `STORED`.
    generated: Option<(String, bool)>,
    invisible: bool,
}

fn small(t: Option<&Tok>) -> Option<u8> {
    match t {
        Some(Tok::Num(n)) => n.parse().ok(),
        _ => None,
    }
}

/// The type named `word` with the arguments `args` (`None`: no list).
fn base_type(word: &str, args: Option<&[Tok]>) -> std::result::Result<(Base, u32), String> {
    let n = |i: usize| -> Option<u32> {
        match args?.get(i) {
            Some(Tok::Num(n)) => n.parse().ok(),
            _ => None,
        }
    };
    let fsp = || -> std::result::Result<u8, String> {
        match args {
            None => Ok(0),
            Some(a) => small(a.first())
                .filter(|f| *f <= 6)
                .ok_or_else(|| "a fractional-seconds precision".to_owned()),
        }
    };
    let members = || -> std::result::Result<Vec<String>, String> {
        let mut m = Vec::new();
        for (i, t) in args.unwrap_or(&[]).iter().enumerate() {
            match t {
                Tok::Str(s) if i % 2 == 0 => m.push(s.clone()),
                Tok::Punct(',') if i % 2 == 1 => {}
                _ => return Err("a member list".into()),
            }
        }
        if m.is_empty() {
            return Err("a member list".into());
        }
        Ok(m)
    };
    let int = |bytes: u8, default_width: u32| (Base::Int(bytes), n(0).unwrap_or(default_width));
    Ok(match word {
        "TINYINT" => int(1, 4),
        "SMALLINT" => int(2, 6),
        "MEDIUMINT" => int(3, 9),
        "INT" | "INTEGER" => int(4, 11),
        "BIGINT" => int(8, 20),
        "BIT" => (
            Base::Bit(u8::try_from(n(0).unwrap_or(1)).map_err(|_| "BIT width")?),
            0,
        ),
        "DECIMAL" => {
            let p = u8::try_from(n(0).unwrap_or(10)).map_err(|_| "DECIMAL precision")?;
            let s = match args {
                Some(a) if a.len() >= 3 => small(a.get(2)).ok_or("DECIMAL scale")?,
                _ => 0,
            };
            (Base::Decimal(p, s), 0)
        }
        "FLOAT" => (Base::Float, 0),
        "DOUBLE" => (Base::Double, 0),
        "DATE" => (Base::Date, 0),
        "TIME" => (Base::Time(fsp()?), 0),
        "DATETIME" => (Base::DateTime(fsp()?), 0),
        "TIMESTAMP" => (Base::Timestamp(fsp()?), 0),
        "YEAR" => {
            if n(0).is_some_and(|w| w != 4) {
                return Err("YEAR(2)".into());
            }
            (Base::Year, 0)
        }
        "CHAR" => (Base::Char(n(0).unwrap_or(1)), 0),
        "VARCHAR" => (Base::VarChar(n(0).ok_or("VARCHAR length")?), 0),
        "TINYTEXT" => (Base::Text(LobSize::Tiny), 0),
        "TEXT" => (Base::Text(LobSize::Normal), 0),
        "MEDIUMTEXT" => (Base::Text(LobSize::Medium), 0),
        "LONGTEXT" => (Base::Text(LobSize::Long), 0),
        "BINARY" => (Base::Binary(n(0).unwrap_or(1)), 0),
        "VARBINARY" => (Base::VarBinary(n(0).ok_or("VARBINARY length")?), 0),
        "TINYBLOB" => (Base::Blob(LobSize::Tiny), 0),
        "BLOB" => (Base::Blob(LobSize::Normal), 0),
        "MEDIUMBLOB" => (Base::Blob(LobSize::Medium), 0),
        "LONGBLOB" => (Base::Blob(LobSize::Long), 0),
        "ENUM" => (Base::Enum(members()?), 0),
        "SET" => (Base::Set(members()?), 0),
        "UUID" => (Base::Fbt(Fbt::Uuid), 0),
        "INET4" => (Base::Fbt(Fbt::Inet4), 0),
        "INET6" => (Base::Fbt(Fbt::Inet6), 0),
        other => return Err(format!("column type {other}")),
    })
}

/// A number in the text as a constant: an integer, a decimal, or a double
/// when it has an exponent.
fn number(n: &str) -> Option<Datum> {
    if n.contains(['e', 'E']) {
        return n.parse::<f64>().ok().map(Datum::Double);
    }
    if n.contains('.') {
        return Decimal::parse(n).map(Datum::Decimal);
    }
    if let Ok(i) = n.parse::<i64>() {
        return Some(Datum::Int(i));
    }
    if let Ok(u) = n.parse::<u64>() {
        return Some(Datum::UInt(u));
    }
    Decimal::parse(n).map(Datum::Decimal)
}

fn hex(s: &str) -> Option<Bytes> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| {
        char::from(c)
            .to_digit(16)
            .and_then(|d| u8::try_from(d).ok())
    };
    s.as_bytes()
        .chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect::<Option<Vec<u8>>>()
        .map(Bytes::from)
}

/// The collation a name in the text means: MySQL 8.4's for the
/// `utf8mb4_0900_*` names, MariaDB 11.8's for the others.
pub(crate) fn collation_named(name: &str) -> Option<Collation> {
    let flavor = if name.to_ascii_lowercase().contains("_0900_") {
        Flavor::MySql
    } else {
        Flavor::MariaDb
    };
    Collation::resolve(name, flavor)
}

/// The compiled default collation of a character set, for text that names
/// only the character set.
fn charset_default(cs: &str) -> Option<&'static str> {
    Some(match cs.to_ascii_lowercase().as_str() {
        "utf8mb4" => "utf8mb4_general_ci",
        "utf8mb3" | "utf8" => "utf8mb3_general_ci",
        "latin1" => "latin1_swedish_ci",
        "ascii" => "ascii_general_ci",
        "binary" => "binary",
        _ => return None,
    })
}

fn is_mb3(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.starts_with("utf8mb3") || l == "utf8" || l.starts_with("utf8_")
}

/// Text of the definition (UTF-8) in the character set of `c`: the same
/// bytes for UTF-8 and ASCII, Windows-1252 bytes for `latin1`. `None`
/// when a character has no place in it.
pub(crate) fn text_in(charset: Charset, s: &str) -> Option<Bytes> {
    match charset {
        Charset::Utf8mb4 | Charset::Binary => Some(Bytes::copy_from_slice(s.as_bytes())),
        Charset::Ascii => s.is_ascii().then(|| Bytes::copy_from_slice(s.as_bytes())),
        Charset::Latin1 => s
            .chars()
            .map(super::assign::latin1_byte)
            .collect::<Option<Vec<u8>>>()
            .map(Bytes::from),
    }
}

/// Parses MariaDB's `SHOW CREATE TABLE` text of object `object`.
#[allow(clippy::too_many_lines)] // one pass over the definition
pub(crate) fn parse(object: ObjectId, version: u64, db: DbId, create_sql: &str) -> Parsed {
    let bad = |why: &str| Err(format!("table definition not served by the engine: {why}"));
    // The partitioning, which the engine's own text prints last.
    let (create_sql, part_text) = match create_sql.find("\n PARTITION BY ") {
        Some(i) => (&create_sql[..i], Some(&create_sql[i..])),
        None => (create_sql, None),
    };
    let Some(t) = tokens(create_sql) else {
        return bad("unreadable text");
    };
    let mut p = P { t, i: 0 };
    if !(p.word("CREATE") && p.word("TABLE")) {
        return bad("not CREATE TABLE");
    }
    let Some(name) = p.ident() else {
        return bad("no name");
    };
    if !p.punct('(') {
        return bad("no column list");
    }
    let mut cols: Vec<RawCol> = Vec::new();
    let mut pk: Vec<RawPart> = Vec::new();
    let mut keys: Vec<(String, Vec<RawPart>, bool, bool)> = Vec::new();
    let mut fts: Vec<(String, Vec<RawPart>, Option<String>)> = Vec::new();
    // `CHECK` constraints: the column they were declared on, their name,
    // their expression.
    let mut raw_checks: Vec<(Option<usize>, Option<String>, String)> = Vec::new();
    let mut raw_fks: Vec<RawFk> = Vec::new();
    loop {
        match p.peek().cloned() {
            Some(Tok::Ident(c)) => {
                p.i += 1;
                let Some(Tok::Word(ty)) = p.next() else {
                    return bad("column type");
                };
                let args = if p.punct('(') {
                    let Some(a) = p.args() else {
                        return bad("type arguments");
                    };
                    Some(a)
                } else {
                    None
                };
                let (base, width) = match base_type(&ty, args.as_deref()) {
                    Ok(b) => b,
                    Err(why) => return bad(&why),
                };
                let float_md = match (&base, args.as_deref()) {
                    (Base::Float | Base::Double, Some(a)) => {
                        match (small(a.first()), small(a.get(2))) {
                            (Some(m), Some(d)) if d <= 30 && d <= m => Some((m, d)),
                            _ => return bad("FLOAT precision"),
                        }
                    }
                    _ => None,
                };
                let unsigned = p.word("UNSIGNED");
                if p.word("ZEROFILL") {
                    return bad("ZEROFILL");
                }
                let mut rc = RawCol {
                    name: c,
                    base,
                    unsigned,
                    width,
                    float_md,
                    nullable: true,
                    default: None,
                    on_update_now: false,
                    auto_inc: false,
                    charset: None,
                    collation: None,
                    json: false,
                    generated: None,
                    invisible: false,
                };
                loop {
                    if p.word("NOT") {
                        if !p.word("NULL") {
                            return bad("NOT without NULL");
                        }
                        rc.nullable = false;
                    } else if p.word("NULL") {
                        rc.nullable = true;
                    } else if p.word("CHARACTER") {
                        if !p.word("SET") {
                            return bad("CHARACTER without SET");
                        }
                        rc.charset = p.ident();
                    } else if p.word("COLLATE") {
                        rc.collation = p.ident();
                    } else if p.word("AUTO_INCREMENT") {
                        rc.auto_inc = true;
                    } else if p.word("COMMENT") {
                        if !matches!(p.next(), Some(Tok::Str(_))) {
                            return bad("a column comment");
                        }
                    } else if p.word("ON") {
                        if !(p.word("UPDATE") && p.word("CURRENT_TIMESTAMP")) {
                            return bad("ON UPDATE");
                        }
                        if p.punct('(') && p.args().is_none() {
                            return bad("ON UPDATE");
                        }
                        rc.on_update_now = true;
                    } else if p.word("CHECK") {
                        let Some(g) = p.group() else {
                            return bad("a CHECK constraint");
                        };
                        // The check MariaDB adds to a JSON column makes the
                        // column JSON; any other is a constraint.
                        let json = rc.base == Base::Text(LobSize::Long)
                            && matches!(
                                g.as_slice(),
                                [Tok::Word(w), Tok::Punct('('), Tok::Ident(n), Tok::Punct(')')]
                                    if w == "JSON_VALID" && *n == rc.name
                            );
                        if json {
                            rc.json = true;
                        } else {
                            raw_checks.push((Some(cols.len()), None, sql_of(&g)));
                        }
                    } else if p.word("DEFAULT") {
                        if p.peek() == Some(&Tok::Punct('(')) {
                            let Some(g) = p.group() else {
                                return bad("a DEFAULT expression");
                            };
                            rc.default = Some(RawDefault::Expr(sql_of(&g)));
                            continue;
                        }
                        rc.default = Some(match p.next() {
                            Some(Tok::Word(w)) if w == "NULL" => RawDefault::Null,
                            Some(Tok::Word(w)) if w == "CURRENT_TIMESTAMP" => {
                                if p.punct('(') && p.args().is_none() {
                                    return bad("a DEFAULT expression");
                                }
                                RawDefault::Now
                            }
                            // A function call (`curdate()`, `concat('x','y')`).
                            Some(Tok::Word(w)) if p.peek() == Some(&Tok::Punct('(')) => {
                                let Some(g) = p.group() else {
                                    return bad("a DEFAULT expression");
                                };
                                RawDefault::Expr(format!("{w}({})", sql_of(&g)))
                            }
                            Some(Tok::Word(w)) if w == "X" => match p.next() {
                                Some(Tok::Str(s)) => match hex(&s) {
                                    Some(b) => RawDefault::Lit(Datum::Bytes(b)),
                                    None => return bad("default"),
                                },
                                _ => return bad("a DEFAULT expression"),
                            },
                            Some(Tok::Word(w)) if w == "B" => match p.next() {
                                Some(Tok::Str(s)) => match u64::from_str_radix(&s, 2) {
                                    Ok(v) => RawDefault::Lit(Datum::Bit(v)),
                                    Err(_) => return bad("default"),
                                },
                                _ => return bad("a DEFAULT expression"),
                            },
                            Some(Tok::Num(n)) => match number(&n) {
                                Some(d) => RawDefault::Lit(d),
                                None => return bad("default"),
                            },
                            Some(Tok::Str(s)) => {
                                RawDefault::Lit(Datum::Str(Bytes::from(s.into_bytes())))
                            }
                            _ => return bad("a DEFAULT expression"),
                        });
                    } else if p.word("GENERATED") {
                        if !(p.word("ALWAYS") && p.word("AS")) {
                            return bad("a generated column");
                        }
                        let Some(g) = p.group() else {
                            return bad("a generated column");
                        };
                        let stored = p.word("STORED") || p.word("PERSISTENT");
                        if !stored {
                            let _ = p.word("VIRTUAL");
                        }
                        rc.generated = Some((sql_of(&g), stored));
                    } else if p.word("AS") {
                        return bad("a generated column");
                    } else if p.word("INVISIBLE") {
                        rc.invisible = true;
                    } else if p.word("VISIBLE") {
                    } else if p.word("COMPRESSED") {
                        return bad("a compressed column");
                    } else {
                        break;
                    }
                }
                cols.push(rc);
            }
            Some(Tok::Word(w)) if w == "PRIMARY" => {
                p.i += 1;
                if !p.word("KEY") {
                    return bad("PRIMARY without KEY");
                }
                let Some(l) = p.key_parts() else {
                    return bad("primary key parts");
                };
                if p.word("USING") {
                    let _ = p.ident();
                }
                pk = l;
            }
            Some(Tok::Word(w)) if w == "UNIQUE" || w == "KEY" || w == "INDEX" => {
                p.i += 1;
                let unique = w == "UNIQUE";
                if unique && !p.word("KEY") {
                    let _ = p.word("INDEX");
                }
                let Some(n) = p.ident() else {
                    return bad("index name");
                };
                let Some(l) = p.key_parts() else {
                    return bad("index parts");
                };
                if p.word("USING") {
                    let _ = p.ident();
                }
                if p.word("COMMENT") && !matches!(p.next(), Some(Tok::Str(_))) {
                    return bad("an index comment");
                }
                // MariaDB's `IGNORED` (and `NOT IGNORED`): every write keeps
                // the entries, no plan reads them.
                let ignored = if p.word("NOT") {
                    if !p.word("IGNORED") {
                        return bad("NOT after an index");
                    }
                    false
                } else {
                    p.word("IGNORED")
                };
                keys.push((n, l, unique, ignored));
            }
            Some(Tok::Word(w)) if w == "FULLTEXT" => {
                p.i += 1;
                if !p.word("KEY") {
                    let _ = p.word("INDEX");
                }
                let Some(n) = p.ident() else {
                    return bad("a fulltext index name");
                };
                let Some(l) = p.key_parts() else {
                    return bad("fulltext index parts");
                };
                let mut parser = None;
                loop {
                    if p.word("WITH") {
                        if !p.word("PARSER") {
                            return bad("WITH after a fulltext index");
                        }
                        parser = p.ident().map(|x| x.to_ascii_lowercase());
                    } else if p.word("COMMENT") {
                        if !matches!(p.next(), Some(Tok::Str(_))) {
                            return bad("an index comment");
                        }
                    } else {
                        break;
                    }
                }
                fts.push((n, l, parser));
            }
            Some(Tok::Word(w)) if w == "SPATIAL" || w == "VECTOR" => {
                return bad(&format!("a {} index", w.to_ascii_lowercase()));
            }
            Some(Tok::Word(w)) if w == "CONSTRAINT" => {
                p.i += 1;
                let Some(n) = p.ident() else {
                    return bad("a constraint");
                };
                if p.word("FOREIGN") {
                    match raw_fk(&mut p, n) {
                        Some(f) => raw_fks.push(f),
                        None => return bad("a foreign key"),
                    }
                    if p.punct(',') {
                        continue;
                    }
                    if p.punct(')') {
                        break;
                    }
                    return bad("table elements");
                }
                if !p.word("CHECK") {
                    return bad("a constraint");
                }
                let Some(g) = p.group() else {
                    return bad("a CHECK constraint");
                };
                raw_checks.push((None, Some(n), sql_of(&g)));
            }
            Some(Tok::Word(w)) if w == "FOREIGN" || w == "CHECK" => {
                return bad("a foreign key or CHECK constraint");
            }
            _ => return bad("a table element"),
        }
        if p.punct(',') {
            continue;
        }
        if p.punct(')') {
            break;
        }
        return bad("table elements");
    }
    // Table options: the default character set and collation.
    let mut t_charset: Option<String> = None;
    let mut t_collation: Option<String> = None;
    let mut auto_inc_start = 1u64;
    while let Some(tok) = p.next() {
        if let Tok::Word(w) = tok {
            match w.as_str() {
                "CHARSET" => {
                    p.punct('=');
                    t_charset = p.ident();
                }
                "CHARACTER" => {
                    if p.word("SET") {
                        p.punct('=');
                        t_charset = p.ident();
                    }
                }
                "COLLATE" => {
                    p.punct('=');
                    t_collation = p.ident();
                }
                "AUTO_INCREMENT" => {
                    p.punct('=');
                    if let Some(Tok::Num(n)) = p.next() {
                        auto_inc_start = n.parse().unwrap_or(1);
                    }
                }
                "PARTITION" => return bad("partitions"),
                "VERSIONING" => return bad("system versioning"),
                // Other engines' rules (no rollback, table locks) aren't
                // reproduced yet: only InnoDB tables are served.
                "ENGINE" => {
                    p.punct('=');
                    match p.ident() {
                        Some(e) if e.eq_ignore_ascii_case("innodb") => {}
                        Some(e) => return bad(&format!("ENGINE={e}")),
                        None => return bad("ENGINE"),
                    }
                }
                _ => {}
            }
        }
    }
    // The name of the collation a column without its own takes.
    let default_coll = |cs: Option<&str>| -> Option<String> {
        match (cs, t_charset.as_deref(), t_collation.as_deref()) {
            (Some(cs), Some(tcs), Some(tc)) if cs.eq_ignore_ascii_case(tcs) => Some(tc.to_owned()),
            (Some(cs), _, _) => charset_default(cs).map(str::to_owned),
            (None, _, Some(tc)) => Some(tc.to_owned()),
            (None, Some(tcs), None) => charset_default(tcs).map(str::to_owned),
            (None, None, None) => None,
        }
    };
    let mut columns = Vec::with_capacity(cols.len());
    // Per column: the collation its name had among the first seven.
    let mut raw_defaults = Vec::with_capacity(cols.len());
    let mut raw_generated: Vec<(usize, String, bool)> = Vec::new();
    for mut rc in cols {
        let stringy = matches!(
            rc.base,
            Base::Char(_) | Base::VarChar(_) | Base::Text(_) | Base::Enum(_) | Base::Set(_)
        );
        let (collation, mb3) = if stringy {
            let Some(cn) = rc
                .collation
                .clone()
                .or_else(|| default_coll(rc.charset.as_deref()))
            else {
                return bad(&format!("the collation of column {}", rc.name));
            };
            let Some(c) = collation_named(&cn) else {
                return bad(&format!(
                    "collation {} (column {})",
                    cn.to_lowercase(),
                    rc.name
                ));
            };
            let mb3 = is_mb3(&cn) || rc.charset.as_deref().is_some_and(is_mb3);
            (Some(c), mb3)
        } else {
            (None, false)
        };
        if rc.unsigned
            && !matches!(
                rc.base,
                Base::Int(_) | Base::Decimal(..) | Base::Float | Base::Double
            )
        {
            return bad("UNSIGNED on this type");
        }
        let mut members: Vec<Bytes> = Vec::new();
        if let Base::Enum(m) | Base::Set(m) = &rc.base {
            let cs = collation.map_or(Charset::Utf8mb4, Collation::charset);
            for s in m {
                match text_in(cs, s) {
                    Some(b) => members.push(b),
                    None => return bad(&format!("a member of column {}", rc.name)),
                }
            }
        }
        let own = collation.unwrap_or(Collation::Binary);
        let ty = match &rc.base {
            Base::Int(bytes) => ColType::Int {
                bytes: *bytes,
                unsigned: rc.unsigned,
            },
            Base::Bit(bits) => ColType::Bit { bits: *bits },
            Base::Decimal(precision, scale) => ColType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Base::Float => ColType::Float,
            Base::Double => ColType::Double,
            Base::Date => ColType::Date,
            Base::Time(fsp) => ColType::Time { fsp: *fsp },
            Base::DateTime(fsp) => ColType::DateTime { fsp: *fsp },
            Base::Timestamp(fsp) => ColType::Timestamp { fsp: *fsp },
            Base::Year => ColType::Year,
            Base::Char(n) => ColType::Char {
                chars: u16::try_from(*n).unwrap_or(u16::MAX),
                collation: own,
            },
            Base::VarChar(n) => ColType::VarChar {
                chars: *n,
                collation: own,
            },
            Base::Text(LobSize::Long) if rc.json => ColType::Json,
            Base::Text(size) => ColType::Text {
                size: *size,
                collation: own,
            },
            Base::Binary(n) => ColType::Binary {
                bytes: u16::try_from(*n).unwrap_or(u16::MAX),
            },
            Base::VarBinary(n) => ColType::VarBinary { bytes: *n },
            Base::Blob(size) => ColType::Blob { size: *size },
            Base::Enum(m) => ColType::Enum {
                members: u16::try_from(m.len()).unwrap_or(0),
            },
            Base::Set(m) => ColType::Set {
                members: u8::try_from(m.len()).unwrap_or(0),
            },
            Base::Fbt(f) => ColType::Fbt(*f),
        };
        if ty.validate().is_err() {
            return bad(&format!("the type of column {}", rc.name));
        }
        let family = Family::of(ty);
        if !family.served() {
            return bad(family.what());
        }
        if rc.auto_inc && !matches!(ty, ColType::Int { .. }) {
            return bad("AUTO_INCREMENT on a column that isn't an integer");
        }
        if rc.auto_inc && matches!(rc.default, Some(RawDefault::Expr(_))) {
            return bad("a DEFAULT expression on an AUTO_INCREMENT column");
        }
        if (rc.on_update_now || rc.default == Some(RawDefault::Now))
            && !matches!(ty, ColType::DateTime { .. } | ColType::Timestamp { .. })
        {
            return bad("CURRENT_TIMESTAMP on a column that isn't DATETIME or TIMESTAMP");
        }
        if rc.generated.is_some() && (rc.auto_inc || rc.default.is_some() || rc.on_update_now) {
            return bad("a generated column with a DEFAULT or AUTO_INCREMENT");
        }
        if let Some((text, stored)) = rc.generated.take() {
            raw_generated.push((columns.len(), text, stored));
        }
        raw_defaults.push(rc.default);
        columns.push(Column {
            name: rc.name,
            ty,
            nullable: rc.nullable,
            default: None,
            on_update_now: rc.on_update_now,
            auto_inc: rc.auto_inc,
            width: rc.width,
            unsigned: rc.unsigned,
            float_md: rc.float_md,
            members: Arc::from(members),
            collation: if matches!(ty, ColType::Json) {
                None
            } else {
                collation
            },
            mb3,
            flags: 0,
            generated: None,
            invisible: rc.invisible,
        });
    }
    // Defaults, in each column's stored form (expressions are compiled
    // once the table is whole).
    let mut default_exprs: Vec<(usize, String)> = Vec::new();
    for (i, (c, raw)) in columns.iter_mut().zip(raw_defaults).enumerate() {
        c.default = match raw {
            None => None,
            Some(RawDefault::Null) => Some(ColDefault::Value(Datum::Null)),
            Some(RawDefault::Now) => Some(ColDefault::Now),
            Some(RawDefault::Expr(text)) => {
                default_exprs.push((i, text));
                // A place holder until the expression is compiled: the
                // column has a default.
                Some(ColDefault::Value(Datum::Null))
            }
            Some(RawDefault::Lit(d)) => match super::assign::constant(c, d) {
                Some(v) => Some(ColDefault::Value(v)),
                None => return bad(&format!("the default of column {}", c.name)),
            },
        };
    }
    let find = |n: &str| columns.iter().position(|c| c.name.eq_ignore_ascii_case(n));
    // A key part: its column, and how keys encode it.
    let part =
        |rp: &RawPart, nullable: bool, coll: Option<Collation>| -> Option<(usize, KeyColumn)> {
            let i = find(&rp.name)?;
            let mut ty = columns[i].ty;
            if !ty.is_key_type() {
                return None;
            }
            // An ENUM or SET orders by its number; a string by its collation.
            if let (
                Some(c),
                ColType::Char { collation, .. }
                | ColType::VarChar { collation, .. }
                | ColType::Text { collation, .. },
            ) = (coll, &mut ty)
            {
                *collation = c;
            }
            let mut k = KeyColumn::new(ty);
            if nullable {
                k = k.nullable();
            }
            if rp.desc {
                k = k.desc();
            }
            if let Some(n) = rp.prefix {
                k = k.prefix(n);
            }
            Some((i, k))
        };
    // Without a primary key, both servers take the first unique key whose
    // columns are all `NOT NULL` and whole (no prefix) as the table's
    // primary key (the definition lists such keys first): its columns
    // report `PRI`, the rows are kept in its order, and its errors name
    // it. Without one either, the rows are keyed by a hidden row number.
    let mut pk_name = String::from("PRIMARY");
    if pk.is_empty()
        && let Some(at) = keys.iter().position(|(_, parts, unique, _)| {
            *unique
                && parts.iter().all(|rp| {
                    rp.prefix.is_none() && find(&rp.name).is_some_and(|i| !columns[i].nullable)
                })
        })
    {
        let (n, parts, _, _) = keys.remove(at);
        pk = parts;
        pk_name = n;
    }
    let hidden = pk.is_empty();
    let mut pk_idx = Vec::new();
    let mut pk_parts = Vec::new();
    for rp in &pk {
        let Some((i, k)) = part(rp, false, None) else {
            return bad("primary key column");
        };
        pk_idx.push(i);
        pk_parts.push(k);
    }
    if hidden {
        pk_idx.push(columns.len());
        pk_parts.push(KeyColumn::new(HIDDEN_TYPE));
    }
    let mut indexes = Vec::new();
    for (n, l, unique, ignored) in &keys {
        let mut idx = Vec::new();
        let mut parts = Vec::new();
        for rp in l {
            let Some((i, k)) = part(rp, true, None) else {
                return bad("index column");
            };
            idx.push(i);
            parts.push(k);
        }
        indexes.push(Index {
            name: n.clone(),
            cols: idx,
            parts,
            unique: *unique,
            ignored: *ignored,
        });
    }
    let auto_inc = columns.iter().position(|c| c.auto_inc);
    // Result-metadata flags, as both servers set them for base columns.
    for (i, c) in columns.iter_mut().enumerate() {
        let mut f = ColumnFlags::empty();
        if !c.nullable {
            f |= ColumnFlags::NOT_NULL;
        }
        if c.unsigned
            || matches!(
                c.ty,
                ColType::Int { unsigned: true, .. }
                    | ColType::Bit { .. }
                    | ColType::Year
                    | ColType::Timestamp { .. }
                    | ColType::Fbt(_)
            )
        {
            f |= ColumnFlags::UNSIGNED;
        }
        if matches!(c.ty, ColType::Year) {
            f |= ColumnFlags::ZEROFILL;
        }
        if c.auto_inc {
            f |= ColumnFlags::AUTO_INCREMENT;
        }
        if !c.nullable && c.default.is_none() && !c.auto_inc {
            f |= ColumnFlags::NO_DEFAULT_VALUE;
        }
        // Both servers set it for every collation that sorts by code
        // (`binary` and the `_bin` collations), not for `binary` alone;
        // and for the types without a character set.
        let bin_collation = c
            .collation
            .is_some_and(|x| x == Collation::Binary || x.name().ends_with("_bin"));
        if bin_collation
            || matches!(
                c.ty,
                ColType::Date
                    | ColType::Time { .. }
                    | ColType::DateTime { .. }
                    | ColType::Timestamp { .. }
                    | ColType::Binary { .. }
                    | ColType::VarBinary { .. }
                    | ColType::Blob { .. }
                    | ColType::Json
                    | ColType::Fbt(_)
            )
        {
            f |= ColumnFlags::BINARY;
        }
        if matches!(
            c.ty,
            ColType::Text { .. } | ColType::Blob { .. } | ColType::Json
        ) {
            f |= ColumnFlags::BLOB;
        }
        match c.ty {
            ColType::Enum { .. } => f |= ColumnFlags::ENUM,
            ColType::Set { .. } => f |= ColumnFlags::SET,
            // A TIMESTAMP that sets itself.
            ColType::Timestamp { .. } if c.on_update_now || c.default == Some(ColDefault::Now) => {
                f |= ColumnFlags::TIMESTAMP;
            }
            _ => {}
        }
        if c.on_update_now {
            f |= ColumnFlags::ON_UPDATE_NOW;
        }
        let mut bits = f.bits();
        if pk_idx.contains(&i) {
            bits |= ColumnFlags::PRI_KEY.bits() | PART_KEY;
        }
        for x in &indexes {
            if let Some(pos) = x.cols.iter().position(|&ci| ci == i) {
                bits |= PART_KEY;
                if x.unique && x.cols.len() == 1 {
                    bits |= ColumnFlags::UNIQUE_KEY.bits();
                } else if pos == 0 {
                    bits |= ColumnFlags::MULTIPLE_KEY.bits();
                }
            }
        }
        c.flags = bits;
    }
    // The row record's columns. Column changes aren't served yet, so a
    // column's permanent id is its place in the definition and every row
    // is of record version 1.
    let mut record_cols = Vec::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        let id = u32::try_from(i + 1).unwrap_or(u32::MAX);
        let rc = RecordColumn::new(id, c.ty);
        record_cols.push(if c.nullable {
            rc
        } else {
            rc.not_null(c.implicit_default())
        });
    }
    let Ok(schema) = RowSchema::new(RECORD_VERSION, record_cols) else {
        return bad("the row layout");
    };
    let mut def = TableDef {
        object,
        version,
        db,
        name,
        columns,
        pk: pk_idx,
        pk_parts,
        pk_name,
        hidden,
        indexes,
        auto_inc,
        auto_inc_start,
        index_sigs: Vec::new(),
        schema,
        checks: Vec::new(),
        checks_mysql: Vec::new(),
        generated_cols: Vec::new(),
        fks: Vec::new(),
        partition: None,
        fulltext: Vec::new(),
    };
    def.index_sigs = super::indexes::sigs(&def);
    for (n, l, parser) in fts {
        let mut cols = Vec::with_capacity(l.len());
        for rp in &l {
            let Some(i) = def.column(&rp.name) else {
                return bad("a fulltext index column");
            };
            cols.push(i);
        }
        def.fulltext.push(FtKey {
            name: n,
            cols,
            parser,
        });
    }
    for f in raw_fks {
        let mut cols = Vec::with_capacity(f.cols.len());
        for c in &f.cols {
            let Some(i) = def.column(c) else {
                return bad("a foreign key column");
            };
            cols.push(i);
        }
        def.fks.push(ForeignKey {
            name: f.name,
            cols,
            ref_db: f.ref_db,
            ref_table: f.ref_table,
            ref_cols: f.ref_cols,
            on_delete: f.on_delete,
            on_update: f.on_update,
        });
    }
    // Expressions, compiled against the whole table.
    let mut defaults = Vec::with_capacity(default_exprs.len());
    for (i, text) in default_exprs {
        match compile_expr(&def, &text) {
            Ok(e) => {
                let constant = !expr::reads_row_or_clock(&e);
                defaults.push((
                    i,
                    DefaultExpr {
                        expr: Arc::new(e),
                        constant,
                    },
                ));
            }
            Err(why) => {
                return bad(&format!("a DEFAULT the engine can't evaluate yet ({why})"));
            }
        }
    }
    for (i, d) in defaults {
        def.columns[i].default = Some(ColDefault::Expr(d));
    }
    let mut generated = Vec::with_capacity(raw_generated.len());
    for (i, text, stored) in raw_generated {
        match compile_expr(&def, &text) {
            Ok(e) => generated.push((
                i,
                Generated {
                    expr: Arc::new(e),
                    stored,
                    text,
                },
            )),
            Err(why) => {
                return bad(&format!(
                    "a generated column the engine can't work out yet ({why})"
                ));
            }
        }
    }
    for (i, g) in generated {
        def.columns[i].generated = Some(g);
        def.generated_cols.push(i);
    }
    let mut checks = Vec::with_capacity(raw_checks.len());
    // MySQL's numbers go on after the highest a constraint already has (a
    // table made through MySQL's port has its constraints named that way).
    let numbered = format!("{}_chk_", def.name.to_lowercase());
    let mut unnamed = raw_checks
        .iter()
        .filter_map(|(_, n, _)| {
            n.as_ref()?
                .to_lowercase()
                .strip_prefix(&numbered)?
                .parse::<usize>()
                .ok()
        })
        .max()
        .unwrap_or(0);
    for (column, name, text) in raw_checks {
        let expr = match compile_expr(&def, &text) {
            Ok(e) => e,
            Err(why) => {
                return bad(&format!(
                    "a CHECK constraint the engine can't evaluate yet ({why})"
                ));
            }
        };
        let name = match (column, name) {
            (Some(c), _) => format!("{}.{}", def.name, def.columns[c].name),
            (None, n) => n.unwrap_or_default(),
        };
        // MySQL numbers the constraints the client didn't name (MariaDB
        // calls a table's `CONSTRAINT_n`).
        let auto = column.is_some()
            || name
                .strip_prefix("CONSTRAINT_")
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
        let mysql_name = if auto {
            unnamed += 1;
            format!("{}_chk_{unnamed}", def.name)
        } else {
            name.clone()
        };
        checks.push(Check {
            name,
            mysql_name,
            column,
            text,
            expr,
        });
    }
    let mut order: Vec<usize> = (0..checks.len()).collect();
    order.sort_by_key(|&i| checks[i].mysql_name.to_lowercase());
    def.checks = checks;
    def.checks_mysql = order;
    if let Some(text) = part_text {
        let Ok(t) = super::define::lex::lex(text) else {
            return bad("the partitioning");
        };
        let mut p = super::define::P {
            t: &t,
            i: 0,
            mysql: false,
        };
        let Ok(spec) = super::define::partition::read(&mut p, "utf8mb4") else {
            return bad("the partitioning");
        };
        let expr = match &spec.expr {
            Some(e) => match compile_expr(&def, &super::define::expr::print(e)) {
                Ok(x) => Some(x),
                Err(why) => {
                    return bad(&format!(
                        "a partitioning expression the engine can't evaluate ({why})"
                    ));
                }
            },
            None => None,
        };
        def.partition = Some(Arc::new(super::partition::Rule::new(spec, expr, &def)));
    }
    Ok(Arc::new(def))
}

/// The version every row record carries until column changes are served.
const RECORD_VERSION: u32 = 1;

/// `PART_KEY_FLAG`: the column is part of some key.
const PART_KEY: u16 = 1 << 14;

/// Checks a client's `CREATE TABLE` text before the table is created in an
/// engine database: a column type family the engine doesn't serve yet, a
/// collation it doesn't have, and the table elements it can't keep
/// (foreign keys, generated columns, full-text and spatial indexes,
/// partitions) are refused here, with the reason. `CHECK` constraints and
/// `DEFAULT` expressions are judged on the definition that results
/// (`super::ddlexec`): one the engine can't evaluate fails the `CREATE`
/// there.
///
/// The check reads only what it is sure of; whatever it doesn't recognise
/// is left to [`parse`], which reads the definition once the table exists
/// and refuses the table's statements if it can't serve it.
///
/// # Errors
/// The reason, for `ER_SPRIG_NOT_ON_ENGINE`.
#[allow(clippy::too_many_lines)] // one pass over the statement's elements
pub(crate) fn check_create(text: &str) -> std::result::Result<(), String> {
    let Some(t) = tokens(text) else {
        return Ok(());
    };
    let Some(open) = t.iter().position(|x| *x == Tok::Punct('(')) else {
        return Ok(());
    };
    // `CREATE TABLE a LIKE b`, `CREATE TABLE a (SELECT ...)`.
    if matches!(t.get(open + 1), Some(Tok::Word(w)) if w == "SELECT" || w == "LIKE") {
        return Ok(());
    }
    let collation = |name: &str| -> std::result::Result<(), String> {
        if collation_named(name).is_none() && charset_default(name).is_none() {
            return Err(format!(
                "collation or character set {}",
                name.to_lowercase()
            ));
        }
        Ok(())
    };
    // The elements of the column list, split at its top-level commas.
    let mut depth = 0usize;
    let mut start = open + 1;
    let mut end = t.len();
    let mut elements: Vec<&[Tok]> = Vec::new();
    for (i, x) in t.iter().enumerate().skip(open) {
        match x {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => {
                depth -= 1;
                if depth == 0 {
                    elements.push(&t[start..i]);
                    end = i + 1;
                    break;
                }
            }
            Tok::Punct(',') if depth == 1 => {
                elements.push(&t[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    for e in elements {
        let word = |i: usize| match e.get(i) {
            Some(Tok::Word(w)) => Some(w.as_str()),
            _ => None,
        };
        match word(0) {
            Some("SPATIAL" | "VECTOR") => {
                return Err(format!(
                    "a {} index",
                    word(0).unwrap_or_default().to_ascii_lowercase()
                ));
            }
            // Foreign keys are enforced by the engine.
            Some("FOREIGN" | "CONSTRAINT" | "CHECK") => continue,
            Some("PERIOD") => return Err("an application-time period".into()),
            Some("PRIMARY" | "UNIQUE" | "KEY" | "INDEX" | "FULLTEXT") if e.len() > 1 => continue,
            _ => {}
        }
        // A column: its name, then its type.
        let Some(ty) = word(1) else {
            continue;
        };
        let family =
            match ty {
                "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "INTEGER" | "BIGINT" | "INT1"
                | "INT2" | "INT3" | "INT4" | "INT8" | "MIDDLEINT" | "BOOL" | "BOOLEAN"
                | "SERIAL" => Some(Family::Integer),
                "BIT" => Some(Family::Bit),
                "DECIMAL" | "DEC" | "NUMERIC" | "FIXED" => Some(Family::Decimal),
                "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "REAL" => Some(Family::Float),
                "DATE" | "TIME" | "DATETIME" | "TIMESTAMP" | "YEAR" => Some(Family::Temporal),
                "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "NCHAR"
                | "NVARCHAR" => Some(Family::Text),
                "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" => {
                    Some(Family::Binary)
                }
                "ENUM" | "SET" => Some(Family::Members),
                "JSON" => Some(Family::Json),
                "UUID" | "INET4" | "INET6" => Some(Family::Fixed),
                "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT"
                | "MULTILINESTRING" | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "VECTOR" => {
                    return Err(format!("column type {ty}"));
                }
                // `CHAR`, `CHARACTER [VARYING]`, `CHAR BYTE`, `LONG [VARBINARY]`,
                // `NATIONAL ...` and what isn't known: read once it exists.
                _ => None,
            };
        if let Some(f) = family
            && !f.served()
        {
            return Err(f.what().to_owned());
        }
        for (i, x) in e.iter().enumerate().skip(2) {
            let Tok::Word(w) = x else { continue };
            let name = |k: usize| match e.get(k) {
                Some(Tok::Word(n) | Tok::Ident(n) | Tok::Str(n)) => Some(n.as_str()),
                _ => None,
            };
            match w.as_str() {
                "ZEROFILL" => return Err("ZEROFILL".into()),
                "REFERENCES" => return Err("a foreign key".into()),
                "COMPRESSED" => return Err("a compressed column".into()),
                "COLLATE" | "CHARSET" => {
                    if let Some(n) = name(i + 1) {
                        collation(n)?;
                    }
                }
                "CHARACTER" if word(i + 1) == Some("SET") => {
                    if let Some(n) = name(i + 2) {
                        collation(n)?;
                    }
                }
                _ => {}
            }
        }
    }
    // Table options.
    let opts = &t[end.min(t.len())..];
    for (i, x) in opts.iter().enumerate() {
        let Tok::Word(w) = x else { continue };
        let value = |mut k: usize| {
            if opts.get(k) == Some(&Tok::Punct('=')) {
                k += 1;
            }
            match opts.get(k) {
                Some(Tok::Word(n) | Tok::Ident(n) | Tok::Str(n)) => Some(n.as_str()),
                _ => None,
            }
        };
        match w.as_str() {
            // Partitioning is the definition's (`define::partition`).
            "PARTITION" | "SELECT" => return Ok(()),
            "VERSIONING" => return Err("system versioning".into()),
            "COLLATE" | "CHARSET" => {
                if let Some(n) = value(i + 1) {
                    collation(n)?;
                }
            }
            "CHARACTER" if opts.get(i + 1) == Some(&Tok::Word("SET".into())) => {
                if let Some(n) = value(i + 2) {
                    collation(n)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SBTEST: &str = "CREATE TABLE `sbtest1` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `k` int(11) NOT NULL DEFAULT 0,\n  `c` char(120) NOT NULL DEFAULT '',\n  `pad` char(60) NOT NULL DEFAULT '',\n  PRIMARY KEY (`id`),\n  KEY `k_1` (`k`)\n) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

    #[test]
    fn sysbench_table_parses_with_the_servers_flags() {
        let t = parse(ObjectId(7), 1, DbId(1), SBTEST).unwrap();
        assert_eq!(t.columns.len(), 4);
        assert_eq!(t.pk, vec![0]);
        assert_eq!(t.auto_inc, Some(0));
        assert_eq!(t.auto_inc_start, 3);
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].cols, vec![1]);
        // As MariaDB 11.8 and MySQL 8.4 report them.
        let flags: Vec<u16> = t.columns.iter().map(|c| c.flags).collect();
        assert_eq!(flags, vec![0x4203, 0x4009, 0x0001, 0x0001]);
        assert_eq!(t.columns[2].collation, Some(Collation::Utf8mb4Uca1400AiCi));
        assert_eq!(t.columns[1].default_value(), Some(&Datum::Int(0)));
        assert_eq!(t.columns[0].width, 11);
        // Keys: the primary key's parts carry no NULL marker, index parts do.
        assert!(!t.pk_parts[0].nullable);
        assert!(t.indexes[0].parts[0].nullable);
    }

    #[test]
    fn unique_and_unsigned_flags_match_the_servers() {
        let sql = "CREATE TABLE `t2` (\n  `a` int(11) DEFAULT NULL,\n  `b` bigint(20) unsigned NOT NULL,\n  `v` varchar(10) DEFAULT NULL,\n  `u` int(11) DEFAULT NULL,\n  PRIMARY KEY (`b`),\n  UNIQUE KEY `u` (`u`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(8), 1, DbId(1), sql).unwrap();
        let flags: Vec<u16> = t.columns.iter().map(|c| c.flags).collect();
        assert_eq!(flags, vec![0x0000, 0x5023, 0x0000, 0x4004]);
    }

    #[test]
    fn binary_collations_set_the_binary_flag() {
        // As MariaDB 11.8 and MySQL 8.4 report a `_bin` column: BINARY
        // (128) beside the rest; a column in another collation without it.
        let sql = "CREATE TABLE `b` (\n  `id` int(11) NOT NULL,\n  `name` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n  `plain` varchar(20) NOT NULL,\n  `a` char(4) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(9), 1, DbId(1), sql).unwrap();
        let flags: Vec<u16> = t.columns.iter().map(|c| c.flags).collect();
        assert_eq!(flags, vec![0x5003, 0x1081, 0x1001, 0x0080]);
        // The table's default collation counts too.
        let sql = "CREATE TABLE `b` (\n  `name` varchar(20) NOT NULL,\n  PRIMARY KEY (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin";
        let t = parse(ObjectId(9), 1, DbId(1), sql).unwrap();
        assert_eq!(t.columns[0].flags & 0x80, 0x80);
    }

    #[test]
    fn collations_resolve_by_name_and_other_tables_are_refused() {
        let s = "CREATE TABLE `t` (\n  `a` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), s).unwrap();
        assert_eq!(
            t.pk_parts[0].ty.collation(),
            Some(Collation::Utf8mb4Uca1400AiCi)
        );
        // A latin1 key: the engine has the collation now.
        let l = "CREATE TABLE `t` (\n  `a` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci";
        let t = parse(ObjectId(1), 1, DbId(1), l).unwrap();
        assert_eq!(t.columns[0].collation, Some(Collation::Latin1SwedishCi));
        // utf8mb3 is the utf8mb4 collation of the same name, BMP only.
        let u = "CREATE TABLE `t` (\n  `a` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), u).unwrap();
        assert_eq!(t.columns[0].collation, Some(Collation::Utf8mb4GeneralCi));
        assert!(t.columns[0].mb3);
        // A collation the engine doesn't have, another engine, a foreign key.
        let c = "CREATE TABLE `t` (\n  `a` varchar(10) COLLATE utf8mb4_swedish_ci NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let e = parse(ObjectId(1), 1, DbId(1), c).unwrap_err();
        assert!(e.contains("utf8mb4_swedish_ci"), "{e}");
        let m = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=MyISAM";
        assert!(parse(ObjectId(1), 1, DbId(1), m).is_err());
        let f = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `b` (`b`),\n  CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `p` (`id`)\n) ENGINE=InnoDB";
        // A foreign key is read into the definition.
        let fk = parse(ObjectId(1), 1, DbId(1), f).unwrap();
        assert_eq!(fk.fks.len(), 1);
        assert_eq!(fk.fks[0].cols, vec![1]);
        assert_eq!(fk.fks[0].ref_table, "p");
        assert_eq!(
            fk.fks[0].text(&fk, false),
            "CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `p` (`id`)"
        );
        let g = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `b` (`b`),\n  CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `d`.`p` (`id`) ON DELETE RESTRICT ON UPDATE NO ACTION\n) ENGINE=InnoDB";
        let g = parse(ObjectId(1), 1, DbId(1), g).unwrap();
        assert_eq!(
            g.fks[0].text(&g, false),
            "CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `d`.`p` (`id`) ON UPDATE NO ACTION"
        );
        assert_eq!(
            g.fks[0].text(&g, true),
            "CONSTRAINT `fk` FOREIGN KEY (`b`) REFERENCES `d`.`p` (`id`) ON DELETE RESTRICT"
        );
        // A generated column is served when its expression is one the
        // engine works out.
        let g = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL,\n  `c` int(11) GENERATED ALWAYS AS (`a` * `b`) STORED,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB";
        let t = parse(ObjectId(1), 1, DbId(1), g).unwrap();
        assert_eq!(t.generated_cols, vec![1, 2]);
        let stored: Vec<bool> = t
            .generated_cols
            .iter()
            .map(|&i| t.columns[i].generated.as_ref().unwrap().stored)
            .collect();
        assert_eq!(stored, vec![false, true]);
        // A function the single-table compiler doesn't have: worked out by
        // the expression engine.
        let g = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `b` varchar(9) GENERATED ALWAYS AS (concat(`a`,'x')) VIRTUAL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), g).unwrap();
        assert!(matches!(
            t.columns[1].generated.as_ref().map(|g| &*g.expr),
            Some(CExpr::Exec(_))
        ));
        // Except over a column whose stored form it would misread.
        let g = "CREATE TABLE `t` (\n  `a` enum('x','y') NOT NULL,\n  `b` varchar(9) GENERATED ALWAYS AS (concat(`a`,'x')) VIRTUAL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let e = parse(ObjectId(1), 1, DbId(1), g).unwrap_err();
        assert!(
            e.contains("generated column the engine can't work out"),
            "{e}"
        );
    }

    #[test]
    fn the_0900_names_mean_mysqls_collations() {
        let s = "CREATE TABLE `t` (\n  `a` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), s).unwrap();
        assert_eq!(
            t.columns[0].collation,
            Some(Collation::Utf8mb4Mysql0900AiCi)
        );
        assert_eq!(
            t.pk_key(false)[0].ty.collation(),
            Some(Collation::Utf8mb4Mysql0900AiCi)
        );
    }

    #[test]
    fn key_parts_keep_prefixes_and_directions() {
        let s = "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  `n` varchar(40) DEFAULT NULL,\n  `o` char(4) NOT NULL DEFAULT 'x',\n  PRIMARY KEY (`a`),\n  KEY `ix` (`n`(4),`o` DESC)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), s).unwrap();
        let p = &t.indexes[0].parts;
        assert_eq!((p[0].prefix, p[0].desc), (Some(4), false));
        assert_eq!((p[1].prefix, p[1].desc), (None, true));
        // The index record tells a prefix index from a whole-column one.
        assert_ne!(t.index_sigs[0].cols[0], "n");
        assert_eq!(
            t.columns[2].default_value(),
            Some(&Datum::Str(Bytes::from_static(b"x")))
        );
    }

    #[test]
    fn checks_and_default_expressions_are_compiled() {
        let sql = "CREATE TABLE `q1` (\n  `id` int(11) NOT NULL,\n  `a` int(11) DEFAULT NULL CHECK (`a` > 0),\n  `c` int(11) DEFAULT (1 + 2) CHECK (`c` <> 5),\n  `e` int(11) DEFAULT (`id` * 2),\n  `f` date DEFAULT curdate(),\n  `j` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`j`)),\n  PRIMARY KEY (`id`),\n  CONSTRAINT `z1` CHECK (`a` < 100),\n  CONSTRAINT `CONSTRAINT_1` CHECK (`a` + `c` <> 50 and `a` not in (7,8)),\n  CONSTRAINT `Mixed Case` CHECK (`c` between 1 and 90 or `c` is null)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), sql).unwrap();
        // The JSON column's own check is its type, not a constraint.
        assert_eq!(t.columns[5].ty, ColType::Json);
        let names: Vec<(&str, &str)> = t
            .checks
            .iter()
            .map(|c| (c.name.as_str(), c.mysql_name.as_str()))
            .collect();
        assert_eq!(
            names,
            vec![
                ("q1.a", "q1_chk_1"),
                ("q1.c", "q1_chk_2"),
                ("z1", "z1"),
                ("CONSTRAINT_1", "q1_chk_3"),
                ("Mixed Case", "Mixed Case")
            ]
        );
        // MySQL evaluates them by name.
        assert_eq!(t.checks_mysql, vec![4, 0, 1, 3, 2]);
        assert_eq!(t.checks[3].text, "`a` + `c` <> 50 AND `a` NOT IN ( 7 , 8 )");
        let constant = |i: usize| match &t.columns[i].default {
            Some(ColDefault::Expr(d)) => Some(d.constant),
            _ => None,
        };
        assert_eq!(
            (constant(2), constant(3), constant(4), constant(1)),
            (Some(true), Some(false), Some(false), None)
        );
        // What the single-table compiler doesn't have goes to the
        // expression engine.
        let like = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `d` varchar(9) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `CONSTRAINT_1` CHECK (`d`  not like 'z%')\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let t = parse(ObjectId(1), 1, DbId(1), like).unwrap();
        assert!(matches!(t.checks[0].expr, CExpr::Exec(_)));
        let call = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `d` varchar(9) DEFAULT concat('x','y'),\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        parse(ObjectId(1), 1, DbId(1), call).unwrap();
        // What neither can work out is refused, naming it.
        let sub = "CREATE TABLE `t` (\n  `id` int(11) NOT NULL,\n  `d` varchar(9) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  CONSTRAINT `CONSTRAINT_1` CHECK (`d` in (select 1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";
        let e = parse(ObjectId(1), 1, DbId(1), sub).unwrap_err();
        assert!(
            e.contains("CHECK constraint the engine can't evaluate"),
            "{e}"
        );
    }

    #[test]
    fn create_text_is_checked_before_the_table_exists() {
        assert!(check_create("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))").is_ok());
        assert!(check_create("create table `t` (id bigint unsigned not null auto_increment, primary key (id)) engine=innodb").is_ok());
        let e = check_create("CREATE TABLE t (id INT PRIMARY KEY, g GEOMETRY)").unwrap_err();
        assert!(e.contains("GEOMETRY"), "{e}");
        assert!(check_create("CREATE TABLE t (id INT PRIMARY KEY, u UUID)").is_ok());
        assert!(check_create("CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(3))").is_err());
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, p INT REFERENCES o (id))").is_err()
        );
        assert!(
            check_create(
                "CREATE TABLE t (id INT PRIMARY KEY, p INT, FOREIGN KEY (p) REFERENCES o (id))"
            )
            .is_ok()
        );
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, b TEXT, FULLTEXT KEY (b))").is_ok()
        );
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, g GEOMETRY, SPATIAL KEY (g))")
                .is_err()
        );
        assert!(
            check_create(
                "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(9) COLLATE utf8mb4_swedish_ci)"
            )
            .is_err()
        );
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(9)) DEFAULT CHARSET=sjis")
                .is_err()
        );
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(9)) CHARSET=latin1")
                .is_ok()
        );
        // Generated columns are judged on the definition that results.
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY, c INT AS (id + 1) VIRTUAL)").is_ok()
        );
        // Partitioning is the definition's to judge.
        assert!(
            check_create("CREATE TABLE t (id INT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 4")
                .is_ok()
        );
        // Not a column list: left to the definition's reader.
        assert!(check_create("CREATE TABLE t LIKE o").is_ok());
    }
}
