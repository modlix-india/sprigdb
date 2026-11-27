//! `PARTITION BY` on engine tables: the partitioning is the table's
//! metadata over its one storage unit, read, checked and printed as the
//! servers do.
//!
//! ```text
//! PARTITION BY [LINEAR] HASH (expr) [PARTITIONS n] [(PARTITION p, ...)]
//!            | [LINEAR] KEY [ALGORITHM = n] ([col, ...]) [PARTITIONS n] [(...)]
//!            | RANGE (expr) | RANGE COLUMNS (col, ...)
//!              (PARTITION p VALUES LESS THAN (v, ...) | MAXVALUE [options], ...)
//!            | LIST (expr) | LIST COLUMNS (col, ...)
//!              (PARTITION p VALUES IN (v, ...) [options], ...)
//! options: [STORAGE] ENGINE [=] e, COMMENT [=] 's', and the storage
//!          options the servers take (directories, MAX_ROWS, ...), not kept
//! ```
//!
//! A bound that is an expression (`TO_DAYS('2020-01-01')`) is kept as the
//! constant it gives, as the servers print it. Subpartitions are refused.

use std::fmt::Write as _;
use std::sync::Arc;

use sprig_engine::row::Value as V;

use super::expr::{self, E, quote_name, quote_str};
use super::lex::T;
use super::{Def, Fail, KeyKind, P, err};

/// The partitioning method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Method {
    Range,
    List,
    Hash,
    Key,
}

/// A bound or list value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Lit {
    Int(i64),
    /// A string (`COLUMNS` partitioning of text, date and time columns).
    Str(String),
    Null,
    Max,
}

impl Lit {
    fn print(&self) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Str(s) => quote_str(s),
            Self::Null => "NULL".into(),
            Self::Max => "MAXVALUE".into(),
        }
    }
}

/// A partition's values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Values {
    /// A `HASH` or `KEY` partition.
    None,
    /// `VALUES LESS THAN (..)`: one value per partitioning column.
    LessThan(Vec<Lit>),
    /// `VALUES IN (..)`: each a tuple of one value per partitioning column.
    In(Vec<Vec<Lit>>),
}

/// One partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Part {
    pub name: String,
    pub values: Values,
    pub comment: Option<String>,
}

/// A table's partitioning.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Spec {
    pub method: Method,
    pub linear: bool,
    /// `RANGE COLUMNS` / `LIST COLUMNS`.
    pub columns: bool,
    /// The expression of `RANGE`, `LIST` and `HASH`.
    pub expr: Option<E>,
    /// The columns of `COLUMNS` partitioning and of `KEY` (empty `KEY ()`:
    /// the primary key's).
    pub cols: Vec<String>,
    /// The partitions, named `p0`, `p1`, ... when the statement didn't list
    /// them.
    pub parts: Vec<Part>,
    /// The statement listed the partitions (they are printed).
    pub named: bool,
}

impl Spec {
    /// The partition named `n` (names compare without case).
    pub(crate) fn part(&self, n: &str) -> Option<usize> {
        self.parts
            .iter()
            .position(|p| p.name.eq_ignore_ascii_case(n))
    }

    /// `information_schema.PARTITIONS.PARTITION_METHOD`.
    pub(crate) fn method_name(&self) -> String {
        let m = match self.method {
            Method::Range => "RANGE",
            Method::List => "LIST",
            Method::Hash => "HASH",
            Method::Key => "KEY",
        };
        match (self.linear, self.columns) {
            (true, _) => format!("LINEAR {m}"),
            (_, true) => format!("{m} COLUMNS"),
            _ => m.to_owned(),
        }
    }

    /// `PARTITION_EXPRESSION`: the expression, or the column list.
    pub(crate) fn expression(&self) -> String {
        match &self.expr {
            Some(e) => expr::print(e),
            None => self
                .cols
                .iter()
                .map(|c| quote_name(c))
                .collect::<Vec<_>>()
                .join(","),
        }
    }

    /// `PARTITION_DESCRIPTION` of partition `i`.
    pub(crate) fn description(&self, i: usize) -> Option<String> {
        match &self.parts.get(i)?.values {
            Values::None => None,
            Values::LessThan(v) => Some(v.iter().map(Lit::print).collect::<Vec<_>>().join(",")),
            Values::In(v) => Some(in_text(v, self.columns && self.cols.len() > 1)),
        }
    }

    /// The columns the partitioning reads (lower case not applied).
    pub(crate) fn read_columns(&self, def: &Def) -> Vec<String> {
        match &self.expr {
            Some(e) => {
                let mut v = Vec::new();
                columns_of(e, &mut v);
                v
            }
            None if self.cols.is_empty() => def
                .primary()
                .map(|k| k.parts.iter().map(|p| p.col.clone()).collect())
                .unwrap_or_default(),
            None => self.cols.clone(),
        }
    }

    /// The clause as MariaDB prints it (a leading newline, as `SHOW CREATE
    /// TABLE` puts it after the table options).
    pub(crate) fn print(&self) -> String {
        let cols = |cs: &[String]| {
            cs.iter()
                .map(|c| quote_name(c))
                .collect::<Vec<_>>()
                .join(",")
        };
        let linear = if self.linear { "LINEAR " } else { "" };
        let mut s = match (self.method, self.columns) {
            (Method::Range | Method::List, true) => format!(
                "\n PARTITION BY {}  COLUMNS({})",
                if self.method == Method::Range {
                    "RANGE"
                } else {
                    "LIST"
                },
                cols(&self.cols)
            ),
            (Method::Range, false) => format!(
                "\n PARTITION BY RANGE ({})",
                self.expr.as_ref().map(expr::print).unwrap_or_default()
            ),
            (Method::List, false) => format!(
                "\n PARTITION BY LIST ({})",
                self.expr.as_ref().map(expr::print).unwrap_or_default()
            ),
            (Method::Hash, _) => format!(
                "\n PARTITION BY {linear}HASH ({})",
                self.expr.as_ref().map(expr::print).unwrap_or_default()
            ),
            (Method::Key, _) => format!("\n PARTITION BY {linear}KEY ({})", cols(&self.cols)),
        };
        if self.named || matches!(self.method, Method::Range | Method::List) {
            let parts: Vec<String> = self
                .parts
                .iter()
                .map(|p| {
                    let mut l = format!("PARTITION {}", quote_name(&p.name));
                    match &p.values {
                        Values::None => {}
                        Values::LessThan(v) if v == &[Lit::Max] && !self.columns => {
                            l.push_str(" VALUES LESS THAN MAXVALUE");
                        }
                        Values::LessThan(v) => {
                            _ = write!(
                                l,
                                " VALUES LESS THAN ({})",
                                v.iter().map(Lit::print).collect::<Vec<_>>().join(",")
                            );
                        }
                        Values::In(v) => {
                            _ = write!(
                                l,
                                " VALUES IN ({})",
                                in_text(v, self.columns && self.cols.len() > 1)
                            );
                        }
                    }
                    if let Some(c) = &p.comment {
                        _ = write!(l, " COMMENT = {}", quote_str(c).replace("\\'", "''"));
                    }
                    l.push_str(" ENGINE = InnoDB");
                    l
                })
                .collect();
            _ = write!(s, "\n({})", parts.join(",\n "));
        } else if self.parts.len() > 1 {
            _ = write!(s, "\nPARTITIONS {}", self.parts.len());
        }
        s
    }
}

/// A `VALUES IN` list as the servers print it: `NULL` first; tuples in
/// parentheses for `LIST COLUMNS` of several columns.
fn in_text(v: &[Vec<Lit>], tuples: bool) -> String {
    let mut items: Vec<&Vec<Lit>> = v
        .iter()
        .filter(|t| t.iter().all(|l| *l == Lit::Null))
        .collect();
    items.extend(v.iter().filter(|t| !t.iter().all(|l| *l == Lit::Null)));
    items
        .iter()
        .map(|t| {
            let inner = t.iter().map(Lit::print).collect::<Vec<_>>().join(",");
            if tuples { format!("({inner})") } else { inner }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The columns expression `e` reads.
fn columns_of(e: &E, out: &mut Vec<String>) {
    match e {
        E::Col(c) => {
            if !out.iter().any(|x| x.eq_ignore_ascii_case(c)) {
                out.push(c.clone());
            }
        }
        E::Unary(_, x) | E::Not(x) | E::Bang(x) | E::Cast(x, _) | E::Collate(x, _) => {
            columns_of(x, out);
        }
        E::Bin2(_, a, b) | E::Regexp(a, b) => {
            columns_of(a, out);
            columns_of(b, out);
        }
        E::Logic(_, v) | E::Func(_, v) => v.iter().for_each(|x| columns_of(x, out)),
        E::Is(x, _, _) => columns_of(x, out),
        E::Like(a, b, c, _) => {
            columns_of(a, out);
            columns_of(b, out);
            if let Some(c) = c {
                columns_of(c, out);
            }
        }
        E::In(a, v, _) => {
            columns_of(a, out);
            for x in v {
                columns_of(x, out);
            }
        }
        E::Between(a, b, c, _) => {
            columns_of(a, out);
            columns_of(b, out);
            columns_of(c, out);
        }
        E::Case(o, w, x) => {
            if let Some(o) = o {
                columns_of(o, out);
            }
            for (a, b) in w {
                columns_of(a, out);
                columns_of(b, out);
            }
            if let Some(x) = x {
                columns_of(x, out);
            }
        }
        _ => {}
    }
}

fn refuse(what: &str) -> Fail {
    Fail::Err(sprig_common::SprigError::standard(
        1235,
        *b"42000",
        format!("This version of SprigDB doesn't yet support '{what}'"),
    ))
}

/// The constant expression `e` gives (a bound or list value).
pub(crate) fn constant(e: &E) -> Option<Lit> {
    match e {
        E::Num(n) => n.parse::<i64>().ok().map(Lit::Int),
        E::Unary("-", x) => match &**x {
            E::Num(n) => n.parse::<i64>().ok().map(|v| Lit::Int(-v)),
            _ => evaluate(e),
        },
        E::Str(s, _) => Some(Lit::Str(s.clone())),
        E::Null => Some(Lit::Null),
        _ => evaluate(e),
    }
}

/// `e` worked out by the expression engine (it reads no column).
fn evaluate(e: &E) -> Option<Lit> {
    let text = expr::print(e);
    let ast = sqlparser::parser::Parser::new(&sprig_plan::dialect::SprigMySql)
        .try_with_sql(&text)
        .ok()?
        .parse_expr()
        .ok()?;
    let session = sprig_exec::Session::new(sprig_exec::Persona::MariaDb);
    let db = sprig_plan::mem::MemDb::new("");
    let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new(db.clone());
    let planner = sprig_plan::Planner::new(&db, storage, &session);
    let table = sprig_plan::catalog::TableDef {
        schema: String::new(),
        name: String::new(),
        columns: Vec::new(),
        primary: None,
        indexes: Vec::new(),
    };
    let (x, _) = planner.scalar(&table, &ast).ok()?;
    let c = sprig_exec::compile(&x, &session).ok()?;
    let mut ctx = sprig_exec::Ctx::with_params(&session, &[]);
    match c.eval(&mut ctx, &[]).ok()? {
        V::Int(n) => Some(Lit::Int(n)),
        V::UInt(n) => i64::try_from(n).ok().map(Lit::Int),
        V::Null => Some(Lit::Null),
        V::Str(b) => Some(Lit::Str(String::from_utf8_lossy(&b).into_owned())),
        _ => None,
    }
}

/// One value of a `VALUES` list: `MAXVALUE` (where `max`) or a constant.
fn value(p: &mut P<'_>, cs: &str, max: bool) -> Result<Lit, Fail> {
    if max && p.eat("MAXVALUE") {
        return Ok(Lit::Max);
    }
    let at = p.i;
    let e = p.expr(cs)?;
    constant(&e).ok_or(Fail::Syntax(at))
}

/// `(v, ...)`.
fn tuple(p: &mut P<'_>, cs: &str, max: bool) -> Result<Vec<Lit>, Fail> {
    p.want_op("(")?;
    let mut v = vec![value(p, cs, max)?];
    while p.eat_op(",") {
        v.push(value(p, cs, max)?);
    }
    p.want_op(")")?;
    Ok(v)
}

/// A partition's options (kept: its comment).
fn part_options(p: &mut P<'_>) -> Result<Option<String>, Fail> {
    let mut comment = None;
    loop {
        if p.eat("STORAGE") {
            p.want("ENGINE")?;
            p.eat_op("=");
            p.ident()?;
        } else if p.eat("ENGINE") {
            p.eat_op("=");
            p.ident()?;
        } else if p.eat("COMMENT") {
            p.eat_op("=");
            comment = Some(p.string()?);
        } else if p.is("DATA") || p.is("INDEX") {
            p.i += 1;
            p.want("DIRECTORY")?;
            p.eat_op("=");
            p.string()?;
        } else if p.is("MAX_ROWS") || p.is("MIN_ROWS") {
            p.i += 1;
            p.eat_op("=");
            p.number()?;
        } else if p.eat("TABLESPACE") || p.eat("NODEGROUP") {
            p.eat_op("=");
            if p.ident().is_err() {
                p.number()?;
            }
        } else {
            return Ok(comment);
        }
    }
}

/// `(PARTITION p [VALUES ...] [options], ...)`.
pub(crate) fn part_list(p: &mut P<'_>, cs: &str) -> Result<Vec<Part>, Fail> {
    p.want_op("(")?;
    let mut parts = Vec::new();
    loop {
        p.want("PARTITION")?;
        let name = p.ident()?;
        let values = if p.eat("VALUES") {
            if p.eat("LESS") {
                p.want("THAN")?;
                if p.eat("MAXVALUE") {
                    Values::LessThan(vec![Lit::Max])
                } else {
                    Values::LessThan(tuple(p, cs, true)?)
                }
            } else {
                p.want("IN")?;
                p.want_op("(")?;
                let mut items = Vec::new();
                loop {
                    if p.op("(") {
                        items.push(tuple(p, cs, false)?);
                    } else {
                        items.push(vec![value(p, cs, false)?]);
                    }
                    if p.eat_op(",") {
                        continue;
                    }
                    p.want_op(")")?;
                    break;
                }
                Values::In(items)
            }
        } else {
            Values::None
        };
        if p.is("SUBPARTITION") || p.op("(") {
            return Err(refuse("subpartitions"));
        }
        let comment = part_options(p)?;
        parts.push(Part {
            name,
            values,
            comment,
        });
        if p.eat_op(",") {
            continue;
        }
        p.want_op(")")?;
        return Ok(parts);
    }
}

/// `(col, ...)` (`KEY ()` may be empty).
fn col_list(p: &mut P<'_>, empty: bool) -> Result<Vec<String>, Fail> {
    p.want_op("(")?;
    let mut v = Vec::new();
    if empty && p.eat_op(")") {
        return Ok(v);
    }
    loop {
        v.push(p.ident()?);
        if p.eat_op(",") {
            continue;
        }
        p.want_op(")")?;
        return Ok(v);
    }
}

/// Reads `PARTITION BY ...` (the cursor on `PARTITION`).
///
/// # Errors
/// 1064; 1235 for subpartitions; the count's 1064.
pub(crate) fn read(p: &mut P<'_>, cs: &str) -> Result<Spec, Fail> {
    p.want("PARTITION")?;
    p.want("BY")?;
    let linear = p.eat("LINEAR");
    let method = if p.eat("RANGE") {
        Method::Range
    } else if p.eat("LIST") {
        Method::List
    } else if p.eat("HASH") {
        Method::Hash
    } else if p.eat("KEY") {
        Method::Key
    } else {
        return Err(Fail::Syntax(p.i));
    };
    if linear && matches!(method, Method::Range | Method::List) {
        return Err(Fail::Syntax(p.i - 1));
    }
    let mut spec = Spec {
        method,
        linear,
        columns: false,
        expr: None,
        cols: Vec::new(),
        parts: Vec::new(),
        named: false,
    };
    match method {
        Method::Range | Method::List if p.eat("COLUMNS") => {
            spec.columns = true;
            spec.cols = col_list(p, false)?;
        }
        Method::Key => {
            if p.eat("ALGORITHM") {
                p.want_op("=")?;
                p.number()?;
            }
            spec.cols = col_list(p, true)?;
        }
        _ => spec.expr = Some(p.paren_expr(cs)?),
    }
    let mut count = None;
    let count_at = p.i;
    if p.eat("PARTITIONS") {
        let n = p.number()?;
        if n == 0 {
            return Err(err(
                1492,
                "HY000",
                "Number of partitions = 0 is not an allowed value",
            ));
        }
        count = Some(u32::try_from(n).unwrap_or(u32::MAX));
    }
    if p.is("SUBPARTITION") {
        return Err(refuse("subpartitions"));
    }
    if p.op("(") {
        let list_at = p.i;
        spec.parts = part_list(p, cs)?;
        spec.named = true;
        if count.is_some_and(|n| n as usize != spec.parts.len()) {
            let _ = count_at;
            return Err(Fail::Err(sprig_common::SprigError::standard(
                1064,
                *b"42000",
                format!(
                    "Wrong number of partitions defined, mismatch with previous setting near \
                     '{}' at line 1",
                    near(p, list_at)
                ),
            )));
        }
    } else if matches!(method, Method::Hash | Method::Key) {
        spec.parts = (0..count.unwrap_or(1))
            .map(|i| Part {
                name: format!("p{i}"),
                values: Values::None,
                comment: None,
            })
            .collect();
    }
    Ok(spec)
}

/// The statement's tokens from `at` as the servers quote them after
/// `near` (approximately: tokens joined by spaces).
fn near(p: &P<'_>, at: usize) -> String {
    let mut s = String::new();
    for t in &p.t[at..] {
        let piece = match &t.t {
            T::Word(w) | T::Num(w) => w.clone(),
            T::Quoted(q) => q.clone(),
            T::Str(x) => format!("'{x}'"),
            T::Op(o) => (*o).to_owned(),
            T::Var(v) => v.clone(),
            T::Hex(h) | T::HexNum(h) | T::Bin(h) => h.clone(),
        };
        let glue = matches!(t.t, T::Op("(" | ")" | ",")) || s.ends_with('(');
        if !s.is_empty() && !glue {
            s.push(' ');
        }
        s.push_str(&piece);
    }
    s.chars().take(80).collect()
}

/// Whether a column of type `base` may be a `COLUMNS` partitioning column.
fn columns_type(base: &str) -> bool {
    matches!(
        base,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "bigint"
            | "date"
            | "datetime"
            | "char"
            | "varchar"
            | "binary"
            | "varbinary"
    )
}

fn int_type(base: &str) -> bool {
    matches!(
        base,
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "year" | "bit"
    )
}

/// Checks `spec` against `def` as the servers do (`mysql`: MySQL's
/// wording).
///
/// # Errors
/// 1054, 1480, 1481, 1492, 1493, 1495, 1503, 1517, 1659.
#[allow(clippy::too_many_lines)] // one check after another, in the servers' order
pub(crate) fn check(spec: &Spec, def: &Def, mysql: bool) -> Result<(), Fail> {
    let what = |m: Method| match m {
        Method::Range => "RANGE",
        _ => "LIST",
    };
    // Columns that exist, of allowed types.
    let read = spec.read_columns(def);
    for c in &read {
        let Some(i) = def.col(c) else {
            return Err(err(
                1054,
                "42S22",
                format!(
                    "Unknown column '{c}' in '{}'",
                    if mysql {
                        "partition function"
                    } else {
                        "PARTITION BY"
                    }
                ),
            ));
        };
        let base = def.cols[i].ty.base.as_str();
        let allowed = if spec.columns {
            columns_type(base)
        } else {
            match (&spec.method, &spec.expr) {
                (Method::Key, _) => !matches!(
                    base,
                    "tinyblob"
                        | "blob"
                        | "mediumblob"
                        | "longblob"
                        | "tinytext"
                        | "text"
                        | "mediumtext"
                        | "longtext"
                        | "json"
                ),
                (_, Some(E::Col(_))) => int_type(base),
                _ => true,
            }
        };
        if !allowed {
            return Err(err(
                1659,
                "HY000",
                format!(
                    "Field '{}' is of a not allowed type for this type of partitioning",
                    def.cols[i].name
                ),
            ));
        }
    }
    if matches!(spec.method, Method::Range | Method::List) && spec.parts.is_empty() {
        return Err(err(
            1492,
            "HY000",
            format!(
                "For {} partitions each partition must be defined",
                what(spec.method)
            ),
        ));
    }
    let width = if spec.columns { spec.cols.len() } else { 1 };
    let mut names: Vec<String> = Vec::new();
    for (k, part) in spec.parts.iter().enumerate() {
        if names.iter().any(|n| n.eq_ignore_ascii_case(&part.name)) {
            return Err(err(
                1517,
                "HY000",
                format!("Duplicate partition name {}", part.name),
            ));
        }
        names.push(part.name.clone());
        match (&spec.method, &part.values) {
            (Method::Range | Method::Hash | Method::Key, Values::In(_)) => {
                return Err(err(
                    1480,
                    "HY000",
                    "Only LIST PARTITIONING can use VALUES IN in partition definition",
                ));
            }
            (Method::List | Method::Hash | Method::Key, Values::LessThan(_)) => {
                return Err(err(
                    1480,
                    "HY000",
                    "Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition",
                ));
            }
            (Method::Range, Values::None) => {
                return Err(err(
                    1479,
                    "HY000",
                    "Syntax error: RANGE PARTITIONING requires definition of VALUES LESS THAN \
                     for each partition",
                ));
            }
            (Method::List, Values::None) => {
                return Err(err(
                    1479,
                    "HY000",
                    "Syntax error: LIST PARTITIONING requires definition of VALUES IN for each \
                     partition",
                ));
            }
            (Method::Range, Values::LessThan(v)) => {
                if v.len() != width {
                    return Err(err(
                        1653,
                        "HY000",
                        "Inconsistency in usage of column lists for partitioning",
                    ));
                }
                if !spec.columns && v.iter().any(|l| matches!(l, Lit::Str(_) | Lit::Null)) {
                    return Err(err(
                        1697,
                        "HY000",
                        format!(
                            "VALUES value for partition '{}' must have type INT",
                            part.name
                        ),
                    ));
                }
                if v.first() == Some(&Lit::Max) && k + 1 != spec.parts.len() {
                    return Err(err(
                        1481,
                        "HY000",
                        "MAXVALUE can only be used in last partition definition",
                    ));
                }
                if k > 0
                    && let Values::LessThan(prev) = &spec.parts[k - 1].values
                    && !increasing(prev, v)
                {
                    return Err(err(
                        1493,
                        "HY000",
                        "VALUES LESS THAN value must be strictly increasing for each partition",
                    ));
                }
            }
            (Method::List, Values::In(items)) => {
                for t in items {
                    if t.len() != width {
                        return Err(err(
                            1653,
                            "HY000",
                            "Inconsistency in usage of column lists for partitioning",
                        ));
                    }
                    if !spec.columns && t.iter().any(|l| matches!(l, Lit::Str(_))) {
                        return Err(err(
                            1697,
                            "HY000",
                            format!(
                                "VALUES value for partition '{}' must have type INT",
                                part.name
                            ),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    // A value in two LIST partitions.
    if spec.method == Method::List {
        let mut seen: Vec<&Vec<Lit>> = Vec::new();
        for part in &spec.parts {
            if let Values::In(items) = &part.values {
                for t in items {
                    if seen.contains(&t) {
                        return Err(err(
                            1495,
                            "HY000",
                            "Multiple definition of same constant in list partitioning",
                        ));
                    }
                    seen.push(t);
                }
            }
        }
    }
    // Every unique key covers the partitioning's columns.
    for k in &def.keys {
        if !matches!(k.kind, KeyKind::Primary | KeyKind::Unique) {
            continue;
        }
        let covers = read.iter().all(|c| {
            k.parts
                .iter()
                .any(|p| p.col.eq_ignore_ascii_case(c) && p.len.is_none())
        });
        if !covers {
            let which = if k.kind == KeyKind::Primary {
                "PRIMARY KEY"
            } else {
                "UNIQUE INDEX"
            };
            let suffix = if mysql {
                " (prefixed columns are not considered)."
            } else {
                ""
            };
            return Err(err(
                1503,
                "HY000",
                format!(
                    "A {which} must include all columns in the table's partitioning function{suffix}"
                ),
            ));
        }
    }
    Ok(())
}

/// Whether bound `b` is strictly above bound `a` (column by column, as
/// `RANGE COLUMNS` compares; `MAXVALUE` above everything).
fn increasing(a: &[Lit], b: &[Lit]) -> bool {
    for (x, y) in a.iter().zip(b) {
        match (x, y) {
            (Lit::Max, Lit::Max) => {}
            (Lit::Max, _) => return false,
            (_, Lit::Max) => return true,
            (Lit::Int(x), Lit::Int(y)) if x != y => return x < y,
            (Lit::Str(x), Lit::Str(y)) if x != y => return x < y,
            _ => {}
        }
    }
    false
}

/// A partitioning change of `ALTER TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Change {
    /// `PARTITION BY ...`.
    By(Spec),
    /// `REMOVE PARTITIONING`.
    Remove,
    /// `ADD PARTITION (...)`.
    Add(Vec<Part>),
    /// `ADD PARTITION PARTITIONS n`.
    AddCount(u32),
    /// `DROP PARTITION p, ...`.
    Drop(Vec<String>),
    /// `TRUNCATE PARTITION p, ... | ALL` (`None`: all).
    Truncate(Option<Vec<String>>),
    /// `COALESCE PARTITION n`.
    Coalesce(u32),
    /// `REORGANIZE PARTITION p, ... INTO (...)`.
    Reorganize(Vec<String>, Vec<Part>),
    /// `ANALYZE | CHECK | OPTIMIZE | REBUILD | REPAIR PARTITION ...`: the
    /// names (`None`: all); nothing changes.
    Maintain(Option<Vec<String>>),
}

fn name_list(p: &mut P<'_>) -> Result<Vec<String>, Fail> {
    let mut v = vec![p.ident()?];
    while p.eat_op(",") {
        v.push(p.ident()?);
    }
    Ok(v)
}

/// Reads one partitioning clause of `ALTER TABLE` at the cursor; `None`
/// when there is none there.
///
/// # Errors
/// 1064; 1235 for `EXCHANGE`, `DISCARD`, `IMPORT` and subpartitions.
pub(crate) fn read_change(p: &mut P<'_>, cs: &str) -> Result<Option<Change>, Fail> {
    if p.is("PARTITION") && p.is_at(1, "BY") {
        return read(p, cs).map(|s| Some(Change::By(s)));
    }
    if p.is("REMOVE") && p.is_at(1, "PARTITIONING") {
        p.i += 2;
        return Ok(Some(Change::Remove));
    }
    let save = p.i;
    let verb = match p.peek().map(|t| &t.t) {
        Some(T::Word(w)) => w.to_ascii_uppercase(),
        _ => return Ok(None),
    };
    if !p.is_at(1, "PARTITION") {
        return Ok(None);
    }
    p.i += 2;
    let all_or_names = |p: &mut P<'_>| -> Result<Option<Vec<String>>, Fail> {
        if p.eat("ALL") {
            Ok(None)
        } else {
            name_list(p).map(Some)
        }
    };
    let c = match verb.as_str() {
        "ADD" => {
            if p.eat("PARTITIONS") {
                let n = p.number()?;
                Change::AddCount(u32::try_from(n).unwrap_or(u32::MAX))
            } else {
                Change::Add(part_list(p, cs)?)
            }
        }
        "DROP" => Change::Drop(name_list(p)?),
        "TRUNCATE" => Change::Truncate(all_or_names(p)?),
        "COALESCE" => {
            let n = p.number()?;
            Change::Coalesce(u32::try_from(n).unwrap_or(u32::MAX))
        }
        "REORGANIZE" => {
            let names = name_list(p)?;
            p.want("INTO")?;
            Change::Reorganize(names, part_list(p, cs)?)
        }
        "ANALYZE" | "CHECK" | "OPTIMIZE" | "REBUILD" | "REPAIR" => {
            Change::Maintain(all_or_names(p)?)
        }
        "EXCHANGE" | "DISCARD" | "IMPORT" => {
            return Err(refuse(&format!("ALTER TABLE ... {verb} PARTITION")));
        }
        _ => {
            p.i = save;
            return Ok(None);
        }
    };
    Ok(Some(c))
}

fn unknown(name: &str, table: &str) -> Fail {
    err(
        1735,
        "HY000",
        format!("Unknown partition '{name}' in table '{table}'"),
    )
}

/// `def` after change `c` (its rows are the caller's: dropped and
/// truncated partitions' rows are deleted, a new layout's checked).
///
/// # Errors
/// The servers' errors for each change: 1505 (not partitioned), 1507,
/// 1508, 1509, 1510, 1512, 1735, and [`check`]'s for the result.
#[allow(clippy::too_many_lines)] // one arm per change
pub(crate) fn apply(def: &mut Def, c: &Change, mysql: bool) -> Result<(), Fail> {
    let not_partitioned = || {
        err(
            1505,
            "HY000",
            "Partition management on a not partitioned table is not possible",
        )
    };
    match c {
        Change::By(s) => {
            def.partition = Some(s.clone());
        }
        Change::Remove => {
            if def.partition.is_none() {
                return Err(err(
                    1505,
                    "HY000",
                    "Partition management on a not partitioned table is not possible",
                ));
            }
            def.partition = None;
            return Ok(());
        }
        Change::Add(parts) => {
            let s = def.partition.as_mut().ok_or_else(not_partitioned)?;
            if matches!(s.method, Method::Hash | Method::Key) && !s.named {
                s.named = true;
            }
            s.parts.extend(parts.iter().cloned());
        }
        Change::AddCount(n) => {
            let s = def.partition.as_mut().ok_or_else(not_partitioned)?;
            if !matches!(s.method, Method::Hash | Method::Key) {
                return Err(err(
                    1480,
                    "HY000",
                    "Only RANGE PARTITIONING can use VALUES LESS THAN in partition definition",
                ));
            }
            let base = s.parts.len();
            for i in 0..*n as usize {
                s.parts.push(Part {
                    name: format!("p{}", base + i),
                    values: Values::None,
                    comment: None,
                });
            }
        }
        Change::Drop(names) => {
            let table = def.name.clone();
            let s = def.partition.as_mut().ok_or_else(not_partitioned)?;
            if !matches!(s.method, Method::Range | Method::List) {
                return Err(err(
                    1512,
                    "HY000",
                    "DROP PARTITION can only be used on RANGE/LIST partitions",
                ));
            }
            if names.iter().any(|n| s.part(n).is_none()) {
                let _ = table;
                return Err(err(
                    1507,
                    "HY000",
                    if mysql {
                        "Error in list of partitions to DROP"
                    } else {
                        "Wrong partition name or partition list"
                    },
                ));
            }
            if names.len() >= s.parts.len() {
                return Err(err(
                    1508,
                    "HY000",
                    "Cannot remove all partitions, use DROP TABLE instead",
                ));
            }
            s.parts
                .retain(|p| !names.iter().any(|n| n.eq_ignore_ascii_case(&p.name)));
        }
        Change::Truncate(names) | Change::Maintain(names) => {
            let s = def.partition.as_ref().ok_or_else(not_partitioned)?;
            if let Some(names) = names
                && let Some(n) = names.iter().find(|n| s.part(n).is_none())
            {
                return Err(unknown(n, &def.name));
            }
            return Ok(());
        }
        Change::Coalesce(n) => {
            let s = def.partition.as_mut().ok_or_else(not_partitioned)?;
            if !matches!(s.method, Method::Hash | Method::Key) {
                return Err(err(
                    1509,
                    "HY000",
                    "COALESCE PARTITION can only be used on HASH/KEY partitions",
                ));
            }
            if *n as usize >= s.parts.len() {
                return Err(err(
                    1508,
                    "HY000",
                    "Cannot remove all partitions, use DROP TABLE instead",
                ));
            }
            let keep = s.parts.len() - *n as usize;
            s.parts.truncate(keep);
        }
        Change::Reorganize(names, parts) => {
            let table = def.name.clone();
            let s = def.partition.as_mut().ok_or_else(not_partitioned)?;
            let _ = &table;
            let mut at: Vec<usize> = Vec::new();
            for n in names {
                at.push(s.part(n).ok_or_else(|| {
                    err(
                        1507,
                        "HY000",
                        if mysql {
                            "Error in list of partitions to REORGANIZE"
                        } else {
                            "Wrong partition name or partition list"
                        },
                    )
                })?);
            }
            at.sort_unstable();
            if at.windows(2).any(|w| w[1] != w[0] + 1) && s.method == Method::Range {
                return Err(err(
                    1519,
                    "HY000",
                    "When reorganizing a set of partitions they must be in consecutive order",
                ));
            }
            let first = at[0];
            // RANGE: the new partitions cover the same range, or extend the
            // last partition's.
            if s.method == Method::Range {
                let last_of = |v: &[Part]| match v.last().map(|p| &p.values) {
                    Some(Values::LessThan(b)) => Some(b.clone()),
                    _ => None,
                };
                let old_hi = match &s.parts[*at.last().unwrap_or(&first)].values {
                    Values::LessThan(b) => Some(b.clone()),
                    _ => None,
                };
                let new_hi = last_of(parts);
                let is_last = at.last().copied() == Some(s.parts.len() - 1);
                let ok = match (&old_hi, &new_hi) {
                    (Some(o), Some(n)) if o == n => true,
                    (Some(o), Some(n)) => is_last && increasing(o, n),
                    _ => false,
                };
                if !ok {
                    return Err(err(
                        1520,
                        "HY000",
                        "Reorganize of range partitions cannot change total ranges except for \
                         last partition where it can extend the range",
                    ));
                }
            }
            for &i in at.iter().rev() {
                s.parts.remove(i);
            }
            for (k, part) in parts.iter().enumerate() {
                s.parts.insert(first + k, part.clone());
            }
            s.named = true;
        }
    }
    if let Some(s) = &def.partition {
        check(s, def, mysql)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(sql: &str) -> Spec {
        let t = super::super::lex::lex(sql).unwrap();
        let mut p = P {
            t: &t,
            i: 0,
            mysql: false,
        };
        read(&mut p, "utf8mb4").unwrap()
    }

    #[test]
    fn partitionings_read_and_print_as_mariadb() {
        let s = spec(
            "PARTITION BY RANGE (y) (PARTITION p0 VALUES LESS THAN (2020) COMMENT 'old', \
             PARTITION pm VALUES LESS THAN MAXVALUE)",
        );
        assert_eq!(
            s.print(),
            "\n PARTITION BY RANGE (`y`)\n(PARTITION `p0` VALUES LESS THAN (2020) COMMENT = \
             'old' ENGINE = InnoDB,\n PARTITION `pm` VALUES LESS THAN MAXVALUE ENGINE = InnoDB)"
        );
        let s = spec("PARTITION BY LIST (id) (PARTITION a VALUES IN (3, NULL))");
        assert_eq!(
            s.print(),
            "\n PARTITION BY LIST (`id`)\n(PARTITION `a` VALUES IN (NULL,3) ENGINE = InnoDB)"
        );
        let s = spec("PARTITION BY HASH (YEAR(d)) PARTITIONS 4");
        assert_eq!(s.print(), "\n PARTITION BY HASH (year(`d`))\nPARTITIONS 4");
        assert_eq!(s.parts[3].name, "p3");
        let s = spec("PARTITION BY LINEAR KEY ALGORITHM=2 (a) PARTITIONS 2");
        assert_eq!(s.print(), "\n PARTITION BY LINEAR KEY (`a`)\nPARTITIONS 2");
        let s = spec(
            "PARTITION BY RANGE COLUMNS (a, b) (PARTITION p0 VALUES LESS THAN (10, 'm'), \
             PARTITION p1 VALUES LESS THAN (MAXVALUE, MAXVALUE))",
        );
        assert_eq!(
            s.print(),
            "\n PARTITION BY RANGE  COLUMNS(`a`,`b`)\n(PARTITION `p0` VALUES LESS THAN \
             (10,'m') ENGINE = InnoDB,\n PARTITION `p1` VALUES LESS THAN (MAXVALUE,MAXVALUE) \
             ENGINE = InnoDB)"
        );
        assert_eq!(
            spec("PARTITION BY HASH (a)").print(),
            "\n PARTITION BY HASH (`a`)"
        );
        let s = spec(
            "PARTITION BY RANGE (TO_DAYS(d)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2020-01-01')))",
        );
        assert_eq!(s.parts[0].values, Values::LessThan(vec![Lit::Int(737_790)]));
    }
}
