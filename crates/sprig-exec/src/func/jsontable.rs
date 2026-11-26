//! `JSON_TABLE(doc, path COLUMNS (...))`: the rows a JSON document gives.
//!
//! The path selects the document's row nodes; each gives one row, or one
//! row per match of its `NESTED PATH`s (sibling nested paths give their
//! rows one after the other, the other's columns `NULL`; a node no nested
//! path matches still gives one row). A column is the node's ordinal
//! (`FOR ORDINALITY`), whether a path matches (`EXISTS PATH`), or the
//! value a path finds converted to the column's type:
//!
//! - nothing found: `ON EMPTY` (`NULL` by default, `DEFAULT 'v'`, or
//!   `ERROR`);
//! - a value that doesn't convert (an array or object for a scalar
//!   column, text that is no number): `ON ERROR`. MySQL converts as
//!   `JSON_VALUE ... RETURNING` does and fails with 3156 under `ERROR ON
//!   ERROR`; MariaDB converts the value's text as a string stored into the
//!   column (`'q'` is 0 in an `INT` column) and gives `NULL` for an array
//!   or object;
//! - MySQL reads a `DEFAULT` as JSON text (`'"x"'` is `x`), MariaDB as the
//!   column's text.

use bytes::Bytes;
use sprig_engine::row::Value;

use super::json::{
    J, Leg, Returning, compact_into, json_type, parse, parse_error_at, parse_path, print, returned,
    select,
};
use crate::error::{Result, SqlError};
use crate::expr::{Env, coerce_fn};
use crate::session::{Ctx, Session};
use crate::types::{Derivation, ExprType, Kind};

/// What `ON EMPTY` or `ON ERROR` says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JtOnMiss {
    /// `NULL` (the default).
    Null,
    /// `ERROR`.
    Error,
    /// `DEFAULT 'text'`.
    Default(String),
}

/// One entry of `COLUMNS (...)`, as written.
#[derive(Debug, Clone)]
pub enum JtColumn {
    /// `name FOR ORDINALITY`.
    Ordinality(String),
    /// `name type [EXISTS] PATH 'p' [on empty] [on error]`; `ty` is the
    /// type as written (`INT`, `VARCHAR(10)`, `JSON`, ...).
    Path {
        /// The column's name.
        name: String,
        /// Its type as written.
        ty: String,
        /// The path, relative to the row's node.
        path: String,
        /// `EXISTS PATH`: 1 when the path matches, else 0.
        exists: bool,
        /// `ON EMPTY`.
        on_empty: JtOnMiss,
        /// `ON ERROR`.
        on_error: JtOnMiss,
    },
    /// `NESTED [PATH] 'p' COLUMNS (...)`.
    Nested {
        /// The path, relative to the row's node.
        path: String,
        /// Its columns.
        columns: Vec<JtColumn>,
    },
}

enum Col {
    Ordinality(usize),
    Path {
        slot: usize,
        name: String,
        ty: ExprType,
        ret: Returning,
        path: Vec<Leg>,
        exists: bool,
        on_empty: JtOnMiss,
        on_error: JtOnMiss,
    },
    Nested {
        path: Vec<Leg>,
        cols: Vec<Col>,
        /// The slots its columns (and its nested paths') fill.
        slots: Vec<usize>,
    },
}

/// A compiled `JSON_TABLE`.
pub struct JsonTable {
    root: Vec<Leg>,
    cols: Vec<Col>,
    names: Vec<String>,
    types: Vec<ExprType>,
    maria: bool,
    /// The table's alias (MariaDB's messages name it).
    alias: String,
}

fn bad_path(maria: bool, n: usize, path: &str) -> SqlError {
    if maria {
        // A path that doesn't start with `$` fails at its first character.
        let at = if path.trim_start().starts_with('$') {
            String::new()
        } else {
            " at position 1".to_owned()
        };
        SqlError::new(
            4042,
            "HY000",
            format!("Syntax error in JSON path in argument {n} to function 'JSON_TABLE'{at}"),
        )
    } else {
        SqlError::new(
            3143,
            "42000",
            "Invalid JSON path expression. The error is around character position 1.",
        )
    }
}

/// A column type as written, as the value type and the conversion.
fn column_type(text: &str, session: &Session) -> Result<(ExprType, Returning)> {
    // A string column's `CHARACTER SET cs` / `COLLATE c` (the planner hands
    // such a type over quoted, as the parser reads none there): its
    // collation, the character set's default without one.
    let text = text.trim().trim_matches('`');
    let upper = text.to_ascii_uppercase();
    let word_after = |key: &str| {
        let at = upper.find(key)? + key.len();
        text[at..]
            .split_whitespace()
            .next()
            .map(str::to_ascii_lowercase)
    };
    let named = word_after(" COLLATE ").or_else(|| {
        word_after(" CHARACTER SET ")
            .or_else(|| word_after(" CHARSET "))
            .map(|cs| default_collation(&cs, session.persona.is_maria()))
    });
    let text = match upper
        .find(" CHARACTER SET ")
        .or_else(|| upper.find(" CHARSET "))
        .or_else(|| upper.find(" COLLATE "))
    {
        Some(at) => &text[..at],
        None => text,
    };
    let (mut ty, ret) = column_type_of(text, session)?;
    if let Some(name) = named
        && ty.kind == Kind::Str
        && ty.coll != sprig_engine::keys::collation::Collation::Binary
    {
        let flavor = if session.persona.is_maria() && !name.contains("_0900_") {
            sprig_engine::keys::collation::Flavor::MariaDb
        } else {
            sprig_engine::keys::collation::Flavor::MySql
        };
        let Some(c) = sprig_engine::keys::collation::Collation::resolve(&name, flavor) else {
            return Err(SqlError::new(
                1273,
                "HY000",
                format!("Unknown collation: '{name}'"),
            ));
        };
        ty.coll = c;
    }
    Ok((ty, ret))
}

/// A character set's default collation on a server (`utf8`, `utf8mb3`
/// and `utf8mb4` read as `utf8mb4`'s, which weighs the same strings).
fn default_collation(cs: &str, maria: bool) -> String {
    match (cs, maria) {
        ("utf8mb4" | "utf8mb3" | "utf8", true) => "utf8mb4_uca1400_ai_ci".into(),
        ("utf8mb4", false) => "utf8mb4_0900_ai_ci".into(),
        ("utf8mb3" | "utf8", false) => "utf8mb4_general_ci".into(),
        ("latin1", _) => "latin1_swedish_ci".into(),
        ("binary", _) => "binary".into(),
        (cs, _) => format!("{cs}_general_ci"),
    }
}

/// [`column_type`] of the type alone.
fn column_type_of(text: &str, session: &Session) -> Result<(ExprType, Returning)> {
    let t = text.trim().to_ascii_uppercase();
    let (word, arg) = match t.split_once('(') {
        Some((w, rest)) => (
            w.trim().to_owned(),
            Some(rest.trim_end_matches(')').to_owned()),
        ),
        None => (t.clone(), None),
    };
    let unsigned = word.ends_with(" UNSIGNED") || t.contains("UNSIGNED");
    let word = word.trim_end_matches(" UNSIGNED").trim().to_owned();
    let n = |a: &Option<String>| {
        a.as_ref()
            .and_then(|a| a.split(',').next()?.trim().parse::<u32>().ok())
    };
    let conn = session.collation_connection;
    let ty_ret = match word.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" => {
            let len = match word.as_str() {
                "BIGINT" => 20,
                "SMALLINT" => 6,
                "TINYINT" => 4,
                "MEDIUMINT" => 9,
                _ => 11,
            };
            let mut ty = ExprType::int(len, unsigned);
            ty.width = match word.as_str() {
                "BIGINT" => 8,
                "SMALLINT" => 2,
                "TINYINT" => 1,
                "MEDIUMINT" => 3,
                _ => 4,
            };
            (
                ty,
                if unsigned {
                    Returning::Unsigned
                } else {
                    Returning::Signed
                },
            )
        }
        "DECIMAL" | "NUMERIC" | "DEC" => {
            let (p, s) = match &arg {
                Some(a) => match a.split_once(',') {
                    Some((p, s)) => (
                        p.trim().parse().unwrap_or(10),
                        s.trim().parse().unwrap_or(0),
                    ),
                    None => (a.trim().parse().unwrap_or(10), 0),
                },
                None => (10, 0),
            };
            let p: u32 = p.clamp(1, 65);
            let s: u32 = s.min(30).min(p);
            (
                ExprType::decimal(p, s, unsigned),
                Returning::Decimal(p as u8, s as u8),
            )
        }
        "DOUBLE" | "REAL" => (ExprType::double(), Returning::Double),
        "FLOAT" => (
            ExprType {
                kind: Kind::Float,
                ..ExprType::double()
            },
            Returning::Float,
        ),
        "CHAR" | "VARCHAR" | "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
            let len = n(&arg).unwrap_or(match word.as_str() {
                "CHAR" => 1,
                "TEXT" => 65535,
                "TINYTEXT" => 255,
                "MEDIUMTEXT" => 16_777_215,
                "LONGTEXT" => u32::MAX,
                _ => 255,
            });
            (
                ExprType::string(len, conn, Derivation::Implicit),
                Returning::Char(Some(len)),
            )
        }
        "BINARY" | "VARBINARY" | "BLOB" => {
            let len = n(&arg).unwrap_or(65535);
            (
                ExprType::string(
                    len,
                    sprig_engine::keys::collation::Collation::Binary,
                    Derivation::Implicit,
                ),
                Returning::Binary(Some(len)),
            )
        }
        "DATE" => (ExprType::date(), Returning::Date),
        "TIME" => {
            let f = n(&arg).unwrap_or(0).min(6) as u8;
            (ExprType::time(f), Returning::Time(f))
        }
        "DATETIME" | "TIMESTAMP" => {
            let f = n(&arg).unwrap_or(0).min(6) as u8;
            (ExprType::datetime(f), Returning::DateTime(f))
        }
        "YEAR" => (
            ExprType {
                kind: Kind::Year,
                unsigned: true,
                width: 1,
                ..ExprType::int(4, true)
            },
            Returning::Year,
        ),
        "JSON" => {
            let env = Env {
                s: session,
                prep: None,
                counts: None,
            };
            (json_type(env), Returning::Json)
        }
        _ => {
            return Err(SqlError::unsupported(&format!(
                "a JSON_TABLE column of type {text}"
            )));
        }
    };
    let (mut ty, ret) = ty_ret;
    ty.nullable = true;
    Ok((ty, ret))
}

impl JsonTable {
    /// Compiles `JSON_TABLE(doc, path COLUMNS (columns))`.
    ///
    /// # Errors
    /// The servers' path errors (MySQL 3143, MariaDB 4042), 1060 for a
    /// column name used twice, and 1235 for a column type not covered.
    pub fn new(path: &str, columns: &[JtColumn], session: &Session, alias: &str) -> Result<Self> {
        let maria = session.persona.is_maria();
        let root = parse_path(path).ok_or_else(|| bad_path(maria, 2, path))?;
        let mut names = Vec::new();
        let mut types = Vec::new();
        let cols = Self::compile(columns, session, &mut names, &mut types)?;
        for (i, n) in names.iter().enumerate() {
            if names[..i].iter().any(|m| m.eq_ignore_ascii_case(n)) {
                return Err(SqlError::new(
                    1060,
                    "42S21",
                    format!("Duplicate column name '{n}'"),
                ));
            }
        }
        Ok(Self {
            root,
            cols,
            names,
            types,
            maria,
            alias: alias.to_owned(),
        })
    }

    fn compile(
        columns: &[JtColumn],
        session: &Session,
        names: &mut Vec<String>,
        types: &mut Vec<ExprType>,
    ) -> Result<Vec<Col>> {
        let maria = session.persona.is_maria();
        let mut out = Vec::with_capacity(columns.len());
        for c in columns {
            match c {
                JtColumn::Ordinality(name) => {
                    out.push(Col::Ordinality(names.len()));
                    names.push(name.clone());
                    // MariaDB: INT(11); MySQL: a BIGINT (MySQL shows a
                    // display width of 10, which a result column's type
                    // doesn't carry here).
                    let mut t = ExprType::int(if maria { 11 } else { 20 }, false);
                    t.width = if maria { 4 } else { 8 };
                    t.nullable = true;
                    types.push(t);
                }
                JtColumn::Path {
                    name,
                    ty,
                    path,
                    exists,
                    on_empty,
                    on_error,
                } => {
                    let (mut t, ret) = column_type(ty, session)?;
                    // MySQL reads a DEFAULT as JSON text, before any row.
                    if !maria {
                        for m in [on_empty, on_error] {
                            if let JtOnMiss::Default(d) = m
                                && parse(d.as_bytes()).is_none()
                            {
                                return Err(SqlError::new(
                                    3141,
                                    "22032",
                                    format!(
                                        "Invalid JSON text in argument 1 to function JSON_TABLE: \
                                         \"Invalid value.\" at position {}.",
                                        parse_error_at(d.as_bytes())
                                    ),
                                ));
                            }
                        }
                    }
                    // MySQL: an `EXISTS PATH` column is an INT whatever its
                    // declared type; MariaDB keeps the declared type.
                    if *exists && !maria {
                        t = ExprType::int(11, false);
                        t.width = 4;
                        t.nullable = true;
                    }
                    let legs = parse_path(path).ok_or_else(|| bad_path(maria, 2, path))?;
                    out.push(Col::Path {
                        slot: names.len(),
                        name: name.clone(),
                        ty: t,
                        ret,
                        path: legs,
                        exists: *exists,
                        on_empty: on_empty.clone(),
                        on_error: on_error.clone(),
                    });
                    names.push(name.clone());
                    types.push(t);
                }
                JtColumn::Nested { path, columns } => {
                    let legs = parse_path(path).ok_or_else(|| bad_path(maria, 2, path))?;
                    let first = names.len();
                    let cols = Self::compile(columns, session, names, types)?;
                    out.push(Col::Nested {
                        path: legs,
                        cols,
                        slots: (first..names.len()).collect(),
                    });
                }
            }
        }
        Ok(out)
    }

    /// The column names.
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// The column types.
    #[must_use]
    pub fn types(&self) -> &[ExprType] {
        &self.types
    }

    /// The rows `doc` (of static type `doc_ty`) gives: none for `NULL`.
    ///
    /// # Errors
    /// Text that isn't JSON (MySQL 3141, MariaDB 4038), `ERROR ON EMPTY`
    /// / `ERROR ON ERROR` (MySQL 3665 / 3156; MariaDB 4174 / 4175).
    pub fn rows(&self, ctx: &mut Ctx<'_>, doc: &Value) -> Result<Vec<Vec<Value>>> {
        let bytes = match doc {
            Value::Null => return Ok(Vec::new()),
            Value::Json(b) | Value::Str(b) | Value::Bytes(b) => b.clone(),
            other => {
                crate::value_text(other, &ExprType::double(), ctx.persona()).unwrap_or_default()
            }
        };
        let Some(j) = parse(&bytes) else {
            let at = parse_error_at(&bytes);
            return Err(if self.maria && at >= bytes.len() {
                SqlError::new(
                    4037,
                    "HY000",
                    "Unexpected end of JSON text in argument 1 to function 'JSON_TABLE'",
                )
            } else if self.maria {
                SqlError::new(
                    4038,
                    "HY000",
                    format!(
                        "Syntax error in JSON text in argument 1 to function 'JSON_TABLE' at \
                         position {}",
                        at + 1
                    ),
                )
            } else {
                SqlError::new(
                    3141,
                    "22032",
                    format!(
                        "Invalid JSON text in argument 1 to function json_table: \"Invalid \
                         value.\" at position {at}."
                    ),
                )
            });
        };
        let mut nodes = Vec::new();
        select(&j, &self.root, &mut nodes);
        let mut out = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            let mut row = vec![Value::Null; self.names.len()];
            let at = out.len() + 1;
            self.fill(ctx, node, i + 1, &self.cols, &mut row, &mut out, at)?;
        }
        Ok(out)
    }

    /// Fills `row` from `node` for `cols`, pushing each finished row.
    #[allow(clippy::too_many_arguments)]
    fn fill(
        &self,
        ctx: &mut Ctx<'_>,
        node: &J,
        ordinal: usize,
        cols: &[Col],
        row: &mut Vec<Value>,
        out: &mut Vec<Vec<Value>>,
        at: usize,
    ) -> Result<()> {
        let mut nested = Vec::new();
        for c in cols {
            match c {
                Col::Ordinality(slot) => {
                    row[*slot] = Value::Int(i64::try_from(ordinal).unwrap_or(i64::MAX));
                }
                Col::Path {
                    slot,
                    name,
                    ty,
                    ret,
                    path,
                    exists,
                    on_empty,
                    on_error,
                } => {
                    let mut found = Vec::new();
                    select(node, path, &mut found);
                    row[*slot] = if *exists && self.maria {
                        let bit = if found.is_empty() { "0" } else { "1" };
                        store_text(ctx, bit, ty, name, at)
                    } else if *exists {
                        Value::Int(i64::from(!found.is_empty()))
                    } else {
                        self.value(ctx, &found, name, ty, *ret, on_empty, on_error, at)?
                    };
                }
                Col::Nested { path, cols, slots } => nested.push((path, cols, slots)),
            }
        }
        if nested.is_empty() {
            out.push(row.clone());
            return Ok(());
        }
        let mut any = false;
        for (k, (path, cols, slots)) in nested.iter().enumerate() {
            let mut found = Vec::new();
            select(node, path, &mut found);
            for (n, f) in found.iter().enumerate() {
                any = true;
                // The other nested paths' columns are NULL here.
                for (m, (_, _, other)) in nested.iter().enumerate() {
                    if m != k {
                        for s in other.iter() {
                            row[*s] = Value::Null;
                        }
                    }
                }
                self.fill(ctx, f, n + 1, cols, row, out, at)?;
            }
            for s in slots.iter() {
                row[*s] = Value::Null;
            }
        }
        if !any {
            out.push(row.clone());
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn value(
        &self,
        ctx: &mut Ctx<'_>,
        found: &[&J],
        name: &str,
        ty: &ExprType,
        ret: Returning,
        on_empty: &JtOnMiss,
        on_error: &JtOnMiss,
        at: usize,
    ) -> Result<Value> {
        let maria = self.maria;
        let Some(first) = found.first() else {
            return match on_empty {
                JtOnMiss::Null => Ok(Value::Null),
                JtOnMiss::Error => Err(if maria {
                    SqlError::new(
                        4174,
                        "HY000",
                        format!(
                            "Field '{name}' can't be set for JSON_TABLE '{}'.",
                            self.alias
                        ),
                    )
                } else {
                    SqlError::new(
                        3665,
                        "22035",
                        format!("Missing value for JSON_TABLE column '{name}'"),
                    )
                }),
                JtOnMiss::Default(d) => self.default_value(ctx, d, ty, ret, name, at),
            };
        };
        if found.len() > 1 && !maria {
            return self.on_error(ctx, on_error, name, ty, ret, at, true);
        }
        if maria {
            // MariaDB: the value's text (as written) stored into the column.
            return Ok(match first {
                J::Null => Value::Null,
                J::Arr(_) | J::Obj(_) if ret != Returning::Json => {
                    return self.on_error(ctx, on_error, name, ty, ret, at, false);
                }
                j => {
                    let numeric = matches!(
                        ty.kind,
                        Kind::Int | Kind::Decimal | Kind::Double | Kind::Float | Kind::Year
                    );
                    let text = match j {
                        _ if ret == Returning::Json => {
                            let mut s = String::new();
                            compact_into(j, &mut s);
                            s
                        }
                        J::Str(s) | J::Num(s) => s.clone(),
                        J::Bool(b) if numeric => u8::from(*b).to_string(),
                        J::Bool(b) => b.to_string(),
                        other => {
                            let mut s = String::new();
                            print(other, &mut s);
                            s
                        }
                    };
                    store_text(ctx, &text, ty, name, at)
                }
            });
        }
        if matches!(first, J::Null) {
            return Ok(Value::Null);
        }
        // MySQL: an array or object is no scalar column's value.
        if matches!(first, J::Arr(_) | J::Obj(_)) && ret != Returning::Json {
            return self.on_error(ctx, on_error, name, ty, ret, at, false);
        }
        match returned(ctx, first, ret).filter(|v| in_range(v, ty)) {
            Some(v) => Ok(v),
            None => self.on_error(ctx, on_error, name, ty, ret, at, false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_error(
        &self,
        ctx: &mut Ctx<'_>,
        on_error: &JtOnMiss,
        name: &str,
        ty: &ExprType,
        ret: Returning,
        at: usize,
        many: bool,
    ) -> Result<Value> {
        match on_error {
            JtOnMiss::Null => Ok(Value::Null),
            JtOnMiss::Default(d) => self.default_value(ctx, d, ty, ret, name, at),
            JtOnMiss::Error => Err(if self.maria {
                SqlError::new(
                    4175,
                    "HY000",
                    format!(
                        "Field '{name}' can't be set for JSON_TABLE '{}'.",
                        self.alias
                    ),
                )
            } else if many {
                SqlError::new(
                    3666,
                    "22034",
                    format!(
                        "Can't store an array or an object in the scalar JSON_TABLE column '{name}'"
                    ),
                )
            } else {
                let word = match ty.kind {
                    Kind::Int | Kind::Year | Kind::Bit => "INTEGER",
                    Kind::Decimal => "DECIMAL",
                    Kind::Double | Kind::Float => "DOUBLE",
                    Kind::Date => "DATE",
                    Kind::Time => "TIME",
                    Kind::DateTime | Kind::Timestamp => "DATETIME",
                    _ => "CHAR",
                };
                SqlError::new(
                    3156,
                    "22018",
                    format!("Invalid JSON value for CAST to {word} from column {name} at row {at}"),
                )
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn default_value(
        &self,
        ctx: &mut Ctx<'_>,
        d: &str,
        ty: &ExprType,
        ret: Returning,
        name: &str,
        at: usize,
    ) -> Result<Value> {
        if self.maria {
            return Ok(store_text(ctx, d, ty, name, at));
        }
        // MySQL reads the default as JSON text.
        Ok(match parse(d.as_bytes()) {
            Some(j) => returned(ctx, &j, ret).unwrap_or(Value::Null),
            None => Value::Null,
        })
    }
}

/// The integer range of a column of type `ty` (its storage width).
fn int_range(ty: &ExprType) -> (i128, i128) {
    let bits = match ty.width {
        1 => 8,
        2 => 16,
        3 => 24,
        4 => 32,
        _ => 64,
    };
    if ty.unsigned {
        (0, (1i128 << bits) - 1)
    } else {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    }
}

/// MySQL: whether a converted value fits its integer column (else the
/// column's `ON ERROR`).
fn in_range(v: &Value, ty: &ExprType) -> bool {
    if ty.kind != Kind::Int {
        return true;
    }
    let (lo, hi) = int_range(ty);
    match v {
        Value::Int(i) => (lo..=hi).contains(&i128::from(*i)),
        Value::UInt(u) => (lo..=hi).contains(&i128::from(*u)),
        _ => true,
    }
}

/// The length of the number at the start of `t` (sign, digits, fraction,
/// exponent); 0 when there is none.
fn number_prefix(t: &str) -> usize {
    let b = t.as_bytes();
    let mut i = 0;
    if matches!(b.first(), Some(b'+' | b'-')) {
        i = 1;
    }
    let start = i;
    while b.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
    }
    let mut digits = i > start;
    if b.get(i) == Some(&b'.') {
        let f = i + 1;
        let mut j = f;
        while b.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if j > f || digits {
            digits |= j > f;
            i = j;
        }
    }
    if !digits {
        return 0;
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(b.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        let e = j;
        while b.get(j).is_some_and(u8::is_ascii_digit) {
            j += 1;
        }
        if j > e {
            i = j;
        }
    }
    i
}

/// MariaDB: text stored into a column of type `ty`, as a string assigned
/// to a column outside strict mode, with the warnings that gives: cut to
/// the column's length (1265); a number read from the text's start (1366
/// when there is none, 1265 when other text follows) and held in the
/// column's range (1264); a date or time that doesn't read is zero
/// (1265). `name` and `at` (the row) are for the messages.
fn store_text(ctx: &mut Ctx<'_>, text: &str, ty: &ExprType, name: &str, at: usize) -> Value {
    let truncated = |ctx: &mut Ctx<'_>| {
        ctx.warn(
            1265,
            format!("Data truncated for column '{name}' at row {at}"),
        );
    };
    if ty.kind == Kind::Str {
        let max = usize::try_from(ty.len).unwrap_or(usize::MAX);
        let cut: String = text.chars().take(max).collect();
        if cut.len() < text.len() {
            truncated(ctx);
        }
        return if ty.coll == sprig_engine::keys::collation::Collation::Binary {
            Value::Bytes(Bytes::from(cut.into_bytes()))
        } else {
            Value::Str(Bytes::from(cut.into_bytes()))
        };
    }
    let numeric = matches!(
        ty.kind,
        Kind::Int | Kind::Decimal | Kind::Double | Kind::Float | Kind::Year
    );
    let mut text = text.to_owned();
    if numeric {
        let t = text.trim_start();
        let n = number_prefix(t);
        if n == 0 {
            let word = match ty.kind {
                Kind::Decimal => "decimal",
                Kind::Double | Kind::Float => "double",
                _ => "integer",
            };
            ctx.warn(
                1366,
                format!(
                    "Incorrect {word} value: '{text}' for column `*any*`.`json_table`.`{name}` \
                     at row {at}"
                ),
            );
            text = "0".to_owned();
        } else {
            if !t[n..].trim().is_empty() {
                truncated(ctx);
            }
            text = t[..n].to_owned();
        }
        // Held in the column's range.
        let f: f64 = text.parse().unwrap_or(0.0);
        let bound = match ty.kind {
            Kind::Int => {
                let (lo, hi) = int_range(ty);
                #[allow(clippy::cast_precision_loss)]
                let r = f.round();
                #[allow(clippy::cast_precision_loss)]
                if r < lo as f64 {
                    Some(lo.to_string())
                } else if r > hi as f64 {
                    Some(hi.to_string())
                } else if text.contains(['.', 'e', 'E']) {
                    // A fraction or exponent: the number, rounded.
                    #[allow(clippy::cast_possible_truncation)]
                    let n = r as i128;
                    text = n.to_string();
                    None
                } else {
                    None
                }
            }
            Kind::Decimal => {
                let s = i32::from(ty.dec);
                let int_digits = (i32::from(ty.prec) - s).max(0);
                let max = 10f64.powi(int_digits) - 10f64.powi(-s);
                let lo = if ty.unsigned { 0.0 } else { -max };
                let digits = usize::try_from(s).unwrap_or(0);
                if f > max {
                    Some(format!("{max:.digits$}"))
                } else if f < lo {
                    Some(format!("{lo:.digits$}"))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(b) = bound {
            ctx.warn(
                1264,
                format!("Out of range value for column '{name}' at row {at}"),
            );
            text = b;
        }
    }
    let from = ExprType::string(
        u32::try_from(text.chars().count()).unwrap_or(u32::MAX),
        ctx.session.collation_connection,
        Derivation::Implicit,
    );
    let env = Env {
        s: ctx.session,
        prep: None,
        counts: None,
    };
    ctx.mute += 1;
    let v = coerce_fn(&from, ty, env)(ctx, Value::Str(Bytes::from(text.into_bytes())));
    ctx.mute -= 1;
    match v {
        Ok(v) if !v.is_null() => v,
        _ if numeric => Value::Null,
        _ => {
            // A date or time that doesn't read: zero.
            truncated(ctx);
            let zero = match ty.kind {
                Kind::Time => "00:00:00",
                Kind::Date => "0000-00-00",
                _ => "0000-00-00 00:00:00",
            };
            let env = Env {
                s: ctx.session,
                prep: None,
                counts: None,
            };
            ctx.mute += 1;
            let z = coerce_fn(
                &ExprType::string(19, ctx.session.collation_connection, Derivation::Implicit),
                ty,
                env,
            )(ctx, Value::Str(Bytes::from_static(zero.as_bytes())));
            ctx.mute -= 1;
            z.unwrap_or(Value::Null)
        }
    }
}
