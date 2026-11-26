//! Column types as written, normalized to the server's spelling, and the
//! character set rules of string columns.

use super::lex::{T, Tok};
use super::{Fail, err};

/// A column's type, normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // independent attributes of a type
pub(crate) struct Ty {
    /// The base name as printed: `int`, `varchar`, `decimal`, `enum`, ...
    pub base: String,
    /// The printed arguments: `(11)`, `(10,2)`, `('a','b')`; empty for none.
    pub args: String,
    /// `unsigned`.
    pub unsigned: bool,
    /// `zerofill`.
    pub zerofill: bool,
    /// A string column's character set and collation (resolved).
    pub cs: Option<(String, String)>,
    /// The type was written `JSON` (adds the `json_valid` check).
    pub json: bool,
    /// `SERIAL` (adds `NOT NULL AUTO_INCREMENT` and a unique key).
    pub serial: bool,
    /// The members of an `ENUM` / `SET`, unescaped.
    pub members: Vec<String>,
    /// The length (characters or bytes) of a string type, for key prefix
    /// checks; the precision of a number.
    pub len: u32,
}

impl Ty {
    /// The type holds characters (has a character set).
    pub(crate) fn is_string(&self) -> bool {
        matches!(
            self.base.as_str(),
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        )
    }

    /// A string or byte type a key may take a prefix of.
    pub(crate) fn prefixable(&self) -> bool {
        matches!(
            self.base.as_str(),
            "char"
                | "varchar"
                | "tinytext"
                | "text"
                | "mediumtext"
                | "longtext"
                | "binary"
                | "varbinary"
                | "tinyblob"
                | "blob"
                | "mediumblob"
                | "longblob"
        )
    }

    /// A `BLOB` / `TEXT` type (a key on it needs a prefix).
    pub(crate) fn is_lob(&self) -> bool {
        matches!(
            self.base.as_str(),
            "tinytext"
                | "text"
                | "mediumtext"
                | "longtext"
                | "tinyblob"
                | "blob"
                | "mediumblob"
                | "longblob"
        )
    }

    /// An integer type.
    pub(crate) fn is_int(&self) -> bool {
        matches!(
            self.base.as_str(),
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
        )
    }

    /// A numeric type (an `AUTO_INCREMENT` column must be one).
    pub(crate) fn is_numeric(&self) -> bool {
        self.is_int() || matches!(self.base.as_str(), "float" | "double" | "decimal")
    }

    /// The printed type: base, arguments, `unsigned`, `zerofill`.
    pub(crate) fn print(&self) -> String {
        let mut s = format!("{}{}", self.base, self.args);
        if self.unsigned {
            s.push_str(" unsigned");
        }
        if self.zerofill {
            s.push_str(" zerofill");
        }
        s
    }

    /// The most bytes one character of the column's character set takes.
    pub(crate) fn mbmax(&self) -> u32 {
        self.cs.as_ref().map_or(1, |(cs, _)| mbmax(cs))
    }
}

/// Bytes per character, at most.
pub(crate) fn mbmax(cs: &str) -> u32 {
    match cs {
        "utf8mb4" | "utf16" | "utf16le" | "utf32" => 4,
        "utf8mb3" | "eucjpms" | "ujis" => 3,
        "ucs2" | "big5" | "gbk" | "gb2312" | "sjis" | "cp932" | "euckr" => 2,
        _ => 1,
    }
}

/// Character sets the engine knows names of, with MariaDB 11.8's default
/// collation of each.
const CHARSETS: [(&str, &str); 12] = [
    ("utf8mb4", "utf8mb4_uca1400_ai_ci"),
    ("utf8mb3", "utf8mb3_uca1400_ai_ci"),
    ("latin1", "latin1_swedish_ci"),
    ("ascii", "ascii_general_ci"),
    ("binary", "binary"),
    ("ucs2", "ucs2_uca1400_ai_ci"),
    ("utf16", "utf16_uca1400_ai_ci"),
    ("utf16le", "utf16le_general_ci"),
    ("utf32", "utf32_uca1400_ai_ci"),
    ("latin2", "latin2_general_ci"),
    ("cp1251", "cp1251_general_ci"),
    ("greek", "greek_general_ci"),
];

/// A character set's name as the server prints it (`utf8` is `utf8mb3`).
pub(crate) fn charset_name(cs: &str) -> Option<&'static str> {
    let l = cs.to_ascii_lowercase();
    let l = if l == "utf8" { "utf8mb3".to_owned() } else { l };
    CHARSETS.iter().find(|(n, _)| *n == l).map(|(n, _)| *n)
}

/// A character set's default collation.
pub(crate) fn default_collation(cs: &str) -> &'static str {
    CHARSETS
        .iter()
        .find(|(n, _)| *n == cs)
        .map_or("binary", |(_, c)| *c)
}

/// A character set's default collation on a port: MySQL 8.4's on the
/// MySQL port (`utf8mb4_0900_ai_ci`, `utf8mb3_general_ci`), MariaDB
/// 11.8's otherwise.
pub(crate) fn port_collation(cs: &str, mysql: bool) -> &'static str {
    match (mysql, cs) {
        (true, "utf8mb4") => "utf8mb4_0900_ai_ci",
        (true, "utf8mb3") => "utf8mb3_general_ci",
        _ => default_collation(cs),
    }
}

/// A database's default character set and collation as a port reads it.
/// A database records MariaDB's default collation of its character set
/// when it was created without one; on the MySQL port that stands for
/// MySQL's default of the same character set, so tables created there
/// get MySQL's collation.
pub(crate) fn port_db_cs(db: &(String, String), mysql: bool) -> (String, String) {
    if mysql && db.1.eq_ignore_ascii_case(default_collation(&db.0)) {
        (db.0.clone(), port_collation(&db.0, true).to_owned())
    } else {
        db.clone()
    }
}

/// A collation's name as printed and its character set, or `None` for a
/// collation the engine doesn't know.
pub(crate) fn collation(name: &str) -> Option<(String, &'static str)> {
    let l = name.to_ascii_lowercase();
    if l == "binary" {
        return Some(("binary".into(), "binary"));
    }
    // `utf8_*` names are `utf8mb3_*`.
    let l = match l.strip_prefix("utf8_") {
        Some(rest) => format!("utf8mb3_{rest}"),
        None => l,
    };
    let cs = CHARSETS
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| l.starts_with(&format!("{n}_")))
        .max_by_key(|n| n.len())?;
    // The engine's collations decide what is real.
    crate::engine::table::collation_named(&l)?;
    Some((l, cs))
}

/// What the column definition said about its character set.
#[derive(Debug, Default, Clone)]
pub(crate) struct CsSpec {
    pub charset: Option<String>,
    pub collate: Option<String>,
    /// `BINARY` after a string type: the character set's `_bin` collation.
    pub binary: bool,
    /// `NATIONAL` / `NCHAR`: utf8mb3.
    pub national: bool,
    /// `ASCII` / `BYTE` / `UNICODE` shorthands.
    pub short: Option<&'static str>,
}

/// Resolves a string column's character set and collation against the
/// table's.
///
/// # Errors
/// 1115, 1273, 1253.
pub(crate) fn resolve_cs(
    spec: &CsSpec,
    table: (&str, &str),
    mysql: bool,
) -> Result<(String, String), Fail> {
    let named_cs = match (&spec.charset, spec.short, spec.national) {
        (Some(c), _, _) => Some(
            charset_name(c)
                .ok_or_else(|| err(1115, "42000", format!("Unknown character set: '{c}'")))?,
        ),
        (None, Some(s), _) => Some(s),
        (None, None, true) => Some("utf8mb3"),
        _ => None,
    };
    let coll = match &spec.collate {
        Some(c) => Some(
            collation(c).ok_or_else(|| err(1273, "HY000", format!("Unknown collation: '{c}'")))?,
        ),
        None => None,
    };
    match (named_cs, coll) {
        (Some(cs), Some((co, ccs))) => {
            if cs != ccs {
                return Err(err(
                    1253,
                    "42000",
                    format!("COLLATION '{co}' is not valid for CHARACTER SET '{cs}'"),
                ));
            }
            Ok((cs.to_owned(), co))
        }
        (Some(cs), None) => Ok((cs.to_owned(), bin_or_default(cs, spec.binary, mysql))),
        (None, Some((co, ccs))) => Ok((ccs.to_owned(), co)),
        (None, None) if spec.binary => {
            Ok((table.0.to_owned(), bin_or_default(table.0, true, mysql)))
        }
        (None, None) => Ok((table.0.to_owned(), table.1.to_owned())),
    }
}

fn bin_or_default(cs: &str, binary: bool, mysql: bool) -> String {
    if binary {
        if cs == "binary" {
            "binary".into()
        } else {
            format!("{cs}_bin")
        }
    } else {
        port_collation(cs, mysql).to_owned()
    }
}

/// Reads a type at `t[*i]`; `col` names the column in errors.
///
/// # Errors
/// 4161 for an unknown type, the length and precision errors, or a syntax
/// error (`Fail::Syntax`).
#[allow(clippy::too_many_lines)] // one arm per type spelling
pub(crate) fn read(t: &[Tok], i: &mut usize, col: &str, spec: &mut CsSpec) -> Result<Ty, Fail> {
    let Some(first) = t.get(*i) else {
        return Err(Fail::Syntax(*i));
    };
    let T::Word(w) = &first.t else {
        return Err(Fail::Syntax(*i));
    };
    let mut w = w.to_ascii_uppercase();
    *i += 1;
    // Multi-word spellings.
    let next_is = |i: usize, s: &str| t.get(i).is_some_and(|x| x.is(s));
    if w == "NATIONAL" {
        spec.national = true;
        let Some(T::Word(n)) = t.get(*i).map(|x| &x.t) else {
            return Err(Fail::Syntax(*i));
        };
        w = n.to_ascii_uppercase();
        *i += 1;
    }
    if (w == "CHAR" || w == "CHARACTER") && next_is(*i, "VARYING") {
        *i += 1;
        w = "VARCHAR".into();
    }
    if w == "DOUBLE" && next_is(*i, "PRECISION") {
        *i += 1;
    }
    if w == "LONG" {
        if next_is(*i, "VARBINARY") {
            *i += 1;
            w = "MEDIUMBLOB".into();
        } else if next_is(*i, "VARCHAR") || next_is(*i, "VARCHARACTER") {
            *i += 1;
            w = "MEDIUMTEXT".into();
        } else if next_is(*i, "BYTE") {
            *i += 1;
            w = "MEDIUMBLOB".into();
        } else {
            w = "MEDIUMTEXT".into();
        }
    }
    let args = read_args(t, i)?;
    let num = |k: usize| -> Option<u64> { args.get(k).and_then(|a| a.parse::<u64>().ok()) };
    let mut ty = Ty {
        base: String::new(),
        args: String::new(),
        unsigned: false,
        zerofill: false,
        cs: None,
        json: false,
        serial: false,
        members: Vec::new(),
        len: 0,
    };
    let int = |ty: &mut Ty, base: &str, signed_w: u64| {
        ty.base = base.into();
        ty.len = u32::try_from(signed_w).unwrap_or(u32::MAX);
    };
    match w.as_str() {
        "TINYINT" | "INT1" => int(&mut ty, "tinyint", 4),
        "BOOL" | "BOOLEAN" => {
            int(&mut ty, "tinyint", 1);
            ty.args = "(1)".into();
        }
        "SMALLINT" | "INT2" => int(&mut ty, "smallint", 6),
        "MEDIUMINT" | "INT3" | "MIDDLEINT" => int(&mut ty, "mediumint", 9),
        "INT" | "INTEGER" | "INT4" => int(&mut ty, "int", 11),
        "BIGINT" | "INT8" => int(&mut ty, "bigint", 20),
        "SERIAL" => {
            int(&mut ty, "bigint", 20);
            ty.unsigned = true;
            ty.serial = true;
        }
        "BIT" => {
            let n = if args.is_empty() {
                1
            } else {
                num(0).ok_or(Fail::Syntax(*i))?
            };
            if n > 64 {
                return Err(err(
                    1439,
                    "42000",
                    format!("Display width out of range for '{col}' (max = 64)"),
                ));
            }
            ty.base = "bit".into();
            ty.args = format!("({})", n.max(1));
        }
        "DECIMAL" | "DEC" | "NUMERIC" | "FIXED" => {
            let m = if args.is_empty() {
                10
            } else {
                num(0).ok_or(Fail::Syntax(*i))?
            };
            let d = if args.len() > 1 {
                num(1).ok_or(Fail::Syntax(*i))?
            } else {
                0
            };
            if m > 65 {
                return Err(err(
                    1426,
                    "42000",
                    format!("Too big precision specified for '{col}'. Maximum is 65"),
                ));
            }
            if d > 38 {
                return Err(err(
                    1425,
                    "42000",
                    format!("Too big scale specified for '{col}'. Maximum is 38"),
                ));
            }
            if m < d {
                return Err(err(
                    1427,
                    "42000",
                    format!(
                        "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{col}')"
                    ),
                ));
            }
            ty.base = "decimal".into();
            ty.args = format!("({},{})", m.max(1), d);
            ty.len = u32::try_from(m).unwrap_or(65);
        }
        "FLOAT" | "FLOAT4" => {
            if args.len() == 1 {
                let p = num(0).ok_or(Fail::Syntax(*i))?;
                if p > 53 {
                    return Err(err(
                        1063,
                        "42000",
                        format!("Incorrect column specifier for column '{col}'"),
                    ));
                }
                ty.base = if p > 24 { "double" } else { "float" }.into();
            } else {
                ty.base = "float".into();
                if args.len() == 2 {
                    md_args(&mut ty, &args, col, *i)?;
                }
            }
        }
        "DOUBLE" | "REAL" | "FLOAT8" => {
            ty.base = "double".into();
            if args.len() == 2 {
                md_args(&mut ty, &args, col, *i)?;
            } else if !args.is_empty() {
                return Err(Fail::Syntax(*i));
            }
        }
        "CHAR" | "CHARACTER" | "NCHAR" => {
            if w == "NCHAR" {
                spec.national = true;
            }
            let n = if args.is_empty() {
                1
            } else {
                num(0).ok_or(Fail::Syntax(*i))?
            };
            if n > 255 {
                return Err(err(
                    1074,
                    "42000",
                    format!(
                        "Column length too big for column '{col}' (max = 255); use BLOB or TEXT instead"
                    ),
                ));
            }
            ty.base = "char".into();
            ty.args = format!("({n})");
            ty.len = u32::try_from(n).unwrap_or(255);
        }
        "VARCHAR" | "VARCHARACTER" | "NVARCHAR" => {
            if w == "NVARCHAR" {
                spec.national = true;
            }
            let n = num(0).ok_or(Fail::Syntax(*i))?;
            ty.base = "varchar".into();
            ty.args = format!("({n})");
            ty.len = u32::try_from(n).unwrap_or(u32::MAX);
        }
        "BINARY" => {
            let n = if args.is_empty() {
                1
            } else {
                num(0).ok_or(Fail::Syntax(*i))?
            };
            if n > 255 {
                return Err(err(
                    1074,
                    "42000",
                    format!(
                        "Column length too big for column '{col}' (max = 255); use BLOB or TEXT instead"
                    ),
                ));
            }
            ty.base = "binary".into();
            ty.args = format!("({n})");
            ty.len = u32::try_from(n).unwrap_or(255);
        }
        "VARBINARY" => {
            let n = num(0).ok_or(Fail::Syntax(*i))?;
            if n > 65532 {
                return Err(err(
                    1074,
                    "42000",
                    format!(
                        "Column length too big for column '{col}' (max = 65532); use BLOB or TEXT instead"
                    ),
                ));
            }
            ty.base = "varbinary".into();
            ty.args = format!("({n})");
            ty.len = u32::try_from(n).unwrap_or(u32::MAX);
        }
        "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            ty.base = w.to_ascii_lowercase();
            ty.len = u32::MAX;
        }
        "TEXT" | "BLOB" => {
            // `TEXT(n)` / `BLOB(n)`: the smallest type for n bytes (text: n
            // characters).
            ty.base = w.to_ascii_lowercase();
            ty.len = u32::MAX;
            if let Some(n) = num(0) {
                let text = w == "TEXT";
                let bytes = if text {
                    n * u64::from(spec_mb(spec))
                } else {
                    n
                };
                let kind = if bytes < 256 {
                    "tiny"
                } else if bytes < 65536 {
                    ""
                } else if bytes < 16_777_216 {
                    "medium"
                } else {
                    "long"
                };
                ty.base = format!("{kind}{}", if text { "text" } else { "blob" });
            }
        }
        "JSON" => {
            ty.base = "longtext".into();
            ty.json = true;
            ty.len = u32::MAX;
        }
        "ENUM" | "SET" => {
            ty.base = w.to_ascii_lowercase();
            let members = read_members(t, i)?;
            ty.args = format!(
                "({})",
                members
                    .iter()
                    .map(|m| super::expr::quote_str(m).replace("\\'", "''"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            ty.members = members;
        }
        "DATE" => ty.base = "date".into(),
        "TIME" | "DATETIME" | "TIMESTAMP" => {
            ty.base = w.to_ascii_lowercase();
            if let Some(f) = num(0) {
                if f > 6 {
                    return Err(err(
                        1426,
                        "42000",
                        format!("Too big precision specified for '{col}'. Maximum is 6"),
                    ));
                }
                if f > 0 {
                    ty.args = format!("({f})");
                }
            } else if !args.is_empty() {
                return Err(Fail::Syntax(*i));
            }
        }
        "YEAR" => {
            ty.base = "year".into();
            let n = num(0).unwrap_or(4);
            ty.args = if n == 2 { "(2)".into() } else { "(4)".into() };
        }
        "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "UUID" | "INET4" | "INET6" | "VECTOR" => {
            ty.base = w.to_ascii_lowercase();
        }
        _ => {
            return Err(err(
                4161,
                "HY000",
                format!("Unknown data type: '{}'", first_text(first)),
            ));
        }
    }
    // Integer display widths.
    if ty.is_int()
        && let Some(wd) = num(0)
    {
        if wd > 255 {
            return Err(err(
                1439,
                "42000",
                format!("Display width out of range for '{col}' (max = 255)"),
            ));
        }
        if ty.args.is_empty() {
            ty.args = format!("({wd})");
        }
    }
    // Attributes after the type: SIGNED / UNSIGNED / ZEROFILL, the string
    // ones (BINARY, ASCII, UNICODE, BYTE, CHARACTER SET, COLLATE) are the
    // column parser's.
    loop {
        match t.get(*i) {
            Some(x) if x.is("UNSIGNED") => {
                ty.unsigned = true;
                *i += 1;
            }
            Some(x) if x.is("SIGNED") => *i += 1,
            Some(x) if x.is("ZEROFILL") => {
                ty.zerofill = true;
                ty.unsigned = true;
                *i += 1;
            }
            _ => break,
        }
    }
    if ty.is_int() && ty.args.is_empty() {
        let wd = match (ty.base.as_str(), ty.unsigned) {
            ("tinyint", false) => 4,
            ("tinyint", true) => 3,
            ("smallint", false) => 6,
            ("smallint", true) => 5,
            ("mediumint", false) => 9,
            ("mediumint", true) => 8,
            ("int", false) => 11,
            ("int", true) => 10,
            _ => 20,
        };
        ty.args = format!("({wd})");
    }
    if !ty.is_numeric() && ty.base != "bit" && (ty.unsigned || ty.zerofill) {
        return Err(Fail::Syntax(*i - 1));
    }
    Ok(ty)
}

/// Bytes per character for `TEXT(n)`, from what was said so far.
fn spec_mb(spec: &CsSpec) -> u32 {
    if spec.national { 3 } else { 4 }
}

fn first_text(t: &Tok) -> String {
    match &t.t {
        T::Word(w) => w.clone(),
        _ => String::new(),
    }
}

fn md_args(ty: &mut Ty, args: &[String], col: &str, at: usize) -> Result<(), Fail> {
    let m: u64 = args[0].parse().map_err(|_| Fail::Syntax(at))?;
    let d: u64 = args[1].parse().map_err(|_| Fail::Syntax(at))?;
    if m > 255 {
        return Err(err(
            1439,
            "42000",
            format!("Display width out of range for '{col}' (max = 255)"),
        ));
    }
    if d > 30 {
        return Err(err(
            1425,
            "42000",
            format!("Too big scale specified for '{col}'. Maximum is 30"),
        ));
    }
    if m < d {
        return Err(err(
            1427,
            "42000",
            format!("For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '{col}')"),
        ));
    }
    ty.args = format!("({m},{d})");
    Ok(())
}

/// `(n)` or `(m,d)` after a type: the numbers as written.
fn read_args(t: &[Tok], i: &mut usize) -> Result<Vec<String>, Fail> {
    if !t.get(*i).is_some_and(|x| x.op("(")) {
        return Ok(Vec::new());
    }
    // ENUM / SET members are strings: leave them to `read_members`.
    if matches!(t.get(*i + 1).map(|x| &x.t), Some(T::Str(_))) {
        return Ok(Vec::new());
    }
    *i += 1;
    let mut v = Vec::new();
    loop {
        match t.get(*i).map(|x| &x.t) {
            Some(T::Num(n)) if n.bytes().all(|b| b.is_ascii_digit()) => {
                v.push(n.clone());
                *i += 1;
            }
            _ => return Err(Fail::Syntax(*i)),
        }
        match t.get(*i) {
            Some(x) if x.op(",") => *i += 1,
            Some(x) if x.op(")") => {
                *i += 1;
                return Ok(v);
            }
            _ => return Err(Fail::Syntax(*i)),
        }
    }
}

fn read_members(t: &[Tok], i: &mut usize) -> Result<Vec<String>, Fail> {
    if !t.get(*i).is_some_and(|x| x.op("(")) {
        return Err(Fail::Syntax(*i));
    }
    *i += 1;
    let mut v = Vec::new();
    loop {
        match t.get(*i).map(|x| &x.t) {
            Some(T::Str(s)) => {
                v.push(s.clone());
                *i += 1;
            }
            _ => return Err(Fail::Syntax(*i)),
        }
        match t.get(*i) {
            Some(x) if x.op(",") => *i += 1,
            Some(x) if x.op(")") => {
                *i += 1;
                return Ok(v);
            }
            _ => return Err(Fail::Syntax(*i)),
        }
    }
}
