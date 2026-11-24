//! Built-in functions: the registry that maps a name and its compiled
//! arguments to a typed closure.

mod datetime;
mod hash;
mod json;
pub(crate) mod misc;
mod numeric;
mod regexp;
pub(crate) mod sequence;
mod string;
mod strtail;

pub(crate) use string::latin1_as_utf8;

use super::expr::{Env, Expr, Node, build as build_expr, write_list};
use crate::error::{Result, SqlError};
use crate::expr::control;
use sprig_engine::keys::collation::Collation;

fn arity(name: &str, args: &[Node], min: usize, max: usize) -> Result<()> {
    if args.len() < min || args.len() > max {
        return Err(SqlError::param_count(name));
    }
    Ok(())
}

/// Takes the arguments out of the vector as a fixed-size array.
fn take<const N: usize>(name: &str, args: Vec<Node>) -> Result<[Node; N]> {
    <[Node; N]>::try_from(args).map_err(|_| SqlError::param_count(name))
}

/// Compiles the call of built-in function `name`.
pub(crate) fn build(name: &str, args: &[Expr], env: Env<'_>) -> Result<Node> {
    if sequence::NAMES.contains(&name) {
        let nodes = args
            .iter()
            .skip(1)
            .filter(|a| !matches!(a, Expr::Keyword(_)))
            .map(|a| build_expr(a, env))
            .collect::<Result<Vec<_>>>()?;
        let nodes = if name == "SPRIG_SEQUENCE_STATE" {
            Vec::new()
        } else {
            nodes
        };
        return sequence::build(name, args, nodes, env);
    }
    let nodes = args
        .iter()
        .filter(|a| !matches!(a, Expr::Keyword(_)))
        .map(|a| build_expr(a, env))
        .collect::<Result<Vec<_>>>()?;
    match name {
        "IF" => {
            let [c, a, b] = take(name, nodes)?;
            control::if_(c, a, b, env)
        }
        "IFNULL" => {
            let [a, b] = take(name, nodes)?;
            control::ifnull(a, b, env)
        }
        "NULLIF" => {
            let [a, b] = take(name, nodes)?;
            control::nullif(a, b, env)
        }
        "COALESCE" => {
            arity(name, &nodes, 1, usize::MAX)?;
            control::coalesce(nodes, env)
        }
        "GREATEST" | "LEAST" => {
            arity(name, &nodes, 2, usize::MAX)?;
            control::extreme(nodes, name == "GREATEST", env)
        }
        "ISNULL" => {
            let [a] = take(name, nodes)?;
            crate::expr::is_null(a, false, env)
        }
        "CONCAT" => {
            arity(name, &nodes, 1, usize::MAX)?;
            string::concat(nodes, env)
        }
        "CONCAT_WS" => {
            arity(name, &nodes, 2, usize::MAX)?;
            string::concat_ws(nodes, env)
        }
        "LENGTH" | "OCTET_LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "BIT_LENGTH" | "ASCII"
        | "ORD" => {
            let [a] = take(name, nodes)?;
            Ok(string::measure_fn(name, a, env))
        }
        "UPPER" | "UCASE" | "LOWER" | "LCASE" | "REVERSE" | "LTRIM" | "RTRIM" | "TRIM" => {
            let [a] = take(name, nodes)?;
            Ok(string::map_fn(name, a, env))
        }
        "TRIM_BOTH" | "TRIM_LEADING" | "TRIM_TRAILING" => {
            let [rem, s] = take(name, nodes)?;
            string::trim_remove(rem, s, name != "TRIM_TRAILING", name != "TRIM_LEADING", env)
        }
        "SUBSTRING" | "SUBSTR" | "MID" => {
            arity(name, &nodes, 2, 3)?;
            string::substring(name, nodes, env)
        }
        "LEFT" | "RIGHT" => {
            arity(name, &nodes, 2, 2)?;
            string::substring(name, nodes, env)
        }
        "REPLACE" => {
            let [s, from, to] = take(name, nodes)?;
            string::replace(s, from, to, env)
        }
        "LPAD" | "RPAD" => {
            let [s, n, p] = take(name, nodes)?;
            string::pad(if name == "LPAD" { "lpad" } else { "rpad" }, s, n, p, env)
        }
        "LOCATE" => {
            arity(name, &nodes, 2, 3)?;
            let mut it = nodes.into_iter();
            let (sub, s, pos) = (it.next(), it.next(), it.next());
            match (sub, s) {
                (Some(sub), Some(s)) => string::locate(sub, s, pos, env),
                _ => Err(SqlError::param_count(name)),
            }
        }
        "INSTR" => {
            let [s, sub] = take(name, nodes)?;
            string::locate(sub, s, None, env)
        }
        "REPEAT" => {
            let [s, n] = take(name, nodes)?;
            string::repeat(s, n, "repeat", env)
        }
        "SPACE" => {
            let [n] = take(name, nodes)?;
            let space = build_expr(&Expr::str(" "), env)?;
            string::repeat(space, n, "space", env)
        }
        "STRCMP" => {
            let [a, b] = take(name, nodes)?;
            string::strcmp(a, b, env)
        }
        "COLLATION" | "CHARSET" | "COERCIBILITY" => {
            let [a] = take(name, nodes)?;
            Ok(string::describe(name, a, env))
        }
        "HEX" => {
            let [a] = take(name, nodes)?;
            Ok(string::hex(a, env))
        }
        "ABS" => {
            let [a] = take(name, nodes)?;
            Ok(numeric::abs(a, env))
        }
        "SIGN" => {
            let [a] = take(name, nodes)?;
            Ok(numeric::sign(a, env))
        }
        "CEIL" | "CEILING" | "FLOOR" => {
            let [a] = take(name, nodes)?;
            Ok(numeric::ceil_floor(a, name != "FLOOR", env))
        }
        "ROUND" => {
            arity(name, &nodes, 1, 2)?;
            let mut it = nodes.into_iter();
            match it.next() {
                Some(a) => numeric::round(a, it.next(), false, env),
                None => Err(SqlError::param_count(name)),
            }
        }
        "TRUNCATE" => {
            let [a, d] = take(name, nodes)?;
            numeric::round(a, Some(d), true, env)
        }
        "MOD" => {
            let [a, b] = take(name, nodes)?;
            Ok(crate::expr::arith::binary(crate::BinOp::Mod, a, b, env))
        }
        "POW" | "POWER" => {
            arity(name, &nodes, 2, 2)?;
            Ok(numeric::real(name, nodes, env))
        }
        "SQRT" | "EXP" | "LN" | "LOG2" | "LOG10" => {
            arity(name, &nodes, 1, 1)?;
            Ok(numeric::real(name, nodes, env))
        }
        "LOG" => {
            arity(name, &nodes, 1, 2)?;
            Ok(numeric::real(name, nodes, env))
        }
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" | "CURDATE"
        | "CURRENT_DATE" | "CURTIME" | "CURRENT_TIME" | "UTC_TIMESTAMP" => {
            arity(name, &nodes, 0, 1)?;
            datetime::current(name, &nodes, env)
        }
        "YEAR" | "MONTH" | "DAY" | "DAYOFMONTH" | "QUARTER" | "HOUR" | "MINUTE" | "SECOND"
        | "MICROSECOND" | "DAYOFWEEK" | "WEEKDAY" | "DAYOFYEAR" | "TO_DAYS" | "WEEKOFYEAR"
            if nodes.len() == 1 =>
        {
            let [a] = take(name, nodes)?;
            datetime::part(name, a, env)
        }
        "DATE" | "TIME" | "LAST_DAY" => {
            let [a] = take(name, nodes)?;
            datetime::convert(name, a, env)
        }
        "WEEK" | "YEARWEEK" => {
            arity(name, &nodes, 1, 2)?;
            let mut it = nodes.into_iter();
            match it.next() {
                Some(d) => Ok(datetime::week_fn(name, d, it.next(), env)),
                None => Err(SqlError::param_count(name)),
            }
        }
        "EXTRACT" => {
            let unit = unit_of(args)?;
            let [d] = take(name, nodes)?;
            Ok(datetime::extract(unit, d, env))
        }
        "TIMESTAMPDIFF" => {
            let unit = unit_of(args)?;
            let [a, b] = take(name, nodes)?;
            Ok(datetime::timestampdiff(unit, a, b))
        }
        "TIMESTAMPADD" => {
            let unit = unit_of(args)?;
            let [n, d] = take(name, nodes)?;
            datetime::date_add(d, n, unit, false, env)
        }
        "CONVERT_TZ" => {
            let [d, from, to] = take(name, nodes)?;
            Ok(datetime::convert_tz(d, from, to, env))
        }
        "STR_TO_DATE" => {
            let [sv, f] = take(name, nodes)?;
            datetime::str_to_date(sv, f, env)
        }
        "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => {
            let unit = args
                .iter()
                .find_map(|a| match a {
                    Expr::Keyword(k) => datetime::Unit::parse(k),
                    _ => None,
                })
                .unwrap_or(datetime::Unit::Day);
            let [d, n] = take(name, nodes)?;
            datetime::date_add(
                d,
                n,
                unit,
                name.starts_with("DATE_SUB") || name == "SUBDATE",
                env,
            )
        }
        "UUID_TO_BIN" | "BIN_TO_UUID" if env.maria() => Err(SqlError::no_function(name)),
        "IS_UUID" if env.maria() => Err(SqlError::no_function(name)),
        "UUID_TO_BIN" | "BIN_TO_UUID" => {
            arity(name, &nodes, 1, 2)?;
            let mut it = nodes.into_iter();
            let (Some(a), swap) = (it.next(), it.next()) else {
                return Err(SqlError::param_count(name));
            };
            Ok(if name == "UUID_TO_BIN" {
                misc::uuid_to_bin(a, swap, env)
            } else {
                misc::bin_to_uuid(a, swap, env)
            })
        }
        "IS_UUID" => {
            let [a] = take(name, nodes)?;
            Ok(misc::is_uuid(a, env))
        }
        "TO_BASE64" => {
            let [a] = take(name, nodes)?;
            Ok(misc::to_base64(a, env))
        }
        "FROM_BASE64" => {
            let [a] = take(name, nodes)?;
            Ok(misc::from_base64(a, env))
        }
        "INET_ATON" => {
            let [a] = take(name, nodes)?;
            Ok(misc::inet_aton(a, env))
        }
        "INET6_ATON" => {
            let [a] = take(name, nodes)?;
            Ok(misc::inet6_aton(a, env))
        }
        "INET6_NTOA" => {
            let [a] = take(name, nodes)?;
            Ok(misc::inet6_ntoa(a, env))
        }
        "INET_NTOA" => {
            let [a] = take(name, nodes)?;
            Ok(misc::inet_ntoa(a, env))
        }
        "BIT_COUNT" => {
            let [a] = take(name, nodes)?;
            Ok(misc::bit_count(a, env))
        }
        "FROM_DAYS" => {
            let [a] = take(name, nodes)?;
            Ok(misc::from_days(a))
        }
        "MAKEDATE" => {
            let [y, d] = take(name, nodes)?;
            Ok(misc::makedate(y, d))
        }
        "SEC_TO_TIME" => {
            let [a] = take(name, nodes)?;
            Ok(misc::sec_to_time(a))
        }
        "UUID" => {
            arity(name, &nodes, 0, 0)?;
            Ok(misc::uuid(env))
        }
        "JSON_EXTRACT" => json::extract(nodes, env),
        "JSON_UNQUOTE" => {
            let [a] = take(name, nodes)?;
            Ok(json::unquote(a, env))
        }
        "JSON_VALUE" => {
            let [d, p] = take(name, nodes)?;
            Ok(json::value(d, p, env))
        }
        "JSON_TYPE" => {
            let [a] = take(name, nodes)?;
            Ok(json::type_of(a, env))
        }
        "JSON_VALID" => {
            let [a] = take(name, nodes)?;
            Ok(json::valid(a, env))
        }
        "JSON_LENGTH" | "JSON_KEYS" => json::length_or_keys(name == "JSON_KEYS", nodes, env),
        "JSON_OBJECT" | "JSON_ARRAY" => json::build(name == "JSON_OBJECT", nodes, env),
        "JSON_QUOTE" => {
            let [a] = take(name, nodes)?;
            Ok(json::quote(a, env))
        }
        "JSON_CONTAINS" => json::contains_fn(nodes, env),
        "JSON_SET" => json::modify(json::Modify::Set, nodes, env),
        "JSON_INSERT" => json::modify(json::Modify::Insert, nodes, env),
        "JSON_REPLACE" => json::modify(json::Modify::Replace, nodes, env),
        "JSON_REMOVE" => json::remove_fn(nodes, env),
        "JSON_ARRAY_APPEND" => json::array_append(nodes, env),
        "JSON_MERGE_PATCH" => json::merge_patch_fn(nodes, env),
        "MEMBER OF" if !env.maria() => {
            let [x, d] = take(name, nodes)?;
            Ok(json::member_of(x, d, env))
        }
        "TIME_TO_SEC" => {
            let [a] = take(name, nodes)?;
            Ok(misc::time_to_sec(a, env))
        }
        "MAKETIME" => {
            let [h, m, sec] = take(name, nodes)?;
            Ok(misc::maketime(h, m, sec))
        }
        "TIMEDIFF" => {
            let [a, b] = take(name, nodes)?;
            Ok(misc::timediff(a, b))
        }
        "SOUNDEX" => {
            let [a] = take(name, nodes)?;
            Ok(misc::soundex(a, env))
        }
        "TIME_FORMAT" => {
            let [d, f] = take(name, nodes)?;
            Ok(datetime::time_format(d, f, env))
        }
        "DATABASE" | "SCHEMA" | "CONNECTION_ID" | "LAST_INSERT_ID" | "USER" | "SESSION_USER"
        | "SYSTEM_USER" | "CURRENT_USER" | "VERSION" => {
            if !nodes.is_empty() {
                return Err(SqlError::unsupported(&format!("{name} with an argument")));
            }
            misc::session_value(name, env)
        }
        "DATEDIFF" => {
            let [a, b] = take(name, nodes)?;
            Ok(datetime::datediff(a, b, env))
        }
        "DATE_FORMAT" => {
            let [d, f] = take(name, nodes)?;
            Ok(datetime::date_format(d, f, env))
        }
        "UNIX_TIMESTAMP" => {
            arity(name, &nodes, 0, 1)?;
            Ok(datetime::unix_timestamp(nodes.into_iter().next(), env))
        }
        "FROM_UNIXTIME" => {
            arity(name, &nodes, 1, 2)?;
            let mut it = nodes.into_iter();
            match it.next() {
                Some(n) => Ok(datetime::from_unixtime(n, it.next(), env)),
                None => Err(SqlError::param_count(name)),
            }
        }
        "DAYNAME" | "MONTHNAME" => {
            let [a] = take(name, nodes)?;
            Ok(datetime::name_of(name, a, env))
        }
        "SUBSTRING_INDEX" => {
            let [s, d, n] = take(name, nodes)?;
            strtail::substring_index(s, d, n, env)
        }
        "INSERT" => {
            let [s, p, l, n] = take(name, nodes)?;
            strtail::insert(s, p, l, n, env)
        }
        "FIELD" => {
            arity(name, &nodes, 2, usize::MAX)?;
            strtail::field(nodes, env)
        }
        "ELT" => {
            arity(name, &nodes, 2, usize::MAX)?;
            strtail::elt(nodes, env)
        }
        "FIND_IN_SET" => {
            let [s, l] = take(name, nodes)?;
            strtail::find_in_set(s, l, env)
        }
        "FORMAT" => {
            arity(name, &nodes, 2, 3)?;
            Ok(strtail::format(nodes, env))
        }
        "QUOTE" => {
            let [a] = take(name, nodes)?;
            Ok(strtail::quote(a, env))
        }
        "UNHEX" => {
            let [a] = take(name, nodes)?;
            Ok(strtail::unhex(a, env))
        }
        "BIN" | "OCT" => {
            let [a] = take(name, nodes)?;
            let to = if name == "BIN" { 2 } else { 8 };
            Ok(strtail::conv_fn(a, None, None, Some((10, to)), env))
        }
        "CONV" => {
            let [a, f, t] = take(name, nodes)?;
            Ok(strtail::conv_fn(a, Some(f), Some(t), None, env))
        }
        "CHAR" => {
            arity(name, &nodes, 1, usize::MAX)?;
            let using = match args.iter().find_map(|a| match a {
                Expr::Keyword(k) => k.strip_prefix("USING "),
                _ => None,
            }) {
                Some(cs) => Some(charset_collation(cs, env)?),
                None => None,
            };
            Ok(strtail::char_fn(nodes, using, env))
        }
        "MD5" => {
            let [a] = take(name, nodes)?;
            Ok(strtail::digest("md5", a, env))
        }
        "SHA1" | "SHA" => {
            let [a] = take(name, nodes)?;
            Ok(strtail::digest("sha1", a, env))
        }
        "SHA2" => {
            let [a, b] = take(name, nodes)?;
            strtail::sha2(a, b, env)
        }
        "CRC32" => {
            let [a] = take(name, nodes)?;
            Ok(strtail::crc32(a, env))
        }
        "INTERVAL" => {
            arity(name, &nodes, 2, usize::MAX)?;
            Ok(strtail::interval(nodes, env))
        }
        "SIN" | "COS" | "TAN" | "COT" | "ASIN" | "ACOS" => {
            arity(name, &nodes, 1, 1)?;
            Ok(numeric::trig(name, nodes))
        }
        "ATAN" => {
            arity(name, &nodes, 1, 2)?;
            Ok(numeric::trig(name, nodes))
        }
        "ATAN2" => {
            arity(name, &nodes, 2, 2)?;
            Ok(numeric::trig(name, nodes))
        }
        "DEGREES" | "RADIANS" => {
            let [a] = take(name, nodes)?;
            Ok(numeric::degrees(name, a))
        }
        "PI" => {
            arity(name, &nodes, 0, 0)?;
            Ok(numeric::pi())
        }
        "RAND" => {
            arity(name, &nodes, 0, 1)?;
            Ok(numeric::rand(nodes.into_iter().next()))
        }
        "ROW" => Err(crate::expr::row::operand_columns(1)),
        "CONVERT_USING" => {
            let cs = keyword(args).ok_or_else(|| SqlError::param_count(name))?;
            let coll = charset_collation(cs, env)?;
            let [a] = take(name, nodes)?;
            Ok(strtail::convert_using(a, coll, env))
        }
        "_CHARSET" => {
            let cs = keyword(args).ok_or_else(|| SqlError::param_count(name))?;
            let coll = charset_collation(cs, env)?;
            let mut text = String::new();
            write_sql(&mut text, name, args);
            match args.iter().find(|a| !matches!(a, Expr::Keyword(_))) {
                Some(Expr::Str(s)) => Ok(strtail::introducer(s.as_bytes(), text, coll)),
                Some(Expr::Hex(b)) => Ok(strtail::introducer(b, text, coll)),
                _ => Err(SqlError::new(
                    1064,
                    "42000",
                    "You have an error in your SQL syntax",
                )),
            }
        }
        "REGEXP" => regexp::like(nodes, name, env),
        // MariaDB has no REGEXP_LIKE.
        "REGEXP_LIKE" if !env.maria() => regexp::like(nodes, name, env),
        "REGEXP_INSTR" => regexp::instr(nodes, env),
        "REGEXP_SUBSTR" => regexp::substr(nodes, env),
        "REGEXP_REPLACE" => regexp::replace(nodes, env),
        _ => Err(SqlError::no_function(name)),
    }
}

/// The first keyword argument of a call.
fn keyword(args: &[Expr]) -> Option<&str> {
    args.iter().find_map(|a| match a {
        Expr::Keyword(k) => Some(k.as_str()),
        _ => None,
    })
}

/// The default collation of a character set named in `USING` or an
/// introducer, as the persona sets it.
pub(crate) fn charset_collation(name: &str, env: Env<'_>) -> Result<Collation> {
    use sprig_engine::keys::collation::Collation as C;
    Ok(match name.to_ascii_lowercase().as_str() {
        "utf8mb4" | "utf8mb3" | "utf8" => {
            if env.maria() {
                C::Utf8mb4Uca1400AiCi
            } else {
                C::Utf8mb4Mysql0900AiCi
            }
        }
        "latin1" => C::Latin1SwedishCi,
        "ascii" => C::AsciiGeneralCi,
        "binary" => C::Binary,
        other => {
            return Err(SqlError::new(
                1115,
                "42000",
                format!("Unknown character set: '{other}'"),
            ));
        }
    })
}

/// The interval unit among a call's arguments.
fn unit_of(args: &[Expr]) -> Result<datetime::Unit> {
    args.iter()
        .find_map(|a| match a {
            Expr::Keyword(k) => datetime::Unit::parse(k),
            _ => None,
        })
        .ok_or_else(|| SqlError::new(1064, "42000", "You have an error in your SQL syntax"))
}

/// Writes a function call as SQL text.
pub(crate) fn write_sql(out: &mut String, name: &str, args: &[Expr]) {
    if let ("REGEXP", [s, p]) = (name, args) {
        out.push('(');
        out.push_str(&s.to_sql());
        out.push_str(" REGEXP ");
        out.push_str(&p.to_sql());
        out.push(')');
        return;
    }
    if let ("TRIM_BOTH" | "TRIM_LEADING" | "TRIM_TRAILING", [rem, s]) = (name, args) {
        out.push_str("TRIM(");
        out.push_str(&name[5..]);
        out.push(' ');
        out.push_str(&rem.to_sql());
        out.push_str(" FROM ");
        out.push_str(&s.to_sql());
        out.push(')');
        return;
    }
    if let ("EXTRACT", [Expr::Keyword(unit), d]) = (name, args) {
        out.push_str("EXTRACT(");
        out.push_str(unit);
        out.push_str(" FROM ");
        out.push_str(&d.to_sql());
        out.push(')');
        return;
    }
    if let ("DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE", [d, n, Expr::Keyword(unit)]) =
        (name, args)
    {
        out.push_str(name);
        out.push('(');
        out.push_str(&d.to_sql());
        out.push_str(", INTERVAL ");
        out.push_str(&n.to_sql());
        out.push(' ');
        out.push_str(unit);
        out.push(')');
        return;
    }
    if let ("CONVERT_USING", [e, Expr::Keyword(cs)]) = (name, args) {
        out.push_str("CONVERT(");
        out.push_str(&e.to_sql());
        out.push_str(" USING ");
        out.push_str(cs);
        out.push(')');
        return;
    }
    if let ("_CHARSET", [Expr::Keyword(cs), lit]) = (name, args) {
        out.push('_');
        out.push_str(cs);
        if matches!(lit, Expr::Hex(_)) {
            out.push(' ');
        }
        out.push_str(&lit.to_sql());
        return;
    }
    if let ("CHAR", [rest @ .., Expr::Keyword(using)]) = (name, args) {
        out.push_str("CHAR(");
        write_list(out, rest);
        out.push(' ');
        out.push_str(using);
        out.push(')');
        return;
    }
    out.push_str(name);
    out.push('(');
    write_list(out, args);
    out.push(')');
}
