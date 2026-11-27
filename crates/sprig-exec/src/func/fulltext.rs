//! `MATCH (columns) AGAINST (text [modifier])`, which the planner binds as
//! `SPRIG_FT_MATCH(spec, text, primary key columns...)`.
//!
//! `spec` is a keyword naming the table, the columns and the modifier
//! ([`FtSpec`]). The search itself is the host's ([`FullText`], filled into
//! [`super::misc::SessionInfo::fulltext`]): it runs once per statement and
//! answers each row's score from the row's primary key. The value is a
//! nullable `DOUBLE`, 0 for a row the search doesn't find, as both servers
//! report it.

use bytes::Bytes;
use sprig_engine::row::Value;

use crate::error::{Result, SqlError};
use crate::expr::{Env, Expr, Node};
use crate::types::ExprType;

/// The function's name.
pub(crate) const NAME: &str = "SPRIG_FT_MATCH";

/// `AGAINST`'s modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FtMode {
    /// `IN NATURAL LANGUAGE MODE` (the default).
    Natural,
    /// `IN BOOLEAN MODE`.
    Boolean,
    /// `WITH QUERY EXPANSION` (after natural language mode).
    Expansion,
}

/// What one `MATCH` names: its table and columns, and the modifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FtSpec {
    /// The table's database.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The columns, as named (any order).
    pub columns: Vec<String>,
    /// The modifier.
    pub mode: FtMode,
}

const SEP: char = '\u{1f}';

impl FtSpec {
    /// The function the planner binds `MATCH` to.
    pub const FUNCTION: &'static str = NAME;

    /// The keyword the planner passes.
    #[must_use]
    pub fn encode(&self) -> String {
        let m = match self.mode {
            FtMode::Natural => "N",
            FtMode::Boolean => "B",
            FtMode::Expansion => "E",
        };
        format!(
            "{}{SEP}{}{SEP}{}{SEP}{m}",
            self.schema,
            self.table,
            self.columns.join(",")
        )
    }

    /// The spec of a `SPRIG_FT_MATCH` call's arguments.
    #[must_use]
    pub fn of_call(args: &[Expr]) -> Option<Self> {
        spec_of(args)
    }

    /// The spec of a keyword; `None` when it isn't one.
    #[must_use]
    pub fn decode(s: &str) -> Option<Self> {
        let mut it = s.split(SEP);
        let schema = it.next()?.to_owned();
        let table = it.next()?.to_owned();
        let columns = it.next()?.split(',').map(str::to_owned).collect();
        let mode = match it.next()? {
            "N" => FtMode::Natural,
            "B" => FtMode::Boolean,
            "E" => FtMode::Expansion,
            _ => return None,
        };
        it.next().is_none().then_some(Self {
            schema,
            table,
            columns,
            mode,
        })
    }
}

/// Where `MATCH` gets its answers: the host's full-text indexes, as one
/// statement sees them.
pub trait FullText: Send + Sync + std::fmt::Debug {
    /// Checks that a `FULLTEXT` index covers exactly the columns of `spec`
    /// (1191 otherwise), when the statement is compiled.
    ///
    /// # Errors
    /// 1191, or the table isn't one the host serves.
    fn check(&self, spec: &FtSpec) -> Result<()>;

    /// The score of the row whose primary key values are `pk` (key order)
    /// for `query`; 0 when the search doesn't find it.
    ///
    /// # Errors
    /// The search failed (a syntax error of boolean mode is 1064).
    fn score(&self, spec: &FtSpec, query: &[u8], pk: &[Value]) -> Result<f64>;

    /// The rows the search finds: the index's name, and each row's primary
    /// key values (key order), best first, as `WHERE MATCH` returns them.
    /// `None` when the host can't give them (the planner then reads the
    /// table).
    ///
    /// # Errors
    /// The search failed.
    fn hits(&self, spec: &FtSpec, query: &[u8]) -> Result<Option<(String, Vec<Vec<Value>>)>>;
}

/// The spec of a call's first argument.
#[must_use]
pub(crate) fn spec_of(args: &[Expr]) -> Option<FtSpec> {
    match args.first() {
        Some(Expr::Keyword(k)) => FtSpec::decode(k),
        _ => None,
    }
}

fn unserved() -> SqlError {
    SqlError::unsupported("MATCH ... AGAINST here")
}

/// The search text of a value: `None` for `NULL`.
fn text_of(v: &Value, ty: &ExprType, persona: crate::Persona) -> Option<Bytes> {
    match v {
        Value::Null => None,
        Value::Str(b) | Value::Bytes(b) => Some(b.clone()),
        other => crate::value_text(other, ty, persona),
    }
}

/// Compiles `SPRIG_FT_MATCH(spec, text, pk...)`: `nodes` are the compiled
/// text and key columns.
pub(crate) fn build(args: &[Expr], nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let spec = spec_of(args).ok_or_else(|| SqlError::param_count("match"))?;
    let mut nodes = nodes.into_iter();
    let query = nodes.next().ok_or_else(|| SqlError::param_count("match"))?;
    let pk: Vec<Node> = nodes.collect();
    if let Some(ft) = env.s.info.fulltext.as_ref() {
        ft.check(&spec)?;
    }
    let qty = query.ty;
    let ty = ExprType::double().nullable();
    Ok(Node::new(ty, "match()", move |ctx, row| {
        let ft = ctx.session.info.fulltext.clone().ok_or_else(unserved)?;
        let q = query.eval(ctx, row)?;
        let Some(q) = text_of(&q, &qty, ctx.persona()) else {
            return Ok(Value::Double(0.0));
        };
        let mut key = Vec::with_capacity(pk.len());
        for n in &pk {
            key.push(n.eval(ctx, row)?);
        }
        Ok(Value::Double(ft.score(&spec, &q, &key)?))
    }))
}

/// `MATCH (...) AGAINST (...)` as SQL text.
pub(crate) fn write_sql(out: &mut String, args: &[Expr]) {
    let Some(spec) = spec_of(args) else {
        out.push_str("match()");
        return;
    };
    out.push_str("(match ");
    out.push_str(&spec.columns.join(","));
    out.push_str(" against (");
    if let Some(q) = args.get(1) {
        out.push_str(&q.to_sql());
    }
    out.push_str(match spec.mode {
        FtMode::Natural => "",
        FtMode::Boolean => " in boolean mode",
        FtMode::Expansion => " with query expansion",
    });
    out.push_str("))");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_round_trip() {
        let s = FtSpec {
            schema: "db".into(),
            table: "t".into(),
            columns: vec!["a".into(), "b".into()],
            mode: FtMode::Boolean,
        };
        assert_eq!(FtSpec::decode(&s.encode()), Some(s));
        assert_eq!(FtSpec::decode("x"), None);
    }
}
