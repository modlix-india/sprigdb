//! MariaDB's sequence functions: `NEXTVAL(s)` (`NEXT VALUE FOR s`),
//! `LASTVAL(s)` (`PREVIOUS VALUE FOR s`) and `SETVAL(s, n [, used
//! [, round]])`, plus the internal `SPRIG_SEQUENCE_STATE(s, field)` that a
//! read of the sequence as a table uses.
//!
//! The planner resolves the sequence's name and passes the sequence as a
//! keyword argument (its identity, as text); the values come from the
//! session's [`Sequences`] handle, which the host fills
//! ([`super::misc::SessionInfo::sequences`]).

use sprig_engine::row::Value;

use crate::conv::to_int;
use crate::error::{Result, SqlError};
use crate::expr::{Env, Expr, Node};
use crate::types::ExprType;

/// Where a statement's sequence functions get their values: the host's
/// sequences, as one session sees them.
pub trait Sequences: Send + Sync + std::fmt::Debug {
    /// The next value of sequence `seq` (`NEXTVAL`); it becomes the
    /// session's last value of the sequence.
    ///
    /// # Errors
    /// The sequence has run out, or its values couldn't be reserved.
    fn next(&self, seq: &str) -> Result<i64>;

    /// The session's last value of `seq` (`LASTVAL`); `None` before its
    /// first `NEXTVAL`.
    ///
    /// # Errors
    /// The sequence is gone.
    fn last(&self, seq: &str) -> Result<Option<i64>>;

    /// `SETVAL(seq, n, used, round)`: `Some(n)` when the sequence moved,
    /// `None` when `n` is behind its next value.
    ///
    /// # Errors
    /// The value couldn't be stored.
    fn set(&self, seq: &str, n: i64, used: bool, round: u64) -> Result<Option<i64>>;

    /// The stored state of `seq` as its table shows it: field 0 is the next
    /// value not cached, field 1 the number of cycles done.
    ///
    /// # Errors
    /// The state couldn't be read.
    fn state(&self, seq: &str, field: u8) -> Result<i64>;
}

/// The names of the sequence functions.
pub(crate) const NAMES: &[&str] = &["NEXTVAL", "LASTVAL", "SETVAL", "SPRIG_SEQUENCE_STATE"];

/// No handle: the statement runs where sequences aren't served.
fn unserved() -> SqlError {
    SqlError::unsupported("sequences here")
}

/// Compiles sequence function `name`: `args[0]` is the sequence's keyword
/// (its identity), the rest its value arguments.
pub(crate) fn build(name: &str, args: &[Expr], nodes: Vec<Node>, _env: Env<'_>) -> Result<Node> {
    let Some(Expr::Keyword(seq)) = args.first() else {
        return Err(SqlError::param_count(name));
    };
    let seq: std::sync::Arc<str> = seq.as_str().into();
    let lower = name.to_ascii_lowercase();
    // MariaDB's type for each: `BIGINT(20)`, nullable.
    let mut ty = ExprType::int(20, false);
    ty.nullable = true;
    match name {
        "NEXTVAL" => {
            if !nodes.is_empty() {
                return Err(SqlError::param_count(&lower));
            }
            Ok(Node::new(ty, "nextval()", move |ctx, _| {
                let s = ctx.session.info.sequences.clone().ok_or_else(unserved)?;
                Ok(Value::Int(s.next(&seq)?))
            }))
        }
        "LASTVAL" => {
            if !nodes.is_empty() {
                return Err(SqlError::param_count(&lower));
            }
            ty.nullable = true;
            Ok(Node::new(ty, "lastval()", move |ctx, _| {
                let s = ctx.session.info.sequences.clone().ok_or_else(unserved)?;
                Ok(s.last(&seq)?.map_or(Value::Null, Value::Int))
            }))
        }
        "SETVAL" => {
            if nodes.is_empty() || nodes.len() > 3 {
                return Err(SqlError::param_count(&lower));
            }
            ty.nullable = true;
            Ok(Node::new(ty, "setval()", move |ctx, row| {
                let mut vals = Vec::with_capacity(nodes.len());
                for n in &nodes {
                    let v = n.eval(ctx, row)?;
                    vals.push(to_int(ctx, &v));
                }
                let Some(n) = vals[0] else {
                    return Ok(Value::Null);
                };
                let used = vals.get(1).copied().flatten().is_none_or(|u| u.v != 0);
                let round = vals
                    .get(2)
                    .copied()
                    .flatten()
                    .map_or(0, |r| u64::try_from(r.v).unwrap_or(0));
                let s = ctx.session.info.sequences.clone().ok_or_else(unserved)?;
                Ok(s.set(&seq, n.v, used, round)?
                    .map_or(Value::Null, Value::Int))
            }))
        }
        _ => {
            let field = match args.get(1) {
                Some(Expr::Int(f)) => f.parse::<u8>().unwrap_or(0),
                _ => 0,
            };
            Ok(Node::new(ty, "sprig_sequence_state()", move |ctx, _| {
                let s = ctx.session.info.sequences.clone().ok_or_else(unserved)?;
                Ok(Value::Int(s.state(&seq, field)?))
            }))
        }
    }
}
