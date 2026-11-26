//! User variables read and assigned inside a statement: `@v` and
//! `@v := e`.
//!
//! The planner turns `@v` into `SPRIG_USER_VAR(v)` and `@v := e` into
//! `SPRIG_SET_USER_VAR(v, e)` (the name as a keyword argument). The values
//! come from the session's [`UserVars`] handle, which the host fills for
//! the statement ([`super::misc::SessionInfo::user_vars`]). As on both
//! servers, a read has the type the variable had when the statement was
//! planned: a value assigned since is converted to it (`SELECT @r := 'x',
//! @r` reads 0 when `@r` held a number). An assignment's value and type are
//! its expression's; MySQL warns once per statement (1287) that assigning
//! inside an expression is deprecated.

use std::sync::Arc;

use sprig_engine::row::Value;

use crate::error::{Result, SqlError};
use crate::expr::{Env, Expr, Node, coerce_fn};
use crate::types::{ExprType, Kind};

/// The session's user variables as one statement sees them.
pub trait UserVars: Send + Sync + std::fmt::Debug {
    /// Variable `name` (lower case): its value and type; `None` when it
    /// was never set.
    fn get(&self, name: &str) -> Option<(Value, ExprType)>;

    /// Assigns variable `name` (lower case).
    fn set(&self, name: &str, value: Value, ty: ExprType);

    /// `true` the first time it is asked in a statement (MySQL's warning
    /// is given once).
    fn first_assignment(&self) -> bool;
}

/// The names of the user variable functions.
pub(crate) const NAMES: &[&str] = &["SPRIG_USER_VAR", "SPRIG_SET_USER_VAR"];

/// MySQL's deprecation warning for an assignment inside an expression.
const DEPRECATED: &str = "Setting user variables within expressions is deprecated and will be \
                          removed in a future release. Consider alternatives: 'SET \
                          variable=expression, ...', or 'SELECT expression(s) INTO \
                          variables(s)'.";

fn unserved() -> SqlError {
    SqlError::unsupported("user variables here")
}

/// Compiles `SPRIG_USER_VAR(v)` or `SPRIG_SET_USER_VAR(v, e)`: `args[0]` is
/// the variable's name as a keyword, `nodes` the rest.
pub(crate) fn build(name: &str, args: &[Expr], nodes: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let Some(Expr::Keyword(var)) = args.first() else {
        return Err(SqlError::param_count(name));
    };
    let var: Arc<str> = var.as_str().into();
    match name {
        "SPRIG_USER_VAR" => {
            if !nodes.is_empty() {
                return Err(SqlError::param_count(name));
            }
            let known = env.s.info.user_vars.as_ref().and_then(|u| u.get(&var));
            let text = format!("@{var}");
            // A variable not set when the statement began reads as `NULL`
            // throughout it, as on both servers, even once assigned.
            let Some((_, ty)) = known else {
                return Ok(Node::new(ExprType::null(), text, |_, _| Ok(Value::Null)));
            };
            let ty = ty.nullable();
            Ok(Node::new(ty, text, move |ctx, _| {
                let Some(u) = ctx.session.info.user_vars.clone() else {
                    return Err(unserved());
                };
                let Some((v, vty)) = u.get(&var) else {
                    return Ok(Value::Null);
                };
                if v.is_null() || same_kind(&vty, &ty) {
                    return Ok(v);
                }
                let env = Env {
                    s: ctx.session,
                    prep: None,
                    counts: None,
                };
                let f = coerce_fn(&vty, &ty, env);
                ctx.mute += 1;
                let r = f(ctx, v);
                ctx.mute -= 1;
                r
            }))
        }
        "SPRIG_SET_USER_VAR" => {
            let Ok([e]) = <[Node; 1]>::try_from(nodes) else {
                return Err(SqlError::param_count(name));
            };
            // The value's own type; an integer is kept as a BIGINT.
            let mut ty = e.ty;
            if ty.kind == Kind::Int {
                ty.width = 8;
            }
            let text = format!("@{var} := {}", e.text);
            Ok(Node::new(ty, text, move |ctx, row| {
                let v = (e.f)(ctx, row)?;
                let Some(u) = ctx.session.info.user_vars.clone() else {
                    return Err(unserved());
                };
                if !ctx.maria() && u.first_assignment() {
                    ctx.warn(1287, DEPRECATED);
                }
                u.set(&var, v.clone(), ty);
                Ok(v)
            }))
        }
        _ => Err(SqlError::no_function(name)),
    }
}

/// Whether a value of type `a` is read as type `b` unchanged.
fn same_kind(a: &ExprType, b: &ExprType) -> bool {
    a.kind == b.kind || (a.kind == Kind::Int && b.kind == Kind::Int)
}
