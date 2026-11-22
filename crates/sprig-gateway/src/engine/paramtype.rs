//! The types MySQL 8.4 gives a prepared statement's `?` parameters from
//! their context, for statements the planner or the multi-table write
//! path runs (the single-table path types its own while it compiles).
//!
//! As observed on MySQL 8.4: a parameter compared with a column, in an
//! `IN` list or `BETWEEN` beside one, or assigned to one takes the
//! column's type; one in arithmetic with a column takes that column's,
//! and in arithmetic with anything else a signed 64-bit integer. The
//! first context that gives a type wins. Every other parameter keeps the
//! default (a string), as before.

use std::ops::ControlFlow;

use sqlparser::ast::{self, BinaryOperator, Expr, Statement};

use super::table::{ColType, TableDef};

/// One table a statement reads: the names it is read under (lower case)
/// and its definition.
pub(crate) struct Named<'a> {
    pub aliases: &'a [String],
    pub def: &'a TableDef,
}

/// The `n`th placeholder of an expression numbered by [`number`].
fn param(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(v) => match &v.value {
            ast::Value::Placeholder(p) => p.strip_prefix("?#")?.parse().ok(),
            _ => None,
        },
        Expr::Nested(x) => param(x),
        _ => None,
    }
}

/// Numbers the statement's `?` in the order they are written (`?#0`,
/// `?#1`, ...), as the planner does.
fn number(stmt: &mut Statement) -> usize {
    let mut n = 0usize;
    let _ = ast::visit_expressions_mut(stmt, |e: &mut Expr| {
        if let Expr::Value(v) = e
            && let ast::Value::Placeholder(p) = &mut v.value
            && p == "?"
        {
            *p = format!("?#{n}");
            n += 1;
        }
        ControlFlow::<()>::Continue(())
    });
    n
}

fn column(tables: &[Named<'_>], parts: &[ast::Ident]) -> Option<ColType> {
    let (qual, name) = match parts {
        [c] => (None, &c.value),
        [t, c] | [_, t, c] => (Some(t.value.to_ascii_lowercase()), &c.value),
        _ => return None,
    };
    tables
        .iter()
        .filter(|t| {
            qual.as_ref().is_none_or(|q| {
                t.aliases.iter().any(|a| a == q) || t.def.name.eq_ignore_ascii_case(q)
            })
        })
        .find_map(|t| t.def.column(name).map(|i| t.def.columns[i].ty))
}

fn column_of(tables: &[Named<'_>], e: &Expr) -> Option<ColType> {
    match e {
        Expr::Identifier(i) => column(tables, std::slice::from_ref(i)),
        Expr::CompoundIdentifier(p) => column(tables, p),
        Expr::Nested(x) => column_of(tables, x),
        _ => None,
    }
}

const INT: ColType = ColType::Int {
    bytes: 8,
    unsigned: false,
};

fn arithmetic(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::MyIntegerDivide
    )
}

/// The context types of `stmt`'s parameters (`None`: the default).
pub(crate) fn infer(stmt: &Statement, tables: &[Named<'_>]) -> Vec<Option<ColType>> {
    let mut stmt = stmt.clone();
    let n = number(&mut stmt);
    let mut out: Vec<Option<ColType>> = vec![None; n];
    let mut set = |k: usize, t: ColType| {
        if let Some(slot) = out.get_mut(k)
            && slot.is_none()
        {
            *slot = Some(t);
        }
    };
    // `SET col = ?` of an UPDATE, in its place among the expressions: the
    // walk below visits the assignments' values in order, so they are
    // typed first only where they come first.
    let mut assigned: Vec<(usize, ColType)> = Vec::new();
    if let Statement::Update(u) = &stmt {
        for a in &u.assignments {
            if let (ast::AssignmentTarget::ColumnName(name), Some(k)) = (&a.target, param(&a.value))
            {
                let parts: Vec<ast::Ident> = name
                    .0
                    .iter()
                    .filter_map(|p| p.as_ident().cloned())
                    .collect();
                if let Some(t) = column(tables, &parts) {
                    assigned.push((k, t));
                }
            }
        }
    }
    for (k, t) in assigned {
        set(k, t);
    }
    let _ = ast::visit_expressions(&stmt, |e: &Expr| {
        match e {
            Expr::BinaryOp { left, op, right } => {
                let (l, r) = (param(left), param(right));
                if let (Some(k), Some(t)) = (r, column_of(tables, left)) {
                    set(k, t);
                }
                if let (Some(k), Some(t)) = (l, column_of(tables, right)) {
                    set(k, t);
                }
                if arithmetic(op) {
                    for k in [l, r].into_iter().flatten() {
                        set(k, INT);
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Some(t) = column_of(tables, expr) {
                    for k in list.iter().filter_map(param) {
                        set(k, t);
                    }
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                if let Some(t) = column_of(tables, expr) {
                    for k in [param(low), param(high)].into_iter().flatten() {
                        set(k, t);
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types(sql: &str, defs: &[(&str, &TableDef)]) -> Vec<Option<ColType>> {
        use sqlparser::dialect::MySqlDialect;
        let stmt = sqlparser::parser::Parser::parse_sql(&MySqlDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap();
        let aliases: Vec<Vec<String>> = defs.iter().map(|(a, _)| vec![(*a).to_owned()]).collect();
        let named: Vec<Named<'_>> = defs
            .iter()
            .zip(&aliases)
            .map(|((_, d), a)| Named { aliases: a, def: d })
            .collect();
        infer(&stmt, &named)
    }

    #[test]
    fn parameters_take_their_columns_types() {
        let def = |o: u64, sql: &str| {
            crate::engine::table::parse(
                sprig_common::ids::ObjectId(o),
                1,
                sprig_common::ids::DbId(1),
                sql,
            )
            .unwrap()
        };
        let a = def(
            1,
            "CREATE TABLE `mp_a` (`id` int(11) NOT NULL, `v` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        );
        let b = def(
            2,
            "CREATE TABLE `mp_b` (`id` int(11) NOT NULL, `w` varchar(10) DEFAULT NULL, `aid` int(11) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci",
        );
        let int = Some(ColType::Int {
            bytes: 4,
            unsigned: false,
        });
        let t = types(
            "UPDATE mp_a a JOIN mp_b b ON b.aid = a.id SET a.v = a.v + ? WHERE b.w = ?",
            &[("a", &a), ("b", &b)],
        );
        assert_eq!(t[0], int);
        assert!(matches!(t[1], Some(ColType::VarChar { .. })));
        let t = types(
            "DELETE FROM mp_b WHERE aid IN (SELECT id FROM mp_a WHERE v > ?)",
            &[("mp_b", &b), ("mp_a", &a)],
        );
        assert_eq!(t[0], int);
        let t = types("SELECT ? + 1, ?", &[]);
        assert_eq!(t, vec![Some(INT), None]);
    }
}
