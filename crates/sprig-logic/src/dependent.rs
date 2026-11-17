//! A table's columns changing under the validations and after-commit
//! triggers bound to it.
//!
//! A compiled validation or trigger reads the row through parameters typed
//! when it was deployed: its `BEFORE` triggers pass `NEW.c` and `OLD.c` to
//! a routine whose parameters have the column's type then, and an
//! after-commit trigger's routine is called the same way. When `ALTER
//! TABLE` drops, renames or retypes such a column, MariaDB doesn't look
//! inside the triggers: writes would fail later (an unknown column), or
//! the value would be converted to the old type (a `DOUBLE` cut to a
//! `BIGINT`) and the code would decide on the wrong value.
//!
//! ```text
//! ALTER TABLE t MODIFY | CHANGE | RENAME COLUMN | DROP COLUMN | ALTER COLUMN ... TYPE:
//!   for each active validation and after-commit trigger bound to t:
//!     compile its active version against today's columns and against
//!     the columns the statement would leave              (rebind)
//!   the same parameters, of the same types: nothing to do
//!   the same parameter names, other types (a column it reads changes
//!     type): the gateway runs the statement, then deploys the same
//!     source again as a new version, for the module's owner
//!   it would no longer compile (a column it reads is dropped or
//!     renamed): 39481, naming the code and why; drop it, change the
//!     table, and create it again
//! ```
//!
//! Only the compiled parameters count: code that doesn't read a changed
//! column, or a change that keeps the column's type in the IR
//! (`VARCHAR(16)` to `VARCHAR(64)`, `INT` to `BIGINT`), runs on.

use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, Runtime, StaticColumns, sql};
use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, DataType, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// One change a statement makes to a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnOp {
    /// `DROP [COLUMN] c`.
    Drop(String),
    /// `RENAME COLUMN a TO b`, or `CHANGE a b ...`: the name (the type
    /// may change too, see `Retype`).
    Rename(String, String),
    /// `MODIFY c TYPE`, `CHANGE a b TYPE`, `ALTER COLUMN c TYPE`: the
    /// column's new type (`None`: a type the compiler has no IR type for).
    Retype(String, Option<Ty>),
}

/// An `ALTER TABLE` that changes existing columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChange {
    /// The database, if the name was qualified.
    pub db: Option<String>,
    /// The table.
    pub table: String,
    /// What happens to which columns, in order.
    pub ops: Vec<ColumnOp>,
}

/// Whether `sql` may be an `ALTER TABLE` that changes existing columns
/// (a cheap look before [`parse`]).
#[must_use]
pub fn is_candidate(sql: &str) -> bool {
    let head: String = sql
        .chars()
        .take(8192)
        .collect::<String>()
        .to_ascii_lowercase();
    let t = head.trim_start();
    t.starts_with("alter")
        && t.contains("table")
        && [
            "modify",
            "change",
            "drop",
            "rename",
            "alter column",
            " type ",
        ]
        .iter()
        .any(|w| t.contains(w))
}

/// The IR type of a column of SQL type `d`.
fn ty_of(d: &DataType) -> Option<Ty> {
    let full = d.to_string().to_ascii_lowercase();
    let name = full
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_owned();
    sql::ty_of_column(&name, &full)
}

/// The column changes of an `ALTER TABLE`; `None` for any other statement,
/// or one that changes no existing column.
#[must_use]
pub fn parse(text: &str) -> Option<ColumnChange> {
    let mut stmts = Parser::parse_sql(&MySqlDialect {}, text).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::AlterTable(at) = stmts.pop()? else {
        return None;
    };
    let parts: Vec<String> = at
        .name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
        .collect();
    let (db, table) = match parts.as_slice() {
        [t] => (None, t.clone()),
        [d, t] => (Some(d.clone()), t.clone()),
        _ => return None,
    };
    let mut ops = Vec::new();
    for op in &at.operations {
        match op {
            AlterTableOperation::DropColumn { column_names, .. } => {
                ops.extend(column_names.iter().map(|c| ColumnOp::Drop(c.value.clone())));
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => ops.push(ColumnOp::Rename(
                old_column_name.value.clone(),
                new_column_name.value.clone(),
            )),
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                ..
            } => ops.push(ColumnOp::Retype(col_name.value.clone(), ty_of(data_type))),
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                ..
            } => {
                if !old_name.value.eq_ignore_ascii_case(&new_name.value) {
                    ops.push(ColumnOp::Rename(
                        old_name.value.clone(),
                        new_name.value.clone(),
                    ));
                }
                ops.push(ColumnOp::Retype(new_name.value.clone(), ty_of(data_type)));
            }
            AlterTableOperation::AlterColumn {
                column_name,
                op: AlterColumnOperation::SetDataType { data_type, .. },
            } => ops.push(ColumnOp::Retype(
                column_name.value.clone(),
                ty_of(data_type),
            )),
            _ => {}
        }
    }
    (!ops.is_empty()).then_some(ColumnChange { db, table, ops })
}

/// `cols` as `change` would leave `table`'s columns.
#[must_use]
pub fn predict(cols: &StaticColumns, table: &str, ops: &[ColumnOp]) -> StaticColumns {
    let mut out = cols.clone();
    for op in ops {
        match op {
            ColumnOp::Drop(c) => {
                out.remove(table, c);
            }
            ColumnOp::Rename(a, b) => {
                if let Some(t) = out.remove(table, a) {
                    out.add(table, b, t);
                }
            }
            ColumnOp::Retype(c, ty) => {
                out.remove(table, c);
                if let Some(t) = ty {
                    out.add(table, c, *t);
                }
            }
        }
    }
    out
}

/// What a column change means for one validation or trigger
/// ([`rebind`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rebind {
    /// It compiles to the same routine: nothing to do.
    Same,
    /// It still compiles, with other parameter types (a column it reads
    /// changes type): it is recompiled and redeployed after the change,
    /// as its owner. The text says which parameters change.
    Recompile(String),
    /// It would no longer compile (a column it reads is dropped, renamed,
    /// or gets a type the compiler has no type for): the change is refused
    /// with 39481. The text says why.
    Refuse(String),
}

/// What the change from `before` to `after` means for a validation's or
/// trigger's active version (`source` in `runtime`): the same routine
/// (the same parameters `NEW.c`, `OLD.c` and `op`, by name, of the same
/// types), a recompile (the same names, other types), or a refusal.
#[must_use]
pub fn rebind(
    runtime: Runtime,
    name: &str,
    source: &str,
    table: &str,
    before: &StaticColumns,
    after: &StaticColumns,
) -> Rebind {
    match still_binds(runtime, name, source, table, before, after) {
        Ok(()) => Rebind::Same,
        Err(Changed::Types(why)) => Rebind::Recompile(why),
        Err(Changed::Broken(why)) => Rebind::Refuse(why),
    }
}

/// Why a version doesn't bind the same way.
enum Changed {
    /// The same parameter names, other types.
    Types(String),
    /// It doesn't compile, or a parameter is gone.
    Broken(String),
}

fn still_binds(
    runtime: Runtime,
    name: &str,
    source: &str,
    table: &str,
    before: &StaticColumns,
    after: &StaticColumns,
) -> std::result::Result<(), Changed> {
    let compile = |cols: &StaticColumns| match runtime {
        Runtime::Javascript => sprig_logic_compile::js::compile_with(
            name,
            source,
            &sprig_logic_compile::js::Signature::row_of(table),
            cols,
            &Options::default(),
        ),
        Runtime::Lua => sprig_logic_compile::lua::compile_with(
            name,
            source,
            &sprig_logic_compile::js::Signature::row_of(table),
            cols,
            &Options::default(),
        ),
        _ => sprig_logic_compile::compile(runtime, source, cols, &Options::default()),
    };
    let params = |c: &sprig_logic_compile::Compiled| -> Vec<(String, Ty)> {
        c.ir.as_ref()
            .map(|f| f.params.iter().map(|p| (p.name.clone(), p.ty)).collect())
            .unwrap_or_default()
    };
    let now = match compile(before) {
        Ok(c) if c.compilable() => c,
        // Not compiled today: nothing of it is in MariaDB to break.
        _ => return Ok(()),
    };
    let then = compile(after).map_err(|e| Changed::Broken(e.message))?;
    if !then.compilable() {
        let why: Vec<String> = then
            .analysis
            .reasons
            .iter()
            .map(ToString::to_string)
            .collect();
        return Err(Changed::Broken(format!(
            "it would no longer compile ({})",
            why.join("; ")
        )));
    }
    let (a, b) = (params(&now), params(&then));
    if a == b {
        return Ok(());
    }
    let changed: Vec<String> = a
        .iter()
        .filter(|p| !b.contains(p))
        .map(|(n, t)| match b.iter().find(|(m, _)| m == n) {
            Some((_, u)) => format!("{n} ({t} becomes {u})"),
            None => format!("{n} (gone)"),
        })
        .collect();
    let names = |v: &[(String, Ty)]| v.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    let why = format!(
        "the columns it reads would change under it: {}",
        changed.join(", ")
    );
    if names(&a) == names(&b) {
        Err(Changed::Types(why))
    } else {
        Err(Changed::Broken(why))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn cols() -> StaticColumns {
        StaticColumns::new().table(
            "sales",
            &[
                ("id", Ty::Int),
                ("qty", Ty::Int),
                ("total", Ty::Float),
                ("note", Ty::Str),
            ],
        )
    }

    #[test]
    fn column_changes_are_read() {
        let c = parse("ALTER TABLE shop.sales MODIFY qty DOUBLE, DROP COLUMN note").unwrap();
        assert_eq!(c.db.as_deref(), Some("shop"));
        assert_eq!(c.table, "sales");
        assert_eq!(
            c.ops,
            [
                ColumnOp::Retype("qty".into(), Some(Ty::Float)),
                ColumnOp::Drop("note".into())
            ]
        );
        let c = parse("ALTER TABLE sales CHANGE qty amount BIGINT NOT NULL").unwrap();
        assert_eq!(
            c.ops,
            [
                ColumnOp::Rename("qty".into(), "amount".into()),
                ColumnOp::Retype("amount".into(), Some(Ty::Int))
            ]
        );
        let c = parse("ALTER TABLE sales RENAME COLUMN note TO memo").unwrap();
        assert_eq!(c.ops, [ColumnOp::Rename("note".into(), "memo".into())]);
        assert!(parse("ALTER TABLE sales ADD COLUMN x INT").is_none());
        assert!(parse("SELECT 1").is_none());
        assert!(is_candidate("alter table sales modify qty double"));
        assert!(!is_candidate("alter table sales add column x int"));
    }

    #[test]
    fn a_type_change_recompiles_and_a_lost_column_refuses() {
        let src = "export default function (row) { if (row.qty > 10) sprig.reject('too many'); }";
        let before = cols();
        let after = |sql: &str| predict(&before, "sales", &parse(sql).unwrap().ops);
        let r = |a: &StaticColumns| rebind(Runtime::Javascript, "v", src, "sales", &before, a);
        assert_eq!(
            r(&after("ALTER TABLE sales MODIFY qty BIGINT")),
            Rebind::Same
        );
        // A column it doesn't read may change freely.
        assert_eq!(
            r(&after("ALTER TABLE sales DROP COLUMN note")),
            Rebind::Same
        );
        // A column it reads changes type: recompiled.
        match r(&after("ALTER TABLE sales MODIFY qty DOUBLE")) {
            Rebind::Recompile(why) => assert!(why.contains("qty (i64 becomes f64)"), "{why}"),
            other => panic!("{other:?}"),
        }
        // A column it reads is renamed or dropped: refused.
        match r(&after("ALTER TABLE sales RENAME COLUMN qty TO amount")) {
            Rebind::Refuse(why) => assert!(why.contains("no longer compile"), "{why}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            r(&after("ALTER TABLE sales DROP COLUMN qty")),
            Rebind::Refuse(_)
        ));
    }
}
