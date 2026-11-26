//! `JSON_TABLE` in `FROM`.
//!
//! The document expression may read the `FROM` items before it (MySQL and
//! MariaDB make a table function lateral). Such an item is joined after
//! them, its rows made again for each row so far ([`JsonTableInner`]); one
//! whose document reads nothing is made once.

use std::rc::Rc;

use sprig_engine::row::Value;
use sprig_exec::ops::{InnerSource, Row, RowSource};
use sprig_exec::{Compiled, Ctx, Expr, ExprType, JsonTable, JtColumn, JtOnMiss, Result, SqlError};
use sqlparser::ast;

use crate::exprs;
use crate::plan::Cx;
use crate::resolve::{Clause, ExprCx, Rel, RelKind, Scope, derived_cols, unsupported};

/// A `JSON_TABLE` item: the compiled columns and the document expression
/// (over the earlier items' slots).
pub(crate) struct JtRel {
    pub(crate) table: JsonTable,
    pub(crate) doc: Expr,
}

impl JtRel {
    /// Whether the document reads earlier items.
    pub(crate) fn lateral(&self) -> bool {
        !exprs::slots(&self.doc).is_empty()
    }
}

/// The rows of a `JSON_TABLE` whose document reads no item.
pub(crate) struct JsonTableSource {
    pub(crate) table: Rc<JtRel>,
    pub(crate) doc: Compiled,
    pub(crate) rows: Option<std::vec::IntoIter<Row>>,
}

impl RowSource for JsonTableSource {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if self.rows.is_none() {
            let d = self.doc.eval(ctx, &[])?;
            self.rows = Some(self.table.table.rows(ctx, &d)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn types(&self) -> Vec<ExprType> {
        self.table.table.types().to_vec()
    }
}

/// The rows of a lateral `JSON_TABLE`, made for each outer row.
pub(crate) struct JsonTableInner {
    pub(crate) table: Rc<JtRel>,
    /// The document, over the outer row.
    pub(crate) doc: Compiled,
    pub(crate) rows: std::vec::IntoIter<Row>,
}

impl InnerSource for JsonTableInner {
    fn open(&mut self, ctx: &mut Ctx<'_>, outer: &[Value]) -> Result<()> {
        let d = self.doc.eval(ctx, outer)?;
        self.rows = self.table.table.rows(ctx, &d)?.into_iter();
        Ok(())
    }

    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        Ok(self.rows.next())
    }

    fn types(&self) -> Vec<ExprType> {
        self.table.table.types().to_vec()
    }
}

fn text_of(v: &ast::ValueWithSpan) -> Result<String> {
    match &v.value {
        ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
        _ => Err(unsupported("a JSON_TABLE path that isn't a string")),
    }
}

/// `ON EMPTY` / `ON ERROR`. A `DEFAULT` that isn't a string is its text
/// on MariaDB; MySQL refuses it (1235).
fn on_miss(h: Option<&ast::JsonTableColumnErrorHandling>, maria: bool) -> Result<JtOnMiss> {
    Ok(match h {
        None | Some(ast::JsonTableColumnErrorHandling::Null) => JtOnMiss::Null,
        Some(ast::JsonTableColumnErrorHandling::Error) => JtOnMiss::Error,
        Some(ast::JsonTableColumnErrorHandling::Default(v)) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                JtOnMiss::Default(s.clone())
            }
            ast::Value::Number(n, _) if maria => JtOnMiss::Default(n.clone()),
            ast::Value::Boolean(b) if maria => JtOnMiss::Default(u8::from(*b).to_string()),
            _ if maria => return Err(unsupported("this JSON_TABLE DEFAULT value")),
            _ => {
                return Err(SqlError::new(
                    1235,
                    "42000",
                    "This version of MySQL doesn't yet support 'non-string DEFAULT value for a \
                     column in a JSON_TABLE expression'",
                ));
            }
        },
    })
}

fn columns(cols: &[ast::JsonTableColumn], maria: bool) -> Result<Vec<JtColumn>> {
    cols.iter()
        .map(|c| {
            Ok(match c {
                ast::JsonTableColumn::ForOrdinality(n) => JtColumn::Ordinality(n.value.clone()),
                ast::JsonTableColumn::Named(n) => JtColumn::Path {
                    name: n.name.value.clone(),
                    ty: n.r#type.to_string(),
                    path: text_of(&n.path)?,
                    exists: n.exists,
                    on_empty: on_miss(n.on_empty.as_ref(), maria)?,
                    on_error: on_miss(n.on_error.as_ref(), maria)?,
                },
                ast::JsonTableColumn::Nested(n) => JtColumn::Nested {
                    path: text_of(&n.path)?,
                    columns: columns(&n.columns, maria)?,
                },
            })
        })
        .collect()
}

impl Cx<'_> {
    /// A `JSON_TABLE` item, its document resolved in `scope` (the items
    /// before it).
    pub(crate) fn json_table_rel(
        &mut self,
        tf: &ast::TableFactor,
        scope: &Scope<'_>,
    ) -> Result<Rel> {
        let ast::TableFactor::JsonTable {
            json_expr,
            json_path,
            columns: cols,
            alias,
        } = tf
        else {
            return Err(unsupported("this table function"));
        };
        let maria = self.session.persona.is_maria();
        let Some(alias) = alias else {
            return Err(if maria {
                SqlError::new(4177, "HY000", "Every table function must have an alias.")
            } else {
                SqlError::new(3667, "42000", "Every table function must have an alias")
            });
        };
        let mut x = ExprCx::new(scope, Clause::On);
        let before = self.outer_refs;
        let doc = self.expr(json_expr, &mut x)?;
        // A document read from an outer query: the subquery is planned
        // again for each outer row instead (the dependent path takes this
        // refusal), never evaluated once without that row.
        if self.outer_refs != before {
            self.outer_refs = before;
            return Err(unsupported(
                "a table function that reads its outer query here",
            ));
        }
        let table = JsonTable::new(
            &text_of(json_path)?,
            &columns(cols, maria)?,
            &self.session,
            &alias.name.value,
        )?;
        let names = table.names().to_vec();
        let types = table.types().to_vec();
        let cols = derived_cols(0, &names, &types);
        Ok(Rel {
            shown: alias.name.value.clone(),
            alias: alias.name.value.clone(),
            qualified: None,
            width: cols.len(),
            kind: RelKind::JsonTable(Rc::new(JtRel { table, doc })),
            cols,
            base: 0,
            outer_on: None,
            outer_dep: None,
            pending: Vec::new(),
            hidden: false,
            derived_id: None,
        })
    }
}

/// `sql` with each `JSON_TABLE` column type that carries `CHARACTER SET cs`
/// or `COLLATE c` written as one quoted name (`` `VARCHAR(10) CHARACTER
/// SET utf8mb4` ``), which the parser reads as a type and the expression
/// engine reads back; `None` when there is none. Both servers take those
/// clauses there; the parser takes none.
#[must_use]
pub fn quote_column_charsets(sql: &str) -> Option<String> {
    let b = sql.as_bytes();
    let upper = sql.to_ascii_uppercase();
    let word_at = |i: usize, w: &str| {
        upper[i..].starts_with(w)
            && (i == 0 || !is_word_byte(b[i - 1]))
            && b.get(i + w.len()).is_none_or(|c| !is_word_byte(*c))
    };
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut i = 0;
    while let Some(at) = upper[i..].find("JSON_TABLE") {
        let start = i + at;
        i = start + "JSON_TABLE".len();
        if !word_at(start, "JSON_TABLE") {
            continue;
        }
        // The call's parentheses, strings skipped.
        let Some(open) = sql[i..].find('(').map(|o| i + o) else {
            break;
        };
        let Some(close) = matching(b, open) else {
            break;
        };
        // Each column definition ends at `PATH`: the type is what lies
        // between its name and `PATH` (or `EXISTS PATH`).
        let mut k = open + 1;
        let mut seg = k;
        let mut quote: Option<u8> = None;
        // Open parentheses inside the call: `true` for a `COLUMNS` list.
        let mut stack: Vec<bool> = Vec::new();
        while k < close {
            let c = b[k];
            match quote {
                Some(q) => {
                    if c == q {
                        quote = None;
                    } else if c == b'\\' {
                        k += 1;
                    }
                }
                None => {
                    if matches!(c, b'\'' | b'"' | b'`') {
                        quote = Some(c);
                    } else if c == b'(' {
                        let list = upper[..k].trim_end().ends_with("COLUMNS");
                        stack.push(list);
                        if list {
                            seg = k + 1;
                        }
                    } else if c == b')' {
                        stack.pop();
                    } else if c == b',' && stack.last() == Some(&true) {
                        seg = k + 1;
                    } else if stack.last() == Some(&true) && word_at(k, "PATH") {
                        if let Some(e) = typed_segment(sql, &upper, seg, k) {
                            edits.push(e);
                        }
                        seg = k + 4;
                    }
                }
            }
            k += 1;
        }
        i = close;
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + 8 * edits.len());
    let mut at = 0;
    for (a, z, text) in edits {
        out.push_str(&sql[at..a]);
        out.push_str(&text);
        at = z;
    }
    out.push_str(&sql[at..]);
    Some(out)
}

fn is_word_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$'
}

/// The `)` matching the `(` at `open`, strings skipped.
fn matching(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut k = open;
    while k < b.len() {
        let c = b[k];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else if c == b'\\' {
                    k += 1;
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(k);
                    }
                }
                _ => {}
            },
        }
        k += 1;
    }
    None
}

/// One column definition `name type ... ` in `[seg, path)`: the edit that
/// quotes its type when the type names a character set or collation.
fn typed_segment(
    sql: &str,
    upper: &str,
    seg: usize,
    path: usize,
) -> Option<(usize, usize, String)> {
    let body = &upper[seg..path];
    if !(body.contains(" CHARACTER SET ")
        || body.contains(" CHARSET ")
        || body.contains(" COLLATE "))
    {
        return None;
    }
    let text = &sql[seg..path];
    let lead = text.len() - text.trim_start().len();
    let rest = text.trim_start();
    // The column's name, then its type.
    let name_len = match rest.strip_prefix('`') {
        Some(quoted) => quoted.find('`')? + 2,
        None => rest.find(char::is_whitespace)?,
    };
    let ty_start = seg + lead + name_len;
    let mut ty = sql[ty_start..path].trim_end();
    let mut ty_end = ty_start + (sql[ty_start..path].trim_end().len());
    // `EXISTS PATH` keeps its word outside the type.
    if ty.to_ascii_uppercase().ends_with(" EXISTS") {
        ty = &ty[..ty.len() - " EXISTS".len()];
        ty_end -= " EXISTS".len();
    }
    let ty = ty.trim();
    if ty.contains('`') {
        return None;
    }
    let pad = sql[ty_start..].len() - sql[ty_start..].trim_start().len();
    Some((ty_start + pad, ty_end, format!("`{ty}`")))
}

#[cfg(test)]
mod charset_tests {
    use super::quote_column_charsets;

    #[test]
    fn column_types_with_a_character_set_are_quoted() {
        assert_eq!(
            quote_column_charsets(
                "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INT PATH '$.id', d DECIMAL(10,2) PATH '$.d', s VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s', e INT EXISTS PATH '$.e', NESTED PATH '$.n' COLUMNS (m CHAR(3) CHARSET latin1 PATH '$.m'))) AS t"
            )
            .as_deref(),
            Some(
                "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INT PATH '$.id', d DECIMAL(10,2) PATH '$.d', s `VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin` PATH '$.s', e INT EXISTS PATH '$.e', NESTED PATH '$.n' COLUMNS (m `CHAR(3) CHARSET latin1` PATH '$.m'))) AS t"
            )
        );
        assert_eq!(
            quote_column_charsets("SELECT * FROM JSON_TABLE(d, '$' COLUMNS (s TEXT PATH '$.a')) t"),
            None
        );
    }
}
