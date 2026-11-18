//! Functional key parts in `CREATE INDEX` and `ALTER TABLE ... ADD INDEX`
//! on port 3306.
//!
//! The hidden column standing for a part needs a type, which the text
//! rewrite (`sprig_sql::compat::fxindex`) infers from the statement; in
//! `CREATE TABLE` the columns' types are in the text, here they aren't.
//! The gateway reads them from the table's `information_schema.COLUMNS`
//! through the session and rewrites the statement with them
//! (`fxindex::rewrite_with`). Django creates functional unique
//! constraints this way (`CREATE UNIQUE INDEX u ON t ((LOWER(name)), rank)`).
//! An `ALTER TABLE ... ADD INDEX ((e))` without a name also reads the
//! table's index names, so the index gets the name MySQL would give it
//! (`functional_index`, or the first free `functional_index_<n>`).
//! `DROP INDEX` of a functional index also drops the hidden checks of its
//! parts (`fxindex::FX_CHECK_PREFIX`) that no other index shares.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use bytes::Bytes;
use sprig_common::Result;
use sprig_proto_mysql::Persona;
use sprig_sql::compat::fxindex;

use super::PassSession;
use crate::pipeline::ExecCtx;

impl PassSession {
    /// The statement with its functional parts typed from the table's
    /// columns; `None` when it needs nothing (or the types don't help).
    pub(super) async fn fx_typed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &[u8],
    ) -> Result<Option<Bytes>> {
        if self.persona != Persona::MySql84 {
            return Ok(None);
        }
        let (lex, _) = self.lex_opts();
        // A descending index over a foreign key's columns, and the hidden
        // checks of a functional index's parts.
        if let Some((table, index)) = fxindex::drop_index_target(text, &lex) {
            let checks = Box::pin(self.fx_checks_of(cx, &table, &index))
                .await
                .unwrap_or_default();
            let kept = Box::pin(self.fk_desc_drop(cx, &table, &index))
                .await
                .unwrap_or(None);
            if checks.is_empty() {
                return Ok(kept);
            }
            let mut sql = kept.map_or_else(
                || {
                    format!(
                        "ALTER TABLE {table} DROP INDEX `{}`",
                        index.replace('`', "``")
                    )
                },
                |b| String::from_utf8_lossy(&b).into_owned(),
            );
            for c in &checks {
                let _ = write!(sql, ", DROP CONSTRAINT `{}`", c.replace('`', "``"));
            }
            return Ok(Some(Bytes::from(sql)));
        }
        // An unnamed functional index added to a table gets the first
        // name MySQL would give it that the table doesn't use.
        let unnamed = fxindex::unnamed_alter_table(text, &lex);
        let Some(table) = fxindex::needs_types(text, &lex).or_else(|| unnamed.clone()) else {
            return Ok(None);
        };
        let Some((schema, name)) = self.table_literals(&table) else {
            return Ok(None);
        };
        let q = format!(
            "SELECT LOWER(COLUMN_NAME), COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
        );
        let got = self.run_checked(cx, &q, false).await?;
        let known: BTreeMap<String, String> = got
            .rows
            .iter()
            .filter_map(|r| {
                let n = r.first()?.as_deref()?;
                let t = r.get(1)?.as_deref()?;
                Some((
                    String::from_utf8_lossy(n).into_owned(),
                    String::from_utf8_lossy(t).into_owned(),
                ))
            })
            .collect();
        let mut existing: Vec<String> = Vec::new();
        if unnamed.is_some() {
            let q = format!(
                "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
            );
            let got = self.run_checked(cx, &q, false).await?;
            existing = got
                .rows
                .iter()
                .filter_map(|r| {
                    r.first()?
                        .as_deref()
                        .map(|n| String::from_utf8_lossy(n).into_owned())
                })
                .collect();
        }
        Ok(fxindex::rewrite_named(text, &lex, &known, &existing).map(Bytes::from))
    }

    /// The table `table` (as written, perhaps qualified) as two SQL
    /// literals for `information_schema`: its schema (`DATABASE()` when
    /// unqualified) and name.
    pub(super) fn table_literals(&self, table: &str) -> Option<(String, String)> {
        let (lex, _) = self.lex_opts();
        let names: Vec<String> = sprig_sql::lex::lex(table.as_bytes(), &lex)
            .map(|t| {
                t.iter()
                    .filter(|t| t.kind.is_ident_like())
                    .map(|t| {
                        String::from_utf8_lossy(&sprig_sql::lex::ident_value(
                            t.bytes(table.as_bytes()),
                        ))
                        .into_owned()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        match names.as_slice() {
            [t] => Some(("DATABASE()".to_owned(), lit(t))),
            [d, t] => Some((lit(d), lit(t))),
            _ => None,
        }
    }

    /// The hidden checks (`fxindex::FX_CHECK_PREFIX`) of the functional
    /// parts of index `index` of `table` (as written) that no other index of
    /// the table has, read from MariaDB's `information_schema` on the
    /// session's backend connection: they go with the index, as MySQL's
    /// hidden column does. `None` when the table can't be read.
    async fn fx_checks_of(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &str,
        index: &str,
    ) -> Option<Vec<String>> {
        let (lex, _) = self.lex_opts();
        let (schema, name) = self.physical_written(table)?;
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        let (schema, name) = (lit(&schema), lit(&name));
        let parts: Vec<(String, String)> = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
            )
            .await
            .ok()?
            .into_iter()
            .filter_map(|r| Some((r.first()?.clone()?, r.get(1)?.clone()?)))
            .collect();
        let own: Vec<&String> = parts
            .iter()
            .filter(|(i, c)| i.eq_ignore_ascii_case(index) && fxindex::is_fx_column(c))
            .map(|(_, c)| c)
            .filter(|c| {
                !parts
                    .iter()
                    .any(|(i, o)| !i.eq_ignore_ascii_case(index) && o.eq_ignore_ascii_case(c))
            })
            .collect();
        if own.is_empty() {
            return Some(Vec::new());
        }
        let checks = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT CONSTRAINT_NAME FROM information_schema.CHECK_CONSTRAINTS \
                     WHERE CONSTRAINT_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
            )
            .await
            .ok()?;
        Some(
            checks
                .into_iter()
                .filter_map(|r| r.first()?.clone())
                .filter(|n| {
                    fxindex::fx_column_of_check(n)
                        .is_some_and(|c| own.iter().any(|o| o.eq_ignore_ascii_case(&c)))
                })
                .collect(),
        )
    }

    /// `DROP INDEX i` where `i` leads with a foreign key's columns, one of
    /// them descending, and no other index serves the key: MySQL kept the
    /// key's own index when `i` was created, MariaDB dropped it and now
    /// refuses with 1553. The drop becomes `ALTER TABLE t ADD INDEX
    /// <key> (cols), DROP INDEX i`, MySQL's end state
    /// (`fxindex::fk_indexes_kept`). `None` otherwise, and
    /// when reading the table fails (the statement then runs as written).
    async fn fk_desc_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &str,
        index: &str,
    ) -> Result<Option<Bytes>> {
        let Some((schema, name)) = self.table_literals(table) else {
            return Ok(None);
        };
        let text = |r: &Vec<Option<Vec<u8>>>, i: usize| {
            r.get(i)
                .and_then(|v| v.as_deref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
        };
        let got = self
            .run_checked(
                cx,
                &format!(
                    "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION \
                     FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
                false,
            )
            .await?;
        let parts: Vec<fxindex::IndexPart> = got
            .rows
            .iter()
            .filter_map(|r| {
                Some(fxindex::IndexPart {
                    index: text(r, 0)?,
                    seq: text(r, 1)?.parse().ok()?,
                    column: text(r, 2),
                    desc: text(r, 3).as_deref() == Some("D"),
                })
            })
            .collect();
        if !parts
            .iter()
            .any(|p| p.desc && p.index.eq_ignore_ascii_case(index))
        {
            return Ok(None);
        }
        let got = self
            .run_checked(
                cx,
                &format!(
                    "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name} \
                     AND REFERENCED_TABLE_NAME IS NOT NULL \
                     ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"
                ),
                false,
            )
            .await?;
        let mut fks: Vec<(String, Vec<String>)> = Vec::new();
        for r in &got.rows {
            let (Some(c), Some(col)) = (text(r, 0), text(r, 1)) else {
                continue;
            };
            match fks.last_mut() {
                Some((n, cols)) if *n == c => cols.push(col),
                _ => fks.push((c, vec![col])),
            }
        }
        let kept = fxindex::fk_indexes_kept(index, &parts, &fks);
        if kept.is_empty() {
            return Ok(None);
        }
        let q = |n: &str| format!("`{}`", n.replace('`', "``"));
        let mut sql = format!("ALTER TABLE {table} ");
        for (fk, cols) in &kept {
            let cols: Vec<String> = cols.iter().map(|c| q(c)).collect();
            let _ = write!(sql, "ADD INDEX {} ({}), ", q(fk), cols.join(", "));
        }
        let _ = write!(sql, "DROP INDEX {}", q(index));
        Ok(Some(Bytes::from(sql)))
    }
}
