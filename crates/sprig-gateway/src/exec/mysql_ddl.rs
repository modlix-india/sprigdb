//! MySQL 8.4's answers to DDL that MariaDB answers differently, where the
//! gateway has to read the table first. The statement analysis is
//! `sprig_sql::compat::mysqlddl`; this module does the lookups, through
//! the session with logical names, as `fxindex` does.
//!
//! - Before a statement runs ([`PassSession::mysql_ddl_checks`]): a name
//!   longer than 64 characters (1059 on 3306, 1103 on 3307), a descending
//!   key part on a `MyISAM` or `MEMORY` table (1178), and an `ALTER TABLE ...
//!   DROP COLUMN` of a column in a multi-column unique index, which
//!   becomes a rebuild of that index without the column.
//! - After a DDL statement failed ([`PassSession::mysql_ddl_error`]):
//!   MariaDB's 1005 for a foreign key becomes MySQL's 1824 when a
//!   referenced table doesn't exist, or 1826 when a constraint name is
//!   taken in the database. Errors naming a functional key part's hidden
//!   column (`_sprig_fx_..`) become MySQL's: 1901 is 3758 with the index's
//!   name, and a `DROP COLUMN`'s 1054 `in 'GENERATED ALWAYS AS'` is 3837
//!   (a functional index uses the column) or 3108 (a generated column).
//!   Dropping every visible column of a table with a functional index
//!   (MariaDB's 1113) is 3837 when the index uses one of them. A `DROP
//!   COLUMN`'s 1054 `in 'CHECK'` is 3959 naming the check, and `in
//!   'DEFAULT'` is 3766 (another column's default expression uses it).
//! - Before an `ALTER TABLE` that renames columns (`RENAME COLUMN`,
//!   `CHANGE`): 3959 when a check constraint uses one, 3766 when another
//!   column's default expression does, 3108 when a generated column does,
//!   3837 when a functional index does ([`PassSession::column_uses`]).
//!   MariaDB would rename it inside all of them.
//! - After an `INSERT`, `REPLACE`, `UPDATE` or `LOAD DATA` failed with a
//!   value that doesn't fit a hidden column ([`PassSession::fx_dml_error`]):
//!   MariaDB's 1264 is 3752, its 1265, 1292, 1366 and 1406 are 3751, each
//!   naming the index. So are its 1292 and 1264 from computing a hidden
//!   column's expression (a cast to an integer, `DOUBLE` or `DECIMAL(m,d)`,
//!   a date function of a string), when the table's columns show the key
//!   part ([`PassSession::fx_eval_dml_error`]).
//!
//! The session's own `information_schema` never shows hidden columns, so
//! the lookups that need them read MariaDB's `information_schema` on the
//! session's backend connection, with the table's physical schema
//! ([`PassSession::generated_columns`]).

use bytes::Bytes;
use sprig_common::SprigError;
use sprig_proto_mysql::Persona;
use sprig_sql::compat::errmap::WireError;
use sprig_sql::compat::mysqlddl;

use super::PassSession;
use crate::pipeline::ExecCtx;

/// What [`PassSession::mysql_ddl_checks`] decided.
pub(super) enum DdlCheck {
    /// The statement runs as it is.
    Run,
    /// The statement runs as this text.
    Rewritten(Bytes),
    /// The statement fails with this error.
    Refused(SprigError),
}

fn refused(e: WireError) -> DdlCheck {
    DdlCheck::Refused(SprigError::standard(e.code, e.sqlstate, e.message))
}

/// A text column of a looked-up row.
fn col(r: &[Option<Vec<u8>>], i: usize) -> Option<String> {
    r.get(i)
        .and_then(|v| v.as_deref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
}

impl PassSession {
    /// The checks of the module docs, before the statement runs. A lookup
    /// that fails leaves the statement as it is.
    pub(super) async fn mysql_ddl_checks(&mut self, cx: &mut ExecCtx<'_>, text: &[u8]) -> DdlCheck {
        let (lex, _) = self.lex_opts();
        if let Some(e) = mysqlddl::long_name(text, &lex) {
            return refused(e);
        }
        if self.persona != Persona::MySql84 {
            return DdlCheck::Run;
        }
        if let Some(e) = mysqlddl::engine_descending(text, &lex) {
            return refused(e);
        }
        // A renamed column that a check constraint, a default expression,
        // a generated column or a functional index uses.
        if let Some((table, renamed)) = mysqlddl::renamed_columns(text, &lex)
            && let Some((name, columns, checks)) = Box::pin(self.column_uses(cx, &table)).await
            && let Some(e) = mysqlddl::rename_dependency(&renamed, &name, &columns, &checks)
        {
            return refused(e);
        }
        if let Some(table) = mysqlddl::descending_index_table(text, &lex)
            && let Some((schema, name)) = self.table_literals(&table)
        {
            let q = format!(
                "SELECT ENGINE FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
            );
            if let Ok(got) = self.run_checked(cx, &q, false).await
                && got
                    .rows
                    .first()
                    .and_then(|r| col(r, 0))
                    .is_some_and(|e| mysqlddl::engine_lacks_descending(&e))
            {
                return refused(mysqlddl::no_descending());
            }
            return DdlCheck::Run;
        }
        if let Some((table, at, dropped)) = mysqlddl::dropped_columns(text, &lex)
            && let Some((schema, name)) = self.table_literals(&table)
        {
            let q = format!(
                "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, COLLATION, NON_UNIQUE, \
                 INDEX_TYPE, INDEX_COMMENT, IS_VISIBLE FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
            );
            let Ok(got) = self.run_checked(cx, &q, false).await else {
                return DdlCheck::Run;
            };
            let parts: Vec<mysqlddl::KeyPart> = got
                .rows
                .iter()
                .filter_map(|r| {
                    Some(mysqlddl::KeyPart {
                        index: col(r, 0)?,
                        seq: col(r, 1)?.parse().ok()?,
                        column: col(r, 2),
                        sub_part: col(r, 3).and_then(|v| v.parse().ok()),
                        desc: col(r, 4).as_deref() == Some("D"),
                        unique: col(r, 5).as_deref() == Some("0"),
                        index_type: col(r, 6).unwrap_or_default(),
                        comment: col(r, 7).unwrap_or_default(),
                        visible: col(r, 8).as_deref() != Some("NO"),
                    })
                })
                .collect();
            if let Some(sql) =
                mysqlddl::unique_rebuild(text, at, &dropped, &parts, lex.no_backslash_escapes)
            {
                return DdlCheck::Rewritten(Bytes::from(sql));
            }
        }
        DdlCheck::Run
    }

    /// A failed DDL statement's error as MySQL reports it (see the module
    /// docs); `e` itself when nothing applies. `original` is the client's
    /// statement.
    pub(super) async fn mysql_ddl_error(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        e: SprigError,
    ) -> SprigError {
        if self.persona != Persona::MySql84 {
            return e;
        }
        let (sprig_common::ErrorKind::Backend { errno: code, .. }
        | sprig_common::ErrorKind::Standard { errno: code, .. }) = e.kind
        else {
            return e;
        };
        // Functional key parts: errors naming their hidden columns.
        if let Some(m) = Box::pin(self.fx_ddl_error(cx, original, code, &e.message)).await {
            return SprigError::standard(m.code, m.sqlstate, m.message);
        }
        let formed = mysqlddl::is_fk_formed_error(code, &e.message);
        let duplicate = mysqlddl::is_fk_duplicate_error(code, &e.message);
        if !formed && !duplicate {
            return e;
        }
        let (lex, _) = self.lex_opts();
        let Some(fks) = mysqlddl::fk_refs(original, &lex) else {
            return e;
        };
        let nbe = lex.no_backslash_escapes;
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, nbe);
        if formed {
            for (db, table) in &fks.refs {
                // A table referencing itself is the one being created.
                if db.is_none() && table.eq_ignore_ascii_case(&fks.table) {
                    continue;
                }
                let schema = db.as_deref().map_or_else(|| "DATABASE()".to_owned(), lit);
                let q = format!(
                    "SELECT COUNT(*) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {}",
                    lit(table)
                );
                match self.run_checked(cx, &q, false).await {
                    Ok(got) if got.rows.first().and_then(|r| col(r, 0)).as_deref() == Some("0") => {
                        let m = mysqlddl::fk_missing_table(db.as_deref(), table);
                        return SprigError::standard(m.code, m.sqlstate, m.message);
                    }
                    Ok(_) => {}
                    Err(_) => return e,
                }
            }
            return e;
        }
        for (i, name) in fks.names.iter().enumerate() {
            if fks.names[..i].iter().any(|n| n.eq_ignore_ascii_case(name)) {
                let m = mysqlddl::fk_duplicate_name(name);
                return SprigError::standard(m.code, m.sqlstate, m.message);
            }
            let q = format!(
                "SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS \
                 WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = {}",
                lit(name)
            );
            match self.run_checked(cx, &q, false).await {
                Ok(got) if got.rows.first().and_then(|r| col(r, 0)).as_deref() != Some("0") => {
                    let m = mysqlddl::fk_duplicate_name(name);
                    return SprigError::standard(m.code, m.sqlstate, m.message);
                }
                Ok(_) => {}
                Err(_) => return e,
            }
        }
        e
    }

    /// MySQL's error for a DDL error that MariaDB raised through a
    /// functional key part's hidden column (`_sprig_fx_..`): 1901 becomes
    /// 3758 naming the index; 1054 `in 'GENERATED ALWAYS AS'` from a
    /// `DROP COLUMN` becomes 3108 when one of the table's own generated
    /// columns uses the column, else 3837 (only hidden columns are left,
    /// and the session doesn't show them); 1054 `in 'CHECK'` becomes 3959
    /// and `in 'DEFAULT'` 3766. `None` otherwise, or when the lookup fails.
    async fn fx_ddl_error(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        code: u16,
        message: &str,
    ) -> Option<WireError> {
        let (lex, _) = self.lex_opts();
        if let Some(m) = mysqlddl::functional_index_error(code, message, original, &lex) {
            return Some(m);
        }
        // Every visible column dropped, one of them used by a functional
        // index.
        if code == 1113 {
            let (table, _, dropped) = mysqlddl::dropped_columns(original, &lex)?;
            let generated = Box::pin(self.generated_columns(cx, &table)).await?;
            return mysqlddl::fx_drop_all(code, &dropped, &generated);
        }
        // A dropped column that a check of several columns or another
        // column's default expression uses.
        if let Some((column, check)) = mysqlddl::dropped_dependency_column(code, message) {
            let (table, _, _) = mysqlddl::dropped_columns(original, &lex)?;
            let (schema, name) = self.physical_written(&table)?;
            let checks = if check {
                Box::pin(self.table_checks(cx, &schema, &name)).await?
            } else {
                Vec::new()
            };
            if let Some(m) = mysqlddl::drop_dependency(column, check, &name, &checks) {
                return Some(m);
            }
            // Only a functional key part's hidden check names it: MySQL's
            // answer for the part (or for a generated column using it too).
            if !check {
                return None;
            }
            let generated = Box::pin(self.generated_columns(cx, &table)).await?;
            return mysqlddl::generated_dependency_of(&[column.to_owned()], &generated);
        }
        let column = mysqlddl::generated_dependency_column(code, message)?;
        let (table, _, _) = mysqlddl::dropped_columns(original, &lex)?;
        let (schema, name) = self.table_literals(&table)?;
        let q = format!(
            "SELECT GENERATION_EXPRESSION FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name} \
             AND GENERATION_EXPRESSION IS NOT NULL"
        );
        let got = self.run_checked(cx, &q, false).await.ok()?;
        let user_column = got
            .rows
            .iter()
            .filter_map(|r| col(r, 0))
            .any(|x| mysqlddl::expression_uses(&x, column));
        Some(mysqlddl::generated_dependency(column, user_column))
    }

    /// The physical schema of database `db` (the session's when `None`)
    /// and table `table`, for statements on the session's backend
    /// connection. `None` for an engine database: MariaDB holds none of
    /// its tables, and the engine answers these checks itself.
    fn physical_table(&self, db: Option<&str>, table: &str) -> Option<(String, String)> {
        let ts = self.tenant.as_deref()?;
        let db = match db {
            Some(d) => d.to_owned(),
            None => self.current_logical()?,
        };
        if self.engine_database_named(&db) {
            return None;
        }
        Some((ts.schema_of(Some(&db))?, table.to_owned()))
    }

    /// The physical schema and name of `table` as written (perhaps
    /// qualified, quoted).
    pub(super) fn physical_written(&self, table: &str) -> Option<(String, String)> {
        let (lex, _) = self.lex_opts();
        let names: Vec<String> = sprig_sql::lex::lex(table.as_bytes(), &lex)
            .ok()?
            .iter()
            .filter(|t| t.kind.is_ident_like())
            .map(|t| {
                String::from_utf8_lossy(&sprig_sql::lex::ident_value(t.bytes(table.as_bytes())))
                    .into_owned()
            })
            .collect();
        match names.as_slice() {
            [t] => self.physical_table(None, t),
            [d, t] => self.physical_table(Some(d), t),
            _ => None,
        }
    }

    /// What may use a column of `table` (as written): its name, its
    /// columns with their generation expressions and defaults (hidden ones
    /// included), and its table-level check constraints, read from
    /// MariaDB's `information_schema` on the session's backend connection.
    /// `None` when they can't be read.
    pub(super) async fn column_uses(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &str,
    ) -> Option<(
        String,
        Vec<mysqlddl::TableColumn>,
        Vec<mysqlddl::TableCheck>,
    )> {
        let (lex, _) = self.lex_opts();
        let (schema, name) = self.physical_written(table)?;
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        let rows = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT COLUMN_NAME, GENERATION_EXPRESSION, COLUMN_DEFAULT \
                     FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} \
                     ORDER BY ORDINAL_POSITION",
                    lit(&schema),
                    lit(&name)
                ),
            )
            .await
            .ok()?;
        let columns = rows
            .into_iter()
            .filter_map(|r| {
                Some(mysqlddl::TableColumn {
                    name: r.first()?.clone()?,
                    generation: r.get(1)?.clone(),
                    default: r.get(2)?.clone(),
                })
            })
            .collect();
        let checks = self.table_checks(cx, &schema, &name).await?;
        Some((name, columns, checks))
    }

    /// The table-level check constraints of physical `schema`.`name`,
    /// without functional key parts' hidden checks (their column's
    /// dependencies are the part's).
    async fn table_checks(
        &mut self,
        cx: &mut ExecCtx<'_>,
        schema: &str,
        name: &str,
    ) -> Option<Vec<mysqlddl::TableCheck>> {
        let (lex, _) = self.lex_opts();
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        let rows = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT CONSTRAINT_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS \
                     WHERE CONSTRAINT_SCHEMA = {} AND TABLE_NAME = {} AND LEVEL = 'Table'",
                    lit(schema),
                    lit(name)
                ),
            )
            .await
            .ok()?;
        Some(
            rows.into_iter()
                .filter_map(|r| {
                    Some(mysqlddl::TableCheck {
                        name: r.first()?.clone()?,
                        clause: r.get(1)?.clone()?,
                    })
                })
                .filter(|k| sprig_sql::compat::fxindex::fx_column_of_check(&k.name).is_none())
                .collect(),
        )
    }

    /// The generated columns of `table` (as written, perhaps qualified),
    /// hidden ones included: name and `GENERATION_EXPRESSION`, read on the
    /// session's backend connection. `None` when they can't be read.
    pub(super) async fn generated_columns(
        &mut self,
        cx: &mut ExecCtx<'_>,
        table: &str,
    ) -> Option<Vec<(String, String)>> {
        let (lex, _) = self.lex_opts();
        let (schema, name) = self.physical_written(table)?;
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        let q = format!(
            "SELECT COLUMN_NAME, GENERATION_EXPRESSION FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND GENERATION_EXPRESSION IS NOT NULL",
            lit(&schema),
            lit(&name)
        );
        let rows = self.backend_rows(cx, &q).await.ok()?;
        Some(
            rows.into_iter()
                .filter_map(|r| Some((r.first()?.clone()?, r.get(1)?.clone()?)))
                .collect(),
        )
    }

    /// MySQL's error for an `INSERT`, `REPLACE`, `UPDATE` or `LOAD DATA`
    /// (`original`, the client's text) that failed storing a value in a
    /// functional key part's hidden column: 3752 (out of range) or 3751,
    /// naming the index, which the table's `STATISTICS` on the backend
    /// connection give. `e` itself for other errors, on the MariaDB persona,
    /// or when the lookup fails (the index is then `functional_index`).
    /// An error raised while computing a hidden column's expression (a
    /// failed cast or date conversion, which doesn't name the column) is
    /// mapped the same way once the table's columns show which key part
    /// raised it ([`Self::fx_eval_dml_error`]).
    pub(super) async fn fx_dml_error(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        e: SprigError,
    ) -> SprigError {
        if self.persona != Persona::MySql84 {
            return e;
        }
        let Some(v) = mysqlddl::fx_value_error(e.protocol().mysql_errno, &e.message) else {
            return Box::pin(self.fx_eval_dml_error(cx, original, e)).await;
        };
        let (lex, _) = self.lex_opts();
        let index = match sprig_sql::compat::errmap::dml_target(original, &lex)
            .and_then(|(db, t)| self.physical_table(db.as_deref(), &t))
        {
            Some((schema, name)) => {
                let lit =
                    |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
                let q = format!(
                    "SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                    lit(&schema),
                    lit(&name)
                );
                let parts: Vec<(String, String)> = self
                    .backend_rows(cx, &q)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|r| Some((r.first()?.clone()?, r.get(1)?.clone()?)))
                    .filter(|(_, c)| sprig_sql::compat::fxindex::is_fx_column(c))
                    .collect();
                mysqlddl::fx_index_name(&parts, &v.column)
            }
            None => None,
        };
        let m =
            mysqlddl::functional_index_value(index.as_deref().unwrap_or("functional_index"), &v);
        fx_wire_error(&e, m)
    }

    /// [`Self::fx_dml_error`] for an error MariaDB raised while computing
    /// an expression ([`mysqlddl::fx_eval_error`]): the table's columns and
    /// key parts, read on the session's backend connection, name the
    /// functional index whose expression raised it, and MySQL's 3751 or
    /// 3752 names it. `e` itself when no key part's expression can raise
    /// it, when one of the table's own generated columns or the statement
    /// itself can, or when the lookup fails.
    async fn fx_eval_dml_error(
        &mut self,
        cx: &mut ExecCtx<'_>,
        original: &[u8],
        e: SprigError,
    ) -> SprigError {
        let Some(v) = mysqlddl::fx_eval_error(e.protocol().mysql_errno, &e.message) else {
            return e;
        };
        if mysqlddl::statement_converts(original, &v.kind) {
            return e;
        }
        let (lex, _) = self.lex_opts();
        let Some((schema, name)) = sprig_sql::compat::errmap::dml_target(original, &lex)
            .and_then(|(db, t)| self.physical_table(db.as_deref(), &t))
        else {
            return e;
        };
        let lit = |v: &str| sprig_sql::names::rewrite::sql_string(v, lex.no_backslash_escapes);
        let (schema, name) = (lit(&schema), lit(&name));
        let Ok(columns) = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT COLUMN_NAME, DATA_TYPE, GENERATION_EXPRESSION \
                     FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
            )
            .await
        else {
            return e;
        };
        let columns: Vec<(String, String, Option<String>)> = columns
            .into_iter()
            .filter_map(|r| Some((r.first()?.clone()?, r.get(1)?.clone()?, r.get(2)?.clone())))
            .collect();
        let Ok(parts) = self
            .backend_rows(
                cx,
                &format!(
                    "SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA = {schema} AND TABLE_NAME = {name}"
                ),
            )
            .await
        else {
            return e;
        };
        let parts: Vec<(String, String)> = parts
            .into_iter()
            .filter_map(|r| Some((r.first()?.clone()?, r.get(1)?.clone()?)))
            .filter(|(_, c)| sprig_sql::compat::fxindex::is_fx_column(c))
            .collect();
        let Some(column) = mysqlddl::fx_eval_part(&v, &columns, &parts) else {
            return e;
        };
        let index =
            mysqlddl::fx_index_name(&parts, &column).unwrap_or_else(|| "functional_index".into());
        let value = mysqlddl::FxValueError {
            column,
            row: v.row,
            out_of_range: v.kind == mysqlddl::FxEvalKind::DecimalRange,
        };
        fx_wire_error(&e, mysqlddl::functional_index_value(&index, &value))
    }
}

/// `m` as an error of `e`'s kind (from the backend or the gateway), keeping
/// whether `e` was retryable.
fn fx_wire_error(e: &SprigError, m: WireError) -> SprigError {
    let mut out = match e.kind {
        sprig_common::ErrorKind::Backend { .. } => {
            SprigError::backend(m.code, m.sqlstate, m.message)
        }
        _ => SprigError::standard(m.code, m.sqlstate, m.message),
    };
    out.retryable = e.retryable;
    out
}
