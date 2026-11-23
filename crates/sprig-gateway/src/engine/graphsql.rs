//! The SQL of a mapped property graph over an engine database's tables.
//!
//! A mapped graph ([`sprig_graph::mapped::MappedStore`]) reads its vertex
//! and edge tables with plain `SELECT`s. On a database of the storage
//! engine there is no MariaDB table to send them to: [`EngineSql`] plans
//! and runs each one in process through the general planner, bound to the
//! session's catalog snapshot, each in a transaction of its own (a
//! consistent read, no locks), and hands the rows over as the binary
//! protocol would carry them, so the graph layer reads them as it reads a
//! MariaDB connection's.
//!
//! The caller checks the session's privileges on every source table before
//! the graph is read (as it does for a MariaDB-backed graph), and the
//! column lists of a graph definition come from the engine's definitions
//! ([`EngineSql::columns`]).

use std::sync::Arc;

use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Row, Value as MyValue};
use sprig_catalog::TenantCatalog;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_engine::txn::TxnOptions;
use sprig_proto_mysql::resultset::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::assign::Env;
use super::general::{self, RunArgs, Seed};
use super::meta::{Results, ResultsCs};
use super::plan::{BindCtx, CatalogNames};
use super::{catalog, expr, not_on_engine};

/// Runs a mapped graph's SQL on the engine, for one tenant's catalog
/// snapshot.
pub(crate) struct EngineSql<'c> {
    tenant: TenantId,
    cat: &'c TenantCatalog,
    lower_case: bool,
    mysql: bool,
}

impl<'c> EngineSql<'c> {
    /// A runner over `cat` (the session's snapshot) of `tenant`.
    pub(crate) fn new(
        tenant: TenantId,
        cat: &'c TenantCatalog,
        lower_case: bool,
        mysql: bool,
    ) -> Self {
        Self {
            tenant,
            cat,
            lower_case,
            mysql,
        }
    }

    /// The columns of engine table `table` of database `db` in their
    /// order, as its definition lists them; `None` when there is no such
    /// table.
    pub(crate) fn columns(
        cat: &TenantCatalog,
        db: DbId,
        table: &str,
        lower_case: bool,
    ) -> Option<Vec<String>> {
        let def = catalog::of(cat)?
            .table(db, table, lower_case)?
            .as_ref()
            .as_ref()
            .ok()?
            .clone();
        Some(def.columns.iter().map(|c| c.name.clone()).collect())
    }

    fn names(&self) -> Result<CatalogNames<'_>> {
        Ok(CatalogNames {
            cat: self.cat,
            eng: catalog::of(self.cat)
                .ok_or_else(|| not_on_engine("a mapped graph outside the engine's databases"))?,
            lower_case: self.lower_case,
        })
    }
}

/// A parameter as the general planner takes it.
fn param(v: MyValue) -> Result<super::row::Datum> {
    let v = match v {
        MyValue::NULL => Value::Null,
        MyValue::Int(i) => Value::Int(i),
        MyValue::UInt(u) => Value::UInt(u),
        MyValue::Float(f) => Value::Float(f),
        MyValue::Double(f) => Value::Double(f),
        MyValue::Bytes(b) => Value::Bytes(b.into()),
        other => Value::Bytes(other.as_sql(true).trim_matches('\'').to_owned().into()),
    };
    expr::param_datum(&v)
}

/// A value of the binary protocol as the client library holds it.
fn client_value(v: Value) -> MyValue {
    match v {
        Value::Null => MyValue::NULL,
        Value::Int(i) => MyValue::Int(i),
        Value::UInt(u) => MyValue::UInt(u),
        Value::Float(f) => MyValue::Float(f),
        Value::Double(f) => MyValue::Double(f),
        Value::Bytes(b) => MyValue::Bytes(b.to_vec()),
        Value::DateTime(d) => {
            MyValue::Date(d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro)
        }
        Value::Time(t) => MyValue::Time(t.negative, t.days, t.hour, t.minute, t.second, t.micro),
    }
}

#[async_trait::async_trait]
impl sprig_graph::mapped::SqlRunner for EngineSql<'_> {
    async fn rows(&mut self, sql: &str, params: Vec<MyValue>) -> Result<Vec<Row>> {
        let eng = super::engine()?;
        let mut stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| {
            SprigError::internal(format!(
                "engine: a mapped graph's statement doesn't parse: {e}"
            ))
        })?;
        let (Some(stmt), true) = (stmts.pop(), stmts.is_empty()) else {
            return Err(SprigError::internal(
                "engine: a mapped graph's statement isn't one statement",
            ));
        };
        let g = {
            let names = self.names()?;
            let cx = BindCtx {
                tenant: self.tenant,
                names: &names,
                current: None,
                backslash: true,
            };
            general::bind(&cx, &stmt, Some(sql), None)?.ok_or_else(|| {
                not_on_engine("a mapped graph's statement that reads no engine table")
            })?
        };
        let params = params.into_iter().map(param).collect::<Result<Vec<_>>>()?;
        let seed = Seed::from_vars(self.mysql, &Env::STRICT, |_| None);
        let a = RunArgs {
            tenant: self.tenant.0,
            params: &params,
            seed: &seed,
            deadline: None,
            stop: None,
        };
        let ran = eng
            .general_run(&g, &a, TxnOptions::default(), Env::STRICT)
            .await?;
        let results = Results {
            cs: Some(ResultsCs { id: 224, mbmax: 4 }),
            mysql: self.mysql,
        };
        let layout = general::results::layout(&g, &ran.columns, results);
        let cols: Arc<[mysql_async::Column]> = layout
            .defs
            .iter()
            .map(|d| {
                mysql_async::Column::new(
                    ColumnType::try_from(d.col_type).unwrap_or(ColumnType::MYSQL_TYPE_VAR_STRING),
                )
                .with_name(&d.name)
                .with_flags(ColumnFlags::from_bits_truncate(d.flags))
                .with_character_set(d.charset)
                .with_decimals(d.decimals)
            })
            .collect::<Vec<_>>()
            .into();
        let mut out = Vec::with_capacity(ran.rows.len());
        for r in &ran.rows {
            let mut vals = Vec::with_capacity(r.len());
            for ((v, shape), def) in r.iter().zip(&layout.shapes).zip(&layout.defs) {
                vals.push(client_value(general::results::binary(
                    &g,
                    shape,
                    def,
                    v,
                    &Env::STRICT,
                    results,
                )?));
            }
            out.push(mysql_common::row::new_row(vals, Arc::clone(&cols)));
        }
        Ok(out)
    }
}
