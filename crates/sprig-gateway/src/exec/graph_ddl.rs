//! Graph DDL in a tenant session: `CREATE GRAPH [IF NOT EXISTS] [db.]name` and `DROP GRAPH [IF
//! EXISTS] [db.]name` on 3306 and 3307, through [`crate::graphs`].
//! `CREATE [OR REPLACE] PROPERTY GRAPH` makes a mapped graph;
//! `ALTER GRAPH` answers 39901.
//!
//! SQL/PGQ's `GRAPH_TABLE (g MATCH ... COLUMNS (...))` in any statement:
//! each call runs as a Cypher read through [`GatewayGraphs::query_read`]
//! and is replaced by its rows as a derived table before the pipeline,
//! and the rewritten statement runs as usual.
//!
//! `CREATE GRAPH` needs `CREATE` on the database, `DROP GRAPH` needs
//! `DROP` on the graph or its database; `CREATE PROPERTY
//! GRAPH` also needs `SELECT` on every table it maps.

use bytes::Bytes;
use sprig_auth::authz::Action;
use sprig_catalog::Freshness;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::catalog::GraphDdl;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use sprig_proto_mysql::command::QueryAttrs;

use super::PassSession;
use super::tenant::Step;
use crate::graphs::GatewayGraphs;
use crate::pipeline::ResultSink;
use crate::pipeline::{ExecCtx, RowFormat};

fn mentions_graph(text: &[u8]) -> bool {
    text.len() < 4096 && text.windows(5).any(|w| w.eq_ignore_ascii_case(b"graph"))
}

fn mentions_graph_table(text: &[u8]) -> bool {
    std::str::from_utf8(text).is_ok_and(sprig_graph::graph_table::mentions)
}

/// A graph value as the router takes it.
fn lit(v: &sprig_graph::value::Value) -> sprig_graph::graph_table::GraphLit {
    use sprig_graph::value::Value;
    match v {
        Value::Null => sprig_graph::graph_table::GraphLit::Null,
        Value::Int(i) => sprig_graph::graph_table::GraphLit::Int(*i),
        Value::String(s) => sprig_graph::graph_table::GraphLit::Str(s.clone()),
        other => sprig_graph::graph_table::GraphLit::Other(sprig_graph::pgq::literal(other)),
    }
}

impl PassSession {
    /// `GRAPH_TABLE` in a statement: each call runs now and the statement
    /// continues with its rows in place.
    /// `None` when the text has none.
    pub(super) async fn tenant_graph_table(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !mentions_graph_table(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let calls = match sprig_graph::graph_table::find(sql) {
            Ok(v) if v.is_empty() => return Ok(None),
            Ok(v) => v,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let persona = self.persona;
        let rewritten: Result<String> = async {
            let Some(ts) = self.tenant.as_deref() else {
                return Err(SprigError::internal("no tenant"));
            };
            let graphs = GatewayGraphs::shared(ts.services());
            let cat = graphs.catalog(ts.tenant, Freshness::Latest).await?;
            let grants = graphs.grants(&cat, ts.principal.user_id);
            let user = ts.principal.user.clone();
            let current = self.current_logical();
            let mut s = String::with_capacity(sql.len());
            let mut last = 0;
            for c in calls {
                let db =
                    c.db.clone().or_else(|| current.clone()).ok_or_else(|| {
                        SprigError::standard(1046, *b"3D000", "No database selected")
                    })?;
                let o = graphs
                    .query_read(&cat, grants.as_deref(), &user, &db, &c.graph, &c.cypher)
                    .await?;
                let rows: Vec<Vec<sprig_graph::graph_table::GraphLit>> =
                    o.rows.iter().map(|r| r.iter().map(lit).collect()).collect();
                let body = sprig_graph::graph_table::rows_sql_in(
                    &c.columns,
                    &rows,
                    persona == sprig_proto_mysql::Persona::MySql84,
                );
                if body.len() > sprig_graph::pgq::MAX_INLINE_BYTES {
                    return Err(sprig_graph::exec::budget_error(
                        "rows",
                        format!(
                            "a GRAPH_TABLE result is larger than {} bytes",
                            sprig_graph::pgq::MAX_INLINE_BYTES
                        ),
                    ));
                }
                s.push_str(&sql[last..c.span.0]);
                s.push('(');
                s.push_str(&body);
                s.push(')');
                last = c.span.1;
            }
            s.push_str(&sql[last..]);
            Ok(s)
        }
        .await;
        let rewritten = match rewritten {
            Ok(s) => Bytes::from(s),
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        Box::pin(self.tenant_text(cx, &rewritten, attrs, more, format, out))
            .await
            .map(Some)
    }

    /// The graph DDL step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't graph DDL.
    pub(super) async fn tenant_graph_ddl(
        &mut self,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !mentions_graph(text) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let ddl = match sprig_graph::catalog::parse_ddl(sql) {
            Ok(None) => return Ok(None),
            Ok(Some(d)) => d,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let current = self.current_logical();
        let r = self.graph_ddl(ddl, current).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        if let Err(e) = r {
            return Ok(Some(self.fail(out, &e).await?));
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    #[allow(clippy::too_many_lines)] // one arm per statement
    async fn graph_ddl(&mut self, ddl: GraphDdl, current: Option<String>) -> Result<()> {
        let Some(ts) = self.tenant.as_deref() else {
            return Err(SprigError::internal("no tenant"));
        };
        let graphs = GatewayGraphs::shared(ts.services());
        let (tenant, user) = (ts.tenant, ts.principal.user_id);
        let pick = |db: Option<String>| {
            db.or_else(|| current.clone())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))
        };
        let cat = graphs.catalog(tenant, Freshness::Latest).await?;
        let grants = graphs.grants(&cat, user);
        let denied = |a: &str, db: &str, name: &str| {
            SprigError::standard(
                1142,
                *b"42000",
                format!(
                    "{a} command denied to user '{}' for graph '{db}.{name}'",
                    ts.principal.user
                ),
            )
        };
        match ddl {
            GraphDdl::Create {
                db,
                name,
                if_not_exists,
            } => {
                let db = pick(db)?;
                let lctn = graphs.lctn(&cat);
                let Some(row) = cat.database(&sprig_catalog::ddl::database_key(&db, lctn)) else {
                    return Err(SprigError::standard(
                        1049,
                        *b"42000",
                        format!("Unknown database '{db}'"),
                    ));
                };
                if !graphs.allows(
                    grants.as_deref(),
                    &cat,
                    &db,
                    row.db_id,
                    None,
                    Action::Create,
                ) {
                    return Err(denied("CREATE", &db, &name));
                }
                let (_, v) = graphs.create(tenant, &db, &name, if_not_exists).await?;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    let _ = ts
                        .services()
                        .cache
                        .get(tenant, Freshness::Cached { min_version: v })
                        .await;
                }
                Ok(())
            }
            GraphDdl::CreateProperty(c) => {
                let db = pick(c.db.clone())?;
                let lctn = graphs.lctn(&cat);
                let Some(row) = cat.database(&sprig_catalog::ddl::database_key(&db, lctn)) else {
                    return Err(SprigError::standard(
                        1049,
                        *b"42000",
                        format!("Unknown database '{db}'"),
                    ));
                };
                let db_id = row.db_id;
                if !graphs.allows(grants.as_deref(), &cat, &db, db_id, None, Action::Create) {
                    return Err(denied("CREATE", &db, &c.name));
                }
                let can_read = |o: u64| {
                    graphs.allows(grants.as_deref(), &cat, &db, db_id, Some(o), Action::Select)
                };
                let v = graphs.create_mapped(tenant, &db, &c, &can_read).await?;
                if let Some(ts) = self.tenant.as_deref_mut() {
                    let _ = ts
                        .services()
                        .cache
                        .get(tenant, Freshness::Cached { min_version: v })
                        .await;
                }
                Ok(())
            }
            GraphDdl::Drop {
                db,
                name,
                if_exists,
            } => {
                let db = pick(db)?;
                let g = match graphs.resolve(&cat, &db, &name) {
                    Ok(g) => g,
                    Err(e) if e.code() == Some(SprigCode::GraphNotFound) && if_exists => None,
                    Err(e) => return Err(e),
                };
                if let Some(g) = &g
                    && !graphs.allows(
                        grants.as_deref(),
                        &cat,
                        &g.db,
                        g.db_id,
                        Some(g.graph_id),
                        Action::Drop,
                    )
                {
                    return Err(denied("DROP", &db, &name));
                }
                GatewayGraphs::drop(&graphs, tenant, &db, &name, if_exists).await?;
                Ok(())
            }
        }
    }
}

/// A prepared statement's value as a Cypher literal, for a `?` inside a
/// `GRAPH_TABLE` call: numbers as numbers, text as a
/// quoted string, dates and times as their text; a value Cypher can't
/// write (NaN, infinity, bytes that aren't UTF-8) is refused.
///
/// # Errors
/// 1210 for such a value.
pub(crate) fn cypher_param(p: &sprig_proto_mysql::command::BoundParam) -> Result<String> {
    use sprig_proto_mysql::Value;
    use sprig_proto_mysql::command::ParamValue;
    let bad = |what: &str| {
        SprigError::standard(
            1210,
            *b"HY000",
            format!("Incorrect arguments to EXECUTE: {what} can't be a GRAPH_TABLE parameter"),
        )
    };
    let quote = |t: &str| {
        let mut o = String::with_capacity(t.len() + 2);
        o.push('\'');
        for c in t.chars() {
            match c {
                '\'' => o.push_str("\\'"),
                '\\' => o.push_str("\\\\"),
                _ => o.push(c),
            }
        }
        o.push('\'');
        o
    };
    let float = |f: f64| {
        if f.is_finite() {
            Ok(format!("{f:?}"))
        } else {
            Err(bad("a NaN or infinite number"))
        }
    };
    let ParamValue::Value(v) = &p.value else {
        return Ok("null".to_owned());
    };
    match v {
        Value::Null => Ok("null".to_owned()),
        Value::Int(i) => Ok(i.to_string()),
        Value::UInt(u) => Ok(u.to_string()),
        Value::Float(f) => float(f64::from(*f)),
        Value::Double(f) => float(*f),
        Value::Bytes(b) => std::str::from_utf8(b)
            .map(quote)
            .map_err(|_| bad("bytes that aren't UTF-8")),
        Value::DateTime(d) => Ok(quote(&format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            d.year, d.month, d.day, d.hour, d.minute, d.second, d.micro
        ))),
        Value::Time(t) => Ok(quote(&format!(
            "{}{}:{:02}:{:02}.{:06}",
            if t.negative { "-" } else { "" },
            u64::from(t.days) * 24 + u64::from(t.hour),
            t.minute,
            t.second,
            t.micro
        ))),
    }
}
