//! Event streams in a tenant session: stream DDL, publishing and the SQL
//! reads, for streams stored on the node's engine.
//!
//! Before the name layer's plan, each statement of a tenant text goes to
//! [`PassSession::engine_streams`]: `CREATE | ALTER | DROP STREAM`, `SHOW
//! STREAMS`, `INSERT INTO <stream>` (inside the session's engine
//! transaction when one is open) and `SELECT ... FROM <stream>`
//! ([`select`]).

mod select;

use std::sync::Arc;

use bytes::Bytes;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};

/// A bound value of a prepared publish as a literal: text as
/// `_utf8mb4 X'..'` (the engine tier's form), other bytes as `X'..'`, so
/// no `sql_mode` changes them and the stream layer measures them exactly.
/// Numbers, times and `NULL` as for a prepared search.
pub(super) fn param_literal(p: &sprig_proto_mysql::command::BoundParam) -> String {
    use sprig_proto_mysql::Value;
    use sprig_proto_mysql::command::ParamValue;
    use std::fmt::Write as _;
    let ParamValue::Value(Value::Bytes(b)) = &p.value else {
        return super::search::param_literal(p);
    };
    let mut h = String::with_capacity(b.len() * 2 + 12);
    if std::str::from_utf8(b).is_ok() {
        h.push_str("_utf8mb4 ");
    }
    h.push_str("X'");
    for x in b {
        let _ = write!(h, "{x:02X}");
    }
    h.push('\'');
    h
}

impl PassSession {
    fn streams_status(&self, more: bool) -> StatusFlags {
        let s = self.status();
        if more {
            s | StatusFlags::MORE_RESULTS_EXISTS
        } else {
            s
        }
    }

    /// The streams step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't the stream layer's.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tenant_streams(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        _attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        self.engine_streams(cx, text, more, format, cat, out).await
    }

    /// Whether `sql` publishes into a stream through the prepared-statement
    /// path: never; prepared statements on streams answer 1235.
    #[allow(clippy::unused_self)]
    pub(super) fn stream_publish(&self, _sql: &[u8]) -> bool {
        false
    }

    /// `COM_STMT_PREPARE` of a publish into a stream: none (see
    /// [`Self::stream_publish`]).
    #[allow(clippy::unused_self)]
    pub(super) fn streams_prepare(
        &mut self,
        _sql: &Bytes,
        _charset: u16,
    ) -> Option<crate::pipeline::ExecSummary> {
        None
    }

    /// After a successful `ALTER TABLE t SPRIG SET | RESET (...)`: nothing.
    /// Tables' change topics (`stream.change_topic`) wait for the engine's
    /// change log.
    #[allow(clippy::unused_self, clippy::unused_async)]
    pub(super) async fn streams_after_settings(&mut self, _text: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// What the engine streams' statements ask of the session.
struct EngineStreamHost<'s> {
    sess: &'s PassSession,
    cat: &'s TenantCatalog,
    tenant: u64,
}

impl sprig_streams::seg::sql::SqlHost for EngineStreamHost<'_> {
    fn tenant(&self) -> u64 {
        self.tenant
    }

    fn current_db(&self) -> Option<String> {
        self.sess.current_logical()
    }

    fn on_engine(&self, db: &str) -> bool {
        crate::kafka::engine::tenants().has(self.tenant) || self.sess.engine_database_named(db)
    }

    fn allowed(&self, db: &str, stream: &str, need: sprig_streams::seg::kafka::Need) -> bool {
        use sprig_streams::seg::kafka::Need;
        let Some(ts) = self.sess.tenant.as_deref() else {
            return false;
        };
        let grants = crate::tenant::authz::privileges(ts, self.cat);
        if grants.unrestricted() {
            return true;
        }
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(db, lctn);
        let a = match need {
            Need::Read => Action::Select,
            Need::Write => Action::Insert,
            Need::Create => Action::Create,
            Need::Alter => Action::Alter,
            Need::Drop => Action::Drop,
        };
        if !stream.is_empty() {
            let name_key = object_key(ObjectKind::Table, stream, lctn);
            if grants.allows(
                a,
                Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            ) {
                return true;
            }
        }
        grants.allows(a, Target::Database { db_key: &db_key })
    }

    fn user(&self) -> (String, String) {
        self.sess
            .tenant
            .as_deref()
            .map_or_else(Default::default, |t| {
                (t.principal.user.clone(), t.principal.client_ip.to_string())
            })
    }

    fn in_transaction(&self) -> bool {
        self.sess.offsets_in_transaction()
    }

    fn engine_transaction(&self) -> bool {
        self.sess.eng.txn.is_open()
    }
}

/// Whether `text` can only be a stream statement (`CREATE | ALTER | DROP
/// STREAM`, `SHOW STREAMS`); an `INSERT`, `DROP DATABASE` or a read may as
/// well be about tables.
fn surely_stream_statement(text: &[u8], lex: sprig_sql::lex::LexOptions) -> bool {
    use sprig_streams::grammar::Stmt;
    matches!(
        sprig_streams::grammar::classify(text, &lex),
        Ok(Stmt::Create(_) | Stmt::Alter(_) | Stmt::Drop { .. } | Stmt::Show { .. })
    )
}

impl PassSession {
    /// The stream statements of engine databases (streams stored on the
    /// node's engine): `None` when the statement isn't one, or the tenant
    /// has no engine database.
    #[allow(clippy::too_many_lines)] // one branch per kind of answer
    async fn engine_streams(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        use sprig_streams::seg::sql::SqlHost as _;
        if self
            .tenant
            .as_deref()
            .is_none_or(|ts| ts.types.internal || ts.principal.must_change_password)
        {
            return Ok(None);
        }
        let tenant = cat.tenant.0;
        let any = crate::kafka::engine::tenants().has(tenant)
            || crate::engine::catalog::of(cat).is_some_and(|e| !e.dbs.is_empty());
        if !any || !sprig_streams::grammar::maybe_stream(text, true) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let (lex, _) = self.lex_opts();
        let mut selected: Option<select::Answer> = None;
        let planned = {
            let host = EngineStreamHost {
                sess: self,
                cat,
                tenant,
            };
            let on_any = |q: &str| host.on_engine(q);
            // Cheap first: only open the stream log for a statement that
            // names an engine database (or runs in one).
            let names_engine = host.current_db().is_some_and(|d| on_any(d.as_str()))
                || sprig_streams::grammar::identifiers(text, &lex)
                    .iter()
                    .any(|w| on_any(w.as_str()));
            if !names_engine {
                return Ok(None);
            }
            let Some(eng) = crate::kafka::engine::streams() else {
                // The stream log failed to open: only statements that are
                // surely about streams fail; the rest (tables, documents,
                // anything else on the engine database) go on.
                if !surely_stream_statement(text, lex) {
                    return Ok(None);
                }
                return Ok(Some(
                    self.fail(
                        out,
                        &SprigError::internal(
                            "streams of engine databases are stored on the SprigDB engine, which \
                             failed to start on this node; see the node's log",
                        ),
                    )
                    .await?,
                ));
            };
            match sprig_streams::seg::sql::plan(&eng, sql, &lex, &host) {
                Ok(Some(p)) => Ok(Some((eng, p))),
                // A query may read streams.
                Ok(None) => {
                    let persona = if self.persona == sprig_proto_mysql::Persona::MySql84 {
                        sprig_exec::Persona::MySql
                    } else {
                        sprig_exec::Persona::MariaDb
                    };
                    select::run(&eng, &host, sql, lex, persona, cx.proto.collation).map(|a| {
                        selected = a;
                        None
                    })
                }
                Err(e) => Err(e),
            }
        };
        if let Some((columns, rows)) = selected.take() {
            if format == RowFormat::Binary {
                return Ok(Some(
                    self.fail(
                        out,
                        &SprigError::standard(
                            1235,
                            *b"42000",
                            "This version of SprigDB doesn't yet support 'a prepared SELECT that \
                             reads a stream of an engine database'",
                        ),
                    )
                    .await?,
                ));
            }
            let status = self.streams_status(more);
            let rows = sprig_types::Rows { columns, rows };
            super::types::send_rows(out, &rows, format, status).await?;
            return Ok(Some(Step {
                errored: false,
                status: Some(status),
            }));
        }
        let (eng, plan) = match planned {
            Ok(None) => return Ok(None),
            Ok(Some(x)) => x,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let status = self.streams_status(more);
        let step = Step {
            errored: false,
            status: Some(status),
        };
        // A publish inside the session's engine transaction joins it.
        let in_engine_txn = matches!(plan, sprig_streams::seg::sql::Plan::Publish { .. })
            && self.offsets_in_transaction();
        let ran = if in_engine_txn {
            match self.eng.txn.open_tx() {
                Some(tx) => {
                    sprig_streams::seg::sql::publish_in(&eng, tenant, plan, tx.engine_txn()).await
                }
                None => Err(SprigError::internal(
                    "stream publish: the session's engine transaction ended",
                )),
            }
        } else {
            sprig_streams::seg::sql::run(&eng, tenant, plan).await
        };
        match ran {
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(sprig_streams::seg::sql::Answer::Continue) => Ok(None),
            Ok(sprig_streams::seg::sql::Answer::Ok(affected)) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = affected;
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(sprig_streams::seg::sql::Answer::Rows { columns, rows }) => {
                let cols: Vec<ColumnDef> = columns
                    .iter()
                    .map(|c| crate::pipeline::text_column(c, cx.proto.collation))
                    .collect();
                let rows = sprig_types::Rows {
                    columns: cols,
                    rows: rows
                        .into_iter()
                        .map(|r| r.into_iter().map(|v| v.map(String::into_bytes)).collect())
                        .collect(),
                };
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
        }
    }
}

#[cfg(test)]
mod engine_stream_tests {
    use super::surely_stream_statement;

    #[test]
    fn only_stream_statements_fail_when_the_stream_log_is_down() {
        let lex = sprig_sql::lex::LexOptions::mariadb();
        for s in [
            "CREATE STREAM s PARTITIONS 2",
            "ALTER STREAM db.s PARTITIONS 4",
            "DROP STREAM IF EXISTS s",
            "SHOW STREAMS FROM db",
        ] {
            assert!(surely_stream_statement(s.as_bytes(), lex), "{s}");
        }
        for s in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "CREATE TABLE t (a INT)",
            "DROP DATABASE db",
            "DROP TABLE t",
            "BEGIN",
            "CREATE STREAM",
        ] {
            assert!(!surely_stream_statement(s.as_bytes(), lex), "{s}");
        }
    }
}
