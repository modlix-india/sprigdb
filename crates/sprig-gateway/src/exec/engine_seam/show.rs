//! `SHOW` statements about tables, answered by the storage engine: the
//! statement as the name layer rewrote it (physical schema names) is
//! answered in MariaDB's shape (`crate::engine::show`), and the caller
//! sends that through the statement's name mapping as it would a
//! backend's answer.

use std::sync::Arc;

use sprig_common::Result;
use sprig_common::ids::DbId;
use sprig_common::names::SchemaName;
use sprig_sql::names::show::{ShowKind, classify, like_match};

use crate::engine::show::{self, Answer};
use crate::engine::{self, catalog, define};
use crate::exec::PassSession;

impl PassSession {
    /// The engine's answer to `sql` (a rewritten `SHOW`), when it is a
    /// `SHOW` about a table, or the tables, of one of the tenant's
    /// databases. `None`: not the engine's (the backend answers, as
    /// for a table that doesn't exist).
    pub(in crate::exec) async fn engine_show(&self, sql: &[u8]) -> Option<Result<Answer>> {
        let lex = sprig_sql::lex::LexOptions::mariadb();
        let toks: Vec<_> = sprig_sql::lex::lex(sql, &lex)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        let Some(info) = classify(sql, &toks, false) else {
            return self.engine_infoschema(sql).await;
        };
        if !matches!(
            info.kind,
            ShowKind::CreateTable | ShowKind::Columns | ShowKind::Index | ShowKind::Tables
        ) || info.filter.is_some()
        {
            return None;
        }
        let ts = self.tenant.as_deref()?;
        let cat = Arc::clone(ts.catalog()?);
        let eng = catalog::of(&cat)?;
        let lctn = ts.lower_case_table_names();
        let schema_name = match &info.schema {
            Some(s) => s.clone(),
            None => self.bs.as_ref()?.vars().schema.clone()?,
        };
        let schema: SchemaName = schema_name.parse().ok()?;
        if schema.tenant() != ts.tenant {
            return None;
        }
        let db: DbId = schema.db()?;
        if !eng.dbs.contains(&db) {
            return None;
        }
        let ci = lctn;
        if info.kind == ShowKind::Tables {
            let mut names: Vec<String> = eng
                .by_name
                .keys()
                .filter(|(d, n)| *d == db && !n.starts_with("__sprig_"))
                .map(|(_, n)| n.clone())
                .filter(|n| info.like.as_deref().is_none_or(|p| like_match(p, n, ci)))
                .collect();
            names.sort();
            return Some(Ok(show::tables(&schema_name, &names, info.full)));
        }
        let name = info.object.clone()?;
        let parsed = eng.table(db, &name, lctn)?;
        let td = parsed.as_ref().as_ref().ok()?;
        let text = eng.texts.get(&td.object)?;
        let def = match define::parse_stored(text) {
            Ok(d) => d,
            Err(e) => return Some(Err(e)),
        };
        let e = match engine::engine() {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        Some(match info.kind {
            ShowKind::CreateTable => match e.next_counter(ts.tenant.0, td).await {
                Ok(next) => Ok(show::create_table(&def.name, &show::with_counter(&def, next))),
                Err(x) => Err(x),
            },
            ShowKind::Columns => {
                let pat = info.like.clone();
                let f = move |n: &str| pat.as_deref().is_none_or(|p| like_match(p, n, true));
                Ok(show::columns(&def, info.full, Some(&f)))
            }
            _ => match e.row_count(ts.tenant.0, td, 1_000_000).await {
                Ok(n) => Ok(show::index(&def, n)),
                Err(x) => Err(x),
            },
        })
    }
}

impl PassSession {
    /// Sends the engine's `SHOW` answer as `forward` sends a backend's.
    #[allow(clippy::too_many_arguments)] // the forward path's own arguments
    pub(in crate::exec) async fn engine_show_answer(
        &mut self,
        cx: &mut crate::exec::ExecCtx<'_>,
        answer: Result<Answer>,
        more: bool,
        format: crate::pipeline::RowFormat,
        mut mapper: sprig_sql::names::ResultMapper,
        map: &dyn sprig_sql::names::NameMap,
        out: &mut dyn crate::pipeline::ResultSink,
    ) -> Result<crate::exec::tenant::Step> {
        use sprig_proto_mysql::StatusFlags;
        let answer = match answer {
            Ok(a) => a,
            Err(e) => {
                return self.fail(out, &e).await;
            }
        };
        let accounts = crate::tenant::names::Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
        let aliases = self
            .tenant
            .as_deref()
            .map(crate::tenant::session::TenantSession::aliases);
        let mut names = crate::exec::relay::NamesOut {
            mapper: &mut mapper,
            ctx: sprig_sql::names::OutputCtx {
                map,
                accounts: Some(&accounts),
                lex: sprig_sql::lex::LexOptions::mariadb(),
            },
            aliases: aliases.as_ref(),
        };
        let renames = Vec::new();
        let r = crate::exec::relay::Relay {
            more,
            format,
            cursor: false,
            fetch: false,
            backend_caps: self.backend_caps(cx),
            infile: crate::exec::relay::Infile { name: None, allowed: false },
            conn_status: None,
            renames: &renames,
            mysql_nowait: false,
        };
        let autocommit = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().vars.get("autocommit").cloned())
            .is_none_or(|v| !(v == "0" || v.eq_ignore_ascii_case("OFF")));
        let mut status = StatusFlags::empty();
        if autocommit {
            status |= StatusFlags::AUTOCOMMIT;
        }
        if answer.no_index {
            status |= StatusFlags::NO_INDEX_USED;
        }
        let relayed = crate::exec::relay::relay_local(out, &r, &answer.defs, answer.rows, status, Some(&mut names)).await?;
        Ok(crate::exec::tenant::Step {
            errored: false,
            status: relayed.status,
        })
    }
}

impl PassSession {
    /// The engine's `SHOW CREATE TABLE` text for `sql` (a rewritten `SHOW
    /// CREATE TABLE`), when the table is the engine's.
    pub(in crate::exec) async fn engine_show_create_text(&self, sql: &[u8]) -> Option<Result<String>> {
        let answer = match self.engine_show(sql).await? {
            Ok(a) => a,
            Err(e) => return Some(Err(e)),
        };
        let row = answer.rows.first()?;
        let vals = sprig_proto_mysql::resultset::decode_text_row(row, 2).ok()?;
        let text = vals.get(1).copied().flatten()?;
        Some(Ok(String::from_utf8_lossy(text).into_owned()))
    }
}

impl PassSession {
    /// The engine's answer to an `information_schema` query about tables
    /// (`crate::engine::infoschema`).
    async fn engine_infoschema(&self, sql: &[u8]) -> Option<Result<Answer>> {
        let text = std::str::from_utf8(sql).ok()?;
        if !crate::engine::infoschema::reads_tables(text) {
            if text.to_ascii_lowercase().contains("information_schema") {
                tracing::info!(sql = text, "information_schema: not one of the engine's tables");
            }
            return None;
        }
        let ts = self.tenant.as_deref()?;
        let cat = Arc::clone(ts.catalog()?);
        let eng = catalog::of(&cat)?;
        let e = engine::engine().ok()?;
        let mut counters = std::collections::HashMap::new();
        for p in eng.tables.values() {
            if let Ok(td) = p.as_ref().as_ref()
                && td.auto_inc.is_some()
                && let Ok(Some(n)) = e.next_counter(ts.tenant.0, td).await
            {
                counters.insert(td.object.0, n);
            }
        }
        let src = crate::engine::infoschema::Source {
            eng,
            tenant: ts.tenant.0,
            counters: &counters,
        };
        let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
        crate::engine::infoschema::answer(text, &src, mysql).map(Ok)
    }
}
