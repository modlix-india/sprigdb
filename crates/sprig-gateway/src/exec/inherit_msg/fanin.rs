//! `SELECT ... FROM s FAN IN`: the declarer of an inherited stream reads
//! every member's instance together.
//!
//! ```text
//! SELECT ... FROM [db.]s FAN IN ...     (one FAN IN per query)
//!   s           a FOR EACH TENANT stream this tenant declares (else the
//!               statement goes on as usual: 1146 for no such table)
//!   privileges  SPRIG_FAN_IN (1227) and SELECT on db.s (1142)
//!   members     every member below this tenant with an instance (this
//!               tenant too with INCLUDING SELF), at most
//!               inherit.fan_in_sql_max_tenants (39244 above it)
//!   rows        each member's newest 1,000 records, as the stream's
//!               columns with `tenant` (the member's name) first
//!   the query   planned and run over those rows as the stream reads are
//! ```
//!
//! The rows are read when the statement starts: a fan-in read is a look at
//! the members' recent records, not a position kept anywhere.

use std::sync::Arc;

use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::TenantCatalog;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::ops::Row;
use sprig_inherit::msg::{DefKind, DefState, Definition, MsgState};
use sprig_plan::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use sprig_plan::catalog::{Catalog, ColumnDef as PlanColumn, KeyId, TableDef};
use sprig_sql::lex::{LexOptions, lex};
use sprig_streams::seg::kafka::{EngineStreams, decode_stored};

use super::{allows, denied};
use crate::exec::PassSession;
use crate::exec::streams::select;
use crate::inherited;
use crate::tenant::TenantServices;

/// Records read from each member.
const PER_MEMBER: usize = 1000;

/// Bytes read from the log at a time.
const READ_BYTES: usize = 1 << 20;

/// A query's `[db.]s FAN IN`: the definition, and the text without the
/// two words.
pub(super) struct FanIn {
    def: Arc<Definition>,
    db: String,
    sql: String,
}

/// Finds `[db.]s FAN IN` naming a stream definition of `own` in a query.
pub(super) fn find(
    sql: &str,
    lex_opts: &LexOptions,
    own: &MsgState,
    current: Option<&str>,
) -> Option<FanIn> {
    if own.defs.is_empty()
        || !sql
            .as_bytes()
            .windows(3)
            .any(|w| w.eq_ignore_ascii_case(b"fan"))
    {
        return None;
    }
    let toks: Vec<_> = lex(sql.as_bytes(), lex_opts)
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let word = |i: usize, w: &str| {
        toks.get(i)
            .is_some_and(|t| sql[t.range()].eq_ignore_ascii_case(w))
    };
    let ident = |i: usize| {
        toks.get(i).filter(|t| t.kind.is_ident_like()).map(|t| {
            String::from_utf8_lossy(&sprig_sql::lex::ident_value(t.bytes(sql.as_bytes())))
                .into_owned()
        })
    };
    for i in 1..toks.len() {
        if !(word(i, "fan") && word(i + 1, "in")) {
            continue;
        }
        let name = ident(i - 1)?;
        let db = if i >= 3 && word(i - 2, ".") {
            ident(i - 3)?
        } else {
            current?.to_owned()
        };
        let def = own.def(&db, DefKind::Stream, &name)?;
        let (a, b) = (toks[i].range(), toks[i + 1].range());
        let mut text = String::with_capacity(sql.len());
        text.push_str(&sql[..a.start]);
        text.push(' ');
        text.push_str(&sql[b.end..]);
        return Some(FanIn {
            def: Arc::clone(def),
            db,
            sql: text,
        });
    }
    None
}

/// The fan-in table's columns: `tenant`, then the stream's.
fn fan_in_def(db: &str, name: &str) -> TableDef {
    let mut d = select::stream_def(db, name);
    d.columns.insert(
        0,
        PlanColumn::new(
            "tenant",
            ColumnType::VarChar {
                chars: 255,
                collation: sprig_engine::keys::collation::Collation::Utf8mb4Bin,
            },
        )
        .not_null(),
    );
    d.primary = None;
    d
}

/// The one table of a fan-in query.
struct FanInCatalog {
    current: Option<String>,
    def: Arc<TableDef>,
}

impl Catalog for FanInCatalog {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>> {
        (schema.eq_ignore_ascii_case(&self.def.schema) && name.eq_ignore_ascii_case(&self.def.name))
            .then(|| Arc::clone(&self.def))
    }

    fn default_schema(&self) -> Option<String> {
        self.current.clone()
    }
}

/// The members' records, read when the statement started.
struct FanInRows {
    rows: Arc<Vec<Row>>,
}

impl Storage for FanInRows {
    fn table_rows(&self, _table: &TableDef) -> u64 {
        self.rows.len() as u64
    }

    fn rows_in_range(&self, _table: &TableDef, _key: KeyId, _range: &KeyRange) -> u64 {
        self.rows.len() as u64
    }

    fn rows_per_key(&self, _table: &TableDef, _key: KeyId, _parts: usize) -> Option<f64> {
        None
    }

    fn get(
        &self,
        _table: &TableDef,
        _key: &[Value],
        _counters: &Counters,
    ) -> sprig_exec::Result<Option<Row>> {
        Ok(None)
    }

    fn scan(
        &self,
        _table: &TableDef,
        _key: KeyId,
        _ranges: Vec<KeyRange>,
        _reverse: bool,
        _fetch: Fetch,
        counters: Arc<Counters>,
    ) -> sprig_exec::Result<Box<dyn RowCursor>> {
        // Every row: the conditions are checked on each one.
        Ok(Box::new(Cursor {
            rows: Arc::clone(&self.rows),
            at: 0,
            counters,
        }))
    }
}

struct Cursor {
    rows: Arc<Vec<Row>>,
    at: usize,
    counters: Arc<Counters>,
}

impl RowCursor for Cursor {
    fn next(&mut self) -> sprig_exec::Result<Option<Row>> {
        let r = self.rows.get(self.at).cloned();
        if r.is_some() {
            self.at += 1;
            self.counters.add_row();
        }
        Ok(r)
    }
}

/// Member `t`'s newest records of stream `stream` (at most [`PER_MEMBER`]),
/// as fan-in rows.
fn member_rows(eng: &EngineStreams, t: u64, name: &str, stream: u64) -> Result<Vec<Row>> {
    let mut recs: Vec<(i64, u32, i64, Row)> = Vec::new();
    for (p, b) in eng.log().partitions(t, stream) {
        let key = sprig_streams::seg::Key {
            tenant: t,
            stream,
            partition: p,
        };
        let mut at = b
            .log_start
            .max(b.high_watermark - i64::try_from(PER_MEMBER).unwrap_or(i64::MAX));
        while at < b.high_watermark {
            let bytes = match eng.log().read(key, at, READ_BYTES) {
                Ok((_, bytes)) => bytes,
                Err(Ok(_)) => break,
                Err(Err(e)) => return Err(e),
            };
            if bytes.is_empty() {
                break;
            }
            let mut next = at;
            for r in decode_stored(&bytes, at)? {
                next = next.max(r.offset + 1);
                if r.offset < at {
                    continue;
                }
                let mut row = select::row_of(p, &r);
                row.insert(0, Value::str(name));
                recs.push((r.timestamp_ms, p, r.offset, row));
            }
            // Where the batches end (compaction leaves gaps).
            let mut pos = 0;
            while let Some(h) = sprig_streams::seg::batch::Head::parse(&bytes[pos..]) {
                next = next.max(h.next_offset());
                pos += h.total;
                if pos >= bytes.len() {
                    break;
                }
            }
            if next <= at {
                break;
            }
            at = next;
        }
    }
    // The newest, kept in (time, partition, offset) order.
    recs.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    let skip = recs.len().saturating_sub(PER_MEMBER);
    Ok(recs.into_iter().skip(skip).map(|r| r.3).collect())
}

impl PassSession {
    /// Runs a fan-in query (see the module docs); `charset` is the
    /// connection's results character set.
    pub(super) async fn fan_in_select(
        &self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        f: FanIn,
        charset: u16,
    ) -> Result<select::Answer> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let def = &f.def;
        let grants = crate::tenant::authz::privileges(ts, cat);
        if !grants.unrestricted() && !grants.allows(Action::FanIn, AuthzTarget::Tenant) {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SPRIG_FAN_IN privilege(s) for this \
                 operation",
            ));
        }
        if !allows(ts, cat, Action::Select, &f.db, &def.name) {
            return Err(denied(ts, "SELECT", &def.name));
        }
        if def.state != DefState::Active {
            return Err(inherited::dropping(format!("stream {}.{}", f.db, def.name)));
        }
        let eng = crate::kafka::engine::streams().ok_or_else(|| {
            SprigError::internal(
                "streams are stored on the SprigDB engine, which failed to start on this node",
            )
        })?;
        // The members with an instance, below this tenant (and itself with
        // INCLUDING SELF), in tenant order.
        let me = cat.tenant;
        let mut members: Vec<TenantId> = inherited::instances_of(svc, def.key())
            .await?
            .into_iter()
            .map(|e| e.tenant)
            .collect();
        members.sort_unstable();
        members.dedup();
        let max = svc.settings.get(
            &sprig_inherit::msg::settings::FAN_IN_SQL_MAX_TENANTS,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        );
        if members.len() as u64 > max {
            return Err(SprigError::new(
                SprigCode::InheritBroadcastLimit,
                format!(
                    "ER_SPRIG_INHERIT_BROADCAST_LIMIT: {} members have an instance of {}.{}; a \
                     fan-in SELECT reads at most inherit.fan_in_sql_max_tenants = {max}",
                    members.len(),
                    f.db,
                    def.name
                ),
            ));
        }
        let full = format!("{}.{}", def.db_name, def.name);
        let mut rows = Vec::new();
        for t in members {
            if t == me && !def.scope.including_self {
                continue;
            }
            let Some(topic) = eng.topic(t.0, &full)? else {
                continue;
            };
            let Ok(name) = inherited::tenant_name(svc, t).await else {
                continue;
            };
            rows.extend(member_rows(&eng, t.0, &name, topic.stream_id)?);
        }
        let catalog = FanInCatalog {
            current: self.current_logical(),
            def: Arc::new(fan_in_def(&f.db, &def.name)),
        };
        let quoted = sprig_streams::read::quote_columns(&f.sql, &self.lex_opts().0);
        let stmt =
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, &quoted)
                .ok()
                .and_then(|mut v| (v.len() == 1).then(|| v.remove(0)))
                .filter(|s| matches!(s, sqlparser::ast::Statement::Query(_)))
                .ok_or_else(|| {
                    SprigError::standard(
                        1064,
                        *b"42000",
                        "a FAN IN read is a SELECT over one FOR EACH TENANT stream",
                    )
                })?;
        let persona = if self.persona == sprig_proto_mysql::Persona::MySql84 {
            sprig_exec::Persona::MySql
        } else {
            sprig_exec::Persona::MariaDb
        };
        let storage: Arc<dyn Storage> = Arc::new(FanInRows {
            rows: Arc::new(rows),
        });
        select::plan_and_run(&catalog, storage, &stmt, persona, charset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_inherit::msg::{DefState, Scope};

    fn own_with(db: &str, name: &str) -> MsgState {
        let d = Definition {
            declarer: TenantId(5),
            object_id: 9,
            kind: DefKind::Stream,
            db_name: db.into(),
            name: name.into(),
            scope: Scope::default(),
            definition: serde_json::json!({}),
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: 0,
            created_at_ms: 0,
            changed_version: 0,
        };
        let mut st = MsgState::default();
        st.defs.insert(
            (db.to_ascii_lowercase(), DefKind::Stream, name.to_owned()),
            Arc::new(d),
        );
        st
    }

    #[test]
    fn fan_in_names_a_definition_of_this_tenant() {
        let lex = LexOptions::mariadb();
        let own = own_with("shop", "ev");
        let f = find(
            "SELECT tenant FROM ev FAN IN WHERE 1",
            &lex,
            &own,
            Some("shop"),
        )
        .unwrap();
        assert_eq!(
            f.sql.split_whitespace().collect::<Vec<_>>(),
            ["SELECT", "tenant", "FROM", "ev", "WHERE", "1"]
        );
        assert!(find("SELECT * FROM shop.ev fan in", &lex, &own, None).is_some());
        assert!(find("SELECT * FROM other.ev FAN IN", &lex, &own, None).is_none());
        assert!(find("SELECT * FROM ev", &lex, &own, Some("shop")).is_none());
        assert!(find("SELECT 'fan in' FROM ev", &lex, &own, Some("shop")).is_none());
    }
}
