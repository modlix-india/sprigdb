//! `SHOW` statements about tables, answered by the storage engine: the
//! statement as the name layer rewrote it (physical schema names) is
//! answered in MariaDB's shape (`crate::engine::show`), and the caller
//! sends that through the statement's name mapping as it would a
//! backend's answer.

use std::sync::Arc;

use sprig_common::ids::DbId;
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigError};
use sprig_sql::names::show::{ShowKind, classify, like_match};

use crate::engine::show::{self, Answer};
use crate::engine::{self, catalog, define, not_on_engine};
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
        // `ANALYZE`, `CHECK`, `OPTIMIZE` and `REPAIR TABLE`.
        if let Some(a) = self.engine_table_admin(sql, &toks).await {
            return Some(a);
        }
        // `DESCRIBE t [col]` is `SHOW COLUMNS FROM t [LIKE 'col']` (the MySQL
        // port's comes here already rewritten as a query).
        let described = sprig_sql::compat::schema::describe_target(sql, &toks, false).map(|d| {
            sprig_sql::names::show::ShowInfo {
                kind: ShowKind::Columns,
                full: false,
                schema: d.schema,
                object: Some(d.table),
                like: d.like,
                filter: None,
            }
        });
        let Some(info) = described.or_else(|| classify(sql, &toks, false)) else {
            return self.engine_infoschema(sql).await;
        };
        // An engine database has no MariaDB schema to ask.
        if info.kind == ShowKind::CreateDatabase {
            let if_not_exists = toks
                .iter()
                .any(|t| t.bytes(sql).eq_ignore_ascii_case(b"EXISTS"));
            return self.engine_show_create_database(&info, if_not_exists);
        }
        // Stored procedures and functions (`routine`).
        if matches!(
            info.kind,
            ShowKind::CreateProcedure
                | ShowKind::CreateFunction
                | ShowKind::ProcedureStatus
                | ShowKind::FunctionStatus
        ) {
            return self.engine_show_routine(&info, sql);
        }
        // SQL triggers (`routine::trigger`).
        if matches!(info.kind, ShowKind::CreateTrigger | ShowKind::Triggers) {
            return self.engine_show_trigger(&info, sql);
        }
        // SQL events (`routine::event`).
        if matches!(info.kind, ShowKind::CreateEvent | ShowKind::Events) {
            return self.engine_show_event(&info, sql);
        }
        if !matches!(
            info.kind,
            ShowKind::CreateTable
                | ShowKind::CreateView
                | ShowKind::CreateSequence
                | ShowKind::Columns
                | ShowKind::Index
                | ShowKind::Tables
                | ShowKind::TableStatus
        ) {
            return None;
        }
        // `SHOW ... WHERE expr`: the answer's rows the condition keeps.
        if let Some(range) = info.filter.clone() {
            if matches!(
                info.kind,
                ShowKind::CreateTable | ShowKind::CreateView | ShowKind::CreateSequence
            ) {
                return None;
            }
            let mut plain = info.clone();
            plain.filter = None;
            plain.like = None;
            let answer = match self.engine_show_of(&plain).await? {
                Ok(a) => a,
                Err(e) => return Some(Err(e)),
            };
            let cond = std::str::from_utf8(sql.get(range)?).ok()?;
            return crate::engine::infoschema::filter_answer(answer, cond).map(Ok);
        }
        self.engine_show_of(&info).await
    }

    /// `ANALYZE`, `CHECK`, `OPTIMIZE` and `REPAIR TABLE` of engine tables,
    /// with the servers' answers for InnoDB tables: the engine keeps no
    /// statistics to refresh and has nothing to rebuild or repair; `CHECK`
    /// reads every row. `None`: another statement, a form this doesn't
    /// read, or a table of a database that isn't the engine's.
    #[allow(clippy::too_many_lines)] // read the statement, find the tables, one answer per table
    async fn engine_table_admin(
        &self,
        sql: &[u8],
        toks: &[sprig_sql::lex::Token],
    ) -> Option<Result<Answer>> {
        use sprig_proto_mysql::resultset::encode_text_row;
        use sprig_sql::lex::TokenKind;
        let word = |i: usize| {
            toks.get(i)
                .filter(|t| t.kind == TokenKind::Word)
                .map(|t| t.bytes(sql).to_ascii_uppercase())
        };
        let punct = |i: usize, c: &[u8]| {
            toks.get(i)
                .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(sql) == c)
        };
        let op = match word(0)?.as_slice() {
            b"ANALYZE" => "analyze",
            b"CHECK" => "check",
            b"OPTIMIZE" => "optimize",
            b"REPAIR" => "repair",
            _ => return None,
        };
        let mut i = 1;
        while matches!(word(i).as_deref(), Some(b"NO_WRITE_TO_BINLOG" | b"LOCAL")) {
            i += 1;
        }
        if !matches!(word(i).as_deref(), Some(b"TABLE" | b"TABLES")) {
            return None;
        }
        i += 1;
        let mut names: Vec<(Option<String>, String)> = Vec::new();
        loop {
            let a = super::sqlprep::ident(toks.get(i)?, sql)?;
            i += 1;
            if punct(i, b".") {
                let b = super::sqlprep::ident(toks.get(i + 1)?, sql)?;
                i += 2;
                names.push((Some(a), b));
            } else {
                names.push((None, a));
            }
            if !punct(i, b",") {
                break;
            }
            i += 1;
        }
        // The options that change nothing here.
        while let Some(w) = word(i) {
            match (op, w.as_slice()) {
                ("check", b"QUICK" | b"FAST" | b"MEDIUM" | b"EXTENDED" | b"CHANGED")
                | ("repair", b"QUICK" | b"EXTENDED" | b"USE_FRM") => i += 1,
                ("check", b"FOR") if word(i + 1).as_deref() == Some(b"UPGRADE") => i += 2,
                _ => return None,
            }
        }
        if toks.get(i).is_some_and(|_| !punct(i, b";")) {
            return None;
        }
        let ts = self.tenant.as_deref()?;
        let cat = self.eng.temps.overlay(ts.catalog()?);
        let eng = catalog::of(&cat)?;
        let lctn = ts.lower_case_table_names();
        let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
        let current = match self.bs.as_ref().and_then(|b| b.vars().schema.clone()) {
            Some(s) => Some(s),
            None => ts.schema_of(self.current_logical().as_deref()),
        };
        // Every name must be in one of the tenant's engine databases.
        let mut found = Vec::with_capacity(names.len());
        for (db, t) in names {
            let schema_name = match db {
                Some(d) => d,
                None => current.clone()?,
            };
            let schema: SchemaName = schema_name.parse().ok()?;
            if schema.tenant() != ts.tenant {
                return None;
            }
            let db: DbId = schema.db()?;
            if !eng.dbs.contains(&db) {
                return None;
            }
            found.push((schema_name, db, t));
        }
        let e = match engine::engine() {
            Ok(e) => e,
            Err(x) => return Some(Err(x)),
        };
        let mut rows = Vec::new();
        let mut row = |name: &str, ty: &str, text: &str| {
            rows.push(encode_text_row([
                Some(name.as_bytes()),
                Some(op.as_bytes()),
                Some(ty.as_bytes()),
                Some(text.as_bytes()),
            ]));
        };
        for (schema_name, db, t) in found {
            let shown = format!("{schema_name}.{t}");
            if let Some(td) = eng.table(db, &t, lctn) {
                let Ok(td) = td.as_ref().as_ref() else {
                    row(&shown, "status", "OK");
                    continue;
                };
                match op {
                    "check" => match e.row_count(ts.tenant.0, td, u64::MAX).await {
                        Ok(_) => row(&shown, "status", "OK"),
                        Err(x) => {
                            row(&shown, "Error", &x.to_string());
                            row(&shown, "status", "Operation failed");
                        }
                    },
                    "optimize" => {
                        row(
                            &shown,
                            "note",
                            "Table does not support optimize, doing recreate + analyze instead",
                        );
                        row(&shown, "status", "OK");
                    }
                    "repair" if mysql => row(
                        &shown,
                        "note",
                        "The storage engine for the table doesn't support repair",
                    ),
                    _ => row(&shown, "status", "OK"),
                }
            } else if eng.view(db, &t, lctn).is_some() {
                if op == "check" {
                    row(&shown, "status", "OK");
                } else {
                    row(
                        &shown,
                        "Error",
                        &if mysql {
                            format!("'{shown}' is not BASE TABLE")
                        } else {
                            format!("'{shown}' is not of type 'BASE TABLE'")
                        },
                    );
                    row(&shown, "status", "Operation failed");
                }
            } else {
                row(&shown, "Error", &format!("Table '{shown}' doesn't exist"));
                row(&shown, "status", "Operation failed");
            }
        }
        let def = |n: &str, len: u32, ty: u8| show::coldef("", "", n, "", 224, len, ty, 0, 0x27);
        Some(Ok(Answer {
            defs: vec![
                def("Table", 512, 0xfd),
                def("Op", 40, 0xfd),
                def("Msg_type", 40, 0xfd),
                def("Msg_text", 1_572_864, 0xfa),
            ],
            rows,
            no_index: false,
        }))
    }

    /// `SHOW CREATE DATABASE` of an engine database, from its catalog row;
    /// `None`: not the engine's.
    fn engine_show_create_database(
        &self,
        info: &sprig_sql::names::show::ShowInfo,
        if_not_exists: bool,
    ) -> Option<Result<Answer>> {
        let ts = self.tenant.as_deref()?;
        let cat = Arc::clone(ts.catalog()?);
        let eng = catalog::of(&cat)?;
        let schema_name = info.schema.clone()?;
        let schema: SchemaName = schema_name.parse().ok()?;
        if schema.tenant() != ts.tenant {
            return None;
        }
        let db: DbId = schema.db()?;
        if !eng.dbs.contains(&db) {
            return None;
        }
        let row = cat.databases.get(db)?;
        let (charset, collation) = if row.default_charset.is_empty() {
            ("utf8mb4", define::default_collation("utf8mb4"))
        } else {
            (row.default_charset.as_str(), row.default_collation.as_str())
        };
        let text = format!(
            "CREATE DATABASE {}`{}` /*!40100 DEFAULT CHARACTER SET {charset} COLLATE {collation} */",
            if if_not_exists {
                "/*!32312 IF NOT EXISTS*/ "
            } else {
                ""
            },
            schema_name.replace('`', "``"),
        );
        Some(Ok(show::create_database(&schema_name, &text)))
    }

    /// The engine's answer to one `SHOW` about a table or the tables of a
    /// database (no `WHERE`); `None`: not the engine's.
    #[allow(clippy::too_many_lines)] // one answer per SHOW kind
    async fn engine_show_of(
        &self,
        info: &sprig_sql::names::show::ShowInfo,
    ) -> Option<Result<Answer>> {
        let ts = self.tenant.as_deref()?;
        let base = Arc::clone(ts.catalog()?);
        // The session's temporary tables hide tables of the same name.
        let cat = self.eng.temps.overlay(&base);
        let eng = catalog::of(&cat)?;
        let lctn = ts.lower_case_table_names();
        let schema_name = match &info.schema {
            Some(s) => s.clone(),
            // The session's current database: the backend session's schema,
            // or (an engine database has none on MariaDB) its own.
            None => match self.bs.as_ref().and_then(|b| b.vars().schema.clone()) {
                Some(s) => s,
                None => ts.schema_of(self.current_logical().as_deref())?,
            },
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
            // The catalog's tables (a temporary table hides none of them
            // here); MariaDB lists the session's temporary tables first.
            let eng = catalog::of(&base)?;
            let mut temps: Vec<(String, &'static str)> = Vec::new();
            if self.persona != sprig_proto_mysql::packets::Persona::MySql84 {
                temps = self
                    .eng
                    .temps
                    .iter()
                    .filter(|t| t.db == db)
                    .filter(|t| {
                        info.like
                            .as_deref()
                            .is_none_or(|p| like_match(p, &t.name, ci))
                    })
                    .map(|t| (t.name.clone(), "TEMPORARY TABLE"))
                    .collect();
                temps.sort();
            }
            let mut names: Vec<(String, &'static str)> = eng
                .by_name
                .keys()
                .filter(|(d, n)| *d == db && !n.starts_with("__sprig_"))
                .map(|(_, n)| (n.clone(), "BASE TABLE"))
                .chain(
                    eng.views
                        .keys()
                        .filter(|(d, _)| *d == db)
                        .map(|(_, n)| (n.clone(), "VIEW")),
                )
                .chain(
                    eng.seq_names
                        .keys()
                        .filter(|(d, _)| *d == db)
                        .map(|(_, n)| (n.clone(), "SEQUENCE")),
                )
                .filter(|(n, _)| info.like.as_deref().is_none_or(|p| like_match(p, n, ci)))
                .collect();
            names.sort();
            temps.append(&mut names);
            let names = temps;
            return Some(Ok(show::tables_typed(
                &schema_name,
                &names,
                info.full,
                info.like.as_deref(),
            )));
        }
        if info.kind == ShowKind::TableStatus {
            let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
            let sql = table_status_query(&schema_name, info.like.as_deref(), mysql);
            return self.engine_infoschema(sql.as_bytes()).await;
        }
        let name = info.object.clone()?;
        // A sequence: its own text, or the table MariaDB keeps it in.
        if matches!(info.kind, ShowKind::CreateTable | ShowKind::CreateSequence)
            && let Some(s) = eng.sequence(db, &name, lctn)
        {
            let shown = s.def.as_ref().map_or(name.as_str(), |d| d.name.as_str());
            return Some(Ok(show::create_table(
                shown,
                &if info.kind == ShowKind::CreateSequence {
                    s.text.clone()
                } else {
                    crate::engine::sequence::table_text(shown)
                },
            )));
        }
        if info.kind == ShowKind::CreateSequence {
            let db_name = cat
                .databases
                .get(db)
                .map_or_else(String::new, |d| d.name.clone());
            return Some(Err(if eng.table(db, &name, lctn).is_some() {
                SprigError::standard(
                    4090,
                    *b"42S02",
                    format!("'{db_name}.{name}' is not a SEQUENCE"),
                )
            } else {
                SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{db_name}.{name}' doesn't exist"),
                )
            }));
        }
        if matches!(info.kind, ShowKind::CreateTable | ShowKind::CreateView)
            && let Some(v) = eng.view(db, &name, lctn)
        {
            return Some(match v.as_ref() {
                Ok(v) => {
                    let (cs, coll) = self.view_charsets();
                    Ok(show::create_view(&v.name, &v.text, &cs, &coll))
                }
                Err(why) => Err(not_on_engine(why)),
            });
        }
        if info.kind == ShowKind::CreateView {
            let db_name = cat
                .databases
                .get(db)
                .map_or_else(String::new, |d| d.name.clone());
            // A table: the servers' 1347; nothing: 1146.
            let Some(td) = eng.table(db, &name, lctn) else {
                return Some(Err(SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{db_name}.{name}' doesn't exist"),
                )));
            };
            let shown = td
                .as_ref()
                .as_ref()
                .map_or_else(|_| name.clone(), |d| d.name.clone());
            let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
            return Some(Err(SprigError::standard(
                1347,
                *b"HY000",
                if mysql {
                    format!("'{db_name}.{shown}' is not VIEW")
                } else {
                    format!("'{db_name}.{shown}' is not of type 'VIEW'")
                },
            )));
        }
        // Every table of an engine database is the engine's: one it doesn't
        // have doesn't exist (MariaDB has no schema to ask).
        let Some(parsed) = eng.table(db, &name, lctn) else {
            let db_name = cat
                .databases
                .get(db)
                .map_or_else(String::new, |d| d.name.clone());
            return Some(Err(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{db_name}.{name}' doesn't exist"),
            )));
        };
        let td = parsed.as_ref().as_ref().ok()?;
        let text = eng.texts.get(&td.object)?;
        let def = match define::parse_stored(text) {
            Ok(d) => show::visible(d, show_auto_indexes(ts, self.persona)),
            Err(e) => return Some(Err(e)),
        };
        let e = match engine::engine() {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };
        Some(match info.kind {
            ShowKind::CreateTable => match e.next_counter(ts.tenant.0, td).await {
                Ok(next) if eng.temps.contains(&td.object) => Ok(show::create_table(
                    &def.name,
                    &crate::engine::temp::shown(&show::with_counter(&def, next)),
                )),
                Ok(next) => Ok(show::create_table(
                    &def.name,
                    &show::with_counter(&def, next),
                )),
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

/// `SHOW TABLE STATUS [LIKE p]` of database `schema` as a query over
/// `information_schema.TABLES`, in the server's column names (MariaDB
/// adds `Max_index_length` and `Temporary`).
fn table_status_query(schema: &str, like: Option<&str>, mysql: bool) -> String {
    let quote = |t: &str| sprig_sql::names::rewrite::sql_string(t, false);
    let mut cols = vec![
        ("TABLE_NAME", "Name"),
        ("ENGINE", "Engine"),
        ("VERSION", "Version"),
        ("ROW_FORMAT", "Row_format"),
        ("TABLE_ROWS", "Rows"),
        ("AVG_ROW_LENGTH", "Avg_row_length"),
        ("DATA_LENGTH", "Data_length"),
        ("MAX_DATA_LENGTH", "Max_data_length"),
        ("INDEX_LENGTH", "Index_length"),
        ("DATA_FREE", "Data_free"),
        ("AUTO_INCREMENT", "Auto_increment"),
        ("CREATE_TIME", "Create_time"),
        ("UPDATE_TIME", "Update_time"),
        ("CHECK_TIME", "Check_time"),
        ("TABLE_COLLATION", "Collation"),
        ("CHECKSUM", "Checksum"),
        ("CREATE_OPTIONS", "Create_options"),
        ("TABLE_COMMENT", "Comment"),
    ];
    if !mysql {
        cols.push(("MAX_INDEX_LENGTH", "Max_index_length"));
        cols.push(("TEMPORARY", "Temporary"));
    }
    let list = cols
        .iter()
        .map(|(c, n)| format!("`{c}` AS `{n}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let like = like.map_or_else(String::new, |p| {
        format!(" AND `TABLE_NAME` LIKE {}", quote(p))
    });
    format!(
        "SELECT {list} FROM information_schema.TABLES WHERE `TABLE_SCHEMA` = {}{like} ORDER BY `TABLE_NAME`",
        quote(schema)
    )
}

/// Whether the session shows automatic indexes in metadata
/// (`@sprig_show_auto_indexes = 1`, or `@sprig_show_internal`).
fn show_auto_indexes(
    ts: &crate::tenant::session::TenantSession,
    persona: sprig_proto_mysql::packets::Persona,
) -> bool {
    let v = ts.visibility(persona);
    v.auto_indexes || v.internal
}

/// `SHOW [FULL] TABLES` and `SHOW CREATE TABLE` as MySQL 8.4 describes
/// them (MariaDB's shape otherwise): every column in the results
/// character set (`cs`, when `character_set_results` isn't NULL); the
/// table name column 256 bytes long and marked binary; no
/// `SERVER_QUERY_NO_INDEX_USED` for `SHOW TABLES`.
fn mysql_show_meta(
    answer: &mut Answer,
    results: Option<crate::engine::meta::ResultsCs>,
    persona: sprig_proto_mysql::Persona,
    client: sprig_proto_mysql::Caps,
) {
    let cs = results.map(|r| r.id);
    let caps = answer_caps(persona, client);
    // `information_schema` columns described as MySQL 8.4 describes them
    // carry their text columns' character set unset.
    if answer.defs.iter().any(|d| {
        sprig_proto_mysql::ColumnDef::decode(d, caps)
            .is_ok_and(|c| c.charset == crate::engine::infoschema_mysql::TEXT)
    }) {
        for d in &mut answer.defs {
            if let Ok(mut c) = sprig_proto_mysql::ColumnDef::decode(d, caps) {
                crate::engine::infoschema_mysql::fill_text(&mut c, results);
                *d = c.encode(caps);
            }
        }
    }
    let Some(first) = answer
        .defs
        .first()
        .and_then(|d| sprig_proto_mysql::ColumnDef::decode(d, caps).ok())
    else {
        return;
    };
    if mysql_fixed_defs(answer, &first, results, caps) {
        return;
    }
    let tables = &first.org_table[..] == b"TABLE_NAMES";
    let create =
        first.table.is_empty() && (&first.name[..] == b"Table" || &first.name[..] == b"View");
    if !tables && !create {
        return;
    }
    let mut defs = Vec::with_capacity(answer.defs.len());
    for (i, d) in answer.defs.iter().enumerate() {
        let Ok(mut c) = sprig_proto_mysql::ColumnDef::decode(d, caps) else {
            return;
        };
        if let Some(cs) = cs {
            c.charset = cs;
        }
        if tables {
            // MySQL 8.4 names its data dictionary view, not the column.
            c.schema = bytes::Bytes::new();
            c.table = bytes::Bytes::from_static(b"TABLES");
            c.org_table = bytes::Bytes::from_static(b"tables");
            c.org_name = c.name.clone();
        }
        if tables && i == 0 {
            c.length = 256;
            c.flags |= sprig_proto_mysql::resultset::ColumnFlags::BINARY.bits();
        }
        // MySQL 8.4's `Table_type` is its dictionary's enumeration.
        if tables && i == 1 {
            c.col_type = 0xfe;
            c.length = 44;
            c.flags = 0x1181;
        }
        // `SHOW CREATE VIEW`'s `collation_connection`.
        if !tables && i == 3 && &c.name[..] == b"collation_connection" {
            c.length = 128;
        }
        defs.push(c.encode(caps));
    }
    answer.defs = defs;
    if tables {
        answer.no_index = false;
    }
}

/// One column of a MySQL 8.4 `SHOW` result: name, its `org_table`
/// (`""`: none), its `org_name` (`None`: the name), length in characters
/// (`u32::MAX`: the most, whatever the character set), type, flags, and
/// whether it is a number (binary character set, length in bytes).
type FixedCol = (
    &'static str,
    &'static str,
    Option<&'static str>,
    u32,
    u8,
    u16,
    bool,
);

/// MySQL 8.4's `SHOW [FULL] COLUMNS` columns (its data dictionary view
/// `COLUMNS`), as it describes them (observed on MySQL 8.4).
const MYSQL_SHOW_COLUMNS: &[FixedCol] = &[
    ("Field", "", None, 64, 0xfd, 0x0000, false),
    ("Type", "columns", None, 16_777_215, 0xfc, 0x1091, false),
    ("Collation", "", None, 64, 0xfd, 0x0000, false),
    ("Null", "", None, 3, 0xfd, 0x0001, false),
    ("Key", "columns", None, 3, 0xfe, 0x1181, false),
    ("Default", "columns", None, 65_535, 0xfc, 0x0090, false),
    ("Extra", "", None, 256, 0xfd, 0x0000, false),
    ("Privileges", "", None, 154, 0xfd, 0x0000, false),
    ("Comment", "", None, 6_144, 0xfc, 0x0091, false),
];

/// MySQL 8.4's `SHOW INDEX` columns (its view `SHOW_STATISTICS`).
const MYSQL_SHOW_INDEX: &[FixedCol] = &[
    ("Table", "tables", None, 64, 0xfd, 0x1081, false),
    ("Non_unique", "", None, 2, 0x03, 0x0001, true),
    ("Key_name", "", None, 64, 0xfd, 0x0000, false),
    (
        "Seq_in_index",
        "index_column_usage",
        None,
        10,
        0x03,
        0x1021,
        true,
    ),
    ("Column_name", "", None, 64, 0xfd, 0x0000, false),
    ("Collation", "", None, 1, 0xfd, 0x0000, false),
    ("Cardinality", "", None, 21, 0x08, 0x0000, true),
    ("Sub_part", "", None, 21, 0x08, 0x0000, true),
    ("Packed", "", Some(""), 0, 0x06, 0x0080, true),
    ("Null", "", None, 3, 0xfd, 0x0001, false),
    ("Index_type", "", None, 11, 0xfd, 0x0081, false),
    ("Comment", "", None, 8, 0xfd, 0x0001, false),
    ("Index_comment", "indexes", None, 2_048, 0xfd, 0x1081, false),
    ("Visible", "", None, 3, 0xfd, 0x0001, false),
    ("Expression", "", None, u32::MAX, 0xfc, 0x0090, false),
];

/// MySQL 8.4's `SHOW TABLE STATUS` columns (its view `TABLES`).
const MYSQL_SHOW_TABLE_STATUS: &[FixedCol] = &[
    ("Name", "tables", None, 64, 0xfd, 0x1081, false),
    ("Engine", "", None, 64, 0xfd, 0x0000, false),
    ("Version", "", None, 3, 0x03, 0x0000, true),
    ("Row_format", "tables", None, 10, 0xfe, 0x0180, false),
    ("Rows", "", None, 21, 0x08, 0x0020, true),
    ("Avg_row_length", "", None, 21, 0x08, 0x0020, true),
    ("Data_length", "", None, 21, 0x08, 0x0020, true),
    ("Max_data_length", "", None, 21, 0x08, 0x0020, true),
    ("Index_length", "", None, 21, 0x08, 0x0020, true),
    ("Data_free", "", None, 21, 0x08, 0x0020, true),
    ("Auto_increment", "", None, 21, 0x08, 0x0020, true),
    ("Create_time", "tables", None, 19, 0x07, 0x1081, true),
    ("Update_time", "", None, 19, 0x0c, 0x0080, true),
    ("Check_time", "", None, 19, 0x0c, 0x0080, true),
    ("Collation", "collations", None, 64, 0xfd, 0x1000, false),
    ("Checksum", "", None, 21, 0x08, 0x0000, true),
    ("Create_options", "", None, 256, 0xfd, 0x0000, false),
    ("Comment", "", None, 6_144, 0xfc, 0x0010, false),
];

/// The MySQL port's `SHOW COLUMNS`, `SHOW INDEX` and `SHOW TABLE STATUS`
/// answers (the engine's `information_schema` queries the name layer or
/// the seam writes for them) with the column definitions MySQL 8.4 sends.
/// `false`: not one of them.
fn mysql_fixed_defs(
    answer: &mut Answer,
    first: &sprig_proto_mysql::ColumnDef,
    results: Option<crate::engine::meta::ResultsCs>,
    caps: sprig_proto_mysql::Caps,
) -> bool {
    // Known by their first two names (the query's table may be named
    // after a subquery's).
    let second = answer
        .defs
        .get(1)
        .and_then(|d| sprig_proto_mysql::ColumnDef::decode(d, caps).ok())
        .map(|c| c.name.to_ascii_lowercase())
        .unwrap_or_default();
    let (view, cols) = match (&first.name[..], &second[..]) {
        (b"Field", b"type") => ("COLUMNS", MYSQL_SHOW_COLUMNS),
        (b"Table", b"non_unique") => ("SHOW_STATISTICS", MYSQL_SHOW_INDEX),
        (b"Name", b"engine") => ("TABLES", MYSQL_SHOW_TABLE_STATUS),
        _ => return false,
    };
    // Each column of the answer must be one of the view's.
    let mut defs = Vec::with_capacity(answer.defs.len());
    for d in &answer.defs {
        let Ok(mut c) = sprig_proto_mysql::ColumnDef::decode(d, caps) else {
            return false;
        };
        let Some(&(name, org_table, org_name, chars, ty, flags, number)) = cols
            .iter()
            .find(|x| x.0.as_bytes().eq_ignore_ascii_case(&c.name[..]))
        else {
            return false;
        };
        c.name = bytes::Bytes::from_static(name.as_bytes());
        let (cs, mb) = match results {
            Some(r) => (r.id, r.mbmax),
            None => (c.charset, 4),
        };
        c.schema = bytes::Bytes::new();
        // A column MySQL computes (`Packed`) names no table.
        c.table = if org_name == Some("") {
            bytes::Bytes::new()
        } else {
            bytes::Bytes::from_static(view.as_bytes())
        };
        c.org_table = bytes::Bytes::from_static(org_table.as_bytes());
        c.org_name = bytes::Bytes::from_static(org_name.unwrap_or(name).as_bytes());
        c.charset = if number { 63 } else { cs };
        c.length = if number || chars == u32::MAX {
            chars
        } else {
            chars.saturating_mul(mb)
        };
        c.col_type = ty;
        c.flags = flags;
        c.decimals = 0;
        defs.push(c.encode(caps));
    }
    answer.defs = defs;
    // MySQL reads its data dictionary through indexes here.
    answer.no_index = false;
    true
}

/// The MySQL port's probe of the table a `SHOW COLUMNS` or `SHOW INDEX`
/// names, inside the query it becomes (`sprig_sql::compat::schema`).
const PROBE_HEAD: &str = " AND (SELECT 1 FROM ";
const PROBE_TAIL: &str = " LIMIT 0) IS NULL";

/// A query with the probe taken out, and the table it probed.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Probe {
    /// The physical schema.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The query without the probe.
    pub rest: String,
}

/// One backquoted identifier at the start of `s`: its value and the rest.
fn backquoted(s: &str) -> Option<(String, &str)> {
    let mut chars = s.char_indices();
    if chars.next()?.1 != '`' {
        return None;
    }
    let mut v = String::new();
    let b = s.as_bytes();
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'`' {
            if b.get(i + 1) == Some(&b'`') {
                v.push('`');
                i += 2;
                continue;
            }
            return Some((v, &s[i + 1..]));
        }
        let ch = s[i..].chars().next()?;
        v.push(ch);
        i += ch.len_utf8();
    }
    None
}

/// The probe of `sql`, when it has one (see [`PROBE_HEAD`]).
pub(super) fn strip_probe(sql: &str) -> Option<Probe> {
    let at = sql.find(PROBE_HEAD)?;
    let after = &sql[at + PROBE_HEAD.len()..];
    let (schema, after) = backquoted(after)?;
    let after = after.strip_prefix('.')?;
    let (table, after) = backquoted(after)?;
    let after = after.strip_prefix(PROBE_TAIL)?;
    let rest = format!("{}{after}", &sql[..at]);
    Some(Probe {
        schema,
        table,
        rest,
    })
}

/// The capabilities the engine's own `SHOW` and `information_schema`
/// answers are read back with: a backend's, less MariaDB's extended
/// metadata block, which the engine's column definitions never carry
/// (`show::coldef`). Reading them as if they had one fails with 1835
/// ("Malformed communication packet") on the MySQL port, which always
/// asks a backend for that block, and on the MariaDB port for clients
/// that negotiate it.
fn answer_caps(
    persona: sprig_proto_mysql::Persona,
    client: sprig_proto_mysql::Caps,
) -> sprig_proto_mysql::Caps {
    crate::exec::mariadb_caps(persona, client)
        - sprig_proto_mysql::Caps::MARIADB_CLIENT_EXTENDED_METADATA
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
        let mut answer = match answer {
            Ok(a) => a,
            Err(e) => {
                return self.fail(out, &e).await;
            }
        };
        if self.persona == sprig_proto_mysql::Persona::MySql84 {
            let results = self.engine_vars(cx).results;
            mysql_show_meta(&mut answer, results.cs, self.persona, cx.proto.caps);
        }
        let accounts =
            crate::tenant::names::Accounts(self.tenant.as_deref().and_then(|t| t.auth()));
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
            backend_caps: answer_caps(self.persona, cx.proto.caps),
            infile: crate::exec::relay::Infile {
                name: None,
                allowed: false,
            },
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
        let relayed = crate::exec::relay::relay_local(
            out,
            &r,
            &answer.defs,
            answer.rows,
            status,
            Some(&mut names),
        )
        .await?;
        Ok(crate::exec::tenant::Step {
            errored: false,
            status: relayed.status,
        })
    }
}

impl PassSession {
    /// The session's `character_set_client` and `collation_connection`,
    /// as `SHOW CREATE VIEW` shows them.
    #[allow(clippy::redundant_closure_for_method_calls)] // the backend session's type is private
    fn view_charsets(&self) -> (String, String) {
        let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
        let vars = self.bs.as_ref().map(|b| b.vars());
        let get = |k: &str| {
            vars.and_then(|v| v.vars.get(k))
                .map(ToString::to_string)
                .filter(|x| !x.is_empty())
        };
        (
            get("character_set_client").unwrap_or_else(|| "utf8mb4".into()),
            get("collation_connection").unwrap_or_else(|| {
                if mysql {
                    "utf8mb4_0900_ai_ci".into()
                } else {
                    "utf8mb4_uca1400_ai_ci".into()
                }
            }),
        )
    }

    /// The engine's `SHOW CREATE TABLE` text for `sql` (a rewritten `SHOW
    /// CREATE TABLE`), when the table is the engine's.
    pub(in crate::exec) async fn engine_show_create_text(
        &self,
        sql: &[u8],
    ) -> Option<Result<String>> {
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
                tracing::info!(
                    sql = text,
                    "information_schema: not one of the engine's tables"
                );
            }
            return None;
        }
        let ts = self.tenant.as_deref()?;
        // `SHOW COLUMNS` and `SHOW INDEX` of a temporary table read it;
        // other queries list the catalog's tables only.
        let cat = if strip_probe(text).is_some() {
            self.eng.temps.overlay(ts.catalog()?)
        } else {
            Arc::clone(ts.catalog()?)
        };
        let eng = catalog::of(&cat)?;
        // The MySQL port's `SHOW COLUMNS` and `SHOW INDEX` (queries over
        // `COLUMNS` and `STATISTICS`) read the table itself, for 1146 when
        // it doesn't exist: for an engine table the probe goes; for any
        // other the backend answers, as before.
        let stripped;
        let text = match strip_probe(text) {
            Some(p) => {
                let schema: SchemaName = p.schema.parse().ok()?;
                if schema.tenant() != ts.tenant {
                    return None;
                }
                let db = schema.db()?;
                eng.table(db, &p.table, ts.lower_case_table_names())?
                    .as_ref()
                    .as_ref()
                    .ok()?;
                stripped = p.rest;
                stripped.as_str()
            }
            None => text,
        };
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
        // Row counts when the query reads them (`TABLE_ROWS`, `SHOW TABLE
        // STATUS`): each table counted up to a bound, as an estimate.
        let lower = text.to_ascii_lowercase();
        let mut rows = std::collections::HashMap::new();
        if lower.contains("table_rows") || lower.contains("avg_row_length") {
            for p in eng.tables.values() {
                if let Ok(td) = p.as_ref().as_ref()
                    && let Ok(n) = e.row_count(ts.tenant.0, td, ROW_COUNT_BOUND).await
                {
                    rows.insert(td.object.0, n);
                }
            }
        }
        let logic = if text.to_ascii_lowercase().contains("routines")
            || text.to_ascii_lowercase().contains("parameters")
        {
            logic_routines(ts).await
        } else {
            Vec::new()
        };
        let src = crate::engine::infoschema::Source {
            eng,
            tenant: ts.tenant.0,
            counters: &counters,
            rows: &rows,
            logic: &logic,
            show_auto_indexes: show_auto_indexes(ts, self.persona),
        };
        let mysql = self.persona == sprig_proto_mysql::packets::Persona::MySql84;
        crate::engine::infoschema::answer(text, &src, mysql).map(Ok)
    }
}

/// The most rows `information_schema.TABLES` counts in one table for
/// `TABLE_ROWS`; a larger table shows about this many (an estimate, as the
/// servers' own figure is).
const ROW_COUNT_BOUND: u64 = 100_000;

/// `secs` since the Unix epoch as `YYYY-MM-DD hh:mm:ss` (UTC).
fn utc_text(secs: u64) -> String {
    crate::engine::infoschema::utc_datetime(secs)
}

/// The tenant's live logic functions and procedures (engine-tier code,
/// `CREATE FUNCTION ... LANGUAGE kirun | javascript | lua`), as
/// `information_schema.ROUTINES` lists them; none when the logic catalog
/// can't be read.
async fn logic_routines(
    ts: &crate::tenant::session::TenantSession,
) -> Vec<crate::engine::infoschema::LogicRoutine> {
    use crate::engine::infoschema::{LogicRoutine, logic_sql_type};
    let svc = Arc::clone(ts.services());
    let Ok(mut conn) = svc.records.begin(ts.tenant).await else {
        return Vec::new();
    };
    let store = sprig_logic::LogicStore::default();
    let Ok(modules) = store.modules(&mut *conn, None).await else {
        return Vec::new();
    };
    let created = store.created_times(&mut *conn).await.unwrap_or_default();
    let mut out = Vec::new();
    for m in modules {
        let function = match m.kind.as_str() {
            "function" => true,
            "procedure" => false,
            _ => continue,
        };
        if m.state != "active" || m.runtime == "sql" {
            continue;
        }
        let Some(v) = m.active_version else { continue };
        let Ok((sig, _)) = store.version_signature(&mut *conn, m.module_id, v).await else {
            continue;
        };
        let params = sig["params"]
            .as_array()
            .map(|ps| {
                ps.iter()
                    .map(|p| {
                        let name = p["name"].as_str().unwrap_or_default().to_owned();
                        let ty = p["sql"].as_str().map_or_else(
                            || logic_sql_type(p["type"].as_str().unwrap_or_default()).to_owned(),
                            str::to_owned,
                        );
                        (name, ty)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let definer = ts
            .auth()
            .and_then(|a| a.user(sprig_common::ids::UserId(m.created_by)).cloned())
            .map_or_else(String::new, |u| format!("{}@{}", u.row.name, u.row.host));
        out.push(LogicRoutine {
            db: sprig_common::ids::DbId(m.db_id),
            name: m.name,
            function,
            runtime: m.runtime,
            invoker: m.security == "invoker",
            deterministic: sig["deterministic"].as_bool().unwrap_or(false),
            params,
            returns: sig["returns"].as_str().map(str::to_owned),
            definer,
            created: utc_text(created.get(&m.module_id).copied().unwrap_or(0) / 1000),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn creation_times_print_in_utc() {
        assert_eq!(super::utc_text(0), "1970-01-01 00:00:00");
        assert_eq!(super::utc_text(1_795_550_501), "2026-11-24 20:01:41");
        assert_eq!(super::utc_text(951_782_400), "2000-02-29 00:00:00");
    }

    use bytes::Bytes;
    use futures::StreamExt;
    use sprig_proto_mysql::packets::Persona;
    use sprig_proto_mysql::resultset::{decode_column_count, decode_text_row};
    use sprig_proto_mysql::{Caps, ColumnDef, PacketCodec, StatusFlags};
    use tokio_util::codec::Framed;

    use super::answer_caps;
    use crate::engine::show;
    use crate::exec::relay::{Infile, Relay, relay_local};
    use crate::mysql::sink::WireSink;
    use crate::pipeline::RowFormat;

    fn answers() -> Vec<show::Answer> {
        vec![
            show::tables("t61306000000_d1", &["a".into(), "b".into()], false, None),
            show::tables("t61306000000_d1", &["a".into()], true, Some("a%")),
            show::create_table("a", "CREATE TABLE `a` (\n  `id` int(11) NOT NULL\n)"),
        ]
    }

    const BASE: Caps = Caps::PROTOCOL_41
        .union(Caps::SECURE_CONNECTION)
        .union(Caps::TRANSACTIONS)
        .union(Caps::DEPRECATE_EOF);

    /// The engine's column definitions read back with the capabilities the
    /// seam uses, on both ports and whether or not the client asked for
    /// MariaDB's extended metadata. With a backend's capabilities they
    /// don't on the MySQL port (the 1835 its clients got for `SHOW TABLES`).
    #[test]
    fn engine_definitions_read_back_on_both_ports() {
        let ext = Caps::MARIADB_CLIENT_EXTENDED_METADATA;
        for persona in [Persona::MySql84, Persona::MariaDb] {
            for client in [BASE, BASE | ext] {
                let caps = answer_caps(persona, client);
                for a in answers() {
                    for d in &a.defs {
                        let c = ColumnDef::decode(d, caps);
                        assert!(c.is_ok(), "{persona:?} {client:?}: {c:?}");
                    }
                }
            }
        }
        let old = crate::exec::mariadb_caps(Persona::MySql84, BASE);
        let a = show::tables("t61306000000_d1", &["a".into()], false, None);
        assert!(ColumnDef::decode(&a.defs[0], old).is_err());
    }

    /// The MySQL port's `SHOW COLUMNS` and `SHOW INDEX` queries give up
    /// their probe of the table, and only those.
    #[test]
    fn the_mysql_ports_probe_comes_out() {
        let lit = |s: &str| format!("'{}'", s.replace('\'', "''"));
        for sql in [
            sprig_sql::compat::schema::show_columns_sql(
                "t61306000000_d5",
                "a`b",
                true,
                None,
                None,
                &lit,
            ),
            sprig_sql::compat::schema::show_index_sql("t61306000000_d5", "a`b", None, &lit),
        ] {
            let p = super::strip_probe(std::str::from_utf8(&sql).unwrap()).unwrap();
            assert_eq!(p.schema, "t61306000000_d5");
            assert_eq!(p.table, "a`b");
            assert!(!p.rest.contains("LIMIT 0"), "{}", p.rest);
            assert!(crate::engine::infoschema::reads_tables(&p.rest));
        }
        assert!(super::strip_probe("SELECT 1 FROM t WHERE a = 1").is_none());
    }

    /// The answer through a client connection's wire: column count,
    /// definitions in the client's capabilities, rows, and the end.
    #[tokio::test]
    async fn engine_show_answer_over_the_wire() {
        for persona in [Persona::MySql84, Persona::MariaDb] {
            for client in [BASE, BASE | Caps::MARIADB_CLIENT_EXTENDED_METADATA] {
                for a in answers() {
                    let (srv, cli) = tokio::io::duplex(1 << 20);
                    let mut framed = Framed::new(srv, PacketCodec::new(1 << 24));
                    let ncols = a.defs.len();
                    let nrows = a.rows.len();
                    let renames = Vec::new();
                    let r = Relay {
                        more: false,
                        format: RowFormat::Text,
                        cursor: false,
                        fetch: false,
                        backend_caps: answer_caps(persona, client),
                        infile: Infile {
                            name: None,
                            allowed: false,
                        },
                        conn_status: None,
                        renames: &renames,
                        mysql_nowait: false,
                    };
                    {
                        let mut sink = WireSink::new(&mut framed, persona, client);
                        let res = relay_local(
                            &mut sink,
                            &r,
                            &a.defs,
                            a.rows,
                            StatusFlags::AUTOCOMMIT,
                            None,
                        )
                        .await;
                        assert!(res.is_ok(), "{persona:?}: {:?}", res.err());
                    }
                    crate::mysql::sink::flush(&mut framed).await.unwrap();
                    drop(framed);
                    let mut rd = Framed::new(cli, PacketCodec::new(1 << 24));
                    let mut got: Vec<Bytes> = Vec::new();
                    while let Some(p) = rd.next().await {
                        got.push(p.unwrap().payload);
                    }
                    assert_eq!(decode_column_count(&got[0]).unwrap(), ncols as u64);
                    for d in &got[1..=ncols] {
                        assert!(ColumnDef::decode(d, client).is_ok());
                    }
                    for row in &got[1 + ncols..1 + ncols + nrows] {
                        assert_eq!(decode_text_row(row, ncols).unwrap().len(), ncols);
                    }
                    assert_eq!(got.len(), 2 + ncols + nrows, "{persona:?}");
                    assert_eq!(got.last().unwrap()[0], 0xfe);
                }
            }
        }
    }
}
