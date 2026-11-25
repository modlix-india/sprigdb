//! SQL events in the session: a `CREATE` / `ALTER EVENT`'s schedule
//! evaluated before the catalog's DDL path stores it, the scheduler's
//! marked statement that runs a body (`crate::sql_events`), and `SHOW
//! CREATE EVENT` / `SHOW EVENTS`.

use sprig_sql::names::{DdlKind, DdlTarget};

use super::{
    Arc, Bytes, ExecCtx, LocalWarning, OkPacket, PassSession, Result, ResultSink, Routine,
    SprigError, StatusFlags, Step, catalog, database_key, not_on_engine, with_stack,
};
use crate::engine::routine::event::{self, Eval, Event};
use crate::engine::routine::parse::{self, Kind, Schedule};

impl PassSession {
    /// The engine database `db` names (the current one when `None`):
    /// its id and logical name.
    fn ev_db(&self, db: Option<&str>) -> Option<(sprig_common::ids::DbId, String)> {
        let name = match db {
            Some(d) => d.to_owned(),
            None => self.current_logical()?,
        };
        let ts = self.tenant.as_deref()?;
        let cat = self.engine_cat()?;
        let row = cat.database(&database_key(&name, ts.lower_case_table_names()))?;
        catalog::of(&cat)?
            .dbs
            .contains(&row.db_id)
            .then(|| (row.db_id, row.name.clone()))
    }

    /// The event `name` of database `db`.
    fn ev_find(&self, db: sprig_common::ids::DbId, name: &str) -> Option<Arc<Event>> {
        let cat = self.engine_cat()?;
        catalog::of(&cat)?
            .events
            .get(&(db, name.to_lowercase()))
            .cloned()
    }

    /// Text values of `SELECT exprs` in the session.
    async fn ev_values(
        &mut self,
        cx: &mut ExecCtx<'_>,
        exprs: &[String],
    ) -> Result<Vec<Option<String>>> {
        let sql = format!("SELECT {}", exprs.join(", "));
        let ran = self
            .sp_statement(cx, &sql, None, true)
            .await
            .map_err(|r| r.to_error())?;
        let row = ran.rows.into_iter().next().unwrap_or_default();
        Ok((0..exprs.len())
            .map(|i| {
                row.get(i)
                    .and_then(|v| v.as_deref())
                    .map(|b| String::from_utf8_lossy(b).into_owned())
            })
            .collect())
    }

    /// Evaluates a `CREATE` / `ALTER EVENT`'s schedule on an engine
    /// database before the catalog's DDL path runs it (`None`: go on).
    /// A time already past with `NOT PRESERVE` is answered here: `CREATE`
    /// makes nothing (note 1588), `ALTER` fails (1589).
    pub(in crate::exec) async fn engine_event_prepare(
        &mut self,
        cx: &mut ExecCtx<'_>,
        target: &DdlTarget,
        text: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Option<Result<Step>> {
        if !matches!(
            target,
            DdlTarget::CreateObject {
                kind: DdlKind::Event,
                ..
            } | DdlTarget::AlterObject {
                kind: DdlKind::Event,
                ..
            }
        ) {
            return None;
        }
        self.eng.event_eval = None;
        let text = std::str::from_utf8(text).ok()?;
        let st = match parse::parse_event(text) {
            Ok(st) => st,
            Err(mut e) => {
                if self.persona == sprig_proto_mysql::Persona::MySql84 {
                    e.message = e.message.replace("MariaDB server", "MySQL server");
                }
                return Some(self.fail(out, &e).await);
            }
        };
        let (db, _) = self.ev_db(st.db.as_deref())?;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let r = Box::pin(self.ev_prepare(cx, &st, db, mysql)).await;
        match r {
            Ok(None) => None,
            Ok(Some(note)) => {
                let mut ok = OkPacket::new(status);
                ok.warnings = 1;
                self.diag_set(vec![note], 0);
                Some(match out.ok(ok).await {
                    Ok(()) => Ok(Step {
                        errored: false,
                        status: Some(status),
                    }),
                    Err(e) => Err(e),
                })
            }
            Err(e) => Some(self.fail(out, &e).await),
        }
    }

    /// The schedule's values kept for the DDL path; `Some(note)` when the
    /// statement is answered with that note and changes nothing.
    #[allow(clippy::too_many_lines)] // the checks, then the evaluation
    async fn ev_prepare(
        &mut self,
        cx: &mut ExecCtx<'_>,
        st: &parse::EvStmt,
        db: sprig_common::ids::DbId,
        mysql: bool,
    ) -> Result<Option<LocalWarning>> {
        let old = if st.create {
            None
        } else {
            let Some(e) = self.ev_find(db, &st.name) else {
                return Err(SprigError::standard(
                    1539,
                    *b"HY000",
                    format!("Unknown event '{}'", st.name),
                ));
            };
            Some(e)
        };
        if let Some((rdb, rname)) = &st.rename {
            let same_db = rdb
                .as_deref()
                .is_none_or(|d| st.db.as_deref().is_none_or(|o| o.eq_ignore_ascii_case(d)));
            if same_db && rname.eq_ignore_ascii_case(&st.name) {
                return Err(SprigError::standard(
                    1551,
                    *b"HY000",
                    "Same old and new event name",
                ));
            }
            // The move itself is the catalog's rename (`EngineDdl`).
        }
        let Some(schedule) = &st.schedule else {
            return Ok(None);
        };
        let at = |x: &str| {
            [
                format!("CAST(({x}) AS DATETIME)"),
                format!("UNIX_TIMESTAMP({x})"),
            ]
        };
        let mut exprs = vec![
            "@@time_zone".to_owned(),
            "CAST(NOW() AS DATETIME)".to_owned(),
            "UNIX_TIMESTAMP()".to_owned(),
        ];
        match schedule {
            Schedule::At(x) => exprs.extend(at(x)),
            Schedule::Every {
                value,
                starts,
                ends,
                ..
            } => {
                exprs.push(format!("({value})"));
                exprs.extend(at(starts.as_deref().unwrap_or("NULL")));
                exprs.extend(at(ends.as_deref().unwrap_or("NULL")));
            }
        }
        let v = self.ev_values(cx, &exprs).await?;
        let pair = |i: usize| -> Option<(String, i64)> {
            let t = v.get(i)?.clone()?;
            let n = v.get(i + 1)?.as_deref()?;
            let secs = n.split('.').next()?.parse::<i64>().ok()?;
            Some((t, secs))
        };
        let now = pair(1).ok_or_else(|| SprigError::internal("NOW() read as NULL"))?;
        let mut eval = Eval {
            time_zone: v
                .first()
                .cloned()
                .flatten()
                .unwrap_or_else(|| "SYSTEM".into()),
            now: now.clone(),
            ..Eval::default()
        };
        match schedule {
            Schedule::At(_) => eval.at = pair(3),
            Schedule::Every { starts, ends, .. } => {
                eval.value = v.get(3).cloned().flatten();
                if starts.is_some() {
                    eval.starts = pair(4);
                }
                if ends.is_some() {
                    eval.ends = pair(6);
                }
            }
        }
        let old_def = old.as_ref().and_then(|e| e.def.as_ref().ok());
        let def = event::build(st, Some(&eval), old_def, mysql)?;
        if event::past(&def, now.1) && !def.preserve {
            if !st.create {
                return Err(SprigError::standard(
                    1589,
                    *b"HY000",
                    "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was not changed. Specify a time in the future.",
                ));
            }
            let stop = if mysql { "." } else { "" };
            return Ok(Some(LocalWarning {
                level: "Note",
                code: 1588,
                message: format!(
                    "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation{stop}"
                ),
            }));
        }
        self.eng.event_eval = Some(eval);
        Ok(None)
    }

    /// The scheduler's marked statement (`crate::sql_events`): runs the
    /// event's body as its definer. `None` for any other statement.
    pub(in crate::exec) async fn engine_sql_event_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(req) = crate::sql_events::request(text) else {
            return Ok(None);
        };
        let Some(req) = req.ok().filter(|_| self.tenant.is_some()) else {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: an event run isn't a client statement",
            );
            return self.fail(out, &e).await.map(Some);
        };
        let found = self
            .ev_db(Some(&req.db))
            .and_then(|(db, name)| Some((self.ev_find(db, &req.name)?, db, name)));
        let Some((ev, db, db_name)) = found else {
            let e = SprigError::standard(1539, *b"HY000", format!("Unknown event '{}'", req.name));
            return self.fail(out, &e).await.map(Some);
        };
        let (c, prog) = match &ev.code {
            Ok(x) => x.clone(),
            Err(why) => return self.fail(out, &not_on_engine(why)).await.map(Some),
        };
        let r = Arc::new(Routine {
            db,
            name: ev.name.clone(),
            kind: Kind::Procedure,
            cx: ev.cx.clone(),
            show: String::new(),
            def: Ok((c, Arc::clone(&prog))),
        });
        let tenant = self.tenant.as_deref().map_or(0, |ts| ts.tenant.0);
        let (secs, _) = crate::engine::temporal::now();
        let now = crate::engine::temporal::from_timestamp(
            sprig_engine::row::Timestamp {
                seconds: u32::try_from(secs).unwrap_or(0),
                micros: 0,
            },
            0,
        );
        event::ran(tenant, db, &ev.name, now.to_string());
        let fut = async {
            let mut frame = self.sp_frame(cx, &r, &prog, &db_name, &[], &[])?;
            self.sp_run(cx, &mut frame, None).await
        };
        let ran = Box::pin(with_stack(fut)).await;
        match ran {
            Ok(_) => {
                let mut status = self.status();
                if more {
                    status |= StatusFlags::MORE_RESULTS_EXISTS;
                }
                out.ok(OkPacket::new(status)).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => self.fail(out, &e).await.map(Some),
        }
    }

    /// `SHOW CREATE EVENT` and `SHOW EVENTS` on engine databases (`None`:
    /// not an engine database's).
    #[allow(clippy::too_many_lines)] // two answers, each its columns
    pub(in crate::exec) fn engine_show_event(
        &self,
        info: &sprig_sql::names::show::ShowInfo,
        sql: &[u8],
    ) -> Option<Result<crate::engine::show::Answer>> {
        use crate::engine::show::{Answer, coldef};
        use sprig_proto_mysql::resultset::encode_text_row;
        use sprig_sql::names::show::{ShowKind, like_match};
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let cs = if mysql { 45u16 } else { 224 };
        let (db, db_name) = if let Some(s) = info.schema.as_deref() {
            let db = s.parse::<sprig_common::names::SchemaName>().ok()?.db()?;
            let cat = self.engine_cat()?;
            if !catalog::of(&cat)?.dbs.contains(&db) {
                return None;
            }
            let name = cat.databases.get(db)?.name.clone();
            (db, name)
        } else {
            self.ev_db(None)?
        };
        let c = |n: &str, len: u32| coldef("", "", n, "", cs, len, 0xfd, 0x1001, 0);
        if info.kind == ShowKind::CreateEvent {
            let name = info.object.clone()?;
            let Some(e) = self.ev_find(db, &name) else {
                return Some(Err(SprigError::standard(
                    1539,
                    *b"HY000",
                    format!("Unknown event '{name}'"),
                )));
            };
            let def = match &e.def {
                Ok(d) => d,
                Err(why) => return Some(Err(not_on_engine(why))),
            };
            let defs = vec![
                c("Event", 256),
                c("sql_mode", if mysql { 468 } else { 356 }),
                c("time_zone", 24),
                c("Create Event", 748),
                c("character_set_client", 128),
                c("collation_connection", if mysql { 128 } else { 256 }),
                c("Database Collation", if mysql { 128 } else { 256 }),
            ];
            let show = event::show_create(&e, def, mysql);
            let row = encode_text_row([
                Some(e.name.as_bytes()),
                Some(e.cx.sql_mode.as_bytes()),
                Some(def.time_zone.as_bytes()),
                Some(show.as_bytes()),
                Some(e.cx.charset_client.as_bytes()),
                Some(e.cx.collation_connection.as_bytes()),
                Some(e.cx.db_collation.as_bytes()),
            ]);
            return Some(Ok(Answer {
                defs,
                rows: vec![row],
                no_index: false,
            }));
        }
        if info.kind != ShowKind::Events {
            return None;
        }
        // `Db` as the name layer maps answers back: the physical schema.
        let db_name = if let Some(s) = info.schema.clone() {
            s
        } else {
            let map = self.tenant.as_deref()?.map();
            sprig_sql::names::NameMap::physical(
                &*map,
                &db_name,
                sprig_sql::names::Purpose::Metadata,
            )
            .ok()
            .and_then(|p| p.0.first().map(ToString::to_string))
            .unwrap_or(db_name)
        };
        let cat = self.engine_cat()?;
        let eng = catalog::of(&cat)?;
        let mut list: Vec<&Arc<Event>> = eng
            .events
            .values()
            .filter(|e| e.db == db && e.def.is_ok())
            .filter(|e| {
                info.like
                    .as_deref()
                    .is_none_or(|p| like_match(p, &e.name, false))
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        let dt = |n: &str| coldef("", "", n, "", 63, 19, 0x0c, 0x0080, 0);
        let defs = vec![
            c("Db", 256),
            c("Name", 256),
            c("Definer", if mysql { 1152 } else { 1536 }),
            c("Time zone", 256),
            c("Type", 36),
            dt("Execute at"),
            c("Interval value", 1024),
            c("Interval field", 72),
            dt("Starts"),
            dt("Ends"),
            c("Status", if mysql { 84 } else { 72 }),
            coldef(
                "",
                "",
                "Originator",
                "",
                63,
                10,
                if mysql { 0x03 } else { 0x08 },
                0x1021,
                0,
            ),
            c("character_set_client", if mysql { 256 } else { 128 }),
            c("collation_connection", 256),
            c("Database Collation", 256),
        ];
        let rows = list
            .iter()
            .filter_map(|e| {
                let d = e.def.as_ref().ok()?;
                let definer = format!("{}@{}", e.cx.definer.0, e.cx.definer.1);
                let t = |x: &Option<(String, i64)>| x.as_ref().map(|v| v.0.clone().into_bytes());
                let vals: Vec<Option<Vec<u8>>> = vec![
                    Some(db_name.clone().into_bytes()),
                    Some(e.name.clone().into_bytes()),
                    Some(definer.into_bytes()),
                    Some(d.time_zone.clone().into_bytes()),
                    Some(
                        if d.at.is_some() {
                            "ONE TIME"
                        } else {
                            "RECURRING"
                        }
                        .into(),
                    ),
                    t(&d.at),
                    d.every.as_ref().map(|x| x.0.clone().into_bytes()),
                    d.every.as_ref().map(|x| x.1.clone().into_bytes()),
                    t(&d.starts),
                    t(&d.ends),
                    Some(event::status_word(d.status, mysql).into()),
                    Some(b"1".to_vec()),
                    Some(e.cx.charset_client.clone().into_bytes()),
                    Some(e.cx.collation_connection.clone().into_bytes()),
                    Some(e.cx.db_collation.clone().into_bytes()),
                ];
                Some(encode_text_row(vals.iter().map(Option::as_deref)))
            })
            .collect();
        let answer = Answer {
            defs,
            rows,
            no_index: true,
        };
        match info.filter.clone() {
            Some(range) => {
                let cond = std::str::from_utf8(sql.get(range)?).ok()?;
                crate::engine::infoschema::filter_answer(answer, cond).map(Ok)
            }
            None => Some(Ok(answer)),
        }
    }
}
