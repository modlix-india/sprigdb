//! SQL triggers on engine tables: a catalog object of kind `trigger`
//! whose parent is its table. Its text is the routine context line (with
//! the trigger's place in its table's order) and the `SHOW CREATE
//! TRIGGER` statement. The body compiles, against the table's columns
//! when it fires, to the routines' bytecode with one slot per `NEW.col`
//! and `OLD.col`; the session runs it (`exec/engine_seam/trigger.rs`).

use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};

use super::code::{self, Program};
use super::ddl::{Session, default_sql_mode};
use super::parse::{self, Event, TrigCreate};
use super::{Context, ident};

/// The gap between the order keys of triggers created one after another.
const STEP: i64 = 1 << 20;

/// One trigger, as the catalog has it.
#[derive(Debug)]
pub(crate) struct Trigger {
    /// Its database.
    pub db: DbId,
    /// Its table.
    pub table: ObjectId,
    /// Its name as created.
    pub name: String,
    /// The creation context; `order` places it among its table's triggers
    /// of the same timing and event.
    pub cx: Context,
    /// The `SHOW CREATE TRIGGER` statement.
    pub show: String,
    /// The parsed definition, or why the engine can't read it.
    pub def: std::result::Result<TrigCreate, String>,
}

impl Trigger {
    /// (`BEFORE`, event), when readable.
    pub(crate) fn when(&self) -> Option<(bool, Event)> {
        self.def.as_ref().ok().map(|d| (d.before, d.event))
    }

    /// Whether an `UPDATE` assigning the columns `assigned` fires it: a
    /// trigger `UPDATE OF c, ...` only when one of its columns is assigned
    /// (names compared as column names are).
    pub(crate) fn fires_on(&self, assigned: &[&str]) -> bool {
        match &self.def {
            Ok(d) if !d.of.is_empty() => {
                d.of.iter()
                    .any(|c| assigned.iter().any(|a| a.eq_ignore_ascii_case(c)))
            }
            _ => true,
        }
    }

    /// The body compiled against the table's columns.
    ///
    /// # Errors
    /// The definition can't be read, or names columns the table no longer
    /// has.
    pub(crate) fn program(&self, cols: &[String]) -> Result<Arc<Program>> {
        let d = self.def.as_ref().map_err(|e| super::not_on_engine(e))?;
        Ok(Arc::new(code::compile_trigger(d, cols)?))
    }
}

/// Reads a trigger's catalog text.
pub(crate) fn load(db: DbId, table: ObjectId, name: &str, text: &str) -> Trigger {
    let (cx, show) = super::split_stored(text);
    Trigger {
        db,
        table,
        name: name.to_owned(),
        cx,
        show: show.to_owned(),
        def: parse::parse_trigger(show).map_err(|e| e.message),
    }
}

/// The catalog text of a `CREATE TRIGGER` on a table with columns `cols`.
/// `others`: the table's other triggers (name, `BEFORE`, event, order key),
/// for `FOLLOWS` / `PRECEDES` and the new one's place.
///
/// # Errors
/// The servers' creation errors (1054, 1362, 1363, 1415, 4031, ...).
#[allow(clippy::too_many_arguments)] // the statement and its context
pub(crate) fn create(
    sql: &str,
    sess: &Session,
    account: &(String, String),
    mysql: bool,
    db_cs: &(String, String),
    cols: &[String],
    others: &[(String, bool, Event, i64)],
) -> Result<String> {
    let text = sess
        .entry
        .as_deref()
        .filter(|e| parse::parse_trigger(e).is_ok())
        .unwrap_or(sql);
    let t = parse::parse_trigger(text)?;
    // `UPDATE OF` is MariaDB's; MySQL's parser stops at `OF`.
    if mysql && let Some(near) = &t.of_text {
        return Err(SprigError::standard(
            1064,
            *b"42000",
            format!(
                "You have an error in your SQL syntax; check the manual that corresponds to your \
                 MySQL server version for the right syntax to use near '{near}' at line 1"
            ),
        ));
    }
    code::compile_trigger(&t, cols)?;
    let mut same: Vec<(&str, i64)> = others
        .iter()
        .filter(|(n, b, e, _)| *b == t.before && *e == t.event && *n != t.name)
        .map(|(n, _, _, k)| (n.as_str(), *k))
        .collect();
    same.sort_by_key(|x| x.1);
    let order = match &t.order {
        None => same.last().map_or(STEP, |x| x.1 + STEP),
        Some((follows, other)) => {
            let Some(i) = same.iter().position(|x| x.0 == other.as_str()) else {
                // MySQL's number and full stop.
                let (code, stop) = if mysql { (3011, ".") } else { (4031, "") };
                return Err(SprigError::standard(
                    code,
                    *b"HY000",
                    format!(
                        "Referenced trigger '{other}' for the given action time and event type does not exist{stop}"
                    ),
                ));
            };
            let k = same[i].1;
            if *follows {
                same.get(i + 1).map_or(k + STEP, |n| k + (n.1 - k) / 2)
            } else {
                i.checked_sub(1)
                    .and_then(|p| same.get(p))
                    .map_or(k - STEP, |p| p.1 + (k - p.1) / 2)
            }
        }
    };
    let definer = match &t.definer {
        Some((u, h)) if !u.is_empty() || !h.is_empty() => (u.clone(), h.clone()),
        _ => account.clone(),
    };
    let show = format!(
        "CREATE DEFINER={}@{} {}",
        ident(&definer.0),
        ident(&definer.1),
        t.rest
    );
    let dflt = if mysql {
        "utf8mb4_0900_ai_ci"
    } else {
        "utf8mb4_uca1400_ai_ci"
    };
    let or = |s: &str, d: &str| {
        if s.is_empty() {
            d.to_owned()
        } else {
            s.to_owned()
        }
    };
    let cx = Context {
        sql_mode: or(&sess.sql_mode, default_sql_mode(mysql)),
        charset_client: or(&sess.charset_client, "utf8mb4"),
        collation_connection: or(&sess.collation_connection, dflt),
        db_collation: or(&db_cs.1, dflt),
        created: sess.now.clone(),
        modified: sess.now.clone(),
        definer,
        order,
        event: None,
    };
    Ok(super::stored(&cx, &show))
}

/// The table a `CREATE TRIGGER` statement names: (database, table).
pub(crate) fn table_of(sql: &str) -> Option<(Option<String>, String)> {
    parse::parse_trigger(sql)
        .ok()
        .map(|t| (t.table_db, t.table))
}

/// The triggers of `table` firing `before` an `event`, in their order.
pub(crate) fn firing<'a>(
    all: impl Iterator<Item = &'a Arc<Trigger>>,
    table: ObjectId,
    before: bool,
    event: Event,
) -> Vec<Arc<Trigger>> {
    let mut v: Vec<Arc<Trigger>> = all
        .filter(|t| t.table == table && t.when() == Some((before, event)))
        .cloned()
        .collect();
    v.sort_by_key(|t| t.cx.order);
    v
}
