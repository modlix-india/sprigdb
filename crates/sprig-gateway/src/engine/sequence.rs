//! MariaDB's sequences on engine databases.
//!
//! **Definition.** A sequence is a catalog object whose text is MariaDB's
//! `SHOW CREATE SEQUENCE` text (`CREATE SEQUENCE `s` start with 1 minvalue
//! 1 maxvalue ... increment by 1 cache 1000 nocycle ENGINE=InnoDB`); the
//! engine reads its options back from that text once per catalog load.
//!
//! **Values.** The sequence's stored state is one engine record: the next
//! value not handed out to any cache (`next_not_cached_value`) and the
//! number of cycles done. `NEXTVAL` takes values from this node's cache
//! of the sequence; an empty cache reserves the next `CACHE` values with a
//! commit of its own that moves the record on, outside the statement's
//! transaction, as MariaDB does (values are never given back: a rollback
//! or a restart leaves a gap). A cache filled under an older leader
//! epoch, or for an older definition, is dropped.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::ids::ObjectId;
use sprig_common::{Result, SprigError};
use sprig_engine::txn::{LockRequest, Locked, TxnOptions};

use super::define::lex::{self, T, Tok};

/// A sequence's options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeqDef {
    /// Its name.
    pub name: String,
    /// `AS type` as written in lower case (`tinyint unsigned`); `None` for
    /// the default `bigint`.
    pub as_type: Option<String>,
    /// `START WITH`.
    pub start: i64,
    /// `MINVALUE`.
    pub min: i64,
    /// `MAXVALUE`.
    pub max: i64,
    /// `INCREMENT BY` (0: the session's `auto_increment_increment`, read as 1).
    pub increment: i64,
    /// `CACHE` (0: `NOCACHE`).
    pub cache: u64,
    /// `CYCLE`.
    pub cycle: bool,
}

/// A sequence as the catalog holds it.
#[derive(Debug)]
pub(crate) struct SeqEntry {
    /// Its object.
    pub object: ObjectId,
    /// Its definition's version (a new one empties every cache).
    pub version: u64,
    /// Its options, or why the engine can't read them.
    pub def: std::result::Result<SeqDef, String>,
    /// The stored text.
    pub text: String,
}

impl SeqDef {
    /// The step values move by.
    fn step(&self) -> i64 {
        if self.increment == 0 {
            1
        } else {
            self.increment
        }
    }

    /// `v` is past the end the sequence moves towards.
    fn past(&self, v: i128) -> bool {
        if self.step() > 0 {
            v > i128::from(self.max)
        } else {
            v < i128::from(self.min)
        }
    }
}

/// The range of `AS type`: (lowest, highest).
fn type_range(ty: Option<&str>) -> Option<(i128, i128)> {
    let Some(ty) = ty else {
        return Some((i128::from(i64::MIN), i128::from(i64::MAX)));
    };
    let mut w = ty.split_whitespace();
    let base = w.next()?;
    let unsigned = match w.next() {
        None | Some("signed") => false,
        Some("unsigned") => true,
        _ => return None,
    };
    let bits: u32 = match base {
        "tinyint" => 8,
        "smallint" => 16,
        "mediumint" => 24,
        "int" | "integer" => 32,
        "bigint" => 64,
        _ => return None,
    };
    if unsigned {
        Some((0, (1i128 << bits) - 1))
    } else {
        Some((-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1))
    }
}

/// 4085: options out of range.
fn out_of_range(db: &str, name: &str) -> SprigError {
    SprigError::standard(
        4085,
        *b"HY000",
        format!("Sequence '{db}.{name}' has out of range value for options"),
    )
}

/// 4084: the sequence has no value left.
pub(crate) fn run_out(db: &str, name: &str) -> SprigError {
    SprigError::standard(
        4084,
        *b"HY000",
        format!("Sequence '{db}.{name}' has run out"),
    )
}

fn syntax(near: &str) -> SprigError {
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near '{near}' at line 1"
        ),
    )
}

/// The options a statement gives (each `None` when not given).
#[derive(Debug, Default, Clone)]
#[allow(clippy::option_option)] // not given, `NO MINVALUE`, or a value
pub(crate) struct Options {
    pub as_type: Option<String>,
    pub start: Option<i64>,
    /// `Some(None)`: `NO MINVALUE`.
    pub min: Option<Option<i64>>,
    pub max: Option<Option<i64>>,
    pub increment: Option<i64>,
    pub cache: Option<u64>,
    pub cycle: Option<bool>,
    /// `ALTER SEQUENCE ... RESTART [WITH n]`: `Some(None)` restarts at the
    /// start value.
    pub restart: Option<Option<i64>>,
}

/// A `CREATE` or `ALTER SEQUENCE` statement read.
#[derive(Debug)]
pub(crate) struct Statement {
    pub options: Options,
}

/// A signed number at `toks[*i]` (`-5`, `+5`, `5`).
fn number(toks: &[Tok], i: &mut usize, text: &str) -> Result<i64> {
    let mut neg = false;
    while let Some(t) = toks.get(*i) {
        if t.op("-") {
            neg = !neg;
        } else if !t.op("+") {
            break;
        }
        *i += 1;
    }
    let Some(Tok { t: T::Num(n), at }) = toks.get(*i) else {
        return Err(syntax(toks.get(*i).map_or("", |t| &text[t.at..])));
    };
    let v: i128 = n.parse().map_err(|_| syntax(&text[*at..]))?;
    let v = if neg { -v } else { v };
    *i += 1;
    // Values beyond 64 bits are out of every type's range.
    Ok(i64::try_from(v).unwrap_or(if neg { i64::MIN } else { i64::MAX }))
}

fn eat(toks: &[Tok], i: &mut usize, w: &str) -> bool {
    if toks.get(*i).is_some_and(|t| t.is(w)) {
        *i += 1;
        true
    } else {
        false
    }
}

fn eat_op(toks: &[Tok], i: &mut usize, o: &str) -> bool {
    if toks.get(*i).is_some_and(|t| t.op(o)) {
        *i += 1;
        true
    } else {
        false
    }
}

/// Reads a `CREATE SEQUENCE` or `ALTER SEQUENCE` statement's options
/// (`text`, after the name: everything else was read by the name layer).
///
/// # Errors
/// 1064 for options in a form MariaDB doesn't take.
pub(crate) fn parse(text: &str) -> Result<Statement> {
    let toks = lex::lex(text).map_err(|at| syntax(&text[at..]))?;
    // Past `CREATE [OR REPLACE] [TEMPORARY] SEQUENCE [IF NOT EXISTS] name`
    // or `ALTER SEQUENCE [IF EXISTS] name`.
    let mut i = toks
        .iter()
        .position(|t| t.is("SEQUENCE"))
        .ok_or_else(|| syntax(text))?
        + 1;
    if eat(&toks, &mut i, "IF") {
        eat(&toks, &mut i, "NOT");
        eat(&toks, &mut i, "EXISTS");
    }
    // The name: `a` or `a.b`.
    i += 1;
    if eat_op(&toks, &mut i, ".") {
        i += 1;
    }
    let mut o = Options::default();
    while let Some(t) = toks.get(i) {
        let near = &text[t.at..];
        i += 1;
        if t.is("INCREMENT") {
            let _ = eat(&toks, &mut i, "BY") || eat_op(&toks, &mut i, "=");
            o.increment = Some(number(&toks, &mut i, text)?);
        } else if t.is("START") {
            let _ = eat(&toks, &mut i, "WITH") || eat_op(&toks, &mut i, "=");
            o.start = Some(number(&toks, &mut i, text)?);
        } else if t.is("MINVALUE") {
            eat_op(&toks, &mut i, "=");
            o.min = Some(Some(number(&toks, &mut i, text)?));
        } else if t.is("MAXVALUE") {
            eat_op(&toks, &mut i, "=");
            o.max = Some(Some(number(&toks, &mut i, text)?));
        } else if t.is("NOMINVALUE") {
            o.min = Some(None);
        } else if t.is("NOMAXVALUE") {
            o.max = Some(None);
        } else if t.is("NOCACHE") {
            o.cache = Some(0);
        } else if t.is("NOCYCLE") {
            o.cycle = Some(false);
        } else if t.is("CYCLE") {
            o.cycle = Some(true);
        } else if t.is("NO") {
            if eat(&toks, &mut i, "MINVALUE") {
                o.min = Some(None);
            } else if eat(&toks, &mut i, "MAXVALUE") {
                o.max = Some(None);
            } else if eat(&toks, &mut i, "CYCLE") {
                o.cycle = Some(false);
            } else if eat(&toks, &mut i, "CACHE") {
                o.cache = Some(0);
            } else {
                return Err(syntax(near));
            }
        } else if t.is("CACHE") {
            eat_op(&toks, &mut i, "=");
            let n = number(&toks, &mut i, text)?;
            o.cache = Some(u64::try_from(n).map_err(|_| syntax(near))?);
        } else if t.is("RESTART") {
            if eat(&toks, &mut i, "WITH")
                || eat_op(&toks, &mut i, "=")
                || toks
                    .get(i)
                    .is_some_and(|t| matches!(t.t, T::Num(_)) || t.op("-") || t.op("+"))
            {
                o.restart = Some(Some(number(&toks, &mut i, text)?));
            } else {
                o.restart = Some(None);
            }
        } else if t.is("AS") {
            let Some(Tok {
                t: T::Word(base), ..
            }) = toks.get(i)
            else {
                return Err(syntax(near));
            };
            let mut ty = base.to_ascii_lowercase();
            i += 1;
            if eat(&toks, &mut i, "UNSIGNED") {
                ty.push_str(" unsigned");
            } else {
                eat(&toks, &mut i, "SIGNED");
            }
            if type_range(Some(&ty)).is_none() {
                return Err(syntax(near));
            }
            o.as_type = Some(ty);
        } else if t.is("ENGINE") || t.is("COMMENT") {
            // A table option: the engine keeps none of them.
            eat_op(&toks, &mut i, "=");
            i += 1;
        } else if t.op(",") || t.op(";") {
        } else {
            return Err(syntax(near));
        }
    }
    Ok(Statement { options: o })
}

/// The definition `o` gives a new sequence `name` of database `db`,
/// MariaDB's defaults filled in and its checks made.
///
/// # Errors
/// 4085 when the options don't fit together.
pub(crate) fn define(db: &str, name: &str, o: &Options) -> Result<SeqDef> {
    let (lo, hi) = type_range(o.as_type.as_deref()).ok_or_else(|| out_of_range(db, name))?;
    let increment = o.increment.unwrap_or(1);
    let up = increment >= 0;
    let (dmin, dmax) = if up || lo >= 0 {
        (1, hi - 1)
    } else {
        (lo + 1, -1)
    };
    let mut min = match o.min {
        Some(Some(v)) => i128::from(v),
        _ => dmin,
    };
    let mut max = match o.max {
        Some(Some(v)) => i128::from(v),
        _ => dmax,
    };
    // The type's own ends are out of reach.
    if max >= hi {
        max = hi - 1;
    }
    if min <= lo {
        min = lo + 1;
    }
    let start = o.start.map_or(if up { min } else { max }, i128::from);
    let cache = o.cache.unwrap_or(1000);
    if min >= max || start < min || start > max || max < lo || min > hi {
        return Err(out_of_range(db, name));
    }
    let fit = |v: i128| i64::try_from(v).map_err(|_| out_of_range(db, name));
    Ok(SeqDef {
        name: name.to_owned(),
        as_type: o.as_type.clone(),
        start: fit(start)?,
        min: fit(min)?,
        max: fit(max)?,
        increment,
        cache,
        cycle: o.cycle.unwrap_or(false),
    })
}

/// `def` changed by `ALTER SEQUENCE` options `o`.
///
/// # Errors
/// 4085 when the options don't fit together.
pub(crate) fn alter(db: &str, def: &SeqDef, o: &Options) -> Result<SeqDef> {
    let merged = Options {
        as_type: o.as_type.clone().or_else(|| def.as_type.clone()),
        start: Some(o.start.unwrap_or(def.start)),
        min: Some(o.min.unwrap_or(Some(def.min))),
        max: Some(o.max.unwrap_or(Some(def.max))),
        increment: Some(o.increment.unwrap_or(def.increment)),
        cache: Some(o.cache.unwrap_or(def.cache)),
        cycle: Some(o.cycle.unwrap_or(def.cycle)),
        restart: None,
    };
    let d = define(db, &def.name, &merged)?;
    if let Some(Some(r)) = o.restart
        && (r < d.min || r > d.max)
    {
        return Err(out_of_range(db, &def.name));
    }
    Ok(d)
}

fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The text MariaDB's `SHOW CREATE SEQUENCE` shows for `def`.
pub(crate) fn render(def: &SeqDef) -> String {
    let mut s = format!("CREATE SEQUENCE {} ", quote(&def.name));
    if let Some(t) = &def.as_type {
        s.push_str("as ");
        s.push_str(t);
        s.push(' ');
    }
    let _ = write!(
        s,
        "start with {} minvalue {} maxvalue {} increment by {} ",
        def.start, def.min, def.max, def.increment
    );
    if def.cache == 0 {
        s.push_str("nocache ");
    } else {
        let _ = write!(s, "cache {} ", def.cache);
    }
    s.push_str(if def.cycle { "cycle" } else { "nocycle" });
    s.push_str(" ENGINE=InnoDB");
    s
}

/// Reads a stored text back.
///
/// # Errors
/// The text isn't one [`render`] wrote.
pub(crate) fn load(text: &str) -> std::result::Result<SeqDef, String> {
    let toks = lex::lex(text).map_err(|_| "unreadable sequence text".to_owned())?;
    let name = match toks.get(2) {
        Some(Tok {
            t: T::Quoted(n) | T::Word(n),
            ..
        }) => n.clone(),
        _ => return Err("unreadable sequence text".into()),
    };
    let st = parse(text).map_err(|e| e.message)?;
    let o = st.options;
    let get = |v: Option<i64>| v.ok_or_else(|| "incomplete sequence text".to_owned());
    Ok(SeqDef {
        name,
        as_type: o.as_type,
        start: get(o.start)?,
        min: get(o.min.flatten())?,
        max: get(o.max.flatten())?,
        increment: get(o.increment)?,
        cache: o.cache.unwrap_or(1000),
        cycle: o.cycle.unwrap_or(false),
    })
}

/// `SHOW CREATE TABLE` of a sequence: the table MariaDB keeps it in.
pub(crate) fn table_text(name: &str) -> String {
    format!(
        "CREATE TABLE {} (\n  `next_not_cached_value` bigint(21) NOT NULL,\n  `minimum_value` bigint(21) NOT NULL,\n  `maximum_value` bigint(21) NOT NULL,\n  `start_value` bigint(21) NOT NULL COMMENT 'start value when sequences is created or value if RESTART is used',\n  `increment` bigint(21) NOT NULL COMMENT 'increment value',\n  `cache_size` bigint(21) unsigned NOT NULL,\n  `cycle_option` tinyint(1) unsigned NOT NULL COMMENT '0 if no cycles are allowed, 1 if the sequence should begin a new cycle when maximum_value is passed',\n  `cycle_count` bigint(21) NOT NULL COMMENT 'How many cycles have been done'\n) ENGINE=InnoDB SEQUENCE=1",
        quote(name)
    )
}

/// The derived table a read of sequence `db`.`name` (`def`) reads: its one
/// row, as MariaDB's sequence table holds it.
pub(crate) fn as_select(db: &str, name: &str, def: &SeqDef) -> String {
    let r = format!("{}.{}", quote(db), quote(name));
    format!(
        "SELECT SPRIG_SEQUENCE_STATE({r}, 0) AS `next_not_cached_value`, CAST({} AS SIGNED) AS `minimum_value`, CAST({} AS SIGNED) AS `maximum_value`, CAST({} AS SIGNED) AS `start_value`, CAST({} AS SIGNED) AS `increment`, CAST({} AS UNSIGNED) AS `cache_size`, CAST({} AS UNSIGNED) AS `cycle_option`, SPRIG_SEQUENCE_STATE({r}, 1) AS `cycle_count`",
        def.min,
        def.max,
        def.start,
        def.increment,
        def.cache,
        u8::from(def.cycle)
    )
}

// ----- stored state -----

/// A sequence's stored state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    /// The next value no cache holds.
    pub next: i64,
    /// Cycles done.
    pub round: i64,
}

impl Record {
    fn encode(self) -> Bytes {
        let mut b = Vec::with_capacity(16);
        b.extend_from_slice(&self.next.to_be_bytes());
        b.extend_from_slice(&self.round.to_be_bytes());
        Bytes::from(b)
    }

    fn decode(v: &[u8]) -> Result<Self> {
        let (a, b) = v
            .split_at_checked(8)
            .filter(|(_, b)| b.len() == 8)
            .ok_or_else(|| SprigError::internal("engine: a sequence's record is corrupt"))?;
        let n = |x: &[u8]| i64::from_be_bytes(x.try_into().unwrap_or([0; 8]));
        Ok(Self {
            next: n(a),
            round: n(b),
        })
    }
}

/// The key of sequence `object`'s record.
fn record_key(tenant: u64, object: u32) -> Bytes {
    Bytes::from(super::keys::sequence_key(tenant, object))
}

/// A node's cache of one sequence.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Cache {
    epoch: u64,
    version: u64,
    /// The next value to hand out.
    next: i128,
    /// Where the reserved values end (exclusive, in the step's direction).
    limit: i128,
    round: i64,
}

impl Cache {
    fn empty(&self, step: i64) -> bool {
        if step > 0 {
            self.next >= self.limit
        } else {
            self.next <= self.limit
        }
    }
}

/// Runs `f`, which may wait for a commit, off the async scheduler's
/// queue when it runs on one with several workers.
fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

impl super::Engine {
    fn seq_object(object: ObjectId) -> Result<u32> {
        u32::try_from(object.0).map_err(|_| SprigError::internal("engine: sequence id too large"))
    }

    /// The stored state of sequence `e` (its start when nothing is stored).
    pub(crate) fn seq_record(&self, tenant: u64, e: &SeqEntry, def: &SeqDef) -> Result<Record> {
        let key = record_key(tenant, Self::seq_object(e.object)?);
        blocking(|| {
            let mut t = self.eng.begin(TxnOptions::default())?;
            match t.get_snapshot(&key)? {
                Some(v) => Record::decode(&v),
                None => Ok(Record {
                    next: def.start,
                    round: 0,
                }),
            }
        })
    }

    /// Stores `rec` as sequence `object`'s state (`None`: deletes it), in a
    /// commit of its own, and empties this node's cache of it.
    pub(crate) fn seq_store(
        &self,
        tenant: u64,
        object: ObjectId,
        rec: Option<Record>,
    ) -> Result<()> {
        let o = Self::seq_object(object)?;
        let key = record_key(tenant, o);
        self.seqs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(tenant, o));
        blocking(|| {
            let mut t = self.eng.begin(TxnOptions::default())?;
            match rec {
                Some(r) => t.put(key.to_vec(), r.encode())?,
                None => t.delete(key.to_vec())?,
            }
            t.commit()?.wait()?;
            Ok(())
        })
    }

    /// Reserves the next block of sequence `e`'s values: moves the stored
    /// state on in a commit of its own.
    fn seq_reserve(
        &self,
        tenant: u64,
        object: u32,
        db: &str,
        def: &SeqDef,
        epoch: u64,
        version: u64,
    ) -> Result<Cache> {
        let key = record_key(tenant, object);
        blocking(|| {
            let mut t = self.eng.begin(TxnOptions::default())?;
            let rec = match t.lock_get_blocking(&key, LockRequest::for_update())? {
                Locked::Row(Some(v)) => Record::decode(&v)?,
                Locked::Row(None) | Locked::Skipped => Record {
                    next: def.start,
                    round: 0,
                },
            };
            let step = def.step();
            let mut v = i128::from(rec.next);
            let mut round = rec.round;
            if def.past(v) {
                if !def.cycle {
                    return Err(run_out(db, &def.name));
                }
                v = if step > 0 {
                    i128::from(def.min)
                } else {
                    i128::from(def.max)
                };
                round += 1;
            }
            let mut limit = v + i128::from(def.cache.max(1)) * i128::from(step);
            if step > 0 && limit > i128::from(def.max) {
                limit = i128::from(def.max) + 1;
            } else if step < 0 && limit < i128::from(def.min) {
                limit = i128::from(def.min) - 1;
            }
            let next = i64::try_from(limit)
                .map_err(|_| SprigError::internal("engine: a sequence left its range"))?;
            t.put(key.to_vec(), Record { next, round }.encode())?;
            t.commit()?.wait()?;
            Ok(Cache {
                epoch,
                version,
                next: v,
                limit,
                round,
            })
        })
    }

    /// `NEXTVAL` of sequence `e` of `tenant`'s database `db`.
    ///
    /// # Errors
    /// 4084 when it has run out; the node doesn't lead the tenant.
    pub(crate) fn seq_next(&self, tenant: u64, db: &str, e: &SeqEntry) -> Result<i64> {
        let def = e.def.as_ref().map_err(|w| super::not_on_engine(w))?;
        let o = Self::seq_object(e.object)?;
        let epoch = self.epoch_of(tenant)?;
        let step = def.step();
        let mut map = self.seqs.lock().unwrap_or_else(PoisonError::into_inner);
        let c = match map.get(&(tenant, o)) {
            Some(c) if c.epoch == epoch && c.version == e.version && !c.empty(step) => *c,
            _ => self.seq_reserve(tenant, o, db, def, epoch, e.version)?,
        };
        let mut c = c;
        if c.empty(step) || def.past(c.next) {
            // A block of nothing (the stored state was at the end).
            c = self.seq_reserve(tenant, o, db, def, epoch, e.version)?;
        }
        let v = i64::try_from(c.next).map_err(|_| run_out(db, &def.name))?;
        c.next += i128::from(step);
        map.insert((tenant, o), c);
        Ok(v)
    }

    /// `SETVAL(e, n, used, round)`: `Some(n)` when the sequence moved on.
    ///
    /// # Errors
    /// The state couldn't be read or stored.
    pub(crate) fn seq_set(
        &self,
        tenant: u64,
        e: &SeqEntry,
        n: i64,
        used: bool,
        round: u64,
    ) -> Result<Option<i64>> {
        let def = e.def.as_ref().map_err(|w| super::not_on_engine(w))?;
        let o = Self::seq_object(e.object)?;
        let epoch = self.epoch_of(tenant)?;
        let step = def.step();
        let round = i64::try_from(round).unwrap_or(i64::MAX);
        let mut map = self.seqs.lock().unwrap_or_else(PoisonError::into_inner);
        let cached = map
            .get(&(tenant, o))
            .filter(|c| c.epoch == epoch && c.version == e.version)
            .copied();
        let (cur_round, cur_next) = if let Some(c) = cached {
            (c.round, c.next)
        } else {
            let r = self.seq_record(tenant, e, def)?;
            (r.round, i128::from(r.next))
        };
        let target = if used {
            i128::from(n) + i128::from(step)
        } else {
            i128::from(n)
        };
        if round < cur_round {
            return Ok(None);
        }
        if round == cur_round {
            let behind = if step > 0 {
                target < cur_next
            } else {
                target > cur_next
            };
            if behind {
                return Ok(None);
            }
            if target == cur_next {
                return Ok(Some(n));
            }
        }
        if let Some(mut c) = cached
            && round == c.round
            && !(if step > 0 {
                target >= c.limit
            } else {
                target <= c.limit
            })
        {
            c.next = target;
            map.insert((tenant, o), c);
            return Ok(Some(n));
        }
        let next = i64::try_from(target).unwrap_or(if step > 0 { i64::MAX } else { i64::MIN });
        let key = record_key(tenant, o);
        blocking(|| -> Result<()> {
            let mut t = self.eng.begin(TxnOptions::default())?;
            t.put(key.to_vec(), Record { next, round }.encode())?;
            t.commit()?.wait()?;
            Ok(())
        })?;
        map.insert(
            (tenant, o),
            Cache {
                epoch,
                version: e.version,
                next: target,
                limit: target,
                round,
            },
        );
        Ok(Some(n))
    }
}

tokio::task_local! {
    /// The running statement's sequences, for the expression engine's
    /// values of an `INSERT`'s `VALUES` (`NEXTVAL(s)` there).
    pub(crate) static CURRENT: Option<Arc<dyn sprig_exec::Sequences>>;
}

/// The running statement's sequences, when it has any.
pub(crate) fn current() -> Option<Arc<dyn sprig_exec::Sequences>> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

/// Resolves a sequence name for a statement bound by the single-table
/// binder: `schema` as written (empty: the current database).
pub(crate) fn resolve(
    cx: &super::plan::BindCtx<'_>,
    schema: &str,
    name: &str,
) -> std::result::Result<String, sprig_exec::SqlError> {
    let db = if schema.is_empty() {
        cx.current
    } else {
        super::plan::physical_db(cx.tenant, schema).or_else(|| cx.names.db_by_name(schema))
    };
    let Some(db) = db else {
        return Err(sprig_exec::SqlError::new(
            1046,
            "3D000",
            "No database selected",
        ));
    };
    if let Some(e) = cx.names.sequence(db, name) {
        return Ok(e.object.0.to_string());
    }
    let db_name = cx.names.db_name(db);
    Err(if cx.names.table(db, name).is_some() {
        sprig_exec::SqlError::new(
            4089,
            "42S02",
            format!("'{db_name}.{name}' is not a SEQUENCE"),
        )
    } else {
        sprig_exec::SqlError::new(4091, "42S02", format!("Unknown SEQUENCE: '{name}'"))
    })
}

/// One session's view of its tenant's sequences, for the expression
/// engine's sequence functions.
pub(crate) struct SessionSequences {
    pub(crate) eng: &'static super::Engine,
    pub(crate) cat: Arc<TenantCatalog>,
    /// The session's last value of each sequence (`LASTVAL`), by object.
    pub(crate) last: Arc<Mutex<HashMap<u64, i64>>>,
}

impl std::fmt::Debug for SessionSequences {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionSequences")
    }
}

impl SessionSequences {
    fn entry(&self, seq: &str) -> sprig_exec::Result<(Arc<SeqEntry>, String)> {
        let object = seq
            .parse::<u64>()
            .map(ObjectId)
            .map_err(|_| sprig_exec::SqlError::unsupported("this sequence"))?;
        let eng = super::catalog::of(&self.cat);
        let e = eng
            .and_then(|c| c.seq_by_object.get(&object))
            .ok_or_else(|| {
                sprig_exec::SqlError::new(4091, "42S02", format!("Unknown SEQUENCE: '{seq}'"))
            })?;
        let db = self
            .cat
            .objects
            .get(object)
            .and_then(|o| self.cat.databases.get(o.db_id))
            .map(|d| d.name.clone())
            .unwrap_or_default();
        Ok((Arc::clone(e), db))
    }
}

fn sql(e: &SprigError) -> sprig_exec::SqlError {
    let p = e.protocol();
    sprig_exec::SqlError::new(p.mysql_errno, "HY000", e.message.clone())
}

impl sprig_exec::Sequences for SessionSequences {
    fn next(&self, seq: &str) -> sprig_exec::Result<i64> {
        let (e, db) = self.entry(seq)?;
        let v = self
            .eng
            .seq_next(self.cat.tenant.0, &db, &e)
            .map_err(|x| sql(&x))?;
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(e.object.0, v);
        Ok(v)
    }

    fn last(&self, seq: &str) -> sprig_exec::Result<Option<i64>> {
        let (e, _) = self.entry(seq)?;
        Ok(self
            .last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&e.object.0)
            .copied())
    }

    fn set(&self, seq: &str, n: i64, used: bool, round: u64) -> sprig_exec::Result<Option<i64>> {
        let (e, _) = self.entry(seq)?;
        self.eng
            .seq_set(self.cat.tenant.0, &e, n, used, round)
            .map_err(|x| sql(&x))
    }

    fn state(&self, seq: &str, field: u8) -> sprig_exec::Result<i64> {
        let (e, _) = self.entry(seq)?;
        let def = e
            .def
            .as_ref()
            .map_err(|w| sprig_exec::SqlError::unsupported(w))?;
        let r = self
            .eng
            .seq_record(self.cat.tenant.0, &e, def)
            .map_err(|x| sql(&x))?;
        Ok(if field == 0 { r.next } else { r.round })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_read_and_print_as_mariadb_shows_them() {
        let st = parse("CREATE SEQUENCE `s`").unwrap();
        let d = define("db", "s", &st.options).unwrap();
        assert_eq!(
            render(&d),
            "CREATE SEQUENCE `s` start with 1 minvalue 1 maxvalue 9223372036854775806 increment by 1 cache 1000 nocycle ENGINE=InnoDB"
        );
        assert_eq!(load(&render(&d)).unwrap(), d);
        let st = parse(
            "create sequence db.s2 as int start with 10 increment by -2 minvalue 1 maxvalue 20 cache 3 cycle",
        )
        .unwrap();
        let d = define("db", "s2", &st.options).unwrap();
        assert_eq!(
            render(&d),
            "CREATE SEQUENCE `s2` as int start with 10 minvalue 1 maxvalue 20 increment by -2 cache 3 cycle ENGINE=InnoDB"
        );
        assert_eq!(load(&render(&d)).unwrap(), d);
        let d = define(
            "db",
            "s",
            &parse("create sequence s increment by -3").unwrap().options,
        )
        .unwrap();
        assert_eq!(
            (d.start, d.min, d.max),
            (-1, -9_223_372_036_854_775_807, -1)
        );
        let d = define(
            "db",
            "s",
            &parse("create sequence s as tinyint unsigned")
                .unwrap()
                .options,
        )
        .unwrap();
        assert_eq!((d.min, d.max), (1, 254));
        let d = define(
            "db",
            "s",
            &parse("create sequence s cache 0").unwrap().options,
        )
        .unwrap();
        assert!(render(&d).contains(" nocache nocycle"));
        assert_eq!(
            define(
                "db",
                "s",
                &parse("create sequence s start with 5 minvalue 10")
                    .unwrap()
                    .options
            )
            .unwrap_err()
            .protocol()
            .mysql_errno,
            4085
        );
        let st = parse("alter sequence s restart with 7").unwrap();
        assert_eq!(st.options.restart, Some(Some(7)));
    }
}
