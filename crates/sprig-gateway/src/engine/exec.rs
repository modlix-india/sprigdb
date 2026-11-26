//! Running a [`Plan`] against the engine.
//!
//! A statement runs in its own engine transaction (autocommit,
//! [`Engine::run_env`]) or as one statement of the session's transaction
//! ([`Engine::run_in`]): atomic there, so a statement that fails undoes
//! its own writes and leaves the transaction open with its locks.
//! A `SELECT` reads at the transaction's read view without locks (a point
//! select is one `get`). `UPDATE` and `DELETE` find candidate rows the
//! same way, then lock each one and read it again (InnoDB's current
//! read), checking the `WHERE` on the locked version; `INSERT` locks each
//! new key and checks it is absent. The answer goes out once the commit
//! (autocommit) and every pending version read are durable.
//!
//! Values stored follow the servers' assignment rules under the session's
//! `sql_mode` ([`super::assign`]): in strict mode a value that doesn't fit
//! fails the statement, otherwise it is clipped and the warning counted.
//! Error texts are MariaDB's (the 3306 persona's error mapping rewords
//! them as for any backend error).
//!
//! A key is made from a `WHERE` constant only when the constant is a value
//! of the key column's type as the comparison would see it ([`key_const`]):
//! `id = '12'` reads key 12, `id = 1.5` reads nothing, and `name = 12` (a
//! string compared as a number) scans with the whole `WHERE`.

use bytes::Bytes;
use sprig_common::{Result, SprigError};

use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::txn::{LockMode, TxnOptions, Wait};

use super::assign::{self, Condition, Env, Level, Store};
use super::expr::{self, Args, CExpr};
use super::keys::{self, Space};
use super::meta;
use super::plan::{Access, Op, Plan, RowLock};
use super::row::{self, Datum};
use super::store::Tx;
use super::table::{ColDefault, ColType, Column, TableDef};
use super::temporal;
use super::{Engine, Pace};

/// What a statement did.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    /// Selected rows (the output columns' values).
    pub rows: Vec<Vec<Datum>>,
    /// Affected rows (writes).
    pub affected: u64,
    /// The first generated `AUTO_INCREMENT` value, or 0.
    pub last_insert_id: u64,
    /// The id the OK packet reports when none was generated (an `INSERT
    /// ... SELECT` reports its last row's), leaving `LAST_INSERT_ID()` as
    /// it was.
    pub reported_id: u64,
    /// The OK packet's info text.
    pub info: Option<String>,
    /// The scan used no index (`NO_INDEX_USED`).
    pub no_index: bool,
    /// Rows read from the store.
    pub examined: u64,
    /// The notes and warnings the statement raised.
    pub conds: Vec<Condition>,
}

impl Outcome {
    /// The warning count an OK or end-of-rows packet carries (notes
    /// included, as the servers count them).
    pub(crate) fn warnings(&self) -> u16 {
        u16::try_from(self.conds.len()).unwrap_or(u16::MAX)
    }
}

/// How many entries a scan reads per store call.
const SCAN_BATCH: usize = 256;

/// A `WHERE` constant as a key value of a column.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyConst {
    /// The column value the constant is equal to, and no other.
    Exact(Datum),
    /// No value of the column is equal to the constant.
    NoRow,
    /// The comparison isn't made in the column's type (a string column
    /// with a number), or the engine doesn't convert the pair: the rows
    /// are compared one by one.
    Scan,
}

/// The value of column `c` that `d` is equal to, as the comparison of the
/// two sees them.
pub(crate) fn key_const(d: &Datum, c: &Column, env: &Env) -> KeyConst {
    if d.is_null() {
        return KeyConst::NoRow;
    }
    match c.ty {
        ColType::Int { .. } => int_key(d, c.ty),
        ColType::Decimal { precision, scale } => decimal_key(d, precision, scale),
        ColType::Float | ColType::Double => float_key(d, c),
        ColType::Date
        | ColType::DateTime { .. }
        | ColType::Timestamp { .. }
        | ColType::Time { .. }
        | ColType::Year => temporal_key(d, c, env),
        // Byte strings compare byte by byte with strings; with a number
        // they compare as numbers.
        ColType::Binary { bytes } => match d {
            Datum::Str(s) | Datum::Bytes(s) if s.len() == usize::from(bytes) => {
                KeyConst::Exact(Datum::Bytes(s.clone()))
            }
            Datum::Str(_) | Datum::Bytes(_) => KeyConst::NoRow,
            _ => KeyConst::Scan,
        },
        ColType::VarBinary { .. } | ColType::Blob { .. } => match d {
            Datum::Str(s) | Datum::Bytes(s) => KeyConst::Exact(Datum::Bytes(s.clone())),
            _ => KeyConst::Scan,
        },
        ColType::Enum { members } => match d {
            // With a string an ENUM compares as its member's text.
            Datum::Str(s) => {
                let cx = expr::CmpCx {
                    collation: c.collation,
                    mb3: c.mb3,
                    text_right: true,
                    ..expr::CmpCx::default()
                };
                let mut found = None;
                for i in 0..=members {
                    let text = Datum::Str(expr::enum_text(&c.members, i));
                    match expr::compare(&text, d, &cx, env, "=") {
                        Ok(Some(std::cmp::Ordering::Equal)) => {
                            found = Some(i);
                            break;
                        }
                        Ok(_) => {}
                        // Text the column's character set can't hold:
                        // the row-by-row comparison reports it.
                        Err(_) => return KeyConst::Scan,
                    }
                }
                let _ = s;
                found.map_or(KeyConst::NoRow, |i| KeyConst::Exact(Datum::Enum(i)))
            }
            Datum::Int(_) | Datum::UInt(_) => match int_key(d, ColType::smallint(true)) {
                KeyConst::Exact(Datum::UInt(n)) if n <= u64::from(members) => {
                    KeyConst::Exact(Datum::Enum(u16::try_from(n).unwrap_or(0)))
                }
                KeyConst::Scan => KeyConst::Scan,
                _ => KeyConst::NoRow,
            },
            _ => KeyConst::Scan,
        },
        ColType::Set { members } => match d {
            Datum::Int(_) | Datum::UInt(_) => match int_key(d, ColType::bigint(true)) {
                KeyConst::Exact(Datum::UInt(n)) if members >= 64 || n < 1u64 << members => {
                    KeyConst::Exact(Datum::Set(n))
                }
                KeyConst::Scan => KeyConst::Scan,
                _ => KeyConst::NoRow,
            },
            _ => KeyConst::Scan,
        },
        ColType::Bit { bits } => match d {
            Datum::Int(_) | Datum::UInt(_) | Datum::Bytes(_) => {
                match int_key(d, ColType::bigint(true)) {
                    KeyConst::Exact(Datum::UInt(n)) if bits >= 64 || n < 1u64 << bits => {
                        KeyConst::Exact(Datum::Bit(n))
                    }
                    KeyConst::Scan => KeyConst::Scan,
                    _ => KeyConst::NoRow,
                }
            }
            _ => KeyConst::Scan,
        },
        ColType::Char { .. } | ColType::VarChar { .. } | ColType::Text { .. } => match d {
            Datum::Str(s) => {
                let charset = c.collation.map_or(Charset::Utf8mb4, Collation::charset);
                match charset {
                    // A utf8mb3 column holds no character of four bytes:
                    // whether one in the constant matters is the
                    // collation's to say, row by row.
                    Charset::Utf8mb4 if !c.mb3 || !s.iter().any(|b| *b >= 0xF0) => {
                        KeyConst::Exact(Datum::Str(s.clone()))
                    }
                    // The statement's text is UTF-8; these columns hold
                    // another encoding, the same for ASCII.
                    _ if s.is_ascii() => KeyConst::Exact(Datum::Str(s.clone())),
                    Charset::Latin1 => match std::str::from_utf8(s).ok().and_then(|t| {
                        t.chars()
                            .map(assign::latin1_byte)
                            .collect::<Option<Vec<u8>>>()
                    }) {
                        Some(b) => KeyConst::Exact(Datum::Str(Bytes::from(b))),
                        None => KeyConst::Scan,
                    },
                    _ => KeyConst::Scan,
                }
            }
            // A string column with a number compares as numbers.
            _ => KeyConst::Scan,
        },
        ColType::Json => KeyConst::Scan,
    }
}

/// A `DECIMAL(precision, scale)` key value: the constant at the column's
/// scale, when it has no more digits than the column.
fn decimal_key(d: &Datum, precision: u8, scale: u8) -> KeyConst {
    let num = match d {
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Bytes(_) => {
            assign::number_digits(d)
        }
        // With a double or a string the comparison is a double's: one
        // column value at most is equal while the column's digits fit a
        // double exactly.
        Datum::Float(_) | Datum::Double(_) if precision <= 15 => assign::number_digits(d),
        Datum::Str(s) if precision <= 15 => {
            let p = assign::num_prefix(s);
            if p.any {
                Some(p)
            } else {
                assign::number_digits(&Datum::Int(0))
            }
        }
        _ => None,
    };
    let Some(num) = num else {
        return KeyConst::Scan;
    };
    if num.point.unsigned_abs() > 80 {
        return KeyConst::NoRow;
    }
    let (int, frac, dropped) = num.to_scale(scale);
    if dropped || int.len() > usize::from(precision - scale) {
        return KeyConst::NoRow;
    }
    match sprig_engine::row::Decimal::from_parts(num.negative, &int, &frac) {
        Some(x) => KeyConst::Exact(Datum::Decimal(x)),
        None => KeyConst::NoRow,
    }
}

/// A `FLOAT` or `DOUBLE` key value: the comparison is a double's, so the
/// constant as a double, when the column can hold exactly that.
fn float_key(d: &Datum, c: &Column) -> KeyConst {
    if !matches!(
        d,
        Datum::Int(_)
            | Datum::UInt(_)
            | Datum::Decimal(_)
            | Datum::Float(_)
            | Datum::Double(_)
            | Datum::Str(_)
            | Datum::Bytes(_)
    ) {
        return KeyConst::Scan;
    }
    let v = expr::as_f64(d);
    if !v.is_finite() {
        return KeyConst::NoRow;
    }
    if matches!(c.ty, ColType::Double) {
        return KeyConst::Exact(Datum::Double(v));
    }
    // A FLOAT(M,D) column compares as its decimals print: row by row.
    if c.float_md.is_some() {
        return KeyConst::Scan;
    }
    // In range, or not equal to any FLOAT: the cast rounds.
    #[allow(clippy::cast_possible_truncation)]
    let f = v as f32;
    // Exactly equal, or no FLOAT is: that is the comparison's own test.
    #[allow(clippy::float_cmp)]
    let same = f.is_finite() && f64::from(f) == v;
    if same {
        KeyConst::Exact(Datum::Float(f))
    } else {
        KeyConst::NoRow
    }
}

/// A date or time key value: the constant read as the column's type,
/// when the column can hold exactly what it says. Text that is no date,
/// or carries other text, is left to the row-by-row comparison (which
/// knows each port's answer to it).
fn temporal_key(d: &Datum, c: &Column, env: &Env) -> KeyConst {
    use sprig_engine::row::temporal::frac_fits;
    let moment = |d: &Datum| -> Option<temporal::Read> {
        match d {
            Datum::Str(s) => temporal::datetime_from_str(s),
            Datum::Date(x) => Some(temporal::Read {
                dt: sprig_engine::row::DateTime::new(*x, 0, 0, 0, 0),
                has_time: false,
                seventh: 0,
                long_fraction: false,
                garbage: false,
            }),
            Datum::DateTime(x) => Some(temporal::Read {
                dt: *x,
                has_time: true,
                seventh: 0,
                long_fraction: false,
                garbage: false,
            }),
            Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) => {
                assign::number_digits(d).and_then(|p| temporal::datetime_from_number(&p))
            }
            _ => None,
        }
    };
    match c.ty {
        ColType::Year => match assign::year_number(d) {
            Some((n, _)) => match temporal::year_from_int(n) {
                Some(y) => KeyConst::Exact(Datum::Year(y)),
                None => KeyConst::NoRow,
            },
            None => KeyConst::Scan,
        },
        ColType::Time { fsp } => {
            let r = match d {
                Datum::Time(t) => return KeyConst::Exact(Datum::Time(*t)),
                Datum::Str(s) => temporal::time_from_str(s),
                Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) => {
                    assign::number_digits(d).and_then(|p| temporal::time_from_number(&p))
                }
                _ => return KeyConst::Scan,
            };
            let Some(r) = r.filter(|r| !r.garbage) else {
                return KeyConst::Scan;
            };
            let frac = u32::try_from(r.micros.unsigned_abs() % 1_000_000).unwrap_or(0);
            match sprig_engine::row::Time::from_micros(r.micros) {
                Some(t) if r.seventh == 0 && !r.long_fraction && frac_fits(frac, fsp) => {
                    KeyConst::Exact(Datum::Time(t))
                }
                _ => KeyConst::NoRow,
            }
        }
        ColType::Date | ColType::DateTime { .. } | ColType::Timestamp { .. } => {
            if let (Datum::Timestamp(t), ColType::Timestamp { .. }) = (d, c.ty) {
                return KeyConst::Exact(Datum::Timestamp(*t));
            }
            let Some(r) = moment(d).filter(|r| !r.garbage) else {
                return KeyConst::Scan;
            };
            if r.seventh != 0 || r.long_fraction {
                return KeyConst::NoRow;
            }
            let x = r.dt;
            match c.ty {
                ColType::Date => {
                    if x.hour != 0 || x.minute != 0 || x.second != 0 || x.micros != 0 {
                        KeyConst::NoRow
                    } else {
                        KeyConst::Exact(Datum::Date(x.date))
                    }
                }
                ColType::DateTime { fsp } if frac_fits(x.micros, fsp) => {
                    KeyConst::Exact(Datum::DateTime(x))
                }
                ColType::Timestamp { fsp } if frac_fits(x.micros, fsp) => {
                    let Some(tz) = env.tz else {
                        return KeyConst::Scan;
                    };
                    match temporal::to_timestamp(x, tz, false) {
                        Some(t) => KeyConst::Exact(Datum::Timestamp(t)),
                        None => KeyConst::NoRow,
                    }
                }
                _ => KeyConst::NoRow,
            }
        }
        _ => KeyConst::Scan,
    }
}

fn int_key(d: &Datum, ty: ColType) -> KeyConst {
    let Some((lo, hi)) = assign::int_range(ty) else {
        return KeyConst::Scan;
    };
    let exact = |v: i128| {
        if v < lo || v > hi {
            return KeyConst::NoRow;
        }
        KeyConst::Exact(match ty {
            ColType::Int { unsigned: true, .. } => Datum::UInt(u64::try_from(v).unwrap_or(0)),
            _ => Datum::Int(i64::try_from(v).unwrap_or(0)),
        })
    };
    match d {
        Datum::Int(v) => exact(i128::from(*v)),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => exact(i128::from(*v)),
        Datum::Enum(v) | Datum::Year(v) => exact(i128::from(*v)),
        Datum::Bytes(b) => match assign::bytes_number(b) {
            Some(n) => exact(i128::from(n)),
            None => KeyConst::NoRow,
        },
        Datum::Decimal(x) => {
            if x.frac_digits().iter().any(|f| *f != 0) {
                return KeyConst::NoRow;
            }
            exact(assign::decimal_rounded(x))
        }
        Datum::Float(_) | Datum::Double(_) => {
            let f = expr::as_f64(d);
            // Past 2^53 a double is equal to several integers.
            if !f.is_finite() || f.abs() >= 9_007_199_254_740_992.0 {
                return KeyConst::Scan;
            }
            if f.fract() != 0.0 {
                return KeyConst::NoRow;
            }
            // Whole and below 2^53: the cast is exact.
            #[allow(clippy::cast_possible_truncation)]
            let v = f as i128;
            exact(v)
        }
        Datum::Str(s) => {
            let p = assign::num_prefix(s);
            if !p.any {
                // Text without a number compares as 0.
                return exact(0);
            }
            // The two servers compare an integer with a string in
            // different types (exact, or a double): the same answer while
            // the number has few enough digits for both.
            if p.digits.len() > 15 || p.point.unsigned_abs() > 15 {
                return KeyConst::Scan;
            }
            match p.decimal() {
                Some(x) if x.frac_digits().iter().all(|f| *f == 0) => {
                    exact(assign::decimal_rounded(&x))
                }
                Some(_) => KeyConst::NoRow,
                None => KeyConst::Scan,
            }
        }
        _ => KeyConst::Scan,
    }
}

/// 1062 as MariaDB words it, for the values of columns `cols` of `row`.
pub(crate) fn duplicate(def: &TableDef, cols: &[usize], row: &[Datum], key: &str) -> SprigError {
    // The key's parts: a part on a column prefix shows the prefix only.
    let bare = key
        .strip_prefix(def.name.as_str())
        .and_then(|k| k.strip_prefix('.'))
        .unwrap_or(key);
    let parts = if bare == def.pk_name {
        Some(&def.pk_parts[..])
    } else {
        def.indexes
            .iter()
            .find(|x| x.name == bare)
            .map(|x| &x.parts[..])
    };
    // A hidden row number is no column: it is never part of the text.
    let v: Vec<String> = cols
        .iter()
        .enumerate()
        .filter(|&(_, &i)| i < def.columns.len())
        .map(|(n, &i)| {
            meta::text(&def.columns[i], &row[i]).map_or_else(
                || "NULL".into(),
                |t| {
                    let t = String::from_utf8_lossy(&t).into_owned();
                    match parts.and_then(|p| p.get(n)).and_then(|kc| kc.prefix) {
                        Some(len) => t.chars().take(len as usize).collect(),
                        None => t,
                    }
                },
            )
        })
        .collect();
    SprigError::standard(
        1062,
        *b"23000",
        format!("Duplicate entry '{}' for key '{key}'", v.join("-")),
    )
}

/// Row-level security's `WITH CHECK` on a new row version: 39363 unless
/// it holds (a `NULL` result fails, as in MariaDB's write triggers, whose
/// message text this is, so a client sees the same error on either kind
/// of database).
fn check_new_row(plan: &Plan, row: &[Datum], a: &Args<'_>) -> Result<()> {
    if let Some((c, db, view, mysql)) = &plan.view_check
        && row::truth(&expr::eval(c, row, a)?) != Some(true)
    {
        return Err(super::view::check_failed(db, view, *mysql));
    }
    let Some(c) = &plan.check else {
        return Ok(());
    };
    if row::truth(&expr::eval(c, row, a)?) == Some(true) {
        return Ok(());
    }
    Err(SprigError::new(
        sprig_common::SprigCode::RlsCheckFailed,
        format!(
            "ER_SPRIG_RLS_CHECK_FAILED: new row violates row-level security policy for table '{}'",
            String::from_utf8_lossy(&plan.org_table)
        ),
    ))
}

/// A statement gave generated column `c` a value: MySQL refuses (3105);
/// MariaDB refuses in strict mode (1906) and otherwise ignores the value
/// with a warning.
fn generated_given(
    plan: &Plan,
    c: &Column,
    a: &Args<'_>,
    conds: &mut Vec<Condition>,
) -> Result<()> {
    let table = String::from_utf8_lossy(&plan.org_table);
    if a.env.mysql {
        return Err(SprigError::standard(
            3105,
            *b"HY000",
            format!(
                "The value specified for generated column '{}' in table '{table}' is not allowed.",
                c.name
            ),
        ));
    }
    let m = format!(
        "The value specified for generated column '{}' in table '{table}' has been ignored",
        c.name
    );
    if a.env.strict {
        return Err(SprigError::standard(1906, *b"HY000", m));
    }
    conds.push(Condition {
        level: Level::Warning,
        code: 1906,
        message: m,
    });
    Ok(())
}

/// The table's `CHECK` constraints and partitioning on a new row version: the server's
/// error (4025 on MariaDB's port, 3819 on MySQL's) for the first that is
/// false.
fn check_constraints(plan: &Plan, row: &[Datum], a: &Args<'_>) -> Result<()> {
    let def = &*plan.table;
    if !def.checks.is_empty()
        && let Some(ch) = def.failed_check(row, a, None)?
    {
        return Err(def.check_error(ch, &String::from_utf8_lossy(&plan.db_name), a.env.mysql));
    }
    // A partitioned table: a partition takes the row (1526), one the
    // statement's `PARTITION (...)` names (1748).
    super::partition::check_row(def, row, a)
}

/// 1020 as MariaDB 11.8 words it, naming the table the client wrote.
fn name_table(e: SprigError, plan: &Plan) -> SprigError {
    if matches!(
        e.kind,
        sprig_common::ErrorKind::Standard { errno: 1020, .. }
    ) {
        return SprigError::standard(
            1020,
            *b"HY000",
            format!(
                "Record has changed since last read in table '{}'; try restarting transaction",
                String::from_utf8_lossy(&plan.org_table)
            ),
        );
    }
    e
}

/// The store context of one statement on `plan`'s table.
fn store_cx<'a>(plan: &'a Plan, env: Env, conds: &'a mut Vec<Condition>) -> Store<'a> {
    Store {
        env,
        db: &plan.db_name,
        table: &plan.table.name,
        row: 1,
        conds,
    }
}

/// The count an `UPDATE`'s and a multi-row `INSERT`'s info text reports
/// as "Warnings": every condition, notes included, as on the servers.
pub(crate) fn warning_count(conds: &[Condition]) -> usize {
    conds.len()
}

impl Engine {
    /// Runs `plan` with `params` in its own transaction (`opts`, from the
    /// session) under the default rules (strict mode, UTC), and waits
    /// until its effects and what it read are durable.
    ///
    /// # Errors
    /// As for [`Engine::run_env`].
    #[cfg(test)]
    pub(crate) async fn run(
        &self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        opts: TxnOptions,
    ) -> Result<Outcome> {
        self.run_env(tenant, plan, params, found_rows, opts, Env::STRICT)
            .await
    }

    /// Runs `plan` with `params` in its own transaction (`opts`, from the
    /// session) under the session's rules `env` (its `sql_mode` and time
    /// zone), and waits until its effects and what it read are durable.
    ///
    /// # Errors
    /// The statement's (strict mode, duplicates), a lock error (1205,
    /// 1213), or the engine's.
    pub(crate) async fn run_env(
        &self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        opts: TxnOptions,
        env: Env,
    ) -> Result<Outcome> {
        let sp = self.space(tenant, &plan.table).await?;
        let mut tx = self.begin(opts)?;
        tx.set_env(env);
        let a = Args {
            params,
            env,
            now: std::sync::OnceLock::new(),
            conds: crate::engine::expr::Conds::default(),
        };
        let o = self.statement(&sp, &mut tx, plan, &a, found_rows).await?;
        tx.finish().await?;
        Ok(o)
    }

    /// Runs `plan` as one statement of the session's transaction `tx`,
    /// under the rules the transaction was last given ([`Tx::set_env`]),
    /// and waits until what it read is durable. On an error the
    /// statement's writes are undone and the transaction stays open with
    /// its locks, unless the engine rolled the whole transaction back
    /// (1213, 1020, 1206; `tx.is_open()` is then false).
    ///
    /// # Errors
    /// As for [`Engine::run_env`], and 1020.
    pub(crate) async fn run_in(
        &self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        tx: &mut Tx,
    ) -> Result<Outcome> {
        let sp = if let Some(sp) = tx.held(&plan.table) {
            sp
        } else {
            let sp = self.space(tenant, &plan.table).await?;
            tx.hold(&plan.table, &sp);
            sp
        };
        let a = Args {
            params,
            env: tx.env(),
            now: std::sync::OnceLock::new(),
            conds: crate::engine::expr::Conds::default(),
        };
        tx.begin_statement();
        let r = match self.statement(&sp, tx, plan, &a, found_rows).await {
            Ok(o) => tx.gate().await.map(|()| o),
            Err(e) => Err(e),
        };
        match r {
            Ok(o) => Ok(o),
            Err(e) => {
                tx.rollback_statement();
                Err(name_table(e, plan))
            }
        }
    }

    /// Runs `plan` as a part of the statement `tx` is running (a write of
    /// one row of a multi-table write): no statement of its own, so an
    /// error leaves the undoing to the caller.
    ///
    /// # Errors
    /// As for [`Engine::run_in`].
    pub(crate) async fn run_part(
        &self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        tx: &mut Tx,
    ) -> Result<Outcome> {
        let sp = if let Some(sp) = tx.held(&plan.table) {
            sp
        } else {
            let sp = self.space(tenant, &plan.table).await?;
            tx.hold(&plan.table, &sp);
            sp
        };
        let a = Args {
            params,
            env: tx.env(),
            now: std::sync::OnceLock::new(),
            conds: crate::engine::expr::Conds::default(),
        };
        self.statement(&sp, tx, plan, &a, found_rows)
            .await
            .map_err(|e| name_table(e, plan))
    }

    /// One statement in `tx`, with the conditions its expressions raised
    /// (reading the rows) ahead of those its writes raised.
    async fn statement(
        &self,
        sp: &Space,
        tx: &mut Tx,
        plan: &Plan,
        a: &Args<'_>,
        found_rows: bool,
    ) -> Result<Outcome> {
        let mut o = Box::pin(self.statement_rows(sp, tx, plan, a, found_rows)).await?;
        let raised = a.take_conds();
        if !raised.is_empty() {
            let mut all = raised;
            all.append(&mut o.conds);
            o.conds = all;
            // An UPDATE's info counts every condition.
            if let Some(info) = &o.info
                && let Some(at) = info.find("Warnings: ")
            {
                o.info = Some(format!(
                    "{}Warnings: {}",
                    &info[..at],
                    warning_count(&o.conds)
                ));
            }
        }
        Ok(o)
    }

    /// One statement in `tx`.
    #[allow(clippy::too_many_lines)] // one arm per statement kind
    async fn statement_rows(
        &self,
        sp: &Space,
        tx: &mut Tx,
        plan: &Plan,
        a: &Args<'_>,
        found_rows: bool,
    ) -> Result<Outcome> {
        let def = &*plan.table;
        Ok(match &plan.op {
            Op::Select { out } => {
                let mut o = Outcome::default();
                let mut skip = plan.offset;
                let limit = plan.limit.unwrap_or(u64::MAX);
                let mut produced = 0u64;
                let mut rows = Vec::new();
                // A locking read, and any read of a SERIALIZABLE
                // transaction: each row is locked and read as it is now.
                let lock = plan.lock.map(Locking::from).or_else(|| {
                    tx.shared_reads().then_some(Locking {
                        mode: LockMode::Shared,
                        wait: Wait::Block,
                    })
                });
                if let Some(lk) = lock {
                    let max = limit.saturating_add(skip);
                    let found = locked_rows(sp, tx, plan, a, &mut o, lk, max, false).await?;
                    let skip = usize::try_from(skip).unwrap_or(usize::MAX);
                    rows.extend(
                        found
                            .into_iter()
                            .skip(skip)
                            .map(|(_, r)| out.iter().map(|oc| r[oc.col].clone()).collect()),
                    );
                } else if limit > 0 {
                    visit(sp, tx, plan, a, &mut o, &mut |_, r| {
                        if skip > 0 {
                            skip -= 1;
                            return Ok(true);
                        }
                        produced += 1;
                        rows.push(out.iter().map(|oc| r[oc.col].clone()).collect());
                        Ok(produced < limit)
                    })
                    .await?;
                }
                o.rows = rows;
                o
            }
            Op::Insert { cols, rows, on_dup } => {
                Box::pin(self.insert(sp, tx, plan, a, cols, rows, on_dup, found_rows)).await?
            }
            Op::Update { sets } => {
                let mut o = Outcome::default();
                let matched = locked_rows(
                    sp,
                    tx,
                    plan,
                    a,
                    &mut o,
                    Locking::WRITE,
                    plan.limit.unwrap_or(u64::MAX),
                    true,
                )
                .await?;
                let (mut changed, n) = (0u64, matched.len());
                let mut conds = Vec::new();
                let mut pace = Pace::default();
                for (ri, (key, old)) in matched.into_iter().enumerate() {
                    pace.add(1).await;
                    // Left to right, as MariaDB assigns: a later assignment
                    // sees the values earlier ones stored.
                    let mut new = old.clone();
                    let mut cx = store_cx(plan, a.env, &mut conds);
                    cx.row = ri + 1;
                    for (ci, e) in sets {
                        if def.columns[*ci].generated.is_some() {
                            generated_given(plan, &def.columns[*ci], a, cx.conds)?;
                            continue;
                        }
                        let v = expr::eval(e, &new, a)?;
                        new[*ci] = assign::store(&mut cx, &def.columns[*ci], v)?;
                    }
                    // Generated columns follow the columns they read.
                    for &ci in &def.generated_cols {
                        let c = &def.columns[ci];
                        if let Some(g) = &c.generated {
                            let v = expr::eval(&g.expr, &new, a)?;
                            new[ci] = assign::store(&mut cx, c, v)?;
                        }
                    }
                    if new != old {
                        // `ON UPDATE CURRENT_TIMESTAMP` columns the
                        // statement didn't set follow a row that changes.
                        for (ci, c) in def.columns.iter().enumerate() {
                            if c.on_update_now && !sets.iter().any(|(s, _)| *s == ci) {
                                new[ci] = now_value(c, a)?;
                            }
                        }
                    }
                    check_new_row(plan, &new, a)?;
                    if new == old {
                        continue;
                    }
                    check_constraints(plan, &new, a)?;
                    changed += 1;
                    if let Some(g) = plan.fks.as_deref().filter(|_| !a.env.no_fk_checks) {
                        let mut chain = super::fk::Chain::of(def.object, &key, true);
                        self.fk_children(
                            sp.tenant,
                            tx,
                            g,
                            def.object,
                            &old,
                            Some(&new),
                            0,
                            &mut chain,
                        )
                        .await?;
                        replace_row(sp, def, tx, key, &old, &new).await?;
                        self.fk_parents(sp.tenant, tx, g, def.object, &new, Some(&old))
                            .await?;
                        continue;
                    }
                    replace_row(sp, def, tx, key, &old, &new).await?;
                }
                o.affected = if found_rows { n as u64 } else { changed };
                o.info = Some(format!(
                    "Rows matched: {n}  Changed: {changed}  Warnings: {}",
                    warning_count(&conds)
                ));
                o.conds = conds;
                o
            }
            Op::Delete => {
                let mut o = Outcome::default();
                let matched = locked_rows(
                    sp,
                    tx,
                    plan,
                    a,
                    &mut o,
                    Locking::WRITE,
                    plan.limit.unwrap_or(u64::MAX),
                    false,
                )
                .await?;
                let fks = plan.fks.as_deref().filter(|_| !a.env.no_fk_checks);
                let mut deleted = 0u64;
                let mut pace = Pace::default();
                for (key, old) in matched {
                    pace.add(1).await;
                    if let Some(g) = fks {
                        // A cascade of an earlier row may have taken this
                        // one already (a table that references itself).
                        if g.refers_to_itself(def.object)
                            && !matches!(
                                tx.lock_key(&key, LockMode::Exclusive).await?,
                                Some(Some(_))
                            )
                        {
                            continue;
                        }
                        let mut chain = super::fk::Chain::of(def.object, &key, false);
                        self.fk_children(sp.tenant, tx, g, def.object, &old, None, 0, &mut chain)
                            .await?;
                    }
                    delete_row(sp, def, tx, key, &old).await?;
                    deleted += 1;
                }
                o.affected = deleted;
                o
            }
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn insert(
        &self,
        sp: &Space,
        tx: &mut Tx,
        plan: &Plan,
        a: &Args<'_>,
        cols: &[usize],
        rows: &[Vec<Option<expr::CExpr>>],
        on_dup: &super::plan::OnDup,
        found_rows: bool,
    ) -> Result<Outcome> {
        use super::plan::OnDup;
        let def = &*plan.table;
        let mut o = Outcome::default();
        // `INSERT IGNORE`: what strict mode refuses is a warning.
        let mut env = a.env;
        if matches!(on_dup, OnDup::Ignore) {
            env.strict = false;
        }
        let ncols = def.columns.len();
        let mut dups = 0u64;
        let mut conds = Vec::new();
        // MySQL 8.4 deprecates `VALUES(c)` in `ON DUPLICATE KEY UPDATE`.
        if let OnDup::Update(_, uses) = on_dup
            && env.mysql
        {
            for _ in 0..*uses {
                conds.push(Condition {
                    level: Level::Warning,
                    code: 1287,
                    message: "'VALUES function' is deprecated and will be removed in a future \
                              release. Please use an alias (INSERT INTO ... VALUES (...) AS \
                              alias) and replace VALUES(col) in the ON DUPLICATE KEY UPDATE \
                              clause with alias.col instead"
                        .into(),
                });
            }
        }
        let mut pace = Pace::default();
        for (ri, exprs) in rows.iter().enumerate() {
            pace.add(1).await;
            let mut cx = store_cx(plan, env, &mut conds);
            cx.row = ri + 1;
            // What the statement gives each column: a value, or nothing
            // (the column isn't listed, or its value is `DEFAULT`).
            let mut vals: Vec<Option<Datum>> = vec![None; def.columns.len()];
            for (pos, e) in exprs.iter().enumerate() {
                if let Some(e) = e {
                    vals[cols[pos]] = Some(expr::eval(e, &[], a)?);
                }
            }
            let mut row = Vec::with_capacity(def.columns.len());
            // Columns whose value is their DEFAULT expression: worked out
            // over the row once the others have theirs.
            let mut computed: Vec<usize> = Vec::new();
            for (ci, v) in vals.into_iter().enumerate() {
                let c = &def.columns[ci];
                if c.generated.is_some() {
                    if v.is_some() {
                        generated_given(plan, c, a, cx.conds)?;
                    }
                    computed.push(ci);
                    row.push(Datum::Null);
                    continue;
                }
                let generate = c.auto_inc
                    && match &v {
                        None | Some(Datum::Null) => true,
                        Some(Datum::Int(0) | Datum::UInt(0)) => !a.env.no_auto_value_on_zero,
                        Some(_) => false,
                    };
                if generate {
                    let id = self.next_autoinc(sp, def)?;
                    if o.last_insert_id == 0 {
                        o.last_insert_id = id;
                    }
                    let d = match c.ty {
                        ColType::Int { unsigned: true, .. } => Datum::UInt(id),
                        _ => Datum::Int(i64::try_from(id).unwrap_or(i64::MAX)),
                    };
                    row.push(assign::store(&mut cx, c, d)?);
                    continue;
                }
                let stored = match v {
                    Some(Datum::Null) if !c.nullable && !a.env.strict && rows.len() > 1 => {
                        // A multi-row INSERT outside strict mode stores the
                        // type's own default for NULL in a NOT NULL column.
                        cx.conds.push(Condition {
                            level: Level::Warning,
                            code: 1048,
                            message: format!("Column '{}' cannot be null", c.name),
                        });
                        c.implicit_default()
                    }
                    Some(v) => assign::store(&mut cx, c, v)?,
                    None => match &c.default {
                        Some(ColDefault::Value(d)) => d.clone(),
                        Some(ColDefault::Now) => now_value(c, a)?,
                        Some(ColDefault::Expr(_)) => {
                            computed.push(ci);
                            Datum::Null
                        }
                        None if c.nullable => Datum::Null,
                        None => {
                            let m = format!("Field '{}' doesn't have a default value", c.name);
                            if env.strict {
                                return Err(SprigError::standard(1364, *b"HY000", m));
                            }
                            cx.conds.push(Condition {
                                level: Level::Warning,
                                code: 1364,
                                message: m,
                            });
                            c.implicit_default()
                        }
                    },
                };
                if c.auto_inc {
                    self.seen_autoinc(sp, def, &stored)?;
                }
                row.push(stored);
            }
            for ci in computed {
                let c = &def.columns[ci];
                let e = match (&c.generated, &c.default) {
                    (Some(g), _) => &g.expr,
                    (None, Some(ColDefault::Expr(d))) => &d.expr,
                    _ => continue,
                };
                let v = expr::eval(e, &row, a)?;
                row[ci] = assign::store(&mut cx, c, v)?;
            }
            check_new_row(plan, &row, a)?;
            check_constraints(plan, &row, a)?;
            if def.hidden {
                row.push(Datum::UInt(self.next_rowid(sp)?));
            }
            // A key another row has: what the statement says to do.
            if !matches!(on_dup, OnDup::Error) {
                let mut done = false;
                while let Some((ckey, old, kcols, kname)) = conflict(sp, def, tx, &row).await? {
                    match on_dup {
                        OnDup::Ignore => {
                            // MySQL names the key with its table.
                            let kname = if env.mysql {
                                format!("{}.{kname}", def.name)
                            } else {
                                kname
                            };
                            let e = duplicate(def, &kcols, &row, &kname);
                            dups += 1;
                            cx.conds.push(Condition {
                                level: Level::Warning,
                                code: 1062,
                                message: e.message,
                            });
                            done = true;
                        }
                        OnDup::Replace => {
                            delete_row(sp, def, tx, ckey, &old).await?;
                            o.affected += 1;
                            dups += 1;
                            continue;
                        }
                        OnDup::Update(sets, _) => {
                            let mut both: Vec<Datum> = old[..ncols.min(old.len())].to_vec();
                            both.extend_from_slice(&row[..ncols.min(row.len())]);
                            let mut new = old.clone();
                            // Left to right, as both servers assign: a later
                            // assignment reads the values earlier ones stored.
                            for (ci, e) in sets {
                                if def.columns[*ci].generated.is_some() {
                                    generated_given(plan, &def.columns[*ci], a, cx.conds)?;
                                    continue;
                                }
                                let v = expr::eval(e, &both, a)?;
                                new[*ci] = assign::store(&mut cx, &def.columns[*ci], v)?;
                                both[*ci] = new[*ci].clone();
                            }
                            // Generated columns follow the columns they read.
                            for &ci in &def.generated_cols {
                                let c = &def.columns[ci];
                                if let Some(g) = &c.generated {
                                    let v = expr::eval(&g.expr, &new, a)?;
                                    new[ci] = assign::store(&mut cx, c, v)?;
                                }
                            }
                            if new != old {
                                for (ci, c) in def.columns.iter().enumerate() {
                                    if c.on_update_now && !sets.iter().any(|(s, _)| *s == ci) {
                                        new[ci] = now_value(c, a)?;
                                    }
                                }
                            }
                            if new == old {
                                if found_rows {
                                    o.affected += 1;
                                }
                            } else {
                                check_constraints(plan, &new, a)?;
                                replace_row(sp, def, tx, ckey, &old, &new).await?;
                                o.affected += 2;
                            }
                            dups += 1;
                            done = true;
                        }
                        OnDup::Error => {}
                    }
                    break;
                }
                if done {
                    continue;
                }
            }
            insert_row(sp, def, tx, &row).await?;
            if let Some(g) = plan.fks.as_deref().filter(|_| !a.env.no_fk_checks) {
                self.fk_parents(sp.tenant, tx, g, def.object, &row, None)
                    .await?;
            }
            o.affected += 1;
        }
        if rows.len() > 1 {
            o.info = Some(format!(
                "Records: {}  Duplicates: {dups}  Warnings: {}",
                rows.len(),
                warning_count(&conds)
            ));
        }
        o.conds = conds;
        Ok(o)
    }
}

/// The statement's time as column `c` stores it (`DEFAULT
/// CURRENT_TIMESTAMP`, `ON UPDATE CURRENT_TIMESTAMP`), cut to the column's
/// precision.
fn now_value(c: &Column, a: &Args<'_>) -> Result<Datum> {
    let (secs, micros) = *a.now.get_or_init(temporal::now);
    match c.ty {
        ColType::DateTime { fsp } => {
            let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
            let t = sprig_engine::row::Timestamp::new(
                u32::try_from(secs).unwrap_or(u32::MAX),
                micros - micros % unit,
            );
            Ok(Datum::DateTime(temporal::from_timestamp(
                t,
                assign::zone(a.env)?,
            )))
        }
        ColType::Timestamp { fsp } => {
            let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
            Ok(Datum::Timestamp(sprig_engine::row::Timestamp::new(
                u32::try_from(secs).unwrap_or(u32::MAX),
                micros - micros % unit,
            )))
        }
        _ => Err(super::not_on_engine(
            "CURRENT_TIMESTAMP as this column's value",
        )),
    }
}

/// The row whose primary or unique key new row `row` would take, locked:
/// its key, its values, and the key's columns and name. `None` when the
/// row's keys are free.
async fn conflict(
    sp: &Space,
    def: &TableDef,
    tx: &mut Tx,
    row: &[Datum],
) -> Result<Option<(Bytes, Vec<Datum>, Vec<usize>, String)>> {
    if !def.hidden {
        let key = keys::row_key_of(sp, def, row)
            .ok_or_else(|| SprigError::internal("engine: a key value of the wrong type"))?;
        let key = Bytes::from(key);
        if let Some(Some(raw)) = tx.lock_key(&key, LockMode::Exclusive).await? {
            let old = row::decode(def, sp, &key, &raw)?;
            return Ok(Some((key, old, def.pk.clone(), def.pk_name.clone())));
        }
    }
    for (ix, x) in def.indexes.iter().enumerate() {
        if !x.unique || x.cols.iter().any(|&c| row[c].is_null()) {
            continue;
        }
        let Some((k, _)) = keys::index_entry(sp, def, ix, row) else {
            continue;
        };
        let k = Bytes::from(k);
        let Some(Some(v)) = tx.lock_key(&k, LockMode::Exclusive).await? else {
            continue;
        };
        let Some(t) = keys::entry_target(sp, def, &k, &v) else {
            continue;
        };
        let mut rk = sp.rows();
        rk.extend_from_slice(t);
        let rk = Bytes::from(rk);
        if let Some(Some(raw)) = tx.lock_key(&rk, LockMode::Exclusive).await? {
            let old = row::decode(def, sp, &rk, &raw)?;
            return Ok(Some((rk, old, x.cols.clone(), x.name.clone())));
        }
    }
    Ok(None)
}

async fn insert_row(sp: &Space, def: &TableDef, tx: &mut Tx, row: &[Datum]) -> Result<()> {
    let key = keys::row_key_of(sp, def, row)
        .ok_or_else(|| SprigError::internal("engine: a key value of the wrong type"))?;
    if !tx
        .insert(Bytes::from(key), row::encode(def, sp, row)?, true)
        .await?
    {
        return Err(duplicate(def, &def.pk, row, &def.pk_name));
    }
    for ix in 0..def.indexes.len() {
        put_index_entry(sp, def, tx, ix, row).await?;
    }
    Ok(())
}

async fn put_index_entry(
    sp: &Space,
    def: &TableDef,
    tx: &mut Tx,
    ix: usize,
    row: &[Datum],
) -> Result<()> {
    let x = &def.indexes[ix];
    let Some((k, v)) = keys::index_entry(sp, def, ix, row) else {
        return Err(SprigError::internal(
            "engine: an index value of the wrong type",
        ));
    };
    // A unique entry without NULL parts is keyed by its values alone.
    if x.unique && !x.cols.iter().any(|&c| row[c].is_null()) {
        if !tx.insert(Bytes::from(k), v, false).await? {
            return Err(duplicate(def, &x.cols, row, &x.name));
        }
        return Ok(());
    }
    tx.put_new(Bytes::from(k), v).await
}

/// Deletes row `old` (at `key`) and its index entries.
pub(crate) async fn delete_row(
    sp: &Space,
    def: &TableDef,
    tx: &mut Tx,
    key: Bytes,
    old: &[Datum],
) -> Result<()> {
    tx.delete(key).await?;
    for ix in 0..def.indexes.len() {
        if let Some((k, _)) = keys::index_entry(sp, def, ix, old) {
            tx.delete(Bytes::from(k)).await?;
        }
    }
    Ok(())
}

/// Replaces row `old` (at `key`) by `new`, touching only the index
/// entries that change.
pub(crate) async fn replace_row(
    sp: &Space,
    def: &TableDef,
    tx: &mut Tx,
    key: Bytes,
    old: &[Datum],
    new: &[Datum],
) -> Result<()> {
    // The key's bytes decide: values that differ can still share a key
    // (`'a'` and `'A'` under a case-insensitive collation).
    let nk = keys::row_key_of(sp, def, new)
        .ok_or_else(|| SprigError::internal("engine: a key value of the wrong type"))?;
    let pk_same = nk[..] == key[..];
    let mut changed = Vec::new();
    for ix in 0..def.indexes.len() {
        let before = keys::index_entry(sp, def, ix, old);
        let after = keys::index_entry(sp, def, ix, new);
        if before != after {
            if let Some((k, _)) = before {
                tx.delete(Bytes::from(k)).await?;
            }
            changed.push(ix);
        }
    }
    if pk_same {
        tx.put(key, row::encode(def, sp, new)?).await?;
    } else {
        tx.delete(key).await?;
        if !tx
            .insert(Bytes::from(nk), row::encode(def, sp, new)?, true)
            .await?
        {
            return Err(duplicate(def, &def.pk, new, &def.pk_name));
        }
    }
    for ix in changed {
        put_index_entry(sp, def, tx, ix, new).await?;
    }
    Ok(())
}

/// How a statement locks the rows it reads.
#[derive(Debug, Clone, Copy)]
struct Locking {
    mode: LockMode,
    wait: Wait,
}

impl Locking {
    /// DML's: exclusive, waiting.
    const WRITE: Self = Self {
        mode: LockMode::Exclusive,
        wait: Wait::Block,
    };
}

impl From<RowLock> for Locking {
    fn from(l: RowLock) -> Self {
        Self {
            mode: if l.exclusive {
                LockMode::Exclusive
            } else {
                LockMode::Shared
            },
            wait: l.wait,
        }
    }
}

/// Where a statement's rows are, once its constants are key values.
enum Found {
    /// No row can match.
    Nothing,
    /// One row key.
    Point(Vec<u8>),
    /// Several row keys, in the order they are read.
    Points(Vec<Vec<u8>>),
    /// The entries `[lo, hi)` of secondary index `ix`.
    Index { ix: usize, lo: Vec<u8>, hi: Vec<u8> },
    /// The rows `[lo, hi)`; `all`: the whole table, read without a key.
    Rows { lo: Vec<u8>, hi: Vec<u8>, all: bool },
}

/// Whether a top-level `AND` term of `filter` is `column = constant`
/// with a constant no value of an integer, `DECIMAL` or `DATE` column is
/// equal to (`tiny = 1.5`, `price = 1.005` for a `DECIMAL(5,2)`, `day =
/// '2024-02-29 13:14:15'`).
fn impossible(def: &TableDef, filter: Option<&CExpr>, a: &Args<'_>) -> Result<bool> {
    let Some(f) = filter else {
        return Ok(false);
    };
    Ok(match f {
        CExpr::And(l, r) => impossible(def, Some(l), a)? || impossible(def, Some(r), a)?,
        CExpr::Cmp(expr::Cmp::Eq, l, r, _) => {
            let (col, c) = match (&**l, &**r) {
                (CExpr::Col(i), c @ (CExpr::Lit(_) | CExpr::Param(_)))
                | (c @ (CExpr::Lit(_) | CExpr::Param(_)), CExpr::Col(i)) => (&def.columns[*i], c),
                _ => return Ok(false),
            };
            if !matches!(
                col.ty,
                ColType::Int { .. } | ColType::Decimal { .. } | ColType::Date
            ) {
                return Ok(false);
            }
            let v = expr::eval(c, &[], a)?;
            !v.is_null() && key_const(&v, col, &a.env) == KeyConst::NoRow
        }
        _ => false,
    })
}

/// Resolves the plan's access path with this execution's constants, and
/// gives the filter that goes with it.
#[allow(clippy::too_many_lines)] // one arm per access path
fn locate<'p>(sp: &Space, plan: &'p Plan, a: &Args<'_>) -> Result<(Found, Option<&'p CExpr>)> {
    let def = &*plan.table;
    let p = sp.rows();
    let whole = |p: Vec<u8>| {
        let hi = keys::prefix_end(&p);
        Found::Rows {
            lo: p,
            hi,
            all: true,
        }
    };
    let part_failed = || SprigError::internal("engine: a key value of the wrong type");
    Ok(match &plan.access {
        Access::Point(vals) => {
            // The key grows in the prefix's own buffer: a point read
            // allocates once here.
            let prefix = p.len();
            let mut k = p;
            for ((e, &ci), kc) in vals.iter().zip(&def.pk).zip(def.pk_key(sp.first_format)) {
                let v = expr::eval(e, &[], a)?;
                // The hidden row number (a write through a join finds a
                // table's rows by it): the value read, as it is.
                let kv = match def.columns.get(ci) {
                    Some(col) => key_const(&v, col, &a.env),
                    None => match v {
                        Datum::UInt(_) => KeyConst::Exact(v.clone()),
                        _ => KeyConst::NoRow,
                    },
                };
                match kv {
                    KeyConst::Exact(d) => {
                        if !keys::put_part(&mut k, &d, kc) {
                            return Err(part_failed());
                        }
                    }
                    KeyConst::NoRow => return Ok((Found::Nothing, None)),
                    KeyConst::Scan => {
                        k.truncate(prefix);
                        return Ok((whole(k), plan.unkeyed.as_ref()));
                    }
                }
            }
            (Found::Point(k), plan.filter.as_ref())
        }
        Access::PkPrefix(vals) => {
            let mut k = p.clone();
            for ((e, &ci), kc) in vals.iter().zip(&def.pk).zip(def.pk_key(sp.first_format)) {
                let v = expr::eval(e, &[], a)?;
                match key_const(&v, &def.columns[ci], &a.env) {
                    KeyConst::Exact(d) => {
                        if !keys::put_part(&mut k, &d, kc) {
                            return Err(part_failed());
                        }
                    }
                    KeyConst::NoRow => return Ok((Found::Nothing, None)),
                    KeyConst::Scan => return Ok((whole(p), plan.filter.as_ref())),
                }
            }
            let hi = keys::prefix_end(&k);
            (
                Found::Rows {
                    lo: k,
                    hi,
                    all: false,
                },
                plan.filter.as_ref(),
            )
        }
        Access::PkIn(list) => {
            let c = &def.columns[def.pk[0]];
            let kc = &def.pk_key(sp.first_format)[0];
            let mut ks = Vec::with_capacity(list.len());
            for e in list {
                let v = expr::eval(e, &[], a)?;
                match key_const(&v, c, &a.env) {
                    KeyConst::Exact(d) => {
                        let mut k = p.clone();
                        if !keys::put_part(&mut k, &d, kc) {
                            return Err(part_failed());
                        }
                        ks.push(k);
                    }
                    // No row has it (NULL, a fraction for an integer).
                    KeyConst::NoRow => {}
                    KeyConst::Scan => return Ok((whole(p), plan.filter.as_ref())),
                }
            }
            ks.sort_unstable();
            ks.dedup();
            if plan.reverse {
                ks.reverse();
            }
            (Found::Points(ks), plan.filter.as_ref())
        }
        Access::IndexEq { ix, val } => {
            let x = &def.indexes[*ix];
            crate::autoindex::usage::note_read(sp.tenant, def.object.0, &x.name);
            let v = expr::eval(val, &[], a)?;
            match key_const(&v, &def.columns[x.cols[0]], &a.env) {
                KeyConst::Exact(d) => {
                    let mut lo = sp.index(def, *ix);
                    if !keys::put_index_parts(&mut lo, sp, def, *ix, &[&d]) {
                        return Err(part_failed());
                    }
                    let hi = keys::prefix_end(&lo);
                    (Found::Index { ix: *ix, lo, hi }, plan.filter.as_ref())
                }
                KeyConst::NoRow => (Found::Nothing, None),
                KeyConst::Scan => (whole(p), plan.filter.as_ref()),
            }
        }
        Access::PkRange { lo: l, hi: h } => {
            let c = &def.columns[def.pk[0]];
            let kc = &def.pk_key(sp.first_format)[0];
            // The key of a bound; `None` when the bound can't be a key
            // (the filter alone decides then). A NULL bound matches no row.
            let bound = |e: &CExpr| -> Result<Option<Option<Vec<u8>>>> {
                let v = expr::eval(e, &[], a)?;
                if v.is_null() {
                    return Ok(None);
                }
                Ok(Some(match key_const(&v, c, &a.env) {
                    KeyConst::Exact(d) => {
                        let mut k = p.clone();
                        if !keys::put_part(&mut k, &d, kc) {
                            return Err(part_failed());
                        }
                        Some(k)
                    }
                    KeyConst::NoRow | KeyConst::Scan => None,
                }))
            };
            let lower = match l {
                Some((e, incl)) => match bound(e)? {
                    Some(k) => k.map(|k| (k, *incl)),
                    None => return Ok((Found::Nothing, None)),
                },
                None => None,
            };
            let upper = match h {
                Some((e, incl)) => match bound(e)? {
                    Some(k) => k.map(|k| (k, *incl)),
                    None => return Ok((Found::Nothing, None)),
                },
                None => None,
            };
            // A descending key holds larger values first; a prefix key
            // holds every value that shares the prefix, so its bounds
            // stay inclusive (the filter is exact).
            let (lower, upper) = if kc.desc {
                (upper, lower)
            } else {
                (lower, upper)
            };
            let exact = kc.prefix.is_none();
            let lo = match lower {
                Some((k, incl)) if incl || !exact => k,
                Some((k, _)) => keys::prefix_end(&k),
                None => p.clone(),
            };
            let hi = match upper {
                Some((k, incl)) if incl || !exact => keys::prefix_end(&k),
                Some((k, _)) => k,
                None => keys::prefix_end(&p),
            };
            if lo >= hi {
                (Found::Nothing, None)
            } else {
                (Found::Rows { lo, hi, all: false }, plan.filter.as_ref())
            }
        }
        Access::Full => {
            // MySQL sees that no row can be equal to a constant outside
            // an integer or DECIMAL column's values, and reads nothing
            // (MariaDB scans; the answer is the same, the status isn't).
            if a.env.mysql && impossible(def, plan.filter.as_ref(), a)? {
                (Found::Nothing, None)
            } else {
                (whole(p), plan.filter.as_ref())
            }
        }
    })
}

/// One candidate row of a current read: locked, read again, and its
/// `WHERE` (and, for a row found through an index, the index condition)
/// checked on the locked version. `true`: `row` holds a row to use.
/// A row another transaction holds is waited for, skipped (`SKIP
/// LOCKED`) or an error (`NOWAIT`, `WAIT n`), as `lk` says.
#[allow(clippy::too_many_arguments)]
async fn lock_candidate(
    sp: &Space,
    tx: &mut Tx,
    plan: &Plan,
    filter: Option<&CExpr>,
    a: &Args<'_>,
    lk: Locking,
    key: &Bytes,
    entry: Option<(usize, &[u8])>,
    row: &mut Vec<Datum>,
) -> Result<bool> {
    let def = &*plan.table;
    let Some(now) = tx.lock_read(key, lk.mode, lk.wait).await? else {
        return Ok(false);
    };
    let Some(raw) = now else {
        // Gone since it was found.
        tx.unlock_unmatched(key);
        return Ok(false);
    };
    row::decode_into(def, sp, key, &raw, row)?;
    let mut keep = entry
        .is_none_or(|(ix, ik)| keys::index_entry(sp, def, ix, row).is_some_and(|(k, _)| k == ik));
    if keep && let Some(fl) = filter {
        keep = row::truth(&expr::eval(fl, row, a)?) == Some(true);
    }
    if !keep {
        // READ COMMITTED keeps no lock on a row its `WHERE` refused, except
        // where the servers keep it (both were probed):
        // - a locking `SELECT` by the whole primary key, on both;
        // - a row reached through a secondary index: on MariaDB always, on
        //   MySQL for a locking `SELECT` by a whole unique key.
        let select = matches!(plan.op, Op::Select { .. });
        let kept = match entry {
            None => select && matches!(plan.access, Access::Point(_)),
            Some((ix, _)) => {
                let x = &def.indexes[ix];
                !tx.mysql() || (select && x.unique && x.cols.len() == 1)
            }
        };
        if !kept {
            tx.unlock_unmatched(key);
        }
        return Ok(false);
    }
    Ok(true)
}

/// The rows of a current read (an `UPDATE`'s or `DELETE`'s, a locking
/// `SELECT`'s), in key order, up to `max`: every row the access path
/// reaches is locked and read as it is now, and those whose `WHERE` holds
/// on the locked version are returned with their keys. `dml`: an
/// `UPDATE` (its primary-key scan may skip a locked row that doesn't
/// match, at READ COMMITTED where the servers do; a `DELETE` and a
/// locking `SELECT` wait for the row on both servers).
///
/// - A primary key: one locking read (a missing row's place is locked
///   against inserts at REPEATABLE READ).
/// - A primary-key range or the whole table: a locking scan, which at
///   REPEATABLE READ and SERIALIZABLE also keeps other transactions from
///   inserting into the stretch it read.
/// - A secondary index: its entries by a locking scan, then each row.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn locked_rows(
    sp: &Space,
    tx: &mut Tx,
    plan: &Plan,
    a: &Args<'_>,
    o: &mut Outcome,
    lk: Locking,
    max: u64,
    dml: bool,
) -> Result<Vec<(Bytes, Vec<Datum>)>> {
    let def = &*plan.table;
    let mut out = Vec::new();
    if max == 0 {
        return Ok(out);
    }
    let mut row = Vec::with_capacity(def.columns.len());
    let mut pace = Pace::default();
    let (found, filter) = locate(sp, plan, a)?;
    match found {
        Found::Nothing => {}
        Found::Point(k) => {
            o.examined += 1;
            let k = Bytes::from(k);
            if lock_candidate(sp, tx, plan, filter, a, lk, &k, None, &mut row).await? {
                out.push((k, row));
            }
        }
        Found::Points(ks) => {
            for k in ks {
                o.examined += 1;
                let k = Bytes::from(k);
                if lock_candidate(sp, tx, plan, filter, a, lk, &k, None, &mut row).await? {
                    out.push((k, row.clone()));
                    if out.len() as u64 >= max {
                        return Ok(out);
                    }
                }
                pace.add(1).await;
            }
        }
        Found::Index { ix, lo, hi } => {
            // The index's entries are locked in ascending order only: a
            // descending answer would come out in the wrong order.
            if plan.reverse {
                return Err(super::not_on_engine(
                    "a locking read in descending key order through a secondary index",
                ));
            }
            let mut from = lo;
            let rowp = sp.rows();
            loop {
                let batch = tx
                    .scan_lock_entries(&from, &hi, SCAN_BATCH, lk.mode, lk.wait)
                    .await?;
                let more = batch.len() == SCAN_BATCH;
                for (ik, iv) in &batch {
                    let Some(tail) = keys::entry_target(sp, def, ik, iv) else {
                        return Err(SprigError::internal(
                            "engine: an index entry doesn't decode",
                        ));
                    };
                    let mut rk = rowp.clone();
                    rk.extend_from_slice(tail);
                    let rk = Bytes::from(rk);
                    o.examined += 1;
                    let entry = Some((ix, &ik[..]));
                    if lock_candidate(sp, tx, plan, filter, a, lk, &rk, entry, &mut row).await? {
                        out.push((rk, row.clone()));
                        if out.len() as u64 >= max {
                            return Ok(out);
                        }
                    }
                }
                pace.add(batch.len()).await;
                match batch.last() {
                    Some((k, _)) if more => from = keys::prefix_end(k),
                    _ => break,
                }
            }
        }
        Found::Rows {
            mut lo,
            mut hi,
            all,
        } => {
            // MySQL doesn't report a scan in primary-key order for an
            // `ORDER BY` as one that used no index.
            if all && !(plan.ordered && a.env.mysql) {
                o.no_index = true;
            }
            let rev = plan.reverse;
            loop {
                let want = usize::try_from(max - out.len() as u64)
                    .unwrap_or(usize::MAX)
                    .min(SCAN_BATCH);
                // The `WHERE`, on each row as the scan holds it; its first
                // error ends the statement.
                let mut failed: Option<SprigError> = None;
                let mut seen = 0u64;
                let batch = {
                    let keep = |key: &[u8], raw: &[u8]| -> bool {
                        seen += 1;
                        if failed.is_some() {
                            return false;
                        }
                        let raw = Bytes::copy_from_slice(raw);
                        let kept = row::decode_into(def, sp, key, &raw, &mut row).and_then(|()| {
                            Ok(match filter {
                                Some(fl) => row::truth(&expr::eval(fl, &row, a)?) == Some(true),
                                None => true,
                            })
                        });
                        match kept {
                            Ok(k) => k,
                            Err(e) => {
                                failed = Some(e);
                                false
                            }
                        }
                    };
                    tx.scan_lock_where(&lo, &hi, rev, want, lk.mode, lk.wait, dml, keep)
                        .await?
                };
                o.examined += seen;
                if let Some(e) = failed {
                    return Err(e);
                }
                let more = batch.len() == want;
                for (k, raw) in &batch {
                    out.push((k.clone(), row::decode(def, sp, k, raw)?));
                }
                if out.len() as u64 >= max {
                    return Ok(out);
                }
                pace.add(usize::try_from(seen).unwrap_or(usize::MAX)).await;
                match batch.last() {
                    Some((k, _)) if more => {
                        if rev {
                            hi = k.to_vec();
                        } else {
                            lo = keys::prefix_end(k);
                        }
                    }
                    _ => break,
                }
            }
        }
    }
    Ok(out)
}

/// Calls `f(key, row)` for each row the plan's access path and filter
/// select (plain reads at the read view), in key order, until `f`
/// answers `false`.
#[allow(clippy::too_many_lines)] // one arm per access path
async fn visit(
    sp: &Space,
    tx: &mut Tx,
    plan: &Plan,
    a: &Args<'_>,
    o: &mut Outcome,
    f: &mut (dyn FnMut(Bytes, &[Datum]) -> Result<bool> + Send),
) -> Result<()> {
    let mut pace = Pace::default();
    let def = &*plan.table;
    let (found, filter) = locate(sp, plan, a)?;
    let mut pass = |key: Bytes, raw: &Bytes| -> Result<bool> {
        let row = row::decode(def, sp, &key, raw)?;
        if let Some(fl) = filter
            && row::truth(&expr::eval(fl, &row, a)?) != Some(true)
        {
            return Ok(true);
        }
        f(key, &row)
    };
    match found {
        Found::Nothing => {}
        Found::Point(key) => {
            o.examined += 1;
            if let Some(raw) = tx.get(&key)? {
                pass(Bytes::from(key), &raw)?;
            }
        }
        Found::Points(ks) => {
            for key in ks {
                o.examined += 1;
                if let Some(raw) = tx.get(&key)?
                    && !pass(Bytes::from(key), &raw)?
                {
                    return Ok(());
                }
                pace.add(1).await;
            }
        }
        Found::Index { ix: _, lo, hi } => {
            let rowp = sp.rows();
            let mut from = lo;
            let mut hi = hi;
            // Entries of one value are in primary-key order: `ORDER BY pk
            // DESC` reads them backwards.
            let rev = plan.reverse;
            loop {
                let batch = tx.scan(&from, &hi, rev, SCAN_BATCH)?;
                let full = batch.len() == SCAN_BATCH;
                for (ik, iv) in &batch {
                    let Some(tail) = keys::entry_target(sp, def, ik, iv) else {
                        return Err(SprigError::internal(
                            "engine: an index entry doesn't decode",
                        ));
                    };
                    let mut rk = rowp.clone();
                    rk.extend_from_slice(tail);
                    o.examined += 1;
                    if let Some(raw) = tx.get(&rk)?
                        && !pass(Bytes::from(rk), &raw)?
                    {
                        return Ok(());
                    }
                }
                pace.add(batch.len()).await;
                match batch.last() {
                    Some((k, _)) if full && rev => hi = k.to_vec(),
                    Some((k, _)) if full => from = keys::prefix_end(k),
                    _ => break,
                }
            }
        }
        Found::Rows {
            mut lo,
            mut hi,
            all,
        } => {
            // MySQL doesn't report a scan in primary-key order for an
            // `ORDER BY` as one that used no index.
            if all && !(plan.ordered && a.env.mysql) {
                o.no_index = true;
            }
            let rev = plan.reverse;
            loop {
                let batch = tx.scan(&lo, &hi, rev, SCAN_BATCH)?;
                let full = batch.len() == SCAN_BATCH;
                for (k, raw) in &batch {
                    o.examined += 1;
                    if !pass(k.clone(), raw)? {
                        return Ok(());
                    }
                }
                pace.add(batch.len()).await;
                match batch.last() {
                    Some((k, _)) if full => {
                        if rev {
                            hi = k.to_vec();
                        } else {
                            lo = keys::prefix_end(k);
                        }
                    }
                    _ => break,
                }
            }
        }
    }
    Ok(())
}
