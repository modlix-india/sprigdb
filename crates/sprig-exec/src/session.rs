//! What a statement's evaluation depends on besides its rows: which server
//! the client believes it talks to, the session's `sql_mode` and variables,
//! the statement's clock, and the list of warnings it collects.

use sprig_engine::keys::collation::{Collation, Flavor};
use sprig_engine::row::Value;

use crate::error::{Level, Warning};

/// Which server's behaviour an expression follows where the two differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Persona {
    /// MariaDB 11.8.
    MariaDb,
    /// MySQL 8.4.
    MySql,
}

impl Persona {
    /// The collation flavor of this persona.
    #[must_use]
    pub fn flavor(self) -> Flavor {
        match self {
            Persona::MariaDb => Flavor::MariaDb,
            Persona::MySql => Flavor::MySql,
        }
    }

    /// Whether this is MariaDB.
    #[must_use]
    pub fn is_maria(self) -> bool {
        self == Persona::MariaDb
    }

    /// The "decimals not fixed" marker in result metadata of non-numeric
    /// values: 39 on MariaDB, 31 on MySQL.
    #[must_use]
    pub fn not_fixed_dec(self) -> u8 {
        match self {
            Persona::MariaDb => 39,
            Persona::MySql => 31,
        }
    }
}

/// The `sql_mode` flags the executor reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlMode(pub u32);

impl SqlMode {
    /// Division by zero raises warning 1365.
    pub const ERROR_FOR_DIVISION_BY_ZERO: u32 = 1;
    /// Unsigned minus unsigned is signed.
    pub const NO_UNSIGNED_SUBTRACTION: u32 = 1 << 1;
    /// `0000-00-00` is not a valid date.
    pub const NO_ZERO_DATE: u32 = 1 << 2;
    /// A date with a zero month or day is not valid.
    pub const NO_ZERO_IN_DATE: u32 = 1 << 3;
    /// Only the month (1 to 12) and day (1 to 31) ranges are checked.
    pub const ALLOW_INVALID_DATES: u32 = 1 << 4;
    /// Fractional seconds beyond the wanted precision round instead of
    /// being cut (MySQL's rule; MariaDB's with `TIME_ROUND_FRACTIONAL`).
    pub const TIME_ROUND_FRACTIONAL: u32 = 1 << 5;
    /// The backslash is an ordinary character (so `LIKE` has no default
    /// escape character).
    pub const NO_BACKSLASH_ESCAPES: u32 = 1 << 6;
    /// A strict mode is on (the caller escalates warnings in DML).
    pub const STRICT: u32 = 1 << 7;

    /// Whether every flag of `flags` is set.
    #[must_use]
    pub fn has(self, flags: u32) -> bool {
        self.0 & flags == flags
    }
}

/// The session state an evaluation reads. The caller fills it once per
/// statement.
#[derive(Debug, Clone)]
pub struct Session {
    /// Which server's rules apply.
    pub persona: Persona,
    /// The session's `sql_mode`.
    pub sql_mode: SqlMode,
    /// `collation_connection`: the collation of string literals and of
    /// numbers turned into strings.
    pub collation_connection: Collation,
    /// `div_precision_increment` (4 by default).
    pub div_precision_increment: u8,
    /// The session time zone as seconds east of UTC.
    pub time_zone_offset: i32,
    /// The session time zone by name (`Europe/Berlin`): when set, and
    /// known to the bundled time zone database, it gives the offset at
    /// each instant instead of `time_zone_offset`.
    pub time_zone_name: Option<String>,
    /// The statement's start time: seconds since the Unix epoch, UTC.
    pub now_seconds: i64,
    /// The microseconds of the statement's start time.
    pub now_micros: u32,
    /// `max_allowed_packet`: string functions answer `NULL` beyond it.
    pub max_allowed_packet: u64,
    /// `group_concat_max_len`.
    pub group_concat_max_len: u64,
    /// `default_week_format`.
    pub default_week_format: u8,
    /// Whether the statement reads tables. MySQL then computes a constant
    /// sub-expression once and reuses it for every row; without tables,
    /// and on MariaDB, it is computed in place each time.
    pub fold_constants: bool,
    /// The session's own values (`DATABASE()`, `LAST_INSERT_ID()`, ...).
    pub info: crate::func::misc::SessionInfo,
}

impl Session {
    /// A session with the persona's server defaults, at UTC, with the
    /// statement clock at the epoch.
    #[must_use]
    pub fn new(persona: Persona) -> Self {
        let maria = persona.is_maria();
        Self {
            persona,
            sql_mode: SqlMode(if maria {
                SqlMode::STRICT | SqlMode::ERROR_FOR_DIVISION_BY_ZERO
            } else {
                SqlMode::STRICT
                    | SqlMode::ERROR_FOR_DIVISION_BY_ZERO
                    | SqlMode::NO_ZERO_DATE
                    | SqlMode::NO_ZERO_IN_DATE
                    | SqlMode::TIME_ROUND_FRACTIONAL
            }),
            collation_connection: if maria {
                Collation::Utf8mb4Uca1400AiCi
            } else {
                Collation::Utf8mb4Mysql0900AiCi
            },
            div_precision_increment: 4,
            time_zone_offset: 0,
            time_zone_name: None,
            now_seconds: 0,
            now_micros: 0,
            max_allowed_packet: if maria { 16 << 20 } else { 64 << 20 },
            group_concat_max_len: if maria { 1 << 20 } else { 1024 },
            default_week_format: 0,
            fold_constants: false,
            info: crate::func::misc::SessionInfo::default(),
        }
    }
}

impl Session {
    /// The session time zone's offset from UTC, in seconds, at an instant
    /// (seconds since the epoch).
    #[must_use]
    pub fn zone_offset_at(&self, unix: i64) -> i64 {
        match self.time_zone_name.as_deref().and_then(crate::tz::lookup) {
            Some(tz) => crate::tz::offset_at(&tz, unix),
            None => i64::from(self.time_zone_offset),
        }
    }

    /// Whether the session's zone is a named one the time zone database
    /// knows.
    #[must_use]
    pub fn zone_is_named(&self) -> bool {
        self.time_zone_name
            .as_deref()
            .and_then(crate::tz::lookup)
            .is_some()
    }

    /// The instant (seconds since the epoch) of a wall-clock time of the
    /// session's zone, given as seconds since 1970-01-01 00:00 local.
    #[must_use]
    pub fn zone_to_unix(&self, local: i64) -> i64 {
        match self.time_zone_name.as_deref().and_then(crate::tz::lookup) {
            Some(tz) => crate::tz::local_to_unix(&tz, local),
            None => local - i64::from(self.time_zone_offset),
        }
    }
}

/// The most conditions one statement keeps (`max_error_count`).
pub const MAX_WARNINGS: usize = 64;

/// The state of one statement's evaluation: the session, the parameter
/// values, and the warnings raised so far.
#[derive(Debug)]
pub struct Ctx<'a> {
    /// The session.
    pub session: &'a Session,
    /// The values of the statement's `?` parameters.
    pub params: &'a [Value],
    pub(crate) warnings: Vec<Warning>,
    pub(crate) warning_count: usize,
    /// While above zero, conditions are dropped: some conversions are
    /// silent on the servers.
    pub(crate) mute: u32,
    /// Whether the string being converted is a binary string (its bytes
    /// print as `\xNN` in messages).
    pub(crate) err_bin: bool,
    /// Values of constant sub-expressions already computed for this
    /// statement, by the sub-expression's number.
    pub(crate) consts: std::collections::HashMap<usize, Value>,
    /// How many decimal results needed more than the 65 digits a value
    /// holds (see [`Ctx::wide_decimals`]).
    pub(crate) wide_decimals: usize,
    /// How many decimal results were a zero that MariaDB keeps negative
    /// (see [`Ctx::negative_zeros`]).
    pub(crate) negative_zeros: usize,
}

impl<'a> Ctx<'a> {
    /// A fresh evaluation state without parameters.
    #[must_use]
    pub fn new(session: &'a Session) -> Self {
        Self::with_params(session, &[])
    }

    /// A fresh evaluation state with parameter values.
    #[must_use]
    pub fn with_params(session: &'a Session, params: &'a [Value]) -> Self {
        Self {
            session,
            params,
            warnings: Vec::new(),
            warning_count: 0,
            mute: 0,
            err_bin: false,
            consts: std::collections::HashMap::new(),
            wide_decimals: 0,
            negative_zeros: 0,
        }
    }

    /// How many decimal results of this statement needed more than 65
    /// digits. The servers compute with up to 81; here such a result
    /// loses digits after the point, or, with more than 65 before it,
    /// fails with error 1690. A statement that counts any may differ from
    /// the servers in those digits.
    #[must_use]
    pub fn wide_decimals(&self) -> usize {
        self.wide_decimals
    }

    /// How many decimal results of this statement were a zero that
    /// MariaDB would keep negative: the remainder of a negative number
    /// that divides evenly, a negative quotient too small for its digits.
    /// MariaDB shows such a zero as `-0.00` and compares it as less than
    /// zero; here it is plain zero. A MariaDB-persona statement that
    /// counts any may differ from MariaDB because of it.
    #[must_use]
    pub fn negative_zeros(&self) -> usize {
        self.negative_zeros
    }

    /// The persona.
    #[must_use]
    pub fn persona(&self) -> Persona {
        self.session.persona
    }

    /// Whether the persona is MariaDB.
    #[must_use]
    pub fn maria(&self) -> bool {
        self.session.persona.is_maria()
    }

    /// Records a condition. Only the first [`MAX_WARNINGS`] are kept; all
    /// are counted.
    pub fn push(&mut self, level: Level, code: u16, message: impl Into<String>) {
        if self.mute > 0 {
            return;
        }
        self.warning_count += 1;
        if self.warnings.len() < MAX_WARNINGS {
            self.warnings.push(Warning {
                level,
                code,
                message: message.into(),
            });
        }
    }

    /// Records a warning.
    pub fn warn(&mut self, code: u16, message: impl Into<String>) {
        self.push(Level::Warning, code, message);
    }

    /// Records a note.
    pub fn note(&mut self, code: u16, message: impl Into<String>) {
        self.push(Level::Note, code, message);
    }

    /// The conditions kept so far, oldest first.
    #[must_use]
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    /// How many conditions were raised, including those beyond the kept
    /// list.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.warning_count
    }

    /// Takes the kept conditions, leaving the list empty.
    pub fn take_warnings(&mut self) -> Vec<Warning> {
        self.warning_count = 0;
        std::mem::take(&mut self.warnings)
    }
}
