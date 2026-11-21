//! Errors and warnings as the SQL client sees them: a MySQL error number, an
//! SQLSTATE and the message text.

use std::fmt;

/// A statement-ending error with the number and SQLSTATE the servers use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    /// The MySQL error number, for example 1690.
    pub code: u16,
    /// The five-character SQLSTATE.
    pub sqlstate: &'static str,
    /// The message text.
    pub message: String,
}

impl SqlError {
    /// An error from its parts.
    #[must_use]
    pub fn new(code: u16, sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            sqlstate,
            message: message.into(),
        }
    }

    /// 1690: a `BIGINT`, `BIGINT UNSIGNED`, `DECIMAL` or `DOUBLE` result
    /// left its range. `what` is the type name, `text` the expression.
    #[must_use]
    pub fn out_of_range(what: &str, text: &str) -> Self {
        Self::new(
            1690,
            "22003",
            format!("{what} value is out of range in '{text}'"),
        )
    }

    /// 1582: a built-in function was called with the wrong number of
    /// arguments.
    #[must_use]
    pub fn param_count(name: &str) -> Self {
        Self::new(
            1582,
            "42000",
            format!("Incorrect parameter count in the call to native function '{name}'"),
        )
    }

    /// 1305: no built-in function of that name.
    #[must_use]
    pub fn no_function(name: &str) -> Self {
        Self::new(1305, "42000", format!("FUNCTION {name} does not exist"))
    }

    /// 1235: a construct this executor doesn't evaluate yet.
    #[must_use]
    pub fn unsupported(what: &str) -> Self {
        Self::new(
            1235,
            "42000",
            format!("This version of SprigDB doesn't yet support '{what}'"),
        )
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ERROR {} ({}): {}",
            self.code, self.sqlstate, self.message
        )
    }
}

impl std::error::Error for SqlError {}

/// The executor's result type.
pub type Result<T, E = SqlError> = std::result::Result<T, E>;

/// How serious a recorded condition is, as `SHOW WARNINGS` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// A note: never escalated by strict mode.
    Note,
    /// A warning.
    Warning,
    /// An error kept in the list (the failed statement's own error).
    Error,
}

impl Level {
    /// The word `SHOW WARNINGS` prints.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Level::Note => "Note",
            Level::Warning => "Warning",
            Level::Error => "Error",
        }
    }
}

/// One row of `SHOW WARNINGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    /// Note, warning or error.
    pub level: Level,
    /// The MySQL error number.
    pub code: u16,
    /// The message text.
    pub message: String,
}
