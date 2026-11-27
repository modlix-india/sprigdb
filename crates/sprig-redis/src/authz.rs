//! What a user needs to run a command on a database stored on the engine.
//!
//! On the key-value tables every command was SQL on the user's own
//! session, so the tables' grants decided. On the engine nothing checks a
//! command unless the session does, so it asks the login's SQL session
//! ([`sprig_kv::SqlRunner::holds_on_database`]) for rights on the tenant's
//! logical database that holds its Redis databases
//! (`redis.logical_database`):
//!
//! ```text
//! command                                   needs, on that logical database
//! reads (GET, SCAN, XREAD, WATCH, ...)      SELECT
//! writes (SET, DEL, LPOP, EXPIRE, ...)      INSERT, UPDATE and DELETE
//! FLUSHALL, SWAPDB                          those and DROP
//! EVAL, EVALSHA, FCALL, SCRIPT, FUNCTION    EXECUTE; every command the script
//!   (LIST, STATS, DUMP)                       calls is checked as a client's
//! FUNCTION LOAD, DELETE, FLUSH, RESTORE     CREATE ROUTINE and ALTER ROUTINE
//! connection and server commands            nothing here
//! ```
//!
//! A write needs all three write privileges because one Redis command
//! creates, changes and removes keys (`SET` replaces, `LPOP` removes the
//! key it empties). A grant on the whole tenant counts. The check is made
//! where a command enters: one command, a pipeline's batch, a command
//! queued in `MULTI`, and each call a script makes.
//!
//! Not decided here: rights per numbered Redis database, per key pattern,
//! per channel or per command category (Redis's `ACL SETUSER` rules).

use bytes::Bytes;
use sprig_kv::sql::DbRight;
use sprig_proto_redis::Frame;

use crate::spec::Spec;

/// What a command needs (module notes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// Nothing on the logical database.
    Nothing,
    /// Reading keys.
    Read,
    /// Writing keys.
    Write,
    /// Emptying or swapping whole databases.
    WriteAll,
    /// Running a script or a function.
    Script,
    /// Changing the function libraries.
    Library,
}

impl Need {
    /// The rights on the logical database.
    #[must_use]
    pub fn rights(self) -> &'static [DbRight] {
        match self {
            Need::Nothing => &[],
            Need::Read => &[DbRight::Select],
            Need::Write => &[DbRight::Insert, DbRight::Update, DbRight::Delete],
            Need::WriteAll => &[
                DbRight::Insert,
                DbRight::Update,
                DbRight::Delete,
                DbRight::Drop,
            ],
            Need::Script => &[DbRight::Execute],
            Need::Library => &[DbRight::CreateRoutine, DbRight::AlterRoutine],
        }
    }
}

/// What command `name` (lower case) with `args` (the name first) needs;
/// `data`: the engine runs it (it reads or writes keys).
#[must_use]
pub fn need(name: &str, args: &[Bytes], spec: &Spec, data: bool) -> Need {
    let sub = |s: &[u8]| args.get(1).is_some_and(|x| x.eq_ignore_ascii_case(s));
    match name {
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro" | "script" => {
            Need::Script
        }
        "function" => {
            if sub(b"load") || sub(b"delete") || sub(b"flush") || sub(b"restore") {
                Need::Library
            } else {
                Need::Script
            }
        }
        "flushall" | "swapdb" => Need::WriteAll,
        // `WATCH` reads its keys though the session answers it.
        _ if !data && name != "watch" => Need::Nothing,
        _ if spec.has('w') => Need::Write,
        _ => Need::Read,
    }
}

/// Redis's reply to a command the user may not run.
#[must_use]
pub fn noperm(user: &str, name: &str) -> Frame {
    Frame::error(format!(
        "NOPERM User {user} has no permissions to run the '{name}' command"
    ))
}

/// Redis's reply to a script's call the user may not make.
#[must_use]
pub fn noperm_in_script() -> Frame {
    Frame::err(
        "ACL failure in script: The user executing the script can't run this command or subcommand",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(cmd: &[&str], data: bool) -> Need {
        let args: Vec<Bytes> = cmd.iter().map(|a| Bytes::from((*a).to_owned())).collect();
        let name = cmd[0].to_ascii_lowercase();
        let spec = crate::engine::lookup(&name).unwrap();
        need(&name, &args, spec, data)
    }

    #[test]
    fn commands_need_what_they_do() {
        assert_eq!(n(&["GET", "k"], true), Need::Read);
        assert_eq!(n(&["SCAN", "0"], true), Need::Read);
        assert_eq!(n(&["HTTL", "k", "FIELDS", "1", "f"], true), Need::Read);
        assert_eq!(n(&["SET", "k", "v"], true), Need::Write);
        assert_eq!(n(&["DEL", "k"], true), Need::Write);
        assert_eq!(n(&["LPOP", "k"], true), Need::Write);
        assert_eq!(n(&["EXPIRE", "k", "1"], true), Need::Write);
        assert_eq!(n(&["FLUSHDB"], true), Need::Write);
        assert_eq!(n(&["FLUSHALL"], true), Need::WriteAll);
        assert_eq!(n(&["SWAPDB", "0", "1"], true), Need::WriteAll);
        assert_eq!(n(&["WATCH", "k"], false), Need::Read);
        assert_eq!(n(&["EVAL", "return 1", "0"], false), Need::Script);
        assert_eq!(n(&["FCALL", "f", "0"], false), Need::Script);
        assert_eq!(n(&["SCRIPT", "LOAD", "return 1"], false), Need::Script);
        assert_eq!(n(&["FUNCTION", "LIST"], false), Need::Script);
        assert_eq!(n(&["FUNCTION", "load", "x"], false), Need::Library);
        assert_eq!(n(&["FUNCTION", "FLUSH"], false), Need::Library);
        // Connection and server commands the session answers itself.
        for c in [
            &["PING"][..],
            &["SELECT", "3"],
            &["CLIENT", "ID"],
            &["INFO"],
        ] {
            assert_eq!(n(c, false), Need::Nothing, "{c:?}");
        }
        assert!(Need::Nothing.rights().is_empty());
        assert_eq!(Need::Read.rights(), [DbRight::Select]);
        assert!(Need::Write.rights().contains(&DbRight::Delete));
        assert!(Need::WriteAll.rights().contains(&DbRight::Drop));
        assert_eq!(Need::Script.rights(), [DbRight::Execute]);
    }

    #[test]
    fn replies_are_redis_s() {
        assert!(
            matches!(noperm("alice", "set"), Frame::Error(e) if &e[..] == b"NOPERM User alice has no permissions to run the 'set' command")
        );
        assert!(
            matches!(noperm_in_script(), Frame::Error(e) if e.starts_with(b"ERR ACL failure in script"))
        );
    }
}
