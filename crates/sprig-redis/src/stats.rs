//! Commands answered, by command and result: the counts behind
//! `sprig_redis_commands_total`, which the gateway publishes.
//!
//! The connection loop ([`crate::conn`]) counts each command once its reply
//! is ready. The counts are relaxed atomics in a table with one row per
//! command of [`crate::spec::ALL`] and one for names it doesn't list
//! (`unknown`, so a client can't grow the label set), so counting costs no
//! lock and no allocation on the command path.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_proto_redis::{Command, Frame};

/// The `result` label's values, in the table's column order: the reply
/// was not an error, an error, a `WRONGTYPE` error, a `NOPERM` error.
pub const RESULTS: [&str; 4] = ["ok", "error", "wrongtype", "noperm"];

/// The `command` label of a name the command table doesn't list.
pub const UNKNOWN: &str = "unknown";

/// Longest command name looked up; longer ones count as [`UNKNOWN`].
const NAME_MAX: usize = 32;

struct Table {
    index: HashMap<&'static str, usize>,
    counts: Box<[[AtomicU64; 4]]>,
}

fn table() -> &'static Table {
    static T: OnceLock<Table> = OnceLock::new();
    T.get_or_init(|| {
        let all = crate::spec::ALL;
        Table {
            index: all.iter().enumerate().map(|(i, s)| (s.name, i)).collect(),
            counts: (0..=all.len()).map(|_| Default::default()).collect(),
        }
    })
}

/// The table row of a command name: its place in [`crate::spec::ALL`], or
/// the last row for a name it doesn't list.
fn row(t: &Table, name: &[u8]) -> usize {
    let unknown = t.counts.len() - 1;
    if name.len() > NAME_MAX {
        return unknown;
    }
    let mut buf = [0u8; NAME_MAX];
    let lower = &mut buf[..name.len()];
    lower.copy_from_slice(name);
    lower.make_ascii_lowercase();
    std::str::from_utf8(lower)
        .ok()
        .and_then(|n| t.index.get(n).copied())
        .unwrap_or(unknown)
}

/// The result column of a command's reply frames: the first error's kind,
/// or `ok` when no frame is an error.
fn column(reply: &[Frame]) -> usize {
    let Some(e) = reply.iter().find_map(|f| match f {
        Frame::Error(e) => Some(e),
        _ => None,
    }) else {
        return 0;
    };
    if e.starts_with(b"WRONGTYPE") {
        2
    } else if e.starts_with(b"NOPERM") {
        3
    } else {
        1
    }
}

/// Counts each command of `cmds` with its reply in `replies` (the same
/// order; commands past the replies aren't counted).
pub fn count(cmds: &[Command], replies: &[Vec<Frame>]) {
    let t = table();
    for (c, r) in cmds.iter().zip(replies) {
        let name = c.args.first().map_or(&[][..], |a| &a[..]);
        t.counts[row(t, name)][column(r)].fetch_add(1, Ordering::Relaxed);
    }
}

/// Every non-zero count so far, as (command, result, count).
#[must_use]
pub fn snapshot() -> Vec<(&'static str, &'static str, u64)> {
    let t = table();
    let all = crate::spec::ALL;
    let mut out = Vec::new();
    for (i, row) in t.counts.iter().enumerate() {
        let name = all.get(i).map_or(UNKNOWN, |s| s.name);
        for (j, n) in row.iter().enumerate() {
            let n = n.load(Ordering::Relaxed);
            if n > 0 {
                out.push((name, RESULTS[j], n));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn of(name: &str, result: &str) -> u64 {
        snapshot()
            .into_iter()
            .find(|(c, r, _)| *c == name && *r == result)
            .map_or(0, |(_, _, n)| n)
    }

    #[test]
    fn counts_by_command_and_result() {
        let get0 = of("get", "ok");
        let set_wt0 = of("set", "wrongtype");
        let hget_np0 = of("hget", "noperm");
        let unknown0 = of(UNKNOWN, "error");
        let cmds = [
            Command::from_parts(["GET", "k"]),
            Command::from_parts(["set", "k", "v"]),
            Command::from_parts(["HgEt", "h", "f"]),
            Command::from_parts(["NOSUCHCOMMAND"]),
            Command::from_parts(["get", "k"]),
        ];
        let replies = vec![
            vec![Frame::Bulk(Bytes::from_static(b"v"))],
            vec![crate::reply::wrongtype()],
            vec![Frame::error("NOPERM this user has no permissions")],
            vec![Frame::err("unknown command 'NOSUCHCOMMAND'")],
            Vec::new(),
        ];
        count(&cmds, &replies);
        assert_eq!(of("get", "ok") - get0, 2);
        assert_eq!(of("set", "wrongtype") - set_wt0, 1);
        assert_eq!(of("hget", "noperm") - hget_np0, 1);
        assert_eq!(of(UNKNOWN, "error") - unknown0, 1);
    }

    #[test]
    fn a_long_or_odd_name_counts_as_unknown() {
        let t = table();
        let unknown = t.counts.len() - 1;
        assert_eq!(row(t, &[b'g'; NAME_MAX + 1]), unknown);
        assert_eq!(row(t, &[0xff, 0xfe]), unknown);
        assert_eq!(row(t, b""), unknown);
        assert_ne!(row(t, b"PING"), unknown);
    }
}
