//! Apply observers: consumers of committed log entries that keep their own
//! files beside the store (stream segment files).
//!
//! An observer sees every committed entry of every unit, in index order, on
//! the apply step, right after the store applied it. It writes what it
//! needs into its own files without syncing them; the log is its
//! write-ahead log. Its **log floor** is the highest index its files hold
//! durably: the engine's checkpoint never trims the log above a floor, so
//! after a crash the observer replays from its floor through
//! [`crate::Engine::observe`]. Floors are stored in the `local` column
//! family, synced, so they hold across restarts even before the observer
//! registers again.

use crate::log::{LogEntry, UnitId};

/// See the module docs.
pub trait ApplyObserver: Send + Sync {
    /// Called on the apply step with committed entries of `unit`,
    /// contiguous and in index order, after the store applied them. Every
    /// unit's entries come through here; each unit has its own index
    /// sequence. It must not wait for the log (the apply step is the log
    /// writer's thread). Failures are the observer's own to report to its
    /// callers; the engine goes on.
    fn applied(&self, unit: UnitId, entries: &[LogEntry]);
}

/// Change hints: payload notes a writer adds beside a versioned write,
/// saying what the write was in its model's terms (an insert, an update, a
/// replacement or a delete) with an optional description. The change log
/// attaches each to the change of the same logical key in the same entry.
pub mod hint {
    /// The note prefix of a hint.
    pub const MAGIC: &[u8; 4] = b"CHG1";

    /// What a write was, as the writer saw it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum HintOp {
        /// A new record.
        Insert,
        /// Some fields changed.
        Update,
        /// The whole record was replaced.
        Replace,
        /// The record went.
        Delete,
    }

    impl HintOp {
        /// Its byte.
        #[must_use]
        pub fn code(self) -> u8 {
            match self {
                Self::Insert => 1,
                Self::Update => 2,
                Self::Replace => 3,
                Self::Delete => 4,
            }
        }

        /// The operation of a byte.
        #[must_use]
        pub fn of(c: u8) -> Option<Self> {
            Some(match c {
                1 => Self::Insert,
                2 => Self::Update,
                3 => Self::Replace,
                4 => Self::Delete,
                _ => return None,
            })
        }
    }

    fn put_varint(out: &mut Vec<u8>, mut v: usize) {
        while v >= 0x80 {
            #[allow(clippy::cast_possible_truncation)]
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        #[allow(clippy::cast_possible_truncation)]
        out.push(v as u8);
    }

    fn varint(b: &[u8]) -> Option<(usize, &[u8])> {
        let mut v = 0usize;
        for (i, x) in b.iter().enumerate().take(10) {
            v |= usize::from(x & 0x7F) << (7 * i);
            if *x < 0x80 {
                return Some((v, &b[i + 1..]));
            }
        }
        None
    }

    /// A hint note for the write of `key` (a logical key, without version).
    #[must_use]
    pub fn encode(key: &[u8], op: HintOp, desc: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(key.len() + desc.len() + 12);
        out.extend_from_slice(MAGIC);
        put_varint(&mut out, key.len());
        out.extend_from_slice(key);
        out.push(op.code());
        put_varint(&mut out, desc.len());
        out.extend_from_slice(desc);
        out
    }

    /// The parts of a hint note: key, operation, description. `None` for
    /// any other note.
    #[must_use]
    pub fn decode(note: &[u8]) -> Option<(&[u8], HintOp, &[u8])> {
        let rest = note.strip_prefix(&MAGIC[..])?;
        let (n, rest) = varint(rest)?;
        let key = rest.get(..n)?;
        let rest = &rest[n..];
        let (&op, rest) = rest.split_first()?;
        let op = HintOp::of(op)?;
        let (n, rest) = varint(rest)?;
        let desc = rest.get(..n)?;
        Some((key, op, desc))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn round_trip() {
            let n = encode(&[9u8; 200], HintOp::Update, b"desc");
            assert_eq!(
                decode(&n),
                Some((&[9u8; 200][..], HintOp::Update, &b"desc"[..]))
            );
            assert_eq!(decode(b"other"), None);
            assert_eq!(decode(b"CHG1"), None);
        }
    }
}
