//! Frames: everything one node sends another in one driver turn.
//!
//! ```text
//! magic 'R' | version 1 | from u64 | to u64 | epoch u64 | seq u64
//! | ack_epoch u64 | n_acks u16 | acks u64...
//! | n_items u32 | items: kind u8 | group u64 | len u32 | bytes
//! ```
//! Integers are little-endian. Kind 0 is a raft-rs message: the sender's
//! applied index (`u64`), then the message (protobuf). Kind 1 is a quiesce
//! notice (`term u64 | index u64`).
//!
//! The applied index lets a replica that was removed (and left a
//! tombstone) tell a leader that re-added it, having applied past the
//! removal, from one that hasn't applied the removal yet.
//!
//! `epoch` is the sender's incarnation (random per start) and `seq` counts
//! its frames to this receiver. `acks` are the seqs of the receiver's frames
//! (of incarnation `ack_epoch`) the sender processed since its last frame to
//! the receiver: what leader leases are built from.

use protobuf::Message as _;
use raft::prelude::Message;

use crate::{GroupId, NodeId};

const MAGIC: u8 = b'R';
const VERSION: u8 = 1;
const MAX_ACKS: usize = 64;

/// One item of a frame.
#[derive(Debug, Clone)]
pub(crate) enum Item {
    /// A raft-rs message of a group, with the sender's applied index.
    Raft(GroupId, u64, Message),
    /// The group's leader quiesced it at `term` with its log ending at
    /// `index`.
    Quiesce {
        group: GroupId,
        term: u64,
        index: u64,
    },
}

/// A decoded frame.
#[derive(Debug, Clone, Default)]
pub(crate) struct Frame {
    pub(crate) from: NodeId,
    pub(crate) to: NodeId,
    pub(crate) epoch: u64,
    pub(crate) seq: u64,
    pub(crate) ack_epoch: u64,
    pub(crate) acks: Vec<u64>,
    pub(crate) items: Vec<Item>,
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

impl Frame {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(64 + self.items.len() * 64);
        b.push(MAGIC);
        b.push(VERSION);
        for v in [self.from, self.to, self.epoch, self.seq, self.ack_epoch] {
            put_u64(&mut b, v);
        }
        let acks = &self.acks[self.acks.len().saturating_sub(MAX_ACKS)..];
        b.extend_from_slice(&u16::try_from(acks.len()).unwrap_or(0).to_le_bytes());
        for a in acks {
            put_u64(&mut b, *a);
        }
        b.extend_from_slice(&u32::try_from(self.items.len()).unwrap_or(0).to_le_bytes());
        for it in &self.items {
            match it {
                Item::Raft(g, applied, m) => {
                    b.push(0);
                    put_u64(&mut b, *g);
                    let bytes = m.write_to_bytes().unwrap_or_default();
                    b.extend_from_slice(&u32::try_from(bytes.len() + 8).unwrap_or(0).to_le_bytes());
                    put_u64(&mut b, *applied);
                    b.extend_from_slice(&bytes);
                }
                Item::Quiesce { group, term, index } => {
                    b.push(1);
                    put_u64(&mut b, *group);
                    b.extend_from_slice(&16u32.to_le_bytes());
                    put_u64(&mut b, *term);
                    put_u64(&mut b, *index);
                }
            }
        }
        b
    }

    /// `None` for anything malformed.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Frame> {
        let mut r = Reader { b: bytes, at: 0 };
        if r.u8()? != MAGIC || r.u8()? != VERSION {
            return None;
        }
        let mut f = Frame {
            from: r.u64()?,
            to: r.u64()?,
            epoch: r.u64()?,
            seq: r.u64()?,
            ack_epoch: r.u64()?,
            ..Frame::default()
        };
        let n = usize::from(r.u16()?);
        for _ in 0..n {
            f.acks.push(r.u64()?);
        }
        let n = r.u32()?;
        for _ in 0..n {
            let kind = r.u8()?;
            let group = r.u64()?;
            let len = usize::try_from(r.u32()?).ok()?;
            let body = r.take(len)?;
            match kind {
                0 => {
                    let mut br = Reader { b: body, at: 0 };
                    let applied = br.u64()?;
                    let m = Message::parse_from_bytes(&body[8..]).ok()?;
                    f.items.push(Item::Raft(group, applied, m));
                }
                1 => {
                    let mut br = Reader { b: body, at: 0 };
                    f.items.push(Item::Quiesce {
                        group,
                        term: br.u64()?,
                        index: br.u64()?,
                    });
                }
                _ => return None,
            }
        }
        (r.at == bytes.len()).then_some(f)
    }
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let s = self.b.get(self.at..end)?;
        self.at = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|s| s[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2)
            .map(|s| u16::from_le_bytes(s.try_into().unwrap_or([0; 2])))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|s| u32::from_le_bytes(s.try_into().unwrap_or([0; 4])))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .map(|s| u64::from_le_bytes(s.try_into().unwrap_or([0; 8])))
    }
}

/// The sender node of an encoded frame, without decoding the rest.
#[cfg(test)]
pub(crate) fn frame_from(b: &[u8]) -> Option<NodeId> {
    let mut r = Reader { b, at: 0 };
    if r.u8()? != MAGIC || r.u8()? != VERSION {
        return None;
    }
    r.u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::prelude::{Entry, MessageType};

    #[test]
    fn frames_round_trip() {
        let mut msg = Message::default();
        msg.set_msg_type(MessageType::MsgAppend);
        msg.from = 1;
        msg.to = 2;
        msg.term = 7;
        let entry = Entry {
            index: 9,
            data: b"hello".to_vec().into(),
            ..Entry::default()
        };
        msg.mut_entries().push(entry);
        let frame = Frame {
            from: 1,
            to: 2,
            epoch: 3,
            seq: 4,
            ack_epoch: 5,
            acks: vec![10, 11],
            items: vec![
                Item::Raft(42, 77, msg.clone()),
                Item::Quiesce {
                    group: 43,
                    term: 7,
                    index: 9,
                },
            ],
        };
        let b = frame.encode();
        assert_eq!(frame_from(&b), Some(1));
        let got = Frame::decode(&b).expect("decodes");
        assert_eq!(
            (got.from, got.to, got.epoch, got.seq, got.ack_epoch),
            (1, 2, 3, 4, 5)
        );
        assert_eq!(got.acks, vec![10, 11]);
        match &got.items[0] {
            Item::Raft(g, applied, dm) => {
                assert_eq!((*g, *applied), (42, 77));
                assert_eq!(dm, &msg);
            }
            Item::Quiesce { .. } => panic!("wrong item"),
        }
        assert!(matches!(
            got.items[1],
            Item::Quiesce {
                group: 43,
                term: 7,
                index: 9
            }
        ));
        // Truncated or padded frames are refused.
        assert!(Frame::decode(&b[..b.len() - 1]).is_none());
        let mut longer = b.clone();
        longer.push(0);
        assert!(Frame::decode(&longer).is_none());
    }
}
