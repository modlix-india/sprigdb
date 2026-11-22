//! Leader leases on monotonic clocks.
//!
//! A leader records, per follower, the send time of each recent frame that
//! carried one of this group's messages. When the follower's next frame
//! acknowledges one of those frames and carries a response of this group in
//! the leader's term, the follower accepted the leader after that send, so
//! it refuses other candidates for an election timeout from a moment no
//! earlier than the send. The lease starts at the send time a quorum of
//! voters supports (the leader counts as now) and is used for the lease
//! minus the margin.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use raft::prelude::ConfState;

use crate::NodeId;

/// Sends remembered per follower.
const KEEP: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct Lease {
    term: u64,
    /// Off for the rest of the term (a leader transfer started: its target
    /// may win votes despite their leases).
    disabled: bool,
    /// Per follower: (frame seq, send time) of recent frames carrying this
    /// group's messages.
    sent: Vec<(NodeId, VecDeque<(u64, Instant)>)>,
    /// Per follower: the latest send time it accepted.
    support: Vec<(NodeId, Instant)>,
}

impl Lease {
    /// Starts over for a new term (as leader) or drops everything.
    pub(crate) fn reset(&mut self, term: u64) {
        *self = Lease {
            term,
            ..Lease::default()
        };
    }

    pub(crate) fn term(&self) -> u64 {
        self.term
    }

    pub(crate) fn disable(&mut self) {
        self.disabled = true;
    }

    /// A frame with seq `seq` carrying this group's message went to `to`.
    pub(crate) fn on_send(&mut self, to: NodeId, seq: u64, now: Instant) {
        let i = if let Some(i) = self.sent.iter().position(|(n, _)| *n == to) {
            i
        } else {
            self.sent.push((to, VecDeque::new()));
            self.sent.len() - 1
        };
        let q = &mut self.sent[i].1;
        if q.len() == KEEP {
            q.pop_front();
        }
        q.push_back((seq, now));
    }

    /// `from` acknowledged frames `acks` in a frame that also carried its
    /// response in this term.
    pub(crate) fn on_ack(&mut self, from: NodeId, acks: &[u64]) {
        let Some(i) = self.sent.iter().position(|(n, _)| *n == from) else {
            return;
        };
        let q = &mut self.sent[i].1;
        let mut best: Option<Instant> = None;
        for (seq, t) in q.iter() {
            if acks.contains(seq) && best.is_none_or(|b| *t > b) {
                best = Some(*t);
            }
        }
        let Some(t) = best else {
            return;
        };
        q.retain(|(_, st)| *st > t);
        match self.support.iter_mut().find(|(n, _)| *n == from) {
            Some((_, s)) => {
                if t > *s {
                    *s = t;
                }
            }
            None => self.support.push((from, t)),
        }
    }

    fn support_of(&self, n: NodeId) -> Option<Instant> {
        self.support.iter().find(|(m, _)| *m == n).map(|(_, t)| *t)
    }

    /// The send time a majority of `voters` supports (`me` as `now`).
    fn quorum_start(&self, voters: &[u64], me: NodeId, now: Instant) -> Option<Instant> {
        if voters.is_empty() {
            return Some(now);
        }
        let mut times: Vec<Option<Instant>> = voters
            .iter()
            .map(|v| {
                if *v == me {
                    Some(now)
                } else {
                    self.support_of(*v)
                }
            })
            .collect();
        // Newest first; `None` (no support) sorts last.
        times.sort_by(|a, b| b.cmp(a));
        let q = voters.len() / 2 + 1;
        times.get(q - 1).copied().flatten()
    }

    /// When the lease ends, if it holds now: both halves of a joint
    /// configuration must support it.
    pub(crate) fn valid_until(
        &self,
        cs: &ConfState,
        me: NodeId,
        now: Instant,
        usable: Duration,
    ) -> Option<(Instant, Instant)> {
        if self.disabled {
            return None;
        }
        let a = self.quorum_start(&cs.voters, me, now)?;
        let b = self.quorum_start(&cs.voters_outgoing, me, now)?;
        let start = a.min(b);
        let end = start + usable;
        (now < end).then_some((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(voters: &[u64]) -> ConfState {
        let mut c = ConfState::default();
        c.voters = voters.to_vec();
        c
    }

    #[test]
    fn a_quorum_of_acks_starts_the_lease() {
        let t0 = Instant::now();
        let usable = Duration::from_millis(800);
        let mut l = Lease::default();
        l.reset(3);
        l.on_send(2, 10, t0);
        l.on_send(3, 20, t0 + Duration::from_millis(5));
        // No acks yet: no lease (the leader alone is not a quorum of 3).
        assert!(
            l.valid_until(&cs(&[1, 2, 3]), 1, t0 + Duration::from_millis(10), usable)
                .is_none()
        );
        // An ack for a frame that didn't carry the group counts for nothing.
        l.on_ack(2, &[9]);
        assert!(
            l.valid_until(&cs(&[1, 2, 3]), 1, t0 + Duration::from_millis(10), usable)
                .is_none()
        );
        l.on_ack(2, &[10]);
        let (start, end) = l
            .valid_until(&cs(&[1, 2, 3]), 1, t0 + Duration::from_millis(10), usable)
            .expect("lease");
        assert_eq!(start, t0);
        assert_eq!(end, t0 + usable);
        // It expires.
        assert!(
            l.valid_until(&cs(&[1, 2, 3]), 1, t0 + usable, usable)
                .is_none()
        );
        // Five voters need two acks besides the leader.
        assert!(
            l.valid_until(&cs(&[1, 2, 3, 4, 5]), 1, t0, usable)
                .is_none()
        );
        // A joint configuration needs both halves.
        let mut j = cs(&[1, 2, 3]);
        j.voters_outgoing = vec![1, 4, 5];
        assert!(l.valid_until(&j, 1, t0, usable).is_none());
        l.disable();
        assert!(l.valid_until(&cs(&[1, 2, 3]), 1, t0, usable).is_none());
    }
}
