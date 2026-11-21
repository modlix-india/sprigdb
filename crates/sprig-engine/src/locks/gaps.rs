//! Gap locks: which stretches of an index a transaction has closed to other
//! transactions' inserts.
//!
//! A gap lock never conflicts with another gap lock or with a record lock.
//! Its only effect is on inserts: a transaction that wants to insert a key
//! waits while another transaction's gap covers that key.
//!
//! Places in key order are [`Pos`] values: "at key `k`" and "just after key
//! `k`". A locked stretch is a half-open interval of places, so the four
//! shapes a locking read needs are all one type:
//!
//! | Shape | Interval |
//! |---|---|
//! | the open gap between keys `a` and `b` | `[after(a), at(b))` |
//! | that gap and the entry `b` itself (next-key) | `[after(a), after(b))` |
//! | a scanned range `[a, b)` | `[at(a), at(b))` |
//! | one missing key `k` | `[at(k), after(k))` |
//!
//! Each transaction's intervals in one index are kept merged, so a scan over
//! a million rows holds one interval, and the check an insert makes is one
//! ordered lookup per transaction that holds gaps in that index.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::keys::{self, PREFIX_LEN};

/// A place in key order.
///
/// `Key(k, false)` is "at `k`": the key itself and everything after it.
/// `Key(k, true)` is "just after `k`". They sort in key order, with "at"
/// before "after" for the same key, and [`Pos::End`] after every key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Pos {
    /// At a key (`false`) or just after it (`true`).
    Key(Bytes, bool),
    /// After every key.
    End,
}

impl Pos {
    /// At `key`: an interval starting here includes the key; one ending here
    /// doesn't.
    #[must_use]
    pub fn at(key: impl Into<Bytes>) -> Pos {
        Pos::Key(key.into(), false)
    }

    /// Just after `key`: an interval starting here excludes the key; one
    /// ending here includes it.
    #[must_use]
    pub fn after(key: impl Into<Bytes>) -> Pos {
        Pos::Key(key.into(), true)
    }

    fn len(&self) -> usize {
        match self {
            Pos::Key(k, _) => k.len(),
            Pos::End => 0,
        }
    }
}

/// Approximate memory charged per gap interval beyond its two bounds.
pub(super) const GAP_OVERHEAD_BYTES: usize = 64;

fn cost(lo: &Pos, hi: &Pos) -> usize {
    lo.len() + hi.len() + GAP_OVERHEAD_BYTES
}

/// The memory a new interval `[lo, hi)` is charged before merging.
pub(super) fn cost_of(lo: &Pos, hi: &Pos) -> usize {
    cost(lo, hi)
}

/// One transaction's gap locks in one index: disjoint, merged intervals.
#[derive(Debug, Default)]
pub(super) struct GapSet {
    /// `lo → hi` for each interval `[lo, hi)`.
    ivs: BTreeMap<Pos, Pos>,
    bytes: usize,
}

impl GapSet {
    /// Whether some interval contains `p`.
    pub(super) fn covers(&self, p: &Pos) -> bool {
        self.ivs
            .range::<Pos, _>(..=p)
            .next_back()
            .is_some_and(|(_, hi)| hi > p)
    }

    /// Whether one interval contains all of `[lo, hi)`. The set is merged,
    /// so a stretch split over two intervals has a hole in it.
    pub(super) fn covers_range(&self, lo: &Pos, hi: &Pos) -> bool {
        self.ivs
            .range::<Pos, _>(..=lo)
            .next_back()
            .is_some_and(|(_, h)| h >= hi)
    }

    /// Adds `[lo, hi)`, merging it with every interval it touches. Returns
    /// the set's memory before and after.
    pub(super) fn add(&mut self, mut lo: Pos, mut hi: Pos) -> (usize, usize) {
        let before = self.bytes;
        if lo >= hi {
            return (before, before);
        }
        // An interval that starts at or before `lo` and reaches it.
        if let Some((l, h)) = self.ivs.range::<Pos, _>(..=&lo).next_back()
            && *h >= lo
        {
            if *h >= hi {
                return (before, before);
            }
            lo = l.clone();
        }
        // Every interval that starts inside `[lo, hi]` is absorbed.
        let absorbed: Vec<Pos> = self
            .ivs
            .range::<Pos, _>(&lo..=&hi)
            .map(|(l, _)| l.clone())
            .collect();
        for l in absorbed {
            if let Some(h) = self.ivs.remove(&l) {
                self.bytes = self.bytes.saturating_sub(cost(&l, &h));
                if h > hi {
                    hi = h;
                }
            }
        }
        self.bytes += cost(&lo, &hi);
        self.ivs.insert(lo, hi);
        (before, self.bytes)
    }

    /// Number of merged intervals.
    pub(super) fn len(&self) -> usize {
        self.ivs.len()
    }

    /// Memory charged for the set.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// The intervals, in key order.
    pub(super) fn intervals(&self) -> impl Iterator<Item = (&Pos, &Pos)> {
        self.ivs.iter()
    }
}

/// The index a key belongs to: its fixed head (tenant, shard, object, kind,
/// index number). Shorter keys have none.
pub(super) fn bucket_of_key(key: &[u8]) -> Option<&[u8]> {
    key.get(..PREFIX_LEN)
}

/// The index an interval lies in, or the empty bucket when it spans more
/// than one index (every insert checks that bucket too).
pub(super) fn bucket_of(lo: &Pos, hi: &Pos) -> Bytes {
    let Pos::Key(l, _) = lo else {
        return Bytes::new();
    };
    if l.len() < PREFIX_LEN {
        return Bytes::new();
    }
    let head = &l[..PREFIX_LEN];
    let inside = match hi {
        Pos::Key(h, after) => {
            h.starts_with(head) || (!after && keys::prefix_end(head).as_deref() == Some(&h[..]))
        }
        Pos::End => false,
    };
    if inside {
        l.slice(..PREFIX_LEN)
    } else {
        Bytes::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> Pos {
        Pos::at(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn after(s: &str) -> Pos {
        Pos::after(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn places_sort_in_key_order_with_at_before_after() {
        assert!(at("a") < after("a"));
        assert!(after("a") < at("b"));
        assert!(after("a") < at("a\0"));
        assert!(after("zzz") < Pos::End);
    }

    #[test]
    fn an_open_gap_excludes_both_keys_and_a_next_key_includes_the_upper() {
        let mut g = GapSet::default();
        g.add(after("b"), at("f"));
        assert!(!g.covers(&at("b")));
        assert!(g.covers(&at("c")));
        assert!(g.covers(&at("e")));
        assert!(!g.covers(&at("f")));
        g.add(after("b"), after("f"));
        assert!(g.covers(&at("f")));
        assert!(!g.covers(&at("g")));
        assert_eq!(g.len(), 1);
    }

    #[test]
    fn a_missing_key_is_one_place() {
        let mut g = GapSet::default();
        g.add(at("m"), after("m"));
        assert!(g.covers(&at("m")));
        assert!(!g.covers(&at("l")));
        assert!(!g.covers(&at("m\0")));
        assert!(!g.covers(&at("n")));
    }

    #[test]
    fn touching_and_overlapping_intervals_merge() {
        let mut g = GapSet::default();
        g.add(at("a"), at("c"));
        g.add(at("e"), at("g"));
        g.add(at("i"), at("k"));
        assert_eq!(g.len(), 3);
        // Touches the first, overlaps the second.
        assert!(!g.covers_range(&at("b"), &at("f")));
        g.add(at("c"), at("f"));
        assert!(g.covers_range(&at("b"), &at("f")));
        assert_eq!(g.len(), 2);
        assert!(g.covers(&at("d")));
        assert!(!g.covers(&at("g")));
        // Swallows everything.
        g.add(at("0"), Pos::End);
        assert_eq!(g.len(), 1);
        assert!(g.covers(&at("zz")));
        // Already covered: nothing changes.
        assert!(g.covers_range(&at("b"), &at("c")));
        assert!(g.covers_range(&at("0"), &Pos::End));
        assert!(!g.covers_range(&at("/"), &at("c")));
        let (b0, b1) = g.add(at("b"), at("c"));
        assert_eq!(b0, b1);
        assert_eq!(g.bytes(), cost(&at("0"), &Pos::End));
    }

    #[test]
    fn a_scan_that_moves_forward_holds_one_interval() {
        let mut g = GapSet::default();
        let mut from = at("k000");
        for i in 1..1000 {
            let to = after(&format!("k{i:03}"));
            g.add(from, to.clone());
            from = to;
        }
        assert_eq!(g.len(), 1);
        assert!(g.covers(&at("k500")));
        assert!(g.covers(&at("k500x")));
        assert!(!g.covers(&at("k999x")));
    }

    #[test]
    fn empty_and_backwards_intervals_add_nothing() {
        let mut g = GapSet::default();
        g.add(at("b"), at("b"));
        g.add(after("b"), at("b"));
        assert_eq!(g.len(), 0);
        assert_eq!(g.bytes(), 0);
    }

    proptest::proptest! {
        /// Against a plain model: one flag per place over a small alphabet.
        #[test]
        fn a_merged_set_covers_exactly_what_was_added(
            adds in proptest::collection::vec((0usize..24, 0usize..24), 0..40)
        ) {
            // Place 2i is "at key i", place 2i+1 is "just after key i".
            let place = |n: usize| {
                let key = Bytes::from(vec![b'a' + u8::try_from(n / 2).unwrap_or(0)]);
                if n.is_multiple_of(2) { Pos::at(key) } else { Pos::after(key) }
            };
            let mut g = GapSet::default();
            let mut model = [false; 24];
            for (a, b) in adds {
                g.add(place(a), place(b));
                for slot in model.iter_mut().take(b).skip(a) {
                    *slot = true;
                }
                // Disjoint, in order, never touching: fully merged.
                let ivs: Vec<_> = g.intervals().collect();
                for w in ivs.windows(2) {
                    proptest::prop_assert!(w[0].1 < w[1].0);
                }
                let bytes: usize = ivs.iter().map(|(l, h)| cost(l, h)).sum();
                proptest::prop_assert_eq!(g.bytes(), bytes);
            }
            for (n, want) in model.iter().enumerate() {
                proptest::prop_assert_eq!(g.covers(&place(n)), *want, "place {}", n);
            }
            for a in 0..24 {
                for b in a + 1..24 {
                    let whole = model[a..b].iter().all(|c| *c);
                    proptest::prop_assert_eq!(g.covers_range(&place(a), &place(b)), whole);
                }
            }
        }
    }

    #[test]
    fn intervals_are_filed_under_their_index() {
        let head = [7u8; PREFIX_LEN];
        let k = |tail: &[u8]| {
            let mut v = head.to_vec();
            v.extend_from_slice(tail);
            Bytes::from(v)
        };
        let end = Bytes::from(keys::prefix_end(&head).unwrap_or_default());
        assert_eq!(
            &bucket_of(&Pos::at(k(b"a")), &Pos::after(k(b"b")))[..],
            head
        );
        assert_eq!(
            &bucket_of(&Pos::at(k(b"")), &Pos::at(end.clone()))[..],
            head
        );
        // Past the index's end, or unbounded: the shared bucket.
        assert!(bucket_of(&Pos::at(k(b"a")), &Pos::after(end)).is_empty());
        assert!(bucket_of(&Pos::at(k(b"a")), &Pos::End).is_empty());
        assert!(bucket_of(&at("short"), &at("shorter")).is_empty());
        assert_eq!(bucket_of_key(&k(b"x")), Some(&head[..]));
        assert_eq!(bucket_of_key(b"short"), None);
    }
}
