//! A transaction's buffered writes, with an undo log for statement rollback
//! and savepoints.

use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::keys::KeyRange;

/// A position in the undo log: statement starts and savepoints.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Mark(usize);

/// A key's buffered entry before a write.
#[derive(Debug)]
enum Prev {
    /// Not buffered.
    Absent,
    /// Buffered with this value (`None`: delete).
    Was(Option<Bytes>),
}

/// Buffered writes: logical key → value (`None`: delete).
#[derive(Debug, Default)]
pub struct WriteSet {
    map: BTreeMap<Bytes, Option<Bytes>>,
    /// `(key, previous entry)`, oldest first.
    undo: Vec<(Bytes, Prev)>,
    bytes: usize,
}

impl WriteSet {
    /// Records a write.
    pub fn put(&mut self, key: Bytes, value: Option<Bytes>) {
        self.bytes += key.len() + value.as_ref().map_or(0, Bytes::len);
        let prev = self
            .map
            .insert(key.clone(), value)
            .map_or(Prev::Absent, Prev::Was);
        self.undo.push((key, prev));
    }

    /// The buffered entry for `key`: `Some(None)` is a buffered delete.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&Option<Bytes>> {
        self.map.get(key)
    }

    /// Buffered entries in a key range, in key order.
    pub fn range<'a>(
        &'a self,
        range: &KeyRange,
    ) -> impl DoubleEndedIterator<Item = (&'a Bytes, &'a Option<Bytes>)> + 'a {
        let lo: Bound<&[u8]> = Bound::Included(range.start.as_slice());
        let hi: Bound<&[u8]> = range
            .end
            .as_deref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        self.map.range::<[u8], _>((lo, hi))
    }

    /// The current undo position.
    #[must_use]
    pub fn mark(&self) -> Mark {
        Mark(self.undo.len())
    }

    /// Undoes every write after `mark`.
    pub fn rollback_to(&mut self, mark: Mark) {
        while self.undo.len() > mark.0 {
            let Some((k, prev)) = self.undo.pop() else {
                break;
            };
            match prev {
                Prev::Absent => {
                    self.map.remove(&k);
                }
                Prev::Was(p) => {
                    self.map.insert(k, p);
                }
            }
        }
    }

    /// Number of distinct keys written.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether nothing is buffered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Undo entries (the deadlock weight's "undo records").
    #[must_use]
    pub fn undo_len(&self) -> usize {
        self.undo.len()
    }

    /// Bytes written so far (keys plus values, repeats included).
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The writes, in key order, consuming the set.
    #[must_use]
    pub fn into_writes(self) -> Vec<(Bytes, Option<Bytes>)> {
        self.map.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn rollback_to_restores_earlier_states() {
        let mut ws = WriteSet::default();
        ws.put(b("a"), Some(b("1")));
        let m1 = ws.mark();
        ws.put(b("a"), Some(b("2")));
        ws.put(b("b"), None);
        let m2 = ws.mark();
        ws.put(b("c"), Some(b("3")));
        ws.rollback_to(m2);
        assert_eq!(ws.get(b"c"), None);
        assert_eq!(ws.get(b"b"), Some(&None));
        ws.rollback_to(m1);
        assert_eq!(ws.get(b"a"), Some(&Some(b("1"))));
        assert_eq!(ws.get(b"b"), None);
        assert_eq!(ws.len(), 1);
        ws.rollback_to(Mark(0));
        assert!(ws.is_empty());
    }
}
