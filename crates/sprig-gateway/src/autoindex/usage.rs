//! Which automatic indexes plans read: a count per (tenant, table, index)
//! since this process started, and the time of the last read, for the
//! loop's watch. Only names starting `sprig_auto_` are counted, so a
//! statement on other indexes pays a prefix check and nothing else. The
//! loop writes the counts into the index records every minute and when the
//! process stops ([`take_dirty`] names the tenants with new reads), so a
//! restart forgets at most a minute of them.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_plan::ExplainRow;

use crate::engine::general::General;

type Key = (u64, u64, String);

#[derive(Default)]
struct Counts {
    /// Reads and the last read's time (µs since the epoch), per index.
    by_index: HashMap<Key, (u64, u64)>,
    /// Tenants read since the last [`take_dirty`].
    dirty: BTreeSet<u64>,
}

fn counts() -> &'static Mutex<Counts> {
    static C: OnceLock<Mutex<Counts>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(Counts::default()))
}

fn now_us() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(u64::MAX)
}

fn auto(name: &str) -> bool {
    name.len() >= 11 && name[..11].eq_ignore_ascii_case("sprig_auto_")
}

/// A plan of table `object` of `tenant` reads index `name`.
pub(crate) fn note_read(tenant: u64, object: u64, name: &str) {
    if !auto(name) {
        return;
    }
    let at = now_us();
    let mut c = counts().lock().unwrap_or_else(PoisonError::into_inner);
    let e = c
        .by_index
        .entry((tenant, object, name.to_lowercase()))
        .or_default();
    e.0 += 1;
    e.1 = at;
    c.dirty.insert(tenant);
}

/// The keys a general plan of `g` reads, from its `EXPLAIN` rows.
pub(crate) fn note_plan(tenant: u64, g: &General, explain: &[ExplainRow]) {
    for key in explain.iter().filter_map(|r| r.key.as_deref()) {
        for k in key.split(',').map(str::trim).filter(|k| auto(k)) {
            if let Some(t) = g
                .tables
                .iter()
                .find(|t| t.def.indexes.iter().any(|x| x.name.eq_ignore_ascii_case(k)))
            {
                note_read(tenant, t.def.object.0, k);
            }
        }
    }
}

/// Plans that read index `name` of table `object` of `tenant`.
pub(crate) fn reads(tenant: u64, object: u64, name: &str) -> u64 {
    counts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .by_index
        .get(&(tenant, object, name.to_lowercase()))
        .map_or(0, |e| e.0)
}

/// When a plan last read index `name` of table `object` of `tenant`.
pub(crate) fn last_read_us(tenant: u64, object: u64, name: &str) -> Option<u64> {
    counts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .by_index
        .get(&(tenant, object, name.to_lowercase()))
        .map(|e| e.1)
}

/// The tenants whose automatic indexes were read since the last call.
pub(crate) fn take_dirty() -> Vec<u64> {
    let mut c = counts().lock().unwrap_or_else(PoisonError::into_inner);
    std::mem::take(&mut c.dirty).into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_automatic_indexes_are_counted() {
        note_read(9_999_001, 5, "k_user");
        note_read(9_999_001, 5, "sprig_auto_0011aabb");
        note_read(9_999_001, 5, "SPRIG_AUTO_0011AABB");
        assert_eq!(reads(9_999_001, 5, "k_user"), 0);
        assert_eq!(reads(9_999_001, 5, "sprig_auto_0011aabb"), 2);
        assert_eq!(reads(9_999_001, 6, "sprig_auto_0011aabb"), 0);
        assert!(last_read_us(9_999_001, 5, "sprig_auto_0011aabb").is_some());
        assert!(last_read_us(9_999_001, 5, "k_user").is_none());
        assert!(take_dirty().contains(&9_999_001));
    }
}
