//! Which automatic indexes plans read: a count per (tenant, table, index)
//! since this process started, for the loop's watch. Only names starting
//! `sprig_auto_` are counted, so a statement on other indexes pays a
//! prefix check and nothing else.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use sprig_plan::ExplainRow;

use crate::engine::general::General;

type Key = (u64, u64, String);

fn counts() -> &'static Mutex<HashMap<Key, u64>> {
    static C: OnceLock<Mutex<HashMap<Key, u64>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn auto(name: &str) -> bool {
    name.len() >= 11 && name[..11].eq_ignore_ascii_case("sprig_auto_")
}

/// A plan of table `object` of `tenant` reads index `name`.
pub(crate) fn note_read(tenant: u64, object: u64, name: &str) {
    if !auto(name) {
        return;
    }
    *counts()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry((tenant, object, name.to_lowercase()))
        .or_default() += 1;
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
        .get(&(tenant, object, name.to_lowercase()))
        .copied()
        .unwrap_or(0)
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
    }
}
