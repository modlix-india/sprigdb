//! The references of overrides that become orphans across tenants, in the
//! orphan pass over a tenant tree ([`crate::unshare::orphan_pass`]), as the
//! copy worker's pass keeps them on MariaDB databases.
//!
//! ```text
//! before x's overrides of keys K become orphans (fill):
//!   for each foreign key l of x's table, the overrides of K that inherit
//!   it (l's override bits unset) take the value x's view showed when the
//!   record left: walking up from x's parent, the first level whose row
//!   holds the record as its own (own or orphan), else the first level
//!   whose handover claim of l covers it (links::HANDOVER, newest last)
//! after they became orphans (claim):
//!   each orphan of K holding all of l's columns claims the key it
//!   references, in the reference index of the referenced table's tree root
//! ```
//!
//! Once an orphan has its own claim, a delete of the referenced key sees
//! it there, and the handover that stood in for it is no longer needed
//! (`links::handover_live` finds the claim and lets the handover go).
//! Every step reads the current state and can run again.

use std::collections::{BTreeMap, BTreeSet};

use sprig_catalog::ddl::database_key;
use sprig_common::Result;
use sprig_common::ids::{ObjectId, TenantId};

use crate::Runner;
use crate::ids::TreeTable;
use crate::links::{At, HANDOVER, Ref, bits, canon, hex_values};
use crate::model::{InheritableObject, ROOT_SPACE};
use crate::plan::{state_of, tenant_edge};
use crate::refs::RefsOp;
use crate::unshare::{CHUNK, key_pred};
use crate::view::q;

/// One level above the orphans' tenant: its tenant, its object of the
/// table, the table's qualified physical name and its key.
struct Level {
    tenant: TenantId,
    object: ObjectId,
    table: String,
    key: Vec<String>,
}

fn text(row: &[Option<Vec<u8>>], i: usize) -> Option<String> {
    row.get(i)
        .and_then(Option::as_ref)
        .map(|v| String::from_utf8_lossy(v).into_owned())
}

/// A key part as [`hex_values`] writes it (`CONVERT(X'..' USING utf8mb4)`)
/// with a character set introducer instead (`_utf8mb4 X'..'`), which the
/// engine's single-table writes take.
fn introduced(v: &str) -> String {
    v.strip_prefix("CONVERT(")
        .and_then(|r| r.strip_suffix(" USING utf8mb4)"))
        .map_or_else(|| v.to_owned(), |h| format!("_utf8mb4 {h}"))
}

/// The tree root (tenant, object) of table `db.table` as tenant `x`
/// inherits it: where the claims of keys it holds live.
///
/// # Errors
/// The cache's; 39892.
pub async fn ref_root(
    r: &mut dyn Runner,
    x: TenantId,
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<Option<(TenantId, ObjectId)>> {
    let cat = r.tenant_catalog(x).await?;
    let Some(st) = state_of(&cat) else {
        return Ok(None);
    };
    let Some(d) = cat.database(&database_key(db, lctn)) else {
        return Ok(None);
    };
    let Some(o) = st.object_named(d.db_id, table, lctn) else {
        return Ok(None);
    };
    let mut root = (x, o.object_id);
    let mut lineage = Vec::with_capacity(cat.ancestry.len() + 1);
    for a in cat.ancestry.iter() {
        lineage.push(r.tenant_catalog(a.id).await?);
    }
    lineage.push(std::sync::Arc::clone(&cat));
    let mut j = lineage.len() - 1;
    while j >= 1 {
        match tenant_edge(&lineage[j - 1], &lineage[j], db, table, lctn)? {
            Some(up) => {
                root = (lineage[j - 1].tenant, up.parent_obj.object_id);
                j -= 1;
            }
            None => break,
        }
    }
    Ok(Some(root))
}

/// The levels of `above` (top first) holding `table`.
async fn levels(
    r: &mut dyn Runner,
    above: &[&TreeTable],
    db: &str,
    table: &str,
    lctn: bool,
) -> Result<Vec<Level>> {
    let mut out = Vec::with_capacity(above.len());
    for y in above {
        let cat = r.tenant_catalog(y.tenant).await?;
        let Some(st) = state_of(&cat) else { continue };
        let Some(d) = cat.database(&database_key(db, lctn)) else {
            continue;
        };
        let Some(o) = st.object_named(d.db_id, table, lctn) else {
            continue;
        };
        out.push(Level {
            tenant: y.tenant,
            object: o.object_id,
            table: format!("{}.{}", q(&y.schema), q(&o.table)),
            key: o.key.clone(),
        });
    }
    Ok(out)
}

/// Before `x`'s overrides of `lost` become orphans: each foreign key they
/// inherit takes the value `x`'s view showed when the record left (module
/// docs). `above`: the levels whose records `x` inherits, top first;
/// `xtable`: `x`'s table, qualified.
///
/// # Errors
/// The statements'.
#[allow(clippy::too_many_arguments)] // the pass's parts, as it holds them
pub async fn fill(
    r: &mut dyn Runner,
    x: &TreeTable,
    xo: &InheritableObject,
    db: &str,
    above: &[&TreeTable],
    xtable: &str,
    lost: &[Vec<String>],
    lctn: bool,
) -> Result<()> {
    let levels = levels(r, above, db, &xo.table, lctn).await?;
    let key_hex = canon(&xo.key, None);
    for l in &xo.links {
        let b = bits(xo, &l.columns);
        let mut need: BTreeSet<String> = BTreeSet::new();
        // Each needed row's key, as literals (the writes below find rows by
        // key: the engine's single-table writes take plain comparisons).
        let mut by_hex: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let cols = xo.key.iter().map(|k| q(k)).collect::<Vec<_>>().join(", ");
        for part in lost.chunks(CHUNK) {
            let rows = r
                .ctl_query(
                    x.tenant,
                    &format!(
                        "SELECT HEX({key_hex}), {cols} FROM {xtable} WHERE `_sprig_space` = {} \
                         AND `_sprig_state` = 'ovr' AND `_sprig_ovr` & {b} <> {b} AND {}",
                        ROOT_SPACE.0,
                        key_pred(&xo.key, part)
                    ),
                )
                .await?;
            for row in rows {
                let Some(h) = text(&row, 0) else { continue };
                let lits = row[1..]
                    .iter()
                    .map(|v| {
                        crate::copy::key_text(v.as_deref().map(String::from_utf8_lossy).as_deref())
                    })
                    .collect();
                need.insert(h.clone());
                by_hex.insert(h, lits);
            }
        }
        if need.is_empty() {
            continue;
        }
        let lits = |need: &BTreeSet<String>| -> Vec<String> {
            need.iter().map(|h| format!("X'{h}'")).collect()
        };
        // The handovers of every level above, newest last.
        let mut handed: BTreeMap<(u64, String), String> = BTreeMap::new();
        if !levels.is_empty()
            && let Some(root) = ref_root(r, x.tenant, db, &l.ref_table, lctn).await?
        {
            let pairs: Vec<(u64, u64)> = levels.iter().map(|y| (y.tenant.0, y.object.0)).collect();
            let link = format!("{HANDOVER}{}", l.name);
            let froms = lits(&need);
            for chunk in froms.chunks(CHUNK) {
                let rows = r
                    .refs(
                        root.0,
                        &RefsOp::Handed {
                            ref_obj: root.1,
                            link: &link,
                            pairs: &pairs,
                            from_keys: chunk,
                        },
                    )
                    .await?;
                for row in rows {
                    if let (Some(t), Some(k), Some(v)) =
                        (text(&row, 0), text(&row, 1), text(&row, 2))
                    {
                        handed.insert((t.parse().unwrap_or(0), k), v);
                    }
                }
            }
        }
        let mut values: BTreeMap<String, String> = BTreeMap::new();
        for y in levels.iter().rev() {
            if need.is_empty() {
                break;
            }
            let ykey = canon(&y.key, None);
            for chunk in lits(&need).chunks(CHUNK) {
                let rows = r
                    .ctl_query(
                        y.tenant,
                        &format!(
                            "SELECT HEX({ykey}), HEX({}) FROM {} WHERE `_sprig_space` = {} AND \
                             `_sprig_state` IN ('own', 'orphan') AND {ykey} IN ({})",
                            canon(&l.columns, None),
                            y.table,
                            ROOT_SPACE.0,
                            chunk.join(", ")
                        ),
                    )
                    .await?;
                for row in rows {
                    let Some(k) = text(&row, 0) else { continue };
                    need.remove(&k);
                    if let Some(v) = text(&row, 1) {
                        values.insert(k, v);
                    }
                }
            }
            for k in need.clone() {
                if let Some(v) = handed.get(&(y.tenant.0, k.clone())) {
                    values.insert(k.clone(), v.clone());
                    need.remove(&k);
                }
            }
        }
        tracing::debug!(tenant = %x.tenant, table = %xo.table, link = %l.name, left = need.len(), filled = values.len(), "orphans' inherited references filled");
        for (k, v) in &values {
            let Some(key) = by_hex.get(k) else { continue };
            let sets: Vec<String> = l
                .columns
                .iter()
                .zip(hex_values(&format!("X'{v}'")))
                .map(|(col, val)| format!("{} = {}", q(col), introduced(&val)))
                .collect();
            r.ctl_query(
                x.tenant,
                &format!(
                    "UPDATE {xtable} SET {} WHERE `_sprig_space` = {} AND `_sprig_state` = 'ovr' \
                     AND {}",
                    sets.join(", "),
                    ROOT_SPACE.0,
                    key_pred(&xo.key, std::slice::from_ref(key))
                ),
            )
            .await?;
        }
    }
    Ok(())
}

/// After `x`'s overrides of `lost` became orphans: claims, in the
/// reference index, of what each orphan references (module docs).
///
/// # Errors
/// The statements' and the index's.
pub async fn claim(
    r: &mut dyn Runner,
    x: &TreeTable,
    xo: &InheritableObject,
    db: &str,
    xtable: &str,
    lost: &[Vec<String>],
    lctn: bool,
) -> Result<()> {
    for l in &xo.links {
        let Some(root) = ref_root(r, x.tenant, db, &l.ref_table, lctn).await? else {
            continue;
        };
        let mut found: Vec<Ref> = Vec::new();
        for part in lost.chunks(CHUNK) {
            let rows = r
                .ctl_query(
                    x.tenant,
                    &format!(
                        "SELECT HEX({}), HEX({}) FROM {xtable} WHERE `_sprig_space` = {} AND \
                         `_sprig_state` = 'orphan' AND {} AND {}",
                        canon(&xo.key, None),
                        canon(&l.columns, None),
                        ROOT_SPACE.0,
                        key_pred(&xo.key, part),
                        l.columns
                            .iter()
                            .map(|c| format!("{} IS NOT NULL", q(c)))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    ),
                )
                .await?;
            found.extend(rows.iter().filter_map(|row| {
                Some(Ref {
                    from_hex: format!("X'{}'", text(row, 0)?),
                    ref_hex: format!("X'{}'", text(row, 1)?),
                    ref_sql: String::new(),
                })
            }));
        }
        let at = At {
            tenant: x.tenant,
            space: ROOT_SPACE,
        };
        for chunk in found.chunks(CHUNK) {
            r.refs(
                root.0,
                &RefsOp::Claim {
                    ref_obj: root.1,
                    here: at,
                    from_obj: xo.object_id,
                    link: &l.name,
                    refs: chunk,
                },
            )
            .await?;
        }
        metrics::counter!("sprig_inherit_orphan_claims_total").increment(found.len() as u64);
        tracing::debug!(tenant = %x.tenant, table = %xo.table, link = %l.name, claims = found.len(), root = %root.0, "orphans claimed their references");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn key_parts_take_an_introducer() {
        assert_eq!(
            super::introduced("CONVERT(X'3130' USING utf8mb4)"),
            "_utf8mb4 X'3130'"
        );
        assert_eq!(super::introduced("''"), "''");
    }
}
