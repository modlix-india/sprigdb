//! Unique lookup indexes in plans:
//!
//! - a single-table read, `UPDATE` or `DELETE` over every shard with an
//!   equality (`c = v`, `c IN (...)`) on each of a public lookup's columns
//!   (or `(a, b) = (v, w)`, `(a, b) IN ((v, w), ...)` for a lookup of
//!   several columns) becomes [`Plan::ViaLookup`]: the executor reads the
//!   owners' routing hashes and keeps only their shards;
//! - an `INSERT` into a table with maintained lookups claims each row's
//!   values ([`Claim`]) before the rows are written, in the write's
//!   transaction;
//! - a `DELETE` reads the values and primary keys of the rows it removes
//!   ([`Release`]) and releases them after the transaction commits;
//! - forms that would leave a lookup unmaintained are refused with
//!   reason `lookup`.

use std::collections::HashMap;
use std::sync::Arc;

use sprig_shard::{Charset, KeyValueOwned};
use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, Insert, SelectItem, SetExpr, Statement, TableWithJoins, Value,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::{
    PlanCtx, R, Rel, col_rel, conjuncts, empty_select, ident_parts, key_literal, pass_sql, relation,
};
use crate::plan::{Claim, LookupProbe, LookupSet, Plan, Release, Target, ViaLookup};
use crate::reject::{Reason, Rejection, reject};
use crate::topology::{Group, KeyClass, KeyValue, LookupRef, TableEntry};

/// `plan` narrowed by a lookup when it runs on several shards of one
/// table whose `WHERE` has an equality on a public lookup's column.
pub(super) fn via_lookup(stmt: &Statement, plan: Plan, cx: &PlanCtx<'_>) -> Plan {
    let multi = match &plan {
        Plan::Read(r) => r.targets.len() > 1,
        Plan::Write(w) => w.targets.len() > 1,
        _ => false,
    };
    if !multi {
        return plan;
    }
    let Some((rel, selection)) = single_table(stmt, cx) else {
        return plan;
    };
    let Some(entry) = rel.entry else {
        return plan;
    };
    if !entry.lookups.iter().any(|l| l.routes()) {
        return plan;
    }
    let rels = vec![rel];
    let mut cs = Vec::new();
    conjuncts(selection, &mut cs);
    for l in entry.lookups.iter().filter(|l| l.routes()) {
        if let Some(values) = probe_values(&rels, l, &cs, cx) {
            return Plan::ViaLookup(Box::new(ViaLookup {
                probe: LookupProbe {
                    lookup: Arc::clone(l),
                    values,
                },
                group: Arc::clone(&entry.group),
                plan,
            }));
        }
    }
    plan
}

/// The one table and the `WHERE` of a single-table `SELECT`, `UPDATE` or
/// `DELETE`.
fn single_table<'s, 'a>(stmt: &'s Statement, cx: &PlanCtx<'a>) -> Option<(Rel<'a>, &'s Expr)> {
    let (twj, selection): (&TableWithJoins, Option<&Expr>) = match stmt {
        Statement::Query(q) => {
            let SetExpr::Select(sel) = &*q.body else {
                return None;
            };
            let [twj] = sel.from.as_slice() else {
                return None;
            };
            (twj, sel.selection.as_ref())
        }
        Statement::Update(u) if u.from.is_none() => (&u.table, u.selection.as_ref()),
        Statement::Delete(d) if d.tables.is_empty() && d.using.is_none() => {
            let from = match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            let [twj] = from.as_slice() else {
                return None;
            };
            (twj, d.selection.as_ref())
        }
        _ => return None,
    };
    if !twj.joins.is_empty() {
        return None;
    }
    Some((relation(&twj.relation, cx, 0)?, selection?))
}

/// The values the conjuncts `cs` give every column of lookup `l`, one
/// `Vec` per value to look up (a part per column): a tuple equality or
/// `IN` naming the lookup's columns, or for each column `c = v`, `v = c`
/// or `c IN (v, ...)` (several columns: every combination, at most
/// `router.max_route_values`), every value a literal of its column's class.
fn probe_values(
    rels: &[Rel<'_>],
    l: &LookupRef,
    cs: &[&Expr],
    cx: &PlanCtx<'_>,
) -> Option<Vec<Vec<KeyValueOwned>>> {
    if l.columns.len() > 1
        && let Some(v) = cs.iter().find_map(|c| tuple_values(rels, l, c, cx))
    {
        return Some(v);
    }
    let mut per: Vec<Option<Vec<KeyValueOwned>>> = vec![None; l.columns.len()];
    for c in cs {
        for (i, slot) in per.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = column_values(rels, l, i, c, cx);
            }
        }
    }
    let per: Vec<Vec<KeyValueOwned>> = per.into_iter().collect::<Option<_>>()?;
    let total = per
        .iter()
        .try_fold(1usize, |acc, v| acc.checked_mul(v.len()))?;
    if total == 0 || total > cx.max_route_values {
        return None;
    }
    let mut out: Vec<Vec<KeyValueOwned>> = vec![Vec::new()];
    for vs in per {
        let mut next = Vec::with_capacity(out.len() * vs.len());
        for prefix in &out {
            for v in &vs {
                let mut row = prefix.clone();
                row.push(v.clone());
                next.push(row);
            }
        }
        out = next;
    }
    Some(out)
}

/// The position of column expression `e` among `l`'s value columns.
fn lookup_col(rels: &[Rel<'_>], l: &LookupRef, e: &Expr) -> Option<usize> {
    let (_, n) = col_rel(rels, e)?;
    l.columns.iter().position(|c| *c == n)
}

/// `(a, b) = (v, w)` or `(a, b) IN ((v, w), ...)` naming exactly `l`'s
/// columns (in any order): the values in key order.
fn tuple_values(
    rels: &[Rel<'_>],
    l: &LookupRef,
    c: &Expr,
    cx: &PlanCtx<'_>,
) -> Option<Vec<Vec<KeyValueOwned>>> {
    let (cols, rows): (&[Expr], Vec<&Expr>) = match c {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (&**left, &**right) {
            (Expr::Tuple(cols), r @ Expr::Tuple(_)) | (r @ Expr::Tuple(_), Expr::Tuple(cols))
                if cols.iter().all(|e| lookup_col(rels, l, e).is_some()) =>
            {
                (cols.as_slice(), vec![r])
            }
            _ => return None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => match &**expr {
            Expr::Tuple(cols) => (cols.as_slice(), list.iter().collect()),
            _ => return None,
        },
        _ => return None,
    };
    let at: Vec<usize> = cols
        .iter()
        .map(|e| lookup_col(rels, l, e))
        .collect::<Option<_>>()?;
    let mut sorted = at.clone();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.len() != l.columns.len() || at.len() != l.columns.len() {
        return None;
    }
    if rows.is_empty() || rows.len() > cx.max_route_values {
        return None;
    }
    rows.into_iter()
        .map(|r| {
            let Expr::Tuple(vals) = r else {
                return None;
            };
            if vals.len() != at.len() {
                return None;
            }
            let mut out = vec![KeyValueOwned::Null; at.len()];
            for (e, &i) in vals.iter().zip(&at) {
                out[i] = value_of(e, l.classes[i], l.index.columns.get(i)?.class)?;
            }
            Some(out)
        })
        .collect()
}

/// The values conjunct `c` gives lookup `l`'s column `i`: `c = v`,
/// `v = c`, `c IN (v, ...)`, every value a literal of the column's class.
fn column_values(
    rels: &[Rel<'_>],
    l: &LookupRef,
    i: usize,
    c: &Expr,
    cx: &PlanCtx<'_>,
) -> Option<Vec<KeyValueOwned>> {
    let name = l.columns.get(i)?;
    let is_col = |e: &Expr| col_rel(rels, e).is_some_and(|(_, n)| n == *name);
    let lits: Vec<&Expr> = match c {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_col(left) {
                vec![&**right]
            } else if is_col(right) {
                vec![&**left]
            } else {
                return None;
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_col(expr) => list.iter().collect(),
        _ => return None,
    };
    if lits.is_empty() || lits.len() > cx.max_route_values {
        return None;
    }
    let class = *l.classes.get(i)?;
    let part = l.index.columns.get(i)?.class;
    lits.into_iter().map(|e| value_of(e, class, part)).collect()
}

/// A literal as the value `sprig-shard` normalizes: `None` when it isn't
/// one exact value of the class (an expression, `42.5` against an
/// integer). `NULL` is [`KeyValueOwned::Null`].
pub(super) fn value_of(
    e: &Expr,
    class: KeyClass,
    part: sprig_shard::KeyClass,
) -> Option<KeyValueOwned> {
    if let Expr::Value(v) = e
        && matches!(v.value, Value::Null)
    {
        return Some(KeyValueOwned::Null);
    }
    Some(match key_literal(e, class)? {
        KeyValue::Int(i) => KeyValueOwned::Int(i),
        KeyValue::Bytes(b) => match part {
            sprig_shard::KeyClass::String { .. } => KeyValueOwned::Str {
                bytes: b,
                charset: Charset::Utf8mb4,
            },
            _ => KeyValueOwned::Bytes(b),
        },
    })
}

/// The router's literal class of a `sprig-shard` key part.
fn router_class(part: sprig_shard::KeyClass) -> Option<KeyClass> {
    match part {
        sprig_shard::KeyClass::Numeric => Some(KeyClass::Int),
        sprig_shard::KeyClass::String { collation } => Some(KeyClass::Str {
            collation: collation.0,
        }),
        sprig_shard::KeyClass::Binary => Some(KeyClass::Binary),
        _ => None,
    }
}

/// Refuses writes to a table whose maintained lookups the router can't
/// follow.
pub(super) fn check_gap(entry: &TableEntry) -> R<()> {
    if entry.lookup_gap {
        return reject(
            Reason::Lookup,
            "a write to a table with a lookup index the router can't maintain",
        );
    }
    Ok(())
}

/// What an `INSERT` into a table with lookups needs: the forms that keep
/// lookups exact, and each row's claims. `key_pos` is the key column's
/// position in each row; `rows` the `VALUES` rows.
pub(super) fn insert_claims(
    ins: &Insert,
    entry: &TableEntry,
    key_pos: usize,
    rows: &[impl std::ops::Deref<Target = Vec<Expr>>],
) -> R<Vec<Claim>> {
    if entry.lookups.is_empty() {
        return Ok(Vec::new());
    }
    if ins.on.is_some() || ins.replace_into || ins.ignore {
        return reject(
            Reason::Lookup,
            "an upsert, REPLACE or INSERT IGNORE on a table with a unique lookup",
        );
    }
    if ins.returning.is_some() {
        return reject(
            Reason::Lookup,
            "INSERT … RETURNING on a table with a unique lookup",
        );
    }
    let position = |name: &str, ordinal: Option<usize>| -> R<usize> {
        if ins.columns.is_empty() {
            return ordinal.ok_or_else(|| {
                Rejection::new(
                    Reason::Lookup,
                    "an INSERT without a column list (a lookup column's position is unknown)",
                )
            });
        }
        ins.columns
            .iter()
            .position(|c| {
                ident_parts(c)
                    .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(name)))
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                Rejection::new(
                    Reason::Lookup,
                    format!("an INSERT without column {name} (a unique lookup's value or key)"),
                )
            })
    };
    let mut lookups: Vec<&Arc<LookupRef>> = entry.lookups.iter().collect();
    lookups.sort_by_key(|l| l.index.id);
    let mut claims = Vec::new();
    for l in lookups {
        let nv = l.columns.len();
        let mut cols: Vec<(&str, Option<sprig_shard::KeyClass>)> = l
            .columns
            .iter()
            .zip(&l.index.columns)
            .map(|(n, c)| (n.as_str(), Some(c.class)))
            .collect();
        cols.extend(
            l.pk.iter()
                .zip(&l.index.owner_pk)
                .map(|(n, c)| (n.as_str(), Some(c.class))),
        );
        let mut at = Vec::new();
        for (i, (name, _)) in cols.iter().enumerate() {
            at.push(position(name, l.ordinals.get(i).copied().flatten())?);
        }
        for row in rows {
            let mut parts = Vec::new();
            for ((_, part), p) in cols.iter().zip(&at) {
                let part = part.ok_or_else(|| Rejection::new(Reason::Lookup, "a column class"))?;
                let class = router_class(part)
                    .ok_or_else(|| Rejection::new(Reason::Lookup, "a column class"))?;
                let e = row
                    .get(*p)
                    .ok_or_else(|| Rejection::new(Reason::Lookup, "a short VALUES row"))?;
                parts.push(value_of(e, class, part).ok_or_else(|| {
                    Rejection::new(
                        Reason::Lookup,
                        format!("a unique lookup's value or key that isn't a literal: {e}"),
                    )
                })?);
            }
            let rk = owner_rk(&entry.group, row.get(key_pos))?;
            let owner_pk = parts.split_off(nv);
            claims.push(Claim {
                lookup: Arc::clone(l),
                value: parts,
                owner_pk,
                owner_rk: rk,
            });
        }
    }
    Ok(claims)
}

fn owner_rk(g: &Group, key: Option<&Expr>) -> R<u64> {
    key.and_then(|e| key_literal(e, g.key_class))
        .and_then(|v| g.hash_of(&v))
        .ok_or_else(|| Rejection::new(Reason::InsertKey, "a key value"))
}

/// An `UPDATE` must leave the primary key of a table with lookups alone
/// (the lookup rows name their owners by it).
pub(super) fn check_update(entry: &TableEntry, assigns: &dyn Fn(&str) -> bool) -> R<()> {
    for l in &entry.lookups {
        if l.pk.iter().any(|c| assigns(c)) {
            return reject(
                Reason::Lookup,
                "an UPDATE of the primary key of a table with a lookup index",
            );
        }
    }
    Ok(())
}

/// The reads an `UPDATE` of a lookup's column needs before it runs: per
/// lookup it sets, the old value, primary key and key column of every row
/// it changes, on each shard in `shards`, locked. Each row then claims the
/// new value and releases its old one. The new value must be a literal of
/// the column's class (or `NULL`); of a lookup of several columns, the
/// parts the statement doesn't set are each row's own. A parent member
/// (no key column of its own) or a limited update is refused.
pub(super) fn update_releases(
    stmt: &Statement,
    rel: &Rel<'_>,
    entry: &TableEntry,
    shards: &[usize],
    limited: bool,
) -> R<Vec<Release>> {
    let Statement::Update(u) = stmt else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for l in &entry.lookups {
        let mut set: Vec<Option<&sqlparser::ast::Assignment>> = vec![None; l.columns.len()];
        for a in &u.assignments {
            let sqlparser::ast::AssignmentTarget::ColumnName(n) = &a.target else {
                continue;
            };
            let Some(i) = ident_parts(n).and_then(|p| {
                let last = p.last()?;
                l.columns
                    .iter()
                    .position(|c| last.value.eq_ignore_ascii_case(c))
            }) else {
                continue;
            };
            if set[i].replace(a).is_some() {
                return reject(Reason::Lookup, "a lookup column assigned twice");
            }
        }
        if set.iter().all(Option::is_none) {
            continue;
        }
        if limited {
            return reject(
                Reason::Lookup,
                "an UPDATE with ORDER BY or LIMIT of a lookup index's column",
            );
        }
        let Some(key) = entry.key() else {
            return reject(
                Reason::Lookup,
                "an UPDATE of a lookup index's column on a table that follows its parent",
            );
        };
        let mut value = Vec::with_capacity(set.len());
        let mut keep = Vec::with_capacity(set.len());
        for (i, a) in set.iter().enumerate() {
            let part = l
                .index
                .columns
                .get(i)
                .map(|c| c.class)
                .ok_or_else(|| Rejection::new(Reason::Lookup, "a column class"))?;
            let Some(a) = a else {
                value.push(KeyValueOwned::Null);
                keep.push(true);
                continue;
            };
            value.push(value_of(&a.value, l.classes[i], part).ok_or_else(|| {
                Rejection::new(
                    Reason::Lookup,
                    format!(
                        "an UPDATE setting a lookup index's column to a non-literal: {}",
                        a.value
                    ),
                )
            })?);
            keep.push(false);
        }
        if !keep.contains(&true) {
            keep.clear();
        }
        let mut items: Vec<String> = l
            .columns
            .iter()
            .zip(&l.index.columns)
            .map(|(n, c)| read_expr(n, Some(c.class)))
            .collect();
        items.extend(
            l.pk.iter()
                .zip(&l.index.owner_pk)
                .map(|(n, c)| read_expr(n, Some(c.class))),
        );
        items.push(key_read_expr(key, entry.group.key_class));
        let reads = locked_reads(
            vec![u.table.clone()],
            u.selection.clone(),
            &items,
            rel,
            entry,
            shards,
        )?;
        out.push(Release {
            lookup: Arc::clone(l),
            reads,
            set: Some(LookupSet {
                value,
                keep,
                group: Arc::clone(&entry.group),
            }),
        });
    }
    Ok(out)
}

/// How an update's read selects the key column: as the router hashes it.
fn key_read_expr(col: &str, class: KeyClass) -> String {
    let c = format!("`{}`", col.replace('`', "``"));
    match class {
        KeyClass::Str { .. } => format!("HEX(CONVERT({c} USING utf8mb4))"),
        KeyClass::Binary => format!("HEX({c})"),
        KeyClass::Int => c,
    }
}

/// A key column's value as an update's read returned it.
#[must_use]
pub(crate) fn decode_key(v: Option<&[u8]>, class: KeyClass) -> Option<KeyValue> {
    let text = std::str::from_utf8(v?).ok()?;
    Some(match class {
        KeyClass::Str { .. } | KeyClass::Binary => KeyValue::Bytes(unhex(text)?),
        KeyClass::Int => KeyValue::Int(text.parse().ok()?),
    })
}

/// `SELECT <items> FROM <from> WHERE <selection> FOR UPDATE` on each shard
/// in `shards`.
fn locked_reads(
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
    items: &[String],
    rel: &Rel<'_>,
    entry: &TableEntry,
    shards: &[usize],
) -> R<Vec<Target>> {
    let proj = projection(&items.join(", "))
        .ok_or_else(|| Rejection::new(Reason::Lookup, "a lookup read"))?;
    let Some(mut sel) = empty_select(from, selection) else {
        return reject(Reason::Lookup, "a lookup read");
    };
    sel.projection = proj;
    let q = sqlparser::ast::Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(sel))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    let select = Statement::Query(Box::new(q));
    Ok(shards
        .iter()
        .map(|&s| {
            let loc = &entry.group.shards[s];
            let mut m = HashMap::new();
            m.insert(rel.loc, loc.schema.clone());
            let mut by = HashMap::new();
            by.insert(
                (rel.default_schema.clone(), rel.table.clone()),
                loc.schema.clone(),
            );
            let mut sql = pass_sql(&select, None, &m, &by);
            sql.push_str(" FOR UPDATE");
            Target {
                cluster: loc.cluster.clone(),
                shard: loc.shard,
                sql,
                at: entry.group.at(s),
            }
        })
        .collect())
}

/// The reads a `DELETE` of `stmt` needs before it runs: per lookup, the
/// value and primary key of every row it removes, on each shard in
/// `shards` (indexes into the group), locked.
pub(super) fn delete_releases(
    stmt: &Statement,
    rel: &Rel<'_>,
    entry: &TableEntry,
    shards: &[usize],
    limited: bool,
) -> R<Vec<Release>> {
    if entry.lookups.is_empty() {
        return Ok(Vec::new());
    }
    if limited {
        return reject(
            Reason::Lookup,
            "a DELETE with ORDER BY or LIMIT on a table with a unique lookup",
        );
    }
    let Statement::Delete(d) = stmt else {
        return Ok(Vec::new());
    };
    let from = match &d.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v.clone(),
    };
    let mut out = Vec::new();
    for l in &entry.lookups {
        let mut items: Vec<String> = l
            .columns
            .iter()
            .zip(&l.index.columns)
            .map(|(n, c)| read_expr(n, Some(c.class)))
            .collect();
        items.extend(
            l.pk.iter()
                .zip(&l.index.owner_pk)
                .map(|(n, c)| read_expr(n, Some(c.class))),
        );
        let reads = locked_reads(
            from.clone(),
            d.selection.clone(),
            &items,
            rel,
            entry,
            shards,
        )?;
        out.push(Release {
            lookup: Arc::clone(l),
            reads,
            set: None,
        });
    }
    Ok(out)
}

/// How a release read selects a column: strings and binary values
/// hex-encoded, so neither the session's character set nor escaping
/// changes them.
fn read_expr(col: &str, part: Option<sprig_shard::KeyClass>) -> String {
    let c = format!("`{}`", col.replace('`', "``"));
    match part {
        Some(sprig_shard::KeyClass::String { .. }) => format!("HEX(CONVERT({c} USING utf8mb4))"),
        Some(sprig_shard::KeyClass::Binary) => format!("HEX({c})"),
        _ => c,
    }
}

fn projection(items: &str) -> Option<Vec<SelectItem>> {
    let Ok(Statement::Query(q)) = Parser::new(&MySqlDialect {})
        .try_with_sql(&format!("SELECT {items}"))
        .and_then(|mut p| p.parse_statement())
    else {
        return None;
    };
    let SetExpr::Select(s) = *q.body else {
        return None;
    };
    Some(s.projection)
}

/// A value a release read returned, back as the key value it encodes.
#[must_use]
pub(crate) fn decode_read(v: Option<&[u8]>, part: sprig_shard::KeyClass) -> Option<KeyValueOwned> {
    let Some(v) = v else {
        return Some(KeyValueOwned::Null);
    };
    let text = std::str::from_utf8(v).ok()?;
    Some(match part {
        sprig_shard::KeyClass::String { .. } => KeyValueOwned::Str {
            bytes: unhex(text)?,
            charset: Charset::Utf8mb4,
        },
        sprig_shard::KeyClass::Binary => KeyValueOwned::Bytes(unhex(text)?),
        sprig_shard::KeyClass::Numeric => match text.parse::<i128>() {
            Ok(i) => KeyValueOwned::Int(i),
            Err(_) => KeyValueOwned::Decimal(text.to_owned()),
        },
        _ => return None,
    })
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use sprig_common::ids::{ClusterId, GroupId, TenantId};
    use sprig_shard::lookup::{LookupOrigin, LookupState};
    use sprig_shard::{KeySpec, ShardMap};

    use super::*;
    use crate::planner::plan;
    use crate::topology::StaticTopology;
    use crate::topology::catalog::{CatalogGroup, CatalogLookup, CatalogTable, Membership, build};

    const DEF: &str = "t7_d1_g1_s1";
    const ACCOUNTS: &str = "CREATE TABLE `accounts` (\n  `aid` int(11) NOT NULL,\n  `email` varchar(40) COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `cid` int(11) DEFAULT NULL,\n  PRIMARY KEY (`aid`),\n  UNIQUE KEY `email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci";

    fn topo(state: LookupState, columns: &[&str]) -> StaticTopology {
        let c1 = ClusterId::new("c1").unwrap();
        let c2 = ClusterId::new("c2").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let map =
            ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, 4, &[c1.clone(), c2]).unwrap();
        let lmap = ShardMap::presplit(TenantId(7), GroupId(5), 0, 2, &[c1]).unwrap();
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let lookup = CatalogLookup {
            id: 11,
            owner: 70,
            index_name: "email".into(),
            columns: columns.iter().map(|c| (*c).to_owned()).collect(),
            unique: true,
            origin: LookupOrigin::UniqueKey,
            state,
            map: Some(Arc::new(lmap)),
        };
        build(
            7,
            &[g],
            &[CatalogTable {
                object: 0,
                db: 1,
                default_schema: DEF.into(),
                name: "accounts".into(),
                member: Membership::Sharded {
                    group: 3,
                    key_columns: Some(vec!["aid".into()]),
                    create_sql: Some(ACCOUNTS.into()),
                    lookups: vec![lookup],
                    fill_rk: false,
                    parent_fk: None,
                },
            }],
        )
    }

    fn p_with(t: &StaticTopology, sql: &str) -> Plan {
        let cx = PlanCtx {
            topo: t,
            default_schema: Some(DEF),
            session_cluster: "c1",
            max_route_values: 1024,
            lookup_batch: 256,
            ids: crate::planner::IdMode::default(),
        };
        plan(sql, &cx)
    }

    fn p(sql: &str) -> Plan {
        p_with(&topo(LookupState::Public, &["email"]), sql)
    }

    fn lookup_reason(sql: &str) {
        match p(sql) {
            Plan::Reject(r) => assert_eq!(r.reason, Reason::Lookup, "{sql}: {r:?}"),
            other => panic!("{sql}: expected a lookup rejection, got {other:?}"),
        }
    }

    #[test]
    fn the_catalog_lookup_becomes_a_lookup_ref() {
        let t = topo(LookupState::Public, &["email"]);
        let e = crate::topology::Topology::table(&t, DEF, "accounts").unwrap();
        let [l] = e.lookups.as_slice() else {
            panic!("{:?}", e.lookups)
        };
        assert_eq!(l.column, "email");
        assert_eq!(l.pk, ["aid"]);
        assert_eq!(l.ordinals, [Some(1), Some(0)]);
        assert!(matches!(
            l.index.columns[0].class,
            sprig_shard::KeyClass::String { .. }
        ));
        assert_eq!(
            l.index.columns[0].sql_type,
            "varchar(40) COLLATE utf8mb4_general_ci"
        );
        assert!(matches!(
            l.index.owner_pk[0].class,
            sprig_shard::KeyClass::Numeric
        ));
        assert!(!e.lookup_gap);
        // Two value columns: followed as one lookup of two parts.
        let t = topo(LookupState::Public, &["email", "cid"]);
        let e = crate::topology::Topology::table(&t, DEF, "accounts").unwrap();
        let [l] = e.lookups.as_slice() else {
            panic!("{:?}", e.lookups)
        };
        assert!(!e.lookup_gap);
        assert_eq!(l.columns, ["email", "cid"]);
        assert_eq!(l.classes, [l.class, KeyClass::Int]);
        assert_eq!(l.ordinals, [Some(1), Some(2), Some(0)]);
        assert_eq!(l.index.columns.len(), 2);
    }

    #[test]
    fn lookups_of_several_columns_route_claim_and_release() {
        let t = topo(LookupState::Public, &["email", "cid"]);
        let pt = |sql: &str| p_with(&t, sql);
        let s = |b: &str| KeyValueOwned::Str {
            bytes: b.as_bytes().to_vec(),
            charset: Charset::Utf8mb4,
        };
        // Every column named: routed, the combinations looked up.
        let Plan::ViaLookup(v) = pt("SELECT * FROM accounts WHERE cid = 3 AND email = 'a@x'")
        else {
            panic!()
        };
        assert_eq!(v.probe.values, [vec![s("a@x"), KeyValueOwned::Int(3)]]);
        let Plan::ViaLookup(v) =
            pt("SELECT aid FROM accounts WHERE email IN ('a@x', 'b@x') AND cid IN (1, 2)")
        else {
            panic!()
        };
        assert_eq!(v.probe.values.len(), 4);
        let Plan::ViaLookup(v) =
            pt("SELECT aid FROM accounts WHERE (cid, email) IN ((1, 'a@x'), (2, 'b@x'))")
        else {
            panic!()
        };
        assert_eq!(
            v.probe.values,
            [
                vec![s("a@x"), KeyValueOwned::Int(1)],
                vec![s("b@x"), KeyValueOwned::Int(2)]
            ]
        );
        assert!(matches!(
            pt("DELETE FROM accounts WHERE (email, cid) = ('a@x', 7)"),
            Plan::ViaLookup(_)
        ));
        // One column only: no lookup can name the rows.
        assert!(matches!(
            pt("SELECT * FROM accounts WHERE email = 'a@x'"),
            Plan::Read(_)
        ));
        // Inserts claim both parts.
        let Plan::Write(w) = pt("INSERT INTO accounts (cid, email, aid) VALUES (4, 'a@x', 10)")
        else {
            panic!()
        };
        assert_eq!(w.claims[0].value, [s("a@x"), KeyValueOwned::Int(4)]);
        assert_eq!(w.claims[0].owner_pk, [KeyValueOwned::Int(10)]);
        // An update of one part keeps each row's other part.
        let Plan::Write(w) = pt("UPDATE accounts SET cid = 9 WHERE aid = 10") else {
            panic!()
        };
        let set = w.releases[0].set.as_ref().unwrap();
        assert_eq!(set.keep, [true, false]);
        assert_eq!(
            set.value_for(&[s("old@x"), KeyValueOwned::Int(4)]),
            [s("old@x"), KeyValueOwned::Int(9)]
        );
        assert!(
            w.releases[0].reads[0].sql.starts_with(
                "SELECT HEX(CONVERT(`email` USING utf8mb4)), `cid`, `aid`, `aid` FROM"
            ),
            "{}",
            w.releases[0].reads[0].sql
        );
        // Deletes read both parts.
        let Plan::Write(w) = pt("DELETE FROM accounts WHERE aid = 10") else {
            panic!()
        };
        assert!(
            w.releases[0].reads[0]
                .sql
                .starts_with("SELECT HEX(CONVERT(`email` USING utf8mb4)), `cid`, `aid` FROM")
        );
    }

    #[test]
    fn equalities_on_a_public_lookup_narrow_reads_and_writes() {
        for sql in [
            "SELECT * FROM accounts WHERE email = 'a@x'",
            "SELECT cid FROM accounts WHERE 'a@x' = email AND cid > 3",
            "SELECT COUNT(*) FROM accounts WHERE email IN ('a@x', 'b@x')",
            "UPDATE accounts SET cid = 5 WHERE email = 'a@x'",
        ] {
            let Plan::ViaLookup(v) = p(sql) else {
                panic!("{sql}: {:?}", p(sql))
            };
            assert_eq!(v.probe.lookup.column, "email", "{sql}");
            assert!(matches!(v.probe.values[0][0], KeyValueOwned::Str { .. }));
            // The owners' hashes keep their shards only; no owner keeps one.
            let h = v.group.shards[2].lo + 1;
            match v.narrow(&[h]) {
                Plan::Read(r) => assert_eq!(r.targets.len(), 1),
                Plan::Write(w) => {
                    assert_eq!(w.targets.len(), 1);
                    assert_eq!(w.targets[0].shard, v.group.shards[2].shard);
                }
                other => panic!("{other:?}"),
            }
            match v.narrow(&[]) {
                Plan::Read(r) => assert_eq!(r.targets.len(), 1),
                Plan::Write(w) => assert_eq!(w.targets.len(), 1),
                other => panic!("{other:?}"),
            }
        }
        // Not narrowed: the key routes already, an expression, a join, a
        // lookup that isn't public yet.
        assert!(matches!(
            p("SELECT * FROM accounts WHERE aid = 4 AND email = 'a@x'"),
            Plan::Pass(_)
        ));
        assert!(matches!(
            p("SELECT * FROM accounts WHERE email = CONCAT('a', '@x')"),
            Plan::Read(_)
        ));
        assert!(matches!(
            p_with(
                &topo(LookupState::Backfilling, &["email"]),
                "SELECT * FROM accounts WHERE email = 'a@x'"
            ),
            Plan::Read(_)
        ));
    }

    #[test]
    fn inserts_claim_every_rows_value() {
        let Plan::Write(w) = p("INSERT INTO accounts (cid, email, aid) VALUES (1, 'a@x', 10)")
        else {
            panic!()
        };
        assert_eq!(w.targets.len(), 1, "one shard, still a write: it claims");
        let [c] = w.claims.as_slice() else {
            panic!("{:?}", w.claims)
        };
        assert_eq!(
            c.value,
            [KeyValueOwned::Str {
                bytes: b"a@x".to_vec(),
                charset: Charset::Utf8mb4
            }]
        );
        assert_eq!(c.owner_pk, [KeyValueOwned::Int(10)]);
        let t = topo(LookupState::Public, &["email"]);
        let g = &crate::topology::Topology::table(&t, DEF, "accounts")
            .unwrap()
            .group;
        assert_eq!(Some(c.owner_rk), g.hash_of(&KeyValue::Int(10)));
        // Without a column list, by position; a NULL value claims nothing
        // later (claim skips it) but is planned.
        let Plan::Write(w) =
            p("INSERT INTO accounts VALUES (1, 'a@x', 1), (2, NULL, 1), (3, 'c@x', 2)")
        else {
            panic!()
        };
        assert_eq!(w.claims.len(), 3);
        assert_eq!(w.claims[1].value, [KeyValueOwned::Null]);
        assert_eq!(w.rows, 3);
    }

    #[test]
    fn forms_that_would_skip_a_lookup_are_refused() {
        lookup_reason("INSERT IGNORE INTO accounts VALUES (1, 'a@x', 1)");
        // Upserts are refused already: the unique key lacks the key column.
        for sql in [
            "REPLACE INTO accounts VALUES (1, 'a@x', 1)",
            "INSERT INTO accounts VALUES (1, 'a@x', 1) ON DUPLICATE KEY UPDATE cid = 2",
        ] {
            assert!(matches!(p(sql), Plan::Reject(_)), "{sql}");
        }
        lookup_reason("INSERT INTO accounts SET aid = 1, email = 'a@x'");
        lookup_reason("INSERT INTO accounts (aid, cid) VALUES (1, 2)");
        lookup_reason("INSERT INTO accounts VALUES (1, CONCAT('a', '@x'), 1)");
        lookup_reason("UPDATE accounts SET email = CONCAT(email, 'x') WHERE aid = 1");
        lookup_reason("DELETE FROM accounts WHERE cid = 1 LIMIT 1");
    }

    #[test]
    fn deletes_read_what_they_release() {
        let Plan::Write(w) = p("DELETE FROM accounts WHERE aid = 10") else {
            panic!("{:?}", p("DELETE FROM accounts WHERE aid = 10"))
        };
        let [r] = w.releases.as_slice() else { panic!() };
        let [read] = r.reads.as_slice() else { panic!() };
        assert_eq!(read.shard, w.targets[0].shard);
        assert!(
            read.sql
                .starts_with("SELECT HEX(CONVERT(`email` USING utf8mb4)), `aid` FROM `t7_d1_g3_s")
                && read.sql.ends_with("WHERE aid = 10 FOR UPDATE"),
            "{}",
            read.sql
        );
        let Plan::ViaLookup(v) = p("DELETE FROM accounts WHERE email = 'a@x'") else {
            panic!()
        };
        let Plan::Write(w) = &v.plan else { panic!() };
        assert_eq!(w.releases[0].reads.len(), 4);
        let Plan::Write(n) = v.narrow(&[v.group.shards[1].lo]) else {
            panic!()
        };
        assert_eq!(n.targets.len(), 1);
        assert_eq!(n.releases[0].reads.len(), 1);
        assert_eq!(n.releases[0].reads[0].shard, n.targets[0].shard);
    }

    #[test]
    fn updates_of_the_lookup_column_claim_and_release() {
        let Plan::Write(w) = p("UPDATE accounts SET email = 'b@x', cid = 2 WHERE aid = 10") else {
            panic!(
                "{:?}",
                p("UPDATE accounts SET email = 'b@x' WHERE aid = 10")
            )
        };
        let [r] = w.releases.as_slice() else {
            panic!("{:?}", w.releases)
        };
        let set = r.set.as_ref().unwrap();
        assert_eq!(
            set.value,
            [KeyValueOwned::Str {
                bytes: b"b@x".to_vec(),
                charset: Charset::Utf8mb4
            }]
        );
        let [read] = r.reads.as_slice() else { panic!() };
        assert!(
            read.sql.starts_with(
                "SELECT HEX(CONVERT(`email` USING utf8mb4)), `aid`, `aid` FROM `t7_d1_g3_s"
            ) && read.sql.ends_with("WHERE aid = 10 FOR UPDATE"),
            "{}",
            read.sql
        );
        // Through the lookup when the WHERE names the old value; NULL
        // releases only.
        let Plan::ViaLookup(v) = p("UPDATE accounts SET email = NULL WHERE email = 'a@x'") else {
            panic!()
        };
        let Plan::Write(w) = &v.plan else { panic!() };
        assert_eq!(
            w.releases[0].set.as_ref().unwrap().value,
            [KeyValueOwned::Null]
        );
        // Updates that leave the column alone read nothing.
        let Plan::Pass(_) = p("UPDATE accounts SET cid = 3 WHERE aid = 10") else {
            panic!()
        };
        assert_eq!(
            decode_key(Some(b"42"), KeyClass::Int),
            Some(KeyValue::Int(42))
        );
    }

    #[test]
    fn release_reads_decode() {
        let s = sprig_shard::KeyClass::String {
            collation: sprig_shard::collation::collation_by_name("utf8mb4_general_ci")
                .unwrap()
                .id,
        };
        assert_eq!(
            decode_read(Some(b"6140"), s),
            Some(KeyValueOwned::Str {
                bytes: b"a@".to_vec(),
                charset: Charset::Utf8mb4
            })
        );
        assert_eq!(
            decode_read(Some(b"-12"), sprig_shard::KeyClass::Numeric),
            Some(KeyValueOwned::Int(-12))
        );
        assert_eq!(decode_read(None, s), Some(KeyValueOwned::Null));
        assert_eq!(decode_read(Some(b"6"), s), None);
    }

    /// Once a member is `filling`, every
    /// INSERT carries the row's routing hash in `_sprig_rk`; a positional
    /// INSERT gets the visible column list first.
    #[test]
    fn inserts_carry_the_routing_hash_once_filling() {
        let c1 = ClusterId::new("c1").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let map = ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, 1, &[c1]).unwrap();
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let table = |fill_rk| CatalogTable {
            object: 0,
            db: 1,
            default_schema: DEF.into(),
            name: "accounts".into(),
            member: Membership::Sharded {
                group: 3,
                key_columns: Some(vec!["aid".into()]),
                create_sql: Some(ACCOUNTS.into()),
                lookups: Vec::new(),
                fill_rk,
                parent_fk: None,
            },
        };
        let t = build(7, std::slice::from_ref(&g), &[table(true)]);
        let e = crate::topology::Topology::table(&t, DEF, "accounts").unwrap();
        let h = |k: i128| e.group.hash_of(&KeyValue::Int(k)).unwrap();
        let sql_of = |p: Plan| match p {
            Plan::Pass(t) => t.sql,
            other => panic!("expected one shard, got {other:?}"),
        };
        let s = sql_of(p_with(&t, "INSERT INTO accounts VALUES (10, 'a@x', 1)"));
        assert!(
            s.contains("(aid, email, cid, _sprig_rk)") && s.contains(&h(10).to_string()),
            "{s}"
        );
        let s = sql_of(p_with(
            &t,
            "INSERT INTO accounts (email, aid) VALUES ('a', 10), ('b', 11)",
        ));
        assert!(s.contains("_sprig_rk"), "{s}");
        assert!(
            s.contains(&h(10).to_string()) && s.contains(&h(11).to_string()),
            "{s}"
        );
        let s = sql_of(p_with(&t, "INSERT INTO accounts SET aid = 12, email = 'c'"));
        assert!(s.contains(&format!("_sprig_rk = {}", h(12))), "{s}");
        // A member still `absent` (or a statement naming the column) is left alone.
        let t0 = build(7, std::slice::from_ref(&g), &[table(false)]);
        let s = sql_of(p_with(&t0, "INSERT INTO accounts VALUES (10, 'a@x', 1)"));
        assert!(!s.contains("_sprig_rk"), "{s}");
        let s = sql_of(p_with(
            &t,
            "INSERT INTO accounts (aid, _sprig_rk) VALUES (10, 5)",
        ));
        assert!(s.contains("VALUES (10, 5)"), "{s}");
    }
}
