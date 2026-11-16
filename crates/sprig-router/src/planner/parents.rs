//! Inserts into parent members.
//!
//! A parent member (`order_items`) has no key column: its rows go where
//! their parent row (`orders`) is, through the declared foreign key, and
//! carry the parent's routing hash in `_sprig_rk`.
//!
//! - **One shard:** the statement goes to it unchanged; once the member
//!   fills `_sprig_rk`, each row gets `(SELECT _sprig_rk FROM parent WHERE
//!   … LIMIT 1)`, so MariaDB reads the parent in the same statement, and
//!   its own foreign-key check answers for a missing parent (1452, or a
//!   warning under `IGNORE`).
//! - **Several shards:** [`Plan::ViaParent`]. The executor reads where
//!   each distinct parent key is (through the parent's public unique
//!   lookup when it has one, else one `UNION ALL` statement per shard),
//!   then [`ViaParent::finish`] sends each row to its parent's shard.
//!   Rows whose parent is nowhere go to the first shard without a hash,
//!   so MariaDB's foreign-key check answers for them exactly as an
//!   unsharded table would; with `foreign_key_checks = 0` they go to the
//!   orphanage: the shard holding routing hash 0, with `_sprig_rk = 0`
//!   (the executor first marks the key's `orphanage` present, so every
//!   gateway's inserts into the parent take waiting rows along).
//!
//! **Adoption.** An insert into a table with such children
//! ([`adoption`]) also moves, in the same transaction, the waiting rows
//! whose foreign key names one of the new parent rows: on the hash-0 shard
//! itself an `UPDATE` of their `_sprig_rk`, elsewhere an
//! `INSERT … SELECT` from it on the same cluster and a `DELETE` there.
//!
//! **Joins.** Two more writes take waiting rows along, with statements
//! that find the parents themselves ([`join_adoption`]), on the shards of
//! the hash-0 shard's cluster: an `UPDATE` of a parent's referenced column
//! ([`update_adoption`]), and a row that goes to the orphanage, whose
//! parent may have been inserted on another connection since the parent
//! was looked for. Their statements are side statements
//! ([`crate::plan::SIDE_MARK`]): the rows they move don't count in the
//! client's answer.
//!
//! The foreign-key check on the shard is what makes this safe: a child row
//! can only commit next to its parent, so a parent that moved or vanished
//! between the read and the write fails the statement (a fence error or
//! 1452) rather than misplacing the row.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, Insert, ObjectName, ObjectNamePart, SetExpr,
    Statement, UnaryOperator, Value, visit_relations_mut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::{PlanCtx, R, Src, assigns_key, ident_parts, loc_of, one_shard_insert, shard_insert};
use crate::plan::{ParentHits, Plan, SIDE_MARK, Target, ViaParent, WriteKind, WritePlan};
use crate::reject::{Reason, Rejection, reject};
use crate::topology::{ParentLink, TableEntry};

const RK: &str = sprig_shard::rk::RK_COLUMN;

/// Plans an `INSERT` into the parent member `entry`, linked by `link`.
pub(super) fn plan_insert(
    stmt: &Statement,
    text_src: Option<&Src<'_>>,
    entry: &TableEntry,
    link: &ParentLink,
    default_schema: &str,
    cx: &PlanCtx<'_>,
) -> R<Plan> {
    let Statement::Insert(ins) = stmt else {
        return reject(Reason::Unsupported, "not an INSERT");
    };
    let sqlparser::ast::TableObject::TableName(name) = &ins.table else {
        return reject(Reason::Unsupported, "INSERT INTO a table function");
    };
    let loc = loc_of(name);
    if (ins.on.is_some() || ins.replace_into) && !link.upsert_safe {
        return reject(
            Reason::UpsertUnique,
            "an upsert on a table whose unique keys lack the foreign key its rows follow",
        );
    }
    if let Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments)) = &ins.on
        && assignments
            .iter()
            .any(|a| link.columns.iter().any(|c| assigns_key(&a.target, c)))
    {
        return reject(
            Reason::KeyChange,
            "ON DUPLICATE KEY UPDATE of the foreign key a parent member follows",
        );
    }
    super::lookups::check_gap(entry)?;
    if !entry.lookups.is_empty() {
        return reject(
            Reason::Lookup,
            "an INSERT into a parent member with a unique lookup",
        );
    }
    let one_shard = entry.group.shards.len() == 1;
    // Each row's foreign-key values.
    let keys: Vec<Vec<&Expr>> = match &ins.source {
        None => {
            // INSERT … SET: one row.
            let mut row = Vec::new();
            for c in &link.columns {
                let Some(a) = ins.assignments.iter().find(|a| assigns_key(&a.target, c)) else {
                    return reject(
                        Reason::InsertKey,
                        format!("an INSERT without {c}, the foreign key its rows follow"),
                    );
                };
                row.push(&a.value);
            }
            vec![row]
        }
        Some(src) => {
            let SetExpr::Values(values) = &*src.body else {
                return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
            };
            let width = if ins.columns.is_empty() {
                entry.columns.len()
            } else {
                ins.columns.len()
            };
            if width > 0 && values.rows.iter().any(|r| r.len() != width) {
                // A count mismatch: MariaDB's own 1136 says so, from any
                // shard.
                return Ok(Plan::Pass(one_shard_insert(
                    stmt,
                    text_src,
                    entry,
                    0,
                    loc,
                    default_schema,
                )));
            }
            let mut at = Vec::new();
            for (i, c) in link.columns.iter().enumerate() {
                let p = if ins.columns.is_empty() {
                    link.ordinals.get(i).copied().flatten()
                } else {
                    ins.columns.iter().position(|n| named(n, c))
                };
                let Some(p) = p else {
                    return reject(
                        Reason::InsertKey,
                        format!("an INSERT without {c}, the foreign key its rows follow"),
                    );
                };
                at.push(p);
            }
            let mut rows = Vec::with_capacity(values.rows.len());
            for r in &values.rows {
                let mut row = Vec::with_capacity(at.len());
                for p in &at {
                    let Some(e) = r.get(*p) else {
                        return reject(Reason::InsertKey, "a row without its foreign key");
                    };
                    row.push(e);
                }
                rows.push(row);
            }
            rows
        }
    };
    // Each distinct parent key, as a condition on the parent.
    let mut probes: Vec<String> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut row_probe = Vec::with_capacity(keys.len());
    let mut first_of: Vec<Vec<&Expr>> = Vec::new();
    for row in &keys {
        let mut conds = Vec::with_capacity(row.len());
        for (e, pc) in row.iter().zip(&link.parent_columns) {
            conds.push(format!("{} = {}", quote(pc), literal(e)?));
        }
        let cond = conds.join(" AND ");
        let next = probes.len();
        let i = *index.entry(cond.clone()).or_insert(next);
        if i == next {
            probes.push(cond);
            first_of.push(row.clone());
        }
        row_probe.push(i);
    }
    if one_shard {
        if !entry.fill_rk || names_rk(ins) {
            return Ok(Plan::Pass(one_shard_insert(
                stmt,
                text_src,
                entry,
                0,
                loc,
                default_schema,
            )));
        }
        let schema = entry.group.shards[0].schema.clone();
        let rks: Vec<Expr> = row_probe
            .iter()
            .map(|&p| rk_subquery(&schema, &link.table, &probes[p]))
            .collect::<R<_>>()?;
        let filled = with_rk(ins, entry, &rks)?;
        return Ok(Plan::Pass(one_shard_insert(
            &filled,
            None,
            entry,
            0,
            loc,
            default_schema,
        )));
    }
    if ins.returning.is_some() {
        return reject(
            Reason::Unsupported,
            "INSERT … RETURNING into a parent member over several shards",
        );
    }
    // The statement finish() splits: with the column list written out
    // when `_sprig_rk` will be added.
    let mut planned = ins.clone();
    let fill = entry.fill_rk && !names_rk(ins);
    if fill && ins.source.is_some() && ins.columns.is_empty() {
        if entry.columns.is_empty() {
            return reject(
                Reason::InsertKey,
                "an INSERT without a column list into a table whose columns are unknown",
            );
        }
        planned.columns = entry.columns.iter().map(|c| column(c)).collect();
    }
    let key_literals: Vec<String> = if link.columns.len() == 1 {
        first_of
            .iter()
            .filter_map(|r| r.first().map(|e| literal(e)))
            .collect::<R<_>>()?
    } else {
        Vec::new()
    };
    let lookup = link.lookup.as_ref().and_then(|l| {
        let part = l.index.columns.first()?.class;
        let values = first_of
            .iter()
            .map(|row| match row.as_slice() {
                [e] => super::lookups::value_of(e, l.class, part),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some((Arc::clone(l), values))
    });
    Ok(Plan::ViaParent(Box::new(ViaParent {
        stmt: Statement::Insert(planned),
        table: entry.name.clone(),
        default_schema: default_schema.to_owned(),
        parent: link.table.clone(),
        group: Arc::clone(&entry.group),
        probes,
        lookup,
        row_probe,
        fill_rk: fill,
        batch: cx.lookup_batch.max(1),
        fk: link.fk.clone(),
        fk_columns: (link.columns.clone(), link.parent_columns.clone()),
        orphans: link.orphans,
        orphanable: link.orphanable,
        key_literals,
        columns: entry.columns.clone(),
        parent_unique: link.parent_unique,
    })))
}

impl ViaParent {
    /// The plan once the parents were found: `hits[p]` is where probe `p`'s
    /// parent row is; `fk_checks` is the session's `foreign_key_checks`.
    ///
    /// # Errors
    /// 39420 (`insert_key`) for a parent key held on several shards, a
    /// parent row without its routing hash, or a missing parent with
    /// `foreign_key_checks = 0`.
    pub fn finish(&self, hits: &[ParentHits], fk_checks: bool) -> Result<Plan, Rejection> {
        let Statement::Insert(ins) = &self.stmt else {
            return reject(Reason::Unsupported, "not an INSERT");
        };
        let sqlparser::ast::TableObject::TableName(name) = &ins.table else {
            return reject(Reason::Unsupported, "INSERT INTO a table function");
        };
        let loc = loc_of(name);
        // Per row: its shard and its hash.
        let mut placed: Vec<(usize, Option<u64>)> = Vec::with_capacity(self.row_probe.len());
        // Probes whose rows wait in the orphanage.
        let mut orphaned: Vec<usize> = Vec::new();
        for &p in &self.row_probe {
            let mut at: Option<(usize, Option<u64>)> = None;
            for &(s, rk) in hits.get(p).map_or(&[][..], Vec::as_slice) {
                match at {
                    None => at = Some((s, rk)),
                    Some((s0, _)) if s0 != s => {
                        return reject(
                            Reason::InsertKey,
                            "a parent key held by rows on several shards",
                        );
                    }
                    Some(_) => {}
                }
            }
            match at {
                Some((s, rk)) => {
                    if self.fill_rk && rk.is_none() {
                        return reject(
                            Reason::InsertKey,
                            "a parent row without its routing hash (_sprig_rk)",
                        );
                    }
                    placed.push((s, rk));
                }
                // MariaDB's foreign-key check answers for the row there.
                None if fk_checks => placed.push((0, None)),
                // The orphanage: the row waits where hash 0 lives, until a
                // parent's insert takes it to the parent's shard.
                None if self.orphans && self.orphanable && self.fill_rk => {
                    let h = sprig_shard::fk::ORPHAN_RK;
                    placed.push((self.group.route(h), Some(h)));
                    if !orphaned.contains(&p) {
                        orphaned.push(p);
                    }
                }
                None => {
                    return reject(
                        Reason::InsertKey,
                        "a row whose parent doesn't exist, with foreign_key_checks = 0 \
                         (the foreign key's orphanage couldn't be opened)",
                    );
                }
            }
        }
        let mut by_shard: Vec<(usize, Vec<usize>)> = Vec::new();
        for (r, (s, _)) in placed.iter().enumerate() {
            match by_shard.iter_mut().find(|(x, _)| x == s) {
                Some((_, rows)) => rows.push(r),
                None => by_shard.push((*s, vec![r])),
            }
        }
        let number = |h: Option<u64>| match h {
            Some(h) => Expr::value(Value::Number(h.to_string(), false)),
            None => Expr::value(Value::Null),
        };
        let mut targets = Vec::with_capacity(by_shard.len());
        for (shard, rows) in &by_shard {
            let mut out = ins.clone();
            match &mut out.source {
                Some(src) => {
                    let SetExpr::Values(values) = &mut *src.body else {
                        return reject(Reason::InsertSelect, "INSERT … SELECT");
                    };
                    let all = std::mem::take(&mut values.rows);
                    values.rows = rows
                        .iter()
                        .filter_map(|&r| {
                            let mut row = all.get(r)?.clone();
                            if self.fill_rk {
                                row.push(number(placed[r].1));
                            }
                            Some(row)
                        })
                        .collect();
                    if self.fill_rk {
                        out.columns.push(column(RK));
                    }
                }
                None if self.fill_rk => out.assignments.push(Assignment {
                    target: AssignmentTarget::ColumnName(column(RK)),
                    value: number(placed.first().and_then(|p| p.1)),
                }),
                None => {}
            }
            targets.push(shard_insert(
                &Statement::Insert(out),
                None,
                &self.group,
                &self.table,
                *shard,
                loc,
                &self.default_schema,
            ));
        }
        // A parent inserted on another connection since it was looked
        // for: its rows are taken along now, after they are written.
        if !orphaned.is_empty()
            && self.parent_unique
            && self.key_literals.len() == self.probes.len()
            && let ([column], [parent_column]) =
                (self.fk_columns.0.as_slice(), self.fk_columns.1.as_slice())
        {
            let values: Vec<String> = orphaned
                .iter()
                .filter_map(|p| self.key_literals.get(*p).cloned())
                .collect();
            let child = ident_parts(name)
                .and_then(|p| p.last().map(|i| i.value.clone()))
                .unwrap_or_else(|| self.table.clone());
            let all: Vec<usize> = (0..self.group.shards.len()).collect();
            targets.extend(join_adoption(
                &self.group,
                &Waiting {
                    child: &child,
                    column,
                    columns: &self.columns,
                    parent: &self.parent,
                    parent_column,
                },
                &all,
                Some(&values),
            ));
        }
        if targets.len() == 1 {
            return Ok(Plan::Pass(targets.remove(0)));
        }
        Ok(Plan::Write(WritePlan {
            kind: WriteKind::Insert,
            targets,
            rows: self.row_probe.len() as u64,
            claims: Vec::new(),
            releases: Vec::new(),
            fk: None,
            limited: None,
        }))
    }
}

fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn column(n: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new(n))])
}

fn named(c: &ObjectName, n: &str) -> bool {
    ident_parts(c)
        .and_then(|p| p.last().map(|i| i.value.eq_ignore_ascii_case(n)))
        .unwrap_or(false)
}

/// Whether the statement writes `_sprig_rk` itself (left as written).
fn names_rk(ins: &Insert) -> bool {
    ins.columns.iter().any(|c| named(c, RK))
        || ins.assignments.iter().any(|a| assigns_key(&a.target, RK))
}

/// A foreign-key value as SQL text: a literal only (a number, a string, a
/// hex or charset-introduced string, a signed number). `NULL` names no
/// parent, so the row has no shard.
/// A parent row's referenced value for adoption: its literal text, `None`
/// for `NULL` (no child waits for it).
///
/// # Errors
/// 39420 (`insert_key`) for a value that isn't a literal while children
/// wait for parents.
fn adopt_literal(e: &Expr) -> R<Option<String>> {
    match e {
        Expr::Value(v) if matches!(v.value, Value::Null) => Ok(None),
        Expr::Nested(inner) => adopt_literal(inner),
        _ => literal(e).map(Some).map_err(|_| {
            Rejection::new(
                Reason::InsertKey,
                format!(
                    "a parent row whose referenced value isn't a literal ({e}) while rows wait \
                     for their parent"
                ),
            )
        }),
    }
}

/// The statements that take the rows waiting for an inserted parent row
/// to its shard: `rows` are the new parent rows (their shard and routing
/// hash); `value(row, column)` is a row's value of `column`. Per child in
/// `entry.orphan_children` and per shard: on the hash-0 shard itself an
/// `UPDATE` of `_sprig_rk`; on another shard of the same cluster an
/// `INSERT … SELECT` from the hash-0 shard, then a `DELETE` there.
///
/// # Errors
/// 39420 (`cross_cluster`) when the parent's shard and the hash-0 shard
/// are on different clusters; (`insert_key`) for a value that isn't a
/// literal, or a child whose columns are unknown.
pub(super) fn adoption(
    entry: &TableEntry,
    rows: &[(usize, u64)],
    value: &dyn Fn(usize, &str) -> Option<Expr>,
) -> R<Vec<crate::plan::Target>> {
    use crate::plan::Target;
    let mut out = Vec::new();
    if entry.orphan_children.is_empty() {
        return Ok(out);
    }
    let q = |s: &str| format!("`{}`", s.replace('`', "``"));
    let s0 = entry.group.route(sprig_shard::fk::ORPHAN_RK);
    let Some(loc0) = entry.group.shards.get(s0) else {
        return Ok(out);
    };
    for c in &entry.orphan_children {
        // Per shard: (literal, hash) of each new parent row.
        let mut by_shard: Vec<(usize, Vec<(String, u64)>)> = Vec::new();
        for (r, &(shard, h)) in rows.iter().enumerate() {
            let Some(v) = value(r, &c.parent_column) else {
                return reject(
                    Reason::InsertKey,
                    format!(
                        "an INSERT without {}, which rows of {} wait for",
                        c.parent_column, c.table
                    ),
                );
            };
            let Some(lit) = adopt_literal(&v)? else {
                continue;
            };
            match by_shard.iter_mut().find(|(s, _)| *s == shard) {
                Some((_, l)) => l.push((lit, h)),
                None => by_shard.push((shard, vec![(lit, h)])),
            }
        }
        let fk = q(&c.column);
        let rk = q(RK);
        for (shard, list) in by_shard {
            let Some(loc) = entry.group.shards.get(shard) else {
                continue;
            };
            let in_list = list
                .iter()
                .map(|(l, _)| l.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let case = format!(
                "CASE {fk} {} END",
                list.iter()
                    .map(|(l, h)| format!("WHEN {l} THEN {h}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            let waiting = format!(
                "{fk} IN ({in_list}) AND {rk} = {}",
                sprig_shard::fk::ORPHAN_RK
            );
            let from = format!("{}.{}", q(&loc0.schema), q(&c.table));
            let target = |i: usize, l: &crate::topology::ShardLoc, sql: String| Target {
                cluster: l.cluster.clone(),
                shard: l.shard,
                sql: format!("{SIDE_MARK}{sql}"),
                at: entry.group.at(i),
            };
            if shard == s0 {
                out.push(target(
                    s0,
                    loc0,
                    format!("UPDATE {from} SET {rk} = {case} WHERE {waiting}"),
                ));
                continue;
            }
            if loc.cluster != loc0.cluster {
                return reject(
                    Reason::CrossCluster,
                    format!(
                        "a parent row on another cluster than the rows of {} waiting for it",
                        c.table
                    ),
                );
            }
            if c.columns.is_empty() {
                return reject(
                    Reason::InsertKey,
                    format!(
                        "rows of {} wait for this parent, but its columns are unknown",
                        c.table
                    ),
                );
            }
            let cols = c
                .columns
                .iter()
                .map(|x| q(x))
                .collect::<Vec<_>>()
                .join(", ");
            out.push(target(
                shard,
                loc,
                format!(
                    "INSERT INTO {}.{} ({cols}, {rk}) SELECT {cols}, {case} FROM {from} WHERE {waiting}",
                    q(&loc.schema),
                    q(&c.table)
                ),
            ));
            out.push(target(
                s0,
                loc0,
                format!("DELETE FROM {from} WHERE {waiting}"),
            ));
        }
    }
    Ok(out)
}

/// A table whose rows may wait in the orphanage, and its parent, as
/// [`join_adoption`] names them.
pub(super) struct Waiting<'a> {
    /// The child table.
    pub(super) child: &'a str,
    /// Its foreign-key column.
    pub(super) column: &'a str,
    /// Its visible columns, in order.
    pub(super) columns: &'a [String],
    /// The parent table.
    pub(super) parent: &'a str,
    /// The parent's referenced column.
    pub(super) parent_column: &'a str,
}

/// Side statements that take the rows of `w.child` waiting on the hash-0
/// shard (`_sprig_rk = 0`, and a key among `values` when given) to the
/// shard among `shards` whose copy of the parent holds their parent row,
/// with its `_sprig_rk`: on the hash-0 shard an `UPDATE … JOIN`, on each
/// other shard an `INSERT … SELECT … JOIN`, then one `DELETE` of what was
/// copied. Shards on other clusters are left out (their rows keep
/// waiting). Run in one transaction, under `REPEATABLE READ`, the copy's
/// locking read keeps the parents and the waiting rows from changing
/// before the `DELETE`, so it removes exactly what was copied. The
/// parent's referenced column must hold a unique key, or one row would be
/// copied once per parent row.
pub(super) fn join_adoption(
    group: &crate::topology::Group,
    w: &Waiting<'_>,
    shards: &[usize],
    values: Option<&[String]>,
) -> Vec<Target> {
    let mut out = Vec::new();
    if values.is_some_and(<[String]>::is_empty) {
        return out;
    }
    let s0 = group.route(sprig_shard::fk::ORPHAN_RK);
    let Some(loc0) = group.shards.get(s0) else {
        return out;
    };
    let rk = quote(RK);
    let (fk, pc) = (quote(w.column), quote(w.parent_column));
    let child0 = format!("{}.{}", quote(&loc0.schema), quote(w.child));
    let only = values.map_or_else(String::new, |v| {
        format!(" AND c.{fk} IN ({})", v.join(", "))
    });
    let mut here = false;
    let mut away: Vec<usize> = Vec::new();
    for &s in shards {
        let Some(loc) = group.shards.get(s) else {
            continue;
        };
        if loc.cluster != loc0.cluster {
            continue;
        }
        if s == s0 {
            here = true;
        } else if !away.contains(&s) {
            away.push(s);
        }
    }
    let side = |i: usize, sql: String| {
        let l = &group.shards[i];
        Target {
            cluster: l.cluster.clone(),
            shard: l.shard,
            sql: format!("{SIDE_MARK}{sql}"),
            at: group.at(i),
        }
    };
    if here {
        out.push(side(
            s0,
            format!(
                "UPDATE {child0} c JOIN {}.{} p ON p.{pc} = c.{fk} SET c.{rk} = p.{rk} \
                 WHERE c.{rk} = 0 AND p.{rk} IS NOT NULL{only}",
                quote(&loc0.schema),
                quote(w.parent)
            ),
        ));
    }
    if w.columns.is_empty() || away.is_empty() {
        return out;
    }
    let cols: Vec<String> = w.columns.iter().map(|c| quote(c)).collect();
    let list = cols.join(", ");
    let from_c = cols
        .iter()
        .map(|c| format!("c.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut exists = Vec::with_capacity(away.len());
    for s in away {
        let schema = quote(&group.shards[s].schema);
        let parent = format!("{schema}.{}", quote(w.parent));
        out.push(side(
            s,
            format!(
                "INSERT INTO {schema}.{} ({list}, {rk}) SELECT {from_c}, p.{rk} FROM {child0} c \
                 JOIN {parent} p ON p.{pc} = c.{fk} WHERE c.{rk} = 0 AND p.{rk} IS NOT NULL{only}",
                quote(w.child)
            ),
        ));
        exists.push(format!(
            "EXISTS (SELECT 1 FROM {parent} p WHERE p.{pc} = c.{fk} AND p.{rk} IS NOT NULL)"
        ));
    }
    out.push(side(
        s0,
        format!(
            "DELETE c FROM {child0} c WHERE c.{rk} = 0{only} AND ({})",
            exists.join(" OR ")
        ),
    ));
    out
}

/// The side statements of an `UPDATE` of parent `entry` (named `parent`
/// in the statement) on `shards`: for each child whose rows may wait for
/// it and whose referenced column the statement sets, [`join_adoption`],
/// narrowed to the new value when it is a literal.
pub(super) fn update_adoption(
    entry: &TableEntry,
    parent: &str,
    assignments: &[&Assignment],
    shards: &[usize],
) -> Vec<Target> {
    let mut out = Vec::new();
    for c in &entry.orphan_children {
        if !c.parent_unique {
            continue;
        }
        let Some(a) = assignments
            .iter()
            .find(|a| assigns_key(&a.target, &c.parent_column))
        else {
            continue;
        };
        let values = match adopt_literal(&a.value) {
            // NULL: no row waits for it.
            Ok(None) => continue,
            Ok(Some(l)) => Some(vec![l]),
            // An expression: every waiting row is looked at.
            Err(_) => None,
        };
        out.extend(join_adoption(
            &entry.group,
            &Waiting {
                child: &c.table,
                column: &c.column,
                columns: &c.columns,
                parent,
                parent_column: &c.parent_column,
            },
            shards,
            values.as_deref(),
        ));
    }
    out
}

fn literal(e: &Expr) -> R<String> {
    let ok = match e {
        Expr::Value(v) => match &v.value {
            Value::Null => {
                return reject(
                    Reason::InsertKey,
                    "a NULL foreign key in a table whose rows follow their parent",
                );
            }
            Value::Placeholder(_) => false,
            _ => true,
        },
        Expr::Prefixed { value, .. } => matches!(&**value, Expr::Value(_)),
        Expr::UnaryOp {
            op: UnaryOperator::Minus | UnaryOperator::Plus,
            expr,
        } => matches!(&**expr, Expr::Value(v) if matches!(v.value, Value::Number(..))),
        Expr::Nested(inner) => return literal(inner),
        _ => false,
    };
    if ok {
        Ok(e.to_string())
    } else {
        reject(
            Reason::InsertKey,
            format!("a foreign key that isn't a literal: {e}"),
        )
    }
}

/// `(SELECT _sprig_rk FROM schema.parent WHERE cond LIMIT 1)`, its names
/// without parser positions (the renderer moves names by position).
fn rk_subquery(schema: &str, parent: &str, cond: &str) -> R<Expr> {
    let text = format!(
        "(SELECT {} FROM {}.{} WHERE {cond} LIMIT 1)",
        quote(RK),
        quote(schema),
        quote(parent)
    );
    let mut e = Parser::new(&MySqlDialect {})
        .try_with_sql(&text)
        .and_then(|mut p| p.parse_expr())
        .map_err(|_| Rejection::new(Reason::InsertKey, "a parent lookup"))?;
    let _ = visit_relations_mut(&mut e, |n| {
        let parts: Vec<ObjectNamePart> = ident_parts(n)
            .unwrap_or_default()
            .into_iter()
            .map(|i| ObjectNamePart::Identifier(Ident::with_quote('`', i.value.clone())))
            .collect();
        *n = ObjectName(parts);
        ControlFlow::<()>::Continue(())
    });
    Ok(e)
}

/// `ins` with `_sprig_rk` set to `rks[r]` in each row `r` (the column list
/// written out first when the statement has none).
fn with_rk(ins: &Insert, entry: &TableEntry, rks: &[Expr]) -> R<Statement> {
    let mut out = ins.clone();
    match &mut out.source {
        None => {
            let Some(v) = rks.first() else {
                return reject(Reason::InsertKey, "a row");
            };
            out.assignments.push(Assignment {
                target: AssignmentTarget::ColumnName(column(RK)),
                value: v.clone(),
            });
        }
        Some(src) => {
            let SetExpr::Values(values) = &mut *src.body else {
                return reject(Reason::InsertSelect, "INSERT … SELECT into a sharded table");
            };
            if out.columns.is_empty() {
                if entry.columns.is_empty() {
                    return reject(
                        Reason::InsertKey,
                        "an INSERT without a column list into a table whose columns are unknown",
                    );
                }
                out.columns = entry.columns.iter().map(|c| column(c)).collect();
            }
            out.columns.push(column(RK));
            for (row, v) in values.rows.iter_mut().zip(rks) {
                row.push(v.clone());
            }
        }
    }
    Ok(Statement::Insert(out))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use sprig_common::Result;
    use sprig_common::ids::{ClusterId, GroupId, TenantId};
    use sprig_shard::{KeySpec, ShardMap};

    use crate::exec::{Budget, Output, ShardBackend, ShardOk, ShardOutcome, ShardRows, execute};
    use crate::plan::{Plan, Target};
    use crate::planner::{PlanCtx, plan};
    use crate::reject::Reason;
    use crate::topology::catalog::{CatalogGroup, CatalogTable, Membership, TableShape, build};
    use crate::topology::{KeyValue, StaticTopology, Topology};

    const DEF: &str = "t7_d1_g1_s1";
    const INVOICES: &str = "CREATE TABLE `invoices` (\n  `iid` int(11) NOT NULL,\n  `cid` int(11) NOT NULL,\n  `total` decimal(10,2) DEFAULT NULL,\n  PRIMARY KEY (`iid`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    const LINES: &str = "CREATE TABLE `invoice_lines` (\n  `iid` int(11) NOT NULL,\n  `line_no` int(11) NOT NULL,\n  `sku` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`iid`,`line_no`),\n  KEY `fk_il_inv` (`iid`),\n  CONSTRAINT `fk_il_inv` FOREIGN KEY (`iid`) REFERENCES `invoices` (`iid`) ON DELETE CASCADE\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    fn topo(shards: u32, fill_rk: bool, parent_group: u32) -> StaticTopology {
        let c1 = ClusterId::new("c1").unwrap();
        let c2 = ClusterId::new("c2").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let group = |id: u32| CatalogGroup {
            id,
            spec: spec.clone(),
            map: Arc::new(
                ShardMap::presplit(
                    TenantId(7),
                    GroupId(id),
                    spec.digest,
                    shards,
                    &[c1.clone(), c2.clone()],
                )
                .unwrap(),
            ),
        };
        let table =
            |name: &str, group: u32, key: Option<&str>, sql: &str, fk: Option<&str>| CatalogTable {
                object: 0,
                db: 1,
                default_schema: DEF.into(),
                name: name.into(),
                member: Membership::Sharded {
                    group,
                    key_columns: key.map(|k| vec![k.to_owned()]),
                    create_sql: Some(sql.into()),
                    lookups: Vec::new(),
                    fill_rk,
                    parent_fk: fk.map(str::to_owned),
                },
            };
        build(
            7,
            &[group(3), group(4)],
            &[
                table("invoices", parent_group, Some("cid"), INVOICES, None),
                table("invoice_lines", 3, None, LINES, Some("fk_il_inv")),
            ],
        )
    }

    fn p_with(t: &StaticTopology, sql: &str) -> Plan {
        let cx = PlanCtx {
            topo: t,
            default_schema: Some(DEF),
            session_cluster: "c1",
            max_route_values: 1024,
            lookup_batch: 2,
            ids: crate::planner::IdMode::default(),
        };
        plan(sql, &cx)
    }

    fn rejected(t: &StaticTopology, sql: &str) -> Reason {
        match p_with(t, sql) {
            Plan::Reject(r) => r.reason,
            other => panic!("{sql}: expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn foreign_keys_are_read_from_show_create_table() {
        let s = TableShape::parse(LINES);
        let [f] = s.foreign_keys.as_slice() else {
            panic!("{:?}", s.foreign_keys)
        };
        assert_eq!(f.name, "fk_il_inv");
        assert_eq!(f.columns, ["iid"]);
        assert_eq!(f.ref_table, "invoices");
        assert_eq!(f.ref_columns, ["iid"]);
        let q = TableShape::parse(
            "CREATE TABLE `x` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL,\n  CONSTRAINT `two` FOREIGN KEY (`a`, `b`) REFERENCES `other_db`.`p` (`pa`, `pb`)\n) ENGINE=InnoDB",
        );
        assert_eq!(q.foreign_keys[0].ref_table, "p");
        assert_eq!(q.foreign_keys[0].columns, ["a", "b"]);
        assert_eq!(q.foreign_keys[0].ref_columns, ["pa", "pb"]);
    }

    #[test]
    fn the_link_holds_only_inside_one_group() {
        let t = topo(4, false, 3);
        let e = t.table(DEF, "invoice_lines").unwrap();
        let l = e.parent.as_ref().unwrap();
        assert_eq!(l.table, "invoices");
        assert_eq!(l.columns, ["iid"]);
        assert_eq!(l.ordinals, [Some(0)]);
        assert!(l.upsert_safe);
        // A parent in another group: no link, inserts refused as before.
        let t = topo(4, false, 4);
        assert!(t.table(DEF, "invoice_lines").unwrap().parent.is_none());
        assert_eq!(
            rejected(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')"),
            Reason::InsertKey
        );
    }

    #[test]
    fn one_shard_passes_and_fills_rk_from_the_parent_in_the_statement() {
        let t = topo(1, false, 3);
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')") else {
            panic!()
        };
        assert_eq!(
            x.sql,
            "INSERT INTO `t7_d1_g3_s1`.invoice_lines VALUES (1, 1, 'a')"
        );
        let t = topo(1, true, 3);
        let Plan::Pass(x) = p_with(
            &t,
            "INSERT IGNORE INTO invoice_lines (line_no, iid) VALUES (1, 7), (2, '8')",
        ) else {
            panic!()
        };
        assert!(
            x.sql.contains(
                "(line_no, iid, _sprig_rk) VALUES (1, 7, (SELECT `_sprig_rk` FROM `t7_d1_g3_s1`.`invoices` WHERE `iid` = 7 LIMIT 1))"
            ),
            "{}",
            x.sql
        );
        assert!(x.sql.contains("WHERE `iid` = '8' LIMIT 1))"), "{}", x.sql);
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines VALUES (1, 1, 'a')") else {
            panic!()
        };
        assert!(
            x.sql.contains("(iid, line_no, sku, _sprig_rk)"),
            "{}",
            x.sql
        );
        let Plan::Pass(x) = p_with(&t, "INSERT INTO invoice_lines SET iid = 3, line_no = 1") else {
            panic!()
        };
        assert!(x.sql.contains("_sprig_rk = (SELECT"), "{}", x.sql);
    }

    #[test]
    fn refusals() {
        let t = topo(4, true, 3);
        for (sql, r) in [
            (
                "INSERT INTO invoice_lines VALUES (NULL, 1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1 + 1, 1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines (line_no, sku) VALUES (1, 'a')",
                Reason::InsertKey,
            ),
            (
                "INSERT INTO invoice_lines SELECT * FROM invoice_lines",
                Reason::InsertSelect,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') ON DUPLICATE KEY UPDATE iid = 2",
                Reason::KeyChange,
            ),
            (
                "UPDATE invoice_lines SET iid = 2 WHERE line_no = 1",
                Reason::KeyChange,
            ),
            (
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') RETURNING sku",
                Reason::Unsupported,
            ),
        ] {
            assert_eq!(rejected(&t, sql), r, "{sql}");
        }
        // Upserts are exact: every unique key holds the foreign key.
        assert!(matches!(
            p_with(
                &t,
                "INSERT INTO invoice_lines VALUES (1, 1, 'a') ON DUPLICATE KEY UPDATE sku = 'b'"
            ),
            Plan::ViaParent(_)
        ));
        // One shard, no _sprig_rk yet: changing the key is harmless.
        let t1 = topo(1, false, 3);
        assert!(matches!(
            p_with(&t1, "UPDATE invoice_lines SET iid = 2 WHERE line_no = 1"),
            Plan::Pass(_)
        ));
    }

    #[test]
    fn a_join_to_the_parent_on_the_foreign_key_is_co_located() {
        let t = topo(4, true, 3);
        let x = match p_with(
            &t,
            "SELECT i.iid, l.line_no FROM invoices i JOIN invoice_lines l ON l.iid = i.iid WHERE i.cid = 42",
        ) {
            Plan::Pass(x) => x,
            other => panic!("{other:?}"),
        };
        assert!(
            x.sql.contains(&format!("`t7_d1_g3_s{}`.invoices", x.shard))
                && x.sql
                    .contains(&format!("`t7_d1_g3_s{}`.invoice_lines", x.shard)),
            "{}",
            x.sql
        );
        match p_with(
            &t,
            "SELECT i.cid, COUNT(*) FROM invoice_lines l LEFT JOIN invoices i ON i.iid = l.iid GROUP BY i.cid",
        ) {
            Plan::Read(r) => assert_eq!(r.targets.len(), 4),
            other => panic!("{other:?}"),
        }
        // Another column is no parent edge: not planned as co-located.
        assert!(matches!(
            p_with(
                &t,
                "SELECT * FROM invoices i JOIN invoice_lines l ON l.line_no = i.iid"
            ),
            Plan::Reject(_) | Plan::LookupJoin(_) | Plan::Gather(_)
        ));
    }

    /// Answers parent lookups from `parents` (iid → cid) and records the
    /// rest.
    struct Fake {
        t: StaticTopology,
        parents: HashMap<i64, i64>,
        fk_checks: bool,
        log: Vec<(u32, String)>,
        /// What `open_orphanage` answers, and how often it was asked.
        opens: bool,
        opened: usize,
    }

    #[async_trait::async_trait]
    impl ShardBackend for Fake {
        async fn run(
            &mut self,
            target: &Target,
            sql: &str,
            _budget: &mut Budget,
        ) -> Result<ShardOutcome> {
            self.log.push((target.shard, sql.to_owned()));
            if !sql.starts_with("SELECT ") {
                return Ok(ShardOutcome::Ok(ShardOk {
                    affected: 1,
                    ..ShardOk::default()
                }));
            }
            let g = &self.t.table(DEF, "invoices").unwrap().group;
            let mut rows = Vec::new();
            for part in sql.split(" UNION ALL ") {
                let b = |s: String| Some(bytes::Bytes::from(s));
                if part.contains("@@foreign_key_checks") {
                    let f = if self.fk_checks { "1" } else { "0" };
                    rows.push(vec![b("-1".into()), b(f.into())]);
                    continue;
                }
                let words: Vec<&str> = part.split_whitespace().collect();
                let probe = words[1].trim_end_matches(',');
                let iid: i64 = words.last().unwrap().trim_matches('\'').parse().unwrap();
                let Some(cid) = self.parents.get(&iid) else {
                    continue;
                };
                let i = g.shard_of(&KeyValue::Int(i128::from(*cid))).unwrap();
                if g.shards[i].shard != target.shard {
                    continue;
                }
                let h = g.hash_of(&KeyValue::Int(i128::from(*cid))).unwrap();
                rows.push(vec![b(probe.into()), b(h.to_string())]);
            }
            Ok(ShardOutcome::Rows(ShardRows {
                columns: Vec::new(),
                rows,
                warnings: 0,
            }))
        }
        fn in_transaction(&self) -> bool {
            false
        }
        async fn open_orphanage(&mut self, _v: &crate::plan::ViaParent) -> Result<bool> {
            self.opened += 1;
            Ok(self.opens)
        }
    }

    async fn run(fk_checks: bool, sql: &str) -> (Result<Output>, Vec<(u32, String)>, Fake) {
        run_opening(fk_checks, false, sql).await
    }

    async fn run_opening(
        fk_checks: bool,
        opens: bool,
        sql: &str,
    ) -> (Result<Output>, Vec<(u32, String)>, Fake) {
        let t = topo(4, true, 3);
        let plan = p_with(&t, sql);
        assert!(matches!(plan, Plan::ViaParent(_)), "{plan:?}");
        let mut f = Fake {
            t: topo(4, true, 3),
            parents: [(10, 1), (11, 2), (12, 3), (13, 4), (14, 5)]
                .into_iter()
                .collect(),
            fk_checks,
            log: Vec::new(),
            opens,
            opened: 0,
        };
        let mut budget = Budget::mb(16);
        let out = execute(&plan, &mut f, &mut budget).await;
        let log = std::mem::take(&mut f.log);
        (out, log, f)
    }

    #[tokio::test]
    async fn rows_follow_their_parents_to_their_shards() {
        let (out, log, f) = run(
            true,
            "INSERT INTO invoice_lines VALUES (10, 1, 'a'), (11, 1, 'b'), (12, 1, 'c'), (10, 2, 'd'), (13, 1, 'e'), (14, 1, 'f')",
        )
        .await;
        out.unwrap();
        let g = &f.t.table(DEF, "invoices").unwrap().group;
        // Five distinct parents, batches of two: three statements a shard.
        let reads = log.iter().filter(|(_, s)| s.starts_with("SELECT")).count();
        assert_eq!(reads, 12, "{log:#?}");
        let writes: Vec<&(u32, String)> = log
            .iter()
            .filter(|(_, s)| s.starts_with("INSERT"))
            .collect();
        let mut seen = 0;
        for (iid, cid) in [(10, 1), (11, 2), (12, 3), (13, 4), (14, 5)] {
            let i = g.shard_of(&KeyValue::Int(cid)).unwrap();
            let h = g.hash_of(&KeyValue::Int(cid)).unwrap();
            let s = &g.shards[i];
            let (_, w) = writes
                .iter()
                .find(|(sh, w)| *sh == s.shard && w.contains(&format!("({iid}, ")))
                .unwrap_or_else(|| panic!("{iid} not on shard {}: {writes:#?}", s.shard));
            assert!(w.contains(&format!("`{}`.invoice_lines", s.schema)), "{w}");
            assert!(w.contains("_sprig_rk)"), "{w}");
            assert!(w.contains(&h.to_string()), "{w}");
            seen += 1;
        }
        assert_eq!(seen, 5);
        // Both lines of invoice 10 went in one statement, in order.
        let ten = writes.iter().find(|(_, w)| w.contains("(10, 1,")).unwrap();
        assert!(ten.1.find("(10, 1,").unwrap() < ten.1.find("(10, 2,").unwrap());
    }

    #[tokio::test]
    async fn a_missing_parent_is_left_to_mariadb_or_refused() {
        let (out, log, _) = run(true, "INSERT INTO invoice_lines VALUES (99, 1, 'x')").await;
        out.unwrap();
        let w = log.iter().find(|(_, s)| s.starts_with("INSERT")).unwrap();
        assert_eq!(w.0, 1, "the first shard answers with its own 1452");
        assert!(w.1.ends_with("(99, 1, 'x', NULL)"), "{}", w.1);
        let (out, log, _) = run(false, "INSERT INTO invoice_lines VALUES (99, 1, 'x')").await;
        let e = out.unwrap_err().to_string();
        assert!(
            e.contains("insert_key") && e.contains("foreign_key_checks"),
            "{e}"
        );
        assert!(!log.iter().any(|(_, s)| s.starts_with("INSERT")));
    }

    /// With `foreign_key_checks = 0`, a row whose parent is missing opens
    /// the key's orphanage and waits on the shard holding hash 0, with
    /// `_sprig_rk = 0`; rows with a parent still follow it.
    #[tokio::test]
    async fn a_row_without_its_parent_waits_in_the_orphanage() {
        let (out, log, f) = run_opening(
            false,
            true,
            "INSERT INTO invoice_lines VALUES (99, 1, 'x'), (10, 1, 'a')",
        )
        .await;
        out.unwrap();
        assert_eq!(f.opened, 1);
        let g = &f.t.table(DEF, "invoices").unwrap().group;
        let s0 = &g.shards[g.route(0)];
        let w = log
            .iter()
            .find(|(_, s)| s.starts_with("INSERT") && s.contains("(99, 1,"))
            .unwrap();
        assert_eq!(w.0, s0.shard, "{log:#?}");
        assert!(w.1.contains("(99, 1, 'x', 0)"), "{}", w.1);
        // Its parent is there: invoice 10's line follows it.
        let h = g.hash_of(&KeyValue::Int(1)).unwrap();
        assert!(
            log.iter()
                .any(|(_, s)| s.contains("(10, 1, 'a', ") && s.contains(&h.to_string())),
            "{log:#?}"
        );
        // A parent inserted since it was looked for: side statements, after
        // the row is written, take it along (narrowed to its key).
        let side: Vec<&str> = log
            .iter()
            .map(|(_, s)| s.as_str())
            .filter(|s| crate::plan::is_side(s))
            .collect();
        let written = log.iter().position(|(_, s)| s.contains("(99, 1,")).unwrap();
        let first_side = log
            .iter()
            .position(|(_, s)| crate::plan::is_side(s))
            .unwrap();
        assert!(written < first_side, "{log:#?}");
        assert!(
            side.iter()
                .any(|s| s.contains("UPDATE") && s.contains("c.`iid` IN (99)")),
            "{side:#?}"
        );
        assert!(side.iter().all(|s| s.contains("IN (99)")), "{side:#?}");
    }

    /// An `UPDATE` that sets a parent's referenced column takes the rows
    /// waiting for the new value along, on the hash-0 shard's cluster:
    /// in place on the hash-0 shard, copied to the others and then
    /// deleted there.
    #[test]
    fn an_update_of_the_referenced_column_adopts() {
        use crate::plan::is_side;
        let t = orphan_topo(&["c1", "c2"]);
        let g = &t.table(DEF, "invoices").unwrap().group;
        let s0 = g.route(0);
        let s0_schema = &g.shards[s0].schema;
        let same = (0..g.shards.len())
            .filter(|i| g.shards[*i].cluster == g.shards[s0].cluster)
            .count();
        assert!(same > 1 && same < g.shards.len());
        let Plan::Write(w) = p_with(
            &t,
            "UPDATE invoices SET iid = 77, total = 1 WHERE total > 5",
        ) else {
            panic!()
        };
        let side: Vec<&str> = w
            .targets
            .iter()
            .filter(|x| is_side(&x.sql))
            .map(|x| x.sql.as_str())
            .collect();
        assert_eq!(w.targets.len() - side.len(), g.shards.len());
        // The update itself first, then the hash-0 shard, the copies and
        // one delete.
        assert!(!is_side(&w.targets[0].sql) && is_side(&w.targets.last().unwrap().sql));
        assert_eq!(side.len(), 1 + (same - 1) + 1, "{side:#?}");
        assert_eq!(
            side[0],
            format!(
                "/* sprig: adopt */ UPDATE `{s0_schema}`.`invoice_lines` c JOIN \
                 `{s0_schema}`.`invoices` p ON p.`iid` = c.`iid` SET c.`_sprig_rk` = \
                 p.`_sprig_rk` WHERE c.`_sprig_rk` = 0 AND p.`_sprig_rk` IS NOT NULL AND \
                 c.`iid` IN (77)"
            )
        );
        assert!(
            side[1].contains("INSERT INTO") && side[1].contains("(`iid`, `line_no`, `sku`, `_sprig_rk`) SELECT c.`iid`, c.`line_no`, c.`sku`, p.`_sprig_rk`"),
            "{}",
            side[1]
        );
        assert!(
            side.last().unwrap().starts_with(&format!(
                "/* sprig: adopt */ DELETE c FROM `{s0_schema}`.`invoice_lines` c WHERE \
                 c.`_sprig_rk` = 0 AND c.`iid` IN (77) AND (EXISTS (SELECT 1 FROM"
            )),
            "{}",
            side.last().unwrap()
        );
        // Every side statement runs on the hash-0 shard's cluster.
        assert!(
            w.targets
                .iter()
                .filter(|x| is_side(&x.sql))
                .all(|x| x.cluster == g.shards[s0].cluster)
        );
        // A new value that is an expression: every waiting row is looked at.
        let Plan::Write(w) = p_with(&t, "UPDATE invoices SET iid = iid + 1000 WHERE total > 5")
        else {
            panic!()
        };
        let side: Vec<&str> = w
            .targets
            .iter()
            .filter(|x| is_side(&x.sql))
            .map(|x| x.sql.as_str())
            .collect();
        assert!(!side.is_empty() && side.iter().all(|s| !s.contains(" IN (")));
        // Other columns, or a NULL: nothing more.
        for sql in [
            "UPDATE invoices SET total = 2 WHERE total > 5",
            "UPDATE invoices SET iid = NULL WHERE total > 5",
        ] {
            match p_with(&t, sql) {
                Plan::Write(w) => assert!(w.targets.iter().all(|x| !is_side(&x.sql)), "{sql}"),
                Plan::Pass(x) => assert!(!is_side(&x.sql)),
                p => panic!("{p:?}"),
            }
        }
    }

    /// A topology whose `invoice_lines` key has its orphanage open, on
    /// `clusters`.
    fn orphan_topo(clusters: &[&str]) -> StaticTopology {
        use sprig_shard::fk::{FkAction, FkEnforcement, FkState, ForeignKey};
        let cs: Vec<ClusterId> = clusters
            .iter()
            .map(|c| ClusterId::new(*c).unwrap())
            .collect();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let g = CatalogGroup {
            id: 3,
            spec: spec.clone(),
            map: Arc::new(
                ShardMap::presplit(TenantId(7), GroupId(3), spec.digest, 4, &cs).unwrap(),
            ),
        };
        let table = |object: u64, name: &str, key: Option<&str>, sql: &str, fk: Option<&str>| {
            CatalogTable {
                object,
                db: 1,
                default_schema: DEF.into(),
                name: name.into(),
                member: Membership::Sharded {
                    group: 3,
                    key_columns: key.map(|k| vec![k.to_owned()]),
                    create_sql: Some(sql.into()),
                    lookups: Vec::new(),
                    fill_rk: true,
                    parent_fk: fk.map(str::to_owned),
                },
            }
        };
        crate::topology::catalog::build_with_fks(
            7,
            &[g],
            &[
                table(1, "invoices", Some("cid"), INVOICES, None),
                table(2, "invoice_lines", None, LINES, Some("fk_il_inv")),
            ],
            &[ForeignKey {
                id: 1,
                name: "fk_il_inv".into(),
                child: 2,
                child_columns: vec!["iid".into()],
                parent: 1,
                parent_columns: vec!["iid".into()],
                on_delete: FkAction::Cascade,
                on_update: FkAction::Restrict,
                enforcement: FkEnforcement::Native,
                child_lookup: None,
                state: FkState::Enforced,
                orphans: true,
            }],
        )
    }

    /// An insert into the parent takes the rows waiting for it to its own
    /// shard, after its own rows, in the same write.
    #[test]
    fn a_parents_insert_adopts_its_waiting_rows() {
        let t = orphan_topo(&["c1"]);
        let e = t.table(DEF, "invoices").unwrap();
        assert_eq!(e.orphan_children.len(), 1);
        assert_eq!(e.orphan_children[0].columns, ["iid", "line_no", "sku"]);
        let g = &e.group;
        let s0 = g.route(0);
        // Two parents: one whose shard is the hash-0 shard and one that
        // isn't (customers are picked so).
        let at = |cid: i64| g.shard_of(&KeyValue::Int(i128::from(cid))).unwrap();
        let home = (1..200).find(|c| at(*c) == s0).unwrap();
        let away = (1..200).find(|c| at(*c) != s0).unwrap();
        let sql = format!(
            "INSERT INTO invoices (iid, cid, total) VALUES (50, {home}, 1.5), (51, {away}, 2)"
        );
        let Plan::Write(w) = p_with(&t, &sql) else {
            panic!("{:?}", p_with(&t, &sql))
        };
        let sqls: Vec<&str> = w.targets.iter().map(|x| x.sql.as_str()).collect();
        let h_home = g.hash_of(&KeyValue::Int(i128::from(home))).unwrap();
        let h_away = g.hash_of(&KeyValue::Int(i128::from(away))).unwrap();
        let s0_schema = &g.shards[s0].schema;
        let away_schema = &g.shards[at(away)].schema;
        assert!(sqls[0].starts_with("INSERT INTO") && sqls[1].starts_with("INSERT INTO"));
        assert!(
            sqls.contains(&format!(
                "/* sprig: adopt */ UPDATE `{s0_schema}`.`invoice_lines` SET `_sprig_rk` = CASE \
                 `iid` WHEN 50 THEN {h_home} END WHERE `iid` IN (50) AND `_sprig_rk` = 0"
            )
            .as_str()),
            "{sqls:#?}"
        );
        assert!(
            sqls.contains(&format!(
                "/* sprig: adopt */ INSERT INTO `{away_schema}`.`invoice_lines` (`iid`, `line_no`, \
                 `sku`, `_sprig_rk`) SELECT `iid`, `line_no`, `sku`, CASE `iid` WHEN 51 THEN {h_away} END FROM \
                 `{s0_schema}`.`invoice_lines` WHERE `iid` IN (51) AND `_sprig_rk` = 0"
            )
            .as_str()),
            "{sqls:#?}"
        );
        let del = sqls.iter().position(|s| s.contains("*/ DELETE")).unwrap();
        let ins = sqls
            .iter()
            .position(|s| s.contains("SELECT `iid`"))
            .unwrap();
        assert!(ins < del, "copied before it is deleted");
        // A parent on another cluster than the waiting rows: refused.
        let t2 = orphan_topo(&["c1", "c2"]);
        let g2 = &t2.table(DEF, "invoices").unwrap().group;
        let s02 = g2.route(0);
        let far = (1..400)
            .find(|c| {
                let i = g2.shard_of(&KeyValue::Int(i128::from(*c))).unwrap();
                g2.shards[i].cluster != g2.shards[s02].cluster
            })
            .unwrap();
        assert_eq!(
            rejected(
                &t2,
                &format!("INSERT INTO invoices (iid, cid, total) VALUES (52, {far}, 1)")
            ),
            Reason::CrossCluster
        );
        // Without an open orphanage nothing more is planned.
        let plain = topo(4, true, 3);
        assert!(
            plain
                .table(DEF, "invoices")
                .unwrap()
                .orphan_children
                .is_empty()
        );
    }

    /// A parent with a public unique lookup on its referenced column is
    /// found through it: no statement per shard.
    #[tokio::test]
    async fn parents_are_found_through_their_unique_lookup() {
        use crate::plan::LookupProbe;
        use crate::topology::catalog::CatalogLookup;
        use sprig_shard::lookup::{LookupOrigin, LookupState};

        struct ByLookup {
            log: Vec<String>,
            owners: HashMap<i128, u64>,
        }
        #[async_trait::async_trait]
        impl ShardBackend for ByLookup {
            async fn run(
                &mut self,
                _t: &Target,
                sql: &str,
                _b: &mut Budget,
            ) -> Result<ShardOutcome> {
                self.log.push(sql.to_owned());
                Ok(ShardOutcome::Ok(ShardOk::default()))
            }
            fn in_transaction(&self) -> bool {
                false
            }
            async fn lookup_owners(&mut self, probe: &LookupProbe) -> Result<Vec<u64>> {
                let [v] = probe.values.as_slice() else {
                    panic!()
                };
                let [sprig_shard::KeyValueOwned::Int(i)] = v.as_slice() else {
                    panic!("{v:?}")
                };
                Ok(self.owners.get(i).copied().into_iter().collect())
            }
        }
        let c1 = ClusterId::new("c1").unwrap();
        let spec = KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap();
        let map = ShardMap::presplit(
            TenantId(7),
            GroupId(3),
            spec.digest,
            4,
            std::slice::from_ref(&c1),
        )
        .unwrap();
        let lmap = ShardMap::presplit(TenantId(7), GroupId(5), 0, 2, &[c1]).unwrap();
        let g = CatalogGroup {
            id: 3,
            spec,
            map: Arc::new(map),
        };
        let lookup = CatalogLookup {
            id: 11,
            owner: 70,
            index_name: "PRIMARY".into(),
            columns: vec!["iid".into()],
            unique: true,
            origin: LookupOrigin::PrimaryKey,
            state: LookupState::Public,
            map: Some(Arc::new(lmap)),
            rehashing: false,
        };
        let t = build(
            7,
            &[g],
            &[
                CatalogTable {
                    object: 0,
                    db: 1,
                    default_schema: DEF.into(),
                    name: "invoices".into(),
                    member: Membership::Sharded {
                        group: 3,
                        key_columns: Some(vec!["cid".into()]),
                        create_sql: Some(INVOICES.into()),
                        lookups: vec![lookup],
                        fill_rk: true,
                        parent_fk: None,
                    },
                },
                CatalogTable {
                    object: 0,
                    db: 1,
                    default_schema: DEF.into(),
                    name: "invoice_lines".into(),
                    member: Membership::Sharded {
                        group: 3,
                        key_columns: None,
                        create_sql: Some(LINES.into()),
                        lookups: Vec::new(),
                        fill_rk: true,
                        parent_fk: Some("FK_IL_INV".into()),
                    },
                },
            ],
        );
        let e = t.table(DEF, "invoice_lines").unwrap();
        assert!(e.parent.as_ref().unwrap().lookup.is_some());
        let plan = p_with(
            &t,
            "INSERT INTO invoice_lines VALUES (10, 1, 'a'), (11, 1, 'b')",
        );
        let Plan::ViaParent(v) = &plan else {
            panic!("{plan:?}")
        };
        assert!(v.lookup.is_some());
        let h = |c: i128| e.group.hash_of(&KeyValue::Int(c)).unwrap();
        let mut be = ByLookup {
            log: Vec::new(),
            owners: [(10, h(1)), (11, h(1))].into_iter().collect(),
        };
        execute(&plan, &mut be, &mut Budget::mb(16)).await.unwrap();
        // Both parents hash alike: one statement, nothing read from shards.
        let [w] = be.log.as_slice() else {
            panic!("{:?}", be.log)
        };
        assert!(w.starts_with("INSERT"), "{w}");
        assert!(w.contains(&format!("(10, 1, 'a', {})", h(1))), "{w}");
    }

    #[tokio::test]
    async fn insert_set_follows_its_parent() {
        let (out, log, f) = run(true, "INSERT INTO invoice_lines SET iid = 12, line_no = 3").await;
        out.unwrap();
        let g = &f.t.table(DEF, "invoices").unwrap().group;
        let h = g.hash_of(&KeyValue::Int(3)).unwrap();
        let w = log.iter().find(|(_, s)| s.starts_with("INSERT")).unwrap();
        assert!(w.1.ends_with(&format!("_sprig_rk = {h}")), "{}", w.1);
    }
}
