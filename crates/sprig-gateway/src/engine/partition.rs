//! A partitioned engine table's rows and their partitions. The rows stay
//! in the table's one storage unit; the partitioning decides what a row
//! may hold (1526 for a value no `RANGE` or `LIST` partition takes), which
//! rows `PARTITION (p)` names, and which rows dropping or truncating a
//! partition deletes.
//!
//! ```text
//! RANGE (e)          the first partition whose bound is above e (NULL: the first)
//! LIST (e)           the partition listing e (NULL: the one listing NULL)
//! RANGE COLUMNS (c)  the first bound above the row's columns, compared in order
//! LIST COLUMNS (c)   the partition listing the row's columns
//! HASH (e)           |e| mod n (NULL: 0)
//! LINEAR HASH (e)    e's low bits under the next power of two, halved while
//!                    at or above n (the servers' documented algorithm)
//! KEY                the servers' own hash: not known here
//! ```

use sprig_common::{Result, SprigError};
use std::cmp::Ordering;

use super::define::partition::{Lit, Method, Spec, Values};
use super::expr::{self, Args, CExpr, CmpCx};
use super::row::Datum;
use super::table::TableDef;

tokio::task_local! {
    /// A statement's `PARTITION (p, ...)` of an `INSERT`, `REPLACE` or
    /// `UPDATE`: the table object and the partitions a written row must be
    /// in (else 1748).
    pub(crate) static SELECTED: (u32, Vec<usize>);
}

/// A table's partitioning as rows are judged by it.
#[derive(Debug)]
pub(crate) struct Rule {
    pub spec: Spec,
    /// The expression of `RANGE`, `LIST` and `HASH`.
    pub expr: Option<CExpr>,
    /// The columns of `COLUMNS` partitioning (positions in the table).
    pub cols: Vec<usize>,
}

/// Where a row goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Placed {
    /// Partition `usize`.
    In(usize),
    /// No partition takes it: the value as 1526 prints it.
    Nowhere(String),
    /// `KEY` partitioning: the servers' hash isn't known here.
    Unknown,
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)] // the servers' integer of a value
fn int_of(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int(v) => Some(*v),
        Datum::UInt(v) | Datum::Bit(v) => Some(*v as i64),
        Datum::Year(y) => Some(i64::from(*y)),
        Datum::Double(f) => Some(f.round() as i64),
        Datum::Float(f) => Some(f.round() as i64),
        Datum::Decimal(d) => d.to_string().parse::<f64>().ok().map(|f| f.round() as i64),
        _ => None,
    }
}

/// `v`'s partition of `n` under `LINEAR HASH` (on `v`'s two's
/// complement bits, as the servers compute it).
#[allow(clippy::cast_sign_loss)]
fn linear(v: i64, n: u64) -> u64 {
    let mut pow = n.max(1).next_power_of_two();
    let mut k = (v as u64) & (pow - 1);
    while k >= n && pow > 1 {
        pow >>= 1;
        k &= pow - 1;
    }
    k
}

impl Rule {
    /// The rule of `spec` over `def`'s columns.
    pub(crate) fn new(spec: Spec, expr: Option<CExpr>, def: &TableDef) -> Self {
        let cols = if spec.columns {
            spec.cols.iter().filter_map(|c| def.column(c)).collect()
        } else {
            Vec::new()
        };
        Self { spec, expr, cols }
    }

    /// The value the partitioning expression gives `row` (`None`: NULL).
    fn value(&self, row: &[Datum], a: &Args<'_>) -> Result<Option<i64>> {
        let Some(e) = &self.expr else {
            return Ok(None);
        };
        let d = expr::eval(e, row, a)?;
        Ok(int_of(&d))
    }

    /// Where `row` of `def` goes.
    ///
    /// # Errors
    /// The expression's.
    pub(crate) fn place(&self, def: &TableDef, row: &[Datum], a: &Args<'_>) -> Result<Placed> {
        let n = self.spec.parts.len();
        if self.spec.columns {
            return Ok(self.place_columns(def, row, a));
        }
        match self.spec.method {
            Method::Key => Ok(Placed::Unknown),
            Method::Hash => {
                // NULL hashes as the smallest BIGINT, as the servers place it.
                let v = self.value(row, a)?.unwrap_or(i64::MIN);
                let k = if self.spec.linear {
                    linear(v, n as u64)
                } else {
                    v.unsigned_abs() % (n.max(1) as u64)
                };
                Ok(Placed::In(usize::try_from(k).unwrap_or(0)))
            }
            Method::Range => {
                let v = self.value(row, a)?;
                let Some(v) = v else {
                    return Ok(Placed::In(0));
                };
                for (i, p) in self.spec.parts.iter().enumerate() {
                    if let Values::LessThan(b) = &p.values {
                        match b.first() {
                            Some(Lit::Max) => return Ok(Placed::In(i)),
                            Some(Lit::Int(hi)) if v < *hi => return Ok(Placed::In(i)),
                            _ => {}
                        }
                    }
                }
                Ok(Placed::Nowhere(v.to_string()))
            }
            Method::List => {
                let v = self.value(row, a)?;
                let want = v.map_or(Lit::Null, Lit::Int);
                for (i, p) in self.spec.parts.iter().enumerate() {
                    if let Values::In(items) = &p.values
                        && items.iter().any(|t| t.first() == Some(&want))
                    {
                        return Ok(Placed::In(i));
                    }
                }
                Ok(Placed::Nowhere(
                    v.map_or_else(|| "NULL".to_owned(), |x| x.to_string()),
                ))
            }
        }
    }

    /// Row value `d` of column `c` against literal `l`.
    fn cmp(def: &TableDef, c: usize, d: &Datum, l: &Lit, a: &Args<'_>) -> Option<Ordering> {
        let lit = match l {
            Lit::Int(v) => Datum::Int(*v),
            Lit::Str(s) => Datum::Str(bytes::Bytes::from(s.clone().into_bytes())),
            Lit::Null => Datum::Null,
            Lit::Max => return Some(Ordering::Less),
        };
        if matches!(d, Datum::Null) || matches!(lit, Datum::Null) {
            // NULL sorts below every value, as the servers place it.
            return Some(match (d, &lit) {
                (Datum::Null, Datum::Null) => Ordering::Equal,
                (Datum::Null, _) => Ordering::Less,
                _ => Ordering::Greater,
            });
        }
        let col = def.columns.get(c)?;
        let cx = CmpCx {
            collation: col.collation,
            mb3: col.mb3,
            text_right: true,
            ..CmpCx::default()
        };
        expr::compare(d, &lit, &cx, &a.env, "<").ok().flatten()
    }

    fn place_columns(&self, def: &TableDef, row: &[Datum], a: &Args<'_>) -> Placed {
        let vals: Vec<&Datum> = self
            .cols
            .iter()
            .map(|&c| row.get(c).unwrap_or(&Datum::Null))
            .collect();
        let shown = || {
            vals.iter()
                .map(|d| match d {
                    Datum::Null => "NULL".to_owned(),
                    Datum::Int(v) => v.to_string(),
                    Datum::UInt(v) => v.to_string(),
                    Datum::Str(b) => format!("'{}'", String::from_utf8_lossy(b)),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(",")
        };
        for (i, p) in self.spec.parts.iter().enumerate() {
            match &p.values {
                Values::LessThan(b) => {
                    let mut below = false;
                    for (k, l) in b.iter().enumerate() {
                        let c = self.cols.get(k).copied().unwrap_or(0);
                        match Self::cmp(def, c, vals.get(k).copied().unwrap_or(&Datum::Null), l, a)
                        {
                            Some(Ordering::Less) => {
                                below = true;
                                break;
                            }
                            Some(Ordering::Equal) => {}
                            _ => break,
                        }
                    }
                    if below {
                        return Placed::In(i);
                    }
                }
                Values::In(items) => {
                    let hit = items.iter().any(|t| {
                        t.iter().enumerate().all(|(k, l)| {
                            let c = self.cols.get(k).copied().unwrap_or(0);
                            Self::cmp(def, c, vals.get(k).copied().unwrap_or(&Datum::Null), l, a)
                                == Some(Ordering::Equal)
                        })
                    });
                    if hit {
                        return Placed::In(i);
                    }
                }
                Values::None => {}
            }
        }
        let _ = shown;
        Placed::Nowhere("from column_list".to_owned())
    }
}

/// 1526 for a row no partition takes.
pub(crate) fn nowhere(value: &str) -> SprigError {
    SprigError::standard(
        1526,
        *b"HY000",
        format!("Table has no partition for value {value}"),
    )
}

/// 1748 for a row outside a statement's `PARTITION (...)`.
pub(crate) fn not_selected() -> SprigError {
    SprigError::standard(
        1748,
        *b"HY000",
        "Found a row not matching the given partition set",
    )
}

/// The partition checks of a row version written to `def` (object
/// `object`): 1526 when no partition takes it, 1748 when the statement's
/// `PARTITION (...)` doesn't name its partition.
///
/// # Errors
/// As above; the expression's.
pub(crate) fn check_row(def: &TableDef, row: &[Datum], a: &Args<'_>) -> Result<()> {
    let Some(rule) = &def.partition else {
        return Ok(());
    };
    let placed = rule.place(def, row, a)?;
    if let Placed::Nowhere(v) = &placed {
        return Err(nowhere(v));
    }
    let object = u32::try_from(def.object.0).unwrap_or(u32::MAX);
    let selected = SELECTED
        .try_with(|(o, parts)| (*o == object).then(|| parts.clone()))
        .ok()
        .flatten();
    if let (Some(parts), Placed::In(i)) = (selected, &placed)
        && !parts.contains(i)
    {
        return Err(not_selected());
    }
    Ok(())
}

fn lit_sql(l: &Lit) -> String {
    match l {
        Lit::Int(n) => n.to_string(),
        Lit::Str(s) => super::define::expr::quote_str(s),
        Lit::Null | Lit::Max => "NULL".into(),
    }
}

/// `(a, b)` of `items`, or the item alone.
fn tuple_sql(items: &[String]) -> String {
    if items.len() == 1 {
        items[0].clone()
    } else {
        format!("({})", items.join(", "))
    }
}

/// `(c..) < (v..)` for a `RANGE COLUMNS` bound with `MAXVALUE` at its
/// first such column cut there (`None`: no upper bound).
fn below(cols: &[String], bound: &[Lit]) -> Option<String> {
    let cut = bound
        .iter()
        .position(|l| *l == Lit::Max)
        .unwrap_or(bound.len());
    if cut == 0 {
        return None;
    }
    let c = tuple_sql(&cols[..cut]);
    let v = tuple_sql(&bound[..cut].iter().map(lit_sql).collect::<Vec<_>>());
    Some(if cut < bound.len() {
        format!("{c} <= {v}")
    } else {
        format!("{c} < {v}")
    })
}

/// SQL text true for exactly the rows of partitions `which` of `spec`, over
/// the table's columns unqualified; `None` for `KEY` partitioning, whose
/// hash isn't known here.
#[allow(clippy::too_many_lines)] // one arm per method
pub(crate) fn condition(spec: &Spec, which: &[usize]) -> Option<String> {
    use super::define::expr::{print, quote_name};
    let n = spec.parts.len();
    let e = spec.expr.as_ref().map(|x| format!("({})", print(x)));
    let cols: Vec<String> = spec.cols.iter().map(|c| quote_name(c)).collect();
    let mut ors = Vec::new();
    for &i in which {
        let part = spec.parts.get(i)?;
        let c = match (spec.method, spec.columns, &part.values) {
            (Method::Hash, _, _) => {
                let e = e.clone()?;
                let v = format!("IFNULL({e}, 0)");
                if spec.linear {
                    let mut pow = (n.max(1) as u64).next_power_of_two();
                    let mut x = format!("({v} & {})", pow - 1);
                    while pow > 1 {
                        pow >>= 1;
                        x = format!("IF({x} >= {n}, {x} & {}, {x})", pow.saturating_sub(1));
                    }
                    format!("{x} = {i}")
                } else {
                    // NULL hashes as the smallest BIGINT.
                    let null = (1u64 << 63) % (n.max(1) as u64);
                    format!("IF({e} IS NULL, {null}, MOD(ABS({e}), {n})) = {i}")
                }
            }
            (Method::Range, false, Values::LessThan(hi)) => {
                let e = e.clone()?;
                let upper = match hi.first() {
                    Some(Lit::Int(h)) => Some(format!("{e} < {h}")),
                    _ => None,
                };
                let lower = match i.checked_sub(1).map(|k| &spec.parts[k].values) {
                    Some(Values::LessThan(lo)) => match lo.first() {
                        Some(Lit::Int(l)) => Some(format!("{e} >= {l}")),
                        _ => None,
                    },
                    _ => None,
                };
                match (lower, upper) {
                    (None, Some(u)) => format!("({e} IS NULL OR {u})"),
                    (None, None) => "1".to_owned(),
                    (Some(l), Some(u)) => format!("({l} AND {u})"),
                    (Some(l), None) => l,
                }
            }
            (Method::List, false, Values::In(items)) => {
                let e = e.clone()?;
                let vals: Vec<String> = items
                    .iter()
                    .filter_map(|t| t.first())
                    .filter(|l| **l != Lit::Null)
                    .map(lit_sql)
                    .collect();
                let null = items.iter().any(|t| t.first() == Some(&Lit::Null));
                let mut parts = Vec::new();
                if !vals.is_empty() {
                    parts.push(format!("{e} IN ({})", vals.join(", ")));
                }
                if null {
                    parts.push(format!("{e} IS NULL"));
                }
                if parts.is_empty() {
                    "0".to_owned()
                } else {
                    format!("({})", parts.join(" OR "))
                }
            }
            (Method::Range, true, Values::LessThan(hi)) => {
                let upper = below(&cols, hi);
                let lower = match i.checked_sub(1).map(|k| &spec.parts[k].values) {
                    Some(Values::LessThan(lo)) => below(&cols, lo).map(|b| format!("NOT ({b})")),
                    _ => None,
                };
                let nulls = cols
                    .iter()
                    .map(|c| format!("{c} IS NULL"))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                match (lower, upper) {
                    (None, Some(u)) => format!("({u} OR {nulls})"),
                    (None, None) => "1".to_owned(),
                    (Some(l), Some(u)) => format!("({l} AND {u})"),
                    (Some(l), None) => l,
                }
            }
            (Method::List, true, Values::In(items)) => {
                let c = tuple_sql(&cols);
                let mut alts = Vec::new();
                for t in items {
                    if t.contains(&Lit::Null) {
                        let eq: Vec<String> = cols
                            .iter()
                            .zip(t)
                            .map(|(c, l)| match l {
                                Lit::Null => format!("{c} IS NULL"),
                                l => format!("{c} = {}", lit_sql(l)),
                            })
                            .collect();
                        alts.push(format!("({})", eq.join(" AND ")));
                    } else {
                        alts.push(format!(
                            "{c} = {}",
                            tuple_sql(&t.iter().map(lit_sql).collect::<Vec<_>>())
                        ));
                    }
                }
                if alts.is_empty() {
                    "0".to_owned()
                } else {
                    format!("({})", alts.join(" OR "))
                }
            }
            _ => return None,
        };
        ors.push(c);
    }
    Some(if ors.is_empty() {
        "0".to_owned()
    } else {
        format!("({})", ors.join(" OR "))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_hash_follows_the_documented_algorithm() {
        let got: Vec<u64> = [0, 1, 2, 3, 4, 5, 6, 7, 13, -7]
            .iter()
            .map(|&v| linear(v, 6))
            .collect();
        assert_eq!(got, [0, 1, 2, 3, 4, 5, 2, 3, 5, 1]);
    }
}
