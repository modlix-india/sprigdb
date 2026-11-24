//! The statements validation replays, and their what-if plans.
//!
//! ```text
//! statements(pattern):
//!   parameter samples (workload.capture_samples): each sample's literals
//!     put back into the pattern's text (`?` one value, `(...)` a list)
//!   no samples: the pattern's text with values from the table's first
//!     rows (each `?` a predicate compares with gets that column's value)
//!   UPDATE t SET ... WHERE w / DELETE FROM t WHERE w  ->  SELECT * FROM t WHERE w
//! replay(statement, spec):
//!   bound as the engine binds a SELECT (the pattern's database current),
//!   planned without and with spec as a hypothetical index:
//!   rows examined before and after, and whether the plan reads it
//! ```

use std::sync::Arc;

use sprig_advisor::autoindex::candidates::{ColumnInfo, ExistingIndex, TableInfo};
use sprig_advisor::autoindex::model::{IndexSpec, KeyCol};
use sprig_advisor::autoindex::policy::Replay;
use sprig_catalog::ddl::database_key;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_engine::txn::TxnOptions;
use sprig_plan::catalog::{IndexDef, IndexPart};
use sprig_plan::whatif::{Hypothetical, SAMPLE_ROWS};
use sprig_workload::keyring::TenantKeyring;
use sprig_workload::shape::StmtType;
use sprig_workload::store::WorkloadTables;
use sprig_workload::window::{FpWindowStats, SampledLiterals, SampledSlot};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::ddl::{lctn, table_def};
use crate::engine::catalog;
use crate::engine::general::{self, Seed};
use crate::engine::plan::{BindCtx, CatalogNames};
use crate::engine::row::Datum;
use crate::engine::table::{ColType, TableDef};
use crate::engine::{self, keys};
use crate::tenant::TenantServices;

/// Rows read from a table for its figures and for values.
const TABLE_SAMPLE: usize = 1000;

/// Up to `n` rows of `def`, in primary-key order.
pub(crate) async fn first_rows(
    tenant: TenantId,
    def: &TableDef,
    n: usize,
) -> Result<Vec<Vec<Datum>>> {
    let eng = engine::engine()?;
    let sp = eng.space(tenant.0, def).await?;
    let p = sp.rows();
    let mut tx = eng.begin(TxnOptions::default())?;
    let got = tx.scan(&p, &keys::prefix_end(&p), false, n)?;
    got.iter()
        .map(|(k, v)| crate::engine::row::decode(def, &sp, k, v))
        .collect()
}

/// Whether a column of this type can be a whole key part.
fn indexable(ty: ColType) -> bool {
    !matches!(
        ty,
        ColType::Text { .. } | ColType::Blob { .. } | ColType::Json
    )
}

/// The generator's view of `def`: columns with distinct counts from a
/// sample, the primary key, the indexes.
pub(crate) async fn table_info(tenant: TenantId, db: &str, def: &TableDef) -> Result<TableInfo> {
    let rows = first_rows(tenant, def, TABLE_SAMPLE).await?;
    let complete = rows.len() < TABLE_SAMPLE;
    let columns = def
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let mut seen: Vec<&Datum> = rows.iter().filter_map(|r| r.get(i)).collect();
            seen.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            seen.dedup_by(|a, b| a == b);
            let d = seen.len() as f64;
            // Mostly distinct in a partial sample: more values beyond it.
            let distinct = if !complete && d > rows.len() as f64 * 0.9 {
                d * 10.0
            } else {
                d
            };
            ColumnInfo {
                name: c.name.clone(),
                indexable: indexable(c.ty),
                distinct: Some(distinct),
            }
        })
        .collect();
    let name_of = |i: usize| def.columns.get(i).map(|c| c.name.clone());
    Ok(TableInfo {
        db: db.to_owned(),
        table: def.name.clone(),
        columns,
        pk: if def.hidden {
            Vec::new()
        } else {
            def.pk.iter().filter_map(|&i| name_of(i)).collect()
        },
        indexes: def
            .indexes
            .iter()
            .map(|x| ExistingIndex {
                name: x.name.clone(),
                cols: x
                    .cols
                    .iter()
                    .zip(&x.parts)
                    .filter_map(|(&c, kc)| {
                        Some(KeyCol {
                            column: name_of(c)?,
                            desc: kc.desc,
                        })
                    })
                    .collect(),
                ignored: x.ignored,
            })
            .collect(),
        rows: rows.len() as u64,
    })
}

/// A value as SQL literal text.
fn literal(v: &Datum) -> String {
    match v {
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) | Datum::Bit(u) | Datum::Set(u) => u.to_string(),
        Datum::Double(d) => format!("{d:e}"),
        Datum::Float(f) => format!("{f:e}"),
        Datum::Year(y) => y.to_string(),
        Datum::Enum(e) => e.to_string(),
        Datum::Str(b) | Datum::Json(b) => {
            format!(
                "'{}'",
                String::from_utf8_lossy(b)
                    .replace('\\', "\\\\")
                    .replace('\'', "''")
            )
        }
        Datum::Bytes(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!("X'{hex}'")
        }
        Datum::Null => "NULL".into(),
        other => format!("'{}'", format!("{other:?}").replace('\'', "''")),
    }
}

/// `text` with each `?` and `(...)` replaced by `values[slot]`.
fn fill(text: &str, value: &dyn Fn(usize, bool) -> String) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len() + 16);
    let (mut i, mut slot) = (0usize, 0usize);
    while i < b.len() {
        match b[i] {
            q @ (b'`' | b'\'' | b'"') => {
                let start = i;
                i += 1;
                while i < b.len() && b[i] != q {
                    i += 1;
                }
                i = (i + 1).min(b.len());
                out.push_str(&text[start..i]);
            }
            b'?' => {
                out.push_str(&value(slot, false));
                slot += 1;
                i += 1;
            }
            b'(' if text[i..].starts_with("(...)") => {
                out.push('(');
                out.push_str(&value(slot, true));
                out.push(')');
                slot += 1;
                i += 5;
            }
            _ => {
                let ch = text[i..].chars().next().unwrap_or(' ');
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    out
}

fn from_sample(text: &str, l: &SampledLiterals) -> String {
    fill(
        text,
        &|slot, list| match l.slot(u32::try_from(slot).unwrap_or(u32::MAX)) {
            Some(SampledSlot::One(v)) => String::from_utf8_lossy(&v.text).into_owned(),
            Some(SampledSlot::List(vs)) if !vs.is_empty() => vs
                .iter()
                .map(|v| String::from_utf8_lossy(&v.text).into_owned())
                .collect::<Vec<_>>()
                .join(", "),
            _ if list => "NULL".into(),
            _ => "1".into(),
        },
    )
}

/// A write's `WHERE` as a `SELECT` (a `SELECT` as it is).
fn select_form(sql: &str, kind: StmtType) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let find = |w: &str| upper.find(w);
    match kind {
        StmtType::Select => Some(sql.to_owned()),
        StmtType::Update => {
            let table = sql.get(6..find(" SET ")?)?.trim().to_owned();
            let w = find(" WHERE ")?;
            Some(format!("SELECT * FROM {table}{}", &sql[w..]))
        }
        StmtType::Delete => {
            let from = find("FROM ")?;
            Some(format!("SELECT * {}", &sql[from..]))
        }
        _ => None,
    }
}

/// The statements to replay for `f`, at most `max`.
pub(crate) async fn statements(
    svc: &TenantServices,
    keyring: &dyn TenantKeyring,
    tenant: TenantId,
    f: &FpWindowStats,
    max: usize,
) -> Result<Vec<String>> {
    let mut texts = Vec::new();
    {
        let mut rec = svc.records.begin(tenant).await?;
        let opened = WorkloadTables::open_samples(&mut *rec, keyring, tenant, f.id, max).await?;
        rec.commit().await?;
        for s in &opened {
            texts.push(from_sample(&f.text, &s.payload.literals));
        }
    }
    if texts.is_empty() {
        // No samples: values from the main table's first rows.
        let cat = svc.cache.get(tenant, Freshness::Latest).await?;
        let Some(t) = f.shape.tables.iter().find(|t| !t.derived) else {
            return Ok(Vec::new());
        };
        let Some(db) = t.db.as_deref() else {
            return Ok(Vec::new());
        };
        let Some(def) = table_def(svc, &cat, db, &t.table) else {
            return Ok(Vec::new());
        };
        let rows = first_rows(tenant, &def, max.max(1) * 7).await?;
        let slot_col: Vec<(u32, usize)> = f
            .shape
            .predicates
            .iter()
            .flat_map(|p| {
                let c = def.column(&p.column.col);
                p.args.iter().filter_map(move |a| Some((*a, c?)))
            })
            .collect();
        for r in rows.iter().step_by(7).take(max) {
            texts.push(fill(&f.text, &|slot, _| {
                slot_col
                    .iter()
                    .find(|(s, _)| *s as usize == slot)
                    .and_then(|(_, c)| r.get(*c))
                    .map_or_else(|| "1".into(), literal)
            }));
        }
    }
    Ok(texts
        .iter()
        .filter_map(|t| select_form(t, f.shape.stmt_type))
        .collect())
}

/// Plans `sql` (current database `db`) without and with `spec`.
pub(crate) async fn replay_one(
    svc: &TenantServices,
    tenant: TenantId,
    cat: &Arc<TenantCatalog>,
    db: &str,
    sql: &str,
    spec: &IndexSpec,
) -> Result<Option<Replay>> {
    let Ok(mut stmts) = Parser::parse_sql(&MySqlDialect {}, sql) else {
        return Ok(None);
    };
    if stmts.len() != 1 {
        return Ok(None);
    }
    let stmt = stmts.remove(0);
    // Bound in a block of its own: the binder's names aren't `Send`, and
    // the plans below wait.
    let g = {
        let Some(eng_cat) = catalog::of(cat) else {
            return Ok(None);
        };
        let lower = lctn(svc, cat);
        let names = CatalogNames {
            cat,
            eng: eng_cat,
            lower_case: lower,
        };
        let current = cat.database(&database_key(db, lower)).map(|d| d.db_id);
        let cx = BindCtx {
            tenant,
            names: &names,
            current,
            backslash: true,
        };
        match general::bind(&cx, &stmt, Some(sql), None) {
            Ok(Some(g)) => g,
            _ => return Ok(None),
        }
    };
    let Some(def) = table_def(svc, cat, &spec.db, &spec.table) else {
        return Ok(None);
    };
    let mut parts = Vec::new();
    for c in &spec.cols {
        let Some(i) = def.column(&c.column) else {
            return Ok(None);
        };
        parts.push(if c.desc {
            IndexPart::desc(i)
        } else {
            IndexPart::asc(i)
        });
    }
    let hypo = Hypothetical {
        schema: spec.db.clone(),
        table: spec.table.clone(),
        index: IndexDef::new(&spec.name(), false, parts),
        sample_rows: SAMPLE_ROWS,
    };
    let seed = Seed::from_vars(false, &crate::engine::assign::Env::STRICT, |_| None);
    let eng = engine::engine()?;
    match eng
        .general_whatif(tenant.0, &g, &[], &seed, Some(&hypo))
        .await
    {
        Ok((before, Some(after))) => Ok(Some(Replay {
            before: before.examined,
            after: after.examined,
            reads: after.reads(&spec.name()),
        })),
        Ok((_, None)) => Ok(None),
        Err(e) => {
            tracing::debug!(error = %e, "automatic indexing: a replayed statement didn't plan");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_go_back_into_the_text() {
        let t = "SELECT * FROM `o?` WHERE a = ? AND b IN (...) AND c = '?'";
        let got = fill(t, &|slot, list| {
            if list {
                "1, 2".into()
            } else {
                format!("v{slot}")
            }
        });
        assert_eq!(
            got,
            "SELECT * FROM `o?` WHERE a = v0 AND b IN (1, 2) AND c = '?'"
        );
    }

    #[test]
    fn writes_are_replayed_as_selects() {
        assert_eq!(
            select_form("UPDATE t SET a = 1 WHERE b = 2", StmtType::Update).as_deref(),
            Some("SELECT * FROM t WHERE b = 2")
        );
        assert_eq!(
            select_form("DELETE FROM t WHERE b = 2", StmtType::Delete).as_deref(),
            Some("SELECT * FROM t WHERE b = 2")
        );
        assert_eq!(
            select_form("INSERT INTO t VALUES (1)", StmtType::Insert),
            None
        );
    }

    #[test]
    fn values_become_literals() {
        assert_eq!(literal(&Datum::Int(-4)), "-4");
        assert_eq!(
            literal(&Datum::Str(bytes::Bytes::from_static(b"o'k"))),
            "'o''k'"
        );
        assert_eq!(literal(&Datum::Null), "NULL");
    }
}
