//! How a database's tables join, for `relationships`: declared foreign
//! keys first, then joins inferred from names and types for columns no
//! foreign key covers. Deterministic; the host reads the columns and keys
//! from `information_schema` as the caller.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

/// One column, as `information_schema.COLUMNS` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Col {
    /// The table.
    pub table: String,
    /// The column.
    pub column: String,
    /// `DATA_TYPE`: `bigint`, `varchar`, ...
    pub ty: String,
    /// It is part of the primary key.
    pub primary: bool,
}

/// One declared foreign key column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fk {
    /// The constraint.
    pub name: String,
    /// The referencing table and column.
    pub table: String,
    /// Its column.
    pub column: String,
    /// The referenced table.
    pub ref_table: String,
    /// The referenced column.
    pub ref_column: String,
}

/// Joins listed at most.
pub const MAX_JOINS: usize = 300;

fn family(ty: &str) -> &'static str {
    match ty.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => "int",
        "char" | "varchar" | "binary" | "varbinary" | "uuid" => "text",
        "decimal" | "numeric" => "decimal",
        _ => "other",
    }
}

/// `customers` → `customer`, `categories` → `category`, `boxes` → `box`.
fn singular(t: &str) -> String {
    let l = t.to_ascii_lowercase();
    if let Some(s) = l.strip_suffix("ies") {
        format!("{s}y")
    } else if l.ends_with("sses") || l.ends_with("xes") || l.ends_with("ches") {
        l[..l.len() - 2].to_owned()
    } else if let Some(s) = l.strip_suffix('s') {
        s.to_owned()
    } else {
        l
    }
}

/// The stems a column such as `customer_id` or `customerId` refers to.
fn stem(column: &str) -> Option<String> {
    let l = column.to_ascii_lowercase();
    if let Some(s) = l.strip_suffix("_id").filter(|s| !s.is_empty()) {
        return Some(s.to_owned());
    }
    // camelCase: customerId.
    column
        .strip_suffix("Id")
        .or_else(|| column.strip_suffix("ID"))
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut out = String::new();
            for (i, c) in s.chars().enumerate() {
                if c.is_ascii_uppercase() && i > 0 {
                    out.push('_');
                }
                out.push(c.to_ascii_lowercase());
            }
            out
        })
}

/// The joins of `cols` and `fks`; with `only`, those that touch it.
#[must_use]
pub fn relationships(cols: &[Col], fks: &[Fk], only: Option<&str>) -> Value {
    // Single-column primary keys, by table.
    let mut pks: BTreeMap<&str, Vec<&Col>> = BTreeMap::new();
    for c in cols.iter().filter(|c| c.primary) {
        pks.entry(c.table.as_str()).or_default().push(c);
    }
    let single: BTreeMap<&str, &Col> = pks
        .into_iter()
        .filter(|(_, v)| v.len() == 1)
        .map(|(t, v)| (t, v[0]))
        .collect();
    let mut joins = Vec::new();
    let mut covered: BTreeSet<(String, String)> = BTreeSet::new();
    for f in fks {
        covered.insert((f.table.to_ascii_lowercase(), f.column.to_ascii_lowercase()));
        joins.push(json!({
            "from": format!("{}.{}", f.table, f.column),
            "to": format!("{}.{}", f.ref_table, f.ref_column),
            "source": "foreign_key",
            "constraint": f.name,
        }));
    }
    for c in cols {
        if covered.contains(&(c.table.to_ascii_lowercase(), c.column.to_ascii_lowercase())) {
            continue;
        }
        // A table's own single-column key refers to nothing.
        if single
            .get(c.table.as_str())
            .is_some_and(|k| k.column == c.column)
        {
            continue;
        }
        let stem = stem(&c.column);
        let target = single.iter().find(|(t, k)| {
            **t != c.table
                && family(&k.ty) == family(&c.ty)
                && family(&c.ty) != "other"
                && (k.column.eq_ignore_ascii_case(&c.column)
                    && !k.column.eq_ignore_ascii_case("id")
                    || stem.as_deref().is_some_and(|s| {
                        k.column.eq_ignore_ascii_case("id")
                            && (t.eq_ignore_ascii_case(s) || singular(t) == s)
                    }))
        });
        if let Some((t, k)) = target {
            joins.push(json!({
                "from": format!("{}.{}", c.table, c.column),
                "to": format!("{t}.{}", k.column),
                "source": "inferred",
                "why": "the column's name and type match that table's primary key; no foreign key declares it",
            }));
        }
    }
    if let Some(o) = only {
        let touches = |v: &Value, k: &str| {
            v[k].as_str()
                .and_then(|s| s.split_once('.'))
                .is_some_and(|(t, _)| t.eq_ignore_ascii_case(o))
        };
        joins.retain(|j| touches(j, "from") || touches(j, "to"));
    }
    let more = joins.len() > MAX_JOINS;
    joins.truncate(MAX_JOINS);
    let mut out = json!({"joins": joins, "more": more});
    if out["joins"].as_array().is_some_and(Vec::is_empty) {
        out["note"] = json!(
            "No foreign keys and no column that matches another table's key by name and type. Describe the tables and ask the user how they relate."
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(t: &str, c: &str, ty: &str, pk: bool) -> Col {
        Col {
            table: t.into(),
            column: c.into(),
            ty: ty.into(),
            primary: pk,
        }
    }

    #[test]
    fn foreign_keys_first_then_names_and_types() {
        let cols = vec![
            col("customers", "id", "bigint", true),
            col("categories", "id", "int", true),
            col("products", "id", "bigint", true),
            col("products", "categoryId", "int", false),
            col("orders", "id", "bigint", true),
            col("orders", "customer_id", "bigint", false),
            col("orders", "region", "varchar", false),
            col("order_items", "order_id", "bigint", true),
            col("order_items", "product_id", "bigint", true),
            col("order_items", "status_id", "varchar", false),
            col("statuses", "id", "int", true),
        ];
        let fks = vec![Fk {
            name: "fk_items_order".into(),
            table: "order_items".into(),
            column: "order_id".into(),
            ref_table: "orders".into(),
            ref_column: "id".into(),
        }];
        let r = relationships(&cols, &fks, None);
        let pairs: Vec<(String, String, String)> = r["joins"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| {
                (
                    j["from"].as_str().unwrap().to_owned(),
                    j["to"].as_str().unwrap().to_owned(),
                    j["source"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let has = |f: &str, t: &str, s: &str| pairs.contains(&(f.into(), t.into(), s.into()));
        assert!(has("order_items.order_id", "orders.id", "foreign_key"));
        assert!(has("orders.customer_id", "customers.id", "inferred"));
        assert!(has("order_items.product_id", "products.id", "inferred"));
        assert!(has("products.categoryId", "categories.id", "inferred"));
        // A text column never joins an integer key.
        assert!(
            !pairs.iter().any(|p| p.0 == "order_items.status_id"),
            "{pairs:?}"
        );
        assert!(!pairs.iter().any(|p| p.0 == "orders.region"));
        assert_eq!(pairs.len(), 4, "{pairs:?}");
        let only = relationships(&cols, &fks, Some("customers"));
        assert_eq!(only["joins"].as_array().unwrap().len(), 1);
        let none = relationships(&cols[..1], &[], None);
        assert!(none["note"].as_str().unwrap().contains("ask the user"));
    }
}
