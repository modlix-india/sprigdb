//! Show Me's rules: which chart marks fit the fields on a spec's shelves.
//!
//! The rules are data, `chart-rules.json`, shared with Studio's chart
//! editor (it keeps a copy that its tests hold equal), so a tile the
//! assistant proposes and one a person builds are checked the same way.
//!
//! Fields are counted on the shelves:
//! - a **dimension** is a `columns`, `rows` or `color` entry without `agg`;
//! - a **measure** is a `columns`, `rows`, `color` or `size` entry with
//!   `agg` (`size` always counts);
//! - a **date** is a dimension with a `datePart`;
//! - **geo** is a field named like a place (`lat`, `country`, ...);
//! - **lat/lon** is a latitude field (`lat`, `latitude`) and a longitude
//!   field (`lon`, `lng`, `long`, `longitude`), on any shelf. A bare one
//!   is always a dimension: a location is never summed.
//!
//! As in Studio, a point map needs a latitude and a longitude: a place
//! name alone (`country`) would need a filled map, and filled maps aren't
//! drawn yet (they need region shapes), so Show Me goes on to the next
//! rule and the check refuses `map-filled`.
//!
//! A bare `columns` or `rows` entry (no `agg`, no `datePart`) may be
//! either: a query that already groups returns its measures as plain
//! columns. Such an entry counts as a dimension, and a mark fits when it
//! would fit with some of them taken as measures. Studio knows the
//! columns' types and gives every measure an `agg`, so its counts are
//! exact.
//!
//! Each mark has a range of dimensions, measures and dates it fits. A
//! tile whose mark doesn't fit is refused with the marks that do.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use serde::Deserialize;
use serde_json::Value;

/// The rules' text, as Studio reads it too.
pub const RULES_JSON: &str = include_str!("chart-rules.json");

/// A count range; `None` has no upper limit.
pub type Range = (u32, Option<u32>);

/// What one mark needs.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkRule {
    /// Dimensions.
    pub dims: Range,
    /// Measures.
    pub measures: Range,
    /// Date dimensions, when they are limited.
    #[serde(default)]
    pub dates: Option<Range>,
    /// Every dimension must be a date (a KPI's sparkline).
    #[serde(default)]
    pub dims_are_dates: bool,
    /// Needs a geographic field.
    #[serde(default)]
    pub geo: bool,
}

/// One "best mark" rule: the first whose conditions hold wins.
#[derive(Debug, Clone, Deserialize)]
pub struct BestRule {
    /// The conditions (`dims`, `measures`, `dates`, `geo`, `categories`).
    pub when: BTreeMap<String, Value>,
    /// The mark.
    pub mark: String,
    /// Why, in words.
    pub why: String,
}

/// The whole rule set.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rules {
    /// The keys a spec may hold.
    pub keys: Vec<String>,
    /// Aggregations.
    pub aggs: Vec<String>,
    /// Date parts.
    pub date_parts: Vec<String>,
    /// Field names taken as geographic.
    pub geo_names: Vec<String>,
    /// Each mark's needs.
    pub marks: BTreeMap<String, MarkRule>,
    /// The best mark, in order.
    pub best: Vec<BestRule>,
}

/// The rules, parsed once.
///
/// # Panics
/// Never in a build whose tests pass: the file is checked by them.
pub static RULES: LazyLock<Rules> = LazyLock::new(|| {
    #[allow(clippy::expect_used)] // a test parses the same text
    serde_json::from_str(RULES_JSON).expect("chart-rules.json parses")
});

/// The fields a spec puts on its shelves, counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Dimensions.
    pub dims: u32,
    /// Measures.
    pub measures: u32,
    /// Date dimensions.
    pub dates: u32,
    /// A geographic field is on a shelf.
    pub geo: bool,
    /// A latitude and a longitude field are on the shelves.
    pub lat_lon: bool,
    /// Dimensions (counted in `dims`) that might be measures: bare
    /// `columns` and `rows` entries.
    pub unknown: u32,
}

fn entries(v: Option<&Value>) -> Vec<&Value> {
    match v {
        Some(Value::Array(a)) => a.iter().filter(|x| !x.is_null()).collect(),
        Some(Value::Null) | None => Vec::new(),
        Some(x) => vec![x],
    }
}

fn field(v: &Value) -> Option<&str> {
    v.as_str()
        .or_else(|| v.get("field").and_then(Value::as_str))
        .filter(|f| !f.trim().is_empty())
}

fn is_geo(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    RULES.geo_names.contains(&n)
}

/// Field names read as a latitude (any case), as in Studio.
pub const LAT_NAMES: [&str; 2] = ["lat", "latitude"];
/// Field names read as a longitude (any case), as in Studio.
pub const LON_NAMES: [&str; 4] = ["lon", "lng", "long", "longitude"];

fn is_lat(name: &str) -> bool {
    LAT_NAMES.contains(&name.to_ascii_lowercase().as_str())
}

fn is_lon(name: &str) -> bool {
    LON_NAMES.contains(&name.to_ascii_lowercase().as_str())
}

/// Marks Studio doesn't draw yet, and why.
pub const NOT_DRAWN: [(&str, &str); 1] = [(
    "map-filled",
    "it needs region shapes (the outlines of countries or states), which aren't drawn yet",
)];

/// `a, b or c`.
fn either(names: &[&str]) -> String {
    match names.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
        _ => names.join(""),
    }
}

fn not_drawn(mark: &str) -> Option<&'static str> {
    NOT_DRAWN
        .iter()
        .find(|(m, _)| *m == mark)
        .map(|(_, why)| *why)
}

/// Counts the dimensions, measures and dates on `spec`'s shelves.
#[must_use]
pub fn counts(spec: &Value) -> Counts {
    let mut c = Counts::default();
    let (mut lat, mut lon) = (false, false);
    let mut place = |f: &str, c: &mut Counts| {
        c.geo |= is_geo(f);
        lat |= is_lat(f);
        lon |= is_lon(f);
    };
    for shelf in ["columns", "rows", "color", "size"] {
        for e in entries(spec.get(shelf)) {
            let Some(f) = field(e) else { continue };
            place(f, &mut c);
            let agg = e.get("agg").is_some_and(|a| !a.is_null());
            if agg || shelf == "size" {
                c.measures += 1;
            } else {
                c.dims += 1;
                if e.get("datePart").is_some_and(|d| !d.is_null()) {
                    c.dates += 1;
                } else if matches!(shelf, "columns" | "rows") && !is_lat(f) && !is_lon(f) {
                    // A bare latitude or longitude is a location, never a
                    // measure.
                    c.unknown += 1;
                }
            }
        }
    }
    for shelf in ["detail", "tooltip", "label"] {
        for e in entries(spec.get(shelf)) {
            if let Some(f) = field(e) {
                place(f, &mut c);
            }
        }
    }
    c.lat_lon = lat && lon;
    c
}

fn within(n: u32, r: Range) -> bool {
    n >= r.0 && r.1.is_none_or(|hi| n <= hi)
}

fn fits_exactly(r: &MarkRule, c: Counts) -> bool {
    within(c.dims, r.dims)
        && within(c.measures, r.measures)
        && r.dates.is_none_or(|d| within(c.dates, d))
        && (!r.dims_are_dates || c.dates == c.dims)
        && (!r.geo || c.geo)
}

/// Whether `mark` fits `c`, by its rule, with any of the bare entries
/// taken as measures. A point map also needs a latitude and a longitude,
/// and a mark that isn't drawn yet never fits.
#[must_use]
pub fn fits(mark: &str, c: Counts) -> bool {
    let Some(r) = RULES.marks.get(mark) else {
        return false;
    };
    if not_drawn(mark).is_some() || (mark == "map-point" && !c.lat_lon) {
        return false;
    }
    (0..=c.unknown).any(|k| {
        fits_exactly(
            r,
            Counts {
                dims: c.dims - k,
                measures: c.measures + k,
                unknown: 0,
                ..c
            },
        )
    })
}

/// The marks that fit `c`, in the rules' order.
#[must_use]
pub fn fitting(c: Counts) -> Vec<&'static str> {
    RULES
        .marks
        .keys()
        .map(String::as_str)
        .filter(|m| fits(m, c))
        .collect()
}

fn holds(when: &BTreeMap<String, Value>, c: Counts) -> bool {
    when.iter().all(|(k, v)| {
        let range = || -> Range {
            let lo = v.get(0).and_then(Value::as_u64).unwrap_or(0);
            let hi = v.get(1).and_then(Value::as_u64);
            (
                u32::try_from(lo).unwrap_or(u32::MAX),
                hi.map(|h| u32::try_from(h).unwrap_or(u32::MAX)),
            )
        };
        match k.as_str() {
            "dims" => within(c.dims, range()),
            "measures" => within(c.measures, range()),
            "dates" => within(c.dates, range()),
            "geo" => v.as_bool() == Some(c.geo),
            // Categories are known only once the rows are in: Studio checks
            // them, a spec alone doesn't rule a mark out by them.
            _ => true,
        }
    })
}

/// The best mark for `c` and why: the first rule that holds, skipping
/// marks that aren't drawn and a point map without a latitude and a
/// longitude (as Studio's Show Me does).
#[must_use]
pub fn best(c: Counts) -> (&'static str, &'static str) {
    RULES
        .best
        .iter()
        .find(|b| {
            holds(&b.when, c)
                && not_drawn(&b.mark).is_none()
                && (b.mark != "map-point" || c.lat_lon)
        })
        .map_or(("text-table", ""), |b| (b.mark.as_str(), b.why.as_str()))
}

/// Refuses a mark that doesn't fit its shelves, naming those that do.
///
/// # Errors
/// What is wrong, for the model or the user.
pub fn check_fit(spec: &Value) -> Result<(), String> {
    let Some(mark) = spec.get("mark").and_then(Value::as_str) else {
        return Ok(());
    };
    let c = counts(spec);
    if fits(mark, c) {
        return Ok(());
    }
    let (b, why) = best(c);
    if let Some(not) = not_drawn(mark) {
        return Err(format!(
            "a {mark} chart isn't available: {not}. Use map-point with a latitude and a longitude \
             field, or another mark; marks that fit: {}. Show Me would pick {b} ({why})",
            fitting(c).join(", ")
        ));
    }
    if mark == "map-point" && !c.lat_lon {
        return Err(format!(
            "a map-point chart needs a latitude field (named {}) and a longitude field (named {}) \
             on the shelves; a place name alone isn't a point map, and filled maps aren't drawn yet; \
             marks that fit: {}. Show Me would pick {b} ({why})",
            either(&LAT_NAMES),
            either(&LON_NAMES),
            fitting(c).join(", ")
        ));
    }
    Err(format!(
        "a {mark} chart doesn't fit {} dimension(s), {} measure(s) and {} date(s) on the shelves; \
         marks that fit: {}. Show Me would pick {b} ({why}). A measure is a field with agg; a dimension has none",
        c.dims,
        c.measures,
        c.dates,
        fitting(c).join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::tools::builtin::{AGGS, CHART_KEYS, DATE_PARTS, MARKS};

    #[test]
    fn the_rules_parse_and_agree_with_the_checks_lists() {
        let r = &*RULES;
        assert_eq!(r.keys, CHART_KEYS);
        assert_eq!(r.aggs, AGGS);
        assert_eq!(r.date_parts, DATE_PARTS);
        let mut marks: Vec<&str> = r.marks.keys().map(String::as_str).collect();
        let mut want = MARKS.to_vec();
        marks.sort_unstable();
        want.sort_unstable();
        assert_eq!(marks, want);
        for b in &r.best {
            assert!(r.marks.contains_key(&b.mark), "{}", b.mark);
        }
    }

    #[test]
    fn show_me_follows_the_rules_of_thumb() {
        let c = |v: Value| counts(&v);
        // One date and a measure: line.
        let date = c(json!({"columns": [{"field": "week", "datePart": "week"}],
                             "rows": [{"field": "revenue", "agg": "sum"}]}));
        assert_eq!(
            date,
            Counts {
                dims: 1,
                measures: 1,
                dates: 1,
                geo: false,
                lat_lon: false,
                unknown: 0
            }
        );
        assert_eq!(best(date).0, "line");
        // One dimension and one measure: bar; pie fits too.
        let one =
            c(json!({"columns": [{"field": "status"}], "rows": [{"field": "n", "agg": "count"}]}));
        assert_eq!(best(one).0, "bar");
        assert!(fits("pie", one) && fits("donut", one));
        // Exact counts (Studio's) rule a scatter out; a bare name might be
        // a measure, so the spec alone doesn't.
        assert!(!fits("point", Counts { unknown: 0, ..one }));
        assert!(fits("point", one));
        // A grouped query's measure as a bare name fits a bar.
        assert!(fits(
            "bar",
            c(json!({"columns": ["status"], "rows": ["n"]}))
        ));
        assert!(!fits("bar", c(json!({"color": "status"}))));
        // Two measures: scatter.
        let two = c(
            json!({"columns": [{"field": "a", "agg": "sum"}], "rows": [{"field": "b", "agg": "sum"}]}),
        );
        assert_eq!(best(two).0, "point");
        // One measure alone: KPI; with a date it's still a KPI with a sparkline.
        assert_eq!(
            best(c(json!({"rows": [{"field": "a", "agg": "sum"}]}))).0,
            "kpi"
        );
        assert!(fits("kpi", date));
        assert!(!fits("kpi", one));
        // Two dimensions and a measure: heatmap.
        let heat = c(
            json!({"columns": ["day"], "rows": ["hour"], "color": {"field": "n", "agg": "count"}}),
        );
        assert_eq!(best(heat).0, "heatmap");
        // A place name alone would be a filled map, which isn't drawn: the
        // next rule.
        let country = c(json!({"columns": ["country"], "rows": [{"field": "n", "agg": "sum"}]}));
        assert!(country.geo && !country.lat_lon);
        assert_eq!(best(country).0, "bar");
        // Nothing: a table, and every mark that needs fields is refused.
        assert_eq!(best(Counts::default()).0, "text-table");
        assert!(check_fit(&json!({"mark": "bar"})).is_err());
        let e = check_fit(
            &json!({"mark": "pie", "columns": ["a", "b"], "rows": [{"field": "n", "agg": "sum"}]}),
        )
        .unwrap_err();
        assert!(e.contains("marks that fit") && e.contains("heatmap"), "{e}");
    }

    #[test]
    fn a_point_map_needs_a_latitude_and_a_longitude() {
        let c = |v: Value| counts(&v);
        // Both, in any case and on any shelf: a point map, and Show Me picks it.
        let ll = c(json!({"columns": ["Longitude"], "rows": ["lat"],
                          "size": {"field": "n", "agg": "sum"}}));
        assert!(ll.lat_lon && ll.geo);
        assert_eq!(
            ll.unknown, 0,
            "a bare latitude or longitude is never a measure"
        );
        assert_eq!(best(ll).0, "map-point");
        assert!(fits("map-point", ll));
        let lng = c(json!({"columns": ["latitude"], "detail": ["lng"]}));
        assert!(lng.lat_lon);
        assert!(fits("map-point", lng));
        assert!(check_fit(&json!({"mark": "map-point", "columns": ["LAT", "long"]})).is_ok());
        // Only one of them, or a place name: no point map.
        for spec in [
            json!({"mark": "map-point", "columns": ["country"], "rows": [{"field": "n", "agg": "sum"}]}),
            json!({"mark": "map-point", "columns": ["lat"], "rows": [{"field": "n", "agg": "sum"}]}),
            json!({"mark": "map-point", "columns": ["city", "longitude"]}),
        ] {
            let k = counts(&spec);
            assert!(!k.lat_lon && !fits("map-point", k), "{spec}");
            assert_ne!(best(k).0, "map-point", "{spec}");
            let e = check_fit(&spec).unwrap_err();
            assert!(
                e.contains("needs a latitude field") && e.contains("marks that fit"),
                "{e}"
            );
        }
        // Lone place names fall through to the next rule.
        assert_eq!(
            best(c(
                json!({"columns": ["state", "city"], "rows": [{"field": "n", "agg": "sum"}]})
            ))
            .0,
            "heatmap"
        );
        assert_eq!(best(c(json!({"columns": ["country"]}))).0, "text-table");
    }

    #[test]
    fn a_filled_map_is_refused_until_it_is_drawn() {
        let spec = json!({"mark": "map-filled", "columns": ["country"],
                          "rows": [{"field": "n", "agg": "sum"}]});
        let c = counts(&spec);
        assert!(!fits("map-filled", c));
        assert!(!fitting(c).contains(&"map-filled"));
        let e = check_fit(&spec).unwrap_err();
        assert!(
            e.contains("region shapes") && e.contains("map-point") && e.contains("bar"),
            "{e}"
        );
        // Every not-drawn mark is a mark of the rules.
        for (m, _) in NOT_DRAWN {
            assert!(RULES.marks.contains_key(m), "{m}");
        }
    }

    #[test]
    fn latitude_and_longitude_names_are_geographic() {
        for n in LAT_NAMES.iter().chain(LON_NAMES.iter()) {
            assert!(is_geo(n), "{n}");
        }
    }
}
